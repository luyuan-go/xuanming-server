"""owner 权威 MySQL/TiDB 数据层 —— 对应 Go 侧 internal/data/owner_repo.go。

★ 全仓正确性最敏感的一段代码。核心约束(§9.22):

    owner_epoch、lease 截止、admit_not_before、PENDING→ADMITTED
    **必须处于同一个线性一致事务域**。

    禁止把 owner 放 MySQL、准入 lease / 屏障放 Redis 或 etcd 后再跨存储"先查后写" ——
    那样 CAS 的线性化点与屏障计算不在同一致性域,脑裂窗口重新打开。

所以每个 transition 的形状固定为:

    BEGIN
      SELECT ... FROM owner_record WHERE player_id=? FOR UPDATE     ← 串行化锚点
      SELECT ... FROM ds_instance_lease WHERE instance_uid=? FOR UPDATE  ← 屏障取值
      (判定 / 计算 / CAS / 写审计)
    COMMIT

锁序固定 `owner_record → ds_instance_lease`(Renew 只锁 lease 行)—— 无环无死锁。

TiDB 安全:只锁**存在行** + 条件更新,不依赖间隙锁。
⚠️ TiDB 无 gap 锁,`FOR UPDATE` 在零行时**不加锁** —— 所以"记录不存在"的分支
不能靠 FOR UPDATE 互斥,必须靠主键 INSERT 的唯一键冲突来兜(见 _ensure_record)。
"""

from __future__ import annotations

import contextlib

from pandorapy import errcode, mysqlx
from pandorapy import log as plog
from pandorapy import source_revision as source_revision_mod
from pandorapy.services.owner import data as odata


class MySQLOwnerRepo:
    """基于 asyncmy / aiomysql 的 OwnerRepo。"""

    __slots__ = ("_pool", "_reject_legacy_source_revision")

    def __init__(self, pool) -> None:  # noqa: ANN001
        self._pool = pool
        # 全局 legacy(source_revision=0)拒绝门。默认**关**= 兼容窗行为。
        #
        # 它是 INC-20260818-003 分阶段发布的最后一步,只有在**证明旧 hub_allocator
        # 已排空**之后才允许打开;打开后任何不带来源版本的 Begin 一律被拒。
        # 逐玩家那条规则(见过非零版本就永久拒 legacy)**不受本开关控制** ——
        # 它从第一个新写者写下版本那一刻起就对该玩家自动生效。
        self._reject_legacy_source_revision = False

    def set_reject_legacy_source_revision(self, reject: bool) -> None:
        """打开 / 关闭全局 legacy 拒绝门。对应 Go 的 SetRejectLegacySourceRevision。"""
        self._reject_legacy_source_revision = bool(reject)

    # ── 读 ───────────────────────────────────────────────────────────────────

    async def query(self, player_id: int) -> odata.OwnerRecord:
        """读当前记录(无行返回 epoch=0/none;附带派生 lease 截止)。

        ★ 调用方查询失败一律按 UNKNOWN 处理 —— 所以这里数据库出错必须**抛异常**,
        绝不能返回一条空记录。空记录的语义是"确实没有 owner",会让调用方放行第二个 owner。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(_SQL_SELECT_RECORD, (player_id,))
            row = await cur.fetchone()
            if row is None:
                return odata.OwnerRecord(player_id=player_id)
            rec = _row_to_record(row)
            lease = await _read_lease(cur, rec.target.instance_uid)
        return _with_lease(rec, lease)

    # ── BeginTransition ──────────────────────────────────────────────────────

    async def begin_transition(
        self,
        player_id: int,
        expect_epoch: int,
        operation_id: str,
        owner_type: int,
        target: odata.OwnerTarget,
        source_revision: int,
        skew_margin_seconds: int,
    ) -> odata.OwnerRecord:
        """CAS expect_epoch → epoch+1 / PENDING / newTarget。

        判定顺序(与 Go 侧逐条一致,**顺序本身是契约**):
          1. hub_source_revision 闸门 → ErrOwnerSourceRevisionStale
          2. 幂等重放(同 operation + epoch=expect+1 + 目标全等)→ 原样返回
          3. 同 exact 身份的重复投递 → no-op,原样返回既有记录
          4. expect_epoch 不符 → ErrOwnerEpochConflict(**附当前记录**)
          5. 真实迁移 → epoch+1、算屏障、写记录 + 审计

        ★ 第 1 步必须在**所有**后续分支之前(2026-08-19 修正:本文件此前把它排在
          no-op 与 epoch CAS 之后,是错的)。理由就是 INC-20260818-003 的事故形状:
          旧 binary 手上握着一个**合法**的 expect_epoch(它先 Begin 后 CAS),
          所以 epoch 检查放不倒它 —— 能判定"谁的来源更新"的只有本闸。
          闸排在 no-op 之后时,重复投递整条跳过校验,门等于没设。

        ★ 高水位推进必须在 2、3 两条 no-op 早退分支里**也做**(同批修正)。
          hub 侧把存量 legacy(0)补成 R 时 target 一个字节都不变,这次 Begin 必然
          落到幂等重放 / same_target 的 return;推水位的代码若只在下游就永远走不到。
          后果是 assignment 侧已经有号、owner 侧水位永久停在 0,
          「某玩家见过非零版本就永久拒 legacy」这条逐玩家防线对这批玩家从不 arm。
        """
        now = odata.now_ms()
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    await _ensure_record(cur, player_id)
                    await cur.execute(_SQL_SELECT_RECORD_FOR_UPDATE, (player_id,))
                    row = await cur.fetchone()
                    current = _row_to_record(row) if row else odata.OwnerRecord(player_id=player_id)

                    same_target = current.target == target

                    # ① 来源版本闸门(INC-20260818-003)。★ 必须在所有后续分支之前。
                    #
                    #    只对 HUB 生效:来源版本由 hub_allocator 领号,BATTLE 迁移不带号
                    #    也不动水位。若这里对 BATTLE 也比较,battle 的 revision=0 会被
                    #    「见过非零就拒 legacy」那条挡下,玩家将**永远进不了战斗** ——
                    #    这是本条最容易踩的一脚。
                    #
                    #    ★ same_target 要传**真值**:同一版本号指向**不同** target
                    #    = 铸号被复制(两个写者共用同一任期),必须拒;同 target 的重投
                    #    则是正常重试。写死 False 会把后者也判成 reuse。
                    if owner_type == odata.OWNER_TYPE_HUB:
                        decision = source_revision_mod.classify(
                            incoming=source_revision,
                            high_water=current.hub_source_revision,
                            same_target=same_target,
                            reject_legacy_globally=self._reject_legacy_source_revision,
                        )
                        if not source_revision_mod.is_allowed(decision):
                            await conn.rollback()
                            plog.get().warning(
                                "owner_source_revision_rejected",
                                player_id=player_id,
                                reason=decision,
                                incoming_revision=source_revision,
                                high_water=current.hub_source_revision,
                                current_epoch=current.owner_epoch,
                                expect_epoch=expect_epoch,
                                operation_id=operation_id,
                                same_target=same_target,
                                hint=(
                                    "来源更旧的 hub assignment 被拒;调用方应重查自身 "
                                    "assignment,不要拿更大的 epoch 重试"
                                ),
                            )
                            raise errcode.PandoraError(
                                errcode.ErrOwnerSourceRevisionStale,
                                "hub source revision %d rejected against high-water %d (%s)",
                                source_revision,
                                current.hub_source_revision,
                                decision,
                            )

                    # 高水位推进值。★ 两条 no-op 早退分支里也要落库 —— 见 docstring。
                    next_revision = current.hub_source_revision
                    if (
                        owner_type == odata.OWNER_TYPE_HUB
                        and source_revision > current.hub_source_revision
                    ):
                        next_revision = source_revision

                    async def _advance_high_water() -> None:
                        """no-op 早退前把高水位落库(闸门上面已判 allow,这里只落库)。"""
                        if next_revision == current.hub_source_revision:
                            return
                        await cur.execute(
                            _SQL_ADVANCE_SOURCE_REVISION,
                            (next_revision, odata.now_ms(), player_id),
                        )

                    # ② 幂等重放:同 operation 且记录就是本次 Begin 的结果
                    #    (epoch=expect+1 / 类型与目标全等)。响应丢失后的原样重试拿回
                    #    同一结果,不再推进 epoch(§9.23 端到端幂等)。
                    #    operation_id 为空时本分支不适用(空 = 调用方未持显式幂等键,
                    #    交由 ③ 的同实例收敛)。
                    if (
                        operation_id
                        and current.operation_id == operation_id
                        and current.owner_epoch == expect_epoch + 1
                        and current.owner_type == owner_type
                        and same_target
                    ):
                        lease = await _read_lease(cur, current.target.instance_uid)
                        await _advance_high_water()
                        await conn.commit()
                        return _with_lease(
                            dataclasses_replace(current, hub_source_revision=next_revision), lease
                        )

                    # ③ 同 exact owner 身份的重复投递 → no-op,原样返回既有记录
                    #    (不推进 epoch、不改 phase、**不覆盖 operation_id**)。
                    #    这正是"权威铸造 operation"能成立的前提:重复投递不换 operation。
                    #
                    #    必须要求**完整** Target 相等:assignment_or_allocation_id 是票据/
                    #    准入所绑定的归属版本,release_track 也是 exact 身份的一部分。
                    #    只按物理实例做 no-op 会让新 assignment 继承旧 epoch/ADMITTED phase,
                    #    旧票与新归属共享 fencing 版本。
                    if (
                        current.owner_type == owner_type
                        and same_target
                        and current.phase
                        in (odata.OWNER_PHASE_PENDING, odata.OWNER_PHASE_ADMITTED)
                    ):
                        lease = await _read_lease(cur, current.target.instance_uid)
                        await _advance_high_water()
                        await conn.commit()
                        return _with_lease(
                            dataclasses_replace(current, hub_source_revision=next_revision), lease
                        )

                    # ④ epoch CAS。附当前记录 —— 调用方要靠它决定是重试还是放弃。
                    if current.owner_epoch != expect_epoch:
                        lease = await _read_lease(cur, current.target.instance_uid)
                        await conn.rollback()
                        # 单次冲突是 §9.23 query-first 的正常竞争(故 INFO);同一 player
                        # 高频冲突 = 两个调用方在抢 owner 迁移,靠这条可观测频率与双方 epoch
                        # (否则 in-band 业务码只被 access log 记 DEBUG,看不见)。
                        plog.get().info(
                            "owner_epoch_conflict",
                            player_id=player_id,
                            expect_epoch=expect_epoch,
                            current_epoch=current.owner_epoch,
                            operation_id=operation_id,
                        )
                        raise _epoch_conflict(_with_lease(current, lease), expect_epoch)

                    # ④ 屏障:同事务 FOR UPDATE 读**旧**实例租约,取 CAS 线性化点观察值。
                    #    读的是旧 target 的 uid —— 屏障问的是"旧 owner 什么时候一定停了"。
                    #
                    # ★ 只对**旧 owner 是 BATTLE** 时读(与 Go 的
                    # `if rec.OwnerType == OwnerTypeBattle && rec.Target.InstanceUID != ""` 一致)。
                    # HUB 分支刻意不读实例租约:hub 租约被 allocator 持续代续,等它是
                    # 恒定 ~27s 的纯延迟、零安全收益(屏障值对非 BATTLE 本来就恒为 now)。
                    # 放开条件的代价不是算错屏障,是**白拿一把行锁**:同一 hub 实例上并发的
                    # HUB→BATTLE Begin 会在 ds_instance_lease 同一行上串行,还与 allocator
                    # 的续租 UPDATE 互相排队 —— 而 Go 侧同负载下这把锁根本不存在。
                    # ★ 读租约与算屏障必须在**同一个条件**里(与 Go 的单一 if 结构一致):
                    # 分开写的话,「旧 owner 是 BATTLE 但 instance_uid 为空」会走进
                    # compute_admit_not_before_ms 的 BATTLE 分支,拿到 now + 余量 ——
                    # 而 Go 在这一格是 now(不加余量:本分支不依赖旧 DS 的本地自 fencing
                    # 时钟,Admit 的判定与这里同库同钟,没有跨机偏移要补)。
                    # 多出来的那个余量是纯延迟:每次这类迁移白等一个 skew。
                    old_lease = 0
                    barrier = now
                    if (
                        current.owner_type == odata.OWNER_TYPE_BATTLE
                        and current.target.instance_uid
                    ):
                        old_lease = await _read_lease_for_update(
                            cur, current.target.instance_uid
                        )
                        barrier = odata.compute_admit_not_before_ms(
                            current.owner_type, old_lease, now, skew_margin_seconds
                        )
                    # **屏障必须跨 Release 存活**(INC-20260824-003,2026-08-24)。
                    #
                    # 上面这套分流的判据是「**当前**记录还指向哪台 BATTLE 实例」,而
                    # release 的 UPDATE 恰好清空 owner_type / instance_uid ——「先释放、
                    # 后迁移」的顺序会让这里落到 now,屏障塌成 0,而那台旧战斗 DS 可能
                    # 仍活着(Pawn 仍被模拟、journal 迟到写在途)。这不是某条链的疏忽:
                    # login 登出释放对 BATTLE 归属一视同仁,对局中登出再重登就走它。
                    #
                    # 修法是把屏障从「归属指针的派生量」改成「玩家这一行的留存事实」:
                    # release 时算好盖进 admit_not_before,这里取 max 认回来。于是
                    # 「谁持有归属」与「何时才允许再次可玩」解耦,安全性不再依赖
                    # 「记得在实例回收之后才释放」这种调用方纪律。
                    # 取 max 而非直接采用:屏障只前进,陈旧留存值(早已过期)天然失效。
                    if current.admit_not_before_ms > barrier:
                        barrier = current.admit_not_before_ms

                    new_epoch = current.owner_epoch + 1
                    await cur.execute(
                        _SQL_UPDATE_RECORD,
                        (
                            new_epoch,
                            owner_type,
                            odata.OWNER_PHASE_PENDING,
                            target.pod_name,
                            target.instance_uid,
                            target.instance_epoch,
                            target.assignment_or_allocation_id,
                            target.release_track,
                            operation_id,
                            barrier,
                            next_revision,
                            now,
                            player_id,
                            current.owner_epoch,  # 再次 CAS:防同事务外的并发写
                        ),
                    )
                    if cur.rowcount != 1:
                        await conn.rollback()
                        raise _epoch_conflict(current, expect_epoch)

                    await _write_log(
                        cur,
                        player_id,
                        current.owner_epoch,
                        new_epoch,
                        odata.TRANSITION_OP_BEGIN,
                        operation_id,
                        odata.transition_detail(target, barrier, current.target.pod_name),
                    )
                    new_lease = await _read_lease(cur, target.instance_uid)
                await conn.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise

        plog.get().info(
            "owner_transition_begin",
            player_id=player_id,
            from_epoch=current.owner_epoch,
            to_epoch=new_epoch,
            owner_type=owner_type,
            operation_id=operation_id,
            admit_not_before_ms=barrier,
            barrier_wait_ms=max(0, barrier - now),
        )
        return odata.OwnerRecord(
            player_id=player_id,
            owner_epoch=new_epoch,
            owner_type=owner_type,
            phase=odata.OWNER_PHASE_PENDING,
            target=target,
            operation_id=operation_id,
            admit_not_before_ms=barrier,
            lease_deadline_ms=new_lease,
            updated_at_ms=now,
            hub_source_revision=next_revision,
        )

    # ── Admit ────────────────────────────────────────────────────────────────

    async def admit(
        self, player_id: int, owner_epoch: int, operation_id: str, target: odata.OwnerTarget
    ) -> tuple[odata.OwnerRecord, int]:
        """屏障开 + epoch/operation/实例全等 → PENDING→ADMITTED。

        已 ADMITTED **幂等重放**(ACK 丢失后重放必须返回同一结果,不能再分配)。
        屏障未开 → ErrOwnerBarrierNotOpen(retry_after_ms > 0)。
        """
        now = odata.now_ms()
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    await cur.execute(_SQL_SELECT_RECORD_FOR_UPDATE, (player_id,))
                    row = await cur.fetchone()
                    found = row is not None
                    current = _row_to_record(row) if found else odata.OwnerRecord(player_id=player_id)

                    reason = odata.admit_mismatch_reason(
                        found, current, owner_epoch, operation_id, target
                    )
                    if reason:
                        await conn.rollback()
                        plog.get().warning(
                            "owner_admit_rejected",
                            player_id=player_id,
                            reason=reason,
                            req_epoch=owner_epoch,
                            cur_epoch=current.owner_epoch,
                            req_operation_id=operation_id,
                            cur_operation_id=current.operation_id,
                            req_pod=target.pod_name,
                            cur_pod=current.target.pod_name,
                        )
                        raise errcode.PandoraError(
                            errcode.ErrOwnerIdentityMismatch, "admit rejected: %s", reason
                        )

                    # 已 ADMITTED:幂等重放,直接返回(不再写库、不重复审计)。
                    if current.phase == odata.OWNER_PHASE_ADMITTED:
                        lease = await _read_lease(cur, target.instance_uid)
                        await conn.commit()
                        return _with_lease(current, lease), 0

                    # ★ 屏障:now < admit_not_before 一律拒。这是核心时序不等式的执行点。
                    wait_ms = odata.barrier_wait_ms(current, now)
                    if wait_ms > 0:
                        await conn.rollback()
                        odata.log_barrier_not_open(player_id, current, wait_ms)
                        raise odata.barrier_not_open_error(wait_ms, current)

                    await cur.execute(
                        _SQL_ADMIT,
                        (odata.OWNER_PHASE_ADMITTED, now, player_id, owner_epoch, operation_id),
                    )
                    if cur.rowcount != 1:
                        await conn.rollback()
                        raise errcode.PandoraError(
                            errcode.ErrOwnerIdentityMismatch,
                            "admit lost race for player %d epoch %d",
                            player_id,
                            owner_epoch,
                        )
                    await _write_log(
                        cur,
                        player_id,
                        owner_epoch,
                        owner_epoch,
                        odata.TRANSITION_OP_ADMIT,
                        operation_id,
                        odata.transition_detail(target, current.admit_not_before_ms),
                    )
                    lease = await _read_lease(cur, target.instance_uid)
                await conn.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise

        plog.get().info(
            "owner_transition_admit",
            player_id=player_id,
            owner_epoch=owner_epoch,
            operation_id=operation_id,
        )
        admitted = odata.OwnerRecord(
            player_id=player_id,
            owner_epoch=owner_epoch,
            owner_type=current.owner_type,
            phase=odata.OWNER_PHASE_ADMITTED,
            target=target,
            operation_id=operation_id,
            admit_not_before_ms=current.admit_not_before_ms,
            lease_deadline_ms=lease,
            updated_at_ms=now,
            hub_source_revision=current.hub_source_revision,
        )
        return admitted, 0

    # ── RenewInstanceLease ───────────────────────────────────────────────────

    async def renew_instance_lease(
        self, target: odata.OwnerTarget, lease_seconds: int
    ) -> int:
        """实例租约续期。★ deadline **只前进**;实例纪元不符拒。

        只前进很重要:允许回退等于让一次迟到的短续租**缩短**已经算进屏障的截止时刻,
        新 owner 就可能提前开始可玩 —— 脑裂。
        """
        now = odata.now_ms()
        want = now + lease_seconds * 1000
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    await cur.execute(_SQL_SELECT_LEASE_FOR_UPDATE, (target.instance_uid,))
                    row = await cur.fetchone()
                    if row is None:
                        await cur.execute(
                            _SQL_INSERT_LEASE,
                            (
                                target.instance_uid,
                                target.pod_name,
                                target.instance_epoch,
                                target.release_track,
                                want,
                                now,
                            ),
                        )
                        await conn.commit()
                        return want

                    cur_pod, cur_epoch, cur_deadline = row[1], int(row[2]), int(row[4])
                    # 纪元守卫:只对"双方都非零且不同"拒 —— hub 凭据不携带实例纪元。
                    if target.instance_epoch and cur_epoch and target.instance_epoch != cur_epoch:
                        await conn.rollback()
                        raise errcode.PandoraError(
                            errcode.ErrOwnerLeaseRegressed,
                            "instance epoch mismatch for %s: req=%d cur=%d",
                            target.instance_uid,
                            target.instance_epoch,
                            cur_epoch,
                        )
                    effective = max(cur_deadline, want)
                    await cur.execute(
                        _SQL_UPDATE_LEASE,
                        (
                            target.pod_name or cur_pod,
                            target.instance_epoch or cur_epoch,
                            target.release_track,
                            effective,
                            now,
                            target.instance_uid,
                        ),
                    )
                await conn.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
        return effective

    # ── Release ──────────────────────────────────────────────────────────────

    async def release(
        self, player_id: int, owner_epoch: int, operation_id: str, skew_margin_seconds: int
    ) -> odata.OwnerRecord:
        """epoch + operation 匹配 → 置 none(**epoch 保留**);不匹配(迟到)幂等 no-op。

        ★ 三个关键点:
          - epoch **不清零**:清零等于让下一次 Begin 的 expect_epoch=0 通过,
            旧写者的迟到 CAS 又能命中。
          - hub_source_revision **不动**:清零等于「打完一局回大厅」就把门重新对
            legacy(0)敞开,滚动窗口里的旧写者随即又能写进来(INC-20260818-003)。
          - 释放 BATTLE 归属时 **admit_not_before 必须盖上算好的再入屏障**
            (INC-20260824-003):屏障的判据是 owner_type=BATTLE + instance_uid,
            而本操作正要抹掉这两列,不盖新值等于亲手把屏障降到 0。
            skew_margin_seconds 与 begin_transition 同源同值。
        """
        now = odata.now_ms()
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    await cur.execute(_SQL_SELECT_RECORD_FOR_UPDATE, (player_id,))
                    row = await cur.fetchone()
                    if row is None:
                        await conn.commit()
                        return odata.OwnerRecord(player_id=player_id)
                    current = _row_to_record(row)

                    # 迟到 Release(旧 epoch / 旧 operation / **已释放**)→ 幂等 no-op,
                    # 只能"compare-delete 自己"。
                    # **不能报错** —— 迟到登出是正常现象,报错会让调用方无谓重试。
                    #
                    # ★ owner_type == NONE 这一条不能少:保留 operation_id 之后,
                    #   重放的 Release 会带着**匹配**的 epoch+operation 再进来一次,
                    #   靠前两条拦不住,会重复写一条审计流水。
                    if (
                        current.owner_epoch != owner_epoch
                        or current.operation_id != operation_id
                        or current.owner_type == odata.OWNER_TYPE_NONE
                    ):
                        await conn.commit()
                        # ★ WARN 而不是 DEBUG。no-op 时 owner 记录仍指向那台已死的 DS,
                        #   玩家「卡在旧 DS」直到下一次 BeginTransition —— 而 RPC 返回
                        #   OK + 当前记录、access log 只记 rpc_ok(DEBUG),排查时完全看
                        #   不出释放请求到过、又被以什么理由拒了(login 登出释放与
                        #   allocator 回滚 / 终局释放都走这条)。
                        plog.get().warning(
                            "owner_release_noop",
                            player_id=player_id,
                            reason=odata.release_noop_reason(
                                True, current, owner_epoch, operation_id
                            ),
                            found=True,
                            req_epoch=owner_epoch,
                            current_epoch=current.owner_epoch,
                            req_operation_id=operation_id,
                            current_operation_id=current.operation_id,
                            current_owner_type=current.owner_type,
                            current_phase=current.phase,
                            current_pod=current.target.pod_name,
                            current_instance_uid=current.target.instance_uid,
                            current_instance_epoch=current.target.instance_epoch,
                            current_assignment_id=current.target.assignment_or_allocation_id,
                            current_updated_at_ms=current.updated_at_ms,
                        )
                        return current

                    # 释放前按与 begin_transition 同一个公式算好再入屏障并留存
                    # (INC-20260824-003)。只对 BATTLE 归属算:HUB 归属的屏障按设计
                    # 恒为 now(协作迁移,双写由 epoch fencing 拦、双可玩由客户端单连接
                    # 拆链拦),给它盖屏障等于每次进大厅白卡一个 27s。
                    retained_barrier = current.admit_not_before_ms
                    if (
                        current.owner_type == odata.OWNER_TYPE_BATTLE
                        and current.target.instance_uid
                    ):
                        old_lease = await _read_lease_for_update(
                            cur, current.target.instance_uid
                        )
                        stamped = odata.compute_admit_not_before_ms(
                            current.owner_type, old_lease, now, skew_margin_seconds
                        )
                        # 只前进:连续两次释放不把已建立的屏障往回调。
                        if stamped > retained_barrier:
                            retained_barrier = stamped
                    await cur.execute(
                        _SQL_RELEASE, (retained_barrier, now, player_id, owner_epoch)
                    )
                    await _write_log(
                        cur,
                        player_id,
                        owner_epoch,
                        owner_epoch,
                        odata.TRANSITION_OP_RELEASE,
                        operation_id,
                        odata.transition_detail(current.target, 0),
                    )
                await conn.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise

        # 释放是三个不可逆推进点的最后一个(§11.3 R1):没有它就无法证明玩家是
        # 「被正常放开」还是「记录还挂在旧 DS 上」—— 两者在 owner_record 上都表现为
        # owner_type=none / 仍有值,而时间线只在这条日志与审计流水里。
        plog.get().info(
            "owner_released",
            player_id=player_id,
            owner_epoch=owner_epoch,
            operation_id=operation_id,
            released_owner_type=current.owner_type,
            # 留存屏障要能对账:「释放后下一次 Begin 为什么还等 / 为什么不等」的唯一现场
            # 依据(INC-20260824-003)。缺了它,屏障塌成 0 与屏障正常留存在日志里同形。
            retained_admit_not_before_ms=retained_barrier,
            retained_barrier_remaining_ms=retained_barrier - now,
            pod=current.target.pod_name,
            instance_uid=current.target.instance_uid,
            instance_epoch=current.target.instance_epoch,
            assignment_or_allocation_id=current.target.assignment_or_allocation_id,
        )
        return odata.OwnerRecord(
            player_id=player_id,
            owner_epoch=owner_epoch,  # ★ epoch 保留:清零等于让下一次 Begin 的
            #                            expect_epoch=0 通过,旧写者随即可回滚归属
            owner_type=odata.OWNER_TYPE_NONE,
            phase=odata.OWNER_PHASE_NONE,
            # ★ operation_id 随库里一起保留(与 _SQL_RELEASE 的列清单一致)。清空会让
            #   重放的 Release 无法与「另一条链拿过期 operation 来释放」区分开 ——
            #   详见 _SQL_RELEASE 上方注释。
            operation_id=current.operation_id,
            # ★ admit_not_before_ms 回显**刚盖上的留存屏障**,不是释放前的旧值
            #   (INC-20260824-003)。调用方靠它对账「释放后下一次 Begin 为什么还等」。
            admit_not_before_ms=retained_barrier,
            lease_deadline_ms=0,
            updated_at_ms=now,
            hub_source_revision=current.hub_source_revision,  # ★ 永不清零
        )

    # ── 审计清理 ─────────────────────────────────────────────────────────────

    async def sweep_transition_log(self, retention_days: int, batch: int) -> int:
        """删除超保留期审计行(有界批量,§9.24)。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(_SQL_SWEEP_LOG, (retention_days, batch))
            deleted = cur.rowcount or 0
            await conn.commit()
        return deleted


# ── SQL ──────────────────────────────────────────────────────────────────────

_RECORD_COLS = (
    "player_id, owner_epoch, owner_type, phase, pod_name, instance_uid, instance_epoch, "
    "assignment_or_allocation_id, release_track, operation_id, admit_not_before_ms, "
    "hub_source_revision, updated_at_ms"
)

_SQL_SELECT_RECORD = f"SELECT {_RECORD_COLS} FROM owner_record WHERE player_id = %s"  # noqa: S608
_SQL_SELECT_RECORD_FOR_UPDATE = _SQL_SELECT_RECORD + " FOR UPDATE"

# TiDB 无 gap 锁,FOR UPDATE 在零行时不加锁 —— "记录不存在"的并发分支靠主键
# INSERT IGNORE 的唯一键来互斥,确保后续 FOR UPDATE 一定锁到存在行。
_SQL_ENSURE_RECORD = "INSERT IGNORE INTO owner_record (player_id) VALUES (%s)"

_SQL_UPDATE_RECORD = """UPDATE owner_record SET
    owner_epoch = %s, owner_type = %s, phase = %s,
    pod_name = %s, instance_uid = %s, instance_epoch = %s,
    assignment_or_allocation_id = %s, release_track = %s,
    operation_id = %s, admit_not_before_ms = %s,
    hub_source_revision = %s, updated_at_ms = %s
WHERE player_id = %s AND owner_epoch = %s"""

_SQL_ADMIT = """UPDATE owner_record SET phase = %s, updated_at_ms = %s
WHERE player_id = %s AND owner_epoch = %s AND operation_id = %s"""

# 高水位单独推进(no-op 早退分支用)。只动 hub_source_revision + updated_at_ms,
# 不碰 epoch / phase / target —— 那些正是 no-op 分支承诺不动的东西。
_SQL_ADVANCE_SOURCE_REVISION = (
    "UPDATE owner_record SET hub_source_revision = %s, updated_at_ms = %s WHERE player_id = %s"
)

# ★ Release 只置 type/phase/operation,**不动 owner_epoch 与 hub_source_revision**。
# ⚠️ 列清单里**刻意没有** hub_source_revision(INC-20260818-003):释放归属不该把
# 来源版本高水位一起抹掉。抹掉的后果是「打完一局 / 掉一次线」就把该玩家的门重新对
# legacy(0)敞开,滚动窗口里的旧写者随即又能写进来。以后往这条 UPDATE 加列时,
# 别顺手把它补上 —— 它不在这里是结论,不是遗漏。
#
# ⚠️ 同理**刻意没有** operation_id(2026-08-19 与 Go 对齐时修正:本文件此前清了它)。
# 清掉它会让「已释放」这个状态失去锚点:迟到 Release 的判定就只能靠 operation_id 对不上
# 来兜,而那与「另一条链拿着过期 operation 来释放」完全无法区分 —— 两者在日志里都只剩
# operation_mismatch。保留原值,再配合守卫里的 owner_type == NONE 一条,才能把
# already_released 单独认出来。
#
# ★ admit_not_before_ms 于 2026-08-24 **改为写入**(INC-20260824-003):此前它与
# operation_id 一并保留原值,但那是「不清空」,不是「盖新值」。屏障的判据(owner_type=
# BATTLE + instance_uid)恰恰被本 UPDATE 清空,不盖新值等于释放即拆围栏。现在按与
# begin_transition 同一个公式算好再盖进来 —— 它出现在列清单里是结论,不是手滑。
# 这不影响上面那条锚点:already_released 认的是 owner_type == NONE,与本列无关。
_SQL_RELEASE = """UPDATE owner_record SET
    owner_type = 0, phase = 0,
    pod_name = '', instance_uid = '', instance_epoch = 0,
    assignment_or_allocation_id = '', release_track = '',
    admit_not_before_ms = %s, updated_at_ms = %s
WHERE player_id = %s AND owner_epoch = %s"""

_SQL_SELECT_LEASE = (
    "SELECT instance_uid, pod_name, instance_epoch, release_track, lease_deadline_ms "
    "FROM ds_instance_lease WHERE instance_uid = %s"
)
_SQL_SELECT_LEASE_FOR_UPDATE = _SQL_SELECT_LEASE + " FOR UPDATE"

_SQL_INSERT_LEASE = (
    "INSERT INTO ds_instance_lease "
    "(instance_uid, pod_name, instance_epoch, release_track, lease_deadline_ms, updated_at_ms) "
    "VALUES (%s, %s, %s, %s, %s, %s)"
)

_SQL_UPDATE_LEASE = """UPDATE ds_instance_lease SET
    pod_name = %s, instance_epoch = %s, release_track = %s,
    lease_deadline_ms = %s, updated_at_ms = %s
WHERE instance_uid = %s"""

_SQL_INSERT_LOG = (
    "INSERT INTO owner_transition_log "
    "(player_id, from_epoch, to_epoch, op, operation_id, detail) "
    "VALUES (%s, %s, %s, %s, %s, %s)"
)

_SQL_SWEEP_LOG = (
    "DELETE FROM owner_transition_log "
    "WHERE created_at < DATE_SUB(NOW(), INTERVAL %s DAY) LIMIT %s"
)


# ── 辅助 ─────────────────────────────────────────────────────────────────────


async def _ensure_record(cur, player_id: int) -> None:  # noqa: ANN001
    """保证 owner_record 行存在,让后续 FOR UPDATE 一定锁到存在行(TiDB 无 gap 锁)。"""
    await cur.execute(_SQL_ENSURE_RECORD, (player_id,))


def _row_to_record(row) -> odata.OwnerRecord:  # noqa: ANN001
    return odata.OwnerRecord(
        player_id=int(row[0]),
        owner_epoch=int(row[1]),
        owner_type=int(row[2]),
        phase=int(row[3]),
        target=odata.OwnerTarget(
            pod_name=row[4] or "",
            instance_uid=row[5] or "",
            instance_epoch=int(row[6] or 0),
            assignment_or_allocation_id=row[7] or "",
            release_track=row[8] or "",
        ),
        operation_id=row[9] or "",
        admit_not_before_ms=int(row[10] or 0),
        hub_source_revision=int(row[11] or 0),
        updated_at_ms=int(row[12] or 0),
    )


def _with_lease(rec: odata.OwnerRecord, lease_deadline_ms: int) -> odata.OwnerRecord:
    return dataclasses_replace(rec, lease_deadline_ms=lease_deadline_ms)


def dataclasses_replace(rec: odata.OwnerRecord, **changes) -> odata.OwnerRecord:
    import dataclasses

    return dataclasses.replace(rec, **changes)


async def _read_lease(cur, instance_uid: str) -> int:  # noqa: ANN001
    if not instance_uid:
        return 0
    await cur.execute(_SQL_SELECT_LEASE, (instance_uid,))
    row = await cur.fetchone()
    return int(row[4]) if row else 0


async def _read_lease_for_update(cur, instance_uid: str) -> int:  # noqa: ANN001
    """★ 屏障取值必须 FOR UPDATE —— 取的是 CAS **线性化点**的观察值。

    普通读会让一次并发的续租在我们算完屏障之后提交,于是屏障基于一个已经过期的
    截止时刻算出,新 owner 提前开始可玩。
    """
    if not instance_uid:
        return 0
    await cur.execute(_SQL_SELECT_LEASE_FOR_UPDATE, (instance_uid,))
    row = await cur.fetchone()
    return int(row[4]) if row else 0


async def _write_log(  # noqa: ANN001
    cur, player_id: int, from_epoch: int, to_epoch: int, op: int, operation_id: str, detail: str
) -> None:
    await cur.execute(
        _SQL_INSERT_LOG, (player_id, from_epoch, to_epoch, op, operation_id, detail)
    )


def _epoch_conflict(current: odata.OwnerRecord, expect: int) -> errcode.PandoraError:
    """epoch 冲突 —— **附当前记录**,调用方靠它决定重试还是放弃。"""
    err = errcode.PandoraError(
        errcode.ErrOwnerEpochConflict,
        "expect_epoch %d != current %d",
        expect,
        current.owner_epoch,
    )
    err.current_record = current
    return err


# mysqlx 的错误判别在这里也用得上(唯一键冲突 = 并发建行,可安全忽略)。
__all__ = ["MySQLOwnerRepo", "mysqlx"]
