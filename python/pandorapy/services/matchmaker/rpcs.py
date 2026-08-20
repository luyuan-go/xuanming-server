"""matchmaker 的其余 5 个 RPC 主体 —— 对应 Go 侧 internal/biz/match.go 里
CancelMatch / ConfirmMatch / ReleaseMatch / GetMatchProgress / ResolvePlayerMatchContext。

拆成 mixin 只是文件尺寸问题;它们与 StartMatch 同属**RPC 面**(每个副本都跑),
与 matchloop.py 的后台写者面不同 —— 后者才是必须过 leader election 的那一半。
"""

from __future__ import annotations

import asyncio
import time

from pandora.match.v1 import match_pb2 as matchpb

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy.services.matchmaker import helpers as h


def _now_ms() -> int:
    return int(time.time() * 1000)


class MatchRpcMixin:
    """CancelMatch / ConfirmMatch / ReleaseMatch / GetMatchProgress / Resolve。"""

    # ── RPC 2:CancelMatch ──────────────────────────────────────────────────

    async def cancel_match(self, player_id: int) -> None:
        """取消匹配。以 player_id 为准定位其当前票据。

          - 尚在 start saga(票据主体还没落地)→ 把 saga CAS 成 COMPENSATING
          - 票据仍在排队(未撮合)→ CAS 条件删票据 + 释放成员归属
          - 票据已进 match(确认期)→ 等价于该玩家拒绝确认,走 match 失败流程

        排队路径用 delete_ticket_if_unmatched(CAS)而非"读到 match_id==0 就盲删":
        否则在读与删之间撮合循环可能刚好预留了这张票,盲删会把已进 match 的票据删掉
        并释放成员 claim → 玩家可再排队,同人两场(违反不变量 §1)。
        """
        # 预排队 saga 可能已经建好了正式 claim。优先在它的可发现索引还在时取消它 ——
        # 只删票据/claim 会让那个 durable worker 把它们重建并在本 RPC 返回成功之后入队。
        try:
            handled = await self._cancel_starting_match(player_id)
        except errcode.PandoraError as exc:
            if errcode.as_code(exc) != errcode.ErrMatchConcurrent:
                raise
            handled = False  # QUEUED 交接抢先了:落到下面的 canonical 票据路径
        if handled:
            return

        ticket_id, found = await self.repo.get_player_ticket(player_id)
        if not found:
            raise errcode.PandoraError(
                errcode.ErrMatchNotFound, "player %d not in any queue", player_id
            )
        ticket, found = await self.repo.get_ticket(ticket_id)
        if not found:
            # 票据已消失,清理残留 claim(CAS:仅当仍指向这张旧票,防误删并发新 claim)。
            await self.repo.delete_player_index_if_matches(player_id, ticket_id)
            raise errcode.PandoraError(errcode.ErrMatchNotFound, "ticket %d gone", ticket_id)
        self.require_local_game_mode(ticket.game_mode)

        if ticket.match_id != 0:
            await self._reject_or_reap_orphan(player_id, ticket.match_id)
            return

        deleted, reserved_match = await self.repo.delete_ticket_if_unmatched(ticket_id)
        if not deleted:
            if reserved_match != 0:
                # 读后被撮合循环抢先预留 → 转拒绝确认路径。
                await self._reject_or_reap_orphan(player_id, reserved_match)
                return
            await self.repo.delete_player_index_if_matches(player_id, ticket_id)
            raise errcode.PandoraError(errcode.ErrMatchNotFound, "ticket %d gone", ticket_id)

        await self.rollback_claims(ticket_id, h.member_player_ids(ticket.members))
        # FAILED 补推给票据**全体**成员:取消可能不是本人发起(队长取消 / team 离队
        # 联动撤票),其余队友的客户端仍停在 QUEUEING,不推会一直转圈。
        await self.push_progress(
            ticket.ticket_id, h.STAGE_FAILED, list(ticket.members), "", ticket.map_id
        )
        plog.get().debug("match_cancel", ticket_id=ticket_id, player_id=player_id)

    async def _cancel_starting_match(self, player_id: int) -> bool:
        """在正式 claim 存在之前,把取消记到 durable StartMatch saga 上。

        堵的是 ACCEPTED/TICKET_READY/CLAIMING 这个窗口:冷启动 Resume 已经能看到
        STARTING,而旧 CancelMatch 路径会答 NOT_FOUND 并让 worker 在之后照常入队。

        phase CAS 是取消的**提交点**。清空 lease 同时围栏住一个可能正在跑的 worker:
        它之后用陈旧 lease token 做的任何 phase 写入都会被拒,而它可能已经完成的外部
        写入由幂等的 COMPENSATING worker 清掉。due 索引是派生的;权威 phase 一旦提交,
        索引写失败**不得**把一次已受理的取消变成 RPC 失败(reconciler 会重建索引)。
        """
        ticket_id, found = await self.repo.get_start_player_operation(player_id)
        if not found:
            return False
        op, found = await self.repo.get_start_operation(ticket_id)
        if not found:
            await self.repo.delete_start_player_if_matches(player_id, ticket_id)
            return False
        if h.member_index(op.members, player_id) < 0:
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "start player index %d points to unrelated operation %d",
                player_id,
                ticket_id,
            )
        self.require_local_game_mode(op.game_mode)

        stamp = _now_ms()
        state = {"committed": False}

        def _mutate(rec: matchpb.MatchStartOperationStorageRecord) -> None:
            if h.member_index(rec.members, player_id) < 0:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "start operation %d no longer owns player %d",
                    ticket_id,
                    player_id,
                )
            if rec.phase == matchpb.MATCH_START_PHASE_QUEUED:
                # 归属正在交接给 canonical 票据。客户端必须重查 / 重试,
                # 而不是把这场竞态当成终态成功。
                raise errcode.PandoraError(
                    errcode.ErrMatchConcurrent,
                    "start operation %d already handed off to queue",
                    ticket_id,
                )
            if rec.phase == matchpb.MATCH_START_PHASE_FAILED:
                state["committed"] = True
                return
            if rec.phase != matchpb.MATCH_START_PHASE_COMPENSATING:
                rec.phase = matchpb.MATCH_START_PHASE_COMPENSATING
            state["committed"] = True
            rec.next_attempt_at_ms = stamp
            rec.lease_token = ""
            rec.lease_deadline_ms = 0

        await self.repo.update_start_operation_with_lock(
            ticket_id, self.cfg.optimistic_retry, _mutate, self.ticket_ttl_sec()
        )
        if not state["committed"]:
            raise errcode.PandoraError(
                errcode.ErrMatchConcurrent,
                "start operation %d cancellation not committed",
                ticket_id,
            )
        try:
            await self.repo.ensure_start_active(ticket_id, stamp)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "match_start_cancel_index_deferred",
                ticket_id=ticket_id,
                player_id=player_id,
                err=str(exc),
            )
        plog.get().debug(
            "match_start_cancel_accepted", ticket_id=ticket_id, player_id=player_id
        )
        return True

    async def _reject_or_reap_orphan(self, player_id: int, match_id: int) -> None:
        """把"已被 match 预留的票据"的取消转成拒绝确认;若 match 已死则收割孤儿票据。

        match 已死的两种形态(都是崩溃残留,正常流程不会出现):
          - ErrMatchNotFound:match 记录不存在(回滚中途崩溃 / match 被释放但票据残留)
          - ErrMatchDeclined:match 已 FAILED(写 FAILED 后、退票完成前崩溃)
        两种情况下票据都既不在队列也不受超时扫描,成员 claim 卡到永久 —— 而玩家意图
        本就是取消,直接删票 + 释放归属 + 推 FAILED,让全员立刻可再匹配。
        安全守卫:重读票据,仅当其仍归属该 match 才收割,并发变化时原样返错不误删。
        """
        try:
            await self.confirm_match(player_id, match_id, False)
            return
        except errcode.PandoraError as exc:
            code = errcode.as_code(exc)
            if code not in (errcode.ErrMatchNotFound, errcode.ErrMatchDeclined):
                raise
            original = exc

        tid, found = await self.repo.get_player_ticket(player_id)
        if not found:
            raise original  # 已无归属可清理,原样返回
        ticket, found = await self.repo.get_ticket(tid)
        if not found:
            # claim 指向已消失的票据:顺手清理(CAS 防误删并发新 claim),取消语义成立。
            await self.repo.delete_player_index_if_matches(player_id, tid)
            return
        if ticket.match_id != match_id:
            raise original  # 票据已归属他处(并发变化),不误删
        await self.repo.delete_ticket(tid)
        await self.rollback_claims(tid, h.member_player_ids(ticket.members))
        await self.push_progress(
            tid, h.STAGE_FAILED, list(ticket.members), "", ticket.map_id
        )
        plog.get().warning(
            "match_cancel_reaped_orphan_ticket",
            ticket_id=tid,
            match_id=match_id,
            player_id=player_id,
        )

    # ── RPC 3:ConfirmMatch ────────────────────────────────────────────────

    async def confirm_match(self, player_id: int, match_id: int, accept: bool) -> None:
        """确认 / 拒绝匹配。

          - accept=False 或任一人拒绝 → match FAILED,其余票据退回队列(保留排队时长)
          - 全员 accept → ALLOCATING(只提交 durable job,拉 DS 交给后台 worker)
        """
        outcome = {"value": "pending", "snapshot": None}

        def _mutate(m: matchpb.MatchStorageRecord) -> None:
            self.require_local_game_mode(m.game_mode)
            # 终态幂等:已失败返回 declined。
            if m.stage == h.STAGE_FAILED:
                raise errcode.PandoraError(
                    errcode.ErrMatchDeclined, "match %d already failed", match_id
                )
            if m.stage in (h.STAGE_ALLOCATING, h.STAGE_READY):
                if not accept:
                    # INC-20260724-001:ALLOCATING 期必须给玩家一个真实出口(§9.20
                    # 禁止"只能杀进程恢复")。边界:仅允许 **未 checkpoint** 的
                    # ALLOCATING 取消(battle_target 未写且 phase ∈ {PENDING,REQUESTING})。
                    # 已 checkpoint / ABORTING / READY 一律维持拒绝语义 —— 那时票已签或
                    # DS 已固化,假装取消会让客户端与 READY 推送打架。
                    idx = h.member_index(m.members, player_id)
                    if idx >= 0 and _cancelable_uncheckpointed(m):
                        m.members[idx].confirm = h.CONFIRM_REJECTED
                        m.stage = h.STAGE_FAILED
                        m.allocation_phase = matchpb.MATCH_ALLOCATION_PHASE_FAILED
                        outcome["value"] = "failed"
                        outcome["snapshot"] = h.clone_match(m)
                        return
                    raise errcode.PandoraError(
                        errcode.ErrInvalidState,
                        "match %d locked (stage=%d, alloc_phase=%d), cannot reject",
                        match_id,
                        int(m.stage),
                        int(m.allocation_phase),
                    )
                # accept 幂等成功。
                outcome["value"] = "pending"
                outcome["snapshot"] = h.clone_match(m)
                return

            idx = h.member_index(m.members, player_id)
            if idx < 0:
                raise errcode.PandoraError(
                    errcode.ErrMatchNotFound, "player %d not in match %d", player_id, match_id
                )
            if not accept:
                m.members[idx].confirm = h.CONFIRM_REJECTED
                m.stage = h.STAGE_FAILED
                outcome["value"] = "failed"
                outcome["snapshot"] = h.clone_match(m)
                return
            m.members[idx].confirm = h.CONFIRM_ACCEPTED
            if h.all_accepted(m.members):
                m.stage = h.STAGE_ALLOCATING
                if not m.allocation_operation_id:
                    m.allocation_operation_id = _new_operation_id()
                m.allocation_phase = matchpb.MATCH_ALLOCATION_PHASE_PENDING
                m.allocation_next_attempt_at_ms = _now_ms()
                outcome["value"] = "all_ready"
            else:
                m.stage = h.STAGE_CONFIRM
                outcome["value"] = "pending"
            outcome["snapshot"] = h.clone_match(m)

        await self.repo.update_match_with_lock(match_id, self.cfg.optimistic_retry, _mutate)

        snapshot = outcome["snapshot"]
        if outcome["value"] == "failed":
            await self.on_match_failed(snapshot, player_id)
        elif outcome["value"] == "all_ready":
            # durable handoff:最后一名确认者只提交 ALLOCATING job。
            # Allocate/placement/READY 由 RunMatchLoop 的服务生命周期 worker 推进,
            # 不再绑定玩家 RPC 的生命周期(玩家一断线分配就中断是旧实现的坑)。
            plog.get().debug(
                "match_allocation_queued",
                match_id=match_id,
                operation_id=snapshot.allocation_operation_id,
            )
        elif snapshot is not None and snapshot.stage == h.STAGE_CONFIRM:
            await self.push_progress(
                match_id, h.STAGE_CONFIRM, list(snapshot.members), "", snapshot.map_id
            )
        plog.get().debug(
            "match_confirm",
            match_id=match_id,
            player_id=player_id,
            accept=accept,
            outcome=outcome["value"],
        )

    # ── RPC 5:ReleaseMatch ────────────────────────────────────────────────

    async def release_match(self, match_id: int, fallback_player_ids: list[int]) -> None:
        """释放一场已结束对局的全部撮合状态,由 battle_result 结算落库后调用。

        ★ 为什么它是**唯一**释放点:对局走完 READY 之后,player→ticket 归属(claim)、
        票据、match 记录仍是**非终态、无 TTL 的持久状态**(见 repo 铁律 ①)。
        不调用就不会随时间自愈 —— 玩家回 Hub 再次 StartMatch 会被 claim 撞上,
        永久报 ErrMatchAlreadyMatching(4002)。释放失败必须靠 battle_result 的 outbox
        持续重试到成功,不能当作"等等就好"。

        全部对象幂等;任一步状态未知就整体返回错误,让 outbox 按同一份证明重试。
        """
        if match_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "match_id required")

        player_set: set[int] = set()
        team_rosters: dict[int, dict] = {}
        ticket_ids: list[int] = []
        match_found = False

        m, found = await self.repo.get_match(match_id)
        if found:
            match_found = True
            ticket_ids = list(m.ticket_ids)
            player_set.update(h.member_player_ids(m.members))
            _collect_team_rosters(team_rosters, m.members)
        for pid in fallback_player_ids:
            if pid != 0:
                player_set.add(pid)

        ticket_set = set(ticket_ids)
        if not match_found:
            # canonical match 已缺失时,只能从 fallback roster 建立**机械证明**:
            # claim 精确指向 tid、tid 精确声明本 match、且 player 确实在该 ticket
            # roster 里。任何缺票 / 损坏都是 UNKNOWN,不能猜成"这是旧局 claim"。
            for pid in list(player_set):
                tid, claimed = await self.repo.get_player_ticket(pid)
                if not claimed:
                    continue
                ticket, found = await self.repo.get_ticket(tid)
                if not found:
                    raise errcode.PandoraError(
                        errcode.ErrUnavailable,
                        "missing match %d player %d claim points to missing ticket %d",
                        match_id,
                        pid,
                        tid,
                    )
                if ticket.match_id != match_id:
                    continue  # 精确证明这是更新/别的操作,原样留着
                if h.member_index(ticket.members, pid) < 0:
                    raise errcode.PandoraError(
                        errcode.ErrUnavailable,
                        "missing match %d player %d is not member of claimed ticket %d",
                        match_id,
                        pid,
                        tid,
                    )
                if tid not in ticket_set:
                    ticket_set.add(tid)
                    ticket_ids.append(tid)
                player_set.update(h.member_player_ids(ticket.members))
                _collect_team_rosters(team_rosters, ticket.members)

        # ① 票据阶段:CAS 删确属本局的票据。先完成全部票据;任一漂移 / 错误都
        # **不进入 claim 阶段** —— 顺序反了会先释放玩家再删票,那张票会被下一轮
        # 撮合重新捡起来。
        problems: list[str] = []
        for tid in ticket_ids:
            try:
                _, found, current = await self.repo.delete_ticket_if_match(tid, match_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                problems.append(f"compare-delete ticket {tid}: {exc}")
                continue
            if found and current != match_id:
                problems.append(f"ticket {tid} drifted from match {match_id} to {current}")
        if problems:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "release match %d: %s", match_id, "; ".join(problems)
            )

        # ② claim 阶段:仅当确属本局才删(防误删结算后新一局 claim)。
        for pid in player_set:
            try:
                await self._release_player_claim(match_id, pid, ticket_set)
            except errcode.PandoraError as exc:
                problems.append(str(exc))
        if problems:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "release match %d: %s", match_id, "; ".join(problems)
            )

        # ③ 复位各队的准备状态(INC-20260813-001 第一根因)。
        # 位置刻意在 claim 释放**之后**、delete_match **之前**:
        #   之后 —— claim 是玩家能不能开下一局的硬闸,不能被队伍状态的可用性拖住
        #   之前 —— 失败时保留 canonical match,让 outbox 按同一份 roster 重投重试
        await self._end_team_matches(match_id, team_rosters)

        # ④ 所有成员清理明确成功后,才硬删 match 记录 + 移出 active。
        await self.repo.delete_match(match_id)
        plog.get().info(
            "match_released",
            match_id=match_id,
            match_found=match_found,
            players=len(player_set),
            tickets=len(ticket_ids),
            teams=len(team_rosters),
        )

    async def _release_player_claim(
        self, match_id: int, player_id: int, ticket_set: set[int]
    ) -> None:
        """释放单个玩家的归属,**仅当其当前 claim 确属本局**。

        玩家若已发起新一局,其 claim 指向新票据(不同 match_id / 不在本局票据集),
        此处不动 —— 误删新 claim 会让他能同时开出第二张票。
        """
        tid, ok = await self.repo.get_player_ticket(player_id)
        if not ok:
            return  # claim 已释放
        belongs = False
        if tid in ticket_set:
            # ticket_id 全局不复用。仍在 claim CAS 之前做一次 fail-closed 复核,
            # 防住滚动升级 / 运维手改破坏该不变量的情况。
            current, found = await self.repo.get_ticket(tid)
            if found and current.match_id != match_id:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "player %d claim ticket %d was reused by match %d",
                    player_id,
                    tid,
                    current.match_id,
                )
            belongs = True
        else:
            t, found = await self.repo.get_ticket(tid)
            if not found:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "player %d claim points to unproven missing ticket %d while releasing match %d",
                    player_id,
                    tid,
                    match_id,
                )
            if t.match_id == match_id:
                # 发现阶段之后又冒出一条同局的边。从头重来,让它的票据先被条件删除,
                # 再删它的 claim(顺序不能反,见 release_match ①)。
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "late ticket %d for match %d requires release rediscovery",
                    tid,
                    match_id,
                )
        if not belongs:
            plog.get().info(
                "release_skip_stale_claim",
                match_id=match_id,
                player_id=player_id,
                current_ticket=tid,
            )
            return
        await self.repo.delete_player_index_if_matches(player_id, tid)

    async def _end_team_matches(self, match_id: int, rosters: dict[int, dict]) -> None:
        """逐队复位准备状态。任一队失败即整体抛错,由上游 outbox 重投。

        ★ 滚动升级共存窗口刻意做成「无顺序依赖」(§9.21):本调用是 matchmaker 新增
        的跨服务出站依赖。若把 team 未升级(ErrNotImplemented)也当成普通失败,就等于
        给发布引入一条「team 必须先于 matchmaker 上线」的顺序约束 —— 而顺序一旦搞错,
        outbox 会一直空转、canonical match 持续积压,且**没有任何机械手段能拦住**。
        所以两类错误分开:
          - ErrNotImplemented → 弱依赖降级(Warn + 跳过)。跳过的后果恰好等于本修复
            落地之前的行为(队伍停在 READY),不产生任何新的错误状态;team 一旦滚上
            新版本,此后每一局的释放都会正常复位。反过来 fail-closed 的话团队状态
            **同样**停在 READY(调用根本没成功),只是额外多了一堆积压 —— 严格更差。
          - 其它任何错误 → 如实上抛,由 outbox 重投到成功
        """
        if self.reader is None or not rosters:
            return
        problems: list[str] = []
        for team_id, roster in rosters.items():
            try:
                await self.reader.end_team_match(
                    team_id, roster["players"], roster["ready_generation"]
                )
            except errcode.PandoraError as exc:
                if errcode.as_code(exc) == errcode.ErrNotImplemented:
                    plog.get().warning(
                        "match_release_end_team_unsupported",
                        match_id=match_id,
                        team_id=team_id,
                        err=str(exc),
                        hint="team 尚未滚到带 EndTeamMatch 的版本;本局队伍不复位准备状态"
                        "(等同修复前行为),team 升级后自动恢复",
                    )
                    continue
                problems.append(f"end team match team={team_id}: {exc}")
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                problems.append(f"end team match team={team_id}: {exc}")
        if problems:
            plog.get().warning(
                "match_release_end_team_failed",
                match_id=match_id,
                err="; ".join(problems),
                hint="队伍会停在 READY,队长可能带着还没回大厅的队友再开一局;outbox 会重试",
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "release match %d: %s", match_id, "; ".join(problems)
            )

    # ── RPC 4:GetMatchProgress ────────────────────────────────────────────

    async def get_match_progress(self, caller_id: int, handle: int) -> matchpb.MatchProgress:
        """查询进度。

          - handle 是客户端句柄:match_id(已撮合)或 ticket_id(排队中)。
            重新登录 / 换设备丢了句柄时传 0,服务端用 caller_id 反查其当前票据。
          - 鉴权(反外挂):caller 必须是该 match/ticket 的成员才返回进度;否则按
            "不存在"处理,不暴露他人对局的存在性 —— **match_id 不是授权凭证**。
          - READY 阶段且 caller 是本局成员时,给他现签一张新 battle 票(新 jti),
            支持换手机 / 掉线重连。
        """
        if caller_id == 0:
            raise errcode.PandoraError(errcode.ErrUnauthorized, "missing caller identity")

        if handle == 0:
            tid, found = await self.repo.get_player_ticket(caller_id)
            if not found:
                tid, found = await self.repo.get_start_player_operation(caller_id)
            if not found:
                raise errcode.PandoraError(
                    errcode.ErrMatchNotFound, "player %d not in any queue", caller_id
                )
            handle = tid

        # ⚠️ handle 跨**两个 ID 空间**:排队中是 ticket_id,已撮合是 match_id,而这两个
        # 空间由同一 nodeID 的两个发号器铸造 —— 同一秒里各自的第 K 个号**逐位相同是
        # 常态**,即某玩家的 ticket_id 可以恰好等于另一局无关的 match_id。
        # 因此探两侧时,"命中了但 caller 不是成员"只说明**这一侧撞的是别人的实体**,
        # 绝不能就此判 4001 短路:必须继续探另一侧,否则排队玩家会被无关对局遮蔽,
        # 拿到误报的 NOT_FOUND,再被客户端错误降级成 Hub 路由。
        # 对外可见行为不变:两侧都不属于 caller 时仍在末尾统一返回 4001。
        prog = await self._read_canonical_progress(caller_id, handle)
        if prog is not None:
            return prog

        # StartMatch 的线性化点是 durable start operation;票据主体由后台 worker
        # 稍后创建。RPC 已返回"已受理"后立即查询时,不能把这个正常窗口误报成 4001。
        op, start_found = await self.repo.get_start_operation(handle)
        if start_found:
            if op.ticket_id != handle or not op.members:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable, "match start operation %d graph is invalid", handle
                )
            if h.member_index(op.members, caller_id) < 0:
                raise errcode.PandoraError(
                    errcode.ErrMatchNotFound, "match/ticket %d not found", handle
                )
            self.require_local_game_mode(op.game_mode)
            if op.phase in (
                matchpb.MATCH_START_PHASE_ACCEPTED,
                matchpb.MATCH_START_PHASE_TICKET_READY,
                matchpb.MATCH_START_PHASE_CLAIMING,
                matchpb.MATCH_START_PHASE_CLAIMS_READY,
                matchpb.MATCH_START_PHASE_QUEUED,
            ):
                return h.ticket_to_progress(h.ticket_from_start_operation(op))
            if op.phase in (
                matchpb.MATCH_START_PHASE_COMPENSATING,
                matchpb.MATCH_START_PHASE_FAILED,
            ):
                return matchpb.MatchProgress(match_id=handle, stage=h.STAGE_FAILED)
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "match start operation %d has invalid phase %d",
                handle,
                int(op.phase),
            )

        # Worker 按「先写 ticket,后删 start operation」交接;若上面的首次 canonical
        # 读取早于 ticket 写入,而 start-op 读取晚于删除,第二次读取必能看到 ticket
        # 或已经形成的 match,避免在两个权威记录之间制造瞬时 NOT_FOUND。
        prog = await self._read_canonical_progress(caller_id, handle)
        if prog is not None:
            return prog
        raise errcode.PandoraError(
            errcode.ErrMatchNotFound, "match/ticket %d not found", handle
        )

    async def _read_canonical_progress(
        self, caller_id: int, handle: int
    ) -> matchpb.MatchProgress | None:
        m, match_found = await self.repo.get_match(handle)
        if match_found and h.member_index(m.members, caller_id) >= 0:
            self.require_local_game_mode(m.game_mode)
            prog = h.match_to_progress(m)
            await self._refresh_battle_ticket(m, caller_id, prog)
            return prog

        t, found = await self.repo.get_ticket(handle)
        if not found:
            return None
        if h.member_index(t.members, caller_id) < 0:
            # 同上:这一侧撞的是别人的票据,交给调用链继续走 start-operation / 末尾 4001。
            return None
        self.require_local_game_mode(t.game_mode)
        if t.match_id != 0:
            # ⚠️ 这里的 t.match_id 是票据里**存下来的真实交叉引用**,不是客户端句柄,
            # 不存在跨空间混叠。所以"caller 在票据里却不在它指向的 match 里"是真正的
            # 数据不一致,必须 fail-closed 报错,**不能**照抄上面的继续探测写法。
            m2, found2 = await self.repo.get_match(t.match_id)
            if found2:
                if h.member_index(m2.members, caller_id) < 0:
                    raise errcode.PandoraError(
                        errcode.ErrMatchNotFound, "match/ticket %d not found", handle
                    )
                self.require_local_game_mode(m2.game_mode)
                prog = h.match_to_progress(m2)
                await self._refresh_battle_ticket(m2, caller_id, prog)
                return prog
        return h.ticket_to_progress(t)

    async def _refresh_battle_ticket(
        self, m: matchpb.MatchStorageRecord, caller_id: int, prog: matchpb.MatchProgress
    ) -> None:
        """READY 阶段为查询者本人现签一张新 battle 票(新 jti),覆盖存储里的旧票。

        换手机 / 掉线重连每次都拿新 jti,不会撞 DS 侧 jti 一次性防重放;票 sub 锁定
        调用者本人,比共享票更严。

        签发链在场但签失败时**整个查询 fail-closed**(可重试 Unavailable):
        存量票绑定的是 claim 时刻的 sjti,顶号换机后必被 DS 兑换点拒绝 ——
        把它交出去只会让客户端拿着废票撞墙。
        allocation 不完整(legacy/dev 记录)保留旧行为:告警 + 沿用存储票字段。
        """
        if caller_id == 0 or m.stage != h.STAGE_READY or not m.battle_ds_addr:
            return
        if h.member_index(m.members, caller_id) < 0:
            return  # 非本局成员,不签票
        allocation, ok = h.allocation_from_match(m)
        if not ok:
            plog.get().warning(
                "resign_battle_ticket_missing_persisted_target",
                match_id=m.match_id,
                player_id=caller_id,
            )
            return
        try:
            token = await self.allocator.sign_battle_ticket(caller_id, m.match_id, allocation)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "resign_battle_ticket_failed",
                match_id=m.match_id,
                player_id=caller_id,
                err=str(exc),
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle ticket resign unavailable for match %d; retry",
                m.match_id,
            ) from exc
        prog.battle_ticket = token

    # ── RPC 6:ResolvePlayerMatchContext ───────────────────────────────────

    async def resolve_player_match_context(
        self, player_id: int
    ) -> matchpb.ResolvePlayerMatchContextResponse:
        """内部恢复读:只读 canonical 的 start-operation / claim / ticket / match 图。

        queue 与 active ZSET **刻意排除**:它们是派生的、game-mode 本地的索引;
        排除它们让 PVP 实例也能解析出一场 PVE match(两个模式共享 canonical 记录)。

        ★ 任何一条边断了都是 UNKNOWN,**绝不是 NONE**。本方法只读:恢复读不能推进、
        不能补偿、不能删除,也不能从 Redis TTL 推断出一个业务终态。
        """
        unknown = matchpb.ResolvePlayerMatchContextResponse(
            state=matchpb.PLAYER_MATCH_CONTEXT_STATE_UNSPECIFIED,
            stage=matchpb.PLAYER_MATCH_RESUME_STAGE_UNSPECIFIED,
        )
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")

        try:
            start_ticket_id, start_found = await self.repo.get_start_player_operation(player_id)
            claim_ticket_id, claim_found = await self.repo.get_player_ticket(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "read match player index: %s", exc
            ) from exc

        if start_found:
            op, found = await self.repo.get_start_operation(start_ticket_id)
            if (
                not found
                or op.ticket_id != start_ticket_id
                or h.member_index(op.members, player_id) < 0
                or h.start_operation_terminal(op.phase)
                or (claim_found and claim_ticket_id != start_ticket_id)
            ):
                return unknown
            # 取消已提交但清理仍可重放。不报 STARTING(那会在 Cancel 成功后把转圈
            # 重新召唤回来),也不报 NONE(compare-delete 清理还没真正完成)。
            if op.phase == matchpb.MATCH_START_PHASE_COMPENSATING:
                return unknown
            return matchpb.ResolvePlayerMatchContextResponse(
                state=matchpb.PLAYER_MATCH_CONTEXT_STATE_ACTIVE,
                stage=matchpb.PLAYER_MATCH_RESUME_STAGE_STARTING,
                ticket_id=start_ticket_id,
                game_mode=op.game_mode,
                map_id=op.map_id,
            )

        if not claim_found:
            return matchpb.ResolvePlayerMatchContextResponse(
                state=matchpb.PLAYER_MATCH_CONTEXT_STATE_NONE,
                stage=matchpb.PLAYER_MATCH_RESUME_STAGE_UNSPECIFIED,
            )

        ticket, found = await self.repo.get_ticket(claim_ticket_id)
        if (
            not found
            or ticket.ticket_id != claim_ticket_id
            or h.member_index(ticket.members, player_id) < 0
        ):
            return unknown
        base = matchpb.ResolvePlayerMatchContextResponse(
            state=matchpb.PLAYER_MATCH_CONTEXT_STATE_ACTIVE,
            ticket_id=claim_ticket_id,
            game_mode=ticket.game_mode,
            map_id=ticket.map_id,
        )
        if ticket.match_id == 0:
            base.stage = matchpb.PLAYER_MATCH_RESUME_STAGE_QUEUED
            return base

        m, found = await self.repo.get_match(ticket.match_id)
        if (
            not found
            or m.match_id != ticket.match_id
            or h.member_index(m.members, player_id) < 0
            or claim_ticket_id not in list(m.ticket_ids)
        ):
            return unknown
        if ticket.game_mode and m.game_mode and ticket.game_mode != m.game_mode:
            return unknown
        if m.game_mode:
            base.game_mode = m.game_mode
        if m.map_id != 0:
            # match 记录继承自票据;两者都有时以 match 为准(0=未指定,保留票据值)。
            base.map_id = m.map_id
        base.match_id = m.match_id

        if m.stage in (h.STAGE_FOUND, h.STAGE_CONFIRM):
            base.stage = matchpb.PLAYER_MATCH_RESUME_STAGE_CONFIRMING
            return base
        if m.stage == h.STAGE_ALLOCATING:
            base.stage = matchpb.PLAYER_MATCH_RESUME_STAGE_ALLOCATING
            return base
        if m.stage == h.STAGE_READY:
            allocation, ok = h.allocation_from_match(m)
            if (
                not ok
                or not allocation.address
                or allocation.address != m.battle_ds_addr
                or self.allocator is None
            ):
                return unknown
            # 冷启动 / 换设备恢复不能回退到 login 的 roster projection 重新拼票。
            # READY match 中持久化的 exact target 才是唯一可重签输入;
            # 签名失败时整条路由保持 UNKNOWN,**绝不返回半票**。
            try:
                battle_ticket = await self.allocator.sign_battle_ticket(
                    player_id, m.match_id, allocation
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrUnavailable, "re-sign canonical READY battle ticket: %s", exc
                ) from exc
            if not battle_ticket:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable, "re-sign canonical READY battle ticket: empty"
                )
            base.stage = matchpb.PLAYER_MATCH_RESUME_STAGE_READY
            base.battle_ds_addr = allocation.address
            base.battle_ticket = battle_ticket
            return base
        return unknown


def _cancelable_uncheckpointed(m: matchpb.MatchStorageRecord) -> bool:
    """ALLOCATING 期是否还允许玩家取消:target 未写且 phase 仍在 PENDING/REQUESTING。

    并发安全:本判定在 update_match_with_lock 的 CAS 内,冲突会用新快照重跑;
    与分配 worker 的 checkpoint 互斥 —— 后者写 battle_target 前要求记录仍是精确的
    REQUESTING 代际,本处一旦把 stage/phase 翻成 FAILED 它必然 CAS 失败;
    反之若它先赢,本处重跑会看到 battle_target 已存在而拒绝取消。只有一个能成功。
    """
    if m.HasField("battle_target"):
        return False
    return m.allocation_phase in (
        matchpb.MATCH_ALLOCATION_PHASE_UNSPECIFIED,
        matchpb.MATCH_ALLOCATION_PHASE_PENDING,
        matchpb.MATCH_ALLOCATION_PHASE_REQUESTING,
    )


def _collect_team_rosters(out: dict[int, dict], members) -> None:  # noqa: ANN001
    """把成员按 team_id 分组累加(去重)。team_id==0 的成员是单人入口,没有队伍可复位。

    ready_generation 取自本队任一成员(同队必然同值,都来自同一次 BeginTeamMatch)。
    0 = 旧记录 / 旧 team 服务没回代际,EndTeamMatch 退化为「只在还挂着 ready 时复位」。
    """
    for m in members:
        team_id, pid = m.team_id, m.player_id
        if team_id == 0 or pid == 0:
            continue
        roster = out.setdefault(team_id, {"players": [], "ready_generation": 0})
        if roster["ready_generation"] == 0:
            roster["ready_generation"] = m.team_ready_generation
        if pid not in roster["players"]:
            roster["players"].append(pid)


def _new_operation_id() -> str:
    from pandorapy.services.matchmaker.clients import new_operation_id

    return new_operation_id()
