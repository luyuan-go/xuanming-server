"""matchmaker 的**后台写者面** —— 对应 Go 侧 internal/biz/match.go 的
RunMatchLoop / matchTickOnce 及其全部子步骤。

★ 这一整面都是**未分区权威**:撮合循环在共享队列上做全局优化,天然是单写者问题
  (docs/design/decision-revisit-matchmaker-single-writer.md)。多副本部署时若每个
  副本都无条件跑,会重复成局 —— 同一玩家进两场 match,违反不变量 §1,而且
  **没有任何下游能发现**(两场都各自 READY、各自拉一台 DS)。
  所以 main.py 里它必须挂在 etcd leader election 之下(§9.21)。

单 tick 的步骤顺序与 Go 逐字相同,顺序本身是契约:
    ① reconcile_start_operations_once   从权威 record 重建 saga due 索引(5s 节流)
    ② advance_start_operations_once     推进到期的 StartMatch saga
    ③ match_once                        撮合 / 直进成局
    ④ reconcile_active_once             从权威 match 重建 active 索引(5s 节流)
    ⑤ advance_allocations_once          推进 ALLOCATING;补推滞留的 READY
    ⑥ expire_once                       确认期超时判失败
    ⑦ liveness / queue_absence sweep    10s 节流的两道离线回收

每一轮都过 safego.run_once:单轮异常只丢本轮,下个 tick 继续 ——
所有状态推进都经 Redis CAS 幂等,重入安全。没有它的话任一 latent 异常会直接
弄死整条循环,本分片撮合彻底停摆而进程照常 Ready(§9.19)。
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

from pandora.match.v1 import match_pb2 as matchpb

from pandorapy import cellroute
from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import safego
from pandorapy.services.matchmaker import helpers as h
from pandorapy.services.matchmaker import region_affinity

START_OPERATION_LEASE_MS = 15_000
CANONICAL_RECONCILE_EVERY_SEC = 5.0
LIVENESS_SWEEP_INTERVAL_SEC = 10.0
STALE_TICKET_WARN_AGE_MS = 10 * 60 * 1000


def _now_ms() -> int:
    return int(time.time() * 1000)


class MatchLoopMixin:
    """后台撮合循环。"""

    # ── 循环骨架 ────────────────────────────────────────────────────────────

    async def run_match_loop(self) -> None:
        """周期跑 match tick,直到被取消。

        取消必须能穿透(CancelledError 向外抛):leader 失主时靠取消停下这条循环,
        吞掉就等于失主之后还在写共享队列 —— 两个 leader 同时成局。
        """
        interval = self.cfg.match_interval_td().total_seconds()
        plog.get().info("match_loop_started", interval_sec=interval)
        try:
            await safego.loop("matchmaker_match_loop", interval, self.match_tick_once)
        except asyncio.CancelledError:
            plog.get().info("match_loop_stopped")
            raise

    async def match_tick_once(self) -> None:
        """单个撮合 tick。每一步独立兜底,一步失败不拖垮同 tick 的其余步骤。"""
        for name, step in (
            ("match_start_reconcile_failed", self.reconcile_start_operations_once),
            ("match_start_batch_failed", self.advance_start_operations_once),
            ("match_once_failed", self.match_once),
            ("match_active_reconcile_failed", self.reconcile_active_once),
            ("match_allocation_batch_failed", self.advance_allocations_once),
            ("expire_once_failed", self.expire_once),
        ):
            try:
                await step()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(name, err=str(exc))

        # 两道离线回收共用 10s 节流。撮合 tick 是秒级、远小于它;
        # locator TTL 30s,10s 一扫意味着死票最多存活 ~40s 就被清,
        # 且批量 presence 查询压力可控。
        now = time.monotonic()
        if now - self._last_liveness_sweep >= LIVENESS_SWEEP_INTERVAL_SEC:
            self._last_liveness_sweep = now
            for name, step in (
                ("liveness_sweep_failed", self.liveness_sweep_once),
                ("queue_absence_sweep_failed", self.queue_absence_sweep_once),
            ):
                try:
                    await step()
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    plog.get().warning(name, err=str(exc))

    # ── ① / ② StartMatch saga ──────────────────────────────────────────────

    async def reconcile_start_operations_once(self) -> None:
        """遍历全 master 的权威 start operation,修复 due 索引(5s 节流)。

        索引是派生的,权威 record 才是真相 —— 一次 ZADD 失败不该让一条已受理的
        StartMatch 永远没人推进。完整遍历按 5s 节流,避免每个撮合 tick 扫全库。
        """
        now = time.monotonic()
        if self._last_start_reconcile and now - self._last_start_reconcile < CANONICAL_RECONCILE_EVERY_SEC:
            return
        self._last_start_reconcile = now
        for ticket_id in await self.repo.scan_start_operation_ids(128):
            op, found = await self.repo.get_start_operation(ticket_id)
            if not found:
                continue
            if op.game_mode and op.game_mode != self.cfg.game_mode:
                # 全库扫描看得见每个模式;只有归属模式才可以重建它的派生索引或
                # 去 claim 玩家 —— 否则 PVP 的 reconciler 会去认领 PVE 的 saga。
                continue
            if h.start_operation_terminal(op.phase):
                await self.repo.remove_start_active(ticket_id)
                continue
            for member in op.members:
                existing, claimed = await self.repo.claim_start_player(
                    member.player_id, ticket_id
                )
                if not claimed and existing != ticket_id:
                    # 保持这条 op 到期可见。advance 会把它写成 COMPENSATING,
                    # 并只 compare-delete 属于本 op 的 claim。
                    plog.get().warning(
                        "match_start_claim_conflict",
                        reason="reconcile_claim_owned_elsewhere",
                        ticket_id=ticket_id,
                        player_id=member.player_id,
                        owner_ticket_id=existing,
                    )
            due_ms = max(op.next_attempt_at_ms, op.lease_deadline_ms)
            await self.repo.ensure_start_active(ticket_id, due_ms)

    async def advance_start_operations_once(self) -> None:
        for ticket_id in await self.repo.range_due_start_operations(_now_ms()):
            op, found = await self.repo.get_start_operation(ticket_id)
            if not found:
                # canonical 明确不存在时,派生索引才可清理。
                await self.repo.remove_start_active(ticket_id)
                continue
            if op.game_mode and op.game_mode != self.cfg.game_mode:
                await self.repo.remove_start_active(ticket_id)
                continue
            if h.start_operation_terminal(op.phase):
                await self.repo.remove_start_active(ticket_id)
                continue
            try:
                await self.advance_start_operation(op)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "match_start_advance_failed", ticket_id=ticket_id, err=str(exc)
                )

    async def advance_start_operation(
        self, current: matchpb.MatchStartOperationStorageRecord
    ) -> None:
        """推进一条 StartMatch saga。

        所有外部写都可幂等重放;lease 只防 leader 交接窗口内并行推进,
        lease 丢失时旧 worker 不能再提交 phase(它的写会 CAS 失败)。
        """
        if current is None or h.start_operation_terminal(current.phase):
            return
        self.require_local_game_mode(current.game_mode)

        stamp = _now_ms()
        lease_token = _uuid4()
        holder: dict = {}

        def _acquire(rec: matchpb.MatchStartOperationStorageRecord) -> None:
            if h.start_operation_terminal(rec.phase):
                raise errcode.PandoraError(
                    errcode.ErrInvalidState, "start operation %d terminal", rec.ticket_id
                )
            if rec.next_attempt_at_ms > stamp or (
                rec.lease_token and rec.lease_deadline_ms > stamp
            ):
                raise errcode.PandoraError(
                    errcode.ErrMatchConcurrent,
                    "start operation %d not due or leased",
                    rec.ticket_id,
                )
            rec.attempt += 1
            rec.lease_token = lease_token
            rec.lease_deadline_ms = stamp + START_OPERATION_LEASE_MS
            holder["op"] = h.clone_start_operation(rec)

        try:
            await self.repo.update_start_operation_with_lock(
                current.ticket_id, self.cfg.optimistic_retry, _acquire, self.ticket_ttl_sec()
            )
        except errcode.PandoraError as exc:
            if errcode.as_code(exc) in (errcode.ErrInvalidState, errcode.ErrMatchConcurrent):
                return  # 别人在推 / 已终态,不是错误
            raise
        op = holder.get("op")
        if op is None:
            return

        if op.phase == matchpb.MATCH_START_PHASE_COMPENSATING:
            await self._compensate_start_operation(op, lease_token)
            return

        # 每一相之前都先把 start-player 索引占稳:这是「一人一票」在 saga 期的
        # 可发现性边。被别人占走 = 冲突,整条 saga 转补偿(不是硬失败:玩家可能
        # 只是双击了两次开始匹配)。
        for member in op.members:
            try:
                existing, claimed = await self.repo.claim_start_player(
                    member.player_id, op.ticket_id
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                await self._defer_start_operation(op, lease_token, exc)
                return
            if not claimed and existing != op.ticket_id:
                op = await self._set_start_phase(
                    op, lease_token, matchpb.MATCH_START_PHASE_COMPENSATING
                )
                await self._compensate_start_operation(op, lease_token)
                return

        ticket = h.ticket_from_start_operation(op)

        if op.phase == matchpb.MATCH_START_PHASE_ACCEPTED:
            try:
                await self.repo.create_ticket_record(ticket)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                await self._defer_start_operation(op, lease_token, exc)
                return
            op = await self._set_start_phase(
                op, lease_token, matchpb.MATCH_START_PHASE_TICKET_READY
            )

        if op.phase == matchpb.MATCH_START_PHASE_TICKET_READY:
            op = await self._set_start_phase(
                op, lease_token, matchpb.MATCH_START_PHASE_CLAIMING
            )

        if op.phase == matchpb.MATCH_START_PHASE_CLAIMING:
            for member in op.members:
                try:
                    claimed = await self._claim_player_for_start(
                        member.player_id, op.ticket_id
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    await self._defer_start_operation(op, lease_token, exc)
                    return
                if not claimed:
                    op = await self._set_start_phase(
                        op, lease_token, matchpb.MATCH_START_PHASE_COMPENSATING
                    )
                    await self._compensate_start_operation(op, lease_token)
                    return
                op = await self._record_claimed_player(op, lease_token, member.player_id)
            op = await self._set_start_phase(
                op, lease_token, matchpb.MATCH_START_PHASE_CLAIMS_READY
            )

        if op.phase == matchpb.MATCH_START_PHASE_CLAIMS_READY:
            try:
                await self.repo.enqueue_ticket(ticket)
                for pid in h.member_player_ids(op.members):
                    await self.repo.delete_start_player_if_matches(pid, op.ticket_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                await self._defer_start_operation(op, lease_token, exc)
                return
            op = await self._set_start_phase(
                op, lease_token, matchpb.MATCH_START_PHASE_QUEUED, clear_lease=True
            )
            await self.push_progress(
                op.ticket_id, h.STAGE_QUEUEING, list(op.members), "", op.map_id
            )
            # QUEUED 是一次**显式的归属交接**:durable 票据 + player claim 从此是权威。
            # 直接删除 start operation,而不是等一个缓存 TTL 去暗示"完成了"。
            await self.repo.delete_start_operation(op.ticket_id)
            plog.get().debug(
                "match_start_queued", ticket_id=op.ticket_id, operation_id=op.operation_id
            )

    async def _claim_player_for_start(self, player_id: int, ticket_id: int) -> bool:
        """durable saga 版的 claim。

        崩溃若发生在 SETNX 成功、phase 持久化之前,重放会看到 existing==ticket_id,
        并把它识别为**本操作已完成**,而不是冲突。
        """
        for _attempt in range(2):
            existing, claimed = await self.repo.claim_player(player_id, ticket_id)
            if claimed or existing == ticket_id:
                return True
            _, found = await self.repo.get_ticket(existing)
            if found:
                return False  # 占用者是活票据 —— 真冲突
            # 占用者指向一张已不存在的票据 = 僵尸,CAS 清掉后再抢一次。
            await self.repo.delete_player_index_if_matches(player_id, existing)
        return False

    async def _set_start_phase(
        self,
        op: matchpb.MatchStartOperationStorageRecord,
        lease_token: str,
        phase: int,
        *,
        clear_lease: bool = False,
    ) -> matchpb.MatchStartOperationStorageRecord:
        holder: dict = {}

        def _mutate(rec: matchpb.MatchStartOperationStorageRecord) -> None:
            if rec.lease_token != lease_token:
                raise errcode.PandoraError(
                    errcode.ErrMatchConcurrent,
                    "start operation %d lease changed",
                    op.ticket_id,
                )
            rec.phase = phase
            if clear_lease:
                rec.next_attempt_at_ms = 0
                rec.lease_token = ""
                rec.lease_deadline_ms = 0
            holder["op"] = h.clone_start_operation(rec)

        await self.repo.update_start_operation_with_lock(
            op.ticket_id, self.cfg.optimistic_retry, _mutate, self.ticket_ttl_sec()
        )
        return holder["op"]

    async def _record_claimed_player(
        self, op: matchpb.MatchStartOperationStorageRecord, lease_token: str, player_id: int
    ) -> matchpb.MatchStartOperationStorageRecord:
        holder: dict = {}

        def _mutate(rec: matchpb.MatchStartOperationStorageRecord) -> None:
            if rec.lease_token != lease_token:
                raise errcode.PandoraError(
                    errcode.ErrMatchConcurrent,
                    "start operation %d lease changed",
                    op.ticket_id,
                )
            if player_id not in list(rec.claimed_player_ids):
                rec.claimed_player_ids.append(player_id)
            holder["op"] = h.clone_start_operation(rec)

        await self.repo.update_start_operation_with_lock(
            op.ticket_id, self.cfg.optimistic_retry, _mutate, self.ticket_ttl_sec()
        )
        return holder["op"]

    async def _defer_start_operation(
        self,
        op: matchpb.MatchStartOperationStorageRecord,
        lease_token: str,
        cause: BaseException,
    ) -> None:
        """外部写失败:退避重排,**保留 saga**。

        失败的是一次尝试,不是这条业务意图 —— 直接判死会让玩家的"开始匹配"凭空消失,
        而他手上的 UI 已经转起来了。
        """
        next_ms = _now_ms() + int(h.start_retry_delay_sec(op.attempt) * 1000)
        holder: dict = {}

        def _mutate(rec: matchpb.MatchStartOperationStorageRecord) -> None:
            if rec.lease_token != lease_token:
                raise errcode.PandoraError(
                    errcode.ErrMatchConcurrent,
                    "start operation %d lease changed",
                    op.ticket_id,
                )
            rec.next_attempt_at_ms = next_ms
            rec.lease_token = ""
            rec.lease_deadline_ms = 0
            holder["op"] = h.clone_start_operation(rec)

        try:
            await self.repo.update_start_operation_with_lock(
                op.ticket_id, self.cfg.optimistic_retry, _mutate, self.ticket_ttl_sec()
            )
            await self.repo.ensure_start_active(op.ticket_id, next_ms)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "match_start_defer_failed",
                ticket_id=op.ticket_id,
                cause=str(cause),
                err=str(exc),
            )
            return
        plog.get().warning(
            "match_start_deferred",
            ticket_id=op.ticket_id,
            attempt=op.attempt,
            next_attempt_at_ms=next_ms,
            err=str(cause),
        )

    async def _compensate_start_operation(
        self, op: matchpb.MatchStartOperationStorageRecord, lease_token: str
    ) -> None:
        """回滚一条 saga:compare-delete 只属于本 operation 的 claim 与索引,再删票据。

        全部 compare-delete:无条件删会把同一玩家**新一局**的 claim 也删掉。
        任一步失败就退避重试(不进 FAILED)—— 半个回滚比不回滚更难查。
        """
        problems: list[str] = []
        for pid in h.member_player_ids(op.members):
            for coro, tag in (
                (self.repo.delete_player_index_if_matches(pid, op.ticket_id), "player"),
                (self.repo.delete_start_player_if_matches(pid, op.ticket_id), "start player"),
            ):
                try:
                    await coro
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    problems.append(f"rollback {tag} {pid}: {exc}")
        if not problems:
            try:
                await self.repo.delete_ticket(op.ticket_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                problems.append(f"delete ticket {op.ticket_id}: {exc}")
        if problems:
            await self._defer_start_operation(
                op, lease_token, RuntimeError("; ".join(problems))
            )
            return

        failed = await self._set_start_phase(
            op, lease_token, matchpb.MATCH_START_PHASE_FAILED, clear_lease=True
        )
        await self.push_progress(
            failed.ticket_id, h.STAGE_FAILED, list(failed.members), "", failed.map_id
        )
        await self.repo.remove_start_active(failed.ticket_id)

    # ── ③ 撮合 ─────────────────────────────────────────────────────────────

    async def match_once(self) -> None:
        """扫描一次队列,尽可能多地凑出 match。

        算法:按 avg_mmr 升序取票据,贪心累积进一个组,当组内**总人数**达到
        side_count×team_size 且 MMR 跨度在动态窗口内时,用 largest-first 装箱拆成各方。
        凑局单位是**人数不是票数**,所以「3 人队 + 2 人散排拼成一方」与「5 个单排凑一方」
        天然支持,无需额外的拼队机制。
        """
        ticket_ids = await self.repo.range_queue_tickets()
        if not ticket_ids:
            return

        tickets: list[matchpb.MatchTicketStorageRecord] = []
        for tid in ticket_ids:
            try:
                t, found = await self.repo.get_ticket(tid)
            except asyncio.CancelledError:
                raise
            except BaseException:  # noqa: BLE001 —— 单张票读失败不该拖垮整轮
                continue
            if not found:
                # 票据记录已消失但 queue ZSET 残留(跨 slot 索引漂移的天然兜底):
                # best-effort 补清,避免 queue 无界堆积。失败无妨,下一轮再补。
                try:
                    await self.repo.delete_ticket(tid)
                except asyncio.CancelledError:
                    raise
                except BaseException:  # noqa: BLE001, S110
                    pass
                continue
            if t.match_id != 0:
                continue  # 已被预留进某局,不参与本轮
            tickets.append(t)
        tickets.sort(key=lambda t: t.avg_mmr)

        # 入口模式分流:逐票按**票据上落定的 entry_mode** 决定直进还是撮合。
        # 为什么读票不读表:关卡表 entry_mode=BOTH 的图两个入口共存,
        # "这张票是排队还是直进"只有玩家的选择知道,回查表答不了。
        pending: list[matchpb.MatchTicketStorageRecord] = []
        for t in tickets:
            if not self.is_walk_in_ticket(t):
                pending.append(t)
                continue
            try:
                await self.form_solo_match(t)
            except errcode.PandoraError as exc:
                if errcode.as_code(exc) == errcode.ErrRateLimited:
                    # 成局冷却窗内的静默节流:票据留队等下轮,不是异常。
                    plog.get().debug("form_solo_match_throttled", ticket_id=t.ticket_id)
                else:
                    plog.get().warning(
                        "form_solo_match_failed", ticket_id=t.ticket_id, err=str(exc)
                    )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "form_solo_match_failed", ticket_id=t.ticket_id, err=str(exc)
                )
        if not pending:
            return

        stamp = _now_ms()
        # 按 map_id 分组:同一 game_mode 下不同副本各自独立撮合,避免不同副本的玩家
        # 被凑进同一局。分组 key 用 **effective** map_id(0→cfg.map_id)归一化 ——
        # 旧客户端省略 map_id 与新客户端显式发默认 map_id 语义相同,按原始值分组会
        # 拆成两个池永不互相撮合。
        for group in self._partition_tickets_by_map(pending):
            await self._form_matches_in_pool(group, stamp)

    def _partition_tickets_by_map(
        self, tickets: list[matchpb.MatchTicketStorageRecord]
    ) -> list[list[matchpb.MatchTicketStorageRecord]]:
        buckets: dict[int, list] = {}
        for t in tickets:
            buckets.setdefault(self._effective_map_id(t.map_id), []).append(t)
        return [buckets[k] for k in sorted(buckets)]  # 按 map_id 升序保证撮合确定性

    async def _form_matches_in_pool(
        self, tickets: list[matchpb.MatchTicketStorageRecord], now: int
    ) -> None:
        """在「同一副本(map_id)」的票据组内撮合。

        单 Cell / dev(router 未注入)走单桶贪心;多 Region 走两级
        (region 内优先 + 跨 region 溢出兜底)。与 Go 的 `formMatchesInPool` 同构。

        ★ 2026-08-21 补齐:本函数原先**只有**单桶贪心,靠一句
        「cell_route.mode 非空一律拒启」的注释声称不存在"配了却静默按单桶跑"的窗口。
        但同日 `pandorapy.config` 补齐 cellroute 装配后那道拒启闸已改成只校验 mode
        合法性 —— 于是配了多 Region 的部署会**静默退化成单桶**,跨 region 玩家被
        随意凑进同一局(无比例上限)、久等票据也拿不到溢出兜底,且零错误日志。
        """
        pool_map_id = tickets[0].map_id if tickets else 0
        team_size = self.team_size_for_map(pool_map_id)
        side_count = self.side_count_for_map(pool_map_id)
        need = side_count * team_size
        used: set[int] = set()

        # 单 Cell / dev / 阶段 1~2(router 未配)→ 单桶贪心(历史行为,零分区开销)。
        if self.router is None:
            await self._greedy_form_matches(tickets, used, now, team_size, side_count, None)
            return

        # 多 Region(阶段 3)两级撮合(scale-cellular-20m.md §4.4):
        #  ① region 内优先:按 owner region 分桶,各桶内独立贪心(绝大多数对局同 region)。
        #  ② 跨 region 溢出:本 region 凑不齐且等待超阈值的剩余票据,进跨 region 兜底贪心,
        #     且每局受"跨 region 玩家比例软上限"约束(_within_cross_region_cap)。
        buckets, order = region_affinity.partition_tickets_by_region(tickets, self._ticket_region)
        for region in order:
            await self._greedy_form_matches(
                buckets[region], used, now, team_size, side_count, None
            )

        # 收集本 region 内未成局的剩余票据(保持 MMR 升序),挑出可溢出者跨 region 兜底撮合。
        leftover = [t for t in tickets if t.ticket_id not in used]
        # 本地候选是否充足须基于 region 内撮合**后**的 leftover、按 (region, MMR 桶) 细分判定:
        # region 总人数足够但本轮同段位/MMR 窗口剩余不足时,久等票据仍应放开跨 region(§2.2)。
        leftover_totals = region_affinity.leftover_region_bucket_totals(
            leftover, self._ticket_region, self._ticket_mmr_bucket
        )
        overflow = region_affinity.select_overflow_tickets(
            leftover,
            self._ticket_region,
            leftover_totals,
            self._ticket_mmr_bucket,
            need,
            self.region_policy,
            self._ticket_tier,
            now,
        )
        if overflow:
            await self._greedy_form_matches(
                overflow, used, now, team_size, side_count, self._within_cross_region_cap
            )

    # ── 两级撮合的票据属性解析(router 未配时全部退化为单桶口径)──────────────

    def _ticket_region(self, t: matchpb.MatchTicketStorageRecord) -> int:
        """一张票据的 owner region(以队长 captain_id 为 owner 锚点)。

        router 为 None(单 Cell / dev)或 route 报错 → 返回 0(未知 / 单桶),不阻断撮合。
        这里**刻意**不 fail-closed:撮合是可降级的(退化成不分区仍能成局),
        而 §9.22 要求 fail-closed 的是"玩家数据写到哪个 region"那类权威判定。
        """
        if self.router is None or t is None:
            return 0
        try:
            return self.router.route(t.captain_id).region_id
        except cellroute.CellRouteError:
            return 0

    def _ticket_tier(self, t: matchpb.MatchTicketStorageRecord) -> int:
        """票据的段位档(以 avg_mmr 经 region_policy.mmr_tier 计算)。

        高分段档位更高 → 溢出阈值更短(高分段人稀,早点跨 region)。
        """
        if t is None:
            return 0
        return self.region_policy.mmr_tier(t.avg_mmr)

    def _ticket_mmr_bucket(self, t: matchpb.MatchTicketStorageRecord) -> int:
        """票据的 MMR 桶(判 local_enough 的分组口径,§2.3)。

        同 region 内须落同一 MMR 桶才算彼此可成局的本地候选。
        """
        if t is None:
            return 0
        return self.region_policy.mmr_bucket(t.avg_mmr)

    def _within_cross_region_cap(
        self, group: list[matchpb.MatchTicketStorageRecord]
    ) -> bool:
        """跨 region 溢出贪心的成局守卫:一局玩家的 region 分布须满足比例软上限。

        ★ 按**人数**展开(每张票据的 region 重复 len(members) 次)而不是按票数:
        一张 5 人队和一个单排在比例里的权重必须不同,否则"1 张 5 人本区票 + 1 张
        单排外区票"会被算成 50% 跨区而误拒。与 Go 的 `withinCrossRegionCap` 同口径。
        """
        regions: list[int] = []
        for t in group:
            r = self._ticket_region(t)
            regions.extend([r] * len(t.members))
        return self.region_policy.within_cross_region_cap(regions)

    def _battle_placement(
        self, player_ids: list[int]
    ) -> tuple[region_affinity.CellLocation, bool]:
        """battle DS 应落的 (region, cell):参战玩家多数所在落点。

        scale-cellular-20m.md §4.4/§5 —— 让多数玩家就近连入。
        router 为 None(单 Cell / dev)或全部玩家路由失败时返回 ok=False,
        调用方退化为不带放置提示(由 ds_allocator 默认选 Cell)。绝不阻断成局。

        ★ 单个玩家路由失败只跳过该玩家(continue),不是整体失败:多数派判定本就
        容忍缺样本,而让一个查不到的 logical_cell 抹掉整局的就近落点是过度 fail-closed。
        """
        if self.router is None:
            return region_affinity.CellLocation(region_id=0, cell_id=0), False
        locs: list[region_affinity.CellLocation] = []
        for pid in player_ids:
            try:
                loc = self.router.route(pid)
            except cellroute.CellRouteError:
                continue
            locs.append(
                region_affinity.CellLocation(region_id=loc.region_id, cell_id=loc.cell_id)
            )
        return region_affinity.majority_cell_location(locs)

    async def _greedy_form_matches(
        self,
        tickets: list[matchpb.MatchTicketStorageRecord],
        used: set[int],
        now: int,
        team_size: int,
        side_count: int,
        validate: Callable[[list[matchpb.MatchTicketStorageRecord]], bool] | None,
    ) -> None:
        """在给定票据切片(已按 MMR 升序)上做"按 MMR 窗口贪心装箱凑 need"撮合。

        成局即 form_match 并把票据标记进 used。`validate` 非 None 时,装箱成功后还须
        通过该守卫才成局(跨 region 溢出用它做比例上限校验);None 表示无额外约束
        (单桶 / region 内)。对应 Go 的 `greedyFormMatches`。

        need 是一局所需**总人数** = 方数 × 每方人数;凑局按人数累加(见下方 total),
        因此「3 人队 + 2 人散排」与「5 个单排」都能凑满一方,无需额外的拼队机制。
        """
        need = side_count * team_size
        for start in range(len(tickets)):
            if tickets[start].ticket_id in used:
                continue
            group: list[matchpb.MatchTicketStorageRecord] = []
            total = 0
            for j in range(start, len(tickets)):
                if total >= need:
                    break
                t = tickets[j]
                if t.ticket_id in used:
                    continue
                if group and not h.within_window(
                    group[0],
                    t,
                    now,
                    base_window=self.cfg.mmr_base_window,
                    widen_per_sec=self.cfg.mmr_widen_per_sec,
                    max_window=self.cfg.mmr_max_window,
                ):
                    break  # 已按 MMR 排序,后面只会更远
                group.append(t)
                total += len(t.members)
            if total != need:
                continue
            sides, ok = h.bin_pack(group, team_size, side_count)
            if not ok:
                continue
            if validate is not None and not validate(group):
                continue  # 跨 region 比例超上限等约束未过,放弃该组合
            try:
                await self.form_match(sides)
            except errcode.PandoraError as exc:
                if errcode.as_code(exc) == errcode.ErrRateLimited:
                    plog.get().debug("form_match_throttled")
                else:
                    plog.get().warning("form_match_failed", err=str(exc))
                continue
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning("form_match_failed", err=str(exc))
                continue
            used.update(t.ticket_id for t in group)

    async def _try_form_cooldown(self, ticket_id: int) -> None:
        """成局提交前占用票据的成局冷却窗(首次 SETNX 成功即零延迟放行)。

        压制 requeue 风暴:退票重排队后每 match_interval(2s)就重成局一次,
        窗内的**重**成局被压到本节拍。limiter 未注入 / 窗口 <=0 / Redis 故障放行。
        """
        if self.entry_limiter is None:
            return
        window = self.cfg.match_form_cooldown_td().total_seconds()
        if window <= 0:
            return
        ok, err = await self.entry_limiter.try_form_cooldown(ticket_id, window)
        if err is not None:
            plog.get().warning("form_cooldown_check_failed", ticket_id=ticket_id, err=str(err))
            return
        if not ok:
            raise errcode.PandoraError(
                errcode.ErrRateLimited, "ticket %d in form cooldown", ticket_id
            )

    async def _any_ticket_in_form_cooldown(self, sides) -> bool:  # noqa: ANN001
        """撮合组队路径的只读探测:组内任一票据仍在冷却窗内则本轮放弃该组合。

        容量耗尽退票布设的静默窗对撮合路径同样生效,否则同一批票据每 tick 重新
        组局 → 分配 → 再失败,与 solo 是同一个风暴。探测失败 fail-open。
        """
        if self.entry_limiter is None or self.cfg.match_form_cooldown_td().total_seconds() <= 0:
            return False
        for side in sides:
            for t in side:
                inside, err = await self.entry_limiter.in_form_cooldown(t.ticket_id)
                if err is not None:
                    plog.get().warning(
                        "form_cooldown_probe_failed", ticket_id=t.ticket_id, err=str(err)
                    )
                    continue
                if inside:
                    return True
        return False

    async def form_solo_match(self, ticket: matchpb.MatchTicketStorageRecord) -> None:
        """「即时开局 / walk-in」成局路径:单张票据(单人或整队)直接成局。

        跳过撮合与确认,不与陌生人凑对手。这是 PVE 实例「组好队 / 单人直进副本」的
        **生产核心路径**(函数名与 solo_match_found 日志键沿用旧称未改 —— 日志键是
        可观测性契约,被事故档案时间线当证据引用过,正名只落在配置键 walk_in 上)。
        """
        self.require_local_game_mode(ticket.game_mode)
        await self._try_form_cooldown(ticket.ticket_id)
        # 成局前离场复查:solo/walk-in 票同样可能是隔夜幽灵票(单人排队后关客户端,
        # 恰好是 INC-20260814-001 的形状),不复查就直接白拉一台 DS。
        await self.reject_absent_tickets([ticket])

        # 每次成局用**新雪花 match_id**。此前 solo 复用 ticket_id 做 match_id:
        # 退票重排队不换 ticket_id ⇒ 同一 match_id 反复成局,会撞 ds_allocator 侧
        # 保留 2h 的 uncertain/abandoned claim。客户端句柄不变(QUEUEING 仍是 ticket_id)。
        match_id = self.id_gen.generate()
        stamp = _now_ms()
        members = [
            matchpb.MatchMemberStorageRecord(
                player_id=m.player_id,
                team_id=m.team_id,
                mmr=m.mmr,
                hero_id=m.hero_id,
                side=0,
                confirm=h.CONFIRM_ACCEPTED,
                # 代际必须跟着成员走完「票据 → match 记录」这一跳:ReleaseMatch 只从
                # match 成员收 roster,这里掉了它,EndTeamMatch 的跨代 CAS 在主路径上
                # 就恒为退化档 —— 迟到重投会把玩家结算后新点的准备抹掉。
                team_ready_generation=m.team_ready_generation,
            )
            for m in ticket.members
        ]
        match = matchpb.MatchStorageRecord(
            match_id=match_id,
            # 即便是 walk-in 也先落 CONFIRM:ALLOCATING 才是"预留与 claim 都已持久"的提交点。
            stage=h.STAGE_CONFIRM,
            members=members,
            ticket_ids=[ticket.ticket_id],
            created_at_ms=stamp,
            confirm_deadline_ms=stamp
            + int(self.cfg.confirm_timeout_td().total_seconds() * 1000),
            map_id=ticket.map_id,
            game_mode=ticket.game_mode,
        )

        # 一致性顺序(先建 match 再预留):match 先落库并进 active ZSET,
        # 预留后崩溃也能被 expire_once 兼带清理,不留「match_id 指向不存在 match」的孤儿票据。
        await self.repo.create_match(match)
        ticket.match_id = match_id
        try:
            await self.repo.reserve_ticket(ticket)
        except BaseException:
            await self.repo.delete_match(match_id)  # 票据未预留成功,删空 match 即可
            raise
        await self._persist_claims(ticket)
        queued = await self.queue_accepted_match_allocation(match)

        await self._notify_matching(h.member_player_ids(members), match_id)
        plog.get().info(
            "solo_match_found",
            match_id=match_id,
            ticket_id=ticket.ticket_id,
            players=len(members),
            operation_id=queued.allocation_operation_id,
            ticket_age_ms=h.oldest_ticket_age_ms(stamp, [ticket]),
        )

    async def form_match(self, sides) -> None:  # noqa: ANN001
        """把已装箱的各方票据组成一场 match:写 match 记录 + 预留票据 + 推 FOUND/CONFIRM。

        sides 长度 = 关卡表 side_count(PVE 合作 1 方、常规对抗 2 方、混战 N 方);
        成员的 side 即其所在方的下标,DS 侧阵营分配本就支持 >2 方。
        """
        flat: list[matchpb.MatchTicketStorageRecord] = []
        for side in sides:
            for ticket in side:
                self.require_local_game_mode(ticket.game_mode)
                flat.append(ticket)

        if await self._any_ticket_in_form_cooldown(sides):
            raise errcode.PandoraError(errcode.ErrRateLimited, "tickets in form cooldown")
        # 成局前离场复查:扫除是 10s 节流的,旧票可能在两次扫除之间被凑进局。
        # 用**装箱后的最终票据集合**再查一次,含离场超窗成员的票当场回收,
        # 无辜票据留在队列由下轮重新凑局,不整组陪葬。
        await self.reject_absent_tickets(flat)

        match_id = self.id_gen.generate()
        stamp = _now_ms()
        deadline = stamp + int(self.cfg.confirm_timeout_td().total_seconds() * 1000)
        map_id = flat[0].map_id if flat else 0

        # auto_confirm:本局要不要跳过确认期。两个来源,任一成立即跳过 ——
        #   ① 部署级 auto_confirm_match:无 UI 的脚本联调 / 压测机器人专用
        #   ② 本图 ready_mode=PRE_READY:玩家已在组队面板点过准备,不该为同一局
        #      再点第二次(2026-08-18 拍板两模式互斥)
        # 这里读的是与 BeginTeamMatch 门槛**同一个判定**(requires_pre_match_ready),
        # 两处分别判会出现「既要先准备、又要再接受」或「两道都没有」的错配。
        auto_confirm = self.cfg.auto_confirm_match or self.requires_pre_match_ready(map_id)
        initial_confirm = h.CONFIRM_ACCEPTED if auto_confirm else h.CONFIRM_PENDING

        members: list[matchpb.MatchMemberStorageRecord] = []
        ticket_ids: list[int] = []
        for side_idx, side in enumerate(sides):
            for t in side:
                ticket_ids.append(t.ticket_id)
                for m in t.members:
                    members.append(
                        matchpb.MatchMemberStorageRecord(
                            player_id=m.player_id,
                            team_id=m.team_id,
                            mmr=m.mmr,
                            hero_id=m.hero_id,
                            side=side_idx,
                            confirm=initial_confirm,
                            team_ready_generation=m.team_ready_generation,
                        )
                    )

        match = matchpb.MatchStorageRecord(
            match_id=match_id,
            stage=h.STAGE_CONFIRM,
            members=members,
            ticket_ids=ticket_ids,
            created_at_ms=stamp,
            confirm_deadline_ms=deadline,
            map_id=map_id,
            game_mode=self.cfg.game_mode,
        )

        # 一致性流程(先建 match,再预留票据):
        #   1. CreateMatch(含写 active ZSET)。失败则票据未动、全在队列,下轮重试。
        #   2. 逐张预留(移出队列 + 写 match_id + 固化 claim),防下一轮重复撮合。
        #   3. 任一预留失败 → 先把已预留票据退回队列,**再**删 match(顺序不可倒:
        #      先删 match 会让并发的孤儿清理路径误删即将退回的票据)。
        # 为什么先建 match:若先预留后建 match,两步之间崩溃会留下「match_id 指向不存在
        # match」的孤儿票据 —— 不在队列、不在 active ZSET,谁都看不见,claim 卡死。
        await self.repo.create_match(match)
        reserved: list[matchpb.MatchTicketStorageRecord] = []
        persist_problems: list[str] = []
        for side in sides:
            for t in side:
                t.match_id = match_id
                try:
                    await self.repo.reserve_ticket(t)
                except BaseException as exc:
                    await self._rollback_reservations(reserved)
                    await self.repo.delete_match(match_id)
                    plog.get().error(
                        "reserve_ticket_failed",
                        match_id=match_id,
                        ticket_id=t.ticket_id,
                        err=str(exc),
                    )
                    raise
                reserved.append(t)
                try:
                    await self._persist_claims(t)
                except errcode.PandoraError as exc:
                    # 继续预留完整的权威票据集合。中途 return 会让后面的票据仍留在
                    # 队列里而 match 已经引用了它们 —— 那种图 reconciler 猜不出来。
                    persist_problems.append(str(exc))
        if persist_problems:
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "persist match %d claims before FOUND: %s",
                match_id,
                "; ".join(persist_problems),
            )

        queued = None
        if auto_confirm:
            queued = await self.queue_accepted_match_allocation(match)

        await self._notify_matching(h.member_player_ids(members), match_id)
        await self.push_progress(match_id, h.STAGE_FOUND, members, "", map_id)
        await self.push_progress(match_id, h.STAGE_CONFIRM, members, "", map_id)

        oldest_age = h.oldest_ticket_age_ms(stamp, flat)
        plog.get().info(
            "match_found",
            match_id=match_id,
            players=len(members),
            # auto_confirm 是本局的**最终判定**(部署开关 OR 本图 PRE_READY),不是配置原值 ——
            # 排障时要回答的是「这一局到底进没进确认期」,而不是「配置写了什么」。
            auto_confirm=auto_confirm,
            ready_mode=int(self.ready_mode_for_map(map_id)),
            ticket_ids=ticket_ids,
            oldest_ticket_age_ms=oldest_age,
        )
        if oldest_age > STALE_TICKET_WARN_AGE_MS:
            plog.get().warning(
                "stale_ticket_matched",
                match_id=match_id,
                oldest_ticket_age_ms=oldest_age,
                ticket_ids=ticket_ids,
            )
        if auto_confirm and queued is not None:
            plog.get().debug(
                "match_allocation_queued",
                match_id=match_id,
                operation_id=queued.allocation_operation_id,
            )

    async def _rollback_reservations(self, reserved) -> None:  # noqa: ANN001
        """把一批已预留的票据退回队列(清 match_id,保留 enqueued_at_ms)。

        守卫退队:这些票刚被置 match_id=matchID;若并发 CancelMatch 已删其一,
        守卫退队 no-op 不复活(见 repo.requeue_ticket_if_owned)。
        """
        for t in reserved:
            expected = t.match_id
            t.match_id = 0
            try:
                requeued = await self.repo.requeue_ticket_if_owned(t, expected)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "rollback_reservation_failed", ticket_id=t.ticket_id, err=str(exc)
                )
                continue
            if requeued:
                await self._refresh_claims(t)

    async def _persist_claims(self, ticket: matchpb.MatchTicketStorageRecord) -> None:
        problems: list[str] = []
        for m in ticket.members:
            try:
                await self.repo.persist_player_claim(m.player_id, ticket.ticket_id)
            except errcode.PandoraError as exc:
                problems.append(f"persist player {m.player_id} ticket {ticket.ticket_id}: {exc}")
        if problems:
            raise errcode.PandoraError(errcode.ErrUnavailable, "%s", "; ".join(problems))

    async def _refresh_claims(self, ticket: matchpb.MatchTicketStorageRecord) -> None:
        """把滚动升级遗留的 TTL claim 原子升级成 persistent。

        退回队列会让票据继续存活,claim 必须同步固化 —— 否则 claim 先于票据消失,
        玩家可再开新票 → 双票双局(违反不变量 §1)。
        """
        for m in ticket.members:
            try:
                await self.repo.refresh_player_claim(m.player_id, ticket.ticket_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "refresh_claim_failed",
                    player_id=m.player_id,
                    ticket_id=ticket.ticket_id,
                    err=str(exc),
                )

    async def _notify_matching(self, player_ids: list[int], match_id: int) -> None:
        """弱依赖:locator 未注入跳过 / 失败仅 Warn(上报晚一拍不影响撮合正确性)。"""
        if self.locator is None:
            return
        try:
            await self.locator.notify_matching(player_ids, match_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "locator_notify_matching_failed", match_id=match_id, err=str(exc)
            )

    async def queue_accepted_match_allocation(
        self, candidate: matchpb.MatchStorageRecord
    ) -> matchpb.MatchStorageRecord:
        """auto-confirm / walk-in 的**成局提交点**。

        权威 match 先落在 CONFIRM,然后每一张票据预留与每一个 claim 都固化,
        只有这之后才能 CAS 到 ALLOCATING。因此在此点之前的崩溃**不可能**为一场
        只成了一半的对局拉起 Battle DS。
        操作幂等:进程崩溃或 Redis ACK 丢失后,reconciler 能完成完全相同的交接。
        """
        if candidate is None or candidate.match_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "accepted match required")
        if candidate.stage != h.STAGE_CONFIRM or not h.all_accepted(candidate.members):
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "match %d is not a fully accepted formation",
                candidate.match_id,
            )
        # 这一步顺带修复 claim 固化的 ACK 丢失。缺失 / 漂移的票据预留仍可重试,
        # 并 fail-closed 地挡住本次转换。
        await self.ensure_match_discovery(candidate)

        holder: dict = {}

        def _mutate(rec: matchpb.MatchStorageRecord) -> None:
            if rec.stage in (h.STAGE_ALLOCATING, h.STAGE_READY):
                holder["m"] = h.clone_match(rec)  # 上一次尝试已提交但丢了 ACK
                return
            if rec.stage != h.STAGE_CONFIRM:
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "match %d stage=%d cannot enter allocation",
                    rec.match_id,
                    int(rec.stage),
                )
            if not h.all_accepted(rec.members):
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "match %d no longer fully accepted",
                    rec.match_id,
                )
            rec.stage = h.STAGE_ALLOCATING
            if not rec.allocation_operation_id:
                rec.allocation_operation_id = _uuid4()
            rec.allocation_phase = matchpb.MATCH_ALLOCATION_PHASE_PENDING
            rec.allocation_next_attempt_at_ms = _now_ms()
            holder["m"] = h.clone_match(rec)

        await self.repo.update_match_with_lock(
            candidate.match_id, self.cfg.optimistic_retry, _mutate
        )
        queued = holder.get("m")
        if queued is None:
            raise errcode.PandoraError(
                errcode.ErrMatchConcurrent,
                "match %d allocation handoff not committed",
                candidate.match_id,
            )
        return queued

    # ── ④ active 索引重建 ──────────────────────────────────────────────────

    async def reconcile_active_once(self) -> None:
        """从权威 match 记录修复派生的 active ZSET(5s 节流)。

        必须遍历**全部 master**;单节点 SCAN 会永久漏掉其他 slot 上的局 ——
        那些局会卡在 CONFIRM/ALLOCATING 没有任何人推进,而且没有任何日志。
        """
        now = time.monotonic()
        if self._last_match_reconcile and now - self._last_match_reconcile < CANONICAL_RECONCILE_EVERY_SEC:
            return
        self._last_match_reconcile = now
        for mid in await self.repo.scan_match_ids(128):
            m, found = await self.repo.get_match(mid)
            if not found:
                continue
            if m.game_mode and m.game_mode != self.cfg.game_mode:
                # Scan 是全局的而 active 是模式本地的。绝不让 PVP 的 reconciler
                # 认领 PVE 的分配 job(反之亦然)。
                continue
            discovery_checked = False
            if m.stage == h.STAGE_CONFIRM and h.all_accepted(m.members):
                # auto-confirm / walk-in 成局刻意先落成"全员已接受的 CONFIRM"。
                # 若创建者在预留完整图之后、ALLOCATING CAS 之前死掉,由本扫描完成交接。
                discovery_checked = True
                try:
                    m = await self.queue_accepted_match_allocation(m)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    plog.get().warning(
                        "match_allocation_handoff_retry", match_id=mid, err=str(exc)
                    )
            if not discovery_checked and m.stage in (
                h.STAGE_CONFIRM,
                h.STAGE_ALLOCATING,
                h.STAGE_READY,
            ):
                try:
                    await self.ensure_match_discovery(m)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    plog.get().warning(
                        "match_discovery_incomplete", match_id=mid, err=str(exc)
                    )

            # 索引修复**不受**发现图健康度控制。尤其是:CreateMatch 之后、全部预留
            # 之前崩溃的局必须重新拿到 active 表项,expire_once 才能把这个半成品
            # 判失败并退票。
            if m.stage in (h.STAGE_CONFIRM, h.STAGE_ALLOCATING):
                await self.repo.ensure_active(mid, m.confirm_deadline_ms)
            elif m.stage == h.STAGE_FAILED:
                if m.allocation_next_attempt_at_ms == -1:
                    await self.repo.remove_active(mid)
                else:
                    await self.repo.ensure_active(mid, m.confirm_deadline_ms)
            # READY 的 active 表项语义是「推送交付未确认」,由 finalize_ready_match
            # 补推后移出。扫描无法区分「已交付」与「未交付」,既不清除(会拆掉补推
            # 驱动)也不补建(会对已交付的局每 5s 重复推送一整场)。

    async def ensure_match_discovery(self, m: matchpb.MatchStorageRecord) -> None:
        """校验并固化「match ↔ 票据 ↔ claim」这张发现图,**任何缺口都 fail-closed**。

        为什么要在拉 DS 前做这件事:一个遗留 / 在途的创建者可能在全部票据预留或
        claim 固化之前就写了 ALLOCATING。子集 / 超集 / 重复的图都是 UNKNOWN,
        绝不能送到 AllocateBattle —— 那会开出一场名单对不上的对局。
        """
        if m is None or m.match_id == 0 or not m.members or not m.ticket_ids:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "incomplete canonical match discovery graph"
            )
        expected: set[int] = set()
        for member in m.members:
            if member.player_id == 0:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "match %d has zero player in canonical roster",
                    m.match_id,
                )
            if member.player_id in expected:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "match %d canonical roster duplicates player %d",
                    m.match_id,
                    member.player_id,
                )
            expected.add(member.player_id)

        problems: list[str] = []
        tickets: list[matchpb.MatchTicketStorageRecord] = []
        seen_tickets: set[int] = set()
        for ticket_id in m.ticket_ids:
            if ticket_id == 0:
                problems.append(f"match {m.match_id} has zero ticket id")
                continue
            if ticket_id in seen_tickets:
                problems.append(f"match {m.match_id} duplicates ticket {ticket_id}")
                continue
            seen_tickets.add(ticket_id)
            ticket, found = await self.repo.get_ticket(ticket_id)
            if not found or ticket.match_id != m.match_id:
                problems.append(
                    f"match {m.match_id} ticket {ticket_id} discovery edge missing/drifted"
                )
                continue
            tickets.append(ticket)
        if problems:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "%s", "; ".join(problems)
            )

        # 票据并集必须**精确等于**权威名单,才允许创建或固化任何 claim。
        seen_players: set[int] = set()
        for ticket in tickets:
            if not ticket.members:
                problems.append(
                    f"match {m.match_id} ticket {ticket.ticket_id} has empty roster"
                )
                continue
            for member in ticket.members:
                pid = member.player_id
                if pid not in expected:
                    problems.append(
                        f"match {m.match_id} ticket {ticket.ticket_id} contains unexpected player {pid}"
                    )
                    continue
                if pid in seen_players:
                    problems.append(
                        f"match {m.match_id} ticket graph duplicates player {pid}"
                    )
                    continue
                seen_players.add(pid)
        for pid in expected:
            if pid not in seen_players:
                problems.append(f"match {m.match_id} ticket graph omits player {pid}")
        if problems:
            raise errcode.PandoraError(errcode.ErrUnavailable, "%s", "; ".join(problems))

        for ticket in tickets:
            for member in ticket.members:
                existing, claimed = await self.repo.claim_player(
                    member.player_id, ticket.ticket_id
                )
                if not claimed and existing != ticket.ticket_id:
                    problems.append(
                        f"match {m.match_id} player {member.player_id} claim owned by ticket {existing}"
                    )
                    continue
                try:
                    await self.repo.persist_player_claim(member.player_id, ticket.ticket_id)
                except errcode.PandoraError as exc:
                    problems.append(str(exc))
        if problems:
            raise errcode.PandoraError(errcode.ErrUnavailable, "%s", "; ".join(problems))

    # ── ⑤ 分配推进 ─────────────────────────────────────────────────────────

    async def advance_allocations_once(self) -> None:
        """推进 active 里所有到期的 durable allocation job,并补推滞留的 READY。"""
        jobs: list[matchpb.MatchStorageRecord] = []
        for mid in await self.repo.range_active_matches():
            m, found = await self.repo.get_match(mid)
            if not found:
                # canonical 明确不存在时,派生索引才可清理。
                await self.repo.remove_active(mid)
                continue
            if m.game_mode and m.game_mode != self.cfg.game_mode:
                await self.repo.remove_active(mid)
                continue
            if m.stage == h.STAGE_ALLOCATING:
                # 攒批,listing 结束后有界并发推进:分配含最长 ~60s 的 RPC,
                # 不得在 listing 循环内串行内联(READY 补推会被头阻塞)。
                jobs.append(m)
            elif m.stage == h.STAGE_FAILED:
                if m.allocation_next_attempt_at_ms == -1:
                    await self.repo.remove_active(mid)
                else:
                    try:
                        await self.fail_match(m, _failed_match_classifier(m))
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:  # noqa: BLE001
                        plog.get().warning(
                            "match_failed_cleanup_retry", match_id=mid, err=str(exc)
                        )
            elif m.stage == h.STAGE_READY:
                # READY 仍在 active = 推送交付未确认(崩溃窗口 / Kafka 中断)。
                # 幂等补推,全员成功才移出 active;失败保留下轮重试。
                try:
                    await self.finalize_ready_match(m)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    plog.get().warning(
                        "match_ready_finalize_deferred", match_id=mid, err=str(exc)
                    )
        if jobs:
            await self._advance_allocations_parallel(jobs)

    async def _advance_allocations_parallel(self, jobs) -> None:  # noqa: ANN001
        """有界并发推进一批 ALLOCATING match,**返回前 join 全部完成**。

        join 保持「返回即已尝试完毕」的同步契约:同一 match 绝无跨 tick 并发尝试
        (分配重试退避 2~8s 短于最坏 RPC,不 join 会叠加尝试)。
        tick 分配耗时从 Σ(每局时延) 降为 max(单局时延)。
        """

        async def _one(m: matchpb.MatchStorageRecord) -> None:
            async with self._alloc_sem:
                try:
                    await self.advance_allocation(m)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    plog.get().warning(
                        "match_allocation_failed", match_id=m.match_id, err=str(exc)
                    )

        await asyncio.gather(*(_one(m) for m in jobs))

    async def advance_allocation(self, m: matchpb.MatchStorageRecord) -> None:
        """推进一场 ALLOCATING match 到 READY。

        ★ 结果未知一律保持 ALLOCATING:传输错误 / Redis 错误 / allocation_uncertain
        都**不能**被推断成失败并退票 —— 那样会在一台可能已经起来的 DS 之外再开一台。
        只有分配器明确证明「没有产生可用 DS」(ErrDSNoAvailable / ErrDSAllocationFailed)
        才 CAS FAILED 并走退票补偿。
        """
        if m is None or m.stage != h.STAGE_ALLOCATING:
            return
        stamp = _now_ms()
        if m.allocation_next_attempt_at_ms > stamp:
            return

        holder: dict = {}

        def _acquire(rec: matchpb.MatchStorageRecord) -> None:
            if rec.stage != h.STAGE_ALLOCATING:
                raise errcode.PandoraError(
                    errcode.ErrInvalidState, "match %d no longer allocating", rec.match_id
                )
            if rec.allocation_next_attempt_at_ms > stamp:
                raise errcode.PandoraError(
                    errcode.ErrMatchConcurrent, "match %d allocation not due", rec.match_id
                )
            if not rec.allocation_operation_id:
                rec.allocation_operation_id = _uuid4()
            rec.allocation_attempt += 1
            rec.allocation_phase = matchpb.MATCH_ALLOCATION_PHASE_REQUESTING
            rec.allocation_next_attempt_at_ms = stamp + int(
                h.allocation_retry_delay_sec(rec.allocation_attempt) * 1000
            )
            holder["m"] = h.clone_match(rec)

        try:
            await self.repo.update_match_with_lock(
                m.match_id, self.cfg.optimistic_retry, _acquire
            )
        except errcode.PandoraError as exc:
            if errcode.as_code(exc) in (errcode.ErrInvalidState, errcode.ErrMatchConcurrent):
                return
            raise
        job = holder.get("m")
        if job is None:
            return

        player_ids = h.member_player_ids(job.members)
        # 遗留 / 在途的创建者可能在全部预留或 claim 固化之前写了 ALLOCATING。
        # 在精确的发现图完整之前绝不创建外部 DS。UNKNOWN 仍是同一个可重试的
        # ALLOCATING 操作,绝不解释成"不存在"。
        await self.ensure_match_discovery(job)

        # 成局最终门(默认关闭)。开关关闭 / locator 未配 / 查询失败 → 跳过。
        # ❗ 这道门的证据来源对「已成局 match 的成员」结构性失效(INC-20260724-001),
        # 全部实跑配置已回退为 false;重开前必须先修证据链。
        if not job.HasField("battle_target"):
            offline = await self.find_offline_members(player_ids)
            if offline:
                plog.get().warning(
                    "match_liveness_failed", match_id=job.match_id, offline_players=offline
                )
                failed = await self._cas_allocation_failed(job, offline)
                if failed is not None:
                    await self.fail_match(failed, _failed_match_classifier(failed))
                return

        # 两级撮合放置(scale-cellular-20m.md §4.4):算出"参战玩家多数所在 region/cell",
        # 让 battle DS 就近落到该 Cell。当前先作为放置提示落日志(多 region RTT 排障 / 观测);
        # 把它透传进 AllocateBattleRequest(region_id/cell_id)由 ds_allocator 按 Cell 选 k8s,
        # 属 proto + 跨服务改动,与 Go 侧同样留待跟进(见 PROGRESS 落地记录)。
        # router 为 None(单 Cell / dev)时 ok=False,不打印、行为不变。
        place, place_ok = self._battle_placement(player_ids)
        if place_ok:
            plog.get().debug(
                "battle_placement",
                match_id=job.match_id,
                region_id=place.region_id,
                cell_id=place.cell_id,
                players=len(player_ids),
            )

        allocation, checkpointed = h.allocation_from_match(job)
        if job.HasField("battle_target") and not checkpointed:
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "match %d has incomplete durable battle target",
                job.match_id,
            )
        if not checkpointed:
            try:
                factions = h.combat_factions_from_members(job.members)
            except ValueError as exc:
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "match %d combat factions invalid: %s",
                    job.match_id,
                    exc,
                ) from exc
            try:
                allocation = await self.allocator.allocate_battle_with_combat_factions(
                    job.match_id, player_ids, factions, job.map_id
                )
            except errcode.PandoraError as exc:
                plog.get().error(
                    "ds_allocate_failed", match_id=job.match_id, err=str(exc)
                )
                code = errcode.as_code(exc)
                if code not in (errcode.ErrDSAllocationFailed, errcode.ErrDSNoAvailable):
                    raise  # 未知结果,保持 ALLOCATING
                failed = await self._cas_allocation_failed(job, [])
                if failed is not None:
                    # 容量耗尽对玩家不是失败:静默窗 + QUEUEING(带倒计时)退队,
                    # 不推 FAILED(§6 第 3 项)。
                    await self.on_match_no_capacity(failed)
                raise
            if allocation is None or not allocation.address or not allocation.complete_battle():
                raise errcode.PandoraError(
                    errcode.ErrDSAllocationFailed,
                    "allocator returned incomplete battle target for match %d",
                    job.match_id,
                )
            allocation = await self._checkpoint_battle_allocation(job, allocation)

        tickets = await self.allocator.sign_battle_tickets(
            job.match_id, player_ids, allocation
        )
        try:
            h.validate_signed_battle_tickets(player_ids, tickets)
        except ValueError as exc:
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed, "match %d: %s", job.match_id, exc
            ) from exc
        ds_addr = allocation.address

        # BATTLE 投影必须**先于** READY 提交写入(强依赖):否则 READY 推送已发、
        # 玩家已向 battle 迁移,而 locator 无 BATTLE 记录 —— 这个窗口内断线重登会被
        # 误路由回 Hub(双在场)。失败 → 抛错,allocation 已 checkpoint,下轮幂等重试。
        await self._notify_battle_strict(player_ids, job.match_id, ds_addr)

        holder = {}

        def _to_ready(rec: matchpb.MatchStorageRecord) -> None:
            # stage 守卫:仅 ALLOCATING + 同一 operation + 同一 target 才可推进到 READY。
            # 盲写会把已被 expire_once 判 FAILED(票据已退回队列)的局翻成 READY,
            # 造成"票在队列里但人被拉进战斗"的脏状态。
            if (
                rec.stage != h.STAGE_ALLOCATING
                or rec.allocation_phase != matchpb.MATCH_ALLOCATION_PHASE_REQUESTING
                or rec.allocation_operation_id != job.allocation_operation_id
            ):
                raise errcode.PandoraError(
                    errcode.ErrMatchConcurrent,
                    "match %d allocation checkpoint changed before READY",
                    job.match_id,
                )
            rec.stage = h.STAGE_READY
            rec.battle_ds_addr = ds_addr
            rec.battle_target.CopyFrom(h.battle_target_storage(allocation))
            rec.allocation_phase = matchpb.MATCH_ALLOCATION_PHASE_COMPLETED
            rec.allocation_next_attempt_at_ms = 0
            holder["m"] = h.clone_match(rec)

        await self.repo.update_match_with_lock(
            job.match_id, self.cfg.optimistic_retry, _to_ready
        )
        ready = holder["m"]

        # 投影已在 READY 前强写入;这里再刷一次纯属弱依赖续期,失败仅 Warn。
        if self.locator is not None:
            try:
                await self.locator.notify_battle(player_ids, job.match_id, ds_addr)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "locator_notify_battle_failed", match_id=job.match_id, err=str(exc)
                )

        # 交付是 at-least-once:全员推送成功才把 match 移出 active
        # (不变量:READY ∈ active ⟺ 推送交付未确认)。
        try:
            await self.push_ready_strict(ready, ds_addr, tickets)
        except errcode.PandoraError as exc:
            plog.get().warning(
                "match_ready_push_deferred", match_id=job.match_id, err=str(exc)
            )
        else:
            await self._remove_active(job.match_id)
        plog.get().info(
            "match_ready", match_id=job.match_id, ds_addr=ds_addr, players=len(player_ids)
        )

    async def _cas_allocation_failed(
        self, job: matchpb.MatchStorageRecord, offline: list[int]
    ) -> matchpb.MatchStorageRecord | None:
        """把一次**明确的**分配失败 CAS 成 FAILED。

        守卫绑定精确的 REQUESTING / operation / 未 checkpoint 快照:并发的
        checkpoint 或新一代 generation 由对方继续推进,这里不收尾 ——
        否则会把一台已经拉起来的 DS 丢在那里没人回收。
        """
        holder: dict = {}

        def _mutate(rec: matchpb.MatchStorageRecord) -> None:
            if (
                rec.stage != h.STAGE_ALLOCATING
                or rec.allocation_phase != matchpb.MATCH_ALLOCATION_PHASE_REQUESTING
                or rec.allocation_operation_id != job.allocation_operation_id
                or rec.HasField("battle_target")
            ):
                raise errcode.PandoraError(
                    errcode.ErrMatchConcurrent,
                    "match %d allocation generation changed before definitive failure",
                    job.match_id,
                )
            rec.stage = h.STAGE_FAILED
            rec.allocation_phase = matchpb.MATCH_ALLOCATION_PHASE_FAILED
            for member in rec.members:
                if member.player_id in offline:
                    member.confirm = h.CONFIRM_REJECTED
            holder["m"] = h.clone_match(rec)

        try:
            await self.repo.update_match_with_lock(
                job.match_id, self.cfg.optimistic_retry, _mutate
            )
        except errcode.PandoraError as exc:
            plog.get().warning(
                "match_allocation_fail_cas_skipped", match_id=job.match_id, err=str(exc)
            )
            return None
        return holder.get("m")

    async def _checkpoint_battle_allocation(
        self, job: matchpb.MatchStorageRecord, allocation
    ):  # noqa: ANN001
        """把分配结果固化到权威 match 上。

        一旦精确 target 被 checkpoint,之后**每一次**尝试都必须复用它;
        再调一次分配器并接受一个不同的 target,会让玩家被丢在一台更早的、
        已经部分发布出去的 DS 上。
        """

        def _mutate(rec: matchpb.MatchStorageRecord) -> None:
            if (
                rec.stage != h.STAGE_ALLOCATING
                or rec.allocation_operation_id != job.allocation_operation_id
            ):
                raise errcode.PandoraError(
                    errcode.ErrMatchConcurrent,
                    "match %d allocation generation changed before checkpoint",
                    job.match_id,
                )
            if rec.HasField("battle_target") and rec.battle_target.allocation_id:
                return  # 别人已经 checkpoint 过同一代,复用它
            rec.battle_target.CopyFrom(h.battle_target_storage(allocation))

        await self.repo.update_match_with_lock(
            job.match_id, self.cfg.optimistic_retry, _mutate
        )
        current, _ = await self.repo.get_match(job.match_id)
        stored, ok = h.allocation_from_match(current) if current is not None else (None, False)
        return stored if ok else allocation

    async def _notify_battle_strict(
        self, player_ids: list[int], match_id: int, ds_addr: str
    ) -> None:
        if self.locator is None:
            return
        try:
            await self.locator.notify_battle(player_ids, match_id, ds_addr)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().error(
                "locator_notify_battle_failed_pre_ready", match_id=match_id, err=str(exc)
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle location projection must commit before READY for match %d",
                match_id,
            ) from exc

    async def finalize_ready_match(self, m: matchpb.MatchStorageRecord) -> None:
        """补推一场 READY 后仍滞留 active 的 match,交付确认后移出 active。

        每次补推为全员**重签**票据(新 jti),与 GetMatchProgress 同口径 ——
        复用旧票会撞 DS 侧 jti 一次性防重放。推送成功前绝不 remove_active。
        """
        if self.pusher is None:
            await self.repo.remove_active(m.match_id)
            return
        allocation, ok = h.allocation_from_match(m)
        if not ok or not m.battle_ds_addr:
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "match %d READY without complete persisted battle target",
                m.match_id,
            )
        player_ids = h.member_player_ids(m.members)
        tickets = await self.allocator.sign_battle_tickets(m.match_id, player_ids, allocation)
        try:
            h.validate_signed_battle_tickets(player_ids, tickets)
        except ValueError as exc:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "match %d: %s", m.match_id, exc
            ) from exc
        await self.push_ready_strict(m, m.battle_ds_addr, tickets)
        await self.repo.remove_active(m.match_id)

    # ── 失败收尾 ────────────────────────────────────────────────────────────

    async def on_match_failed(self, m: matchpb.MatchStorageRecord, rejecter_id: int) -> None:
        """确认失败:无过错票据退回队列,过错票据删除并释放归属,推 FAILED。

        定责规则:
          - 显式拒绝(rejecter_id!=0):仅拒绝者所在票据过错,其余(含尚未点确认的)退队
          - 超时(rejecter_id==0):含未确认(AFK)成员的票据过错 —— 否则低在线时段
            同一批人 + 同一个挂机者会立刻重新凑成同一场 → 15s 超时 → 再凑,无限循环,
            其余人被永远劫持在「FOUND→超时」里(典型"匹配不了")
        """
        confirm_of = {mem.player_id: mem.confirm for mem in m.members}

        def _is_faulty(_tid: int, ticket: matchpb.MatchTicketStorageRecord) -> bool:
            if rejecter_id != 0:
                return h.member_index(ticket.members, rejecter_id) >= 0
            return not h.ticket_all_accepted(ticket, confirm_of)

        await self.fail_match(m, _is_faulty)
        plog.get().info("match_failed", match_id=m.match_id, rejecter_id=rejecter_id)

    async def on_match_no_capacity(self, m: matchpb.MatchStorageRecord) -> None:
        """容量耗尽的失败收尾。

        与 on_match_failed 的区别是**玩家视角不是失败**:票据自动退回队列、撮合循环
        稍后自动重试,客户端只看到带 estimated_wait_seconds 倒计时的 QUEUEING,
        不闪 FAILED。同时给每张票布设静默窗,免得撮合循环持续打爆 allocator。
        """
        delay = self.cfg.no_capacity_requeue_delay_td().total_seconds()
        if delay <= 0:
            delay = self.cfg.match_form_cooldown_td().total_seconds()  # 显式关闭时退化
        if self.entry_limiter is not None and delay > 0:
            for tid in m.ticket_ids:
                err = await self.entry_limiter.arm_form_cooldown(tid, delay)
                if err is not None:
                    plog.get().warning(
                        "no_capacity_form_cooldown_arm_failed", ticket_id=tid, err=str(err)
                    )
        wait_sec = int(delay) + (1 if delay % 1 else 0)
        await self.fail_match(
            m,
            lambda _tid, _ticket: False,  # 无人有过错,全票退队
            skip_failed_push=True,
            requeue_wait_sec=wait_sec,
        )
        plog.get().info(
            "match_no_capacity_requeued",
            match_id=m.match_id,
            tickets=len(m.ticket_ids),
            retry_after_sec=wait_sec,
        )

    async def fail_match(
        self,
        m: matchpb.MatchStorageRecord,
        is_faulty,  # noqa: ANN001
        *,
        skip_failed_push: bool = False,
        requeue_wait_sec: int = 0,
    ) -> None:
        """失败收尾的公共骨架,**可幂等重跑**。

        守卫:只处理仍归属本 match 的票据(match_id 相等)。已被并发退回 / 归属他局的
        票据盲写会把他局在进票据抽回队列(违反不变量 §1),一律跳过。
        """
        if not skip_failed_push:
            await self.push_progress(
                m.match_id, h.STAGE_FAILED, list(m.members), "", m.map_id
            )

        problems: list[str] = []
        for tid in m.ticket_ids:
            try:
                ticket, found = await self.repo.get_ticket(tid)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                problems.append(str(exc))
                continue
            if not found:
                # 票据删除可能已提交而它的 claim 清理没做完。
                # compare-delete 只清仍指向这张**确切旧票**的 claim。
                for mem in m.members:
                    claimed_ticket, claimed = await self.repo.get_player_ticket(mem.player_id)
                    if claimed and claimed_ticket == tid:
                        await self.repo.delete_player_index_if_matches(mem.player_id, tid)
                continue
            if ticket.match_id != m.match_id:
                if ticket.match_id == 0:
                    # 已退回队列:claim 必须固化,否则它会先于票据消失。
                    for pid in h.member_player_ids(ticket.members):
                        await self.repo.refresh_player_claim(pid, tid)
                continue  # 已退回(0)/已归属他局:绝不盲写
            if is_faulty(tid, ticket):
                try:
                    await self.repo.delete_ticket(tid)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    problems.append(str(exc))
                    continue
                for pid in h.member_player_ids(ticket.members):
                    await self.repo.delete_player_index_if_matches(pid, tid)
                continue

            expected = ticket.match_id
            ticket.match_id = 0
            try:
                requeued = await self.repo.requeue_ticket_if_owned(ticket, expected)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning("match_requeue_failed", ticket_id=tid, err=str(exc))
                problems.append(str(exc))
                continue
            if not requeued:
                continue  # 已被取消删除 / 已归属他局
            for pid in h.member_player_ids(ticket.members):
                await self.repo.refresh_player_claim(pid, tid)
            # 补推 QUEUEING:客户端刚收到 FAILED,若不告知"你已自动回到队列",
            # 其再点匹配会撞 ErrMatchAlreadyMatching(4002) 卡死在"匹配不了"。
            if requeue_wait_sec > 0:
                await self.push_queueing_wait(ticket, requeue_wait_sec)
            else:
                await self.push_progress(
                    ticket.ticket_id, h.STAGE_QUEUEING, list(ticket.members), "", ticket.map_id
                )

        if problems:
            # active 索引保留;durable worker 下轮继续重放确定性清理。
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "fail match %d cleanup incomplete: %s",
                m.match_id,
                "; ".join(problems),
            )
        await self.repo.expire_match(m.match_id, self.match_ttl_sec())

    # ── ⑥ 确认期超时 ───────────────────────────────────────────────────────

    async def expire_once(self) -> None:
        """扫描 active ZSET,把确认期已超时的 match 标记失败。"""
        stamp = _now_ms()
        for mid in await self.repo.range_expired_matches(stamp):
            state: dict = {"snapshot": None, "keep_active": False}

            def _mutate(m: matchpb.MatchStorageRecord, _state=state) -> None:
                _state["snapshot"] = None
                _state["keep_active"] = False
                if m.stage == h.STAGE_READY:
                    # READY 滞留 active = 推送交付未确认,由 finalize_ready_match 补推后
                    # 移出;确认期 deadline 对 READY 无意义,不清索引也不判失败。
                    _state["keep_active"] = True
                    return
                if m.stage == h.STAGE_FAILED:
                    if m.allocation_next_attempt_at_ms != -1:
                        _state["snapshot"] = h.clone_match(m)  # 清理未 ACK,继续重放
                    return
                if m.stage == h.STAGE_ALLOCATING:
                    # ALLOCATING 是 durable job。外部结果可能未知(尤其
                    # allocation_uncertain),**本地时间绝不能把未知推断成失败并重排**;
                    # worker / reconciler 会持续推进。
                    _state["keep_active"] = True
                    return
                m.stage = h.STAGE_FAILED
                _state["snapshot"] = h.clone_match(m)

            try:
                await self.repo.update_match_with_lock(
                    mid, self.cfg.optimistic_retry, _mutate
                )
            except errcode.PandoraError as exc:
                plog.get().warning("expire_lock_failed", match_id=mid, err=str(exc))
                # 只有 canonical 明确不存在才清派生索引;瞬态错误必须保留重试。
                if errcode.as_code(exc) == errcode.ErrMatchNotFound:
                    await self._remove_active(mid)
                continue

            if state["keep_active"]:
                continue
            snapshot = state["snapshot"]
            if snapshot is None:
                await self._remove_active(mid)
                continue
            try:
                await self.fail_match(snapshot, _failed_match_classifier(snapshot))
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "match_failed_cleanup_retry", match_id=mid, err=str(exc)
                )
                continue
            plog.get().info("match_confirm_timeout", match_id=mid)

    # ── ⑦ 两道离线回收 ─────────────────────────────────────────────────────

    async def find_offline_members(self, player_ids: list[int]) -> list[int]:
        """成局前在线校验(**默认关闭**的弱依赖)。

        开关关闭 / locator 未配 / 查询失败 → 返回空(跳过校验,宁可多拉一局不误杀)。
        """
        if not self.cfg.liveness_gate_enabled or self.locator is None:
            return []
        try:
            return await self.locator.find_offline_players(player_ids)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning("match_liveness_check_skipped", err=str(exc))
            return []

    async def liveness_sweep_once(self) -> None:
        """主动清扫队列里掉线玩家的死票(**默认关闭**)。

        ❗ INC-20260724-001:本扫除与成局最终门共用同一份 presence 证据,已一并回退为
        关闭。除成局门那条 MATCHING 不续期的缺陷外,本扫除还有独立的第二受害面:
        玩家一旦进过 MATCHING,该 key 到期消失后 locator 只 EXPIRE 不创建 ⇒ 该玩家
        此后被恒判离线,连"匹配失败后重新排队"的新票据也会在 ≤10s 内被误删。
        """
        if not self.cfg.liveness_gate_enabled or self.locator is None:
            return
        tickets, player_ids = await self._load_queued_tickets()
        if not tickets:
            return
        try:
            offline = await self.locator.find_offline_players(player_ids)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning("liveness_sweep_query_skipped", err=str(exc))
            return  # 弱依赖:locator 抖动不误删任何票
        if not offline:
            return
        offline_set = set(offline)
        for t in tickets:
            if not any(m.player_id in offline_set for m in t.members):
                continue
            deleted, _ = await self.repo.delete_ticket_if_unmatched(t.ticket_id)
            if not deleted:
                continue  # 撞上并发预留 → 放弃,交给成局最终门
            await self.rollback_claims(t.ticket_id, h.member_player_ids(t.members))
            await self.push_progress(
                t.ticket_id, h.STAGE_FAILED, list(t.members), "", t.map_id
            )
            plog.get().info(
                "liveness_sweep_reaped_ticket",
                ticket_id=t.ticket_id,
                members=len(t.members),
            )

    async def queue_absence_sweep_once(self) -> None:
        """周期回收「玩家已离场超过判死窗」的排队票(INC-20260814-001 隔夜幽灵票)。

        与 liveness_sweep_once(已回退关闭)的本质区别是**证据**:那道门按
        「locator 此刻查不查得到」判死,对正常路径上的投影缺席是结构性假阳性;
        本回收按「离开了多久」判(absent_beyond,UNKNOWN 一律放行),证据由 Hub DS
        心跳按 census 全员持续续期,坐在大厅里的人 last_alive_ms 恒为「刚刚」。
        """
        window_ms = int(self.cfg.queue_absence_reap_after_td().total_seconds() * 1000)
        if self.presence is None or window_ms <= 0:
            return
        tickets, player_ids = await self._load_queued_tickets()
        if not tickets:
            return
        try:
            offline, _, _ = await self.absent_beyond(player_ids, window_ms)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning("queue_absence_sweep_skipped", err=str(exc))
            return  # 弱依赖:presence 抖动不误删任何票
        if not offline:
            return
        offline_set = set(offline)
        for t in tickets:
            absent = h.ticket_absent_members(t, offline_set)
            if absent:
                await self._reap_absent_ticket(t, absent, "sweep")

    async def reject_absent_tickets(self, tickets) -> None:  # noqa: ANN001
        """成局装箱前的最终复查(与队列扫除同判据、同判死窗)。

        堵的是扫除节流间隔(10s)内旧票恰好被凑进局的竞态窗。
        弱依赖方向与扫除一致但动作相反:presence 查询失败 → **放行成局**(fail-open)。
        本复查是纵深防御,不给 locator 抖动阻断全部成局的权力;真离线成员被凑进去后,
        由 DS 侧 roster 到齐期限兜底回收对局。
        """
        window_ms = int(self.cfg.queue_absence_reap_after_td().total_seconds() * 1000)
        if self.presence is None or window_ms <= 0 or not tickets:
            return
        ids: list[int] = []
        for t in tickets:
            ids.extend(h.member_player_ids(t.members))
        try:
            offline, _, _ = await self.absent_beyond(ids, window_ms)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning("match_form_absence_check_skipped", err=str(exc))
            return
        if not offline:
            return
        offline_set = set(offline)
        reaped = 0
        for t in tickets:
            absent = h.ticket_absent_members(t, offline_set)
            if absent and await self._reap_absent_ticket(t, absent, "form_recheck"):
                reaped += 1
        raise errcode.PandoraError(
            errcode.ErrMatchMemberOffline,
            "players %s absent beyond %dms; reaped %d stale ticket(s) before forming match",
            offline,
            window_ms,
            reaped,
        )

    async def _reap_absent_ticket(
        self, t: matchpb.MatchTicketStorageRecord, absent: list[int], via: str
    ) -> bool:
        """CAS 回收一张仍未撮合的陈旧票并释放全体成员归属。

        撞上并发预留(票已进 match)或删除失败 → 放弃返回 False,
        下轮扫除 / 成局复查兜底。
        """
        deleted, _ = await self.repo.delete_ticket_if_unmatched(t.ticket_id)
        if not deleted:
            return False
        await self.rollback_claims(t.ticket_id, h.member_player_ids(t.members))
        # FAILED 推给票据全体成员:同票在线队友立刻知道排队被取消;
        # 离场者本人收不到,重连后 GetMatchProgress 兜底。
        await self.push_progress(t.ticket_id, h.STAGE_FAILED, list(t.members), "", t.map_id)
        queued_ms = _now_ms() - t.enqueued_at_ms if t.enqueued_at_ms > 0 else 0
        plog.get().info(
            "queue_absence_reaped_ticket",
            ticket_id=t.ticket_id,
            members=len(t.members),
            absent_players=absent,
            queued_ms=queued_ms,
            via=via,
        )
        return True

    async def _load_queued_tickets(self):  # noqa: ANN201
        """载入仍在排队(match_id==0)的票据 + 其全体成员 ID。"""
        tickets: list[matchpb.MatchTicketStorageRecord] = []
        player_ids: list[int] = []
        for tid in await self.repo.range_queue_tickets():
            try:
                t, found = await self.repo.get_ticket(tid)
            except asyncio.CancelledError:
                raise
            except BaseException:  # noqa: BLE001
                continue
            if not found or t.match_id != 0:
                continue  # 已消失 / 已进 match 的票据不归回收管
            tickets.append(t)
            player_ids.extend(h.member_player_ids(t.members))
        return tickets, player_ids


def _failed_match_classifier(m: matchpb.MatchStorageRecord):  # noqa: ANN201
    """从一条已 FAILED 的 match 反推定责函数(供幂等重跑用)。

    有显式拒绝者时只判拒绝者所在票据有过错;没有(超时)时判「含未确认成员」的票据。
    """
    confirm_of = {member.player_id: member.confirm for member in m.members}
    has_rejected = any(member.confirm == h.CONFIRM_REJECTED for member in m.members)

    def _classify(_tid: int, ticket: matchpb.MatchTicketStorageRecord) -> bool:
        if has_rejected:
            return any(
                confirm_of.get(member.player_id) == h.CONFIRM_REJECTED
                for member in ticket.members
            )
        return not h.ticket_all_accepted(ticket, confirm_of)

    return _classify


def _uuid4() -> str:
    import uuid

    return str(uuid.uuid4())
