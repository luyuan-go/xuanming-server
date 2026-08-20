"""leaderboard 业务逻辑层 —— 对应 Go 侧 internal/biz/leaderboard.go。

职责(docs/design/decision-revisit-leaderboard.md):
  - SubmitScore:按 mode(SET_IF_HIGHER / SET / INCREMENT)写 Redis ZSET,
    首次按 Options 建榜(TTL 临时榜 / max_size 截断 / 时间 tie-break);
  - 读查询(GetRank / GetRange / GetAround)按榜 meta(ascending)选排序方向;
  - SettleBoard:取 Top-N → 落 MySQL 快照 + 批次(uk 防重复结算)→ 按 RewardTable
    幂等发奖(调 inventory.GrantItems,uk grant_idem 防重复发奖)+ 发 kafka → 可选 reset。

写入(Submit / Settle / Remove / Delete)是系统接口,鉴权由 service 层 / 内网边界保证。
"""

from __future__ import annotations

import asyncio

import dataclasses
import time

from pandora.leaderboard.v1 import leaderboard_pb2

from pandorapy import dbguard, errcode
from pandorapy import log as plog
from pandorapy.services.leaderboard import board_store as bs
from pandorapy.services.leaderboard import budgets as lbbudgets
from pandorapy.services.leaderboard import conf as lbconf
from pandorapy.services.leaderboard import repo as lbrepo
from pandorapy.services.leaderboard import reward_client as lbreward


def now_ms() -> int:
    """当前毫秒。对应 Go 的 nowMs()。"""
    return int(time.time() * 1000)


def validate_board(b: bs.BoardKey) -> None:
    """校验 BoardKey 合法性。对应 Go 的 validateBoard。

    scope 越界必须拒:scope 参与 board 串(即 Redis key),放行一个越界值等于
    凭空开出一个谁也读不到的榜 —— 玩家分上报"成功"了,榜上永远没有他。
    """
    if b.board_type == 0:
        raise errcode.PandoraError(errcode.ErrLeaderboardInvalidBoard, "board_type required")
    if b.scope < bs.SCOPE_GLOBAL or b.scope > bs.SCOPE_CUSTOM:
        raise errcode.PandoraError(
            errcode.ErrLeaderboardInvalidBoard, "invalid scope %d", int(b.scope)
        )


@dataclasses.dataclass(slots=True)
class RankView:
    """GetRank 的返回:精确名次,或(未进精确榜时)直方图区间估算名次。"""

    entry: bs.Entry | None = None
    found: bool = False
    # estimated=True → entry.rank 是榜外区间估算(约值),entry.updated_at_ms 恒 0。
    estimated: bool = False
    # 直方图口径参与总人数(仅 estimated=True 时填,供百分位展示)。
    total_submitters: int = 0


@dataclasses.dataclass(slots=True)
class SettleResult:
    """SettleBoard 的返回。"""

    settlement_id: int
    settled_count: int
    already_settled: bool = False
    winners: list[bs.Entry] = dataclasses.field(default_factory=list)


def rewards_for_rank(table, rank: int) -> list[lbreward.RewardGrant]:  # noqa: ANN001
    """某名次命中的奖励(**取第一个匹配区间**)。对应 Go 的 rewardsForRank。

    "第一个匹配"而不是"合并全部匹配"是刻意的:RewardTable 由运营配,区间重叠时
    合并会让重叠段的玩家拿双份;取第一个则行为可预测(按配置顺序),配错也只是少发。
    """
    if table is None:
        return []
    for tier in table.tiers:
        if tier.rank_from <= rank <= tier.rank_to:
            return [
                lbreward.RewardGrant(item_config_id=it.item_config_id, count=it.count)
                for it in tier.items
                if it.count > 0
            ]
    return []


def encode_reward_grants(items: list[lbreward.RewardGrant]) -> bytes:
    """发奖明细 → pb 二进制(列 reward_pb),落库前过写入侧字节闸(§9.24)。"""
    rec = leaderboard_pb2.RewardGrantStorageRecord(
        items=[
            leaderboard_pb2.RewardItem(item_config_id=it.item_config_id, count=it.count)
            for it in items
        ]
    )
    raw = rec.SerializeToString()
    dbguard.check_payload(
        "pandora_leaderboard.leaderboard_reward_log.reward_pb",
        raw,
        lbbudgets.REWARD_PAYLOAD_MAX_BYTES,
    )
    return raw


def decode_reward_grants(raw: bytes) -> list[lbreward.RewardGrant]:
    """pb 二进制 → 发奖明细(补发重放入参)。解不出来时抛,由调用方标 FAILED。"""
    rec = leaderboard_pb2.RewardGrantStorageRecord()
    rec.ParseFromString(raw)
    return [
        lbreward.RewardGrant(item_config_id=it.item_config_id, count=it.count)
        for it in rec.items
    ]


class LeaderboardUsecase:
    """leaderboard 业务逻辑核心。对应 Go 的 biz.LeaderboardUsecase。"""

    __slots__ = ("_repo", "_board", "_granter", "_events", "_sf", "_cfg")

    def __init__(
        self,
        repo: lbrepo.MySQLLeaderboardRepo,
        board: bs.RedisBoardStore,
        granter,  # noqa: ANN001 —— 最小接口 grant(player_id, idem_key, items)
        events,  # noqa: ANN001 —— 弱依赖,可为 None
        snowflake,  # noqa: ANN001 —— 最小接口 generate()
        cfg: lbconf.LeaderboardConf,
    ) -> None:
        self._repo = repo
        self._board = board
        # granter 为 None 时退化为 Noop(与 Go 同)。注意这**不是**给生产用的兜底 ——
        # 生产漏配由 main.py 的 reward_granter_missing 闸在启动期拦住。
        self._granter = granter if granter is not None else lbreward.NoopRewardGranter()
        self._events = events
        self._sf = snowflake
        # ★ 这里再兜一次默认值(与 Go 的 NewLeaderboardUsecase 同):conf.apply_defaults
        # 已经填过,但 biz 允许被测试直接构造 —— 少了这层,一个 cfg 全零的单测会
        # "通过"却在断言完全不同的行为(limit=0 → 恒空列表)。
        self._cfg = _with_defaults(cfg)

    # ── 写 ───────────────────────────────────────────────────────────────────

    async def submit_score(
        self, b: bs.BoardKey, entity_id: int, score: int, mode: int, opt: bs.Options
    ) -> tuple[int, int]:
        """写入分数。返回 (新真实分, 1-based 名次;0=未上榜)。"""
        validate_board(b)
        if entity_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "entity_id required")
        if mode < bs.MODE_SET_IF_HIGHER or mode > bs.MODE_INCREMENT:
            # 越界 mode 兜成 SET_IF_HIGHER(与 Go 同)而不是报错:客户端漏填 mode
            # (proto 零值 UNSPECIFIED)是常态,报错会让整条上报链停摆。
            mode = bs.MODE_SET_IF_HIGHER
        if opt.estimate_bucket_width <= 0:
            opt = dataclasses.replace(
                opt, estimate_bucket_width=self._cfg.default_estimate_bucket_width
            )
        return await self._board.submit(b, entity_id, score, mode, opt, now_ms())

    async def remove_entry(self, b: bs.BoardKey, entity_id: int) -> None:
        """移除某 entity(封号 / 作弊清理)。"""
        validate_board(b)
        if entity_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "entity_id required")
        await self._board.remove(b, entity_id)

    async def delete_board(self, b: bs.BoardKey) -> None:
        """删整个榜。"""
        validate_board(b)
        await self._board.delete(b)

    # ── 读 ───────────────────────────────────────────────────────────────────

    async def _board_ascending(self, b: bs.BoardKey) -> tuple[bool, bool]:
        """读榜排序方向。返回 (ascending, exists);meta 缺失默认降序。"""
        asc, _tie, exists = await self._board.get_meta(b)
        return asc, exists

    async def get_rank(self, b: bs.BoardKey, entity_id: int) -> RankView:
        """查某 entity 名次:精确榜命中回精确名次;被 max_size 截断出榜的回退直方图
        区间估算(约值);从未上报 → found=False。"""
        validate_board(b)
        asc, _exists = await self._board_ascending(b)
        entry, found = await self._board.rank(b, entity_id, asc)
        if found:
            return RankView(entry=entry, found=True)
        est_entry, total, est_found = await self._board.estimate(b, entity_id, asc)
        if not est_found:
            return RankView()
        return RankView(
            entry=est_entry, found=True, estimated=True, total_submitters=total
        )

    async def get_range(
        self, b: bs.BoardKey, offset: int, limit: int
    ) -> tuple[list[bs.Entry], int]:
        """取榜区间;返回 (entries, 榜总人数)。"""
        validate_board(b)
        if limit <= 0:
            limit = self._cfg.default_list_limit
        if limit > self._cfg.max_list_limit:
            limit = self._cfg.max_list_limit
        if offset < 0:
            offset = 0
        asc, _exists = await self._board_ascending(b)
        entries = await self._board.range(b, offset, limit, asc)
        total = await self._board.total(b)
        return entries, total

    async def get_around(
        self, b: bs.BoardKey, entity_id: int, radius: int
    ) -> tuple[list[bs.Entry], bool]:
        """取某 entity 上下 radius 名。"""
        validate_board(b)
        if radius <= 0:
            radius = self._cfg.default_around_radius
        if radius > self._cfg.max_list_limit:
            radius = self._cfg.max_list_limit
        asc, _exists = await self._board_ascending(b)
        return await self._board.around(b, entity_id, radius, asc)

    # ── 结算 ─────────────────────────────────────────────────────────────────

    async def settle_board(
        self,
        b: bs.BoardKey,
        top_n: int,
        reward_table,  # noqa: ANN001 —— leaderboard_pb2.RewardTable | None
        reset_after: bool,
        settle_idem_key: str,
    ) -> SettleResult:
        """结算:取 Top-N → 落快照 + 批次(幂等)→ 发奖 + kafka → 可选 reset。

        幂等:settle_idempotency_key(默认 = board 串)命中 → already=True,
        不重复发奖(回放已存批次的快照)。

        发奖:仅对"按玩家发奖"的榜(GLOBAL / INSTANCE / CUSTOM,entity=player_id)
        调 granter;GUILD 榜 entity=guild_id,不直接发玩家背包,只落快照 + 发 kafka
        由工会服务消费分发。
        """
        validate_board(b)
        if top_n <= 0:
            top_n = self._cfg.default_settle_top_n
        if not settle_idem_key:
            settle_idem_key = "lb:" + b.board_str()

        asc, exists = await self._board_ascending(b)
        if not exists:
            # ★ 必须在 ClaimSettlement **之前**拦。榜不存在还往下走会占掉 settle uk,
            # 这一期就再也结不出来了(重放只会命中幂等回放空快照)。
            raise errcode.PandoraError(
                errcode.ErrLeaderboardBoardNotFound, "board not found: %s", b.board_str()
            )

        winners = await self._board.range(b, 0, top_n, asc)

        now = now_ms()
        rec = lbrepo.SettlementRecord(
            settlement_id=self._sf.generate(),
            board_type=b.board_type,
            scope=int(b.scope),
            scope_id=b.scope_id,
            period=b.period,
            top_n=top_n,
            settled_count=len(winners),
            settle_idem_key=settle_idem_key,
            reset_after=reset_after,
            created_at_ms=now,
        )
        existing, already = await self._repo.claim_settlement(rec)
        if already:
            # §9.2 结算幂等:重复 SettleBoard(重试 / 重复调度 / 多副本)命中。
            # SettleBoard 是稀有管理操作,INFO 量级极低;能区分"首次结算"与"幂等重放"
            # 是排查疑似重复发奖时唯一的现场证据。
            plog.get().info(
                "lb_settle_idempotent_hit",
                settlement_id=existing.settlement_id,
                board=b.board_str(),
            )
            snap_winners = await self._load_snapshot_winners(existing.settlement_id)
            return SettleResult(
                settlement_id=existing.settlement_id,
                settled_count=existing.settled_count,
                already_settled=True,
                winners=snap_winners,
            )

        # 落 Top-N 快照
        rows = [
            lbrepo.SnapshotRow(
                rank=w.rank, entity_id=w.entity_id, score=w.score, created_at_ms=now
            )
            for w in winners
        ]
        await self._repo.save_snapshot(rec.settlement_id, rows)

        # 发奖(仅按玩家发奖的榜)
        if reward_table is not None and len(reward_table.tiers) > 0 and b.scope != bs.SCOPE_GUILD:
            await self._grant_rewards(rec.settlement_id, winners, reward_table)

        # kafka 结算事件(弱依赖:发不出去不影响结算已经落库的事实)
        if self._events is not None:
            pb_winners = [
                leaderboard_pb2.LeaderboardEntry(
                    entity_id=w.entity_id,
                    score=w.score,
                    rank=w.rank,
                    updated_at_ms=w.updated_at_ms,
                )
                for w in winners
            ]
            try:
                await self._events.push_settle(rec.settlement_id, b, pb_winners)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001 —— 弱依赖不得让结算回滚
                plog.get().warning(
                    "lb_settle_event_push_failed",
                    settlement_id=rec.settlement_id,
                    err=str(exc),
                )

        # reset(周期榜进入下一周期)
        if reset_after:
            try:
                await self._board.clear(b)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                # 清不掉只是"下一周期起点带着旧分",比让整个结算失败轻得多 ——
                # 结算已经落库,抛出去会让调用方重试并命中幂等,而榜仍然没清。
                plog.get().warning(
                    "lb_settle_reset_failed", board=b.board_str(), err=str(exc)
                )

        return SettleResult(
            settlement_id=rec.settlement_id,
            settled_count=len(winners),
            winners=winners,
        )

    async def _load_snapshot_winners(self, settlement_id: int) -> list[bs.Entry]:
        """从 MySQL 快照按 rank 升序回放 winners(幂等命中复用)。

        快照不存 updated_at,updated_at_ms 留 0:结算快照是名次 + 分数的权威归档,
        展示时间非必需。
        """
        rows = await self._repo.load_snapshot(settlement_id)
        return [
            bs.Entry(entity_id=r.entity_id, score=r.score, rank=r.rank) for r in rows
        ]

    async def _grant_rewards(self, settlement_id: int, winners: list[bs.Entry], table) -> None:  # noqa: ANN001
        """按 RewardTable 给 Top-N 逐名次幂等发奖(**失败不中断整批**,逐条记 log)。

        一个人发失败就中断的话,后面所有名次都收不到奖,而且没有任何补偿路径 ——
        逐条记 FAILED 交给补发扫描才是可自愈的形态。
        """
        logger = plog.get()
        for w in winners:
            items = rewards_for_rank(table, w.rank)
            if not items:
                continue
            grant_key = f"lb:{settlement_id}:{w.entity_id}"
            now = now_ms()
            try:
                payload = encode_reward_grants(items)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                # 编码不出来就**不能** Claim:留下一条解不开的补发入参比不发更糟
                # (补扫会永远标 FAILED,而且没人知道原本该发什么)。
                logger.error(
                    "lb_reward_encode_failed",
                    settlement_id=settlement_id,
                    entity=w.entity_id,
                    items=len(items),
                    err=str(exc),
                )
                continue
            log_rec = lbrepo.RewardLogRecord(
                settlement_id=settlement_id,
                entity_id=w.entity_id,
                rank=w.rank,
                grant_idem_key=grant_key,
                status=lbrepo.REWARD_PENDING,
                reward_payload=payload,
                created_at_ms=now,
                updated_at_ms=now,
            )
            try:
                already = await self._repo.claim_reward(log_rec)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error(
                    "lb_reward_claim_failed",
                    settlement_id=settlement_id,
                    entity=w.entity_id,
                    err=str(exc),
                )
                continue
            if already:
                continue  # 本名次已发过(幂等)
            try:
                await self._granter.grant(w.entity_id, grant_key, items)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error(
                    "lb_reward_grant_failed",
                    settlement_id=settlement_id,
                    entity=w.entity_id,
                    err=str(exc),
                )
                await self._mark_reward_quiet(
                    grant_key, lbrepo.REWARD_FAILED, settlement_id, w.entity_id, "FAILED"
                )
                continue
            # 发奖成功但标 GRANTED 失败 → 该行滞留 PENDING,被补扫再次 Grant
            # (幂等键兜住不双发,但状态会漂移),所以只 WARN 不改变结算结果。
            await self._mark_reward_quiet(
                grant_key, lbrepo.REWARD_GRANTED, settlement_id, w.entity_id, "GRANTED"
            )

    async def _mark_reward_quiet(
        self, grant_key: str, status: int, settlement_id: int, entity_id: int, target: str
    ) -> None:
        """标状态,失败只 WARN(对应 Go 里那两处 `if merr := ...; merr != nil` 分支)。"""
        try:
            await self._repo.mark_reward(grant_key, status, now_ms())
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
            # 吞掉之后取消就**不再传播** —— 该停的停不下来:
            #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
            #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
            # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "lb_mark_reward_failed",
                settlement_id=settlement_id,
                entity=entity_id,
                target_status=target,
                err=str(exc),
            )

    # ── 发奖补扫(后台循环调)──────────────────────────────────────────────

    async def retry_ungranted_rewards(
        self, older_than_sec: float, limit: int
    ) -> tuple[int, int]:
        """补发未成的结算奖励。返回 (本轮补发成功数, 仍失败数)。

        覆盖两类漏发:FAILED(inventory 拒绝 / 不可达)和 PENDING(ClaimReward 后、
        MarkReward 前进程崩残留)。older_than 把"刚结算还在同步发"的批次挡在扫描外;
        Grant 以 grant_idempotency_key 幂等,多副本并发补扫 / 与同步路径重叠都不会双发。
        """
        logger = plog.get()
        try:
            rows = await self._repo.list_ungranted_rewards(
                now_ms() - int(older_than_sec * 1000), limit
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
            # 吞掉之后取消就**不再传播** —— 该停的停不下来:
            #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
            #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
            # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("lb_reward_retry_list_failed", err=str(exc))
            return 0, 0

        granted = 0
        failed = 0
        for rec in rows:
            try:
                items = decode_reward_grants(rec.reward_payload)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                items = []
                decode_err: BaseException | None = exc
            else:
                decode_err = None
            if decode_err is not None or not items:
                # 脏数据:标 FAILED 不再重扫死循环,靠 ERROR 告警人工介入。
                logger.error(
                    "lb_reward_retry_bad_payload",
                    key=rec.grant_idem_key,
                    payload_bytes=len(rec.reward_payload),
                    err=str(decode_err) if decode_err else "empty items",
                )
                await self._mark_reward_silent(rec.grant_idem_key, lbrepo.REWARD_FAILED)
                failed += 1
                continue
            try:
                await self._granter.grant(rec.entity_id, rec.grant_idem_key, items)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.warning(
                    "lb_reward_retry_grant_failed",
                    key=rec.grant_idem_key,
                    entity=rec.entity_id,
                    err=str(exc),
                )
                await self._mark_reward_silent(rec.grant_idem_key, lbrepo.REWARD_FAILED)
                failed += 1
                continue
            try:
                await self._repo.mark_reward(rec.grant_idem_key, lbrepo.REWARD_GRANTED, now_ms())
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                # 已发成但没标上:下轮补扫会再 Grant(幂等 no-op)后重标,自愈。
                logger.warning(
                    "lb_reward_retry_mark_failed", key=rec.grant_idem_key, err=str(exc)
                )
            granted += 1

        if granted or failed:
            logger.info("lb_reward_retry_done", granted=granted, failed=failed)
        return granted, failed

    async def _mark_reward_silent(self, grant_key: str, status: int) -> None:
        """对应 Go 补扫里的 `_ = u.repo.MarkReward(...)`(刻意丢弃返回值)。

        为什么这里可以丢:标不上的后果只是下一轮再扫一次,而上面已经打了导致本次
        失败的那条日志 —— 再叠一条"标记也失败"只会让同一件事刷两行。
        """
        try:
            await self._repo.mark_reward(grant_key, status, now_ms())
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
            # 吞掉之后取消就**不再传播** —— 该停的停不下来:
            #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
            #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
            # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
            raise
        except BaseException:  # noqa: BLE001, S110
            pass


def _with_defaults(cfg: lbconf.LeaderboardConf) -> lbconf.LeaderboardConf:
    """把 <=0 的参数兜成服务默认值(对应 Go NewLeaderboardUsecase 里那串 if)。

    返回副本而不是原地改:usecase 收到的 cfg 可能是被别处共享的对象,原地改会让
    "谁改了我的配置"变成一个无法排查的问题。
    """
    out = cfg.model_copy(deep=True)
    if out.default_list_limit <= 0:
        out.default_list_limit = lbconf.DEFAULT_LIST_LIMIT
    if out.max_list_limit <= 0:
        out.max_list_limit = lbconf.DEFAULT_MAX_LIST_LIMIT
    if out.default_around_radius <= 0:
        out.default_around_radius = lbconf.DEFAULT_AROUND_RADIUS
    if out.default_settle_top_n <= 0:
        out.default_settle_top_n = lbconf.DEFAULT_SETTLE_TOP_N
    if out.default_estimate_bucket_width <= 0:
        out.default_estimate_bucket_width = bs.DEFAULT_ESTIMATE_BUCKET_WIDTH
    return out
