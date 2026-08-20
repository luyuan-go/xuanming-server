"""auction 业务逻辑层(全服拍卖行 / 撮合引擎)—— 对应 Go 侧 internal/biz/auction.go。

职责:
  - 挂单(SELL)/ 出价(BUY)按 market_id 分片,MySQL 权威保存;
  - 「每个 market 单写者」从 MySQL 精确 item 候选**串行**撮合,价格-时间优先
    (成交价 = 被动挂单价);
  - 三层幂等:挂单 idempotency_key / 冻结 order_id / 结算 match_id;
  - 成交发 kafka pandora.auction.match(MySQL outbox 至少一次),
    流转发 pandora.auction.audit(弱依赖,有界队列,满则丢)。

★ 单写者由两层叠成,少任何一层都不够:
    进程内条带锁   同实例内同 market 串行(固定 256 条带,**不按 market_id 建 map**
                   —— market_id 是客户端可控字段,惰性建 map 永不删 = 内存 DoS)
    Redis market 锁 跨实例互斥。它**不是 fencing token**,正确性最终由 MySQL 行锁、
                   条件状态迁移和唯一键兜底(见 market_locker.py)。

★ 撮合候选**只能**来自 MySQL。Redis ZSET 是旧版本兼容缓存:它的 key 只含 market_id,
  无法区分同品类里的不同 item_config_id,拿它选单会跨物品成交。

owner_id / buyer_id 一律以 JWT ctx 为准(R5),service 层注入。
"""

from __future__ import annotations

import asyncio
import contextlib
import time

from pandora.auction.v1 import auction_pb2

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import logwindow
from pandorapy.services.auction import repo as arepo
from pandorapy.services.auction.market_router import MarketRouter
from pandorapy.services.auction.owner_slots import OwnerOrderSlot
from pandorapy.services.auction.submit import (
    SIDE_BUY,
    SIDE_SELL,
    STATUS_CANCELED,
    STATUS_EXPIRED,
    STATUS_OPEN,
    STATUS_PARTIAL,
    STATUS_PENDING,
    AuctionSubmitter,
    OrderRecord,
    is_terminal,
    validate_submit,
)

# 进程内 market 条带锁数量(内存恒定,与 market 基数无关)。
MARKET_LOCK_STRIPES = 256

# 退化期日志的最小重打间隔(毫秒)。这两处都在**每请求**路径上,退化时会按 QPS 刷屏。
DEGRADE_LOG_WINDOW_MS = 5000

# Redis 兼容缓存单次写入的短超时。撮合候选由 MySQL 权威库选择,因此缓存失败只能告警,
# 不能让"已冻结资产且已持久化的活跃订单"对撮合不可见。
BOOK_CACHE_WRITE_TIMEOUT_SEC = 0.25

# 请求取消后仍留一个短窗口清理终态配额成员;MySQL 终态是权威,清理失败会在
# 玩家下次触顶时惰性重试。
OWNER_SLOT_WRITE_TIMEOUT_SEC = 2.0

# 给遵守取消的 pusher 一个发送上限;不遵守的底层实现也只会卡住独立 audit worker。
AUDIT_DISPATCH_TIMEOUT_SEC = 5.0

# 外部账本单次尝试上限(Go: context.WithTimeout(ctx, 5*time.Second))。
LEDGER_ATTEMPT_TIMEOUT_SEC = 5.0

# 持久 marker 的写入短超时(Go: plog.Detach + 2s)。
MARKER_WRITE_TIMEOUT_SEC = 2.0

# 失败副作用的退避时长。
SIDE_EFFECT_RETRY_DELAY_MS = 30_000

# 单次撮合里 CAS/TOCTOU 冲突的重试上限。热点 market / 异常替身竞争会耗尽它。
MAX_RESERVE_CONFLICTS = 64

_MY_ORDERS_DEFAULT_LIMIT = 50
_MY_ORDERS_MAX_LIMIT = 100


def now_ms() -> int:
    return int(time.time() * 1000)


def opposite(side: int) -> int:
    return SIDE_BUY if side == SIDE_SELL else SIDE_SELL


def crosses(side: int, price: int, best_price: int) -> bool:
    """incoming 价格能否吃到对手盘最优价。

    - incoming SELL @ P:对手是 BUY,最高买价 >= P 才成交。
    - incoming BUY  @ P:对手是 SELL,最低卖价 <= P 才成交。

    ★ 判据写反不会报错,只会让所有能成交的单都挂在簿上不动(或反过来乱成交)。
    """
    if side == SIDE_SELL:
        return best_price >= price
    return best_price <= price


def to_proto_order(r: OrderRecord) -> auction_pb2.AuctionOrder:
    return auction_pb2.AuctionOrder(
        order_id=r.order_id,
        market_id=r.market_id,
        owner_id=r.owner_id,
        side=r.side,
        item_config_id=r.item_config_id,
        quantity=r.quantity,
        filled_quantity=r.filled_quantity,
        price=r.price,
        status=r.status,
        created_at_ms=r.created_at_ms,
        updated_at_ms=r.updated_at_ms,
    )


def to_proto_match(m: arepo.MatchRecord) -> auction_pb2.AuctionMatchEvent:
    return auction_pb2.AuctionMatchEvent(
        match_id=m.match_id,
        market_id=m.market_id,
        sell_order_id=m.sell_order_id,
        buy_order_id=m.buy_order_id,
        seller_id=m.seller_id,
        buyer_id=m.buyer_id,
        item_config_id=m.item_config_id,
        quantity=m.quantity,
        price=m.price,
        matched_at_ms=m.matched_at_ms,
    )


def _copy_into(dst: OrderRecord, src: OrderRecord) -> None:
    """把权威快照整体覆盖进调用方持有的记录(对应 Go 的 `*incoming = *updatedIncoming`)。

    ★ 必须整体覆盖,不能只挑几个字段:撮合循环靠 incoming 的 status /
    filled_quantity / match_pending 决定还要不要继续、要不要清 marker。
    漏抄一个字段就会多撮合一轮或少清一次 marker,而两者都不报错。
    """
    for field in (
        "order_id",
        "market_id",
        "owner_id",
        "side",
        "item_config_id",
        "quantity",
        "filled_quantity",
        "price",
        "status",
        "release_pending",
        "match_pending",
        "escrow_verified",
        "reconcile_next_attempt_at_ms",
        "release_next_attempt_at_ms",
        "idempotency_key",
        "created_at_ms",
        "updated_at_ms",
    ):
        setattr(dst, field, getattr(src, field))


class _OwnerSlotGate:
    """给 AuctionSubmitter 的 owner 名额门面。

    submit 只关心"能不能占到名额",预热 / 惰性清理的复杂度留在 usecase 里
    (与 Go 一致:reserveOwnerSlotPruning 是 biz 的方法,不是 data 的)。
    """

    __slots__ = ("_uc",)

    def __init__(self, uc: "AuctionUsecase") -> None:
        self._uc = uc

    async def reserve(self, rec: OrderRecord) -> None:
        await self._uc.reserve_owner_slot_pruning(rec)

    async def release(self, rec: OrderRecord) -> None:
        # 这里**不吞异常**:调用方(submitter)自己会 WARN 一条,
        # 两处都吞会让"名额没释放"变成完全无痕。
        await self._uc.release_owner_slot_raw(rec)


class AuctionUsecase:
    """auction 服务业务逻辑核心。"""

    def __init__(
        self,
        repo: arepo.MySQLAuctionRepo,
        book,  # noqa: ANN001 —— RedisBookStore | None(兼容缓存,弱依赖)
        slots,  # noqa: ANN001 —— RedisOwnerSlotLimiter
        ledger,  # noqa: ANN001 —— GrpcInventoryLedger | NoopSettlementLedger
        events,  # noqa: ANN001 —— AuctionEventPusher | None(弱依赖,可为 None)
        order_sf,  # noqa: ANN001
        match_sf,  # noqa: ANN001
        cfg,  # noqa: ANN001 —— conf.AuctionConf
    ) -> None:
        self._repo = repo
        self._book = book
        self._slots = slots
        self._ledger = ledger
        self._events = events
        # ★ order_id 与 match_id 是两个互不相干的 ID 空间,各持一个独立发号器:
        # 一笔大单在撮合循环里会连铸 N 个 match_id,与并发挂单的 order_id 各走各的
        # step 池(每池 32768/s)。
        # ⚠️ 两者共用同一 nodeID,发出的 ID 会**逐位相同** —— 必须各自留在自己的表 /
        # 唯一键里,禁止混进同一容器比较。
        self._order_sf = order_sf
        self._match_sf = match_sf
        self._cfg = cfg
        self._rate_quota = None
        self._market_locker = None
        self._market_router: MarketRouter | None = None

        self._locks = [asyncio.Lock() for _ in range(MARKET_LOCK_STRIPES)]
        self._audit_queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.audit_queue_capacity)
        self._audit_task: asyncio.Task | None = None
        self._audit_closed = False
        # 退化期日志限流(模式 C):两处都在每请求路径上,不按 market_id 分桶
        # (客户端可控字段,分桶会被恶意刷成内存 DoS)。
        self._misrouted_log = logwindow.Window()
        self._audit_drop_log = logwindow.Window()

        self._submitter = AuctionSubmitter(
            repo, ledger, _OwnerSlotGate(self), order_sf, cfg, matcher=self._match
        )

    # ── 装配 ──────────────────────────────────────────────────────────────

    def set_rate_quota(self, quota) -> None:  # noqa: ANN001
        """注入频率配额(可选;不注入 = 不限,dev 联调兼容)。"""
        self._rate_quota = quota

    def set_market_locker(self, locker) -> None:  # noqa: ANN001
        self._market_locker = locker

    def set_market_router(self, router: MarketRouter | None) -> None:
        self._market_router = router

    def start(self) -> None:
        """启动弱依赖 audit worker。必须在 event loop 内调用。"""
        if self._audit_task is None:
            self._audit_task = asyncio.create_task(
                self._run_audit_worker(), name="auction_audit_worker"
            )

    async def close(self) -> None:
        """停止 audit worker。**必须在关闭底层 Kafka producer 之前**调用。

        ★ 刻意**不排空队列**:broker 故障时排空会让进程退出时间与积压条数成正比,
        而 audit 本来就不承担资产正确性 —— 停机拖十分钟比丢几条 audit 严重得多。
        """
        self._audit_closed = True
        task = self._audit_task
        self._audit_task = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    # ── 单写者保护 ────────────────────────────────────────────────────────

    @contextlib.asynccontextmanager
    async def _guard_market(self, market_id: int):
        """先进程内条带锁(总是),再叠跨实例 Redis 锁。释放按相反顺序。"""
        self._observe_routing(market_id)
        stripe = self._locks[market_id % MARKET_LOCK_STRIPES]
        await stripe.acquire()
        lease = None
        try:
            if self._market_locker is not None:
                # 抢锁超时抛 ErrAuctionMarketBusy;进程内锁必须先回退,
                # 否则一次 Busy 会把该条带永久锁死。
                lease = await self._market_locker.lock(market_id)
        except BaseException:
            stripe.release()
            raise
        try:
            yield
        finally:
            if lease is not None:
                await lease.release()
            stripe.release()

    def _observe_routing(self, market_id: int) -> None:
        """路由观测:非 owner 实例处理某 market 说明路由抖动 / rebalance。

        ★ 只告警**不阻断** —— 正确性由 market 锁兜底,真正的跨实例转发属基础设施。
        限流为首错 + 每窗口一条 + 累计数:rebalance 期会有成千上万条误路由请求。
        """
        router = self._market_router
        if router is None:
            return
        if not router.owns_market(market_id):
            ok, streak = self._misrouted_log.admit(now_ms(), DEGRADE_LOG_WINDOW_MS)
            if ok:
                plog.get().warning(
                    "auction_market_not_owned",
                    market_id=market_id,
                    self=router.self_id(),
                    owner=router.owner(market_id),
                    streak=streak,
                )
            return
        total, _ = self._misrouted_log.recovered()
        if total > 0:
            plog.get().info("auction_market_routing_recovered", misrouted_total=total)

    async def _allow_action(self, action: str, player_id: int) -> None:
        """频率配额门:窗内超额返回 ErrRateLimited(先于一切副作用);**fail-open**。

        ★ fail-open 是刻意的(anti-abuse §2 铁律:限流是背压门不是权威门)。
        判定失败就拒的话,一次 Redis 抖动会把全服挂单挡在门外,而总量闸
        (max_active_orders_per_player)仍在保护资产正确性。
        """
        quota = self._rate_quota
        if quota is None:
            return
        # ★ 读 `(ok, exc)` 而不是等异常：`allow()` 从不抛，原先那段
        #   `except ... : log("auction_rate_quota_check_failed")` 是**不可达死代码**，
        #   于是这个 Loki 键在代码里存在、永远不会触发。
        ok, quota_exc = await quota.allow(action, player_id)
        if quota_exc is not None:
            # 背压门判定失败一律放行（§9.20：限流不得卡玩家）。
            plog.get().warning(
                "auction_rate_quota_check_failed",
                action=action,
                player_id=player_id,
                err=str(quota_exc),
                fail_open=True,
            )
            return
        if not ok:
            plog.get().warning(
                "auction_rate_quota_rejected", action=action, player_id=player_id
            )
            raise errcode.PandoraError(
                errcode.ErrRateLimited, "auction %s rate limited, retry later", action
            )

    # ── 挂单 / 出价 ───────────────────────────────────────────────────────

    async def place_order(
        self,
        owner_id: int,
        market_id: int,
        item_config_id: int,
        quantity: int,
        price: int,
        idem_key: str,
    ) -> auction_pb2.AuctionOrder:
        return await self._submit(
            owner_id, SIDE_SELL, market_id, item_config_id, quantity, price, idem_key
        )

    async def bid(
        self,
        owner_id: int,
        market_id: int,
        item_config_id: int,
        quantity: int,
        price: int,
        idem_key: str,
    ) -> auction_pb2.AuctionOrder:
        return await self._submit(
            owner_id, SIDE_BUY, market_id, item_config_id, quantity, price, idem_key
        )

    async def _submit(
        self,
        owner_id: int,
        side: int,
        market_id: int,
        item_config_id: int,
        quantity: int,
        price: int,
        idem_key: str,
    ) -> auction_pb2.AuctionOrder:
        if self._cfg.passive_warmup:
            raise errcode.PandoraError(errcode.ErrUnavailable, "auction is in passive warmup")
        if owner_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "owner required")
        # ★ 频率配额按发起方计,先于 PENDING 登记等一切副作用。
        # 同 idem 的合法重试也计数 —— 20/min 的额度下无感,不为它开豁免通道
        # (豁免即绕过面:攻击者只要一直用同一个 idem 就能免限流)。
        await self._allow_action("order", owner_id)
        # 入口校验必须在**取 market 锁之前**:一条参数非法的请求不该去抢
        # 全局 market 锁(那正是恶意刷单最省力的放大手段)。
        # 校验本身是纯函数,submitter 里还会再跑一次,不产生副作用。
        validate_submit(
            owner_id=owner_id,
            market_id=market_id,
            item_config_id=item_config_id,
            quantity=quantity,
            price=price,
            idem_key=idem_key,
            max_quantity=self._cfg.max_quantity_per_order,
            max_price=self._cfg.max_price,
        )

        async with self._guard_market(market_id):
            outcome = await self._submitter.submit(
                owner_id=owner_id,
                side=side,
                market_id=market_id,
                item_config_id=item_config_id,
                quantity=quantity,
                price=price,
                idem_key=idem_key,
                now_ms=now_ms(),
            )
            rec = outcome.record
            if outcome.finalize:
                await self._settle_book_and_release(rec)
                self._push_audit(to_proto_order(rec))
            return to_proto_order(rec)

    async def _settle_book_and_release(self, rec: OrderRecord) -> None:
        if arepo.remaining_of(rec) > 0:
            await self._add_book_cache(rec)
        else:
            await self._remove_book_cache(rec)
            await self._try_release_order(rec.market_id, rec.order_id)

    # ── 撮合 ──────────────────────────────────────────────────────────────

    async def _match(self, incoming: OrderRecord) -> None:
        """让 incoming 与同一具体物品的对手盘逐笔撮合。调用方已持 market 锁。"""
        opp = opposite(incoming.side)
        conflicts = 0
        while arepo.remaining_of(incoming) > 0:
            resting, found = await self._repo.find_best_active_order(
                incoming.market_id, incoming.item_config_id, opp, incoming.owner_id
            )
            if not found or resting is None:
                break
            if not crosses(incoming.side, incoming.price, resting.price):
                break
            (
                m,
                updated_incoming,
                updated_resting,
                reserved,
            ) = await self._repo.reserve_match(
                incoming.market_id,
                incoming.order_id,
                resting.order_id,
                self._match_sf.generate(),
                now_ms(),
            )
            if updated_incoming is not None:
                _copy_into(incoming, updated_incoming)
            if not reserved:
                # 候选在 SELECT 与 FOR UPDATE 之间被别的实例消费。
                # 有限重试防"异常替身 / 热点竞争"忙等成死循环。
                conflicts += 1
                if arepo.remaining_of(incoming) <= 0 or is_terminal(incoming.status):
                    break
                if conflicts >= MAX_RESERVE_CONFLICTS:
                    plog.get().warning(
                        "auction_reserve_conflicts_exhausted",
                        market_id=incoming.market_id,
                        order_id=incoming.order_id,
                        conflicts=conflicts,
                    )
                    raise errcode.PandoraError(
                        errcode.ErrAuctionMarketBusy,
                        "too many concurrent reserve conflicts market=%d",
                        incoming.market_id,
                    )
                continue
            conflicts = 0
            if m is not None and await self._settle_match(m):
                await self._try_release_order(m.market_id, m.sell_order_id)
                await self._try_release_order(m.market_id, m.buy_order_id)
            resting = updated_resting
            for terminal in (incoming, resting):
                if terminal is not None and is_terminal(terminal.status):
                    await self._remove_book_cache(terminal)
                    await self._release_owner_slot(terminal)
            if resting is not None:
                self._push_audit(to_proto_order(resting))
        if incoming.match_pending and incoming.status in (STATUS_OPEN, STATUS_PARTIAL):
            # 一次主动撮合完整扫到"无交叉候选"才准清 marker ——
            # 提前清会让一张仍有对手盘的 PARTIAL 单再也无人续跑。
            await self._repo.clear_match_pending(incoming.market_id, incoming.order_id)
            incoming.match_pending = False

    async def _settle_match(self, m: arepo.MatchRecord) -> bool:
        """执行已持久化成交的外部结算。失败时保留 PENDING,由后台以 match_id 幂等重试。"""
        try:
            await asyncio.wait_for(
                self._ledger.settle(m), timeout=LEDGER_ATTEMPT_TIMEOUT_SEC
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().error("auction_settle_pending", match_id=m.match_id, err=str(exc))
            await self._defer(self._repo.defer_match_settlement, m.market_id, m.match_id,
                              "auction_defer_settlement_failed", match_id=m.match_id)
            return False
        try:
            completed = await self._repo.complete_match(m.market_id, m.match_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # Settle 已成功但状态仍 PENDING:下一轮重复 Settle 安全(match_id 是幂等键)。
            plog.get().error(
                "auction_complete_match_failed", match_id=m.match_id, err=str(exc)
            )
            await self._defer(self._repo.defer_match_settlement, m.market_id, m.match_id,
                              "auction_defer_settlement_failed", match_id=m.match_id)
            return False
        if not completed:
            # 其他实例可能已完成同一 PENDING 快照;Settle 本身以 match_id 幂等,
            # 视为已收敛,但**只有条件更新成功者**发送成交事件。
            return True
        m.settlement_status = arepo.SETTLEMENT_COMPLETED
        m.event_pending = True
        return True

    async def _publish_match_event(self, m: arepo.MatchRecord) -> bool:
        """投递已由 MySQL outbox marker 登记的成交事件。

        ★ 发送成功后清 marker 前退出只会**重复**同一 match_id(至少一次),
        不再产生确定性丢失。反过来"先清 marker 再发"会把丢事件变成确定行为。
        """
        if self._events is not None:
            try:
                await asyncio.wait_for(
                    self._events.push_match(to_proto_match(m)),
                    timeout=LEDGER_ATTEMPT_TIMEOUT_SEC,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "auction_match_push_pending", match_id=m.match_id, err=str(exc)
                )
                await self._defer(self._repo.defer_match_event, m.market_id, m.match_id,
                                  "auction_defer_match_event_failed", match_id=m.match_id)
                return False
        try:
            await asyncio.wait_for(
                self._repo.clear_match_event_pending(m.market_id, m.match_id),
                timeout=MARKER_WRITE_TIMEOUT_SEC,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "auction_match_event_clear_failed", match_id=m.match_id, err=str(exc)
            )
            await self._defer(self._repo.defer_match_event, m.market_id, m.match_id,
                              "auction_defer_match_event_failed", match_id=m.match_id)
            return False
        return True

    async def _defer(self, fn, market_id: int, entity_id: int, event: str, **fields) -> None:  # noqa: ANN001
        """持久化一次退避时间。用独立短超时:请求取消不能让永久失败记录继续占住批次。"""
        try:
            await asyncio.wait_for(
                fn(market_id, entity_id, now_ms() + SIDE_EFFECT_RETRY_DELAY_MS),
                timeout=MARKER_WRITE_TIMEOUT_SEC,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().error(event, err=str(exc), market_id=market_id, **fields)

    # ── owner 名额 ────────────────────────────────────────────────────────

    async def reserve_owner_slot_pruning(self, o: OrderRecord) -> None:
        """先从 MySQL 权威库有界读取该玩家最老的 active/PENDING 订单,再原子预热
        Redis 配额索引并预留当前订单。

        ★ 预热(Sync)不能省:升级后首次新写若只看 Redis,legacy 活跃单还没建索引,
        玩家会**暂时突破硬上限**(而且是静默的)。
        ★ 触顶时清理终态 / 已不存在成员后**只重试一次**:无限重试会让一个卡住的
        清理把请求线程一直占着。
        """
        if self._slots is None:
            raise errcode.PandoraError(errcode.ErrInternal, "owner slot limiter not configured")
        max_slots = self._cfg.max_active_orders_per_player
        slot = OwnerOrderSlot(market_id=o.market_id, order_id=o.order_id)
        for attempt in range(2):
            authoritative = await self._repo.list_owner_active_and_pending(
                o.owner_id, max_slots + 1
            )
            legacy = [
                OwnerOrderSlot(market_id=e.market_id, order_id=e.order_id)
                for e in authoritative
            ]
            await self._slots.sync(o.owner_id, legacy, max_slots)
            if await self._slots.reserve(o.owner_id, slot, max_slots):
                return
            if attempt == 0 and await self._prune_owner_slots(o.owner_id) > 0:
                continue
            break
        raise errcode.PandoraError(
            errcode.ErrAuctionOrderLimit,
            "player %d has too many active auction orders (max %d)",
            o.owner_id,
            max_slots,
        )

    async def _prune_owner_slots(self, owner_id: int) -> int:
        slots = await self._slots.list(owner_id, self._cfg.max_active_orders_per_player)
        pruned = 0
        for slot in slots:
            try:
                o, found = await self._repo.get_order(slot.market_id, slot.order_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                # ★ 状态查询不确定时**绝不释放名额**(fail-closed)。
                # 释放一个其实还活着的名额 = 配额被穿透,而且没人会发现。
                plog.get().debug(
                    "auction_prune_order_read_failed",
                    owner_id=owner_id,
                    market_id=slot.market_id,
                    order_id=slot.order_id,
                    err=str(exc),
                )
                continue
            if found and o is not None:
                if o.status in (STATUS_PENDING, STATUS_OPEN, STATUS_PARTIAL):
                    continue
                if not is_terminal(o.status):
                    # 未知新状态也 fail-closed,避免未来状态演进误清活跃订单。
                    continue
            try:
                await self._slots.release(owner_id, slot)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "auction_owner_slot_prune_failed",
                    owner_id=owner_id,
                    market_id=slot.market_id,
                    order_id=slot.order_id,
                    err=str(exc),
                )
                continue
            pruned += 1
        if pruned > 0:
            plog.get().info("auction_owner_slots_pruned", owner_id=owner_id, pruned=pruned)
        return pruned

    async def release_owner_slot_raw(self, o: OrderRecord) -> None:
        """释放名额,失败原样上抛(由调用方决定怎么记)。"""
        if self._slots is None or o.owner_id == 0 or o.market_id == 0 or o.order_id == 0:
            return
        await asyncio.wait_for(
            self._slots.release(
                o.owner_id, OwnerOrderSlot(market_id=o.market_id, order_id=o.order_id)
            ),
            timeout=OWNER_SLOT_WRITE_TIMEOUT_SEC,
        )

    async def _release_owner_slot(self, o: OrderRecord | None) -> None:
        if o is None:
            return
        try:
            await self.release_owner_slot_raw(o)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "auction_owner_slot_release_failed",
                owner_id=o.owner_id,
                market_id=o.market_id,
                order_id=o.order_id,
                err=str(exc),
            )

    async def _reject_pending_after_slot_failure(self, o: OrderRecord) -> None:
        """条件取消尚未冻结/激活的 PENDING(后台恢复路径用)。"""
        changed = await self._repo.reject_pending_order(o.market_id, o.order_id, now_ms())
        if not changed:
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "pending order %d could not be rejected after slot failure",
                o.order_id,
            )
        o.status = STATUS_CANCELED
        o.release_pending = True
        await self._release_owner_slot(o)
        await self._try_release_order(o.market_id, o.order_id)

    # ── 订单簿兼容缓存(弱依赖)────────────────────────────────────────

    async def _add_book_cache(self, o: OrderRecord) -> None:
        if self._book is None:
            return
        try:
            await asyncio.wait_for(
                self._book.add(o.market_id, o.side, o.order_id, o.price),
                timeout=BOOK_CACHE_WRITE_TIMEOUT_SEC,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 缓存失败绝不能影响权威撮合
            plog.get().debug(
                "auction_book_cache_add_failed",
                market_id=o.market_id,
                order_id=o.order_id,
                err=str(exc),
            )

    async def _remove_book_cache(self, o: OrderRecord) -> None:
        if self._book is None:
            return
        try:
            await asyncio.wait_for(
                self._book.remove(o.market_id, o.side, o.order_id),
                timeout=BOOK_CACHE_WRITE_TIMEOUT_SEC,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().debug(
                "auction_book_cache_remove_failed",
                market_id=o.market_id,
                order_id=o.order_id,
                err=str(exc),
            )

    # ── audit(弱依赖,有界队列)──────────────────────────────────────

    def _push_audit(self, order: auction_pb2.AuctionOrder) -> None:
        """只做有界、非阻塞入队;绝不在持有 market 锁时碰 Kafka。"""
        if self._audit_closed:
            return
        copy = auction_pb2.AuctionOrder()
        copy.CopyFrom(order)
        try:
            self._audit_queue.put_nowait(copy)
        except asyncio.QueueFull:
            # broker 慢 / 挂时队列持续打满,逐条打会按 QPS 洪泛。
            # 限流后首条即带累计丢弃数,比逐条更能回答"丢了多少 audit"。
            total = self._audit_drop_log.add_extra(1)
            ok, streak = self._audit_drop_log.admit(now_ms(), DEGRADE_LOG_WINDOW_MS)
            if ok:
                plog.get().warning(
                    "auction_audit_queue_full_drop",
                    order_id=order.order_id,
                    capacity=self._cfg.audit_queue_capacity,
                    dropped_total=total,
                    streak=streak,
                )
            return
        failed, dropped = self._audit_drop_log.recovered()
        if failed > 0:
            plog.get().info(
                "auction_audit_queue_recovered", dropped_total=dropped, windows=failed
            )

    async def _run_audit_worker(self) -> None:
        while True:
            order = await self._audit_queue.get()
            if self._events is None:
                continue
            try:
                await asyncio.wait_for(
                    self._events.push_audit(order), timeout=AUDIT_DISPATCH_TIMEOUT_SEC
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 单条失败不许弄死 worker
                plog.get().warning(
                    "auction_audit_push_failed", order_id=order.order_id, err=str(exc)
                )

    # ── 撤单 / 过期 ───────────────────────────────────────────────────────

    async def cancel_order(self, owner_id: int, market_id: int, order_id: int) -> None:
        if self._cfg.passive_warmup:
            raise errcode.PandoraError(errcode.ErrUnavailable, "auction is in passive warmup")
        if owner_id == 0 or market_id == 0 or order_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "owner / market / order required")
        # 频率配额:下撤循环两端都限,只限一端等于没限。
        await self._allow_action("cancel", owner_id)

        async with self._guard_market(market_id):
            o, found = await self._repo.get_order(market_id, order_id)
            if not found or o is None:
                raise errcode.PandoraError(
                    errcode.ErrAuctionOrderNotFound, "order %d not found", order_id
                )
            if o.owner_id != owner_id:
                raise errcode.PandoraError(
                    errcode.ErrAuctionNotOwner,
                    "player %d not owner of order %d",
                    owner_id,
                    order_id,
                )
            if is_terminal(o.status):
                raise errcode.PandoraError(
                    errcode.ErrAuctionWrongState, "order %d already terminal", order_id
                )
            if o.status not in (STATUS_OPEN, STATUS_PARTIAL):
                raise errcode.PandoraError(
                    errcode.ErrAuctionWrongState, "order %d is not active", order_id
                )
            changed = await self._repo.mark_order_terminal(
                market_id, order_id, STATUS_CANCELED, now_ms()
            )
            if not changed:
                raise errcode.PandoraError(
                    errcode.ErrAuctionWrongState, "order %d changed concurrently", order_id
                )
            o.status = STATUS_CANCELED
            o.release_pending = True
            o.updated_at_ms = now_ms()
            await self._remove_book_cache(o)
            await self._release_owner_slot(o)
            # 终态与 release_pending 已原子提交;账本失败只延迟释放,不会丢失补偿意图。
            await self._try_release_order(market_id, order_id)
            self._push_audit(to_proto_order(o))

    async def expire_due_orders(self) -> int:
        """清扫一批已过期仍未成交的挂单(置 EXPIRED、移出簿、退还 escrow)。"""
        if self._cfg.passive_warmup:
            raise errcode.PandoraError(errcode.ErrUnavailable, "auction is in passive warmup")
        if self._cfg.order_ttl_seconds <= 0:
            return 0
        cutoff = now_ms() - self._cfg.order_ttl_seconds * 1000
        batch = self._cfg.expiry_sweep_batch or 200
        due = await self._repo.list_expirable_orders(cutoff, batch)
        done = 0
        for o in due:
            try:
                await self._expire_one(o.market_id, o.order_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 单条失败不阻断整批
                plog.get().warning(
                    "auction_expire_one_failed",
                    market_id=o.market_id,
                    order_id=o.order_id,
                    err=str(exc),
                )
                continue
            done += 1
        return done

    async def _expire_one(self, market_id: int, order_id: int) -> None:
        async with self._guard_market(market_id):
            # 持锁后**重读**:可能已被撮合 / 撤单到终态,避免误改。
            o, found = await self._repo.get_order(market_id, order_id)
            if not found or o is None or is_terminal(o.status):
                return
            changed = await self._repo.mark_order_terminal(
                market_id, order_id, STATUS_EXPIRED, now_ms()
            )
            if not changed:
                return
            o.status = STATUS_EXPIRED
            o.release_pending = True
            o.updated_at_ms = now_ms()
            await self._remove_book_cache(o)
            await self._release_owner_slot(o)
            await self._try_release_order(market_id, order_id)
            self._push_audit(to_proto_order(o))

    # ── 查询 ──────────────────────────────────────────────────────────────

    async def list_market(
        self, market_id: int, side: int, limit: int
    ) -> list[auction_pb2.AuctionOrder]:
        if market_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "market_id required")
        if limit <= 0:
            limit = self._cfg.default_list_limit
        if limit > self._cfg.max_list_limit:
            limit = self._cfg.max_list_limit
        out: list[auction_pb2.AuctionOrder] = []
        # side=UNSPECIFIED(0)→ 返回买 + 卖两侧(与 Go 同)。
        if side in (SIDE_SELL, 0):
            recs = await self._repo.list_market_orders(market_id, SIDE_SELL, limit)
            out.extend(to_proto_order(r) for r in recs)
        if side in (SIDE_BUY, 0):
            recs = await self._repo.list_market_orders(market_id, SIDE_BUY, limit)
            out.extend(to_proto_order(r) for r in recs)
        return out

    async def list_my_orders(
        self, owner_id: int, active_only: bool, cursor_order_id: int, limit: int
    ) -> tuple[list[auction_pb2.AuctionOrder], int, bool]:
        if owner_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "owner required")
        limit = _clamp_my_orders_limit(limit)
        # 多取一条判断是否还有下一页(与 Go 同)。
        recs = await self._repo.list_owner_orders(
            owner_id, active_only, cursor_order_id, limit + 1
        )
        has_more = len(recs) > limit
        if has_more:
            recs = recs[:limit]
        next_cursor = recs[-1].order_id if (has_more and recs) else 0
        return [to_proto_order(r) for r in recs], next_cursor, has_more

    # ── 释放补偿 ──────────────────────────────────────────────────────────

    async def _release_order(self, market_id: int, order_id: int) -> bool:
        """只处理 MySQL 已确认「无待结算成交引用」的终态订单。"""
        o, found = await self._repo.get_releasable_order(market_id, order_id)
        if not found or o is None:
            return False
        # 订单已由 MySQL 确认为终态,owner 配额与外部 escrow 释放可独立收敛。
        await self._release_owner_slot(o)
        try:
            await asyncio.wait_for(
                self._ledger.release(o.owner_id, o.order_id),
                timeout=LEDGER_ATTEMPT_TIMEOUT_SEC,
            )
        except asyncio.CancelledError:
            raise
        except BaseException:  # noqa: BLE001
            await self._defer(self._repo.defer_order_release, market_id, order_id,
                              "auction_defer_release_failed", order_id=order_id)
            raise
        try:
            return await self._repo.clear_release_pending(market_id, order_id)
        except asyncio.CancelledError:
            raise
        except BaseException:  # noqa: BLE001
            await self._defer(self._repo.defer_order_release, market_id, order_id,
                              "auction_defer_release_failed", order_id=order_id)
            raise

    async def _try_release_order(self, market_id: int, order_id: int) -> None:
        try:
            await self._release_order(market_id, order_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().error(
                "auction_release_pending",
                market_id=market_id,
                order_id=order_id,
                err=str(exc),
            )

    # ── 后台恢复(补偿链)────────────────────────────────────────────────

    async def _recover_pending_order(self, snapshot: OrderRecord) -> None:
        """恢复 Claim 后进程退出留下的内部 PENDING。

        即使客户端永不重试,后台也会用**同一个 order_id** 幂等 Freeze 后激活;
        Freeze 返回任何错误都按"结果不确定"处理为终态并补 Release。
        """
        async with self._guard_market(snapshot.market_id):
            o, found = await self._repo.get_order(snapshot.market_id, snapshot.order_id)
            if not found or o is None or o.status != STATUS_PENDING:
                return
            # 后台恢复也必须先占有 owner slot,不能因客户端不再重试而绕过硬上限。
            try:
                await self.reserve_owner_slot_pruning(o)
            except asyncio.CancelledError:
                raise
            except errcode.PandoraError as exc:
                await self._reject_pending_after_slot_failure(o)
                plog.get().warning(
                    "auction_pending_order_rejected_after_slot_failure",
                    order_id=o.order_id,
                    err=str(exc),
                )
                return
            try:
                await asyncio.wait_for(
                    self._ledger.freeze(
                        owner_id=o.owner_id,
                        order_id=o.order_id,
                        side=o.side,
                        item_config_id=o.item_config_id,
                        quantity=o.quantity,
                        price=o.price,
                    ),
                    timeout=LEDGER_ATTEMPT_TIMEOUT_SEC,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                changed = await self._repo.reject_pending_order(
                    o.market_id, o.order_id, now_ms()
                )
                if changed:
                    await self._release_owner_slot(o)
                    await self._release_order(o.market_id, o.order_id)
                plog.get().warning(
                    "auction_pending_order_rejected_after_freeze_error",
                    order_id=o.order_id,
                    err=str(exc),
                )
                return
            confirmed = await self._repo.confirm_order_escrow(
                o.market_id, o.order_id, now_ms()
            )
            if not confirmed:
                raise errcode.PandoraError(
                    errcode.ErrInternal, "confirm recovered escrow lost order %d", o.order_id
                )
            o.escrow_verified = True
            await self._match(o)
            if o.status == STATUS_PENDING:
                if not await self._repo.activate_order(o.market_id, o.order_id, now_ms()):
                    return
                o.status = STATUS_OPEN
                o.updated_at_ms = now_ms()
            await self._settle_book_and_release(o)
            self._push_audit(to_proto_order(o))

    async def _recover_unverified_active_order(self, snapshot: OrderRecord) -> None:
        """验证 / 补冻旧二进制遗留的 OPEN/PARTIAL。

        ★ 只有 inventory 确认剩余托管充足后才置 verified 并主动续跑撮合;
        **确定性不一致**(资产真的不够 / 幂等冲突 / 参数非法)才取消并幂等退还残余。
        把"暂时失败"也当成确定性不一致会误杀正常订单,反过来则永远收敛不了。
        """
        async with self._guard_market(snapshot.market_id):
            o, found = await self._repo.get_order(snapshot.market_id, snapshot.order_id)
            if (
                not found
                or o is None
                or o.escrow_verified
                or o.status not in (STATUS_OPEN, STATUS_PARTIAL)
            ):
                return
            try:
                await self.reserve_owner_slot_pruning(o)
            except asyncio.CancelledError:
                raise
            except errcode.PandoraError as exc:
                if exc.code != errcode.ErrAuctionOrderLimit:
                    raise
                await self._cancel_unverified_order(o, exc)
                return
            try:
                await asyncio.wait_for(
                    self._ledger.ensure(
                        owner_id=o.owner_id,
                        order_id=o.order_id,
                        side=o.side,
                        item_config_id=o.item_config_id,
                        remaining=arepo.remaining_of(o),
                        price=o.price,
                    ),
                    timeout=LEDGER_ATTEMPT_TIMEOUT_SEC,
                )
            except asyncio.CancelledError:
                raise
            except errcode.PandoraError as exc:
                if exc.code in (
                    errcode.ErrAuctionInsufficient,
                    errcode.ErrInventoryInsufficient,
                    errcode.ErrInventoryIdempotencyConflict,
                    errcode.ErrInvalidArg,
                ):
                    await self._cancel_unverified_order(o, exc)
                    return
                raise
            confirmed = await self._repo.confirm_order_escrow(
                o.market_id, o.order_id, now_ms()
            )
            if not confirmed:
                raise errcode.PandoraError(
                    errcode.ErrInternal, "confirm legacy escrow lost order %d", o.order_id
                )
            o.escrow_verified = True
            o.match_pending = True
            await self._match(o)
            await self._settle_book_and_release(o)
            self._push_audit(to_proto_order(o))

    async def _cancel_unverified_order(self, o: OrderRecord, cause: BaseException) -> None:
        changed = await self._repo.mark_order_terminal(
            o.market_id, o.order_id, STATUS_CANCELED, now_ms()
        )
        if not changed:
            return
        o.status = STATUS_CANCELED
        o.escrow_verified = False
        o.match_pending = False
        o.release_pending = True
        await self._remove_book_cache(o)
        await self._release_owner_slot(o)
        await self._try_release_order(o.market_id, o.order_id)
        self._push_audit(to_proto_order(o))
        plog.get().warning(
            "auction_legacy_order_canceled", order_id=o.order_id, err=str(cause)
        )

    async def _recover_match_pending_order(self, snapshot: OrderRecord) -> None:
        """续跑 Reserve 事务已提交、但 incoming 尚未把交叉对手盘扫尽的订单。"""
        async with self._guard_market(snapshot.market_id):
            o, found = await self._repo.get_order(snapshot.market_id, snapshot.order_id)
            if (
                not found
                or o is None
                or not o.match_pending
                or o.status not in (STATUS_OPEN, STATUS_PARTIAL)
            ):
                return
            await self._match(o)
            await self._settle_book_and_release(o)
            self._push_audit(to_proto_order(o))

    async def reconcile_pending_side_effects(self) -> tuple[int, int, BaseException | None]:
        """补齐事务提交后未完成的账本副作用。可由多实例并发调用。

        Settle(match_id)、Release(order_id) 都幂等,Complete/Clear 又是条件更新,
        因此不会重复转资。

        ★ 失败**按阶段累加、轮末汇总一条**,不逐条打 Error:
        inventory / MySQL 故障时每分片每轮最多 batch(默认 100)条全失败,
        且每轮对同一批 backlog 重打 —— 逐条打会把 Loki 刷爆,反而看不出是哪一环卡住。
        """
        if self._cfg.passive_warmup:
            raise errcode.PandoraError(errcode.ErrUnavailable, "auction is in passive warmup")
        batch = self._cfg.side_effect_reconcile_batch or 100
        settled = 0
        released = 0
        counters = {
            "release_failed": 0,
            "recover_pending_failed": 0,
            "recover_unverified_failed": 0,
            "recover_match_pending_failed": 0,
        }
        sample_order_id = 0
        first_err: BaseException | None = None

        def note(order_id: int, exc: BaseException, key: str) -> None:
            nonlocal sample_order_id, first_err
            counters[key] += 1
            if sample_order_id == 0:
                sample_order_id = order_id
            if first_err is None:
                first_err = exc

        try:
            for o in await self._repo.list_terminal_orders_for_repair(batch):
                try:
                    if await self._repo.repair_terminal_markers(o.market_id, o.order_id):
                        await self._release_owner_slot(o)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    if first_err is None:
                        first_err = exc

            # ★ 已就绪的终态 escrow **优先**释放。放在末尾的话,后面每条 legacy /
            # Settle 都可能等外部 inventory,大批慢请求会让本可立即归还的资产延迟数分钟。
            for o in await self._repo.list_releasable_orders(batch):
                try:
                    if await self._release_order(o.market_id, o.order_id):
                        released += 1
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    note(o.order_id, exc, "release_failed")

            for o in await self._repo.list_pending_orders(batch):
                try:
                    await self._recover_pending_order(o)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    await self._defer(self._repo.defer_order_reconcile, o.market_id,
                                      o.order_id, "auction_defer_order_reconcile_failed",
                                      order_id=o.order_id)
                    note(o.order_id, exc, "recover_pending_failed")

            for o in await self._repo.list_unverified_active_orders(batch):
                try:
                    await self._recover_unverified_active_order(o)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    await self._defer(self._repo.defer_order_reconcile, o.market_id,
                                      o.order_id, "auction_defer_order_reconcile_failed",
                                      order_id=o.order_id)
                    note(o.order_id, exc, "recover_unverified_failed")

            for o in await self._repo.list_match_pending_orders(batch):
                try:
                    await self._recover_match_pending_order(o)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    await self._defer(self._repo.defer_order_reconcile, o.market_id,
                                      o.order_id, "auction_defer_order_reconcile_failed",
                                      order_id=o.order_id)
                    note(o.order_id, exc, "recover_match_pending_failed")

            for m in await self._repo.list_pending_matches(batch):
                if await self._settle_match(m):
                    settled += 1
                    for order_id in (m.sell_order_id, m.buy_order_id):
                        try:
                            if await self._release_order(m.market_id, order_id):
                                released += 1
                        except asyncio.CancelledError:
                            raise
                        except BaseException as exc:  # noqa: BLE001
                            note(order_id, exc, "release_failed")
                elif first_err is None:
                    first_err = errcode.PandoraError(
                        errcode.ErrInternal,
                        "match %d side effect remains pending",
                        m.match_id,
                    )
        finally:
            total = sum(counters.values())
            if total:
                plog.get().error(
                    "auction_side_effect_reconcile_failed",
                    batch=batch,
                    failed_total=total,
                    sample_order_id=sample_order_id,
                    first_err=str(first_err),
                    **counters,
                )
        # ★ 返回错误而不是抛:Go 侧同样是 (settled, released, err) —— 一轮里
        # 既有成功也有失败是常态,抛出去会让本轮已经完成的补偿数在日志里消失,
        # 而那正是判断"补偿链有没有在推进"的唯一依据。
        return settled, released, first_err

    async def reconcile_pending_match_events(self) -> tuple[int, BaseException | None]:
        """独立消费成交 outbox。

        ★ 它**不能**与资产补偿共用一条循环:底层同步 producer 无法被取消可靠中断,
        broker 故障可能阻塞很久 —— 共用会让下一轮 Settle/Release/legacy 恢复
        跟着一起停摆(而那些恰恰是玩家资产还锁着的路径)。
        """
        if self._cfg.passive_warmup:
            raise errcode.PandoraError(errcode.ErrUnavailable, "auction is in passive warmup")
        batch = self._cfg.side_effect_reconcile_batch or 100
        published = 0
        first_err: BaseException | None = None
        for m in await self._repo.list_pending_match_events(batch):
            if await self._publish_match_event(m):
                published += 1
            elif first_err is None:
                first_err = errcode.PandoraError(
                    errcode.ErrInternal, "match %d event remains pending", m.match_id
                )
        return published, first_err


def _clamp_my_orders_limit(limit: int) -> int:
    if limit <= 0:
        return _MY_ORDERS_DEFAULT_LIMIT
    if limit > _MY_ORDERS_MAX_LIMIT:
        return _MY_ORDERS_MAX_LIMIT
    return limit
