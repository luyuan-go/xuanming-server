"""trade 业务逻辑层 —— 对应 Go 侧 internal/biz(trade.go + trade_settlement.go)。

状态机(INC-20260722-001 修订:结算意图先落库):

    PENDING ──买方确认──▶ BUYER_CONFIRMED ──卖方确认──▶ SELLER_CONFIRMED ──结算成功──▶ COMPLETED
       │                       │                            │
       │                       │                            └──余额/物品不足──▶ FAILED
       └──任一方 Cancel────────┴──▶ CANCELED   (SELLER_CONFIRMED 拒 Cancel/过期:结算围栏)
       └──超时(惰性)──────────────▶ EXPIRED

★ 这个服务的核心是一次真实事故的修复,移植时**不能**"简化"回事故前的形状:

    事故根因:Settle(不可回滚的跨服务资产转移)曾放在 Redis WATCH 回调里执行。
    WATCH 回调的语义是"可能不提交、也可能重跑" —— 于是 EXEC 冲突或进程崩溃时,
    资产已经转移而订单还能被并发 Cancel 成 CANCELED,账本与订单永久撕裂。

    修复:卖方确认时**先把 SELLER_CONFIRMED 经 WATCH/EXEC 原子提交**(= 本订单进入
    结算通道的线性化点),提交成功后才在**锁外**调用 Settle。SELLER_CONFIRMED 从此
    是结算围栏:Cancel / 惰性过期 / 配额清理一律 fail-closed,订单只向
    COMPLETED / FAILED 收敛。

    恢复路径:结算窗口内进程退出 / Settle 瞬时失败 / 终态写失败 → 订单停留
    SELLER_CONFIRMED,任一方重试 Confirm 幂等重新驱动(Settle 幂等键 = order_id),
    **无需回滚**。

不变量 §9.7(交易资源扣减必须原子 + 有补偿幂等键)由 ResourceLedger 承载,
幂等键恒为 order_id。
"""

from __future__ import annotations

import asyncio

from typing import Protocol

from pandora.trade.v1 import trade_pb2

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy.protoenum import enum_name

# 分页上限(decision-revisit-list-pagination.md)。
DEFAULT_PAGE_LIMIT = 50
MAX_PAGE_LIMIT = 100

_S = trade_pb2.OrderState


def _with_state(exc: errcode.PandoraError, state: int) -> errcode.PandoraError:
    """给异常挂上"订单现在停在哪",返回同一个异常对象供 `raise` 用。

    Go 的 `ConfirmOrder` 是 `(state, err)` 双返回,**失败路径上 state 仍然有效**;
    Python 用异常传播,不显式带上就静默丢成 0。0 在 proto 里是
    ORDER_STATE_UNSPECIFIED,与"服务端不知道"无法区分 —— 而这两件事对客户端
    是相反的指令(重试 vs 放弃)。四处对应关系见 errcode.PandoraError.order_state 的注释。

    走 slots 而不是 setattr:拼错字段名时 setattr 照样成功,读的那侧永远拿默认值。
    """
    exc.order_state = state
    return exc


# 终态集合:不可再流转。
_TERMINAL = frozenset(
    {
        _S.ORDER_STATE_COMPLETED,
        _S.ORDER_STATE_FAILED,
        _S.ORDER_STATE_EXPIRED,
        _S.ORDER_STATE_CANCELED,
    }
)


def now_ms() -> int:
    import time

    return int(time.time() * 1000)


class ResourceLedger(Protocol):
    """「原子扣减交易双方资源 + 幂等」的账本操作(不变量 §9.7)。

    settle 在卖方确认、订单进入 COMPLETED 前调用:卖方物品转买方、买方货币转卖方。
    idempotency_key = order_id 保证同一订单重复结算只生效一次。
    余额 / 物品不足抛 ErrTradeInsufficient,biz 将订单置 FAILED。
    """

    async def settle(self, order, idempotency_key: int) -> None: ...


class NoopResourceLedger:
    """占位实现:总是结算成功(**不真实扣转**背包 / 货币)。

    仅供联调 / 单测。★ 闸在 `TradeUsecase.__init__`:不显式设 `allow_noop_ledger=true`
    就根本构造不出来(而不是"生产由 main 强制" —— Python 侧 trade 没有 main,
    那句话曾是一句不成立的承诺,见构造函数注释)。
    """

    async def settle(self, order, idempotency_key: int) -> None:  # noqa: D102
        return None


class TradeAuditPusher(Protocol):
    """把订单流转快照发 kafka pandora.trade.audit(弱依赖,None 静默)。"""

    async def push_audit(self, order) -> None: ...


class ActionRateQuota(Protocol):
    """下单/撤单的 per-player 频率配额(anti-abuse §6 第 6 项)。

    总量闸(max_orders_per_player)只限「同时挂多少」,挡不住「下单-撤单-再下单」
    的循环(每轮产生托管写 + 流水行),频率配额补这一维。
    背压非权威门:判定 error 时调用方 fail-open 放行。

    ★ 返回 `(是否放行, 故障)`,与 Go 的 `Allow(...) (bool, error)` 同形 ——
    **不是**裸 bool。Protocol 运行期不做检查,照着 `-> bool` 写实现的话测试照样绿,
    而 `_allow_action` 的 `allowed, quota_exc = await ...` 会在每次下单/撤单上抛
    `TypeError: cannot unpack non-iterable bool object`。
    """

    async def allow(self, action: str, subject: int) -> tuple[bool, Exception | None]: ...


class TradeUsecase:
    """trade 业务逻辑核心。对应 Go 的 biz.TradeUsecase。"""

    __slots__ = ("_repo", "_ledger", "_audit", "_sf", "_cfg", "_router", "_rate_quota")

    def __init__(self, repo, ledger, audit, snowflake, cfg) -> None:
        self._repo = repo
        # ★ 没有真实账本时**必须显式授权**才允许退回 Noop,否则当场拒绝构造。
        #
        # 这道闸原先只存在于 Go 的 main.go,而 Python 侧 trade **没有 main** ——
        # docstring 却写着"生产由 main 强制 fail-fast",于是它是一句不成立的承诺:
        # 哪天有人写 trade/main.py 忘了接账本,交易会**成交但不扣转**,
        # 而订单状态、审计流水、客户端提示全都显示成功。
        #
        # 所以闸下沉到构造函数:忘记接账本在结构上不可能通过,不依赖任何人记得。
        if ledger is None and not getattr(cfg, "allow_noop_ledger", False):
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "trade: 未注入 ResourceLedger 且 allow_noop_ledger 未开启。"
                "Noop 账本会让成交**不真实扣转**背包 / 货币而一切显示成功 —— "
                "接真实 inventory,或在联调 / 单测里显式设 allow_noop_ledger=true",
            )
        if ledger is None:
            # 已经过了上面的闸(= 显式授权了),但**必须留痕**:对应 Go main.go 的
            # resource_ledger_noop WARN。没有这条日志的话,一个误开了
            # allow_noop_ledger 的环境从启动到出事之间零信号 ——
            # 而"成交不扣转"这种错,发现时通常已经产生了一批脏数据。
            plog.get().warning(
                "resource_ledger_noop",
                hint="allow_noop_ledger=true:成交**不会**真实扣转背包 / 货币,仅限联调与单测",
            )
        self._ledger = ledger if ledger is not None else NoopResourceLedger()
        self._audit = audit  # 允许 None(弱依赖)
        self._sf = snowflake
        self._cfg = cfg
        self._router = None
        self._rate_quota = None

    def set_cell_router(self, router) -> None:
        """注入 region/cell 路由器。None-safe(单 Cell / dev 不注入)。"""
        self._router = router

    def set_rate_quota(self, quota) -> None:
        """注入频率配额。不注入 = 不限(dev 联调兼容)。"""
        self._rate_quota = quota

    # ── 频率配额门 ────────────────────────────────────────────────────────────

    async def _allow_action(self, action: str, player_id: int) -> None:
        """窗内超额抛 ErrRateLimited(**先于一切副作用**);判定失败 fail-open。

        fail-open 是有意的:这是背压门不是权威门。配额存储抖动时把交易全关掉,
        等于把一个防刷工具变成故障开关(与 killswitch 同理)。
        """
        if self._rate_quota is None:
            return
        # ★ 读 `(ok, exc)` 而不是等异常:`allow()` 从不抛,原先那段
        #   `except ...: log("trade_rate_quota_check_failed")` 是**不可达死代码**,
        #   于是这个 Loki 键在代码里存在、永远不会触发。
        allowed, quota_exc = await self._rate_quota.allow(action, player_id)
        if quota_exc is not None:
            # 背压门判定失败一律放行(§9.20:限流不得卡玩家)。
            plog.get().warning(
                "trade_rate_quota_check_failed",
                action=action,
                player_id=player_id,
                err=str(quota_exc),
                fail_open=True,
            )
            return
        if not allowed:
            plog.get().warning(
                "trade_rate_quota_rejected", action=action, player_id=player_id
            )
            raise errcode.PandoraError(
                errcode.ErrRateLimited, "trade %s rate limited, retry later", action
            )

    # ── CreateOrder ──────────────────────────────────────────────────────────

    async def create_order(
        self, seller_id: int, buyer_id: int, items, buyer_items, price: int
    ) -> int:
        """卖方挂单。seller_id 由 service 从鉴权上下文得到(R5)。

        items = 卖家交付买家的道具(必填);buyer_items = 买家交付卖家的道具(可空 =
        纯金币购买);price = 买家付卖家的金币(>=0)。道具一律用 item_config_id。
        """
        if seller_id == 0 or buyer_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "seller / buyer required")
        if seller_id == buyer_id:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "cannot trade with self")
        # 频率配额按发起方(卖方)计,**先于一切副作用**。
        await self._allow_action("order", seller_id)

        items = list(items or [])
        buyer_items = list(buyer_items or [])
        if not items:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "items required")
        total = len(items) + len(buyer_items)
        if total > self._cfg.max_items_per_order:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "too many items: %d > %d",
                total,
                self._cfg.max_items_per_order,
            )
        for it in (*items, *buyer_items):
            if it.item_config_id == 0 or it.count <= 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "invalid item: config_id=%d count=%d",
                    it.item_config_id,
                    it.count,
                )
        # 价格双向闸。**两道都要**,原因两边不同:
        #
        # 下界:Go 侧 price 是 uint64,`price < 0` 恒为 false 所以已删;Python **没有类型保护**,
        #      biz 可以被进程内任何调用方传进真正的负数(既有用例正是这么测的),
        #      所以这道闸在 Python 侧仍然有效、必须保留。
        # 上界:proto 的 price 是 uint64,一个 price=-1 的老客户端 / 被篡改请求会被解成 1.8e19。
        #      没有上界就会一路落 Redis、占配额,直到 inventory 结算才被挡下。
        #      与 Go 侧 services/economy/trade/internal/biz/trade.go 的同名闸逐字对齐。
        if price < 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "price must be >= 0")
        if price > self._cfg.max_trade_price:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "price out of range: %d (max %d)",
                price,
                self._cfg.max_trade_price,
            )

        now = now_ms()
        order = trade_pb2.Order(
            order_id=self._sf.generate(),
            seller_id=seller_id,
            buyer_id=buyer_id,
            items=items,
            buyer_items=buyer_items,
            price=price,
            state=_S.ORDER_STATE_PENDING,
            created_at_ms=now,
            expires_at_ms=now + int(self._cfg.order_expire_td().total_seconds() * 1000),
        )

        # 写序铁律:① 先写订单主体(order_id 新发、无人引用)→ ② 原子预留双方索引配额。
        # 主体先落地保证「索引成员无主体 ≡ 真死成员」,配额清理绝不误删 in-flight 预留。
        # 任一步失败 → 回滚已预留名额 + 删主体,无残留。
        ttl = self._cfg.order_ttl_td()
        await self._repo.create_order(order, ttl)
        try:
            await self._reserve_slot_pruning(seller_id, order.order_id)
        except BaseException:
            await self._rollback_create(order.order_id)
            raise
        try:
            await self._reserve_slot_pruning(buyer_id, order.order_id)
        except BaseException:
            try:
                await self._repo.release_order_slot(seller_id, order.order_id)
            except Exception as exc:  # noqa: BLE001
                plog.get().warning(
                    "trade_release_slot_failed",
                    player_id=seller_id,
                    order_id=order.order_id,
                    err=str(exc),
                )
            await self._rollback_create(order.order_id)
            raise

        await self._push_audit(order)
        return order.order_id

    async def _rollback_create(self, order_id: int) -> None:
        """删掉刚写入、尚未对外返回的订单主体。

        失败仅 Warn:残留主体无索引指向、TTL 到期自收,无业务影响。
        """
        try:
            await self._repo.delete_order(order_id)
        except Exception as exc:  # noqa: BLE001
            plog.get().warning("trade_rollback_order_failed", order_id=order_id, err=str(exc))

    async def _reserve_slot_pruning(self, player_id: int, order_id: int) -> None:
        """预留一个索引名额;首次满员时先清死成员再重试一次,仍满抛 ErrTradeOrderLimit。

        死成员来源:终态/过期订单不实时移除索引(保留历史供 ListMyOrders 回看),
        配额满时才惰性回收 —— 正常玩家永远碰不到,遭恶意刷单的受害者取消后也能自愈。
        """
        ttl = self._cfg.order_ttl_td()
        for attempt in range(2):
            ok = await self._repo.reserve_order_slot(
                player_id, order_id, self._cfg.max_orders_per_player, ttl
            )
            if ok:
                return
            if attempt == 0:
                pruned = await self._prune_dead_order_slots(player_id)
                if pruned > 0:
                    continue
            break
        raise errcode.PandoraError(
            errcode.ErrTradeOrderLimit,
            "player %d has too many orders (max %d)",
            player_id,
            self._cfg.max_orders_per_player,
        )

    async def _prune_dead_order_slots(self, player_id: int) -> int:
        """清理「主体已被 Redis 回收」或「已进终态」的死成员,返回清理数。

        仅在配额满时调用,遍历规模被 max 硬上限钉住(默认 200)。
        安全性:create_order 先写主体后预留,in-flight 预留必有存活主体(PENDING),
        不会被误判。
        """
        ids = await self._repo.list_player_order_ids(player_id)
        pruned = 0
        for oid in ids:
            try:
                order = await self._repo.get_order(oid)
            except Exception as exc:  # noqa: BLE001
                # fail-closed 正确,但 redis 抖动时清理会静默退化为 no-op:玩家可能持续
                # 撞 ERR_TRADE_ORDER_LIMIT 而无任何线索指向「清理被读失败挡住」→ DEBUG 留证。
                plog.get().debug(
                    "trade_prune_order_read_failed",
                    player_id=player_id,
                    order_id=oid,
                    err=str(exc),
                )
                continue
            if order is not None:
                if order.state == _S.ORDER_STATE_SELLER_CONFIRMED:
                    continue  # 结算中(意图已落库,不过期):真占用,绝不回收
                stale = order.expires_at_ms > 0 and now_ms() >= order.expires_at_ms
                if order.state not in _TERMINAL and not stale:
                    continue  # 存活非终态且未过期 → 真占用
            try:
                await self._repo.release_order_slot(player_id, oid)
            except Exception as exc:  # noqa: BLE001
                plog.get().warning(
                    "trade_prune_slot_failed", player_id=player_id, order_id=oid, err=str(exc)
                )
                continue
            pruned += 1
        if pruned > 0:
            plog.get().info(
                "trade_pruned_dead_order_slots", player_id=player_id, pruned=pruned
            )
        return pruned

    # ── ConfirmOrder(两阶段确认 + 两步结算)──────────────────────────────────

    async def confirm_order(self, player_id: int, order_id: int) -> int:
        """确认订单。返回最新状态。player_id 由 service 从鉴权上下文得到(R5)。

        - 买方 + PENDING          → BUYER_CONFIRMED
        - 卖方 + BUYER_CONFIRMED  → **先原子提交 SELLER_CONFIRMED** → 锁外结算 → COMPLETED
        - 任一方 + SELLER_CONFIRMED → 幂等重新驱动结算(恢复路径)
        """
        if player_id == 0 or order_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player / order required")

        state = {"drive_settle": None, "expired": False}

        def mutate(order) -> None:
            # WATCH 重试会重跑本回调:先清残留,只信最后一次提交成功的快照。
            state["drive_settle"] = None
            if _expire_if_stale(order):
                # 惰性置 EXPIRED:**不抛异常**,让 update_with_lock 把 EXPIRED 写回 Redis。
                # (若抛错则不写回,过期态永远落不了库 —— Go 侧曾有这个 bug)
                state["expired"] = True
                return
            if player_id not in (order.seller_id, order.buyer_id):
                raise errcode.PandoraError(
                    errcode.ErrUnauthorized,
                    "player %d not party of order %d",
                    player_id,
                    order_id,
                )
            if player_id == order.buyer_id and order.state == _S.ORDER_STATE_PENDING:
                order.state = _S.ORDER_STATE_BUYER_CONFIRMED
                return
            if player_id == order.seller_id and order.state == _S.ORDER_STATE_BUYER_CONFIRMED:
                # ★ 结算意图先原子落库。本回调**只改状态、不做任何外部副作用**;
                # EXEC 成功 = 结算通道已 fencing(Cancel/过期从此拒),之后才在锁外 Settle。
                order.state = _S.ORDER_STATE_SELLER_CONFIRMED
                state["drive_settle"] = _clone(order)
                return
            if order.state == _S.ORDER_STATE_SELLER_CONFIRMED:
                # 恢复驱动:意图已落库,买卖任一方重试都可推进(Settle/终态 CAS 均幂等)。
                state["drive_settle"] = _clone(order)
                return
            raise errcode.PandoraError(
                errcode.ErrTradeWrongState,
                "player %d cannot confirm order %d in state %s",
                player_id,
                order_id,
                # ★ order.state 读自 Redis pb 快照。滚动升级时新副本可能已经写入一个
                # 旧副本不认识的 ORDER_STATE——而那正是会走到本分支的情况。
                # 裸 `.Name()` 会把一个干净的 ErrTradeWrongState 变成 ValueError。
                enum_name(_S, order.state),
            )

        await self._repo.update_with_lock(
            order_id, self._cfg.optimistic_retry, mutate, self._cfg.order_ttl_td()
        )

        # 惰性过期:EXPIRED 已在锁内写回,读回做 audit 并返回过期错误。
        if state["expired"]:
            order = await self._safe_get(order_id)
            if order is not None:
                await self._push_audit(order)
            # Go trade.go:351 在这条失败路径上仍回 EXPIRED —— 客户端据此知道
            # "不是网络问题,是订单已经过期了",不该再重试 Confirm。
            raise _with_state(
                errcode.PandoraError(
                    errcode.ErrTradeOrderExpired, "order %d expired", order_id
                ),
                _S.ORDER_STATE_EXPIRED,
            )

        if state["drive_settle"] is not None:
            return await self._drive_settlement(state["drive_settle"])

        # 买方确认路径:读回最新状态做 audit + 返回。
        order = await self._safe_get(order_id)
        if order is None:
            # 写成功但读回失败:返回我们已知的推进结果。
            return _S.ORDER_STATE_BUYER_CONFIRMED
        await self._push_audit(order)
        return order.state

    async def _drive_settlement(self, order) -> int:
        """驱动一个已落库 SELLER_CONFIRMED 意图的订单走完结算(幂等,可重入)。

        Settle 成功            → CAS → COMPLETED(资产账本为权威)
        余额/物品不足           → 资产未动(inventory 原子拒),CAS → FAILED 终态
        瞬时失败 / UNKNOWN      → 停留 SELLER_CONFIRMED(**可能已入账!绝不回滚、
                                 绝不置 FAILED**),返回可重试错误
        终态 CAS 写失败         → 同上停留,重试收敛;Error 告警
        """
        order_id = order.order_id
        try:
            await self._ledger.settle(order, order_id)
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
            # 吞掉之后取消就**不再传播** —— 该停的停不下来:
            #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
            #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
            # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
            raise
        except BaseException as exc:
            if errcode.as_code(exc) == errcode.ErrTradeInsufficient:
                # 结算原子失败(资产未动):SELLER_CONFIRMED → FAILED 终态并 audit。
                def to_failed(o) -> None:
                    if o.state == _S.ORDER_STATE_SELLER_CONFIRMED:
                        o.state = _S.ORDER_STATE_FAILED

                try:
                    await self._repo.update_with_lock(
                        order_id, self._cfg.optimistic_retry, to_failed,
                        self._cfg.order_ttl_td(),
                    )
                except Exception as ferr:  # noqa: BLE001
                    # 置 FAILED 失败:订单暂留 SELLER_CONFIRMED,重试 Confirm 会重走
                    # 结算 → 再次到达这里,可自愈;必须留 Error 便于告警。
                    plog.get().error(
                        "trade_mark_failed_state_failed", order_id=order_id, err=str(ferr)
                    )
                latest = await self._safe_get(order_id)
                if latest is not None:
                    await self._push_audit(latest)
                # Go trade.go:398 回 FAILED —— 资产未动的终态,客户端不该重试。
                raise _with_state(exc, _S.ORDER_STATE_FAILED)
            # 瞬时 / UNKNOWN(超时 / inventory 不可达 / 回包丢失):结算**可能已生效**,
            # 绝不回滚订单;意图态留在库里,由重试幂等收敛(Settle 幂等键命中即成功)。
            plog.get().warning(
                "trade_settlement_inflight_retryable", order_id=order_id, err=str(exc)
            )
            # Go trade.go:403 回 SELLER_CONFIRMED —— 意图态还在库里、结算可能已生效,
            # 客户端**必须**重试 Confirm(幂等键命中即成功)。回 0 的话它判不出这一点。
            raise _with_state(
                errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "order %d settlement in flight, retry confirm: %s",
                    order_id,
                    exc,
                ),
                _S.ORDER_STATE_SELLER_CONFIRMED,
            ) from exc

        # 结算已成功:资产账本为权威,状态收敛到 COMPLETED。
        completed = {"order": None}

        def to_completed(o) -> None:
            if o.state != _S.ORDER_STATE_COMPLETED:
                if o.state != _S.ORDER_STATE_SELLER_CONFIRMED:
                    # 不该发生(SELLER_CONFIRMED 已 fencing Cancel/过期):
                    # 按账本权威强制收敛并告警。
                    plog.get().error(
                        "trade_settled_state_diverged_converging",
                        order_id=order_id,
                        state=enum_name(_S, o.state),
                    )
                o.state = _S.ORDER_STATE_COMPLETED
            completed["order"] = _clone(o)

        try:
            await self._repo.update_with_lock(
                order_id, self._cfg.optimistic_retry, to_completed, self._cfg.order_ttl_td()
            )
        except Exception as cerr:  # noqa: BLE001
            # 资产已结算而终态未落库:停留 SELLER_CONFIRMED,重试 Confirm 收敛。
            plog.get().error("trade_mark_completed_failed", order_id=order_id, err=str(cerr))
            # Go trade.go:424 回 SELLER_CONFIRMED —— 资产**已经结算**、只是终态没落库。
            # 这条最不能丢:客户端拿到 0 会当"订单没动"而放弃,实际钱货已经过户了。
            raise _with_state(
                errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "order %d settled, state convergence pending, retry confirm",
                    order_id,
                ),
                _S.ORDER_STATE_SELLER_CONFIRMED,
            ) from cerr

        if completed["order"] is not None:
            await self._push_audit(completed["order"])
            self._log_settlement_routing(
                order_id, completed["order"].buyer_id, completed["order"].seller_id
            )
        return _S.ORDER_STATE_COMPLETED

    # ── CancelOrder ──────────────────────────────────────────────────────────

    async def cancel_order(self, player_id: int, order_id: int) -> None:
        """任一方在 PENDING / BUYER_CONFIRMED 下取消。

        SELLER_CONFIRMED 一律拒(结算围栏):结算意图已落库,资产转移可能已发生或
        即将发生,取消会与账本撕裂;订单只向 COMPLETED / FAILED 收敛。
        """
        if player_id == 0 or order_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player / order required")
        # 下撤循环两端都限:单限下单会把刷子逼到撤单侧。
        await self._allow_action("cancel", player_id)

        def mutate(order) -> None:
            if player_id not in (order.seller_id, order.buyer_id):
                raise errcode.PandoraError(
                    errcode.ErrUnauthorized,
                    "player %d not party of order %d",
                    player_id,
                    order_id,
                )
            if order.state == _S.ORDER_STATE_SELLER_CONFIRMED:
                raise errcode.PandoraError(
                    errcode.ErrTradeWrongState,
                    "order %d is settling (seller confirmed), cannot cancel",
                    order_id,
                )
            if order.state in _TERMINAL:
                raise errcode.PandoraError(
                    errcode.ErrTradeWrongState,
                    "order %d already terminal: %s",
                    order_id,
                    enum_name(_S, order.state),
                )
            order.state = _S.ORDER_STATE_CANCELED

        await self._repo.update_with_lock(
            order_id, self._cfg.optimistic_retry, mutate, self._cfg.order_ttl_td()
        )
        latest = await self._safe_get(order_id)
        if latest is not None:
            await self._push_audit(latest)

    # ── ListMyOrders ─────────────────────────────────────────────────────────

    async def list_my_orders(
        self, player_id: int, active_only: bool, cursor: int, limit: int
    ) -> tuple[list, int]:
        """列玩家参与的订单,按 order_id 降序游标分页。next_cursor 为本页末;0=无更多。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        limit = _clamp_limit(limit)
        ids = sorted(await self._repo.list_player_order_ids(player_id), reverse=True)

        out: list = []
        next_cursor = 0
        for oid in ids:
            if cursor != 0 and oid >= cursor:
                continue  # 上页已返回
            if len(out) == limit:
                next_cursor = out[-1].order_id  # 还有更多
                break
            order = await self._safe_get(oid)
            if order is None:
                continue  # 订单已过期被 Redis 回收 → 跳过
            if _expire_if_stale(order):
                # 惰性过期:尽力写回,不阻断列表。回调内必须**同样排除**
                # SELLER_CONFIRMED(读-写间隙订单可能刚进入结算围栏)。
                def to_expired(o) -> None:
                    stale = o.expires_at_ms > 0 and now_ms() >= o.expires_at_ms
                    if (
                        o.state != _S.ORDER_STATE_SELLER_CONFIRMED
                        and o.state not in _TERMINAL
                        and stale
                    ):
                        o.state = _S.ORDER_STATE_EXPIRED

                try:
                    await self._repo.update_with_lock(
                        oid, self._cfg.optimistic_retry, to_expired,
                        self._cfg.order_ttl_td(),
                    )
                except Exception:  # noqa: BLE001
                    pass  # 尽力而为,与 Go 侧 `_ =` 一致
            if active_only and order.state in _TERMINAL:
                continue
            out.append(order)
        return out, next_cursor

    # ── 辅助 ─────────────────────────────────────────────────────────────────

    async def _safe_get(self, order_id: int):
        """读订单,失败返回 None(用于 audit / 读回这类"尽力"路径)。"""
        try:
            return await self._repo.get_order(order_id)
        except Exception:  # noqa: BLE001
            return None

    async def _push_audit(self, order) -> None:
        """弱依赖审计推送:audit 为 None 或失败只 warn,不影响主流程。"""
        if self._audit is None:
            return
        try:
            await self._audit.push_audit(order)
        except Exception as exc:  # noqa: BLE001
            plog.get().warning(
                "trade_audit_push_failed",
                order_id=order.order_id,
                state=enum_name(_S, order.state),
                err=str(exc),
            )

    def _log_settlement_routing(self, order_id: int, buyer_id: int, seller_id: int) -> None:
        """结算成功时观测跨分片落点。router 为 None(单 Cell)→ 不打。"""
        if self._router is None:
            return
        try:
            buyer = self._router.route(buyer_id)
            seller = self._router.route(seller_id)
        except Exception:  # noqa: BLE001
            plog.get().debug("trade_settlement_route_failed", order_id=order_id)
            return
        plog.get().debug(
            "trade_settlement_placement",
            order_id=order_id,
            buyer_region=buyer.region_id,
            buyer_cell=buyer.cell_id,
            seller_region=seller.region_id,
            seller_cell=seller.cell_id,
            cross_cell=buyer.cell_id != seller.cell_id,
            cross_region=buyer.region_id != seller.region_id,
        )


def _expire_if_stale(order) -> bool:
    """已过期且非终态且非 SELLER_CONFIRMED → 就地置 EXPIRED 并返回 True。

    ★ SELLER_CONFIRMED **不过期**(结算围栏):结算意图已落库、资产可能已转移,
    置 EXPIRED 会与账本撕裂,只允许向 COMPLETED / FAILED 收敛。
    """
    if order.state == _S.ORDER_STATE_SELLER_CONFIRMED or order.state in _TERMINAL:
        return False
    if order.expires_at_ms > 0 and now_ms() >= order.expires_at_ms:
        order.state = _S.ORDER_STATE_EXPIRED
        return True
    return False


def _clone(order):
    """深拷一份 proto —— **禁止值拷贝**(§5.10:proto message 含内部状态,不能浅拷)。"""
    copy = trade_pb2.Order()
    copy.CopyFrom(order)
    return copy


def _clamp_limit(limit: int) -> int:
    if limit <= 0:
        return DEFAULT_PAGE_LIMIT
    return min(limit, MAX_PAGE_LIMIT)
