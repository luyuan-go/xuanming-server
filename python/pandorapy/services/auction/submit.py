"""拍卖挂单 / 出价的统一入口 —— 对应 Go 侧 internal/biz/auction.go 的 submit。

流程:PENDING 幂等登记 → 幂等冻结 → 权威撮合 → 激活未成交余量。

★ PENDING 这个中间态**不是多余的**:
    它让进程在「登记后、冻结前」退出时,订单**不会被撮合选中**。
    没有它的话,一个还没冻结资产的订单会被撮合成交 —— 卖家的道具没扣、
    买家的钱没冻,成交却发生了。

★ 三层幂等键,各管一段,**不能共用**:

    订单登记   idempotency_key(uk owner+key)  → 重试不重复挂单
    资产冻结   order_id                        → 重试只确认同一笔冻结
    成交对转   match_id                        → 资产只转一次
    退还残余   order_id                        → 撤单/过期/完全成交后退 escrow

★ 最容易写错的一条(Go 侧有专门注释警告):

    ClaimOrder 只要返回权威快照,就**必须无条件沿用其中的 canonical order_id**。
    `already` 只决定"能不能直接回放终态",**不能**决定后续资产幂等键用哪个 ID。
    用本地新铸的 order_id 去 Freeze 会绕开幂等 —— 同一次重试冻两次资产。
"""

from __future__ import annotations

import asyncio
import dataclasses
import re

from pandora.auction.v1 import auction_pb2

from pandorapy import errcode
from pandorapy import log as plog

# ★ 状态 / 方向一律取 pb2 生成物,**不许手抄字面量**。
# 这两组值同时是**落库值**(auction_orders.status / side)和**跨服务传输值**
# (下游 inventory 的 EscrowSide 与 proto OrderSide 同号:SELL=1 / BUY=2),
# 手抄一次错位就会同时污染库里的历史行和对下游的冻结方向,而两边都不会报错。
# Go 真源:services/economy/auction/internal/data/auction_repo.go 的
# Side / Status 常量块 —— 其数值与 proto 枚举逐一对齐。
#
# STATUS_PENDING 是 Go 侧的**内部恢复态**:proto 没有它的名字,数值上占用
# UNSPECIFIED(0)。它绝不能被当作 proto 语义暴露给客户端列表,只在
# "已登记未冻结"这一段内部流转 —— 撮合**看不到**它。
STATUS_PENDING = auction_pb2.AUCTION_ORDER_STATUS_UNSPECIFIED
STATUS_OPEN = auction_pb2.AUCTION_ORDER_STATUS_OPEN
STATUS_PARTIAL = auction_pb2.AUCTION_ORDER_STATUS_PARTIALLY_FILLED
STATUS_FILLED = auction_pb2.AUCTION_ORDER_STATUS_FILLED
STATUS_CANCELED = auction_pb2.AUCTION_ORDER_STATUS_CANCELED
STATUS_EXPIRED = auction_pb2.AUCTION_ORDER_STATUS_EXPIRED

# ★ 终态 = 不可再流转。**PARTIAL 不是终态** —— 它剩余量还挂在簿上继续撮合。
# 把 PARTIAL 算进终态会让部分成交的订单被提前释放 owner 名额,
# 玩家凭一个仍在簿上的挂单白拿一个额度(配额恒被穿透,且不报错)。
# Go 真源:internal/biz/auction.go 的 isTerminal —— 只有 FILLED/CANCELED/EXPIRED。
#
# proto 里没有 REJECTED:Go 的 "reject pending order" 写入的是 CANCELED
# (auction_repo.go rejectPendingOrder),所以这里也不得凭空造一个状态。
_TERMINAL = frozenset({STATUS_FILLED, STATUS_CANCELED, STATUS_EXPIRED})

SIDE_SELL = auction_pb2.ORDER_SIDE_SELL
SIDE_BUY = auction_pb2.ORDER_SIDE_BUY

# 幂等键字符集:1..64 个 ASCII [A-Za-z0-9._:-]。
# 限死字符集是因为它会进 uk 索引并出现在日志 / 审计里 —— 放开会引入编码与注入面。
#
# ⚠️ 必须用 `\Z` 而不是 `$`(2026-08-18 被测试抓到的 Python 特有陷阱):
#     Python 的 `$` 匹配"字符串末尾**或末尾换行之前**",于是 "a\n" 会通过校验 ——
#     一个带尾换行的幂等键就此进入 uk 索引和日志行。
#     Go 的 `regexp` 里 `$`(非多行模式)只匹配文本末尾,没有这个行为,
#     所以照抄 Go 的正则会**静默放宽**校验。`\Z` 才是 Python 里的"绝对末尾"。
_IDEM_KEY_RE = re.compile(r"\A[A-Za-z0-9._:-]{1,64}\Z")

# int64 上界(下游 inventory 结算会算 total = quantity * unit_price)。
_MAX_INT64 = 2**63 - 1


def valid_idempotency_key(key: str) -> bool:
    return bool(_IDEM_KEY_RE.match(key or ""))


def is_terminal(status: int) -> bool:
    return status in _TERMINAL


@dataclasses.dataclass(slots=True)
class OrderRecord:
    order_id: int = 0
    market_id: int = 0
    owner_id: int = 0
    side: int = SIDE_SELL
    item_config_id: int = 0
    quantity: int = 0
    filled_quantity: int = 0
    price: int = 0
    status: int = STATUS_PENDING
    # 终态订单仍需用 order_id 幂等释放 escrow。
    release_pending: bool = False
    # 该订单曾作为 incoming 部分成交,仍需主动把已交叉对手盘扫尽。
    match_pending: bool = False
    # ★ 只有 inventory 确认剩余托管充足后才允许成为撮合双方。
    # 少了这一列,旧二进制遗留的 OPEN/PARTIAL(可能根本没冻过资产)会直接被新
    # matcher 选中成交 —— 卖家的道具没扣、买家的钱没冻,成交却发生了。
    escrow_verified: bool = False
    # PENDING / legacy escrow / 撮合续跑的公平退避时间。
    reconcile_next_attempt_at_ms: int = 0
    # 把永久失败的释放记录暂时移出就绪批次,避免饿死后续订单。
    release_next_attempt_at_ms: int = 0
    idempotency_key: str = ""
    created_at_ms: int = 0
    updated_at_ms: int = 0


def validate_submit(
    *,
    owner_id: int,
    market_id: int,
    item_config_id: int,
    quantity: int,
    price: int,
    idem_key: str,
    max_quantity: int,
    max_price: int,
) -> None:
    """入口校验。★ 顺序与 Go 一致 —— 频率配额在**更外层**,先于这里。"""
    if owner_id == 0:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "owner required")
    if market_id == 0 or item_config_id == 0:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "market_id / item_config_id required"
        )
    if quantity <= 0 or quantity > max_quantity:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg,
            "quantity out of range: %d (max %d)",
            quantity,
            max_quantity,
        )
    if price <= 0 or price > max_price:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "price out of range: %d (max %d)", price, max_price
        )
    # ★ 成交总额溢出守卫:即便单值都在上界内,极端组合仍可能溢出 int64。
    # 下游 inventory 会算 total = quantity * unit_price —— 溢出后金额回绕,
    # 可能变成负数或极小值,**不报错**。必须在入口拒。
    if quantity > _MAX_INT64 // price:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg,
            "total value overflow: quantity %d * price %d",
            quantity,
            price,
        )
    if not valid_idempotency_key(idem_key):
        raise errcode.PandoraError(
            errcode.ErrInvalidArg,
            "idempotency_key must be 1..64 ASCII characters [A-Za-z0-9._:-]",
        )


@dataclasses.dataclass(slots=True)
class SubmitOutcome:
    """一次 submit 的结果。

    ★ `finalize` 不是可有可无的返回值:它区分「本次真的推进了这张单」和
      「只是回放了一份权威快照」。

      回放路径(已激活 / 已终态的重试、以及 activate 竞态后回读到的当前快照)
      **绝不能**再去写订单簿缓存或发 audit ——
      给一张已经 CANCELED 的单 ZADD 回订单簿,它会以终态身份重新出现在旧实例的
      买卖盘里,而 MySQL 权威侧一切正常,两边都不报错。
    """

    record: OrderRecord
    finalize: bool


class AuctionSubmitter:
    """挂单 / 出价的统一入口(步骤序列与 Go 的 `AuctionUsecase.submit` 逐条对应)。

    调用方(biz)负责这一层之外的事:频率配额、market 单写者锁、订单簿缓存、
    audit 入队。**撮合**由 `matcher` 回调注入 —— 它必须发生在
    「确认 escrow 之后、激活之前」:

      - 先确认再撮合:ReserveMatch 只接受 escrow_verified=1 的双方;
      - 先撮合再激活:若反过来,进程恰在两步之间退出会留下一张已交叉却
        无人主动处理的 OPEN 单(它不会被任何人当成 incoming 续跑)。
    """

    __slots__ = ("_repo", "_ledger", "_slots", "_snowflake", "_cfg", "_matcher")

    def __init__(self, repo, ledger, slot_limiter, snowflake, cfg, matcher=None) -> None:  # noqa: ANN001
        self._repo = repo
        self._ledger = ledger
        self._slots = slot_limiter
        self._snowflake = snowflake
        self._cfg = cfg
        self._matcher = matcher

    async def submit(
        self,
        *,
        owner_id: int,
        side: int,
        market_id: int,
        item_config_id: int,
        quantity: int,
        price: int,
        idem_key: str,
        now_ms: int,
    ) -> SubmitOutcome:
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

        rec = OrderRecord(
            order_id=self._snowflake.generate(),
            market_id=market_id,
            owner_id=owner_id,
            side=side,
            item_config_id=item_config_id,
            quantity=quantity,
            price=price,
            status=STATUS_PENDING,
            idempotency_key=idem_key,
            created_at_ms=now_ms,
            updated_at_ms=now_ms,
        )

        existing, already = await self._repo.claim_order(rec)

        # ★ 只要拿到权威快照就**无条件沿用它的 order_id**。
        # already 只决定"能不能直接回放",**不能**决定后续资产幂等键 ——
        # 用本地新铸的 order_id 去 Freeze 会绕开幂等,同一次重试冻两次资产。
        if existing is not None:
            rec = existing

        if already and rec.status != STATUS_PENDING:
            # 已激活 / 已终态 → 直接回放。只有 PENDING 才需要恢复冻结+激活。
            if is_terminal(rec.status):
                await self._release_owner_slot(rec)
            return SubmitOutcome(rec, finalize=False)

        # ★ PENDING 已落库**之后**才预留 owner 名额:
        # 这样成员始终能按 market_id + order_id 回查到权威状态。
        # 预留失败必须**条件终态化** —— 不能留下一个绕过配额的可恢复 PENDING。
        try:
            await self._slots.reserve(rec)
        except asyncio.CancelledError:
            raise
        except BaseException:  # noqa: BLE001
            # ★ 这里**必须**是 catch-all,不能只收 PandoraError(对齐 Go:
            #   `if slotErr := reserveOwnerSlotPruning(...); slotErr != nil`)。
            #   Redis 连接被重置、超时、DNS 挂了都是裸异常 —— 只收 PandoraError
            #   会让它们直接逃出去,而那张已落库的 PENDING 单**不会被终态化**:
            #   它既占着配额名额、又处在"可恢复"状态,等于一个绕过配额上限的洞。
            await self._reject_pending(rec, now_ms)
            raise

        # 冻结。幂等键 = order_id:首次成功后即使在激活前崩溃,同 idem 重试也只确认同一笔。
        try:
            await self._ledger.freeze(
                owner_id=rec.owner_id,
                order_id=rec.order_id,
                side=rec.side,
                item_config_id=rec.item_config_id,
                quantity=rec.quantity,
                price=rec.price,
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except
            # 吞掉。吞掉之后取消不再传播 —— grpc.aio 正是用取消终止在途 handler,
            # 而这里还会把取消翻译成"冻结失败"去终态化一张其实好好的订单。
            raise
        except BaseException:  # noqa: BLE001
            # ★ 冻结失败的结果**可能包含网络不确定性** —— 也许其实冻成功了只是响应丢了。
            # 所以终态化的同时登记 release_pending,由 Release(幂等键 order_id)
            # 消除"其实冻结成功但响应丢失"造成的永久锁资。
            try:
                changed = await self._repo.reject_pending_order(
                    rec.market_id, rec.order_id, now_ms
                )
            except asyncio.CancelledError:
                raise
            except BaseException as uerr:  # noqa: BLE001
                # ★ 冻结失败后连"终态化"这一步也失败 —— 这张单会带着可能已冻结的
                #   资产停在 PENDING。事件名对齐 Go,是补偿链排查的唯一入口。
                plog.get().error(
                    "auction_reject_after_freeze_fail_persist_failed",
                    order_id=rec.order_id,
                    market_id=rec.market_id,
                    err=str(uerr),
                )
                raise
            if changed:
                await self._release_owner_slot(rec)
                await self._try_release(rec)
            raise

        # 确认 escrow:把 inventory 的验证结果幂等落库,撮合前必须成功。
        confirmed = await self._repo.confirm_order_escrow(rec.market_id, rec.order_id, now_ms)
        if not confirmed:
            # 已冻结的权威 PENDING 单在两步之间凭空消失 = 严重状态一致性异常。
            plog.get().error(
                "auction_confirm_escrow_lost",
                market_id=rec.market_id,
                order_id=rec.order_id,
                step="confirm_pending",
            )
            raise errcode.PandoraError(
                errcode.ErrInternal, "confirm escrow lost pending order %d", rec.order_id
            )
        rec.escrow_verified = True

        # ★ 保持 incoming=PENDING 先撮合:ReserveMatch 只允许 incoming 是该内部状态,
        # resting 必须活跃。先 Activate 再撮合会留下已交叉却无人主动处理的 OPEN 单。
        if self._matcher is not None:
            await self._matcher(rec)

        if rec.status == STATUS_PENDING:
            activated = await self._repo.activate_order(rec.market_id, rec.order_id, now_ms)
            if not activated:
                current, found = await self._repo.get_order(rec.market_id, rec.order_id)
                if not found or current is None:
                    plog.get().error(
                        "auction_activated_order_disappeared",
                        market_id=rec.market_id,
                        order_id=rec.order_id,
                    )
                    raise errcode.PandoraError(
                        errcode.ErrInternal, "activated order %d disappeared", rec.order_id
                    )
                return SubmitOutcome(current, finalize=False)
            rec.status = STATUS_OPEN
            rec.updated_at_ms = now_ms

        return SubmitOutcome(rec, finalize=True)

    async def _reject_pending(self, rec: OrderRecord, now_ms: int) -> None:
        """slot 预留失败后条件终态化 PENDING。对齐 Go `rejectPendingAfterSlotFailure`。

        ★ `changed == False` 是**异常**,不是"没什么可做"。

        走到这里说明我们刚刚亲手写下过一张 PENDING 单,而条件更新却说没有 PENDING
        可改 —— 要么有并发写者在动同一张单,要么状态机已经不自洽。静默放过的话:
        那张单继续占着配额名额、继续处在可恢复态,而调用方只看到原始的 slot 错误,
        没有任何线索指向这里。所以按 Go 抛 `ErrInternal`。

        终态化成功后还要:①把内存记录也翻成 CANCELED + `release_pending`
        (调用方后续可能还会用这个 rec);②`_try_release` 退还可能已冻结的 escrow。
        只放 owner 名额是不够的 —— 那只解决"能挂几张单",不解决资产被锁住。
        """
        changed = await self._repo.reject_pending_order(rec.market_id, rec.order_id, now_ms)
        if not changed:
            plog.get().error(
                "auction_reject_pending_after_slot_failure_no_change",
                market_id=rec.market_id,
                order_id=rec.order_id,
                hint="刚写下的 PENDING 单无法被条件终态化:并发写者或状态机不自洽",
            )
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "pending order %d could not be rejected after slot failure",
                rec.order_id,
            )
        rec.status = STATUS_CANCELED
        rec.release_pending = True
        await self._release_owner_slot(rec)
        await self._try_release(rec)

    async def _release_owner_slot(self, rec: OrderRecord) -> None:
        try:
            await self._slots.release(rec)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 名额释放失败只影响该玩家后续能挂多少单,不影响资产正确性 → WARN 不阻断。
            plog.get().warning(
                "auction_release_owner_slot_failed",
                owner_id=rec.owner_id,
                order_id=rec.order_id,
                err=str(exc),
            )

    async def _try_release(self, rec: OrderRecord) -> None:
        """尽力退还 escrow 残余(幂等键 = order_id)。失败留痕,由后台补偿链收敛。"""
        try:
            await self._ledger.release(rec.owner_id, rec.order_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "auction_release_escrow_failed",
                owner_id=rec.owner_id,
                order_id=rec.order_id,
                err=str(exc),
                hint="escrow 残余未退还,由后台 release_pending 补偿链收敛",
            )
