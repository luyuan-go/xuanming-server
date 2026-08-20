"""资产对转结算 —— 对应 Go 侧 internal/biz/inventory.go 的 SettleAuctionMatch / SettlePlayerTrade。

两种对转,预冻结与否是根本区别:

    拍卖成交  资产已在 FreezeForOrder 冻进 escrow → 成交**不会因余额不足失败**
    P2P 交易  无预冻结,从双方活跃余额直接扣转 → 任一方不足即整笔回滚

★ 幂等键格式是**跨服务契约**,必须与 Go 逐字一致:

        auction:settle:<match_id>
        trade:settle:<order_id>

    它是账本(inventory_ledger)里的去重键。格式变一个字符,
    Go 版写过的那笔在 Python 版看来就是"没结算过" —— **重复入账**。
    迁移期两栈并存时这不是理论风险。

★ 自成交 / 自交易必须拒:
    净额为零看似无害,但它会让**同一玩家在同一幂等键下写两条流水**(一进一出),
    唯一键冲突 → 整笔失败。与其让它在数据层炸,不如在入口说清楚。
"""

from __future__ import annotations

import dataclasses

from pandorapy import errcode

_MIN_INT64 = -(2**63)
_MAX_INT64 = 2**63 - 1


@dataclasses.dataclass(frozen=True, slots=True)
class ItemGrant:
    item_config_id: int
    count: int


def safe_mul_int64(a: int, b: int) -> tuple[int, bool]:
    """int64 乘法溢出守卫。返回 (积, 是否安全)。

    ★ Python 的 int 无限精度**不会**溢出,所以必须显式检查上下界 ——
    否则 Python 版会算出一个 Go 版根本表示不了的金额,写进 BIGINT 列时
    严格模式报错、非严格模式**静默截断**(§9.24)。
    """
    product = a * b
    if product < _MIN_INT64 or product > _MAX_INT64:
        return 0, False
    return product, True


def auction_settle_key(match_id: int) -> str:
    """★ 与 Go 侧逐字一致。格式变了 = 迁移期重复入账。"""
    return f"auction:settle:{match_id}"


def trade_settle_key(order_id: int) -> str:
    """★ 与 Go 侧逐字一致。"""
    return f"trade:settle:{order_id}"


def auction_settle_detail(
    match_id: int, item_config_id: int, quantity: int, total_gold: int
) -> str:
    """账本 detail 列(给人读的审计串)。与 Go 侧同格式,便于两栈流水混看。"""
    return (
        f"auction settle match={match_id} item={item_config_id} "
        f"qty={quantity} gold={total_gold}"
    )


def validate_auction_settle(
    *,
    match_id: int,
    seller_id: int,
    buyer_id: int,
    sell_order_id: int,
    buy_order_id: int,
    item_config_id: int,
    quantity: int,
    unit_price: int,
) -> int:
    """拍卖成交结算入参校验。返回总金额(已过溢出守卫)。"""
    if match_id == 0:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "match_id required")
    if seller_id == 0 or buyer_id == 0:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "seller_id / buyer_id required")
    if sell_order_id == 0 or buy_order_id == 0:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "sell_order_id / buy_order_id required"
        )
    if seller_id == buyer_id:
        # ★ 自成交:净额为零,且会让同一玩家在同一幂等键下写两条流水 → 唯一键冲突。
        # 撮合侧本就应避免自撮合;这里 fail-closed 而不是让它在数据层炸。
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "seller and buyer must differ: %d", seller_id
        )
    if item_config_id == 0:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "item_config_id required")
    if quantity <= 0:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "quantity must be positive")
    if unit_price <= 0:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "unit_price must be positive")

    total_gold, ok = safe_mul_int64(unit_price, quantity)
    if not ok or total_gold <= 0:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg,
            "settle amount overflow match=%d price=%d qty=%d",
            match_id,
            unit_price,
            quantity,
        )
    return total_gold


def validate_player_trade_settle(
    *,
    order_id: int,
    seller_id: int,
    buyer_id: int,
    seller_items: list[ItemGrant],
    buyer_items: list[ItemGrant],
    price: int,
) -> None:
    """P2P 交易结算入参校验。

    与拍卖不同:**无预冻结**,任一方资产不足 → ErrInventoryInsufficient,整笔回滚。
    这里只做形状校验,余额判定在数据层的 `SELECT ... FOR UPDATE` 锁行内做
    (避免并发超扣 —— 在这里判就是 TOCTOU)。
    """
    if order_id == 0:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "order_id required")
    if seller_id == 0 or buyer_id == 0:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "seller_id / buyer_id required")
    if seller_id == buyer_id:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "seller and buyer must differ: %d", seller_id
        )
    if price < 0:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "price must not be negative")
    if not seller_items and not buyer_items and price == 0:
        # 空交易:没有任何资产变动却要写一条流水,只会污染账本。
        raise errcode.PandoraError(errcode.ErrInvalidArg, "nothing to settle")

    for items, side in ((seller_items, "seller"), (buyer_items, "buyer")):
        for it in items:
            if it.item_config_id == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "%s item_config_id required", side
                )
            if it.count <= 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "%s count must be positive: item=%d",
                    side,
                    it.item_config_id,
                )
