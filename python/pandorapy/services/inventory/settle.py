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

★ 金额乘法守卫在 currency.safe_mul_currency(不在本文件):
    多币种改造后上界是 MAX_CURRENCY_AMOUNT=2^62 而不是 int64 上限,
    再留一份"int64 版"的守卫只会有两个会漂移的真相。

★ 自成交 / 自交易必须拒:
    净额为零看似无害,但它会让**同一玩家在同一幂等键下写两条流水**(一进一出),
    唯一键冲突 → 整笔失败。与其让它在数据层炸,不如在入口说清楚。
"""

from __future__ import annotations

import dataclasses

from pandorapy import errcode
from pandorapy.services.inventory import currency as ccy

@dataclasses.dataclass(frozen=True, slots=True)
class ItemGrant:
    item_config_id: int
    count: int


def auction_settle_key(match_id: int) -> str:
    """★ 与 Go 侧逐字一致。格式变了 = 迁移期重复入账。"""
    return f"auction:settle:{match_id}"


def trade_settle_key(order_id: int) -> str:
    """★ 与 Go 侧逐字一致。"""
    return f"trade:settle:{order_id}"


def auction_settle_detail(
    match_id: int, item_config_id: int, quantity: int, total_amount: int
) -> str:
    """账本 detail 列(给人读的审计串)。与 Go 侧同格式,便于两栈流水混看。

    ★ 字段名保持 `gold=`(而不是随多币种改成 `amount=`):detail 是人读审计串,
      两栈流水混看时格式必须同源;币种由 ledger 的 result_currency_delta 列承担。
    """
    return (
        f"auction settle match={match_id} item={item_config_id} "
        f"qty={quantity} gold={total_amount}"
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
    kind: int,
    unit_price: int,
) -> int:
    """拍卖成交结算入参校验。返回总金额(已过溢出守卫)。

    ★ unit_price 在协议里已是 uint64:`== 0` 就是完整的"必须为正"校验。
      Python 没有类型保护,所以仍显式写 `<= 0` 把负单价一起拒掉 ——
      负单价 × 正数量 = 负总价,一路穿到扣款处会让 `have < n` 恒为假,
      把"从买家 escrow 扣钱"变成"给买家加钱"。
    ★ 币种必须显式校验,**不得对 UNSPECIFIED 回退成金币**(currency.proto):
      静默回退会让配错的订单按金币结算,是不可观测的经济事故。
    """
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
    ccy.validate_currency_kind(kind)

    total_amount, ok = ccy.safe_mul_currency(unit_price, quantity)
    if not ok or total_amount == 0:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg,
            "settle amount overflow match=%d price=%d qty=%d",
            match_id,
            unit_price,
            quantity,
        )
    return total_amount


def validate_player_trade_settle(
    *,
    order_id: int,
    seller_id: int,
    buyer_id: int,
    seller_items: list[ItemGrant],
    buyer_items: list[ItemGrant],
    kind: int,
    price: int,
) -> None:
    """P2P 交易结算入参校验。

    与拍卖不同:**无预冻结**,任一方资产不足 → ErrInventoryInsufficient,整笔回滚。
    这里只做形状校验,余额判定在数据层的 `SELECT ... FOR UPDATE` 锁行内做
    (避免并发超扣 —— 在这里判就是 TOCTOU)。

    ★ 旧实现有一句 `if price < 0`。协议把 price 改成 uint64 后,那句在 Go 侧
      **恒为 false**(闸门被静默拆掉)。这里按语义重判成两条:
        ① price > 0 时必须说清楚是哪种钱(校验币种),否则 UNSPECIFIED 会一路穿到扣款;
        ② price < 0 仍显式拒 —— Python 的 int 没有无符号保护,负价格会把
           "买家付钱"变成"买家收钱"。
      纯物物交换(price == 0)不校验币种:一分钱没动,币种没有语义。
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
    if price > 0:
        ccy.validate_currency_kind(kind)
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
