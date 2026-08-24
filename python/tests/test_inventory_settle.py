"""资产对转结算测试。

重点:
  1. ★ 幂等键格式是跨服务契约 —— 变一个字符 = 迁移期重复入账
  2. ★ 自成交必须拒(同一玩家同幂等键写两条流水 → 唯一键冲突)
  3. ★ 溢出守卫(Python 的 int 不会溢出,必须显式检查;上界是
     currency.MAX_CURRENCY_AMOUNT=2^62,不是 int64 上限)
  4. 空交易拒绝
"""

from __future__ import annotations

import pytest

from pandorapy import errcode
from pandorapy.services.inventory import currency as ccy
from pandorapy.services.inventory import settle


def _auction(**kw):
    base = dict(
        match_id=555, seller_id=1001, buyer_id=2002,
        sell_order_id=11, buy_order_id=22,
        item_config_id=5001, quantity=3,
        kind=ccy.CURRENCY_GOLD, unit_price=100,
    )
    base.update(kw)
    return settle.validate_auction_settle(**base)


def _trade(**kw):
    base = dict(
        order_id=777, seller_id=1001, buyer_id=2002,
        seller_items=[settle.ItemGrant(5001, 1)], buyer_items=[],
        kind=ccy.CURRENCY_GOLD, price=100,
    )
    base.update(kw)
    return settle.validate_player_trade_settle(**base)


# ── ★ 1. 幂等键格式(跨服务契约)────────────────────────────────────────────


def _go_format(repo_root, pattern: str) -> str:
    """从 Go 源码里取出 fmt.Sprintf 的格式串。

    ★ 判据必须取自 **Go 源码**,不是在测试里再抄一份字面量。
    抄一份的话 Go 改了格式,这条"对拍"照样绿 —— 而它存在的唯一理由
    就是发现两边分叉。
    """
    import re

    src = (
        repo_root / "services" / "economy" / "inventory" / "internal" / "biz"
        / "inventory.go"
    ).read_text(encoding="utf-8")
    m = re.search(pattern, src)
    assert m, f"没在 Go 源码里找到格式串:{pattern}"
    return m.group(1)


def test_idempotency_key_formats_match_go(repo_root) -> None:
    """★ 幂等键是账本里的去重键,格式必须与 Go **逐字一致**。

    格式变一个字符,Go 版写过的那笔在 Python 版看来就是"没结算过" ——
    **重复入账**。迁移期两栈并存时这不是理论风险。
    """
    auction_fmt = _go_format(repo_root, r'idempotencyKey := fmt\.Sprintf\("(auction:settle:[^"]+)"')
    trade_fmt = _go_format(repo_root, r'idempotencyKey := fmt\.Sprintf\("(trade:settle:[^"]+)"')
    assert settle.auction_settle_key(555) == auction_fmt.replace("%d", "555")
    assert settle.trade_settle_key(777) == trade_fmt.replace("%d", "777")


def test_detail_format_matches_go(repo_root) -> None:
    """账本 detail 同格式 —— 两栈流水要能混在一起看。"""
    detail_fmt = _go_format(repo_root, r'detail := fmt\.Sprintf\("(auction settle [^"]+)"')
    expected = detail_fmt
    for value in ("555", "5001", "3", "300"):
        expected = expected.replace("%d", value, 1)
    assert settle.auction_settle_detail(555, 5001, 3, 300) == expected


def test_keys_are_distinct_per_id() -> None:
    """不同成交 / 订单的键必须不同(否则第二笔被当成重复)。"""
    assert settle.auction_settle_key(1) != settle.auction_settle_key(2)
    assert settle.auction_settle_key(1) != settle.trade_settle_key(1)


# ── ★ 2. 自成交 ────────────────────────────────────────────────────────────


def test_auction_self_trade_rejected() -> None:
    """★ 自成交必须拒。

    净额为零看似无害,但它会让**同一玩家在同一幂等键下写两条流水**(一进一出),
    唯一键冲突 → 整笔失败。与其让它在数据层炸,不如在入口说清楚。
    """
    with pytest.raises(errcode.PandoraError, match="must differ"):
        _auction(seller_id=1001, buyer_id=1001)


def test_player_trade_self_trade_rejected() -> None:
    with pytest.raises(errcode.PandoraError, match="must differ"):
        _trade(seller_id=1001, buyer_id=1001)


# ── ★ 3. 溢出守卫(乘法本体在 test_inventory_currency.py)─────────────────


def test_settle_amount_overflow_rejected() -> None:
    """总额溢出时整笔拒绝,不让它进数据层。"""
    with pytest.raises(errcode.PandoraError, match="overflow"):
        _auction(quantity=2**40, unit_price=2**40)


def test_near_limit_amount_allowed() -> None:
    """不溢出的极端组合应当放行(守卫不能过严)。

    上界是 currency.MAX_CURRENCY_AMOUNT = 2^62(与 Go 的 MaxCurrencyAmount 同值),
    **不是** int64 上限:货币改无符号后,加法的天花板由业务上限而不是类型宽度决定。
    """
    total = _auction(quantity=2, unit_price=(2**61))
    assert total == 2**62
    assert total == ccy.MAX_CURRENCY_AMOUNT


def test_returned_total_is_product() -> None:
    assert _auction(quantity=3, unit_price=100) == 300


# ── ★ 4. 空交易 ────────────────────────────────────────────────────────────


def test_empty_trade_rejected() -> None:
    """没有任何资产变动却要写一条流水,只会污染账本。"""
    with pytest.raises(errcode.PandoraError, match="nothing to settle"):
        _trade(seller_items=[], buyer_items=[], price=0)


def test_pure_gold_trade_allowed() -> None:
    """纯金币交易(无道具)合法。"""
    _trade(seller_items=[], buyer_items=[], price=100)


def test_pure_diamond_trade_allowed() -> None:
    """非金币币种同样合法 —— 结算路径按 kind 参数化,不是"只有金币能交易"。"""
    _trade(seller_items=[], buyer_items=[], kind=ccy.CURRENCY_DIAMOND, price=100)


def test_pure_item_trade_allowed() -> None:
    """纯道具交易(无金币)合法。"""
    _trade(seller_items=[settle.ItemGrant(5001, 1)], buyer_items=[], price=0)


# ── 入参校验 ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("match_id", 0),
        ("seller_id", 0),
        ("buyer_id", 0),
        ("sell_order_id", 0),
        ("buy_order_id", 0),
        ("item_config_id", 0),
        ("quantity", 0),
        ("quantity", -1),
        ("unit_price", 0),
        ("unit_price", -1),
    ],
)
def test_auction_required_fields(field: str, value: int) -> None:
    with pytest.raises(errcode.PandoraError):
        _auction(**{field: value})


@pytest.mark.parametrize(("field", "value"), [("order_id", 0), ("seller_id", 0), ("buyer_id", 0)])
def test_trade_required_fields(field: str, value: int) -> None:
    with pytest.raises(errcode.PandoraError):
        _trade(**{field: value})


def test_trade_negative_price_rejected() -> None:
    """★ 协议改 uint64 后,Go 侧那句 `price < 0` 已恒为 false —— 闸门被静默拆掉。

    Python 的 int 没有无符号保护,所以这条闸必须**真的留着**:
    负价格会把"买家付钱"变成"买家收钱"。
    """
    with pytest.raises(errcode.PandoraError, match="must not be negative"):
        _trade(price=-1)


def test_trade_positive_price_requires_known_currency() -> None:
    """★ `price < 0` 退化后,真正该补上的是"有钱就必须说清楚是哪种钱"。

    UNSPECIFIED 一律 fail-closed,**不得回退成金币**(currency.proto):
    静默回退会让配错的订单按金币结算,是不可观测的经济事故。
    """
    with pytest.raises(errcode.PandoraError, match="unsupported currency kind"):
        _trade(kind=ccy.CURRENCY_KIND_UNSPECIFIED, price=100)


def test_trade_zero_price_skips_currency_check() -> None:
    """纯物物交换(price=0)不校验币种:一分钱没动,币种没有语义。

    这条与上一条是一对:把币种校验无条件前置会让存量的纯物物交换订单
    在升级瞬间全部报 ERR_INVALID_ARG。
    """
    _trade(
        kind=ccy.CURRENCY_KIND_UNSPECIFIED,
        seller_items=[settle.ItemGrant(5001, 1)],
        buyer_items=[],
        price=0,
    )


def test_auction_requires_known_currency() -> None:
    """拍卖结算同样 fail-closed:币种未知即拒,不猜。"""
    with pytest.raises(errcode.PandoraError, match="unsupported currency kind"):
        _auction(kind=ccy.CURRENCY_KIND_UNSPECIFIED)


@pytest.mark.parametrize("bad", [settle.ItemGrant(0, 1), settle.ItemGrant(5001, 0),
                                 settle.ItemGrant(5001, -1)])
def test_trade_item_shape_validated_on_both_sides(bad) -> None:
    """买卖双方的道具列表都要校验 —— 只校验一侧会让另一侧的脏数据进数据层。"""
    with pytest.raises(errcode.PandoraError):
        _trade(seller_items=[bad])
    with pytest.raises(errcode.PandoraError):
        _trade(seller_items=[], buyer_items=[bad], price=0)
