"""多币种货币原语测试 —— 对应 pandorapy/services/inventory/currency.py。

重点全部围绕"**无符号存储 + Python 无类型保护**"这一对矛盾:

  ① 减法:SQL 里绝不能出现 `amount = amount - ?`(UNSIGNED 列上负结果在非严格
     sql_mode 会被静默截断成 0 = 余额清零)。这条用**源码机械检查**守。
  ② 加法:有硬上限 MAX_CURRENCY_AMOUNT,越界返回 ERR_INVENTORY_CURRENCY_OVERFLOW,
     不回绕。
  ③ 乘法:单价 × 数量用溢出安全乘法。

  ④ 额外一条 Python 专有:Go 的 uint64 保证"负数不可能存在",Python 的 int 不保证。
     所有原语必须**显式**拒负数 —— 类型系统在这边一点忙都帮不上。
"""

from __future__ import annotations

import pathlib
import re

import pytest

from pandorapy import errcode
from pandorapy.services.inventory import currency as ccy
from pandorapy.services.inventory import fingerprint as ifp
from tests.srcprobe import code_text, module_code_text

GOLD = ccy.CURRENCY_GOLD
DIAMOND = ccy.CURRENCY_DIAMOND
HONOR = ccy.CURRENCY_HONOR


# ── ① 减法:SQL 里不许出现 amount 的自减 ───────────────────────────────────


def test_wallet_sql_never_subtracts_in_place() -> None:
    """★ 本文件里最要紧的一条,而且**只能**用源码检查守。

    `UPDATE player_wallet SET amount = amount - %s` 在 UNSIGNED 列上:
      - 严格模式   → Error 1690,写入失败(还算能发现)
      - 非严格模式 → **静默截断成 0**,等于把"扣款失败"变成"余额清零"

    第二种没有任何运行期信号,单测也测不出来(测试库是严格模式就永远碰不到)。
    所以判据只能是"这种写法根本不存在于源码里" —— 扣款一律
    「FOR UPDATE 锁行读出 → 在 Python 里比较 → 写绝对值」。
    """
    # 扫**整个 inventory 包**而不是只扫 currency.py。
    #
    # 今天钱包 SQL 确实只写在 currency.py 里,但这条纪律要防的是
    # "将来有人在 repo.py / repo_sql.py 里顺手写一句钱包自减" ——
    # 探针只盯一个文件的话,那种改动照样全绿通过,等于没守。
    pkg_dir = pathlib.Path(ccy.__file__).parent
    scanned = sorted(pkg_dir.glob("*.py"))
    assert len(scanned) > 1, f"包内只扫到 {len(scanned)} 个模块,探针没真正铺开"

    offenders = []
    wallet_absolute_write_seen = False
    for path in scanned:
        src = code_text(path)
        # 允许 `amount = %s`(写绝对值),禁止 `amount = amount ± ...`。
        if re.search(r"amount\s*=\s*amount\s*[-+]", src):
            offenders.append(path.name)
        if "SET amount = %s" in src:
            wallet_absolute_write_seen = True

    assert not offenders, (
        f"这些模块的 SQL 里出现了 amount 的原地加减: {offenders} —— "
        "UNSIGNED 列上负结果会被非严格 sql_mode 静默截断成 0"
    )
    assert wallet_absolute_write_seen, "写绝对值那条 UPDATE 不见了"


def test_wallet_table_is_not_the_legacy_one() -> None:
    """钱包表必须是 player_wallet。

    legacy 的 `player_currency` 是单列 gold、PK 只有 player_id,**没有**
    currency_kind / amount 两列;新装库(deploy/mysql-init)干脆不建它。
    打错表名的表现不是"报错",而是所有货币 SQL 在新库上 1146 / 在存量库上 1054。
    """
    assert ccy.WALLET_TABLE == "player_wallet"
    src = module_code_text(ccy)
    assert "player_currency" not in src


# ── ② 加法上限 ─────────────────────────────────────────────────────────────


def test_safe_add_caps_at_max_currency_amount() -> None:
    assert ccy.safe_add_uint64(1, 2) == (3, True)
    assert ccy.safe_add_uint64(ccy.MAX_CURRENCY_AMOUNT, 0) == (ccy.MAX_CURRENCY_AMOUNT, True)
    _, ok = ccy.safe_add_uint64(ccy.MAX_CURRENCY_AMOUNT, 1)
    assert not ok, "越过 2^62 没被判为溢出 —— 回绕会让首富瞬间变零元户"


def test_max_currency_amount_matches_go() -> None:
    """上限必须与 Go 的 MaxCurrencyAmount 同值。

    两边取值不同的后果:同一笔入账在一栈成功、另一栈报 OVERFLOW,
    而调用方(活动 / 战后结算)只会看到"偶发失败"。
    """
    assert ccy.MAX_CURRENCY_AMOUNT == 1 << 62


def test_safe_add_rejects_negative() -> None:
    """★ Go 的 uint64 让这条分支不存在;Python 必须显式写。"""
    assert ccy.safe_add_uint64(-1, 5)[1] is False
    assert ccy.safe_add_uint64(5, -1)[1] is False


# ── ③ 乘法 ─────────────────────────────────────────────────────────────────


def test_safe_mul_currency() -> None:
    assert ccy.safe_mul_currency(7, 3) == (21, True)
    assert ccy.safe_mul_currency(0, 10) == (0, True)
    assert ccy.safe_mul_currency(2, 2**61) == (2**62, True)
    assert ccy.safe_mul_currency(3, 2**61)[1] is False
    # 负数在 Go 侧不可能,在 Python 侧"算得出来"—— 必须显式拒。
    assert ccy.safe_mul_currency(-1, 5)[1] is False
    assert ccy.safe_mul_currency(5, -1)[1] is False


# ── ④ 币种校验:fail-closed,绝不回退金币 ─────────────────────────────────


@pytest.mark.parametrize("kind", [GOLD, DIAMOND, HONOR])
def test_known_kinds_pass(kind: int) -> None:
    ccy.validate_currency_kind(kind)


@pytest.mark.parametrize("kind", [ccy.CURRENCY_KIND_UNSPECIFIED, -1, 4, 999])
def test_unknown_kind_is_rejected_not_defaulted(kind: int) -> None:
    """★ UNSPECIFIED 与未知值一律拒,**不得回退成金币**。

    静默回退会让配错表的商品按金币扣钱 —— 玩家用金币买到了本该花钻石的东西,
    而账目两边都平,没有任何错误可查。
    """
    with pytest.raises(errcode.PandoraError) as ei:
        ccy.validate_currency_kind(kind)
    assert ei.value.code == errcode.ErrInvalidArg


# ── 余额快照编解码(落 ledger.result_currencies)────────────────────────────


def test_encode_decode_round_trip() -> None:
    balances = {DIAMOND: 7, GOLD: 3}
    raw = ccy.encode_balances(balances)
    assert ccy.decode_balances(raw) == balances


def test_empty_balances_encode_to_null() -> None:
    """空余额 → None(列写 NULL),不写一个空 message。

    省字节之外更重要的是:肉眼能区分"没记"与"记了全零"。
    """
    assert ccy.encode_balances({}) is None
    assert ccy.encode_balances({GOLD: 0}) is None
    assert ccy.decode_balances(None) == {}
    assert ccy.decode_balances(b"") == {}


def test_encode_is_stable_regardless_of_insert_order() -> None:
    """★ 编码顺序必须只由 kind 决定,不由 dict 插入序决定。

    不稳定的话,同一份余额会编出两串不同字节 —— 而这串字节的顺序同时决定
    下行协议顺序与(非金币路径的)幂等指纹。指纹一漂移,同一笔重试就被判冲突。
    """
    a = ccy.encode_balances({GOLD: 3, DIAMOND: 7})
    b = ccy.encode_balances({DIAMOND: 7, GOLD: 3})
    assert a == b
    assert ccy.balances_sorted({DIAMOND: 7, GOLD: 3}) == [(GOLD, 3), (DIAMOND, 7)]


def test_zero_entries_never_leak_into_snapshot() -> None:
    """零余额不外露:与"没有这一行"语义等价,避免下行出现一堆 0。"""
    assert ccy.balances_sorted({GOLD: 0, DIAMOND: 5}) == [(DIAMOND, 5)]
    assert ccy.decode_balances(ccy.encode_balances({GOLD: 5, DIAMOND: 0})) == {GOLD: 5}


def test_payload_byte_gate_rejects_oversized_snapshot() -> None:
    """写入侧字节闸(§9.24):超 256 字节拒写,不让半截 pb 落库。

    非严格 sql_mode 下超长会被静默截断成半截 pb,重放时解不出来 →
    幂等结果凭空变形。宁可拒写。
    """
    # 币种是枚举有界的,正常永远到不了;这里用超多伪币种把 payload 撑爆,
    # 验证的是"闸在",不是"业务上会发生"。
    huge = {k: 2**62 - 1 for k in range(1, 60)}
    with pytest.raises(errcode.PandoraError) as ei:
        ccy.encode_balances(huge)
    assert ei.value.code == errcode.ErrInternal


# ── describe:它同时进 detail 与非金币指纹,格式是契约 ─────────────────────


def test_describe_balances_format() -> None:
    assert ccy.describe_balances({}) == "none"
    assert ccy.describe_balances({GOLD: 3, DIAMOND: 7}) == "1:3,2:7"
    assert ccy.describe_balances({DIAMOND: 7, GOLD: 3}) == "1:3,2:7"


def test_gold_only_predicate_drives_legacy_fingerprint_format() -> None:
    """is_gold_only 是**旧指纹格式的适用条件**,不是业务判断。

    零额项必须忽略:{GOLD:0, DIAMOND:0} 与 {} 对指纹是同一件事,
    否则一次"没发钱"的发放会因为上游多塞了个 0 而算出新格式指纹。
    """
    assert ifp.is_gold_only({}) is True
    assert ifp.is_gold_only({GOLD: 100}) is True
    assert ifp.is_gold_only({GOLD: 100, DIAMOND: 0}) is True
    assert ifp.is_gold_only({DIAMOND: 1}) is False


# ── 商店 detail 编解码(购买幂等回放的唯一依据)────────────────────────────


def test_purchase_detail_round_trip() -> None:
    d = ifp.purchase_detail(1, 10001, 3, 30, [])
    assert d == "shop_buy shop=1 item=10001 units=3 count=30 inst="
    assert ifp.parse_purchase_detail(d) == (30, [])

    d2 = ifp.purchase_detail(1, 10156, 1, 0, [123, 124])
    assert d2 == "shop_buy shop=1 item=10156 units=1 count=0 inst=123,124"
    assert ifp.parse_purchase_detail(d2) == (0, [123, 124])


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "grant_inst ids=1",
        "shop_buy shop=1 item=2 units=3 count=4",  # 缺 inst= 段
        "shop_buy shop=x item=2 units=3 count=4 inst=",
        "shop_buy shop=1 item=2 units=3 count=4 inst=abc",
        "shop_buy shop=1 item=2 units=3 count=4 inst=0",  # id 0 不合法
    ],
)
def test_purchase_detail_parse_fails_closed(bad: str) -> None:
    """解析不出首次发货事实必须 **fail-closed**。

    编一个"发了什么"回去会让 UI 显示玩家其实没拿到的东西 ——
    比报一个内部错难查得多。
    """
    assert ifp.parse_purchase_detail(bad) is None
