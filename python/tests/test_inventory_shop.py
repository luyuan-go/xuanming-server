"""NPC 商店 biz / service 层测试 —— 对应 Go 侧 internal/biz/currency.go + service/shop.go。

重点:
  1. ★ 定价链只有一条:商店表 → unit_price × unit_count。**客户端不传价格**,
     所以根本不存在"客户端报价与服务端不一致"这个失败模式(§17.2)。
  2. ★ fail-closed:商店表未注入 / 商店不存在 / 不在售 / 币种未知,一律拒,不猜。
  3. ★ 同一商店里同一道具配了多档 → **加载期整批拒**;查询期 fail-closed 只是兜底。
  4. ★ 买入价必须严格高于回收总价 —— 否则"买进立刻卖出"是一条闭合的无限刷钱环,
     而且它**没有任何运行期信号**(每笔买卖单独看都合法),只能挡在加载期。
  5. 鉴权边界:GetShop 不鉴权(价目表是公开展示信息),PurchaseShopItem 以
     Envoy 注入的调用者身份为准,不信任请求体 player_id。
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import pathlib
import shutil

import pytest

from pandora.common.v1 import errcode_pb2 as commonpb
from pandora.inventory.v1 import inventory_pb2 as pb

from pandorapy import errcode
from pandorapy.services.inventory import biz as ibiz
from pandorapy.services.inventory import catalog as icat
from pandorapy.services.inventory import conf as iconf
from pandorapy.services.inventory import currency as ccy
from pandorapy.services.inventory import service as isvc
from pandorapy.services.inventory.currency_biz import ShopEntry
from pandorapy.services.inventory.models import ItemGrant, PurchaseOutcome

GOLD = ccy.CURRENCY_GOLD
DIAMOND = ccy.CURRENCY_DIAMOND

STACK_ITEM = 1001
EQUIP_ITEM = 10


class _FakeShops:
    """最小 ShopCatalog 替身。"""

    def __init__(self, entries: dict[int, list[ShopEntry]]) -> None:
        self._entries = entries

    def list_shop(self, shop_id: int):  # noqa: ANN201
        return self._entries.get(shop_id)

    def lookup_shop_entry(self, shop_id: int, item_config_id: int):  # noqa: ANN201
        found = None
        for e in self._entries.get(shop_id) or ():
            if e.item_config_id != item_config_id:
                continue
            if found is not None:
                return None  # 多档 → fail-closed
            found = e
        return found


@dataclasses.dataclass(frozen=True, slots=True)
class _Def:
    equipment: bool
    lobby_usable: bool = False
    battle_usable: bool = False
    sell_unit_price: int = 0
    max_stack: int = 99


class _FakeCatalog:
    def __init__(self, defs: dict[int, _Def]) -> None:
        self._defs = defs

    def lookup(self, item_config_id: int):  # noqa: ANN201
        return self._defs.get(item_config_id)

    def identify_rule(self, item_config_id: int):  # noqa: ANN201, ARG002
        return None


class _FakeSnowflake:
    """按下标填 id —— 只需要"每件一个唯一 id",不需要真雪花。"""

    def __init__(self) -> None:
        self.calls = 0

    def generate_into(self, buf: list[int]) -> None:
        for i in range(len(buf)):
            self.calls += 1
            buf[i] = 900 + self.calls


class _FakeRepo:
    def __init__(self) -> None:
        self.last_req = None

    async def purchase_shop_item(self, player_id: int, req):  # noqa: ANN001, ARG002
        self.last_req = req
        return (
            PurchaseOutcome(
                balances={req.kind: 42},
                cost=req.total_cost,
                kind=req.kind,
                items=[ItemGrant(item_config_id=req.item_config_id, count=req.total_items)]
                if not req.is_equipment
                else [],
            ),
            False,
        )


def _uc(shops=None, defs=None, sf=None, **cfg):  # noqa: ANN001
    uc = ibiz.InventoryUsecase(_FakeRepo(), iconf.InventoryConf(capacity=10, **cfg))
    uc.set_item_catalog(_FakeCatalog(defs or {STACK_ITEM: _Def(equipment=False)}))
    if shops is not None:
        uc.set_shop_catalog(_FakeShops(shops))
    if sf is not None:
        uc.set_snowflake(sf)
    return uc


def _entry(**kw) -> ShopEntry:
    base = dict(
        item_config_id=STACK_ITEM, count_per_unit=1, currency_kind=GOLD,
        unit_price=60, sort_order=10,
    )
    base.update(kw)
    return ShopEntry(**base)


# ── GetShop ────────────────────────────────────────────────────────────────


def test_get_shop_requires_catalog() -> None:
    """★ 未注入商店表即 fail-closed。

    一份可能与客户端漂移的兜底价格参与扣钱,比拒掉一次购买危险得多。
    """
    uc = _uc()
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.get_shop(1))
    assert ei.value.code == errcode.ErrInternal


def test_get_shop_rejects_zero_and_missing() -> None:
    uc = _uc(shops={1: [_entry()]})
    with pytest.raises(errcode.PandoraError) as zero:
        asyncio.run(uc.get_shop(0))
    assert zero.value.code == errcode.ErrInvalidArg
    with pytest.raises(errcode.PandoraError) as missing:
        asyncio.run(uc.get_shop(999))
    assert missing.value.code == errcode.ErrInvalidArg


def test_get_shop_returns_entries() -> None:
    uc = _uc(shops={1: [_entry(), _entry(item_config_id=1002, unit_price=80, sort_order=20)]})
    entries = asyncio.run(uc.get_shop(1))
    assert [(e.item_config_id, e.unit_price) for e in entries] == [(1001, 60), (1002, 80)]


# ── PurchaseShopItem:定价与形状 ────────────────────────────────────────────


def test_purchase_prices_from_the_table_not_the_client() -> None:
    """总价 = 表里的 unit_price × 份数;发放量 = count_per_unit × 份数。

    ★ 请求里**没有**价格字段,所以"客户端报价"这个攻击面根本不存在。
    """
    uc = _uc(shops={1: [_entry(unit_price=60, count_per_unit=10)]})
    outcome = asyncio.run(uc.purchase_shop_item(1, 1, STACK_ITEM, 3, "k"))
    req = uc._repo.last_req  # noqa: SLF001
    assert (req.total_cost, req.total_items, req.kind) == (180, 30, GOLD)
    assert outcome.cost == 180


def test_purchase_uses_the_entry_currency_kind() -> None:
    """币种取自商店表那一档,不是全局默认 —— "钻石商品"就该扣钻石。"""
    uc = _uc(shops={1: [_entry(currency_kind=DIAMOND, unit_price=5)]})
    asyncio.run(uc.purchase_shop_item(1, 1, STACK_ITEM, 2, "k"))
    assert uc._repo.last_req.kind == DIAMOND  # noqa: SLF001


def test_purchase_rejects_unknown_currency_in_the_table() -> None:
    """表里配了未知币种 → 拒,**不回退成金币**。

    静默回退会让配错表的商品按金币扣钱 —— 玩家用金币买到了本该花钻石的东西,
    账目两边还都平。
    """
    uc = _uc(shops={1: [_entry(currency_kind=0)]})
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.purchase_shop_item(1, 1, STACK_ITEM, 1, "k"))
    assert ei.value.code == errcode.ErrInvalidArg


def test_purchase_requires_catalog_and_key() -> None:
    uc = _uc()
    with pytest.raises(errcode.PandoraError) as no_shop:
        asyncio.run(uc.purchase_shop_item(1, 1, STACK_ITEM, 1, "k"))
    assert no_shop.value.code == errcode.ErrInternal

    uc2 = _uc(shops={1: [_entry()]})
    with pytest.raises(errcode.PandoraError) as no_key:
        asyncio.run(uc2.purchase_shop_item(1, 1, STACK_ITEM, 1, ""))
    assert no_key.value.code == errcode.ErrInvalidArg


@pytest.mark.parametrize("units", [0, -1])
def test_purchase_rejects_non_positive_units(units: int) -> None:
    """★ Go 侧 unit_count 是 uint32(`== 0` 即完整校验);Python 必须连负数一起拒。

    负份数会让 unit_price × units 得到负总价,一路穿到扣款处。
    """
    uc = _uc(shops={1: [_entry()]})
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.purchase_shop_item(1, 1, STACK_ITEM, units, "k"))
    assert ei.value.code == errcode.ErrInvalidArg


def test_purchase_units_limit_uses_a_precise_code() -> None:
    """份数上限用 NOT_PURCHASABLE 而不是含糊的参数溢出 —— 客户端能给出准确提示。"""
    uc = _uc(shops={1: [_entry()]}, max_shop_units_per_purchase=5)
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.purchase_shop_item(1, 1, STACK_ITEM, 6, "k"))
    assert ei.value.code == errcode.ErrInventoryNotPurchasable
    # 边界值本身放行。
    asyncio.run(uc.purchase_shop_item(1, 1, STACK_ITEM, 5, "k"))


def test_purchase_missing_shop_and_missing_item_share_one_code() -> None:
    """★ 商店不存在与"该商店不卖这个"刻意返回同一个码。

    区分开会把"哪些商店存在"变成可探测信息。
    """
    uc = _uc(shops={1: [_entry()]})
    for shop_id, item_id in ((999, STACK_ITEM), (1, 4242)):
        with pytest.raises(errcode.PandoraError) as ei:
            asyncio.run(uc.purchase_shop_item(1, shop_id, item_id, 1, "k"))
        assert ei.value.code == errcode.ErrInventoryNotPurchasable


def test_purchase_rejects_item_configured_twice_in_one_shop() -> None:
    """★ 同一商店里同一道具配了多档 → 拒(fail-closed)。

    购买请求只带 item_config_id,分不出档位;返回第一档等于"服务端替玩家选便宜的"。
    打包商品应当用独立的道具ID,而不是同一道具配两个单价。
    """
    uc = _uc(
        shops={1: [_entry(unit_price=60), _entry(unit_price=560, count_per_unit=10)]}
    )
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.purchase_shop_item(1, 1, STACK_ITEM, 1, "k"))
    assert ei.value.code == errcode.ErrInventoryNotPurchasable


def test_purchase_fails_closed_when_item_table_drifted() -> None:
    """商店表引用了道具表里不存在的道具 = 两张表批次漂移 → ErrInternal。"""
    uc = _uc(shops={1: [_entry(item_config_id=7777)]}, defs={})
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.purchase_shop_item(1, 1, 7777, 1, "k"))
    assert ei.value.code == errcode.ErrInternal


def test_purchase_cost_overflow_is_rejected() -> None:
    """单价 × 份数溢出必须在算总价那一步就拒,不能等到写库才发现。"""
    uc = _uc(shops={1: [_entry(unit_price=2**62)]}, max_shop_units_per_purchase=999)
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.purchase_shop_item(1, 1, STACK_ITEM, 2, "k"))
    assert ei.value.code == errcode.ErrInvalidArg


# ── 装备类商品 ─────────────────────────────────────────────────────────────


def test_purchase_equipment_mints_one_instance_per_unit() -> None:
    """装备类每份生成一件独立未鉴定实例 —— 与掉落 / 活动发放同一套实例模型,
    买来的装备照样要鉴定,不走捷径。"""
    sf = _FakeSnowflake()
    uc = _uc(
        shops={1: [_entry(item_config_id=EQUIP_ITEM, count_per_unit=1, unit_price=100)]},
        defs={EQUIP_ITEM: _Def(equipment=True)},
        sf=sf,
    )
    asyncio.run(uc.purchase_shop_item(1, 1, EQUIP_ITEM, 3, "k"))
    req = uc._repo.last_req  # noqa: SLF001
    assert req.is_equipment is True
    assert len(req.instance_ids) == 3 and len(set(req.instance_ids)) == 3
    assert req.total_items == 0


def test_purchase_equipment_needs_instance_bag_enabled() -> None:
    """未装配雪花(实例背包未启用)时买装备必须拒,而不是发一件没有 id 的东西。"""
    uc = _uc(
        shops={1: [_entry(item_config_id=EQUIP_ITEM, unit_price=100)]},
        defs={EQUIP_ITEM: _Def(equipment=True)},
    )
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.purchase_shop_item(1, 1, EQUIP_ITEM, 1, "k"))
    assert ei.value.code == errcode.ErrInvalidArg


def test_purchase_equipment_rejected_when_capacity_disabled() -> None:
    uc = ibiz.InventoryUsecase(_FakeRepo(), iconf.InventoryConf(capacity=0))
    uc.set_item_catalog(_FakeCatalog({EQUIP_ITEM: _Def(equipment=True)}))
    uc.set_shop_catalog(_FakeShops({1: [_entry(item_config_id=EQUIP_ITEM, unit_price=100)]}))
    uc.set_snowflake(_FakeSnowflake())
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(uc.purchase_shop_item(1, 1, EQUIP_ITEM, 1, "k"))
    assert ei.value.code == errcode.ErrInventoryCapacityFull


# ── service 层:鉴权边界 ───────────────────────────────────────────────────


class _Ctx:
    def __init__(self, player_id: int = 0) -> None:
        self._md = [("x-pandora-player-id", str(player_id))] if player_id else []

    def invocation_metadata(self):  # noqa: ANN201
        return self._md


class _ShopUsecase:
    def __init__(self) -> None:
        self.purchased_as = None

    async def get_shop(self, shop_id: int):  # noqa: ANN201, ARG002
        return [_entry(unit_price=60), _entry(item_config_id=1002, unit_price=80, sort_order=20)]

    async def purchase_shop_item(self, player_id, shop_id, item_config_id, unit_count, key):  # noqa: ANN001, ARG002
        self.purchased_as = player_id
        return PurchaseOutcome(
            balances={GOLD: 820}, cost=180, kind=GOLD,
            items=[ItemGrant(item_config_id=item_config_id, count=30)],
        )


def test_get_shop_is_not_gated_on_caller_identity() -> None:
    """★ 价目表本来就是要在客户端展示的公开信息;加鉴权只会让 DS / 工具查价变麻烦。

    与 Go 侧 service/shop.go 的边界一致 —— 它也不校验调用者。
    """
    svc = isvc.InventoryService(_ShopUsecase())
    resp = asyncio.run(svc.GetShop(pb.GetShopRequest(shop_id=1), _Ctx(0)))
    assert resp.code == commonpb.OK
    assert resp.shop_id == 1
    assert [(e.item_config_id, e.unit_price) for e in resp.entries] == [(1001, 60), (1002, 80)]
    assert resp.entries[0].currency_kind == GOLD


def test_purchase_requires_a_caller_and_ignores_nothing() -> None:
    """★ 动钱包的 RPC 以调用者身份为准,且请求体带了**别人的** player_id 直接拒。

    "忽略它"会让一次越权尝试悄悄变成一次正常的自购,攻击面上完全看不见。
    """
    uc = _ShopUsecase()
    svc = isvc.InventoryService(uc)

    # 无 JWT(内网直连)→ 未授权。
    r0 = asyncio.run(svc.PurchaseShopItem(pb.PurchaseShopItemRequest(shop_id=1), _Ctx(0)))
    assert r0.code == commonpb.ERR_UNAUTHORIZED

    # 请求体 player_id 与调用者不符 → 拒。
    r1 = asyncio.run(
        svc.PurchaseShopItem(pb.PurchaseShopItemRequest(player_id=9, shop_id=1), _Ctx(7))
    )
    assert r1.code == commonpb.ERR_PERMISSION_DENY

    # 请求体不带 player_id → 用调用者身份。
    r2 = asyncio.run(
        svc.PurchaseShopItem(
            pb.PurchaseShopItemRequest(shop_id=1, item_config_id=1001, unit_count=3,
                                       idempotency_key="k"),
            _Ctx(7),
        )
    )
    assert r2.code == commonpb.OK
    assert uc.purchased_as == 7
    assert (r2.cost.kind, r2.cost.amount) == (GOLD, 180)
    assert (r2.balance.kind, r2.balance.amount) == (GOLD, 820)
    assert [(g.item_config_id, g.count) for g in r2.granted_items] == [(1001, 30)]


# ── 真实配置表:加载 + 排序 + 当前批次干净 ─────────────────────────────────
#
# ★ 这一节与下一节是**两层**,谁也证明不了谁:
#     真实 dist 断言的是"当前发布数据干净"(数据回归);
#     合成批次断言的是"闸门本身有效"(代码回归)。
#   只留前者:闸门被误删也照样绿(因为数据本来就没触发它)。
#   只留后者:策划把表配成刷钱档时没有任何测试会红。


def test_real_dist_shop_table_loads_and_sorts(configtable_dist) -> None:
    """用**真实** dist 批次验加载器与排序 —— 造假数据只能验代码自己。

    排序在服务端定死(sort_order → item_config_id):商品顺序是策划的展示意图,
    两端各排一次迟早会因并列项的处理差异而不一致。
    """
    store = icat.Store(icat.load_tables(configtable_dist).tables, str(configtable_dist))
    entries = store.list_shop(1)
    assert entries, "真实 dist 里 shop_id=1 应当有在售商品"
    assert [e.sort_order for e in entries] == sorted(e.sort_order for e in entries)
    for e in entries:
        assert e.unit_price > 0 and e.count_per_unit >= 1
        ccy.validate_currency_kind(e.currency_kind)


def test_real_dist_shop_table_is_clean(configtable_dist) -> None:
    """★ 当前发布批次必须**不含**重复档、也不含买入价 <= 回收总价的档。

    load_tables 本身已经会拒掉这两类数据(见下一节的合成批次用例),所以本条真正的
    作用是把失败信息落到**具体哪一档**上 —— 只靠加载期抛异常的话,策划看到的是
    "整批拒载",而这里能直接指出是哪个商店哪件道具。

    历史:这条用例曾经反过来断言"真表里存在重复档"(shop_id=1 的道具 10001 配了
    单买 60 / 10 个装 560 两档),但那份数据从未发布过 —— 表被重导后用例就永远红。
    断言"当前数据干净"才是能长期成立的口径。
    """
    result = icat.load_tables(configtable_dist)
    store = icat.Store(result.tables, str(configtable_dist))
    items = result.tables.items
    for shop_id in sorted(store.tables.shops):
        seen: set[int] = set()
        for e in store.list_shop(shop_id):
            assert e.item_config_id not in seen, (
                f"商店 {shop_id} 的道具 {e.item_config_id} 配了多档:购买请求无法区分档位,"
                f"该道具会永远买不了;打包商品请用独立道具 ID"
            )
            seen.add(e.item_config_id)
            if e.currency_kind != GOLD:
                continue  # 非金币计价与金币回收不可比(与加载期口径一致)
            recycle = items[e.item_config_id].sell_price * e.count_per_unit
            assert e.unit_price > recycle, (
                f"商店 {shop_id} 的道具 {e.item_config_id} 买入价 {e.unit_price} "
                f"<= 回收总价 {recycle}:买进立刻卖出即可刷钱"
            )


# ── 合成批次:证明两条加载期闸门本身有效 ───────────────────────────────────


# load_tables 只读这五个文件(其余 dist 表与本服务无关),所以合成批次只复制它们 ——
# 整棵 dist 复制一遍要 650KB,而这里每个用例都要建一份。
_LOADED_FILES = ("manifest.json", "item.json", "equipment_affix.json", "role_attr_map.json",
                 "shop.json")


def _fork_dist(src: pathlib.Path, dst: pathlib.Path, mutate) -> pathlib.Path:  # noqa: ANN001
    """从真实 dist 派生一份改过 shop.json 的批次(checksum / rows 同步重算)。

    ★ 必须重算 manifest 里的 checksum 与 rows:加载器会先验这两项,不重算的话
      用例会**在校验层就红**,根本走不到我们想验的商店闸 —— 那是一条假绿(假红)。
    """
    dst.mkdir(parents=True, exist_ok=True)
    for name in _LOADED_FILES:
        shutil.copyfile(src / name, dst / name)

    shop = json.loads((dst / "shop.json").read_text("utf-8"))
    mutate(shop["rows"])
    raw = json.dumps(shop, ensure_ascii=False).encode("utf-8")
    (dst / "shop.json").write_bytes(raw)

    manifest = json.loads((dst / "manifest.json").read_text("utf-8"))
    for t in manifest["tables"]:
        if t["name"] == "shop":
            t["rows"] = len(shop["rows"])
            t["checksum"] = "sha256:" + hashlib.sha256(raw).hexdigest()
    (dst / "manifest.json").write_bytes(json.dumps(manifest, ensure_ascii=False).encode("utf-8"))
    return dst


def test_duplicate_shop_entry_rejects_the_whole_batch(configtable_dist, tmp_path) -> None:
    """★ 同店同道具配两档 → **整批拒载**,而不是"这件道具买不了、其余照常上线"。

    购买期挡只会让个别商品报错,配错的表照样上线;加载期拒批次会保留上一份正确配置
    继续服务(§9.15 加载成功才切换),策划立刻看到失败原因。
    """
    def add_dupe(rows: list) -> None:
        first = dict(rows[0])
        first["id"] = max(r["id"] for r in rows) + 1
        first["unit_price"] = str(int(first["unit_price"]) * 10)
        first["count_per_unit"] = 10
        rows.append(first)

    active = _fork_dist(configtable_dist, tmp_path / "dupe", add_dupe)
    with pytest.raises(icat.ConfigTableError) as ei:
        icat.load_tables(active)
    assert "配了多档" in str(ei.value)


def test_underpriced_shop_entry_rejects_the_whole_batch(configtable_dist, tmp_path) -> None:
    """★ 买入价 <= 回收总价 → 整批拒载(无限刷钱环,没有任何运行期信号)。

    这里刻意把买入价压到**恰等于**回收总价:判据必须是"严格高于",配成相等时
    买进卖出不亏不赚,已经足以用来洗额度/刷任务量。
    """
    items = {r.id: r for r in icat.load_tables(configtable_dist).tables.items.values()}

    def underprice(rows: list) -> None:
        row = rows[0]
        assert row["currency_kind"] == GOLD
        recycle = items[row["item_config_id"]].sell_price * row["count_per_unit"]
        assert recycle > 0, "本用例需要一件 sell_price > 0 的道具"
        row["unit_price"] = str(recycle)

    active = _fork_dist(configtable_dist, tmp_path / "cheap", underprice)
    with pytest.raises(icat.ConfigTableError) as ei:
        icat.load_tables(active)
    assert "买进立刻卖出即可刷钱" in str(ei.value)


def test_non_gold_entry_is_not_compared_against_gold_recycle(configtable_dist, tmp_path) -> None:
    """非金币计价的档**不比**金币回收价 —— 那是汇率设计,不能靠这条不变量兜。

    (否则钻石标价 1 的商品会因为"1 <= 金币回收价"被误拒,整批加载不了。)
    """
    def to_diamond(rows: list) -> None:
        rows[0]["currency_kind"] = DIAMOND
        rows[0]["unit_price"] = "1"

    active = _fork_dist(configtable_dist, tmp_path / "diamond", to_diamond)
    assert icat.load_tables(active).tables.shops  # 不抛即为通过


def test_lookup_shop_entry_fails_closed_on_duplicate() -> None:
    """查询期兜底:万一重复档绕过了加载期闸,lookup 必须返回 None 而不是挑一档。

    ★ 这条不能靠真实 dist 来证明(当前数据是干净的,永远走不到这个分支),
      只能用合成 Tables —— 与 Go 的 ShopEntryOf 同语义。
    """
    tables = icat.Tables(
        version=1, source_rev="test", items={}, affix_by_pool={}, role_attrs={},
        shops={1: [_entry(unit_price=60), _entry(unit_price=560, count_per_unit=10)]},
    )
    store = icat.Store(tables, "test")
    assert store.lookup_shop_entry(1, STACK_ITEM) is None
    # 同一件道具只配一档时**必须取得到** —— 否则上面那个 None 可能只是"这个查询恒空",
    # 证明不了 fail-closed 分支真的被走到了。
    tables.shops[2] = [_entry(unit_price=60)]
    assert store.lookup_shop_entry(2, STACK_ITEM) is not None
