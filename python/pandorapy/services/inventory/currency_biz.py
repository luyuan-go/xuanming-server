"""多币种货币与 NPC 商店的业务规则 —— 对应 Go 侧 internal/biz/currency.go(2026-08-22)。

本文件只放"规则",不放 SQL:余额怎么加减、锁怎么拿、幂等怎么落,全在 repo /
currency.py。这里负责的是:币种取哪个、价格从哪张表读、份数上限、溢出闸、以及
"不可出售 / 不在售"这类由配置裁决的业务判定,外加 proto ↔ 内部结构的货币转换。

★ 为什么做成 mixin 而不是自由函数:GetShop / PurchaseShopItem 要读 `self._shops`
  (商店表)、`self._sf`(雪花)、`self._cfg`(上限)、`self._catalog`(道具表),
  与 InventoryUsecase 是同一批依赖。拆成自由函数就得把这四样逐个传进去,
  调用点会比现在更长且更容易漏传。`__slots__ = ()` 保证不给实例引入 __dict__。
"""

from __future__ import annotations

import dataclasses
from typing import Protocol

from pandora.common.v1 import currency_pb2 as _cur_pb2

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy.services.inventory import currency as ccy
from pandorapy.services.inventory.models import PurchaseRequest

# DEFAULT_MAX_CURRENCY_PER_GRANT 单次发放某币种的默认上限(1e12,与 Go 同值)。
#
# 取值依据:即便全服每人每天入账 100 万,单笔也到不了 1e12;真到这个量级只可能是
# 上游算错(战后结算 / 活动 / 补偿脚本把倍率或单位算错)。它拦的不是玩家
# (玩家无法直接调 GrantItems),而是**上游算错**——没有这道闸就会把天文数字灌进
# 经济体,而且事后无法区分"正常大额"与"算错"。
DEFAULT_MAX_CURRENCY_PER_GRANT = 1_000_000_000_000

# DEFAULT_MAX_SHOP_UNITS_PER_PURCHASE 单次商店购买的默认份数上限。
#
# 份数是价格乘数,不设上限时一次请求就能顶到溢出闸;有上限则错误码更精确
# (ERR_INVENTORY_NOT_PURCHASABLE 而不是含糊的参数溢出)。
DEFAULT_MAX_SHOP_UNITS_PER_PURCHASE = 999


@dataclasses.dataclass(frozen=True, slots=True)
class ShopEntry:
    """商店表投影出来的一档商品(biz 只取自己要用的列,不把整行配置外露)。"""

    item_config_id: int
    count_per_unit: int
    currency_kind: int
    unit_price: int
    sort_order: int


class ShopCatalog(Protocol):
    """商店表的只读热更视图(生产实现 = configtable shop 表适配器)。

    与 ItemCatalog 同一纪律:**未注入即 fail-closed** —— 一份可能与客户端漂移的
    兜底价格参与扣钱,比拒掉一次购买危险得多。
    """

    def list_shop(self, shop_id: int) -> list[ShopEntry] | None:
        """某商店的全部在售商品(按 sort_order、再按 item_config_id 升序)。

        None 表示该商店不存在(与"存在但空"合并成同一件事,见 get_shop 注释)。
        """

    def lookup_shop_entry(self, shop_id: int, item_config_id: int) -> ShopEntry | None:
        """精确取某商店里某道具的档位。None 表示该商店不卖这个。"""


# ── proto ↔ 内部结构的货币转换(service 层用;放这里是为了让转换规则只有一份)──


def balances_from_proto(items) -> dict[int, int]:  # noqa: ANN001
    """上行的 CurrencyAmount 列表 → {kind: amount}。对应 Go 的 BalancesFromProto。

    这是**唯一**的类型边界:proto 的 amount 已是 uint64(protobuf 运行期就会拒负数),
    所以这里校验的是语义合法性 —— 币种必须已知、数量必须为正、同一币种不得重复出现。

    ★ 重复币种必须**拒**而不是相加:客户端 / 上游发出 {GOLD:10, GOLD:20} 时,
      "相加成 30"和"取后者 20"都是猜,猜错就是发错钱。
    """
    out: dict[int, int] = {}
    for it in items:
        kind = int(it.kind)
        ccy.validate_currency_kind(kind)
        amount = int(it.amount)
        if amount == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "currency amount must be positive: kind=%d", kind
            )
        if kind in out:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "duplicated currency kind: %d", kind
            )
        out[kind] = amount
    return out


def balances_to_proto(balances) -> list[_cur_pb2.CurrencyAmount]:  # noqa: ANN001
    """{kind: amount} → 按 kind 升序的下行列表(只含非零项)。"""
    return ccy.balances_to_amounts(balances)


def currency_amount_proto(kind: int, amount: int) -> _cur_pb2.CurrencyAmount:
    """组一个单币种下行值(balance / earned / cost 用)。"""
    return _cur_pb2.CurrencyAmount(kind=kind, amount=amount)


def describe_currencies(balances) -> str:  # noqa: ANN001
    """人读摘要(日志用)。与 currency.describe_balances 同格式。"""
    return ccy.describe_balances(balances)


class ShopUsecaseMixin:
    """InventoryUsecase 的货币规则 + NPC 商店部分。"""

    __slots__ = ()

    # ── 配置读取(留空即取默认,与 Go 的三个 getter 逐条对应)──

    def _sell_currency_kind(self) -> int:
        """出售道具的结算币种(配置留空 → 金币)。"""
        configured = int(getattr(self._cfg, "sell_currency_kind", 0) or 0)
        return ccy.CURRENCY_GOLD if configured == 0 else configured

    def _max_currency_per_grant(self) -> int:
        configured = int(getattr(self._cfg, "max_currency_per_grant", 0) or 0)
        return DEFAULT_MAX_CURRENCY_PER_GRANT if configured == 0 else configured

    def _max_shop_units_per_purchase(self) -> int:
        configured = int(getattr(self._cfg, "max_shop_units_per_purchase", 0) or 0)
        return DEFAULT_MAX_SHOP_UNITS_PER_PURCHASE if configured == 0 else configured

    # ── 注入 ──────────────────────────────────────────────────────────────

    def set_shop_catalog(self, catalog: ShopCatalog | None) -> None:
        """注入商店表。不注入 → get_shop / purchase_shop_item 一律拒。"""
        self._shops = catalog

    # ── NPC 商店 ──────────────────────────────────────────────────────────

    async def get_shop(self, shop_id: int) -> list[ShopEntry]:
        """读某商店的权威价目表(客户端据此渲染,与扣费同源)。

        展示口径与扣费口径同源:旧的客户端本地商店从道具表读 SellPrice 当买入价,
        与服务端毫无关系,改表后两边会静默漂移(§17.3)。
        """
        if shop_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "shop_id required")
        if self._shops is None:
            raise errcode.PandoraError(errcode.ErrInternal, "shop catalog not configured")
        entries = self._shops.list_shop(shop_id)
        if not entries:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "shop not found: %d", shop_id)
        return entries

    async def purchase_shop_item(
        self, player_id: int, shop_id: int, item_config_id: int, unit_count: int, idempotency_key: str
    ):
        """服务端权威的 NPC 商店购买。返回 data 层的 PurchaseOutcome。

        定价链:商店表 (shop_id, item_config_id) → unit_price × unit_count = 总价。
        **客户端不传价格**,所以没有"客户端报价与服务端不一致"这个失败模式(§17.2)。

        装备类商品每份生成一件独立未鉴定实例:与掉落 / 活动发放同一套实例模型,
        买来的装备照样要鉴定,不走捷径。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if shop_id == 0 or item_config_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "shop_id and item_config_id required"
            )
        if unit_count <= 0:
            # Go 侧 unit_count 是 uint32(`== 0` 即完整校验)。Python 没有类型保护,
            # 所以这里必须连负数一起拒:负份数会让总价乘出负数,一路穿到扣款处。
            raise errcode.PandoraError(errcode.ErrInvalidArg, "unit_count must be positive")
        if unit_count > self._max_shop_units_per_purchase():
            raise errcode.PandoraError(
                errcode.ErrInventoryNotPurchasable,
                "unit_count exceeds per-purchase limit: got=%d limit=%d",
                unit_count,
                self._max_shop_units_per_purchase(),
            )
        if not idempotency_key:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "idempotency_key required")
        if self._shops is None:
            raise errcode.PandoraError(errcode.ErrInternal, "shop catalog not configured")

        entry = self._shops.lookup_shop_entry(shop_id, item_config_id)
        if entry is None:
            # 商店不存在、或该商店不卖这件东西 —— 对客户端是同一件事:你不能在这里买它。
            # 刻意不区分,避免把"哪些商店存在"变成可探测信息。
            raise errcode.PandoraError(
                errcode.ErrInventoryNotPurchasable,
                "item not on sale shop=%d item=%d",
                shop_id,
                item_config_id,
            )
        ccy.validate_currency_kind(entry.currency_kind)
        if entry.unit_price <= 0 or entry.count_per_unit <= 0:
            # 表加载期已校验过(shop.proto 要求 required + >=1),走到这里说明适配器有 bug。
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "invalid shop entry shop=%d item=%d price=%d count=%d",
                shop_id,
                item_config_id,
                entry.unit_price,
                entry.count_per_unit,
            )

        total_cost, ok = ccy.safe_mul_currency(entry.unit_price, unit_count)
        if not ok:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "purchase cost overflow shop=%d item=%d price=%d units=%d",
                shop_id,
                item_config_id,
                entry.unit_price,
                unit_count,
            )

        definition = self._item_definition(item_config_id)
        if definition is None:
            # 商店表的外键校验保证道具存在;这里查不到 = 两张表批次漂移,fail-closed。
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "shop item missing from item table shop=%d item=%d",
                shop_id,
                item_config_id,
            )

        total_units, ok = ccy.safe_mul_currency(entry.count_per_unit, unit_count)
        if not ok or total_units == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "purchase quantity overflow shop=%d item=%d",
                shop_id,
                item_config_id,
            )

        req = PurchaseRequest(
            shop_id=shop_id,
            item_config_id=item_config_id,
            unit_count=unit_count,
            kind=entry.currency_kind,
            total_cost=total_cost,
            is_equipment=definition.equipment,
            capacity=self._cfg.capacity,
            idempotency_key=idempotency_key,
        )
        if definition.equipment:
            if self._sf is None:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "instance inventory disabled: cannot purchase equipment item=%d",
                    item_config_id,
                )
            if self._cfg.capacity <= 0:
                raise errcode.PandoraError(
                    errcode.ErrInventoryCapacityFull,
                    "instance inventory disabled (capacity<=0) player=%d",
                    player_id,
                )
            # 每件一个雪花 ID;一次 CAS 预留整段,避免逐件 CAS(与 grant_instances 同法)。
            # 产出严格递增且唯一,但不保证连续(跨秒有空洞);此处只按下标一一对应取用。
            instance_ids = [0] * total_units
            self._sf.generate_into(instance_ids)
            req.instance_ids = instance_ids
        else:
            req.total_items = total_units

        outcome, already = await self._repo.purchase_shop_item(player_id, req)
        if already:
            plog.get().info(
                "shop_purchase_idempotent_hit",
                player_id=player_id,
                shop_id=shop_id,
                item=item_config_id,
                idempotency_key=idempotency_key,
                cost=outcome.cost,
            )
        return outcome
