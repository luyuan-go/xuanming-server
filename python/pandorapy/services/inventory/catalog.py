"""道具规则的唯一权威 —— 与 UE 同源的 configtable item / equipment_affix 表。

对应 Go 侧 `cmd/inventory/configtable.go`(catalog 适配器 + 整批校验器)
和 `pkg/configtable`(manifest + sha256 + 整批 fail-closed 的加载语义)。

★ 为什么 inventory 必须强依赖配置表(main.py 缺 config_table.dir 直接拒启):
  道具的 type / max_stack / sell_price / usable / identify_pool_id 是**扣减和入账
  的裁决依据**。一份可能与客户端漂移的 YAML 兜底数值参与扣减,比拒掉一次请求
  危险得多 —— 曾经的 `item_rules` YAML 表已随 ca8f6304 整块删除,不要再加回来。

★ 加载语义逐条照抄 Go 的 Store.Load,任一失败**整批不切换**:
    manifest 缺表 / proto 名不符 / 文件缺失 / checksum 不符 / protojson 解析失败 /
    行数与 manifest 声明不符 / 整批校验器不过。
  manifest 未列出的 *.json 视为脏数据,**只告警不拒载**(hotreload doc §5)。

★ 复用 pandorapy.configtable 的 read_manifest / verify_checksum,不再抄一份:
  checksum 的字节口径(含尾换行的 LF 全字节 sha256)已经踩过一次事故,
  两份实现必然漂移,而漂移的表现是"Go 版拒载的批次 Python 版放行"。
"""

from __future__ import annotations

import dataclasses
import pathlib

from google.protobuf import json_format

from pandora.config.v1 import equipment_affix_pb2 as _affix_pb2
from pandora.config.v1 import item_pb2 as _item_pb2
from pandora.config.v1 import role_attr_map_pb2 as _attr_pb2
from pandora.config.v1 import shop_pb2 as _shop_pb2

from pandorapy.configtable import ConfigTableError, MANIFEST_FILE_NAME, ReloadMutex, read_manifest, verify_checksum
from pandorapy.services.inventory import currency as _ccy
from pandorapy.services.inventory.currency_biz import ShopEntry

# 装备鉴定池的总权重上限(与 Go 侧 1_000_000 同值)。
# 不设的话,一条权重写成 2^62 的行会让加权抽取整型溢出/永远抽中它。
MAX_POOL_TOTAL_WEIGHT = 1_000_000

# 当前战斗属性映射只为这三类定义了权威单位。新增属性必须先实现 UE 应用/卸载对账,
# 再改这里放行 —— 否则配表能悄悄造出"只显示不生效"的词条。
# ★ 数值与 code_name 都从 Go 逐字抄:放宽一个 id 就等于放行一类无对账语义的词条。
ALLOWED_ATTRS: dict[int, str] = {3: "Atk", 7: "MoveSpeedRate", 9: "Defense"}

# 装备类型枚举 —— 从 pb 引用,**不手抄数值**(本仓刚修完 13 处手抄错位)。
ITEM_TYPE_EQUIPMENT: int = _item_pb2.ITEM_TYPE_EQUIPMENT

# item.proto / pkg/configtable/item.go 的回血组合契约，直接引用生成枚举避免跨语言漂移。
ITEM_HEAL_TYPE_UNSPECIFIED: int = _item_pb2.ITEM_HEAL_TYPE_UNSPECIFIED
ITEM_HEAL_TYPE_FIXED: int = _item_pb2.ITEM_HEAL_TYPE_FIXED
ITEM_HEAL_TYPE_MAX_HP_PERCENT: int = _item_pb2.ITEM_HEAL_TYPE_MAX_HP_PERCENT
MAX_CLIENT_FIXED_HEAL_HP = (1 << 31) - 1


@dataclasses.dataclass(frozen=True, slots=True)
class ItemDefinition:
    """inventory 真正需要的道具表最小投影(对应 Go 的 biz.ItemDefinition)。

    展示字段(icon / description / 模型挂点)不进入本域。
    """

    equipment: bool
    # lobby_usable 只有存在**服务端大厅效果处理器**时才可为真。
    # item.usable 的真实语义是"局内 UE GAS 可消费",大厅没有效果派发器,
    # 所以配置表适配器必须保持本字段恒为 False —— 否则 UseItem 会扣掉道具
    # 而效果一个也不发生(玩家的东西凭空消失)。
    lobby_usable: bool
    # battle_usable 表示可由**可信战斗进度事实**触发持久扣减;
    # 它不赋予玩家 JWT 直接调用大厅 UseItem 的权限。
    battle_usable: bool
    sell_unit_price: int
    max_stack: int


@dataclasses.dataclass(frozen=True, slots=True)
class IdentifyAttrDefinition:
    """鉴定池的一条候选词条。weight 参与加权不放回抽取;min/max 是服务端最终写入的闭区间。"""

    attr_id: int
    weight: int
    min: int
    max: int


@dataclasses.dataclass(frozen=True, slots=True)
class IdentifyDefinition:
    """某件装备当前热更批次对应的鉴定规则。"""

    attr_count: int
    pool: list[IdentifyAttrDefinition]


@dataclasses.dataclass(slots=True)
class Tables:
    """一个已校验的原子批次。热更时整份替换,读侧永远看到自洽的一批。"""

    version: int
    source_rev: str
    items: dict[int, _item_pb2.ItemRow]
    # affix_by_pool 按 pool_id 聚合(表内顺序保留 —— attr_count 取 rows[0] 与 Go 一致)。
    affix_by_pool: dict[int, list[_affix_pb2.EquipmentAffixRow]]
    role_attrs: dict[int, _attr_pb2.RoleAttrMapRow]
    # shops 按 shop_id 聚合,组内已按「排序 → 道具ID」升序(与 Go 的 ShopEntriesOf 同序)。
    # ★ 排序在**服务端**定死而不是让客户端自己排:商品顺序是策划的展示意图,
    #   两端各排一次迟早会因并列项的处理差异而不一致。
    shops: dict[int, list[ShopEntry]] = dataclasses.field(default_factory=dict)

    def item_count(self) -> int:
        return len(self.items)

    def affix_count(self) -> int:
        return sum(len(rows) for rows in self.affix_by_pool.values())

    def pool_count(self) -> int:
        return len(self.affix_by_pool)


@dataclasses.dataclass(slots=True)
class LoadResult:
    version: int
    source_rev: str
    warnings: list[str]
    tables: Tables


# ── 加载 ───────────────────────────────────────────────────────────────────

_TABLE_PROTOS: dict[str, tuple[str, type]] = {
    "item": ("pandora.config.v1.ItemTableData", _item_pb2.ItemTableData),
    "equipment_affix": (
        "pandora.config.v1.EquipmentAffixTableData",
        _affix_pb2.EquipmentAffixTableData,
    ),
    "role_attr_map": ("pandora.config.v1.RoleAttrMapTableData", _attr_pb2.RoleAttrMapTableData),
    "shop": ("pandora.config.v1.ShopTableData", _shop_pb2.ShopTableData),
}


def _load_one(active: pathlib.Path, manifest, name: str):  # noqa: ANN001, ANN202
    mt = manifest.tables.get(name)
    if mt is None:
        raise ConfigTableError(f"manifest 缺少本进程必需的表 {name!r},整批拒绝")
    expect_proto, container_cls = _TABLE_PROTOS[name]
    if mt.proto != expect_proto:
        raise ConfigTableError(
            f"{name} 表 proto 不符: 期望 {expect_proto} 实际 {mt.proto}(接错文件?)"
        )
    path = active / mt.file
    if not path.is_file():
        raise ConfigTableError(f"manifest 列出的表文件不存在: {path}")
    raw = path.read_bytes()
    verify_checksum(raw, mt.checksum)

    container = container_cls()
    try:
        # ★ ignore_unknown_fields=True —— 与 Go 侧 protojson + DiscardUnknown 一致,
        # **不是**放松校验。严格校验属于生成阶段;运行期必须容忍新增列,否则
        # 标准发布序(先发配置、再滚二进制)会让尚未滚上的旧进程整批拒载。
        json_format.Parse(raw.decode("utf-8"), container, ignore_unknown_fields=True)
    except json_format.ParseError as exc:
        raise ConfigTableError(f"{path} protojson 解析失败: {exc}") from exc

    rows = list(container.rows)
    # ★ 判据是 `len(rows) != mt.rows`,**不是** `if mt.rows and ...`。
    # 后者会让 manifest 声明 rows=0 的表整条跳过行数校验 —— 而这道校验防的正是
    # "发布拷贝被截断":截成 0 行的表配上声明 0 行的 manifest,两边"自洽",
    # 服务照常启动,道具表整个是空的。
    if len(rows) != mt.rows:
        raise ConfigTableError(f"{name} 表行数不符: manifest 声明 {mt.rows} 实际 {len(rows)}")
    return rows


def load_tables(active_dir: str | pathlib.Path, expect_version: int = 0) -> LoadResult:
    """加载 inventory 需要的三张表(item / equipment_affix / role_attr_map)。

    整批 fail-closed:任一条不过都抛 ConfigTableError,不返回半个批次。
    """
    active = pathlib.Path(active_dir)
    if not active.is_dir():
        raise ConfigTableError(f"配置表目录不存在: {active}")

    manifest = read_manifest(active)
    if expect_version and manifest.version != expect_version:
        raise ConfigTableError(f"manifest 版本不符: 期望 {expect_version} 实际 {manifest.version}")

    item_rows = _load_one(active, manifest, "item")
    affix_rows = _load_one(active, manifest, "equipment_affix")
    attr_rows = _load_one(active, manifest, "role_attr_map")
    shop_rows = _load_one(active, manifest, "shop")

    items: dict[int, _item_pb2.ItemRow] = {}
    for row in item_rows:
        if row.id in items:
            raise ConfigTableError(f"item 表 id 重复: {row.id}")
        items[row.id] = row

    affix_by_pool: dict[int, list[_affix_pb2.EquipmentAffixRow]] = {}
    for row in affix_rows:
        affix_by_pool.setdefault(row.pool_id, []).append(row)

    role_attrs: dict[int, _attr_pb2.RoleAttrMapRow] = {}
    for row in attr_rows:
        if row.id in role_attrs:
            raise ConfigTableError(f"role_attr_map 表 id 重复: {row.id}")
        role_attrs[row.id] = row

    shops = _index_shop_rows(shop_rows, items)

    tables = Tables(
        version=manifest.version,
        source_rev=manifest.source_rev,
        items=items,
        affix_by_pool=affix_by_pool,
        role_attrs=role_attrs,
        shops=shops,
    )
    validate_inventory_tables(tables)

    listed = {MANIFEST_FILE_NAME} | {t.file for t in manifest.tables.values()}
    warnings = [
        f"active 目录存在 manifest 未列出的文件 {p.name!r}(脏数据)"
        for p in sorted(active.glob("*.json"))
        if p.name not in listed
    ]
    return LoadResult(
        version=manifest.version, source_rev=manifest.source_rev, warnings=warnings, tables=tables
    )


# ── 整批门禁(启动与热更共用)──────────────────────────────────────────────


def validate_inventory_tables(t: Tables) -> None:
    """对应 Go 的 validateInventoryTables。任一条不过 → 整批拒绝切换。

    不设这些闸会怎样(逐条):
      ① 属性白名单:配表能造出没有 UE 应用/卸载对账语义的词条 ——
         玩家面板上看得见,实际战斗里一点用没有,而且没人会发现。
      ② 池内 attr_count 一致:同池两行写了不同 attr_count 时,"抽几条"取决于
         哪一行先被读到 —— 同一件装备的鉴定结果随表行序变化。
      ③ 池内 attr_id 不重复:不放回抽取会重复抽到同一属性,词条互相覆盖。
      ④ 总权重上限:权重和溢出会让加权抽取退化成"永远抽中最大那条"。
      ⑤ attr_count ≤ 唯一候选数:抽不满时 rollIdentifyAttrs 返回空 →
         装备被永久写成 identified=true 且零词条(不可逆)。
      ⑥ 装备必须引用存在的池:缺池 = 鉴定拿不到规则,同样是"identified 但零词条"。
      ⑦ 孤儿池:没人引用的池说明表在漂移(改了 item 没改 affix 或反之)。
    """
    if not t.items or not t.role_attrs or not t.affix_by_pool:
        raise ConfigTableError("item / role_attr_map / equipment_affix tables required")

    for attr_id, code_name in ALLOWED_ATTRS.items():
        row = t.role_attrs.get(attr_id)
        if row is None or row.code_name != code_name:
            raise ConfigTableError(
                f"role_attr_map id {attr_id} must be {code_name!r} for equipment gameplay semantics"
            )

    for item in t.items.values():
        _validate_item_heal(item)

    # pool_id → (attr_count, {attr_id}, total_weight)
    pool_attr_count: dict[int, int] = {}
    pool_attrs: dict[int, set[int]] = {}
    pool_total: dict[int, int] = {}
    for pool_id, rows in t.affix_by_pool.items():
        for row in rows:
            if row.attr_id not in ALLOWED_ATTRS:
                raise ConfigTableError(
                    f"equipment_affix row {row.id} attr_id {row.attr_id} has no gameplay "
                    f"apply/reconcile semantics"
                )
            if pool_id not in pool_attr_count:
                pool_attr_count[pool_id] = row.attr_count
                pool_attrs[pool_id] = set()
                pool_total[pool_id] = 0
            if pool_attr_count[pool_id] != row.attr_count:
                raise ConfigTableError(
                    f"equipment_affix pool {pool_id} has inconsistent attr_count "
                    f"{pool_attr_count[pool_id]}/{row.attr_count}"
                )
            if row.attr_id in pool_attrs[pool_id]:
                raise ConfigTableError(
                    f"equipment_affix pool {pool_id} duplicates attr_id {row.attr_id}"
                )
            pool_attrs[pool_id].add(row.attr_id)
            pool_total[pool_id] += row.weight
            if pool_total[pool_id] > MAX_POOL_TOTAL_WEIGHT:
                raise ConfigTableError(
                    f"equipment_affix pool {pool_id} total weight exceeds {MAX_POOL_TOTAL_WEIGHT}"
                )

    for pool_id, attr_count in pool_attr_count.items():
        unique = len(pool_attrs[pool_id])
        if attr_count == 0 or attr_count > unique:
            raise ConfigTableError(
                f"equipment_affix pool {pool_id} attr_count {attr_count} exceeds unique "
                f"candidates {unique}"
            )

    referenced: set[int] = set()
    for item in t.items.values():
        if item.type != ITEM_TYPE_EQUIPMENT:
            continue
        pool_id = item.identify_pool_id
        if pool_id not in pool_attr_count:
            raise ConfigTableError(
                f"equipment item {item.id} references missing identify pool {pool_id}"
            )
        referenced.add(pool_id)
    for pool_id in pool_attr_count:
        if pool_id not in referenced:
            raise ConfigTableError(
                f"equipment_affix pool {pool_id} is orphaned (no item references it)"
            )


def _index_shop_rows(rows, items) -> dict[int, list[ShopEntry]]:  # noqa: ANN001
    """商店表逐行校验 + 外键 + 按 shop_id 聚合排序。

    对应 Go 的 validateShopRow(逐行业务校验)+ validateCrossTables 里的
    shop.道具ID → item 外键 + ShopEntriesOf(排序)。

    这些校验之所以必须在**加载期整批拒**而不是购买时逐次拒:一张配错的商店表
    会让整个商店页签在玩家面前半死不活(有的商品能买有的报错),而加载期拒批次
    会保留上一份正确配置继续服务(§9.15 加载成功才切换)。

    ★ 外键这一条尤其不能省:Go 侧在加载期就拒,Python 侧若放行,同一份漂移批次
      会出现"Go 副本拒载、Python 副本正常启动"的不对称 —— 而 Python 副本上的表现
      是每次购买那件道具都报 ErrInternal,查起来比"整批拒载"难得多。
    """
    grouped: dict[int, list[ShopEntry]] = {}
    for row in rows:
        if row.id == 0:
            raise ConfigTableError("shop 表主键为 0")
        if row.shop_id == 0:
            raise ConfigTableError(f"shop 主键 {row.id}: shop_id 必须 > 0")
        if row.item_config_id == 0:
            raise ConfigTableError(f"shop 表 主键 {row.id} 的 道具ID 为 0(必填外键)")
        if row.item_config_id not in items:
            raise ConfigTableError(
                f"shop 表 主键 {row.id} 的 道具ID({row.item_config_id})在表 item 中不存在"
            )
        if row.count_per_unit == 0:
            raise ConfigTableError(f"shop 主键 {row.id}: 每份数量 必须 >= 1")
        if row.unit_price == 0:
            # 0 价商品等于免费发放,却绕过了发放审计链(GrantItems 的幂等流水与额度闸)。
            # 要送东西请走活动 / 邮件。
            raise ConfigTableError(
                f"shop 主键 {row.id}: 单价 必须 > 0(免费发放请走活动/邮件,不要配 0 价商品)"
            )
        if int(row.currency_kind) not in (
            _ccy.CURRENCY_GOLD,
            _ccy.CURRENCY_DIAMOND,
            _ccy.CURRENCY_HONOR,
        ):
            # UNSPECIFIED / 未知值一律拒,**不回退成金币**:静默回退会让配错的商品
            # 按金币扣钱,是不可观测的经济事故(currency.proto)。
            raise ConfigTableError(f"shop 主键 {row.id}: 货币类型 非法: {int(row.currency_kind)}")
        grouped.setdefault(row.shop_id, []).append(
            ShopEntry(
                item_config_id=row.item_config_id,
                count_per_unit=row.count_per_unit,
                currency_kind=int(row.currency_kind),
                unit_price=row.unit_price,
                sort_order=row.sort_order,
            )
        )
    for shop_id, entries in grouped.items():
        entries.sort(key=lambda e: (e.sort_order, e.item_config_id))
    return grouped


def _validate_item_heal(item: _item_pb2.ItemRow) -> None:
    """与 Go validateItemRow 相同的回血类型/数值组合门禁。"""
    if item.use_heal_type in (ITEM_HEAL_TYPE_UNSPECIFIED, ITEM_HEAL_TYPE_FIXED):
        if item.use_heal_max_hp_percent != 0:
            raise ConfigTableError(f"item {item.id} fixed heal requires use_heal_max_hp_percent=0")
        if item.use_heal_hp > MAX_CLIENT_FIXED_HEAL_HP:
            raise ConfigTableError(f"item {item.id} use_heal_hp exceeds client int32 max")
        if item.usable and item.use_heal_hp == 0:
            raise ConfigTableError(f"item {item.id} usable fixed heal requires use_heal_hp>0")
        if not item.usable and item.use_heal_hp != 0:
            raise ConfigTableError(f"item {item.id} non-usable item cannot configure fixed heal")
        return

    if item.use_heal_type == ITEM_HEAL_TYPE_MAX_HP_PERCENT:
        if not item.usable:
            raise ConfigTableError(f"item {item.id} percent heal requires usable=true")
        if item.use_heal_hp != 0:
            raise ConfigTableError(f"item {item.id} percent heal requires use_heal_hp=0")
        if not 1 <= item.use_heal_max_hp_percent <= 100:
            raise ConfigTableError(f"item {item.id} use_heal_max_hp_percent must be within 1..100")
        return

    raise ConfigTableError(f"item {item.id} has unknown use_heal_type={item.use_heal_type}")


# ── biz 侧只读视图 ────────────────────────────────────────────────────────


class Store:
    """当前生效批次的持有者。热更时整份替换 `_tables`(单次赋值,读侧无锁)。

    与 Go 侧 configtable.Store 同一语义:每次查询都读**当前**批次,
    所以热更后的下一次 Use/Sell/Grant/Identify 立即使用新规则;
    已经鉴定并落库的实例不会被重 roll。
    """

    __slots__ = ("_tables", "_dir", "reload_mutex")

    def __init__(self, tables: Tables, active_dir: str) -> None:
        self._tables = tables
        self._dir = active_dir
        # 热更互斥(见 pandorapy/configtable.ReloadMutex 的注释:
        # 少了它,并发 reload 会用**过期的** current_version 做单调判断,
        # 于是版本静默回退而两次热更都报成功)。
        self.reload_mutex = ReloadMutex()

    @property
    def tables(self) -> Tables:
        return self._tables

    @property
    def active_dir(self) -> str:
        return self._dir

    def replace(self, tables: Tables) -> None:
        """原子换批(单次属性赋值 —— 读侧要么看到旧的一整批,要么看到新的一整批)。"""
        self._tables = tables

    def lookup(self, item_config_id: int) -> ItemDefinition | None:
        """道具规则投影。未知 ID → None,由 biz fail-closed(未知 ID 不能入账)。"""
        row = self._tables.items.get(item_config_id)
        if row is None:
            return None
        return ItemDefinition(
            equipment=row.type == ITEM_TYPE_EQUIPMENT,
            # 见 ItemDefinition.lobby_usable 的注释:恒 False,不是漏写。
            lobby_usable=False,
            battle_usable=row.usable,
            sell_unit_price=row.sell_price,
            max_stack=row.max_stack_size,
        )

    def identify_rule(self, item_config_id: int) -> IdentifyDefinition | None:
        """装备的鉴定规则。非装备 / 无池 / 池空 → None(调用方 fail-closed)。"""
        item = self._tables.items.get(item_config_id)
        if item is None or item.type != ITEM_TYPE_EQUIPMENT or item.identify_pool_id == 0:
            return None
        rows = self._tables.affix_by_pool.get(item.identify_pool_id)
        if not rows:
            return None
        # attr_count 取池内第一行 —— 与 Go 一致;整批门禁已保证同池各行一致。
        return IdentifyDefinition(
            attr_count=rows[0].attr_count,
            pool=[
                IdentifyAttrDefinition(
                    attr_id=r.attr_id, weight=r.weight, min=r.min_value, max=r.max_value
                )
                for r in rows
            ],
        )

    def list_shop(self, shop_id: int) -> list[ShopEntry] | None:
        """某商店的全部在售商品(已按 sort_order → item_config_id 升序)。

        商店不存在 / 无在售商品 → None,由 biz 翻成"商店不可用"。
        """
        return self._tables.shops.get(shop_id)

    def lookup_shop_entry(self, shop_id: int, item_config_id: int) -> ShopEntry | None:
        """精确取某商店里某道具的档位(购买定价的唯一来源)。

        ★ 同一商店里同一道具配了多档(例如"单买"与"10 个装")时**返回第一档并不安全** ——
          购买请求只带 item_config_id,无法区分档位,两档价格不同就成了
          "服务端替玩家选便宜的"。因此发现重复时返回 None(fail-closed),
          由表设计避免这种配法:打包商品应当用独立的道具ID,而不是同一道具配两个单价。
        """
        found: ShopEntry | None = None
        for entry in self._tables.shops.get(shop_id) or ():
            if entry.item_config_id != item_config_id:
                continue
            if found is not None:
                return None
            found = entry
        return found

    def item_max_stacks(self) -> list[tuple[int, int]]:
        """把同源 item.max_stack_size 投影给后端驻留背包段(对应 Go 的 itemMaxStacksFromTables)。

        BagConf 是启动快照,因此热更后改变堆叠上限需滚动重启 inventory;
        但规则值本身不再手抄 YAML,启动时始终取当前发布批次。
        """
        return [(row.id, row.max_stack_size) for row in self._tables.items.values()]
