"""battle_result 的配置表视图 —— 对应 Go 侧 pkg/configtable 的 item / drop / role_level
三张表,以及 cmd/battle_result/main.go 里那两个 `*FromStore` 适配器。

三张表在本服的用途各不相同,缺一张的后果也各不相同:

    role_level   怪物击杀经验的**唯一数值权威**(源表 j_角色等级.xlsx 的「击杀经验」列)。
                 缺表 → 每条击杀事实按可重试错误退回,DS 原批重试到天荒地老。
                 → 所以是**启动强依赖**(main.py 的闸⑤⑥),不是"配了才用"。

    item         裁决掉落是不是装备(走实例背包)、是不是局内可消费、单格堆叠上限。
                 缺表 → 掉落无法分流,只能 fail-closed 全丢。

    drop         裁决某个 item_config_id **能不能作为战斗掉落**(DS 不可信)。
                 缺表 → 所有 DS 上报的掉落都被过滤掉,玩家"打完没掉落"。

★ 持 Store 而不是 Tables 快照(与 Go 的 `monsterExpFromStore` / `battleItemCatalogFromStore`
  一致):ReloadConfigTable 热更时 Store 原子换批次,持快照会让服务一直用启动那一版,
  策划改完数值要重启才生效 —— §9.15 热更流水线的意义就在于不重启。

★ 整批 fail-closed:任一表 checksum / 行数 / proto 名不符都抛 ConfigTableError,
  **不返回半个批次**。半个批次比加载失败危险:掉落规则用新表、经验用旧表,
  两边都"跑得过"。
"""

from __future__ import annotations

import dataclasses
import pathlib

from google.protobuf import json_format
from pandora.config.v1 import drop_pb2 as _drop_pb2
from pandora.config.v1 import item_pb2 as _item_pb2
from pandora.config.v1 import role_level_pb2 as _role_level_pb2

from pandorapy.configtable import MANIFEST_FILE_NAME, ConfigTableError, read_manifest, verify_checksum

# 主键里"角色 ID"与"等级"的进制:主键 = 角色ID×1000 + 等级。
# 口径来自源表 j_角色等级.xlsx 的 ID 公式列(B×1000+D),Go 侧 RoleLevelKeyLevelSpan 同值。
# 改这个数等于改主键口径,两栈必须同时改 —— 不同则同一 (角色,等级) 在两边查到不同的行。
ROLE_LEVEL_KEY_LEVEL_SPAN = 1000

_TABLE_PROTOS: dict[str, tuple[str, type]] = {
    "item": ("pandora.config.v1.ItemTableData", _item_pb2.ItemTableData),
    "drop": ("pandora.config.v1.DropTableData", _drop_pb2.DropTableData),
    "role_level": ("pandora.config.v1.RoleLevelTableData", _role_level_pb2.RoleLevelTableData),
}


@dataclasses.dataclass(frozen=True, slots=True)
class BattleItemDefinition:
    """battle_result 对同源 item/drop 表需要的最小投影。对应 Go 的 biz.BattleItemDefinition。"""

    equipment: bool
    battle_usable: bool
    droppable: bool
    max_stack: int


@dataclasses.dataclass(slots=True)
class Tables:
    """一个配置表批次里本服需要的三张表。"""

    version: int
    source_rev: str
    # item_config_id → ItemRow
    items: dict[int, object]
    # 可作为战斗掉落的 item_config_id 集合(drop 表里出现过即可掉落)。
    #
    # ★ Go 每次 Lookup 都线性扫一遍 drop 表(main.go 的 battleItemCatalogFromStore)。
    #   这里预先聚成集合:**结论完全相同**(存在性判定),只是把 O(n) 扫描换成 O(1)。
    #   预聚合的前提是它随批次一起换 —— Tables 是不可变的,热更换的是整个 Tables。
    droppable_ids: set[int]
    # role_level 主键(role_id*1000+level) → RoleLevelRow
    role_levels: dict[int, object]

    def item_count(self) -> int:
        return len(self.items)

    def drop_count(self) -> int:
        return self._drop_rows

    def role_level_count(self) -> int:
        return len(self.role_levels)

    # drop 表原始行数(日志里要打的是**行数**而不是去重后的可掉落 ID 数,
    # 与 Go 的 tb.Drop.Count() 对齐 —— 两个数不一样时按错的那个对不上导表产物)。
    _drop_rows: int = 0


@dataclasses.dataclass(slots=True)
class LoadResult:
    version: int
    source_rev: str
    warnings: list[str]
    tables: Tables


def _load_one(active: pathlib.Path, manifest, name: str):  # noqa: ANN001, ANN202
    """加载并核验一张表(manifest 声明 / proto 名 / checksum / 行数 四道)。"""
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
    # ★ 判据是 `len(rows) != mt.rows`,**不是** `if mt.rows and ...`:
    # 后者会让声明 rows=0 的表整条跳过行数校验 —— 而这道校验防的正是"发布拷贝被截断":
    # 截成 0 行的表配上声明 0 行的 manifest,两边"自洽",服务照常启动,表整个是空的。
    if len(rows) != mt.rows:
        raise ConfigTableError(f"{name} 表行数不符: manifest 声明 {mt.rows} 实际 {len(rows)}")
    return rows


def role_level_key(role_id: int, level: int) -> int:
    """按源表公式口径组合主键。对应 Go 的 roleLevelKey。"""
    return role_id * ROLE_LEVEL_KEY_LEVEL_SPAN + level


def validate_role_level_row(row) -> None:  # noqa: ANN001
    """逐行业务校验 —— 对应 Go 的 validateRoleLevelRow。

    与生成阶段校验重复是**有意的 fail-closed**:服务端不信任产物一定出自本生成器。

    不设这些闸会怎样:
      ① 等级 ≥1000 会进位污染角色段 → "查某角色某级查到别人的经验",静默错值;
      ② 主键与 (角色ID, 等级) 不自洽 = 源表 ID 公式列没重算(xlsxlite 读的是缓存值),
         同样表现为查到别人的行。
    """
    if row.role_id == 0:
        raise ConfigTableError("role_level 行:角色ID(role_id)为 0")
    if row.level == 0:
        raise ConfigTableError("role_level 行:角色等级(level)为 0")
    if row.level >= ROLE_LEVEL_KEY_LEVEL_SPAN:
        raise ConfigTableError(
            f"role_level 行:角色等级 {row.level} 超出主键进制 "
            f"{ROLE_LEVEL_KEY_LEVEL_SPAN}(会进位污染角色段)"
        )
    want = role_level_key(row.role_id, row.level)
    if row.id != want:
        raise ConfigTableError(
            f"role_level 行:主键 {row.id} 与 (角色ID={row.role_id}, 等级={row.level}) "
            f"不自洽(应为 {want};源表 ID 是公式列,改行后须让 Excel 重算并保存)"
        )


def load_tables(active_dir: str | pathlib.Path, expect_version: int = 0) -> LoadResult:
    """加载 battle_result 需要的三张表(item / drop / role_level)。整批 fail-closed。"""
    active = pathlib.Path(active_dir)
    if not active.is_dir():
        raise ConfigTableError(f"配置表目录不存在: {active}")

    manifest = read_manifest(active)
    if expect_version and manifest.version != expect_version:
        raise ConfigTableError(f"manifest 版本不符: 期望 {expect_version} 实际 {manifest.version}")

    item_rows = _load_one(active, manifest, "item")
    drop_rows = _load_one(active, manifest, "drop")
    role_level_rows = _load_one(active, manifest, "role_level")

    items: dict[int, object] = {}
    for row in item_rows:
        if row.id in items:
            raise ConfigTableError(f"item 表 id 重复: {row.id}")
        items[row.id] = row

    droppable: set[int] = set()
    for row in drop_rows:
        # item_config_id 为 0 的行不构成"可掉落"事实,跳过而不拒批 ——
        # 与 Go 的线性扫描一致(它比的是 `row.GetItemConfigId() == itemConfigID`,
        # 而 buildDropOutbox 一侧已先排除了 id==0)。
        if not row.item_config_id:
            continue
        # ★ 跨表引用完整性(drop.item_config_id → item),对应 proto 的 `excel_fk = "item"`
        #   注解与 Go 生成的 validateCrossTables(tables.gen.go)。文案与 Go **逐字相同**:
        #   两栈同一条 Loki 告警要能同时命中。
        #
        #   少了这道闸会怎样:坏批次照常启动,故障从"服务起不来"退化成"这个怪打完永远
        #   不掉那件东西" —— 运行期 lookup() 在 items 里查不到,fail-closed 把整条掉落
        #   丢掉(方向对),而 battle_drop_all_filtered 那条 WARN 只在某玩家本场掉落被
        #   **整条过滤光**时才打:一次掉 3 件而只坏 1 件时零日志静默消失。配表是与 UE
        #   同源的策划数值,发布链任一环写坏都该在启动期炸,而不是等玩家报"掉落没了"。
        #
        #   为什么只补这一条 FK:Go 的 Store 是全二进制注册表(31 张表全在手),所以能查
        #   全部 13 对生成 FK;本服只加载 item / drop / role_level 三张,能查的只有
        #   drop→item 这一对。补更多不是"更严",是拿查不到的表当不存在,会把好批次拒掉。
        if row.item_config_id not in items:
            raise ConfigTableError(
                f"表 drop 主键 {row.id} 的 物品ID({row.item_config_id})在表 item 中不存在"
            )
        droppable.add(row.item_config_id)

    role_levels: dict[int, object] = {}
    for row in role_level_rows:
        validate_role_level_row(row)
        if row.id in role_levels:
            raise ConfigTableError(f"role_level 表 id 重复: {row.id}")
        role_levels[row.id] = row

    tables = Tables(
        version=manifest.version,
        source_rev=manifest.source_rev,
        items=items,
        droppable_ids=droppable,
        role_levels=role_levels,
        _drop_rows=len(drop_rows),
    )

    listed = {MANIFEST_FILE_NAME} | {t.file for t in manifest.tables.values()}
    warnings = [
        f"active 目录存在 manifest 未列出的文件 {p.name!r}(脏数据)"
        for p in sorted(active.glob("*.json"))
        if p.name not in listed
    ]
    return LoadResult(
        version=manifest.version, source_rev=manifest.source_rev, warnings=warnings, tables=tables
    )


class Store:
    """热更批次的原子持有者。对应 Go 的 configtable.Store(读路径无锁)。

    ★ 注入给 usecase 的是**本对象**而不是 `tables`:热更 replace 之后读路径立即生效。
      注入快照的话策划改数值要重启才生效(§9.15)。
    """

    __slots__ = ("_tables", "_active_dir")

    def __init__(self, tables: Tables, active_dir: str) -> None:
        self._tables = tables
        self._active_dir = active_dir

    @property
    def tables(self) -> Tables:
        return self._tables

    @property
    def active_dir(self) -> str:
        return self._active_dir

    def replace(self, tables: Tables) -> None:
        """整批换指针(热更)。Python 的属性赋值本身是原子的,读侧不会看到半个批次。"""
        self._tables = tables

    # ── biz.MonsterExpTable ───────────────────────────────────────────────

    def kill_exp_of(self, role_id: int, level: int) -> tuple[int, bool]:
        """查击杀某角色某等级形态的整份经验。对应 Go 的 RoleLevelTable.KillExpOf。

        `level == 0` 按 1 级处理:旧批次刷怪点表没有「怪物等级」列,导表后取到 0,
        此时必须与"等级硬编码 1"的历史行为一致 —— 否则同一只怪在换表前后经验不同。

        返回 ok=False 表示**该 (角色, 等级) 在表里没有行**。调用方必须把它与
        "有行但经验为 0"区分开:前者是配置漏项(该告警),后者是策划有意不给经验。
        """
        if role_id == 0:
            return 0, False
        if level == 0:
            level = 1
        if level >= ROLE_LEVEL_KEY_LEVEL_SPAN:
            return 0, False
        row = self._tables.role_levels.get(role_level_key(role_id, level))
        if row is None:
            return 0, False
        return int(row.kill_exp), True

    # ── biz.BattleItemCatalog ─────────────────────────────────────────────

    def lookup(self, item_config_id: int) -> BattleItemDefinition | None:
        """查一个道具在战斗域的最小投影。对应 Go 的 battleItemCatalogFromStore.Lookup。

        **未知 ID 一律 fail-closed 返回 None**(Go 返回 ok=false):
        放行未知 ID 等于让 DS 决定发放什么 —— 那正是 §9.6 要防的。
        """
        tb = self._tables
        row = tb.items.get(item_config_id)
        if row is None:
            return None
        return BattleItemDefinition(
            equipment=row.type == _item_pb2.ITEM_TYPE_EQUIPMENT,
            battle_usable=bool(row.usable),
            droppable=item_config_id in tb.droppable_ids,
            max_stack=int(row.max_stack_size),
        )
