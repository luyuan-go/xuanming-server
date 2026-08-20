"""ds_allocator 的配置表视图 —— 对应 Go 侧 `pkg/configtable` 里本服用到的那一小块
(`level.go` 的 `LevelPackagePath` / `BattleLaunchURL` / `ValidateBattleLaunchURLs`,
以及 `cmd/ds_allocator/main.go` 里那段 `ctStore` 接线)。

ds_allocator **只读关卡表的三列**,用途只有一个:`mode=local` 拉起本机 DS 时按 map_id
现查 `g_关卡.xlsx` 拼命令行地图参数(取代 2026-08-04 之前那张手抄的 `local_ds.maps`
影子表)。

    asset_path        UE 关卡资产路径 → 归一成长包名
    game_mode_class   UE GameMode 类路径(空 = 沿用关卡自带的 GameMode)
    category          只有战斗 / 副本类关卡能开局

★ 为什么只抄这三列的校验:本服一个都不读 team_size / rating_pool / entry_mode,
  在这里重抄一遍它们的校验只会制造第二处漂移源(与 matchmaker/catalog.py 的取舍同因)。
  抄过来的是 `validate_battle_launch_urls` —— 那是本服**真正消费**的那条不变量。

★ 为什么不复用 `services/matchmaker/catalog.py`:那份的逐行校验是撮合的输入
  (team_size / min_team_size 上下限),与本服消费的列不是一回事;跨服务 import
  会让 matchmaker 的一次校验口径调整静默改变 ds_allocator 的加载门禁。
  两份共享的是**加载引擎**(`pandorapy.configtable` 的 read_manifest / verify_checksum),
  不是彼此。

★ 持 Store 而不是 Tables 快照(与 Go 的 `SetMapURLResolver` 闭包一致):
  `ReloadConfigTable` 热更时 Store 原子换批次,持快照会让本服一直用启动那一版 ——
  §9.15 热更流水线的意义正在于"策划新增副本无需重启 ds_allocator"。

★ 整批 fail-closed:manifest 缺表 / proto 名不符 / 文件缺失 / checksum 不符 /
  protojson 解析失败 / 行数与 manifest 声明不符 / 批次校验器不过 —— 任一条都抛
  `ConfigTableError`,**不返回半个批次**。半批比加载失败危险得多:一半新图一半旧图,
  分配出去的 DS 会去加载一张已经改名的关卡,失败点跑到 DS 启动里。
"""

from __future__ import annotations

import dataclasses
import pathlib

from google.protobuf import json_format
from pandora.config.v1 import level_pb2 as _level_pb2

from pandorapy.configtable import (
    MANIFEST_FILE_NAME,
    ConfigTableError,
    ReloadMutex,
    read_manifest,
    verify_checksum,
)

LEVEL_TABLE_NAME = "level"
LEVEL_TABLE_PROTO = "pandora.config.v1.LevelTableData"


def level_package_path(asset_path: str) -> str:
    """把关卡表「关卡资源」列归一成 UE **长包名**。对应 Go 的 `LevelPackagePath`。

    表里的值可能是 ObjectPath(`/Game/A/B.B` —— 导表从 SoftObjectPath 落下来就是
    这形状),而 UE 的 ServerTravel / DS 命令行地图参数只吃包路径,带 `.对象名` 会
    解析失败。规则与 UE 侧 `APandoraDSLoaderGameMode::BuildTravelURL` 逐字一致:
    剥掉**最后一个斜杠之后**的那个点及其后缀。

    ★ 判据是 `dot > last_slash` 而不是 `"." in p`:后者会误伤 `/Game/A.B/C` 这类
      目录名带点的路径(剥成 `/Game/A`,指向一张不存在的关卡)。
    """
    p = asset_path.strip()
    dot = p.rfind(".")
    if dot > p.rfind("/"):
        p = p[:dot]
    return p


@dataclasses.dataclass(slots=True)
class Tables:
    """一个已校验的原子批次。热更时整份替换,读侧永远看到自洽的一批。"""

    version: int
    source_rev: str
    levels: dict[int, _level_pb2.LevelRow]

    def level_count(self) -> int:
        return len(self.levels)

    def by_id(self, level_id: int) -> _level_pb2.LevelRow | None:
        return self.levels.get(level_id)

    def battle_launch_url(self, level_id: int) -> str:
        """按 map_id 返回战斗 DS 要加载的关卡 URL(`<长包名>[?game=<GameMode 类>]`)。

        对应 Go 的 `LevelTable.BattleLaunchURL`。这是"关卡表是唯一权威源"在服务端的
        落点:与 UE 侧 Loader GameMode 同规则、同数据源(g_关卡.xlsx),两端不会各拼一套。

        **失败一律抛 ConfigTableError,绝不给兜底关卡**:调用方(local_allocator)必须让
        本次分配整体失败。回退默认图会让 DS 起错图,随后被 DS 侧关卡门判 Mismatch 自杀,
        表现成"玩家一直排队中" —— 2026-08-04 map_id=11 事故的形状。

        `game_mode_class` 为空 = 沿用关卡自带的 GameMode(不拼 `?game=`),与 UE 侧同义;
        不在这里塞猜的默认 GameMode(要改就改表)。
        """
        row = self.levels.get(level_id)
        if row is None:
            raise ConfigTableError(f"关卡表(g_关卡.xlsx)没有 map_id={level_id} 的行")
        if row.category != _level_pb2.LEVEL_CATEGORY_BATTLE:
            raise ConfigTableError(
                f"map_id={level_id}({row.name})关卡类别={row.category},"
                f"不是战斗 / 副本类关卡,不能开局"
            )
        pkg = level_package_path(row.asset_path)
        if pkg == "":
            raise ConfigTableError(f"map_id={level_id}({row.name})关卡资源(asset_path)为空")
        game_mode = row.game_mode_class.strip()
        if game_mode != "":
            return pkg + "?game=" + game_mode
        return pkg


@dataclasses.dataclass(slots=True)
class LoadResult:
    version: int
    source_rev: str
    warnings: list[str]
    tables: Tables


class Store:
    """当前生效批次的持有者。热更时整份替换 `_tables`(单次赋值,读侧无锁)。

    与 Go 侧 `configtable.Store` 同一语义:每次查询都读**当前**批次,所以热更后的
    下一次 `allocate` 立即用新表 —— 这正是 `SetMapURLResolver` 注入闭包("现查"),
    而不是启动时快照的理由。

    validators 是**批次级**校验器:启动首载与之后每次热 reload 走同一道门禁,
    失败整批不切换、保留旧表(§9.15)。
    """

    __slots__ = ("_dir", "_tables", "_validators", "reload_mutex")

    def __init__(self, tables: Tables, active_dir: str) -> None:
        self._tables = tables
        self._dir = active_dir
        self._validators: list = []
        # 热更互斥(见 pandorapy/configtable.ReloadMutex 的注释:少了它,并发 reload
        # 会用**过期的** current_version 做单调判断,于是版本静默回退而两次都报成功)。
        self.reload_mutex = ReloadMutex()

    @property
    def tables(self) -> Tables:
        return self._tables

    @property
    def active_dir(self) -> str:
        return self._dir

    def add_validator(self, fn) -> None:  # noqa: ANN001 —— Callable[[Tables], None]
        """登记批次级校验器(抛异常 = 整批拒绝)。对应 Go 的 `Store.AddValidator`。"""
        self._validators.append(fn)

    def run_validators(self, tables: Tables) -> None:
        for fn in self._validators:
            fn(tables)

    def replace(self, tables: Tables) -> None:
        """原子换批(单次属性赋值 —— 读侧要么看到旧的一整批,要么看到新的一整批)。"""
        self._tables = tables

    def battle_launch_url(self, map_id: int) -> str:
        """`map_id → 关卡 URL` 解析器,直接喂给 `LocalGameServerAllocator.set_map_url_resolver`。

        ★ 每次调用现查 `self._tables`(而不是在注入时把 Tables 闭进去):
          热更后新增的副本无需重启本服务即可开局。
        """
        return self._tables.battle_launch_url(map_id)


def validate_battle_launch_urls(tables: Tables) -> None:
    """批次级校验器:表里**每一张**战斗类关卡都必须能构造出合法启动 URL。

    对应 Go 的 `LevelTable.ValidateBattleLaunchURLs`(main.go 里 `ctStore.AddValidator`
    登记的那一个)。启动首载与之后每次热 reload 走同一门禁,坏批次整批不切换、保留旧表 ——
    把"某张图资源列填错"挡在加载边界,而不是等玩家恰好选中那张图才炸。
    """
    for row in tables.levels.values():
        if row.category != _level_pb2.LEVEL_CATEGORY_BATTLE:
            continue
        tables.battle_launch_url(row.id)


def load_tables(active_dir: str | pathlib.Path, expect_version: int = 0) -> LoadResult:
    """加载关卡表。整批 fail-closed:任一条不过都抛 ConfigTableError,不返回半个批次。"""
    active = pathlib.Path(active_dir)
    if not active.is_dir():
        raise ConfigTableError(f"配置表目录不存在: {active}")

    manifest = read_manifest(active)
    if expect_version and manifest.version != expect_version:
        raise ConfigTableError(
            f"manifest 版本不符: 期望 {expect_version} 实际 {manifest.version}"
        )

    mt = manifest.tables.get(LEVEL_TABLE_NAME)
    if mt is None:
        raise ConfigTableError(
            f"manifest 缺少本进程必需的表 {LEVEL_TABLE_NAME!r},整批拒绝"
        )
    if mt.proto != LEVEL_TABLE_PROTO:
        raise ConfigTableError(
            f"{LEVEL_TABLE_NAME} 表 proto 不符: 期望 {LEVEL_TABLE_PROTO} 实际 {mt.proto}(接错文件?)"
        )
    path = active / mt.file
    if not path.is_file():
        raise ConfigTableError(f"manifest 列出的表文件不存在: {path}")
    raw = path.read_bytes()
    verify_checksum(raw, mt.checksum)

    container = _level_pb2.LevelTableData()
    try:
        # ignore_unknown_fields=True —— 与 Go 侧 protojson + DiscardUnknown 一致,
        # **不是**放松校验。严格校验属于生成阶段;运行期必须容忍新增列,否则
        # 标准发布序(先发配置、再滚二进制)会让尚未滚上的旧进程整批拒载。
        json_format.Parse(raw.decode("utf-8"), container, ignore_unknown_fields=True)
    except json_format.ParseError as exc:
        raise ConfigTableError(f"{path} protojson 解析失败: {exc}") from exc

    rows = list(container.rows)
    # 判据是 `len(rows) != mt.rows`,**不是** `if mt.rows and ...`:后者会让 manifest
    # 声明 rows=0 的表整条跳过行数校验 —— 而这道校验防的正是"发布拷贝被截断":
    # 截成 0 行的表配上声明 0 行的 manifest,两边"自洽",服务照常启动,关卡表整个是空的
    # (于是每一次 allocate 都报"没有 map_id=N 的行")。
    if len(rows) != mt.rows:
        raise ConfigTableError(
            f"{LEVEL_TABLE_NAME} 表行数不符: manifest 声明 {mt.rows} 实际 {len(rows)}"
        )

    levels: dict[int, _level_pb2.LevelRow] = {}
    for row in rows:
        if row.id == 0:
            raise ConfigTableError("level 表存在 id=0 的行(主键不可为 0)")
        if row.id in levels:
            raise ConfigTableError(f"level 表 id 重复: {row.id}")
        levels[row.id] = row

    tables = Tables(version=manifest.version, source_rev=manifest.source_rev, levels=levels)

    listed = {MANIFEST_FILE_NAME} | {t.file for t in manifest.tables.values()}
    warnings = [
        f"active 目录存在 manifest 未列出的文件 {p.name!r}(脏数据)"
        for p in sorted(active.glob("*.json"))
        if p.name not in listed
    ]
    return LoadResult(
        version=manifest.version,
        source_rev=manifest.source_rev,
        warnings=warnings,
        tables=tables,
    )


__all__ = [
    "LEVEL_TABLE_NAME",
    "LEVEL_TABLE_PROTO",
    "LoadResult",
    "Store",
    "Tables",
    "level_package_path",
    "load_tables",
    "validate_battle_launch_urls",
]
