"""关卡表(configtable level)加载 —— 对应 Go 的 pkg/configtable 里 matchmaker 用到的那一小块。

matchmaker 从关卡表读的全是**开局形状**:
    category    这张图是不是"战斗类"(不是就根本不该 StartMatch)
    game_mode   这张图属于哪个撮合池(与本部署的 game_mode 交叉校验)
    team_size   一方几人
    side_count  几方
    min_team_size  直进(walk-in)的人数下限
    entry_mode  这张图允许排队 / 直进 / 两者
    ready_mode  组队面板先准备(PRE_READY)还是撮合后确认(POST_CONFIRM)

★ 加载语义逐条照抄 Go 的 Store.Load,任一失败**整批不切换**:
  manifest 缺表 / proto 名不符 / 文件缺失 / checksum 不符 / protojson 解析失败 /
  行数与 manifest 声明不符 / 逐行校验不过 / 整批校验器不过。
  manifest 未列出的 *.json 视为脏数据,**只告警不拒载**(hotreload doc §5)。

★ 复用 pandorapy.configtable 的 read_manifest / verify_checksum,不再抄一份:
  checksum 的字节口径(含尾换行的 LF 全字节 sha256)已经踩过一次事故,
  两份实现必然漂移,而漂移的表现是"Go 版拒载的批次 Python 版放行"。

★ 逐行校验只抄 matchmaker 会**用到**的那几条(team_size 上限 / min≤max)。
  不抄全部(asset_path、rating_pool、prepare_duration…)是刻意的:那些列
  matchmaker 一个都不读,在这里重抄一份校验只会制造第二处漂移源;它们由
  写这些列的服务与生成器负责。抄过来的这两条是**撮合预分配的输入**,
  不校验会让一次热更把撮合进程打爆或让整张图永远进不去(§16.5)。
"""

from __future__ import annotations

import dataclasses
import pathlib

from google.protobuf import json_format

from pandora.config.v1 import level_pb2 as _level_pb2

from pandorapy.configtable import (
    ReloadMutex,
    MANIFEST_FILE_NAME,
    ConfigTableError,
    read_manifest,
    verify_checksum,
)

# 与 Go 的 configtable.MaxLevelTeamSize 同值(conf.py 里也有一份同名常量,
# 那一份钳的是**全局 YAML 兜底值**,这一份挡的是**表里的值**;两个入口都要挡,
# 不能只挡一个 —— 撮合的 need 可能来自其中任意一个)。
MAX_LEVEL_TEAM_SIZE = 50

LEVEL_TABLE_NAME = "level"
LEVEL_TABLE_PROTO = "pandora.config.v1.LevelTableData"


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

    def is_battle_level(self, level_id: int) -> bool:
        """对应 Go 的 LevelTable.IsBattleLevel:存在且 category==BATTLE。

        「不存在」与「存在但不是战斗类」在这里刻意合成同一个 False:
        StartMatch 对两者的处置相同(拒),而 mapRejectReason 会分开报原因。
        """
        row = self.levels.get(level_id)
        return row is not None and row.category == _level_pb2.LEVEL_CATEGORY_BATTLE


@dataclasses.dataclass(slots=True)
class LoadResult:
    version: int
    source_rev: str
    warnings: list[str]
    tables: Tables


class Store:
    """当前生效批次的持有者。热更时整份替换 `_tables`(单次赋值,读侧无锁)。

    与 Go 侧 configtable.Store 同一语义:每次查询都读**当前**批次,所以热更后的
    下一次 StartMatch 立即使用新表;已经落库的票据保留它成票那一刻冻结的
    map_id / entry_mode(见 MatchTicketStorageRecord 的字段注释),不被热更改写。

    validators 是**批次级**校验器:启动首载与之后每次热 reload 走同一道门禁
    (审计 P1:只查启动时,坏批次热更后默认 map_id 的请求会全部失败),
    失败整批不切换、保留旧表。
    """

    __slots__ = ("_tables", "_dir", "_validators", "reload_mutex")

    def __init__(self, tables: Tables, active_dir: str) -> None:
        self._tables = tables
        self._dir = active_dir
        self._validators: list = []
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

    def add_validator(self, fn) -> None:  # noqa: ANN001 —— Callable[[Tables], None]
        """登记批次级校验器(抛异常 = 整批拒绝)。对应 Go 的 Store.AddValidator。"""
        self._validators.append(fn)

    def run_validators(self, tables: Tables) -> None:
        for fn in self._validators:
            fn(tables)

    def replace(self, tables: Tables) -> None:
        """原子换批(单次属性赋值 —— 读侧要么看到旧的一整批,要么看到新的一整批)。"""
        self._tables = tables


def _validate_level_row(row: _level_pb2.LevelRow) -> None:
    """逐行校验(只抄 matchmaker 真正消费的那几条,理由见模块 docstring)。"""
    if row.team_size > MAX_LEVEL_TEAM_SIZE:
        raise ConfigTableError(
            f"level {row.id}: 队伍人数(team_size={row.team_size})超过上限 "
            f"{MAX_LEVEL_TEAM_SIZE}(防撮合预分配爆内存)"
        )
    if row.side_count > MAX_LEVEL_TEAM_SIZE:
        raise ConfigTableError(
            f"level {row.id}: 对局方数(side_count={row.side_count})超过上限 "
            f"{MAX_LEVEL_TEAM_SIZE}(方数 × 每方人数 是撮合预分配的输入)"
        )
    if row.min_team_size > 0:
        # ① 必须同时填上限:下限相对上限才有意义;上限留 0 表示"沿用服务端全局
        #    team_size",而全局值逐部署不同,加载期无从比对 —— 放过去等于让一张表
        #    在不同部署里下限时而合法时而大于上限(那会让该图任何人数都进不去,
        #    是个静默拒服务)。
        if row.team_size == 0:
            raise ConfigTableError(
                f"level {row.id}: 队伍人数下限(min_team_size={row.min_team_size})已填,"
                f"但队伍人数(team_size)留空沿用全局兜底;两者必须同时填"
            )
        if row.min_team_size > row.team_size:
            raise ConfigTableError(
                f"level {row.id}: 队伍人数下限(min_team_size={row.min_team_size})"
                f"大于队伍人数上限(team_size={row.team_size})"
            )


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
    # 判据是 `len(rows) != mt.rows`,**不是** `if mt.rows and ...`:后者会让
    # manifest 声明 rows=0 的表整条跳过行数校验 —— 而这道校验防的正是"发布拷贝
    # 被截断":截成 0 行的表配上声明 0 行的 manifest,两边"自洽",服务照常启动,
    # 关卡表整个是空的(于是每一次 StartMatch 都报 map 不是战斗关卡)。
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
        _validate_level_row(row)
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


def default_map_validator(default_map_id: int):  # noqa: ANN201 —— Callable[[Tables], None]
    """批次级校验器:兜底默认副本(match.map_id)必须是关卡表里的战斗类关卡。

    不设这道闸会怎样:老客户端不发 map_id(0=用服务端默认副本),这些请求会一路
    透传成 DS 的 PANDORA_MAP_ID,拉起一台去加载不存在关卡的 DS ——
    失败点跑到 DS 启动里,离配置错误隔了三个服务。
    热更同样过这道门(启动首载与 reload 共用),坏批次整批不切换、保留旧表。
    """

    def _validate(tables: Tables) -> None:
        if not tables.is_battle_level(default_map_id):
            raise ConfigTableError(
                f"match.map_id {default_map_id} 不是关卡表中的战斗类关卡(g_关卡.xlsx)"
            )

    return _validate
