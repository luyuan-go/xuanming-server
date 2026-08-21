"""确定性玩家路由 —— 对应 Go 侧 pkg/cellroute。

两步映射(scale-cellular-20m.md §4.2):

    logical_cell = player_id % LogicalCellCount        # 第 1 步:确定性,纯计算
    (region, cell) = table.lookup(logical_cell)        # 第 2 步:查小映射表

为什么两步而不是 `player_id % 物理Cell数`:
    逻辑分片数 4096 远大于物理 Cell 数(~16~24)。扩缩容时只需把**部分逻辑区间**
    迁到新 Cell,而不是把所有玩家重新取模(那会让几乎每个玩家换 Cell,违反 owner 不变量)。

owner 不变量(§9.22):
    同一 player_id 的所有 owner 数据(档案 / 背包 / 段位 / 好友 / 对话会话)必落同一
    region 同一 cell。这靠"每个物理 Cell 自带其所属 Region"保证 —— 建表时就校验
    entry 的 RegionID 与该 Cell 登记的 region 一致,从源头杜绝 region/cell 错配。
    错配的后果:玩家的背包在 A 区、档案在 B 区,读不回来。

⚠️ LogicalCellCount 改动 = 全体玩家重新分片 = 所有 owner 数据错位。这个数是永久契约,
   与 Go 侧必须**同值**(4096)。迁移期两个实现同时在线,不同值会让同一玩家被路由到
   两个 cell —— 而且不报错。
"""

from __future__ import annotations

import dataclasses
import re
from typing import Protocol

from pydantic import BaseModel, Field

# 决策 2026-06-26:采纳 4096。改这个数等于全体玩家重新分片,是永久契约。
LOGICAL_CELL_COUNT = 4096

# etcd value 的严格格式。用 \A…\Z 而不是 ^…$:后者在 Python 里能匹配到换行前,
# "12:34\n5" 会被 ^…$ 放行。
_ENTRY_VALUE = re.compile(r"\A(\d+):(\d+)\Z")

_UINT32_MAX = 0xFFFFFFFF


class CellRouteError(RuntimeError):
    """路由失败。调用方必须 fail-closed(§9.22:查询失败返回 UNKNOWN,不得冒充默认值)。"""


@dataclasses.dataclass(frozen=True, slots=True)
class Entry:
    """一个逻辑分片的物理落点。"""

    region_id: int
    cell_id: int


@dataclasses.dataclass(frozen=True, slots=True)
class Location:
    """一次路由结果。logical_cell 保留用于调试 / 迁移灰度判定。"""

    region_id: int
    cell_id: int
    logical_cell: int


@dataclasses.dataclass(frozen=True, slots=True)
class FullLocation:
    """三层全路径定位。对应 Go 的 `cellroute.FullLocation`。

    shards_per_cell 回带是刻意的:调用方能自查"这个下标是按几库算出来的"。
    不回带的话,分库数改了之后旧日志里的 shard 下标无法解读。
    """

    region_id: int
    cell_id: int
    logical_cell: int
    in_cell_shard: int
    shards_per_cell: int


class Table(Protocol):
    """映射表的读接口 —— `StaticTable` 与 `AtomicTable` 都满足。

    有这个 Protocol 才能让 `Router` 同时接住不可变表和热更表,
    而不必在 Router 里判类型。
    """

    def lookup(self, logical_cell: int) -> Entry | None: ...

    def __len__(self) -> int: ...


def logical_cell_of(player_id: int) -> int:
    """player_id → 逻辑分片号。纯函数,确定性。对应 Go 的 LogicalCellOf。"""
    return player_id % LOGICAL_CELL_COUNT


def in_cell_shard(player_id: int, shards_per_cell: int) -> int:
    """owner Cell 内的 MySQL 分库下标。对应 Go 的 `InCellShard`。

    `shards_per_cell` 必须 ≥ 1:不强制的话零值会除零,而"默认当成 1"会让一个
    忘配分库数的部署静默把所有玩家算到 0 号库。单库必须显式传 1。
    """
    if shards_per_cell < 1:
        raise CellRouteError(f"cellroute: shardsPerCell {shards_per_cell} < 1")
    return player_id % shards_per_cell


def cell_tag(region_id: int, cell_id: int) -> str:
    """Cell 作用域资源标签 "r<region>c<cell>"。对应 Go 的 `CellTag`。

    用途:Redis key 前缀、Kafka consumer group 后缀、metrics 低基数维度。
    ⚠️ 只含 region/cell 两个**低基数**拓扑维度;不要把 player_id 拼进来
    (CLAUDE.md §12 禁止高基数 prometheus label)。
    """
    return f"r{region_id}c{cell_id}"


class StaticTable:
    """不可变内存映射表:下标即 logical_cell,长度固定 LOGICAL_CELL_COUNT。

    对应 Go 的 cellroute.StaticTable。构造后纯读,并发安全。
    """

    __slots__ = ("_entries",)

    def __init__(self, entries: list[Entry], region_of_cell: dict[int, int]) -> None:
        if len(entries) != LOGICAL_CELL_COUNT:
            raise CellRouteError(
                f"cellroute: entries 长度 {len(entries)} != LogicalCellCount {LOGICAL_CELL_COUNT}"
            )
        # ★ 建表期校验 region/cell 自洽 —— 这是 owner 不变量的源头闸。
        # 放过一个错配,后果是那批玩家的 owner 数据分散在两个 region,读不回来,
        # 而且要等到玩家实际访问才发现。
        for logical_cell, entry in enumerate(entries):
            declared = region_of_cell.get(entry.cell_id)
            if declared is None:
                raise CellRouteError(
                    f"cellroute: logical_cell {logical_cell} -> cell {entry.cell_id} "
                    f"未在 region_of_cell 中登记"
                )
            if declared != entry.region_id:
                raise CellRouteError(
                    f"cellroute: logical_cell {logical_cell} -> cell {entry.cell_id} "
                    f"region 不匹配: entry={entry.region_id} topology={declared}"
                )
        self._entries = tuple(entries)

    def lookup(self, logical_cell: int) -> Entry | None:
        """越界或未配置返回 None,调用方 fail-closed。"""
        if not 0 <= logical_cell < len(self._entries):
            return None
        return self._entries[logical_cell]

    def __len__(self) -> int:
        return len(self._entries)


class Router:
    """路由器。读路径无锁(表不可变),对应 Go 的 cellroute.Router。"""

    __slots__ = ("_table",)

    def __init__(self, table: "Table") -> None:
        self._table = table

    def route(self, player_id: int) -> Location:
        """把 player_id 路由到 (region, cell)。查不到抛异常。

        ⚠️ 抛异常而不是返回默认落点:§9.22 明确要求查询失败必须 UNKNOWN + fail-closed,
        **禁止冒充默认值**。返回一个"默认 cell"会让玩家的数据被写到错误的 region。
        """
        logical_cell = logical_cell_of(player_id)
        entry = self._table.lookup(logical_cell)
        if entry is None:
            raise CellRouteError(
                f"cellroute: logical_cell {logical_cell} 未映射 (player_id={player_id})"
            )
        return Location(
            region_id=entry.region_id, cell_id=entry.cell_id, logical_cell=logical_cell
        )

    def route_full(self, player_id: int, shards_per_cell: int) -> FullLocation:
        """三层全路径定位。对应 Go 的 `Router.RouteFull`。

        第 3 层(Cell 内 MySQL 分库下标)与 `mysqlx.ShardSet` **同口径**(都是 id % N)。
        这里再导出一份纯计算是给不持有连接池的场景用(日志 / 灰度判定 / 运维自查);
        两套口径分叉会让"日志说落 3 号库、实际写进 5 号库"。
        """
        loc = self.route(player_id)
        shard = in_cell_shard(player_id, shards_per_cell)
        return FullLocation(
            region_id=loc.region_id,
            cell_id=loc.cell_id,
            logical_cell=loc.logical_cell,
            in_cell_shard=shard,
            shards_per_cell=shards_per_cell,
        )


class AtomicTable:
    """可整体原子替换的 Table 实现。对应 Go 的 `cellroute.AtomicTable`。

    ★ 为什么是"整表替换"而不是逐 key 改:读路径必须永远看到某个**完整一致**的快照。
      原地改表意味着存在一个"改了一半"的瞬间,那一瞬间被路由的玩家会落到旧新混合的
      映射上 —— 而且不报错。整表替换让每次 `lookup` 要么全看到旧表、要么全看到新表。

    ★ Python 侧无需 `atomic.Pointer`:属性重绑定本身是原子的(单条 STORE_ATTR 字节码,
      不会被解释器在中途切走成"半个引用")。真正的并发风险在**读到旧表**,而那正是
      整表替换语义允许的 —— 与 Go 逐字同义。
    """

    __slots__ = ("_cur",)

    def __init__(self, initial: StaticTable) -> None:
        if initial is None:
            raise CellRouteError("cellroute: nil initial table")
        self._cur = initial

    def store(self, nxt: StaticTable) -> None:
        """整体替换。next 不可为 None —— 空映射用一张全量 StaticTable 表达。

        允许 None 会让"表没配好"退化成"表是空的",而空表的 lookup 全部 miss,
        Router 会对**每个**玩家抛 CellRouteError,把配置问题伪装成运行时故障。
        """
        if nxt is None:
            raise CellRouteError("cellroute: store nil table")
        self._cur = nxt

    def lookup(self, logical_cell: int) -> Entry | None:
        return self._cur.lookup(logical_cell)

    def __len__(self) -> int:
        return len(self._cur)


def encode_entry(entry: Entry) -> str:
    """Entry → etcd value 文本 "region:cell"。对应 Go 的 `EncodeEntry`。

    读写两侧共用同一个编解码对,是"铺表工具写的东西服务一定读得懂"的唯一保证。
    """
    return f"{entry.region_id}:{entry.cell_id}"


def decode_entries(raw: dict[int, str]) -> tuple[list[Entry], dict[int, int]]:
    """etcd 原始映射 → entries + regionOfCell 拓扑。对应 Go 的 `DecodeEntries`。

    raw 的 key 是 logical_cell,value 形如 "12:34"。要求**恰好覆盖**
    `[0, LOGICAL_CELL_COUNT)`:缺项直接报错,绝不静默补 0 号 Cell —— 否则一个配置
    缺口会变成"那批玩家全被路由到 0 号 Cell"的静默错落点。

    ⚠️ **与 Go 的一处已知差异(有意,方向安全)**:Go 用 `fmt.Sscanf(v, "%d:%d", …)`,
    它在匹配完格式后**不检查尾部剩余输入**,因此 `"12:34garbage"` 在 Go 上被接受成
    (12,34)。这里改用严格全匹配,`"12:34garbage"` 会被拒。差异只在**畸形配置**上显现,
    且方向是 Python 在**启动期**大声拒绝(还没路由过任何玩家),而不是运行期静默分叉。
    """
    if len(raw) != LOGICAL_CELL_COUNT:
        raise CellRouteError(
            f"cellroute: raw map has {len(raw)} keys, want LogicalCellCount {LOGICAL_CELL_COUNT}"
        )
    entries: list[Entry] = []
    region_of_cell: dict[int, int] = {}
    for logical_cell in range(LOGICAL_CELL_COUNT):
        value = raw.get(logical_cell)
        if value is None:
            raise CellRouteError(
                f"cellroute: missing logical_cell {logical_cell} in raw map"
            )
        matched = _ENTRY_VALUE.fullmatch(value)
        if matched is None:
            raise CellRouteError(
                f"cellroute: logical_cell {logical_cell} bad value {value!r} "
                f'(want "region:cell")'
            )
        region, cell = int(matched.group(1)), int(matched.group(2))
        # Go 侧这两个字段是 uint32,越界在 Sscanf 里就报错了。Python 的 int 无上界,
        # 不显式查会让一个 Go 拒载的表在 Python 上加载成功 —— 两栈对同一份 etcd
        # 内容结论不同,正是要避免的事。
        if region > _UINT32_MAX or cell > _UINT32_MAX:
            raise CellRouteError(
                f"cellroute: logical_cell {logical_cell} value {value!r} exceeds uint32"
            )
        seen = region_of_cell.get(cell)
        if seen is not None and seen != region:
            raise CellRouteError(
                f"cellroute: cell {cell} mapped to region {seen} and {region}"
            )
        region_of_cell[cell] = region
        entries.append(Entry(region_id=region, cell_id=cell))
    return entries, region_of_cell


def build_static_table_from_raw(raw: dict[int, str]) -> StaticTable:
    """`decode_entries` + `StaticTable` 的便捷组合。对应 Go 的 `BuildStaticTableFromRaw`。"""
    entries, region_of_cell = decode_entries(raw)
    return StaticTable(entries, region_of_cell)


@dataclasses.dataclass(frozen=True, slots=True)
class CellSpec:
    region_id: int
    cell_id: int


def build_balanced_entries(
    cells: list[CellSpec],
) -> tuple[list[Entry], dict[int, int]]:
    """把 LOGICAL_CELL_COUNT 个逻辑分片**连续**切给物理 Cell 列表。

    对应 Go 的 BuildBalancedEntries。用于初始部署 / 测试铺表;
    真实扩缩容改用"迁移部分区间"的灰度流程,**不重铺全表**
    (重铺会让几乎每个玩家换 cell)。

    连续区间分配(非 round-robin)正是为了扩容时能按区间迁移。
    """
    if not cells:
        raise CellRouteError("cellroute: cells 不能为空")

    region_of_cell: dict[int, int] = {}
    for c in cells:
        existing = region_of_cell.get(c.cell_id)
        if existing is not None and existing != c.region_id:
            raise CellRouteError(
                f"cellroute: cell {c.cell_id} 被声明在两个 region {existing} 和 {c.region_id}"
            )
        region_of_cell[c.cell_id] = c.region_id

    entries: list[Entry] = []
    base, remainder = divmod(LOGICAL_CELL_COUNT, len(cells))
    for idx, c in enumerate(cells):
        # 余数摊到前几个 Cell(与 Go 一致)
        count = base + (1 if idx < remainder else 0)
        entries.extend(Entry(region_id=c.region_id, cell_id=c.cell_id) for _ in range(count))
    if len(entries) != LOGICAL_CELL_COUNT:
        raise CellRouteError(
            f"cellroute: 内部错误,铺出 {len(entries)} 项 != {LOGICAL_CELL_COUNT}"
        )
    return entries, region_of_cell


# ── 装配层(对应 Go 的 pkg/cellroute/config.go)────────────────────────────────

# 模式常量。值必须与 Go 逐字相同 —— 它们直接来自同一份 yaml。
MODE_OFF = ""  # 单 Cell:不路由,所有服务 nil-safe 回退(默认)
MODE_STATIC = "static"  # 本地配置铺表
MODE_ETCD = "etcd"  # etcd 热更新,Router 经 cellroute_etcd 装配


class CellEntry(BaseModel):
    """配置里一个物理 Cell 的归属。对应 Go 的 `cellroute.CellEntry`。"""

    region_id: int = 0
    cell_id: int = 0


class RouterConfig(BaseModel):
    """部署拓扑配置(放进 `config.BaseConf`,各服务共用)。

    对应 Go 的 `cellroute.RouterConfig`,字段名与 yaml 逐字相同。
    """

    model_config = {"extra": "allow"}

    mode: str = ""
    self_region: int = 0
    self_cell: int = 0
    cells: list[CellEntry] = Field(default_factory=list)
    etcd_endpoints: list[str] = Field(default_factory=list)
    etcd_prefix: str = ""
    # 仅 auction 用:HRW 市场归属。
    market_peers: list[str] = Field(default_factory=list)
    market_self: str = ""

    def enabled(self) -> bool:
        """是否需要构造 router(static/etcd 为真,off 为假)。"""
        return self.mode in (MODE_STATIC, MODE_ETCD)

    def validate_mode(self) -> None:
        """配置加载后的基础自检。对应 Go 的 `RouterConfig.Validate`。

        ★ 未知 mode 必须**报错而不是当成 off**:把 `mode: statik`(拼错)吞成单 Cell,
          等于运维以为分片开了、实际全落一个 Cell,而且不报错。
        """
        if self.mode == MODE_OFF:
            return
        if self.mode == MODE_STATIC:
            if not self.cells:
                raise CellRouteError("cellroute: static mode needs cells")
            return
        if self.mode == MODE_ETCD:
            if not self.etcd_endpoints:
                raise CellRouteError("cellroute: etcd mode needs etcd_endpoints")
            return
        raise CellRouteError(f"cellroute: unknown mode {self.mode!r}")

    def market_peer_list(self) -> list[str]:
        """归一化 auction market peers(去空去重、确保 self 在内)。

        对应 Go 的 `MarketPeerList`。**顺序必须与 Go 一致**:HRW 归属虽与顺序无关,
        但去重后的列表会进日志与自检对比,顺序不同会让两栈的对比结果看起来不一样。
        """
        out: list[str] = []
        seen: set[str] = set()
        for peer in self.market_peers:
            p = peer.strip()
            if not p or p in seen:
                continue
            seen.add(p)
            out.append(p)
        this = self.market_self.strip()
        if this and this not in seen:
            out.append(this)
        return out


def build_router(cfg: RouterConfig) -> Router | None:
    """按配置构造 Router。对应 Go 的 `cellroute.BuildRouter`。

    - off:返回 None(调用方注入 None = 单 Cell 行为不变)。
    - static:本地铺表。
    - etcd:**不在此构造**(需要 etcd I/O),抛错指引改用
      `pandorapy.cellroute_etcd.build_router`。与 Go 同样的分流,
      免得两个入口对 etcd 模式给出不同答案。
    """
    if cfg.mode == MODE_OFF:
        return None
    if cfg.mode == MODE_STATIC:
        if not cfg.cells:
            raise CellRouteError("cellroute: static mode needs cells")
        specs = [CellSpec(region_id=c.region_id, cell_id=c.cell_id) for c in cfg.cells]
        entries, region_of_cell = build_balanced_entries(specs)
        return Router(StaticTable(entries, region_of_cell))
    if cfg.mode == MODE_ETCD:
        if not cfg.etcd_endpoints:
            raise CellRouteError("cellroute: etcd mode needs etcd_endpoints")
        raise CellRouteError(
            "cellroute: etcd mode router must be built via pandorapy.cellroute_etcd in main"
        )
    raise CellRouteError(f"cellroute: unknown mode {cfg.mode!r} (want off/static/etcd)")
