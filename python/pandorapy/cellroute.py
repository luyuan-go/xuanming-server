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

# 决策 2026-06-26:采纳 4096。改这个数等于全体玩家重新分片,是永久契约。
LOGICAL_CELL_COUNT = 4096


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


def logical_cell_of(player_id: int) -> int:
    """player_id → 逻辑分片号。纯函数,确定性。对应 Go 的 LogicalCellOf。"""
    return player_id % LOGICAL_CELL_COUNT


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

    def __init__(self, table: StaticTable) -> None:
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
