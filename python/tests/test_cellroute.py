"""cellroute 装配层测试 —— 逐条对应 Go 侧 pkg/cellroute 的表测试。

为什么这些用例值得逐条写:cellroute 的失败形态**全是静默的**。
路由算错不会抛异常,只会让玩家的背包写进 A 区、档案写进 B 区;
表铺歪不会报错,只会让一批玩家永远落在 0 号 Cell。
所以这里的断言几乎全部指向"错了会不会被拒",而不是"对了会不会通过"。

命名与 Go 的 `cellroute_test.go` / `config_test.go` / `table_hotreload_test.go`
/ `keyspace_test.go` 一一对应,便于两栈对照 review。
"""

from __future__ import annotations

import pytest

from pandorapy import cellroute, cellroute_etcd


def _full_raw(region: int = 1, cell: int = 7) -> dict[int, str]:
    """铺一张全量合法 raw 表(所有逻辑分片指向同一个 Cell)。"""
    return {lc: f"{region}:{cell}" for lc in range(cellroute.LOGICAL_CELL_COUNT)}


# ── 装配:build_router ────────────────────────────────────────────────────────


def test_build_router_off_returns_none() -> None:
    """mode 为空 = 单 Cell 关闭态 → None(调用方注入 None,行为不变)。"""
    assert cellroute.build_router(cellroute.RouterConfig()) is None


def test_build_router_static_routes_player() -> None:
    cfg = cellroute.RouterConfig(
        mode="static",
        cells=[
            cellroute.CellEntry(region_id=1, cell_id=1),
            cellroute.CellEntry(region_id=1, cell_id=2),
        ],
    )
    router = cellroute.build_router(cfg)
    assert router is not None
    # 第 0 号逻辑分片必落第一个 Cell(连续区间分配的直接后果)。
    loc = router.route(cellroute.LOGICAL_CELL_COUNT)  # logical_cell == 0
    assert (loc.region_id, loc.cell_id, loc.logical_cell) == (1, 1, 0)
    # 同一 player_id 反复路由必须恒定 —— owner 不变量的最小可测形式。
    assert router.route(cellroute.LOGICAL_CELL_COUNT) == loc


def test_build_router_static_needs_cells() -> None:
    """static 但没配 cells:必须报错,不能退化成"空表" —— 空表会让每个玩家都路由失败。"""
    with pytest.raises(cellroute.CellRouteError, match="static mode needs cells"):
        cellroute.build_router(cellroute.RouterConfig(mode="static"))


def test_build_router_etcd_not_in_proc() -> None:
    """etcd 模式**刻意**不在此构造:它需要 I/O,必须走 cellroute_etcd。

    这条不是形式主义:若这里悄悄返回一张本地表,配了 etcd 的部署会拿到一张
    **永不热更**的表,扩容后新 Cell 的玩家全被路由到旧 Cell,且毫无日志。
    """
    cfg = cellroute.RouterConfig(mode="etcd", etcd_endpoints=["127.0.0.1:2379"])
    with pytest.raises(cellroute.CellRouteError, match="cellroute_etcd"):
        cellroute.build_router(cfg)


def test_build_router_unknown_mode_rejected() -> None:
    """拼错的 mode 必须报错而不是当成 off —— 否则运维以为分片开了、实际全落一个 Cell。"""
    with pytest.raises(cellroute.CellRouteError, match="unknown mode"):
        cellroute.build_router(cellroute.RouterConfig(mode="statik"))


# ── 装配:RouterConfig.validate_mode / enabled ───────────────────────────────


def test_router_config_validate() -> None:
    ok = cellroute.RouterConfig()
    ok.validate_mode()  # off 恒合法
    assert ok.enabled() is False

    static_ok = cellroute.RouterConfig(
        mode="static", cells=[cellroute.CellEntry(region_id=1, cell_id=1)]
    )
    static_ok.validate_mode()
    assert static_ok.enabled() is True

    with pytest.raises(cellroute.CellRouteError):
        cellroute.RouterConfig(mode="static").validate_mode()
    with pytest.raises(cellroute.CellRouteError):
        cellroute.RouterConfig(mode="etcd").validate_mode()
    with pytest.raises(cellroute.CellRouteError, match="unknown mode"):
        cellroute.RouterConfig(mode="hash").validate_mode()

    etcd_ok = cellroute.RouterConfig(mode="etcd", etcd_endpoints=["127.0.0.1:2379"])
    etcd_ok.validate_mode()
    assert etcd_ok.enabled() is True


# ── auction HRW:market_peer_list ────────────────────────────────────────────


def test_market_peer_list_dedup() -> None:
    cfg = cellroute.RouterConfig(
        market_peers=["a", " b ", "a", "", "  ", "b"], market_self="a"
    )
    # 去空去重 + **保序**(顺序会进日志与两栈对比)
    assert cfg.market_peer_list() == ["a", "b"]


def test_market_peer_list_includes_self() -> None:
    """self 不在 peers 里时必须补进去 —— 否则本实例把自己的 market 判给别人。"""
    cfg = cellroute.RouterConfig(market_peers=["b", "c"], market_self="a")
    assert cfg.market_peer_list() == ["b", "c", "a"]


def test_market_peer_list_empty_self_not_appended() -> None:
    cfg = cellroute.RouterConfig(market_peers=["b"], market_self="   ")
    assert cfg.market_peer_list() == ["b"]


# ── 表编解码:encode_entry / decode_entries ──────────────────────────────────


def test_encode_entry() -> None:
    assert cellroute.encode_entry(cellroute.Entry(region_id=12, cell_id=34)) == "12:34"


def test_decode_entries_round_trip() -> None:
    raw = {
        lc: cellroute.encode_entry(cellroute.Entry(region_id=2, cell_id=5))
        for lc in range(cellroute.LOGICAL_CELL_COUNT)
    }
    entries, region_of_cell = cellroute.decode_entries(raw)
    assert len(entries) == cellroute.LOGICAL_CELL_COUNT
    assert region_of_cell == {5: 2}
    assert entries[0] == cellroute.Entry(region_id=2, cell_id=5)


def test_decode_entries_rejects_missing_key() -> None:
    """缺项绝不能静默补 0 号 Cell:那会把一个配置缺口变成一批玩家的错落点。

    ★ 数量必须仍然是 4096 —— 否则先撞上数量闸,这条就退化成
      test_decode_entries_rejects_wrong_count 的重复,测不到"逐下标查在不在"。
    """
    raw = _full_raw()
    del raw[0]
    raw[cellroute.LOGICAL_CELL_COUNT] = "1:7"  # 越界 key 补足数量
    assert len(raw) == cellroute.LOGICAL_CELL_COUNT
    with pytest.raises(cellroute.CellRouteError, match="missing logical_cell 0"):
        cellroute.decode_entries(raw)


def test_decode_entries_rejects_wrong_count() -> None:
    raw = _full_raw()
    del raw[0]
    with pytest.raises(cellroute.CellRouteError, match="want LogicalCellCount"):
        cellroute.decode_entries(raw)


def test_decode_entries_rejects_bad_value() -> None:
    raw = _full_raw()
    raw[3] = "not-a-pair"
    with pytest.raises(cellroute.CellRouteError, match="bad value"):
        cellroute.decode_entries(raw)


def test_decode_entries_rejects_trailing_garbage() -> None:
    """★ 与 Go 的有意差异:Go 的 Sscanf 会接受 "12:34garbage",这里严格拒。

    差异只在畸形配置上显现,方向是 Python 在**启动期**大声拒绝,
    而不是运行期把 (12,34) 与一份别人看不懂的表一起用下去。
    """
    raw = _full_raw()
    raw[3] = "12:34garbage"
    with pytest.raises(cellroute.CellRouteError, match="bad value"):
        cellroute.decode_entries(raw)


def test_decode_entries_rejects_newline_injection() -> None:
    """正则用 \\A…\\Z 而不是 ^…$ 的原因:后者会放行 "12:34\\n5"。"""
    raw = _full_raw()
    raw[3] = "12:34\n5"
    with pytest.raises(cellroute.CellRouteError, match="bad value"):
        cellroute.decode_entries(raw)


def test_decode_entries_rejects_uint32_overflow() -> None:
    """Python 的 int 无上界;不显式查会让 Go 拒载的表在 Python 上加载成功。"""
    raw = _full_raw()
    raw[3] = f"1:{0xFFFFFFFF + 1}"
    with pytest.raises(cellroute.CellRouteError, match="exceeds uint32"):
        cellroute.decode_entries(raw)


def test_decode_entries_rejects_cell_region_conflict() -> None:
    """同一个 Cell 被声明在两个 Region:玩家的背包在 A 区、档案在 B 区,读不回来。"""
    raw = _full_raw(region=1, cell=7)
    raw[3] = "2:7"
    with pytest.raises(cellroute.CellRouteError, match="cell 7 mapped to region"):
        cellroute.decode_entries(raw)


def test_build_static_table_from_raw() -> None:
    table = cellroute.build_static_table_from_raw(_full_raw(region=3, cell=9))
    assert len(table) == cellroute.LOGICAL_CELL_COUNT
    assert table.lookup(0) == cellroute.Entry(region_id=3, cell_id=9)


# ── 热更表:AtomicTable ──────────────────────────────────────────────────────


def test_atomic_table_hot_swap() -> None:
    old = cellroute.build_static_table_from_raw(_full_raw(region=1, cell=1))
    new = cellroute.build_static_table_from_raw(_full_raw(region=1, cell=2))
    at = cellroute.AtomicTable(old)
    assert at.lookup(0) == cellroute.Entry(region_id=1, cell_id=1)
    at.store(new)
    # 整表替换:替换后**每个**下标都看到新表,不存在新旧混合。
    assert at.lookup(0) == cellroute.Entry(region_id=1, cell_id=2)
    assert at.lookup(cellroute.LOGICAL_CELL_COUNT - 1) == cellroute.Entry(
        region_id=1, cell_id=2
    )
    assert len(at) == cellroute.LOGICAL_CELL_COUNT


def test_atomic_table_rejects_nil() -> None:
    """允许 None 会把"表没配好"退化成"表是空的",空表让每个玩家路由失败。"""
    with pytest.raises(cellroute.CellRouteError, match="nil initial table"):
        cellroute.AtomicTable(None)  # type: ignore[arg-type]
    at = cellroute.AtomicTable(cellroute.build_static_table_from_raw(_full_raw()))
    with pytest.raises(cellroute.CellRouteError, match="store nil table"):
        at.store(None)  # type: ignore[arg-type]


def test_router_accepts_atomic_table() -> None:
    """Router 必须能直接吃热更表(Table Protocol 的存在理由)。"""
    at = cellroute.AtomicTable(
        cellroute.build_static_table_from_raw(_full_raw(region=4, cell=8))
    )
    router = cellroute.Router(at)
    assert router.route(12345).cell_id == 8
    at.store(cellroute.build_static_table_from_raw(_full_raw(region=4, cell=9)))
    # 热更后**同一个 router 实例**立刻看到新落点,无需重建。
    assert router.route(12345).cell_id == 9


# ── keyspace:in_cell_shard / route_full / cell_tag ──────────────────────────


def test_in_cell_shard() -> None:
    assert cellroute.in_cell_shard(0, 4) == 0
    assert cellroute.in_cell_shard(7, 4) == 3
    assert cellroute.in_cell_shard(8, 4) == 0
    # 单库必须显式传 1
    assert cellroute.in_cell_shard(123456789, 1) == 0


def test_in_cell_shard_rejects_bad_shard_count() -> None:
    """0 会除零;"默认当 1" 会让忘配分库数的部署静默把所有玩家算到 0 号库。"""
    for bad in (0, -1):
        with pytest.raises(cellroute.CellRouteError, match="shardsPerCell"):
            cellroute.in_cell_shard(1, bad)


def test_route_full_composes_three_tiers() -> None:
    router = cellroute.Router(
        cellroute.build_static_table_from_raw(_full_raw(region=2, cell=6))
    )
    full = router.route_full(1234567, 4)
    assert full.region_id == 2
    assert full.cell_id == 6
    assert full.logical_cell == 1234567 % cellroute.LOGICAL_CELL_COUNT
    assert full.in_cell_shard == 1234567 % 4
    # shards_per_cell 回带:分库数改了之后旧日志里的 shard 下标仍可解读。
    assert full.shards_per_cell == 4


def test_route_full_deterministic() -> None:
    router = cellroute.Router(cellroute.build_static_table_from_raw(_full_raw()))
    assert router.route_full(99, 8) == router.route_full(99, 8)


def test_route_full_rejects_bad_shard_count() -> None:
    router = cellroute.Router(cellroute.build_static_table_from_raw(_full_raw()))
    with pytest.raises(cellroute.CellRouteError, match="shardsPerCell"):
        router.route_full(99, 0)


def test_cell_tag() -> None:
    assert cellroute.cell_tag(1, 2) == "r1c2"
    assert cellroute.cell_tag(0, 0) == "r0c0"


# ── 逻辑分片映射本身 ────────────────────────────────────────────────────────


def test_logical_cell_count_is_the_contract() -> None:
    """★ 这个常量与 Go 侧必须同值。改它 = 全体玩家重新分片 = 所有 owner 数据错位。

    迁移期两栈同时在线,不同值会让同一玩家被路由到两个 cell,**而且不报错**。
    """
    assert cellroute.LOGICAL_CELL_COUNT == 4096


def test_logical_cell_of() -> None:
    assert cellroute.logical_cell_of(0) == 0
    assert cellroute.logical_cell_of(4096) == 0
    assert cellroute.logical_cell_of(4097) == 1


# ── etcd 装配口(不连 etcd 的三条分支)──────────────────────────────────────


@pytest.mark.asyncio
async def test_etcd_build_router_off() -> None:
    """off:不连 etcd,不起 watcher —— 单 Cell 部署必须一个连接都不建。"""
    router, watcher = await cellroute_etcd.build_router(cellroute.RouterConfig())
    assert router is None
    assert watcher is None


@pytest.mark.asyncio
async def test_etcd_build_router_static_does_not_touch_etcd() -> None:
    """static:本地铺表,watcher 为 None(没有需要 close 的东西)。"""
    cfg = cellroute.RouterConfig(
        mode="static", cells=[cellroute.CellEntry(region_id=1, cell_id=1)]
    )
    router, watcher = await cellroute_etcd.build_router(cfg)
    assert router is not None
    assert watcher is None
    assert router.route(1).cell_id == 1


@pytest.mark.asyncio
async def test_etcd_build_router_rejects_endpoint_without_port() -> None:
    """endpoint 缺端口必须报错。

    "默认补 2379" 会让一个写错的 endpoint 连到一台**碰巧存在**的 etcd 上,
    拿回一张别的集群的映射表 —— 玩家被路由到不存在的 Cell,且启动日志一切正常。
    """
    cfg = cellroute.RouterConfig(mode="etcd", etcd_endpoints=["etcd-host"])
    with pytest.raises(cellroute.CellRouteError, match="host:port"):
        await cellroute_etcd.build_router(cfg)


def test_etcd_default_prefix_matches_go() -> None:
    """前缀是跨栈契约:Go 铺的表 Python 必须读得到,反之亦然。"""
    assert cellroute_etcd.DEFAULT_PREFIX == "/pandora/cellroute/table/"
