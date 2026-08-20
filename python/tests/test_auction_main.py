"""auction 服务入口(main.py)、配置与几件"只在多实例/分库下才出事"的组件测试。

覆盖的是"起不来 / 起错了 / 起来了但算错"这一族缺陷 —— 它们全都**不会**在
普通业务测试里露头:

  - conf 默认值与 Go 侧 Defaults() 分叉:同一份 yaml 喂两个实现行为不同,两边都不报错
  - `rate_quota_per_min` 判据写成 `<= 0`:"-1 关闭限流"被悄悄改写成 20
  - `market_lock_ttl_seconds` 漏掉上界钳制:yaml 写 300 就拿着 5 分钟的锁跑
  - 分库路由公式漂移:历史订单被路由到另一个库 = 订单凭空消失
  - HRW 归属与 Go 算不出同一个 owner:迁移期两栈**同时**认为自己是 owner
  - 分片拓扑 identity 材料变了:每次启动都判"漂移"而拒启,或覆盖成 Go 认不出的值
  - 撮合方向判据写反:能成交的单全挂着不动(或反过来乱成交)

默认值 parity 刻意**从 Go 源码里读**而不是抄一份常量:抄一份的话,Go 改了默认值
这个测试照样绿(它验的是"我抄的值等于我抄的值")。
"""

from __future__ import annotations

import asyncio
import hashlib
import pathlib
import re

import pytest

from pandorapy import dbguard, errcode
from pandorapy.services.auction import biz as abiz
from pandorapy.services.auction import book as abook
from pandorapy.services.auction import conf as aconf
from pandorapy.services.auction import events as aevents
from pandorapy.services.auction import main as amain
from pandorapy.services.auction import owner_slots as aslots
from pandorapy.services.auction import repo as arepo
from pandorapy.services.auction import shard_topology as atopology
from pandorapy.services.auction import submit as asub
from pandorapy.services.auction.market_router import MarketRouter, hrw_score

GO_CONF = "services/economy/auction/internal/conf/conf.go"
GO_MAIN = "services/economy/auction/cmd/auction/main.go"
GO_ROUTER = "services/economy/auction/internal/biz/market_router.go"
GO_TOPOLOGY = "services/economy/auction/internal/data/shard_topology.go"
DEV_YAML = "services/economy/auction/etc/auction-dev.yaml"


# ── 配置:默认值与 Go 逐个对齐 ───────────────────────────────────────────────


def test_defaults_match_go_source(repo_root: pathlib.Path) -> None:
    """apply_defaults() 的每个默认值都从 Go 源码抓出来比对。"""
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")

    cfg = aconf.Config()
    cfg.apply_defaults()
    a = cfg.auction

    assert a.shard_topology_generation == "auction-v1"
    assert '"auction-v1"' in src

    for field, value, go_name in (
        ("max_quantity_per_order", 1_000_000, "MaxQuantityPerOrder"),
        ("max_price", 1_000_000_000, "MaxPrice"),
        ("max_active_orders_per_player", 200, "MaxActiveOrdersPerPlayer"),
        ("rate_quota_per_min", 20, "RateQuotaPerMin"),
        ("default_list_limit", 50, "DefaultListLimit"),
        ("max_list_limit", 200, "MaxListLimit"),
        ("expiry_sweep_interval_seconds", 60, "ExpirySweepIntervalSeconds"),
        ("expiry_sweep_batch", 200, "ExpirySweepBatch"),
        ("side_effect_reconcile_interval_seconds", 5, "SideEffectReconcileIntervalSeconds"),
        ("side_effect_reconcile_batch", 100, "SideEffectReconcileBatch"),
        ("audit_queue_capacity", 1024, "AuditQueueCapacity"),
        ("market_lock_ttl_seconds", 30, "MarketLockTTLSeconds"),
        ("market_lock_max_wait_ms", 3000, "MarketLockMaxWaitMs"),
        ("retention_days", 90, "RetentionDays"),
        ("retention_sweep_interval_seconds", 3600, "RetentionSweepIntervalSeconds"),
        ("retention_sweep_batch", 500, "RetentionSweepBatch"),
    ):
        assert getattr(a, field) == value, field
        # Go 的字面量写法允许下划线分隔(1_000_000),两种都认。
        pattern = rf"c\.Auction\.{go_name}\s*=\s*{value:_}\b|c\.Auction\.{go_name}\s*=\s*{value}\b"
        assert re.search(pattern, src), go_name

    # 端口:Envoy cluster / run_services.ps1 端口占用检查 / K8s Service 都钉在这两个数上。
    assert cfg.server.grpc.addr == ":20016"
    assert cfg.server.http.addr == ":21016"
    assert '":20016"' in src and '":21016"' in src


def test_rate_quota_uses_equals_zero_not_le_zero(repo_root: pathlib.Path) -> None:
    """★ rate_quota_per_min 的判据是 `== 0`,不是 `<= 0` —— 负值 = **显式关闭**配额。

    换成 `<= 0` 会把 "-1 关闭限流" 悄悄改写成 20,配置意图被反转且无任何日志。
    """
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")
    assert re.search(r"c\.Auction\.RateQuotaPerMin\s*==\s*0", src)

    cfg = aconf.Config(auction=aconf.AuctionConf(rate_quota_per_min=-1))
    cfg.apply_defaults()
    assert cfg.auction.rate_quota_per_min == -1


def test_market_lock_ttl_is_clamped_to_30s(repo_root: pathlib.Path) -> None:
    """★ 上界钳制(不变量 §10:Redis lock TTL ≤ 30s)不能省。

    只判 `<= 0` 会放行 yaml 里写的 300s —— 一个卡死的实例会把整个 market
    锁住 5 分钟,而锁本身"工作正常",没有任何错误日志。
    """
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")
    assert re.search(r"MarketLockTTLSeconds\s*<=\s*0\s*\|\|.*MarketLockTTLSeconds\s*>\s*30", src)

    cfg = aconf.Config(auction=aconf.AuctionConf(market_lock_ttl_seconds=300))
    cfg.apply_defaults()
    assert cfg.auction.market_lock_ttl_seconds == 30

    cfg = aconf.Config(auction=aconf.AuctionConf(market_lock_ttl_seconds=5))
    cfg.apply_defaults()
    assert cfg.auction.market_lock_ttl_seconds == 5, "合法值不该被改写"


def test_loads_the_same_dev_yaml_as_go(repo_root: pathlib.Path) -> None:
    """读 Go 版**同一份** yaml,而不是测试专用配置。

    kafka / cell_route 两段尤其要验:它们在 Go 侧属于 config.Base,Python 的
    BaseConf 还没建到 —— 没建模就会静默落进 extra,表现为"配了却不生效、零信号"。
    kafka.brokers 落进 extra 的具体后果是启动期 `kafka_brokers_required`
    把一份**配置正确**的部署拒之门外。
    """
    cfg = aconf.Config.load(repo_root / DEV_YAML)

    assert cfg.server.grpc.addr == ":20016"
    assert cfg.server.http.addr == ":21016"
    assert cfg.server.grpc.enable_reflection is True
    assert cfg.node.node_id == 1
    assert cfg.node.mysql_client.dsn.startswith("pandora:")
    assert cfg.node.mysql_client.shards == []
    assert cfg.node.redis_client.host == "127.0.0.1:6380"
    assert cfg.kafka.brokers == ["127.0.0.1:9093"], "kafka 段落进了 extra"
    assert cfg.kafka.idempotent is True
    assert cfg.auction.inventory_addr == "127.0.0.1:20015"
    assert cfg.auction.order_ttl_seconds == 604800
    assert cfg.auction.passive_warmup is False
    assert cfg.auction.allow_shard_topology_bootstrap is False


def test_cell_route_mode_still_fails_fast(repo_root: pathlib.Path) -> None:
    """★ auction 把 cell_route 建成了正式字段,基类那道 mode 闸必须重新接上。

    基类 `BaseConf.assert_unsupported_sections` 读的是 model_extra —— 一旦建模,
    它就查不到 cell_route 了。漏掉重写的后果:配了 `cell_route.mode: static`
    的部署照常启动、所有玩家落在单 Cell 上,而 yaml 看起来一切正常。
    """
    with pytest.raises(Exception, match="cell_route"):
        aconf.Config.model_validate({"cell_route": {"mode": "static"}})

    # 关闭态(mode 为空)是**合法的单 Cell 配置**,不能被这道闸误伤。
    cfg = aconf.Config.model_validate({"cell_route": {"mode": "", "market_self": "a-0"}})
    assert cfg.cell_route.market_self == "a-0"


def test_market_peer_list_dedups_and_includes_self() -> None:
    """peers 归一化:去空、去重、确保 self 在内(对应 Go 的 MarketPeerList)。

    漏了"补入 self"会让本实例把自己的 market 判给别人,每条写请求都打一条
    误路由 WARN 而业务照常(锁兜底)—— 排查时极难联想到配置。
    """
    c = aconf.CellRouteConf(
        market_self="auction-1", market_peers=["auction-0", "", "auction-0", " auction-2 "]
    )
    assert c.market_peer_list() == ["auction-0", "auction-2", "auction-1"]

    c2 = aconf.CellRouteConf(market_self="auction-0", market_peers=["auction-0"])
    assert c2.market_peer_list() == ["auction-0"]


def test_retention_mode_typo_is_rejected() -> None:
    """★ 拼错的清理模式必须 fail-fast,不能静默回落 report_only。"""
    cfg = aconf.Config(auction=aconf.AuctionConf(retention_mode="delet"))
    with pytest.raises(ValueError):
        cfg.auction.validate_retention_mode()

    cfg = aconf.Config(auction=aconf.AuctionConf(retention_mode="delete"))
    cfg.auction.validate_retention_mode()
    assert cfg.auction.retention_mode_parsed() is dbguard.Mode.DELETE

    # 留空 = 默认只报告不删(2026-07-22 用户指令)。
    assert aconf.AuctionConf().retention_mode_parsed() is dbguard.Mode.REPORT_ONLY


# ── 启动闸:事件名 / 顺序与 Go 一致 ─────────────────────────────────────────


def test_gate_event_names_exist_in_go_main(repo_root: pathlib.Path) -> None:
    """★ 事件名必须与 Go **逐字相同**:Loki 上按事件名建的告警对不上 = 静默失去覆盖。"""
    go_src = (repo_root / GO_MAIN).read_text(encoding="utf-8")
    py_src = pathlib.Path(amain.__file__).read_text(encoding="utf-8")

    shared = [
        "config_load_failed",
        "config_scan_failed",
        "auction_retention_mode_invalid",
        "auction_mysql_shard_count_unsupported",
        "auction_mysql_single_shard_list_invalid",
        "mysql_shardset_failed",
        "mysql_required",
        "mysql_strict_mode_required",
        "auction_mysql_shard_topology_rejected",
        "auction_mysql_shard_topology_verified",
        "redis_endpoint_required",
        "redis_ping_failed",
        "kafka_brokers_required",
        "kafka_match_events_explicitly_disabled",
        "kafka_match_producer_init_failed_outbox_will_retry",
        "kafka_audit_producer_init_failed",
        "kafka_producer_ready",
        "settlement_ledger_missing",
        "settlement_ledger_noop",
        "settlement_ledger_ready",
        "auction_rate_quota_ready",
        "market_router_enabled",
        "cross_instance_lock_forced_on",
        "market_locker_ready",
        "auction_passive_warmup_enabled",
        "side_effect_reconciler_ready",
        "match_event_reconciler_ready",
        "retention_sweeper_ready",
        "expiry_sweeper_ready",
        "service_ready",
        "mysql_connected",
        "redis_connected",
    ]
    for name in shared:
        assert name in go_src, f"Go 侧没有 {name},清单过期了"
        assert name in py_src, f"Python 侧漏了启动事件 {name}"


def test_shard_count_gate_rejects_more_than_two() -> None:
    """★ 分片数 > 2 必须拒:owner idempotency registry 还没全量回填,
    跨分片幂等查询会漏掉历史订单 → 同一 idem 重复挂单(玩家挂了两次)。"""
    logs: list[tuple[str, dict]] = []

    class _Logger:
        def error(self, event, **kw):  # noqa: ANN001, ANN201
            logs.append((event, kw))

    cfg = aconf.Config.model_validate(
        {"node": {"mysql_client": {"shards": ["a", "b", "c"]}}}
    )
    assert amain._build_pools_plan(cfg, _Logger()) is None
    assert logs[0][0] == "auction_mysql_shard_count_unsupported"


def test_single_shard_list_gate() -> None:
    """★ shards 只写 1 个必须拒:路由公式退化成 `id % 1`(恒 0)却仍走分库路径,
    将来加第二片时全部历史数据都在错的位置。"""
    logs: list[str] = []

    class _Logger:
        def error(self, event, **kw):  # noqa: ANN001, ANN201
            logs.append(event)

    cfg = aconf.Config.model_validate({"node": {"mysql_client": {"shards": ["only-one"]}}})
    assert amain._build_pools_plan(cfg, _Logger()) is None
    assert logs == ["auction_mysql_single_shard_list_invalid"]


def test_mysql_required_gate() -> None:
    logs: list[str] = []

    class _Logger:
        def error(self, event, **kw):  # noqa: ANN001, ANN201
            logs.append(event)

    assert amain._build_pools_plan(aconf.Config(), _Logger()) is None
    assert logs == ["mysql_required"]


def test_plan_keeps_dsn_order(repo_root: pathlib.Path) -> None:
    """★ DSN 顺序是拓扑身份的一部分(shard_index 按下标写进 marker)。

    调换两个 DSN 的顺序 = 全部历史数据被路由到另一个库,而且不报错。
    """
    cfg = aconf.Config.model_validate(
        {"node": {"mysql_client": {"shards": ["dsn-a", "dsn-b"]}}}
    )
    plan = amain._build_pools_plan(cfg, None)
    assert plan == (["dsn-a", "dsn-b"], "sharded")


# ── 分库路由:公式必须与 Go 逐字相同 ───────────────────────────────────────


def test_db_router_formula_matches_go() -> None:
    """shard = id % N。改 N 或改取模对象都会让历史数据被路由丢失(查不到 = 订单消失)。"""
    pools = ["shard0", "shard1"]
    r = arepo.DBRouter(pools)
    assert r.count() == 2
    for i in range(10):
        assert r.for_market(i) == pools[i % 2]
        assert r.for_owner(i) == pools[i % 2]
    assert r.all() == ("shard0", "shard1")


def test_db_router_rejects_empty() -> None:
    with pytest.raises(ValueError):
        arepo.DBRouter([])


# ── HRW 市场归属:必须与 Go 算出同一个 owner ───────────────────────────────


def test_hrw_score_matches_go_algorithm(repo_root: pathlib.Path) -> None:
    """★ 逐位复算一遍 Go 的 hrwScore,证明常量与字节序都没抄错。

    两栈算不出同一个 owner 的后果:迁移期 Go 副本和 Python 副本**同时**认为
    自己是某 market 的 owner,双写窗口被打开,而两边日志只会各打一条
    "非 owner 实例处理"的 WARN(还是打在对方那边)。
    """
    src = (repo_root / GO_ROUTER).read_text(encoding="utf-8")
    # 三个魔数与大端字节序都得在 Go 源码里对得上。
    assert "0x9e3779b97f4a7c15" in src
    assert "0xbf58476d1ce4e5b9" in src
    assert "0x94d049bb133111eb" in src
    assert "byte(v >> 24)" in src

    mask = (1 << 64) - 1

    def fnv(data: bytes) -> int:
        h = 0xCBF29CE484222325
        for b in data:
            h ^= b
            h = (h * 0x100000001B3) & mask
        return h

    def expect(peer: str, market: int) -> int:
        hn = fnv(peer.encode())
        hk = fnv(bytes(((market >> 24) & 0xFF, (market >> 16) & 0xFF,
                        (market >> 8) & 0xFF, market & 0xFF)))
        z = hn ^ ((hk + 0x9E3779B97F4A7C15 + ((hn << 6) & mask) + (hn >> 2)) & mask)
        z = (z + 0x9E3779B97F4A7C15) & mask
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & mask
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & mask
        return (z ^ (z >> 31)) & mask

    for peer in ("auction-0", "auction-1", "n1", "n2"):
        for market in (0, 1, 7, 4002, 0xFFFFFFFF):
            assert hrw_score(peer, market) == expect(peer, market), (peer, market)


def test_market_router_is_deterministic_and_balanced() -> None:
    """同一 market 恒定落同一实例;成员集合不变时结果稳定。

    近似 ID(只差末位)必须能轮流胜出 —— 单遍 FNV 扩散不足会让某实例恒定拿全部
    market,那时跨实例锁竞争回到最坏情形,而没有任何日志说明。
    """
    router = MarketRouter.build("n1", ["n1", "n2", "n3"])
    assert router is not None
    owners = {m: router.owner(m) for m in range(1, 200)}
    assert {router.owner(m) for m in range(1, 200)} == {"n1", "n2", "n3"}
    for m, owner in owners.items():
        assert router.owner(m) == owner
        assert router.owns_market(m) == (owner == "n1")


def test_market_router_none_when_self_empty() -> None:
    """self 留空 = 单实例,本实例拥有全部 market(退化为现状)。"""
    assert MarketRouter.build("", ["a", "b"]) is None


def test_market_router_adds_self_to_peers() -> None:
    router = MarketRouter.build("n9", ["n1", "n2"])
    assert router is not None
    assert router.peer_count() == 3
    assert any(router.owns_market(m) for m in range(1, 500)), "self 没被算进候选"


# ── 分片拓扑 marker ────────────────────────────────────────────────────────


def test_shard_identity_material_matches_go(repo_root: pathlib.Path) -> None:
    """★ identity 的**材料与顺序**是契约:network \\x00 lower(addr) \\x00 dbname。

    Go 已经把这个值写进了生产库的 marker 行。Python 算出别的值 =
    每次启动都判"拓扑漂移"而拒启;bootstrap 开着时更糟 —— 会把 marker
    覆盖成 Go 再也认不出来的值。
    """
    src = (repo_root / GO_TOPOLOGY).read_text(encoding="utf-8")
    assert 'network + "\\x00" + strings.ToLower(cfg.Addr) + "\\x00" + cfg.DBName' in src
    assert '"auction-shard-topology-v1\\x00"' in src

    dsn = "u:p@tcp(127.0.0.1:3307)/pandora_auction?parseTime=true"
    expected = hashlib.sha256(b"tcp\x00127.0.0.1:3307\x00pandora_auction").hexdigest()
    assert atopology.shard_identity(dsn) == expected


def test_shard_identity_defaults_port_3306() -> None:
    """DSN 省略端口时 Go 的 ParseDSN 会补 :3306,两边必须补出同一个 addr。"""
    a = atopology.shard_identity("u:p@tcp(db-host)/pandora_auction")
    b = atopology.shard_identity("u:p@tcp(db-host:3306)/pandora_auction")
    assert a == b


def test_build_expected_rejects_duplicate_shards() -> None:
    """两个 DSN 指向同一个逻辑库 = 分库其实没分,写入会互相覆盖。"""
    dsn = "u:p@tcp(127.0.0.1:3307)/pandora_auction"
    with pytest.raises(atopology.ShardTopologyError, match="same logical database"):
        atopology.build_expected("auction-v1", [dsn, dsn])


def test_build_expected_rejects_bad_generation() -> None:
    for bad in ("", "a" * 65, "auction v1", "auction/v1"):
        with pytest.raises(atopology.ShardTopologyError):
            atopology.build_expected(bad, ["u:p@tcp(h:3306)/db"])


def test_topology_hash_changes_with_order() -> None:
    """★ 换 DSN 顺序必须换 hash —— 否则调换顺序这种致命漂移检测不到。"""
    a = "u:p@tcp(h1:3306)/pandora_auction"
    b = "u:p@tcp(h2:3306)/pandora_auction"
    assert (
        atopology.build_expected("auction-v1", [a, b]).topology_hash
        != atopology.build_expected("auction-v1", [b, a]).topology_hash
    )


@pytest.mark.asyncio
async def test_bootstrap_gate_blocks_uninitialized_two_shards() -> None:
    """★ 两分片首次登记必须显式授权;`allow_bootstrap=false` 时拒启。

    不拒的话,一次误配(比如把 shards 从 1 个改成 2 个)会当场把新 marker 写进去,
    从此"拓扑漂移"这道唯一防线永远也拦不住第一次。
    """
    pools = [_FakeTopologyPool(None), _FakeTopologyPool(None)]
    dsns = ["u:p@tcp(h1:3306)/pandora_auction", "u:p@tcp(h2:3306)/pandora_auction"]
    with pytest.raises(atopology.ShardTopologyError, match="uninitialized"):
        await atopology.validate_shard_topology(pools, "auction-v1", dsns, False)
    assert all(not p.inserted for p in pools), "被拒之后仍然写了 marker"


@pytest.mark.asyncio
async def test_single_shard_bootstraps_automatically() -> None:
    """单库首次升级可自动登记(与 Go 同):没有"顺序 / 片数"可漂移。"""
    pool = _FakeTopologyPool(None)
    dsns = ["u:p@tcp(h1:3306)/pandora_auction"]
    await atopology.validate_shard_topology([pool], "auction-v1", dsns, False)
    assert pool.inserted


@pytest.mark.asyncio
async def test_mismatched_marker_is_rejected_even_with_bootstrap() -> None:
    """★ 已有 marker 不一致时,bootstrap 开关也**绝不允许覆盖**。"""
    expected = atopology.build_expected("auction-v1", ["u:p@tcp(h1:3306)/pandora_auction"])
    stored = ("auction-v0", expected.topology_hash, 1, 0, expected.identities[0])
    pool = _FakeTopologyPool(stored)
    with pytest.raises(atopology.ShardTopologyError, match="mismatch"):
        await atopology.validate_shard_topology(
            [pool], "auction-v1", ["u:p@tcp(h1:3306)/pandora_auction"], True
        )


class _FakeTopologyPool:
    """最小 asyncmy 连接池替身:只支持拓扑 marker 的读 / INSERT IGNORE。"""

    def __init__(self, stored) -> None:  # noqa: ANN001
        self.stored = stored
        self.inserted = False

    def acquire(self):  # noqa: ANN201
        return _FakeConnCtx(self)


class _FakeConnCtx:
    def __init__(self, pool: _FakeTopologyPool) -> None:
        self._pool = pool

    async def __aenter__(self):  # noqa: ANN204
        return self

    async def __aexit__(self, *exc) -> bool:  # noqa: ANN002
        return False

    def cursor(self):  # noqa: ANN201
        return _FakeCursorCtx(self._pool)


class _FakeCursorCtx:
    def __init__(self, pool: _FakeTopologyPool) -> None:
        self._pool = pool
        self._row = None

    async def __aenter__(self):  # noqa: ANN204
        return self

    async def __aexit__(self, *exc) -> bool:  # noqa: ANN002
        return False

    async def execute(self, sql: str, params) -> None:  # noqa: ANN001
        if sql.strip().upper().startswith("SELECT"):
            self._row = self._pool.stored
            return
        # INSERT IGNORE:登记后立刻可读回(与真实库同)。
        self._pool.inserted = True
        self._pool.stored = (params[1], params[2], params[3], params[4], params[5])

    async def fetchone(self):  # noqa: ANN201
        return self._row


# ── 撮合判据 / 订单簿编码 ─────────────────────────────────────────────────


def test_crosses_direction() -> None:
    """★ 判据写反不会报错,只会让能成交的单全挂着不动(或反过来乱成交)。"""
    # incoming SELL @100:对手最高买价 >= 100 才成交。
    assert abiz.crosses(asub.SIDE_SELL, 100, 120)
    assert abiz.crosses(asub.SIDE_SELL, 100, 100)
    assert not abiz.crosses(asub.SIDE_SELL, 100, 99)
    # incoming BUY @100:对手最低卖价 <= 100 才成交。
    assert abiz.crosses(asub.SIDE_BUY, 100, 80)
    assert abiz.crosses(asub.SIDE_BUY, 100, 100)
    assert not abiz.crosses(asub.SIDE_BUY, 100, 101)


def test_opposite_side() -> None:
    assert abiz.opposite(asub.SIDE_SELL) == asub.SIDE_BUY
    assert abiz.opposite(asub.SIDE_BUY) == asub.SIDE_SELL


def test_book_key_and_score_encoding() -> None:
    """★ 这套编码是**跨版本契约**:旧实例还在按它读簿。

    改 key 前缀 / score 正负 / member 宽度,旧实例会读到空簿或顺序反了的簿,
    而两边都不报错。
    """
    assert abook.book_key(4002, asub.SIDE_SELL) == "pandora:auction:book:{4002}:ask"
    assert abook.book_key(4002, asub.SIDE_BUY) == "pandora:auction:book:{4002}:bid"
    # 卖盘升序取最低价;买盘取负分,升序即最高价。
    assert abook.score_of(asub.SIDE_SELL, 100) == 100.0
    assert abook.score_of(asub.SIDE_BUY, 100) == -100.0
    assert abook.member_of(42) == "0" * 18 + "42"
    assert len(abook.member_of(42)) == 20


def test_owner_slot_member_carries_market_id() -> None:
    """★ member 必须带 market_id。

    只存 order_id 的话,两分片部署下惰性清理会去错库、查无此单 →
    按"不存在"释放名额,而那张单其实还活着 —— 配额被穿透且不报错。
    """
    slot = aslots.OwnerOrderSlot(market_id=4002, order_id=987654321)
    member = aslots.owner_slot_member(slot)
    assert member == f"{4002:010d}:{987654321:020d}"
    assert aslots.parse_owner_slot_member(member) == slot
    assert aslots.owner_slot_key(7) == "pandora:auction:owner-slots:{7}"


@pytest.mark.parametrize("bad", ["", "abc", "0:1", "1:0", "a:b", "1:2:3"])
def test_parse_owner_slot_member_rejects_garbage(bad: str) -> None:
    with pytest.raises(ValueError):
        aslots.parse_owner_slot_member(bad)


def test_kafka_topics_match_go_build_topic(repo_root: pathlib.Path) -> None:
    """topic 名是 producer 与 consumer 唯一的约会地点,写错一个字符两侧都不报错。"""
    go_src = (repo_root / GO_MAIN).read_text(encoding="utf-8")
    assert 'BuildTopic("auction", "match")' in go_src
    assert 'BuildTopic("auction", "audit")' in go_src
    assert aevents.MATCH_TOPIC == "pandora.auction.match"
    assert aevents.AUDIT_TOPIC == "pandora.auction.audit"


# ── 撮合引擎:用假 repo 驱动一整条成交链 ─────────────────────────────────


class _FakeMatchRepo:
    """只实现撮合链要用的几个方法。"""

    def __init__(self, resting: asub.OrderRecord | None) -> None:
        self.resting = resting
        self.cleared_match_pending: list[int] = []
        self.completed: list[int] = []
        self.reserved_calls = 0

    async def find_best_active_order(self, market_id, item_config_id, side, exclude_owner):  # noqa: ANN001
        if self.resting is None:
            return None, False
        return self.resting, True

    async def reserve_match(self, market_id, incoming_id, resting_id, match_id, ts):  # noqa: ANN001
        self.reserved_calls += 1
        incoming = _clone(self._incoming)
        resting = _clone(self.resting)
        qty = min(
            incoming.quantity - incoming.filled_quantity,
            resting.quantity - resting.filled_quantity,
        )
        incoming.filled_quantity += qty
        resting.filled_quantity += qty
        incoming.status = (
            asub.STATUS_FILLED
            if incoming.filled_quantity == incoming.quantity
            else asub.STATUS_PARTIAL
        )
        resting.status = (
            asub.STATUS_FILLED
            if resting.filled_quantity == resting.quantity
            else asub.STATUS_PARTIAL
        )
        incoming.match_pending = incoming.filled_quantity < incoming.quantity
        m = arepo.MatchRecord(
            match_id=match_id,
            market_id=market_id,
            sell_order_id=resting_id,
            buy_order_id=incoming_id,
            seller_id=resting.owner_id,
            buyer_id=incoming.owner_id,
            item_config_id=incoming.item_config_id,
            quantity=qty,
            price=resting.price,
            matched_at_ms=ts,
        )
        self.resting = None if resting.filled_quantity == resting.quantity else resting
        return m, incoming, resting, True

    async def complete_match(self, market_id, match_id):  # noqa: ANN001
        self.completed.append(match_id)
        return True

    async def clear_match_pending(self, market_id, order_id):  # noqa: ANN001
        self.cleared_match_pending.append(order_id)
        return True

    async def get_releasable_order(self, market_id, order_id):  # noqa: ANN001
        return None, False


def _clone(o: asub.OrderRecord) -> asub.OrderRecord:
    c = asub.OrderRecord()
    for f in o.__slots__:
        setattr(c, f, getattr(o, f))
    return c


class _RecordingLedger:
    def __init__(self) -> None:
        self.settled: list[arepo.MatchRecord] = []

    async def settle(self, m) -> None:  # noqa: ANN001
        self.settled.append(m)

    async def release(self, owner_id, order_id) -> None:  # noqa: ANN001
        return None


class _Snowflake:
    def __init__(self, start: int = 5000) -> None:
        self._n = start

    def generate(self) -> int:
        self._n += 1
        return self._n


def _usecase(repo, ledger) -> abiz.AuctionUsecase:  # noqa: ANN001
    cfg = aconf.Config()
    cfg.apply_defaults()
    return abiz.AuctionUsecase(
        repo, None, None, ledger, None, _Snowflake(), _Snowflake(9000), cfg.auction
    )


@pytest.mark.asyncio
async def test_match_settles_and_uses_resting_price() -> None:
    """★ 成交价 = **被动挂单价**(resting.price),不是 incoming 的报价。

    传错的话买家多付 / 卖家少收,而账目两边都平 —— 对不出账,只有玩家能察觉。
    """
    resting = asub.OrderRecord(
        order_id=111, market_id=1, owner_id=2001, side=asub.SIDE_SELL,
        item_config_id=5001, quantity=10, price=80, status=asub.STATUS_OPEN,
        escrow_verified=True,
    )
    repo = _FakeMatchRepo(resting)
    ledger = _RecordingLedger()
    uc = _usecase(repo, ledger)

    incoming = asub.OrderRecord(
        order_id=222, market_id=1, owner_id=1001, side=asub.SIDE_BUY,
        item_config_id=5001, quantity=10, price=100, status=asub.STATUS_PENDING,
        escrow_verified=True,
    )
    repo._incoming = incoming
    await uc._match(incoming)

    assert len(ledger.settled) == 1
    assert ledger.settled[0].price == 80, "用了 incoming 的报价而不是被动挂单价"
    assert incoming.filled_quantity == 10
    assert incoming.status == asub.STATUS_FILLED
    assert repo.completed == [ledger.settled[0].match_id]


@pytest.mark.asyncio
async def test_match_stops_when_price_does_not_cross() -> None:
    """价格不交叉时一笔都不该成交(而且不能空转)。"""
    resting = asub.OrderRecord(
        order_id=111, market_id=1, owner_id=2001, side=asub.SIDE_SELL,
        item_config_id=5001, quantity=10, price=200, status=asub.STATUS_OPEN,
        escrow_verified=True,
    )
    repo = _FakeMatchRepo(resting)
    ledger = _RecordingLedger()
    uc = _usecase(repo, ledger)
    incoming = asub.OrderRecord(
        order_id=222, market_id=1, owner_id=1001, side=asub.SIDE_BUY,
        item_config_id=5001, quantity=10, price=100, status=asub.STATUS_PENDING,
        escrow_verified=True,
    )
    repo._incoming = incoming
    await uc._match(incoming)
    assert ledger.settled == []
    assert repo.reserved_calls == 0
    assert incoming.filled_quantity == 0


@pytest.mark.asyncio
async def test_match_clears_match_pending_when_book_exhausted() -> None:
    """★ marker 只有在"完整扫到无交叉候选"之后才准清。

    提前清会让一张仍有对手盘的 PARTIAL 单再也无人续跑(它不会被任何人当 incoming)。
    """
    repo = _FakeMatchRepo(None)
    uc = _usecase(repo, _RecordingLedger())
    incoming = asub.OrderRecord(
        order_id=222, market_id=1, owner_id=1001, side=asub.SIDE_BUY,
        item_config_id=5001, quantity=10, filled_quantity=4, price=100,
        status=asub.STATUS_PARTIAL, escrow_verified=True, match_pending=True,
    )
    repo._incoming = incoming
    await uc._match(incoming)
    assert repo.cleared_match_pending == [222]
    assert incoming.match_pending is False


@pytest.mark.asyncio
async def test_audit_queue_drops_instead_of_blocking() -> None:
    """★ audit 队列满时**丢弃**,绝不反压交易主路径。

    反压的后果:broker 一慢,撮合就跟着卡在持有 market 锁的临界区里,
    整个 market 停摆 —— 而 audit 本来就不承担资产正确性。
    """
    cfg = aconf.Config(auction=aconf.AuctionConf(audit_queue_capacity=2))
    cfg.apply_defaults()
    uc = abiz.AuctionUsecase(
        _FakeMatchRepo(None), None, None, _RecordingLedger(), None,
        _Snowflake(), _Snowflake(9000), cfg.auction,
    )
    order = abiz.to_proto_order(asub.OrderRecord(order_id=1, market_id=1, owner_id=2))
    # 没有 worker 在消费,第 3 条起必然丢弃;不阻塞就是通过判据。
    for _ in range(5):
        await asyncio.wait_for(asyncio.to_thread(lambda: None), timeout=1)
        uc._push_audit(order)
    assert uc._audit_queue.qsize() == 2


@pytest.mark.asyncio
async def test_list_my_orders_pagination_contract() -> None:
    """多取一条判 has_more;next_cursor 是**本页末**的 order_id。

    写成"下一页首条"会让分页每次跳过一条订单,而客户端完全看不出来。
    """
    recs = [
        asub.OrderRecord(order_id=oid, market_id=1, owner_id=7, status=asub.STATUS_OPEN)
        for oid in (500, 400, 300)
    ]

    class _Repo:
        async def list_owner_orders(self, owner_id, active_only, cursor, limit):  # noqa: ANN001
            assert limit == 3, "没有多取一条,has_more 只能靠猜"
            return recs

    uc = _usecase(_Repo(), _RecordingLedger())
    orders, next_cursor, has_more = await uc.list_my_orders(7, False, 0, 2)
    assert [o.order_id for o in orders] == [500, 400]
    assert has_more is True
    assert next_cursor == 400


@pytest.mark.asyncio
async def test_list_my_orders_limit_is_clamped() -> None:
    """limit 上限 100(§9.18 读取侧上限);0 → 默认 50。"""
    seen: list[int] = []

    class _Repo:
        async def list_owner_orders(self, owner_id, active_only, cursor, limit):  # noqa: ANN001
            seen.append(limit)
            return []

    uc = _usecase(_Repo(), _RecordingLedger())
    await uc.list_my_orders(7, False, 0, 0)
    await uc.list_my_orders(7, False, 0, 9999)
    assert seen == [51, 101]


@pytest.mark.asyncio
async def test_passive_warmup_rejects_writes() -> None:
    """★ 只读预热档必须拒掉挂单 / 出价 / 撤单和全部补偿。

    漏掉任何一条都会让 R3 green 在旧 matcher 还活着时开始写 —— 双写超卖。
    """
    cfg = aconf.Config(auction=aconf.AuctionConf(passive_warmup=True))
    cfg.apply_defaults()
    uc = abiz.AuctionUsecase(
        _FakeMatchRepo(None), None, None, _RecordingLedger(), None,
        _Snowflake(), _Snowflake(9000), cfg.auction,
    )
    with pytest.raises(errcode.PandoraError) as e1:
        await uc.place_order(1, 1, 5001, 1, 1, "k")
    assert e1.value.code == errcode.ErrUnavailable
    with pytest.raises(errcode.PandoraError):
        await uc.bid(1, 1, 5001, 1, 1, "k")
    with pytest.raises(errcode.PandoraError):
        await uc.cancel_order(1, 1, 1)
    with pytest.raises(errcode.PandoraError):
        await uc.expire_due_orders()
    with pytest.raises(errcode.PandoraError):
        await uc.reconcile_pending_side_effects()
    with pytest.raises(errcode.PandoraError):
        await uc.reconcile_pending_match_events()
