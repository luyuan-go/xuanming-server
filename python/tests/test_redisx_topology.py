"""Redis 选型与启动期 Ping 闸 —— 对应 Go `redisx.NewUniversalClient` + 18 个服务的
`redis_ping_failed` 闸。

**这里补的是一个实测过的静默分叉**（2026-08-19）：建模之前 `node.redis_client` 整段
落进 pydantic 的 extra，`new_client(addr)` 只认单实例，`addrs` / `master_name`
在 Python 侧**没有任何代码路径**。于是 Sentinel 部署常见的"只填 addrs、host 留空"：

    Python: host='' → new_client('') → 实际连 **127.0.0.1:6379**，进程照常起来
    Go:     panic("redis endpoint required ...")

连上一个无关的本机 Redis 还能正常 Ready，是最糟的一档 —— 所有读都 miss、所有写都
落到别处，而两边日志都干净。
"""

from __future__ import annotations

import pytest

from pandorapy import config, redisx


def _conf(**kw) -> config.RedisConf:
    return config.RedisConf(**kw)


# ── 端点缺失必须拒绝，不得回落 localhost ─────────────────────────────────

def test_missing_endpoint_is_refused_not_defaulted_to_localhost() -> None:
    """★ 本文件的核心。回落 127.0.0.1 = 连上无关 Redis 并正常启动。"""
    with pytest.raises(redisx.RedisEndpointMissingError):
        redisx.new_universal_client(_conf())


def test_endpoints_prefers_addrs_over_host_like_go() -> None:
    """Go: `addrs := c.Addrs; if len(addrs)==0 { addrs = []string{c.Host} }`。"""
    assert _conf(host="h:6379").endpoints() == ["h:6379"]
    assert _conf(addrs=["a:1", "b:2"]).endpoints() == ["a:1", "b:2"]
    # 两者都填时以 addrs 为准（与 Go 同）——写反会让 Cluster 配置退化成单实例
    assert _conf(host="h:6379", addrs=["a:1", "b:2"]).endpoints() == ["a:1", "b:2"]
    assert _conf().endpoints() == []


# ── 拓扑选型逐格对齐 go-redis ────────────────────────────────────────────

def test_single_host_builds_standalone() -> None:
    c = redisx.new_universal_client(_conf(host="127.0.0.1:6379"))
    assert type(c).__name__ == "Redis"


def test_single_element_addrs_is_still_standalone() -> None:
    """★ go-redis 按**原始地址数量**选型：addrs 只有 1 个仍是 standalone。

    判成 Cluster 的话客户端会去发 CLUSTER SLOTS，对普通 Redis 直接报错 —— 那是响的；
    反过来把多节点判成单实例才是静默的（只连第一个节点，其余分片的 key 全 MOVED）。
    """
    c = redisx.new_universal_client(_conf(addrs=["127.0.0.1:6379"]))
    assert type(c).__name__ == "Redis"


def test_multiple_addrs_build_cluster() -> None:
    c = redisx.new_universal_client(_conf(addrs=["a:7000", "b:7000", "c:7000"]))
    assert type(c).__name__ == "RedisCluster"


def test_master_name_wins_over_address_count() -> None:
    """master_name 非空 = Sentinel，与地址个数无关（go-redis 的判定顺序）。"""
    c = redisx.new_universal_client(_conf(addrs=["s1:26379"], master_name="mymaster"))
    pool = type(c.connection_pool).__name__
    assert "Sentinel" in pool, f"没走 Sentinel 故障转移池，实际是 {pool}"

    c2 = redisx.new_universal_client(
        _conf(addrs=["s1:26379", "s2:26379", "s3:26379"], master_name="mymaster")
    )
    assert "Sentinel" in type(c2.connection_pool).__name__, "多哨兵被误判成 Cluster"


def test_cluster_with_nonzero_db_is_refused() -> None:
    """Redis Cluster 只有 db0。静默忽略会让所有 key 落 db0 而运维以为隔离了。"""
    with pytest.raises(ValueError, match="db0|不支持"):
        redisx.new_universal_client(_conf(addrs=["a:7000", "b:7000"], db=3))


# ── 地址解析 ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    ("addr", "want"),
    [
        ("127.0.0.1:6379", ("127.0.0.1", 6379)),
        ("redis.svc", ("redis.svc", 6379)),          # 省略端口用默认
        ("[::1]:6380", ("::1", 6380)),               # IPv6 字面量
        ("[fe80::1]", ("fe80::1", 6379)),
    ],
)
def test_split_host_port(addr: str, want: tuple[str, int]) -> None:
    assert redisx._split_host_port(addr) == want  # noqa: SLF001


# ── 启动期 Ping 闸 ───────────────────────────────────────────────────────

async def test_must_connect_raises_when_authority_is_down(monkeypatch) -> None:
    """★ 不探这一下，服务会带着死 Redis 正常 Ready，第一条业务命令才暴露。"""

    class _Dead:
        async def ping(self):
            raise ConnectionError("connection refused")

        async def aclose(self):
            pass

    monkeypatch.setattr(redisx, "new_universal_client", lambda *a, **k: _Dead())
    with pytest.raises(ConnectionError, match="redis ping failed"):
        await redisx.must_connect(_conf(host="127.0.0.1:1"), ping_timeout_sec=0.5)


async def test_must_connect_closes_the_client_it_failed_to_use(monkeypatch) -> None:
    """探测失败要把半开的客户端关掉，否则每次重启都泄漏一组连接。"""
    closed = {"n": 0}

    class _Dead:
        async def ping(self):
            raise ConnectionError("nope")

        async def aclose(self):
            closed["n"] += 1

    monkeypatch.setattr(redisx, "new_universal_client", lambda *a, **k: _Dead())
    with pytest.raises(ConnectionError):
        await redisx.must_connect(_conf(host="x:1"))
    assert closed["n"] == 1


async def test_must_connect_returns_the_client_on_success(monkeypatch) -> None:
    class _Live:
        async def ping(self):
            return True

    sentinel = _Live()
    monkeypatch.setattr(redisx, "new_universal_client", lambda *a, **k: sentinel)
    assert await redisx.must_connect(_conf(host="x:1")) is sentinel


# ── 配置建模本身 ─────────────────────────────────────────────────────────

def test_redis_client_is_typed_not_swallowed_into_extra() -> None:
    """★ 落进 extra 的话 addrs/master_name 永远读不到，而且没有任何信号。"""
    node = config.NodeConf.model_validate(
        {"node_id": 1, "redis_client": {"addrs": ["a:1", "b:2"], "master_name": "m",
                                        "pool_size": 32, "read_timeout": "2s"}}
    )
    assert isinstance(node.redis_client, config.RedisConf)
    assert node.redis_client.addrs == ["a:1", "b:2"]
    assert node.redis_client.master_name == "m"
    assert node.redis_client.pool_size == 32
    assert node.redis_client.read_timeout_td().total_seconds() == 2.0


def test_redis_conf_models_every_field_go_has(repo_root) -> None:
    """★ 机械核对：Go `pkg/config.RedisConf` 的每个字段 Python 都要有对应建模。

    漏一个的后果不是报错，是**那个字段配了不生效且零信号** —— 与 addrs/master_name
    这次的形状完全相同。所以不靠"记得同步"，靠一条会红的检查。

    Go 侧新增字段时这条会红：要么建模，要么在下面的 `_KNOWN_UNUSED` 里显式登记
    并写明为什么 Python 不需要它。
    """
    import re

    text = (repo_root / "pkg" / "config" / "config.go").read_text(encoding="utf-8")
    block = re.search(r"type RedisConf struct \{(.*?)\n\}", text, re.DOTALL)
    assert block, "Go 侧 RedisConf 结构变了，本测试要跟着改"
    go_fields = set(re.findall(r'yaml:"([a-z_0-9]+)', block.group(1)))
    assert go_fields, "没解析到 yaml tag"

    py_fields = set(config.RedisConf.model_fields.keys())
    missing = go_fields - py_fields - _KNOWN_UNUSED
    assert not missing, (
        f"Go 的 RedisConf 有这些字段而 Python 没建模：{sorted(missing)}。"
        "配了不会报错，只会**不生效**——把它们加进 RedisConf，或登记进 _KNOWN_UNUSED 并写明理由。"
    )


# Go 有、Python 刻意不实现的字段（登记即豁免，但必须写明为什么）。
#
# 注意用 `set()` 而不是 `{}` —— 后者是**空 dict**，`set - dict` 会当场 TypeError。
# 现在是空的：Go 的字段已全部建模，`maint_notifications` 虽然 redis-py 没有对应能力，
# 但仍建了模（读到非空值不做任何事），这样它就不会落进 extra 被静默吞掉。
_KNOWN_UNUSED: set[str] = set()
