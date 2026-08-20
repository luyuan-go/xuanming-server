"""ds_allocator **Pod UID 发布预检 / Redis 只读安全体检**回归测试。

覆盖 `pandorapy/services/ds_allocator/poduidpreflight.py`(移植自 Go 侧
`services/battle/ds_allocator/internal/poduidpreflight/` 四个文件)。

## 这份测试为什么不用真 Redis

本模块测的是**闸的方向**:该拒的拒了、该放行的放行了。这些判定的输入是 Redis 的
**回包字节**,而恰恰是"回包不规范 / 权限被放宽 / 拓扑在观测期间变了"这些形态,
真 Redis **造不出来**(要造得改 ACL、拔网线、触发 failover)。所以这里用脚本化替身
精确投喂每一种回包 —— 与 Go 侧 `scriptedRedisCommander` 的做法一致。

真 Redis 的联调另有其位:Go 侧 `redis_security_test.go` 的 Redis 8 集成用例
(`deploy/docker-compose.ci-db.yml` 里那套 ACL)。Python 侧不重复造那套环境,
但**摘要必须与 Go 逐字节相同** —— 见 `test_digest_vectors_match_go`,
里面的期望值是从真实 Go 实现里跑出来的(见该用例注释)。

## 每条用例都做了变异验证

做法:把被测的那一行改坏 → 确认本文件变红 → 还原。探针**不留在产品代码里**
(`tests/test_no_mutation_probe_residue.py` 是机械闸)。
"""

from __future__ import annotations

import hashlib
import uuid as _uuid

import pytest
from pandora.ds.v1 import allocator_pb2 as dspb

from pandorapy import config as pconfig
from pandorapy.services.ds_allocator import poduidpreflight as pre

# ── 替身 ────────────────────────────────────────────────────────────────────

RUN_ID = "0123456789abcdef0123456789abcdef01234567"
NODE_A = "a" * 40
NODE_B = "b" * 40
NODE_C = "c" * 40

# Go 侧 `canonicalACLGetUser()` 的**实际回包**(2026-08-20 从 Go 测试里打印出来的,
# 见文件头)。RESP2 形态:交替的 key/value 数组。
CANONICAL_ACL_COMMANDS_TEXT = (
    "-@all +ping +get +scan +info +acl|whoami +acl|dryrun +acl|getuser "
    "+cluster|myid +cluster|shards +cluster|slots +cluster|info +cluster|nodes"
)


def canonical_acl_get_user() -> list:
    return [
        "flags",
        ["on", "sanitize-payload"],
        "passwords",
        ["a" * 64],
        "commands",
        CANONICAL_ACL_COMMANDS_TEXT,
        "keys",
        "%R~pandora:ds:battle:*",
        "channels",
        "",
        "selectors",
        [],
    ]


class ScriptedNode:
    """脚本化的 Redis 替身 —— 对应 Go 的 `scriptedRedisCommander`。

    默认状态 = **完全规范**的专用只读身份;每个用例只改坏一处,验证对应的闸变红。
    刻意**不提供** `keys()` 方法:产品代码一旦改用 `KEYS` 会直接 AttributeError,
    而 `KEYS` 会阻塞整个 Redis(硬性要求 8)。
    """

    def __init__(self) -> None:
        self.whoami = pre.CANONICAL_READ_ONLY_USERNAME
        self.get_user: object = canonical_acl_get_user()
        self.get_user_failure: BaseException | None = None
        self.allowed_forbidden: set[str] = set()
        self.allow_outside_get = False
        self.dry_run_failure: BaseException | None = None
        self.info_server = f"# Server\r\nredis_version:8.0.0\r\nrun_id:{RUN_ID}\r\n"
        self.info_cluster = "# Cluster\r\ncluster_enabled:0\r\n"
        self.info_replication = "# Replication\r\nrole:master\r\n"
        self.calls: list[tuple] = []

    async def execute_command(self, *args):  # noqa: ANN001, ANN201
        self.calls.append(tuple(args))
        head = tuple(str(a) for a in args[:2])
        if head == ("ACL", "WHOAMI"):
            return self.whoami.encode()
        if head == ("ACL", "GETUSER"):
            if self.get_user_failure is not None:
                raise self.get_user_failure
            return self.get_user
        if head == ("ACL", "DRYRUN"):
            if self.dry_run_failure is not None:
                raise self.dry_run_failure
            name = str(args[3]).lower()
            key = str(args[4]) if len(args) > 4 else ""
            if name in self.allowed_forbidden:
                return b"OK"
            if name == "get":
                if key.startswith("pandora:ds:battle:"):
                    return b"OK"
                if self.allow_outside_get:
                    return b"OK"
                return (
                    b"This user has no permissions to access "
                    b"one of the keys used as arguments"
                )
            if name == "scan":
                return b"OK"
            return f"This user has no permissions to run the '{name}' command".encode()
        if args[0] == "INFO server":
            return self.info_server.encode()
        if args[0] == "INFO cluster":
            return self.info_cluster.encode()
        if args[0] == "INFO replication":
            return self.info_replication.encode()
        raise AssertionError(f"unscripted command: {args}")


class ClusterScriptedNode(ScriptedNode):
    """再加上 CLUSTER MYID / INFO / NODES / SLOTS 四条集群取证命令。

    `topology_mutator` 用来模拟"两次观测之间拓扑变了"。
    """

    def __init__(self) -> None:
        super().__init__()
        self.my_id = NODE_A
        self.cluster_info = (
            "cluster_state:ok\r\n"
            "cluster_slots_assigned:16384\r\n"
            "cluster_slots_ok:16384\r\n"
            "cluster_slots_pfail:0\r\n"
            "cluster_slots_fail:0\r\n"
            "cluster_known_nodes:3\r\n"
            "cluster_size:2\r\n"
            "cluster_current_epoch:7\r\n"
        )
        self.cluster_nodes = canonical_cluster_nodes_body()
        self.cluster_slots = canonical_cluster_slots_reply()
        self.observations = 0
        self.topology_mutator = None

    async def execute_command(self, *args):  # noqa: ANN001, ANN201
        head = tuple(str(a) for a in args[:2])
        if head == ("CLUSTER", "MYID"):
            self.calls.append(tuple(args))
            return self.my_id.encode()
        if head == ("CLUSTER", "INFO"):
            self.calls.append(tuple(args))
            self.observations += 1
            if self.topology_mutator is not None and self.observations == 2:
                self.topology_mutator(self)
            return self.cluster_info.encode()
        if head == ("CLUSTER", "NODES"):
            self.calls.append(tuple(args))
            return self.cluster_nodes.encode()
        if head == ("CLUSTER", "SLOTS"):
            self.calls.append(tuple(args))
            return self.cluster_slots
        return await super().execute_command(*args)


def canonical_cluster_nodes_body() -> str:
    return "\n".join(
        [
            NODE_A + " 10.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-8191",
            NODE_B + " 10.0.0.2:6379@16379 master - 0 0 2 connected 8192-16383",
            NODE_C + " 10.0.0.3:6379@16379 slave " + NODE_A + " 0 0 1 connected",
        ]
    )


def canonical_cluster_slots_reply() -> list:
    """`CLUSTER SLOTS` 的原始嵌套数组(redis-py 不解析,由本模块自己拆)。"""
    return [
        [0, 8191, [b"10.0.0.1", 6379, NODE_A.encode(), []]],
        [8192, 16383, [b"10.0.0.2", 6379, NODE_B.encode(), []]],
    ]


class ScanNode(ScriptedNode):
    """带 SCAN / GET 的替身。`pages` 是 `[(next_cursor, [key...]), ...]`。"""

    def __init__(self, pages: list[tuple[int, list[str]]], values: dict[str, bytes]) -> None:
        super().__init__()
        self.pages = pages
        self.values = values
        self.scan_calls: list[tuple[int, str, int]] = []
        self.get_calls: list[str] = []

    async def scan(self, cursor: int, match: str, count: int):  # noqa: ANN201
        self.scan_calls.append((cursor, match, count))
        index = 0
        if cursor != 0:
            index = [page[0] for page in self.pages].index(cursor) + 1
        next_cursor, keys = self.pages[index]
        return next_cursor, [key.encode() for key in keys]

    async def get(self, key: str):  # noqa: ANN201
        self.get_calls.append(key)
        return self.values.get(key)


class FakeUnauthenticated:
    """`_unauthenticated_clone` 的替身:模拟"空凭据连接"的三种结局。"""

    def __init__(self, exc: BaseException | None) -> None:
        self.exc = exc
        self.closed = False

    async def ping(self) -> bool:
        if self.exc is not None:
            raise self.exc
        return True

    async def aclose(self) -> None:
        self.closed = True


def battle_record(**kwargs) -> dspb.BattleStorageRecord:  # noqa: ANN003
    rec = dspb.BattleStorageRecord()
    rec.match_id = kwargs.pop("match_id", 12345)
    rec.state = kwargs.pop("state", "ready")
    rec.allocation_id = kwargs.pop("allocation_id", str(_uuid.uuid4()))
    for key, value in kwargs.items():
        setattr(rec, key, value)
    return rec


def exact_record(**kwargs) -> dspb.BattleStorageRecord:  # noqa: ANN003
    defaults = {
        "ds_pod_name": "battle-stable-abcde",
        "gameserver_uid": "11111111-2222-3333-4444-555555555555",
        "pod_uid": "66666666-7777-8888-9999-000000000000",
        "release_track": "stable",
    }
    defaults.update(kwargs)
    return battle_record(**defaults)


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        chunk = value & 0x7F
        value >>= 7
        if value:
            out.append(chunk | 0x80)
        else:
            out.append(chunk)
            return bytes(out)


def with_unknown_field(rec: dspb.BattleStorageRecord) -> dspb.BattleStorageRecord:
    """给记录挂一个本二进制不认识的字段(模拟更新的写者)。"""
    payload = rec.SerializeToString() + _varint((9999 << 3) | 0) + _varint(1)
    out = dspb.BattleStorageRecord()
    out.ParseFromString(payload)
    return out


# ── ① 跨语言摘要向量 ─────────────────────────────────────────────────────────


def test_digest_vectors_match_go():
    """摘要必须与 Go **逐字节相同**。

    期望值来源:2026-08-20 在 `services/battle/ds_allocator/internal/poduidpreflight`
    里临时加过一个探针 test,直接调用 Go 的 `IdentifyRedisConfig` /
    `runtimeMasterSetDigest` / `lengthPrefixedSHA256` / `safeRedisSource` /
    `digestStrings` / `parseClusterNodesOwnership` / `parseClusterInfoFence` 打印
    真实取值,抄进来后**探针文件已删除**(Go 包目录已核对恢复原状)。

    为什么值得钉死:目标身份摘要是激活证据的锚点 —— 激活脚本用
    `-expected-config-identity` 把它与 Go 侧算出的值比对。两栈算不一样时,
    表现不是"哪里报错",而是**永远对不上**,而排查的人会去怀疑 Redis 配置。
    """
    identity = pre.identify_redis_config(
        pconfig.RedisConf(addrs=["redis-b:6379", "redis-a:6379"])
    )
    assert identity.topology == "cluster"
    assert identity.digest == (
        "sha256:33a115857f38e940bfa4a9cf7c05ac305baddc7c680b7f4675cf3d49e35fffdc"
    )
    assert pre.valid_target_identity(identity.digest)

    assert pre.runtime_master_set_digest([NODE_A]) == (
        "sha256:07654d1acb887256dfb938b162aedd65463af48bef2490e99d5fe370d8c9fddf"
    )
    # 长度前缀的存在证明:["ab","c"] 与 ["a","bc"] 必须**不同**。
    assert pre.length_prefixed_sha256(["ab", "c"]).hex() == (
        "601d5476e2ccfe2c87a2bba7a322659734a05749d5b5aa781f513e4912db0d5f"
    )
    assert pre.length_prefixed_sha256(["a", "bc"]).hex() == (
        "3fafa1cf2f19a7c1129beb20cf0983f73a489a221fc0dd2f16d1be292d089205"
    )
    assert pre.safe_redis_source("redis-cluster-master", "10.0.0.1:6379") == (
        "redis-cluster-master-da8f53415961"
    )
    assert pre.digest_strings("pod-uid-preflight-standalone-topology-v1", [NODE_B]) == (
        "sha256:207f98480e489647480757926337506d54d1e5fe96356afa2db0e77db0eab620"
    )
    view = pre.parse_cluster_nodes_ownership(canonical_cluster_nodes_body())
    assert view.digest == (
        "sha256:548fd17a3599a3fc8abded7c7c111464233afc997927accbf3a6b63242d2af28"
    )
    assert view.node_count == 3
    assert view.self_id == NODE_A
    assert list(view.master_ids) == [NODE_A, NODE_B]
    fence = pre.parse_cluster_info_fence(
        "cluster_state:ok\ncluster_slots_assigned:16384\ncluster_slots_ok:16384\n"
        "cluster_slots_pfail:0\ncluster_slots_fail:0\ncluster_known_nodes:3\n"
        "cluster_size:2\ncluster_current_epoch:7\n"
    )
    assert fence.digest == (
        "sha256:17d2e2b952dda352f746543c40b41450971db8893f964ed6edb04c9f1983e244"
    )


def test_safe_source_never_leaks_the_endpoint():
    """发现列表里的 source 必须是**摘要**,不能出现内网地址。"""
    source = pre.safe_redis_source("redis-cluster-master", " 10.0.0.7:6379 ")
    assert "10.0.0.7" not in source
    # 大小写 / 前后空白归一后是同一个标签(同一台机器的发现要能聚合)。
    assert source == pre.safe_redis_source("redis-cluster-master", "10.0.0.7:6379")
    assert pre.safe_redis_source("redis-primary", "") == "redis-primary"


# ── ② ACL 只读契约(两个方向)─────────────────────────────────────────────


async def test_prove_read_only_acl_accepts_the_exact_contract():
    """规范身份必须**放行**,且只查自己的 GETUSER(不读别人的口令哈希)。"""
    node = ScriptedNode()
    await pre.prove_read_only_acl(node, pre.CANONICAL_READ_ONLY_USERNAME)
    get_user_calls = [c for c in node.calls if c[:2] == ("ACL", "GETUSER")]
    assert len(get_user_calls) == 1
    assert get_user_calls[0][2] == pre.CANONICAL_READ_ONLY_USERNAME
    assert not [c for c in node.calls if str(c[0]).upper() == "COMMAND"]


def _mutate_get_user(node: ScriptedNode, index: int, value: object) -> None:
    fields = list(node.get_user)
    fields[index] = value
    node.get_user = fields


ACL_DRIFTS = {
    # (mutate, 期望错误里必须出现的片段)—— 与 Go 的
    # TestProveReadOnlyACLRejectsEveryCanonicalFieldDrift 表逐条对应。
    "wrong whoami": (lambda n: setattr(n, "whoami", "default"), "WHOAMI"),
    "off flag": (lambda n: _mutate_get_user(n, 1, ["off", "sanitize-payload"]), "flags"),
    "missing sanitize flag": (lambda n: _mutate_get_user(n, 1, ["on"]), "flags"),
    "multiple passwords": (
        lambda n: _mutate_get_user(n, 3, ["a" * 64, "b" * 64]),
        "exactly one",
    ),
    "malformed password hash": (
        lambda n: _mutate_get_user(n, 3, ["not-a-canonical-hash"]),
        "password metadata",
    ),
    "extra command": (
        lambda n: _mutate_get_user(n, 5, CANONICAL_ACL_COMMANDS_TEXT + " +exists"),
        "allowlist",
    ),
    "noncanonical command spacing": (
        lambda n: _mutate_get_user(
            n, 5, CANONICAL_ACL_COMMANDS_TEXT.replace(" +ping", "  +ping", 1)
        ),
        "commands are malformed",
    ),
    "category grant": (
        lambda n: _mutate_get_user(
            n, 5, CANONICAL_ACL_COMMANDS_TEXT.replace("+get", "+@read", 1)
        ),
        "allowlist",
    ),
    "writable key rule": (
        lambda n: _mutate_get_user(n, 7, "~" + pre.BATTLE_SCAN_PATTERN),
        "key rules",
    ),
    "channel rule": (lambda n: _mutate_get_user(n, 9, "&*"), "channel rules"),
    "selector": (lambda n: _mutate_get_user(n, 11, [["commands", "+get"]]), "selectors"),
    "semantic write grant": (
        lambda n: n.allowed_forbidden.add("set"),
        "unexpectedly allowed",
    ),
    "outside namespace get": (
        lambda n: setattr(n, "allow_outside_get", True),
        "out-of-namespace",
    ),
    "dryrun unavailable": (
        lambda n: setattr(n, "dry_run_failure", RuntimeError("ERR unknown subcommand 'DRYRUN'")),
        "must be allowed",
    ),
}


@pytest.mark.parametrize("name", sorted(ACL_DRIFTS))
async def test_prove_read_only_acl_rejects_every_canonical_field_drift(name: str):
    """契约的**每一个字段**漂移都必须被拒 —— 少拒一条就是一个权限洞。"""
    mutate, want = ACL_DRIFTS[name]
    node = ScriptedNode()
    mutate(node)
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.prove_read_only_acl(node, pre.CANONICAL_READ_ONLY_USERNAME)
    assert want in excinfo.value.msg


async def test_prove_read_only_acl_requires_the_canonical_username_string():
    """连"名字对但 GETUSER 不可用"也必须失败(证明不了 = 失败)。"""
    node = ScriptedNode()
    node.get_user_failure = RuntimeError("NOPERM")
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.prove_read_only_acl(node, pre.CANONICAL_READ_ONLY_USERNAME)
    assert "ACL GETUSER is unavailable" in excinfo.value.msg


def test_parse_acl_get_user_supports_resp2_and_resp3_without_returning_hashes():
    """RESP2(交替数组)与 RESP3(map)必须解析成**同一个**快照,且不留口令哈希。"""
    resp2 = canonical_acl_get_user()
    want = pre.parse_acl_get_user(resp2)
    resp3 = {resp2[i]: resp2[i + 1] for i in range(0, len(resp2), 2)}
    assert pre.parse_acl_get_user(resp3) == want
    assert want.password_count == 1
    assert "a" * 64 not in repr(want)
    # redis-py 默认返回 bytes;两种编码必须等价。
    resp2_bytes = [
        item.encode() if isinstance(item, str) else item for item in canonical_acl_get_user()
    ]
    resp2_bytes[1] = [b"on", b"sanitize-payload"]
    resp2_bytes[3] = [b"a" * 64]
    assert pre.parse_acl_get_user(resp2_bytes) == want


@pytest.mark.parametrize(
    ("value", "want"),
    [
        ([], "missing flags field"),
        (["flags", ["on"]], "missing passwords field"),
        (["flags"], "odd field array"),
        ("not-a-list", "unexpected response shape"),
    ],
)
def test_parse_acl_get_user_rejects_malformed_shapes(value: object, want: str):
    """缺字段 = 拒。"没检查到"和"检查通过"在结构上不可区分。"""
    with pytest.raises(pre.PreflightError) as excinfo:
        pre.parse_acl_get_user(value)
    assert want in excinfo.value.msg


def test_parse_acl_get_user_rejects_extra_field():
    """六个字段之外多一个也拒(Go 的 `len(fields) != 6`)。"""
    fields = canonical_acl_get_user() + ["future", "x"]
    with pytest.raises(pre.PreflightError) as excinfo:
        pre.parse_acl_get_user(fields)
    assert "unexpected field set" in excinfo.value.msg


def test_canonical_acl_token_set_rejects_duplicates_and_case():
    with pytest.raises(pre.PreflightError):
        pre.canonical_acl_token_set(["+get", "+get"])
    with pytest.raises(pre.PreflightError):
        pre.canonical_acl_token_set(["+GET"])
    with pytest.raises(pre.PreflightError):
        pre.canonical_acl_token_set([])
    assert pre.canonical_acl_token_set(["+scan", "+get"]) == ["+get", "+scan"]


def test_acl_key_rule_must_be_read_only_prefix():
    """`%R~` 是**只读** key 前缀;写成 `~` 就是读写身份 —— 一个字符的差别。"""
    snapshot = pre.parse_acl_get_user(canonical_acl_get_user())
    pre.validate_canonical_read_only_acl(snapshot)
    writable = dict_replace(snapshot, keys="~" + pre.BATTLE_SCAN_PATTERN)
    with pytest.raises(pre.PreflightError):
        pre.validate_canonical_read_only_acl(writable)


def dict_replace(snapshot: pre.ACLUserSnapshot, **kwargs) -> pre.ACLUserSnapshot:  # noqa: ANN003
    import dataclasses

    return dataclasses.replace(snapshot, **kwargs)


# ── ③ DRYRUN 三种方向 ───────────────────────────────────────────────────────


async def test_dry_run_helpers_directions():
    """三个 helper 的方向必须**互不冒充**:

      allowed     → 必须明确 OK,否则失败;
      denied      → 必须是点名**该命令**的权限拒绝;
      key_denied  → 必须是点名 **key** 的权限拒绝。
    """
    node = ScriptedNode()
    await pre.require_acl_dry_run_allowed(
        node, pre.CANONICAL_READ_ONLY_USERNAME, ["GET", "pandora:ds:battle:{1}"]
    )
    await pre.require_acl_dry_run_denied(
        node, pre.CANONICAL_READ_ONLY_USERNAME, ["SET", "pandora:ds:battle:{1}", "x"]
    )
    await pre.require_acl_dry_run_key_denied(
        node, pre.CANONICAL_READ_ONLY_USERNAME, ["GET", "pandora:outside-preflight-trust-domain"]
    )

    # 「命令整个被禁」不得冒充「key 越界被拒」:前者会让审计一条记录都读不到。
    node.allow_outside_get = False
    command_denied = ScriptedNode()
    command_denied.allowed_forbidden = set()

    class _CommandOnlyDenial(ScriptedNode):
        async def execute_command(self, *args):  # noqa: ANN001, ANN201
            if tuple(str(a) for a in args[:2]) == ("ACL", "DRYRUN"):
                return b"This user has no permissions to run the 'get' command"
            return await super().execute_command(*args)

    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.require_acl_dry_run_key_denied(
            _CommandOnlyDenial(),
            pre.CANONICAL_READ_ONLY_USERNAME,
            ["GET", "pandora:outside-preflight-trust-domain"],
        )
    assert "explicit key permission denial" in excinfo.value.msg


async def test_dry_run_denied_requires_explicit_denial_not_just_absence_of_ok():
    """含糊的失败(超时 / 连接错)**不算**拒绝证明 —— 拿不到明确拒绝就是失败。"""

    class _Vague(ScriptedNode):
        async def execute_command(self, *args):  # noqa: ANN001, ANN201
            if tuple(str(a) for a in args[:2]) == ("ACL", "DRYRUN"):
                raise RuntimeError("connection reset by peer")
            return await super().execute_command(*args)

    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.require_acl_dry_run_denied(
            _Vague(), pre.CANONICAL_READ_ONLY_USERNAME, ["SET", "k", "v"]
        )
    assert "did not return an explicit command permission denial" in excinfo.value.msg


# ── ④ 口令强制(两个方向)───────────────────────────────────────────────────


async def test_prove_password_required_directions(monkeypatch):
    """空凭据 PING:被规范拒绝 → 放行;成功 → 拒;含糊失败 → 拒。"""
    clones: list[FakeUnauthenticated] = []

    def make(exc: BaseException | None):
        def factory(node):  # noqa: ANN001
            clone = FakeUnauthenticated(exc)
            clones.append(clone)
            return clone

        return factory

    monkeypatch.setattr(
        pre, "_unauthenticated_clone", make(RuntimeError("NOAUTH Authentication required."))
    )
    await pre.prove_password_required(object())
    assert clones[-1].closed is True

    monkeypatch.setattr(pre, "_unauthenticated_clone", make(None))
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.prove_password_required(object())
    assert "accepted unauthenticated PING" in excinfo.value.msg
    assert clones[-1].closed is True

    monkeypatch.setattr(
        pre, "_unauthenticated_clone", make(RuntimeError("connection refused"))
    )
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.prove_password_required(object())
    assert "canonical authentication denial" in excinfo.value.msg


# ── ⑤ INFO / 拓扑闸(两个方向)──────────────────────────────────────────────


def test_parse_redis_info_fields_rejects_injection_shapes():
    """宽松解析会让"注入一行 role:master"把从库伪装成主库,所以三条判据都要留。"""
    assert pre.parse_redis_info_fields("# c\r\nrole:master\r\n") == {"role": "master"}
    for body in (" role:master", "role :master", "role:master\nrole:slave"):
        with pytest.raises(pre.PreflightError):
            pre.parse_redis_info_fields(body)


def test_server_primary_and_cluster_disabled_directions():
    pre.parse_server_primary("role:master\n")
    pre.parse_server_cluster_disabled("cluster_enabled:0\n")
    for body in ("role:slave\n", "", "connected_slaves:0\n"):
        with pytest.raises(pre.PreflightError) as excinfo:
            pre.parse_server_primary(body)
        assert "role=master" in excinfo.value.msg
    for body in ("cluster_enabled:1\n", ""):
        with pytest.raises(pre.PreflightError) as excinfo:
            pre.parse_server_cluster_disabled(body)
        assert "cluster_enabled=0" in excinfo.value.msg


async def test_standalone_runtime_id_directions():
    node = ScriptedNode()
    assert await pre.standalone_runtime_id(node) == RUN_ID
    node.info_server = "# Server\nredis_version:8.0.0\n"
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.standalone_runtime_id(node)
    assert "omitted run_id" in excinfo.value.msg
    node.info_server = "run_id:not-canonical\n"
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.standalone_runtime_id(node)
    assert "non-canonical run_id" in excinfo.value.msg


def test_parse_strict_uint_field_boundaries():
    """Go 由 `ParseUint(...,10,64)` 免费拿到的边界,Python 必须显式判。"""
    assert pre.parse_strict_uint_field({"x": "18446744073709551615"}, "x") == pre.UINT64_MAX
    for value in ("18446744073709551616", "007", "+7", "7\n", "-1", "", "0x7"):
        with pytest.raises(pre.PreflightError):
            pre.parse_strict_uint_field({"x": value}, "x")
    with pytest.raises(pre.PreflightError):
        pre.parse_strict_positive_int_field({"x": "0"}, "x")
    with pytest.raises(pre.PreflightError):
        pre.parse_strict_positive_int_field({"x": str(pre.INT64_MAX + 1)}, "x")


CLUSTER_NODES_DRIFTS = {
    "importing slot": (
        canonical_cluster_nodes_body() + " [0-<-" + NODE_B + "]",
        "importing or migrating slot",
    ),
    "unassigned slot": (
        canonical_cluster_nodes_body().replace("8192-16383", "8192-16382"),
        "unassigned",
    ),
    "duplicate node": (
        canonical_cluster_nodes_body().replace(NODE_B, NODE_A),
        "duplicate node ID",
    ),
    "unknown flag": (
        canonical_cluster_nodes_body().replace("myself,master", "myself,master,future"),
        "unknown node flag",
    ),
    "unsafe flag": (
        canonical_cluster_nodes_body().replace("myself,master", "myself,master,fail"),
        "unsafe node flag",
    ),
    "disconnected": (
        canonical_cluster_nodes_body().replace("connected 0-8191", "disconnected 0-8191"),
        "disconnected node",
    ),
    "replica owns slots": (
        canonical_cluster_nodes_body() + " 100-200",
        "replica unexpectedly owns slots",
    ),
    "zero slot master": (
        "\n".join(
            [
                NODE_A + " 10.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-16383",
                NODE_B + " 10.0.0.2:6379@16379 master - 0 0 2 connected",
            ]
        ),
        "zero-slot master",
    ),
    "ambiguous role": (
        canonical_cluster_nodes_body().replace("myself,master", "myself,master,slave"),
        "ambiguous node role",
    ),
    "master with parent": (
        canonical_cluster_nodes_body().replace(
            "myself,master - 0", "myself,master " + NODE_B + " 0"
        ),
        "master has a parent identity",
    ),
    "short record": ("short record here\n" + canonical_cluster_nodes_body(), "short record"),
    "uppercase node id": (
        canonical_cluster_nodes_body().replace(NODE_A, NODE_A.upper(), 1),
        "non-canonical node ID",
    ),
    "invalid config epoch": (
        canonical_cluster_nodes_body().replace("0 0 1 connected 0-8191", "0 0 01 connected 0-8191"),
        "invalid config epoch",
    ),
}


@pytest.mark.parametrize("name", sorted(CLUSTER_NODES_DRIFTS))
def test_parse_cluster_nodes_rejects_every_unsafe_shape(name: str):
    body, want = CLUSTER_NODES_DRIFTS[name]
    with pytest.raises(pre.PreflightError) as excinfo:
        pre.parse_cluster_nodes_ownership(body)
    assert want in excinfo.value.msg


def test_parse_cluster_slots_requires_exact_contiguous_cover():
    """0..16383 必须被**精确连续**覆盖,留洞 / 重叠 / 非规范 ID 一律拒。"""
    slots = pre._parse_cluster_slots_reply(canonical_cluster_slots_reply())
    view = pre.parse_cluster_slots_ownership(slots)
    assert list(view.master_ids) == [NODE_A, NODE_B]

    gap = [[0, 8191, [b"10.0.0.1", 6379, NODE_A.encode(), []]]]
    with pytest.raises(pre.PreflightError) as excinfo:
        pre.parse_cluster_slots_ownership(pre._parse_cluster_slots_reply(gap))
    assert "does not cover all 16384 slots" in excinfo.value.msg

    overlap = [
        [0, 8191, [b"10.0.0.1", 6379, NODE_A.encode(), []]],
        [8000, 16383, [b"10.0.0.2", 6379, NODE_B.encode(), []]],
    ]
    with pytest.raises(pre.PreflightError) as excinfo:
        pre.parse_cluster_slots_ownership(pre._parse_cluster_slots_reply(overlap))
    assert "contiguous" in excinfo.value.msg

    bad_id = [[0, 16383, [b"10.0.0.1", 6379, b"NOT-HEX", []]]]
    with pytest.raises(pre.PreflightError) as excinfo:
        pre.parse_cluster_slots_ownership(pre._parse_cluster_slots_reply(bad_id))
    assert "non-canonical master ID" in excinfo.value.msg

    with pytest.raises(pre.PreflightError):
        pre.parse_cluster_slots_ownership([])
    # Python 独有的一层原始回包解析:形状不对必须 fail-closed,不能当空表放行。
    for malformed in ("not-a-list", [[0]], [[0, 1, "not-a-node"]]):
        with pytest.raises(pre.PreflightError):
            pre._parse_cluster_slots_reply(malformed)


async def test_observe_cluster_topology_directions():
    """三方一致 → 通过;NODES 与 SLOTS 不一致 / 观测期间变化 → 拒。"""
    node = ClusterScriptedNode()
    snapshot = await pre.observe_stable_cluster_topology(node, NODE_A)
    assert snapshot.master_count == 2
    assert snapshot.master_set_digest == pre.runtime_master_set_digest([NODE_A, NODE_B])

    disagree = ClusterScriptedNode()
    disagree.cluster_slots = [
        [0, 8191, [b"10.0.0.1", 6379, NODE_B.encode(), []]],
        [8192, 16383, [b"10.0.0.2", 6379, NODE_A.encode(), []]],
    ]
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.observe_cluster_topology(disagree, NODE_A)
    assert "disagree on exact slot ownership" in excinfo.value.msg

    wrong_self = ClusterScriptedNode()
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.observe_cluster_topology(wrong_self, NODE_B)
    assert "disagree on the connected master" in excinfo.value.msg

    size_mismatch = ClusterScriptedNode()
    size_mismatch.cluster_info = size_mismatch.cluster_info.replace(
        "cluster_size:2", "cluster_size:1"
    )
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.observe_cluster_topology(size_mismatch, NODE_A)
    assert "does not match CLUSTER NODES" in excinfo.value.msg

    unstable = ClusterScriptedNode()

    def mutate(target: ClusterScriptedNode) -> None:
        target.cluster_info = target.cluster_info.replace(
            "cluster_current_epoch:7", "cluster_current_epoch:8"
        )

    unstable.topology_mutator = mutate
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.observe_stable_cluster_topology(unstable, NODE_A)
    assert "topology changed during observation" in excinfo.value.msg


# ── ⑥ 端到端只读证明 ────────────────────────────────────────────────────────


async def test_prove_read_only_and_identify_standalone(monkeypatch):
    """standalone 全链通过,并产出稳定的目标身份摘要。"""
    monkeypatch.setattr(
        pre,
        "_unauthenticated_clone",
        lambda node: FakeUnauthenticated(RuntimeError("NOAUTH Authentication required.")),
    )
    node = ScriptedNode()
    rc = pconfig.RedisConf(host="redis.internal:6379")
    identity = await pre.prove_read_only_and_identify(
        node, rc, pre.CANONICAL_READ_ONLY_USERNAME
    )
    assert identity.topology == "standalone"
    assert identity.nodes == 1
    assert pre.valid_target_identity(identity.digest)
    assert identity.master_set_digest == pre.runtime_master_set_digest([RUN_ID])
    assert identity.topology_digest == pre.digest_strings(
        "pod-uid-preflight-standalone-topology-v1", [RUN_ID]
    )
    # 幂等:同一目标 + 同一运行时身份 → 同一摘要(激活证据要能重复比对)。
    again = await pre.prove_read_only_and_identify(
        node, rc, pre.CANONICAL_READ_ONLY_USERNAME
    )
    assert again == identity


async def test_prove_read_only_and_identify_rejects_wrong_username_and_shape(monkeypatch):
    """身份名不对 / 客户端形态与配置形态不符 → 拒。"""
    monkeypatch.setattr(
        pre,
        "_unauthenticated_clone",
        lambda node: FakeUnauthenticated(RuntimeError("NOAUTH Authentication required.")),
    )
    node = ScriptedNode()
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.prove_read_only_and_identify(
            node, pconfig.RedisConf(host="redis.internal:6379"), "default"
        )
    assert "canonical read-only username" in excinfo.value.msg

    # 配置算出 cluster,但客户端不是 cluster client。
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.prove_read_only_and_identify(
            node,
            pconfig.RedisConf(addrs=["redis-a:6379", "redis-b:6379"]),
            pre.CANONICAL_READ_ONLY_USERNAME,
        )
    assert "client is not a cluster client" in excinfo.value.msg

    # 反向:客户端是 cluster,但配置算出 standalone。
    class _ClusterClient:
        def get_primaries(self):  # noqa: ANN201
            return []

    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.prove_read_only_and_identify(
            _ClusterClient(),
            pconfig.RedisConf(host="redis-a:6379"),
            pre.CANONICAL_READ_ONLY_USERNAME,
        )
    assert "normalized config topology is standalone" in excinfo.value.msg


async def test_prove_read_only_and_identify_cluster_requires_full_master_cover(monkeypatch):
    """cluster 分支:各 master 必须**恰好**覆盖拓扑里的 master 集合。"""
    monkeypatch.setattr(
        pre,
        "_unauthenticated_clone",
        lambda node: FakeUnauthenticated(RuntimeError("NOAUTH Authentication required.")),
    )

    node_a = ClusterScriptedNode()
    node_b = ClusterScriptedNode()
    node_b.my_id = NODE_B
    node_b.cluster_nodes = canonical_cluster_nodes_body().replace(
        NODE_A + " 10.0.0.1:6379@16379 myself,master", NODE_A + " 10.0.0.1:6379@16379 master"
    ).replace(NODE_B + " 10.0.0.2:6379@16379 master", NODE_B + " 10.0.0.2:6379@16379 myself,master")

    class _Cluster:
        def __init__(self, nodes):  # noqa: ANN001
            self._nodes = nodes

        def get_primaries(self):  # noqa: ANN201
            return self._nodes

    rc = pconfig.RedisConf(addrs=["redis-a:6379", "redis-b:6379"])
    identity = await pre.prove_read_only_and_identify(
        _Cluster([node_a, node_b]), rc, pre.CANONICAL_READ_ONLY_USERNAME
    )
    assert identity.topology == "cluster"
    assert identity.nodes == 2
    assert identity.master_set_digest == pre.runtime_master_set_digest([NODE_A, NODE_B])

    # 只回调到一个 master:拓扑说有两个 → 必须拒(否则半个库没扫也算"扫完了")。
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.prove_read_only_and_identify(
            _Cluster([node_a]), rc, pre.CANONICAL_READ_ONLY_USERNAME
        )
    assert "do not exactly cover the topology master set" in excinfo.value.msg

    # 一个 master 都没回调到。
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.prove_read_only_and_identify(
            _Cluster([]), rc, pre.CANONICAL_READ_ONLY_USERNAME
        )
    assert "visited zero slot-owning masters" in excinfo.value.msg


# ── ⑦ 配置身份与 YAML 严格解析 ──────────────────────────────────────────────


UNSAFE_CONFIGS = {
    "duplicate": pconfig.RedisConf(addrs=["redis-a:6379", "redis-a:6379"]),
    "outer whitespace": pconfig.RedisConf(host=" redis-a:6379 "),
    "uppercase": pconfig.RedisConf(host="REDIS-A:6379"),
    "port leading zero": pconfig.RedisConf(host="redis-a:06379"),
    "noncanonical ipv4": pconfig.RedisConf(host="127.0.0.001:6379"),
    "numeric ipv4 shorthand": pconfig.RedisConf(host="2130706433:6379"),
    "sentinel whitespace": pconfig.RedisConf(host="redis-a:6379", master_name=" master "),
    "non-zero db": pconfig.RedisConf(host="redis-a:6379", db=1),
    "negative db": pconfig.RedisConf(host="redis-a:6379", db=-1),
    "no endpoint": pconfig.RedisConf(),
    "missing port": pconfig.RedisConf(host="redis-a"),
    "zero port": pconfig.RedisConf(host="redis-a:0"),
    "port overflow": pconfig.RedisConf(host="redis-a:65536"),
    "bare ipv6": pconfig.RedisConf(host="::1:6379"),
    "trailing dot label": pconfig.RedisConf(host="redis-a.:6379"),
    "leading dash label": pconfig.RedisConf(host="-redis:6379"),
}


@pytest.mark.parametrize("name", sorted(UNSAFE_CONFIGS))
def test_identify_redis_config_rejects_unsafe_targets(name: str):
    with pytest.raises(pre.PreflightError):
        pre.identify_redis_config(UNSAFE_CONFIGS[name])


def test_identify_redis_config_accepts_canonical_targets():
    """该放行的必须放行 —— 否则表现是"配置一个字没错,服务起不来"。"""
    assert pre.identify_redis_config(pconfig.RedisConf(host="redis-a:6379")).topology == (
        "standalone"
    )
    assert pre.identify_redis_config(
        pconfig.RedisConf(host="127.0.0.1:6379")
    ).topology == "standalone"
    assert pre.identify_redis_config(
        pconfig.RedisConf(host="[::1]:6379")
    ).topology == "standalone"
    assert pre.identify_redis_config(
        pconfig.RedisConf(host="redis-a:6379", master_name="mymaster")
    ).topology == "sentinel"
    # go-redis 的选型口径:原始 addrs 只有一个 → standalone。
    assert pre.identify_redis_config(
        pconfig.RedisConf(host="ignored:6379", addrs=["redis-a:6379"])
    ).topology == "standalone"


CANONICAL_READ_ONLY_YAML = """node:
  redis_client:
    host: redis.internal:6379
    db: 0
    dial_timeout: 2s
    read_timeout: 3s
    write_timeout: 4s
    maint_notifications: disabled
"""

WRITER_YAML = """node:
  redis_client:
    host: ignored-writer-host:6379
    addrs: [redis-b:6379, redis-a:6379]
    master_name: ""
    password: writer-secret-never-forwarded
    db: 0
"""

READ_ONLY_CLUSTER_YAML = """node:
  redis_client:
    addrs:
      - redis-a:6379
      - redis-b:6379
    db: 0
    maint_notifications: disabled
"""


def test_parse_read_only_yaml_accepts_the_credential_free_canonical_document():
    rc = pre.parse_read_only_redis_config_yaml(CANONICAL_READ_ONLY_YAML.encode())
    assert rc.host == "redis.internal:6379"
    assert rc.password == "" and rc.username == ""
    assert rc.dial_timeout == "2s" and rc.read_timeout == "3s" and rc.write_timeout == "4s"


UNSAFE_READ_ONLY_YAML = {
    "empty username": CANONICAL_READ_ONLY_YAML.replace(
        "    db: 0", '    username: ""\n    db: 0', 1
    ),
    "empty password": CANONICAL_READ_ONLY_YAML.replace(
        "    db: 0", '    password: ""\n    db: 0', 1
    ),
    "unknown redis field": CANONICAL_READ_ONLY_YAML.replace(
        "    db: 0", "    future_field: x\n    db: 0", 1
    ),
    "unknown node field": CANONICAL_READ_ONLY_YAML.replace(
        "  redis_client:", "  future_node_field: x\n  redis_client:", 1
    ),
    "unknown top-level field": "future_root: x\n" + CANONICAL_READ_ONLY_YAML,
    "second document": CANONICAL_READ_ONLY_YAML + "---\nnode: {}\n",
    "empty trailing document": CANONICAL_READ_ONLY_YAML + "---\n",
    "maintenance auto": CANONICAL_READ_ONLY_YAML.replace("disabled", "auto", 1),
    "maintenance whitespace": CANONICAL_READ_ONLY_YAML.replace(
        "maint_notifications: disabled", 'maint_notifications: " disabled "', 1
    ),
    "non-zero db": CANONICAL_READ_ONLY_YAML.replace("db: 0", "db: 5", 1),
    "typed host": CANONICAL_READ_ONLY_YAML.replace("host: redis.internal:6379", "host: 6379", 1),
    "bad duration": CANONICAL_READ_ONLY_YAML.replace("dial_timeout: 2s", "dial_timeout: abc", 1),
    "mapping duration": CANONICAL_READ_ONLY_YAML.replace(
        "dial_timeout: 2s", "dial_timeout: {a: b}", 1
    ),
    "empty body": "",
}


@pytest.mark.parametrize("name", sorted(UNSAFE_READ_ONLY_YAML))
def test_parse_read_only_yaml_is_strict_single_document_and_credential_free(name: str):
    with pytest.raises(pre.PreflightError):
        pre.parse_read_only_redis_config_yaml(UNSAFE_READ_ONLY_YAML[name].encode())


def test_compare_redis_config_yaml_matches_normalized_target():
    identity = pre.compare_redis_config_yaml(
        WRITER_YAML.encode(), READ_ONLY_CLUSTER_YAML.encode()
    )
    assert identity.topology == "cluster"
    assert pre.valid_target_identity(identity.digest)
    # 与直接算出来的一致(证明"比对通过"用的就是那个身份)。
    assert identity == pre.identify_redis_config(
        pconfig.RedisConf(addrs=["redis-a:6379", "redis-b:6379"])
    )


UNSAFE_COMPARE_YAML = {
    "inline password": READ_ONLY_CLUSTER_YAML.replace(
        "maint_notifications: disabled", "password: leaked\n    maint_notifications: disabled", 1
    ),
    "inline username": READ_ONLY_CLUSTER_YAML.replace(
        "maint_notifications: disabled", "username: writer\n    maint_notifications: disabled", 1
    ),
    "maintenance auto": READ_ONLY_CLUSTER_YAML.replace("disabled", "auto", 1),
    "non-zero db": READ_ONLY_CLUSTER_YAML.replace("db: 0", "db: 5", 1),
    "different endpoint": READ_ONLY_CLUSTER_YAML.replace("redis-b:6379", "redis-c:6379", 1),
    "duplicate endpoint": READ_ONLY_CLUSTER_YAML.replace("- redis-b:6379", "- redis-a:6379", 1),
    "whitespace endpoint": READ_ONLY_CLUSTER_YAML.replace(
        "- redis-b:6379", '- " redis-b:6379 "', 1
    ),
    "uppercase endpoint": READ_ONLY_CLUSTER_YAML.replace("redis-b:6379", "REDIS-B:6379", 1),
}


@pytest.mark.parametrize("name", sorted(UNSAFE_COMPARE_YAML))
def test_compare_redis_config_yaml_rejects_unsafe_snapshots(name: str):
    with pytest.raises(pre.PreflightError):
        pre.compare_redis_config_yaml(
            WRITER_YAML.encode(), UNSAFE_COMPARE_YAML[name].encode()
        )


def test_compare_redis_config_yaml_never_leaks_secrets():
    """错误消息里绝不能出现写者口令或内网地址 —— 调用方一记日志就等于把口令发布了。"""
    secret = "never-print-this-writer-password"
    writer = f"node:\n  redis_client:\n    host: redis:6379\n    password: {secret}\n"
    read_only = "node:\n  redis_client:\n    host: other:6379\n    maint_notifications: disabled\n"
    with pytest.raises(pre.PreflightError) as excinfo:
        pre.compare_redis_config_yaml(writer.encode(), read_only.encode())
    rendered = repr(excinfo.value) + str(excinfo.value) + excinfo.value.msg
    assert secret not in rendered
    assert "redis:6379" not in rendered
    # `from None` 必须切断异常链,否则 traceback 里仍能翻出输入内容。
    assert excinfo.value.__cause__ is None


# ── ⑧ key 解析与分级 ───────────────────────────────────────────────────────


def test_parse_battle_record_key_directions():
    assert pre.parse_battle_record_key("pandora:ds:battle:{12345}") == 12345
    assert pre.parse_battle_record_key(f"pandora:ds:battle:{{{pre.UINT64_MAX}}}") == (
        pre.UINT64_MAX
    )
    for key, want in [
        ("pandora:ds:battle:12345", "unexpected key shape"),
        ("pandora:ds:battle:{}", "unexpected key shape"),
        ("pandora:ds:battle:{12a}", "unexpected key shape"),
        ("pandora:ds:battle:{12345}\n", "unexpected key shape"),
        ("pandora:ds:battle:{007}", "non-canonical decimal"),
        ("pandora:ds:battle:{0}", "zero is reserved"),
        (f"pandora:ds:battle:{{{pre.UINT64_MAX + 1}}}", "value out of range"),
    ]:
        with pytest.raises(pre.PreflightError) as excinfo:
            pre.parse_battle_record_key(key)
        assert want in excinfo.value.msg


def test_classify_battle_accepts_safe_records():
    """该放行的必须放行:完整四元组 = exact_identity 且**没有** reason。"""
    result = pre.classify_battle(12345, exact_record(state="running"))
    assert result.category == pre.CATEGORY_EXACT_IDENTITY
    assert result.reasons == []

    uncertain = pre.classify_battle(12345, battle_record(state="allocation_uncertain"))
    assert uncertain.category == pre.CATEGORY_ALLOCATION_UNCERTAIN
    assert uncertain.reasons == []

    tombstone = pre.classify_battle(
        12345, battle_record(state="allocation_reconcile_empty_tombstone")
    )
    assert tombstone.category == pre.CATEGORY_NO_PHYSICAL_IDENTITY

    abandoned_empty = pre.classify_battle(12345, battle_record(state="abandoned"))
    assert abandoned_empty.category == pre.CATEGORY_NO_PHYSICAL_IDENTITY
    assert abandoned_empty.reasons == []

    allocating = pre.classify_battle(12345, battle_record(state="allocating"))
    assert allocating.category == pre.CATEGORY_NO_PHYSICAL_IDENTITY
    assert allocating.reasons == []


def test_classify_battle_missing_pod_uid_is_unsafe():
    """本闸存在的**唯一理由**:有 exact 身份却没有 pod_uid = ABA 风险。"""
    rec = exact_record(state="ready")
    rec.pod_uid = ""
    result = pre.classify_battle(12345, rec)
    assert result.category == pre.CATEGORY_UNSAFE
    assert result.reasons == ["ready exact allocation identity is missing pod_uid"]


def test_classify_battle_collects_every_missing_identity_field():
    """四条一次列全(不是遇到第一条就返回)。"""
    rec = battle_record(state="running", ds_addr="10.0.0.1:7777")
    result = pre.classify_battle(12345, rec)
    assert result.category == pre.CATEGORY_UNSAFE
    assert result.reasons == [
        "running exact identity has empty ds_pod_name",
        "running exact identity has empty gameserver_uid",
        "running exact identity has invalid release_track",
        "running exact allocation identity is missing pod_uid",
    ]


def test_classify_battle_rejects_partial_identity_in_no_identity_states():
    for state in ("allocating", "allocation_uncertain", "allocation_reconcile_empty_tombstone"):
        rec = battle_record(state=state, ds_pod_name="battle-stable-abcde")
        result = pre.classify_battle(12345, rec)
        assert result.category == pre.CATEGORY_UNSAFE
        assert result.reasons == [
            f"{state} carries a partial or unexpected physical GameServer identity"
        ]


def test_classify_battle_rejects_unknown_state_and_unknown_proto_fields():
    unknown_state = pre.classify_battle(12345, battle_record(state="future_state"))
    assert unknown_state.category == pre.CATEGORY_UNSAFE
    assert unknown_state.reasons == ['unknown canonical battle state "future_state"']

    newer_writer = with_unknown_field(exact_record(state="ready"))
    result = pre.classify_battle(12345, newer_writer)
    assert result.category == pre.CATEGORY_UNSAFE
    assert result.reasons[0] == (
        "battle record contains unknown protobuf fields that this release gate cannot audit"
    )


def test_classify_battle_key_and_record_identity_must_agree():
    result = pre.classify_battle(999, exact_record(state="ready", match_id=12345))
    assert "record match_id=12345 does not match key match_id=999" in result.reasons

    zero_key = pre.classify_battle(0, exact_record(state="ready", match_id=0))
    assert "battle key contains match_id=0" in zero_key.reasons

    assert pre.classify_battle(1, None).reasons == ["record is nil"]

    bad_alloc = exact_record(state="ready")
    bad_alloc.allocation_id = "not-a-uuid"
    assert "battle record has non-canonical UUIDv4 allocation_id" in pre.classify_battle(
        12345, bad_alloc
    ).reasons
    # v1 UUID(可由 MAC / 时间推导)同样不可接受。
    assert not pre.valid_allocation_id(str(_uuid.uuid1()))
    assert pre.valid_allocation_id(str(_uuid.uuid4()))


# ── ⑨ SCAN 审计 ────────────────────────────────────────────────────────────


def battle_payload(match_id: int, **kwargs) -> bytes:  # noqa: ANN003
    return exact_record(match_id=match_id, **kwargs).SerializeToString()


async def test_scan_redis_node_pages_with_cursor_and_never_uses_keys():
    """SCAN 必须走游标分批;`KEYS` 会阻塞整个 Redis(替身根本没有 keys 方法)。"""
    keys = [f"pandora:ds:battle:{{{i}}}" for i in range(1, 6)]
    values = {key: battle_payload(int(key[19:-1]), state="ready") for key in keys}
    node = ScanNode([(7, keys[:2]), (9, keys[2:4]), (0, keys[4:])], values)
    summary = pre.AuditSummary()
    await pre.scan_redis_node(node, RUN_ID, "redis-primary", 128, summary)
    assert [call[0] for call in node.scan_calls] == [0, 7, 9]
    assert {call[1] for call in node.scan_calls} == {pre.BATTLE_SCAN_PATTERN}
    assert {call[2] for call in node.scan_calls} == {128}
    assert summary.keys_visited == 5
    assert summary.records_decoded == 5
    assert summary.findings == []
    assert not hasattr(node, "keys")


async def test_scan_records_findings_for_malformed_keys_and_bodies():
    good = "pandora:ds:battle:{1}"
    malformed_key = "pandora:ds:battle:legacy"
    bad_body = "pandora:ds:battle:{2}"
    values = {
        good: battle_payload(1, state="ready"),
        malformed_key: b"whatever",
        # 明确无法解析成 BattleStorageRecord 的字节(field 1 声明为 varint,
        # 这里给一个截断的 varint)。
        bad_body: b"\x08",
    }
    node = ScanNode([(0, [good, malformed_key, bad_body])], values)
    summary = pre.AuditSummary()
    await pre.scan_redis_node(node, RUN_ID, "redis-primary", 10, summary)
    summary.sort_findings()
    reasons = {finding.key: finding.reason for finding in summary.findings}
    assert reasons[malformed_key] == "unexpected key shape under battle namespace"
    assert reasons[bad_body].startswith("protobuf decode failed: ")
    # ★ reason 里绝不能出现 "errcode=1" —— 那是 Python 异常的 str(),不是 Go 的错误文本。
    assert "errcode=" not in reasons[malformed_key]
    assert summary.records_decoded == 1
    assert malformed_key not in node.get_calls  # 形状不对就不去 GET


async def test_scan_duplicate_keys_are_safe_but_changed_bodies_fail_closed():
    """SCAN 允许重复 key:同内容忽略,**内容变了必须整轮失败**。"""
    key = "pandora:ds:battle:{1}"
    payload = battle_payload(1, state="ready")
    node = ScanNode([(0, [key, key])], {key: payload})
    summary = pre.AuditSummary()
    await pre.scan_redis_node(node, RUN_ID, "redis-primary", 10, summary)
    assert summary.keys_visited == 1
    assert summary.records_decoded == 1

    class _Changing(ScanNode):
        async def get(self, key: str):  # noqa: ANN201
            self.get_calls.append(key)
            return battle_payload(1, state="ready") if len(self.get_calls) == 1 else (
                battle_payload(1, state="running")
            )

    changing = _Changing([(0, [key, key])], {key: payload})
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.scan_redis_node(changing, RUN_ID, "redis-primary", 10, pre.AuditSummary())
    assert "changed during audit" in excinfo.value.msg


async def test_scan_fails_closed_when_a_key_disappears():
    key = "pandora:ds:battle:{1}"
    node = ScanNode([(0, [key])], {})
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.scan_redis_node(node, RUN_ID, "redis-primary", 10, pre.AuditSummary())
    assert "disappeared during audit" in excinfo.value.msg


def test_summary_rejects_same_key_on_two_masters():
    """同一个 key 出现在两个 master 上 = slot 正在搬,审计看不全。"""
    summary = pre.AuditSummary()
    assert summary.register_scanned_key("pandora:ds:battle:{1}", NODE_A) is True
    assert summary.register_scanned_key("pandora:ds:battle:{1}", NODE_A) is False
    with pytest.raises(pre.PreflightError) as excinfo:
        summary.register_scanned_key("pandora:ds:battle:{1}", NODE_B)
    assert "appeared on multiple Redis masters" in excinfo.value.msg


def test_summary_master_coverage_must_match_visited_count():
    summary = pre.AuditSummary()
    with pytest.raises(pre.PreflightError):
        summary.runtime_master_set_digest()
    summary.master_started()
    summary.register_runtime_master(NODE_A)
    assert summary.runtime_master_set_digest() == pre.runtime_master_set_digest([NODE_A])
    with pytest.raises(pre.PreflightError) as excinfo:
        summary.register_runtime_master(NODE_A)
    assert "duplicate Redis master identity" in excinfo.value.msg
    summary.master_started()
    with pytest.raises(pre.PreflightError) as excinfo:
        summary.runtime_master_set_digest()
    assert "coverage does not match" in excinfo.value.msg


def test_summary_counts_allocation_uncertain_separately_and_sorts_findings():
    summary = pre.AuditSummary()
    summary.record_decoded(pre.CATEGORY_EXACT_IDENTITY)
    summary.record_decoded(pre.CATEGORY_ALLOCATION_UNCERTAIN)
    assert summary.records_decoded == 2
    assert summary.allocation_uncertain == 1
    summary.add_finding(pre.Finding(source="s2", key="b", reason="r"))
    summary.add_finding(pre.Finding(source="s1", key="a", reason="z"))
    summary.add_finding(pre.Finding(source="s1", key="a", reason="a"))
    summary.sort_findings()
    assert [(f.key, f.reason) for f in summary.findings] == [
        ("a", "a"),
        ("a", "z"),
        ("b", "r"),
    ]


async def test_audit_redis_standalone_requires_stable_runtime_identity():
    key = "pandora:ds:battle:{1}"
    node = ScanNode([(0, [key])], {key: battle_payload(1, state="ready")})
    summary = pre.AuditSummary()
    await pre.audit_redis(node, 100, summary)
    assert summary.masters_visited == 1
    assert summary.runtime_master_set_digest() == pre.runtime_master_set_digest([RUN_ID])

    class _Flipping(ScanNode):
        """扫描前后各取一次 run_id,第二次换一个 —— 模拟期间发生过 failover / 重建。

        计数必须自己数:`INFO server` 这一支提前返回,不会进 `self.calls`,
        拿 `self.calls` 判"第几次"永远是第一次。
        """

        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.info_server_calls = 0

        async def execute_command(self, *args):  # noqa: ANN001, ANN201
            if args[0] == "INFO server":
                self.info_server_calls += 1
                run_id = RUN_ID if self.info_server_calls == 1 else "d" * 40
                return f"run_id:{run_id}\r\n".encode()
            return await super().execute_command(*args)

    flipping = _Flipping([(0, [key])], {key: battle_payload(1, state="ready")})
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.audit_redis(flipping, 100, pre.AuditSummary())
    assert "runtime identity changed during scan" in excinfo.value.msg


@pytest.mark.parametrize("scan_count", [0, -1, pre.INT64_MAX + 1, True, 1.5])
async def test_audit_redis_rejects_invalid_scan_count(scan_count):
    node = ScanNode([(0, [])], {})
    with pytest.raises(pre.PreflightError) as excinfo:
        await pre.audit_redis(node, scan_count, pre.AuditSummary())
    assert "positive scan count" in excinfo.value.msg
    with pytest.raises(pre.PreflightError):
        await pre.audit_redis(None, 10, pre.AuditSummary())
    with pytest.raises(pre.PreflightError):
        await pre.audit_redis(node, 10, None)


async def test_audit_redis_visits_every_cluster_master():
    """cluster 下必须逐 master 扫;只扫一个分片的结论不能当发布依据。"""
    key_a = "pandora:ds:battle:{1}"
    key_b = "pandora:ds:battle:{2}"

    class _ClusterScanNode(ScanNode, ClusterScriptedNode):
        def __init__(self, pages, values, my_id):  # noqa: ANN001
            ClusterScriptedNode.__init__(self)
            self.pages = pages
            self.values = values
            self.scan_calls = []
            self.get_calls = []
            self.my_id = my_id

    node_a = _ClusterScanNode([(0, [key_a])], {key_a: battle_payload(1, state="ready")}, NODE_A)
    node_b = _ClusterScanNode([(0, [key_b])], {key_b: battle_payload(2, state="ready")}, NODE_B)

    class _Node:
        def __init__(self, client, name):  # noqa: ANN001
            self.redis_connection = client
            self.name = name

    class _Cluster:
        def get_primaries(self):  # noqa: ANN201
            return [_Node(node_a, "10.0.0.1:6379"), _Node(node_b, "10.0.0.2:6379")]

    summary = pre.AuditSummary()
    await pre.audit_redis(_Cluster(), 50, summary)
    assert summary.masters_visited == 2
    assert summary.keys_visited == 2
    assert summary.runtime_master_set_digest() == pre.runtime_master_set_digest([NODE_A, NODE_B])
    # 两个 master 的 source 必须不同(发现才能按 master 聚合),且都不含明文地址。
    sources = {
        pre.safe_redis_source("redis-cluster-master", "10.0.0.1:6379"),
        pre.safe_redis_source("redis-cluster-master", "10.0.0.2:6379"),
    }
    assert len(sources) == 2
    assert all("10.0.0." not in source for source in sources)


# ── ⑩ Go 语义的文本原语(分叉点)────────────────────────────────────────────


def test_go_text_primitives_differ_from_python_defaults():
    """U+001C..U+001F:Python 当空白,Go 不当。

    ★ 分叉方向:用 Python 的 `str.split()` 会把"一个含控制字符的 token"切成
      "两个干净 token",于是 ACL 命令清单里夹一个 U+001C 也可能被判成规范 ——
      **该拒的没拒**。所以本模块自带 `_go_fields` / `_go_trim_space`。
    """
    assert "\x1c".isspace() is True  # Python 的口径
    assert pre._go_is_space("\x1c") is False  # Go 的口径
    assert pre._go_is_control("\x1c") is True
    assert pre._go_fields("+get\x1c+scan") == ["+get\x1c+scan"]
    assert "+get\x1c+scan".split() == ["+get", "+scan"]
    assert pre._go_trim_space("\x1cabc\x1c") == "\x1cabc\x1c"
    assert pre._go_trim_space(" \t abc \n ") == "abc"
    # Cf(软连字符)在 Go 不算控制字符,不能用 category[0]=="C" 一刀切。
    assert pre._go_is_control("\u00ad") is False


def test_go_split_host_port_matches_go_semantics():
    """`rsplit(':',1)` 不是 `net.SplitHostPort` 的等价物。"""
    assert pre._split_host_port("redis:6379") == ("redis", "6379")
    assert pre._split_host_port("[::1]:6379") == ("::1", "6379")
    for bad in ("::1:6379", "redis", "[::1]6379", "[::1", ""):
        with pytest.raises(ValueError):
            pre._split_host_port(bad)


def test_length_prefix_makes_the_digest_injective():
    """没有长度前缀时 ["ab","c"] 与 ["a","bc"] 会撞成同一个摘要。"""
    assert pre.length_prefixed_sha256(["ab", "c"]) != pre.length_prefixed_sha256(["a", "bc"])
    # 朴素拼接(反例)确实会撞 —— 证明这个前缀不是装饰。
    assert hashlib.sha256(b"abc").digest() == hashlib.sha256(b"abc").digest()
