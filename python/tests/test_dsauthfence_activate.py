"""`pandorapy.dsauthfence_activate` 的行为测试。

★ 依赖隔离:`pandorapy.dsauthfence` 正在并行移植,本文件在导入被测模块之前
  把一个**行为忠于 Go 侧**的假件塞进 `sys.modules`。假件不是"随便返回点什么",
  它按 fence.go 的取值与判定实现 —— 否则测试会在假的常量上打绿。

★ etcd 假件是一个**真的 mini-etcd**:自己实现 revision / version /
  create_revision / mod_revision / lease,以及 etcd 的 compare 语义
  (含"空 range + Value 比较恒 false、空 range + Create==0 为 true")。
  历史教训是假件根本造不出目标失败态、测试假绿;这里的 CAS 测试要想有意义,
  compare 就必须真的被求值,而不是被假件无条件放行。
  compare / op 对象直接用 **aetcd 的真类**(`aetcd.client.Transactions`),
  这样"被测模块调用 aetcd 的姿势对不对"也一并被钉住。

每条断言旁标了 `★ 变异:...→ 本条红`,并已逐条把产品代码改成该变异跑过一遍确认转红。
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import re
import sys
import time
import types
import typing

import pytest

from aetcd import transactions as _aetcd_txn
from aetcd.client import Transactions as _AetcdTransactions
from aetcd.rpc import Compare as _Compare


# ── 假 dsauthfence(fence.go 的取值/判定,逐条照抄)──────────────────────────
def _build_fake_dsauthfence() -> types.ModuleType:
    mod = types.ModuleType("pandorapy.dsauthfence")

    mod.PROTOCOL_EPOCH_V2 = 2
    mod.REQUIRED_POLICY_V2 = "ds-auth-v2-pod-uid-write-invariant-v1"
    mod.REQUIRED_VALUE_V2 = "2@" + mod.REQUIRED_POLICY_V2
    mod.REQUIRED_POLICY_V3 = "ds-auth-v2-hub-successor-lease-v1"
    mod.REQUIRED_VALUE_V3 = "2@" + mod.REQUIRED_POLICY_V3
    mod.REQUIRED_POLICY_GENERATION_V1 = 1
    mod.REQUIRED_POLICY_GENERATION_V2 = 2
    mod.REQUIRED_POLICY_GENERATION_V3 = 3
    mod.DEFAULT_PREFIX = "/pandora/ds-auth/"
    mod.DEFAULT_DIAL_TIMEOUT = 5.0
    mod.ErrTopologyChangeLockProviderUnavailable = RuntimeError(
        "dsauthfence: authoritative Redis topology-change lock provider is not wired; "
        "target epoch CAS is disabled"
    )

    # ★ 刻意保留 Go 原样的 `^...$` 锚定(一个"直译但不安全"的移植会这么写)。
    #   被测模块必须用 fullmatch 把 Python 的 `$` 尾换行洞堵死 —— 见对应用例。
    mod.digest_pattern = re.compile(r"^sha256:[0-9a-f]{64}$")
    _feature_pattern = re.compile(r"\A[a-z][a-z0-9-]{2,63}\Z")

    _v2_features = {
        "login": [],
        "player_locator": [],
        "ds_allocator": [
            "battle-release-expected-tuple-v1",
            "battle-storage-pod-uid-write-invariant-v1",
        ],
        "hub_allocator": [
            "hub-reservation-ledger-v1",
            "hub-heartbeat-capacity-v1",
            "hub-owner-cleanup-v1",
            "hub-physical-eviction-v1",
        ],
        "battle_result": ["battle-terminal-outbox-v1"],
    }
    _v3_features = {
        **{k: list(v) for k, v in _v2_features.items()},
        "hub_allocator": [*_v2_features["hub_allocator"], "hub-successor-lease-v1"],
    }
    mod.requiredPolicyV2Features = _v2_features
    mod.requiredPolicyV3Features = _v3_features

    @dataclasses.dataclass(slots=True)
    class RequiredState:
        epoch: int = 0
        policy_generation: int = 0
        policy_id: str = ""
        raw_value: str = ""

    @dataclasses.dataclass(slots=True)
    class Capability:
        service: str = ""
        instance_uid: str = ""
        writer_epoch: int = 0
        supported_policy_generation: int = 0
        supported_policy_id: str = ""
        acquired_policy_generation: int = 0
        acquired_policy_id: str = ""
        image_digest: str = ""
        keyset_revision: str = ""
        etcd_identity_revision: str = ""
        started_at_ms: int = 0
        features: list[str] = dataclasses.field(default_factory=list)

        @classmethod
        def from_json_bytes(cls, raw: bytes) -> Capability:
            obj = json.loads(raw.decode("utf-8"))
            if not isinstance(obj, dict):
                raise ValueError("capability must be a JSON object")
            return cls(
                service=obj.get("service", ""),
                instance_uid=obj.get("instance_uid", ""),
                writer_epoch=obj.get("writer_epoch", 0),
                supported_policy_generation=obj.get("supported_policy_generation", 0),
                supported_policy_id=obj.get("supported_policy_id", ""),
                acquired_policy_generation=obj.get("acquired_policy_generation", 0),
                acquired_policy_id=obj.get("acquired_policy_id", ""),
                image_digest=obj.get("image_digest", ""),
                keyset_revision=obj.get("keyset_revision", ""),
                etcd_identity_revision=obj.get("etcd_identity_revision", ""),
                started_at_ms=obj.get("started_at_ms", 0),
                features=list(obj.get("features") or []),
            )

        def to_json_bytes(self) -> bytes:
            payload = {
                "service": self.service,
                "instance_uid": self.instance_uid,
                "writer_epoch": self.writer_epoch,
                "supported_policy_generation": self.supported_policy_generation,
                "supported_policy_id": self.supported_policy_id,
                "acquired_policy_generation": self.acquired_policy_generation,
                "image_digest": self.image_digest,
                "keyset_revision": self.keyset_revision,
                "started_at_ms": self.started_at_ms,
            }
            if self.acquired_policy_id:
                payload["acquired_policy_id"] = self.acquired_policy_id
            if self.etcd_identity_revision:
                payload["etcd_identity_revision"] = self.etcd_identity_revision
            if self.features:
                payload["features"] = list(self.features)
            return json.dumps(payload, separators=(",", ":")).encode("utf-8")

    @dataclasses.dataclass(slots=True)
    class ClientSecurity:
        pass

    mod.RequiredState = RequiredState
    mod.Capability = Capability
    mod.ClientSecurity = ClientSecurity

    def clean_prefix(prefix: str) -> str:
        return (prefix[:-1] if prefix.endswith("/") else prefix) + "/"

    mod.clean_prefix = clean_prefix
    mod.required_key = lambda p: clean_prefix(p) + "required-writer-epoch"
    mod.capability_prefix = lambda p: clean_prefix(p) + "capabilities/"
    mod.activation_lock_key = lambda p: clean_prefix(p) + "activation-lock"
    mod.capability_key = lambda p, s, u: clean_prefix(p) + "capabilities/" + s + "/" + u

    def parse_required_state(raw: bytes) -> RequiredState:
        s = raw.decode("utf-8")
        if s == "1":
            return RequiredState(1, 1, "", s)
        if s == mod.REQUIRED_VALUE_V2:
            return RequiredState(2, 2, mod.REQUIRED_POLICY_V2, s)
        if s == mod.REQUIRED_VALUE_V3:
            return RequiredState(2, 3, mod.REQUIRED_POLICY_V3, s)
        raise ValueError('invalid or unsupported required writer policy "%s"' % s)

    mod.parse_required_state = parse_required_state

    def required_value_for_epoch(epoch: int) -> str:
        if epoch == 1:
            return "1"
        if epoch == 2:
            return mod.REQUIRED_VALUE_V2
        raise ValueError("unsupported required writer epoch %d" % epoch)

    def required_value_for_policy_generation(gen: int) -> str:
        return {1: "1", 2: mod.REQUIRED_VALUE_V2, 3: mod.REQUIRED_VALUE_V3}.get(gen) or _raise(
            "unsupported required policy generation %d" % gen
        )

    def required_policy_id_for_generation(gen: int) -> str:
        if gen == 1:
            return ""
        if gen == 2:
            return mod.REQUIRED_POLICY_V2
        if gen == 3:
            return mod.REQUIRED_POLICY_V3
        raise ValueError("unsupported required policy generation %d" % gen)

    def required_writer_epoch_for_policy_generation(gen: int) -> int:
        if gen == 1:
            return 1
        if gen in (2, 3):
            return 2
        raise ValueError("unsupported required policy generation %d" % gen)

    def required_features_for_policy_generation(gen: int):
        if gen == 2:
            return _v2_features
        if gen == 3:
            return _v3_features
        raise ValueError("unsupported required policy generation %d" % gen)

    def _raise(msg: str) -> typing.NoReturn:
        raise ValueError(msg)

    mod.required_value_for_epoch = required_value_for_epoch
    mod.required_value_for_policy_generation = required_value_for_policy_generation
    mod.required_policy_id_for_generation = required_policy_id_for_generation
    mod.required_writer_epoch_for_policy_generation = required_writer_epoch_for_policy_generation
    mod.required_features_for_policy_generation = required_features_for_policy_generation

    def validate_features(features) -> None:
        seen = set()
        for feature in features:
            if not feature or feature.strip() != feature or not _feature_pattern.fullmatch(feature):
                raise ValueError("dsauthfence: invalid capability feature")
            if feature in seen:
                raise ValueError("dsauthfence: duplicate capability feature")
            seen.add(feature)

    mod.validate_features = validate_features

    def validate_activation_policy_generation(gen, services, features) -> None:
        expected_policy = required_features_for_policy_generation(gen)
        policy_id = required_policy_id_for_generation(gen)
        if len(services) != len(expected_policy):
            raise ValueError("activation service set does not match %s" % policy_id)
        for service, expected in expected_policy.items():
            if services.get(service, 0) <= 0:
                raise ValueError("activation service %s missing from %s" % (service, policy_id))
            actual = features.get(service) or set()
            if len(actual) != len(expected):
                raise ValueError(
                    "activation feature policy for %s does not match %s" % (service, policy_id)
                )
            for feature in expected:
                if feature not in actual:
                    raise ValueError(
                        "activation feature policy for %s misses %s" % (service, feature)
                    )
        if gen == 3 and services.get("hub_allocator", 0) != 1:
            raise ValueError(
                "activation policy %s requires exactly one hub_allocator writer" % policy_id
            )
        for service in features:
            if service not in expected_policy:
                raise ValueError("activation feature policy contains unknown service %s" % service)

    mod.validate_activation_policy_generation = validate_activation_policy_generation

    def validate_required_policy_for_capability(state, service, writer_epoch, features) -> None:
        if state.raw_value == "" or state.epoch == 0:
            raise ValueError("dsauthfence: empty required policy state")
        if service not in _v2_features:
            raise ValueError('dsauthfence: unknown writer service "%s"' % service)
        if state.policy_generation == 2:
            if set(features) == set(_v2_features[service]):
                return
            if service == "hub_allocator" and set(features) == set(_v3_features[service]):
                return
            raise ValueError(
                "dsauthfence: service %s does not advertise exact V2 or staged V3 features" % service
            )
        if state.policy_generation == 3:
            if set(features) != set(_v3_features[service]):
                raise ValueError(
                    "dsauthfence: service %s does not advertise the exact %s feature policy"
                    % (service, mod.REQUIRED_POLICY_V3)
                )
            return
        if state.policy_generation == 1:
            return
        raise ValueError(
            "dsauthfence: unsupported required policy generation %d" % state.policy_generation
        )

    mod.validate_required_policy_for_capability = validate_required_policy_for_capability

    def expected_services_hash(services) -> str:
        body = "".join("%s=%d\n" % (k, services[k]) for k in sorted(services))
        return hashlib.sha256(body.encode("utf-8")).hexdigest()

    mod.expected_services_hash = expected_services_hash
    mod.new_etcd_client = lambda endpoints, timeout, prefix, security: FakeEtcd()
    return mod


# ── mini-etcd ───────────────────────────────────────────────────────────────
class _Entry:
    __slots__ = ("value", "create_revision", "mod_revision", "version", "lease")

    def __init__(self, value: bytes, revision: int, lease: int) -> None:
        self.value = value
        self.create_revision = revision
        self.mod_revision = revision
        self.version = 1
        self.lease = lease


class _GetView:
    """与 `aetcd.rtypes.Get` / `KeyValue` 同形的只读视图。"""

    __slots__ = ("key", "value", "create_revision", "mod_revision", "version", "lease")

    def __init__(self, key: bytes, entry: _Entry) -> None:
        self.key = key
        self.value = entry.value
        self.create_revision = entry.create_revision
        self.mod_revision = entry.mod_revision
        self.version = entry.version
        self.lease = entry.lease


_ZERO_ENTRY = _Entry(b"", 0, 0)
_ZERO_ENTRY.create_revision = 0
_ZERO_ENTRY.mod_revision = 0
_ZERO_ENTRY.version = 0


class FakeLease:
    def __init__(self, client: FakeEtcd, lease_id: int, ttl: int) -> None:
        self._client = client
        self.id = lease_id
        self.ttl = ttl

    async def refresh(self):
        return types.SimpleNamespace(ID=self.id, TTL=self._client.refresh_ttl(self.id))


class FakeEtcd:
    """够真的内存 etcd:revision / version / lease / compare 语义都实现。"""

    def __init__(self) -> None:
        self.store: dict[bytes, _Entry] = {}
        self.revision = 0
        self.leases: dict[int, int] = {}
        self._next_lease = 1
        self.transactions = _AetcdTransactions()
        self.txn_calls = 0
        self.revoked: list[int] = []
        self.closed = False
        # 测试可以把某个 lease 标成"服务端已认定不存在"(TTL<=0)。
        self.dead_leases: set[int] = set()

    # ── 客户端面 ────────────────────────────────────────────────────────
    async def lease(self, ttl: int, lease_id: int | None = None) -> FakeLease:
        lease_id = lease_id or self._next_lease
        self._next_lease = max(self._next_lease, lease_id) + 1
        self.leases[lease_id] = ttl
        return FakeLease(self, lease_id, ttl)

    async def revoke_lease(self, lease_id: int) -> None:
        self.revoked.append(lease_id)
        self.leases.pop(lease_id, None)
        for key in [k for k, e in self.store.items() if e.lease == lease_id]:
            del self.store[key]

    def refresh_ttl(self, lease_id: int) -> int:
        if lease_id in self.dead_leases or lease_id not in self.leases:
            return 0
        return self.leases[lease_id]

    async def get(self, key: bytes):
        entry = self.store.get(key)
        return None if entry is None else _GetView(key, entry)

    async def get_prefix(self, prefix: bytes):
        end = _prefix_range_end(prefix)
        return [_GetView(k, e) for k, e in self._range(prefix, end)]

    async def close(self) -> None:
        self.closed = True

    async def transaction(self, compare, success=None, failure=None):
        self.txn_calls += 1
        ok = all(self._eval(c) for c in compare)
        ops = (success if ok else failure) or []
        # ★ 真 etcd 一个事务只推进**一个** revision,所以同事务写下的多个 key
        #   拿到相同的 mod/create revision —— 「required 与审计记录是同一次事务写的」
        #   这条断言完全建立在这个语义上。假件必须照做,否则会把产品代码的正确实现判成错。
        if any(isinstance(op, _aetcd_txn.Put) for op in ops):
            self.revision += 1
        revision = self.revision
        responses = []
        for op in ops:
            if isinstance(op, _aetcd_txn.Put):
                self._apply_put(op.key, op.value, op.lease, revision)
                responses.append(object())
            elif isinstance(op, _aetcd_txn.Get):
                responses.append(
                    [(e.value, _GetView(k, e)) for k, e in self._range(op.key, op.range_end)]
                )
            else:  # pragma: no cover - 被测模块只用 Put / Get
                raise NotImplementedError(type(op))
        return ok, responses

    # ── 内部 ────────────────────────────────────────────────────────────
    def _put(self, key: bytes, value: bytes, lease: int | None) -> None:
        """测试直接写:每次算**一个独立事务**,revision 各自推进。"""
        self.revision += 1
        self._apply_put(key, value, lease, self.revision)

    def _apply_put(self, key: bytes, value: bytes, lease: int | None, revision: int) -> None:
        entry = self.store.get(key)
        if entry is None:
            self.store[key] = _Entry(value, revision, int(lease or 0))
            return
        entry.value = value
        entry.mod_revision = revision
        entry.version += 1
        if lease:
            entry.lease = int(lease)

    def _range(self, key: bytes, range_end: bytes | None) -> list[tuple[bytes, _Entry]]:
        if range_end is None:
            entry = self.store.get(key)
            return [(key, entry)] if entry is not None else []
        return sorted((k, e) for k, e in self.store.items() if key <= k < range_end)

    def _eval(self, c) -> bool:
        if c.op != _Compare.EQUAL:  # pragma: no cover - 被测模块只用 ==
            raise NotImplementedError("fake etcd only implements EQUAL compares")
        kvs = self._range(_to_bytes(c.key), _to_bytes(c.range_end) if c.range_end else None)
        if not kvs:
            # etcd applyCompare:空 range 上比 Value 恒 false,其它 target 用零值 KV 比。
            if isinstance(c, _aetcd_txn.Value):
                return False
            return self._cmp_one(c, _ZERO_ENTRY)
        return all(self._cmp_one(c, e) for _, e in kvs)

    @staticmethod
    def _cmp_one(c, entry: _Entry) -> bool:
        if isinstance(c, _aetcd_txn.Value):
            return entry.value == _to_bytes(c.value)
        if isinstance(c, _aetcd_txn.Version):
            return entry.version == int(c.value)
        if isinstance(c, _aetcd_txn.Create):
            return entry.create_revision == int(c.value)
        if isinstance(c, _aetcd_txn.Mod):
            return entry.mod_revision == int(c.value)
        raise NotImplementedError(type(c))  # pragma: no cover


def _to_bytes(v):
    return v if isinstance(v, bytes) else v.encode("utf-8")


def _prefix_range_end(prefix: bytes) -> bytes:
    buf = bytearray(prefix)
    for i in reversed(range(len(buf))):
        if buf[i] < 0xFF:
            buf[i] += 1
            return bytes(buf[: i + 1])
    return b"\0"


# ── 导入被测模块 ───────────────────────────────────────────────────────────
#
# ★ 这里**刻意不再**把假件塞进 `sys.modules["pandorapy.dsauthfence"]`。
#   当初 dsauthfence 与 dsauthfence_activate 是并行移植的,真模块还不存在,只能用
#   假件顶上;如今真模块已完成(fence + etcd + security 全量),假件就成了纯负债 ——
#   而且那次替换是**全局且不恢复**的:pytest 在收集阶段就 import 全部测试模块,
#   一旦被替换,后面任何在模块级读 `dsauthfence.XXX` 的产品代码(例如
#   `player_locator/main.py` 的 `DS_AUTH_FENCE_FEATURES`)都会 AttributeError,
#   表现成"某个八竿子打不着的测试文件收集失败",排查代价极高。
#
#   `dsauthfence_activate` 对 dsauthfence 的依赖是**惰性 getattr**(见该模块
#   `_resolve` / `_Deps`),所以直接跑真模块即可,不需要任何注入。
#
#   `_FAKE` 仍然保留,但**只作本文件的常量/键名辅助源**(`_FAKE.required_key(...)`、
#   `_FAKE.REQUIRED_VALUE_V2` 等),不再顶替真模块。这样反而多一层交叉校验:被测代码
#   跑真 dsauthfence,期望值由独立手写的一份推导,两边算不到一块就会红。
_FAKE = _build_fake_dsauthfence()
import pandorapy  # noqa: E402, F401
from pandorapy import dsauthfence_activate as act  # noqa: E402
from pandorapy.errcode import PandoraError  # noqa: E402

PREFIX = "/pandora/ds-auth/"
DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64
EVIDENCE = "sha256:" + "c" * 64
NONCE = "nonce:" + "9" * 64


def _client(fake: FakeEtcd | None = None) -> tuple[act.ActivationClient, FakeEtcd]:
    fake = fake or FakeEtcd()
    return act.ActivationClient(cli=fake, prefix=PREFIX, timeout=2.0), fake


def _capability(service: str, uid: str, **over) -> object:
    base = {
        "service": service,
        "instance_uid": uid,
        "writer_epoch": 2,
        "supported_policy_generation": 2,
        "supported_policy_id": _FAKE.REQUIRED_POLICY_V2,
        "acquired_policy_generation": 2,
        "acquired_policy_id": _FAKE.REQUIRED_POLICY_V2,
        "image_digest": DIGEST_A,
        "keyset_revision": "ks-1",
        "features": list(_FAKE.requiredPolicyV2Features[service]),
    }
    base.update(over)
    return _FAKE.Capability(**base)


async def _acquire_lock(client: act.ActivationClient, ttl: int = 30) -> act.ActivationLock:
    lock = await client.acquire_lock(ttl)
    return lock


# ── 1. 拓扑锁 provider 未接线 ⇒ epoch 推进整条关闭 ─────────────────────────
async def test_advance_required_is_disabled_without_topology_lock_provider() -> None:
    client, fake = _client()
    lock = await _acquire_lock(client)
    try:
        before = fake.txn_calls
        with pytest.raises(BaseException) as got:
            await lock.advance_required(1, 2, 3, {}, [], EVIDENCE, 1)
        # ★ 变异:把 ActivationLock.__init__ 的 _topology_lease_verified 置 True
        #    → 本条红(不再抛 topology 错,而且会真的发出 CAS)。
        assert "topology-change lock provider is not wired" in str(got.value)
        # 它必须在**发出任何 etcd 请求之前**就拒 —— 否则等于用一次真 CAS 去试探。
        assert fake.txn_calls == before
    finally:
        await lock.close()


# ── 2. 推进 CAS 必须同时钉住 value 与 mod_revision(1→2→1 的 ABA)────────────
async def test_advance_cas_pins_mod_revision_so_aba_cannot_pass() -> None:
    client, fake = _client()
    await client.bootstrap_required(1)
    snapshot = await client.required_snapshot()
    lock = await _acquire_lock(client)
    lock._topology_lease_verified = True  # 只在测试里放行,产品代码没有这条路径
    try:
        # 快照拿到之后,required 被人 1→2→1 兜了一圈:值又回到 "1",但 mod_revision 变了。
        required_key = _FAKE.required_key(PREFIX).encode()
        fake._put(required_key, _FAKE.REQUIRED_VALUE_V2.encode(), None)
        fake._put(required_key, b"1", None)
        assert fake.store[required_key].value == b"1"
        assert fake.store[required_key].mod_revision != snapshot.mod_revision

        audited = _v2_audit_set(fake)
        with pytest.raises(PandoraError) as got:
            await lock.advance_required(
                1, 2, snapshot.mod_revision, _V2_SERVICES, audited, EVIDENCE, 1
            )
        # ★ 变异:_build_advance_compares 去掉 `t.mod(key) == expected_mod_revision`
        #    → ABA 直接通过,本条红。
        assert "required epoch CAS failed" in got.value.msg
        assert fake.store[required_key].value == b"1"
    finally:
        await lock.close()


# ── 3. 每条被审计的 capability 都要钉住 mod_revision ───────────────────────
async def test_advance_cas_pins_every_audited_capability_revision() -> None:
    client, fake = _client()
    await client.bootstrap_required(1)
    snapshot = await client.required_snapshot()
    lock = await _acquire_lock(client)
    lock._topology_lease_verified = True
    try:
        audited = _v2_audit_set(fake)
        # 审计之后、CAS 之前,有个 writer 重写了自己的 capability(换了镜像/换了副本)。
        victim = audited[0]
        fake._put(victim.key.encode(), b'{"service":"x"}', 7)

        with pytest.raises(PandoraError) as got:
            await lock.advance_required(
                1, 2, snapshot.mod_revision, _V2_SERVICES, audited, EVIDENCE, 1
            )
        # ★ 变异:_build_advance_compares 里不再为 audited 追加 mod 比较
        #    → 快照失效也照样推进,本条红。
        assert "required epoch CAS failed" in got.value.msg
    finally:
        await lock.close()


# ── 4. 推进成功时,required 与不可变审计记录在同一个事务里落 ────────────────
async def test_advance_writes_required_and_immutable_record_in_one_txn() -> None:
    client, fake = _client()
    await client.bootstrap_required(1)
    snapshot = await client.required_snapshot()
    lock = await _acquire_lock(client)
    lock._topology_lease_verified = True
    try:
        audited = _v2_audit_set(fake)
        await lock.advance_required(
            1, 2, snapshot.mod_revision, _V2_SERVICES, audited, EVIDENCE, 1
        )
        required_key = _FAKE.required_key(PREFIX).encode()
        record_key = (_FAKE.clean_prefix(PREFIX) + "activations/2").encode()
        assert fake.store[required_key].value == _FAKE.REQUIRED_VALUE_V2.encode()
        record = act._decode_activation_record(fake.store[record_key].value)
        assert record.from_ == 1 and record.to == 2
        assert record.from_mod_revision == snapshot.mod_revision
        assert record.activation_evidence_sha256 == EVIDENCE
        # ★ 变异:把 record 与 required 拆成两次 txn → mod_revision 不再相等,本条红。
        assert fake.store[required_key].mod_revision == fake.store[record_key].create_revision

        # 同一次 CAS 不可重放:record 是 create-only。
        with pytest.raises(PandoraError):
            await lock.advance_required(
                1, 2, snapshot.mod_revision, _V2_SERVICES, audited, EVIDENCE, 1
            )
    finally:
        await lock.close()


# ── 5. 失租 ⇒ 立刻自 fencing,推进在本地就被拒(不发 CAS)──────────────────
async def test_lease_gone_self_fences_before_issuing_any_cas() -> None:
    client, fake = _client()
    lock = await client.acquire_lock(1)  # ttl=1s → 续约间隔 ~0.33s
    try:
        fake.dead_leases.add(lock._lease_id)  # 服务端从此回 TTL=0
        await asyncio.wait_for(lock.lost.wait(), timeout=3.0)
        # ★ 变异:_declare_lost 只打日志不 self._lost.set() → 这里超时,本条红。
        assert lock.lost_reason == "lease_gone"

        before = fake.txn_calls
        with pytest.raises(PandoraError) as got:
            await lock.advance_required_policy_v3(
                act.RequiredSnapshot(2, 2, _FAKE.REQUIRED_POLICY_V2, _FAKE.REQUIRED_VALUE_V2, 5),
                _V2_SERVICES,
                [],
                EVIDENCE,
                1,
            )
        assert "activation lock lease lost" in got.value.msg
        # ★ 变异:advance_* 里去掉 _ensure_held() → 会真的打出一次 CAS,本条红。
        assert fake.txn_calls == before
    finally:
        await lock.close()


# ── 6. 本地安全线必须走 monotonic,墙钟冻结/回拨不能让它永真 ───────────────
async def test_local_safety_deadline_uses_monotonic_not_wall_clock(monkeypatch) -> None:
    client, fake = _client()
    lease = await fake.lease(1)
    # 不启动续约循环:本条只考"越过本地安全线之后还认不认为自己持有"。
    lock = act.ActivationLock(client=client, lease=lease, token="t" * 32, ttl_sec=1)

    # 墙钟冻死在 0:任何基于 time.time() 的安全线都永远到不了期。
    monkeypatch.setattr(time, "time", lambda: 0.0)
    lock._ensure_held()  # 刚建好,安全窗内

    await asyncio.sleep(0.75)  # ttl=1 → 安全窗 ≈ 0.667s,已越线
    with pytest.raises(PandoraError) as got:
        lock._ensure_held()
    # ★ 变异:__init__ 与 _ensure_held 改用 time.time() → 墙钟冻结下永不越线,本条红。
    assert "activation lock lease lost" in got.value.msg
    assert lock.lost_reason == "deadline_exceeded"


# ── 7. 释放必须精确:只 revoke 自己那把 lease,不碰继任者 ──────────────────
async def test_release_only_revokes_own_lease_and_never_deletes_successor_lock() -> None:
    client, fake = _client()
    lock = await client.acquire_lock(30)
    lock_key = _FAKE.activation_lock_key(PREFIX).encode()
    old_token = fake.store[lock_key].value

    # 模拟本副本失租:etcd 把旧 lease 的 key 收走,继任者随即拿到锁。
    await fake.revoke_lease(lock._lease_id)
    successor = await client.acquire_lock(30)
    assert fake.store[lock_key].value != old_token

    await lock.close()  # 旧持有者姗姗来迟地释放
    # ★ 变异:close() 改成按 key 删激活锁 → 继任者的锁被删,本条红。
    assert lock_key in fake.store
    assert fake.store[lock_key].value == successor.token.encode()
    await successor.close()


# ── 8. digest 校验必须 fullmatch:尾换行不是合法 digest ─────────────────────
def test_digest_check_rejects_trailing_newline_even_with_dollar_anchored_pattern() -> None:
    assert act._digest_ok(DIGEST_A) is True
    # 假件里的 pattern 是 Go 原样的 `^...$`;Python 的 `$` 会匹配"末尾换行之前"。
    assert _FAKE.digest_pattern.match(DIGEST_A + "\n") is not None
    # ★ 变异:_digest_ok 改用 .match() → 本条红(带尾换行的 digest 被判合法)。
    assert act._digest_ok(DIGEST_A + "\n") is False
    with pytest.raises(PandoraError):
        act._validate_activation_evidence_sha256(EVIDENCE + "\n")


# ── 9. genesis token:大写 hex 与内嵌空白必须拒 ─────────────────────────────
def test_genesis_continuity_token_requires_exact_lowercase_hex() -> None:
    act.validate_genesis_continuity_token(NONCE)
    for bad in (
        "nonce:" + "A" * 64,  # bytes.fromhex 接受大写,回环比较必须拒
        "nonce:" + "9" * 62,
        "nonce:" + "9" * 63 + "g",
        "nonce:" + "99 " + "9" * 61,  # bytes.fromhex 接受内嵌空白
        "9" * 64,
    ):
        with pytest.raises(PandoraError):
            # ★ 变异:去掉 `raw.hex() != raw_hex` 回环 → 大写/空白变体被放行,本条红。
            act.validate_genesis_continuity_token(bad)


# ── 10. 审计记录解码:未知字段 / 尾随 JSON / 负数与字符串整数一律拒 ────────
def test_decode_activation_record_is_strict() -> None:
    good = act.ActivationRecord(
        from_=1,
        to=2,
        from_required_value="1",
        to_required_value=_FAKE.REQUIRED_VALUE_V2,
        required_policy_id=_FAKE.REQUIRED_POLICY_V2,
        from_mod_revision=7,
        expected_services_hash="d" * 64,
        activation_evidence_sha256=EVIDENCE,
        activation_evidence_completed_at_ms=1,
        activated_at_ms=2,
    )
    raw = good.to_json_bytes()
    # ★ 变异:to_json_bytes 漏掉 omitempty(把 zero_writer_bootstrap 也写出去)
    #    → 载荷不再与 Go 逐字节一致,本条红。
    assert raw == (
        b'{"from":1,"to":2,"from_required_value":"1",'
        b'"to_required_value":"2@ds-auth-v2-pod-uid-write-invariant-v1",'
        b'"required_policy_id":"ds-auth-v2-pod-uid-write-invariant-v1",'
        b'"from_mod_revision":7,"expected_services_hash":"' + b"d" * 64 + b'",'
        b'"activation_evidence_sha256":"' + EVIDENCE.encode() + b'",'
        b'"activation_evidence_completed_at_ms":1,"activated_at_ms":2}'
    )
    assert act._decode_activation_record(raw) == good

    for bad in (
        raw[:-1] + b',"extra":1}',  # 未知字段
        raw + b'{"from":1}',  # 尾随 JSON
        raw.replace(b'"from":1', b'"from":-1'),  # uint32 收负数
        raw.replace(b'"from":1', b'"from":"1"'),  # 字符串冒充整数
        raw.replace(b'"from":1', b'"from":true'),  # bool 冒充整数
        raw.replace(b'"activated_at_ms":2', b'"activated_at_ms":%d' % (1 << 63)),  # int64 溢出
        b"[]",
    ):
        with pytest.raises(PandoraError):
            act._decode_activation_record(bad)


# ── 11. epoch 证据校验:记录被覆盖过(version!=1)必须拒 ────────────────────
async def test_verify_activation_evidence_rejects_overwritten_record() -> None:
    client, fake = _client()
    await _seed_epoch2_activation(fake)
    await client.verify_activation_evidence(2, EVIDENCE, 1)

    # 攻击者把 JSON 字段原样重写一遍:内容看起来没变,但 version 变成 2。
    record_key = (_FAKE.clean_prefix(PREFIX) + "activations/2").encode()
    fake._put(record_key, fake.store[record_key].value, None)
    with pytest.raises(PandoraError) as got:
        await client.verify_activation_evidence(2, EVIDENCE, 1)
    # ★ 变异:去掉「记录不可变」判定(`version != 1` 与 `mod_revision != create_revision`
    #    在真 etcd 里等价冗余,必须成对删才能造出失败态)→ 被覆盖过的记录被当成
    #    不可变证据放行,本条红。
    assert "is not the immutable required-epoch transaction" in got.value.msg


async def test_verify_activation_evidence_requires_exact_digest_and_completion() -> None:
    client, fake = _client()
    await _seed_epoch2_activation(fake)
    with pytest.raises(PandoraError) as got:
        await client.verify_activation_evidence(2, "sha256:" + "d" * 64, 1)
    assert "evidence mismatch" in got.value.msg
    with pytest.raises(PandoraError) as got:
        await client.verify_activation_evidence(2, EVIDENCE, 2)
    assert "evidence completion mismatch" in got.value.msg
    with pytest.raises(PandoraError):
        await client.verify_activation_evidence(1, EVIDENCE, 1)


# ── 12. capability 列举:坏记录 fail-closed,不跳过;排序按字节 ─────────────
async def test_capabilities_fail_closed_on_missing_lease_and_sort_by_key_bytes() -> None:
    client, fake = _client()
    _put_capability(fake, _capability("ds_allocator", "uid-b"), lease=11)
    _put_capability(fake, _capability("battle_result", "uid-a"), lease=12)
    live = await client.capabilities()
    assert [c.key for c in live] == sorted(c.key for c in live)
    assert live[0].capability.service == "battle_result"

    # 无 lease 的 capability = 没有存活证明,必须让整次审计失败而不是被跳过。
    _put_capability(fake, _capability("login", "uid-c"), lease=0)
    with pytest.raises(PandoraError) as got:
        await client.capabilities()
    # ★ 变异:capabilities() 把 lease==0 的记录 continue 掉 → 本条红。
    assert "has no lease" in got.value.msg


# ── 13. audit findings 文案与排序与 Go 逐字节一致 ──────────────────────────
def test_audit_findings_text_matches_go() -> None:
    policy = act.AuditPolicy(
        prefix=PREFIX,
        required_services={"login": 1},
        target_epoch=2,
        keyset_revision="ks-1",
        expected_digests={"login": DIGEST_A},
        required_features={"login": set()},
    )
    live = act.LiveCapability(
        capability=_capability(
            "login", "uid-1", image_digest=DIGEST_B, keyset_revision="ks-9", features=["extra-feat"]
        ),
        lease_id=0,
        key=_FAKE.capability_key(PREFIX, "login", "uid-1"),
        mod_revision=3,
    )
    findings = act.audit_capabilities([live], policy)
    # ★ 变异:_go_quote 换成 Python 的 repr() → 引号变单引号,本条红。
    assert findings == sorted(findings)
    assert "login/uid-1 无 lease" in findings
    assert 'login/uid-1 image_digest="%s", service expected="%s"' % (DIGEST_B, DIGEST_A) in findings
    assert 'login/uid-1 keyset_revision="ks-9", expected="ks-1"' in findings
    assert "login/uid-1 含未批准 capability feature=extra-feat" in findings
    assert "login/uid-1 image_digest 不在本次激活清单" not in findings

    ok = act.audit_capabilities(
        [
            act.LiveCapability(
                capability=_capability("login", "uid-1"),
                lease_id=7,
                key=_FAKE.capability_key(PREFIX, "login", "uid-1"),
                mod_revision=3,
            )
        ],
        policy,
    )
    assert ok == []


def test_audit_flags_extra_writer_and_key_identity_mismatch() -> None:
    policy = act.AuditPolicy(
        prefix=PREFIX,
        required_services={"login": 1},
        target_epoch=2,
        keyset_revision="ks-1",
        expected_digests={"login": DIGEST_A},
        required_features={"login": set()},
        required_instances={"login": {"uid-1"}},
    )
    live = act.LiveCapability(
        capability=_capability("ds_allocator", "uid-x"),
        lease_id=7,
        key=_FAKE.capability_key(PREFIX, "login", "uid-1"),  # key 与 payload 身份不一致
        mod_revision=3,
    )
    findings = act.audit_capabilities([live], policy)
    assert "capability key 与 payload 身份不一致: %s" % live.key in findings
    assert "发现未在激活清单中的旧/额外 writer ds_allocator=1" in findings
    assert "K8s live Pod login/uid-1 缺 capability lease" in findings
    assert "login capability=0, expected=1" in findings


# ── 14. zero-writer / genesis 路径的"前缀必须为空"是真比较,不是注释 ────────
async def test_zero_writer_v3_advance_refuses_when_any_capability_exists() -> None:
    client, fake = _client()
    await client.bootstrap_required(1)
    snapshot = await client.required_snapshot()
    lock = await _acquire_lock(client)
    try:
        _put_capability(fake, _capability("login", "uid-1"), lease=9)
        with pytest.raises(PandoraError) as got:
            await lock.advance_required_policy_v3_from_zero_writers(snapshot, EVIDENCE, 1)
        # ★ 变异:_build_zero_writer_policy_advance_compares 去掉 capability range 比较
        #    → 有活着的 writer 也能推进,本条红。
        assert "zero-writer required policy CAS failed" in got.value.msg
    finally:
        await lock.close()


async def test_zero_writer_v3_advance_succeeds_on_empty_prefix() -> None:
    client, fake = _client()
    await client.bootstrap_required(1)
    snapshot = await client.required_snapshot()
    lock = await _acquire_lock(client)
    try:
        await lock.advance_required_policy_v3_from_zero_writers(snapshot, EVIDENCE, 1)
        await client.verify_required_policy_v3_activation_record()
        await client.verify_required_policy_v3_activation_evidence(EVIDENCE, 1)
        with pytest.raises(PandoraError):
            await client.verify_required_policy_v3_activation_evidence(EVIDENCE, 2)
    finally:
        await lock.close()


async def test_genesis_bootstrap_requires_sentinel_created_before_record() -> None:
    client, fake = _client()
    await client.prepare_missing_required_policy_v3_continuity(NONCE)
    await client.prepare_missing_required_policy_v3_continuity(NONCE)  # 重入幂等
    lock = await _acquire_lock(client)
    try:
        await lock.bootstrap_required_policy_v3_from_missing(EVIDENCE, 1, NONCE)
        await client.verify_genesis_continuity(NONCE)
        await client.verify_required_policy_v3_activation_evidence_and_continuity(
            EVIDENCE, 1, NONCE
        )
        # 换一个 token 就必须拒 —— 哨兵与 K8s 标记是"双份持有同一随机数"。
        with pytest.raises(PandoraError):
            await client.verify_genesis_continuity("nonce:" + "1" * 64)
    finally:
        await lock.close()


async def test_genesis_prepare_refuses_when_authority_prefix_is_not_empty() -> None:
    client, fake = _client()
    await client.bootstrap_required(1)  # 权威前缀里已经有 required key
    with pytest.raises(PandoraError) as got:
        await client.prepare_missing_required_policy_v3_continuity(NONCE)
    # ★ 变异:去掉 prepare 的 authority-range 比较 → 非空前缀也能建哨兵,本条红。
    assert "genesis continuity prepare CAS failed" in got.value.msg


# ── 15. 激活锁本身:create-only,已被持有就拒,且不留悬空 lease ─────────────
async def test_acquire_lock_is_create_only_and_revokes_lease_on_contention() -> None:
    client, fake = _client()
    first = await client.acquire_lock(30)
    try:
        with pytest.raises(PandoraError) as got:
            await client.acquire_lock(30)
        assert got.value.msg == "activation lock is held"
        # ★ 变异:抢锁失败后不 revoke 新 grant 的 lease → 悬空 lease 积累,本条红。
        assert fake.revoked, "抢锁失败必须把刚 grant 的 lease 收回"
    finally:
        await first.close()


async def test_bootstrap_required_is_baseline_only_and_create_only() -> None:
    client, fake = _client()
    with pytest.raises(PandoraError) as got:
        await client.bootstrap_required(2)
    assert "bootstrap epoch must be immutable baseline 1" in got.value.msg
    await client.bootstrap_required(1)
    with pytest.raises(PandoraError) as got:
        await client.bootstrap_required(1)
    # ★ 变异:bootstrap 用无条件 put 取代 create-only CAS → 会静默覆盖,本条红。
    assert "already exists" in got.value.msg


async def test_required_snapshot_rejects_unparseable_value() -> None:
    client, fake = _client()
    fake._put(_FAKE.required_key(PREFIX).encode(), b"2", None)  # 裸 "2" 是回滚栅栏,必须拒
    with pytest.raises(Exception):
        await client.required_snapshot()
    with pytest.raises(PandoraError) as got:
        await _client()[0].required_snapshot()
    assert got.value.msg == "required epoch missing"


# ── 16. 清单解析:比 Python 内置更严 ───────────────────────────────────────
def test_parse_expected_services_is_stricter_than_int() -> None:
    assert act.parse_expected_services("login=1, hub_allocator=2") == {
        "login": 1,
        "hub_allocator": 2,
    }
    for bad in ("login=1_0", "login= 1", "login=１", "login=0", "login=-1", "login=1=2", "", "=1"):
        with pytest.raises(PandoraError):
            # ★ 变异:_atoi 换成 int() → `1_0`/全角/带空白都被吃下,本条红。
            act.parse_expected_services(bad)
    with pytest.raises(PandoraError) as got:
        act.parse_expected_services("login=1,login=2")
    assert "duplicate expected service" in got.value.msg


def test_parse_expected_digests_and_features_and_instances() -> None:
    assert act.parse_expected_digests("login=%s" % DIGEST_A) == {"login": DIGEST_A}
    for bad in ("login=%s" % DIGEST_A.upper(), "login=sha256:zz", "lo/gin=%s" % DIGEST_A, ""):
        with pytest.raises(PandoraError):
            act.parse_expected_digests(bad)

    assert act.parse_required_features("hub_allocator=hub-owner-cleanup-v1") == {
        "hub_allocator": {"hub-owner-cleanup-v1"}
    }
    assert act.parse_required_features("  ") == {}
    for bad in ("hub_allocator=BAD", "hub_allocator=", "a=x|x", "a=ok-feature,a=ok-feature"):
        with pytest.raises(PandoraError):
            act.parse_required_features(bad)

    assert act.parse_expected_instances("login=u1|u2") == {"login": {"u1", "u2"}}
    for bad in ("login=u1|u1", "login=", "login=a/b", ""):
        with pytest.raises(PandoraError):
            act.parse_expected_instances(bad)


def test_validate_activation_evidence_input_fail_closed_on_target() -> None:
    act.validate_activation_evidence_input(1, 2, "", 0, False)  # legacy 只读审计
    with pytest.raises(PandoraError):
        act.validate_activation_evidence_input(1, 2, "", 0, True)  # 推进必须带证据
    with pytest.raises(PandoraError):
        act.validate_activation_evidence_input(2, 2, "", 0, False)  # 已在目标上同样要证据
    with pytest.raises(PandoraError):
        act.validate_activation_evidence_input(1, 2, EVIDENCE, 0, True)
    act.validate_activation_evidence_input(1, 2, EVIDENCE, 1, True)


# ── 17. 越界整型显式拒 ─────────────────────────────────────────────────────
async def test_integer_bounds_are_checked_explicitly() -> None:
    client, fake = _client()
    lock = await _acquire_lock(client)
    lock._topology_lease_verified = True
    try:
        with pytest.raises(PandoraError) as got:
            await lock.advance_required(1, 1 << 32, 3, {}, [], EVIDENCE, 1)
        # ★ 变异:去掉 _require_u32 → 2**32 一路飘进 CAS,本条红。
        assert "uint32" in got.value.msg
        with pytest.raises(PandoraError) as got:
            await lock.advance_required(1, 2, 1 << 63, {}, [], EVIDENCE, 1)
        assert "int64" in got.value.msg
    finally:
        await lock.close()


# ── 辅助 ────────────────────────────────────────────────────────────────────
_V2_SERVICES = {
    "login": 1,
    "player_locator": 1,
    "ds_allocator": 1,
    "hub_allocator": 1,
    "battle_result": 1,
}


def _put_capability(fake: FakeEtcd, capability, *, lease: int) -> str:
    key = _FAKE.capability_key(PREFIX, capability.service, capability.instance_uid)
    fake._put(key.encode(), capability.to_json_bytes(), lease)
    return key


def _v2_audit_set(fake: FakeEtcd) -> list:
    """按 V2 策略造出完整、合法的一整套 writer capability(每个服务 1 副本)。"""
    out = []
    for index, service in enumerate(_V2_SERVICES):
        capability = _capability(service, "uid-%d" % index)
        key = _put_capability(fake, capability, lease=100 + index)
        out.append(
            act.LiveCapability(
                capability=capability,
                lease_id=100 + index,
                key=key,
                mod_revision=fake.store[key.encode()].mod_revision,
            )
        )
    return out


async def _seed_epoch2_activation(fake: FakeEtcd) -> None:
    """直接铺一份"合法的 epoch-2 激活现场"(required + 同事务不可变记录)。"""
    record = act.ActivationRecord(
        from_=1,
        to=2,
        from_required_value="1",
        to_required_value=_FAKE.REQUIRED_VALUE_V2,
        required_policy_id=_FAKE.REQUIRED_POLICY_V2,
        from_mod_revision=1,
        expected_services_hash=_FAKE.expected_services_hash(_V2_SERVICES),
        activation_evidence_sha256=EVIDENCE,
        activation_evidence_completed_at_ms=1,
        activated_at_ms=2,
    )
    fake._put(b"/pandora/ds-auth/placeholder", b"x", None)  # 抬高 revision,让 from_mod_revision 更早
    del fake.store[b"/pandora/ds-auth/placeholder"]
    required_key = _FAKE.required_key(PREFIX).encode()
    record_key = (_FAKE.clean_prefix(PREFIX) + "activations/2").encode()
    # 同一次事务 = 同一个 revision:这里手工把两个 key 写在同一个 revision 上。
    fake.revision += 1
    rev = fake.revision
    fake.store[required_key] = _Entry(_FAKE.REQUIRED_VALUE_V2.encode(), rev, 0)
    fake.store[record_key] = _Entry(record.to_json_bytes(), rev, 0)
