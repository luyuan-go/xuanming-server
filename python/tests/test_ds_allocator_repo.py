"""ds_allocator 对局仓储层回归测试 —— 覆盖 `pandorapy/services/ds_allocator/repo.py`。

★ 用**真 Redis**(默认 `127.0.0.1:16379`,docker 容器 `pandora-redis`)。
连不上就整体 skip 并说明原因,**不假装通过**:

    docker run -d --name pandora-redis -p 16379:6379 redis:8-alpine

为什么不用 fakeredis:本文件测的恰好是 fake 实现最容易"差不多对"的地方 ——
`WATCH/MULTI/EXEC` 的乐观锁语义、`SET NX` 的 init-only、`SET ... KEEPTTL` 与
`PERSIST` 同事务、`PTTL` 的 `-1/-2` 三态、`ZADD NX` 不覆盖既有 score。
用 fake 测出来的绿色对生产没有信息量。

★ 每个用例用**独立的 match_id 段**;fixture 另外还按 PID 挑一个空的逻辑库,
不与并发跑的 pytest 抢。
"""

from __future__ import annotations

import asyncio
import os
import uuid as _uuid

import pytest
from google.protobuf import unknown_fields as _unknown_fields
from pandora.ds.v1 import allocator_pb2 as dspb

from pandorapy import errcode
from pandorapy.services.ds_allocator import repo as dsrepo

# ── fixture ─────────────────────────────────────────────────────────────────


@pytest.fixture
async def rdb():
    """独占一个空的 Redis 逻辑库;拿不到就 skip(不冲别人的库)。"""
    import redis.asyncio as aioredis

    addr = os.getenv("PANDORA_TEST_REDIS_ADDR", "127.0.0.1:16379")
    host, _, port = addr.rpartition(":")

    client = None
    for i in range(16):
        db = (os.getpid() + i) % 16
        candidate = aioredis.Redis(
            host=host or "127.0.0.1",
            port=int(port),
            db=db,
            decode_responses=False,
            socket_connect_timeout=3,
            socket_timeout=3,
        )
        try:
            await asyncio.wait_for(candidate.ping(), timeout=4)
        except Exception as exc:  # noqa: BLE001
            await candidate.aclose()
            pytest.skip(
                f"Redis 不可用 @ {addr} ({exc}) —— ds_allocator 数据层测试跳过"
                f"(不假装通过)。起一个:docker run -d -p 16379:6379 redis:8-alpine"
            )
        if await candidate.dbsize() == 0:
            client = candidate
            break
        await candidate.aclose()
    if client is None:
        pytest.skip(f"Redis @ {addr} 的 16 个逻辑库都非空 —— 多半有别的 pytest 正在跑")
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


@pytest.fixture
def repo(rdb) -> dsrepo.RedisBattleRepo:  # noqa: ANN001
    return dsrepo.RedisBattleRepo(rdb)


# ── 构造辅助 ────────────────────────────────────────────────────────────────


def _alloc_id() -> str:
    return str(_uuid.uuid4())


def _claim(match_id: int, allocation_id: str, **kw):
    rec = dspb.BattleStorageRecord(
        match_id=match_id,
        allocation_id=allocation_id,
        state="allocating",
        last_heartbeat_ms=1_700_000_000_000,
    )
    for k, v in kw.items():
        setattr(rec, k, v)
    return rec


def _warming(match_id: int, allocation_id: str, **kw):
    """完整 exact 身份的 warming 记录(Model-B finalize 的合法入参)。"""
    rec = dspb.BattleStorageRecord(
        match_id=match_id,
        allocation_id=allocation_id,
        state="warming",
        ds_pod_name="battle-0",
        ds_addr="10.0.0.7:7777",
        gameserver_uid="gs-uid-1",
        pod_uid="pod-uid-1",
        release_track="stable",
        last_heartbeat_ms=1_700_000_000_000,
    )
    for k, v in kw.items():
        setattr(rec, k, v)
    return rec


def _allocation(allocation_id: str, **kw) -> dsrepo.AuthoritativeGameServerAllocation:
    base = {
        "pod_name": "battle-0",
        "addr": "10.0.0.7:7777",
        "instance_uid": "gs-uid-1",
        "pod_uid": "pod-uid-1",
        "instance_epoch": 0,
        "resource_version": "42",
        "allocation_id": allocation_id,
        "release_track": "stable",
        "annotations_present": True,
    }
    base.update(kw)
    return dsrepo.AuthoritativeGameServerAllocation(**base)


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _with_unknown(rec, field_number: int = 900, value: int = 12345):
    """给一条记录塞一个本副本不认识的 varint 字段(模拟滚动升级期的新副本写入)。"""
    payload = rec.SerializeToString() + _varint((field_number << 3) | 0) + _varint(value)
    out = dspb.BattleStorageRecord()
    out.ParseFromString(payload)
    return out


class _ZRemFails:
    """真 Redis 代理,只把 `zrem` 变成故障(派生索引清理失败的故障注入)。"""

    def __init__(self, inner) -> None:  # noqa: ANN001
        self._inner = inner

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    async def zrem(self, *args, **kwargs):  # noqa: ANN002,ANN003
        raise ConnectionError("zrem injected failure")


class _ZAddFails:
    def __init__(self, inner) -> None:  # noqa: ANN001
        self._inner = inner

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    async def zadd(self, *args, **kwargs):  # noqa: ANN002,ANN003
        raise ConnectionError("zadd injected failure")


class _SetNxAlwaysLoses:
    """`SET NX` 恒失败(模拟"另一个副本先抢到了")。"""

    def __init__(self, inner) -> None:  # noqa: ANN001
        self._inner = inner

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    async def set(self, *args, **kwargs):  # noqa: ANN002,ANN003
        if kwargs.get("nx"):
            return None
        return await self._inner.set(*args, **kwargs)


class _ExecResponseLost:
    """EXEC **照常提交**,但把响应丢掉(抛连接错误)—— 模拟"提交与否未知"。"""

    def __init__(self, inner) -> None:  # noqa: ANN001
        self._inner = inner
        self.commits = 0

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    def pipeline(self, transaction: bool = True):  # noqa: FBT001,FBT002
        return _PipeResponseLost(self._inner.pipeline(transaction=transaction), self)


class _PipeResponseLost:
    def __init__(self, pipe, owner: _ExecResponseLost) -> None:  # noqa: ANN001
        self._pipe = pipe
        self._owner = owner

    def __getattr__(self, name: str):
        return getattr(self._pipe, name)

    async def __aenter__(self):
        await self._pipe.__aenter__()
        return self

    async def __aexit__(self, *exc_info):  # noqa: ANN002
        return await self._pipe.__aexit__(*exc_info)

    async def execute(self, *args, **kwargs):  # noqa: ANN002,ANN003
        await self._pipe.execute(*args, **kwargs)
        self._owner.commits += 1
        raise ConnectionError("EXEC response lost")


# ── ① key 模板逐字对齐 Go(两栈并存的硬契约)──────────────────────────────


def test_key_templates_match_go_literally() -> None:
    """★ key 差一个字符 = 两栈各自维护一份"权威",且没有任何运行期信号。

    hashtag 的位置尤其关键:`{match_id}` 必须只包住 id。包多了(把
    `pandora:ds:battle:` 也括进去)会让所有对局落进**同一个** slot;包少了则
    battle / auth 不同 slot,"授权与镜像同事务"直接 CROSSSLOT 报错。
    """
    assert dsrepo.ACTIVE_KEY == "pandora:ds:active"
    assert dsrepo.ALLOCATION_LEDGER_KEY == "pandora:ds:allocation_ledger"
    assert dsrepo.BATTLE_KEY_SCAN_PATTERN == "pandora:ds:battle:{*}"
    assert dsrepo.battle_key(9001) == "pandora:ds:battle:{9001}"
    assert dsrepo.battle_auth_key(9001) == "pandora:ds:auth:{9001}"
    assert dsrepo.battle_key(2**64 - 1) == "pandora:ds:battle:{18446744073709551615}"


def test_battle_state_constants_match_go_literally() -> None:
    """★ `state` 在 proto 里是 **string**(不是 enum),常量只能是字面量。

    抄错一个字母的后果不是"状态名不好看":`allocation_uncertain` 是墓碑态,
    Python 写成别的字符串后,Go 副本读到会落进 `default` 分支 →
    `unknown canonical battle state` → 整轮 active 索引重建中止。
    """
    assert dsrepo.BATTLE_STATE_ALLOCATION_UNCERTAIN == "allocation_uncertain"
    assert (
        dsrepo.BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING
        == "allocation_reconcile_release_pending"
    )
    assert (
        dsrepo.BATTLE_STATE_ALLOCATION_RECONCILE_EMPTY_TOMBSTONE
        == "allocation_reconcile_empty_tombstone"
    )
    assert dsrepo.BATTLE_STATE_PREACTIVE_RELEASE_PENDING == "preactive_release_pending"
    assert dsrepo.BATTLE_STATE_ALLOCATION_ABORT_PENDING == "allocation_abort_pending"


def test_cas_attempts_is_four_not_three() -> None:
    """★ Go 写的是 `for attempt := 0; attempt <= 3`,即**四次**尝试。

    抄成 3 会让"并发重试耗尽"提前一轮触发,而那条错误是 fail-closed 终态:
    高并发下 AllocateBattle 会莫名其妙多出一批 ErrDSAllocationFailed。
    """
    assert dsrepo.BATTLE_CAS_ATTEMPTS == 4


def test_id_keys_reject_out_of_uint64_range() -> None:
    """★ Go 的 uint64 类型免费挡住的东西,Python 必须显式判。

    不判就会拼出 `pandora:ds:battle:{-1}` 这种 Go 侧永远写不出的 key,
    两栈的镜像从此对不上,而两边都不报错。
    """
    for bad in (-1, 2**64, "9001", True):
        with pytest.raises(errcode.PandoraError) as caught:
            dsrepo.battle_key(bad)  # type: ignore[arg-type]
        assert caught.value.code == errcode.ErrInvalidArg


# ── ② 严格解析:`\A..\Z` 与 Go 的 ParseUint 对齐 ────────────────────────────


def test_parse_ids_rejects_trailing_newline() -> None:
    """★ 用 `^...$` 的话 `"12\\n"` 会被判成合法十进制 —— Go 的 `strconv.ParseUint`
    不接受任何空白。active ZSET 的成员是写入方能影响的字节串,这条差异可注入:
    一个 `"12\\n"` 成员在 Go 侧让整轮扫描报错(fail-closed),在 Python 侧却被
    当成 match 12 —— 于是**别人的对局**被判超时并回滚段位。
    """
    # 直接钉住锚点本身:`fullmatch` 会顺手掩盖 `^..$` 的这个坑,所以额外用
    # `search` 语义验一遍 —— 只有 `\A..\Z` 能让它为 None。
    assert dsrepo._DECIMAL_RE.search("12\n") is None, r"锚点必须是 \A..\Z 而不是 ^..$"  # noqa: SLF001
    assert dsrepo._DECIMAL_RE.search("\n12") is None  # noqa: SLF001

    assert dsrepo.parse_ids([b"12", b"9001"]) == [12, 9001]
    for poison in (b"12\n", b" 12", b"+12", b"12 ", b"", b"0x0c", b"-1"):
        with pytest.raises(dsrepo.BattleDataError):
            dsrepo.parse_ids([poison])


def test_parse_ids_rejects_above_uint64() -> None:
    """Python int 无限精度:不显式判上界就会解出一个 Go 侧不可能存在的 match_id。"""
    with pytest.raises(dsrepo.BattleDataError):
        dsrepo.parse_ids([str(2**64).encode()])


def test_parse_battle_id_from_key() -> None:
    assert dsrepo.parse_battle_id_from_key("pandora:ds:battle:{7}") == 7
    for bad in (
        "pandora:ds:battle:7",  # 缺 hashtag
        "pandora:ds:battle:{7",  # 缺右括号
        "pandora:ds:battle:{0}",  # 0 是保留值
        "pandora:ds:battle:{ 7}",  # 空白
        "pandora:hub:shard:{7}",  # 别的域
    ):
        with pytest.raises(dsrepo.BattleDataError):
            dsrepo.parse_battle_id_from_key(bad)


# ── ③ allocation_id 规范性 ──────────────────────────────────────────────────


def test_canonical_allocation_id_requires_canonical_lowercase_v4() -> None:
    """★ 四条判据缺一不可。

    尤其 `str(parsed) == value` 这条回写比较:Python 的 `uuid.UUID()` 和 Go 的
    `uuid.Parse()` 都接受 `{...}` / `urn:uuid:` / 32 位无横线 / 大写,不做回写比较
    的话同一个分配会有 5 种字符串形式 —— 而 Redis 里的 allocation_id 比较是
    **字节比较**,fencing 当场失效。
    """
    good = "3f2504e0-4f89-41d3-9a0c-0305e82c3301"
    assert dsrepo.canonical_battle_allocation_id(good)
    assert dsrepo.canonical_battle_allocation_id(str(_uuid.uuid4()))

    assert not dsrepo.canonical_battle_allocation_id(good.upper()), "大写不是规范形"
    assert not dsrepo.canonical_battle_allocation_id("{" + good + "}")
    assert not dsrepo.canonical_battle_allocation_id("urn:uuid:" + good)
    assert not dsrepo.canonical_battle_allocation_id(good.replace("-", ""))
    assert not dsrepo.canonical_battle_allocation_id("00000000-0000-0000-0000-000000000000")
    assert not dsrepo.canonical_battle_allocation_id(str(_uuid.uuid1())), "v1 可由 MAC 推导"
    assert not dsrepo.canonical_battle_allocation_id("")
    assert not dsrepo.canonical_battle_allocation_id("not-a-uuid")


def test_canonical_identity_value_rejects_space_and_control() -> None:
    """★ 控制字符判定必须是 Cc(= Go 的 `unicode.IsControl` 定义域)。

    写成 `category[0] == "C"` 会把 Cf(如 U+00AD 软连字符)也算进去 —— 于是
    Go 写得进 Redis 的记录,Python 读出来判非法,存量对局一步也走不动。
    """
    assert dsrepo.canonical_battle_identity_value("battle-0")
    assert dsrepo.canonical_battle_identity_value("\u00adx"), "Cf 不是 Cc,Go 也放行"
    for bad in ("", " battle-0", "battle-0 ", "battle 0", "battle\t0", "battle\x00", "\x1fx"):
        assert not dsrepo.canonical_battle_identity_value(bad), bad


# ── ④ 写入侧不变量 ──────────────────────────────────────────────────────────


def test_write_invariant_state_buckets() -> None:
    aid = _alloc_id()
    # 三个"必须空身份"的状态
    for state in (
        "allocating",
        dsrepo.BATTLE_STATE_ALLOCATION_UNCERTAIN,
        dsrepo.BATTLE_STATE_ALLOCATION_RECONCILE_EMPTY_TOMBSTONE,
    ):
        empty = dspb.BattleStorageRecord(match_id=1, allocation_id=aid, state=state)
        dsrepo.validate_battle_storage_write(empty)
        dirty = dspb.BattleStorageRecord(
            match_id=1, allocation_id=aid, state=state, ds_pod_name="battle-0"
        )
        with pytest.raises(dsrepo.BattleDataError):
            dsrepo.validate_battle_storage_write(dirty)

    # warming 必须完整四元组;缺 pod_uid 不行
    dsrepo.validate_battle_storage_write(_warming(1, aid))
    with pytest.raises(dsrepo.BattleDataError):
        dsrepo.validate_battle_storage_write(_warming(1, aid, pod_uid=""))
    with pytest.raises(dsrepo.BattleDataError):
        dsrepo.validate_battle_storage_write(_warming(1, aid, release_track="green"))

    # abandoned:全空 OK,完整 OK,半个不行
    dsrepo.validate_battle_storage_write(
        dspb.BattleStorageRecord(match_id=1, allocation_id=aid, state="abandoned")
    )
    dsrepo.validate_battle_storage_write(_warming(1, aid, state="abandoned"))
    with pytest.raises(dsrepo.BattleDataError):
        dsrepo.validate_battle_storage_write(_warming(1, aid, state="abandoned", pod_uid=""))

    # 未知状态 fail-closed
    with pytest.raises(dsrepo.BattleDataError):
        dsrepo.validate_battle_storage_write(
            dspb.BattleStorageRecord(match_id=1, allocation_id=aid, state="teleporting")
        )


def test_write_invariant_rejects_unknown_fields_on_new_record() -> None:
    """新建记录不得携带 unknown fields —— 那说明它其实是"改写别人的记录"。"""
    rec = _with_unknown(_warming(1, _alloc_id()))
    assert dsrepo.has_unknown_fields(rec)
    with pytest.raises(dsrepo.BattleDataError):
        dsrepo.validate_battle_storage_write(rec)
    # 但作为"已存在记录的形状"是合法的(滚动升级常态)
    dsrepo.validate_existing_battle_storage_shape(rec)


def test_unknown_bytes_roundtrip_and_equality() -> None:
    """★ `unknown_bytes` / `set_unknown_bytes` 是 Go `GetUnknown/SetUnknown` 的替身。

    Python protobuf **没有** SetUnknown,这一对函数是整条滚动升级不变量
    (§9 不变量 17)在 Python 侧的唯一实现;它们错了的表现是
    `validate_battle_storage_transition` 的 unknown 相等判定恒 false ——
    所有状态迁移被拒,一台 DS 都分不出去。
    """
    src = _with_unknown(_warming(1, _alloc_id()))
    raw = dsrepo.unknown_bytes(src)
    assert raw, "构造的未知字段没生效"

    dst = _warming(1, src.allocation_id)
    assert not dsrepo.has_unknown_fields(dst)
    dsrepo.set_unknown_bytes(dst, raw)
    assert dsrepo.has_unknown_fields(dst)
    assert dsrepo.unknown_bytes(dst) == raw
    assert len(_unknown_fields.UnknownFieldSet(dst)) == 1
    assert dsrepo.proto_equal(src, dst)

    # 清空
    dsrepo.set_unknown_bytes(dst, b"")
    assert not dsrepo.has_unknown_fields(dst)
    assert not dsrepo.proto_equal(src, dst), "proto_equal 必须把 unknown 算进去"


def test_transition_immutable_identity() -> None:
    aid = _alloc_id()
    prev = _warming(5, aid)

    # 合法:只改 state
    dsrepo.validate_battle_storage_transition(prev, _warming(5, aid, state="ready"))

    # match / allocation 身份不可变
    with pytest.raises(dsrepo.BattleDataError):
        dsrepo.validate_battle_storage_transition(prev, _warming(6, aid))
    with pytest.raises(dsrepo.BattleDataError):
        dsrepo.validate_battle_storage_transition(prev, _warming(5, _alloc_id()))

    # pod_uid 一旦非空不可变(ABA 闸)
    with pytest.raises(dsrepo.BattleDataError):
        dsrepo.validate_battle_storage_transition(prev, _warming(5, aid, pod_uid="pod-uid-2"))


def test_transition_rejects_unknown_field_loss() -> None:
    """★ §9 不变量 17 的机械检查:回写不得丢掉本副本不认识的字段。"""
    aid = _alloc_id()
    prev = _with_unknown(_warming(5, aid))
    lost = _warming(5, aid, state="ready")  # 没带 unknown
    with pytest.raises(dsrepo.BattleDataError):
        dsrepo.validate_battle_storage_transition(prev, lost)

    kept = _warming(5, aid, state="ready")
    dsrepo.set_unknown_bytes(kept, dsrepo.unknown_bytes(prev))
    dsrepo.validate_battle_storage_transition(prev, kept)


def test_transition_legacy_pod_uid_backfill_only() -> None:
    """pod_uid 落盘前的旧记录:唯一合法写入是**同记录精确回填 pod_uid**。

    ★ legacy 分支必须**先于**常规分支判定 —— 否则旧记录会被
    `validate_existing_battle_storage_shape` 判成"不可写",存量对局既不能推进
    也不能回收。
    """
    aid = _alloc_id()
    legacy = _warming(5, aid, pod_uid="")
    assert dsrepo.legacy_battle_missing_pod_uid(legacy)

    ok = _warming(5, aid, pod_uid="pod-uid-1")
    dsrepo.validate_battle_storage_transition(legacy, ok)

    # 回填的同时改了别的字段 → 拒
    with pytest.raises(dsrepo.BattleDataError):
        dsrepo.validate_battle_storage_transition(
            legacy, _warming(5, aid, pod_uid="pod-uid-1", state="ready")
        )
    # 没回填 pod_uid → 拒
    with pytest.raises(dsrepo.BattleDataError):
        dsrepo.validate_battle_storage_transition(legacy, _warming(5, aid, pod_uid=""))


# ── ⑤ active 索引必需性 ─────────────────────────────────────────────────────


def test_active_index_required_matrix() -> None:
    """★ `abandoned` 依赖 `persistent`:ACK 之后的留档审计记录**不得被复活**
    (复活 = 已结束的对局重新进入心跳超时扫描,反复触发补偿)。
    """
    for state in (
        "allocating",
        "warming",
        "ready",
        "running",
        dsrepo.BATTLE_STATE_ALLOCATION_UNCERTAIN,
        dsrepo.BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING,
        dsrepo.BATTLE_STATE_ALLOCATION_RECONCILE_EMPTY_TOMBSTONE,
        dsrepo.BATTLE_STATE_PREACTIVE_RELEASE_PENDING,
        dsrepo.BATTLE_STATE_ALLOCATION_ABORT_PENDING,
    ):
        assert dsrepo.active_index_required(state, persistent=False) is True, state
    assert dsrepo.active_index_required("abandoned", persistent=True) is True
    assert dsrepo.active_index_required("abandoned", persistent=False) is False
    assert dsrepo.active_index_required("ended", persistent=True) is False
    with pytest.raises(dsrepo.BattleDataError):
        dsrepo.active_index_required("teleporting", persistent=True)


# ── ⑥ claim:幂等重放 / 部分失败 ────────────────────────────────────────────


async def test_claim_battle_is_first_writer_wins(repo, rdb) -> None:  # noqa: ANN001
    """SET NX 让并发的两个副本只有一个拿到分配权 —— §9 不变量 1 的入口。"""
    match_id, aid_a, aid_b = 10_001, _alloc_id(), _alloc_id()
    claimed, existing = await repo.claim_battle(_claim(match_id, aid_a), 120.0)
    assert claimed is True and existing is None
    assert await rdb.zscore(dsrepo.ACTIVE_KEY, str(match_id)) == 1_700_000_000_000

    claimed, existing = await repo.claim_battle(_claim(match_id, aid_b), 120.0)
    assert claimed is False
    assert existing is not None and existing.allocation_id == aid_a, "输家必须看到赢家的 claim"


async def test_claim_battle_concurrent_only_one_wins(repo) -> None:  # noqa: ANN001
    """并发 CAS 竞争:20 个协程同抢一个 match,只能有一个 claimed。"""
    match_id = 10_002
    results = await asyncio.gather(
        *(repo.claim_battle(_claim(match_id, _alloc_id()), 120.0) for _ in range(20)),
        return_exceptions=True,
    )
    winners = [r for r in results if not isinstance(r, BaseException) and r[0]]
    losers = [r for r in results if not isinstance(r, BaseException) and not r[0]]
    assert len(winners) == 1, f"同一 match 出现 {len(winners)} 个分配权 —— 一局两台 DS"
    assert len(losers) + len(winners) == 20, [r for r in results if isinstance(r, BaseException)]


async def test_claim_battle_rejects_malformed_claim(repo) -> None:  # noqa: ANN001
    for bad in (
        _claim(0, _alloc_id()),
        _claim(10_003, ""),
        _claim(10_003, _alloc_id(), state="warming"),
    ):
        with pytest.raises(errcode.PandoraError) as caught:
            await repo.claim_battle(bad, 120.0)
        assert caught.value.code == errcode.ErrInvalidArg


async def test_claim_battle_disappeared_claim_fails_closed(rdb) -> None:  # noqa: ANN001
    """★ SETNX=false 之后 key 恰好过期时**不擅自再抢**。

    再抢会让一次 RPC 内产生两次外部 GSA POST —— 两台 Pod,而调用方只知道一台。
    这个窗口在真 Redis 上不可控,故用"SETNX 恒失败"的代理精确命中该分支。
    """
    match_id = 10_004
    repo = dsrepo.RedisBattleRepo(_SetNxAlwaysLoses(rdb))
    with pytest.raises(errcode.PandoraError) as caught:
        await repo.claim_battle(_claim(match_id, _alloc_id()), 120.0)
    assert caught.value.code == errcode.ErrDSAllocationFailed
    assert await rdb.exists(dsrepo.battle_key(match_id)) == 0


async def test_claim_battle_rolls_back_when_index_write_fails(rdb) -> None:  # noqa: ANN001
    """★ 部分失败:索引登记失败必须撤 claim —— **此刻还没碰过 Agones**,撤是安全的。

    不撤的话那个 allocating key 会卡满整个 BattleTTL(2h),而且 GSA 的未知结果
    永远没人按 allocation_id 对账。
    """
    match_id, aid = 10_005, _alloc_id()
    repo = dsrepo.RedisBattleRepo(_ZAddFails(rdb))
    with pytest.raises(dsrepo.BattleDataError) as caught:
        await repo.claim_battle(_claim(match_id, aid), 120.0)
    assert "claim inflight index" in caught.value.msg
    assert caught.value.code == errcode.ErrUnknown, "Go 的裸 fmt.Errorf 归 ErrUnknown"
    assert await rdb.exists(dsrepo.battle_key(match_id)) == 0, "claim 没有被撤销"


# ── ⑦ fence:GSA POST 前的线性化点 ──────────────────────────────────────────


async def test_fence_battle_allocation_persists_uncertain(repo, rdb) -> None:  # noqa: ANN001
    """★ 状态替换与去 TTL 必须在**同一个 EXEC**:分两步的话崩溃窗口里那条本该
    永久的 fail-closed 墓碑会过期 —— 同一个 match 被允许发第二次 GSA POST。
    """
    match_id, aid = 10_010, _alloc_id()
    await repo.claim_battle(_claim(match_id, aid), 120.0)
    assert 0 < await rdb.pttl(dsrepo.battle_key(match_id)) <= 120_000

    assert await repo.fence_battle_allocation(match_id, aid) is True
    rec = await repo.get_battle(match_id)
    assert rec.state == dsrepo.BATTLE_STATE_ALLOCATION_UNCERTAIN
    assert await rdb.pttl(dsrepo.battle_key(match_id)) == -1, "墓碑必须永不过期"


async def test_fence_battle_allocation_rejects_stale_allocation_id(repo) -> None:  # noqa: ANN001
    """fencing:旧 allocation_id 的调用者拿不到 POST 权,且**零变更**。"""
    match_id, aid = 10_011, _alloc_id()
    await repo.claim_battle(_claim(match_id, aid), 120.0)
    assert await repo.fence_battle_allocation(match_id, _alloc_id()) is False
    assert (await repo.get_battle(match_id)).state == "allocating"


async def test_fence_battle_allocation_is_idempotent_no_op(repo) -> None:  # noqa: ANN001
    """重放:已是 uncertain 时不再匹配 `allocating`,返回 False 且零变更。"""
    match_id, aid = 10_012, _alloc_id()
    await repo.claim_battle(_claim(match_id, aid), 120.0)
    assert await repo.fence_battle_allocation(match_id, aid) is True
    assert await repo.fence_battle_allocation(match_id, aid) is False


async def test_fence_battle_allocation_missing_key_is_false(repo) -> None:  # noqa: ANN001
    assert await repo.fence_battle_allocation(10_013, _alloc_id()) is False


async def test_fence_battle_allocation_requires_args(repo) -> None:  # noqa: ANN001
    for match_id, aid in ((0, _alloc_id()), (10_014, "")):
        with pytest.raises(errcode.PandoraError) as caught:
            await repo.fence_battle_allocation(match_id, aid)
        assert caught.value.code == errcode.ErrInvalidArg


# ── ⑧ finalize ──────────────────────────────────────────────────────────────


async def test_finalize_from_allocating_sets_ttl(repo, rdb) -> None:  # noqa: ANN001
    match_id, aid = 10_020, _alloc_id()
    await repo.claim_battle(_claim(match_id, aid), 120.0)
    assert await repo.finalize_battle_allocation(_warming(match_id, aid), 300.0) is True
    assert (await repo.get_battle(match_id)).state == "warming"
    assert 200_000 < await rdb.pttl(dsrepo.battle_key(match_id)) <= 300_000


async def test_fenced_finalize_refuses_to_skip_uncertain(repo) -> None:  # noqa: ANN001
    """★ Model-B 的 finalize **拒绝**从 allocating 直跳 warming。

    跳过去等于在"严格 UID/RV 确认"之前就把 claim 变成可路由镜像 —— 一台身份
    未经确认的 DS 会开始接玩家。
    """
    match_id, aid = 10_021, _alloc_id()
    await repo.claim_battle(_claim(match_id, aid), 120.0)
    assert await repo.finalize_fenced_battle_allocation(_warming(match_id, aid), 300.0) is False
    assert (await repo.get_battle(match_id)).state == "allocating"


async def test_fenced_finalize_keeps_record_persistent(repo, rdb) -> None:  # noqa: ANN001
    match_id, aid = 10_022, _alloc_id()
    await repo.claim_battle(_claim(match_id, aid), 120.0)
    await repo.fence_battle_allocation(match_id, aid)
    assert await repo.finalize_fenced_battle_allocation(_warming(match_id, aid), 300.0) is True
    assert await rdb.pttl(dsrepo.battle_key(match_id)) == -1, "激活前必须保持永久"
    assert await rdb.zscore(dsrepo.ACTIVE_KEY, str(match_id)) == 1_700_000_000_000


async def test_fenced_finalize_requires_complete_identity(repo) -> None:  # noqa: ANN001
    """★ epoch 必须仍为 0(Model-B finalize 早于 PrepareCredential 赋 epoch),
    且未来做精确 release 所需的每一项外部身份都必须已持久。
    """
    match_id, aid = 10_023, _alloc_id()
    for kw in (
        {"ds_addr": ""},
        {"gameserver_uid": ""},
        {"pod_uid": ""},
        {"instance_epoch": 3},
        {"release_track": ""},
    ):
        with pytest.raises(errcode.PandoraError) as caught:
            await repo.finalize_fenced_battle_allocation(_warming(match_id, aid, **kw), 300.0)
        assert caught.value.code == errcode.ErrInvalidArg, kw


async def test_finalize_rejects_stale_allocation_id(repo) -> None:  # noqa: ANN001
    """fencing 旧 epoch 被拒:另一次分配的 finalize **绝不覆盖当前赢家**。"""
    match_id, aid = 10_024, _alloc_id()
    await repo.claim_battle(_claim(match_id, aid), 120.0)
    assert await repo.finalize_battle_allocation(_warming(match_id, _alloc_id()), 300.0) is False
    assert (await repo.get_battle(match_id)).state == "allocating"


async def test_finalize_preserves_unknown_fields_from_authority(repo, rdb) -> None:  # noqa: ANN001
    """★ finalize 是 read-modify-write:必须用 WATCH 内刚读到的权威 unknown fields
    覆盖调用方快照(§9 不变量 17)。不覆盖的话,滚动升级期本副本每 finalize 一次
    就把新副本写下的新字段静默抹掉一批。
    """
    match_id, aid = 10_025, _alloc_id()
    claim = _with_unknown(_claim(match_id, aid), field_number=901, value=777)
    await rdb.set(dsrepo.battle_key(match_id), claim.SerializeToString(), px=120_000)
    await rdb.zadd(dsrepo.ACTIVE_KEY, {str(match_id): float(claim.last_heartbeat_ms)})

    assert await repo.finalize_battle_allocation(_warming(match_id, aid), 300.0) is True
    after = await repo.get_battle(match_id)
    assert after.state == "warming"
    assert dsrepo.unknown_bytes(after) == dsrepo.unknown_bytes(claim), "未来字段被抹掉了"


async def test_fenced_finalize_recovers_from_lost_exec_response(rdb) -> None:  # noqa: ANN001
    """★ 部分失败:EXEC 已提交但响应丢失 → 严格 read-back 确认后仍算成功。

    把它当"仍未 finalize"直接返回失败,调用方就会停止凭据投递,而 Redis 里那条
    warming 已经是 GSA 生命周期 fence —— 一台已分配的 DS 永远等不到凭据。
    """
    match_id, aid = 10_026, _alloc_id()
    base = dsrepo.RedisBattleRepo(rdb)
    await base.claim_battle(_claim(match_id, aid), 120.0)
    await base.fence_battle_allocation(match_id, aid)

    flaky = _ExecResponseLost(rdb)
    repo = dsrepo.RedisBattleRepo(flaky)
    assert await repo.finalize_fenced_battle_allocation(_warming(match_id, aid), 300.0) is True
    assert flaky.commits == 1, "read-back 分支不该重发第二次 EXEC"
    assert (await base.get_battle(match_id)).state == "warming"
    assert await rdb.pttl(dsrepo.battle_key(match_id)) == -1


async def test_legacy_finalize_does_not_read_back(rdb) -> None:  # noqa: ANN001
    """★ read-back 只属于 persistent 档。非 persistent 路径的 EXEC 故障必须**原样
    上抛** —— 那条记录带 TTL,靠 read-back 认成功会把"有限 TTL 的旧 writer 提交"
    误认成自己的。
    """
    match_id, aid = 10_027, _alloc_id()
    base = dsrepo.RedisBattleRepo(rdb)
    await base.claim_battle(_claim(match_id, aid), 120.0)
    repo = dsrepo.RedisBattleRepo(_ExecResponseLost(rdb))
    with pytest.raises(ConnectionError):
        await repo.finalize_battle_allocation(_warming(match_id, aid), 300.0)


# ── ⑨ fencing delete ────────────────────────────────────────────────────────


async def test_delete_if_allocation_matches_whitelist(repo, rdb) -> None:  # noqa: ANN001
    """★ 只允许 allocating / warming / abandoned;`allocation_uncertain` 与任何
    未知状态一律 fail-closed —— 删掉 uncertain 墓碑 = 放行第二次 GSA POST。
    """
    match_id, aid = 10_030, _alloc_id()
    await repo.claim_battle(_claim(match_id, aid), 120.0)
    await repo.fence_battle_allocation(match_id, aid)
    assert await repo.delete_battle_if_allocation_matches(match_id, aid, "") is False
    assert await rdb.exists(dsrepo.battle_key(match_id)) == 1

    await repo.finalize_fenced_battle_allocation(_warming(match_id, aid), 300.0)
    assert await repo.delete_battle_if_allocation_matches(match_id, aid, "battle-0") is True
    assert await rdb.exists(dsrepo.battle_key(match_id)) == 0
    assert await rdb.zscore(dsrepo.ACTIVE_KEY, str(match_id)) is None


async def test_delete_if_allocation_matches_rejects_wrong_pod(repo, rdb) -> None:  # noqa: ANN001
    match_id, aid = 10_031, _alloc_id()
    await repo.claim_battle(_claim(match_id, aid), 120.0)
    await repo.fence_battle_allocation(match_id, aid)
    await repo.finalize_fenced_battle_allocation(_warming(match_id, aid), 300.0)
    assert await repo.delete_battle_if_allocation_matches(match_id, aid, "battle-9") is False
    assert await repo.delete_battle_if_allocation_matches(match_id, _alloc_id(), "") is False
    assert await rdb.exists(dsrepo.battle_key(match_id)) == 1


async def test_delete_if_allocation_matches_missing_key_is_false(repo) -> None:  # noqa: ANN001
    assert await repo.delete_battle_if_allocation_matches(10_032, _alloc_id(), "") is False


async def test_delete_index_failure_still_reports_delete_right(rdb) -> None:  # noqa: ANN001
    """★ Go 的 `return true, err` 在 Python 里只能挂到异常上。

    `deleted=True` 是**释放对应 GameServer 的权利**:调用方丢了它就不会去
    Release,那台 14Gi 的 Pod 一直挂着,而权威 key 已经删了 —— 再也没人能证明
    它属于谁。证据用声明式 `__slots__`,不用 setattr(拼错不报错)。
    """
    match_id, aid = 10_033, _alloc_id()
    base = dsrepo.RedisBattleRepo(rdb)
    await base.claim_battle(_claim(match_id, aid), 120.0)

    repo = dsrepo.RedisBattleRepo(_ZRemFails(rdb))
    with pytest.raises(dsrepo.BattleActiveIndexError) as caught:
        await repo.delete_battle_if_allocation_matches(match_id, aid, "")
    assert caught.value.deleted is True
    assert "deleted" in dsrepo.BattleActiveIndexError.__slots__
    assert await rdb.exists(dsrepo.battle_key(match_id)) == 0, "权威删除本身必须已生效"


# ── ⑩ CRUD ──────────────────────────────────────────────────────────────────


async def test_create_battle_refuses_overwrite(repo) -> None:  # noqa: ANN001
    """★ `SET NX` 而不是 `SET`:覆盖会把一局正在打的对局的 roster / 身份整体换掉。"""
    match_id, aid = 10_040, _alloc_id()
    await repo.create_battle(_warming(match_id, aid), 300.0)
    with pytest.raises(errcode.PandoraError) as caught:
        await repo.create_battle(_warming(match_id, _alloc_id()), 300.0)
    assert caught.value.code == errcode.ErrDSAllocationFailed
    assert (await repo.get_battle(match_id)).allocation_id == aid


async def test_get_battle_missing_returns_none(repo) -> None:  # noqa: ANN001
    assert await repo.get_battle(10_041) is None


async def test_get_battle_rejects_id_mismatch(repo, rdb) -> None:  # noqa: ANN001
    """★ 记录内 id 与 key 不符时**报错**,不是"以 key 为准改掉它"。

    无论信哪一边,另一边的账本都会错 —— 只能拒,让上层去查。
    """
    other = _warming(999, _alloc_id())
    await rdb.set(dsrepo.battle_key(10_042), other.SerializeToString())
    with pytest.raises(dsrepo.BattleDataError):
        await repo.get_battle(10_042)


async def test_get_battle_backfills_zero_match_id(repo, rdb) -> None:  # noqa: ANN001
    """match_id 为 0 的早期记录用 key 里的 id 补齐(兼容,不报错)。"""
    legacy = _warming(10_043, _alloc_id())
    legacy.match_id = 0
    await rdb.set(dsrepo.battle_key(10_043), legacy.SerializeToString())
    assert (await repo.get_battle(10_043)).match_id == 10_043


async def test_update_with_lock_refreshes_ttl_and_active(repo, rdb) -> None:  # noqa: ANN001
    match_id, aid = 10_044, _alloc_id()
    await repo.create_battle(_warming(match_id, aid), 100.0)

    def _bump(rec) -> None:  # noqa: ANN001
        rec.state = "ready"
        rec.last_heartbeat_ms = 1_700_000_009_000

    await repo.update_battle_with_lock(match_id, 3, _bump, 300.0)
    assert (await repo.get_battle(match_id)).state == "ready"
    assert 200_000 < await rdb.pttl(dsrepo.battle_key(match_id)) <= 300_000
    assert await rdb.zscore(dsrepo.ACTIVE_KEY, str(match_id)) == 1_700_000_009_000


async def test_update_keep_ttl_does_not_extend_compensation_window(repo, rdb) -> None:  # noqa: ANN001
    """★ 补偿重试路径必须保留原 TTL。

    刷 TTL 的话,一个永远补偿不成功的对局会**永久**留在 Redis 里:每轮 sweep
    重试都把 BattleTTL 续满,`GetBattle` 永远 hit,active 永远清不掉。
    """
    match_id, aid = 10_045, _alloc_id()
    await repo.create_battle(_warming(match_id, aid), 100.0)
    before = await rdb.pttl(dsrepo.battle_key(match_id))

    def _abandon(rec) -> None:  # noqa: ANN001
        rec.state = "abandoned"

    await repo.update_battle_keep_ttl(match_id, 3, _abandon)
    after = await rdb.pttl(dsrepo.battle_key(match_id))
    assert (await repo.get_battle(match_id)).state == "abandoned"
    assert 0 < after <= before, f"TTL 被刷新了:{before} -> {after}"


async def test_update_with_lock_missing_battle_fails_fast(repo) -> None:  # noqa: ANN001
    """★ 镜像不存在时**立即冒泡**,不消耗 max_retry —— 重试一个不存在的键只是
    把同一个结论重复 N 次,还把预算从"并发冲突"挪走。
    """
    calls = 0

    def _fn(rec) -> None:  # noqa: ANN001
        nonlocal calls
        calls += 1

    with pytest.raises(errcode.PandoraError) as caught:
        await repo.update_battle_with_lock(10_046, 5, _fn, 300.0)
    assert caught.value.code == errcode.ErrDSPodNotFound
    assert calls == 0


async def test_update_with_lock_business_error_is_not_retried(repo) -> None:  # noqa: ANN001
    """★ fn 抛的业务错误必须与 WATCH 冲突**分开**处理。

    混在一起会把它吞成"重试 N 次后 CAS 耗尽",线上看到的原因是错的。
    """
    match_id, aid = 10_047, _alloc_id()
    await repo.create_battle(_warming(match_id, aid), 300.0)
    calls = 0

    def _fn(rec) -> None:  # noqa: ANN001
        nonlocal calls
        calls += 1
        raise errcode.PandoraError(errcode.ErrInvalidState, "battle already ended")

    with pytest.raises(errcode.PandoraError) as caught:
        await repo.update_battle_with_lock(match_id, 5, _fn, 300.0)
    assert caught.value.code == errcode.ErrInvalidState
    assert calls == 1, "业务错误被当成 CAS 冲突重试了"


async def test_update_with_lock_reruns_fn_on_cas_conflict(repo, rdb) -> None:  # noqa: ANN001
    """★ CAS 冲突时 fn **基于重新 GET 的最新镜像整体重跑**。

    这正是 sweep 的 `firstAbandon` 防 double-release 所依赖的语义:状态迁移
    X→Y 全局只有一个 EXEC 能成功,输家重跑后读到 Y 就不再置位。
    """
    match_id, aid = 10_048, _alloc_id()
    await repo.create_battle(_warming(match_id, aid), 300.0)
    seen_states: list[str] = []

    async def _fn(rec) -> None:  # noqa: ANN001
        seen_states.append(rec.state)
        if len(seen_states) == 1:
            # 在 WATCH 窗口内做一次带外写,必然让本轮 EXEC 失败
            other = _warming(match_id, aid, state="running")
            await rdb.set(dsrepo.battle_key(match_id), other.SerializeToString())
        rec.player_count = 5

    await repo.update_battle_with_lock(match_id, 3, _fn, 300.0)
    assert seen_states == ["warming", "running"], seen_states
    assert (await repo.get_battle(match_id)).player_count == 5


async def test_update_with_lock_retry_exhausted(repo, rdb) -> None:  # noqa: ANN001
    match_id, aid = 10_049, _alloc_id()
    await repo.create_battle(_warming(match_id, aid), 300.0)

    async def _fn(rec) -> None:  # noqa: ANN001
        await rdb.set(dsrepo.battle_key(match_id), _warming(match_id, aid).SerializeToString())
        rec.player_count = 1

    with pytest.raises(errcode.PandoraError) as caught:
        await repo.update_battle_with_lock(match_id, 2, _fn, 300.0)
    assert caught.value.code == errcode.ErrDSAllocationFailed


async def test_expire_battle_sub_second_ttl_is_not_immediate_delete(repo, rdb) -> None:  # noqa: ANN001
    """★ go-redis 的 `formatSec` 把 `0<d<1s` **抬成 1 秒**;Python 直接 `int()`
    会得到 0,而 `EXPIRE key 0` 是**立即删除** —— `ExpireBattle` 的语义
    ("改短 TTL,终态保留供查询")当场反转成"抹掉终态"。
    """
    assert dsrepo._format_sec(0.4) == 1  # noqa: SLF001
    assert dsrepo._format_sec(90.9) == 90  # noqa: SLF001

    match_id, aid = 10_050, _alloc_id()
    await repo.create_battle(_warming(match_id, aid), 300.0)
    await repo.expire_battle(match_id, 0.4)
    assert await rdb.exists(dsrepo.battle_key(match_id)) == 1, "终态被立即抹掉了"
    assert await rdb.zscore(dsrepo.ACTIVE_KEY, str(match_id)) is None


async def test_delete_and_remove_active(repo, rdb) -> None:  # noqa: ANN001
    match_id, aid = 10_051, _alloc_id()
    await repo.create_battle(_warming(match_id, aid), 300.0)
    await repo.touch_active(match_id, 1_700_000_010_000)
    assert await rdb.zscore(dsrepo.ACTIVE_KEY, str(match_id)) == 1_700_000_010_000
    await repo.remove_active(match_id)
    assert await rdb.zscore(dsrepo.ACTIVE_KEY, str(match_id)) is None
    await repo.delete_battle(match_id)
    assert await repo.get_battle(match_id) is None


# ── ⑪ 超时扫描的边界 ────────────────────────────────────────────────────────


async def test_range_stale_battles_is_inclusive_at_threshold(repo, rdb) -> None:  # noqa: ANN001
    """★ 阈值是**闭区间**(`last_heartbeat_ms <= threshold`)。

    写成开区间的话,恰好卡在阈值上的那一局永远不会被判超时 —— §9 不变量 4 的
    "15s 超时 → abandoned → 段位回滚"对它永久失效。
    """
    await rdb.zadd(
        dsrepo.ACTIVE_KEY,
        {"7001": 1_000.0, "7002": 2_000.0, "7003": 3_000.0},
    )
    assert await repo.range_stale_battles(2_000) == [7001, 7002]
    assert await repo.range_stale_battles(999) == []
    assert sorted(await repo.range_active_battles()) == [7001, 7002, 7003]


async def test_range_rejects_out_of_int64_threshold(repo) -> None:  # noqa: ANN001
    with pytest.raises(errcode.PandoraError) as caught:
        await repo.range_stale_battles(2**63)
    assert caught.value.code == errcode.ErrInvalidArg


async def test_range_active_battles_rejects_poisoned_member(repo, rdb) -> None:  # noqa: ANN001
    """★ 脏成员**整轮报错**而不是跳过(与 hub 侧的"跳过"刻意相反)。

    这里的返回值直接决定"哪些对局要被回收",跳过一个就是漏扫,而漏扫等价于
    "崩溃的 DS 永远不补偿"。
    """
    await rdb.zadd(dsrepo.ACTIVE_KEY, {"7004": 1.0, "70\n05": 2.0})
    with pytest.raises(dsrepo.BattleDataError):
        await repo.range_active_battles()


# ── ⑫ uncertain 对账三件套 ─────────────────────────────────────────────────


async def test_uncertain_release_full_lifecycle(repo, rdb) -> None:  # noqa: ANN001
    """fence(捕获 exact 身份)→ complete(终态)→ 空结果墓碑,全程永不过期。"""
    match_id, aid = 10_060, _alloc_id()
    await repo.claim_battle(_claim(match_id, aid), 120.0)
    await repo.fence_battle_allocation(match_id, aid)

    assert await repo.fence_allocation_uncertain_release(match_id, aid, _allocation(aid)) is True
    rec = await repo.get_battle(match_id)
    assert rec.state == dsrepo.BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING
    assert (rec.ds_pod_name, rec.gameserver_uid, rec.pod_uid) == (
        "battle-0",
        "gs-uid-1",
        "pod-uid-1",
    )
    assert await rdb.pttl(dsrepo.battle_key(match_id)) == -1

    # 幂等重放:同一 tuple 再来一次仍返回 True 且零变更
    assert await repo.fence_allocation_uncertain_release(match_id, aid, _allocation(aid)) is True

    assert await repo.complete_allocation_uncertain_release(match_id, aid, "gs-uid-1") is True
    assert (await repo.get_battle(match_id)).state == "abandoned"
    # 幂等重放
    assert await repo.complete_allocation_uncertain_release(match_id, aid, "gs-uid-1") is True


async def test_uncertain_release_tuple_conflict_is_fail_closed(repo) -> None:  # noqa: ANN001
    """★ 已 fence 过的 tuple 与本次对账结果不一致 → **报错**,不是"反正已经是
    pending 了就算成功"。不一致说明查到了另一个 GameServer,继续走会删错对象。
    """
    match_id, aid = 10_061, _alloc_id()
    await repo.claim_battle(_claim(match_id, aid), 120.0)
    await repo.fence_battle_allocation(match_id, aid)
    await repo.fence_allocation_uncertain_release(match_id, aid, _allocation(aid))

    with pytest.raises(errcode.PandoraError) as caught:
        await repo.fence_allocation_uncertain_release(
            match_id, aid, _allocation(aid, instance_uid="gs-uid-2")
        )
    assert caught.value.code == errcode.ErrInvalidState


async def test_uncertain_release_requires_complete_allocation(repo) -> None:  # noqa: ANN001
    """★ `instance_epoch == 0` 是判据而不是笔误:非 0 说明凭据已建立,
    这条记录不再归准入前对账器管。
    """
    aid = _alloc_id()
    for kw in (
        {"pod_name": ""},
        {"instance_uid": ""},
        {"pod_uid": ""},
        {"resource_version": ""},
        {"instance_epoch": 1},
        {"release_track": "green"},
    ):
        with pytest.raises(errcode.PandoraError) as caught:
            await repo.fence_allocation_uncertain_release(10_062, aid, _allocation(aid, **kw))
        assert caught.value.code == errcode.ErrInvalidArg, kw
    with pytest.raises(errcode.PandoraError):
        await repo.fence_allocation_uncertain_release(10_062, aid, None)


async def test_uncertain_paths_refuse_once_credential_authority_exists(repo, rdb) -> None:  # noqa: ANN001
    """★ auth 键一旦存在,生命周期就不归准入前对账器管了。

    它继续推进 = 把一台**正在服务玩家**的 DS 判成待回收。
    """
    match_id, aid = 10_063, _alloc_id()
    await repo.claim_battle(_claim(match_id, aid), 120.0)
    await repo.fence_battle_allocation(match_id, aid)
    await rdb.set(dsrepo.battle_auth_key(match_id), b"credential")

    for coro in (
        repo.fence_allocation_uncertain_release(match_id, aid, _allocation(aid)),
        repo.complete_allocation_uncertain_release(match_id, aid, ""),
        repo.mark_allocation_uncertain_empty_lifecycle_published(match_id, aid),
    ):
        with pytest.raises(errcode.PandoraError) as caught:
            await coro
        assert caught.value.code == errcode.ErrInvalidState


async def test_empty_allocation_tombstone_keeps_cleanup_authority(repo, rdb) -> None:  # noqa: ANN001
    """权威空结果:complete 走无身份分支,随后 Kafka ACK 只换状态、**不放弃**
    allocation_id 的清理权(超时的 POST 仍可能在空 LIST 之后生效)。
    """
    match_id, aid = 10_064, _alloc_id()
    await repo.claim_battle(_claim(match_id, aid), 120.0)
    await repo.fence_battle_allocation(match_id, aid)

    assert await repo.complete_allocation_uncertain_release(match_id, aid, "") is True
    assert (await repo.get_battle(match_id)).state == "abandoned"
    assert await repo.mark_allocation_uncertain_empty_lifecycle_published(match_id, aid) is True
    assert (
        await repo.get_battle(match_id)
    ).state == dsrepo.BATTLE_STATE_ALLOCATION_RECONCILE_EMPTY_TOMBSTONE
    assert await rdb.pttl(dsrepo.battle_key(match_id)) == -1
    # 幂等重放
    assert await repo.mark_allocation_uncertain_empty_lifecycle_published(match_id, aid) is True


async def test_uncertain_tombstone_rejects_non_persistent_key(repo, rdb) -> None:  # noqa: ANN001
    """★ `PTTL != -1 → 报错` 是"墓碑永不过期"的机械检查。

    放宽成"pttl > 0 才报错"就等于允许一条会过期的墓碑存在 —— 过期之后同一个
    match 被允许发第二次 GSA POST。
    """
    match_id, aid = 10_065, _alloc_id()
    await repo.claim_battle(_claim(match_id, aid), 120.0)
    await repo.fence_battle_allocation(match_id, aid)
    await repo.complete_allocation_uncertain_release(match_id, aid, "")
    await repo.mark_allocation_uncertain_empty_lifecycle_published(match_id, aid)
    await rdb.pexpire(dsrepo.battle_key(match_id), 60_000)  # 人为给墓碑加 TTL

    with pytest.raises(errcode.PandoraError) as caught:
        await repo.mark_allocation_uncertain_empty_lifecycle_published(match_id, aid)
    assert caught.value.code == errcode.ErrInvalidState


async def test_uncertain_paths_ignore_other_allocation(repo) -> None:  # noqa: ANN001
    match_id, aid = 10_066, _alloc_id()
    await repo.claim_battle(_claim(match_id, aid), 120.0)
    await repo.fence_battle_allocation(match_id, aid)
    other = _alloc_id()
    assert (
        await repo.fence_allocation_uncertain_release(match_id, other, _allocation(other)) is False
    )
    assert await repo.complete_allocation_uncertain_release(match_id, other, "") is False
    assert (await repo.get_battle(match_id)).state == dsrepo.BATTLE_STATE_ALLOCATION_UNCERTAIN


# ── ⑬ active 索引重建 ───────────────────────────────────────────────────────


async def test_reconcile_rebuilds_index_and_keeps_newer_score(repo, rdb) -> None:  # noqa: ANN001
    """★ `ZADD NX`:已有 score 是**更新的**心跳事实,重建器不得把它拍回记录里
    那个可能陈旧的 last_heartbeat_ms —— 拍回去 = 一局活着的对局被判超时。
    """
    running = _warming(10_070, _alloc_id(), state="running", last_heartbeat_ms=1_000)
    ended = _warming(10_071, _alloc_id(), state="ended")
    abandoned_persistent = _warming(10_072, _alloc_id(), state="abandoned")
    abandoned_ttl = _warming(10_073, _alloc_id(), state="abandoned")

    await rdb.set(dsrepo.battle_key(10_070), running.SerializeToString())
    await rdb.set(dsrepo.battle_key(10_071), ended.SerializeToString())
    await rdb.set(dsrepo.battle_key(10_072), abandoned_persistent.SerializeToString())
    await rdb.set(dsrepo.battle_key(10_073), abandoned_ttl.SerializeToString(), px=300_000)
    # 已存在的、更新的心跳事实
    await rdb.zadd(dsrepo.ACTIVE_KEY, {"10070": 9_999.0})

    await repo.reconcile_battle_active_index(64)

    assert await rdb.zscore(dsrepo.ACTIVE_KEY, "10070") == 9_999.0, "ZADD NX 覆盖了更新的心跳"
    assert await rdb.zscore(dsrepo.ACTIVE_KEY, "10071") is None, "ended 不该进索引"
    assert await rdb.zscore(dsrepo.ACTIVE_KEY, "10072") is not None, "永久 abandoned 必须在索引里"
    assert await rdb.zscore(dsrepo.ACTIVE_KEY, "10073") is None, "已 ACK 的留档审计不得被复活"


async def test_reconcile_fails_closed_on_unknown_state(repo, rdb) -> None:  # noqa: ANN001
    """★ 不认识的状态说明写者比本副本新;漏建索引的后果是那局永远没人推进,
    所以整轮**报错**而不是"当作不需要"。
    """
    weird = _warming(10_074, _alloc_id(), state="teleporting")
    await rdb.set(dsrepo.battle_key(10_074), weird.SerializeToString())
    with pytest.raises(dsrepo.BattleDataError):
        await repo.reconcile_battle_active_index(64)


# ── ⑭ 分配台账 ──────────────────────────────────────────────────────────────


async def test_allocation_ledger_binds_authority_to_deletion(repo) -> None:  # noqa: ANN001
    """★ 台账为空 ⇒ 一台 GS 都删不掉(fail-closed)。

    这是"配置漂移 / failover 到空实例的副本机械化全删载人 Pod"那条 P0 的闸:
    读到健康但空的 Redis 时,`RangeActiveBattles` 会成功返回空集而非报错,
    只有台账能把"我读的权威"与"我要删的 GS"绑定起来。
    """
    aid = _alloc_id()
    assert await repo.allocation_ledger_contains(aid) is False
    assert await repo.allocation_ledger_contains("") is False

    await repo.record_allocation_ledger(aid, 1_700_000_000_000)
    assert await repo.allocation_ledger_contains(aid) is True
    # 幂等:重复 ZADD 只刷 score
    await repo.record_allocation_ledger(aid, 1_700_000_001_000)
    assert await repo.allocation_ledger_contains(aid) is True

    await repo.record_allocation_ledger("", 1)  # 空 id 静默 no-op
    assert await repo.prune_allocation_ledger(1_700_000_000_999) == 0, "score 已被刷新"
    assert await repo.prune_allocation_ledger(1_700_000_001_000) == 1
    assert await repo.allocation_ledger_contains(aid) is False


# ── ⑮ no-show 记账 ─────────────────────────────────────────────────────────


async def test_no_show_recorder_keys_and_counters(rdb) -> None:  # noqa: ANN001
    """★ key 必须与 matchmaker 的读者(`NoShowPenaltyRemaining`)完全一致 ——
    两端都经 `redisx.rl_key` 构造。差一个字符的表现是"罚记上了但没人读到"。
    """
    rec = dsrepo.RedisNoShowRecorder(rdb)
    player_id = 123_456

    count, exc = await rec.record_no_show(player_id, 600.0)
    assert exc is None and count == 1
    count, exc = await rec.record_no_show(player_id, 600.0)
    assert exc is None and count == 2
    assert await rdb.exists("pandora:rl:match:noshow:123456") == 1

    assert await rec.arm_penalty(player_id, 30.0) is None
    assert 0 < await rdb.pttl("pandora:rl:match:noshowcd:123456") <= 30_000

    # 新罚**覆盖**旧罚剩余(不是取较大值)
    assert await rec.arm_penalty(player_id, 5.0) is None
    assert 0 < await rdb.pttl("pandora:rl:match:noshowcd:123456") <= 5_000


async def test_no_show_recorder_rejects_bad_player_id(rdb) -> None:  # noqa: ANN001
    rec = dsrepo.RedisNoShowRecorder(rdb)
    with pytest.raises(errcode.PandoraError):
        await rec.record_no_show(-1, 600.0)
    with pytest.raises(errcode.PandoraError):
        await rec.arm_penalty(2**64, 30.0)


# ── ⑯ 严格 Model-B 写档 ─────────────────────────────────────────────────────


async def test_strict_model_b_writes_enforce_invariants(repo) -> None:  # noqa: ANN001
    """严格档开启后,任何缺 exact 身份的写入在**序列化阶段**就被拒。"""
    assert repo.strict_model_b_writes_enabled() is False
    await repo.create_battle(_warming(10_080, _alloc_id(), pod_uid=""), 300.0)  # 宽松档放行

    repo.enable_strict_model_b_writes()
    assert repo.strict_model_b_writes_enabled() is True
    with pytest.raises(dsrepo.BattleDataError):
        await repo.create_battle(_warming(10_081, _alloc_id(), pod_uid=""), 300.0)
    with pytest.raises(dsrepo.BattleDataError):
        await repo.create_battle(_warming(10_082, "not-a-uuid"), 300.0)
    await repo.create_battle(_warming(10_083, _alloc_id()), 300.0)
