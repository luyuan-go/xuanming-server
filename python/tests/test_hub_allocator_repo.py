"""hub_allocator 数据层回归测试 —— 覆盖 repo.py / writer_fence.py / locator_client.py。

★ 用**真 Redis**(默认 `127.0.0.1:16379`,docker 容器 `pandora-redis`)。
连不上就整体 skip 并说明原因,**不假装通过**:

    docker run -d --name pandora-redis -p 16379:6379 redis:8-alpine

为什么不用 fakeredis:本文件测的是 WATCH/MULTI/EXEC 的**乐观锁语义**、
`SET NX` 的 init-only 语义、`PTTL` 的 -1/-2 三态。这些恰好是 fake 实现最容易
"差不多对"的地方 —— 用 fake 测出来的绿色对生产没有信息量。

★ 每个用例用**独立的 pod 名 / player_id 段**,避免跨用例污染
(fixture 另外还按 PID 挑一个空的逻辑库,不与并发跑的 pytest 抢)。
"""

from __future__ import annotations

import asyncio
import os

import grpc
import pytest
from pandora.common.v1 import errcode_pb2
from pandora.hub.v1 import allocator_pb2
from pandora.locator.v1 import locator_pb2

from pandorapy import errcode
from pandorapy.services.hub_allocator import locator_client as lc
from pandorapy.services.hub_allocator import repo as hubrepo
from pandorapy.services.hub_allocator import writer_fence as wfence

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
                f"Redis 不可用 @ {addr} ({exc}) —— hub_allocator 数据层测试跳过"
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


class FakeFence:
    """写者租约替身。★ 返回顺序照 `pandorapy.writerlease.Lease`:`(held, token)`。"""

    def __init__(self, token: int, held: bool = True) -> None:
        self.token = token
        self.held = held

    def current(self) -> tuple[bool, int]:
        return self.held, self.token


def _shard(pod: str, **kw):
    rec = allocator_pb2.HubShardStorageRecord(hub_pod_name=pod, capacity=500)
    for k, v in kw.items():
        setattr(rec, k, v)
    return rec


def _assignment(player_id: int, pod: str, **kw):
    rec = allocator_pb2.HubAssignmentStorageRecord(player_id=player_id, hub_pod_name=pod)
    for k, v in kw.items():
        setattr(rec, k, v)
    return rec


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


# ── ① key 模板逐字对齐 Go(两栈并存的硬契约)──────────────────────────────


def test_key_templates_match_go_literally() -> None:
    """★ key 差一个字符 = 两栈各自维护一份"权威",且没有任何运行期信号。

    hashtag 的位置尤其关键:`{pod}` 必须只包住 pod 名,包多了(比如把
    `pandora:hub:shard:` 也括进去)会让所有 pod 落进**同一个** slot,
    Redis Cluster 上整个 hub 域退化成单 slot 热点;包少了则 shard / members /
    wfence 不同 slot,同 slot 事务直接 CROSSSLOT 报错。
    """
    assert hubrepo.SHARDS_SET_KEY == "pandora:hub:shards"
    assert hubrepo.ACTIVE_KEY == "pandora:hub:active"
    assert hubrepo.TRANSFER_CLEANUP_PODS_KEY == "pandora:hub:transfer_cleanup:pods"
    assert hubrepo.shard_key("hub-0") == "pandora:hub:shard:{hub-0}"
    assert hubrepo.members_key("hub-0") == "pandora:hub:shard:members:{hub-0}"
    assert hubrepo.transfer_cleanup_key("hub-0") == "pandora:hub:transfer_cleanup:{hub-0}"
    assert hubrepo.assign_key(42) == "pandora:hub:player:42"
    assert hubrepo.team_key(7) == "pandora:hub:team:7"
    assert hubrepo.transfer_cooldown_key(42) == "pandora:hub:transfer_cd:42"
    assert wfence.wfence_key("hub-0") == "pandora:hub:wfence:{hub-0}"


def test_id_keys_reject_out_of_uint64_range() -> None:
    """★ Go 的 uint64 类型免费挡住的东西,Python 必须显式判。

    不判就会拼出 `pandora:hub:player:-1` 这种 Go 侧永远写不出的 key,
    两栈的归属从此对不上,而两边都不报错。
    """
    for bad in (-1, 1 << 64):
        with pytest.raises(errcode.PandoraError) as ei:
            hubrepo.assign_key(bad)
        assert ei.value.code == errcode.ErrInvalidArg


# ── ② fence 水位值的严格解析 ────────────────────────────────────────────────


def test_parse_fence_value_is_strict_like_go_parseuint() -> None:
    """★ `int()` 会照收 Go 的 ParseUint 拒绝的三类脏值,每类都有独立后果:

        "-1"      负水位恒小于任何 token → 水位形同虚设
        "5\\n"     `^...$` 写法会放行(Python 的 `$` 匹配末尾换行)
        巨大整数   恒大于任何 token → 该 pod 所有写永久被拒(静默不可写)
    """
    assert wfence.parse_fence_value("p", None) == 0
    assert wfence.parse_fence_value("p", b"7") == 7
    for bad in (b"-1", b"5\n", b" 5", b"+5", b"5.0", b"abc"):
        with pytest.raises(errcode.PandoraError):
            wfence.parse_fence_value("p", bad)
    with pytest.raises(errcode.PandoraError):
        wfence.parse_fence_value("p", str(1 << 64).encode())


def test_fence_snapshot_is_the_only_unpack_point() -> None:
    """★ Go 是 `(token, held)`、Python 是 `(held, token)` —— 抄反了 fence 静默失效。"""
    assert wfence.fence_snapshot(None) == (True, 0)
    assert wfence.fence_snapshot(FakeFence(9)) == (True, 9)
    assert wfence.fence_snapshot(FakeFence(9, held=False)) == (False, 9)


# ── ③ transfer cleanup ref 编解码 ───────────────────────────────────────────


def test_transfer_cleanup_ref_validation() -> None:
    """★ 三条判据缺一不可(见 TransferCleanupRef.valid 的 docstring)。"""
    assert hubrepo.TransferCleanupRef(1, "a-1").valid()
    assert not hubrepo.TransferCleanupRef(0, "a-1").valid()
    assert not hubrepo.TransferCleanupRef(1, "  ").valid()
    # 含冒号会让 decode 截断出一个**不同的** assignment_id 去清理。
    assert not hubrepo.TransferCleanupRef(1, "a:1").valid()


def test_transfer_cleanup_ref_roundtrip_and_dirty_members() -> None:
    ref = hubrepo.TransferCleanupRef(1234, "assign-abc")
    encoded = hubrepo.encode_transfer_cleanup_ref(ref)
    assert encoded == "1234:assign-abc"
    assert hubrepo.decode_transfer_cleanup_ref(encoded) == ref
    # ★ 非十进制 / 缺冒号 / 空 assignment / 负 player_id 全部按脏成员丢弃。
    for bad in (b"1234", b"1234:", b"abc:x", b"-1:x", b":x"):
        assert hubrepo.decode_transfer_cleanup_ref(bad) is None


# ── ④ 心跳状态机(纯函数)────────────────────────────────────────────────────


def test_heartbeat_state_machine_matches_go() -> None:
    """★ 四条分支各自防一种"闸被冲掉"。"""
    # warming + 无 drain 上报 → ready
    rec = _shard("p", state="warming")
    hubrepo.apply_heartbeat_state_to_shard(rec, "", 100)
    assert rec.state == "ready" and rec.last_heartbeat_ms == 100
    # warming + DS 首跳已报 draining → 采纳上报,不强行 ready
    rec = _shard("p", state="warming")
    hubrepo.apply_heartbeat_state_to_shard(rec, "draining", 100)
    assert rec.state == "draining"
    # 空上报不动状态
    rec = _shard("p", state="draining", draining_since_ms=5)
    hubrepo.apply_heartbeat_state_to_shard(rec, "", 100)
    assert rec.state == "draining"
    # ★ 强制整合排空的 draining(draining_since_ms>0)是 sticky 的,ready 冲不掉
    rec = _shard("p", state="draining", draining_since_ms=5)
    hubrepo.apply_heartbeat_state_to_shard(rec, "ready", 100)
    assert rec.state == "draining"
    # ★ 心跳超时误标的 draining(draining_since_ms==0)被健康心跳复位
    rec = _shard("p", state="draining", draining_since_ms=0)
    hubrepo.apply_heartbeat_state_to_shard(rec, "ready", 100)
    assert rec.state == "ready"
    # drain 升级
    rec = _shard("p", state="draining", draining_since_ms=5)
    hubrepo.apply_heartbeat_state_to_shard(rec, "stopping", 100)
    assert rec.state == "stopping"


# ── ⑤ 分片镜像 CRUD ─────────────────────────────────────────────────────────


async def test_create_shard_is_init_only(rdb) -> None:
    """★ CE7:`SET NX` 只初始化,已存在**绝不覆盖**。

    两个并发的 get_shard-miss 种子调用若互相覆盖,后到的那个会把先写入的
    心跳 / last_verified / 状态清回初始值 —— 分片看起来"刚建好",
    于是超时扫描立刻把它判成从未心跳。
    """
    pod = "t5-hub-a"
    repo = hubrepo.RedisHubRepo(rdb)
    await repo.create_shard(_shard(pod, state="warming"), 60.0)
    await repo.update_shard_with_lock(
        pod, 3, lambda r: setattr(r, "last_heartbeat_ms", 9999), 60.0
    )
    # 第二次种子:必须不覆盖
    await repo.create_shard(_shard(pod, state="warming"), 60.0)
    rec = await repo.get_shard(pod)
    assert rec.last_heartbeat_ms == 9999
    assert (await rdb.smembers(hubrepo.SHARDS_SET_KEY)) == {pod.encode()}


async def test_list_shards_self_heals_stale_membership(rdb) -> None:
    """★ 镜像已过期但 SET 残留 → 顺手 SREM。不清的话索引随 Pod 轮换单调膨胀。"""
    repo = hubrepo.RedisHubRepo(rdb)
    await repo.create_shard(_shard("t6-alive"), 60.0)
    await rdb.sadd(hubrepo.SHARDS_SET_KEY, "t6-ghost")
    shards = await repo.list_shards()
    assert [s.hub_pod_name for s in shards] == ["t6-alive"]
    assert (await rdb.smembers(hubrepo.SHARDS_SET_KEY)) == {b"t6-alive"}


async def test_unmarshal_shard_rejects_pod_mismatch(rdb) -> None:
    """★ 记录内 pod 与 key 不符必须**拒**,不能"以 key 为准"改掉它。

    信任任何一边,另一边的账本都会错;只能拒,让上层去查。
    """
    await rdb.set(hubrepo.shard_key("t7-a"), _shard("t7-b").SerializeToString())
    repo = hubrepo.RedisHubRepo(rdb)
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.get_shard("t7-a")
    assert "pod mismatch" in ei.value.msg


async def test_update_shard_with_lock_propagates_fn_error_without_writing(rdb) -> None:
    """★ fn 抛的异常必须与 WatchError 分开:混在一起会变成"重试 N 次后 CAS 耗尽",
    线上看到的原因是错的,而且**不写**这一点也会被掩盖。
    """
    pod = "t8-hub"
    repo = hubrepo.RedisHubRepo(rdb)
    await repo.create_shard(_shard(pod, state="warming"), 60.0)
    sentinel = errcode.PandoraError(errcode.ErrInvalidState, "no change")

    def _fn(rec):  # noqa: ANN001
        rec.state = "stopping"
        raise sentinel

    with pytest.raises(errcode.PandoraError) as ei:
        await repo.update_shard_with_lock(pod, 3, _fn, 60.0)
    assert ei.value is sentinel
    assert (await repo.get_shard(pod)).state == "warming"


async def test_update_shard_with_lock_missing_shard(rdb) -> None:
    repo = hubrepo.RedisHubRepo(rdb)
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.update_shard_with_lock("t9-nope", 2, lambda r: None, 60.0)
    assert ei.value.code == errcode.ErrHubNoAvailable


# ── ⑥ 心跳:代际门 fail-closed ──────────────────────────────────────────────


async def test_heartbeat_orphan_shard_creates_nothing(rdb) -> None:
    """孤儿 DS:分片不存在 → 返回 False 且**不建档**(由 biz 回 stop)。"""
    repo = hubrepo.RedisHubRepo(rdb)
    assert await repo.heartbeat_shard("t10-ghost", 3, "ready", 111, 0, False, 60.0) is False
    assert await rdb.exists(hubrepo.shard_key("t10-ghost")) == 0
    assert await rdb.zcard(hubrepo.ACTIVE_KEY) == 0


async def test_heartbeat_stale_generation_changes_nothing(rdb) -> None:
    """★ 审核 P1:代际校验必须在**任何镜像变更之前**。

    过期代际的心跳一旦被放过一点点(哪怕只刷 last_heartbeat_ms / TTL),
    一台已被换代的旧 DS 就能靠心跳**保活并伪造在场** —— 超时扫描永远不会
    把它标成 draining,它占着的座位也永远回不来。
    """
    pod = "t11-hub"
    repo = hubrepo.RedisHubRepo(rdb)
    await repo.create_shard(
        _shard(pod, state="warming", current_token_gen=7, last_heartbeat_ms=100), 60.0
    )
    with pytest.raises(hubrepo.ShardTokenStaleError) as ei:
        await repo.heartbeat_shard(pod, 5, "ready", 999, 6, True, 60.0)
    assert ei.value.code == errcode.ErrUnauthorized

    rec = await repo.get_shard(pod)
    assert rec.state == "warming"  # 没被翻成 ready
    assert rec.last_heartbeat_ms == 100  # 没被刷新
    assert rec.player_count == 0  # 没被写入
    assert await rdb.zcard(hubrepo.ACTIVE_KEY) == 0  # 没进 active 索引


async def test_heartbeat_without_generation_is_stale_when_required(rdb) -> None:
    """★ 第二种 stale:enforce 代际门开着、心跳却不带代际(legacy gen0 绕行)。

    只判"镜像有代际且不等"会漏掉它:一台从不带 gen 的旧 DS 打到一个
    current_token_gen 尚未写入(=0)的分片上,两条判据都不成立 → 被放行。
    """
    pod = "t12-hub"
    repo = hubrepo.RedisHubRepo(rdb)
    await repo.create_shard(_shard(pod, state="warming", current_token_gen=0), 60.0)
    with pytest.raises(hubrepo.ShardTokenStaleError):
        await repo.heartbeat_shard(pod, 1, "ready", 222, 0, True, 60.0)
    # 关掉代际门则放行(dev / 未启用 enforce 的部署行为不变)
    assert await repo.heartbeat_shard(pod, 1, "ready", 222, 0, False, 60.0) is True


async def test_heartbeat_happy_path_updates_indexes(rdb) -> None:
    pod = "t13-hub"
    repo = hubrepo.RedisHubRepo(rdb)
    await repo.create_shard(_shard(pod, state="warming", current_token_gen=7), 60.0)
    assert await repo.heartbeat_shard(pod, 4, "ready", 333, 7, True, 60.0) is True
    rec = await repo.get_shard(pod)
    assert (rec.state, rec.player_count, rec.last_heartbeat_ms) == ("ready", 4, 333)
    assert await rdb.zscore(hubrepo.ACTIVE_KEY, pod) == 333.0


async def test_range_stale_shards_excludes_never_heartbeat_seeds(rdb) -> None:
    """★ `(0` 是开区间:排除 score=0 的 Mock 种子。

    写成闭区间会让每一轮扫描都把刚建档、尚未首跳的分片判成心跳超时 ——
    新 Pod 永远起不来。
    """
    repo = hubrepo.RedisHubRepo(rdb)
    await rdb.zadd(hubrepo.ACTIVE_KEY, {"t14-seed": 0, "t14-old": 100, "t14-new": 900})
    assert await repo.range_stale_shards(500) == ["t14-old"]
    await repo.remove_active("t14-old")
    assert await repo.range_stale_shards(500) == []


# ── ⑦ 归属 CAS(无 fence)────────────────────────────────────────────────────


async def test_assignment_cas_create_and_mismatch(rdb) -> None:
    """expected=None 只在键不存在时创建;前置快照不符一律零写入。"""
    repo = hubrepo.RedisHubRepo(rdb)
    pid = 1500001
    first = _assignment(pid, "t15-a", assignment_id="a1")
    assert await repo.compare_and_swap_assignment(pid, None, first, 60.0) is True
    # 键已存在 → expected=None 的创建必须失败
    assert await repo.compare_and_swap_assignment(pid, None, _assignment(pid, "t15-b"), 60.0) is (
        False
    )
    assert (await repo.get_assignment(pid)).hub_pod_name == "t15-a"
    # 前置快照相符 → 覆盖成功
    second = _assignment(pid, "t15-b", assignment_id="a2")
    assert await repo.compare_and_swap_assignment(pid, first, second, 60.0) is True
    assert (await repo.get_assignment(pid)).hub_pod_name == "t15-b"
    # 用过期快照再 CAS → 零写入
    assert await repo.compare_and_swap_assignment(pid, first, _assignment(pid, "t15-c"), 60.0) is (
        False
    )
    assert (await repo.get_assignment(pid)).hub_pod_name == "t15-b"


async def test_assignment_cas_rejects_player_id_mismatch(rdb) -> None:
    repo = hubrepo.RedisHubRepo(rdb)
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.compare_and_swap_assignment(1600001, None, _assignment(1600002, "x"), 60.0)
    assert ei.value.code == errcode.ErrInvalidArg


async def test_assignment_cas_compares_unknown_fields(rdb) -> None:
    """★ §9 不变量 17:比较必须覆盖 unknown fields。

    滚动升级期新副本写了新字段,旧副本读出来是 unknown。若比较漏掉它们,
    旧副本会判定"和我的 expected 相等"并覆盖写回 —— 新字段被**静默抹掉**,
    而 CAS 报告成功。
    """
    repo = hubrepo.RedisHubRepo(rdb)
    pid = 1700001
    rec = _assignment(pid, "t17-a", assignment_id="a1")
    assert await repo.compare_and_swap_assignment(pid, None, rec, 60.0) is True
    # 模拟"更新的副本"追加了一个本副本不认识的字段(field 200 varint)
    raw = await rdb.get(hubrepo.assign_key(pid))
    await rdb.set(hubrepo.assign_key(pid), raw + _varint(200 << 3) + _varint(7))

    stale_expected = _assignment(pid, "t17-a", assignment_id="a1")
    assert await repo.compare_and_swap_assignment(
        pid, stale_expected, _assignment(pid, "t17-b"), 60.0
    ) is False
    # 未知字段仍在(没有被抹掉)
    assert await rdb.get(hubrepo.assign_key(pid)) == raw + _varint(200 << 3) + _varint(7)


async def test_delete_assignment_only_when_pod_matches(rdb) -> None:
    """★ 删除必须带前置校验:并发 Assign/Transfer 已写入新归属时不能误删。"""
    repo = hubrepo.RedisHubRepo(rdb)
    pid = 1800001
    await repo.compare_and_swap_assignment(pid, None, _assignment(pid, "t18-a"), 60.0)
    assert await repo.delete_assignment_if_pod_matches(pid, "t18-other") is False
    assert (await repo.get_assignment(pid)) is not None
    assert await repo.delete_assignment_if_pod_matches(pid, "t18-a") is True
    assert (await repo.get_assignment(pid)) is None
    # 幂等:已不存在
    assert await repo.delete_assignment_if_pod_matches(pid, "t18-a") is False


# ── ⑧ 每玩家写者水位(fence 开)────────────────────────────────────────────


async def test_assignment_rejects_higher_writer_token(rdb) -> None:
    """★ 覆盖边界 ⑤:记录已被更高代写者触碰 → 本副本永久出局(零写入)。

    并且**证据不能丢** —— 冲突时的当前记录挂在异常的声明式 slots 上,
    调用方不必再打一次 GET(少一个 TOCTOU 窗口)。
    """
    repo = hubrepo.RedisHubRepo(rdb)
    repo.set_writer_fence(FakeFence(5))
    pid = 1900001
    await rdb.set(
        hubrepo.assign_key(pid),
        _assignment(pid, "t19-a", writer_token=9).SerializeToString(),
    )
    with pytest.raises(wfence.WriterSupersededError) as ei:
        await repo.get_assignment(pid)
    assert ei.value.code == errcode.ErrUnavailable

    with pytest.raises(wfence.WriterSupersededError) as ei2:
        await repo.compare_and_swap_assignment(
            pid, _assignment(pid, "t19-a", writer_token=9), _assignment(pid, "t19-b"), 60.0
        )
    assert ei2.value.current_record is not None
    assert ei2.value.current_record.writer_token == 9
    # 零写入
    rec = allocator_pb2.HubAssignmentStorageRecord()
    rec.ParseFromString(await rdb.get(hubrepo.assign_key(pid)))
    assert rec.hub_pod_name == "t19-a"


async def test_zero_writer_token_is_no_watermark_not_lowest(rdb) -> None:
    """★ `writer_token == 0` = "**尚无水位**",必须放行(滚动升级双向兼容)。

    把 0 当成"最小 token"参与比较是同一段代码最容易写反的地方:方向反了以后
    所有本字段上线前写的历史归属在任何非零任期下都被判成"旧",在场玩家的归属
    集体消失。
    """
    repo = hubrepo.RedisHubRepo(rdb)
    repo.set_writer_fence(FakeFence(5))
    pid = 2000001
    legacy = _assignment(pid, "t20-a")  # writer_token 默认 0
    await rdb.set(hubrepo.assign_key(pid), legacy.SerializeToString())
    got = await repo.get_assignment(pid)
    assert got is not None and got.hub_pod_name == "t20-a"
    # 本届首次 CAS 会原子把水位升到自己的 token
    assert await repo.compare_and_swap_assignment(pid, legacy, _assignment(pid, "t20-b"), 60.0)
    after = await repo.get_assignment(pid)
    assert after.hub_pod_name == "t20-b" and after.writer_token == 5


async def test_delete_writes_tombstone_and_blocks_resurrection(rdb) -> None:
    """★ 覆盖边界 ⑤a:删除写**墓碑**而非裸 DEL。

    裸 DEL 把水位随业务记录一起抹掉,于继任者「创建 → 合法删除」之后,
    失主旧写者看到键不存在就能用旧 token 重建归属(借尸还魂)——
    玩家被拉回一台已经不该有他的 Hub。
    """
    pid = 2100001
    successor = hubrepo.RedisHubRepo(rdb)
    successor.set_writer_fence(FakeFence(9))
    await successor.compare_and_swap_assignment(pid, None, _assignment(pid, "t21-a"), 60.0)
    assert await successor.delete_assignment_if_pod_matches(pid, "t21-a") is True

    # 业务上不可见,但键还在(墓碑),水位=9
    assert await successor.get_assignment(pid) is None
    tomb_raw = await rdb.get(hubrepo.assign_key(pid))
    assert tomb_raw is not None
    tomb = allocator_pb2.HubAssignmentStorageRecord()
    tomb.ParseFromString(tomb_raw)
    assert wfence.is_assignment_fence_tombstone(tomb)
    assert tomb.writer_token == 9
    assert tomb.assignment_id != ""  # ★ 每次删除独立身份,防 tombstone ABA
    # 墓碑必须有 TTL(持久化会无界增长,§9.24)
    assert 0 < await rdb.pttl(hubrepo.assign_key(pid)) <= int(
        wfence.ASSIGNMENT_FENCE_TOMBSTONE_TTL_SEC * 1000
    )

    # 失主旧写者(token=5)想在墓碑上重建 → 被水位拒绝
    loser = hubrepo.RedisHubRepo(rdb)
    loser.set_writer_fence(FakeFence(5))
    with pytest.raises(wfence.WriterSupersededError):
        await loser.compare_and_swap_assignment(pid, None, _assignment(pid, "t21-b"), 60.0)
    assert await successor.get_assignment(pid) is None


async def test_set_assignment_is_fail_closed_under_fence(rdb) -> None:
    """★ 无条件 Set 是纯粹的 fencing 旁路(无 WATCH 无比较)→ 启用 fence 后必须拒。"""
    repo = hubrepo.RedisHubRepo(rdb)
    repo.set_writer_fence(FakeFence(3))
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.set_assignment(_assignment(2200001, "t22-a"), 60.0)
    assert ei.value.code == errcode.ErrInternal
    assert await rdb.exists(hubrepo.assign_key(2200001)) == 0
    # 未启用 fence 时保持旧行为(dev / 单副本)
    plain = hubrepo.RedisHubRepo(rdb)
    await plain.set_assignment(_assignment(2200001, "t22-a"), 60.0)
    assert (await plain.get_assignment(2200001)).hub_pod_name == "t22-a"


async def test_writer_who_lost_lease_cannot_write(rdb) -> None:
    """★ `held=False` 一律 fail-closed,连读到的水位都不必看。"""
    repo = hubrepo.RedisHubRepo(rdb)
    repo.set_writer_fence(FakeFence(5, held=False))
    with pytest.raises(wfence.WriterSupersededError):
        await repo.compare_and_swap_assignment(2300001, None, _assignment(2300001, "x"), 60.0)
    with pytest.raises(wfence.WriterSupersededError):
        await repo.add_shard_member("t23-hub", 2300001, 60.0)
    with pytest.raises(wfence.WriterSupersededError):
        await repo.set_team_shard(2300001, "t23-hub", 60.0)


# ── ⑨ per-{pod} 水位事务 ────────────────────────────────────────────────────


async def test_fenced_pod_tx_advances_watermark_and_rejects_stale_writer(rdb) -> None:
    """★ 逐 slot 懒推进:继任者第一次写某 slot 起,前任在该 slot 永久被拒。"""
    pod = "t24-hub"
    successor = hubrepo.RedisHubRepo(rdb)
    successor.set_writer_fence(FakeFence(9))
    await successor.add_shard_member(pod, 2400001, 60.0)
    assert await rdb.get(wfence.wfence_key(pod)) == b"9"
    # 水位键必须**持久**(不设 TTL):删除即复位会给借尸还魂开门
    assert await rdb.pttl(wfence.wfence_key(pod)) == -1

    loser = hubrepo.RedisHubRepo(rdb)
    loser.set_writer_fence(FakeFence(5))
    with pytest.raises(wfence.WriterSupersededError):
        await loser.add_shard_member(pod, 2400002, 60.0)
    assert await rdb.smembers(hubrepo.members_key(pod)) == {b"2400001"}


async def test_shard_members_persist_when_ttl_not_positive(rdb) -> None:
    """★ `ttl<=0` → PERSIST,不是"用默认 TTL"。

    长连玩家的成员索引一旦过期就从 drain 枚举里消失,强制整合搬不动他们。
    """
    pod = "t25-hub"
    repo = hubrepo.RedisHubRepo(rdb)
    await repo.add_shard_member(pod, 2500001, 60.0)
    assert await rdb.pttl(hubrepo.members_key(pod)) > 0
    await repo.add_shard_member(pod, 2500002, 0)
    assert await rdb.pttl(hubrepo.members_key(pod)) == -1
    assert sorted(await repo.list_shard_members(pod)) == [2500001, 2500002]
    # 脏成员跳过,不炸
    await rdb.sadd(hubrepo.members_key(pod), b"not-a-number")
    assert sorted(await repo.list_shard_members(pod)) == [2500001, 2500002]
    await repo.remove_shard_member(pod, 2500001)
    assert await repo.list_shard_members(pod) == [2500002]


async def test_remove_shard_clears_indexes_but_not_watermark(rdb) -> None:
    """★ fence key 故意**不随分片删除** —— 水位必须比业务记录长寿。"""
    pod = "t26-hub"
    repo = hubrepo.RedisHubRepo(rdb)
    repo.set_writer_fence(FakeFence(4))
    await repo.create_shard(_shard(pod), 60.0)
    await repo.add_shard_member(pod, 2600001, 60.0)
    await rdb.zadd(hubrepo.ACTIVE_KEY, {pod: 100})

    await repo.remove_shard(pod)
    assert await rdb.exists(hubrepo.shard_key(pod)) == 0
    assert await rdb.exists(hubrepo.members_key(pod)) == 0
    assert await rdb.sismember(hubrepo.SHARDS_SET_KEY, pod) == 0
    assert await rdb.zscore(hubrepo.ACTIVE_KEY, pod) is None
    assert await rdb.get(wfence.wfence_key(pod)) == b"4"


# ── ⑩ 继任者水位推扫(接流前硬门)──────────────────────────────────────────


async def test_advance_writer_fences_covers_shards_and_cleanup_pods(rdb) -> None:
    """★ 两个来源都要枚举。

    只扫分片 SET 会漏掉「分片已删、cleanup saga 还挂着」的 pod ——
    前任在那些 slot 上仍然可写,而推扫报告成功。
    """
    repo = hubrepo.RedisHubRepo(rdb)
    repo.set_writer_fence(FakeFence(3))
    await repo.create_shard(_shard("t27-shard"), 60.0)
    await repo.register_transfer_cleanup(
        "t27-cleanup", hubrepo.TransferCleanupRef(2700001, "a1")
    )

    successor = hubrepo.RedisHubRepo(rdb)
    await successor.advance_writer_fences_for_token(11)
    assert await rdb.get(wfence.wfence_key("t27-shard")) == b"11"
    assert await rdb.get(wfence.wfence_key("t27-cleanup")) == b"11"


async def test_advance_writer_fences_is_monotonic_and_idempotent(rdb) -> None:
    """★ 幂等只进不退;遇到更大 token(自己已被继任)立即 fail-closed。"""
    repo = hubrepo.RedisHubRepo(rdb)
    repo.set_writer_fence(FakeFence(6))
    await repo.create_shard(_shard("t28-hub"), 60.0)
    await repo.advance_writer_fences()
    await repo.advance_writer_fences()  # 幂等
    assert await rdb.get(wfence.wfence_key("t28-hub")) == b"6"

    await rdb.set(wfence.wfence_key("t28-hub"), b"99")
    with pytest.raises(wfence.WriterSupersededError):
        await repo.advance_writer_fences()
    assert await rdb.get(wfence.wfence_key("t28-hub")) == b"99"


async def test_advance_for_token_requires_non_zero(rdb) -> None:
    """★ token=0 = "没有任期",拿它推扫等于把所有 slot 的水位钉在 0(等于没有水位)。"""
    repo = hubrepo.RedisHubRepo(rdb)
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.advance_writer_fences_for_token(0)
    assert ei.value.code == errcode.ErrInvalidArg


async def test_advance_for_token_does_not_need_held_lease(rdb) -> None:
    """★ 接流前硬门:此时 `current()` 故意还不返回 held,推扫**不能**依赖写权。

    反过来依赖就是循环依赖 —— 激活钩子永远跑不起来,副本永远不可写。
    """
    repo = hubrepo.RedisHubRepo(rdb)
    repo.set_writer_fence(FakeFence(7, held=False))
    await rdb.sadd(hubrepo.SHARDS_SET_KEY, "t29-hub")
    await repo.advance_writer_fences_for_token(7)
    assert await rdb.get(wfence.wfence_key("t29-hub")) == b"7"


# ── ⑪ transfer cleanup / 冷却 ───────────────────────────────────────────────


async def test_transfer_cleanup_registers_global_index_first(rdb) -> None:
    """★ 全局 pod 索引必须先写:反过来会出现「ref 在、reconciler 扫不到这个 pod」
    的永久漏扫,旧 owner 的 seat 永远没人退。
    """
    repo = hubrepo.RedisHubRepo(rdb)
    ref = hubrepo.TransferCleanupRef(3000001, "assign-x")
    await repo.register_transfer_cleanup("t30-src", ref)
    assert await repo.list_transfer_cleanup_pods() == ["t30-src"]
    assert await repo.list_transfer_cleanups("t30-src") == [ref]
    # 摘 ref 后 pod 索引是**持久 superset**,不因空集合删除(避免并发漏扫)
    await repo.remove_transfer_cleanup("t30-src", ref)
    assert await repo.list_transfer_cleanups("t30-src") == []
    assert await repo.list_transfer_cleanup_pods() == ["t30-src"]
    # cleanup 记录**无 TTL**:route/cleanup 不依赖过期
    with pytest.raises(errcode.PandoraError):
        await repo.register_transfer_cleanup("", ref)
    with pytest.raises(errcode.PandoraError):
        await repo.register_transfer_cleanup("t30-src", hubrepo.TransferCleanupRef(0, "x"))


async def test_transfer_cooldown_semantics(rdb) -> None:
    """★ `cooldown <= 0` 恒放行 —— 与 conf.py「负值 = 显式关闭切线冷却」同一条契约。

    写成"<=0 用默认值"会让一份明确关闭冷却的 yaml 悄悄开着闸,两边都不报错。
    """
    repo = hubrepo.RedisHubRepo(rdb)
    pid = 3100001
    assert await repo.try_transfer_cooldown(pid, 0) is True
    assert await repo.try_transfer_cooldown(pid, -1) is True
    assert await rdb.exists(hubrepo.transfer_cooldown_key(pid)) == 0

    assert await repo.try_transfer_cooldown(pid, 30.0) is True
    assert await repo.try_transfer_cooldown(pid, 30.0) is False
    await repo.clear_transfer_cooldown(pid)
    assert await repo.try_transfer_cooldown(pid, 30.0) is True


async def test_team_shard_hint_roundtrip(rdb) -> None:
    repo = hubrepo.RedisHubRepo(rdb)
    assert await repo.get_team_shard(3200001) is None
    await repo.set_team_shard(3200001, "t32-hub", 60.0)
    assert await repo.get_team_shard(3200001) == "t32-hub"
    assert await rdb.pttl(hubrepo.team_key(3200001)) > 0


# ── ⑫ locator 客户端:fail-closed 判定 ─────────────────────────────────────


class _FakeLocationStub:
    def __init__(self, resp=None, exc: Exception | None = None) -> None:
        self._resp = resp
        self._exc = exc

    async def GetLocation(self, req, timeout=None, metadata=None):  # noqa: ANN001, N802
        if self._exc is not None:
            raise self._exc
        return self._resp


def _checker(stub) -> lc.GrpcHubLocationChecker:  # noqa: ANN001
    # ★ 用真 channel 构 stub 再替换：grpc.aio.insecure_channel 是惰性的
    #    (不发起连接)，但 PlayerLocatorServiceStub 需要一个有 unary_unary 的对象。
    obj = lc.GrpcHubLocationChecker(channel=grpc.aio.insecure_channel("127.0.0.1:1"))
    obj._stub = stub  # noqa: SLF001
    return obj


def _loc_resp(state, code=errcode_pb2.OK):  # noqa: ANN001
    return locator_pb2.GetLocationResponse(
        code=code, location=locator_pb2.Location(state=state)
    )


async def test_locator_blocks_matching_and_battle() -> None:
    for state in (
        locator_pb2.LOCATION_STATE_MATCHING,
        locator_pb2.LOCATION_STATE_BATTLE,
    ):
        checker = _checker(_FakeLocationStub(_loc_resp(state)))
        assert await checker.in_battle_or_matching(1) is True


async def test_locator_allows_only_hub() -> None:
    """★ HUB 是**唯一**放行态:presence 明确证明玩家在大厅。"""
    checker = _checker(_FakeLocationStub(_loc_resp(locator_pb2.LOCATION_STATE_HUB)))
    assert await checker.in_battle_or_matching(1) is False


async def test_locator_uncertain_states_are_fail_closed() -> None:
    """★ INC-20260722-002:OFFLINE / UNSPECIFIED / 未知状态一律**抛**,不能返回 False。

    返回 False 的语义是"已证明玩家在 Hub,可以切线" —— 那是 locator 半失效时
    最危险的答案:切线会把玩家送进另一台 Hub DS,不确定态放行 = 潜在双 DS。
    OFFLINE(含 key miss / TTL 消失)只说明 presence 不可见(§9.22)。
    """
    for state in (
        locator_pb2.LOCATION_STATE_OFFLINE,
        locator_pb2.LOCATION_STATE_UNSPECIFIED,
        locator_pb2.LOCATION_STATE_LOGIN_PENDING,
        99,  # 滚动升级期旧副本看到的“未来枚举值”
    ):
        checker = _checker(_FakeLocationStub(_loc_resp(state)))
        with pytest.raises(errcode.PandoraError) as ei:
            await checker.in_battle_or_matching(1)
        assert ei.value.code == errcode.ErrUnavailable


async def test_locator_non_ok_code_is_fail_closed() -> None:
    checker = _checker(
        _FakeLocationStub(
            _loc_resp(locator_pb2.LOCATION_STATE_HUB, code=errcode_pb2.ERR_UNAVAILABLE)
        )
    )
    with pytest.raises(errcode.PandoraError) as ei:
        await checker.in_battle_or_matching(1)
    assert ei.value.code == errcode.ErrUnavailable
