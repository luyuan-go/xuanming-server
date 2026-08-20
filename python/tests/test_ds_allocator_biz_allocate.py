"""`pandorapy.services.ds_allocator.biz_allocate` 的不变量测试。

对照 Go `services/battle/ds_allocator/internal/biz/allocator.go` 第 547–1195 行。

每条用例的 docstring 说明:**守的是什么不变量** / **放行会怎样** / **★ 变异**
(一次真实做过、确认转红后已恢复的最小改动)。
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime as _dt
from typing import Any

import pytest
from pandora.ds.v1 import allocator_pb2 as dspb

from pandorapy import dsmetadata, errcode, releasetrack
from pandorapy.services.ds_allocator import biz_allocate as BA
from pandorapy.services.ds_allocator.battle_auth import (
    BattleAuthoritySnapshot,
    BattleCredentialSeed,
)
from pandorapy.services.ds_allocator.biz_base import (
    STATE_ALLOCATING,
    STATE_ALLOCATION_ABORT,
    STATE_ALLOCATION_RECONCILING,
    STATE_ALLOCATION_UNCERTAIN,
    STATE_ENDED,
    STATE_PREACTIVE_RELEASING,
    STATE_READY,
    STATE_RUNNING,
    STATE_WARMING,
    AllocateResult,
    BattleWaitOwnershipLostError,
    ReadyWaitTimeoutError,
)

NOW_MS = 1_700_000_000_000
PLAYERS = [11, 22, 33]
MATCH_ID = 9001
MAP_ID = 4002
POD = "battle-abc"
ADDR = "10.0.0.7:7777"
UID = "uid-instance-1"
POD_UID = "pod-uid-1"
RV = "rv-42"


# ══ 记录器 / 桩 ══════════════════════════════════════════════════════════════


class LogRecorder:
    """替换 `plog.get()`,把 event 名与字段原样收下来供断言。"""

    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict[str, Any]]] = []

    def _add(self, level: str, event: str, **fields: Any) -> None:
        self.records.append((level, event, fields))

    def debug(self, event: str, **f: Any) -> None:
        self._add("debug", event, **f)

    def info(self, event: str, **f: Any) -> None:
        self._add("info", event, **f)

    def warning(self, event: str, **f: Any) -> None:
        self._add("warning", event, **f)

    def error(self, event: str, **f: Any) -> None:
        self._add("error", event, **f)

    def events(self) -> list[str]:
        return [e for _, e, _ in self.records]

    def find(self, event: str) -> dict[str, Any]:
        for _, e, f in self.records:
            if e == event:
                return f
        raise AssertionError(f"未记录 event={event};实际={self.events()}")


class FakeCfg:
    roster_policy_generation = 7

    def ready_wait_timeout_td(self) -> _dt.timedelta:
        return _dt.timedelta(seconds=10)


class FakeRepo:
    def __init__(self) -> None:
        self.claim_result: tuple[bool, Any] = (True, None)
        self.claim_exc: BaseException | None = None
        self.claimed: Any = None
        self.fence_result = True
        self.fence_exc: BaseException | None = None
        self.fence_calls: list[tuple[int, str]] = []
        self.finalize_result = True
        self.finalize_exc: BaseException | None = None
        self.finalized: Any = None
        self.finalize_kind = ""
        self.battle: Any = None
        self.get_exc: BaseException | None = None

    async def claim_battle(self, claim: Any, ttl: float) -> tuple[bool, Any]:
        self.claimed = claim
        self.claim_ttl = ttl
        if self.claim_exc is not None:
            raise self.claim_exc
        return self.claim_result

    async def fence_battle_allocation(self, match_id: int, allocation_id: str) -> bool:
        self.fence_calls.append((match_id, allocation_id))
        if self.fence_exc is not None:
            raise self.fence_exc
        return self.fence_result

    async def finalize_fenced_battle_allocation(self, battle: Any, ttl: float) -> bool:
        self.finalized, self.finalize_kind = battle, "fenced"
        if self.finalize_exc is not None:
            raise self.finalize_exc
        return self.finalize_result

    async def finalize_battle_allocation(self, battle: Any, ttl: float) -> bool:
        self.finalized, self.finalize_kind = battle, "legacy"
        if self.finalize_exc is not None:
            raise self.finalize_exc
        return self.finalize_result

    async def get_battle(self, match_id: int) -> Any:
        if self.get_exc is not None:
            raise self.get_exc
        return self.battle


class FakeLegacyAlloc:
    """legacy 本地面分配器:同时实现 roster sink 与实例身份源两个 Protocol。"""

    def __init__(self) -> None:
        self.allocate_exc: BaseException | None = None
        self.actual_track = releasetrack.STABLE
        self.roster_calls: list[tuple] = []
        self.identity: tuple[str, int] | None = (UID, 3)

    async def set_pending_battle_roster(
        self, match_id, player_ids, combat_faction_by_player, allocation_id, release_track
    ) -> None:
        self.roster_calls.append(
            (match_id, list(player_ids), combat_faction_by_player, allocation_id, release_track)
        )

    async def allocate(self, match_id, map_id, game_mode, release_track):
        if self.allocate_exc is not None:
            raise self.allocate_exc
        return POD, ADDR, self.actual_track

    def local_instance_identity(self, pod_name: str) -> tuple[str, int] | None:
        return self.identity

    async def release(self, pod_name: str) -> None:  # pragma: no cover - 未在本文件触发
        return None


class FakeAuthoritativeAlloc:
    def __init__(self) -> None:
        self.allocate_exc: BaseException | None = None
        self.allocation: Any = None
        self.allocate_args: list[tuple] = []
        self.echo_allocation_id = True
        self.calls = 0
        self.deliver_calls: list[tuple[Any, dict[str, str]]] = []
        self.deliver_exc: BaseException | None = None

    async def allocate_authoritative(self, *args: Any) -> Any:
        self.calls += 1
        self.allocate_args.append(args)
        if self.allocate_exc is not None:
            raise self.allocate_exc
        if self.allocation is None or not self.echo_allocation_id:
            return self.allocation
        # 真实的 Agones 路径会把本次 allocation_id 写成 annotation 再严格回读,回来的
        # 身份必然带着它。替身不 echo 的话,每个用例都会卡在"身份不完整"上,
        # 永远走不到它想压的那个分支。
        return dataclasses.replace(self.allocation, allocation_id=args[1])

    async def deliver_credential(self, allocation: Any, annotations: dict[str, str]) -> str:
        self.deliver_calls.append((allocation, dict(annotations)))
        if self.deliver_exc is not None:
            raise self.deliver_exc
        return RV


class FakeAuthRepo:
    def __init__(self) -> None:
        self.seed = BattleCredentialSeed(instance_epoch=5, gen=2)
        self.staged: Any = None
        self.mark_exc: BaseException | None = None
        self.mark_calls = 0
        self.snapshot: Any = None
        self.read_exc: BaseException | None = None
        self.read_calls = 0

    async def prepare_credential(self, binding: Any) -> BattleCredentialSeed:
        self.binding = binding
        return self.seed

    async def stage_pending(self, inp: Any) -> None:
        self.staged = inp

    async def mark_delivered(self, match_id, allocation_id, expected, rv, ttl) -> None:
        self.mark_calls += 1
        self.mark_args = (match_id, allocation_id, expected, rv)
        if self.mark_exc is not None:
            raise self.mark_exc

    async def read_authority(self, match_id: int) -> Any:
        self.read_calls += 1
        if self.read_exc is not None:
            raise self.read_exc
        return self.snapshot


class FakeSigner:
    def __init__(self) -> None:
        self.exp_ms = NOW_MS + 120_000
        self.calls: list[tuple] = []

    def sign_battle_credential(
        self, match_id, pod, instance_uid, instance_epoch, gen, jti, ttl_sec
    ):
        self.calls.append((match_id, pod, instance_uid, instance_epoch, gen, jti, ttl_sec))
        return _Signed(self.exp_ms)


class _Signed:
    def __init__(self, exp_ms: int) -> None:
        self.token = "tok"
        self.exp_ms = exp_ms
        self.kid = "kid-1"
        self.token_sha256 = "a" * 64
        self.writer_epoch = BA.BATTLE_DS_WRITER_EPOCH_V2


class Harness(BA.AllocateMixin):
    """把 mixin 装配成可调用对象,并给三个尚未移植的方法提供契约桩。"""

    def __init__(self, *, model_b: bool = False) -> None:
        self.cfg = FakeCfg()
        self.repo = FakeRepo()
        self.alloc = FakeLegacyAlloc()
        self.model_b = model_b
        self.auth_repo = FakeAuthRepo()
        self.authoritative_alloc = FakeAuthoritativeAlloc()
        self.ds_signer = FakeSigner()
        self.ds_credential_ttl_sec = 120.0
        self.release_policy = None
        self.allocation_ledger = None
        self.owner_auth = object()
        # 桩:尚未移植的 ReleaseMixin 方法
        self.wait_result: AllocateResult | None = None
        self.wait_exc: BaseException | None = None
        self.wait_calls: list[tuple] = []
        self.fail_calls: list[tuple] = []
        self.cleanup_calls: list[tuple] = []

    def battle_ttl_sec(self) -> float:
        return 600.0

    def heartbeat_timeout_ms(self) -> int:
        return 15_000

    async def wait_battle_ready(self, match_id, pod_name, allocation_id) -> AllocateResult:
        self.wait_calls.append((match_id, pod_name, allocation_id))
        if self.wait_exc is not None:
            raise self.wait_exc
        assert self.wait_result is not None
        return self.wait_result

    async def fail_ready_wait_timeout(
        self, match_id, allocation_id, pod_name, authoritative, owned
    ) -> BaseException:
        self.fail_calls.append((match_id, allocation_id, pod_name, authoritative, owned))
        return errcode.PandoraError(errcode.ErrDSAllocationFailed, "ready wait timeout")

    async def cleanup_allocated_battle(
        self, match_id, allocation_id, pod_name, authoritative
    ) -> None:
        self.cleanup_calls.append((match_id, allocation_id, pod_name, authoritative))


@pytest.fixture
def log(monkeypatch: pytest.MonkeyPatch) -> LogRecorder:
    rec = LogRecorder()
    monkeypatch.setattr(BA.plog, "get", lambda **_: rec)
    monkeypatch.setattr(BA, "now_ms", lambda: NOW_MS)
    return rec


@pytest.fixture
def owner_ok(monkeypatch: pytest.MonkeyPatch) -> dict[str, list]:
    calls: dict[str, list] = {"begin": [], "verify": []}

    async def _begin(auth, players, owner_type, target, budget):
        calls["begin"].append((list(players), owner_type, target, budget))

    async def _verify(auth, players, owner_type, target, budget):
        calls["verify"].append((list(players), owner_type, target, budget))

    monkeypatch.setattr(BA, "owner_begin_players", _begin)
    monkeypatch.setattr(BA, "owner_verify_players_exact", _verify)
    return calls


def make_battle(**over: Any) -> Any:
    base: dict[str, Any] = {
        "match_id": MATCH_ID,
        "ds_pod_name": POD,
        "ds_addr": ADDR,
        "state": STATE_READY,
        "player_ids": list(PLAYERS),
        "map_id": MAP_ID,
        "game_mode": "pvp",
        "allocated_at_ms": NOW_MS - 5_000,
        "last_heartbeat_ms": NOW_MS - 1_000,
        "player_count": len(PLAYERS),
        "allocation_id": "alloc-1",
        "release_track": releasetrack.STABLE,
        "gameserver_uid": UID,
        "instance_epoch": 5,
    }
    base.update(over)
    return dspb.BattleStorageRecord(**base)


def make_authoritative(allocation_id: str = "", **over: Any) -> Any:
    from pandorapy.services.ds_allocator.agones_allocator import (
        AuthoritativeGameServerAllocation,
    )

    kw: dict[str, Any] = {
        "pod_name": POD,
        "addr": ADDR,
        "instance_uid": UID,
        "pod_uid": POD_UID,
        "instance_epoch": 0,
        "resource_version": RV,
        "allocation_id": allocation_id,
        "release_track": releasetrack.STABLE,
        "annotations_present": True,
    }
    kw.update(over)
    return AuthoritativeGameServerAllocation(**kw)


async def run_allocate(h: Harness, **over: Any) -> AllocateResult:
    kw: dict[str, Any] = {
        "match_id": MATCH_ID,
        "player_ids": list(PLAYERS),
        "combat_faction_by_player": None,
        "map_id": MAP_ID,
        "game_mode": "pvp",
        "rating_mode": 2,
        "rating_pool": "ranked_5v5",
    }
    kw.update(over)
    return await h.allocate_battle_with_combat_factions(**kw)


# ══ 模块级纯函数 ═════════════════════════════════════════════════════════════


def test_combat_faction_records_follow_canonical_roster_order() -> None:
    """不变量:阵营记录的顺序取自 canonical roster(升序),不是调用方 dict 的插入序。

    放行会怎样:同一批玩家在两次调用里产出不同顺序的记录,而
    `same_battle_allocation_request` 是**逐下标**比对该列表的 —— 一次 ACK-loss 重试
    会被判成"快照冲突",分配直接失败,玩家进不去副本(§9.20)。

    ★ 变异:把 `combat_faction_records` 里的
      `for player_id in canonical_players` 改成 `for player_id in combat_faction_by_player`
      → 本条红(顺序变成 33,11,22)。
    """
    canonical, _ = dsmetadata.canonical_roster(list(PLAYERS))
    # 刻意用与 canonical 相反的插入序构造 dict
    mapping = {33: 2, 11: 1, 22: 1}
    records = BA.combat_faction_records(canonical, mapping)
    assert records is not None
    assert [r.player_id for r in records] == canonical
    assert [r.combat_faction_id for r in records] == [1, 1, 2]


def test_combat_faction_records_empty_map_is_none() -> None:
    """不变量:空映射 → None(滚动升级中旧 matchmaker 不带阵营,必须与"空列表"等价)。

    放行会怎样:落进 proto 的是一个长度为 0 的 repeated,与 `None` 在
    `clone_combat_faction_records` / 比对逻辑上虽等价,但会让"该局是否有阵营快照"
    这一判据依赖调用方是否传了空 dict。
    """
    assert BA.combat_faction_records(list(PLAYERS), None) is None
    assert BA.combat_faction_records(list(PLAYERS), {}) is None


def test_clone_combat_faction_records_is_deep() -> None:
    """不变量:克隆必须是深拷贝,claim 记录与 battle 记录不得共享子消息。

    放行会怎样:两份要分别落进 Redis 不同状态的快照会互相串改,而漂移在把字节读回来
    之前完全不可见。

    ★ 变异:把 `clone_combat_faction_records` 的返回改成 `return list(records)`
      → 本条红(改克隆即改原件)。
    """
    src = BA.combat_faction_records([11, 22], {11: 1, 22: 2})
    assert src is not None
    dup = BA.clone_combat_faction_records(src)
    assert dup is not None
    dup[0].combat_faction_id = 999
    assert src[0].combat_faction_id == 1


def test_combat_faction_map_from_records_rejects_non_canonical_order() -> None:
    """不变量:落盘记录必须与 canonical roster **逐下标**对齐才认。

    放行会怎样:一份被旧副本或人工改乱序的快照会解出错位的阵营,DS 侧
    `ResolveCampForSpawn` 拿到错误阵营 —— 玩家进了图站错边(比 RejectSpawn 更糟,
    因为它不报错)。

    ★ 变异:把 `if record is None or record.player_id != ids[i]:` 改成
      `if record is None:` → 本条红。
    """
    canonical, _ = dsmetadata.canonical_roster(list(PLAYERS))
    good = BA.combat_faction_records(canonical, {11: 1, 22: 1, 33: 2})
    assert good is not None
    swapped = [good[1], good[0], good[2]]
    with pytest.raises(ValueError, match="canonical roster order"):
        BA.combat_faction_map_from_records(canonical, swapped)


def test_combat_faction_map_from_records_rejects_out_of_range_faction() -> None:
    """不变量:faction_id 必须落在 DS 阵营取值域内。

    放行会怎样:越界值一路带到 DS,`ResolveCampForSpawn` 解不出权威阵营会 RejectSpawn
    **且禁止回退默认 Pawn** —— 玩家进了图但根本没有角色。

    ★ 变异:把 `> dsmetadata.MAX_COMBAT_FACTION_ID` 改成 `> (1 << 62)` → 本条红。
    """
    rec = dspb.BattlePlayerCombatFaction(
        player_id=11, combat_faction_id=dsmetadata.MAX_COMBAT_FACTION_ID + 1
    )
    with pytest.raises(ValueError, match="exceeds DS camp range"):
        BA.combat_faction_map_from_records([11], [rec])


def test_combat_faction_map_from_records_empty_is_none() -> None:
    """不变量:没有阵营快照 → None,不是异常。

    放行会怎样:滚动升级窗口里旧 matchmaker 写的记录会让每一次读取都抛错,
    整批存量对局无法被 sweep / 重连链处理。
    """
    assert BA.combat_faction_map_from_records(list(PLAYERS), None) is None
    assert BA.combat_faction_map_from_records(list(PLAYERS), []) is None


def test_same_battle_allocation_request_ignores_rating_mode() -> None:
    """不变量:幂等比对**不含** rating_mode。

    放行会怎样:共存窗口里旧 matchmaker 不带该字段、新的带,同一 match 的 ACK-loss
    重试会被判成"快照冲突"直接 ErrUnavailable,玩家进不去场景(§9.21 / §9.20)。

    ★ 变异:在 `same_battle_allocation_request` 的比较里加一条
      `or existing.rating_mode != expected.rating_mode` → 本条红。
    """
    a = make_battle(rating_mode=0)
    b = make_battle(rating_mode=3)
    assert BA.same_battle_allocation_request(a, b) is True


def test_same_battle_allocation_request_detects_faction_drift() -> None:
    """不变量:阵营快照属身份,漂移必须判成不同请求。

    放行会怎样:一个把玩家换到对面阵营的"重试"会被当成同一次分配,直接复用已就绪
    实例返回 —— 玩家用另一套阵营进了同一局。

    ★ 变异:把逐项比较的 for 循环整段删掉(只留 `len(a) != len(b)` 判据)→ 本条红。
    """
    canonical, _ = dsmetadata.canonical_roster(list(PLAYERS))
    a = make_battle(
        player_combat_factions=BA.combat_faction_records(canonical, {11: 1, 22: 1, 33: 2})
    )
    b = make_battle(
        player_combat_factions=BA.combat_faction_records(canonical, {11: 2, 22: 1, 33: 2})
    )
    assert BA.same_battle_allocation_request(a, b) is False
    assert BA.same_battle_allocation_request(a, make_battle()) is False


def test_battle_ready_for_pod_requires_strictly_newer_heartbeat() -> None:
    """不变量:ready 判定要求 `last_heartbeat_ms` **严格大于** `allocated_at_ms`。

    放行会怎样:finalize 时把两者初始化成同一个值(仅作 sweep 宽限基准),写成 `>=`
    会让一台**从未心跳**的 warming 实例立刻被判成 ready,ds_addr 直接回给
    matchmaker —— 玩家连上一个还没读到 match-id 的 DS,被 PreLogin 拒票。

    ★ 变异:把 `b.last_heartbeat_ms > allocated_at_ms` 改成 `>=` → 本条红。
    """
    t = NOW_MS
    never = make_battle(allocated_at_ms=t, last_heartbeat_ms=t)
    assert BA.battle_ready_for_pod(never, POD, MATCH_ID, t) is False
    beat = make_battle(allocated_at_ms=t, last_heartbeat_ms=t + 1)
    assert BA.battle_ready_for_pod(beat, POD, MATCH_ID, t) is True


def test_battle_ready_for_pod_binds_exact_pod_and_match() -> None:
    """不变量:ready 只对 exact (match_id, pod) 成立;None 记录一律 False。

    放行会怎样:把另一台 pod 的就绪当成本次分配就绪,票据与地址指向不同实例。
    """
    b = make_battle()
    assert BA.battle_ready_for_pod(b, "other-pod", MATCH_ID, b.allocated_at_ms) is False
    assert BA.battle_ready_for_pod(b, POD, MATCH_ID + 1, b.allocated_at_ms) is False
    assert BA.battle_ready_for_pod(None, POD, MATCH_ID, 0) is False


def test_battle_wait_state_progressable_is_whitelist() -> None:
    """不变量:可继续等待的状态是**白名单**,未知新状态一律 False。

    放行会怎样:滚动升级引入新状态字面量时,黑名单对它们默认"可以继续等",旧副本会
    去等一个永远不会推进的墓碑,并在超时后对一条不归它管的记录执行清理。

    ★ 变异:把白名单元组加上 `STATE_ALLOCATION_UNCERTAIN` → 本条红。
    """
    for ok in (STATE_ALLOCATING, STATE_WARMING, STATE_READY, STATE_RUNNING):
        assert BA.battle_wait_state_progressable(ok) is True
    for bad in (
        STATE_ENDED,
        STATE_ALLOCATION_UNCERTAIN,
        STATE_ALLOCATION_RECONCILING,
        STATE_PREACTIVE_RELEASING,
        STATE_ALLOCATION_ABORT,
        "some_future_state",
        "",
    ):
        assert BA.battle_wait_state_progressable(bad) is False


def _pending_snapshot(cred: Any, allocation_id: str, rv: str) -> BattleAuthoritySnapshot:
    auth = dspb.BattleDSAuthStorageRecord(
        match_id=MATCH_ID, allocation_id=allocation_id, delivered_rv=rv
    )
    auth.pending.CopyFrom(cred)
    return BattleAuthoritySnapshot(auth=auth, auth_found=True)


def _credential() -> Any:
    return dspb.BattleDSCredential(
        gen=2,
        jti="jti-1",
        exp_ms=NOW_MS + 120_000,
        kid="kid-1",
        instance_uid=UID,
        instance_epoch=5,
        token_sha256="a" * 64,
        writer_epoch=BA.BATTLE_DS_WRITER_EPOCH_V2,
    )


def test_battle_pending_delivered_requires_delivered_rv() -> None:
    """不变量:`delivered_rv` 必须等于本轮 PATCH 返回的 rv。

    放行会怎样:把"另一轮投递的结果"当成本轮成功,本轮 pending 凭据永远停在未投递态
    —— DS 拿不到可用令牌,而后端认为分配已完成。

    ★ 变异:删掉 `and auth.delivered_rv == rv` → 本条红。
    """
    cred = _credential()
    assert BA.battle_pending_delivered(_pending_snapshot(cred, "a1", RV), "a1", cred, RV)
    assert not BA.battle_pending_delivered(
        _pending_snapshot(cred, "a1", "rv-other"), "a1", cred, RV
    )


def test_battle_pending_delivered_requires_token_hash_and_allocation() -> None:
    """不变量:token 摘要与 allocation_id 都必须 exact 命中;auth 未找到一律 False。

    放行会怎样:一份属于**上一次分配**的 pending 会被判成本轮已投递,cleanup 不再
    发生,DS 侧拿着旧令牌通过 PreLogin 进入一个已被撤销的实例。

    ★ 变异:删掉 `and hmac.compare_digest(pending.token_sha256, expected.token_sha256)`
      → 本条红。
    """
    cred = _credential()
    other = _credential()
    other.token_sha256 = "b" * 64
    assert not BA.battle_pending_delivered(_pending_snapshot(other, "a1", RV), "a1", cred, RV)
    assert not BA.battle_pending_delivered(_pending_snapshot(cred, "a2", RV), "a1", cred, RV)
    assert not BA.battle_pending_delivered(BattleAuthoritySnapshot(), "a1", cred, RV)


def test_annotation_keys_are_byte_exact() -> None:
    """不变量:九个 annotation key 是跨进程契约(ds_allocator 写、UE DS 读)。

    放行会怎样:错一个字母不报错,而是 DS 读不到凭据 → PreLogin fail-closed 拒票
    → 玩家连上就被踢,后端这边却"分配成功"。

    ★ 变异:把 `BATTLE_TOKEN_EXP_ANNOTATION_KEY` 改成 `"pandora.dev/ds-token-exp"`
      → 本条红。
    """
    assert BA.BATTLE_TOKEN_ANNOTATION_KEY == "pandora.dev/ds-token"
    assert BA.BATTLE_TOKEN_EXP_ANNOTATION_KEY == "pandora.dev/ds-token-exp-ms"
    assert BA.BATTLE_TOKEN_GEN_ANNOTATION_KEY == "pandora.dev/ds-token-gen"
    assert BA.BATTLE_TOKEN_JTI_ANNOTATION_KEY == "pandora.dev/ds-token-jti"
    assert BA.BATTLE_INSTANCE_UID_ANNOTATION_KEY == "pandora.dev/ds-instance-uid"
    assert BA.BATTLE_INSTANCE_EPOCH_KEY == "pandora.dev/ds-instance-epoch"
    assert BA.BATTLE_WRITER_EPOCH_KEY == "pandora.dev/ds-writer-epoch"
    assert BA.BATTLE_TOKEN_KID_KEY == "pandora.dev/ds-token-kid"
    assert BA.BATTLE_TOKEN_HASH_KEY == "pandora.dev/ds-token-sha256"


# ══ 轨道选择 ═════════════════════════════════════════════════════════════════


def test_desired_release_track_falls_back_to_stable() -> None:
    """不变量:未注入发布策略时回落 **stable**。

    放行会怎样:回落 canary = 未注入策略的部署把全部对局灌进未验证轨道,正是 §9.21
    明令禁止的方向。

    ★ 变异:把 `_desired_release_track` 的 `return releasetrack.STABLE` 改成
      `return releasetrack.CANARY` → 本条红。
    """
    h = Harness()
    assert h._desired_release_track(MATCH_ID) == releasetrack.STABLE
    h.release_policy = releasetrack.new(100, "seed")
    assert h._desired_release_track(MATCH_ID) == releasetrack.CANARY


# ══ 入口参数闸 ═══════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_allocate_rejects_zero_match_id_before_touching_repo(log: LogRecorder) -> None:
    """不变量:match_id 缺失在**任何权威写之前**拒掉,并带 reason 打点。

    放行会怎样:match_id=0 会算出一个谁也读不到的 Redis slot,表现为"分配了但没人
    找得到这局",而 matchmaker 只看到一个没有根因分档的错误(§9.20)。

    ★ 变异:把 `if match_id == 0 or match_id < 0 or match_id > _UINT64_MAX:` 里的
      `match_id == 0 or` 删掉 → 本条红。
    """
    h = Harness()
    with pytest.raises(errcode.PandoraError) as ei:
        await run_allocate(h, match_id=0)
    assert ei.value.code == errcode.ErrInvalidArg
    assert h.repo.claimed is None
    assert log.find("battle_allocate_refused")["reason"] == BA.REASON_ALLOC_MATCH_ID_REQUIRED


@pytest.mark.asyncio
async def test_allocate_rejects_out_of_range_match_id(log: LogRecorder) -> None:
    """不变量:Python 整数不回绕,uint64 越界必须显式拒。

    放行会怎样:越界值一路带到 protobuf 序列化才抛一个既没有业务码、也说不清是哪个
    字段的 ValueError,而此时 claim 已经打出去了。

    ★ 变异:把入口判据里的 `or match_id > _UINT64_MAX` 删掉 → 本条红。
    """
    h = Harness()
    with pytest.raises(errcode.PandoraError) as ei:
        await run_allocate(h, match_id=(1 << 64))
    assert ei.value.code == errcode.ErrInvalidArg
    assert h.repo.claimed is None


@pytest.mark.asyncio
async def test_allocate_rejects_invalid_roster(log: LogRecorder) -> None:
    """不变量:roster 非法 → ErrInvalidArg + `roster_invalid` reason,且不写 claim。

    放行会怎样:带 0 的 roster 会被写进权威快照,DS 侧拿它建人 → player_id=0 的幽灵
    角色,后续 owner / 票据 / 结算全链都没有对应玩家。

    注意:重复 id **不是**非法 —— `dsmetadata.CanonicalRoster` 按设计会排序去重,
    拿 `[11, 11]` 当非法用例根本压不到这条分支。

    ★ 变异:把该分支的 `errcode.ErrInvalidArg` 改成 `errcode.ErrInternal` → 本条红。
    """
    h = Harness()
    with pytest.raises(errcode.PandoraError) as ei:
        await run_allocate(h, player_ids=[11, 0])
    assert ei.value.code == errcode.ErrInvalidArg
    assert h.repo.claimed is None
    assert log.find("battle_allocate_refused")["reason"] == BA.REASON_ALLOC_ROSTER_INVALID


@pytest.mark.asyncio
async def test_allocate_rejects_faction_map_not_covering_roster(log: LogRecorder) -> None:
    """不变量:非空阵营映射必须**精确覆盖** roster。

    放行会怎样:漏掉的玩家在 DS 侧解不出权威阵营 → RejectSpawn 且禁止回退默认 Pawn,
    玩家进了图没有角色。

    ★ 变异:把 `if combat_faction_by_player:` 改成 `if False:`(跳过校验)→ 本条红。
    """
    h = Harness()
    with pytest.raises(errcode.PandoraError) as ei:
        await run_allocate(h, combat_faction_by_player={11: 1})
    assert ei.value.code == errcode.ErrInvalidArg
    assert h.repo.claimed is None
    assert log.find("battle_allocate_refused")["reason"] == BA.REASON_ALLOC_FACTIONS_INVALID


# ══ claim 线性化点 ═══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_claim_write_failure_reports_pre_agones_reason(log: LogRecorder) -> None:
    """不变量:claim 写失败必须带 `claim_write_failed`,把"还没到 Agones"标出来。

    放行会怎样:matchmaker 只看到一个错误码 —— "分配不到 DS"到底是没到 Agones 还是
    Agones 没货,查不出来(§9.20 根因分档)。

    ★ 变异:把 `reason=REASON_ALLOC_CLAIM_FAILED` 改成
      `reason=REASON_ALLOC_TRACK_INVALID` → 本条红。
    """
    h = Harness()
    h.repo.claim_exc = RuntimeError("redis down")
    with pytest.raises(RuntimeError):
        await run_allocate(h)
    fields = log.find("battle_allocate_refused")
    assert fields["reason"] == BA.REASON_ALLOC_CLAIM_FAILED
    assert fields["match_id"] == MATCH_ID


@pytest.mark.asyncio
async def test_claim_loser_snapshot_mismatch_never_awaits_existing(log: LogRecorder) -> None:
    """不变量:claim 输家的请求与已落定快照不一致时 fail-closed,**不进入等待链**。

    放行会怎样:一个 roster / map 不同的请求会去等别人的实例,并在就绪后把它当成
    自己的分配返回 —— 一批玩家被路由进另一局(§9.1)。

    ★ 变异:把 `if not same_battle_allocation_request(existing, claim):` 改成
      `if same_battle_allocation_request(existing, claim):` → 本条红。
    """
    h = Harness()
    h.repo.claim_result = (False, make_battle(map_id=MAP_ID + 1))
    with pytest.raises(errcode.PandoraError) as ei:
        await run_allocate(h)
    assert ei.value.code == errcode.ErrUnavailable
    assert h.wait_calls == []
    assert "battle_allocation_idempotency_snapshot_mismatch" in log.events()


@pytest.mark.asyncio
async def test_claim_records_policy_generation_and_rating(log: LogRecorder, owner_ok) -> None:
    """不变量:claim 与 battle 两份记录都冻结 `roster_policy_generation`,并原样存
    `rating_mode` / `rating_pool`(本服务不解释、不下发 DS)。

    放行会怎样:策略代不冻结会让存量局在配置代前进后被按新口径判到齐,批量误判弃局;
    rating 被"顺手校验 / 改写"则把段位判定权从 battle_result 挪到了这里(§9.6)。

    ★ 变异:把 claim 的 `roster_policy_generation=self.cfg.roster_policy_generation`
      改成 `roster_policy_generation=0` → 本条红。
    """
    h = Harness()
    h.wait_result = AllocateResult(
        ds_addr=ADDR, ds_pod_name=POD, allocation_id="x", release_track=releasetrack.STABLE
    )
    await run_allocate(h)
    assert h.repo.claimed.roster_policy_generation == 7
    assert h.repo.finalized.roster_policy_generation == 7
    assert h.repo.claimed.rating_mode == 2
    assert h.repo.finalized.rating_pool == "ranked_5v5"
    assert h.repo.claimed.state == STATE_ALLOCATING
    assert h.repo.finalized.state == STATE_WARMING


# ══ Model B 的 fence / 不确定墓碑 ════════════════════════════════════════════


@pytest.mark.asyncio
async def test_model_b_fence_failure_never_posts_to_orchestrator(log: LogRecorder) -> None:
    """不变量:pre-allocation fence 未确认成功时**绝不**调用外部 GSA。

    放行会怎样:同一 match 可能发出第二次 POST,产生两个 Allocated GameServer —— 一批
    玩家被分到两台 DS(§9.1 / §9.22 脑裂)。

    ★ 变异:把 `if fence_exc is not None or not fenced:` 改成
      `if fence_exc is not None:` → 本条红。
    """
    h = Harness(model_b=True)
    h.repo.fence_result = False
    with pytest.raises(errcode.PandoraError) as ei:
        await run_allocate(h)
    assert ei.value.code == errcode.ErrUnavailable
    assert h.authoritative_alloc.calls == 0
    assert h.cleanup_calls == []
    assert "gameserver_preallocation_fence_failed" in log.events()


@pytest.mark.asyncio
async def test_model_b_allocate_error_retains_uncertain_without_cleanup(
    log: LogRecorder,
) -> None:
    """不变量:Model B 下 GSA 调用失败 = **结果未知**,保持永久 uncertain,不做回收。

    放行会怎样:自动 Release / 删 claim 会把一个"已应用但响应迟到"的分配悄悄抹掉,
    随后同 match 重试再发一次 POST —— 两台 DS 同时认领同一批玩家。

    ★ 变异:把 uncertain 分支的 `if self.model_b:` 改成 `if not self.model_b:`
      → 本条红(会走到 cleanup)。
    """
    h = Harness(model_b=True)
    h.authoritative_alloc.allocate_exc = RuntimeError("post timeout")
    with pytest.raises(errcode.PandoraError) as ei:
        await run_allocate(h)
    assert ei.value.code == errcode.ErrUnavailable
    assert h.cleanup_calls == []
    assert "gameserver_allocation_uncertain_retained" in log.events()


@pytest.mark.asyncio
async def test_no_available_gameserver_gets_its_own_reason(log: LogRecorder) -> None:
    """不变量:"Fleet 没货"(要扩容)与"控制面调用失败"(要查 k8s)必须分档。

    放行会怎样:两者共用一条日志,扩容与排障用同一个信号,谁也定位不了(§9.20)。

    ★ 变异:把 `alloc_reason = "no_available_gameserver"` 改成
      `alloc_reason = "control_plane_failed"` → 本条红。
    """
    h = Harness()
    h.alloc.allocate_exc = errcode.PandoraError(errcode.ErrDSNoAvailable, "empty fleet")
    with pytest.raises(errcode.PandoraError) as ei:
        await run_allocate(h)
    assert ei.value.code == errcode.ErrDSAllocationFailed
    assert log.find("gameserver_allocate_failed")["reason"] == "no_available_gameserver"
    # legacy 路径拥有 claim,必须回收刚分配的 pod
    assert len(h.cleanup_calls) == 1


@pytest.mark.asyncio
async def test_model_b_identity_incomplete_is_fail_closed(log: LogRecorder) -> None:
    """不变量:副作用边界复核 exact 身份,allocation_id 不匹配一律 fail-closed 且不清理。

    放行会怎样:错误实现 / 测试桩能以"不抛异常"绕过 UID/RV/allocation_id 确认,
    后端把一个身份不明的实例当成本次分配交付,票据绑不住 DS(§9.22 exact 绑定)。

    ★ 变异:删掉完整性判据里的 `or authoritative.allocation_id != allocation_id`
      → 本条红。
    """
    h = Harness(model_b=True)
    # 关掉替身的 allocation_id 回显,模拟"回读到的 allocation_id 不是本次的"。
    h.authoritative_alloc.echo_allocation_id = False
    h.authoritative_alloc.allocation = make_authoritative(allocation_id="not-mine")
    with pytest.raises(errcode.PandoraError) as ei:
        await run_allocate(h)
    assert ei.value.code == errcode.ErrUnavailable
    assert h.cleanup_calls == []
    assert "gameserver_authoritative_identity_incomplete" in log.events()


@pytest.mark.asyncio
async def test_invalid_actual_release_track_recycles_pod(log: LogRecorder) -> None:
    """不变量:落库的必须是编排层回读到的**合法** actual 轨道,否则回收已分配的 pod。

    放行会怎样:一个轨道不明的实例被交给 matchmaker,金丝雀粘滞被打穿:同一玩家
    可能在 Stable / Canary 之间来回(§9.21)。

    ★ 变异:把 `if not releasetrack.valid(actual_release_track):` 改成
      `if releasetrack.valid(actual_release_track):` → 本条红。
    """
    h = Harness()
    h.alloc.actual_track = "weird-track"
    with pytest.raises(errcode.PandoraError) as ei:
        await run_allocate(h)
    assert ei.value.code == errcode.ErrDSAllocationFailed
    assert len(h.cleanup_calls) == 1
    fields = log.find("battle_allocate_refused")
    assert fields["reason"] == BA.REASON_ALLOC_TRACK_INVALID
    assert fields["actual_track"] == "weird-track"


# ══ finalize ═════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_legacy_finalize_lost_recycles_pod(log: LogRecorder) -> None:
    """不变量:legacy 路径 finalize 返回 False(claim 已不属本次分配)必须回收 pod。

    放行会怎样:刚分配的 GameServer 无人认领,泄漏成孤儿,Fleet 容量被慢慢吃光。

    ★ 变异:把 `if finalize_exc is not None or not finalized:` 改成
      `if finalize_exc is not None:` → 本条红。
    """
    h = Harness()
    h.repo.finalize_result = False
    with pytest.raises(errcode.PandoraError) as ei:
        await run_allocate(h)
    assert ei.value.code == errcode.ErrDSAllocationFailed
    assert len(h.cleanup_calls) == 1
    assert log.find("battle_allocate_refused")["reason"] == BA.REASON_ALLOC_FINALIZE_LOST


@pytest.mark.asyncio
async def test_model_b_finalize_failure_does_not_recycle(log: LogRecorder) -> None:
    """不变量:Model B 下 finalize 失败 / 响应未知**不得**自动释放。

    放行会怎样:事务可能已把 persistent uncertain 成功改成 warming,本请求凭本地结果
    删 claim / 碰 K8s 会撤掉一个其实已经生效的分配。

    ★ 变异:把 finalize 失败分支里的 `if self.model_b:` 改成 `if not self.model_b:`
      → 本条红(会走到 cleanup)。
    """
    h = Harness(model_b=True)
    h.authoritative_alloc.allocation = make_authoritative()
    h.repo.finalize_exc = RuntimeError("redis unknown")
    with pytest.raises(errcode.PandoraError) as ei:
        await run_allocate(h)
    assert ei.value.code == errcode.ErrUnavailable
    assert h.cleanup_calls == []
    assert "gameserver_fenced_finalize_unavailable" in log.events()
    assert h.repo.finalize_kind == "fenced"


@pytest.mark.asyncio
async def test_legacy_backfills_local_instance_identity(log: LogRecorder, owner_ok) -> None:
    """不变量:legacy 本地面必须回填 exact 实例身份(gameserver_uid / instance_epoch)。

    放行会怎样:两者恒零 → matchmaker 判「未回填完整 DS 目标」拒签 v2 战斗票、对局
    直接 FAILED,玩家永远进不去副本。

    ★ 变异:把 `if isinstance(self.alloc, LocalInstanceIdentitySource):` 改成
      `if False:`(或删掉整段 elif)→ 本条红。
    """
    h = Harness()
    h.alloc.identity = ("uid-local", 9)
    h.wait_result = AllocateResult(ds_pod_name=POD, allocation_id="x")
    await run_allocate(h)
    assert h.repo.finalized.gameserver_uid == "uid-local"
    assert h.repo.finalized.instance_epoch == 9
    # roster sink 必须在 allocate 之前登记(DS 进程 env 在 allocate 内定型)
    assert len(h.alloc.roster_calls) == 1


# ══ ready 等待 / owner 定案 ══════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_ready_wait_timeout_raises_returned_error_as_owner(log: LogRecorder) -> None:
    """不变量:claim 赢家超时走 `fail_ready_wait_timeout(owned=True)`,并抛它**返回**的错误。

    放行会怎样:`owned` 传错会让拥有者跳过回收(pod 泄漏),或让输家去回收别人的
    allocation(与 sweep 并发跑第二路 release)。

    ★ 变异:把该调用的最后一个实参 `True` 改成 `False` → 本条红。
    """
    h = Harness()
    h.wait_exc = errcode.PandoraError(
        errcode.ErrUnavailable, "wait", cause=ReadyWaitTimeoutError()
    )
    with pytest.raises(errcode.PandoraError) as ei:
        await run_allocate(h)
    assert ei.value.code == errcode.ErrDSAllocationFailed
    assert len(h.fail_calls) == 1
    assert h.fail_calls[0][4] is True
    assert h.cleanup_calls == []


@pytest.mark.asyncio
async def test_ready_wait_ownership_lost_skips_cleanup(log: LogRecorder) -> None:
    """不变量:等待期间所有权已被 sweep / 新分配接管时,owner **放弃** cleanup。

    放行会怎样:与在途的 fenced 回收链并发跑第二路 `fence_preactive_release` /
    `release_expected` —— 那两个只有幂等最终一致保证,**没有单次调用保证**。

    ★ 变异:删掉 `if _is_battle_wait_ownership_lost(werr):` 整块 → 本条红。
    """
    h = Harness()
    h.wait_exc = errcode.PandoraError(
        errcode.ErrUnavailable, "superseded", cause=BattleWaitOwnershipLostError()
    )
    with pytest.raises(errcode.PandoraError):
        await run_allocate(h)
    assert h.cleanup_calls == []
    assert "battle_ready_wait_ownership_lost" in log.events()


@pytest.mark.asyncio
async def test_ownership_lost_is_detected_through_cause_chain(log: LogRecorder) -> None:
    """不变量:哨兵识别沿 `PandoraError.cause` 链走,不是只看最外层。

    放行会怎样:`errcode.NewCause` 的"外层 code 给客户端、内层 cause 给控制流"被抹平,
    ownership-lost 被当成普通失败 → 并发第二路回收(同上条)。

    ★ 变异:把 `_cause_chain_has` 的 while 循环改成只判一次
      `return isinstance(exc, kind)` → 本条红(两层嵌套不再命中)。
    """
    inner = errcode.PandoraError(
        errcode.ErrUnavailable, "inner", cause=BattleWaitOwnershipLostError()
    )
    outer = errcode.PandoraError(errcode.ErrUnavailable, "outer", cause=inner)
    assert BA._is_battle_wait_ownership_lost(outer) is True
    assert BA._is_battle_wait_ownership_lost(errcode.PandoraError(errcode.ErrInternal)) is False


@pytest.mark.asyncio
async def test_wait_failure_other_than_sentinels_cleans_up(log: LogRecorder) -> None:
    """不变量:非哨兵失败(repo 出错等)由 claim 赢家回收刚分配的 pod。

    放行会怎样:pod 泄漏成孤儿,Fleet 容量被吃光。
    """
    h = Harness()
    h.wait_exc = RuntimeError("redis blew up")
    with pytest.raises(RuntimeError):
        await run_allocate(h)
    assert len(h.cleanup_calls) == 1


@pytest.mark.asyncio
async def test_owner_begin_failure_refuses_ready_and_cleans_up(
    log: LogRecorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    """不变量:owner 归属没定案就**不交付 READY**。

    放行会怎样:客户端拿票进这台 Battle DS,而归属未定案 —— 玩家可能同时被两台 DS
    认领(§9.22 / 底线第 3 条)。

    ★ 变异:把 `await owner_begin_players(...)` 整段 try 删掉(直接不调用)→ 本条红。
    """

    async def _boom(*_a: Any, **_k: Any) -> None:
        raise errcode.PandoraError(errcode.ErrUnavailable, "owner down")

    monkeypatch.setattr(BA, "owner_begin_players", _boom)
    h = Harness()
    h.wait_result = AllocateResult(ds_pod_name=POD, allocation_id="x")
    with pytest.raises(errcode.PandoraError):
        await run_allocate(h)
    assert len(h.cleanup_calls) == 1
    assert "battle_ready_refused_owner_begin_failed" in log.events()


@pytest.mark.asyncio
async def test_owner_begin_unknown_outcome_is_retained(
    log: LogRecorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    """不变量:owner Begin **结果未知**时保留 allocation/pod,不回收、不 rollback。

    放行会怎样:回收 Pod 会把一个可能已落库的 PENDING owner 变成永久死目标 ——
    玩家的归属指向一台已经不存在的 DS,重连链只能一直 WAIT(§9.23)。

    ★ 变异:把 `if is_owner_begin_outcome_unknown(owner_exc):` 改成
      `if not is_owner_begin_outcome_unknown(owner_exc):` → 本条红。
    """
    from pandorapy.services.ds_allocator.owner_authority import OWNER_BEGIN_OUTCOME_UNKNOWN

    async def _unknown(*_a: Any, **_k: Any) -> None:
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "readback failed", cause=OWNER_BEGIN_OUTCOME_UNKNOWN
        )

    monkeypatch.setattr(BA, "owner_begin_players", _unknown)
    h = Harness()
    h.wait_result = AllocateResult(ds_pod_name=POD, allocation_id="x")
    with pytest.raises(errcode.PandoraError):
        await run_allocate(h)
    assert h.cleanup_calls == []
    assert "battle_owner_begin_outcome_unknown_retained" in log.events()


@pytest.mark.asyncio
async def test_success_path_logs_warming_and_ready(log: LogRecorder, owner_ok) -> None:
    """不变量:分配链两端各有一条阶段推进日志,字段名与 Go 逐字节一致。

    放行会怎样:少了 `battle_warming` 就无法证明"分配到底有没有走到 Agones 这一步";
    少了 `battle_ready_after_heartbeat` 就算不出"等 DS 冷启动花了多久"。

    ★ 变异:把 `ready_wait=` 改成 `ready_wait_sec=` → 本条红。
    """
    h = Harness()
    h.wait_result = AllocateResult(
        ds_addr=ADDR,
        ds_pod_name=POD,
        gameserver_uid=UID,
        instance_epoch=3,
        allocation_id="x",
        release_track=releasetrack.STABLE,
    )
    res = await run_allocate(h)
    assert res is h.wait_result
    warming = log.find("battle_warming")
    assert warming["ready_wait"] == "10s"
    assert warming["release_track"] == releasetrack.STABLE
    ready = log.find("battle_ready_after_heartbeat")
    assert ready["uid"] == UID
    assert ready["epoch"] == 3
    assert ready["players"] == len(PLAYERS)
    # owner 定案必须发生在返回之前,且用 BATTLE 类型
    assert len(owner_ok["begin"]) == 1
    assert owner_ok["begin"][0][1] == BA.OWNER_TYPE_BATTLE
    assert owner_ok["begin"][0][3] == BA.OWNER_BEGIN_BUDGET_SEC


# ══ 凭据投递 ═════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_provision_writes_back_instance_epoch_and_annotations() -> None:
    """不变量:领到 seed 后必须把 `instance_epoch` 回写进 allocation。

    放行会怎样:后续 `deliver_credential` 的条件 PATCH 与 cleanup 的 exact 回收都拿着
    epoch=0 去对账 —— 投递打在错误的实例世代上(§9.22 exact 绑定)。

    ★ 变异:删掉 `allocation.instance_epoch = seed.instance_epoch` → 本条红。
    """
    h = Harness(model_b=True)
    alloc = make_authoritative(allocation_id="a1")
    await h.provision_battle_credential(MATCH_ID, "a1", alloc)
    assert alloc.instance_epoch == 5
    _, ann = h.authoritative_alloc.deliver_calls[0]
    assert ann[BA.BATTLE_INSTANCE_EPOCH_KEY] == "5"
    assert ann[BA.BATTLE_TOKEN_GEN_ANNOTATION_KEY] == "2"
    assert ann[BA.BATTLE_TOKEN_HASH_KEY] == "a" * 64
    assert set(ann) == {
        BA.BATTLE_TOKEN_ANNOTATION_KEY,
        BA.BATTLE_TOKEN_EXP_ANNOTATION_KEY,
        BA.BATTLE_TOKEN_GEN_ANNOTATION_KEY,
        BA.BATTLE_TOKEN_JTI_ANNOTATION_KEY,
        BA.BATTLE_INSTANCE_UID_ANNOTATION_KEY,
        BA.BATTLE_INSTANCE_EPOCH_KEY,
        BA.BATTLE_WRITER_EPOCH_KEY,
        BA.BATTLE_TOKEN_KID_KEY,
        BA.BATTLE_TOKEN_HASH_KEY,
    }


@pytest.mark.asyncio
async def test_provision_rejects_non_positive_exp() -> None:
    """不变量:签名器给出非正 exp 一律 ErrInvalidState,且**不投递**。

    放行会怎样:一张永不过期(或已过期)的令牌被 PATCH 进 GameServer annotation,
    §9.3 的"短 exp 是 B1 纯本地验票下唯一的吊销手段"直接失效。

    ★ 变异:把 `if signed.exp_ms <= 0 or ...` 改成 `if signed.exp_ms < 0 or ...`
      → 本条红。
    """
    h = Harness(model_b=True)
    h.ds_signer.exp_ms = 0
    with pytest.raises(errcode.PandoraError) as ei:
        await h.provision_battle_credential(MATCH_ID, "a1", make_authoritative("a1"))
    assert ei.value.code == errcode.ErrInvalidState
    assert h.authoritative_alloc.deliver_calls == []


@pytest.mark.asyncio
async def test_provision_requires_authoritative_allocation() -> None:
    """不变量:没有权威分配结果时 fail-closed,不凭本地拼一个身份去投递。

    放行会怎样:`allocation.pod_name` 之类会直接 AttributeError,而此刻可能已经有
    在途的 Redis 写 —— 失败被降级成一个说不清位置的崩溃。
    """
    h = Harness(model_b=True)
    with pytest.raises(errcode.PandoraError) as ei:
        await h.provision_battle_credential(MATCH_ID, "a1", None)
    assert ei.value.code == errcode.ErrInvalidState


@pytest.mark.asyncio
async def test_provision_swallows_mark_failure_only_when_readback_proves() -> None:
    """不变量:`mark_delivered` 响应不确定时**只认权威 read-back**。

    放行会怎样:以本地 expected 或 K8s 镜像猜测成功,会把一次未完成的投递记成完成,
    DS 拿不到可用令牌而后端认为分配已完成(§9.22:UNKNOWN 不得冒充成功)。

    ★ 变异:把 `if not battle_pending_delivered(...):` 改成
      `if battle_pending_delivered(...):` → 本条红。
    """
    h = Harness(model_b=True)
    h.auth_repo.mark_exc = RuntimeError("redis unknown")

    # 权威证明本轮已投递 → 吞掉 mark 失败
    def _snapshot_for(staged: Any, rv: str) -> BattleAuthoritySnapshot:
        return _pending_snapshot(staged.credential, staged.allocation_id, rv)

    alloc = make_authoritative("a1")
    h.auth_repo.snapshot = None

    class _LateSnapshotRepo(FakeAuthRepo):
        async def read_authority(self, match_id: int) -> Any:
            self.read_calls += 1
            return _snapshot_for(self.staged, RV)

    late = _LateSnapshotRepo()
    late.mark_exc = RuntimeError("redis unknown")
    h.auth_repo = late
    await h.provision_battle_credential(MATCH_ID, "a1", alloc)
    assert late.read_calls == 1

    # 权威**未**证明 → 原错误上抛
    h2 = Harness(model_b=True)
    h2.auth_repo.mark_exc = RuntimeError("redis unknown")
    h2.auth_repo.snapshot = BattleAuthoritySnapshot()
    with pytest.raises(RuntimeError, match="redis unknown"):
        await h2.provision_battle_credential(MATCH_ID, "a1", make_authoritative("a1"))


@pytest.mark.asyncio
async def test_provision_readback_failure_reraises_original() -> None:
    """不变量:read-back 本身失败时按**原**错误上抛,不冒充成功也不换错误。

    放行会怎样:UNKNOWN 被当成"读不到就算没投递成功"以外的任何结论,都会让
    §9.22 的 fail-closed 失效。
    """
    h = Harness(model_b=True)
    h.auth_repo.mark_exc = RuntimeError("mark unknown")
    h.auth_repo.read_exc = RuntimeError("read also down")
    with pytest.raises(RuntimeError, match="mark unknown"):
        await h.provision_battle_credential(MATCH_ID, "a1", make_authoritative("a1"))


# ══ claim 输家 / 幂等重试 ════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_await_existing_missing_record(log: LogRecorder) -> None:
    """不变量:输家读不回赢家记录 → ErrDSAllocationFailed + `claim_record_missing`。

    放行会怎样:对 None 继续走等待链会直接 AttributeError,把一次可重试的业务失败
    变成崩溃。
    """
    h = Harness()
    with pytest.raises(errcode.PandoraError) as ei:
        await h.await_existing_allocation(MATCH_ID, None)
    assert ei.value.code == errcode.ErrDSAllocationFailed
    assert log.find("battle_allocate_refused")["reason"] == BA.REASON_AWAIT_CLAIM_MISSING


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [STATE_ALLOCATION_UNCERTAIN, STATE_ALLOCATION_RECONCILING])
async def test_await_existing_uncertain_never_waits(log: LogRecorder, state: str) -> None:
    """不变量:uncertain / reconciling 一律 ErrUnavailable,不等待、不清理、不碰 K8s。

    放行会怎样:输家去等一个永远不会推进的墓碑(白耗 ready_wait),或更糟 —— 帮它
    "清理"掉,于是永久 claim 消失,同 match 重试发出第二次 POST。

    ★ 变异:删掉该分支 → 本条红(会落进等待链)。
    """
    h = Harness()
    with pytest.raises(errcode.PandoraError) as ei:
        await h.await_existing_allocation(MATCH_ID, make_battle(state=state))
    assert ei.value.code == errcode.ErrUnavailable
    assert h.wait_calls == []
    assert "allocate_idempotent_uncertain" in log.events()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [STATE_PREACTIVE_RELEASING, STATE_ALLOCATION_ABORT])
async def test_await_existing_release_unconfirmed(log: LogRecorder, state: str) -> None:
    """不变量:上一次分配的回收未确认时,永久 fence 必须继续阻止再分配。

    放行会怎样:一台可能还活着的旧 GameServer 与新分配并存,同一 match 两台 DS
    (§9.1 / §9.22)。

    ★ 变异:把 `REASON_AWAIT_RELEASE_UNCONFIRMED` 改成 `REASON_AWAIT_CLAIM_MISSING`
      → 本条红。
    """
    h = Harness()
    with pytest.raises(errcode.PandoraError) as ei:
        await h.await_existing_allocation(MATCH_ID, make_battle(state=state))
    assert ei.value.code == errcode.ErrUnavailable
    assert h.wait_calls == []
    fields = log.find("battle_allocate_refused")
    assert fields["reason"] == BA.REASON_AWAIT_RELEASE_UNCONFIRMED
    assert fields["state"] == state


@pytest.mark.asyncio
async def test_await_existing_unusable_state(log: LogRecorder) -> None:
    """不变量:legacy 下已终结的状态(ended)不可分配,直接 ErrDSAllocationFailed。

    放行会怎样:等待一个已结束的对局直到 ready_wait 超时,matchmaker 被白白拖住。
    """
    h = Harness()
    with pytest.raises(errcode.PandoraError) as ei:
        await h.await_existing_allocation(MATCH_ID, make_battle(state=STATE_ENDED))
    assert ei.value.code == errcode.ErrDSAllocationFailed
    assert h.wait_calls == []
    assert "allocate_idempotent_unusable" in log.events()


@pytest.mark.asyncio
async def test_await_existing_timeout_uses_owned_false(log: LogRecorder) -> None:
    """不变量:输家超时收尾必须 `owned=False`,**绝不**回收 pod 或删镜像。

    放行会怎样:两路并发回收同一个 allocation,而
    `fence_preactive_release` / `release_expected` 只有幂等最终一致保证。

    ★ 变异:把该调用的最后一个实参 `False` 改成 `True` → 本条红。
    """
    h = Harness()
    h.wait_exc = errcode.PandoraError(
        errcode.ErrUnavailable, "timeout", cause=ReadyWaitTimeoutError()
    )
    with pytest.raises(errcode.PandoraError):
        await h.await_existing_allocation(MATCH_ID, make_battle(state=STATE_WARMING))
    assert len(h.fail_calls) == 1
    assert h.fail_calls[0][4] is False
    assert h.fail_calls[0][3] is None
    assert h.cleanup_calls == []


@pytest.mark.asyncio
async def test_await_existing_ready_hit_verifies_owner_then_snapshot(
    log: LogRecorder, owner_ok
) -> None:
    """不变量:legacy 幂等命中要先只读校验 owner exact,再重读 READY 快照。

    放行会怎样:owner Query 期间 allocation 被回收 / 替换的 TOCTOU 没人堵,输家把一个
    已经换人的实例当成自己的分配返回(§9.22)。

    ★ 变异:把 `verify_existing_ready_delivery` 里 owner 校验与 postcheck 的顺序对调
      → 本条红(先 postcheck 时 owner 尚未 bind)。
    """
    h = Harness()
    existing = make_battle(state=STATE_READY, last_heartbeat_ms=NOW_MS)
    h.repo.battle = existing
    res = await h.await_existing_allocation(MATCH_ID, existing)
    assert res.ds_pod_name == POD
    assert len(owner_ok["verify"]) == 1
    assert owner_ok["verify"][0][3] == BA.OWNER_VERIFY_BUDGET_SEC
    assert "allocate_idempotent_hit" in log.events()


@pytest.mark.asyncio
async def test_verify_existing_ready_delivery_owner_not_exact(
    log: LogRecorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    """不变量:owner 未 exact bind → ErrUnavailable,且**不**进入 postcheck、不清理。

    放行会怎样:输家在唯一 winner 完成归属定案前就交付 READY,客户端拿票进 DS 而
    owner 指向别处(§9.22)。

    ★ 变异:把 `errcode.ErrUnavailable` 改成 `errcode.ErrDSAllocationFailed`
      → 本条红。
    """
    calls: list[int] = []

    async def _bad(*_a: Any, **_k: Any) -> None:
        raise errcode.PandoraError(errcode.ErrUnavailable, "not exact")

    async def _snap(self: Any, *_a: Any, **_k: Any) -> None:
        calls.append(1)

    monkeypatch.setattr(BA, "owner_verify_players_exact", _bad)
    monkeypatch.setattr(BA.AllocateMixin, "verify_ready_allocation_snapshot", _snap)
    h = Harness()
    with pytest.raises(errcode.PandoraError) as ei:
        await h.verify_existing_ready_delivery(
            MATCH_ID, list(PLAYERS), AllocateResult(ds_pod_name=POD, allocation_id="a1")
        )
    assert ei.value.code == errcode.ErrUnavailable
    assert calls == []
    assert "battle_ready_refused_owner_not_exact" in log.events()


@pytest.mark.asyncio
async def test_verify_snapshot_detects_target_drift(log: LogRecorder) -> None:
    """不变量:postcheck 不止"仍然 READY",还要求**还是同一个 exact 实例**。

    放行会怎样:allocation 在 owner Query 期间被回收并由新分配取代时状态照样是 READY,
    但地址 / UID / allocation_id 已经换人;当成幂等命中返回,玩家就拿着旧票去连新实例。

    ★ 变异:把最后一条判据里的 `or got != expected` 删掉 → 本条红。
    """
    h = Harness()
    h.repo.battle = make_battle(last_heartbeat_ms=NOW_MS, allocation_id="alloc-NEW")
    expected = AllocateResult(
        ds_addr=ADDR,
        ds_pod_name=POD,
        allocated_at_ms=h.repo.battle.allocated_at_ms,
        gameserver_uid=UID,
        instance_epoch=5,
        allocation_id="alloc-OLD",
        release_track=releasetrack.STABLE,
    )
    with pytest.raises(errcode.PandoraError) as ei:
        await h.verify_ready_allocation_snapshot(MATCH_ID, expected)
    assert ei.value.code == errcode.ErrUnavailable


@pytest.mark.asyncio
async def test_verify_snapshot_requires_still_ready(log: LogRecorder) -> None:
    """不变量:legacy postcheck 里"不再 ready"与"记录已不在"都必须 fail-closed。

    放行会怎样:一个刚被 sweep 判弃的实例仍被当成可交付目标。
    """
    h = Harness()
    h.repo.battle = None
    with pytest.raises(errcode.PandoraError) as ei:
        await h.verify_ready_allocation_snapshot(MATCH_ID, AllocateResult(ds_pod_name=POD))
    assert ei.value.code == errcode.ErrUnavailable
    h.repo.battle = make_battle(state=STATE_WARMING, last_heartbeat_ms=NOW_MS)
    with pytest.raises(errcode.PandoraError):
        await h.verify_ready_allocation_snapshot(MATCH_ID, AllocateResult(ds_pod_name=POD))


@pytest.mark.asyncio
async def test_verify_snapshot_model_b_uses_authority(log: LogRecorder) -> None:
    """不变量:Model B 的 postcheck 走 auth 权威快照的 `ready_authorized`,不看投影。

    放行会怎样:投影可能被任意一次心跳刷成 ready,而授权 active 尚未建立 —— 票据与
    实例的 exact 绑定被绕过(§9.22)。

    ★ 变异:把 `if self.model_b:` 改成 `if False:`(强制走 legacy 分支)→ 本条红。
    """
    h = Harness(model_b=True)
    h.auth_repo.snapshot = BattleAuthoritySnapshot()  # auth-missing → 不再 ready
    with pytest.raises(errcode.PandoraError) as ei:
        await h.verify_ready_allocation_snapshot(MATCH_ID, AllocateResult(ds_pod_name=POD))
    assert ei.value.code == errcode.ErrUnavailable
    assert h.auth_repo.read_calls == 1


@pytest.mark.asyncio
async def test_verify_snapshot_read_failure_is_unavailable(log: LogRecorder) -> None:
    """不变量:权威读失败 → ErrUnavailable(UNKNOWN 不得冒充"目标已变"或成功)。

    放行会怎样:Redis 抖动被当成"目标漂移"从而触发上层的不同处置,或更糟被当成通过。
    """
    h = Harness()
    h.repo.get_exc = RuntimeError("redis down")
    with pytest.raises(errcode.PandoraError) as ei:
        await h.verify_ready_allocation_snapshot(MATCH_ID, AllocateResult(ds_pod_name=POD))
    assert ei.value.code == errcode.ErrUnavailable


# ══ 取消传播 ═════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_cancelled_error_is_never_swallowed(log: LogRecorder) -> None:
    """不变量:`CancelledError` 必须穿过所有宽 `except BaseException`。

    放行会怎样:停机时这条调用不退出,优雅摘流量 / 在途排空被无限期拖住(§9.16)。

    ★ 变异:把 claim 那一处的 `except asyncio.CancelledError: raise` 删掉
      → 本条红(会被当成 claim 写失败并打成 `claim_write_failed`)。
    """
    h = Harness()
    h.repo.claim_exc = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await run_allocate(h)
    assert "battle_allocate_refused" not in log.events()
