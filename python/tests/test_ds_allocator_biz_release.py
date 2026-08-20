"""`pandorapy/services/ds_allocator/biz_release.py` 回归测试。

对照 Go 侧 `services/battle/ds_allocator/internal/biz/allocator.go` 第 1197–2436 行;
每条断言的行为口径以那份 Go 源码为准,不以 Python 实现"看起来该怎样"为准。

## 本文件盯死的五件事(§9 不变量 4 + 22)

  ① **`allocation_id` 是"POST 结果未知"之后唯一的 fencing token**。GSA POST 超时后
     后端永远不知道那台 GameServer 建没建出来,唯一可依赖的事实是请求里带了一个
     不可变的 `allocation_id` label。所以对账只允许"按它 LIST → 恰零个 / 恰一个"两种
     结论;LIST 失败、能力探测失败、fence 被拒,一律**保留永久 fence**,绝不降级成
     "当作没分配过"。放宽 = 同一 match 出现第二台 DS。
  ② **preactive fenced 释放(epoch=0)是独立一条路**。`fence_preactive_release_expected`
     是它强制的 Redis 线性化前置(失租的旧写者在这里就被挡下);epoch=0 从未准入过
     玩家、从未签出过票,所以必须物理回收 + purge,而**绝不能**伪造一份
     instance-teardown proof;epoch≠0 一律回落带完整证明链的 `release_game_server`。
  ③ **exact 身份比对缺一即拒**。`release_battle_expected` / `finalize_battle_release_expected`
     的 proof 三格(pod_name / instance_uid / instance_epoch)、
     `ensure_durable_release_pod_uid` 的七格身份门,少任何一格都会让"同名 Pod 重建"
     被当成同一台机器 —— 而这条路的下一步是 DELETE。
  ④ **allocation-uncertain 不得被当成"没分配"**。terminal CAS 前必须先把那次
     可能迟到生效的 POST 关掉(空结果也要发一次按 allocation_id 的 release);
     Kafka ACK 之前不许 expire;空结果墓碑**永不** expire/delete。
  ⑤ **`wait_battle_ready` 的超时是 §16.10 的合法兜底,不是掩盖**。deadline 是唯一
     上界(基础设施抖动容忍到下个 tick),而每个提前失败都是**权威判定**;到期动作是
     "回收 pod + 报错让 matchmaker 重查",不是"等够了就假设 DS 已就绪"。

## 依赖策略:全内存替身,不碰 Redis / k8s

本模块测的是**调用方的判定与顺序**(先 fence 还是先删、失败后还 purge 不 purge、
身份不全时到底动没动 K8s),不是 Redis / Agones 的行为。所有替身把调用按发生顺序
写进一条共享磁带 `TAPE`,顺序类断言直接对磁带写死值,而不是"某个 mock 被调用过"
——后者压不住"顺序反了"这类正是本模块要防的缺陷。

★ 每条用例 docstring 的 `★ 变异:` 一行都**真跑过**(改坏产品代码 → 确认本条红 →
  改回 → 确认绿),不是照着代码猜的。
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pandora.ds.v1 import allocator_pb2 as dspb

from pandorapy import battleabort, errcode, placement, releasetrack
from pandorapy.services.ds_allocator import biz_release as BR
from pandorapy.services.ds_allocator.agones_allocator import (
    AuthoritativeGameServerAllocation,
)
from pandorapy.services.ds_allocator.battle_auth import (
    BattleCredentialIdentity,
    BattleExpectedInstance,
    BattleResultAuthorizationProof,
)
from pandorapy.services.ds_allocator.biz_base import (
    STATE_ABANDONED,
    STATE_ALLOCATING,
    STATE_ALLOCATION_ABORT,
    STATE_ALLOCATION_EMPTY_FENCE,
    STATE_ALLOCATION_RECONCILING,
    STATE_ALLOCATION_UNCERTAIN,
    STATE_ENDED,
    STATE_PREACTIVE_RELEASING,
    STATE_READY,
    STATE_RUNNING,
    STATE_WARMING,
    BattleWaitOwnershipLostError,
    ReadyWaitTimeoutError,
)
from pandorapy.services.ds_allocator.departure import BattleAllocationAbortFenceResult
from pandorapy.services.ds_allocator.repo import BattleActiveIndexError

# ── 常量 ────────────────────────────────────────────────────────────────────

MATCH_ID = 700_000_000_000_000_001
POD = "pandora-battle-7f9c-abcde"
UID = "gs-uid-A"
POD_UID = "pod-uid-A"
ALLOC = "5a1f1b7e-4a1e-4a3c-9f4e-2b7c1d8e6a05"
OP_ID = "9c3b8e21-6d4f-4a7b-8e15-2f6a9c0d3b47"
DS_ADDR = "10.244.1.7:7777"
EPOCH = 5
PLAYERS = [800_000_000_000_000_002, 800_000_000_000_000_003]
MAP_ID = 4002
GAME_MODE = "pvp"

EXPECTED = BattleExpectedInstance(
    allocation_id=ALLOC, instance_uid=UID, instance_epoch=EPOCH
)

#: 所有替身共享的调用磁带(按发生顺序)。autouse fixture 每条用例前清空。
TAPE: list[tuple[Any, ...]] = []


def names() -> list[str]:
    """磁带上的事件名序列 —— 顺序断言直接对它写死值。"""
    return [t[0] for t in TAPE]


# ── 日志录音机 ──────────────────────────────────────────────────────────────


class LogRecorder:
    """替换 `plog.get()`。`reason=` 是 §11.3 R2 的固定词表,必须能逐条断言。"""

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

    def reasons(self, event: str) -> list[str]:
        return [f.get("reason", "") for _, e, f in self.records if e == event]


@pytest.fixture(autouse=True)
def log(monkeypatch: pytest.MonkeyPatch) -> LogRecorder:
    TAPE.clear()
    rec = LogRecorder()
    monkeypatch.setattr(BR.plog, "get", lambda **_kw: rec)
    return rec


@pytest.fixture(autouse=True)
def owner_release(monkeypatch: pytest.MonkeyPatch) -> None:
    """把 owner 弱依赖释放接到同一条磁带上 —— 它与删 Pod / 删记录的**相对顺序**是安全边界。"""

    async def _rel(auth, players, self_pod, self_uid, budget) -> None:  # noqa: ANN001
        TAPE.append(("owner_release", list(players), self_pod, self_uid, budget))

    monkeypatch.setattr(BR, "owner_release_abandoned_players_weak", _rel)


# ── 值对象构造 ──────────────────────────────────────────────────────────────


def battle(**over: Any) -> Any:
    """一份**完整合法**的 canonical 投影;每个用例只打坏其中一格。"""
    base: dict[str, Any] = {
        "match_id": MATCH_ID,
        "ds_pod_name": POD,
        "ds_addr": DS_ADDR,
        "state": STATE_RUNNING,
        "player_ids": list(PLAYERS),
        "map_id": MAP_ID,
        "game_mode": GAME_MODE,
        "gameserver_uid": UID,
        "instance_epoch": EPOCH,
        "allocation_id": ALLOC,
        "release_track": releasetrack.STABLE,
        "pod_uid": POD_UID,
        "allocated_at_ms": 1_700_000_000_000,
        "last_heartbeat_ms": 1_700_000_005_000,
    }
    base.update(over)
    return dspb.BattleStorageRecord(**base)


def auth_record(**over: Any) -> Any:
    base: dict[str, Any] = {
        "match_id": MATCH_ID,
        "ds_pod_name": POD,
        "instance_uid": UID,
        "instance_epoch": EPOCH,
        "allocation_id": ALLOC,
        "phase": dspb.BATTLE_AUTH_PHASE_ACTIVE,
    }
    base.update(over)
    return dspb.BattleDSAuthStorageRecord(**base)


class Snap:
    """`BattleAuthoritySnapshot` 的替身:只回放 `ready_authorized` 与四个字段。

    ★ 刻意不用真的 `BattleAuthoritySnapshot`:它的 `ready_authorized` 有十条判据
      (auth/battle 投影一致、gen/jti/writer_epoch 对齐、心跳新鲜度……),那些由
      `tests/test_ds_allocator_battle_auth.py` 专门覆盖。在这里再搭一遍,测的仍是
      那边的逻辑,却会让本文件因为无关的字段变动而假红。
    """

    def __init__(
        self,
        b: Any = None,
        a: Any = None,
        *,
        battle_found: bool | None = None,
        auth_found: bool | None = None,
        ready: bool = False,
        reason: str = "",
    ) -> None:
        self.battle = b
        self.auth = a
        self.battle_found = (b is not None) if battle_found is None else battle_found
        self.auth_found = (a is not None) if auth_found is None else auth_found
        self._ready = ready
        self._reason = reason

    def ready_authorized(self, now_ms_value: int, max_age_ms: int) -> tuple[bool, str]:
        return self._ready, self._reason


def proof(**over: Any) -> BattleResultAuthorizationProof:
    cred = dict(pod_name=POD, instance_uid=UID, instance_epoch=EPOCH)
    cred.update(over)
    return BattleResultAuthorizationProof(
        credential=BattleCredentialIdentity(**cred), authorized_at_ms=1_700_000_000_000
    )


def abort_request(**over: Any) -> battleabort.Request:
    tgt = dict(
        pod_name=POD,
        instance_uid=UID,
        instance_epoch=EPOCH,
        allocation_id=ALLOC,
        release_track=releasetrack.STABLE,
    )
    tgt.update(over.pop("target", {}))
    base: dict[str, Any] = {"match_id": MATCH_ID, "operation_id": OP_ID}
    base.update(over)
    return battleabort.Request(target=placement.Target(**tgt), **base)


def allocation(**over: Any) -> AuthoritativeGameServerAllocation:
    base: dict[str, Any] = {
        "pod_name": POD,
        "instance_uid": UID,
        "pod_uid": POD_UID,
        "instance_epoch": EPOCH,
        "allocation_id": ALLOC,
        "release_track": releasetrack.STABLE,
    }
    base.update(over)
    return AuthoritativeGameServerAllocation(**base)


# ── 替身 ────────────────────────────────────────────────────────────────────


def _pop(script: list, default: Any = None) -> Any:
    """取下一个脚本项:多于一项逐个消费,只剩一项时**粘住**(表达稳态)。"""
    if not script:
        return default
    item = script[0] if len(script) == 1 else script.pop(0)
    if isinstance(item, BaseException):
        raise item
    return item


class FakeRepo:
    """canonical battle 仓替身。**刻意不实现** `AllocationUncertainRepo` 的三个方法。"""

    def __init__(self) -> None:
        self.battles: dict[int, Any] = {}
        self.get_script: list = []
        self.delete_if_matches: list = [True]
        self.update_exc: BaseException | None = None

    async def get_battle(self, match_id: int) -> Any:
        TAPE.append(("get_battle", match_id))
        if self.get_script:
            return _pop(self.get_script)
        return self.battles.get(match_id)

    async def delete_battle(self, match_id: int) -> None:
        TAPE.append(("delete_battle", match_id))
        self.battles.pop(match_id, None)

    async def delete_battle_if_allocation_matches(
        self, match_id: int, allocation_id: str, pod_name: str
    ) -> bool:
        TAPE.append(("delete_if_matches", match_id, allocation_id, pod_name))
        return _pop(self.delete_if_matches, True)

    async def update_battle_keep_ttl(self, match_id: int, retries: int, fn: Any) -> None:
        TAPE.append(("update_keep_ttl", match_id, retries))
        if self.update_exc is not None:
            raise self.update_exc
        rec = self.battles.get(match_id)
        if rec is not None:
            fn(rec)

    async def record_instance_teardown(self, match_id: int, source: Any) -> None:
        TAPE.append(("record_teardown", match_id, source.pod_uid, source.instance_epoch))

    async def expire_battle(self, match_id: int, ttl: float) -> None:
        TAPE.append(("expire_battle", match_id, ttl))

    async def touch_active(self, match_id: int, score_ms: int) -> None:
        TAPE.append(("touch_active", match_id))


class FakeUncertainRepo(FakeRepo):
    """额外实现 `AllocationUncertainRepo` 的三方法(能力探测通过)。"""

    def __init__(self) -> None:
        super().__init__()
        self.fence_uncertain: list = [True]
        self.complete_uncertain: list = [True]
        self.mark_empty: list = [True]

    async def fence_allocation_uncertain_release(
        self, match_id: int, allocation_id: str, alloc: Any
    ) -> bool:
        TAPE.append(("fence_uncertain", match_id, allocation_id, alloc.instance_uid))
        return _pop(self.fence_uncertain, True)

    async def complete_allocation_uncertain_release(
        self, match_id: int, allocation_id: str, instance_uid: str
    ) -> bool:
        TAPE.append(("complete_uncertain", match_id, allocation_id, instance_uid))
        return _pop(self.complete_uncertain, True)

    async def mark_allocation_uncertain_empty_lifecycle_published(
        self, match_id: int, allocation_id: str
    ) -> bool:
        TAPE.append(("mark_empty", match_id, allocation_id))
        return _pop(self.mark_empty, True)


class FakeAuthRepo:
    def __init__(self) -> None:
        self.read_script: list = []
        self.fence_preactive: list = [True]
        self.purge_preactive: list = [True]
        self.terminate_result: list = [True]
        self.expire_terminated: list = [True]

    async def read_authority(self, match_id: int) -> Any:
        TAPE.append(("read_authority", match_id))
        return _pop(self.read_script, Snap())

    async def fence_preactive_release_expected(
        self, match_id: int, expected: BattleExpectedInstance
    ) -> bool:
        TAPE.append(("fence_preactive", match_id, expected.allocation_id))
        return _pop(self.fence_preactive, True)

    async def purge_preactive_released_expected(
        self, match_id: int, expected: BattleExpectedInstance
    ) -> bool:
        TAPE.append(("purge_preactive", match_id, expected.allocation_id))
        return _pop(self.purge_preactive, True)

    async def terminate_result_expected(
        self, match_id: int, expected: BattleExpectedInstance, prf: Any
    ) -> bool:
        TAPE.append(("terminate_result", match_id, expected.instance_uid))
        return _pop(self.terminate_result, True)

    async def expire_result_terminated_expected(
        self, match_id: int, expected: BattleExpectedInstance, prf: Any, ttl: float
    ) -> bool:
        TAPE.append(("expire_terminated", match_id, expected.instance_uid, ttl))
        return _pop(self.expire_terminated, True)


class FakeAlloc:
    """legacy / local 分配器:只有 `release`(按 pod 名,无 UID precondition)。"""

    def __init__(self) -> None:
        self.release_exc: BaseException | None = None

    async def release(self, pod_name: str) -> None:
        TAPE.append(("legacy_release", pod_name))
        if self.release_exc is not None:
            raise self.release_exc


class FakeAuthoritativeAlloc:
    """Model B 权威分配器。**不实现** `resolve_allocation_by_id`(对账能力探测落空)。"""

    def __init__(self) -> None:
        self.release_script: list = []
        self.resolve_pod_uid: list = [POD_UID]

    async def release_expected(self, alloc: Any) -> None:
        TAPE.append(
            ("release_expected", alloc.pod_name, alloc.instance_uid, alloc.allocation_id)
        )
        _pop(self.release_script, None)

    async def resolve_expected_pod_uid(self, alloc: Any) -> str:
        TAPE.append(("resolve_pod_uid", alloc.pod_name, alloc.instance_uid))
        return _pop(self.resolve_pod_uid, POD_UID)


class FakeResolverAlloc(FakeAuthoritativeAlloc):
    """额外实现 `UncertainGameServerAllocationResolver`(对账能力探测通过)。"""

    def __init__(self) -> None:
        super().__init__()
        self.resolve_script: list = [(None, False)]

    async def resolve_allocation_by_id(
        self,
        match_id: int,
        allocation_id: str,
        player_ids: list[int],
        combat_faction_by_player: dict[int, int] | None,
        map_id: int,
        game_mode: str,
    ) -> tuple[Any, bool]:
        TAPE.append(("resolve_by_id", match_id, allocation_id))
        return _pop(self.resolve_script, (None, False))


class FakeAbortRepo:
    def __init__(self) -> None:
        self.fence_script: list = []
        self.complete_script: list = [True]

    async def fence_allocation_abort_expected(self, request: Any) -> Any:
        TAPE.append(("fence_abort", request.match_id, request.operation_id))
        return _pop(
            self.fence_script,
            BattleAllocationAbortFenceResult(battle=battle(), released=False),
        )

    async def complete_allocation_abort_expected(
        self, request: Any, cred_ttl: float, battle_ttl: float
    ) -> bool:
        TAPE.append(("complete_abort", request.match_id, request.operation_id))
        return _pop(self.complete_script, True)


class FakeProofRepo:
    def __init__(self) -> None:
        self.exc: BaseException | None = None

    async def record_allocation_lifecycle_published(self, match_id: int, target: Any) -> None:
        TAPE.append(("lifecycle_marker", match_id, target.allocation_id))
        if self.exc is not None:
            raise self.exc


class Harness(BR.ReleaseMixin):
    """把 `ReleaseMixin` 装配成可调用对象。

    ★ 字段全部按 `AllocatorUsecaseBase` 的名字给,accessor 也照它的语义实现 ——
      mixin 本身**不许**持有状态,这里也就不能替它发明字段名。
    """

    def __init__(
        self,
        *,
        model_b: bool = True,
        repo: Any = None,
        authoritative: Any = None,
        ready_wait_sec: float = 0.05,
    ) -> None:
        self.repo = repo if repo is not None else FakeUncertainRepo()
        self.alloc = FakeAlloc()
        self.auth_repo = FakeAuthRepo()
        self.abort_repo = FakeAbortRepo()
        self.lifecycle_proof_repo = FakeProofRepo()
        self.authoritative_alloc = (
            authoritative if authoritative is not None else FakeResolverAlloc()
        )
        self.model_b = model_b
        self.owner_auth = object()
        self.ds_credential_ttl_sec = 120.0
        self._ready_wait_sec = ready_wait_sec
        #: `deliver_abandoned` 由 `SweepMixin` 提供(别批移植),这里给契约桩。
        self.deliver_result: list = [True]
        self.deliver_calls: list[tuple] = []

    def battle_ttl_sec(self) -> float:
        return 600.0

    def ready_wait_timeout_sec(self) -> float:
        return self._ready_wait_sec

    def heartbeat_timeout_ms(self) -> int:
        return 15_000

    async def deliver_abandoned(
        self,
        match_id: int,
        pod_name: str,
        instance_uid: str,
        player_ids: list[int],
        map_id: int,
        game_mode: str,
    ) -> bool:
        TAPE.append(("deliver_abandoned", match_id, pod_name, instance_uid, list(player_ids)))
        self.deliver_calls.append((match_id, pod_name, instance_uid, list(player_ids)))
        return _pop(self.deliver_result, True)


@pytest.fixture
def fast_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    """把 ready 轮询间隔调小 —— Go 侧 `readyPollInterval` 同为可写模块变量。"""
    monkeypatch.setattr(BR.base, "READY_POLL_INTERVAL_SEC", 0.001)


# ══ ① 模块级判据(白名单 / exact 身份门)═════════════════════════════════════


def test_battle_wait_state_progressable_is_a_whitelist_not_a_blacklist():
    """`battle_wait_state_progressable` 必须是白名单:未知状态一律**不**继续等。

    守的是什么:黑名单实现遇到"另一栈新加的状态"会默认放行,于是一个**已被别人
    接管**的分配会被 owner 当成"还在路上"继续等,超时后再走一遍 cleanup ——
    并发第二路 fence/release 只有幂等最终一致保证,**没有单次调用保证**。

    ★ 变异:把 `biz_release.py` 的 `battle_wait_state_progressable` 改成
      `return state not in (STATE_ENDED, STATE_ABANDONED)` → 本条红。
    """
    for ok in (STATE_ALLOCATING, STATE_WARMING, STATE_READY, STATE_RUNNING):
        assert BR.battle_wait_state_progressable(ok) is True, ok
    for bad in (
        STATE_ENDED,
        STATE_ABANDONED,
        STATE_PREACTIVE_RELEASING,
        STATE_ALLOCATION_ABORT,
        STATE_ALLOCATION_UNCERTAIN,
        STATE_ALLOCATION_RECONCILING,
        STATE_ALLOCATION_EMPTY_FENCE,
        "some_state_added_by_the_other_stack",
        "",
    ):
        assert BR.battle_wait_state_progressable(bad) is False, bad


def test_release_state_allows_pod_uid_backfill_excludes_allocating_and_tombstones():
    """回填 pod_uid 的状态白名单里**没有** `allocating`,也没有各类对账墓碑。

    守的是什么:`allocating` 时 exact 身份尚未定格(GSA POST 还没回),此刻回填
    pod_uid 等于把一个未定的身份写成"权威已确认";uncertain / empty_fence 归对账链
    管,不走这条回收 —— 放行会让两条链同时对同一个 allocation 发 DELETE。

    ★ 变异:在 `release_state_allows_pod_uid_backfill` 的元组里加上 `STATE_ALLOCATING`
      → 本条红。
    """
    for ok in (
        STATE_WARMING,
        STATE_READY,
        STATE_RUNNING,
        STATE_ENDED,
        STATE_ABANDONED,
        STATE_PREACTIVE_RELEASING,
        STATE_ALLOCATION_ABORT,
    ):
        assert BR.release_state_allows_pod_uid_backfill(ok) is True, ok
    for bad in (
        STATE_ALLOCATING,
        STATE_ALLOCATION_UNCERTAIN,
        STATE_ALLOCATION_RECONCILING,
        STATE_ALLOCATION_EMPTY_FENCE,
        "",
    ):
        assert BR.release_state_allows_pod_uid_backfill(bad) is False, bad


def test_exact_release_battle_identity_requires_every_field():
    """七项身份判据逐格必需 —— 少一格就等于允许"同名 Pod 重建"冒充原实例。

    守的是什么:这条判据的**下一步是 DELETE**。`gameserver_uid` / `instance_epoch`
    是 k8s 同名重建后唯一还不相等的东西(教科书式 ABA);`allocation_id` 挡的是
    "记录已被推进到另一次分配";`release_track` 挡的是 Stable/Canary 串轨。
    误删一台 Allocated GameServer = 载人 DS 被杀(红线
    never-delete-allocated-gameserver-20260803)。

    ★ 变异:把 `exact_release_battle_identity` 里的
      `and battle.gameserver_uid == expected.instance_uid` 整行删掉 → 本条红。
    """
    good = battle()
    assert BR.exact_release_battle_identity(good, MATCH_ID, POD, EXPECTED, "") is True

    checks = {
        "match_id": battle(match_id=MATCH_ID + 1),
        "ds_pod_name": battle(ds_pod_name="pandora-battle-7f9c-zzzzz"),
        "gameserver_uid": battle(gameserver_uid="gs-uid-B"),
        "instance_epoch": battle(instance_epoch=EPOCH + 1),
        "allocation_id": battle(allocation_id="1b2c3d4e-5f60-4a71-8b92-c3d4e5f60718"),
        "release_track_invalid": battle(release_track="chaos"),
        "state_not_backfillable": battle(state=STATE_ALLOCATING),
    }
    for label, rec in checks.items():
        assert (
            BR.exact_release_battle_identity(rec, MATCH_ID, POD, EXPECTED, "") is False
        ), f"{label} 被漏判"
    # 期望轨非空时必须逐字相符(空串 = 不检查轨,是 Go 的既定口径)。
    assert (
        BR.exact_release_battle_identity(good, MATCH_ID, POD, EXPECTED, releasetrack.CANARY)
        is False
    )
    assert (
        BR.exact_release_battle_identity(good, MATCH_ID, POD, EXPECTED, releasetrack.STABLE)
        is True
    )
    assert BR.exact_release_battle_identity(None, MATCH_ID, POD, EXPECTED, "") is False


def test_exact_release_identity_snapshot_tolerates_missing_auth_but_not_wrong_auth():
    """auth **允许缺失**(callback auth 的 TTL 可能自然到期),但只要在就必须逐格相符。

    守的是什么:把"auth 缺失"当成拒绝,会让结算中继在 TTL 到期后**永远**收不了 DS
    (pod 泄漏);把"auth 存在但指向别的实例"当成通过,则是拿一份别人的授权去删机器。
    两个方向都错,所以四个字段一个都不能省。

    ★ 变异:把 `exact_release_identity_snapshot` 结尾的
      `and snapshot.auth.instance_epoch == expected.instance_epoch` 删掉 → 本条红。
    """
    b = battle()
    assert BR.exact_release_identity_snapshot(None, MATCH_ID, POD, EXPECTED, "") is False
    assert (
        BR.exact_release_identity_snapshot(Snap(b, auth_found=False), MATCH_ID, POD, EXPECTED, "")
        is True
    ), "auth 缺失必须放行"
    assert (
        BR.exact_release_identity_snapshot(
            Snap(b, auth_record()), MATCH_ID, POD, EXPECTED, ""
        )
        is True
    )
    for label, a in {
        "match_id": auth_record(match_id=MATCH_ID + 1),
        "allocation_id": auth_record(allocation_id="1b2c3d4e-5f60-4a71-8b92-c3d4e5f60718"),
        "pod": auth_record(ds_pod_name="pandora-battle-7f9c-zzzzz"),
        "uid": auth_record(instance_uid="gs-uid-B"),
        "epoch": auth_record(instance_epoch=EPOCH + 1),
    }.items():
        assert (
            BR.exact_release_identity_snapshot(Snap(b, a), MATCH_ID, POD, EXPECTED, "") is False
        ), f"auth.{label} 被漏判"


def test_exact_terminated_release_snapshot_requires_terminating_ended_and_pod_uid():
    """终态 CAS **之后**的重读门比回收门更严:auth 必须在且 TERMINATING,battle 必须
    `ended` 且 `pod_uid` 非空。

    守的是什么:CAS 已经成功、重读却不自洽 = 有人在这中间改了权威。此时**不碰 K8s**,
    让 outbox 用同一 tuple 幂等重试;放行等于拿一份可疑身份去发 DELETE。

    ★ 变异:把 `exact_terminated_release_snapshot` 的
      `and snapshot.auth.phase == dspb.BATTLE_AUTH_PHASE_TERMINATING` 删掉 → 本条红。
    """
    ok_b = battle(state=STATE_ENDED)
    ok_a = auth_record(phase=dspb.BATTLE_AUTH_PHASE_TERMINATING)
    assert (
        BR.exact_terminated_release_snapshot(Snap(ok_b, ok_a), MATCH_ID, POD, EXPECTED) is True
    )
    assert BR.exact_terminated_release_snapshot(None, MATCH_ID, POD, EXPECTED) is False
    assert (
        BR.exact_terminated_release_snapshot(
            Snap(ok_b, auth_found=False), MATCH_ID, POD, EXPECTED
        )
        is False
    ), "auth 必须存在"
    assert (
        BR.exact_terminated_release_snapshot(
            Snap(ok_b, auth_record(phase=dspb.BATTLE_AUTH_PHASE_ACTIVE)),
            MATCH_ID,
            POD,
            EXPECTED,
        )
        is False
    ), "phase 必须 TERMINATING"
    assert (
        BR.exact_terminated_release_snapshot(
            Snap(battle(state=STATE_RUNNING), ok_a), MATCH_ID, POD, EXPECTED
        )
        is False
    ), "battle 必须 ended"
    assert (
        BR.exact_terminated_release_snapshot(
            Snap(battle(state=STATE_ENDED, pod_uid=""), ok_a), MATCH_ID, POD, EXPECTED
        )
        is False
    ), "pod_uid 必须已持久化"


# ══ ② wait_battle_ready:有界 + 每个提前失败都是权威判定 ══════════════════════


async def test_wait_battle_ready_returns_projection_from_the_same_snapshot(fast_poll):
    """ready 授权通过时,出参的四格身份必须来自**同一份**权威快照。

    守的是什么:`ds_addr` 与 `gameserver_uid/instance_epoch/allocation_id` 拼自不同
    时点时,票据与地址会指向不同实例 —— 玩家连上一台 DS 却被它以"票不是我的"踢掉。

    ★ 变异:把 `wait_battle_ready` model-B 分支的 `result = allocate_result_from_battle(b)`
      改成 `allocate_result_from_battle(battle_pb2.BattleStorageRecord())` 之类的空记录
      (实测用 `AllocateResult()`)→ 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [Snap(battle(), auth_record(), ready=True)]
    res = await h.wait_battle_ready(MATCH_ID, POD, ALLOC)
    assert res.ds_addr == DS_ADDR
    assert res.ds_pod_name == POD
    assert res.gameserver_uid == UID
    assert res.instance_epoch == EPOCH
    assert res.allocation_id == ALLOC
    assert res.release_track == releasetrack.STABLE


async def test_wait_battle_ready_transient_read_error_is_bounded_by_deadline(fast_poll):
    """基础设施抖动**只**容忍到 deadline —— 到期抛 `ReadyWaitTimeoutError`,不是永远重试。

    守的是什么(§16.10 合法兜底的判别口诀:到期后"重查权威"才算兜底):这里的到期
    动作是把无界等待收敛成有界并交给调用方回收 pod;删掉 deadline 检查后本协程会
    在 Redis 持续不可读时**永远**转下去,而 matchmaker 那侧已经超时走人 ——
    一台已分配的 DS 再没人回收。

    ★ 变异:把 model-B 抖动分支里的
      `if time.monotonic() >= deadline: raise ReadyWaitTimeoutError` 改成 `pass`
      → 本条红(挂死后被 `asyncio.timeout(2)` 打成 TimeoutError)。
    """
    h = Harness(ready_wait_sec=0.02)
    h.auth_repo.read_script = [errcode.PandoraError(errcode.ErrUnavailable, "redis down")]
    async with asyncio.timeout(2):
        with pytest.raises(ReadyWaitTimeoutError):
            await h.wait_battle_ready(MATCH_ID, POD, ALLOC)
    assert names().count("read_authority") >= 2, "抖动必须至少重试过一次"


async def test_wait_battle_ready_purged_authority_fails_at_once_with_ownership_lost(fast_poll):
    """battle 键消失 = 判死回收链已 purge:**立即**失败,且带所有权哨兵。

    守的是什么:两件事。① 这是**权威判定**不是抖动,混进"容忍到下个 tick"会把
    「本分配已失效」拖成空转满 ready_wait(历史上 141.85s 总耗时的根因);
    ② `cause` 必须是 `BattleWaitOwnershipLostError` —— owner 据此**不再** cleanup,
    因为回收权已经属于别人了。

    ★ 变异:把该分支的 `cause=BattleWaitOwnershipLostError()` 删掉 → 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [Snap(None, auth_found=False)]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.wait_battle_ready(MATCH_ID, POD, ALLOC)
    assert ei.value.code == errcode.ErrDSAllocationFailed
    assert isinstance(ei.value.cause, BattleWaitOwnershipLostError)
    assert names() == ["read_authority"], "权威判定必须一次到位,不得再转一圈"


async def test_wait_battle_ready_allocation_superseded_fails_at_once(fast_poll, log):
    """镜像里的 `allocation_id` 已被换掉 = 本次分配被取代:立即放弃,不得继续等。

    守的是什么:继续等下去会在超时后对**新分配**的 pod 走 cleanup —— 杀掉的是别人
    刚刚分配好的机器。

    ★ 变异:把 `if allocation_id != "" and b.allocation_id != allocation_id:` 改成
      `if False:` → 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [
        Snap(battle(allocation_id="1b2c3d4e-5f60-4a71-8b92-c3d4e5f60718"), auth_record())
    ]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.wait_battle_ready(MATCH_ID, POD, ALLOC)
    assert isinstance(ei.value.cause, BattleWaitOwnershipLostError)
    assert BR.REASON_WAIT_ALLOC_SUPERSEDED in log.reasons("battle_ready_wait_aborted")


async def test_wait_battle_ready_reclaimed_state_fails_at_once(fast_poll, log):
    """墓碑状态(这里用 `preactive_release_pending`)= 回收链已接管:立即失败。

    守的是什么:该状态下 owner 若继续等、超时再 cleanup,就是对同一 allocation
    起第二路 fence/release。

    ★ 变异:把状态白名单判断 `if not battle_wait_state_progressable(b.state):`
      改成 `if b.state == STATE_ENDED:` → 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [Snap(battle(state=STATE_PREACTIVE_RELEASING), auth_record())]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.wait_battle_ready(MATCH_ID, POD, ALLOC)
    assert isinstance(ei.value.cause, BattleWaitOwnershipLostError)
    assert BR.REASON_WAIT_STATE_RECLAIMED in log.reasons("battle_ready_wait_aborted")


async def test_wait_battle_ready_auth_phase_fenced_beats_a_healthy_looking_battle(fast_poll, log):
    """battle 投影还是 `running`,但 auth 相位已 TERMINATING —— 仍必须立即失败。

    守的是什么:这正是"半状态"。授权面已被永久 fence,该分配不可能再被授权 ready;
    只看 battle 投影会让 owner 一直等到超时,期间那台 DS 谁也不敢动。

    ★ 变异:把相位门里的 `or phase == dspb.BATTLE_AUTH_PHASE_QUARANTINED` 连同
      TERMINATING 一并改成 `if False:` → 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [
        Snap(battle(), auth_record(phase=dspb.BATTLE_AUTH_PHASE_TERMINATING))
    ]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.wait_battle_ready(MATCH_ID, POD, ALLOC)
    assert isinstance(ei.value.cause, BattleWaitOwnershipLostError)
    assert BR.REASON_WAIT_AUTH_PHASE_FENCED in log.reasons("battle_ready_wait_aborted")


async def test_wait_battle_ready_missing_auth_outside_grace_fails(fast_poll, log):
    """缺 auth 且状态已过 allocating/warming = 不可授权:立即失败,不进宽限计时。

    守的是什么:与 `AbandonIfStale` 的 allocation-grace 契约同判。放宽成"再等等"
    会让一个**根本没有授权**的分配把 ready_wait 耗满。

    ★ 变异:把 `if b.state != STATE_ALLOCATING and b.state != STATE_WARMING:` 改成
      `if False:` → 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [Snap(battle(state=STATE_RUNNING), auth_found=False)]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.wait_battle_ready(MATCH_ID, POD, ALLOC)
    assert isinstance(ei.value.cause, BattleWaitOwnershipLostError)
    assert BR.REASON_WAIT_AUTH_OUTSIDE_GRACE in log.reasons("battle_ready_wait_aborted")


async def test_wait_battle_ready_missing_auth_in_grace_is_bounded_by_heartbeat_timeout(
    fast_poll, log, monkeypatch
):
    """缺 auth 且在 warming 宽限内:允许等,但上界是 `heartbeat_timeout` 而**不是**
    完整的 ready_wait。

    守的是什么:凭据投递半失败(Redis stage→K8s PATCH→delivered CAS 中断)是确定性
    残留,不是慢。用完整冷加载宽限空等 120s 只会把这局的失败推迟两分钟,期间玩家
    停在 loading。

    ★ 变异:把 `grace_sec = self.heartbeat_timeout_ms() / 1000.0` 改成
      `self.ready_wait_timeout_sec()` → 本条红(宽限变得比 ready_wait 还长,
      永远走不到 provision_dead 分支)。
    """
    h = Harness(ready_wait_sec=5.0)
    # 心跳超时压到 20ms:宽限一到就该判 provision 已死。
    monkeypatch.setattr(h, "heartbeat_timeout_ms", lambda: 20)
    h.auth_repo.read_script = [Snap(battle(state=STATE_WARMING), auth_found=False)]
    async with asyncio.timeout(2):
        with pytest.raises(errcode.PandoraError) as ei:
            await h.wait_battle_ready(MATCH_ID, POD, ALLOC)
    assert isinstance(ei.value.cause, BattleWaitOwnershipLostError)
    assert BR.REASON_WAIT_AUTH_PROVISION_DEAD in log.reasons("battle_ready_wait_aborted")


async def test_wait_battle_ready_legacy_record_gone_fails_at_once(fast_poll, log):
    """legacy 面镜像消失同样立即失败(滚动共存期两条面必须同判)。

    守的是什么:legacy 副本若把"记录没了"当成抖动继续等,同一 match 上就会出现
    两条对 ready 的判断,而只有一条会去 cleanup。

    ★ 变异:把 legacy 分支的 `if legacy is None:` 改成 `if False:` → 本条红
      (随后 `legacy.allocation_id` 抛 AttributeError,同样不是 PandoraError)。
    """
    h = Harness(model_b=False)
    h.repo.get_script = [None]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.wait_battle_ready(MATCH_ID, POD, ALLOC)
    assert ei.value.code == errcode.ErrDSAllocationFailed
    assert isinstance(ei.value.cause, BattleWaitOwnershipLostError)
    assert BR.REASON_WAIT_RECORD_GONE in log.reasons("battle_ready_wait_aborted")


async def test_wait_battle_ready_legacy_requires_post_allocation_heartbeat(fast_poll):
    """legacy ready 判定要求 `last_heartbeat_ms` **严格大于** `allocated_at_ms`。

    守的是什么:分配时写下的初始化时间戳不是心跳。判等放行 = 把"刚建好、还没起来"
    的 DS 当成 ready 交给玩家,客户端连上去是黑屏。

    ★ 变异:把 `battle_ready_for_pod` 的 `b.last_heartbeat_ms > allocated_at_ms`
      改成 `>=` → 本条红。
    """
    same = battle(state=STATE_READY, allocated_at_ms=1_700_000_000_000)
    same.last_heartbeat_ms = same.allocated_at_ms
    assert BR.battle_ready_for_pod(same, POD, MATCH_ID, same.allocated_at_ms) is False
    later = battle(state=STATE_READY, allocated_at_ms=1_700_000_000_000)
    later.last_heartbeat_ms = later.allocated_at_ms + 1
    assert BR.battle_ready_for_pod(later, POD, MATCH_ID, later.allocated_at_ms) is True

    h = Harness(model_b=False, ready_wait_sec=0.02)
    h.repo.get_script = [same]
    async with asyncio.timeout(2):
        with pytest.raises(ReadyWaitTimeoutError):
            await h.wait_battle_ready(MATCH_ID, POD, ALLOC)


async def test_wait_battle_ready_propagates_caller_cancellation(fast_poll, log):
    """调用方(matchmaker)撤了 → `CancelledError` 原样重抛,不被宽 except 吞掉。

    守的是什么:取消是停机 / 放弃的控制流。吞掉它这个协程会在 matchmaker 早已走人后
    继续跑到 ready_wait 结束,并在那时用一份过期的判断去 cleanup。

    ★ 变异:把 `tick()` 里的 `except asyncio.CancelledError: ... raise` 改成
      `... return` → 本条红。
    """
    h = Harness(ready_wait_sec=30.0)
    h.auth_repo.read_script = [Snap(battle(), auth_record(), ready=False)]
    task = asyncio.create_task(h.wait_battle_ready(MATCH_ID, POD, ALLOC))
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert BR.REASON_WAIT_CTX_DONE in log.reasons("battle_ready_wait_aborted")


async def test_fail_ready_wait_timeout_only_owner_cleans_up(fast_poll):
    """`owner=False`(joiner)时**不**做任何回收,只报错。

    守的是什么:回收所有权属 owner。joiner 也去 cleanup = 同一 allocation 上两路并发
    fence/release,而那条链只有幂等最终一致保证,没有单次调用保证。

    ★ 变异:把 `if owner:` 改成 `if True:` → 本条红。
    """
    h = Harness()
    with pytest.raises(errcode.PandoraError) as ei:
        await h.fail_ready_wait_timeout(MATCH_ID, ALLOC, POD, allocation(), owner=False)
    assert ei.value.code == errcode.ErrDSAllocationFailed
    assert names() == [], f"joiner 不得触碰任何权威/编排面,实际={names()}"


# ══ ③ cleanup:fence 先于 K8s,purge 后于确认 ═══════════════════════════════


async def test_cleanup_allocated_battle_fences_before_k8s_and_purges_after(fast_poll):
    """顺序必须是 **fence → release → purge**,一格都不能调换。

    守的是什么:旧实现"先删 Redis 再 Release",DELETE 超时会留下"Redis 已无 claim、
    GSA 仍可能活着"的窗口 —— 下一个请求就能对同 match 发第二次 POST(§9.22 ④)。

    ★ 变异:在 `_cleanup_allocated_battle_inner` 里把
      `await self.release_fenced_preactive_game_server(...)` 那段整体移到
      `fence_preactive_release_expected` 之前 → 本条红。
    """
    h = Harness()
    alloc = allocation(instance_epoch=0)
    await h.cleanup_allocated_battle(MATCH_ID, ALLOC, POD, alloc)
    assert names() == ["fence_preactive", "release_expected", "purge_preactive"]


async def test_cleanup_allocated_battle_does_not_touch_k8s_when_fence_refused(fast_poll):
    """fence 没拿到(失租的旧写者 / 身份不符)→ **绝不**触碰 K8s,也不 purge。

    守的是什么:`fence_preactive_release_expected` 是这条路的 Redis 线性化前置。
    fence 被拒说明这份身份已经不是当前赢家;继续删就是拿旧身份去杀新实例。

    ★ 变异:把 `if fence_err is not None or not fenced:` 改成
      `if fence_err is not None:` → 本条红。
    """
    h = Harness()
    h.auth_repo.fence_preactive = [False]
    await h.cleanup_allocated_battle(MATCH_ID, ALLOC, POD, allocation(instance_epoch=0))
    assert names() == ["fence_preactive"]


async def test_cleanup_allocated_battle_keeps_fence_when_release_unconfirmed(fast_poll):
    """ReleaseExpected 结果未知 → 保留永久 fence,**不得** purge。

    守的是什么:purge 掉墓碑就等于宣布"这次分配确实没了",而实际上 GSA 可能还活着。
    保留 fence 是下一轮 sweep 能重来的唯一依据。

    ★ 变异:把 release 的 `except BaseException ... return` 里的 `return` 删掉
      (让它继续往下 purge)→ 本条红。
    """
    h = Harness()
    h.authoritative_alloc.release_script = [TimeoutError("k8s delete timeout")]
    await h.cleanup_allocated_battle(MATCH_ID, ALLOC, POD, allocation(instance_epoch=0))
    assert names() == ["fence_preactive", "release_expected"]


async def test_cleanup_allocated_battle_refuses_without_exact_identity(fast_poll, log):
    """fence 已写但 `allocation is None` → 停在 fence 上,不删任何东西。

    守的是什么:没有 exact 身份就没有删除权(§9.22 ②)。按名字删 = 误杀一台正在
    服务玩家的 Allocated GameServer。

    ★ 变异:把 `if pod_name == "" or allocation is None:` 改成 `if pod_name == "":`
      → 本条红。
    """
    h = Harness()
    await h.cleanup_allocated_battle(MATCH_ID, ALLOC, POD, None)
    assert names() == ["fence_preactive"]
    assert "ready_wait_cleanup_identity_missing" in log.events()


async def test_cleanup_allocated_battle_legacy_keeps_release_right_on_index_error(fast_poll):
    """legacy 面:权威 key 已按 fencing 条件删除、只是派生索引清理失败 → **仍要**释放 pod。

    守的是什么:`BattleActiveIndexError.deleted` 携带的是 Go 的 `return true, err`,
    即"释放对应 GameServer 的权利"。丢掉它,那台 Pod 会一直挂着,而权威 key 已经没了
    —— 再没人能证明它属于谁。

    ★ 变异:把 `except BattleActiveIndexError` 分支里的 `deleted = exc.deleted`
      改成 `deleted = False` → 本条红。
    """
    h = Harness(model_b=False)
    err = BattleActiveIndexError("zrem failed")
    err.deleted = True
    h.repo.delete_if_matches = [err]
    await h.cleanup_allocated_battle(MATCH_ID, ALLOC, POD, None)
    assert names() == ["delete_if_matches", "legacy_release"]


async def test_cleanup_allocated_battle_legacy_skips_release_when_claim_not_ours(fast_poll):
    """legacy 面:`delete_battle_if_allocation_matches` 返回 False(claim 已不是我们的)
    → **不**释放。

    守的是什么:删不掉说明那条 claim 属于另一次分配;照样 release 就是杀别人的机器。

    ★ 变异:把 `if not deleted or pod_name == "": return` 改成
      `if pod_name == "": return` → 本条红。
    """
    h = Harness(model_b=False)
    h.repo.delete_if_matches = [False]
    await h.cleanup_allocated_battle(MATCH_ID, ALLOC, POD, None)
    assert names() == ["delete_if_matches"]


# ══ ④ release_game_server / preactive fenced 释放 ════════════════════════════


async def test_release_game_server_refuses_incomplete_tuple(fast_poll, log):
    """expected 元组缺任一格 → `ErrInvalidState`,**零** K8s 调用。

    守的是什么:身份不全一律不删(宁可占位)。这条闸放行的代价是按残缺身份发 DELETE。

    ★ 变异:把 `or allocation.instance_epoch == 0` 从那条 or 链里删掉 → 本条红。
    """
    h = Harness()
    for label, alloc in {
        "no_uid": allocation(instance_uid=""),
        "no_epoch": allocation(instance_epoch=0),
        "no_alloc_id": allocation(allocation_id=""),
        "pod_mismatch": allocation(pod_name="pandora-battle-7f9c-zzzzz"),
    }.items():
        TAPE.clear()
        with pytest.raises(errcode.PandoraError) as ei:
            await h.release_game_server(MATCH_ID, POD, alloc)
        assert ei.value.code == errcode.ErrInvalidState, label
        assert names() == [], f"{label} 仍触碰了外部面:{names()}"
    with pytest.raises(errcode.PandoraError):
        await h.release_game_server(0, POD, allocation())
    assert BR.REASON_RELEASE_TUPLE_INCOMPLETE in log.reasons("battle_gameserver_release_refused")


async def test_release_game_server_refuses_when_pod_uid_cannot_be_persisted(fast_poll, log):
    """拿不到 durable Pod UID → `ErrInvalidState`,**绝不按名字删**。

    守的是什么:k8s 同名 Pod 可以被重建(ABA)。没有 pod_uid 就没有 DELETE
    precondition —— 这时候删,删的可能是刚重建出来、正在服务玩家的那台。

    ★ 变异:把 `if allocation.pod_uid == "":` 的第二道检查(resolve 之后那道)
      改成 `if False:` → 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [Snap(battle(pod_uid=""), auth_record())]
    h.authoritative_alloc.resolve_pod_uid = [""]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.release_game_server(MATCH_ID, POD, allocation(pod_uid=""))
    # resolve 失败先被 ensure_durable_release_pod_uid 判成可重试的 ErrUnavailable。
    assert ei.value.code == errcode.ErrUnavailable
    assert "release_expected" not in names()
    assert BR.REASON_POD_UID_RESOLVE_FAILED in log.reasons("battle_pod_uid_preflight_refused")


async def test_release_game_server_records_teardown_proof_after_release(fast_poll):
    """teardown proof 必须写在 `release_expected` **成功之后**。

    守的是什么:proof 的语义是"某个 epoch 的实例已拆除"。先写 proof 再删,一旦删除
    失败就留下一份**假证明**,后续对账会据此相信那台机器已经没了。

    ★ 变异:把 `await self.repo.record_instance_teardown(...)` 整段移到
      `await self.authoritative_alloc.release_expected(allocation)` 之前 → 本条红。
    """
    h = Harness()
    await h.release_game_server(MATCH_ID, POD, allocation())
    assert names() == ["release_expected", "record_teardown"]
    assert TAPE[1] == ("record_teardown", MATCH_ID, POD_UID, EPOCH)


async def test_release_game_server_legacy_uses_pod_name_only(fast_poll, log):
    """legacy 面按 pod 名回收,且必须留下删除权留证(precondition=pod_name_only)。

    守的是什么:红线 never-delete-allocated-gameserver-20260803 要求"每次真正回收 DS
    都留下删了什么"。legacy 面没有 UID precondition,更需要事后能核对。

    ★ 变异:把 legacy 分支里那条 `plog.get().info("battle_gameserver_released", ...)`
      删掉 → 本条红。
    """
    h = Harness(model_b=False)
    await h.release_game_server(MATCH_ID, POD, None)
    assert names() == ["legacy_release"]
    fields = log.find("battle_gameserver_released")
    assert fields["authority"] == "legacy"
    assert fields["precondition"] == "pod_name_only"


async def test_release_fenced_preactive_never_fabricates_a_teardown_proof(fast_poll):
    """epoch=0 的 preactive 回收:物理删 + 不写 teardown proof。

    守的是什么:epoch=0 表示 PrepareCredential 还没分配实例纪元 —— 它**从未准入过
    玩家、从未签出过票**。凭空造一条"有凭据的实例已拆除"证明,会让后续对账相信一个
    从未存在的凭据。

    ★ 变异:在 `release_fenced_preactive_game_server` 末尾补一句
      `await self.repo.record_instance_teardown(...)` → 本条红。
    """
    h = Harness()
    await h.release_fenced_preactive_game_server(MATCH_ID, POD, allocation(instance_epoch=0))
    assert names() == ["release_expected"]


async def test_release_fenced_preactive_falls_back_for_nonzero_epoch(fast_poll):
    """epoch≠0 一律回落 `release_game_server`(带完整证明链)。

    守的是什么:epoch≠0 的实例可能已经签出过票、准入过玩家,回收它必须留下 teardown
    proof。走 preactive 那条捷径等于把有凭据的实例按"从未存在"处理。

    ★ 变异:把 `if allocation is None or allocation.instance_epoch != 0:` 改成
      `if allocation is None:` → 本条红。
    """
    h = Harness()
    await h.release_fenced_preactive_game_server(MATCH_ID, POD, allocation(instance_epoch=3))
    assert names() == ["release_expected", "record_teardown"]


async def test_release_fenced_preactive_refuses_incomplete_fenced_tuple(fast_poll):
    """preactive 路同样要求 exact 元组(model_b + pod + uid + allocation_id 齐备)。

    ★ 变异:把该 or 链里的 `or allocation.instance_uid == ""` 删掉 → 本条红。
    """
    h = Harness()
    for alloc in (
        allocation(instance_epoch=0, instance_uid=""),
        allocation(instance_epoch=0, allocation_id=""),
        allocation(instance_epoch=0, pod_name="pandora-battle-7f9c-zzzzz"),
    ):
        TAPE.clear()
        with pytest.raises(errcode.PandoraError) as ei:
            await h.release_fenced_preactive_game_server(MATCH_ID, POD, alloc)
        assert ei.value.code == errcode.ErrInvalidState
        assert names() == []


async def test_reconcile_preactive_release_reports_three_distinct_outcomes(fast_poll):
    """三态结果必须能区分「不适用」与「外部未确认」,不能都塌成 False。

    守的是什么(复审 P1-2):调用方据此决定要不要登记 allocation-bound 退避。合成同一个
    False 时,要么该退避的不退避(持续打爆控制面),要么不该退避的退避了(回收被无谓
    推迟一个心跳窗)。

    ★ 变异:把 `if not fenced:` 分支的 `return PreactiveReleaseOutcome.SKIPPED` 改成
      `return PreactiveReleaseOutcome.UNCONFIRMED` → 本条红。
    """
    # ① 身份不完整 → SKIPPED,零外部副作用
    h = Harness()
    assert (
        await h.reconcile_preactive_release(battle(allocation_id=""))
        == BR.PreactiveReleaseOutcome.SKIPPED
    )
    assert names() == []

    # ② fence 明确拒绝 → SKIPPED(零副作用,不该退避)
    h = Harness()
    h.auth_repo.read_script = [Snap(battle(), auth_record())]
    h.auth_repo.fence_preactive = [False]
    assert (
        await h.reconcile_preactive_release(battle()) == BR.PreactiveReleaseOutcome.SKIPPED
    )
    assert "release_expected" not in names()

    # ③ purge 未确认 → UNCONFIRMED
    h = Harness()
    h.auth_repo.read_script = [Snap(battle(), auth_record())]
    h.auth_repo.purge_preactive = [False]
    assert (
        await h.reconcile_preactive_release(battle()) == BR.PreactiveReleaseOutcome.UNCONFIRMED
    )

    # ④ 全链成功 → COMPLETED
    h = Harness()
    h.auth_repo.read_script = [Snap(battle(), auth_record())]
    assert await h.reconcile_preactive_release(battle()) == BR.PreactiveReleaseOutcome.COMPLETED


# ══ ⑤ allocation-uncertain 对账链(唯一 fencing token = allocation_id)═════════


async def test_reconcile_allocation_uncertain_is_fail_closed_without_capabilities(fast_poll, log):
    """repo / resolver 任一不支持对账 → 打 Error 并返回 False,永久 fence 原样留着。

    守的是什么:能力探测本身就是业务逻辑。降级成"按名字清理"就是 §9.22 明令禁止的
    "用一次 apiserver 抖动换来可以安全地当作没分配过的结论"。滚动升级期的旧 writer
    必须继续停在那道 fence 上。

    ★ 变异:把 `if not repo_ok or not resolver_ok:` 改成 `if False:` → 本条红。
    """
    # repo 不支持
    h = Harness(repo=FakeRepo())
    assert await h.reconcile_allocation_uncertain(battle(state=STATE_ALLOCATION_UNCERTAIN)) is False
    assert names() == []
    assert "allocation_uncertain_reconciler_unavailable_fail_closed" in log.events()

    # resolver 不支持
    h = Harness(authoritative=FakeAuthoritativeAlloc())
    assert await h.reconcile_allocation_uncertain(battle(state=STATE_ALLOCATION_UNCERTAIN)) is False
    assert names() == []


async def test_reconcile_allocation_uncertain_keeps_fence_when_list_fails(fast_poll, log):
    """按 allocation_id 的 LIST 失败 → 返回 False,**不**推进任何终态。

    守的是什么(核心 ①):LIST 失败时后端仍然不知道那台 GameServer 建没建出来。
    此时推进 CAS 等于宣布"没分配过",而那次 POST 可能随时生效 —— 同一 match 出现
    第二台 DS。

    ★ 变异:把 resolve 的 `except BaseException ... return False` 改成
      `... allocation, found = None, False`(继续往下走空结果分支)→ 本条红。
    """
    h = Harness()
    h.authoritative_alloc.resolve_script = [TimeoutError("apiserver timeout")]
    assert await h.reconcile_allocation_uncertain(battle(state=STATE_ALLOCATION_UNCERTAIN)) is False
    assert names() == ["resolve_by_id"], f"不得有任何后续副作用:{names()}"
    assert "allocation_uncertain_resolve_failed_will_retry" in log.events()


async def test_reconcile_allocation_uncertain_empty_result_still_closes_the_post_window(
    fast_poll,
):
    """LIST 权威空结果:也必须先按 `allocation_id` 发一次 release,**再**做终态 CAS。

    守的是什么:空结果只说明"此刻看不见",不说明"那次 POST 不会迟到生效"。
    DeleteCollection+LIST 这一步关掉的正是迟到窗口,必须在发布终态**之前**关掉。
    顺序反了就会先宣布终态、再关窗,中间那一瞬仍能冒出第二台 DS。

    ★ 变异:把空结果分支里 `await self.authoritative_alloc.release_expected(allocation)`
      整段删掉 → 本条红。
    """
    h = Harness()
    h.authoritative_alloc.resolve_script = [(None, False)]
    h.repo.get_script = [None]  # 末尾的 terminal 重读(本条不关心)
    await h.reconcile_allocation_uncertain(battle(state=STATE_ALLOCATION_UNCERTAIN))
    assert names()[:3] == ["resolve_by_id", "release_expected", "complete_uncertain"]
    # 空结果的 terminal CAS 必须以空 instance_uid 提交(没有实例可绑)。
    assert TAPE[2] == ("complete_uncertain", MATCH_ID, ALLOC, "")
    # release 只能按 allocation_id 发(没有 pod/uid 可用)。
    assert TAPE[1] == ("release_expected", "", "", ALLOC)


async def test_reconcile_allocation_uncertain_exact_result_fences_before_release(fast_poll):
    """LIST 恰一份:顺序必须是 **fence → release → complete**。

    守的是什么:fence 是 Redis 侧的线性化点,它把这次回收锁给唯一赢家。先删后 fence
    的话,两个副本会同时对同一台 GameServer 发 DELETE 并各自推进终态。

    ★ 变异:把 `fence_allocation_uncertain_release` 那段整体移到
      `release_expected` 之后 → 本条红。
    """
    h = Harness()
    h.authoritative_alloc.resolve_script = [(allocation(), True)]
    h.repo.get_script = [None]
    await h.reconcile_allocation_uncertain(battle(state=STATE_ALLOCATION_UNCERTAIN))
    assert names()[:4] == [
        "resolve_by_id",
        "fence_uncertain",
        "release_expected",
        "complete_uncertain",
    ]
    assert TAPE[3] == ("complete_uncertain", MATCH_ID, ALLOC, UID)


async def test_reconcile_allocation_uncertain_fence_refused_stops_before_k8s(fast_poll, log):
    """fence 被拒 → 停在 fence 上,**不**触碰 K8s,不推终态。

    ★ 变异:把 `if fence_err is not None or not fenced:` 改成
      `if fence_err is not None:` → 本条红。
    """
    h = Harness()
    h.authoritative_alloc.resolve_script = [(allocation(), True)]
    h.repo.fence_uncertain = [False]
    assert await h.reconcile_allocation_uncertain(battle(state=STATE_ALLOCATION_UNCERTAIN)) is False
    assert names() == ["resolve_by_id", "fence_uncertain"]
    assert "allocation_uncertain_exact_release_fence_failed" in log.events()


async def test_reconcile_allocation_uncertain_reconciling_requires_full_identity(fast_poll):
    """`allocation_reconcile_release_pending` 续跑要求 pod/uid/pod_uid 齐全且 epoch=0。

    守的是什么:这条分支会直接发 exact DELETE。缺 `pod_uid` 就没有 precondition;
    `instance_epoch != 0` 说明它根本不属于这条 preactive 对账链。

    ★ 变异:把该分支里的 `or battle.pod_uid == ""` 删掉 → 本条红。
    """
    h = Harness()
    for over in (
        {"pod_uid": ""},
        {"ds_pod_name": ""},
        {"gameserver_uid": ""},
        {"instance_epoch": 3},
    ):
        TAPE.clear()
        kw: dict = {"instance_epoch": 0}
        kw.update(over)
        rec = battle(state=STATE_ALLOCATION_RECONCILING, **kw)
        assert await h.reconcile_allocation_uncertain(rec) is False
        assert names() == [], f"{over} 仍触碰了外部面:{names()}"


async def test_reconcile_allocation_uncertain_rejects_unknown_state(fast_poll):
    """既不是 uncertain 也不是 reconciling 的记录一律不处理。

    ★ 变异:把结尾的 `else: return False` 改成 `else: pass` → 本条红。
    """
    h = Harness()
    assert await h.reconcile_allocation_uncertain(battle(state=STATE_RUNNING)) is False
    assert names() == []


async def test_publish_reconciled_abandoned_requires_kafka_ack_before_expire(fast_poll):
    """`deliver_abandoned` 没成功 → **不得** expire battle 记录。

    守的是什么(§9 不变量 4):Redis battle 记录在 Kafka ACK 之前保持永久。反过来做
    = 补偿事件丢了而记录已过期,玩家段位永远不回滚,而且没有任何告警。

    ★ 变异:把 `if not await self.deliver_abandoned(...): return False` 里的
      `return False` 删掉 → 本条红。
    """
    h = Harness()
    term = battle(state=STATE_ABANDONED, instance_epoch=0)
    h.auth_repo.read_script = [Snap(term, auth_found=False)]
    h.deliver_result = [False]
    assert await h.publish_reconciled_allocation_abandoned(term) is False
    assert "expire_battle" not in names()


async def test_publish_reconciled_abandoned_expires_only_after_ack(fast_poll):
    """ACK 之后才 expire,并带上 `battle_ttl_sec()`(终态保留期)。

    ★ 变异:把 `await self.repo.expire_battle(battle.match_id, self.battle_ttl_sec())`
      改成 `expire_battle(battle.match_id, 0)` → 本条红。
    """
    h = Harness()
    term = battle(state=STATE_ABANDONED, instance_epoch=0)
    h.auth_repo.read_script = [Snap(term, auth_found=False)]
    assert await h.publish_reconciled_allocation_abandoned(term) is True
    assert names() == [
        "read_authority",
        "deliver_abandoned",
        "expire_battle",
    ]
    assert TAPE[2] == ("expire_battle", MATCH_ID, 600.0)


async def test_publish_reconciled_abandoned_rejects_snapshot_with_surviving_auth(fast_poll, log):
    """重读快照里 auth 还在 → 拒绝发布终态。

    守的是什么:auth 尚存说明授权面还没被回收链清干净,这份 ABANDONED 不是最终事实。
    据此发补偿事件会让 battle_result 提前回滚一局还没结束的对战。

    ★ 变异:把重读校验里的 `or snapshot.auth_found` 删掉 → 本条红。
    """
    h = Harness()
    term = battle(state=STATE_ABANDONED, instance_epoch=0)
    h.auth_repo.read_script = [Snap(term, auth_record())]
    assert await h.publish_reconciled_allocation_abandoned(term) is False
    assert "deliver_abandoned" not in names()
    assert "allocation_uncertain_terminal_snapshot_rejected" in log.events()


async def test_empty_allocation_tombstone_is_marked_and_never_expired(fast_poll):
    """空 LIST 结果的终态:写 empty 墓碑 + 重排索引,**永不** expire/delete。

    守的是什么(核心 ①的收尾):空 LIST 足以解放玩家(补偿事件已发),却**不足以**
    交出清理权 —— 原来那次超时的 POST 仍可能在这次 LIST 之后生效。墓碑退休只能靠
    将来显式的静默期证明,不能靠 TTL 让它自己消失。

    ★ 变异:在 `publish_reconciled_allocation_abandoned` 的空 uid 分支末尾补一句
      `await self.repo.expire_battle(battle.match_id, self.battle_ttl_sec())` → 本条红。
    """
    h = Harness()
    term = battle(state=STATE_ABANDONED, instance_epoch=0, gameserver_uid="", pod_uid="")
    h.auth_repo.read_script = [Snap(term, auth_found=False)]
    assert await h.publish_reconciled_allocation_abandoned(term) is True
    assert names() == ["read_authority", "deliver_abandoned", "mark_empty", "touch_active"]
    assert "expire_battle" not in names()
    assert "delete_battle" not in names()


async def test_empty_tombstone_without_capability_is_fail_closed(fast_poll):
    """空 uid 分支下 repo 不支持对账 → 返回 False(不假装墓碑已登记)。

    ★ 变异:把 `if not isinstance(self.repo, AllocationUncertainRepo): return False`
      改成 `return True` → 本条红。
    """
    h = Harness(repo=FakeRepo())
    term = battle(state=STATE_ABANDONED, instance_epoch=0, gameserver_uid="", pod_uid="")
    h.auth_repo.read_script = [Snap(term, auth_found=False)]
    assert await h.publish_reconciled_allocation_abandoned(term) is False


async def test_resume_empty_allocation_tombstone_keeps_polling_forever(fast_poll):
    """空墓碑续跑:只做 exact release + 重排索引,**永不** expire。

    守的是什么:一个迟到的原始 POST 没有凭据、准入不了玩家,但只要它变得可见就必须
    被收掉。所以这块墓碑要一直被 sweep 看见。

    ★ 变异:在 `resume_empty_allocation_tombstone` 末尾补
      `await self.repo.expire_battle(battle.match_id, self.battle_ttl_sec())` → 本条红。
    """
    h = Harness()
    tomb = battle(
        state=STATE_ALLOCATION_EMPTY_FENCE,
        instance_epoch=0,
        ds_pod_name="",
        gameserver_uid="",
        pod_uid="",
    )
    h.auth_repo.read_script = [Snap(tomb, auth_found=False)]
    assert await h.resume_empty_allocation_tombstone(tomb) is True
    assert names() == ["read_authority", "release_expected", "touch_active"]
    assert TAPE[1] == ("release_expected", "", "", ALLOC)


async def test_resume_reconciled_abandoned_requires_pod_uid_when_instance_known(fast_poll):
    """续跑时若记录带 `gameserver_uid`,则 pod/pod_uid 必须齐 —— 否则整条放弃。

    守的是什么:带 uid 的重复释放走的是 exact DELETE 路径,缺 pod_uid 就没有
    precondition。退化成"只按 allocation_id 删"会让一台同名重建的机器被顺手删掉。

    ★ 变异:把 `if battle.ds_pod_name == "" or battle.pod_uid == "": return False`
      改成 `pass` → 本条红。
    """
    h = Harness()
    rec = battle(state=STATE_ABANDONED, instance_epoch=0, pod_uid="")
    h.auth_repo.read_script = [Snap(rec, auth_found=False)]
    assert await h.resume_reconciled_allocation_abandoned(rec) is False
    assert names() == ["read_authority"]


# ══ ⑥ ReleaseBattle(legacy)/ Model-B 禁用 ═══════════════════════════════════


async def test_release_battle_refuses_match_id_only_under_model_b(fast_poll, log):
    """Model B 下 match_id-only 回收一律拒绝。

    守的是什么:旧调用方拿着 match_id 就来删,可能误杀同 match **重建出来的新 UID**。
    Model B 的正常结算必须走 `release_battle_expected` 的 expected tuple。

    ★ 变异:把 `if self.model_b:` 改成 `if False:` → 本条红。
    """
    h = Harness(model_b=True)
    with pytest.raises(errcode.PandoraError) as ei:
        await h.release_battle(MATCH_ID, "completed")
    assert ei.value.code == errcode.ErrInvalidArg
    assert names() == []
    assert BR.REASON_RELEASE_MODEL_B_UNSUPPORTED in log.reasons("battle_release_refused")


async def test_release_battle_legacy_refuses_every_model_b_fence_state(fast_poll, log):
    """legacy 副本遇到五种 Model-B 永久 fence 状态 → `ErrUnavailable`,只读跳过。

    守的是什么:滚动共存期同版本但跑 legacy 配置的 writer,绝不能把另一个 Model-B
    writer 写下的永久 POST fence 清掉 —— 那正是"未知结果"唯一的证据。

    ★ 变异:把该元组里的 `STATE_ALLOCATION_UNCERTAIN` 删掉 → 本条红。
    """
    h = Harness(model_b=False)
    for state in (
        STATE_ALLOCATION_UNCERTAIN,
        STATE_ALLOCATION_RECONCILING,
        STATE_ALLOCATION_EMPTY_FENCE,
        STATE_PREACTIVE_RELEASING,
        STATE_ALLOCATION_ABORT,
    ):
        TAPE.clear()
        h.repo.battles[MATCH_ID] = battle(state=state)
        with pytest.raises(errcode.PandoraError) as ei:
            await h.release_battle(MATCH_ID, "completed")
        assert ei.value.code == errcode.ErrUnavailable, state
        assert names() == ["get_battle"], f"{state} 仍触碰了编排面:{names()}"
    assert BR.REASON_RELEASE_FENCED in log.reasons("battle_release_refused")


async def test_release_battle_legacy_releases_owner_before_deleting_the_record(fast_poll):
    """owner 释放必须在 `delete_battle` **之前** —— 删完就取不到名单和身份了。

    守的是什么(INC-20260804-001 缺口⑦):不释放 owner 的话,对局正常结算后 owner
    仍是 BATTLE/ADMITTED 指向一台刚被销毁的 DS,login 的 query-first 会一直把玩家
    指回去 —— 玩家打完副本回不了大厅。

    ★ 变异:把 `await owner_release_abandoned_players_weak(...)` 整段移到
      `await self.repo.delete_battle(match_id)` 之后 → 本条红。
    """
    h = Harness(model_b=False)
    h.repo.battles[MATCH_ID] = battle(state=STATE_ENDED)
    await h.release_battle(MATCH_ID, "completed")
    assert names() == ["get_battle", "legacy_release", "owner_release", "delete_battle"]
    assert TAPE[2][1] == PLAYERS
    assert TAPE[2][2:] == (POD, UID, BR.OWNER_RELEASE_BUDGET_SEC)


async def test_release_battle_legacy_is_idempotent_on_missing_mirror(fast_poll):
    """镜像不存在视为已释放,直接返回(幂等)。

    ★ 变异:把 `if battle is None: ... return` 改成 `pass` → 本条红(AttributeError)。
    """
    h = Harness(model_b=False)
    await h.release_battle(MATCH_ID, "completed")
    assert names() == ["get_battle"]


# ══ ⑦ ReleaseBattleExpected / FinalizeBattleReleaseExpected ══════════════════


async def test_release_battle_expected_requires_each_proof_field(fast_poll, log):
    """proof 三格 + reason 逐项必需,任一不符 → `ErrInvalidArg` 且零外部副作用。

    守的是什么:这是 battle_result outbox 与 allocator 权威之间**唯一**的身份握手。
    放宽任一格就是允许一份别的实例的结算证明来回收这台机器。

    ★ 变异:把 `or proof.credential.instance_epoch != expected.instance_epoch`
      从那条 or 链里删掉 → 本条红。
    """
    h = Harness()
    cases = {
        "pod": (proof(pod_name="pandora-battle-7f9c-zzzzz"), "completed"),
        "uid": (proof(instance_uid="gs-uid-B"), "completed"),
        "epoch": (proof(instance_epoch=EPOCH + 1), "completed"),
        "reason": (proof(), "abandoned"),
    }
    for label, (prf, reason) in cases.items():
        TAPE.clear()
        with pytest.raises(errcode.PandoraError) as ei:
            await h.release_battle_expected(MATCH_ID, reason, POD, EXPECTED, prf)
        assert ei.value.code == errcode.ErrInvalidArg, label
        assert names() == [], f"{label} 仍触碰了权威面:{names()}"
    assert BR.REASON_RELEASE_PROOF_INCOMPLETE in log.reasons("battle_release_refused")


async def test_release_battle_expected_stops_when_terminal_cas_misses(fast_poll, log):
    """终态 CAS 未命中(stable 身份已变)→ `ErrDSAllocationFailed`,**不碰 K8s**。

    守的是什么:CAS 未命中说明这个 match 的当前实例已经不是 proof 里那台(同名 Pod
    重建 / 已被判弃 / 已回收)。继续删 = 杀掉接替它的那台。

    ★ 变异:把 `if not terminated:` 后的 `raise` 改成 `pass` → 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [Snap(battle(), auth_record())]
    h.auth_repo.terminate_result = [False]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.release_battle_expected(MATCH_ID, "completed", POD, EXPECTED, proof())
    assert ei.value.code == errcode.ErrDSAllocationFailed
    assert "release_expected" not in names()
    assert BR.REASON_RELEASE_IDENTITY_CHANGED in log.reasons("battle_release_refused")


async def test_release_battle_expected_refuses_when_reread_is_not_self_consistent(fast_poll, log):
    """终态 CAS 成功、重读却不自洽 → `ErrUnavailable`,本轮不碰 K8s。

    守的是什么:CAS 与重读之间有人改了权威。此时拿这份可疑身份去删 Pod 是最坏选择;
    正确做法是让 outbox 用同一 tuple 幂等重试。

    ★ 变异:把 `if not exact_terminated_release_snapshot(...)` 改成 `if False:`
      → 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [
        Snap(battle(), auth_record()),  # ensure_durable_release_pod_uid 的前置读
        Snap(battle(state=STATE_RUNNING), auth_record()),  # CAS 后重读:phase 还不是 TERMINATING
    ]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.release_battle_expected(MATCH_ID, "completed", POD, EXPECTED, proof())
    assert ei.value.code == errcode.ErrUnavailable
    assert "release_expected" not in names()
    assert BR.REASON_RELEASE_SNAPSHOT_STALE in log.reasons("battle_release_refused")


async def test_release_battle_expected_releases_owner_only_after_gameserver_is_gone(fast_poll):
    """owner 释放必须排在 `release_game_server` 成功**之后**。

    守的是什么(安全边界①):提前释放会在旧 DS 可能仍在跑时放行新归属 = 双 DS
    (§9.22)。K8s UID precondition 删除返回,才等于"本实例回收已确认"。

    ★ 变异:把 `await owner_release_abandoned_players_weak(...)` 整段移到
      `await self.release_game_server(...)` 之前 → 本条红。
    """
    h = Harness()
    ended = battle(state=STATE_ENDED)
    h.auth_repo.read_script = [
        Snap(ended, auth_record()),
        Snap(ended, auth_record(phase=dspb.BATTLE_AUTH_PHASE_TERMINATING)),
    ]
    await h.release_battle_expected(MATCH_ID, "completed", POD, EXPECTED, proof())
    assert names() == [
        "read_authority",
        "terminate_result",
        "read_authority",
        "release_expected",
        "record_teardown",
        "owner_release",
    ]
    assert TAPE[5][1] == PLAYERS


async def test_release_battle_expected_requires_model_b(fast_poll, log):
    """未启用 Redis 权威 → `ErrInvalidState`,零副作用(battle_result 会持续重试)。

    ★ 变异:把 `if not self.model_b or self.auth_repo is None ...` 改成 `if False:`
      → 本条红。
    """
    h = Harness(model_b=False)
    with pytest.raises(errcode.PandoraError) as ei:
        await h.release_battle_expected(MATCH_ID, "completed", POD, EXPECTED, proof())
    assert ei.value.code == errcode.ErrInvalidState
    assert names() == []
    assert BR.REASON_RELEASE_AUTHORITY_OFF in log.reasons("battle_release_refused")


async def test_finalize_battle_release_expected_never_calls_kubernetes(fast_poll):
    """phase2 只恢复 TTL,**绝不**调用 Kubernetes。

    守的是什么:phase1 已经确认删过那台 GameServer 了。phase2 再删一次,删到的可能是
    同名重建后的新实例;而 phase2 是 outbox 会反复重放的那一段。

    ★ 变异:在 `finalize_battle_release_expected` 里补一句
      `await self.authoritative_alloc.release_expected(...)` → 本条红。
    """
    h = Harness()
    await h.finalize_battle_release_expected(MATCH_ID, POD, EXPECTED, proof())
    assert names() == ["expire_terminated"]
    assert TAPE[0] == ("expire_terminated", MATCH_ID, UID, 600.0)


async def test_finalize_battle_release_expected_requires_proof_identity(fast_poll, log):
    """finalize 的 proof 三格同样逐项必需。

    ★ 变异:把 `or proof.credential.instance_uid != expected.instance_uid` 删掉
      → 本条红。
    """
    h = Harness()
    for prf in (
        proof(pod_name="pandora-battle-7f9c-zzzzz"),
        proof(instance_uid="gs-uid-B"),
        proof(instance_epoch=EPOCH + 1),
    ):
        TAPE.clear()
        with pytest.raises(errcode.PandoraError) as ei:
            await h.finalize_battle_release_expected(MATCH_ID, POD, EXPECTED, prf)
        assert ei.value.code == errcode.ErrInvalidArg
        assert names() == []
    assert BR.REASON_FINALIZE_PROOF_INCOMPLETE in log.reasons("battle_release_refused")


async def test_finalize_battle_release_expected_reports_unconfirmed_tombstone(fast_poll, log):
    """墓碑没能恢复有界 TTL → `ErrUnavailable`(outbox 同 tuple 重试),不是静默成功。

    守的是什么:静默返回成功会让 battle_result 认为收尾完成,而 Redis 里那三把永久
    key 谁也不会再来清 —— 一条永远不过期的墓碑。

    ★ 变异:把 `if not expired:` 后的 `raise` 改成 `pass` → 本条红。
    """
    h = Harness()
    h.auth_repo.expire_terminated = [False]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.finalize_battle_release_expected(MATCH_ID, POD, EXPECTED, proof())
    assert ei.value.code == errcode.ErrUnavailable
    assert BR.REASON_FINALIZE_TOMBSTONE_PENDING in log.reasons("battle_release_refused")


# ══ ⑧ ensure_durable_release_pod_uid ═════════════════════════════════════════


async def test_ensure_pod_uid_returns_persisted_value_without_touching_k8s(fast_poll):
    """权威记录里已有 pod_uid → 直接返回,零 K8s 调用。

    ★ 变异:把 `if snapshot.battle.pod_uid != "": return snapshot.battle.pod_uid`
      删掉 → 本条红(会多出 resolve/backfill 调用)。
    """
    h = Harness()
    h.auth_repo.read_script = [Snap(battle(), auth_record())]
    got = await h.ensure_durable_release_pod_uid(MATCH_ID, POD, EXPECTED, "")
    assert got == POD_UID
    assert names() == ["read_authority"]


async def test_ensure_pod_uid_identity_changed_is_terminal_not_retryable(fast_poll, log):
    """exact 身份已变 → `ErrDSAllocationFailed`(终态),不是可重试的 `ErrUnavailable`。

    守的是什么:两个错误码的下游行为完全不同。把"身份变了"报成"暂时不可用",
    outbox 会拿着一份**过期身份**永远重试下去,而正确动作是让上层放弃这次回收。

    ★ 变异:把该分支的 `errcode.ErrDSAllocationFailed` 改成 `errcode.ErrUnavailable`
      → 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [Snap(battle(gameserver_uid="gs-uid-B"), auth_record())]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.ensure_durable_release_pod_uid(MATCH_ID, POD, EXPECTED, "")
    assert ei.value.code == errcode.ErrDSAllocationFailed
    assert names() == ["read_authority"]
    fields = log.find("battle_pod_uid_preflight_refused")
    assert fields["reason"] == BR.REASON_POD_UID_IDENTITY_CHANGED
    assert fields["want_uid"] == UID
    assert fields["got_uid"] == "gs-uid-B"


async def test_ensure_pod_uid_resolve_failure_stays_retryable(fast_poll):
    """K8s 精确回读不到 pod_uid → `ErrUnavailable`,**不创建**永久 TERMINATING fence。

    守的是什么:legacy 记录缺 pod_uid 是历史遗留,不是身份变了。报成终态会让这台机器
    再没有人来收。

    ★ 变异:把 resolve 失败分支的 `errcode.ErrUnavailable` 改成
      `errcode.ErrDSAllocationFailed` → 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [Snap(battle(pod_uid=""), auth_record())]
    h.authoritative_alloc.resolve_pod_uid = [""]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.ensure_durable_release_pod_uid(MATCH_ID, POD, EXPECTED, "")
    assert ei.value.code == errcode.ErrUnavailable
    assert names() == ["read_authority", "resolve_pod_uid"]


async def test_ensure_pod_uid_verifies_authority_after_backfill(fast_poll, log):
    """回填之后必须**再读一次权威**核对,不一致就拒绝。

    守的是什么:CAS 成功不等于读得到 —— 中间可能有另一条链把记录推进走了,而我们
    马上要拿这个 pod_uid 去发 DELETE。

    ★ 变异:把 `verified = await self.auth_repo.read_authority(match_id)` 之后的
      整段校验改成 `if False:` → 本条红。
    """
    h = Harness()
    h.repo.battles[MATCH_ID] = battle(pod_uid="")
    h.auth_repo.read_script = [
        Snap(battle(pod_uid=""), auth_record()),  # 前置读:缺 pod_uid
        Snap(battle(pod_uid="pod-uid-OTHER"), auth_record()),  # 回填后重读:被别人改了
    ]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.ensure_durable_release_pod_uid(MATCH_ID, POD, EXPECTED, "")
    assert ei.value.code == errcode.ErrUnavailable
    fields = log.find("battle_pod_uid_preflight_refused")
    assert fields["reason"] == BR.REASON_POD_UID_VERIFY_FAILED
    assert fields["want_pod_uid"] == POD_UID
    assert fields["got_pod_uid"] == "pod-uid-OTHER"


async def test_ensure_pod_uid_backfill_callback_refuses_concurrent_identity_change(fast_poll):
    """乐观锁回调内再校验一次身份:CAS 期间记录被换掉 → 整体失败。

    守的是什么:`update_battle_keep_ttl` 的回调在冲突时会**整体重跑**,重跑时读到的
    可能已经是另一次分配的记录。不再校验就会把 A 的 pod_uid 写进 B 的记录。

    ★ 变异:把 `_backfill` 里的
      `if not exact_release_battle_identity(...): raise ...` 删掉 → 本条红。
    """
    h = Harness()
    # 事务里读到的是另一个实例(uid 已变)。
    h.repo.battles[MATCH_ID] = battle(pod_uid="", gameserver_uid="gs-uid-B")
    h.auth_repo.read_script = [Snap(battle(pod_uid=""), auth_record())]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.ensure_durable_release_pod_uid(MATCH_ID, POD, EXPECTED, "")
    assert ei.value.code == errcode.ErrUnavailable  # backfill_failed 包装成可重试
    assert h.repo.battles[MATCH_ID].pod_uid == "", "绝不能把 pod_uid 写进别的实例"


# ══ ⑨ AbortPreactiveBattle(matchmaker saga 补偿)════════════════════════════


async def test_abort_refuses_incomplete_request(fast_poll, log):
    """请求形状不全(缺 operation_id / 轨非法 / epoch=0)→ `ErrInvalidArg`,零副作用。

    守的是什么:`operation_id` 是这次中止的幂等键。写法不归一会被当成两次不同的
    operation,于是同一份补偿被执行两遍。

    ★ 变异:把 `if not request.complete():` 改成 `if False:` → 本条红。
    """
    h = Harness()
    for req in (
        abort_request(operation_id=""),
        abort_request(target={"release_track": "chaos"}),
        abort_request(target={"instance_epoch": 0}),
        abort_request(target={"allocation_id": ""}),
    ):
        TAPE.clear()
        with pytest.raises(errcode.PandoraError) as ei:
            await h.abort_preactive_battle(req)
        assert ei.value.code == errcode.ErrInvalidArg
        assert names() == []
    assert BR.REASON_ABORT_REQUEST_INCOMPLETE in log.reasons("battle_allocation_abort_refused")


async def test_abort_requires_model_b_authority(fast_poll, log):
    """未启用 Redis 权威 → `ErrInvalidState`,不做任何补偿。

    ★ 变异:把 `if not self.model_b or self.abort_repo is None ...` 改成 `if False:`
      → 本条红。
    """
    h = Harness(model_b=False)
    with pytest.raises(errcode.PandoraError) as ei:
        await h.abort_preactive_battle(abort_request())
    assert ei.value.code == errcode.ErrInvalidState
    assert names() == []
    assert BR.REASON_ABORT_AUTHORITY_OFF in log.reasons("battle_allocation_abort_refused")


async def test_abort_persists_pod_uid_before_creating_the_permanent_fence(fast_poll):
    """battle 记录还在时,必须先把 pod_uid 落权威,**再**建永久 ABORT fence。

    守的是什么:fence 一旦建立,这次分配就永久不可路由;若此时还没有 exact pod_uid,
    后面就只剩"按名字删"或者"永远删不掉"两条路,两条都不可接受。

    ★ 变异:把 `if preflight.battle_found:` 那段 preflight 整体删掉 → 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [
        Snap(battle(), auth_record()),  # abort 自己的 preflight 读
        Snap(battle(), auth_record()),  # ensure_durable_release_pod_uid 的读
    ]
    await h.abort_preactive_battle(abort_request())
    assert names()[:3] == ["read_authority", "read_authority", "fence_abort"]


async def test_abort_ack_loss_replay_short_circuits_without_touching_k8s(fast_poll):
    """journal 已是 RELEASED(ACK-loss 重放)→ 只补保留期清理,**不再**碰 K8s。

    守的是什么:永久 journal 的存在就是"外部副作用已经做完"的证据。重放时再删一次,
    删到的可能是同 match 重建出来的新实例。

    ★ 变异:把 `if fence.released:` 分支末尾的 `return` 删掉 → 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [Snap(None, auth_found=False, battle_found=False)]
    h.abort_repo.fence_script = [
        BattleAllocationAbortFenceResult(battle=None, released=True)
    ]
    await h.abort_preactive_battle(abort_request())
    assert names() == ["read_authority", "fence_abort", "complete_abort"]


async def test_abort_refuses_k8s_delete_without_exact_pod_authority(fast_poll, log):
    """fence 已写却拿不到 exact pod 权威(pod_uid 空)→ `ErrInvalidState`,不发 DELETE。

    守的是什么:这是"宁可占位也不误删"的最后一道闸。没有 pod_uid 的 DELETE 只能按名字
    发,而同名 Pod 可能已经是别人的了。

    ★ 变异:把该判据里的 `or battle.pod_uid == ""` 删掉 → 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [Snap(None, auth_found=False, battle_found=False)]
    h.abort_repo.fence_script = [
        BattleAllocationAbortFenceResult(battle=battle(pod_uid=""), released=False)
    ]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.abort_preactive_battle(abort_request())
    assert ei.value.code == errcode.ErrInvalidState
    assert "release_expected" not in names()
    assert BR.REASON_ABORT_FENCE_IDENTITY_MISSING in log.reasons(
        "battle_allocation_abort_refused"
    )


async def test_abort_full_path_order_release_then_lifecycle_then_owner_then_marker(fast_poll):
    """完整补偿链的顺序:release → lifecycle → owner 释放 → 永久 marker → complete。

    守的是什么:① owner 释放必须在 GameServer 回收确认之后(否则双 DS);
    ② lifecycle 投递失败要能整体重试,所以它排在 marker 之前;
    ③ marker 是"补偿事件已发"的永久见证,没有它 fence 不能放开。

    ★ 变异:把 `record_allocation_lifecycle_published` 那段移到
      `deliver_abandoned` 之前 → 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [
        Snap(battle(), auth_record()),
        Snap(battle(), auth_record()),
    ]
    await h.abort_preactive_battle(abort_request())
    assert names() == [
        "read_authority",
        "read_authority",
        "fence_abort",
        "release_expected",
        "record_teardown",
        "deliver_abandoned",
        "owner_release",
        "lifecycle_marker",
        "complete_abort",
    ]


async def test_abort_lifecycle_publish_pending_blocks_completion(fast_poll, log):
    """`deliver_abandoned` 失败 → `ErrUnavailable`,**不**释放 owner、**不**置 RELEASED。

    守的是什么(§9 不变量 4):GameServer 已回收但补偿事件没发出去,这局的段位回滚
    还没着落。此时把 journal 置 RELEASED 就等于宣布补偿完成,sweep 再也不会重试。

    ★ 变异:把 `if not await self.deliver_abandoned(...)` 后的 `raise` 改成 `pass`
      → 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [Snap(battle(), auth_record()), Snap(battle(), auth_record())]
    h.deliver_result = [False]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.abort_preactive_battle(abort_request())
    assert ei.value.code == errcode.ErrUnavailable
    assert "complete_abort" not in names()
    assert "owner_release" not in names()
    assert BR.REASON_ABORT_LIFECYCLE_PENDING in log.reasons("battle_allocation_abort_refused")


async def test_abort_completion_not_confirmed_is_retryable(fast_poll, log):
    """外部副作用全做完、仅永久日志置 RELEASED 未确认 → `ErrUnavailable`(同 request 重试)。

    守的是什么:这一步不确认就返回成功,journal 会停在非 RELEASED,下一次重放会**再走
    一遍**外部删除。返回可重试错误让调用方带着同一个 operation_id 回来,是幂等的。

    ★ 变异:把 `if not completed:` 后的 `raise` 改成 `pass` → 本条红。
    """
    h = Harness()
    h.auth_repo.read_script = [Snap(battle(), auth_record()), Snap(battle(), auth_record())]
    h.abort_repo.complete_script = [False]
    with pytest.raises(errcode.PandoraError) as ei:
        await h.abort_preactive_battle(abort_request())
    assert ei.value.code == errcode.ErrUnavailable
    assert BR.REASON_ABORT_COMPLETION_PENDING in log.reasons("battle_allocation_abort_refused")


# ══ ⑩ 常量口径 ══════════════════════════════════════════════════════════════


def test_owner_release_budget_is_a_single_shared_constant():
    """三条 owner 释放路径共用同一个 2s 预算常量,不是三处各写 `2.0`。

    守的是什么:它是弱依赖的**尾延迟上界**。三处各写字面量时,改动漏一处的表现是
    "某条路上 owner 释放悄悄多花几倍时间",而没有任何告警。

    ★ 变异:把 `OWNER_RELEASE_BUDGET_SEC = 2.0` 改成 `2.5` → 本条红。
    """
    assert BR.OWNER_RELEASE_BUDGET_SEC == 2.0


def test_preactive_release_outcome_values_are_distinct():
    """三态枚举值互不相等 —— 合并任意两个都会让调用方的退避判定失准。

    ★ 变异:把 `UNCONFIRMED = 1` 改成 `UNCONFIRMED = 0` → 本条红。
    """
    vals = {
        BR.PreactiveReleaseOutcome.SKIPPED,
        BR.PreactiveReleaseOutcome.UNCONFIRMED,
        BR.PreactiveReleaseOutcome.COMPLETED,
    }
    assert len(vals) == 3
    assert int(BR.PreactiveReleaseOutcome.SKIPPED) == 0
    assert int(BR.PreactiveReleaseOutcome.UNCONFIRMED) == 1
    assert int(BR.PreactiveReleaseOutcome.COMPLETED) == 2
