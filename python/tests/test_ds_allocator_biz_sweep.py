"""`pandorapy/services/ds_allocator/biz_sweep.py` 回归测试。

对照 Go 侧 `services/battle/ds_allocator/internal/biz/allocator.go` 第 3359–4048 行
(`SetSweepWriterLease` / `sweepIsLeader` / `RunHeartbeatSweep` / `heartbeatSweepTick` /
`noteSweepDeferral` / `sweepDeferralActive` / `pruneSweepDeferrals` / `stuckReconcileState` /
`sweepRoundBudget` / `sweepOnce` / `reconcileActiveIndexIfDue` / `deliverAbandoned` /
`rosterGateArmable`)。每条断言的行为口径以那份 Go 源码为准,不以 Python 实现"看起来
该怎样"为准。

## 本文件盯死的五件事(§9 不变量 4 + 21)

  ① **非 leader 不得执行任何清扫写**(§9.21 单写者)。热备副本照样服务 Heartbeat /
     AllocateBattle,但**一条 sweep 写都不能发** —— 滚动升级重叠窗口里两个扫描者
     同时对同一批 abandoned 发 terminate/release,是把"补偿"变成"并发误删"的路径。
     领导权门必须排在**本地内存卫生之后、任何存储读写之前**。

  ② **退避是进程内非权威调度提示,绝不能写进 active ZSET 的 score**
     (真实事故 INC-20260724-001)。三条硬约束,任何一条被破坏都**不会报错**:
       - score 的权威语义是 `last_heartbeat_ms`,挪作调度时间戳会让
         「score ≤ 阈值 ⇔ 心跳超时」这条判据整体失效;
       - abort fence 与 auth quarantine 用 **score==0** 表示「下一轮必须立即对账」,
         派生索引重建专门用 `ZADD NX` 保护这个哨兵 —— 写退避会把最高危状态的
         **立即**对账降级成**延迟**对账;
       - `touch_active` 是无条件 `ZADD`(无 NX/XX/GT),迟到的退避写会把已被
         `remove_active` 的终态项**复活**回补偿 outbox,重复投递 lifecycle。
     进程内表还顺带保住「重启即扫」的既定意图(重启即清空)。

  ③ **单轮清扫有墙钟预算,且至少推进一项**。无预算的一轮会把下一 tick 直接叠上来;
     `processed > 0` 这个前置条件保证预算再小也不会活锁(否则一项都推进不了 =
     §9.4 补偿链完全停摆,而且**没有任何报错**)。

  ④ **`deliver_abandoned` 是 at-least-once outbox 的闸门**。投递失败必须保留在
     active(下轮重试),成功才 expire/移出;legacy 路径的 `first_abandon` 保证
     Release 全局恰好一次,而补偿投递可以幂等重试任意多次。

  ⑤ **心跳超时判定的时间边界**。`last_heartbeat_ms > cutoff` 里的 `>` 是"恰好等于
     阈值即判失联";写成 `>=` 会让恰好卡在边界的失联局每轮都被当成"只是索引陈旧"
     修一下 —— 永远不判弃,§9.4 补偿永远不发生。warming 与 active 走两个阈值。

## 依赖策略:全内存替身,不碰 Redis / k8s

测的是**调用方的判定与顺序**(轮到谁扫、写不写 ZSET、这一项该不该处理、失败后
留不留在 outbox),不是 Redis / Agones 的行为。所有替身把调用按发生顺序写进一条
共享磁带 `TAPE`,顺序类断言直接对磁带写死值 —— "某个 mock 被调用过"压不住"顺序反了"
和"多写了一次"这两类正是本文件要防的缺陷。

替身与 Harness 风格沿用同批已落地的 `tests/test_ds_allocator_biz_release.py`
(`TAPE` / `_pop` 脚本 / `LogRecorder` / `Harness` 只装一个 mixin)。

★ 每条用例 docstring 的 `★ 变异:` 一行都是**可执行的具体改法**;标注 `(已实跑)`
  的那些是真改坏产品代码 → 确认本条红 → 改回 → 确认绿,不是照着代码猜的。
"""

from __future__ import annotations

import asyncio
import datetime as _dt
from typing import Any

import pytest
from pandora.ds.v1 import allocator_pb2 as dspb

from pandorapy import errcode, releasetrack
from pandorapy.services.ds_allocator import biz_sweep as BS
from pandorapy.services.ds_allocator.agones_allocator import (
    ReleaseDeletionPendingError,
)
from pandorapy.services.ds_allocator.battle_auth import (
    BattleAbandonResult,
    BattleExpectedInstance,
)
from pandorapy.services.ds_allocator.biz_base import (
    ACTIVE_INDEX_RECONCILE_INTERVAL_SEC,
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
)
from pandorapy.services.ds_allocator.biz_release import PreactiveReleaseOutcome

# ── 常量 ────────────────────────────────────────────────────────────────────

MATCH_ID = 700_000_000_000_000_001
MATCH_ID_2 = 700_000_000_000_000_002
MATCH_ID_3 = 700_000_000_000_000_003
POD = "pandora-battle-7f9c-abcde"
UID = "gs-uid-A"
POD_UID = "pod-uid-A"
ALLOC = "5a1f1b7e-4a1e-4a3c-9f4e-2b7c1d8e6a05"
ALLOC_2 = "1b2c3d4e-5f60-4a71-8b92-c3d4e5f60718"
DS_ADDR = "10.244.1.7:7777"
EPOCH = 5
PLAYERS = [800_000_000_000_000_002, 800_000_000_000_000_003]
MAP_ID = 4002
GAME_MODE = "pvp"

#: 固定墙钟(ms)。`BS.now_ms` 被 autouse fixture 钉死到它 —— 心跳阈值是**逐毫秒**的
#: 判据,用真实时钟写不出"恰好等于阈值"这条边界断言。
FIXED_NOW_MS = 1_800_000_000_000

HEARTBEAT_TIMEOUT_SEC = 15.0
READY_WAIT_SEC = 10.0
SWEEP_INTERVAL_SEC = 5.0
BATTLE_TTL_SEC = 600.0

#: 两个阈值的解析结果,断言里直接引用(而不是在断言里重算一遍公式)。
ACTIVE_CUTOFF_MS = FIXED_NOW_MS - 15_000
WARMING_CUTOFF_MS = FIXED_NOW_MS - 10_000

EXPECTED = BattleExpectedInstance(
    allocation_id=ALLOC, instance_uid=UID, instance_epoch=EPOCH
)

#: 所有替身共享的调用磁带(按发生顺序)。autouse fixture 每条用例前清空。
TAPE: list[tuple[Any, ...]] = []


def names() -> list[str]:
    """磁带上的事件名序列 —— 顺序断言直接对它写死值。"""
    return [t[0] for t in TAPE]


def _pop(script: list, default: Any = None) -> Any:
    """取下一个脚本项:多于一项逐个消费,只剩一项时**粘住**(表达稳态)。"""
    if not script:
        return default
    item = script[0] if len(script) == 1 else script.pop(0)
    if isinstance(item, BaseException):
        raise item
    return item


# ── 日志录音机 ──────────────────────────────────────────────────────────────


class LogRecorder:
    """替换 `plog.get()`。事件名是 Loki 面板的 key,必须能逐条断言。"""

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

    def count(self, event: str) -> int:
        return sum(1 for _, e, _ in self.records if e == event)


@pytest.fixture(autouse=True)
def log(monkeypatch: pytest.MonkeyPatch) -> LogRecorder:
    TAPE.clear()
    rec = LogRecorder()
    monkeypatch.setattr(BS.plog, "get", lambda *_a, **_kw: rec)
    return rec


@pytest.fixture(autouse=True)
def frozen_wall_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """钉死墙钟(`now_ms`),**不动** `time.monotonic`。

    ★ 这是模块头「两个时钟」形变的直接体现:墙钟只用来和 Redis 里各副本写入的
      `last_heartbeat_ms` 比较,必须可钉死才写得出逐毫秒边界断言;而预算 / 退避到期 /
      索引节流用的是单调钟,把它一起钉死会让"预算耗尽"这类用例永远进不去。
    """
    monkeypatch.setattr(BS, "now_ms", lambda: FIXED_NOW_MS)


@pytest.fixture(autouse=True)
def owner_release(monkeypatch: pytest.MonkeyPatch) -> None:
    """把 owner 弱依赖释放接到同一条磁带上 —— 它与 lifecycle 投递的**相对顺序**是安全边界。"""

    async def _rel(auth, players, self_pod, self_uid, budget) -> None:  # noqa: ANN001
        TAPE.append(("owner_release", list(players), self_pod, self_uid, budget))

    monkeypatch.setattr(BS, "owner_release_abandoned_players_weak", _rel)


# ── 值对象构造 ──────────────────────────────────────────────────────────────


def battle(**over: Any) -> Any:
    """一份**完整合法**的 canonical 投影;每个用例只打坏其中一格。

    默认 `last_heartbeat_ms` 落在 active 阈值**之前**(= 已失联),因为本模块的
    绝大多数路径都从"这一项该被判弃"开始。
    """
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
        "allocated_at_ms": FIXED_NOW_MS - 300_000,
        "last_heartbeat_ms": ACTIVE_CUTOFF_MS - 1,
    }
    base.update(over)
    return dspb.BattleStorageRecord(**base)


# ── 替身 ────────────────────────────────────────────────────────────────────


class FakeCfg:
    """`AllocatorConf` 里 sweep 链真正读到的五个 duration。

    ★ 刻意不构造真的 pydantic `AllocatorConf`:它有几十个字段与交叉校验,在这里搭一遍
      测的是配置解析(那边有自己的用例),却会让本文件因无关字段变动而假红。
    """

    def __init__(
        self,
        *,
        sweep_interval_sec: float = SWEEP_INTERVAL_SEC,
        heartbeat_timeout_sec: float = HEARTBEAT_TIMEOUT_SEC,
        ready_wait_sec: float = READY_WAIT_SEC,
        battle_ttl_sec: float = BATTLE_TTL_SEC,
    ) -> None:
        self._sweep = sweep_interval_sec
        self._hb = heartbeat_timeout_sec
        self._ready = ready_wait_sec
        self._battle_ttl = battle_ttl_sec

    def sweep_interval_td(self) -> _dt.timedelta:
        return _dt.timedelta(seconds=self._sweep)

    def heartbeat_timeout_td(self) -> _dt.timedelta:
        return _dt.timedelta(seconds=self._hb)

    def ready_wait_timeout_td(self) -> _dt.timedelta:
        return _dt.timedelta(seconds=self._ready)

    def battle_ttl_td(self) -> _dt.timedelta:
        return _dt.timedelta(seconds=self._battle_ttl)

    def resolve_orphan_gs_reclaim_after(self) -> _dt.timedelta:
        return _dt.timedelta(seconds=300)


class FakeLease:
    """`SweepWriterLease` 替身。`current()` 返回 `(token, held)`,只有第二个被消费。"""

    def __init__(self, script: list[tuple[int, bool]]) -> None:
        self.script = list(script)
        self.calls = 0

    def current(self) -> tuple[int, bool]:
        self.calls += 1
        return _pop(self.script, (0, False))


class FakeRepo:
    """canonical battle 仓 + 派生 active ZSET 替身。

    `zset` 是本文件最关键的一格:它记录**谁写过 score、写进去的是什么值**。
    INC-20260724-001 的形状正是"退避被写进了 score",而那在生产里不会报任何错。
    """

    def __init__(self) -> None:
        self.battles: dict[int, Any] = {}
        self.stale: list[int] = []
        self.stale_threshold_ms: int | None = None
        self.range_exc: BaseException | None = None
        self.get_exc: dict[int, BaseException] = {}
        self.update_exc: BaseException | None = None
        self.get_delay_sec: float = 0.0
        #: match_id → 最近一次 ZADD 进去的 score。预置值 = "上一轮留下的心跳时间"。
        self.zset: dict[int, int] = {}
        self.delete_if_matches: list = [True]

    async def range_stale_battles(self, threshold_ms: int) -> list[int]:
        TAPE.append(("range_stale", threshold_ms))
        self.stale_threshold_ms = threshold_ms
        if self.range_exc is not None:
            raise self.range_exc
        return list(self.stale)

    async def get_battle(self, match_id: int) -> Any:
        TAPE.append(("get_battle", match_id))
        if self.get_delay_sec > 0:
            await asyncio.sleep(self.get_delay_sec)
        exc = self.get_exc.get(match_id)
        if exc is not None:
            raise exc
        return self.battles.get(match_id)

    async def update_battle_keep_ttl(self, match_id: int, retries: int, fn: Any) -> None:
        TAPE.append(("update_keep_ttl", match_id, retries))
        if self.update_exc is not None:
            raise self.update_exc
        rec = self.battles.get(match_id)
        if rec is not None:
            fn(rec)

    async def delete_battle_if_allocation_matches(
        self, match_id: int, allocation_id: str, pod_name: str
    ) -> bool:
        TAPE.append(("delete_if_matches", match_id, allocation_id, pod_name))
        return _pop(self.delete_if_matches, True)

    async def touch_active(self, match_id: int, last_heartbeat_ms: int) -> None:
        TAPE.append(("touch_active", match_id, last_heartbeat_ms))
        self.zset[match_id] = last_heartbeat_ms

    async def remove_active(self, match_id: int) -> None:
        TAPE.append(("remove_active", match_id))
        self.zset.pop(match_id, None)

    async def expire_battle(self, match_id: int, ttl_sec: float) -> None:
        TAPE.append(("expire_battle", match_id, ttl_sec))


class FakeAuthRepo:
    def __init__(self) -> None:
        self.abandon_script: list = []
        self.terminate_script: list = [True]
        self.expire_script: list = [True]
        self.abandon_cutoffs: list = []

    async def abandon_if_stale(
        self, match_id: int, cutoffs: Any, cred_ttl: float, battle_ttl: float
    ) -> Any:
        TAPE.append(("abandon_if_stale", match_id, cutoffs.active_heartbeat_ms))
        self.abandon_cutoffs.append(cutoffs)
        return _pop(self.abandon_script, BattleAbandonResult())

    async def terminate_expected(
        self,
        match_id: int,
        expected: BattleExpectedInstance,
        state: str,
        cred_ttl: float,
        battle_ttl: float,
    ) -> bool:
        TAPE.append(("terminate_expected", match_id, expected.instance_uid, state))
        return _pop(self.terminate_script, True)

    async def expire_terminated_expected(
        self,
        match_id: int,
        expected: BattleExpectedInstance,
        cred_ttl: float,
        battle_ttl: float,
    ) -> bool:
        TAPE.append(("expire_terminated", match_id, expected.instance_uid))
        return _pop(self.expire_script, True)


class FakeAbortRepo:
    def __init__(self) -> None:
        self.read_script: list = [(None, False, True)]

    async def read_allocation_abort(self, match_id: int) -> tuple[Any, bool, bool]:
        TAPE.append(("read_abort_journal", match_id))
        return _pop(self.read_script, (None, False, True))


class FakeAlloc:
    """legacy / local 分配器:只有 `release`(按 pod 名,无 UID precondition)。"""

    def __init__(self) -> None:
        self.release_exc: BaseException | None = None

    async def release(self, pod_name: str) -> None:
        TAPE.append(("legacy_release", pod_name))
        if self.release_exc is not None:
            raise self.release_exc


class FakeAuthoritativeAlloc:
    """Model B 权威分配器。**不实现** `probe_expected_instance_gone`(warming probe 不可用)。"""


class FakeProbingAlloc(FakeAuthoritativeAlloc):
    """额外实现 `WarmingInstanceProber` —— warming 判死加速出口的能力探测通过。"""

    def __init__(self) -> None:
        self.probe_script: list = [False]

    async def probe_expected_instance_gone(
        self, pod_name: str, instance_uid: str, pod_uid: str
    ) -> bool:
        TAPE.append(("probe_gone", pod_name, instance_uid))
        return _pop(self.probe_script, False)


class FakeIndexReconciler:
    def __init__(self) -> None:
        self.exc: BaseException | None = None
        self.calls = 0

    async def reconcile_battle_active_index(self, count: int) -> None:
        TAPE.append(("reconcile_index", count))
        self.calls += 1
        if self.exc is not None:
            raise self.exc


class FakeLifecycle:
    def __init__(self) -> None:
        self.exc: BaseException | None = None
        self.events: list[Any] = []

    async def publish_lifecycle(self, evt: Any) -> None:
        TAPE.append(("publish_lifecycle", evt.match_id, evt.phase))
        if self.exc is not None:
            raise self.exc
        self.events.append(evt)


class FakeProofRepo:
    def __init__(self) -> None:
        self.exc: BaseException | None = None

    async def record_allocation_lifecycle_published(self, match_id: int, target: Any) -> None:
        TAPE.append(("lifecycle_marker", match_id, target.allocation_id))
        if self.exc is not None:
            raise self.exc


class Harness(BS.SweepMixin):
    """把 `SweepMixin` 装配成可调用对象。

    ★ 字段全部按 `AllocatorUsecaseBase` 的名字给,accessor 也照它的语义实现(从 cfg
      算,而不是各写各的常数)—— mixin 本身**不许**持有状态,这里也就不能替它发明
      字段名或让 accessor 与 cfg 脱钩。
    ★ 兄弟 mixin(Release / Allocate / Heartbeat)提供的方法在这里是**契约桩**:本文件
      测的是 sweep 什么时候调它们、调完怎么分支,不是它们自己的逻辑(那些在
      `tests/test_ds_allocator_biz_release.py`)。
    """

    def __init__(
        self,
        *,
        model_b: bool = False,
        cfg: FakeCfg | None = None,
        authoritative: Any = None,
        with_index_reconciler: bool = True,
        lifecycle: Any = None,
        lifecycle_required: bool = False,
    ) -> None:
        self.cfg = cfg if cfg is not None else FakeCfg()
        self.repo = FakeRepo()
        self.alloc = FakeAlloc()
        self.auth_repo = FakeAuthRepo()
        self.abort_repo = FakeAbortRepo()
        self.lifecycle_proof_repo = FakeProofRepo()
        self.authoritative_alloc = (
            authoritative if authoritative is not None else FakeAuthoritativeAlloc()
        )
        self.active_index_reconciler = FakeIndexReconciler() if with_index_reconciler else None
        self.lifecycle = lifecycle
        self.lifecycle_required = lifecycle_required
        self.model_b = model_b
        self.owner_auth = object()
        self.ds_credential_ttl_sec = 120.0
        # 孤儿 GS 清扫的两项能力都不给 → janitor 自动禁用(它有自己的测试文件)。
        self.orphan_gs_reconciler = None
        self.allocation_ledger = None
        # `AllocatorUsecaseBase.__init__` 建的进程内状态,逐字同名。
        self.owner_admitted: dict[str, float] = {}
        self.sweep_defer_until: dict[int, Any] = {}
        self.sweep_lease: Any = None
        self.sweep_lease_held = False
        self.last_active_index_reconcile: float = 0.0
        # 兄弟 mixin 的契约桩返回值脚本。
        self.preactive_script: list = [PreactiveReleaseOutcome.COMPLETED]
        self.resume_abandoned_script: list = [True]
        self.pod_uid_script: list = [POD_UID]
        self.release_gs_script: list = [None]
        self.kill_calls: list[tuple[int, str, str]] = []

    # ── 基座 accessor(照 AllocatorUsecaseBase 的实现口径)──────────────

    def battle_ttl_sec(self) -> float:
        return self.cfg.battle_ttl_td().total_seconds()

    def ready_wait_timeout_sec(self) -> float:
        return self.cfg.ready_wait_timeout_td().total_seconds()

    def heartbeat_timeout_ms(self) -> int:
        return int(self.cfg.heartbeat_timeout_td() / _dt.timedelta(milliseconds=1))

    def kill_stranded_ds(self, match_id: int, pod_name: str, reason: str) -> None:
        TAPE.append(("kill_stranded", match_id, pod_name, reason))
        self.kill_calls.append((match_id, pod_name, reason))

    # ── 兄弟 mixin 契约桩 ─────────────────────────────────────────────

    async def reconcile_allocation_uncertain(self, inflight: Any) -> None:
        TAPE.append(("reconcile_uncertain", inflight.match_id))

    async def resume_empty_allocation_tombstone(self, inflight: Any) -> None:
        TAPE.append(("resume_empty_fence", inflight.match_id))

    async def resume_reconciled_allocation_abandoned(self, inflight: Any) -> bool:
        TAPE.append(("resume_abandoned", inflight.match_id))
        return _pop(self.resume_abandoned_script, True)

    async def reconcile_preactive_release(self, rec: Any) -> PreactiveReleaseOutcome:
        TAPE.append(("reconcile_preactive", rec.match_id))
        return _pop(self.preactive_script, PreactiveReleaseOutcome.COMPLETED)

    async def abort_preactive_battle(self, request: Any) -> None:
        TAPE.append(("abort_preactive", request))

    async def ensure_durable_release_pod_uid(
        self, match_id: int, pod_name: str, expected: Any, release_track: str
    ) -> str:
        TAPE.append(("ensure_pod_uid", match_id, pod_name))
        return _pop(self.pod_uid_script, POD_UID)

    async def release_game_server(self, match_id: int, pod_name: str, alloc: Any) -> None:
        TAPE.append(("release_gs", match_id, pod_name, alloc.allocation_id))
        _pop(self.release_gs_script, None)


def abandon_out(**over: Any) -> BattleAbandonResult:
    """`abandon_if_stale` 的默认返回:**已判弃、auth/active 都在**(走完整回收链)。"""
    base: dict[str, Any] = {
        "abandoned": True,
        "already_terminal": False,
        "auth_found": True,
        "active_found": True,
        "battle": battle(state=STATE_ABANDONED),
    }
    base.update(over)
    return BattleAbandonResult(**base)


# ══ ① 单写者:非 leader 一条清扫写都不许发(§9.21)═══════════════════════════


def test_sweep_is_leader_defaults_to_true_without_lease_and_logs_nothing(
    log: LogRecorder,
) -> None:
    """未注入租约 = 单副本 Recreate 的历史形态,恒为 leader 且**不打跃迁日志**。

    守的是什么:这是 local / dev 单副本档的默认形态。若这里返回 False,整个 §9.4
    补偿链在开发与压测环境**静默停摆**(进程还在、health 还答 SERVING)。反过来,
    每 tick 打一条跃迁日志会把同文件里的 WARN 冲走(§11.3 R4)。

    ★ 变异:把 `sweep_is_leader` 的 `if self.sweep_lease is None: return True`
      改成 `return False` → 本条红。
    """
    h = Harness()
    assert h.sweep_is_leader() is True
    assert h.sweep_is_leader() is True
    assert log.events() == []


def test_sweep_leadership_logs_only_on_transition(log: LogRecorder) -> None:
    """领导权日志只在**跃迁**时打:持有→持有不重复,让位→让位也不重复。

    守的是什么:sweep 每 5s 一轮。逐轮打这条会在 Loki 里把真正需要看见的
    `heartbeat_sweep_failed` / `allocation_sweep_round_budget_exhausted` 冲走;
    而完全不打,则"这个副本是什么时候不再扫描的"根本无从回答。

    ★ 变异:把 `if held != self.sweep_lease_held:` 改成 `if True:` → 本条红。
    """
    h = Harness()
    h.set_sweep_writer_lease(FakeLease([(7, True), (7, True), (8, False), (8, False)]))
    assert h.sweep_is_leader() is True
    assert h.sweep_is_leader() is True
    assert h.sweep_is_leader() is False
    assert h.sweep_is_leader() is False
    assert log.events() == [
        "heartbeat_sweep_leadership_acquired",
        "heartbeat_sweep_leadership_released",
    ]
    assert log.find("heartbeat_sweep_leadership_acquired")["token"] == 7


async def test_non_leader_tick_performs_zero_sweep_reads_and_writes() -> None:
    """**非 leader 必须一条清扫读写都不发**,但本地内存卫生照做。

    守的是什么(§9.21 单写者 + §9.18):滚动升级的重叠窗口里 Stable 与 Canary 同时在线,
    若领导权门失效,两个副本会对同一批 abandoned 并发 terminate / release / 投递
    lifecycle —— 补偿变并发误删,且**没有任何报错**。反过来,把领导权门排到内存卫生
    **之前**,热备副本的 census 准入缓存就永不老化,热备期越长内存越涨。

    ★ 变异(已实跑):把 `heartbeat_sweep_tick` 里的
      `if not self.sweep_is_leader(): return` 整条删掉 → 本条红。
    """
    h = Harness()
    h.set_sweep_writer_lease(FakeLease([(1, False)]))
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle()
    h.owner_admitted["old-instance"] = -10_000.0  # 单调钟早于任何 cutoff → 该清
    h.owner_admitted["fresh-instance"] = 10.0**12  # 远未到期 → 该留

    await h.heartbeat_sweep_tick()

    assert names() == [], "非 leader 发生了清扫读写"
    assert h.owner_admitted == {"fresh-instance": 10.0**12}


async def test_initial_sweep_on_startup_is_gated_by_leadership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """启动首扫也必须过领导权门 —— 竞选是异步的,启动瞬间通常尚未当选。

    守的是什么:无条件首扫等于在滚动升级重叠窗口里凭空多一个并发扫描者。跳过无损:
    当选后的第一个 tick 就补上,间隔仅一个 SweepInterval。

    ★ 变异:把 `run_heartbeat_sweep` 里的 `if self.sweep_is_leader():` 改成 `if True:`
      → 本条红。
    """
    looped: list[tuple[str, float]] = []

    async def _fake_loop(name: str, interval: float, fn: Any) -> None:
        looped.append((name, interval))

    monkeypatch.setattr(BS.safego, "loop", _fake_loop)

    h = Harness()
    h.set_sweep_writer_lease(FakeLease([(1, False)]))
    h.repo.stale = [MATCH_ID]
    await h.run_heartbeat_sweep()

    assert names() == [], "尚未当选就跑了首扫"
    assert looped == [(BS.SWEEP_TASK_NAME, SWEEP_INTERVAL_SEC)]


async def test_leader_startup_runs_initial_sweep_and_survives_its_failure(
    monkeypatch: pytest.MonkeyPatch, log: LogRecorder
) -> None:
    """当选时首扫必跑;首扫失败只记 `heartbeat_initial_sweep_failed`,不阻断循环启动。

    守的是什么:重启后不立刻重建派生索引,会让"一条丢失的 ZADD 造成的永久墓碑"在
    **每次重启后**都有整整一个 sweep 间隔完全不可见。而首扫失败若向外抛,后台循环
    压根起不来 —— 补偿链从进程启动那一刻就是死的。

    ★ 变异:把首扫的 `except BaseException as exc:` 分支整段删掉(让异常向外抛)
      → 本条红。
    """
    async def _fake_loop(name: str, interval: float, fn: Any) -> None:
        TAPE.append(("loop_started", name))

    monkeypatch.setattr(BS.safego, "loop", _fake_loop)

    h = Harness()
    h.repo.range_exc = RuntimeError("redis down")
    await h.run_heartbeat_sweep()

    assert log.count("heartbeat_initial_sweep_failed") == 1
    assert log.find("heartbeat_initial_sweep_failed")["err"] == "redis down"
    assert ("loop_started", BS.SWEEP_TASK_NAME) in TAPE


async def test_tick_records_sweep_failure_without_killing_the_loop(log: LogRecorder) -> None:
    """单轮清扫抛异常只记 `heartbeat_sweep_failed`,不向外传播(下 tick 继续)。

    守的是什么:tick 向外抛 = 后台循环终止 = §9.4 补偿链静默停摆。进程还活着、端口
    还在听、health 还答 SERVING —— 没有任何人看得见。

    ★ 变异:把 `heartbeat_sweep_tick` 里 `except BaseException as exc:` 那段删掉 → 本条红。
    """
    h = Harness()
    h.repo.range_exc = RuntimeError("boom")
    await h.heartbeat_sweep_tick()  # 不抛
    assert log.find("heartbeat_sweep_failed")["err"] == "boom"


# ══ ② INC-20260724-001:退避只能记进程内,绝不写 ZSET score ══════════════════


async def test_head_of_line_deferral_never_writes_active_zset_score() -> None:
    """**本文件的核心用例**:队头退避只写进程内表,active ZSET 的 score 一个字节都不动。

    守的是什么(INC-20260724-001,三条硬约束,破坏后**均不报错**):
      ① score 的权威语义是 `last_heartbeat_ms`。写成退避时间戳后
         「score ≤ 阈值 ⇔ 心跳超时」这条判据整体失效 —— 失联的局扫不到,
         活着的局被误判。
      ② abort fence / auth quarantine 用 **score==0** 表示「下一轮必须立即对账」,
         索引重建专门用 `ZADD NX` 保护这个哨兵。写退避会把这个最高危状态的
         **立即**对账降级成**延迟**对账。
      ③ `touch_active` 是无条件 `ZADD`(无 NX/XX/GT)。迟到的退避写会把已被
         `remove_active` 的终态项**复活**回补偿 outbox,重复投递 lifecycle。

    造故障的方式是真的让被测分支进入退避态:`allocation_uncertain` 属
    `stuck_reconcile_state`,legacy writer 对它只读跳过 —— 于是这一轮里除了
    `range_stale` + `get_battle` 不该再有任何仓调用。

    ★ 变异(已实跑):把 `sweep_once` 里的
      `self.note_sweep_deferral(mid, defer_key, round_start)` 改成
      `await self.repo.touch_active(mid, now_ms() + 15_000)` → 本条红。
    """
    h = Harness(model_b=False, with_index_reconciler=False)
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle(state=STATE_ALLOCATION_UNCERTAIN)
    # 上一轮留下的真实心跳 score —— 退避绝不能覆盖它。
    h.repo.zset[MATCH_ID] = ACTIVE_CUTOFF_MS - 1
    # score==0 哨兵:abort fence / auth quarantine 要求"下一轮立即对账"。
    h.repo.zset[MATCH_ID_2] = 0

    await h.sweep_once()

    assert names() == ["range_stale", "get_battle"], "退避路径发生了额外的仓调用"
    assert "touch_active" not in names()
    assert h.repo.zset == {MATCH_ID: ACTIVE_CUTOFF_MS - 1, MATCH_ID_2: 0}, "ZSET score 被改写"
    # 退避确实生效了(否则上面的"没写 ZSET"是空断言)。
    entry = h.sweep_defer_until[MATCH_ID]
    assert entry.state == STATE_ALLOCATION_UNCERTAIN
    assert entry.until > 0.0


async def test_deferral_table_is_process_local_so_a_restart_re_scans_immediately() -> None:
    """退避表是**进程内**状态:新实例(= 重启)的表是空的,同一项立刻重新处理。

    守的是什么:「重启即扫」是 `run_heartbeat_sweep` 开头的既定意图。若退避被持久化
    (写进 ZSET score 或任何 Redis 键),重启后那一项仍在退避窗内 —— 运维重启服务来
    "推一把卡住的补偿"这个最常用的手段直接失效。

    ★ 变异:把 `sweep_defer_until` 改成模块级全局字典(跨实例共享)→ 本条红。
    """
    h1 = Harness(model_b=False, with_index_reconciler=False)
    h1.repo.stale = [MATCH_ID]
    h1.repo.battles[MATCH_ID] = battle(state=STATE_ALLOCATION_EMPTY_FENCE)
    await h1.sweep_once()
    assert MATCH_ID in h1.sweep_defer_until

    h2 = Harness(model_b=False, with_index_reconciler=False)
    assert h2.sweep_defer_until == {}, "退避表跨实例存活 = 重启不再立即重扫"


async def test_index_repair_writes_authoritative_heartbeat_as_score_not_a_schedule_stamp() -> None:
    """修派生索引时写进 score 的必须是**事务快照里的权威心跳时间**,不是任何调度时间戳。

    守的是什么:这是"score 语义 = `last_heartbeat_ms`"的正面锁定。写别的值(轮次开始
    时刻、退避到期时刻、`now`)都会让这一项在下一轮被错误地判成"心跳新鲜",于是一个
    真正失联的 DS 永远不被判弃 —— §9 不变量 4 的补偿链对它永久断链,且零报错。

    ★ 变异(已实跑):把 `_sweep_model_b_item` 里的
      `await _discard_error(self.repo.touch_active(mid, b.last_heartbeat_ms))`
      改成 `... touch_active(mid, int(round_start * 1000))` → 本条红。
    """
    authoritative_hb = FIXED_NOW_MS - 3_000
    h = Harness(model_b=True)
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle()
    h.auth_repo.abandon_script = [
        abandon_out(
            abandoned=False,
            already_terminal=False,
            battle=battle(last_heartbeat_ms=authoritative_hb),
        )
    ]

    await h.sweep_once()

    assert h.repo.zset[MATCH_ID] == authoritative_hb
    assert ("touch_active", MATCH_ID, authoritative_hb) in TAPE


# ══ ③ 退避助手的判据本身 ═════════════════════════════════════════════════════


def test_sweep_deferral_active_expires_exactly_at_until_not_after() -> None:
    """到期判据是 `now >= until`(Go 的 `!now.Before(d.until)`):**恰好等于即到期**。

    守的是什么:写成 `now > until` 会让退避多活一个浮点 tick —— 单看无害,但它与
    `prune_sweep_deferrals` 的判据必须逐字一致,否则 prune 会删掉 `sweep_deferral_active`
    仍认为有效的项(或反过来),同一项在一轮里既"在退避中"又"已清掉",行为随调用顺序
    抖动。到期后必须**就地删除**:留着一条已失效的记录,会在同一 match 后续状态回到
    旧值时被误认为"还在退避窗内"。

    ★ 变异(已实跑):把 `sweep_deferral_active` 的 `now >= d.until` 改成 `now > d.until`
      → 本条红。
    """
    h = Harness()
    h.note_sweep_deferral(MATCH_ID, STATE_ALLOCATION_ABORT, 100.0)
    until = h.sweep_defer_until[MATCH_ID].until
    assert until == 100.0 + HEARTBEAT_TIMEOUT_SEC

    assert h.sweep_deferral_active(MATCH_ID, STATE_ALLOCATION_ABORT, until - 0.001) is True
    assert MATCH_ID in h.sweep_defer_until, "未到期不该删"
    assert h.sweep_deferral_active(MATCH_ID, STATE_ALLOCATION_ABORT, until) is False
    assert MATCH_ID not in h.sweep_defer_until, "到期必须就地删除"


def test_sweep_deferral_invalidated_immediately_when_state_changed() -> None:
    """记录状态一变即作废退避 —— 调度优化不得拖慢真正的终态收敛。

    守的是什么:退避的前提是"这一项重试也收敛不了"。状态变了(被并发 RPC 推进、
    已进入 §9.4 补偿链、或 abort fence 要求立即对账)说明前提已不成立,还压着它
    15s 就是让最该推进的那一项白等一个窗口。

    ★ 变异:把 `sweep_deferral_active` 的 `if d.state != state or now >= d.until:`
      改成 `if now >= d.until:` → 本条红。
    """
    h = Harness()
    h.note_sweep_deferral(MATCH_ID, STATE_ALLOCATION_UNCERTAIN, 100.0)
    # 同一 match、退避窗内,但状态已推进到 abandoned 的补偿链
    assert h.sweep_deferral_active(MATCH_ID, STATE_ABANDONED, 101.0) is False
    assert MATCH_ID not in h.sweep_defer_until

    # 未登记过的 match 恒不退避
    assert h.sweep_deferral_active(MATCH_ID_2, STATE_ABANDONED, 101.0) is False


def test_prune_sweep_deferrals_bounds_the_table_by_one_backoff_window() -> None:
    """`prune_sweep_deferrals` 只清已到期项,保留窗内项(§9.18 进程内容器有界)。

    守的是什么:不清 = 表随历史 match_id 无界增长,长压测 OOM;清过头 = 退避形同虚设,
    队头饥饿(INC-20260724-001)复发。到期判据必须与 `sweep_deferral_active` 同为
    `now >= until`。

    ★ 变异:把 `prune_sweep_deferrals` 的 `if now >= d.until` 改成 `if False`
      → 本条红(过期项不再被清)。
    """
    h = Harness()
    h.note_sweep_deferral(MATCH_ID, STATE_ALLOCATION_ABORT, 0.0)  # until = 15.0
    h.note_sweep_deferral(MATCH_ID_2, STATE_ALLOCATION_ABORT, 100.0)  # until = 115.0
    h.prune_sweep_deferrals(15.0)
    assert list(h.sweep_defer_until) == [MATCH_ID_2]
    h.prune_sweep_deferrals(115.0)
    assert h.sweep_defer_until == {}


def test_note_sweep_deferral_falls_back_to_15s_when_timeout_nonpositive() -> None:
    """`heartbeat_timeout <= 0`(误配 / 未配)时退避窗兜底 15s,而不是 0(= 退避失效)。

    守的是什么:0 秒退避 = 每轮都重试同一项 = 队头饥饿原样复发,而配置里看起来
    一切正常。Go 用的是同一个字面量 15s。

    ★ 变异:把 `note_sweep_deferral` 里 `if backoff <= 0:` 改成 `if backoff < 0:` → 本条红。
    """
    h = Harness(cfg=FakeCfg(heartbeat_timeout_sec=0.0))
    h.note_sweep_deferral(MATCH_ID, STATE_ALLOCATION_ABORT, 50.0)
    assert h.sweep_defer_until[MATCH_ID].until == 50.0 + BS.DEFAULT_SWEEP_DEFER_BACKOFF_SEC
    assert BS.DEFAULT_SWEEP_DEFER_BACKOFF_SEC == 15.0


def test_stuck_reconcile_state_is_an_exact_five_state_whitelist() -> None:
    """`stuck_reconcile_state` 是精确五态白名单,**不含** abandoned。

    守的是什么:`abandoned + instance_epoch==0` 的 resume 分支是 §9.4 补偿的**最后一棒**,
    必须保持最高优先级重试(它不产生外部 GSA POST,单次耗时也远小于其余几种)。
    把它加进来 = 把补偿链最后一步也无条件推迟 15s。反过来漏掉任何一个"靠重试收敛"
    的状态,该状态在控制面故障期会恒排队头、串行吃掉整轮预算(事故当天的形状)。

    ★ 变异(已实跑):在 `stuck_reconcile_state` 的元组里加上 `STATE_ABANDONED` → 本条红。
    """
    for ok in (
        STATE_ALLOCATION_UNCERTAIN,
        STATE_ALLOCATION_RECONCILING,
        STATE_ALLOCATION_EMPTY_FENCE,
        STATE_PREACTIVE_RELEASING,
        STATE_ALLOCATION_ABORT,
    ):
        assert BS.stuck_reconcile_state(ok) is True, ok
    for bad in (
        STATE_ABANDONED,
        STATE_ENDED,
        STATE_ALLOCATING,
        STATE_WARMING,
        STATE_READY,
        STATE_RUNNING,
        "",
    ):
        assert BS.stuck_reconcile_state(bad) is False, bad


async def test_preactive_tombstone_defers_by_allocation_id_not_by_state() -> None:
    """`preactive_releasing` 的退避键用 `allocation_id`,不是状态串。

    守的是什么(复审 P1-2):abandoned → fence 成功 → release 未确认 会发生
    `abandoned → preactive_releasing` 的**跨状态迁移**。若仍按状态键退避,上一轮登记的
    退避会被状态变化误判失效,于是每轮又重复一次外部调用 —— 队头饥饿原样复发。
    `allocation_id` 不随该迁移变化,正是这条链上唯一稳定的身份。

    ★ 变异:把 `sweep_once` 里
      `defer_key = RELEASE_PENDING_DEFER_PREFIX + inflight.allocation_id`
      改成 `defer_key = inflight.state` → 本条红。
    """
    h = Harness(model_b=False, with_index_reconciler=False)
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle(state=STATE_PREACTIVE_RELEASING)

    await h.sweep_once()

    assert (
        h.sweep_defer_until[MATCH_ID].state == BS.RELEASE_PENDING_DEFER_PREFIX + ALLOC
    )
    assert BS.RELEASE_PENDING_DEFER_PREFIX == "release-pending:"


async def test_second_round_skips_the_deferred_item_entirely() -> None:
    """退避生效的可观察后果:下一轮**连 `get_battle` 之后的处理都不做**,直接让出队头。

    守的是什么:退避若不真的跳过,只是记了一张表,那就等于没退避 —— 控制面持续故障
    时最老那一项仍每轮吃满预算,队尾的 §9.4 abandoned 补偿被无限推后。

    ★ 变异:把 `sweep_once` 里 `if self.sweep_deferral_active(mid, defer_key, round_start):
      continue` 的 `continue` 删掉 → 本条红。
    """
    h = Harness(model_b=True)
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle(state=STATE_ALLOCATION_UNCERTAIN)

    await h.sweep_once()
    assert ("reconcile_uncertain", MATCH_ID) in TAPE

    TAPE.clear()
    await h.sweep_once()
    # 第二轮的索引重建被 30s 节流掉,故磁带上只剩粗筛 + 单项读。
    assert names() == ["range_stale", "get_battle"]
    assert ("reconcile_uncertain", MATCH_ID) not in TAPE

# ══ ④ 单轮预算:有上界,且至少推进一项 ══════════════════════════════════════


def test_sweep_round_budget_uses_interval_and_falls_back_to_5s() -> None:
    """预算 = `sweep_interval`;非正配置兜底 5s(不是 0 = 每轮只做一项)。

    守的是什么:预算 0 会让每轮固定只处理一项,长队列永远追不上;预算无穷则下一 tick
    直接叠上来。这两种都不会报错。

    ★ 变异:把 `sweep_round_budget` 的 `if d > 0:` 改成 `if d >= 0:` → 本条红。
    """
    assert Harness(cfg=FakeCfg(sweep_interval_sec=5.0)).sweep_round_budget() == 5.0
    assert Harness(cfg=FakeCfg(sweep_interval_sec=0.0)).sweep_round_budget() == 5.0
    assert Harness(cfg=FakeCfg(sweep_interval_sec=-1.0)).sweep_round_budget() == 5.0
    assert BS.DEFAULT_SWEEP_ROUND_BUDGET_SEC == 5.0


async def test_round_budget_stops_the_round_and_leaves_the_rest_in_the_outbox(
    log: LogRecorder,
) -> None:
    """单项耗时超预算时**本轮就此打住**,剩下的留在 active ZSET 等下一 tick。

    守的是什么(INC-20260724-001):控制面持续超时时单项可耗时数十秒。没有预算的一轮
    会把下一 tick 直接叠上来(同一批项被两个协程同时处理),而且队尾那些**能**收敛的
    对局被无限推后。未处理项必须留在 active(outbox 语义),不能当成"已处理"。

    这里把单项耗时(20ms)造得明显超过预算(10ms):第一项无条件推进,第二项开始前
    预算检查即中断。

    ★ 变异(已实跑):把 `sweep_once` 里预算检查的 `break` 改成 `pass` → 本条红。
    """
    h = Harness(model_b=False, with_index_reconciler=False, cfg=FakeCfg(sweep_interval_sec=0.01))
    ids = [MATCH_ID, MATCH_ID_2, MATCH_ID_3]
    h.repo.stale = list(ids)
    for mid in ids:
        h.repo.battles[mid] = battle(match_id=mid, state=STATE_ALLOCATION_UNCERTAIN)
        h.repo.zset[mid] = ACTIVE_CUTOFF_MS - 1
    h.repo.get_delay_sec = 0.02

    await h.sweep_once()

    assert names() == ["range_stale", "get_battle"], "预算耗尽后仍继续处理了后续项"
    exhausted = log.find("allocation_sweep_round_budget_exhausted")
    assert exhausted["processed"] == 1
    assert exhausted["deferred_to_next_tick"] == 2
    # 未处理项必须仍在 outbox 里(score 未被动过)
    assert h.repo.zset == {mid: ACTIVE_CUTOFF_MS - 1 for mid in ids}


async def test_round_budget_always_advances_at_least_one_item(log: LogRecorder) -> None:
    """预算再小也必须推进**至少一项** —— `processed > 0` 是防活锁的前置条件。

    守的是什么:去掉 `processed > 0` 后,只要上一步(索引重建)耗时已超过一个极小的
    预算,本轮就一项都不处理;每一轮都如此 ⇒ §9.4 补偿链**永久停摆**,而日志里只会
    看到一条看似正常的"预算耗尽"。这正是"没有报错的死锁"。

    ★ 变异(已实跑):把 `if processed > 0 and time.monotonic() - round_start >= budget:`
      改成 `if time.monotonic() - round_start >= budget:` → 本条红。
    """
    h = Harness(model_b=False, with_index_reconciler=False, cfg=FakeCfg(sweep_interval_sec=1e-6))
    h.repo.stale = [MATCH_ID, MATCH_ID_2]
    for mid in (MATCH_ID, MATCH_ID_2):
        h.repo.battles[mid] = battle(match_id=mid, state=STATE_ALLOCATION_UNCERTAIN)
    h.repo.get_delay_sec = 0.005

    await h.sweep_once()

    assert names().count("get_battle") == 1, "一项都没推进 = 活锁"
    assert log.find("allocation_sweep_round_budget_exhausted")["processed"] == 1


async def test_round_summary_reports_stale_processed_and_deferred() -> None:
    """整轮只出一条聚合 debug,能回答"扫了几个 / 处理了几个 / 顺延几个"。

    守的是什么(§11.3 判据 5 / R4):逐项打 INFO 会把同文件的 WARN 拒绝冲走;完全不打
    则"这一轮到底扫没扫到东西"无法从日志回答 —— 而 sweep 停摆的表现就是"什么都没发生"。

    ★ 变异:把 `if stale:` 改成 `if False:` → 本条红。
    """
    h = Harness(model_b=False, with_index_reconciler=False)
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle(state=STATE_ALLOCATION_UNCERTAIN)
    log_rec = BS.plog.get()

    await h.sweep_once()

    done = log_rec.find("allocation_sweep_round_done")
    assert done["stale"] == 1
    assert done["processed"] == 1
    assert done["deferred"] == 0


# ══ ⑤ 心跳超时判定的时间边界 ═══════════════════════════════════════════════


async def test_stale_cutoff_is_now_minus_heartbeat_timeout_exactly() -> None:
    """`range_stale_battles` 的粗筛阈值 = `now_ms - heartbeat_timeout_ms`,逐毫秒相等。

    守的是什么(§9 不变量 4 的入口):阈值算错一个数量级,要么扫不到任何失联对局
    (补偿永不发生),要么把在打的对局全捞进来交给事务复核(白烧 Redis)。这条把
    公式钉死到毫秒。

    ★ 变异:把 `active_cutoff = now_wall_ms - self.heartbeat_timeout_ms()` 改成
      `= now_wall_ms` → 本条红。
    """
    h = Harness(model_b=False, with_index_reconciler=False)
    await h.sweep_once()
    assert h.repo.stale_threshold_ms == ACTIVE_CUTOFF_MS
    assert h.heartbeat_timeout_ms() == 15_000


async def test_heartbeat_exactly_at_cutoff_is_stale_one_ms_later_is_not() -> None:
    """边界:`last_heartbeat_ms == cutoff` 判失联;`cutoff + 1` 只修索引。

    守的是什么:Go 的判据是 `if b.LastHeartbeatMs > cutoff { staleIndexOnly }`。改成
    `>=` 后,恰好卡在边界的失联局每轮都被当成"只是索引陈旧"修一下,**永远不判弃** ——
    玩家卡在一局已经不存在的战斗里,段位回滚永不发生,而日志里只有一条看似健康的
    `sweep_repaired_stale_index`。

    ★ 变异(已实跑):把 `_sweep_legacy_item` 的 `if b.last_heartbeat_ms > cutoff:`
      改成 `>=` → 本条红。
    """
    h = Harness(model_b=False, with_index_reconciler=False, lifecycle=FakeLifecycle())
    h.repo.stale = [MATCH_ID, MATCH_ID_2]
    h.repo.battles[MATCH_ID] = battle(last_heartbeat_ms=ACTIVE_CUTOFF_MS)
    h.repo.battles[MATCH_ID_2] = battle(match_id=MATCH_ID_2, last_heartbeat_ms=ACTIVE_CUTOFF_MS + 1)

    await h.sweep_once()

    assert h.repo.battles[MATCH_ID].state == STATE_ABANDONED, "恰好等于阈值必须判弃"
    assert h.repo.battles[MATCH_ID_2].state == STATE_RUNNING, "晚 1ms 不得判弃"
    assert ("legacy_release", POD) in TAPE
    assert ("expire_battle", MATCH_ID, BATTLE_TTL_SEC) in TAPE
    assert ("expire_battle", MATCH_ID_2, BATTLE_TTL_SEC) not in TAPE
    assert BS.plog.get().count("sweep_repaired_stale_index") == 1


async def test_warming_uses_ready_wait_cutoff_not_the_heartbeat_cutoff() -> None:
    """warming 冷加载走 `ready_wait_timeout` 阈值,不是业务心跳阈值。

    守的是什么:大图 `ServerTravel` → `GameMode BeginPlay` 之前没有业务心跳是**正常
    行为**,与 AllocateBattle 在途的 ready 等待同一口径。两个阈值合并成一个的后果是
    双向的:统一用 15s 会放跑一批实际早已死掉的 warming 实例(GameServer 永久泄漏);
    统一用 10s 则会把正在加载大图的正常局判弃(玩家进不去场景,§9 不变量 20)。

    这里的 `last_heartbeat_ms` 刻意落在两阈值之间(晚于 active cutoff、早于 warming
    cutoff):只有正确取用 warming 阈值才会判弃。

    ★ 变异(已实跑):把 `_sweep_legacy_item` 里
      `if b.state == STATE_WARMING: cutoff = warming_cutoff` 整段删掉 → 本条红。
    """
    between = FIXED_NOW_MS - 12_000
    assert ACTIVE_CUTOFF_MS < between < WARMING_CUTOFF_MS
    h = Harness(model_b=False, with_index_reconciler=False, lifecycle=FakeLifecycle())
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle(state=STATE_WARMING, last_heartbeat_ms=between)

    await h.sweep_once()

    assert h.repo.battles[MATCH_ID].state == STATE_ABANDONED
    assert ("legacy_release", POD) in TAPE


async def test_model_b_passes_both_cutoffs_into_the_authoritative_transaction() -> None:
    """Model B 把**两个阈值一起**交给 `abandon_if_stale`,由事务按权威快照选一个。

    守的是什么(TOCTOU):外层读到的 `state` 会与首次 `activate_heartbeat` 并发。外层若
    自己按快照挑一个阈值传下去,就可能拿 warming 阈值去判一个刚刚激活的 active 局
    (误判弃一场正在打的对局)。选择必须在 WATCH 事务内、按同一份权威快照做。

    ★ 变异:把 `BattleStaleCutoffs(...)` 里的 `warming_heartbeat_ms=warming_cutoff`
      改成 `=active_cutoff` → 本条红。
    """
    h = Harness(model_b=True)
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle()
    h.auth_repo.abandon_script = [abandon_out(abandoned=False, battle=None)]

    await h.sweep_once()

    cutoffs = h.auth_repo.abandon_cutoffs[0]
    assert cutoffs.active_heartbeat_ms == ACTIVE_CUTOFF_MS
    assert cutoffs.warming_heartbeat_ms == WARMING_CUTOFF_MS
    assert cutoffs.warming_forfeit is None


async def test_warming_probe_failure_defers_and_falls_back_to_the_time_bound() -> None:
    """warming 判死 probe 失败只**退避 + 回退时间界**,绝不据此直接 Release。

    守的是什么:probe 是 advisory 只读加速出口。把读失败当成"实例已死"就是拿一次
    控制面抖动去杀一台可能正在加载大图的 DS。同时,控制面持续超时的 probe 不得跨轮
    占住队头(INC-20260724-001),所以失败必须按**分配身份**记一次退避。

    ★ 变异:把 probe 的 `except` 分支里 `self.note_sweep_deferral(mid, probe_defer_state,
      round_start)` 删掉 → 本条红。
    """
    prober = FakeProbingAlloc()
    prober.probe_script = [RuntimeError("apiserver 5s timeout")]
    h = Harness(model_b=True, authoritative=prober)
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle(state=STATE_WARMING)
    h.auth_repo.abandon_script = [abandon_out(abandoned=False, battle=None)]

    await h.sweep_once()

    assert h.sweep_defer_until[MATCH_ID].state == BS.WARMING_PROBE_DEFER_PREFIX + ALLOC
    assert BS.WARMING_PROBE_DEFER_PREFIX == "warming-probe:"
    # 仍然走完了事务(回退时间界),而不是提前 forfeit
    assert h.auth_repo.abandon_cutoffs[0].warming_forfeit is None


async def test_warming_probe_confirmed_dead_forfeits_grace_bound_to_exact_identity() -> None:
    """probe 确认实例已死时才 forfeit 宽限,且 forfeit **绑定 exact 分配身份**(防 ABA)。

    守的是什么:probe 挂起期间 A 可能已被清理、同 match 的新分配 B 就位。A 的判死
    绝不能杀 B —— 所以 forfeit 里必须带满 `allocation_id + instance_uid + instance_epoch`
    三格,由事务内精确核验。少一格就等于允许"同 match 的下一次分配"被上一次的判死误伤。

    ★ 变异:把 `BattleWarmingForfeit(instance=BattleExpectedInstance(...))` 里的
      `instance_epoch=inflight.instance_epoch` 改成 `instance_epoch=0` → 本条红。
    """
    prober = FakeProbingAlloc()
    prober.probe_script = [True]
    h = Harness(model_b=True, authoritative=prober)
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle(state=STATE_WARMING)
    h.auth_repo.abandon_script = [abandon_out(abandoned=False, battle=None)]

    await h.sweep_once()

    forfeit = h.auth_repo.abandon_cutoffs[0].warming_forfeit
    assert forfeit is not None
    assert forfeit.instance == EXPECTED
    assert forfeit.heartbeat_ms == FIXED_NOW_MS
    assert MATCH_ID not in h.sweep_defer_until, "probe 成功不该记退避"


# ══ ⑥ deliver_abandoned:补偿 outbox 的闸门 ═════════════════════════════════


async def test_deliver_abandoned_releases_owner_before_publishing() -> None:
    """owner 权威释放排在 lifecycle 投递**之前**,且带 2s 预算。

    守的是什么(INC-20260729-002 P0-B1):lifecycle 失败会 return False 让 sweep 整轮重试,
    而 owner 释放本身幂等(compare-delete),重跑无副作用;反过来若放在 return True 之后,
    Kafka 长时间不可用时玩家的 owner 记录会一直指向已删除的 Pod —— 玩家被永久钉在一台
    不存在的 DS 上(§9.22)。2s 预算是因为整轮墙钟预算才 5s,owner 抖动不得吃满它。

    ★ 变异:把 `deliver_abandoned` 里 `await owner_release_abandoned_players_weak(...)`
      挪到 `return True` 之前(publish 之后)→ 本条红。
    """
    lc = FakeLifecycle()
    h = Harness(model_b=False, lifecycle=lc)
    ok = await h.deliver_abandoned(MATCH_ID, POD, UID, list(PLAYERS), MAP_ID, GAME_MODE)

    assert ok is True
    assert names() == ["owner_release", "publish_lifecycle"]
    assert TAPE[0] == ("owner_release", list(PLAYERS), POD, UID, 2.0)
    evt = lc.events[0]
    assert evt.match_id == MATCH_ID
    assert evt.phase == dspb.DS_LIFECYCLE_PHASE_ABANDONED
    assert list(evt.player_ids) == PLAYERS
    assert evt.map_id == MAP_ID
    assert evt.game_mode == GAME_MODE
    assert evt.ts_ms == FIXED_NOW_MS


async def test_deliver_abandoned_returns_false_on_publish_failure(log: LogRecorder) -> None:
    """投递失败返回 False —— 这一项**留在 active outbox**,下一轮重试。

    守的是什么(§9 不变量 4 的 at-least-once 闭环):返回 True 会让 sweep 把它移出 active,
    于是这局的 abandoned 补偿事件永远不会再被投递 —— battle_result 侧的段位回滚 /
    match 释放**永久丢失**,而且没有任何报错(Kafka 恢复后也不会自愈)。

    ★ 变异(已实跑):把 publish 失败分支的 `return False` 改成 `return True` → 本条红。
    """
    lc = FakeLifecycle()
    lc.exc = RuntimeError("kafka unavailable")
    h = Harness(model_b=False, lifecycle=lc)
    ok = await h.deliver_abandoned(MATCH_ID, POD, UID, list(PLAYERS), MAP_ID, GAME_MODE)
    assert ok is False
    assert log.find("ds_lifecycle_publish_failed_will_retry")["err"] == "kafka unavailable"


async def test_deliver_abandoned_fail_closed_when_required_publisher_is_missing(
    log: LogRecorder,
) -> None:
    """生产 required 但 publisher 为 None 时 fail-closed 返回 False。

    守的是什么:这是一道**独立于 main 启动校验**的保险。返回 True 会让 abandoned 在
    完全没有 match release / exit proof 的情况下被 expire 掉 —— 补偿链断在最后一米,
    且日志级别只有 warn。

    ★ 变异:把 `if self.lifecycle_required:` 分支里的 `return False` 改成 `return True`
      → 本条红。
    """
    h = Harness(model_b=False, lifecycle=None, lifecycle_required=True)
    assert await h.deliver_abandoned(MATCH_ID, POD, UID, [], MAP_ID, GAME_MODE) is False
    assert log.records[-1][0] == "error"
    assert log.records[-1][1] == "ds_lifecycle_publisher_missing_fail_closed"


async def test_deliver_abandoned_dev_best_effort_when_publisher_not_required(
    log: LogRecorder,
) -> None:
    """显式 local/off 开发配置(未接 Kafka)返回 True,并留一条 warn 说明后果。

    守的是什么:开发档若也 fail-closed,本地跑起来的每一局失联战斗都会永远留在 active,
    下一轮又扫到 —— 本地环境看起来"sweep 一直在忙"却什么都没推进。

    ★ 变异:把该分支的 `return True` 改成 `return False` → 本条红。
    """
    h = Harness(model_b=False, lifecycle=None, lifecycle_required=False)
    assert await h.deliver_abandoned(MATCH_ID, POD, UID, [], MAP_ID, GAME_MODE) is True
    assert "ds_lifecycle_disabled_dev_best_effort" in log.events()


async def test_legacy_keeps_item_in_active_until_delivery_succeeds() -> None:
    """投递失败 ⇒ **不** `expire_battle`(留在 outbox);下一轮重试成功才 expire。

    守的是什么:这是 outbox 语义的可观察形式。先 expire 再投递(或不管投递结果都
    expire)= 补偿事件永久丢失。同时这条也压住"重试轮次不得重复 Release":第二轮读到
    的记录已是 abandoned,`first_abandon` 必须为 False。

    ★ 变异(已实跑):把 `deliver_abandoned` 里 publish 失败分支的 `return False` 改成
      `return True`(等价于「不管投递结果都 expire」)→ 本条红。
    """
    lc = FakeLifecycle()
    lc.exc = RuntimeError("kafka down")
    h = Harness(model_b=False, with_index_reconciler=False, lifecycle=lc)
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle()

    await h.sweep_once()
    assert ("legacy_release", POD) in TAPE, "首次迁移必须回收一次 pod"
    assert "expire_battle" not in names(), "投递失败却移出了 outbox"

    # 第二轮:Kafka 恢复
    TAPE.clear()
    lc.exc = None
    await h.sweep_once()
    assert ("legacy_release", POD) not in TAPE, "重试轮次重复 Release"
    assert ("expire_battle", MATCH_ID, BATTLE_TTL_SEC) in TAPE


async def test_legacy_release_happens_exactly_once_across_retry_rounds() -> None:
    """`first_abandon` 只在"非 abandoned → abandoned"的首次迁移为真,Release 全局一次。

    守的是什么:WATCH CAS 保证该迁移跨副本 / 跨轮次只成功一次,因此 Release 恰好一次。
    写成恒真后,每一轮重试都会再发一次 Release —— 对已消失的 GameServer 是幂等的,但
    对**同名重建**的新实例就是误删一台载人 DS(红线
    never-delete-allocated-gameserver-20260803)。

    ★ 变异(已实跑):把 `first_abandon = b.state != STATE_ABANDONED` 改成
      `first_abandon = True` → 本条红。
    """
    lc = FakeLifecycle()
    lc.exc = RuntimeError("kafka down")
    h = Harness(model_b=False, with_index_reconciler=False, lifecycle=lc)
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle()

    for _ in range(3):
        await h.sweep_once()

    assert names().count("legacy_release") == 1
    assert names().count("publish_lifecycle") == 3, "补偿投递必须每轮幂等重试"


async def test_ended_battle_is_reclaimed_without_compensation() -> None:
    """正常结算(`ended`)只做幽灵 DS 收尾 + 移出 active,**不发补偿事件**。

    守的是什么:给一局正常结束的对局补发 ABANDONED,会让 battle_result 侧把已结算的
    段位再回滚一次(§9 不变量 2 的幂等只保证"同一 match_id 只落库一次",挡不住语义
    相反的第二种事件)。反过来不移出 active,这局每轮都被重扫。

    ★ 变异:把 `if b.state == STATE_ENDED: ended_skip = True` 那两行删掉 → 本条红。
    """
    lc = FakeLifecycle()
    h = Harness(model_b=False, with_index_reconciler=False, lifecycle=lc)
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle(state=STATE_ENDED)
    h.repo.zset[MATCH_ID] = ACTIVE_CUTOFF_MS - 1

    await h.sweep_once()

    assert h.kill_calls == [(MATCH_ID, POD, "ended")]
    assert ("remove_active", MATCH_ID) in TAPE
    assert "publish_lifecycle" not in names()
    assert h.repo.zset == {}


async def test_expired_mirror_only_clears_the_residual_index() -> None:
    """镜像 TTL 已过期(`ErrDSPodNotFound`)时只清残留 active,不报 `sweep_lock_failed`。

    守的是什么:BattleTTL 是补偿重试的**天然上界**。把这种"记录已随 TTL 消失"当成锁失败
    会让残留索引项永远留在 ZSET,每轮扫一次、每轮失败一次 —— 一条永不收敛的告警噪音,
    还把真正的锁故障淹掉。

    ★ 变异:把 `if errcode.as_code(lerr) == errcode.ErrDSPodNotFound:` 改成
      `if False:` → 本条红。
    """
    h = Harness(model_b=False, with_index_reconciler=False)
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle()
    h.repo.update_exc = errcode.PandoraError(errcode.ErrDSPodNotFound, "battle mirror gone")
    h.repo.zset[MATCH_ID] = ACTIVE_CUTOFF_MS - 1

    await h.sweep_once()

    assert ("remove_active", MATCH_ID) in TAPE
    assert "sweep_lock_failed" not in BS.plog.get().events()


# ══ ⑦ Model B 回收链的顺序与失败保留 ════════════════════════════════════════


async def test_model_b_full_reclaim_order_terminate_release_deliver_marker_expire() -> None:
    """Model B 完整回收链的顺序:UID preflight → terminate fence → release → 投递 → 见证 → expire。

    守的是什么:顺序反了就是安全洞。先投递后回收 ⇒ 补偿已发但 GameServer 永久泄漏;
    先 expire 后见证 ⇒ 无 ACK 见证却放开了 fence,Release 响应未知时会开出第二次 POST。
    `expire_terminated_expected` 必须排在 lifecycle 见证之后(未知结果宁可不可用也不丢 fence)。

    ★ 变异:把 `record_allocation_lifecycle_published` 失败分支的 `return` 删掉
      (改成继续 expire)→ 本条红(顺序序列变了)。
    """
    lc = FakeLifecycle()
    h = Harness(model_b=True, lifecycle=lc)
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle()
    h.auth_repo.abandon_script = [abandon_out()]

    await h.sweep_once()

    assert names() == [
        "reconcile_index",
        "range_stale",
        "get_battle",
        "abandon_if_stale",
        "ensure_pod_uid",
        "terminate_expected",
        "release_gs",
        "owner_release",
        "publish_lifecycle",
        "lifecycle_marker",
        "expire_terminated",
    ]


async def test_model_b_release_failure_defers_by_allocation_and_keeps_the_fence(
    log: LogRecorder,
) -> None:
    """release 任何未确认结果都按 `allocation_id` 退避,并且**不投递、不 expire**。

    守的是什么:未确认的 release 意味着那台 GameServer 可能还在。此时投补偿并放开
    fence,就等于"玩家已被判弃 + 旧 DS 可能还活着 + fence 已开放第二次 POST" ——
    §9.22 的双 DS 形状。退避是为了不让必然失败的重试占住队头(INC-20260724-001),
    但 outbox 语义不变:下一轮还会重试。

    ★ 变异:把 release 失败分支的 `self.note_sweep_deferral(...)` 删掉 → 本条红。
    """
    h = Harness(model_b=True, lifecycle=FakeLifecycle())
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle()
    h.auth_repo.abandon_script = [abandon_out()]
    h.release_gs_script = [ReleaseDeletionPendingError("pod terminating")]

    await h.sweep_once()

    assert h.sweep_defer_until[MATCH_ID].state == BS.RELEASE_PENDING_DEFER_PREFIX + ALLOC
    assert "publish_lifecycle" not in names()
    assert "expire_terminated" not in names()
    # deletion-pending 是"删除已受理、正在等宽限",不是失败 → 只留 debug 不打 WARN
    assert "model_b_sweep_release_grace_pending" in log.events()
    assert "model_b_sweep_release_failed" not in log.events()


async def test_model_b_terminate_fence_failure_stops_before_touching_k8s(
    log: LogRecorder,
) -> None:
    """terminate fence 没成功就**绝不发 release** —— 永久 fence 是外部回收的前置条件。

    守的是什么:fence 未提交时 battle/auth 仍是有限 TTL,Release 响应未知后 TTL 一过
    就会自行开放第二次 GSA POST(同一 match 两台 DS)。所以顺序必须是"先把 fence 钉死,
    再碰 K8s"。

    ★ 变异:把 `if not terminated:` 后的 `return` 删掉 → 本条红。
    """
    h = Harness(model_b=True, lifecycle=FakeLifecycle())
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle()
    h.auth_repo.abandon_script = [abandon_out()]
    h.auth_repo.terminate_script = [False]

    await h.sweep_once()

    assert "release_gs" not in names()
    assert "publish_lifecycle" not in names()
    assert "model_b_sweep_terminate_fence_failed" in log.events()


async def test_model_b_authority_read_failure_never_releases_from_the_derived_index(
    log: LogRecorder,
) -> None:
    """权威读失败时 fail-closed:绝不凭派生 ZSET 直接 Release;仅当 battle 已消失才清索引。

    守的是什么(§9.22):active ZSET 是**派生**索引,不是权威。Redis 权威读不出来时拿它
    当证据去 Release,就是用一份可能陈旧的投影删一台可能载人的 DS。而 battle 记录确认
    已随 TTL 消失时,清掉残留索引项是安全且必要的(否则它每轮都被扫)。

    ★ 变异:把 `except` 分支里的 `if existing is None:` 改成 `if True:` → 本条红。
    """
    h = Harness(model_b=True, lifecycle=FakeLifecycle())
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle()
    h.auth_repo.abandon_script = [RuntimeError("redis WATCH aborted")]
    h.repo.zset[MATCH_ID] = ACTIVE_CUTOFF_MS - 1

    await h.sweep_once()

    assert "release_gs" not in names()
    assert "remove_active" not in names(), "battle 仍在却清了索引"
    assert h.repo.zset == {MATCH_ID: ACTIVE_CUTOFF_MS - 1}
    assert log.find("model_b_sweep_authority_check_failed")["err"] == "redis WATCH aborted"


async def test_model_b_stale_allocating_claim_is_revoked_by_allocation_id() -> None:
    """仍为 `allocating` 的陈旧 claim 按 `allocation_id` 直接撤销,**不发任何外部 release**。

    守的是什么:Model B 只有 fence 成功把 state 推进到 `allocation_uncertain` 之后才允许
    GSA POST。仍是 `allocating` 机械证明外部副作用尚未开始 —— 此时伪造一个缺 UID 的
    release 反而会在同名重建场景下删错机器。

    ★ 变异:把 `delete_battle_if_allocation_matches(mid, inflight.allocation_id, ...)` 的
      `inflight.allocation_id` 改成 `""` → 本条红。
    """
    h = Harness(model_b=True, lifecycle=FakeLifecycle())
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle(state=STATE_ALLOCATING)

    await h.sweep_once()

    assert ("delete_if_matches", MATCH_ID, ALLOC, POD) in TAPE
    assert "release_gs" not in names()
    assert "abandon_if_stale" not in names()


async def test_epoch_zero_abandoned_resume_defers_only_after_a_failed_attempt() -> None:
    """`abandoned + epoch==0` 的 resume 保持最高优先级:失败才退避,且键是 allocation。

    守的是什么:这条是 §9.4 补偿的**最后一棒**(uncertain 对账已提交 ABANDONED,但在
    Kafka ACK / Expire 之前崩溃)。把它加进 `stuck_reconcile_state` 会让补偿链最后一步
    每次都先白等 15s;完全不退避则控制面持续故障时它每轮吃满预算。所以:先试,
    失败才退。

    ★ 变异:把 `if not resumed and inflight.allocation_id != "":` 改成
      `if inflight.allocation_id != "":` → 本条红(成功也会被退避)。
    """
    h = Harness(model_b=True, lifecycle=FakeLifecycle())
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle(state=STATE_ABANDONED, instance_epoch=0)
    h.resume_abandoned_script = [True]

    await h.sweep_once()
    assert ("resume_abandoned", MATCH_ID) in TAPE
    assert MATCH_ID not in h.sweep_defer_until, "成功的 resume 不该被退避"

    TAPE.clear()
    h.resume_abandoned_script = [False]
    await h.sweep_once()
    assert h.sweep_defer_until[MATCH_ID].state == BS.RELEASE_PENDING_DEFER_PREFIX + ALLOC


async def test_bootstrap_reclaim_defers_only_on_unconfirmed_outcome() -> None:
    """auth/active 缺失分支按**结构化结果**退避:UNCONFIRMED 退,COMPLETED / SKIPPED 不退。

    守的是什么(复审 P1-2):`SKIPPED` 表示零外部副作用(不适用),退避它等于凭空延后
    一个可行的收敛;`UNCONFIRMED` 表示外部结果未知,不退就会每轮重复外部调用。用
    bool 表达这三态一定会把其中两态混在一起。

    ★ 变异:把 `elif outcome == PreactiveReleaseOutcome.UNCONFIRMED:` 改成 `else:`
      → 本条红(SKIPPED 也会被退避)。
    """
    h = Harness(model_b=True, lifecycle=FakeLifecycle())
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle()
    h.auth_repo.abandon_script = [abandon_out(auth_found=False, active_found=False)]
    h.preactive_script = [PreactiveReleaseOutcome.SKIPPED]

    await h.sweep_once()
    assert ("reconcile_preactive", MATCH_ID) in TAPE
    assert MATCH_ID not in h.sweep_defer_until

    TAPE.clear()
    h.auth_repo.abandon_script = [abandon_out(auth_found=False, active_found=False)]
    h.preactive_script = [PreactiveReleaseOutcome.UNCONFIRMED]
    await h.sweep_once()
    assert h.sweep_defer_until[MATCH_ID].state == BS.RELEASE_PENDING_DEFER_PREFIX + ALLOC


async def test_single_item_read_failure_does_not_abort_the_round() -> None:
    """单项 `get_battle` 失败只跳过该项,后续项照常处理。

    守的是什么:一次 Redis 抖动不该让整轮 §9.4 补偿停摆 —— 队尾那些**能**收敛的对局
    必须照常推进。

    ★ 变异:把 `allocation_sweep_read_failed` 分支的 `continue` 改成 `raise` → 本条红。
    """
    h = Harness(model_b=False, with_index_reconciler=False, lifecycle=FakeLifecycle())
    h.repo.stale = [MATCH_ID, MATCH_ID_2]
    h.repo.get_exc[MATCH_ID] = RuntimeError("slot moved")
    h.repo.battles[MATCH_ID_2] = battle(match_id=MATCH_ID_2)

    await h.sweep_once()

    assert BS.plog.get().find("allocation_sweep_read_failed")["match_id"] == MATCH_ID
    assert h.repo.battles[MATCH_ID_2].state == STATE_ABANDONED


# ══ ⑧ 派生 active 索引重建:fail-closed + 节流 ══════════════════════════════


async def test_model_b_without_index_reconciler_fails_the_whole_round_closed(
    log: LogRecorder,
) -> None:
    """Model B 缺派生索引重建能力 ⇒ **整轮 sweep fail-closed**,一项都不扫。

    守的是什么:没有重建能力时,一次丢失的 ZADD 就让某个永久墓碑**永远**没人扫到
    (对应的 GameServer 永久占位)。此时继续扫描等于"用一份已知可能残缺的索引宣称
    自己在做补偿" —— 比不扫更危险,因为它看起来是健康的。

    ★ 变异(已实跑):把 `reconcile_active_index_if_due` 里 `if self.model_b:` 的
      `raise errcode.PandoraError(...)` 改成 `return` → 本条红。
    """
    h = Harness(model_b=True, with_index_reconciler=False)
    h.repo.stale = [MATCH_ID]

    with pytest.raises(errcode.PandoraError) as ei:
        await h.sweep_once()

    assert errcode.as_code(ei.value) == errcode.ErrInvalidState
    assert "range_stale" not in names(), "fail-closed 却仍扫描了"
    assert log.find("battle_active_index_refused")["reason"] == BS.REASON_ACTIVE_INDEX_UNAVAILABLE


async def test_index_rebuild_failure_aborts_the_round_before_any_scan(log: LogRecorder) -> None:
    """重建失败 ⇒ 本轮整体不执行(判弃补偿链顺延一个周期),且 `last_*` 不推进。

    守的是什么:重建失败意味着索引可能仍缺项。用一份可能缺项的索引继续判弃,会漏掉
    真正失联的对局;而节流时间戳若被推进,下一轮还会跳过重建 —— 一次失败变成 30s 的
    盲区。

    ★ 变异:把 `reconcile_battle_active_index` 的 `except` 里 `raise` 改成 `pass`
      → 本条红。
    """
    h = Harness(model_b=True)
    h.active_index_reconciler.exc = RuntimeError("scan timeout")
    h.repo.stale = [MATCH_ID]

    with pytest.raises(errcode.PandoraError) as ei:
        await h.sweep_once()

    assert errcode.as_code(ei.value) == errcode.ErrUnavailable
    assert "range_stale" not in names()
    assert h.last_active_index_reconcile == 0.0, "失败却推进了节流时间戳"
    assert log.find("battle_active_index_refused")["reason"] == BS.REASON_ACTIVE_INDEX_REBUILD


async def test_index_reconcile_is_throttled_to_thirty_seconds() -> None:
    """索引重建按 30s 节流,不是每轮(5s)都全量 SCAN。

    守的是什么:每轮全量 SCAN canonical 记录会把 Redis 打满(sweep 每 5s 一轮);完全不
    节流地关掉重建则永久墓碑无人发现。30s 是两者之间的既定折中,且必须与 Go 同值,
    否则两栈的"漏扫窗口"口径对不上。

    ★ 变异:把节流判据 `now - self.last_active_index_reconcile <
      ACTIVE_INDEX_RECONCILE_INTERVAL_SEC` 里的常量改成 `0` → 本条红。
    """
    h = Harness(model_b=True)
    await h.sweep_once()
    assert h.active_index_reconciler.calls == 1
    assert h.last_active_index_reconcile > 0.0
    first = h.last_active_index_reconcile

    await h.sweep_once()
    assert h.active_index_reconciler.calls == 1, "30s 内重复重建"
    assert h.last_active_index_reconcile == first

    # 把上次重建时间推回到 30s 之前 → 再次到期
    h.last_active_index_reconcile = first - ACTIVE_INDEX_RECONCILE_INTERVAL_SEC - 0.001
    await h.sweep_once()
    assert h.active_index_reconciler.calls == 2
    assert ACTIVE_INDEX_RECONCILE_INTERVAL_SEC == 30.0
    assert BS.ACTIVE_INDEX_RECONCILE_SCAN_COUNT == 256


async def test_legacy_without_reconciler_keeps_sweeping() -> None:
    """legacy(非 Model B)缺重建能力时**不** fail-closed,照常扫描。

    守的是什么:fail-closed 只对 Model B 成立(它的永久墓碑依赖索引重建才能被发现)。
    对 legacy 一并 fail-closed 会让 local / mock 档的 §9.4 补偿链整体停摆 —— 开发与压测
    环境全部失去 abandoned 回收,而且没有报错。

    ★ 变异:把 `if self.active_index_reconciler is None:` 里的 `if self.model_b:` 改成
      `if True:` → 本条红。
    """
    h = Harness(model_b=False, with_index_reconciler=False, lifecycle=FakeLifecycle())
    h.repo.stale = [MATCH_ID]
    h.repo.battles[MATCH_ID] = battle()

    await h.sweep_once()

    assert h.repo.battles[MATCH_ID].state == STATE_ABANDONED


# ══ ⑨ rosterGateArmable ═════════════════════════════════════════════════════


def test_roster_gate_armable_is_fail_safe_on_missing_age() -> None:
    """拿不到年龄(`allocated_at_ms<=0`)或窗口未配就**不武装**;边界两端都闭合。

    守的是什么:这个闸的动作是**判弃一局对局**。滚动升级中接手心跳的新副本对一局
    老 battle 必须给出与前任相同的答案(判据只能是本局年龄,不能是任何进程内状态);
    拿不到年龄时宁可漏防一局缺员局,也不能误判弃一局正在打的对局(§9 不变量 20)。

    ★ 变异:把 `battle_auth.roster_gate_armable` 的
      `if arm_window_ms <= 0 or allocated_at_ms <= 0: return False` 删掉 → 本条红。
    """
    assert BS.roster_gate_armable(1_000, 1_000, 60_000) is True  # age == 0
    assert BS.roster_gate_armable(1_000, 61_000, 60_000) is True  # age == 窗口上界
    assert BS.roster_gate_armable(1_000, 61_001, 60_000) is False  # 超窗 1ms
    assert BS.roster_gate_armable(2_000, 1_000, 60_000) is False  # 负年龄(时钟倒退)
    assert BS.roster_gate_armable(0, 61_000, 60_000) is False  # 旧记录 / mock
    assert BS.roster_gate_armable(1_000, 61_000, 0) is False  # 闸未配置
