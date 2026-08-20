"""ds_allocator 业务层 **释放 / 中止 / 对账链** —— 对应 Go 侧
`services/battle/ds_allocator/internal/biz/allocator.go` 第 1197–2436 行。

本模块提供 `ReleaseMixin`(Go 里带 `(u *AllocatorUsecase)` receiver 的那批)与
Go 里同段落的模块级纯函数。最终组装(与 `hub_allocator` 同构):

    class AllocatorUsecase(SweepMixin, HeartbeatMixin, ReleaseMixin,
                           AllocateMixin, AllocatorUsecaseBase): ...

`ReleaseMixin` **不继承任何东西**:字段(`self.repo` / `self.auth_repo` /
`self.authoritative_alloc` / `self.abort_repo` / `self.lifecycle_proof_repo` /
`self.owner_auth` / `self.model_b` / `self.cfg`)与 accessor(`battle_ttl_sec()` /
`ready_wait_timeout_sec()` / `heartbeat_timeout_ms()`)全部来自
`biz_base.AllocatorUsecaseBase`,一个都不在这里重复实现 —— 重复实现会在 MRO 里
制造"两份同名方法、谁在前谁赢"的静默分叉。

本模块**调用但不实现**的两个同类成员(属别批移植范围):

  - `self.deliver_abandoned(...)`  Go allocator.go:3994,`SweepMixin` 提供。
    它是"GameServer 已确认回收 → 发 ABANDONED 生命周期事件"的唯一出口
    (§9 不变量 4 的补偿链)。
  - `self.allocate_battle_with_combat_factions(...)` 不在本模块调用链上。

═══════════════════════════════════════════════════════════════════════════════
这批代码守的是 §9 不变量 22 与不变量 4,四条判据一条都不能"看着差不多"
═══════════════════════════════════════════════════════════════════════════════

  ① **allocation_id 是未知结果对账的唯一 fencing token**
     GSA POST 超时后,后端**永远不知道**那台 GameServer 到底建没建出来。唯一
     可依赖的事实是:POST 请求里带了一个不可变的 `allocation_id` label。
     `reconcile_allocation_uncertain` 只允许"按 allocation_id LIST → 恰零个 /
     恰一个"两种结论;LIST 失败、多于一个,一律保留永久 fence 交下一轮 ——
     **绝不允许再发第二次 POST**。放宽这一条 = 同一 match 出现第二台 DS。

  ② **pod_uid 是 exact 身份,没有它就没有删除权**
     Kubernetes 里同名 Pod 可以被重建 —— 这是教科书式 ABA。`ensure_durable_release_pod_uid`
     必须先把 pod_uid **持久化**到 Redis 权威记录里,才允许任何 DELETE。
     拿不到就 `ErrUnavailable`(可重试),**绝不按名字删**:按名字删 = 误杀一台
     正在服务玩家的 Allocated GameServer(项目红线
     never-delete-allocated-gameserver-20260803)。

  ③ **preactive fenced 释放(epoch=0)与已授权释放是两条路**
     `release_fenced_preactive_game_server` 只处理"exact 身份已定格、但
     PrepareCredential 还没分配 instance_epoch"的崩溃窗口。epoch=0 从未准入过
     玩家、从未签出过票,所以它**必须**物理回收 + purge,而**不得**伪造一份
     "有凭据的 instance-teardown proof"。反过来 epoch≠0 一律回落
     `release_game_server`(带 RecordInstanceTeardown 的完整证明链)。

  ④ **fence 先于 K8s,purge 后于确认**
     `cleanup_allocated_battle` / `reconcile_preactive_release` 的顺序是
     **永久 release fence → UID 条件回收 → 明确成功后才 purge**。
     旧实现"先删 Redis 再 Release"会留下"Redis 已无 claim、GSA 仍可能活着"的窗口,
     下一个请求就能对同 match 发第二次 POST。ReleaseExpected 的
     timeout/unknown **必须**保留永久 fence,不得 purge。

`wait_battle_ready` 的 `ready_wait` 超时属 `CLAUDE.md §16.10` 的**合法兜底**而非
掩盖:到期动作是"回收 pod + 删镜像 + 返回 ErrDSAllocationFailed",即**重查权威并
把无界等待收敛成有界**,不是"到点就假设 DS 已就绪"。其有界结构(单一 deadline +
固定 tick + 每个提前失败都带枚举 reason)原样照搬。

═══════════════════════════════════════════════════════════════════════════════
与 Go 的必要形变(其余逐行同构,逐条都有理由)
═══════════════════════════════════════════════════════════════════════════════

  1. **`(值, error)` → 返回值 + 抛异常**。唯一保留 bool 的是 Go 用 bool 表达
     **控制流**的地方(`publish_reconciled_allocation_abandoned` 等四个对账推进
     函数、`reconcile_preactive_release` 的三态 outcome)——那不是错误传播,而是
     "本轮能不能往下走"的判定,吞掉它会让墓碑被提前 purge。

  2. **`err != nil → return false` 的每一处都写成 `try/except`**。Go 那批
     `if err != nil || <形状不符> { warn; return false }` 把"读失败"和"读到的不对"
     收敛到同一条日志,Python 必须把异常先接住存进变量,再走同一个 `or` 链 ——
     写成两条独立的 except/if 会让日志里少掉一半上下文。
     每处 `except BaseException` 之上都紧邻 `except asyncio.CancelledError: raise`:
     取消是停机控制流,被吞掉会让进程停不下来。

  3. **`ctx` 不存在**。Go 的 `ctx.Done()`(调用方 matchmaker 先放弃)在 Python 里
     就是 `await` 点抛 `CancelledError`;`wait_battle_ready` 捕获后打
     `reason=caller_ctx_done` 再**原样重抛**,与 Go `return ctx.Err()` 同义。

  4. **`plog.Detach(ctx)` 的等价物是"结构上拿不到请求 ctx"**。
     `cleanup_allocated_battle` 用 `asyncio.timeout(DETACHED_CLEANUP_TIMEOUT_SEC)`
     兜住 Go 那句"入站 ctx 多半已超时,复用它会立刻失败"的动机(deadline 隔离)。
     ⚠️ **一处如实标注的语义分叉**:Go 的 detached ctx 在调用方 goroutine 被取消后
     仍能跑完;Python 的 task 被 cancel 时,本协程内的 await 一样会被打断。
     真正的"调用方撤了也要回收"必须由调用方用 `safego.spawn` 起独立任务
     (基座的 `kill_stranded_ds` 即此形状),不能靠本函数自己变出一条命。

  5. **时间源分两种,不混用**。等待窗口 / 宽限期用 `time.monotonic()`
     (NTP 回拨不会让 ready_wait 忽然变成几小时);写进 Redis / 索引 score 的
     时间戳仍用 `now_ms()`(权威快照里全是 unix ms,换单调钟会与另一栈对不上)。

  6. **`allocation` 是可变 dataclass**。Go 里 `allocation.PodUID = podUID` 是对
     指针的原地写,调用方看得到。Python 侧必须用
     `agones_allocator.AuthoritativeGameServerAllocation`(`slots=True`,**非
     frozen**)而不是 `repo.AuthoritativeGameServerAllocation`(frozen 搬运壳)——
     后者赋值会抛 `FrozenInstanceError`,把一次正常回收变成 500。
"""

from __future__ import annotations

import asyncio
import enum
import time
from typing import Any, Protocol, runtime_checkable

from pandora.ds.v1 import allocator_pb2 as dspb

from pandorapy import dsmetadata, errcode
from pandorapy import log as plog
from pandorapy import battleabort, releasetrack
from pandorapy.protoenum import enum_name
from pandorapy.services.ds_allocator import biz_base as base
from pandorapy.services.ds_allocator.agones_allocator import (
    AuthoritativeGameServerAllocation,
)
from pandorapy.services.ds_allocator.battle_auth import (
    BattleAuthoritySnapshot,
    BattleExpectedInstance,
    BattleResultAuthorizationProof,
    now_ms,
)
from pandorapy.services.ds_allocator.biz_base import (
    DETACHED_CLEANUP_TIMEOUT_SEC,
    REASON_ABORT_ACK_CLEANUP_PENDING,
    REASON_ABORT_AUTHORITY_OFF,
    REASON_ABORT_COMPLETION_PENDING,
    REASON_ABORT_FENCE_FAILED,
    REASON_ABORT_FENCE_IDENTITY_MISSING,
    REASON_ABORT_LIFECYCLE_MARKER,
    REASON_ABORT_LIFECYCLE_PENDING,
    REASON_ABORT_PREFLIGHT_FAILED,
    REASON_ABORT_REQUEST_INCOMPLETE,
    REASON_ALLOC_MATCH_ID_REQUIRED,
    REASON_FINALIZE_PROOF_INCOMPLETE,
    REASON_FINALIZE_TOMBSTONE_PENDING,
    REASON_POD_UID_AUTHORITY_READ,
    REASON_POD_UID_BACKFILL_FAILED,
    REASON_POD_UID_IDENTITY_CHANGED,
    REASON_POD_UID_RESOLVE_FAILED,
    REASON_POD_UID_VERIFY_FAILED,
    REASON_RELEASE_AUTHORITY_OFF,
    REASON_RELEASE_AUTHORITY_REREAD,
    REASON_RELEASE_FENCED,
    REASON_RELEASE_IDENTITY_CHANGED,
    REASON_RELEASE_MODEL_B_UNSUPPORTED,
    REASON_RELEASE_POD_UID_MISSING,
    REASON_RELEASE_PROOF_INCOMPLETE,
    REASON_RELEASE_SNAPSHOT_STALE,
    REASON_RELEASE_TUPLE_INCOMPLETE,
    REASON_WAIT_ALLOC_SUPERSEDED,
    REASON_WAIT_AUTH_OUTSIDE_GRACE,
    REASON_WAIT_AUTH_PHASE_FENCED,
    REASON_WAIT_AUTH_PROVISION_DEAD,
    REASON_WAIT_AUTHORITY_PURGED,
    REASON_WAIT_CTX_DONE,
    REASON_WAIT_RECORD_GONE,
    REASON_WAIT_STATE_RECLAIMED,
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
    UPDATE_MAX_RETRY,
    AllocateResult,
    BattleWaitOwnershipLostError,
    ReadyWaitTimeoutError,
    allocate_result_from_battle,
)
from pandorapy.services.ds_allocator.departure import BattleDepartureSource
from pandorapy.services.ds_allocator.gameserver import (
    UncertainGameServerAllocationResolver,
)
from pandorapy.services.ds_allocator.owner_authority import (
    owner_release_abandoned_players_weak,
)
from pandorapy.services.ds_allocator.repo import BattleActiveIndexError

__all__ = [
    "OWNER_RELEASE_BUDGET_SEC",
    "AllocationUncertainRepo",
    "PreactiveReleaseOutcome",
    "ReleaseMixin",
    "battle_ready_for_pod",
    "battle_wait_state_progressable",
    "exact_release_battle_identity",
    "exact_release_identity_snapshot",
    "exact_terminated_release_snapshot",
    "release_state_allows_pod_uid_backfill",
]


#: owner 精确释放的**总**时间预算(秒)。Go 三处调用点写死 `2*time.Second`。
#:
#: ★ 抽成常量而不是三处各写 `2.0`:它是弱依赖的**尾延迟上界**,三条路径
#:   (legacy 正常结算 / abort saga 补偿 / Model B 正常结算)必须同时改,
#:   漏一处的表现是"某条路上 owner 释放悄悄多花几倍时间",而没有任何告警。
OWNER_RELEASE_BUDGET_SEC = 2.0


# ── 弱依赖 / 能力协议 ────────────────────────────────────────────────────────


@runtime_checkable
class AllocationUncertainRepo(Protocol):
    """POST 结果未知之后的**加法式**恢复写面。Go: `data.AllocationUncertainRepo`。

    ★ Go 用 `u.repo.(data.AllocationUncertainRepo)` 做**能力探测**,探测本身就是
      业务逻辑:探测不到 = 对账整体 fail-closed(永久 fence 原样留着交下一轮),
      而**不是**降级成"按名字清理"。滚动升级期的旧 writer 与测试替身必须继续停在
      那道 fence 上,不能因为多了个方法名就被当作"支持对账"。

    刻意与 `BattleRepo` 分离(最小权限):能读写对局镜像的代码不会顺带获得
    "把一次未知分配推进成终态 ABANDONED"的权限。
    """

    async def fence_allocation_uncertain_release(
        self, match_id: int, allocation_id: str, allocation: Any
    ) -> bool: ...

    async def complete_allocation_uncertain_release(
        self, match_id: int, allocation_id: str, instance_uid: str
    ) -> bool: ...

    async def mark_allocation_uncertain_empty_lifecycle_published(
        self, match_id: int, allocation_id: str
    ) -> bool: ...


# ── preactive/bootstrap 回收的三态结果 ───────────────────────────────────────


class PreactiveReleaseOutcome(enum.IntEnum):
    """preactive/bootstrap 回收单次推进的结构化结果。Go: `preactiveReleaseOutcome`。

    ★ 为什么不是 bool(复审 P1-2):调用方要据此决定**是否登记 allocation-bound
      退避**。单一 bool 会把「不适用」(零外部副作用,下轮状态自然分流)和
      「外部未确认」(控制面正在故障,再来一轮就是又一次外部调用占队头)混成
      同一个 false,于是要么该退避的不退避(打爆控制面),要么不该退避的退避了
      (回收被无谓推迟)。
    """

    #: 身份不完整 / fence 判定不适用(如 ACTIVE 赢家)。零外部副作用,无需退避。
    SKIPPED = 0
    #: preflight / release / purge 任一步结果未确认。重试前应按 exact allocation 退避。
    UNCONFIRMED = 1
    #: 释放 + purge 全链明确成功。
    COMPLETED = 2


# ── 模块级判据(Go 里同段落的无 receiver 函数)────────────────────────────────


def battle_ready_for_pod(b: Any, pod_name: str, match_id: int, allocated_at_ms: int) -> bool:
    """DS 是否已用 Heartbeat 确认 ready。对应 Go 的 `battleReadyForPod`。

    pod/match 对得上、有**分配之后**的真实心跳(`last_heartbeat_ms` 严格大于
    `allocated_at_ms`)、状态进入 ready 或 running。

    ★ `running` 也算可进入:当前 UE 侧上报的是 running(不一定先发 ready),
      后端先把 running 一并视为可进入状态 —— 照抄 Go 的口径,不"顺手收紧"。
    """
    return (
        b is not None
        and b.match_id == match_id
        and b.ds_pod_name == pod_name
        and b.last_heartbeat_ms > allocated_at_ms
        and (b.state == STATE_READY or b.state == STATE_RUNNING)
    )


def battle_wait_state_progressable(state: str) -> bool:
    """wait 可继续前进的状态**白名单**。对应 Go 的 `battleWaitStateProgressable`(复审 P1-3)。

    allocating/warming 是在途,ready/running 即将命中 ready 判定。其余一切状态
    (ended/abandoned、preactive_release_pending、allocation_abort_pending、
    allocation_uncertain/reconciling/empty_fence、**以及任何未知新状态**)都已由
    回收/对账链接管或属 fail-closed 墓碑 —— 继续等待只会白耗 ready_wait,
    owner 也绝不能对其 cleanup。

    ★ 必须是白名单而不是黑名单:黑名单遇到"另一栈新加的状态"会默认放行,
      于是一个已被别人接管的分配会被 owner 二次 cleanup(并发第二路
      fence/release 只有幂等最终一致保证,没有单次调用保证)。
    """
    return state in (STATE_ALLOCATING, STATE_WARMING, STATE_READY, STATE_RUNNING)


def exact_release_battle_identity(
    battle: Any,
    match_id: int,
    pod_name: str,
    expected: BattleExpectedInstance,
    expected_release_track: str,
) -> bool:
    """回收路径的 exact 身份门。对应 Go 的 `exactReleaseBattleIdentity`。

    七项逐条与判:match/allocation/pod/uid/epoch 全等、release_track 合法、
    (期望轨非空时)轨相符、状态允许 pod_uid 回填。少任一项都会让"同名 Pod 重建"
    或"记录已被推进到别的实例"被当成同一台机器。
    """
    return (
        battle is not None
        and battle.match_id == match_id
        and battle.allocation_id == expected.allocation_id
        and battle.ds_pod_name == pod_name
        and battle.gameserver_uid == expected.instance_uid
        and battle.instance_epoch == expected.instance_epoch
        and releasetrack.valid(battle.release_track)
        and (expected_release_track == "" or battle.release_track == expected_release_track)
        and release_state_allows_pod_uid_backfill(battle.state)
    )


def release_state_allows_pod_uid_backfill(state: str) -> bool:
    """允许回填 pod_uid 的状态白名单。对应 Go 的 `releaseStateAllowsPodUIDBackfill`。

    ★ 同样是白名单:`allocating` **不在**其中(exact 身份尚未定格),
      各类 uncertain / empty_fence 墓碑也不在(它们归对账链管,不走这条回收)。
    """
    return state in (
        STATE_WARMING,
        STATE_READY,
        STATE_RUNNING,
        STATE_ENDED,
        STATE_ABANDONED,
        STATE_PREACTIVE_RELEASING,
        STATE_ALLOCATION_ABORT,
    )


def exact_release_identity_snapshot(
    snapshot: BattleAuthoritySnapshot | None,
    match_id: int,
    pod_name: str,
    expected: BattleExpectedInstance,
    expected_release_track: str,
) -> bool:
    """快照级 exact 身份门。对应 Go 的 `exactReleaseIdentitySnapshot`。

    ★ auth **允许缺失**(结算中继之前 callback auth 的 TTL 可能自然到期,
      `terminate_result_expected` 有一条 proof-bound 的重建路径);但只要它在,
      就必须仍绑定同一份 stable allocation —— 四个字段逐项比,一项都不能省。
    """
    if snapshot is None or not snapshot.battle_found:
        return False
    if not exact_release_battle_identity(
        snapshot.battle, match_id, pod_name, expected, expected_release_track
    ):
        return False
    return not snapshot.auth_found or (
        snapshot.auth is not None
        and snapshot.auth.match_id == match_id
        and snapshot.auth.allocation_id == expected.allocation_id
        and snapshot.auth.ds_pod_name == pod_name
        and snapshot.auth.instance_uid == expected.instance_uid
        and snapshot.auth.instance_epoch == expected.instance_epoch
    )


def exact_terminated_release_snapshot(
    snapshot: BattleAuthoritySnapshot | None,
    match_id: int,
    pod_name: str,
    expected: BattleExpectedInstance,
) -> bool:
    """终态 CAS **之后**的自洽重读门。对应 Go 的 `exactTerminatedReleaseSnapshot`。

    ★ 与 `exact_release_identity_snapshot` 的关键差别:这里 auth **必须存在**
      且 `phase == TERMINATING`、battle 必须 `state == ended` 且 `pod_uid != ""`。
      终态 CAS 已经成功,重读却不自洽 = 有人在这中间改了权威 —— 此时**不碰 K8s**,
      让 outbox 用同一 tuple 幂等重试,而不是拿一份可疑身份去删 Pod。
    """
    if snapshot is None:
        return False
    return (
        snapshot.auth_found
        and snapshot.auth is not None
        and snapshot.battle_found
        and snapshot.battle is not None
        and exact_release_battle_identity(snapshot.battle, match_id, pod_name, expected, "")
        and snapshot.battle.state == STATE_ENDED
        and snapshot.battle.pod_uid != ""
        and snapshot.auth.match_id == match_id
        and snapshot.auth.allocation_id == expected.allocation_id
        and snapshot.auth.ds_pod_name == pod_name
        and snapshot.auth.instance_uid == expected.instance_uid
        and snapshot.auth.instance_epoch == expected.instance_epoch
        and snapshot.auth.phase == dspb.BATTLE_AUTH_PHASE_TERMINATING
    )


def _combat_faction_map_from_records(
    player_ids: list[int], records: Any
) -> dict[int, int] | None:
    """roster 阵营记录 → `{player_id: faction_id}`。对应 Go 的 `combatFactionMapFromRecords`
    (allocator.go:887)。

    ★ **权威归属说明**:Go 侧这个函数属 `biz_allocate` 的移植范围(它同时被分配主链
      使用),但本模块的 `reconcile_allocation_uncertain` 是它在本批次内的调用点,
      而 `biz_allocate` 尚未落地。这里给出的是**完整实现**(不是占位),`biz_allocate`
      落地时必须改成从单一处 import,不得两份并存 —— 两份的漂移表现是
      "同一份 roster 在分配时通过、在对账时被判非法",于是一台泄漏的 Pod 永远删不掉。

    Raises:
        ValueError: 记录与 canonical roster 不对齐 / 顺序不对 / faction_id 越界。
            对应 Go 的 `errors.New(...)`(调用方 fail-closed,**不**继续对账)。
    """
    if not records:
        # Go: `len(records) == 0 → (nil, nil)`。空阵营映射是合法的(旧 matchmaker
        # 不带该字段),不是错误。
        return None
    canonical_players, _ = dsmetadata.canonical_roster(player_ids)
    if list(player_ids) != canonical_players or len(records) != len(player_ids):
        raise ValueError("combat faction records must align with canonical battle roster")
    factions: dict[int, int] = {}
    for i, record in enumerate(records):
        if record is None or record.player_id != player_ids[i]:
            raise ValueError("combat faction records are not in canonical roster order")
        if record.combat_faction_id > dsmetadata.MAX_COMBAT_FACTION_ID:
            raise ValueError("combat faction_id exceeds DS camp range")
        factions[record.player_id] = record.combat_faction_id
    dsmetadata.canonical_combat_factions(canonical_players, factions)
    return factions


def _get_state(battle: Any) -> str:
    """`battle.GetState()` 的 Python 等价(nil-safe)。

    ★ Go 的 getter 在 nil receiver 上返回零值,Python 会 `AttributeError`。
      这一行差别只出现在**诊断日志**里,而那正是"该拒绝"被变成 500 的常见姿势。
    """
    return battle.state if battle is not None else ""


def _get_pod_uid(battle: Any) -> str:
    """`battle.GetPodUid()` 的 Python 等价(nil-safe)。理由同 `_get_state`。"""
    return battle.pod_uid if battle is not None else ""


class ReleaseMixin:
    """Go `(u *AllocatorUsecase)` 第 1197–2436 行的方法集。

    ★ 不继承任何东西:所有状态经 `self.xxx` 由 `AllocatorUsecaseBase` 提供。
      在这里写 `__init__` 或再声明一遍字段,会让"基座改了默认值、mixin 还是旧值"
      的分叉在组装后静默生效。
    """

    # ── ready 等待(AllocateBattle 的最后一段)────────────────────────────

    async def wait_battle_ready(  # noqa: C901, PLR0912, PLR0915 —— 与 Go 逐条对应
        self, match_id: int, pod_name: str, allocation_id: str
    ) -> AllocateResult:
        """轮询 Redis 镜像直到 DS 心跳确认 ready。对应 Go 的 `waitBattleReady`。

        Returns:
            该 match 的分配投影(非 None)。

        Raises:
            ReadyWaitTimeoutError: 到 `ready_wait_timeout` 仍未 ready。Go: `errReadyWaitTimeout`。
                调用方据此走 `fail_ready_wait_timeout`(回收 pod + 删镜像)。
            errcode.PandoraError(ErrDSAllocationFailed): 提前失败。
                ★ `cause` 为 `BattleWaitOwnershipLostError` 时表示**回收/接管所有权
                  已属他人**,owner **不得**再 cleanup(并发第二路 fence/release
                  只有幂等最终一致保证,没有单次调用保证)。
            asyncio.CancelledError: 调用方(matchmaker)先放弃。对应 Go 的 `ctx.Err()`。

        有界结构(§16.10 合法兜底,照搬):
          - `deadline` 是**唯一**上界。基础设施抖动(ReadAuthority / GetBattle 的
            传输错误)容忍到下个 tick,但**只容忍这一类** —— 下面每个 return 都是
            **权威判定**(键已 purge / 分配被取代 / auth 相位 fenced / 状态不可推进),
            它们是确定性事实,混进来一起吞会把「本分配已失效」拖成空转满 ready_wait
            (那正是历史上 141.85s 总耗时的根因)。
          - `waited_ms` 只进日志(§11.3 判据 5「慢在哪」),**不参与任何判定**。
        """
        # ★ 单调钟:墙钟被 NTP 回拨时,`ready_wait` 会忽然变成"几小时都不超时"。
        wait_start = time.monotonic()
        deadline = wait_start + self.ready_wait_timeout_sec()
        # auth 缺失的累计观察起点(单调;None = 当前不缺 auth)。owner 完成 provision
        # 后才进入 wait,正常路径永不缺 auth;只有 joiner 会在 Claim→Prepare、
        # Finalize→provision 在途窗口短暂看到缺 auth。
        auth_missing_since: float | None = None

        def waited_ms() -> int:
            return int((time.monotonic() - wait_start) * 1000)

        async def tick() -> None:
            """等一个轮询间隔;调用方先放弃时打枚举 reason 后原样重抛。

            ★ 间隔按**模块属性**读(`base.READY_POLL_INTERVAL_SEC`),与 Go 的
              `var readyPollInterval` 同为可写模块变量,单测调小即可避免慢测。
            """
            try:
                await asyncio.sleep(base.READY_POLL_INTERVAL_SEC)
            except asyncio.CancelledError:
                plog.get().warning(
                    "battle_ready_wait_aborted",
                    reason=REASON_WAIT_CTX_DONE,
                    match_id=match_id,
                    pod=pod_name,
                    allocation_id=allocation_id,
                    waited_ms=waited_ms(),
                    ready_wait=self.ready_wait_timeout_sec(),
                    hint="入站 ctx 已取消/超时,owner 会用 detached ctx 回收 pod 与 warming 镜像",
                )
                raise

        while True:
            if self.model_b:
                snapshot: BattleAuthoritySnapshot | None = None
                read_err: BaseException | None = None
                try:
                    snapshot = await self.auth_repo.read_authority(match_id)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001 —— 只容忍基础设施抖动
                    read_err = exc
                if read_err is not None:
                    # 基础设施抖动容忍到下个 tick(与下方 legacy 分支同语义,2026-08-04)。
                    # ⚠️ 本分支是**生产路径**(Model B = authority_mode=redis)。
                    plog.get().warning(
                        "battle_ready_wait_authority_read_transient",
                        match_id=match_id,
                        pod=pod_name,
                        err=str(read_err),
                        hint="基础设施抖动,下个 tick 重读;deadline 仍是唯一上界",
                    )
                    if time.monotonic() >= deadline:
                        raise ReadyWaitTimeoutError
                    await tick()
                    continue
                b = snapshot.battle
                # battle 键是分配的第一条持久痕迹:wait 上下文里它消失(无论 auth 是否
                # 残留)只可能是判死回收链已 purge 或 TTL 到期,本分配不可能再 ready。
                if not snapshot.battle_found:
                    plog.get().warning(
                        "battle_ready_wait_aborted",
                        reason=REASON_WAIT_AUTHORITY_PURGED,
                        match_id=match_id,
                        pod=pod_name,
                        allocation_id=allocation_id,
                        auth_found=snapshot.auth_found,
                        waited_ms=waited_ms(),
                        hint="battle 键已被判死回收链 purge 或 TTL 到期,本分配不可能再 ready",
                    )
                    raise errcode.PandoraError(
                        errcode.ErrDSAllocationFailed,
                        "battle %d authority purged while waiting ready (auth_found=%s)",
                        match_id,
                        snapshot.auth_found,
                        cause=BattleWaitOwnershipLostError(),
                    )
                if allocation_id != "" and b.allocation_id != allocation_id:
                    plog.get().warning(
                        "battle_ready_wait_aborted",
                        reason=REASON_WAIT_ALLOC_SUPERSEDED,
                        match_id=match_id,
                        pod=pod_name,
                        allocation_id=allocation_id,
                        current_allocation_id=b.allocation_id,
                        state=b.state,
                        waited_ms=waited_ms(),
                    )
                    raise errcode.PandoraError(
                        errcode.ErrDSAllocationFailed,
                        "battle %d allocation superseded",
                        match_id,
                        cause=BattleWaitOwnershipLostError(),
                    )
                # Auth/Battle 非对称处理(复审必修):
                #   - 缺 auth 且状态在 allocating/warming 之外:与 AbandonIfStale 的
                #     allocation-grace 契约同判,不可授权,立即失败;
                #   - 缺 auth 且在 grace 状态:只允许 joiner 的 provisioning 在途窗口,
                #     上界取 HeartbeatTimeout(15s)—— 超界即 provision 已死(半失败
                #     残留),不得复用完整冷加载宽限空等 120s。
                if not snapshot.auth_found:
                    if b.state != STATE_ALLOCATING and b.state != STATE_WARMING:
                        plog.get().warning(
                            "battle_ready_wait_aborted",
                            reason=REASON_WAIT_AUTH_OUTSIDE_GRACE,
                            match_id=match_id,
                            pod=pod_name,
                            allocation_id=allocation_id,
                            state=b.state,
                            waited_ms=waited_ms(),
                        )
                        raise errcode.PandoraError(
                            errcode.ErrDSAllocationFailed,
                            "battle %d missing auth outside allocation grace "
                            "while waiting ready: state=%s",
                            match_id,
                            b.state,
                            cause=BattleWaitOwnershipLostError(),
                        )
                    grace_sec = self.heartbeat_timeout_ms() / 1000.0
                    if auth_missing_since is None:
                        auth_missing_since = time.monotonic()
                    elif time.monotonic() - auth_missing_since > grace_sec:
                        plog.get().warning(
                            "battle_ready_wait_aborted",
                            reason=REASON_WAIT_AUTH_PROVISION_DEAD,
                            match_id=match_id,
                            pod=pod_name,
                            allocation_id=allocation_id,
                            state=b.state,
                            auth_missing_ms=int(
                                (time.monotonic() - auth_missing_since) * 1000
                            ),
                            grace=grace_sec,
                            hint=(
                                "凭据投递(Redis stage→K8s PATCH→delivered CAS)半失败残留,"
                                "交 sweep 按 grace 回收"
                            ),
                        )
                        raise errcode.PandoraError(
                            errcode.ErrDSAllocationFailed,
                            "battle %d auth provisioning stalled beyond grace "
                            "while waiting ready: state=%s",
                            match_id,
                            b.state,
                            cause=BattleWaitOwnershipLostError(),
                        )
                else:
                    auth_missing_since = None
                # 状态白名单(复审 P1-3):终态、abort/uncertain/release 各类墓碑与
                # 未知状态一律视为回收/对账链已接管,立即携带所有权哨兵失败。
                if not battle_wait_state_progressable(b.state):
                    plog.get().warning(
                        "battle_ready_wait_aborted",
                        reason=REASON_WAIT_STATE_RECLAIMED,
                        match_id=match_id,
                        pod=pod_name,
                        allocation_id=allocation_id,
                        state=b.state,
                        authority="redis",
                        waited_ms=waited_ms(),
                        hint="该分配已被回收/对账链接管(终态或各类墓碑),owner 不再 cleanup",
                    )
                    raise errcode.PandoraError(
                        errcode.ErrDSAllocationFailed,
                        "battle %d reclaimed while waiting ready: state=%s",
                        match_id,
                        b.state,
                        cause=BattleWaitOwnershipLostError(),
                    )
                # auth 相位门:TERMINATING/QUARANTINED 是回收/隔离链写下的永久 fence ——
                # 即使 battle 投影仍是 warming/ready/running(半状态),该分配也不可能
                # 再被授权 ready。
                if snapshot.auth_found:
                    phase = snapshot.auth.phase if snapshot.auth is not None else 0
                    if (
                        phase == dspb.BATTLE_AUTH_PHASE_TERMINATING
                        or phase == dspb.BATTLE_AUTH_PHASE_QUARANTINED
                    ):
                        plog.get().warning(
                            "battle_ready_wait_aborted",
                            reason=REASON_WAIT_AUTH_PHASE_FENCED,
                            match_id=match_id,
                            pod=pod_name,
                            allocation_id=allocation_id,
                            phase=enum_name(dspb.BattleAuthPhase, phase),
                            state=b.state,
                            waited_ms=waited_ms(),
                            hint=(
                                "battle 投影还是 warming/ready 的半状态,"
                                "但授权已被永久 fence,不可能再放行"
                            ),
                        )
                        raise errcode.PandoraError(
                            errcode.ErrDSAllocationFailed,
                            "battle %d auth fenced while waiting ready: phase=%s state=%s",
                            match_id,
                            enum_name(dspb.BattleAuthPhase, phase),
                            b.state,
                            cause=BattleWaitOwnershipLostError(),
                        )
                ready, _ = snapshot.ready_authorized(now_ms(), self.heartbeat_timeout_ms())
                if ready and (pod_name == "" or b.ds_pod_name == pod_name):
                    result = allocate_result_from_battle(b)
                    assert result is not None
                    return result
            else:
                legacy: Any = None
                legacy_err: BaseException | None = None
                try:
                    legacy = await self.repo.get_battle(match_id)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001 —— 只容忍基础设施抖动
                    legacy_err = exc
                if legacy_err is not None:
                    # 基础设施抖动容忍到下个 tick(legacy/local 面,2026-08-04)。
                    # 本机同时跑 UE 编辑器 + 多个 DS + 21 个服务时,单次读偶发超过
                    # read_timeout 即触发;一次抖动打掉整局分配是结构性脆弱。
                    plog.get().warning(
                        "battle_ready_wait_read_transient",
                        match_id=match_id,
                        pod=pod_name,
                        err=str(legacy_err),
                        hint="基础设施抖动,下个 tick 重读;deadline 仍是唯一上界",
                    )
                    if time.monotonic() >= deadline:
                        raise ReadyWaitTimeoutError
                    await tick()
                    continue
                # wait 由 finalize 之后进入,镜像消失只可能是回收/TTL,同 modelB 立即失败。
                if legacy is None:
                    plog.get().warning(
                        "battle_ready_wait_aborted",
                        reason=REASON_WAIT_RECORD_GONE,
                        match_id=match_id,
                        pod=pod_name,
                        allocation_id=allocation_id,
                        authority="legacy",
                        waited_ms=waited_ms(),
                    )
                    raise errcode.PandoraError(
                        errcode.ErrDSAllocationFailed,
                        "battle %d record gone while waiting ready",
                        match_id,
                        cause=BattleWaitOwnershipLostError(),
                    )
                if allocation_id != "" and legacy.allocation_id != allocation_id:
                    plog.get().warning(
                        "battle_ready_wait_aborted",
                        reason=REASON_WAIT_ALLOC_SUPERSEDED,
                        match_id=match_id,
                        pod=pod_name,
                        allocation_id=allocation_id,
                        current_allocation_id=legacy.allocation_id,
                        authority="legacy",
                        waited_ms=waited_ms(),
                    )
                    raise errcode.PandoraError(
                        errcode.ErrDSAllocationFailed,
                        "battle %d allocation superseded",
                        match_id,
                        cause=BattleWaitOwnershipLostError(),
                    )
                # legacy 同用白名单:滚动共存期也可能读到 modelB 写下的墓碑状态,一律不等。
                if not battle_wait_state_progressable(legacy.state):
                    plog.get().warning(
                        "battle_ready_wait_aborted",
                        reason=REASON_WAIT_STATE_RECLAIMED,
                        match_id=match_id,
                        pod=pod_name,
                        allocation_id=allocation_id,
                        state=legacy.state,
                        authority="legacy",
                        waited_ms=waited_ms(),
                    )
                    raise errcode.PandoraError(
                        errcode.ErrDSAllocationFailed,
                        "battle %d reclaimed while waiting ready: state=%s",
                        match_id,
                        legacy.state,
                        cause=BattleWaitOwnershipLostError(),
                    )
                if (pod_name == "" or legacy.ds_pod_name == pod_name) and battle_ready_for_pod(
                    legacy, legacy.ds_pod_name, match_id, legacy.allocated_at_ms
                ):
                    result = allocate_result_from_battle(legacy)
                    assert result is not None
                    return result
            if time.monotonic() >= deadline:
                raise ReadyWaitTimeoutError
            await tick()

    async def fail_ready_wait_timeout(
        self,
        match_id: int,
        allocation_id: str,
        pod_name: str,
        allocation: AuthoritativeGameServerAllocation | None,
        owner: bool,
    ) -> None:
        """处理 ready 等待超时。对应 Go 的 `failReadyWaitTimeout`。

        回收 pod + 删镜像,然后抛 `ErrDSAllocationFailed`。

        ★ **绝不把 ds_addr 回给 matchmaker** —— 客户端连上一台 `match_id` 仍为 0 的
          DS 会被 PreLogin 拒,表现是"分配成功但进不去",比直接失败更难查。
        ★ `owner=False`(joiner)时**不 cleanup**:回收所有权属 owner,
          两路并发 fence/release 没有单次调用保证。
        """
        plog.get().warning(
            "battle_ready_wait_timeout",
            match_id=match_id,
            pod=pod_name,
            allocation_id=allocation_id,
            ready_wait=self.ready_wait_timeout_sec(),
            owner=owner,
            hint=(
                "DS 在等待窗口内没上报 ready/running 心跳:查该 pod 的 DS 进程日志与镜像关卡表;"
                "owner=true 时本调用会回收 pod 并删 warming 镜像"
            ),
        )
        if owner:
            await self.cleanup_allocated_battle(match_id, allocation_id, pod_name, allocation)
        raise errcode.PandoraError(
            errcode.ErrDSAllocationFailed, "battle %d ds not ready within wait timeout", match_id
        )

    # ── ready 等待失败后的回收 ───────────────────────────────────────────

    async def cleanup_allocated_battle(  # noqa: C901 —— 与 Go 逐条对应
        self,
        match_id: int,
        allocation_id: str,
        pod_name: str,
        allocation: AuthoritativeGameServerAllocation | None,
    ) -> None:
        """永久 release fence → UID 条件回收 → 明确成功后 purge。
        对应 Go 的 `cleanupAllocatedBattle`。

        **顺序不可调换**(§9.22 ④):旧实现先 `DeleteExpected` 后 `ReleaseExpected`,
        DELETE 超时会留下"Redis 已无 claim、GSA 仍可能活着"的窗口,下一请求可对同
        match 发第二次 POST。

        best-effort:全程只 Warn,不抛 —— 调用方(`fail_ready_wait_timeout`)已经
        决定了要返回什么错误,回收失败不改变那个结论(残留由 sweep 对账)。

        ★ 独立超时预算 `DETACHED_CLEANUP_TIMEOUT_SEC`:ready 等待失败的常见原因
          **正是**入站 ctx 被取消/超时,复用它会立刻失败,留下 warming 镜像 +
          已分配 pod 泄漏。
        ⚠️ 语义分叉(见模块头注释 4):Python 侧无法在调用方 task 被 cancel 后继续跑。
        """
        try:
            async with asyncio.timeout(DETACHED_CLEANUP_TIMEOUT_SEC):
                await self._cleanup_allocated_battle_inner(
                    match_id, allocation_id, pod_name, allocation
                )
        except asyncio.CancelledError:
            # ★ 必须紧邻宽 except 之上。注意:`asyncio.timeout` 到期抛的是
            #   `TimeoutError`(由内部 CancelledError 转换),不会被这条吞掉。
            raise
        except BaseException as exc:  # noqa: BLE001 —— best-effort,失败只告警
            plog.get().warning(
                "ready_wait_cleanup_failed",
                match_id=match_id,
                allocation_id=allocation_id,
                pod=pod_name,
                err=str(exc),
            )

    async def _cleanup_allocated_battle_inner(  # noqa: C901 —— 与 Go 逐条对应
        self,
        match_id: int,
        allocation_id: str,
        pod_name: str,
        allocation: AuthoritativeGameServerAllocation | None,
    ) -> None:
        """`cleanup_allocated_battle` 的主体(Go 那个函数的 detached ctx 之内的部分)。"""
        if self.model_b:
            # 先把 auth+battle 同槽锁成永久 pre-active release fence,再触碰 K8s。
            expected = BattleExpectedInstance(
                allocation_id=allocation_id,
                instance_uid=allocation.instance_uid if allocation is not None else "",
                instance_epoch=allocation.instance_epoch if allocation is not None else 0,
            )
            fenced = False
            fence_err: BaseException | None = None
            try:
                fenced = await self.auth_repo.fence_preactive_release_expected(
                    match_id, expected
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                fence_err = exc
            if fence_err is not None or not fenced:
                plog.get().warning(
                    "ready_wait_cleanup_fence_failed",
                    match_id=match_id,
                    allocation_id=allocation_id,
                    fenced=fenced,
                    err=str(fence_err) if fence_err is not None else None,
                )
                return
            if pod_name == "" or allocation is None:
                plog.get().warning(
                    "ready_wait_cleanup_identity_missing",
                    match_id=match_id,
                    allocation_id=allocation_id,
                )
                return
            try:
                await self.release_fenced_preactive_game_server(match_id, pod_name, allocation)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                # ReleaseExpected timeout/unknown 必须保留永久 fence;不得 purge。
                plog.get().warning(
                    "ready_wait_cleanup_release_unconfirmed",
                    match_id=match_id,
                    pod=pod_name,
                    err=str(exc),
                )
                return
            purged = False
            purge_err: BaseException | None = None
            try:
                purged = await self.auth_repo.purge_preactive_released_expected(
                    match_id, expected
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                purge_err = exc
            if purge_err is not None or not purged:
                plog.get().warning(
                    "ready_wait_cleanup_purge_failed",
                    match_id=match_id,
                    allocation_id=allocation_id,
                    purged=purged,
                    err=str(purge_err) if purge_err is not None else None,
                )
            return

        deleted = False
        try:
            deleted = await self.repo.delete_battle_if_allocation_matches(
                match_id, allocation_id, pod_name
            )
        except asyncio.CancelledError:
            raise
        except BattleActiveIndexError as exc:
            # ★ Go 的 `return true, err`:权威 key 已按 fencing 条件删除,派生索引
            #   清理失败。`deleted=True` 是**释放对应 GameServer 的权利**,丢掉它
            #   那台 Pod 会一直挂着,而权威 key 已经没了、再没人能证明它属于谁。
            deleted = exc.deleted
            plog.get().warning(
                "ready_wait_cleanup_delete_failed", match_id=match_id, err=str(exc)
            )
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "ready_wait_cleanup_delete_failed", match_id=match_id, err=str(exc)
            )
        if not deleted or pod_name == "":
            return
        try:
            await self.release_game_server(match_id, pod_name, allocation)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "ready_wait_cleanup_release_failed",
                match_id=match_id,
                pod=pod_name,
                err=str(exc),
            )

    # ── GameServer 物理回收 ─────────────────────────────────────────────

    async def release_game_server(
        self,
        match_id: int,
        pod_name: str,
        allocation: AuthoritativeGameServerAllocation | None,
    ) -> None:
        """回收一台 GameServer。对应 Go 的 `releaseGameServer`。

        Raises:
            errcode.PandoraError(ErrInvalidState): expected 元组不全 / 拿不到 Pod UID。
                **身份不全一律不删(宁可占位)** —— 这正是防误删 exact 身份门在起作用。
            其它: `release_expected` / `record_instance_teardown` 的错误原样上抛。

        ★ `record_instance_teardown` 在 `release_expected` **成功之后**:先写 durable
          teardown proof,才允许上层 purge/expire battle+auth。proof 写失败必须整体
          返错保留永久 release fence;后续重试 ReleaseExpected(404 幂等成功)可补齐证明。
        """
        if not self.model_b:
            await self.alloc.release(pod_name)
            # 删除权留证(项目红线 never-delete-allocated-gameserver-20260803):
            # 每次真正回收 DS 都必须留下「删了什么」。legacy 面按 pod 名回收,没有
            # UID precondition,更需要能事后核对删的是不是该删的那台。
            plog.get().info(
                "battle_gameserver_released",
                match_id=match_id,
                pod=pod_name,
                authority="legacy",
                precondition="pod_name_only",
            )
            return
        if (
            match_id == 0
            or allocation is None
            or allocation.instance_uid == ""
            or allocation.instance_epoch == 0
            or allocation.allocation_id == ""
            or pod_name == ""
            or allocation.pod_name != pod_name
        ):
            # allocation 可能为 None,字段单独取,不在日志表达式里解引用。
            uid = allocation.instance_uid if allocation is not None else ""
            alloc_id = allocation.allocation_id if allocation is not None else ""
            alloc_pod = allocation.pod_name if allocation is not None else ""
            epoch = allocation.instance_epoch if allocation is not None else 0
            plog.get().warning(
                "battle_gameserver_release_refused",
                reason=REASON_RELEASE_TUPLE_INCOMPLETE,
                match_id=match_id,
                pod=pod_name,
                has_allocation=allocation is not None,
                uid=uid,
                epoch=epoch,
                allocation_id=alloc_id,
                allocation_pod=alloc_pod,
                hint="身份不全一律不删(宁可占位):这正是防误删 exact 身份门在起作用",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "battle Model B release requires complete expected GameServer tuple",
            )
        if allocation.pod_uid == "":
            allocation.pod_uid = await self.ensure_durable_release_pod_uid(
                match_id,
                pod_name,
                BattleExpectedInstance(
                    allocation_id=allocation.allocation_id,
                    instance_uid=allocation.instance_uid,
                    instance_epoch=allocation.instance_epoch,
                ),
                allocation.release_track,
            )
        if allocation.pod_uid == "":
            plog.get().warning(
                "battle_gameserver_release_refused",
                reason=REASON_RELEASE_POD_UID_MISSING,
                match_id=match_id,
                pod=pod_name,
                uid=allocation.instance_uid,
                epoch=allocation.instance_epoch,
                allocation_id=allocation.allocation_id,
                hint="拿不到 Pod UID 就没有 DELETE precondition,绝不按名字删",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "battle Model B release requires durable expected Pod UID",
            )
        await self.authoritative_alloc.release_expected(allocation)
        # 删除权留证(项目红线:绝不误删 Allocated GameServer)。UID+PodUID 双
        # precondition 已在 data 层生效,这里把生效的那组身份完整落盘,事后能逐字段
        # 核对删的是不是该删的实例。
        plog.get().info(
            "battle_gameserver_released",
            match_id=match_id,
            pod=pod_name,
            uid=allocation.instance_uid,
            pod_uid=allocation.pod_uid,
            epoch=allocation.instance_epoch,
            allocation_id=allocation.allocation_id,
            release_track=allocation.release_track,
            authority="redis",
            precondition="gs_uid+pod_uid",
        )
        await self.repo.record_instance_teardown(
            match_id,
            BattleDepartureSource(
                ds_pod_name=pod_name,
                gameserver_uid=allocation.instance_uid,
                instance_epoch=allocation.instance_epoch,
                allocation_id=allocation.allocation_id,
                pod_uid=allocation.pod_uid,
            ),
        )

    async def release_fenced_preactive_game_server(
        self,
        match_id: int,
        pod_name: str,
        allocation: AuthoritativeGameServerAllocation | None,
    ) -> None:
        """处理"exact 身份已定格、PrepareCredential 尚未分配 instance_epoch"的崩溃窗口。
        对应 Go 的 `releaseFencedPreactiveGameServer`。

        `fence_preactive_release_expected` 是本方法**强制的** Redis 线性化前置。

        ★ epoch=0 从未准入过 DS、从未签出过票,所以必须物理回收 + purge,而
          **绝不能**伪造一份"有凭据的 instance-teardown proof"(那份 proof 的语义是
          "某个 epoch 的实例已拆除",凭空造一条会让后续对账相信一个从未存在的凭据)。
        ★ epoch≠0 一律回落 `release_game_server`(带完整证明链)。
        """
        if allocation is None or allocation.instance_epoch != 0:
            await self.release_game_server(match_id, pod_name, allocation)
            return
        if (
            not self.model_b
            or match_id == 0
            or pod_name == ""
            or allocation.pod_name != pod_name
            or allocation.instance_uid == ""
            or allocation.allocation_id == ""
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "precredential release requires exact fenced GameServer tuple",
            )
        if allocation.pod_uid == "":
            allocation.pod_uid = await self.ensure_durable_release_pod_uid(
                match_id,
                pod_name,
                BattleExpectedInstance(
                    allocation_id=allocation.allocation_id,
                    instance_uid=allocation.instance_uid,
                    instance_epoch=0,
                ),
                allocation.release_track,
            )
        if allocation.pod_uid == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "precredential release requires durable expected Pod UID",
            )
        await self.authoritative_alloc.release_expected(allocation)

    async def reconcile_preactive_release(  # noqa: C901 —— 与 Go 逐条对应
        self, battle: Any
    ) -> PreactiveReleaseOutcome:
        """幂等完成 永久 pre-active release fence → UID 条件删除 → purge。
        对应 Go 的 `reconcilePreactiveRelease`。

        Returns:
            `SKIPPED` 只表示"不适用"(零外部副作用);`UNCONFIRMED` 表示外部结果
            未确认,调用方应按 exact allocation 退避。

        ★ 返回非 COMPLETED **不代表不安全**:墓碑保持永久,安全性不依赖下一轮一定执行。
        """
        if battle is None or battle.allocation_id == "" or battle.gameserver_uid == "":
            return PreactiveReleaseOutcome.SKIPPED
        expected = BattleExpectedInstance(
            allocation_id=battle.allocation_id,
            instance_uid=battle.gameserver_uid,
            instance_epoch=battle.instance_epoch,
        )
        try:
            pod_uid = await self.ensure_durable_release_pod_uid(
                battle.match_id, battle.ds_pod_name, expected, battle.release_track
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "preactive_release_pod_uid_preflight_failed",
                match_id=battle.match_id,
                allocation_id=battle.allocation_id,
                err=str(exc),
            )
            return PreactiveReleaseOutcome.UNCONFIRMED
        battle.pod_uid = pod_uid
        fenced = False
        fence_err: BaseException | None = None
        try:
            fenced = await self.auth_repo.fence_preactive_release_expected(
                battle.match_id, expected
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            fence_err = exc
        if fence_err is not None:
            plog.get().warning(
                "preactive_release_fence_failed",
                match_id=battle.match_id,
                allocation_id=battle.allocation_id,
                fenced=fenced,
                err=str(fence_err),
            )
            return PreactiveReleaseOutcome.UNCONFIRMED
        if not fenced:
            # fence 明确拒绝(身份不符/已激活赢家/状态不适用):零副作用,属不适用而非未确认。
            plog.get().debug(
                "preactive_release_fence_not_applicable",
                match_id=battle.match_id,
                allocation_id=battle.allocation_id,
            )
            return PreactiveReleaseOutcome.SKIPPED
        allocation = AuthoritativeGameServerAllocation(
            pod_name=battle.ds_pod_name,
            instance_uid=battle.gameserver_uid,
            pod_uid=battle.pod_uid,
            instance_epoch=battle.instance_epoch,
            allocation_id=battle.allocation_id,
        )
        try:
            await self.release_fenced_preactive_game_server(
                battle.match_id, battle.ds_pod_name, allocation
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "preactive_release_unconfirmed",
                match_id=battle.match_id,
                allocation_id=battle.allocation_id,
                err=str(exc),
            )
            return PreactiveReleaseOutcome.UNCONFIRMED
        purged = False
        purge_err: BaseException | None = None
        try:
            purged = await self.auth_repo.purge_preactive_released_expected(
                battle.match_id, expected
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            purge_err = exc
        if purge_err is not None or not purged:
            plog.get().warning(
                "preactive_release_purge_failed",
                match_id=battle.match_id,
                allocation_id=battle.allocation_id,
                purged=purged,
                err=str(purge_err) if purge_err is not None else None,
            )
            return PreactiveReleaseOutcome.UNCONFIRMED
        return PreactiveReleaseOutcome.COMPLETED

    # ── allocation-uncertain 对账链 ──────────────────────────────────────

    async def publish_reconciled_allocation_abandoned(  # noqa: C901 —— 与 Go 逐条对应
        self, battle: Any
    ) -> bool:
        """物理确认已回收 → 把 ABANDONED 交给 battle_result。
        对应 Go 的 `publishReconciledAllocationAbandoned`。

        ★ Redis battle 记录在 Kafka ACK 之前**保持永久**;只有 ACK 之后才可以拿到
          终态保留 TTL 并离开 active 恢复索引。反过来做 = 补偿事件丢了而记录已过期,
          玩家段位永远不回滚(§9 不变量 4 静默停摆)。
        """
        if (
            battle is None
            or battle.match_id == 0
            or battle.state != STATE_ABANDONED
            or battle.allocation_id == ""
            or battle.instance_epoch != 0
            or len(battle.player_ids) == 0
        ):
            return False
        snapshot: BattleAuthoritySnapshot | None = None
        read_err: BaseException | None = None
        try:
            snapshot = await self.auth_repo.read_authority(battle.match_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            read_err = exc
        if (
            read_err is not None
            or snapshot.auth_found
            or not snapshot.battle_found
            or snapshot.battle is None
            or snapshot.battle.state != STATE_ABANDONED
            or snapshot.battle.allocation_id != battle.allocation_id
            or snapshot.battle.gameserver_uid != battle.gameserver_uid
        ):
            plog.get().warning(
                "allocation_uncertain_terminal_snapshot_rejected",
                match_id=battle.match_id,
                allocation_id=battle.allocation_id,
                err=str(read_err) if read_err is not None else None,
            )
            return False
        battle = snapshot.battle
        if not await self.deliver_abandoned(
            battle.match_id,
            battle.ds_pod_name,
            battle.gameserver_uid,
            list(battle.player_ids),
            battle.map_id,
            battle.game_mode,
        ):
            return False
        if battle.gameserver_uid == "":
            # 空 LIST 足以解放玩家,却**不足以**交出清理权:原来那次超时的 POST
            # 仍可能在这次 LIST 之后生效。把 Kafka ACK 记成一个独立的墓碑状态,
            # 并永远继续轮询这个 allocation_id(直到将来有显式静默期证明)。
            if not isinstance(self.repo, AllocationUncertainRepo):
                return False
            marked = False
            mark_err: BaseException | None = None
            try:
                marked = await self.repo.mark_allocation_uncertain_empty_lifecycle_published(
                    battle.match_id, battle.allocation_id
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                mark_err = exc
            if mark_err is not None or not marked:
                plog.get().warning(
                    "allocation_uncertain_empty_tombstone_failed",
                    match_id=battle.match_id,
                    allocation_id=battle.allocation_id,
                    marked=marked,
                    err=str(mark_err) if mark_err is not None else None,
                )
                return False
            # 把下一轮清理推迟一个心跳窗,既不改动永久 battle 记录,也不假装这是
            # 一次 DS 心跳。
            try:
                await self.repo.touch_active(battle.match_id, now_ms())
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "allocation_uncertain_empty_tombstone_index_failed",
                    match_id=battle.match_id,
                    allocation_id=battle.allocation_id,
                    err=str(exc),
                )
                return False
            plog.get().info(
                "allocation_uncertain_empty_tombstone_retained",
                match_id=battle.match_id,
                allocation_id=battle.allocation_id,
            )
            return True
        try:
            await self.repo.expire_battle(battle.match_id, self.battle_ttl_sec())
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "allocation_uncertain_terminal_expire_failed",
                match_id=battle.match_id,
                allocation_id=battle.allocation_id,
                err=str(exc),
            )
            return False
        plog.get().info(
            "allocation_uncertain_terminal_delivered",
            match_id=battle.match_id,
            allocation_id=battle.allocation_id,
        )
        return True

    async def resume_reconciled_allocation_abandoned(self, battle: Any) -> bool:
        """durable ABANDONED CAS 与生命周期发布之间崩溃后的续跑。
        对应 Go 的 `resumeReconciledAllocationAbandoned`。

        重复的释放是 exact 且幂等的(UID+Pod UID;原始 POST 从未产出对象时退化为
        allocation_id label)。
        """
        if (
            battle is None
            or battle.state != STATE_ABANDONED
            or battle.instance_epoch != 0
            or battle.allocation_id == ""
        ):
            return False
        snapshot: BattleAuthoritySnapshot | None = None
        read_err: BaseException | None = None
        try:
            snapshot = await self.auth_repo.read_authority(battle.match_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            read_err = exc
        if (
            read_err is not None
            or snapshot.auth_found
            or not snapshot.battle_found
            or snapshot.battle is None
            or snapshot.battle.state != STATE_ABANDONED
            or snapshot.battle.allocation_id != battle.allocation_id
        ):
            return False
        battle = snapshot.battle
        allocation = AuthoritativeGameServerAllocation(allocation_id=battle.allocation_id)
        if battle.gameserver_uid != "":
            if battle.ds_pod_name == "" or battle.pod_uid == "":
                return False
            allocation.pod_name = battle.ds_pod_name
            allocation.instance_uid = battle.gameserver_uid
            allocation.pod_uid = battle.pod_uid
            allocation.release_track = battle.release_track
        try:
            await self.authoritative_alloc.release_expected(allocation)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "allocation_uncertain_terminal_release_unconfirmed",
                match_id=battle.match_id,
                allocation_id=battle.allocation_id,
                err=str(exc),
            )
            return False
        return await self.publish_reconciled_allocation_abandoned(battle)

    async def resume_empty_allocation_tombstone(self, battle: Any) -> bool:
        """玩家终态已发布之后,持续清理那个不可变的 allocation_id。
        对应 Go 的 `resumeEmptyAllocationTombstone`。

        ★ 它**刻意永不** expire/delete 这块墓碑:一个迟到的原始 POST 没有凭据、
          准入不了玩家,但只要它变得可见就必须被收掉。把墓碑退休只能靠将来显式的
          静默期证明 / 运维确认,不能靠 TTL 让它自己消失。
        """
        if (
            battle is None
            or battle.state != STATE_ALLOCATION_EMPTY_FENCE
            or battle.allocation_id == ""
            or battle.instance_epoch != 0
            or battle.ds_pod_name != ""
            or battle.gameserver_uid != ""
            or battle.pod_uid != ""
        ):
            return False
        snapshot: BattleAuthoritySnapshot | None = None
        read_err: BaseException | None = None
        try:
            snapshot = await self.auth_repo.read_authority(battle.match_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            read_err = exc
        if (
            read_err is not None
            or snapshot.auth_found
            or not snapshot.battle_found
            or snapshot.battle is None
            or snapshot.battle.state != STATE_ALLOCATION_EMPTY_FENCE
            or snapshot.battle.allocation_id != battle.allocation_id
        ):
            return False
        try:
            await self.authoritative_alloc.release_expected(
                AuthoritativeGameServerAllocation(allocation_id=battle.allocation_id)
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "allocation_uncertain_empty_tombstone_cleanup_failed",
                match_id=battle.match_id,
                allocation_id=battle.allocation_id,
                err=str(exc),
            )
            return False
        try:
            await self.repo.touch_active(battle.match_id, now_ms())
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "allocation_uncertain_empty_tombstone_reindex_failed",
                match_id=battle.match_id,
                allocation_id=battle.allocation_id,
                err=str(exc),
            )
            return False
        return True

    async def reconcile_allocation_uncertain(  # noqa: C901, PLR0911, PLR0912 —— 与 Go 逐条对应
        self, battle: Any
    ) -> bool:
        """把一次结果未知的 GSA POST 变成**有界、重启安全**的终态取消 ——
        全程**绝不发第二次 POST**。对应 Go 的 `reconcileAllocationUncertain`。

        链路:allocation_id LIST → 权威空结果 或 恰一份 exact GS+Pod → 永久 exact
        release fence → 物理消失已确认 → durable ABANDONED → 生命周期 outbox。
        歧义 / API 未知的结果**原样保留**原有永久 fence 与 active 索引,交下一轮 sweep。

        ★ 能力探测失败(repo / resolver 不支持对账)是 **fail-closed**:打 Error 日志
          并返回 False,永久 fence 原样留着。降级成"按名字清理"就是 §9.22 明令禁止的
          "用一次 apiserver 抖动换来可以安全地当作没分配过的结论"。
        """
        if (
            battle is None
            or not self.model_b
            or self.auth_repo is None
            or self.authoritative_alloc is None
        ):
            return False
        repo_ok = isinstance(self.repo, AllocationUncertainRepo)
        resolver_ok = isinstance(
            self.authoritative_alloc, UncertainGameServerAllocationResolver
        )
        if not repo_ok or not resolver_ok:
            plog.get().error(
                "allocation_uncertain_reconciler_unavailable_fail_closed",
                match_id=battle.match_id,
                allocation_id=battle.allocation_id,
            )
            return False
        repo = self.repo
        resolver = self.authoritative_alloc

        if battle.state == STATE_ALLOCATION_UNCERTAIN:
            try:
                combat_faction_by_player = _combat_faction_map_from_records(
                    list(battle.player_ids), battle.player_combat_factions
                )
            except (ValueError, TypeError) as exc:
                plog.get().error(
                    "allocation_uncertain_combat_factions_invalid_fail_closed",
                    match_id=battle.match_id,
                    allocation_id=battle.allocation_id,
                    err=str(exc),
                )
                return False
            try:
                allocation, found = await resolver.resolve_allocation_by_id(
                    battle.match_id,
                    battle.allocation_id,
                    list(battle.player_ids),
                    combat_faction_by_player,
                    battle.map_id,
                    battle.game_mode,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "allocation_uncertain_resolve_failed_will_retry",
                    match_id=battle.match_id,
                    allocation_id=battle.allocation_id,
                    err=str(exc),
                )
                return False
            if not found:
                # 即使只读的空结果也要再跑一遍 DeleteCollection+LIST:这关掉的是
                # "超时的 POST 迟到生效"窗口 —— 必须在发布终态**之前**关掉。
                allocation = AuthoritativeGameServerAllocation(
                    allocation_id=battle.allocation_id
                )
                try:
                    await self.authoritative_alloc.release_expected(allocation)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    plog.get().warning(
                        "allocation_uncertain_empty_release_unconfirmed",
                        match_id=battle.match_id,
                        allocation_id=battle.allocation_id,
                        err=str(exc),
                    )
                    return False
                completed = False
                complete_err: BaseException | None = None
                try:
                    completed = await repo.complete_allocation_uncertain_release(
                        battle.match_id, battle.allocation_id, ""
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    complete_err = exc
                if complete_err is not None or not completed:
                    plog.get().warning(
                        "allocation_uncertain_empty_terminal_cas_failed",
                        match_id=battle.match_id,
                        allocation_id=battle.allocation_id,
                        completed=completed,
                        err=str(complete_err) if complete_err is not None else None,
                    )
                    return False
            else:
                fenced = False
                fence_err: BaseException | None = None
                try:
                    fenced = await repo.fence_allocation_uncertain_release(
                        battle.match_id, battle.allocation_id, allocation
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    fence_err = exc
                if fence_err is not None or not fenced:
                    plog.get().warning(
                        "allocation_uncertain_exact_release_fence_failed",
                        match_id=battle.match_id,
                        allocation_id=battle.allocation_id,
                        fenced=fenced,
                        err=str(fence_err) if fence_err is not None else None,
                    )
                    return False
                try:
                    await self.authoritative_alloc.release_expected(allocation)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    plog.get().warning(
                        "allocation_uncertain_exact_release_unconfirmed",
                        match_id=battle.match_id,
                        allocation_id=battle.allocation_id,
                        err=str(exc),
                    )
                    return False
                completed = False
                complete_err = None
                try:
                    completed = await repo.complete_allocation_uncertain_release(
                        battle.match_id, battle.allocation_id, allocation.instance_uid
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    complete_err = exc
                if complete_err is not None or not completed:
                    plog.get().warning(
                        "allocation_uncertain_exact_terminal_cas_failed",
                        match_id=battle.match_id,
                        allocation_id=battle.allocation_id,
                        completed=completed,
                        err=str(complete_err) if complete_err is not None else None,
                    )
                    return False
        elif battle.state == STATE_ALLOCATION_RECONCILING:
            if (
                battle.allocation_id == ""
                or battle.ds_pod_name == ""
                or battle.gameserver_uid == ""
                or battle.pod_uid == ""
                or battle.instance_epoch != 0
            ):
                return False
            allocation = AuthoritativeGameServerAllocation(
                pod_name=battle.ds_pod_name,
                instance_uid=battle.gameserver_uid,
                pod_uid=battle.pod_uid,
                allocation_id=battle.allocation_id,
                release_track=battle.release_track,
            )
            try:
                await self.authoritative_alloc.release_expected(allocation)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "allocation_uncertain_exact_release_resume_failed",
                    match_id=battle.match_id,
                    allocation_id=battle.allocation_id,
                    err=str(exc),
                )
                return False
            completed = False
            try:
                completed = await repo.complete_allocation_uncertain_release(
                    battle.match_id, battle.allocation_id, battle.gameserver_uid
                )
            except asyncio.CancelledError:
                raise
            except BaseException:  # noqa: BLE001 —— Go 这里不打日志,只 return false
                return False
            if not completed:
                return False
        else:
            return False

        try:
            terminal = await self.repo.get_battle(battle.match_id)
        except asyncio.CancelledError:
            raise
        except BaseException:  # noqa: BLE001 —— Go: `err != nil → return false`
            return False
        if terminal is None or terminal.state != STATE_ABANDONED:
            return False
        return await self.publish_reconciled_allocation_abandoned(terminal)

    # ── RPC 2:ReleaseBattle ─────────────────────────────────────────────

    async def release_battle(self, match_id: int, reason: str) -> None:
        """回收战斗 DS(legacy 正常结算路径)。对应 Go 的 `ReleaseBattle`。

        幂等:镜像不存在视为已释放,直接返回。

        Raises:
            errcode.PandoraError(ErrInvalidArg): match_id 缺失,或本副本已开 Model B
                (Model B **禁止** match_id-only 回收 —— 旧请求可能误杀同 match
                重建出的新 UID;正常结算必须走 `release_battle_expected`)。
            errcode.PandoraError(ErrUnavailable): 该 match 被另一个 Model-B writer
                永久 fence 住,legacy 副本只读跳过,等 sweep 对账。
        """
        if match_id == 0:
            plog.get().warning(
                "battle_release_refused",
                reason=REASON_ALLOC_MATCH_ID_REQUIRED,
                release_reason=reason,
                authority="legacy",
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "match_id required")
        if self.model_b:
            plog.get().warning(
                "battle_release_refused",
                reason=REASON_RELEASE_MODEL_B_UNSUPPORTED,
                match_id=match_id,
                release_reason=reason,
                hint=(
                    "有调用方还在用 match_id-only 回收;"
                    "Model B 必须走 ReleaseBattleExpected 的 expected tuple"
                ),
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle %d Model-B release requires terminal outbox expected tuple",
                match_id,
            )
        battle = await self.repo.get_battle(match_id)
        if battle is None:
            plog.get().debug("release_idempotent_miss", match_id=match_id, reason=reason)
            return
        if battle.state in (
            STATE_ALLOCATION_UNCERTAIN,
            STATE_ALLOCATION_RECONCILING,
            STATE_ALLOCATION_EMPTY_FENCE,
            STATE_PREACTIVE_RELEASING,
            STATE_ALLOCATION_ABORT,
        ):
            # 即使当前副本仍以 legacy 配置运行,也不能清理由另一个 Model-B writer
            # 写下的永久 POST fence。混跑期间最多返回不可用,绝不 Release/Delete。
            plog.get().warning(
                "battle_release_refused",
                reason=REASON_RELEASE_FENCED,
                match_id=match_id,
                release_reason=reason,
                state=battle.state,
                pod=battle.ds_pod_name,
                allocation_id=battle.allocation_id,
                hint=(
                    "该 match 被另一个 Model-B writer 永久 fence 住,"
                    "legacy 副本只读跳过,等 sweep 对账"
                ),
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d allocation/release result requires explicit reconciliation",
                match_id,
            )
        try:
            await self.alloc.release(battle.ds_pod_name)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 与 Go 同:失败只告警,继续删记录
            plog.get().warning(
                "gameserver_release_failed",
                match_id=match_id,
                pod=battle.ds_pod_name,
                err=str(exc),
            )
        # owner 精确释放(legacy 正常结算路径;INC-20260804-001 缺口⑦)。
        #
        # 与 abort / abandon 两条路完全同因:READY 交付前的强 Begin 已把这批玩家的
        # 归属写成 BATTLE/本实例,而本函数此前只做「杀 DS + 删对局记录」,**从不动
        # owner**。后果:对局正常结算后 owner 仍是 BATTLE/ADMITTED 指向一台刚被销毁
        # 的 DS,login 的 §9.23 query-first 一直把玩家指回去,而 match 记录已删 ——
        # 玩家打完副本回不了大厅。
        #
        # ★ 必须在 delete_battle **之前**取名单与身份 —— 删完就取不到了。
        await owner_release_abandoned_players_weak(
            self.owner_auth,
            list(battle.player_ids),
            battle.ds_pod_name,
            battle.gameserver_uid,
            OWNER_RELEASE_BUDGET_SEC,
        )
        await self.repo.delete_battle(match_id)
        # R1:对局记录被删除是不可逆状态推进,且与 battle_warming /
        # battle_ready_after_heartbeat 成对收尾。缺了它就无法证明"这局到底是正常
        # 回收的还是被 sweep 判弃的"。
        plog.get().info(
            "battle_released",
            match_id=match_id,
            pod=battle.ds_pod_name,
            reason=reason,
            state=battle.state,
            players=len(battle.player_ids),
            allocation_id=battle.allocation_id,
            authority="legacy",
        )

    # ── matchmaker 分配 saga 的补偿入口 ─────────────────────────────────

    async def abort_preactive_battle(  # noqa: C901, PLR0912 —— 与 Go 逐条对应
        self, request: battleabort.Request
    ) -> None:
        """准入前分配中止。对应 Go 的 `AbortPreactiveBattle`。

        只有**载荷已鉴权**的服务间 RPC 提供了 exact operation + DS 元组之后才补偿。
        Redis fence+journal 在触碰 Kubernetes **之前**提交;每个未知结果都可重试,
        且永久 journal 即使在有界的 battle/auth 记录过期之后仍能认出 ACK-loss 重放。

        Raises:
            errcode.PandoraError: 各阶段 fail-closed;`ErrUnavailable` 一律表示
                "同 request 幂等重试即可",不是终态失败。
        """
        if not request.complete():
            plog.get().warning(
                "battle_allocation_abort_refused",
                reason=REASON_ABORT_REQUEST_INCOMPLETE,
                match_id=request.match_id,
                operation_id=request.operation_id,
                pod=request.target.pod_name,
                uid=request.target.instance_uid,
                epoch=request.target.instance_epoch,
                allocation_id=request.target.allocation_id,
                release_track=request.target.release_track,
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "complete battle allocation abort request required"
            )
        if not self.model_b or self.abort_repo is None or self.authoritative_alloc is None:
            plog.get().warning(
                "battle_allocation_abort_refused",
                reason=REASON_ABORT_AUTHORITY_OFF,
                match_id=request.match_id,
                operation_id=request.operation_id,
                hint="本副本未启用 Redis 权威;matchmaker 的分配 saga 补偿只能等 sweep 兜底回收",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "battle allocation abort requires Redis Model-B authority",
            )

        # 在 legacy active 记录的 exact Pod UID 持久化之前,不得创建永久 ABORT fence。
        # battle/auth 已经消失时,交给下面的永久 abort journal,ACK-loss 重放照样可用。
        try:
            preflight = await self.auth_repo.read_authority(request.match_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            plog.get().warning(
                "battle_allocation_abort_refused",
                reason=REASON_ABORT_PREFLIGHT_FAILED,
                match_id=request.match_id,
                operation_id=request.operation_id,
                pod=request.target.pod_name,
                err=str(exc),
            )
            raise
        if preflight.battle_found:
            try:
                await self.ensure_durable_release_pod_uid(
                    request.match_id,
                    request.target.pod_name,
                    BattleExpectedInstance(
                        allocation_id=request.target.allocation_id,
                        instance_uid=request.target.instance_uid,
                        instance_epoch=request.target.instance_epoch,
                    ),
                    request.target.release_track,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                plog.get().warning(
                    "battle_allocation_abort_refused",
                    reason=REASON_ABORT_PREFLIGHT_FAILED,
                    match_id=request.match_id,
                    operation_id=request.operation_id,
                    pod=request.target.pod_name,
                    allocation_id=request.target.allocation_id,
                    err=str(exc),
                    hint=(
                        "pod_uid 未持久化前不得创建 ABORT fence;"
                        "详因见同 trace_id 的 battle_pod_uid_preflight_refused"
                    ),
                )
                raise
        try:
            fence = await self.abort_repo.fence_allocation_abort_expected(request)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            plog.get().warning(
                "battle_allocation_abort_refused",
                reason=REASON_ABORT_FENCE_FAILED,
                match_id=request.match_id,
                operation_id=request.operation_id,
                pod=request.target.pod_name,
                allocation_id=request.target.allocation_id,
                err=str(exc),
            )
            raise
        if fence.released:
            try:
                completed = await self.abort_repo.complete_allocation_abort_expected(
                    request, self.ds_credential_ttl_sec, self.battle_ttl_sec()
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                plog.get().warning(
                    "battle_allocation_abort_refused",
                    reason=REASON_ABORT_ACK_CLEANUP_PENDING,
                    match_id=request.match_id,
                    operation_id=request.operation_id,
                    err=str(exc),
                )
                raise
            if not completed:
                plog.get().warning(
                    "battle_allocation_abort_refused",
                    reason=REASON_ABORT_ACK_CLEANUP_PENDING,
                    match_id=request.match_id,
                    operation_id=request.operation_id,
                    hint="ACK-loss 重放已认出 RELEASED,但保留期清理未确认;调用方同 request 幂等重试",
                )
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "battle %d allocation abort ACK cleanup pending",
                    request.match_id,
                )
            return
        battle = fence.battle
        if (
            battle is None
            or battle.match_id != request.match_id
            or battle.ds_pod_name != request.target.pod_name
            or battle.pod_uid == ""
        ):
            plog.get().error(
                "battle_allocation_abort_refused",
                reason=REASON_ABORT_FENCE_IDENTITY_MISSING,
                match_id=request.match_id,
                operation_id=request.operation_id,
                want_pod=request.target.pod_name,
                got_pod=battle.ds_pod_name if battle is not None else "",
                got_pod_uid=_get_pod_uid(battle),
                allocation_id=request.target.allocation_id,
                hint="fence 已写却拿不到 exact pod 权威,不得发 K8s 删除;人工对账",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "battle %d allocation abort fence lacks exact pod authority",
                request.match_id,
            )
        allocation = AuthoritativeGameServerAllocation(
            pod_name=request.target.pod_name,
            instance_uid=request.target.instance_uid,
            pod_uid=battle.pod_uid,
            instance_epoch=request.target.instance_epoch,
            allocation_id=request.target.allocation_id,
            release_track=request.target.release_track,
        )
        await self.release_game_server(request.match_id, request.target.pod_name, allocation)
        if not await self.deliver_abandoned(
            request.match_id,
            request.target.pod_name,
            request.target.instance_uid,
            list(battle.player_ids),
            battle.map_id,
            battle.game_mode,
        ):
            plog.get().warning(
                "battle_allocation_abort_refused",
                reason=REASON_ABORT_LIFECYCLE_PENDING,
                match_id=request.match_id,
                operation_id=request.operation_id,
                pod=request.target.pod_name,
                players=len(battle.player_ids),
                hint="GameServer 已回收但 ABANDONED 补偿事件未投递,玩家段位回滚会延后;sweep 会重试",
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d allocation abort lifecycle publish pending",
                request.match_id,
            )
        # owner 精确释放(§9.23 取消/失败路径)。
        #
        # 判弃的这台实例正在被回收,但 READY 交付前的强 Begin 已经把这批玩家的归属
        # 写成 BATTLE/本实例。不释放的话 query-first 会一直把玩家指回一台死 DS:
        # 客户端 Travel 失败 → 重试 → 拿到同一个 TARGET → 再失败,正是验收底线第 1 条
        # 要防的「只能杀进程恢复」。
        await owner_release_abandoned_players_weak(
            self.owner_auth,
            list(battle.player_ids),
            request.target.pod_name,
            request.target.instance_uid,
            OWNER_RELEASE_BUDGET_SEC,
        )
        try:
            await self.lifecycle_proof_repo.record_allocation_lifecycle_published(
                request.match_id, request.target
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            plog.get().warning(
                "battle_allocation_abort_refused",
                reason=REASON_ABORT_LIFECYCLE_MARKER,
                match_id=request.match_id,
                operation_id=request.operation_id,
                allocation_id=request.target.allocation_id,
                err=str(exc),
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d allocation abort lifecycle marker pending",
                request.match_id,
                cause=exc,
            ) from exc
        try:
            completed = await self.abort_repo.complete_allocation_abort_expected(
                request, self.ds_credential_ttl_sec, self.battle_ttl_sec()
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            plog.get().warning(
                "battle_allocation_abort_refused",
                reason=REASON_ABORT_COMPLETION_PENDING,
                match_id=request.match_id,
                operation_id=request.operation_id,
                err=str(exc),
            )
            raise
        if not completed:
            plog.get().warning(
                "battle_allocation_abort_refused",
                reason=REASON_ABORT_COMPLETION_PENDING,
                match_id=request.match_id,
                operation_id=request.operation_id,
                allocation_id=request.target.allocation_id,
                hint="外部副作用已全部完成,仅永久日志置 RELEASED 未确认;同 request 幂等重试",
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d allocation abort completion pending",
                request.match_id,
            )
        plog.get().info(
            "battle_allocation_abort_completed",
            match_id=request.match_id,
            operation_id=request.operation_id,
            allocation_id=request.target.allocation_id,
        )

    # ── Model-B 正常结算回收(phase1 / phase2)───────────────────────────

    async def release_battle_expected(
        self,
        match_id: int,
        reason: str,
        pod_name: str,
        expected: BattleExpectedInstance,
        proof: BattleResultAuthorizationProof,
    ) -> None:
        """Model-B 正常结算 phase1 服务端回收入口。对应 Go 的 `ReleaseBattleExpected`。

        严格顺序:
          1. MySQL 持久 proof 与当前 Redis stable identity 做 terminal+receipt 原子 CAS;
          2. 用 Kubernetes GameServer UID delete precondition 回收。

        ★ 本方法**绝不恢复 Redis TTL**。battle_result 必须先把成功 durable CAS 为
          `released_at_ms`,再调用 `finalize_battle_release_expected`;DB ACK 长期
          失败时永久墓碑不会先消失。任一步 timeout/unknown 都抛错;pending outbox
          以同 tuple 幂等重试。
        """
        if not self.model_b or self.auth_repo is None or self.authoritative_alloc is None:
            plog.get().warning(
                "battle_release_refused",
                reason=REASON_RELEASE_AUTHORITY_OFF,
                match_id=match_id,
                pod=pod_name,
                release_reason=reason,
                hint="本副本未启用 Redis 权威/权威分配器,正常结算回收不可用;battle_result 会持续重试",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "battle terminal release requires Redis authority"
            )
        if (
            match_id == 0
            or reason != "completed"
            or pod_name == ""
            or proof.credential.pod_name != pod_name
            or proof.credential.instance_uid != expected.instance_uid
            or proof.credential.instance_epoch != expected.instance_epoch
        ):
            # 逐项带出"哪一项对不上":这是 battle_result outbox 与 allocator 权威之间
            # 唯一的身份握手,对不上就永远收不了 DS(pod 泄漏),但错误码只是 InvalidArg。
            plog.get().warning(
                "battle_release_refused",
                reason=REASON_RELEASE_PROOF_INCOMPLETE,
                match_id=match_id,
                pod=pod_name,
                release_reason=reason,
                want_pod=pod_name,
                got_proof_pod=proof.credential.pod_name,
                want_uid=expected.instance_uid,
                got_proof_uid=proof.credential.instance_uid,
                want_epoch=expected.instance_epoch,
                got_proof_epoch=proof.credential.instance_epoch,
                allocation_id=expected.allocation_id,
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "battle terminal release proof is incomplete"
            )
        # 滚动升级门:pod_uid 是第一批 Model-B 记录之后才加的字段,必须在终态 CAS
        # 之前 durable 存在。legacy 记录只允许从 exact K8s GameServer/allocation/
        # owned-Pod 读回填;缺失或同名替换对象保持可重试,**不创建**永久 TERMINATING fence。
        await self.ensure_durable_release_pod_uid(match_id, pod_name, expected, "")
        terminated = await self.auth_repo.terminate_result_expected(match_id, expected, proof)
        if not terminated:
            plog.get().warning(
                "battle_release_refused",
                reason=REASON_RELEASE_IDENTITY_CHANGED,
                match_id=match_id,
                pod=pod_name,
                uid=expected.instance_uid,
                epoch=expected.instance_epoch,
                allocation_id=expected.allocation_id,
                hint="终态 CAS 未命中:该 match 的 stable 身份已变(同名 Pod 重建/已被判弃/已回收)",
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "battle %d stable identity changed before terminal release",
                match_id,
            )
        try:
            snapshot = await self.auth_repo.read_authority(match_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            plog.get().warning(
                "battle_release_refused",
                reason=REASON_RELEASE_AUTHORITY_REREAD,
                match_id=match_id,
                pod=pod_name,
                allocation_id=expected.allocation_id,
                err=str(exc),
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d terminal authority reread failed",
                match_id,
                cause=exc,
            ) from exc
        if not exact_terminated_release_snapshot(snapshot, match_id, pod_name, expected):
            plog.get().warning(
                "battle_release_refused",
                reason=REASON_RELEASE_SNAPSHOT_STALE,
                match_id=match_id,
                pod=pod_name,
                uid=expected.instance_uid,
                epoch=expected.instance_epoch,
                allocation_id=expected.allocation_id,
                auth_found=snapshot.auth_found,
                battle_found=snapshot.battle_found,
                state=_get_state(snapshot.battle),
                phase=enum_name(
                    dspb.BattleAuthPhase,
                    snapshot.auth.phase if snapshot.auth is not None else 0,
                ),
                hint="终态 CAS 已成功但重读不自洽,本轮不碰 K8s;outbox 同 tuple 幂等重试",
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d terminal authority snapshot is incomplete or changed",
                match_id,
            )
        allocation = AuthoritativeGameServerAllocation(
            pod_name=pod_name,
            instance_uid=expected.instance_uid,
            instance_epoch=expected.instance_epoch,
            allocation_id=expected.allocation_id,
            pod_uid=snapshot.battle.pod_uid,
            release_track=snapshot.battle.release_track,
        )
        try:
            await self.release_game_server(match_id, pod_name, allocation)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            plog.get().warning(
                "terminal_gameserver_release_unconfirmed",
                match_id=match_id,
                allocation_id=expected.allocation_id,
                pod=pod_name,
                err=str(exc),
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d terminal gameserver release not confirmed",
                match_id,
                cause=exc,
            ) from exc
        # owner 精确释放(Model B 正常结算收口;INC-20260804-001 缺口⑦-B)。
        #
        # 时序:必须在 release_game_server **成功之后**。该调用是 K8s UID precondition
        # 删除,返回即「本实例回收已确认」,满足安全边界①(提前释放会在旧 DS 可能仍在
        # 跑时放行新归属 = 双 DS)。
        #
        # 弱依赖:失败只告警,不改变本函数的返回。phase-1 已完成的事实(Redis 终态
        # CAS + GameServer 已删)不能因为 owner 抖动而回退成"未完成",否则
        # battle_result 的 outbox 会重放一次已经删过 GameServer 的回收。
        await owner_release_abandoned_players_weak(
            self.owner_auth,
            list(snapshot.battle.player_ids),
            pod_name,
            expected.instance_uid,
            OWNER_RELEASE_BUDGET_SEC,
        )
        plog.get().info(
            "battle_terminal_release_phase1_completed",
            match_id=match_id,
            allocation_id=expected.allocation_id,
            pod=pod_name,
            uid=expected.instance_uid,
        )

    async def ensure_durable_release_pod_uid(  # noqa: C901 —— 与 Go 逐条对应
        self,
        match_id: int,
        pod_name: str,
        expected: BattleExpectedInstance,
        expected_release_track: str,
    ) -> str:
        """所有精确回收(结算 / 判弃 / abort)的共同前提:把 exact Pod UID 落到权威记录。
        对应 Go 的 `ensureDurableReleasePodUID`。

        Returns:
            该实例的 Pod UID(非空)。

        Raises:
            errcode.PandoraError(ErrUnavailable): 权威读失败 / K8s 精确回读不到 /
                回填或回填校验失败。**保持可重试,不创建永久 TERMINATING fence。**
            errcode.PandoraError(ErrDSAllocationFailed): exact 身份已变(同名 Pod
                重建,或记录已被推进到别的实例)。

        ★ 回填之后还要**再读一次权威**核对(`verified`):CAS 成功不等于读得到 ——
          中间可能有另一条链把记录推进走了,而我们即将拿这个 pod_uid 去发 DELETE。
        """
        try:
            snapshot = await self.auth_repo.read_authority(match_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            plog.get().warning(
                "battle_pod_uid_preflight_refused",
                reason=REASON_POD_UID_AUTHORITY_READ,
                match_id=match_id,
                pod=pod_name,
                allocation_id=expected.allocation_id,
                err=str(exc),
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d release preflight authority read failed",
                match_id,
                cause=exc,
            ) from exc
        if not exact_release_identity_snapshot(
            snapshot, match_id, pod_name, expected, expected_release_track
        ):
            # 带出期望与实际,才能一眼分出是"同名 Pod 重建"还是"记录已被推进到别的实例"。
            b = snapshot.battle
            plog.get().warning(
                "battle_pod_uid_preflight_refused",
                reason=REASON_POD_UID_IDENTITY_CHANGED,
                match_id=match_id,
                want_pod=pod_name,
                got_pod=b.ds_pod_name if b is not None else "",
                want_uid=expected.instance_uid,
                got_uid=b.gameserver_uid if b is not None else "",
                want_epoch=expected.instance_epoch,
                got_epoch=b.instance_epoch if b is not None else 0,
                want_allocation_id=expected.allocation_id,
                got_allocation_id=b.allocation_id if b is not None else "",
                want_release_track=expected_release_track,
                got_release_track=b.release_track if b is not None else "",
                battle_found=snapshot.battle_found,
                state=_get_state(b),
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "battle %d release preflight identity changed",
                match_id,
            )
        if snapshot.battle.pod_uid != "":
            return snapshot.battle.pod_uid
        pod_uid = ""
        resolve_err: BaseException | None = None
        try:
            pod_uid = await self.authoritative_alloc.resolve_expected_pod_uid(
                AuthoritativeGameServerAllocation(
                    pod_name=pod_name,
                    instance_uid=expected.instance_uid,
                    instance_epoch=expected.instance_epoch,
                    allocation_id=expected.allocation_id,
                )
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            resolve_err = exc
        if resolve_err is not None or pod_uid == "":
            plog.get().warning(
                "battle_pod_uid_preflight_refused",
                reason=REASON_POD_UID_RESOLVE_FAILED,
                match_id=match_id,
                pod=pod_name,
                uid=expected.instance_uid,
                epoch=expected.instance_epoch,
                allocation_id=expected.allocation_id,
                resolved_pod_uid=pod_uid,
                err=str(resolve_err) if resolve_err is not None else None,
                hint=(
                    "legacy 记录缺 pod_uid 且从 K8s 精确回读不到;"
                    "回收保持可重试,不创建永久 TERMINATING fence"
                ),
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d legacy pod UID exact preflight failed",
                match_id,
                cause=resolve_err,
            )

        def _backfill(battle: Any) -> None:
            """`update_battle_keep_ttl` 的乐观锁回调(CAS 冲突时**整体重跑**)。

            ★ 无副作用:只改传入的记录,不写任何调用方捕获的变量 —— 重跑契约要求如此。
            """
            if not exact_release_battle_identity(
                battle, match_id, pod_name, expected, expected_release_track
            ):
                raise errcode.PandoraError(
                    errcode.ErrDSAllocationFailed,
                    "battle %d changed during pod UID backfill",
                    match_id,
                )
            if battle.pod_uid == "":
                battle.pod_uid = pod_uid
            elif battle.pod_uid != pod_uid:
                raise errcode.PandoraError(
                    errcode.ErrDSAllocationFailed,
                    "battle %d pod UID changed during backfill",
                    match_id,
                )

        try:
            await self.repo.update_battle_keep_ttl(match_id, UPDATE_MAX_RETRY, _backfill)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            plog.get().warning(
                "battle_pod_uid_preflight_refused",
                reason=REASON_POD_UID_BACKFILL_FAILED,
                match_id=match_id,
                pod=pod_name,
                pod_uid=pod_uid,
                allocation_id=expected.allocation_id,
                err=str(exc),
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d legacy pod UID durable backfill failed",
                match_id,
                cause=exc,
            ) from exc
        verified: BattleAuthoritySnapshot | None = None
        verify_err: BaseException | None = None
        try:
            verified = await self.auth_repo.read_authority(match_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            verify_err = exc
        if (
            verify_err is not None
            or not exact_release_identity_snapshot(
                verified, match_id, pod_name, expected, expected_release_track
            )
            or _get_pod_uid(verified.battle if verified is not None else None) != pod_uid
        ):
            plog.get().warning(
                "battle_pod_uid_preflight_refused",
                reason=REASON_POD_UID_VERIFY_FAILED,
                match_id=match_id,
                pod=pod_name,
                want_pod_uid=pod_uid,
                got_pod_uid=_get_pod_uid(verified.battle if verified is not None else None),
                allocation_id=expected.allocation_id,
                err=str(verify_err) if verify_err is not None else None,
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d legacy pod UID backfill verification failed",
                match_id,
                cause=verify_err,
            )
        # R4:本函数在 sweep 每轮、每次心跳前置都可能被调用,成功侧只能 debug。
        # 但"回填过一次 pod_uid"是 legacy 记录被补齐的证据,查回收链时要看得到。
        plog.get().debug(
            "battle_pod_uid_backfilled",
            match_id=match_id,
            pod=pod_name,
            pod_uid=pod_uid,
            allocation_id=expected.allocation_id,
        )
        return pod_uid

    async def finalize_battle_release_expected(
        self,
        match_id: int,
        pod_name: str,
        expected: BattleExpectedInstance,
        proof: BattleResultAuthorizationProof,
    ) -> None:
        """durable `released_at_ms` 之后的 phase2。对应 Go 的 `FinalizeBattleReleaseExpected`。

        它只校验同一 proof 的 Redis terminal/receipt 墓碑并**恢复 TTL**,
        **绝不调用 Kubernetes**。若上一次 finalize 响应丢失且 TTL 已把三键全部清空,
        则按幂等成功返回(该判定在 `expire_result_terminated_expected` 内部)。
        """
        if not self.model_b or self.auth_repo is None:
            plog.get().warning(
                "battle_release_refused",
                reason=REASON_RELEASE_AUTHORITY_OFF,
                match_id=match_id,
                pod=pod_name,
                release_reason="completed-finalize",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "battle terminal finalize requires Redis authority"
            )
        if (
            match_id == 0
            or pod_name == ""
            or proof.credential.pod_name != pod_name
            or proof.credential.instance_uid != expected.instance_uid
            or proof.credential.instance_epoch != expected.instance_epoch
        ):
            plog.get().warning(
                "battle_release_refused",
                reason=REASON_FINALIZE_PROOF_INCOMPLETE,
                match_id=match_id,
                pod=pod_name,
                want_uid=expected.instance_uid,
                got_proof_uid=proof.credential.instance_uid,
                want_epoch=expected.instance_epoch,
                got_proof_epoch=proof.credential.instance_epoch,
                allocation_id=expected.allocation_id,
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "battle terminal finalize proof is incomplete"
            )
        try:
            expired = await self.auth_repo.expire_result_terminated_expected(
                match_id, expected, proof, self.battle_ttl_sec()
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            plog.get().warning(
                "battle_release_refused",
                reason=REASON_FINALIZE_TOMBSTONE_PENDING,
                match_id=match_id,
                pod=pod_name,
                allocation_id=expected.allocation_id,
                err=str(exc),
            )
            raise
        if not expired:
            plog.get().warning(
                "battle_release_refused",
                reason=REASON_FINALIZE_TOMBSTONE_PENDING,
                match_id=match_id,
                pod=pod_name,
                uid=expected.instance_uid,
                epoch=expected.instance_epoch,
                allocation_id=expected.allocation_id,
                hint="永久墓碑未能恢复有界 TTL(身份不匹配或已被推进);outbox 会同 tuple 重试",
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d terminal tombstone retention not confirmed",
                match_id,
            )
        plog.get().info(
            "battle_terminal_release_finalized",
            match_id=match_id,
            allocation_id=expected.allocation_id,
            pod=pod_name,
            uid=expected.instance_uid,
        )
