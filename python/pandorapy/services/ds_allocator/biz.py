"""ds_allocator 业务层**组装点** —— 对应 Go 侧
`services/battle/ds_allocator/internal/biz/allocator.go` 的整体(4048 行)。

Go 那边一个文件一个 `AllocatorUsecase`;Python 侧按**失败模式**切成五块分批移植,
在这里合体:

    biz_base.py       AllocatorUsecaseBase  字段 / 构造 / setter / accessor + 只读重连查询
    biz_allocate.py   AllocateMixin         分配主链(claim → GSA → 凭据投递 → 等 ready)
    biz_release.py    ReleaseMixin          回收 / 释放 / abort saga / 未知结果对账
    biz_heartbeat.py  HeartbeatMixin        DS 心跳 / 激活 / census / 位置续期
    biz_sweep.py      SweepMixin            心跳超时清扫 / 单写者 leader / 索引对账 / abandoned 投递

上层(service / main)**只 import 本模块**:

    from pandorapy.services.ds_allocator import biz
    uc = biz.AllocatorUsecase(repo, alloc, cfg)

═══════════════════════════════════════════════════════════════════════════════
MRO 顺序不是风格问题
═══════════════════════════════════════════════════════════════════════════════

    class AllocatorUsecase(SweepMixin, HeartbeatMixin, ReleaseMixin,
                           AllocateMixin, AllocatorUsecaseBase)

Python 的方法解析按**声明顺序从左到右**,先找到谁就用谁。这里的顺序编码了一条
事实:**同名方法只允许存在一份**,顺序只是"谁负责实现"的声明,不是"谁覆盖谁"。

  - `AllocatorUsecaseBase` 必须**最右**:它持有 `__init__` 与全部字段,四个 mixin
    都假设这些字段已存在。放前面会让构造链断在 mixin 上。
  - `SweepMixin` 最左:它是后台唯一驱动者,`sweep_once` 会调到其余三批
    (`reconcile_allocation_uncertain` / `reconcile_preactive_release` /
    `ensure_durable_release_pod_uid` / `release_game_server` / `abort_preactive_battle`
    / `kill_stranded_ds`)。放最左让读者一眼确认:后台链是入口,不是被调方。
  - `ReleaseMixin` 在 `AllocateMixin` 之前:分配失败的清理走回收链,而不是反过来。

`tests/test_ds_allocator_biz_sweep.py` 对 `__mro__` 做了断言 —— 这五个类的相对顺序
被改动时必须有人看见。

═══════════════════════════════════════════════════════════════════════════════
re-export 的边界
═══════════════════════════════════════════════════════════════════════════════

只 re-export **service / main 层真正需要**的名字:出参结构、注入用 Protocol、会出现在
日志 / 错误分流里的常量、以及少量无 receiver 的纯函数。

分配链与回收链的内部细节(claim 拼装、凭据投递 annotation 的读写、fence 状态机的
中间判据)**不**外露 —— 外露就会被上层拿去重新实现一遍判定,而那正是 §9.6
「派生判定只有一份」要防的事。
"""

from __future__ import annotations

from pandorapy.services.ds_allocator.biz_allocate import (
    OWNER_BEGIN_BUDGET_SEC,
    OWNER_VERIFY_BUDGET_SEC,
    AllocateMixin,
    battle_pending_delivered,
    battle_ready_for_pod,
    combat_faction_map_from_records,
    combat_faction_records,
    same_battle_allocation_request,
)
from pandorapy.services.ds_allocator.biz_base import (
    ACTIVE_INDEX_RECONCILE_INTERVAL_SEC,
    COMMAND_NONE,
    COMMAND_STOP,
    DETACHED_CLEANUP_TIMEOUT_SEC,
    LOCATION_REFRESH_TIMEOUT_SEC,
    REASON_ABORT_ACK_CLEANUP_PENDING,
    REASON_ABORT_AUTHORITY_OFF,
    REASON_ABORT_COMPLETION_PENDING,
    REASON_ABORT_FENCE_FAILED,
    REASON_ABORT_FENCE_IDENTITY_MISSING,
    REASON_ABORT_LIFECYCLE_MARKER,
    REASON_ABORT_LIFECYCLE_PENDING,
    REASON_ABORT_PREFLIGHT_FAILED,
    REASON_ABORT_REQUEST_INCOMPLETE,
    REASON_ACTIVE_INDEX_REBUILD,
    REASON_ACTIVE_INDEX_UNAVAILABLE,
    REASON_ALLOC_CLAIM_FAILED,
    REASON_ALLOC_FACTIONS_INVALID,
    REASON_ALLOC_FINALIZE_LOST,
    REASON_ALLOC_MATCH_ID_REQUIRED,
    REASON_ALLOC_ROSTER_INVALID,
    REASON_ALLOC_TRACK_INVALID,
    REASON_AWAIT_CLAIM_MISSING,
    REASON_AWAIT_RELEASE_UNCONFIRMED,
    REASON_FINALIZE_PROOF_INCOMPLETE,
    REASON_FINALIZE_TOMBSTONE_PENDING,
    REASON_HEARTBEAT_ACTIVATE_REJECTED,
    REASON_HEARTBEAT_AUTHORITY_READ,
    REASON_HEARTBEAT_CENSUS_INCONSISTENT,
    REASON_HEARTBEAT_DEPARTURE_RECONCILE,
    REASON_HEARTBEAT_MODEL_B_OFF,
    REASON_HEARTBEAT_OWNER_LEASE,
    REASON_HEARTBEAT_UPDATE_FAILED,
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
    REASON_RESOLVE_ARGS_INVALID,
    REASON_RESOLVE_AUTHORITY_OFF,
    REASON_RESOLVE_AUTHORITY_READ,
    REASON_RESOLVE_NOT_AUTHORIZED,
    REASON_RESOLVE_PROJECTION_INCOMPLETE,
    REASON_STOP_BATTLE_ABANDONED,
    REASON_STOP_BATTLE_ENDED,
    REASON_STOP_BATTLE_MISSING,
    REASON_STOP_TERMINAL_AUTH,
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
    AllocatorUsecaseBase,
    AuthoritativeGameServerAllocator,
    BattleActiveIndexReconciler,
    BattleAllocationAbortRepo,
    BattleAllocationLedger,
    BattleAllocationLifecycleRepo,
    BattleCredentialSigner,
    BattleWaitOwnershipLostError,
    DSLifecyclePusher,
    HeartbeatAllocationFencedError,
    HeartbeatPodMismatchError,
    HeartbeatTerminalError,
    LocationRefresher,
    NoShowRecorder,
    OrphanGameServerReconciler,
    ReadyWaitTimeoutError,
    StrictModelBBattleStorage,
    SweepWriterLease,
    allocate_result_from_battle,
    owner_target_from_allocate_result,
)

# ★ `biz_heartbeat.py` 由同批另一位产出。这里**照常 import**:缺它时本模块 import 即
#   ImportError,而不是悄悄组装出一个没有心跳能力的 `AllocatorUsecase`(那种半成品会
#   在第一次 DS 心跳时才以 AttributeError 暴露,且现场只剩一个方法名)。
from pandorapy.services.ds_allocator.biz_heartbeat import HeartbeatMixin
from pandorapy.services.ds_allocator.biz_release import (
    OWNER_RELEASE_BUDGET_SEC,
    AllocationUncertainRepo,
    PreactiveReleaseOutcome,
    ReleaseMixin,
    exact_release_battle_identity,
    exact_release_identity_snapshot,
    exact_terminated_release_snapshot,
    release_state_allows_pod_uid_backfill,
)
from pandorapy.services.ds_allocator.biz_sweep import (
    ACTIVE_INDEX_RECONCILE_SCAN_COUNT,
    DEFAULT_SWEEP_DEFER_BACKOFF_SEC,
    DEFAULT_SWEEP_ROUND_BUDGET_SEC,
    RELEASE_PENDING_DEFER_PREFIX,
    SWEEP_TASK_NAME,
    WARMING_PROBE_DEFER_PREFIX,
    SweepDeferral,
    SweepMixin,
    roster_gate_armable,
    stuck_reconcile_state,
)


class AllocatorUsecase(SweepMixin, HeartbeatMixin, ReleaseMixin, AllocateMixin, AllocatorUsecaseBase):
    """ds_allocator 的完整业务层。对应 Go 的 `biz.AllocatorUsecase`。

    构造与注入全部继承自 `AllocatorUsecaseBase`(`__init__(repo, alloc, cfg)` +
    `set_*` 系列 + `enable_redis_authority`),本类**刻意为空**:任何在这里补的方法
    都会绕过"某个 mixin 才是该职责唯一实现"的约定,并在 MRO 里制造一份找不到来源
    的覆盖 —— 而覆盖了谁、被谁覆盖,在 Python 里不会有任何提示。
    """


__all__ = [
    # ── 组装体 ────────────────────────────────────────────────────────────
    "AllocateMixin",
    "AllocatorUsecase",
    "AllocatorUsecaseBase",
    "HeartbeatMixin",
    "ReleaseMixin",
    "SweepMixin",
    # ── 注入用 Protocol(main 装配期按能力探测)────────────────────────────
    "AllocationUncertainRepo",
    "AuthoritativeGameServerAllocator",
    "BattleActiveIndexReconciler",
    "BattleAllocationAbortRepo",
    "BattleAllocationLedger",
    "BattleAllocationLifecycleRepo",
    "BattleCredentialSigner",
    "DSLifecyclePusher",
    "LocationRefresher",
    "NoShowRecorder",
    "OrphanGameServerReconciler",
    "StrictModelBBattleStorage",
    "SweepWriterLease",
    # ── 出参 / 控制流哨兵 ─────────────────────────────────────────────────
    "AllocateResult",
    "BattleWaitOwnershipLostError",
    "HeartbeatAllocationFencedError",
    "HeartbeatPodMismatchError",
    "HeartbeatTerminalError",
    "PreactiveReleaseOutcome",
    "ReadyWaitTimeoutError",
    "SweepDeferral",
    # ── 对局状态 / 心跳指令 ───────────────────────────────────────────────
    "COMMAND_NONE",
    "COMMAND_STOP",
    "STATE_ABANDONED",
    "STATE_ALLOCATING",
    "STATE_ALLOCATION_ABORT",
    "STATE_ALLOCATION_EMPTY_FENCE",
    "STATE_ALLOCATION_RECONCILING",
    "STATE_ALLOCATION_UNCERTAIN",
    "STATE_ENDED",
    "STATE_PREACTIVE_RELEASING",
    "STATE_READY",
    "STATE_RUNNING",
    "STATE_WARMING",
    # ── 结构化拒绝原因(日志 / 告警按此分流,禁止在上层另写字面量)────────
    "REASON_ABORT_ACK_CLEANUP_PENDING",
    "REASON_ABORT_AUTHORITY_OFF",
    "REASON_ABORT_COMPLETION_PENDING",
    "REASON_ABORT_FENCE_FAILED",
    "REASON_ABORT_FENCE_IDENTITY_MISSING",
    "REASON_ABORT_LIFECYCLE_MARKER",
    "REASON_ABORT_LIFECYCLE_PENDING",
    "REASON_ABORT_PREFLIGHT_FAILED",
    "REASON_ABORT_REQUEST_INCOMPLETE",
    "REASON_ACTIVE_INDEX_REBUILD",
    "REASON_ACTIVE_INDEX_UNAVAILABLE",
    "REASON_ALLOC_CLAIM_FAILED",
    "REASON_ALLOC_FACTIONS_INVALID",
    "REASON_ALLOC_FINALIZE_LOST",
    "REASON_ALLOC_MATCH_ID_REQUIRED",
    "REASON_ALLOC_ROSTER_INVALID",
    "REASON_ALLOC_TRACK_INVALID",
    "REASON_AWAIT_CLAIM_MISSING",
    "REASON_AWAIT_RELEASE_UNCONFIRMED",
    "REASON_FINALIZE_PROOF_INCOMPLETE",
    "REASON_FINALIZE_TOMBSTONE_PENDING",
    "REASON_HEARTBEAT_ACTIVATE_REJECTED",
    "REASON_HEARTBEAT_AUTHORITY_READ",
    "REASON_HEARTBEAT_CENSUS_INCONSISTENT",
    "REASON_HEARTBEAT_DEPARTURE_RECONCILE",
    "REASON_HEARTBEAT_MODEL_B_OFF",
    "REASON_HEARTBEAT_OWNER_LEASE",
    "REASON_HEARTBEAT_UPDATE_FAILED",
    "REASON_POD_UID_AUTHORITY_READ",
    "REASON_POD_UID_BACKFILL_FAILED",
    "REASON_POD_UID_IDENTITY_CHANGED",
    "REASON_POD_UID_RESOLVE_FAILED",
    "REASON_POD_UID_VERIFY_FAILED",
    "REASON_RELEASE_AUTHORITY_OFF",
    "REASON_RELEASE_AUTHORITY_REREAD",
    "REASON_RELEASE_FENCED",
    "REASON_RELEASE_IDENTITY_CHANGED",
    "REASON_RELEASE_MODEL_B_UNSUPPORTED",
    "REASON_RELEASE_POD_UID_MISSING",
    "REASON_RELEASE_PROOF_INCOMPLETE",
    "REASON_RELEASE_SNAPSHOT_STALE",
    "REASON_RELEASE_TUPLE_INCOMPLETE",
    "REASON_RESOLVE_ARGS_INVALID",
    "REASON_RESOLVE_AUTHORITY_OFF",
    "REASON_RESOLVE_AUTHORITY_READ",
    "REASON_RESOLVE_NOT_AUTHORIZED",
    "REASON_RESOLVE_PROJECTION_INCOMPLETE",
    "REASON_STOP_BATTLE_ABANDONED",
    "REASON_STOP_BATTLE_ENDED",
    "REASON_STOP_BATTLE_MISSING",
    "REASON_STOP_TERMINAL_AUTH",
    "REASON_WAIT_ALLOC_SUPERSEDED",
    "REASON_WAIT_AUTH_OUTSIDE_GRACE",
    "REASON_WAIT_AUTH_PHASE_FENCED",
    "REASON_WAIT_AUTH_PROVISION_DEAD",
    "REASON_WAIT_AUTHORITY_PURGED",
    "REASON_WAIT_CTX_DONE",
    "REASON_WAIT_RECORD_GONE",
    "REASON_WAIT_STATE_RECLAIMED",
    # ── 机械上限 / 超时预算 / 点位名(压测与告警阈值引用同一份)────────────
    "ACTIVE_INDEX_RECONCILE_INTERVAL_SEC",
    "ACTIVE_INDEX_RECONCILE_SCAN_COUNT",
    "DEFAULT_SWEEP_DEFER_BACKOFF_SEC",
    "DEFAULT_SWEEP_ROUND_BUDGET_SEC",
    "DETACHED_CLEANUP_TIMEOUT_SEC",
    "LOCATION_REFRESH_TIMEOUT_SEC",
    "OWNER_BEGIN_BUDGET_SEC",
    "OWNER_RELEASE_BUDGET_SEC",
    "OWNER_VERIFY_BUDGET_SEC",
    "RELEASE_PENDING_DEFER_PREFIX",
    "SWEEP_TASK_NAME",
    "UPDATE_MAX_RETRY",
    "WARMING_PROBE_DEFER_PREFIX",
    # ── 无 receiver 的纯函数(service 层组装出参 / 校验入参时复用)────────
    "allocate_result_from_battle",
    "battle_pending_delivered",
    "battle_ready_for_pod",
    "combat_faction_map_from_records",
    "combat_faction_records",
    "exact_release_battle_identity",
    "exact_release_identity_snapshot",
    "exact_terminated_release_snapshot",
    "owner_target_from_allocate_result",
    "release_state_allows_pod_uid_backfill",
    "roster_gate_armable",
    "same_battle_allocation_request",
    "stuck_reconcile_state",
]
