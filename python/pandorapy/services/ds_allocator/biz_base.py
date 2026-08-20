"""ds_allocator 业务层**基座** —— 对应 Go 侧
`services/battle/ds_allocator/internal/biz/allocator.go` 第 1–546 行。

本模块只放「哨兵错误 + 常量词表 + 弱依赖协议 + 字段 / 构造 / setter / accessor +
两个模块级投影函数 + 只读重连查询」,不含分配主链、回收、心跳、扫描:那四批分别由

    class AllocatorUsecase(SweepMixin, HeartbeatMixin, ReleaseMixin,
                           AllocateMixin, AllocatorUsecaseBase): ...

组装(与 `hub_allocator` 同构)。拆开的理由不是"文件太长",而是 Go 那 3000+ 行里
四类关注点的失败模式完全不同(基座错 = 全域行为漂移;分配错 = 玩家进不去副本;
回收错 = Pod 泄漏 / 误杀在场对局;扫描错 = §9 不变量 4 的补偿链静默停摆),
放一起时任何一处变异实验都要重跑整份测试。

## 常量为什么必须逐字抄

`REASON_*` / `STATE_*` / `COMMAND_*` 这些串**不是给人看的自由文本**:

  - `STATE_*` 是 Redis 里 `BattleStorageRecord.state` 的 **wire 值**(proto 声明为
    `string`)。两栈并存期同一份记录可能被 Go 写、被 Python 读,错一个字母的表现
    不是报错,而是 `active_index_required()` 抛 unknown state / `battle_terminal()`
    恒 false —— 一局永远没人推进,或者已终结的局被反复补偿。
  - `COMMAND_STOP` 是发给 UE DS 的**控制指令字面量**,DS 侧按字符串比对。
  - `REASON_*` 是运维照着建 Loki / Grafana 查询的固定词表(infra.md §11.3 R2)。
    一半请求走 Go、一半走 Python 时,任一侧改一个字母,面板就只统计到另一半的量 ——
    而"少了一半"这件事本身没有任何告警。

所以 `STATE_ALLOCATION_*` 五项**从 `repo.py` import**而不是在这里再抄一遍:那边是
`battle_storage_invariant` 的写入侧判据处,两份副本漂移时两边都不报错。

## 与 Go 的必要形变(其余逐行同构)

  1. **Duration 一律以「秒」为单位并在名字里写出来**(`battle_ttl_sec()`)。Go 是
     `time.Duration` 强类型,传错单位编译不过;Python 传 float 谁也拦不住,而
     `repo` / `auth_repo` 全套接口收的都是秒。写进名字是这里唯一不依赖人眼的防线。
     唯一例外是 `heartbeat_timeout_ms()` —— 它喂给 `ready_authorized()` 的入参
     本来就是毫秒(权威快照里的时间戳全是 unix ms),换成秒反而要在调用点乘 1000。
  2. **`(值, error)` 一律改成「返回值 + 抛异常」**。Go 的 `ResolveBattleTarget`
     失败时**同时**返回 `nil` 和 err,调用方只看 err;Python 抛出即可。
  3. **Go 的哨兵 `error` 变成异常类型**。`errHeartbeatTerminal` 那五个哨兵在 Go 里
     是从乐观锁回调 `return` 出来、再用 `errors.Is` 比对的值;Python 侧做成异常类,
     mixin 里 `raise` / `except` 即可,语义与 `errors.Is` 一致(且不会被误当成
     "正常返回值"往下传)。
  4. **`u == nil` 的接收者判空删掉**。Go 的 `ResolveBattleTarget` 首行判了
     `u == nil`(Go 允许 nil 指针调用方法);Python 里 `self` 不可能是 None,
     保留那条判断只会写出一行永远为假的死代码(§15.5)。其余两个条件
     (`!u.modelB` / `u.authRepo == nil`)逐字保留。

## 本模块**没有**移植的东西(移植 mixin 的人必须自己接)

  - `AllocateBattleWithCombatFactions`(Go 546 行以下)→ `biz_allocate.py`;
    本模块的 `allocate_battle()` 只是那条链的旧口径薄封装,直接调用 mixin 方法。
  - `SetSweepWriterLease` / `sweep_is_leader`(Go 3352 行附近)→ `biz_sweep.py`;
    基座只声明 `sweep_lease` / `sweep_lease_held` 两个字段(Go 结构体里就有)。
  - `SetOwnerLeaseRenewer` / `SetOwnerAuthority` 在 Go 里定义于
    `owner_lease.go` / `owner_authority.go`,但**只写基座字段**,故随基座一起提供
    (与 `hub_allocator/biz_base.py` 的处理一致)。
"""

from __future__ import annotations

import asyncio
import datetime as _dt
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from pandora.config.v1 import level_pb2 as configpb
from pandora.ds.v1 import allocator_pb2 as dspb

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import releasetrack, safego
from pandorapy.services.ds_allocator import repo as R

# ★ `now_ms` **复用** battle_auth 的那一个,不在这里再写一份 `int(time.time()*1000)`:
#   它是 `ready_authorized()` 新鲜度判据的时间源,两份实现漂移(比如一份用墙钟一份
#   用单调钟)时两边都不报错,只会让重连在边界上时灵时不灵。
#   测试要冻结时间时 monkeypatch **本模块**的 `now_ms`(调用点按模块属性解析)。
from pandorapy.services.ds_allocator.battle_auth import now_ms
from pandorapy.services.ds_allocator.clients import OwnerTargetView
from pandorapy.services.ds_allocator.conf import AllocatorConf

__all__ = [
    "ACTIVE_INDEX_RECONCILE_INTERVAL_SEC",
    "COMMAND_NONE",
    "COMMAND_STOP",
    "DETACHED_CLEANUP_TIMEOUT_SEC",
    "KILL_STRANDED_TASK_NAME",
    "LOCATION_REFRESH_TIMEOUT_SEC",
    "READY_POLL_INTERVAL_SEC",
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
    "UPDATE_MAX_RETRY",
    "AllocateResult",
    "AllocatorUsecaseBase",
    "AuthoritativeGameServerAllocator",
    "BattleAllocationAbortRepo",
    "BattleAllocationLedger",
    "BattleAllocationLifecycleRepo",
    "BattleActiveIndexReconciler",
    "BattleCredentialSigner",
    "BattleWaitOwnershipLostError",
    "DSLifecyclePusher",
    "HeartbeatAllocationFencedError",
    "HeartbeatPodMismatchError",
    "HeartbeatTerminalError",
    "LocationRefresher",
    "NoShowRecorder",
    "OrphanGameServerReconciler",
    "ReadyWaitTimeoutError",
    "StrictModelBBattleStorage",
    "SweepWriterLease",
    "allocate_result_from_battle",
    "owner_target_from_allocate_result",
]

# ── 无符号边界(Go 由 uint32/uint64 类型系统免费获得)──────────────────────────
#
# ★ Python 整数不回绕,Go 会。于是"传了个 2^64 的 match_id"在 Go 里根本不可能
#   编译通过,在 Python 里则会一路带到 protobuf 序列化才抛一个既没有业务码、
#   也说不清是哪个字段的 ValueError。入口显式判断把它变回 InvalidArg。
_UINT64_MAX = (1 << 64) - 1


# ── 哨兵错误(Go 的 `var errX = errors.New(...)`)──────────────────────────────


class HeartbeatTerminalError(Exception):
    """镜像已是终态(ended/abandoned)时从乐观锁回调抛出。Go: `errHeartbeatTerminal`。

    抛出即**中止写回**(不刷新 last_heartbeat_ms / TTL / active score),由 Heartbeat
    捕获后转成 stop 指令。保证 abandoned 后 DS 继续心跳不会推迟补偿重试、不会刷新
    BattleTTL 上界(W4 ⑧ Codex 复审 P1)。
    """


class HeartbeatPodMismatchError(Exception):
    """心跳上报的 pod 与镜像记录不一致。Go: `errHeartbeatPodMismatch`。

    旧 DS / 孤儿 DS / 重分配后残留的上一个 pod。抛出 → 不写回该镜像,并令上报方停机,
    避免污染新对局的状态(last_heartbeat_ms / state / player_count)。
    """


class HeartbeatAllocationFencedError(Exception):
    """该 match 处于 GSA 结果未知或外部 release 未确认的永久墓碑。

    Go: `errHeartbeatAllocationFenced`。即使同版本副本仍跑 legacy 配置,也必须零写入
    返回 stop,不能让旧 DS 心跳把 state 改回 running 并恢复 TTL。
    """


class ReadyWaitTimeoutError(Exception):
    """AllocateBattle 等待 DS ready 心跳超时。Go: `errReadyWaitTimeout`。

    由 `wait_battle_ready` 抛出,调用方据此走「回收 pod + 删镜像 + 返回
    ErrDSAllocationFailed」的清理路径。
    """


class BattleWaitOwnershipLostError(Exception):
    """wait 提前终止且该分配的回收/接管所有权已属他人。Go: `errBattleWaitOwnershipLost`。

    ★ owner 见到本哨兵**不得**再执行 `cleanup_allocated_battle` —— sweep 的 fenced
      回收链可能正在途,并发第二路 `fence_preactive_release` / `release_expected`
      只有幂等最终一致保证,**没有单次调用保证**(复审必修:回收所有权必须可识别)。

    随 `ErrDSAllocationFailed` 一起挂在 `PandoraError.cause` 上(对应 Go 的
    `errcode.NewCause`),调用方用 `isinstance(err.cause, BattleWaitOwnershipLostError)`
    识别,与 Go 的 `errors.Is` 同形。
    """


# ── 战斗 DS 状态常量(对应 proto 的 string state 字段)────────────────────────

STATE_ALLOCATING = "allocating"
#: 以下五项**从 repo.py import**,不再抄一份:那边是写入侧不变量校验的判据处。
STATE_ALLOCATION_UNCERTAIN = R.BATTLE_STATE_ALLOCATION_UNCERTAIN
STATE_ALLOCATION_RECONCILING = R.BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING
STATE_ALLOCATION_EMPTY_FENCE = R.BATTLE_STATE_ALLOCATION_RECONCILE_EMPTY_TOMBSTONE
STATE_PREACTIVE_RELEASING = R.BATTLE_STATE_PREACTIVE_RELEASE_PENDING
STATE_ALLOCATION_ABORT = R.BATTLE_STATE_ALLOCATION_ABORT_PENDING
STATE_WARMING = "warming"
STATE_READY = "ready"
STATE_RUNNING = "running"
STATE_ENDED = "ended"
STATE_ABANDONED = "abandoned"

# ── Heartbeat 响应控制指令常量 ────────────────────────────────────────────────

COMMAND_NONE = ""
COMMAND_STOP = "stop"  # 通知孤儿 DS(无对应镜像)自行停机

# ── 时间 / 重试常量 ──────────────────────────────────────────────────────────

#: 乐观锁重试次数(心跳/状态更新冲突)。Go: `updateMaxRetry`。
UPDATE_MAX_RETRY = 3

#: AllocateBattle 等待 DS ready 心跳时轮询 Redis 镜像的间隔(秒)。Go: `readyPollInterval`。
#:
#: 1s 足够:DS 心跳 5s 一跳,ready 等待窗口 10s,1s 轮询既不漏判也不给 Redis 添压。
#: ★ 与 Go 同样是**可写模块变量**(Go 用 `var` 而非 `const`),便于单测调小避免慢测。
READY_POLL_INTERVAL_SEC = 1.0

#: 从 canonical 记录重建派生 active ZSET 的节流间隔(秒)。Go: `activeIndexReconcileInterval`。
#: 首扫总会跑一次;之后周期重复,使"单独一次 ZSET 写丢失"不会永久搁浅一个恢复墓碑。
ACTIVE_INDEX_RECONCILE_INTERVAL_SEC = 30.0

#: ready 等待失败后回收 pod + 删镜像的独立超时预算(秒)。Go: `detachedCleanupTimeout`。
#:
#: ready 等待失败的常见原因**正是**入站 ctx 被取消/超时,复用它做 Release/DeleteBattle
#: 会立刻失败,留下 warming 镜像 + 已分配 pod 泄漏;故清理必须用与入站解耦的短超时。
DETACHED_CLEANUP_TIMEOUT_SEC = 5.0

#: 心跳后异步续期玩家 BATTLE 位置的独立超时预算(秒)。Go: `locationRefreshTimeout`。
#: 短超时防 locator 卡死时后台续期协程泄漏。
LOCATION_REFRESH_TIMEOUT_SEC = 3.0

#: `kill_stranded_ds` 的 safego 点位名。裸 `asyncio.create_task` 的异常会被静默吞进
#: Task,直到 GC 才打一条认不出主人的 "never retrieved";带名字才能在
#: `panic_recovered{name=...}` 上直接定位。
KILL_STRANDED_TASK_NAME = "ds_kill_stranded"

# ── 分配 / 就绪等待 / 心跳 / 回收链路的拒绝 reason 枚举(infra.md §11.3 R2)──────
#
# 全部取自**已经存在的判定分支**,一个分支一个常量:凡一个 if 里收敛了多个条件的,
# 都按条件拆开。"进不去副本"的三类根因(没有可用 GameServer / Agones 分配失败 /
# roster 没到齐被判弃)必须能靠 reason 直接分开统计,不能只看到一句"分配失败"。
#
# snake_case 值,稳定不变(日志系统按 event+reason 聚合)。

# AllocateBattle 入参与 claim 阶段。
REASON_ALLOC_MATCH_ID_REQUIRED = "match_id_required"
REASON_ALLOC_ROSTER_INVALID = "roster_invalid"
REASON_ALLOC_FACTIONS_INVALID = "combat_factions_invalid"
REASON_ALLOC_CLAIM_FAILED = "claim_write_failed"
REASON_ALLOC_TRACK_INVALID = "release_track_invalid"
REASON_ALLOC_FINALIZE_LOST = "finalize_claim_lost"
# 幂等重试 / claim 输家(await_existing_allocation)。
REASON_AWAIT_CLAIM_MISSING = "claim_record_missing"
REASON_AWAIT_RELEASE_UNCONFIRMED = "preactive_release_unconfirmed"
# wait_battle_ready 的提前失败(每条对应一个既有 return,不合并)。
REASON_WAIT_AUTHORITY_PURGED = "authority_purged"
REASON_WAIT_ALLOC_SUPERSEDED = "allocation_superseded"
REASON_WAIT_AUTH_OUTSIDE_GRACE = "auth_missing_outside_grace"
REASON_WAIT_AUTH_PROVISION_DEAD = "auth_provision_stalled"
REASON_WAIT_STATE_RECLAIMED = "state_reclaimed"
REASON_WAIT_AUTH_PHASE_FENCED = "auth_phase_fenced"
REASON_WAIT_RECORD_GONE = "battle_record_gone"
REASON_WAIT_CTX_DONE = "caller_ctx_done"

# `ListBattles` 单次返回上限(§9 不变量 18)。Go 侧无上限 —— active 集合到千级时
# 会一次全量回。本接口只是运维 / 调试面,截断不影响任何玩家路径,截断时打
# `battle_list_truncated`。
LIST_BATTLES_MAX = 500
# 心跳链路。
REASON_HEARTBEAT_MODEL_B_OFF = "redis_authority_disabled"
REASON_HEARTBEAT_CENSUS_INCONSISTENT = "player_count_exceeds_census"
REASON_HEARTBEAT_AUTHORITY_READ = "authority_read_failed"
REASON_HEARTBEAT_ACTIVATE_REJECTED = "activate_rejected"
REASON_HEARTBEAT_OWNER_LEASE = "owner_lease_renew_failed"
REASON_HEARTBEAT_DEPARTURE_RECONCILE = "departure_reconcile_failed"
REASON_HEARTBEAT_UPDATE_FAILED = "battle_update_failed"
# 心跳 stop 指令(Model B),按判定依据分档。
REASON_STOP_TERMINAL_AUTH = "auth_terminating"
REASON_STOP_BATTLE_MISSING = "battle_record_missing"
REASON_STOP_BATTLE_ENDED = "battle_ended"
REASON_STOP_BATTLE_ABANDONED = "battle_abandoned"
# 回收 / 释放。
REASON_RELEASE_TUPLE_INCOMPLETE = "expected_tuple_incomplete"
REASON_RELEASE_POD_UID_MISSING = "expected_pod_uid_missing"
# 重连重签的只读权威查询(resolve_battle_target)。
REASON_RESOLVE_ARGS_INVALID = "resolve_args_invalid"
REASON_RESOLVE_AUTHORITY_OFF = "read_only_authority_unavailable"
REASON_RESOLVE_AUTHORITY_READ = "authority_read_failed"
REASON_RESOLVE_NOT_AUTHORIZED = "target_not_authorized"
REASON_RESOLVE_PROJECTION_INCOMPLETE = "target_projection_incomplete"
# ReleaseBattle / ReleaseBattleExpected / FinalizeBattleReleaseExpected。
REASON_RELEASE_MODEL_B_UNSUPPORTED = "model_b_requires_expected_tuple"
REASON_RELEASE_FENCED = "allocation_fence_requires_reconcile"
REASON_RELEASE_AUTHORITY_OFF = "redis_authority_required"
REASON_RELEASE_PROOF_INCOMPLETE = "terminal_proof_incomplete"
REASON_RELEASE_IDENTITY_CHANGED = "stable_identity_changed"
REASON_RELEASE_AUTHORITY_REREAD = "terminal_authority_reread_failed"
REASON_RELEASE_SNAPSHOT_STALE = "terminal_snapshot_incomplete"
REASON_FINALIZE_PROOF_INCOMPLETE = "finalize_proof_incomplete"
REASON_FINALIZE_TOMBSTONE_PENDING = "terminal_tombstone_not_confirmed"
# AbortPreactiveBattle(matchmaker 分配 saga 补偿)。
REASON_ABORT_REQUEST_INCOMPLETE = "abort_request_incomplete"
REASON_ABORT_AUTHORITY_OFF = "abort_requires_redis_authority"
REASON_ABORT_PREFLIGHT_FAILED = "abort_preflight_failed"
REASON_ABORT_FENCE_FAILED = "abort_fence_failed"
REASON_ABORT_ACK_CLEANUP_PENDING = "abort_ack_cleanup_pending"
REASON_ABORT_FENCE_IDENTITY_MISSING = "abort_fence_pod_authority_missing"
REASON_ABORT_LIFECYCLE_PENDING = "abort_lifecycle_publish_pending"
REASON_ABORT_LIFECYCLE_MARKER = "abort_lifecycle_marker_pending"
REASON_ABORT_COMPLETION_PENDING = "abort_completion_pending"
# Pod UID 持久化前置(所有精确回收的共同前提)。
REASON_POD_UID_AUTHORITY_READ = "pod_uid_preflight_authority_read_failed"
REASON_POD_UID_IDENTITY_CHANGED = "pod_uid_preflight_identity_changed"
REASON_POD_UID_RESOLVE_FAILED = "pod_uid_resolve_failed"
REASON_POD_UID_BACKFILL_FAILED = "pod_uid_backfill_failed"
REASON_POD_UID_VERIFY_FAILED = "pod_uid_backfill_verify_failed"
# 派生 active 索引重建。
REASON_ACTIVE_INDEX_UNAVAILABLE = "active_index_reconciler_unavailable"
REASON_ACTIVE_INDEX_REBUILD = "active_index_rebuild_failed"


# ── 弱依赖 / 能力协议 ────────────────────────────────────────────────────────
#
# ★ 为什么这里建 Protocol,而 `repo.py` 明确说不建:
#   `repo.py` 的四个 Go 接口是**同一个实现类**的切面,Python 侧建 Protocol 纯属
#   §15.3 预设性复杂化。而下面这几个是 Go 里真正做了**能力探测**
#   (`x.(Iface)` type assertion)的接口 —— 探测本身是业务逻辑的一部分
#   (探测不到 = 清扫禁用 / Model B 拒绝启动),必须在 Python 侧有等价物。
#   `runtime_checkable` 的 `isinstance` 只查方法存在性,与 Go 的结构化断言同语义。


@runtime_checkable
class DSLifecyclePusher(Protocol):
    """发 `pandora.ds.lifecycle` 事件(W4 ③)。Go: `DSLifecyclePusher`。

    心跳超时标记 abandoned 后,由它把 `DSLifecycleEvent{phase=ABANDONED}` 发给
    battle_result 做玩家段位回滚补偿(§9 不变量 4 DS 崩溃必有补偿)。

    W4 ⑧:投递失败不再静默丢 —— `sweep_once` 把对局保留在 active ZSET,下一轮重试,
    直到投递成功或镜像 TTL 过期;配合 battle_result 幂等消费构成 at-least-once 闭环。
    实现可在内部失败时**抛异常**(由 `sweep_once` 触发重试),对应 Go 的 `return err`。
    """

    async def publish_lifecycle(self, evt: Any) -> None: ...


@runtime_checkable
class LocationRefresher(Protocol):
    """续期玩家 BATTLE 位置 TTL(断线重连,docs/design/battle-reconnect.md §2.2)。

    Go: `LocationRefresher`。心跳成功且对局处于 ready/running 时,用它把该对局玩家的
    位置刷新为 BATTLE(同 match_id 续期,BATTLE→BATTLE),使玩家整局在线期间 login
    都能检测到"在战斗中",从而支持中途掉线重登直连回原 battle DS。

    由 player_locator gRPC 客户端实现(`clients.GrpcLocationRefresher`);可为 None
    (未配 locator_addr → 不续期,弱依赖,不影响心跳 / 对局,仅长对局中途重登可能因
    位置过期退化为回大厅)。
    """

    async def refresh_battle_locations(
        self, player_ids: list[int], match_id: int, ds_addr: str
    ) -> None: ...


@runtime_checkable
class NoShowRecorder(Protocol):
    """no-show 记账 → 进入侧退避的写者。Go: `NoShowRecorder`。

    背压非权威门:错误只 Warn,**绝不阻断判弃收尾**(anti-abuse §2 铁律)。
    """

    async def record_no_show(self, player_id: int, window_sec: float) -> int:
        """记一次 no-show,返回窗口内累计次数(含本次)。"""
        ...

    async def arm_penalty(self, player_id: int, seconds: float) -> None:
        """布设进入侧退避窗(matchmaker StartMatch 读取执行)。"""
        ...


@runtime_checkable
class BattleCredentialSigner(Protocol):
    """把 Allocator 可见能力收窄为 DS callback 凭据签发。Go: `BattleCredentialSigner`。

    生产实现是 `auth.DSCallbackSigner`,**无法**从该字段调用玩家 Session / DSTicket
    签发方法 —— 这是刻意的能力收窄,不是接口洁癖:签战斗回调凭据的代码路径一旦
    顺手拿到玩家票据签发权,一个 DS 侧漏洞就能升级成"给任意玩家签任意进场票"。

    ★ `ttl_sec` 是**秒**(Go 是 `time.Duration`);返回值对应 Go 的
      `auth.HubCredentialResult`,该类型尚未移植,故标注为 `Any`(见交付报告)。
    """

    def sign_battle_credential(
        self,
        match_id: int,
        pod: str,
        instance_uid: str,
        epoch: int,
        gen: int,
        jti: str,
        ttl_sec: float,
    ) -> Any: ...


@runtime_checkable
class SweepWriterLease(Protocol):
    """心跳扫描循环的领导权来源。Go: `SweepWriterLease`。

    ★ `current()` 返回 `(token, held)` —— **与 hub_allocator 的 writer_fence 顺序相同**,
      但本接口**只消费第二个返回值**(Go 头注释明写)。ds_allocator 的 sweep 不携带
      跨轮次权威意图,选举的收益是降载与去重,防脑裂由按 match 凭据 CAS 承担。
    """

    def current(self) -> tuple[int, bool]: ...


@runtime_checkable
class OrphanGameServerReconciler(Protocol):
    """孤儿 Allocated GameServer 对账清扫所需的编排层能力。Go: 同名接口。

    只有 Agones 分配器实现;local / mock 不实现 → 清扫**自动禁用**(它们没有
    Agones Allocated 概念)。
    """

    async def list_allocated_game_servers(self) -> list[Any]: ...

    async def delete_allocated_game_server_exact(
        self, name: str, uid: str, expected_allocation_id: str
    ) -> bool: ...


@runtime_checkable
class BattleAllocationLedger(Protocol):
    """防误删④的权威台账能力。Go: `BattleAllocationLedger`(`RedisBattleRepo` 实现)。

    未实现该接口的 repo 下清扫整体禁用 —— **没有出身证明就没有删除权**。
    """

    async def record_allocation_ledger(self, allocation_id: str, at_ms: int) -> None: ...

    async def allocation_ledger_contains(self, allocation_id: str) -> bool: ...

    async def prune_allocation_ledger(self, before_ms: int) -> int: ...


@runtime_checkable
class BattleActiveIndexReconciler(Protocol):
    """从 canonical battle 记录重建派生 active ZSET。Go: `data.BattleActiveIndexReconciler`。

    加法式能力,使旧 writer / 测试替身不能意外获得恢复权限;生产 Redis 实现它,
    缺失时 usecase **fail-closed**(`enable_redis_authority` 拒绝启动)。
    """

    async def reconcile_battle_active_index(self, count: int) -> None: ...


@runtime_checkable
class AuthoritativeGameServerAllocator(Protocol):
    """Agones Model B 的额外能力。Go: `AuthoritativeGameServerAllocator`。

    分配时先取得实例 UID/RV,Redis stage 成功后再用 UID+RV 条件 PATCH 投递 annotation。
    **K8s 仅是投递镜像,不是授权权威。**
    """

    async def allocate_authoritative(self, *args: Any, **kwargs: Any) -> Any: ...

    async def deliver_credential(self, *args: Any, **kwargs: Any) -> str: ...

    async def resolve_expected_pod_uid(self, *args: Any, **kwargs: Any) -> str: ...

    async def release_expected(self, allocation: Any) -> None: ...


@runtime_checkable
class BattleAllocationAbortRepo(Protocol):
    """exact、同 slot 的 abort 日志。Go: `data.BattleAllocationAbortRepo`。

    与 `BattleAuthRepo` 是**分离的能力**:能轮换 DS 回调凭据的代码不会自动获得
    破坏性的 allocation 权限。
    """

    async def fence_allocation_abort_expected(self, *args: Any, **kwargs: Any) -> Any: ...

    async def read_allocation_abort(self, *args: Any, **kwargs: Any) -> Any: ...

    async def complete_allocation_abort_expected(self, *args: Any, **kwargs: Any) -> bool: ...


@runtime_checkable
class BattleAllocationLifecycleRepo(Protocol):
    """为一个 exact、已拆除的 allocation 见证 Kafka ACK。Go: 同名接口。

    刻意与 `BattleRepo` 分离:只有 Model-B 生命周期恢复路径需要这个权限。
    """

    async def record_allocation_lifecycle_published(self, *args: Any, **kwargs: Any) -> None: ...


@runtime_checkable
class StrictModelBBattleStorage(Protocol):
    """epoch-2 严格写档开关。Go: `data.StrictModelBBattleStorage`。

    启动全 master 预检通过后、任何 Model-B RPC/worker 可见之前打开,**不可逆**。
    """

    def enable_strict_model_b_writes(self) -> None: ...

    def strict_model_b_writes_enabled(self) -> bool: ...


# ── RPC 1:AllocateBattle 的出参 ──────────────────────────────────────────────


@dataclass(slots=True)
class AllocateResult:
    """AllocateBattle 的出参。对应 Go 的 `AllocateResult`。

    `gameserver_uid` / `instance_epoch` / `allocation_id`(DSTicket v2,方案 B):
    matchmaker 签 battle 票时把票绑死到唯一 DS 实例。三者与 `ds_addr` 来自**同一份**
    权威快照(Redis `BattleStorageRecord`),不得从其它时点 / 其它源拼凑,否则地址与
    票据可能指向不同实例 —— 表现是玩家连上一台 DS 却被它以"票不是我的"踢掉。
    """

    ds_addr: str = ""
    ds_pod_name: str = ""
    allocated_at_ms: int = 0  # Go: int64
    gameserver_uid: str = ""
    instance_epoch: int = 0  # Go: uint32
    allocation_id: str = ""
    release_track: str = ""


def allocate_result_from_battle(b: Any) -> AllocateResult | None:
    """权威 `BattleStorageRecord` → 出参投影。对应 Go 的 `allocateResultFromBattle`。

    ★ 字段名逐个对着 `pandora/ds/v1/allocator_pb2.pyi` 核过,**没有**从 Go 的
      `GetGameserverUid()` 反推成 `gameserver_u_id` 之类:proto 里就是
      `gameserver_uid`(单个下划线),Go 的驼峰是生成器加的。
    """
    if b is None:
        return None
    return AllocateResult(
        ds_addr=b.ds_addr,
        ds_pod_name=b.ds_pod_name,
        allocated_at_ms=b.allocated_at_ms,
        gameserver_uid=b.gameserver_uid,
        instance_epoch=b.instance_epoch,
        allocation_id=b.allocation_id,
        release_track=b.release_track,
    )


def owner_target_from_allocate_result(res: AllocateResult | None) -> OwnerTargetView:
    """出参 → owner 权威的 exact 身份视图。对应 Go 的 `ownerTargetFromAllocateResult`。

    ★ 刻意**不填** `source_revision`(留 0):Go 侧同样只搬五个身份字段。
      `OwnerTargetView.source_revision` 是 assignment 的**来源版本**
      (INC-20260818-003),由 hub_allocator 的写者租约铸号器产生;battle 侧
      没有那条铸号链,凭空塞一个非零值会让 owner 的全序判定拿到一个假水位。
      "本部署没有版本"必须表达成 0(= 与任何非零都不可比),不是表达成"1"。
    """
    if res is None:
        return OwnerTargetView()
    return OwnerTargetView(
        pod_name=res.ds_pod_name,
        instance_uid=res.gameserver_uid,
        instance_epoch=res.instance_epoch,
        assignment_or_allocation_id=res.allocation_id,
        release_track=res.release_track,
    )


# ── 基座 ─────────────────────────────────────────────────────────────────────


class AllocatorUsecaseBase:
    """`AllocatorUsecase` 的字段 / 构造 / setter / accessor + 只读重连查询。

    对应 Go 的 `AllocatorUsecase` 结构体与 `allocator.go` 第 1–546 行的方法。

    ★ 刻意**不**加 `__slots__`:四个 mixin 叠加时 slots 布局要在每层重复声明,
      漏一层就静默退回 `__dict__`,反而制造"以为有约束其实没有"的假象
      (与 `hub_allocator/biz_base.py` 同因)。
    """

    def __init__(self, repo: Any, alloc: Any, cfg: AllocatorConf) -> None:
        self.repo = repo
        self.alloc = alloc
        self.cfg = cfg

        # 可为 None;仅显式 local/off 开发配置允许 best-effort 降级。
        self.lifecycle: DSLifecyclePusher | None = None
        # Redis authority / 生产 enforce 路径为 True。即便启动装配被未来改坏,
        # None publisher 也只能保留 active outbox 重试,**绝不能**把 abandoned
        # 当作已恢复并 Expire 掉 Battle fence。
        self.lifecycle_required = False

        # no-show 记账 → 进入侧退避(anti-abuse §6 第 8 项)。None = 不记罚。
        self.no_show_recorder: NoShowRecorder | None = None

        # owner 权威实例租约双写(owner-authority.md migrate ⑥;
        # None = 未启用;required 语义见 biz 的 renew_owner_lease_gate)。
        self.owner_lease: Any | None = None
        self.owner_lease_required = False

        # owner 迁移弱依赖调用面 + census 已准入缓存(owner-authority.md migrate ②/③)。
        self.owner_auth: Any | None = None
        # Go 是 `sync.Map`,key = "instanceUID|playerID",value = last-touch 时刻。
        # asyncio 单线程事件循环下普通 dict 足够,且 `sweep_stale_owner_admitted`
        # 需要遍历 + 按时间淘汰 —— 用 dict 才能做到。
        # ⚠️ 若将来把本服务改成多线程 executor,这里必须换回带锁结构。
        self.owner_admitted: dict[str, float] = {}

        # 可为 None(未配 locator_addr 时不续期 BATTLE 位置)。
        self.locator: LocationRefresher | None = None

        # Model B 仅在 agones+enforce+authority_mode=redis 时由 main 注入。
        # Redis auth_repo 是**唯一授权权威**;K8s annotation 只投递 pending 凭据。
        self.auth_repo: Any | None = None
        self.abort_repo: Any | None = None
        self.lifecycle_proof_repo: Any | None = None
        self.authoritative_alloc: Any | None = None
        self.ds_signer: BattleCredentialSigner | None = None
        self.ds_credential_ttl_sec: float = 0.0
        self.model_b = False
        self.release_policy: Any | None = None
        self.active_index_reconciler: BattleActiveIndexReconciler | None = None
        #: 上次派生索引重建的**单调**时刻(秒);0 = 从未重建(首扫必跑)。
        #: ★ 用 `asyncio.get_running_loop().time()` 而非墙钟:NTP 回拨会让节流窗
        #:   忽然变成"几小时都不该重建"。
        self.last_active_index_reconcile: float = 0.0

        # sweep 队头公平性的**进程内**退避表(INC-20260724-001)。
        # key = match_id,value = biz_sweep 侧定义的 `SweepDeferral`
        # (Go 在 allocator.go:3447 定义,本次移植范围之外,故标注 Any)。
        #
        # 只有 `sweep_once` 读写,而 `sweep_once` 只由 `run_heartbeat_sweep` 单协程
        # 驱动(与 last_active_index_reconcile 同一并发域),故不加锁。
        #
        # 它是**纯调度提示、非权威**:不参与准入 / 归属 / 扣减任何决策,丢失只会让
        # 某项早一轮被重试(方向安全),因此不构成 §9.22 意义上的影子状态。
        # 权威仍是 active ZSET。
        self.sweep_defer_until: dict[int, Any] = {}

        # 心跳扫描循环的领导权来源(None = 未启用继任租约,本副本无条件扫描)。
        # 只在启动装配期写入,之后只读。作用域**仅限 sweep_once**,不参与任何 RPC 写路径。
        self.sweep_lease: SweepWriterLease | None = None
        # 上一轮观察到的领导权,只由 `run_heartbeat_sweep` 单协程读写,
        # 用于把"接任 / 让位"打成状态跃迁日志而不是每 tick 刷屏。
        self.sweep_lease_held = False

        # 心跳判定某 DS 该停机(orphan / pod_mismatch / 终态)时,是否由后端主动回收该 pod。
        # local 模式打开 —— 本机 UE DS 没有 Agones,收到 stop 指令不会自杀,残留进程会
        # 幽灵般占着监听端口污染下一局。Agones 模式默认关闭:孤儿 GameServer 由 Agones
        # 生命周期回收,避免 Redis 抖动误判 orphan 时误删正常 pod。
        self.kill_orphan_on_stop = False

        # 孤儿 Allocated GameServer 对账清扫(2026-08-03)。
        # 二者任一缺失则清扫整体禁用。
        self.orphan_gs_reconciler: OrphanGameServerReconciler | None = None
        self.allocation_ledger: BattleAllocationLedger | None = None
        #: 与 sweep_defer_until 同一并发域(仅 sweep_once 单协程读写,不加锁),
        #: 同为进程内非权威调度提示:丢失只是把回收推迟一个观察期,方向安全。
        self.last_orphan_gs_reconcile: float = 0.0
        self.orphan_gs_first_seen: dict[str, float] = {}

        # ── 构造期能力探测(对应 Go `NewAllocatorUsecase` 的三次 type assertion)──
        if isinstance(repo, BattleActiveIndexReconciler):
            self.active_index_reconciler = repo
        if isinstance(alloc, OrphanGameServerReconciler):
            # 仅 Agones 分配器实现;local/mock 自动禁用
            self.orphan_gs_reconciler = alloc
        if isinstance(repo, BattleAllocationLedger):
            # 孤儿清扫的权威台账(防误删④);无台账则清扫禁用
            self.allocation_ledger = repo

    # ── setter(全部对应 Go 的 Set*)────────────────────────────────────────

    def set_lifecycle_pusher(self, p: DSLifecyclePusher | None) -> None:
        """注入 ds.lifecycle 事件发送器(main 在 Kafka 就绪时调用)。Go: `SetLifecyclePusher`。"""
        self.lifecycle = p

    def set_lifecycle_pusher_required(self, required: bool) -> None:
        """把生产发布策略注入业务层。Go: `SetLifecyclePusherRequired`。

        Redis authority 在 `enable_redis_authority` 内还会**无条件打开**此门,
        避免调用方漏配。
        """
        self.lifecycle_required = required

    def validate_lifecycle_pusher_ready(self) -> None:
        """启动装配的最后一道门。Go: `ValidateLifecyclePusherReady`。

        配置宣称可靠发布时,只有非 None publisher 才允许启动 sweep/RPC。

        ★ Go 返回 error,Python 抛 `PandoraError`(§ 与 Go 的必要形变 2)。
        """
        if self.lifecycle_required and self.lifecycle is None:
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "reliable ds.lifecycle publisher is required before allocator startup",
            )

    def set_location_refresher(self, r: LocationRefresher | None) -> None:
        """注入 BATTLE 位置续期器(main 在 locator_addr 已配时调用,弱依赖)。"""
        self.locator = r

    def set_no_show_recorder(self, r: NoShowRecorder | None) -> None:
        """注入 no-show 记罚器(可选;不注入 = 不记罚,dev 联调兼容)。"""
        self.no_show_recorder = r

    def set_release_track_policy(self, p: Any | None) -> None:
        """在启动期注入 match 级确定性 cohort 策略。Go: `SetReleaseTrackPolicy`。"""
        self.release_policy = p

    def set_kill_orphan_on_stop(self, v: bool) -> None:
        """打开「心跳 stop 时主动回收该 DS」(main 在 mode=local 时调用)。

        见 `kill_orphan_on_stop` 字段说明:local 模式的 UE DS 收到 stop 不自杀,
        需后端主动 kill 防幽灵占端口。
        """
        self.kill_orphan_on_stop = v

    def set_owner_lease_renewer(self, r: Any | None, required: bool) -> None:
        """注入 owner 实例租约续写器 + 档位。对应 Go 的 `SetOwnerLeaseRenewer`。

        (Go 侧定义在 `owner_lease.go`,但只写基座字段,故随基座一起提供。)
        """
        self.owner_lease = r
        self.owner_lease_required = required

    def set_owner_authority(self, a: Any | None) -> None:
        """注入 owner 权威调用面(nil-safe)。对应 Go 的 `SetOwnerAuthority`。

        (Go 侧定义在 `owner_authority.go`,同样只写基座字段。)
        """
        self.owner_auth = a

    # ── Model B 激活 ───────────────────────────────────────────────────────

    def enable_redis_authority(
        self,
        repo: Any,
        signer: BattleCredentialSigner | None,
        token_ttl_sec: float,
    ) -> None:
        """打开 Battle Model B。对应 Go 的 `EnableRedisAuthority`。

        依赖必须**一次完整注入**;任何缺失都拒绝启动,禁止出现"配置说 redis
        authority、实际悄悄回退 legacy"的半开启状态 —— 那种状态下 `resolve_battle_target`
        会返回 `ErrUnavailable`,玩家重连被静默退化成回大厅,而运维看到的是"服务健康"。

        六项能力 + 三项取值缺一不可(顺序照抄 Go 的单个 `if`,不拆成早退):
          `repo` / `signer` 非空、`token_ttl_sec > 0`、
          alloc 实现 `AuthoritativeGameServerAllocator`、
          repo 实现 `BattleAllocationAbortRepo` / `BattleAllocationLifecycleRepo`、
          `active_index_reconciler` 已在构造期探测到、
          **两个** battle 写视图都实现 `StrictModelBBattleStorage`。

        ★ 严格写档的打开**不可逆且刻意排在所有能力检查之后**:启动全 master 预检
          通过之后,不允许任何 Model-B RPC/worker 在两个 battle 写视图强制同一份
          连续存储不变量之前变为可见。

        Raises:
            errcode.PandoraError(ErrInvalidState): 任一依赖缺失,或严格写档没有真正激活。
        """
        ok = isinstance(self.alloc, AuthoritativeGameServerAllocator)
        abort_ok = isinstance(repo, BattleAllocationAbortRepo)
        lifecycle_proof_ok = isinstance(repo, BattleAllocationLifecycleRepo)
        battle_strict_ok = isinstance(self.repo, StrictModelBBattleStorage)
        auth_strict_ok = isinstance(repo, StrictModelBBattleStorage)
        if (
            repo is None
            or signer is None
            or token_ttl_sec <= 0
            or not ok
            or not abort_ok
            or not lifecycle_proof_ok
            or self.active_index_reconciler is None
            or not battle_strict_ok
            or not auth_strict_ok
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "battle Model B requires auth/abort/lifecycle repo, signer, positive ttl, "
                "authoritative Agones allocator, canonical active-index reconciler and "
                "strict storage writers",
            )
        # Irreversible and deliberately last among capability checks.
        self.repo.enable_strict_model_b_writes()
        repo.enable_strict_model_b_writes()
        if not self.repo.strict_model_b_writes_enabled() or not repo.strict_model_b_writes_enabled():
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "battle Model B strict storage write gate did not activate",
            )
        self.auth_repo = repo
        self.abort_repo = repo
        self.lifecycle_proof_repo = repo
        self.authoritative_alloc = self.alloc
        self.ds_signer = signer
        self.ds_credential_ttl_sec = token_ttl_sec
        self.model_b = True
        self.lifecycle_required = True

    # ── 幽灵 DS 回收(fire-and-forget)──────────────────────────────────────

    def kill_stranded_ds(self, match_id: int, pod_name: str, reason: str) -> None:
        """心跳判定某 DS 该停机时,异步回收其 pod(local 模式防幽灵 DS 占端口)。

        对应 Go 的 `killStrandedDS`。`kill_orphan_on_stop` 关闭(Agones 模式)时为
        no-op,pod 回收交 Agones 生命周期。

        **fire-and-forget**,三条缺一不可:
          - 走 `safego.spawn` 带名字(裸 `asyncio.create_task` 的异常会被静默吞进
            Task,直到 GC 才打一条认不出主人的 "never retrieved");
          - **独立短超时** `DETACHED_CLEANUP_TIMEOUT_SEC`:编排层卡死既不给心跳响应
            加尾延迟,也不泄漏协程;
          - best-effort:失败只 Warn,绝不影响心跳主流程。

        ★ Go 侧用 `plog.Detach(ctx)` 显式剥掉请求 ctx(§16.7:请求 ctx 含 Kratos
          transport / metadata,不得逃逸进后台协程)。Python 侧本方法**不接受 ctx
          参数** —— 结构上就不可能把请求上下文带进去,是同一条纪律的更强形式。
        ★ 三个入参都是不可变标量,不存在 hub 侧 `player_ids` 那种"调用方复用切片"
          的问题,故不拷贝。
        """
        if not self.kill_orphan_on_stop or pod_name == "":
            return

        async def _run() -> None:
            try:
                async with asyncio.timeout(DETACHED_CLEANUP_TIMEOUT_SEC):
                    await self.alloc.release(pod_name)
            except asyncio.CancelledError:
                # ★ 必须紧邻宽 except 之上:CancelledError 继承 BaseException,
                #   被吞掉会让停机时这条任务不退出。
                raise
            except BaseException as exc:  # noqa: BLE001 —— best-effort,失败只告警
                plog.get().warning(
                    "kill_stranded_ds_failed",
                    match_id=match_id,
                    pod=pod_name,
                    reason=reason,
                    err=str(exc),
                )
                return
            plog.get().info(
                "kill_stranded_ds", match_id=match_id, pod=pod_name, reason=reason
            )

        safego.spawn(KILL_STRANDED_TASK_NAME, _run)

    # ── accessor ───────────────────────────────────────────────────────────

    def battle_ttl_sec(self) -> float:
        """战斗镜像 TTL(秒)。对应 Go 的 `battleTTL()`。"""
        return self.cfg.battle_ttl_td().total_seconds()

    def ready_wait_timeout_sec(self) -> float:
        """AllocateBattle 等待 DS ready 心跳的最长时间(秒,默认 10s)。

        对应 Go 的 `readyWaitTimeout()`。
        """
        return self.cfg.ready_wait_timeout_td().total_seconds()

    def heartbeat_timeout_ms(self) -> int:
        """心跳超时阈值(毫秒)。对应 Go 的 `u.cfg.HeartbeatTimeout.Std().Milliseconds()`。

        ★ 用 `td / timedelta(milliseconds=1)` 再 `int()`:那是**向零截断**,与 Go 的
          `Duration.Milliseconds()` 逐位一致。写成 `int(total_seconds() * 1000)` 会
          在 `2.5h` 这类值上被浮点尾数咬掉 1ms —— 单看没影响,但它是
          `ready_authorized()` 的新鲜度判据,边界上会抖。
        """
        return int(self.cfg.heartbeat_timeout_td() / _dt.timedelta(milliseconds=1))

    # ── RPC:ResolveBattleTarget(重连重签的只读权威查询)────────────────────

    async def resolve_battle_target(self, match_id: int, player_id: int) -> AllocateResult:
        """重连重签的只读权威查询。对应 Go 的 `ResolveBattleTarget`。

        它**只读**同一份 Redis auth+projection 快照:不创建 allocation claim、
        不调用 Agones、不刷新 TTL/heartbeat/index。任何"顺手补一下"的写都会让一次
        重连查询变成一次归属变更。

        Returns:
            该 match 当前 exact 实例的分配投影(非 None)。

        Raises:
            errcode.PandoraError(ErrInvalidArg): match_id / player_id 缺失或越界。
            errcode.PandoraError(ErrUnavailable): 本副本未启用 Redis 权威,或投影缺
                exact 实例绑定字段。
            errcode.PandoraError(ErrPermissionDeny): 该玩家不属这局 / 对局未就绪。
            其它: `read_authority` 的错误**原样上抛**(§9.22 权威读失败按 UNKNOWN
                fail-closed,客户端应退避重查而非默认回大厅)。
        """
        if (
            match_id == 0
            or player_id == 0
            or match_id > _UINT64_MAX
            or player_id > _UINT64_MAX
            or match_id < 0
            or player_id < 0
        ):
            # ★ 越界与 0 走同一条拒绝路径:Go 的 uint64 形参使越界在编译期就不可能
            #   发生,Python 里唯一等价的表达就是把它归到 InvalidArg(而不是让它
            #   一路飘到 protobuf 序列化才抛一个没有业务码的 ValueError)。
            plog.get().warning(
                "battle_target_refused",
                reason=REASON_RESOLVE_ARGS_INVALID,
                match_id=match_id,
                player_id=player_id,
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "match_id and player_id required")
        if not self.model_b or self.auth_repo is None:
            # 这条以前完全静默:重连拿不到目标 → 玩家被退化成回大厅,而后端连"是因为
            # 本副本没开 Redis 权威"都看不出来(legacy 副本在滚动共存期就会走到这里)。
            plog.get().warning(
                "battle_target_refused",
                reason=REASON_RESOLVE_AUTHORITY_OFF,
                match_id=match_id,
                player_id=player_id,
                hint="本副本未启用 Redis 权威(legacy/滚动共存),只读重连查询不可用",
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle read-only authority unavailable"
            )
        try:
            snapshot = await self.auth_repo.read_authority(match_id)
        except asyncio.CancelledError:
            # ★ 必须紧邻宽 except 之上:取消是停机控制流,不是"权威读失败"。
            raise
        except BaseException as exc:
            plog.get().warning(
                "battle_target_refused",
                reason=REASON_RESOLVE_AUTHORITY_READ,
                match_id=match_id,
                player_id=player_id,
                err=str(exc),
                hint="权威读失败按 UNKNOWN fail-closed(§9.22),客户端应退避重查而非默认回大厅",
            )
            raise
        ready, reason = snapshot.ready_authorized(now_ms(), self.heartbeat_timeout_ms())
        battle = snapshot.battle
        in_roster = battle is not None and player_id in battle.player_ids
        if not ready or battle is None or not in_roster:
            # `ready_authorized` 内部把近十条判据收敛成一个字符串 reason,这里必须原样
            # 带出来(auth-missing / phase-not-active / heartbeat-stale /
            # projection-mismatch …),否则"玩家重连被拒"只剩一个 PermissionDeny,
            # 分不清是对局没就绪还是人不在名单里。
            plog.get().warning(
                "battle_target_refused",
                reason=REASON_RESOLVE_NOT_AUTHORIZED,
                match_id=match_id,
                player_id=player_id,
                ready=ready,
                ready_reason=reason,
                in_roster=in_roster,
                # Go 的 `battle.GetState()` 在 battle==nil 时返回零值,Python 侧必须
                # 显式兜住 None —— 否则这条**诊断日志本身**会抛 AttributeError,
                # 把一个"该拒绝"变成一个 500。
                state=battle.state if battle is not None else "",
                pod=battle.ds_pod_name if battle is not None else "",
                allocation_id=battle.allocation_id if battle is not None else "",
                hint="ready_reason 是 ReadyAuthorized 的具体判据;in_roster=false 说明该玩家不属这局",
            )
            raise errcode.PandoraError(
                errcode.ErrPermissionDeny,
                "battle target not authorized for reconnect (reason=%s)",
                reason,
            )
        if (
            battle.ds_addr == ""
            or battle.ds_pod_name == ""
            or battle.gameserver_uid == ""
            or battle.instance_epoch == 0
            or battle.allocation_id == ""
            or not releasetrack.valid(battle.release_track)
        ):
            # 逐字段带出"哪一项缺了":重签 v2 战斗票要求 exact 实例绑定齐全,缺任一项
            # matchmaker/login 都签不出票,玩家表现为"重连一直失败"。
            plog.get().error(
                "battle_target_refused",
                reason=REASON_RESOLVE_PROJECTION_INCOMPLETE,
                match_id=match_id,
                player_id=player_id,
                ds_addr=battle.ds_addr,
                pod=battle.ds_pod_name,
                uid=battle.gameserver_uid,
                epoch=battle.instance_epoch,
                allocation_id=battle.allocation_id,
                release_track=battle.release_track,
                hint="投影已 ready 却缺 exact 实例绑定字段,重签票据不可能成功;查 finalize/凭据投递链",
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle target projection incomplete"
            )
        # R1:重连重签是一次**路由判定结果**(玩家将被指回这台 exact 实例)。低频、
        # 每次重连至多一条,是"玩家为什么被送回某台 DS"的唯一后端证据。
        plog.get().info(
            "battle_target_resolved",
            match_id=match_id,
            player_id=player_id,
            pod=battle.ds_pod_name,
            ds_addr=battle.ds_addr,
            uid=battle.gameserver_uid,
            epoch=battle.instance_epoch,
            allocation_id=battle.allocation_id,
            release_track=battle.release_track,
            state=battle.state,
        )
        result = allocate_result_from_battle(battle)
        # `battle` 已在上面判过非 None,故投影必然非 None;这行只做类型收窄,
        # **不是**运行期防线(真正的防线是上面那两组 fail-closed 判定)。
        assert result is not None
        return result

    # ── RPC:ListBattles(运维 / 调试只读列表)──────────────────────────────

    async def list_battles(self, state_filter: str, limit: int = 0) -> list[Any]:
        """列出当前战斗实例。对应 Go 的 `ListBattles`。

        Args:
            state_filter: 非空时按 `state` 精确过滤。
            limit: 单次返回上限;`<=0` 取 `LIST_BATTLES_MAX`。

        Returns:
            `dspb.BattleInfo` 列表(6 字段最小视图)。

        Raises:
            其它: `range_active_battles` 的错误原样上抛(索引读失败没法用部分结果
                冒充全量,那会让运维以为"就这么几局")。

        ★ **与 Go 的一处有意差异:读取侧上限**。Go 无上限,active 集合可达千级时一次
          全量回。§9 不变量 18 要求列表接口必须有单次返回上限,故这里截断并打
          `battle_list_truncated`。本接口是运维 / 调试面,截断不影响任何玩家路径。
        ★ 返回的是 `BattleInfo` 而不是 `BattleStorageRecord`(§9 不变量 14):权威快照
          里有 roster、凭据代际、writer_epoch、pod_uid 一整套 fencing 事实,不外露。
        """
        cap_ = LIST_BATTLES_MAX if limit <= 0 else min(limit, LIST_BATTLES_MAX)
        try:
            match_ids = await self.repo.range_active_battles()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            plog.get().warning(
                "battle_list_refused",
                reason=REASON_HEARTBEAT_AUTHORITY_READ,
                state_filter=state_filter,
                err=str(exc),
            )
            raise
        out: list[Any] = []
        # 读失败 / 索引残留在循环里被静默跳过。逐条打会刷屏(active 集合可达千级),
        # 故聚合:只记数量 + 首错 + 一个样本,循环后一条。
        read_failed = 0
        index_orphan = 0
        first_err = ""
        sample_match_id = 0
        truncated = 0
        for mid in match_ids:
            if len(out) >= cap_:
                truncated += 1
                continue
            try:
                battle = await self.repo.get_battle(mid)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                read_failed += 1
                if first_err == "":
                    first_err, sample_match_id = str(exc), mid
                continue
            if battle is None:
                index_orphan += 1
                if sample_match_id == 0:
                    sample_match_id = mid
                continue
            if state_filter != "" and battle.state != state_filter:
                continue
            out.append(
                dspb.BattleInfo(
                    match_id=battle.match_id,
                    ds_pod_name=battle.ds_pod_name,
                    ds_addr=battle.ds_addr,
                    state=battle.state,
                    player_count=battle.player_count,
                    allocated_at_ms=battle.allocated_at_ms,
                )
            )
        if read_failed > 0 or index_orphan > 0:
            plog.get().warning(
                "battle_list_partial",
                active=len(match_ids),
                returned=len(out),
                read_failed=read_failed,
                index_orphan=index_orphan,
                state_filter=state_filter,
                sample_match_id=sample_match_id,
                first_err=first_err,
                hint="index_orphan = active ZSET 有项但权威镜像已不在(TTL 到期/已删),由 sweep 自愈",
            )
        if truncated > 0:
            plog.get().warning(
                "battle_list_truncated",
                active=len(match_ids),
                returned=len(out),
                skipped=truncated,
                limit=cap_,
                state_filter=state_filter,
                hint="§9 不变量 18 读取侧上限;需要全量请按 state_filter 收窄",
            )
        return out

    # ── RPC:AllocateBattle(旧口径薄封装)──────────────────────────────────

    async def allocate_battle(
        self, match_id: int, player_ids: list[int], map_id: int, game_mode: str
    ) -> AllocateResult:
        """为 match 申请战斗 DS(不带阵营快照 / 不带计分模式的旧口径入口)。

        对应 Go 的 `AllocateBattle`。dev / 单测 / 兼容路径使用;`rating_mode` 留
        `UNSPECIFIED` = battle_result 按旧口径结算(见
        `BattleStorageRecord.rating_mode` 的 proto 注释)。

        关键(照抄 Go 的头注释,因为它解释了为什么这条链不能"拿到 pod 就回地址"):
        Agones Allocated(pod 被分配)≠ 战斗 DS Ready。DS 进程要先读到
        `pandora.dev/match-id` 才能在 PreLogin 放行客户端票据。所以真实链路是

            Allocate → create_battle(state=warming) → 轮询等 DS Heartbeat 上报正确
            match_id/pod 且进入 ready/running → 回 ds_addr;
            ReadyWaitTimeout 内没等到 → 回收 pod + 删镜像 + 分配失败。

        用 Redis 镜像轮询(而非内存 queue):Heartbeat RPC 可能落到另一个
        ds_allocator 副本,只有共享的 Redis 镜像能跨副本观察到 DS 的就绪心跳。

        ★ 真正的实现在 `biz_allocate.AllocateMixin.allocate_battle_with_combat_factions`
          (Go 546 行以下)。基座单独实例化时调用本方法会 `AttributeError` —— 这是
          刻意的:比起在基座里放一个 `raise NotImplementedError` 的假实现,让组装
          缺 mixin 的错误在**第一次调用**就带着方法名炸出来更容易定位。
        """
        return await self.allocate_battle_with_combat_factions(  # type: ignore[attr-defined]
            match_id,
            player_ids,
            None,
            map_id,
            game_mode,
            configpb.LEVEL_RATING_MODE_UNSPECIFIED,
            "",
        )
