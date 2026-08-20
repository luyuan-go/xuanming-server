"""hub_allocator 业务层**组装点** —— 对应 Go 侧
`services/battle/hub_allocator/internal/biz/hub.go` 的整体(3979 行)。

Go 那边一个文件一个 `HubUsecase`;Python 侧按职责切成四块分批移植,在这里合体:

    biz_base.py   HubUsecaseBase   字段 / 构造 / setter / TTL accessor   (hub.go 1–450)
    biz_shard.py  ShardMixin       分片选择 / 座位 / 归属 saga / 签票     (内部辅助)
    biz_rpc.py    RpcMixin         RPC 主流程                            (hub.go 450–1983)
    biz_sweep.py  SweepMixin       后台扫描 / 伸缩 / 整合迁移            (hub.go 1984–2137, 3440–3979)

上层(service / main)**只 import 本模块**:

    from pandorapy.services.hub_allocator import biz
    uc = biz.HubUsecase(repo, fleet, signer, cfg)

═══════════════════════════════════════════════════════════════════════════════
MRO 顺序不是风格问题
═══════════════════════════════════════════════════════════════════════════════

    class HubUsecase(SweepMixin, RpcMixin, ShardMixin, HubUsecaseBase)

Python 的方法解析按**声明顺序从左到右**,先找到谁就用谁。这里的顺序编码了一条
事实:**同名方法只允许存在一份**,顺序只是"谁负责实现"的声明,不是"谁覆盖谁"。

  - `add_shard_member` / `remove_shard_member` 由 `SweepMixin` 实现(Go hub.go
    3958/3964),`ShardMixin.replace_assignment_saga` 与 `RpcMixin.assign_hub`
    都只调用。放在最左侧,读者一眼能确认调用落到哪。
  - `ShardMixin` 放在 `RpcMixin` 之后,是因为 RPC 是入口、分片辅助是被调方;
    真出现同名(不该发生)时,入口层的语义优先,而两批的**补偿语义完全不同**
    (saga 的 CAS loser 要退座,drain 迁移的 CAS loser 不重试)——
    静默取错一份不会报错,只会在故障注入时才暴露成座位泄漏或误踢。
  - `HubUsecaseBase` 必须**最右**:它持有 `__init__` 与全部字段,mixin 们都假设
    这些字段已存在。放前面会让 `super().__init__` 链断在 mixin 上。

`tests/test_hub_allocator_biz.py` 之外,`test_hub_allocator_biz_sweep.py` 也对
`__mro__` 做了断言 —— 这四个类的相对顺序被改动时必须有人看见。

═══════════════════════════════════════════════════════════════════════════════
re-export 的边界
═══════════════════════════════════════════════════════════════════════════════

只 re-export **service / main 层真正需要**的名字:出参结构、注入用 Protocol、
会出现在日志 / 错误分流里的常量、以及少量无 receiver 的纯函数。
分片选择的内部细节(census / seat / saga 辅助)**不**外露 —— 外露就会被上层拿去
重新实现一遍判定,而那正是 §9.6「派生判定只有一份」要防的事。
"""

from __future__ import annotations

from pandorapy.services.hub_allocator.biz_base import (
    COMMAND_DRAIN,
    COMMAND_NONE,
    COMMAND_STOP,
    MIGRATE_REASON_CONSOLIDATION,
    PRESENCE_REFRESH_TIMEOUT_SEC,
    REASON_ALL_CANDIDATES_RESERVE_REJECTED,
    REASON_ALL_SHARDS_DRAINING,
    REASON_ALL_SHARDS_FULL,
    REASON_ALL_SHARDS_TRACK_INVALID,
    REASON_ALL_SHARDS_WARMING,
    REASON_ASSIGN_CAS_EXHAUSTED,
    REASON_CANARY_NO_CAPACITY,
    REASON_CANDIDATES_VANISHED,
    REASON_INVALID_RELEASE_TRACK,
    REASON_LIST_SHARDS_FAILED,
    REASON_NO_SHARD_CANDIDATE,
    REASON_NO_SHARD_IN_REGION,
    REASON_NO_SHARD_IN_RELEASE_TRACK,
    REASON_NO_SHARD_MIRROR,
    REASON_OWNER_BARRIER_NOT_OPEN,
    REASON_OWNER_POINTS_ELSEWHERE,
    REASON_RELEASE_CAS_EXHAUSTED,
    REASON_RESERVE_SEAT_ERROR,
    REASON_SEED_TOKEN_NOT_READY,
    REASON_SHARD_HEARTBEAT_TIMEOUT,
    REASON_TEAM_SHARD_LOOKUP_FAILED,
    REASON_TRANSFER_CAS_EXHAUSTED,
    REASON_WRITER_LEASE_NOT_HELD,
    STATE_DRAINING,
    STATE_READY,
    STATE_STOPPING,
    STATE_WARMING,
    AssignResult,
    HubCredential,
    HubMigratePusher,
    HubTicketBinding,
    HubUsecaseBase,
    TicketSigner,
)
from pandorapy.services.hub_allocator.biz_rpc import (
    ASSIGNMENT_CAS_ATTEMPTS,
    MAX_HUB_EVICTION_ORDERS_PER_HEARTBEAT,
    AcknowledgeAdmissionResult,
    AcknowledgeDepartureResult,
    HeartbeatResult,
    HubEvictionOrder,
    HubLineView,
    RpcMixin,
    TransferResult,
    TransferToLineResult,
    assignment_matches_admission,
    build_hub_lines,
    build_hub_lines_for_track,
    hub_credential_identity,
    line_no_of_shard,
)
from pandorapy.services.hub_allocator.biz_shard import (
    LocalHubCredentialSource,
    PreparedHubTicket,
    ShardMixin,
    assignment_binding_v2_complete,
    authoritative_shard,
    effective_role_id,
    least_loaded,
    no_routable_shard_reason,
    select_transfer_target,
    sticky_release_track,
    sticky_release_track_or_none,
    ticket_binding_from_assignment,
)
from pandorapy.services.hub_allocator.biz_sweep import (
    SWEEP_TASK_NAME,
    SweepMixin,
    sum_players,
)


class HubUsecase(SweepMixin, RpcMixin, ShardMixin, HubUsecaseBase):
    """hub_allocator 的完整业务层。对应 Go 的 `biz.HubUsecase`。

    构造与注入全部继承自 `HubUsecaseBase`(`__init__(repo, fleet, signer, cfg)`
    + `set_*` 系列),本类**刻意为空**:任何在这里补的方法都会绕过"某个 mixin 才是
    该职责唯一实现"的约定,并在 MRO 里制造一份找不到来源的覆盖。
    """


__all__ = [
    # ── 组装体 ────────────────────────────────────────────────────────────
    "HubUsecase",
    "HubUsecaseBase",
    "RpcMixin",
    "ShardMixin",
    "SweepMixin",
    # ── 注入用 Protocol / 结构 ────────────────────────────────────────────
    "HubCredential",
    "HubMigratePusher",
    "HubTicketBinding",
    "LocalHubCredentialSource",
    "PreparedHubTicket",
    "TicketSigner",
    # ── RPC 出参 ──────────────────────────────────────────────────────────
    "AcknowledgeAdmissionResult",
    "AcknowledgeDepartureResult",
    "AssignResult",
    "HeartbeatResult",
    "HubEvictionOrder",
    "HubLineView",
    "TransferResult",
    "TransferToLineResult",
    # ── 分片状态 / 心跳指令 / 迁移原因 ────────────────────────────────────
    "COMMAND_DRAIN",
    "COMMAND_NONE",
    "COMMAND_STOP",
    "MIGRATE_REASON_CONSOLIDATION",
    "STATE_DRAINING",
    "STATE_READY",
    "STATE_STOPPING",
    "STATE_WARMING",
    # ── 结构化拒绝原因(日志 / 告警按此分流,禁止在上层另写字面量)────────
    "REASON_ALL_CANDIDATES_RESERVE_REJECTED",
    "REASON_ALL_SHARDS_DRAINING",
    "REASON_ALL_SHARDS_FULL",
    "REASON_ALL_SHARDS_TRACK_INVALID",
    "REASON_ALL_SHARDS_WARMING",
    "REASON_ASSIGN_CAS_EXHAUSTED",
    "REASON_CANARY_NO_CAPACITY",
    "REASON_CANDIDATES_VANISHED",
    "REASON_INVALID_RELEASE_TRACK",
    "REASON_LIST_SHARDS_FAILED",
    "REASON_NO_SHARD_CANDIDATE",
    "REASON_NO_SHARD_IN_REGION",
    "REASON_NO_SHARD_IN_RELEASE_TRACK",
    "REASON_NO_SHARD_MIRROR",
    "REASON_OWNER_BARRIER_NOT_OPEN",
    "REASON_OWNER_POINTS_ELSEWHERE",
    "REASON_RELEASE_CAS_EXHAUSTED",
    "REASON_RESERVE_SEAT_ERROR",
    "REASON_SEED_TOKEN_NOT_READY",
    "REASON_SHARD_HEARTBEAT_TIMEOUT",
    "REASON_TEAM_SHARD_LOOKUP_FAILED",
    "REASON_TRANSFER_CAS_EXHAUSTED",
    "REASON_WRITER_LEASE_NOT_HELD",
    # ── 机械上限 / 超时预算(压测与告警阈值引用同一份)────────────────────
    "ASSIGNMENT_CAS_ATTEMPTS",
    "MAX_HUB_EVICTION_ORDERS_PER_HEARTBEAT",
    "PRESENCE_REFRESH_TIMEOUT_SEC",
    "SWEEP_TASK_NAME",
    # ── 无 receiver 的纯函数(service 层组装出参 / 校验入参时复用)────────
    "assignment_binding_v2_complete",
    "assignment_matches_admission",
    "authoritative_shard",
    "build_hub_lines",
    "build_hub_lines_for_track",
    "effective_role_id",
    "hub_credential_identity",
    "least_loaded",
    "line_no_of_shard",
    "no_routable_shard_reason",
    "select_transfer_target",
    "sticky_release_track",
    "sticky_release_track_or_none",
    "sum_players",
    "ticket_binding_from_assignment",
]
