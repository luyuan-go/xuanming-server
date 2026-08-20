"""ds_allocator 业务层 **分配主链** —— 对应 Go 侧
`services/battle/ds_allocator/internal/biz/allocator.go` 第 547–1195 行。

本模块提供 `AllocateMixin`(Go 里带 `(u *AllocatorUsecase)` receiver 的那批)与一组
模块级纯函数(Go 里没有 receiver 的那批)。最终组装:

    class AllocatorUsecase(SweepMixin, HeartbeatMixin, ReleaseMixin,
                           AllocateMixin, AllocatorUsecaseBase): ...

字段(`self.repo` / `self.auth_repo` / `self.alloc` / `self.cfg` / `self.model_b` /
`self.owner_auth` / TTL accessor …)与 `allocate_result_from_battle` /
`owner_target_from_allocate_result` / 全部 `REASON_*` / `STATE_*` 常量都来自
`biz_base.AllocatorUsecaseBase`,本文件**一个都不重复实现** —— 重复实现会在 MRO 里
制造"两份同名符号、谁在前谁赢"的静默分叉。

═══════════════════════════════════════════════════════════════════════════════
这条链错了会怎样(为什么它单独成一个模块)
═══════════════════════════════════════════════════════════════════════════════

  §9.1 / §9.22(一人一 DS)
      claim 的单键 `SET NX` 是并发 `AllocateBattle` 的**线性化点**:只有持有本次
      `allocation_id` 的赢家才允许调用外部 Agones。输家(`await_existing_allocation`)
      只观察同一份权威记录,**绝不**再分配第二个 Pod、绝不清理不属于自己的记录。

  §9.20 / §9.19(玩家不卡死)
      每条拒绝分支都必须带 `reason` 打点。这批 `REASON_ALLOC_*` /
      `REASON_AWAIT_*` 就是"进不去副本"的根因分档:没有可用 GameServer(要扩容)、
      控制面调用失败(要查 k8s)、上一次回收未确认(等 sweep 收敛)三者的处置完全
      不同,合并成一句"分配失败"等于把排障入口堵死。

  §9.21(金丝雀轨道粘滞)
      `desired_release_track` 只是**意图**;落库的必须是编排层回读到的
      `actual_release_track`。轨道非法一律回收已分配的 pod,不允许把一个轨道不明的
      实例交给 matchmaker。

  Model B 的"结果未知即永久墓碑"
      外部 GSA POST 前先把 claim CAS 成 `allocation_uncertain` 并去掉 TTL。POST 之后
      的任何失败(transport / 解析 / 严格 GET / finalize)都**保持** uncertain:
      不自动 Release、不恢复 TTL、不删 claim。代价是需要显式对账,收益是同一个 match
      永远不会发出第二次 POST。

═══════════════════════════════════════════════════════════════════════════════
与 Go 的形变(其余逐行同构,逐条都有理由)
═══════════════════════════════════════════════════════════════════════════════

  1. **`ctx` 不存在**。请求上下文不进 biz 层(与 `biz_base` 同一约定);Go 里靠
     `plog.With(ctx)` 带出的 trace 字段由 Python 侧的 logging 中间件负责。

  2. `(值, error)` → 返回值 + 抛异常。**唯一例外**是 Go 用 bool 表达控制流的
     `ClaimBattle` 的 `(claimed, existing)`,照抄二元组(`repo.claim_battle` 已是
     这个形状)。

  3. `errors.Is(err, errReadyWaitTimeout)` → `_is_ready_wait_timeout(exc)`;
     `errors.Is(err, errBattleWaitOwnershipLost)` → `_is_battle_wait_ownership_lost(exc)`。
     两者都**沿 `PandoraError.cause` 链**判定,不是只看最外层 —— `errcode.NewCause`
     的语义就是"外层 code 给客户端、内层 cause 给控制流",只看最外层等于把这个区分
     丢掉,而 ownership-lost 一旦被当成普通失败,owner 就会与在途的 sweep 回收链并发
     跑第二路 `fence_preactive_release` / `release_expected`(那两个只有幂等最终一致
     保证,**没有单次调用保证**)。

  4. 错误消息里的 `%v` 一律改写成 `%s`。Python 的 `%` 格式化没有 `%v`,写了会在
     **失败路径上**再抛一个 ValueError —— 把一次可诊断的业务失败变成一条毫不相干
     的格式化异常。

  5. `time.Now().UnixMilli()` → `now_ms()`(**复用** `battle_auth` 的那一个,与
     `biz_base` 同源;测试冻结时间时 monkeypatch **本模块**的 `now_ms`)。

  6. Go 的 type assertion(`u.alloc.(localBattleRosterSink)` /
     `u.alloc.(localInstanceIdentitySource)`)→ `isinstance` + `gameserver.py` 里
     已声明的 `runtime_checkable` Protocol。**不新建一份 Protocol**:那两个是生产
     隔离的机械门(Agones / Mock 不实现 → legacy 分支在生产恒不可达),两份定义
     漂移时"生产上这条分支不可达"就从结构性事实降级成一句注释。

  7. `u.releasePolicy.Select(matchID)`:Go 的 `releasetrack.Policy` 是值类型、零值
     (`percent=0`)恒回 stable,所以那边不判空;Python 的 `release_policy` 允许为
     None(dev / 未注入),由 `_desired_release_track` 收在一处并回落 **stable** ——
     回落 canary 等于把全服灌进未验证轨道,正是 §9.21 明令禁止的方向。

  8. `int32` / `uint32` / `uint64` 的边界:Go 由类型系统免费获得,Python 整数不回绕,
     故在入口显式判。不判的话越界值会一路带到 protobuf 序列化才抛一个既没有业务码、
     也说不清是哪个字段的 ValueError。

  ※ 本段范围内**没有** `source_revision` 闸,也**没有** `writer_token` 水位比较
    (前者是 hub_allocator 的写者租约铸号链,后者是 hub 分片单写者门)。这里不凭空
    造一个 —— `owner_target_from_allocate_result` 刻意把 `source_revision` 留 0 的
    理由见 `biz_base` 该函数的注释。

═══════════════════════════════════════════════════════════════════════════════
本模块**依赖但尚未移植**的 mixin 方法(移植者必须按此契约实现)
═══════════════════════════════════════════════════════════════════════════════

    async def wait_battle_ready(match_id, pod_name, allocation_id) -> AllocateResult
        轮询 Redis 镜像直到 DS 心跳确认 ready。超时抛 `ReadyWaitTimeoutError`;
        回收/接管所有权已属他人时抛的错误其 `cause` 链上带
        `BattleWaitOwnershipLostError`。Go: `waitBattleReady`(allocator.go:1195+)。

    async def fail_ready_wait_timeout(match_id, allocation_id, pod_name,
                                      authoritative, owned) -> BaseException
        ★ **返回**要抛的异常,自己不抛 —— 与 Go 的
          `return nil, u.failReadyWaitTimeout(...)` 同形。做成"自己抛"会让调用点
          必须在后面补一句永远执行不到的 `raise`,静态检查与人眼都看不出真实控制流。

    async def cleanup_allocated_battle(match_id, allocation_id, pod_name, authoritative)
        回收 pod + 按 allocation_id 条件删镜像。best-effort,不抛。
        Go: `cleanupAllocatedBattle`。
"""

from __future__ import annotations

import asyncio
import hmac
import uuid
from typing import Any

from pandora.ds.v1 import allocator_pb2 as dspb

from pandorapy import dsmetadata, errcode, godur, releasetrack
from pandorapy import log as plog
from pandorapy.services.ds_allocator.battle_auth import (
    BATTLE_DS_WRITER_EPOCH_V2,
    BattleAuthorityBinding,
    BattleAuthoritySnapshot,
    BattleStageInput,
    now_ms,
)
from pandorapy.services.ds_allocator.biz_base import (
    REASON_ALLOC_CLAIM_FAILED,
    REASON_ALLOC_FACTIONS_INVALID,
    REASON_ALLOC_FINALIZE_LOST,
    REASON_ALLOC_MATCH_ID_REQUIRED,
    REASON_ALLOC_ROSTER_INVALID,
    REASON_ALLOC_TRACK_INVALID,
    REASON_AWAIT_CLAIM_MISSING,
    REASON_AWAIT_RELEASE_UNCONFIRMED,
    STATE_ALLOCATING,
    STATE_ALLOCATION_ABORT,
    STATE_ALLOCATION_RECONCILING,
    STATE_ALLOCATION_UNCERTAIN,
    STATE_PREACTIVE_RELEASING,
    STATE_READY,
    STATE_RUNNING,
    STATE_WARMING,
    AllocateResult,
    BattleWaitOwnershipLostError,
    ReadyWaitTimeoutError,
    allocate_result_from_battle,
    owner_target_from_allocate_result,
)
from pandorapy.services.ds_allocator.gameserver import (
    LocalBattleRosterSink,
    LocalInstanceIdentitySource,
)
from pandorapy.services.ds_allocator.owner_authority import (
    OWNER_TYPE_BATTLE,
    is_owner_begin_outcome_unknown,
    owner_begin_players,
    owner_verify_players_exact,
)

__all__ = [
    "BATTLE_INSTANCE_EPOCH_KEY",
    "BATTLE_INSTANCE_UID_ANNOTATION_KEY",
    "BATTLE_TOKEN_ANNOTATION_KEY",
    "BATTLE_TOKEN_EXP_ANNOTATION_KEY",
    "BATTLE_TOKEN_GEN_ANNOTATION_KEY",
    "BATTLE_TOKEN_HASH_KEY",
    "BATTLE_TOKEN_JTI_ANNOTATION_KEY",
    "BATTLE_TOKEN_KID_KEY",
    "BATTLE_WRITER_EPOCH_KEY",
    "OWNER_BEGIN_BUDGET_SEC",
    "OWNER_VERIFY_BUDGET_SEC",
    "AllocateMixin",
    "battle_pending_delivered",
    "battle_ready_for_pod",
    "battle_wait_state_progressable",
    "clone_combat_faction_records",
    "combat_faction_map_from_records",
    "combat_faction_records",
    "same_battle_allocation_request",
]

# ── 无符号 / 有符号边界(Go 由类型系统免费获得)────────────────────────────────

_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1

#: `cause` 链的最大追溯深度。纯防环 / 防病态深链,正常链路只有 1~2 层。
#: 与 `owner_authority._CAUSE_CHAIN_LIMIT` 同值同因。
_CAUSE_CHAIN_LIMIT = 16

# ── owner 归属定案 / 校验的时间预算 ──────────────────────────────────────────
#
# Go 两处都写字面量 `5*time.Second`。抄成常量而不是就地写 5.0,是因为改动其一时
# 两条路径的语义会静默分叉:交付前的 Begin 与 claim loser 的只读 Verify 必须共享
# 同一个上界,否则 loser 可能在 winner 还没写完时就判"归属不 exact"。
#
# 预算的来历(照抄 Go 注释):一局最多 10 人,每人单轮 Query+Begin;EPOCH_CONFLICT
# fail-closed 交外层重走分配链并重读 allocation。串行执行,按本地 TiDB 往返量级留足余量。

#: 交付 READY 前逐玩家 `Begin(BATTLE)` 的总预算(秒)。Go: `5*time.Second`。
OWNER_BEGIN_BUDGET_SEC = 5.0

#: claim loser / 幂等重试交付 READY 前的**只读** exact 校验总预算(秒)。Go: 同上。
OWNER_VERIFY_BUDGET_SEC = 5.0

# ── Model B 凭据投递的 annotation key(Go 的 battleToken*AnnotationKey 一族)──────
#
# ★ 逐字符照抄。这九个串是**跨进程契约**:ds_allocator 写、UE DS 的
#   `ApplyAgonesAdmissionMetadata` 读。错一个字母的表现不是报错,而是 DS 读不到
#   凭据 → PreLogin 一律 fail-closed 拒票 → 玩家连上就被踢,而后端这边"分配成功"。

BATTLE_TOKEN_ANNOTATION_KEY = "pandora.dev/ds-token"
BATTLE_TOKEN_EXP_ANNOTATION_KEY = "pandora.dev/ds-token-exp-ms"
BATTLE_TOKEN_GEN_ANNOTATION_KEY = "pandora.dev/ds-token-gen"
BATTLE_TOKEN_JTI_ANNOTATION_KEY = "pandora.dev/ds-token-jti"
BATTLE_INSTANCE_UID_ANNOTATION_KEY = "pandora.dev/ds-instance-uid"
BATTLE_INSTANCE_EPOCH_KEY = "pandora.dev/ds-instance-epoch"
BATTLE_WRITER_EPOCH_KEY = "pandora.dev/ds-writer-epoch"
BATTLE_TOKEN_KID_KEY = "pandora.dev/ds-token-kid"
BATTLE_TOKEN_HASH_KEY = "pandora.dev/ds-token-sha256"


# ── 哨兵识别(Go 的 errors.Is)──────────────────────────────────────────────


def _cause_chain_has(exc: BaseException | None, kind: type[BaseException]) -> bool:
    """沿 `PandoraError.cause` 链判定异常族。对应 Go 的 `errors.Is(err, sentinel)`。

    ★ 必须走链而不是只看最外层:`errcode.NewCause` 把"外层 code 给客户端、内层
      cause 给控制流"分开了,只看最外层等于把这个区分丢掉。
    """
    seen = 0
    cur = exc
    while cur is not None and seen < _CAUSE_CHAIN_LIMIT:
        if isinstance(cur, kind):
            return True
        cur = getattr(cur, "cause", None)
        seen += 1
    return False


def _is_ready_wait_timeout(exc: BaseException | None) -> bool:
    """Go: `errors.Is(werr, errReadyWaitTimeout)`。"""
    return _cause_chain_has(exc, ReadyWaitTimeoutError)


def _is_battle_wait_ownership_lost(exc: BaseException | None) -> bool:
    """Go: `errors.Is(werr, errBattleWaitOwnershipLost)`。

    ★ 判成 False 的后果不是"少一条日志":owner 会去跑 `cleanup_allocated_battle`,
      与 sweep 在途的 fenced 回收链并发第二路 `fence_preactive_release` /
      `release_expected` —— 那两个只有幂等最终一致保证,没有单次调用保证。
    """
    return _cause_chain_has(exc, BattleWaitOwnershipLostError)


# ── 模块级纯函数(Go 里没有 receiver 的那批)──────────────────────────────────


def combat_faction_records(
    canonical_players: list[int],
    combat_faction_by_player: dict[int, int] | None,
) -> list[Any] | None:
    """roster + 阵营映射 → **按 canonical roster 顺序**的 proto 记录列表。

    对应 Go 的 `combatFactionRecords`。

    ★ 顺序取自 `canonical_players`(升序去重后的 roster),**不是** dict 的插入序。
      Python dict 保插入序,但那不是排序:直接 iterate 映射会得到一个随调用方构造
      顺序变化的列表,与 Go 侧对同一批玩家产出的记录不一致 ——
      而 `same_battle_allocation_request` 是**逐下标**比对这个列表的,顺序一变,
      同一次分配的 ACK-loss 重试会被判成"快照冲突"直接分配失败。
    """
    if not combat_faction_by_player:
        return None
    return [
        dspb.BattlePlayerCombatFaction(
            player_id=player_id,
            combat_faction_id=combat_faction_by_player[player_id],
        )
        for player_id in canonical_players
    ]


def clone_combat_faction_records(records: list[Any] | None) -> list[Any] | None:
    """深拷贝阵营记录列表。对应 Go 的 `cloneCombatFactionRecords`。

    ★ 为什么不是 `list(records)`:proto message 是**可变**对象,浅拷贝后 claim 记录
      与 battle 记录会共享同一批子消息。任何一侧被改(哪怕只是 protobuf 内部把它
      挂进另一个父消息)都会波及另一侧,而两者是要分别落进 Redis 两个不同状态的
      快照 —— 漂移在读回来之前完全不可见。
    """
    if not records:
        return None
    return [
        dspb.BattlePlayerCombatFaction(
            player_id=record.player_id,
            combat_faction_id=record.combat_faction_id,
        )
        for record in records
    ]


def combat_faction_map_from_records(
    player_ids: list[int],
    records: list[Any] | None,
) -> dict[int, int] | None:
    """已落盘的阵营记录 → match-local 映射(带完整重校验)。

    对应 Go 的 `combatFactionMapFromRecords`。

    Returns:
        `None` = 该局没有阵营快照(滚动升级中的旧 matchmaker 写的记录)。

    Raises:
        ValueError: 记录与 canonical roster 对不上(条数 / 顺序 / 越界)。

    ★ 三层判据缺一不可,而且**必须重校验**而不是信任落盘值:这批字节可能是旧版本
      副本写的,也可能被运维手工改过。解不出权威阵营时 DS 侧 `ResolveCampForSpawn`
      会 RejectSpawn **且禁止回退默认 Pawn** —— 玩家进了图但根本没有角色。
    """
    if not records:
        return None
    ids = list(player_ids)
    try:
        canonical_players, _ = dsmetadata.canonical_roster(ids)
    except ValueError as exc:
        # Go 把 `err != nil` 与另外两条判据收敛进同一个 if、返回同一句话。这里保留
        # 原因链(`from exc`),但对外消息逐字与 Go 相同 —— 上层按消息聚合。
        raise ValueError(
            "combat faction records must align with canonical battle roster"
        ) from exc
    if ids != canonical_players or len(records) != len(ids):
        raise ValueError("combat faction records must align with canonical battle roster")
    factions: dict[int, int] = {}
    for i, record in enumerate(records):
        if record is None or record.player_id != ids[i]:
            raise ValueError("combat faction records are not in canonical roster order")
        if record.combat_faction_id > dsmetadata.MAX_COMBAT_FACTION_ID:
            raise ValueError("combat faction_id exceeds DS camp range")
        factions[record.player_id] = record.combat_faction_id
    # 最后再过一遍权威校验器:上面三条是"记录自洽",这一条是"与 roster 一一映射"。
    # 少了它,一个把两个玩家写成同一 player_id 的记录列表能通过前面全部判据。
    dsmetadata.canonical_combat_factions(canonical_players, factions)
    return factions


def same_battle_allocation_request(existing: Any, expected: Any) -> bool:
    """同 match 的重试请求与已落定的 claim 是否**同一次分配**。

    对应 Go 的 `sameBattleAllocationRequest`。

    刻意**不比** `rating_mode`(照抄 Go 注释,这不是省事):
      ① 它不是身份 / 安全字段(身份是 roster + map_id + game_mode,三者都比了),
         而是随 map_id 确定的本局元数据;
      ② 共存窗口里旧 matchmaker 不带本字段、新 matchmaker 带,同一 match 的
         ACK-loss 重试会让该值从 0 变成显式值 —— 把它纳入比对会把正常重试判成
         "快照冲突"直接分配失败,玩家进不去场景(§9.21 / §9.20)。
    落定口径按 claim 赢家:后到的重试不改已定格的值。
    """
    if (
        existing is None
        or expected is None
        or existing.match_id != expected.match_id
        or existing.map_id != expected.map_id
        or existing.game_mode != expected.game_mode
        or list(existing.player_ids) != list(expected.player_ids)
    ):
        return False
    a = existing.player_combat_factions
    b = expected.player_combat_factions
    if len(a) != len(b):
        return False
    for x, y in zip(a, b, strict=True):
        if x.player_id != y.player_id or x.combat_faction_id != y.combat_faction_id:
            return False
    return True


def battle_pending_delivered(
    snapshot: BattleAuthoritySnapshot,
    allocation_id: str,
    expected: Any,
    rv: str,
) -> bool:
    """Redis 响应不确定时,权威 read-back 是否证明本轮 pending 已 delivered。

    对应 Go 的 `battlePendingDelivered`。

    ★ 它是 `mark_delivered` 响应丢失后的**唯一**成功依据:不以本地 expected、
      也不以 K8s 镜像猜测成功。少一项比较(尤其 `delivered_rv`)就会把"另一轮
      投递的结果"当成本轮成功,于是本轮 pending 凭据永远停在未投递态,DS 拿不到
      可用令牌 —— 而后端认为分配已完成。
    """
    auth = snapshot.auth
    if not snapshot.auth_found or auth is None or expected is None:
        return False
    pending = auth.pending
    return (
        auth.allocation_id == allocation_id
        and auth.delivered_rv == rv
        and pending.gen == expected.gen
        and pending.jti == expected.jti
        and pending.exp_ms == expected.exp_ms
        and pending.kid == expected.kid
        and pending.instance_uid == expected.instance_uid
        and pending.instance_epoch == expected.instance_epoch
        # ★ token 摘要用 `hmac.compare_digest`:它是令牌本体的 sha256,属加密材料
        #   的等值比较。两侧都是十六进制 ASCII 串,结果与 `==` 逐位相同,只是不再
        #   按前缀提前短路。
        and hmac.compare_digest(pending.token_sha256, expected.token_sha256)
        and pending.writer_epoch == expected.writer_epoch
    )


def battle_ready_for_pod(
    b: Any, pod_name: str, match_id: int, allocated_at_ms: int
) -> bool:
    """DS 是否已用 Heartbeat 确认 ready。对应 Go 的 `battleReadyForPod`。

    四条同时成立:pod/match 对得上、有**分配之后**的真实心跳
    (`last_heartbeat_ms` 严格大于 `allocated_at_ms`)、状态进入 ready 或 running。

    ★ "严格大于"不是笔误:`finalize` 时把 `last_heartbeat_ms` 初始化成了
      `allocated_at_ms`(仅作 sweep 宽限基准)。写成 `>=` 会让一台**从未心跳**的
      warming 实例立刻被判成 ready,ds_addr 直接回给 matchmaker —— 玩家连上一个
      还没读到 match-id 的 DS,被 PreLogin 拒票。

    当前 UE 侧上报的是 running(不一定先发 ready),所以后端先把 running 也视为
    可进入状态。
    """
    return (
        b is not None
        and b.match_id == match_id
        and b.ds_pod_name == pod_name
        and b.last_heartbeat_ms > allocated_at_ms
        and (b.state == STATE_READY or b.state == STATE_RUNNING)
    )


def battle_wait_state_progressable(state: str) -> bool:
    """wait 可继续前进的状态**白名单**。对应 Go 的 `battleWaitStateProgressable`。

    allocating/warming 是在途,ready/running 即将命中 ready 判定。其余一切状态
    (ended/abandoned、preactive_release_pending、allocation_abort_pending、
    allocation_uncertain/reconciling/empty_fence、**以及任何未知新状态**)都已由
    回收 / 对账链接管或属 fail-closed 墓碑 —— 继续等待只会白耗 ready_wait,
    owner 也绝不能对其 cleanup。

    ★ 必须是白名单而不是黑名单:滚动升级里新版本会引入新状态字面量,黑名单对它们
      默认"可以继续等",于是旧副本会去等一个永远不会推进的墓碑,并在超时后对一条
      不归它管的记录执行清理。
    """
    return state in (STATE_ALLOCATING, STATE_WARMING, STATE_READY, STATE_RUNNING)


# ── mixin ────────────────────────────────────────────────────────────────────


class AllocateMixin:
    """`AllocatorUsecase` 的分配主链。对应 Go allocator.go 547–1195 行。

    ★ 刻意**不**加 `__slots__`(与 `AllocatorUsecaseBase` 同因):多 mixin 叠加时
      slots 布局要在每层重复声明,漏一层就静默退回 `__dict__`,制造"以为有约束
      其实没有"的假象。
    """

    # 下面这些由 AllocatorUsecaseBase 提供,这里只声明给读者看:
    #   repo / alloc / cfg / model_b / auth_repo / authoritative_alloc / ds_signer
    #   ds_credential_ttl_sec / release_policy / allocation_ledger / owner_auth
    #   battle_ttl_sec() / ready_wait_timeout_sec() / heartbeat_timeout_ms()
    # 这几个由尚未移植的 ReleaseMixin 提供(契约见模块头):
    #   wait_battle_ready() / fail_ready_wait_timeout() / cleanup_allocated_battle()

    # ── 小工具 ───────────────────────────────────────────────────────────

    def _desired_release_track(self, match_id: int) -> str:
        """本次分配的**意图**轨道。对应 Go 的 `u.releasePolicy.Select(matchID)`。

        Go 的 `releasetrack.Policy` 是值类型,零值(`percent=0`)恒回 stable,所以
        那边不需要判空。Python 的 `Policy` 没有默认字段、`release_policy` 允许为
        None(dev / 未注入),这里回落 stable —— 与 Go 零值行为逐字节相同。

        ★ 回落方向只能是 stable。回落 canary = 未注入策略的部署把**全部对局**灌进
          未验证轨道,而这恰恰是 §9.21 明令禁止的方向。
        ★ 返回值只是意图,不是事实:编排层可能因 canary 无容量回退 stable,落库的
          必须是回读到的 `actual_release_track`。
        """
        if self.release_policy is None:
            return releasetrack.STABLE
        return self.release_policy.select(match_id)

    # ── RPC 1:AllocateBattleWithCombatFactions ───────────────────────────

    async def allocate_battle_with_combat_factions(  # noqa: C901,PLR0912,PLR0915,PLR0913 —— 与 Go 同形状,拆开就对不上行
        self,
        match_id: int,
        player_ids: list[int],
        combat_faction_by_player: dict[int, int] | None,
        map_id: int,
        game_mode: str,
        rating_mode: int,
        rating_pool: str,
    ) -> AllocateResult:
        """为 match 申请战斗 DS,并持久化完整的 match-local 玩家阵营快照。

        对应 Go 的 `AllocateBattleWithCombatFactions`。空映射仅兼容滚动升级中的旧
        matchmaker;非空映射必须**精确覆盖** roster。

        `rating_mode` 是 matchmaker 成局时按 map_id 从关卡表定格的「本局算不算段位」。
        本服务**只原样存进 canonical `BattleStorageRecord`,不解释、不校验、不下发
        DS**:判定权在 battle_result(§9.6 数值不信 DS,派生数值只在结算服务算)。
        0=UNSPECIFIED 是合法值(旧 matchmaker / 旧批次表),结算侧按旧口径兜底(§9.21)。

        Returns:
            已就绪实例的分配投影(非 None)。

        Raises:
            errcode.PandoraError(ErrInvalidArg): match_id / roster / 阵营快照非法。
            errcode.PandoraError(ErrUnavailable): Model B 的"结果未知"永久墓碑
                (claim 保持 uncertain,同 match 后续重试一律 fail-closed)。
            errcode.PandoraError(ErrDSAllocationFailed): 编排层分配失败 / 轨道非法 /
                claim 已不属本次分配 / 凭据投递失败。
            其它: `wait_battle_ready` 与 owner Begin 的错误原样上抛。
        """
        if match_id == 0 or match_id < 0 or match_id > _UINT64_MAX:
            # ★ 越界与 0 走同一条拒绝路径:Go 的 uint64 形参使越界在编译期就不可能
            #   发生。match_id 是 Redis key 的 hashtag 内容,越界值会算出一个谁也
            #   读不到的 slot,表现为"分配了但没人找得到这局"。
            plog.get().warning(
                "battle_allocate_refused", reason=REASON_ALLOC_MATCH_ID_REQUIRED
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "match_id required")
        if map_id < 0 or map_id > _UINT32_MAX:
            # Go 的形参是 uint32,这里是它在 Python 侧唯一的等价物。刻意**不打**
            # `battle_allocate_refused` 日志:那条日志的 reason 是一份跨语言固定词表
            # (运维照着建 Loki 查询),不能为一个 Go 侧不可能发生的分支新造一个词。
            raise errcode.PandoraError(errcode.ErrInvalidArg, "map_id out of uint32 range")
        try:
            canonical_players, _ = dsmetadata.canonical_roster(list(player_ids))
        except ValueError as roster_err:
            plog.get().warning(
                "battle_allocate_refused",
                reason=REASON_ALLOC_ROSTER_INVALID,
                match_id=match_id,
                players=len(player_ids),
                err=str(roster_err),
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "invalid battle roster: %s", roster_err
            ) from roster_err
        players = canonical_players
        combat_factions: list[Any] | None = None
        if combat_faction_by_player:
            try:
                canonical_faction_players, _ = dsmetadata.canonical_combat_factions(
                    players, combat_faction_by_player
                )
            except ValueError as faction_err:
                plog.get().warning(
                    "battle_allocate_refused",
                    reason=REASON_ALLOC_FACTIONS_INVALID,
                    match_id=match_id,
                    players=len(players),
                    factions=len(combat_faction_by_player),
                    err=str(faction_err),
                    hint="阵营快照与 roster 对不上;DS 侧 ResolveCampForSpawn 解不出权威阵营会 RejectSpawn",
                )
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "invalid battle combat factions: %s", faction_err
                ) from faction_err
            players = canonical_faction_players
            combat_factions = combat_faction_records(players, combat_faction_by_player)
            # 脱离调用方 map,避免 RPC 返回前外部复用 / 修改 map 造成 TOCTOU 或数据竞争。
            # 重建的来源是**已校验过的记录列表**,不是原 map —— 少了这一步,一个在
            # await 点被外部改掉的 map 会让 Agones annotation 与 Redis 快照不一致。
            combat_faction_by_player = {
                record.player_id: record.combat_faction_id for record in combat_factions
            }
        else:
            plog.get().warning("battle_combat_factions_legacy_missing", match_id=match_id)
        desired_release_track = self._desired_release_track(match_id)

        # 单 key SET NX claim 是并发 AllocateBattle 的线性化点。只有持有本次
        # allocation_id 的赢家才允许调用外部 Agones;输家只观察同一权威记录,
        # 绝不再分配第二个 Pod。
        claim_at = now_ms()
        allocation_id = str(uuid.uuid4())
        claim = dspb.BattleStorageRecord(
            match_id=match_id,
            state=STATE_ALLOCATING,
            player_ids=list(players),
            map_id=map_id,
            game_mode=game_mode,
            rating_mode=rating_mode,
            rating_pool=rating_pool,
            allocated_at_ms=claim_at,
            last_heartbeat_ms=claim_at,
            player_count=len(players),
            allocation_id=allocation_id,
            player_combat_factions=clone_combat_faction_records(combat_factions),
            # 分配时冻结到齐期限策略代(observe→enforce 激活协议的执行依据,proto 字段注释)。
            roster_policy_generation=self.cfg.roster_policy_generation,
        )
        try:
            claimed, existing = await self.repo.claim_battle(claim, self.battle_ttl_sec())
        except asyncio.CancelledError:
            # ★ 必须紧邻宽 except 之上:取消是停机控制流,不是"claim 写失败"。
            raise
        except BaseException as exc:
            # claim 是整条分配链的第一步。这一步失败时此前一行日志都没有,matchmaker
            # 只看到一个错误码 —— "分配不到 DS"到底是没到 Agones 还是 Agones 没货,
            # 查不出来。
            plog.get().warning(
                "battle_allocate_refused",
                reason=REASON_ALLOC_CLAIM_FAILED,
                match_id=match_id,
                allocation_id=allocation_id,
                players=len(players),
                map_id=map_id,
                err=str(exc),
                hint="还没走到 Agones:Redis 权威写失败,与 Fleet 容量无关",
            )
            raise
        if not claimed:
            if not same_battle_allocation_request(existing, claim):
                plog.get().error(
                    "battle_allocation_idempotency_snapshot_mismatch",
                    match_id=match_id,
                    allocation_id=existing.allocation_id if existing is not None else "",
                )
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "battle %d allocation request conflicts with existing snapshot",
                    match_id,
                )
            return await self.await_existing_allocation(match_id, existing)
        # allocation 台账(孤儿清扫防误删④):claim 赢家在任何外部 GSA POST 前把
        # allocation_id 记入本权威台账;孤儿清扫只准回收「台账能证明出身于本权威」
        # 的 GS。写失败**不阻断分配**(可用性优先)—— 代价只是本次分配若泄漏,清扫
        # 因台账查无而保留不删,方向安全。
        if self.allocation_ledger is not None:
            try:
                await self.allocation_ledger.record_allocation_ledger(allocation_id, claim_at)
            except asyncio.CancelledError:
                raise
            except BaseException as ledger_exc:  # noqa: BLE001 —— 与 Go 的 `lerr != nil` 同宽
                plog.get().warning(
                    "battle_allocation_ledger_record_failed",
                    match_id=match_id,
                    allocation_id=allocation_id,
                    err=str(ledger_exc),
                )

        authoritative: Any = None
        pod_name = ""
        addr = ""
        actual_release_track = desired_release_track
        alloc_err: BaseException | None = None
        if self.model_b:
            # 外部 GSA POST 前先把 claim CAS 成永久 allocation_uncertain。该 fence 是
            # "是否允许 POST"的唯一线性化点;失败 / 响应未知时**绝不能**访问 K8s。
            fenced = False
            fence_exc: BaseException | None = None
            try:
                fenced = await self.repo.fence_battle_allocation(match_id, allocation_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 与 Go 的 `fenceErr != nil` 同宽
                fence_exc = exc
            if fence_exc is not None or not fenced:
                plog.get().error(
                    "gameserver_preallocation_fence_failed",
                    match_id=match_id,
                    allocation_id=allocation_id,
                    fenced=fenced,
                    err=str(fence_exc) if fence_exc is not None else None,
                )
                raise errcode.PandoraError(
                    errcode.ErrUnavailable, "battle %d allocation fence unavailable", match_id
                )
            try:
                authoritative = await self.authoritative_alloc.allocate_authoritative(
                    match_id,
                    allocation_id,
                    list(players),
                    combat_faction_by_player or {},
                    map_id,
                    game_mode,
                    desired_release_track,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 与 Go 的 `err != nil` 同宽
                alloc_err = exc
                # Go 的 `AllocateAuthoritative` 在 POST 结果不可解析时仍
                # `return partial, err`。Python 用异常传播,那个 partial 挂在
                # `BattleAllocationError.allocation` 上 —— **必须**取出来:
                # allocation_id 是未知结果对账 / 回收的唯一 fencing token。
                authoritative = getattr(exc, "allocation", None)
            if authoritative is not None:
                pod_name, addr = authoritative.pod_name, authoritative.addr
        else:
            # legacy 本地面:DS 进程在 allocate 内 exec、env 那一刻定型,所以权威准入
            # 元数据(roster/allocation-id/release-track/combat-factions)必须**先**
            # 登记。生产走 Agones annotation,这里是它在 mode=local 的等价投递通道。
            #
            # 双重隔离:`not self.model_b` + Protocol 断言(Agones / Mock 不实现),
            # 生产恒不进。
            if isinstance(self.alloc, LocalBattleRosterSink):
                await self.alloc.set_pending_battle_roster(
                    match_id,
                    list(players),
                    combat_faction_by_player,
                    allocation_id,
                    desired_release_track,
                )
            try:
                pod_name, addr, actual_release_track = await self.alloc.allocate(
                    match_id, map_id, game_mode, desired_release_track
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 与 Go 的 `err != nil` 同宽
                alloc_err = exc
        if alloc_err is not None:
            # R2:reason 必须能把"Fleet 没有空闲 GameServer"(要扩容)与"控制面调用
            # 失败"(要查 k8s / 网络)分开 —— 两者的处置完全不同,而以前它们共用
            # 一条日志。
            alloc_reason = "control_plane_failed"
            if errcode.as_code(alloc_err) == errcode.ErrDSNoAvailable:
                alloc_reason = "no_available_gameserver"
            plog.get().error(
                "gameserver_allocate_failed",
                match_id=match_id,
                reason=alloc_reason,
                allocation_id=allocation_id,
                map_id=map_id,
                game_mode=game_mode,
                release_track=desired_release_track,
                err=str(alloc_err),
            )
            if self.model_b:
                # POST transport、响应解析、严格 GET 任一步失败都可能对应"已应用但
                # 结果迟到"。永久 uncertain claim 是唯一安全结果:不自动
                # Release/Delete、不恢复 TTL,后续同 match 重试只能 fail-closed,
                # 绝不产生第二次 POST。
                plog.get().error(
                    "gameserver_allocation_uncertain_retained",
                    match_id=match_id,
                    allocation_id=allocation_id,
                    pod=pod_name,
                )
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "battle %d gameserver allocation result uncertain",
                    match_id,
                ) from alloc_err
            await self.cleanup_allocated_battle(
                match_id, allocation_id, pod_name, authoritative
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed, "allocate ds for match %d failed", match_id
            ) from alloc_err
        if self.model_b and (
            authoritative is None
            or authoritative.pod_name == ""
            or authoritative.addr == ""
            or authoritative.instance_uid == ""
            or authoritative.pod_uid == ""
            or authoritative.resource_version == ""
            or authoritative.allocation_id != allocation_id
            or not releasetrack.valid(authoritative.release_track)
        ):
            # data 层虽已做严格 GET,这里仍在副作用边界复核完整身份,防错误实现 /
            # 测试桩以"不抛异常"绕过 UID/RV/allocation_id 确认。保持永久 uncertain,
            # 不做清理。
            plog.get().error(
                "gameserver_authoritative_identity_incomplete",
                match_id=match_id,
                allocation_id=allocation_id,
                allocation=repr(authoritative),
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle %d gameserver identity unavailable", match_id
            )

        now = now_ms()
        if authoritative is not None:
            actual_release_track = authoritative.release_track
        if not releasetrack.valid(actual_release_track):
            plog.get().error(
                "battle_allocate_refused",
                reason=REASON_ALLOC_TRACK_INVALID,
                match_id=match_id,
                allocation_id=allocation_id,
                pod=pod_name,
                desired_track=desired_release_track,
                actual_track=actual_release_track,
                hint="§9.21 灰度轨道粘滞被破坏,已分配的 pod 会被立即回收",
            )
            await self.cleanup_allocated_battle(
                match_id, allocation_id, pod_name, authoritative
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "allocator returned invalid actual release_track %r",
                actual_release_track,
            )
        battle = dspb.BattleStorageRecord(
            match_id=match_id,
            ds_pod_name=pod_name,
            ds_addr=addr,
            # 等 DS 心跳确认 ready 才回 matchmaker;不把 Agones Allocated 当成 ready
            state=STATE_WARMING,
            player_ids=list(players),
            map_id=map_id,
            game_mode=game_mode,
            rating_mode=rating_mode,
            rating_pool=rating_pool,
            allocated_at_ms=now,
            # 仅作 sweep 宽限基准;ready 判定要求 last_heartbeat_ms **严格大于**此值
            # (即必须是一次真实心跳),见 `battle_ready_for_pod`。
            last_heartbeat_ms=now,
            player_count=len(players),
            allocation_id=allocation_id,
            release_track=actual_release_track,
            player_combat_factions=clone_combat_faction_records(combat_factions),
            # 同 claim:策略代随分配冻结,永不改写(回滚 / 再激活靠配置代前进,
            # 不回填存量局)。
            roster_policy_generation=self.cfg.roster_policy_generation,
        )
        if authoritative is not None:
            battle.gameserver_uid = authoritative.instance_uid
            battle.pod_uid = authoritative.pod_uid
        elif not self.model_b:
            # legacy 本地面回填 exact 实例身份(2026-08-04):不回填则 gameserver_uid /
            # instance_epoch 恒零,matchmaker 判「未回填完整 DS 目标」拒签 v2 战斗票、
            # 对局直接 FAILED,玩家永远进不去副本。身份取自拉起该进程时生成、且已签进
            # 其 DS 回调令牌的同一组值,与 DS 自报身份天然相等。
            #
            # 双重机械隔离(与 hub 侧 local_ticket_binding 同手法):
            #   ① `not self.model_b`;
            #   ② Protocol 断言 —— Agones 分配器不实现 `LocalInstanceIdentitySource`,
            #      生产恒 False。
            # 任一门不过都保持旧行为(留零值),Agones 路径零变更。
            if isinstance(self.alloc, LocalInstanceIdentitySource):
                identity = self.alloc.local_instance_identity(pod_name)
                if identity is not None:
                    battle.gameserver_uid, battle.instance_epoch = identity
        finalized = False
        finalize_exc: BaseException | None = None
        try:
            if self.model_b:
                finalized = await self.repo.finalize_fenced_battle_allocation(
                    battle, self.battle_ttl_sec()
                )
            else:
                finalized = await self.repo.finalize_battle_allocation(
                    battle, self.battle_ttl_sec()
                )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 与 Go 的 `err != nil` 同宽
            finalize_exc = exc
        if finalize_exc is not None or not finalized:
            if self.model_b:
                # UID/RV 已确认但 Redis finalize 失败 / 响应未知时仍不自动释放:事务
                # 可能已把 persistent uncertain 成功改成 warming。后续只能由权威读回 /
                # 审计收敛,本请求绝不能凭本地结果删除 claim 或触碰 K8s。
                plog.get().error(
                    "gameserver_fenced_finalize_unavailable",
                    match_id=match_id,
                    allocation_id=allocation_id,
                    pod=pod_name,
                    finalized=finalized,
                    err=str(finalize_exc) if finalize_exc is not None else None,
                )
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "battle %d allocation finalize unavailable",
                    match_id,
                ) from finalize_exc
            # legacy 路径仍可按 allocation_id 清理;它没有 POST 结果未知的 persistent fence。
            plog.get().warning(
                "battle_allocate_refused",
                reason=REASON_ALLOC_FINALIZE_LOST,
                match_id=match_id,
                allocation_id=allocation_id,
                pod=pod_name,
                finalized=finalized,
                err=str(finalize_exc) if finalize_exc is not None else None,
                hint="claim 已不属本次分配(被回收或被新分配取代),刚拿到的 pod 会被回收",
            )
            await self.cleanup_allocated_battle(
                match_id, allocation_id, pod_name, authoritative
            )
            if finalize_exc is not None:
                raise finalize_exc
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "battle %d allocation claim no longer owned",
                match_id,
            )
        if self.model_b:
            try:
                await self.provision_battle_credential(
                    match_id, allocation_id, authoritative
                )
            except asyncio.CancelledError:
                raise
            except BaseException as cred_exc:  # noqa: BLE001 —— 与 Go 的 `err != nil` 同宽
                plog.get().error(
                    "battle_credential_provision_failed",
                    match_id=match_id,
                    pod=pod_name,
                    uid=authoritative.instance_uid,
                    err=str(cred_exc),
                )
                await self.cleanup_allocated_battle(
                    match_id, allocation_id, pod_name, authoritative
                )
                raise errcode.PandoraError(
                    errcode.ErrDSAllocationFailed,
                    "battle %d credential delivery failed",
                    match_id,
                ) from cred_exc

        # R1:这是本次分配唯一一条"pod 已定、身份已定格、开始等 DS 就绪"的阶段推进。
        # 缺了它就无法证明"分配到底有没有走到 Agones 这一步"。
        plog.get().info(
            "battle_warming",
            match_id=match_id,
            pod=pod_name,
            ds_addr=addr,
            players=len(players),
            allocation_id=allocation_id,
            uid=battle.gameserver_uid,
            release_track=actual_release_track,
            ready_wait=godur.duration_string(self.cfg.ready_wait_timeout_td()),
        )

        # 等 DS 用正确 match_id/pod 的心跳上报 ready/running,后端才把 ds_addr 回给 matchmaker。
        try:
            res = await self.wait_battle_ready(match_id, pod_name, allocation_id)
        except asyncio.CancelledError:
            raise
        except BaseException as werr:  # noqa: BLE001 —— 下面按哨兵分流
            if _is_ready_wait_timeout(werr):
                raise await self.fail_ready_wait_timeout(
                    match_id, allocation_id, pod_name, authoritative, True
                ) from werr
            if _is_battle_wait_ownership_lost(werr):
                # 回收 / 接管所有权已属 sweep 或新分配:owner 放弃 cleanup,避免与在途
                # 的 fenced 回收链并发第二路 fence_preactive_release / release_expected
                #(复审必修:reclaimed/purged/superseded outcome 可识别,owner 跳过)。
                plog.get().info(
                    "battle_ready_wait_ownership_lost",
                    match_id=match_id,
                    allocation_id=allocation_id,
                    err=str(werr),
                )
                raise
            # 入站取消 / 超时或 repo 出错等非超时失败:本次刚分配的 pod 由本调用持有,
            # 用独立 cleanup 预算回收 pod + 删 warming 镜像,避免泄漏。
            await self.cleanup_allocated_battle(
                match_id, allocation_id, pod_name, authoritative
            )
            raise

        # owner 归属定案(owner-authority.md ②;contract 阶段=强依赖):READY 交付前
        # 逐玩家 Begin(BATTLE)。**写不进 owner 就不交付 READY** —— 交付即意味着客户端
        # 会拿票进这台 Battle DS,归属没定案就放行等于让玩家可能同时被两台 DS 认领
        # (§9.22 / 底线第 3 条)。失败按分配失败处理:既有补偿链回收 claim 与 pod,
        # 撮合重试,客户端按 §9.23 退避重查。
        try:
            await owner_begin_players(
                self.owner_auth,
                list(players),
                OWNER_TYPE_BATTLE,
                owner_target_from_allocate_result(res),
                OWNER_BEGIN_BUDGET_SEC,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as owner_exc:  # noqa: BLE001 —— 下面按哨兵分流
            if is_owner_begin_outcome_unknown(owner_exc):
                # Begin 可能已在 owner 提交,只是回包与判定 Query 都失败。此时回收 Pod
                # 会把可能已落库的 PENDING owner 变成永久死目标;保留同 allocation 的
                # 技术 READY,让 owner 恢复后的 claim loser 只读 exact+postcheck 收敛。
                plog.get().error(
                    "battle_owner_begin_outcome_unknown_retained",
                    match_id=match_id,
                    allocation_id=allocation_id,
                    pod=res.ds_pod_name,
                    players=len(players),
                    err=str(owner_exc),
                    hint="不交付 READY,不 rollback owner,不 cleanup allocation/pod;等待权威恢复后只读收敛",
                )
                raise
            plog.get().warning(
                "battle_ready_refused_owner_begin_failed",
                match_id=match_id,
                pod=res.ds_pod_name,
                players=len(players),
                err=str(owner_exc),
                hint="归属未定案不交付 READY(§9.22);走既有分配失败补偿链",
            )
            await self.cleanup_allocated_battle(
                match_id, allocation_id, pod_name, authoritative
            )
            raise

        # R1:分配链的终点(owner 已定案、ds_addr 即将回给 matchmaker),与 battle_warming
        # 成对。两条都有 ts 就能算出"等 DS 冷启动花了多久"(判据 5)。
        plog.get().info(
            "battle_ready_after_heartbeat",
            match_id=match_id,
            pod=pod_name,
            ds_addr=addr,
            allocation_id=allocation_id,
            uid=res.gameserver_uid,
            epoch=res.instance_epoch,
            players=len(players),
        )
        return res

    # ── Model B 凭据投递(stage → K8s 条件投递 → delivered CAS)──────────────

    async def provision_battle_credential(
        self, match_id: int, allocation_id: str, allocation: Any
    ) -> None:
        """完成 Redis stage → K8s 条件投递 → Redis delivered CAS。

        对应 Go 的 `provisionBattleCredential`。任何半失败都**不会产生 active**;
        cleanup 只按 expected allocation_id 撤销本轮 warming 实例。

        Raises:
            errcode.PandoraError(ErrInvalidState): 缺权威分配结果 / 签名器给出非法 exp。
            其它: `prepare_credential` / 签名 / `stage_pending` / `deliver_credential`
                的错误原样上抛;`mark_delivered` 的错误在权威 read-back 证明已投递时
                被吞掉(且**只在**证明成立时)。
        """
        if allocation is None:
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "missing authoritative gameserver allocation"
            )
        seed = await self.auth_repo.prepare_credential(
            BattleAuthorityBinding(
                match_id=match_id,
                allocation_id=allocation_id,
                pod_name=allocation.pod_name,
                instance_uid=allocation.instance_uid,
                required_writer_epoch=BATTLE_DS_WRITER_EPOCH_V2,
                auth_ttl_sec=self.ds_credential_ttl_sec,
                battle_ttl_sec=self.battle_ttl_sec(),
            )
        )
        # ★ 回写 epoch:后续 `deliver_credential` 的条件 PATCH 与 cleanup 的 exact
        #   回收都按这份 allocation 走,漏了这一行会让它们拿着 epoch=0 去对账。
        allocation.instance_epoch = seed.instance_epoch
        jti = str(uuid.uuid4())
        signed = self.ds_signer.sign_battle_credential(
            match_id,
            allocation.pod_name,
            allocation.instance_uid,
            seed.instance_epoch,
            seed.gen,
            jti,
            self.ds_credential_ttl_sec,
        )
        if signed.exp_ms <= 0 or signed.exp_ms > _UINT64_MAX:
            # Go 只判 `<= 0`(int64→uint64 转换在那边由类型系统兜住)。上界是它在
            # Python 侧的等价物:一个越界 exp 会一路带到 protobuf 才抛没有业务码的
            # ValueError,而此刻 Redis 里已经有一个领过号的 pending 槽位。
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "battle credential signer returned invalid exp"
            )
        credential = dspb.BattleDSCredential(
            gen=seed.gen,
            jti=jti,
            exp_ms=signed.exp_ms,
            kid=signed.kid,
            instance_uid=allocation.instance_uid,
            instance_epoch=seed.instance_epoch,
            token_sha256=signed.token_sha256,
            writer_epoch=signed.writer_epoch,
        )
        await self.auth_repo.stage_pending(
            BattleStageInput(
                match_id=match_id,
                allocation_id=allocation_id,
                credential=credential,
                auth_ttl_sec=self.ds_credential_ttl_sec,
            )
        )
        annotations = {
            BATTLE_TOKEN_ANNOTATION_KEY: signed.token,
            BATTLE_TOKEN_EXP_ANNOTATION_KEY: str(credential.exp_ms),
            BATTLE_TOKEN_GEN_ANNOTATION_KEY: str(credential.gen),
            BATTLE_TOKEN_JTI_ANNOTATION_KEY: credential.jti,
            BATTLE_INSTANCE_UID_ANNOTATION_KEY: credential.instance_uid,
            BATTLE_INSTANCE_EPOCH_KEY: str(credential.instance_epoch),
            BATTLE_WRITER_EPOCH_KEY: str(credential.writer_epoch),
            BATTLE_TOKEN_KID_KEY: credential.kid,
            BATTLE_TOKEN_HASH_KEY: credential.token_sha256,
        }
        rv = await self.authoritative_alloc.deliver_credential(allocation, annotations)
        try:
            await self.auth_repo.mark_delivered(
                match_id, allocation_id, credential, rv, self.ds_credential_ttl_sec
            )
        except asyncio.CancelledError:
            raise
        except BaseException as mark_exc:  # noqa: BLE001 —— 与 Go 的 `err != nil` 同宽
            # Redis 响应不确定时**只认权威 read-back**;不以本地 expected 或 K8s 镜像
            # 猜测成功。read 本身失败也按原错误上抛(§9.22:UNKNOWN 不得冒充成功)。
            try:
                snapshot = await self.auth_repo.read_authority(match_id)
            except asyncio.CancelledError:
                raise
            except BaseException:  # noqa: BLE001 —— 与 Go 的 `readErr != nil` 同宽
                raise mark_exc from None
            if not battle_pending_delivered(snapshot, allocation_id, credential, rv):
                raise
            # 到这里:权威已证明本轮 pending 就是 delivered 的那一份,mark 的失败只是
            # 响应丢失。吞掉它是**幂等收敛**,不是忽略错误。

    # ── claim 输家 / 幂等重试 ─────────────────────────────────────────────

    async def await_existing_allocation(
        self, match_id: int, existing: Any
    ) -> AllocateResult:
        """处理 claim 输家 / 幂等重试。对应 Go 的 `awaitExistingAllocation`。

        ★ 调用方**不拥有** allocation_id,故等待失败时只抛错误,**绝不**清理记录或
          释放 Pod;资源生命周期只由 claim 赢家或 sweep 管理。这条不是洁癖:两路
          并发回收同一个 allocation 时,`fence_preactive_release` / `release_expected`
          只有幂等最终一致保证,没有单次调用保证。

        Raises:
            errcode.PandoraError(ErrDSAllocationFailed): 赢家记录已不见 / 状态不可分配。
            errcode.PandoraError(ErrUnavailable): GSA 结果未知或上一次回收未确认的
                永久 fence(必须显式对账后才可再分配)。
        """
        if existing is None:
            plog.get().warning(
                "battle_allocate_refused",
                reason=REASON_AWAIT_CLAIM_MISSING,
                match_id=match_id,
                hint="claim 输家但读不回赢家的记录:记录已被回收或 TTL 过期,matchmaker 需重试",
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed, "battle %d allocation claim missing", match_id
            )
        if existing.state in (STATE_ALLOCATION_UNCERTAIN, STATE_ALLOCATION_RECONCILING):
            # 该状态表示 GSA POST 可能迟到应用。调用方不等待、不清理、不查删 K8s;
            # 只返回暂不可用,永久 claim 继续阻止同 match 第二次 POST。
            plog.get().warning(
                "allocate_idempotent_uncertain",
                match_id=match_id,
                allocation_id=existing.allocation_id,
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d allocation result requires explicit reconciliation",
                match_id,
            )
        if existing.state in (STATE_PREACTIVE_RELEASING, STATE_ALLOCATION_ABORT):
            # 外部 UID 条件删除尚未得到明确成功;永久 release fence 必须继续阻止本请求
            # 发第二次 GSA POST。回收可由幂等 sweep 重试,但**安全不依赖重试**。
            plog.get().warning(
                "battle_allocate_refused",
                reason=REASON_AWAIT_RELEASE_UNCONFIRMED,
                match_id=match_id,
                state=existing.state,
                allocation_id=existing.allocation_id,
                pod=existing.ds_pod_name,
                hint="上一次分配的 GameServer 回收未确认,永久 fence 阻止本次再分配;等 sweep 收敛后重试",
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d preactive gameserver release is not confirmed",
                match_id,
            )
        if not self.model_b and battle_ready_for_pod(
            existing, existing.ds_pod_name, match_id, existing.allocated_at_ms
        ):
            # R1:这是一次**路由判定结果**(本次重试直接复用已就绪的实例,不再分配)。
            # 它是"同一个 match 为什么两次拿到同一个 ds_addr"的唯一证据,不能只在 Debug。
            plog.get().info(
                "allocate_idempotent_hit",
                match_id=match_id,
                ds_addr=existing.ds_addr,
                state=existing.state,
                allocation_id=existing.allocation_id,
                pod=existing.ds_pod_name,
            )
            return await self.verify_existing_ready_delivery(
                match_id, list(existing.player_ids), allocate_result_from_battle(existing)
            )
        if existing.state not in (STATE_ALLOCATING, STATE_WARMING) and (
            not self.model_b or existing.state not in (STATE_READY, STATE_RUNNING)
        ):
            plog.get().warning(
                "allocate_idempotent_unusable", match_id=match_id, state=existing.state
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "battle %d in state %s, not allocatable",
                match_id,
                existing.state,
            )
        plog.get().debug(
            "allocate_idempotent_wait",
            match_id=match_id,
            pod=existing.ds_pod_name,
            state=existing.state,
            allocation_id=existing.allocation_id,
        )
        try:
            res = await self.wait_battle_ready(
                match_id, existing.ds_pod_name, existing.allocation_id
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 下面按哨兵分流
            if _is_ready_wait_timeout(exc):
                # ★ `owned=False`:输家不拥有本次 allocation,超时收尾只允许记录 /
                #   构造错误,不允许回收 pod 或删镜像。
                raise await self.fail_ready_wait_timeout(
                    match_id, existing.allocation_id, existing.ds_pod_name, None, False
                ) from exc
            raise
        return await self.verify_existing_ready_delivery(
            match_id, list(existing.player_ids), res
        )

    async def verify_existing_ready_delivery(
        self, match_id: int, players: list[int], res: AllocateResult | None
    ) -> AllocateResult:
        """claim loser / 幂等重试返回 READY 前的**双重只读门**。

        对应 Go 的 `verifyExistingReadyDelivery`。

        先确认全部玩家 owner 已被唯一 claim winner exact bind,再 one-shot 重读
        allocation 的 READY 快照,堵住 owner Query 期间 allocation 被回收 / 替换的
        TOCTOU。loser 不拥有本次 allocation,故任一失败只抛 `ErrUnavailable`,
        **绝不 Begin / Release / cleanup**。
        """
        target = owner_target_from_allocate_result(res)
        try:
            await owner_verify_players_exact(
                self.owner_auth, players, OWNER_TYPE_BATTLE, target, OWNER_VERIFY_BUDGET_SEC
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 与 Go 的 `err != nil` 同宽
            plog.get().warning(
                "battle_ready_refused_owner_not_exact",
                match_id=match_id,
                allocation_id=target.assignment_or_allocation_id,
                pod=target.pod_name,
                players=len(players),
                err=str(exc),
                hint="claim loser 只读等待唯一 winner 完成 owner bind,不触发 Begin/Release/cleanup",
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d owner binding not ready: %s",
                match_id,
                exc,
            ) from exc
        try:
            await self.verify_ready_allocation_snapshot(match_id, res)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 与 Go 的 `err != nil` 同宽
            plog.get().warning(
                "battle_ready_refused_postcheck_failed",
                match_id=match_id,
                allocation_id=target.assignment_or_allocation_id,
                pod=target.pod_name,
                err=str(exc),
                hint="owner 校验后 allocation 已漂移或不再 READY;claim loser fail-closed 且不清理",
            )
            raise
        # 到这里 res 必然非 None(`verify_ready_allocation_snapshot` 对 None 已 fail-closed)。
        assert res is not None
        return res

    async def verify_ready_allocation_snapshot(
        self, match_id: int, expected: AllocateResult | None
    ) -> None:
        """owner 校验之后的 one-shot READY 快照复核。

        对应 Go 的 `verifyReadyAllocationSnapshot`。

        ★ 只比"仍然 READY"是不够的:必须逐字段确认**还是同一个 exact 实例**
          (`AllocateResult` 的七元组整体相等)。allocation 在 owner Query 期间被回收
          并由新分配取代时,状态照样是 READY,而地址 / UID / allocation_id 已经换人 ——
          把它当成幂等命中返回,玩家就会拿着旧票去连新实例。

        Raises:
            errcode.PandoraError(ErrUnavailable): 权威读失败 / 不再 READY / 目标漂移。
        """
        if expected is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle %d ready result missing", match_id
            )
        current: Any = None
        if self.model_b:
            if self.auth_repo is None:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable, "battle %d authority repo missing", match_id
                )
            try:
                snapshot = await self.auth_repo.read_authority(match_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 与 Go 的 `err != nil` 同宽
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "battle %d ready authority postcheck failed: %s",
                    match_id,
                    exc,
                ) from exc
            ready, reason = snapshot.ready_authorized(now_ms(), self.heartbeat_timeout_ms())
            if not ready:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "battle %d no longer ready after owner check: %s",
                    match_id,
                    reason,
                )
            current = snapshot.battle
        else:
            try:
                battle = await self.repo.get_battle(match_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 与 Go 的 `err != nil` 同宽
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "battle %d ready projection postcheck failed: %s",
                    match_id,
                    exc,
                ) from exc
            if battle is None or not battle_ready_for_pod(
                battle, expected.ds_pod_name, match_id, battle.allocated_at_ms
            ):
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "battle %d no longer ready after owner check",
                    match_id,
                )
            current = battle
        got = allocate_result_from_battle(current)
        if current is None or current.match_id != match_id or got is None or got != expected:
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d allocation target changed after owner check",
                match_id,
            )
