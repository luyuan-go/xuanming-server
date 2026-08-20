"""hub_allocator 业务层 **RPC 主流程** —— 对应 Go 侧
`services/battle/hub_allocator/internal/biz/hub.go` 第 450–1983 行。

本模块提供 `RpcMixin`(Go 里带 `(u *HubUsecase)` receiver 的那批)与一组模块级
纯函数(Go 里没有 receiver 的那批)。最终组装:

    class HubUsecase(SweepMixin, RpcMixin, ShardMixin, HubUsecaseBase): ...

字段(`self.repo` / `self.auth_repo` / `self.cfg` / TTL accessor / `require_writer` /
`confirm_writer_for_ticket` / `backfill_source_revision` …)全部来自
`biz_base.HubUsecaseBase`;座位 / 归属 saga / 签票辅助(`select_and_reserve_shard` /
`replace_assignment_saga` / `prepare_hub_ticket` / `resume_assignment_cleanup` …)
全部来自 `biz_shard.ShardMixin`。本文件**一个都不重复实现** —— 重复实现会在 MRO 里
制造"两份同名方法、谁在前谁赢"的静默分叉,而这两批的补偿语义完全不同。

`add_shard_member` / `remove_shard_member` / `try_scale_out_on_no_capacity` /
`reconcile_shard_topology` 中的前三个由**第三批**(`SweepMixin`,Go hub.go:3446/3958/3964)
提供,本模块只调用不实现(与 `biz_shard.py` 同一约定)。

═══════════════════════════════════════════════════════════════════════════════
这批代码是 §9 不变量 19 / 20 / 21 / 22 / 23 的**执行体**
═══════════════════════════════════════════════════════════════════════════════

  §9.19 / §9.20(玩家不卡死)
      每条失败路径都必须返回**带明确原因**的错误,而不是静默挂起或默默放行:
      `no_routable_shard_reason`(在 `biz_shard`)、`hub_admission_barrier_not_open`
      的 `retry_after_ms`、`heartbeat_unknown_hub_waiting_topology` 都是客户端
      "还要等多久 / 该不该重试"的唯一依据。

  §9.21(金丝雀共存)
      `assign_hub` 的 canary→stable 单向回退:**只降级、绝不反向**。运维把
      canary_percent 调 0 只改 `release_policy`,而已有归属走的是持久化的
      `sticky_release_track` —— 没有这条回退,已粘 canary 的玩家会一直拿
      `ErrHubNoAvailable` 直到 assignment TTL 过期。

  §9.22(唯一 owner + fencing + 再入屏障)
      `acknowledge_admission` → `admit_owner_for_admission` 是 exact 四元组门:
      归属指向别处一律 fail-closed;`admit_not_before` 屏障未开返回**可重试**错误
      (放行就是双 DS);`pending_hub_eviction_orders` 只把驱逐令下发给**恰好那个**
      源实例 UID(给替换实例发 = 踢掉无辜玩家)。

  §9.23(单一幂等进场链)
      `assign_hub` 对同一玩家幂等:已有可路由归属 → 重签票、不二次占座、不产生
      第二个 owner;`acknowledge_admission` 对同一 `(admission_id, seq)` 幂等,
      ACK 回包丢失后重放返回同一结果。

═══════════════════════════════════════════════════════════════════════════════
与 Go 的形变(其余逐行同构,逐条都有理由)
═══════════════════════════════════════════════════════════════════════════════

  1. **`ctx` 不存在,请求方会话 jti 显式传参**。Go 在 `TransferHub` /
     `transferToLineInner` 里用 `pmw.SessionJTIFromContext(ctx)` 从请求上下文取;
     Python 侧 biz 层不持有 gRPC context(与 `prepare_hub_ticket` 已有的
     `session_jti` 形参同一约定),由 service 层读出后显式传入
     `caller_session_jti`。**不能**改成从某个全局 / ContextVar 里捞:
     `revert_line_transfer` 会在一次失败的切线里再发起一次 `transfer_hub`,
     隐式来源会让"用谁的会话签票"变成不可审计的隐藏状态。

  2. `(值, error)` → 返回值 + 抛异常。**唯一例外**是 Go 用 bool 表达控制流的地方
     (`resume_assignment_cleanup` 的 `still_found`、`replace_assignment_saga` 的
     `retry`、`assignment_routable` 的 `reusable`),照抄二元组。

  3. 错误消息里的 `%v` 一律改写成 `%s`。Python 的 `%` 格式化没有 `%v`,
     写了会在**失败路径上**再抛一个 ValueError —— 把一次可诊断的业务失败变成
     一条毫不相干的格式化异常。

  4. `time.Now().UnixMilli()` → `ledger.now_ms()`(全仓唯一实现)。

  5. `go func(){...}()` → `safego.spawn(name, factory)`。裸 `asyncio.create_task`
     的异常会被静默吞进 Task 对象,直到 GC 才打一条 "never retrieved";
     `safego` 挂 done-callback 计数并打栈。**必须给 name**,否则告警里认不出是谁。

  6. `releasetrack.Policy` 在 Go 是值类型(零值可用),Python 里 `Policy` 没有默认
     字段、`self.release_policy` 允许为 None。`policy_release_track` 把这层差异
     收在一处并回落 `stable`(= Go 零值 `percent=0` 的行为),不在每个调用点各写
     一份 None 判断。
"""

from __future__ import annotations

import asyncio
import dataclasses
import uuid
from typing import Any

from pandora.hub.v1 import allocator_pb2 as hubpb

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import releasetrack, safego
from pandorapy.services.hub_allocator import auth_repo as AR
from pandorapy.services.hub_allocator import ledger as L
from pandorapy.services.hub_allocator import owner_authority as OA
from pandorapy.services.hub_allocator import repo as R
from pandorapy.services.hub_allocator.biz_base import (
    COMMAND_DRAIN,
    COMMAND_NONE,
    COMMAND_STOP,
    PRESENCE_REFRESH_TIMEOUT_SEC,
    REASON_CANARY_NO_CAPACITY,
    REASON_OWNER_BARRIER_NOT_OPEN,
    REASON_OWNER_POINTS_ELSEWHERE,
    REASON_RELEASE_CAS_EXHAUSTED,
    REASON_TRANSFER_CAS_EXHAUSTED,
    REASON_WRITER_LEASE_NOT_HELD,
    STATE_DRAINING,
    STATE_READY,
    STATE_STOPPING,
    AssignResult,
    HubCredential,
)
from pandorapy.services.hub_allocator.biz_base import (
    REASON_ASSIGN_CAS_EXHAUSTED as _REASON_ASSIGN_CAS_EXHAUSTED,
)
from pandorapy.services.hub_allocator.biz_shard import (
    LocalHubCredentialSource,
    assignment_binding_v2_complete,
    assignment_instance_identity,
    assignment_same_instance,
    authoritative_shard,
    bind_assignment_auth,
    effective_role_id,
    select_transfer_target,
    sticky_release_track,
    sticky_release_track_or_none,
    transfer_cleanup_ref,
    transfer_cleanup_source,
)
from pandorapy.services.hub_allocator.owner_lease import renew_owner_lease_gate
from pandorapy.services.hub_allocator.owner_lease_client import OwnerTargetView

# Go 侧三条主流程(AssignHub / ReleaseHub / TransferHub)的 CAS 重试上限都是**字面量 8**,
# 不读 `cfg.OptimisticRetry`(那个只给分片镜像的 update-with-lock 用)。这里抄成常量而不是
# 就地写 8,是因为它同时出现在循环边界与耗尽日志的 `attempts` 字段里 —— 两处写岔了,
# 运维照日志算出的"重试了几次"就是错的。
ASSIGNMENT_CAS_ATTEMPTS = 8

# 单次心跳最多下发多少条驱逐令。对应 Go 的 `maxHubEvictionOrdersPerHeartbeat`。
# 有界的理由:清理索引的长度由"待清理的旧 owner 数"决定,排空一整台 Hub 时可达数百;
# 一次全下发会让心跳响应体和 DS 侧的 Kick 风暴同时失控。剩余项下一跳继续发。
MAX_HUB_EVICTION_ORDERS_PER_HEARTBEAT = 256

# owner census 代提交 Admit 的预算(秒)。Go: `2*time.Second`。
CENSUS_ADMIT_BUDGET_SEC = 2.0

# `shard_id` / `target_shard_id` 是 proto 的 uint32。
_UINT32_MAX = (1 << 32) - 1


# ── 出参结构 ─────────────────────────────────────────────────────────────────


@dataclasses.dataclass(slots=True)
class TransferResult:
    """`transfer_hub` 的出参。对应 Go 的 `TransferResult`。

    `new_assignment_id` 是本次迁移落地后的 assignment 标识:供调用方在 post-check
    失败时做「**仍是本次迁移产物**才回退」的条件补偿(见 `revert_line_transfer`)。
    少了它,回退就会把并发写者刚落地的归属一起回滚。
    """

    new_hub_ds_addr: str = ""
    new_hub_ticket: str = ""
    new_hub_pod_name: str = ""
    ticket_exp_ms: int = 0  # Go: int64
    new_assignment_id: str = ""


@dataclasses.dataclass(slots=True)
class HubLineView:
    """`list_hub_lines_for_player` 的单条出参。对应 Go 的 `HubLineView`。

    ★ 客户端可见结构,**刻意不含 pod 名 / DS 地址**(§9 不变量 14):线路视图只给
      玩家渲染拥挤度与选择器,内部拓扑不外发。
    """

    line_no: int = 0  # Go: uint32
    shard_id: int = 0  # Go: uint32
    player_count: int = 0  # Go: int32
    capacity: int = 0  # Go: int32
    is_full: bool = False
    is_current: bool = False


@dataclasses.dataclass(slots=True)
class TransferToLineResult:
    """`transfer_to_line_for_player` 的出参。对应 Go 的 `TransferToLineResult`。"""

    new_hub_ds_addr: str = ""
    new_hub_ticket: str = ""
    new_shard_id: int = 0  # Go: uint32
    line_no: int = 0  # Go: uint32


@dataclasses.dataclass(slots=True)
class HubEvictionOrder:
    """点名**一条 exact 物理源连接**的驱逐令。对应 Go 的 `HubEvictionOrder`。

    ★ 只在"以该源 GameServer 实例身份鉴权通过"的那次心跳上返回。四元组
      (pod + uid + protocol_epoch + writer_epoch)少比一格,同名 Pod 重建后的
      **新**实例就会收到指向死实例的驱逐令 —— 它会照单踢掉自己身上那个同名玩家。
    """

    player_id: int = 0  # Go: uint64
    assignment_id: str = ""
    admission_id: str = ""
    admission_seq: int = 0  # Go: uint64
    source_instance_uid: str = ""
    source_protocol_epoch: int = 0  # Go: uint32
    source_writer_epoch: int = 0  # Go: uint32
    cleanup_assignment_id: str = ""


@dataclasses.dataclass(slots=True)
class HeartbeatResult:
    """下发给 Hub DS 的控制指令。对应 Go 的 `HeartbeatResult`。

    `accepted_*` 五项是 Model B 令牌激活确认:DS 侧 `IsBoundToRequest` 逐字段比对
    自持凭据,不通过就**不打开准入租约**。legacy/off 路径恒 0/空(由
    `apply_local_credential_ack` 在 mode=local 下回显)。
    """

    command: str = COMMAND_NONE
    grace_seconds: int = 0  # Go: int32,仅 drain/stop 非 0
    eviction_orders: list[HubEvictionOrder] = dataclasses.field(default_factory=list)
    accepted_token_gen: int = 0  # Go: uint64
    accepted_token_jti: str = ""
    accepted_instance_uid: str = ""
    accepted_protocol_epoch: int = 0  # Go: uint32
    accepted_writer_epoch: int = 0  # Go: uint32


@dataclasses.dataclass(slots=True)
class AcknowledgeAdmissionResult:
    """对应 Go 的 `AcknowledgeAdmissionResult`。只暴露 DS 状态机需要的那一格。"""

    admitted: bool = False


@dataclasses.dataclass(slots=True)
class AcknowledgeDepartureResult:
    """对应 Go 的 `AcknowledgeDepartureResult`。

    `conflict=True` = 旧连接晚到的 Logout:DS 必须**零副作用**停止重试,
    不能把它当成"离场失败"继续刷 —— 那条连接对应的 owner 早就不是它了。
    """

    departed: bool = False
    conflict: bool = False


# ── 模块级纯函数 ─────────────────────────────────────────────────────────────


def build_hub_lines_for_track(
    shards: list, region: str, cur_pod: str, release_track: str
) -> list[HubLineView]:
    """把某 region + 轨的 ready 分片按 `shard_id` 升序编成 1-based 线路视图。

    对应 Go 的 `buildHubLinesForTrack`。

    ★ 排序键必须是 `shard_id` 而不是遍历顺序:`list_shards` 的顺序来自 Redis SET,
      两次调用可能不同。线路号不稳定 = 玩家看到的"3 线"每次刷新指向不同分片,
      点进去进的不是刚才看的那条。

    ★ 非法持久化轨道用 `sticky_release_track_or_none` **跳过这一条**而不是整轮失败
      (Go 那侧写的是 `err == nil && ...`):一条脏分片不该让整个线路列表拉不出来。
    """
    ready = []
    for s in shards:
        track = sticky_release_track_or_none(s.release_track)
        if (
            track is not None
            and s.region == region
            and s.state == STATE_READY
            and (release_track == "" or track == release_track)
        ):
            ready.append(s)
    ready.sort(key=lambda s: s.shard_id)
    return [
        HubLineView(
            line_no=i + 1,
            shard_id=s.shard_id,
            player_count=s.player_count,
            capacity=s.capacity,
            is_full=s.player_count >= s.capacity,
            is_current=cur_pod != "" and s.hub_pod_name == cur_pod,
        )
        for i, s in enumerate(ready)
    ]


def build_hub_lines(shards: list, region: str, cur_pod: str) -> list[HubLineView]:
    """不限轨版本。对应 Go 的 `buildHubLines`(空 `release_track` = 不筛轨)。"""
    return build_hub_lines_for_track(shards, region, cur_pod, "")


def line_no_of_shard(shards: list, region: str, release_track: str, shard_id: int) -> int:
    """目标 `shard_id` 在 region + 轨内的 1-based 线路号;不在 ready 列表返 0。

    对应 Go 的 `lineNoOfShard`。**必须复用** `build_hub_lines_for_track` 而不是自己
    再数一遍:两份编号逻辑一旦漂移,玩家切到"3 线"后客户端显示的就是别的号。
    """
    for v in build_hub_lines_for_track(shards, region, "", release_track):
        if v.shard_id == shard_id:
            return v.line_no
    return 0


def modelb_heartbeat_result(
    res: AR.ActivateResult, command: str, grace_seconds: int
) -> HeartbeatResult:
    """把 `activate_heartbeat` 的结果摊成心跳应答。对应 Go 的 `modelBHeartbeatResult`。"""
    return HeartbeatResult(
        command=command,
        grace_seconds=grace_seconds,
        accepted_token_gen=res.active_gen,
        accepted_token_jti=res.active_jti,
        accepted_instance_uid=res.instance_uid,
        accepted_protocol_epoch=res.protocol_epoch,
        accepted_writer_epoch=res.writer_epoch,
    )


def assignment_matches_admission(
    a: Any, player_id: int, assignment_id: str, pod: str, cred: HubCredential | None
) -> bool:
    """归属记录是否仍精确对应这次 Admission。对应 Go 的 `assignmentMatchesAdmission`。

    ★ 六格全比,少一格就是一个开门口子:
      只比 `(player, assignment_id)` → 同一 assignment 在**同名 Pod 重建**后仍匹配,
      新实例凭旧票开 spawn gate;不比 `auth_writer_epoch` → legacy 绑定也能过。
    """
    return (
        a is not None
        and cred is not None
        and player_id != 0
        and a.player_id == player_id
        and a.assignment_id == assignment_id
        and a.hub_pod_name == pod
        and a.hub_instance_uid == cred.instance_uid
        and a.auth_epoch == cred.protocol_epoch
        and a.auth_writer_epoch == cred.writer_epoch
    )


def hub_credential_identity(cred: HubCredential) -> L.CredentialIdentity:
    """biz 凭据 → 账本身份。对应 Go 的 `hubCredentialIdentity`。"""
    return L.CredentialIdentity(
        gen=cred.gen,
        jti=cred.jti,
        instance_uid=cred.instance_uid,
        protocol_epoch=cred.protocol_epoch,
        token_sha256=cred.token_sha256,
        kid=cred.kid,
        writer_epoch=cred.writer_epoch,
    )


def _require_uint32(name: str, value: int) -> int:
    """proto uint32 字段的显式边界检查。

    ★ Go 的 `uint32(x)` **静默截断**,Python 的 int 不会。不显式挡,同一个越界入参
      在两栈会走出不同分支(Go 误匹配到低位相同的分片,Python 一路带着大整数往下
      走直到某个 proto 序列化点才炸)。照抄"先拒绝再使用",两栈同为拒绝。
    """
    if not isinstance(value, int) or isinstance(value, bool) or value < 0 or value > _UINT32_MAX:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "%s %r out of uint32 range", name, value
        )
    return value


# ── RpcMixin ────────────────────────────────────────────────────────────────


class RpcMixin:
    """`HubUsecase` 的 RPC 主流程。对应 Go hub.go 450–1983 行。

    ★ 刻意**不**加 `__slots__`(与 `HubUsecaseBase` / `ShardMixin` 同因):多 mixin
      叠加时 slots 布局要在每层重复声明,漏一层就静默退回 `__dict__`,制造
      "以为有约束其实没有"的假象。
    """

    # 下面这些由 HubUsecaseBase / ShardMixin / SweepMixin 提供,这里只声明给读者看:
    #   repo / fleet / auth_repo / signer / locator / sess_gate / owner_auth
    #   owner_lease / owner_lease_required / owner_admitted / release_policy / cfg
    #   require_writer() / confirm_writer_for_ticket() / backfill_source_revision()
    #   shard_ttl_sec() / assign_ttl_sec() / assignment_saga_ttl_sec() / auth_ttl_sec()
    #   heartbeat_max_age_ms() / ds_token_generation
    #   ensure_shards() / select_and_reserve_shard() / reserve_routable_seat()
    #   ensure_existing_assignment_seat() / assignment_routable() / routable_shard_views()
    #   replace_assignment_saga() / resume_assignment_cleanup() / remove_transfer_cleanup_ref()
    #   compensate_reserved_seat() / release_assignment_seat() / prepare_hub_ticket()
    #   bind_owner_for_published_hub_assignment() / sign_result() / reconcile_shard_topology()
    #   resolve_owner_target_from_assignment() / owner_target_for_hub_ticket()
    #   add_shard_member() / remove_shard_member() / try_scale_out_on_no_capacity()  ← 第三批

    # ── 小工具 ───────────────────────────────────────────────────────────

    def policy_release_track(self, player_id: int) -> str:
        """无归属玩家**首次尝试**的轨。对应 Go 的 `u.releasePolicy.Select(playerID)`。

        Go 的 `releasetrack.Policy` 是值类型,零值(`percent=0`)恒回 stable,所以那边
        不需要判空。Python 的 `Policy` 没有默认字段、`release_policy` 允许为 None
        (dev / 未注入),这里回落 stable —— 与 Go 零值行为逐字节相同。

        ★ 回落方向只能是 stable。回落 canary = 未注入策略的部署把**全服**灌进
          未验证轨道,而这恰恰是 §9.21 明令禁止的方向。
        """
        if self.release_policy is None:
            return releasetrack.STABLE
        return self.release_policy.select(player_id)

    # ── RPC 1:AssignHub ─────────────────────────────────────────────────

    async def assign_hub(  # noqa: C901,PLR0912,PLR0915 —— 与 Go 同形状,拆开就对不上行
        self,
        player_id: int,
        region: str,
        team_id: int,
        role_id: int,
        source_match_id: int,
        session_jti: str,
    ) -> AssignResult:
        """为玩家分配一个大厅 DS 分片。对应 Go 的 `AssignHub`。

        **幂等**(§9.23):已有归属且仍可路由 → 只重签票并刷新 TTL,不二次占座、
        不产生第二个 owner。这条是"重复 AssignHub"用例的全部意义 —— 少了它,
        客户端一次重试就会让同一玩家占住两个座位,分片在几分钟内假满。

        Args:
            role_id: login 从 `player_roles` 读出透传的已选角色。>0 覆盖归属镜像
                (login 是角色数据权威,换角以新值为准);0 = 调用方不知角色,
                保留镜像已存值(Transfer / 重签路径不丢角色)。
            source_match_id: Battle→Hub 回流 fence。>0 时盖进本次票据的
                `source_match_id` claim,Hub DS 准入后用它通过 locator 的
                BATTLE→HUB guard。**只进票据不进归属镜像**:fence 是一次性回流
                凭证,Transfer / 迁移重签绝不携带。
            session_jti: login 透传的请求方会话 jti,盖进票据 `sjti` claim。
                空 = 旧调用方 / dev 直连(兼容窗)。
        """
        started_at = L.now_ms()
        try:
            self.require_writer()
        except errcode.PandoraError as exc:
            plog.get().warning(
                "hub_assign_rejected_biz",
                player_id=player_id,
                region=region,
                reason=REASON_WRITER_LEASE_NOT_HELD,
                err=str(exc),
            )
            raise
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if region == "":
            region = self.cfg.default_region

        for attempt in range(ASSIGNMENT_CAS_ATTEMPTS):
            existing = await self.repo.get_assignment(player_id)
            found = existing is not None
            if found and (
                existing.transfer_cleanup_pending or existing.release_cleanup_pending
            ):
                existing, still_found = await self.resume_assignment_cleanup(
                    player_id, existing.assignment_id
                )
                if not still_found:
                    continue
            effective_role = role_id
            desired_track = self.policy_release_track(player_id)
            if found:
                desired_track = sticky_release_track(existing.release_track)
                effective_role = effective_role_id(role_id, existing.role_id)
                # Redis / 授权读取失败**不能**降级成"另找一个分片":那等于在看不清
                # 当前归属的情况下再造一个 owner(§9.22)。异常原样上抛。
                current, reusable = await self.assignment_routable(player_id, existing)
                if reusable or assignment_same_instance(existing, current):
                    # assignment 可以比 admission ledger 活得更久:clean Logout 会精确删
                    # session,未进场 reservation 会绝对到期。重签票前必须在 {pod} 同槽
                    # 事务里重新确保"已有 session 或新鲜 reservation",否则会返回一张
                    # 必被 Admission 拒绝的票 —— 玩家连上就被踢,客户端只看到"闪断"。
                    ensure_failed: BaseException | None = None
                    ensured = None
                    try:
                        ensured = await self.ensure_existing_assignment_seat(
                            player_id, existing, current
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:  # noqa: BLE001 —— 下面按码分流
                        ensure_failed = exc
                    if ensure_failed is None:
                        next_rec = R.clone(existing)
                        next_rec.hub_addr = ensured.hub_addr
                        next_rec.shard_id = ensured.shard_id
                        next_rec.region = ensured.region
                        next_rec.release_track = ensured.release_track
                        next_rec.role_id = effective_role
                        if next_rec.assignment_id == "":
                            next_rec.assignment_id = str(uuid.uuid4())
                        bind_assignment_auth(next_rec, ensured)
                        # 存量 legacy(revision=0)在这里收敛:复用 / 续期是它唯一会被
                        # 重写的时机(INC-20260818-003)。
                        backfilled = self.backfill_source_revision(next_rec)
                        # 即使归属 bytes 完全相同也必须走 CAS SET 刷新 assignment TTL。
                        # 失败时**不清理** ensure 出来的座位:它可能是原连接的共享
                        # session,退掉就是把在场玩家踢下线。交 winner 精确释放,或让
                        # 新建 reservation 的有界 TTL 回收。
                        swapped = await self.repo.compare_and_swap_assignment(
                            player_id, existing, next_rec, self.assignment_saga_ttl_sec()
                        )
                        if not swapped:
                            continue
                        if backfilled != 0:
                            # 只有 CAS 真的落地才算「补上」:本事件是 rollout 第 5 步
                            # 开门前判空的依据,早于存储成功打出去就是伪证。
                            plog.get().info(
                                "hub_assignment_source_revision_backfilled",
                                player_id=player_id,
                                assignment_id=next_rec.assignment_id,
                                source_revision=backfilled,
                            )
                        await self.add_shard_member(next_rec.hub_pod_name, player_id)
                        self.confirm_writer_for_ticket(player_id)
                        return await self.sign_result(
                            player_id, effective_role, next_rec, source_match_id, session_jti
                        )
                    if errcode.as_code(ensure_failed) != errcode.ErrHubNoAvailable:
                        raise ensure_failed
                    # 旧 assignment 已无 seat 且原分片已满 / 漂移:继续走新 assignment
                    # 选择。**绝不能**反复刷新旧归属后返回一张永远无法 Admission 的票
                    # (那正是 §9.19 的"无人驱动的静默等待")。

            await self.ensure_shards(region, desired_track)
            assignment_id = str(uuid.uuid4())
            select_err: BaseException | None = None
            target = None
            seat = None
            try:
                target, seat = await self.select_and_reserve_shard(
                    player_id, assignment_id, region, team_id, "", desired_track
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 下面按码分流
                select_err = exc
            # canary 明确无可用容量时回退 stable;stable 绝不反向进入 canary。
            #
            # 这里**不限定**"首次分配":走到本行时已经在为玩家新建 assignment,
            # 旧归属要么不可路由、要么其分片已满 / 漂移,粘性本就无从保留。此时再坚持
            # canary 只会让整个 canary cohort 里"座位已失效"的那批玩家反复拿
            # ErrHubNoAvailable,直到 assignment TTL 过期才自愈 —— 而 canary 无容量最
            # 典型的成因恰恰是金丝雀存在的理由本身(镜像 CrashLoop / 永不心跳 /
            # 回滚把 replicas 调 0)。
            #
            # 更关键的是运维止血手段会失灵:把 canary_percent 调 0 只改 release_policy,
            # 而已有归属走的是 sticky_release_track 这条持久化路径、根本不读策略。
            # §9.21 要求"异常时能立即把 Canary 权重归零、Stable 继续服务",该要求必须
            # 对已粘住的玩家同样成立。回退方向是 canary→stable(降级到安全轨),
            # 反向(stable→canary)仍然禁止。
            if (
                select_err is not None
                and desired_track == releasetrack.CANARY
                and errcode.as_code(select_err) == errcode.ErrHubNoAvailable
            ):
                # R1 路由判定结果:玩家本属 canary cohort 却被降级到 stable。这是金丝雀
                # 异常的第一手信号,缺它就无法证明"当时到底走的哪一轨"(§9.21 排障必需)。
                plog.get().info(
                    "hub_assign_track_fallback",
                    player_id=player_id,
                    region=region,
                    hub_assignment_id=assignment_id,
                    from_track=releasetrack.CANARY,
                    to_track=releasetrack.STABLE,
                    reason=REASON_CANARY_NO_CAPACITY,
                )
                await self.ensure_shards(region, releasetrack.STABLE)
                select_err = None
                try:
                    target, seat = await self.select_and_reserve_shard(
                        player_id,
                        assignment_id,
                        region,
                        team_id,
                        "",
                        releasetrack.STABLE,
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    select_err = exc
            if select_err is not None:
                if errcode.as_code(select_err) == errcode.ErrHubNoAvailable:
                    await self.try_scale_out_on_no_capacity(region)
                raise select_err

            assignment = R.clone(existing) if found else hubpb.HubAssignmentStorageRecord()
            assignment.player_id = player_id
            assignment.hub_pod_name = target.hub_pod_name
            assignment.hub_addr = target.hub_addr
            assignment.shard_id = target.shard_id
            assignment.region = target.region
            assignment.team_id = team_id
            assignment.assigned_at_ms = L.now_ms()
            assignment.role_id = effective_role
            assignment.assignment_id = assignment_id
            assignment.release_track = target.release_track
            bind_assignment_auth(assignment, seat)
            # 先做**纯签名**、再发布 assignment:签名器失败时仍可用 reservation identity
            # 精确补偿。owner Begin 被刻意拆到 CAS winner 之后,CAS loser 机械上碰不到 owner。
            try:
                prepared = await self.prepare_hub_ticket(
                    player_id, effective_role, assignment, source_match_id, session_jti
                )
            except asyncio.CancelledError:
                raise
            except BaseException:  # noqa: BLE001 —— 补偿后原样上抛
                await self.compensate_reserved_seat(
                    target.hub_pod_name, player_id, assignment_id, seat
                )
                raise

            target_pod = target.hub_pod_name

            # `pod=target_pod` 是**默认值绑定**而不是闭包捕获:Python 的闭包是后期绑定,
            # 而 `target` / `target_pod` 都是循环变量。saga 若把回调留到下一轮才跑,
            # 闭包版本会把队友索引写到**下一次尝试**选中的分片上 —— 玩家进的是 A 分片,
            # 队友亲和却指向 B,而且不会有任何错误。
            async def _on_swapped(pod: str = target_pod) -> None:
                # 队友亲和索引是 best-effort:写失败只影响"下一个队友会不会被优先放到
                # 同一分片",不影响本次归属的正确性,绝不能因此让已提交的 saga 失败。
                if team_id != 0:
                    try:
                        await self.repo.set_team_shard(team_id, pod, self.assign_ttl_sec())
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:  # noqa: BLE001
                        plog.get().warning(
                            "set_team_shard_failed", team_id=team_id, err=str(exc)
                        )

            retry_saga = await self.replace_assignment_saga(
                player_id,
                existing if found else None,
                assignment,
                seat,
                _on_swapped,
                "Hub replacement assignment disappeared during cleanup",
            )
            if retry_saga:
                continue
            # assignment / seat 已 durable 发布,**不能**补偿 winner;失败时下次 Assign
            # 读取当前记录重签即可。
            await self.bind_owner_for_published_hub_assignment(player_id, assignment, prepared)
            plog.get().info(
                "hub_assigned",
                player_id=player_id,
                pod=target.hub_pod_name,
                ds_pod=target.hub_pod_name,
                hub_assignment_id=assignment_id,
                shard_id=target.shard_id,
                region=target.region,
                release_track=target.release_track,
                team_id=team_id,
                role_id=effective_role,
                reassigned=found,
                attempt=attempt + 1,
                shard_players=target.player_count,
                shard_capacity=target.capacity,
                elapsed_ms=L.now_ms() - started_at,
            )
            return AssignResult(
                hub_ds_addr=assignment.hub_addr,
                hub_ticket=prepared.token,
                hub_pod_name=assignment.hub_pod_name,
                shard_id=assignment.shard_id,
                ticket_exp_ms=prepared.expires_at_ms,
            )

        # 重试耗尽:与"真没容量"同为 ErrHubNoAvailable,但处置完全相反(并发写者互撞
        # vs 需要扩容),必须能从日志一眼分开。
        plog.get().warning(
            "hub_assign_cas_exhausted",
            player_id=player_id,
            region=region,
            team_id=team_id,
            attempts=ASSIGNMENT_CAS_ATTEMPTS,
            reason=_REASON_ASSIGN_CAS_EXHAUSTED,
            elapsed_ms=L.now_ms() - started_at,
        )
        raise errcode.PandoraError(
            errcode.ErrHubNoAvailable, "player %d assignment changed concurrently", player_id
        )

    # ── RPC 2:ReleaseHub ────────────────────────────────────────────────

    async def release_hub(self, player_id: int) -> None:  # noqa: C901,PLR0912
        """玩家离开大厅:退分片占位 + 删归属。对应 Go 的 `ReleaseHub`。

        **幂等**:无归属视为已离开,直接返回。返回错误会让调用方无限重试一件已经
        做完的事(§9.19 的"无出口等待"在服务端一侧的形态)。
        """
        started_at = L.now_ms()
        try:
            self.require_writer()
        except errcode.PandoraError as exc:
            plog.get().warning(
                "hub_release_rejected_biz",
                player_id=player_id,
                reason=REASON_WRITER_LEASE_NOT_HELD,
                err=str(exc),
            )
            raise
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")

        for attempt in range(ASSIGNMENT_CAS_ATTEMPTS):
            assignment = await self.repo.get_assignment(player_id)
            if assignment is None:
                return  # 幂等
            if assignment.transfer_cleanup_pending or assignment.release_cleanup_pending:
                _, still_found = await self.resume_assignment_cleanup(
                    player_id, assignment.assignment_id
                )
                if not still_found:
                    return
                continue
            if self.auth_repo is not None:
                current, reusable = await self.assignment_routable(player_id, assignment)
                if not reusable:
                    if assignment_same_instance(assignment, current):
                        # 同实例普通凭据轮换:先把归属 CAS 到当前 active,再重新进入
                        # 精确 Release。不占新座,也不会拿旧 tuple 删除归属后退座失败
                        # (那会留下一个谁也清不掉的幽灵座位)。
                        next_rec = R.clone(assignment)
                        bind_assignment_auth(next_rec, current)
                        await self.repo.compare_and_swap_assignment(
                            player_id,
                            assignment,
                            next_rec,
                            self.assignment_saga_ttl_sec(),
                        )
                        continue
                    raise errcode.PandoraError(
                        errcode.ErrInvalidState,
                        "hub assignment is not bound to the current active credential",
                    )
                ref = transfer_cleanup_ref(assignment)
                # Index-first:登记成功之后,无论进程在墓碑 CAS 之前还是之后崩溃,
                # 这条待清理都仍然**可枚举**。反过来做(先 CAS 再登记)会出现
                # "归属已删、旧座位没人清"的永久残留。
                await self.repo.register_transfer_cleanup(assignment.hub_pod_name, ref)
                next_rec = R.clone(assignment)
                next_rec.release_cleanup_pending = True
                next_rec.release_cleanup_match_id = 0
                next_rec.release_cleanup_placement_version = 0
                next_rec.release_cleanup_operation_id = ""
                try:
                    marked = await self.repo.compare_and_swap_assignment(
                        player_id, assignment, next_rec, self.assignment_saga_ttl_sec()
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException:  # noqa: BLE001
                    # CAS 结果**未知**:保留 ref。对账器会移除孤儿或续跑持久墓碑;
                    # 这里绝不能报成功(报了就没人再回来清)。
                    raise
                if not marked:
                    await self.remove_transfer_cleanup_ref(assignment.hub_pod_name, ref)
                    continue
                _, still_found = await self.resume_assignment_cleanup(
                    player_id, assignment.assignment_id
                )
                if still_found:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidState, "Hub release cleanup did not delete assignment"
                    )
                plog.get().info(
                    "hub_released",
                    player_id=player_id,
                    pod=assignment.hub_pod_name,
                    ds_pod=assignment.hub_pod_name,
                    hub_assignment_id=assignment.assignment_id,
                    channel="model_b",
                    attempt=attempt + 1,
                    elapsed_ms=L.now_ms() - started_at,
                )
                return
            # legacy / off 路径没有 exact Model-B owner,保留历史 CAS/delete 行为。
            # placement enforce 到不了这一支:严格 Admission 本身就要求 auth_repo。
            deleted = await self.repo.compare_and_swap_assignment(player_id, assignment, None, 0)
            if not deleted:
                continue
            await self.release_assignment_seat(assignment)
            await self.remove_shard_member(assignment.hub_pod_name, player_id)
            plog.get().info(
                "hub_released",
                player_id=player_id,
                pod=assignment.hub_pod_name,
                ds_pod=assignment.hub_pod_name,
                hub_assignment_id=assignment.assignment_id,
                channel="legacy",
                attempt=attempt + 1,
                elapsed_ms=L.now_ms() - started_at,
            )
            return

        plog.get().warning(
            "hub_release_cas_exhausted",
            player_id=player_id,
            attempts=ASSIGNMENT_CAS_ATTEMPTS,
            reason=REASON_RELEASE_CAS_EXHAUSTED,
            elapsed_ms=L.now_ms() - started_at,
        )
        raise errcode.PandoraError(
            errcode.ErrInternal, "player %d release CAS retry exhausted", player_id
        )

    # ── RPC 3:TransferHub ───────────────────────────────────────────────

    async def transfer_hub(  # noqa: C901,PLR0912,PLR0915 —— 与 Go 同形状
        self, player_id: int, target_hub_id: int, caller_session_jti: str = ""
    ) -> TransferResult:
        """跨分片传送。对应 Go 的 `TransferHub`。

        顺序是**先占新分片 → 再切归属 → 最后退旧分片 → 重签票**:占新失败时旧分片
        一个字节不动(玩家还在原地正常玩),而反过来做会出现"旧的退了、新的没占到"
        的空档 —— 那一瞬间玩家哪都不在。

        `caller_session_jti` 对应 Go 的 `pmw.SessionJTIFromContext(ctx)`(见模块头
        形变 ①):盖进重签票据的 `sjti`,兑换点复核会话现行性。
        """
        self.require_writer()
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")

        for attempt in range(ASSIGNMENT_CAS_ATTEMPTS):
            assignment = await self.repo.get_assignment(player_id)
            if assignment is None:
                raise errcode.PandoraError(
                    errcode.ErrHubTransferFailed, "player %d not in any hub", player_id
                )
            if assignment.transfer_cleanup_pending or assignment.release_cleanup_pending:
                assignment, still_found = await self.resume_assignment_cleanup(
                    player_id, assignment.assignment_id
                )
                if not still_found:
                    raise errcode.PandoraError(
                        errcode.ErrHubTransferFailed,
                        "player %d Hub assignment was released",
                        player_id,
                    )
            # 只为校验持久化轨道合法(非法即 fail-closed,不能拿脏轨去筛目标)。
            sticky_release_track(assignment.release_track)
            if self.auth_repo is not None and not assignment_binding_v2_complete(
                assignment, player_id
            ):
                raise errcode.PandoraError(
                    errcode.ErrHubTransferFailed,
                    "player %d assignment is not a complete writer-v2 binding",
                    player_id,
                )
            shards = await self.repo.list_shards()
            target = select_transfer_target(shards, assignment, target_hub_id)
            if target is None:
                raise errcode.PandoraError(
                    errcode.ErrHubTransferFailed,
                    "no ready target shard for player %d (target_hub_id=%d)",
                    player_id,
                    target_hub_id,
                )

            if target.hub_pod_name == assignment.hub_pod_name:
                try:
                    current, reusable = await self.assignment_routable(player_id, assignment)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrHubTransferFailed,
                        "check current shard: %s",
                        exc,
                        cause=exc,
                    ) from exc
                if reusable or assignment_same_instance(assignment, current):
                    try:
                        ensured = await self.ensure_existing_assignment_seat(
                            player_id, assignment, current
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:  # noqa: BLE001
                        raise errcode.PandoraError(
                            errcode.ErrHubTransferFailed,
                            "ensure current shard %s admission seat: %s",
                            target.hub_pod_name,
                            exc,
                            cause=exc,
                        ) from exc
                    next_rec = R.clone(assignment)
                    next_rec.hub_addr = ensured.hub_addr
                    next_rec.shard_id = ensured.shard_id
                    next_rec.region = ensured.region
                    next_rec.release_track = ensured.release_track
                    if next_rec.assignment_id == "":
                        next_rec.assignment_id = str(uuid.uuid4())
                    bind_assignment_auth(next_rec, ensured)
                    prepared = await self.prepare_hub_ticket(
                        player_id, next_rec.role_id, next_rec, 0, caller_session_jti
                    )
                    swapped = await self.repo.compare_and_swap_assignment(
                        player_id, assignment, next_rec, self.assignment_saga_ttl_sec()
                    )
                    if not swapped:
                        continue
                    self.confirm_writer_for_ticket(player_id)
                    await self.bind_owner_for_published_hub_assignment(
                        player_id, next_rec, prepared
                    )
                    return TransferResult(
                        new_hub_ds_addr=next_rec.hub_addr,
                        new_hub_ticket=prepared.token,
                        new_hub_pod_name=next_rec.hub_pod_name,
                        ticket_exp_ms=prepared.expires_at_ms,
                        new_assignment_id=next_rec.assignment_id,
                    )

            new_assignment_id = str(uuid.uuid4())
            try:
                seat = await self.reserve_routable_seat(
                    target.hub_pod_name, player_id, new_assignment_id
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrHubTransferFailed,
                    "reserve target shard %s failed: %s",
                    target.hub_pod_name,
                    exc,
                    cause=exc,
                ) from exc
            target = authoritative_shard(target, seat)
            new_assignment = R.clone(assignment)
            new_assignment.player_id = player_id
            new_assignment.hub_pod_name = target.hub_pod_name
            new_assignment.hub_addr = target.hub_addr
            new_assignment.shard_id = target.shard_id
            new_assignment.region = target.region
            new_assignment.team_id = assignment.team_id
            new_assignment.assigned_at_ms = L.now_ms()
            new_assignment.role_id = assignment.role_id
            new_assignment.assignment_id = new_assignment_id
            new_assignment.release_track = target.release_track
            bind_assignment_auth(new_assignment, seat)
            try:
                prepared = await self.prepare_hub_ticket(
                    player_id, assignment.role_id, new_assignment, 0, caller_session_jti
                )
            except asyncio.CancelledError:
                raise
            except BaseException:  # noqa: BLE001 —— 补偿后原样上抛
                await self.compensate_reserved_seat(
                    target.hub_pod_name, player_id, new_assignment_id, seat
                )
                raise
            retry_saga = await self.replace_assignment_saga(
                player_id,
                assignment,
                new_assignment,
                seat,
                None,
                "Hub transfer assignment disappeared during cleanup",
            )
            if retry_saga:
                continue
            await self.bind_owner_for_published_hub_assignment(
                player_id, new_assignment, prepared
            )
            plog.get().info(
                "hub_transferred",
                player_id=player_id,
                **{"from": assignment.hub_pod_name},
                to=target.hub_pod_name,
                from_pod=assignment.hub_pod_name,
                ds_pod=target.hub_pod_name,
                hub_assignment_id=new_assignment_id,
                from_assignment_id=assignment.assignment_id,
                shard_id=target.shard_id,
                release_track=target.release_track,
                target_hub_id=target_hub_id,
                attempt=attempt + 1,
            )
            return TransferResult(
                new_hub_ds_addr=new_assignment.hub_addr,
                new_hub_ticket=prepared.token,
                new_hub_pod_name=new_assignment.hub_pod_name,
                ticket_exp_ms=prepared.expires_at_ms,
                new_assignment_id=new_assignment.assignment_id,
            )

        plog.get().warning(
            "hub_transfer_cas_exhausted",
            player_id=player_id,
            target_hub_id=target_hub_id,
            attempts=ASSIGNMENT_CAS_ATTEMPTS,
            reason=REASON_TRANSFER_CAS_EXHAUSTED,
        )
        raise errcode.PandoraError(
            errcode.ErrHubTransferFailed,
            "player %d assignment changed concurrently",
            player_id,
        )

    # ── 玩家侧:线路列表 + 主动切线 ──────────────────────────────────────
    # 经 Envoy :8443 客户端面(jwt_authn 注入 x-pandora-player-id),player_id 取自
    # JWT sub,**不信请求体**。ListHubs / TransferHub 是后端内部 / DS 调用,不经客户端面。

    async def list_hub_lines_for_player(
        self, player_id: int, req_region: str
    ) -> list[HubLineView]:
        """列出玩家当前 region 可切换的大厅线路。对应 Go 的 `ListHubLinesForPlayer`。

        `req_region` 留空 = 用玩家当前归属的 region(**服务端权威,忽略客户端申报**);
        无归属时回退 `req_region` 或默认 region。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        region, cur_pod = req_region, ""
        release_track = self.policy_release_track(player_id)
        assignment = await self.repo.get_assignment(player_id)
        if assignment is not None:
            if self.auth_repo is not None:
                _, reusable = await self.assignment_routable(player_id, assignment)
                if not reusable:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidState,
                        "hub assignment is not bound to the current active credential",
                    )
            region = assignment.region  # 归属 region 权威
            cur_pod = assignment.hub_pod_name
            release_track = sticky_release_track(assignment.release_track)
        if region == "":
            region = self.cfg.default_region
        shards = await self.repo.list_shards()
        shards = await self.routable_shard_views(shards)
        return build_hub_lines_for_track(shards, region, cur_pod, release_track)

    async def transfer_to_line_for_player(
        self, player_id: int, target_shard_id: int, caller_session_jti: str = ""
    ) -> TransferToLineResult:
        """玩家主动切换到指定线路(换实例,AB 互不可见)。对应 Go 的 `TransferToLineForPlayer`。

        四道护栏,**顺序即语义**:
          1. 战斗 / 匹配中禁切(查 player_locator,**fail-closed**:presence 不确定即拒,
             INC-20260722-002);
          2. 冷却防刷(SET NX EX;后续任何失败都释放占坑,让玩家可立即重试 —— 不释放
             就是把一次失败变成 N 秒的"按钮点不动",§9.20);
          3. 目标线路不存在 / 非本 region → `ErrHubTransferFailed`;已满 → `ErrHubLineFull`;
          4. 复用内部 `transfer_hub` 完成 占新→切归属→退旧→重签票。
        """
        self.require_writer()
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        _require_uint32("target_shard_id", target_shard_id)

        # 护栏 1:切线 = 进入另一台 Hub DS。locator RPC 失败、非 OK、OFFLINE / 未知状态
        # 都**不能证明**玩家不在旧 DS 战斗 / 匹配,必须在任何副作用(冷却占坑 / 占座 /
        # 签票)之前 fail-closed 拒绝,客户端退避重试(§9.22:UNKNOWN 不得授权新归属;
        # INC-20260722-002 废止原"弱依赖告警放行"契约)。
        if self.locator is not None:
            try:
                blocked = await self.locator.in_battle_or_matching(player_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "transfer_locator_check_failed_fail_closed",
                    player_id=player_id,
                    err=str(exc),
                )
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "player %d presence unknown, hub line switch rejected, retry later",
                    player_id,
                    cause=exc,
                ) from exc
            if blocked:
                raise errcode.PandoraError(
                    errcode.ErrHubTransferNotInHub,
                    "player %d in battle/matching, cannot switch hub line",
                    player_id,
                )
        else:
            # None = dev 联调模式(locator 未配)。生产装配缺失属部署错误:每次放行都
            # 留痕,防静默跳过护栏(INC-20260722-002 的放大因素)。
            plog.get().warning(
                "transfer_locator_checker_absent_dev_only", player_id=player_id
            )

        # 任何 cooldown SET 副作用之前,先证明当前 assignment 是完整 writer-v2 且仍精确
        # 绑定 Redis active。legacy / future / 缺 JTI 的旧 writer 记录必须**零变更**拒绝。
        if self.auth_repo is not None:
            assignment = await self.repo.get_assignment(player_id)
            if assignment is None:
                raise errcode.PandoraError(
                    errcode.ErrHubTransferNotInHub, "player %d not in any hub", player_id
                )
            _, reusable = await self.assignment_routable(player_id, assignment)
            if not reusable:
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "hub assignment is not bound to the current active credential",
                )

        # 护栏 2:冷却防刷(先占坑;后续失败再释放让玩家可立即重试)。
        ok = await self.repo.try_transfer_cooldown(player_id, self.transfer_cooldown_sec())
        if not ok:
            raise errcode.PandoraError(
                errcode.ErrHubTransferCooldown, "player %d transfer on cooldown", player_id
            )

        try:
            return await self.transfer_to_line_inner(
                player_id, target_shard_id, caller_session_jti
            )
        except asyncio.CancelledError:
            raise
        except BaseException:  # noqa: BLE001 —— 释放占坑后原样上抛
            try:
                await self.repo.clear_transfer_cooldown(player_id)
            except asyncio.CancelledError:
                raise
            except BaseException as cerr:  # noqa: BLE001
                plog.get().warning(
                    "clear_transfer_cooldown_failed", player_id=player_id, err=str(cerr)
                )
            raise

    async def require_caller_session_current(self, player_id: int, caller_jti: str) -> None:
        """玩家侧写路径**临界区内**的会话终检。对应 Go 的 `requireCallerSessionCurrent`。

        RPC 入口的 SessionCurrent 中间件只保证"进门时现行",到内部占坑 / CAS 之间是
        开放窗口;本检查在副作用临界点复核请求方自证 jti(Envoy 验签 payload 头)
        仍是权威当前代。

        `caller_jti` 空(内网直连 / dev 无证据)或 `sess_gate` 为 None → 跳过,保持
        dev 语义。**权威不可达一律 fail-closed**(`ErrUnavailable`):查不到 ≠ 会话有效。
        """
        if self.sess_gate is None or caller_jti == "":
            return
        try:
            cur, found = await self.sess_gate.current_jti(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "session authority unavailable during hub line transfer",
                cause=exc,
            ) from exc
        if not found:
            raise errcode.PandoraError(
                errcode.ErrUnauthorized,
                "player %d has no current session; hub line transfer rejected",
                player_id,
            )
        if cur != caller_jti:
            plog.get().warning("hub_transfer_session_superseded", player_id=player_id)
            raise errcode.PandoraError(
                errcode.ErrSessionSuperseded,
                "hub line transfer requested by a superseded session",
            )

    async def transfer_to_line_inner(
        self, player_id: int, target_shard_id: int, caller_session_jti: str = ""
    ) -> TransferToLineResult:
        """目标解析 + 满员判定 + 委托内部 `transfer_hub`。对应 Go 的 `transferToLineInner`。"""
        assignment = await self.repo.get_assignment(player_id)
        if assignment is None:
            raise errcode.PandoraError(
                errcode.ErrHubTransferNotInHub, "player %d not in any hub", player_id
            )
        if self.auth_repo is not None:
            _, reusable = await self.assignment_routable(player_id, assignment)
            if not reusable:
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "hub assignment is not bound to the current active credential",
                )

        shards = await self.repo.list_shards()
        # 目标线路必须是本 region、**同轨**的 ready 分片(§9.21:一次切线不该换轨)。
        target = None
        assignment_track = sticky_release_track(assignment.release_track)
        for s in shards:
            shard_track = sticky_release_track_or_none(s.release_track)
            if (
                shard_track is not None
                and shard_track == assignment_track
                and s.shard_id == target_shard_id
                and s.region == assignment.region
                and s.state == STATE_READY
            ):
                target = s
                break
        if target is None:
            raise errcode.PandoraError(
                errcode.ErrHubTransferFailed,
                "line shard_id=%d not available in region %s",
                target_shard_id,
                assignment.region,
            )
        # 已满且不是当前线路 → 明确"线路已满",而不是笼统的 transfer failed:
        # 客户端据此提示"换一条线",否则玩家只会反复点同一条。
        if (
            target.hub_pod_name != assignment.hub_pod_name
            and target.player_count >= target.capacity
        ):
            raise errcode.PandoraError(
                errcode.ErrHubLineFull, "line shard_id=%d is full", target_shard_id
            )

        # 进入不可逆迁移(占新→切归属→退旧)前的会话终检。入口中间件检查后到此处的
        # 窗口内若发生顶号,旧会话请求在这里被拒,**零** assignment / 容量 / 清退副作用。
        # 此检查到 CAS 之间的残余毫秒窗由下面的 post-check + 票据绑请求方 jti +
        # ACK 消费点复核(acknowledge_admission)三层兜底。
        await self.require_caller_session_current(player_id, caller_session_jti)

        # 迁移前先固定原线路:post-check 发现被顶时用它做条件回退。
        original_shard_id = assignment.shard_id

        tr = await self.transfer_hub(player_id, target_shard_id, caller_session_jti)
        # post-check:CAS 已落地后复核。此刻发现被顶,说明轮换落在终检与 CAS 之间的
        # 毫秒窗内。除扣票外(票本就绑旧 jti,兑换点必拒),还尝试把**路由副作用**回退
        # 到原线路:否则旧会话的失败请求仍把新会话的归属搬去目标线路(卡容量 / 改位置)。
        try:
            await self.require_caller_session_current(player_id, caller_session_jti)
        except asyncio.CancelledError:
            raise
        except BaseException:  # noqa: BLE001 —— 条件回退后原样上抛
            await self.revert_line_transfer(
                player_id, original_shard_id, tr, caller_session_jti
            )
            raise
        line_no = line_no_of_shard(
            shards, assignment.region, assignment_track, target_shard_id
        )
        return TransferToLineResult(
            new_hub_ds_addr=tr.new_hub_ds_addr,
            new_hub_ticket=tr.new_hub_ticket,
            new_shard_id=target_shard_id,
            line_no=line_no,
        )

    async def revert_line_transfer(
        self,
        player_id: int,
        original_shard_id: int,
        tr: TransferResult | None,
        caller_session_jti: str = "",
    ) -> None:
        """post-check 判被顶后,**条件回退**本次迁移的路由副作用。

        对应 Go 的 `revertLineTransfer`。

        ★ 只有当前归属仍是本次迁移落地的那个 `assignment_id` 时才回退。归属已被并发
          操作推进(新会话切线 / 清退链)就跳过 —— 回滚别人刚落地的结果比不回滚危险
          得多:那是把一个**已交付**的归属改掉,直接违反"绝不回退玩家已获得的东西"。

        失败仅告警,不改写调用方错误(新会话下次 resolve / 自行切线即可收敛)。
        """
        if tr is None or tr.new_assignment_id == "":
            return
        try:
            cur = await self.repo.get_assignment(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "transfer_supersede_revert_read_failed", player_id=player_id, err=str(exc)
            )
            return
        if cur is None or cur.assignment_id != tr.new_assignment_id:
            plog.get().info(
                "transfer_supersede_revert_skipped",
                player_id=player_id,
                reason="assignment already advanced by another actor",
            )
            return
        try:
            await self.transfer_hub(player_id, original_shard_id, caller_session_jti)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "transfer_supersede_revert_failed",
                player_id=player_id,
                original_shard_id=original_shard_id,
                err=str(exc),
            )
            return
        plog.get().info(
            "transfer_supersede_reverted",
            player_id=player_id,
            original_shard_id=original_shard_id,
        )

    # ── RPC 4:ListHubs ──────────────────────────────────────────────────

    async def list_hubs(self, region: str) -> list:
        """列出分片负载(运维 / 调试),region 非空时过滤。对应 Go 的 `ListHubs`。"""
        shards = await self.repo.list_shards()
        out = []
        for s in shards:
            if region != "" and s.region != region:
                continue
            out.append(
                hubpb.HubInfo(
                    hub_pod_name=s.hub_pod_name,
                    hub_addr=s.hub_addr,
                    region=s.region,
                    player_count=s.player_count,
                    capacity=s.capacity,
                    state=s.state,
                )
            )
        return out

    # ── RPC 5:Heartbeat ─────────────────────────────────────────────────

    async def heartbeat(
        self,
        pod: str,
        player_count: int,
        state: str,
        ts_ms: int,
        token_gen: int,
    ) -> HeartbeatResult:
        """legacy 心跳入口(Hub DS 每 5s 调)。对应 Go 的 `Heartbeat`。

        `token_gen` 是本次心跳携带的**已验签** DS 回调令牌代际(service 层从 Guard
        claims 的 `ds_gen` 取,无已验签令牌时为 0)。代际绑定下 warming→ready
        只接受与镜像代际**精确相等**的心跳。
        """
        res = await self._heartbeat(pod, player_count, None, 0, state, ts_ms, token_gen, None)
        self.apply_local_credential_ack(pod, res)
        return res

    def apply_local_credential_ack(self, pod: str, res: HeartbeatResult | None) -> None:
        """mode=local 下回显本机 Hub DS 的凭据 ACK。对应 Go 的 `applyLocalCredentialACK`。

        **为什么必须回显**:UE 的 `SendHubHeartbeat` 无条件用 `IsBoundToRequest` 校验
        应答里的 CredentialAck(五项须与 DS 自持凭据逐字段相等),不通过就不调
        `NotifyAuthorizedActiveHeartbeat`;而 `IsAcceptingNewPlayers()` 对
        local-off-v1 **没有豁免** —— 拿不到绑定式 ACK,准入租约永不打开,玩家连上
        大厅也会被拒。

        **为什么这不是"伪造回显"**:ACK 的值直接取自本进程签发、并经 env 下发给该
        DS 的**同一份**凭据,确是"服务端仍授权本实例"的真实证据。且严格 fail-closed:
        pod 不匹配 / 凭据不全 → 不回显;已有 Accepted 身份(Model B promote 已回填)
        → 不覆盖;`LocalHubCredentialSource` 只有 `LocalHubFleetProvider` 实现,
        Agones / Mock 的 `isinstance` 恒 False(线上是死代码)。
        """
        if res is None or res.accepted_instance_uid != "":
            return
        if not isinstance(self.fleet, LocalHubCredentialSource):
            return
        cred = self.fleet.local_credential_ack(pod)
        if cred is None:
            return
        res.accepted_token_gen = cred.gen
        res.accepted_token_jti = cred.jti
        res.accepted_instance_uid = cred.instance_uid
        res.accepted_protocol_epoch = cred.protocol_epoch
        res.accepted_writer_epoch = cred.writer_epoch

    async def heartbeat_with_credential(
        self,
        pod: str,
        player_count: int,
        player_ids: list[int] | None,
        max_players: int,
        state: str,
        ts_ms: int,
        cred: HubCredential | None,
    ) -> HeartbeatResult:
        """Model B 心跳入口。对应 Go 的 `HeartbeatWithCredential`。

        `auth_repo` 已装配 → 走 `activate_heartbeat` **单事务线性化点**:首个合法
        pending 心跳在 authKey + shardKey 同事务内原子完成 promote(pending→active)
        + 分片 warming→ready + 投影 active 元组;stale 一律 fail-closed
        (`ErrUnauthorized`,两键零变更)。
        """
        return await self._heartbeat(
            pod, player_count, player_ids, max_players, state, ts_ms, 0, cred
        )

    async def _heartbeat(  # noqa: PLR0913 —— 与 Go 的不导出 `heartbeat` 逐参对应
        self,
        pod: str,
        player_count: int,
        player_ids: list[int] | None,
        max_players: int,
        state: str,
        ts_ms: int,
        token_gen: int,
        cred: HubCredential | None,
    ) -> HeartbeatResult:
        """两条心跳入口的共同体。对应 Go 的不导出 `heartbeat`。"""
        self.require_writer()
        if pod == "":
            raise errcode.PandoraError(errcode.ErrInvalidArg, "hub_pod_name required")
        # 请求 ts_ms **不参与**授权 / 存活权威:统一用服务端接收时间。
        # 信 DS 上报的时刻,一台时钟超前的 DS 就能把自己的可路由窗口无限延长
        # (§9.6:派生判定一律服务端算)。
        ts_ms = L.now_ms()
        if self.auth_repo is not None:
            return await self.heartbeat_model_b(
                pod, player_count, player_ids, max_players, state, ts_ms, cred
            )
        found = await self.repo.heartbeat_shard(
            pod,
            player_count,
            state,
            ts_ms,
            token_gen,
            self.ds_token_generation,
            self.shard_ttl_sec(),
        )
        if not found:
            # 新建 / 重建的 Hub GameServer 可能早于周期拓扑刷新发来业务心跳。先主动刷
            # 一次拓扑,避免首跳把健康 pod 误判成孤儿并下发 stop(下了就是让一台好
            # DS 自杀)。
            reconciled = True
            try:
                await self.reconcile_shard_topology()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                reconciled = False
                plog.get().warning(
                    "heartbeat_topology_reconcile_failed", pod=pod, err=str(exc)
                )
            if reconciled:
                found = await self.repo.heartbeat_shard(
                    pod,
                    player_count,
                    state,
                    ts_ms,
                    token_gen,
                    self.ds_token_generation,
                    self.shard_ttl_sec(),
                )
            if not found:
                plog.get().warning("heartbeat_unknown_hub_waiting_topology", pod=pod)
                raise errcode.PandoraError(
                    errcode.ErrUnavailable, "hub shard %s topology not confirmed", pod
                )
        # 分片被标记 draining / stopping → 下发迁移 / 停机指令(与 Kafka 推送双通道)。
        shard = None
        try:
            shard = await self.repo.get_shard(pod)
        except asyncio.CancelledError:
            raise
        except BaseException:  # noqa: BLE001 —— 对齐 Go 的 `gerr == nil && ok`
            shard = None
        if shard is not None:
            # legacy 面 owner 实例租约续写(与 Model B 心跳同语义、同时序约束):
            # login 的 applyOwnerPlacement 只在「ADMITTED 且实例租约剩余 > 安全余量」
            # 时报 STABLE。legacy 心跳不续租 = 租约恒过期,客户端 Admission ACK 已到手
            # 却永远等不到 STABLE,30s deadline 后反复重查(mode=local 实测的第三道墙)。
            # 实例身份回源分片镜像;mock 镜像无实例身份 → uid 为空跳过,行为不变。
            uid = shard.gameserver_uid
            if uid != "":
                await renew_owner_lease_gate(
                    self.owner_lease, self.owner_lease_required, pod, uid, 0, ""
                )
            if shard.state == STATE_DRAINING:
                return HeartbeatResult(
                    command=COMMAND_DRAIN, grace_seconds=self.cfg.migrate_grace_seconds
                )
            if shard.state == STATE_STOPPING:
                return HeartbeatResult(
                    command=COMMAND_STOP, grace_seconds=self.cfg.migrate_grace_seconds
                )
        return HeartbeatResult(command=COMMAND_NONE)

    async def heartbeat_model_b(  # noqa: PLR0913 —— 与 Go 逐参对应
        self,
        pod: str,
        player_count: int,
        player_ids: list[int] | None,
        max_players: int,
        state: str,
        ts_ms: int,
        cred: HubCredential | None,
    ) -> HeartbeatResult:
        """Model B 心跳(`auth_repo` 已装配)。对应 Go 的 `heartbeatModelB`。

        `cred is None`(legacy 令牌 / 无凭据)在 Model B 下**一律拒**:Redis 授权权威
        模式不接受未携带 Model B 凭据的心跳借旧令牌保活或翻 ready(纵深防御,
        service 层也会拦)。
        """
        if cred is None:
            raise errcode.PandoraError(
                errcode.ErrUnauthorized,
                "hub heartbeat requires model B credential under redis authority",
            )
        players = list(player_ids or ())
        inp = AR.ActivateHeartbeatInput(
            player_count=player_count,
            player_ids=tuple(players),
            max_players=max_players,
            state=state,
            ts_ms=ts_ms,
            auth_ttl_sec=self.auth_ttl_sec(),
            shard_ttl_sec=self.shard_ttl_sec(),
        )
        ident = hub_credential_identity(cred)
        # 授权未激活 / 不匹配 / 相位锁定一律上抛(ErrUnauthorized),fail-closed。
        res = await self.auth_repo.activate_heartbeat(pod, ident, inp)
        if not res.shard_found:
            # 分片镜像缺失(孤儿 / 早于拓扑种子):先刷一次拓扑再重试,保证 promote 与
            # ready **同事务**(杜绝半激活:授权已 promote 而分片还在 warming)。
            try:
                await self.reconcile_shard_topology()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "heartbeat_topology_reconcile_failed", pod=pod, err=str(exc)
                )
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "hub shard %s topology reconcile: %s",
                    pod,
                    exc,
                    cause=exc,
                ) from exc
            res = await self.auth_repo.activate_heartbeat(pod, ident, inp)
            if not res.shard_found:
                plog.get().warning("heartbeat_unknown_hub_waiting_topology", pod=pod)
                raise errcode.PandoraError(
                    errcode.ErrUnavailable, "hub shard %s topology not confirmed", pod
                )
        # owner 权威实例租约双写:必须在心跳响应返回前完成,强 / 弱依赖语义见
        # `renew_owner_lease_gate`。hub 凭据无实例纪元 → epoch 传 0。
        await renew_owner_lease_gate(
            self.owner_lease, self.owner_lease_required, pod, res.instance_uid, 0, ""
        )
        # owner 迁移准入代提交(授权 census 即准入证据)。弱依赖,失败 / 屏障未开都不
        # 影响心跳。**无条件调用**(不能用 `if players` 守卫):空 census(最后一名玩家
        # 离场)也必须进函数按本实例剪枝 admitted 缓存,否则 stale 项残留、玩家回流
        # 同实例被误吞跳过 Admit。
        await OA.owner_admit_census_weak(
            self.owner_auth,
            self.owner_admitted,
            players,
            OA.OWNER_TYPE_HUB,
            pod,
            res.instance_uid,
            CENSUS_ADMIT_BUDGET_SEC,
            self.resolve_owner_target_from_assignment,
        )
        command, grace_seconds = COMMAND_NONE, 0
        if res.shard_state == STATE_DRAINING:
            command, grace_seconds = COMMAND_DRAIN, self.cfg.migrate_grace_seconds
        elif res.shard_state == STATE_STOPPING:
            command, grace_seconds = COMMAND_STOP, self.cfg.migrate_grace_seconds
        out = modelb_heartbeat_result(res, command, grace_seconds)
        try:
            orders = await self.pending_hub_eviction_orders(
                pod, res.instance_uid, res.protocol_epoch, res.writer_epoch
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 心跳 / 授权**已经提交**。保持 DS 授权租约健康、下一跳重试发现驱逐令;
            # 绝不能在那次提交之后把一次健康心跳降级成"凭据失败"(DS 会据此自杀)。
            plog.get().warning("hub_eviction_order_discovery_failed", pod=pod, err=str(exc))
        else:
            out.eviction_orders = orders
        return out

    async def pending_hub_eviction_orders(  # noqa: C901,PLR0912
        self, source_pod: str, instance_uid: str, protocol_epoch: int, writer_epoch: int
    ) -> list[HubEvictionOrder]:
        """把持久清理索引投影成源 DS 的控制面驱逐令。对应 Go 的 `pendingHubEvictionOrders`。

        这里对 connected 账本是**只读**的:驱逐令要到 DS 真的踢掉那条 exact local
        admission、并由 `acknowledge_departure` 摘除(或确认的 GameServer teardown
        清掉整份 UID 账本)才算完成。服务端在 PC/Pawn 尚存时**不能**自证 Departure。
        """
        refs = await self.repo.list_transfer_cleanups(source_pod)
        orders: list[HubEvictionOrder] = []
        for ref in refs:
            if len(orders) >= MAX_HUB_EVICTION_ORDERS_PER_HEARTBEAT:
                break
            assignment = await self.repo.get_assignment(ref.player_id)
            if assignment is None or assignment.assignment_id != ref.target_assignment_id:
                continue  # 孤儿清理由 reconciler 移除
            if assignment.transfer_cleanup_pending:
                if not assignment.transfer_target_bound:
                    # 目标 Bind 未持久之前**绝不**驱逐源:那一刻玩家的新家还没建好。
                    continue
                source = transfer_cleanup_source(assignment)
            elif assignment.release_cleanup_pending:
                source = assignment
            else:
                continue
            if (
                source.hub_pod_name != source_pod
                or source.hub_instance_uid != instance_uid
                or source.auth_epoch != protocol_epoch
                or source.auth_writer_epoch != writer_epoch
            ):
                # 替换实例**永远不该**收到指向死旧 UID 的驱逐令(它会照单踢掉自己身上
                # 那个同名玩家)。那份账本的清理证明归权威 GameServer teardown。
                continue
            seat = await self.auth_repo.inspect_assignment_seat(
                source_pod, assignment_instance_identity(source)
            )
            if seat.conflict:
                # 源席位账本上出现与预期不符的 owner 身份 = 潜在双 owner / 脑裂
                # (§9.22 核心不变量)。上层只把它泛化成 hub_eviction_order_discovery_failed,
                # 丢了"是身份冲突"这一关键区分 → 在此打 ERROR 带精确 fencing 身份。
                plog.get().error(
                    "hub_eviction_owner_conflict",
                    source_pod=source_pod,
                    instance_uid=instance_uid,
                    protocol_epoch=protocol_epoch,
                    writer_epoch=writer_epoch,
                )
                raise errcode.PandoraError(
                    errcode.ErrInvalidState, "Hub eviction source owner identity conflict"
                )
            if not seat.connected:
                continue
            orders.append(
                HubEvictionOrder(
                    player_id=ref.player_id,
                    assignment_id=source.assignment_id,
                    admission_id=seat.admission_id,
                    admission_seq=seat.admission_seq,
                    source_instance_uid=source.hub_instance_uid,
                    source_protocol_epoch=source.auth_epoch,
                    source_writer_epoch=source.auth_writer_epoch,
                    cleanup_assignment_id=ref.target_assignment_id,
                )
            )
        return orders

    async def acknowledge_admission(  # noqa: C901,PLR0912,PLR0915 —— 与 Go 同形状
        self,
        player_id: int,
        assignment_id: str,
        pod: str,
        admission_id: str,
        admission_seq: int,
        ticket_session_jti: str,
        cred: HubCredential | None,
    ) -> AcknowledgeAdmissionResult:
        """把本地已验签 Hub DSTicket 对应的 reservation 原子转为 connected owner。

        对应 Go 的 `AcknowledgeAdmission`。这是 §9.23 的**服务端完成点**:
        Admitted=true 正是 DS 开 spawn gate 的信号。

        `ticket_session_jti` 是票据的 `sjti` claim:v2 Hub 本地验票**不经 Login 在线
        兑换**,ACK 是唯一在线权威接触点,装配 `sess_gate` 后在消费 reservation
        **之前**复核会话现行性。
        """
        self.require_writer()
        if self.auth_repo is None or cred is None:
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "hub admission requires model B authority"
            )
        # 会话现行性**前置**复核:失败时不消费 reservation、不产生任何副作用。
        # 空 sjti 由 `sess_gate_require_sjti` 门控制(滚动兼容):默认兼容档告警放行
        # (旧 Hub DS 不转发 sjti / 旧签发面残票);全 fleet DS 排空 + 票据最大 TTL 过后
        # 由运维置 True 硬拒。非空 sjti 无论档位都全量复核,不可达 fail-closed。
        if self.sess_gate is not None:
            if ticket_session_jti == "":
                if self.sess_gate_require_sjti:
                    raise errcode.PandoraError(
                        errcode.ErrUnauthorized,
                        "hub admission ticket lacks session binding (sjti); reissue required",
                    )
                plog.get().warning(
                    "hub_admission_missing_sjti_tolerated",
                    player_id=player_id,
                    assignment_id=assignment_id,
                    pod=pod,
                    hint="混版兼容窗;旧 DS 排空后开 session_gate.require_ticket_sjti 收口",
                )
            else:
                try:
                    cur_jti, cur_found = await self.sess_gate.current_jti(player_id)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrUnavailable,
                        "session authority unavailable during hub admission",
                        cause=exc,
                    ) from exc
                if not cur_found:
                    raise errcode.PandoraError(
                        errcode.ErrUnauthorized,
                        "player %d has no current session; hub admission rejected",
                        player_id,
                    )
                if cur_jti != ticket_session_jti:
                    plog.get().warning(
                        "hub_admission_session_superseded",
                        player_id=player_id,
                        assignment_id=assignment_id,
                        pod=pod,
                    )
                    raise errcode.PandoraError(
                        errcode.ErrSessionSuperseded,
                        "hub admission ticket was issued for a superseded session",
                    )

        assignment = await self.repo.get_assignment(player_id)
        if assignment is not None and (
            assignment.transfer_cleanup_pending or assignment.release_cleanup_pending
        ):
            # 这发生在目标 AcknowledgeAdmission **之前**:Redis / 源清理失败是可重试的,
            # 不会创建任何新的目标 session / spawn。
            assignment, still_found = await self.resume_assignment_cleanup(
                player_id, assignment.assignment_id
            )
            if not still_found:
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "Hub admission assignment was released during owner cleanup",
                )
        if assignment is None or not assignment_matches_admission(
            assignment, player_id, assignment_id, pod, cred
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "hub admission assignment is no longer current"
            )
        ident = hub_credential_identity(cred)
        reservation = L.ReservationIdentity(
            player_id=player_id,
            assignment_id=assignment_id,
            instance_uid=cred.instance_uid,
            protocol_epoch=cred.protocol_epoch,
            writer_epoch=cred.writer_epoch,
        )
        result = await self.auth_repo.acknowledge_admission(
            pod, ident, reservation, admission_id, admission_seq, L.now_ms(), self.shard_ttl_sec()
        )
        if not result.admitted:
            return AcknowledgeAdmissionResult(admitted=False)
        # durable ledger 写是入场线性化点,写成功后必须**再复核一次**会话现行性,
        # 关闭「预检通过 → 消费 reservation 之间轮换」的 TOCTOU。
        #
        # 结果分型(回退只用于**确定性否定**):
        #   - 权威不可达 = 结果未知:**不回退** connected owner,返回 Unavailable。
        #     ledger 的 ACK 对相同 (admission_id, seq) 幂等,DS 用同一 identity 重试会
        #     完整重跑两次会话复核拿到确定结果;若回退,普通 reservation 已被消费且
        #     Departure 不会恢复它,重试必然 fail-closed 死路 —— 玩家被迫整链重 resolve。
        #     owner 保留期间客户端仍未过 spawn gate,无授权面影响。
        #   - 确定性否定(会话消失 / 已被顶):exact 回退刚建立的 connected owner
        #     (同 identity 的 Departure,幂等)。回退后同票重试本就该失败(持票会话已死)。
        #   - 回退本身失败也拒绝:seat 残留由 DS Kick 后的物理 Logout proof 收敛,
        #     绝不向已判定非现行的会话开门。
        # 空 sjti(兼容窗放行)无绑定可比,跳过。
        if self.sess_gate is not None and ticket_session_jti != "":
            gerr: BaseException | None = None
            cur_jti, cur_found = "", False
            try:
                cur_jti, cur_found = await self.sess_gate.current_jti(player_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                gerr = exc
            if gerr is not None:
                plog.get().warning(
                    "hub_admission_postcheck_indeterminate",
                    player_id=player_id,
                    assignment_id=assignment_id,
                    pod=pod,
                    err=str(gerr),
                    hint="owner 保留,DS 以同 identity 重试 ACK 重跑复核;spawn gate 未开",
                )
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "session authority unavailable during hub admission post-check",
                    cause=gerr,
                ) from gerr
            if not cur_found or cur_jti != ticket_session_jti:
                try:
                    await self.auth_repo.acknowledge_departure(
                        pod,
                        ident,
                        reservation,
                        admission_id,
                        admission_seq,
                        L.now_ms(),
                        self.shard_ttl_sec(),
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as derr:  # noqa: BLE001
                    plog.get().error(
                        "hub_admission_postcheck_revert_failed",
                        player_id=player_id,
                        assignment_id=assignment_id,
                        pod=pod,
                        err=str(derr),
                        hint="connected owner 残留,等待 DS Kick 后物理 Logout proof 收敛",
                    )
                if not cur_found:
                    raise errcode.PandoraError(
                        errcode.ErrUnauthorized,
                        "player %d session vanished during hub admission; spawn refused",
                        player_id,
                    )
                plog.get().warning(
                    "hub_admission_postcheck_superseded",
                    player_id=player_id,
                    assignment_id=assignment_id,
                    pod=pod,
                )
                raise errcode.PandoraError(
                    errcode.ErrSessionSuperseded,
                    "hub admission superseded by a newer login before spawn gate opened",
                )
        # assignment 与 {pod} ledger 不同 slot:ACK 后必须**再查一次**。若 Transfer /
        # Release 已赢得 CAS,保留 exact connected owner 并拒绝开放 spawn gate。
        current = await self.repo.get_assignment(player_id)
        if current is None or not assignment_matches_admission(
            current, player_id, assignment_id, pod, cred
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "hub admission assignment changed during acknowledge"
            )
        # owner 权威准入(§9.23 服务端完成点)。这里是 Admission 链的**原生**提交点,
        # 取代原先由心跳 census 代提交的近似 —— census 只能证明"该实例正在服务该玩家",
        # 而本处是玩家**本次**进场的线性化点,exact identity 就在手上。
        owner_target, owner_target_ok = await self.owner_target_for_hub_ticket(current)
        if not owner_target_ok:
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "hub admission current assignment lacks exact owner target",
            )
        await self.admit_owner_for_admission(player_id, owner_target)
        return AcknowledgeAdmissionResult(admitted=result.admitted)

    async def admit_owner_for_admission(
        self, player_id: int, target: OwnerTargetView
    ) -> None:
        """把 owner 记录从 PENDING 推进到 ADMITTED(§9.23 完成点)。

        对应 Go 的 `admitOwnerForAdmission`。

        先 Query 再 Admit **不是** TOCTOU:Admit 自身在 owner 的行锁事务内按 exact
        (player_id, owner_epoch, operation_id, 实例四元组)做 CAS,查到的值只是拿来当
        CAS 期望;期间记录若被推进,Admit 会以 IDENTITY_MISMATCH / EPOCH_CONFLICT 拒绝。

        `owner_auth is None` = owner 未部署(部署形态问题),不在本函数收敛。
        """
        if self.owner_auth is None:
            return
        try:
            rec = await self.owner_auth.query_owner(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 结果不可判定 → fail-closed,绝不冒充"已准入"开门(§9.22)。
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "owner authority unavailable during hub admission player=%d",
                player_id,
                cause=exc,
            ) from exc
        if (
            rec.owner_type != OA.OWNER_TYPE_HUB
            or rec.pod_name != target.pod_name
            or rec.instance_uid != target.instance_uid
            or rec.instance_epoch != target.instance_epoch
            or rec.assignment_or_allocation_id != target.assignment_or_allocation_id
            or rec.release_track != target.release_track
        ):
            # 归属已经不指向本次 exact assignment(Transfer / 顶号 / 同实例换发 / 灾备
            # 接管都会走到这里)。assignment / track 校验**必须早于** ADMITTED 幂等快路;
            # 否则旧 assignment 只要仍在同一物理实例,就能绕过 owner Admit 的 exact CAS
            # 开 spawn gate。不 Admit、不开门:玩家的 owner 在别处,这台 DS 无权创建
            # 可操作玩家态。
            plog.get().warning(
                "hub_admission_owner_points_elsewhere",
                player_id=player_id,
                want_pod=target.pod_name,
                ds_pod=target.pod_name,
                want_uid=target.instance_uid,
                want_epoch=target.instance_epoch,
                hub_assignment_id=target.assignment_or_allocation_id,
                want_release_track=target.release_track,
                owner_pod=rec.pod_name,
                owner_type=rec.owner_type,
                owner_uid=rec.instance_uid,
                owner_instance_epoch=rec.instance_epoch,
                owner_assignment_id=rec.assignment_or_allocation_id,
                owner_release_track=rec.release_track,
                owner_epoch=rec.owner_epoch,
                operation_id=rec.operation_id,
                reason=REASON_OWNER_POINTS_ELSEWHERE,
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "owner no longer points at this exact hub assignment; admission refused player=%d",
                player_id,
            )
        if rec.phase == OA.OWNER_PHASE_ADMITTED:
            # 幂等:ACK 重放 / 回包丢失后重试,原样返回已准入,不产生第二个 owner。
            return
        if rec.phase != OA.OWNER_PHASE_PENDING:
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "owner phase %d is not admissible player=%d",
                rec.phase,
                player_id,
            )
        admit_err: BaseException | None = None
        try:
            await self.owner_auth.admit(player_id, rec.owner_epoch, rec.operation_id, target)
            return
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 按 retry_after 分流
            # ★ 必须把 exc 搬出 except 块:Python 在 except 结束时会 `del exc`
            #   (避免 traceback 引用环),块外再用就是 NameError —— 而这条路径本来
            #   就是失败路径,那会把一次可诊断的业务拒绝变成一个无关的 NameError。
            admit_err = exc
        retry_after_ms = (
            admit_err.retry_after_ms if isinstance(admit_err, errcode.PandoraError) else 0
        )
        if retry_after_ms > 0:
            # admit_not_before 屏障未开:旧 DS 最晚安全截止时间之前放行就是双 DS。
            # 可重试错误 + 剩余毫秒,DS 退避后用同 identity 重放 ACK(§9.23 WAIT 语义)。
            #
            # 级别是 warning 而不是 debug:屏障未开是玩家"卡在进场最后一步"的头号成因
            # (DS 会按秒重放 ACK),而 retry_after_ms / owner_epoch 是唯一能判"还要等
            # 多久"的依据字段。
            plog.get().warning(
                "hub_admission_barrier_not_open",
                player_id=player_id,
                ds_pod=target.pod_name,
                hub_assignment_id=target.assignment_or_allocation_id,
                retry_after_ms=retry_after_ms,
                owner_epoch=rec.owner_epoch,
                operation_id=rec.operation_id,
                reason=REASON_OWNER_BARRIER_NOT_OPEN,
            )
            err = errcode.PandoraError(
                errcode.ErrUnavailable,
                "owner admit barrier not open; retry after %dms",
                retry_after_ms,
                cause=admit_err,
            )
            # Go 靠返回值把 retryAfterMs 交给调用方;Python 用异常传播就**必须**把它
            # 挂回去(errcode.PandoraError 的声明式 slot),否则 §9.23 要求的
            # "WAIT 必须带明确 retry_after" 在这一跳静默丢失,DS 只能空转或干等。
            err.retry_after_ms = retry_after_ms
            raise err from admit_err
        raise errcode.PandoraError(
            errcode.ErrUnavailable,
            "owner admit failed during hub admission player=%d",
            player_id,
            cause=admit_err,
        ) from admit_err

    async def acknowledge_local_admission(
        self, player_id: int, assignment_id: str, pod: str
    ) -> AcknowledgeAdmissionResult:
        """mode=local(legacy 权威面)下的准入完成点。对应 Go 的 `AcknowledgeLocalAdmission`。

        **为什么必须单开一条路径**:`acknowledge_admission` 消费的是 Redis 授权面的
        reservation,而 local-off-v1 压根没有 `auth_repo` —— 走那条第一行就撞
        "hub admission requires model B authority" 返回 `ErrUnauthorized`。DS 侧的重试
        策略把 UNAUTHORIZED / INVALID_ARG 判为"明确拒绝" → FailAdmission → KickPlayer,
        于是玩家过了 PostLogin 的可信 claims 门,却在紧接着的 Admission ACK 上被踢,
        表现为"连上大厅立刻掉线"。

        **本路径不发明新的授权语义**,只做 Model B 也做的两件事:①用归属记录复核
        (player, assignment, pod) 三元组仍是当前分配;②调用**完全相同**的
        `admit_owner_for_admission`。

        **被刻意省略的是 reservation 消费与 admission_id/seq 排序**:它们是 Redis 授权面
        用来在多实例下仲裁"谁是当前 owner"的对象,而 local 下只有一台 Hub DS,不存在
        可仲裁的对象。真要多实例就该上 Model B,而不是给 legacy 面伪造一套 reservation。
        """
        self.require_writer()
        if self.auth_repo is not None:
            # 装了 Redis 授权面还走 legacy 准入 = 绕过 reservation 仲裁,fail-closed。
            raise errcode.PandoraError(
                errcode.ErrUnauthorized,
                "local hub admission is rejected while model B authority is configured",
            )
        if player_id == 0 or assignment_id == "" or pod == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "incomplete local hub admission identity"
            )
        assignment = await self.repo.get_assignment(player_id)
        if (
            assignment is None
            or assignment.player_id != player_id
            or assignment.assignment_id != assignment_id
            or assignment.hub_pod_name != pod
        ):
            # 与 Model B 同语义:票据签发后玩家已被重新分配 / 释放,这张票不再代表当前归属。
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "hub admission assignment is no longer current player=%d",
                player_id,
            )
        target, ok = await self.owner_target_for_hub_ticket(assignment)
        if not ok:
            # 拼不出 exact 身份就不能 Admit(§9.22)。签票点同样拼不出时压根不会 Begin,
            # 此刻 owner 面无记录,开门等于无权威背书 —— 宁可拒一次让客户端重试。
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "hub admission lacks exact owner identity player=%d",
                player_id,
            )
        await self.admit_owner_for_admission(player_id, target)
        return AcknowledgeAdmissionResult(admitted=True)

    async def acknowledge_local_departure(
        self, player_id: int, assignment_id: str, pod: str
    ) -> AcknowledgeDepartureResult:
        """mode=local(legacy 权威面)下的离场确认点。对应 Go 的 `AcknowledgeLocalDeparture`。

        **为什么必须单开一条路径**:与 `acknowledge_local_admission` 完全同源 ——
        local-off-v1 没有 `auth_repo`,走 `acknowledge_departure` 第一道门就撞
        "hub departure requires model B authority"。而 DS 侧只认三层全 OK,拿不到
        code=0 就按 1s 周期**无限重试**:日志被灌爆,队列项永不出队。

        **本路径没有可删对象**:Model B 删的是 Redis 授权面里的 connected ownership,
        local 下那条记录压根不存在。玩家在线态由 player_locator 的 30s TTL 自然收敛,
        大厅归属由 Release / Transfer 显式替换。返回 `departed=True` 表达幂等语义:
        该 exact identity 的 connected owner 此刻确实不存在。

        **刻意不复核归属三元组**(与 `acknowledge_local_admission` 相反):准入是开门,
        归属过期必须 fail-closed 拒(否则一人两 DS);离场是关门,没有可授权的对象。
        且玩家离场最常见的原因就是归属已被改写(进 Battle / 顶号 / 切线),此时复核
        必然失败 → 返回非 OK → DS 又回到每秒重试,正好把本函数要修的 bug 原样重建。
        """
        self.require_writer()
        if self.auth_repo is not None:
            raise errcode.PandoraError(
                errcode.ErrUnauthorized,
                "local hub departure is rejected while model B authority is configured",
            )
        if player_id == 0 or assignment_id == "" or pod == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "incomplete local hub departure identity"
            )
        return AcknowledgeDepartureResult(departed=True)

    async def acknowledge_departure(
        self,
        player_id: int,
        assignment_id: str,
        pod: str,
        admission_id: str,
        admission_seq: int,
        cred: HubCredential | None,
    ) -> AcknowledgeDepartureResult:
        """exact 删除当前 admission owner。对应 Go 的 `AcknowledgeDeparture`。

        `conflict` 由旧连接晚到的 Logout 触发 —— 那条 identity 已经不是当前 owner,
        必须零副作用返回,而不是"顺手删掉现在这个"。

        ★ Departure 只移除**物理** connected ownership。持久 assignment / 成员索引保留
          到 exact Release / Transfer 相位替换或删除它,这样离线玩家在本分片排空时
          仍然可枚举(否则一次掉线就让他从强制整合的名单里消失)。
        """
        self.require_writer()
        if self.auth_repo is None or cred is None:
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "hub departure requires model B authority"
            )
        ident = hub_credential_identity(cred)
        result = await self.auth_repo.acknowledge_departure(
            pod,
            ident,
            L.ReservationIdentity(
                player_id=player_id,
                assignment_id=assignment_id,
                instance_uid=cred.instance_uid,
                protocol_epoch=cred.protocol_epoch,
                writer_epoch=cred.writer_epoch,
            ),
            admission_id,
            admission_seq,
            L.now_ms(),
            self.shard_ttl_sec(),
        )
        return AcknowledgeDepartureResult(
            departed=result.departed, conflict=result.conflict
        )

    # ── 在场保活(fire-and-forget)────────────────────────────────────────

    def refresh_hub_presence(
        self, pod: str, player_ids: list[int], bearer_token: str
    ) -> None:
        """把心跳捎带的在场 player_ids 转发给 player_locator 批量续期 HUB TTL。

        对应 Go 的 `RefreshHubPresence`。在线保活链路:DS 每 5s 上报,locator TTL 30s,
        玩家掉线 → DS 停报该 id → 30s 自然过期 = 好友视角离线。

        **fire-and-forget**,三条缺一不可:
          - 走 `safego.spawn` 带 name(裸 `create_task` 的异常会被静默吞进 Task,
            直到 GC 才打一条认不出主人的 "never retrieved");
          - **独立短超时**(`PRESENCE_REFRESH_TIMEOUT_SEC`):locator 抖动 / 卡死既不
            拖慢心跳响应尾延迟,也不泄漏协程;
          - best-effort 弱依赖:未配(None)/ 转发失败只记 Warn,绝不影响心跳主流程
            (心跳是分片存活信号,不能因旁路观测链路抖动而失败)。

        ★ `player_ids` 必须**拷贝**:调用方(gRPC 请求对象)会复用/回收那块内存,
          异步任务读到的就不是这次心跳的名单了。
        ★ `bearer_token` 仅闭包内存中短暂转发;**禁止**日志 / 持久化。
        """
        if self.locator is None or pod == "" or not player_ids:
            return
        players = list(player_ids)  # 拷贝,脱离调用方切片复用
        token = bearer_token

        async def _run() -> None:
            try:
                async with asyncio.timeout(PRESENCE_REFRESH_TIMEOUT_SEC):
                    await self.locator.refresh_hub_locations(pod, players, token)
            except asyncio.CancelledError:
                # ★ 必须先于宽 except 放行:被吞掉会让停机时这条任务不退出。
                raise
            except BaseException as exc:  # noqa: BLE001 —— 弱依赖,失败只告警
                plog.get().warning(
                    "hub_presence_refresh_failed",
                    pod=pod,
                    players=len(players),
                    err=str(exc),
                )

        safego.spawn("hub_presence_refresh", _run)
