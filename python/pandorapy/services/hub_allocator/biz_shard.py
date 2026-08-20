"""hub_allocator 业务层**分片 / 归属 / 签票辅助** —— 对应 Go 侧
`services/battle/hub_allocator/internal/biz/hub.go` 的 `// ── 内部辅助` 段
(第 2138–3440 行)。

本模块提供 `ShardMixin`(带 `(u *HubUsecase)` receiver 的那批)与一组模块级纯函数
(Go 里没有 receiver 的那批)。最终组装:

    class HubUsecase(SweepMixin, RpcMixin, ShardMixin, HubUsecaseBase): ...

字段(`self.repo` / `self.fleet` / `self.auth_repo` / `self.cfg` / TTL accessor /
`require_writer` / `confirm_writer_for_ticket` / `mint_source_revision` …)全部定义在
`biz_base.HubUsecaseBase`,本文件一个都不重复声明 —— 重复声明会在 MRO 里制造
"两份同名字段,谁先初始化谁赢"的静默分叉。

`add_shard_member` / `remove_shard_member` 由**另一批**(Go hub.go:3958/3964)提供,
本模块只调用不实现:它们是 best-effort 的成员反向索引维护,与本批的座位 / 归属
saga 完全不同的失败语义(漂移不影响正确性),硬塞进来会让两种语义的补偿规则混在
一起改。

═══════════════════════════════════════════════════════════════════════════════
这批代码是 §9 不变量 21 / 22 / 23 的**执行体**,三条各自落在哪
═══════════════════════════════════════════════════════════════════════════════

  §9.21(金丝雀共存)
      `sticky_release_track` 是**唯一**的旧值迁移规则:空轨 → stable,其余未知值
      fail-closed。`least_loaded` / `select_transfer_target` / `select_and_reserve_shard`
      / `assignment_routable` 全部按归一化后的轨严格筛 —— 少一处,已粘 canary 的
      玩家就会被甩回 stable。

  §9.22(唯一 owner + fencing + 再入屏障)
      `assignment_binding_v2_complete` / `assignment_same_instance` 是"同名 Pod 凭据
      轮换"与"同名 GameServer 重建"的分界;`reserve_routable_seat` 把授权 + 路由 +
      占座压进**一个** {pod} 事务(消灭 TOCTOU);`resume_assignment_cleanup` 是
      transfer/release 旧 owner 驱逐的唯一相位驱动,**无进程内状态**,每次重试都从
      当前 assignment 重新开始并用 exact CAS 推进。

  §9.23(单一幂等进场链)
      `replace_assignment_saga` 是"把玩家归属置换为 next"的唯一实现;
      `prepare_hub_ticket`(纯确定性,无副作用)与
      `bind_owner_for_published_hub_assignment`(唯一 owner 副作用点)刻意拆开,
      于是"签名器失败"不会发布归属、也不会泄漏座位,而 CAS loser 机械上碰不到 owner。

═══════════════════════════════════════════════════════════════════════════════
与 Go 的形变(其余逐行同构,逐条都有理由)
═══════════════════════════════════════════════════════════════════════════════

  1. `(值, error)` → 返回值 + 抛异常。**唯一例外**是那些 Go 用 `bool` 表达
     "查到了没 / 要不要重试"这种**控制流**的二元组:
     `assignment_routable` → `(ReserveResult, bool)`、
     `owner_target_for_hub_ticket` → `(OwnerTargetView, bool)`、
     `resume_assignment_cleanup` → `(rec | None, bool)`、
     `replace_assignment_saga` → `retry: bool`。
     把控制流也改成异常会让"CAS 输了要重试"和"存储炸了"长得一样。

  2. `stickyReleaseTrack` 的错误类型。逻辑**直接复用** `fleet.sticky_release_track`
     (那是全仓唯一实现),但它抛 `ValueError`,而 Go 抛的是
     `errcode.ErrInvalidState` —— 错误码会一路传到 RPC 边界。所以这里包一层做
     码值对齐,并另给一个不抛的 `sticky_release_track_or_none` 供
     `least_loaded` / census 这类"Go 里只判 `trackErr == nil`"的调用点使用
     (那些地方用异常做控制流会把一次正常的脏数据跳过变成整轮失败)。

  3. `shardExclusionCensus.fields()` 在 Go 返回 `[]any` 交替 kv,这里返回 `dict`
     直接给 `**kwargs`。**字段名逐字节不变** —— 它们是 Loki / Grafana 面板的列名,
     两栈并存期改一个字母,面板就只统计到另一半的量,而"少了一半"没有任何告警。

  4. `proto.Clone` → `repo.clone`(`CopyFrom`,带走 unknown fields)。
     read-modify-write 路径**禁止** `DiscardUnknown`(§9 不变量 17):混版期旧副本
     每回写一次就把新副本的字段静默抹掉一批。

  5. `time.Now().UnixMilli()` → `ledger.now_ms()`(全仓唯一实现,不再各写一份)。
"""

from __future__ import annotations

import asyncio
import dataclasses
from typing import Any, Protocol, runtime_checkable

from pandora.hub.v1 import allocator_pb2 as hubpb

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import releasetrack
from pandorapy.auth import DS_AUTH_WRITER_EPOCH_V2
from pandorapy.services.hub_allocator import fleet as F
from pandorapy.services.hub_allocator import ledger as L
from pandorapy.services.hub_allocator import owner_authority as OA
from pandorapy.services.hub_allocator import repo as R
from pandorapy.services.hub_allocator.biz_base import (
    REASON_ALL_CANDIDATES_RESERVE_REJECTED,
    REASON_ALL_SHARDS_DRAINING,
    REASON_ALL_SHARDS_FULL,
    REASON_ALL_SHARDS_TRACK_INVALID,
    REASON_ALL_SHARDS_WARMING,
    REASON_CANDIDATES_VANISHED,
    REASON_INVALID_RELEASE_TRACK,
    REASON_LIST_SHARDS_FAILED,
    REASON_NO_SHARD_CANDIDATE,
    REASON_NO_SHARD_IN_RELEASE_TRACK,
    REASON_NO_SHARD_IN_REGION,
    REASON_NO_SHARD_MIRROR,
    REASON_RESERVE_SEAT_ERROR,
    REASON_SHARD_EXCLUDED_POD,
    REASON_SHARD_FULL,
    REASON_SHARD_NOT_READY,
    REASON_SHARD_REGION_MISMATCH,
    REASON_SHARD_TRACK_INVALID,
    REASON_SHARD_TRACK_MISMATCH,
    REASON_TEAM_SHARD_LOOKUP_FAILED,
    STATE_DRAINING,
    STATE_READY,
    STATE_STOPPING,
    STATE_WARMING,
    AssignResult,
    HubTicketBinding,
)
from pandorapy.services.hub_allocator.owner_lease_client import OwnerTargetView

# `shard_id` 是 proto 的 uint32。Go 的 `targetHubID > math.MaxUint32` 判据在 Python
# 里**必须显式写出来**:Go 的 `uint32(targetHubID)` 会静默截断,Python 的 int 不会,
# 于是同一个越界 target_hub_id 在两栈会得出不同的匹配结果(Go 可能误匹配到低位相同
# 的分片,Python 恒不匹配)。照抄 Go 的"先拒绝再转换",两栈同为"不匹配"。
_UINT32_MAX = (1 << 32) - 1

# `bind_owner_for_published_hub_assignment` 的 owner Begin 预算(秒)。
# Go: `3*time.Second`。签票是玩家在场路径上的同步调用,预算耗尽即 fail-closed 拒票
# (§9.23:拒一次不会卡死玩家,客户端按退避重查即可;而放行会造成双 owner)。
OWNER_BIND_BUDGET_SEC = 3.0

# 「为什么没 hub」诊断日志里逐分片明细的采样上限。Go: shardCensusSampleLimit。
# 分片数被 Fleet max_replicas 有界(几~几十),但日志行长度不该随之无界增长;
# 超出部分只体现在计数器里(counts 恒完整,样本才截断)。
SHARD_CENSUS_SAMPLE_LIMIT = 12


# ── 弱依赖协议 ───────────────────────────────────────────────────────────────


@runtime_checkable
class LocalHubCredentialSource(Protocol):
    """mode=local 的 fleet provider 给出本机 Hub DS 的完整凭据身份。

    对应 Go 的 `localHubCredentialSource` 接口断言。`runtime_checkable` 的
    `isinstance` 只查方法存在性 —— 与 Go 的接口断言语义一致:
    Agones / Mock provider 刻意不实现,断言恒 False,线上路径零变更。
    """

    def local_credential_ack(self, pod: str) -> Any: ...


@runtime_checkable
class HubInstanceTeardownProofRepo(Protocol):
    """记录 exact GameServer UID 拆机证明。对应 Go 的 `data.HubInstanceTeardownProofRepo`。"""

    async def record_instance_teardown_proof(
        self, pod: str, instance_uid: str, proof_ttl_sec: float
    ) -> None: ...


# ── release track ───────────────────────────────────────────────────────────


def sticky_release_track(track: str) -> str:
    """**已持久化记录**的轨道读取规则(抛错版)。对应 Go 的 `stickyReleaseTrack`。

    逻辑复用 `fleet.sticky_release_track`(全仓唯一实现);这里只做**错误码对齐**:
    fleet 抛 `ValueError`,而 Go 抛 `errcode.ErrInvalidState`,这个码会一路传到
    RPC 边界并决定客户端是"重试"还是"当参数错"。丢掉码值 = 客户端收到
    `UNKNOWN`,§9.23 要求的"每次等待有明确原因"当场打穿。
    """
    try:
        return F.sticky_release_track(track)
    except ValueError as exc:
        raise errcode.PandoraError(
            errcode.ErrInvalidState, 'invalid persisted hub release_track "%s"', track
        ) from exc


def sticky_release_track_or_none(track: str) -> str | None:
    """不抛版。对应 Go 那些只判 `trackErr == nil` 的调用点。

    ★ 为什么必须单独有这个:`least_loaded` / census / `assignment_same_instance`
      在 Go 里遇到非法轨道是**跳过这一条候选**,不是"整轮失败"。用异常做控制流
      会把"库里有一条脏分片"升级成"这次分配彻底失败" —— 一条脏数据就能让整个
      region 不可分配。
    """
    try:
        return F.sticky_release_track(track)
    except ValueError:
        return None


# ── 纯函数:身份 / 判定 ──────────────────────────────────────────────────────


def effective_role_id(requested: int, stored: int) -> int:
    """选角生效值。对应 Go 的 `effectiveRoleID`。

    调用方显式传的 `requested`(>0)优先 —— login 是角色数据权威;否则回退归属镜像
    已存值,让 Transfer / 重签路径不丢角色(那些路径 login 不在环上,没有第二个来源)。
    """
    if requested > 0:
        return requested
    return stored


def assignment_binding_v2_complete(a: Any, player_id: int) -> bool:
    """归属是否是一份**完整的 writer-v2 绑定**。对应 Go 的 `assignmentBindingV2Complete`。

    Model B 数据面永久只接受 `writer_epoch == 2` 的完整 tuple。legacy / 缺字段 /
    future writer 的一次性迁移必须由 activation 控制面在开放业务流量**之前**完成 ——
    在请求路径里静默"升级"等于让一条半截绑定自己给自己发通行证。
    """
    if a is None:
        return False
    if sticky_release_track_or_none(a.release_track) is None:
        return False
    return (
        player_id != 0
        and a.player_id == player_id
        and a.hub_pod_name != ""
        and a.hub_instance_uid != ""
        and a.auth_epoch != 0
        and a.auth_gen != 0
        and a.auth_jti != ""
        and a.assignment_id != ""
        and a.auth_writer_epoch == DS_AUTH_WRITER_EPOCH_V2
    )


def assignment_same_instance(a: Any, current: L.ReserveResult | None) -> bool:
    """区分"同名 Pod 的凭据轮换"与"同名 GameServer 重建"。对应 Go 的 `assignmentSameInstance`。

    只有 UID + protocol epoch 仍**完全相同**且当前权威可路由时,旧 assignment 的座位
    才可原地重绑;UID / epoch 任一变化必须走新占座,旧 token / assignment 永不复用 ——
    否则一台被重建的 GameServer 会"继承"上一条实例的全部在场玩家(§9.22 脑臂)。
    """
    if a is None or current is None:
        return False
    a_track = sticky_release_track_or_none(a.release_track)
    current_track = sticky_release_track_or_none(current.release_track)
    return (
        current.ok
        and a.hub_pod_name != ""
        and a_track is not None
        and current_track is not None
        and a_track == current_track
        and a.hub_instance_uid != ""
        and a.hub_instance_uid == current.instance_uid
        and a.auth_epoch != 0
        and a.auth_epoch == current.protocol_epoch
        and a.auth_gen != 0
        and a.auth_jti != ""
        and a.auth_writer_epoch == DS_AUTH_WRITER_EPOCH_V2
        and current.writer_epoch == DS_AUTH_WRITER_EPOCH_V2
    )


def authoritative_shard(shard: Any, seat: L.ReserveResult | None) -> Any:
    """把 {pod} 事务里读到的**权威**路由快照盖回分片镜像克隆。对应 Go 的 `authoritativeShard`。

    ★ 必须是克隆:分片镜像来自 `list_shards` 的共享列表,原地改会污染同一轮里其它
      候选的判定(Python 的 proto message 是引用语义,`a = b` 之后改 a 就是改 b)。
    """
    out = R.clone(shard)
    if seat is not None:
        out.hub_addr = seat.hub_addr
        out.region = seat.region
        out.shard_id = seat.shard_id
        out.player_count = seat.player_count
        out.capacity = seat.capacity
        out.release_track = seat.release_track
    return out


def bind_assignment_auth(a: Any, seat: L.ReserveResult | None) -> None:
    """把 Model B 占座时确认的 active 元组钉进归属记录。对应 Go 的 `bindAssignmentAuth`。

    `seat is None`(legacy / off,`reserve_seat` 不返回座位)时**不动** —— 在 legacy
    面伪造一份绑定会让 `assignment_binding_v2_complete` 通过,而那份身份指向虚无。
    """
    if seat is None:
        return
    a.hub_instance_uid = seat.instance_uid
    a.auth_epoch = seat.protocol_epoch
    a.auth_gen = seat.active_gen
    a.auth_jti = seat.active_jti
    a.auth_writer_epoch = seat.writer_epoch


def assignment_instance_identity(a: Any) -> L.AssignmentInstanceIdentity:
    """赢得 assignment CAS 后退座所需的 exact 实例身份。对应 Go 的 `assignmentInstanceIdentity`。"""
    if a is None:
        return L.AssignmentInstanceIdentity()
    return L.AssignmentInstanceIdentity(
        player_id=a.player_id,
        assignment_id=a.assignment_id,
        instance_uid=a.hub_instance_uid,
        protocol_epoch=a.auth_epoch,
        writer_epoch=a.auth_writer_epoch,
    )


# ── 纯函数:transfer cleanup saga 的字段搬运 ────────────────────────────────


def transfer_cleanup_ref(a: Any) -> R.TransferCleanupRef:
    """一条待清理的旧 owner 引用(index-first ref)。对应 Go 的 `transferCleanupRef`。"""
    if a is None:
        return R.TransferCleanupRef(player_id=0, target_assignment_id="")
    return R.TransferCleanupRef(player_id=a.player_id, target_assignment_id=a.assignment_id)


def bind_transfer_cleanup_source(target: Any, source: Any) -> None:
    """把 source 的 exact 身份钉进 target 的 cleanup 阶段字段。对应 Go 的 `bindTransferCleanupSource`。

    十条前置判据一条都不能少 —— 每一条对应一种"清不掉的残留":缺 pod / uid / epoch
    就没法 exact 匹配旧座位;`target.assignment_id == source.assignment_id` 说明调用方
    把自己当成了源,清理会把**刚提交的新 owner** 的座位退掉。
    """
    if (
        target is None
        or source is None
        or target.player_id == 0
        or target.player_id != source.player_id
        or target.assignment_id == ""
        or source.assignment_id == ""
        or target.assignment_id == source.assignment_id
        or source.hub_pod_name == ""
        or source.hub_instance_uid == ""
        or source.auth_epoch == 0
        or source.auth_writer_epoch != DS_AUTH_WRITER_EPOCH_V2
    ):
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "complete exact Hub transfer source owner required"
        )
    target.transfer_cleanup_pending = True
    target.transfer_target_bound = False
    target.transfer_source_hub_pod_name = source.hub_pod_name
    target.transfer_source_assignment_id = source.assignment_id
    target.transfer_source_instance_uid = source.hub_instance_uid
    target.transfer_source_auth_epoch = source.auth_epoch
    target.transfer_source_auth_writer_epoch = source.auth_writer_epoch
    target.release_cleanup_pending = False


def transfer_cleanup_source(a: Any) -> Any:
    """从 cleanup 阶段字段重建 source owner 记录。对应 Go 的 `transferCleanupSource`。

    ★ 重建出来的是一份**只含 exact 身份**的记录(五个字段),不是 source 的完整快照:
      释放旧座位只需要 exact 身份,多带一个字段就多一处"用错了但看起来对"的机会。
    """
    if (
        a is None
        or not a.transfer_cleanup_pending
        or a.release_cleanup_pending
        or a.player_id == 0
        or a.assignment_id == ""
        or a.transfer_source_hub_pod_name == ""
        or a.transfer_source_assignment_id == ""
        or a.transfer_source_assignment_id == a.assignment_id
        or a.transfer_source_instance_uid == ""
        or a.transfer_source_auth_epoch == 0
        or a.transfer_source_auth_writer_epoch != DS_AUTH_WRITER_EPOCH_V2
    ):
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "Hub transfer cleanup source identity invalid"
        )
    return hubpb.HubAssignmentStorageRecord(
        player_id=a.player_id,
        hub_pod_name=a.transfer_source_hub_pod_name,
        assignment_id=a.transfer_source_assignment_id,
        hub_instance_uid=a.transfer_source_instance_uid,
        auth_epoch=a.transfer_source_auth_epoch,
        auth_writer_epoch=a.transfer_source_auth_writer_epoch,
    )


def clear_transfer_cleanup(a: Any) -> None:
    """清空 transfer cleanup 阶段字段。对应 Go 的 `clearTransferCleanup`。

    ★ 七个字段必须**全清**。留一个非零就会被 `resume_assignment_cleanup` 的
      "orphan cleanup fields" 判据抓成 `ErrInvalidState`,该玩家从此每次进场都失败。
    """
    if a is None:
        return
    a.transfer_cleanup_pending = False
    a.transfer_target_bound = False
    a.transfer_source_hub_pod_name = ""
    a.transfer_source_assignment_id = ""
    a.transfer_source_instance_uid = ""
    a.transfer_source_auth_epoch = 0
    a.transfer_source_auth_writer_epoch = 0


# ── 纯函数:票据 / owner 目标 ───────────────────────────────────────────────


def ticket_binding_from_assignment(a: Any) -> HubTicketBinding:
    """从归属记录抽出 hub 票据的七元组绑定。对应 Go 的 `ticketBindingFromAssignment`。

    任一格缺失 → 返回**零值**绑定(而不是半截绑定):半截绑定签出的票在 UE Hub DS
    PostLogin 会 fail-closed 踢人,而客户端只看到"连上又被踢",排查方向完全错。
    """
    if a is None:
        return HubTicketBinding()
    release_track = sticky_release_track_or_none(a.release_track)
    if (
        release_track is None
        or a.hub_pod_name == ""
        or a.hub_instance_uid == ""
        or a.auth_epoch == 0
        or a.auth_gen == 0
        or a.auth_jti == ""
        or a.assignment_id == ""
        or a.auth_writer_epoch != DS_AUTH_WRITER_EPOCH_V2
    ):
        return HubTicketBinding()
    return HubTicketBinding(
        pod_name=a.hub_pod_name,
        instance_uid=a.hub_instance_uid,
        protocol_epoch=a.auth_epoch,
        credential_gen=a.auth_gen,
        credential_jti=a.auth_jti,
        hub_assignment_id=a.assignment_id,
        writer_epoch=a.auth_writer_epoch,
        release_track=release_track,
    )


def hub_assignment_delivery_equal(a: Any, b: Any, player_id: int) -> bool:
    """比较一次票据交付所依赖的完整 assignment 快照。对应 Go 的 `hubAssignmentDeliveryEqual`。

    刻意**忽略** cleanup phase / `assigned_at_ms` 等 saga 元数据 —— 那些字段会在
    cleanup 推进时合法变化,把它们算进来会让"旧 owner 正在被清理"误判成"归属已被
    别人抢走",于是一次正常的 Transfer 永远交付不出票。
    反过来,任何会改变**返回路由 / 角色 / 票据 binding** 的字段都必须相等。
    """
    if a is None or b is None or a.player_id != player_id or b.player_id != player_id:
        return False
    a_track = sticky_release_track_or_none(a.release_track)
    b_track = sticky_release_track_or_none(b.release_track)
    return (
        a_track is not None
        and b_track is not None
        and a_track == b_track
        and a.assignment_id == b.assignment_id
        and a.hub_pod_name == b.hub_pod_name
        and a.hub_addr == b.hub_addr
        and a.shard_id == b.shard_id
        and a.region == b.region
        and a.team_id == b.team_id
        and a.role_id == b.role_id
        and a.hub_instance_uid == b.hub_instance_uid
        and a.auth_epoch == b.auth_epoch
        and a.auth_gen == b.auth_gen
        and a.auth_jti == b.auth_jti
        and a.auth_writer_epoch == b.auth_writer_epoch
    )


def owner_target_view_equal(a: OwnerTargetView, b: OwnerTargetView) -> bool:
    """对应 Go 的 `ownerTargetViewEqual`(Go 是可比较结构体的 `==`)。

    `OwnerTargetView` 是 `frozen=True, slots=True` 的 dataclass,`==` 即**逐字段**
    比较,与 Go 的结构体相等语义一致(含 `source_revision`)。
    """
    return a == b


# ── 纯函数:选分片 ──────────────────────────────────────────────────────────


def least_loaded(
    shards: list, region: str, release_track: str, exclude_pod: str
) -> Any | None:
    """region + 轨内最空的 ready 且未满分片;并列取 `shard_id` 小者。对应 Go 的 `leastLoaded`。

    ★ 并列的 tie-break 必须是 `shard_id` 而不是遍历顺序:`list_shards` 的顺序来自
      Redis SET,两次调用可能不同,不定序会让同一批队友被打散到不同分片。
    """
    best = None
    for s in shards:
        track = sticky_release_track_or_none(s.release_track)
        if (
            track is None
            or track != release_track
            or s.region != region
            or s.state != STATE_READY
            or s.player_count >= s.capacity
        ):
            continue
        if exclude_pod != "" and s.hub_pod_name == exclude_pod:
            continue
        if (
            best is None
            or s.player_count < best.player_count
            or (s.player_count == best.player_count and s.shard_id < best.shard_id)
        ):
            best = s
    return best


def select_transfer_target(shards: list, cur: Any, target_hub_id: int) -> Any | None:
    """切线目标分片。对应 Go 的 `selectTransferTarget`。

    `target_hub_id != 0` 点名 `shard_id` 匹配的分片;否则同 region 最空的「非当前」
    ready 分片。**必须同轨**(§9.21:玩家轨道粘性,一次切线不该把人换轨)。

    ★ `target_hub_id > 2^32-1` 直接返回 None:Go 那侧 `uint32(targetHubID)` 会**截断**,
      于是 `0x1_0000_0001` 会误匹配到 shard_id=1 的分片。Python 的 int 不截断,但若
      不显式拒绝,两栈对同一个越界入参会给出不同结论(Go 误匹配 / Python 恒不匹配)。
      照抄 Go 的"先拒绝再转换",两栈同为"不匹配"。
    """
    cur_track = sticky_release_track_or_none(cur.release_track)
    if cur_track is None:
        return None
    if target_hub_id != 0:
        if target_hub_id > _UINT32_MAX:
            return None
        want = target_hub_id
        for s in shards:
            track = sticky_release_track_or_none(s.release_track)
            if (
                track is not None
                and track == cur_track
                and s.shard_id == want
                and s.region == cur.region
                and s.state == STATE_READY
            ):
                # 目标就是当前分片时不要求"未满":那是幂等重签,不占新座位。
                if s.hub_pod_name == cur.hub_pod_name or s.player_count < s.capacity:
                    return s
        return None
    return least_loaded(shards, cur.region, cur_track, cur.hub_pod_name)


@dataclasses.dataclass(slots=True)
class ShardExclusionCensus:
    """「本次选分片时,每个分片各自因为什么被排除」。对应 Go 的 `shardExclusionCensus`。

    存在理由:`ErrHubNoAvailable` 在中间件里不算服务端故障,access log 只落 DEBUG,
    线上默认 info 级下**一条都不出**;即使打了日志,只写一句「没有可用 hub」也无法
    区分「真的满了」「全在 warming 没心跳」「release track 筛错了」「region 写错了」——
    这四种的处置方案完全不同(等 / 查 DS 心跳 / 调 canary 权重 / 查 login 传参)。
    """

    total: int = 0
    track_invalid: int = 0  # 持久化 release_track 非法(fail-closed 排除)
    track_mismatch: int = 0  # release_track 不是本次要的轨(canary/stable 筛选)
    region_mismatch: int = 0
    excluded_pod: int = 0  # 显式排除(Transfer 时排除当前分片)
    not_ready: int = 0  # state != ready(warming/draining/stopping)
    full: int = 0  # player_count >= capacity
    warming: int = 0  # not_ready 里 state==warming 的细分(= 从未收到过鉴权心跳)
    draining: int = 0
    stopping: int = 0
    candidates: int = 0
    # 通过静态筛选、但被 {pod} 原子授权+占座门拒的候选数
    # (授权未激活 / 心跳陈旧 / 实例元组不符 / 并发占满)。
    reserve_rejected: int = 0
    sample: list[str] = dataclasses.field(default_factory=list)

    def observe(self, s: Any, verdict: str) -> None:
        """记一条逐分片明细样本。格式与 Go 的 `fmt.Sprintf` **逐字符一致**。"""
        if len(self.sample) < SHARD_CENSUS_SAMPLE_LIMIT:
            self.sample.append(
                f"{s.hub_pod_name}|{s.state}|{s.release_track}|shard={s.shard_id}"
                f"|{s.player_count}/{s.capacity}|{verdict}"
            )

    def fields(self) -> dict[str, Any]:
        """展开成日志 kv。**键名逐字节照抄 Go**(Loki 查询跨栈共用)。"""
        return {
            "shards_total": self.total,
            "candidates": self.candidates,
            "excl_track_invalid": self.track_invalid,
            "excl_track_mismatch": self.track_mismatch,
            "excl_region_mismatch": self.region_mismatch,
            "excl_pod": self.excluded_pod,
            "excl_not_ready": self.not_ready,
            "excl_warming": self.warming,
            "excl_draining": self.draining,
            "excl_stopping": self.stopping,
            "excl_full": self.full,
            "reserve_rejected": self.reserve_rejected,
            "shard_census": self.sample,
        }


def no_routable_shard_reason(c: ShardExclusionCensus) -> str:
    """把「一个 `ErrHubNoAvailable` 收敛 N 种成因」拆回唯一枚举 reason。

    对应 Go 的 `noRoutableShardReason`。**判定顺序 = 排障时的处置优先级**,不能重排:
    先分清「压根没分片」→「有候选但被原子门拒」→「分片都不在这个 region/轨」→
    「分片在但没通过心跳」→「真满了」。
    """
    if c.total == 0:
        return REASON_NO_SHARD_MIRROR
    if c.candidates > 0 and c.reserve_rejected > 0:
        return REASON_ALL_CANDIDATES_RESERVE_REJECTED
    if c.candidates > 0:
        return REASON_CANDIDATES_VANISHED
    if c.full > 0 and c.full >= c.not_ready:
        return REASON_ALL_SHARDS_FULL
    if c.warming > 0:
        return REASON_ALL_SHARDS_WARMING
    if c.draining > 0 or c.stopping > 0:
        return REASON_ALL_SHARDS_DRAINING
    if c.region_mismatch > 0 and c.track_mismatch == 0:
        return REASON_NO_SHARD_IN_REGION
    if c.track_mismatch > 0:
        return REASON_NO_SHARD_IN_RELEASE_TRACK
    if c.track_invalid > 0:
        return REASON_ALL_SHARDS_TRACK_INVALID
    return REASON_NO_SHARD_CANDIDATE


@dataclasses.dataclass(slots=True)
class PreparedHubTicket:
    """`prepare_hub_ticket` 的出参。对应 Go 的 `preparedHubTicket`。"""

    token: str = ""
    expires_at_ms: int = 0
    owner_target: OwnerTargetView = OwnerTargetView()
    target_ok: bool = False


# ── ShardMixin ──────────────────────────────────────────────────────────────


class ShardMixin:
    """`HubUsecase` 的分片 / 座位 / 归属 saga / 签票辅助。

    ★ 刻意**不**加 `__slots__`(与 `HubUsecaseBase` 同因):多 mixin 叠加时 slots
      布局要在每层重复声明,漏一层就静默退回 `__dict__`,制造"以为有约束其实没有"。
    """

    # 下面这些属性/方法由 HubUsecaseBase 与其它 mixin 提供,这里只声明给读者看:
    #   repo / fleet / auth_repo / signer / owner_auth / cfg
    #   retry() / shard_ttl_sec() / assign_ttl_sec() / reservation_ttl_sec()
    #   assignment_saga_ttl_sec() / heartbeat_max_age_ms()
    #   initial_shard_state() / candidate_token_exp() / candidate_token_gen()
    #   require_heartbeat_ready / mint_source_revision() / confirm_writer_for_ticket()
    #   add_shard_member() / remove_shard_member()   ← 由另一批提供

    # ── 分片拓扑 ─────────────────────────────────────────────────────────

    async def ensure_shards(self, region: str, release_track: str) -> None:
        """region 无候选分片时,按 Fleet 拓扑种入 Redis(lazy-seed)。对应 Go 的 `ensureShards`。

        ★ 热路径只在该 region **首次**无分片时打 Fleet;已有分片直接返回,不打 k8s。
          拓扑漂移(pod 改名 / 下线)的对账交后台 `reconcile_shard_topology` —— 每次
          登录都查 apiserver 会让 AssignHub 的延迟随集群规模劣化,而漂移本身不紧急。
        """
        if not releasetrack.valid(release_track):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, 'invalid hub release_track "%s"', release_track
            )
        shards = await self.repo.list_shards()
        for s in shards:
            track = sticky_release_track_or_none(s.release_track)
            if track is not None and s.region == region and track == release_track:
                return  # 已有该 region + track 分片
        cands = await self.fleet.list_shards(region)
        now = L.now_ms()
        for c in cands:
            if not c.token_ready or not releasetrack.valid(c.release_track):
                # enforce 下令牌不可用的分片:不种 ready 镜像,否则 AssignHub 会把玩家
                # 分到一台「回调被全拒」的 Hub —— 玩家连得上、心跳全 401、进不去。
                continue
            rec = hubpb.HubShardStorageRecord(
                hub_pod_name=c.pod_name,
                hub_addr=c.addr,
                region=c.region,
                shard_id=c.shard_id,
                player_count=0,
                capacity=c.capacity,
                state=self.initial_shard_state(),
                # 种子:从未心跳(扫描排除;require_heartbeat_ready 时为 warming 不可分配)
                last_heartbeat_ms=0,
                created_at_ms=now,
                current_token_exp_ms=self.candidate_token_exp(c.token_exp_ms),
                current_token_gen=self.candidate_token_gen(c.token_gen),
                release_track=c.release_track,
                # exact 实例身份:仅 mode=local 非空。agones 留空 —— 其身份由 Model B
                # promote 后投影,不能由拓扑发现抢先写(抢先写 = 拓扑成了身份权威)。
                gameserver_uid=c.instance_uid,
                auth_epoch=c.protocol_epoch,
            )
            await self.repo.create_shard(rec, self.shard_ttl_sec())

    async def reconcile_shard_topology(self) -> None:  # noqa: C901,PLR0912,PLR0915
        """后台按 Fleet 拓扑对账 Redis 分片镜像。对应 Go 的 `reconcileShardTopology`。

        解决的问题:minikube / Agones 重启后 pod 名 / 端口变化,旧分片在 Redis 里成为
        孤儿 —— 心跳超时只会把它标 draining(无 `draining_since_ms`),回收逻辑跳过、
        sweep 又每 tick 续 TTL,于是**永久残留**并让重登玩家拿到过期 `hub_ds_addr`。

        ★ Fleet 暂不可用或某 region 候选为空时**保留现有镜像**(绝不误删):
          「Fleet 列举里没有」永远不等于「进程已死」(§9.22)。真正的拆机证明只能来自
          exact GameServer UID 观测,由下面那段可选的 observer 铸。
        """
        shards = await self.repo.list_shards()
        # 需对账的 region:已存在分片的 region + 默认 region(便于发现首个分片)。
        regions = {self.cfg.default_region}
        for s in shards:
            if s.region != "":
                regions.add(s.region)
        now = L.now_ms()
        for region in sorted(regions):
            try:
                cands = await self.fleet.list_shards(region)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 降级:Fleet 不可用时保留现有镜像
                plog.get().warning(
                    "reconcile_topology_list_failed", region=region, err=str(exc)
                )
                continue
            # 空的 routable 列表**不是**物理拆机。下面仍会走一遍现有镜像把它们标成
            # 不可路由,并要求 exact GameServer+Pod 观测才铸拆机证明。
            live: set[str] = set()
            present_candidate: set[str] = set()
            for c in cands:
                present_candidate.add(c.pod_name)
                if not c.token_ready or not releasetrack.valid(c.release_track):
                    # 凭据 / 发布元数据失败只取消**路由资格**,对进程只字未提。
                    # 保留全部归属账本,恢复后必须重新过一次鉴权心跳才放行。
                    def _to_warming(s: Any) -> None:
                        if s.state != STATE_STOPPING and not (
                            s.state == STATE_DRAINING and s.draining_since_ms > 0
                        ):
                            s.state = STATE_WARMING

                    try:
                        await self.repo.update_shard_with_lock(
                            c.pod_name, self.retry(), _to_warming, self.shard_ttl_sec()
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException:  # noqa: BLE001 —— 对齐 Go 的 `_ =`(整条忽略)
                        pass
                    continue
                live.add(c.pod_name)
                try:
                    existing = await self.repo.get_shard(c.pod_name)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    plog.get().warning(
                        "reconcile_topology_get_failed", pod=c.pod_name, err=str(exc)
                    )
                    continue
                if existing is not None:
                    # 已有镜像:刷新地址 / 容量(pod 复用旧名但换端口 / 扩缩容时同步)。
                    def _refresh(s: Any, c: Any = c) -> None:
                        if s.release_track == "":
                            # additive rollout:旧镜像只迁为实际 metadata 轨。
                            s.release_track = c.release_track
                        elif s.release_track != c.release_track:
                            s.state = STATE_DRAINING
                            return  # metadata 轨漂移:保留原轨证据并退出可分配集
                        s.hub_addr = c.addr
                        s.region = c.region
                        s.shard_id = c.shard_id
                        s.capacity = c.capacity
                        # 滚动升级投毒防护:旧镜像 allocator(require_heartbeat_ready=False)
                        # 可能把分片建成 ready + last_heartbeat_ms=0。新镜像开启心跳门控后,
                        # 该分片**从未发过(鉴权)心跳却是 ready**,会被 AssignHub 直接选中。
                        # 这里降级回 warming,等首个通过 Guard 的心跳再放行分配。
                        if (
                            self.require_heartbeat_ready
                            and s.last_heartbeat_ms == 0
                            and s.state != STATE_DRAINING
                            and s.state != STATE_STOPPING
                        ):
                            s.state = STATE_WARMING
                        # 拓扑 / 心跳丢失导致的 draining 没有整合时间戳。可路由候选只能
                        # 通过**一次新的心跳**再进来;有时间戳的主动缩容排空不可逆。
                        if s.state == STATE_DRAINING and s.draining_since_ms == 0:
                            if self.require_heartbeat_ready:
                                s.state = STATE_WARMING
                            else:
                                s.state = STATE_READY
                        # 令牌代际**只增不减**,绝不被 0 / 低代际清除或回退。
                        # permissive 副本 / annotation 缺失 → 候选 gen=0 → 保持镜像既有
                        # 代际不变(旧方案的「off/permissive 清 0 自愈」是 fail-open 向量)。
                        # 只有严格更高的候选代际才推进,并复位 warming 等新代际鉴权心跳 ——
                        # 挡住旧令牌迟到心跳把轮换后的分片重新置 ready。
                        gen = self.candidate_token_gen(c.token_gen)
                        if gen > s.current_token_gen:
                            s.current_token_gen = gen
                            s.current_token_exp_ms = self.candidate_token_exp(c.token_exp_ms)
                            if (
                                self.require_heartbeat_ready
                                and s.state != STATE_DRAINING
                                and s.state != STATE_STOPPING
                            ):
                                s.state = STATE_WARMING

                    try:
                        await self.repo.update_shard_with_lock(
                            c.pod_name, self.retry(), _refresh, self.shard_ttl_sec()
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:  # noqa: BLE001
                        if errcode.as_code(exc) != errcode.ErrHubNoAvailable:
                            plog.get().warning(
                                "reconcile_topology_update_failed",
                                pod=c.pod_name,
                                err=str(exc),
                            )
                    continue
                # 新 pod:补齐镜像。
                rec = hubpb.HubShardStorageRecord(
                    hub_pod_name=c.pod_name,
                    hub_addr=c.addr,
                    region=c.region,
                    shard_id=c.shard_id,
                    player_count=0,
                    capacity=c.capacity,
                    state=self.initial_shard_state(),
                    last_heartbeat_ms=0,
                    created_at_ms=now,
                    current_token_exp_ms=self.candidate_token_exp(c.token_exp_ms),
                    current_token_gen=self.candidate_token_gen(c.token_gen),
                    release_track=c.release_track,
                    gameserver_uid=c.instance_uid,
                    auth_epoch=c.protocol_epoch,
                )
                try:
                    await self.repo.create_shard(rec, self.shard_ttl_sec())
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    plog.get().warning(
                        "reconcile_topology_create_failed", pod=c.pod_name, err=str(exc)
                    )
            # 不在 routable 集合里的分片保留为**物理 owner 栅栏**。候选缺席包含
            # Scheduled / Unhealthy / 令牌失败,绝不能据此抹掉会话。只有可选的 exact
            # 观测者能记录 UID 专属拆机证明;归属清理一次消费一个 owner。
            for s in shards:
                if s.region != region:
                    continue
                if s.hub_pod_name in live:
                    continue
                if s.hub_pod_name not in present_candidate:

                    def _fence_stale(current: Any) -> None:
                        if current.state != STATE_STOPPING and not (
                            current.state == STATE_DRAINING and current.draining_since_ms > 0
                        ):
                            current.state = STATE_DRAINING

                    try:
                        await self.repo.update_shard_with_lock(
                            s.hub_pod_name, self.retry(), _fence_stale, self.shard_ttl_sec()
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:  # noqa: BLE001
                        if errcode.as_code(exc) != errcode.ErrHubNoAvailable:
                            plog.get().warning(
                                "reconcile_topology_fence_stale_failed",
                                pod=s.hub_pod_name,
                                region=region,
                                err=str(exc),
                            )
                observer_ok = isinstance(self.fleet, F.HubFleetPhysicalObserver)
                proof_ok = isinstance(self.auth_repo, HubInstanceTeardownProofRepo)
                if observer_ok and proof_ok and s.gameserver_uid != "":
                    try:
                        observation = await self.fleet.observe_shard_instance(s.hub_pod_name)
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:  # noqa: BLE001
                        plog.get().warning(
                            "reconcile_topology_physical_observation_failed",
                            pod=s.hub_pod_name,
                            expected_uid=s.gameserver_uid,
                            err=str(exc),
                        )
                        continue
                    if not observation.proves_teardown(s.gameserver_uid):
                        continue
                    try:
                        await self.auth_repo.record_instance_teardown_proof(
                            s.hub_pod_name, s.gameserver_uid, self.assignment_saga_ttl_sec()
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:  # noqa: BLE001
                        plog.get().warning(
                            "reconcile_topology_record_teardown_failed",
                            pod=s.hub_pod_name,
                            expected_uid=s.gameserver_uid,
                            err=str(exc),
                        )
                        continue
                    plog.get().warning(
                        "reconcile_topology_exact_uid_teardown_confirmed",
                        pod=s.hub_pod_name,
                        expected_uid=s.gameserver_uid,
                        observed_uid=observation.game_server_uid,
                    )

    # ── 选分片 / 占座 ────────────────────────────────────────────────────

    async def select_shard(self, region: str, team_id: int) -> Any:
        """队友所在分片优先,否则同 region 最空 ready 分片。对应 Go 的 `selectShard`。

        ★ 队伍提示查询失败**静默降级**为"没有提示"(Go: `gerr == nil && ok`)。
          队伍同分片只是体验优化,让它硬阻断分配等于用一个软提示把玩家挡在门外。
        """
        shards = await self.repo.list_shards()
        if team_id != 0:
            pod: str | None = None
            try:
                pod = await self.repo.get_team_shard(team_id)
            except asyncio.CancelledError:
                raise
            except BaseException:  # noqa: BLE001 —— 对齐 Go 的 `gerr == nil` 静默降级
                pod = None
            if pod is not None:
                for s in shards:
                    if (
                        s.hub_pod_name == pod
                        and s.region == region
                        and s.state == STATE_READY
                        and s.player_count < s.capacity
                    ):
                        return s
        best = least_loaded(shards, region, releasetrack.STABLE, "")
        if best is None:
            raise errcode.PandoraError(
                errcode.ErrHubNoAvailable,
                "no ready hub shard with capacity in region %s",
                region,
            )
        return best

    async def reserve_seat(self, pod: str) -> None:
        """乐观锁占一个座位(复核 ready + 容量,`player_count++`)。对应 Go 的 `reserveSeat`。

        legacy(未装配 `auth_repo`)专用。Model B 走 `reserve_routable_seat`。
        """

        def _take(s: Any) -> None:
            if s.state != STATE_READY:
                raise errcode.PandoraError(
                    errcode.ErrHubNoAvailable, "hub shard %s not ready", pod
                )
            if s.player_count >= s.capacity:
                raise errcode.PandoraError(errcode.ErrHubNoAvailable, "hub shard %s full", pod)
            s.player_count += 1

        await self.repo.update_shard_with_lock(pod, self.retry(), _take, self.shard_ttl_sec())

    async def release_from_shard(self, pod: str) -> None:
        """退一个座位(floor 0)。对应 Go 的 `releaseFromShard`。

        分片不存在 / 锁冲突静默(幂等退位):补偿路径必须能重复跑,把"分片已经没了"
        当失败会让调用方误以为座位还占着而反复重试。
        """
        try:
            def _give_back(s: Any) -> None:
                if s.player_count > 0:
                    s.player_count -= 1

            await self.repo.update_shard_with_lock(
                pod, self.retry(), _give_back, self.shard_ttl_sec()
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            if errcode.as_code(exc) != errcode.ErrHubNoAvailable:
                plog.get().warning("release_from_shard_failed", pod=pod, err=str(exc))

    async def reserve_routable_seat(
        self, pod: str, player_id: int, assignment_id: str
    ) -> L.ReserveResult | None:
        """原子占一个座位。对应 Go 的 `reserveRoutableSeat`。

        Model B(装配了 `auth_repo`)走 `reserve_assignment` **单事务**授权 + 路由 +
        占座门,返回本次绑定的 active 元组供钉进归属;legacy 走单纯 `reserve_seat`
        并返回 `None`。

        不可路由(授权未激活 / 分片非 ready / 元组不符 / 心跳陈旧 / 已满)
        → `ErrHubNoAvailable`(fail-closed)。
        """
        if self.auth_repo is None:
            await self.reserve_seat(pod)  # legacy:纯容量占座
            return None
        now_ms = L.now_ms()
        current = await self.auth_repo.check_routable(pod, now_ms, self.heartbeat_max_age_ms())
        if not current.ok:
            plog.get().warning("hub_reserve_not_routable", pod=pod, reason=current.reason)
            raise errcode.PandoraError(
                errcode.ErrHubNoAvailable,
                "hub shard %s not routable: %s",
                pod,
                current.reason,
            )
        res = await self.auth_repo.reserve_assignment(
            pod,
            L.ReservationIdentity(
                player_id=player_id,
                assignment_id=assignment_id,
                instance_uid=current.instance_uid,
                protocol_epoch=current.protocol_epoch,
                writer_epoch=current.writer_epoch,
                expires_at_ms=now_ms + int(self.reservation_ttl_sec() * 1000),
                assignment_expires_at_ms=now_ms + int(self.assign_ttl_sec() * 1000),
            ),
            now_ms,
            self.heartbeat_max_age_ms(),
            self.shard_ttl_sec(),
        )
        if not res.ok:
            plog.get().warning("hub_reserve_not_routable", pod=pod, reason=res.reason)
            raise errcode.PandoraError(
                errcode.ErrHubNoAvailable, "hub shard %s not routable: %s", pod, res.reason
            )
        return res

    async def ensure_existing_assignment_seat(
        self, player_id: int, assignment: Any, current: L.ReserveResult | None
    ) -> L.ReserveResult | None:
        """旧 assignment 重签 / 同实例凭据重绑前的最终容量门。对应 Go 的 `ensureExistingAssignmentSeat`。

        assignment key 与 `{pod}` ledger **不同 slot**,故调用方仍需随后对完整旧
        assignment 做 CAS;本函数只在线性化的 `{pod}` 事务中保证二选一:

          - exact assignment 已是 connected session:幂等返回,不增加容量;
          - exact assignment 尚无 ledger owner:在仍可路由且未满时创建 / 刷新有界 reservation。

        ⚠️ 调用方在后续 assignment CAS 失败时**不得盲目退座**:返回的 seat 可能是另一条
        仍存活连接的原 session。CAS winner 会按旧 assignment 精确释放;极端反序中新建
        但未被 winner 观察到的 reservation 也只存活 `reservation_ttl`。
        """
        if self.auth_repo is None:
            return current
        if (
            assignment is None
            or current is None
            or not assignment_same_instance(assignment, current)
            or assignment.player_id != player_id
            or assignment.assignment_id == ""
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "hub existing assignment identity is not reusable"
            )
        now_ms = L.now_ms()
        res = await self.auth_repo.reserve_assignment(
            assignment.hub_pod_name,
            L.ReservationIdentity(
                player_id=player_id,
                assignment_id=assignment.assignment_id,
                instance_uid=current.instance_uid,
                protocol_epoch=current.protocol_epoch,
                writer_epoch=current.writer_epoch,
                expires_at_ms=now_ms + int(self.reservation_ttl_sec() * 1000),
                assignment_expires_at_ms=now_ms + int(self.assign_ttl_sec() * 1000),
            ),
            now_ms,
            self.heartbeat_max_age_ms(),
            self.shard_ttl_sec(),
        )
        if not res.ok:
            raise errcode.PandoraError(
                errcode.ErrHubNoAvailable,
                "hub shard %s cannot ensure existing assignment seat: %s",
                assignment.hub_pod_name,
                res.reason,
            )
        return res

    async def assignment_routable(
        self, player_id: int, a: Any
    ) -> tuple[L.ReserveResult, bool]:
        """归属目标的权威路由快照 + 「归属钉住的完整 active 身份仍为当前值」校验。

        对应 Go 的 `assignmentRoutable`。返回 `(快照, 是否可路由)`;非法持久化轨道
        与存储故障一律抛异常(那是"数据不自洽",不是"当前不可路由")。
        """
        assignment_track = sticky_release_track(a.release_track)
        if self.auth_repo is None:
            shard = await self.repo.get_shard(a.hub_pod_name)
            if shard is None or shard.state != STATE_READY:
                return L.ReserveResult(), False
            shard_track = sticky_release_track(shard.release_track)
            if shard_track != assignment_track:
                return L.ReserveResult(), False
            return (
                L.ReserveResult(
                    ok=True,
                    shard_id=shard.shard_id,
                    hub_addr=shard.hub_addr,
                    region=shard.region,
                    player_count=shard.player_count,
                    capacity=shard.capacity,
                    release_track=shard_track,
                ),
                True,
            )
        if not assignment_binding_v2_complete(a, player_id):
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "hub assignment is not a complete writer-v2 binding"
            )
        info = await self.auth_repo.check_routable(
            a.hub_pod_name, L.now_ms(), self.heartbeat_max_age_ms()
        )
        if not info.ok:
            return info, False
        info_track = sticky_release_track(info.release_track)
        if info_track != assignment_track:
            return info, False
        info.release_track = info_track
        if (
            a.hub_instance_uid != info.instance_uid
            or a.auth_epoch != info.protocol_epoch
            or a.auth_gen != info.active_gen
        ):
            return info, False
        if a.auth_jti != info.active_jti:
            return info, False
        if a.auth_writer_epoch != info.writer_epoch:
            return info, False
        return info, True

    async def select_and_reserve_shard(  # noqa: C901,PLR0912
        self,
        player_id: int,
        assignment_id: str,
        region: str,
        team_id: int,
        exclude_pod: str,
        release_track: str,
    ) -> tuple[Any, L.ReserveResult | None]:
        """队友优先、负载升序尝试所有候选;每个候选都必须通过最终原子授权+占座门。

        对应 Go 的 `selectAndReserveShard`。
        """
        if not releasetrack.valid(release_track):
            plog.get().warning(
                "hub_select_rejected",
                player_id=player_id,
                hub_assignment_id=assignment_id,
                region=region,
                release_track=release_track,
                reason=REASON_INVALID_RELEASE_TRACK,
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, 'invalid hub release_track "%s"', release_track
            )
        try:
            shards = await self.repo.list_shards()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "hub_select_rejected",
                player_id=player_id,
                hub_assignment_id=assignment_id,
                region=region,
                release_track=release_track,
                reason=REASON_LIST_SHARDS_FAILED,
                err=str(exc),
            )
            raise
        census = ShardExclusionCensus(total=len(shards))
        candidates: list[Any] = []
        for shard in shards:
            track = sticky_release_track_or_none(shard.release_track)
            if track is None:
                census.track_invalid += 1
                census.observe(shard, REASON_SHARD_TRACK_INVALID)
            elif track != release_track:
                census.track_mismatch += 1
                census.observe(shard, REASON_SHARD_TRACK_MISMATCH)
            elif shard.region != region:
                census.region_mismatch += 1
                census.observe(shard, REASON_SHARD_REGION_MISMATCH)
            elif shard.hub_pod_name == exclude_pod:
                census.excluded_pod += 1
                census.observe(shard, REASON_SHARD_EXCLUDED_POD)
            elif shard.state != STATE_READY:
                census.not_ready += 1
                if shard.state == STATE_WARMING:
                    census.warming += 1
                elif shard.state == STATE_DRAINING:
                    census.draining += 1
                elif shard.state == STATE_STOPPING:
                    census.stopping += 1
                census.observe(shard, REASON_SHARD_NOT_READY)
            elif shard.player_count >= shard.capacity:
                census.full += 1
                census.observe(shard, REASON_SHARD_FULL)
            else:
                candidates.append(shard)
                census.observe(shard, "candidate")
        census.candidates = len(candidates)
        # 与 Go 的 sort.Slice 比较器逐条同序:先按在册人数,再按 shard_id(稳定 tie-break)。
        candidates.sort(key=lambda s: (s.player_count, s.shard_id))
        if team_id != 0:
            try:
                pod = await self.repo.get_team_shard(team_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "hub_select_rejected",
                    player_id=player_id,
                    hub_assignment_id=assignment_id,
                    region=region,
                    team_id=team_id,
                    release_track=release_track,
                    reason=REASON_TEAM_SHARD_LOOKUP_FAILED,
                    err=str(exc),
                )
                raise
            if pod is not None:
                for i, candidate in enumerate(candidates):
                    if candidate.hub_pod_name == pod:
                        candidates[0], candidates[i] = candidates[i], candidates[0]
                        break
        for candidate in candidates:
            try:
                seat = await self.reserve_routable_seat(
                    candidate.hub_pod_name, player_id, assignment_id
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                if errcode.as_code(exc) == errcode.ErrHubNoAvailable:
                    census.reserve_rejected += 1
                    continue
                plog.get().warning(
                    "hub_select_rejected",
                    player_id=player_id,
                    hub_assignment_id=assignment_id,
                    region=region,
                    ds_pod=candidate.hub_pod_name,
                    release_track=release_track,
                    reason=REASON_RESERVE_SEAT_ERROR,
                    err=str(exc),
                )
                raise
            return authoritative_shard(candidate, seat), seat
        # 这是本域最关键的一条排障日志:玩家侧只会看到一句「没有可用 hub」,
        # 到底是真满了、全在 warming、还是筛选条件写错了,只能靠这里的分项计数区分。
        plog.get().warning(
            "hub_no_routable_shard",
            player_id=player_id,
            hub_assignment_id=assignment_id,
            region=region,
            team_id=team_id,
            release_track=release_track,
            exclude_pod=exclude_pod,
            reason=no_routable_shard_reason(census),
            **census.fields(),
        )
        raise errcode.PandoraError(
            errcode.ErrHubNoAvailable,
            "no authoritatively routable hub shard in region %s",
            region,
        )

    async def compensate_reserved_seat(
        self, pod: str, player_id: int, assignment_id: str, seat: L.ReserveResult | None
    ) -> None:
        """占座后置换失败的补偿退座。对应 Go 的 `compensateReservedSeat`。

        Model B 下 `seat is None` 表示**根本没占到座**,直接返回:拿零值身份去
        exact 退座会匹配不到任何 owner,只是白刷一条告警。
        """
        if self.auth_repo is None:
            await self.release_from_shard(pod)
            return
        if seat is None:
            return
        released = False
        exc: BaseException | None = None
        try:
            released = await self.auth_repo.release_assignment_seat(
                pod,
                L.AssignmentInstanceIdentity(
                    player_id=player_id,
                    assignment_id=assignment_id,
                    instance_uid=seat.instance_uid,
                    protocol_epoch=seat.protocol_epoch,
                    writer_epoch=seat.writer_epoch,
                ),
                self.shard_ttl_sec(),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as err:  # noqa: BLE001 —— 补偿失败只告警,交重启对账
            exc = err
        if exc is not None or not released:
            plog.get().warning(
                "hub_reserved_seat_compensation_failed",
                pod=pod,
                released=released,
                err=str(exc) if exc is not None else "",
            )

    async def release_assignment_seat(self, assignment: Any) -> None:
        """按归属记录的 exact 身份退座。对应 Go 的 `releaseAssignmentSeat`。

        ★ `released is False` 打 **Info** 而不是 Warn:它表示"账本里那个座位已经属于
          另一个实例了",在同名 Pod 重建后是**正常**结果,当告警会淹掉真问题。
        """
        if self.auth_repo is None:
            await self.release_from_shard(assignment.hub_pod_name)
            return
        try:
            released = await self.auth_repo.release_assignment_seat(
                assignment.hub_pod_name,
                assignment_instance_identity(assignment),
                self.shard_ttl_sec(),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "hub_assignment_seat_release_failed",
                pod=assignment.hub_pod_name,
                err=str(exc),
            )
            return
        if not released:
            plog.get().info(
                "hub_assignment_seat_release_skipped_stale_instance",
                pod=assignment.hub_pod_name,
            )

    async def routable_shard_views(self, shards: list) -> list:
        """把分片镜像列表换成**权威**路由视图。对应 Go 的 `routableShardViews`。

        legacy(未装配 `auth_repo`)原样返回;Model B 下逐个过 `check_routable`,
        不可路由的直接剔除 —— 对外的 ListHubs 不该展示一台回调会被全拒的 Hub。
        """
        if self.auth_repo is None:
            return shards
        out: list[Any] = []
        for shard in shards:
            if shard.state != STATE_READY:
                continue
            info = await self.auth_repo.check_routable(
                shard.hub_pod_name, L.now_ms(), self.heartbeat_max_age_ms()
            )
            if info.ok:
                out.append(authoritative_shard(shard, info))
        return out

    # ── 归属置换 saga ────────────────────────────────────────────────────

    async def replace_assignment_saga(  # noqa: C901
        self,
        player_id: int,
        old: Any,
        next_rec: Any,
        seat: L.ReserveResult | None,
        on_swapped: Any,
        disappeared_msg: str,
    ) -> bool:
        """「把玩家归属置换为 next」的**唯一** saga。对应 Go 的 `replaceAssignmentSaga`。

        AssignHub 的置换路径与 TransferHub 共用(此前是两份人工镜像的拷贝,补偿规则
        漂移即座位泄漏 / 提前释放已提交的新 owner)。

        调用前提:新座位已占(`seat`)、票据已完成**纯签名**但尚未执行 owner Begin;
        `old is None` 表示新建(无旧 owner)。只有本 saga 的 assignment CAS winner 才能
        在返回后绑定 owner。

        Returns:
            `True` = CAS 输给并发写者、已补偿干净,调用方 `continue` 重试;
            `False` = 本次置换已完成。失败一律抛异常(补偿责任已在本方法内履行完毕,
            或**刻意不补偿**交重启对账 —— 见 CAS 结果未知那一支)。

        ⚠️ drain 迁移(`migrate_player`)是本 saga 的**变体而非拷贝**:它签票在
        `register_transfer_cleanup` **之后**(与这里相反)、CAS 输者不重试、resume 失败
        走「回加源 member 索引」而非上抛、且无写者复核。并入前须先补 drain 侧故障注入
        测试,不得机械合并。
        """
        # 领来源版本(INC-20260818-003)。放在**最前面**:`register_transfer_cleanup`
        # 会把 next 的身份写进清理 ref,版本必须先定下来。
        # 铸不出号即整笔失败,绝不带着 0 走下去 —— 那会在已建立水位的玩家上被 owner 拒,
        # 白白经历一次占座 + 补偿。
        try:
            revision = self.mint_source_revision()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            await self.compensate_reserved_seat(
                next_rec.hub_pod_name, player_id, next_rec.assignment_id, seat
            )
            plog.get().warning(
                "hub_assignment_source_revision_mint_failed",
                player_id=player_id,
                pod=next_rec.hub_pod_name,
                err=str(exc),
                hint="写者租约已失或号段耗尽;本次置换整体放弃并补偿座位",
            )
            raise
        next_rec.source_revision = revision

        cleanup_registered = False
        if old is not None and self.auth_repo is not None:
            try:
                await self.register_transfer_cleanup(next_rec, old)
            except asyncio.CancelledError:
                raise
            except BaseException:  # noqa: BLE001
                await self.compensate_reserved_seat(
                    next_rec.hub_pod_name, player_id, next_rec.assignment_id, seat
                )
                raise
            cleanup_registered = True
        try:
            swapped = await self.repo.compare_and_swap_assignment(
                player_id, old, next_rec, self.assignment_saga_ttl_sec()
            )
        except asyncio.CancelledError:
            raise
        except BaseException:  # noqa: BLE001
            # CAS 结果**可能未知**。保留 index-first ref 与 exact reservation;
            # 重启对账能区分"已提交的 saga"与"孤儿 ref",而盲目补偿会退掉一个可能
            # 已经生效的新 owner 的座位。
            if not cleanup_registered:
                await self.compensate_reserved_seat(
                    next_rec.hub_pod_name, player_id, next_rec.assignment_id, seat
                )
            raise
        if not swapped:
            if cleanup_registered:
                await self.remove_transfer_cleanup_ref(
                    old.hub_pod_name, transfer_cleanup_ref(next_rec)
                )
            await self.compensate_reserved_seat(
                next_rec.hub_pod_name, player_id, next_rec.assignment_id, seat
            )
            return True

        await self.add_shard_member(next_rec.hub_pod_name, player_id)
        if on_swapped is not None:
            result = on_swapped()
            if result is not None and hasattr(result, "__await__"):
                await result
        if cleanup_registered:
            # 旧 owner 驱逐是显式 saga:源席位物理未离场时抛 ErrUnavailable,
            # 保留持久化的新 assignment 供 Login / reconcile 恢复,绝不双 owner。
            _, still_found = await self.resume_assignment_cleanup(
                player_id, next_rec.assignment_id
            )
            if not still_found:
                raise errcode.PandoraError(errcode.ErrInvalidState, "%s", disappeared_msg)
        elif old is not None:
            await self.release_assignment_seat(old)
            await self.remove_shard_member(old.hub_pod_name, player_id)
        self.confirm_writer_for_ticket(player_id)
        return False

    async def register_transfer_cleanup(self, target: Any, source: Any) -> None:
        """登记旧 owner 的精确清理。对应 Go 的 `registerTransferCleanup`。

        ★ **index-first 是刻意的**:崩溃 / CAS loser 至多留下一条可安全识别的孤儿 ref;
          而一次成功的 assignment CAS **绝不可能**对重启对账不可见。反过来做(先 CAS
          再登记索引)就会出现"新 owner 已生效、旧 owner 没人清"的永久残留。
        """
        if self.auth_repo is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "Hub owner cleanup authority unavailable"
            )
        if source.transfer_cleanup_pending or source.release_cleanup_pending:
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "previous Hub owner cleanup is still pending"
            )
        bind_transfer_cleanup_source(target, source)
        await self.repo.register_transfer_cleanup(
            source.hub_pod_name, transfer_cleanup_ref(target)
        )

    async def remove_transfer_cleanup_ref(
        self, source_pod: str, ref: R.TransferCleanupRef
    ) -> None:
        """摘掉一条清理 ref(best-effort)。对应 Go 的 `removeTransferCleanupRef`。"""
        try:
            await self.repo.remove_transfer_cleanup(source_pod, ref)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 残留由 reconciler 兜底
            plog.get().warning(
                "hub_owner_cleanup_index_remove_failed",
                source_pod=source_pod,
                player_id=ref.player_id,
                assignment_id=ref.target_assignment_id,
                err=str(exc),
            )

    async def resume_assignment_cleanup(  # noqa: C901,PLR0912
        self, player_id: int, assignment_id: str
    ) -> tuple[Any | None, bool]:
        """transfer / release owner cleanup 的**唯一相位驱动**。对应 Go 的 `resumeAssignmentCleanup`。

        ★ **无进程内状态**:每次重试都从当前 assignment 重新开始,先确认同一个 target
          Bind 再释放 source,并用 exact CAS 推进。assignment id 一旦变了就**绝不触碰**
          (`ErrLocatorConflict`)—— 那说明有更新的归属胜出,继续清理会删掉 winner。

        Returns:
            `(rec, True)` = cleanup 已清干净,rec 是当前 assignment;
            `(None, False)` = 归属已不存在(release cleanup 完成或本就没有)。
        """
        if player_id == 0 or assignment_id == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "Hub cleanup assignment identity required"
            )
        if self.auth_repo is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "Hub owner cleanup authority unavailable"
            )
        for _attempt in range(16):
            current = await self.repo.get_assignment(player_id)
            if current is None:
                return None, False
            if current.assignment_id != assignment_id:
                raise errcode.PandoraError(
                    errcode.ErrLocatorConflict, "Hub cleanup assignment was superseded"
                )
            if current.transfer_cleanup_pending and current.release_cleanup_pending:
                raise errcode.PandoraError(
                    errcode.ErrInvalidState, "Hub assignment has conflicting cleanup phases"
                )
            if current.release_cleanup_pending:
                ref = transfer_cleanup_ref(current)
                seat = await self.auth_repo.inspect_assignment_seat(
                    current.hub_pod_name, assignment_instance_identity(current)
                )
                if seat.conflict or (
                    not seat.reserved and not seat.connected and not seat.already_absent
                ):
                    raise errcode.PandoraError(
                        errcode.ErrInvalidState, "Hub release cleanup exact owner conflict"
                    )
                result = await self.auth_repo.release_assignment_seat_exact(
                    current.hub_pod_name,
                    assignment_instance_identity(current),
                    self.shard_ttl_sec(),
                )
                if result.departure_required:
                    # 物理栅栏:源 Hub 上真有一条活连接,容量账本清理不是驱逐证明。
                    raise errcode.PandoraError(
                        errcode.ErrUnavailable,
                        "source Hub became connected while release cleanup was running",
                    )
                if result.conflict or (not result.released and not result.already_absent):
                    raise errcode.PandoraError(
                        errcode.ErrInvalidState, "Hub release cleanup exact owner conflict"
                    )
                deleted = await self.repo.compare_and_swap_assignment(
                    player_id, current, None, 0
                )
                if not deleted:
                    continue
                await self.remove_shard_member(current.hub_pod_name, player_id)
                await self.remove_transfer_cleanup_ref(current.hub_pod_name, ref)
                return None, False
            if not current.transfer_cleanup_pending:
                if (
                    current.transfer_target_bound
                    or current.transfer_source_hub_pod_name != ""
                    or current.transfer_source_assignment_id != ""
                    or current.transfer_source_instance_uid != ""
                    or current.transfer_source_auth_epoch != 0
                    or current.transfer_source_auth_writer_epoch != 0
                    or current.release_cleanup_match_id != 0
                    or current.release_cleanup_placement_version != 0
                    or current.release_cleanup_operation_id != ""
                ):
                    raise errcode.PandoraError(
                        errcode.ErrInvalidState, "Hub assignment has orphan cleanup fields"
                    )
                return current, True
            source = transfer_cleanup_source(current)
            ref = transfer_cleanup_ref(current)
            if not current.transfer_target_bound:
                next_rec = R.clone(current)
                next_rec.transfer_target_bound = True
                marked = await self.repo.compare_and_swap_assignment(
                    player_id, current, next_rec, self.assignment_saga_ttl_sec()
                )
                if not marked:
                    continue
                current = next_rec
            seat = await self.auth_repo.inspect_assignment_seat(
                source.hub_pod_name, assignment_instance_identity(source)
            )
            if seat.conflict or (
                not seat.reserved and not seat.connected and not seat.already_absent
            ):
                raise errcode.PandoraError(
                    errcode.ErrInvalidState, "Hub transfer source cleanup exact owner conflict"
                )
            result = await self.auth_repo.release_assignment_seat_exact(
                source.hub_pod_name,
                assignment_instance_identity(source),
                self.shard_ttl_sec(),
            )
            if result.departure_required:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "source Hub became connected while transfer cleanup was running",
                )
            if result.conflict or (not result.released and not result.already_absent):
                raise errcode.PandoraError(
                    errcode.ErrInvalidState, "Hub transfer source cleanup exact owner conflict"
                )
            next_rec = R.clone(current)
            clear_transfer_cleanup(next_rec)
            cleared = await self.repo.compare_and_swap_assignment(
                player_id, current, next_rec, self.assignment_saga_ttl_sec()
            )
            if not cleared:
                continue
            await self.remove_shard_member(source.hub_pod_name, player_id)
            await self.remove_transfer_cleanup_ref(source.hub_pod_name, ref)
            return next_rec, True
        raise errcode.PandoraError(errcode.ErrInternal, "Hub owner cleanup CAS retry exhausted")

    # ── 票据 ─────────────────────────────────────────────────────────────

    def local_ticket_binding(self, a: Any) -> HubTicketBinding:
        """legacy 本地面为 hub 票据补齐完整实例绑定(七元组)。对应 Go 的 `localTicketBinding`。

        legacy 面 writer-v2 绑定的唯一写入点 `bind_assignment_auth` 在 `seat is None`
        时不写,归属记录恒为零值绑定 → 签出的票缺 `hub_assignment_id` 与实例绑定,
        UE Hub DS PostLogin fail-closed 踢人,客户端陷入 7s 重连循环(2026-08-04
        mode=local 实测)。

        这里取 `local_credential_ack` —— 与 env 一次性下发给本机 Hub DS 的**同一份**
        凭据(心跳应答 ACK 回显的也是它),拼出的绑定与 DS 自持身份逐字段相等,
        与 Model B 占座绑定语义等价,**不是伪造**。

        线上隔离(双重机械门):
          - `auth_repo is not None`(Model B 权威面)直接返回零值,不碰本地凭据源;
          - `LocalHubCredentialSource` 仅 `LocalHubFleetProvider` 实现,
            Agones / Mock fleet 的 `isinstance` 恒 False。
        """
        if self.auth_repo is not None:
            return HubTicketBinding()
        if not isinstance(self.fleet, LocalHubCredentialSource):
            return HubTicketBinding()
        release_track = sticky_release_track_or_none(a.release_track)
        if release_track is None or a.hub_pod_name == "" or a.assignment_id == "":
            return HubTicketBinding()
        cred = self.fleet.local_credential_ack(a.hub_pod_name)
        if cred is None:
            return HubTicketBinding()
        return HubTicketBinding(
            pod_name=a.hub_pod_name,
            instance_uid=cred.instance_uid,
            protocol_epoch=cred.protocol_epoch,
            credential_gen=cred.gen,
            credential_jti=cred.jti,
            hub_assignment_id=a.assignment_id,
            writer_epoch=cred.writer_epoch,
            release_track=release_track,
        )

    async def owner_target_for_hub_ticket(self, a: Any) -> tuple[OwnerTargetView, bool]:
        """签票点 owner Begin 的 exact 实例目标(§9.22 四元组 + 分配 ID + 轨道)。

        对应 Go 的 `ownerTargetForHubTicket`。

        **Model B**(`auth_repo is not None`):调用方已过 `assignment_binding_v2_complete`,
        目标逐字段取自归属记录。取不出 = 数据不自洽,返回 `False` 由调用方 fail-closed 拒签,
        **不允许回源分片镜像降级**(授权权威是 Redis 授权记录,分片镜像只是投影)。

        **legacy**(mock / local):归属记录恒无 uid/epoch,历史上因此拼不出 exact 身份、
        owner Begin 必被拒,只能把整个权威面关掉 —— 代价是 owner 权威里永远没有记录,
        而 login 的 §9.23 query-first 回查以 owner 为第一权威:玩家选完角、Hub 也分配成功,
        `GetResumeContext` 仍恒落 WAIT/OWNER_UNKNOWN,永远进不去大厅(2026-08-04 实测)。
        这里回源分片镜像补齐 uid/epoch(mode=local 的分片带真实进程实例身份,与下发给
        Hub DS 的凭据同源);mock 模式镜像里没有实例身份 → 返回 False,保持历史行为。
        """
        release_track = sticky_release_track_or_none(a.release_track)
        if release_track is None or a.hub_pod_name == "" or a.assignment_id == "":
            return OwnerTargetView(), False
        uid, epoch = a.hub_instance_uid, a.auth_epoch
        if uid == "" or epoch == 0:
            if self.auth_repo is not None:
                return OwnerTargetView(), False
            try:
                shard = await self.repo.get_shard(a.hub_pod_name)
            except asyncio.CancelledError:
                raise
            except BaseException:  # noqa: BLE001 —— 对齐 Go 的 `if err != nil { return false }`
                return OwnerTargetView(), False
            if shard is None:
                return OwnerTargetView(), False
            uid, epoch = shard.gameserver_uid, shard.auth_epoch
            if uid == "" or epoch == 0:
                return OwnerTargetView(), False
        return (
            OwnerTargetView(
                pod_name=a.hub_pod_name,
                instance_uid=uid,
                instance_epoch=epoch,
                assignment_or_allocation_id=a.assignment_id,
                release_track=release_track,
                # 来源版本直接取自**已发布的 assignment 记录**,不在这里现铸
                # (INC-20260818-003)。这一点是整条链的关键:Owner 要比较的是
                # 「Redis 里那条 assignment 有多新」,现铸会让一个迟到的 Begin 拿到
                # 比它所绑定的 assignment 更新的号,恰好绕过本门。
                source_revision=a.source_revision,
            ),
            True,
        )

    async def prepare_hub_ticket(
        self,
        player_id: int,
        role_id: int,
        assignment: Any,
        source_match_id: int,
        session_jti: str,
    ) -> PreparedHubTicket:
        """只做确定性的票据校验 / 签名,**不触碰 owner 权威**。对应 Go 的 `prepareHubTicket`。

        新 reservation 路径可在 assignment CAS **之前**调用它,从而保留"签名器失败
        不发布归属 / 不泄漏座位"的补偿语义;真正有副作用的 owner Begin 必须由
        `bind_owner_for_published_hub_assignment` 在 CAS winner 之后执行。
        """
        if assignment is None or self.signer is None:
            raise errcode.PandoraError(errcode.ErrUnavailable, "Hub ticket signer unavailable")
        if self.auth_repo is not None and not assignment_binding_v2_complete(
            assignment, player_id
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "refuse to sign hub ticket from incomplete writer-v2 assignment",
            )
        binding = ticket_binding_from_assignment(assignment)
        if binding.pod_name == "" and self.auth_repo is None:
            binding = self.local_ticket_binding(assignment)
        binding.source_match_id = source_match_id
        binding.session_jti = session_jti
        try:
            token, exp_ms = await self.signer.sign_hub_ticket(player_id, role_id, binding)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().error("sign_hub_ticket_failed", player_id=player_id, err=str(exc))
            raise errcode.PandoraError(
                errcode.ErrInternal, "sign hub ticket failed", cause=exc
            ) from exc
        target, target_ok = await self.owner_target_for_hub_ticket(assignment)
        if not target_ok and self.auth_repo is not None:
            # Model B 下 exact 身份必然可得(上面 assignment_binding_v2_complete 已保证);
            # 取不出说明数据不自洽,照 §9.22 fail-closed,绝不用不完整身份去写权威。
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "refuse to sign hub ticket without exact owner identity",
            )
        return PreparedHubTicket(
            token=token, expires_at_ms=exp_ms, owner_target=target, target_ok=target_ok
        )

    async def bind_owner_for_published_hub_assignment(
        self, player_id: int, assignment: Any, prepared: PreparedHubTicket
    ) -> None:
        """签票链**唯一**的 owner 副作用点。对应 Go 的 `bindOwnerForPublishedHubAssignment`。

        调用前 assignment 必须已由 Redis CAS 发布;Query owner 后、Begin 前再读一次
        Redis,**机械保证 CAS loser 永远碰不到 owner**。Begin 后还要复核一次,避免把
        已被后继 assignment 取代的票交给玩家。
        """

        async def guard() -> None:
            # 与 assignment 复核组成同一个 Begin 前门:旧 writer 即使在入口检查后才失租,
            # 也不得用尚未返回的请求触碰 owner。conflict 重试会重跑此门。
            self.confirm_writer_for_ticket(player_id)
            current = await self.repo.get_assignment(player_id)
            if current is None or not hub_assignment_delivery_equal(
                current, assignment, player_id
            ):
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "hub assignment changed before owner bind player=%d",
                    player_id,
                )
            if prepared.target_ok:
                current_target, ok = await self.owner_target_for_hub_ticket(current)
                if not ok or not owner_target_view_equal(
                    current_target, prepared.owner_target
                ):
                    raise errcode.PandoraError(
                        errcode.ErrUnavailable,
                        "hub owner target changed before owner bind player=%d",
                        player_id,
                    )

        if prepared.target_ok:
            try:
                await OA.owner_begin_player_guarded(
                    self.owner_auth,
                    player_id,
                    OA.OWNER_TYPE_HUB,
                    prepared.owner_target,
                    OWNER_BIND_BUDGET_SEC,
                    guard,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "hub_ticket_refused_owner_begin_failed",
                    player_id=player_id,
                    pod=prepared.owner_target.pod_name,
                    err=str(exc),
                    hint="assignment CAS winner 未能精确写入 owner;扣票并由调用方重试",
                )
                raise
        try:
            await guard()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "hub_ticket_withheld_assignment_changed",
                player_id=player_id,
                assignment_id=assignment.assignment_id,
                err=str(exc),
            )
            raise

    async def sign_hub_ticket(
        self,
        player_id: int,
        role_id: int,
        assignment: Any,
        source_match_id: int,
        session_jti: str,
    ) -> tuple[str, int]:
        """签票 = 纯签名 + owner 绑定。对应 Go 的 `signHubTicket`,返回 `(token, exp_ms)`。"""
        prepared = await self.prepare_hub_ticket(
            player_id, role_id, assignment, source_match_id, session_jti
        )
        await self.bind_owner_for_published_hub_assignment(player_id, assignment, prepared)
        return prepared.token, prepared.expires_at_ms

    async def resolve_owner_target_from_assignment(
        self, player_id: int
    ) -> tuple[OwnerTargetView, bool]:
        """从归属镜像重建 owner Begin 目标(census 自愈路径)。

        对应 Go 的 `resolveOwnerTargetFromAssignment`。与 `sign_hub_ticket` 的 Begin
        目标**同源**(`ticket_binding_from_assignment`),保证 exact 等值自洽;
        绑定不完整 / 无归属 → `(零值, False)` 不自愈。
        """
        try:
            assignment = await self.repo.get_assignment(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException:  # noqa: BLE001 —— 对齐 Go 的 `if err != nil || !found`
            return OwnerTargetView(), False
        if assignment is None:
            return OwnerTargetView(), False
        binding = ticket_binding_from_assignment(assignment)
        if binding.pod_name == "" or binding.instance_uid == "":
            return OwnerTargetView(), False
        return (
            OwnerTargetView(
                pod_name=binding.pod_name,
                instance_uid=binding.instance_uid,
                instance_epoch=binding.protocol_epoch,
                assignment_or_allocation_id=binding.hub_assignment_id,
                release_track=binding.release_track,
                # 来源版本与签票路径同源:取自**已发布的 assignment 记录**。
                # 漏填会让自愈 Begin 恒发 legacy=0,而该玩家水位已非零,owner 按
                # legacy_after_versioned 拒掉 —— 专为修漂移而建的这条通道会 100% 失效。
                source_revision=assignment.source_revision,
            ),
            True,
        )

    async def sign_result(
        self,
        player_id: int,
        role_id: int,
        assignment: Any,
        source_match_id: int,
        session_jti: str,
    ) -> AssignResult:
        """签票并组装 AssignHub 出参。对应 Go 的 `signResult`。"""
        token, exp_ms = await self.sign_hub_ticket(
            player_id, role_id, assignment, source_match_id, session_jti
        )
        return AssignResult(
            hub_ds_addr=assignment.hub_addr,
            hub_ticket=token,
            hub_pod_name=assignment.hub_pod_name,
            shard_id=assignment.shard_id,
            ticket_exp_ms=exp_ms,
        )
