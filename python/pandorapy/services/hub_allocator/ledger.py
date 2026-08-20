"""Hub 容量账本的 Redis 存取层 —— 对应 Go 侧
`services/battle/hub_allocator/internal/data/hub_capacity_ledger.go`。

六把 ledger key 与 auth/shard/wfence 共用 `{pod}` hashtag,可在**一次**
WATCH/MULTI/EXEC 里完成(Redis Cluster 同 slot):

    pandora:hub:reservations:{pod}        HASH assignment_id     -> HubReservationStorageRecord
    pandora:hub:reservation-expiry:{pod}  ZSET assignment_id     -> expires_at_ms
    pandora:hub:sessions:{pod}            HASH assignment_id     -> HubConnectedOwnershipStorageRecord
    pandora:hub:session-expiry:{pod}      ZSET assignment_id     -> expires_at_ms
    pandora:hub:successors:{pod}          HASH exact_capability  -> HubReservationStorageRecord
    pandora:hub:successor-expiry:{pod}    ZSET exact_capability  -> expires_at_ms

═══ 为什么本模块是「下层」,而 auth_repo.py 是「上层」═══

Go 把这些函数分在三个文件里(hub_capacity_ledger.go / hub_auth_repo.go /
hub_authoritative.go),但它们**同属一个 package**,互相调用没有方向。
Python 的 import 必须无环,而依赖是单向的:

    auth_repo.ActivateHeartbeat  →  需要 ledger 的 prune / write / 派生投影
    ledger.reserve_assignment    →  需要凭据谓词 cred_matches / v2_exact

所以「keys / 写者 fence / CAS 退避 / shard 编解码 / 凭据谓词」这些**两边都要用**
的底座落在本模块(下层),`auth_repo.py` import 本模块并把状态机建在上面。
拆法与 Go 的文件边界不同,但语义逐条对齐;每个搬过来的函数都标了 Go 出处。

★ 派生逻辑**不在这里** —— 它在 `capacity.py`,本模块只做「Redis 字节 ↔ proto ↔
  capacity.py 的视图」的搬运。三条不变量(player_count 严格派生 / connected
  ownership 无时间 TTL / successor 不重复计容)只有那一份实现,这里绝不重写。

★ read-modify-write 一律用默认的 `ParseFromString`,**不丢 unknown fields**
  (不变量 §17)。Python protobuf 默认保留 unknown fields 并原样重新序列化 ——
  这正是滚动升级期旧副本回写不得静默丢新字段的前提。
"""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import random
import time
import uuid as _uuid
from typing import Any, Protocol

from pandora.hub.v1 import allocator_pb2 as hubpb
from redis.exceptions import WatchError

from pandorapy import errcode
from pandorapy.auth import DS_AUTH_WRITER_EPOCH_V2
from pandorapy.services.hub_allocator import capacity

# ── Redis key 模板(逐字符照抄 Go;`{pod}` hashtag 一个都不能少)───────────────
#
# ★ 花括号不是装饰,是 Redis Cluster 的 hash tag。去掉之后:单机 Redis 上一切正常,
#   上了 Cluster 才会 CROSSSLOT 拒绝整个事务 —— 而「账本与授权记录同事务」正是
#   Model B 消灭半激活 / TOCTOU 误分配的全部依据。


def auth_key(pod: str) -> str:
    """`pandora:hub:auth:{pod}` —— Model B 唯一授权权威(hub_auth_repo.go authKey)。"""
    return f"pandora:hub:auth:{{{pod}}}"


def shard_key(pod: str) -> str:
    """`pandora:hub:shard:{pod}` —— 分片镜像(hub_repo.go shardKey)。"""
    return f"pandora:hub:shard:{{{pod}}}"


def wfence_key(pod: str) -> str:
    """`pandora:hub:wfence:{pod}` —— 写者继任水位(writer_fence.go wfenceKey)。"""
    return f"pandora:hub:wfence:{{{pod}}}"


def reservations_key(pod: str) -> str:
    return f"pandora:hub:reservations:{{{pod}}}"


def reservation_expiry_key(pod: str) -> str:
    return f"pandora:hub:reservation-expiry:{{{pod}}}"


def sessions_key(pod: str) -> str:
    return f"pandora:hub:sessions:{{{pod}}}"


def session_expiry_key(pod: str) -> str:
    return f"pandora:hub:session-expiry:{{{pod}}}"


def successors_key(pod: str) -> str:
    return f"pandora:hub:successors:{{{pod}}}"


def successor_expiry_key(pod: str) -> str:
    return f"pandora:hub:successor-expiry:{{{pod}}}"


def capacity_ledger_keys(pod: str) -> list[str]:
    """六把 ledger key,顺序与 Go 的 capacityLedgerKeys 逐个相同。"""
    return [
        reservations_key(pod),
        reservation_expiry_key(pod),
        sessions_key(pod),
        session_expiry_key(pod),
        successors_key(pod),
        successor_expiry_key(pod),
    ]


def instance_teardown_proof_key(pod: str) -> str:
    """`pandora:hub:instance-teardown:{pod}` HASH instance_uid -> 记录时刻。

    ★ 按**不可变 GameServer UID** 存,不按 pod 名:同名 Pod 重建后新实例既不能
      作废旧 UID 的 proof,旧 proof 也永远授权不了另一个 UID 的清理。
    """
    return f"pandora:hub:instance-teardown:{{{pod}}}"


# 全局索引(与 {pod} **不同 slot**,只能独立命令,不得并进事务)。
SHARDS_SET_KEY = "pandora:hub:shards"
ACTIVE_KEY = "pandora:hub:active"

# ── 常量(与 Go 同值)─────────────────────────────────────────────────────────

# 整键 retention 在最晚单项绝对到期之上再加的余量(Go capacityLedgerRetentionGuard)。
CAPACITY_LEDGER_RETENTION_GUARD_MS = 30_000

# Go hubAuthCASRetries。同一 Hub 的高并发 Assign/Transfer/Heartbeat 争用 auth+shard
# 两键;冲突重读不重放外部副作用,预算给足让真实并发收敛,耗尽仍 fail-closed。
HUB_AUTH_CAS_RETRIES = 64

SUCCESSOR_CAPABILITY_DOMAIN = "pandora-hub-successor-v1"

_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1


def now_ms() -> int:
    """服务端接收时刻(unix ms)。Go: time.Now().UnixMilli()。"""
    return int(time.time() * 1000)


# ── 错误(Go 的哨兵 error 在 Python 里做成异常类型)──────────────────────────


class AuthStaleError(errcode.PandoraError):
    """对应 Go 的 `errAuthStale`。

    Model B 授权校验失败(无授权记录 / uid|epoch 不符 / gen 非当前 / 凭据不匹配)。
    用 ErrUnauthorized(=8)对 DS 呈现明确鉴权拒绝码,fail-closed:记录零变更。

    ★ 做成**类型**而不是模块级单例:Go 用 `err == errAuthStale` 比身份,Python 里
      单例异常被 raise 后会被写入 `__traceback__`,跨协程复用同一个对象会串味。
      类型判定(`except AuthStaleError`)与 Go 的身份判定等价且没有这个坑。
    """

    __slots__ = ()

    def __init__(self, msg: str = "hub ds credential not authoritative") -> None:
        super().__init__(errcode.ErrUnauthorized, msg)


class WriterSupersededError(errcode.PandoraError):
    """对应 Go 的 `ErrWriterSuperseded`(writer_fence.go)。

    本副本的写者租约已失效 / 被更新写者继任 → fail-closed 零写入。
    ErrUnavailable 语义:对调用方可重试(会被路由到新写者副本),对本副本是终态拒绝。
    """

    __slots__ = ()

    def __init__(
        self,
        msg: str = "hub allocator writer lease superseded; retry against current writer",
    ) -> None:
        super().__init__(errcode.ErrUnavailable, msg)


# ── 写者继任 fencing ─────────────────────────────────────────────────────────


class WriterFence(Protocol):
    """提供当前写者的 fencing token(Go: data.WriterFence)。

    ★ **返回顺序照 Python 侧的 `pandorapy.writerlease.Lease`,不是 Go 的顺序**:

        Go   `WriterFence.Current() (token uint64, held bool)`
        Py   `writerlease.Lease.current() -> (held: bool, token: int)`

      这条契约由 `writer_fence.py` 模块头钉死(全模块唯一解包点 `fence_snapshot`),
      本文件的协议声明与 `guard_writer_fence` 必须与之同向 —— 反了不会有任何
      运行期信号:非零 token 被当成 `held=True`,而 `held` 被当成 token(0/1),
      于是「失主的旧写者」照样放行、真水位(>1)又让每一笔容量事务恒判 superseded。
    """

    def current(self) -> tuple[bool, int]:
        """返回 (是否持有写者租约, token)。token 历届严格单调递增。"""
        ...


def fenced_watch_keys(keys: list[str], pod: str, fence: WriterFence | None) -> list[str]:
    """启用 fence 时把 `wfenceKey(pod)` 并入 WATCH 集(同 slot)。Go: fencedWatchKeys。

    ★ 不并进 WATCH 集就等于「比较与推进之间的并发继任可以绕过乐观锁」——
      前任读到旧水位、继任者刚写完新水位,前任的 EXEC 照样成功。
    """
    if fence is None:
        return list(keys)
    return [*keys, wfence_key(pod)]


async def guard_writer_fence(pipe: Any, pod: str, fence: WriterFence | None):
    """在 WATCH 回调内做 fence 比较,返回「推进闭包」。Go: guardWriterFence。

    cur > mine  → 抛 WriterSupersededError(零写入):继任者已触达此 slot,
                  前任的迟到写永久出局(即使前任进程尚未察觉失主);
    cur < mine  → 返回的闭包把水位推进到 mine(SET 进**同一** EXEC);
    cur == mine → 直接放行。

    ★ 水位键刻意持久(SET 不带 TTL)、RemoveShard 也不删:fencing 水位必须比业务
      记录长寿,否则删除即复位,迟到旧写者可借尸还魂。
    """
    if fence is None:
        return _noop_advance
    # ★ `(held, token)` —— 顺序理由见 WriterFence 协议的 docstring 与
    #   writer_fence.py 模块头。抄成 `mine, held = ...` 会静默拆掉整道 fencing。
    held, mine = fence.current()
    if not held:
        raise WriterSupersededError
    raw = await pipe.get(wfence_key(pod))
    cur = 0
    if raw is not None:
        try:
            cur = int(_as_str(raw))
        except ValueError as exc:
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "hub writer fence %s corrupt value %r", pod, raw
            ) from exc
    if cur > mine:
        raise WriterSupersededError
    if cur == mine:
        return _noop_advance
    key, val = wfence_key(pod), str(mine)

    def _advance(p: Any) -> None:
        p.set(key, val)  # 持久:fencing 水位必须比业务记录长寿

    return _advance


def _noop_advance(_pipe: Any) -> None:
    """未启用 fence / 无需推进时占位。Go: noopAdvance。"""


async def cas_conflict_backoff(attempt: int) -> None:
    """WATCH/CAS 乐观并发冲突后的指数 + 抖动退避。Go: casConflictBackoff。

    ★ 零间隔紧循环在高并发争用同 `{pod}` 键时会互相踩踏:每次 EXEC 成功都会打断
      其余全部在途 WATCH 事务,落后者可能连续输掉全部预算。首次冲突立即重试保住
      低争用延迟;此后自 1ms 指数升至 16ms 封顶,叠加 [-50%,+50%) 抖动打散同拍。
    """
    if attempt <= 0:
        return
    shift = min(attempt - 1, 4)
    base = 0.001 * (1 << shift)
    await asyncio.sleep(base / 2 + random.random() * base)  # noqa: S311 (非密码学用途)


# ── shard 镜像编解码(Go: hub_repo.go)────────────────────────────────────────


def marshal_shard(rec: hubpb.HubShardStorageRecord) -> bytes:
    if rec is None:
        raise errcode.PandoraError(errcode.ErrInvalidState, "nil hub shard")
    return rec.SerializeToString()


def unmarshal_shard(pod: str, payload: bytes) -> hubpb.HubShardStorageRecord:
    """Go: unmarshalShard。pod 不符即整条拒(别的 pod 的镜像绝不能当本 pod 用)。"""
    rec = hubpb.HubShardStorageRecord()
    try:
        rec.ParseFromString(bytes(payload))
    except Exception as exc:  # noqa: BLE001 —— proto 解码失败的具体类型随实现变化
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "hub shard %s bad proto: %s", pod, exc
        ) from exc
    if rec.hub_pod_name == "":
        rec.hub_pod_name = pod
    if rec.hub_pod_name != pod:
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "hub shard %s pod mismatch: %s", pod, rec.hub_pod_name
        )
    return rec


def drain_rank(state: str) -> int:
    """ready<draining<stopping。Go: drainRank。"""
    if state == "draining":
        return 1
    if state == "stopping":
        return 2
    return 0


def apply_heartbeat_state_to_shard(
    rec: hubpb.HubShardStorageRecord, state: str, ts_ms: int
) -> None:
    """Go: applyHeartbeatStateToShard。

    ★ 容量 player_count **不在这里**:它由 reservation + connected ownership 账本
      派生(capacity.sync_shard_projection),绝不接受 DS reported count 覆盖。
    """
    if rec.state == "warming":
        # 首个通过授权的心跳即「DS 已就绪且可信」的直接证据:warming → ready。
        # 若 DS 首跳已上报更高 drain 等级,则采纳其上报,不强行 ready。
        rec.state = state if drain_rank(state) > 0 else "ready"
    elif state == "":
        pass  # 空上报:不动状态。
    elif drain_rank(state) >= drain_rank(rec.state):
        rec.state = state  # 升级或同级 drain → 采用 DS 上报
    elif state == "ready" and rec.state == "draining" and rec.draining_since_ms == 0:
        rec.state = "ready"  # 存活恢复:心跳超时误标的 draining 被健康心跳复位
    # 其余降级(强制整合 draining 被 ready 冲)→ 保持 rec.state 不变。
    rec.last_heartbeat_ms = ts_ms


# ── 凭据谓词(Go: hub_auth_repo.go / hub_authoritative.go)────────────────────


@dataclasses.dataclass(frozen=True, slots=True)
class CredentialIdentity:
    """DS 心跳携带的、已验签的完整凭据身份。Go: data.CredentialIdentity。

    ★ **四元组** `(instance_uid, protocol_epoch, gen, jti)` 才是身份(allocator.proto
      HubDSCredential 注释明写):代际计数器可能因 TTL 复位导致 **gen 复用**,
      只比 gen 会把「复位后重新发出的第 7 代」认成「复位前那第 7 代」。
      kid / token_sha256 再补一层完整性绑定,writer_epoch 是代际能力门。
    """

    gen: int = 0
    jti: str = ""
    instance_uid: str = ""
    protocol_epoch: int = 0
    token_sha256: str = ""
    kid: str = ""
    writer_epoch: int = 0


def phase_locked(phase: int) -> bool:
    """相位是否已锁定(不再接受新 stage / promote / 分配)。Go: phaseLocked。

    QUARANTINED(紧急吊销)与 TERMINATING(实例下线中)一律拒绝写入侧与授权侧副作用,
    只能经显式受控 purge/recreate 恢复;普通 InitAuth(包括换 UID)也不得绕过 tombstone。

    ★ 枚举值从 `_pb2` 取,不手抄:手抄的常量在 proto 改动后**不会报错**,
      只会让「已吊销」被判成「未知相位」→ 一台被隔离的 DS 重新可分配。
    """
    return phase in (
        hubpb.HUB_AUTH_PHASE_QUARANTINED,
        hubpb.HUB_AUTH_PHASE_TERMINATING,
    )


def phase_serving(phase: int) -> bool:
    """phase ∈ {ACTIVE, ROTATING} —— 轮换期 active 仍有效。"""
    return phase in (hubpb.HUB_AUTH_PHASE_ACTIVE, hubpb.HUB_AUTH_PHASE_ROTATING)


def validate_stored_credential(cred, at_ms: int) -> None:
    """拒绝不完整或已过期的权威凭据。Go: validateStoredCredential。

    JWT 本身只在投递 / 中间件处验签;Redis 记录至少必须完整保存能唯一识别凭据的
    uid/epoch/gen/jti/kid/hash/exp/writer_epoch —— 少任何一项,后续的
    `cred_matches` 就只能降级成「比 gen」,而 gen 会复用。
    """
    if cred is None or (
        cred.gen == 0
        or cred.jti == ""
        or cred.instance_uid == ""
        or cred.protocol_epoch == 0
        or cred.kid == ""
        or cred.token_sha256 == ""
        or cred.exp_ms == 0
        or cred.writer_epoch == 0
    ):
        raise errcode.PandoraError(
            errcode.ErrInvalidArg,
            "hub auth credential requires uid/epoch/gen/jti/kid/hash/exp/writer_epoch",
        )
    if cred.writer_epoch != DS_AUTH_WRITER_EPOCH_V2:
        raise AuthStaleError
    if cred.exp_ms <= at_ms:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "hub auth credential already expired")


def _stored_credential_ok(cred, at_ms: int) -> bool:
    """validate_stored_credential 的布尔版(Go 里是 `... != nil` 的取反)。"""
    try:
        validate_stored_credential(cred, at_ms)
    except errcode.PandoraError:
        return False
    return True


def stored_credential_equal(a, b) -> bool:
    """比较**当前协议已知**的完整凭据字段。Go: storedCredentialEqual。

    ★ 刻意不用 proto 的全等比较:滚动更新期间新版本可能追加 unknown fields,
      旧副本必须保留它们,但**不能**因为不认识新字段而把同一 tuple 判成不同。
    """
    return (
        a is not None
        and b is not None
        and a.gen == b.gen
        and a.jti == b.jti
        and a.exp_ms == b.exp_ms
        and a.kid == b.kid
        and a.instance_uid == b.instance_uid
        and a.protocol_epoch == b.protocol_epoch
        and a.token_sha256 == b.token_sha256
        and a.writer_epoch == b.writer_epoch
    )


def cred_matches(cred, ident: CredentialIdentity, at_ms: int | None = None) -> bool:
    """心跳凭据身份与已存凭据是否同一份。Go: credMatches。

    ★ hash 不允许缺失后降级为只比 gen+jti;uid/epoch 同时绑定凭据内字段和记录级
      字段,防损坏记录或计数器回退造成身份混淆。
    """
    at = now_ms() if at_ms is None else at_ms
    if not _stored_credential_ok(cred, at):
        return False
    if (
        ident.gen == 0
        or ident.jti == ""
        or ident.instance_uid == ""
        or ident.protocol_epoch == 0
        or ident.token_sha256 == ""
        or ident.kid == ""
        or ident.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
    ):
        return False
    if (
        cred.gen != ident.gen
        or cred.jti != ident.jti
        or cred.instance_uid != ident.instance_uid
        or cred.protocol_epoch != ident.protocol_epoch
        or cred.kid != ident.kid
        or cred.writer_epoch != ident.writer_epoch
    ):
        return False
    return cred.token_sha256 == ident.token_sha256


def hub_stored_credential_epochs_v2(rec) -> bool:
    """Go: hubStoredCredentialEpochsV2。active/pending 若存在必须是 V2 代际。"""
    if rec is None:
        return False
    active_ok = (not rec.HasField("active")) or rec.active.writer_epoch == DS_AUTH_WRITER_EPOCH_V2
    pending_ok = (
        not rec.HasField("pending")
    ) or rec.pending.writer_epoch == DS_AUTH_WRITER_EPOCH_V2
    return active_ok and pending_ok


def hub_auth_record_v2_exact(rec) -> bool:
    """Go: hubAuthRecordV2Exact。

    ★ `required_writer_epoch` 必须**恰等于** V2,不是 >=。这是 Model B 的机械激活
      栅栏:栅栏只允许 Activation Job 单调推进、永不回退;当前二进制看到更高的
      required 说明这条记录已属于未来 writer,只能由对应未来二进制处理。
    """
    return (
        rec is not None
        and rec.required_writer_epoch == DS_AUTH_WRITER_EPOCH_V2
        and hub_stored_credential_epochs_v2(rec)
    )


def routable_credential_complete(cred, auth_rec, at_ms: int) -> bool:
    """Go: routableCredentialComplete。凭据完整、未过期,且与记录级身份互相钉死。"""
    return (
        cred is not None
        and auth_rec is not None
        and auth_rec.pod_name != ""
        and auth_rec.instance_uid != ""
        and auth_rec.protocol_epoch > 0
        and cred.gen > 0
        and cred.jti != ""
        and cred.exp_ms > at_ms
        and cred.kid != ""
        and cred.token_sha256 != ""
        and cred.instance_uid == auth_rec.instance_uid
        and cred.protocol_epoch == auth_rec.protocol_epoch
        and hub_auth_record_v2_exact(auth_rec)
        and cred.writer_epoch == DS_AUTH_WRITER_EPOCH_V2
    )


# ── 身份 / 结果结构(Go: hub_auth_repo.go 顶部)──────────────────────────────


@dataclasses.dataclass(frozen=True, slots=True)
class ReservationIdentity:
    """逐 assignment 容量 lease 的稳定身份。Go: data.ReservationIdentity。

    ★ 只绑定**不会在同一 GameServer 生命周期内变化**的 UID/epoch/writer:
      凭据 gen/jti 会平滑轮换,把它们钉进 reservation 会让一次正常轮换
      把全场玩家的座位判成「不属于本实例」而被 prune 掉。
    """

    player_id: int = 0
    assignment_id: str = ""
    instance_uid: str = ""
    protocol_epoch: int = 0
    writer_epoch: int = 0
    # Placement*:首次占座可空(placement saga 在选座之后才持久化);已连接 assignment
    # 重签时业务层必填 —— successor lease 就是按这个 exact version+operation 编 key 的,
    # 另一个 placement operation 的票据因此消费不掉它。
    placement_version: int = 0
    placement_operation_id: str = ""
    source_match_id: int = 0
    expires_at_ms: int = 0
    assignment_expires_at_ms: int = 0


@dataclasses.dataclass(frozen=True, slots=True)
class AssignmentInstanceIdentity:
    """赢得 assignment CAS 后退座所需的稳定实例身份。Go: AssignmentInstanceIdentity。"""

    player_id: int = 0
    assignment_id: str = ""
    instance_uid: str = ""
    protocol_epoch: int = 0
    writer_epoch: int = 0
    placement_version: int = 0
    placement_operation_id: str = ""
    source_match_id: int = 0


@dataclasses.dataclass(slots=True)
class ReserveResult:
    """Go: data.ReserveResult。reason 仅日志用,不外露客户端。"""

    ok: bool = False
    reason: str = ""
    active_gen: int = 0
    active_jti: str = ""
    instance_uid: str = ""
    protocol_epoch: int = 0
    writer_epoch: int = 0
    shard_id: int = 0
    hub_addr: str = ""
    region: str = ""
    player_count: int = 0
    capacity: int = 0
    release_track: str = ""


@dataclasses.dataclass(slots=True)
class AdmissionResult:
    """Go: data.AdmissionResult。"""

    admitted: bool = False
    already_admitted: bool = False
    conflict: bool = False
    reserved_count: int = 0
    connected_count: int = 0
    capacity_occupancy: int = 0


@dataclasses.dataclass(slots=True)
class DepartureResult:
    """Go: data.DepartureResult。conflict=True → 旧连接晚到 Logout 必须零副作用停重试。"""

    departed: bool = False
    already_departed: bool = False
    conflict: bool = False
    reserved_count: int = 0
    connected_count: int = 0
    capacity_occupancy: int = 0


@dataclasses.dataclass(slots=True)
class ReleaseAssignmentSeatResult:
    """Go: ReleaseAssignmentSeatResult。

    ★ 为什么不能是一个 bool:false 曾同时表示「已经不在了」和「另一个 owner 占着」,
      而 durable cleanup worker 对这两者的处置完全相反(推进 vs fail-closed)。
    ★ departure_required 是**物理栅栏**:匹配到的是活的 connected owner,容量账本
      清理不是物理驱逐证明,绝不能删,必须由 source DS 踢人并 ACK Departure
      (或其 GameServer UID 被权威 teardown)。
    """

    released: bool = False
    already_absent: bool = False
    conflict: bool = False
    departure_required: bool = False


@dataclasses.dataclass(slots=True)
class AssignmentSeatSnapshot:
    """只读物理 owner 视图。Go: AssignmentSeatSnapshot。四个状态布尔恰有一个为真。"""

    reserved: bool = False
    connected: bool = False
    already_absent: bool = False
    conflict: bool = False
    reservation_expires_at_ms: int = 0
    admission_id: str = ""
    admission_seq: int = 0


# ── successor capability 编解码(Go: successorCapability / decodeSuccessorCapability)


def _go_uuid_v4_canonical(value: str) -> bool:
    """Go 的 `uuid.Parse(s) == nil && parsed != Nil && Version()==4 && String()==s`。

    ★ 刻意**不**复用 `placement.valid_operation_id`:那一份还额外要求 RFC4122
      variant(正则里的 `[89ab]`),比 Go 严。严的方向听起来安全,但这里的失败面是
      「整本账本解码失败」——一条 Go 能写、Python 读不了的 capability 会让整台 Hub
      的容量事务全部报错,不是 fail-closed 而是 fail-stuck。所以逐位照抄 Go。
    """
    if not isinstance(value, str):
        return False
    try:
        parsed = _uuid.UUID(value)
    except (ValueError, AttributeError, TypeError):
        return False
    # Go 的 Version() 是纯位运算,不看 variant;Python 的 .version 在 variant 非
    # RFC4122 时返回 None,直接用会比 Go 严 —— 所以在这里手算那 4 个 bit。
    return int(parsed) != 0 and ((int(parsed) >> 76) & 0xF) == 4 and str(parsed) == value


def reservation_placement_valid(ident) -> bool:
    """Go: reservationPlacementValid。

    version==0 → 三个 placement 字段必须**同时**为空(半截 lineage 不是「旧记录」,
    是坏数据);version!=0 → operation_id 必须是 canonical UUIDv4。
    """
    if ident.placement_version == 0:
        return ident.placement_operation_id == "" and ident.source_match_id == 0
    return _go_uuid_v4_canonical(ident.placement_operation_id)


def successor_capability(pod: str, ident) -> str:
    """把 (domain, player, assignment, pod, uid, epoch, writer, version, op, match)
    十段用 `\\n` 连接后 base64 RawURL 编码。Go: successorCapability。

    ★ 为什么**编码全元组**而不是只哈希:每个 loader 都要能验证「HASH field、
      protobuf value、admission 请求」三者描述的是同一个不可变目标与同一次 placement
      操作。只存哈希就只能验「等不等」,验不出「是什么」。
    ★ 编码前拒绝换行:canonical 形式必须无歧义,否则一个含 `\\n` 的 assignment_id
      能伪造出别人的 capability。
    """
    if (
        pod == ""
        or ident.player_id == 0
        or ident.assignment_id == ""
        or ident.instance_uid == ""
        or ident.protocol_epoch == 0
        or ident.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
        or not reservation_placement_valid(ident)
        or any(
            ch in ("\r", "\n")
            for ch in (pod + ident.assignment_id + ident.instance_uid + ident.placement_operation_id)
        )
    ):
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "hub successor capability identity invalid"
        )
    canonical = "\n".join(
        [
            SUCCESSOR_CAPABILITY_DOMAIN,
            str(ident.player_id),
            ident.assignment_id,
            pod,
            ident.instance_uid,
            str(ident.protocol_epoch),
            str(ident.writer_epoch),
            str(ident.placement_version),
            ident.placement_operation_id,
            str(ident.source_match_id),
        ]
    )
    return base64.urlsafe_b64encode(canonical.encode("utf-8")).decode("ascii").rstrip("=")


def decode_successor_capability(capability: str, expected_pod: str) -> ReservationIdentity:
    """Go: decodeSuccessorCapability。解出的元组必须**重编码后逐字节相等**。

    ★ 那道回环校验(`encoded != capability` 即拒)不是多余:base64 有非 canonical
      变体(补位 bit 非零、带 `=` 填充),不同字节的 field 能解出同一个元组 ——
      于是同一个 assignment 会有两条「都合法」的 successor,重复计容且清不干净。
    """
    pad = "=" * (-len(capability) % 4)
    try:
        raw = base64.urlsafe_b64decode(capability + pad)
    except Exception as exc:  # noqa: BLE001 —— binascii.Error 及其别名
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "invalid hub successor capability encoding"
        ) from exc
    try:
        parts = raw.decode("utf-8").split("\n")
    except UnicodeDecodeError as exc:
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "invalid hub successor capability encoding"
        ) from exc
    if (
        len(parts) != 10
        or parts[0] != SUCCESSOR_CAPABILITY_DOMAIN
        or expected_pod == ""
        or parts[3] != expected_pod
    ):
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "invalid hub successor capability encoding"
        )
    try:
        player_id = _parse_uint(parts[1], 64)
        protocol_epoch = _parse_uint(parts[5], 32)
        writer_epoch = _parse_uint(parts[6], 32)
        placement_version = _parse_uint(parts[7], 64)
        source_match_id = _parse_uint(parts[9], 64)
    except ValueError as exc:
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "invalid hub successor capability numbers"
        ) from exc
    ident = ReservationIdentity(
        player_id=player_id,
        assignment_id=parts[2],
        instance_uid=parts[4],
        protocol_epoch=protocol_epoch,
        writer_epoch=writer_epoch,
        placement_version=placement_version,
        placement_operation_id=parts[8],
        source_match_id=source_match_id,
    )
    try:
        encoded = successor_capability(parts[3], ident)
    except errcode.PandoraError as exc:
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "non-canonical hub successor capability"
        ) from exc
    if encoded != capability:
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "non-canonical hub successor capability"
        )
    return ident


def _parse_uint(text: str, bits: int) -> int:
    """Go 的 strconv.ParseUint(s, 10, bits):不接受符号、前后空白、下划线。"""
    if text == "" or not text.isdigit() or not text.isascii():
        raise ValueError(f"not a base-10 uint: {text!r}")
    value = int(text)
    if value > (1 << bits) - 1:
        raise ValueError(f"uint{bits} overflow: {text!r}")
    return value


# ── 记录匹配谓词(Go: reservationRecordMatches / sessionRecordMatches)────────


def reservation_record_matches(rec, pod: str, ident) -> bool:
    """Go: reservationRecordMatches。

    ★ 对**入参本身**的四条硬约束(player!=0 / assignment!="" / uid!="" / epoch!=0)
      与 `writer == V2` 一条都不能省 —— 它们防的不是「记录脏」,是「调用方拿着空身份
      来问」:空串会与同样是空串的旧格式记录**互相匹配上**。同 capacity.py 的注释。
    """
    return (
        rec is not None
        and ident.player_id != 0
        and ident.assignment_id != ""
        and ident.instance_uid != ""
        and ident.protocol_epoch != 0
        and ident.writer_epoch == DS_AUTH_WRITER_EPOCH_V2
        and rec.player_id == ident.player_id
        and rec.assignment_id == ident.assignment_id
        and rec.hub_pod_name == pod
        and rec.hub_instance_uid == ident.instance_uid
        and rec.auth_epoch == ident.protocol_epoch
        and rec.auth_writer_epoch == ident.writer_epoch
    )


def session_record_matches(rec, pod: str, ident) -> bool:
    """Go: sessionRecordMatches(字段集与 reservation 相同,类型不同)。"""
    return reservation_record_matches(rec, pod, ident)


# ── 账本容器与 capacity.py 的桥 ─────────────────────────────────────────────


@dataclasses.dataclass(slots=True)
class HubCapacityLedger:
    """一台 Hub Pod 的账本(proto 记录)。Go: hubCapacityLedger。"""

    reservations: dict[str, hubpb.HubReservationStorageRecord] = dataclasses.field(
        default_factory=dict
    )
    sessions: dict[str, hubpb.HubConnectedOwnershipStorageRecord] = dataclasses.field(
        default_factory=dict
    )
    # key 是 exact capability(不是 assignment_id)—— 同一 assignment 重签会换 capability。
    successors: dict[str, hubpb.HubReservationStorageRecord] = dataclasses.field(
        default_factory=dict
    )


def _rec_view(rec) -> capacity.LedgerRecord:
    return capacity.LedgerRecord(
        assignment_id=rec.assignment_id,
        hub_pod_name=rec.hub_pod_name,
        hub_instance_uid=rec.hub_instance_uid,
        auth_epoch=rec.auth_epoch,
        auth_writer_epoch=rec.auth_writer_epoch,
        expires_at_ms=rec.expires_at_ms,
    )


def _view(ledger: HubCapacityLedger) -> capacity.CapacityLedger:
    """把 proto 账本投影成 capacity.py 的视图。

    ★ 投影而不是「在这里再算一遍」:三条不变量(严格派生 / 无时间 TTL /
      successor 不重复计容)只有 capacity.py 一份实现。本模块只负责搬字节。
    """
    return capacity.CapacityLedger(
        reservations={k: _rec_view(v) for k, v in ledger.reservations.items()},
        sessions={k: _rec_view(v) for k, v in ledger.sessions.items()},
        successors={k: _rec_view(v) for k, v in ledger.successors.items()},
    )


def prune_ledger(
    ledger: HubCapacityLedger, *, pod: str, uid: str, epoch: int, writer: int, at_ms: int
) -> None:
    """Go: pruneHubCapacityLedger。就地修改;判据整段委托给 capacity.prune。"""
    view = _view(ledger)
    capacity.prune(view, pod=pod, uid=uid, epoch=epoch, writer=writer, now_ms=at_ms)
    for key in [k for k in ledger.reservations if k not in view.reservations]:
        del ledger.reservations[key]
    for key in [k for k in ledger.sessions if k not in view.sessions]:
        del ledger.sessions[key]
    for key in [k for k in ledger.successors if k not in view.successors]:
        del ledger.successors[key]


def ledger_counts(ledger: HubCapacityLedger, shard_capacity: int) -> tuple[int, int]:
    """Go: capacityLedgerCounts。整段委托 capacity.counts(含完整性闸门)。"""
    return capacity.counts(_view(ledger), shard_capacity)


def sync_shard_capacity_projection(
    shard: hubpb.HubShardStorageRecord, ledger: HubCapacityLedger
) -> None:
    """把账本派生结果同步到分片投影。Go: syncShardCapacityProjection。

    ★ `player_count = reserved + connected`,**只从账本来**。心跳实报只写
      `reported_connected_count` 审计字段(见 capacity.apply_heartbeat_audit)。
    ★ 三个字段全部由 capacity.sync_shard_projection 算,本函数只做搬运 ——
      在这里顺手补一句「也更新一下 X」就等于开了第二个派生入口。
    """
    projection = capacity.ShardProjection(capacity=shard.capacity)
    capacity.sync_shard_projection(projection, _view(ledger))
    shard.reserved_count = projection.reserved_count
    shard.connected_ownership_count = projection.connected_ownership_count
    shard.player_count = projection.player_count


# ── 账本读写 ─────────────────────────────────────────────────────────────────


def _as_str(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="strict")
    return str(value)


async def load_hub_capacity_ledger(
    pipe: Any, pod: str, shard_capacity: int
) -> HubCapacityLedger:
    """Go: loadHubCapacityLedger。必须在 WATCH 之后、同一事务快照内调用。

    ★ 先 HLEN 再 HGETALL:损坏 / 被攻击写入的巨型 HASH 会把 HGETALL 变成无界扫描。
      单表上限先挡一道,并集容量在解码后由 capacity.counts 再验一次。
    ★ 任何一条对不上就**整条拒**(不是挑掉坏的继续算):账本自身不自洽说明有并发
      写者绕过了同槽事务,此时任何派生值都不可信,继续算只会把错误写回 Redis。
    """
    if shard_capacity <= 0:
        raise errcode.PandoraError(errcode.ErrInvalidState, "hub capacity must be positive")
    r_key, s_key, x_key = reservations_key(pod), sessions_key(pod), successors_key(pod)
    r_len = await pipe.hlen(r_key)
    s_len = await pipe.hlen(s_key)
    x_len = await pipe.hlen(x_key)
    if (
        r_len > shard_capacity
        or s_len > shard_capacity
        or x_len > shard_capacity
        or r_len + s_len > shard_capacity
    ):
        raise errcode.PandoraError(
            errcode.ErrInvalidState,
            "hub capacity ledger exceeds capacity: reservations=%d sessions=%d "
            "successors=%d capacity=%d",
            r_len,
            s_len,
            x_len,
            shard_capacity,
        )
    raw_reservations = await pipe.hgetall(r_key)
    raw_sessions = await pipe.hgetall(s_key)
    raw_successors = await pipe.hgetall(x_key)

    ledger = HubCapacityLedger()
    for field, raw in (raw_reservations or {}).items():
        assignment_id = _as_str(field)
        rec = hubpb.HubReservationStorageRecord()
        try:
            rec.ParseFromString(bytes(raw))
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "decode hub reservation %s/%s: %s", pod, assignment_id, exc
            ) from exc
        ledger.reservations[assignment_id] = rec
    for field, raw in (raw_sessions or {}).items():
        assignment_id = _as_str(field)
        rec = hubpb.HubConnectedOwnershipStorageRecord()
        try:
            rec.ParseFromString(bytes(raw))
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "decode hub session %s/%s: %s", pod, assignment_id, exc
            ) from exc
        ledger.sessions[assignment_id] = rec
    for field, raw in (raw_successors or {}).items():
        cap_field = _as_str(field)
        rec = hubpb.HubReservationStorageRecord()
        try:
            rec.ParseFromString(bytes(raw))
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "decode hub successor %s/%s: %s", pod, cap_field, exc
            ) from exc
        try:
            identity = decode_successor_capability(cap_field, pod)
        except errcode.PandoraError as exc:
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "hub successor capability identity mismatch"
            ) from exc
        if not reservation_record_matches(rec, pod, identity):
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "hub successor capability identity mismatch"
            )
        ledger.successors[cap_field] = rec

    # ★ 完整性闸门 + 并集容量:HASH field ↔ 记录内 assignment_id、同 assignment 只能
    #   有一条 successor、reservation∩session 为空、总数不超容量 —— 全在 capacity.counts
    #   里(它内部先跑 validate_loaded)。这一步刻意**在 prune 之前**:prune 会删记录,
    #   可能恰好抹掉一条重复 successor,让坏账本蒙混过关。
    ledger_counts(ledger, shard_capacity)
    return ledger


async def load_bounded_successors(
    pipe: Any, pod: str, shard_capacity: int
) -> dict[str, hubpb.HubReservationStorageRecord]:
    """Go: loadBoundedSuccessors。只知道 base assignment owner 的清理路径用。

    必须在 WATCH successorsKey + shardKey 之后调用;HLEN 先对着权威 shard capacity
    校验,损坏输入才不能把清理变成无界扫描。
    """
    count = await pipe.hlen(successors_key(pod))
    if count == 0:
        return {}
    if shard_capacity <= 0 or count > shard_capacity:
        raise errcode.PandoraError(
            errcode.ErrInvalidState,
            "hub successor scan exceeds shard capacity: successors=%d capacity=%d",
            count,
            shard_capacity,
        )
    raw_records = await pipe.hgetall(successors_key(pod))
    out: dict[str, hubpb.HubReservationStorageRecord] = {}
    for field, raw in (raw_records or {}).items():
        cap_field = _as_str(field)
        rec = hubpb.HubReservationStorageRecord()
        rec.ParseFromString(bytes(raw))
        try:
            decoded = decode_successor_capability(cap_field, pod)
        except errcode.PandoraError as exc:
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "hub successor capability identity mismatch"
            ) from exc
        if not reservation_record_matches(rec, pod, decoded):
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "hub successor capability identity mismatch"
            )
        out[cap_field] = rec
    return out


def write_hub_capacity_ledger(pipe: Any, pod: str, ledger: HubCapacityLedger) -> None:
    """把整本账本写回(先 DEL 六键再重建)。Go: writeHubCapacityLedger。

    必须在 `pipe.multi()` 之后调用 —— 命令要落在同一个 EXEC 里。

    ★ 整键 retention 取「最晚单项绝对到期 + guard」:沿用较短的 shardTTL 会提前删掉
      仍然有效的 lease,于是玩家拿着合法票据却发现座位没了。
    ★ session 的 expiry ZSET 只在 `expires_at_ms > 0` 时写:新格式恒 0(不按时间过期),
      写进去就等于给「已连接」加了一个假的到期时刻。
    """
    r_key, rx_key = reservations_key(pod), reservation_expiry_key(pod)
    s_key, sx_key = sessions_key(pod), session_expiry_key(pod)
    x_key, xx_key = successors_key(pod), successor_expiry_key(pod)
    pipe.delete(r_key, rx_key, s_key, sx_key, x_key, xx_key)

    reservation_max = 0
    for assignment_id in sorted(ledger.reservations):
        rec = ledger.reservations[assignment_id]
        pipe.hset(r_key, assignment_id, rec.SerializeToString())
        pipe.zadd(rx_key, {assignment_id: float(rec.expires_at_ms)})
        reservation_max = max(reservation_max, rec.expires_at_ms)

    session_max = 0
    for assignment_id in sorted(ledger.sessions):
        rec = ledger.sessions[assignment_id]
        pipe.hset(s_key, assignment_id, rec.SerializeToString())
        if rec.expires_at_ms > 0:
            pipe.zadd(sx_key, {assignment_id: float(rec.expires_at_ms)})
            session_max = max(session_max, rec.expires_at_ms)

    successor_max = 0
    for cap_field in sorted(ledger.successors):
        rec = ledger.successors[cap_field]
        pipe.hset(x_key, cap_field, rec.SerializeToString())
        pipe.zadd(xx_key, {cap_field: float(rec.expires_at_ms)})
        successor_max = max(successor_max, rec.expires_at_ms)

    if reservation_max > 0:
        deadline = reservation_max + CAPACITY_LEDGER_RETENTION_GUARD_MS
        pipe.pexpireat(r_key, deadline)
        pipe.pexpireat(rx_key, deadline)
    if session_max > 0:
        deadline = session_max + CAPACITY_LEDGER_RETENTION_GUARD_MS
        pipe.pexpireat(s_key, deadline)
        pipe.pexpireat(sx_key, deadline)
    if successor_max > 0:
        deadline = successor_max + CAPACITY_LEDGER_RETENTION_GUARD_MS
        pipe.pexpireat(x_key, deadline)
        pipe.pexpireat(xx_key, deadline)


def successor_for_assignment(
    ledger: HubCapacityLedger, assignment_id: str
) -> tuple[str, hubpb.HubReservationStorageRecord | None, bool]:
    """Go: successorForAssignment。"""
    for cap_field, rec in ledger.successors.items():
        if rec.assignment_id == assignment_id:
            return cap_field, rec, True
    return "", None, False


def successor_matches(
    ledger: HubCapacityLedger, pod: str, ident: ReservationIdentity
) -> tuple[str, hubpb.HubReservationStorageRecord | None, bool, bool]:
    """Go: successorMatches → (capability, rec, exists, exact)。"""
    cap_field, rec, exists = successor_for_assignment(ledger, ident.assignment_id)
    if not exists:
        return "", None, False, False
    try:
        expected = successor_capability(pod, ident)
    except errcode.PandoraError:
        return cap_field, rec, True, False
    if cap_field != expected or not reservation_record_matches(rec, pod, ident):
        return cap_field, rec, True, False
    return cap_field, rec, True, True


def successor_for_cleanup(
    records: dict[str, hubpb.HubReservationStorageRecord],
    pod: str,
    expected: ReservationIdentity,
    at_ms: int,
) -> tuple[str, hubpb.HubReservationStorageRecord | None, bool, bool]:
    """Go: successorForCleanup → (capability, rec, exists, conflict)。

    接受缺失的 placement lineage(兼容已持久化的旧 transfer-cleanup 记录),
    但**绝不接受不同的 base owner**;lineage 存在时必须精确匹配。
    同一 assignment 出现多条 capability 是 ABA / 损坏冲突,不是「可以挑一个」。
    """
    found_field = ""
    found: hubpb.HubReservationStorageRecord | None = None
    for cap_field, rec in records.items():
        if rec.assignment_id != expected.assignment_id:
            continue
        try:
            decoded = decode_successor_capability(cap_field, pod)
        except errcode.PandoraError:
            return "", None, False, True
        if not reservation_record_matches(rec, pod, expected):
            return "", None, False, True
        if expected.placement_version != 0 and rec.expires_at_ms > at_ms:
            if decoded.placement_version > expected.placement_version:
                # 更新 placement 的活 capability 可能属于本次清理快照之后才胜出的
                # 写者;绝不能反向取消它。
                return "", None, False, True
            if decoded.placement_version == expected.placement_version and (
                decoded.placement_operation_id != expected.placement_operation_id
                or decoded.source_match_id != expected.source_match_id
            ):
                # 同版本分叉是 ABA 冲突。严格更旧的 capability 不同:assignment bind
                # 可能在 successor 轮换之前就提交了 vN+1,所以 vN+1 的清理可以取消 vN。
                return "", None, False, True
        if found is not None:
            return "", None, False, True
        found_field, found = cap_field, rec
    return found_field, found, found is not None, False


# ── Model B 路由 / 回调门(Go: modelBRoutableReason / modelBCallbackReason)────


def modelb_routable_reason(
    auth_rec,
    shard,
    pod: str,
    at_ms: int,
    max_heartbeat_age_ms: int,
    credential: CredentialIdentity | None,
) -> str:
    """分配前的完整可路由判据。返回 "" 表示可路由。Go: modelBRoutableReason。"""
    if auth_rec is None or auth_rec.pod_name != pod or not hub_auth_record_v2_exact(auth_rec):
        return "auth-missing-or-invalid"
    if not phase_serving(auth_rec.phase):
        return "phase-not-active"
    if not routable_credential_complete(auth_rec.active, auth_rec, at_ms):
        return "no-active"
    if credential is not None and not cred_matches(auth_rec.active, credential, at_ms):
        return "credential-not-active"
    if shard is None or shard.hub_pod_name != pod or shard.state != "ready":
        return "shard-not-ready"
    active = auth_rec.active
    if (
        shard.last_verified_gen != active.gen
        or shard.last_verified_jti != active.jti
        or shard.gameserver_uid != auth_rec.instance_uid
        or shard.auth_epoch != auth_rec.protocol_epoch
        or shard.last_verified_writer_epoch != active.writer_epoch
    ):
        return "shard-not-verified-by-active"
    if (
        shard.capacity <= 0
        or shard.reported_max_players == 0
        or shard.reported_max_players != shard.capacity
    ):
        return "max-players-mismatch"
    if (
        shard.player_count != shard.reserved_count + shard.connected_ownership_count
        or shard.player_count < 0
        or shard.player_count > shard.capacity
    ):
        return "capacity-projection-invalid"
    # ★ 滚动上线时旧连接尚无 admission ledger:实报人数**大于** connected ownership
    #   必须阻断新分配,不能把 reported count 回填成权威(低估会超发)。
    #   漏报方向只审计、绝不删 owner。
    if (
        shard.reported_connected_count < 0
        or shard.reported_connected_count > shard.connected_ownership_count
    ):
        return "untracked-connected-players"
    if auth_rec.last_active_heartbeat_ms <= 0 or auth_rec.last_active_heartbeat_ms > at_ms:
        return "heartbeat-invalid"
    if (
        max_heartbeat_age_ms > 0
        and at_ms - auth_rec.last_active_heartbeat_ms > max_heartbeat_age_ms
    ):
        return "heartbeat-stale"
    return ""


def modelb_callback_reason(
    auth_rec, shard, pod: str, at_ms: int, credential: CredentialIdentity
) -> str:
    """Admission / Departure 的 callback 身份门。Go: modelBCallbackReason。

    ★ 与 routable 的差别是**刻意**的:已有 reservation/session 的 ACK 不再要求
      shard 仍 ready(此时它可能已经在 draining),否则一台正在排空的 Hub 上的玩家
      永远退不了场;但必须仍由当前 active credential 调用,且实例身份 / MaxPlayers /
      容量投影完全一致。
    """
    if auth_rec is None or auth_rec.pod_name != pod or not hub_auth_record_v2_exact(auth_rec):
        return "auth-missing-or-invalid"
    if not phase_serving(auth_rec.phase):
        return "phase-not-active"
    if not routable_credential_complete(auth_rec.active, auth_rec, at_ms) or not cred_matches(
        auth_rec.active, credential, at_ms
    ):
        return "credential-not-active"
    if shard is None or shard.hub_pod_name != pod:
        return "shard-missing"
    active = auth_rec.active
    if (
        shard.last_verified_gen != active.gen
        or shard.last_verified_jti != active.jti
        or shard.gameserver_uid != auth_rec.instance_uid
        or shard.auth_epoch != auth_rec.protocol_epoch
        or shard.last_verified_writer_epoch != active.writer_epoch
    ):
        return "shard-not-verified-by-active"
    if (
        shard.capacity <= 0
        or shard.reported_max_players == 0
        or shard.reported_max_players != shard.capacity
    ):
        return "max-players-mismatch"
    if (
        shard.player_count != shard.reserved_count + shard.connected_ownership_count
        or shard.player_count < 0
        or shard.player_count > shard.capacity
    ):
        return "capacity-projection-invalid"
    return ""


def _reserve_result_from_state(auth_rec, shard) -> ReserveResult:
    """Go: reserveResultFromState。"""
    return ReserveResult(
        ok=True,
        active_gen=auth_rec.active.gen,
        active_jti=auth_rec.active.jti,
        instance_uid=auth_rec.instance_uid,
        protocol_epoch=auth_rec.protocol_epoch,
        writer_epoch=auth_rec.active.writer_epoch,
        shard_id=shard.shard_id,
        hub_addr=shard.hub_addr,
        region=shard.region,
        player_count=shard.player_count,
        capacity=shard.capacity,
        release_track=shard.release_track,
    )


def _clone_reservation(rec) -> hubpb.HubReservationStorageRecord:
    """Go: proto.Clone。用 CopyFrom 而不是字段逐个抄 —— unknown fields 必须跟着走。"""
    out = hubpb.HubReservationStorageRecord()
    out.CopyFrom(rec)
    return out


# ── 账本事务(Go: RedisHubAuthRepo 的容量方法)────────────────────────────────


class HubCapacityLedgerOps:
    """六把 ledger key 的 WATCH/MULTI/EXEC 事务。Go: RedisHubAuthRepo 的容量部分。

    `auth_repo.RedisHubAuthRepo` 继承本类,于是 Python 侧的方法集与 Go 的
    `*RedisHubAuthRepo` 一致(Go 是同一个 struct 上的方法,只是分了文件)。
    """

    __slots__ = ("_rdb", "_fence")

    def __init__(self, rdb: Any, fence: WriterFence | None = None) -> None:
        self._rdb = rdb
        self._fence = fence

    def set_writer_fence(self, fence: WriterFence | None) -> None:
        """Go: SetWriterFence。"""
        self._fence = fence

    # ── instance teardown proof ──────────────────────────────────────────────

    async def record_instance_teardown_proof(
        self, pod: str, instance_uid: str, proof_ttl_sec: float
    ) -> None:
        """Go: RecordInstanceTeardownProof。

        ★ proof 也必须受写者 fence 约束:失主副本伪造 proof 等于绕过 Departure 物理门
          —— 它解锁的是「删掉一个活着的 connected ownership」。
        """
        if pod == "" or instance_uid == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "hub instance teardown proof requires pod and uid"
            )
        key = instance_teardown_proof_key(pod)
        stamp = now_ms()
        if self._fence is None:
            async with self._rdb.pipeline(transaction=True) as pipe:
                pipe.multi()
                pipe.hset(key, instance_uid, stamp)
                if proof_ttl_sec > 0:
                    pipe.expire(key, int(proof_ttl_sec))
                await pipe.execute()
            return
        for attempt in range(HUB_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(wfence_key(pod))
                    advance = await guard_writer_fence(pipe, pod, self._fence)
                    pipe.multi()
                    advance(pipe)
                    pipe.hset(key, instance_uid, stamp)
                    if proof_ttl_sec > 0:
                        pipe.expire(key, int(proof_ttl_sec))
                    await pipe.execute()
                return
            except WatchError:
                await cas_conflict_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "hub teardown proof %s: cas retry exhausted", pod
        )

    async def has_instance_teardown_proof(self, pod: str, instance_uid: str) -> bool:
        """Go: HasInstanceTeardownProof。"""
        if pod == "" or instance_uid == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "hub instance teardown proof requires pod and uid"
            )
        return bool(await self._rdb.hexists(instance_teardown_proof_key(pod), instance_uid))

    # ── ReserveAssignment ────────────────────────────────────────────────────

    async def reserve_assignment(
        self,
        pod: str,
        reservation: ReservationIdentity,
        at_ms: int,
        max_heartbeat_age_ms: int,
        shard_ttl_sec: float,
    ) -> ReserveResult:
        """以逐 assignment reservation 取代整数 seat++。Go: ReserveAssignment。"""
        if at_ms <= 0:
            at_ms = now_ms()
        if (
            pod == ""
            or reservation.player_id == 0
            or reservation.assignment_id == ""
            or reservation.instance_uid == ""
            or reservation.protocol_epoch == 0
            or reservation.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
            or reservation.expires_at_ms <= at_ms
            or reservation.assignment_expires_at_ms < reservation.expires_at_ms
            or shard_ttl_sec <= 0
            or not reservation_placement_valid(reservation)
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "hub reservation identity/expiry invalid"
            )
        successor_field = successor_capability(pod, reservation)
        a_key, s_key = auth_key(pod), shard_key(pod)
        watch_keys = fenced_watch_keys(
            [a_key, s_key, instance_teardown_proof_key(pod), *capacity_ledger_keys(pod)],
            pod,
            self._fence,
        )
        for attempt in range(HUB_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(*watch_keys)
                    advance = await guard_writer_fence(pipe, pod, self._fence)
                    a_raw = await pipe.get(a_key)
                    if a_raw is None:
                        await _validate_snapshot(pipe, watch_keys)
                        return ReserveResult(reason="auth-missing")
                    s_raw = await pipe.get(s_key)
                    if s_raw is None:
                        await _validate_snapshot(pipe, watch_keys)
                        return ReserveResult(reason="shard-missing")
                    auth_rec = hubpb.HubShardAuthStorageRecord()
                    auth_rec.ParseFromString(bytes(a_raw))
                    shard = unmarshal_shard(pod, s_raw)
                    reason = modelb_routable_reason(
                        auth_rec, shard, pod, at_ms, max_heartbeat_age_ms, None
                    )
                    if reason:
                        await _validate_snapshot(pipe, watch_keys)
                        return ReserveResult(reason=reason)
                    if (
                        reservation.instance_uid != auth_rec.instance_uid
                        or reservation.protocol_epoch != auth_rec.protocol_epoch
                        or reservation.writer_epoch != auth_rec.active.writer_epoch
                    ):
                        await _validate_snapshot(pipe, watch_keys)
                        return ReserveResult(reason="reservation-instance-mismatch")

                    ledger = await load_hub_capacity_ledger(pipe, pod, shard.capacity)
                    prune_ledger(
                        ledger,
                        pod=pod,
                        uid=auth_rec.instance_uid,
                        epoch=auth_rec.protocol_epoch,
                        writer=auth_rec.active.writer_epoch,
                        at_ms=at_ms,
                    )
                    outcome = _reserve_mutate(
                        ledger, pod, reservation, successor_field, at_ms, shard.capacity
                    )
                    if outcome:
                        await _validate_snapshot(pipe, watch_keys)
                        return ReserveResult(reason=outcome)

                    sync_shard_capacity_projection(shard, ledger)
                    shard_payload = marshal_shard(shard)
                    pipe.multi()
                    advance(pipe)
                    write_hub_capacity_ledger(pipe, pod, ledger)
                    pipe.set(s_key, shard_payload, px=int(shard_ttl_sec * 1000))
                    await pipe.execute()
                    return _reserve_result_from_state(auth_rec, shard)
            except WatchError:
                await cas_conflict_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "hub reserve assignment %s: cas retry exhausted", pod
        )

    # ── AcknowledgeAdmission ─────────────────────────────────────────────────

    async def acknowledge_admission(
        self,
        pod: str,
        credential: CredentialIdentity,
        reservation: ReservationIdentity,
        admission_id: str,
        admission_seq: int,
        at_ms: int,
        shard_ttl_sec: float,
    ) -> AdmissionResult:
        """reservation → connected ownership 的原子消费。Go: AcknowledgeAdmission。

        同 assignment 的新 admission_id 表示重连 / 旧连接替换:更新 owner 但**不增加容量**。
        """
        if at_ms <= 0:
            at_ms = now_ms()
        if (
            not _go_uuid_v4_canonical(admission_id)
            or shard_ttl_sec <= 0
            or reservation.player_id == 0
            or reservation.assignment_id == ""
            or reservation.instance_uid == ""
            or reservation.protocol_epoch == 0
            or reservation.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
            or admission_seq == 0
            or not reservation_placement_valid(reservation)
        ):
            raise errcode.PandoraError(errcode.ErrInvalidArg, "hub admission identity invalid")
        a_key, s_key = auth_key(pod), shard_key(pod)
        watch_keys = fenced_watch_keys(
            [a_key, s_key, *capacity_ledger_keys(pod)], pod, self._fence
        )
        for attempt in range(HUB_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(*watch_keys)
                    advance = await guard_writer_fence(pipe, pod, self._fence)
                    a_raw = await pipe.get(a_key)
                    s_raw = None if a_raw is None else await pipe.get(s_key)
                    if a_raw is None or s_raw is None:
                        await _validate_snapshot(pipe, watch_keys)
                        raise AuthStaleError
                    auth_rec = hubpb.HubShardAuthStorageRecord()
                    auth_rec.ParseFromString(bytes(a_raw))
                    shard = unmarshal_shard(pod, s_raw)
                    if (
                        modelb_callback_reason(auth_rec, shard, pod, at_ms, credential) != ""
                        or reservation.instance_uid != auth_rec.instance_uid
                        or reservation.protocol_epoch != auth_rec.protocol_epoch
                        or reservation.writer_epoch != auth_rec.active.writer_epoch
                    ):
                        await _validate_snapshot(pipe, watch_keys)
                        raise AuthStaleError

                    ledger = await load_hub_capacity_ledger(pipe, pod, shard.capacity)
                    prune_ledger(
                        ledger,
                        pod=pod,
                        uid=auth_rec.instance_uid,
                        epoch=auth_rec.protocol_epoch,
                        writer=auth_rec.active.writer_epoch,
                        at_ms=at_ms,
                    )
                    out = AdmissionResult()
                    verdict = _admission_mutate(
                        ledger, pod, reservation, admission_id, admission_seq, at_ms, out
                    )
                    if verdict == "conflict":
                        await _validate_snapshot(pipe, watch_keys)
                        return out
                    if verdict == "rejected":
                        await _validate_snapshot(pipe, watch_keys)
                        raise AuthStaleError

                    sync_shard_capacity_projection(shard, ledger)
                    shard_payload = marshal_shard(shard)
                    pipe.multi()
                    advance(pipe)
                    write_hub_capacity_ledger(pipe, pod, ledger)
                    pipe.set(s_key, shard_payload, px=int(shard_ttl_sec * 1000))
                    await pipe.execute()
                    out.admitted = True
                    out.reserved_count = shard.reserved_count
                    out.connected_count = shard.connected_ownership_count
                    out.capacity_occupancy = shard.player_count
                    return out
            except WatchError:
                await cas_conflict_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "hub admission %s: cas retry exhausted", pod
        )

    # ── AcknowledgeDeparture ─────────────────────────────────────────────────

    async def acknowledge_departure(
        self,
        pod: str,
        credential: CredentialIdentity,
        reservation: ReservationIdentity,
        admission_id: str,
        admission_seq: int,
        at_ms: int,
        shard_ttl_sec: float,
    ) -> DepartureResult:
        """exact 删除当前 admission owner。Go: AcknowledgeDeparture。

        ★ 冲突时必须保持新 owner、TTL、projection **逐字不变**:顺手 prune / 续期
          其他记录会让一条迟到的旧 Logout 变成对新连接的写操作。
        """
        if at_ms <= 0:
            at_ms = now_ms()
        if (
            not _go_uuid_v4_canonical(admission_id)
            or shard_ttl_sec <= 0
            or reservation.player_id == 0
            or reservation.assignment_id == ""
            or reservation.instance_uid == ""
            or reservation.protocol_epoch == 0
            or reservation.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
            or admission_seq == 0
        ):
            raise errcode.PandoraError(errcode.ErrInvalidArg, "hub departure identity invalid")
        a_key, s_key = auth_key(pod), shard_key(pod)
        watch_keys = fenced_watch_keys(
            [a_key, s_key, *capacity_ledger_keys(pod)], pod, self._fence
        )
        for attempt in range(HUB_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(*watch_keys)
                    advance = await guard_writer_fence(pipe, pod, self._fence)
                    a_raw = await pipe.get(a_key)
                    s_raw = None if a_raw is None else await pipe.get(s_key)
                    if a_raw is None or s_raw is None:
                        await _validate_snapshot(pipe, watch_keys)
                        raise AuthStaleError
                    auth_rec = hubpb.HubShardAuthStorageRecord()
                    auth_rec.ParseFromString(bytes(a_raw))
                    shard = unmarshal_shard(pod, s_raw)
                    if (
                        modelb_callback_reason(auth_rec, shard, pod, at_ms, credential) != ""
                        or reservation.instance_uid != auth_rec.instance_uid
                        or reservation.protocol_epoch != auth_rec.protocol_epoch
                        or reservation.writer_epoch != auth_rec.active.writer_epoch
                    ):
                        await _validate_snapshot(pipe, watch_keys)
                        raise AuthStaleError

                    ledger = await load_hub_capacity_ledger(pipe, pod, shard.capacity)
                    prune_ledger(
                        ledger,
                        pod=pod,
                        uid=auth_rec.instance_uid,
                        epoch=auth_rec.protocol_epoch,
                        writer=auth_rec.active.writer_epoch,
                        at_ms=at_ms,
                    )
                    out = DepartureResult()
                    _departure_mutate(ledger, pod, reservation, admission_id, admission_seq, out)
                    if out.conflict:
                        await _validate_snapshot(pipe, watch_keys)
                        return out

                    sync_shard_capacity_projection(shard, ledger)
                    shard_payload = marshal_shard(shard)
                    pipe.multi()
                    advance(pipe)
                    write_hub_capacity_ledger(pipe, pod, ledger)
                    pipe.set(s_key, shard_payload, px=int(shard_ttl_sec * 1000))
                    await pipe.execute()
                    out.reserved_count = shard.reserved_count
                    out.connected_count = shard.connected_ownership_count
                    out.capacity_occupancy = shard.player_count
                    return out
            except WatchError:
                await cas_conflict_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "hub departure %s: cas retry exhausted", pod
        )

    # ── InspectAssignmentSeat(只读)──────────────────────────────────────────

    async def inspect_assignment_seat(
        self, pod: str, expected: AssignmentInstanceIdentity
    ) -> AssignmentSeatSnapshot:
        """只读物理 owner 视图,**零变更**。Go: InspectAssignmentSeat。

        ★ 刻意不把 TTL 或心跳缺席当作 departure 证明:connected ownership 在
          Departure / UID teardown 之前一直有效。
        """
        if (
            pod == ""
            or expected.player_id == 0
            or expected.assignment_id == ""
            or expected.instance_uid == ""
            or expected.protocol_epoch == 0
            or expected.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "complete assignment seat inspection identity required"
            )
        identity = _reservation_identity_of(expected)
        if not reservation_placement_valid(identity):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "assignment seat inspection placement identity invalid"
            )
        watch_keys = [shard_key(pod), reservations_key(pod), sessions_key(pod), successors_key(pod)]
        for attempt in range(HUB_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(*watch_keys)
                    shard_capacity = 0
                    raw = await pipe.get(shard_key(pod))
                    if raw is not None:
                        shard_capacity = unmarshal_shard(pod, raw).capacity
                    successors = await load_bounded_successors(pipe, pod, shard_capacity)
                    at_ms = now_ms()
                    _, successor, successor_exists, successor_conflict = successor_for_cleanup(
                        successors, pod, identity, at_ms
                    )
                    if successor_conflict:
                        await _validate_snapshot(pipe, watch_keys)
                        return AssignmentSeatSnapshot(conflict=True)
                    if successor_exists and successor.expires_at_ms <= at_ms:
                        # 到期的接力既不是物理 owner 也不是容量 owner;只读检查可以忽略。
                        successor, successor_exists = None, False

                    reservation = None
                    raw = await pipe.hget(reservations_key(pod), expected.assignment_id)
                    if raw is not None:
                        reservation = hubpb.HubReservationStorageRecord()
                        reservation.ParseFromString(bytes(raw))
                    session = None
                    raw = await pipe.hget(sessions_key(pod), expected.assignment_id)
                    if raw is not None:
                        session = hubpb.HubConnectedOwnershipStorageRecord()
                        session.ParseFromString(bytes(raw))
                    if reservation is not None and session is not None:
                        raise errcode.PandoraError(
                            errcode.ErrInvalidState,
                            "assignment exists in reservation and session ledgers",
                        )
                    out = _inspect_verdict(
                        reservation, session, successor, successor_exists, pod, identity
                    )
                    # 只读 MULTI/EXEC:验证 WATCH 快照,不刷任何 TTL、不改权威状态。
                    await _validate_snapshot(pipe, watch_keys)
                    return out
            except WatchError:
                await cas_conflict_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrUnavailable,
            "hub assignment seat inspection %s: cas retry exhausted",
            pod,
        )

    # ── ReleaseAssignmentSeat ────────────────────────────────────────────────

    async def release_assignment_seat(
        self, pod: str, expected: AssignmentInstanceIdentity, shard_ttl_sec: float
    ) -> bool:
        """Go: ReleaseAssignmentSeat(布尔封装)。"""
        result = await self.release_assignment_seat_exact(pod, expected, shard_ttl_sec)
        return result.released

    async def release_assignment_seat_exact(
        self, pod: str, expected: AssignmentInstanceIdentity, shard_ttl_sec: float
    ) -> ReleaseAssignmentSeatResult:
        """Go: ReleaseAssignmentSeatExact。

        ★ 只精确删除**尚未 Admission** 的 reservation。live connected ownership 不是
          一条可随意回收的容量记录:它对应旧 Hub 上真实存在的 PlayerController/Pawn,
          必须由 exact Departure 或**已确认的 UID teardown** 删除。
        """
        if (
            pod == ""
            or expected.player_id == 0
            or expected.assignment_id == ""
            or expected.instance_uid == ""
            or expected.protocol_epoch == 0
            or expected.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
            or shard_ttl_sec <= 0
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "complete assignment seat release identity required"
            )
        identity = _reservation_identity_of(expected)
        if not reservation_placement_valid(identity):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "assignment seat release placement identity invalid"
            )
        a_key, s_key = auth_key(pod), shard_key(pod)
        watch_keys = fenced_watch_keys(
            [a_key, s_key, *capacity_ledger_keys(pod)], pod, self._fence
        )
        px = int(shard_ttl_sec * 1000)
        for attempt in range(HUB_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(*watch_keys)
                    advance = await guard_writer_fence(pipe, pod, self._fence)
                    out = await self._release_body(
                        pipe, pod, expected, identity, advance, watch_keys, px
                    )
                    return out
            except WatchError:
                await cas_conflict_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "hub assignment release %s: cas retry exhausted", pod
        )

    async def _release_body(  # noqa: C901 —— 与 Go 同形状,拆开反而对不上行
        self,
        pipe: Any,
        pod: str,
        expected: AssignmentInstanceIdentity,
        identity: ReservationIdentity,
        advance,
        watch_keys: list[str],
        px: int,
    ) -> ReleaseAssignmentSeatResult:
        a_key, s_key = auth_key(pod), shard_key(pod)
        out = ReleaseAssignmentSeatResult()
        scan_capacity = 0
        raw = await pipe.get(s_key)
        if raw is not None:
            scan_capacity = unmarshal_shard(pod, raw).capacity
        successors = await load_bounded_successors(pipe, pod, scan_capacity)
        successor_field, successor, successor_exists, successor_conflict = successor_for_cleanup(
            successors, pod, identity, now_ms()
        )
        if successor_conflict:
            await _validate_snapshot(pipe, watch_keys)
            out.conflict = True
            return out

        reservation = None
        raw = await pipe.hget(reservations_key(pod), expected.assignment_id)
        if raw is not None:
            reservation = hubpb.HubReservationStorageRecord()
            reservation.ParseFromString(bytes(raw))
        session = None
        raw = await pipe.hget(sessions_key(pod), expected.assignment_id)
        if raw is not None:
            session = hubpb.HubConnectedOwnershipStorageRecord()
            session.ParseFromString(bytes(raw))
        if reservation is not None and session is not None:
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "assignment exists in reservation and session ledgers"
            )

        reservation_ok = reservation_record_matches(reservation, pod, identity)
        session_ok = session_record_matches(session, pod, identity)
        successor_ok = (not successor_exists) or reservation_record_matches(
            successor, pod, identity
        )
        if reservation is None and session is None and not successor_exists:
            out.already_absent = True
            # ★ WATCH 在 EXEC 之前没有线性化效果。cleanup 把 AlreadyAbsent 当作权威的
            #   「物理不存在」证明,所以这份空快照必须用一次只读事务验证过才能返回成功。
            await _validate_snapshot(pipe, watch_keys)
            return out
        if (
            (reservation is not None and not reservation_ok)
            or (session is not None and not session_ok)
            or not successor_ok
        ):
            await _validate_snapshot(pipe, watch_keys)
            out.conflict = True
            return out

        if session_ok:
            return await self._release_connected(
                pipe, pod, expected, successor, successor_field, advance, watch_keys, px
            )

        # 只有当前 auth+shard 仍属于同一实例时才改 projection;旧 UID ledger 可精确删,
        # 但绝不能触碰同名新实例的 player_count。
        auth_rec = None
        raw = await pipe.get(a_key)
        if raw is not None:
            auth_rec = hubpb.HubShardAuthStorageRecord()
            auth_rec.ParseFromString(bytes(raw))
        shard = None
        raw = await pipe.get(s_key)
        if raw is not None:
            shard = unmarshal_shard(pod, raw)
        if not _current_projection(auth_rec, shard, pod, expected):
            pipe.multi()
            advance(pipe)
            pipe.hdel(reservations_key(pod), expected.assignment_id)
            pipe.zrem(reservation_expiry_key(pod), expected.assignment_id)
            if successor_field:
                pipe.hdel(successors_key(pod), successor_field)
                pipe.zrem(successor_expiry_key(pod), successor_field)
            await pipe.execute()
            out.released = True
            return out

        ledger = await load_hub_capacity_ledger(pipe, pod, shard.capacity)
        prune_ledger(
            ledger,
            pod=pod,
            uid=expected.instance_uid,
            epoch=expected.protocol_epoch,
            writer=expected.writer_epoch,
            at_ms=now_ms(),
        )
        # target 可能刚被 expiry prune 从内存视图删除;底层精确记录仍是本次读到并匹配
        # 的那个,继续提交整个 ledger 才能真正清掉它并修正 projection。
        ledger.reservations.pop(expected.assignment_id, None)
        if successor_field:
            ledger.successors.pop(successor_field, None)
        sync_shard_capacity_projection(shard, ledger)
        payload = marshal_shard(shard)
        pipe.multi()
        advance(pipe)
        write_hub_capacity_ledger(pipe, pod, ledger)
        pipe.set(s_key, payload, px=px)
        await pipe.execute()
        out.released = True
        return out

    async def _release_connected(
        self,
        pipe: Any,
        pod: str,
        expected: AssignmentInstanceIdentity,
        successor,
        successor_field: str,
        advance,
        watch_keys: list[str],
        px: int,
    ) -> ReleaseAssignmentSeatResult:
        """匹配到的是**活的** connected owner —— 物理栅栏在这里。"""
        a_key, s_key = auth_key(pod), shard_key(pod)
        out = ReleaseAssignmentSeatResult()
        current_auth = None
        raw = await pipe.get(a_key)
        if raw is not None:
            current_auth = hubpb.HubShardAuthStorageRecord()
            current_auth.ParseFromString(bytes(raw))
        current_shard = None
        raw = await pipe.get(s_key)
        if raw is not None:
            current_shard = unmarshal_shard(pod, raw)
        # ★ 「已被完全激活的替换 UID/epoch」才是旧 GameServer 进程的权威 teardown 证明。
        #   心跳过期、Redis 键缺失、半初始化的替换实例都**不算**,仍需旧 DS 的 Logout ACK。
        superseded = (
            current_auth is not None
            and current_shard is not None
            and current_auth.HasField("active")
            and hub_auth_record_v2_exact(current_auth)
            and current_auth.pod_name == pod
            and current_shard.gameserver_uid == current_auth.instance_uid
            and current_shard.auth_epoch == current_auth.protocol_epoch
            and current_shard.last_verified_writer_epoch == current_auth.active.writer_epoch
            and (
                current_auth.instance_uid != expected.instance_uid
                or current_auth.protocol_epoch != expected.protocol_epoch
            )
        )
        teardown_confirmed = bool(
            await pipe.hexists(instance_teardown_proof_key(pod), expected.instance_uid)
        )
        if not superseded and not teardown_confirmed:
            # Release/Transfer 取消尚未消费的接力 capability,但**绝不删物理 owner**。
            # 若 Admission 并发胜出,WATCH 会重试,由调用方去驱逐那个 owner。
            if successor is not None:
                pipe.multi()
                advance(pipe)
                pipe.hdel(successors_key(pod), successor_field)
                pipe.zrem(successor_expiry_key(pod), successor_field)
                await pipe.execute()
            else:
                await _validate_snapshot(pipe, watch_keys)
            out.departure_required = True
            return out

        if teardown_confirmed and _current_projection(current_auth, current_shard, pod, expected):
            # 被 teardown 的 UID 仍是当前投影 → 在同一事务里修正派生容量。
            # 替换实例的投影绝不能用旧 UID 的账本重写。
            ledger = await load_hub_capacity_ledger(pipe, pod, current_shard.capacity)
            ledger.sessions.pop(expected.assignment_id, None)
            if successor_field:
                ledger.successors.pop(successor_field, None)
            sync_shard_capacity_projection(current_shard, ledger)
            payload = marshal_shard(current_shard)
            pipe.multi()
            advance(pipe)
            write_hub_capacity_ledger(pipe, pod, ledger)
            pipe.set(s_key, payload, px=px)
            await pipe.execute()
            out.released = True
            return out

        pipe.multi()
        advance(pipe)
        pipe.hdel(sessions_key(pod), expected.assignment_id)
        pipe.zrem(session_expiry_key(pod), expected.assignment_id)
        if successor_field:
            pipe.hdel(successors_key(pod), successor_field)
            pipe.zrem(successor_expiry_key(pod), successor_field)
        await pipe.execute()
        out.released = True
        return out

    # ── RemoveCapacityLedger ─────────────────────────────────────────────────

    async def remove_capacity_ledger(self, pod: str) -> None:
        """仅在分片已被确认回收后清派生账;活分片绝不调用。Go: RemoveCapacityLedger。"""
        await self._rdb.delete(*capacity_ledger_keys(pod))


# ── 事务内的纯函数(从 Go 的 Watch 回调里抽出来,便于单测直接打)──────────────


async def _validate_snapshot(pipe: Any, watch_keys: list[str]) -> None:
    """跑一次只读 MULTI/EXEC 验证 WATCH 快照。

    ★ 只 WATCH 不 EXEC 不具备快照一致性:WATCH 在 EXEC 之前没有任何线性化效果,
      两次读取之间发生的变化不会被发现。Go 的只读路径同样刻意跑一次空事务。
    """
    pipe.multi()
    pipe.exists(*watch_keys)
    await pipe.execute()


def _reservation_identity_of(expected: AssignmentInstanceIdentity) -> ReservationIdentity:
    return ReservationIdentity(
        player_id=expected.player_id,
        assignment_id=expected.assignment_id,
        instance_uid=expected.instance_uid,
        protocol_epoch=expected.protocol_epoch,
        writer_epoch=expected.writer_epoch,
        placement_version=expected.placement_version,
        placement_operation_id=expected.placement_operation_id,
        source_match_id=expected.source_match_id,
    )


def _new_reservation_record(
    pod: str, reservation: ReservationIdentity, at_ms: int
) -> hubpb.HubReservationStorageRecord:
    return hubpb.HubReservationStorageRecord(
        player_id=reservation.player_id,
        assignment_id=reservation.assignment_id,
        hub_pod_name=pod,
        hub_instance_uid=reservation.instance_uid,
        auth_epoch=reservation.protocol_epoch,
        auth_writer_epoch=reservation.writer_epoch,
        created_at_ms=at_ms,
        expires_at_ms=reservation.expires_at_ms,
        assignment_expires_at_ms=reservation.assignment_expires_at_ms,
    )


def _reserve_mutate(  # noqa: C901 —— 四条互斥分支,与 Go 逐条对应
    ledger: HubCapacityLedger,
    pod: str,
    reservation: ReservationIdentity,
    successor_field: str,
    at_ms: int,
    shard_capacity: int,
) -> str:
    """ReserveAssignment 的账本改写。返回 "" = 成功,否则是拒绝 reason。

    四条互斥分支(与 Go 同序):
      ① 已是 connected ownership → 只准备/刷新 successor 接力,**不重复计容**;
      ② 已是普通 reservation      → 刷新 bound;若同时有 successor 则收敛成 successor;
      ③ 只剩 successor            → 旧 owner 已 Departure,它就是那一个 reserved seat;
      ④ 全新                      → 先过容量闸再建 reservation。
    """
    (
        existing_field,
        existing_successor,
        successor_exists,
        successor_exact,
    ) = successor_matches(ledger, pod, reservation)

    session = ledger.sessions.get(reservation.assignment_id)
    if session is not None:
        if not session_record_matches(session, pod, reservation):
            return "assignment-session-conflict"
        if successor_exists and not successor_exact:
            try:
                previous = decode_successor_capability(existing_field, pod)
            except errcode.PandoraError:
                return "assignment-successor-conflict"
            if (
                not reservation_record_matches(existing_successor, pod, reservation)
                or reservation.placement_version == 0
                or reservation.placement_version <= previous.placement_version
            ):
                return "assignment-successor-conflict"
            # 同 assignment 的 Hub 迁移可能在首次选座之后才推进 canonical placement
            # operation。**只有严格更新的 version** 才能轮换 pending capability;
            # 同版本分叉与迟到调用方一律 fail-closed。
            rotated = _clone_reservation(existing_successor)
            del ledger.successors[existing_field]
            ledger.successors[successor_field] = rotated
            existing_field, existing_successor = successor_field, rotated
            successor_exists, successor_exact = True, True
        if not successor_exists:
            ledger.successors[successor_field] = _new_reservation_record(pod, reservation, at_ms)
        else:
            if reservation.expires_at_ms > existing_successor.expires_at_ms:
                existing_successor.expires_at_ms = reservation.expires_at_ms
            if reservation.assignment_expires_at_ms > existing_successor.assignment_expires_at_ms:
                existing_successor.assignment_expires_at_ms = reservation.assignment_expires_at_ms
            # canonical field 是确定性的;这道闸让未来的编码迁移 fail-closed,
            # 而不是悄悄写出第二条 capability。
            if existing_field != successor_field:
                return "assignment-successor-conflict"
        # 活 session 已经占着物理座位。successor 只是有界交接 lease,在 exact Departure
        # 删掉这个 owner 之前**不进入投影**。
        return ""

    current = ledger.reservations.get(reservation.assignment_id)
    if current is not None:
        if not reservation_record_matches(current, pod, reservation):
            return "assignment-reservation-conflict"
        if successor_exists and not successor_exact:
            return "assignment-successor-conflict"
        if reservation.expires_at_ms > current.expires_at_ms:
            current.expires_at_ms = reservation.expires_at_ms
        if reservation.assignment_expires_at_ms > current.assignment_expires_at_ms:
            current.assignment_expires_at_ms = reservation.assignment_expires_at_ms
        if successor_exists:
            # 混版本 allocator 可能在旧 Departure 之后重建了普通 reservation,而
            # successor 键还活着。把两条 exact lease 收敛回 placement 绑定的 successor。
            if current.expires_at_ms > existing_successor.expires_at_ms:
                existing_successor.expires_at_ms = current.expires_at_ms
            if current.assignment_expires_at_ms > existing_successor.assignment_expires_at_ms:
                existing_successor.assignment_expires_at_ms = current.assignment_expires_at_ms
            del ledger.reservations[reservation.assignment_id]
        return ""

    if successor_exists:
        if not successor_exact:
            try:
                previous = decode_successor_capability(existing_field, pod)
            except errcode.PandoraError:
                return "assignment-successor-conflict"
            if (
                not reservation_record_matches(existing_successor, pod, reservation)
                or reservation.placement_version == 0
                or reservation.placement_version <= previous.placement_version
            ):
                return "assignment-successor-conflict"
            rotated = _clone_reservation(existing_successor)
            del ledger.successors[existing_field]
            ledger.successors[successor_field] = rotated
            existing_field, existing_successor = successor_field, rotated
        # 旧 owner 已离场;这条 exact successor 现在就是那一个 reserved seat。
        # 重试 IssueDSTicket 只刷新它的 bound。
        if reservation.expires_at_ms > existing_successor.expires_at_ms:
            existing_successor.expires_at_ms = reservation.expires_at_ms
        if reservation.assignment_expires_at_ms > existing_successor.assignment_expires_at_ms:
            existing_successor.assignment_expires_at_ms = reservation.assignment_expires_at_ms
        return ""

    reserved, connected = ledger_counts(ledger, shard_capacity)
    if reserved + connected >= shard_capacity:
        return "shard-full"
    ledger.reservations[reservation.assignment_id] = _new_reservation_record(
        pod, reservation, at_ms
    )
    return ""


def _admission_mutate(
    ledger: HubCapacityLedger,
    pod: str,
    reservation: ReservationIdentity,
    admission_id: str,
    admission_seq: int,
    at_ms: int,
    out: AdmissionResult,
) -> str:
    """AcknowledgeAdmission 的账本改写。返回 "" / "conflict" / "rejected"。"""
    successor_field, successor, successor_exists, successor_exact = successor_matches(
        ledger, pod, reservation
    )
    current = ledger.sessions.get(reservation.assignment_id)
    if current is not None:
        if not session_record_matches(current, pod, reservation):
            return "rejected"
        if admission_seq < current.admission_seq:
            out.conflict = True
            return "conflict"
        if admission_seq == current.admission_seq and current.admission_id != admission_id:
            out.conflict = True
            return "conflict"
        if admission_seq == current.admission_seq:
            out.already_admitted = True  # 响应丢失:相同 seq+UUID 幂等重认。
        else:
            # ★ 新的物理 owner 必须消费 IssueDSTicket 备好的 exact successor。
            #   「看到一条活着的旧 session」本身**不是** admission capability:
            #   没有这条 lease 就 fail-closed 拒绝。
            if not successor_exists or not successor_exact or successor.expires_at_ms <= at_ms:
                return "rejected"
            del ledger.successors[successor_field]
            # 只有更大 seq 才允许重连替换 owner;迟到旧 ACK 永远不能反向夺回。
            current.admission_id = admission_id
            current.admission_seq = admission_seq
        current.last_seen_ms = at_ms
        current.expires_at_ms = 0
        return ""

    pending = ledger.reservations.get(reservation.assignment_id)
    pending_exact = pending is None or reservation_record_matches(pending, pod, reservation)
    normal_exact = pending is not None and pending_exact and pending.expires_at_ms > at_ms
    successor_usable = successor_exists and successor_exact and successor.expires_at_ms > at_ms
    if (
        not pending_exact
        or (successor_exists and not successor_exact)
        or (not normal_exact and not successor_usable)
    ):
        return "rejected"
    # 在一个事务里消费掉**全部** exact 表示。普通 lease 的重叠只为混版本上线容忍;
    # 过了这一点两种表示都不可能再被重放。
    if normal_exact:
        del ledger.reservations[reservation.assignment_id]
    if successor_usable:
        del ledger.successors[successor_field]
    ledger.sessions[reservation.assignment_id] = hubpb.HubConnectedOwnershipStorageRecord(
        player_id=reservation.player_id,
        assignment_id=reservation.assignment_id,
        admission_id=admission_id,
        hub_pod_name=pod,
        hub_instance_uid=reservation.instance_uid,
        auth_epoch=reservation.protocol_epoch,
        auth_writer_epoch=reservation.writer_epoch,
        admitted_at_ms=at_ms,
        last_seen_ms=at_ms,
        expires_at_ms=0,  # ★ 新格式恒 0 —— connected ownership 没有时间 TTL。
        admission_seq=admission_seq,
    )
    return ""


def _departure_mutate(
    ledger: HubCapacityLedger,
    pod: str,
    reservation: ReservationIdentity,
    admission_id: str,
    admission_seq: int,
    out: DepartureResult,
) -> None:
    """AcknowledgeDeparture 的账本改写。"""
    current = ledger.sessions.get(reservation.assignment_id)
    if current is None:
        # ★ 一条游离的 successor 证明这个 exact assignment 之前确实有过 owner 且其
        #   Departure 已经赢了。保留它,并把 exact 重试报成幂等成功。
        #   而一条普通 reservation 只说明该 assignment 从未完成 Admission —— 那是冲突。
        _, successor, has_successor = successor_for_assignment(ledger, reservation.assignment_id)
        if has_successor and not reservation_record_matches(successor, pod, reservation):
            out.conflict = True
            return
        pending = ledger.reservations.get(reservation.assignment_id)
        if pending is not None:
            if not reservation_record_matches(pending, pod, reservation) or not has_successor:
                out.conflict = True
            else:
                out.departed = True
                out.already_departed = True
        else:
            out.departed = True
            out.already_departed = True
        return
    if not session_record_matches(current, pod, reservation):
        out.conflict = True
        return
    if current.admission_seq != admission_seq or current.admission_id != admission_id:
        out.conflict = True
        return
    del ledger.sessions[reservation.assignment_id]
    # ★ 不删 exact successor:在同一个事务里,投影会把它从「零成本影子」重新解读成
    #   桥接「旧 Departure → 新 Admission」的那一个 reserved seat。
    out.departed = True


def _inspect_verdict(
    reservation, session, successor, successor_exists: bool, pod: str, identity: ReservationIdentity
) -> AssignmentSeatSnapshot:
    if reservation is None and session is None and not successor_exists:
        return AssignmentSeatSnapshot(already_absent=True)
    if session is not None:
        if (
            not session_record_matches(session, pod, identity)
            or session.admission_id == ""
            or session.admission_seq == 0
        ):
            return AssignmentSeatSnapshot(conflict=True)
        return AssignmentSeatSnapshot(
            connected=True,
            admission_id=session.admission_id,
            admission_seq=session.admission_seq,
        )
    if (reservation is not None and not reservation_record_matches(reservation, pod, identity)) or (
        successor_exists and not reservation_record_matches(successor, pod, identity)
    ):
        return AssignmentSeatSnapshot(conflict=True)
    expires_at = reservation.expires_at_ms if reservation is not None else 0
    if successor_exists and successor.expires_at_ms > expires_at:
        expires_at = successor.expires_at_ms
    return AssignmentSeatSnapshot(reserved=True, reservation_expires_at_ms=expires_at)


def _current_projection(auth_rec, shard, pod: str, expected: AssignmentInstanceIdentity) -> bool:
    """当前 auth+shard 是否仍精确投影 expected 这个实例。Go 里的 currentProjection。"""
    return (
        auth_rec is not None
        and shard is not None
        and auth_rec.HasField("active")
        and hub_auth_record_v2_exact(auth_rec)
        and auth_rec.pod_name == pod
        and auth_rec.instance_uid == expected.instance_uid
        and auth_rec.protocol_epoch == expected.protocol_epoch
        and auth_rec.active.writer_epoch == expected.writer_epoch
        and shard.gameserver_uid == expected.instance_uid
        and shard.auth_epoch == expected.protocol_epoch
        and shard.last_verified_writer_epoch == expected.writer_epoch
    )
