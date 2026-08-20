"""hub_allocator → owner 权威的 gRPC 客户端 —— 对应 Go 侧
`services/battle/hub_allocator/internal/data/owner_lease_client.go`。

这一层只做**搬运**:把 biz 的 View 结构翻成 proto、把 proto 翻回 View,
不做任何归属判定(判定全在 owner 服务的线性一致事务里,CLAUDE.md §9 不变量 22)。

## 四个方法各自守的东西

    renew_instance_lease   实例租约续期。挂在 **DS 心跳响应返回之前**,
                           所以超时必须远小于心跳周期(5s),否则续租把心跳拖死,
                           DS 反而因为"心跳超时"被判掉线 —— 续租机制把自己搞死。
    query_owner            query-first:拿当前 epoch 作为 CAS 期望值。
    begin_transition       归属推进(PENDING)。
    admit                  再入屏障后的 exact CAS(PENDING → ADMITTED)。

## 内网 insecure、不带 JWT

与 Go 的 `grpcclient.MustDialInsecure` 一致。owner 服务对**带玩家 JWT** 的调用
是直接拒的(它认的是内网系统调用),所以这里不能顺手复用带鉴权的 channel。

## Go 的 `(value, err)` 在 Python 怎么表达

Go 侧有两处刻意"**出错也要把值带回去**":

  - `BeginTransition` 非 OK 时仍返回 `recordView(resp.GetRecord())` ——
    EPOCH_CONFLICT 时那是**权威当前记录**,调用方要靠它决定是否重试;
  - `Admit` 非 OK 时仍返回 `resp.GetRetryAfterMs()` ——
    >0 表示"再入屏障还没开",是**预期中的 WAIT**,不能计入失败告警。

Python 不能一边 raise 一边 return,所以把这两个值挂在异常上
(`errcode.PandoraError` 的 `current_record` / `retry_after_ms` 声明槽位)。
★ 不要改成"失败返回 None 不抛" —— 那样调用方少写一个 if 就把归属写失败
  当成写成功了,而这条路径的失败必须 fail-closed。

## 与 Go 的已知差异

  - Go 的 uint64/uint32 由类型系统兜底,Python 的 int 无界。这里在**发送前**
    显式做范围校验并抛 ErrInvalidArg;不校验的话越界值会在 protobuf 序列化时
    抛 ValueError,那是一个既不带业务码、也说不清哪个字段的异常。
  - 传输层异常(`grpc.aio.AioRpcError`)**原样上抛**,与 Go 直接返回 grpc err
    一致 —— 两侧 `errcode.as_code` / `errcode.As` 都会归到 ErrUnknown。
"""

from __future__ import annotations

import dataclasses
from typing import Any

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.owner.v1 import owner_pb2 as ownerpb
from pandora.owner.v1 import owner_pb2_grpc as owner_grpc

from pandorapy import errcode, placement
from pandorapy import source_revision as srev

# 单次 owner RPC 超时。★ 必须远小于 DS 心跳周期(5s):续租是挂在心跳响应
# 返回之前的同步调用,超时取大了会把心跳本身拖过 heartbeat_timeout,
# 于是"为了保住租约"反而把 DS 判成掉线。与 Go 的 ownerLeaseRPCTimeout 同值。
OWNER_LEASE_RPC_TIMEOUT_SEC = 2.0

_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1


def _check_uint(name: str, value: int, limit: int) -> int:
    """发送前的无符号范围校验(Go 由类型系统免费获得,Python 必须显式做)。"""
    if not isinstance(value, int) or isinstance(value, bool):
        raise errcode.PandoraError(errcode.ErrInvalidArg, "owner %s must be int, got %r", name, value)
    if value < 0 or value > limit:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "owner %s out of range: %d", name, value
        )
    return value


@dataclasses.dataclass(frozen=True, slots=True)
class OwnerTargetView:
    """biz 视角的归属目标。对应 Go 的 data.OwnerTargetView。

    ★ frozen + slots 让 `==` 变成**逐字段**比较,这正是 census 自愈路径
      `current != tgt` 依赖的语义:任何一格变了都算"归属已经不是刚才那个了"。
    """

    pod_name: str = ""
    instance_uid: str = ""
    instance_epoch: int = 0
    assignment_or_allocation_id: str = ""
    release_track: str = ""
    # 本次归属的来源版本(INC-20260818-003)。刻意**不放进 proto 的 OwnerTarget**:
    # Admit 对 target 做的是 exact 相等判定,把一个会变的版本号塞进去会让
    # "同一归属的重复 Admit"变成 identity mismatch。
    #
    # ★ 取值 0 是 `source_revision.LEGACY`,含义是"**没有**版本",与任何非零
    #   **不可比**(不是"最小值")。所以判据一律写成 `!= srev.LEGACY`,
    #   绝不能写成 `> 0` —— 后者读起来像"有没有设置",会诱导出
    #   "没设置就放行"的分支,那正是 legacy 记录被当成最旧版本放行的入口。
    #   本层不做判定(判定在 owner 侧 classify),只负责原样搬运。
    source_revision: int = srev.LEGACY


@dataclasses.dataclass(frozen=True, slots=True)
class OwnerRecordView:
    """biz 视角的归属记录快照。对应 Go 的 data.OwnerRecordView。"""

    owner_epoch: int = 0
    owner_type: int = 0
    phase: int = 0
    pod_name: str = ""
    instance_uid: str = ""
    instance_epoch: int = 0
    assignment_or_allocation_id: str = ""
    release_track: str = ""
    operation_id: str = ""
    admit_not_before_ms: int = 0


def target_proto(t: OwnerTargetView) -> ownerpb.OwnerTarget:
    """View → proto。★ 不含 source_revision(见 OwnerTargetView 的注释)。"""
    return ownerpb.OwnerTarget(
        pod_name=t.pod_name,
        instance_uid=t.instance_uid,
        instance_epoch=_check_uint("instance_epoch", t.instance_epoch, _UINT32_MAX),
        assignment_or_allocation_id=t.assignment_or_allocation_id,
        release_track=t.release_track,
    )


def record_view(r: ownerpb.OwnerRecord | None) -> OwnerRecordView:
    """proto → View。对齐 Go 的 `recordView` + GetX() 的 nil-safe 语义。"""
    if r is None:
        return OwnerRecordView()
    return OwnerRecordView(
        owner_epoch=int(r.owner_epoch),
        owner_type=int(r.owner_type),
        phase=int(r.phase),
        pod_name=r.target.pod_name,
        instance_uid=r.target.instance_uid,
        instance_epoch=int(r.target.instance_epoch),
        assignment_or_allocation_id=r.target.assignment_or_allocation_id,
        release_track=r.target.release_track,
        operation_id=r.operation_id,
        admit_not_before_ms=int(r.admit_not_before_ms),
    )


class GrpcOwnerLeaseRenewer:
    """owner 权威的 gRPC 客户端。对应 Go 的 data.GrpcOwnerLeaseRenewer。

    同时实现 biz 侧的两个 Protocol(OwnerLeaseRenewer / OwnerAuthority)——
    Go 也是同一个 struct 兼任两个接口,拆开只会多一条连接。
    """

    __slots__ = ("_channel", "_stub", "_timeout_sec", "_owns_channel")

    def __init__(
        self,
        owner_addr: str = "",
        *,
        channel: Any = None,
        timeout_sec: float = OWNER_LEASE_RPC_TIMEOUT_SEC,
    ) -> None:
        if channel is not None:
            # 注入的 channel 由调用方持有,close() 不碰它(测试与多客户端共享连接)。
            self._channel = channel
            self._owns_channel = False
        else:
            # insecure:内网直连、不带 JWT,与 Go 的 grpcclient.MustDialInsecure 一致。
            self._channel = grpc.aio.insecure_channel(owner_addr)
            self._owns_channel = True
        self._stub = owner_grpc.OwnerServiceStub(self._channel)
        self._timeout_sec = timeout_sec

    async def close(self) -> None:
        if self._owns_channel:
            await self._channel.close()

    # ── 实例租约 ─────────────────────────────────────────────────────────────

    async def renew_instance_lease(
        self, pod_name: str, instance_uid: str, instance_epoch: int, release_track: str
    ) -> None:
        """续期实例租约。失败抛异常,由 biz 的 renew_owner_lease_gate 决定强/弱依赖。"""
        resp = await self._stub.RenewInstanceLease(
            ownerpb.RenewInstanceLeaseRequest(
                target=ownerpb.OwnerTarget(
                    pod_name=pod_name,
                    instance_uid=instance_uid,
                    instance_epoch=_check_uint("instance_epoch", instance_epoch, _UINT32_MAX),
                    release_track=release_track,
                ),
                # 租约长度由**本侧**声明、owner 侧再钳一次:两边同源于 placement,
                # 谁也不能单方面把 DS 自我 fencing 的窗口拉长。
                lease_seconds=placement.DS_FENCE_LEASE_MAX_SECONDS,
            ),
            timeout=self._timeout_sec,
        )
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                int(resp.code),
                "owner renew lease rejected pod=%s uid=%s",
                pod_name,
                instance_uid,
            )

    # ── 归属权威 ─────────────────────────────────────────────────────────────

    async def query_owner(self, player_id: int) -> OwnerRecordView:
        """query-first。★ 失败必须上抛:把"查不到"当成"没有归属"就是 §9.22
        明令禁止的"冒充 OFFLINE",会直接导致第二个 DS 被放进来。"""
        resp = await self._stub.QueryOwner(
            ownerpb.QueryOwnerRequest(
                player_id=_check_uint("player_id", player_id, _UINT64_MAX)
            ),
            timeout=self._timeout_sec,
        )
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                int(resp.code), "owner query rejected player=%d", player_id
            )
        return record_view(resp.record)

    async def begin_transition(
        self,
        player_id: int,
        expect_epoch: int,
        operation_id: str,
        owner_type: int,
        target: OwnerTargetView,
    ) -> OwnerRecordView:
        """推进归属到 PENDING。

        非 OK 时把**权威当前记录**挂在异常的 `current_record` 上:
        EPOCH_CONFLICT 的调用方要靠它判断"是谁抢走了",不能只拿到一个错误码。
        """
        resp = await self._stub.BeginTransition(
            ownerpb.BeginTransitionRequest(
                player_id=_check_uint("player_id", player_id, _UINT64_MAX),
                expect_epoch=_check_uint("expect_epoch", expect_epoch, _UINT64_MAX),
                operation_id=operation_id,
                owner_type=owner_type,
                target=target_proto(target),
                # 来源版本单独发,不进 Target(见 OwnerTargetView.source_revision)。
                source_revision=_check_uint(
                    "source_revision", target.source_revision, _UINT64_MAX
                ),
            ),
            timeout=self._timeout_sec,
        )
        view = record_view(resp.record)
        if resp.code != errcode_pb2.OK:
            err = errcode.PandoraError(
                int(resp.code), "owner begin rejected player=%d", player_id
            )
            err.current_record = view
            raise err
        return view

    async def admit(
        self,
        player_id: int,
        owner_epoch: int,
        operation_id: str,
        target: OwnerTargetView,
    ) -> int:
        """再入屏障后的 exact CAS。成功返回 0。

        非 OK 时把 `retry_after_ms` 挂在异常上:>0 表示**屏障还没到点**,
        那是预期中的 WAIT(旧 DS 的租约还没过安全截止),调用方应静默退避,
        绝不能计进"权威写失败"的告警 —— 否则每次正常迁移都会刷一片假告警。
        """
        resp = await self._stub.Admit(
            ownerpb.AdmitRequest(
                player_id=_check_uint("player_id", player_id, _UINT64_MAX),
                owner_epoch=_check_uint("owner_epoch", owner_epoch, _UINT64_MAX),
                operation_id=operation_id,
                target=target_proto(target),
            ),
            timeout=self._timeout_sec,
        )
        if resp.code != errcode_pb2.OK:
            err = errcode.PandoraError(
                int(resp.code), "owner admit rejected player=%d", player_id
            )
            err.retry_after_ms = int(resp.retry_after_ms)
            raise err
        return 0
