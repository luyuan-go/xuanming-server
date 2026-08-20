"""owner 权威 gRPC service 层(pandora.owner.v1)—— 对应 Go 侧 internal/service/owner.go。

鉴权边界与 Go 侧完全一致,而且方向和别的服务**相反**:

    别的服务:没有玩家身份 → 拒
    owner  :**有**玩家身份 → 拒

全部 RPC 是内部系统接口(调用方 = login / allocator / DS 回调链等内网服务),
带玩家 JWT 的客户端调用一律拒。Envoy 侧对 /pandora.owner.v1/ 前缀另有 403 拦截,
这里是双保险的第二道 —— 因为 §9.22 的 owner 记录一旦能被玩家自己改,
"一人一 DS"就从机制保证退化成了网关配置的一句话。

★ 因此本服务必须用 AuthOptional(auth_required=False)构造 server:
  内部调用方不带 x-pandora-player-id,用 AuthRequired 会把它们全挡在门外。
"""

from __future__ import annotations

import asyncio

from pandora.common.v1 import errcode_pb2 as commonpb
from pandora.owner.v1 import owner_pb2 as opb
from pandora.owner.v1 import owner_pb2_grpc as ogrpc

from pandorapy import errcode
from pandorapy import interceptors as pintercept
from pandorapy import log as plog
from pandorapy.services.owner import data as odata

# ── proto ↔ 内部结构 ─────────────────────────────────────────────────────


def to_proto_target(t: odata.OwnerTarget) -> opb.OwnerTarget:
    return opb.OwnerTarget(
        pod_name=t.pod_name,
        instance_uid=t.instance_uid,
        instance_epoch=t.instance_epoch,
        assignment_or_allocation_id=t.assignment_or_allocation_id,
        release_track=t.release_track,
    )


def from_proto_target(t: opb.OwnerTarget) -> odata.OwnerTarget:
    return odata.OwnerTarget(
        pod_name=t.pod_name,
        instance_uid=t.instance_uid,
        instance_epoch=t.instance_epoch,
        assignment_or_allocation_id=t.assignment_or_allocation_id,
        release_track=t.release_track,
    )


def to_proto_record(rec: odata.OwnerRecord) -> opb.OwnerRecord:
    return opb.OwnerRecord(
        player_id=rec.player_id,
        owner_epoch=rec.owner_epoch,
        owner_type=rec.owner_type,
        phase=rec.phase,
        target=to_proto_target(rec.target),
        operation_id=rec.operation_id,
        admit_not_before_ms=rec.admit_not_before_ms,
        lease_deadline_ms=rec.lease_deadline_ms,
        updated_at_ms=rec.updated_at_ms,
        # 高水位下发给调用方(INC-20260818-003):allocator 据此判断本部署是否已进入
        # 带版本阶段,也让排障时不必开库就能看到门的位置。
        hub_source_revision=rec.hub_source_revision,
    )


def _code_of(exc: BaseException) -> int:
    """把内部异常映射成 in-band ErrCode(与 Go 的 errcode.As 同口径)。"""
    if isinstance(exc, errcode.PandoraError):
        return exc.code
    return errcode.ErrInternal


class OwnerService(ogrpc.OwnerServiceServicer):
    """实现 pandora.owner.v1.OwnerService 的五个 RPC。

    业务失败一律走 **in-band code**(返回 response.code,gRPC status 保持 OK),
    与 Go 侧逐字一致 —— 调用方按 code 分支,不靠 gRPC status 猜。
    """

    def __init__(self, uc) -> None:  # noqa: ANN001 —— biz.OwnerUsecase
        self._uc = uc

    # ── 系统接口守卫 ──────────────────────────────────────────────────────

    @staticmethod
    def _reject_client_caller(context) -> bool:  # noqa: ANN001
        """带玩家 JWT 的调用(caller_id > 0)一律拒 —— 对应 Go 的 rejectClientCaller。"""
        return pintercept.extract_player_id(context) != 0

    @staticmethod
    def _log_client_caller_denied(context, rpc: str, req_player_id: int) -> None:  # noqa: ANN001
        """记录一次玩家 JWT 直敲 owner 内部接口的拒绝。

        ERR_PERMISSION_DENY 是 in-band Code,handler 不抛错 → access log 只记 DEBUG。
        稳态下本日志应恒 0(Envoy 已在前置 403);一旦出现就是「Envoy 拦截漏了」或
        「内部调用方错把玩家上下文透传进来了」,必须可见。
        """
        plog.get().warning(
            "owner_caller_denied",
            rpc=rpc,
            reason="client_caller_on_internal_rpc",
            caller_player_id=pintercept.extract_player_id(context),
            player_id=req_player_id,
        )

    # ── RPC ───────────────────────────────────────────────────────────────

    async def QueryOwner(self, request, context):  # noqa: N802
        """读当前 owner 记录。"""
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "QueryOwner", request.player_id)
            return opb.QueryOwnerResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            rec = await self._uc.query(request.player_id)
        except asyncio.CancelledError:
            # ★ 取消必须穿透。CancelledError 在 3.8+ 是 BaseException，会被下面那条
            # 宽 except 吞掉 —— 而 grpc.aio 正是**用取消**终止在途 handler。
            # 吞掉的后果:优雅停机时把取消映射成业务错误码并返回正常响应,
            # 客户端每次滚动更新都收到一批假的业务失败,而在途请求也没有真的排空。
            raise
        except BaseException as exc:  # noqa: BLE001 —— 与 Go 同:全部转 in-band code
            return opb.QueryOwnerResponse(code=_code_of(exc))
        # 高频只读路径 → DEBUG(§11.3 R4)。开 debug 后它直接回答「这玩家当前归谁、
        # epoch 多少、哪个 operation 推的」—— 与下游的 UNKNOWN 退避日志对账用。
        plog.get().debug(
            "owner_queried",
            player_id=request.player_id,
            owner_epoch=rec.owner_epoch,
            owner_type=rec.owner_type,
            phase=rec.phase,
            cur_pod=rec.target.pod_name,
            cur_instance_uid=rec.target.instance_uid,
            cur_instance_epoch=rec.target.instance_epoch,
            assignment_id=rec.target.assignment_or_allocation_id,
            release_track=rec.target.release_track,
            operation_id=rec.operation_id,
            admit_not_before_ms=rec.admit_not_before_ms,
            lease_deadline_ms=rec.lease_deadline_ms,
            updated_at_ms=rec.updated_at_ms,
        )
        return opb.QueryOwnerResponse(code=commonpb.OK, record=to_proto_record(rec))

    async def BeginTransition(self, request, context):  # noqa: N802
        """发起 owner 迁移。

        ★ EPOCH_CONFLICT 时响应**仍携带当前记录** —— 这是 §9.23 query-first 重查的
        依据:调用方拿到冲突就知道现在的真实 epoch/target,不必再多打一次 QueryOwner
        (多那一次就多一个 TOCTOU 窗口)。
        """
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "BeginTransition", request.player_id)
            return opb.BeginTransitionResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            rec = await self._uc.begin_transition(
                request.player_id,
                request.expect_epoch,
                request.operation_id,
                int(request.owner_type),
                from_proto_target(request.target),
                request.source_revision,
            )
        except errcode.PandoraError as exc:
            resp = opb.BeginTransitionResponse(code=exc.code)
            if exc.code == errcode.ErrOwnerEpochConflict and exc.current_record is not None:
                resp.record.CopyFrom(to_proto_record(exc.current_record))
            return resp
        except asyncio.CancelledError:
            # ★ 取消必须穿透。CancelledError 在 3.8+ 是 BaseException，会被下面那条
            # 宽 except 吞掉 —— 而 grpc.aio 正是**用取消**终止在途 handler。
            # 吞掉的后果:优雅停机时把取消映射成业务错误码并返回正常响应,
            # 客户端每次滚动更新都收到一批假的业务失败,而在途请求也没有真的排空。
            raise
        except BaseException as exc:  # noqa: BLE001
            return opb.BeginTransitionResponse(code=_code_of(exc))
        return opb.BeginTransitionResponse(code=commonpb.OK, record=to_proto_record(rec))

    async def Admit(self, request, context):  # noqa: N802
        """准入提交。

        ★ BARRIER_NOT_OPEN 必须带 retry_after_ms —— §9.23「脑裂时安全优先但不能永久
        卡流程」:调用方据此定时重查,而不是等一个可能永不到达的旧 DS 回调。
        已 ADMITTED 的重放幂等返回同一结果(回包丢失时不能再分配第二个 owner)。
        """
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "Admit", request.player_id)
            return opb.AdmitResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            rec, retry_after = await self._uc.admit(
                request.player_id,
                request.owner_epoch,
                request.operation_id,
                from_proto_target(request.target),
            )
        except errcode.PandoraError as exc:
            resp = opb.AdmitResponse(code=exc.code, retry_after_ms=exc.retry_after_ms)
            if exc.code == errcode.ErrOwnerBarrierNotOpen and exc.current_record is not None:
                resp.record.CopyFrom(to_proto_record(exc.current_record))
            return resp
        except asyncio.CancelledError:
            # ★ 取消必须穿透。CancelledError 在 3.8+ 是 BaseException，会被下面那条
            # 宽 except 吞掉 —— 而 grpc.aio 正是**用取消**终止在途 handler。
            # 吞掉的后果:优雅停机时把取消映射成业务错误码并返回正常响应,
            # 客户端每次滚动更新都收到一批假的业务失败,而在途请求也没有真的排空。
            raise
        except BaseException as exc:  # noqa: BLE001
            return opb.AdmitResponse(code=_code_of(exc))
        return opb.AdmitResponse(code=commonpb.OK, record=to_proto_record(rec))

    async def RenewInstanceLease(self, request, context):  # noqa: N802
        """实例租约续期。"""
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "RenewInstanceLease", 0)
            return opb.RenewInstanceLeaseResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            deadline = await self._uc.renew_instance_lease(
                from_proto_target(request.target), request.lease_seconds
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透。CancelledError 在 3.8+ 是 BaseException，会被下面那条
            # 宽 except 吞掉 —— 而 grpc.aio 正是**用取消**终止在途 handler。
            # 吞掉的后果:优雅停机时把取消映射成业务错误码并返回正常响应,
            # 客户端每次滚动更新都收到一批假的业务失败,而在途请求也没有真的排空。
            raise
        except BaseException as exc:  # noqa: BLE001
            return opb.RenewInstanceLeaseResponse(code=_code_of(exc))
        return opb.RenewInstanceLeaseResponse(code=commonpb.OK, lease_deadline_ms=deadline)

    async def ReleaseOwner(self, request, context):  # noqa: N802
        """显式释放(迟到调用幂等 no-op 返回当前记录)。"""
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "ReleaseOwner", request.player_id)
            return opb.ReleaseOwnerResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            rec = await self._uc.release(
                request.player_id, request.owner_epoch, request.operation_id
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透。CancelledError 在 3.8+ 是 BaseException，会被下面那条
            # 宽 except 吞掉 —— 而 grpc.aio 正是**用取消**终止在途 handler。
            # 吞掉的后果:优雅停机时把取消映射成业务错误码并返回正常响应,
            # 客户端每次滚动更新都收到一批假的业务失败,而在途请求也没有真的排空。
            raise
        except BaseException as exc:  # noqa: BLE001
            return opb.ReleaseOwnerResponse(code=_code_of(exc))
        return opb.ReleaseOwnerResponse(code=commonpb.OK, record=to_proto_record(rec))
