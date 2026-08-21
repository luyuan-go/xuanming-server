"""player_locator gRPC service 层 —— 对应 Go 侧 internal/service/locator.go。

职责:
  - 实现 PlayerLocatorServiceServicer(16 个 RPC:9 个在用 + 7 个已下线 stub)
  - proto Location / LocationState ↔ usecase 入参/出参互转
  - errcode → pandora.common.v1.ErrCode 1:1 映射(**不抛 gRPC 错误**)

⚠️ 返回值形态必须与 Go 一致:业务失败返回 `Response(code=ErrXxx)` 且 gRPC status
   为 OK,客户端读 body 里的 code。改成 context.abort() 会让调用方走到完全不同的
   错误分支 —— 这是最容易在迁移中悄悄改掉的语义。

★ 身份来源:**取请求体里的 player_id,不从 JWT override**。
  与 data_service 同档,理由也相同:locator 是内网服务/DS 面接口(Go 的
  NewGRPCServer 没挂 AuthRequired),调用方是 hub_allocator / login / matchmaker /
  Hub DS —— 它们**代表别的玩家**写位置,自己并不持有那个玩家的 JWT。
  照搬客户端面服务的 extract_player_id 会拿到恒 0,每个 RPC 都 ERR_UNAUTHORIZED,
  整条 presence 链直接断掉。调用方限制由 Envoy 的内网路由 + DS 回调令牌承担。

★ 7 个 placement RPC 是 Go 侧**刻意下线的 stub**(候选 B placement/proof 系统
  2026-07 硬切下线),统一返回 ERR_SERVICE_DISABLED,无业务逻辑。
  这里照抄同一个码 —— "顺手实现"会让已下线的路由权威重新活过来。
"""

from __future__ import annotations

import asyncio
from typing import Any

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.locator.v1 import locator_pb2, locator_pb2_grpc

from pandorapy import auth as pauth
from pandorapy import dsauth
from pandorapy import errcode
from pandorapy import log as plog
from pandorapy.services.player_locator import biz as lbiz
from pandorapy.services.player_locator import hub_credential as lhubcred
from pandorapy.services.player_locator import usecase as lusecase

# DS 回调面拒绝 reason 枚举(§11.3 R2)。取值与 Go 的 reason* 常量逐字节一致。
REASON_DS_GUARD_REJECTED = "ds_callback_guard_rejected"
REASON_HUB_CREDENTIAL_INACTIVE = "hub_credential_not_active"


def _to_proto_code(err: BaseException) -> int:
    """errcode → proto enum,1:1 数值映射。对应 Go 的 toProtoCode。"""
    return errcode.as_code(err)


def _log_callback_rejected(
    rpc: str, reason: str, player_id: int, hub_pod: str, err: str, **extra: Any
) -> None:
    """记录一次 DS 回调面的**副作用前**拒绝 —— 对应 Go 的 logCallbackRejected。

    这些拒绝返回 in-band Code(ErrUnauthorized / ErrPermissionDeny / ErrUnavailable),
    handler 自身正常返回 → access log 记 rpc_ok(DEBUG)。线上默认 info 级下,
    「Hub DS 的位置上报被门禁挡掉了」这件事**一条日志都没有**,现象只剩下
    「玩家在大厅里但 locator 查不到他」。

    ⚠️ 本面不会自动带 player_id(DS 走 AuthOptional,不带 x-pandora-player-id),
    所以 player_id / hub_pod 必须手写,否则定位不到人。
    """
    plog.get().warning(
        "locator_ds_callback_rejected",
        rpc=rpc,
        reason=reason,
        player_id=player_id,
        hub_pod=hub_pod,
        err=err,
        **extra,
    )


def _guard_reject_err(code: int) -> str:
    """守卫拒绝的 `err` 字段文本。

    Go 的 `CheckHubCredential` 走 error 通道,拒绝理由有文本;Python 的
    `DSCallbackGuard.check_credential` 只回 in-band code(**刻意如此**,见
    dsauth.py 里那段长注释:改成抛异常会把 in-band code 契约当场打破)。
    真正的拒绝理由已由守卫内部的 `ds_callback_auth_rejected` 打出并带 reason,
    这里只补一个可对齐的占位,不假装自己知道细节。
    """
    return f"ds callback guard rejected: errcode={code}"


def _fence_from_proto(f: locator_pb2.HubPresenceFence) -> lbiz.HubPresenceFence:
    return lbiz.HubPresenceFence(
        assignment_id=f.assignment_id,
        admission_id=f.admission_id,
        admission_seq=f.admission_seq,
    )


def _location_to_proto(out: lusecase.LocationOutput) -> locator_pb2.Location:
    return locator_pb2.Location(
        state=out.state,
        hub_pod=out.hub_pod,
        shard_id=out.shard_id,
        match_id=out.match_id,
        battle_pod=out.battle_pod,
        updated_at_ms=out.updated_at_ms,
    )


class LocatorService(locator_pb2_grpc.PlayerLocatorServiceServicer):
    """实现 PlayerLocatorServiceServicer。对应 Go 的 service.LocatorService。"""

    __slots__ = ("_uc", "_ds_guard", "_hub_credential_checker")

    def __init__(self, usecase: lusecase.LocatorUsecase) -> None:
        self._uc = usecase
        # None = mode=off(未配置服务零改动),与 Go 的 nil dsGuard 同义:
        # Go 的 `(*DSCallbackGuard)(nil).CheckWithClaims` 直接放行。
        self._ds_guard: dsauth.DSCallbackGuard | None = None
        # 仅在 ds_auth.authority_mode=redis + enforce 时注入;
        # None 表示 legacy/off/permissive,不改变既有行为。
        self._hub_credential_checker: lhubcred.HubCredentialStateChecker | None = None

    def set_ds_callback_guard(self, guard: dsauth.DSCallbackGuard | None) -> None:
        """注入 DS 回调令牌守卫(main 按 ds_auth 配置构建;None 表示 off)。"""
        self._ds_guard = guard

    def set_hub_credential_state_checker(
        self, checker: lhubcred.HubCredentialStateChecker | None
    ) -> None:
        """注入 Model B Redis active credential 终态门。"""
        self._hub_credential_checker = checker

    def _check_ds_callback(
        self, context: grpc.aio.ServicerContext, scope: dsauth.DSScope
    ) -> tuple[dsauth.VerifiedCredential | None, int]:
        """守卫校验 —— 对应 Go 的 `s.dsGuard.CheckHubCredential(ctx, scope)`。

        守卫未注入(mode=off)时返回 `(None, 0)`:与 Go 的 nil 接收者放行同义。
        """
        if self._ds_guard is None:
            return None, 0
        _claims, cred, code = self._ds_guard.check_credential(context, scope)
        return cred, code

    # ── 位置写入 ──────────────────────────────────────────────────────────

    async def SetLocation(  # noqa: N802 —— gRPC 生成的方法名,必须照抄
        self,
        request: locator_pb2.SetLocationRequest,
        context: grpc.aio.ServicerContext,
    ) -> locator_pb2.SetLocationResponse:
        """写入玩家位置。守卫拒绝 → in-band 鉴权码;业务守卫拒绝 → ErrLocatorConflict。

        DS 回调范围绑定:Hub DS 只能写 HUB 状态且 pod 必须与令牌 sub 一致;
        其余状态(MATCHING/BATTLE/OFFLINE 等)只允许内部服务写
        (matchmaker / ds_allocator / login),来自 DS 网关或带 DS 令牌的请求一律拒
        (deny_ds)。

        全仓确认:写 HUB 状态的唯一合法调用者是 Hub DS(经回调令牌),无任何内部 Go
        服务写 HUB(login→LOGIN_PENDING、matchmaker→MATCHING/BATTLE、
        ds_allocator→BATTLE),故 HUB 分支置 require_token:enforce 下无令牌直连
        (绕过 Envoy)一律拒(fail-closed,审核 P1)。
        """
        loc = request.location
        scope = dsauth.DSScope(deny_ds=True)
        if loc.state == lbiz.LOCATION_STATE_HUB:
            scope = dsauth.DSScope(
                ds_type=pauth.DS_TYPE_HUB, pod=loc.hub_pod, require_token=True
            )
        cred, code = self._check_ds_callback(context, scope)
        if code != 0:
            _log_callback_rejected(
                "SetLocation",
                REASON_DS_GUARD_REJECTED,
                request.player_id,
                loc.hub_pod,
                _guard_reject_err(code),
                presence_state=int(loc.state),
                deny_ds=scope.deny_ds,
                require_token=scope.require_token,
            )
            return locator_pb2.SetLocationResponse(code=code)
        if (
            loc.state == lbiz.LOCATION_STATE_HUB
            and self._hub_credential_checker is not None
        ):
            chk = await self._hub_credential_checker.check_active(loc.hub_pod, cred)
            if chk.code != 0:
                _log_callback_rejected(
                    "SetLocation",
                    REASON_HUB_CREDENTIAL_INACTIVE,
                    request.player_id,
                    loc.hub_pod,
                    chk.err,
                    presence_state=int(loc.state),
                )
                return locator_pb2.SetLocationResponse(code=chk.code)
        # §11.3 R3:match_id 在这里第一次被解析出来,写进日志上下文让本请求后续所有
        # 日志(守卫拒绝 / 状态迁移 / CAS 耗尽)自动带上,无需层层传参。
        inp = lbiz.LocationInput(
            player_id=request.player_id,
            state=loc.state,
            hub_pod=loc.hub_pod,
            shard_id=loc.shard_id,
            match_id=loc.match_id,
            battle_pod=loc.battle_pod,
            hub_presence_fence=_fence_from_proto(request.hub_presence_fence),
        )
        try:
            await self._uc.set_location(inp)
        except asyncio.CancelledError:
            # ★ 必须先放行:CancelledError 在 3.8+ 是 BaseException,被下面的宽 except
            # 吞掉的话,优雅停机时会把「取消」映射成 in-band 业务码并返回**正常响应**
            # —— 客户端每次滚动更新都收到一批假失败,而排空在途也没有真的发生。
            raise
        except BaseException as exc:  # noqa: BLE001 —— 与 Go 一样把 error 映射成 code
            return locator_pb2.SetLocationResponse(code=_to_proto_code(exc))
        return locator_pb2.SetLocationResponse(code=errcode_pb2.OK)

    async def ClearLocation(  # noqa: N802
        self,
        request: locator_pb2.ClearLocationRequest,
        context: grpc.aio.ServicerContext,
    ) -> locator_pb2.ClearLocationResponse:
        try:
            await self._uc.clear_location(request.player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return locator_pb2.ClearLocationResponse(code=_to_proto_code(exc))
        return locator_pb2.ClearLocationResponse(code=errcode_pb2.OK)

    # ── 查询面(内部只读,不加 DS 守卫)────────────────────────────────────

    async def GetLocation(  # noqa: N802
        self,
        request: locator_pb2.GetLocationRequest,
        context: grpc.aio.ServicerContext,
    ) -> locator_pb2.GetLocationResponse:
        """key miss 返回 OFFLINE 占位(不报错)。

        ★ 调用方注意(§9.22):OFFLINE 只说明 presence 不可见,**不能**证明玩家
        已离开旧 DS,也不能授权进入另一台 DS。归属判定必须查 owner 权威。
        """
        try:
            out = await self._uc.get_location(request.player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return locator_pb2.GetLocationResponse(code=_to_proto_code(exc))
        return locator_pb2.GetLocationResponse(
            code=errcode_pb2.OK, location=_location_to_proto(out)
        )

    async def BatchGetLocation(  # noqa: N802
        self,
        request: locator_pb2.BatchGetLocationRequest,
        context: grpc.aio.ServicerContext,
    ) -> locator_pb2.BatchGetLocationResponse:
        try:
            outs = await self._uc.batch_get_location(list(request.player_ids))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return locator_pb2.BatchGetLocationResponse(code=_to_proto_code(exc))
        resp = locator_pb2.BatchGetLocationResponse(code=errcode_pb2.OK)
        for pid, out in outs.items():
            resp.locations[pid].CopyFrom(_location_to_proto(out))
        return resp

    async def BatchGetLastSeen(  # noqa: N802
        self,
        request: locator_pb2.BatchGetLastSeenRequest,
        context: grpc.aio.ServicerContext,
    ) -> locator_pb2.BatchGetLastSeenResponse:
        """批量查「最后一次被观测到离开 Hub 的时刻」。

        与 BatchGetLocation 同档:内部只读、不加 DS 守卫。
        分批由调用方负责(pkg/offlinewatch 按固定批量切),此处**不设截断** ——
        静默截断会让调用方误以为「这些玩家都没有记录」,进而按 UNKNOWN 放行,
        比返回大响应糟糕。
        """
        try:
            out = await self._uc.batch_get_last_seen(list(request.player_ids))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return locator_pb2.BatchGetLastSeenResponse(code=_to_proto_code(exc))
        resp = locator_pb2.BatchGetLastSeenResponse(code=errcode_pb2.OK)
        for pid, ms in out.items():
            resp.last_seen_ms[pid] = ms
        return resp

    # ── presence 订阅(§13.4.1)────────────────────────────────────────────

    async def SubscribePresence(  # noqa: N802
        self,
        request: locator_pb2.SubscribePresenceRequest,
        context: grpc.aio.ServicerContext,
    ) -> locator_pb2.SubscribePresenceResponse:
        """客户端打开好友面板 → 订阅这批好友的在线态变更。

        presence 未启用时为 no-op(纯拉模式),**不报错** —— 与 Go 同。
        """
        try:
            self._uc.subscribe_presence(
                request.subscriber_id, list(request.watched_player_ids)
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return locator_pb2.SubscribePresenceResponse(code=_to_proto_code(exc))
        return locator_pb2.SubscribePresenceResponse(code=errcode_pb2.OK)

    async def UnsubscribePresence(  # noqa: N802
        self,
        request: locator_pb2.UnsubscribePresenceRequest,
        context: grpc.aio.ServicerContext,
    ) -> locator_pb2.UnsubscribePresenceResponse:
        try:
            self._uc.unsubscribe_presence(request.subscriber_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return locator_pb2.UnsubscribePresenceResponse(code=_to_proto_code(exc))
        return locator_pb2.UnsubscribePresenceResponse(code=errcode_pb2.OK)

    # ── Hub DS 回调面 ─────────────────────────────────────────────────────

    async def RefreshHubLocations(  # noqa: N802
        self,
        request: locator_pb2.RefreshHubLocationsRequest,
        context: grpc.aio.ServicerContext,
    ) -> locator_pb2.RefreshHubLocationsResponse:
        """Hub DS 心跳捎带的在线保活:批量续期 HUB 位置 TTL。

        整批被拒 = 这台 Hub 上所有玩家的 presence 都不再续期,TTL 后集体蒸发
        (对下游就是「整台服的人同时离线」)—— 所以拒绝必须可见,故守卫与终态门的
        拒绝在这里各打一条日志,而不是只靠 usecase 那侧。
        """
        scope = dsauth.DSScope(
            ds_type=pauth.DS_TYPE_HUB, pod=request.hub_pod, require_token=True
        )
        cred, code = self._check_ds_callback(context, scope)
        if code != 0:
            _log_callback_rejected(
                "RefreshHubLocations",
                REASON_DS_GUARD_REJECTED,
                0,
                request.hub_pod,
                _guard_reject_err(code),
                requested=len(request.player_ids),
            )
            return locator_pb2.RefreshHubLocationsResponse(code=code)
        if self._hub_credential_checker is not None:
            chk = await self._hub_credential_checker.check_active(request.hub_pod, cred)
            if chk.code != 0:
                _log_callback_rejected(
                    "RefreshHubLocations",
                    REASON_HUB_CREDENTIAL_INACTIVE,
                    0,
                    request.hub_pod,
                    chk.err,
                    requested=len(request.player_ids),
                )
                return locator_pb2.RefreshHubLocationsResponse(code=chk.code)
        try:
            refreshed = await self._uc.refresh_hub_locations(
                request.hub_pod, list(request.player_ids)
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return locator_pb2.RefreshHubLocationsResponse(code=_to_proto_code(exc))
        return locator_pb2.RefreshHubLocationsResponse(
            code=errcode_pb2.OK, refreshed=refreshed
        )

    async def ReportDisconnect(  # noqa: N802
        self,
        request: locator_pb2.ReportDisconnectRequest,
        context: grpc.aio.ServicerContext,
    ) -> locator_pb2.ReportDisconnectResponse:
        """快速断线上报:把 HUB 位置 TTL 缩到 grace(只缩不涨)。

        DS 回调范围绑定:hub 令牌 sub 必须等于 req.hub_pod(防伪造别的 pod 缩别人 TTL)。
        全仓确认:唯一合法调用者是 Hub DS,无任何内部 Go 服务调用,故置 require_token
        —— enforce 下无令牌直连(绕过 Envoy)一律拒(fail-closed,审核 P1)。
        """
        scope = dsauth.DSScope(
            ds_type=pauth.DS_TYPE_HUB, pod=request.hub_pod, require_token=True
        )
        cred, code = self._check_ds_callback(context, scope)
        if code != 0:
            _log_callback_rejected(
                "ReportDisconnect",
                REASON_DS_GUARD_REJECTED,
                request.player_id,
                request.hub_pod,
                _guard_reject_err(code),
            )
            return locator_pb2.ReportDisconnectResponse(code=code)
        if self._hub_credential_checker is not None:
            chk = await self._hub_credential_checker.check_active(request.hub_pod, cred)
            if chk.code != 0:
                _log_callback_rejected(
                    "ReportDisconnect",
                    REASON_HUB_CREDENTIAL_INACTIVE,
                    request.player_id,
                    request.hub_pod,
                    chk.err,
                )
                return locator_pb2.ReportDisconnectResponse(code=chk.code)
        try:
            shrunk = await self._uc.report_disconnect(
                request.hub_pod,
                request.player_id,
                _fence_from_proto(request.hub_presence_fence),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return locator_pb2.ReportDisconnectResponse(code=_to_proto_code(exc))
        return locator_pb2.ReportDisconnectResponse(code=errcode_pb2.OK, shrunk=shrunk)

    # ───────────────────────────────────────────────────────────────────────
    # 已删除的 placement RPC(候选 B placement/proof 系统 2026-07 硬切下线)。
    # 路由权威 = TTL 位置租约(SetLocation / RefreshHubLocations / ReportDisconnect)。
    # proto service 定义暂留(不 regen),以下句柄一律返回 ERR_SERVICE_DISABLED。
    # ───────────────────────────────────────────────────────────────────────

    async def GetPlacement(  # noqa: N802
        self, request, context: grpc.aio.ServicerContext
    ) -> locator_pb2.GetPlacementResponse:
        return locator_pb2.GetPlacementResponse(code=_placement_removed("GetPlacement"))

    async def BeginPlacementTransition(  # noqa: N802
        self, request, context: grpc.aio.ServicerContext
    ) -> locator_pb2.BeginPlacementTransitionResponse:
        return locator_pb2.BeginPlacementTransitionResponse(
            code=_placement_removed("BeginPlacementTransition")
        )

    async def BindPlacementTarget(  # noqa: N802
        self, request, context: grpc.aio.ServicerContext
    ) -> locator_pb2.BindPlacementTargetResponse:
        return locator_pb2.BindPlacementTargetResponse(
            code=_placement_removed("BindPlacementTarget")
        )

    async def ConfirmPlacementSourceDeparture(  # noqa: N802
        self, request, context: grpc.aio.ServicerContext
    ) -> locator_pb2.ConfirmPlacementSourceDepartureResponse:
        return locator_pb2.ConfirmPlacementSourceDepartureResponse(
            code=_placement_removed("ConfirmPlacementSourceDeparture")
        )

    async def RetargetPlacementTarget(  # noqa: N802
        self, request, context: grpc.aio.ServicerContext
    ) -> locator_pb2.RetargetPlacementTargetResponse:
        return locator_pb2.RetargetPlacementTargetResponse(
            code=_placement_removed("RetargetPlacementTarget")
        )

    async def CommitPlacementAdmission(  # noqa: N802
        self, request, context: grpc.aio.ServicerContext
    ) -> locator_pb2.CommitPlacementAdmissionResponse:
        return locator_pb2.CommitPlacementAdmissionResponse(
            code=_placement_removed("CommitPlacementAdmission")
        )

    async def BootstrapPlacement(  # noqa: N802
        self, request, context: grpc.aio.ServicerContext
    ) -> locator_pb2.BootstrapPlacementResponse:
        return locator_pb2.BootstrapPlacementResponse(
            code=_placement_removed("BootstrapPlacement")
        )


def _placement_removed(rpc: str) -> int:
    """记录一次对已下线 placement RPC 的调用并给出统一错误码。

    留日志是必要的:调用方若还在打这些接口,说明有一处没跟上硬切,
    而 in-band 的 ERR_SERVICE_DISABLED 在 access log 里只是 rpc_ok。
    """
    plog.get().warning("placement_rpc_removed", rpc=rpc)
    return errcode.ErrServiceDisabled
