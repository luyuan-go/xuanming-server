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

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.locator.v1 import locator_pb2, locator_pb2_grpc

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy.services.player_locator import biz as lbiz
from pandorapy.services.player_locator import usecase as lusecase


def _to_proto_code(err: BaseException) -> int:
    """errcode → proto enum,1:1 数值映射。对应 Go 的 toProtoCode。"""
    return errcode.as_code(err)


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

    __slots__ = ("_uc",)

    def __init__(self, usecase: lusecase.LocatorUsecase) -> None:
        self._uc = usecase

    # ── 位置写入 ──────────────────────────────────────────────────────────

    async def SetLocation(  # noqa: N802 —— gRPC 生成的方法名,必须照抄
        self,
        request: locator_pb2.SetLocationRequest,
        context: grpc.aio.ServicerContext,
    ) -> locator_pb2.SetLocationResponse:
        """写入玩家位置。守卫拒绝 → ErrLocatorConflict(in-band)。

        ⚠️ Go 侧在这里还有一道 **DS 回调令牌守卫**(dsGuard.CheckHubCredential)。
        Python 侧尚未实现该 middleware,因此 main.py 在 `ds_auth.mode != off` 时
        **拒绝启动**(见 main.py 闸⑩)—— 绝不能在这里静默放行:
        那等于把 fail-closed 的令牌校验悄悄降级成 fail-open。
        mode=off(dev/当前生产档)下 Go 的守卫本身就是 no-op,两边行为一致。
        """
        loc = request.location
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
        (对下游就是「整台服的人同时离线」)—— 所以拒绝必须可见,见 usecase。

        ⚠️ Go 侧此处同样有 DS 回调令牌守卫;Python 侧的处理见 SetLocation 的说明。
        """
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
        """快速断线上报:把 HUB 位置 TTL 缩到 grace(只缩不涨)。"""
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
