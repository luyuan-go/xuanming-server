"""team gRPC service 层 —— 对应 Go 侧 internal/service/team.go + match_call_auth.go。

职责:
  - 实现 TeamServiceServicer(17 个 RPC:14 个客户端面 + 3 个内部东西向)
  - 从鉴权上下文取 player_id(R5:override request 字段,防伪造他人身份)
  - proto Request/Response ↔ biz 入参/出参互转(R1:Response 包含完整 Team 快照)
  - errcode → pandora.common.v1.ErrCode 1:1 映射

⚠️ 返回值形态必须与 Go 一致:业务失败返回 `Response(code=ErrXxx)` 且 gRPC status 为 OK,
   客户端读 body 里的 code。改成 `context.abort()` 会让调用方走到完全不同的错误分支 ——
   这是最容易在迁移中悄悄改掉的语义。**team 的 17 个 RPC 无一例外**(逐个对着
   Go 的 internal/service/team.go 核过:每条失败路径都是 `return &XxxResponse{Code: …}, nil`)。

★ 身份来源:客户端面 RPC 一律 `interceptors.extract_player_id`,**忽略请求体里的
  player_id 字段**(R5)。Go 侧同款:`callerID(ctx)` 覆盖 request。
  唯二例外是 BeginTeamMatch 的 captain_id 与 GetPlayerTeam 的 player_id —— 那两个是
  matchmaker / DS 代**别的玩家**发起的东西向调用,自己并不持有那个玩家的 JWT;
  BeginTeamMatch 的 captain_id 仍在 biz 内被复核「那确实是本队队长」(§9.6 派生判定
  服务端重算,不因为「内部调用」就免检)。

★ 可诊断性(§11.3):本服务每个 RPC 都以 `Response(code=ERR_XXX)` + gRPC status OK 的
  形状拒绝,统一 access log 会把它记成 rpc_ok(DEBUG 级)—— 线上默认 info 级下,
  「未登录被拒」与「team_id 没填」这两类拒绝**一条都不出**。所以入参门与身份门必须由
  业务代码自己显式打 WARN,且带枚举 reason(diag.log_rpc_rejected)。
"""

from __future__ import annotations

import asyncio
import time

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.team.v1 import team_pb2, team_pb2_grpc

from pandorapy import dsauth
from pandorapy import errcode
from pandorapy import interceptors
from pandorapy import internalrpcauth
from pandorapy import log as plog
from pandorapy.services.team import diag

# 与 Go 的 teamv1.TeamService_*_FullMethodName 同值。签名把方法名算进 canonical string,
# 写错一个字母 matchmaker 那边签出来的凭证在这里永远验不过(而且只会表现成"验签失败")。
BEGIN_TEAM_MATCH_METHOD = "/pandora.team.v1.TeamService/BeginTeamMatch"
END_TEAM_MATCH_METHOD = "/pandora.team.v1.TeamService/EndTeamMatch"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _to_proto_code(exc: BaseException) -> int:
    """errcode → proto enum,1:1 数值映射。对应 Go 的 toProtoCode。"""
    return errcode.as_code(exc)


def _metadata_dict(context) -> dict[str, str]:  # noqa: ANN001
    """把 grpc metadata 摊成 dict,供 internalrpcauth.Verifier 消费。

    gRPC 保证 key 已小写;重复 key 取最后一个(与 Go 侧 `md.Get(k)[0]` 的差异只在
    「同名头出现多次」这种畸形请求上,两边都会验签失败,方向一致)。
    """
    out: dict[str, str] = {}
    for key, value in context.invocation_metadata() or ():
        if isinstance(value, bytes):
            continue  # -bin 头与本协议无关
        out[key.lower()] = value
    return out


class TeamService(team_pb2_grpc.TeamServiceServicer):
    """实现 TeamServiceServicer。对应 Go 的 service.TeamService。"""

    __slots__ = (
        "_uc",
        "_team_sf",
        "_invite_sf",
        "_ds_guard",
        "_match_call_auth",
        "_match_call_require",
    )

    def __init__(self, usecase, team_sf, invite_sf) -> None:  # noqa: ANN001
        self._uc = usecase
        # team_id 与 invite_id 是两个互不相干的 ID 空间(各自独立的 Redis key 前缀),
        # 各持一个独立发号器,各走各的 step 池。
        #
        # ⚠️ 两者共用同一 nodeID,发出的 ID 会逐位相同(见 snowflake_etcd.provide_node)。
        # team_id 与 invite_id 必须各自留在自己的 key 空间里,禁止混进同一个 dict /
        # 唯一键比较。
        self._team_sf = team_sf
        self._invite_sf = invite_sf
        # DS 回调令牌守卫;None = 未启用(mode=off),行为与接线前一致。
        self._ds_guard: dsauth.DSCallbackGuard | None = None
        # 校验 BeginTeamMatch / EndTeamMatch 确实来自 matchmaker。
        # None = 未配密钥,整道跳过;_match_call_require=False 时验不过只 WARN 放行(观察期)。
        self._match_call_auth: internalrpcauth.Verifier | None = None
        self._match_call_require = False

    # ── 由 main.py 在构造后注入 ────────────────────────────────────────────

    def set_ds_callback_guard(self, guard: dsauth.DSCallbackGuard | None) -> None:
        """不调用即 mode=off。"""
        self._ds_guard = guard

    def set_match_call_auth(
        self, verifier: internalrpcauth.Verifier | None, require: bool
    ) -> None:
        """注入 matchmaker 调用的验签器与是否强制。

        verifier 为 None = 未配密钥,整道跳过(行为与接线前完全一致)。
        """
        self._match_call_auth = verifier
        self._match_call_require = require

    # ── 辅助 ───────────────────────────────────────────────────────────────

    @staticmethod
    def _caller_id(context) -> int:  # noqa: ANN001
        """从鉴权上下文取 player_id。对应 Go 的 callerID(ctx)。"""
        return interceptors.extract_player_id(context)

    def _system_only(self, context) -> int:  # noqa: ANN001
        """内部东西向接口鉴权:带玩家 JWT 的调用一律拒。返回 0 = 放行。

        Envoy 是按 `/pandora.team.v1.TeamService/` **整前缀**路由到本服务的,没有按方法
        的白名单,因此"内部方法"在客户端面同样可达。少了这道门,GetPlayerTeam 就是一个
        "查任意玩家在哪支队"的 IDOR 口子。

        拒绝必须留痕:这类分支以 response Code + OK status 返回,统一 access log 会记成
        rpc_ok,不打日志则线上对越权尝试零可见性。
        """
        caller = self._caller_id(context)
        if caller != 0:
            plog.get().warning(
                "team_authz_denied",
                reason=diag.REASON_SYSTEM_RPC_BY_CLIENT,
                caller_id=caller,
            )
            return errcode.ErrPermissionDeny
        return 0

    async def _verify_match_call(
        self, context, full_method: str, subject: int
    ) -> int:  # noqa: ANN001
        """校验本次调用确实来自 matchmaker(对应 Go 的 verifyMatchCall)。

        # 为什么 system_only 不够

        `system_only` 只能证明「本次调用不带玩家 JWT」——:8444 没有 jwt_authn,集群内网里
        任何 Pod、任何能连到本服务端口的东西都满足 caller==0。而这两个方法的杀伤力是实打
        实的:BeginTeamMatch 能给**任意**队伍上 roster 租约(反复调 = 让那支队伍永远开不了
        局);EndTeamMatch 能把**任意**队伍打回 FORMING。

        # 三档与「不靠发布顺序」(§9.21)

        直接上强制会踩坑:team 先要求签名而 matchmaker 还没滚到签名版本 → 全线拒。
        因此做成可降级三档(留空 / 观察 / 强制),上线顺序是「两边配密钥 → 观察 →
        翻 require」,每一步单独都安全。

        返回 0 表示放行(含观察期的降级放行);其余为应当回给调用方的错误码。
        """
        if self._match_call_auth is None:
            return 0  # 未配密钥:整道不启用
        try:
            await self._match_call_auth.verify(
                _metadata_dict(context), full_method, subject
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            if not self._match_call_require:
                # 观察期:只记不拒。运维据此确认 matchmaker 是否已全量滚上签名版本
                # (这条日志归零后才翻 require=true)。
                plog.get().warning(
                    "team_match_call_auth_observed",
                    method=full_method,
                    subject=subject,
                    err=str(exc),
                    hint="观察期未强制;matchmaker 全量滚上签名版本后把 "
                    "match_call_auth_require 置 true",
                )
                return 0
            code = errcode.ErrPermissionDeny
            if isinstance(exc, internalrpcauth.ErrUnavailable):
                # 重放存储不可用 → 说不清是不是重放,按不确定回,让调用方重试而不是当成越权。
                code = errcode.ErrUnavailable
            plog.get().warning(
                "team_match_call_auth_rejected",
                method=full_method,
                subject=subject,
                code=code,
                err=str(exc),
            )
            return code
        return 0

    # ── 14 个客户端面 RPC ─────────────────────────────────────────────────

    async def CreateTeam(  # noqa: N802 —— gRPC 生成的方法名,必须照抄
        self, request: team_pb2.CreateTeamRequest, context: grpc.aio.ServicerContext
    ) -> team_pb2.CreateTeamResponse:
        """创建队伍。player_id 以鉴权上下文为准(R5)。"""
        player_id = self._caller_id(context)
        if player_id == 0:
            diag.log_rpc_rejected("CreateTeam", diag.REASON_UNAUTHENTICATED)
            return team_pb2.CreateTeamResponse(code=errcode_pb2.ERR_UNAUTHORIZED)

        team_id = self._team_sf.generate()
        try:
            rec = await self._uc.create_team(team_id, player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.CreateTeamResponse(code=_to_proto_code(exc))
        return team_pb2.CreateTeamResponse(
            code=errcode_pb2.OK,
            team_id=rec.team_id,
            team=await self._uc.team_to_proto(rec),
        )

    async def Invite(  # noqa: N802
        self, request: team_pb2.InviteRequest, context: grpc.aio.ServicerContext
    ) -> team_pb2.InviteResponse:
        """邀请玩家。inviter_id 以鉴权上下文为准(R5)。"""
        inviter_id = self._caller_id(context)
        if inviter_id == 0:
            diag.log_rpc_rejected("Invite", diag.REASON_UNAUTHENTICATED)
            return team_pb2.InviteResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.team_id == 0 or request.target_player_id == 0:
            # 一个 if 两个条件 → 两个 reason(R2):缺 team_id 是客户端队伍态丢了,
            # 缺 target 是选人 UI 没传上来,排查方向完全不同。
            reason = diag.REASON_MISSING_TEAM_ID
            if request.team_id != 0:
                reason = diag.REASON_MISSING_TARGET_PLAYER_ID
            diag.log_rpc_rejected(
                "Invite",
                reason,
                team_id=request.team_id,
                target_player_id=request.target_player_id,
            )
            return team_pb2.InviteResponse(code=errcode_pb2.ERR_INVALID_ARG)

        invite_id = self._invite_sf.generate()
        try:
            rec = await self._uc.invite(
                invite_id, request.team_id, inviter_id, request.target_player_id
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.InviteResponse(code=_to_proto_code(exc))
        # expires_at_ms 以"现在"为锚点,与 biz 写 redis 的 TTL 起算点一致;
        # 不能用 rec.updated_at_ms(那是队伍上次变更时间,Invite 不改队伍,会偏早过期)。
        expires_at_ms = _now_ms() + self._uc.invite_ttl_ms()
        return team_pb2.InviteResponse(
            code=errcode_pb2.OK,
            team=await self._uc.team_to_proto(rec),
            invite_id=invite_id,
            expires_at_ms=expires_at_ms,
        )

    async def AcceptInvite(  # noqa: N802
        self, request: team_pb2.AcceptInviteRequest, context: grpc.aio.ServicerContext
    ) -> team_pb2.AcceptInviteResponse:
        """接受邀请。player_id 以鉴权上下文为准(R5)。"""
        player_id = self._caller_id(context)
        if player_id == 0:
            diag.log_rpc_rejected("AcceptInvite", diag.REASON_UNAUTHENTICATED)
            return team_pb2.AcceptInviteResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.team_id == 0:
            diag.log_rpc_rejected(
                "AcceptInvite", diag.REASON_MISSING_TEAM_ID, invite_id=request.invite_id
            )
            return team_pb2.AcceptInviteResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            rec = await self._uc.accept_invite(
                request.invite_id, request.team_id, player_id
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.AcceptInviteResponse(code=_to_proto_code(exc))
        return team_pb2.AcceptInviteResponse(
            code=errcode_pb2.OK, team=await self._uc.team_to_proto(rec)
        )

    async def LeaveTeam(  # noqa: N802
        self, request: team_pb2.LeaveTeamRequest, context: grpc.aio.ServicerContext
    ) -> team_pb2.LeaveTeamResponse:
        """离队。player_id 以鉴权上下文为准(R5)。"""
        player_id = self._caller_id(context)
        if player_id == 0:
            diag.log_rpc_rejected("LeaveTeam", diag.REASON_UNAUTHENTICATED)
            return team_pb2.LeaveTeamResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.team_id == 0:
            diag.log_rpc_rejected("LeaveTeam", diag.REASON_MISSING_TEAM_ID)
            return team_pb2.LeaveTeamResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            rec = await self._uc.leave_team(request.team_id, player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.LeaveTeamResponse(code=_to_proto_code(exc))
        return team_pb2.LeaveTeamResponse(
            code=errcode_pb2.OK, team=await self._uc.team_to_proto(rec)
        )

    async def Kick(  # noqa: N802
        self, request: team_pb2.KickRequest, context: grpc.aio.ServicerContext
    ) -> team_pb2.KickResponse:
        """踢人。captain_id 以鉴权上下文为准(R5)。"""
        captain_id = self._caller_id(context)
        if captain_id == 0:
            diag.log_rpc_rejected("Kick", diag.REASON_UNAUTHENTICATED)
            return team_pb2.KickResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.team_id == 0 or request.target_player_id == 0:
            reason = diag.REASON_MISSING_TEAM_ID
            if request.team_id != 0:
                reason = diag.REASON_MISSING_TARGET_PLAYER_ID
            diag.log_rpc_rejected(
                "Kick",
                reason,
                team_id=request.team_id,
                target_player_id=request.target_player_id,
            )
            return team_pb2.KickResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            rec = await self._uc.kick(
                request.team_id, captain_id, request.target_player_id
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.KickResponse(code=_to_proto_code(exc))
        return team_pb2.KickResponse(
            code=errcode_pb2.OK, team=await self._uc.team_to_proto(rec)
        )

    async def SetReady(  # noqa: N802
        self, request: team_pb2.SetReadyRequest, context: grpc.aio.ServicerContext
    ) -> team_pb2.SetReadyResponse:
        """设置准备状态。player_id 以鉴权上下文为准(R5)。"""
        player_id = self._caller_id(context)
        if player_id == 0:
            diag.log_rpc_rejected(
                "SetReady", diag.REASON_UNAUTHENTICATED, want_ready=request.ready
            )
            return team_pb2.SetReadyResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.team_id == 0:
            diag.log_rpc_rejected(
                "SetReady", diag.REASON_MISSING_TEAM_ID, want_ready=request.ready
            )
            return team_pb2.SetReadyResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            rec = await self._uc.set_ready(
                request.team_id, player_id, request.ready, request.hero_id
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.SetReadyResponse(code=_to_proto_code(exc))
        return team_pb2.SetReadyResponse(
            code=errcode_pb2.OK, team=await self._uc.team_to_proto(rec)
        )

    async def GetTeam(  # noqa: N802
        self, request: team_pb2.GetTeamRequest, context: grpc.aio.ServicerContext
    ) -> team_pb2.GetTeamResponse:
        """查询队伍(只读,无鉴权要求,team_id 即授权)。"""
        if request.team_id == 0:
            diag.log_rpc_rejected("GetTeam", diag.REASON_MISSING_TEAM_ID)
            return team_pb2.GetTeamResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            rec = await self._uc.get_team(request.team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.GetTeamResponse(code=_to_proto_code(exc))
        return team_pb2.GetTeamResponse(
            code=errcode_pb2.OK, team=await self._uc.team_to_proto(rec)
        )

    async def GetMyTeam(  # noqa: N802
        self, request: team_pb2.GetMyTeamRequest, context: grpc.aio.ServicerContext
    ) -> team_pb2.GetMyTeamResponse:
        """查询自己当前所在队伍的完整快照(队伍主界面直接渲染)。

        没队伍是正常态:返 OK + has_team_msg=false,不用 errcode 表达。
        """
        player_id = self._caller_id(context)
        if player_id == 0:
            diag.log_rpc_rejected("GetMyTeam", diag.REASON_UNAUTHENTICATED)
            return team_pb2.GetMyTeamResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            rec, has_team = await self._uc.get_my_team(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.GetMyTeamResponse(code=_to_proto_code(exc))
        if not has_team:
            return team_pb2.GetMyTeamResponse(code=errcode_pb2.OK, has_team_msg=False)
        return team_pb2.GetMyTeamResponse(
            code=errcode_pb2.OK,
            has_team_msg=True,
            team=await self._uc.team_to_proto(rec),
        )

    async def ListMyPendingInvites(  # noqa: N802
        self,
        request: team_pb2.ListMyPendingInvitesRequest,
        context: grpc.aio.ServicerContext,
    ) -> team_pb2.ListMyPendingInvitesResponse:
        """查询"发给我的待处理邀请"(只读,拉取兜底)。

        推送(TeamInviteEvent)只是投影/加速器,这里才是邀请令牌的权威查询(§9-22):
        客户端在登录、回前台、打开组队 UI 时调用,推送丢帧最多延迟弹窗,不丢邀请。
        没有邀请是正常态:返 OK + 空列表。
        """
        player_id = self._caller_id(context)
        if player_id == 0:
            diag.log_rpc_rejected("ListMyPendingInvites", diag.REASON_UNAUTHENTICATED)
            return team_pb2.ListMyPendingInvitesResponse(
                code=errcode_pb2.ERR_UNAUTHORIZED
            )
        try:
            recs = await self._uc.list_pending_invites(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.ListMyPendingInvitesResponse(code=_to_proto_code(exc))
        return team_pb2.ListMyPendingInvitesResponse(
            code=errcode_pb2.OK,
            invites=[
                team_pb2.PendingInvite(
                    team_id=r.team_id,
                    invite_id=r.invite_id,
                    inviter_id=r.inviter_id,
                    expires_at_ms=r.expires_at_ms,
                )
                for r in recs
            ],
        )

    # ── 找队伍:列表 / 申请 / 审批 ────────────────────────────────────────

    async def SetTeamMap(  # noqa: N802
        self, request: team_pb2.SetTeamMapRequest, context: grpc.aio.ServicerContext
    ) -> team_pb2.SetTeamMapResponse:
        """队长设置本队目标关卡。captain_id 以鉴权上下文为准(R5)。

        map_id=0 合法(清空/未指定),因此不做非零校验。
        """
        captain_id = self._caller_id(context)
        if captain_id == 0:
            diag.log_rpc_rejected(
                "SetTeamMap", diag.REASON_UNAUTHENTICATED, map_id=request.map_id
            )
            return team_pb2.SetTeamMapResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.team_id == 0:
            diag.log_rpc_rejected(
                "SetTeamMap", diag.REASON_MISSING_TEAM_ID, map_id=request.map_id
            )
            return team_pb2.SetTeamMapResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            rec = await self._uc.set_team_map(
                request.team_id, captain_id, request.map_id
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.SetTeamMapResponse(code=_to_proto_code(exc))
        return team_pb2.SetTeamMapResponse(
            code=errcode_pb2.OK, team=await self._uc.team_to_proto(rec)
        )

    async def ListOpenTeams(  # noqa: N802
        self, request: team_pb2.ListOpenTeamsRequest, context: grpc.aio.ServicerContext
    ) -> team_pb2.ListOpenTeamsResponse:
        """列正在招募的队伍(只读)。

        要求登录:这是面向客户端的枚举接口(GetTeam 那种"知道 team_id 即授权"的口径
        不适用),未登录一律拒。map_id=0 表示不限关卡;limit 由 biz 钳到
        max_open_teams_per_query。没有开放队伍是正常态:返 OK + 空列表。
        """
        player_id = self._caller_id(context)
        if player_id == 0:
            diag.log_rpc_rejected(
                "ListOpenTeams", diag.REASON_UNAUTHENTICATED, map_id=request.map_id
            )
            return team_pb2.ListOpenTeamsResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            teams = await self._uc.list_open_teams(request.map_id, int(request.limit))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.ListOpenTeamsResponse(code=_to_proto_code(exc))
        return team_pb2.ListOpenTeamsResponse(code=errcode_pb2.OK, teams=teams)

    async def ApplyToTeam(  # noqa: N802
        self, request: team_pb2.ApplyToTeamRequest, context: grpc.aio.ServicerContext
    ) -> team_pb2.ApplyToTeamResponse:
        """申请加入队伍。player_id 以鉴权上下文为准(R5)。

        走"申请待审批"还是"直接入队"由服务端配置决定,客户端不选也不能选。
        """
        player_id = self._caller_id(context)
        if player_id == 0:
            diag.log_rpc_rejected(
                "ApplyToTeam", diag.REASON_UNAUTHENTICATED, team_id=request.team_id
            )
            return team_pb2.ApplyToTeamResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.team_id == 0:
            diag.log_rpc_rejected("ApplyToTeam", diag.REASON_MISSING_TEAM_ID)
            return team_pb2.ApplyToTeamResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            joined, rec, expires_at_ms = await self._uc.apply_to_team(
                request.team_id, player_id
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.ApplyToTeamResponse(code=_to_proto_code(exc))
        resp = team_pb2.ApplyToTeamResponse(
            code=errcode_pb2.OK, joined=joined, expires_at_ms=expires_at_ms
        )
        if joined:
            resp.team.CopyFrom(await self._uc.team_to_proto(rec))
        return resp

    async def ListTeamApplications(  # noqa: N802
        self,
        request: team_pb2.ListTeamApplicationsRequest,
        context: grpc.aio.ServicerContext,
    ) -> team_pb2.ListTeamApplicationsResponse:
        """队长查本队待处理入队申请(只读)。

        没有申请是正常态:返 OK + 空列表。非队长返 ERR_TEAM_NOT_CAPTAIN。
        """
        captain_id = self._caller_id(context)
        if captain_id == 0:
            diag.log_rpc_rejected(
                "ListTeamApplications",
                diag.REASON_UNAUTHENTICATED,
                team_id=request.team_id,
            )
            return team_pb2.ListTeamApplicationsResponse(
                code=errcode_pb2.ERR_UNAUTHORIZED
            )
        if request.team_id == 0:
            diag.log_rpc_rejected(
                "ListTeamApplications", diag.REASON_MISSING_TEAM_ID
            )
            return team_pb2.ListTeamApplicationsResponse(
                code=errcode_pb2.ERR_INVALID_ARG
            )
        try:
            recs = await self._uc.list_team_applications(request.team_id, captain_id)
            applications = await self._uc.team_applications_to_proto(
                request.team_id, recs
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.ListTeamApplicationsResponse(code=_to_proto_code(exc))
        return team_pb2.ListTeamApplicationsResponse(
            code=errcode_pb2.OK,
            applications=applications,
        )

    async def HandleTeamApplication(  # noqa: N802
        self,
        request: team_pb2.HandleTeamApplicationRequest,
        context: grpc.aio.ServicerContext,
    ) -> team_pb2.HandleTeamApplicationResponse:
        """队长同意 / 拒绝一份入队申请。captain_id 以鉴权上下文为准(R5)。"""
        captain_id = self._caller_id(context)
        if captain_id == 0:
            diag.log_rpc_rejected(
                "HandleTeamApplication",
                diag.REASON_UNAUTHENTICATED,
                team_id=request.team_id,
                applicant_id=request.applicant_id,
            )
            return team_pb2.HandleTeamApplicationResponse(
                code=errcode_pb2.ERR_UNAUTHORIZED
            )
        if request.team_id == 0 or request.applicant_id == 0:
            reason = diag.REASON_MISSING_TEAM_ID
            if request.team_id != 0:
                reason = diag.REASON_MISSING_APPLICANT_ID
            diag.log_rpc_rejected(
                "HandleTeamApplication",
                reason,
                team_id=request.team_id,
                applicant_id=request.applicant_id,
                accept=request.accept,
            )
            return team_pb2.HandleTeamApplicationResponse(
                code=errcode_pb2.ERR_INVALID_ARG
            )
        try:
            rec = await self._uc.handle_team_application(
                request.team_id, captain_id, request.applicant_id, request.accept
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.HandleTeamApplicationResponse(code=_to_proto_code(exc))
        return team_pb2.HandleTeamApplicationResponse(
            code=errcode_pb2.OK, team=await self._uc.team_to_proto(rec)
        )

    # ── 3 个内部东西向 RPC ────────────────────────────────────────────────

    async def BeginTeamMatch(  # noqa: N802
        self, request: team_pb2.BeginTeamMatchRequest, context: grpc.aio.ServicerContext
    ) -> team_pb2.BeginTeamMatchResponse:
        """组票前的 roster fence(matchmaker 专用,内部东西向)。

        与其它 RPC 的区别:它不是客户端面接口,身份不取 JWT —— 调用方是 matchmaker,
        captain_id 由它从已校验的 JWT sub 透传过来,team 侧仍会复核那确实是本队队长
        (§9.6 派生判定服务端重算,不因为「内部调用」就免检)。
        """
        # 本方法此前**一道守卫都没有**(2026-08-13 补)。Envoy 按整前缀路由,带玩家 JWT
        # 的客户端同样打得到它 —— 而它能给**任意**队伍上一把 roster 租约,反复调用即可
        # 让那支队伍始终处于「被别人的组票占住」状态,队长自己反而开不了局。
        code = self._system_only(context)
        if code != 0:
            return team_pb2.BeginTeamMatchResponse(code=code)
        # system_only 只挡住「带玩家 JWT 的客户端」;这一道才校验调用方**确实是 matchmaker**。
        code = await self._verify_match_call(
            context, BEGIN_TEAM_MATCH_METHOD, request.team_id
        )
        if code != 0:
            return team_pb2.BeginTeamMatchResponse(code=code)
        # R3:内部东西向面既没有玩家 JWT(player_id 不会自动注入)、也没人写过 team_id。
        # player_id 必须在这里手写进日志上下文,否则 matchmaker→team 这一段在日志里与
        # 队长本人完全串不起来 ——「队长点了开始匹配之后发生了什么」就断在服务边界上。
        # ⚠️ 必须放在 system_only / _verify_match_call **之后**:system_only 判的就是
        # 上下文里有没有 player_id,提前写进去会把这道门直接打穿。
        token = plog.bind_player_id(request.captain_id)
        try:
            # require_ready 由 matchmaker 按本次 map_id 的关卡表 ready_mode 解析后传入;
            # 旧 matchmaker 不发 → False → 无门槛(与该字段上线前一致,§9.21 共存安全)。
            team, expires_at_ms = await self._uc.begin_team_match(
                request.team_id,
                request.captain_id,
                request.operation_id,
                request.lease_ms,
                request.require_ready,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.BeginTeamMatchResponse(code=_to_proto_code(exc))
        finally:
            token.var.reset(token)
        return team_pb2.BeginTeamMatchResponse(
            code=errcode_pb2.OK,
            team=await self._uc.team_to_proto(team),
            lease_expires_at_ms=expires_at_ms,
            # 冻结这份名单那一刻的 ready 代际。matchmaker 必须原样带进 match 记录,
            # 并在 EndTeamMatch 回传 —— 它是「这次复位对应的正是这一局」的唯一凭据。
            ready_generation=team.ready_generation,
        )

    async def EndTeamMatch(  # noqa: N802
        self, request: team_pb2.EndTeamMatchRequest, context: grpc.aio.ServicerContext
    ) -> team_pb2.EndTeamMatchResponse:
        """对局结束后复位队伍准备状态(matchmaker 专用,内部东西向)。

        **必须 system_only**,与 BeginTeamMatch 不同:Begin 拿队长身份当授权(只有本队
        队长能给自己队上一把 5s 自净的租约,越权收益近乎为零);而本方法能把任意队伍打回
        FORMING,客户端可达就等于一个「让任何队伍开不了局」的骚扰口子。Envoy 是按
        `/pandora.team.v1.TeamService/` 整前缀放行的,不显式拒就是对客户端开放。
        """
        code = self._system_only(context)
        if code != 0:
            return team_pb2.EndTeamMatchResponse(code=code)
        code = await self._verify_match_call(
            context, END_TEAM_MATCH_METHOD, request.team_id
        )
        if code != 0:
            return team_pb2.EndTeamMatchResponse(code=code)
        try:
            await self._uc.end_team_match(
                request.team_id,
                list(request.player_ids),
                request.expected_ready_generation,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.EndTeamMatchResponse(code=_to_proto_code(exc))
        return team_pb2.EndTeamMatchResponse(code=errcode_pb2.OK)

    async def GetPlayerTeam(  # noqa: N802
        self, request: team_pb2.GetPlayerTeamRequest, context: grpc.aio.ServicerContext
    ) -> team_pb2.GetPlayerTeamResponse:
        """按 player_id 反查队伍编号(内部东西向,DS 出生编制专用)。

        DS 在玩家进场时把队伍编号写到实体上并复制给全场客户端,「谁和谁是一伙的」才有
        权威来源;否则大厅里所有玩家共用玩家阵营,队友与路人无从区分。
        只回编号不回名单:DS 只判定「同队与否」,发整份 Team 快照是无谓的扩面(§9.14)。
        """
        code = self._system_only(context)
        if code != 0:
            return team_pb2.GetPlayerTeamResponse(code=code)
        # 令牌门。require_token:本方法只可能来自 DS —— 全仓没有任何内部服务调它,
        # 故直连无令牌也一律拒,堵住「被攻破的业务 Pod 绕过 Envoy 直连 20010、无标记
        # 无令牌被当东西向内部信任」。
        # 不绑 ds_type:大厅与对局都可能查(绑死 hub 会让将来任何新玩法的合法查询变成
        # 鉴权失败,而这里的范围收益是零 —— 令牌本身已证明是 DS)。
        # 不绑 pod / match_id:反查与哪台 DS、哪一局无关,填了只会把合法调用拒掉。
        if self._ds_guard is not None:
            code = self._ds_guard.check(context, dsauth.DSScope(require_token=True))
            if code != 0:
                return team_pb2.GetPlayerTeamResponse(code=code)
        player_id = request.player_id
        if player_id == 0:
            diag.log_rpc_rejected("GetPlayerTeam", diag.REASON_MISSING_PLAYER_ID)
            return team_pb2.GetPlayerTeamResponse(code=errcode_pb2.ERR_INVALID_ARG)
        # R3:DS 回调面走 AuthOptional 且 DS 不带 x-pandora-player-id —— player_id 必须
        # 手写,否则"某个玩家进场后队友颜色不对"在 team 侧完全定位不到人。
        # 放在 system_only / ds_guard **之后**(system_only 判的就是有没有 player_id)。
        token = plog.bind_player_id(player_id)
        try:
            team_id, has_team = await self._uc.get_player_team_id(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return team_pb2.GetPlayerTeamResponse(code=_to_proto_code(exc))
        finally:
            token.var.reset(token)
        if not has_team:
            # 没队伍是正常态,不是错误:DS 据此把实体的队伍编号留在「未知」。
            return team_pb2.GetPlayerTeamResponse(code=errcode_pb2.OK, has_team=False)
        return team_pb2.GetPlayerTeamResponse(
            code=errcode_pb2.OK, has_team=True, team_id=team_id
        )
