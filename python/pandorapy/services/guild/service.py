"""guild 的 gRPC service 层 —— 对应 Go 侧 internal/service/{guild,group}.go。

同进程注册**两个 servicer**:GuildService(14 RPC) + GroupService(9 RPC) = 23 RPC。

⚠️ 返回值形态必须与 Go 一致:业务失败返回 `Response(code=ErrXxx)` 且 gRPC status
   为 **OK**,客户端读 body 里的 code。改成 `context.abort()` 会让调用方走到完全
   不同的错误分支 —— 这是最容易在迁移中悄悄改掉的语义。

★ 身份来源(R5):所有写 RPC **强制用鉴权上下文里的 player_id,忽略请求体字段**。
  Envoy 的 jwt_authn 已在路由层 require JWT 并把 sub 注入 x-pandora-player-id;
  这里再兜一次 `caller_id == 0 → ERR_UNAUTHORIZED`。
  照请求体取的话,任何人都能填别人的 player_id 去退别人的会。

★ `GetPlayerGuild` 是**内部东西向**接口(DS 出生编制反查,给铭牌关系上色用),
  两道门缺一不可:
    ① systemOnly:带玩家 JWT 的调用一律拒。Envoy 按
       `/pandora.guild.v1.GuildService/` **整前缀**路由,没有按方法的白名单 ——
       少了它,这就是「查任意玩家属于哪个公会」的 IDOR 口子。
    ② DS 回调令牌守卫:systemOnly 只证明「不带玩家 JWT」,证明不了「调用方是 DS」。
"""

from __future__ import annotations

import asyncio

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.group.v1 import group_pb2, group_pb2_grpc
from pandora.guild.v1 import guild_pb2, guild_pb2_grpc

from pandorapy import errcode, interceptors
from pandorapy import log as plog
from pandorapy.services.guild import biz as gbiz
from pandorapy.services.guild import ds_guard as gds


def _to_proto_code(err: BaseException) -> int:
    """errcode → proto enum,1:1 数值映射。对应 Go 的 `toProtoCode`。"""
    return errcode.as_code(err)


def _caller_id(context: grpc.aio.ServicerContext) -> int:
    """从鉴权上下文取 player_id(R5:**不看请求体**)。"""
    return interceptors.extract_player_id(context)


def _system_only(context: grpc.aio.ServicerContext) -> int:
    """内部东西向接口鉴权:带玩家 JWT 的调用一律拒。返回 proto code。

    拒绝分支以 response code + OK status 返回,统一 access log 会记成 rpc_ok;
    所以这里**必须自己打一条 WARN**,否则越权尝试零可见性
    (与 player 服务的 logAuthzDeny 同口径)。
    """
    caller = _caller_id(context)
    if caller != 0:
        plog.get().warning(
            "guild_authz_denied", reason="system_rpc_by_client", caller_id=caller
        )
        return errcode_pb2.ERR_PERMISSION_DENY
    return errcode_pb2.OK


class GuildService(guild_pb2_grpc.GuildServiceServicer):
    """实现 GuildServiceServicer(14 RPC)。对应 Go 的 `service.GuildService`。"""

    __slots__ = ("_uc", "_guild_sf", "_request_sf", "_ds_guard")

    def __init__(self, uc: gbiz.GuildUsecase, guild_sf, request_sf) -> None:  # noqa: ANN001
        self._uc = uc
        # guild_id 与 request_id 是两个互不相干的 ID 空间(公会主键 / 入会申请主键),
        # 各持一个独立发号器,各走各的 step 池。
        # ⚠️ 两者共用同一 nodeID,发出的 ID 会**逐位相同** —— 必须各自留在自己的表 /
        #    唯一键里,禁止混进同一容器比较。
        self._guild_sf = guild_sf
        self._request_sf = request_sf
        self._ds_guard: gds.DSCallbackGuard | None = None

    def set_ds_callback_guard(self, guard: gds.DSCallbackGuard | None) -> None:
        """由 main 在构造后注入;不调用 = mode off(与接线前行为一致)。"""
        self._ds_guard = guard

    # ── 写路径 ─────────────────────────────────────────────────────────────

    async def CreateGuild(  # noqa: N802 —— gRPC 生成的方法名,必须照抄
        self, request: guild_pb2.CreateGuildRequest, context: grpc.aio.ServicerContext
    ) -> guild_pb2.CreateGuildResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            return guild_pb2.CreateGuildResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            guild_id = await self._uc.create_guild(
                player_id, request.name, self._guild_sf.generate()
            )
        except asyncio.CancelledError:
            # ★ 必须先放行:CancelledError 在 3.8+ 是 BaseException,被下面的宽 except
            # 吞掉的话,优雅停机时会把「取消」映射成 in-band 业务码并返回**正常响应**
            # —— 客户端每次滚动更新都收到一批假失败,而排空在途也没有真的发生。
            raise
        except BaseException as exc:  # noqa: BLE001
            return guild_pb2.CreateGuildResponse(code=_to_proto_code(exc))
        return guild_pb2.CreateGuildResponse(code=errcode_pb2.OK, guild_id=guild_id)

    async def ApplyJoin(  # noqa: N802
        self, request: guild_pb2.ApplyJoinRequest, context: grpc.aio.ServicerContext
    ) -> guild_pb2.ApplyJoinResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            return guild_pb2.ApplyJoinResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            request_id = await self._uc.apply_join(
                player_id, request.guild_id, self._request_sf.generate()
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return guild_pb2.ApplyJoinResponse(code=_to_proto_code(exc))
        return guild_pb2.ApplyJoinResponse(code=errcode_pb2.OK, request_id=request_id)

    async def ApproveJoin(  # noqa: N802
        self, request: guild_pb2.ApproveJoinRequest, context: grpc.aio.ServicerContext
    ) -> guild_pb2.ApproveJoinResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            return guild_pb2.ApproveJoinResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            await self._uc.approve_join(player_id, request.request_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return guild_pb2.ApproveJoinResponse(code=_to_proto_code(exc))
        return guild_pb2.ApproveJoinResponse(code=errcode_pb2.OK)

    async def RejectJoin(  # noqa: N802
        self, request: guild_pb2.RejectJoinRequest, context: grpc.aio.ServicerContext
    ) -> guild_pb2.RejectJoinResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            return guild_pb2.RejectJoinResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            await self._uc.reject_join(player_id, request.request_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return guild_pb2.RejectJoinResponse(code=_to_proto_code(exc))
        return guild_pb2.RejectJoinResponse(code=errcode_pb2.OK)

    async def LeaveGuild(  # noqa: N802
        self, request: guild_pb2.LeaveGuildRequest, context: grpc.aio.ServicerContext
    ) -> guild_pb2.LeaveGuildResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            return guild_pb2.LeaveGuildResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            await self._uc.leave_guild(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return guild_pb2.LeaveGuildResponse(code=_to_proto_code(exc))
        return guild_pb2.LeaveGuildResponse(code=errcode_pb2.OK)

    async def KickMember(  # noqa: N802
        self, request: guild_pb2.KickMemberRequest, context: grpc.aio.ServicerContext
    ) -> guild_pb2.KickMemberResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            return guild_pb2.KickMemberResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.target_id == 0:
            return guild_pb2.KickMemberResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self._uc.kick_member(player_id, request.target_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return guild_pb2.KickMemberResponse(code=_to_proto_code(exc))
        return guild_pb2.KickMemberResponse(code=errcode_pb2.OK)

    async def DisbandGuild(  # noqa: N802
        self, request: guild_pb2.DisbandGuildRequest, context: grpc.aio.ServicerContext
    ) -> guild_pb2.DisbandGuildResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            return guild_pb2.DisbandGuildResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            await self._uc.disband_guild(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return guild_pb2.DisbandGuildResponse(code=_to_proto_code(exc))
        return guild_pb2.DisbandGuildResponse(code=errcode_pb2.OK)

    async def TransferLeader(  # noqa: N802
        self, request: guild_pb2.TransferLeaderRequest, context: grpc.aio.ServicerContext
    ) -> guild_pb2.TransferLeaderResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            return guild_pb2.TransferLeaderResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.target_id == 0:
            return guild_pb2.TransferLeaderResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self._uc.transfer_leader(player_id, request.target_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return guild_pb2.TransferLeaderResponse(code=_to_proto_code(exc))
        return guild_pb2.TransferLeaderResponse(code=errcode_pb2.OK)

    async def SetOfficer(  # noqa: N802
        self, request: guild_pb2.SetOfficerRequest, context: grpc.aio.ServicerContext
    ) -> guild_pb2.SetOfficerResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            return guild_pb2.SetOfficerResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.target_id == 0:
            return guild_pb2.SetOfficerResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self._uc.set_officer(player_id, request.target_id, request.is_officer)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return guild_pb2.SetOfficerResponse(code=_to_proto_code(exc))
        return guild_pb2.SetOfficerResponse(code=errcode_pb2.OK)

    # ── 读路径 ─────────────────────────────────────────────────────────────

    async def GetGuild(  # noqa: N802
        self, request: guild_pb2.GetGuildRequest, context: grpc.aio.ServicerContext
    ) -> guild_pb2.GetGuildResponse:
        """查公会(只读,任意人可查)。"""
        if request.guild_id == 0:
            return guild_pb2.GetGuildResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            g = await self._uc.get_guild(request.guild_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return guild_pb2.GetGuildResponse(code=_to_proto_code(exc))
        return guild_pb2.GetGuildResponse(code=errcode_pb2.OK, guild=g)

    async def GetMyGuild(  # noqa: N802
        self, request: guild_pb2.GetMyGuildRequest, context: grpc.aio.ServicerContext
    ) -> guild_pb2.GetMyGuildResponse:
        """查"我的公会"。**不在任何公会时 code=OK 且 guild 为空**,不是错误。"""
        player_id = _caller_id(context)
        if player_id == 0:
            return guild_pb2.GetMyGuildResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            g = await self._uc.get_my_guild(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return guild_pb2.GetMyGuildResponse(code=_to_proto_code(exc))
        if g is None:
            return guild_pb2.GetMyGuildResponse(code=errcode_pb2.OK)
        return guild_pb2.GetMyGuildResponse(code=errcode_pb2.OK, guild=g)

    async def GetPlayerGuild(  # noqa: N802
        self, request: guild_pb2.GetPlayerGuildRequest, context: grpc.aio.ServicerContext
    ) -> guild_pb2.GetPlayerGuildResponse:
        """按 player_id 反查公会编号(内部东西向,DS 出生编制专用)。

        有了它,客户端就不必靠本机名册反推会友 —— 名册按游标分页,没翻到的那一页
        会把会友判成路人。走**权威**读路径,不吃玩家面板那条 cache-aside 缓存。
        只把编号放进响应,整份 Guild 快照不外发(§9.14 最小视图)。
        """
        code = _system_only(context)
        if code != errcode_pb2.OK:
            return guild_pb2.GetPlayerGuildResponse(code=code)
        # 令牌门,与 team 侧 GetPlayerTeam 同口径:require_token 是因为全仓无内部
        # Go 调用方;不绑 type / pod / match_id 是因为反查与哪台 DS、哪一局无关。
        if self._ds_guard is not None:
            try:
                self._ds_guard.check(context, gds.DSScope(require_token=True))
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                return guild_pb2.GetPlayerGuildResponse(code=_to_proto_code(exc))
        if request.player_id == 0:
            return guild_pb2.GetPlayerGuildResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            guild_id = await self._uc.get_player_guild_id(request.player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return guild_pb2.GetPlayerGuildResponse(code=_to_proto_code(exc))
        if guild_id == 0:
            # 不在任何公会是正常态,不是错误。
            return guild_pb2.GetPlayerGuildResponse(code=errcode_pb2.OK, has_guild=False)
        return guild_pb2.GetPlayerGuildResponse(
            code=errcode_pb2.OK, has_guild=True, guild_id=guild_id
        )

    async def ListMembers(  # noqa: N802
        self, request: guild_pb2.ListMembersRequest, context: grpc.aio.ServicerContext
    ) -> guild_pb2.ListMembersResponse:
        if request.guild_id == 0:
            return guild_pb2.ListMembersResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            members, next_cursor = await self._uc.list_members(
                request.guild_id, request.cursor, int(request.limit)
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return guild_pb2.ListMembersResponse(code=_to_proto_code(exc))
        return guild_pb2.ListMembersResponse(
            code=errcode_pb2.OK, members=members, next_cursor=next_cursor
        )

    async def ListJoinRequests(  # noqa: N802
        self, request: guild_pb2.ListJoinRequestsRequest, context: grpc.aio.ServicerContext
    ) -> guild_pb2.ListJoinRequestsResponse:
        """列挂起申请。请求人以鉴权上下文为准(R5),须 LEADER / OFFICER。"""
        player_id = _caller_id(context)
        if player_id == 0:
            return guild_pb2.ListJoinRequestsResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            requests, next_cursor = await self._uc.list_join_requests(
                player_id, request.cursor, int(request.limit)
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return guild_pb2.ListJoinRequestsResponse(code=_to_proto_code(exc))
        return guild_pb2.ListJoinRequestsResponse(
            code=errcode_pb2.OK, requests=requests, next_cursor=next_cursor
        )


class GroupService(group_pb2_grpc.GroupServiceServicer):
    """实现 GroupServiceServicer(9 RPC)。对应 Go 的 `service.GroupService`。"""

    __slots__ = ("_uc", "_group_sf")

    def __init__(self, uc: gbiz.GroupUsecase, group_sf) -> None:  # noqa: ANN001
        self._uc = uc
        # group_id 独立成一个 ID 空间,与 GuildService 的两个发号器各用各的。
        # ⚠️ 三者共用同一 nodeID,发出的 ID 会逐位相同,必须各留各的表。
        self._group_sf = group_sf

    async def CreateGroup(  # noqa: N802
        self, request: group_pb2.CreateGroupRequest, context: grpc.aio.ServicerContext
    ) -> group_pb2.CreateGroupResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            return group_pb2.CreateGroupResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            group_id = await self._uc.create_group(
                player_id, request.name, list(request.member_ids), self._group_sf.generate()
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return group_pb2.CreateGroupResponse(code=_to_proto_code(exc))
        return group_pb2.CreateGroupResponse(code=errcode_pb2.OK, group_id=group_id)

    async def InviteToGroup(  # noqa: N802
        self, request: group_pb2.InviteToGroupRequest, context: grpc.aio.ServicerContext
    ) -> group_pb2.InviteToGroupResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            return group_pb2.InviteToGroupResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.group_id == 0 or request.target_id == 0:
            return group_pb2.InviteToGroupResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self._uc.invite_to_group(player_id, request.group_id, request.target_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return group_pb2.InviteToGroupResponse(code=_to_proto_code(exc))
        return group_pb2.InviteToGroupResponse(code=errcode_pb2.OK)

    async def LeaveGroup(  # noqa: N802
        self, request: group_pb2.LeaveGroupRequest, context: grpc.aio.ServicerContext
    ) -> group_pb2.LeaveGroupResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            return group_pb2.LeaveGroupResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.group_id == 0:
            return group_pb2.LeaveGroupResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self._uc.leave_group(player_id, request.group_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return group_pb2.LeaveGroupResponse(code=_to_proto_code(exc))
        return group_pb2.LeaveGroupResponse(code=errcode_pb2.OK)

    async def KickFromGroup(  # noqa: N802
        self, request: group_pb2.KickFromGroupRequest, context: grpc.aio.ServicerContext
    ) -> group_pb2.KickFromGroupResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            return group_pb2.KickFromGroupResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.group_id == 0 or request.target_id == 0:
            return group_pb2.KickFromGroupResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self._uc.kick_from_group(player_id, request.group_id, request.target_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return group_pb2.KickFromGroupResponse(code=_to_proto_code(exc))
        return group_pb2.KickFromGroupResponse(code=errcode_pb2.OK)

    async def DisbandGroup(  # noqa: N802
        self, request: group_pb2.DisbandGroupRequest, context: grpc.aio.ServicerContext
    ) -> group_pb2.DisbandGroupResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            return group_pb2.DisbandGroupResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.group_id == 0:
            return group_pb2.DisbandGroupResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self._uc.disband_group(player_id, request.group_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return group_pb2.DisbandGroupResponse(code=_to_proto_code(exc))
        return group_pb2.DisbandGroupResponse(code=errcode_pb2.OK)

    async def TransferOwner(  # noqa: N802
        self, request: group_pb2.TransferOwnerRequest, context: grpc.aio.ServicerContext
    ) -> group_pb2.TransferOwnerResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            return group_pb2.TransferOwnerResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.group_id == 0 or request.target_id == 0:
            return group_pb2.TransferOwnerResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self._uc.transfer_owner(player_id, request.group_id, request.target_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return group_pb2.TransferOwnerResponse(code=_to_proto_code(exc))
        return group_pb2.TransferOwnerResponse(code=errcode_pb2.OK)

    async def GetGroup(  # noqa: N802
        self, request: group_pb2.GetGroupRequest, context: grpc.aio.ServicerContext
    ) -> group_pb2.GetGroupResponse:
        if request.group_id == 0:
            return group_pb2.GetGroupResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            g = await self._uc.get_group(request.group_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return group_pb2.GetGroupResponse(code=_to_proto_code(exc))
        return group_pb2.GetGroupResponse(code=errcode_pb2.OK, group=g)

    async def ListGroupMembers(  # noqa: N802
        self, request: group_pb2.ListGroupMembersRequest, context: grpc.aio.ServicerContext
    ) -> group_pb2.ListGroupMembersResponse:
        if request.group_id == 0:
            return group_pb2.ListGroupMembersResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            members = await self._uc.list_group_members(request.group_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return group_pb2.ListGroupMembersResponse(code=_to_proto_code(exc))
        return group_pb2.ListGroupMembersResponse(code=errcode_pb2.OK, members=members)

    async def ListMyGroups(  # noqa: N802
        self, request: group_pb2.ListMyGroupsRequest, context: grpc.aio.ServicerContext
    ) -> group_pb2.ListMyGroupsResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            return group_pb2.ListMyGroupsResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            groups = await self._uc.list_my_groups(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return group_pb2.ListMyGroupsResponse(code=_to_proto_code(exc))
        return group_pb2.ListMyGroupsResponse(code=errcode_pb2.OK, groups=groups)
