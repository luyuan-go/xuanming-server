"""friend gRPC service 层 —— 对应 Go 侧 internal/service/friend.go。

职责:
  - 实现 FriendServiceServicer(10 个 RPC)
  - 从鉴权上下文取 player_id(**R5**:忽略请求体里的身份字段,防伪造他人身份)
  - proto Request/Response ↔ biz 入参出参互转
  - errcode → pandora.common.v1.ErrCode 1:1 映射(纯数值)

⚠️ 返回值形态必须和 Go 一致:业务失败**不**抛 gRPC 错误,而是返回
   `Response(code=ErrXxx)` 且 gRPC status 为 OK —— 客户端读的是 body 里的 code。
   改成 context.abort() 会让客户端走到完全不同的错误分支(通用"网络错误"弹窗),
   ERR_FRIEND_LIMIT 这类**要给玩家看具体原因**的码就全丢了。

★ player_id == 0 → ERR_UNAUTHORIZED。Envoy jwt_authn 已在路由层 require JWT,
  这里是兜底:直连内网端口(绕过 Envoy)时不能拿到匿名身份就当成"某个玩家"。
"""

from __future__ import annotations

import asyncio

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.friend.v1 import friend_pb2, friend_pb2_grpc

from pandorapy import errcode, interceptors
from pandorapy.services.friend import biz as fbiz


class FriendService(friend_pb2_grpc.FriendServiceServicer):
    """实现 FriendServiceServicer。对应 Go 的 service.FriendService。"""

    __slots__ = ("_uc", "_sf")

    def __init__(self, uc: fbiz.FriendUsecase, snowflake_node) -> None:  # noqa: ANN001
        self._uc = uc
        # request_id 由 service 预生成(与 Go 同):biz 只在真正新建 / 复活请求行时用它,
        # 复用既有 pending 时丢弃 —— 多消耗一个序号,换 data 层不依赖发号器。
        self._sf = snowflake_node

    # ── 写路径 ───────────────────────────────────────────────────────────────

    async def AddFriend(  # noqa: N802 —— gRPC 生成的方法名,必须照抄
        self,
        request: friend_pb2.AddFriendRequest,
        context: grpc.aio.ServicerContext,
    ) -> friend_pb2.AddFriendResponse:
        """发起好友请求。requester 以鉴权上下文为准(R5)。"""
        player_id = interceptors.extract_player_id(context)
        if player_id == 0:
            return friend_pb2.AddFriendResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.target_player_id == 0:
            return friend_pb2.AddFriendResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            request_id = await self._uc.add_friend(
                player_id, request.target_player_id, self._sf.generate()
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透。CancelledError 在 3.8+ 是 BaseException,会被下面那条
            # 宽 except 吞掉 —— 而 grpc.aio 正是**用取消**终止在途 handler。
            # 吞掉的后果:优雅停机时把取消映射成业务错误码并返回正常响应,
            # 客户端每次滚动更新都收到一批假的业务失败,在途请求也没有真的排空。
            raise
        except BaseException as exc:  # noqa: BLE001 —— 与 Go 一样把 error 映射成 code
            return friend_pb2.AddFriendResponse(code=_to_proto_code(exc))
        return friend_pb2.AddFriendResponse(code=errcode_pb2.OK, request_id=request_id)

    async def AcceptFriend(  # noqa: N802
        self,
        request: friend_pb2.AcceptFriendRequest,
        context: grpc.aio.ServicerContext,
    ) -> friend_pb2.AcceptFriendResponse:
        """接受好友请求。接受者以鉴权上下文为准(R5)。"""
        player_id = interceptors.extract_player_id(context)
        if player_id == 0:
            return friend_pb2.AcceptFriendResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.request_id == 0:
            return friend_pb2.AcceptFriendResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self._uc.accept_friend(player_id, request.request_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return friend_pb2.AcceptFriendResponse(code=_to_proto_code(exc))
        return friend_pb2.AcceptFriendResponse(code=errcode_pb2.OK)

    async def RejectFriend(  # noqa: N802
        self,
        request: friend_pb2.RejectFriendRequest,
        context: grpc.aio.ServicerContext,
    ) -> friend_pb2.RejectFriendResponse:
        """拒绝好友请求。拒绝者以鉴权上下文为准(R5)。"""
        player_id = interceptors.extract_player_id(context)
        if player_id == 0:
            return friend_pb2.RejectFriendResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.request_id == 0:
            return friend_pb2.RejectFriendResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self._uc.reject_friend(player_id, request.request_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return friend_pb2.RejectFriendResponse(code=_to_proto_code(exc))
        return friend_pb2.RejectFriendResponse(code=errcode_pb2.OK)

    async def RemoveFriend(  # noqa: N802
        self,
        request: friend_pb2.RemoveFriendRequest,
        context: grpc.aio.ServicerContext,
    ) -> friend_pb2.RemoveFriendResponse:
        """删好友。player_id 以鉴权上下文为准(R5)。"""
        player_id = interceptors.extract_player_id(context)
        if player_id == 0:
            return friend_pb2.RemoveFriendResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.target_player_id == 0:
            return friend_pb2.RemoveFriendResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self._uc.remove_friend(player_id, request.target_player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return friend_pb2.RemoveFriendResponse(code=_to_proto_code(exc))
        return friend_pb2.RemoveFriendResponse(code=errcode_pb2.OK)

    async def Block(  # noqa: N802
        self,
        request: friend_pb2.BlockRequest,
        context: grpc.aio.ServicerContext,
    ) -> friend_pb2.BlockResponse:
        """拉黑 target。player_id 以鉴权上下文为准(R5)。"""
        player_id = interceptors.extract_player_id(context)
        if player_id == 0:
            return friend_pb2.BlockResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.target_player_id == 0:
            return friend_pb2.BlockResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self._uc.block(player_id, request.target_player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return friend_pb2.BlockResponse(code=_to_proto_code(exc))
        return friend_pb2.BlockResponse(code=errcode_pb2.OK)

    async def Unblock(  # noqa: N802
        self,
        request: friend_pb2.UnblockRequest,
        context: grpc.aio.ServicerContext,
    ) -> friend_pb2.UnblockResponse:
        """取消拉黑。player_id 以鉴权上下文为准(R5)。"""
        player_id = interceptors.extract_player_id(context)
        if player_id == 0:
            return friend_pb2.UnblockResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.target_player_id == 0:
            return friend_pb2.UnblockResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self._uc.unblock(player_id, request.target_player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return friend_pb2.UnblockResponse(code=_to_proto_code(exc))
        return friend_pb2.UnblockResponse(code=errcode_pb2.OK)

    # ── 读路径 ───────────────────────────────────────────────────────────────

    async def ListFriendRequests(  # noqa: N802
        self,
        request: friend_pb2.ListFriendRequestsRequest,
        context: grpc.aio.ServicerContext,
    ) -> friend_pb2.ListFriendRequestsResponse:
        """列待处理(收到的)好友请求。player_id 以鉴权上下文为准(R5)。"""
        player_id = interceptors.extract_player_id(context)
        if player_id == 0:
            return friend_pb2.ListFriendRequestsResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            requests = await self._uc.list_friend_requests(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return friend_pb2.ListFriendRequestsResponse(code=_to_proto_code(exc))
        return friend_pb2.ListFriendRequestsResponse(
            code=errcode_pb2.OK, requests=requests
        )

    async def ListFriends(  # noqa: N802
        self,
        request: friend_pb2.ListFriendsRequest,
        context: grpc.aio.ServicerContext,
    ) -> friend_pb2.ListFriendsResponse:
        """列好友。player_id 以鉴权上下文为准(R5)。"""
        player_id = interceptors.extract_player_id(context)
        if player_id == 0:
            return friend_pb2.ListFriendsResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            friends = await self._uc.list_friends(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return friend_pb2.ListFriendsResponse(code=_to_proto_code(exc))
        return friend_pb2.ListFriendsResponse(code=errcode_pb2.OK, friends=friends)

    async def ListBlocks(  # noqa: N802
        self,
        request: friend_pb2.ListBlocksRequest,
        context: grpc.aio.ServicerContext,
    ) -> friend_pb2.ListBlocksResponse:
        """列黑名单。player_id 以鉴权上下文为准(R5)。"""
        player_id = interceptors.extract_player_id(context)
        if player_id == 0:
            return friend_pb2.ListBlocksResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            blocks = await self._uc.list_blocks(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return friend_pb2.ListBlocksResponse(code=_to_proto_code(exc))
        return friend_pb2.ListBlocksResponse(code=errcode_pb2.OK, blocks=blocks)

    async def RecommendFriends(  # noqa: N802
        self,
        request: friend_pb2.RecommendFriendsRequest,
        context: grpc.aio.ServicerContext,
    ) -> friend_pb2.RecommendFriendsResponse:
        """推荐好友(客户端回传 exclude_player_ids 刷新)。player_id 以鉴权上下文为准(R5)。"""
        player_id = interceptors.extract_player_id(context)
        if player_id == 0:
            return friend_pb2.RecommendFriendsResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            recs = await self._uc.recommend_friends(
                player_id, int(request.limit), list(request.exclude_player_ids)
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return friend_pb2.RecommendFriendsResponse(code=_to_proto_code(exc))
        return friend_pb2.RecommendFriendsResponse(
            code=errcode_pb2.OK, recommendations=recs
        )


def _to_proto_code(exc: BaseException) -> int:
    """errcode → proto ErrCode(纯数值,与 Go 的 toProtoCode 同)。

    非 PandoraError(下游库抛的原生异常)由 errcode.as_code 兜成 ERR_UNKNOWN
    (与 Go 的 errcode.As 同)—— 绝不会漏成 OK,那会让客户端把失败当成成功。
    """
    return errcode.as_code(exc)
