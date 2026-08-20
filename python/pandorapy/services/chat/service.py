"""chat gRPC service 层 —— 对应 Go 侧 internal/service/chat.go。

职责:
  - 实现 `chat_pb2_grpc.ChatServiceServicer`(两个 RPC:SendMessage / PullHistory)
  - 从鉴权上下文取 player_id(R5),proto ↔ biz 入参出参互转
  - errcode → `pandora.common.v1.ErrCode` 1:1 数值映射

★ 协议原则(R5,两处都不能少):
  SendMessage 的发送者、PullHistory 的查询者一律以**鉴权上下文**里的 player_id 为准。
  proto 里这两个字段已经删成 `reserved`(老客户端仍会发字段号 1),
  所以这里连"忽略请求体字段"都不用做 —— 但判定必须留着:
  `player_id == 0`(无身份)→ `ERR_UNAUTHORIZED`。少了这一条,任何没过 Envoy jwt_authn
  的直连都能以 sender_id=0 的身份发消息,而 biz 的 `sender required` 会把它报成
  参数错误 —— 客户端看到的是"参数不对"而不是"你没登录"。

⚠️ 返回值形态必须和 Go 一致:业务失败**不**抛 gRPC 错误,而是返回
   `Response(code=ErrXxx)` 且 gRPC status 为 OK。客户端读的是 body 里的 code;
   改成 `context.abort()` 会让客户端走到完全不同的错误分支。
   chat 的两个 RPC 在 Go 侧都是这个形态,没有例外。
"""

from __future__ import annotations

import asyncio

import grpc
from pandora.chat.v1 import chat_pb2, chat_pb2_grpc
from pandora.common.v1 import errcode_pb2

from pandorapy import errcode, interceptors
from pandorapy.services.chat import biz as cbiz

GRPC_SERVICE_FULL_NAME = "pandora.chat.v1.ChatService"


class ChatService(chat_pb2_grpc.ChatServiceServicer):
    """实现 ChatServiceServicer。对应 Go 的 service.ChatService。"""

    __slots__ = ("_uc", "_sf")

    def __init__(self, uc: cbiz.ChatUsecase, sf) -> None:  # noqa: ANN001
        self._uc = uc
        # sf 只需要 generate() —— 与 Go 的 snowflakeGen 最小接口同因:
        # service 层不该因为"要个 ID"就依赖整个雪花实现。
        self._sf = sf

    async def SendMessage(  # noqa: N802 —— gRPC 生成的方法名,必须照抄
        self,
        request: chat_pb2.SendMessageRequest,
        context: grpc.aio.ServicerContext,
    ) -> chat_pb2.SendMessageResponse:
        """发一条聊天消息。sender 以鉴权上下文为准(R5)。"""
        sender_id = _caller_id(context)
        if sender_id == 0:
            return chat_pb2.SendMessageResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            # ★ message_id 在**进 biz 之前**铸好(与 Go 逐字同):biz 里私聊要先落库
            # 再推送,两处必须是同一个 ID;在 biz 内部各铸一次会让库里的 ID 与推给
            # 客户端的 ID 不同,客户端拿着推送里的 ID 去拉历史永远对不上。
            message_id = await self._uc.send_message(
                sender_id,
                request.channel,
                request.target_id,
                request.content,
                self._sf.generate(),
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透。CancelledError 在 3.8+ 是 BaseException,会被下面那条
            # 宽 except 吞掉 —— 而 grpc.aio 正是**用取消**终止在途 handler。
            # 吞掉的后果:优雅停机时把取消映射成业务错误码并返回正常响应,
            # 客户端每次滚动更新都收到一批假的业务失败,而在途请求也没有真的排空。
            raise
        except BaseException as exc:  # noqa: BLE001 —— 与 Go 一样把 error 映射成 code
            return chat_pb2.SendMessageResponse(code=_to_proto_code(exc))
        return chat_pb2.SendMessageResponse(code=errcode_pb2.OK, message_id=message_id)

    async def PullHistory(  # noqa: N802
        self,
        request: chat_pb2.PullHistoryRequest,
        context: grpc.aio.ServicerContext,
    ) -> chat_pb2.PullHistoryResponse:
        """拉私聊历史。查询者以鉴权上下文为准(R5)。

        ★ 只有 PRIVATE 有持久化历史;其余频道由 biz 返回空列表(不是报错) ——
        客户端切到世界频道也会调这个接口,报错会让它以为出问题了。
        """
        player_id = _caller_id(context)
        if player_id == 0:
            return chat_pb2.PullHistoryResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            msgs = await self._uc.pull_history(
                player_id,
                request.channel,
                request.peer_id,
                int(request.limit),
                request.before_ms,
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透(理由同上)。
            raise
        except BaseException as exc:  # noqa: BLE001
            return chat_pb2.PullHistoryResponse(code=_to_proto_code(exc))
        return chat_pb2.PullHistoryResponse(code=errcode_pb2.OK, messages=msgs)


# ── 辅助 ──────────────────────────────────────────────────────────────────────


def _caller_id(context: grpc.aio.ServicerContext) -> int:
    """取鉴权上下文里的 player_id。对应 Go 的 `callerID(ctx)`。

    值来自 Envoy jwt_authn 之后注入的 `x-pandora-player-id` header(AuthInterceptor
    已解析)。chat 的两个 RPC 都是客户端面接口,拿不到身份 = 拒绝,
    与 leaderboard 那种"系统接口反过来拒绝带 JWT"的方向相反。
    """
    return interceptors.extract_player_id(context)


def _to_proto_code(err: BaseException) -> int:
    """errcode → proto enum,1:1 数值映射。对应 Go 的 `toProtoCode`。

    数值一致性由 tools/gen_errcode.py 从 Go 源码生成 + tests 里的 parity 测试保证。
    """
    return errcode.as_code(err)
