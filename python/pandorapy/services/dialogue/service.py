"""dialogue gRPC service 层 —— 对应 Go 侧 internal/service/dialogue.go。

职责(与 Go 侧逐条对应):
  - 实现 DialogueServiceServicer
  - 从 metadata 取 JWT player_id(R5:override request 字段,防伪造他人身份)
  - proto Request/Response ↔ biz 入参/出参互转
  - errcode → pandora.common.v1.ErrCode 1:1 映射(纯数值)

协议原则(R5):所有 RPC 强制用鉴权上下文里的 player_id,忽略请求体里的 player_id 字段
(那些字段号已在 proto 里 reserved);player_id=0 → ERR_UNAUTHORIZED。
Envoy jwt_authn 已在路由层 require JWT,这里再拦一次是兜底 —— 直连内网端口联调时
没有网关注入,这道闸是唯一防线。

⚠️ 返回值形态必须和 Go 侧一致:业务失败**不**抛 gRPC 错误,而是返回
   `Response(code=ErrXxx)` 且 gRPC status 为 OK。客户端读的是 body 里的 code。
   若改成 context.abort(),客户端会走到完全不同的错误分支 —— 这是最容易在迁移中
   悄悄改掉的语义。
"""

from __future__ import annotations

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.dialogue.v1 import dialogue_pb2, dialogue_pb2_grpc

from pandorapy import errcode, interceptors
from pandorapy.services.dialogue import biz as pbiz


class DialogueService(dialogue_pb2_grpc.DialogueServiceServicer):
    """实现 DialogueServiceServicer。对应 Go 的 service.DialogueService。"""

    __slots__ = ("_uc", "_snowflake")

    def __init__(self, usecase: pbiz.DialogueUsecase, snowflake: object) -> None:
        self._uc = usecase
        # snowflake 只需要 generate() —— 用最小接口而不是具体类型,
        # 与 Go 侧 snowflakeGen interface 同样的意图(测试可注入假发号器)。
        self._snowflake = snowflake

    async def StartDialogue(  # noqa: N802 —— gRPC 生成的方法名,必须照抄
        self,
        request: dialogue_pb2.StartDialogueRequest,
        context: grpc.aio.ServicerContext,
    ) -> dialogue_pb2.StartDialogueResponse:
        """开启一次 NPC 对话。player_id 以鉴权上下文为准(R5);dialogue_id 服务端生成。"""
        player_id = _caller_id(context)
        if player_id == 0:
            return dialogue_pb2.StartDialogueResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.npc_id == 0:
            return dialogue_pb2.StartDialogueResponse(code=errcode_pb2.ERR_INVALID_ARG)

        try:
            state = self._uc.start_dialogue(
                player_id, request.npc_id, self._snowflake.generate()
            )
        except Exception as exc:  # noqa: BLE001 —— 与 Go 侧一样把 error 映射成 code
            return dialogue_pb2.StartDialogueResponse(code=_to_proto_code(exc))
        return dialogue_pb2.StartDialogueResponse(code=errcode_pb2.OK, state=state)

    async def ChooseOption(  # noqa: N802
        self,
        request: dialogue_pb2.ChooseOptionRequest,
        context: grpc.aio.ServicerContext,
    ) -> dialogue_pb2.ChooseOptionResponse:
        """选择一个选项推进对话。player_id 以鉴权上下文为准(R5)。"""
        player_id = _caller_id(context)
        if player_id == 0:
            return dialogue_pb2.ChooseOptionResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.dialogue_id == 0 or not request.option_id:
            return dialogue_pb2.ChooseOptionResponse(code=errcode_pb2.ERR_INVALID_ARG)

        try:
            state = self._uc.choose_option(
                player_id, request.dialogue_id, request.option_id
            )
        except Exception as exc:  # noqa: BLE001
            return dialogue_pb2.ChooseOptionResponse(code=_to_proto_code(exc))
        return dialogue_pb2.ChooseOptionResponse(code=errcode_pb2.OK, state=state)

    async def EndDialogue(  # noqa: N802
        self,
        request: dialogue_pb2.EndDialogueRequest,
        context: grpc.aio.ServicerContext,
    ) -> dialogue_pb2.EndDialogueResponse:
        """结束对话(幂等)。player_id 以鉴权上下文为准(R5)。"""
        player_id = _caller_id(context)
        if player_id == 0:
            return dialogue_pb2.EndDialogueResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.dialogue_id == 0:
            return dialogue_pb2.EndDialogueResponse(code=errcode_pb2.ERR_INVALID_ARG)

        try:
            self._uc.end_dialogue(player_id, request.dialogue_id)
        except Exception as exc:  # noqa: BLE001
            return dialogue_pb2.EndDialogueResponse(code=_to_proto_code(exc))
        return dialogue_pb2.EndDialogueResponse(code=errcode_pb2.OK)


# ── 辅助 ──────────────────────────────────────────────────────────────────────


def _caller_id(context: grpc.aio.ServicerContext) -> int:
    """取鉴权上下文里的 player_id。对应 Go 的 callerID(ctx)。

    直接从 metadata 取而不是读 contextvars:拦截器虽然也绑了 contextvars(给日志用),
    但身份判定走同一个真源(Envoy 注入的头)更不容易出错 —— 万一拦截器没挂上,
    这里会拿到 0 并 fail-closed,而不是拿到上一个请求残留的值。
    """
    return interceptors.extract_player_id(context)


def _to_proto_code(err: BaseException) -> int:
    """errcode → proto enum,1:1 数值映射。对应 Go 的 toProtoCode。

    数值一致性由 tools/gen_errcode.py 从 Go 源码生成 + tests 里的 parity 测试保证。
    """
    return errcode.as_code(err)
