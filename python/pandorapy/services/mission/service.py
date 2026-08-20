"""mission gRPC service 层 —— 对应 Go 侧 internal/service/mission.go。

职责:
  · 实现 MissionServiceServicer;
  · 客户端 RPC 从鉴权上下文取 player_id(R5:**忽略请求体身份**,防伪造);
  · 系统 RPC(ReportMissionFacts / CompleteAllMissions)systemOnly:带玩家身份一律
    ERR_PERMISSION_DENY(Envoy 侧另有精确 path direct_response 403 双保险);
  · errcode → commonv1.ErrCode 1:1 映射(数值相同),错误 **in-band 返回**
    (transport status 恒 OK)。

★ 为什么错误走 in-band 而不是 gRPC status:mission 的 6 个 RPC 在 Go 侧全部返回
  `(resp{Code: ...}, nil)`。改成 status 会让 UE 客户端的 `resp.code` 分支全部收不到
  —— 客户端只在 transport OK 时读 code,status 非 OK 走的是"网络错误"分支(退避重连)。
  ERR_MISSION_NOT_CLAIMABLE 被当成网络故障重连,是一个静默的行为改变。

★ systemOnly 的方向与客户端 RPC **相反**:客户端 RPC 是"没有身份就拒",
  系统 RPC 是"**有**身份就拒"。写反了不会报错,只会让玩家能自己给自己上报任务事实
  (直接刷满任务进度)。
"""

from __future__ import annotations

from pandora.common.v1 import errcode_pb2
from pandora.mission.v1 import mission_pb2, mission_pb2_grpc

from pandorapy import errcode, interceptors
from pandorapy import log as plog
from pandorapy.services.mission import engine as eng


def _caller_id(context) -> int:  # noqa: ANN001
    """从鉴权上下文取 Envoy jwt_authn 注入的 player_id(取不到 = 0)。

    读的是 metadata 里的 x-pandora-player-id(网关注入,客户端面不可伪造),
    **绝不读请求体** —— proto 里的 player_id 字段号已在 R5 被 reserved。
    """
    return interceptors.extract_player_id(context)


def _to_proto_code(exc: BaseException) -> int:
    """errcode → proto ErrCode(数值 1:1)。非 PandoraError 归 ERR_INTERNAL。

    不能把未知异常映射成 OK:那会让一次数据库故障对客户端表现为"操作成功",
    玩家看到任务没变化却没有任何错误提示。
    """
    if isinstance(exc, errcode.PandoraError):
        return int(exc.code)
    return int(errcode.ErrInternal)


class MissionService(mission_pb2_grpc.MissionServiceServicer):
    """实现 pandora.mission.v1.MissionService。"""

    __slots__ = ("uc", "log")

    def __init__(self, uc) -> None:  # noqa: ANN001 —— biz.MissionUsecase
        self.uc = uc
        self.log = plog.get()

    # ── 客户端 RPC(JWT 身份)──────────────────────────────────────────────

    async def ListMissions(self, request, context):  # noqa: ANN001, N802
        player_id = _caller_id(context)
        if player_id == 0:
            return mission_pb2.ListMissionsResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            active, completed = await self.uc.list_missions(player_id)
        except Exception as exc:  # noqa: BLE001 —— 业务错误 in-band 返回
            return mission_pb2.ListMissionsResponse(code=_to_proto_code(exc))
        return mission_pb2.ListMissionsResponse(
            code=errcode_pb2.OK, active=active, completed=completed
        )

    async def AcceptMission(self, request, context):  # noqa: ANN001, N802
        player_id = _caller_id(context)
        if player_id == 0:
            return mission_pb2.AcceptMissionResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.mission_config_id == 0:
            return mission_pb2.AcceptMissionResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            mission = await self.uc.accept(player_id, request.mission_config_id)
        except Exception as exc:  # noqa: BLE001
            return mission_pb2.AcceptMissionResponse(code=_to_proto_code(exc))
        return mission_pb2.AcceptMissionResponse(code=errcode_pb2.OK, mission=mission)

    async def AbandonMission(self, request, context):  # noqa: ANN001, N802
        player_id = _caller_id(context)
        if player_id == 0:
            return mission_pb2.AbandonMissionResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.mission_config_id == 0:
            return mission_pb2.AbandonMissionResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self.uc.abandon(player_id, request.mission_config_id)
        except Exception as exc:  # noqa: BLE001
            return mission_pb2.AbandonMissionResponse(code=_to_proto_code(exc))
        return mission_pb2.AbandonMissionResponse(code=errcode_pb2.OK)

    async def ClaimMissionReward(self, request, context):  # noqa: ANN001, N802
        player_id = _caller_id(context)
        if player_id == 0:
            return mission_pb2.ClaimMissionRewardResponse(
                code=errcode_pb2.ERR_UNAUTHORIZED
            )
        if request.mission_config_id == 0:
            return mission_pb2.ClaimMissionRewardResponse(
                code=errcode_pb2.ERR_INVALID_ARG
            )
        try:
            await self.uc.claim(player_id, request.mission_config_id)
        except Exception as exc:  # noqa: BLE001
            return mission_pb2.ClaimMissionRewardResponse(code=_to_proto_code(exc))
        return mission_pb2.ClaimMissionRewardResponse(code=errcode_pb2.OK)

    # ── 系统 RPC(内网直连,caller_id==0)──────────────────────────────────

    async def ReportMissionFacts(self, request, context):  # noqa: ANN001, N802
        if _caller_id(context) != 0:
            # 带玩家身份 = 越权尝试(玩家自报事实就能刷满任务)。
            return mission_pb2.ReportMissionFactsResponse(
                code=errcode_pb2.ERR_PERMISSION_DENY
            )
        if (
            request.player_id == 0
            or not request.facts
            or not request.idempotency_key
        ):
            return mission_pb2.ReportMissionFactsResponse(
                code=errcode_pb2.ERR_INVALID_ARG
            )
        facts = [
            eng.Fact(
                category=int(f.condition_category),
                slot_values=tuple(f.condition_ids),
                amount=int(f.amount),
            )
            for f in request.facts
        ]
        try:
            already = await self.uc.report_facts(
                request.player_id, facts, request.idempotency_key
            )
        except Exception as exc:  # noqa: BLE001
            return mission_pb2.ReportMissionFactsResponse(code=_to_proto_code(exc))
        return mission_pb2.ReportMissionFactsResponse(
            code=errcode_pb2.OK, already=already
        )

    async def CompleteAllMissions(self, request, context):  # noqa: ANN001, N802
        if _caller_id(context) != 0:
            return mission_pb2.CompleteAllMissionsResponse(
                code=errcode_pb2.ERR_PERMISSION_DENY
            )
        if request.player_id == 0:
            return mission_pb2.CompleteAllMissionsResponse(
                code=errcode_pb2.ERR_INVALID_ARG
            )
        try:
            count = await self.uc.complete_all(request.player_id)
        except Exception as exc:  # noqa: BLE001
            return mission_pb2.CompleteAllMissionsResponse(code=_to_proto_code(exc))
        return mission_pb2.CompleteAllMissionsResponse(
            code=errcode_pb2.OK, completed_count=count
        )
