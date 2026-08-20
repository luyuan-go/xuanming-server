"""battle_result gRPC service 层 —— 对应 Go 侧 internal/service/battle_result.go。

职责:
  - 实现 BattleResultServiceServicer(4 个 RPC)
  - proto Request/Response ↔ biz 入参出参互转
  - errcode → pandora.common.v1.ErrCode 1:1 映射(纯数值)

★ 返回值形态必须和 Go 一致:业务失败**不**抛 gRPC 错误,而是返回
  `Response(code=ErrXxx)` 且 gRPC status 为 OK。客户端(这里是 DS)读的是 body 里的 code。
  改成 context.abort() 会让 DS 走到完全不同的错误分支 —— 而 DS 对 in-band code 的处置
  是"按同一 match_id 重试",对 transport error 的处置是别的。

★ 鉴权原则(与 Go 逐条对应):
  ReportResult / ReportProgress 是**纯 DS 回调**,经 Envoy :8444 进来。该监听器没有
  jwt_authn,所以 `x-pandora-player-id` 恒缺失 —— 「callerID==0 即后端内部可信」在这两个
  方法上**不成立**。守卫用 `require_token=True`:enforce 档下无令牌直连一律拒,堵住
  绕过 Envoy 直连业务端口的东西向旁路。令牌 match_id 必须等于上报的 match_id
  (防拿 A 局令牌伪造 B 局结算)。

★ 玩家身份:本服的四个 RPC **都不从鉴权上下文取 player_id**(R5 的"取鉴权上下文"
  针对客户端面服务)。ListPlayerHistory 的 player_id 来自请求体,这是**刻意的**:
  它是后端内部 / 运维查询接口,Go 侧同样如此,且 Envoy 未对客户端暴露该路由。
  如果将来要对客户端开放,必须改成从鉴权上下文取,否则任何玩家都能查别人的战绩。

★ **ReportProgress 在 Python 侧未实现**(实时进度通道整体未迁移,见 main.py 的启动 WARN)。
  返回 ERR_INVALID_STATE —— 这不是随手挑的码,而是 Go 在 `progress_enabled=false` 时
  返回的**同一个码**,DS 收到即停流并回退到局后结算路径(realtime-progression.md)。
  选 ERR_NOT_IMPLEMENTED 会让 DS 走到没有约定处置的分支;选 OK 会让 DS 以为事实已入账
  (那才是真正会丢经验和掉落的选择)。
"""

from __future__ import annotations

import asyncio

import grpc
from pandora.battle.v1 import battle_pb2, battle_pb2_grpc
from pandora.common.v1 import errcode_pb2

from pandorapy import dsauth, errcode
from pandorapy import log as plog
from pandorapy.protoenum import enum_name
from pandorapy.services.battle_result import biz as bbiz

GRPC_SERVICE_FULL_NAME = "pandora.battle.v1.BattleResultService"

# DS 令牌里的类型标识(pkg/auth.DSTypeBattle)。填错会让**所有**战斗 DS 回调被判
# ds_type mismatch —— 而 permissive 档下它只是一条 warn,enforce 档下才成批拒。
DS_TYPE_BATTLE = "battle"


class BattleResultService(battle_pb2_grpc.BattleResultServiceServicer):
    """实现 BattleResultServiceServicer。对应 Go 的 service.BattleResultService。"""

    __slots__ = ("_uc", "_ds_guard")

    def __init__(self, uc: bbiz.BattleResultUsecase) -> None:
        self._uc = uc
        # None = mode=off,不校验(与 Go 的 dsGuard==nil 等价)。
        self._ds_guard: dsauth.DSCallbackGuard | None = None

    def set_ds_callback_guard(self, guard: dsauth.DSCallbackGuard | None) -> None:
        """注入 DS 回调令牌守卫(main 按 ds_auth 配置构建;None 表示 off)。"""
        self._ds_guard = guard

    # ── DS 回调面 ─────────────────────────────────────────────────────────

    async def ReportResult(  # noqa: N802 —— gRPC 生成的方法名,必须照抄
        self,
        request: battle_pb2.ReportResultRequest,
        context: grpc.aio.ServicerContext,
    ) -> battle_pb2.ReportResultResponse:
        """同步上报一场对局结算(幂等)。"""
        result = request.result
        if not request.HasField("result") or result.match_id == 0:
            plog.get().warning(
                "ds_report_result_rejected",
                reason="missing_match_id",
                hint="DS 上报结算缺 result / match_id,请求在鉴权前就被拒",
            )
            return battle_pb2.ReportResultResponse(code=errcode_pb2.ERR_INVALID_ARG)

        # §11.3 R3(**诚实标注的差异**):Go 在这里做 `ctx = plog.WithMatchID(ctx, id)`,
        # 让本请求后续所有日志自动带上 match_id 这个 join key。
        # 共享的 pandorapy/log.py 有 `match_id` contextvar,但**没有导出 bind_match_id**,
        # 而共享件本轮禁改 —— 所以这里改成在每条日志上显式写 match_id(Go 的 biz 层
        # 其实也逐条手写)。缺口只落在 repo 层那些没手写 match_id 的日志上。
        # 补齐办法见交付说明 shared_files_needed:log.py 导出 bind_match_id 后,
        # 把本段换回一次绑定即可。
        # 链路第一站留痕(§11.3 R1):没有这条时,「打完没结算」分不清是 DS 根本没上报,
        # 还是上报了被后面某道门拒掉(鉴权码全是业务码,access log 只记 rpc_ok/DEBUG)。
        plog.get().info(
            "battle_result_received",
            match_id=result.match_id,
            ds_pod_name=result.ds_pod_name,
            players=len(result.stats),
            winner_team=result.winner_team,
            outcome=enum_name(battle_pb2.BattleOutcome, result.outcome),
            final_progress_seq=request.final_progress_seq,
        )
        code = self._check_battle_credential(context, result.match_id)
        if code != 0:
            _log_ds_auth_reject(
                "ReportResult", "check_credential", result.match_id, result.ds_pod_name, code
            )
            return battle_pb2.ReportResultResponse(code=code)

        try:
            already = await self._uc.report_result(result, request.final_progress_seq)
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
            # 吞掉之后取消就**不再传播** —— grpc.aio 正是用取消终止在途 handler,
            # 吞了会把取消变成一个正常应答,而在途请求也没有真的排空(§9.16)。
            raise
        except BaseException as exc:  # noqa: BLE001 —— 与 Go 一样把 error 映射成 code
            return battle_pb2.ReportResultResponse(code=_to_proto_code(exc))
        return battle_pb2.ReportResultResponse(
            code=errcode_pb2.OK, already_recorded=already
        )

    async def ReportProgress(  # noqa: N802
        self,
        request: battle_pb2.ReportProgressRequest,
        context: grpc.aio.ServicerContext,
    ) -> battle_pb2.ReportProgressResponse:
        """战斗中实时进度事实上报。

        ⚠️ **Python 侧未实现实时进度通道**。这里仍然完整跑鉴权链再拒:
        跳过鉴权直接拒会让"伪造令牌的 DS"和"合法 DS 撞上未实现"在日志里长一个样,
        而前者是安全信号。

        拒绝码 ERR_INVALID_STATE 与 Go 在 `progress_enabled=false` 时返回的**完全相同**,
        DS 收到即停流、回退到局后结算路径。acked_seq=0 表示"一条都没入账"。
        """
        if request.match_id == 0 or not request.events:
            plog.get().warning(
                "ds_report_progress_rejected",
                reason="missing_match_id_or_events",
                match_id=request.match_id,
                events=len(request.events),
            )
            return battle_pb2.ReportProgressResponse(code=errcode_pb2.ERR_INVALID_ARG)

        code = self._check_battle_credential(context, request.match_id)
        if code != 0:
            _log_ds_auth_reject("ReportProgress", "check_credential", request.match_id, "", code)
            return battle_pb2.ReportProgressResponse(code=code)
        # 每批一条 WARN(不是每事件):量级 = DS 批次数,不构成噪音,
        # 而"DS 一直在发进度但服务端一条都没收"必须在服务端可见 ——
        # 只在 DS 侧可见的话,运维会把它当成网络问题排查。
        plog.get().warning(
            "battle_progress_channel_unavailable",
            match_id=request.match_id,
            events=len(request.events),
            hint="Python 版 battle_result 未实现实时进度通道;DS 应停流并回退局后结算路径"
            "(与 progress_enabled=false 同一处置)。需要实时通道请用 Go 版跑本服务",
        )
        # acked_seq=0 = 一条都没入账。带非零 acked_seq 会让 UE 释放 action claim,
        # 那是"服务端已确定处理过这个 seq"的语义 —— 这里没有。
        return battle_pb2.ReportProgressResponse(
            code=errcode_pb2.ERR_INVALID_STATE, acked_seq=0
        )

    # ── 查询面 ────────────────────────────────────────────────────────────

    async def GetMatchResult(  # noqa: N802
        self,
        request: battle_pb2.GetMatchResultRequest,
        context: grpc.aio.ServicerContext,  # noqa: ARG002
    ) -> battle_pb2.GetMatchResultResponse:
        """查询一场对局结算。"""
        if request.match_id == 0:
            return battle_pb2.GetMatchResultResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            res = await self._uc.get_match_result(request.match_id)
        except asyncio.CancelledError:
            # ★ 取消必须穿透(理由同 ReportResult)。
            raise
        except BaseException as exc:  # noqa: BLE001
            return battle_pb2.GetMatchResultResponse(code=_to_proto_code(exc))
        if res is None:
            # ★ "查不到"是 ERR_NOT_FOUND,不是 OK+空 result:后者会让调用方
            #   把"这局还没结算"和"这局平局零战绩"混成一件事。
            return battle_pb2.GetMatchResultResponse(code=errcode_pb2.ERR_NOT_FOUND)
        return battle_pb2.GetMatchResultResponse(code=errcode_pb2.OK, result=res)

    async def ListPlayerHistory(  # noqa: N802
        self,
        request: battle_pb2.ListPlayerHistoryRequest,
        context: grpc.aio.ServicerContext,  # noqa: ARG002
    ) -> battle_pb2.ListPlayerHistoryResponse:
        """倒序列出玩家战绩历史。"""
        if request.player_id == 0:
            return battle_pb2.ListPlayerHistoryResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            results = await self._uc.list_player_history(
                request.player_id, int(request.limit), request.before_ms
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return battle_pb2.ListPlayerHistoryResponse(code=_to_proto_code(exc))
        return battle_pb2.ListPlayerHistoryResponse(code=errcode_pb2.OK, results=results)

    # ── 内部 ──────────────────────────────────────────────────────────────

    def _check_battle_credential(self, context, match_id: int) -> int:  # noqa: ANN001
        """DS 回调范围绑定:battle 令牌的 match_id 必须等于上报的 match_id。

        返回 0 = 放行;非 0 = 应回给 DS 的 in-band ErrCode。

        `require_token=True` 是**纯 DS 回调**的标记:enforce 档下无令牌直连一律拒
        (堵住绕过 Envoy 的东西向旁路,审核 P1)。mode=off 时 guard 为 None,直接放行。
        """
        if self._ds_guard is None:
            return 0
        return self._ds_guard.check(
            context,
            dsauth.DSScope(ds_type=DS_TYPE_BATTLE, match_id=match_id, require_token=True),
        )


def _log_ds_auth_reject(
    rpc: str, stage: str, match_id: int, reported_pod: str, code: int
) -> None:
    """记录 DS 回调链的鉴权 / fencing 拒绝(僵尸 / 伪造 / 失租 / 换 pod 的 DS 拿旧票上报)。

    这些拒绝码是业务范围(ErrUnauthorized / ErrPermissionDeny),经 in-band code +
    OK transport 返回,access-log 中间件按 rpc_ok(DEBUG)记录、线上 info 级静默 ——
    但它们正是 §9.6 / §9.22 要能查到的安全信号,故在拒绝点显式打 WARN 留证。
    """
    kv: dict[str, object] = {"rpc": rpc, "stage": stage, "match_id": match_id, "code": code}
    if reported_pod:
        kv["reported_pod"] = reported_pod
    plog.get().warning("ds_auth_rejected", **kv)


def _to_proto_code(exc: BaseException) -> int:
    """把 pkg/errcode 1:1 映射成 proto enum(数值相同)。对应 Go 的 toProtoCode。"""
    return errcode.as_code(exc)
