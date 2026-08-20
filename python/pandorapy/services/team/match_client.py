"""team → matchmaker 的 gRPC 客户端 —— 对应 Go 侧 internal/data/match_canceler.go。

两个能力都只跟 matchmaker 说话,共用同一条连接:

  - `cancel_match`(biz.MatchCanceler):成员离队 / 被踢时联动撤销其所在的匹配票据。
    team 只知道队伍成员、不知道 matchmaker 的 ticket;按 player_id 撤销即可 ——
    matchmaker 由 player→ticket 归属(SETNX claim)定位该成员所在**整张**票据:
    仍在排队 → CAS 删票 + 释放全队 claim;已进确认期 → 等价该玩家拒绝确认(退票)。
  - `is_player_committed_to_match`(biz.MatchCommitmentReader):入队闸门 / 自动摘人闸②③
    的判定依据。

★ 两个方法的鉴权是**刻意不对称**的,别"顺手统一":
    ResolvePlayerMatchContext → matchmaker 侧**强制验签**(caller="team",独立密钥),
                                不签一律 ERR_PERMISSION_DENY(7);
    CancelMatch               → 走 AuthOptional 内部路径(callerID==0 + 请求内 player_id),
                                不需要也不应该签。
  给 CancelMatch 也签上不会更安全,只会在 matchmaker 侧多出一条它并不校验的元数据。

★ 三态严格区分,**不准把 UNKNOWN 压成 False**(§9.22):
    ACTIVE → True;NONE → False;UNSPECIFIED / 非 OK code / RPC error → **抛错**,
    由调用方 fail-closed。压成 False 的后果是「matchmaker 抖一下就把一支正在打的队伍拆了」。
"""

from __future__ import annotations

import asyncio

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.match.v1 import match_pb2, match_pb2_grpc

from pandorapy import errcode
from pandorapy import internalrpcauth
from pandorapy import log as plog

# team→matchmaker 跨服务边界的拒绝 reason 枚举(§11.3 R2)。
#
# 这条边界此前零日志:玩家「离队了但票据还在排队」「明明在打却被判成空闲」两类问题
# 在 team 与 matchmaker 两侧日志里都断线,只能靠猜。取值与 Go 侧逐字相同。
REASON_CANCEL_RPC_FAILED = "matchmaker_cancel_rpc_failed"
REASON_CANCEL_CODE_NOT_OK = "matchmaker_cancel_code_not_ok"
REASON_COMMIT_SIGN_FAILED = "resume_auth_sign_failed"
REASON_COMMIT_RPC_FAILED = "matchmaker_resolve_rpc_failed"
REASON_COMMIT_CODE_NOT_OK = "matchmaker_resolve_code_not_ok"
REASON_COMMIT_STATE_UNKNOWN = "matchmaker_resolve_state_unknown"
REASON_SIGNER_NOT_CONFIGURED = "resume_auth_signer_not_configured"

# 与 Go 的 matchv1.MatchService_ResolvePlayerMatchContext_FullMethodName 同值。
# 签名把方法名算进 canonical string,写错一个字母 matchmaker 侧必然验不过。
RESOLVE_PLAYER_MATCH_CONTEXT_METHOD = (
    "/pandora.match.v1.MatchService/ResolvePlayerMatchContext"
)


class GrpcMatchClient:
    """直连 matchmaker 服务 endpoint(host:port,内网 insecure)。

    signer 可为 None(未配 match_resume_auth_secret):此时
    `is_player_committed_to_match` 照常发出,matchmaker 必回 code=7,按三态约定落到
    抛错分支 fail-closed —— **而不是**被静默当成"没在对局中"。
    """

    __slots__ = ("_channel", "_stub", "_signer", "_timeout_sec")

    def __init__(
        self,
        matchmaker_addr: str,
        signer: internalrpcauth.Signer | None,
        *,
        timeout_sec: float = 5.0,
    ) -> None:
        self._channel = grpc.aio.insecure_channel(matchmaker_addr)
        self._stub = match_pb2_grpc.MatchServiceStub(self._channel)
        self._signer = signer
        self._timeout_sec = timeout_sec

    async def close(self) -> None:
        await self._channel.close()

    async def cancel_match(self, player_id: int) -> None:
        """撤销 player_id 当前所在的匹配票据(整张票据,含全体队友)。

        玩家未在排队时 matchmaker 返回 ErrMatchNotFound(4001),照常抛出,
        由调用方(biz.cancel_matchmaking)按常态忽略。
        """
        logger = plog.get()
        try:
            resp = await self._stub.CancelMatch(
                match_pb2.CancelMatchRequest(player_id=player_id),
                timeout=self._timeout_sec,
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
            # 吞掉之后取消就不再传播 —— grpc.aio 用取消终止在途 handler,吞了会把取消
            # 变成一个正常应答,让 §9.16 的「先摘流量 → 再排空在途」失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            # 弱依赖:调用方只 warn 不阻断离队,残留票据靠确认期超时 / TTL 兜底。
            # 但「离队后仍被匹配拉进对局」的第一嫌疑就是这里,必须留证。
            logger.warning(
                "match_cancel_failed",
                player_id=player_id,
                reason=REASON_CANCEL_RPC_FAILED,
                err=str(exc),
                hint="票据未撤销,等确认期超时 / TTL 回收",
            )
            raise
        code = int(resp.code)
        if code != errcode_pb2.OK:
            if code == errcode.ErrMatchNotFound:
                # 4001 = 玩家本就不在排队,是离队路径的常态(每次离队都会调一次),
                # 走 debug 不刷屏。
                logger.debug(
                    "match_cancel_noop",
                    player_id=player_id,
                    reason="player_not_in_queue",
                )
            else:
                logger.warning(
                    "match_cancel_failed",
                    player_id=player_id,
                    reason=REASON_CANCEL_CODE_NOT_OK,
                    code=code,
                )
            raise errcode.PandoraError(
                code, "matchmaker.CancelMatch code=%d player=%d", code, player_id
            )
        # §11.3 R1:撤票是不可逆状态推进,且是「排队中离队」链路上 team 侧唯一的成功证据。
        logger.info("match_canceled", player_id=player_id)

    async def is_player_committed_to_match(self, player_id: int) -> bool:
        """player_id 当前是否已被一场对局占住。

        权威来源是 matchmaker 的 start 索引 + player→ticket claim(ClaimPlayer 用无 TTL 的
        SETNX 写,只有显式取消 / 失败 / battle_result 调 ReleaseMatch 才删),因此
        STARTING / 排队 / 确认期 / 拉 DS / **整场战斗** 全窗口都会判定为已占住。
        """
        logger = plog.get()
        metadata: list[tuple[str, str]] = []
        if self._signer is not None:
            # 每次调用签一份新鲜的 request-bound 凭证(方法 + player_id + 时间戳 + 一次性
            # nonce)。
            try:
                metadata = self._signer.sign_metadata(
                    RESOLVE_PLAYER_MATCH_CONTEXT_METHOD, player_id
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.warning(
                    "match_commitment_query_failed",
                    player_id=player_id,
                    reason=REASON_COMMIT_SIGN_FAILED,
                    err=str(exc),
                    hint="调用方按 UNKNOWN fail-closed,玩家会被判为「可能在对局中」",
                )
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "sign matchmaker resume-auth credential for player %d",
                    player_id,
                    cause=exc,
                ) from exc
        else:
            # 未配密钥 = 本部署恒查不到对局占用,matchmaker 必回 code=7。
            # 这是部署配置缺口,不是偶发故障,单独一个 reason 便于一眼区分。
            logger.warning(
                "match_commitment_query_unsigned",
                player_id=player_id,
                reason=REASON_SIGNER_NOT_CONFIGURED,
                hint="matchmaker 将回 ERR_PERMISSION_DENY(7),调用方 fail-closed",
            )

        try:
            resp = await self._stub.ResolvePlayerMatchContext(
                match_pb2.ResolvePlayerMatchContextRequest(player_id=player_id),
                metadata=metadata or None,
                timeout=self._timeout_sec,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.warning(
                "match_commitment_query_failed",
                player_id=player_id,
                reason=REASON_COMMIT_RPC_FAILED,
                err=str(exc),
            )
            raise
        code = int(resp.code)
        if code != errcode_pb2.OK:
            logger.warning(
                "match_commitment_query_failed",
                player_id=player_id,
                reason=REASON_COMMIT_CODE_NOT_OK,
                code=code,
                signed=self._signer is not None,
            )
            raise errcode.PandoraError(
                code,
                "matchmaker.ResolvePlayerMatchContext code=%d player=%d",
                code,
                player_id,
            )
        state = resp.state
        if state == match_pb2.PLAYER_MATCH_CONTEXT_STATE_ACTIVE:
            # §11.3 R4:这是查询路径(离队 / 恢复判定都会调),成功侧走 debug 不刷屏。
            logger.debug(
                "match_commitment_queried", player_id=player_id, committed=True
            )
            return True
        if state == match_pb2.PLAYER_MATCH_CONTEXT_STATE_NONE:
            logger.debug(
                "match_commitment_queried", player_id=player_id, committed=False
            )
            return False
        # UNKNOWN:读取错误 / 索引漂移 / 坏记录。不确定就是不确定,交给调用方 fail-closed。
        # 这条必须可见 —— 它意味着玩家被判成「可能在对局中」而被挡住,且原因在 matchmaker 侧。
        logger.warning(
            "match_commitment_query_failed",
            player_id=player_id,
            reason=REASON_COMMIT_STATE_UNKNOWN,
            state=int(state),
            hint="matchmaker 侧索引漂移 / 坏记录,调用方 fail-closed",
        )
        raise errcode.PandoraError(
            errcode.ErrUnavailable,
            "matchmaker.ResolvePlayerMatchContext unknown state for player %d",
            player_id,
        )
