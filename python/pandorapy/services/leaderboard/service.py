"""leaderboard gRPC service 层 —— 对应 Go 侧 internal/service/leaderboard.go。

职责:
  - 实现 LeaderboardServiceServicer
  - proto Request/Response ↔ biz / 存储层入参出参互转
  - errcode → pandora.common.v1.ErrCode 1:1 映射(纯数值)

鉴权原则(与 Go 逐条对应,**方向和别的服务相反**):
  - 写入 / 系统 RPC(SubmitScore / SettleBoard / RemoveEntry / DeleteBoard):
    只允许后端内部直连(无 JWT,caller_id==0);**带玩家 JWT 的调用一律拒绝**
    (同 inventory.GrantItems),且不在 Envoy 暴露这些路由。
    ★ 判据是"带了身份就拒",不是"没身份就拒" —— 写反的话玩家能自助刷榜 / 自助发奖,
      而所有日志看起来完全正常。
  - 读 RPC(GetRank / GetRange / GetAround):允许经 Envoy 的客户端调用。

⚠️ 返回值形态必须和 Go 一致:业务失败**不**抛 gRPC 错误,而是返回
   `Response(code=ErrXxx)` 且 gRPC status 为 OK。客户端读的是 body 里的 code。
   改成 context.abort() 会让客户端走到完全不同的错误分支。
"""

from __future__ import annotations

import asyncio

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.leaderboard.v1 import leaderboard_pb2, leaderboard_pb2_grpc

from pandorapy import errcode, interceptors
from pandorapy.services.leaderboard import biz as lbbiz
from pandorapy.services.leaderboard import board_store as bs


class LeaderboardService(leaderboard_pb2_grpc.LeaderboardServiceServicer):
    """实现 LeaderboardServiceServicer。对应 Go 的 service.LeaderboardService。"""

    __slots__ = ("_uc",)

    def __init__(self, uc: lbbiz.LeaderboardUsecase) -> None:
        self._uc = uc

    # ── 写 / 系统接口(拒绝玩家 JWT)────────────────────────────────────────

    async def SubmitScore(  # noqa: N802 —— gRPC 生成的方法名,必须照抄
        self,
        request: leaderboard_pb2.SubmitScoreRequest,
        context: grpc.aio.ServicerContext,
    ) -> leaderboard_pb2.SubmitScoreResponse:
        """上报分数(系统接口)。"""
        if _caller_id(context) != 0:
            return leaderboard_pb2.SubmitScoreResponse(code=errcode_pb2.ERR_PERMISSION_DENY)
        board = _to_board_key(request.board)
        if board is None:
            return leaderboard_pb2.SubmitScoreResponse(
                code=errcode_pb2.ERR_LEADERBOARD_INVALID_BOARD
            )
        try:
            new_score, rank = await self._uc.submit_score(
                board,
                request.entity_id,
                request.score,
                int(request.mode),
                _to_options(request.options),
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透。CancelledError 在 3.8+ 是 BaseException，会被下面那条
            # 宽 except 吞掉 —— 而 grpc.aio 正是**用取消**终止在途 handler。
            # 吞掉的后果:优雅停机时把取消映射成业务错误码并返回正常响应,
            # 客户端每次滚动更新都收到一批假的业务失败,而在途请求也没有真的排空。
            raise
        except BaseException as exc:  # noqa: BLE001 —— 与 Go 一样把 error 映射成 code
            return leaderboard_pb2.SubmitScoreResponse(code=_to_proto_code(exc))
        return leaderboard_pb2.SubmitScoreResponse(
            code=errcode_pb2.OK, new_score=new_score, rank=rank
        )

    async def RemoveEntry(  # noqa: N802
        self,
        request: leaderboard_pb2.RemoveEntryRequest,
        context: grpc.aio.ServicerContext,
    ) -> leaderboard_pb2.RemoveEntryResponse:
        """移除某 entity(系统接口)。"""
        if _caller_id(context) != 0:
            return leaderboard_pb2.RemoveEntryResponse(code=errcode_pb2.ERR_PERMISSION_DENY)
        board = _to_board_key(request.board)
        if board is None:
            return leaderboard_pb2.RemoveEntryResponse(
                code=errcode_pb2.ERR_LEADERBOARD_INVALID_BOARD
            )
        try:
            await self._uc.remove_entry(board, request.entity_id)
        except asyncio.CancelledError:
            # ★ 取消必须穿透。CancelledError 在 3.8+ 是 BaseException，会被下面那条
            # 宽 except 吞掉 —— 而 grpc.aio 正是**用取消**终止在途 handler。
            # 吞掉的后果:优雅停机时把取消映射成业务错误码并返回正常响应,
            # 客户端每次滚动更新都收到一批假的业务失败,而在途请求也没有真的排空。
            raise
        except BaseException as exc:  # noqa: BLE001
            return leaderboard_pb2.RemoveEntryResponse(code=_to_proto_code(exc))
        return leaderboard_pb2.RemoveEntryResponse(code=errcode_pb2.OK)

    async def SettleBoard(  # noqa: N802
        self,
        request: leaderboard_pb2.SettleBoardRequest,
        context: grpc.aio.ServicerContext,
    ) -> leaderboard_pb2.SettleBoardResponse:
        """结算 + 发奖(系统接口)。"""
        if _caller_id(context) != 0:
            return leaderboard_pb2.SettleBoardResponse(code=errcode_pb2.ERR_PERMISSION_DENY)
        board = _to_board_key(request.board)
        if board is None:
            return leaderboard_pb2.SettleBoardResponse(
                code=errcode_pb2.ERR_LEADERBOARD_INVALID_BOARD
            )
        # HasField 判定而不是"tiers 空就当没有":显式传了空 RewardTable 与不传
        # 在 Go 侧行为相同(都不发奖),这里保持同一形态,避免把 None 传进 biz
        # 后再各判一次。
        table = request.reward_table if request.HasField("reward_table") else None
        try:
            res = await self._uc.settle_board(
                board,
                request.top_n,
                table,
                request.reset_after,
                request.settle_idempotency_key,
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透。CancelledError 在 3.8+ 是 BaseException，会被下面那条
            # 宽 except 吞掉 —— 而 grpc.aio 正是**用取消**终止在途 handler。
            # 吞掉的后果:优雅停机时把取消映射成业务错误码并返回正常响应,
            # 客户端每次滚动更新都收到一批假的业务失败,而在途请求也没有真的排空。
            raise
        except BaseException as exc:  # noqa: BLE001
            return leaderboard_pb2.SettleBoardResponse(code=_to_proto_code(exc))
        return leaderboard_pb2.SettleBoardResponse(
            code=errcode_pb2.OK,
            settlement_id=res.settlement_id,
            settled_count=res.settled_count,
            already_settled=res.already_settled,
            winners=_to_entries(res.winners),
        )

    async def DeleteBoard(  # noqa: N802
        self,
        request: leaderboard_pb2.DeleteBoardRequest,
        context: grpc.aio.ServicerContext,
    ) -> leaderboard_pb2.DeleteBoardResponse:
        """删整个榜(系统接口)。"""
        if _caller_id(context) != 0:
            return leaderboard_pb2.DeleteBoardResponse(code=errcode_pb2.ERR_PERMISSION_DENY)
        board = _to_board_key(request.board)
        if board is None:
            return leaderboard_pb2.DeleteBoardResponse(
                code=errcode_pb2.ERR_LEADERBOARD_INVALID_BOARD
            )
        try:
            await self._uc.delete_board(board)
        except asyncio.CancelledError:
            # ★ 取消必须穿透。CancelledError 在 3.8+ 是 BaseException，会被下面那条
            # 宽 except 吞掉 —— 而 grpc.aio 正是**用取消**终止在途 handler。
            # 吞掉的后果:优雅停机时把取消映射成业务错误码并返回正常响应,
            # 客户端每次滚动更新都收到一批假的业务失败,而在途请求也没有真的排空。
            raise
        except BaseException as exc:  # noqa: BLE001
            return leaderboard_pb2.DeleteBoardResponse(code=_to_proto_code(exc))
        return leaderboard_pb2.DeleteBoardResponse(code=errcode_pb2.OK)

    # ── 读接口(允许客户端)──────────────────────────────────────────────

    async def GetRank(  # noqa: N802
        self,
        request: leaderboard_pb2.GetRankRequest,
        context: grpc.aio.ServicerContext,
    ) -> leaderboard_pb2.GetRankResponse:
        """查名次。精确榜命中回精确名次;被截断玩家回区间估算(estimated=true)。"""
        board = _to_board_key(request.board)
        if board is None:
            return leaderboard_pb2.GetRankResponse(
                code=errcode_pb2.ERR_LEADERBOARD_INVALID_BOARD
            )
        try:
            view = await self._uc.get_rank(board, request.entity_id)
        except asyncio.CancelledError:
            # ★ 取消必须穿透。CancelledError 在 3.8+ 是 BaseException，会被下面那条
            # 宽 except 吞掉 —— 而 grpc.aio 正是**用取消**终止在途 handler。
            # 吞掉的后果:优雅停机时把取消映射成业务错误码并返回正常响应,
            # 客户端每次滚动更新都收到一批假的业务失败,而在途请求也没有真的排空。
            raise
        except BaseException as exc:  # noqa: BLE001
            return leaderboard_pb2.GetRankResponse(code=_to_proto_code(exc))
        resp = leaderboard_pb2.GetRankResponse(code=errcode_pb2.OK, found=view.found)
        if view.found and view.entry is not None:
            resp.entry.CopyFrom(_to_entry(view.entry))
            resp.estimated = view.estimated
            resp.total_submitters = view.total_submitters
        return resp

    async def GetRange(  # noqa: N802
        self,
        request: leaderboard_pb2.GetRangeRequest,
        context: grpc.aio.ServicerContext,
    ) -> leaderboard_pb2.GetRangeResponse:
        """取榜区间。"""
        board = _to_board_key(request.board)
        if board is None:
            return leaderboard_pb2.GetRangeResponse(
                code=errcode_pb2.ERR_LEADERBOARD_INVALID_BOARD
            )
        try:
            entries, total = await self._uc.get_range(
                board, request.offset, int(request.limit)
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透。CancelledError 在 3.8+ 是 BaseException，会被下面那条
            # 宽 except 吞掉 —— 而 grpc.aio 正是**用取消**终止在途 handler。
            # 吞掉的后果:优雅停机时把取消映射成业务错误码并返回正常响应,
            # 客户端每次滚动更新都收到一批假的业务失败,而在途请求也没有真的排空。
            raise
        except BaseException as exc:  # noqa: BLE001
            return leaderboard_pb2.GetRangeResponse(code=_to_proto_code(exc))
        return leaderboard_pb2.GetRangeResponse(
            code=errcode_pb2.OK, entries=_to_entries(entries), total=total
        )

    async def GetAround(  # noqa: N802
        self,
        request: leaderboard_pb2.GetAroundRequest,
        context: grpc.aio.ServicerContext,
    ) -> leaderboard_pb2.GetAroundResponse:
        """取上下 N 名。"""
        board = _to_board_key(request.board)
        if board is None:
            return leaderboard_pb2.GetAroundResponse(
                code=errcode_pb2.ERR_LEADERBOARD_INVALID_BOARD
            )
        try:
            entries, found = await self._uc.get_around(
                board, request.entity_id, int(request.radius)
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透。CancelledError 在 3.8+ 是 BaseException，会被下面那条
            # 宽 except 吞掉 —— 而 grpc.aio 正是**用取消**终止在途 handler。
            # 吞掉的后果:优雅停机时把取消映射成业务错误码并返回正常响应,
            # 客户端每次滚动更新都收到一批假的业务失败,而在途请求也没有真的排空。
            raise
        except BaseException as exc:  # noqa: BLE001
            return leaderboard_pb2.GetAroundResponse(code=_to_proto_code(exc))
        return leaderboard_pb2.GetAroundResponse(
            code=errcode_pb2.OK, entries=_to_entries(entries), found=found
        )


# ── 转换辅助 ──────────────────────────────────────────────────────────────────


def _caller_id(context: grpc.aio.ServicerContext) -> int:
    """取鉴权上下文里的 player_id。对应 Go 的 pmw.PlayerIDFromContext(ctx)。

    直接从 metadata 取而不是读 contextvars:万一拦截器没挂上,这里会拿到 0,
    对**读**接口是放行(与 Go 同)、对**写**接口则是"当成内部直连放行" ——
    所以这些系统 RPC 绝不能在 Envoy 暴露路由,那才是第一道边界。
    """
    return interceptors.extract_player_id(context)


def _to_board_key(pb) -> bs.BoardKey | None:  # noqa: ANN001
    """proto BoardKey → 存储层 BoardKey;board_type==0 / scope 非法 → None。"""
    if pb is None or pb.board_type == 0:
        return None
    scope = int(pb.scope)
    if scope < bs.SCOPE_GLOBAL or scope > bs.SCOPE_CUSTOM:
        return None
    return bs.BoardKey(
        board_type=pb.board_type,
        scope=scope,
        scope_id=pb.scope_id,
        period=pb.period,
    )


def _to_options(pb) -> bs.Options:  # noqa: ANN001
    """proto BoardOptions → 存储层 Options。"""
    if pb is None:
        return bs.Options()
    return bs.Options(
        ttl_seconds=pb.ttl_seconds,
        max_size=pb.max_size,
        tie_break_by_time=pb.tie_break_by_time,
        ascending=pb.ascending,
        estimate_bucket_width=pb.estimate_bucket_width,
    )


def _to_entry(e: bs.Entry) -> leaderboard_pb2.LeaderboardEntry:
    return leaderboard_pb2.LeaderboardEntry(
        entity_id=e.entity_id, score=e.score, rank=e.rank, updated_at_ms=e.updated_at_ms
    )


def _to_entries(entries: list[bs.Entry]) -> list[leaderboard_pb2.LeaderboardEntry]:
    return [_to_entry(e) for e in entries]


def _to_proto_code(err: BaseException) -> int:
    """errcode → proto enum,1:1 数值映射。对应 Go 的 toProtoCode。

    数值一致性由 tools/gen_errcode.py 从 Go 源码生成 + tests 里的 parity 测试保证。
    """
    return errcode.as_code(err)
