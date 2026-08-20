"""trade gRPC service 层 —— 对应 Go 侧 internal/service/trade.go。

协议原则(R5):CreateOrder 的 seller_id、Confirm/Cancel/List 的 player_id 一律以
鉴权上下文里的 player_id 为准,**忽略请求体里的对应字段**;player_id=0 → ERR_UNAUTHORIZED。

⚠️ 与 dialogue 同一条铁律:业务失败**不**抛 gRPC 错误,而是返回 Response(code=ErrXxx)
   且 gRPC status 为 OK。客户端读的是 body 里的 code。
   注意 ConfirmOrder 失败时**仍要带上 new_state** —— Go 侧如此(客户端要知道订单
   现在停在哪个状态才能决定是否重试),漏了这个字段客户端会以为状态未变。
"""

from __future__ import annotations

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.trade.v1 import trade_pb2, trade_pb2_grpc

from pandorapy import errcode, interceptors
from pandorapy.services.trade import biz as tbiz


class TradeService(trade_pb2_grpc.TradeServiceServicer):
    """实现 TradeServiceServicer。"""

    __slots__ = ("_uc",)

    def __init__(self, usecase: tbiz.TradeUsecase) -> None:
        self._uc = usecase

    async def CreateOrder(  # noqa: N802
        self, request: trade_pb2.CreateOrderRequest, context: grpc.aio.ServicerContext
    ) -> trade_pb2.CreateOrderResponse:
        """卖方挂单。seller 以鉴权上下文为准(R5),忽略请求体里的 seller_id。"""
        seller_id = interceptors.extract_player_id(context)
        if seller_id == 0:
            return trade_pb2.CreateOrderResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            order_id = await self._uc.create_order(
                seller_id,
                request.buyer_id,
                request.items,
                request.buyer_items,
                request.price,
            )
        except Exception as exc:  # noqa: BLE001
            return trade_pb2.CreateOrderResponse(code=errcode.as_code(exc))
        return trade_pb2.CreateOrderResponse(code=errcode_pb2.OK, order_id=order_id)

    async def ConfirmOrder(  # noqa: N802
        self, request: trade_pb2.ConfirmOrderRequest, context: grpc.aio.ServicerContext
    ) -> trade_pb2.ConfirmOrderResponse:
        """确认订单(两阶段)。失败也要回传 new_state —— 客户端据此决定是否重试。"""
        player_id = interceptors.extract_player_id(context)
        if player_id == 0:
            return trade_pb2.ConfirmOrderResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.order_id == 0:
            return trade_pb2.ConfirmOrderResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            new_state = await self._uc.confirm_order(player_id, request.order_id)
        except tbiz.errcode.PandoraError as exc:
            # ★ 失败路径也必须回真实 new_state,与 Go service/trade.go:61-62 一致
            # (它把 biz 的 `(newState, err)` 里的 newState 原样透传)。
            # 客户端拿 code 判"成没成"、拿 new_state 判"该不该重试":
            # SELLER_CONFIRMED = 结算可能已生效,必须重试;FAILED/EXPIRED = 终态,别重试。
            # exc.order_state 是 PandoraError 的**声明式** slot,biz 在四条路径上显式挂;
            # 没挂的路径它是 0 = UNSPECIFIED,与 Go 的零值路径(trade.go:355)一致。
            return trade_pb2.ConfirmOrderResponse(
                code=errcode.as_code(exc),
                new_state=exc.order_state,
            )
        except Exception as exc:  # noqa: BLE001
            return trade_pb2.ConfirmOrderResponse(code=errcode.as_code(exc))
        return trade_pb2.ConfirmOrderResponse(code=errcode_pb2.OK, new_state=new_state)

    async def CancelOrder(  # noqa: N802
        self, request: trade_pb2.CancelOrderRequest, context: grpc.aio.ServicerContext
    ) -> trade_pb2.CancelOrderResponse:
        player_id = interceptors.extract_player_id(context)
        if player_id == 0:
            return trade_pb2.CancelOrderResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        if request.order_id == 0:
            return trade_pb2.CancelOrderResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self._uc.cancel_order(player_id, request.order_id)
        except Exception as exc:  # noqa: BLE001
            return trade_pb2.CancelOrderResponse(code=errcode.as_code(exc))
        return trade_pb2.CancelOrderResponse(code=errcode_pb2.OK)

    async def ListMyOrders(  # noqa: N802
        self, request: trade_pb2.ListMyOrdersRequest, context: grpc.aio.ServicerContext
    ) -> trade_pb2.ListMyOrdersResponse:
        player_id = interceptors.extract_player_id(context)
        if player_id == 0:
            return trade_pb2.ListMyOrdersResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            orders, next_cursor = await self._uc.list_my_orders(
                player_id, request.active_only, request.cursor, request.limit
            )
        except Exception as exc:  # noqa: BLE001
            return trade_pb2.ListMyOrdersResponse(code=errcode.as_code(exc))
        return trade_pb2.ListMyOrdersResponse(
            code=errcode_pb2.OK, orders=orders, next_cursor=next_cursor
        )
