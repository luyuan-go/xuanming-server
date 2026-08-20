"""auction gRPC service 层 —— 对应 Go 侧 internal/service/auction.go。

职责:
  - 实现 AuctionServiceServicer
  - 从鉴权上下文取 player_id(R5:**override request**,防伪造他人身份)
  - proto Request/Response ↔ biz 入参/出参互转
  - errcode → pandora.common.v1.ErrCode 1:1 数值映射

★ 协议原则(R5,全部五个 RPC 一致):PlaceOrder 的 seller、Bid 的 buyer、
  Cancel / ListMyOrders 的 player 一律以**鉴权上下文**里的 player_id 为准,
  **忽略请求体里的对应字段**;player_id=0 → ERR_UNAUTHORIZED。
  proto 里本来就没有这些字段(R5 已删),这里再兜一层。

⚠️ 返回值形态必须和 Go 一致:业务失败**不**抛 gRPC 错误,而是返回
   `Response(code=ErrXxx)` 且 gRPC status 为 OK。客户端读的是 body 里的 code。
   改成 context.abort() 会让客户端走到完全不同的错误分支(通常是"网络错误"重试),
   而拍卖行的失败大多是**不该重试**的(余额不足、幂等冲突、不是本人)。
"""

from __future__ import annotations

import asyncio

import grpc
from pandora.auction.v1 import auction_pb2, auction_pb2_grpc
from pandora.common.v1 import errcode_pb2

from pandorapy import errcode, interceptors
from pandorapy.services.auction import biz as abiz

GRPC_SERVICE_FULL_NAME = "pandora.auction.v1.AuctionService"


class AuctionService(auction_pb2_grpc.AuctionServiceServicer):
    """实现 AuctionServiceServicer。对应 Go 的 service.AuctionService。"""

    __slots__ = ("_uc",)

    def __init__(self, uc: abiz.AuctionUsecase) -> None:
        self._uc = uc

    async def PlaceOrder(  # noqa: N802 —— gRPC 生成的方法名,必须照抄
        self,
        request: auction_pb2.PlaceOrderRequest,
        context: grpc.aio.ServicerContext,
    ) -> auction_pb2.PlaceOrderResponse:
        """卖家挂单。seller 以鉴权上下文为准(R5)。"""
        owner_id = _caller_id(context)
        if owner_id == 0:
            return auction_pb2.PlaceOrderResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            order = await self._uc.place_order(
                owner_id,
                request.market_id,
                request.item_config_id,
                request.quantity,
                request.price,
                request.idempotency_key,
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透。CancelledError 在 3.8+ 是 BaseException,会被下面那条
            # 宽 except 吞掉 —— 而 grpc.aio 正是**用取消**终止在途 handler。
            # 吞掉的后果:优雅停机时把取消映射成业务错误码并返回正常响应,
            # 客户端每次滚动更新都收到一批假的业务失败,而在途请求也没有真的排空。
            raise
        except BaseException as exc:  # noqa: BLE001 —— 与 Go 一样把 error 映射成 code
            return auction_pb2.PlaceOrderResponse(code=_to_proto_code(exc))
        return auction_pb2.PlaceOrderResponse(
            code=errcode_pb2.OK,
            order_id=order.order_id,
            status=order.status,
            filled_quantity=order.filled_quantity,
        )

    async def Bid(  # noqa: N802
        self,
        request: auction_pb2.BidRequest,
        context: grpc.aio.ServicerContext,
    ) -> auction_pb2.BidResponse:
        """买家出价。buyer 以鉴权上下文为准(R5)。"""
        owner_id = _caller_id(context)
        if owner_id == 0:
            return auction_pb2.BidResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            order = await self._uc.bid(
                owner_id,
                request.market_id,
                request.item_config_id,
                request.quantity,
                request.price,
                request.idempotency_key,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return auction_pb2.BidResponse(code=_to_proto_code(exc))
        return auction_pb2.BidResponse(
            code=errcode_pb2.OK,
            order_id=order.order_id,
            status=order.status,
            filled_quantity=order.filled_quantity,
        )

    async def CancelOrder(  # noqa: N802
        self,
        request: auction_pb2.CancelOrderRequest,
        context: grpc.aio.ServicerContext,
    ) -> auction_pb2.CancelOrderResponse:
        """撤单(仅挂单本人,未终态前)。player 以鉴权上下文为准(R5)。"""
        owner_id = _caller_id(context)
        if owner_id == 0:
            return auction_pb2.CancelOrderResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        # ★ 这两个入参校验放在 service 层是**与 Go 逐行一致**的:biz 里也有一遍,
        # 但 Go 在这里先拦是为了不让一条明显非法的撤单去抢 market 锁。
        if request.market_id == 0 or request.order_id == 0:
            return auction_pb2.CancelOrderResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self._uc.cancel_order(owner_id, request.market_id, request.order_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return auction_pb2.CancelOrderResponse(code=_to_proto_code(exc))
        return auction_pb2.CancelOrderResponse(code=errcode_pb2.OK)

    async def ListMarket(  # noqa: N802
        self,
        request: auction_pb2.ListMarketRequest,
        context: grpc.aio.ServicerContext,
    ) -> auction_pb2.ListMarketResponse:
        """看市场订单簿。只校验"有身份",不按 owner 过滤(市场是公开的)。"""
        if _caller_id(context) == 0:
            return auction_pb2.ListMarketResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            orders = await self._uc.list_market(
                request.market_id, int(request.side), int(request.limit)
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return auction_pb2.ListMarketResponse(code=_to_proto_code(exc))
        return auction_pb2.ListMarketResponse(code=errcode_pb2.OK, orders=orders)

    async def ListMyOrders(  # noqa: N802
        self,
        request: auction_pb2.ListMyOrdersRequest,
        context: grpc.aio.ServicerContext,
    ) -> auction_pb2.ListMyOrdersResponse:
        """看自己的挂单 / 出价。player 以鉴权上下文为准(R5)。"""
        owner_id = _caller_id(context)
        if owner_id == 0:
            return auction_pb2.ListMyOrdersResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            orders, next_cursor, has_more = await self._uc.list_my_orders(
                owner_id,
                request.active_only,
                request.cursor_order_id,
                int(request.limit),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return auction_pb2.ListMyOrdersResponse(code=_to_proto_code(exc))
        return auction_pb2.ListMyOrdersResponse(
            code=errcode_pb2.OK,
            orders=orders,
            next_cursor_order_id=next_cursor,
            has_more=has_more,
        )


def _caller_id(context: grpc.aio.ServicerContext) -> int:
    """从鉴权上下文取 player_id(Envoy jwt_authn 注入的 x-pandora-player-id)。

    ★ 绝不能改成"请求体里带 player_id 就信":R5 已经把 proto 里的 player_id 字段
    整批删掉,正是因为那条路径等于让客户端自称身份。
    """
    return interceptors.extract_player_id(context)


def _to_proto_code(err: BaseException) -> int:
    """errcode → proto enum,1:1 数值映射。对应 Go 的 toProtoCode。

    数值一致性由 tools/gen_errcode.py 从 Go 源码生成 + tests 里的 parity 测试保证。
    """
    return errcode.as_code(err)
