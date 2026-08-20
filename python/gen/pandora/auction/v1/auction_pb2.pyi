from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OrderSide(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ORDER_SIDE_UNSPECIFIED: _ClassVar[OrderSide]
    ORDER_SIDE_SELL: _ClassVar[OrderSide]
    ORDER_SIDE_BUY: _ClassVar[OrderSide]

class AuctionOrderStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    AUCTION_ORDER_STATUS_UNSPECIFIED: _ClassVar[AuctionOrderStatus]
    AUCTION_ORDER_STATUS_OPEN: _ClassVar[AuctionOrderStatus]
    AUCTION_ORDER_STATUS_PARTIALLY_FILLED: _ClassVar[AuctionOrderStatus]
    AUCTION_ORDER_STATUS_FILLED: _ClassVar[AuctionOrderStatus]
    AUCTION_ORDER_STATUS_CANCELED: _ClassVar[AuctionOrderStatus]
    AUCTION_ORDER_STATUS_EXPIRED: _ClassVar[AuctionOrderStatus]

class AuctionPushEventType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    AUCTION_PUSH_EVENT_TYPE_UNSPECIFIED: _ClassVar[AuctionPushEventType]
ORDER_SIDE_UNSPECIFIED: OrderSide
ORDER_SIDE_SELL: OrderSide
ORDER_SIDE_BUY: OrderSide
AUCTION_ORDER_STATUS_UNSPECIFIED: AuctionOrderStatus
AUCTION_ORDER_STATUS_OPEN: AuctionOrderStatus
AUCTION_ORDER_STATUS_PARTIALLY_FILLED: AuctionOrderStatus
AUCTION_ORDER_STATUS_FILLED: AuctionOrderStatus
AUCTION_ORDER_STATUS_CANCELED: AuctionOrderStatus
AUCTION_ORDER_STATUS_EXPIRED: AuctionOrderStatus
AUCTION_PUSH_EVENT_TYPE_UNSPECIFIED: AuctionPushEventType

class AuctionOrder(_message.Message):
    __slots__ = ("order_id", "market_id", "owner_id", "side", "item_config_id", "quantity", "filled_quantity", "price", "status", "created_at_ms", "updated_at_ms")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    MARKET_ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    SIDE_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    FILLED_QUANTITY_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    order_id: int
    market_id: int
    owner_id: int
    side: OrderSide
    item_config_id: int
    quantity: int
    filled_quantity: int
    price: int
    status: AuctionOrderStatus
    created_at_ms: int
    updated_at_ms: int
    def __init__(self, order_id: _Optional[int] = ..., market_id: _Optional[int] = ..., owner_id: _Optional[int] = ..., side: _Optional[_Union[OrderSide, str]] = ..., item_config_id: _Optional[int] = ..., quantity: _Optional[int] = ..., filled_quantity: _Optional[int] = ..., price: _Optional[int] = ..., status: _Optional[_Union[AuctionOrderStatus, str]] = ..., created_at_ms: _Optional[int] = ..., updated_at_ms: _Optional[int] = ...) -> None: ...

class AuctionMatchEvent(_message.Message):
    __slots__ = ("match_id", "market_id", "sell_order_id", "buy_order_id", "seller_id", "buyer_id", "item_config_id", "quantity", "price", "matched_at_ms")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    MARKET_ID_FIELD_NUMBER: _ClassVar[int]
    SELL_ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    BUY_ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    SELLER_ID_FIELD_NUMBER: _ClassVar[int]
    BUYER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    MATCHED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    market_id: int
    sell_order_id: int
    buy_order_id: int
    seller_id: int
    buyer_id: int
    item_config_id: int
    quantity: int
    price: int
    matched_at_ms: int
    def __init__(self, match_id: _Optional[int] = ..., market_id: _Optional[int] = ..., sell_order_id: _Optional[int] = ..., buy_order_id: _Optional[int] = ..., seller_id: _Optional[int] = ..., buyer_id: _Optional[int] = ..., item_config_id: _Optional[int] = ..., quantity: _Optional[int] = ..., price: _Optional[int] = ..., matched_at_ms: _Optional[int] = ...) -> None: ...

class PlaceOrderRequest(_message.Message):
    __slots__ = ("market_id", "item_config_id", "quantity", "price", "idempotency_key")
    MARKET_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    market_id: int
    item_config_id: int
    quantity: int
    price: int
    idempotency_key: str
    def __init__(self, market_id: _Optional[int] = ..., item_config_id: _Optional[int] = ..., quantity: _Optional[int] = ..., price: _Optional[int] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class PlaceOrderResponse(_message.Message):
    __slots__ = ("code", "order_id", "status", "filled_quantity")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    FILLED_QUANTITY_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    order_id: int
    status: AuctionOrderStatus
    filled_quantity: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., order_id: _Optional[int] = ..., status: _Optional[_Union[AuctionOrderStatus, str]] = ..., filled_quantity: _Optional[int] = ...) -> None: ...

class BidRequest(_message.Message):
    __slots__ = ("market_id", "item_config_id", "quantity", "price", "idempotency_key")
    MARKET_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    market_id: int
    item_config_id: int
    quantity: int
    price: int
    idempotency_key: str
    def __init__(self, market_id: _Optional[int] = ..., item_config_id: _Optional[int] = ..., quantity: _Optional[int] = ..., price: _Optional[int] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class BidResponse(_message.Message):
    __slots__ = ("code", "order_id", "status", "filled_quantity")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    FILLED_QUANTITY_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    order_id: int
    status: AuctionOrderStatus
    filled_quantity: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., order_id: _Optional[int] = ..., status: _Optional[_Union[AuctionOrderStatus, str]] = ..., filled_quantity: _Optional[int] = ...) -> None: ...

class CancelOrderRequest(_message.Message):
    __slots__ = ("market_id", "order_id")
    MARKET_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    market_id: int
    order_id: int
    def __init__(self, market_id: _Optional[int] = ..., order_id: _Optional[int] = ...) -> None: ...

class CancelOrderResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class ListMarketRequest(_message.Message):
    __slots__ = ("market_id", "side", "limit")
    MARKET_ID_FIELD_NUMBER: _ClassVar[int]
    SIDE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    market_id: int
    side: OrderSide
    limit: int
    def __init__(self, market_id: _Optional[int] = ..., side: _Optional[_Union[OrderSide, str]] = ..., limit: _Optional[int] = ...) -> None: ...

class ListMarketResponse(_message.Message):
    __slots__ = ("code", "orders")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ORDERS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    orders: _containers.RepeatedCompositeFieldContainer[AuctionOrder]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., orders: _Optional[_Iterable[_Union[AuctionOrder, _Mapping]]] = ...) -> None: ...

class ListMyOrdersRequest(_message.Message):
    __slots__ = ("active_only", "cursor_order_id", "limit")
    ACTIVE_ONLY_FIELD_NUMBER: _ClassVar[int]
    CURSOR_ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    active_only: bool
    cursor_order_id: int
    limit: int
    def __init__(self, active_only: bool = ..., cursor_order_id: _Optional[int] = ..., limit: _Optional[int] = ...) -> None: ...

class ListMyOrdersResponse(_message.Message):
    __slots__ = ("code", "orders", "next_cursor_order_id", "has_more")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ORDERS_FIELD_NUMBER: _ClassVar[int]
    NEXT_CURSOR_ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    HAS_MORE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    orders: _containers.RepeatedCompositeFieldContainer[AuctionOrder]
    next_cursor_order_id: int
    has_more: bool
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., orders: _Optional[_Iterable[_Union[AuctionOrder, _Mapping]]] = ..., next_cursor_order_id: _Optional[int] = ..., has_more: bool = ...) -> None: ...
