from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OrderState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ORDER_STATE_UNSPECIFIED: _ClassVar[OrderState]
    ORDER_STATE_PENDING: _ClassVar[OrderState]
    ORDER_STATE_BUYER_CONFIRMED: _ClassVar[OrderState]
    ORDER_STATE_SELLER_CONFIRMED: _ClassVar[OrderState]
    ORDER_STATE_COMPLETED: _ClassVar[OrderState]
    ORDER_STATE_FAILED: _ClassVar[OrderState]
    ORDER_STATE_EXPIRED: _ClassVar[OrderState]
    ORDER_STATE_CANCELED: _ClassVar[OrderState]

class TradePushEventType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TRADE_PUSH_EVENT_TYPE_UNSPECIFIED: _ClassVar[TradePushEventType]
ORDER_STATE_UNSPECIFIED: OrderState
ORDER_STATE_PENDING: OrderState
ORDER_STATE_BUYER_CONFIRMED: OrderState
ORDER_STATE_SELLER_CONFIRMED: OrderState
ORDER_STATE_COMPLETED: OrderState
ORDER_STATE_FAILED: OrderState
ORDER_STATE_EXPIRED: OrderState
ORDER_STATE_CANCELED: OrderState
TRADE_PUSH_EVENT_TYPE_UNSPECIFIED: TradePushEventType

class TradeItem(_message.Message):
    __slots__ = ("item_config_id", "count")
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    item_config_id: int
    count: int
    def __init__(self, item_config_id: _Optional[int] = ..., count: _Optional[int] = ...) -> None: ...

class Order(_message.Message):
    __slots__ = ("order_id", "seller_id", "buyer_id", "items", "price", "state", "created_at_ms", "expires_at_ms", "buyer_items")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    SELLER_ID_FIELD_NUMBER: _ClassVar[int]
    BUYER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_MS_FIELD_NUMBER: _ClassVar[int]
    BUYER_ITEMS_FIELD_NUMBER: _ClassVar[int]
    order_id: int
    seller_id: int
    buyer_id: int
    items: _containers.RepeatedCompositeFieldContainer[TradeItem]
    price: int
    state: OrderState
    created_at_ms: int
    expires_at_ms: int
    buyer_items: _containers.RepeatedCompositeFieldContainer[TradeItem]
    def __init__(self, order_id: _Optional[int] = ..., seller_id: _Optional[int] = ..., buyer_id: _Optional[int] = ..., items: _Optional[_Iterable[_Union[TradeItem, _Mapping]]] = ..., price: _Optional[int] = ..., state: _Optional[_Union[OrderState, str]] = ..., created_at_ms: _Optional[int] = ..., expires_at_ms: _Optional[int] = ..., buyer_items: _Optional[_Iterable[_Union[TradeItem, _Mapping]]] = ...) -> None: ...

class CreateOrderRequest(_message.Message):
    __slots__ = ("seller_id", "buyer_id", "items", "price", "buyer_items")
    SELLER_ID_FIELD_NUMBER: _ClassVar[int]
    BUYER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    BUYER_ITEMS_FIELD_NUMBER: _ClassVar[int]
    seller_id: int
    buyer_id: int
    items: _containers.RepeatedCompositeFieldContainer[TradeItem]
    price: int
    buyer_items: _containers.RepeatedCompositeFieldContainer[TradeItem]
    def __init__(self, seller_id: _Optional[int] = ..., buyer_id: _Optional[int] = ..., items: _Optional[_Iterable[_Union[TradeItem, _Mapping]]] = ..., price: _Optional[int] = ..., buyer_items: _Optional[_Iterable[_Union[TradeItem, _Mapping]]] = ...) -> None: ...

class CreateOrderResponse(_message.Message):
    __slots__ = ("code", "order_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    order_id: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., order_id: _Optional[int] = ...) -> None: ...

class ConfirmOrderRequest(_message.Message):
    __slots__ = ("player_id", "order_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    order_id: int
    def __init__(self, player_id: _Optional[int] = ..., order_id: _Optional[int] = ...) -> None: ...

class ConfirmOrderResponse(_message.Message):
    __slots__ = ("code", "new_state")
    CODE_FIELD_NUMBER: _ClassVar[int]
    NEW_STATE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    new_state: OrderState
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., new_state: _Optional[_Union[OrderState, str]] = ...) -> None: ...

class CancelOrderRequest(_message.Message):
    __slots__ = ("player_id", "order_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    order_id: int
    def __init__(self, player_id: _Optional[int] = ..., order_id: _Optional[int] = ...) -> None: ...

class CancelOrderResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class ListMyOrdersRequest(_message.Message):
    __slots__ = ("player_id", "active_only", "cursor", "limit")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_ONLY_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    active_only: bool
    cursor: int
    limit: int
    def __init__(self, player_id: _Optional[int] = ..., active_only: bool = ..., cursor: _Optional[int] = ..., limit: _Optional[int] = ...) -> None: ...

class ListMyOrdersResponse(_message.Message):
    __slots__ = ("code", "orders", "next_cursor")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ORDERS_FIELD_NUMBER: _ClassVar[int]
    NEXT_CURSOR_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    orders: _containers.RepeatedCompositeFieldContainer[Order]
    next_cursor: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., orders: _Optional[_Iterable[_Union[Order, _Mapping]]] = ..., next_cursor: _Optional[int] = ...) -> None: ...
