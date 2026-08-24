from pandora.config.v1 import excel_pb2 as _excel_pb2
from pandora.common.v1 import currency_pb2 as _currency_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ShopRow(_message.Message):
    __slots__ = ("id", "shop_id", "item_config_id", "count_per_unit", "currency_kind", "unit_price", "sort_order")
    ID_FIELD_NUMBER: _ClassVar[int]
    SHOP_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_PER_UNIT_FIELD_NUMBER: _ClassVar[int]
    CURRENCY_KIND_FIELD_NUMBER: _ClassVar[int]
    UNIT_PRICE_FIELD_NUMBER: _ClassVar[int]
    SORT_ORDER_FIELD_NUMBER: _ClassVar[int]
    id: int
    shop_id: int
    item_config_id: int
    count_per_unit: int
    currency_kind: _currency_pb2.CurrencyKind
    unit_price: int
    sort_order: int
    def __init__(self, id: _Optional[int] = ..., shop_id: _Optional[int] = ..., item_config_id: _Optional[int] = ..., count_per_unit: _Optional[int] = ..., currency_kind: _Optional[_Union[_currency_pb2.CurrencyKind, str]] = ..., unit_price: _Optional[int] = ..., sort_order: _Optional[int] = ...) -> None: ...

class ShopTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[ShopRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[ShopRow, _Mapping]]] = ...) -> None: ...
