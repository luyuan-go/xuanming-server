from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EquipmentAffixRow(_message.Message):
    __slots__ = ("id", "pool_id", "attr_count", "attr_id", "weight", "min_value", "max_value")
    ID_FIELD_NUMBER: _ClassVar[int]
    POOL_ID_FIELD_NUMBER: _ClassVar[int]
    ATTR_COUNT_FIELD_NUMBER: _ClassVar[int]
    ATTR_ID_FIELD_NUMBER: _ClassVar[int]
    WEIGHT_FIELD_NUMBER: _ClassVar[int]
    MIN_VALUE_FIELD_NUMBER: _ClassVar[int]
    MAX_VALUE_FIELD_NUMBER: _ClassVar[int]
    id: int
    pool_id: int
    attr_count: int
    attr_id: int
    weight: int
    min_value: int
    max_value: int
    def __init__(self, id: _Optional[int] = ..., pool_id: _Optional[int] = ..., attr_count: _Optional[int] = ..., attr_id: _Optional[int] = ..., weight: _Optional[int] = ..., min_value: _Optional[int] = ..., max_value: _Optional[int] = ...) -> None: ...

class EquipmentAffixTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[EquipmentAffixRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[EquipmentAffixRow, _Mapping]]] = ...) -> None: ...
