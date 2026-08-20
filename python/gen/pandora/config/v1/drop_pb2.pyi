from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DropRow(_message.Message):
    __slots__ = ("id", "monster_id", "item_config_id", "probability_permyriad", "min_count", "max_count")
    ID_FIELD_NUMBER: _ClassVar[int]
    MONSTER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    PROBABILITY_PERMYRIAD_FIELD_NUMBER: _ClassVar[int]
    MIN_COUNT_FIELD_NUMBER: _ClassVar[int]
    MAX_COUNT_FIELD_NUMBER: _ClassVar[int]
    id: int
    monster_id: int
    item_config_id: int
    probability_permyriad: int
    min_count: int
    max_count: int
    def __init__(self, id: _Optional[int] = ..., monster_id: _Optional[int] = ..., item_config_id: _Optional[int] = ..., probability_permyriad: _Optional[int] = ..., min_count: _Optional[int] = ..., max_count: _Optional[int] = ...) -> None: ...

class DropTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[DropRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[DropRow, _Mapping]]] = ...) -> None: ...
