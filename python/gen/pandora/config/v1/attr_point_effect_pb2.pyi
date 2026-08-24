from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AttrPointEffectRow(_message.Message):
    __slots__ = ("id", "attr_point_key", "attr_key", "value_per_point")
    ID_FIELD_NUMBER: _ClassVar[int]
    ATTR_POINT_KEY_FIELD_NUMBER: _ClassVar[int]
    ATTR_KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_PER_POINT_FIELD_NUMBER: _ClassVar[int]
    id: int
    attr_point_key: str
    attr_key: str
    value_per_point: float
    def __init__(self, id: _Optional[int] = ..., attr_point_key: _Optional[str] = ..., attr_key: _Optional[str] = ..., value_per_point: _Optional[float] = ...) -> None: ...

class AttrPointEffectTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[AttrPointEffectRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[AttrPointEffectRow, _Mapping]]] = ...) -> None: ...
