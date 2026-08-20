from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SkillCircleRow(_message.Message):
    __slots__ = ("id", "remark", "pos_option", "circles", "out_range_circles")
    ID_FIELD_NUMBER: _ClassVar[int]
    REMARK_FIELD_NUMBER: _ClassVar[int]
    POS_OPTION_FIELD_NUMBER: _ClassVar[int]
    CIRCLES_FIELD_NUMBER: _ClassVar[int]
    OUT_RANGE_CIRCLES_FIELD_NUMBER: _ClassVar[int]
    id: int
    remark: str
    pos_option: int
    circles: str
    out_range_circles: str
    def __init__(self, id: _Optional[int] = ..., remark: _Optional[str] = ..., pos_option: _Optional[int] = ..., circles: _Optional[str] = ..., out_range_circles: _Optional[str] = ...) -> None: ...

class SkillCircleTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[SkillCircleRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[SkillCircleRow, _Mapping]]] = ...) -> None: ...
