from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SkillBulletRow(_message.Message):
    __slots__ = ("id", "remark", "fx", "length", "width", "count", "angle")
    ID_FIELD_NUMBER: _ClassVar[int]
    REMARK_FIELD_NUMBER: _ClassVar[int]
    FX_FIELD_NUMBER: _ClassVar[int]
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    WIDTH_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    ANGLE_FIELD_NUMBER: _ClassVar[int]
    id: int
    remark: str
    fx: str
    length: float
    width: float
    count: int
    angle: float
    def __init__(self, id: _Optional[int] = ..., remark: _Optional[str] = ..., fx: _Optional[str] = ..., length: _Optional[float] = ..., width: _Optional[float] = ..., count: _Optional[int] = ..., angle: _Optional[float] = ...) -> None: ...

class SkillBulletTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[SkillBulletRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[SkillBulletRow, _Mapping]]] = ...) -> None: ...
