from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SkillSectorRow(_message.Message):
    __slots__ = ("id", "remark", "fx", "radius", "angle")
    ID_FIELD_NUMBER: _ClassVar[int]
    REMARK_FIELD_NUMBER: _ClassVar[int]
    FX_FIELD_NUMBER: _ClassVar[int]
    RADIUS_FIELD_NUMBER: _ClassVar[int]
    ANGLE_FIELD_NUMBER: _ClassVar[int]
    id: int
    remark: str
    fx: str
    radius: float
    angle: float
    def __init__(self, id: _Optional[int] = ..., remark: _Optional[str] = ..., fx: _Optional[str] = ..., radius: _Optional[float] = ..., angle: _Optional[float] = ...) -> None: ...

class SkillSectorTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[SkillSectorRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[SkillSectorRow, _Mapping]]] = ...) -> None: ...
