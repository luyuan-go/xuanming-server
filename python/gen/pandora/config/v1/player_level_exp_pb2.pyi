from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PlayerLevelExpRow(_message.Message):
    __slots__ = ("id", "level", "upgrade_exp", "cumulative_exp")
    ID_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    UPGRADE_EXP_FIELD_NUMBER: _ClassVar[int]
    CUMULATIVE_EXP_FIELD_NUMBER: _ClassVar[int]
    id: int
    level: int
    upgrade_exp: int
    cumulative_exp: int
    def __init__(self, id: _Optional[int] = ..., level: _Optional[int] = ..., upgrade_exp: _Optional[int] = ..., cumulative_exp: _Optional[int] = ...) -> None: ...

class PlayerLevelExpTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[PlayerLevelExpRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[PlayerLevelExpRow, _Mapping]]] = ...) -> None: ...
