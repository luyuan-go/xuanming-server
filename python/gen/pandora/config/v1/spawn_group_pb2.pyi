from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SpawnGroupRow(_message.Message):
    __slots__ = ("id", "level_id", "level_asset_path", "remark", "enabled", "priority")
    ID_FIELD_NUMBER: _ClassVar[int]
    LEVEL_ID_FIELD_NUMBER: _ClassVar[int]
    LEVEL_ASSET_PATH_FIELD_NUMBER: _ClassVar[int]
    REMARK_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    id: int
    level_id: int
    level_asset_path: str
    remark: str
    enabled: bool
    priority: int
    def __init__(self, id: _Optional[int] = ..., level_id: _Optional[int] = ..., level_asset_path: _Optional[str] = ..., remark: _Optional[str] = ..., enabled: bool = ..., priority: _Optional[int] = ...) -> None: ...

class SpawnGroupTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[SpawnGroupRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[SpawnGroupRow, _Mapping]]] = ...) -> None: ...
