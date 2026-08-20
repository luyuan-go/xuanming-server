from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ParticleRow(_message.Message):
    __slots__ = ("id", "remark", "asset_path", "socket_name", "attached", "location_offset", "rotation_offset", "scale")
    ID_FIELD_NUMBER: _ClassVar[int]
    REMARK_FIELD_NUMBER: _ClassVar[int]
    ASSET_PATH_FIELD_NUMBER: _ClassVar[int]
    SOCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    ATTACHED_FIELD_NUMBER: _ClassVar[int]
    LOCATION_OFFSET_FIELD_NUMBER: _ClassVar[int]
    ROTATION_OFFSET_FIELD_NUMBER: _ClassVar[int]
    SCALE_FIELD_NUMBER: _ClassVar[int]
    id: int
    remark: str
    asset_path: str
    socket_name: str
    attached: bool
    location_offset: str
    rotation_offset: str
    scale: str
    def __init__(self, id: _Optional[int] = ..., remark: _Optional[str] = ..., asset_path: _Optional[str] = ..., socket_name: _Optional[str] = ..., attached: bool = ..., location_offset: _Optional[str] = ..., rotation_offset: _Optional[str] = ..., scale: _Optional[str] = ...) -> None: ...

class ParticleTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[ParticleRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[ParticleRow, _Mapping]]] = ...) -> None: ...
