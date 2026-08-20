from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WeaponRow(_message.Message):
    __slots__ = ("id", "weapon_type", "name", "model_info")
    ID_FIELD_NUMBER: _ClassVar[int]
    WEAPON_TYPE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    MODEL_INFO_FIELD_NUMBER: _ClassVar[int]
    id: int
    weapon_type: int
    name: str
    model_info: str
    def __init__(self, id: _Optional[int] = ..., weapon_type: _Optional[int] = ..., name: _Optional[str] = ..., model_info: _Optional[str] = ...) -> None: ...

class WeaponTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[WeaponRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[WeaponRow, _Mapping]]] = ...) -> None: ...
