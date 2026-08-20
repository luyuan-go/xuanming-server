from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RoleAttrMapRow(_message.Message):
    __slots__ = ("id", "code_name", "show_name")
    ID_FIELD_NUMBER: _ClassVar[int]
    CODE_NAME_FIELD_NUMBER: _ClassVar[int]
    SHOW_NAME_FIELD_NUMBER: _ClassVar[int]
    id: int
    code_name: str
    show_name: str
    def __init__(self, id: _Optional[int] = ..., code_name: _Optional[str] = ..., show_name: _Optional[str] = ...) -> None: ...

class RoleAttrMapTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[RoleAttrMapRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[RoleAttrMapRow, _Mapping]]] = ...) -> None: ...
