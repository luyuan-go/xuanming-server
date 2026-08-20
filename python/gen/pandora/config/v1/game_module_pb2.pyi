from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GameModuleRow(_message.Message):
    __slots__ = ("id", "description", "author", "module_class", "view_model_class", "view_class")
    ID_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    MODULE_CLASS_FIELD_NUMBER: _ClassVar[int]
    VIEW_MODEL_CLASS_FIELD_NUMBER: _ClassVar[int]
    VIEW_CLASS_FIELD_NUMBER: _ClassVar[int]
    id: int
    description: str
    author: str
    module_class: str
    view_model_class: str
    view_class: str
    def __init__(self, id: _Optional[int] = ..., description: _Optional[str] = ..., author: _Optional[str] = ..., module_class: _Optional[str] = ..., view_model_class: _Optional[str] = ..., view_class: _Optional[str] = ...) -> None: ...

class GameModuleTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[GameModuleRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[GameModuleRow, _Mapping]]] = ...) -> None: ...
