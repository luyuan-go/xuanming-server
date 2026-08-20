from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GmCommandRow(_message.Message):
    __slots__ = ("id", "command_name", "display_name", "search_keywords", "param_sets", "run_on_ds", "command_class")
    ID_FIELD_NUMBER: _ClassVar[int]
    COMMAND_NAME_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    SEARCH_KEYWORDS_FIELD_NUMBER: _ClassVar[int]
    PARAM_SETS_FIELD_NUMBER: _ClassVar[int]
    RUN_ON_DS_FIELD_NUMBER: _ClassVar[int]
    COMMAND_CLASS_FIELD_NUMBER: _ClassVar[int]
    id: int
    command_name: str
    display_name: str
    search_keywords: str
    param_sets: str
    run_on_ds: bool
    command_class: str
    def __init__(self, id: _Optional[int] = ..., command_name: _Optional[str] = ..., display_name: _Optional[str] = ..., search_keywords: _Optional[str] = ..., param_sets: _Optional[str] = ..., run_on_ds: bool = ..., command_class: _Optional[str] = ...) -> None: ...

class GmCommandTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[GmCommandRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[GmCommandRow, _Mapping]]] = ...) -> None: ...
