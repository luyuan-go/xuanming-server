from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TalentRow(_message.Message):
    __slots__ = ("id", "name", "description", "max_level", "cost_per_level", "require_talent_id", "require_talent_level")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    MAX_LEVEL_FIELD_NUMBER: _ClassVar[int]
    COST_PER_LEVEL_FIELD_NUMBER: _ClassVar[int]
    REQUIRE_TALENT_ID_FIELD_NUMBER: _ClassVar[int]
    REQUIRE_TALENT_LEVEL_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    description: str
    max_level: int
    cost_per_level: int
    require_talent_id: int
    require_talent_level: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., max_level: _Optional[int] = ..., cost_per_level: _Optional[int] = ..., require_talent_id: _Optional[int] = ..., require_talent_level: _Optional[int] = ...) -> None: ...

class TalentTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[TalentRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[TalentRow, _Mapping]]] = ...) -> None: ...
