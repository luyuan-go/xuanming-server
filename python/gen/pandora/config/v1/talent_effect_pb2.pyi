from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TalentEffectRow(_message.Message):
    __slots__ = ("id", "talent_id", "attr_key", "value_per_level")
    ID_FIELD_NUMBER: _ClassVar[int]
    TALENT_ID_FIELD_NUMBER: _ClassVar[int]
    ATTR_KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_PER_LEVEL_FIELD_NUMBER: _ClassVar[int]
    id: int
    talent_id: int
    attr_key: str
    value_per_level: float
    def __init__(self, id: _Optional[int] = ..., talent_id: _Optional[int] = ..., attr_key: _Optional[str] = ..., value_per_level: _Optional[float] = ...) -> None: ...

class TalentEffectTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[TalentEffectRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[TalentEffectRow, _Mapping]]] = ...) -> None: ...
