from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SkillCardRow(_message.Message):
    __slots__ = ("id", "name", "skill_id", "rarity", "max_level")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SKILL_ID_FIELD_NUMBER: _ClassVar[int]
    RARITY_FIELD_NUMBER: _ClassVar[int]
    MAX_LEVEL_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    skill_id: int
    rarity: int
    max_level: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., skill_id: _Optional[int] = ..., rarity: _Optional[int] = ..., max_level: _Optional[int] = ...) -> None: ...

class SkillCardTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[SkillCardRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[SkillCardRow, _Mapping]]] = ...) -> None: ...

class SkillCardUpgradeRow(_message.Message):
    __slots__ = ("id", "rarity", "level", "shard_cost")
    ID_FIELD_NUMBER: _ClassVar[int]
    RARITY_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    SHARD_COST_FIELD_NUMBER: _ClassVar[int]
    id: int
    rarity: int
    level: int
    shard_cost: int
    def __init__(self, id: _Optional[int] = ..., rarity: _Optional[int] = ..., level: _Optional[int] = ..., shard_cost: _Optional[int] = ...) -> None: ...

class SkillCardUpgradeTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[SkillCardUpgradeRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[SkillCardUpgradeRow, _Mapping]]] = ...) -> None: ...
