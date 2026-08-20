from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RoleLevelRow(_message.Message):
    __slots__ = ("id", "role_id", "role_name", "level", "hp", "attack", "defense", "crit_chance", "crit_damage", "hit_chance", "dodge_chance", "kill_exp")
    ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_NAME_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    HP_FIELD_NUMBER: _ClassVar[int]
    ATTACK_FIELD_NUMBER: _ClassVar[int]
    DEFENSE_FIELD_NUMBER: _ClassVar[int]
    CRIT_CHANCE_FIELD_NUMBER: _ClassVar[int]
    CRIT_DAMAGE_FIELD_NUMBER: _ClassVar[int]
    HIT_CHANCE_FIELD_NUMBER: _ClassVar[int]
    DODGE_CHANCE_FIELD_NUMBER: _ClassVar[int]
    KILL_EXP_FIELD_NUMBER: _ClassVar[int]
    id: int
    role_id: int
    role_name: str
    level: int
    hp: int
    attack: int
    defense: int
    crit_chance: float
    crit_damage: float
    hit_chance: float
    dodge_chance: float
    kill_exp: int
    def __init__(self, id: _Optional[int] = ..., role_id: _Optional[int] = ..., role_name: _Optional[str] = ..., level: _Optional[int] = ..., hp: _Optional[int] = ..., attack: _Optional[int] = ..., defense: _Optional[int] = ..., crit_chance: _Optional[float] = ..., crit_damage: _Optional[float] = ..., hit_chance: _Optional[float] = ..., dodge_chance: _Optional[float] = ..., kill_exp: _Optional[int] = ...) -> None: ...

class RoleLevelTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[RoleLevelRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[RoleLevelRow, _Mapping]]] = ...) -> None: ...
