from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EquipmentAttrRow(_message.Message):
    __slots__ = ("id", "name", "quality", "equip_slot", "icon", "hp_rate", "damage_rate", "heal_rate", "crit_rate", "skill_rate", "control_rate", "description")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    QUALITY_FIELD_NUMBER: _ClassVar[int]
    EQUIP_SLOT_FIELD_NUMBER: _ClassVar[int]
    ICON_FIELD_NUMBER: _ClassVar[int]
    HP_RATE_FIELD_NUMBER: _ClassVar[int]
    DAMAGE_RATE_FIELD_NUMBER: _ClassVar[int]
    HEAL_RATE_FIELD_NUMBER: _ClassVar[int]
    CRIT_RATE_FIELD_NUMBER: _ClassVar[int]
    SKILL_RATE_FIELD_NUMBER: _ClassVar[int]
    CONTROL_RATE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    quality: int
    equip_slot: int
    icon: str
    hp_rate: float
    damage_rate: float
    heal_rate: float
    crit_rate: float
    skill_rate: float
    control_rate: float
    description: str
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., quality: _Optional[int] = ..., equip_slot: _Optional[int] = ..., icon: _Optional[str] = ..., hp_rate: _Optional[float] = ..., damage_rate: _Optional[float] = ..., heal_rate: _Optional[float] = ..., crit_rate: _Optional[float] = ..., skill_rate: _Optional[float] = ..., control_rate: _Optional[float] = ..., description: _Optional[str] = ...) -> None: ...

class EquipmentAttrTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[EquipmentAttrRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[EquipmentAttrRow, _Mapping]]] = ...) -> None: ...
