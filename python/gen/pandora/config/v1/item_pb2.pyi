from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ItemType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ITEM_TYPE_UNSPECIFIED: _ClassVar[ItemType]
    ITEM_TYPE_CONSUMABLE: _ClassVar[ItemType]
    ITEM_TYPE_MATERIAL: _ClassVar[ItemType]
    ITEM_TYPE_EQUIPMENT: _ClassVar[ItemType]
    ITEM_TYPE_QUEST: _ClassVar[ItemType]
    ITEM_TYPE_CURRENCY: _ClassVar[ItemType]
ITEM_TYPE_UNSPECIFIED: ItemType
ITEM_TYPE_CONSUMABLE: ItemType
ITEM_TYPE_MATERIAL: ItemType
ITEM_TYPE_EQUIPMENT: ItemType
ITEM_TYPE_QUEST: ItemType
ITEM_TYPE_CURRENCY: ItemType

class ItemRow(_message.Message):
    __slots__ = ("id", "name", "type", "quality", "max_stack_size", "sell_price", "icon", "description", "usable", "use_heal_hp", "equip_slot", "identify_pool_id", "equip_mesh", "equip_socket", "equip_offset_x", "equip_offset_y", "equip_offset_z", "equip_yaw", "equip_pitch", "equip_roll", "equip_scale_x", "equip_scale_y", "equip_scale_z")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    QUALITY_FIELD_NUMBER: _ClassVar[int]
    MAX_STACK_SIZE_FIELD_NUMBER: _ClassVar[int]
    SELL_PRICE_FIELD_NUMBER: _ClassVar[int]
    ICON_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    USABLE_FIELD_NUMBER: _ClassVar[int]
    USE_HEAL_HP_FIELD_NUMBER: _ClassVar[int]
    EQUIP_SLOT_FIELD_NUMBER: _ClassVar[int]
    IDENTIFY_POOL_ID_FIELD_NUMBER: _ClassVar[int]
    EQUIP_MESH_FIELD_NUMBER: _ClassVar[int]
    EQUIP_SOCKET_FIELD_NUMBER: _ClassVar[int]
    EQUIP_OFFSET_X_FIELD_NUMBER: _ClassVar[int]
    EQUIP_OFFSET_Y_FIELD_NUMBER: _ClassVar[int]
    EQUIP_OFFSET_Z_FIELD_NUMBER: _ClassVar[int]
    EQUIP_YAW_FIELD_NUMBER: _ClassVar[int]
    EQUIP_PITCH_FIELD_NUMBER: _ClassVar[int]
    EQUIP_ROLL_FIELD_NUMBER: _ClassVar[int]
    EQUIP_SCALE_X_FIELD_NUMBER: _ClassVar[int]
    EQUIP_SCALE_Y_FIELD_NUMBER: _ClassVar[int]
    EQUIP_SCALE_Z_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    type: ItemType
    quality: int
    max_stack_size: int
    sell_price: int
    icon: str
    description: str
    usable: bool
    use_heal_hp: int
    equip_slot: int
    identify_pool_id: int
    equip_mesh: str
    equip_socket: str
    equip_offset_x: float
    equip_offset_y: float
    equip_offset_z: float
    equip_yaw: float
    equip_pitch: float
    equip_roll: float
    equip_scale_x: float
    equip_scale_y: float
    equip_scale_z: float
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., type: _Optional[_Union[ItemType, str]] = ..., quality: _Optional[int] = ..., max_stack_size: _Optional[int] = ..., sell_price: _Optional[int] = ..., icon: _Optional[str] = ..., description: _Optional[str] = ..., usable: bool = ..., use_heal_hp: _Optional[int] = ..., equip_slot: _Optional[int] = ..., identify_pool_id: _Optional[int] = ..., equip_mesh: _Optional[str] = ..., equip_socket: _Optional[str] = ..., equip_offset_x: _Optional[float] = ..., equip_offset_y: _Optional[float] = ..., equip_offset_z: _Optional[float] = ..., equip_yaw: _Optional[float] = ..., equip_pitch: _Optional[float] = ..., equip_roll: _Optional[float] = ..., equip_scale_x: _Optional[float] = ..., equip_scale_y: _Optional[float] = ..., equip_scale_z: _Optional[float] = ...) -> None: ...

class ItemTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[ItemRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[ItemRow, _Mapping]]] = ...) -> None: ...
