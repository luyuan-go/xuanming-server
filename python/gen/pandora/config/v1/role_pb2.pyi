from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RoleRow(_message.Message):
    __slots__ = ("id", "name", "description", "resource_path", "behavior_tree_path", "icon_path", "entity_type", "sub_type", "skill_ids", "default_weapon_id", "guard_distance", "call_distance", "fight_exit_distance", "patrol_distance", "search_distance", "nameplate_offset_z", "call_allies_radius_on_death", "unlimited_pursuit_on_call", "evac_point_class")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_PATH_FIELD_NUMBER: _ClassVar[int]
    BEHAVIOR_TREE_PATH_FIELD_NUMBER: _ClassVar[int]
    ICON_PATH_FIELD_NUMBER: _ClassVar[int]
    ENTITY_TYPE_FIELD_NUMBER: _ClassVar[int]
    SUB_TYPE_FIELD_NUMBER: _ClassVar[int]
    SKILL_IDS_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_WEAPON_ID_FIELD_NUMBER: _ClassVar[int]
    GUARD_DISTANCE_FIELD_NUMBER: _ClassVar[int]
    CALL_DISTANCE_FIELD_NUMBER: _ClassVar[int]
    FIGHT_EXIT_DISTANCE_FIELD_NUMBER: _ClassVar[int]
    PATROL_DISTANCE_FIELD_NUMBER: _ClassVar[int]
    SEARCH_DISTANCE_FIELD_NUMBER: _ClassVar[int]
    NAMEPLATE_OFFSET_Z_FIELD_NUMBER: _ClassVar[int]
    CALL_ALLIES_RADIUS_ON_DEATH_FIELD_NUMBER: _ClassVar[int]
    UNLIMITED_PURSUIT_ON_CALL_FIELD_NUMBER: _ClassVar[int]
    EVAC_POINT_CLASS_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    description: str
    resource_path: str
    behavior_tree_path: str
    icon_path: str
    entity_type: int
    sub_type: int
    skill_ids: str
    default_weapon_id: str
    guard_distance: float
    call_distance: float
    fight_exit_distance: float
    patrol_distance: float
    search_distance: float
    nameplate_offset_z: float
    call_allies_radius_on_death: float
    unlimited_pursuit_on_call: bool
    evac_point_class: str
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., resource_path: _Optional[str] = ..., behavior_tree_path: _Optional[str] = ..., icon_path: _Optional[str] = ..., entity_type: _Optional[int] = ..., sub_type: _Optional[int] = ..., skill_ids: _Optional[str] = ..., default_weapon_id: _Optional[str] = ..., guard_distance: _Optional[float] = ..., call_distance: _Optional[float] = ..., fight_exit_distance: _Optional[float] = ..., patrol_distance: _Optional[float] = ..., search_distance: _Optional[float] = ..., nameplate_offset_z: _Optional[float] = ..., call_allies_radius_on_death: _Optional[float] = ..., unlimited_pursuit_on_call: bool = ..., evac_point_class: _Optional[str] = ...) -> None: ...

class RoleTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[RoleRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[RoleRow, _Mapping]]] = ...) -> None: ...
