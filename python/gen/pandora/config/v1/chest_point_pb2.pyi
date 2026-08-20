from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ChestPointRow(_message.Message):
    __slots__ = ("id", "chest_group_id", "level_id", "loot_table_id", "remark", "center_x", "center_y", "center_z", "yaw", "pitch", "roll", "chest_blueprint", "static_mesh", "scale_x", "scale_y", "scale_z", "unlock_seconds", "unlock_range_cm", "scatter_radius", "exclusive_to_opener", "respawn_seconds", "first_delay_seconds", "enabled", "spawn_on_start", "quality_rule_set_id", "allow_carry", "carry_move_anim_rate", "carry_socket_name", "carry_offset_x", "carry_offset_y", "carry_offset_z", "carry_yaw", "carry_pitch", "carry_roll", "drop_forward_distance")
    ID_FIELD_NUMBER: _ClassVar[int]
    CHEST_GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    LEVEL_ID_FIELD_NUMBER: _ClassVar[int]
    LOOT_TABLE_ID_FIELD_NUMBER: _ClassVar[int]
    REMARK_FIELD_NUMBER: _ClassVar[int]
    CENTER_X_FIELD_NUMBER: _ClassVar[int]
    CENTER_Y_FIELD_NUMBER: _ClassVar[int]
    CENTER_Z_FIELD_NUMBER: _ClassVar[int]
    YAW_FIELD_NUMBER: _ClassVar[int]
    PITCH_FIELD_NUMBER: _ClassVar[int]
    ROLL_FIELD_NUMBER: _ClassVar[int]
    CHEST_BLUEPRINT_FIELD_NUMBER: _ClassVar[int]
    STATIC_MESH_FIELD_NUMBER: _ClassVar[int]
    SCALE_X_FIELD_NUMBER: _ClassVar[int]
    SCALE_Y_FIELD_NUMBER: _ClassVar[int]
    SCALE_Z_FIELD_NUMBER: _ClassVar[int]
    UNLOCK_SECONDS_FIELD_NUMBER: _ClassVar[int]
    UNLOCK_RANGE_CM_FIELD_NUMBER: _ClassVar[int]
    SCATTER_RADIUS_FIELD_NUMBER: _ClassVar[int]
    EXCLUSIVE_TO_OPENER_FIELD_NUMBER: _ClassVar[int]
    RESPAWN_SECONDS_FIELD_NUMBER: _ClassVar[int]
    FIRST_DELAY_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    SPAWN_ON_START_FIELD_NUMBER: _ClassVar[int]
    QUALITY_RULE_SET_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOW_CARRY_FIELD_NUMBER: _ClassVar[int]
    CARRY_MOVE_ANIM_RATE_FIELD_NUMBER: _ClassVar[int]
    CARRY_SOCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    CARRY_OFFSET_X_FIELD_NUMBER: _ClassVar[int]
    CARRY_OFFSET_Y_FIELD_NUMBER: _ClassVar[int]
    CARRY_OFFSET_Z_FIELD_NUMBER: _ClassVar[int]
    CARRY_YAW_FIELD_NUMBER: _ClassVar[int]
    CARRY_PITCH_FIELD_NUMBER: _ClassVar[int]
    CARRY_ROLL_FIELD_NUMBER: _ClassVar[int]
    DROP_FORWARD_DISTANCE_FIELD_NUMBER: _ClassVar[int]
    id: int
    chest_group_id: int
    level_id: int
    loot_table_id: str
    remark: str
    center_x: float
    center_y: float
    center_z: float
    yaw: float
    pitch: float
    roll: float
    chest_blueprint: str
    static_mesh: str
    scale_x: float
    scale_y: float
    scale_z: float
    unlock_seconds: float
    unlock_range_cm: float
    scatter_radius: float
    exclusive_to_opener: bool
    respawn_seconds: float
    first_delay_seconds: float
    enabled: bool
    spawn_on_start: bool
    quality_rule_set_id: str
    allow_carry: bool
    carry_move_anim_rate: float
    carry_socket_name: str
    carry_offset_x: float
    carry_offset_y: float
    carry_offset_z: float
    carry_yaw: float
    carry_pitch: float
    carry_roll: float
    drop_forward_distance: float
    def __init__(self, id: _Optional[int] = ..., chest_group_id: _Optional[int] = ..., level_id: _Optional[int] = ..., loot_table_id: _Optional[str] = ..., remark: _Optional[str] = ..., center_x: _Optional[float] = ..., center_y: _Optional[float] = ..., center_z: _Optional[float] = ..., yaw: _Optional[float] = ..., pitch: _Optional[float] = ..., roll: _Optional[float] = ..., chest_blueprint: _Optional[str] = ..., static_mesh: _Optional[str] = ..., scale_x: _Optional[float] = ..., scale_y: _Optional[float] = ..., scale_z: _Optional[float] = ..., unlock_seconds: _Optional[float] = ..., unlock_range_cm: _Optional[float] = ..., scatter_radius: _Optional[float] = ..., exclusive_to_opener: bool = ..., respawn_seconds: _Optional[float] = ..., first_delay_seconds: _Optional[float] = ..., enabled: bool = ..., spawn_on_start: bool = ..., quality_rule_set_id: _Optional[str] = ..., allow_carry: bool = ..., carry_move_anim_rate: _Optional[float] = ..., carry_socket_name: _Optional[str] = ..., carry_offset_x: _Optional[float] = ..., carry_offset_y: _Optional[float] = ..., carry_offset_z: _Optional[float] = ..., carry_yaw: _Optional[float] = ..., carry_pitch: _Optional[float] = ..., carry_roll: _Optional[float] = ..., drop_forward_distance: _Optional[float] = ...) -> None: ...

class ChestPointTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[ChestPointRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[ChestPointRow, _Mapping]]] = ...) -> None: ...
