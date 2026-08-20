from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SpawnPointRow(_message.Message):
    __slots__ = ("id", "spawn_group_id", "level_id", "remark", "monster_role_id", "keep_count", "center_x", "center_y", "center_z", "yaw", "pitch", "roll", "monster_blueprint", "skeletal_mesh", "scale_x", "scale_y", "scale_z", "respawn_radius", "respawn_interval_seconds", "first_delay_seconds", "enabled", "spawn_on_start", "patrol_route_id", "weight", "always_tick", "monster_level", "camp")
    ID_FIELD_NUMBER: _ClassVar[int]
    SPAWN_GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    LEVEL_ID_FIELD_NUMBER: _ClassVar[int]
    REMARK_FIELD_NUMBER: _ClassVar[int]
    MONSTER_ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    KEEP_COUNT_FIELD_NUMBER: _ClassVar[int]
    CENTER_X_FIELD_NUMBER: _ClassVar[int]
    CENTER_Y_FIELD_NUMBER: _ClassVar[int]
    CENTER_Z_FIELD_NUMBER: _ClassVar[int]
    YAW_FIELD_NUMBER: _ClassVar[int]
    PITCH_FIELD_NUMBER: _ClassVar[int]
    ROLL_FIELD_NUMBER: _ClassVar[int]
    MONSTER_BLUEPRINT_FIELD_NUMBER: _ClassVar[int]
    SKELETAL_MESH_FIELD_NUMBER: _ClassVar[int]
    SCALE_X_FIELD_NUMBER: _ClassVar[int]
    SCALE_Y_FIELD_NUMBER: _ClassVar[int]
    SCALE_Z_FIELD_NUMBER: _ClassVar[int]
    RESPAWN_RADIUS_FIELD_NUMBER: _ClassVar[int]
    RESPAWN_INTERVAL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    FIRST_DELAY_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    SPAWN_ON_START_FIELD_NUMBER: _ClassVar[int]
    PATROL_ROUTE_ID_FIELD_NUMBER: _ClassVar[int]
    WEIGHT_FIELD_NUMBER: _ClassVar[int]
    ALWAYS_TICK_FIELD_NUMBER: _ClassVar[int]
    MONSTER_LEVEL_FIELD_NUMBER: _ClassVar[int]
    CAMP_FIELD_NUMBER: _ClassVar[int]
    id: int
    spawn_group_id: int
    level_id: int
    remark: str
    monster_role_id: int
    keep_count: int
    center_x: float
    center_y: float
    center_z: float
    yaw: float
    pitch: float
    roll: float
    monster_blueprint: str
    skeletal_mesh: str
    scale_x: float
    scale_y: float
    scale_z: float
    respawn_radius: int
    respawn_interval_seconds: int
    first_delay_seconds: float
    enabled: bool
    spawn_on_start: bool
    patrol_route_id: int
    weight: int
    always_tick: bool
    monster_level: int
    camp: int
    def __init__(self, id: _Optional[int] = ..., spawn_group_id: _Optional[int] = ..., level_id: _Optional[int] = ..., remark: _Optional[str] = ..., monster_role_id: _Optional[int] = ..., keep_count: _Optional[int] = ..., center_x: _Optional[float] = ..., center_y: _Optional[float] = ..., center_z: _Optional[float] = ..., yaw: _Optional[float] = ..., pitch: _Optional[float] = ..., roll: _Optional[float] = ..., monster_blueprint: _Optional[str] = ..., skeletal_mesh: _Optional[str] = ..., scale_x: _Optional[float] = ..., scale_y: _Optional[float] = ..., scale_z: _Optional[float] = ..., respawn_radius: _Optional[int] = ..., respawn_interval_seconds: _Optional[int] = ..., first_delay_seconds: _Optional[float] = ..., enabled: bool = ..., spawn_on_start: bool = ..., patrol_route_id: _Optional[int] = ..., weight: _Optional[int] = ..., always_tick: bool = ..., monster_level: _Optional[int] = ..., camp: _Optional[int] = ...) -> None: ...

class SpawnPointTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[SpawnPointRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[SpawnPointRow, _Mapping]]] = ...) -> None: ...
