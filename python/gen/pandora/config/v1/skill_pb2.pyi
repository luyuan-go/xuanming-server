from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SkillRow(_message.Message):
    __slots__ = ("id", "remark", "name", "icon", "segment", "next_segment_id", "cd_timing", "cd_duration", "disable_move", "ability_path", "target_select_mode", "orient_target_type", "orient_target_id", "target_camp", "use_distance", "hate_to_target", "hate_to_enemy", "need_fight_state", "rot_mode", "damage_rate", "damage_value", "apply_buffs_to_self", "correction_rate", "effect_count", "damage_display", "heal_display", "effect_description")
    ID_FIELD_NUMBER: _ClassVar[int]
    REMARK_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ICON_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_FIELD_NUMBER: _ClassVar[int]
    NEXT_SEGMENT_ID_FIELD_NUMBER: _ClassVar[int]
    CD_TIMING_FIELD_NUMBER: _ClassVar[int]
    CD_DURATION_FIELD_NUMBER: _ClassVar[int]
    DISABLE_MOVE_FIELD_NUMBER: _ClassVar[int]
    ABILITY_PATH_FIELD_NUMBER: _ClassVar[int]
    TARGET_SELECT_MODE_FIELD_NUMBER: _ClassVar[int]
    ORIENT_TARGET_TYPE_FIELD_NUMBER: _ClassVar[int]
    ORIENT_TARGET_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_CAMP_FIELD_NUMBER: _ClassVar[int]
    USE_DISTANCE_FIELD_NUMBER: _ClassVar[int]
    HATE_TO_TARGET_FIELD_NUMBER: _ClassVar[int]
    HATE_TO_ENEMY_FIELD_NUMBER: _ClassVar[int]
    NEED_FIGHT_STATE_FIELD_NUMBER: _ClassVar[int]
    ROT_MODE_FIELD_NUMBER: _ClassVar[int]
    DAMAGE_RATE_FIELD_NUMBER: _ClassVar[int]
    DAMAGE_VALUE_FIELD_NUMBER: _ClassVar[int]
    APPLY_BUFFS_TO_SELF_FIELD_NUMBER: _ClassVar[int]
    CORRECTION_RATE_FIELD_NUMBER: _ClassVar[int]
    EFFECT_COUNT_FIELD_NUMBER: _ClassVar[int]
    DAMAGE_DISPLAY_FIELD_NUMBER: _ClassVar[int]
    HEAL_DISPLAY_FIELD_NUMBER: _ClassVar[int]
    EFFECT_DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    id: int
    remark: str
    name: str
    icon: str
    segment: int
    next_segment_id: int
    cd_timing: int
    cd_duration: float
    disable_move: bool
    ability_path: str
    target_select_mode: int
    orient_target_type: int
    orient_target_id: int
    target_camp: int
    use_distance: float
    hate_to_target: float
    hate_to_enemy: float
    need_fight_state: bool
    rot_mode: int
    damage_rate: float
    damage_value: float
    apply_buffs_to_self: str
    correction_rate: float
    effect_count: int
    damage_display: int
    heal_display: int
    effect_description: str
    def __init__(self, id: _Optional[int] = ..., remark: _Optional[str] = ..., name: _Optional[str] = ..., icon: _Optional[str] = ..., segment: _Optional[int] = ..., next_segment_id: _Optional[int] = ..., cd_timing: _Optional[int] = ..., cd_duration: _Optional[float] = ..., disable_move: bool = ..., ability_path: _Optional[str] = ..., target_select_mode: _Optional[int] = ..., orient_target_type: _Optional[int] = ..., orient_target_id: _Optional[int] = ..., target_camp: _Optional[int] = ..., use_distance: _Optional[float] = ..., hate_to_target: _Optional[float] = ..., hate_to_enemy: _Optional[float] = ..., need_fight_state: bool = ..., rot_mode: _Optional[int] = ..., damage_rate: _Optional[float] = ..., damage_value: _Optional[float] = ..., apply_buffs_to_self: _Optional[str] = ..., correction_rate: _Optional[float] = ..., effect_count: _Optional[int] = ..., damage_display: _Optional[int] = ..., heal_display: _Optional[int] = ..., effect_description: _Optional[str] = ...) -> None: ...

class SkillTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[SkillRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[SkillRow, _Mapping]]] = ...) -> None: ...
