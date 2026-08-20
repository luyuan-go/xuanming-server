from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LevelEntryMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LEVEL_ENTRY_MODE_UNSPECIFIED: _ClassVar[LevelEntryMode]
    LEVEL_ENTRY_MODE_MATCHMAKE: _ClassVar[LevelEntryMode]
    LEVEL_ENTRY_MODE_WALK_IN: _ClassVar[LevelEntryMode]
    LEVEL_ENTRY_MODE_BOTH: _ClassVar[LevelEntryMode]

class LevelExpShareMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LEVEL_EXP_SHARE_MODE_UNSPECIFIED: _ClassVar[LevelExpShareMode]
    LEVEL_EXP_SHARE_MODE_LAST_HIT: _ClassVar[LevelExpShareMode]
    LEVEL_EXP_SHARE_MODE_TEAM_SHARE: _ClassVar[LevelExpShareMode]
    LEVEL_EXP_SHARE_MODE_DAMAGE_SPLIT: _ClassVar[LevelExpShareMode]

class LevelRatingMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LEVEL_RATING_MODE_UNSPECIFIED: _ClassVar[LevelRatingMode]
    LEVEL_RATING_MODE_NONE: _ClassVar[LevelRatingMode]
    LEVEL_RATING_MODE_ELO: _ClassVar[LevelRatingMode]

class LevelReadyMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LEVEL_READY_MODE_UNSPECIFIED: _ClassVar[LevelReadyMode]
    LEVEL_READY_MODE_PRE_READY: _ClassVar[LevelReadyMode]
    LEVEL_READY_MODE_POST_CONFIRM: _ClassVar[LevelReadyMode]

class LevelCategory(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LEVEL_CATEGORY_UNSPECIFIED: _ClassVar[LevelCategory]
    LEVEL_CATEGORY_LOGIN: _ClassVar[LevelCategory]
    LEVEL_CATEGORY_ROLE_SELECT: _ClassVar[LevelCategory]
    LEVEL_CATEGORY_MAIN_CITY: _ClassVar[LevelCategory]
    LEVEL_CATEGORY_BATTLE: _ClassVar[LevelCategory]
LEVEL_ENTRY_MODE_UNSPECIFIED: LevelEntryMode
LEVEL_ENTRY_MODE_MATCHMAKE: LevelEntryMode
LEVEL_ENTRY_MODE_WALK_IN: LevelEntryMode
LEVEL_ENTRY_MODE_BOTH: LevelEntryMode
LEVEL_EXP_SHARE_MODE_UNSPECIFIED: LevelExpShareMode
LEVEL_EXP_SHARE_MODE_LAST_HIT: LevelExpShareMode
LEVEL_EXP_SHARE_MODE_TEAM_SHARE: LevelExpShareMode
LEVEL_EXP_SHARE_MODE_DAMAGE_SPLIT: LevelExpShareMode
LEVEL_RATING_MODE_UNSPECIFIED: LevelRatingMode
LEVEL_RATING_MODE_NONE: LevelRatingMode
LEVEL_RATING_MODE_ELO: LevelRatingMode
LEVEL_READY_MODE_UNSPECIFIED: LevelReadyMode
LEVEL_READY_MODE_PRE_READY: LevelReadyMode
LEVEL_READY_MODE_POST_CONFIRM: LevelReadyMode
LEVEL_CATEGORY_UNSPECIFIED: LevelCategory
LEVEL_CATEGORY_LOGIN: LevelCategory
LEVEL_CATEGORY_ROLE_SELECT: LevelCategory
LEVEL_CATEGORY_MAIN_CITY: LevelCategory
LEVEL_CATEGORY_BATTLE: LevelCategory

class LevelRow(_message.Message):
    __slots__ = ("id", "name", "asset_path", "game_mode_class", "category", "disable_ui_shortcut", "show_in_match_list", "team_size", "allow_exit", "game_mode", "entry_mode", "side_count", "exp_share_mode", "battle_duration_seconds", "min_team_size", "rating_mode", "rating_pool", "prepare_duration_seconds", "settle_duration_seconds", "ready_mode")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ASSET_PATH_FIELD_NUMBER: _ClassVar[int]
    GAME_MODE_CLASS_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    DISABLE_UI_SHORTCUT_FIELD_NUMBER: _ClassVar[int]
    SHOW_IN_MATCH_LIST_FIELD_NUMBER: _ClassVar[int]
    TEAM_SIZE_FIELD_NUMBER: _ClassVar[int]
    ALLOW_EXIT_FIELD_NUMBER: _ClassVar[int]
    GAME_MODE_FIELD_NUMBER: _ClassVar[int]
    ENTRY_MODE_FIELD_NUMBER: _ClassVar[int]
    SIDE_COUNT_FIELD_NUMBER: _ClassVar[int]
    EXP_SHARE_MODE_FIELD_NUMBER: _ClassVar[int]
    BATTLE_DURATION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    MIN_TEAM_SIZE_FIELD_NUMBER: _ClassVar[int]
    RATING_MODE_FIELD_NUMBER: _ClassVar[int]
    RATING_POOL_FIELD_NUMBER: _ClassVar[int]
    PREPARE_DURATION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    SETTLE_DURATION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    READY_MODE_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    asset_path: str
    game_mode_class: str
    category: LevelCategory
    disable_ui_shortcut: bool
    show_in_match_list: bool
    team_size: int
    allow_exit: bool
    game_mode: str
    entry_mode: LevelEntryMode
    side_count: int
    exp_share_mode: LevelExpShareMode
    battle_duration_seconds: int
    min_team_size: int
    rating_mode: LevelRatingMode
    rating_pool: str
    prepare_duration_seconds: int
    settle_duration_seconds: int
    ready_mode: LevelReadyMode
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., asset_path: _Optional[str] = ..., game_mode_class: _Optional[str] = ..., category: _Optional[_Union[LevelCategory, str]] = ..., disable_ui_shortcut: bool = ..., show_in_match_list: bool = ..., team_size: _Optional[int] = ..., allow_exit: bool = ..., game_mode: _Optional[str] = ..., entry_mode: _Optional[_Union[LevelEntryMode, str]] = ..., side_count: _Optional[int] = ..., exp_share_mode: _Optional[_Union[LevelExpShareMode, str]] = ..., battle_duration_seconds: _Optional[int] = ..., min_team_size: _Optional[int] = ..., rating_mode: _Optional[_Union[LevelRatingMode, str]] = ..., rating_pool: _Optional[str] = ..., prepare_duration_seconds: _Optional[int] = ..., settle_duration_seconds: _Optional[int] = ..., ready_mode: _Optional[_Union[LevelReadyMode, str]] = ...) -> None: ...

class LevelTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[LevelRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[LevelRow, _Mapping]]] = ...) -> None: ...
