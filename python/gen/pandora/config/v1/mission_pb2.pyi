from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MissionRow(_message.Message):
    __slots__ = ("id", "name", "mission_type", "mission_sub_type", "condition_ids", "target_counts", "next_mission_ids", "reward_id", "auto_reward")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    MISSION_TYPE_FIELD_NUMBER: _ClassVar[int]
    MISSION_SUB_TYPE_FIELD_NUMBER: _ClassVar[int]
    CONDITION_IDS_FIELD_NUMBER: _ClassVar[int]
    TARGET_COUNTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_MISSION_IDS_FIELD_NUMBER: _ClassVar[int]
    REWARD_ID_FIELD_NUMBER: _ClassVar[int]
    AUTO_REWARD_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    mission_type: int
    mission_sub_type: int
    condition_ids: str
    target_counts: str
    next_mission_ids: str
    reward_id: int
    auto_reward: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., mission_type: _Optional[int] = ..., mission_sub_type: _Optional[int] = ..., condition_ids: _Optional[str] = ..., target_counts: _Optional[str] = ..., next_mission_ids: _Optional[str] = ..., reward_id: _Optional[int] = ..., auto_reward: _Optional[int] = ...) -> None: ...

class MissionTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[MissionRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[MissionRow, _Mapping]]] = ...) -> None: ...

class ConditionRow(_message.Message):
    __slots__ = ("id", "name", "condition_category", "slot1", "slot2", "slot3", "slot4", "target_count", "comparison_op")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONDITION_CATEGORY_FIELD_NUMBER: _ClassVar[int]
    SLOT1_FIELD_NUMBER: _ClassVar[int]
    SLOT2_FIELD_NUMBER: _ClassVar[int]
    SLOT3_FIELD_NUMBER: _ClassVar[int]
    SLOT4_FIELD_NUMBER: _ClassVar[int]
    TARGET_COUNT_FIELD_NUMBER: _ClassVar[int]
    COMPARISON_OP_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    condition_category: int
    slot1: str
    slot2: str
    slot3: str
    slot4: str
    target_count: int
    comparison_op: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., condition_category: _Optional[int] = ..., slot1: _Optional[str] = ..., slot2: _Optional[str] = ..., slot3: _Optional[str] = ..., slot4: _Optional[str] = ..., target_count: _Optional[int] = ..., comparison_op: _Optional[int] = ...) -> None: ...

class ConditionTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[ConditionRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[ConditionRow, _Mapping]]] = ...) -> None: ...

class RewardRow(_message.Message):
    __slots__ = ("id", "name", "item_ids", "item_counts", "exp")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ITEM_IDS_FIELD_NUMBER: _ClassVar[int]
    ITEM_COUNTS_FIELD_NUMBER: _ClassVar[int]
    EXP_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    item_ids: str
    item_counts: str
    exp: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., item_ids: _Optional[str] = ..., item_counts: _Optional[str] = ..., exp: _Optional[int] = ...) -> None: ...

class RewardTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[RewardRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[RewardRow, _Mapping]]] = ...) -> None: ...
