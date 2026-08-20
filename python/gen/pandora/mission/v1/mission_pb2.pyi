from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MissionConditionCategory(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MISSION_CONDITION_CATEGORY_UNSPECIFIED: _ClassVar[MissionConditionCategory]
    MISSION_CONDITION_CATEGORY_KILL_MONSTER: _ClassVar[MissionConditionCategory]
    MISSION_CONDITION_CATEGORY_TALK_NPC: _ClassVar[MissionConditionCategory]
    MISSION_CONDITION_CATEGORY_COMPLETE_CONDITION: _ClassVar[MissionConditionCategory]
    MISSION_CONDITION_CATEGORY_USE_ITEM: _ClassVar[MissionConditionCategory]
    MISSION_CONDITION_CATEGORY_INTERACT: _ClassVar[MissionConditionCategory]
    MISSION_CONDITION_CATEGORY_LEVEL_UP: _ClassVar[MissionConditionCategory]
    MISSION_CONDITION_CATEGORY_CUSTOM: _ClassVar[MissionConditionCategory]
    MISSION_CONDITION_CATEGORY_COMPLETE_MISSION: _ClassVar[MissionConditionCategory]
    MISSION_CONDITION_CATEGORY_PICKUP_ITEM: _ClassVar[MissionConditionCategory]

class MissionRewardState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MISSION_REWARD_STATE_NONE: _ClassVar[MissionRewardState]
    MISSION_REWARD_STATE_CLAIMABLE: _ClassVar[MissionRewardState]
    MISSION_REWARD_STATE_CLAIMED: _ClassVar[MissionRewardState]
MISSION_CONDITION_CATEGORY_UNSPECIFIED: MissionConditionCategory
MISSION_CONDITION_CATEGORY_KILL_MONSTER: MissionConditionCategory
MISSION_CONDITION_CATEGORY_TALK_NPC: MissionConditionCategory
MISSION_CONDITION_CATEGORY_COMPLETE_CONDITION: MissionConditionCategory
MISSION_CONDITION_CATEGORY_USE_ITEM: MissionConditionCategory
MISSION_CONDITION_CATEGORY_INTERACT: MissionConditionCategory
MISSION_CONDITION_CATEGORY_LEVEL_UP: MissionConditionCategory
MISSION_CONDITION_CATEGORY_CUSTOM: MissionConditionCategory
MISSION_CONDITION_CATEGORY_COMPLETE_MISSION: MissionConditionCategory
MISSION_CONDITION_CATEGORY_PICKUP_ITEM: MissionConditionCategory
MISSION_REWARD_STATE_NONE: MissionRewardState
MISSION_REWARD_STATE_CLAIMABLE: MissionRewardState
MISSION_REWARD_STATE_CLAIMED: MissionRewardState

class ActiveMission(_message.Message):
    __slots__ = ("mission_config_id", "progress", "targets", "accepted_at_ms")
    MISSION_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    PROGRESS_FIELD_NUMBER: _ClassVar[int]
    TARGETS_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    mission_config_id: int
    progress: _containers.RepeatedScalarFieldContainer[int]
    targets: _containers.RepeatedScalarFieldContainer[int]
    accepted_at_ms: int
    def __init__(self, mission_config_id: _Optional[int] = ..., progress: _Optional[_Iterable[int]] = ..., targets: _Optional[_Iterable[int]] = ..., accepted_at_ms: _Optional[int] = ...) -> None: ...

class CompletedMission(_message.Message):
    __slots__ = ("mission_config_id", "reward_state", "completed_at_ms")
    MISSION_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    REWARD_STATE_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    mission_config_id: int
    reward_state: MissionRewardState
    completed_at_ms: int
    def __init__(self, mission_config_id: _Optional[int] = ..., reward_state: _Optional[_Union[MissionRewardState, str]] = ..., completed_at_ms: _Optional[int] = ...) -> None: ...

class ListMissionsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListMissionsResponse(_message.Message):
    __slots__ = ("code", "active", "completed")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    active: _containers.RepeatedCompositeFieldContainer[ActiveMission]
    completed: _containers.RepeatedCompositeFieldContainer[CompletedMission]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., active: _Optional[_Iterable[_Union[ActiveMission, _Mapping]]] = ..., completed: _Optional[_Iterable[_Union[CompletedMission, _Mapping]]] = ...) -> None: ...

class AcceptMissionRequest(_message.Message):
    __slots__ = ("mission_config_id",)
    MISSION_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    mission_config_id: int
    def __init__(self, mission_config_id: _Optional[int] = ...) -> None: ...

class AcceptMissionResponse(_message.Message):
    __slots__ = ("code", "mission")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MISSION_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    mission: ActiveMission
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., mission: _Optional[_Union[ActiveMission, _Mapping]] = ...) -> None: ...

class AbandonMissionRequest(_message.Message):
    __slots__ = ("mission_config_id",)
    MISSION_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    mission_config_id: int
    def __init__(self, mission_config_id: _Optional[int] = ...) -> None: ...

class AbandonMissionResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class ClaimMissionRewardRequest(_message.Message):
    __slots__ = ("mission_config_id",)
    MISSION_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    mission_config_id: int
    def __init__(self, mission_config_id: _Optional[int] = ...) -> None: ...

class ClaimMissionRewardResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class MissionFact(_message.Message):
    __slots__ = ("condition_category", "condition_ids", "amount")
    CONDITION_CATEGORY_FIELD_NUMBER: _ClassVar[int]
    CONDITION_IDS_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    condition_category: MissionConditionCategory
    condition_ids: _containers.RepeatedScalarFieldContainer[int]
    amount: int
    def __init__(self, condition_category: _Optional[_Union[MissionConditionCategory, str]] = ..., condition_ids: _Optional[_Iterable[int]] = ..., amount: _Optional[int] = ...) -> None: ...

class ReportMissionFactsRequest(_message.Message):
    __slots__ = ("player_id", "facts", "idempotency_key")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    FACTS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    facts: _containers.RepeatedCompositeFieldContainer[MissionFact]
    idempotency_key: str
    def __init__(self, player_id: _Optional[int] = ..., facts: _Optional[_Iterable[_Union[MissionFact, _Mapping]]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class ReportMissionFactsResponse(_message.Message):
    __slots__ = ("code", "already")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ALREADY_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    already: bool
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., already: bool = ...) -> None: ...

class CompleteAllMissionsRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class CompleteAllMissionsResponse(_message.Message):
    __slots__ = ("code", "completed_count")
    CODE_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_COUNT_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    completed_count: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., completed_count: _Optional[int] = ...) -> None: ...

class MissionUpdateEvent(_message.Message):
    __slots__ = ("progressed", "completed", "auto_accepted", "ts_ms")
    PROGRESSED_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_FIELD_NUMBER: _ClassVar[int]
    AUTO_ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    progressed: _containers.RepeatedCompositeFieldContainer[ActiveMission]
    completed: _containers.RepeatedCompositeFieldContainer[CompletedMission]
    auto_accepted: _containers.RepeatedCompositeFieldContainer[ActiveMission]
    ts_ms: int
    def __init__(self, progressed: _Optional[_Iterable[_Union[ActiveMission, _Mapping]]] = ..., completed: _Optional[_Iterable[_Union[CompletedMission, _Mapping]]] = ..., auto_accepted: _Optional[_Iterable[_Union[ActiveMission, _Mapping]]] = ..., ts_ms: _Optional[int] = ...) -> None: ...

class MissionProgressStorageRecord(_message.Message):
    __slots__ = ("progress",)
    PROGRESS_FIELD_NUMBER: _ClassVar[int]
    progress: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, progress: _Optional[_Iterable[int]] = ...) -> None: ...

class MissionRewardItem(_message.Message):
    __slots__ = ("item_config_id", "count", "equipment")
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    EQUIPMENT_FIELD_NUMBER: _ClassVar[int]
    item_config_id: int
    count: int
    equipment: bool
    def __init__(self, item_config_id: _Optional[int] = ..., count: _Optional[int] = ..., equipment: bool = ...) -> None: ...

class MissionRewardStorageRecord(_message.Message):
    __slots__ = ("items", "exp")
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    EXP_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[MissionRewardItem]
    exp: int
    def __init__(self, items: _Optional[_Iterable[_Union[MissionRewardItem, _Mapping]]] = ..., exp: _Optional[int] = ...) -> None: ...
