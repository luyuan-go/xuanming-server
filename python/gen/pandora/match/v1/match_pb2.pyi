from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from pandora.config.v1 import level_pb2 as _level_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MatchStage(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MATCH_STAGE_UNSPECIFIED: _ClassVar[MatchStage]
    MATCH_STAGE_QUEUEING: _ClassVar[MatchStage]
    MATCH_STAGE_FOUND: _ClassVar[MatchStage]
    MATCH_STAGE_CONFIRM: _ClassVar[MatchStage]
    MATCH_STAGE_ALLOCATING: _ClassVar[MatchStage]
    MATCH_STAGE_READY: _ClassVar[MatchStage]
    MATCH_STAGE_FAILED: _ClassVar[MatchStage]

class MatchAllocationPhase(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MATCH_ALLOCATION_PHASE_UNSPECIFIED: _ClassVar[MatchAllocationPhase]
    MATCH_ALLOCATION_PHASE_PENDING: _ClassVar[MatchAllocationPhase]
    MATCH_ALLOCATION_PHASE_REQUESTING: _ClassVar[MatchAllocationPhase]
    MATCH_ALLOCATION_PHASE_COMPLETED: _ClassVar[MatchAllocationPhase]
    MATCH_ALLOCATION_PHASE_FAILED: _ClassVar[MatchAllocationPhase]
    MATCH_ALLOCATION_PHASE_ABORTING: _ClassVar[MatchAllocationPhase]

class MatchStartPhase(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MATCH_START_PHASE_UNSPECIFIED: _ClassVar[MatchStartPhase]
    MATCH_START_PHASE_ACCEPTED: _ClassVar[MatchStartPhase]
    MATCH_START_PHASE_TICKET_READY: _ClassVar[MatchStartPhase]
    MATCH_START_PHASE_CLAIMING: _ClassVar[MatchStartPhase]
    MATCH_START_PHASE_CLAIMS_READY: _ClassVar[MatchStartPhase]
    MATCH_START_PHASE_QUEUED: _ClassVar[MatchStartPhase]
    MATCH_START_PHASE_COMPENSATING: _ClassVar[MatchStartPhase]
    MATCH_START_PHASE_FAILED: _ClassVar[MatchStartPhase]

class PlayerMatchContextState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PLAYER_MATCH_CONTEXT_STATE_UNSPECIFIED: _ClassVar[PlayerMatchContextState]
    PLAYER_MATCH_CONTEXT_STATE_NONE: _ClassVar[PlayerMatchContextState]
    PLAYER_MATCH_CONTEXT_STATE_ACTIVE: _ClassVar[PlayerMatchContextState]

class PlayerMatchResumeStage(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PLAYER_MATCH_RESUME_STAGE_UNSPECIFIED: _ClassVar[PlayerMatchResumeStage]
    PLAYER_MATCH_RESUME_STAGE_STARTING: _ClassVar[PlayerMatchResumeStage]
    PLAYER_MATCH_RESUME_STAGE_QUEUED: _ClassVar[PlayerMatchResumeStage]
    PLAYER_MATCH_RESUME_STAGE_CONFIRMING: _ClassVar[PlayerMatchResumeStage]
    PLAYER_MATCH_RESUME_STAGE_ALLOCATING: _ClassVar[PlayerMatchResumeStage]
    PLAYER_MATCH_RESUME_STAGE_READY: _ClassVar[PlayerMatchResumeStage]

class MatchPushEventType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MATCH_PUSH_EVENT_TYPE_UNSPECIFIED: _ClassVar[MatchPushEventType]

class MatchConfirmStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MATCH_CONFIRM_STATUS_UNSPECIFIED: _ClassVar[MatchConfirmStatus]
    MATCH_CONFIRM_STATUS_PENDING: _ClassVar[MatchConfirmStatus]
    MATCH_CONFIRM_STATUS_ACCEPTED: _ClassVar[MatchConfirmStatus]
    MATCH_CONFIRM_STATUS_REJECTED: _ClassVar[MatchConfirmStatus]
MATCH_STAGE_UNSPECIFIED: MatchStage
MATCH_STAGE_QUEUEING: MatchStage
MATCH_STAGE_FOUND: MatchStage
MATCH_STAGE_CONFIRM: MatchStage
MATCH_STAGE_ALLOCATING: MatchStage
MATCH_STAGE_READY: MatchStage
MATCH_STAGE_FAILED: MatchStage
MATCH_ALLOCATION_PHASE_UNSPECIFIED: MatchAllocationPhase
MATCH_ALLOCATION_PHASE_PENDING: MatchAllocationPhase
MATCH_ALLOCATION_PHASE_REQUESTING: MatchAllocationPhase
MATCH_ALLOCATION_PHASE_COMPLETED: MatchAllocationPhase
MATCH_ALLOCATION_PHASE_FAILED: MatchAllocationPhase
MATCH_ALLOCATION_PHASE_ABORTING: MatchAllocationPhase
MATCH_START_PHASE_UNSPECIFIED: MatchStartPhase
MATCH_START_PHASE_ACCEPTED: MatchStartPhase
MATCH_START_PHASE_TICKET_READY: MatchStartPhase
MATCH_START_PHASE_CLAIMING: MatchStartPhase
MATCH_START_PHASE_CLAIMS_READY: MatchStartPhase
MATCH_START_PHASE_QUEUED: MatchStartPhase
MATCH_START_PHASE_COMPENSATING: MatchStartPhase
MATCH_START_PHASE_FAILED: MatchStartPhase
PLAYER_MATCH_CONTEXT_STATE_UNSPECIFIED: PlayerMatchContextState
PLAYER_MATCH_CONTEXT_STATE_NONE: PlayerMatchContextState
PLAYER_MATCH_CONTEXT_STATE_ACTIVE: PlayerMatchContextState
PLAYER_MATCH_RESUME_STAGE_UNSPECIFIED: PlayerMatchResumeStage
PLAYER_MATCH_RESUME_STAGE_STARTING: PlayerMatchResumeStage
PLAYER_MATCH_RESUME_STAGE_QUEUED: PlayerMatchResumeStage
PLAYER_MATCH_RESUME_STAGE_CONFIRMING: PlayerMatchResumeStage
PLAYER_MATCH_RESUME_STAGE_ALLOCATING: PlayerMatchResumeStage
PLAYER_MATCH_RESUME_STAGE_READY: PlayerMatchResumeStage
MATCH_PUSH_EVENT_TYPE_UNSPECIFIED: MatchPushEventType
MATCH_CONFIRM_STATUS_UNSPECIFIED: MatchConfirmStatus
MATCH_CONFIRM_STATUS_PENDING: MatchConfirmStatus
MATCH_CONFIRM_STATUS_ACCEPTED: MatchConfirmStatus
MATCH_CONFIRM_STATUS_REJECTED: MatchConfirmStatus

class StartMatchRequest(_message.Message):
    __slots__ = ("team_id", "map_id", "entry_mode")
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    MAP_ID_FIELD_NUMBER: _ClassVar[int]
    ENTRY_MODE_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    map_id: int
    entry_mode: _level_pb2.LevelEntryMode
    def __init__(self, team_id: _Optional[int] = ..., map_id: _Optional[int] = ..., entry_mode: _Optional[_Union[_level_pb2.LevelEntryMode, str]] = ...) -> None: ...

class StartMatchResponse(_message.Message):
    __slots__ = ("code", "match_id", "absent_player_ids")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    ABSENT_PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    match_id: int
    absent_player_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., match_id: _Optional[int] = ..., absent_player_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class CancelMatchRequest(_message.Message):
    __slots__ = ("match_id", "player_id")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    player_id: int
    def __init__(self, match_id: _Optional[int] = ..., player_id: _Optional[int] = ...) -> None: ...

class CancelMatchResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class ConfirmMatchRequest(_message.Message):
    __slots__ = ("match_id", "accept")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    ACCEPT_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    accept: bool
    def __init__(self, match_id: _Optional[int] = ..., accept: bool = ...) -> None: ...

class ConfirmMatchResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class GetMatchProgressRequest(_message.Message):
    __slots__ = ("match_id",)
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    def __init__(self, match_id: _Optional[int] = ...) -> None: ...

class GetMatchProgressResponse(_message.Message):
    __slots__ = ("code", "progress")
    CODE_FIELD_NUMBER: _ClassVar[int]
    PROGRESS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    progress: MatchProgress
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., progress: _Optional[_Union[MatchProgress, _Mapping]] = ...) -> None: ...

class ReleaseMatchRequest(_message.Message):
    __slots__ = ("match_id", "player_ids")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    player_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, match_id: _Optional[int] = ..., player_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class ReleaseMatchResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class ResolvePlayerMatchContextRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class ResolvePlayerMatchContextResponse(_message.Message):
    __slots__ = ("code", "state", "stage", "ticket_id", "match_id", "battle_ds_addr", "battle_ticket", "placement_version", "placement_operation_id", "game_mode", "map_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    STAGE_FIELD_NUMBER: _ClassVar[int]
    TICKET_ID_FIELD_NUMBER: _ClassVar[int]
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    BATTLE_DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    BATTLE_TICKET_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    GAME_MODE_FIELD_NUMBER: _ClassVar[int]
    MAP_ID_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    state: PlayerMatchContextState
    stage: PlayerMatchResumeStage
    ticket_id: int
    match_id: int
    battle_ds_addr: str
    battle_ticket: str
    placement_version: int
    placement_operation_id: str
    game_mode: str
    map_id: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., state: _Optional[_Union[PlayerMatchContextState, str]] = ..., stage: _Optional[_Union[PlayerMatchResumeStage, str]] = ..., ticket_id: _Optional[int] = ..., match_id: _Optional[int] = ..., battle_ds_addr: _Optional[str] = ..., battle_ticket: _Optional[str] = ..., placement_version: _Optional[int] = ..., placement_operation_id: _Optional[str] = ..., game_mode: _Optional[str] = ..., map_id: _Optional[int] = ...) -> None: ...

class MatchProgress(_message.Message):
    __slots__ = ("match_id", "stage", "queue_seconds", "estimated_wait_seconds", "battle_ds_addr", "battle_ticket", "team_a", "team_b", "map_id")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_FIELD_NUMBER: _ClassVar[int]
    QUEUE_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ESTIMATED_WAIT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    BATTLE_DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    BATTLE_TICKET_FIELD_NUMBER: _ClassVar[int]
    TEAM_A_FIELD_NUMBER: _ClassVar[int]
    TEAM_B_FIELD_NUMBER: _ClassVar[int]
    MAP_ID_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    stage: MatchStage
    queue_seconds: int
    estimated_wait_seconds: int
    battle_ds_addr: str
    battle_ticket: str
    team_a: _containers.RepeatedScalarFieldContainer[int]
    team_b: _containers.RepeatedScalarFieldContainer[int]
    map_id: int
    def __init__(self, match_id: _Optional[int] = ..., stage: _Optional[_Union[MatchStage, str]] = ..., queue_seconds: _Optional[int] = ..., estimated_wait_seconds: _Optional[int] = ..., battle_ds_addr: _Optional[str] = ..., battle_ticket: _Optional[str] = ..., team_a: _Optional[_Iterable[int]] = ..., team_b: _Optional[_Iterable[int]] = ..., map_id: _Optional[int] = ...) -> None: ...

class MatchProgressEvent(_message.Message):
    __slots__ = ("progress", "to_player_id", "ts_ms")
    PROGRESS_FIELD_NUMBER: _ClassVar[int]
    TO_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    progress: MatchProgress
    to_player_id: int
    ts_ms: int
    def __init__(self, progress: _Optional[_Union[MatchProgress, _Mapping]] = ..., to_player_id: _Optional[int] = ..., ts_ms: _Optional[int] = ...) -> None: ...

class MatchMemberStorageRecord(_message.Message):
    __slots__ = ("player_id", "team_id", "mmr", "hero_id", "side", "confirm", "team_ready_generation")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    MMR_FIELD_NUMBER: _ClassVar[int]
    HERO_ID_FIELD_NUMBER: _ClassVar[int]
    SIDE_FIELD_NUMBER: _ClassVar[int]
    CONFIRM_FIELD_NUMBER: _ClassVar[int]
    TEAM_READY_GENERATION_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    team_id: int
    mmr: int
    hero_id: int
    side: int
    confirm: MatchConfirmStatus
    team_ready_generation: int
    def __init__(self, player_id: _Optional[int] = ..., team_id: _Optional[int] = ..., mmr: _Optional[int] = ..., hero_id: _Optional[int] = ..., side: _Optional[int] = ..., confirm: _Optional[_Union[MatchConfirmStatus, str]] = ..., team_ready_generation: _Optional[int] = ...) -> None: ...

class MatchTicketStorageRecord(_message.Message):
    __slots__ = ("ticket_id", "team_id", "captain_id", "members", "avg_mmr", "enqueued_at_ms", "match_id", "map_id", "game_mode", "entry_mode", "rating_mode", "rating_pool")
    TICKET_ID_FIELD_NUMBER: _ClassVar[int]
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    CAPTAIN_ID_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    AVG_MMR_FIELD_NUMBER: _ClassVar[int]
    ENQUEUED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    MAP_ID_FIELD_NUMBER: _ClassVar[int]
    GAME_MODE_FIELD_NUMBER: _ClassVar[int]
    ENTRY_MODE_FIELD_NUMBER: _ClassVar[int]
    RATING_MODE_FIELD_NUMBER: _ClassVar[int]
    RATING_POOL_FIELD_NUMBER: _ClassVar[int]
    ticket_id: int
    team_id: int
    captain_id: int
    members: _containers.RepeatedCompositeFieldContainer[MatchMemberStorageRecord]
    avg_mmr: int
    enqueued_at_ms: int
    match_id: int
    map_id: int
    game_mode: str
    entry_mode: _level_pb2.LevelEntryMode
    rating_mode: _level_pb2.LevelRatingMode
    rating_pool: str
    def __init__(self, ticket_id: _Optional[int] = ..., team_id: _Optional[int] = ..., captain_id: _Optional[int] = ..., members: _Optional[_Iterable[_Union[MatchMemberStorageRecord, _Mapping]]] = ..., avg_mmr: _Optional[int] = ..., enqueued_at_ms: _Optional[int] = ..., match_id: _Optional[int] = ..., map_id: _Optional[int] = ..., game_mode: _Optional[str] = ..., entry_mode: _Optional[_Union[_level_pb2.LevelEntryMode, str]] = ..., rating_mode: _Optional[_Union[_level_pb2.LevelRatingMode, str]] = ..., rating_pool: _Optional[str] = ...) -> None: ...

class MatchStorageRecord(_message.Message):
    __slots__ = ("match_id", "stage", "members", "ticket_ids", "created_at_ms", "confirm_deadline_ms", "battle_ds_addr", "battle_ticket", "map_id", "allocation_operation_id", "allocation_phase", "allocation_attempt", "allocation_next_attempt_at_ms", "battle_target", "game_mode", "rating_mode", "rating_pool")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    TICKET_IDS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    CONFIRM_DEADLINE_MS_FIELD_NUMBER: _ClassVar[int]
    BATTLE_DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    BATTLE_TICKET_FIELD_NUMBER: _ClassVar[int]
    MAP_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_PHASE_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_NEXT_ATTEMPT_AT_MS_FIELD_NUMBER: _ClassVar[int]
    BATTLE_TARGET_FIELD_NUMBER: _ClassVar[int]
    GAME_MODE_FIELD_NUMBER: _ClassVar[int]
    RATING_MODE_FIELD_NUMBER: _ClassVar[int]
    RATING_POOL_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    stage: MatchStage
    members: _containers.RepeatedCompositeFieldContainer[MatchMemberStorageRecord]
    ticket_ids: _containers.RepeatedScalarFieldContainer[int]
    created_at_ms: int
    confirm_deadline_ms: int
    battle_ds_addr: str
    battle_ticket: str
    map_id: int
    allocation_operation_id: str
    allocation_phase: MatchAllocationPhase
    allocation_attempt: int
    allocation_next_attempt_at_ms: int
    battle_target: MatchBattleTargetStorageRecord
    game_mode: str
    rating_mode: _level_pb2.LevelRatingMode
    rating_pool: str
    def __init__(self, match_id: _Optional[int] = ..., stage: _Optional[_Union[MatchStage, str]] = ..., members: _Optional[_Iterable[_Union[MatchMemberStorageRecord, _Mapping]]] = ..., ticket_ids: _Optional[_Iterable[int]] = ..., created_at_ms: _Optional[int] = ..., confirm_deadline_ms: _Optional[int] = ..., battle_ds_addr: _Optional[str] = ..., battle_ticket: _Optional[str] = ..., map_id: _Optional[int] = ..., allocation_operation_id: _Optional[str] = ..., allocation_phase: _Optional[_Union[MatchAllocationPhase, str]] = ..., allocation_attempt: _Optional[int] = ..., allocation_next_attempt_at_ms: _Optional[int] = ..., battle_target: _Optional[_Union[MatchBattleTargetStorageRecord, _Mapping]] = ..., game_mode: _Optional[str] = ..., rating_mode: _Optional[_Union[_level_pb2.LevelRatingMode, str]] = ..., rating_pool: _Optional[str] = ...) -> None: ...

class MatchPlayerPlacementBindingStorageRecord(_message.Message):
    __slots__ = ("player_id", "placement_version", "operation_id", "source_match_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    placement_version: int
    operation_id: str
    source_match_id: int
    def __init__(self, player_id: _Optional[int] = ..., placement_version: _Optional[int] = ..., operation_id: _Optional[str] = ..., source_match_id: _Optional[int] = ...) -> None: ...

class MatchBattleTargetStorageRecord(_message.Message):
    __slots__ = ("ds_addr", "ds_pod_name", "ds_instance_uid", "ds_instance_epoch", "allocation_id", "release_track", "player_bindings")
    DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    DS_INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    DS_INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    PLAYER_BINDINGS_FIELD_NUMBER: _ClassVar[int]
    ds_addr: str
    ds_pod_name: str
    ds_instance_uid: str
    ds_instance_epoch: int
    allocation_id: str
    release_track: str
    player_bindings: _containers.RepeatedCompositeFieldContainer[MatchPlayerPlacementBindingStorageRecord]
    def __init__(self, ds_addr: _Optional[str] = ..., ds_pod_name: _Optional[str] = ..., ds_instance_uid: _Optional[str] = ..., ds_instance_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., release_track: _Optional[str] = ..., player_bindings: _Optional[_Iterable[_Union[MatchPlayerPlacementBindingStorageRecord, _Mapping]]] = ...) -> None: ...

class MatchStartOperationStorageRecord(_message.Message):
    __slots__ = ("operation_id", "ticket_id", "team_id", "captain_id", "members", "avg_mmr", "map_id", "phase", "claimed_player_ids", "attempt", "next_attempt_at_ms", "lease_token", "lease_deadline_ms", "created_at_ms", "game_mode", "entry_mode", "rating_mode", "rating_pool")
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    TICKET_ID_FIELD_NUMBER: _ClassVar[int]
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    CAPTAIN_ID_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    AVG_MMR_FIELD_NUMBER: _ClassVar[int]
    MAP_ID_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    CLAIMED_PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    NEXT_ATTEMPT_AT_MS_FIELD_NUMBER: _ClassVar[int]
    LEASE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    LEASE_DEADLINE_MS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    GAME_MODE_FIELD_NUMBER: _ClassVar[int]
    ENTRY_MODE_FIELD_NUMBER: _ClassVar[int]
    RATING_MODE_FIELD_NUMBER: _ClassVar[int]
    RATING_POOL_FIELD_NUMBER: _ClassVar[int]
    operation_id: str
    ticket_id: int
    team_id: int
    captain_id: int
    members: _containers.RepeatedCompositeFieldContainer[MatchMemberStorageRecord]
    avg_mmr: int
    map_id: int
    phase: MatchStartPhase
    claimed_player_ids: _containers.RepeatedScalarFieldContainer[int]
    attempt: int
    next_attempt_at_ms: int
    lease_token: str
    lease_deadline_ms: int
    created_at_ms: int
    game_mode: str
    entry_mode: _level_pb2.LevelEntryMode
    rating_mode: _level_pb2.LevelRatingMode
    rating_pool: str
    def __init__(self, operation_id: _Optional[str] = ..., ticket_id: _Optional[int] = ..., team_id: _Optional[int] = ..., captain_id: _Optional[int] = ..., members: _Optional[_Iterable[_Union[MatchMemberStorageRecord, _Mapping]]] = ..., avg_mmr: _Optional[int] = ..., map_id: _Optional[int] = ..., phase: _Optional[_Union[MatchStartPhase, str]] = ..., claimed_player_ids: _Optional[_Iterable[int]] = ..., attempt: _Optional[int] = ..., next_attempt_at_ms: _Optional[int] = ..., lease_token: _Optional[str] = ..., lease_deadline_ms: _Optional[int] = ..., created_at_ms: _Optional[int] = ..., game_mode: _Optional[str] = ..., entry_mode: _Optional[_Union[_level_pb2.LevelEntryMode, str]] = ..., rating_mode: _Optional[_Union[_level_pb2.LevelRatingMode, str]] = ..., rating_pool: _Optional[str] = ...) -> None: ...

class MatchReleaseStorageRecord(_message.Message):
    __slots__ = ("operation_id", "match_id", "player_ids", "created_at_ms")
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    operation_id: str
    match_id: int
    player_ids: _containers.RepeatedScalarFieldContainer[int]
    created_at_ms: int
    def __init__(self, operation_id: _Optional[str] = ..., match_id: _Optional[int] = ..., player_ids: _Optional[_Iterable[int]] = ..., created_at_ms: _Optional[int] = ...) -> None: ...
