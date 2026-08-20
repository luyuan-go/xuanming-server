from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from pandora.config.v1 import level_pb2 as _level_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BattlePlayerDepartureStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BATTLE_PLAYER_DEPARTURE_STATUS_UNSPECIFIED: _ClassVar[BattlePlayerDepartureStatus]
    BATTLE_PLAYER_DEPARTURE_STATUS_PENDING: _ClassVar[BattlePlayerDepartureStatus]
    BATTLE_PLAYER_DEPARTURE_STATUS_DEPARTED: _ClassVar[BattlePlayerDepartureStatus]
    BATTLE_PLAYER_DEPARTURE_STATUS_SOURCE_TORN_DOWN: _ClassVar[BattlePlayerDepartureStatus]

class BattleAuthPhase(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BATTLE_AUTH_PHASE_UNSPECIFIED: _ClassVar[BattleAuthPhase]
    BATTLE_AUTH_PHASE_BOOTSTRAP: _ClassVar[BattleAuthPhase]
    BATTLE_AUTH_PHASE_ACTIVE: _ClassVar[BattleAuthPhase]
    BATTLE_AUTH_PHASE_ROTATING: _ClassVar[BattleAuthPhase]
    BATTLE_AUTH_PHASE_QUARANTINED: _ClassVar[BattleAuthPhase]
    BATTLE_AUTH_PHASE_TERMINATING: _ClassVar[BattleAuthPhase]

class DSLifecyclePhase(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DS_LIFECYCLE_PHASE_UNSPECIFIED: _ClassVar[DSLifecyclePhase]
    DS_LIFECYCLE_PHASE_ALLOCATED: _ClassVar[DSLifecyclePhase]
    DS_LIFECYCLE_PHASE_RELEASED: _ClassVar[DSLifecyclePhase]
    DS_LIFECYCLE_PHASE_ABANDONED: _ClassVar[DSLifecyclePhase]
BATTLE_PLAYER_DEPARTURE_STATUS_UNSPECIFIED: BattlePlayerDepartureStatus
BATTLE_PLAYER_DEPARTURE_STATUS_PENDING: BattlePlayerDepartureStatus
BATTLE_PLAYER_DEPARTURE_STATUS_DEPARTED: BattlePlayerDepartureStatus
BATTLE_PLAYER_DEPARTURE_STATUS_SOURCE_TORN_DOWN: BattlePlayerDepartureStatus
BATTLE_AUTH_PHASE_UNSPECIFIED: BattleAuthPhase
BATTLE_AUTH_PHASE_BOOTSTRAP: BattleAuthPhase
BATTLE_AUTH_PHASE_ACTIVE: BattleAuthPhase
BATTLE_AUTH_PHASE_ROTATING: BattleAuthPhase
BATTLE_AUTH_PHASE_QUARANTINED: BattleAuthPhase
BATTLE_AUTH_PHASE_TERMINATING: BattleAuthPhase
DS_LIFECYCLE_PHASE_UNSPECIFIED: DSLifecyclePhase
DS_LIFECYCLE_PHASE_ALLOCATED: DSLifecyclePhase
DS_LIFECYCLE_PHASE_RELEASED: DSLifecyclePhase
DS_LIFECYCLE_PHASE_ABANDONED: DSLifecyclePhase

class ResolveBattleTargetRequest(_message.Message):
    __slots__ = ("match_id", "player_id")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    player_id: int
    def __init__(self, match_id: _Optional[int] = ..., player_id: _Optional[int] = ...) -> None: ...

class ResolveBattleTargetResponse(_message.Message):
    __slots__ = ("code", "ds_addr", "ds_pod_name", "allocated_at_ms", "gameserver_uid", "instance_epoch", "allocation_id", "release_track")
    CODE_FIELD_NUMBER: _ClassVar[int]
    DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    ALLOCATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    GAMESERVER_UID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    ds_addr: str
    ds_pod_name: str
    allocated_at_ms: int
    gameserver_uid: str
    instance_epoch: int
    allocation_id: str
    release_track: str
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., ds_addr: _Optional[str] = ..., ds_pod_name: _Optional[str] = ..., allocated_at_ms: _Optional[int] = ..., gameserver_uid: _Optional[str] = ..., instance_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., release_track: _Optional[str] = ...) -> None: ...

class AllocateBattleRequest(_message.Message):
    __slots__ = ("match_id", "player_ids", "map_id", "game_mode", "player_combat_factions", "rating_mode", "rating_pool")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    MAP_ID_FIELD_NUMBER: _ClassVar[int]
    GAME_MODE_FIELD_NUMBER: _ClassVar[int]
    PLAYER_COMBAT_FACTIONS_FIELD_NUMBER: _ClassVar[int]
    RATING_MODE_FIELD_NUMBER: _ClassVar[int]
    RATING_POOL_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    player_ids: _containers.RepeatedScalarFieldContainer[int]
    map_id: int
    game_mode: str
    player_combat_factions: _containers.RepeatedCompositeFieldContainer[BattlePlayerCombatFaction]
    rating_mode: _level_pb2.LevelRatingMode
    rating_pool: str
    def __init__(self, match_id: _Optional[int] = ..., player_ids: _Optional[_Iterable[int]] = ..., map_id: _Optional[int] = ..., game_mode: _Optional[str] = ..., player_combat_factions: _Optional[_Iterable[_Union[BattlePlayerCombatFaction, _Mapping]]] = ..., rating_mode: _Optional[_Union[_level_pb2.LevelRatingMode, str]] = ..., rating_pool: _Optional[str] = ...) -> None: ...

class AllocateBattleResponse(_message.Message):
    __slots__ = ("code", "ds_addr", "ds_pod_name", "allocated_at_ms", "gameserver_uid", "instance_epoch", "allocation_id", "release_track")
    CODE_FIELD_NUMBER: _ClassVar[int]
    DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    ALLOCATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    GAMESERVER_UID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    ds_addr: str
    ds_pod_name: str
    allocated_at_ms: int
    gameserver_uid: str
    instance_epoch: int
    allocation_id: str
    release_track: str
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., ds_addr: _Optional[str] = ..., ds_pod_name: _Optional[str] = ..., allocated_at_ms: _Optional[int] = ..., gameserver_uid: _Optional[str] = ..., instance_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., release_track: _Optional[str] = ...) -> None: ...

class ReleaseBattleRequest(_message.Message):
    __slots__ = ("match_id", "reason", "allocation_id", "ds_pod_name", "gameserver_uid", "instance_epoch", "auth_gen", "auth_jti", "auth_exp_ms", "auth_kid", "auth_token_sha256", "auth_writer_epoch", "authorized_at_ms")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    GAMESERVER_UID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    AUTH_GEN_FIELD_NUMBER: _ClassVar[int]
    AUTH_JTI_FIELD_NUMBER: _ClassVar[int]
    AUTH_EXP_MS_FIELD_NUMBER: _ClassVar[int]
    AUTH_KID_FIELD_NUMBER: _ClassVar[int]
    AUTH_TOKEN_SHA256_FIELD_NUMBER: _ClassVar[int]
    AUTH_WRITER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    AUTHORIZED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    reason: str
    allocation_id: str
    ds_pod_name: str
    gameserver_uid: str
    instance_epoch: int
    auth_gen: int
    auth_jti: str
    auth_exp_ms: int
    auth_kid: str
    auth_token_sha256: str
    auth_writer_epoch: int
    authorized_at_ms: int
    def __init__(self, match_id: _Optional[int] = ..., reason: _Optional[str] = ..., allocation_id: _Optional[str] = ..., ds_pod_name: _Optional[str] = ..., gameserver_uid: _Optional[str] = ..., instance_epoch: _Optional[int] = ..., auth_gen: _Optional[int] = ..., auth_jti: _Optional[str] = ..., auth_exp_ms: _Optional[int] = ..., auth_kid: _Optional[str] = ..., auth_token_sha256: _Optional[str] = ..., auth_writer_epoch: _Optional[int] = ..., authorized_at_ms: _Optional[int] = ...) -> None: ...

class ReleaseBattleResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class AbortPreactiveBattleRequest(_message.Message):
    __slots__ = ("match_id", "allocation_operation_id", "ds_pod_name", "gameserver_uid", "instance_epoch", "allocation_id", "release_track")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    GAMESERVER_UID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    allocation_operation_id: str
    ds_pod_name: str
    gameserver_uid: str
    instance_epoch: int
    allocation_id: str
    release_track: str
    def __init__(self, match_id: _Optional[int] = ..., allocation_operation_id: _Optional[str] = ..., ds_pod_name: _Optional[str] = ..., gameserver_uid: _Optional[str] = ..., instance_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., release_track: _Optional[str] = ...) -> None: ...

class AbortPreactiveBattleResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class EnsurePlayerDepartureRequest(_message.Message):
    __slots__ = ("match_id", "player_id", "operation_id", "ds_pod_name", "gameserver_uid", "instance_epoch", "allocation_id", "placement_version", "source_placement_version", "source_operation_id")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    GAMESERVER_UID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    player_id: int
    operation_id: str
    ds_pod_name: str
    gameserver_uid: str
    instance_epoch: int
    allocation_id: str
    placement_version: int
    source_placement_version: int
    source_operation_id: str
    def __init__(self, match_id: _Optional[int] = ..., player_id: _Optional[int] = ..., operation_id: _Optional[str] = ..., ds_pod_name: _Optional[str] = ..., gameserver_uid: _Optional[str] = ..., instance_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., placement_version: _Optional[int] = ..., source_placement_version: _Optional[int] = ..., source_operation_id: _Optional[str] = ...) -> None: ...

class EnsurePlayerDepartureResponse(_message.Message):
    __slots__ = ("code", "departed", "status")
    CODE_FIELD_NUMBER: _ClassVar[int]
    DEPARTED_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    departed: bool
    status: BattlePlayerDepartureStatus
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., departed: bool = ..., status: _Optional[_Union[BattlePlayerDepartureStatus, str]] = ...) -> None: ...

class BattleEvictionOrder(_message.Message):
    __slots__ = ("departure_id", "match_id", "player_id", "ds_pod_name", "gameserver_uid", "instance_epoch", "allocation_id", "placement_version", "operation_id")
    DEPARTURE_ID_FIELD_NUMBER: _ClassVar[int]
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    GAMESERVER_UID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    departure_id: str
    match_id: int
    player_id: int
    ds_pod_name: str
    gameserver_uid: str
    instance_epoch: int
    allocation_id: str
    placement_version: int
    operation_id: str
    def __init__(self, departure_id: _Optional[str] = ..., match_id: _Optional[int] = ..., player_id: _Optional[int] = ..., ds_pod_name: _Optional[str] = ..., gameserver_uid: _Optional[str] = ..., instance_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., placement_version: _Optional[int] = ..., operation_id: _Optional[str] = ...) -> None: ...

class HeartbeatRequest(_message.Message):
    __slots__ = ("ds_pod_name", "match_id", "player_count", "cpu_pct", "mem_mb", "state", "ts_ms", "active_player_ids", "active_player_snapshot_present", "acknowledged_departure_ids", "player_census_capability_version", "player_census_id")
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_COUNT_FIELD_NUMBER: _ClassVar[int]
    CPU_PCT_FIELD_NUMBER: _ClassVar[int]
    MEM_MB_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_PLAYER_SNAPSHOT_PRESENT_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_DEPARTURE_IDS_FIELD_NUMBER: _ClassVar[int]
    PLAYER_CENSUS_CAPABILITY_VERSION_FIELD_NUMBER: _ClassVar[int]
    PLAYER_CENSUS_ID_FIELD_NUMBER: _ClassVar[int]
    ds_pod_name: str
    match_id: int
    player_count: int
    cpu_pct: float
    mem_mb: float
    state: str
    ts_ms: int
    active_player_ids: _containers.RepeatedScalarFieldContainer[int]
    active_player_snapshot_present: bool
    acknowledged_departure_ids: _containers.RepeatedScalarFieldContainer[str]
    player_census_capability_version: int
    player_census_id: str
    def __init__(self, ds_pod_name: _Optional[str] = ..., match_id: _Optional[int] = ..., player_count: _Optional[int] = ..., cpu_pct: _Optional[float] = ..., mem_mb: _Optional[float] = ..., state: _Optional[str] = ..., ts_ms: _Optional[int] = ..., active_player_ids: _Optional[_Iterable[int]] = ..., active_player_snapshot_present: bool = ..., acknowledged_departure_ids: _Optional[_Iterable[str]] = ..., player_census_capability_version: _Optional[int] = ..., player_census_id: _Optional[str] = ...) -> None: ...

class HeartbeatResponse(_message.Message):
    __slots__ = ("code", "command", "accepted_token_gen", "accepted_token_jti", "accepted_instance_uid", "accepted_instance_epoch", "accepted_writer_epoch", "eviction_orders")
    CODE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_TOKEN_GEN_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_TOKEN_JTI_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_WRITER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    EVICTION_ORDERS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    command: str
    accepted_token_gen: int
    accepted_token_jti: str
    accepted_instance_uid: str
    accepted_instance_epoch: int
    accepted_writer_epoch: int
    eviction_orders: _containers.RepeatedCompositeFieldContainer[BattleEvictionOrder]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., command: _Optional[str] = ..., accepted_token_gen: _Optional[int] = ..., accepted_token_jti: _Optional[str] = ..., accepted_instance_uid: _Optional[str] = ..., accepted_instance_epoch: _Optional[int] = ..., accepted_writer_epoch: _Optional[int] = ..., eviction_orders: _Optional[_Iterable[_Union[BattleEvictionOrder, _Mapping]]] = ...) -> None: ...

class BattleInfo(_message.Message):
    __slots__ = ("match_id", "ds_pod_name", "ds_addr", "state", "player_count", "allocated_at_ms")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    PLAYER_COUNT_FIELD_NUMBER: _ClassVar[int]
    ALLOCATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    ds_pod_name: str
    ds_addr: str
    state: str
    player_count: int
    allocated_at_ms: int
    def __init__(self, match_id: _Optional[int] = ..., ds_pod_name: _Optional[str] = ..., ds_addr: _Optional[str] = ..., state: _Optional[str] = ..., player_count: _Optional[int] = ..., allocated_at_ms: _Optional[int] = ...) -> None: ...

class ListBattlesRequest(_message.Message):
    __slots__ = ("state_filter",)
    STATE_FILTER_FIELD_NUMBER: _ClassVar[int]
    state_filter: str
    def __init__(self, state_filter: _Optional[str] = ...) -> None: ...

class ListBattlesResponse(_message.Message):
    __slots__ = ("code", "battles")
    CODE_FIELD_NUMBER: _ClassVar[int]
    BATTLES_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    battles: _containers.RepeatedCompositeFieldContainer[BattleInfo]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., battles: _Optional[_Iterable[_Union[BattleInfo, _Mapping]]] = ...) -> None: ...

class BattleStorageRecord(_message.Message):
    __slots__ = ("match_id", "ds_pod_name", "ds_addr", "state", "player_ids", "map_id", "game_mode", "allocated_at_ms", "last_heartbeat_ms", "player_count", "empty_since_ms", "gameserver_uid", "instance_epoch", "last_verified_gen", "last_verified_jti", "last_verified_writer_epoch", "allocation_id", "release_track", "pod_uid", "player_combat_factions", "ever_had_players", "rating_mode", "rating_pool", "roster_incomplete_since_ms", "roster_ever_complete", "roster_policy_generation")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    MAP_ID_FIELD_NUMBER: _ClassVar[int]
    GAME_MODE_FIELD_NUMBER: _ClassVar[int]
    ALLOCATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    LAST_HEARTBEAT_MS_FIELD_NUMBER: _ClassVar[int]
    PLAYER_COUNT_FIELD_NUMBER: _ClassVar[int]
    EMPTY_SINCE_MS_FIELD_NUMBER: _ClassVar[int]
    GAMESERVER_UID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    LAST_VERIFIED_GEN_FIELD_NUMBER: _ClassVar[int]
    LAST_VERIFIED_JTI_FIELD_NUMBER: _ClassVar[int]
    LAST_VERIFIED_WRITER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    POD_UID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_COMBAT_FACTIONS_FIELD_NUMBER: _ClassVar[int]
    EVER_HAD_PLAYERS_FIELD_NUMBER: _ClassVar[int]
    RATING_MODE_FIELD_NUMBER: _ClassVar[int]
    RATING_POOL_FIELD_NUMBER: _ClassVar[int]
    ROSTER_INCOMPLETE_SINCE_MS_FIELD_NUMBER: _ClassVar[int]
    ROSTER_EVER_COMPLETE_FIELD_NUMBER: _ClassVar[int]
    ROSTER_POLICY_GENERATION_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    ds_pod_name: str
    ds_addr: str
    state: str
    player_ids: _containers.RepeatedScalarFieldContainer[int]
    map_id: int
    game_mode: str
    allocated_at_ms: int
    last_heartbeat_ms: int
    player_count: int
    empty_since_ms: int
    gameserver_uid: str
    instance_epoch: int
    last_verified_gen: int
    last_verified_jti: str
    last_verified_writer_epoch: int
    allocation_id: str
    release_track: str
    pod_uid: str
    player_combat_factions: _containers.RepeatedCompositeFieldContainer[BattlePlayerCombatFaction]
    ever_had_players: bool
    rating_mode: _level_pb2.LevelRatingMode
    rating_pool: str
    roster_incomplete_since_ms: int
    roster_ever_complete: bool
    roster_policy_generation: int
    def __init__(self, match_id: _Optional[int] = ..., ds_pod_name: _Optional[str] = ..., ds_addr: _Optional[str] = ..., state: _Optional[str] = ..., player_ids: _Optional[_Iterable[int]] = ..., map_id: _Optional[int] = ..., game_mode: _Optional[str] = ..., allocated_at_ms: _Optional[int] = ..., last_heartbeat_ms: _Optional[int] = ..., player_count: _Optional[int] = ..., empty_since_ms: _Optional[int] = ..., gameserver_uid: _Optional[str] = ..., instance_epoch: _Optional[int] = ..., last_verified_gen: _Optional[int] = ..., last_verified_jti: _Optional[str] = ..., last_verified_writer_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., release_track: _Optional[str] = ..., pod_uid: _Optional[str] = ..., player_combat_factions: _Optional[_Iterable[_Union[BattlePlayerCombatFaction, _Mapping]]] = ..., ever_had_players: bool = ..., rating_mode: _Optional[_Union[_level_pb2.LevelRatingMode, str]] = ..., rating_pool: _Optional[str] = ..., roster_incomplete_since_ms: _Optional[int] = ..., roster_ever_complete: bool = ..., roster_policy_generation: _Optional[int] = ...) -> None: ...

class BattleDSCredential(_message.Message):
    __slots__ = ("gen", "jti", "exp_ms", "kid", "instance_uid", "instance_epoch", "token_sha256", "writer_epoch")
    GEN_FIELD_NUMBER: _ClassVar[int]
    JTI_FIELD_NUMBER: _ClassVar[int]
    EXP_MS_FIELD_NUMBER: _ClassVar[int]
    KID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    TOKEN_SHA256_FIELD_NUMBER: _ClassVar[int]
    WRITER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    gen: int
    jti: str
    exp_ms: int
    kid: str
    instance_uid: str
    instance_epoch: int
    token_sha256: str
    writer_epoch: int
    def __init__(self, gen: _Optional[int] = ..., jti: _Optional[str] = ..., exp_ms: _Optional[int] = ..., kid: _Optional[str] = ..., instance_uid: _Optional[str] = ..., instance_epoch: _Optional[int] = ..., token_sha256: _Optional[str] = ..., writer_epoch: _Optional[int] = ...) -> None: ...

class BattleDSAuthStorageRecord(_message.Message):
    __slots__ = ("match_id", "ds_pod_name", "instance_uid", "instance_epoch", "phase", "active", "pending", "high_water_gen", "pending_started_ms", "delivered_rv", "updated_at_ms", "required_writer_epoch", "allocation_id", "last_active_heartbeat_ms")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    PENDING_FIELD_NUMBER: _ClassVar[int]
    HIGH_WATER_GEN_FIELD_NUMBER: _ClassVar[int]
    PENDING_STARTED_MS_FIELD_NUMBER: _ClassVar[int]
    DELIVERED_RV_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_WRITER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_ACTIVE_HEARTBEAT_MS_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    ds_pod_name: str
    instance_uid: str
    instance_epoch: int
    phase: BattleAuthPhase
    active: BattleDSCredential
    pending: BattleDSCredential
    high_water_gen: int
    pending_started_ms: int
    delivered_rv: str
    updated_at_ms: int
    required_writer_epoch: int
    allocation_id: str
    last_active_heartbeat_ms: int
    def __init__(self, match_id: _Optional[int] = ..., ds_pod_name: _Optional[str] = ..., instance_uid: _Optional[str] = ..., instance_epoch: _Optional[int] = ..., phase: _Optional[_Union[BattleAuthPhase, str]] = ..., active: _Optional[_Union[BattleDSCredential, _Mapping]] = ..., pending: _Optional[_Union[BattleDSCredential, _Mapping]] = ..., high_water_gen: _Optional[int] = ..., pending_started_ms: _Optional[int] = ..., delivered_rv: _Optional[str] = ..., updated_at_ms: _Optional[int] = ..., required_writer_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., last_active_heartbeat_ms: _Optional[int] = ...) -> None: ...

class BattlePlayerDepartureStorageRecord(_message.Message):
    __slots__ = ("departure_id", "match_id", "player_id", "operation_id", "ds_pod_name", "gameserver_uid", "instance_epoch", "allocation_id", "status", "requested_at_ms", "issued_at_ms", "departed_at_ms", "placement_version", "source_placement_version", "source_operation_id", "acknowledged_at_ms", "acknowledged_census_id")
    DEPARTURE_ID_FIELD_NUMBER: _ClassVar[int]
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    GAMESERVER_UID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    ISSUED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    DEPARTED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_CENSUS_ID_FIELD_NUMBER: _ClassVar[int]
    departure_id: str
    match_id: int
    player_id: int
    operation_id: str
    ds_pod_name: str
    gameserver_uid: str
    instance_epoch: int
    allocation_id: str
    status: BattlePlayerDepartureStatus
    requested_at_ms: int
    issued_at_ms: int
    departed_at_ms: int
    placement_version: int
    source_placement_version: int
    source_operation_id: str
    acknowledged_at_ms: int
    acknowledged_census_id: str
    def __init__(self, departure_id: _Optional[str] = ..., match_id: _Optional[int] = ..., player_id: _Optional[int] = ..., operation_id: _Optional[str] = ..., ds_pod_name: _Optional[str] = ..., gameserver_uid: _Optional[str] = ..., instance_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., status: _Optional[_Union[BattlePlayerDepartureStatus, str]] = ..., requested_at_ms: _Optional[int] = ..., issued_at_ms: _Optional[int] = ..., departed_at_ms: _Optional[int] = ..., placement_version: _Optional[int] = ..., source_placement_version: _Optional[int] = ..., source_operation_id: _Optional[str] = ..., acknowledged_at_ms: _Optional[int] = ..., acknowledged_census_id: _Optional[str] = ...) -> None: ...

class BattlePlayerDepartureJournalStorageRecord(_message.Message):
    __slots__ = ("match_id", "departures")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    DEPARTURES_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    departures: _containers.RepeatedCompositeFieldContainer[BattlePlayerDepartureStorageRecord]
    def __init__(self, match_id: _Optional[int] = ..., departures: _Optional[_Iterable[_Union[BattlePlayerDepartureStorageRecord, _Mapping]]] = ...) -> None: ...

class BattleInstanceTeardownStorageRecord(_message.Message):
    __slots__ = ("match_id", "ds_pod_name", "gameserver_uid", "instance_epoch", "allocation_id", "torn_down_at_ms", "pod_uid")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    GAMESERVER_UID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    TORN_DOWN_AT_MS_FIELD_NUMBER: _ClassVar[int]
    POD_UID_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    ds_pod_name: str
    gameserver_uid: str
    instance_epoch: int
    allocation_id: str
    torn_down_at_ms: int
    pod_uid: str
    def __init__(self, match_id: _Optional[int] = ..., ds_pod_name: _Optional[str] = ..., gameserver_uid: _Optional[str] = ..., instance_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., torn_down_at_ms: _Optional[int] = ..., pod_uid: _Optional[str] = ...) -> None: ...

class BattleAllocationAbortStorageRecord(_message.Message):
    __slots__ = ("match_id", "allocation_operation_id", "ds_pod_name", "gameserver_uid", "instance_epoch", "allocation_id", "release_track", "requested_at_ms", "released_at_ms")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    GAMESERVER_UID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    RELEASED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    allocation_operation_id: str
    ds_pod_name: str
    gameserver_uid: str
    instance_epoch: int
    allocation_id: str
    release_track: str
    requested_at_ms: int
    released_at_ms: int
    def __init__(self, match_id: _Optional[int] = ..., allocation_operation_id: _Optional[str] = ..., ds_pod_name: _Optional[str] = ..., gameserver_uid: _Optional[str] = ..., instance_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., release_track: _Optional[str] = ..., requested_at_ms: _Optional[int] = ..., released_at_ms: _Optional[int] = ...) -> None: ...

class BattleAllocationLifecyclePublishedStorageRecord(_message.Message):
    __slots__ = ("match_id", "ds_pod_name", "gameserver_uid", "instance_epoch", "allocation_id", "release_track", "phase", "published_at_ms")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    GAMESERVER_UID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    PUBLISHED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    ds_pod_name: str
    gameserver_uid: str
    instance_epoch: int
    allocation_id: str
    release_track: str
    phase: DSLifecyclePhase
    published_at_ms: int
    def __init__(self, match_id: _Optional[int] = ..., ds_pod_name: _Optional[str] = ..., gameserver_uid: _Optional[str] = ..., instance_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., release_track: _Optional[str] = ..., phase: _Optional[_Union[DSLifecyclePhase, str]] = ..., published_at_ms: _Optional[int] = ...) -> None: ...

class DSLifecycleEvent(_message.Message):
    __slots__ = ("match_id", "ds_pod_name", "phase", "player_ids", "map_id", "game_mode", "ts_ms")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    MAP_ID_FIELD_NUMBER: _ClassVar[int]
    GAME_MODE_FIELD_NUMBER: _ClassVar[int]
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    ds_pod_name: str
    phase: DSLifecyclePhase
    player_ids: _containers.RepeatedScalarFieldContainer[int]
    map_id: int
    game_mode: str
    ts_ms: int
    def __init__(self, match_id: _Optional[int] = ..., ds_pod_name: _Optional[str] = ..., phase: _Optional[_Union[DSLifecyclePhase, str]] = ..., player_ids: _Optional[_Iterable[int]] = ..., map_id: _Optional[int] = ..., game_mode: _Optional[str] = ..., ts_ms: _Optional[int] = ...) -> None: ...

class BattlePlayerCombatFaction(_message.Message):
    __slots__ = ("player_id", "combat_faction_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    COMBAT_FACTION_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    combat_faction_id: int
    def __init__(self, player_id: _Optional[int] = ..., combat_faction_id: _Optional[int] = ...) -> None: ...
