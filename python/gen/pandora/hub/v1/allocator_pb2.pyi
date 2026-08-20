from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class HubAuthPhase(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    HUB_AUTH_PHASE_UNSPECIFIED: _ClassVar[HubAuthPhase]
    HUB_AUTH_PHASE_BOOTSTRAP: _ClassVar[HubAuthPhase]
    HUB_AUTH_PHASE_ACTIVE: _ClassVar[HubAuthPhase]
    HUB_AUTH_PHASE_ROTATING: _ClassVar[HubAuthPhase]
    HUB_AUTH_PHASE_QUARANTINED: _ClassVar[HubAuthPhase]
    HUB_AUTH_PHASE_TERMINATING: _ClassVar[HubAuthPhase]
HUB_AUTH_PHASE_UNSPECIFIED: HubAuthPhase
HUB_AUTH_PHASE_BOOTSTRAP: HubAuthPhase
HUB_AUTH_PHASE_ACTIVE: HubAuthPhase
HUB_AUTH_PHASE_ROTATING: HubAuthPhase
HUB_AUTH_PHASE_QUARANTINED: HubAuthPhase
HUB_AUTH_PHASE_TERMINATING: HubAuthPhase

class AssignHubRequest(_message.Message):
    __slots__ = ("player_id", "region", "team_id", "role_id", "placement_version", "placement_operation_id", "source_match_id", "session_jti")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_JTI_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    region: str
    team_id: int
    role_id: int
    placement_version: int
    placement_operation_id: str
    source_match_id: int
    session_jti: str
    def __init__(self, player_id: _Optional[int] = ..., region: _Optional[str] = ..., team_id: _Optional[int] = ..., role_id: _Optional[int] = ..., placement_version: _Optional[int] = ..., placement_operation_id: _Optional[str] = ..., source_match_id: _Optional[int] = ..., session_jti: _Optional[str] = ...) -> None: ...

class AssignHubResponse(_message.Message):
    __slots__ = ("code", "hub_ds_addr", "hub_ticket", "hub_pod_name", "shard_id", "placement_version", "placement_operation_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    HUB_DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    HUB_TICKET_FIELD_NUMBER: _ClassVar[int]
    HUB_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    SHARD_ID_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    hub_ds_addr: str
    hub_ticket: str
    hub_pod_name: str
    shard_id: int
    placement_version: int
    placement_operation_id: str
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., hub_ds_addr: _Optional[str] = ..., hub_ticket: _Optional[str] = ..., hub_pod_name: _Optional[str] = ..., shard_id: _Optional[int] = ..., placement_version: _Optional[int] = ..., placement_operation_id: _Optional[str] = ...) -> None: ...

class ReleaseHubRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class ReleaseHubResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class EnsureHubDepartureForBattleRequest(_message.Message):
    __slots__ = ("player_id", "match_id", "placement_version", "placement_operation_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    match_id: int
    placement_version: int
    placement_operation_id: str
    def __init__(self, player_id: _Optional[int] = ..., match_id: _Optional[int] = ..., placement_version: _Optional[int] = ..., placement_operation_id: _Optional[str] = ...) -> None: ...

class EnsureHubDepartureForBattleResponse(_message.Message):
    __slots__ = ("code", "departed")
    CODE_FIELD_NUMBER: _ClassVar[int]
    DEPARTED_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    departed: bool
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., departed: bool = ...) -> None: ...

class TransferHubRequest(_message.Message):
    __slots__ = ("player_id", "target_hub_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_HUB_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    target_hub_id: int
    def __init__(self, player_id: _Optional[int] = ..., target_hub_id: _Optional[int] = ...) -> None: ...

class TransferHubResponse(_message.Message):
    __slots__ = ("code", "new_hub_ds_addr", "new_hub_ticket")
    CODE_FIELD_NUMBER: _ClassVar[int]
    NEW_HUB_DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    NEW_HUB_TICKET_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    new_hub_ds_addr: str
    new_hub_ticket: str
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., new_hub_ds_addr: _Optional[str] = ..., new_hub_ticket: _Optional[str] = ...) -> None: ...

class HubLine(_message.Message):
    __slots__ = ("line_no", "shard_id", "player_count", "capacity", "is_full", "is_current")
    LINE_NO_FIELD_NUMBER: _ClassVar[int]
    SHARD_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_COUNT_FIELD_NUMBER: _ClassVar[int]
    CAPACITY_FIELD_NUMBER: _ClassVar[int]
    IS_FULL_FIELD_NUMBER: _ClassVar[int]
    IS_CURRENT_FIELD_NUMBER: _ClassVar[int]
    line_no: int
    shard_id: int
    player_count: int
    capacity: int
    is_full: bool
    is_current: bool
    def __init__(self, line_no: _Optional[int] = ..., shard_id: _Optional[int] = ..., player_count: _Optional[int] = ..., capacity: _Optional[int] = ..., is_full: bool = ..., is_current: bool = ...) -> None: ...

class ListHubLinesRequest(_message.Message):
    __slots__ = ("region",)
    REGION_FIELD_NUMBER: _ClassVar[int]
    region: str
    def __init__(self, region: _Optional[str] = ...) -> None: ...

class ListHubLinesResponse(_message.Message):
    __slots__ = ("code", "lines")
    CODE_FIELD_NUMBER: _ClassVar[int]
    LINES_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    lines: _containers.RepeatedCompositeFieldContainer[HubLine]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., lines: _Optional[_Iterable[_Union[HubLine, _Mapping]]] = ...) -> None: ...

class TransferToLineRequest(_message.Message):
    __slots__ = ("target_shard_id",)
    TARGET_SHARD_ID_FIELD_NUMBER: _ClassVar[int]
    target_shard_id: int
    def __init__(self, target_shard_id: _Optional[int] = ...) -> None: ...

class TransferToLineResponse(_message.Message):
    __slots__ = ("code", "new_hub_ds_addr", "new_hub_ticket", "new_shard_id", "line_no")
    CODE_FIELD_NUMBER: _ClassVar[int]
    NEW_HUB_DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    NEW_HUB_TICKET_FIELD_NUMBER: _ClassVar[int]
    NEW_SHARD_ID_FIELD_NUMBER: _ClassVar[int]
    LINE_NO_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    new_hub_ds_addr: str
    new_hub_ticket: str
    new_shard_id: int
    line_no: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., new_hub_ds_addr: _Optional[str] = ..., new_hub_ticket: _Optional[str] = ..., new_shard_id: _Optional[int] = ..., line_no: _Optional[int] = ...) -> None: ...

class HubInfo(_message.Message):
    __slots__ = ("hub_pod_name", "hub_addr", "region", "player_count", "capacity", "state")
    HUB_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    HUB_ADDR_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    PLAYER_COUNT_FIELD_NUMBER: _ClassVar[int]
    CAPACITY_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    hub_pod_name: str
    hub_addr: str
    region: str
    player_count: int
    capacity: int
    state: str
    def __init__(self, hub_pod_name: _Optional[str] = ..., hub_addr: _Optional[str] = ..., region: _Optional[str] = ..., player_count: _Optional[int] = ..., capacity: _Optional[int] = ..., state: _Optional[str] = ...) -> None: ...

class ListHubsRequest(_message.Message):
    __slots__ = ("region",)
    REGION_FIELD_NUMBER: _ClassVar[int]
    region: str
    def __init__(self, region: _Optional[str] = ...) -> None: ...

class ListHubsResponse(_message.Message):
    __slots__ = ("code", "hubs")
    CODE_FIELD_NUMBER: _ClassVar[int]
    HUBS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    hubs: _containers.RepeatedCompositeFieldContainer[HubInfo]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., hubs: _Optional[_Iterable[_Union[HubInfo, _Mapping]]] = ...) -> None: ...

class HeartbeatRequest(_message.Message):
    __slots__ = ("hub_pod_name", "player_count", "cpu_pct", "mem_mb", "state", "ts_ms", "player_ids", "max_players")
    HUB_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    PLAYER_COUNT_FIELD_NUMBER: _ClassVar[int]
    CPU_PCT_FIELD_NUMBER: _ClassVar[int]
    MEM_MB_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    MAX_PLAYERS_FIELD_NUMBER: _ClassVar[int]
    hub_pod_name: str
    player_count: int
    cpu_pct: float
    mem_mb: float
    state: str
    ts_ms: int
    player_ids: _containers.RepeatedScalarFieldContainer[int]
    max_players: int
    def __init__(self, hub_pod_name: _Optional[str] = ..., player_count: _Optional[int] = ..., cpu_pct: _Optional[float] = ..., mem_mb: _Optional[float] = ..., state: _Optional[str] = ..., ts_ms: _Optional[int] = ..., player_ids: _Optional[_Iterable[int]] = ..., max_players: _Optional[int] = ...) -> None: ...

class HeartbeatResponse(_message.Message):
    __slots__ = ("code", "command", "grace_seconds", "accepted_token_gen", "accepted_token_jti", "accepted_instance_uid", "accepted_protocol_epoch", "accepted_writer_epoch", "eviction_orders")
    CODE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    GRACE_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_TOKEN_GEN_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_TOKEN_JTI_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_PROTOCOL_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_WRITER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    EVICTION_ORDERS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    command: str
    grace_seconds: int
    accepted_token_gen: int
    accepted_token_jti: str
    accepted_instance_uid: str
    accepted_protocol_epoch: int
    accepted_writer_epoch: int
    eviction_orders: _containers.RepeatedCompositeFieldContainer[HubEvictionOrder]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., command: _Optional[str] = ..., grace_seconds: _Optional[int] = ..., accepted_token_gen: _Optional[int] = ..., accepted_token_jti: _Optional[str] = ..., accepted_instance_uid: _Optional[str] = ..., accepted_protocol_epoch: _Optional[int] = ..., accepted_writer_epoch: _Optional[int] = ..., eviction_orders: _Optional[_Iterable[_Union[HubEvictionOrder, _Mapping]]] = ...) -> None: ...

class HubEvictionOrder(_message.Message):
    __slots__ = ("player_id", "assignment_id", "admission_id", "admission_seq", "source_instance_uid", "source_protocol_epoch", "source_writer_epoch", "cleanup_assignment_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ASSIGNMENT_ID_FIELD_NUMBER: _ClassVar[int]
    ADMISSION_ID_FIELD_NUMBER: _ClassVar[int]
    ADMISSION_SEQ_FIELD_NUMBER: _ClassVar[int]
    SOURCE_INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_PROTOCOL_EPOCH_FIELD_NUMBER: _ClassVar[int]
    SOURCE_WRITER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    CLEANUP_ASSIGNMENT_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    assignment_id: str
    admission_id: str
    admission_seq: int
    source_instance_uid: str
    source_protocol_epoch: int
    source_writer_epoch: int
    cleanup_assignment_id: str
    def __init__(self, player_id: _Optional[int] = ..., assignment_id: _Optional[str] = ..., admission_id: _Optional[str] = ..., admission_seq: _Optional[int] = ..., source_instance_uid: _Optional[str] = ..., source_protocol_epoch: _Optional[int] = ..., source_writer_epoch: _Optional[int] = ..., cleanup_assignment_id: _Optional[str] = ...) -> None: ...

class HubShardStorageRecord(_message.Message):
    __slots__ = ("hub_pod_name", "hub_addr", "region", "shard_id", "player_count", "capacity", "state", "last_heartbeat_ms", "created_at_ms", "draining_since_ms", "current_token_exp_ms", "current_token_gen", "last_verified_gen", "last_verified_jti", "gameserver_uid", "auth_epoch", "last_verified_writer_epoch", "release_track", "reported_connected_count", "reserved_count", "connected_ownership_count", "reported_max_players")
    HUB_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    HUB_ADDR_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    SHARD_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_COUNT_FIELD_NUMBER: _ClassVar[int]
    CAPACITY_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    LAST_HEARTBEAT_MS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    DRAINING_SINCE_MS_FIELD_NUMBER: _ClassVar[int]
    CURRENT_TOKEN_EXP_MS_FIELD_NUMBER: _ClassVar[int]
    CURRENT_TOKEN_GEN_FIELD_NUMBER: _ClassVar[int]
    LAST_VERIFIED_GEN_FIELD_NUMBER: _ClassVar[int]
    LAST_VERIFIED_JTI_FIELD_NUMBER: _ClassVar[int]
    GAMESERVER_UID_FIELD_NUMBER: _ClassVar[int]
    AUTH_EPOCH_FIELD_NUMBER: _ClassVar[int]
    LAST_VERIFIED_WRITER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    REPORTED_CONNECTED_COUNT_FIELD_NUMBER: _ClassVar[int]
    RESERVED_COUNT_FIELD_NUMBER: _ClassVar[int]
    CONNECTED_OWNERSHIP_COUNT_FIELD_NUMBER: _ClassVar[int]
    REPORTED_MAX_PLAYERS_FIELD_NUMBER: _ClassVar[int]
    hub_pod_name: str
    hub_addr: str
    region: str
    shard_id: int
    player_count: int
    capacity: int
    state: str
    last_heartbeat_ms: int
    created_at_ms: int
    draining_since_ms: int
    current_token_exp_ms: int
    current_token_gen: int
    last_verified_gen: int
    last_verified_jti: str
    gameserver_uid: str
    auth_epoch: int
    last_verified_writer_epoch: int
    release_track: str
    reported_connected_count: int
    reserved_count: int
    connected_ownership_count: int
    reported_max_players: int
    def __init__(self, hub_pod_name: _Optional[str] = ..., hub_addr: _Optional[str] = ..., region: _Optional[str] = ..., shard_id: _Optional[int] = ..., player_count: _Optional[int] = ..., capacity: _Optional[int] = ..., state: _Optional[str] = ..., last_heartbeat_ms: _Optional[int] = ..., created_at_ms: _Optional[int] = ..., draining_since_ms: _Optional[int] = ..., current_token_exp_ms: _Optional[int] = ..., current_token_gen: _Optional[int] = ..., last_verified_gen: _Optional[int] = ..., last_verified_jti: _Optional[str] = ..., gameserver_uid: _Optional[str] = ..., auth_epoch: _Optional[int] = ..., last_verified_writer_epoch: _Optional[int] = ..., release_track: _Optional[str] = ..., reported_connected_count: _Optional[int] = ..., reserved_count: _Optional[int] = ..., connected_ownership_count: _Optional[int] = ..., reported_max_players: _Optional[int] = ...) -> None: ...

class HubDSCredential(_message.Message):
    __slots__ = ("gen", "jti", "exp_ms", "kid", "instance_uid", "protocol_epoch", "token_sha256", "writer_epoch")
    GEN_FIELD_NUMBER: _ClassVar[int]
    JTI_FIELD_NUMBER: _ClassVar[int]
    EXP_MS_FIELD_NUMBER: _ClassVar[int]
    KID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_EPOCH_FIELD_NUMBER: _ClassVar[int]
    TOKEN_SHA256_FIELD_NUMBER: _ClassVar[int]
    WRITER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    gen: int
    jti: str
    exp_ms: int
    kid: str
    instance_uid: str
    protocol_epoch: int
    token_sha256: str
    writer_epoch: int
    def __init__(self, gen: _Optional[int] = ..., jti: _Optional[str] = ..., exp_ms: _Optional[int] = ..., kid: _Optional[str] = ..., instance_uid: _Optional[str] = ..., protocol_epoch: _Optional[int] = ..., token_sha256: _Optional[str] = ..., writer_epoch: _Optional[int] = ...) -> None: ...

class HubShardAuthStorageRecord(_message.Message):
    __slots__ = ("pod_name", "instance_uid", "protocol_epoch", "phase", "active", "pending", "high_water_gen", "pending_started_ms", "delivered_rv", "updated_at_ms", "last_active_heartbeat_ms", "required_writer_epoch")
    POD_NAME_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_EPOCH_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    PENDING_FIELD_NUMBER: _ClassVar[int]
    HIGH_WATER_GEN_FIELD_NUMBER: _ClassVar[int]
    PENDING_STARTED_MS_FIELD_NUMBER: _ClassVar[int]
    DELIVERED_RV_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    LAST_ACTIVE_HEARTBEAT_MS_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_WRITER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    pod_name: str
    instance_uid: str
    protocol_epoch: int
    phase: HubAuthPhase
    active: HubDSCredential
    pending: HubDSCredential
    high_water_gen: int
    pending_started_ms: int
    delivered_rv: str
    updated_at_ms: int
    last_active_heartbeat_ms: int
    required_writer_epoch: int
    def __init__(self, pod_name: _Optional[str] = ..., instance_uid: _Optional[str] = ..., protocol_epoch: _Optional[int] = ..., phase: _Optional[_Union[HubAuthPhase, str]] = ..., active: _Optional[_Union[HubDSCredential, _Mapping]] = ..., pending: _Optional[_Union[HubDSCredential, _Mapping]] = ..., high_water_gen: _Optional[int] = ..., pending_started_ms: _Optional[int] = ..., delivered_rv: _Optional[str] = ..., updated_at_ms: _Optional[int] = ..., last_active_heartbeat_ms: _Optional[int] = ..., required_writer_epoch: _Optional[int] = ...) -> None: ...

class HubAssignmentStorageRecord(_message.Message):
    __slots__ = ("player_id", "hub_pod_name", "hub_addr", "shard_id", "region", "team_id", "assigned_at_ms", "role_id", "hub_instance_uid", "auth_epoch", "auth_gen", "auth_jti", "assignment_id", "auth_writer_epoch", "release_track", "placement_version", "placement_operation_id", "source_match_id", "placement_proof_type", "transfer_cleanup_pending", "transfer_target_bound", "transfer_source_hub_pod_name", "transfer_source_assignment_id", "transfer_source_instance_uid", "transfer_source_auth_epoch", "transfer_source_auth_writer_epoch", "release_cleanup_pending", "release_cleanup_match_id", "release_cleanup_placement_version", "release_cleanup_operation_id", "writer_token", "source_revision")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    HUB_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    HUB_ADDR_FIELD_NUMBER: _ClassVar[int]
    SHARD_ID_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    ASSIGNED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    HUB_INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    AUTH_EPOCH_FIELD_NUMBER: _ClassVar[int]
    AUTH_GEN_FIELD_NUMBER: _ClassVar[int]
    AUTH_JTI_FIELD_NUMBER: _ClassVar[int]
    ASSIGNMENT_ID_FIELD_NUMBER: _ClassVar[int]
    AUTH_WRITER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_PROOF_TYPE_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_CLEANUP_PENDING_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_TARGET_BOUND_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_SOURCE_HUB_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_SOURCE_ASSIGNMENT_ID_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_SOURCE_INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_SOURCE_AUTH_EPOCH_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_SOURCE_AUTH_WRITER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    RELEASE_CLEANUP_PENDING_FIELD_NUMBER: _ClassVar[int]
    RELEASE_CLEANUP_MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_CLEANUP_PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    RELEASE_CLEANUP_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    WRITER_TOKEN_FIELD_NUMBER: _ClassVar[int]
    SOURCE_REVISION_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    hub_pod_name: str
    hub_addr: str
    shard_id: int
    region: str
    team_id: int
    assigned_at_ms: int
    role_id: int
    hub_instance_uid: str
    auth_epoch: int
    auth_gen: int
    auth_jti: str
    assignment_id: str
    auth_writer_epoch: int
    release_track: str
    placement_version: int
    placement_operation_id: str
    source_match_id: int
    placement_proof_type: int
    transfer_cleanup_pending: bool
    transfer_target_bound: bool
    transfer_source_hub_pod_name: str
    transfer_source_assignment_id: str
    transfer_source_instance_uid: str
    transfer_source_auth_epoch: int
    transfer_source_auth_writer_epoch: int
    release_cleanup_pending: bool
    release_cleanup_match_id: int
    release_cleanup_placement_version: int
    release_cleanup_operation_id: str
    writer_token: int
    source_revision: int
    def __init__(self, player_id: _Optional[int] = ..., hub_pod_name: _Optional[str] = ..., hub_addr: _Optional[str] = ..., shard_id: _Optional[int] = ..., region: _Optional[str] = ..., team_id: _Optional[int] = ..., assigned_at_ms: _Optional[int] = ..., role_id: _Optional[int] = ..., hub_instance_uid: _Optional[str] = ..., auth_epoch: _Optional[int] = ..., auth_gen: _Optional[int] = ..., auth_jti: _Optional[str] = ..., assignment_id: _Optional[str] = ..., auth_writer_epoch: _Optional[int] = ..., release_track: _Optional[str] = ..., placement_version: _Optional[int] = ..., placement_operation_id: _Optional[str] = ..., source_match_id: _Optional[int] = ..., placement_proof_type: _Optional[int] = ..., transfer_cleanup_pending: bool = ..., transfer_target_bound: bool = ..., transfer_source_hub_pod_name: _Optional[str] = ..., transfer_source_assignment_id: _Optional[str] = ..., transfer_source_instance_uid: _Optional[str] = ..., transfer_source_auth_epoch: _Optional[int] = ..., transfer_source_auth_writer_epoch: _Optional[int] = ..., release_cleanup_pending: bool = ..., release_cleanup_match_id: _Optional[int] = ..., release_cleanup_placement_version: _Optional[int] = ..., release_cleanup_operation_id: _Optional[str] = ..., writer_token: _Optional[int] = ..., source_revision: _Optional[int] = ...) -> None: ...

class HubMigrateEvent(_message.Message):
    __slots__ = ("player_id", "from_hub_pod", "to_hub_ds_addr", "to_hub_ticket", "to_hub_pod_name", "to_shard_id", "grace_seconds", "reason", "ts_ms")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    FROM_HUB_POD_FIELD_NUMBER: _ClassVar[int]
    TO_HUB_DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    TO_HUB_TICKET_FIELD_NUMBER: _ClassVar[int]
    TO_HUB_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    TO_SHARD_ID_FIELD_NUMBER: _ClassVar[int]
    GRACE_SECONDS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    from_hub_pod: str
    to_hub_ds_addr: str
    to_hub_ticket: str
    to_hub_pod_name: str
    to_shard_id: int
    grace_seconds: int
    reason: str
    ts_ms: int
    def __init__(self, player_id: _Optional[int] = ..., from_hub_pod: _Optional[str] = ..., to_hub_ds_addr: _Optional[str] = ..., to_hub_ticket: _Optional[str] = ..., to_hub_pod_name: _Optional[str] = ..., to_shard_id: _Optional[int] = ..., grace_seconds: _Optional[int] = ..., reason: _Optional[str] = ..., ts_ms: _Optional[int] = ...) -> None: ...

class AcknowledgeAdmissionRequest(_message.Message):
    __slots__ = ("player_id", "assignment_id", "hub_pod_name", "admission_id", "admission_seq", "placement_version", "placement_operation_id", "source_match_id", "session_jti")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ASSIGNMENT_ID_FIELD_NUMBER: _ClassVar[int]
    HUB_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    ADMISSION_ID_FIELD_NUMBER: _ClassVar[int]
    ADMISSION_SEQ_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_JTI_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    assignment_id: str
    hub_pod_name: str
    admission_id: str
    admission_seq: int
    placement_version: int
    placement_operation_id: str
    source_match_id: int
    session_jti: str
    def __init__(self, player_id: _Optional[int] = ..., assignment_id: _Optional[str] = ..., hub_pod_name: _Optional[str] = ..., admission_id: _Optional[str] = ..., admission_seq: _Optional[int] = ..., placement_version: _Optional[int] = ..., placement_operation_id: _Optional[str] = ..., source_match_id: _Optional[int] = ..., session_jti: _Optional[str] = ...) -> None: ...

class AcknowledgeAdmissionResponse(_message.Message):
    __slots__ = ("code", "admitted", "placement_committed", "placement_version", "placement_operation_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ADMITTED_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_COMMITTED_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    admitted: bool
    placement_committed: bool
    placement_version: int
    placement_operation_id: str
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., admitted: bool = ..., placement_committed: bool = ..., placement_version: _Optional[int] = ..., placement_operation_id: _Optional[str] = ...) -> None: ...

class AcknowledgeDepartureRequest(_message.Message):
    __slots__ = ("player_id", "assignment_id", "hub_pod_name", "admission_id", "admission_seq")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ASSIGNMENT_ID_FIELD_NUMBER: _ClassVar[int]
    HUB_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    ADMISSION_ID_FIELD_NUMBER: _ClassVar[int]
    ADMISSION_SEQ_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    assignment_id: str
    hub_pod_name: str
    admission_id: str
    admission_seq: int
    def __init__(self, player_id: _Optional[int] = ..., assignment_id: _Optional[str] = ..., hub_pod_name: _Optional[str] = ..., admission_id: _Optional[str] = ..., admission_seq: _Optional[int] = ...) -> None: ...

class AcknowledgeDepartureResponse(_message.Message):
    __slots__ = ("code", "departed")
    CODE_FIELD_NUMBER: _ClassVar[int]
    DEPARTED_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    departed: bool
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., departed: bool = ...) -> None: ...

class HubReservationStorageRecord(_message.Message):
    __slots__ = ("player_id", "assignment_id", "hub_pod_name", "hub_instance_uid", "auth_epoch", "auth_writer_epoch", "created_at_ms", "expires_at_ms", "assignment_expires_at_ms")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ASSIGNMENT_ID_FIELD_NUMBER: _ClassVar[int]
    HUB_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    HUB_INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    AUTH_EPOCH_FIELD_NUMBER: _ClassVar[int]
    AUTH_WRITER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_MS_FIELD_NUMBER: _ClassVar[int]
    ASSIGNMENT_EXPIRES_AT_MS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    assignment_id: str
    hub_pod_name: str
    hub_instance_uid: str
    auth_epoch: int
    auth_writer_epoch: int
    created_at_ms: int
    expires_at_ms: int
    assignment_expires_at_ms: int
    def __init__(self, player_id: _Optional[int] = ..., assignment_id: _Optional[str] = ..., hub_pod_name: _Optional[str] = ..., hub_instance_uid: _Optional[str] = ..., auth_epoch: _Optional[int] = ..., auth_writer_epoch: _Optional[int] = ..., created_at_ms: _Optional[int] = ..., expires_at_ms: _Optional[int] = ..., assignment_expires_at_ms: _Optional[int] = ...) -> None: ...

class HubConnectedOwnershipStorageRecord(_message.Message):
    __slots__ = ("player_id", "assignment_id", "admission_id", "hub_pod_name", "hub_instance_uid", "auth_epoch", "auth_writer_epoch", "admitted_at_ms", "last_seen_ms", "expires_at_ms", "admission_seq")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ASSIGNMENT_ID_FIELD_NUMBER: _ClassVar[int]
    ADMISSION_ID_FIELD_NUMBER: _ClassVar[int]
    HUB_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    HUB_INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    AUTH_EPOCH_FIELD_NUMBER: _ClassVar[int]
    AUTH_WRITER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ADMITTED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    LAST_SEEN_MS_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_MS_FIELD_NUMBER: _ClassVar[int]
    ADMISSION_SEQ_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    assignment_id: str
    admission_id: str
    hub_pod_name: str
    hub_instance_uid: str
    auth_epoch: int
    auth_writer_epoch: int
    admitted_at_ms: int
    last_seen_ms: int
    expires_at_ms: int
    admission_seq: int
    def __init__(self, player_id: _Optional[int] = ..., assignment_id: _Optional[str] = ..., admission_id: _Optional[str] = ..., hub_pod_name: _Optional[str] = ..., hub_instance_uid: _Optional[str] = ..., auth_epoch: _Optional[int] = ..., auth_writer_epoch: _Optional[int] = ..., admitted_at_ms: _Optional[int] = ..., last_seen_ms: _Optional[int] = ..., expires_at_ms: _Optional[int] = ..., admission_seq: _Optional[int] = ...) -> None: ...
