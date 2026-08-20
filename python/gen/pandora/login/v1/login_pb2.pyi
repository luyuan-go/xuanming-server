from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.api import annotations_pb2 as _annotations_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ResumeRoute(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RESUME_ROUTE_UNSPECIFIED: _ClassVar[ResumeRoute]
    RESUME_ROUTE_HUB: _ClassVar[ResumeRoute]
    RESUME_ROUTE_BATTLE: _ClassVar[ResumeRoute]
    RESUME_ROUTE_UNKNOWN: _ClassVar[ResumeRoute]

class ResumeMatchStage(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RESUME_MATCH_STAGE_UNSPECIFIED: _ClassVar[ResumeMatchStage]
    RESUME_MATCH_STAGE_NONE: _ClassVar[ResumeMatchStage]
    RESUME_MATCH_STAGE_QUEUED: _ClassVar[ResumeMatchStage]
    RESUME_MATCH_STAGE_CONFIRMING: _ClassVar[ResumeMatchStage]
    RESUME_MATCH_STAGE_ALLOCATING: _ClassVar[ResumeMatchStage]
    RESUME_MATCH_STAGE_READY: _ClassVar[ResumeMatchStage]
    RESUME_MATCH_STAGE_RUNNING: _ClassVar[ResumeMatchStage]

class ResumePlacementState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RESUME_PLACEMENT_STATE_UNSPECIFIED: _ClassVar[ResumePlacementState]
    RESUME_PLACEMENT_STATE_PENDING: _ClassVar[ResumePlacementState]
    RESUME_PLACEMENT_STATE_STABLE: _ClassVar[ResumePlacementState]

class ResumeEntryState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RESUME_ENTRY_STATE_UNSPECIFIED: _ClassVar[ResumeEntryState]
    RESUME_ENTRY_STATE_ROLE_REQUIRED: _ClassVar[ResumeEntryState]
    RESUME_ENTRY_STATE_WAIT: _ClassVar[ResumeEntryState]
    RESUME_ENTRY_STATE_TARGET: _ClassVar[ResumeEntryState]
    RESUME_ENTRY_STATE_REAUTH: _ClassVar[ResumeEntryState]
    RESUME_ENTRY_STATE_TERMINAL: _ClassVar[ResumeEntryState]

class ResumeWaitReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RESUME_WAIT_REASON_UNSPECIFIED: _ClassVar[ResumeWaitReason]
    RESUME_WAIT_REASON_OWNER_UNKNOWN: _ClassVar[ResumeWaitReason]
    RESUME_WAIT_REASON_ADMIT_BARRIER: _ClassVar[ResumeWaitReason]
    RESUME_WAIT_REASON_MATCHING: _ClassVar[ResumeWaitReason]
    RESUME_WAIT_REASON_ROLE_UNKNOWN: _ClassVar[ResumeWaitReason]
    RESUME_WAIT_REASON_NO_CAPACITY: _ClassVar[ResumeWaitReason]
RESUME_ROUTE_UNSPECIFIED: ResumeRoute
RESUME_ROUTE_HUB: ResumeRoute
RESUME_ROUTE_BATTLE: ResumeRoute
RESUME_ROUTE_UNKNOWN: ResumeRoute
RESUME_MATCH_STAGE_UNSPECIFIED: ResumeMatchStage
RESUME_MATCH_STAGE_NONE: ResumeMatchStage
RESUME_MATCH_STAGE_QUEUED: ResumeMatchStage
RESUME_MATCH_STAGE_CONFIRMING: ResumeMatchStage
RESUME_MATCH_STAGE_ALLOCATING: ResumeMatchStage
RESUME_MATCH_STAGE_READY: ResumeMatchStage
RESUME_MATCH_STAGE_RUNNING: ResumeMatchStage
RESUME_PLACEMENT_STATE_UNSPECIFIED: ResumePlacementState
RESUME_PLACEMENT_STATE_PENDING: ResumePlacementState
RESUME_PLACEMENT_STATE_STABLE: ResumePlacementState
RESUME_ENTRY_STATE_UNSPECIFIED: ResumeEntryState
RESUME_ENTRY_STATE_ROLE_REQUIRED: ResumeEntryState
RESUME_ENTRY_STATE_WAIT: ResumeEntryState
RESUME_ENTRY_STATE_TARGET: ResumeEntryState
RESUME_ENTRY_STATE_REAUTH: ResumeEntryState
RESUME_ENTRY_STATE_TERMINAL: ResumeEntryState
RESUME_WAIT_REASON_UNSPECIFIED: ResumeWaitReason
RESUME_WAIT_REASON_OWNER_UNKNOWN: ResumeWaitReason
RESUME_WAIT_REASON_ADMIT_BARRIER: ResumeWaitReason
RESUME_WAIT_REASON_MATCHING: ResumeWaitReason
RESUME_WAIT_REASON_ROLE_UNKNOWN: ResumeWaitReason
RESUME_WAIT_REASON_NO_CAPACITY: ResumeWaitReason

class LoginRequest(_message.Message):
    __slots__ = ("account", "password_hash", "device_id", "client_version", "region", "locale", "defer_role_entry")
    ACCOUNT_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_HASH_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    CLIENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    LOCALE_FIELD_NUMBER: _ClassVar[int]
    DEFER_ROLE_ENTRY_FIELD_NUMBER: _ClassVar[int]
    account: str
    password_hash: str
    device_id: str
    client_version: str
    region: str
    locale: str
    defer_role_entry: bool
    def __init__(self, account: _Optional[str] = ..., password_hash: _Optional[str] = ..., device_id: _Optional[str] = ..., client_version: _Optional[str] = ..., region: _Optional[str] = ..., locale: _Optional[str] = ..., defer_role_entry: bool = ...) -> None: ...

class LoginResponse(_message.Message):
    __slots__ = ("code", "player_id", "session_token", "hub_ds_addr", "hub_ticket", "region_id", "cell_id", "battle_ds_addr", "battle_ticket", "match_id", "selected_role_id", "resume_context", "register_no", "player_no", "account_token", "account_id", "roles", "account_token_exp_ms")
    CODE_FIELD_NUMBER: _ClassVar[int]
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    HUB_DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    HUB_TICKET_FIELD_NUMBER: _ClassVar[int]
    REGION_ID_FIELD_NUMBER: _ClassVar[int]
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    BATTLE_DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    BATTLE_TICKET_FIELD_NUMBER: _ClassVar[int]
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    SELECTED_ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    RESUME_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    REGISTER_NO_FIELD_NUMBER: _ClassVar[int]
    PLAYER_NO_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_TOKEN_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_TOKEN_EXP_MS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    player_id: int
    session_token: str
    hub_ds_addr: str
    hub_ticket: str
    region_id: int
    cell_id: int
    battle_ds_addr: str
    battle_ticket: str
    match_id: int
    selected_role_id: int
    resume_context: ResumeContext
    register_no: int
    player_no: int
    account_token: str
    account_id: int
    roles: _containers.RepeatedCompositeFieldContainer[AccountRole]
    account_token_exp_ms: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., player_id: _Optional[int] = ..., session_token: _Optional[str] = ..., hub_ds_addr: _Optional[str] = ..., hub_ticket: _Optional[str] = ..., region_id: _Optional[int] = ..., cell_id: _Optional[int] = ..., battle_ds_addr: _Optional[str] = ..., battle_ticket: _Optional[str] = ..., match_id: _Optional[int] = ..., selected_role_id: _Optional[int] = ..., resume_context: _Optional[_Union[ResumeContext, _Mapping]] = ..., register_no: _Optional[int] = ..., player_no: _Optional[int] = ..., account_token: _Optional[str] = ..., account_id: _Optional[int] = ..., roles: _Optional[_Iterable[_Union[AccountRole, _Mapping]]] = ..., account_token_exp_ms: _Optional[int] = ...) -> None: ...

class AccountRole(_message.Message):
    __slots__ = ("player_id", "role_name", "role_id", "player_no", "level", "last_login_at_ms", "slot")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_NO_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    LAST_LOGIN_AT_MS_FIELD_NUMBER: _ClassVar[int]
    SLOT_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    role_name: str
    role_id: int
    player_no: int
    level: int
    last_login_at_ms: int
    slot: int
    def __init__(self, player_id: _Optional[int] = ..., role_name: _Optional[str] = ..., role_id: _Optional[int] = ..., player_no: _Optional[int] = ..., level: _Optional[int] = ..., last_login_at_ms: _Optional[int] = ..., slot: _Optional[int] = ...) -> None: ...

class ListAccountRolesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListAccountRolesResponse(_message.Message):
    __slots__ = ("code", "roles")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    roles: _containers.RepeatedCompositeFieldContainer[AccountRole]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., roles: _Optional[_Iterable[_Union[AccountRole, _Mapping]]] = ...) -> None: ...

class EnterRoleRequest(_message.Message):
    __slots__ = ("player_id", "device_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    device_id: str
    def __init__(self, player_id: _Optional[int] = ..., device_id: _Optional[str] = ...) -> None: ...

class EnterRoleResponse(_message.Message):
    __slots__ = ("code", "player_id", "session_token", "hub_ds_addr", "hub_ticket", "region_id", "cell_id", "battle_ds_addr", "battle_ticket", "match_id", "selected_role_id", "resume_context", "player_no")
    CODE_FIELD_NUMBER: _ClassVar[int]
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    HUB_DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    HUB_TICKET_FIELD_NUMBER: _ClassVar[int]
    REGION_ID_FIELD_NUMBER: _ClassVar[int]
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    BATTLE_DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    BATTLE_TICKET_FIELD_NUMBER: _ClassVar[int]
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    SELECTED_ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    RESUME_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PLAYER_NO_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    player_id: int
    session_token: str
    hub_ds_addr: str
    hub_ticket: str
    region_id: int
    cell_id: int
    battle_ds_addr: str
    battle_ticket: str
    match_id: int
    selected_role_id: int
    resume_context: ResumeContext
    player_no: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., player_id: _Optional[int] = ..., session_token: _Optional[str] = ..., hub_ds_addr: _Optional[str] = ..., hub_ticket: _Optional[str] = ..., region_id: _Optional[int] = ..., cell_id: _Optional[int] = ..., battle_ds_addr: _Optional[str] = ..., battle_ticket: _Optional[str] = ..., match_id: _Optional[int] = ..., selected_role_id: _Optional[int] = ..., resume_context: _Optional[_Union[ResumeContext, _Mapping]] = ..., player_no: _Optional[int] = ...) -> None: ...

class ResumeContext(_message.Message):
    __slots__ = ("route", "match_id", "match_stage", "placement_version", "operation_id", "placement_state", "ds_pod_name", "ds_instance_uid", "hub_assignment_id", "ds_instance_epoch", "allocation_id", "release_track", "game_mode", "map_id", "owner_epoch", "retry_after_ms", "entry_state", "wait_reason")
    ROUTE_FIELD_NUMBER: _ClassVar[int]
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    MATCH_STAGE_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_STATE_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    DS_INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    HUB_ASSIGNMENT_ID_FIELD_NUMBER: _ClassVar[int]
    DS_INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    GAME_MODE_FIELD_NUMBER: _ClassVar[int]
    MAP_ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    RETRY_AFTER_MS_FIELD_NUMBER: _ClassVar[int]
    ENTRY_STATE_FIELD_NUMBER: _ClassVar[int]
    WAIT_REASON_FIELD_NUMBER: _ClassVar[int]
    route: ResumeRoute
    match_id: int
    match_stage: ResumeMatchStage
    placement_version: int
    operation_id: str
    placement_state: ResumePlacementState
    ds_pod_name: str
    ds_instance_uid: str
    hub_assignment_id: str
    ds_instance_epoch: int
    allocation_id: str
    release_track: str
    game_mode: str
    map_id: int
    owner_epoch: int
    retry_after_ms: int
    entry_state: ResumeEntryState
    wait_reason: ResumeWaitReason
    def __init__(self, route: _Optional[_Union[ResumeRoute, str]] = ..., match_id: _Optional[int] = ..., match_stage: _Optional[_Union[ResumeMatchStage, str]] = ..., placement_version: _Optional[int] = ..., operation_id: _Optional[str] = ..., placement_state: _Optional[_Union[ResumePlacementState, str]] = ..., ds_pod_name: _Optional[str] = ..., ds_instance_uid: _Optional[str] = ..., hub_assignment_id: _Optional[str] = ..., ds_instance_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., release_track: _Optional[str] = ..., game_mode: _Optional[str] = ..., map_id: _Optional[int] = ..., owner_epoch: _Optional[int] = ..., retry_after_ms: _Optional[int] = ..., entry_state: _Optional[_Union[ResumeEntryState, str]] = ..., wait_reason: _Optional[_Union[ResumeWaitReason, str]] = ...) -> None: ...

class GetResumeContextRequest(_message.Message):
    __slots__ = ("session_token",)
    SESSION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    session_token: str
    def __init__(self, session_token: _Optional[str] = ...) -> None: ...

class GetResumeContextResponse(_message.Message):
    __slots__ = ("code", "context")
    CODE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    context: ResumeContext
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., context: _Optional[_Union[ResumeContext, _Mapping]] = ...) -> None: ...

class LogoutRequest(_message.Message):
    __slots__ = ("session_token",)
    SESSION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    session_token: str
    def __init__(self, session_token: _Optional[str] = ...) -> None: ...

class LogoutResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class DSTicket(_message.Message):
    __slots__ = ("player_id", "match_id", "issued_at_ms", "expires_at_ms", "ds_type", "jti", "region_id", "cell_id", "role_id", "ds_pod_name", "ds_instance_uid", "ds_protocol_epoch", "ds_credential_gen", "ds_credential_jti", "hub_assignment_id", "ds_writer_epoch", "dst_ver", "ds_instance_epoch", "allocation_id", "release_track", "placement_version", "placement_operation_id", "source_match_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    ISSUED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_MS_FIELD_NUMBER: _ClassVar[int]
    DS_TYPE_FIELD_NUMBER: _ClassVar[int]
    JTI_FIELD_NUMBER: _ClassVar[int]
    REGION_ID_FIELD_NUMBER: _ClassVar[int]
    CELL_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    DS_INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    DS_PROTOCOL_EPOCH_FIELD_NUMBER: _ClassVar[int]
    DS_CREDENTIAL_GEN_FIELD_NUMBER: _ClassVar[int]
    DS_CREDENTIAL_JTI_FIELD_NUMBER: _ClassVar[int]
    HUB_ASSIGNMENT_ID_FIELD_NUMBER: _ClassVar[int]
    DS_WRITER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    DST_VER_FIELD_NUMBER: _ClassVar[int]
    DS_INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    match_id: int
    issued_at_ms: int
    expires_at_ms: int
    ds_type: str
    jti: str
    region_id: int
    cell_id: int
    role_id: int
    ds_pod_name: str
    ds_instance_uid: str
    ds_protocol_epoch: int
    ds_credential_gen: int
    ds_credential_jti: str
    hub_assignment_id: str
    ds_writer_epoch: int
    dst_ver: int
    ds_instance_epoch: int
    allocation_id: str
    release_track: str
    placement_version: int
    placement_operation_id: str
    source_match_id: int
    def __init__(self, player_id: _Optional[int] = ..., match_id: _Optional[int] = ..., issued_at_ms: _Optional[int] = ..., expires_at_ms: _Optional[int] = ..., ds_type: _Optional[str] = ..., jti: _Optional[str] = ..., region_id: _Optional[int] = ..., cell_id: _Optional[int] = ..., role_id: _Optional[int] = ..., ds_pod_name: _Optional[str] = ..., ds_instance_uid: _Optional[str] = ..., ds_protocol_epoch: _Optional[int] = ..., ds_credential_gen: _Optional[int] = ..., ds_credential_jti: _Optional[str] = ..., hub_assignment_id: _Optional[str] = ..., ds_writer_epoch: _Optional[int] = ..., dst_ver: _Optional[int] = ..., ds_instance_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., release_track: _Optional[str] = ..., placement_version: _Optional[int] = ..., placement_operation_id: _Optional[str] = ..., source_match_id: _Optional[int] = ...) -> None: ...

class IssueDSTicketRequest(_message.Message):
    __slots__ = ("session_token", "ds_type", "target_id")
    SESSION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    DS_TYPE_FIELD_NUMBER: _ClassVar[int]
    TARGET_ID_FIELD_NUMBER: _ClassVar[int]
    session_token: str
    ds_type: str
    target_id: int
    def __init__(self, session_token: _Optional[str] = ..., ds_type: _Optional[str] = ..., target_id: _Optional[int] = ...) -> None: ...

class IssueDSTicketResponse(_message.Message):
    __slots__ = ("code", "ticket", "hub_ds_addr")
    CODE_FIELD_NUMBER: _ClassVar[int]
    TICKET_FIELD_NUMBER: _ClassVar[int]
    HUB_DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    ticket: str
    hub_ds_addr: str
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., ticket: _Optional[str] = ..., hub_ds_addr: _Optional[str] = ...) -> None: ...

class VerifyDSTicketRequest(_message.Message):
    __slots__ = ("ticket", "ds_pod_name", "admission_id")
    TICKET_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    ADMISSION_ID_FIELD_NUMBER: _ClassVar[int]
    ticket: str
    ds_pod_name: str
    admission_id: str
    def __init__(self, ticket: _Optional[str] = ..., ds_pod_name: _Optional[str] = ..., admission_id: _Optional[str] = ...) -> None: ...

class VerifyDSTicketResponse(_message.Message):
    __slots__ = ("code", "claims")
    CODE_FIELD_NUMBER: _ClassVar[int]
    CLAIMS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    claims: DSTicket
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., claims: _Optional[_Union[DSTicket, _Mapping]] = ...) -> None: ...

class GetPlayerNoRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetPlayerNoResponse(_message.Message):
    __slots__ = ("code", "player_no")
    CODE_FIELD_NUMBER: _ClassVar[int]
    PLAYER_NO_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    player_no: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., player_no: _Optional[int] = ...) -> None: ...

class ResolvePlayerNosRequest(_message.Message):
    __slots__ = ("player_ids",)
    PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    player_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, player_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class ResolvedPlayerNo(_message.Message):
    __slots__ = ("player_id", "player_no")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_NO_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    player_no: int
    def __init__(self, player_id: _Optional[int] = ..., player_no: _Optional[int] = ...) -> None: ...

class ResolvePlayerNosResponse(_message.Message):
    __slots__ = ("code", "entries")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    entries: _containers.RepeatedCompositeFieldContainer[ResolvedPlayerNo]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., entries: _Optional[_Iterable[_Union[ResolvedPlayerNo, _Mapping]]] = ...) -> None: ...

class GetRegisterNoRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetRegisterNoResponse(_message.Message):
    __slots__ = ("code", "register_no")
    CODE_FIELD_NUMBER: _ClassVar[int]
    REGISTER_NO_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    register_no: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., register_no: _Optional[int] = ...) -> None: ...

class SelectRoleRequest(_message.Message):
    __slots__ = ("role_id",)
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    role_id: int
    def __init__(self, role_id: _Optional[int] = ...) -> None: ...

class SelectRoleResponse(_message.Message):
    __slots__ = ("code", "hub_ds_addr", "hub_ticket")
    CODE_FIELD_NUMBER: _ClassVar[int]
    HUB_DS_ADDR_FIELD_NUMBER: _ClassVar[int]
    HUB_TICKET_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    hub_ds_addr: str
    hub_ticket: str
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., hub_ds_addr: _Optional[str] = ..., hub_ticket: _Optional[str] = ...) -> None: ...
