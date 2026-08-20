from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TeamState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TEAM_STATE_UNSPECIFIED: _ClassVar[TeamState]
    TEAM_STATE_FORMING: _ClassVar[TeamState]
    TEAM_STATE_READY: _ClassVar[TeamState]
    TEAM_STATE_MATCHING: _ClassVar[TeamState]
    TEAM_STATE_IN_BATTLE: _ClassVar[TeamState]
    TEAM_STATE_DISBANDED: _ClassVar[TeamState]

class TeamJoinPolicy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TEAM_JOIN_POLICY_UNSPECIFIED: _ClassVar[TeamJoinPolicy]
    TEAM_JOIN_POLICY_APPROVAL: _ClassVar[TeamJoinPolicy]
    TEAM_JOIN_POLICY_OPEN: _ClassVar[TeamJoinPolicy]

class TeamPushEventType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TEAM_PUSH_EVENT_TYPE_UNSPECIFIED: _ClassVar[TeamPushEventType]
    TEAM_PUSH_EVENT_TYPE_INVITE: _ClassVar[TeamPushEventType]

class TeamUpdateReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TEAM_UPDATE_REASON_UNSPECIFIED: _ClassVar[TeamUpdateReason]
    TEAM_UPDATE_REASON_MEMBER_JOINED: _ClassVar[TeamUpdateReason]
    TEAM_UPDATE_REASON_MEMBER_LEFT: _ClassVar[TeamUpdateReason]
    TEAM_UPDATE_REASON_MEMBER_KICKED: _ClassVar[TeamUpdateReason]
    TEAM_UPDATE_REASON_MEMBER_READY: _ClassVar[TeamUpdateReason]
    TEAM_UPDATE_REASON_HERO_CHANGED: _ClassVar[TeamUpdateReason]
    TEAM_UPDATE_REASON_CAPTAIN_TRANSFER: _ClassVar[TeamUpdateReason]
    TEAM_UPDATE_REASON_STATE_CHANGED: _ClassVar[TeamUpdateReason]
    TEAM_UPDATE_REASON_DISBANDED: _ClassVar[TeamUpdateReason]
    TEAM_UPDATE_REASON_INVITE_SENT: _ClassVar[TeamUpdateReason]
    TEAM_UPDATE_REASON_APPLICATION_RECEIVED: _ClassVar[TeamUpdateReason]
    TEAM_UPDATE_REASON_MAP_CHANGED: _ClassVar[TeamUpdateReason]
    TEAM_UPDATE_REASON_MEMBER_OFFLINE_LEFT: _ClassVar[TeamUpdateReason]
TEAM_STATE_UNSPECIFIED: TeamState
TEAM_STATE_FORMING: TeamState
TEAM_STATE_READY: TeamState
TEAM_STATE_MATCHING: TeamState
TEAM_STATE_IN_BATTLE: TeamState
TEAM_STATE_DISBANDED: TeamState
TEAM_JOIN_POLICY_UNSPECIFIED: TeamJoinPolicy
TEAM_JOIN_POLICY_APPROVAL: TeamJoinPolicy
TEAM_JOIN_POLICY_OPEN: TeamJoinPolicy
TEAM_PUSH_EVENT_TYPE_UNSPECIFIED: TeamPushEventType
TEAM_PUSH_EVENT_TYPE_INVITE: TeamPushEventType
TEAM_UPDATE_REASON_UNSPECIFIED: TeamUpdateReason
TEAM_UPDATE_REASON_MEMBER_JOINED: TeamUpdateReason
TEAM_UPDATE_REASON_MEMBER_LEFT: TeamUpdateReason
TEAM_UPDATE_REASON_MEMBER_KICKED: TeamUpdateReason
TEAM_UPDATE_REASON_MEMBER_READY: TeamUpdateReason
TEAM_UPDATE_REASON_HERO_CHANGED: TeamUpdateReason
TEAM_UPDATE_REASON_CAPTAIN_TRANSFER: TeamUpdateReason
TEAM_UPDATE_REASON_STATE_CHANGED: TeamUpdateReason
TEAM_UPDATE_REASON_DISBANDED: TeamUpdateReason
TEAM_UPDATE_REASON_INVITE_SENT: TeamUpdateReason
TEAM_UPDATE_REASON_APPLICATION_RECEIVED: TeamUpdateReason
TEAM_UPDATE_REASON_MAP_CHANGED: TeamUpdateReason
TEAM_UPDATE_REASON_MEMBER_OFFLINE_LEFT: TeamUpdateReason

class TeamMember(_message.Message):
    __slots__ = ("player_id", "nickname", "mmr", "ready", "hero_id", "player_no")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    NICKNAME_FIELD_NUMBER: _ClassVar[int]
    MMR_FIELD_NUMBER: _ClassVar[int]
    READY_FIELD_NUMBER: _ClassVar[int]
    HERO_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_NO_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    nickname: str
    mmr: int
    ready: bool
    hero_id: int
    player_no: int
    def __init__(self, player_id: _Optional[int] = ..., nickname: _Optional[str] = ..., mmr: _Optional[int] = ..., ready: bool = ..., hero_id: _Optional[int] = ..., player_no: _Optional[int] = ...) -> None: ...

class Team(_message.Message):
    __slots__ = ("team_id", "captain_id", "members", "state", "created_at_ms", "max_size", "map_id", "join_policy")
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    CAPTAIN_ID_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    MAX_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAP_ID_FIELD_NUMBER: _ClassVar[int]
    JOIN_POLICY_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    captain_id: int
    members: _containers.RepeatedCompositeFieldContainer[TeamMember]
    state: TeamState
    created_at_ms: int
    max_size: int
    map_id: int
    join_policy: TeamJoinPolicy
    def __init__(self, team_id: _Optional[int] = ..., captain_id: _Optional[int] = ..., members: _Optional[_Iterable[_Union[TeamMember, _Mapping]]] = ..., state: _Optional[_Union[TeamState, str]] = ..., created_at_ms: _Optional[int] = ..., max_size: _Optional[int] = ..., map_id: _Optional[int] = ..., join_policy: _Optional[_Union[TeamJoinPolicy, str]] = ...) -> None: ...

class TeamMemberStorageRecord(_message.Message):
    __slots__ = ("player_id", "nickname", "mmr", "ready", "hero_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    NICKNAME_FIELD_NUMBER: _ClassVar[int]
    MMR_FIELD_NUMBER: _ClassVar[int]
    READY_FIELD_NUMBER: _ClassVar[int]
    HERO_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    nickname: str
    mmr: int
    ready: bool
    hero_id: int
    def __init__(self, player_id: _Optional[int] = ..., nickname: _Optional[str] = ..., mmr: _Optional[int] = ..., ready: bool = ..., hero_id: _Optional[int] = ...) -> None: ...

class TeamStorageRecord(_message.Message):
    __slots__ = ("team_id", "captain_id", "state", "members", "created_at_ms", "updated_at_ms", "max_size", "map_id", "match_lock_until_ms", "match_lock_operation_id", "ready_generation", "match_start_receipt")
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    CAPTAIN_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    MAX_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAP_ID_FIELD_NUMBER: _ClassVar[int]
    MATCH_LOCK_UNTIL_MS_FIELD_NUMBER: _ClassVar[int]
    MATCH_LOCK_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    READY_GENERATION_FIELD_NUMBER: _ClassVar[int]
    MATCH_START_RECEIPT_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    captain_id: int
    state: TeamState
    members: _containers.RepeatedCompositeFieldContainer[TeamMemberStorageRecord]
    created_at_ms: int
    updated_at_ms: int
    max_size: int
    map_id: int
    match_lock_until_ms: int
    match_lock_operation_id: str
    ready_generation: int
    match_start_receipt: MatchStartReceipt
    def __init__(self, team_id: _Optional[int] = ..., captain_id: _Optional[int] = ..., state: _Optional[_Union[TeamState, str]] = ..., members: _Optional[_Iterable[_Union[TeamMemberStorageRecord, _Mapping]]] = ..., created_at_ms: _Optional[int] = ..., updated_at_ms: _Optional[int] = ..., max_size: _Optional[int] = ..., map_id: _Optional[int] = ..., match_lock_until_ms: _Optional[int] = ..., match_lock_operation_id: _Optional[str] = ..., ready_generation: _Optional[int] = ..., match_start_receipt: _Optional[_Union[MatchStartReceipt, _Mapping]] = ...) -> None: ...

class MatchStartReceipt(_message.Message):
    __slots__ = ("attempt_id", "roster", "consumed_ready_generation", "post_ready_generation", "created_at_ms")
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    ROSTER_FIELD_NUMBER: _ClassVar[int]
    CONSUMED_READY_GENERATION_FIELD_NUMBER: _ClassVar[int]
    POST_READY_GENERATION_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    attempt_id: str
    roster: _containers.RepeatedCompositeFieldContainer[TeamMemberStorageRecord]
    consumed_ready_generation: int
    post_ready_generation: int
    created_at_ms: int
    def __init__(self, attempt_id: _Optional[str] = ..., roster: _Optional[_Iterable[_Union[TeamMemberStorageRecord, _Mapping]]] = ..., consumed_ready_generation: _Optional[int] = ..., post_ready_generation: _Optional[int] = ..., created_at_ms: _Optional[int] = ...) -> None: ...

class CreateTeamRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CreateTeamResponse(_message.Message):
    __slots__ = ("code", "team_id", "team")
    CODE_FIELD_NUMBER: _ClassVar[int]
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    team_id: int
    team: Team
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., team_id: _Optional[int] = ..., team: _Optional[_Union[Team, _Mapping]] = ...) -> None: ...

class InviteRequest(_message.Message):
    __slots__ = ("team_id", "target_player_id")
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    target_player_id: int
    def __init__(self, team_id: _Optional[int] = ..., target_player_id: _Optional[int] = ...) -> None: ...

class InviteResponse(_message.Message):
    __slots__ = ("code", "team", "invite_id", "expires_at_ms")
    CODE_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    INVITE_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_MS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    team: Team
    invite_id: int
    expires_at_ms: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., team: _Optional[_Union[Team, _Mapping]] = ..., invite_id: _Optional[int] = ..., expires_at_ms: _Optional[int] = ...) -> None: ...

class AcceptInviteRequest(_message.Message):
    __slots__ = ("team_id", "invite_id")
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    INVITE_ID_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    invite_id: int
    def __init__(self, team_id: _Optional[int] = ..., invite_id: _Optional[int] = ...) -> None: ...

class AcceptInviteResponse(_message.Message):
    __slots__ = ("code", "team")
    CODE_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    team: Team
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., team: _Optional[_Union[Team, _Mapping]] = ...) -> None: ...

class LeaveTeamRequest(_message.Message):
    __slots__ = ("team_id",)
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    def __init__(self, team_id: _Optional[int] = ...) -> None: ...

class LeaveTeamResponse(_message.Message):
    __slots__ = ("code", "team")
    CODE_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    team: Team
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., team: _Optional[_Union[Team, _Mapping]] = ...) -> None: ...

class KickRequest(_message.Message):
    __slots__ = ("team_id", "target_player_id")
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    target_player_id: int
    def __init__(self, team_id: _Optional[int] = ..., target_player_id: _Optional[int] = ...) -> None: ...

class KickResponse(_message.Message):
    __slots__ = ("code", "team")
    CODE_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    team: Team
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., team: _Optional[_Union[Team, _Mapping]] = ...) -> None: ...

class SetReadyRequest(_message.Message):
    __slots__ = ("team_id", "ready", "hero_id")
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    READY_FIELD_NUMBER: _ClassVar[int]
    HERO_ID_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    ready: bool
    hero_id: int
    def __init__(self, team_id: _Optional[int] = ..., ready: bool = ..., hero_id: _Optional[int] = ...) -> None: ...

class SetReadyResponse(_message.Message):
    __slots__ = ("code", "team")
    CODE_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    team: Team
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., team: _Optional[_Union[Team, _Mapping]] = ...) -> None: ...

class GetTeamRequest(_message.Message):
    __slots__ = ("team_id",)
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    def __init__(self, team_id: _Optional[int] = ...) -> None: ...

class GetTeamResponse(_message.Message):
    __slots__ = ("code", "team")
    CODE_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    team: Team
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., team: _Optional[_Union[Team, _Mapping]] = ...) -> None: ...

class GetMyTeamRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetMyTeamResponse(_message.Message):
    __slots__ = ("code", "has_team_msg", "team")
    CODE_FIELD_NUMBER: _ClassVar[int]
    HAS_TEAM_MSG_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    has_team_msg: bool
    team: Team
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., has_team_msg: bool = ..., team: _Optional[_Union[Team, _Mapping]] = ...) -> None: ...

class ListMyPendingInvitesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PendingInvite(_message.Message):
    __slots__ = ("team_id", "invite_id", "inviter_id", "expires_at_ms")
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    INVITE_ID_FIELD_NUMBER: _ClassVar[int]
    INVITER_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_MS_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    invite_id: int
    inviter_id: int
    expires_at_ms: int
    def __init__(self, team_id: _Optional[int] = ..., invite_id: _Optional[int] = ..., inviter_id: _Optional[int] = ..., expires_at_ms: _Optional[int] = ...) -> None: ...

class ListMyPendingInvitesResponse(_message.Message):
    __slots__ = ("code", "invites")
    CODE_FIELD_NUMBER: _ClassVar[int]
    INVITES_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    invites: _containers.RepeatedCompositeFieldContainer[PendingInvite]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., invites: _Optional[_Iterable[_Union[PendingInvite, _Mapping]]] = ...) -> None: ...

class SetTeamMapRequest(_message.Message):
    __slots__ = ("team_id", "map_id")
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    MAP_ID_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    map_id: int
    def __init__(self, team_id: _Optional[int] = ..., map_id: _Optional[int] = ...) -> None: ...

class SetTeamMapResponse(_message.Message):
    __slots__ = ("code", "team")
    CODE_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    team: Team
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., team: _Optional[_Union[Team, _Mapping]] = ...) -> None: ...

class OpenTeamBrief(_message.Message):
    __slots__ = ("team_id", "captain_id", "member_count", "max_size", "map_id", "created_at_ms", "join_policy", "captain_nickname", "captain_player_no")
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    CAPTAIN_ID_FIELD_NUMBER: _ClassVar[int]
    MEMBER_COUNT_FIELD_NUMBER: _ClassVar[int]
    MAX_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAP_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    JOIN_POLICY_FIELD_NUMBER: _ClassVar[int]
    CAPTAIN_NICKNAME_FIELD_NUMBER: _ClassVar[int]
    CAPTAIN_PLAYER_NO_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    captain_id: int
    member_count: int
    max_size: int
    map_id: int
    created_at_ms: int
    join_policy: TeamJoinPolicy
    captain_nickname: str
    captain_player_no: int
    def __init__(self, team_id: _Optional[int] = ..., captain_id: _Optional[int] = ..., member_count: _Optional[int] = ..., max_size: _Optional[int] = ..., map_id: _Optional[int] = ..., created_at_ms: _Optional[int] = ..., join_policy: _Optional[_Union[TeamJoinPolicy, str]] = ..., captain_nickname: _Optional[str] = ..., captain_player_no: _Optional[int] = ...) -> None: ...

class ListOpenTeamsRequest(_message.Message):
    __slots__ = ("map_id", "limit")
    MAP_ID_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    map_id: int
    limit: int
    def __init__(self, map_id: _Optional[int] = ..., limit: _Optional[int] = ...) -> None: ...

class ListOpenTeamsResponse(_message.Message):
    __slots__ = ("code", "teams")
    CODE_FIELD_NUMBER: _ClassVar[int]
    TEAMS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    teams: _containers.RepeatedCompositeFieldContainer[OpenTeamBrief]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., teams: _Optional[_Iterable[_Union[OpenTeamBrief, _Mapping]]] = ...) -> None: ...

class ApplyToTeamRequest(_message.Message):
    __slots__ = ("team_id",)
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    def __init__(self, team_id: _Optional[int] = ...) -> None: ...

class ApplyToTeamResponse(_message.Message):
    __slots__ = ("code", "joined", "team", "expires_at_ms")
    CODE_FIELD_NUMBER: _ClassVar[int]
    JOINED_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_MS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    joined: bool
    team: Team
    expires_at_ms: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., joined: bool = ..., team: _Optional[_Union[Team, _Mapping]] = ..., expires_at_ms: _Optional[int] = ...) -> None: ...

class TeamApplication(_message.Message):
    __slots__ = ("player_id", "expires_at_ms", "nickname", "player_no")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_MS_FIELD_NUMBER: _ClassVar[int]
    NICKNAME_FIELD_NUMBER: _ClassVar[int]
    PLAYER_NO_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    expires_at_ms: int
    nickname: str
    player_no: int
    def __init__(self, player_id: _Optional[int] = ..., expires_at_ms: _Optional[int] = ..., nickname: _Optional[str] = ..., player_no: _Optional[int] = ...) -> None: ...

class ListTeamApplicationsRequest(_message.Message):
    __slots__ = ("team_id",)
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    def __init__(self, team_id: _Optional[int] = ...) -> None: ...

class ListTeamApplicationsResponse(_message.Message):
    __slots__ = ("code", "applications")
    CODE_FIELD_NUMBER: _ClassVar[int]
    APPLICATIONS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    applications: _containers.RepeatedCompositeFieldContainer[TeamApplication]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., applications: _Optional[_Iterable[_Union[TeamApplication, _Mapping]]] = ...) -> None: ...

class HandleTeamApplicationRequest(_message.Message):
    __slots__ = ("team_id", "applicant_id", "accept")
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    APPLICANT_ID_FIELD_NUMBER: _ClassVar[int]
    ACCEPT_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    applicant_id: int
    accept: bool
    def __init__(self, team_id: _Optional[int] = ..., applicant_id: _Optional[int] = ..., accept: bool = ...) -> None: ...

class HandleTeamApplicationResponse(_message.Message):
    __slots__ = ("code", "team")
    CODE_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    team: Team
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., team: _Optional[_Union[Team, _Mapping]] = ...) -> None: ...

class TeamUpdateEvent(_message.Message):
    __slots__ = ("team", "by_player_id", "to_player_id", "ts_ms", "reason", "invite_id")
    TEAM_FIELD_NUMBER: _ClassVar[int]
    BY_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    TO_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    INVITE_ID_FIELD_NUMBER: _ClassVar[int]
    team: Team
    by_player_id: int
    to_player_id: int
    ts_ms: int
    reason: TeamUpdateReason
    invite_id: int
    def __init__(self, team: _Optional[_Union[Team, _Mapping]] = ..., by_player_id: _Optional[int] = ..., to_player_id: _Optional[int] = ..., ts_ms: _Optional[int] = ..., reason: _Optional[_Union[TeamUpdateReason, str]] = ..., invite_id: _Optional[int] = ...) -> None: ...

class TeamInviteEvent(_message.Message):
    __slots__ = ("team_id", "invite_id", "inviter_id", "to_player_id", "ts_ms", "expires_at_ms")
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    INVITE_ID_FIELD_NUMBER: _ClassVar[int]
    INVITER_ID_FIELD_NUMBER: _ClassVar[int]
    TO_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_MS_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    invite_id: int
    inviter_id: int
    to_player_id: int
    ts_ms: int
    expires_at_ms: int
    def __init__(self, team_id: _Optional[int] = ..., invite_id: _Optional[int] = ..., inviter_id: _Optional[int] = ..., to_player_id: _Optional[int] = ..., ts_ms: _Optional[int] = ..., expires_at_ms: _Optional[int] = ...) -> None: ...

class BeginTeamMatchRequest(_message.Message):
    __slots__ = ("team_id", "captain_id", "operation_id", "lease_ms", "require_ready")
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    CAPTAIN_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_MS_FIELD_NUMBER: _ClassVar[int]
    REQUIRE_READY_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    captain_id: int
    operation_id: str
    lease_ms: int
    require_ready: bool
    def __init__(self, team_id: _Optional[int] = ..., captain_id: _Optional[int] = ..., operation_id: _Optional[str] = ..., lease_ms: _Optional[int] = ..., require_ready: bool = ...) -> None: ...

class BeginTeamMatchResponse(_message.Message):
    __slots__ = ("code", "team", "lease_expires_at_ms", "ready_generation")
    CODE_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    LEASE_EXPIRES_AT_MS_FIELD_NUMBER: _ClassVar[int]
    READY_GENERATION_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    team: Team
    lease_expires_at_ms: int
    ready_generation: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., team: _Optional[_Union[Team, _Mapping]] = ..., lease_expires_at_ms: _Optional[int] = ..., ready_generation: _Optional[int] = ...) -> None: ...

class EndTeamMatchRequest(_message.Message):
    __slots__ = ("team_id", "player_ids", "expected_ready_generation")
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_READY_GENERATION_FIELD_NUMBER: _ClassVar[int]
    team_id: int
    player_ids: _containers.RepeatedScalarFieldContainer[int]
    expected_ready_generation: int
    def __init__(self, team_id: _Optional[int] = ..., player_ids: _Optional[_Iterable[int]] = ..., expected_ready_generation: _Optional[int] = ...) -> None: ...

class EndTeamMatchResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class GetPlayerTeamRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class GetPlayerTeamResponse(_message.Message):
    __slots__ = ("code", "has_team", "team_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    HAS_TEAM_FIELD_NUMBER: _ClassVar[int]
    TEAM_ID_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    has_team: bool
    team_id: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., has_team: bool = ..., team_id: _Optional[int] = ...) -> None: ...
