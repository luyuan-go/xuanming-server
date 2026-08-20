from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GuildRole(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GUILD_ROLE_UNSPECIFIED: _ClassVar[GuildRole]
    GUILD_ROLE_LEADER: _ClassVar[GuildRole]
    GUILD_ROLE_OFFICER: _ClassVar[GuildRole]
    GUILD_ROLE_MEMBER: _ClassVar[GuildRole]

class GuildJoinStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GUILD_JOIN_STATUS_UNSPECIFIED: _ClassVar[GuildJoinStatus]
    GUILD_JOIN_STATUS_PENDING: _ClassVar[GuildJoinStatus]
    GUILD_JOIN_STATUS_APPROVED: _ClassVar[GuildJoinStatus]
    GUILD_JOIN_STATUS_REJECTED: _ClassVar[GuildJoinStatus]

class GuildEventType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GUILD_EVENT_TYPE_UNSPECIFIED: _ClassVar[GuildEventType]
    GUILD_EVENT_TYPE_JOIN_APPLIED: _ClassVar[GuildEventType]
    GUILD_EVENT_TYPE_JOIN_APPROVED: _ClassVar[GuildEventType]
    GUILD_EVENT_TYPE_JOIN_REJECTED: _ClassVar[GuildEventType]
    GUILD_EVENT_TYPE_KICKED: _ClassVar[GuildEventType]
    GUILD_EVENT_TYPE_DISBANDED: _ClassVar[GuildEventType]
    GUILD_EVENT_TYPE_LEADER_CHANGED: _ClassVar[GuildEventType]
GUILD_ROLE_UNSPECIFIED: GuildRole
GUILD_ROLE_LEADER: GuildRole
GUILD_ROLE_OFFICER: GuildRole
GUILD_ROLE_MEMBER: GuildRole
GUILD_JOIN_STATUS_UNSPECIFIED: GuildJoinStatus
GUILD_JOIN_STATUS_PENDING: GuildJoinStatus
GUILD_JOIN_STATUS_APPROVED: GuildJoinStatus
GUILD_JOIN_STATUS_REJECTED: GuildJoinStatus
GUILD_EVENT_TYPE_UNSPECIFIED: GuildEventType
GUILD_EVENT_TYPE_JOIN_APPLIED: GuildEventType
GUILD_EVENT_TYPE_JOIN_APPROVED: GuildEventType
GUILD_EVENT_TYPE_JOIN_REJECTED: GuildEventType
GUILD_EVENT_TYPE_KICKED: GuildEventType
GUILD_EVENT_TYPE_DISBANDED: GuildEventType
GUILD_EVENT_TYPE_LEADER_CHANGED: GuildEventType

class GuildMember(_message.Message):
    __slots__ = ("player_id", "nickname", "role", "joined_ms")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    NICKNAME_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    JOINED_MS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    nickname: str
    role: GuildRole
    joined_ms: int
    def __init__(self, player_id: _Optional[int] = ..., nickname: _Optional[str] = ..., role: _Optional[_Union[GuildRole, str]] = ..., joined_ms: _Optional[int] = ...) -> None: ...

class Guild(_message.Message):
    __slots__ = ("guild_id", "name", "leader_id", "member_count", "max_members", "created_ms")
    GUILD_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    MEMBER_COUNT_FIELD_NUMBER: _ClassVar[int]
    MAX_MEMBERS_FIELD_NUMBER: _ClassVar[int]
    CREATED_MS_FIELD_NUMBER: _ClassVar[int]
    guild_id: int
    name: str
    leader_id: int
    member_count: int
    max_members: int
    created_ms: int
    def __init__(self, guild_id: _Optional[int] = ..., name: _Optional[str] = ..., leader_id: _Optional[int] = ..., member_count: _Optional[int] = ..., max_members: _Optional[int] = ..., created_ms: _Optional[int] = ...) -> None: ...

class GuildJoinRequest(_message.Message):
    __slots__ = ("request_id", "guild_id", "from_player_id", "from_nickname", "created_ms")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    GUILD_ID_FIELD_NUMBER: _ClassVar[int]
    FROM_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    FROM_NICKNAME_FIELD_NUMBER: _ClassVar[int]
    CREATED_MS_FIELD_NUMBER: _ClassVar[int]
    request_id: int
    guild_id: int
    from_player_id: int
    from_nickname: str
    created_ms: int
    def __init__(self, request_id: _Optional[int] = ..., guild_id: _Optional[int] = ..., from_player_id: _Optional[int] = ..., from_nickname: _Optional[str] = ..., created_ms: _Optional[int] = ...) -> None: ...

class CreateGuildRequest(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class CreateGuildResponse(_message.Message):
    __slots__ = ("code", "guild_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    GUILD_ID_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    guild_id: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., guild_id: _Optional[int] = ...) -> None: ...

class ApplyJoinRequest(_message.Message):
    __slots__ = ("guild_id",)
    GUILD_ID_FIELD_NUMBER: _ClassVar[int]
    guild_id: int
    def __init__(self, guild_id: _Optional[int] = ...) -> None: ...

class ApplyJoinResponse(_message.Message):
    __slots__ = ("code", "request_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    request_id: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., request_id: _Optional[int] = ...) -> None: ...

class ApproveJoinRequest(_message.Message):
    __slots__ = ("request_id",)
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    request_id: int
    def __init__(self, request_id: _Optional[int] = ...) -> None: ...

class ApproveJoinResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class RejectJoinRequest(_message.Message):
    __slots__ = ("request_id",)
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    request_id: int
    def __init__(self, request_id: _Optional[int] = ...) -> None: ...

class RejectJoinResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class LeaveGuildRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class LeaveGuildResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class KickMemberRequest(_message.Message):
    __slots__ = ("target_id",)
    TARGET_ID_FIELD_NUMBER: _ClassVar[int]
    target_id: int
    def __init__(self, target_id: _Optional[int] = ...) -> None: ...

class KickMemberResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class DisbandGuildRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DisbandGuildResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class TransferLeaderRequest(_message.Message):
    __slots__ = ("target_id",)
    TARGET_ID_FIELD_NUMBER: _ClassVar[int]
    target_id: int
    def __init__(self, target_id: _Optional[int] = ...) -> None: ...

class TransferLeaderResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class SetOfficerRequest(_message.Message):
    __slots__ = ("target_id", "is_officer")
    TARGET_ID_FIELD_NUMBER: _ClassVar[int]
    IS_OFFICER_FIELD_NUMBER: _ClassVar[int]
    target_id: int
    is_officer: bool
    def __init__(self, target_id: _Optional[int] = ..., is_officer: bool = ...) -> None: ...

class SetOfficerResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class GetGuildRequest(_message.Message):
    __slots__ = ("guild_id",)
    GUILD_ID_FIELD_NUMBER: _ClassVar[int]
    guild_id: int
    def __init__(self, guild_id: _Optional[int] = ...) -> None: ...

class GetGuildResponse(_message.Message):
    __slots__ = ("code", "guild")
    CODE_FIELD_NUMBER: _ClassVar[int]
    GUILD_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    guild: Guild
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., guild: _Optional[_Union[Guild, _Mapping]] = ...) -> None: ...

class GetMyGuildRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetMyGuildResponse(_message.Message):
    __slots__ = ("code", "guild")
    CODE_FIELD_NUMBER: _ClassVar[int]
    GUILD_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    guild: Guild
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., guild: _Optional[_Union[Guild, _Mapping]] = ...) -> None: ...

class ListMembersRequest(_message.Message):
    __slots__ = ("guild_id", "cursor", "limit")
    GUILD_ID_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    guild_id: int
    cursor: int
    limit: int
    def __init__(self, guild_id: _Optional[int] = ..., cursor: _Optional[int] = ..., limit: _Optional[int] = ...) -> None: ...

class ListMembersResponse(_message.Message):
    __slots__ = ("code", "members", "next_cursor")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    NEXT_CURSOR_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    members: _containers.RepeatedCompositeFieldContainer[GuildMember]
    next_cursor: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., members: _Optional[_Iterable[_Union[GuildMember, _Mapping]]] = ..., next_cursor: _Optional[int] = ...) -> None: ...

class ListJoinRequestsRequest(_message.Message):
    __slots__ = ("cursor", "limit")
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    cursor: int
    limit: int
    def __init__(self, cursor: _Optional[int] = ..., limit: _Optional[int] = ...) -> None: ...

class ListJoinRequestsResponse(_message.Message):
    __slots__ = ("code", "requests", "next_cursor")
    CODE_FIELD_NUMBER: _ClassVar[int]
    REQUESTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_CURSOR_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    requests: _containers.RepeatedCompositeFieldContainer[GuildJoinRequest]
    next_cursor: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., requests: _Optional[_Iterable[_Union[GuildJoinRequest, _Mapping]]] = ..., next_cursor: _Optional[int] = ...) -> None: ...

class GuildEvent(_message.Message):
    __slots__ = ("type", "guild_id", "to_player_id", "actor_id", "guild_name")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    GUILD_ID_FIELD_NUMBER: _ClassVar[int]
    TO_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ACTOR_ID_FIELD_NUMBER: _ClassVar[int]
    GUILD_NAME_FIELD_NUMBER: _ClassVar[int]
    type: GuildEventType
    guild_id: int
    to_player_id: int
    actor_id: int
    guild_name: str
    def __init__(self, type: _Optional[_Union[GuildEventType, str]] = ..., guild_id: _Optional[int] = ..., to_player_id: _Optional[int] = ..., actor_id: _Optional[int] = ..., guild_name: _Optional[str] = ...) -> None: ...

class GetPlayerGuildRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class GetPlayerGuildResponse(_message.Message):
    __slots__ = ("code", "has_guild", "guild_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    HAS_GUILD_FIELD_NUMBER: _ClassVar[int]
    GUILD_ID_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    has_guild: bool
    guild_id: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., has_guild: bool = ..., guild_id: _Optional[int] = ...) -> None: ...
