from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FriendRequestStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FRIEND_REQUEST_STATUS_UNSPECIFIED: _ClassVar[FriendRequestStatus]
    FRIEND_REQUEST_STATUS_PENDING: _ClassVar[FriendRequestStatus]
    FRIEND_REQUEST_STATUS_ACCEPTED: _ClassVar[FriendRequestStatus]
    FRIEND_REQUEST_STATUS_REJECTED: _ClassVar[FriendRequestStatus]
    FRIEND_REQUEST_STATUS_EXPIRED: _ClassVar[FriendRequestStatus]

class FriendPushEventType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FRIEND_PUSH_EVENT_TYPE_UNSPECIFIED: _ClassVar[FriendPushEventType]

class FriendEventReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FRIEND_EVENT_REASON_UNSPECIFIED: _ClassVar[FriendEventReason]
    FRIEND_EVENT_REASON_REQUEST_RECEIVED: _ClassVar[FriendEventReason]
    FRIEND_EVENT_REASON_REQUEST_ACCEPTED: _ClassVar[FriendEventReason]
FRIEND_REQUEST_STATUS_UNSPECIFIED: FriendRequestStatus
FRIEND_REQUEST_STATUS_PENDING: FriendRequestStatus
FRIEND_REQUEST_STATUS_ACCEPTED: FriendRequestStatus
FRIEND_REQUEST_STATUS_REJECTED: FriendRequestStatus
FRIEND_REQUEST_STATUS_EXPIRED: FriendRequestStatus
FRIEND_PUSH_EVENT_TYPE_UNSPECIFIED: FriendPushEventType
FRIEND_EVENT_REASON_UNSPECIFIED: FriendEventReason
FRIEND_EVENT_REASON_REQUEST_RECEIVED: FriendEventReason
FRIEND_EVENT_REASON_REQUEST_ACCEPTED: FriendEventReason

class FriendInfo(_message.Message):
    __slots__ = ("player_id", "nickname", "is_online", "since_ms", "last_seen_ms")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    NICKNAME_FIELD_NUMBER: _ClassVar[int]
    IS_ONLINE_FIELD_NUMBER: _ClassVar[int]
    SINCE_MS_FIELD_NUMBER: _ClassVar[int]
    LAST_SEEN_MS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    nickname: str
    is_online: bool
    since_ms: int
    last_seen_ms: int
    def __init__(self, player_id: _Optional[int] = ..., nickname: _Optional[str] = ..., is_online: bool = ..., since_ms: _Optional[int] = ..., last_seen_ms: _Optional[int] = ...) -> None: ...

class AddFriendRequest(_message.Message):
    __slots__ = ("target_player_id",)
    TARGET_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    target_player_id: int
    def __init__(self, target_player_id: _Optional[int] = ...) -> None: ...

class AddFriendResponse(_message.Message):
    __slots__ = ("code", "request_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    request_id: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., request_id: _Optional[int] = ...) -> None: ...

class AcceptFriendRequest(_message.Message):
    __slots__ = ("request_id",)
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    request_id: int
    def __init__(self, request_id: _Optional[int] = ...) -> None: ...

class AcceptFriendResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class RejectFriendRequest(_message.Message):
    __slots__ = ("request_id",)
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    request_id: int
    def __init__(self, request_id: _Optional[int] = ...) -> None: ...

class RejectFriendResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class FriendRequestInfo(_message.Message):
    __slots__ = ("request_id", "from_player_id", "from_nickname", "created_ms")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    FROM_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    FROM_NICKNAME_FIELD_NUMBER: _ClassVar[int]
    CREATED_MS_FIELD_NUMBER: _ClassVar[int]
    request_id: int
    from_player_id: int
    from_nickname: str
    created_ms: int
    def __init__(self, request_id: _Optional[int] = ..., from_player_id: _Optional[int] = ..., from_nickname: _Optional[str] = ..., created_ms: _Optional[int] = ...) -> None: ...

class ListFriendRequestsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListFriendRequestsResponse(_message.Message):
    __slots__ = ("code", "requests")
    CODE_FIELD_NUMBER: _ClassVar[int]
    REQUESTS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    requests: _containers.RepeatedCompositeFieldContainer[FriendRequestInfo]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., requests: _Optional[_Iterable[_Union[FriendRequestInfo, _Mapping]]] = ...) -> None: ...

class ListFriendsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListFriendsResponse(_message.Message):
    __slots__ = ("code", "friends")
    CODE_FIELD_NUMBER: _ClassVar[int]
    FRIENDS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    friends: _containers.RepeatedCompositeFieldContainer[FriendInfo]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., friends: _Optional[_Iterable[_Union[FriendInfo, _Mapping]]] = ...) -> None: ...

class RemoveFriendRequest(_message.Message):
    __slots__ = ("target_player_id",)
    TARGET_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    target_player_id: int
    def __init__(self, target_player_id: _Optional[int] = ...) -> None: ...

class RemoveFriendResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class BlockRequest(_message.Message):
    __slots__ = ("target_player_id",)
    TARGET_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    target_player_id: int
    def __init__(self, target_player_id: _Optional[int] = ...) -> None: ...

class BlockResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class UnblockRequest(_message.Message):
    __slots__ = ("target_player_id",)
    TARGET_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    target_player_id: int
    def __init__(self, target_player_id: _Optional[int] = ...) -> None: ...

class UnblockResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class BlockInfo(_message.Message):
    __slots__ = ("player_id", "nickname", "since_ms")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    NICKNAME_FIELD_NUMBER: _ClassVar[int]
    SINCE_MS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    nickname: str
    since_ms: int
    def __init__(self, player_id: _Optional[int] = ..., nickname: _Optional[str] = ..., since_ms: _Optional[int] = ...) -> None: ...

class ListBlocksRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListBlocksResponse(_message.Message):
    __slots__ = ("code", "blocks")
    CODE_FIELD_NUMBER: _ClassVar[int]
    BLOCKS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    blocks: _containers.RepeatedCompositeFieldContainer[BlockInfo]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., blocks: _Optional[_Iterable[_Union[BlockInfo, _Mapping]]] = ...) -> None: ...

class RecommendedFriendInfo(_message.Message):
    __slots__ = ("player_id", "nickname", "is_online", "last_seen_ms", "mutual_friend_count")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    NICKNAME_FIELD_NUMBER: _ClassVar[int]
    IS_ONLINE_FIELD_NUMBER: _ClassVar[int]
    LAST_SEEN_MS_FIELD_NUMBER: _ClassVar[int]
    MUTUAL_FRIEND_COUNT_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    nickname: str
    is_online: bool
    last_seen_ms: int
    mutual_friend_count: int
    def __init__(self, player_id: _Optional[int] = ..., nickname: _Optional[str] = ..., is_online: bool = ..., last_seen_ms: _Optional[int] = ..., mutual_friend_count: _Optional[int] = ...) -> None: ...

class RecommendFriendsRequest(_message.Message):
    __slots__ = ("limit", "exclude_player_ids")
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    limit: int
    exclude_player_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, limit: _Optional[int] = ..., exclude_player_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class RecommendFriendsResponse(_message.Message):
    __slots__ = ("code", "recommendations")
    CODE_FIELD_NUMBER: _ClassVar[int]
    RECOMMENDATIONS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    recommendations: _containers.RepeatedCompositeFieldContainer[RecommendedFriendInfo]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., recommendations: _Optional[_Iterable[_Union[RecommendedFriendInfo, _Mapping]]] = ...) -> None: ...

class FriendEvent(_message.Message):
    __slots__ = ("by_player_id", "to_player_id", "request_id", "reason", "ts_ms")
    BY_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    TO_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    by_player_id: int
    to_player_id: int
    request_id: int
    reason: FriendEventReason
    ts_ms: int
    def __init__(self, by_player_id: _Optional[int] = ..., to_player_id: _Optional[int] = ..., request_id: _Optional[int] = ..., reason: _Optional[_Union[FriendEventReason, str]] = ..., ts_ms: _Optional[int] = ...) -> None: ...
