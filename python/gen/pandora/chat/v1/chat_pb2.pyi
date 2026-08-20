from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ChatChannel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CHAT_CHANNEL_UNSPECIFIED: _ClassVar[ChatChannel]
    CHAT_CHANNEL_WORLD: _ClassVar[ChatChannel]
    CHAT_CHANNEL_TEAM: _ClassVar[ChatChannel]
    CHAT_CHANNEL_PRIVATE: _ClassVar[ChatChannel]
    CHAT_CHANNEL_SYSTEM: _ClassVar[ChatChannel]
    CHAT_CHANNEL_GUILD: _ClassVar[ChatChannel]
    CHAT_CHANNEL_GROUP: _ClassVar[ChatChannel]
CHAT_CHANNEL_UNSPECIFIED: ChatChannel
CHAT_CHANNEL_WORLD: ChatChannel
CHAT_CHANNEL_TEAM: ChatChannel
CHAT_CHANNEL_PRIVATE: ChatChannel
CHAT_CHANNEL_SYSTEM: ChatChannel
CHAT_CHANNEL_GUILD: ChatChannel
CHAT_CHANNEL_GROUP: ChatChannel

class ChatMessage(_message.Message):
    __slots__ = ("message_id", "sender_id", "sender_nickname", "channel", "target_id", "content", "send_time_ms")
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    SENDER_ID_FIELD_NUMBER: _ClassVar[int]
    SENDER_NICKNAME_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    TARGET_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    SEND_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    message_id: int
    sender_id: int
    sender_nickname: str
    channel: ChatChannel
    target_id: int
    content: str
    send_time_ms: int
    def __init__(self, message_id: _Optional[int] = ..., sender_id: _Optional[int] = ..., sender_nickname: _Optional[str] = ..., channel: _Optional[_Union[ChatChannel, str]] = ..., target_id: _Optional[int] = ..., content: _Optional[str] = ..., send_time_ms: _Optional[int] = ...) -> None: ...

class SendMessageRequest(_message.Message):
    __slots__ = ("channel", "target_id", "content", "request_id")
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    TARGET_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    channel: ChatChannel
    target_id: int
    content: str
    request_id: str
    def __init__(self, channel: _Optional[_Union[ChatChannel, str]] = ..., target_id: _Optional[int] = ..., content: _Optional[str] = ..., request_id: _Optional[str] = ...) -> None: ...

class SendMessageResponse(_message.Message):
    __slots__ = ("code", "message_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    message_id: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., message_id: _Optional[int] = ...) -> None: ...

class PullHistoryRequest(_message.Message):
    __slots__ = ("channel", "peer_id", "limit", "before_ms")
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    BEFORE_MS_FIELD_NUMBER: _ClassVar[int]
    channel: ChatChannel
    peer_id: int
    limit: int
    before_ms: int
    def __init__(self, channel: _Optional[_Union[ChatChannel, str]] = ..., peer_id: _Optional[int] = ..., limit: _Optional[int] = ..., before_ms: _Optional[int] = ...) -> None: ...

class PullHistoryResponse(_message.Message):
    __slots__ = ("code", "messages")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    messages: _containers.RepeatedCompositeFieldContainer[ChatMessage]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., messages: _Optional[_Iterable[_Union[ChatMessage, _Mapping]]] = ...) -> None: ...

class ChatPushEvent(_message.Message):
    __slots__ = ("message", "to_player_id")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    TO_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    message: ChatMessage
    to_player_id: int
    def __init__(self, message: _Optional[_Union[ChatMessage, _Mapping]] = ..., to_player_id: _Optional[int] = ...) -> None: ...
