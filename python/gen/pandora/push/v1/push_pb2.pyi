from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class SubscribeRequest(_message.Message):
    __slots__ = ("session_token", "last_seen_ms")
    SESSION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    LAST_SEEN_MS_FIELD_NUMBER: _ClassVar[int]
    session_token: str
    last_seen_ms: int
    def __init__(self, session_token: _Optional[str] = ..., last_seen_ms: _Optional[int] = ...) -> None: ...

class PushFrame(_message.Message):
    __slots__ = ("topic", "payload", "ts_ms", "trace_id", "event_type")
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    topic: str
    payload: bytes
    ts_ms: int
    trace_id: str
    event_type: int
    def __init__(self, topic: _Optional[str] = ..., payload: _Optional[bytes] = ..., ts_ms: _Optional[int] = ..., trace_id: _Optional[str] = ..., event_type: _Optional[int] = ...) -> None: ...
