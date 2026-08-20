from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class KafkaEnvelope(_message.Message):
    __slots__ = ("topic", "key", "payload", "trace_id", "ts_ms")
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    topic: str
    key: str
    payload: bytes
    trace_id: str
    ts_ms: int
    def __init__(self, topic: _Optional[str] = ..., key: _Optional[str] = ..., payload: _Optional[bytes] = ..., trace_id: _Optional[str] = ..., ts_ms: _Optional[int] = ...) -> None: ...
