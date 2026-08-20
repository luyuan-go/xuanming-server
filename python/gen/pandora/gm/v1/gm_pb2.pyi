from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AddItemCommand(_message.Message):
    __slots__ = ("player_id", "config_id", "count", "bag_type")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    BAG_TYPE_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    config_id: int
    count: int
    bag_type: int
    def __init__(self, player_id: _Optional[int] = ..., config_id: _Optional[int] = ..., count: _Optional[int] = ..., bag_type: _Optional[int] = ...) -> None: ...

class GmCommand(_message.Message):
    __slots__ = ("idempotency_key", "match_id", "created_at_ms", "add_item")
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    ADD_ITEM_FIELD_NUMBER: _ClassVar[int]
    idempotency_key: str
    match_id: int
    created_at_ms: int
    add_item: AddItemCommand
    def __init__(self, idempotency_key: _Optional[str] = ..., match_id: _Optional[int] = ..., created_at_ms: _Optional[int] = ..., add_item: _Optional[_Union[AddItemCommand, _Mapping]] = ...) -> None: ...

class SendCommandRequest(_message.Message):
    __slots__ = ("match_id", "add_item")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    ADD_ITEM_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    add_item: AddItemCommand
    def __init__(self, match_id: _Optional[int] = ..., add_item: _Optional[_Union[AddItemCommand, _Mapping]] = ...) -> None: ...

class SendCommandResponse(_message.Message):
    __slots__ = ("code", "idempotency_key")
    CODE_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    idempotency_key: str
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class PollCommandsRequest(_message.Message):
    __slots__ = ("match_id", "ds_pod_name", "max")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    MAX_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    ds_pod_name: str
    max: int
    def __init__(self, match_id: _Optional[int] = ..., ds_pod_name: _Optional[str] = ..., max: _Optional[int] = ...) -> None: ...

class PollCommandsResponse(_message.Message):
    __slots__ = ("code", "commands")
    CODE_FIELD_NUMBER: _ClassVar[int]
    COMMANDS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    commands: _containers.RepeatedCompositeFieldContainer[GmCommand]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., commands: _Optional[_Iterable[_Union[GmCommand, _Mapping]]] = ...) -> None: ...

class AckCommandRequest(_message.Message):
    __slots__ = ("match_id", "idempotency_key", "ok", "message")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    OK_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    idempotency_key: str
    ok: bool
    message: str
    def __init__(self, match_id: _Optional[int] = ..., idempotency_key: _Optional[str] = ..., ok: bool = ..., message: _Optional[str] = ...) -> None: ...

class AckCommandResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...
