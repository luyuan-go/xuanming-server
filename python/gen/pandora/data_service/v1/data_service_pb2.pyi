from google.protobuf import field_mask_pb2 as _field_mask_pb2
from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from proto2mysql import proto2mysql_option_pb2 as _proto2mysql_option_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PlayerData(_message.Message):
    __slots__ = ("player_id", "version", "nickname", "level", "mmr", "avatar", "created_at_ms", "last_seen_ms", "total_battles", "total_wins")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    NICKNAME_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    MMR_FIELD_NUMBER: _ClassVar[int]
    AVATAR_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    LAST_SEEN_MS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_BATTLES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_WINS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    version: int
    nickname: str
    level: int
    mmr: int
    avatar: str
    created_at_ms: int
    last_seen_ms: int
    total_battles: int
    total_wins: int
    def __init__(self, player_id: _Optional[int] = ..., version: _Optional[int] = ..., nickname: _Optional[str] = ..., level: _Optional[int] = ..., mmr: _Optional[int] = ..., avatar: _Optional[str] = ..., created_at_ms: _Optional[int] = ..., last_seen_ms: _Optional[int] = ..., total_battles: _Optional[int] = ..., total_wins: _Optional[int] = ...) -> None: ...

class ReadPlayerRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class ReadPlayerResponse(_message.Message):
    __slots__ = ("code", "data")
    CODE_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    data: PlayerData
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., data: _Optional[_Union[PlayerData, _Mapping]] = ...) -> None: ...

class WritePlayerRequest(_message.Message):
    __slots__ = ("data", "update_mask")
    DATA_FIELD_NUMBER: _ClassVar[int]
    UPDATE_MASK_FIELD_NUMBER: _ClassVar[int]
    data: PlayerData
    update_mask: _field_mask_pb2.FieldMask
    def __init__(self, data: _Optional[_Union[PlayerData, _Mapping]] = ..., update_mask: _Optional[_Union[_field_mask_pb2.FieldMask, _Mapping]] = ...) -> None: ...

class WritePlayerResponse(_message.Message):
    __slots__ = ("code", "new_version")
    CODE_FIELD_NUMBER: _ClassVar[int]
    NEW_VERSION_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    new_version: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., new_version: _Optional[int] = ...) -> None: ...

class InvalidateCacheRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class InvalidateCacheResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...
