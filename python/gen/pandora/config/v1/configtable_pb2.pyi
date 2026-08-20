from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ReloadConfigTableRequest(_message.Message):
    __slots__ = ("expect_version",)
    EXPECT_VERSION_FIELD_NUMBER: _ClassVar[int]
    expect_version: int
    def __init__(self, expect_version: _Optional[int] = ...) -> None: ...

class ReloadConfigTableResponse(_message.Message):
    __slots__ = ("code", "active_version", "reloaded", "detail")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_VERSION_FIELD_NUMBER: _ClassVar[int]
    RELOADED_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    active_version: int
    reloaded: bool
    detail: str
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., active_version: _Optional[int] = ..., reloaded: bool = ..., detail: _Optional[str] = ...) -> None: ...
