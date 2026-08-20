from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OwnerType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OWNER_TYPE_NONE: _ClassVar[OwnerType]
    OWNER_TYPE_HUB: _ClassVar[OwnerType]
    OWNER_TYPE_BATTLE: _ClassVar[OwnerType]

class OwnerPhase(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OWNER_PHASE_UNSPECIFIED: _ClassVar[OwnerPhase]
    OWNER_PHASE_PENDING: _ClassVar[OwnerPhase]
    OWNER_PHASE_ADMITTED: _ClassVar[OwnerPhase]
OWNER_TYPE_NONE: OwnerType
OWNER_TYPE_HUB: OwnerType
OWNER_TYPE_BATTLE: OwnerType
OWNER_PHASE_UNSPECIFIED: OwnerPhase
OWNER_PHASE_PENDING: OwnerPhase
OWNER_PHASE_ADMITTED: OwnerPhase

class OwnerTarget(_message.Message):
    __slots__ = ("pod_name", "instance_uid", "instance_epoch", "assignment_or_allocation_id", "release_track")
    POD_NAME_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ASSIGNMENT_OR_ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    pod_name: str
    instance_uid: str
    instance_epoch: int
    assignment_or_allocation_id: str
    release_track: str
    def __init__(self, pod_name: _Optional[str] = ..., instance_uid: _Optional[str] = ..., instance_epoch: _Optional[int] = ..., assignment_or_allocation_id: _Optional[str] = ..., release_track: _Optional[str] = ...) -> None: ...

class OwnerRecord(_message.Message):
    __slots__ = ("player_id", "owner_epoch", "owner_type", "phase", "target", "operation_id", "admit_not_before_ms", "lease_deadline_ms", "updated_at_ms", "hub_source_revision")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    OWNER_TYPE_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    ADMIT_NOT_BEFORE_MS_FIELD_NUMBER: _ClassVar[int]
    LEASE_DEADLINE_MS_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    HUB_SOURCE_REVISION_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    owner_epoch: int
    owner_type: OwnerType
    phase: OwnerPhase
    target: OwnerTarget
    operation_id: str
    admit_not_before_ms: int
    lease_deadline_ms: int
    updated_at_ms: int
    hub_source_revision: int
    def __init__(self, player_id: _Optional[int] = ..., owner_epoch: _Optional[int] = ..., owner_type: _Optional[_Union[OwnerType, str]] = ..., phase: _Optional[_Union[OwnerPhase, str]] = ..., target: _Optional[_Union[OwnerTarget, _Mapping]] = ..., operation_id: _Optional[str] = ..., admit_not_before_ms: _Optional[int] = ..., lease_deadline_ms: _Optional[int] = ..., updated_at_ms: _Optional[int] = ..., hub_source_revision: _Optional[int] = ...) -> None: ...

class QueryOwnerRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class QueryOwnerResponse(_message.Message):
    __slots__ = ("code", "record")
    CODE_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    record: OwnerRecord
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., record: _Optional[_Union[OwnerRecord, _Mapping]] = ...) -> None: ...

class BeginTransitionRequest(_message.Message):
    __slots__ = ("player_id", "expect_epoch", "operation_id", "owner_type", "target", "source_revision")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECT_EPOCH_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_TYPE_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    SOURCE_REVISION_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    expect_epoch: int
    operation_id: str
    owner_type: OwnerType
    target: OwnerTarget
    source_revision: int
    def __init__(self, player_id: _Optional[int] = ..., expect_epoch: _Optional[int] = ..., operation_id: _Optional[str] = ..., owner_type: _Optional[_Union[OwnerType, str]] = ..., target: _Optional[_Union[OwnerTarget, _Mapping]] = ..., source_revision: _Optional[int] = ...) -> None: ...

class BeginTransitionResponse(_message.Message):
    __slots__ = ("code", "record")
    CODE_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    record: OwnerRecord
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., record: _Optional[_Union[OwnerRecord, _Mapping]] = ...) -> None: ...

class AdmitRequest(_message.Message):
    __slots__ = ("player_id", "owner_epoch", "operation_id", "target")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    owner_epoch: int
    operation_id: str
    target: OwnerTarget
    def __init__(self, player_id: _Optional[int] = ..., owner_epoch: _Optional[int] = ..., operation_id: _Optional[str] = ..., target: _Optional[_Union[OwnerTarget, _Mapping]] = ...) -> None: ...

class AdmitResponse(_message.Message):
    __slots__ = ("code", "record", "retry_after_ms")
    CODE_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    RETRY_AFTER_MS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    record: OwnerRecord
    retry_after_ms: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., record: _Optional[_Union[OwnerRecord, _Mapping]] = ..., retry_after_ms: _Optional[int] = ...) -> None: ...

class RenewInstanceLeaseRequest(_message.Message):
    __slots__ = ("target", "lease_seconds")
    TARGET_FIELD_NUMBER: _ClassVar[int]
    LEASE_SECONDS_FIELD_NUMBER: _ClassVar[int]
    target: OwnerTarget
    lease_seconds: int
    def __init__(self, target: _Optional[_Union[OwnerTarget, _Mapping]] = ..., lease_seconds: _Optional[int] = ...) -> None: ...

class RenewInstanceLeaseResponse(_message.Message):
    __slots__ = ("code", "lease_deadline_ms")
    CODE_FIELD_NUMBER: _ClassVar[int]
    LEASE_DEADLINE_MS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    lease_deadline_ms: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., lease_deadline_ms: _Optional[int] = ...) -> None: ...

class ReleaseOwnerRequest(_message.Message):
    __slots__ = ("player_id", "owner_epoch", "operation_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    owner_epoch: int
    operation_id: str
    def __init__(self, player_id: _Optional[int] = ..., owner_epoch: _Optional[int] = ..., operation_id: _Optional[str] = ...) -> None: ...

class ReleaseOwnerResponse(_message.Message):
    __slots__ = ("code", "record")
    CODE_FIELD_NUMBER: _ClassVar[int]
    RECORD_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    record: OwnerRecord
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., record: _Optional[_Union[OwnerRecord, _Mapping]] = ...) -> None: ...
