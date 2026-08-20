from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GroupRole(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GROUP_ROLE_UNSPECIFIED: _ClassVar[GroupRole]
    GROUP_ROLE_OWNER: _ClassVar[GroupRole]
    GROUP_ROLE_MEMBER: _ClassVar[GroupRole]
GROUP_ROLE_UNSPECIFIED: GroupRole
GROUP_ROLE_OWNER: GroupRole
GROUP_ROLE_MEMBER: GroupRole

class GroupMember(_message.Message):
    __slots__ = ("player_id", "nickname", "role", "joined_ms")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    NICKNAME_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    JOINED_MS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    nickname: str
    role: GroupRole
    joined_ms: int
    def __init__(self, player_id: _Optional[int] = ..., nickname: _Optional[str] = ..., role: _Optional[_Union[GroupRole, str]] = ..., joined_ms: _Optional[int] = ...) -> None: ...

class Group(_message.Message):
    __slots__ = ("group_id", "name", "owner_id", "member_count", "max_members", "created_ms")
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    MEMBER_COUNT_FIELD_NUMBER: _ClassVar[int]
    MAX_MEMBERS_FIELD_NUMBER: _ClassVar[int]
    CREATED_MS_FIELD_NUMBER: _ClassVar[int]
    group_id: int
    name: str
    owner_id: int
    member_count: int
    max_members: int
    created_ms: int
    def __init__(self, group_id: _Optional[int] = ..., name: _Optional[str] = ..., owner_id: _Optional[int] = ..., member_count: _Optional[int] = ..., max_members: _Optional[int] = ..., created_ms: _Optional[int] = ...) -> None: ...

class CreateGroupRequest(_message.Message):
    __slots__ = ("name", "member_ids")
    NAME_FIELD_NUMBER: _ClassVar[int]
    MEMBER_IDS_FIELD_NUMBER: _ClassVar[int]
    name: str
    member_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, name: _Optional[str] = ..., member_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class CreateGroupResponse(_message.Message):
    __slots__ = ("code", "group_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    group_id: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., group_id: _Optional[int] = ...) -> None: ...

class InviteToGroupRequest(_message.Message):
    __slots__ = ("group_id", "target_id")
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_ID_FIELD_NUMBER: _ClassVar[int]
    group_id: int
    target_id: int
    def __init__(self, group_id: _Optional[int] = ..., target_id: _Optional[int] = ...) -> None: ...

class InviteToGroupResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class LeaveGroupRequest(_message.Message):
    __slots__ = ("group_id",)
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    group_id: int
    def __init__(self, group_id: _Optional[int] = ...) -> None: ...

class LeaveGroupResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class KickFromGroupRequest(_message.Message):
    __slots__ = ("group_id", "target_id")
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_ID_FIELD_NUMBER: _ClassVar[int]
    group_id: int
    target_id: int
    def __init__(self, group_id: _Optional[int] = ..., target_id: _Optional[int] = ...) -> None: ...

class KickFromGroupResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class DisbandGroupRequest(_message.Message):
    __slots__ = ("group_id",)
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    group_id: int
    def __init__(self, group_id: _Optional[int] = ...) -> None: ...

class DisbandGroupResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class TransferOwnerRequest(_message.Message):
    __slots__ = ("group_id", "target_id")
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_ID_FIELD_NUMBER: _ClassVar[int]
    group_id: int
    target_id: int
    def __init__(self, group_id: _Optional[int] = ..., target_id: _Optional[int] = ...) -> None: ...

class TransferOwnerResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class GetGroupRequest(_message.Message):
    __slots__ = ("group_id",)
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    group_id: int
    def __init__(self, group_id: _Optional[int] = ...) -> None: ...

class GetGroupResponse(_message.Message):
    __slots__ = ("code", "group")
    CODE_FIELD_NUMBER: _ClassVar[int]
    GROUP_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    group: Group
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., group: _Optional[_Union[Group, _Mapping]] = ...) -> None: ...

class ListGroupMembersRequest(_message.Message):
    __slots__ = ("group_id",)
    GROUP_ID_FIELD_NUMBER: _ClassVar[int]
    group_id: int
    def __init__(self, group_id: _Optional[int] = ...) -> None: ...

class ListGroupMembersResponse(_message.Message):
    __slots__ = ("code", "members")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    members: _containers.RepeatedCompositeFieldContainer[GroupMember]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., members: _Optional[_Iterable[_Union[GroupMember, _Mapping]]] = ...) -> None: ...

class ListMyGroupsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListMyGroupsResponse(_message.Message):
    __slots__ = ("code", "groups")
    CODE_FIELD_NUMBER: _ClassVar[int]
    GROUPS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    groups: _containers.RepeatedCompositeFieldContainer[Group]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., groups: _Optional[_Iterable[_Union[Group, _Mapping]]] = ...) -> None: ...
