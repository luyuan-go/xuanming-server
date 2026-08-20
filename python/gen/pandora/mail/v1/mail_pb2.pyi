from pandora.bag.v1 import bag_pb2 as _bag_pb2
from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MailChannel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MAIL_CHANNEL_UNSPECIFIED: _ClassVar[MailChannel]
    MAIL_CHANNEL_SYSTEM: _ClassVar[MailChannel]
    MAIL_CHANNEL_GUILD: _ClassVar[MailChannel]
    MAIL_CHANNEL_PERSONAL: _ClassVar[MailChannel]

class MailStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MAIL_STATUS_UNSPECIFIED: _ClassVar[MailStatus]
    MAIL_STATUS_UNREAD: _ClassVar[MailStatus]
    MAIL_STATUS_READ: _ClassVar[MailStatus]
    MAIL_STATUS_CLAIMED: _ClassVar[MailStatus]

class MailPushEventType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MAIL_PUSH_EVENT_TYPE_UNSPECIFIED: _ClassVar[MailPushEventType]
MAIL_CHANNEL_UNSPECIFIED: MailChannel
MAIL_CHANNEL_SYSTEM: MailChannel
MAIL_CHANNEL_GUILD: MailChannel
MAIL_CHANNEL_PERSONAL: MailChannel
MAIL_STATUS_UNSPECIFIED: MailStatus
MAIL_STATUS_UNREAD: MailStatus
MAIL_STATUS_READ: MailStatus
MAIL_STATUS_CLAIMED: MailStatus
MAIL_PUSH_EVENT_TYPE_UNSPECIFIED: MailPushEventType

class MailAttachment(_message.Message):
    __slots__ = ("stack", "instance", "transfer")
    STACK_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_FIELD_NUMBER: _ClassVar[int]
    stack: StackAttachment
    instance: InstanceAttachment
    transfer: TransferAttachment
    def __init__(self, stack: _Optional[_Union[StackAttachment, _Mapping]] = ..., instance: _Optional[_Union[InstanceAttachment, _Mapping]] = ..., transfer: _Optional[_Union[TransferAttachment, _Mapping]] = ...) -> None: ...

class StackAttachment(_message.Message):
    __slots__ = ("item_config_id", "count")
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    item_config_id: int
    count: int
    def __init__(self, item_config_id: _Optional[int] = ..., count: _Optional[int] = ...) -> None: ...

class InstanceAttachment(_message.Message):
    __slots__ = ("item_config_id", "count")
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    item_config_id: int
    count: int
    def __init__(self, item_config_id: _Optional[int] = ..., count: _Optional[int] = ...) -> None: ...

class TransferAttachment(_message.Message):
    __slots__ = ("item", "source_player_id")
    ITEM_FIELD_NUMBER: _ClassVar[int]
    SOURCE_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    item: _bag_pb2.BagItem
    source_player_id: int
    def __init__(self, item: _Optional[_Union[_bag_pb2.BagItem, _Mapping]] = ..., source_player_id: _Optional[int] = ...) -> None: ...

class Mail(_message.Message):
    __slots__ = ("mail_id", "channel", "title", "body", "attachments", "status", "claimed", "created_ms", "expire_ms")
    MAIL_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    ATTACHMENTS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CLAIMED_FIELD_NUMBER: _ClassVar[int]
    CREATED_MS_FIELD_NUMBER: _ClassVar[int]
    EXPIRE_MS_FIELD_NUMBER: _ClassVar[int]
    mail_id: int
    channel: MailChannel
    title: str
    body: str
    attachments: _containers.RepeatedCompositeFieldContainer[MailAttachment]
    status: MailStatus
    claimed: bool
    created_ms: int
    expire_ms: int
    def __init__(self, mail_id: _Optional[int] = ..., channel: _Optional[_Union[MailChannel, str]] = ..., title: _Optional[str] = ..., body: _Optional[str] = ..., attachments: _Optional[_Iterable[_Union[MailAttachment, _Mapping]]] = ..., status: _Optional[_Union[MailStatus, str]] = ..., claimed: bool = ..., created_ms: _Optional[int] = ..., expire_ms: _Optional[int] = ...) -> None: ...

class ListMailRequest(_message.Message):
    __slots__ = ("cursor", "limit")
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    cursor: int
    limit: int
    def __init__(self, cursor: _Optional[int] = ..., limit: _Optional[int] = ...) -> None: ...

class ListMailResponse(_message.Message):
    __slots__ = ("code", "mails", "next_cursor")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MAILS_FIELD_NUMBER: _ClassVar[int]
    NEXT_CURSOR_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    mails: _containers.RepeatedCompositeFieldContainer[Mail]
    next_cursor: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., mails: _Optional[_Iterable[_Union[Mail, _Mapping]]] = ..., next_cursor: _Optional[int] = ...) -> None: ...

class ReadMailRequest(_message.Message):
    __slots__ = ("mail_id",)
    MAIL_ID_FIELD_NUMBER: _ClassVar[int]
    mail_id: int
    def __init__(self, mail_id: _Optional[int] = ...) -> None: ...

class ReadMailResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class ClaimMailRequest(_message.Message):
    __slots__ = ("mail_id",)
    MAIL_ID_FIELD_NUMBER: _ClassVar[int]
    mail_id: int
    def __init__(self, mail_id: _Optional[int] = ...) -> None: ...

class ClaimMailResponse(_message.Message):
    __slots__ = ("code", "attachments")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ATTACHMENTS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    attachments: _containers.RepeatedCompositeFieldContainer[MailAttachment]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., attachments: _Optional[_Iterable[_Union[MailAttachment, _Mapping]]] = ...) -> None: ...

class DeleteMailRequest(_message.Message):
    __slots__ = ("mail_id",)
    MAIL_ID_FIELD_NUMBER: _ClassVar[int]
    mail_id: int
    def __init__(self, mail_id: _Optional[int] = ...) -> None: ...

class DeleteMailResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class SendSystemMailRequest(_message.Message):
    __slots__ = ("title", "body", "attachments", "start_ms", "end_ms")
    TITLE_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    ATTACHMENTS_FIELD_NUMBER: _ClassVar[int]
    START_MS_FIELD_NUMBER: _ClassVar[int]
    END_MS_FIELD_NUMBER: _ClassVar[int]
    title: str
    body: str
    attachments: _containers.RepeatedCompositeFieldContainer[MailAttachment]
    start_ms: int
    end_ms: int
    def __init__(self, title: _Optional[str] = ..., body: _Optional[str] = ..., attachments: _Optional[_Iterable[_Union[MailAttachment, _Mapping]]] = ..., start_ms: _Optional[int] = ..., end_ms: _Optional[int] = ...) -> None: ...

class SendSystemMailResponse(_message.Message):
    __slots__ = ("code", "mail_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MAIL_ID_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    mail_id: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., mail_id: _Optional[int] = ...) -> None: ...

class SendGuildMailRequest(_message.Message):
    __slots__ = ("guild_id", "title", "body", "attachments", "start_ms", "end_ms")
    GUILD_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    ATTACHMENTS_FIELD_NUMBER: _ClassVar[int]
    START_MS_FIELD_NUMBER: _ClassVar[int]
    END_MS_FIELD_NUMBER: _ClassVar[int]
    guild_id: int
    title: str
    body: str
    attachments: _containers.RepeatedCompositeFieldContainer[MailAttachment]
    start_ms: int
    end_ms: int
    def __init__(self, guild_id: _Optional[int] = ..., title: _Optional[str] = ..., body: _Optional[str] = ..., attachments: _Optional[_Iterable[_Union[MailAttachment, _Mapping]]] = ..., start_ms: _Optional[int] = ..., end_ms: _Optional[int] = ...) -> None: ...

class SendGuildMailResponse(_message.Message):
    __slots__ = ("code", "mail_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MAIL_ID_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    mail_id: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., mail_id: _Optional[int] = ...) -> None: ...

class SendPersonalMailRequest(_message.Message):
    __slots__ = ("to_player_id", "title", "body", "attachments", "expire_ms", "instance_grant_key")
    TO_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    ATTACHMENTS_FIELD_NUMBER: _ClassVar[int]
    EXPIRE_MS_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_GRANT_KEY_FIELD_NUMBER: _ClassVar[int]
    to_player_id: int
    title: str
    body: str
    attachments: _containers.RepeatedCompositeFieldContainer[MailAttachment]
    expire_ms: int
    instance_grant_key: str
    def __init__(self, to_player_id: _Optional[int] = ..., title: _Optional[str] = ..., body: _Optional[str] = ..., attachments: _Optional[_Iterable[_Union[MailAttachment, _Mapping]]] = ..., expire_ms: _Optional[int] = ..., instance_grant_key: _Optional[str] = ...) -> None: ...

class SendPersonalMailResponse(_message.Message):
    __slots__ = ("code", "mail_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MAIL_ID_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    mail_id: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., mail_id: _Optional[int] = ...) -> None: ...

class MailContentStorageRecord(_message.Message):
    __slots__ = ("title", "body", "attachments", "instance_grant_key")
    TITLE_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    ATTACHMENTS_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_GRANT_KEY_FIELD_NUMBER: _ClassVar[int]
    title: str
    body: str
    attachments: _containers.RepeatedCompositeFieldContainer[MailAttachment]
    instance_grant_key: str
    def __init__(self, title: _Optional[str] = ..., body: _Optional[str] = ..., attachments: _Optional[_Iterable[_Union[MailAttachment, _Mapping]]] = ..., instance_grant_key: _Optional[str] = ...) -> None: ...

class MailClaimIntentStorageRecord(_message.Message):
    __slots__ = ("items", "transfer_instance_ids")
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_INSTANCE_IDS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[_bag_pb2.BagItem]
    transfer_instance_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, items: _Optional[_Iterable[_Union[_bag_pb2.BagItem, _Mapping]]] = ..., transfer_instance_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class GetClaimableAttachmentsRequest(_message.Message):
    __slots__ = ("player_id", "mail_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    MAIL_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    mail_id: int
    def __init__(self, player_id: _Optional[int] = ..., mail_id: _Optional[int] = ...) -> None: ...

class GetClaimableAttachmentsResponse(_message.Message):
    __slots__ = ("code", "items", "claim_key", "already_claimed")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    CLAIM_KEY_FIELD_NUMBER: _ClassVar[int]
    ALREADY_CLAIMED_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    items: _containers.RepeatedCompositeFieldContainer[_bag_pb2.BagItem]
    claim_key: str
    already_claimed: bool
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., items: _Optional[_Iterable[_Union[_bag_pb2.BagItem, _Mapping]]] = ..., claim_key: _Optional[str] = ..., already_claimed: bool = ...) -> None: ...

class MarkMailClaimedRequest(_message.Message):
    __slots__ = ("player_id", "mail_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    MAIL_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    mail_id: int
    def __init__(self, player_id: _Optional[int] = ..., mail_id: _Optional[int] = ...) -> None: ...

class MarkMailClaimedResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...
