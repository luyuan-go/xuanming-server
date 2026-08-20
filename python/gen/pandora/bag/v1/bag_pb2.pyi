from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BagItemAttribute(_message.Message):
    __slots__ = ("attr_id", "value")
    ATTR_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    attr_id: int
    value: int
    def __init__(self, attr_id: _Optional[int] = ..., value: _Optional[int] = ...) -> None: ...

class BagItem(_message.Message):
    __slots__ = ("item_config_id", "count", "slot", "instance_id", "identified", "attrs")
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    SLOT_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    IDENTIFIED_FIELD_NUMBER: _ClassVar[int]
    ATTRS_FIELD_NUMBER: _ClassVar[int]
    item_config_id: int
    count: int
    slot: int
    instance_id: int
    identified: bool
    attrs: _containers.RepeatedCompositeFieldContainer[BagItemAttribute]
    def __init__(self, item_config_id: _Optional[int] = ..., count: _Optional[int] = ..., slot: _Optional[int] = ..., instance_id: _Optional[int] = ..., identified: bool = ..., attrs: _Optional[_Iterable[_Union[BagItemAttribute, _Mapping]]] = ...) -> None: ...

class BagSection(_message.Message):
    __slots__ = ("bag_type", "generation", "capacity", "items")
    BAG_TYPE_FIELD_NUMBER: _ClassVar[int]
    GENERATION_FIELD_NUMBER: _ClassVar[int]
    CAPACITY_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    bag_type: int
    generation: int
    capacity: int
    items: _containers.RepeatedCompositeFieldContainer[BagItem]
    def __init__(self, bag_type: _Optional[int] = ..., generation: _Optional[int] = ..., capacity: _Optional[int] = ..., items: _Optional[_Iterable[_Union[BagItem, _Mapping]]] = ...) -> None: ...

class BagStorageRecord(_message.Message):
    __slots__ = ("sections",)
    SECTIONS_FIELD_NUMBER: _ClassVar[int]
    sections: _containers.RepeatedCompositeFieldContainer[BagSection]
    def __init__(self, sections: _Optional[_Iterable[_Union[BagSection, _Mapping]]] = ...) -> None: ...

class PickupGrantOp(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[BagItem]
    def __init__(self, items: _Optional[_Iterable[_Union[BagItem, _Mapping]]] = ...) -> None: ...

class MailClaimOp(_message.Message):
    __slots__ = ("mail_id", "claim_key", "items")
    MAIL_ID_FIELD_NUMBER: _ClassVar[int]
    CLAIM_KEY_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    mail_id: int
    claim_key: str
    items: _containers.RepeatedCompositeFieldContainer[BagItem]
    def __init__(self, mail_id: _Optional[int] = ..., claim_key: _Optional[str] = ..., items: _Optional[_Iterable[_Union[BagItem, _Mapping]]] = ...) -> None: ...

class TransferOp(_message.Message):
    __slots__ = ("to_bag_type", "to_generation", "items")
    TO_BAG_TYPE_FIELD_NUMBER: _ClassVar[int]
    TO_GENERATION_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    to_bag_type: int
    to_generation: int
    items: _containers.RepeatedCompositeFieldContainer[BagItem]
    def __init__(self, to_bag_type: _Optional[int] = ..., to_generation: _Optional[int] = ..., items: _Optional[_Iterable[_Union[BagItem, _Mapping]]] = ...) -> None: ...

class ConsumeOp(_message.Message):
    __slots__ = ("consume_items", "produce_bag_type", "produce_generation", "produce_items")
    CONSUME_ITEMS_FIELD_NUMBER: _ClassVar[int]
    PRODUCE_BAG_TYPE_FIELD_NUMBER: _ClassVar[int]
    PRODUCE_GENERATION_FIELD_NUMBER: _ClassVar[int]
    PRODUCE_ITEMS_FIELD_NUMBER: _ClassVar[int]
    consume_items: _containers.RepeatedCompositeFieldContainer[BagItem]
    produce_bag_type: int
    produce_generation: int
    produce_items: _containers.RepeatedCompositeFieldContainer[BagItem]
    def __init__(self, consume_items: _Optional[_Iterable[_Union[BagItem, _Mapping]]] = ..., produce_bag_type: _Optional[int] = ..., produce_generation: _Optional[int] = ..., produce_items: _Optional[_Iterable[_Union[BagItem, _Mapping]]] = ...) -> None: ...

class BagJournalEntry(_message.Message):
    __slots__ = ("journal_seq", "bag_type", "generation", "idempotency_key", "ts_ms", "pickup_grant", "mail_claim", "transfer", "consume")
    JOURNAL_SEQ_FIELD_NUMBER: _ClassVar[int]
    BAG_TYPE_FIELD_NUMBER: _ClassVar[int]
    GENERATION_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    PICKUP_GRANT_FIELD_NUMBER: _ClassVar[int]
    MAIL_CLAIM_FIELD_NUMBER: _ClassVar[int]
    TRANSFER_FIELD_NUMBER: _ClassVar[int]
    CONSUME_FIELD_NUMBER: _ClassVar[int]
    journal_seq: int
    bag_type: int
    generation: int
    idempotency_key: str
    ts_ms: int
    pickup_grant: PickupGrantOp
    mail_claim: MailClaimOp
    transfer: TransferOp
    consume: ConsumeOp
    def __init__(self, journal_seq: _Optional[int] = ..., bag_type: _Optional[int] = ..., generation: _Optional[int] = ..., idempotency_key: _Optional[str] = ..., ts_ms: _Optional[int] = ..., pickup_grant: _Optional[_Union[PickupGrantOp, _Mapping]] = ..., mail_claim: _Optional[_Union[MailClaimOp, _Mapping]] = ..., transfer: _Optional[_Union[TransferOp, _Mapping]] = ..., consume: _Optional[_Union[ConsumeOp, _Mapping]] = ...) -> None: ...

class LoadBagRequest(_message.Message):
    __slots__ = ("player_id", "owner_epoch")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    owner_epoch: int
    def __init__(self, player_id: _Optional[int] = ..., owner_epoch: _Optional[int] = ...) -> None: ...

class BagEffectiveCapacity(_message.Message):
    __slots__ = ("bag_type", "capacity")
    BAG_TYPE_FIELD_NUMBER: _ClassVar[int]
    CAPACITY_FIELD_NUMBER: _ClassVar[int]
    bag_type: int
    capacity: int
    def __init__(self, bag_type: _Optional[int] = ..., capacity: _Optional[int] = ...) -> None: ...

class LoadBagResponse(_message.Message):
    __slots__ = ("code", "snapshot", "tail", "last_journal_seq", "effective_capacities")
    CODE_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    TAIL_FIELD_NUMBER: _ClassVar[int]
    LAST_JOURNAL_SEQ_FIELD_NUMBER: _ClassVar[int]
    EFFECTIVE_CAPACITIES_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    snapshot: BagStorageRecord
    tail: _containers.RepeatedCompositeFieldContainer[BagJournalEntry]
    last_journal_seq: int
    effective_capacities: _containers.RepeatedCompositeFieldContainer[BagEffectiveCapacity]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., snapshot: _Optional[_Union[BagStorageRecord, _Mapping]] = ..., tail: _Optional[_Iterable[_Union[BagJournalEntry, _Mapping]]] = ..., last_journal_seq: _Optional[int] = ..., effective_capacities: _Optional[_Iterable[_Union[BagEffectiveCapacity, _Mapping]]] = ...) -> None: ...

class AppendJournalRequest(_message.Message):
    __slots__ = ("player_id", "owner_epoch", "entries")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    owner_epoch: int
    entries: _containers.RepeatedCompositeFieldContainer[BagJournalEntry]
    def __init__(self, player_id: _Optional[int] = ..., owner_epoch: _Optional[int] = ..., entries: _Optional[_Iterable[_Union[BagJournalEntry, _Mapping]]] = ...) -> None: ...

class AppendJournalResponse(_message.Message):
    __slots__ = ("code", "acked_seq")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ACKED_SEQ_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    acked_seq: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., acked_seq: _Optional[int] = ...) -> None: ...

class SaveCheckpointRequest(_message.Message):
    __slots__ = ("player_id", "owner_epoch", "snapshot", "covered_journal_seq")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    COVERED_JOURNAL_SEQ_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    owner_epoch: int
    snapshot: BagStorageRecord
    covered_journal_seq: int
    def __init__(self, player_id: _Optional[int] = ..., owner_epoch: _Optional[int] = ..., snapshot: _Optional[_Union[BagStorageRecord, _Mapping]] = ..., covered_journal_seq: _Optional[int] = ...) -> None: ...

class SaveCheckpointResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class PurchaseCapacityRequest(_message.Message):
    __slots__ = ("player_id", "owner_epoch", "bag_type")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_EPOCH_FIELD_NUMBER: _ClassVar[int]
    BAG_TYPE_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    owner_epoch: int
    bag_type: int
    def __init__(self, player_id: _Optional[int] = ..., owner_epoch: _Optional[int] = ..., bag_type: _Optional[int] = ...) -> None: ...

class PurchaseCapacityResponse(_message.Message):
    __slots__ = ("code", "purchases", "extra", "effective_capacity", "gold_cost", "gold_remaining")
    CODE_FIELD_NUMBER: _ClassVar[int]
    PURCHASES_FIELD_NUMBER: _ClassVar[int]
    EXTRA_FIELD_NUMBER: _ClassVar[int]
    EFFECTIVE_CAPACITY_FIELD_NUMBER: _ClassVar[int]
    GOLD_COST_FIELD_NUMBER: _ClassVar[int]
    GOLD_REMAINING_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    purchases: int
    extra: int
    effective_capacity: int
    gold_cost: int
    gold_remaining: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., purchases: _Optional[int] = ..., extra: _Optional[int] = ..., effective_capacity: _Optional[int] = ..., gold_cost: _Optional[int] = ..., gold_remaining: _Optional[int] = ...) -> None: ...

class GetSectionsRequest(_message.Message):
    __slots__ = ("player_id", "bag_types")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    BAG_TYPES_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    bag_types: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, player_id: _Optional[int] = ..., bag_types: _Optional[_Iterable[int]] = ...) -> None: ...

class GetSectionsResponse(_message.Message):
    __slots__ = ("code", "sections")
    CODE_FIELD_NUMBER: _ClassVar[int]
    SECTIONS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    sections: _containers.RepeatedCompositeFieldContainer[BagSection]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., sections: _Optional[_Iterable[_Union[BagSection, _Mapping]]] = ...) -> None: ...
