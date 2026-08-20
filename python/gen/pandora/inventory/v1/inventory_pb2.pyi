from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from pandora.bag.v1 import bag_pb2 as _bag_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EscrowSide(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ESCROW_SIDE_UNSPECIFIED: _ClassVar[EscrowSide]
    ESCROW_SIDE_SELL: _ClassVar[EscrowSide]
    ESCROW_SIDE_BUY: _ClassVar[EscrowSide]

class CurrencyKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CURRENCY_KIND_UNSPECIFIED: _ClassVar[CurrencyKind]
    CURRENCY_KIND_GOLD: _ClassVar[CurrencyKind]
ESCROW_SIDE_UNSPECIFIED: EscrowSide
ESCROW_SIDE_SELL: EscrowSide
ESCROW_SIDE_BUY: EscrowSide
CURRENCY_KIND_UNSPECIFIED: CurrencyKind
CURRENCY_KIND_GOLD: CurrencyKind

class ItemStack(_message.Message):
    __slots__ = ("item_config_id", "count")
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    item_config_id: int
    count: int
    def __init__(self, item_config_id: _Optional[int] = ..., count: _Optional[int] = ...) -> None: ...

class ItemGrant(_message.Message):
    __slots__ = ("item_config_id", "count")
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    item_config_id: int
    count: int
    def __init__(self, item_config_id: _Optional[int] = ..., count: _Optional[int] = ...) -> None: ...

class Inventory(_message.Message):
    __slots__ = ("player_id", "gold", "items", "capacity", "instances")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    GOLD_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    CAPACITY_FIELD_NUMBER: _ClassVar[int]
    INSTANCES_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    gold: int
    items: _containers.RepeatedCompositeFieldContainer[ItemStack]
    capacity: int
    instances: _containers.RepeatedCompositeFieldContainer[ItemInstance]
    def __init__(self, player_id: _Optional[int] = ..., gold: _Optional[int] = ..., items: _Optional[_Iterable[_Union[ItemStack, _Mapping]]] = ..., capacity: _Optional[int] = ..., instances: _Optional[_Iterable[_Union[ItemInstance, _Mapping]]] = ...) -> None: ...

class ItemAttribute(_message.Message):
    __slots__ = ("attr_id", "value")
    ATTR_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    attr_id: int
    value: int
    def __init__(self, attr_id: _Optional[int] = ..., value: _Optional[int] = ...) -> None: ...

class ItemInstanceAttributesStorageRecord(_message.Message):
    __slots__ = ("attributes",)
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    attributes: _containers.RepeatedCompositeFieldContainer[ItemAttribute]
    def __init__(self, attributes: _Optional[_Iterable[_Union[ItemAttribute, _Mapping]]] = ...) -> None: ...

class ItemInstance(_message.Message):
    __slots__ = ("instance_id", "item_config_id", "identified", "attributes", "slot_index", "bound")
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    IDENTIFIED_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    SLOT_INDEX_FIELD_NUMBER: _ClassVar[int]
    BOUND_FIELD_NUMBER: _ClassVar[int]
    instance_id: int
    item_config_id: int
    identified: bool
    attributes: _containers.RepeatedCompositeFieldContainer[ItemAttribute]
    slot_index: int
    bound: bool
    def __init__(self, instance_id: _Optional[int] = ..., item_config_id: _Optional[int] = ..., identified: bool = ..., attributes: _Optional[_Iterable[_Union[ItemAttribute, _Mapping]]] = ..., slot_index: _Optional[int] = ..., bound: bool = ...) -> None: ...

class GetInventoryRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class GetInventoryResponse(_message.Message):
    __slots__ = ("code", "inventory")
    CODE_FIELD_NUMBER: _ClassVar[int]
    INVENTORY_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    inventory: Inventory
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., inventory: _Optional[_Union[Inventory, _Mapping]] = ...) -> None: ...

class GrantItemsRequest(_message.Message):
    __slots__ = ("player_id", "items", "gold", "idempotency_key")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    GOLD_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    items: _containers.RepeatedCompositeFieldContainer[ItemGrant]
    gold: int
    idempotency_key: str
    def __init__(self, player_id: _Optional[int] = ..., items: _Optional[_Iterable[_Union[ItemGrant, _Mapping]]] = ..., gold: _Optional[int] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class GrantItemsResponse(_message.Message):
    __slots__ = ("code", "gold")
    CODE_FIELD_NUMBER: _ClassVar[int]
    GOLD_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    gold: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., gold: _Optional[int] = ...) -> None: ...

class CheckItemsOwnedRequest(_message.Message):
    __slots__ = ("player_id", "item_config_ids")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_IDS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    item_config_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, player_id: _Optional[int] = ..., item_config_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class CheckItemsOwnedResponse(_message.Message):
    __slots__ = ("code", "owned_item_config_ids")
    CODE_FIELD_NUMBER: _ClassVar[int]
    OWNED_ITEM_CONFIG_IDS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    owned_item_config_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., owned_item_config_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class InstanceOwnershipQuery(_message.Message):
    __slots__ = ("instance_id", "item_config_id")
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    instance_id: int
    item_config_id: int
    def __init__(self, instance_id: _Optional[int] = ..., item_config_id: _Optional[int] = ...) -> None: ...

class CheckInstancesOwnedRequest(_message.Message):
    __slots__ = ("player_id", "instances")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCES_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    instances: _containers.RepeatedCompositeFieldContainer[InstanceOwnershipQuery]
    def __init__(self, player_id: _Optional[int] = ..., instances: _Optional[_Iterable[_Union[InstanceOwnershipQuery, _Mapping]]] = ...) -> None: ...

class CheckInstancesOwnedResponse(_message.Message):
    __slots__ = ("code", "owned_instance_ids", "owned_instances")
    CODE_FIELD_NUMBER: _ClassVar[int]
    OWNED_INSTANCE_IDS_FIELD_NUMBER: _ClassVar[int]
    OWNED_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    owned_instance_ids: _containers.RepeatedScalarFieldContainer[int]
    owned_instances: _containers.RepeatedCompositeFieldContainer[ItemInstance]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., owned_instance_ids: _Optional[_Iterable[int]] = ..., owned_instances: _Optional[_Iterable[_Union[ItemInstance, _Mapping]]] = ...) -> None: ...

class UseItemRequest(_message.Message):
    __slots__ = ("player_id", "item_config_id", "count", "idempotency_key")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    item_config_id: int
    count: int
    idempotency_key: str
    def __init__(self, player_id: _Optional[int] = ..., item_config_id: _Optional[int] = ..., count: _Optional[int] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class UseItemResponse(_message.Message):
    __slots__ = ("code", "remaining")
    CODE_FIELD_NUMBER: _ClassVar[int]
    REMAINING_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    remaining: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., remaining: _Optional[int] = ...) -> None: ...

class ConsumeBattleItemRequest(_message.Message):
    __slots__ = ("player_id", "item_config_id", "count", "idempotency_key")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    item_config_id: int
    count: int
    idempotency_key: str
    def __init__(self, player_id: _Optional[int] = ..., item_config_id: _Optional[int] = ..., count: _Optional[int] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class ConsumeBattleItemResponse(_message.Message):
    __slots__ = ("code", "remaining")
    CODE_FIELD_NUMBER: _ClassVar[int]
    REMAINING_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    remaining: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., remaining: _Optional[int] = ...) -> None: ...

class DiscardBattleItemRequest(_message.Message):
    __slots__ = ("player_id", "item_config_id", "count", "idempotency_key")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    item_config_id: int
    count: int
    idempotency_key: str
    def __init__(self, player_id: _Optional[int] = ..., item_config_id: _Optional[int] = ..., count: _Optional[int] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class DiscardBattleItemResponse(_message.Message):
    __slots__ = ("code", "remaining")
    CODE_FIELD_NUMBER: _ClassVar[int]
    REMAINING_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    remaining: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., remaining: _Optional[int] = ...) -> None: ...

class SellItemRequest(_message.Message):
    __slots__ = ("player_id", "item_config_id", "count", "idempotency_key")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    item_config_id: int
    count: int
    idempotency_key: str
    def __init__(self, player_id: _Optional[int] = ..., item_config_id: _Optional[int] = ..., count: _Optional[int] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class SellItemResponse(_message.Message):
    __slots__ = ("code", "remaining", "gold")
    CODE_FIELD_NUMBER: _ClassVar[int]
    REMAINING_FIELD_NUMBER: _ClassVar[int]
    GOLD_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    remaining: int
    gold: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., remaining: _Optional[int] = ..., gold: _Optional[int] = ...) -> None: ...

class DiscardItemRequest(_message.Message):
    __slots__ = ("player_id", "item_config_id", "count", "idempotency_key")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    item_config_id: int
    count: int
    idempotency_key: str
    def __init__(self, player_id: _Optional[int] = ..., item_config_id: _Optional[int] = ..., count: _Optional[int] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class DiscardItemResponse(_message.Message):
    __slots__ = ("code", "remaining")
    CODE_FIELD_NUMBER: _ClassVar[int]
    REMAINING_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    remaining: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., remaining: _Optional[int] = ...) -> None: ...

class SettleAuctionMatchRequest(_message.Message):
    __slots__ = ("match_id", "seller_id", "buyer_id", "item_config_id", "quantity", "unit_price", "sell_order_id", "buy_order_id")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    SELLER_ID_FIELD_NUMBER: _ClassVar[int]
    BUYER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    UNIT_PRICE_FIELD_NUMBER: _ClassVar[int]
    SELL_ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    BUY_ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    seller_id: int
    buyer_id: int
    item_config_id: int
    quantity: int
    unit_price: int
    sell_order_id: int
    buy_order_id: int
    def __init__(self, match_id: _Optional[int] = ..., seller_id: _Optional[int] = ..., buyer_id: _Optional[int] = ..., item_config_id: _Optional[int] = ..., quantity: _Optional[int] = ..., unit_price: _Optional[int] = ..., sell_order_id: _Optional[int] = ..., buy_order_id: _Optional[int] = ...) -> None: ...

class SettleAuctionMatchResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class SettlePlayerTradeRequest(_message.Message):
    __slots__ = ("order_id", "seller_id", "buyer_id", "seller_items", "buyer_items", "price")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    SELLER_ID_FIELD_NUMBER: _ClassVar[int]
    BUYER_ID_FIELD_NUMBER: _ClassVar[int]
    SELLER_ITEMS_FIELD_NUMBER: _ClassVar[int]
    BUYER_ITEMS_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    order_id: int
    seller_id: int
    buyer_id: int
    seller_items: _containers.RepeatedCompositeFieldContainer[ItemGrant]
    buyer_items: _containers.RepeatedCompositeFieldContainer[ItemGrant]
    price: int
    def __init__(self, order_id: _Optional[int] = ..., seller_id: _Optional[int] = ..., buyer_id: _Optional[int] = ..., seller_items: _Optional[_Iterable[_Union[ItemGrant, _Mapping]]] = ..., buyer_items: _Optional[_Iterable[_Union[ItemGrant, _Mapping]]] = ..., price: _Optional[int] = ...) -> None: ...

class SettlePlayerTradeResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class FreezeForOrderRequest(_message.Message):
    __slots__ = ("player_id", "order_id", "side", "item_config_id", "quantity", "unit_price")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    SIDE_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    UNIT_PRICE_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    order_id: int
    side: EscrowSide
    item_config_id: int
    quantity: int
    unit_price: int
    def __init__(self, player_id: _Optional[int] = ..., order_id: _Optional[int] = ..., side: _Optional[_Union[EscrowSide, str]] = ..., item_config_id: _Optional[int] = ..., quantity: _Optional[int] = ..., unit_price: _Optional[int] = ...) -> None: ...

class FreezeForOrderResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class EnsureAuctionEscrowRequest(_message.Message):
    __slots__ = ("player_id", "order_id", "side", "item_config_id", "remaining_quantity", "unit_price")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    SIDE_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    REMAINING_QUANTITY_FIELD_NUMBER: _ClassVar[int]
    UNIT_PRICE_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    order_id: int
    side: EscrowSide
    item_config_id: int
    remaining_quantity: int
    unit_price: int
    def __init__(self, player_id: _Optional[int] = ..., order_id: _Optional[int] = ..., side: _Optional[_Union[EscrowSide, str]] = ..., item_config_id: _Optional[int] = ..., remaining_quantity: _Optional[int] = ..., unit_price: _Optional[int] = ...) -> None: ...

class EnsureAuctionEscrowResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class ReleaseEscrowRequest(_message.Message):
    __slots__ = ("player_id", "order_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    order_id: int
    def __init__(self, player_id: _Optional[int] = ..., order_id: _Optional[int] = ...) -> None: ...

class ReleaseEscrowResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class GrantInstancesRequest(_message.Message):
    __slots__ = ("player_id", "item_config_ids", "idempotency_key")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_IDS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    item_config_ids: _containers.RepeatedScalarFieldContainer[int]
    idempotency_key: str
    def __init__(self, player_id: _Optional[int] = ..., item_config_ids: _Optional[_Iterable[int]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class GrantInstancesResponse(_message.Message):
    __slots__ = ("code", "instances")
    CODE_FIELD_NUMBER: _ClassVar[int]
    INSTANCES_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    instances: _containers.RepeatedCompositeFieldContainer[ItemInstance]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., instances: _Optional[_Iterable[_Union[ItemInstance, _Mapping]]] = ...) -> None: ...

class IdentifyItemRequest(_message.Message):
    __slots__ = ("player_id", "instance_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    instance_id: int
    def __init__(self, player_id: _Optional[int] = ..., instance_id: _Optional[int] = ...) -> None: ...

class IdentifyItemResponse(_message.Message):
    __slots__ = ("code", "instance")
    CODE_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    instance: ItemInstance
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., instance: _Optional[_Union[ItemInstance, _Mapping]] = ...) -> None: ...

class DiscardInstanceRequest(_message.Message):
    __slots__ = ("player_id", "instance_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    instance_id: int
    def __init__(self, player_id: _Optional[int] = ..., instance_id: _Optional[int] = ...) -> None: ...

class DiscardInstanceResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class MoveInstanceRequest(_message.Message):
    __slots__ = ("player_id", "instance_id", "to_slot_index")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    TO_SLOT_INDEX_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    instance_id: int
    to_slot_index: int
    def __init__(self, player_id: _Optional[int] = ..., instance_id: _Optional[int] = ..., to_slot_index: _Optional[int] = ...) -> None: ...

class MoveInstanceResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class SellInstanceRequest(_message.Message):
    __slots__ = ("player_id", "instance_id", "item_config_id", "idempotency_key")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    instance_id: int
    item_config_id: int
    idempotency_key: str
    def __init__(self, player_id: _Optional[int] = ..., instance_id: _Optional[int] = ..., item_config_id: _Optional[int] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class SellInstanceResponse(_message.Message):
    __slots__ = ("code", "gold")
    CODE_FIELD_NUMBER: _ClassVar[int]
    GOLD_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    gold: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., gold: _Optional[int] = ...) -> None: ...

class EscrowOutInstancesRequest(_message.Message):
    __slots__ = ("source_player_id", "to_player_id", "instance_ids", "escrow_key")
    SOURCE_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    TO_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_IDS_FIELD_NUMBER: _ClassVar[int]
    ESCROW_KEY_FIELD_NUMBER: _ClassVar[int]
    source_player_id: int
    to_player_id: int
    instance_ids: _containers.RepeatedScalarFieldContainer[int]
    escrow_key: str
    def __init__(self, source_player_id: _Optional[int] = ..., to_player_id: _Optional[int] = ..., instance_ids: _Optional[_Iterable[int]] = ..., escrow_key: _Optional[str] = ...) -> None: ...

class EscrowOutInstancesResponse(_message.Message):
    __slots__ = ("code", "items")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    items: _containers.RepeatedCompositeFieldContainer[_bag_pb2.BagItem]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., items: _Optional[_Iterable[_Union[_bag_pb2.BagItem, _Mapping]]] = ...) -> None: ...

class TransferClaimItem(_message.Message):
    __slots__ = ("instance_id", "item_config_id")
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    instance_id: int
    item_config_id: int
    def __init__(self, instance_id: _Optional[int] = ..., item_config_id: _Optional[int] = ...) -> None: ...

class ClaimTransferInstancesRequest(_message.Message):
    __slots__ = ("to_player_id", "items", "idempotency_key")
    TO_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    to_player_id: int
    items: _containers.RepeatedCompositeFieldContainer[TransferClaimItem]
    idempotency_key: str
    def __init__(self, to_player_id: _Optional[int] = ..., items: _Optional[_Iterable[_Union[TransferClaimItem, _Mapping]]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class ClaimTransferInstancesResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class ReleaseTransferEscrowRequest(_message.Message):
    __slots__ = ("instance_ids",)
    INSTANCE_IDS_FIELD_NUMBER: _ClassVar[int]
    instance_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, instance_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class ReleaseTransferEscrowResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class ConsumeTransferEscrowRequest(_message.Message):
    __slots__ = ("to_player_id", "instance_ids")
    TO_PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_IDS_FIELD_NUMBER: _ClassVar[int]
    to_player_id: int
    instance_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, to_player_id: _Optional[int] = ..., instance_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class ConsumeTransferEscrowResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...
