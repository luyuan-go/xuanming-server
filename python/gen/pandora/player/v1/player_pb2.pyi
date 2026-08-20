from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from pandora.inventory.v1 import inventory_pb2 as _inventory_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PlayerPushEventType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PLAYER_PUSH_EVENT_TYPE_LEGACY_UPDATE: _ClassVar[PlayerPushEventType]
    PLAYER_PUSH_EVENT_TYPE_EXPERIENCE: _ClassVar[PlayerPushEventType]

class RewardSourceType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REWARD_SOURCE_TYPE_UNSPECIFIED: _ClassVar[RewardSourceType]
    REWARD_SOURCE_TYPE_PERMANENT: _ClassVar[RewardSourceType]
    REWARD_SOURCE_TYPE_ACTIVITY: _ClassVar[RewardSourceType]
PLAYER_PUSH_EVENT_TYPE_LEGACY_UPDATE: PlayerPushEventType
PLAYER_PUSH_EVENT_TYPE_EXPERIENCE: PlayerPushEventType
REWARD_SOURCE_TYPE_UNSPECIFIED: RewardSourceType
REWARD_SOURCE_TYPE_PERMANENT: RewardSourceType
REWARD_SOURCE_TYPE_ACTIVITY: RewardSourceType

class PlayerRating(_message.Message):
    __slots__ = ("rating_pool", "mmr")
    RATING_POOL_FIELD_NUMBER: _ClassVar[int]
    MMR_FIELD_NUMBER: _ClassVar[int]
    rating_pool: str
    mmr: int
    def __init__(self, rating_pool: _Optional[str] = ..., mmr: _Optional[int] = ...) -> None: ...

class PlayerProfile(_message.Message):
    __slots__ = ("player_id", "nickname", "level", "mmr", "avatar", "created_at_ms", "last_seen_ms", "total_battles", "total_wins", "exp_in_level", "is_max_level", "ratings")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    NICKNAME_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    MMR_FIELD_NUMBER: _ClassVar[int]
    AVATAR_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    LAST_SEEN_MS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_BATTLES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_WINS_FIELD_NUMBER: _ClassVar[int]
    EXP_IN_LEVEL_FIELD_NUMBER: _ClassVar[int]
    IS_MAX_LEVEL_FIELD_NUMBER: _ClassVar[int]
    RATINGS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    nickname: str
    level: int
    mmr: int
    avatar: str
    created_at_ms: int
    last_seen_ms: int
    total_battles: int
    total_wins: int
    exp_in_level: int
    is_max_level: bool
    ratings: _containers.RepeatedCompositeFieldContainer[PlayerRating]
    def __init__(self, player_id: _Optional[int] = ..., nickname: _Optional[str] = ..., level: _Optional[int] = ..., mmr: _Optional[int] = ..., avatar: _Optional[str] = ..., created_at_ms: _Optional[int] = ..., last_seen_ms: _Optional[int] = ..., total_battles: _Optional[int] = ..., total_wins: _Optional[int] = ..., exp_in_level: _Optional[int] = ..., is_max_level: bool = ..., ratings: _Optional[_Iterable[_Union[PlayerRating, _Mapping]]] = ...) -> None: ...

class GetProfileRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class GetProfileResponse(_message.Message):
    __slots__ = ("code", "profile")
    CODE_FIELD_NUMBER: _ClassVar[int]
    PROFILE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    profile: PlayerProfile
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., profile: _Optional[_Union[PlayerProfile, _Mapping]] = ...) -> None: ...

class UpdateNicknameRequest(_message.Message):
    __slots__ = ("player_id", "nickname")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    NICKNAME_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    nickname: str
    def __init__(self, player_id: _Optional[int] = ..., nickname: _Optional[str] = ...) -> None: ...

class UpdateNicknameResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class EnsureProfileRequest(_message.Message):
    __slots__ = ("player_id", "nickname")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    NICKNAME_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    nickname: str
    def __init__(self, player_id: _Optional[int] = ..., nickname: _Optional[str] = ...) -> None: ...

class PlayerName(_message.Message):
    __slots__ = ("player_id", "nickname")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    NICKNAME_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    nickname: str
    def __init__(self, player_id: _Optional[int] = ..., nickname: _Optional[str] = ...) -> None: ...

class GetPlayerNamesRequest(_message.Message):
    __slots__ = ("player_ids",)
    PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    player_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, player_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class GetPlayerNamesResponse(_message.Message):
    __slots__ = ("code", "names")
    CODE_FIELD_NUMBER: _ClassVar[int]
    NAMES_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    names: _containers.RepeatedCompositeFieldContainer[PlayerName]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., names: _Optional[_Iterable[_Union[PlayerName, _Mapping]]] = ...) -> None: ...

class EnsureProfileResponse(_message.Message):
    __slots__ = ("code", "created", "effective_nickname", "level")
    CODE_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    EFFECTIVE_NICKNAME_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    created: bool
    effective_nickname: str
    level: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., created: bool = ..., effective_nickname: _Optional[str] = ..., level: _Optional[int] = ...) -> None: ...

class ListHeroesRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class ListHeroesResponse(_message.Message):
    __slots__ = ("code", "hero_ids")
    CODE_FIELD_NUMBER: _ClassVar[int]
    HERO_IDS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    hero_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., hero_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class UnlockHeroRequest(_message.Message):
    __slots__ = ("player_id", "hero_id", "source")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    HERO_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    hero_id: int
    source: str
    def __init__(self, player_id: _Optional[int] = ..., hero_id: _Optional[int] = ..., source: _Optional[str] = ...) -> None: ...

class UnlockHeroResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class GetMMRRequest(_message.Message):
    __slots__ = ("player_id", "rating_pool")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    RATING_POOL_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    rating_pool: str
    def __init__(self, player_id: _Optional[int] = ..., rating_pool: _Optional[str] = ...) -> None: ...

class GetMMRResponse(_message.Message):
    __slots__ = ("code", "mmr", "rating_pool", "found")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MMR_FIELD_NUMBER: _ClassVar[int]
    RATING_POOL_FIELD_NUMBER: _ClassVar[int]
    FOUND_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    mmr: int
    rating_pool: str
    found: bool
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., mmr: _Optional[int] = ..., rating_pool: _Optional[str] = ..., found: bool = ...) -> None: ...

class UpdateMMRRequest(_message.Message):
    __slots__ = ("player_id", "delta", "reason", "idempotency_key", "rating_pool")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    DELTA_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    RATING_POOL_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    delta: int
    reason: str
    idempotency_key: str
    rating_pool: str
    def __init__(self, player_id: _Optional[int] = ..., delta: _Optional[int] = ..., reason: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., rating_pool: _Optional[str] = ...) -> None: ...

class UpdateMMRResponse(_message.Message):
    __slots__ = ("code", "new_mmr")
    CODE_FIELD_NUMBER: _ClassVar[int]
    NEW_MMR_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    new_mmr: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., new_mmr: _Optional[int] = ...) -> None: ...

class PlayerUpdateEvent(_message.Message):
    __slots__ = ("player_id", "match_id", "mmr_delta", "reason", "ts_ms", "rating_pool")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    MMR_DELTA_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    RATING_POOL_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    match_id: int
    mmr_delta: int
    reason: str
    ts_ms: int
    rating_pool: str
    def __init__(self, player_id: _Optional[int] = ..., match_id: _Optional[int] = ..., mmr_delta: _Optional[int] = ..., reason: _Optional[str] = ..., ts_ms: _Optional[int] = ..., rating_pool: _Optional[str] = ...) -> None: ...

class PlayerExperienceEvent(_message.Message):
    __slots__ = ("player_id", "level", "exp_in_level", "is_max_level", "levels_gained", "ts_ms")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    EXP_IN_LEVEL_FIELD_NUMBER: _ClassVar[int]
    IS_MAX_LEVEL_FIELD_NUMBER: _ClassVar[int]
    LEVELS_GAINED_FIELD_NUMBER: _ClassVar[int]
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    level: int
    exp_in_level: int
    is_max_level: bool
    levels_gained: int
    ts_ms: int
    def __init__(self, player_id: _Optional[int] = ..., level: _Optional[int] = ..., exp_in_level: _Optional[int] = ..., is_max_level: bool = ..., levels_gained: _Optional[int] = ..., ts_ms: _Optional[int] = ...) -> None: ...

class AttributeAllocation(_message.Message):
    __slots__ = ("attr_key", "points")
    ATTR_KEY_FIELD_NUMBER: _ClassVar[int]
    POINTS_FIELD_NUMBER: _ClassVar[int]
    attr_key: str
    points: int
    def __init__(self, attr_key: _Optional[str] = ..., points: _Optional[int] = ...) -> None: ...

class SelectHeroRequest(_message.Message):
    __slots__ = ("player_id", "hero_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    HERO_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    hero_id: int
    def __init__(self, player_id: _Optional[int] = ..., hero_id: _Optional[int] = ...) -> None: ...

class SelectHeroResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class GetActiveHeroRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class GetActiveHeroResponse(_message.Message):
    __slots__ = ("code", "hero_id")
    CODE_FIELD_NUMBER: _ClassVar[int]
    HERO_ID_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    hero_id: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., hero_id: _Optional[int] = ...) -> None: ...

class GrantAttributePointsRequest(_message.Message):
    __slots__ = ("player_id", "points", "idempotency_key")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    POINTS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    points: int
    idempotency_key: str
    def __init__(self, player_id: _Optional[int] = ..., points: _Optional[int] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class GrantAttributePointsResponse(_message.Message):
    __slots__ = ("code", "unspent_points")
    CODE_FIELD_NUMBER: _ClassVar[int]
    UNSPENT_POINTS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    unspent_points: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., unspent_points: _Optional[int] = ...) -> None: ...

class AllocateAttributePointsRequest(_message.Message):
    __slots__ = ("player_id", "allocations")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOCATIONS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    allocations: _containers.RepeatedCompositeFieldContainer[AttributeAllocation]
    def __init__(self, player_id: _Optional[int] = ..., allocations: _Optional[_Iterable[_Union[AttributeAllocation, _Mapping]]] = ...) -> None: ...

class AllocateAttributePointsResponse(_message.Message):
    __slots__ = ("code", "unspent_points")
    CODE_FIELD_NUMBER: _ClassVar[int]
    UNSPENT_POINTS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    unspent_points: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., unspent_points: _Optional[int] = ...) -> None: ...

class ResetAttributesRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class ResetAttributesResponse(_message.Message):
    __slots__ = ("code", "unspent_points")
    CODE_FIELD_NUMBER: _ClassVar[int]
    UNSPENT_POINTS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    unspent_points: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., unspent_points: _Optional[int] = ...) -> None: ...

class GetAttributesRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class GetAttributesResponse(_message.Message):
    __slots__ = ("code", "attributes", "unspent_points")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    UNSPENT_POINTS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    attributes: _containers.RepeatedCompositeFieldContainer[AttributeAllocation]
    unspent_points: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., attributes: _Optional[_Iterable[_Union[AttributeAllocation, _Mapping]]] = ..., unspent_points: _Optional[int] = ...) -> None: ...

class LoadoutEquipment(_message.Message):
    __slots__ = ("slot", "item_config_id", "instance_id", "identified", "attributes")
    SLOT_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    IDENTIFIED_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    slot: int
    item_config_id: int
    instance_id: int
    identified: bool
    attributes: _containers.RepeatedCompositeFieldContainer[_inventory_pb2.ItemAttribute]
    def __init__(self, slot: _Optional[int] = ..., item_config_id: _Optional[int] = ..., instance_id: _Optional[int] = ..., identified: bool = ..., attributes: _Optional[_Iterable[_Union[_inventory_pb2.ItemAttribute, _Mapping]]] = ...) -> None: ...

class TalentNode(_message.Message):
    __slots__ = ("talent_id", "level")
    TALENT_ID_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    talent_id: int
    level: int
    def __init__(self, talent_id: _Optional[int] = ..., level: _Optional[int] = ...) -> None: ...

class SetEquipmentRequest(_message.Message):
    __slots__ = ("player_id", "equipment")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    EQUIPMENT_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    equipment: _containers.RepeatedCompositeFieldContainer[LoadoutEquipment]
    def __init__(self, player_id: _Optional[int] = ..., equipment: _Optional[_Iterable[_Union[LoadoutEquipment, _Mapping]]] = ...) -> None: ...

class SetEquipmentResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class GetEquipmentRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class GetEquipmentResponse(_message.Message):
    __slots__ = ("code", "equipment")
    CODE_FIELD_NUMBER: _ClassVar[int]
    EQUIPMENT_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    equipment: _containers.RepeatedCompositeFieldContainer[LoadoutEquipment]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., equipment: _Optional[_Iterable[_Union[LoadoutEquipment, _Mapping]]] = ...) -> None: ...

class GrantTalentPointsRequest(_message.Message):
    __slots__ = ("player_id", "points", "idempotency_key")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    POINTS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    points: int
    idempotency_key: str
    def __init__(self, player_id: _Optional[int] = ..., points: _Optional[int] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class GrantTalentPointsResponse(_message.Message):
    __slots__ = ("code", "unspent_points")
    CODE_FIELD_NUMBER: _ClassVar[int]
    UNSPENT_POINTS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    unspent_points: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., unspent_points: _Optional[int] = ...) -> None: ...

class SetTalentsRequest(_message.Message):
    __slots__ = ("player_id", "talents")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    TALENTS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    talents: _containers.RepeatedCompositeFieldContainer[TalentNode]
    def __init__(self, player_id: _Optional[int] = ..., talents: _Optional[_Iterable[_Union[TalentNode, _Mapping]]] = ...) -> None: ...

class SetTalentsResponse(_message.Message):
    __slots__ = ("code", "unspent_points")
    CODE_FIELD_NUMBER: _ClassVar[int]
    UNSPENT_POINTS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    unspent_points: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., unspent_points: _Optional[int] = ...) -> None: ...

class ResetTalentsRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class ResetTalentsResponse(_message.Message):
    __slots__ = ("code", "unspent_points")
    CODE_FIELD_NUMBER: _ClassVar[int]
    UNSPENT_POINTS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    unspent_points: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., unspent_points: _Optional[int] = ...) -> None: ...

class GetTalentsRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class GetTalentsResponse(_message.Message):
    __slots__ = ("code", "talents", "unspent_points")
    CODE_FIELD_NUMBER: _ClassVar[int]
    TALENTS_FIELD_NUMBER: _ClassVar[int]
    UNSPENT_POINTS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    talents: _containers.RepeatedCompositeFieldContainer[TalentNode]
    unspent_points: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., talents: _Optional[_Iterable[_Union[TalentNode, _Mapping]]] = ..., unspent_points: _Optional[int] = ...) -> None: ...

class SkillCard(_message.Message):
    __slots__ = ("card_id", "level", "shards")
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    SHARDS_FIELD_NUMBER: _ClassVar[int]
    card_id: int
    level: int
    shards: int
    def __init__(self, card_id: _Optional[int] = ..., level: _Optional[int] = ..., shards: _Optional[int] = ...) -> None: ...

class SkillSlot(_message.Message):
    __slots__ = ("slot", "card_id")
    SLOT_FIELD_NUMBER: _ClassVar[int]
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    slot: int
    card_id: int
    def __init__(self, slot: _Optional[int] = ..., card_id: _Optional[int] = ...) -> None: ...

class GrantSkillCardsRequest(_message.Message):
    __slots__ = ("player_id", "grants", "idempotency_key")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    GRANTS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    grants: _containers.RepeatedCompositeFieldContainer[SkillCardGrant]
    idempotency_key: str
    def __init__(self, player_id: _Optional[int] = ..., grants: _Optional[_Iterable[_Union[SkillCardGrant, _Mapping]]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class SkillCardGrant(_message.Message):
    __slots__ = ("card_id", "shards")
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    SHARDS_FIELD_NUMBER: _ClassVar[int]
    card_id: int
    shards: int
    def __init__(self, card_id: _Optional[int] = ..., shards: _Optional[int] = ...) -> None: ...

class GrantSkillCardsResponse(_message.Message):
    __slots__ = ("code", "cards", "already")
    CODE_FIELD_NUMBER: _ClassVar[int]
    CARDS_FIELD_NUMBER: _ClassVar[int]
    ALREADY_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    cards: _containers.RepeatedCompositeFieldContainer[SkillCard]
    already: bool
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., cards: _Optional[_Iterable[_Union[SkillCard, _Mapping]]] = ..., already: bool = ...) -> None: ...

class UpgradeSkillCardRequest(_message.Message):
    __slots__ = ("player_id", "card_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    card_id: int
    def __init__(self, player_id: _Optional[int] = ..., card_id: _Optional[int] = ...) -> None: ...

class UpgradeSkillCardResponse(_message.Message):
    __slots__ = ("code", "card", "shard_cost")
    CODE_FIELD_NUMBER: _ClassVar[int]
    CARD_FIELD_NUMBER: _ClassVar[int]
    SHARD_COST_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    card: SkillCard
    shard_cost: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., card: _Optional[_Union[SkillCard, _Mapping]] = ..., shard_cost: _Optional[int] = ...) -> None: ...

class SetSkillSlotsRequest(_message.Message):
    __slots__ = ("player_id", "slots")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    SLOTS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    slots: _containers.RepeatedCompositeFieldContainer[SkillSlot]
    def __init__(self, player_id: _Optional[int] = ..., slots: _Optional[_Iterable[_Union[SkillSlot, _Mapping]]] = ...) -> None: ...

class SetSkillSlotsResponse(_message.Message):
    __slots__ = ("code", "slots")
    CODE_FIELD_NUMBER: _ClassVar[int]
    SLOTS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    slots: _containers.RepeatedCompositeFieldContainer[SkillSlot]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., slots: _Optional[_Iterable[_Union[SkillSlot, _Mapping]]] = ...) -> None: ...

class GetSkillCardsRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class GetSkillCardsResponse(_message.Message):
    __slots__ = ("code", "cards", "slots")
    CODE_FIELD_NUMBER: _ClassVar[int]
    CARDS_FIELD_NUMBER: _ClassVar[int]
    SLOTS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    cards: _containers.RepeatedCompositeFieldContainer[SkillCard]
    slots: _containers.RepeatedCompositeFieldContainer[SkillSlot]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., cards: _Optional[_Iterable[_Union[SkillCard, _Mapping]]] = ..., slots: _Optional[_Iterable[_Union[SkillSlot, _Mapping]]] = ...) -> None: ...

class PlayerLoadout(_message.Message):
    __slots__ = ("player_id", "active_hero_id", "attributes", "unspent_attr_points", "equipment", "talents", "unspent_talent_points", "skill_cards")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_HERO_ID_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
    UNSPENT_ATTR_POINTS_FIELD_NUMBER: _ClassVar[int]
    EQUIPMENT_FIELD_NUMBER: _ClassVar[int]
    TALENTS_FIELD_NUMBER: _ClassVar[int]
    UNSPENT_TALENT_POINTS_FIELD_NUMBER: _ClassVar[int]
    SKILL_CARDS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    active_hero_id: int
    attributes: _containers.RepeatedCompositeFieldContainer[AttributeAllocation]
    unspent_attr_points: int
    equipment: _containers.RepeatedCompositeFieldContainer[LoadoutEquipment]
    talents: _containers.RepeatedCompositeFieldContainer[TalentNode]
    unspent_talent_points: int
    skill_cards: _containers.RepeatedCompositeFieldContainer[LoadoutSkillCard]
    def __init__(self, player_id: _Optional[int] = ..., active_hero_id: _Optional[int] = ..., attributes: _Optional[_Iterable[_Union[AttributeAllocation, _Mapping]]] = ..., unspent_attr_points: _Optional[int] = ..., equipment: _Optional[_Iterable[_Union[LoadoutEquipment, _Mapping]]] = ..., talents: _Optional[_Iterable[_Union[TalentNode, _Mapping]]] = ..., unspent_talent_points: _Optional[int] = ..., skill_cards: _Optional[_Iterable[_Union[LoadoutSkillCard, _Mapping]]] = ...) -> None: ...

class LoadoutSkillCard(_message.Message):
    __slots__ = ("slot", "card_id", "level")
    SLOT_FIELD_NUMBER: _ClassVar[int]
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    slot: int
    card_id: int
    level: int
    def __init__(self, slot: _Optional[int] = ..., card_id: _Optional[int] = ..., level: _Optional[int] = ...) -> None: ...

class GetLoadoutRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class GetLoadoutResponse(_message.Message):
    __slots__ = ("code", "loadout")
    CODE_FIELD_NUMBER: _ClassVar[int]
    LOADOUT_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    loadout: PlayerLoadout
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., loadout: _Optional[_Union[PlayerLoadout, _Mapping]] = ...) -> None: ...

class RewardClaimStorageRecord(_message.Message):
    __slots__ = ("permanent", "activity")
    class PermanentEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: bytes
        def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
    class ActivityEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: bytes
        def __init__(self, key: _Optional[int] = ..., value: _Optional[bytes] = ...) -> None: ...
    PERMANENT_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_FIELD_NUMBER: _ClassVar[int]
    permanent: _containers.ScalarMap[str, bytes]
    activity: _containers.ScalarMap[int, bytes]
    def __init__(self, permanent: _Optional[_Mapping[str, bytes]] = ..., activity: _Optional[_Mapping[int, bytes]] = ...) -> None: ...

class ClaimRewardRequest(_message.Message):
    __slots__ = ("player_id", "source_type", "source", "activity_instance_id", "reward_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    REWARD_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    source_type: RewardSourceType
    source: str
    activity_instance_id: int
    reward_id: int
    def __init__(self, player_id: _Optional[int] = ..., source_type: _Optional[_Union[RewardSourceType, str]] = ..., source: _Optional[str] = ..., activity_instance_id: _Optional[int] = ..., reward_id: _Optional[int] = ...) -> None: ...

class ClaimRewardResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class GetRewardClaimsRequest(_message.Message):
    __slots__ = ("player_id", "source_type", "source", "activity_instance_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    source_type: RewardSourceType
    source: str
    activity_instance_id: int
    def __init__(self, player_id: _Optional[int] = ..., source_type: _Optional[_Union[RewardSourceType, str]] = ..., source: _Optional[str] = ..., activity_instance_id: _Optional[int] = ...) -> None: ...

class GetRewardClaimsResponse(_message.Message):
    __slots__ = ("code", "claimed_reward_ids")
    CODE_FIELD_NUMBER: _ClassVar[int]
    CLAIMED_REWARD_IDS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    claimed_reward_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., claimed_reward_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class AddExperienceRequest(_message.Message):
    __slots__ = ("player_id", "exp_delta", "reason", "idempotency_key", "rating_pool")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    EXP_DELTA_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    RATING_POOL_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    exp_delta: int
    reason: str
    idempotency_key: str
    rating_pool: str
    def __init__(self, player_id: _Optional[int] = ..., exp_delta: _Optional[int] = ..., reason: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., rating_pool: _Optional[str] = ...) -> None: ...

class AddExperienceResponse(_message.Message):
    __slots__ = ("code", "level", "exp_in_level", "is_max_level", "levels_gained", "already")
    CODE_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    EXP_IN_LEVEL_FIELD_NUMBER: _ClassVar[int]
    IS_MAX_LEVEL_FIELD_NUMBER: _ClassVar[int]
    LEVELS_GAINED_FIELD_NUMBER: _ClassVar[int]
    ALREADY_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    level: int
    exp_in_level: int
    is_max_level: bool
    levels_gained: int
    already: bool
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., level: _Optional[int] = ..., exp_in_level: _Optional[int] = ..., is_max_level: bool = ..., levels_gained: _Optional[int] = ..., already: bool = ...) -> None: ...
