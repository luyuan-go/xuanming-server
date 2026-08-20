from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LeaderboardScope(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LEADERBOARD_SCOPE_UNSPECIFIED: _ClassVar[LeaderboardScope]
    LEADERBOARD_SCOPE_GLOBAL: _ClassVar[LeaderboardScope]
    LEADERBOARD_SCOPE_GUILD: _ClassVar[LeaderboardScope]
    LEADERBOARD_SCOPE_INSTANCE: _ClassVar[LeaderboardScope]
    LEADERBOARD_SCOPE_CUSTOM: _ClassVar[LeaderboardScope]

class SubmitMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SUBMIT_MODE_UNSPECIFIED: _ClassVar[SubmitMode]
    SUBMIT_MODE_SET_IF_HIGHER: _ClassVar[SubmitMode]
    SUBMIT_MODE_SET: _ClassVar[SubmitMode]
    SUBMIT_MODE_INCREMENT: _ClassVar[SubmitMode]
LEADERBOARD_SCOPE_UNSPECIFIED: LeaderboardScope
LEADERBOARD_SCOPE_GLOBAL: LeaderboardScope
LEADERBOARD_SCOPE_GUILD: LeaderboardScope
LEADERBOARD_SCOPE_INSTANCE: LeaderboardScope
LEADERBOARD_SCOPE_CUSTOM: LeaderboardScope
SUBMIT_MODE_UNSPECIFIED: SubmitMode
SUBMIT_MODE_SET_IF_HIGHER: SubmitMode
SUBMIT_MODE_SET: SubmitMode
SUBMIT_MODE_INCREMENT: SubmitMode

class BoardKey(_message.Message):
    __slots__ = ("board_type", "scope", "scope_id", "period")
    BOARD_TYPE_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    SCOPE_ID_FIELD_NUMBER: _ClassVar[int]
    PERIOD_FIELD_NUMBER: _ClassVar[int]
    board_type: int
    scope: LeaderboardScope
    scope_id: int
    period: str
    def __init__(self, board_type: _Optional[int] = ..., scope: _Optional[_Union[LeaderboardScope, str]] = ..., scope_id: _Optional[int] = ..., period: _Optional[str] = ...) -> None: ...

class BoardOptions(_message.Message):
    __slots__ = ("ttl_seconds", "max_size", "tie_break_by_time", "ascending", "estimate_bucket_width")
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    MAX_SIZE_FIELD_NUMBER: _ClassVar[int]
    TIE_BREAK_BY_TIME_FIELD_NUMBER: _ClassVar[int]
    ASCENDING_FIELD_NUMBER: _ClassVar[int]
    ESTIMATE_BUCKET_WIDTH_FIELD_NUMBER: _ClassVar[int]
    ttl_seconds: int
    max_size: int
    tie_break_by_time: bool
    ascending: bool
    estimate_bucket_width: int
    def __init__(self, ttl_seconds: _Optional[int] = ..., max_size: _Optional[int] = ..., tie_break_by_time: bool = ..., ascending: bool = ..., estimate_bucket_width: _Optional[int] = ...) -> None: ...

class LeaderboardEntry(_message.Message):
    __slots__ = ("entity_id", "score", "rank", "updated_at_ms")
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    RANK_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    entity_id: int
    score: int
    rank: int
    updated_at_ms: int
    def __init__(self, entity_id: _Optional[int] = ..., score: _Optional[int] = ..., rank: _Optional[int] = ..., updated_at_ms: _Optional[int] = ...) -> None: ...

class RewardItem(_message.Message):
    __slots__ = ("item_config_id", "count")
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    item_config_id: int
    count: int
    def __init__(self, item_config_id: _Optional[int] = ..., count: _Optional[int] = ...) -> None: ...

class RewardTier(_message.Message):
    __slots__ = ("rank_from", "rank_to", "items")
    RANK_FROM_FIELD_NUMBER: _ClassVar[int]
    RANK_TO_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    rank_from: int
    rank_to: int
    items: _containers.RepeatedCompositeFieldContainer[RewardItem]
    def __init__(self, rank_from: _Optional[int] = ..., rank_to: _Optional[int] = ..., items: _Optional[_Iterable[_Union[RewardItem, _Mapping]]] = ...) -> None: ...

class RewardTable(_message.Message):
    __slots__ = ("tiers",)
    TIERS_FIELD_NUMBER: _ClassVar[int]
    tiers: _containers.RepeatedCompositeFieldContainer[RewardTier]
    def __init__(self, tiers: _Optional[_Iterable[_Union[RewardTier, _Mapping]]] = ...) -> None: ...

class RewardGrantStorageRecord(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[RewardItem]
    def __init__(self, items: _Optional[_Iterable[_Union[RewardItem, _Mapping]]] = ...) -> None: ...

class LeaderboardSettleEvent(_message.Message):
    __slots__ = ("settlement_id", "board", "winners", "settled_at_ms")
    SETTLEMENT_ID_FIELD_NUMBER: _ClassVar[int]
    BOARD_FIELD_NUMBER: _ClassVar[int]
    WINNERS_FIELD_NUMBER: _ClassVar[int]
    SETTLED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    settlement_id: int
    board: BoardKey
    winners: _containers.RepeatedCompositeFieldContainer[LeaderboardEntry]
    settled_at_ms: int
    def __init__(self, settlement_id: _Optional[int] = ..., board: _Optional[_Union[BoardKey, _Mapping]] = ..., winners: _Optional[_Iterable[_Union[LeaderboardEntry, _Mapping]]] = ..., settled_at_ms: _Optional[int] = ...) -> None: ...

class SubmitScoreRequest(_message.Message):
    __slots__ = ("board", "entity_id", "score", "mode", "options")
    BOARD_FIELD_NUMBER: _ClassVar[int]
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    board: BoardKey
    entity_id: int
    score: int
    mode: SubmitMode
    options: BoardOptions
    def __init__(self, board: _Optional[_Union[BoardKey, _Mapping]] = ..., entity_id: _Optional[int] = ..., score: _Optional[int] = ..., mode: _Optional[_Union[SubmitMode, str]] = ..., options: _Optional[_Union[BoardOptions, _Mapping]] = ...) -> None: ...

class SubmitScoreResponse(_message.Message):
    __slots__ = ("code", "new_score", "rank")
    CODE_FIELD_NUMBER: _ClassVar[int]
    NEW_SCORE_FIELD_NUMBER: _ClassVar[int]
    RANK_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    new_score: int
    rank: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., new_score: _Optional[int] = ..., rank: _Optional[int] = ...) -> None: ...

class GetRankRequest(_message.Message):
    __slots__ = ("board", "entity_id")
    BOARD_FIELD_NUMBER: _ClassVar[int]
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    board: BoardKey
    entity_id: int
    def __init__(self, board: _Optional[_Union[BoardKey, _Mapping]] = ..., entity_id: _Optional[int] = ...) -> None: ...

class GetRankResponse(_message.Message):
    __slots__ = ("code", "found", "entry", "estimated", "total_submitters")
    CODE_FIELD_NUMBER: _ClassVar[int]
    FOUND_FIELD_NUMBER: _ClassVar[int]
    ENTRY_FIELD_NUMBER: _ClassVar[int]
    ESTIMATED_FIELD_NUMBER: _ClassVar[int]
    TOTAL_SUBMITTERS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    found: bool
    entry: LeaderboardEntry
    estimated: bool
    total_submitters: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., found: bool = ..., entry: _Optional[_Union[LeaderboardEntry, _Mapping]] = ..., estimated: bool = ..., total_submitters: _Optional[int] = ...) -> None: ...

class GetRangeRequest(_message.Message):
    __slots__ = ("board", "offset", "limit")
    BOARD_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    board: BoardKey
    offset: int
    limit: int
    def __init__(self, board: _Optional[_Union[BoardKey, _Mapping]] = ..., offset: _Optional[int] = ..., limit: _Optional[int] = ...) -> None: ...

class GetRangeResponse(_message.Message):
    __slots__ = ("code", "entries", "total")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    entries: _containers.RepeatedCompositeFieldContainer[LeaderboardEntry]
    total: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., entries: _Optional[_Iterable[_Union[LeaderboardEntry, _Mapping]]] = ..., total: _Optional[int] = ...) -> None: ...

class GetAroundRequest(_message.Message):
    __slots__ = ("board", "entity_id", "radius")
    BOARD_FIELD_NUMBER: _ClassVar[int]
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    RADIUS_FIELD_NUMBER: _ClassVar[int]
    board: BoardKey
    entity_id: int
    radius: int
    def __init__(self, board: _Optional[_Union[BoardKey, _Mapping]] = ..., entity_id: _Optional[int] = ..., radius: _Optional[int] = ...) -> None: ...

class GetAroundResponse(_message.Message):
    __slots__ = ("code", "entries", "found")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    FOUND_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    entries: _containers.RepeatedCompositeFieldContainer[LeaderboardEntry]
    found: bool
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., entries: _Optional[_Iterable[_Union[LeaderboardEntry, _Mapping]]] = ..., found: bool = ...) -> None: ...

class RemoveEntryRequest(_message.Message):
    __slots__ = ("board", "entity_id")
    BOARD_FIELD_NUMBER: _ClassVar[int]
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    board: BoardKey
    entity_id: int
    def __init__(self, board: _Optional[_Union[BoardKey, _Mapping]] = ..., entity_id: _Optional[int] = ...) -> None: ...

class RemoveEntryResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class SettleBoardRequest(_message.Message):
    __slots__ = ("board", "top_n", "reward_table", "reset_after", "settle_idempotency_key")
    BOARD_FIELD_NUMBER: _ClassVar[int]
    TOP_N_FIELD_NUMBER: _ClassVar[int]
    REWARD_TABLE_FIELD_NUMBER: _ClassVar[int]
    RESET_AFTER_FIELD_NUMBER: _ClassVar[int]
    SETTLE_IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    board: BoardKey
    top_n: int
    reward_table: RewardTable
    reset_after: bool
    settle_idempotency_key: str
    def __init__(self, board: _Optional[_Union[BoardKey, _Mapping]] = ..., top_n: _Optional[int] = ..., reward_table: _Optional[_Union[RewardTable, _Mapping]] = ..., reset_after: bool = ..., settle_idempotency_key: _Optional[str] = ...) -> None: ...

class SettleBoardResponse(_message.Message):
    __slots__ = ("code", "settlement_id", "settled_count", "already_settled", "winners")
    CODE_FIELD_NUMBER: _ClassVar[int]
    SETTLEMENT_ID_FIELD_NUMBER: _ClassVar[int]
    SETTLED_COUNT_FIELD_NUMBER: _ClassVar[int]
    ALREADY_SETTLED_FIELD_NUMBER: _ClassVar[int]
    WINNERS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    settlement_id: int
    settled_count: int
    already_settled: bool
    winners: _containers.RepeatedCompositeFieldContainer[LeaderboardEntry]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., settlement_id: _Optional[int] = ..., settled_count: _Optional[int] = ..., already_settled: bool = ..., winners: _Optional[_Iterable[_Union[LeaderboardEntry, _Mapping]]] = ...) -> None: ...

class DeleteBoardRequest(_message.Message):
    __slots__ = ("board",)
    BOARD_FIELD_NUMBER: _ClassVar[int]
    board: BoardKey
    def __init__(self, board: _Optional[_Union[BoardKey, _Mapping]] = ...) -> None: ...

class DeleteBoardResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...
