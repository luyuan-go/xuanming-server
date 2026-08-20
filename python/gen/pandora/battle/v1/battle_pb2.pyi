from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BattleOutcome(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BATTLE_OUTCOME_UNSPECIFIED: _ClassVar[BattleOutcome]
    BATTLE_OUTCOME_NORMAL: _ClassVar[BattleOutcome]
    BATTLE_OUTCOME_ABANDONED: _ClassVar[BattleOutcome]
BATTLE_OUTCOME_UNSPECIFIED: BattleOutcome
BATTLE_OUTCOME_NORMAL: BattleOutcome
BATTLE_OUTCOME_ABANDONED: BattleOutcome

class PlayerStats(_message.Message):
    __slots__ = ("player_id", "hero_id", "team", "kills", "deaths", "assists", "damage_dealt", "damage_taken", "healing", "gold", "mmr_delta", "dropped_item_config_ids")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    HERO_ID_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    KILLS_FIELD_NUMBER: _ClassVar[int]
    DEATHS_FIELD_NUMBER: _ClassVar[int]
    ASSISTS_FIELD_NUMBER: _ClassVar[int]
    DAMAGE_DEALT_FIELD_NUMBER: _ClassVar[int]
    DAMAGE_TAKEN_FIELD_NUMBER: _ClassVar[int]
    HEALING_FIELD_NUMBER: _ClassVar[int]
    GOLD_FIELD_NUMBER: _ClassVar[int]
    MMR_DELTA_FIELD_NUMBER: _ClassVar[int]
    DROPPED_ITEM_CONFIG_IDS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    hero_id: int
    team: int
    kills: int
    deaths: int
    assists: int
    damage_dealt: int
    damage_taken: int
    healing: int
    gold: int
    mmr_delta: int
    dropped_item_config_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, player_id: _Optional[int] = ..., hero_id: _Optional[int] = ..., team: _Optional[int] = ..., kills: _Optional[int] = ..., deaths: _Optional[int] = ..., assists: _Optional[int] = ..., damage_dealt: _Optional[int] = ..., damage_taken: _Optional[int] = ..., healing: _Optional[int] = ..., gold: _Optional[int] = ..., mmr_delta: _Optional[int] = ..., dropped_item_config_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class BattleResult(_message.Message):
    __slots__ = ("match_id", "started_at_ms", "ended_at_ms", "winner_team", "stats", "ds_pod_name", "game_mode", "map_id", "outcome")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    ENDED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    WINNER_TEAM_FIELD_NUMBER: _ClassVar[int]
    STATS_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    GAME_MODE_FIELD_NUMBER: _ClassVar[int]
    MAP_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    started_at_ms: int
    ended_at_ms: int
    winner_team: int
    stats: _containers.RepeatedCompositeFieldContainer[PlayerStats]
    ds_pod_name: str
    game_mode: str
    map_id: int
    outcome: BattleOutcome
    def __init__(self, match_id: _Optional[int] = ..., started_at_ms: _Optional[int] = ..., ended_at_ms: _Optional[int] = ..., winner_team: _Optional[int] = ..., stats: _Optional[_Iterable[_Union[PlayerStats, _Mapping]]] = ..., ds_pod_name: _Optional[str] = ..., game_mode: _Optional[str] = ..., map_id: _Optional[int] = ..., outcome: _Optional[_Union[BattleOutcome, str]] = ...) -> None: ...

class ReportResultRequest(_message.Message):
    __slots__ = ("result", "final_progress_seq")
    RESULT_FIELD_NUMBER: _ClassVar[int]
    FINAL_PROGRESS_SEQ_FIELD_NUMBER: _ClassVar[int]
    result: BattleResult
    final_progress_seq: int
    def __init__(self, result: _Optional[_Union[BattleResult, _Mapping]] = ..., final_progress_seq: _Optional[int] = ...) -> None: ...

class ReportResultResponse(_message.Message):
    __slots__ = ("code", "already_recorded")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ALREADY_RECORDED_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    already_recorded: bool
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., already_recorded: bool = ...) -> None: ...

class GetMatchResultRequest(_message.Message):
    __slots__ = ("match_id",)
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    def __init__(self, match_id: _Optional[int] = ...) -> None: ...

class GetMatchResultResponse(_message.Message):
    __slots__ = ("code", "result")
    CODE_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    result: BattleResult
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., result: _Optional[_Union[BattleResult, _Mapping]] = ...) -> None: ...

class ListPlayerHistoryRequest(_message.Message):
    __slots__ = ("player_id", "limit", "before_ms")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    BEFORE_MS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    limit: int
    before_ms: int
    def __init__(self, player_id: _Optional[int] = ..., limit: _Optional[int] = ..., before_ms: _Optional[int] = ...) -> None: ...

class ListPlayerHistoryResponse(_message.Message):
    __slots__ = ("code", "results")
    CODE_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    results: _containers.RepeatedCompositeFieldContainer[BattleResult]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., results: _Optional[_Iterable[_Union[BattleResult, _Mapping]]] = ...) -> None: ...

class MonsterKillFact(_message.Message):
    __slots__ = ("monster_config_id", "count", "share_permille", "monster_level")
    MONSTER_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    SHARE_PERMILLE_FIELD_NUMBER: _ClassVar[int]
    MONSTER_LEVEL_FIELD_NUMBER: _ClassVar[int]
    monster_config_id: int
    count: int
    share_permille: int
    monster_level: int
    def __init__(self, monster_config_id: _Optional[int] = ..., count: _Optional[int] = ..., share_permille: _Optional[int] = ..., monster_level: _Optional[int] = ...) -> None: ...

class ItemPickupFact(_message.Message):
    __slots__ = ("item_config_id", "count")
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    item_config_id: int
    count: int
    def __init__(self, item_config_id: _Optional[int] = ..., count: _Optional[int] = ...) -> None: ...

class ItemConsumeFact(_message.Message):
    __slots__ = ("item_config_id", "count")
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    item_config_id: int
    count: int
    def __init__(self, item_config_id: _Optional[int] = ..., count: _Optional[int] = ...) -> None: ...

class ItemDiscardFact(_message.Message):
    __slots__ = ("item_config_id", "count")
    ITEM_CONFIG_ID_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    item_config_id: int
    count: int
    def __init__(self, item_config_id: _Optional[int] = ..., count: _Optional[int] = ...) -> None: ...

class BattleProgressEvent(_message.Message):
    __slots__ = ("seq", "player_id", "monster_kill", "item_pickup", "item_consume", "item_discard", "ts_ms")
    SEQ_FIELD_NUMBER: _ClassVar[int]
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    MONSTER_KILL_FIELD_NUMBER: _ClassVar[int]
    ITEM_PICKUP_FIELD_NUMBER: _ClassVar[int]
    ITEM_CONSUME_FIELD_NUMBER: _ClassVar[int]
    ITEM_DISCARD_FIELD_NUMBER: _ClassVar[int]
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    seq: int
    player_id: int
    monster_kill: MonsterKillFact
    item_pickup: ItemPickupFact
    item_consume: ItemConsumeFact
    item_discard: ItemDiscardFact
    ts_ms: int
    def __init__(self, seq: _Optional[int] = ..., player_id: _Optional[int] = ..., monster_kill: _Optional[_Union[MonsterKillFact, _Mapping]] = ..., item_pickup: _Optional[_Union[ItemPickupFact, _Mapping]] = ..., item_consume: _Optional[_Union[ItemConsumeFact, _Mapping]] = ..., item_discard: _Optional[_Union[ItemDiscardFact, _Mapping]] = ..., ts_ms: _Optional[int] = ...) -> None: ...

class ReportProgressRequest(_message.Message):
    __slots__ = ("match_id", "events")
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    match_id: int
    events: _containers.RepeatedCompositeFieldContainer[BattleProgressEvent]
    def __init__(self, match_id: _Optional[int] = ..., events: _Optional[_Iterable[_Union[BattleProgressEvent, _Mapping]]] = ...) -> None: ...

class ReportProgressResponse(_message.Message):
    __slots__ = ("code", "acked_seq")
    CODE_FIELD_NUMBER: _ClassVar[int]
    ACKED_SEQ_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    acked_seq: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., acked_seq: _Optional[int] = ...) -> None: ...
