"""ds_allocator —— 玩家离场 / 对局中止 / 生命周期证明。

忠实移植 Go 侧三个文件(它们在 Go 里同属 `internal/data` 包,共享未导出符号,
因此这里合成一个 Python 模块):

    services/battle/ds_allocator/internal/data/battle_departure.go        (702 行)
    services/battle/ds_allocator/internal/data/battle_abort.go            (540 行)
    services/battle/ds_allocator/internal/data/battle_lifecycle_proof.go  (220 行)

────────────────────────────────────────────────────────────────────────────────
★ 这三条链路守的是同一条不变量:**玩家同一时刻只能在一个可操作 DS**(§9 不变量 1)

  Battle→Hub 回流时,Hub 必须拿到「旧 Battle DS 已经放手」的**物理证明**才敢让
  玩家进新场景。本模块提供且只提供两种可接受的物理证明:

    ① 驱逐单被 credential-bound Battle DS 的**完整 census 快照**证明缺席
       (DEPARTED);
    ② 外部 GameServer UID 条件回收明确成功后写下的 teardown proof
       (SOURCE_TORN_DOWN)。

  **刻意不使用的"证明"**(每一条都是 §9.22 点名禁止的假权威):
    - battle key 缺失 / TTL 过期   —— 只说明镜像不在了,不说明 DS 放手了;
    - 心跳超时                      —— 只说明我们看不见它,不说明它停了;
    - locator key miss             —— presence 投影的 miss 不证明玩家已离开旧 DS;
    - 单独的 teardown proof        —— 见 abort 链:必须与 lifecycle ACK **合取**。

  所以本模块里几乎每一个 early return 都是 **fail-closed**:证据不足时返回可重试
  错误,而不是"看起来没问题就放行"。放行一次的代价是双 DS,而双 DS 不报错 ——
  两台 DS 会各自认为自己有权写这个玩家。

────────────────────────────────────────────────────────────────────────────────
★ Redis key 与 slot

    journal    pandora:ds:departures:{match_id}
    teardown   pandora:ds:teardown:{match_id}:<sha256[:16] of exact instance tuple>
    abort      pandora:ds:allocation-abort:{match_id}
    lifecycle  pandora:ds:allocation-lifecycle-published:{match_id}
    battle     pandora:ds:battle:{match_id}          (repo.battle_key)
    auth       pandora:ds:auth:{match_id}            (battle_auth.battle_auth_key)

  六个 key 的 `{...}` 里都是 **match_id**,因此 Redis Cluster 下落在同一个 slot,
  WATCH/MULTI/EXEC 才是**真事务**。hashtag 的位置不是排版,是正确性:把花括号
  挪到别处(哪怕只是 `pandora:ds:{departures}:%d`)会让这些 key 散到不同 slot,
  MULTI 直接报 CROSSSLOT —— 那还算好的;更糟的是 WATCH 分散后**乐观锁失效但不报错**。

★ Go 的 `rdb.Watch(ctx, fn, keys...)` → Python 的
  `async with rdb.pipeline(transaction=True) as pipe: await pipe.watch(...)`。
  go-redis 在 EXEC 返回 nil(WATCH 被打断)时给 `redis.TxFailedErr`,
  redis-py 抛 `WatchError` —— 两者都由外层循环 `continue` 重试。
  ★ Go 侧这三个文件的重试循环**没有退避**(裸 `continue`),这里照抄;
    不引入 `battle_auth._cas_backoff`,否则同一段代码两栈的重试时序不同,
    压测出来的 CAS 冲突率没法互相印证。

★ 与 Go 的已知差异(逐条都是"Go 由类型系统免费获得、Python 必须显式补"):
  - uint32/uint64/int64 边界:Python int 无限精度,越界值会一路走到 protobuf
    序列化才炸,那时已经写了一半。这里在**进入事务前**显式判。
  - `PTTL` 的三态:go-redis 把 `-1` 编码成 `-time.Nanosecond`、`-2` 成
    `-2 * time.Nanosecond`(Duration 语义),Go 代码里写作 `ttl == -time.Nanosecond`;
    redis-py 直接返回整数 `-1` / `-2`。语义一一对应,见
    `_teardown_proof_remaining_retention`。
  - go-redis 的 `Set(key, val, dur)` 用 **PX 毫秒**、`Expire(key, dur)` 用
    **EXPIRE 整秒(formatSec)**。本模块的保留期来自 PTTL(毫秒),因此内部一律
    以**毫秒整数**传递,避免 `ms → 秒(float) → ms` 往返丢 1 毫秒。
  - Go 的多返回 `(value, err)` 在失败路径上仍带值的地方,Python 用异常传播;
    本模块的失败路径调用方都只看 err(Go 侧同样丢弃 result),故无需挂载证据槽位。
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib

from pandora.ds.v1 import allocator_pb2 as dspb
from redis.exceptions import WatchError

from pandorapy import battleabort, errcode, placement
from pandorapy import log as plog
from pandorapy.services.ds_allocator.battle_auth import (
    ACTIVE_KEY,
    BATTLE_AUTH_CAS_RETRIES,
    BATTLE_STATE_ALLOCATION_ABORT_PENDING,
    BattleExpectedInstance,
    _read_bound_authority,
    _set,
    battle_auth_key,
    battle_auth_record_v2_exact,
    battle_result_stable_authority_matches,
    expected_battle_instance_matches,
    now_ms,
    unmarshal_battle_auth,
)
from pandorapy.services.ds_allocator.battle_auth import (
    RedisBattleAuthRepo as _RedisBattleAuthRepo,
)
from pandorapy.services.ds_allocator.repo import (
    _format_sec,
    _require_uint64,
    battle_key,
    clone,
    marshal_battle_transition,
    unmarshal_battle,
    validate_battle_storage_transition,
)
from pandorapy.services.ds_allocator.repo import RedisBattleRepo as _RedisBattleRepo

# ── 常量(逐个与 Go const 同值)──────────────────────────────────────────────

# Go: battleDepartureCASRetries。
BATTLE_DEPARTURE_CAS_RETRIES = 64

# Go: maxBattleDepartureOrders。单场对局的驱逐单硬上限(§9 不变量 18 的写入侧闸)。
MAX_BATTLE_DEPARTURE_ORDERS = 1024

# Go: BattleDepartureTerminalRetention = 7 * 24 * time.Hour。
# ★ 只有「exact UID teardown proof」+「journal 全终态」两者同时成立才会挂上这个
#   保留期。pending / 未知状态的 journal **永久保留** —— TTL 在这里绝不参与
#   授权判定,它只负责给"已经证完的历史"收口(§9.24 只增数据必须有界)。
BATTLE_DEPARTURE_TERMINAL_RETENTION_MS = 7 * 24 * 60 * 60 * 1000

# Go: BattlePlayerCensusCapabilityVersionV1 uint32 = 1。
# capability>=1 的含义是「快照覆盖所有可能持有 admission 的玩家」,而不仅是
# PostLogin ActivePlayers。旧 DS 上报的零值**绝不能**被解释成"全员已离场"。
BATTLE_PLAYER_CENSUS_CAPABILITY_VERSION_V1 = 1

# Go: evictionOrderStalledWarnMs = 30_000。
# 依据:心跳周期 5s,超过 6 拍仍 PENDING 已明显异常(正常 ack 在下一拍);待实测复核。
EVICTION_ORDER_STALLED_WARN_MS = 30_000

_UINT32_MAX = (1 << 32) - 1

# ── 拒绝原因取值(Go 的 const 块,逐字节相同)────────────────────────────────
#
# ★★ 忠实移植说明:Go 侧这 21 个常量与下面的 `departure_rejected()` 助手
#    **一个调用点都没有**(2026 全仓 grep 只命中声明处)。也就是说 Go 的离场
#    拒绝路径当前**不打这条结构化日志**。这里照抄声明而不擅自接线 ——
#    补上日志会让两栈的日志流不同,Loki 上按 reason 聚合时 Python 副本凭空
#    多出一批事件,灰度期直接把人引向错误结论。发现的这处死代码已在交付报告
#    中列出,是否接线由 Go 侧决定,不在移植范围内擅自改语义。

# EnsurePlayerDeparture(Hub 侧申请离场证明)。
DEPARTURE_REJECT_EXPECTED_INCOMPLETE = "departure_expected_incomplete"
DEPARTURE_REJECT_TEARDOWN_TUPLE_CONFLICT = "teardown_proof_tuple_conflict"
DEPARTURE_REJECT_IDEMPOTENCY_CONFLICT = "departure_idempotency_tuple_conflict"
DEPARTURE_REJECT_JOURNAL_FULL = "departure_journal_full"
DEPARTURE_REJECT_SOURCE_MISSING = "battle_source_missing_without_teardown_proof"
DEPARTURE_REJECT_SOURCE_TUPLE_MISMATCH = "battle_source_tuple_mismatch"
DEPARTURE_REJECT_PLAYER_NOT_IN_ROSTER = "player_not_in_authoritative_roster"
DEPARTURE_REJECT_CAS_EXHAUSTED = "departure_cas_retry_exhausted"
# ReconcilePlayerDepartures(DS 心跳上报 census 时对账)。
DEPARTURE_REJECT_HEARTBEAT_SOURCE_INCOMPLETE = "heartbeat_source_incomplete"
DEPARTURE_REJECT_CENSUS_ZERO_PLAYER = "census_contains_zero_player"
DEPARTURE_REJECT_CENSUS_DUPLICATE_PLAYER = "census_contains_duplicate_player"
DEPARTURE_REJECT_ACK_ID_EMPTY = "acknowledged_departure_id_empty"
DEPARTURE_REJECT_ACK_ID_DUPLICATE = "acknowledged_departure_id_duplicate"
DEPARTURE_REJECT_CENSUS_CAPABILITY_TOO_OLD = "census_capability_or_id_missing"
DEPARTURE_REJECT_CENSUS_PAYLOAD_WITHOUT_PRESENT = "census_payload_without_present"
DEPARTURE_REJECT_ACK_BEFORE_ISSUE = "ack_before_order_issued"
DEPARTURE_REJECT_ACK_WHILE_ACTIVE = "ack_while_player_still_active"
DEPARTURE_REJECT_ACK_UNKNOWN_ID = "ack_unknown_departure_id"
DEPARTURE_REJECT_RECONCILE_CAS_EXHAUSTED = "departure_reconcile_cas_retry_exhausted"
# RecordInstanceTeardown(整实例回收证明)。
DEPARTURE_REJECT_TEARDOWN_INCOMPLETE = "teardown_source_incomplete"
DEPARTURE_REJECT_TEARDOWN_CAS_EXHAUSTED = "teardown_proof_cas_retry_exhausted"


def departure_rejected(
    msg: str,
    match_id: int,
    source: BattleDepartureSource,
    reason: str,
    **kv: object,
) -> None:
    """Go: `departureRejected`。统一带齐实例 join key,免得每个 early return 手抄四个字段。

    ★ Go 的 `plog.With(ctx).Warnw(fields...)` 是**纯 kv 变参**(不是 zap 的
      `Warnw(msg, kv...)`),第一对是 `"msg", msg`。Python 侧 `plog` 的
      `EventRenamer("msg")` 把 event 改名成 `msg`,所以位置参数即 Go 的 msg 值,
      字段名逐字对齐。
    """
    plog.get().warning(
        msg,
        reason=reason,
        match_id=match_id,
        ds_pod=source.ds_pod_name,
        ds_uid=source.gameserver_uid,
        ds_instance_epoch=source.instance_epoch,
        allocation_id=source.allocation_id,
        **kv,
    )


# ── key ─────────────────────────────────────────────────────────────────────


def battle_departure_journal_key(match_id: int) -> str:
    """`pandora:ds:departures:{<match_id>}`。花括号是 Cluster hashtag。"""
    return f"pandora:ds:departures:{{{_require_uint64('match_id', match_id)}}}"


def battle_instance_teardown_key(match_id: int, source: BattleDepartureSource) -> str:
    """`pandora:ds:teardown:{<match_id>}:<sha256[:16]>`。Go: `battleInstanceTeardownKey`。

    ★ 摘要输入 `pod\\x00uid\\x00epoch\\x00allocation_id` 必须逐字节与 Go 相同:
      它是**实例身份**的指纹。少一个分隔符就可能让 ("a","bc") 与 ("ab","c") 撞成
      同一个 key —— 两个不同实例共用一份 teardown proof,等于用 A 的拆除证明
      给 B 放行。
    """
    digest = hashlib.sha256(
        (
            "%s\x00%s\x00%d\x00%s"
            % (
                source.ds_pod_name,
                source.gameserver_uid,
                source.instance_epoch,
                source.allocation_id,
            )
        ).encode("utf-8")
    ).digest()
    return f"pandora:ds:teardown:{{{_require_uint64('match_id', match_id)}}}:{digest[:16].hex()}"


def battle_allocation_abort_key(match_id: int) -> str:
    """`pandora:ds:allocation-abort:{<match_id>}`。Go: `battleAllocationAbortKey`。"""
    return f"pandora:ds:allocation-abort:{{{_require_uint64('match_id', match_id)}}}"


def battle_allocation_lifecycle_published_key(match_id: int) -> str:
    """`pandora:ds:allocation-lifecycle-published:{<match_id>}`。

    Go: `battleAllocationLifecyclePublishedKey`。
    """
    return (
        "pandora:ds:allocation-lifecycle-published:"
        f"{{{_require_uint64('match_id', match_id)}}}"
    )


# ── 值对象 ──────────────────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True, slots=True)
class BattleDepartureSource:
    """placement 快照与 Battle Redis 权威都必须精确匹配的物理实例栅栏。

    Go: `BattleDepartureSource`。
    """

    ds_pod_name: str = ""
    gameserver_uid: str = ""
    instance_epoch: int = 0
    allocation_id: str = ""
    # ★ pod_uid 只由 allocator 在分配时从 K8s 权威 GET 捕获;heartbeat / Ensure
    #   **不信任调用方传入的该值**,因此它刻意**不在** valid() 里
    #   (Go 同样如此)。只有 RecordInstanceTeardown 额外要求它非空。
    pod_uid: str = ""

    def valid(self) -> bool:
        """Go: `BattleDepartureSource.valid`。

        ★ `instance_epoch <= _UINT32_MAX` 是 Python 必须补的一条:Go 的 uint32
          让越界根本不可表达,Python 不判就会把一个 Go 侧永远写不出的 epoch
          带进 key 摘要与 proto 字段 —— 前者算出一个 Go 找不到的 teardown key,
          后者要到 SerializeToString 才炸,那时事务已经开始了。
        """
        return (
            self.ds_pod_name != ""
            and self.gameserver_uid != ""
            and isinstance(self.instance_epoch, int)
            and not isinstance(self.instance_epoch, bool)
            and 0 < self.instance_epoch <= _UINT32_MAX
            and self.allocation_id != ""
        )


@dataclasses.dataclass(frozen=True, slots=True)
class BattlePlayerDepartureExpected:
    """`ensure_player_departure` 的完整幂等输入。Go: `BattlePlayerDepartureExpected`。"""

    match_id: int = 0
    player_id: int = 0
    # placement_version / operation_id 是 Begin 后当前 PENDING→HUB 代际。
    placement_version: int = 0
    operation_id: str = ""
    # source_* 是 Begin 原子捕获的 STABLE BATTLE claims。
    source_placement_version: int = 0
    source_operation_id: str = ""
    source: BattleDepartureSource = dataclasses.field(default_factory=BattleDepartureSource)


@dataclasses.dataclass(slots=True)
class BattlePlayerDepartureResult:
    """Go: `BattlePlayerDepartureResult`(可变,事务闭包内逐步填充)。

    区分心跳离场与整个 UID teardown 便于观测,但两者都是 Hub ticket / admission
    可接受的物理证明。
    """

    departed: bool = False
    status: int = dspb.BATTLE_PLAYER_DEPARTURE_STATUS_UNSPECIFIED
    departure_id: str = ""


@dataclasses.dataclass(slots=True)
class BattleAllocationAbortFenceResult:
    """预准入中止线性化点返回的持久快照。Go: `BattleAllocationAbortFenceResult`。

    ★ `released` 可能在 auth/battle 的有界审计记录早已过期后仍为 True ——
      exact ACK journal 是**刻意永久**的。反过来说:审计记录还在**不代表**未释放。
    """

    battle: object | None = None
    released: bool = False


class BattleAbortActiveIndexError(errcode.PandoraError):
    """权威侧中止已提交、但派生 active 索引(跨槽 ZSET)写失败。

    ★ 这是 Go 的 `return result, err` / `return true, err` —— 一个 Python 异常
      天然表达不了的返回形状。丢掉那个 "true/result" 的后果是**实质性**的:
        - fence 路径:调用方拿不到 `battle` 快照,就不会去做 UID 条件回收,
          一台已被锁进 TERMINATING 的 Pod 无人拆除;
        - complete 路径:调用方以为中止没完成,会重跑一次外部删除。
      两个证据都挂在**声明式 `__slots__`** 上,不用 `setattr` —— 后者能写进去
      (Exception 自带 `__dict__`),但拼错一个字母不会报错,读的那侧只会永远拿到
      默认值,与"根本没传"在日志里长得一模一样。
    """

    __slots__ = ("completed", "fence_result")

    def __init__(
        self,
        msg: str = "",
        *args: object,
        cause: BaseException | None = None,
        fence_result: BattleAllocationAbortFenceResult | None = None,
    ) -> None:
        super().__init__(errcode.ErrUnknown, msg, *args, cause=cause)
        # 权威变更已按 fencing 条件提交,调用方**必须**据此继续外部收尾。
        self.completed: bool = True
        self.fence_result: BattleAllocationAbortFenceResult | None = fence_result


# ── 纯判据(Go 的同名 helper,逐条件对齐)────────────────────────────────────


def stable_departure_id(expected: BattlePlayerDepartureExpected) -> str:
    """Go: `stableDepartureID`。

    ★ 这是 §9.23 要求的稳定幂等键:同一次真实离场的重试、回包丢失、进程重启都
      必须算出同一个 id。十个字段的顺序、`\\x00` 分隔符、`%d` / `%s` 的选择全都
      参与摘要,任何一处与 Go 不同都会让两栈算出不同 id —— 表现是"同一次离场
      在 journal 里出现两条",而两条都不会终态。
    """
    payload = "%d\x00%d\x00%d\x00%s\x00%d\x00%s\x00%s\x00%s\x00%d\x00%s" % (
        expected.match_id,
        expected.player_id,
        expected.placement_version,
        expected.operation_id,
        expected.source_placement_version,
        expected.source_operation_id,
        expected.source.ds_pod_name,
        expected.source.gameserver_uid,
        expected.source.instance_epoch,
        expected.source.allocation_id,
    )
    return hashlib.sha256(payload.encode("utf-8")).digest()[:16].hex()


def departure_source_equals_record(source: BattleDepartureSource, record) -> bool:  # noqa: ANN001
    """Go: `departureSourceEqualsRecord`。"""
    return (
        record is not None
        and record.ds_pod_name == source.ds_pod_name
        and record.gameserver_uid == source.gameserver_uid
        and record.instance_epoch == source.instance_epoch
        and record.allocation_id == source.allocation_id
    )


def departure_source_equals_battle(source: BattleDepartureSource, battle) -> bool:  # noqa: ANN001
    """Go: `departureSourceEqualsBattle`。"""
    return (
        battle is not None
        and battle.ds_pod_name == source.ds_pod_name
        and battle.gameserver_uid == source.gameserver_uid
        and battle.instance_epoch == source.instance_epoch
        and battle.allocation_id == source.allocation_id
    )


def departure_source_equals_teardown(source: BattleDepartureSource, proof) -> bool:  # noqa: ANN001
    """Go: `departureSourceEqualsTeardown`。

    ★ 比 `..._equals_record` **多两条**,少一条都会把证明放宽:
        `proof.pod_uid != ""`                 —— pod_uid 是同名 Pod 重建的唯一区分,
                                                 空 pod_uid 的证明证不了"哪一个 Pod";
        `source.pod_uid == "" or 相等`         —— 调用方给了 pod_uid 就必须精确命中;
                                                 没给(heartbeat / Ensure 不信任该值)
                                                 才允许放过这一格。
      写成 `proof.pod_uid == source.pod_uid` 会在 source.pod_uid 为空时要求证明
      也为空 —— 恰好把**唯一合格**的证明排除掉。
    """
    return (
        proof is not None
        and proof.ds_pod_name == source.ds_pod_name
        and proof.gameserver_uid == source.gameserver_uid
        and proof.instance_epoch == source.instance_epoch
        and proof.allocation_id == source.allocation_id
        and proof.pod_uid != ""
        and (source.pod_uid == "" or proof.pod_uid == source.pod_uid)
    )


def departure_source_equals_source(a: BattleDepartureSource, b: BattleDepartureSource) -> bool:
    """Go: `departureSourceEqualsSource`(**含** pod_uid,与上面三个不同)。"""
    return (
        a.ds_pod_name == b.ds_pod_name
        and a.gameserver_uid == b.gameserver_uid
        and a.instance_epoch == b.instance_epoch
        and a.allocation_id == b.allocation_id
        and a.pod_uid == b.pod_uid
    )


def departure_status_terminal(status: int) -> bool:
    """Go: `departureStatusTerminal`。DEPARTED / SOURCE_TORN_DOWN 都是终态。"""
    return status in (
        dspb.BATTLE_PLAYER_DEPARTURE_STATUS_DEPARTED,
        dspb.BATTLE_PLAYER_DEPARTURE_STATUS_SOURCE_TORN_DOWN,
    )


def departure_journal_terminal(journal) -> bool:  # noqa: ANN001
    """Go: `departureJournalTerminal`。

    ★ **空 journal 返回 True**(Go 的空循环同样如此)。这不是笔误:没有任何
      pending 驱逐单时,journal 确实"全终态",可以挂保留期。
    """
    if journal is None:
        return False
    for order in journal.departures:
        if not departure_status_terminal(order.status):
            return False
    return True


def battle_contains_player(battle, player_id: int) -> bool:  # noqa: ANN001
    """Go: `battleContainsPlayer`。权威 roster 判据,不是缓存。"""
    for candidate in battle.player_ids:
        if candidate == player_id:
            return True
    return False


# ── TTL / 序列化小工具 ──────────────────────────────────────────────────────


def _format_sec_ms(ttl_ms: int) -> int:
    """毫秒 → `EXPIRE` 的整秒参数。**逐条照抄 go-redis 的 `formatSec`**。

    ★ `0 < d < 1s` 时 go-redis 抬成 **1 秒**(而不是截成 0)。Python 直接
      `ttl_ms // 1000` 会得到 0,而 `EXPIRE key 0` 是**立即删除** —— 本模块里
      那意味着把刚证完的 journal / teardown proof 当场抹掉,读侧看到 key miss,
      而 miss 在离场链上恰恰是"没有证明"。
    ★ `int(x / 1000)` 而不是 `//`:Go 的 `int64(dur/time.Second)` **朝零截断**,
      Python 的 `//` 是**向下取整**,负数上两者不同。这里虽不会传负值,但判据
      本身不该依赖"调用方保证不会"。
    """
    if 0 < ttl_ms < 1000:
        return 1
    return int(ttl_ms / 1000)


def _set_px_ms(pipe, key: str, payload, ttl_ms: int) -> None:  # noqa: ANN001
    """`SET key payload [PX ms]`。对应 go-redis 的 `Set(ctx,key,val,dur)`(formatMs)。

    `ttl_ms <= 0` = 不带过期,对应 Go 的 `time.Duration(0)`。
    """
    if ttl_ms > 0:
        pipe.set(key, payload, px=ttl_ms)
    else:
        pipe.set(key, payload)


def _marshal_departure_message(message) -> bytes:  # noqa: ANN001
    """Go: `marshalDepartureMessage`(`proto.MarshalOptions{Deterministic:true}`)。

    ★ 确定性序列化不是洁癖:journal 会被反复读改写,非确定性的 map 顺序会让
      同一份逻辑内容产生不同字节,`proto_equal` 之类的字节级比较随机失败。
    """
    try:
        return message.SerializeToString(deterministic=True)
    except Exception as exc:  # noqa: BLE001 —— 对齐 Go 的 %w 包装(具体类型随实现变化)
        raise errcode.PandoraError(
            errcode.ErrInternal, "marshal battle departure record", cause=exc
        ) from exc


def _marshal_allocation_abort(record) -> bytes:  # noqa: ANN001
    """Go: `marshalAllocationAbort`。"""
    try:
        return record.SerializeToString(deterministic=True)
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "marshal battle allocation abort journal", cause=exc
        ) from exc


def _marshal_allocation_lifecycle_published(record) -> bytes:  # noqa: ANN001
    """Go: `marshalAllocationLifecyclePublished`。"""
    try:
        return record.SerializeToString(deterministic=True)
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "marshal battle allocation lifecycle marker", cause=exc
        ) from exc


# ── 读 ──────────────────────────────────────────────────────────────────────


async def _read_departure_journal(cmd, match_id: int):  # noqa: ANN001
    """Go: `readDepartureJournal`。key miss = **空 journal**(不是错误)。

    ★ 这是本模块里唯一一处"缺失即合法"的读:journal 不存在只说明还没有人申请过
      离场证明,而不是"证明丢了"。teardown proof / abort journal / lifecycle marker
      的缺失都**不**等价于任何终态,见各自的调用点。
    """
    payload = await cmd.get(battle_departure_journal_key(match_id))
    if payload is None:
        journal = dspb.BattlePlayerDepartureJournalStorageRecord()
        journal.match_id = match_id
        return journal
    journal = dspb.BattlePlayerDepartureJournalStorageRecord()
    try:
        journal.ParseFromString(bytes(payload))
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "decode battle departure journal %d", match_id, cause=exc
        ) from exc
    if journal.match_id != match_id:
        raise errcode.PandoraError(
            errcode.ErrInvalidState,
            "battle departure journal match mismatch: got %d want %d",
            journal.match_id,
            match_id,
        )
    return journal


async def _read_teardown_proof(cmd, match_id: int, source: BattleDepartureSource):  # noqa: ANN001
    """Go: `readTeardownProof`。返回 `(proof, found)`。"""
    payload = await cmd.get(battle_instance_teardown_key(match_id, source))
    if payload is None:
        return None, False
    proof = dspb.BattleInstanceTeardownStorageRecord()
    try:
        proof.ParseFromString(bytes(payload))
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "decode battle teardown proof %d", match_id, cause=exc
        ) from exc
    if proof.match_id != match_id:
        raise errcode.PandoraError(
            errcode.ErrInvalidState,
            "battle teardown proof match mismatch: got %d want %d",
            proof.match_id,
            match_id,
        )
    return proof, True


async def _teardown_proof_remaining_retention(
    pipe, key: str, found: bool
) -> tuple[int, bool]:  # noqa: ANN001
    """Go: `teardownProofRemainingRetention`。返回 `(remaining_ms, repair_legacy)`。

    保留窗口由**第一次持久写**锚定、**不可续期** —— 否则每次幂等重试都把窗口
    往后推,已经结束的对局就永远清不掉(§9.24)。

    PTTL 三态(redis-py 直接给整数,go-redis 包成 Duration):
        > 0   还有剩余:钳到 7 天上限后**只带剩余**;
        == -1 key 在但没有 TTL(历史遗留的永久 proof):补一次 7 天(repair_legacy);
        == -2 key 在本次 WATCH 期间**过期消失**了 → 抛 `WatchError` 让外层重试。

    ★ 最后一条抛的是 `WatchError` 而不是业务错误:Go 返回的是
      `redis.TxFailedErr`,语义就是"这一轮的前提没了,重来",不是"请求非法"。
      写成业务错误会把一次正常的竞态变成对调用方可见的失败。
    """
    if not found:
        return 0, False
    ttl_ms = int(await pipe.pttl(key))
    if ttl_ms > 0:
        if ttl_ms > BATTLE_DEPARTURE_TERMINAL_RETENTION_MS:
            ttl_ms = BATTLE_DEPARTURE_TERMINAL_RETENTION_MS
        return ttl_ms, False
    if ttl_ms == -1:
        return BATTLE_DEPARTURE_TERMINAL_RETENTION_MS, True
    raise WatchError("teardown proof expired during watch")


async def _read_allocation_abort_record(cmd, match_id: int, key: str):  # noqa: ANN001
    """Go: `readAllocationAbortRecord`。返回 `(record, found)`。

    ★ 形状不合格(match 不符 / target 不完整 / requested_at_ms<=0)一律**报错**
      而不是当成 not found:一条读不懂的中止台账既不能证明"没中止过",也不能
      证明"中止完成了" —— 把它降级成 not found 会让下一步凭空重新开始一次中止。
    """
    payload = await cmd.get(key)
    if payload is None:
        return None, False
    record = dspb.BattleAllocationAbortStorageRecord()
    try:
        record.ParseFromString(bytes(payload))
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "decode battle %d allocation abort journal", match_id, cause=exc
        ) from exc
    if (
        record.match_id != match_id
        or not abort_request_from_record(record).complete()
        or record.requested_at_ms <= 0
    ):
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "battle %d allocation abort journal is malformed", match_id
        )
    return record, True


async def _read_allocation_lifecycle_published(cmd, match_id: int, key: str):  # noqa: ANN001
    """Go: `readAllocationLifecyclePublished`。返回 `(record, found)`。"""
    payload = await cmd.get(key)
    if payload is None:
        return None, False
    record = dspb.BattleAllocationLifecyclePublishedStorageRecord()
    try:
        record.ParseFromString(bytes(payload))
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "decode battle %d allocation lifecycle marker", match_id, cause=exc
        ) from exc
    if (
        record.match_id != match_id
        or not battleabort.valid_target(lifecycle_published_target(record))
        or record.phase != dspb.DS_LIFECYCLE_PHASE_ABANDONED
        or record.published_at_ms <= 0
    ):
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "battle %d allocation lifecycle marker is malformed", match_id
        )
    return record, True


# ── abort / lifecycle 的纯判据 ──────────────────────────────────────────────


def abort_record_matches_request(record, request: battleabort.Request) -> bool:  # noqa: ANN001
    """Go: `abortRecordMatchesRequest`。七个字段全等,少一格就是幂等键被放宽。"""
    return (
        record is not None
        and record.match_id == request.match_id
        and record.allocation_operation_id == request.operation_id
        and record.ds_pod_name == request.target.pod_name
        and record.gameserver_uid == request.target.instance_uid
        and record.instance_epoch == request.target.instance_epoch
        and record.allocation_id == request.target.allocation_id
        and record.release_track == request.target.release_track
    )


def abort_request_from_record(record) -> battleabort.Request:  # noqa: ANN001
    """Go: `abortRequestFromRecord`。"""
    if record is None:
        return battleabort.Request()
    return battleabort.Request(
        match_id=record.match_id,
        operation_id=record.allocation_operation_id,
        target=placement.Target(
            pod_name=record.ds_pod_name,
            instance_uid=record.gameserver_uid,
            instance_epoch=record.instance_epoch,
            allocation_id=record.allocation_id,
            release_track=record.release_track,
        ),
    )


def new_allocation_abort_record(
    request: battleabort.Request, requested_at_ms: int, released_at_ms: int
):
    """Go: `newAllocationAbortRecord`。"""
    return dspb.BattleAllocationAbortStorageRecord(
        match_id=request.match_id,
        allocation_operation_id=request.operation_id,
        ds_pod_name=request.target.pod_name,
        gameserver_uid=request.target.instance_uid,
        instance_epoch=request.target.instance_epoch,
        allocation_id=request.target.allocation_id,
        release_track=request.target.release_track,
        requested_at_ms=requested_at_ms,
        released_at_ms=released_at_ms,
    )


def lifecycle_published_target(record) -> placement.Target:  # noqa: ANN001
    """Go: `lifecyclePublishedTarget`。"""
    if record is None:
        return placement.Target()
    return placement.Target(
        pod_name=record.ds_pod_name,
        instance_uid=record.gameserver_uid,
        instance_epoch=record.instance_epoch,
        allocation_id=record.allocation_id,
        release_track=record.release_track,
    )


def lifecycle_published_record_matches(
    record, match_id: int, target: placement.Target
) -> bool:  # noqa: ANN001
    """Go: `lifecyclePublishedRecordMatches`。"""
    return (
        record is not None
        and record.match_id == match_id
        and record.phase == dspb.DS_LIFECYCLE_PHASE_ABANDONED
        and record.published_at_ms > 0
        and lifecycle_published_target(record).equal(target)
    )


def _abort_source(request: battleabort.Request) -> BattleDepartureSource:
    """把中止请求的 target 翻成实例栅栏(Go 在三处各手写一遍同样的字面量)。

    ★ 刻意**不带 pod_uid**:与 Go 的三处构造完全一致。中止链手上没有 K8s 权威
      GET 到的 pod_uid,`departure_source_equals_teardown` 的
      `source.pod_uid == ""` 分支就是为这里留的。
    """
    return BattleDepartureSource(
        ds_pod_name=request.target.pod_name,
        gameserver_uid=request.target.instance_uid,
        instance_epoch=request.target.instance_epoch,
        allocation_id=request.target.allocation_id,
    )


async def _allocation_abort_terminal_proofs_match(
    cmd,
    request: battleabort.Request,
    teardown_key: str,  # noqa: ARG001 —— Go 侧同样未使用,保留以便逐行对照
    lifecycle_key: str,
) -> bool:  # noqa: ANN001
    """Go: `allocationAbortTerminalProofsMatch`。

    ★★ 这里是**合取**而不是析取,是整条中止链最关键的一行:

        exact UID+Pod teardown proof  ∧  exact full-target lifecycle ACK

      只有两者同时命中同一个 target,才允许把一次中止认成"外部效果已完成"。
      任何一个单独成立都不够 ——
        - 只有 teardown:Pod 拆了,但 battle_result 那边可能还没收到 ABANDONED,
          玩家段位回滚(§9 不变量 4)会永久丢失;
        - 只有 lifecycle ACK:事件发了,但 GameServer 可能还活着,一台 14Gi 的
          Pod 无人回收,而权威记录已经按"已中止"往下走。
      改成 `or` 不会有任何测试变红(两个证明通常一起出现),但会在**恰好只有
      一个**的崩溃恢复窗口里静默放行。
    """
    source = _abort_source(request)
    teardown, teardown_found = await _read_teardown_proof(cmd, request.match_id, source)
    marker, marker_found = await _read_allocation_lifecycle_published(
        cmd, request.match_id, lifecycle_key
    )
    return (
        teardown_found
        and departure_source_equals_teardown(source, teardown)
        and marker_found
        and lifecycle_published_record_matches(marker, request.match_id, request.target)
    )


def allocation_abort_fence_matches(auth_record, battle, request: battleabort.Request) -> bool:  # noqa: ANN001
    """Go: `allocationAbortFenceMatches`。"""
    expected = BattleExpectedInstance(
        allocation_id=request.target.allocation_id,
        instance_uid=request.target.instance_uid,
        instance_epoch=request.target.instance_epoch,
    )
    return (
        request.complete()
        and battle_auth_record_v2_exact(auth_record)
        and expected_battle_instance_matches(auth_record, battle, expected)
        and auth_record.phase == dspb.BATTLE_AUTH_PHASE_TERMINATING
        and battle.state == BATTLE_STATE_ALLOCATION_ABORT_PENDING
        and battle.ds_pod_name == request.target.pod_name
        and battle.release_track == request.target.release_track
    )


def allocation_abort_completion_authority_matches(
    auth_record, battle, request: battleabort.Request
) -> bool:  # noqa: ANN001
    """Go: `allocationAbortCompletionAuthorityMatches`。

    比 fence 版多接受一种形状:battle 已经被 lifecycle ACK 推进到 `abandoned`。
    这不是放宽,而是同一条链的**下一站** —— lifecycle 提交与 abort 收尾之间
    崩溃过一次时,重试必须还能认出自己刚推进到哪。
    """
    if allocation_abort_fence_matches(auth_record, battle, request):
        return True
    expected = BattleExpectedInstance(
        allocation_id=request.target.allocation_id,
        instance_uid=request.target.instance_uid,
        instance_epoch=request.target.instance_epoch,
    )
    return (
        request.complete()
        and battle_auth_record_v2_exact(auth_record)
        and expected_battle_instance_matches(auth_record, battle, expected)
        and auth_record.phase == dspb.BATTLE_AUTH_PHASE_TERMINATING
        and battle.state == "abandoned"
        and battle.ds_pod_name == request.target.pod_name
        and battle.release_track == request.target.release_track
    )


def allocation_abort_released_auth_matches(auth_record, request: battleabort.Request) -> bool:  # noqa: ANN001
    """Go: `allocationAbortReleasedAuthMatches`。"""
    return (
        request.complete()
        and battle_auth_record_v2_exact(auth_record)
        and auth_record.match_id == request.match_id
        and auth_record.allocation_id == request.target.allocation_id
        and auth_record.ds_pod_name == request.target.pod_name
        and auth_record.instance_uid == request.target.instance_uid
        and auth_record.instance_epoch == request.target.instance_epoch
        and auth_record.phase == dspb.BATTLE_AUTH_PHASE_TERMINATING
    )


def allocation_abort_released_battle_matches(battle, request: battleabort.Request) -> bool:  # noqa: ANN001
    """Go: `allocationAbortReleasedBattleMatches`。"""
    return (
        request.complete()
        and battle is not None
        and battle.match_id == request.match_id
        and battle.state == "abandoned"
        and battle.ds_pod_name == request.target.pod_name
        and battle.gameserver_uid == request.target.instance_uid
        and battle.instance_epoch == request.target.instance_epoch
        and battle.allocation_id == request.target.allocation_id
        and battle.release_track == request.target.release_track
    )


# ── 事务体:EnsurePlayerDeparture ────────────────────────────────────────────


async def _ensure_departure_tx(  # noqa: ANN001
    pipe,
    expected: BattlePlayerDepartureExpected,
    departure_id: str,
    j_key: str,
    t_key: str,
    b_key: str,
    result: BattlePlayerDepartureResult,
) -> None:
    """`EnsurePlayerDeparture` 的 Go 闭包体(逐分支对照)。"""
    journal = await _read_departure_journal(pipe, expected.match_id)
    proof, proof_found = await _read_teardown_proof(pipe, expected.match_id, expected.source)
    if proof_found and not departure_source_equals_teardown(expected.source, proof):
        raise errcode.PandoraError(
            errcode.ErrInvalidState,
            "battle %d teardown proof tuple conflict",
            expected.match_id,
        )
    proof_retention_ms, repair_proof_ttl = await _teardown_proof_remaining_retention(
        pipe, t_key, proof_found
    )

    existing = None
    for order in journal.departures:
        if order.departure_id == departure_id:
            existing = order
            break

    if existing is not None:
        if (
            existing.match_id != expected.match_id
            or existing.player_id != expected.player_id
            or existing.operation_id != expected.operation_id
            or existing.placement_version != expected.placement_version
            or existing.source_operation_id != expected.source_operation_id
            or existing.source_placement_version != expected.source_placement_version
            or not departure_source_equals_record(expected.source, existing)
        ):
            # 同一个 departure_id 却对不上元组 = 摘要碰撞或调用方串了参数。
            # 两种都必须停,继续走会把 A 的离场证明记在 B 头上。
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "battle departure idempotency tuple conflict"
            )
        if departure_status_terminal(existing.status):
            result.departed, result.status = True, existing.status
            if (
                proof_found
                and departure_source_equals_teardown(expected.source, proof)
                and departure_journal_terminal(journal)
            ):
                pipe.multi()
                pipe.expire(j_key, _format_sec_ms(proof_retention_ms))
                if repair_proof_ttl:
                    pipe.expire(t_key, _format_sec_ms(BATTLE_DEPARTURE_TERMINAL_RETENTION_MS))
                await pipe.execute()
            return
        if proof_found and departure_source_equals_teardown(expected.source, proof):
            existing.status = dspb.BATTLE_PLAYER_DEPARTURE_STATUS_SOURCE_TORN_DOWN
            existing.departed_at_ms = proof.torn_down_at_ms
            payload = _marshal_departure_message(journal)
            ttl_ms = 0
            if departure_journal_terminal(journal):
                ttl_ms = proof_retention_ms
            pipe.multi()
            _set_px_ms(pipe, j_key, payload, ttl_ms)
            if repair_proof_ttl:
                pipe.expire(t_key, _format_sec_ms(BATTLE_DEPARTURE_TERMINAL_RETENTION_MS))
            await pipe.execute()
            # ★ 只有写**成功**(execute 未抛)才置 departed;Go 是 `if err == nil`。
            result.departed, result.status = True, existing.status
            return
        result.status = dspb.BATTLE_PLAYER_DEPARTURE_STATUS_PENDING
        return

    if len(journal.departures) >= MAX_BATTLE_DEPARTURE_ORDERS:
        # §9 不变量 18:客户端可触发的累积列表必须有写入侧上限。
        raise errcode.PandoraError(
            errcode.ErrRateLimited, "battle %d departure journal full", expected.match_id
        )
    current_ms = now_ms()
    order = dspb.BattlePlayerDepartureStorageRecord(
        departure_id=departure_id,
        match_id=expected.match_id,
        player_id=expected.player_id,
        placement_version=expected.placement_version,
        operation_id=expected.operation_id,
        source_placement_version=expected.source_placement_version,
        source_operation_id=expected.source_operation_id,
        ds_pod_name=expected.source.ds_pod_name,
        gameserver_uid=expected.source.gameserver_uid,
        instance_epoch=expected.source.instance_epoch,
        allocation_id=expected.source.allocation_id,
        requested_at_ms=current_ms,
        status=dspb.BATTLE_PLAYER_DEPARTURE_STATUS_PENDING,
    )
    if proof_found and departure_source_equals_teardown(expected.source, proof):
        order.status = dspb.BATTLE_PLAYER_DEPARTURE_STATUS_SOURCE_TORN_DOWN
        order.departed_at_ms = proof.torn_down_at_ms
        result.departed, result.status = True, order.status
    else:
        battle_payload = await pipe.get(b_key)
        if battle_payload is None:
            # ★★ 全模块最容易被"优化"掉的一行。battle key 缺失 **不是** 离场证明:
            #   TTL 过期、镜像被清、Redis 故障都会让它消失,而 DS 可能还在跑。
            #   返回可重试的 UNAVAILABLE(而不是"已离场")是 §9.22 的 fail-closed。
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle %d source missing without exact teardown proof",
                expected.match_id,
            )
        battle = unmarshal_battle(expected.match_id, battle_payload)
        if not departure_source_equals_battle(expected.source, battle):
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "battle %d source tuple mismatch", expected.match_id
            )
        if not battle_contains_player(battle, expected.player_id):
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "player %d not in battle %d authoritative roster",
                expected.player_id,
                expected.match_id,
            )
        result.status = order.status
    journal.departures.append(order)
    payload = _marshal_departure_message(journal)
    ttl_ms = 0
    if (
        proof_found
        and departure_source_equals_teardown(expected.source, proof)
        and departure_journal_terminal(journal)
    ):
        ttl_ms = proof_retention_ms
    pipe.multi()
    _set_px_ms(pipe, j_key, payload, ttl_ms)
    if repair_proof_ttl:
        pipe.expire(t_key, _format_sec_ms(BATTLE_DEPARTURE_TERMINAL_RETENTION_MS))
    await pipe.execute()


# ── 事务体:ReconcilePlayerDepartures ────────────────────────────────────────


async def _reconcile_departures_tx(  # noqa: ANN001
    pipe,
    match_id: int,
    source: BattleDepartureSource,
    snapshot_present: bool,
    census_id: str,
    active: set[int],
    acked: set[str],
    j_key: str,
):
    """`ReconcilePlayerDepartures` 的 Go 闭包体。返回台账五元组。"""
    orders: list = []
    issued_player_ids: list[int] = []
    issued_departure_ids: list[str] = []
    stalled_player_ids: list[int] = []
    oldest_stalled_ms = 0

    journal = await _read_departure_journal(pipe, match_id)
    changed = False
    known_acks: set[str] = set()
    current_ms = now_ms()
    for order in journal.departures:
        if order.match_id != match_id or not departure_source_equals_record(source, order):
            continue
        # ★ 必须在可能改写它之前快照:下面会把 acknowledged_at_ms 置成本轮时间,
        #   之后再读就分不清"本轮才 ack"与"上一轮就 ack 过"。
        acknowledged_before_this_census = order.acknowledged_at_ms > 0
        if order.departure_id in acked:
            known_acks.add(order.departure_id)
            if order.issued_at_ms == 0:
                # DS 声称执行了一条我们从没下发过的驱逐单 —— 只可能是伪造或串线。
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "departure %s acknowledged before an order was issued",
                    order.departure_id,
                )
            if order.player_id in active:
                # 同一份 census 里"已踢掉"和"还在场"自相矛盾,fail-closed。
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "departure %s acknowledged while player %d remains active",
                    order.departure_id,
                    order.player_id,
                )
            if not acknowledged_before_this_census:
                order.acknowledged_at_ms = current_ms
                order.acknowledged_census_id = census_id
                changed = True
        if order.status != dspb.BATTLE_PLAYER_DEPARTURE_STATUS_PENDING:
            continue
        if (
            snapshot_present
            and acknowledged_before_this_census
            and order.acknowledged_census_id != census_id
        ):
            # ★ 提交 DEPARTED 需要**另一份**(census_id 不同的)完整快照证明缺席。
            #   首次 ACK 只证明"驱逐单执行了",不证明玩家真的不在了。
            if order.player_id not in active:
                order.status = dspb.BATTLE_PLAYER_DEPARTURE_STATUS_DEPARTED
                order.departed_at_ms = current_ms
                changed = True
                continue
        if order.issued_at_ms == 0:
            order.issued_at_ms = current_ms
            changed = True
            issued_player_ids.append(order.player_id)
            issued_departure_ids.append(order.departure_id)
        else:
            age = current_ms - order.issued_at_ms
            if age >= EVICTION_ORDER_STALLED_WARN_MS:
                stalled_player_ids.append(order.player_id)
                if age > oldest_stalled_ms:
                    oldest_stalled_ms = age
        orders.append(
            dspb.BattleEvictionOrder(
                departure_id=order.departure_id,
                match_id=order.match_id,
                player_id=order.player_id,
                ds_pod_name=order.ds_pod_name,
                gameserver_uid=order.gameserver_uid,
                instance_epoch=order.instance_epoch,
                allocation_id=order.allocation_id,
                # ★ 下发的是 **source_** 代际,不是 transition 代际:驱逐单要
                #   精确命中**旧 Battle** 的 admission,用新代际会打空。
                placement_version=order.source_placement_version,
                operation_id=order.source_operation_id,
            )
        )
    if snapshot_present and len(known_acks) != len(acked):
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "heartbeat acknowledged unknown departure id"
        )
    if not changed:
        return orders, issued_player_ids, issued_departure_ids, stalled_player_ids, oldest_stalled_ms
    payload = _marshal_departure_message(journal)
    pipe.multi()
    _set_px_ms(pipe, j_key, payload, 0)
    await pipe.execute()
    return orders, issued_player_ids, issued_departure_ids, stalled_player_ids, oldest_stalled_ms


def log_departure_issuance(
    match_id: int,
    source: BattleDepartureSource,
    issued_player_ids: list[int],
    issued_departure_ids: list[str],
    stalled_player_ids: list[int],
    oldest_stalled_ms: int,
) -> None:
    """Go: `logDepartureIssuance`。事务落定后打驱逐单台账。

    - 首发 INFO:`issued_at_ms` 首次置位那一轮,每 departure 恰一条。此前首发只有
      biz 层 DEBUG——「点了退出没反应」时,后端→DS 的唯一指令(驱逐单)发没发,
      生产 info 级查不到。
    - 超龄 WARN:首发后超阈值仍 PENDING,按本轮聚合点名(玩家可能卡在退出副本上)。

    ★ 事件名与字段名与 Go 逐字节相同,否则 Loki 上同一条 LogQL 查不到 Python 副本。
    """
    if issued_player_ids:
        plog.get().info(
            "battle_eviction_order_first_issued",
            match_id=match_id,
            ds_pod=source.ds_pod_name,
            player_ids=issued_player_ids,
            departure_ids=issued_departure_ids,
        )
    if stalled_player_ids:
        plog.get().warning(
            "battle_eviction_order_stalled",
            match_id=match_id,
            ds_pod=source.ds_pod_name,
            player_ids=stalled_player_ids,
            oldest_age_ms=oldest_stalled_ms,
            hint="驱逐单已发但 DS 未 ack:玩家可能卡在退出副本,查 DS 侧离场处理与心跳链",
        )


# ── 事务体:RecordInstanceTeardown ───────────────────────────────────────────


async def _record_instance_teardown_tx(  # noqa: ANN001
    pipe, match_id: int, source: BattleDepartureSource, j_key: str, t_key: str
) -> None:
    """`RecordInstanceTeardown` 的 Go 闭包体。"""
    journal = await _read_departure_journal(pipe, match_id)
    existing, found = await _read_teardown_proof(pipe, match_id, source)
    if found and not departure_source_equals_teardown(source, existing):
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "battle %d teardown proof tuple conflict", match_id
        )
    proof_retention_ms, repair_proof_ttl = await _teardown_proof_remaining_retention(
        pipe, t_key, found
    )
    if not found:
        proof_retention_ms = BATTLE_DEPARTURE_TERMINAL_RETENTION_MS
    current_ms = now_ms()
    proof = existing
    if proof is None:
        proof = dspb.BattleInstanceTeardownStorageRecord(
            match_id=match_id,
            ds_pod_name=source.ds_pod_name,
            gameserver_uid=source.gameserver_uid,
            instance_epoch=source.instance_epoch,
            allocation_id=source.allocation_id,
            torn_down_at_ms=current_ms,
            pod_uid=source.pod_uid,
        )
    journal_changed = False
    for order in journal.departures:
        if order.status == dspb.BATTLE_PLAYER_DEPARTURE_STATUS_PENDING and (
            departure_source_equals_record(source, order)
        ):
            order.status = dspb.BATTLE_PLAYER_DEPARTURE_STATUS_SOURCE_TORN_DOWN
            order.departed_at_ms = proof.torn_down_at_ms
            journal_changed = True
    proof_payload = _marshal_departure_message(proof)
    journal_payload = None
    if journal_changed:
        journal_payload = _marshal_departure_message(journal)
    journal_terminal = departure_journal_terminal(journal)
    pipe.multi()
    # teardown proof 的权威性来自「ReleaseExpected 已经明确成功」,而不是它有没有
    # TTL。重写一份旧的永久 proof 顺便把上线前的遗留形态修成有界(幂等重试安全)。
    if found:
        pipe.set(t_key, proof_payload, keepttl=True)
        if repair_proof_ttl:
            pipe.expire(t_key, _format_sec_ms(BATTLE_DEPARTURE_TERMINAL_RETENTION_MS))
    else:
        _set_px_ms(pipe, t_key, proof_payload, BATTLE_DEPARTURE_TERMINAL_RETENTION_MS)
    if journal_changed:
        if journal_terminal:
            _set_px_ms(pipe, j_key, journal_payload, proof_retention_ms)
        else:
            _set_px_ms(pipe, j_key, journal_payload, 0)
    elif journal_terminal:
        # 幂等重放:给一份"字节早已按 TTL=0 提交过"的全终态 journal 补上界。
        pipe.expire(j_key, _format_sec_ms(proof_retention_ms))
    await pipe.execute()


# ── 事务体:RecordAllocationLifecyclePublished ───────────────────────────────


async def _record_lifecycle_published_tx(  # noqa: ANN001
    pipe,
    strict_model_b_writes: bool,
    match_id: int,
    target: placement.Target,
    source: BattleDepartureSource,
    m_key: str,
    b_key: str,
    a_key: str,
    j_key: str,
) -> None:
    """`recordAllocationLifecyclePublished` 的 Go 闭包体。"""
    marker, found = await _read_allocation_lifecycle_published(pipe, match_id, m_key)
    if found:
        if not lifecycle_published_record_matches(marker, match_id, target):
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "battle %d allocation lifecycle marker tuple conflict",
                match_id,
            )
        return
    teardown, teardown_found = await _read_teardown_proof(pipe, match_id, source)
    if not teardown_found or not departure_source_equals_teardown(source, teardown):
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "battle %d lifecycle ACK lacks exact teardown proof", match_id
        )
    auth_record, battle = await _read_bound_authority(pipe, match_id, a_key, b_key)
    battle_before = clone(battle)
    expected = BattleExpectedInstance(
        allocation_id=target.allocation_id,
        instance_uid=target.instance_uid,
        instance_epoch=target.instance_epoch,
    )
    abort_transition = False
    if battle.state == BATTLE_STATE_ALLOCATION_ABORT_PENDING:
        abort_record, abort_found = await _read_allocation_abort_record(pipe, match_id, j_key)
        abort_transition = (
            abort_found
            and abort_record.released_at_ms == 0
            and abort_request_from_record(abort_record).target.equal(target)
        )
    if (
        not battle_auth_record_v2_exact(auth_record)
        or auth_record.phase != dspb.BATTLE_AUTH_PHASE_TERMINATING
        or not expected_battle_instance_matches(auth_record, battle, expected)
        or (battle.state != "abandoned" and not abort_transition)
        or battle.ds_pod_name != target.pod_name
        or battle.gameserver_uid != target.instance_uid
        or battle.instance_epoch != target.instance_epoch
        or battle.allocation_id != target.allocation_id
        or battle.release_track != target.release_track
    ):
        raise errcode.PandoraError(
            errcode.ErrInvalidState,
            "battle %d lifecycle ACK lacks exact abandoned/terminating authority",
            match_id,
        )
    marker = dspb.BattleAllocationLifecyclePublishedStorageRecord(
        match_id=match_id,
        ds_pod_name=target.pod_name,
        gameserver_uid=target.instance_uid,
        instance_epoch=target.instance_epoch,
        allocation_id=target.allocation_id,
        release_track=target.release_track,
        phase=dspb.DS_LIFECYCLE_PHASE_ABANDONED,
        published_at_ms=now_ms(),
    )
    payload = _marshal_allocation_lifecycle_published(marker)
    battle_payload = None
    if abort_transition:
        battle.state = "abandoned"
        if strict_model_b_writes:
            try:
                battle_payload = marshal_battle_transition(battle_before, battle)
            except errcode.PandoraError:
                raise
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "battle %d storage invariant failed: %s",
                    match_id,
                    exc,
                    cause=exc,
                ) from exc
        else:
            battle_payload = battle.SerializeToString()
    pipe.multi()
    _set_px_ms(pipe, m_key, payload, 0)
    if abort_transition:
        # lifecycle ACK 是一次分配中止的终态迁移。两把权威 key 保持**永久**,
        # 直到中止台账自己被原子标记 RELEASED 为止。
        _set_px_ms(pipe, b_key, battle_payload, 0)
    await pipe.execute()


async def record_allocation_lifecycle_published(
    rdb, strict_model_b_writes: bool, match_id: int, target: placement.Target
) -> None:  # noqa: ANN001
    """Go: 自由函数 `recordAllocationLifecyclePublished`(两个 repo 共用)。"""
    if match_id == 0 or not battleabort.valid_target(target):
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "complete battle allocation lifecycle target required"
        )
    source = BattleDepartureSource(
        ds_pod_name=target.pod_name,
        gameserver_uid=target.instance_uid,
        instance_epoch=target.instance_epoch,
        allocation_id=target.allocation_id,
    )
    t_key = battle_instance_teardown_key(match_id, source)
    m_key = battle_allocation_lifecycle_published_key(match_id)
    b_key = battle_key(match_id)
    a_key = battle_auth_key(match_id)
    j_key = battle_allocation_abort_key(match_id)
    for _attempt in range(BATTLE_AUTH_CAS_RETRIES):
        try:
            async with rdb.pipeline(transaction=True) as pipe:
                await pipe.watch(t_key, m_key, b_key, a_key, j_key)
                await _record_lifecycle_published_tx(
                    pipe, strict_model_b_writes, match_id, target, source, m_key, b_key, a_key, j_key
                )
        except WatchError:
            continue
        return
    raise errcode.PandoraError(
        errcode.ErrInternal, "battle %d allocation lifecycle marker CAS retry exhausted", match_id
    )


# ── 混入:离场(Go 的 *RedisBattleRepo 方法集)─────────────────────────────


class BattleDepartureMixin:
    """混入 `repo.RedisBattleRepo`。Go 侧这些是同一个 struct 的方法。

    ★ 为什么是 mixin 而不是直接改 `repo.py`:Go 用同一个包的多个文件给同一个
      struct 挂方法,Python 没有这个能力;直接把方法写进 `repo.py` 会让那个已经
      1600 行的文件继续膨胀,而 import 反向依赖(repo → departure)又会成环。
      装配请用文件末尾的 `RedisBattleDepartureRepo`。
    """

    __slots__ = ()

    async def ensure_player_departure(
        self, expected: BattlePlayerDepartureExpected
    ) -> BattlePlayerDepartureResult:
        """Go: `RedisBattleRepo.EnsurePlayerDeparture`。

        只创建 / 查询 exact pending order。**battle key 缺失不是 departure proof**;
        没有 durable teardown 时返回可重试的 UNAVAILABLE。
        """
        if (
            expected.match_id == 0
            or expected.player_id == 0
            or expected.placement_version == 0
            or expected.operation_id == ""
            or expected.source_placement_version == 0
            or expected.source_operation_id == ""
            or not expected.source.valid()
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "complete battle departure operation and source tuple required",
            )
        departure_id = stable_departure_id(expected)
        j_key = battle_departure_journal_key(expected.match_id)
        t_key = battle_instance_teardown_key(expected.match_id, expected.source)
        b_key = battle_key(expected.match_id)

        for _attempt in range(BATTLE_DEPARTURE_CAS_RETRIES):
            result = BattlePlayerDepartureResult(departure_id=departure_id)
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(j_key, t_key, b_key)
                    await _ensure_departure_tx(
                        pipe, expected, departure_id, j_key, t_key, b_key, result
                    )
            except WatchError:
                continue
            return result
        raise errcode.PandoraError(
            errcode.ErrDSAllocationFailed,
            "battle %d departure CAS retry exhausted",
            expected.match_id,
        )

    async def reconcile_player_departures(
        self,
        match_id: int,
        source: BattleDepartureSource,
        snapshot_present: bool,
        census_capability_version: int,
        census_id: str,
        active_player_ids: list[int],
        acknowledged_departure_ids: list[str],
    ) -> list:
        """Go: `RedisBattleRepo.ReconcilePlayerDepartures`。

        只在 Heartbeat 已通过 active/pending credential 验证后调用。
        """
        if match_id == 0 or not source.valid():
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "complete battle heartbeat source tuple required"
            )
        active: set[int] = set()
        for player_id in active_player_ids:
            if player_id == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "active player snapshot contains zero player"
                )
            if player_id in active:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "active player snapshot contains duplicate player %d",
                    player_id,
                )
            active.add(player_id)
        acked: set[str] = set()
        for departure_id in acknowledged_departure_ids:
            if departure_id == "":
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "empty acknowledged departure id"
                )
            if departure_id in acked:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "duplicate acknowledged departure id"
                )
            acked.add(departure_id)
        if snapshot_present and (
            census_capability_version < BATTLE_PLAYER_CENSUS_CAPABILITY_VERSION_V1
            or census_id == ""
        ):
            # ★ 旧 DS(capability 0)的空快照绝不能被当成"全员已离场"。
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "complete battle player census requires capability_version>=1 and census_id",
            )
        if not snapshot_present and (
            len(active) != 0
            or len(acked) != 0
            or census_capability_version != 0
            or census_id != ""
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle active player snapshot payload requires present=true",
            )

        j_key = battle_departure_journal_key(match_id)
        for _attempt in range(BATTLE_DEPARTURE_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(j_key)
                    (
                        orders,
                        issued_player_ids,
                        issued_departure_ids,
                        stalled_player_ids,
                        oldest_stalled_ms,
                    ) = await _reconcile_departures_tx(
                        pipe, match_id, source, snapshot_present, census_id, active, acked, j_key
                    )
            except WatchError:
                continue
            log_departure_issuance(
                match_id,
                source,
                issued_player_ids,
                issued_departure_ids,
                stalled_player_ids,
                oldest_stalled_ms,
            )
            return orders
        raise errcode.PandoraError(
            errcode.ErrDSAllocationFailed,
            "battle %d departure reconcile CAS retry exhausted",
            match_id,
        )

    async def record_instance_teardown(
        self, match_id: int, source: BattleDepartureSource
    ) -> None:
        """Go: `RedisBattleRepo.RecordInstanceTeardown`。

        必须在 ReleaseExpected 已明确成功之后调用:先写 durable proof,再允许上层
        purge battle/auth。Redis 回包不确定时上层保留 release fence 并重试 UID
        条件回收,404 幂等成功后可重建证明。

        ★ 这里额外要求 `source.pod_uid != ""` —— 它是同名 Pod 重建(ABA)的唯一
          区分。允许空 pod_uid 落盘会造出一份"证不了是哪个 Pod"的终态证明。
        """
        if match_id == 0 or not source.valid() or source.pod_uid == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "complete battle teardown source tuple required"
            )
        j_key = battle_departure_journal_key(match_id)
        t_key = battle_instance_teardown_key(match_id, source)
        for _attempt in range(BATTLE_DEPARTURE_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(j_key, t_key)
                    await _record_instance_teardown_tx(pipe, match_id, source, j_key, t_key)
            except WatchError:
                continue
            return
        raise errcode.PandoraError(
            errcode.ErrDSAllocationFailed, "battle %d teardown proof CAS retry exhausted", match_id
        )

    async def record_allocation_lifecycle_published(
        self, match_id: int, target: placement.Target
    ) -> None:
        """Go: `RedisBattleRepo.RecordAllocationLifecyclePublished`。"""
        await record_allocation_lifecycle_published(
            self._rdb, self.strict_model_b_writes_enabled(), match_id, target
        )


# ── 混入:分配中止(Go 的 *RedisBattleAuthRepo 方法集)──────────────────────


class BattleAllocationAbortMixin:
    """混入 `battle_auth.RedisBattleAuthRepo`。装配请用 `RedisBattleAbortAuthRepo`。

    ★ Go 把中止能力**刻意**放在 auth repo 而不是 battle repo:能轮换 DS 回调凭据
      的代码不会顺带获得"删分配"的破坏性权限(最小权限)。这里保持同样的边界。
    """

    __slots__ = ()

    async def fence_allocation_abort_expected(
        self, request: battleabort.Request
    ) -> BattleAllocationAbortFenceResult:
        """Go: `RedisBattleAuthRepo.FenceAllocationAbortExpected`。

        **外部 UID 删除之前唯一的线性化点**:原子地把 auth+battle 锁进永久不可路由
        状态,并建立不可变的 operation+instance 台账。记录缺失、终态、任何元组漂移、
        或者已经有玩家准入 —— 四种情况全部 fail-closed。
        """
        if not request.complete():
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "complete battle allocation abort request required"
            )
        a_key = battle_auth_key(request.match_id)
        b_key = battle_key(request.match_id)
        j_key = battle_allocation_abort_key(request.match_id)
        source = _abort_source(request)
        t_key = battle_instance_teardown_key(request.match_id, source)
        l_key = battle_allocation_lifecycle_published_key(request.match_id)

        for _attempt in range(BATTLE_AUTH_CAS_RETRIES):
            result = BattleAllocationAbortFenceResult()
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key, j_key, t_key, l_key)
                    await self._fence_abort_tx(
                        pipe, request, a_key, b_key, j_key, t_key, l_key, result
                    )
            except WatchError:
                continue
            if result.released:
                return result
            if result.battle is None:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "battle %d allocation abort fence result unavailable",
                    request.match_id,
                )
            # 派生 active 索引与权威 key 不同 slot,只能事务外单独写。score=0 让
            # 对账器**下一轮立刻**处理它(§16.10:这不是"等一会儿",是"立即入队")。
            try:
                await self._rdb.zadd(ACTIVE_KEY, {str(request.match_id): 0.0})
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 对齐 Go 的 `return result, err`
                raise BattleAbortActiveIndexError(
                    "battle %d allocation abort active index add failed: %s",
                    request.match_id,
                    exc,
                    cause=exc,
                    fence_result=result,
                ) from exc
            return result
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "battle %d allocation abort fence cas retry exhausted",
            request.match_id,
        )

    async def _fence_abort_tx(  # noqa: ANN001
        self,
        pipe,
        request: battleabort.Request,
        a_key: str,
        b_key: str,
        j_key: str,
        t_key: str,
        l_key: str,
        result: BattleAllocationAbortFenceResult,
    ) -> None:
        """`FenceAllocationAbortExpected` 的 Go 闭包体。"""
        record, found = await _read_allocation_abort_record(pipe, request.match_id, j_key)
        if found:
            if not abort_record_matches_request(record, request):
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "battle %d allocation abort idempotency tuple conflict",
                    request.match_id,
                )
            if record.released_at_ms > 0:
                result.released = True
                return
            terminal = await _allocation_abort_terminal_proofs_match(
                pipe, request, t_key, l_key
            )
            if terminal:
                record.released_at_ms = self._now_ms()
                payload = _marshal_allocation_abort(record)
                pipe.multi()
                _set_px_ms(pipe, j_key, payload, 0)
                await pipe.execute()
                result.released = True
                return
            auth_record, battle = await _read_bound_authority(pipe, request.match_id, a_key, b_key)
            if not allocation_abort_fence_matches(auth_record, battle, request):
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "battle %d allocation abort fence changed",
                    request.match_id,
                )
            result.battle = clone(battle)
            return

        # 台账缺失。一个"空对局终态 worker"可能在 Matchmaker 观察到 placement 冲突
        # 之前就跑完了 —— 只有 exact 永久 UID+Pod teardown proof 与 exact full-target
        # Kafka ACK 标记的**合取**才能关掉这个竞态。终态、TTL 记录缺失、任一单独的
        # 证明都不够。
        terminal = await _allocation_abort_terminal_proofs_match(pipe, request, t_key, l_key)
        if terminal:
            current_ms = self._now_ms()
            record = new_allocation_abort_record(request, current_ms, current_ms)
            payload = _marshal_allocation_abort(record)
            pipe.multi()
            _set_px_ms(pipe, j_key, payload, 0)
            await pipe.execute()
            result.released = True
            return

        auth_record, battle = await _read_bound_authority(pipe, request.match_id, a_key, b_key)
        battle_before = clone(battle)
        expected = BattleExpectedInstance(
            allocation_id=request.target.allocation_id,
            instance_uid=request.target.instance_uid,
            instance_epoch=request.target.instance_epoch,
        )
        if (
            not battle_result_stable_authority_matches(auth_record, battle, expected)
            or battle.state not in ("ready", "running")
            or battle.ds_pod_name != request.target.pod_name
            or battle.release_track != request.target.release_track
            or battle.player_count != 0
        ):
            # ★ `player_count != 0` 是"预准入"三个字的全部含义:已经有玩家进来了
            #   就不再是可以直接拆掉的目标,必须走正常的对局结束链。
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "battle %d is not an exact zero-admission abort target",
                request.match_id,
            )

        current_ms = self._now_ms()
        record = new_allocation_abort_record(request, current_ms, 0)
        journal_payload = _marshal_allocation_abort(record)
        auth_record.phase = dspb.BATTLE_AUTH_PHASE_TERMINATING
        auth_record.ClearField("pending")
        auth_record.pending_started_ms = 0
        auth_record.delivered_rv = ""
        auth_record.updated_at_ms = current_ms
        try:
            auth_payload = auth_record.SerializeToString(deterministic=True)
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "marshal battle ds auth record", cause=exc
            ) from exc
        battle.state = BATTLE_STATE_ALLOCATION_ABORT_PENDING
        # 让派生的 active 索引立刻可被恢复:规范对账器会用这个值修复丢失的跨槽 ZADD。
        battle.last_heartbeat_ms = 0
        battle_payload = self._marshal_battle_transition(battle_before, battle)
        pipe.multi()
        _set_px_ms(pipe, a_key, auth_payload, 0)
        _set_px_ms(pipe, b_key, battle_payload, 0)
        _set_px_ms(pipe, j_key, journal_payload, 0)
        await pipe.execute()
        result.battle = clone(battle)

    async def read_allocation_abort(
        self, match_id: int
    ) -> tuple[battleabort.Request, bool, bool]:
        """Go: `RedisBattleAuthRepo.ReadAllocationAbort`。返回 `(request, released, found)`。

        进程崩溃后由 service 生命周期对账器读取的**永久** operation 台账。
        """
        if match_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "match_id required")
        record, found = await _read_allocation_abort_record(
            self._rdb, match_id, battle_allocation_abort_key(match_id)
        )
        if not found:
            return battleabort.Request(), False, found
        return abort_request_from_record(record), record.released_at_ms > 0, True

    async def complete_allocation_abort_expected(
        self, request: battleabort.Request, auth_ttl_sec: float, battle_ttl_sec: float
    ) -> bool:
        """Go: `RedisBattleAuthRepo.CompleteAllocationAbortExpected`。

        只在 UID 删除与 lifecycle 投递**都**返回明确 ACK 之后调用:先把永久台账
        标 RELEASED,然后才给 auth/battle 有界的审计 TTL。

        ★ Go 的 `authTTL, battleTTL time.Duration` 在 Python 里表达成**秒**
          (`float`),与 `repo.py` / `battle_auth.py` 既有的 `*_ttl_sec` 一致。
          写入时按 go-redis 的编码分别用 `EXPIRE`(整秒)和 `SET ... PX`。
        """
        if not request.complete() or auth_ttl_sec <= 0 or battle_ttl_sec <= 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "complete battle allocation abort and positive retention required",
            )
        a_key = battle_auth_key(request.match_id)
        b_key = battle_key(request.match_id)
        j_key = battle_allocation_abort_key(request.match_id)
        source = _abort_source(request)
        t_key = battle_instance_teardown_key(request.match_id, source)
        l_key = battle_allocation_lifecycle_published_key(request.match_id)

        for _attempt in range(BATTLE_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key, j_key, t_key, l_key)
                    completed = await self._complete_abort_tx(
                        pipe, request, a_key, b_key, j_key, t_key, l_key,
                        auth_ttl_sec, battle_ttl_sec,
                    )
            except WatchError:
                continue
            if not completed:
                return False
            # 权威已终态,派生索引跨槽单独移除;失败也要告诉调用方"已完成"
            # (Go 的 `return true, err`)—— 残留由规范对账器清。
            try:
                await self._rdb.zrem(ACTIVE_KEY, str(request.match_id))
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 对齐 Go 的 `return true, err`
                raise BattleAbortActiveIndexError(
                    "battle %d allocation abort active index remove failed: %s",
                    request.match_id,
                    exc,
                    cause=exc,
                ) from exc
            return True
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "battle %d allocation abort completion cas retry exhausted",
            request.match_id,
        )

    async def _complete_abort_tx(  # noqa: ANN001
        self,
        pipe,
        request: battleabort.Request,
        a_key: str,
        b_key: str,
        j_key: str,
        t_key: str,
        l_key: str,
        auth_ttl_sec: float,
        battle_ttl_sec: float,
    ) -> bool:
        """`CompleteAllocationAbortExpected` 的 Go 闭包体。返回 `completed`。"""
        record, found = await _read_allocation_abort_record(pipe, request.match_id, j_key)
        if not found or not abort_record_matches_request(record, request):
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "battle %d allocation abort journal missing or changed",
                request.match_id,
            )
        if record.released_at_ms > 0:
            # RELEASED 是**外部效果**的持久 ACK,不代表本地保留期清理也提交了。
            # 重试认出永久 teardown+lifecycle 证明之后、auth/battle 拿到有界 TTL
            # 之前可能崩过。逐个 exact key 独立对账:缺失即已干净,而**不匹配的
            # key 一律不碰**(它可能属于更晚的一次权威化身)。
            auth_raw = await pipe.get(a_key)
            battle_raw = await pipe.get(b_key)
            auth_record = None
            if auth_raw is not None:
                auth_record = dspb.BattleDSAuthStorageRecord()
                unmarshal_battle_auth(request.match_id, auth_raw, auth_record)
            battle = None
            if battle_raw is not None:
                battle = unmarshal_battle(request.match_id, battle_raw)
            auth_exact = auth_record is not None and allocation_abort_released_auth_matches(
                auth_record, request
            )
            battle_exact = battle is not None and allocation_abort_released_battle_matches(
                battle, request
            )
            if (auth_record is not None and not auth_exact) or (
                battle is not None and not battle_exact
            ):
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "battle %d released abort found a different remaining authority",
                    request.match_id,
                )
            if battle_exact and self.strict_model_b_writes_enabled():
                try:
                    validate_battle_storage_transition(battle, battle)
                except errcode.PandoraError as exc:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidState,
                        "battle %d released abort storage invariant failed",
                        request.match_id,
                        cause=exc,
                    ) from exc
            if auth_exact or battle_exact:
                pipe.multi()
                if auth_exact:
                    pipe.expire(a_key, _format_sec(auth_ttl_sec))
                if battle_exact:
                    # ★ 回写的是**原始字节** battle_raw,不是重新序列化的对象:
                    #   滚动升级期本副本读不懂的 unknown fields 必须原样搬运
                    #   (§9 不变量 17),重新 Marshal 会把它们悄悄抹掉。
                    _set(pipe, b_key, battle_raw, battle_ttl_sec)
                await pipe.execute()
            return True
        terminal = await _allocation_abort_terminal_proofs_match(pipe, request, t_key, l_key)
        if not terminal:
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "battle %d allocation abort completion lacks teardown/lifecycle proofs",
                request.match_id,
            )
        auth_record, battle = await _read_bound_authority(pipe, request.match_id, a_key, b_key)
        battle_before = clone(battle)
        if not allocation_abort_completion_authority_matches(auth_record, battle, request):
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "battle %d allocation abort fence changed before completion",
                request.match_id,
            )
        # ★ 两把权威 key 必须**仍然永久**(PTTL == -1)。已经带上 TTL 说明有人
        #   给它们收过口 —— 那就不是本次中止的现场,继续写会覆盖别人的收尾。
        a_ttl = int(await pipe.pttl(a_key))
        if a_ttl != -1:
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "battle %d abort auth fence is not permanent", request.match_id
            )
        b_ttl = int(await pipe.pttl(b_key))
        if b_ttl != -1:
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "battle %d abort battle fence is not permanent",
                request.match_id,
            )

        current_ms = self._now_ms()
        record.released_at_ms = current_ms
        journal_payload = _marshal_allocation_abort(record)
        battle.state = "abandoned"
        battle_payload = self._marshal_battle_transition(battle_before, battle)
        pipe.multi()
        _set_px_ms(pipe, j_key, journal_payload, 0)
        _set(pipe, b_key, battle_payload, battle_ttl_sec)
        pipe.expire(a_key, _format_sec(auth_ttl_sec))
        await pipe.execute()
        return True

    async def record_allocation_lifecycle_published(
        self, match_id: int, target: placement.Target
    ) -> None:
        """Go: `RedisBattleAuthRepo.RecordAllocationLifecyclePublished`。

        与 `RedisBattleRepo` 上的同名方法委托到同一个自由函数 —— Go 也是这样
        把同一份最小能力暴露给两个 repo 视图,而不是让调用方为了写这份证明
        拿到更大的变更面。
        """
        await record_allocation_lifecycle_published(
            self._rdb, self.strict_model_b_writes_enabled(), match_id, target
        )


# ── 装配用的组合类 ──────────────────────────────────────────────────────────
#
# Go 侧 `RedisBattleRepo` / `RedisBattleAuthRepo` 各自是**一个**类型,方法散在
# 多个文件里。Python 没有"给已有类补方法"的等价物,又不能让 `repo.py` /
# `battle_auth.py` 反向 import 本模块(成环),所以用 mixin + 组合类收口。
#
# ★ main / wire 一律装配下面两个类,不要直接 new `repo.RedisBattleRepo` ——
#   后者能跑,但少了离场与生命周期证明能力,而缺失表现为 `AttributeError`
#   发生在**第一次真的有玩家要离场**的时候,不是启动时。


class RedisBattleDepartureRepo(BattleDepartureMixin, _RedisBattleRepo):
    """`repo.RedisBattleRepo` + 离场 / teardown / lifecycle 证明。"""

    __slots__ = ()


class RedisBattleAbortAuthRepo(BattleAllocationAbortMixin, _RedisBattleAuthRepo):
    """`battle_auth.RedisBattleAuthRepo` + 分配中止 / lifecycle 证明。"""

    __slots__ = ()
