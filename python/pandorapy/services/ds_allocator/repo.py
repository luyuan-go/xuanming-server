"""ds_allocator 对局仓储层 —— 对应 Go 侧
`services/battle/ds_allocator/internal/data/` 的五个文件:

    battle.go                      主体(claim / fence / finalize / CRUD / active 索引)
    allocation_ledger.go           分配台账(孤儿 GS 清扫的 fail-closed 依据)
    noshow_recorder.go             no-show 记账 → 进入侧退避
    battle_storage_invariant.go    严格镜像的写入侧不变量
    battle_active_reconciler.go    active ZSET 的全 master 重建

Redis key 模板(**逐字符照抄,一个字符都不能改**):

    pandora:ds:battle:{<match_id>}   → BattleStorageRecord proto bytes,TTL=battle_ttl
    pandora:ds:auth:{<match_id>}     → 授权权威(本模块只 EXISTS 探测,不读写)
    pandora:ds:active               → ZSET(score=last_heartbeat_ms,member=match_id)
    pandora:ds:allocation_ledger    → ZSET(member=allocation_id,score=ms)

★ **两栈并存期 key 必须完全一致**。Go 副本和 Python 副本会同时读写同一份对局镜像:
key 差一个字符 = 两边各自维护一份"权威",双方都判定 SETNX 成功,**没有任何运行期
信号**,而同一个 match 会被分配两台 Battle DS(§9 不变量 1)。所以本文件的 key 模板
不是实现细节,是跨语言硬契约。

★ `{match_id}` 是 Redis Cluster hashtag,不是格式化占位符。它让 battle / auth /
  journal 三把键落同一 slot,"授权与镜像同事务"才成立;而全局 `pandora:ds:active`
  与 `pandora:ds:allocation_ledger` **不带 hashtag**,与 battle key 必然异 slot ——
  所以 Go 把它们从事务里搬了出去(独立幂等命令),本文件照搬这条拆分,不得"顺手
  合并进一个 EXEC"(单机 Redis 上看不出问题,上 Cluster 才 CROSSSLOT)。

★ read-modify-write 一律用标准 `ParseFromString`(**不** DiscardUnknown,§9 不变量 17):
  滚动升级期新副本写入的新字段必须被旧副本原样读出并写回,否则旧副本每回写一次
  就把新字段静默抹掉一批。

────────────────────────────────────────────────────────────────────────────
★ 本模块**没有** source_revision / writer_token 闸

`BattleStorageRecord`(proto/pandora/ds/v1/allocator.proto:250)只有 26 个字段,
既没有 `source_revision` 也没有 `writer_token` —— 那两道闸属于 hub_allocator 的
**每玩家归属**面(`pkg/placement/source_revision.go` / `hub_allocator/writer_fence.go`)。
battle 面的 fencing 走的是另一套:`allocation_id`(UUIDv4)+ exact
`gameserver_uid`/`pod_uid` 三元组 + 状态机(见下)。移植时**不得**凭"其它 repo 有
所以这里也该有"补一道并不存在的闸 —— 那会让 Python 侧拒掉 Go 侧合法的写。

────────────────────────────────────────────────────────────────────────────
★ 状态机:五个"永不过期"的墓碑态

`state` 在 proto 里是 **string** 字段(不是 enum),所以下面的常量只能是字面量,
必须与 Go 的 `const` 逐字节相同。它们全部**刻意不可路由、不带 TTL**:

    allocation_uncertain
        GSA POST 已发出但结果未知。旧 writer 遇到它必须 fail-closed,
        **不得**靠 TTL / sweep / 幂等重试把它当"未分配"后再 POST 一次。
    allocation_reconcile_release_pending
        allocation_id 查到了唯一 exact GameServer+Pod,并在首次 DELETE **之前**
        持久化。重启只会续做这一次精确 release,永远不会再 POST。
    allocation_reconcile_empty_tombstone
        首轮对账没看到 GameServer,但超时的 POST 仍可能事后生效 ——
        所以留在 active 恢复索引里反复 DeleteCollection+LIST。
    preactive_release_pending
        已确认 UID 的未激活分配正在回收;UID 条件删除被确认成功前不许物理 purge。
    allocation_abort_pending
        Matchmaker 签名 abort 路径写下的永久 fence。

这五个状态**带 TTL 就是 bug**:墓碑过期 = 同一个 match 被允许发第二次 GSA POST。
`FenceAllocationUncertainRelease` / `CompleteAllocationUncertainRelease` /
`MarkAllocationUncertainEmptyLifecyclePublished` 里的 `pttl != -1 → 报错` 就是
这条不变量的机械检查,不能"顺手放宽成 pttl > 0 才报错"。
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import re
import unicodedata
import uuid as _uuid

from google.protobuf import unknown_fields as _unknown_fields
from pandora.ds.v1 import allocator_pb2 as dspb
from redis.exceptions import WatchError

from pandorapy import errcode, redisx, releasetrack

# ── key 模板 ─────────────────────────────────────────────────────────────────

ACTIVE_KEY = "pandora:ds:active"

# allocation_ledger.go: 台账 ZSET(member=allocation_id,score=ms)。
ALLOCATION_LEDGER_KEY = "pandora:ds:allocation_ledger"

# battle_active_reconciler.go: SCAN 用的权威键模式。`{*}` 里的花括号是 **字面量**
# (Redis 的 glob 不认 hashtag),`*` 才是通配符。写成 `pandora:ds:battle:*` 也能扫到,
# 但会连未来任何 `pandora:ds:battle:xxx` 的非权威键一起吞进来并因解析失败整轮中止。
BATTLE_KEY_SCAN_PATTERN = "pandora:ds:battle:{*}"

# 严格 fenced finalize 的 read-back 独立预算(秒)。对应 Go 的
# `fencedFinalizeReadbackTimeout = 3 * time.Second`。
FENCED_FINALIZE_READBACK_TIMEOUT_SEC = 3.0

# WATCH 冲突重试预算。Go 写的是 `for attempt := 0; attempt <= 3; attempt++`,
# 即**四次**尝试(0/1/2/3),不是三次 —— 抄成 range(3) 会让高并发下的
# "耗尽"提前一轮触发,而那条错误码是 fail-closed 的终态。
BATTLE_CAS_ATTEMPTS = 4

# ── 状态字面量(proto 里 state 是 string,不是 enum;必须与 Go const 逐字节相同)──

BATTLE_STATE_ALLOCATION_UNCERTAIN = "allocation_uncertain"
BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING = "allocation_reconcile_release_pending"
BATTLE_STATE_ALLOCATION_RECONCILE_EMPTY_TOMBSTONE = "allocation_reconcile_empty_tombstone"
BATTLE_STATE_PREACTIVE_RELEASE_PENDING = "preactive_release_pending"
BATTLE_STATE_ALLOCATION_ABORT_PENDING = "allocation_abort_pending"

# `DeleteBattleIfAllocationMatches` 允许删除的三个"已知可回收"状态。
# ★ 白名单而不是黑名单:未来新增的任何状态默认**不可删**(fail-closed)。
#   反过来写(排除 allocation_uncertain)会让下一个加状态的人默认拿到删除权。
DELETABLE_BATTLE_STATES = ("allocating", "warming", "abandoned")

# uint64 / int64 边界。★ Go 的类型系统天然挡住越界,Python int 无限精度 ——
# 不显式判就会把一个 2^70 的 match_id 拼进 key(合法字符串!),
# 于是"同一场对局"在 Go 侧根本不可能存在,两栈的镜像永久对不上。
UINT64_MAX = (1 << 64) - 1
INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1

# ★ `\A...\Z` 而不是 `^...$`:Python 的 `$` 也匹配**末尾换行**,`"123\n"` 会被判成
# 合法十进制;Go 的 `strconv.ParseUint` 不接受任何空白 / 正负号 / 前后缀。
# active ZSET 的成员是可被写入方影响的字节串,这条差异是可注入的。
_DECIMAL_RE = re.compile(r"\A[0-9]+\Z")


class BattleDataError(errcode.PandoraError):
    """对应 Go 侧本包里的裸 `fmt.Errorf(...)`。

    ★ 码必须是 `ErrUnknown`(=1) 而不是看起来更"正确"的 `ErrInternal`(=2):
      Go 的 `fmt.Errorf` 不带 errcode,`errcode.As(err)` 沿 Unwrap 链找不到
      *errcode.Error 时**回落 ErrUnknown**。service 层把这个码原样翻成 proto
      ErrCode 发给调用方 —— 挑一个"更合适"的码 = 同一份故障在两栈上呈现成
      两种语义,而两边都不报错。
    """

    __slots__ = ()

    def __init__(self, msg: str = "", *args: object, cause: BaseException | None = None) -> None:
        super().__init__(errcode.ErrUnknown, msg, *args, cause=cause)


class BattleActiveIndexError(BattleDataError):
    """权威删除已成功、但派生 active 索引清理失败。

    ★ 这是 Go 的 `return true, err` —— 一个 Python 异常天然表达不了的返回形状。
      `DeleteBattleIfAllocationMatches` 的 `deleted=true` 是**释放对应 GameServer
      的权利**:调用方丢了它就不会去 Release,那台 14Gi 的 Pod 会一直挂着,而
      权威 key 已经删了、再也没人能证明它属于谁(孤儿清扫的台账门也救不了 ——
      台账只证明"曾分配过",不授权删除)。

      所以证据挂在**声明式 `__slots__`** 上,不用 `setattr`:后者能写进去
      (Exception 自带 `__dict__`),但拼错一个字母不会报错,读的那侧只会永远
      拿到默认值 —— 与"根本没传"在日志里长得一模一样。
    """

    __slots__ = ("deleted",)

    def __init__(self, msg: str = "", *args: object, cause: BaseException | None = None) -> None:
        super().__init__(msg, *args, cause=cause)
        # 权威 key 已按 fencing 条件删除;调用方**必须**据此执行 Release。
        self.deleted: bool = True


def _require_uint64(name: str, value: int) -> int:
    """把外部传入的 ID 钳在 uint64 内。

    ★ 这道判据在 Go 里由 `uint64` 类型免费提供;Python 没有,不判就会:
      - 负数拼出 `pandora:ds:battle:{-1}` —— 一个 Go 侧永远写不出的 key;
      - 超 uint64 的值赋给 proto `uint64` 字段时才抛 ValueError,
        而那已经是**写了一半**之后的事(active 索引可能已经登记了)。
    """
    if not isinstance(value, int) or isinstance(value, bool):
        raise errcode.PandoraError(errcode.ErrInvalidArg, "%s must be an integer", name)
    if value < 0 or value > UINT64_MAX:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "%s %d out of uint64 range", name, value
        )
    return value


def _require_int64(name: str, value: int) -> int:
    """int64 边界(毫秒时间戳 / 阈值)。

    ★ Go 的 `strconv.FormatInt(thresholdMs, 10)` 只接受 int64;Python 不判就会把
      一个 2^80 的阈值格式化进 ZRANGEBYSCORE 的 max —— Redis 按 double 解析,
      精度悄悄丢失,扫出来的"超时对局"集合与 Go 侧不同。
    """
    if not isinstance(value, int) or isinstance(value, bool):
        raise errcode.PandoraError(errcode.ErrInvalidArg, "%s must be an integer", name)
    if value < INT64_MIN or value > INT64_MAX:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "%s %d out of int64 range", name, value)
    return value


def battle_key(match_id: int) -> str:
    """`pandora:ds:battle:{<match_id>}`。花括号是 Cluster hashtag,不是占位符。"""
    return f"pandora:ds:battle:{{{_require_uint64('match_id', match_id)}}}"


def battle_auth_key(match_id: int) -> str:
    """`pandora:ds:auth:{<match_id>}` —— 与 battle key 同 slot 的授权权威。

    ★ 权威实现在 Go 的 `battle_auth.go`(本次移植范围之外)。这里之所以要有一份,
      是因为三个 uncertain 对账入口都必须**先证明授权尚未建立**才敢动镜像:
      一旦 PrepareCredential 已经建档,生命周期就不再归这个"准入前对账器"管,
      它继续推进状态 = 把一台已经在服务玩家的 DS 判成待回收。

      移植 battle_auth.py 时**务必**改成从那里 import,不要留两份字面量 ——
      key 拼写漂移在这里的表现是 EXISTS 恒为 0,即"授权检查恒通过"。
    """
    return f"pandora:ds:auth:{{{_require_uint64('match_id', match_id)}}}"


def _px(ttl_sec: float | None) -> dict:
    """把秒 TTL 翻成 redis-py 的 `px=` kwargs。`<=0` = 无过期(对齐 Go 的 `0`)。"""
    if ttl_sec is None or ttl_sec <= 0:
        return {}
    return {"px": int(ttl_sec * 1000)}


def _format_sec(ttl_sec: float) -> int:
    """秒 TTL → EXPIRE 的整秒参数。**逐条照抄 go-redis 的 `formatSec`**。

    ★ `0 < d < 1s` 时 go-redis 会**抬成 1 秒**(并打一条 warning),不是截成 0。
      Python 直接 `int()` 会得到 0 —— 而 `EXPIRE key 0` 是**立即删除**:
      `ExpireBattle` 的语义("改短 TTL,终态保留供查询")当场反转成"抹掉终态",
      查询侧看到的是 key miss,而 miss 在 sweep 里等价于"已释放"。
    """
    if 0 < ttl_sec < 1:
        return 1
    return int(ttl_sec)  # Go 的 int64(dur/time.Second) 同样朝零截断


def _quote_go(value: str) -> str:
    """近似 Go 的 `%q`(双引号 + 转义)。

    只用在 `active bad match_id %q` 这一条错误消息上,为的是两栈日志能被同一条
    Loki 查询命中。用 `json.dumps` 而不是 Python 的 `repr`:后者是**单**引号,
    与 Go 的输出不同。
    """
    return json.dumps(value, ensure_ascii=False)


def parse_ids(vals) -> list[int]:  # noqa: ANN001
    """严格解析 active ZSET 的成员。对应 Go 的 `parseIDs`。

    ★ 解析失败**整轮报错**而不是跳过(与 hub 侧的"脏成员跳过"刻意相反):
      这里的返回值直接决定"哪些对局要被判超时 / 被回收",少一个就是漏扫,
      而漏扫在 §9 不变量 4 下等价于"崩溃的 DS 永远不补偿"。Go 也是 return err。
    """
    out: list[int] = []
    for raw in vals:
        text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
        if not _DECIMAL_RE.fullmatch(text):
            raise BattleDataError("active bad match_id %s: invalid syntax", _quote_go(text))
        value = int(text)
        if value > UINT64_MAX:
            raise BattleDataError("active bad match_id %s: value out of range", _quote_go(text))
        out.append(value)
    return out


# ── proto 辅助 ───────────────────────────────────────────────────────────────


def clone(msg):  # noqa: ANN001
    """深拷贝一条 proto。对应 Go 的 `proto.Clone`。

    ★ 必须用 `CopyFrom` 而不是 Python 的赋值:proto message 是引用语义,
    `a = b` 之后改 a 就是改 b。Go 侧同样禁止值拷贝(§5 第 10 条)。
    `CopyFrom` 会连 unknown fields 一起带走(upb 实测成立),这是 §9 不变量 17
    要求的 —— 克隆丢 unknown = 回写时把新副本的字段抹掉。
    """
    out = type(msg)()
    out.CopyFrom(msg)
    return out


def proto_equal(a, b) -> bool:  # noqa: ANN001
    """完整相等(**含 unknown fields**)。对应 Go 的 `proto.Equal`。

    ★ 为什么不直接用 `a == b`:CPython 的 protobuf 有 upb / pure-Python 两套实现,
    只有 upb 的 `__eq__` 覆盖 unknown fields。`confirmPersistentFencedFinalize`
    的严格 read-back 一旦漏掉 unknown,滚动升级期就会出现:新副本写了新字段 →
    本副本读出来(unknown)→ 与 expected "相等" → 把这次提交认成自己的,
    而它其实是另一次分配写下的。用确定性序列化比较则与实现无关。
    """
    if a is None or b is None:
        return a is b
    if type(a) is not type(b):
        return False
    return a.SerializeToString(deterministic=True) == b.SerializeToString(
        deterministic=True
    )


def _encode_varint(value: int) -> bytes:
    # unknown 里的 varint 语义上是 uint64;Python int 无限精度,必须显式截到 64 位,
    # 否则一个来自 sint64 负数的 unknown 会编出一串 Go 永远写不出的字节。
    value &= UINT64_MAX
    out = bytearray()
    while True:
        chunk = value & 0x7F
        value >>= 7
        if value:
            out.append(chunk | 0x80)
        else:
            out.append(chunk)
            return bytes(out)


def _encode_unknown_set(field_set) -> bytes:  # noqa: ANN001
    out = bytearray()
    for field in field_set:
        tag = (field.field_number << 3) | field.wire_type
        out += _encode_varint(tag)
        if field.wire_type == 0:
            out += _encode_varint(int(field.data))
        elif field.wire_type == 1:
            out += int(field.data).to_bytes(8, "little")
        elif field.wire_type == 2:
            out += _encode_varint(len(field.data))
            out += field.data
        elif field.wire_type == 3:  # start group:递归 + 补 end group tag
            out += _encode_unknown_set(field.data)
            out += _encode_varint((field.field_number << 3) | 4)
        elif field.wire_type == 5:
            out += int(field.data).to_bytes(4, "little")
        else:
            raise BattleDataError(
                "unsupported protobuf unknown wire type %d", field.wire_type
            )
    return bytes(out)


def unknown_bytes(msg) -> bytes:  # noqa: ANN001
    """顶层 unknown fields 的规范化字节。对应 Go 的 `ProtoReflect().GetUnknown()`。

    ★ Python 的 protobuf **没有** `SetUnknown`/`GetUnknown`,只给了只读的
      `UnknownFieldSet`。所以这里按 wire format 重新编码一遍(确定性:字段按
      解析顺序、每项按其 wire type 定长编码)。

      为什么不用"整体序列化减去去 unknown 后的序列化"这条更短的路:那依赖
      "unknown 一定被追加在末尾"这个**未写进文档的布局假设**。实测(upb 7.35)
      确实成立,但它是实现细节,换个 protobuf 实现就可能不成立,而失败形态是
      `validateBattleStorageTransition` 的 unknown 相等判定恒为 false ——
      滚动升级期**所有**状态迁移被拒,DS 一台都分不出去。
    """
    return _encode_unknown_set(_unknown_fields.UnknownFieldSet(msg))


def has_unknown_fields(msg) -> bool:  # noqa: ANN001
    """顶层是否带 unknown fields。对应 Go 的 `len(...GetUnknown()) != 0`。

    ★ 只看**顶层**(与 Go 一致)。Go 的 `GetUnknown()` 同样只返回本 message 的
      unknown,嵌套子消息里的不算。
    """
    return len(_unknown_fields.UnknownFieldSet(msg)) != 0


def set_unknown_bytes(msg, raw: bytes) -> None:  # noqa: ANN001
    """把一段 unknown 字节装回 message。对应 Go 的 `ProtoReflect().SetUnknown(raw)`。

    实现:先清空,再 `MergeFromString(raw)` —— raw 里的字段号按定义就是本 message
    类型不认识的,所以解析后必然重新落进 unknown 集合(upb 实测 round-trip 等价)。

    ★ 与 Go 的一处**已知窄边差异**:`DiscardUnknownFields()` 是**递归**的,会连
      `player_combat_factions` 等子消息里的 unknown 一起清掉,而 Go 的
      `SetUnknown` 只动顶层。本模块所有调用点的目标都是"刚 clone 出来的调用方
      快照"(子消息由本副本构造,不可能带 unknown),故实际行为一致;移植
      battle_departure 等含深层嵌套的结构时**必须**重新评估这一点。
    """
    msg.DiscardUnknownFields()
    if raw:
        msg.MergeFromString(raw)


def unmarshal_battle(match_id: int, payload: bytes):
    """反序列化对局镜像并校验 match 身份。对应 Go 的 `unmarshalBattle`。

    ★ 两条判据的**顺序**是契约:
        match_id == 0 → 用 key 里的 id 补齐(兼容早期未写该字段的记录);
        补齐后仍不等 → **报错**,不是"以 key 为准改掉它"。
      记录内的 id 与 key 不符说明有人按错 key 写了镜像;此时无论信哪一边,
      另一边的账本都会错 —— 只能拒,让上层去查。
    """
    rec = dspb.BattleStorageRecord()
    try:
        # ★ 标准解析:保留 unknown fields(§9 不变量 17)。
        rec.ParseFromString(payload)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001 —— 解码失败一律归为存储损坏
        raise BattleDataError(
            "battle %d bad proto: %s", match_id, exc, cause=exc
        ) from exc
    if rec.match_id == 0:
        rec.match_id = match_id
    if rec.match_id != match_id:
        raise BattleDataError("battle %d id mismatch: %d", match_id, rec.match_id)
    return rec


# ── 写入侧不变量(battle_storage_invariant.go)────────────────────────────────


def canonical_battle_allocation_id(value: str) -> bool:
    """allocation_id 必须是**规范小写 36 字符的 UUIDv4**。对应 Go 的
    `canonicalBattleAllocationID`。

    四条判据缺一不可(顺序无所谓,少一条都有洞):
        解析得出来   非 UUID 直接拒;
        != Nil       全零 UUID 是"没有 allocation"的伪装,它会让 fencing 比较
                     在两个不同的分配之间意外相等;
        version == 4 v1/v5 可由 MAC / 名字推导 —— allocation_id 同时是 GameServer
                     label 值,可推导 = 可被外部构造出一个"看起来是我们发的"的 id;
        Variant RFC4122 + `str(parsed) == value`
                     强制**规范形态**:Python 的 `uuid.UUID()` 与 Go 的 `uuid.Parse()`
                     都接受 `{...}` / `urn:uuid:` / 32 位无横线 / 大写等多种写法,
                     不做回写比较的话,同一个分配会有 5 种字符串形式,
                     而 Redis 里的相等比较是**字节比较**。
    """
    if not isinstance(value, str) or value == "":
        return False
    try:
        parsed = _uuid.UUID(value)
    except (ValueError, AttributeError, TypeError):
        return False
    return (
        parsed.int != 0
        and parsed.variant == _uuid.RFC_4122
        and parsed.version == 4
        and str(parsed) == value
    )


def canonical_battle_identity_value(value: str) -> bool:
    """pod 名 / UID 这类 exact 身份串的规范性。对应 Go 的 `canonicalBattleIdentityValue`。

    ★ 两处 Go/Python 的字符类差异,这里刻意用"并集"抹平:
      - `str.strip()` 比 Go 的 `strings.TrimSpace` 多剥 U+001C..U+001F;
      - `str.isspace()` 同样比 `unicode.IsSpace` 多认这几个字符。
      但它们全部属于 Cc 控制字符,第二条判据在两栈都会拒 —— 所以"是否规范"的
      **结论**一致。控制字符判定用 `category == "Cc"`(正是 Go `unicode.IsControl`
      的定义域),不能写成 `category[0] == "C"`:那会把 Cf(如 U+00AD 软连字符)
      也算进去,而 Go 不算,于是 Go 写得进的记录 Python 读出来判非法。
    """
    if not isinstance(value, str) or value == "":
        return False
    if value.strip() != value:
        return False
    return not any(ch.isspace() or unicodedata.category(ch) == "Cc" for ch in value)


def battle_physical_identity_empty(record) -> bool:  # noqa: ANN001
    """记录是否**完全不含**物理 GameServer 身份。对应 Go 的 `battlePhysicalIdentityEmpty`。

    六个字段必须**同时**为空:少判一个(尤其 `ds_addr`)就等于允许"半个身份"落盘,
    而半个身份既不能用来精确 release、又会让下一个 writer 以为分配已完成。
    """
    return (
        record.ds_pod_name == ""
        and record.ds_addr == ""
        and record.gameserver_uid == ""
        and record.pod_uid == ""
        and record.release_track == ""
        and record.instance_epoch == 0
    )


def validate_exact_battle_physical_identity(record) -> None:  # noqa: ANN001
    """完整 ABA-safe 四元组校验。对应 Go 的 `validateExactBattlePhysicalIdentity`。

    ★ `pod_uid` 不是冗余:GameServer DELETE 返回 2xx 只代表"删除已受理",
      同名 Pod 可以在此之后被重建。没有 pod_uid 就没法证明"我等的那个物理进程
      真的没了",于是回收判定退化成按名字比对 —— 这正是 ABA。
    """
    if not canonical_battle_identity_value(record.ds_pod_name):
        raise BattleDataError(
            "battle state %s requires canonical ds_pod_name", _quote_go(record.state)
        )
    if not canonical_battle_identity_value(record.gameserver_uid):
        raise BattleDataError(
            "battle state %s requires canonical gameserver_uid", _quote_go(record.state)
        )
    if not canonical_battle_identity_value(record.pod_uid):
        raise BattleDataError(
            "battle state %s requires canonical pod_uid", _quote_go(record.state)
        )
    if not releasetrack.valid(record.release_track):
        raise BattleDataError(
            "battle state %s requires canonical release_track", _quote_go(record.state)
        )


def validate_battle_storage_write(record) -> None:  # noqa: ANN001
    """严格镜像的写入侧闸。对应 Go 的 `validateBattleStorageWrite`。

    三档状态、三种要求:
        allocating / allocation_uncertain / ..._empty_tombstone
            **禁止**携带任何物理身份。uncertain 尤其如此:它的语义就是"还不知道
            对面是谁",带上半个身份会让对账器以为已经查到了 exact 目标。
        abandoned
            要么完全没有身份(空分配的终态),要么必须是**完整**四元组 ——
            不允许中间形态。
        其余(warming / ready / running / ended / 三个 pending)
            必须是完整四元组。
    未知状态一律报错(fail-closed):不认识的状态说明写者比本副本新,
    此时"放行"= 让一个本副本无法正确解释的记录进入权威面。
    """
    if record is None:
        raise BattleDataError("nil battle storage record")
    if record.match_id == 0 or not canonical_battle_allocation_id(record.allocation_id):
        raise BattleDataError(
            "battle storage record requires match_id and canonical UUIDv4 allocation_id"
        )
    if has_unknown_fields(record):
        raise BattleDataError("new battle storage record cannot contain protobuf unknown fields")

    state = record.state
    if state in (
        "allocating",
        BATTLE_STATE_ALLOCATION_UNCERTAIN,
        BATTLE_STATE_ALLOCATION_RECONCILE_EMPTY_TOMBSTONE,
    ):
        if not battle_physical_identity_empty(record):
            raise BattleDataError(
                "battle state %s cannot carry physical GameServer identity", _quote_go(state)
            )
    elif state == "abandoned":
        if not battle_physical_identity_empty(record):
            validate_exact_battle_physical_identity(record)
    elif state in (
        "warming",
        "ready",
        "running",
        "ended",
        BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING,
        BATTLE_STATE_PREACTIVE_RELEASE_PENDING,
        BATTLE_STATE_ALLOCATION_ABORT_PENDING,
    ):
        validate_exact_battle_physical_identity(record)
    else:
        raise BattleDataError(
            "battle storage record has unsupported state %s", _quote_go(state)
        )


def validate_existing_battle_storage_shape(record) -> None:  # noqa: ANN001
    """对**已存在**记录跑同一套形状校验,但豁免 unknown fields。

    对应 Go 的 `validateExistingBattleStorageShape`。存量记录带 unknown 是滚动
    升级的正常现象(新副本写的新字段),不能因此判它"不可写"。
    """
    if record is None:
        raise BattleDataError("nil battle storage record")
    known = clone(record)
    set_unknown_bytes(known, b"")
    validate_battle_storage_write(known)


def legacy_battle_missing_pod_uid(record) -> bool:  # noqa: ANN001
    """识别唯一一种"可读但一般不可写"的滚动升级形态:pod_uid 落盘之前的旧记录。

    对应 Go 的 `legacyBattleMissingPodUID`。它唯一合法的写入是**同记录精确回填
    pod_uid**(见 `validate_battle_storage_transition`)。
    """
    if (
        record is None
        or record.pod_uid != ""
        or not canonical_battle_identity_value(record.ds_pod_name)
        or not canonical_battle_identity_value(record.gameserver_uid)
        or not releasetrack.valid(record.release_track)
    ):
        return False
    return record.state in (
        "warming",
        "ready",
        "running",
        "ended",
        "abandoned",
        BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING,
        BATTLE_STATE_PREACTIVE_RELEASE_PENDING,
        BATTLE_STATE_ALLOCATION_ABORT_PENDING,
    )


def validate_battle_storage_transition(previous, next_rec) -> None:  # noqa: ANN001
    """状态迁移闸。对应 Go 的 `validateBattleStorageTransition`。

    ★ 三条 immutable 判据都是 fencing 的一部分:
        match_id / allocation_id 不得变  —— 变了就不是同一次分配,CAS 的前提消失;
        pod_uid 一旦非空不得变          —— 变了就是 ABA(同名 Pod 重建);
        unknown fields 不得变           —— §9 不变量 17:本副本不认识的字段只能
                                          原样搬运,改了 = 把新副本的数据改坏。

    ★ legacy 回填分支必须**先于**常规分支:旧记录跑常规分支会被
      `validateExistingBattleStorageShape` 判"缺 pod_uid 不可写",于是存量对局
      一步也走不动(既不能推进也不能回收)。
    """
    if next_rec is None:
        raise BattleDataError("nil next battle storage record")
    if previous is None:
        validate_battle_storage_write(next_rec)
        return
    if previous.match_id != next_rec.match_id or previous.allocation_id != next_rec.allocation_id:
        raise BattleDataError("battle storage transition changed match/allocation identity")

    if legacy_battle_missing_pod_uid(previous):
        if not canonical_battle_identity_value(next_rec.pod_uid):
            raise BattleDataError("legacy battle physical identity may only backfill pod_uid")
        if unknown_bytes(previous) != unknown_bytes(next_rec):
            raise BattleDataError("legacy battle pod_uid backfill changed protobuf unknown bytes")
        without_pod_uid = clone(next_rec)
        without_pod_uid.pod_uid = ""
        if not proto_equal(previous, without_pod_uid):
            raise BattleDataError("legacy battle pod_uid backfill changed another field")
        validate_existing_battle_storage_shape(next_rec)
        return

    try:
        validate_existing_battle_storage_shape(previous)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001 —— 对齐 Go 的 %w 包装
        raise BattleDataError(
            "unsafe existing battle storage record is not writable: %s", exc, cause=exc
        ) from exc
    if previous.pod_uid != "" and previous.pod_uid != next_rec.pod_uid:
        raise BattleDataError("battle storage transition changed immutable pod_uid")
    if unknown_bytes(previous) != unknown_bytes(next_rec):
        raise BattleDataError("battle storage transition changed protobuf unknown fields")
    validate_existing_battle_storage_shape(next_rec)


def marshal_battle(record) -> bytes:  # noqa: ANN001
    """严格档的新建序列化。对应 Go 的 `marshalBattle`。"""
    validate_battle_storage_write(record)
    return record.SerializeToString()


def marshal_battle_transition(previous, next_rec) -> bytes:  # noqa: ANN001
    """严格档的迁移序列化。对应 Go 的 `marshalBattleTransition`。"""
    validate_battle_storage_transition(previous, next_rec)
    return next_rec.SerializeToString()


# ── Agones 分配结果(agones_allocator.go 的 AuthoritativeGameServerAllocation)──


@dataclasses.dataclass(frozen=True, slots=True)
class AuthoritativeGameServerAllocation:
    """Model B 分配结果。对应 Go 的 `AuthoritativeGameServerAllocation`。

    ★ 权威定义在 `agones_allocator.go`(本次移植范围之外)。这里放一份**字段完全
      一致**的搬运壳,只因 `FenceAllocationUncertainRelease` 的签名需要它。移植
      agones_allocator.py 时应把它搬过去并从那里 import,不要各留一份 —— 字段漂移
      在这里的表现是 `complete_resolved_uncertain_allocation` 恒为 false,即
      "uncertain 记录永远对账不完",一台泄漏的 Pod 永远删不掉。
    """

    pod_name: str = ""
    addr: str = ""
    instance_uid: str = ""
    pod_uid: str = ""
    instance_epoch: int = 0
    resource_version: str = ""
    allocation_id: str = ""
    release_track: str = ""
    annotations_present: bool = False


def complete_resolved_uncertain_allocation(
    allocation: AuthoritativeGameServerAllocation | None, allocation_id: str
) -> bool:
    """对账结果是否"完整且确凿"。对应 Go 的 `completeResolvedUncertainAllocation`。

    ★ `instance_epoch == 0` 是判据而不是笔误:Model-B 的 epoch 由
      PrepareCredential 赋予,准入前对账器看到非 0 epoch 说明凭据已经建立,
      这条记录不再归它管。
    """
    return (
        allocation is not None
        and allocation_id != ""
        and allocation.allocation_id == allocation_id
        and allocation.pod_name != ""
        and allocation.instance_uid != ""
        and allocation.pod_uid != ""
        and allocation.resource_version != ""
        and allocation.instance_epoch == 0
        and releasetrack.valid(allocation.release_track)
    )


def uncertain_release_tuple_matches(
    battle, allocation_id: str, allocation: AuthoritativeGameServerAllocation | None
) -> bool:  # noqa: ANN001
    """已落盘的 release-pending 记录是否与本次对账结果**逐项**一致。

    对应 Go 的 `uncertainReleaseTupleMatches`。用于幂等重入:重复调用必须确认
    "我要 fence 的正是已经 fence 过的那一个",而不是"反正已经是 pending 了"。
    """
    return (
        battle is not None
        and complete_resolved_uncertain_allocation(allocation, allocation_id)
        and battle.allocation_id == allocation_id
        and battle.ds_pod_name == allocation.pod_name
        and battle.gameserver_uid == allocation.instance_uid
        and battle.pod_uid == allocation.pod_uid
        and battle.instance_epoch == 0
        and battle.release_track == allocation.release_track
    )


# ── active 索引重建(battle_active_reconciler.go)─────────────────────────────


@dataclasses.dataclass(frozen=True, slots=True)
class ActiveBattleCandidate:
    """对应 Go 的 `activeBattleCandidate`。"""

    match_id: int
    score: int


def active_index_required(state: str, persistent: bool) -> bool:
    """该状态是否**必须**在 active 索引里。对应 Go 的 `activeIndexRequired`。

    ★ `abandoned` 依赖 `persistent`:Model-B 的终态记录在物理 release 与生命周期
      投递双双 ACK 之前保持永久;ACK 之后 `ExpireBattle` 给它一个 TTL 并移出
      active —— 重建器**不得**把那份留档审计记录复活(复活 = 已结束的对局重新
      进入心跳超时扫描,反复触发补偿)。
    ★ 未知状态**报错**而不是"当作不需要":不认识的状态说明写者更新,
      漏建索引的后果是那局永远没人推进。
    """
    if state in (
        "allocating",
        "warming",
        "ready",
        "running",
        BATTLE_STATE_ALLOCATION_UNCERTAIN,
        BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING,
        BATTLE_STATE_ALLOCATION_RECONCILE_EMPTY_TOMBSTONE,
        BATTLE_STATE_PREACTIVE_RELEASE_PENDING,
        BATTLE_STATE_ALLOCATION_ABORT_PENDING,
    ):
        return True
    if state == "abandoned":
        return persistent
    if state == "ended":
        return False
    raise BattleDataError("unknown canonical battle state %s", _quote_go(state))


def parse_battle_id_from_key(key: str) -> int:
    """从权威键反解 match_id。对应 Go 的 `parseBattleIDFromKey`。"""
    prefix = "pandora:ds:battle:{"
    if not key.startswith(prefix) or not key.endswith("}"):
        raise BattleDataError("invalid canonical battle key %s", _quote_go(key))
    raw = key[len(prefix) : -1]
    if not _DECIMAL_RE.fullmatch(raw):
        raise BattleDataError(
            "invalid canonical battle id in key %s: invalid syntax", _quote_go(key)
        )
    value = int(raw)
    if value > UINT64_MAX:
        raise BattleDataError(
            "invalid canonical battle id in key %s: value out of range", _quote_go(key)
        )
    if value == 0:
        raise BattleDataError(
            "invalid canonical battle id in key %s: zero is reserved", _quote_go(key)
        )
    return value


async def scan_battle_candidates(node, count: int) -> list[ActiveBattleCandidate]:  # noqa: ANN001
    """在**单个** Redis 节点上枚举需要进 active 索引的对局。

    对应 Go 的 `scanBattleCandidates`。任何一步出错都整轮中止(不"跳过继续") ——
    半份候选集会让调用方以为重建完成了。
    """
    if count <= 0:
        count = 128
    out: list[ActiveBattleCandidate] = []
    cursor = 0
    while True:
        cursor, keys = await node.scan(cursor=cursor, match=BATTLE_KEY_SCAN_PATTERN, count=count)
        for raw_key in keys:
            key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else str(raw_key)
            match_id = parse_battle_id_from_key(key)
            payload = await node.get(key)
            if payload is None:
                continue
            record = unmarshal_battle(match_id, payload)
            pttl = int(await node.pttl(key))
            if pttl == -2:  # 读到 payload 之后键刚过期:本轮没有可信证据,跳过
                continue
            if active_index_required(record.state, pttl == -1):
                out.append(
                    ActiveBattleCandidate(match_id=match_id, score=record.last_heartbeat_ms)
                )
        if cursor == 0:
            return out


# ── Redis 实现 ───────────────────────────────────────────────────────────────


class RedisBattleRepo:
    """基于 redis.asyncio 的 BattleRepo。对应 Go 的 `RedisBattleRepo`。

    ★ Go 的 `BattleRepo` / `StrictModelBBattleStorage` / `AllocationUncertainRepo` /
      `BattleActiveIndexReconciler` 四个接口在 Python 里**不建 Protocol**:
      Go 拆接口是为了让"旧 writer / 测试替身不能意外获得对账写权限"(编译期
      能力探测),而 Python 的 duck typing 里 Protocol 拦不住任何东西 ——
      建一堆只读不用的 Protocol 属 §15.3 预设性复杂化。调用方要做能力探测时
      仍可用 `hasattr`,与 Go 的 type assertion 同形。
    """

    __slots__ = ("_rdb", "_strict_model_b_writes")

    def __init__(self, rdb) -> None:  # noqa: ANN001
        self._rdb = rdb
        # 对应 Go 的 atomic.Bool。Python 单线程事件循环里普通 bool 已足够:
        # 没有指令重排,读写都在同一个 loop 线程。
        self._strict_model_b_writes = False

    # ── 严格 Model-B 写档(不可逆)────────────────────────────────────────

    def enable_strict_model_b_writes(self) -> None:
        """开启 epoch-2 严格写档。**不可逆**,由启动预检通过后调用一次。"""
        self._strict_model_b_writes = True

    def strict_model_b_writes_enabled(self) -> bool:
        return self._strict_model_b_writes

    def _marshal_battle_create(self, record) -> bytes:  # noqa: ANN001
        if self._strict_model_b_writes:
            return marshal_battle(record)
        return record.SerializeToString()

    def _marshal_battle_transition(self, previous, next_rec) -> bytes:  # noqa: ANN001
        if self._strict_model_b_writes:
            return marshal_battle_transition(previous, next_rec)
        return next_rec.SerializeToString()

    # ── 分配所有权 ───────────────────────────────────────────────────────

    async def claim_battle(self, claim, battle_ttl_sec: float) -> tuple[bool, object | None]:  # noqa: ANN001
        """以单键 `SET NX` 取得本轮分配所有权。对应 Go 的 `ClaimBattle`。

        返回 `(claimed, existing)`:`claimed=True` 才允许访问外部 Agones
        Allocation API,`allocation_id` 是后续 finalize/cleanup 的 fencing token。

        ★ claim 同时登记 active ZSET 作为 **inflight 扫描索引**。不登记的话,
          进程在 SETNX 之后崩溃会让 allocating key 卡满整个 BattleTTL,而且
          GSA 的未知结果永远没人按 allocation_id 对账。

        ★ 索引登记失败时可以安全撤 claim ——**因为此刻还没碰过 Agones**。
          撤销也失败只会留下一个不可分配的 claim,绝不会产生第二个 Pod。
        """
        if (
            claim is None
            or claim.match_id == 0
            or claim.allocation_id == ""
            or claim.state != "allocating"
        ):
            raise errcode.PandoraError(errcode.ErrInvalidArg, "invalid battle allocation claim")
        payload = self._marshal_battle_create(claim)
        ok = await self._rdb.set(
            battle_key(claim.match_id), payload, nx=True, **_px(battle_ttl_sec)
        )
        if ok:
            try:
                await self._rdb.zadd(
                    ACTIVE_KEY, {str(claim.match_id): float(claim.last_heartbeat_ms)}
                )
            except asyncio.CancelledError:
                raise
            except BaseException as zexc:  # noqa: BLE001 —— 对齐 Go 的 err != nil
                try:
                    await self.delete_battle_if_allocation_matches(
                        claim.match_id, claim.allocation_id, claim.ds_pod_name
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as cleanup_exc:  # noqa: BLE001
                    raise BattleDataError(
                        "claim inflight index: %s; cleanup: %s", zexc, cleanup_exc, cause=zexc
                    ) from zexc
                raise BattleDataError("claim inflight index: %s", zexc, cause=zexc) from zexc
            return True, None

        existing = await self.get_battle(claim.match_id)
        if existing is None:
            # key 可能恰在 SETNX=false 之后过期;本轮**不擅自再抢**,否则一次 RPC
            # 内会产生两次外部分配。调用方重试会领到新的 allocation_id。
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "battle %d allocation claim disappeared",
                claim.match_id,
            )
        return False, existing

    async def fence_battle_allocation(self, match_id: int, allocation_id: str) -> bool:
        """外部 GSA POST 之前的 Redis 线性化点。对应 Go 的 `FenceBattleAllocation`。

        只有当前 allocation_id 的 `allocating` owner 能把记录 CAS 成
        `allocation_uncertain` **并去掉 TTL**;只有返回 True 的调用者才准 POST。

        ★ `SET KEEPTTL` + `PERSIST` 必须在**同一个 EXEC** 里:分成两步的话,
          "已写 uncertain 但还没 persist"的窗口里进程崩溃,那条本该永久的
          fail-closed 墓碑会过期 —— 于是同一个 match 被允许发第二次 POST。

        ★ EXEC 响应未知时调用方也**必须按失败处理且绝不 POST**;最坏只留下一个
          永久 fail-closed、需要显式审计的 uncertain claim(方向安全)。
        """
        if match_id == 0 or allocation_id == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "match_id and allocation_id required"
            )
        key = battle_key(match_id)
        for _attempt in range(BATTLE_CAS_ATTEMPTS):
            fenced = False
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    current_bytes = await pipe.get(key)
                    if current_bytes is not None:
                        current = unmarshal_battle(match_id, current_bytes)
                        if (
                            current.allocation_id == allocation_id
                            and current.state == "allocating"
                        ):
                            previous = clone(current)
                            current.state = BATTLE_STATE_ALLOCATION_UNCERTAIN
                            payload = self._marshal_battle_transition(previous, current)
                            pipe.multi()
                            pipe.set(key, payload, keepttl=True)
                            pipe.persist(key)
                            await pipe.execute()
                            fenced = True
            except WatchError:
                continue
            return fenced
        raise errcode.PandoraError(
            errcode.ErrDSAllocationFailed,
            "battle %d pre-allocation fence concurrent retry exhausted",
            match_id,
        )

    # ── uncertain 对账三件套 ──────────────────────────────────────────────

    async def fence_allocation_uncertain_release(
        self,
        match_id: int,
        allocation_id: str,
        allocation: AuthoritativeGameServerAllocation | None,
    ) -> bool:
        """把 allocation_id 查到的唯一 exact 身份**持久化到 Kubernetes DELETE 之前**。

        对应 Go 的 `FenceAllocationUncertainRelease`。

        ★ `auth` 键必须仍不存在:一旦凭据准备已经发生,生命周期就不再归这个
          准入前对账器管(它继续推进 = 把一台正在服务玩家的 DS 判成待回收)。
        ★ 已是 release_pending 时走**幂等确认**分支:必须逐项确认是同一个 tuple,
          并确认 `PTTL == -1`(墓碑永不过期)。
        """
        if match_id == 0 or not complete_resolved_uncertain_allocation(allocation, allocation_id):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "complete resolved uncertain allocation identity required"
            )
        b_key, a_key = battle_key(match_id), battle_auth_key(match_id)
        for _attempt in range(BATTLE_CAS_ATTEMPTS):
            fenced = False
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key)
                    if int(await pipe.exists(a_key)) != 0:
                        raise errcode.PandoraError(
                            errcode.ErrInvalidState,
                            "battle %d uncertain allocation already has credential authority",
                            match_id,
                        )
                    payload = await pipe.get(b_key)
                    if payload is None:
                        return False
                    current = unmarshal_battle(match_id, payload)
                    if current.state == BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING:
                        if not uncertain_release_tuple_matches(
                            current, allocation_id, allocation
                        ):
                            raise errcode.PandoraError(
                                errcode.ErrInvalidState,
                                "battle %d uncertain release tuple conflict",
                                match_id,
                            )
                        if int(await pipe.pttl(b_key)) != -1:
                            raise errcode.PandoraError(
                                errcode.ErrInvalidState,
                                "battle %d uncertain release fence is not persistent",
                                match_id,
                            )
                        return True
                    if (
                        current.state != BATTLE_STATE_ALLOCATION_UNCERTAIN
                        or current.allocation_id != allocation_id
                        or current.ds_pod_name != ""
                        or current.gameserver_uid != ""
                        or current.pod_uid != ""
                        or current.instance_epoch != 0
                    ):
                        return False
                    previous = clone(current)
                    current.state = BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING
                    current.ds_pod_name = allocation.pod_name
                    current.gameserver_uid = allocation.instance_uid
                    current.pod_uid = allocation.pod_uid
                    current.release_track = allocation.release_track
                    encoded = self._marshal_battle_transition(previous, current)
                    pipe.multi()
                    pipe.set(b_key, encoded)  # TTL=0:墓碑永不过期
                    await pipe.execute()
                    fenced = True
            except WatchError:
                continue
            return fenced
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "battle %d uncertain release fence concurrent retry exhausted",
            match_id,
        )

    async def complete_allocation_uncertain_release(
        self, match_id: int, allocation_id: str, instance_uid: str
    ) -> bool:
        """确认目标 GameServer 已不存在后,写下持久终态。

        对应 Go 的 `CompleteAllocationUncertainRelease`。两条合法入口:
            instance_uid == ""  权威空结果:记录必须仍是**无身份**的 uncertain;
            instance_uid != ""  精确结果:记录必须是 release_pending 且 UID 相符。
        `already` 分支是幂等重入,同样要求 `PTTL == -1`。
        """
        if match_id == 0 or allocation_id == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "match_id and allocation_id required for uncertain release completion",
            )
        b_key, a_key = battle_key(match_id), battle_auth_key(match_id)
        for _attempt in range(BATTLE_CAS_ATTEMPTS):
            completed = False
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key)
                    if int(await pipe.exists(a_key)) != 0:
                        raise errcode.PandoraError(
                            errcode.ErrInvalidState,
                            "battle %d uncertain completion found credential authority",
                            match_id,
                        )
                    payload = await pipe.get(b_key)
                    if payload is None:
                        return False
                    current = unmarshal_battle(match_id, payload)
                    if current.allocation_id != allocation_id or current.instance_epoch != 0:
                        return False
                    exact_empty = (
                        instance_uid == ""
                        and current.state == BATTLE_STATE_ALLOCATION_UNCERTAIN
                        and current.ds_pod_name == ""
                        and current.gameserver_uid == ""
                        and current.pod_uid == ""
                    )
                    exact_resolved = (
                        instance_uid != ""
                        and current.state
                        == BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING
                        and current.gameserver_uid == instance_uid
                        and current.ds_pod_name != ""
                        and current.pod_uid != ""
                        and releasetrack.valid(current.release_track)
                    )
                    already = current.state == "abandoned" and (
                        (
                            instance_uid == ""
                            and current.gameserver_uid == ""
                            and current.ds_pod_name == ""
                            and current.pod_uid == ""
                        )
                        or (
                            instance_uid != ""
                            and current.gameserver_uid == instance_uid
                            and current.ds_pod_name != ""
                            and current.pod_uid != ""
                        )
                    )
                    if already:
                        if int(await pipe.pttl(b_key)) != -1:
                            raise errcode.PandoraError(
                                errcode.ErrInvalidState,
                                "battle %d uncertain terminal fence is not persistent",
                                match_id,
                            )
                        return True
                    if not exact_empty and not exact_resolved:
                        return False
                    previous = clone(current)
                    current.state = "abandoned"
                    encoded = self._marshal_battle_transition(previous, current)
                    pipe.multi()
                    pipe.set(b_key, encoded)
                    await pipe.execute()
                    completed = True
            except WatchError:
                continue
            return completed
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "battle %d uncertain release completion concurrent retry exhausted",
            match_id,
        )

    async def mark_allocation_uncertain_empty_lifecycle_published(
        self, match_id: int, allocation_id: str
    ) -> bool:
        """记录 Kafka ACK,但**不交出** allocation_id 的清理权。

        对应 Go 的 `MarkAllocationUncertainEmptyLifecyclePublished`。只服务
        "权威空结果"这条路径:超时的 GSA POST 仍可能在一次空 LIST **之后**生效,
        所以这条记录必须留在 active 恢复索引里持续 DeleteCollection+LIST,
        且永不过期 —— 退休只能靠未来显式的静默期证明 / 运维确认。
        """
        if match_id == 0 or allocation_id == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "match_id and allocation_id required for empty allocation tombstone",
            )
        b_key, a_key = battle_key(match_id), battle_auth_key(match_id)
        for _attempt in range(BATTLE_CAS_ATTEMPTS):
            marked = False
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key)
                    if int(await pipe.exists(a_key)) != 0:
                        raise errcode.PandoraError(
                            errcode.ErrInvalidState,
                            "battle %d empty allocation tombstone found credential authority",
                            match_id,
                        )
                    payload = await pipe.get(b_key)
                    if payload is None:
                        return False
                    current = unmarshal_battle(match_id, payload)
                    if (
                        current.allocation_id != allocation_id
                        or current.instance_epoch != 0
                        or current.ds_pod_name != ""
                        or current.gameserver_uid != ""
                        or current.pod_uid != ""
                    ):
                        return False
                    if current.state == BATTLE_STATE_ALLOCATION_RECONCILE_EMPTY_TOMBSTONE:
                        if int(await pipe.pttl(b_key)) != -1:
                            raise errcode.PandoraError(
                                errcode.ErrInvalidState,
                                "battle %d empty allocation tombstone is not persistent",
                                match_id,
                            )
                        return True
                    if current.state != "abandoned":
                        return False
                    previous = clone(current)
                    current.state = BATTLE_STATE_ALLOCATION_RECONCILE_EMPTY_TOMBSTONE
                    encoded = self._marshal_battle_transition(previous, current)
                    pipe.multi()
                    pipe.set(b_key, encoded)
                    await pipe.execute()
                    marked = True
            except WatchError:
                continue
            return marked
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "battle %d empty allocation tombstone concurrent retry exhausted",
            match_id,
        )

    # ── finalize ─────────────────────────────────────────────────────────

    async def finalize_battle_allocation(self, battle, battle_ttl_sec: float) -> bool:  # noqa: ANN001
        """legacy(Model-A)finalize:`allocating` → `warming`,带正常 TTL。"""
        return await self._finalize_battle_allocation(
            battle, "allocating", battle_ttl_sec, persistent=False
        )

    async def finalize_fenced_battle_allocation(self, battle, battle_ttl_sec: float) -> bool:  # noqa: ANN001
        """Model-B 唯一 finalize 入口:`allocation_uncertain` → `warming`,保持永久。

        ★ 它**拒绝**从 `allocating` 直接跳到 `warming` —— 严格 UID/RV 确认之前,
          Redis claim 必须一直停在永久 uncertain 上。
        """
        return await self._finalize_battle_allocation(
            battle, BATTLE_STATE_ALLOCATION_UNCERTAIN, battle_ttl_sec, persistent=True
        )

    async def _finalize_battle_allocation(  # noqa: ANN001
        self, battle, expected_state: str, battle_ttl_sec: float, *, persistent: bool
    ) -> bool:
        """`finalizeBattleAllocation` 的共享实现。

        ★ 权威 battle key 与 CAS 在同 slot / 单事务;active ZSET 是**跨 slot 派生
          索引**,ZADD 失败会把错误抛给调用方,由 expected-allocation cleanup 把
          warming 镜像撤掉 —— 绝不放行 ready。
        """
        if (
            battle is None
            or battle.match_id == 0
            or battle.allocation_id == ""
            or battle.state != "warming"
            or battle.ds_pod_name == ""
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "invalid finalized battle allocation"
            )
        if persistent and (
            battle.ds_addr == ""
            or battle.gameserver_uid == ""
            or battle.pod_uid == ""
            or battle.instance_epoch != 0
            or not releasetrack.valid(battle.release_track)
        ):
            # Model-B finalize 发生在 PrepareCredential 赋 epoch **之前**,
            # 故 epoch 必须仍为 0;未来做精确 release 所需的外部对象身份必须已持久。
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "incomplete authoritative Model-B allocation identity"
            )
        if expected_state not in ("allocating", BATTLE_STATE_ALLOCATION_UNCERTAIN):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "invalid battle allocation source state"
            )

        key = battle_key(battle.match_id)
        for _attempt in range(BATTLE_CAS_ATTEMPTS):
            matched = False
            tx_exc: BaseException | None = None
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    current_bytes = await pipe.get(key)
                    if current_bytes is not None:
                        current = unmarshal_battle(battle.match_id, current_bytes)
                        if (
                            current.allocation_id == battle.allocation_id
                            and current.state == expected_state
                        ):
                            # finalize 是 read-modify-write:以 WATCH 内刚读到的权威
                            # unknown fields 覆盖调用方快照,防旧/并发 writer 在滚动
                            # 更新中静默丢未来字段(§9 不变量 17)。
                            next_rec = clone(battle)
                            set_unknown_bytes(next_rec, unknown_bytes(current))
                            payload = self._marshal_battle_transition(current, next_rec)
                            # ★ 与 Go 同序:`matched` 在 EXEC **之前**置位。
                            #   EXEC 响应丢失时 matched=True 且有异常,正是下面
                            #   read-back 分支要处理的那种"提交与否未知"。
                            matched = True
                            pipe.multi()
                            pipe.set(key, payload, **({} if persistent else _px(battle_ttl_sec)))
                            await pipe.execute()
            except WatchError:
                continue
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 对齐 Go 的 err 捕获
                tx_exc = exc

            if persistent and (tx_exc is not None or not matched):
                # EXEC 可能已经提交、但响应在客户端侧丢失。此时不能把永久
                # allocation_uncertain 错当成"仍未 finalize"后直接返回:提交后的
                # warming 也是 GSA 生命周期 fence,调用方应继续凭据投递。
                # 只在严格 GET read-back 同时确认 allocation/UID/pod/state 且
                # PTTL=-1 时才认定成功。
                confirmed = False
                read_exc: BaseException | None = None
                try:
                    confirmed = await asyncio.wait_for(
                        self._confirm_persistent_fenced_finalize(battle),
                        FENCED_FINALIZE_READBACK_TIMEOUT_SEC,
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    read_exc = exc
                if confirmed and read_exc is None:
                    matched = True
                    tx_exc = None
                elif tx_exc is not None:
                    if read_exc is not None:
                        raise BattleDataError(
                            "battle %d finalize response uncertain: %s; read-back: %s",
                            battle.match_id,
                            tx_exc,
                            read_exc,
                            cause=tx_exc,
                        ) from tx_exc
                    raise tx_exc
                elif read_exc is not None:
                    raise read_exc

            if tx_exc is not None:
                raise tx_exc
            if not matched:
                return False
            await self._rdb.zadd(
                ACTIVE_KEY, {str(battle.match_id): float(battle.last_heartbeat_ms)}
            )
            return True
        raise errcode.PandoraError(
            errcode.ErrDSAllocationFailed,
            "battle %d finalize concurrent retry exhausted",
            battle.match_id,
        )

    async def _confirm_persistent_fenced_finalize(self, expected) -> bool:  # noqa: ANN001
        """Model-B 的 response-lost read-back。对应 Go 的
        `confirmPersistentFencedFinalize`。

        ★ **不能只看 state=warming**:同 match 的另一次分配、同名 Pod 重建、
          或一个有限 TTL 的旧 writer 都会呈现 warming。UID、allocation_id、pod、
          地址、实例 epoch、roster、时间戳**逐项**相等,且 key 必须已无过期时间,
          才算是"我这一次提交"。抽查三四个身份字段不够 —— 那正是 ABA 的入口。
        """
        key = battle_key(expected.match_id)
        payload = await self._rdb.get(key)
        if payload is None:
            return False
        current = unmarshal_battle(expected.match_id, payload)
        strict_expected = clone(expected)
        # future unknown fields 来自 WATCH 内的原 claim,属于必须保留的滚动升级
        # 数据;除它们外所有已知字段都须与本次 intended write 完全相等。
        set_unknown_bytes(strict_expected, unknown_bytes(current))
        if current.state != "warming" or not proto_equal(current, strict_expected):
            return False
        if int(await self._rdb.pttl(key)) != -1:
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "battle %d fenced finalize read-back is not persistent",
                expected.match_id,
            )
        return True

    # ── fencing delete ───────────────────────────────────────────────────

    async def delete_battle_if_allocation_matches(
        self, match_id: int, allocation_id: str, pod_name: str
    ) -> bool:
        """旧请求清理路径的 fencing delete。对应 Go 的
        `DeleteBattleIfAllocationMatches`。

        事务内再次确认 allocation_id / pod,且**只允许**已知的
        allocating / warming / abandoned;`allocation_uncertain`、ready/running
        以及未来任何未知状态全部 fail-closed。只有返回 True 的调用方才可 Release。

        ★ 权威 key 删成功但 active 索引清理失败时抛 `BattleActiveIndexError`
          且 `deleted=True` —— 对应 Go 的 `return true, err`。调用方**必须**
          据此仍去 Release,残留 ZSET 由 sweep 的 miss 分支清理。
        """
        if match_id == 0 or allocation_id == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "match_id and allocation_id required"
            )
        key = battle_key(match_id)
        for _attempt in range(BATTLE_CAS_ATTEMPTS):
            deleted = False
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    payload = await pipe.get(key)
                    if payload is not None:
                        current = unmarshal_battle(match_id, payload)
                        if (
                            current.allocation_id == allocation_id
                            and (pod_name == "" or current.ds_pod_name == pod_name)
                            and current.state in DELETABLE_BATTLE_STATES
                        ):
                            deleted = True
                            pipe.multi()
                            pipe.delete(key)
                            await pipe.execute()
            except WatchError:
                continue
            if not deleted:
                return False
            try:
                await self._rdb.zrem(ACTIVE_KEY, str(match_id))
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise BattleActiveIndexError(
                    "battle %d active index cleanup after fenced delete: %s",
                    match_id,
                    exc,
                    cause=exc,
                ) from exc
            return True
        raise errcode.PandoraError(
            errcode.ErrDSAllocationFailed,
            "battle %d cleanup concurrent retry exhausted",
            match_id,
        )

    # ── 常规 CRUD ────────────────────────────────────────────────────────

    async def create_battle(self, battle, battle_ttl_sec: float) -> None:  # noqa: ANN001
        """写对局镜像(权威)并登记全局 active ZSET。对应 Go 的 `CreateBattle`。

        ★ Cluster 兼容:`battle_key{match}` 与全局 `ACTIVE_KEY` 分属不同 slot,
          不能捆同一事务(否则 CROSSSLOT)。① 单键 `SET NX` 权威落库;
          ② 独立 ZADD 登记(必须成功,否则心跳扫描漏这局)。两步幂等,可重入。

        ★ `SET NX` 而不是 `SET`:已存在时**拒绝覆盖**并报错。覆盖会把一局正在打的
          对局的 roster / 心跳 / 身份整体换掉,而调用方以为只是"重建了镜像"。
        """
        payload = self._marshal_battle_create(battle)
        ok = await self._rdb.set(
            battle_key(battle.match_id), payload, nx=True, **_px(battle_ttl_sec)
        )
        if not ok:
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "battle %d already exists; refusing overwrite",
                battle.match_id,
            )
        await self._rdb.zadd(
            ACTIVE_KEY, {str(battle.match_id): float(battle.last_heartbeat_ms)}
        )

    async def get_battle(self, match_id: int):
        """读对局镜像。不存在返回 `None`(对应 Go 的 `(nil, false, nil)`)。"""
        payload = await self._rdb.get(battle_key(match_id))
        if payload is None:
            return None
        return unmarshal_battle(match_id, payload)

    async def update_battle_with_lock(
        self, match_id: int, max_retry: int, fn, battle_ttl_sec: float
    ) -> None:  # noqa: ANN001
        """WATCH/MULTI/EXEC 读-改-写,并**刷新** battle key 的 TTL(心跳 / 正常状态更新)。"""
        return await self._update_with_lock(
            match_id, max_retry, fn, ttl_sec=battle_ttl_sec, keep_ttl=False
        )

    async def update_battle_keep_ttl(self, match_id: int, max_retry: int, fn) -> None:  # noqa: ANN001
        """同上,但用 `KEEPTTL` **保留原 TTL 不刷新**。

        ★ sweep 的 abandoned 标记 + 补偿重试路径专用:BattleTTL(从最后一次心跳
          起算)因此成为补偿重试的**天然上界** —— Kafka 长期不可用时镜像最终过期,
          GetBattle miss → 清理 active,不会因每轮重试无限刷 TTL / 无限堆积。
          写成刷新 TTL 会让一个永远补偿不成功的对局**永久**留在 Redis 里。
        """
        return await self._update_with_lock(match_id, max_retry, fn, ttl_sec=0.0, keep_ttl=True)

    async def _update_with_lock(  # noqa: ANN001
        self, match_id: int, max_retry: int, fn, *, ttl_sec: float, keep_ttl: bool
    ) -> None:
        """`updateWithLock` 的共享实现。

        ⚠️ **fn 重跑契约**(与 Go 的接口注释逐条一致):CAS 冲突时 fn 会**基于重新
        GET 的最新镜像整体重跑**,故 fn 必须无副作用 —— 只准改传入的记录和调用方
        捕获的出参变量,且出参必须在 fn 开头重置。由此,"读到旧状态 X 才置位"的
        出参标记天然具备**跨副本恰好一次**语义:状态迁移 X→Y 全局只有一个 EXEC
        能成功,输家重跑后读到 Y 不再置位(sweep 的 firstAbandon 防 double-release
        就靠这一条)。

        ★ `fn` 抛的异常必须与 redis / WatchError **分开**处理(对应 Go 的
          `txErr == fnErr` 那一支)。混在一起有两个后果:被冲突分支吞掉会变成
          "重试 N 次后 CAS 耗尽"(线上看到的原因是错的);把业务哨兵当存储故障
          会刷一条假告警。

        ★ 镜像不存在时抛 `ErrDSPodNotFound` 并**立即冒泡**(不重试):重试一个不
          存在的键只是把同一个结论重复 N 次,还把 max_retry 的预算从并发冲突挪走。
        """
        key = battle_key(match_id)
        set_kwargs = {"keepttl": True} if keep_ttl else _px(ttl_sec)

        for _attempt in range(max_retry + 1):
            fn_raised = False
            last_heartbeat_ms = 0
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    payload = await pipe.get(key)
                    if payload is None:
                        raise errcode.PandoraError(
                            errcode.ErrDSPodNotFound, "battle %d not found", match_id
                        )
                    battle = unmarshal_battle(match_id, payload)
                    previous = clone(battle)
                    try:
                        result = fn(battle)
                        if result is not None and hasattr(result, "__await__"):
                            await result
                    except asyncio.CancelledError:
                        raise
                    except BaseException:  # noqa: BLE001 —— 只做标记,原样再抛
                        fn_raised = True
                        raise
                    new_payload = self._marshal_battle_transition(previous, battle)
                    last_heartbeat_ms = battle.last_heartbeat_ms
                    pipe.multi()
                    pipe.set(key, new_payload, **set_kwargs)
                    await pipe.execute()
            except WatchError:
                if fn_raised:
                    raise
                continue  # CAS 冲突,重读重跑
            # 其余异常(fn 业务错误 / not found / 序列化 / redis 故障)一律原样冒泡。

            # active 索引:与 battle key 不同 slot,独立 ZADD 刷新 score。
            # 幂等;失败下一轮心跳 / sweep 即补,不影响权威镜像。
            await self._rdb.zadd(ACTIVE_KEY, {str(match_id): float(last_heartbeat_ms)})
            return
        raise errcode.PandoraError(
            errcode.ErrDSAllocationFailed,
            "battle %d update concurrent retry exhausted",
            match_id,
        )

    # ── active 索引维护 ──────────────────────────────────────────────────

    async def touch_active(self, match_id: int, last_heartbeat_ms: int) -> None:
        """刷新 active ZSET 中该 match 的 score(last_heartbeat_ms)。"""
        _require_int64("last_heartbeat_ms", last_heartbeat_ms)
        await self._rdb.zadd(
            ACTIVE_KEY, {str(_require_uint64("match_id", match_id)): float(last_heartbeat_ms)}
        )

    async def remove_active(self, match_id: int) -> None:
        """把 match 移出 active ZSET(战斗结束 / 释放,不再心跳扫描)。"""
        await self._rdb.zrem(ACTIVE_KEY, str(_require_uint64("match_id", match_id)))

    async def delete_battle(self, match_id: int) -> None:
        """删镜像 + 移出 active。两条命令跨 slot,均幂等;ZRem 失败留下的残留由
        sweep / list 扫到镜像已删时跳过并补清(自愈)。"""
        await self._rdb.delete(battle_key(match_id))
        await self._rdb.zrem(ACTIVE_KEY, str(match_id))

    async def expire_battle(self, match_id: int, ttl_sec: float) -> None:
        """改短 battle key TTL(终态保留供查询)并移出 active。"""
        await self._rdb.expire(battle_key(match_id), _format_sec(ttl_sec))
        await self._rdb.zrem(ACTIVE_KEY, str(match_id))

    async def range_stale_battles(self, threshold_ms: int) -> list[int]:
        """返回 `last_heartbeat_ms <= threshold_ms` 的 match_id(心跳已超时)。"""
        vals = await self._rdb.zrangebyscore(
            ACTIVE_KEY, "-inf", str(_require_int64("threshold_ms", threshold_ms))
        )
        return parse_ids(vals)

    async def range_active_battles(self) -> list[int]:
        """返回 active ZSET 中全部 match_id(ListBattles 用)。"""
        vals = await self._rdb.zrange(ACTIVE_KEY, 0, -1)
        return parse_ids(vals)

    async def reconcile_battle_active_index(self, count: int) -> None:
        """从权威记录重建派生的 active ZSET。对应 Go 的 `ReconcileBattleActiveIndex`。

        ★ 必须**逐个 Cluster master** SCAN。直接对 UniversalClient 调 SCAN 只会
          问到一个分片,其它 hash slot 上的恢复记录会被**永久**漏掉 —— 而本方法
          的用途正是"从权威记录修复丢失的索引",漏掉的那些局永远没人推进。

        ★ 回填用 `ZADD NX`:已有 score 是**更新的**心跳事实,重建器不得把它拍回
          记录里那个可能陈旧的 last_heartbeat_ms(拍回去 = 一局活着的对局被判超时)。
        """
        if self._rdb is None:
            raise BattleDataError("battle active reconciler redis unavailable")
        candidates: list[ActiveBattleCandidate] = []
        nodes_fn = getattr(self._rdb, "get_primaries", None)
        if callable(nodes_fn):  # RedisCluster
            for node in nodes_fn():
                candidates.extend(await scan_battle_candidates(node.redis_connection, count))
        else:
            candidates.extend(await scan_battle_candidates(self._rdb, count))
        for candidate in candidates:
            await self._rdb.zadd(
                ACTIVE_KEY, {str(candidate.match_id): float(candidate.score)}, nx=True
            )

    # ── 分配台账(allocation_ledger.go)──────────────────────────────────

    async def record_allocation_ledger(self, allocation_id: str, at_ms: int) -> None:
        """把一次分配的 allocation_id 记入台账。幂等(重复 ZADD 只刷 score)。

        ★ 台账把"我读的权威"与"我要删的 GS"绑定:孤儿清扫删一台 GameServer 之前
          必须证明它的 `pandora.dev/allocation-id` label **曾在本权威里出现过**。
          配置漂移 / failover 到空实例的副本台账必然为空 ⇒ 一台都删不掉
          (fail-closed);而本权威分配出去后泄漏的 GS 台账必然有记录 ⇒ 照常回收。
        """
        if allocation_id == "":
            return
        _require_int64("at_ms", at_ms)
        await self._rdb.zadd(ALLOCATION_LEDGER_KEY, {allocation_id: float(at_ms)})

    async def allocation_ledger_contains(self, allocation_id: str) -> bool:
        """allocation_id 是否曾在本权威中出现过。

        ★ 查询错误**如实上抛**(调用方 fail-closed 保留候选),不得把错误冒充成
          False —— 冒充 False 的方向是"证据查不到就当没分配过",而清扫侧的
          "没分配过"意味着**允许删除**。
        """
        if allocation_id == "":
            return False
        return await self._rdb.zscore(ALLOCATION_LEDGER_KEY, allocation_id) is not None

    async def prune_allocation_ledger(self, before_ms: int) -> int:
        """清掉 score 早于 `before_ms` 的台账条目,返回清除数(§9.24 有界性闸)。"""
        return int(
            await self._rdb.zremrangebyscore(
                ALLOCATION_LEDGER_KEY, "-inf", str(_require_int64("before_ms", before_ms))
            )
        )


# ── no-show 记账(noshow_recorder.go)────────────────────────────────────────


class RedisNoShowRecorder:
    """no-show 记账 → 进入侧退避的写者侧。对应 Go 的 `RedisNoShowRecorder`。

    key 契约(登记于 `docs/design/infra.md` §3.2「RateLimit」;读者是 matchmaker
    StartMatch 的 NoShowPenaltyRemaining,两端都经 `redisx.rl_key` 构造):

        pandora:rl:match:noshow:<player_id>    记账计数器(窗口 no_show_ledger_window)
        pandora:rl:match:noshowcd:<player_id>  退避惩罚窗(matchmaker 执行拒绝)

    ★ 两个方法都返回 `(值, 异常)` / `异常 | None`,与 Go 的 `(int64, error)` /
      `error` **同形**,而不是"就地吞掉打条 warn"。原因见 `redisx.arm_penalty`
      的注释:惩罚窗是**写入侧**,写失败没有 fail-open 兜底可言 —— 它就是真的漏
      了一次罚,而漏罚这件事必须有人知道。
    """

    __slots__ = ("_rdb",)

    def __init__(self, rdb) -> None:  # noqa: ANN001
        self._rdb = rdb

    async def record_no_show(
        self, player_id: int, window_sec: float
    ) -> tuple[int, Exception | None]:
        """记一次 no-show,返回 (窗口内累计次数(含本次), 故障)。"""
        return await redisx.incr_window(
            self._rdb,
            redisx.rl_key("match", "noshow", _require_uint64("player_id", player_id)),
            window_sec,
        )

    async def arm_penalty(self, player_id: int, seconds: float) -> Exception | None:
        """布设进入侧退避窗(新罚**覆盖**旧罚剩余,不是取较大值)。"""
        return await redisx.arm_penalty(
            self._rdb,
            redisx.rl_key("match", "noshowcd", _require_uint64("player_id", player_id)),
            seconds,
        )
