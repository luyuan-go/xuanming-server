"""hub_allocator 数据层(Redis 分片镜像 + 玩家归属)—— 对应 Go 侧
`internal/data/hub_repo.go`。

Redis key 模板(**逐字照搬,一个字符都不能改**):

    pandora:hub:shard:{<hub_pod_name>}          → HubShardStorageRecord proto bytes,TTL=shard_ttl
    pandora:hub:shard:members:{<hub_pod_name>}  → SET(成员=player_id 十进制),强制整合枚举源
    pandora:hub:shards                          → SET(成员=hub_pod_name),ListHubs / 候选遍历
    pandora:hub:active                          → ZSET(score=last_heartbeat_ms),心跳超时扫描
    pandora:hub:player:<player_id>              → HubAssignmentStorageRecord proto bytes,TTL=assignment_ttl
    pandora:hub:team:<team_id>                  → string(hub_pod_name),队友同分片提示
    pandora:hub:transfer_cd:<player_id>         → string,SET NX EX,切线防刷占坑
    pandora:hub:transfer_cleanup:pods           → SET(成员=source pod),reconciler 全局索引
    pandora:hub:transfer_cleanup:{<pod>}        → SET(成员="<player_id>:<target_assignment_id>")
    pandora:hub:wfence:{<pod>}                  → 见 writer_fence.py

★ **两栈并存期 key 必须完全一致**。Go 副本和 Python 副本会同时读写同一份归属:
key 差一个字符 = 两边各自维护一份"权威",双方都判定自己成功,**没有任何运行期信号**,
而玩家会同时被两台 Hub DS 认领(§9 不变量 1)。所以本文件的 key 模板不是实现细节,
是跨语言硬契约。

★ read-modify-write 一律用标准 `ParseFromString`(**不** DiscardUnknown,§9 不变量 17):
滚动升级期新副本写入的新字段必须被旧副本原样读出并写回,否则旧副本每回写一次
就把新字段静默抹掉一批。

────────────────────────────────────────────────────────────────────────────
★ 归属写的五层收口(per-player key 无 hashtag,进不了 {pod} 水位事务)

    ① biz 入口 writer gate(失主副本快速拒写);
    ② `compare_and_swap_assignment` 的精确前置快照 CAS;
    ③ 继任者水位推扫 `advance_writer_fences_for_token` 是**接流前硬门**;
    ④ biz 出票前写者复核;
    ⑤ **每玩家持久水位** `HubAssignmentStorageRecord.writer_token`(proto field 31)——
       同一 key 的 WATCH/MULTI/EXEC 天然原子,比较与写入在同一线性化点。
       ⑤a 删除写**墓碑**不裸 DEL(裸 DEL 把水位随业务记录一起抹掉,
           继任者「创建 → 合法删除」之后失主旧写者就能借尸还魂);
       ⑤b 租约在**事务内**读(读在循环外时,旧写者暂停任意长仍带着陈旧 token
           走完事务)。
"""

from __future__ import annotations

import asyncio
import dataclasses
import re
import time

from pandora.hub.v1 import allocator_pb2
from redis.exceptions import WatchError

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy.services.hub_allocator import writer_fence as wfence

# ── key 模板 ─────────────────────────────────────────────────────────────────

SHARDS_SET_KEY = "pandora:hub:shards"
ACTIVE_KEY = "pandora:hub:active"
TRANSFER_CLEANUP_PODS_KEY = "pandora:hub:transfer_cleanup:pods"

# uint64 / int64 边界。★ Go 的类型系统天然挡住越界,Python int 无限精度 ——
# 不显式判就会把一个 2^70 的 player_id 拼进 key(合法字符串!),
# 于是"同一个玩家"在 Go 侧根本不可能存在,两栈的归属永久对不上。
UINT64_MAX = (1 << 64) - 1
INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1
INT32_MIN = -(1 << 31)
INT32_MAX = (1 << 31) - 1

# ★ `\A...\Z` 而不是 `^...$`:Python 的 `$` 也匹配**末尾换行**,`"123\n"` 会被判成
# 合法十进制;Go 的 `strconv.ParseUint` 不接受任何空白 / 正负号。
_DECIMAL_RE = re.compile(r"\A[0-9]+\Z")


def shard_key(pod: str) -> str:
    """`pandora:hub:shard:{<pod>}`。花括号是 Cluster hashtag,不是格式化占位符。"""
    return f"pandora:hub:shard:{{{pod}}}"


def assign_key(player_id: int) -> str:
    """`pandora:hub:player:<player_id>`。"""
    return f"pandora:hub:player:{_require_uint64('player_id', player_id)}"


def team_key(team_id: int) -> str:
    """`pandora:hub:team:<team_id>`。"""
    return f"pandora:hub:team:{_require_uint64('team_id', team_id)}"


def transfer_cooldown_key(player_id: int) -> str:
    """玩家主动切线冷却占坑键(string,SET NX EX,TTL=cooldown)。

    防止玩家高频刷线切换;冷却窗口内再切被拒(ErrHubTransferCooldown)。
    """
    return f"pandora:hub:transfer_cd:{_require_uint64('player_id', player_id)}"


def members_key(pod: str) -> str:
    """分片成员反向索引(SET,成员=player_id 十进制字符串)。

    hashtag {pod} 与 `shard_key` 同 slot,强制整合时按分片枚举玩家做服务端权威搬迁。
    best-effort:漂移不影响正确性(双通道中 Hub DS drain 心跳指令兜底漏听的玩家)。
    """
    return f"pandora:hub:shard:members:{{{pod}}}"


def transfer_cleanup_key(pod: str) -> str:
    """与 source shard 同 hashtag 的 per-pod cleanup 索引。

    注册顺序固定为 全局 pod 索引 → per-pod exact ref → assignment CAS;
    因此 CAS 成功前崩溃至多留下可安全识别的 orphan,CAS 成功后**绝不会缺
    reconciler 索引**。pod 索引是持久 superset,不因空集合删除,避免并发 register
    与「最后一项删除」竞态造成漏扫。
    """
    return f"pandora:hub:transfer_cleanup:{{{pod}}}"


def _require_uint64(name: str, value: int) -> int:
    """把外部传入的 ID 钳在 uint64 内。

    ★ 这道判据在 Go 里由 `uint64` 类型免费提供;Python 没有,不判就会:
      - 负数拼出 `pandora:hub:player:-1` —— 一个 Go 侧永远写不出的 key;
      - 超 uint64 的值赋给 proto `uint64` 字段时才抛 ValueError,
        而那已经是**写了一半**之后的事(索引可能已经加进去了)。
    """
    if not isinstance(value, int) or isinstance(value, bool):
        raise errcode.PandoraError(errcode.ErrInvalidArg, "%s must be an integer", name)
    if value < 0 or value > UINT64_MAX:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "%s %d out of uint64 range", name, value
        )
    return value


def parse_decimal_uint64(raw: bytes | str) -> int | None:
    """严格解析十进制 uint64。解析不了返回 None(调用方按「脏成员」跳过)。

    对应 Go 的 `strconv.ParseUint(m, 10, 64)` + `if perr != nil { continue }`。
    """
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
    if not _DECIMAL_RE.fullmatch(text):
        return None
    value = int(text)
    return value if value <= UINT64_MAX else None


# ── transfer cleanup ref ─────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True, slots=True)
class TransferCleanupRef:
    """一条待清理的旧 owner 引用。对应 Go 的 `TransferCleanupRef`。"""

    player_id: int
    target_assignment_id: str

    def valid(self) -> bool:
        """★ 三条判据缺一不可,每条都对应一种「清不掉的残留」:

            player_id != 0            0 = 没有玩家上下文,清理时找不到任何 owner;
            target_assignment_id 非空  ref 必须绑定**目标** assignment ——
                                      不绑的话 CAS loser 会删掉并发 winner 的 ref,
                                      winner 的旧 seat 从此没人退;
            不含 ':'                   编码是 `<player_id>:<assignment_id>`,
                                      assignment_id 里再有冒号会让 decode 截断,
                                      解出一个**不同的** assignment_id 并去清它。
        """
        return (
            self.player_id != 0
            and self.target_assignment_id.strip() != ""
            and ":" not in self.target_assignment_id
        )


def encode_transfer_cleanup_ref(ref: TransferCleanupRef) -> str:
    """`<player_id>:<target_assignment_id>`。"""
    return f"{ref.player_id}:{ref.target_assignment_id}"


def decode_transfer_cleanup_ref(raw: bytes | str) -> TransferCleanupRef | None:
    """解码;不合法返回 None。对应 Go 的 `decodeTransferCleanupRef`。

    ★ 用 `partition(":")`(= Go 的 `strings.Cut`)而不是 `split(":")` 后取 [0]/[1]:
    split 会把带冒号的脏 ref 悄悄截成一个**看似合法**的短 assignment_id,
    reconciler 就会去清一个不存在的 owner 并把这条 ref 摘掉 —— 真正的残留永远留下。
    """
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
    player, sep, assignment_id = text.partition(":")
    if not sep or assignment_id == "":
        return None
    player_id = parse_decimal_uint64(player)
    if player_id is None:
        return None
    ref = TransferCleanupRef(player_id=player_id, target_assignment_id=assignment_id)
    return ref if ref.valid() else None


# ── proto 辅助 ───────────────────────────────────────────────────────────────


def clone(msg):  # noqa: ANN001
    """深拷贝一条 proto。对应 Go 的 `proto.Clone`。

    ★ 必须用 `CopyFrom` 而不是 Python 的赋值:proto message 是引用语义,
    `a = b` 之后改 a 就是改 b。Go 侧同样禁止值拷贝(§5 第 10 条)。
    `CopyFrom` 会连 unknown fields 一起带走(实测 upb 成立),这是 §9 不变量 17
    要求的 —— 克隆丢 unknown = 回写时把新副本的字段抹掉。
    """
    out = type(msg)()
    out.CopyFrom(msg)
    return out


def proto_equal(a, b) -> bool:  # noqa: ANN001
    """完整相等(**含 unknown fields**)。对应 Go 的 `proto.Equal`。

    ★ 为什么不直接用 `a == b`:CPython 的 protobuf 有 upb / pure-Python 两套实现,
    只有 upb 的 `__eq__` 覆盖 unknown fields。CAS 的前置快照比较一旦漏掉 unknown,
    滚动升级期就会出现:新副本写了新字段 → 旧副本读出来(unknown)→ 与 expected
    "相等" → 覆盖写回并抹掉新字段,而 CAS 报告成功。用确定性序列化比较则与
    实现无关。

    ★ `deterministic=True` 只保证 map 有序;普通字段本就按编号写,unknown fields
    在解析顺序上追加 —— 对"同一份 bytes 解出来的记录 vs 它的克隆"这个实际用法
    是稳定的。
    """
    if a is None or b is None:
        return a is b
    if type(a) is not type(b):
        return False
    return a.SerializeToString(deterministic=True) == b.SerializeToString(deterministic=True)


def marshal_shard(rec) -> bytes:  # noqa: ANN001
    """序列化分片镜像。对应 Go 的 `marshalShard`。"""
    if rec is None:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "nil hub shard")
    return rec.SerializeToString()


def unmarshal_shard(pod: str, payload: bytes):
    """反序列化分片镜像并校验 pod 身份。对应 Go 的 `unmarshalShard`。

    ★ 两条判据的**顺序**是契约:
        空 hub_pod_name → 用 key 里的 pod 补齐(兼容早期未写该字段的记录);
        补齐后仍不等   → **报错**,不是"以 key 为准改掉它"。
      记录内的 pod 与 key 不符说明有人按错 key 写了镜像;此时无论信哪一边,
      另一边的账本都会错 —— 只能拒,让上层去查。
    """
    rec = allocator_pb2.HubShardStorageRecord()
    # ★ 标准解析:保留 unknown fields(§9 不变量 17)。
    rec.ParseFromString(payload)
    if rec.hub_pod_name == "":
        rec.hub_pod_name = pod
    if rec.hub_pod_name != pod:
        raise errcode.PandoraError(
            errcode.ErrInternal, "hub shard %s pod mismatch: %s", pod, rec.hub_pod_name
        )
    return rec


def unmarshal_assignment(player_id: int, payload: bytes):
    """反序列化玩家归属。对应 Go 的 inline `proto.Unmarshal` + bad proto 包装。"""
    rec = allocator_pb2.HubAssignmentStorageRecord()
    try:
        rec.ParseFromString(payload)
    except Exception as exc:  # noqa: BLE001 —— 解码失败一律归为存储损坏
        raise errcode.PandoraError(
            errcode.ErrInternal, "assignment %d bad proto: %s", player_id, exc, cause=exc
        ) from exc
    return rec


# ── 心跳状态机 ───────────────────────────────────────────────────────────────


def drain_rank(state: str) -> int:
    """把分片状态映射成排空等级(ready < draining < stopping)。

    心跳路径用它防止 allocator 标记的 draining/stopping 被 DS 上报的 ready 降级 ——
    不设这道序,一台正在被强制整合排空的 Hub 只要继续心跳就能把自己翻回 ready,
    缩容永远收不了尾。
    """
    if state == "draining":
        return 1
    if state == "stopping":
        return 2
    return 0  # "ready" / "" / 未知


def apply_heartbeat_state_to_shard(rec, state: str, ts_ms: int) -> None:  # noqa: ANN001
    """Model B 专用状态更新:容量 player_count 由 ledger 派生,**不接受 DS 实报覆盖**。

    对应 Go 的 `applyHeartbeatStateToShard`。四条分支的顺序即契约:

        warming                → 首个通过 Guard/授权的心跳即「DS 已就绪且可信」的
                                 直接证据 → ready;但 DS 首跳已报更高 drain 等级时
                                 采纳其上报,不强行 ready(否则一台开机就在排空的
                                 DS 会被拉回 ready 并开始接人)。
        state == ""            → 空上报不动状态(不能当成 ready:空是"没报",
                                 不是"我很好")。
        drain_rank(state) >= 当前 → 升级或同级 drain,采用 DS 上报。
        ready + draining + draining_since_ms == 0
                               → **存活恢复**:心跳超时误标的 draining 不是
                                 allocator 的主动意图,只是「DS 可能已死」的推断;
                                 一个健康心跳即该推断失效的直接证据。不放这一条,
                                 活着的 DS 被误判超时后会永久卡 draining。
        其余降级              → 保持不变(强制整合的 draining 是 sticky 的)。
    """
    if rec.state == "warming":
        rec.state = state if drain_rank(state) > 0 else "ready"
    elif state == "":
        pass
    elif drain_rank(state) >= drain_rank(rec.state):
        rec.state = state
    elif state == "ready" and rec.state == "draining" and rec.draining_since_ms == 0:
        rec.state = "ready"
    rec.last_heartbeat_ms = _require_int64("ts_ms", ts_ms)


def apply_heartbeat_to_shard(rec, player_count: int, state: str, ts_ms: int) -> None:  # noqa: ANN001
    """legacy 代际门路径的心跳应用(对账在线数 + 推进状态机 + 刷 last_heartbeat)。

    对应 Go 的 `applyHeartbeatToShard`。调用方须保证心跳已通过代际 / 授权校验 ——
    stale 心跳必须在调用本函数**之前** fail-closed 返回,零变更。
    """
    rec.player_count = _require_int32("player_count", player_count)
    apply_heartbeat_state_to_shard(rec, state, ts_ms)


def _require_int64(name: str, value: int) -> int:
    """int64 边界。proto 的 int64 字段越界会抛 ValueError,但那时可能已写了别的键。"""
    if not isinstance(value, int) or isinstance(value, bool):
        raise errcode.PandoraError(errcode.ErrInvalidArg, "%s must be an integer", name)
    if value < INT64_MIN or value > INT64_MAX:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "%s %d out of int64 range", name, value)
    return value


def _require_int32(name: str, value: int) -> int:
    """int32 边界(player_count / capacity 等)。"""
    if not isinstance(value, int) or isinstance(value, bool):
        raise errcode.PandoraError(errcode.ErrInvalidArg, "%s must be an integer", name)
    if value < INT32_MIN or value > INT32_MAX:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "%s %d out of int32 range", name, value)
    return value


class ShardTokenStaleError(errcode.PandoraError):
    """enforce 代际门下心跳令牌代际过期 / 缺失 → fail-closed。

    对应 Go 的 `errShardTokenStale`,**错误码与文案逐字一致**(ErrUnauthorized=8)。
    返回前对镜像**零变更**(不刷 player_count / state / last_heartbeat_ms / TTL,
    不进 active 索引),service 层据此**不刷 presence**:旧代际心跳不得保活、占位、
    伪造在场(审核 P1)。
    """

    def __init__(self) -> None:
        super().__init__(errcode.ErrUnauthorized, "hub heartbeat token generation stale")


# ── TTL 快照 ─────────────────────────────────────────────────────────────────


@dataclasses.dataclass(slots=True)
class AssignmentTTLState:
    """事务前归属的 TTL 快照。对应 Go 的 `assignmentTTLState`。

    ★ 补偿必须按**已流逝时间**扣减,不能把原本 30m 的记录恢复成永久键 ——
    永久归属键会让一个早该过期的玩家归属永远挡着他的下一次进场(§9.20)。
    """

    known: bool = False
    persistent: bool = False
    remaining_sec: float = 0.0
    sampled_at: float = 0.0  # time.monotonic()

    def restore_ttl(self) -> tuple[float, bool]:
        """返回 (补偿时应设的剩余 TTL 秒, 是否已自然到期)。

        已到期时调用方只保留 fencing 墓碑 —— 不能拿一个 0 / 负 TTL 去 SET
        (redis 会直接报错或立即删),更不能改成永久键。
        """
        if not self.known or self.persistent:
            return 0.0, False
        remaining = self.remaining_sec - (time.monotonic() - self.sampled_at)
        if remaining <= 0:
            return 0.0, True
        return remaining, False


async def read_assignment_ttl_state(pipe, key: str) -> AssignmentTTLState:  # noqa: ANN001
    """在 WATCH 事务内采样 TTL。对应 Go 的 `readAssignmentTTLState`。

    redis 的 PTTL 语义:`-1` = 键存在但无过期;`-2` = 键不存在。
    ★ `-2` 必须**报错**而不是当成"无过期":本函数只在"刚读到记录"之后调用,
    此刻键消失说明有并发写者绕过了本 CAS,继续按永久键补偿只会把脏归属钉死。
    """
    ttl_ms = int(await pipe.pttl(key))
    if ttl_ms == -1:
        return AssignmentTTLState(known=True, persistent=True, sampled_at=time.monotonic())
    if ttl_ms >= 0:
        return AssignmentTTLState(
            known=True, remaining_sec=ttl_ms / 1000.0, sampled_at=time.monotonic()
        )
    raise errcode.PandoraError(
        errcode.ErrInternal, "assignment key disappeared while sampling TTL: %s", key
    )


def _px(ttl_sec: float) -> dict:
    """把秒 TTL 翻成 redis-py 的 `px=` kwargs。`<=0` = 无过期(对齐 Go 的 `0`)。"""
    if ttl_sec is None or ttl_sec <= 0:
        return {}
    return {"px": int(ttl_sec * 1000)}


# 写后自检 / 补偿的独立超时(秒)。对应 Go 的 assignmentReconcileTimeout。
ASSIGNMENT_RECONCILE_TIMEOUT_SEC = 3.0

# 归属 CAS 的重试预算。对应 Go 的两个 casMaxRetry(8 / 3)。
ASSIGNMENT_CAS_MAX_RETRY = 8
ASSIGNMENT_DELETE_CAS_MAX_RETRY = 3
ASSIGNMENT_REVERT_CAS_MAX_RETRY = 3


class RedisHubRepo:
    """基于 redis.asyncio 的 HubRepo。对应 Go 的 `RedisHubRepo`。"""

    __slots__ = ("_rdb", "_fence")

    def __init__(self, rdb) -> None:  # noqa: ANN001
        self._rdb = rdb
        # 写者继任 fencing token 源(writer_fence.py)。None = 未启用,保持原行为。
        self._fence: wfence.WriterFence | None = None

    def set_writer_fence(self, fence: wfence.WriterFence | None) -> None:
        """注入写者继任 fencing(Model B 生产由 main 注入)。"""
        self._fence = fence

    # ── 分片镜像 ─────────────────────────────────────────────────────────

    async def get_shard(self, pod: str):
        """读分片镜像。不存在返回 None(对应 Go 的 `(nil, false, nil)`)。"""
        payload = await self._rdb.get(shard_key(pod))
        if payload is None:
            return None
        return unmarshal_shard(pod, payload)

    async def list_shards(self) -> list:
        """列出全部已登记分片(ListHubs / 候选遍历用)。

        ★ 镜像已过期但 SET 残留时**顺手 SREM 清理**(自愈)。不清的话
        `pandora:hub:shards` 会随 Pod 轮换单调膨胀,每次候选遍历都要为一批
        早已不存在的 pod 各打一次 GET —— 分配延迟随集群寿命线性劣化。
        """
        pods = await self._rdb.smembers(SHARDS_SET_KEY)
        out = []
        for raw in sorted(pods):
            pod = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            rec = await self.get_shard(pod)
            if rec is None:
                await self._rdb.srem(SHARDS_SET_KEY, pod)
                continue
            out.append(rec)
        return out

    async def create_shard(self, rec, shard_ttl_sec: float) -> None:  # noqa: ANN001
        """写分片镜像(TTL=shard_ttl)并加入 shards SET。**不进 active**,等首次心跳。

        ★ **init-only 语义**(审核二轮 CE7):用 `SET NX` 只在分片键不存在时初始化,
        已存在则**绝不覆盖** —— 两个并发 get_shard-miss 的种子调用(ensureShards /
        reconcile 新 pod 分支)不会互相把对方刚写入的心跳 / last_verified / 状态
        清回初始值。已存在分片的地址 / 容量刷新由 reconcile 的 `update_shard_with_lock`
        单调合并负责,不走本路径覆盖。

        ★ Redis Cluster 兼容:`shard_key{pod}` 与全局 `SHARDS_SET_KEY` 分属不同 slot,
        不能捆同一事务。① 单键 SET NX 初始化;② 独立 SADD 登记 membership
        (必须成功,否则 list_shards 漏这个分片)。两步幂等,失败重试可重入。

        ★ 启用 fence 后初始化也必须和同 slot 水位在一个 EXEC;否则失主旧写者可绕过
        其它全部 fenced 更新入口,**重新种出一个已被继任者移除的 shard**。
        """
        payload = marshal_shard(rec)
        key = shard_key(rec.hub_pod_name)
        px = _px(shard_ttl_sec)
        if self._fence is None:
            await self._rdb.set(key, payload, nx=True, **px)
        else:
            await wfence.fenced_pod_tx(
                self._rdb,
                self._fence,
                rec.hub_pod_name,
                [key],
                lambda pipe: pipe.set(key, payload, nx=True, **px),
            )
        # 全局 membership 是可重建的 superset 索引,跨 slot 只能独立幂等补齐;
        # 权威 shard 初始化已在上面的 {pod} 事务里受 writer fence 保护。
        await self._rdb.sadd(SHARDS_SET_KEY, rec.hub_pod_name)

    async def update_shard_with_lock(
        self, pod: str, max_retry: int, fn, shard_ttl_sec: float
    ) -> None:  # noqa: ANN001
        """WATCH/MULTI/EXEC 读-改-写分片。对应 Go 的 `UpdateShardWithLock`。

        CAS 冲突重试 `max_retry` 次,耗尽返 `ErrHubNoAvailable`。

        ★ `fn` 抛的异常必须与 redis / WatchError **分开**处理(对应 Go 的
        `txErr == fnErr` 那一支)。混在一起有两个后果:被冲突分支吞掉会变成
        「重试 N 次后 CAS 耗尽」(线上看到的原因是错的);把业务哨兵当存储故障
        会刷一条假告警。

        ★ 分片不存在时抛 `ErrHubNoAvailable` 并**立即冒泡**(不重试):
        重试一个不存在的键只是把同一个结论重复 N 次,还把 max_retry 的预算
        从"并发冲突"挪走。
        """
        key = shard_key(pod)
        watch_keys = wfence.fenced_watch_keys([key], pod, self._fence)

        for attempt in range(max_retry + 1):
            fn_raised = False
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(*watch_keys)
                    # 写者继任 fence:迟到旧写者 fail-closed 零写入。
                    advance = await wfence.guard_writer_fence(pipe, pod, self._fence)
                    payload = await pipe.get(key)
                    if payload is None:
                        fn_raised = True
                        raise errcode.PandoraError(
                            errcode.ErrHubNoAvailable, "hub shard %s not found", pod
                        )
                    rec = unmarshal_shard(pod, payload)
                    try:
                        result = fn(rec)
                        if result is not None and hasattr(result, "__await__"):
                            await result
                    except BaseException:
                        fn_raised = True
                        raise
                    new_payload = marshal_shard(rec)
                    pipe.multi()
                    advance(pipe)
                    # Cluster 兼容:WATCH/SET 只围 {pod} 单 slot;全局 SET 移出事务。
                    pipe.set(key, new_payload, **_px(shard_ttl_sec))
                    await pipe.execute()
            except WatchError:
                if fn_raised:
                    raise
                await wfence.cas_conflict_backoff(attempt)
                continue
            # shards membership re-ensure(独立命令,幂等;membership 已在 create_shard
            # 建立,best-effort:失败不影响权威镜像,list_shards 自愈 + 下次心跳补)。
            try:
                await self._rdb.sadd(SHARDS_SET_KEY, pod)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 —— best-effort,对齐 Go 的 `_ =`
                pass
            return
        raise errcode.PandoraError(
            errcode.ErrHubNoAvailable, "hub shard %s update concurrent retry exhausted", pod
        )

    async def heartbeat_shard(
        self,
        pod: str,
        player_count: int,
        state: str,
        ts_ms: int,
        token_gen: int,
        gen_required: bool,
        shard_ttl_sec: float,
    ) -> bool:
        """Hub DS 心跳上报(legacy 代际门路径)。对应 Go 的 `HeartbeatShard`。

        返回 `found`:分片不存在(孤儿 DS)返回 False 且**不建档**,由 biz 下发 stop。

        ★ 令牌代际校验必须在**任何镜像变更之前**(审核 P1)。过期 / 缺失代际的心跳
        一律 fail-closed 抛 `ShardTokenStaleError`,镜像零变更:不刷 player_count /
        state / last_heartbeat_ms / TTL,也不进 active 索引 —— 旧代际、无令牌的 DS
        不能借心跳保活、占位、伪造在场。stale 两种情形:
            ① 镜像已绑定代际(current_token_gen != 0)但心跳代际不等(**含 0**);
            ② gen_required(enforce 代际门开)但心跳无代际(token_gen == 0),
               挡的是「legacy gen0 关掉代际门」这条绕行。

        ★ gen 来自 Redis INCR 单调值,**精确相等**才算当前代际。旧实现用 JWT exp
        比较,而 exp 是秒精度:同一秒内两次重签得到相同 exp,旧令牌会被误判为当前代际。
        """
        _require_uint64("token_gen", token_gen)
        key = shard_key(pod)
        found = False
        async with self._rdb.pipeline(transaction=True) as pipe:
            await pipe.watch(*wfence.fenced_watch_keys([key], pod, self._fence))
            # 写者继任 fence:与令牌代际门同理,必须在任何镜像变更之前 fail-closed。
            advance = await wfence.guard_writer_fence(pipe, pod, self._fence)
            payload = await pipe.get(key)
            if payload is None:
                # 孤儿 DS:不建档(零写入),由 biz 回 stop。
                return False
            rec = unmarshal_shard(pod, payload)
            found = True
            if (rec.current_token_gen != 0 and token_gen != rec.current_token_gen) or (
                gen_required and token_gen == 0
            ):
                # 零变更返回:不 EXEC,镜像 / 索引 / TTL 全不动。
                raise ShardTokenStaleError()
            # —— 代际校验通过,方可变更镜像 ——
            apply_heartbeat_to_shard(rec, player_count, state, ts_ms)
            new_payload = marshal_shard(rec)
            pipe.multi()
            advance(pipe)
            pipe.set(key, new_payload, **_px(shard_ttl_sec))
            await pipe.execute()

        if found:
            # 全局索引:与 shard_key 不同 slot,各自独立命令。幂等;心跳高频,失败下次即补。
            await self._rdb.sadd(SHARDS_SET_KEY, pod)
            await self._rdb.zadd(ACTIVE_KEY, {pod: float(ts_ms)})
        return found

    async def remove_shard(self, pod: str) -> None:
        """删分片镜像 + 成员索引 + 全局 shards/active 登记。

        ★ per-pod 同 slot 的两把键必须**同事务**删,且受写者水位保护:删分片是
        不可回收的破坏性写 —— 失主旧写者删掉继任者刚建立的分片会让在场玩家的
        心跳 / 席位全部失去镜像。

        ★ 全局索引跨 slot,只能独立命令。走到这里说明本副本刚通过了同 pod 的水位
        比较,是当届写者;残留由 list_shards 自愈 + active 扫到已删镜像跳过兜底。
        """
        s_key, m_key = shard_key(pod), members_key(pod)

        def _mutate(pipe) -> None:  # noqa: ANN001
            pipe.delete(s_key)
            pipe.delete(m_key)

        await wfence.fenced_pod_tx(self._rdb, self._fence, pod, [s_key, m_key], _mutate)
        await self._rdb.srem(SHARDS_SET_KEY, pod)
        await self._rdb.zrem(ACTIVE_KEY, pod)

    async def range_stale_shards(self, threshold_ms: int) -> list[str]:
        """active ZSET 中 `last_heartbeat_ms <= threshold_ms` 且 `> 0` 的 pod。

        ★ `Min="(0"` 是**开区间**,排除从未心跳的 Mock 种子(score=0)。
        写成闭区间的话,每一轮超时扫描都会把所有刚建档、尚未首跳的分片判成
        「心跳超时」并标 draining —— 新 Pod 永远起不来。
        """
        _require_int64("threshold_ms", threshold_ms)
        members = await self._rdb.zrangebyscore(ACTIVE_KEY, "(0", threshold_ms)
        return [m.decode("utf-8") if isinstance(m, bytes) else m for m in members]

    async def remove_active(self, pod: str) -> None:
        """把 pod 移出 active ZSET(不再心跳扫描)。"""
        await self._rdb.zrem(ACTIVE_KEY, pod)

    # ── 玩家归属 ─────────────────────────────────────────────────────────

    async def get_assignment(self, player_id: int):
        """读玩家归属。不存在返回 None。

        ★ 两条读侧判据:

          ① **fencing 墓碑对业务不可见** —— 玩家确实无归属,只是水位还在
             (writer_fence.py ⑤a)。不隐藏的话,墓碑会被当成一条 hub_pod_name
             为空的"归属",上层拿它去路由会得到一个空 pod。

          ② **比本副本任期还新的记录**不能当普通归属继续用:这表示本副本已经被
             继任但尚未观察到失主 → `WriterSupersededError`。
             反过来,**旧 token 记录不能仅因换届就丢弃** —— 它可能是上届合法留下的
             当前归属,会在本届首次 CAS 时原子升级水位。丢弃它等于让在场玩家的
             归属在每次 allocator 换届时集体消失。

        ★ `writer_token == 0` 的记录直接放行:0 = 本字段上线前的旧记录 / 未启用
        fencing,按「尚无水位」处理(滚动升级双向兼容,§9 不变量 17)。
        判据不能写成 `if rec.writer_token > 0` 之外的方向 —— 把 0 当"最小 token"
        参与比较,会让所有历史记录在任何非零任期下都被判成"旧",全部被拒。
        """
        payload = await self._rdb.get(assign_key(player_id))
        if payload is None:
            return None
        rec = unmarshal_assignment(player_id, payload)
        if wfence.is_assignment_fence_tombstone(rec):
            return None
        if self._fence is not None and rec.writer_token != 0:
            held, mine = wfence.fence_snapshot(self._fence)
            if held and rec.writer_token > mine:
                raise wfence.WriterSupersededError()
        return rec

    async def set_assignment(self, rec, assignment_ttl_sec: float) -> None:  # noqa: ANN001
        """无条件写归属(旧接口,仅 dev / 测试路径)。

        ★ 启用写者 fencing 后它是**纯粹的 fencing 旁路** —— 无 WATCH 无比较,
        失主旧写者可借它覆盖继任者的归属 —— 故 fail-closed 拒绝。
        归属写一律走 `compare_and_swap_assignment`。
        """
        if self._fence is not None:
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "assignment unconditional Set is fencing-unsafe; use CompareAndSwapAssignment",
            )
        await self._rdb.set(
            assign_key(rec.player_id), rec.SerializeToString(), **_px(assignment_ttl_sec)
        )

    async def compare_and_swap_assignment(
        self, player_id: int, expected, next_rec, assignment_ttl_sec: float
    ) -> bool:  # noqa: ANN001
        """以玩家单键为线性化点精确 CAS 归属。对应 Go 的 `CompareAndSwapAssignment`。

        `expected=None` 表示仅当键不存在时创建;`next_rec=None` 表示仅当当前值
        **完整等于** expected 时删除。比较覆盖 unknown fields,滚动更新期间不会把
        新副本字段静默当成相同。返回 False 表示前置快照已变化,**零写入**。

        ★ 每玩家持久写者水位(R10 P0-4;R11 收口两处交错):assignment 键无 hashtag,
        进不了 {pod} slot 事务,用不了 `pandora:hub:wfence:{pod}`。水位记在归属记录
        **自身**里 —— 同一 key 的 WATCH/MULTI/EXEC 天然原子,比较与写入同线性化点。

        ★ 租约必须在**事务内**读(R11 问题 B,检查后执行)。读在重试循环之外时,
        旧写者暂停任意长后仍带着陈旧的 mine 走完事务;放进回调后失主至迟在下一次
        attempt 被发现,窗口收敛到「本次 EXEC」,上界即 etcd 租约 TTL。

        ★ 删除写**墓碑**而非裸 DEL(R11 问题 A,删除即复位):裸 DEL 会把水位随业务
        记录一起抹掉,于继任者「创建 → 合法删除」之后,失主旧写者看到键不存在便能
        以旧 token 重建归属(借尸还魂)。

        ★ 写后自检 + 精确补偿:租约可能在「事务内读 current() 之后、EXEC 之前」丢失。
        该窗口无法用 Redis 消除(per-player 键进不了 {pod} 水位事务域,而给每个玩家
        配同 slot 水位键要迁 key scheme + 回填,§9.16 禁止停服迁移)。标准做法是
        **不追求消除窗口,而是限制损害**:写后立刻自检租约,失主就精确撤销刚才这笔,
        把脏归属的存活时间从"直到下次被触碰"压成一个补偿往返。
        """
        _require_uint64("player_id", player_id)
        if (
            player_id == 0
            or (expected is not None and expected.player_id != player_id)
            or (next_rec is not None and next_rec.player_id != player_id)
        ):
            raise errcode.PandoraError(errcode.ErrInvalidArg, "assignment CAS player_id mismatch")
        key = assign_key(player_id)

        for attempt in range(ASSIGNMENT_CAS_MAX_RETRY):
            matched = False
            written_token = 0
            intended = None
            previous_ttl = AssignmentTTLState()
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    held, mine = wfence.fence_snapshot(self._fence)
                    if self._fence is not None and not held:
                        raise wfence.WriterSupersededError()

                    payload = await pipe.get(key)
                    current = None
                    if payload is None:
                        if expected is not None:
                            return False  # 期望有值却不存在 → 零写入
                    else:
                        current = unmarshal_assignment(player_id, payload)
                        # 水位只进不退:记录已被更高代写者触碰 → 本副本永久出局(零写入)。
                        # 0 = 本字段上线前的旧记录 / 未启用 fencing,按"尚无水位"放行。
                        if self._fence is not None and current.writer_token > mine:
                            exc = wfence.WriterSupersededError()
                            # ★ 证据不能丢:Go 是 `(false, err)` 多返回,调用方仍能从
                            # 别处重读;Python 用异常传播时不挂上去,调用方就得再打一次
                            # GET —— 多一个 TOCTOU 窗口。挂在声明式 slots 上(errcode.py)。
                            exc.current_record = current
                            raise exc
                        # 墓碑 = 业务上无归属、水位仍在。比较按"键不存在"处理,水位比较照旧。
                        if wfence.is_assignment_fence_tombstone(current):
                            if expected is not None:
                                return False
                        elif expected is None or not proto_equal(current, expected):
                            return False

                    if expected is not None:
                        previous_ttl = await read_assignment_ttl_state(pipe, key)

                    write = next_rec
                    if next_rec is None and self._fence is not None:
                        write = wfence.new_assignment_fence_tombstone(player_id, mine)
                    new_payload = None
                    if write is not None:
                        if self._fence is not None and write.writer_token != mine:
                            # 克隆后盖水位:调用方(biz)可能复用同一 message 做后续比较,
                            # 不能就地改写它的字段。
                            write = clone(write)
                            write.writer_token = mine
                        new_payload = write.SerializeToString()
                    if self._fence is not None:
                        written_token = mine
                        intended = clone(write)

                    pipe.multi()
                    if write is None:
                        pipe.delete(key)  # 未启用 fencing:保持旧行为
                    elif next_rec is None:
                        pipe.set(
                            key, new_payload, **_px(wfence.ASSIGNMENT_FENCE_TOMBSTONE_TTL_SEC)
                        )
                    else:
                        pipe.set(key, new_payload, **_px(assignment_ttl_sec))
                    await pipe.execute()
                    matched = True
            except WatchError:
                await wfence.cas_conflict_backoff(attempt)
                continue
            except asyncio.CancelledError:
                raise
            except wfence.WriterSupersededError:
                raise
            except errcode.PandoraError:
                raise
            except Exception as exc:  # noqa: BLE001
                # EXEC 可能已生效但响应丢失。用独立有界超时回读;若确实是本次写,
                # 再按操作捕获的 token 做失租补偿。
                if self._fence is not None and written_token != 0 and intended is not None:
                    await self._reconcile_ambiguous_write(
                        player_id,
                        key,
                        written_token,
                        intended,
                        expected,
                        previous_ttl,
                        event="hub_assignment_ambiguous_write_reconcile_failed",
                    )
                raise exc

            if matched:
                await self._reconcile_owned(
                    player_id, key, written_token, intended, expected, previous_ttl
                )
            return matched

        # 高并发下 WATCH 连续冲突只表示 expected 已不再稳定;交给上层重读最新归属
        # 重试,零写入。
        return False

    async def delete_assignment_if_pod_matches(self, player_id: int, pod: str) -> bool:
        """CAS 删玩家归属:仅当当前归属仍指向 `pod` 才删。

        ★ 防止 ReleaseHub 读到旧归属后、并发 Assign/Transfer 已写入新归属时无条件
        DEL 误删新归属(写序铁律:删除必须带前置校验)。已不存在或已指向其它分片 →
        返回 False 不删。

        ★ 与 `compare_and_swap_assignment` 同契约:事务内读租约,删除写墓碑而非裸 DEL,
        否则本路径同样是「删除即复位」的水位抹除口。
        """
        _require_uint64("player_id", player_id)
        key = assign_key(player_id)

        for attempt in range(ASSIGNMENT_DELETE_CAS_MAX_RETRY):
            deleted = False
            written_token = 0
            intended = None
            expected = None
            previous_ttl = AssignmentTTLState()
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    held, mine = wfence.fence_snapshot(self._fence)
                    if self._fence is not None and not held:
                        raise wfence.WriterSupersededError()

                    payload = await pipe.get(key)
                    if payload is None:
                        return False  # 已不存在,幂等视为无需删
                    rec = unmarshal_assignment(player_id, payload)
                    if self._fence is not None and rec.writer_token > mine:
                        exc = wfence.WriterSupersededError()
                        exc.current_record = rec
                        raise exc
                    if wfence.is_assignment_fence_tombstone(rec):
                        return False  # 已是墓碑:业务上无归属,幂等无需删
                    if rec.hub_pod_name != pod:
                        return False  # 并发 Assign/Transfer 已指向新分片,不能删

                    new_payload = None
                    if self._fence is not None:
                        previous_ttl = await read_assignment_ttl_state(pipe, key)
                        expected = clone(rec)
                        intended = wfence.new_assignment_fence_tombstone(player_id, mine)
                        written_token = mine
                        new_payload = intended.SerializeToString()

                    pipe.multi()
                    if new_payload is None:
                        pipe.delete(key)  # 未启用 fencing:保持旧行为
                    else:
                        pipe.set(
                            key, new_payload, **_px(wfence.ASSIGNMENT_FENCE_TOMBSTONE_TTL_SEC)
                        )
                    await pipe.execute()
                    deleted = True
            except WatchError:
                await wfence.cas_conflict_backoff(attempt)
                continue  # WATCH 期间归属被改写,重读再判
            except asyncio.CancelledError:
                raise
            except wfence.WriterSupersededError:
                raise
            except errcode.PandoraError:
                raise
            except Exception as exc:  # noqa: BLE001
                if self._fence is not None and written_token != 0 and intended is not None:
                    await self._reconcile_ambiguous_write(
                        player_id,
                        key,
                        written_token,
                        intended,
                        expected,
                        previous_ttl,
                        event="hub_assignment_delete_ambiguous_reconcile_failed",
                    )
                raise exc

            if deleted and self._fence is not None:
                await self._reconcile_owned(
                    player_id, key, written_token, intended, expected, previous_ttl
                )
            return deleted

        # 重试耗尽:归属正被并发频繁改写,安全侧不删(新归属为准)。
        return False

    # ── 写后自检 / 精确补偿 ──────────────────────────────────────────────

    async def _reconcile_owned(
        self,
        player_id: int,
        key: str,
        written_token: int,
        intended,  # noqa: ANN001
        expected,  # noqa: ANN001
        previous_ttl: AssignmentTTLState,
    ) -> None:
        """写成功后的租约自检。对应 Go 的 `ensureAssignmentWriteOwned`(带独立 ctx)。

        ★ 用 `shield` 把补偿与调用方的取消隔开:Go 显式建了一个新的 `context.Background()`
        + 3s 超时,理由是「EXEC 已提交 + 请求 ctx 已取消」时补偿天然发不出去。
        Python 里直接 `await` 会被外层取消一起干掉 —— 同一个坑,同一个解法。
        """
        if self._fence is None:
            return
        await asyncio.shield(
            asyncio.wait_for(
                self._ensure_assignment_write_owned(
                    player_id, key, written_token, intended, expected, previous_ttl
                ),
                timeout=ASSIGNMENT_RECONCILE_TIMEOUT_SEC,
            )
        )

    async def _reconcile_ambiguous_write(
        self,
        player_id: int,
        key: str,
        written_token: int,
        intended,  # noqa: ANN001
        expected,  # noqa: ANN001
        previous_ttl: AssignmentTTLState,
        *,
        event: str,
    ) -> None:
        """EXEC 结果不明(响应丢失 / 连接断)时:回读确认后再补偿。

        ★ 回读**必须**用完整相等比较(`_assignment_write_applied`),不能只看 token:
        同一届内另一个并发请求也带相同 token,按 token 判会把别人的写当成自己的,
        补偿时直接抹掉。
        """

        async def _run() -> None:
            applied = await self._assignment_write_applied(key, intended)
            if applied:
                await self._ensure_assignment_write_owned(
                    player_id, key, written_token, intended, expected, previous_ttl
                )

        try:
            await asyncio.shield(
                asyncio.wait_for(_run(), timeout=ASSIGNMENT_RECONCILE_TIMEOUT_SEC)
            )
        except asyncio.CancelledError:
            raise
        except wfence.WriterSupersededError:
            raise
        except Exception as exc:  # noqa: BLE001
            # 回读本身失败:不掩盖根因(调用方仍会拿到原始异常),只留一条痕。
            plog.get().error(event, player_id=player_id, token=written_token, err=str(exc))

    async def _assignment_write_applied(self, key: str, intended) -> bool:  # noqa: ANN001
        """当前值是否**完整等于**本次 intended。对应 Go 的 `assignmentWriteApplied`。"""
        payload = await self._rdb.get(key)
        if payload is None:
            return False
        current = allocator_pb2.HubAssignmentStorageRecord()
        current.ParseFromString(payload)
        return proto_equal(current, intended)

    async def _ensure_assignment_write_owned(
        self,
        player_id: int,
        key: str,
        written_token: int,
        intended,  # noqa: ANN001
        expected,  # noqa: ANN001
        previous_ttl: AssignmentTTLState,
    ) -> None:
        """用**本次事务实际写入的 token** 做写后自检。

        ★ 判据是 `held and current_token == written_token` —— **相等**,不是 `>=`。
        同进程快速再选到更大 token 也必须撤销旧届写入:那一笔是以旧任期的名义写的,
        新任期不该继承它(继承等于让一次失租悄悄"洗白")。
        """
        if self._fence is None:
            return
        held, current_token = wfence.fence_snapshot(self._fence)
        if held and current_token == written_token:
            return
        await self._revert_assignment_write(
            player_id, key, written_token, intended, expected, previous_ttl
        )

    async def _revert_assignment_write(
        self,
        player_id: int,
        key: str,
        written_token: int,
        intended,  # noqa: ANN001
        expected,  # noqa: ANN001
        previous_ttl: AssignmentTTLState,
    ) -> None:
        """精确撤销「本副本已不再持有那一届租约时写下的那一笔」。

        ★ 精确性保证(绝不误删别人的写):撤销走同一把 key 的 WATCH/MULTI/EXEC,
        且**只在当前值仍与本次 intended 完整相等时**才动手。只比较 token 不够 ——
        同一届内另一个并发请求也带相同 token,按 token 撤销会误删后写。
        继任者或同届后写一律原样保留。

        撤销目标:
          - expected 非空 → 恢复成事务前的那条记录(我的写是覆盖,撤销即还原);
          - expected 为空 → 我的写是"创建 / 在墓碑上重建",撤销即回到 fencing 墓碑
            (**不能裸 DEL**:那会抹掉水位,重新打开借尸还魂的门)。

        ★ 补偿不能把水位回退到 expected 的旧 token:业务内容恢复,fencing 水位保持
        至少为本次已触达的 token。

        无论撤销成功与否,最后都抛 `WriterSupersededError` —— 撤销失败不掩盖根因,
        调用方仍必须拿到"你已不是写者"。
        """
        if self._fence is None:
            return
        restore = None
        if expected is not None:
            restore_record = clone(expected)
            if restore_record.writer_token < written_token:
                restore_record.writer_token = written_token
            restore = restore_record.SerializeToString()

        for attempt in range(ASSIGNMENT_REVERT_CAS_MAX_RETRY):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    payload = await pipe.get(key)
                    if payload is None:
                        break  # 已被继任者删掉,无需撤销
                    cur = unmarshal_assignment(player_id, payload)
                    if cur.writer_token != written_token or not proto_equal(cur, intended):
                        break  # 继任者或同届后写,原样保留
                    pipe.multi()
                    if restore is not None:
                        ttl_sec, expired = previous_ttl.restore_ttl()
                        if expired:
                            tomb = wfence.new_assignment_fence_tombstone(player_id, written_token)
                            pipe.set(
                                key,
                                tomb.SerializeToString(),
                                **_px(wfence.ASSIGNMENT_FENCE_TOMBSTONE_TTL_SEC),
                            )
                        else:
                            pipe.set(key, restore, **_px(ttl_sec))
                    else:
                        # 回到墓碑:水位必须留存,否则又给"删除即复位"开了门。
                        tomb = wfence.new_assignment_fence_tombstone(player_id, written_token)
                        pipe.set(
                            key,
                            tomb.SerializeToString(),
                            **_px(wfence.ASSIGNMENT_FENCE_TOMBSTONE_TTL_SEC),
                        )
                    await pipe.execute()
            except WatchError:
                await wfence.cas_conflict_backoff(attempt)
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                plog.get().error(
                    "hub_assignment_revert_after_lease_loss_failed",
                    player_id=player_id,
                    token=written_token,
                    err=str(exc),
                    hint="脏归属等继任者下次 CAS 覆盖;spawn gate 由 Admission 侧会话复核兜住",
                )
                break
            break
        raise wfence.WriterSupersededError()

    # ── 队伍同分片提示 ───────────────────────────────────────────────────

    async def get_team_shard(self, team_id: int) -> str | None:
        """读队伍同分片提示。不存在返回 None。"""
        raw = await self._rdb.get(team_key(team_id))
        if raw is None:
            return None
        return raw.decode("utf-8") if isinstance(raw, bytes) else raw

    async def set_team_shard(self, team_id: int, pod: str, assignment_ttl_sec: float) -> None:
        """写「队伍→分片」提示键。

        ★ `team_key` 无 hashtag,与任何 {pod} 水位不同 slot,**做不成原子 fencing**。
        该键是带 TTL 的**软提示**(只影响队友是否被优先安排到同分片,不参与准入 /
        归属 / 容量判定),失主旧写者写脏它最多让一次组队分流不理想,TTL 到期即自愈。
        故降级为入口级写者校验 —— 这是 `require_writer_held` 允许使用的场景之一。
        """
        wfence.require_writer_held(self._fence)
        await self._rdb.set(team_key(team_id), pod, **_px(assignment_ttl_sec))

    # ── 分片成员反向索引 ─────────────────────────────────────────────────

    async def add_shard_member(
        self, pod: str, player_id: int, assignment_ttl_sec: float
    ) -> None:
        """把 player_id 记入分片成员反向索引(强制整合枚举玩家用)。

        `assignment_ttl_sec <= 0` 表示**持久化**到 explicit Departure / assignment
        cleanup,避免长连玩家从 drain 枚举里消失。

        ★ `members_key` 与 pod 同 hashtag,可原子 fencing。虽然成员索引漂移不影响
        正确性(强制整合有 DS drain 心跳双通道兜底),但它是服务端权威搬迁的枚举源,
        失主旧写者往里加 / 删成员会让整合扫到错误的玩家集合 —— 能原子挡就挡。
        """
        member = str(_require_uint64("player_id", player_id))
        key = members_key(pod)

        def _mutate(pipe) -> None:  # noqa: ANN001
            pipe.sadd(key, member)
            if assignment_ttl_sec > 0:
                pipe.pexpire(key, int(assignment_ttl_sec * 1000))
            else:
                pipe.persist(key)

        await wfence.fenced_pod_tx(self._rdb, self._fence, pod, [key], _mutate)

    async def remove_shard_member(self, pod: str, player_id: int) -> None:
        """把 player_id 移出分片成员反向索引。"""
        member = str(_require_uint64("player_id", player_id))
        key = members_key(pod)
        await wfence.fenced_pod_tx(
            self._rdb, self._fence, pod, [key], lambda pipe: pipe.srem(key, member)
        )

    async def list_shard_members(self, pod: str) -> list[int]:
        """列出分片成员反向索引中的 player_id。脏成员跳过(对齐 Go 的 `continue`)。"""
        members = await self._rdb.smembers(members_key(pod))
        out: list[int] = []
        for raw in members:
            pid = parse_decimal_uint64(raw)
            if pid is None:
                continue
            out.append(pid)
        return out

    # ── transfer cleanup saga 索引 ───────────────────────────────────────

    async def register_transfer_cleanup(self, source_pod: str, ref: TransferCleanupRef) -> None:
        """必须在切换 assignment CAS **之前**成功。

        ref 绑定目标 assignment_id,CAS loser 只能删除自己的 ref,不能误删并发 winner。
        记录**无 TTL**;route / cleanup 不依赖过期。

        ★ 全局 pod 索引先写:任一命令结果不明时,调用方**不得**发布 assignment;
        留下的 orphan 由 reconciler 清掉。反过来(先写 per-pod ref 再写全局索引)
        会出现「ref 在、reconciler 扫不到这个 pod」的永久漏扫。

        ★ 全局索引跨 slot,进不了 per-pod 事务,只能入口级校验;它是持久 superset,
        多写一个 pod 只让 reconciler 多扫一轮,不破坏不变量。
        """
        if source_pod.strip() == "" or not ref.valid():
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "complete Hub transfer cleanup ref required"
            )
        wfence.require_writer_held(self._fence)
        await self._rdb.sadd(TRANSFER_CLEANUP_PODS_KEY, source_pod)
        key = transfer_cleanup_key(source_pod)
        member = encode_transfer_cleanup_ref(ref)
        await wfence.fenced_pod_tx(
            self._rdb, self._fence, source_pod, [key], lambda pipe: pipe.sadd(key, member)
        )

    async def remove_transfer_cleanup(self, source_pod: str, ref: TransferCleanupRef) -> None:
        """摘索引 = 宣告该 ref 的清理已完成。

        ★ 与注册同受水位约束:失主旧写者摘掉继任者仍在处理的 ref,会让旧 owner 的
        seat / session **永远没人退**。
        """
        if source_pod.strip() == "" or not ref.valid():
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "complete Hub transfer cleanup ref required"
            )
        key = transfer_cleanup_key(source_pod)
        member = encode_transfer_cleanup_ref(ref)
        await wfence.fenced_pod_tx(
            self._rdb, self._fence, source_pod, [key], lambda pipe: pipe.srem(key, member)
        )

    async def list_transfer_cleanup_pods(self) -> list[str]:
        """列出待清理 saga 的 source pod(持久 superset,不因空集合删除)。"""
        members = await self._rdb.smembers(TRANSFER_CLEANUP_PODS_KEY)
        return sorted(m.decode("utf-8") if isinstance(m, bytes) else m for m in members)

    async def list_transfer_cleanups(self, source_pod: str) -> list[TransferCleanupRef]:
        """列出某 source pod 下的全部待清理 ref。不合法的成员跳过。"""
        if source_pod.strip() == "":
            raise errcode.PandoraError(errcode.ErrInvalidArg, "source Hub pod required")
        raw_members = await self._rdb.smembers(transfer_cleanup_key(source_pod))
        out: list[TransferCleanupRef] = []
        for item in raw_members:
            ref = decode_transfer_cleanup_ref(item)
            if ref is not None:
                out.append(ref)
        return out

    # ── 切线冷却 ─────────────────────────────────────────────────────────

    async def try_transfer_cooldown(self, player_id: int, cooldown_sec: float) -> bool:
        """玩家主动切线防刷占坑(SET NX EX)。

        冷却窗口内首次切线返回 True 并占坑;窗口内再切返回 False(应拒绝)。

        ★ `cooldown_sec <= 0` **视为不限流,恒返回 True** —— 与 conf.py 的
        「负值 = 显式关闭切线冷却」是同一条契约。写成"<=0 用默认值"会让一份
        写着关闭的 yaml 悄悄开着闸。

        ★ 该键无 hashtag,做不成原子 fencing;它是**防刷限流**键(带 TTL,不参与
        准入 / 归属 / 容量判定),失主旧写者动它最多让某个玩家多切一次线,TTL 到期
        自愈。故降级为入口级写者校验。
        """
        if cooldown_sec <= 0:
            return True  # 不限流
        wfence.require_writer_held(self._fence)
        ok = await self._rdb.set(
            transfer_cooldown_key(player_id), b"1", nx=True, px=int(cooldown_sec * 1000)
        )
        return bool(ok)

    async def clear_transfer_cooldown(self, player_id: int) -> None:
        """清除切线冷却占坑(切线失败时释放,让玩家可立即重试)。best-effort。"""
        wfence.require_writer_held(self._fence)
        await self._rdb.delete(transfer_cooldown_key(player_id))

    # ── 继任者水位推扫 ───────────────────────────────────────────────────

    async def advance_writer_fences(self) -> None:
        """把**全部已知 pod** 的 fence 推进到本届 token。对应 Go 的 `AdvanceWriterFences`。

        ★ 逐 slot 懒推进只在继任者**写过**的 slot 生效;推扫消灭「继任者尚未触碰的
        pod」这块盲区 —— 完成后前任写者在任何 {pod} slot 上的席位 / 账本写永久出局,
        其签出的票在 Admission 点必然找不到席位。

        幂等,可在同一届内重复调用(cur == mine 直接跳过)。任一 pod 推扫遇到更大
        token(自己已被继任)立即抛 `WriterSupersededError`。fence 未注入时 no-op。
        """
        if self._fence is None:
            return
        held, mine = wfence.fence_snapshot(self._fence)
        if not held:
            raise wfence.WriterSupersededError()
        await self._advance_writer_fences_to(mine)

    async def advance_writer_fences_for_token(self, token: int) -> None:
        """用**显式 token** 推扫。对应 Go 的 `AdvanceWriterFencesForToken`。

        ★ 供写者租约的「接流前激活钩子」调用:此时本副本已当选、尚未宣告持有,
        `current()` 故意还不返回 held —— **推扫成功是获得写权的前置条件**,
        不能反过来依赖写权(那是循环依赖,钩子永远跑不起来)。
        """
        _require_uint64("token", token)
        if token == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "hub writer fence advance requires a non-zero token"
            )
        await self._advance_writer_fences_to(token)

    async def _advance_writer_fences_to(self, mine: int) -> None:
        """枚举全部已知 pod(分片 SET ∪ 待清理 saga 源 pod)并逐 slot 只进不退推进。

        ★ 两个来源都要枚举:只扫分片 SET 会漏掉「分片已删、但 cleanup saga 还挂着」
        的 pod,前任在那些 slot 上仍可写。
        """
        pods: set[str] = set()
        for raw in await self._rdb.smembers(SHARDS_SET_KEY):
            pods.add(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        for pod in await self.list_transfer_cleanup_pods():
            pods.add(pod)
        for pod in sorted(pods):
            await self._advance_writer_fence_pod(pod, mine)

    async def _advance_writer_fence_pod(self, pod: str, mine: int) -> None:
        """单 pod 水位推进:WATCH/MULTI/EXEC 只进不退。"""
        key = wfence.wfence_key(pod)
        for attempt in range(wfence.CAS_MAX_RETRY):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    cur = wfence.parse_fence_value(pod, await pipe.get(key))
                    if cur > mine:
                        raise wfence.WriterSupersededError()
                    if cur == mine:
                        return
                    pipe.multi()
                    pipe.set(key, str(mine))
                    await pipe.execute()
            except WatchError:
                await wfence.cas_conflict_backoff(attempt)
                continue
            return
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "hub writer fence advance contention on pod %s", pod
        )
