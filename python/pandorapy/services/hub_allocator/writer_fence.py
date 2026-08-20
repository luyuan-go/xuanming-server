"""hub_allocator 写者继任 fencing —— 对应 Go 侧 internal/data/writer_fence.go
(INC-20260722-004 R9 P0-7 收口;docs/design/session-generation-rollout.md §5)。

语义:每届 hub_allocator 写者持有一个严格单调递增的 fencing token(etcd 继任租约,
`pandorapy.writerlease`)。所有 per-{pod} 授权 / 容量账本事务在同一 WATCH/MULTI/EXEC
内比较并推进与业务键同 slot 的 fence key:

    pandora:hub:wfence:{<pod>} → 已见最大写者 token(十进制字符串,持久键)

    cur > mine  → 拒绝(WriterSupersededError,零写入):继任者已触达此 slot,
                  前任的迟到写永久出局(即使前任进程尚未察觉失主);
    cur < mine  → 本事务顺带把 fence 推进到 mine(SET 进同一 EXEC);
    cur == mine → 直接放行。

逐 slot 懒推进的正确性:继任者第一次写某 {pod} slot 起,前任在该 slot 永久被拒;
继任者尚未触达的 slot 上,前任写在语义上线性化于交接之前(继任者随后读到并接续),
不构成账本冲突。fence key 故意**不设 TTL、RemoveShard 也不删**:fencing 水位必须
比业务记录长寿,否则删除即复位,迟到旧写者可借尸还魂。

per-player assignment key(`pandora:hub:player:<id>`)无 hashtag、与任何 {pod} slot
不可同事务,用不了上面的 {pod} 水位键。该路径由五层组合收口(见 repo.py 的
`compare_and_swap_assignment` docstring),本模块提供其中 ⑤ 的两个原语:
`is_assignment_fence_tombstone` / `new_assignment_fence_tombstone`。

────────────────────────────────────────────────────────────────────────────
★ 移植期最容易反向抄错的一处:**`current()` 的返回顺序两边相反**

    Go   `WriterFence.Current() (token uint64, held bool)`
    Py   `writerlease.Lease.current() -> (held: bool, token: int)`

    Python 侧沿用 `pandorapy.writerlease` 已有的 `(held, token)`,**不改公共件**
    (handoff §5.5:改公共件会打穿正确的调用方)。抄成 `token, held = ...` 时,
    非零 token 会被当成 `held=True`(真值),而 token 变成 `False`(=0)——
    于是"尚无水位"的记录反而被判成"来自 0 号写者",所有比较恒成立:
    **fence 静默失效,没有任何运行期信号**。所以本模块只经 `fence_snapshot()`
    这一个入口取值,别处不许直接解包。
"""

from __future__ import annotations

import asyncio
import random
import re
import uuid
from collections.abc import Callable, Sequence
from typing import Protocol

from pandora.hub.v1 import allocator_pb2
from redis.exceptions import WatchError

from pandorapy import errcode

# ── key 模板(与 Go 逐字一致,两栈并存时必须落同一把 key)───────────────────
#
# ★ 与 shardKey / authKey / capacityLedgerKeys 同 hashtag {pod} 同 slot,
# 可捆进同一个 Redis Cluster 事务(decision-revisit-hub-crossslot.md 单 slot 铁律)。
_WFENCE_KEY_FMT = "pandora:hub:wfence:{{{pod}}}"


def wfence_key(pod: str) -> str:
    """`pandora:hub:wfence:{<pod>}`。花括号是 Redis Cluster hashtag,不是格式化占位符。"""
    return _WFENCE_KEY_FMT.format(pod=pod)


# uint64 上界。★ Go 的 `strconv.ParseUint(raw, 10, 64)` 对负数、`+1`、前后空白、
# 超过 64 位的值全部报错;Python 的 `int()` 三种都照收(`int("-1")` = -1,
# `int(" 1 ")` = 1,大整数无限精度)。照抄 Go 的写法在 Python 里等于把这三类
# 脏值静默接受成合法水位 —— 负数水位恒小于任何 token,水位形同虚设。
UINT64_MAX = (1 << 64) - 1


class WriterSupersededError(errcode.PandoraError):
    """本副本的写者租约已失效 / 被更新写者继任 → fail-closed 零写入。

    对应 Go 的 `ErrWriterSuperseded`,**错误码与文案逐字一致**:
    `ErrUnavailable` 语义 = 对调用方可重试(重试会被路由到新写者副本),
    对本副本是终态拒绝。

    ★ 做成子类而不是每处新建一个 `PandoraError(ErrUnavailable, ...)`:
    Go 侧 biz 用 `errors.Is(err, ErrWriterSuperseded)` 判定"要不要把请求引到
    新写者"。Python 侧若只靠比对 code,会把**所有** ErrUnavailable(Redis 抖动、
    CAS 预算耗尽…)一并当成失主,让本来该重试本副本的请求全部被误导。
    """

    def __init__(self) -> None:
        super().__init__(
            errcode.ErrUnavailable,
            "hub allocator writer lease superseded; retry against current writer",
        )


class WriterFence(Protocol):
    """当前写者的 fencing token 源。

    `pandorapy.writerlease.Lease` 满足此协议;None = 未启用继任租约
    (dev / mock / 单副本 Recreate 部署,保持原行为)。
    """

    def current(self) -> tuple[bool, int]:
        """返回 (是否持有写者租约, token)。token 历届严格单调递增。"""


def fence_snapshot(fence: WriterFence | None) -> tuple[bool, int]:
    """取一次 (held, token) 快照 —— **全模块唯一的解包点**(见模块头)。

    fence 为 None 时返回 `(True, 0)`:未启用 fencing 的部署一律放行,
    token=0 会让后续比较全部走"尚无水位"分支。
    """
    if fence is None:
        return True, 0
    held, token = fence.current()
    return bool(held), int(token)


# ── 每玩家 fencing 墓碑(覆盖边界 ⑤a)────────────────────────────────────────

# 每玩家 fencing 墓碑的存活时间。对应 Go 的 assignmentFenceTombstoneTTL。
#
# 墓碑只需活过「旧写者仍可能以为自己持有租约」的最长时间。归属 CAS 在**事务内**
# 读 `fence.current()`,因此旧写者带进 EXEC 的 token 至多陈旧 = etcd 租约剩余寿命
# (writerlease.DEFAULT_LEASE_TTL_SEC=15s)+ 一次 EXEC 往返。5 分钟 ≈ 20× 租约 TTL,
# 给足时钟偏移与 GC / 暂停余量。
#
# 取有限 TTL 而非持久键:墓碑数量 = 曾有归属的玩家数,持久化会无界增长(§9.24)。
ASSIGNMENT_FENCE_TOMBSTONE_TTL_SEC = 300.0


def is_assignment_fence_tombstone(rec) -> bool:  # noqa: ANN001
    """归属记录是否只是 fencing 墓碑。

    判据 = `hub_pod_name == ""`:真实归属必有分片身份(分片是归属的**本体**),
    没有分片名的记录在业务上就等于"无归属"。

    ★ 判据不能改成"看 assignment_id 是否为空":新墓碑复用 `assignment_id` 作为
    本次删除的 operation-scoped 唯一身份,而**旧墓碑该字段为空**。按 assignment_id
    判会把旧墓碑认成真实归属 —— 于是滚动升级期旧墓碑被当成"玩家还在某个 Hub 上",
    再入被拒、玩家进不去场景(§9.20)。按 hub_pod_name 判则新旧墓碑都成立,
    不改 wire schema、不依赖新 PB,双向兼容。
    """
    return rec is not None and rec.hub_pod_name == ""


def new_assignment_fence_tombstone(player_id: int, token: int):
    """构造一次删除专属的墓碑。对应 Go 的 newAssignmentFenceTombstone。

    ★ `assignment_id` 必须每次新铸(uuid4)。只带 token 无法区分同一任期内的
    A / B 两次删除,会产生 tombstone ABA:A 的迟到补偿会匹配上 B 写的墓碑并
    把它"撤销"回 A 的旧值 —— 一个已被合法删除的归属就此复活。
    """
    return allocator_pb2.HubAssignmentStorageRecord(
        player_id=player_id,
        writer_token=token,
        assignment_id=str(uuid.uuid4()),
    )


# ── CAS 冲突退避 ─────────────────────────────────────────────────────────────

# Go 侧常量,逐个照抄(hub_auth_repo.go)。
CAS_MAX_RETRY = 8
HUB_AUTH_CAS_RETRIES = 64


async def cas_conflict_backoff(attempt: int) -> None:
    """WATCH/CAS 乐观并发冲突后按指数 + 抖动退避。对应 Go 的 casConflictBackoff。

    ★ 零间隔紧循环在高并发争用同 {pod} / 同玩家键时会互相踩踏:每次 EXEC 成功都会
    打断其余全部在途 WATCH 事务,落后者可能连续输掉全部预算(2026-07-21 存量 flake:
    32 并发 AssignHub 把 64 次预算全部冲突耗尽后报 exhausted)。

    首次冲突立即重试保住低争用延迟;此后自 1ms 指数升至 16ms 封顶,
    叠加 [-50%, +50%) 抖动打散同拍。

    ★ Go 用 `select { case <-ctx.Done(): case <-timer.C: }` 让取消立即结束等待;
    Python 里 `asyncio.sleep` 本身就响应取消(抛 CancelledError 穿透出去),
    **不要**在这里 try/except 掉它 —— 吞掉取消会让优雅停机排空不了在途事务。
    """
    if attempt <= 0:
        return
    shift = min(attempt - 1, 4)
    base_ms = 1 << shift
    delay_ms = base_ms / 2 + random.random() * base_ms
    await asyncio.sleep(delay_ms / 1000.0)


# ── 水位比较 / 推进 ──────────────────────────────────────────────────────────


def _noop_advance(pipe) -> None:  # noqa: ANN001
    """未启用 fence / 无需推进时的占位推进闭包。"""


def fenced_watch_keys(keys: Sequence[str], pod: str, fence: WriterFence | None) -> list[str]:
    """启用 fence 时把 `wfence_key(pod)` 并入 WATCH 集(同 slot)。

    ★ 不并入的话,"读水位"与"推进水位"之间的并发继任会**绕过乐观锁**:
    继任者在两者之间写高水位,本事务照样 EXEC 成功 —— fence 比较通过了,
    但比较用的是已经过期的快照。
    """
    if fence is None:
        return list(keys)
    return [*keys, wfence_key(pod)]


def parse_fence_value(pod: str, raw: bytes | str | None) -> int:
    """解析 fence key 的十进制水位值。对应 Go 的 `strconv.ParseUint(raw, 10, 64)`。

    ★ 正则用 `\\A[0-9]+\\Z` 而不是 `^[0-9]+$`:Python 的 `$` 也匹配**末尾换行**,
    于是 `"5\\n"` 会被判成合法水位。Go 的 ParseUint 不接受任何空白。

    ★ 上界必须显式判:Python int 无限精度,一个 `"99999999999999999999999"`
    会被 `int()` 照单全收并恒大于任何真实 token —— 该 pod 的所有写从此永久被拒
    (整个分片静默不可写),而 Go 会直接报 corrupt。
    """
    if raw is None:
        return 0
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
    if not re.fullmatch(r"\A[0-9]+\Z", text):
        raise errcode.PandoraError(
            errcode.ErrInternal, "hub writer fence %s corrupt value %r", pod, text
        )
    value = int(text)
    if value > UINT64_MAX:
        raise errcode.PandoraError(
            errcode.ErrInternal, "hub writer fence %s value %r exceeds uint64", pod, text
        )
    return value


async def guard_writer_fence(
    pipe, pod: str, fence: WriterFence | None
) -> Callable[[object], None]:  # noqa: ANN001
    """在 WATCH 之后、MULTI 之前做 fence 比较,返回「推进闭包」。

    调用方必须已经 `await pipe.watch(*fenced_watch_keys(...))`(含 wfence key),
    此时 pipeline 处于 immediate 模式,可以直接读。返回的闭包由写事务在
    **同一个 EXEC** 内调用来推进水位。

    只读事务可只调本函数校验、不执行推进(检查恒保守安全)。

    三条判据(顺序即契约):
        未持有租约 → WriterSupersededError(连读到的水位都不必看:本副本没有写权)
        cur > mine → WriterSupersededError(继任者已触达此 slot,零写入)
        cur == mine → 放行且不推进(省一次 SET)
        cur < mine → 放行并把 mine 写进同一 EXEC
    """
    if fence is None:
        return _noop_advance
    held, mine = fence_snapshot(fence)
    if not held:
        raise WriterSupersededError()
    cur = parse_fence_value(pod, await pipe.get(wfence_key(pod)))
    if cur > mine:
        raise WriterSupersededError()
    if cur == mine:
        return _noop_advance

    key = wfence_key(pod)
    value = str(mine)

    def advance(tx_pipe) -> None:  # noqa: ANN001
        # 持久键:fencing 水位必须比业务记录长寿(见模块头)。
        tx_pipe.set(key, value)

    return advance


def require_writer_held(fence: WriterFence | None) -> None:
    """**入口级**写者校验。对应 Go 的 `requireWriterHeld`。

    用于键无法与任何 {pod} 水位同事务的写路径(无 hashtag 的 per-team 提示键、
    per-player 冷却键、以及跨 slot 的全局索引)。

    ★ 它严格**弱于** `guard_writer_fence`:只挡「本副本已知失主」,挡不住
    「检查通过后才失租」。因此只允许用在**丢失即自愈、且不参与准入 / 归属判定**
    的键上,每个调用点必须注释写明为什么做不成原子 fencing。
    归属、席位、容量账本一律不得降级到本函数。
    """
    if fence is None:
        return
    held, _ = fence_snapshot(fence)
    if not held:
        raise WriterSupersededError()


async def fenced_pod_tx(
    rdb,  # noqa: ANN001
    fence: WriterFence | None,
    pod: str,
    keys: Sequence[str],
    mutate: Callable[[object], None],
) -> None:
    """在 {pod} slot 上跑一个受写者水位保护的 WATCH/MULTI/EXEC。

    对应 Go 的 `RedisHubRepo.fencedPodTx`。水位比较、业务写与水位推进落在**同一个
    EXEC**,失主副本或落后 token 一律零写入 `WriterSupersededError`。

    ★ `keys` 必须全部与 pod 同 hashtag(shard / members / transfer_cleanup 均满足),
    否则 Redis Cluster 会 CROSSSLOT 拒绝整个事务。

    ★ 未启用 fence(dev / 单副本 Recreate)时**退化为无 WATCH 的 MULTI/EXEC**,
    行为逐字不变 —— 不引入新的乐观锁冲突重试。这条不是省事:给未启用 fencing 的
    部署凭空加一层 WATCH,会让原本必然成功的写在高并发下开始返回「预算耗尽」,
    而那条错误路径在 dev 从未被走过。

    `mutate(pipe)` 只允许**排队**命令(pipeline 已处于 buffered 模式,命令不是
    协程),不要在里面 await。
    """
    if fence is None:
        async with rdb.pipeline(transaction=True) as pipe:
            mutate(pipe)
            await pipe.execute()
        return

    watch = fenced_watch_keys(keys, pod, fence)
    for attempt in range(CAS_MAX_RETRY):
        try:
            async with rdb.pipeline(transaction=True) as pipe:
                await pipe.watch(*watch)
                advance = await guard_writer_fence(pipe, pod, fence)
                pipe.multi()
                mutate(pipe)
                advance(pipe)
                await pipe.execute()
        except WatchError:
            await cas_conflict_backoff(attempt)
            continue
        return
    raise errcode.PandoraError(
        errcode.ErrUnavailable, "hub fenced pod tx contention on pod %s", pod
    )
