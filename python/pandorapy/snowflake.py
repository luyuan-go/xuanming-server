"""Snowflake 发号器 —— 与 Go 侧 pkg/snowflake 的位布局**完全一致**。

为什么位布局必须逐位一致:
    dialogue_id / trade_id / mail 游标等等都是 snowflake,它们已经落库、已经在客户端
    手里、已经在日志里。Python 版和 Go 版在迁移期会同时发号(strangler 双栈),
    如果位布局不同:
      - 同一个逻辑秒里两边可能发出**相同**的 ID(node 段错位)
      - 或者 ID 大小顺序错乱(时间段位宽不同),破坏"ID 单调递增"这个被依赖的性质
    这是静默数据损坏,不是崩溃。

Go 侧口径(pkg/snowflake/snowflake.go:22):
    Epoch    = 1781161165        # 2026-06-11 06:59:25 UTC,**秒**级,不是毫秒
    NodeBits = 17                # node 段,最多 131072 个节点
    StepBits = 15                # 每个逻辑秒每节点 32768 个序号
    timeShift = 17 + 15 = 32
    nodeShift = 15

    ID = (unix_sec - Epoch) << 32 | (node_id << 15) | step

⚠️ 时间粒度是**秒**而不是毫秒 —— 这点和大多数 snowflake 实现不同,照抄网上的实现
   一定会错。每节点每秒上限 32768 个 ID,超了就阻塞到下一秒(与 Go 一致,不是丢号)。

node_id 唯一性:
    本模块只管**用**给定的 node_id 发号,不管这个号从哪来。号的来源由
    `pandorapy/snowflake_etcd.provide_node` 按 `snowflake.node_id_source` 二选一:

        ""/"static"  读 yaml 的 node.node_id(单副本 / dev 默认,不碰 etcd)
        "etcd"       etcd 自动抢占 + **失租即退出进程**

    ⚠️ 多副本部署必须走 etcd 档:两个副本用同一个 node_id 会重号(§9 不变量 11),
    而重号在背包域表现为 DuplicateGuid fail-closed 卡住玩家领取 —— 现场看到的是
    "玩家领不了奖",查不到这里。
"""

from __future__ import annotations

import threading
import time

# ── 位布局(与 Go 逐个对齐,不要改)────────────────────────────────────────────
EPOCH: int = 1781161165
NODE_BITS: int = 17
STEP_BITS: int = 15

_TIME_SHIFT = NODE_BITS + STEP_BITS  # 32
_NODE_SHIFT = STEP_BITS  # 15
_STEP_MASK = (1 << STEP_BITS) - 1  # 32767
NODE_MASK: int = (1 << NODE_BITS) - 1  # 131071


class ClockBeforeEpochError(RuntimeError):
    """系统时钟早于 Epoch。

    Go 侧在这里直接 panic(snowflake.go:202),理由是:uint64 减法会下溢出成垃圾时间位,
    发出的 ID 会污染全局有序性且无法回收。Python 侧同样必须硬失败而不是返回一个坏 ID。
    """


class Node:
    """一个发号节点。线程安全(对应 Go 侧的 sync.Mutex)。

    注意:这里用线程锁而不是 asyncio.Lock —— 发号是纯 CPU 操作、不 await,
    用线程锁能同时保护 asyncio 与线程池两条调用路径,且不需要把 Generate 变成协程
    (变协程会让每个调用点都得 await,污染整条业务链路)。
    """

    __slots__ = ("_node_shifted", "_last_sec", "_step", "_lock", "node_id")

    def __init__(self, node_id: int) -> None:
        if not 0 <= node_id <= NODE_MASK:
            raise ValueError(
                f"snowflake node_id={node_id} 超出范围 [0, {NODE_MASK}]"
                f"(NodeBits={NODE_BITS});多副本请走 etcd 自动分配"
            )
        self.node_id = node_id
        self._node_shifted = node_id << _NODE_SHIFT
        self._last_sec = -1
        self._step = 0
        self._lock = threading.Lock()

    def generate(self) -> int:
        """铸一个 ID。步池耗尽时阻塞到下一逻辑秒(与 Go 一致:不丢号、不重号)。"""
        with self._lock:
            now = _now_epoch()
            if now == self._last_sec:
                self._step = (self._step + 1) & _STEP_MASK
                if self._step == 0:
                    # 本逻辑秒的 32768 个序号用光了 —— 等到时钟走过这一秒。
                    # Go 侧在这里会打一条 error 日志(超过 2s 视为时钟异常),
                    # Python 侧交由调用方观测 QPS,不在锁内做 I/O。
                    now = _wait_next_second(self._last_sec)
                    self._last_sec = now
            else:
                if now < self._last_sec:
                    # 时钟回拨:继续用上一个逻辑秒,序号往下走。绝不发小于已发出的 ID。
                    self._step = (self._step + 1) & _STEP_MASK
                    if self._step == 0:
                        now = _wait_next_second(self._last_sec)
                        self._last_sec = now
                    return (self._last_sec << _TIME_SHIFT) | self._node_shifted | self._step
                self._last_sec = now
                self._step = 0
            return (self._last_sec << _TIME_SHIFT) | self._node_shifted | self._step

    def generate_into(self, dst: list[int]) -> None:
        """批量铸号,原地填满 dst。对应 Go 的 Node.GenerateInto。

        语义与逐个 generate() 完全一致:严格递增、互不重复、可与 generate() 混用;
        **只保证递增+唯一,不保证连续**(跨秒会有空洞)——调用方按下标取用即可,
        任何"相邻 ID 差 1"的假设都是错的。

        ★ dst 由调用方分配,和 Go 一样不按外部输入决定分配大小:
        件数是可被上游数据放大的量(一封邮件的附件 count 是 uint32),
        在这里按传入数量自行 malloc 就等于把它变成内存 DoS 面。
        数量上限必须由业务侧(mail 的 max_instances_per_mail)在分配**之前**卡死。

        实现刻意是"循环调用 generate()"而不是 Go 那种一次 CAS 预留整段:
        Python 侧 Node 本就是锁实现(见类注释),预留段只会多一份自己的
        step/秒边界处理代码,而那正是最容易与 generate() 漂移的地方 ——
        两条铸号路径口径不一致 = 重号,且只在跨秒边界偶发。
        """
        for i in range(len(dst)):
            dst[i] = self.generate()


def _now_epoch() -> int:
    """当前时间距 Epoch 的**秒**数。时钟早于 Epoch 直接硬失败。"""
    ts = int(time.time())
    if ts < EPOCH:
        raise ClockBeforeEpochError(
            f"系统时钟 {ts} 早于 snowflake epoch {EPOCH} "
            f"({time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(EPOCH))})"
        )
    return ts - EPOCH


def _wait_next_second(last_sec: int) -> int:
    """自旋到时钟越过 last_sec。"""
    while True:
        now = _now_epoch()
        if now > last_sec:
            return now
        # 秒级粒度,睡到本秒结束就够了;不做忙等,避免烧 CPU。
        time.sleep(0.005)


def timestamp_of(snowflake_id: int) -> int:
    """从 ID 反解 unix 秒。排障时把一个 ID 还原成"什么时候发的"。"""
    return (snowflake_id >> _TIME_SHIFT) + EPOCH


def node_of(snowflake_id: int) -> int:
    """从 ID 反解 node_id。查"重号是哪两个副本"时用。"""
    return (snowflake_id >> _NODE_SHIFT) & NODE_MASK


def step_of(snowflake_id: int) -> int:
    """从 ID 反解逻辑秒内序号。"""
    return snowflake_id & _STEP_MASK


def min_id_at(unix_sec: int) -> int:
    """unix_sec 时刻可能生成的**最小** ID(时间段取该秒,node/step 全零)。

    对应 Go 的 snowflake.MinIDAt。用途是把"创建时间早于 cutoff"翻译成
    "mail_id < min_id_at(cutoff)" —— 时间条件变成主键范围条件,清理走索引扫描
    而不是全表 + 反解时间。

    ★ 不设这个换算的话,player_mail_claim 这类"按创建时间保留 N 天"的清理只能
    要么全表扫、要么另存一列创建时间(多一份会漂移的真相)。

    unix_sec 早于 Epoch 时返回 0(该时刻之前不存在任何 ID),与 Go 一致 ——
    返回负数会让 `mail_id < x` 这种无符号比较翻车。
    """
    if unix_sec < EPOCH:
        return 0
    return (int(unix_sec) - EPOCH) << _TIME_SHIFT
