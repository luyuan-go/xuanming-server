"""etcd 选主 —— 对应 Go 侧 pkg/leader/etcdleader(concurrency.Election)。

背景(decision-revisit-matchmaker-single-writer.md):贪心批量撮合是"在共享池上做全局优化",
天然是单写者问题。多副本时若每个副本都无条件跑撮合循环,会在同一队列上重复成局
(同一玩家进两场 match,违反不变量 §1)。本模块选出唯一 leader,仅当选副本跑循环。

与 snowflake 的失主语义区别(照抄 Go 侧,这个区别很重要):
    snowflake 失租必须**退出进程**(防同 nodeID 双活发号);
    本模块失去领导权**不退出进程** —— 只取消 leader 任务,本副本继续服务 RPC,
    新 leader 在 lease TTL 内接管。这正是不停机滚动更新(不变量 §16)。

⚠️ aetcd 没有 Go clientv3.KeepAlive 的自动续约(2026-08-18 实测确认):
    Go:  clientv3.Grant + KeepAlive → 后台 goroutine 自动按 TTL/3 续,失败会通知
    Py:  只有 lease.refresh(),**续约节奏与失败处理全部由本模块负责**
    这是整个迁移里最容易写出脑裂的一处。本模块的续约循环遵守 §9.22:
      - 只把**当前 lease 的成功响应**视为续约成功(已发送、超时、UNKNOWN 都不算)
      - 用 monotonic 时钟维护一个**比 lease 到期更早**的本地安全截止时间
      - 到达安全截止线立即放弃领导权并取消 leader 任务,不等"可能永不到来"的响应
    etcd 语义本身已由 tools/verify_etcd.py 实测(11/11 PASS),本模块只负责用对。
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable

import aetcd

from pandorapy import log as plog
from pandorapy.etcdlease import LeaseGoneError, refresh_or_raise

# 与 Go 侧 pkg/leader/etcdleader 的常量逐个对齐。
DEFAULT_PREFIX = "/pandora/leader/"
DEFAULT_LEASE_TTL_SEC = 15
DEFAULT_DIAL_TIMEOUT_SEC = 5.0
# 重连 / 重竞选前的退避,避免 etcd 抖动时忙等。
RECONNECT_BACKOFF_SEC = 2.0

# 续约节奏 = TTL / 3,与 Go clientv3 内部一致。
_KEEPALIVE_DIVISOR = 3
# 本地安全余量:比 lease 到期提前这么多就主动放弃领导权。
#
# 为什么需要余量而不是"到期才放弃":lease 的到期判定在 etcd 服务端,本地时钟、网络
# 往返、GIL 抖动都会让"我以为还有 1s"实际已经过期。§9.22 要求
# `旧 owner 最晚停止时间 < 新 owner 最早开始时间`,余量就是这个不等式的兑现。
# 取 TTL/3:与续约间隔同量级 —— 只要连续丢一次续约就进入放弃流程。
_SAFETY_MARGIN_DIVISOR = 3


class LeaderElection:
    """单个选举名上的领导权竞选器。

    用法(对应 Go 的 etcdleader.Run):

        election = LeaderElection(
            endpoints=["127.0.0.1:2379"],
            election="matchmaker/5v5_ranked/r1",   # 分片键:mode/region
        )
        await election.run(usecase.run_match_loop)   # 阻塞;run 必须遵守 CancelledError
    """

    __slots__ = (
        "_endpoints",
        "_key_prefix",
        "_my_key",
        "_ttl",
        "_dial_timeout",
        "_election_name",
        "is_leader",
        "queued",
        "lost_leadership_count",
        "term_count",
    )

    def __init__(
        self,
        endpoints: list[str],
        election: str,
        *,
        prefix: str = DEFAULT_PREFIX,
        lease_ttl_sec: int = DEFAULT_LEASE_TTL_SEC,
        dial_timeout_sec: float = DEFAULT_DIAL_TIMEOUT_SEC,
    ) -> None:
        if not endpoints:
            raise ValueError("etcdleader: endpoints 不能为空")
        if not election:
            raise ValueError("etcdleader: election 名不能为空")
        self._endpoints = endpoints
        self._election_name = election
        # etcd Election 的 keyPrefix = <electionKey> + "/",候选者 key 挂在它下面。
        # 与 Go 的 concurrency.NewElection(session, prefix+election) 逐字一致。
        self._key_prefix = f"{prefix or DEFAULT_PREFIX}{election}/"
        self._my_key: bytes | None = None
        self._ttl = lease_ttl_sec if lease_ttl_sec > 0 else DEFAULT_LEASE_TTL_SEC
        self._dial_timeout = dial_timeout_sec if dial_timeout_sec > 0 else DEFAULT_DIAL_TIMEOUT_SEC
        self.is_leader = False
        # queued:本副本的候选 key 已在选举前缀下(排队中),但未必是队首。
        # 「已入队未当选」与「根本没入队」是两种不同的运维状态:前者是正常热备,
        # 后者说明 etcd 写不进去 —— 只看 is_leader 分不出来,两者都是 False。
        self.queued = False
        # 可观测计数:失主次数异常增长 = etcd 抖动或 TTL 配得太紧。
        self.lost_leadership_count = 0
        self.term_count = 0

    def _client(self) -> aetcd.Client:
        host, _, port = self._endpoints[0].rpartition(":")
        return aetcd.Client(
            host=host or "127.0.0.1",
            port=int(port or 2379),
            timeout=self._dial_timeout,
        )

    async def run(self, leader_task: Callable[[], Awaitable[None]]) -> None:
        """竞选并在持有领导权期间运行 leader_task()。阻塞直到被取消。

        leader_task 必须遵守 asyncio 取消(收到 CancelledError 时尽快返回)——
        对应 Go 侧 "run 必须遵守 leaderCtx.Done()"。
        """
        logger = plog.get()
        try:
            while True:
                try:
                    await self._one_term(leader_task, logger)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001
                    # etcd 不可达 / 竞选失败:退避重试,**不退出进程**。
                    logger.exception(
                        "leader_campaign_failed", election=self._election_name
                    )
                await asyncio.sleep(RECONNECT_BACKOFF_SEC)
        except asyncio.CancelledError:
            # 正常下线路径:让位后返回(对应 Go 的 Resign)。
            logger.info("leader_shutdown", election=self._election_name)
            raise

    async def _one_term(
        self, leader_task: Callable[[], Awaitable[None]], logger: object
    ) -> None:
        """一个任期:入队 → 排到队首即当选 → 跑任务 → 失主/让位 → 返回。

        ★ 队列语义(与 Go 的 concurrency.Election.Campaign 一致):
            候选者建好自己的 key 之后**一直持有它**(靠续约保活),排在队列里等前面的人退出。
            这样 CreateRevision 就是稳定的排队序号,先到先当选,不会有副本饿死。

            最初写成"没排到队首就 revoke 掉自己的 key、退避后重新建" —— 那样每轮都会
            拿到一个更大的 CreateRevision,等于**每次都重新排到队尾**。互斥仍然成立
            (只有队首当选),但公平性没有:一个副本可能永远排不上。
        """
        async with self._client() as client:
            lease = await client.lease(self._ttl)
            keepalive: asyncio.Task | None = None
            try:
                my_key = await self._enqueue(client, lease)
                if my_key is None:
                    return  # 入队失败(极罕见:lease id 撞了),退避重来

                # ★ 续约必须**从入队就开始**,而不是当选之后 ——
                # 排队期间 lease 到期会让自己的 key 消失,等于悄悄掉出队列,
                # 而本副本还以为在排队,于是永远等不到。
                lost = asyncio.Event()
                keepalive = asyncio.create_task(
                    self._keepalive_loop(lease, lost),
                    name=f"leader-keepalive:{self._election_name}",
                )

                if not await self._wait_for_turn(client, my_key, lost):
                    return  # 排队期间失租 / 被取消

                self.is_leader = True
                self.term_count += 1
                logger.info(  # type: ignore[attr-defined]
                    "leader_elected",
                    election=self._election_name,
                    lease_id=lease.id,
                    ttl_sec=self._ttl,
                    term=self.term_count,
                )

                task = asyncio.create_task(leader_task(), name=f"leader:{self._election_name}")
                try:
                    await self._lead_until_lost(client, my_key, task, lost)
                finally:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await task
            finally:
                if keepalive is not None:
                    keepalive.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await keepalive
                self.queued = False
                if self.is_leader:
                    self.is_leader = False
                    self.lost_leadership_count += 1
                    logger.warning(  # type: ignore[attr-defined]
                        "leader_lost",
                        election=self._election_name,
                        lost_count=self.lost_leadership_count,
                    )
                # 主动 revoke = 让位,后继无需等 TTL。失败无所谓:lease 到期会自动释放。
                with contextlib.suppress(Exception):
                    await client.revoke_lease(lease.id)

    async def _enqueue(self, client: aetcd.Client, lease: aetcd.Lease) -> bytes | None:
        """建自己的候选者 key 入队。返回 key;失败返回 None。"""
        my_key = f"{self._key_prefix}{lease.id:x}".encode()
        succeeded, _ = await client.transaction(
            compare=[client.transactions.version(my_key) == 0],
            success=[client.transactions.put(my_key, b"leader", lease=lease.id)],
            failure=[],
        )
        if not succeeded:
            return None
        self._my_key = my_key
        # 入队成功。此后本副本一直持有这个 key(靠续约保活)直到当选或退出,
        # 所以 queued 在整个排队期间为真 —— 它与 is_leader 一起构成三态:
        #   (queued=False, is_leader=False) 没入队(etcd 写不进去 = 要告警)
        #   (queued=True,  is_leader=False) 排队中(正常热备)
        #   (queued=True,  is_leader=True ) 当选
        self.queued = True
        return my_key

    async def _wait_for_turn(
        self, client: aetcd.Client, my_key: bytes, lost: asyncio.Event
    ) -> bool:
        """排队等到自己是队首(CreateRevision 最小)。失租 / 被取消返回 False。

        用轮询而不是 watch 前驱 key:
            Go 的 waitDeletes 会 watch 恰好排在自己前面那个 key 的删除事件,接管更快。
            这里用 TTL/3 的轮询 —— 接管延迟上界是一个轮询周期(默认 5s),对撮合循环
            这种"慢一点没关系、绝不能两个同时跑"的场景完全够用,而且省掉一条长连接
            和一套 watch 重连逻辑(§15.2 最少复杂度)。
            **互斥性与轮询无关**,由 CreateRevision 规则保证;轮询只影响接管快慢。
        """
        poll = max(0.2, self._ttl / _KEEPALIVE_DIVISOR)
        while True:
            if lost.is_set():
                return False
            present, is_first = await self._candidate_status(client, my_key)
            if not present:
                # ★ 自己的候选者 key 已经不在前缀下 = 悄悄掉出了队列。
                #
                # 必须**结束本轮任期**让 run() 退避后重新 _enqueue,而不是继续等。
                # 早先这里只判"是不是队首":key 没了永远不是队首,于是本协程每
                # poll 秒查一次、**永不返回** —— _one_term 不返回、run() 的重试
                # 循环走不到、_enqueue 再也不会被调用,而 leader_lost 日志只在
                # finally 里 is_leader=True 时才打,全程**零日志**。
                # 表现是:这个副本从此再不参与选举,而它自己以为还在排队。
                plog.get().warning(
                    "leader_requeue",
                    election=self._election_name,
                    hint="候选者 key 已不在前缀下(失租 / 被手工删),结束本轮重新入队",
                )
                return False
            if is_first:
                return True
            try:
                await asyncio.wait_for(lost.wait(), timeout=poll)
                return False  # 等待期间失租
            except TimeoutError:
                continue  # 正常轮询周期到,再看一次

    async def _lead_until_lost(
        self,
        client: aetcd.Client,
        my_key: bytes,
        task: asyncio.Task,
        lost: asyncio.Event,
    ) -> None:
        """持有领导权期间守着三件事:续约是否还活着、leader 任务是否结束、自己是否仍是队首。

        第三件是**防御性复查**:正常情况下队首身份只会因 lease 消失而丢,而那已经由
        续约循环覆盖。但如果有人手工删了 key、或 etcd 做过异常恢复,复查能让本副本
        及时停手,而不是抱着一个"自以为是 leader"的状态继续跑撮合循环。
        """
        poll = max(0.2, self._ttl / _KEEPALIVE_DIVISOR)
        # ★ lost 的等待 Task **建一次、复用**,并用 asyncio.wait 自带的 timeout。
        #
        # 原先写的是 `wait_for(wait({task, create_task(lost.wait())}), timeout=poll)`:
        # 每轮新建一个 lost.wait() Task,而 wait_for 超时取消的是外层 asyncio.wait 协程 ——
        # 被取消的 asyncio.wait **不会**取消它正在等的 awaitable。于是每轮泄漏一个
        # 常驻 pending Task 挂在同一个 Event 的 _waiters 上:poll 默认 5s,
        # 一个稳定当选 7 天的 leader 会攒下约 12 万个,内存持续增长,
        # 且 lost.set() 时要一次性唤醒这十几万个 future。
        lost_task = asyncio.create_task(lost.wait(), name=f"leader-lost:{self._election_name}")
        try:
            while True:
                if task.done() or lost.is_set():
                    return
                await asyncio.wait(
                    {task, lost_task},
                    timeout=poll,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if task.done() or lost.is_set():
                    return
                # ★ 这是**防御性**复查,不是权威判定 —— 权威是 lease(续约循环在管)。
                #
                # 所以一次读失败**不能**当成"我不是 leader 了":那会让一次 etcd 瞬时
                # 抖动就终结任期、取消撮合循环、revoke 让位,而实际上租约好好的。
                # 真正的失主由 _keepalive_loop 置 lost,那条路径才有安全窗与 TTL 判定。
                try:
                    still_leader = await self._is_leader(client, my_key)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    plog.get().warning(
                        "leader_recheck_failed",
                        election=self._election_name,
                        err=str(exc),
                        hint="防御性复查读失败,不据此让位(权威是 lease,由续约循环判定)",
                    )
                    continue
                if not still_leader:
                    plog.get().warning(
                        "leader_key_vanished",
                        election=self._election_name,
                        hint="自己的候选者 key 不再是队首(被手工删除?)—— 主动停手防双 leader",
                    )
                    return
        finally:
            lost_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await lost_task

    async def _candidate_status(
        self, client: aetcd.Client, my_key: bytes
    ) -> tuple[bool, bool]:
        """一次查询回答两个问题:`(自己的 key 还在吗, 自己是不是队首)`。

        ★ 这两件事必须分开。合成一个布尔的话,"排队中"和"我的 key 已经没了"
        长得完全一样 —— 而处置**相反**:前者继续等,后者必须立刻重新入队。
        混在一起的后果是排队副本永久挂死(见 _wait_for_turn)。

        与 Go 侧同一条规则(CreateRevision 最小者为 leader),所以跨语言互斥成立。
        """
        entries = await client.get_prefix(self._key_prefix.encode())
        best_rev: int | None = None
        best_key: bytes | None = None
        mine_rev: int | None = None
        for kv in entries:
            rev = kv.create_revision
            if kv.key == my_key:
                mine_rev = rev
            if best_rev is None or rev < best_rev:
                best_rev, best_key = rev, kv.key
        if mine_rev is None:
            return False, False  # 自己的 key 不在了(lease 已过期 / 被手工删)
        return True, best_key == my_key

    async def _is_leader(self, client: aetcd.Client, my_key: bytes) -> bool:
        """自己是不是当前 leader。查不到自己的 key 一律 False —— fail-closed。"""
        present, is_first = await self._candidate_status(client, my_key)
        return present and is_first

    async def _keepalive_loop(self, lease: aetcd.Lease, lost: asyncio.Event) -> None:
        """续约循环 —— **从入队就开始跑**,一直跑到失租或被取消。

        为什么不是"当选后才续约":排队期间 lease 到期会让自己的候选者 key 消失,
        等于悄悄掉出队列,而本副本还在傻等 —— 永远等不到。

        aetcd 没有 Go clientv3.KeepAlive 的自动续约,节奏与失败处理全在这里。
        遵守 §9.22:
          - 只把**当前 lease 的成功响应**视为续约成功(已发送、超时、UNKNOWN 都不算)
          - 用 monotonic 时钟维护一个**比 lease 到期更早**的本地安全截止线
          - 越过安全线立即置位 lost,不等"可能永不到来"的响应
        """
        interval = self._ttl / _KEEPALIVE_DIVISOR
        margin = self._ttl / _SAFETY_MARGIN_DIVISOR
        safe_deadline = time.monotonic() + self._ttl - margin

        try:
            while True:
                sleep_for = min(interval, max(0.05, safe_deadline - time.monotonic()))
                await asyncio.sleep(sleep_for)

                now = time.monotonic()
                try:
                    await refresh_or_raise(lease, timeout=interval)
                except asyncio.CancelledError:
                    raise
                except LeaseGoneError as exc:
                    # ★ 服务端明确回复 lease 不存在 —— 自己的 key 已经消失,
                    # 当选态是已失去领导权,排队态是已掉出队列。两种都必须**立即**
                    # 置位 lost:再等安全窗只会让第二个 leader 多跑一会儿。
                    plog.get().warning(
                        "leader_lease_gone",
                        election=self._election_name,
                        err=str(exc),
                        hint="etcd 回复 lease 不存在,立即放弃领导权/队列位置(防脑裂)",
                    )
                    lost.set()
                    return
                except Exception:  # noqa: BLE001
                    # 续约失败:**不立即放弃**,还有安全窗口就继续试(etcd 短抖动很常见)。
                    # 越过安全截止线就必须放弃 —— 此时无法证明 lease 仍有效。
                    if time.monotonic() >= safe_deadline:
                        plog.get().warning(
                            "leader_lease_keepalive_deadline_exceeded",
                            election=self._election_name,
                            hint="连续续约失败越过本地安全截止线,放弃领导权/队列位置(防脑裂)",
                        )
                        lost.set()
                        return
                    continue

                # ★ 只有**成功响应**才推进安全截止线,且从**发起时刻**算起(保守方向) ——
                # 服务端可能在请求刚到达时就重置了 TTL,按较早的时刻算不会高估剩余时间。
                safe_deadline = now + self._ttl - margin
        except asyncio.CancelledError:
            raise


async def run_leader(
    endpoints: list[str],
    election: str,
    leader_task: Callable[[], Awaitable[None]],
    **kwargs: object,
) -> None:
    """便捷入口 —— 对应 Go 的 etcdleader.Run(ctx, cfg, run)。"""
    el = LeaderElection(endpoints, election, **kwargs)  # type: ignore[arg-type]
    await el.run(leader_task)
