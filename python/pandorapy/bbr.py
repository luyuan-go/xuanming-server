"""BBR 自适应限流 —— 对应 Go 侧 `pkg/middleware/ratelimit.go`。

Go 那边是三行包装:

    func RateLimit() middleware.Middleware { return ratelimit.Server() }

`ratelimit.Server()` 底下是 `github.com/go-kratos/aegis` 的 BBR。本文件是它的 Python 对应物,
算法逐步对齐 `aegis/ratelimit/bbr/bbr.go`。

为什么是自己写而不是装一个库(2026-08-20 查证)
---------------------------------------------------------------------------
**没有可拿来用的**。当天实查:

    GitHub 仓库搜 "python adaptive concurrency limit load shedding"  → 0 个仓库
    GitHub 仓库搜 "BBR rate limit python"                            → 0 个仓库
    go-kratos/aegis        239 star,Go 98.9%,最后 release 三年前
    Netflix/concurrency-limits  3.6k star,**Java 100%**(Vegas/Gradient2/AIMD)

Python 侧 star 最多的那几个是**另一类东西**,不是这个东西的替代品:

    slowapi      2.0k star  ← 只是 limits 的 FastAPI 包装
    aiolimiter   775 star   ← 漏桶,AsyncLimiter(100, 30)
    limits       642 star   ← 固定窗 / 滑动窗 / 滑窗计数
    PyrateLimiter 515 star  ← 漏桶家族

它们全是**配额限流**:你告诉它"每分钟 5 次",它按 key 计数。BBR 是**过载丢负载**:
不配任何阈值,按 CPU / inflight / RT 自己判断机器扛不住了才丢,不区分调用方。
拿配额限流器冒充过载保护,恰好复现 `server.py` 那道 fail-fast 闸要防的事故 ——
yaml 写着有保护、进程里确实有个限流器、但它守的不是这件事。所以只能移植。

CPU 信号为什么必须换掉(唯一一处**有意**偏离 aegis)
---------------------------------------------------------------------------
aegis 的 `gCPU` 读的是 **cgroup 整容器 CPU 使用率**(千分比)。直接照搬到 Python 会得到
一个**永远不触发**的限流器:

    本仓 Python 服务 = 单进程 asyncio,业务全在**一个事件循环线程**上(GIL)。
    容器给 4 核时,事件循环线程吃满 = 整容器 25% = 250‰,永远够不到阈值 800‰。
    → shouldDrop 的 CPU 分支永不成立 → 一条请求都不会丢。

那正是 fail-fast 闸要防的失败形态(声称有保护、实际没有),而且比不实现更隐蔽 ——
指标、日志、配置三样都显示"BBR 已启用"。

所以信号换成**事件循环线程自己的 CPU 饱和度**:`time.thread_time()` 的增量 / 墙钟增量,
同样归一到 0..1000 千分比,阈值仍是 800。语义是对齐的 —— Go 那边 800‰ 表示
"这个进程的可用 CPU 快吃满了",这里 800‰ 表示"事件循环这一核快吃满了",
两者都是**该进程的硬资源上限**。

三个细节:

    · 采样必须发生在事件循环线程上(`should_drop()` 由 handler 调,天然满足)。
      `thread_time()` 是**每线程**的,在别的线程上采会量到错误的对象。
    · 循环被打满时采样任务本身也会被推迟 —— 墙钟增量随之变大,而 CPU 增量同步变大,
      比值仍然收敛到 1000‰。饱和反而让这个测量更准,不会失真。
    · 等 I/O(await 数据库 / Redis)时 CPU 增量不涨、墙钟涨 → 饱和度低 → 不丢。
      这是对的:那种情况不是本进程扛不住,并发量该由 maxInFlight(RT 那一路)去管。

并发前提
---------------------------------------------------------------------------
单事件循环线程,`should_drop` / `begin` / `end` 之间**没有 await**,所以
`self._in_flight += 1` 这类读改写是原子的,不需要锁。若将来放到多线程执行器上跑,
这条前提就破了,必须重新审。
"""

from __future__ import annotations

import dataclasses
import math
import time
from collections.abc import Callable, Iterator

# ── 默认参数:与 aegis NewLimiter 的默认值逐项一致 ────────────────────────────
# (aegis/ratelimit/bbr/bbr.go:NewLimiter → options{Window: 10s, Bucket: 100, CPUThreshold: 800})
DEFAULT_WINDOW_SEC = 10.0
DEFAULT_BUCKET_COUNT = 100
DEFAULT_CPU_THRESHOLD = 800

# EMA 衰减系数与采样周期,对齐 aegis 的 `decay = 0.95` + 500ms ticker:
#     cpu = cpu(t-1) * decay + cpu(t) * (1 - decay)
# 时间常数约 10s —— 刻意迟钝,避免被单个慢请求带出来的 CPU 尖峰触发丢负载。
_CPU_DECAY = 0.95
_CPU_SAMPLE_INTERVAL_SEC = 0.5
# 千分比上限,对应 aegis 的 `stat.Usage = min(stat.Usage, 1000)`。
_CPU_MAX_PERMILLE = 1000

# 「刚开始丢」的记忆窗口,对应 aegis shouldDrop 里的 `<= time.Second`。
# CPU 回落到阈值以下后仍按 inflight 兜一秒,防止 CPU 一抖就立刻全量放行、
# 把刚缓过来的进程再打死(震荡)。
_PREV_DROP_MEMORY_SEC = 1.0


@dataclasses.dataclass(frozen=True, slots=True)
class Stat:
    """BBR 快照 —— 对应 aegis 的 `bbr.Stat`。只用于测试与排查,不参与判定。"""

    cpu: int
    in_flight: int
    max_in_flight: int
    min_rt: int
    max_pass: int


class _RollingCounter:
    """滑动窗计数器 —— 对应 aegis 内部的 `window.RollingCounter`。

    环形桶:`size` 个桶,每桶 `bucket_sec` 秒,每桶存一串点。
    读的时候**跳过当前桶** —— 当前桶还没攒满,把它算进去会让 maxPASS 系统性偏低、
    maxInFlight 跟着偏低,变成"越忙越容易误丢"。aegis 的注释写的就是这件事
    ("Value of current bucket is not counted in real time")。
    """

    __slots__ = ("_buckets", "_size", "_bucket_sec", "_offset", "_bucket_start")

    def __init__(self, size: int, bucket_sec: float, now: float) -> None:
        self._size = size
        self._bucket_sec = bucket_sec
        self._buckets: list[list[float]] = [[] for _ in range(size)]
        self._offset = 0
        self._bucket_start = now

    def _advance(self, now: float) -> None:
        """把时间推进到 now,途经的桶清空。"""
        elapsed = now - self._bucket_start
        if elapsed < self._bucket_sec:
            return
        steps = int(elapsed // self._bucket_sec)
        if steps >= self._size:
            # 整窗都过期了(长时间没流量)。逐个清比绕一圈快,也避免 steps 极大时空转。
            for bucket in self._buckets:
                bucket.clear()
            self._offset = 0
            self._bucket_start = now
            return
        for _ in range(steps):
            self._offset = (self._offset + 1) % self._size
            self._buckets[self._offset].clear()
        # ★ 用累加而不是 `= now`:后者会把桶边界钉死在最后一次调用的时刻上,
        # 让每个桶实际时长在 [bucket_sec, 2*bucket_sec) 漂移,窗口总时长跟着失真。
        self._bucket_start += steps * self._bucket_sec

    def add(self, value: float, now: float) -> None:
        self._advance(now)
        self._buckets[self._offset].append(value)

    def past_buckets(self, now: float) -> Iterator[list[float]]:
        """除当前桶外的各桶,从最旧到最新。共 size-1 个。

        对应 aegis `Reduce` 里的 `for i := 1; iterator.Next() && i < opts.Bucket; i++`。
        """
        self._advance(now)
        for i in range(1, self._size):
            yield self._buckets[(self._offset + i) % self._size]


class _EventLoopThreadCpu:
    """事件循环线程的 CPU 饱和度,千分比 0..1000。

    见模块头「CPU 信号为什么必须换掉」。`thread_time()` 只算**当前线程**的 CPU 时间,
    不含 grpc C core 的内部线程 —— 这是要的:我们保护的是 Python 事件循环这一核。
    """

    __slots__ = ("_last_wall", "_last_cpu")

    def __init__(self, now: float) -> None:
        self._last_wall = now
        self._last_cpu = time.thread_time()

    def __call__(self, now: float) -> int:
        cpu_now = time.thread_time()
        wall_delta = now - self._last_wall
        cpu_delta = cpu_now - self._last_cpu
        self._last_wall = now
        self._last_cpu = cpu_now
        if wall_delta <= 0:
            return 0
        return int(cpu_delta / wall_delta * _CPU_MAX_PERMILLE)


class BBR:
    """BBR 限流器 —— 对应 aegis 的 `bbr.BBR`。

    用法(与 aegis 的 `Allow() (DoneFunc, error)` 同形,只是拆成两个方法):

        if limiter.should_drop():
            raise PandoraError(errcode.ErrRateLimited, "...")
        start = limiter.begin()
        try:
            ...
        finally:
            limiter.end(start)

    `clock` / `cpu_source` 可注入是为了能被断言 —— 真实 CPU 与真实时间都不可控,
    不注入就只能写"构造成功"这种什么都证明不了的用例。
    """

    __slots__ = (
        "_bucket_sec",
        "_bucket_per_second",
        "_bucket_count",
        "_cpu_threshold",
        "_cpu_source",
        "_clock",
        "_cpu",
        "_last_cpu_sample",
        "_pass_stat",
        "_rt_stat",
        "_in_flight",
        "_prev_drop_at",
        "_max_pass_cache",
        "_min_rt_cache",
    )

    def __init__(
        self,
        *,
        window_sec: float = DEFAULT_WINDOW_SEC,
        bucket_count: int = DEFAULT_BUCKET_COUNT,
        cpu_threshold: int = DEFAULT_CPU_THRESHOLD,
        cpu_source: Callable[[float], int] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if window_sec <= 0:
            raise ValueError("bbr: window_sec 必须为正")
        if bucket_count <= 1:
            # 1 个桶时 past_buckets 恒为空 → maxPASS 恒为 1、minRT 恒为 1,
            # maxInFlight 退化成常数,限流器不再自适应。直接拒配,不静默劣化。
            raise ValueError("bbr: bucket_count 必须 > 1")
        self._clock = clock or time.monotonic
        now = self._clock()

        # ★ 用纳秒整除复刻 Go 的 duration 整数除法:
        #     bucketDuration := opt.Window / time.Duration(opt.Bucket)
        #     bucketPerSecond := int64(time.Second / bucketDuration)
        # 用浮点算 1.0/bucket_sec 会在 window/bucket 除不尽时差一(如 10s/3 桶),
        # 而 bucketPerSecond 是 maxInFlight 的乘数,差一就是限流阈值差一档。
        window_ns = int(window_sec * 1_000_000_000)
        bucket_ns = window_ns // bucket_count
        if bucket_ns <= 0:
            raise ValueError("bbr: window_sec 太小,单桶不足 1ns")
        self._bucket_sec = bucket_ns / 1_000_000_000
        self._bucket_per_second = 1_000_000_000 // bucket_ns
        self._bucket_count = bucket_count
        self._cpu_threshold = cpu_threshold
        self._cpu_source = cpu_source or _EventLoopThreadCpu(now)

        # aegis 的 gCPU 初值是 0,EMA 从 0 爬起。照做:进程刚起来时不丢负载。
        self._cpu = 0
        self._last_cpu_sample = now
        self._pass_stat = _RollingCounter(bucket_count, self._bucket_sec, now)
        self._rt_stat = _RollingCounter(bucket_count, self._bucket_sec, now)
        self._in_flight = 0
        # None = 尚未开始丢(对应 aegis 的 prevDropTime == 0)。
        # 用 None 而不是 0.0:monotonic 起点未定义,0.0 可能是合法时刻。
        self._prev_drop_at: float | None = None
        self._max_pass_cache: tuple[float, int] | None = None
        self._min_rt_cache: tuple[float, int] | None = None

    # ── CPU ────────────────────────────────────────────────────────────────

    def _sample_cpu(self, now: float) -> None:
        """按需采样,最多每 `_CPU_SAMPLE_INTERVAL_SEC` 一次。

        ★ aegis 用一个常驻 goroutine 定时采;这里改成在 `should_drop` 里按需采。
        原因是这样**不需要后台任务**:没有启动 / 停止 / 泄漏 / 关服时序要管(§15.2)。
        代价只有"没流量时不更新",而没流量时本来也无可丢。
        """
        if now - self._last_cpu_sample < _CPU_SAMPLE_INTERVAL_SEC:
            return
        self._last_cpu_sample = now
        raw = int(self._cpu_source(now))
        # 对应 aegis 的 min(stat.Usage, 1000);下界 0 是本实现追加的 ——
        # 单调时钟回拨或采样源异常时负值会把 EMA 拉成负数,让限流器此后永不触发。
        raw = min(max(raw, 0), _CPU_MAX_PERMILLE)
        self._cpu = int(self._cpu * _CPU_DECAY + raw * (1.0 - _CPU_DECAY))

    # ── 窗口统计 ────────────────────────────────────────────────────────────

    def _max_pass(self, now: float) -> int:
        """窗口内单桶最大通过数。缓存一个桶时长,对应 aegis 的 maxPASSCache。"""
        cached = self._max_pass_cache
        if cached is not None and now - cached[0] < self._bucket_sec:
            return cached[1]
        # 下限 1.0 对应 aegis 的 `var result = 1.0` —— 没有样本时按"至少通过过 1 个"
        # 算,否则 maxInFlight 恒 0,冷启动第一波流量会被整片丢掉。
        result = 1.0
        for bucket in self._pass_stat.past_buckets(now):
            if bucket:
                result = max(result, math.fsum(bucket))
        value = int(result)
        self._max_pass_cache = (now, value)
        return value

    def _min_rt(self, now: float) -> int:
        """窗口内「单桶平均 RT」的最小值,毫秒。缓存一个桶时长。"""
        cached = self._min_rt_cache
        if cached is not None and now - cached[0] < self._bucket_sec:
            return cached[1]
        result = math.inf
        for bucket in self._rt_stat.past_buckets(now):
            if not bucket:
                continue
            result = min(result, math.fsum(bucket) / len(bucket))
        if math.isinf(result):
            # ★ 这里**刻意不复刻** aegis。它对空窗口算的是 int64(math.Ceil(MaxFloat64)),
            # 那在 Go 里是未定义行为,靠后面 `if rawMinRT <= 0 { rawMinRT = 1 }` 兜住。
            # Python 的 int(ceil(inf)) 直接抛 OverflowError;就算不抛,得到的巨大值会让
            # maxInFlight 大到永不触发 —— 又是"声称有保护、实际没有"。显式取 1。
            value = 1
        else:
            value = int(math.ceil(result))
            if value <= 0:
                value = 1
        self._min_rt_cache = (now, value)
        return value

    def _max_in_flight(self, now: float) -> int:
        """并发上限 = 小定律:maxPASS × bucketPerSecond(次/秒) × minRT(秒)。

        对应 aegis:

            int64(math.Floor(float64(maxPASS*minRT*bucketPerSecond)/1000.0) + 0.5)

        `+ 0.5` 在 Go 里是**空操作**(Floor 已取整,再 +0.5 后 int64 截断又抹掉),
        原样保留是为了让两栈在边界值上逐位一致,不是笔误。
        """
        raw = self._max_pass(now) * self._min_rt(now) * self._bucket_per_second
        return int(math.floor(raw / 1000.0) + 0.5)

    # ── 判定 ────────────────────────────────────────────────────────────────

    def should_drop(self) -> bool:
        """是否应当丢弃本次请求 —— 对应 aegis 的 `shouldDrop`,分支逐条对齐。"""
        now = self._clock()
        self._sample_cpu(now)

        if self._cpu < self._cpu_threshold:
            # CPU 没到阈值。
            prev = self._prev_drop_at
            if prev is None:
                return False
            if now - prev <= _PREV_DROP_MEMORY_SEC:
                # 一秒内刚丢过:CPU 刚回落不代表缓过来了,继续按 inflight 兜。
                in_flight = self._in_flight
                return in_flight > 1 and in_flight > self._max_in_flight(now)
            self._prev_drop_at = None
            return False

        # CPU 超阈值:只有并发也确实超了才丢。
        # ★ `in_flight > 1` 这一条不能省:并发为 1 时丢掉唯一的请求毫无意义
        # (丢了也不会让 CPU 降下来),只会让单线程压测直接全红。
        in_flight = self._in_flight
        drop = in_flight > 1 and in_flight > self._max_in_flight(now)
        if drop and self._prev_drop_at is None:
            self._prev_drop_at = now
        return drop

    # ── 计数 ────────────────────────────────────────────────────────────────

    def begin(self) -> float:
        """登记一次放行,返回起始时刻(交给 `end`)。"""
        self._in_flight += 1
        return self._clock()

    def end(self, start: float) -> None:
        """登记一次完成 —— 对应 aegis `Allow()` 返回的 DoneFunc。

        必须放在 finally 里:漏调会让 inflight 只增不减,几个请求之后
        `in_flight > maxInFlight` 恒成立,CPU 一到阈值就全量丢,且永不恢复。
        """
        now = self._clock()
        rt_ms = math.ceil((now - start) * 1000.0)
        if rt_ms > 0:
            self._rt_stat.add(float(rt_ms), now)
        self._in_flight -= 1
        self._pass_stat.add(1.0, now)

    # ── 观测 ────────────────────────────────────────────────────────────────

    @property
    def in_flight(self) -> int:
        return self._in_flight

    def stat(self) -> Stat:
        now = self._clock()
        return Stat(
            cpu=self._cpu,
            in_flight=self._in_flight,
            max_in_flight=self._max_in_flight(now),
            min_rt=self._min_rt(now),
            max_pass=self._max_pass(now),
        )
