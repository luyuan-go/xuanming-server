"""BBR 自适应限流的用例 —— 对应 pandorapy/bbr.py。

★ 这一族用例的核心不是"代码能跑",是**证明它真的会丢**。

BBR 最危险的失败形态不是丢错请求,是**一条都不丢**:配置写着开、指标里有这个
限流器、日志一切正常,过载时却完全不介入。那正是 server.py 那道 fail-fast 闸
原本要防的事故。所以下面每一条"该丢"的用例,都必须能在实现退化成"永远放行"时
变红 —— 光测"不该丢的时候不丢"是测不出这个的(空实现全绿)。

时间与 CPU 全部注入。真实 CPU 不可控,真实时间不可倒流,不注入就只能写
"构造成功"这种什么都证明不了的断言。
"""

from __future__ import annotations

import asyncio
import math
import time

import grpc
import pytest

from pandorapy import bbr
from pandorapy import errcode
from pandorapy import interceptors as pintercept

# 本文件绝大多数用例是纯同步的。唯一那条端到端用例写成 async def 即可 ——
# pyproject 里 asyncio_mode = "auto",不需要额外标记。


class _Clock:
    """可手动推进的单调时钟。"""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, sec: float) -> None:
        self.now += sec


class _Cpu:
    """可手动设定的 CPU 千分比源。"""

    def __init__(self, permille: int = 0) -> None:
        self.permille = permille
        self.calls = 0

    def __call__(self, now: float) -> int:
        self.calls += 1
        return self.permille


def _limiter(clock: _Clock, cpu: _Cpu, **kw: object) -> bbr.BBR:
    return bbr.BBR(clock=clock, cpu_source=cpu, **kw)  # type: ignore[arg-type]


def _saturate_cpu(clock: _Clock, limiter: bbr.BBR, permille: int = 1000) -> None:
    """把 EMA 推到稳态。

    衰减 0.95 / 500ms 一次,时间常数约 10s —— 一次采样只挪 5%,想跨过 800‰
    需要几十次。这本身就是要锁住的行为(见 test_cpu_ema_is_deliberately_slow)。
    """
    for _ in range(400):
        clock.advance(bbr._CPU_SAMPLE_INTERVAL_SEC)
        limiter.should_drop()


# ── CPU 信号 ────────────────────────────────────────────────────────────────


def test_cpu_starts_at_zero_so_cold_start_never_drops() -> None:
    """进程刚起来时 EMA 从 0 爬,不能丢负载(对齐 aegis 的 gCPU 初值 0)。"""
    clock, cpu = _Clock(), _Cpu(permille=1000)
    limiter = _limiter(clock, cpu)
    limiter._in_flight = 10_000
    assert limiter.should_drop() is False


def test_cpu_ema_is_deliberately_slow() -> None:
    """★ 单次采样只能挪 5% —— 慢是**特性**,不是没调好。

    快速跟随会让一个慢请求带出的 CPU 尖峰立刻触发丢负载。锁住这条,
    免得后来的人"顺手把 decay 调灵敏一点"。
    """
    clock, cpu = _Clock(), _Cpu(permille=1000)
    limiter = _limiter(clock, cpu)
    clock.advance(bbr._CPU_SAMPLE_INTERVAL_SEC)
    limiter.should_drop()
    assert limiter.stat().cpu == 50  # 0*0.95 + 1000*0.05


def test_cpu_sampling_is_throttled_to_the_interval() -> None:
    """采样按需但节流:同一个采样周期内多次调用只采一次。"""
    clock, cpu = _Clock(), _Cpu(permille=500)
    limiter = _limiter(clock, cpu)
    for _ in range(50):
        limiter.should_drop()
    assert cpu.calls == 0  # 距构造时刻不足一个采样周期
    clock.advance(bbr._CPU_SAMPLE_INTERVAL_SEC)
    for _ in range(50):
        limiter.should_drop()
    assert cpu.calls == 1


def test_negative_cpu_sample_cannot_poison_the_ema() -> None:
    """★ 负值必须被夹到 0,否则 EMA 变负 → 此后永不触发限流。

    时钟回拨或采样源异常会给出负值。这不是理论风险:一旦 EMA 被拉到负数,
    它要爬回 800‰ 需要极长时间,期间限流器完全失效且毫无迹象。
    """
    clock, cpu = _Clock(), _Cpu(permille=1000)
    limiter = _limiter(clock, cpu)
    _saturate_cpu(clock, limiter)
    assert limiter.stat().cpu >= bbr.DEFAULT_CPU_THRESHOLD
    cpu.permille = -100_000
    clock.advance(bbr._CPU_SAMPLE_INTERVAL_SEC)
    limiter.should_drop()
    assert limiter.stat().cpu >= 0


def test_cpu_sample_is_capped_at_1000() -> None:
    """对应 aegis 的 min(stat.Usage, 1000)。"""
    clock, cpu = _Clock(), _Cpu(permille=999_999)
    limiter = _limiter(clock, cpu)
    _saturate_cpu(clock, limiter)
    assert limiter.stat().cpu <= bbr._CPU_MAX_PERMILLE


# ── 判定 ────────────────────────────────────────────────────────────────────


def test_high_cpu_alone_does_not_drop() -> None:
    """CPU 到顶但并发很低 → 不丢。丢了也不会让 CPU 降下来。"""
    clock, cpu = _Clock(), _Cpu(permille=1000)
    limiter = _limiter(clock, cpu)
    _saturate_cpu(clock, limiter)
    limiter._in_flight = 1
    assert limiter.should_drop() is False


def test_it_actually_drops_when_cpu_and_inflight_both_exceed() -> None:
    """★★ 本文件最重要的一条:过载时它**真的丢**。

    实现退化成"永远放行"(空实现、CPU 信号选错、inflight 没归还导致的死锁式误判
    的反面)时,这条必红。
    """
    clock, cpu = _Clock(), _Cpu(permille=1000)
    limiter = _limiter(clock, cpu)
    _saturate_cpu(clock, limiter)
    limiter._in_flight = 100_000
    assert limiter.should_drop() is True


def test_low_cpu_never_drops_however_high_the_concurrency() -> None:
    """★ CPU 没到阈值 → 无论并发多高都不丢。

    这是 BBR 与配额限流的分界:它保的是"机器别倒",不是"并发别超"。
    单纯并发高但机器扛得住(例如都在等数据库)属于正常,丢了纯属误伤。
    """
    clock, cpu = _Clock(), _Cpu(permille=0)
    limiter = _limiter(clock, cpu)
    _saturate_cpu(clock, limiter)
    limiter._in_flight = 1_000_000
    assert limiter.should_drop() is False


def test_drop_keeps_going_for_one_second_after_cpu_falls_back() -> None:
    """★ CPU 回落后仍按 inflight 兜一秒 —— 防震荡。

    少了这段记忆,CPU 一抖就全量放行,把刚缓过来的进程再打死,来回摆。
    """
    clock, cpu = _Clock(), _Cpu(permille=1000)
    limiter = _limiter(clock, cpu)
    _saturate_cpu(clock, limiter)
    limiter._in_flight = 100_000
    assert limiter.should_drop() is True

    cpu.permille = 0
    _saturate_cpu(clock, limiter)  # EMA 掉回阈值以下
    assert limiter.stat().cpu < bbr.DEFAULT_CPU_THRESHOLD
    # 距上次记录的"开始丢"已远超 1s,记忆过期 → 放行
    assert limiter.should_drop() is False


def test_prev_drop_memory_window_is_one_second() -> None:
    """记忆窗口正好 1s:窗口内按 inflight 继续丢,窗口外放行。"""
    clock, cpu = _Clock(), _Cpu(permille=1000)
    limiter = _limiter(clock, cpu)
    _saturate_cpu(clock, limiter)
    limiter._in_flight = 100_000
    assert limiter.should_drop() is True

    # 让 CPU 立刻降到阈值下,但**不**推进采样周期(避免 EMA 继续变化干扰判定)
    limiter._cpu = 0
    clock.advance(0.5)
    assert limiter.should_drop() is True  # 记忆窗口内
    clock.advance(1.0)  # 累计 1.5s > 1s
    assert limiter.should_drop() is False
    assert limiter._prev_drop_at is None  # 记忆已清


# ── 滑动窗与 maxInFlight ────────────────────────────────────────────────────


def test_max_pass_floors_at_one_so_cold_start_is_not_wiped_out() -> None:
    """没有样本时 maxPASS = 1(对齐 aegis 的 result = 1.0)。

    若返回 0,maxInFlight 恒 0 → 冷启动第一波流量在 CPU 一高时被整片丢掉。
    """
    clock, cpu = _Clock(), _Cpu()
    limiter = _limiter(clock, cpu)
    assert limiter.stat().max_pass == 1


def test_min_rt_on_empty_window_is_one_not_infinity() -> None:
    """★ 空窗口的 minRT 必须是 1。

    aegis 在这里算的是 int64(math.Ceil(MaxFloat64)) —— Go 的未定义行为,靠
    `<= 0` 兜住。照抄到 Python 会 OverflowError,或者得到一个巨大值让
    maxInFlight 大到永不触发(又是"声称有保护、实际没有")。这里显式取 1。
    """
    clock, cpu = _Clock(), _Cpu()
    limiter = _limiter(clock, cpu)
    assert limiter.stat().min_rt == 1


def test_current_bucket_is_excluded_from_the_window() -> None:
    """★ 当前桶不参与统计 —— 它还没攒满。

    算进去会让 maxPASS 系统性偏低 → maxInFlight 偏低 → 越忙越容易误丢。
    """
    clock, cpu = _Clock(), _Cpu()
    limiter = _limiter(clock, cpu, window_sec=1.0, bucket_count=10)
    # 只往当前桶塞,不推进时间
    for _ in range(50):
        limiter.end(limiter.begin())
    assert limiter.stat().max_pass == 1  # 全在当前桶里,统计看不到

    clock.advance(0.1)  # 推进一个桶,刚才那桶变成"过去"
    limiter._max_pass_cache = None
    assert limiter.stat().max_pass == 50


def test_window_fully_expires_after_the_whole_window_elapses() -> None:
    """整窗静默后统计归零,不能留着陈年高水位当 maxPASS。"""
    clock, cpu = _Clock(), _Cpu()
    limiter = _limiter(clock, cpu, window_sec=1.0, bucket_count=10)
    for _ in range(50):
        limiter.end(limiter.begin())
    clock.advance(0.1)
    limiter._max_pass_cache = None
    assert limiter.stat().max_pass == 50

    clock.advance(10.0)  # 远超一整窗
    limiter._max_pass_cache = None
    assert limiter.stat().max_pass == 1


def test_bucket_boundaries_do_not_drift() -> None:
    """★ 桶边界按整数倍累加,不能钉到"最后一次调用时刻"。

    钉住的话每个桶实际时长会在 [d, 2d) 漂移,窗口总时长跟着失真 ——
    maxPASS/minRT 全部偏,而且偏多少取决于调用节奏,不可复现。
    """
    clock = _Clock()
    counter = bbr._RollingCounter(10, 0.1, clock.now)
    start = counter._bucket_start
    clock.advance(0.35)  # 3 个整桶 + 0.05 余
    counter.add(1.0, clock.now)
    assert counter._bucket_start == pytest.approx(start + 0.3)


def test_bucket_per_second_uses_integer_division_like_go() -> None:
    """★ bucketPerSecond 必须复刻 Go 的 duration 整除。

    它是 maxInFlight 的乘数;用浮点 1/bucket_sec 在除不尽时会差一档。
    window=10s / 100 桶 → 100ms/桶 → 每秒 10 桶。
    """
    clock, cpu = _Clock(), _Cpu()
    limiter = _limiter(clock, cpu)
    assert limiter._bucket_per_second == 10


def test_max_in_flight_follows_littles_law() -> None:
    """maxInFlight = floor(maxPASS × minRT(ms) × 桶/秒 / 1000)。

    aegis 那句 `+ 0.5` 是空操作(Floor 后再 +0.5 又被 int 截断),原样保留是为了
    两栈逐位一致。这里用具体数字钉死,免得有人"清理"掉它时算错。
    """
    clock, cpu = _Clock(), _Cpu()
    limiter = _limiter(clock, cpu, window_sec=1.0, bucket_count=10)
    # 造 20 次通过、每次 RT=50ms,全落进同一个桶
    for _ in range(20):
        start = limiter.begin()
        clock.advance(0.05)
        limiter.end(start)
        clock.now = start  # 回到桶内,避免跨桶
    clock.advance(0.1)
    limiter._max_pass_cache = None
    limiter._min_rt_cache = None
    stat = limiter.stat()
    expected = int(math.floor(stat.max_pass * stat.min_rt * 10 / 1000.0) + 0.5)
    assert stat.max_in_flight == expected


# ── 计数 ────────────────────────────────────────────────────────────────────


def test_begin_end_balance_inflight() -> None:
    clock, cpu = _Clock(), _Cpu()
    limiter = _limiter(clock, cpu)
    starts = [limiter.begin() for _ in range(5)]
    assert limiter.in_flight == 5
    for s in starts:
        limiter.end(s)
    assert limiter.in_flight == 0


def test_sub_millisecond_calls_do_not_pollute_rt() -> None:
    """RT 向上取整后为 0 的样本不记 —— 对应 aegis 的 `if rt > 0`。

    记 0 会把 minRT 压到 0,再被下限抬回 1,等于用最快的一次覆盖真实分布。
    """
    clock, cpu = _Clock(), _Cpu()
    limiter = _limiter(clock, cpu, window_sec=1.0, bucket_count=10)
    for _ in range(10):
        limiter.end(limiter.begin())  # 时钟没动,RT=0
    clock.advance(0.1)
    limiter._min_rt_cache = None
    assert limiter.stat().min_rt == 1  # 空 RT 窗口的下限,而不是被 0 拉走


# ── 真实 CPU 采样器 ─────────────────────────────────────────────────────────
#
# ★ 上面所有用例都注入了假 CPU —— 它们证明"信号到了 800‰ 就会丢",但证明不了
# "真实过载时信号真的会到 800‰"。这一段补的就是那一环。这不是可选的补充:
# 整个 BBR 唯一致命的失败形态就是信号选错(比如照搬 aegis 去读 cgroup 整容器
# CPU),表现是永远读不到 800‰、一条都不丢,而配置、指标、日志三样都正常。


def _busy_spin(seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        pass


def test_real_sampler_reports_saturated_when_the_thread_is_busy() -> None:
    """★ 线程被打满 → 接近 1000‰,且必须越过 800‰ 阈值。

    读错对象(整容器 CPU)时这条必红:单线程 Python 在多核容器上永远读不到 800‰。
    """
    src = bbr._EventLoopThreadCpu(time.monotonic())
    _busy_spin(0.2)
    permille = src(time.monotonic())
    assert permille > bbr.DEFAULT_CPU_THRESHOLD, f"实测 {permille}‰,够不到阈值"


def test_real_sampler_reports_idle_when_the_thread_is_waiting() -> None:
    """等 I/O(这里用 sleep 代表)时不该算饱和 —— 那种情况不是本进程扛不住。"""
    src = bbr._EventLoopThreadCpu(time.monotonic())
    time.sleep(0.15)
    permille = src(time.monotonic())
    assert permille < 200, f"实测 {permille}‰,空闲被当成了饱和"


def test_real_sampler_survives_zero_wall_delta() -> None:
    """同一时刻连采两次不能除零。"""
    now = time.monotonic()
    src = bbr._EventLoopThreadCpu(now)
    assert src(now) == 0


async def test_bbr_end_to_end_drops_under_a_saturated_event_loop() -> None:
    """★★ 不注入任何东西的端到端:真时钟 + 真 CPU 采样器 + 真事件循环。

    把事件循环打满后,BBR 必须开始丢。这条是对"整套装配"的最终判据 ——
    前面的用例各自成立、拼起来却不生效(信号量纲不对、采样从没触发、
    inflight 永远是 0)时,只有它会红。

    ★ 阈值调到 200‰ 而不是默认 800‰,原因是 EMA 时间常数:decay=0.95 + 500ms
    采样 → 爬到 800‰ 需要约 32 次采样 = **16 秒真实满载 CPU**,放进单测不可接受。
    降阈值只改这一个入参,`should_drop` 的判定路径与生产完全同一条;而"EMA 爬得慢"
    本身已由 test_cpu_ema_is_deliberately_slow 单独钉住,没有漏测。
    """
    threshold = 200
    limiter = bbr.BBR(window_sec=1.0, bucket_count=10, cpu_threshold=threshold)
    # 先喂一段真实 RT 分布,否则 maxInFlight 停在冷启动下限
    for _ in range(30):
        start = limiter.begin()
        _busy_spin(0.002)
        limiter.end(start)
        await asyncio.sleep(0)
    # 满载 6 个采样周期(3s):EMA ≈ 1000×(1-0.95^6) ≈ 265,越过 200
    for _ in range(6):
        _busy_spin(bbr._CPU_SAMPLE_INTERVAL_SEC)
        limiter.should_drop()
    assert limiter.stat().cpu > threshold, limiter.stat()

    # 造出真实的高并发在途
    holders = [limiter.begin() for _ in range(limiter.stat().max_in_flight + 100)]
    try:
        assert limiter.should_drop() is True, limiter.stat()
    finally:
        for h in holders:
            limiter.end(h)


# ── 配置 ────────────────────────────────────────────────────────────────────


def test_single_bucket_config_is_rejected() -> None:
    """★ 1 个桶时 past_buckets 恒空 → 统计恒为常数,限流器不再自适应。

    静默劣化成"永远放行"是最坏结果,所以直接拒配。
    """
    with pytest.raises(ValueError, match="bucket_count"):
        bbr.BBR(bucket_count=1)


def test_non_positive_window_is_rejected() -> None:
    with pytest.raises(ValueError, match="window_sec"):
        bbr.BBR(window_sec=0)


def test_defaults_match_aegis() -> None:
    """默认值与 go-kratos/aegis 逐项一致 —— 两栈灰度期行为要可比。"""
    assert bbr.DEFAULT_WINDOW_SEC == 10.0
    assert bbr.DEFAULT_BUCKET_COUNT == 100
    assert bbr.DEFAULT_CPU_THRESHOLD == 800
    assert bbr._CPU_DECAY == 0.95
    assert bbr._CPU_SAMPLE_INTERVAL_SEC == 0.5


# ── 拦截器 ──────────────────────────────────────────────────────────────────


class _Details:
    def __init__(self, method: str) -> None:
        self.method = method
        self.invocation_metadata = ()


class _Ctx:
    """最小 ServicerContext:只记录 abort。"""

    def __init__(self) -> None:
        self.aborted_code: grpc.StatusCode | None = None
        self.aborted_details = ""

    async def abort(self, code: grpc.StatusCode, details: str) -> None:
        self.aborted_code = code
        self.aborted_details = details
        raise grpc.aio.AbortError()


def _handler(inner: object, *, streaming: bool = False) -> grpc.RpcMethodHandler:
    if streaming:
        return grpc.unary_stream_rpc_method_handler(inner)  # type: ignore[arg-type]
    return grpc.unary_unary_rpc_method_handler(inner)  # type: ignore[arg-type]


async def _intercept(
    itc: pintercept.RateLimitInterceptor,
    method: str,
    handler: grpc.RpcMethodHandler,
) -> grpc.RpcMethodHandler:
    async def continuation(_: object) -> grpc.RpcMethodHandler:
        return handler

    return await itc.intercept_service(continuation, _Details(method))  # type: ignore[arg-type]


def _forced(drop: bool) -> bbr.BBR:
    """造一个判定确定的 BBR(仍走真实的 begin/end 计数路径)。"""
    clock, cpu = _Clock(), _Cpu(permille=1000 if drop else 0)
    limiter = _limiter(clock, cpu)
    _saturate_cpu(clock, limiter)
    if drop:
        limiter._in_flight = 100_000
    assert limiter.should_drop() is drop
    return limiter


async def test_interceptor_passes_through_and_returns_inflight() -> None:
    limiter = _forced(drop=False)
    before = limiter.in_flight
    calls: list[str] = []

    async def inner(request: object, context: object) -> str:
        calls.append("hit")
        return "ok"

    wrapped = await _intercept(
        pintercept.RateLimitInterceptor(limiter), "/pandora.x.v1.X/Do", _handler(inner)
    )
    assert await wrapped.unary_unary(object(), _Ctx()) == "ok"
    assert calls == ["hit"]
    assert limiter.in_flight == before


async def test_interceptor_actually_rejects_with_resource_exhausted() -> None:
    """★★ 过载时必须真的拒,且拒的是 RESOURCE_EXHAUSTED(与 Go 的
    errcode.ErrRateLimited → RESOURCE_EXHAUSTED 同码,客户端两栈一致)。"""
    limiter = _forced(drop=True)
    calls: list[str] = []

    async def inner(request: object, context: object) -> str:
        calls.append("hit")
        return "ok"

    wrapped = await _intercept(
        pintercept.RateLimitInterceptor(limiter), "/pandora.x.v1.X/Do", _handler(inner)
    )
    ctx = _Ctx()
    with pytest.raises(grpc.aio.AbortError):
        await wrapped.unary_unary(object(), ctx)
    assert ctx.aborted_code == grpc.StatusCode.RESOURCE_EXHAUSTED
    assert str(errcode.ErrRateLimited) in ctx.aborted_details
    assert calls == []  # 业务 handler 一次都没进


async def test_health_check_is_never_dropped() -> None:
    """★ 基础设施方法必须原样放行,连包装都不加。

    丢健康检查 = k8s 判 Pod NotReady 把它摘掉;过载时摘副本会把剩下的压得更狠,
    自适应丢负载当场变成雪崩加速器。
    """
    limiter = _forced(drop=True)
    handler = _handler(lambda req, ctx: "pong")
    wrapped = await _intercept(
        pintercept.RateLimitInterceptor(limiter), "/grpc.health.v1.Health/Check", handler
    )
    assert wrapped is handler


async def test_streaming_handler_is_left_alone() -> None:
    """★ 流不能被重包成 unary —— 那样客户端收到的是协议错误,不是"过载"。"""
    limiter = _forced(drop=True)
    handler = _handler(lambda req, ctx: iter(()), streaming=True)
    wrapped = await _intercept(
        pintercept.RateLimitInterceptor(limiter), "/pandora.push.v1.Push/Subscribe", handler
    )
    assert wrapped is handler


async def test_inflight_is_returned_even_when_the_handler_raises() -> None:
    """★ 漏归还 inflight 会让它单调上涨 → CPU 一到阈值就整片丢且永不恢复,
    而进程看起来完全正常。异常路径必须走 finally。"""
    limiter = _forced(drop=False)
    before = limiter.in_flight

    async def inner(request: object, context: object) -> str:
        raise RuntimeError("boom")

    wrapped = await _intercept(
        pintercept.RateLimitInterceptor(limiter), "/pandora.x.v1.X/Do", _handler(inner)
    )
    with pytest.raises(RuntimeError, match="boom"):
        await wrapped.unary_unary(object(), _Ctx())
    assert limiter.in_flight == before


async def test_sync_handler_is_supported() -> None:
    """同步 servicer 必须能挂 —— 无条件 await 会比 grpcio 本体更严,
    历史上正是这样把同步版 HealthServicer 打挂过(见 _call_handler 注释)。"""
    limiter = _forced(drop=False)
    wrapped = await _intercept(
        pintercept.RateLimitInterceptor(limiter),
        "/pandora.x.v1.X/Do",
        _handler(lambda req, ctx: "sync-ok"),
    )
    assert await wrapped.unary_unary(object(), _Ctx()) == "sync-ok"


async def test_drop_is_decided_per_call_not_once_per_method() -> None:
    """★ 判定不能在 intercept_service 里定死。

    定死的话得到的是"这个方法首次被调用时过不过载",此后永远不变 ——
    压力过去了还在丢,或者过载了却一直放行。
    """
    clock, cpu = _Clock(), _Cpu(permille=0)
    limiter = _limiter(clock, cpu)
    _saturate_cpu(clock, limiter)
    wrapped = await _intercept(
        pintercept.RateLimitInterceptor(limiter),
        "/pandora.x.v1.X/Do",
        _handler(lambda req, ctx: "ok"),
    )
    assert await wrapped.unary_unary(object(), _Ctx()) == "ok"

    # 同一个 wrapped handler,机器过载了
    cpu.permille = 1000
    _saturate_cpu(clock, limiter)
    limiter._in_flight = 100_000
    with pytest.raises(grpc.aio.AbortError):
        await wrapped.unary_unary(object(), _Ctx())
