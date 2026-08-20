"""`pandorapy.safego` + `server.run()` 后台兜底的回归测试。

守的是 2026-08-19 实测出来的这个形状：起真 server + 一个立刻抛异常的 background 协程，
**进程仍在跑、health 仍答 SERVING、日志 0 行**。asyncio 的
"Task exception was never retrieved" 只在 Task 被 GC 时才打，而 `run()` 把任务
持有到进程结束、停机时又 `suppress(..., Exception)` 把异常取走 —— 那条警告因此永不出现。
Go 里同一个缺陷会崩进程（响亮），Python 里完全静默：迁移把一个吵闹的故障变成了哑的。

对齐 Go `pkg/safego` 的三处口径（漂移了 = Loki/Grafana 上按名字建的东西静默失效，
与 dbguard 字段分叉是同一类）：

    事件名  panic_recovered
    字段    name / panic / stack
    指标    pandora_safego_panic_recovered_total{name}

日志捕获走 `PrintLoggerFactory(file=buf)` 而不是 pytest 的 `caplog` ——
structlog 不经过 stdlib logging，`caplog.records` 恒空（本文件第一版就是这么假绿的）。
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json

import pytest
import structlog

from pandorapy import config as pconfig
from pandorapy import log as plog
from pandorapy import safego, server


@pytest.fixture
def logbuf() -> io.StringIO:
    """把 structlog 输出接到内存 —— 与 test_log_field_contract.py 同一套做法。"""
    buf = io.StringIO()
    plog.setup("test-safego")
    structlog.configure(
        processors=structlog.get_config()["processors"],
        wrapper_class=structlog.get_config()["wrapper_class"],
        logger_factory=structlog.PrintLoggerFactory(file=buf),
        cache_logger_on_first_use=False,
    )
    return buf


def _lines(buf: io.StringIO) -> list[dict]:
    out = []
    for ln in buf.getvalue().splitlines():
        if ln.strip():
            with contextlib.suppress(json.JSONDecodeError):
                out.append(json.loads(ln))
    return out


def _events(buf: io.StringIO, msg: str) -> list[dict]:
    return [r for r in _lines(buf) if r.get("msg") == msg]


def _counter(name: str) -> float:
    return float(safego.PANIC_RECOVERED.labels(name)._value.get())  # noqa: SLF001


# ── 1. 与 Go 的字段 / 指标口径 ───────────────────────────────────────────

def test_recovered_uses_go_field_names(logbuf):
    before = _counter("boom_point")
    assert safego.recovered("boom_point", RuntimeError("炸了")) is True
    assert _counter("boom_point") == before + 1

    rec = _events(logbuf, "panic_recovered")
    assert rec, f"没打 panic_recovered，实际：{_lines(logbuf)}"
    entry = rec[-1]
    # Go: plog.Errorw("msg","panic_recovered","name",..,"panic",..,"stack",..)
    assert entry["level"] == "error"
    assert entry["name"] == "boom_point"
    assert "炸了" in entry["panic"]
    assert "RuntimeError" in entry["stack"], "stack 必须是完整 traceback，不是一行 repr"


def test_cancellation_is_not_a_panic(logbuf):
    """停机会取消全部后台任务。把取消当故障 = 每次滚动更新一批假告警（§5.2.1 ⑰ 同坑）。"""
    before = _counter("cancel_point")
    assert safego.recovered("cancel_point", asyncio.CancelledError()) is False
    assert _counter("cancel_point") == before
    assert not _events(logbuf, "panic_recovered")


# ── 2. spawn / loop 的生命周期语义（逐条对齐 Go）─────────────────────────

async def test_spawn_reports_a_dying_coroutine(logbuf):
    async def doomed() -> None:
        raise RuntimeError("SPAWN_BOOM")

    task = safego.spawn("one_shot", doomed)
    with contextlib.suppress(RuntimeError):
        await task
    await asyncio.sleep(0)  # 让 done 回调被调度

    rec = _events(logbuf, "panic_recovered")
    assert rec and "SPAWN_BOOM" in rec[-1]["panic"], (
        f"一次性协程死了却没有任何记录：{_lines(logbuf)}"
    )


async def test_loop_survives_a_failing_round(logbuf):
    """Go 的 Loop：异常只终止**本轮**，下个 tick 继续，循环整体不停摆。"""
    rounds = 0

    async def flaky() -> None:
        nonlocal rounds
        rounds += 1
        if rounds == 1:
            raise RuntimeError("LOOP_BOOM 第一轮炸")

    task = asyncio.create_task(safego.loop("tick", 0.01, flaky))
    # 等条件 + deadline，不用固定 sleep（§5.3：固定 sleep 的并发测试全量跑偶发红）
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 5
    while rounds < 3 and loop.time() < deadline:
        await asyncio.sleep(0.005)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert rounds >= 3, f"第一轮炸掉之后循环没继续，只跑了 {rounds} 轮"
    assert len(_events(logbuf, "panic_recovered")) == 1, "只该记一轮"


async def test_loop_refuses_non_positive_interval(logbuf):
    """interval<=0 必须立刻返回，不能变成自旋（Go 侧同样的防误配闸）。"""
    calls = 0

    async def fn() -> None:
        nonlocal calls
        calls += 1

    await asyncio.wait_for(safego.loop("bad", 0, fn), timeout=2)
    assert calls == 0
    assert _events(logbuf, "safego_loop_invalid_interval")


async def test_loop_does_not_run_before_the_first_tick():
    """Go 的 Ticker 语义：首轮在第一个 tick **之后**。抢跑会让启动瞬间多打一轮。"""
    calls = 0

    async def fn() -> None:
        nonlocal calls
        calls += 1

    task = asyncio.create_task(safego.loop("late", 0.5, fn))
    await asyncio.sleep(0.05)
    assert calls == 0, "还没到第一个 tick 就跑了"
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


# ── 3. 装配层：server.run() 的 background 必须被兜住 ─────────────────────


async def _run_service(*, background, ready: asyncio.Event) -> asyncio.Task:
    return asyncio.create_task(
        server.run(
            service_name="test-safego",
            grpc_server=server.build_grpc_server(pconfig.GrpcConf()),
            grpc_addr=":0",  # 让内核挑端口：全量跑时固定端口会互撞
            background=background,
            on_ready=ready.set,
        )
    )


async def _stop(runner: asyncio.Task) -> None:
    runner.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await runner


async def test_server_run_reports_a_dead_background_loop(logbuf):
    """★ 本文件的核心用例。

    把 `server.run()` 里 background 的 safego 装配拆回裸 `asyncio.create_task`，
    这条**必须**变红（实测拆掉后：服务仍在跑、health 仍 SERVING、日志 0 行）。
    """
    exploded = asyncio.Event()

    async def doomed_sweep() -> None:
        exploded.set()
        raise RuntimeError("SWEEP_BOOM 心跳巡检循环炸了")

    ready = asyncio.Event()
    runner = await _run_service(background=[doomed_sweep], ready=ready)
    try:
        await asyncio.wait_for(ready.wait(), timeout=10)
        await asyncio.wait_for(exploded.wait(), timeout=10)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 5
        while loop.time() < deadline and not _events(logbuf, "panic_recovered"):
            await asyncio.sleep(0.01)
    finally:
        await _stop(runner)

    rec = _events(logbuf, "panic_recovered")
    assert rec, (
        "后台协程死了，服务却一行日志都没打 —— 这正是 2026-08-19 实测到的静默形状。"
        f"实际日志：{_lines(logbuf)}"
    )
    assert "SWEEP_BOOM" in rec[-1]["panic"]
    assert rec[-1]["name"] == "doomed_sweep", "点位名必须能指回是哪条循环死了"


async def test_normal_shutdown_of_a_healthy_background_is_quiet(logbuf):
    """长跑正常的后台协程在停机时被取消，不该产生任何 panic_recovered。"""

    async def healthy() -> None:
        while True:
            await asyncio.sleep(0.01)

    ready = asyncio.Event()
    runner = await _run_service(background=[healthy], ready=ready)
    await asyncio.wait_for(ready.wait(), timeout=10)
    await asyncio.sleep(0.05)
    await _stop(runner)
    await asyncio.sleep(0.05)

    assert not _events(logbuf, "panic_recovered"), (
        "正常停机被记成了故障 —— 每次滚动更新都会产生假告警"
    )
