"""后台协程的统一异常兜底 —— 对应 Go 侧 `pkg/safego`（压测审核【必修-6】）。

**为什么 Python 侧比 Go 更需要它**：Go 里一个没 recover 的 panic 会**崩掉整个进程**，
响亮到不可能忽略；asyncio 相反 —— `create_task()` 出去的协程抛异常后，
异常被存进 Task 对象里**等人来取**，没人取就一直躺着：

  - 进程照常运行、gRPC 端口照常服务、health 照答 `SERVING`；
  - 那条后台循环（撮合 tick / 心跳清扫 / presence tick / 看门狗）**已经死了**；
  - 全程**零日志**。asyncio 的 "Task exception was never retrieved" 只在 Task 被
    垃圾回收时才打，而 `server.run()` 把任务存在列表里持有到进程结束，
    停机时又 `suppress(..., Exception)` 把它取走 —— 那条警告因此永远不会出现。

实测（2026-08-19）：起真 server + 一个立刻抛异常的 background 协程，
1.3 秒内（含一次强制 `gc.collect()`）服务仍在跑、health=SERVING、日志 0 行。
这正是 §5.2.2 那批"起得来、跑得对、就是坏的"缺陷的同一形状。

**与 Go 侧的对齐口径**（字段名漂移 = Loki 告警永不触发，见 dbguard 那次）：

    事件名  panic_recovered
    字段    name / panic / stack
    指标    pandora_safego_panic_recovered_total{name}

两个原语按生命周期选用，语义逐条对齐 Go：

  - `spawn(name, factory)`  ← Go 的 `safego.Go`：一次性协程，异常只终止它自己，
    打完整 traceback + 计数后结束，**不重启**（单次任务没有"续跑"语义）。
  - `loop(name, interval, fn)` ← Go 的 `safego.Loop`：周期循环，异常只终止**本轮**，
    下个 tick 继续 —— 循环整体存活，单轮故障不停摆。
    `interval <= 0` 时立即返回并打 `safego_loop_invalid_interval`（防误配自旋）。
    首轮在**第一个 tick 到达后**执行，不立即执行（`time.Ticker` 语义）。

⚠️ `asyncio.CancelledError` **不算异常**，不计数也不打 ERROR。停机时 `run()`
   会取消全部后台任务，把取消当故障会让每次正常滚动更新都产生一批假告警 ——
   与 §5.2.1 ⑰（panic 兜底把 `CancelledError` 也算 panic）是同一个坑。

边界（与 Go 侧同）：本模块只兜可恢复的异常。事件循环本身被破坏（如 C 扩展段错误）
不在覆盖范围内，不提供任何此类兜底的假象。
"""

from __future__ import annotations

import asyncio
import traceback
from collections.abc import Awaitable, Callable

from prometheus_client import Counter

from pandorapy import log as plog

# 与 Go 侧 pkg/safego.PanicRecovered 同名同 label。
# name 是代码点位名(有界枚举),不是 player_id 等高基数值(§12)。
PANIC_RECOVERED = Counter(
    "pandora_safego_panic_recovered_total",
    "panics recovered by safego, by coroutine/callsite name",
    ["name"],
)


def recovered(name: str, exc: BaseException) -> bool:
    """处理一个已捕获的异常：计数 + 打 stack。对应 Go 的 `safego.Recovered`。

    `CancelledError` 返回 False 且不产生任何记录 —— 它是停机的正常控制流。
    """
    if isinstance(exc, asyncio.CancelledError):
        return False
    PANIC_RECOVERED.labels(name).inc()
    plog.get().error(
        "panic_recovered",
        name=name,
        panic=repr(exc),
        stack="".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
    )
    return True


def _on_done(name: str) -> Callable[[asyncio.Task], None]:
    """done 回调：无论任务怎么结束都**取一次**异常，让它不可能停在"没人取"的状态。"""

    def _cb(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            recovered(name, exc)

    return _cb


def supervise(name: str, task: asyncio.Task) -> asyncio.Task:
    """给一个**已经创建**的 Task 挂上兜底。返回原 task，方便链式写。

    用于 `server.run()` 这类"任务已建好、只是缺兜底"的场合。
    """
    task.add_done_callback(_on_done(name))
    return task


def spawn(name: str, factory: Callable[[], Awaitable[None]]) -> asyncio.Task:
    """一次性后台协程。异常只终止它自己（打 stack + 计数，不重启）。

    对应 Go 的 `safego.Go(ctx, name, fn)`。
    """
    return supervise(name, asyncio.create_task(factory(), name=name))


async def run_once(name: str, fn: Callable[[], Awaitable[None]]) -> bool:
    """同步执行一轮并兜住其中的异常，返回"是否发生了异常"。

    对应 Go 的 `safego.Run` —— 供既有循环就地包装单轮 tick 体。
    `CancelledError` **向外抛**：取消必须能穿透，否则停机时循环退不出去。
    """
    try:
        await fn()
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001 —— 兜底就是要抓全部
        recovered(name, exc)
        return True
    return False


async def loop(
    name: str,
    interval: float,
    fn: Callable[[], Awaitable[None]],
) -> None:
    """周期循环，每轮独立兜底；单轮异常不终止循环。对应 Go 的 `safego.Loop`。

    - `interval <= 0` 立即返回并打 `safego_loop_invalid_interval`（防误配自旋，与 Go 同）。
    - 首轮在第一个 tick **之后**执行，不立即执行（`time.Ticker` 语义，与 Go 同）。
    - 被取消时直接向外抛 `CancelledError`，由调用方（通常是 `server.run()` 的停机段）处理。

    注意这里用 `sleep(interval)` 而不是"到点补齐"的漂移校正：Go 的 `time.Ticker`
    在 `fn` 跑得比 interval 久时会丢 tick 而不是堆积，`fn` 返回后等下一个 interval
    的行为与之等价，且不会在慢轮之后突然连打几轮。
    """
    if interval <= 0:
        plog.get().error("safego_loop_invalid_interval", name=name, interval=interval)
        return
    while True:
        await asyncio.sleep(interval)
        await run_once(name, fn)
