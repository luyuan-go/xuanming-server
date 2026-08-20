"""配置表热更的并发互斥。

★ 这条锁的是一个**版本静默回退**的竞态,不是"多热更一次"。

原形状:

    current = store.tables.version                      # ← 读在 await 之前
    result  = await asyncio.to_thread(load_tables, ...) # ← 让出事件循环
    if result.version < current: reject
    store.replace(result.tables)

两个并发 reload(运维双击、发布脚本重试、canary 与 stable 同时触发)会这样交错:

    A 读 current=4 → A 让出
    B 读 current=4 → B 加载到 v6 → B 判 6>4 通过 → B 切到 v6
    A 加载到 v5    → A 判 5>4 通过(**用的是过期的 4**) → A 切回 v5

结果:内存生效 v5,两次热更都返回成功,运维视图上是 v6。§9.15 的版本单调闸本身
因为读了过期基准而被绕过 —— 此后所有人对"现在生效的是哪批"的判断都是错的。

Go 里 `Store.Load` 整段持 `s.mu`,这里用 `asyncio.Lock` 复现同一个原子段。
"""

from __future__ import annotations

import asyncio

import pytest

from pandorapy.configtable import ReloadMutex


class _FakeStore:
    """只保留竞态相关的三件事:当前版本、换批、互斥。"""

    def __init__(self, version: int) -> None:
        self.version = version
        self.reload_mutex = ReloadMutex()
        self.history: list[int] = []

    def replace(self, version: int) -> None:
        self.version = version
        self.history.append(version)


async def _reload(store: _FakeStore, load_to: int, *, delay: float, locked: bool) -> str:
    """复现 handler 的形状。`locked=False` 是修复前的版本。"""

    async def body() -> str:
        current = store.version
        await asyncio.sleep(delay)          # 站位 asyncio.to_thread(load_tables, ...)
        if load_to == current:
            return "noop"
        if load_to < current:
            return "rejected"
        store.replace(load_to)
        return "reloaded"

    if locked:
        async with store.reload_mutex:
            return await body()
    return await body()


@pytest.mark.asyncio
async def test_unlocked_reload_lets_version_go_backwards() -> None:
    """★ 先证明这个竞态**真的会发生** —— 否则下一条"加锁后没事"没有信息量。

    这一条刻意跑**没有锁**的路径。它红了才说明修复的是真问题。
    """
    store = _FakeStore(4)
    # A 加载慢(到 v5),B 加载快(到 v6)。两者都以 current=4 为基准。
    a = asyncio.create_task(_reload(store, 5, delay=0.05, locked=False))
    b = asyncio.create_task(_reload(store, 6, delay=0.01, locked=False))
    assert await b == "reloaded"
    assert await a == "reloaded"

    assert store.history == [6, 5], "没复现出交错,用例本身失效了"
    assert store.version == 5, (
        "无锁路径下版本应当被回退到 5 —— 这正是要修的缺陷"
    )


@pytest.mark.asyncio
async def test_locked_reload_rejects_the_stale_batch() -> None:
    """★ 同样的交错,加锁后 A 必须被单调闸拒掉。

    关键不只是"串行了",而是 A 在锁内**重新读**到了 B 写下的 6,
    于是 5 < 6 判定成立。锁外读 current 的话,即使串行也照样回退。
    """
    store = _FakeStore(4)
    a = asyncio.create_task(_reload(store, 5, delay=0.05, locked=True))
    b = asyncio.create_task(_reload(store, 6, delay=0.01, locked=True))
    results = {"a": await a, "b": await b}

    assert store.version == 6, f"版本被回退了: {store.history}"
    assert store.history == [6] or store.history == [5, 6], f"意外的切换序列: {store.history}"
    # 谁先拿到锁不确定(取决于调度),但结局唯一:最终版本是 6,且更小的那批被拒。
    assert "rejected" in results.values() or store.history == [5, 6], (
        f"至少有一支应当被单调闸拒掉,实际 {results}"
    )


@pytest.mark.asyncio
async def test_mutex_serialises_overlapping_reloads() -> None:
    """互斥本身:第二个进入者必须等第一个出来。"""
    mu = ReloadMutex()
    trace: list[str] = []

    async def worker(name: str, hold: float) -> None:
        async with mu:
            trace.append(f"{name}-in")
            await asyncio.sleep(hold)
            trace.append(f"{name}-out")

    await asyncio.gather(worker("a", 0.03), worker("b", 0.01))
    # 不许出现 a-in b-in a-out b-out 这种交错
    assert trace in (
        ["a-in", "a-out", "b-in", "b-out"],
        ["b-in", "b-out", "a-in", "a-out"],
    ), trace


@pytest.mark.asyncio
async def test_mutex_released_on_exception() -> None:
    """★ 加载失败(整批拒绝)之后锁必须还回去,否则第一次坏批次就把热更永久锁死。"""
    mu = ReloadMutex()
    with pytest.raises(RuntimeError):
        async with mu:
            raise RuntimeError("bad batch")
    assert not mu.locked
    async with mu:
        pass  # 还能再进,说明没被锁死
