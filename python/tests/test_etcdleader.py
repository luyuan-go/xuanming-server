"""etcdleader 选主测试 —— 打真实 etcd,不用 mock。

为什么不能用 mock:本模块的全部价值是"在 etcd 真实的 lease / txn 语义下不脑裂"。
mock 掉 etcd 就等于把被测对象换成了我自己对 etcd 的想象 —— 而这次迁移的教训恰恰是
"我以为的"和"实测的"经常不一样(aetcd 没有自动 keepalive 就是一例)。

需要一个可用的 etcd。没有就整体 skip(不假装通过):
    docker run -d --name pandora-etcd-verify -p 12379:2379 quay.io/coreos/etcd:v3.5.17 \
      etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://127.0.0.1:12379
环境变量 PANDORA_TEST_ETCD_ENDPOINTS 可覆盖地址(与 Go 侧 CI 的门控变量同名)。
"""

from __future__ import annotations

import asyncio
import contextlib
import time

import pytest

import etcdfixture as efixture
from etcdfixture import ENDPOINT

from pandorapy import etcdleader

@pytest.fixture(scope="module", autouse=True)
async def require_etcd() -> None:
    # 探活与"什么才算不可用"的判据在 tests/etcdfixture.py:
    # 原先这里是 `except Exception: return False` + 不带异常的跳过文案,
    # 配置写错 / 驱动 API 变化会让 44 条真 etcd 用例静默消失而退出码 0。
    await efixture.require_etcd("选主测试")


@pytest.fixture
async def clean_key():
    """每个用例用独立的选举名,避免相互干扰。"""
    import aetcd

    names: list[str] = []

    def _make(name: str) -> str:
        full = f"test/{name}/{time.monotonic_ns()}"
        names.append(full)
        return full

    yield _make

    host, _, port = ENDPOINT.rpartition(":")
    with contextlib.suppress(Exception):
        async with aetcd.Client(host=host or "127.0.0.1", port=int(port)) as c:
            for name in names:
                await c.delete(f"{etcdleader.DEFAULT_PREFIX}{name}".encode())


async def _await_condition(predicate, timeout: float = 15.0, tick: float = 0.05) -> bool:
    """轮询等待条件成立,超时返回 False。

    ★ 为什么不用固定 sleep(2026-08-18 全量跑间歇性变红后改):
        `await asyncio.sleep(2)` 之后直接断言"应该当选了"是 §16.10 点名的
        「用定时器掩盖时序」—— 到期后**假设成功**。机器一忙(全量跑时并行着
        MySQL/Redis 用例)竞选还没完成,测试就红,而代码完全正确。
        改成轮询 + deadline 后,到期是**重查**而不是假设,与时间快慢无关。
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(tick)
    return False


async def _run_for(el: etcdleader.LeaderElection, task, seconds: float) -> asyncio.Task:
    """把 el.run 跑成后台 task,跑 seconds 秒后取消。"""
    handle = asyncio.create_task(el.run(task))
    await asyncio.sleep(seconds)
    handle.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await handle
    return handle


async def test_single_leader_among_three_replicas(clean_key) -> None:
    """★ 三个副本同时竞选,**任意时刻**只能有一个在跑 leader 任务。

    这是本模块存在的唯一理由。若不成立,撮合循环会在同一队列上重复成局,
    同一玩家进两场 match(违反不变量 §1)。
    """
    election = clean_key("single")
    concurrent_peak = 0
    running = 0
    lock = asyncio.Lock()

    async def leader_task() -> None:
        nonlocal running, concurrent_peak
        async with lock:
            running += 1
            concurrent_peak = max(concurrent_peak, running)
        try:
            await asyncio.sleep(3600)  # 一直跑,直到被取消
        finally:
            async with lock:
                running -= 1

    replicas = [
        etcdleader.LeaderElection([ENDPOINT], election, lease_ttl_sec=3) for _ in range(3)
    ]
    handles = [asyncio.create_task(r.run(leader_task)) for r in replicas]
    await asyncio.sleep(4)

    leaders_now = sum(1 for r in replicas if r.is_leader)

    for h in handles:
        h.cancel()
    for h in handles:
        with contextlib.suppress(asyncio.CancelledError):
            await h

    assert concurrent_peak == 1, (
        f"同时有 {concurrent_peak} 个副本在跑 leader 任务 —— 会重复成局(不变量 §1)"
    )
    assert leaders_now == 1, f"is_leader 为真的副本数={leaders_now}(应为 1)"


async def test_acquire_uses_txn_not_read_then_write(clean_key) -> None:
    """★ 抢锁必须是原子 txn,不能是"先读再写"。

    用高并发暴露 TOCTOU:20 个副本同时首次竞选,只能有一个当选。
    "先 get 看有没有,没有就 put"在这个用例下会出现多个当选者。
    """
    election = clean_key("txn")
    started = 0

    async def leader_task() -> None:
        nonlocal started
        started += 1
        await asyncio.sleep(3600)

    replicas = [
        etcdleader.LeaderElection([ENDPOINT], election, lease_ttl_sec=5) for _ in range(20)
    ]
    handles = [asyncio.create_task(r.run(leader_task)) for r in replicas]
    await asyncio.sleep(2.5)
    for h in handles:
        h.cancel()
    for h in handles:
        with contextlib.suppress(asyncio.CancelledError):
            await h

    assert started == 1, f"20 并发首次竞选有 {started} 个当选 —— 抢锁存在 TOCTOU"


async def test_leadership_transfers_after_leader_exits(clean_key) -> None:
    """★ 前任下线后,后继必须能接管(不能永久空位)。"""
    election = clean_key("handover")
    terms: list[str] = []

    def make_task(tag: str):
        async def task() -> None:
            terms.append(tag)
            await asyncio.sleep(3600)

        return task

    first = etcdleader.LeaderElection([ENDPOINT], election, lease_ttl_sec=3)
    h1 = asyncio.create_task(first.run(make_task("A")))
    await asyncio.sleep(2)
    assert first.is_leader, "第一个副本没当选"

    # A 下线(主动 revoke 让位)
    h1.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await h1

    second = etcdleader.LeaderElection([ENDPOINT], election, lease_ttl_sec=3)
    h2 = asyncio.create_task(second.run(make_task("B")))
    await asyncio.sleep(3)
    took_over = second.is_leader
    h2.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await h2

    assert terms == ["A", "B"], f"任期序列={terms}(应为 A 然后 B)"
    assert took_over, "后继没能接管 —— leader 位永久空缺"


async def test_losing_leadership_does_not_kill_process(clean_key) -> None:
    """★ 失去领导权**不能**退出进程,只能停 leader 任务。

    这是与 snowflake 的关键区别(snowflake 失租必须 os.Exit,本模块不能)。
    若这里退出进程,滚动更新时 leader 转移会连带杀掉一个还在正常服务 RPC 的副本。
    验证方式:leader 任务自己抛异常结束后,run() 仍在跑并会重新竞选。
    """
    election = clean_key("survive")
    calls = 0

    async def flaky_task() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("leader 任务自己炸了")

    el = etcdleader.LeaderElection([ENDPOINT], election, lease_ttl_sec=3)
    handle = asyncio.create_task(el.run(flaky_task))
    await asyncio.sleep(6)  # 足够跑 2 个以上任期
    alive = not handle.done()
    handle.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await handle

    assert alive, "leader 任务抛异常后 run() 自己退出了 —— 副本会失去 RPC 服务能力"
    assert calls >= 2, f"只当选了 {calls} 次 —— 失主后没有重新竞选"


async def test_keepalive_holds_leadership_beyond_ttl(clean_key) -> None:
    """★ 续约必须真的能让领导权活过原始 TTL。

    aetcd 没有自动 keepalive,续约完全靠本模块的循环。若循环写错(间隔太长、
    只在成功时才 sleep、把失败当成功),领导权会在 TTL 后被别人抢走,
    而本副本**仍以为自己是 leader** —— 双 leader。
    """
    election = clean_key("keepalive")
    ttl = 3

    async def task() -> None:
        await asyncio.sleep(3600)

    el = etcdleader.LeaderElection([ENDPOINT], election, lease_ttl_sec=ttl)
    handle = asyncio.create_task(el.run(task))
    await asyncio.sleep(ttl * 3)  # 远超原始 TTL

    still_leader = el.is_leader
    lost = el.lost_leadership_count
    terms = el.term_count

    handle.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await handle

    assert still_leader, f"跑过 {ttl*3}s(TTL={ttl}s)后失去了领导权 —— 续约循环无效"
    assert lost == 0, f"期间失主 {lost} 次 —— 续约不稳,存在双 leader 窗口"
    assert terms == 1, f"任期数={terms}(应为 1,说明没有中断过)"


async def test_election_key_layout_matches_go_concurrency_election(clean_key) -> None:
    """★ 选举 key 结构必须与 Go 的 concurrency.Election 一致。

    Go(client/v3/concurrency/election.go):
        keyPrefix = <electionKey> + "/"
        Campaign  → key = fmt.Sprintf("%s%x", keyPrefix, session.Lease())
        leader    = 该前缀下 CreateRevision 最小的候选者

    若 Python 用别的 key 结构(例如单个 <electionKey> 用 version==0 抢),
    两个实现会**各选各的 leader** —— Go 栈和 Python 栈同时跑撮合循环,
    同一玩家进两场 match(违反不变量 §1),而且不报错。
    维护两个分支时这是必然踩到的。
    """
    import aetcd

    election = clean_key("keylayout")
    el = etcdleader.LeaderElection([ENDPOINT], election, lease_ttl_sec=5)

    async def task() -> None:
        await asyncio.sleep(3600)

    handle = asyncio.create_task(el.run(task))
    await asyncio.sleep(2)
    assert el.is_leader, "没当选,无法检查 key 结构"

    host, _, port = ENDPOINT.rpartition(":")
    async with aetcd.Client(host=host or "127.0.0.1", port=int(port)) as c:
        prefix = f"{etcdleader.DEFAULT_PREFIX}{election}/".encode()
        entries = list(await c.get_prefix(prefix))

    handle.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await handle

    assert len(entries) == 1, f"前缀下应恰好一个候选者 key,实际 {len(entries)}"
    key = entries[0].key.decode()
    assert key.startswith(f"{etcdleader.DEFAULT_PREFIX}{election}/"), key
    suffix = key.rsplit("/", 1)[-1]
    # 后缀必须是 lease id 的**十六进制**(Go 用 %x)。十进制会让两边 key 不同。
    int(suffix, 16)
    assert suffix == f"{entries[0].lease:x}", (
        f"key 后缀 {suffix!r} 不是 lease id 的十六进制({entries[0].lease:x}) —— 与 Go 不一致"
    )


async def test_leader_is_lowest_create_revision(clean_key) -> None:
    """★ leader 判定规则 = 前缀下 CreateRevision 最小(队列语义,先到先得)。

    这条规则是跨语言互斥成立的根据:Go 和 Python 用同一条规则看同一批 key,
    才能保证"全局只有一个 leader"。
    """
    election = clean_key("lowestrev")
    started: list[int] = []

    def make(tag: int):
        async def task() -> None:
            started.append(tag)
            await asyncio.sleep(3600)

        return task

    first = etcdleader.LeaderElection([ENDPOINT], election, lease_ttl_sec=6)
    h1 = asyncio.create_task(first.run(make(1)))
    # 等 1 号真的当选(而不是"睡 1.5 秒然后假设它选上了")
    got_first = await _await_condition(lambda: first.is_leader)

    second = etcdleader.LeaderElection([ENDPOINT], election, lease_ttl_sec=6)
    h2 = asyncio.create_task(second.run(make(2)))
    # 2 号必须**抢不到**;这是个否定断言,只能给它足够时间去尝试。
    # 用「等到它至少完整跑过一轮竞选」作为判据:退避一轮 + 余量。
    await asyncio.sleep(etcdleader.RECONNECT_BACKOFF_SEC + 1.0)

    leader_flags = (first.is_leader, second.is_leader)

    for h in (h1, h2):
        h.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await h

    assert got_first, "1 号在 15s 内没能当选 —— 不是时序问题,是选主坏了"
    assert leader_flags == (True, False), (
        f"先到的应当是 leader,实际 first={leader_flags[0]} second={leader_flags[1]}"
    )
    assert started == [1], f"只有 leader 该跑任务,实际跑了 {started}"


async def test_waiting_candidate_keeps_queue_position(clean_key) -> None:
    """★ 排队中的候选者必须**保住自己的队列位置**,leader 让位后按先后顺序接管。

    这条测的是一个真实缺陷的修复(2026-08-18):
        最初的实现在"没排到队首"时会 revoke 掉自己的 lease、退避后重新建 key,
        于是每一轮都拿到一个更大的 CreateRevision —— 等于**每次都重新排到队尾**。
        互斥仍然成立(只有队首当选),但公平性没有:一个副本可能永远排不上,
        而且表现是"服务一直活着但那个副本从来不干活",没有任何报错。

    验证:A B C 依次入队 → A 当选 → A 下线 → 接管的必须是 **B**(不是 C)。
    """
    election = clean_key("fifo")
    order: list[str] = []

    def make(tag: str):
        async def task() -> None:
            order.append(tag)
            await asyncio.sleep(3600)

        return task

    # 时序判据全部用「等条件 + deadline」,不用固定 sleep(见 _await_condition 注释)。
    #
    # ★ TTL 取 20 而不是 3(2026-08-19,全量跑约 1/4 概率变红后改):
    #   本条测的是**队列顺序**,不是租约时长。TTL=3 时,全量跑里前面的
    #   MySQL / Redis 用例把事件循环压住,A 的续约会赶不上 3s 窗口 ——
    #   A 在被 cancel 之前先自己丢了 leadership(日志判据:leader_lost lost_count=1),
    #   于是接管顺序被一个与本条无关的失败模式打乱。
    #   抬 TTL **不会**削弱断言:A 退出走的是 shutdown 主动撤销 lease,
    #   接管速度与 TTL 无关;租约本身的失效行为由 test_revoked_lease_stops_the_writer
    #   专门覆盖,不该在这里顺带测。
    ttl = 20
    a = etcdleader.LeaderElection([ENDPOINT], election, lease_ttl_sec=ttl)
    ha = asyncio.create_task(a.run(make("A")))
    assert await _await_condition(lambda: a.is_leader), "A 没能当选"

    # B 入队:必须等它**真的建好了自己的 key**(而不是睡一会儿假设建好了),
    # 否则 C 可能抢在 B 前面拿到更小的 CreateRevision,队列顺序就不是 A→B→C 了。
    b = etcdleader.LeaderElection([ENDPOINT], election, lease_ttl_sec=ttl)
    hb = asyncio.create_task(b.run(make("B")))
    assert await _await_condition(lambda: b.queued), "B 没能入队"

    c = etcdleader.LeaderElection([ENDPOINT], election, lease_ttl_sec=ttl)
    hc = asyncio.create_task(c.run(make("C")))
    assert await _await_condition(lambda: c.queued), "C 没能入队"

    # A 必须始终是 leader 才能证明后面的接管是"让位"而不是"A 自己掉了"。
    assert a.is_leader, "A 在建队期间就丢了 leadership —— 环境问题,不是队列语义问题"

    assert (a.is_leader, b.is_leader, c.is_leader) == (True, False, False), (
        f"入队最早的应当当选:A={a.is_leader} B={b.is_leader} C={c.is_leader}"
    )

    # A 下线,B 应当接管(它排在 C 前面)
    ha.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await ha
    # 等 B 接管;超时才算失败(而不是"睡 3 秒后假设接管完成")
    took_over_ok = await _await_condition(lambda: b.is_leader, timeout=20.0)

    took_over = (b.is_leader, c.is_leader)
    for h in (hb, hc):
        h.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await h

    assert took_over_ok, "B 在 20s 内没接管 —— 不是时序问题,是队列语义坏了"
    assert took_over == (True, False), (
        f"应由排队更早的 B 接管,实际 B={took_over[0]} C={took_over[1]} —— "
        f"队列位置没保住(候选者被重新排到队尾了?)"
    )
    assert order == ["A", "B"], f"任期顺序={order}(应为 A 然后 B)"


async def test_queue_position_survives_multiple_poll_cycles(clean_key) -> None:
    """排队者在多个轮询周期后仍保持同一个 key(CreateRevision 不变)。

    如果实现每轮重建 key,CreateRevision 会一直变大 —— 这条直接盯住那个行为。

    ⚠️ TTL 取 6 而不是 3(2026-08-19 实测调整):TTL=3 时续约超时只有 1s、
    本地安全窗只有 2s,机器一被压满(本机曾同时跑两套全量测试 + 两个服务进程)
    就会**真的**失租,于是这条用例偶发红在 `leader_lease_keepalive_deadline_exceeded`
    上 —— 那是环境噪音,不是被测行为出问题,而这种红最容易被当成"又是这个 flaky 用例"
    从而掩盖真回归。被测的不变量(key 与 CreateRevision 跨轮询周期不变)与 TTL 大小无关,
    所以把余量放宽是纯收益。观察窗仍取 TTL/3 的两倍以上,保证真跨过多个轮询周期。
    """
    import aetcd

    election = clean_key("stable")
    ttl = 6

    async def task() -> None:
        await asyncio.sleep(3600)

    leader = etcdleader.LeaderElection([ENDPOINT], election, lease_ttl_sec=ttl)
    hl = asyncio.create_task(leader.run(task))
    await asyncio.sleep(1.5)

    waiter = etcdleader.LeaderElection([ENDPOINT], election, lease_ttl_sec=ttl)
    hw = asyncio.create_task(waiter.run(task))
    await asyncio.sleep(1.5)

    host, _, port = ENDPOINT.rpartition(":")
    prefix = f"{etcdleader.DEFAULT_PREFIX}{election}/".encode()

    async def snapshot() -> dict[bytes, int]:
        async with aetcd.Client(host=host or "127.0.0.1", port=int(port)) as c:
            return {kv.key: kv.create_revision for kv in await c.get_prefix(prefix)}

    first = await snapshot()
    await asyncio.sleep(5)  # 跨过多个轮询周期(poll = TTL/3 = 2s)
    second = await snapshot()

    for h in (hl, hw):
        h.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await h

    assert len(first) == 2, f"应有 2 个候选者 key,实际 {len(first)}"
    assert first == second, (
        f"候选者 key 或其 CreateRevision 变了 —— 排队位置没保住。\n"
        f"before={first}\nafter={second}"
    )


async def test_queued_replica_requeues_when_its_key_disappears(clean_key) -> None:
    """★ 排队副本的候选者 key 消失后,必须**重新入队**,不能永远等下去。

    形状:B 在排队(A 是 leader)。此时 B 的 key 因为失租 / 被手工删而消失。

    坏实现里 `_wait_for_turn` 只判"我是不是队首" —— key 没了永远不是队首,于是
    它每个轮询周期查一次、**永不返回**:`_one_term` 不返回 → `run()` 的重试循环
    走不到 → `_enqueue` 再也不会被调用。这个副本从此退出选举,而它自己以为还在排队,
    而且**全程零日志**(leader_lost 只在当选过之后才打)。
    A 随后被滚更下线时,就没人接管了。

    判据取「出现了一个 CreateRevision 更大的新候选者 key」= 它确实重新入过队,
    而不是「B 最终当上了 leader」—— 后者在坏实现里也可能因为别的路径碰巧发生。
    """
    import aetcd

    election = clean_key("requeue")
    host, _, port = ENDPOINT.rpartition(":")
    prefix = f"{etcdleader.DEFAULT_PREFIX}{election}/".encode()

    async def snapshot() -> dict[bytes, int]:
        async with aetcd.Client(host=host or "127.0.0.1", port=int(port)) as c:
            return {kv.key: kv.create_revision for kv in await c.get_prefix(prefix)}

    async def await_async(pred, timeout: float = 30.0, tick: float = 0.2) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if await pred():
                return True
            await asyncio.sleep(tick)
        return False

    async def task() -> None:
        await asyncio.sleep(3600)

    # ★ 时序必须确定:先让 A 当选(它的 key 一定是队首),再放 B 进来排队。
    # 两个一起启动的话谁先入队是竞态,偶发会红在"A 没当选"上。
    leader = etcdleader.LeaderElection([ENDPOINT], election, lease_ttl_sec=6)
    hl = asyncio.create_task(leader.run(task))
    hw = None
    try:
        assert await _await_condition(lambda: leader.is_leader), "A 没当选"

        waiter = etcdleader.LeaderElection([ENDPOINT], election, lease_ttl_sec=6)
        hw = asyncio.create_task(waiter.run(task))

        async def two_keys() -> bool:
            return len(await snapshot()) == 2

        assert await await_async(two_keys), "B 没入队"

        before = await snapshot()
        leader_key = min(before, key=lambda k: before[k])
        waiter_key = next(k for k in before if k != leader_key)
        old_rev = before[waiter_key]

        # 把排队者的 key 从它脚下删掉 —— 等价于「排队期间悄悄掉出了队列」。
        async with aetcd.Client(host=host or "127.0.0.1", port=int(port)) as c:
            await c.delete(waiter_key)

        async def requeued() -> bool:
            now = await snapshot()
            return any(k != leader_key and rev > old_rev for k, rev in now.items())

        assert await await_async(requeued, timeout=30), (
            "排队副本的 key 被删后没有重新入队 —— 它多半永远挂在等队首上了"
        )
    finally:
        for h in (hl, hw):
            if h is None:
                continue
            h.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await h
