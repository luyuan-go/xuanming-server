"""单写者租约故障注入 —— 打真实 etcd。

这是落 allocator 代码前的最后一道前置验证。三个场景都是**静默故障**:

  1. ★ 激活钩子阻塞 → 若无期限,占着 leader key 让全集群无写者,且计数不动
  2. ★ 本地安全截止线 → 越线后必须立刻停止对外宣告可写(即使续约循环还没反应过来)
  3. ★ 两组健康度计数分开 —— 合成一个的话运维不知道该查 etcd 还是查激活钩子

没有 etcd 就整体 skip(不假装通过)。
"""

from __future__ import annotations

import asyncio
import contextlib
import time

import pytest

import etcdfixture as efixture
from etcdfixture import ENDPOINT

from pandorapy import writerlease as wl

@pytest.fixture(scope="module", autouse=True)
async def require_etcd() -> None:
    # 判据见 tests/etcdfixture.py(原先 `except Exception: return False` + 无异常文案)。
    await efixture.require_etcd("writerlease 故障注入")


@pytest.fixture
def lease_name():
    """每个用例独立的租约名。"""
    return lambda tag: f"test/{tag}/{time.monotonic_ns()}"


async def _await(pred, timeout: float = 20.0, tick: float = 0.05) -> bool:
    """等条件成立(不是"睡一会儿然后假设") —— 见 test_etcdleader 的同款说明。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        await asyncio.sleep(tick)
    return False


async def _stop(*tasks) -> None:
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await t


# ── ★ 单写者 ──────────────────────────────────────────────────────────────


async def test_only_one_writer_holds(lease_name) -> None:
    """★ 三个副本竞争,任意时刻只有一个对外宣告可写。"""
    name = lease_name("single")

    async def noop_activate(term: int) -> None:
        return None

    replicas = [wl.WriterLease([ENDPOINT], name, lease_ttl_sec=6) for _ in range(3)]
    tasks = [asyncio.create_task(r.run(noop_activate)) for r in replicas]
    try:
        assert await _await(lambda: any(r.current()[0] for r in replicas)), "没人当选"
        # 观察一段时间,期间任意时刻都只能有一个
        for _ in range(30):
            holders = sum(1 for r in replicas if r.current()[0])
            assert holders <= 1, f"同时有 {holders} 个写者"
            await asyncio.sleep(0.1)
        assert sum(1 for r in replicas if r.current()[0]) == 1
    finally:
        await _stop(*tasks)


# ── ★ 激活钩子阻塞(最危险的静默故障)──────────────────────────────────────


async def test_blocked_activation_does_not_starve_the_cluster(lease_name) -> None:
    """★ 这是本文件存在的首要理由。

    激活期间副本**已当选并占着 etcd leader key**,却还没对外宣告持有。
    钩子若永久阻塞而**没有期限**:
        ① 本副本永远不可写
        ② 它占着 key 不让位,其它副本全排队 → **整个集群无写者**
        ③ 失败计数一次都不加(计数只在 err 分支)→ **完全静默**

    加了期限后:阻塞转成超时 → 让位 → 退避重选 → 另一个副本能接上。
    """
    name = lease_name("blocked")
    blocked_started = asyncio.Event()

    async def blocking_activate(term: int) -> None:
        blocked_started.set()
        await asyncio.sleep(3600)  # 永久阻塞

    async def good_activate(term: int) -> None:
        return None

    # 先让 A 占位并卡在激活里(激活超时设短便于测)
    a = wl.WriterLease([ENDPOINT], name, lease_ttl_sec=6, activation_timeout_sec=3)
    ta = asyncio.create_task(a.run(blocking_activate))
    try:
        assert await _await(blocked_started.is_set), "A 没进入激活"
        assert not a.current()[0], "激活还没完成就宣告持有了"

        # B 加入。若 A 永久占着 key,B 永远起不来。
        b = wl.WriterLease([ENDPOINT], name, lease_ttl_sec=6, activation_timeout_sec=3)
        tb = asyncio.create_task(b.run(good_activate))
        try:
            took_over = await _await(lambda: b.current()[0], timeout=25)
            assert took_over, (
                "B 一直没能接管 —— A 的激活钩子把 leader key 占死了,"
                "整个集群无写者(这正是激活超时要防的形状)"
            )
            # A 的激活失败计数必须动起来 —— 否则降级完全不可观测
            assert a.health().consecutive_activation_errs > 0, (
                "激活反复超时却没有任何计数 —— 长期无主完全静默"
            )
        finally:
            await _stop(tb)
    finally:
        await _stop(ta)


async def test_activation_error_counter_is_separate_from_campaign(lease_name) -> None:
    """★ 两组计数分开 —— 合成一个的话运维不知道该查 etcd 还是查激活钩子。"""
    name = lease_name("counters")

    async def failing_activate(term: int) -> None:
        raise RuntimeError("激活钩子自己炸了")

    r = wl.WriterLease([ENDPOINT], name, lease_ttl_sec=5, activation_timeout_sec=3)
    t = asyncio.create_task(r.run(failing_activate))
    try:
        assert await _await(lambda: r.health().consecutive_activation_errs >= 2, timeout=25)
        h = r.health()
        assert h.consecutive_activation_errs >= 2
        assert h.consecutive_campaign_errs == 0, (
            "激活失败被计到了竞选计数上 —— 两类故障混在一起了"
        )
        assert "激活钩子自己炸了" in h.last_activation_err
    finally:
        await _stop(t)


# ── ★ 本地安全截止线 ──────────────────────────────────────────────────────


def test_local_hold_window_is_shorter_than_lease_ttl() -> None:
    """★ 本地窗口必须**严格短于**服务端 lease TTL。

    etcd 侧续租是周期性的,本地不可能精确知道服务端何时判定过期;
    宁可自己先停手,也不要在服务端已把任期交给别人之后还认为自己持有。
    """
    r = wl.WriterLease([ENDPOINT], "x", lease_ttl_sec=15)
    assert r.local_hold_window_sec() == 12
    assert r.local_hold_window_sec() < 15


def test_current_expires_by_time_without_any_callback() -> None:
    """★ `current()` **自带时间过期** —— 不依赖续约循环来标记失主。

    这条很关键:进程被 SIGSTOP / GIL 卡住 / event loop 饿死时,
    续约循环根本跑不到,但 current() 仍然必须在越线后返回不持有。
    """
    r = wl.WriterLease([ENDPOINT], "x", lease_ttl_sec=15)
    # 手工装一个已经越线的持有态(模拟"续约循环没来得及跑")
    hold = wl._HoldState(token=42, valid_until=time.monotonic() - 0.001)  # noqa: SLF001
    r._hold = hold  # noqa: SLF001
    r._term = 42  # noqa: SLF001
    held, token = r.current()
    assert not held, "越过本地安全截止线仍宣告持有 —— 可能与新写者并存"
    assert token == 0


def test_expired_term_never_comes_back_to_life() -> None:
    """★ 越线是**单调终态**:同一任期不能被一次迟到的续约"续活"。

    坏实现里 `current()` 只做时间比较,于是一次成功的迟到续约把截止线往后一推,
    同一个 token 又变成"持有" —— 上层会看到「持有 → 不持有 → 又持有」这种
    自相矛盾的序列,而 fence 水位那边早已把任期交给别人了。
    Go 侧把它做成 holdState.selfFenced:任一处观察到越线,该 token 永久出局。
    """
    r = wl.WriterLease([ENDPOINT], "x", lease_ttl_sec=15)
    hold = wl._HoldState(token=42, valid_until=time.monotonic() - 0.001)  # noqa: SLF001
    r._hold = hold  # noqa: SLF001
    r._term = 42  # noqa: SLF001

    assert r.current() == (False, 0)          # 观察到越线
    assert hold.self_fenced, "观察到越线必须置单调终态"

    # 迟到的续约证据(remaining 充足)也不得复活本届
    assert not wl._apply_ttl_proof(hold, 15.0, time.monotonic())  # noqa: SLF001
    assert r.current() == (False, 0), "已自 fencing 的任期被一次迟到的续约续活了"


def test_ttl_proof_rejects_window_below_safety_margin() -> None:
    """剩余租期不足安全余量时不得开窗 —— 宁可让位重选。

    这是"激活吃掉了租期"那种场景:激活返回时服务端只剩 2s,而安全余量是 3s,
    此刻宣告持有等于本地截止线**晚于**服务端 lease 真正过期(模块头 ① 的反面)。
    """
    hold = wl._HoldState(token=7)  # noqa: SLF001
    assert not wl._apply_ttl_proof(hold, float(wl.HOLD_SAFETY_MARGIN_SEC), time.monotonic())  # noqa: SLF001
    assert hold.self_fenced

    ok_hold = wl._HoldState(token=8)  # noqa: SLF001
    assert wl._apply_ttl_proof(ok_hold, wl.HOLD_SAFETY_MARGIN_SEC + 5.0, time.monotonic())  # noqa: SLF001
    assert not ok_hold.self_fenced
    assert ok_hold.valid_until > time.monotonic()


def test_current_is_false_before_any_election() -> None:
    r = wl.WriterLease([ENDPOINT], "x")
    assert r.current() == (False, 0)


# ── ★ 健康度可观测 ────────────────────────────────────────────────────────


def test_degraded_triggers_on_either_counter() -> None:
    """任一组计数达阈值即降级 —— 长期无主必须可告警。"""
    h = wl.HealthSnapshot(escalate_after=15)
    assert not h.degraded()
    h.consecutive_campaign_errs = 15
    assert h.degraded()

    h2 = wl.HealthSnapshot(escalate_after=15)
    h2.consecutive_activation_errs = 15
    assert h2.degraded()


def test_escalate_threshold_matches_go() -> None:
    assert wl.CAMPAIGN_ESCALATE_AFTER == 15
    assert wl.RECAMPAIGN_BACKOFF_SEC == 2
    # 15 × 2s = 30s > lease TTL 15s(一次正常接任不会告警)
    assert wl.CAMPAIGN_ESCALATE_AFTER * wl.RECAMPAIGN_BACKOFF_SEC > wl.DEFAULT_LEASE_TTL_SEC


def test_constants_match_go() -> None:
    assert wl.DEFAULT_LEASE_TTL_SEC == 15
    assert wl.HOLD_SAFETY_MARGIN_SEC == 3
    assert wl.DEFAULT_ACTIVATION_TIMEOUT_SEC == 30


# ── 接管 ────────────────────────────────────────────────────────────────────


async def test_successor_takes_over_after_holder_exits(lease_name) -> None:
    """持有者下线后后继必须接管(正常滚动更新路径)。"""
    name = lease_name("handover")

    async def noop(term: int) -> None:
        return None

    a = wl.WriterLease([ENDPOINT], name, lease_ttl_sec=5)
    ta = asyncio.create_task(a.run(noop))
    assert await _await(lambda: a.current()[0]), "A 没当选"
    await _stop(ta)
    assert not a.current()[0], "下线后仍宣告持有"

    b = wl.WriterLease([ENDPOINT], name, lease_ttl_sec=5)
    tb = asyncio.create_task(b.run(noop))
    try:
        assert await _await(lambda: b.current()[0], timeout=20), "B 没接管"
    finally:
        await _stop(tb)


# ── ★ 租约被吊销:refresh 不抛异常的静默失主 ─────────────────────────────────


async def test_revoked_lease_stops_the_writer(lease_name) -> None:
    """★ 租约在脚下被吊销后,必须立刻停止以该任期宣告可写。

    这条防的是 aetcd 的一个**静默语义**(本机真 etcd v3.5.17 实测):

        对一个**已经不存在**的 lease 调 `refresh()`
        → 不抛异常,正常返回 LeaseKeepAliveResponse(TTL=0)

    所以「try: await refresh() / except: 算失败」的续约循环有一个永久的洞:
    租约早没了、leader key 早被别人抢走,而本副本因为"没抛异常"一直把本地
    安全截止线往后推 —— 它会**永远**以同一个任期宣告自己可写。

    判据刻意取「任期必须变化」而不是「某一刻不持有」:
      - 正确实现:检测到 TTL<=0 → 让位 → 重新竞选 → 拿到**新的**任期
      - 坏实现:一直"续约成功",任期恒等于那个已被吊销的 lease id
    超时给到 25s(≫ 一个 5s TTL),所以救场的不可能是 lease 自然过期 ——
    坏实现里那条 lease 早就不存在了,却照样每 TTL/3 "续约成功"一次。
    """
    name = lease_name("revoked")

    async def noop(term: int) -> None:
        return None

    r = wl.WriterLease([ENDPOINT], name, lease_ttl_sec=5)
    task = asyncio.create_task(r.run(noop))
    try:
        assert await _await(lambda: r.current()[0]), "没当选"
        revoked_term = r.current()[1]
        assert revoked_term != 0

        # 从外部把它的 lease 抽掉 —— 等价于「服务端已把任期交给别人」。
        # ★ 任期 token 是 leader key 的 CreateRevision,**不是** lease id
        #   (那正是本模块修过的一个 parity 缺陷),所以 lease 要从 key 上现查。
        import aetcd

        host, _, port = ENDPOINT.rpartition(":")
        async with aetcd.Client(host=host or "127.0.0.1", port=int(port)) as c:
            entries = await c.get_prefix(f"/pandora/writerlease/{name}/".encode())
            leases = [kv.lease for kv in entries if kv.lease]
            assert len(leases) == 1, f"前缀下应只有一个候选者 key,实际 {len(leases)}"
            await c.revoke_lease(leases[0])

        moved = await _await(
            lambda: r.current()[1] != revoked_term or not r.current()[0], timeout=25
        )
        assert moved, (
            f"lease {revoked_term:x} 已被吊销,副本仍以同一任期宣告可写 —— "
            f"续约循环把 TTL=0 的应答当成了续约成功"
        )
    finally:
        await _stop(task)


# ── ★ 任期 token 的编码前提 ─────────────────────────────────────────────────


async def test_term_token_is_usable_as_a_source_revision_term(lease_name) -> None:
    """★ 任期 token 必须能真的当 source_revision 的写者任期用。

    这条把 writerlease 与 source_revision 之间那个**隐式**前提机械钉死:
    source_revision 用高 40 位装任期号,而 etcd 的 lease id 是 57 bit ——
    拿 lease.id 当 token 时 `Minter().next(token)` 会直接 fail-closed,
    hub_allocator 一个来源版本号都铸不出来,而这在竞选/持有阶段毫无征兆。

    正确的 token 是本届 leader key 的 CreateRevision(etcd revision 从 1 起、
    全局单调不回退),既在 40 位内,又满足"历届严格递增"的全序前提。
    """
    from pandorapy import source_revision as sr

    name = lease_name("token")

    async def noop(term: int) -> None:
        return None

    r = wl.WriterLease([ENDPOINT], name, lease_ttl_sec=6)
    task = asyncio.create_task(r.run(noop))
    try:
        assert await _await(lambda: r.current()[0]), "没当选"
        _, term = r.current()
        assert 0 < term <= sr.MAX_TERM, (
            f"token={term} 超出 source_revision 的 40 位任期编码上限 {sr.MAX_TERM}"
        )
        # 真的铸一个号出来 —— 光比大小不够,要证明整条链能走通。
        rev = sr.Minter().next(term)
        assert sr.split(rev) == (term, 1)
    finally:
        await _stop(task)


# ── ★ 续约必须从当选就开始(不是激活完成之后)────────────────────────────────


async def test_lease_survives_an_activation_longer_than_the_ttl(lease_name) -> None:
    """★ 激活耗时超过一个 lease TTL 时,租约必须仍然活着。

    writerlease 在激活成功后**直接**宣告持有,不再复核自己是否仍是队首 ——
    所以"续约从当选就开始跑"是该模块防两个写者的**唯一**防线。

    把 keepalive 挪到激活之后是一次**全绿的回归**:默认 activation_timeout(30s)
    大于 lease TTL(15s),任何耗时介于两者之间的激活都会让本副本的 lease 在激活
    途中过期、后继当选,而本副本激活返回后照旧宣告持有 → 两个写者同时推 fence 水位。
    现有用例一条都没让激活耗时超过 TTL,所以那个回归抓不到。

    这里 TTL=8s、激活 10s,判据是**服务端事实**:激活返回后自己的 leader key
    必须还在 etcd 里,且 CreateRevision 仍等于任期 token。

    TTL 取 8 而不是更小:宣告持有前要向服务端要一次 TTL 证明,而安全余量是
    HOLD_SAFETY_MARGIN_SEC(3s)—— TTL 太小的话可证明窗口本来就是负的,
    用例会红在"剩余租期不足安全余量"上,那是配置不合理,不是被测行为出问题。
    """
    import aetcd

    name = lease_name("slowact")
    activating = asyncio.Event()

    async def slow_activate(term: int) -> None:
        activating.set()
        await asyncio.sleep(10)

    r = wl.WriterLease([ENDPOINT], name, lease_ttl_sec=8, activation_timeout_sec=30)
    task = asyncio.create_task(r.run(slow_activate))
    try:
        assert await _await(activating.is_set, timeout=15), "激活钩子没被调用"
        # 激活期间不得对外宣告持有(激活没完成就不算可写)
        for _ in range(10):
            assert not r.current()[0], "激活尚未完成就宣告了可写"
            await asyncio.sleep(0.2)

        assert await _await(lambda: r.current()[0], timeout=25), (
            "激活完成后没有宣告持有 —— 多半是租约在激活途中就过期了"
        )
        _, term = r.current()

        host, _, port = ENDPOINT.rpartition(":")
        async with aetcd.Client(host=host or "127.0.0.1", port=int(port)) as c:
            entries = await c.get_prefix(f"/pandora/writerlease/{name}/".encode())
            revs = [kv.create_revision for kv in entries]
        assert term in revs, (
            f"激活返回后 leader key 已不在 etcd(term={term}, 现存={revs}) —— "
            f"租约在激活期间死掉了,此刻宣告持有就是第二个写者"
        )
    finally:
        await _stop(task)


async def test_hold_window_never_exceeds_server_evidence(lease_name) -> None:
    """★ 本地安全截止线**不得晚于服务端证据允许的上限**。

    这条分辨的是「用服务端 RemainingTTL 开窗」还是「用配置里的 TTL 开窗」——
    两者在稳态下几乎一样,唯独在**激活吃掉了租期**时分叉:

        激活耗时 3s、还没轮到第一次续约(间隔 = TTL/3 = 5s)
        → 服务端此刻只剩约 12s,而配置 TTL 是 15s
        → 用配置 TTL 开窗会把本地截止线推到比服务端 lease 真正过期**更晚**,
          正好是模块头 ① 那条不等式的反面(而且没有任何运行期信号)。

    判据取服务端的现查值,不取任何本地推算 —— 本地推算正是被测对象本身。
    """
    import aetcd

    name = lease_name("window")
    activating = asyncio.Event()

    async def activate(term: int) -> None:
        activating.set()
        await asyncio.sleep(3)  # < 一个续约间隔(5s),所以期间一次 refresh 都没发生

    r = wl.WriterLease([ENDPOINT], name, lease_ttl_sec=15, activation_timeout_sec=30)
    task = asyncio.create_task(r.run(activate))
    try:
        assert await _await(activating.is_set, timeout=15), "激活钩子没被调用"
        assert await _await(lambda: r.current()[0], timeout=20), "激活完成后没宣告持有"

        hold = r._hold  # noqa: SLF001
        assert hold is not None

        host, _, port = ENDPOINT.rpartition(":")
        async with aetcd.Client(host=host or "127.0.0.1", port=int(port)) as c:
            entries = await c.get_prefix(f"/pandora/writerlease/{name}/".encode())
            leases = [kv.lease for kv in entries if kv.lease]
            assert len(leases) == 1
            observed_at = time.monotonic()
            remaining = await wl._remaining_ttl(c, leases[0])  # noqa: SLF001

        # 服务端证据允许的最晚截止线(+0.5s 容忍这次查询本身的往返)
        upper = observed_at + remaining - wl.HOLD_SAFETY_MARGIN_SEC + 0.5
        assert hold.valid_until <= upper, (
            f"本地截止线 {hold.valid_until - observed_at:.2f}s 晚于服务端证据允许的 "
            f"{remaining - wl.HOLD_SAFETY_MARGIN_SEC:.2f}s —— "
            f"开窗用的是配置 TTL 而不是服务端 RemainingTTL"
        )
    finally:
        await _stop(task)
