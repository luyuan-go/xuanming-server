"""etcd nodeID 抢占 —— 打真实 etcd。

这个模块此前**零测试覆盖**(2026-08-19 审计发现),而它承载的是 §9 不变量 11:
「Snowflake 业务 ID 全局唯一」。它错了不会报错,只会重号 —— 而重号在背包域表现为
DuplicateGuid fail-closed 卡住玩家领取,在业务域表现为两条记录抢同一个主键。

三条最要命的不变量各有一条**会红**的用例:

  1. 动态号段必须从 8 起(低位保留给 UE DS 本地发号器与 static 副本)
  2. close() **不得** revoke lease(秒级粒度下立刻释放会让新副本同秒重号)
  3. lease 被吊销必须被发现(etcd 对已失效 lease 的 refresh 不报错,只回 TTL=0)

没有 etcd 就整体 skip(不假装通过)。
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time

import pytest

import etcdfixture as efixture
from etcdfixture import ENDPOINT

from pandorapy import snowflake_etcd as sne

_host_port = efixture.host_port


@pytest.fixture(scope="module", autouse=True)
async def require_etcd() -> None:
    # 判据见 tests/etcdfixture.py(原先 `except Exception: return False` + 无异常文案)。
    await efixture.require_etcd("nodeID 抢占测试")


@pytest.fixture
def svc():
    """每个用例独立的 service 名 —— 各自一套完整 nodeID 空间,互不干扰。"""
    return lambda tag: f"pytest-{tag}-{time.monotonic_ns()}"


async def _await(pred, timeout: float = 20.0, tick: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        await asyncio.sleep(tick)
    return False


# ── ★ 动态号段起点 ──────────────────────────────────────────────────────────


async def test_first_allocated_node_id_is_the_dynamic_floor(svc) -> None:
    """★ 空空间里第一个抢到的号必须是 8,不是 0。

    0 是 UE DS 本地发号器(FMySnowflake)的机器号;1..7 是 static 模式号段。
    从 0 起扫会在 static→etcd 的滚更共存窗口里领到**旧副本正在用**的号 —— 双活重号,
    而且静态旧副本根本不写 etcd,抢占方完全看不见它。
    """
    holder = await sne.acquire([ENDPOINT], svc("floor"), lease_ttl_sec=5)
    try:
        assert holder.node_id == sne.FIRST_DYNAMIC_NODE_ID == 8, (
            f"第一个动态号是 {holder.node_id},保留号段被发出去了"
        )
    finally:
        await holder.close()


async def test_dynamic_floor_is_above_every_static_node_id() -> None:
    """★ 动态段必须整体在 static 段之上(对应 Go 的 TestFirstDynamicNodeIDIsAboveStaticSegment)。

    static 号段是 [1, FIRST_DYNAMIC_NODE_ID)。谁把 static node_id 配到 8 及以上,
    就会与动态段重叠 —— 那时必须先抬高本常量,而不是直接配。
    """
    assert sne.FIRST_DYNAMIC_NODE_ID >= 8
    # 0 号永远保留给 UE DS 本地发号器
    assert sne.FIRST_DYNAMIC_NODE_ID > 0


async def test_concurrent_acquire_never_hands_out_the_same_id(svc) -> None:
    """并发抢占必须各拿各的号(txn version==0,不是「先查再写」)。"""
    name = svc("concurrent")
    holders = await asyncio.gather(
        *[sne.acquire([ENDPOINT], name, lease_ttl_sec=6) for _ in range(5)]
    )
    try:
        ids = sorted(h.node_id for h in holders)
        assert len(set(ids)) == 5, f"重号:{ids}"
        assert ids == list(range(sne.FIRST_DYNAMIC_NODE_ID, sne.FIRST_DYNAMIC_NODE_ID + 5))
    finally:
        for h in holders:
            await h.close()


# ── ★ close() 不得 revoke ───────────────────────────────────────────────────


async def test_close_does_not_revoke_the_lease(svc) -> None:
    """★ close() 之后 key 必须**还在**(靠 lease 自然过期而不是立即释放)。

    这条是「同秒重号」的唯一防线:snowflake 是秒级粒度,如果 close 立刻 revoke,
    新副本可以在同一日历秒抢到同号并从 step 0 重新数 —— 与本进程这一秒已发出的号
    **逐位相同**。优雅滚更收尾时新进程亚秒启动即可复现。

    判据取「close 后立刻查 key 仍在」:自然过期至少要 TTL 秒,而 revoke 是即时的,
    两者在这一刻的差别是确定性的。
    """
    import aetcd

    name = svc("norevoke")
    holder = await sne.acquire([ENDPOINT], name, lease_ttl_sec=15)
    key = f"{sne.DEFAULT_PREFIX}{name}/{holder.node_id}".encode()

    host, port = _host_port()
    async with aetcd.Client(host=host, port=port) as c:
        assert await c.get(key) is not None, "抢占后 key 就不在,抢了个寂寞"
        await holder.close()
        assert await c.get(key) is not None, (
            "close() 把 lease revoke 掉了 —— nodeID 会在同一秒内被复用,导致逐位重号"
        )


# ── ★ 失租必须被发现 ────────────────────────────────────────────────────────


async def test_revoked_lease_is_detected_and_reported_lost(svc) -> None:
    """★ lease 被外部吊销后,必须置位 lost 并回调 on_lost。

    etcd 对**已不存在**的 lease 的 keepalive 应答是「不报错、TTL=0」(见
    pandorapy/etcdlease.py 头注释的实测)。只看"有没有抛异常"的续约循环会
    **永远**认为自己还独占这个 nodeID,继续发号 → 与抢到该号的新副本逐位重号。

    判据给到 20s(远大于 5s TTL):坏实现里那条 lease 早就不存在了,却照样每
    TTL/3 "续约成功"一次,永远不会置位 lost。
    """
    import aetcd

    name = svc("revoked")
    holder = await sne.acquire([ENDPOINT], name, lease_ttl_sec=5)
    called = asyncio.Event()

    async def on_lost() -> None:
        called.set()

    holder.start_keepalive(on_lost)
    try:
        host, port = _host_port()
        async with aetcd.Client(host=host, port=port) as c:
            await c.revoke_lease(holder._lease.id)  # noqa: SLF001 —— 故障注入需要真实吊销

        assert await _await(lambda: holder.lost.is_set(), timeout=20), (
            "lease 已被吊销,holder 仍认为自己独占该 nodeID —— "
            "续约循环把 TTL=0 的应答当成了续约成功"
        )
        assert await _await(called.is_set, timeout=5), "on_lost 回调没被调用"
    finally:
        with contextlib.suppress(Exception):
            await holder.close()


def _noop_on_lost(holder):  # noqa: ANN001, ANN202
    """测试用的失主处置工厂 —— 什么都不做。

    ★ 测试**绝不能**用生产那个 `exit_process_on_lost`:它会 os._exit(1),
    一次续约抖动就让整个 pytest 进程当场消失、没有任何报告
    (这正是 provide_node 的 on_lost 不给缺省值的原因)。
    """

    async def _noop() -> None:
        return None

    return _noop


# ── provide_node 选路 ───────────────────────────────────────────────────────


async def test_provide_node_static_path_does_not_touch_etcd() -> None:
    """static 档不连 etcd,直接用 yaml 的 node_id(默认档,单副本 / dev)。"""
    node, holder = await sne.provide_node([], "whatever", 3, "")
    assert holder is None
    assert node.node_id == 3

    node2, holder2 = await sne.provide_node([], "whatever", 3, "static")
    assert holder2 is None
    assert node2.node_id == 3


async def test_provide_node_rejects_unknown_source() -> None:
    """未知取值必须报错,不能静默降级成 static —— 那会让多副本部署悄悄重号。"""
    with pytest.raises(ValueError, match="node_id_source"):
        await sne.provide_node([ENDPOINT], "svc", 1, "redis")


async def test_provide_node_etcd_path_acquires(svc) -> None:
    """etcd 档必须真抢号并返回 holder(§14.2:开关打开后的分支必须是真实实现)。"""
    node, holder = await sne.provide_node(
        [ENDPOINT], svc("provide"), 999, "etcd",
        on_lost=_noop_on_lost, lease_ttl_sec=5,
    )
    assert holder is not None
    try:
        assert node.node_id == holder.node_id
        assert node.node_id != 999, "etcd 档不应该用 yaml 里的 static 号"
        assert node.node_id >= sne.FIRST_DYNAMIC_NODE_ID
    finally:
        await holder.close()


async def test_lease_ttl_is_clamped_to_the_reuse_quarantine_floor(svc) -> None:
    """★ TTL 配得过小必须被**钳回下限**,不能照单全收。

    TTL 在这里是**正确性参数**不是保活参数:close() 刻意不 revoke,所以 nodeID 的
    复用隔离期完全等于「停止续约后 lease 自然过期」≈ TTL*2/3。配成 2 秒的话隔离期
    只剩 1.3s —— 小于 snowflake 的 1s 秒粒度 + 现实时钟偏差,新副本同秒抢到同号并从
    step 0 重数,**逐位重号**。与 Go 的 minLeaseTTLSec 同值同行为。
    """
    import aetcd

    assert sne.MIN_LEASE_TTL_SEC == 5
    name = svc("clamp")
    holder = await sne.acquire([ENDPOINT], name, lease_ttl_sec=2)
    try:
        host, port = _host_port()
        async with aetcd.Client(host=host, port=port) as c:
            entries = await c.get_prefix(f"{sne.DEFAULT_PREFIX}{name}/".encode())
            leases = [kv.lease for kv in entries if kv.lease]
            assert len(leases) == 1
            info = await c.get_lease_info(leases[0], keys=False)
            granted = int(getattr(info, "grantedTTL", 0))
        assert granted >= sne.MIN_LEASE_TTL_SEC, (
            f"lease 实际按 {granted}s 授予 —— TTL 没有被钳回下限,复用隔离期被打穿"
        )
    finally:
        await holder.close()


async def test_provide_node_starts_keepalive_itself(svc) -> None:
    """★ etcd 档的续约必须由 `provide_node` **自己拉起**,不要求调用方记得调。

    忘了调 start_keepalive 不会报错:lease 到 TTL 自然过期 → 另一副本抢到同一个
    nodeID → 而本进程**毫不知情地继续用旧号发号**,两边逐位重号(§9 不变量 11)。
    "漏一行就静默重号"必须靠结构排除,不能靠纪律。

    判据取 **etcd 侧的事实**:过了一个完整 TTL 之后,key 仍在、lease 剩余 TTL 仍为正。

    ★ 原来的判据是"lost 未置位",那是**反的**:`lost` 是本地 Event,只由续约循环
    置位 —— 循环压根不跑的话它**更容易**通过。复核用变异实测过:把
    `_keepalive_loop` 开头插一个 return(续约任务起了立刻死),这条用例照样绿。
    本地状态证明不了远端事实,得去问 etcd。
    """
    import aetcd

    name = svc("autokeep")
    node, holder = await sne.provide_node(
        [ENDPOINT], name, 1, "etcd", on_lost=_noop_on_lost, lease_ttl_sec=5,
    )
    assert holder is not None
    try:
        assert node.node_id == holder.node_id
        # 睡 8s(> 5s TTL,余量 3s):没有续约的话 lease 早就过期、key 已被回收。
        await asyncio.sleep(8)
        host, port = _host_port()
        async with aetcd.Client(host=host, port=port) as c:
            kv = await c.get(f"{sne.DEFAULT_PREFIX}{name}/{holder.node_id}".encode())
            assert kv is not None, (
                "过了一个 TTL 后 key 已不在 etcd —— 续约没在跑,"
                "而调用方并没有被要求自己启动它"
            )
            info = await c.get_lease_info(kv.lease, keys=False)
            assert int(getattr(info, "TTL", -1)) > 0, "lease 已过期"
    finally:
        await holder.close()


async def test_provide_node_actually_wires_the_on_lost_factory(svc) -> None:
    """★ 传进去的 on_lost 工厂必须**真的被接上**,而不只是被校验一下。

    复核用变异实测:把 `start_keepalive(on_lost(holder))` 改成 `start_keepalive()`
    (号照抢、续约照起,只是失主处置永不执行)—— 原有用例**全绿**。
    「不传就报错」和「传了会被用上」是两件事,只测前者盖不住这一格。

    判据取端到端:从外部吊销 lease,断言我们传进去的那个回调真的被调用。
    """
    import aetcd

    fired = asyncio.Event()

    def factory(holder):  # noqa: ANN001, ANN202
        async def _on_lost() -> None:
            fired.set()

        return _on_lost

    name = svc("wired")
    _node, holder = await sne.provide_node(
        [ENDPOINT], name, 1, "etcd", on_lost=factory, lease_ttl_sec=5,
    )
    assert holder is not None
    try:
        host, port = _host_port()
        async with aetcd.Client(host=host, port=port) as c:
            await c.revoke_lease(holder._lease.id)  # noqa: SLF001
        assert await _await(fired.is_set, timeout=20), (
            "lease 被吊销了,但传进去的 on_lost 从没被调用 —— 工厂没有真的接上"
        )
    finally:
        with contextlib.suppress(Exception):
            await holder.close()


async def test_provide_node_rejects_a_non_callable_on_lost() -> None:
    """工厂契约必须是**机制**不是注释:传错类型要在抢号之前就炸。"""
    with pytest.raises(TypeError, match="on_lost"):
        await sne.provide_node([ENDPOINT], "svc", 1, "etcd", on_lost="not-callable")


async def test_provide_node_refuses_etcd_without_on_lost() -> None:
    """★ etcd 档不传 on_lost 必须在**装配期**大声报错,而不是取个缺省值。

    两个方向的缺省都不安全:
      缺省成"退出进程" → 忘了传的**测试**会在一次续约抖动时 os._exit,pytest 当场消失
      缺省成"只打日志" → 生产进程毫不知情地继续发号 = **重号**,正是本模块要防的事

    所以不给缺省。忘了传 = 启动期 ValueError,响亮且当场。
    """
    with pytest.raises(ValueError, match="on_lost"):
        await sne.provide_node([ENDPOINT], "svc", 1, "etcd")


async def test_etcd_value_carries_holder_identity(svc) -> None:
    """★ etcd value 里必须是**持有者身份**,不是号本身。

    号已经在 key 里了,value 再写一遍等于没写。运维真正要问的是
    「8 号现在被谁占着」—— 排查重号、排查"抢不到号"、排查滚更残留的旧 holder,
    靠的都是这一行。写号本身的话这些问题一个都答不了,只能翻日志猜。
    Go 侧写的是 host/pid/时间戳。
    """
    import aetcd
    import socket

    name = svc("identity")
    holder = await sne.acquire([ENDPOINT], name, lease_ttl_sec=6)
    try:
        host, port = _host_port()
        async with aetcd.Client(host=host, port=port) as c:
            kv = await c.get(f"{sne.DEFAULT_PREFIX}{name}/{holder.node_id}".encode())
        assert kv is not None
        value = kv.value.decode()
        assert socket.gethostname() in value, f"value 里没有主机名:{value!r}"
        assert str(os.getpid()) in value, f"value 里没有 pid:{value!r}"
        assert value != str(holder.node_id), "value 只写了号本身 —— 等于没写"
    finally:
        await holder.close()


# ── static 档的号段闸(对齐 Go provider.go:96-104)─────────────────────────
#
# 这道闸**不能**指望 snowflake.Node 自己的范围检查:那一条是 `0 <= node_id <= NODE_MASK`,
# 放行 0。而 0 是 **UE DS 本地发号器的保留号** —— 用它发号会与 DS 本地铸的 ID
# 逐位相同,撞进同一玩家的背包键空间。两种越界都不报错,只在数据层面表现为重号。


async def test_static_node_id_zero_is_refused() -> None:
    """★ 漏配 node_id 时 pydantic 默认值恰好是 0 —— 不拦的话"忘了配"稳定落进最危险的一格。"""
    with pytest.raises(ValueError, match=r"\[1,|保留号"):
        await sne.provide_node([], "svc", 0, "static")
    with pytest.raises(ValueError, match=r"\[1,|保留号"):
        await sne.provide_node([], "svc", 0, "")   # 空 source 也走 static


async def test_static_node_id_above_mask_is_refused() -> None:
    """超出 17bit node 段会被**静默截断**,与别的副本撞号。"""
    from pandorapy import snowflake

    with pytest.raises(ValueError, match=r"\[1,"):
        await sne.provide_node([], "svc", snowflake.NODE_MASK + 1, "static")


async def test_static_node_id_in_range_is_accepted() -> None:
    """闸不能把合法值也拦掉:下界 1 与上界 NODE_MASK 都必须放行。"""
    from pandorapy import snowflake

    for nid in (1, snowflake.NODE_MASK):
        node, holder = await sne.provide_node([], "svc", nid, "static")
        assert node is not None and holder is None


async def test_error_message_names_the_service_and_the_reserved_zero() -> None:
    """报错要能直接告诉运维是哪个服务、以及 0 为什么不能用 —— 否则只能去翻代码。"""
    with pytest.raises(ValueError) as exc:
        await sne.provide_node([], "mail", 0, "static")
    msg = str(exc.value)
    assert "mail" in msg and "UE DS" in msg
