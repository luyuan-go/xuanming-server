"""snowflake nodeID 的 etcd 抢占 —— 对应 Go 侧 pkg/snowflake/etcdnode。

与 etcdleader 的**失主语义完全相反**,这是本模块最容易写错的地方:

    etcdleader 失去领导权  → 只停 leader 任务,**进程继续服务 RPC**
    本模块 nodeID 失租     → **必须停止发号并退出进程**

    理由:etcd Lease 是 nodeID 独占权的**事实来源**。lease 一丢,另一个副本就可能
    抢到同一个 nodeID 并开始发号。两个副本用同一 nodeID 发号 = 重号(违反不变量 §9.11),
    而重号的 ID 会进库、进客户端、进日志,不可回收。
    所以这里宁可让 k8s 重新拉起(重新抢一个号),也不能继续发号。

    KeepAlive **不是健康检查,是独占权信号** —— 这句话是 Go 侧注释的原话,照搬。

⚠️ aetcd 没有自动 KeepAlive,续约循环由本模块驱动(同 etcdleader)。
   本地安全截止线比 lease 到期提前 TTL/3:越过就认为独占权已不可证明,立刻触发退出。
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time

import aetcd

from pandorapy import log as plog
from pandorapy.etcdlease import LeaseGoneError, refresh_or_raise
from pandorapy import snowflake

# 与 Go 侧 etcdnode 常量对齐。
DEFAULT_PREFIX = "/pandora/snowflake/node/"
DEFAULT_LEASE_TTL_SEC = 15  # docs/design/infra.md §8.1

_KEEPALIVE_DIVISOR = 3
_SAFETY_MARGIN_DIVISOR = 3

# etcd 动态分配的**最小** nodeID。低于它的号段全部保留,永远不会被分配出去。
# 与 Go 侧 etcdnode.FirstDynamicNodeID 同值,Go 那边还有 TestFirstDynamicNodeIDIsAboveStaticSegment
# 钉住"动态段必须在静态段之上"这条不变量。
#
#   0      UE DS 本地发号器(FMySnowflake)的机器号恒为 0。服务端铸的 instance_id 与
#          DS 本地 guid 汇进同一玩家背包键空间,服务端拿到 0 即撞键
#          (bag 的 DuplicateGuid fail-closed 会卡住玩家领取)。
#   1..7   static 模式(node.node_id)号段。static→etcd 的滚更**共存窗口**里,仍在跑的
#          静态旧副本不写 etcd、对抢占完全不可见;若从 1 起扫,新副本会领到旧副本正在
#          用的号 → 双活发重号。把动态段抬到静态段之上,新旧永不同号。
#
# 新增静态 node_id 必须落在 [1, FIRST_DYNAMIC_NODE_ID) 内;不够用时抬高本常量并同步
# docs/design/infra.md §8.1(抬高是纯扩容,不影响已分配的动态号)。
FIRST_DYNAMIC_NODE_ID = 8

# lease TTL 下限。与 Go 的 minLeaseTTLSec 同值 —— 理由见 acquire 里的钳制注释:
# close() 不 revoke,复用隔离期 ≈ TTL*2/3,必须显著大于 snowflake 的 1s 秒粒度。
MIN_LEASE_TTL_SEC = 5


def _holder_identity(node_id: int) -> bytes:
    """写进 etcd 的 value:**持有者身份**,不是号本身。

    ★ 号已经在 key 里了,value 再写一遍等于没写。Go 侧写的是 host/pid/时间戳 ——
    因为运维真正要问的问题是「8 号现在被谁占着」:排查重号、排查"抢不到号"、
    排查滚更时残留的旧 holder,靠的都是这一行。value 写号本身的话,
    这些问题一个都答不了,只能去翻日志猜。
    """
    import socket

    host = socket.gethostname()
    return f"{host}/{os.getpid()}/{int(time.time())}/node={node_id}".encode()


class NodeIDExhaustedError(RuntimeError):
    """[FIRST_DYNAMIC_NODE_ID, MaxNodeID) 全被占用。副本数超过 13 万才可能,实际是配置错。"""


class NodeIDLostError(RuntimeError):
    """nodeID 独占权丢失。调用方**必须**停止发号并退出进程。"""


class Holder:
    """持有一个抢占成功的 nodeID 及其 etcd lease。

    用法(对应 Go 侧 MustProvideSnowflake 的 etcd 档):

        holder = await acquire(endpoints, service_name="dialogue")
        node = snowflake.Node(holder.node_id)

        async def on_lost() -> None:
            logger.error("snowflake_nodeid_lease_lost",
                         hint="停止发号并退出,交给 k8s 重新拉起重新抢号")
            os._exit(1)      # ← 必须退出,不能降级继续发号

        holder.start_keepalive(on_lost)
    """

    __slots__ = (
        "node_id",
        "_client",
        "_lease",
        "_key",
        "_ttl",
        "_task",
        "_lost",
    )

    def __init__(
        self,
        node_id: int,
        client: aetcd.Client,
        lease: aetcd.Lease,
        key: bytes,
        ttl_sec: int,
    ) -> None:
        self.node_id = node_id
        self._client = client
        self._lease = lease
        self._key = key
        self._ttl = ttl_sec
        self._task: asyncio.Task | None = None
        self._lost = asyncio.Event()

    @property
    def lost(self) -> asyncio.Event:
        """独占权丢失事件。调用方可以 await 它然后退出进程。"""
        return self._lost

    def start_keepalive(self, on_lost=None) -> None:  # noqa: ANN001
        """启动续约循环。丢失独占权时置位 lost 并(若提供)调用 on_lost。"""
        if self._task is not None:
            return
        self._task = asyncio.create_task(
            self._keepalive_loop(on_lost), name=f"snowflake-node-{self.node_id}"
        )

    async def _keepalive_loop(self, on_lost) -> None:  # noqa: ANN001
        interval = self._ttl / _KEEPALIVE_DIVISOR
        margin = self._ttl / _SAFETY_MARGIN_DIVISOR
        safe_deadline = time.monotonic() + self._ttl - margin

        while True:
            try:
                await asyncio.sleep(min(interval, max(0.05, safe_deadline - time.monotonic())))
                now = time.monotonic()
                await refresh_or_raise(self._lease, timeout=interval)
                # ★ 只有成功响应才推进安全线,且从**发起时刻**算(保守方向)。
                safe_deadline = now + self._ttl - margin
            except asyncio.CancelledError:
                return
            except LeaseGoneError as exc:
                # ★ 服务端明确回复 lease 不存在 = nodeID 已经不是我的了。
                # 这是**证据**不是抖动,所以不走下面的安全窗重试:此刻另一副本可能
                # 已经抢到同一个 nodeID,每多发一个号就多一份重号。
                await self._declare_lost(on_lost, reason="lease_gone", err=str(exc))
                return
            except Exception as exc:  # noqa: BLE001
                if time.monotonic() < safe_deadline:
                    # 还在安全窗口内:etcd 短抖动很常见,继续试。
                    continue
                # 越过安全线:证明不了自己还持有,与已证实失主同等处置。
                await self._declare_lost(on_lost, reason="deadline_exceeded", err=str(exc))
                return

    async def _declare_lost(self, on_lost, *, reason: str, err: str) -> None:  # noqa: ANN001
        """宣告 nodeID 独占权丢失。两条失主路径共用,保证处置完全一致。"""
        plog.get().error(
            "snowflake_nodeid_lease_lost",
            node_id=self.node_id,
            reason=reason,
            err=err,
            hint=(
                "nodeID 独占权已不可证明。必须停止发号并退出进程 —— "
                "另一副本可能已抢到同一 nodeID,继续发号会重号(不变量 §9.11)"
            ),
        )
        self._lost.set()
        if on_lost is not None:
            result = on_lost()
            if asyncio.iscoroutine(result):
                await result

    async def close(self) -> None:
        """正常退出:停止续约并断开 etcd。幂等。

        ★ 刻意**不** revoke lease(与 Go 侧 Holder.Close 同):

            立即释放会让这个 nodeID 在**同一个日历秒内**就被新副本抢走 ——
            本进程这一秒已发出的号,与新 holder 从 step 0 重新数起的号**逐位相同**。
            snowflake 是秒级粒度(见 pandorapy/snowflake.py),优雅滚更收尾 + 新进程
            亚秒启动抢号即可复现;跨机器时钟偏差还会放大这个窗口。

            停止续约后 lease 在服务端于「最后一次续约 + TTL」自然过期,天然形成
            ≥ TTL*2/3(默认 ≥10s)的复用隔离期,远大于 1s 的秒粒度与现实 NTP 偏差。
            nodeID 空间共 131072 个,短暂多占一个号没有稀缺压力。
        """
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
        with contextlib.suppress(Exception):
            await self._client.close()


async def acquire(
    endpoints: list[str],
    service_name: str,
    *,
    prefix: str = DEFAULT_PREFIX,
    lease_ttl_sec: int = DEFAULT_LEASE_TTL_SEC,
    max_node_id: int = 0,
) -> Holder:
    """在 [FIRST_DYNAMIC_NODE_ID, max_node_id) 区间抢占一个独占 nodeID。

    对应 Go 的 etcdnode.Acquire。低位号段为什么保留见 FIRST_DYNAMIC_NODE_ID 注释。

    抢占用 txn 的 version == 0(仅当 key 不存在时写入)——**不能**用"先 get 看有没有,
    没有就 put":那是 TOCTOU,两个副本会同时抢到同一个号。
    """
    if not endpoints:
        raise ValueError("etcdnode: endpoints 不能为空")
    if not service_name:
        raise ValueError("etcdnode: service_name 不能为空")
    limit = max_node_id if max_node_id > 0 else snowflake.NODE_MASK + 1
    ttl = lease_ttl_sec if lease_ttl_sec > 0 else DEFAULT_LEASE_TTL_SEC
    # ★ TTL 是**正确性参数**,不只是保活参数。
    #
    # close() 刻意不 revoke,所以 nodeID 的复用隔离期完全等于「停止续约后 lease
    # 自然过期」≈ TTL*2/3 —— 它必须显著大于 snowflake 的 1s 秒粒度 + 现实时钟偏差。
    # 配置误填个小值(比如 2)会把隔离期打穿到 1.3s,新副本同秒抢到同号 → 逐位重号。
    # 与 Go 的 minLeaseTTLSec 同值同行为:钳回下限并告警,而不是照单全收。
    if ttl < MIN_LEASE_TTL_SEC:
        plog.get().warning(
            "snowflake_lease_ttl_clamped",
            configured=ttl,
            clamped_to=MIN_LEASE_TTL_SEC,
            hint="TTL 过小会打穿 nodeID 复用隔离期(close 不 revoke,隔离期≈TTL*2/3)",
        )
        ttl = MIN_LEASE_TTL_SEC

    host, _, port = endpoints[0].rpartition(":")
    client = aetcd.Client(host=host or "127.0.0.1", port=int(port or 2379))

    lease = await client.lease(ttl)
    try:
        # ★ 从 FIRST_DYNAMIC_NODE_ID 起,不是从 0 起 —— 低位号段保留给 UE DS 本地发号器
        # 与 static 副本(见常量注释)。从 0 起扫会在滚更共存窗口里领到别人正在用的号。
        for node_id in range(FIRST_DYNAMIC_NODE_ID, limit):
            key = f"{prefix}{service_name}/{node_id}".encode()
            succeeded, _ = await client.transaction(
                compare=[client.transactions.version(key) == 0],
                success=[client.transactions.put(key, _holder_identity(node_id), lease=lease.id)],
                failure=[],
            )
            if succeeded:
                plog.get().info(
                    "snowflake_nodeid_acquired",
                    service=service_name,
                    node_id=node_id,
                    lease_id=lease.id,
                    ttl_sec=ttl,
                )
                return Holder(node_id, client, lease, key, ttl)
        raise NodeIDExhaustedError(
            f"etcdnode: service={service_name} 的 "
            f"[{FIRST_DYNAMIC_NODE_ID}, {limit}) 全被占用 —— "
            f"副本数不可能这么多,检查是否有残留 key 未随 lease 过期"
        )
    except BaseException:
        with contextlib.suppress(Exception):
            await client.revoke_lease(lease.id)
        with contextlib.suppress(Exception):
            await client.close()
        raise


async def provide_node(
    endpoints: list[str],
    service_name: str,
    static_node_id: int,
    node_id_source: str = "",
    *,
    on_lost=None,  # noqa: ANN001
    **kwargs: object,
) -> tuple[snowflake.Node, Holder | None]:
    """按 node_id_source 选路 —— 对应 Go 的 MustProvideSnowflake。

    ""/"static" → 用 yaml 的 node.node_id(单副本 / dev)
    "etcd"      → etcd 自动抢占 + **在这里就把续约拉起来**(多副本)

    两条路径都是完整实现(CLAUDE.md §14.2:开关打开后的分支必须是真实实现,不是空壳)。

    ★ etcd 档的续约由本函数启动,**不再要求调用方记得调 start_keepalive**。
    忘了调不会报错:lease 到 TTL 自然过期 → 另一副本抢到同一个 nodeID →
    而本进程**毫不知情地继续用旧号发号**,两边逐位重号(§9 不变量 11)。
    这种"漏一行就静默重号"的接线必须靠结构排除,不能靠纪律。

    ★ etcd 档**必须显式传 on_lost**,没有缺省值。

    它是一个**工厂**:签名 `on_lost(holder) -> 无参可调用`。做成工厂是因为调用方
    在 holder 存在之前就要把它传进来,而处置逻辑通常要用到 holder(至少要打 node_id)。

    一度让它缺省成"退出进程"—— 那在生产上是对的,但会让任何忘了传的**测试进程**
    在一次续约抖动时直接 os._exit,pytest 当场消失且没有任何报告(本轮实际写出过
    这个形状)。而缺省成"只打日志"更糟:进程会毫不知情地继续发号 = 重号,
    正是本函数要防的事。

    两个方向都不安全,所以不给缺省 —— 忘了传就在**装配期**大声报错。
    生产用现成的 `exit_process_on_lost`(见下),测试传个无害的回调。
    """
    source = (node_id_source or "static").lower()
    if source in ("", "static"):
        # ★ static 档的号段闸,对齐 Go provider.go:96-104。**必须在这里拦**,
        # 不能靠 snowflake.Node 自己的范围检查 —— 那一条只拒 >NODE_MASK,
        # 放行 0(见 snowflake.py 的 `0 <= node_id`)。
        #
        #   node_id == 0  → 0 是**UE DS 本地发号器的保留号**。用它发号会与 DS
        #                   本地铸的 ID 逐位相同,撞进同一玩家的背包键空间。
        #   > NODE_MASK   → 超出 17bit node 段会被**静默截断**,与别的副本撞号。
        #
        # 两种都不报错、只在数据层面表现为重号,所以只能在装配期拒启。
        # 漏配 node_id 时 pydantic 给的默认值恰好是 0 —— 不拦的话"忘了配"
        # 会稳定落进最危险的那一格。
        if static_node_id == 0 or static_node_id > snowflake.NODE_MASK:
            raise ValueError(
                f"etcdnode: service {service_name} static node_id={static_node_id} "
                f"超出范围 [1,{snowflake.NODE_MASK}](0 是 UE DS 本地发号器的保留号)"
            )
        return snowflake.Node(static_node_id), None
    if source != "etcd":
        raise ValueError(
            f"etcdnode: 未知的 node_id_source={node_id_source!r}(只支持 static / etcd)"
        )
    if on_lost is not None and not callable(on_lost):
        # 契约只写在 docstring 里就等于没有:传错类型时会一路走到
        # `on_lost(holder)` 才炸,而那时 nodeID 已经抢到了。
        raise TypeError(
            f"etcdnode: on_lost 必须是工厂 on_lost(holder) -> 无参可调用,"
            f"实为 {type(on_lost).__name__}"
        )
    if on_lost is None:
        raise ValueError(
            "etcdnode: node_id_source=etcd 必须显式提供 on_lost 工厂 —— "
            "生产传 snowflake_etcd.exit_process_on_lost,测试传个无害的。"
            "缺省值在两个方向上都不安全,见本函数 docstring"
        )
    holder = await acquire(endpoints, service_name, **kwargs)  # type: ignore[arg-type]
    try:
        handler = on_lost(holder)
        if not callable(handler):
            raise TypeError(
                f"etcdnode: on_lost 工厂必须返回无参可调用,实为 {type(handler).__name__}"
            )
        holder.start_keepalive(handler)
    except BaseException:
        # ★ 此刻 nodeID **已经抢到**但续约还没起来。不收拾的话会留下一个
        # 永不续约的 holder + 一条 etcd 连接:那个 nodeID 要到 lease 自然过期
        # 才会被释放,而本进程既不发号也不退出 —— 一个谁都看不见的占用。
        with contextlib.suppress(Exception):
            await holder.close()
        raise
    return snowflake.Node(holder.node_id), holder


def exit_process_on_lost(holder: Holder):  # noqa: ANN201
    """生产用的 on_lost 工厂:打错误日志后**退出进程**,交给 k8s 重新拉起重新抢号。

    为什么不是"停止发号但继续服务":发号是本服务大量写路径的前置,停了等于半瘫,
    而半瘫的进程还会被 readiness 判成健康继续接流量 —— 比直接退出更难排查。
    """

    async def _exit() -> None:
        plog.get().error(
            "snowflake_nodeid_lost_exiting",
            node_id=holder.node_id,
            hint="nodeID 独占权已不可证明,退出进程交给 k8s 重新拉起重新抢号",
        )
        os._exit(1)

    return _exit
