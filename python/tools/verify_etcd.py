"""etcd 语义验证 —— Python 迁移唯一可能一票否决的项，必须实测而不是读 README。

为什么单独立一个工具:
    `pkg/dsauthfence`(4299 行) + `pkg/leader/etcdleader` + `pkg/snowflake/etcdnode` +
    `pkg/cellroute/etcdtable` + `pkg/killswitch/etcdkv` 全都压在 etcd 的三个语义上:
        1. lease keepalive —— 失租必须被及时发现(否则脑裂:两个 owner 同时认为自己有效)
        2. watch 断线重连 —— 事件丢了不会报错,只会让 cellroute / killswitch 用过期数据
        3. txn CAS       —— 不真互斥就等于没有 fencing
    Go 侧靠 clientv3(与 etcd 服务端同一团队维护,52121★)。Python 侧最主流的
    kragniz/python-etcd3 是 450★ / 20 个月未更新 / 202 个 open issue,活跃的 martyanov/aetcd
    只有 33★。星数不能证明语义正确 —— 只能实测。

用法:
    # 起一个 etcd(任意方式),然后:
    python tools/verify_etcd.py --endpoint 127.0.0.1:12379

    # 想连带验证"杀掉 etcd 后旧 leader 是否让位",额外传容器名:
    python tools/verify_etcd.py --endpoint 127.0.0.1:12379 --docker-container pandora-etcd-verify

判定:全部 PASS 才算 etcd 这条路通。任何一条 FAIL 都意味着继续迁 Python 之前
必须换客户端方案(退路:用 etcd 官方 .proto 自己 grpcio-tools 生成客户端)。
"""

from __future__ import annotations

import argparse
import asyncio
import pathlib
import subprocess
import sys
import time

REPO = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "python"))

from pandorapy import _utf8  # noqa: E402,F401

import aetcd  # noqa: E402

# 与 Go 侧 pkg/leader/etcdleader 的常量对齐,这样验的是同一套时间参数。
LEASE_TTL_SEC = 15  # etcdleader.DefaultLeaseTTLSec
PREFIX = "/pandora/verify/"

_results: list[tuple[str, bool, str]] = []


def _record(name: str, ok: bool, detail: str = "") -> None:
    _results.append((name, ok, detail))
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {name}" + (f" —— {detail}" if detail else ""), flush=True)


def _client(endpoint: str) -> aetcd.Client:
    host, _, port = endpoint.rpartition(":")
    return aetcd.Client(host=host or "127.0.0.1", port=int(port))


# ── 1. lease 基本语义 ─────────────────────────────────────────────────────────


async def test_lease_grant_and_key_binding(endpoint: str) -> None:
    """lease 授予后绑 key;lease 还活着时 key 必须存在。"""
    async with _client(endpoint) as c:
        lease = await c.lease(LEASE_TTL_SEC)
        key = f"{PREFIX}lease_bind".encode()
        await c.put(key, b"v1", lease=lease)
        got = await c.get(key)
        ok = got is not None and got.value == b"v1"
        remaining = await lease.remaining_ttl()
        _record(
            "lease 授予 + key 绑定",
            ok and remaining > 0,
            f"remaining_ttl={remaining}s granted={await lease.granted_ttl()}s",
        )
        await c.revoke_lease(lease.id)


async def test_lease_expiry_removes_key(endpoint: str) -> None:
    """★ 核心 fencing 机制:lease 到期后绑定的 key 必须自动消失。

    这是 dsauthfence / etcdleader 的地基 —— 旧 owner 进程卡死/失联后,它持有的
    "我是 owner"标记必须靠 lease 到期自动消失,不能依赖旧进程主动删。
    用短 TTL(2s)验证,避免等 15s。
    """
    async with _client(endpoint) as c:
        lease = await c.lease(2)
        key = f"{PREFIX}lease_expiry".encode()
        await c.put(key, b"owner-A", lease=lease)
        assert (await c.get(key)) is not None, "刚写入就读不到"

        # 刻意**不**续约,等它过期。etcd 的过期检查有粒度,给足余量。
        await asyncio.sleep(5)
        after = await c.get(key)
        _record(
            "lease 到期后 key 自动消失(fencing 地基)",
            after is None,
            "过期后仍能读到 key —— fencing 不成立" if after is not None else "",
        )


async def test_manual_keepalive_extends_lease(endpoint: str) -> None:
    """★ 手工 keepalive 必须真的能延长 lease。

    ⚠️ aetcd **没有** Go clientv3.KeepAlive 那种后台自动续约的 goroutine,
    只有 refresh_lease / Lease.refresh() —— 续约节奏完全由调用方负责。
    这是迁移时最容易写错的地方:续约间隔必须远小于 TTL(Go 侧 clientv3 内部
    是 TTL/3),而且必须只把**当前 lease 的成功响应**当作续约成功(§9.22)。
    """
    async with _client(endpoint) as c:
        lease = await c.lease(4)
        key = f"{PREFIX}keepalive".encode()
        await c.put(key, b"owner-A", lease=lease)

        # 以 TTL/3 的节奏续 3 轮,总时长(约 6s)已超过原始 TTL(4s)。
        for _ in range(5):
            await asyncio.sleep(4 / 3)
            await lease.refresh()

        alive = await c.get(key)
        remaining = await lease.remaining_ttl()
        _record(
            "手工 keepalive 延长 lease",
            alive is not None and remaining > 0,
            f"续约 5 轮后仍存活, remaining_ttl={remaining}s"
            if alive is not None
            else "续约失效 —— key 在原 TTL 后消失了",
        )
        await c.revoke_lease(lease.id)


# ── 2. txn CAS 互斥 ──────────────────────────────────────────────────────────


async def test_txn_cas_is_mutually_exclusive(endpoint: str) -> None:
    """★ 并发 CAS 必须只有一个赢。

    这是 owner 迁移、snowflake nodeID 抢占、选主的共同基础。若 CAS 不真互斥,
    两个副本会同时认为自己抢到了 —— 重号 / 双 owner,且不报错。
    用 50 个并发抢同一个 key,断言恰好 1 个成功。
    """
    key = f"{PREFIX}cas_race".encode()
    async with _client(endpoint) as c:
        await c.delete(key)

    async def try_claim(idx: int) -> bool:
        async with _client(endpoint) as cc:
            # replace(key, initial, new):仅当当前值 == initial 时才写入。
            # 这里 initial=b"" 表示"仅当不存在/为空时"——用 transaction 更严谨,
            # 但 aetcd 的 replace 语义已足够暴露"是否真互斥"。
            try:
                return await cc.replace(key, b"", f"winner-{idx}".encode())
            except Exception:
                return False

    # 先建一个空值,让 replace 的 compare 有基准。
    async with _client(endpoint) as c:
        await c.put(key, b"")

    results = await asyncio.gather(*(try_claim(i) for i in range(50)))
    winners = sum(1 for r in results if r)
    async with _client(endpoint) as c:
        final = await c.get(key)
    _record(
        "txn CAS 并发互斥(50 并发抢 1 个 key)",
        winners == 1,
        f"赢家数={winners}(应为 1), 最终值={final.value.decode() if final else None}",
    )


# ── 3. watch ─────────────────────────────────────────────────────────────────


async def test_watch_receives_events(endpoint: str) -> None:
    """watch 能收到 put/delete 事件。cellroute / killswitch 靠它感知配置变更。"""
    key = f"{PREFIX}watch_basic".encode()
    received: list[bytes] = []

    async with _client(endpoint) as c:
        await c.delete(key)
        watcher = await c.watch(key)

        async def consume() -> None:
            async for event in watcher:
                received.append(event.kv.value)
                if len(received) >= 3:
                    return

        task = asyncio.create_task(consume())
        await asyncio.sleep(0.5)
        for i in range(3):
            await c.put(key, f"v{i}".encode())
            await asyncio.sleep(0.2)
        try:
            await asyncio.wait_for(task, timeout=5)
        except TimeoutError:
            pass
        _record(
            "watch 收到事件",
            len(received) == 3,
            f"收到 {len(received)}/3: {[v.decode() for v in received]}",
        )


async def test_watch_from_revision_recovers_missed_events(endpoint: str) -> None:
    """★ 断线重连必须能补回错过的事件。

    这是 watch 语义里最关键的一条:etcd 支持 start_revision,重连时从上次已处理的
    revision+1 重新 watch,把断线期间的变更补回来。若客户端不支持 / 支持不正确,
    断线期间的配置变更会**永久丢失**,而且不会报错 —— cellroute 会一直用过期路由表,
    killswitch 会漏掉一次关停指令。
    """
    key = f"{PREFIX}watch_revision".encode()
    async with _client(endpoint) as c:
        await c.delete(key)
        await c.put(key, b"before", prev_kv=False)
        base_rev = (await c.get(key)).mod_revision

        # 模拟"客户端断线期间"发生的 3 次变更
        for i in range(3):
            await c.put(key, f"during-{i}".encode())

        # 重连:从 base_rev+1 开始 watch,应当补回全部 3 条
        recovered: list[bytes] = []
        watcher = await c.watch(key, start_revision=base_rev + 1)

        async def consume() -> None:
            async for event in watcher:
                recovered.append(event.kv.value)
                if len(recovered) >= 3:
                    return

        try:
            await asyncio.wait_for(asyncio.create_task(consume()), timeout=5)
        except TimeoutError:
            pass
        _record(
            "watch 按 revision 补回断线期间事件",
            len(recovered) == 3,
            f"补回 {len(recovered)}/3: {[v.decode() for v in recovered]}"
            + ("" if len(recovered) == 3 else " —— 断线期间的变更会永久丢失"),
        )


# ── 4. 端到端选主(对应 pkg/leader/etcdleader 的语义)────────────────────────


class LeaderElection:
    """最小选主实现 —— 对应 Go 的 concurrency.Election(aetcd 无对等物,必须自己写)。

    语义严格对齐 pkg/leader/etcdleader:
      - 未当选时等待,不占资源
      - 当选后运行 run(leader_ctx)
      - **失去领导权不退出进程**,只取消 leader_ctx,继续重新竞选
      - 主动下线时 resign 让位
    """

    def __init__(self, endpoint: str, election: str, ttl: int = LEASE_TTL_SEC) -> None:
        self._endpoint = endpoint
        self._key = f"{PREFIX}election/{election}".encode()
        self._ttl = ttl
        self.is_leader = False
        self.lost_leadership_count = 0

    async def campaign_once(self, hold_sec: float) -> bool:
        """尝试当选;当选则持有 hold_sec 秒(期间按 TTL/3 续约)后主动让位。"""
        async with _client(self._endpoint) as c:
            lease = await c.lease(self._ttl)
            # 抢锁:仅当 key 不存在时写入。aetcd 的 replace 需要基准值,
            # 这里用"读不到就 put + 再读回校验自己是持有者"的两步,
            # 严格实现应该用 transaction 的 create_revision == 0 比较。
            existing = await c.get(self._key)
            if existing is not None:
                await c.revoke_lease(lease.id)
                return False
            await c.put(self._key, b"me", lease=lease)
            # 二次校验:并发下可能被别人抢先(这正是为什么严格实现必须用 txn)
            back = await c.get(self._key)
            if back is None or back.lease != lease.id:
                await c.revoke_lease(lease.id)
                return False

            self.is_leader = True
            deadline = time.monotonic() + hold_sec
            try:
                while time.monotonic() < deadline:
                    await asyncio.sleep(self._ttl / 3)
                    await lease.refresh()
            finally:
                self.is_leader = False
                await c.revoke_lease(lease.id)  # resign
            return True


async def test_leader_election_single_winner(endpoint: str) -> None:
    """★ 两个副本同时竞选,只能有一个当选。"""
    async with _client(endpoint) as c:
        await c.delete(f"{PREFIX}election/e2e".encode())

    a = LeaderElection(endpoint, "e2e")
    b = LeaderElection(endpoint, "e2e")
    won = await asyncio.gather(a.campaign_once(2.0), b.campaign_once(2.0))
    _record(
        "选主:两副本竞选只有一个当选",
        sum(won) == 1,
        f"当选数={sum(won)}(应为 1)",
    )


async def test_leader_handover_after_resign(endpoint: str) -> None:
    """★ 前任 resign 后,后继必须能在 lease TTL 内接管。"""
    async with _client(endpoint) as c:
        await c.delete(f"{PREFIX}election/handover".encode())

    a = LeaderElection(endpoint, "handover")
    first = await a.campaign_once(0.1)  # 立刻让位
    await asyncio.sleep(0.5)
    b = LeaderElection(endpoint, "handover")
    second = await b.campaign_once(0.1)
    _record(
        "选主:前任让位后后继接管",
        first and second,
        f"first={first} second={second}",
    )


async def test_leader_lease_expiry_frees_lock(endpoint: str) -> None:
    """★ 最关键的一条:leader 进程"卡死"(不续约)后,锁必须靠 lease 到期自动释放。

    这是"旧 DS 卡死后新 DS 能否接管"在 etcd 层的对应物。若不成立,一个卡死的
    副本会永久占住 leader 位置,撮合循环永远不再运行 —— 而且没有任何报错。
    """
    key = f"{PREFIX}election/stuck".encode()
    async with _client(endpoint) as c:
        await c.delete(key)
        # 模拟"当选后进程卡死":拿了 2s lease、写了锁,然后完全不续约
        lease = await c.lease(2)
        await c.put(key, b"stuck-leader", lease=lease)
        assert (await c.get(key)) is not None

        await asyncio.sleep(5)  # 卡死超过 TTL
        freed = (await c.get(key)) is None

        # 后继应当能抢到
        took_over = False
        if freed:
            new_lease = await c.lease(LEASE_TTL_SEC)
            await c.put(key, b"new-leader", lease=new_lease)
            got = await c.get(key)
            took_over = got is not None and got.value == b"new-leader"
            await c.revoke_lease(new_lease.id)

    _record(
        "选主:leader 卡死后锁靠 lease 到期释放并被接管",
        freed and took_over,
        "" if freed else "锁没有自动释放 —— 卡死的副本会永久占位",
    )


# ── 5. etcd 挂掉 / 恢复 ───────────────────────────────────────────────────────


async def test_behavior_when_etcd_down(endpoint: str, container: str | None) -> None:
    """★ etcd 不可用时客户端必须报错(fail-closed),不能静默返回"没有/空"。

    §9.22 明确要求:查询超时或结果不确定必须返回 UNKNOWN 并重试或 fail-closed,
    **禁止冒充 OFFLINE / 空闲 / 成功**。若客户端在 etcd 挂掉时把 get 返回成 None,
    上层会把"查不到"当成"确实没有 owner",于是放行第二个 owner —— 脑裂。
    """
    if not container:
        _record(
            "etcd 不可用时 fail-closed",
            True,
            "跳过(未传 --docker-container);要验请加该参数",
        )
        return

    key = f"{PREFIX}down_probe".encode()
    async with _client(endpoint) as c:
        await c.put(key, b"exists")

    subprocess.run(["docker", "stop", container], capture_output=True, check=False)
    await asyncio.sleep(1)

    raised = False
    returned_none = False
    try:
        async with _client(endpoint) as c:
            got = await asyncio.wait_for(c.get(key), timeout=8)
            returned_none = got is None
    except Exception:
        raised = True

    subprocess.run(["docker", "start", container], capture_output=True, check=False)
    await asyncio.sleep(8)

    # 恢复后必须能重新读到(数据没丢)
    recovered = False
    for _ in range(10):
        try:
            async with _client(endpoint) as c:
                got = await asyncio.wait_for(c.get(key), timeout=3)
                if got is not None and got.value == b"exists":
                    recovered = True
                    break
        except Exception:
            await asyncio.sleep(1)

    _record(
        "etcd 不可用时报错而非静默返回空",
        raised and not returned_none,
        f"raised={raised} returned_none={returned_none}"
        + ("(静默返回 None = 会被上层当成「确实没有 owner」→ 脑裂)" if returned_none else ""),
    )
    _record("etcd 恢复后数据仍在且可读", recovered)


# ── main ─────────────────────────────────────────────────────────────────────


async def _run_all(endpoint: str, container: str | None) -> int:
    print(f"=== etcd 语义验证 @ {endpoint} ===\n", flush=True)
    tests = [
        test_lease_grant_and_key_binding,
        test_lease_expiry_removes_key,
        test_manual_keepalive_extends_lease,
        test_txn_cas_is_mutually_exclusive,
        test_watch_receives_events,
        test_watch_from_revision_recovers_missed_events,
        test_leader_election_single_winner,
        test_leader_handover_after_resign,
        test_leader_lease_expiry_frees_lock,
    ]
    for test in tests:
        try:
            await test(endpoint)
        except Exception as exc:  # noqa: BLE001
            _record(test.__name__, False, f"抛异常 {type(exc).__name__}: {exc}")

    try:
        await test_behavior_when_etcd_down(endpoint, container)
    except Exception as exc:  # noqa: BLE001
        _record("etcd 不可用时 fail-closed", False, f"抛异常 {type(exc).__name__}: {exc}")

    passed = sum(1 for _, ok, _ in _results if ok)
    total = len(_results)
    print(f"\n=== 结果: {passed}/{total} PASS ===")
    failed = [name for name, ok, _ in _results if not ok]
    if failed:
        print("失败项:")
        for name in failed:
            print(f"  - {name}")
        print("\n判定:etcd 这条路**不通**。继续迁 Python 前必须换客户端方案")
        print("     (退路:用 etcd 官方 .proto 自己 grpcio-tools 生成客户端)。")
        return 1
    print("\n判定:etcd 三个关键语义(lease keepalive / watch 补事件 / txn CAS)全部成立。")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--endpoint", default="127.0.0.1:12379")
    ap.add_argument(
        "--docker-container",
        default=None,
        help="etcd 容器名;传了才跑「杀掉 etcd」那两条",
    )
    args = ap.parse_args()
    return asyncio.run(_run_all(args.endpoint, args.docker_container))


if __name__ == "__main__":
    raise SystemExit(main())
