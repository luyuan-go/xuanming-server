"""`biz_sweep.py` 的行为测试 —— 对应 Go 侧 `internal/biz/hub_test.go` 里
`TestSweepOnce_*` / `TestReconcile_*` 那批,外加 Go 没有、但 Python 移植必须自证的
几条(`safego.loop` 接线、写者门控顺序、负 `min_replicas` 的切片语义差异)。

## 这批测试到底在守什么

被测代码是**后台循环**。它的失效形状与 RPC 完全不同:RPC 出错客户端立刻看得见,
后台循环死掉**没有任何人看得见** —— 进程还在、端口还在、health 还答 SERVING:

  - 心跳超时不再标 draining → 玩家继续被分配到一台已经不在的 Hub;
  - `reconcile_owner_cleanups` 停摆 → transfer/release 的旧 owner 没人驱逐,
    同一玩家在两台 DS 各有一份 owner(§9.22 脑裂);
  - `migrate_player` 某条失败路径忘了把玩家加回源 member 索引 → 他从 drain 扫描里
    永久消失,唯一一次迁移通知丢失,而**日志全绿**;
  - `reclaim_drained_shards` 哪天"顺手"真删了镜像 → 逻辑计时器冒充物理拆机证明。

所以本文件的重心是**否定路径与副作用记账**:每条失败分支都断言"源索引里还有他"、
"座位退了没有"、"权威被读了几次"。

## 测试口径

不连真 Redis / Agones / owner 服务。被测层是**编排 + 判定**,存储语义在 `repo.py` /
`ledger.py` / `auth_repo.py` 各有自己的测试。这里用能脚本化竞态返回的假件,把
Go 测试里"只在并发窗口出现"的分支变成确定性用例。

**刻意不 `asyncio.sleep` 等后台循环**(那本身就是 §16.10 禁止的形状):
`run_heartbeat_sweep` 的两条用例一条替换 `safego.loop` 只验接线,一条用
`asyncio.Event` 精确等到第一轮真的跑完。

每个用例的 `★ 变异:` 一行写明"把生产代码的哪一行改坏,本用例会红" ——
没有这一行的测试等于没验证过自己有没有牙。
"""

from __future__ import annotations

import asyncio
import dataclasses
from typing import Any

import pytest
from google.protobuf import unknown_fields
from pandora.hub.v1 import allocator_pb2 as hubpb
from structlog.testing import capture_logs

from pandorapy import errcode, releasetrack
from pandorapy import source_revision as srev
from pandorapy.services.hub_allocator import biz_sweep as bz
from pandorapy.services.hub_allocator import conf as hconf
from pandorapy.services.hub_allocator import ledger as L
from pandorapy.services.hub_allocator import repo as R
from pandorapy.services.hub_allocator.biz import HubUsecase
from pandorapy.services.hub_allocator.biz_base import (
    STATE_DRAINING,
    STATE_READY,
    STATE_STOPPING,
)
from pandorapy.services.hub_allocator.owner_authority import OWNER_ADMITTED_STALE_TTL_SEC

PLAYER = 1001
POD_A = "hub-a"
POD_B = "hub-b"

pytestmark = pytest.mark.asyncio


# ── 工厂 ─────────────────────────────────────────────────────────────────────


def shard(
    pod: str,
    *,
    shard_id: int = 1,
    count: int = 0,
    cap: int = 500,
    state: str = STATE_READY,
    region: str = "global",
    track: str = releasetrack.STABLE,
    draining_since_ms: int = 0,
    uid: str = "",
) -> hubpb.HubShardStorageRecord:
    return hubpb.HubShardStorageRecord(
        hub_pod_name=pod,
        hub_addr=pod + ":7777",
        region=region,
        shard_id=shard_id,
        player_count=count,
        capacity=cap,
        state=state,
        release_track=track,
        draining_since_ms=draining_since_ms,
        gameserver_uid=uid,
    )


def assignment(
    *,
    player_id: int = PLAYER,
    pod: str = POD_A,
    shard_id: int = 1,
    aid: str = "assign-old",
    region: str = "global",
    track: str = releasetrack.STABLE,
    role_id: int = 55,
    team_id: int = 0,
    transfer_pending: bool = False,
    release_pending: bool = False,
    transfer_source_pod: str = "",
) -> hubpb.HubAssignmentStorageRecord:
    return hubpb.HubAssignmentStorageRecord(
        player_id=player_id,
        hub_pod_name=pod,
        hub_addr=pod + ":7777",
        shard_id=shard_id,
        region=region,
        assignment_id=aid,
        release_track=track,
        role_id=role_id,
        team_id=team_id,
        transfer_cleanup_pending=transfer_pending,
        release_cleanup_pending=release_pending,
        transfer_source_hub_pod_name=transfer_source_pod,
    )


def cleanup_ref(*, player_id: int = PLAYER, aid: str = "assign-new") -> R.TransferCleanupRef:
    return R.TransferCleanupRef(player_id=player_id, target_assignment_id=aid)


# ── 假件 ─────────────────────────────────────────────────────────────────────


class FakeRepo:
    """脚本化仓储。返回 None = 不存在(与 repo.py 的真实契约一致)。"""

    def __init__(self, shards: list | None = None, *, assignment_rec=None) -> None:
        self.shards = list(shards or [])
        self.assignment = assignment_rec
        self.members: dict[str, list[int]] = {}
        self.cleanup_pods: list[str] = []
        self.cleanups: dict[str, list[R.TransferCleanupRef]] = {}
        self.stale: list[str] = []
        self.removed_active: list[str] = []
        self.member_added: list[tuple[str, int]] = []
        self.member_removed: list[tuple[str, int]] = []
        self.registered: list[tuple[str, R.TransferCleanupRef]] = []
        self.removed_refs: list[tuple[str, R.TransferCleanupRef]] = []
        self.cas_calls: list[tuple] = []
        self.cas_results: list[bool] = []
        self.advance_calls = 0
        self.get_assignment_calls = 0
        self.list_shards_calls = 0
        # 故障注入开关
        self.advance_error: BaseException | None = None
        self.list_shards_error: BaseException | None = None
        self.list_pods_error: BaseException | None = None
        self.list_cleanups_error: BaseException | None = None
        self.get_assignment_error: BaseException | None = None
        self.members_error: BaseException | None = None
        self.stale_error: BaseException | None = None
        self.cas_error: BaseException | None = None
        self.add_member_error: BaseException | None = None
        self.update_error: BaseException | None = None
        self.register_error: BaseException | None = None

    # 分片
    async def list_shards(self) -> list:
        self.list_shards_calls += 1
        if self.list_shards_error is not None:
            raise self.list_shards_error
        return list(self.shards)

    async def get_shard(self, pod: str):
        for s in self.shards:
            if s.hub_pod_name == pod:
                return s
        return None

    async def update_shard_with_lock(self, pod: str, max_retry: int, fn, shard_ttl_sec: float):
        if self.update_error is not None:
            raise self.update_error
        for s in self.shards:
            if s.hub_pod_name == pod:
                fn(s)
                return
        raise errcode.PandoraError(errcode.ErrHubNoAvailable, "hub shard %s not found", pod)

    async def range_stale_shards(self, threshold_ms: int) -> list[str]:
        if self.stale_error is not None:
            raise self.stale_error
        self.stale_threshold = threshold_ms
        return list(self.stale)

    async def remove_active(self, pod: str) -> None:
        self.removed_active.append(pod)

    # 归属
    async def get_assignment(self, player_id: int):
        self.get_assignment_calls += 1
        if self.get_assignment_error is not None:
            raise self.get_assignment_error
        return self.assignment

    async def compare_and_swap_assignment(self, player_id, expected, next_rec, ttl):
        self.cas_calls.append((player_id, expected, next_rec, ttl))
        if self.cas_error is not None:
            raise self.cas_error
        ok = self.cas_results.pop(0) if self.cas_results else True
        if ok:
            self.assignment = next_rec
        return ok

    async def set_team_shard(self, team_id: int, pod: str, ttl: float) -> None:
        return None

    # 成员索引
    async def add_shard_member(self, pod: str, player_id: int, ttl: float) -> None:
        if self.add_member_error is not None:
            raise self.add_member_error
        self.member_added.append((pod, player_id))
        self.members.setdefault(pod, [])
        if player_id not in self.members[pod]:
            self.members[pod].append(player_id)

    async def remove_shard_member(self, pod: str, player_id: int) -> None:
        self.member_removed.append((pod, player_id))
        if player_id in self.members.get(pod, []):
            self.members[pod].remove(player_id)

    async def list_shard_members(self, pod: str) -> list[int]:
        if self.members_error is not None:
            raise self.members_error
        return list(self.members.get(pod, []))

    # cleanup saga 索引
    async def list_transfer_cleanup_pods(self) -> list[str]:
        if self.list_pods_error is not None:
            raise self.list_pods_error
        return list(self.cleanup_pods)

    async def list_transfer_cleanups(self, source_pod: str) -> list:
        if self.list_cleanups_error is not None:
            raise self.list_cleanups_error
        return list(self.cleanups.get(source_pod, []))

    async def register_transfer_cleanup(self, source_pod: str, ref) -> None:
        if self.register_error is not None:
            raise self.register_error
        self.registered.append((source_pod, ref))

    async def remove_transfer_cleanup(self, source_pod: str, ref) -> None:
        self.removed_refs.append((source_pod, ref))

    # 写者水位
    async def advance_writer_fences(self) -> None:
        self.advance_calls += 1
        if self.advance_error is not None:
            raise self.advance_error


class FakeFleet:
    """只实现 `list_shards` 的拓扑源(**刻意不实现** scaler,对应 Mock/Local)。"""

    async def list_shards(self, region: str) -> list:
        return []


class FakeScalerFleet(FakeFleet):
    """叠加 `HubFleetScaler`(对应真 Agones provider)。"""

    def __init__(self, replicas: int = 3) -> None:
        self.replicas = replicas
        self.sets: list[int] = []
        self.get_error: BaseException | None = None
        self.set_error: BaseException | None = None

    async def get_fleet_replicas(self) -> int:
        if self.get_error is not None:
            raise self.get_error
        return self.replicas

    async def set_fleet_replicas(self, replicas: int) -> None:
        if self.set_error is not None:
            raise self.set_error
        self.sets.append(replicas)
        self.replicas = replicas


class FakeSigner:
    def __init__(self, *, token: str = "tok", exp_ms: int = 999, error=None) -> None:
        self.token = token
        self.exp_ms = exp_ms
        self.error = error
        self.calls: list = []

    async def sign_hub_ticket(self, player_id: int, role_id: int, binding):
        self.calls.append((player_id, role_id, binding))
        if self.error is not None:
            raise self.error
        return self.token, self.exp_ms


class FakePusher:
    def __init__(self, *, error: BaseException | None = None) -> None:
        self.error = error
        self.pushed: list[tuple[int, bytes]] = []

    async def push_migrate(self, player_id: int, payload: bytes) -> None:
        if self.error is not None:
            raise self.error
        self.pushed.append((player_id, payload))


@dataclasses.dataclass(slots=True)
class FakeFence:
    """写者继任租约视图。★ `current()` 返回 `(held, token)`(与 Go 顺序相反)。"""

    held: bool = True
    token: int = 7

    def current(self) -> tuple[bool, int]:
        return self.held, self.token


def make_cfg(**kw) -> hconf.HubConf:
    base = {
        "heartbeat_timeout": "30s",
        "sweep_interval": "5s",
        "shard_ttl": "30m",
        "assignment_ttl": "30m",
        "reservation_ttl": "3m",
        "default_region": "global",
        "default_capacity": 500,
        "optimistic_retry": 3,
        "transfer_cooldown": "10s",
        "players_per_hub": 500,
        "min_replicas": 1,
        "max_replicas": 20,
        "migrate_grace_seconds": 30,
        "consolidation_batch": 50,
    }
    base.update(kw)
    return hconf.HubConf(**base)


def make_uc(
    *,
    repo: FakeRepo | None = None,
    fleet=None,
    signer=None,
    cfg: hconf.HubConf | None = None,
    pusher=None,
    fence: FakeFence | None = None,
) -> HubUsecase:
    """组装体切片:legacy 面(`auth_repo is None`),与 Go 的 mock 用例同档。"""
    uc = HubUsecase(repo or FakeRepo(), fleet or FakeFleet(), signer or FakeSigner(), cfg or make_cfg())
    uc.migrate = pusher
    uc.writer_fence = fence
    return uc


def events(logs: list[dict]) -> list[str]:
    return [entry["event"] for entry in logs]


def one(logs: list[dict], event: str) -> dict:
    hits = [entry for entry in logs if entry["event"] == event]
    assert len(hits) == 1, f"want exactly one {event!r}, got {events(logs)}"
    return hits[0]


# ═══════════════════════════════════════════════════════════════════════════
# 1. 组装体自证:MRO 与职责归属
# ═══════════════════════════════════════════════════════════════════════════


async def test_mro_puts_sweep_first_and_base_last() -> None:
    """`add_shard_member` 必须解析到 `SweepMixin`,基座必须在最右。

    MRO 顺序不是风格问题:`ShardMixin.replace_assignment_saga` 与
    `RpcMixin.assign_hub` 都调 `self.add_shard_member`,若哪天别的 mixin 也定义了
    同名方法而顺序又被调过,调用会静默落到另一份实现上 —— 两批的补偿语义完全不同
    (saga 的 CAS loser 要退座,drain 迁移的 CAS loser 不重试)。

    ★ 变异:把 `biz.py` 的 `class HubUsecase(SweepMixin, RpcMixin, ShardMixin,
      HubUsecaseBase)` 改成 `(HubUsecaseBase, SweepMixin, ...)` → 本用例变红
      (Python 会直接抛 MRO 冲突或基座前置断言失败)。
    """
    names = [c.__name__ for c in HubUsecase.__mro__]
    assert names == [
        "HubUsecase",
        "SweepMixin",
        "RpcMixin",
        "ShardMixin",
        "HubUsecaseBase",
        "object",
    ]
    assert HubUsecase.add_shard_member is bz.SweepMixin.add_shard_member
    assert HubUsecase.remove_shard_member is bz.SweepMixin.remove_shard_member


# ═══════════════════════════════════════════════════════════════════════════
# 2. 后台循环接线:safego 是唯一允许的起法
# ═══════════════════════════════════════════════════════════════════════════


async def test_run_heartbeat_sweep_uses_named_safego_loop(monkeypatch) -> None:
    """必须走 `safego.loop` + 具名点位,且 interval 取自配置。

    裸 `asyncio.create_task` 的异常会被存进 Task 对象等人来取,没人取就**零日志**
    地躺着:进程照跑、health 照答 SERVING,而这条补偿链已经死了。点位名同时是
    `pandora_safego_panic_recovered_total{name}` 的 label —— 与 Go 差一个字母,
    告警规则就只覆盖到一半副本。

    ★ 变异:把 `run_heartbeat_sweep` 里的 `SWEEP_TASK_NAME` 改成任意别的串,
      或把 interval 写死成 5.0 → 本用例变红。
    """
    seen: dict[str, Any] = {}

    async def fake_loop(name: str, interval: float, fn) -> None:
        seen["name"] = name
        seen["interval"] = interval
        seen["fn"] = fn
        await fn()  # 真跑一轮,确认传进去的是可 await 的具名函数

    monkeypatch.setattr(bz.safego, "loop", fake_loop)
    uc = make_uc(cfg=make_cfg(sweep_interval="7s"))
    await uc.run_heartbeat_sweep()

    assert seen["name"] == "hub_heartbeat_sweep"
    assert seen["name"] == bz.SWEEP_TASK_NAME
    assert seen["interval"] == 7.0
    # 不许裸 lambda:告警里认不出是谁。
    assert seen["fn"].__name__ == "_tick"


async def test_run_heartbeat_sweep_logs_started_and_stopped_on_cancel() -> None:
    """启停留痕;取消必须**继续抛出** CancelledError。

    吞掉 CancelledError 会让停机时 `server.run()` 的 gather 永远等不到这个任务,
    Pod 卡在 Terminating 直到被 SIGKILL —— §9 不变量 16 要求的优雅摘流量当场失效。

    ★ 变异:把 `except asyncio.CancelledError:` 分支里的 `raise` 删掉 →
      `pytest.raises(CancelledError)` 变红。

    ★ 用 `asyncio.Event` 而不是 `asyncio.sleep` 等第一轮:sleep 碰运气本身就是
      §16.10 禁止的形状,慢机器上会假红。
    """
    ticked = asyncio.Event()
    uc = make_uc(cfg=make_cfg(sweep_interval="10ms"))

    original = uc.heartbeat_sweep_tick

    async def _tick(state) -> None:
        await original(state)
        ticked.set()

    uc.heartbeat_sweep_tick = _tick  # type: ignore[method-assign]

    with capture_logs() as logs:
        task = asyncio.create_task(uc.run_heartbeat_sweep())
        await asyncio.wait_for(ticked.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert "hub_heartbeat_sweep_started" in events(logs)
    assert "hub_heartbeat_sweep_stopped" in events(logs)
    started = one(logs, "hub_heartbeat_sweep_started")
    assert started["interval"] == "10ms"
    assert started["timeout"] == "30s"


# ═══════════════════════════════════════════════════════════════════════════
# 3. tick 的写者门控 + 步骤隔离
# ═══════════════════════════════════════════════════════════════════════════


async def test_tick_prunes_owner_admitted_before_writer_gate() -> None:
    """census 准入缓存的老化剪枝必须在**写者门控之前**无条件执行。

    热备副本自己处理过的心跳也会往 `owner_admitted` 里写项。若剪枝放在写者门之后,
    非写者副本的缓存永不回收 —— 而它是个纯内存 dict,无界增长直到 OOM
    (§9 不变量 18 的内存版)。更隐蔽的是:该副本一旦当选写者,满是死实例的缓存
    会让回流玩家被误吞、跳过 owner Admit。

    ★ 变异:把 `heartbeat_sweep_tick` 里的 `OA.sweep_stale_owner_admitted(...)`
      整块移到 `if self.writer_fence is not None:` 之后 → 本用例变红。
    """
    repo = FakeRepo()
    uc = make_uc(repo=repo, fence=FakeFence(held=False))
    stale_key = "dead-uid|1"
    fresh_key = "live-uid|2"
    now = bz.time.monotonic()
    uc.owner_admitted[stale_key] = now - OWNER_ADMITTED_STALE_TTL_SEC - 1
    uc.owner_admitted[fresh_key] = now

    await uc.heartbeat_sweep_tick(bz._SweepState())

    assert stale_key not in uc.owner_admitted
    assert fresh_key in uc.owner_admitted
    # 非写者:一步权威动作都不该发生。
    assert repo.advance_calls == 0
    assert repo.list_shards_calls == 0


async def test_tick_pause_and_resume_log_only_on_transition() -> None:
    """失主 / 复位各只打一条,且失主时**立即返回**不跑任何权威动作。

    每 5s 刷一条"我不是写者"会把 Loki 灌爆,真正的失主事件反而被淹没;而失主副本
    继续跑 reconcile 就是双写者并发(存储级 fence 是最终防线,这里是快路径)。

    ★ 变异:把 `if state.was_writer:` 守卫删掉(无条件打日志)→ 第二次 tick 的
      "只打一条"断言变红;把 `return` 删掉 → `advance_calls == 0` 断言变红。
    """
    repo = FakeRepo()
    fence = FakeFence(held=False)
    uc = make_uc(repo=repo, fence=fence)
    state = bz._SweepState()

    with capture_logs() as logs:
        await uc.heartbeat_sweep_tick(state)
        await uc.heartbeat_sweep_tick(state)
    assert events(logs).count("hub_heartbeat_sweep_paused_not_writer") == 1
    assert repo.advance_calls == 0

    fence.held = True
    with capture_logs() as logs2:
        await uc.heartbeat_sweep_tick(state)
        await uc.heartbeat_sweep_tick(state)
    assert events(logs2).count("hub_heartbeat_sweep_resumed_writer") == 1


async def test_tick_sweeps_writer_fence_once_per_token() -> None:
    """同一届只推扫一次;换届必须重推。

    推扫是"把全部已知 pod 的 fence 抬到本届 token",幂等但要遍历所有 pod ——
    每 tick 都跑等于每 5s 全量扫一遍 Redis。反过来,换届不重推会让前任在继任者
    尚未触碰过的 {pod} slot 上继续写(§9.22 fencing 出洞)。

    ★ 变异:把 `if token != state.swept_token:` 改成 `if True:` → 第一段的
      `advance_calls == 1` 变红;把 `state.swept_token = token` 删掉 → 同上。
    """
    repo = FakeRepo()
    fence = FakeFence(held=True, token=7)
    uc = make_uc(repo=repo, fence=fence)
    state = bz._SweepState()

    with capture_logs() as logs:
        await uc.heartbeat_sweep_tick(state)
        await uc.heartbeat_sweep_tick(state)
    assert repo.advance_calls == 1
    assert one(logs, "hub_writer_fence_swept")["token"] == 7

    fence.token = 8
    await uc.heartbeat_sweep_tick(state)
    assert repo.advance_calls == 2


async def test_tick_fence_sweep_failure_retries_next_tick() -> None:
    """推扫失败**不得**记为已完成,下个 tick 必须重试。

    记成已完成 = 这一届再也不会推扫,前任写者在未触碰 slot 上的写权永久保留。

    ★ 变异:把 `try/except/else` 的 `else:` 去掉(无论成败都写 swept_token)→
      `advance_calls == 2` 变红。
    """
    repo = FakeRepo()
    repo.advance_error = RuntimeError("redis down")
    uc = make_uc(repo=repo, fence=FakeFence(held=True, token=7))
    state = bz._SweepState()

    with capture_logs() as logs:
        await uc.heartbeat_sweep_tick(state)
        repo.advance_error = None
        await uc.heartbeat_sweep_tick(state)

    assert repo.advance_calls == 2
    assert one(logs, "hub_writer_fence_sweep_failed")["token"] == 7
    assert "hub_writer_fence_swept" in events(logs)


async def test_tick_steps_are_isolated_and_named_distinctly() -> None:
    """四个步骤互不阻断,且各自的失败事件名不同。

    它们之间没有依赖:cleanup 对账挂了不该让心跳超时扫描也停摆。事件名合并成一条
    则运维分不清是哪条链停了 —— 处置完全不同(查 owner 服务 vs 查 DS 心跳)。

    ★ 变异:把 for 循环里的 `try/except` 改成在循环外整体 try(一处失败即 break)
      → `sweep_once` / `reconcile_fleet_replicas` 的调用计数断言变红。
    """
    uc = make_uc()
    called: list[str] = []

    def boom(name: str):
        async def _f() -> None:
            called.append(name)
            raise RuntimeError(name + "-failed")

        return _f

    uc.reconcile_owner_cleanups = boom("cleanups")  # type: ignore[method-assign]
    uc.reconcile_shard_topology = boom("topology")  # type: ignore[method-assign]
    uc.sweep_once = boom("sweep")  # type: ignore[method-assign]
    uc.reconcile_fleet_replicas = boom("replicas")  # type: ignore[method-assign]

    with capture_logs() as logs:
        await uc.heartbeat_sweep_tick(bz._SweepState())

    assert called == ["cleanups", "topology", "sweep", "replicas"]
    got = events(logs)
    assert got == [
        "hub_owner_cleanup_reconcile_failed",
        "hub_reconcile_topology_failed",
        "hub_heartbeat_sweep_failed",
        "hub_reconcile_replicas_failed",
    ]


async def test_tick_propagates_cancelled_error() -> None:
    """步骤里的 CancelledError 必须穿透宽 except。

    `CancelledError` 继承 `BaseException`,被 `except BaseException` 吞掉就是
    "停机时这条循环退不出去"。

    ★ 变异:把步骤循环里的 `except asyncio.CancelledError: raise` 删掉 →
      `pytest.raises` 变红(异常被降级成一条 warning)。
    """
    uc = make_uc()

    async def cancelled() -> None:
        raise asyncio.CancelledError

    uc.reconcile_owner_cleanups = cancelled  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await uc.heartbeat_sweep_tick(bz._SweepState())


# ═══════════════════════════════════════════════════════════════════════════
# 4. reconcile_owner_cleanups:重启恢复
# ═══════════════════════════════════════════════════════════════════════════


async def test_reconcile_cleanups_resumes_matching_transfer_ref() -> None:
    """当前归属仍指名这个 source pod → 继续驱动清理相位。

    ★ 变异:把 `expected_source = assignment.transfer_source_hub_pod_name` 改成
      `assignment.hub_pod_name` → 本用例(source=POD_A、target=POD_B)变红。
    """
    repo = FakeRepo(
        assignment_rec=assignment(
            pod=POD_B, aid="assign-new", transfer_pending=True, transfer_source_pod=POD_A
        )
    )
    repo.cleanup_pods = [POD_A]
    repo.cleanups[POD_A] = [cleanup_ref()]
    uc = make_uc(repo=repo)
    resumed: list[tuple[int, str]] = []

    async def fake_resume(player_id: int, aid: str):
        resumed.append((player_id, aid))
        return repo.assignment, True

    uc.resume_assignment_cleanup = fake_resume  # type: ignore[method-assign]

    await uc.reconcile_owner_cleanups()

    assert resumed == [(PLAYER, "assign-new")]
    assert repo.removed_refs == []  # 仍在处理中,绝不摘索引


async def test_reconcile_cleanups_matches_release_phase_on_current_pod() -> None:
    """release 相位的 source 是归属自身的 `hub_pod_name`(不是 transfer_source)。

    ★ 变异:把 `elif assignment.release_cleanup_pending:` 分支的
      `expected_source = assignment.hub_pod_name` 改成空串 → 本用例会看到 ref
      被误摘,断言变红。
    """
    repo = FakeRepo(assignment_rec=assignment(pod=POD_A, aid="assign-new", release_pending=True))
    repo.cleanup_pods = [POD_A]
    repo.cleanups[POD_A] = [cleanup_ref()]
    uc = make_uc(repo=repo)
    resumed: list[str] = []

    async def fake_resume(player_id: int, aid: str):
        resumed.append(aid)
        return None, False

    uc.resume_assignment_cleanup = fake_resume  # type: ignore[method-assign]

    await uc.reconcile_owner_cleanups()
    assert resumed == ["assign-new"]
    assert repo.removed_refs == []


async def test_reconcile_cleanups_drops_orphan_when_assignment_advanced() -> None:
    """归属已被后继 assignment 取代 → 这条 ref 是孤儿,摘掉。

    不摘会让对账每 tick 都对一条永远推不动的 ref 重跑 `resume`,而 resume 对
    assignment_id 不符会抛 `ErrLocatorConflict` —— 变成一条自我复读的错误日志。

    ★ 变异:把 `if expected_source == "" or expected_source != source_pod:` 里的
      `expected_source == ""` 去掉 → 本用例(assignment_id 不匹配 → expected 为空)
      的 `removed_refs` 断言变红。
    """
    repo = FakeRepo(assignment_rec=assignment(aid="assign-newer", transfer_pending=True))
    repo.cleanup_pods = [POD_A]
    ref = cleanup_ref(aid="assign-new")
    repo.cleanups[POD_A] = [ref]
    uc = make_uc(repo=repo)

    await uc.reconcile_owner_cleanups()
    assert repo.removed_refs == [(POD_A, ref)]


async def test_reconcile_cleanups_drops_ref_pointing_at_another_source_pod() -> None:
    """source pod 与归属记录里的不一致 → 摘掉(不是本 pod 的活)。

    ★ 变异:把 `or expected_source != source_pod` 去掉 → `removed_refs` 变空,红。
    """
    repo = FakeRepo(
        assignment_rec=assignment(
            pod=POD_B, aid="assign-new", transfer_pending=True, transfer_source_pod="hub-z"
        )
    )
    repo.cleanup_pods = [POD_A]
    ref = cleanup_ref(aid="assign-new")
    repo.cleanups[POD_A] = [ref]
    uc = make_uc(repo=repo)

    await uc.reconcile_owner_cleanups()
    assert repo.removed_refs == [(POD_A, ref)]


async def test_reconcile_cleanups_keeps_ref_when_assignment_read_fails() -> None:
    """读权威失败 **≠** ref 陈旧:必须保留,并把错误上报给 tick。

    这是 §9.22「UNKNOWN 不得冒充 OFFLINE」在对账链上的形态。摘掉之后就**再没有人**
    回来清这个旧 owner —— 玩家在两台 DS 各有一份 owner,永久。

    ★ 变异:把 `get_assignment` 的 `except` 分支从 `continue` 改成
      `await self.remove_transfer_cleanup_ref(...)` → `removed_refs == []` 变红。
    """
    repo = FakeRepo()
    repo.get_assignment_error = RuntimeError("redis timeout")
    repo.cleanup_pods = [POD_A]
    repo.cleanups[POD_A] = [cleanup_ref()]
    uc = make_uc(repo=repo)

    with pytest.raises(RuntimeError):
        await uc.reconcile_owner_cleanups()
    assert repo.removed_refs == []


async def test_reconcile_cleanups_continues_other_pods_and_raises_first_error() -> None:
    """一个 pod 的列举失败不阻断其它 pod;最终抛出**第一个**错误。

    ★ 变异:把 `list_transfer_cleanups` 的 `except` 分支从 `continue` 改成
      `raise` → 第二个 pod 的 ref 不会被处理,`removed_refs` 断言变红。
    """
    repo = FakeRepo(assignment_rec=None)
    repo.cleanup_pods = ["hub-broken", POD_A]
    ref = cleanup_ref(aid="assign-new")
    repo.cleanups[POD_A] = [ref]

    boom = RuntimeError("scan failed")
    original = repo.list_transfer_cleanups

    async def flaky(source_pod: str):
        if source_pod == "hub-broken":
            raise boom
        return await original(source_pod)

    repo.list_transfer_cleanups = flaky  # type: ignore[method-assign]
    uc = make_uc(repo=repo)

    with pytest.raises(RuntimeError) as ei:
        await uc.reconcile_owner_cleanups()
    assert ei.value is boom
    assert repo.removed_refs == [(POD_A, ref)]


# ═══════════════════════════════════════════════════════════════════════════
# 5. sweep_once:心跳超时(§9 不变量 4)
# ═══════════════════════════════════════════════════════════════════════════


async def test_sweep_once_marks_stale_ready_shard_draining_and_removes_active() -> None:
    """陈旧分片 → draining + 移出 active 索引 + 结构化告警。

    对应 Go 的 `TestSweepOnce_MarksStaleDraining`。移出 active 是"停止分配"的实际
    动作:只改 state 不移索引,下一轮 sweep 还会把它当陈旧项重扫一遍。

    ★ 变异:把 `await self.repo.remove_active(pod)` 删掉 → `removed_active` 断言变红。
    """
    s = shard(POD_A)
    repo = FakeRepo([s])
    repo.stale = [POD_A]
    uc = make_uc(repo=repo)

    with capture_logs() as logs:
        await uc.sweep_once()

    assert s.state == STATE_DRAINING
    assert repo.removed_active == [POD_A]
    rec = one(logs, "hub_shard_heartbeat_timeout")
    assert rec["pod"] == POD_A
    assert rec["ds_pod"] == POD_A
    assert rec["reason"] == "heartbeat_timeout"
    assert rec["timeout"] == "30s"
    assert rec["stale_total"] == 1


async def test_sweep_once_does_not_downgrade_non_ready_state() -> None:
    """已经 stopping / draining 的分片不被"降级"回 draining 覆盖掉停机意图。

    `stopping` 是"孤儿 pod 自杀"指令的载体;被 sweep 改回 draining 后 DS 只会收到
    drain 指令,永远不停机 —— 留下一台没有分片归属却还在跑的 Hub。

    ★ 变异:把 `_mark_draining` 里的 `if s.state == STATE_READY:` 守卫删掉 → 变红。
    """
    s = shard(POD_A, state=STATE_STOPPING)
    repo = FakeRepo([s])
    repo.stale = [POD_A]
    uc = make_uc(repo=repo)

    await uc.sweep_once()
    assert s.state == STATE_STOPPING


async def test_sweep_once_tolerates_missing_shard_mirror() -> None:
    """镜像已经没了(`ErrHubNoAvailable`)不是错误:本来就要停掉它,不刷告警。

    ★ 变异:把 `if errcode.as_code(exc) != errcode.ErrHubNoAvailable:` 守卫删掉 →
      `sweep_mark_draining_failed` 会出现,断言变红。
    """
    repo = FakeRepo([])  # 镜像不存在 → update_shard_with_lock 抛 ErrHubNoAvailable
    repo.stale = [POD_A]
    uc = make_uc(repo=repo)

    with capture_logs() as logs:
        await uc.sweep_once()

    assert "sweep_mark_draining_failed" not in events(logs)
    assert repo.removed_active == [POD_A]  # 仍然移出 active


async def test_sweep_once_threshold_uses_heartbeat_timeout() -> None:
    """阈值 = now - heartbeat_timeout。写成 `now` 会把**刚刚**心跳过的分片全标死。

    ★ 变异:把 `threshold = L.now_ms() - self.heartbeat_max_age_ms()` 改成
      `L.now_ms()` → 差值断言变红。
    """
    repo = FakeRepo([])
    uc = make_uc(repo=repo, cfg=make_cfg(heartbeat_timeout="45s"))
    before = L.now_ms()
    await uc.sweep_once()
    delta = before - repo.stale_threshold
    assert 45_000 <= delta <= 45_000 + 2_000


async def test_sweep_once_raises_when_range_fails() -> None:
    """列举失败必须上抛给 tick(否则这一轮静默什么也没扫,而日志全绿)。

    ★ 变异:把 `range_stale_shards` 的调用包进 try/except 吞掉 → 变红。
    """
    repo = FakeRepo([])
    repo.stale_error = RuntimeError("zrange failed")
    uc = make_uc(repo=repo)
    with pytest.raises(RuntimeError):
        await uc.sweep_once()


# ═══════════════════════════════════════════════════════════════════════════
# 6. 扩容
# ═══════════════════════════════════════════════════════════════════════════


async def test_auto_scale_requires_both_switch_and_scaler() -> None:
    """开关开了但 fleet 不实现 scaler → 视为未启用。

    Mock / Local provider **刻意不实现** `HubFleetScaler`;若这里只看开关,整套
    reconcile 会对假分片跑强制整合(把不存在的玩家"迁移"到不存在的 pod)。

    ★ 变异:把 `auto_scale_enabled` 的 `and self.scaler is not None` 去掉 → 变红。
    """
    assert make_uc(cfg=make_cfg(autoscale_enabled=True), fleet=FakeFleet()).auto_scale_enabled() is False
    assert make_uc(cfg=make_cfg(autoscale_enabled=False), fleet=FakeScalerFleet()).auto_scale_enabled() is False
    assert make_uc(cfg=make_cfg(autoscale_enabled=True), fleet=FakeScalerFleet()).auto_scale_enabled() is True


async def test_try_scale_out_adds_one_and_clamps_to_max() -> None:
    """无容量兜底扩容 +1,并夹到 [min, max];已在 max 时不发无效请求。

    ★ 变异:把 `if desired == current: return` 删掉 → 第二段的 `sets == []` 变红。
    """
    fleet = FakeScalerFleet(replicas=3)
    uc = make_uc(fleet=fleet, cfg=make_cfg(autoscale_enabled=True, min_replicas=1, max_replicas=5))
    with capture_logs() as logs:
        await uc.try_scale_out_on_no_capacity("global")
    assert fleet.sets == [4]
    rec = one(logs, "hub_scaleout_triggered")
    assert rec["from"] == 3
    assert rec["to"] == 4

    fleet.replicas = 5
    fleet.sets.clear()
    await uc.try_scale_out_on_no_capacity("global")
    assert fleet.sets == []


async def test_try_scale_out_swallows_scaler_errors() -> None:
    """扩容失败只告警。把"没容量"升级成另一种错误码毫无意义 —— 客户端处置是同一个。

    ★ 变异:把 `get_fleet_replicas` 的 `except` 分支从 `return` 改成 `raise` →
      `pytest.raises` 之外的正常返回断言变红。
    """
    fleet = FakeScalerFleet()
    fleet.get_error = RuntimeError("agones down")
    uc = make_uc(fleet=fleet, cfg=make_cfg(autoscale_enabled=True))
    with capture_logs() as logs:
        await uc.try_scale_out_on_no_capacity("global")
    assert one(logs, "hub_scaleout_get_replicas_failed")["region"] == "global"

    fleet.get_error = None
    fleet.set_error = RuntimeError("patch rejected")
    with capture_logs() as logs2:
        await uc.try_scale_out_on_no_capacity("global")
    rec = one(logs2, "hub_scaleout_set_replicas_failed")
    assert rec["current"] == 3
    assert rec["desired"] == 4


# ═══════════════════════════════════════════════════════════════════════════
# 7. reconcile_fleet_replicas
# ═══════════════════════════════════════════════════════════════════════════


async def test_reconcile_scales_out_by_ceiling_and_returns_before_draining() -> None:
    """`ceil(total/players_per_hub)` 大于当前副本数 → 立即扩容并**当轮不再缩**。

    扩完立刻又跑排空会在同一 tick 里一边加 pod 一边搬玩家,新 pod 还没 ready。

    ★ 变异:把扩容分支末尾的 `return` 删掉 → `list_shards_calls`(排空会重读)
      与 draining 断言变红。
    """
    s1 = shard(POD_A, shard_id=1, count=400)
    s2 = shard(POD_B, shard_id=2, count=400)
    repo = FakeRepo([s1, s2])
    fleet = FakeScalerFleet(replicas=1)
    uc = make_uc(
        repo=repo,
        fleet=fleet,
        cfg=make_cfg(autoscale_enabled=True, consolidation_enabled=True, players_per_hub=500),
    )

    with capture_logs() as logs:
        await uc.reconcile_fleet_replicas()

    assert fleet.sets == [2]  # ceil(800/500) = 2
    rec = one(logs, "hub_fleet_scaled_out")
    assert rec["players"] == 800
    assert rec["players_per_hub"] == 500
    assert s1.state == STATE_READY and s2.state == STATE_READY


async def test_reconcile_consolidation_migrates_players_and_keeps_unknown_fields() -> None:
    """强制整合:最空分片被排空,玩家搬到目标,**未知字段一字不丢**。

    对应 Go 的 `TestReconcile_ConsolidationMigratesPlayers`。未知字段那条断言是
    §9 不变量 17 的执行检查:滚动升级期旧副本 read-modify-write 丢掉新副本写入的
    字段,是**静默**的数据损坏 —— 只能靠这种断言发现。

    ★ 变异:把 `migrate_player` 里的 `R.clone(assign)` 改成
      `hubpb.HubAssignmentStorageRecord()` 后逐字段赋值 → 未知字段断言变红。
    """
    s_a = shard(POD_A, shard_id=1, count=1)
    s_b = shard(POD_B, shard_id=2, count=2)
    repo = FakeRepo([s_a, s_b])
    repo.members[POD_A] = [PLAYER]
    repo.members[POD_B] = [1002, 1003]
    rec = assignment(pod=POD_A, shard_id=1)
    # 手工拼一个 field 2046 / wire type 2 的未知字段:tag=(2046<<3)|2=16370 → varint b"\xf2\x7f"。
    # ★ 先断言它真的被解析成 unknown field —— 否则线格式拼错时本用例会"绿着"什么也没验。
    raw = rec.SerializeToString() + b"\xf2\x7f\x06future"
    parsed = hubpb.HubAssignmentStorageRecord()
    parsed.ParseFromString(raw)
    assert len(unknown_fields.UnknownFieldSet(parsed)) == 1, "构造的未知字段没生效"
    repo.assignment = parsed

    pusher = FakePusher()
    fleet = FakeScalerFleet(replicas=2)
    uc = make_uc(
        repo=repo,
        fleet=fleet,
        pusher=pusher,
        cfg=make_cfg(autoscale_enabled=True, consolidation_enabled=True, players_per_hub=500),
    )

    await uc.reconcile_fleet_replicas()

    assert s_a.state == STATE_DRAINING
    assert s_a.draining_since_ms > 0
    assert repo.assignment.hub_pod_name == POD_B
    assert len(unknown_fields.UnknownFieldSet(repo.assignment)) == 1
    assert len(pusher.pushed) == 1
    assert (POD_B, PLAYER) in repo.member_added
    assert (POD_A, PLAYER) in repo.member_removed
    assert fleet.sets == []  # 回收恒 0 → 绝不缩容


async def test_reconcile_zero_players_drains_empty_surplus_and_stamps() -> None:
    """总在线=0:保留 shard_id 最小的 `min_replicas` 个,其余标 draining **并盖戳**。

    对应 Go 的 `TestReconcile_ZeroPlayersDrainsEmptySurplusForReclaim`。不盖戳的
    draining 分片会被回收逻辑跳过、又被 sweep 每 tick 续 TTL,成为永久 stale 镜像,
    并把过期 `hub_ds_addr` 发给重登玩家。

    ★ 变异:把 `drain_and_migrate` 的 `_mark` 里 `s.draining_since_ms = now` 删掉
      → 盖戳断言变红。
    """
    s1 = shard("hub-1", shard_id=1)
    s2 = shard("hub-2", shard_id=2)
    s3 = shard("hub-3", shard_id=3)
    repo = FakeRepo([s1, s2, s3])
    uc = make_uc(
        repo=repo,
        fleet=FakeScalerFleet(replicas=3),
        cfg=make_cfg(autoscale_enabled=True, consolidation_enabled=True, min_replicas=1),
    )

    await uc.reconcile_fleet_replicas()

    assert s1.state == STATE_READY
    for s in (s2, s3):
        assert s.state == STATE_DRAINING
        assert s.draining_since_ms > 0


async def test_reconcile_never_scales_in_even_long_past_grace() -> None:
    """逻辑 grace 到期**不构成**物理拆机证明:镜像保留、Fleet 不缩。

    对应 Go 的 `TestReconcile_LogicalGraceCannotErasePhysicalOwnerFence` /
    `TestReconcile_KeepsDrainedShardWithinGrace`。这是 §16.10 的正面示范 ——
    到期后不假设成功,只留痕。

    ★ 变异:把 `reclaim_drained_shards` 的 `return 0` 改成 `return reclaimed`
      (并在循环里计数)→ `fleet.sets == []` 变红。
    """
    old = shard("hub-old", shard_id=1, state=STATE_DRAINING, draining_since_ms=1, uid="uid-old")
    young = shard(
        "hub-young", shard_id=2, state=STATE_DRAINING, draining_since_ms=L.now_ms()
    )
    repo = FakeRepo([old, young])
    fleet = FakeScalerFleet(replicas=3)
    uc = make_uc(
        repo=repo,
        fleet=fleet,
        cfg=make_cfg(autoscale_enabled=True, consolidation_enabled=True, migrate_grace_seconds=30),
    )

    with capture_logs() as logs:
        await uc.reconcile_fleet_replicas()

    assert fleet.sets == []
    assert len(repo.shards) == 2
    rec = one(logs, "hub_scalein_waiting_exact_instance_teardown")
    assert rec["pod"] == "hub-old"
    assert rec["gameserver_uid"] == "uid-old"


async def test_reconcile_negative_min_replicas_does_not_drain_everything() -> None:
    """负 `min_replicas` 只记 WARN,**绝不**把整个 fleet 排空。

    Go 在 `ready[keep:]` 上会切片越界 panic(等于每轮白跑);Python 的负索引不报错,
    而是从尾部反向取 —— 那会把"配置写错了"变成"把全部空分片标 draining"。
    conf.py 刻意保留负值以暴露配置错误,不是让它生效。

    ★ 变异:把 `drain_empty_shards` 里 `if keep < 0:` 那段删掉 → 三个分片全被标
      draining,断言变红。
    """
    s1 = shard("hub-1", shard_id=1)
    s2 = shard("hub-2", shard_id=2)
    repo = FakeRepo([s1, s2])
    uc = make_uc(
        repo=repo,
        fleet=FakeScalerFleet(replicas=2),
        cfg=make_cfg(autoscale_enabled=True, consolidation_enabled=True, min_replicas=-1),
    )

    with capture_logs() as logs:
        await uc.reconcile_fleet_replicas()

    assert s1.state == STATE_READY
    assert s2.state == STATE_READY
    assert one(logs, "hub_drain_empty_skipped_negative_keep")["keep"] == -1


async def test_consolidate_once_orders_by_count_then_shard_id() -> None:
    """并列人数按 `shard_id` 定序;`list_shards` 的 Redis SET 顺序不可依赖。

    不定序 → 每个 tick 排空不同的分片,谁也排不空,而且日志上看每轮都"在整合"。

    ★ 变异:把排序键从 `(s.player_count, s.shard_id)` 改成 `s.player_count` →
      顺序随输入列表漂移,本用例(故意乱序输入)变红。
    """
    s3 = shard("hub-3", shard_id=3, count=1)
    s1 = shard("hub-1", shard_id=1, count=1)
    s2 = shard("hub-2", shard_id=2, count=9)
    repo = FakeRepo([s3, s1, s2])
    uc = make_uc(repo=repo, fleet=FakeScalerFleet(), cfg=make_cfg(autoscale_enabled=True))

    drained: list[str] = []

    async def fake_drain(s) -> bool:
        drained.append(s.hub_pod_name)
        return True

    uc.drain_and_migrate = fake_drain  # type: ignore[method-assign]

    assert await uc.consolidate_once(repo.shards, 1) is True
    # 保留最满的 1 个(hub-2),排空另外两个,顺序按 (count, shard_id)。
    assert drained == ["hub-1", "hub-3"]


async def test_consolidate_once_skips_negative_need() -> None:
    """负 `need`(来自负 `min_replicas`)fail-closed:不排空任何分片。

    ★ 变异:删掉 `if need < 0:` 那段 → `drained` 非空,断言变红。
    """
    repo = FakeRepo([shard("hub-1", shard_id=1, count=1)])
    uc = make_uc(repo=repo, fleet=FakeScalerFleet(), cfg=make_cfg(autoscale_enabled=True))
    drained: list[str] = []

    async def fake_drain(s) -> bool:
        drained.append(s.hub_pod_name)
        return True

    uc.drain_and_migrate = fake_drain  # type: ignore[method-assign]

    with capture_logs() as logs:
        assert await uc.consolidate_once(repo.shards, -1) is False
    assert drained == []
    assert one(logs, "hub_consolidate_skipped_negative_need")["need"] == -1


# ═══════════════════════════════════════════════════════════════════════════
# 8. drain_and_migrate
# ═══════════════════════════════════════════════════════════════════════════


async def test_drain_and_migrate_honours_batch_limit() -> None:
    """单 tick 每分片最多搬 `consolidation_batch` 人,余下留下个 tick。

    没有上限时一次 drain 会对上千玩家同步跑完整 saga,把 sweep tick 拖到几十秒 ——
    期间心跳超时扫描完全停摆。

    ★ 变异:把 `if moved >= batch: break` 删掉 → `migrated` 长度断言变红。
    """
    src = shard(POD_A, shard_id=1, count=5)
    dst = shard(POD_B, shard_id=2, count=0)
    repo = FakeRepo([src, dst])
    repo.members[POD_A] = [1, 2, 3, 4, 5]
    uc = make_uc(repo=repo, cfg=make_cfg(consolidation_batch=2))
    migrated: list[int] = []

    async def fake_migrate(pid, from_shard, target) -> bool:
        migrated.append(pid)
        return True

    uc.migrate_player = fake_migrate  # type: ignore[method-assign]

    with capture_logs() as logs:
        assert await uc.drain_and_migrate(src) is True

    assert migrated == [1, 2]
    rec = one(logs, "hub_shard_draining")
    assert rec["members"] == 5
    assert rec["moved"] == 2


async def test_drain_and_migrate_warns_when_member_index_incomplete() -> None:
    """索引数少于在册人数 → 告警(这批老玩家只能靠 DS drain 心跳兜底)。

    ★ 变异:把 `if shard.player_count > 0 and len(members) < shard.player_count:`
      整段删掉 → 告警消失,断言变红。
    """
    src = shard(POD_A, shard_id=1, count=7)
    dst = shard(POD_B, shard_id=2)
    repo = FakeRepo([src, dst])
    repo.members[POD_A] = [1]
    uc = make_uc(repo=repo)
    uc.migrate_player = _always_true_migrate  # type: ignore[method-assign]

    with capture_logs() as logs:
        await uc.drain_and_migrate(src)

    rec = one(logs, "drain_members_index_incomplete")
    assert rec["indexed"] == 1
    assert rec["player_count"] == 7


async def test_drain_and_migrate_breaks_without_target() -> None:
    """没有可承接的目标分片 → 停止本轮搬迁(不是把玩家搬到自己身上)。

    ★ 变异:把 `if target is None: ... break` 改成 `continue` → 会对每个成员重复
      打 `drain_no_target`,`events.count` 断言变红。
    """
    src = shard(POD_A, shard_id=1, count=2)
    repo = FakeRepo([src])  # 只有源分片,`least_loaded` 排除自身后无候选
    repo.members[POD_A] = [1, 2]
    uc = make_uc(repo=repo)
    uc.migrate_player = _always_true_migrate  # type: ignore[method-assign]

    with capture_logs() as logs:
        assert await uc.drain_and_migrate(src) is True

    assert events(logs).count("drain_no_target") == 1
    assert one(logs, "hub_shard_draining")["moved"] == 0


async def test_drain_and_migrate_breaks_on_invalid_release_track() -> None:
    """源分片持久化轨道非法 → 停止搬迁(继续搬会把玩家甩到别的轨,§9.21)。

    ★ 变异:把 `except errcode.PandoraError: ... break` 改成 `track = "stable"`
      兜底 → `drain_invalid_release_track` 消失,断言变红。
    """
    src = shard(POD_A, shard_id=1, count=1, track="prod")
    dst = shard(POD_B, shard_id=2)
    repo = FakeRepo([src, dst])
    repo.members[POD_A] = [1]
    uc = make_uc(repo=repo)
    uc.migrate_player = _always_true_migrate  # type: ignore[method-assign]

    with capture_logs() as logs:
        assert await uc.drain_and_migrate(src) is True

    assert one(logs, "drain_invalid_release_track")["pod"] == POD_A
    assert one(logs, "hub_shard_draining")["moved"] == 0


async def test_drain_and_migrate_returns_true_when_topology_read_fails() -> None:
    """已标 draining 但读不到拓扑 → 返回 True(已排空动作已发生),搬迁留下个 tick。

    返回 False 会让 `reconcile_fleet_replicas` 认为"没排空过",从而不重读快照 ——
    但分片其实已经是 draining 了,状态与判断分叉。

    ★ 变异:把 `drain_list_shards_failed` 分支的 `return True` 改成 `return False`
      → 断言变红。
    """
    src = shard(POD_A, shard_id=1, count=1)
    repo = FakeRepo([src])
    repo.members[POD_A] = [1]
    uc = make_uc(repo=repo)

    repo.list_shards_error = RuntimeError("scan failed")

    with capture_logs() as logs:
        assert await uc.drain_and_migrate(src) is True
    assert src.state == STATE_DRAINING
    assert "drain_list_shards_failed" in events(logs)


async def _always_true_migrate(pid, from_shard, target) -> bool:
    return True


# ═══════════════════════════════════════════════════════════════════════════
# 9. migrate_player:每条失败分支的"源索引记账"
# ═══════════════════════════════════════════════════════════════════════════


async def test_migrate_player_keeps_member_when_assignment_read_fails() -> None:
    """读归属失败 **≠** 玩家已离开:必须保留源索引(§9.22 UNKNOWN ≠ OFFLINE)。

    删索引 = 该玩家永远退出 drain 扫描,唯一一次迁移通知永久丢失,分片永远排不空。

    ★ 变异:把 `drain_assignment_read_failed` 分支改成先
      `await self.remove_shard_member(...)` 再 return → `member_removed == []` 变红。
    """
    repo = FakeRepo([shard(POD_A), shard(POD_B, shard_id=2)])
    repo.get_assignment_error = RuntimeError("redis timeout")
    uc = make_uc(repo=repo)

    with capture_logs() as logs:
        assert await uc.migrate_player(PLAYER, repo.shards[0], repo.shards[1]) is False

    assert repo.member_removed == []
    assert one(logs, "drain_assignment_read_failed")["player_id"] == PLAYER


async def test_migrate_player_drops_member_when_assignment_gone() -> None:
    """归属确实不存在(玩家已下线)→ 清掉陈旧索引。

    ★ 变异:把 `if assign is None:` 分支的 `remove_shard_member` 删掉 → 索引永久
      残留,`member_removed` 断言变红。
    """
    repo = FakeRepo([shard(POD_A), shard(POD_B, shard_id=2)], assignment_rec=None)
    uc = make_uc(repo=repo)
    assert await uc.migrate_player(PLAYER, repo.shards[0], repo.shards[1]) is False
    assert repo.member_removed == [(POD_A, PLAYER)]


async def test_migrate_player_drops_member_when_assignment_moved_elsewhere() -> None:
    """归属已在**第三个** pod(玩家自己切走了)→ 纯陈旧索引,清掉。

    ★ 变异:把该分支的 `remove_shard_member` 改成 `add_shard_member` → 变红。
    """
    repo = FakeRepo(
        [shard(POD_A), shard(POD_B, shard_id=2)], assignment_rec=assignment(pod="hub-z")
    )
    uc = make_uc(repo=repo)
    assert await uc.migrate_player(PLAYER, repo.shards[0], repo.shards[1]) is False
    assert repo.member_removed == [(POD_A, PLAYER)]


async def test_migrate_player_renotifies_when_already_on_target() -> None:
    """归属已在 drain 目标上(上轮搬完但通知没发出去)→ **补发**,不是删索引。

    删索引 = 上一轮的搬迁成功了但玩家永远收不到迁移通知,只能等 Login 重签兜底。

    ★ 变异:把 `if target is not None and assign.hub_pod_name == target.hub_pod_name:`
      整个分支删掉(直接走下面的 remove_shard_member)→ `pusher.pushed` 变空,红。
    """
    src = shard(POD_A, shard_id=1)
    dst = shard(POD_B, shard_id=2)
    repo = FakeRepo([src, dst], assignment_rec=assignment(pod=POD_B, shard_id=2))
    pusher = FakePusher()
    uc = make_uc(repo=repo, pusher=pusher)

    assert await uc.migrate_player(PLAYER, src, dst) is True
    assert len(pusher.pushed) == 1
    assert repo.member_removed == [(POD_A, PLAYER)]  # 通知发出后才清源索引


async def test_migrate_player_renotify_keeps_member_when_push_fails() -> None:
    """补发失败 → 回加源索引,下个 tick 重扫重发。

    ★ 变异:把补发分支里 `if not await self.push_migrate(...)` 的
      `return await _keep_scanned()` 改成继续往下走 → `member_added` 断言变红。
    """
    src = shard(POD_A, shard_id=1)
    dst = shard(POD_B, shard_id=2)
    repo = FakeRepo([src, dst], assignment_rec=assignment(pod=POD_B, shard_id=2))
    pusher = FakePusher(error=RuntimeError("kafka down"))
    uc = make_uc(repo=repo, pusher=pusher)

    assert await uc.migrate_player(PLAYER, src, dst) is False
    assert repo.member_added == [(POD_A, PLAYER)]
    assert repo.member_removed == []


async def test_migrate_player_happy_path_moves_seat_and_members() -> None:
    """正常搬迁:目标占座 +1、源退座 -1、成员索引搬家、通知发出。

    ★ 变异:把 `await self.release_assignment_seat(assign)` 删掉 → 源分片计数
      不减,`src.player_count` 断言变红(座位泄漏 = 几小时后分片假满)。
    """
    src = shard(POD_A, shard_id=1, count=3)
    dst = shard(POD_B, shard_id=2, count=1)
    repo = FakeRepo([src, dst], assignment_rec=assignment(pod=POD_A, shard_id=1))
    pusher = FakePusher()
    uc = make_uc(repo=repo, pusher=pusher)

    assert await uc.migrate_player(PLAYER, src, dst) is True
    assert dst.player_count == 2
    assert src.player_count == 2
    assert repo.member_added == [(POD_B, PLAYER)]
    assert repo.member_removed == [(POD_A, PLAYER)]
    assert repo.assignment.hub_pod_name == POD_B
    assert repo.assignment.assignment_id != "assign-old"  # 新 assignment id
    assert repo.assignment.role_id == 55  # 选角镜像随迁移搬走
    assert len(pusher.pushed) == 1


async def test_migrate_player_compensates_seat_when_cas_lost() -> None:
    """CAS 输给并发写者 → 退掉刚占的目标座位,源分片一个字节不动。

    不补偿 = 目标分片凭空多出一个永远不会有人来坐的座位。

    ★ 变异:把 `if not swapped:` 分支的 `compensate_reserved_seat` 删掉 →
      `dst.player_count` 断言变红。
    """
    src = shard(POD_A, shard_id=1, count=3)
    dst = shard(POD_B, shard_id=2, count=1)
    repo = FakeRepo([src, dst], assignment_rec=assignment(pod=POD_A, shard_id=1))
    repo.cas_results = [False]
    uc = make_uc(repo=repo)

    assert await uc.migrate_player(PLAYER, src, dst) is False
    assert dst.player_count == 1  # 占了又退回去
    assert src.player_count == 3  # 源侧未动
    assert repo.member_removed == []


async def test_migrate_player_readds_source_member_when_push_fails() -> None:
    """搬迁已落地但通知发布失败 → 回加源索引,下个 tick 补发。

    把"发布失败"静默当作已送达,玩家就再也收不到迁移通知,只能等 grace 到点被踢。

    ★ 变异:把末尾 `if not await self.push_migrate(...)` 那段删掉(改成无条件
      `return True`)→ `member_added` 里不会有源 pod,断言变红。
    """
    src = shard(POD_A, shard_id=1, count=3)
    dst = shard(POD_B, shard_id=2, count=1)
    repo = FakeRepo([src, dst], assignment_rec=assignment(pod=POD_A, shard_id=1))
    uc = make_uc(repo=repo, pusher=FakePusher(error=RuntimeError("kafka down")))

    assert await uc.migrate_player(PLAYER, src, dst) is False
    assert (POD_A, PLAYER) in repo.member_added


async def test_migrate_player_compensates_when_source_revision_mint_fails() -> None:
    """铸不出来源版本 → 补偿座位并放弃本轮(带 0 走下去会被 owner 按 legacy 拒)。

    对应 INC-20260818-003。`source_revision` 是 owner 判"这条 assignment 有多新"的
    唯一依据;drain 出来的记录带 0 而该玩家水位已非零 → owner 拒 → 玩家卡在旧 Pod。

    ★ 变异:把 `new_assign.source_revision = migrate_revision` 那两行连同 try 一起
      删掉 → `source_revision` 与补偿断言变红。
    """
    src = shard(POD_A, shard_id=1, count=3)
    dst = shard(POD_B, shard_id=2, count=1)
    repo = FakeRepo([src, dst], assignment_rec=assignment(pod=POD_A, shard_id=1))
    # 持有 fence 但铸号器在这一届耗尽 → mint 抛错。
    uc = make_uc(repo=repo, fence=FakeFence(held=False))

    with capture_logs() as logs:
        assert await uc.migrate_player(PLAYER, src, dst) is False

    assert dst.player_count == 1  # 座位已退
    assert repo.assignment.hub_pod_name == POD_A  # 归属未动
    assert one(logs, "drain_source_revision_mint_failed")["player_id"] == PLAYER


async def test_migrate_player_stamps_legacy_revision_without_fence() -> None:
    """未启用写者租约(dev / 单副本)→ 领 `LEGACY` 号,不阻断搬迁。

    ★ 变异:把 `mint_source_revision` 的 `LEGACY` 分支改成抛错 → 本用例变红
      (dev 环境的强制整合会整体停摆)。
    """
    src = shard(POD_A, shard_id=1, count=1)
    dst = shard(POD_B, shard_id=2, count=0)
    repo = FakeRepo([src, dst], assignment_rec=assignment(pod=POD_A, shard_id=1))
    uc = make_uc(repo=repo)

    assert await uc.migrate_player(PLAYER, src, dst) is True
    assert repo.assignment.source_revision == srev.LEGACY


async def test_migrate_player_keeps_member_when_signing_fails() -> None:
    """签票失败 → 补偿座位、归属不动、源索引保留。

    ★ 变异:把 `migrate_sign_ticket_failed` 分支的 `compensate_reserved_seat`
      删掉 → 目标分片计数断言变红。
    """
    src = shard(POD_A, shard_id=1, count=1)
    dst = shard(POD_B, shard_id=2, count=0)
    repo = FakeRepo([src, dst], assignment_rec=assignment(pod=POD_A, shard_id=1))
    uc = make_uc(repo=repo, signer=FakeSigner(error=RuntimeError("kms down")))

    with capture_logs() as logs:
        assert await uc.migrate_player(PLAYER, src, dst) is False

    assert dst.player_count == 0
    assert repo.assignment.hub_pod_name == POD_A
    assert repo.member_removed == []
    assert one(logs, "migrate_sign_ticket_failed")["player_id"] == PLAYER


# ═══════════════════════════════════════════════════════════════════════════
# 10. push_migrate / reclaim / 成员索引 / sum_players
# ═══════════════════════════════════════════════════════════════════════════


async def test_push_migrate_true_when_pusher_absent() -> None:
    """未装配 pusher = 功能关闭,视为"通知路径已走完"(由 drain 心跳 + Login 兜底)。

    返回 False 会让每个玩家的搬迁都被判成失败并无限回加源索引 —— 没接 Kafka 的
    部署会陷入永不收敛的 drain 循环。

    ★ 变异:把 `if self.migrate is None: return True` 改成 `return False` → 变红。
    """
    uc = make_uc(pusher=None)
    assert await uc.push_migrate(PLAYER, shard(POD_A), shard(POD_B), "tok") is True


async def test_push_migrate_carries_exact_reconnect_payload() -> None:
    """事件字段逐个对齐:客户端拿它重连,少一个就连不上。

    ★ 变异:把 `to_hub_ds_addr=target.hub_addr` 改成 `from_shard.hub_addr` →
      客户端会重连回正在排空的那台,断言变红。
    """
    pusher = FakePusher()
    uc = make_uc(pusher=pusher, cfg=make_cfg(migrate_grace_seconds=45))
    src = shard(POD_A, shard_id=1)
    dst = shard(POD_B, shard_id=2)

    assert await uc.push_migrate(PLAYER, src, dst, "tok-xyz") is True
    pid, payload = pusher.pushed[0]
    assert pid == PLAYER
    ev = hubpb.HubMigrateEvent()
    ev.ParseFromString(payload)
    assert ev.player_id == PLAYER
    assert ev.from_hub_pod == POD_A
    assert ev.to_hub_ds_addr == dst.hub_addr
    assert ev.to_hub_ticket == "tok-xyz"
    assert ev.to_hub_pod_name == POD_B
    assert ev.to_shard_id == 2
    assert ev.grace_seconds == 45
    assert ev.reason == "consolidation"
    assert ev.ts_ms > 0


async def test_push_migrate_false_on_publish_error() -> None:
    """真实发布失败必须回 False(调用方据此回加源索引重发)。

    ★ 变异:把 `migrate_push_failed` 分支的 `return False` 改成 `return True`
      → 变红(而线上表现是"迁移通知静默丢失",没有任何告警)。
    """
    uc = make_uc(pusher=FakePusher(error=RuntimeError("kafka down")))
    with capture_logs() as logs:
        assert await uc.push_migrate(PLAYER, shard(POD_A), shard(POD_B), "tok") is False
    assert one(logs, "migrate_push_failed")["player_id"] == PLAYER


async def test_reclaim_only_warns_for_empty_drained_past_grace() -> None:
    """只有"draining + 空 + 已盖戳 + 过 grace"才留痕;其余静默。且恒返回 0。

    ★ 变异:把 `if now - s.draining_since_ms < grace_ms: continue` 删掉 →
      grace 内的分片也会告警,`events.count` 断言变红。
    """
    now = L.now_ms()
    past = shard("hub-past", shard_id=1, state=STATE_DRAINING, draining_since_ms=1)
    inside = shard("hub-inside", shard_id=2, state=STATE_DRAINING, draining_since_ms=now)
    populated = shard(
        "hub-busy", shard_id=3, state=STATE_DRAINING, count=1, draining_since_ms=1
    )
    unstamped = shard("hub-nostamp", shard_id=4, state=STATE_DRAINING)
    ready = shard("hub-ready", shard_id=5)
    uc = make_uc(cfg=make_cfg(migrate_grace_seconds=30))

    with capture_logs() as logs:
        assert await uc.reclaim_drained_shards([past, inside, populated, unstamped, ready]) == 0

    hits = [e for e in logs if e["event"] == "hub_scalein_waiting_exact_instance_teardown"]
    assert [h["pod"] for h in hits] == ["hub-past"]


async def test_shard_member_helpers_swallow_errors() -> None:
    """成员索引是 best-effort:失败只 Warn,绝不回滚一笔**已提交**的归属置换。

    ★ 变异:把 `add_shard_member` 的 `except` 分支改成 `raise` → 变红
      (线上表现:一次 Redis 抖动把已提交的迁移整个回滚)。
    """
    repo = FakeRepo()
    repo.add_member_error = RuntimeError("redis down")
    uc = make_uc(repo=repo)

    with capture_logs() as logs:
        await uc.add_shard_member(POD_A, PLAYER)
    assert one(logs, "add_shard_member_failed")["pod"] == POD_A

    async def boom(pod: str, player_id: int) -> None:
        raise RuntimeError("srem failed")

    repo.remove_shard_member = boom  # type: ignore[method-assign]
    with capture_logs() as logs2:
        await uc.remove_shard_member(POD_A, PLAYER)
    assert one(logs2, "remove_shard_member_failed")["player_id"] == PLAYER


async def test_sum_players_ignores_non_positive_counts() -> None:
    """负数按 0 计:原样累加会把总量算小,进而让 `need` 偏低、触发不该发生的排空。

    ★ 变异:把 `if s.player_count > 0:` 守卫删掉 → 断言变红。
    """
    negative = shard("hub-neg", shard_id=1)
    negative.player_count = -5
    assert bz.sum_players([shard("hub-1", count=3), negative, shard("hub-2", count=7)]) == 10
    assert bz.sum_players([]) == 0
