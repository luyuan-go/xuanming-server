"""`pandorapy/services/ds_allocator/biz_base.py` 回归测试。

对应 Go 侧 `services/battle/ds_allocator/internal/biz/allocator.go` 第 1–546 行;
每条断言的行为口径以那份 Go 源码为准,不以 Python 实现"看起来该怎样"为准。

## 本文件盯死的六件事

  ① **词表逐字符**。`STATE_*` 是 Redis 里 `BattleStorageRecord.state` 的 wire 值,
     `COMMAND_STOP` 是发给 UE DS 的控制指令字面量,`REASON_*` 是运维面板的聚合键。
     两栈并存期任一侧改一个字母都不会报错,只会让一半流量从面板上消失、或让
     一局对局永远没人推进。
  ② **`enable_redis_authority` 的九项前置一项都不能少**(§9.3 / §9.22)。
     半开启状态(配置说 redis authority、实际回退 legacy)的表现是
     `resolve_battle_target` 恒返回 `ErrUnavailable` —— 玩家重连被静默退化成回大厅,
     而运维看到的是"服务健康"。所以**每一项各写一个独立用例**:合并成一条参数化时,
     删掉其中任意一格判据仍会绿。
  ③ **严格写档必须在两个 battle 写视图上都真正激活**,且激活失败时**不得**置
     `model_b = True`(否则 Model-B RPC 已可见、存储不变量却没上闸)。
  ④ **`resolve_battle_target` 是只读且 fail-closed 的**:权威读失败原样上抛
     (§9.22 UNKNOWN 不得冒充 OFFLINE),投影缺任一 exact 绑定字段一律拒签,
     玩家不在名单里一律拒 —— 三条各自一个用例。
  ⑤ **fire-and-forget 的三条纪律**:命名 task(§AGENTS 硬性要求 12)、有界超时
     (§9 不变量 19/20)、`CancelledError` 不被宽 except 吞掉。
  ⑥ **投影函数不臆造字段**。`owner_target_from_allocate_result` 必须留
     `source_revision=0`("没有版本"),不是 1("最小版本")。

## 依赖策略:不碰 Redis

本模块是**纯业务基座**,唯一的外部依赖是 `auth_repo.read_authority()` 的返回值形状。
`BattleAuthoritySnapshot.ready_authorized()` 的十条判据有 `tests/test_ds_allocator_battle_auth.py`
专门覆盖,这里用一个只回放 `(ready, reason)` 的替身 —— 在这里再搭一遍 Redis 授权记录,
测的仍是那边的逻辑,却会让本文件因为 Redis 抖动变红。

★ 每条用例 docstring 的 `★ 变异:` 一行都**真跑过**(改坏 → 红 → 改回 → 绿),不是照代码猜的。
"""

from __future__ import annotations

import asyncio

import pytest
from pandora.config.v1 import level_pb2 as configpb
from pandora.ds.v1 import allocator_pb2 as dspb

from pandorapy import errcode, releasetrack
from pandorapy.services.ds_allocator import biz_base as B
from pandorapy.services.ds_allocator import repo as R
from pandorapy.services.ds_allocator.clients import OwnerTargetView
from pandorapy.services.ds_allocator.conf import AllocatorConf

# ── 常量 ────────────────────────────────────────────────────────────────────

MATCH_ID = 700_000_000_000_000_001
PLAYER_ID = 800_000_000_000_000_002
POD = "pandora-battle-7f9c-abcde"
UID = "gs-uid-A"
ALLOC_ID = "5a1f1b7e-4a1e-4a3c-9f4e-2b7c1d8e6a05"
DS_ADDR = "10.244.1.7:7777"


# ── 替身 ────────────────────────────────────────────────────────────────────


class _Snapshot:
    """`BattleAuthoritySnapshot` 的最小替身:只回放 `(ready, reason)` 与 `.battle`。"""

    def __init__(self, battle, ready: bool = True, reason: str = "") -> None:  # noqa: ANN001
        self.battle = battle
        self._ready = ready
        self._reason = reason
        self.ready_calls: list[tuple[int, int]] = []

    def ready_authorized(self, now_ms_value: int, max_heartbeat_age_ms: int) -> tuple[bool, str]:
        self.ready_calls.append((now_ms_value, max_heartbeat_age_ms))
        return self._ready, self._reason


class _AuthRepo:
    """只读权威替身。记录调用次数,用于断言 `resolve_battle_target` 的**零写入**。"""

    def __init__(self, snapshot: _Snapshot | None = None, err: BaseException | None = None) -> None:
        self._snapshot = snapshot
        self._err = err
        self.read_calls: list[int] = []

    async def read_authority(self, match_id: int):  # noqa: ANN201
        self.read_calls.append(match_id)
        if self._err is not None:
            raise self._err
        return self._snapshot


class _ModelBAuthRepo(_AuthRepo):
    """完整能力的 Model-B 授权仓替身(abort + lifecycle + strict 三件套齐全)。"""

    def __init__(self, strict_works: bool = True) -> None:
        super().__init__()
        self._strict_works = strict_works
        self._strict = False
        self.enable_calls = 0

    async def fence_allocation_abort_expected(self, *a, **kw):  # noqa: ANN002,ANN003,ANN201
        raise AssertionError("not used in biz_base tests")

    async def read_allocation_abort(self, *a, **kw):  # noqa: ANN002,ANN003,ANN201
        raise AssertionError("not used in biz_base tests")

    async def complete_allocation_abort_expected(self, *a, **kw) -> bool:  # noqa: ANN002,ANN003
        raise AssertionError("not used in biz_base tests")

    async def record_allocation_lifecycle_published(self, *a, **kw) -> None:  # noqa: ANN002,ANN003
        raise AssertionError("not used in biz_base tests")

    def enable_strict_model_b_writes(self) -> None:
        self.enable_calls += 1
        if self._strict_works:
            self._strict = True

    def strict_model_b_writes_enabled(self) -> bool:
        return self._strict


class _BattleRepo:
    """canonical battle 仓替身:实现 active-index 重建 + 台账 + 严格写档三种能力。"""

    def __init__(self, strict_works: bool = True) -> None:
        self._strict_works = strict_works
        self._strict = False
        self.enable_calls = 0

    async def reconcile_battle_active_index(self, count: int) -> None:
        raise AssertionError("not used in biz_base tests")

    async def record_allocation_ledger(self, allocation_id: str, at_ms: int) -> None:
        raise AssertionError("not used in biz_base tests")

    async def allocation_ledger_contains(self, allocation_id: str) -> bool:
        raise AssertionError("not used in biz_base tests")

    async def prune_allocation_ledger(self, before_ms: int) -> int:
        raise AssertionError("not used in biz_base tests")

    def enable_strict_model_b_writes(self) -> None:
        self.enable_calls += 1
        if self._strict_works:
            self._strict = True

    def strict_model_b_writes_enabled(self) -> bool:
        return self._strict


class _PlainRepo:
    """legacy / 测试替身:三种能力一个都不实现(Go 侧 type assertion 全部落空)。"""


class _LocalAllocator:
    """mode=local / mock 分配器:只有 `allocate` / `release`。"""

    def __init__(self) -> None:
        self.released: list[str] = []
        self.release_exc: BaseException | None = None
        self.release_hang = False

    async def allocate(self, *a, **kw):  # noqa: ANN002,ANN003,ANN201
        raise AssertionError("not used in biz_base tests")

    async def release(self, pod_name: str) -> None:
        if self.release_hang:
            await asyncio.sleep(3600)
        self.released.append(pod_name)
        if self.release_exc is not None:
            raise self.release_exc


class _AgonesAllocator(_LocalAllocator):
    """Agones 分配器:额外实现 Model-B 权威分配 + 孤儿 GS 对账两组能力。"""

    async def allocate_authoritative(self, *a, **kw):  # noqa: ANN002,ANN003,ANN201
        raise AssertionError("not used in biz_base tests")

    async def deliver_credential(self, *a, **kw) -> str:  # noqa: ANN002,ANN003
        raise AssertionError("not used in biz_base tests")

    async def resolve_expected_pod_uid(self, *a, **kw) -> str:  # noqa: ANN002,ANN003
        raise AssertionError("not used in biz_base tests")

    async def release_expected(self, allocation) -> None:  # noqa: ANN001
        raise AssertionError("not used in biz_base tests")

    async def list_allocated_game_servers(self) -> list:
        raise AssertionError("not used in biz_base tests")

    async def delete_allocated_game_server_exact(
        self, name: str, uid: str, expected_allocation_id: str
    ) -> bool:
        raise AssertionError("not used in biz_base tests")


class _Signer:
    def sign_battle_credential(self, *a, **kw):  # noqa: ANN002,ANN003,ANN201
        raise AssertionError("not used in biz_base tests")


class _Pusher:
    async def publish_lifecycle(self, evt) -> None:  # noqa: ANN001
        raise AssertionError("not used in biz_base tests")


class _LogRecorder:
    """把 `plog.get()` 换成录音机 —— 用于断言“某条路径**没有**打日志”。

    为什么不用 caplog:`pandorapy.log` 走 structlog 的独立管道,并不必然接到
    stdlib logging 上;换掉 `get` 是唯一不依赖 structlog 配置现状的做法。
    """

    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    def debug(self, event: str, **kw) -> None:  # noqa: ANN003
        self.events.append(("debug", event))

    def info(self, event: str, **kw) -> None:  # noqa: ANN003
        self.events.append(("info", event))

    def warning(self, event: str, **kw) -> None:  # noqa: ANN003
        self.events.append(("warning", event))

    def error(self, event: str, **kw) -> None:  # noqa: ANN003
        self.events.append(("error", event))


@pytest.fixture
def logs(monkeypatch) -> _LogRecorder:  # noqa: ANN001
    recorder = _LogRecorder()
    monkeypatch.setattr(B.plog, "get", lambda **_kw: recorder)
    return recorder


# ── 构造辅助 ────────────────────────────────────────────────────────────────


def _cfg(**overrides) -> AllocatorConf:  # noqa: ANN003
    base = {"heartbeat_timeout": "15s", "battle_ttl": "2h", "ready_wait_timeout": "10s"}
    base.update(overrides)
    return AllocatorConf(**base)


def _battle(**overrides) -> dspb.BattleStorageRecord:  # noqa: ANN003
    """一份**完整合法**的 canonical 投影:每个用例只打坏其中一格。"""
    rec = dspb.BattleStorageRecord(
        match_id=MATCH_ID,
        ds_pod_name=POD,
        ds_addr=DS_ADDR,
        state=B.STATE_RUNNING,
        player_ids=[PLAYER_ID, PLAYER_ID + 1],
        gameserver_uid=UID,
        instance_epoch=3,
        allocation_id=ALLOC_ID,
        release_track=releasetrack.STABLE,
        allocated_at_ms=1_700_000_000_000,
        last_heartbeat_ms=1_700_000_005_000,
    )
    for key, value in overrides.items():
        setattr(rec, key, value)
    return rec


def _ready_usecase(battle=None, ready: bool = True, reason: str = ""):  # noqa: ANN001,ANN201
    """一台已开 Model B 的 usecase(直接置字段,不跑 `enable_redis_authority` 的九项闸)。"""
    u = B.AllocatorUsecaseBase(_BattleRepo(), _AgonesAllocator(), _cfg())
    snapshot = _Snapshot(_battle() if battle is None else battle, ready=ready, reason=reason)
    u.auth_repo = _AuthRepo(snapshot)
    u.model_b = True
    return u, snapshot


def _model_b_ready() -> tuple[B.AllocatorUsecaseBase, _BattleRepo, _ModelBAuthRepo]:
    """`enable_redis_authority` 的**全绿**装配(各用例在此基础上只打坏一项)。"""
    battle_repo = _BattleRepo()
    u = B.AllocatorUsecaseBase(battle_repo, _AgonesAllocator(), _cfg())
    return u, battle_repo, _ModelBAuthRepo()


# ── ① 词表逐字符 ───────────────────────────────────────────────────────────


def test_state_constants_are_shared_with_repo_not_re_typed():
    """五个 allocation_* 状态必须与 `repo.py` **同源**,不是各抄一份字面量。

    守的是什么:这些串是 Redis 里 `BattleStorageRecord.state` 的 wire 值,同时也是
    `repo.active_index_required()` / `battle_auth.battle_terminal()` 的判据。放行
    (两处各写一份字面量、其中一处被改)的后果不是报错,而是
    `active_index_required` 抛 unknown state —— 那一局永远没人推进它。

    ★ 变异:把 `biz_base.py` 的 `STATE_ALLOCATION_UNCERTAIN = R.BATTLE_STATE_ALLOCATION_UNCERTAIN`
      改成 `STATE_ALLOCATION_UNCERTAIN = "allocation_uncertain "`(尾随一个空格)→ 本条红。
    """
    assert B.STATE_ALLOCATION_UNCERTAIN == R.BATTLE_STATE_ALLOCATION_UNCERTAIN
    assert B.STATE_ALLOCATION_RECONCILING == R.BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING
    assert B.STATE_ALLOCATION_EMPTY_FENCE == R.BATTLE_STATE_ALLOCATION_RECONCILE_EMPTY_TOMBSTONE
    assert B.STATE_PREACTIVE_RELEASING == R.BATTLE_STATE_PREACTIVE_RELEASE_PENDING
    assert B.STATE_ALLOCATION_ABORT == R.BATTLE_STATE_ALLOCATION_ABORT_PENDING


def test_wire_literals_match_go():
    """裸字面量状态 / 指令逐字符对齐 Go。

    守的是什么:`COMMAND_STOP` 是 DS 侧按**字符串比对**的控制指令 —— 改一个字母
    等于孤儿 DS 再也收不到停机通知,幽灵进程一直占着监听端口污染下一局。
    `STATE_*` 五项同理是 Redis wire 值。

    ★ 变异:把 `STATE_WARMING = "warming"` 改成 `"warmup"` → 本条红。
    """
    assert B.STATE_ALLOCATING == "allocating"
    assert B.STATE_WARMING == "warming"
    assert B.STATE_READY == "ready"
    assert B.STATE_RUNNING == "running"
    assert B.STATE_ENDED == "ended"
    assert B.STATE_ABANDONED == "abandoned"
    assert B.COMMAND_NONE == ""
    assert B.COMMAND_STOP == "stop"


def test_resolve_reason_vocabulary():
    """只读重连查询的五个 reason 值逐字符对齐 Go(infra.md §11.3 R2 固定词表)。

    守的是什么:运维照着这些串建 Loki / Grafana 查询。两栈并存期任一侧改一个字母,
    面板就只统计到另一半的量,而"少了一半"本身没有任何告警。

    ★ 变异:把 `REASON_RESOLVE_AUTHORITY_OFF = "read_only_authority_unavailable"`
      改成 `"readonly_authority_unavailable"` → 本条红。
    """
    assert B.REASON_RESOLVE_ARGS_INVALID == "resolve_args_invalid"
    assert B.REASON_RESOLVE_AUTHORITY_OFF == "read_only_authority_unavailable"
    assert B.REASON_RESOLVE_AUTHORITY_READ == "authority_read_failed"
    assert B.REASON_RESOLVE_NOT_AUTHORIZED == "target_not_authorized"
    assert B.REASON_RESOLVE_PROJECTION_INCOMPLETE == "target_projection_incomplete"


def test_sentinel_errors_do_not_shadow_each_other():
    """五个哨兵互不为父子类。

    守的是什么:`BattleWaitOwnershipLostError` 的语义是"回收所有权已属他人,你**不得**
    再执行 cleanup";`ReadyWaitTimeoutError` 的语义恰恰相反("超时了,去 cleanup")。
    一旦前者继承后者,`except ReadyWaitTimeoutError:` 就会顺手吞掉前者,于是 owner
    与 sweep 的 fenced 回收链并发跑第二路 release —— 而那条链只有幂等最终一致保证,
    没有单次调用保证。

    ★ 变异:把 `class BattleWaitOwnershipLostError(Exception)` 改成
      `class BattleWaitOwnershipLostError(ReadyWaitTimeoutError)` → 本条红。
    """
    sentinels = [
        B.HeartbeatTerminalError,
        B.HeartbeatPodMismatchError,
        B.HeartbeatAllocationFencedError,
        B.ReadyWaitTimeoutError,
        B.BattleWaitOwnershipLostError,
    ]
    for a in sentinels:
        for b in sentinels:
            if a is b:
                continue
            assert not issubclass(a, b), f"{a.__name__} 不该是 {b.__name__} 的子类"


# ── ② 构造期能力探测 ───────────────────────────────────────────────────────


def test_capability_detection_wires_each_capability_from_its_own_source():
    """三次能力探测各自认准自己的来源:index/ledger 看 repo,orphan 看 alloc。

    守的是什么:Go 的 `NewAllocatorUsecase` 是三条 type assertion,探测错来源不会
    报错 —— 只会让"孤儿 GameServer 清扫"或"派生索引重建"静默禁用,表现是 Pod 占位
    泄漏几小时没人回收。

    ★ 变异:把 `if isinstance(repo, BattleAllocationLedger)` 改成
      `if isinstance(alloc, BattleAllocationLedger)` → 本条红。
    """
    repo, alloc = _BattleRepo(), _AgonesAllocator()
    u = B.AllocatorUsecaseBase(repo, alloc, _cfg())
    assert u.active_index_reconciler is repo
    assert u.allocation_ledger is repo
    assert u.orphan_gs_reconciler is alloc


def test_capability_detection_disables_on_local_backend():
    """local / mock 后端与 legacy repo 下三种能力**全部**为 None(清扫自动禁用)。

    守的是什么:`OrphanGameServerReconciler` 探测宽松化(比如改成 `alloc is not None`)
    会让 local 分配器被误认成 Agones,于是清扫链拿着"没有 Allocated 概念"的后端去
    对账并删 Pod。

    ★ 变异:把 `if isinstance(alloc, OrphanGameServerReconciler):` 改成
      `if alloc is not None:` → 本条红。
    """
    u = B.AllocatorUsecaseBase(_PlainRepo(), _LocalAllocator(), _cfg())
    assert u.active_index_reconciler is None
    assert u.allocation_ledger is None
    assert u.orphan_gs_reconciler is None


def test_fresh_usecase_is_legacy_and_not_model_b():
    """刚构造的 usecase 必须是 legacy 档:model_b=False、无权威、无严格写档。

    守的是什么:`model_b` 的默认值若被写成 True,`resolve_battle_target` 会跳过
    "本副本未启用 Redis 权威"这道门,直接对 `auth_repo=None` 取属性 —— 一个本该
    返回 `ErrUnavailable`(客户端退避重查)的路径变成 500。

    ★ 变异:把 `self.model_b = False` 改成 `self.model_b = True` → 本条红。
    """
    u = B.AllocatorUsecaseBase(_BattleRepo(), _AgonesAllocator(), _cfg())
    assert u.model_b is False
    assert u.auth_repo is None
    assert u.ds_signer is None
    assert u.ds_credential_ttl_sec == 0.0
    assert u.lifecycle_required is False
    assert u.kill_orphan_on_stop is False


# ── ③ enable_redis_authority:九项前置各一个用例 ────────────────────────────


def test_enable_redis_authority_happy_path():
    """全绿装配:两个写视图都开严格档,字段全部落位,并**无条件**打开 lifecycle_required。

    守的是什么:`lifecycle_required` 是 §9 不变量 4 的补偿闸 —— Redis authority 下
    abandoned 必须能可靠发出 `ds.lifecycle`。Go 在这里**无条件**置 True 正是"避免
    调用方漏配";丢了这一行,一份漏配 kafka 的生产配置会照常启动,而 abandoned 事件
    默默进不了 battle_result,玩家段位不回滚。

    ★ 变异:把 `self.lifecycle_required = True`(方法最后一行)删掉 → 本条红。
    """
    u, battle_repo, auth_repo = _model_b_ready()
    signer = _Signer()
    u.enable_redis_authority(auth_repo, signer, 300.0)
    assert u.model_b is True
    assert u.auth_repo is auth_repo
    assert u.abort_repo is auth_repo
    assert u.lifecycle_proof_repo is auth_repo
    assert u.authoritative_alloc is u.alloc
    assert u.ds_signer is signer
    assert u.ds_credential_ttl_sec == 300.0
    assert u.lifecycle_required is True
    assert battle_repo.strict_model_b_writes_enabled() is True
    assert auth_repo.strict_model_b_writes_enabled() is True


def test_enable_redis_authority_opens_strict_gate_on_both_writers():
    """严格写档必须在 **battle 与 auth 两个**写视图上各开一次。

    守的是什么:两个视图写的是同一批 battle 记录(canonical + 授权投影)。只开一个
    的后果是另一条写路径继续按 legacy 记录形状写入,同一份记录的连续存储不变量在
    两条路径上不一致 —— 而两边都不报错。

    ★ 变异:把 `repo.enable_strict_model_b_writes()` 一行删掉 → 本条红。
    """
    u, battle_repo, auth_repo = _model_b_ready()
    u.enable_redis_authority(auth_repo, _Signer(), 300.0)
    assert battle_repo.enable_calls == 1
    assert auth_repo.enable_calls == 1


def test_enable_redis_authority_rejects_when_gate_did_not_activate():
    """严格写档 enable 后回读仍为 False → 必须拒绝启动,且**不得**置 model_b。

    守的是什么:这是"不可逆闸没真正合上"的唯一检测点。放行的后果是 Model-B RPC /
    worker 已经可见(model_b=True),而存储不变量没上闸 —— 半激活状态正是 Model B
    要消灭的东西。

    ★ 变异:把方法里第二道 `if not self.repo.strict_model_b_writes_enabled() or ...`
      整块删掉 → 本条红。
    """
    battle_repo = _BattleRepo(strict_works=False)
    u = B.AllocatorUsecaseBase(battle_repo, _AgonesAllocator(), _cfg())
    with pytest.raises(errcode.PandoraError) as ei:
        u.enable_redis_authority(_ModelBAuthRepo(), _Signer(), 300.0)
    assert ei.value.code == errcode.ErrInvalidState
    assert "strict storage write gate did not activate" in ei.value.msg
    assert u.model_b is False
    assert u.auth_repo is None


def test_enable_redis_authority_rejects_none_repo():
    """auth repo 为 None → 拒绝启动。

    守的是什么:Redis 是 Model B 的**唯一授权权威**,没有它就没有 Model B。

    ★ 变异:把 `repo is None` 从那个大 `if` 里删掉 → 本条红
      (会变成 AttributeError 而不是 PandoraError)。
    """
    u, _, _ = _model_b_ready()
    with pytest.raises(errcode.PandoraError) as ei:
        u.enable_redis_authority(None, _Signer(), 300.0)
    assert ei.value.code == errcode.ErrInvalidState
    assert u.model_b is False


def test_enable_redis_authority_rejects_none_signer():
    """凭据签发器为 None → 拒绝启动。

    守的是什么:DS 票据是进场权威的唯一不可伪造搬运通道(§9.3)。没有签发器却宣称
    开了 Model B,等于所有 DS 回调凭据都签不出来,而错误要等到第一次分配才暴露。

    ★ 变异:把 `signer is None` 从那个大 `if` 里删掉 → 本条红。
    """
    u, _, auth_repo = _model_b_ready()
    with pytest.raises(errcode.PandoraError) as ei:
        u.enable_redis_authority(auth_repo, None, 300.0)
    assert ei.value.code == errcode.ErrInvalidState
    assert u.model_b is False


@pytest.mark.parametrize("ttl", [0.0, -1.0])
def test_enable_redis_authority_rejects_non_positive_ttl(ttl: float):
    """凭据 TTL ≤ 0 → 拒绝启动。

    守的是什么:0 在 Redis 语义里是"永不过期" —— 一张永不过期的 DS 回调凭据等于
    §9.3 里那道"短 exp 是纯本地验票下唯一的吊销手段"彻底失效。负值同理荒谬。

    ★ 变异:把 `token_ttl_sec <= 0` 改成 `token_ttl_sec < 0` → ttl=0.0 一档红。
    """
    u, _, auth_repo = _model_b_ready()
    with pytest.raises(errcode.PandoraError) as ei:
        u.enable_redis_authority(auth_repo, _Signer(), ttl)
    assert ei.value.code == errcode.ErrInvalidState
    assert u.model_b is False


def test_enable_redis_authority_rejects_non_authoritative_allocator():
    """分配器不实现 `AuthoritativeGameServerAllocator` → 拒绝启动。

    守的是什么:Model B 要求"先取实例 UID/RV,Redis stage 成功后再条件 PATCH 投递
    annotation"。用普通 `Allocate` 的分配器顶上,等于 K8s annotation 变回授权权威,
    §9.22 的唯一权威约束当场打穿。

    ★ 变异:把 `not ok` 从那个大 `if` 里删掉 → 本条红。
    """
    u = B.AllocatorUsecaseBase(_BattleRepo(), _LocalAllocator(), _cfg())
    with pytest.raises(errcode.PandoraError) as ei:
        u.enable_redis_authority(_ModelBAuthRepo(), _Signer(), 300.0)
    assert ei.value.code == errcode.ErrInvalidState
    assert u.model_b is False


def test_enable_redis_authority_rejects_repo_without_abort_capability():
    """auth repo 缺 abort 日志能力 → 拒绝启动。

    守的是什么:abort 日志是 matchmaker 分配 saga 的补偿依据。缺了它,一次
    pre-admission abort 就无法精确 fence 那个 exact 实例,玩家会被留在一台
    没人负责回收的 DS 上。

    ★ 变异:把 `not abort_ok` 从那个大 `if` 里删掉 → 本条红。
    """

    class _NoAbort(_ModelBAuthRepo):
        fence_allocation_abort_expected = None  # 抹掉方法 → 协议探测落空

    u, _, _ = _model_b_ready()
    with pytest.raises(errcode.PandoraError) as ei:
        u.enable_redis_authority(_NoAbort(), _Signer(), 300.0)
    assert ei.value.code == errcode.ErrInvalidState
    assert u.model_b is False


def test_enable_redis_authority_rejects_repo_without_lifecycle_proof():
    """auth repo 缺 lifecycle ACK 见证能力 → 拒绝启动。

    守的是什么:那条能力是"为一个已拆除的 exact allocation 记下 Kafka ACK"的唯一
    权威。缺了它,§9 不变量 4 的补偿链没法证明自己投递成功,abandoned 记录要么被
    过早 Expire(补偿丢失),要么永久滞留。

    ★ 变异:把 `not lifecycle_proof_ok` 从那个大 `if` 里删掉 → 本条红。
    """

    class _NoProof(_ModelBAuthRepo):
        record_allocation_lifecycle_published = None

    u, _, _ = _model_b_ready()
    with pytest.raises(errcode.PandoraError) as ei:
        u.enable_redis_authority(_NoProof(), _Signer(), 300.0)
    assert ei.value.code == errcode.ErrInvalidState
    assert u.model_b is False


def test_enable_redis_authority_rejects_missing_active_index_reconciler():
    """battle repo 不能重建派生 active 索引 → 拒绝启动。

    守的是什么:active ZSET 是心跳超时扫描的入口。单独一次 ZSET 写丢失会让一个
    恢复墓碑永久搁浅;没有重建能力就没有兜底,那一局永远不会被补偿。

    ★ 变异:把 `self.active_index_reconciler is None` 从那个大 `if` 里删掉 → 本条红。
    """

    class _NoReconcile(_BattleRepo):
        reconcile_battle_active_index = None

    u = B.AllocatorUsecaseBase(_NoReconcile(), _AgonesAllocator(), _cfg())
    assert u.active_index_reconciler is None
    with pytest.raises(errcode.PandoraError) as ei:
        u.enable_redis_authority(_ModelBAuthRepo(), _Signer(), 300.0)
    assert ei.value.code == errcode.ErrInvalidState
    assert u.model_b is False


def test_enable_redis_authority_rejects_non_strict_battle_writer():
    """canonical battle 写视图不支持严格写档 → 拒绝启动(错误发生在**开闸之前**)。

    守的是什么:`self.repo` 与 `repo` 是**两个**写视图,少判其中任一个都会让另一条
    写路径继续按 legacy 记录形状写入。这里专门盯 battle 侧(auth 侧由
    `test_..._non_strict_auth_writer` 盯)。

    ★ 变异:把 `not battle_strict_ok` 从那个大 `if` 里删掉 → 本条红
      (会变成对 None 调用 `enable_strict_model_b_writes` 的 TypeError)。
    """

    class _NoStrict(_BattleRepo):
        enable_strict_model_b_writes = None
        strict_model_b_writes_enabled = None

    u = B.AllocatorUsecaseBase(_NoStrict(), _AgonesAllocator(), _cfg())
    with pytest.raises(errcode.PandoraError) as ei:
        u.enable_redis_authority(_ModelBAuthRepo(), _Signer(), 300.0)
    assert ei.value.code == errcode.ErrInvalidState
    assert u.model_b is False


def test_enable_redis_authority_rejects_non_strict_auth_writer():
    """授权写视图不支持严格写档 → 拒绝启动。

    守的是什么:同上,盯的是 auth 侧那一格。两条各写一个用例是刻意的 —— 合成一条时
    删掉其中任意一格判据仍会绿。

    ★ 变异:把 `not auth_strict_ok` 从那个大 `if` 里删掉 → 本条红。
    """

    class _NoStrict(_ModelBAuthRepo):
        enable_strict_model_b_writes = None
        strict_model_b_writes_enabled = None

    u, _, _ = _model_b_ready()
    with pytest.raises(errcode.PandoraError) as ei:
        u.enable_redis_authority(_NoStrict(), _Signer(), 300.0)
    assert ei.value.code == errcode.ErrInvalidState
    assert u.model_b is False


# ── ④ lifecycle publisher 启动门 ───────────────────────────────────────────


def test_validate_lifecycle_pusher_ready_blocks_only_when_required():
    """required 且无 publisher 才拒;两个"另一半"的组合都必须放行。

    守的是什么:这道门若写成 `if self.lifecycle is None`(丢掉 required 前件),
    dev / local-off 档(刻意不配 Kafka)会启动不起来 —— 把一道生产闸变成开发阻断,
    最终结果是有人把整道门删掉。

    ★ 变异:把 `if self.lifecycle_required and self.lifecycle is None:` 改成
      `if self.lifecycle is None:` → 本条红(第三段断言)。
    """
    u = B.AllocatorUsecaseBase(_BattleRepo(), _AgonesAllocator(), _cfg())
    u.set_lifecycle_pusher_required(True)
    with pytest.raises(errcode.PandoraError) as ei:
        u.validate_lifecycle_pusher_ready()
    assert ei.value.code == errcode.ErrInvalidState

    u.set_lifecycle_pusher(_Pusher())
    u.validate_lifecycle_pusher_ready()  # required + 有 publisher → 放行

    u2 = B.AllocatorUsecaseBase(_BattleRepo(), _AgonesAllocator(), _cfg())
    u2.validate_lifecycle_pusher_ready()  # 不 required + 无 publisher → 放行


# ── ⑤ kill_stranded_ds:fire-and-forget 三条纪律 ────────────────────────────


def _pending_kill_task() -> asyncio.Task:
    """按名字取回 `kill_stranded_ds` 刚 spawn 的后台任务。"""
    tasks = [t for t in asyncio.all_tasks() if t.get_name() == B.KILL_STRANDED_TASK_NAME]
    assert len(tasks) == 1, f"期望恰好一个命名任务,实际 {len(tasks)}"
    return tasks[0]


async def test_kill_stranded_ds_spawns_named_task_and_releases():
    """打开开关后:异步回收该 pod,且后台任务**有名字**。

    守的是什么:两件事。①local 模式的 UE DS 收到 stop 不自杀,不主动 kill 就会留下
    幽灵进程占着监听端口污染下一局。②裸 `asyncio.create_task` 的异常会被静默吞进
    Task,直到 GC 才打一条认不出主人的 "never retrieved" —— 名字是排障的唯一抓手
    (`panic_recovered{name=...}`)。

    ★ 变异:把 `safego.spawn(KILL_STRANDED_TASK_NAME, _run)` 改成
      `asyncio.create_task(_run())` → 本条红(任务名变成 "Task-N")。
    """
    alloc = _LocalAllocator()
    u = B.AllocatorUsecaseBase(_BattleRepo(), alloc, _cfg())
    u.set_kill_orphan_on_stop(True)
    u.kill_stranded_ds(MATCH_ID, POD, "orphan")
    await _pending_kill_task()
    assert alloc.released == [POD]


async def test_kill_stranded_ds_is_noop_when_switch_off():
    """Agones 模式(开关关闭)下必须是 no-op:不回收、不起任务。

    守的是什么:Agones 下孤儿 GameServer 由 Agones 生命周期回收。后端跟着删一遍的
    后果是 Redis 抖动误判 orphan 时**误删正在服务的 pod** —— 把一次误判升级成一局
    在场玩家被踢。

    ★ 变异:把 `if not self.kill_orphan_on_stop or pod_name == "":` 改成
      `if pod_name == "":` → 本条红。
    """
    alloc = _LocalAllocator()
    u = B.AllocatorUsecaseBase(_BattleRepo(), alloc, _cfg())
    u.kill_stranded_ds(MATCH_ID, POD, "orphan")
    await asyncio.sleep(0)
    assert alloc.released == []
    assert [t for t in asyncio.all_tasks() if t.get_name() == B.KILL_STRANDED_TASK_NAME] == []


async def test_kill_stranded_ds_is_noop_on_empty_pod():
    """pod 名为空 → no-op(不能拿空串去调编排层 Release)。

    守的是什么:空 pod 名不是"回收所有" —— 但真去调一次 Release("") 的行为完全取决于
    后端实现,而这条路径正来自"心跳里没带 pod 名"的异常输入。fail-closed 更安全。

    ★ 变异:把 `or pod_name == ""` 从那个 `if` 里删掉 → 本条红。
    """
    alloc = _LocalAllocator()
    u = B.AllocatorUsecaseBase(_BattleRepo(), alloc, _cfg())
    u.set_kill_orphan_on_stop(True)
    u.kill_stranded_ds(MATCH_ID, "", "orphan")
    await asyncio.sleep(0)
    assert alloc.released == []
    assert [t for t in asyncio.all_tasks() if t.get_name() == B.KILL_STRANDED_TASK_NAME] == []


async def test_kill_stranded_ds_release_failure_does_not_escape():
    """编排层 Release 失败只 Warn,不把异常抛回心跳主流程。

    守的是什么:回收是 best-effort 旁路。让它的失败冒泡到心跳,等于"清幽灵进程失败"
    把一台**健康** DS 的心跳也打挂,进而触发 §9 不变量 4 的判弃补偿。

    ★ 变异:把 `except BaseException as exc:` 块里的 `return` 改成 `raise` → 本条红。
    """
    alloc = _LocalAllocator()
    alloc.release_exc = RuntimeError("agones 5xx")
    u = B.AllocatorUsecaseBase(_BattleRepo(), alloc, _cfg())
    u.set_kill_orphan_on_stop(True)
    u.kill_stranded_ds(MATCH_ID, POD, "pod_mismatch")
    task = _pending_kill_task()
    await task
    assert task.exception() is None


async def test_kill_stranded_ds_does_not_swallow_cancellation():
    """`CancelledError` 必须穿透宽 except。

    守的是什么:`CancelledError` 继承 `BaseException`,被 `except BaseException` 吞掉
    的表现是**停机时这条任务不退出** —— 进程 SIGTERM 后挂在那里,滚动升级(§9 不变量 21)
    的排空阶段被一条 best-effort 旁路任务拖住。

    ★ 变异:把 `_run()` 里紧邻宽 except 之上的 `except asyncio.CancelledError: raise`
      两行删掉 → 本条红(任务变成正常完成,`cancelled()` 为 False)。
    """
    alloc = _LocalAllocator()
    alloc.release_exc = asyncio.CancelledError()
    u = B.AllocatorUsecaseBase(_BattleRepo(), alloc, _cfg())
    u.set_kill_orphan_on_stop(True)
    u.kill_stranded_ds(MATCH_ID, POD, "battle_abandoned")
    task = _pending_kill_task()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled() is True


async def test_kill_stranded_ds_has_bounded_timeout(monkeypatch):
    """编排层永久挂起时,后台任务必须在有界时间内自己结束(§9 不变量 19/20)。

    守的是什么:这条路径的常见触发场景正是"编排层已经不健康"。没有独立超时的话,
    每一次 stop 判定都会永久泄漏一个协程,压测几小时后 OOM;而它挂的又是 DS 心跳的
    尾部,拖起来还会连累心跳响应。

    ★ 变异:把 `async with asyncio.timeout(DETACHED_CLEANUP_TIMEOUT_SEC):` 一行删掉
      (`await self.alloc.release(pod_name)` 直接裸调)→ 本条红(0.2s 内不会结束,
      `wait_for` 抛 TimeoutError)。
    """
    monkeypatch.setattr(B, "DETACHED_CLEANUP_TIMEOUT_SEC", 0.05)
    alloc = _LocalAllocator()
    alloc.release_hang = True
    u = B.AllocatorUsecaseBase(_BattleRepo(), alloc, _cfg())
    u.set_kill_orphan_on_stop(True)
    u.kill_stranded_ds(MATCH_ID, POD, "auth_terminating")
    task = _pending_kill_task()
    await asyncio.wait_for(task, timeout=2.0)
    assert task.exception() is None
    assert alloc.released == []  # 挂死在 sleep 里,从未真正 release


# ── ⑥ accessor 单位 ────────────────────────────────────────────────────────


def test_duration_accessors_are_seconds_and_read_their_own_field():
    """两个 accessor 各读自己的配置字段,单位是**秒**。

    守的是什么:Python 传 float 谁也拦不住(Go 有 `time.Duration` 强类型)。
    `battle_ttl_sec` 误读成 ready_wait 的后果是战斗镜像 10 秒就过期 —— 一局刚开打
    权威记录就没了。

    ★ 变异:把 `battle_ttl_sec` 里的 `self.cfg.battle_ttl_td()` 改成
      `self.cfg.ready_wait_timeout_td()` → 本条红。
    """
    u = B.AllocatorUsecaseBase(_BattleRepo(), _AgonesAllocator(), _cfg())
    assert u.battle_ttl_sec() == 7200.0
    assert u.ready_wait_timeout_sec() == 10.0


def test_heartbeat_timeout_ms_truncates_toward_zero():
    """心跳阈值以**毫秒**返回,且亚毫秒部分**向零截断**(Go `Duration.Milliseconds()`)。

    守的是什么:这个值是 `ready_authorized()` 的新鲜度判据。①单位写错(忘了 ×1000)
    会让 15s 的阈值变成 15ms,每个玩家一重连就被判"心跳陈旧";②用 `round()` 会在
    亚毫秒配置上比 Go 多算 1ms,两栈在边界上给出相反结论。

    ★ 变异:把 `int(self.cfg.heartbeat_timeout_td() / _dt.timedelta(milliseconds=1))`
      改成 `round(...)` → 本条红(1500us 一档从 1 变成 2)。
    """
    assert B.AllocatorUsecaseBase(_BattleRepo(), _AgonesAllocator(), _cfg()).heartbeat_timeout_ms() == 15_000
    sub_ms = B.AllocatorUsecaseBase(
        _BattleRepo(), _AgonesAllocator(), _cfg(heartbeat_timeout="1500us")
    )
    assert sub_ms.heartbeat_timeout_ms() == 1


# ── ⑦ 投影函数 ─────────────────────────────────────────────────────────────


def test_allocate_result_from_battle_maps_every_field():
    """七个字段逐个从**同一份**权威快照投影,不许错位。

    守的是什么:`gameserver_uid` / `instance_epoch` / `allocation_id` 是 DSTicket v2
    的 exact 实例绑定。任意一格错位(比如 uid 抄成 pod 名)的表现不是报错,而是
    matchmaker 签出一张绑到"不存在的实例"的票,玩家连上 DS 后被以"票不是我的"踢掉。

    ★ 变异:把 `gameserver_uid=b.gameserver_uid` 改成 `gameserver_uid=b.ds_pod_name`
      → 本条红。
    """
    res = B.allocate_result_from_battle(_battle())
    assert res == B.AllocateResult(
        ds_addr=DS_ADDR,
        ds_pod_name=POD,
        allocated_at_ms=1_700_000_000_000,
        gameserver_uid=UID,
        instance_epoch=3,
        allocation_id=ALLOC_ID,
        release_track=releasetrack.STABLE,
    )


def test_allocate_result_from_battle_none_in_none_out():
    """None 进 None 出(对应 Go 的 `if b == nil { return nil }`)。

    守的是什么:调用点(`resolve_battle_target` 之外还有分配链)会在拿不到记录时
    传 None。少了这条判空,一次"记录已被回收"的正常竞态会变成 AttributeError。

    ★ 变异:把 `if b is None: return None` 两行删掉 → 本条红。
    """
    assert B.allocate_result_from_battle(None) is None


def test_owner_target_leaves_source_revision_unset():
    """五个身份字段照搬,`source_revision` 必须留 **0**。

    守的是什么:0 不是"最小版本"而是"**没有**版本" —— 它与任何非零 revision 都
    不可比(见 `pandorapy/source_revision.py` 的 LEGACY 哨兵)。battle 侧没有写者
    租约铸号链,凭空塞一个非零值等于给 owner 权威一个**假水位**,于是一个更旧的
    来源会被判成更新的。

    ★ 变异:在 `owner_target_from_allocate_result` 的返回里加 `source_revision=1`
      → 本条红。
    """
    target = B.owner_target_from_allocate_result(B.allocate_result_from_battle(_battle()))
    assert target == OwnerTargetView(
        pod_name=POD,
        instance_uid=UID,
        instance_epoch=3,
        assignment_or_allocation_id=ALLOC_ID,
        release_track=releasetrack.STABLE,
    )
    assert target.source_revision == 0


def test_owner_target_none_in_empty_view_out():
    """None 进 → **空视图**出(不是 None)。

    守的是什么:Go 返回的是零值结构体 `data.OwnerTargetView{}`,调用方直接拿它去做
    `current != target` 比较。改成返回 None 会让那个比较变成 `record != None` 恒真,
    于是每次对账都判成"归属变了"并触发一次多余的 owner 迁移。

    ★ 变异:把 `if res is None: return OwnerTargetView()` 改成 `return None` → 本条红。
    """
    assert B.owner_target_from_allocate_result(None) == OwnerTargetView()


# ── ⑧ resolve_battle_target:只读 + fail-closed ─────────────────────────────


@pytest.mark.parametrize(
    ("match_id", "player_id"),
    [(0, PLAYER_ID), (MATCH_ID, 0), (0, 0)],
)
async def test_resolve_rejects_zero_ids_without_touching_authority(match_id: int, player_id: int):
    """任一 ID 为 0 → `ErrInvalidArg`,且**一次权威读都不发**。

    守的是什么:零副作用。这条路径来自畸形/伪造请求,让它打到 Redis 等于给了
    未鉴权入参一条放大通道。

    ★ 变异:把入口 `if` 里的 `match_id == 0` 删掉 → `(0, PLAYER_ID)` 一档红
      (会去读权威,`read_calls` 不再为空)。
    """
    u, _ = _ready_usecase()
    with pytest.raises(errcode.PandoraError) as ei:
        await u.resolve_battle_target(match_id, player_id)
    assert ei.value.code == errcode.ErrInvalidArg
    assert u.auth_repo.read_calls == []


@pytest.mark.parametrize(
    ("match_id", "player_id"),
    [(1 << 64, PLAYER_ID), (MATCH_ID, 1 << 64), (-1, PLAYER_ID), (MATCH_ID, -1)],
)
async def test_resolve_rejects_out_of_uint64_range(match_id: int, player_id: int):
    """越出 uint64 值域的 ID → `ErrInvalidArg`(Python 整数不回绕,Go 会)。

    守的是什么:Go 的 uint64 形参让这类入参在编译期就不可能出现;Python 里它会一路
    飘到 protobuf 序列化才抛一个既没有业务码、也说不清是哪个字段的 ValueError ——
    而那已经是**发起调用之后**了。

    ★ 变异:把入口 `if` 里的 `match_id > _UINT64_MAX` 删掉 → `(1 << 64, PLAYER_ID)`
      一档红。
    """
    u, _ = _ready_usecase()
    with pytest.raises(errcode.PandoraError) as ei:
        await u.resolve_battle_target(match_id, player_id)
    assert ei.value.code == errcode.ErrInvalidArg
    assert u.auth_repo.read_calls == []


async def test_resolve_fails_closed_when_model_b_disabled():
    """本副本未开 Redis 权威 → `ErrUnavailable`(可退避重试),不是 NotFound。

    守的是什么:滚动共存期 legacy 副本必然走到这里。错误码必须是"可重试"语义 ——
    客户端据此退避重查并被路由到已滚动的副本;若返回 NotFound / PermissionDeny,
    客户端会把它当终态,玩家被退化成回大厅(§9.23 单一幂等进场链被打断)。

    ★ 变异:把 `if not self.model_b or self.auth_repo is None:` 改成
      `if self.auth_repo is None:` → 本条红。
    """
    u, _ = _ready_usecase()
    u.model_b = False
    with pytest.raises(errcode.PandoraError) as ei:
        await u.resolve_battle_target(MATCH_ID, PLAYER_ID)
    assert ei.value.code == errcode.ErrUnavailable
    assert u.auth_repo.read_calls == []


async def test_resolve_fails_closed_when_auth_repo_missing():
    """model_b=True 但 auth_repo 为 None(装配被改坏)→ 仍是 `ErrUnavailable`,不是崩。

    守的是什么:这是 `or self.auth_repo is None` 那一格。丢了它就是对 None 取属性,
    一个本该"退避重查"的路径变成 500。

    ★ 变异:把 `or self.auth_repo is None` 删掉 → 本条红(AttributeError)。
    """
    u, _ = _ready_usecase()
    u.auth_repo = None
    with pytest.raises(errcode.PandoraError) as ei:
        await u.resolve_battle_target(MATCH_ID, PLAYER_ID)
    assert ei.value.code == errcode.ErrUnavailable


async def test_resolve_propagates_authority_read_error_verbatim():
    """权威读失败**原样上抛**,不翻译成 PermissionDeny / 不吞成"没有目标"。

    守的是什么:§9.22 —— 查询失败必须是 UNKNOWN / UNAVAILABLE,不得冒充 OFFLINE 或
    任何默认状态。翻译成 PermissionDeny 会让客户端把一次 Redis 抖动当成"你不属这局"
    的终态,直接回大厅。

    ★ 变异:把 `except BaseException as exc:` 块末尾的 `raise` 改成
      `raise errcode.PandoraError(errcode.ErrPermissionDeny, "x")` → 本条红。
    """
    boom = RuntimeError("redis: connection reset")
    u, _ = _ready_usecase()
    u.auth_repo = _AuthRepo(err=boom)
    with pytest.raises(RuntimeError) as ei:
        await u.resolve_battle_target(MATCH_ID, PLAYER_ID)
    assert ei.value is boom


async def test_resolve_propagates_cancellation_without_logging_a_failure(logs: _LogRecorder):
    """权威读被取消 → `CancelledError` 穿透,且**不打** authority_read 告警。

    守的是什么:`CancelledError` 继承 `BaseException`。本函数的宽 except 末尾虽然也
    `raise`,但它会**先把取消当成一次权威读失败**打一条告警 —— 停机时每个在途
    请求刷一条,把真正的故障信号淹掉;而那些告警的 hint 还写着"客户端应退避重查",
    把一次正常停机描述成了权威不可用。

    ★ 变异:把 `resolve_battle_target` 里紧邻宽 except 之上的
      `except asyncio.CancelledError: raise` 两行删掉 → 本条红
      (宽 except 会记一条 `battle_target_refused`)。
    """
    u, _ = _ready_usecase()
    u.auth_repo = _AuthRepo(err=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await u.resolve_battle_target(MATCH_ID, PLAYER_ID)
    assert logs.events == []


async def test_resolve_rejects_when_not_ready_and_carries_ready_reason():
    """`ready_authorized` 判否 → `ErrPermissionDeny`,消息里必须带**具体判据**。

    守的是什么:`ready_authorized` 内部把近十条判据收敛成一个字符串 reason。丢掉它,
    "玩家重连被拒"只剩一个 PermissionDeny,分不清是对局没就绪(该重试)还是人不在
    名单里(该回大厅)—— 两者的正确客户端动作恰好相反。

    ★ 变异:把 `"battle target not authorized for reconnect (reason=%s)", reason`
      改成一个不带 reason 的固定串 → 本条红。
    """
    u, _ = _ready_usecase(ready=False, reason="heartbeat-stale")
    with pytest.raises(errcode.PandoraError) as ei:
        await u.resolve_battle_target(MATCH_ID, PLAYER_ID)
    assert ei.value.code == errcode.ErrPermissionDeny
    assert "heartbeat-stale" in ei.value.msg


async def test_resolve_rejects_player_outside_roster():
    """对局已就绪、但玩家不在 roster → 仍然拒。

    守的是什么:这是"能不能把这个玩家指回这台 DS"的**授权**判据,不是可选校验。
    放行等于任何知道 match_id 的人都能拿到一台正在打的 DS 的 exact 地址与
    allocation_id —— 重签票据的全部素材。

    ★ 变异:把 `if not ready or battle is None or not in_roster:` 里的
      `or not in_roster` 删掉 → 本条红。
    """
    u, _ = _ready_usecase()
    with pytest.raises(errcode.PandoraError) as ei:
        await u.resolve_battle_target(MATCH_ID, 999_999_999)
    assert ei.value.code == errcode.ErrPermissionDeny


async def test_resolve_rejects_when_battle_projection_missing():
    """ready=True 但 `snapshot.battle` 为 None → 拒,而且**不能在打日志时崩**。

    守的是什么:Go 的 `battle.GetState()` 对 nil 接收者返回零值,Python 的
    `battle.state` 会 AttributeError。少了那三处 `if battle is not None else ""`,
    这条**诊断日志本身**会把一个"该拒绝"变成 500。

    ★ 变异:把日志里的 `state=battle.state if battle is not None else ""` 改成
      `state=battle.state` → 本条红(AttributeError 而非 PandoraError)。
    """
    u = B.AllocatorUsecaseBase(_BattleRepo(), _AgonesAllocator(), _cfg())
    u.model_b = True
    u.auth_repo = _AuthRepo(_Snapshot(None, ready=True, reason=""))
    with pytest.raises(errcode.PandoraError) as ei:
        await u.resolve_battle_target(MATCH_ID, PLAYER_ID)
    assert ei.value.code == errcode.ErrPermissionDeny


@pytest.mark.parametrize(
    ("field", "broken"),
    [
        ("ds_addr", ""),
        ("ds_pod_name", ""),
        ("gameserver_uid", ""),
        ("instance_epoch", 0),
        ("allocation_id", ""),
    ],
)
async def test_resolve_rejects_incomplete_exact_binding(field: str, broken):  # noqa: ANN001
    """exact 实例绑定缺任一格 → `ErrUnavailable`(而不是把半份投影交出去)。

    守的是什么:重签 v2 战斗票要求五项绑定齐全。缺任一项时 matchmaker / login 都签
    不出票,玩家表现为"重连一直失败"却拿不到原因。逐格一个参数化档位是刻意的 ——
    合成一条时删掉其中任意一格判据仍会绿。

    ★ 变异:把 `or battle.instance_epoch == 0` 删掉 → `instance_epoch` 一档红。
    """
    u, _ = _ready_usecase(_battle(**{field: broken}))
    with pytest.raises(errcode.PandoraError) as ei:
        await u.resolve_battle_target(MATCH_ID, PLAYER_ID)
    assert ei.value.code == errcode.ErrUnavailable
    assert "projection incomplete" in ei.value.msg


@pytest.mark.parametrize("track", ["", "beta", "STABLE", "canary "])
async def test_resolve_rejects_invalid_release_track(track: str):
    """release_track 不是 `stable`/`canary` 二者之一 → 拒(fail-closed)。

    守的是什么:§9.21 要求同一对局固定 release track。放行一个未知轨道会让票据带着
    垃圾轨道值下发,DS 侧的轨道粘滞判定失效,玩家可能在 Stable / Canary 之间漂 ——
    大小写不同(`STABLE`)和带尾随空格(`canary `)都必须拒:它们与 Redis 里真正的
    键值并不相等。

    ★ 变异:把 `or not releasetrack.valid(battle.release_track)` 改成
      `or battle.release_track == ""` → `beta` / `STABLE` / `canary ` 三档红。
    """
    u, _ = _ready_usecase(_battle(release_track=track))
    with pytest.raises(errcode.PandoraError) as ei:
        await u.resolve_battle_target(MATCH_ID, PLAYER_ID)
    assert ei.value.code == errcode.ErrUnavailable


@pytest.mark.parametrize("track", [releasetrack.STABLE, releasetrack.CANARY])
async def test_resolve_returns_projection_and_reads_authority_once(track: str):
    """全绿路径:返回该 exact 实例的投影,且只读一次权威、不做任何写。

    守的是什么:两件事。①"只读"是这个 RPC 的全部定义 —— 任何"顺手刷一下 TTL /
    heartbeat / index"都会把一次重连查询变成一次归属变更。②新鲜度判据必须拿
    `heartbeat_timeout_ms()` 去问 `ready_authorized`,传 0 等于关掉整道心跳新鲜度门。

    ★ 变异:把 `snapshot.ready_authorized(now_ms(), self.heartbeat_timeout_ms())`
      的第二个实参改成 `0` → 本条红(最后一行断言)。
    """
    u, snapshot = _ready_usecase(_battle(release_track=track))
    res = await u.resolve_battle_target(MATCH_ID, PLAYER_ID)
    assert res.ds_addr == DS_ADDR
    assert res.ds_pod_name == POD
    assert res.gameserver_uid == UID
    assert res.instance_epoch == 3
    assert res.allocation_id == ALLOC_ID
    assert res.release_track == track
    assert u.auth_repo.read_calls == [MATCH_ID]
    assert [age for _, age in snapshot.ready_calls] == [15_000]


# ── ⑨ AllocateBattle 旧口径薄封装 ──────────────────────────────────────────


class _CapturingAllocateMixin:
    """替 `biz_allocate.AllocateMixin` 记下实参(本次移植范围不含那条链)。"""

    async def allocate_battle_with_combat_factions(self, *args):  # noqa: ANN002,ANN201
        self.captured = args
        return B.AllocateResult(ds_addr=DS_ADDR)


class _Assembled(_CapturingAllocateMixin, B.AllocatorUsecaseBase):
    pass


async def test_allocate_battle_delegates_with_legacy_defaults():
    """旧口径入口只补三个默认:无阵营快照、rating_mode=UNSPECIFIED、rating_pool 空。

    守的是什么:`rating_mode` 是"这一局算不算段位"的**唯一权威**,分配时定格、此后
    只读。这里若默认成 `LEVEL_RATING_MODE_ELO`,所有走旧口径入口的对局(dev / 兼容
    路径)都会被 battle_result 当成排位结算 —— 玩家段位被一局练习赛改写,且没有任何
    错误可观测。空 pool 同理:非空会把这些局记进一个不该有的段位池。

    ★ 变异:把 `configpb.LEVEL_RATING_MODE_UNSPECIFIED` 改成
      `configpb.LEVEL_RATING_MODE_ELO` → 本条红。
    """
    u = _Assembled(_BattleRepo(), _AgonesAllocator(), _cfg())
    res = await u.allocate_battle(MATCH_ID, [PLAYER_ID], 4002, "pvp")
    assert res.ds_addr == DS_ADDR
    assert u.captured == (
        MATCH_ID,
        [PLAYER_ID],
        None,
        4002,
        "pvp",
        configpb.LEVEL_RATING_MODE_UNSPECIFIED,
        "",
    )
