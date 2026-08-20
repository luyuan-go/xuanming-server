"""hub_allocator owner 租约三件套的行为测试 —— 对应 Go 侧
`internal/biz/owner_authority_test.go` + `internal/biz/owner_lease_test.go`。

被测的是 §9.22 那条"同一玩家最多只能在一个可玩 DS"防线在 hub 侧的三个落点:

    强 Begin        写不进权威就必须拒绝交付(fail-closed)
    census 弱兜底    幂等补 Admit + 漂移自愈,但**不能**打挂心跳
    租约双写门       migrate 弱 / contract 强两档

## 测试口径

不连真 owner 服务:本层是**搬运 + 判定**,权威判定在 owner 侧、已有它自己的测试。
这里用脚本化的假权威(`ScriptedOwnerAuthority`)精确制造 Go 测试里那些
"只在混版窗口/竞态里才出现"的返回:非 exact 回传、EPOCH_CONFLICT、屏障未开。

gRPC 客户端那几条用假 channel 直接喂 proto 响应 —— 目的不是测 grpc,
而是测**响应码 → 异常 + 随异常带回的证据**这层映射有没有丢东西。

每个用例的 `★ 变异:` 一行写明"把生产代码的哪一行改坏,本用例会红" ——
没有这一行的测试等于没验证过自己有没有牙。
"""

from __future__ import annotations

import asyncio
import time

import pytest
from pandora.common.v1 import errcode_pb2
from pandora.owner.v1 import owner_pb2 as ownerpb

from pandorapy import errcode, placement
from pandorapy import source_revision as srev
from pandorapy.services.hub_allocator import owner_lease as olease
from pandorapy.services.hub_allocator.owner_authority import (
    OWNER_ADMITTED_STALE_TTL_SEC,
    OWNER_PHASE_ADMITTED,
    OWNER_PHASE_PENDING,
    OWNER_TYPE_BATTLE,
    OWNER_TYPE_HUB,
    owner_admit_census_weak,
    owner_begin_player,
    owner_begin_player_guarded,
    owner_record_exactly_targets,
    sweep_stale_owner_admitted,
)
from pandorapy.services.hub_allocator.owner_lease_client import (
    GrpcOwnerLeaseRenewer,
    OwnerRecordView,
    OwnerTargetView,
)

POD = "hub-0"
UID = "uid-abc"
BUDGET = 1.0


def pending_record(pod: str, uid: str, epoch: int) -> OwnerRecordView:
    """对应 Go 测试的 pendingRecord。"""
    return OwnerRecordView(
        owner_epoch=epoch,
        owner_type=OWNER_TYPE_HUB,
        phase=OWNER_PHASE_PENDING,
        pod_name=pod,
        instance_uid=uid,
        instance_epoch=1,
        assignment_or_allocation_id="a1",
        release_track="stable",
        operation_id="op1",
    )


def target_of(record: OwnerRecordView) -> OwnerTargetView:
    return OwnerTargetView(
        pod_name=record.pod_name,
        instance_uid=record.instance_uid,
        instance_epoch=record.instance_epoch,
        assignment_or_allocation_id=record.assignment_or_allocation_id,
        release_track=record.release_track,
    )


def exact_record(
    target: OwnerTargetView, *, owner_type: int = OWNER_TYPE_HUB, epoch: int = 9
) -> OwnerRecordView:
    """造一份**逐格等于** target 的权威回传记录(正常成功路径)。"""
    return OwnerRecordView(
        owner_epoch=epoch,
        owner_type=owner_type,
        phase=OWNER_PHASE_PENDING,
        pod_name=target.pod_name,
        instance_uid=target.instance_uid,
        instance_epoch=target.instance_epoch,
        assignment_or_allocation_id=target.assignment_or_allocation_id,
        release_track=target.release_track,
        operation_id="op-minted",
    )


class ScriptedOwnerAuthority:
    """脚本化假权威。对应 Go 的 scriptedOwnerAuthority。"""

    def __init__(
        self,
        *,
        records: dict[int, OwnerRecordView] | None = None,
        query_err: BaseException | None = None,
        begin_err: BaseException | None = None,
        begin_err_for_player: int = 0,
        conflict_times: int = 0,
        begin_result: OwnerRecordView | None = None,
        admit_err: BaseException | None = None,
        admit_retry_after_ms: int = 0,
    ) -> None:
        self.records = records or {}
        self.query_err = query_err
        self.begin_err = begin_err
        self.begin_err_for_player = begin_err_for_player
        self.conflict_times = conflict_times
        self.begin_result = begin_result
        self.admit_err = admit_err
        self.admit_retry_after_ms = admit_retry_after_ms
        self.queries = 0
        self.begins = 0
        self.admits = 0
        self.begin_ops: list[str] = []
        self.begin_targets: list[OwnerTargetView] = []
        self.admit_targets: list[OwnerTargetView] = []
        self.admit_args: list[tuple[int, int, str]] = []

    async def query_owner(self, player_id: int) -> OwnerRecordView:
        self.queries += 1
        if self.query_err is not None:
            raise self.query_err
        return self.records.get(player_id, OwnerRecordView())

    async def begin_transition(
        self,
        player_id: int,
        expect_epoch: int,
        operation_id: str,
        owner_type: int,
        target: OwnerTargetView,
    ) -> OwnerRecordView:
        self.begins += 1
        self.begin_ops.append(operation_id)
        self.begin_targets.append(target)
        if self.conflict_times > 0:
            self.conflict_times -= 1
            err = errcode.PandoraError(errcode.ErrOwnerEpochConflict, "epoch conflict")
            err.current_record = self.records.get(player_id, OwnerRecordView())
            raise err
        if self.begin_err is not None and self.begin_err_for_player in (0, player_id):
            raise self.begin_err
        if self.begin_result is not None:
            return self.begin_result
        return exact_record(target, owner_type=owner_type)

    async def admit(
        self, player_id: int, owner_epoch: int, operation_id: str, target: OwnerTargetView
    ) -> int:
        self.admits += 1
        self.admit_args.append((player_id, owner_epoch, operation_id))
        self.admit_targets.append(target)
        if self.admit_err is not None:
            err = self.admit_err
            if isinstance(err, errcode.PandoraError):
                err.retry_after_ms = self.admit_retry_after_ms
            raise err
        return 0


class FakeLeaseRenewer:
    """对应 Go 的 fakeLeaseRenewer。"""

    def __init__(self, err: BaseException | None = None) -> None:
        self.err = err
        self.calls = 0
        self.pod = ""
        self.uid = ""
        self.epoch = 0
        self.track = ""

    async def renew_instance_lease(
        self, pod_name: str, instance_uid: str, instance_epoch: int, release_track: str
    ) -> None:
        self.calls += 1
        self.pod, self.uid, self.epoch, self.track = (
            pod_name,
            instance_uid,
            instance_epoch,
            release_track,
        )
        if self.err is not None:
            raise self.err


class _WarnRecorder:
    """替身 logger:只记 warning。

    census 的弱依赖分流(静默 WAIT vs 计失败告警)**只在日志上可观测** ——
    不录日志的话,"把正常迁移误计成失败"这种改坏法没有任何用例能发现。
    """

    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def warning(self, msg: str, **kw: object) -> None:
        self.events.append((msg, kw))

    def __getattr__(self, _name: str) -> object:  # info/debug/error 一律吞掉
        return lambda *_a, **_kw: None

    def names(self) -> list[str]:
        return [msg for msg, _ in self.events]


@pytest.fixture
def warns(monkeypatch: pytest.MonkeyPatch) -> _WarnRecorder:
    rec = _WarnRecorder()
    monkeypatch.setattr("pandorapy.log.get", lambda *_a, **_kw: rec)
    return rec


# ══════════════════════════════════════════════════════════════════════════════
# census 缓存剪枝
# ══════════════════════════════════════════════════════════════════════════════


async def test_census_cache_pruned_on_departure_then_readmits() -> None:
    """玩家离开本实例后缓存项必须被剪掉,回流时才会重新 Query→Admit。

    不剪枝的后果:玩家回流本实例时 owner epoch 已推进、记录是**新的** PENDING,
    但会被上一纪元留下的 admitted 缓存命中吞掉 —— 新纪元的 Admit 永远不提交,
    再入屏障那一边就一直等不到 ADMITTED。

    ★ 变异:把 owner_authority 剪枝循环里的 `admitted.pop(key, None)` 删掉 ——
      第三次 census 后 1001 的缓存项残留,第四次 admits 停在 2 而不是 3,本用例红。
    """
    auth = ScriptedOwnerAuthority(
        records={1001: pending_record(POD, UID, 5), 2002: pending_record(POD, UID, 6)}
    )
    admitted: dict[str, float] = {}

    await owner_admit_census_weak(auth, admitted, [1001], OWNER_TYPE_HUB, POD, UID, BUDGET)
    assert auth.admits == 1

    # 第二轮同一批人:命中缓存,不该再发 Admit。
    await owner_admit_census_weak(auth, admitted, [1001], OWNER_TYPE_HUB, POD, UID, BUDGET)
    assert auth.admits == 1

    # 1001 离场、2002 进场:1001 被剪枝,2002 首见 → 第 2 次 Admit。
    await owner_admit_census_weak(auth, admitted, [2002], OWNER_TYPE_HUB, POD, UID, BUDGET)
    assert auth.admits == 2
    assert f"{UID}|1001" not in admitted

    # 1001 回流:缓存已剪 → 重新 Admit。
    await owner_admit_census_weak(
        auth, admitted, [1001, 2002], OWNER_TYPE_HUB, POD, UID, BUDGET
    )
    assert auth.admits == 3


async def test_census_empty_still_prunes_then_readmits() -> None:
    """★ 本轮 census 为空(最后一名玩家离场)也必须先剪枝再早退。

    这是最容易写错的一处:大多数人会在函数开头写 `if not players: return`。
    那样最后一名玩家的缓存项永远留着,他回流时被误吞。

    ★ 变异:把 owner_authority 里的 `if not players: return` 挪到剪枝**之前** ——
      本用例第二次 census 后 admits 停在 1,红。
    """
    auth = ScriptedOwnerAuthority(records={1001: pending_record(POD, UID, 5)})
    admitted: dict[str, float] = {}

    await owner_admit_census_weak(auth, admitted, [1001], OWNER_TYPE_HUB, POD, UID, BUDGET)
    assert auth.admits == 1
    assert f"{UID}|1001" in admitted

    # 空 census:必须把 1001 剪掉。
    await owner_admit_census_weak(auth, admitted, [], OWNER_TYPE_HUB, POD, UID, BUDGET)
    assert f"{UID}|1001" not in admitted

    await owner_admit_census_weak(auth, admitted, [1001], OWNER_TYPE_HUB, POD, UID, BUDGET)
    assert auth.admits == 2


async def test_census_prune_only_touches_own_instance_keys() -> None:
    """剪枝只能动本实例(self_uid 前缀)的 key,别的实例的项不归本轮 census 管。

    ★ 变异:把 `k.startswith(prefix)` 去掉 —— 另一实例的 key 被误删,本用例红。
    """
    auth = ScriptedOwnerAuthority(records={1001: pending_record(POD, UID, 5)})
    other_key = "uid-other|7777"
    admitted: dict[str, float] = {other_key: time.monotonic()}

    await owner_admit_census_weak(auth, admitted, [1001], OWNER_TYPE_HUB, POD, UID, BUDGET)
    assert other_key in admitted


# ══════════════════════════════════════════════════════════════════════════════
# 准入缓存老化
# ══════════════════════════════════════════════════════════════════════════════


def test_sweep_stale_owner_admitted() -> None:
    """老化清理:活项保留、过期项删除、类型坏掉的值也删(fail-safe 有界)。

    key 里带 instance_uid,实例销毁后没人再续期 —— 不能老化的话缓存会随
    历史实例 UID 无界增长(§9.18)。

    ★ 变异:把 `not isinstance(value, float) or` 去掉 —— 坏值项永远删不掉,本用例红;
      把 `value < cutoff` 改成 `value <= cutoff` 时活项判据仍成立(用例仍绿),
      所以这里刻意让活项时间戳明显晚于 cutoff,不靠边界值碰运气。
    """
    now = time.monotonic()
    admitted: dict[str, float] = {
        f"{UID}|1001": now,  # 活项
        "uid-DEAD|2002": now - 3600.0,  # 一小时没续期 = 实例早没了
        "uid-BAD|3003": "not-a-timestamp",  # type: ignore[dict-item]
    }
    cutoff = now - OWNER_ADMITTED_STALE_TTL_SEC

    # 先确认构造出来的这三项确实是三种不同情形(不然测的是空气)。
    assert admitted[f"{UID}|1001"] > cutoff
    assert admitted["uid-DEAD|2002"] < cutoff
    assert not isinstance(admitted["uid-BAD|3003"], float)

    sweep_stale_owner_admitted(admitted, cutoff)

    assert f"{UID}|1001" in admitted
    assert "uid-DEAD|2002" not in admitted
    assert "uid-BAD|3003" not in admitted


async def test_census_cache_hit_refreshes_only_when_near_expiry() -> None:
    """命中缓存时只在"接近过期"才续期(降写频),但陈旧项必须被续上。

    ★ 变异:把续期条件里的 `now - value > OWNER_ADMITTED_STALE_TTL_SEC / 2` 改成
      恒 False —— 陈旧项不再续期,最终会被 sweep 误清掉活玩家,本用例红。
    """
    auth = ScriptedOwnerAuthority(records={1001: pending_record(POD, UID, 5)})
    key = f"{UID}|1001"
    stale = time.monotonic() - OWNER_ADMITTED_STALE_TTL_SEC  # 早已过半个 TTL
    admitted: dict[str, float] = {key: stale}

    await owner_admit_census_weak(auth, admitted, [1001], OWNER_TYPE_HUB, POD, UID, BUDGET)

    assert auth.queries == 0, "命中缓存不该再 Query"
    assert admitted[key] > stale, "陈旧项必须被续期"

    # 刚续过的项:本轮不该再写(值不变)。
    touched = admitted[key]
    await owner_admit_census_weak(auth, admitted, [1001], OWNER_TYPE_HUB, POD, UID, BUDGET)
    assert admitted[key] == touched


# ══════════════════════════════════════════════════════════════════════════════
# 强 Begin
# ══════════════════════════════════════════════════════════════════════════════


async def test_begin_player_strong_fail_closed() -> None:
    """Begin 失败、Query 失败都必须上抛;auth 未部署(None)才放行。

    ★ Query 失败尤其不能吞:把"查不到"当成"没有归属"就是 §9.22 明令禁止的
      冒充 OFFLINE,直接后果是第二台 DS 被放进来。

    ★ 变异:把 begin_one_player 里 `rec = await auth.query_owner(...)` 包进
      try/except 并回退成空记录 —— 第二段断言红。
    """
    target = target_of(pending_record(POD, UID, 1))

    auth = ScriptedOwnerAuthority(begin_err=errcode.PandoraError(errcode.ErrUnavailable, "boom"))
    with pytest.raises(errcode.PandoraError):
        await owner_begin_player(auth, 1001, OWNER_TYPE_HUB, target, BUDGET)

    auth = ScriptedOwnerAuthority(query_err=errcode.PandoraError(errcode.ErrUnavailable, "boom"))
    with pytest.raises(errcode.PandoraError):
        await owner_begin_player(auth, 1001, OWNER_TYPE_HUB, target, BUDGET)
    assert auth.begins == 0, "Query 失败后绝不能继续 Begin"

    # owner 未部署:部署形态问题,不在本函数收敛。
    await owner_begin_player(None, 1001, OWNER_TYPE_HUB, target, BUDGET)


async def test_begin_player_leaves_operation_to_authority() -> None:
    """operation_id 必须传空串,由权威铸。

    调用方每次现铸一个新 UUID,就等于每次投递一个新 operation —— §9.23 要的
    "一次真实进场用一个稳定 operation_id"直接失效,幂等键形同虚设。

    ★ 变异:把 begin_one_player 的 `""` 改成 `placement.new_operation_id()` ——
      本用例红。
    """
    target = target_of(pending_record(POD, UID, 1))
    auth = ScriptedOwnerAuthority()

    await owner_begin_player(auth, 1001, OWNER_TYPE_HUB, target, BUDGET)

    assert auth.begin_ops == [""]
    # 顺带确认:确实存在"非空"这个反面(不然上面那条断言可能只是在测默认值)。
    assert placement.new_operation_id() != ""


async def test_begin_player_rejects_non_exact_begin_result() -> None:
    """权威返回成功、但记录指向**旧** target 时必须判失败(滚动升级硬门)。

    旧 owner binary 可能仍把"同物理实例、不同 assignment"当 no-op:RPC 返回 nil,
    却把旧记录原样带回。只看错误码的调用方会拿着旧 assignment 去签票。

    ★ 变异:把 begin_one_player 末尾的 owner_record_exactly_targets 判断删掉 ——
      本用例红。
    """
    old = pending_record(POD, UID, 4)
    stale = OwnerRecordView(
        owner_epoch=old.owner_epoch,
        owner_type=old.owner_type,
        phase=old.phase,
        pod_name=old.pod_name,
        instance_uid=old.instance_uid,
        instance_epoch=old.instance_epoch,
        assignment_or_allocation_id="assignment-old",
        release_track=old.release_track,
        operation_id=old.operation_id,
    )
    target = OwnerTargetView(
        pod_name=POD,
        instance_uid=UID,
        instance_epoch=1,
        assignment_or_allocation_id="assignment-new",
        release_track="stable",
    )
    # ★ 先确认这份"非法回传"确实与目标不同 —— 否则测的是同一个值,等于没测。
    assert stale.assignment_or_allocation_id != target.assignment_or_allocation_id

    auth = ScriptedOwnerAuthority(begin_result=stale)
    with pytest.raises(errcode.PandoraError) as ei:
        await owner_begin_player(auth, 1001, OWNER_TYPE_HUB, target, BUDGET)
    assert errcode.as_code(ei.value) == errcode.ErrInvalidState
    # 证据要带回来,便于排查是"旧 binary no-op"还是别的漂移。
    assert ei.value.current_record == stale


@pytest.mark.parametrize("conflict_times", [1, 99])
async def test_begin_player_does_not_blind_retry_epoch_conflict(conflict_times: int) -> None:
    """无 guard 的路径遇 EPOCH_CONFLICT **不重试**,单次 fail-closed。

    assignment UUID 没有单调次序:冲突可能恰恰说明"更新的 assignment 已经胜出"。
    重查 epoch 后继续写旧 target = 把 winner 回滚。

    ★ 变异:把 break 条件里的 `guard is None or` 去掉(注意:光把 max_attempts
      改成恒 2 **不会**让本用例红 —— 真正挡住盲重试的是这个 `guard is None`
      子句,max_attempts 只是它的上界)—— conflict_times=1 那组会变成成功,红。
    """
    target = target_of(pending_record(POD, UID, 1))
    auth = ScriptedOwnerAuthority(conflict_times=conflict_times)

    with pytest.raises(errcode.PandoraError) as ei:
        await owner_begin_player(auth, 1001, OWNER_TYPE_HUB, target, BUDGET)

    assert errcode.as_code(ei.value) == errcode.ErrOwnerEpochConflict
    assert auth.queries == 1
    assert len(auth.begin_ops) == 1


async def test_begin_player_guarded_conflict_rechecks_current_intent() -> None:
    """带 guard 的路径允许**一次**重试,但第二轮必须重跑 Query→guard→Begin。

    严格性来自这个顺序:Query(E) → guard(assignment=A) → Begin(expect=E, A)。
    第二轮 guard 必须再次证明"当前意图仍是 A",所以这不是拿新 epoch 盲写旧 target。

    ★ 变异:把重试分支里的 guard 调用去掉(只重发 Begin)—— 下面的
      guard_calls == 2 断言红。
    """
    target = target_of(pending_record(POD, UID, 1))
    auth = ScriptedOwnerAuthority(conflict_times=1)
    guard_calls = 0

    async def guard() -> None:
        nonlocal guard_calls
        guard_calls += 1

    await owner_begin_player_guarded(auth, 1001, OWNER_TYPE_HUB, target, BUDGET, guard)

    assert auth.queries == 2
    assert len(auth.begin_ops) == 2
    assert guard_calls == 2


async def test_begin_player_guarded_stops_when_intent_drifted() -> None:
    """第二轮 guard 判定意图已漂移时,**不能**再发第二次 Begin。

    ★ 变异:把重试分支里 guard 抛出的异常吞掉继续 Begin ——
      len(begin_ops) 会变成 2,红。
    """
    target = target_of(pending_record(POD, UID, 1))
    auth = ScriptedOwnerAuthority(conflict_times=1)
    guard_calls = 0

    async def guard() -> None:
        nonlocal guard_calls
        guard_calls += 1
        if guard_calls >= 2:
            raise errcode.PandoraError(errcode.ErrUnavailable, "assignment drifted")

    with pytest.raises(errcode.PandoraError) as ei:
        await owner_begin_player_guarded(auth, 1001, OWNER_TYPE_HUB, target, BUDGET, guard)

    assert errcode.as_code(ei.value) == errcode.ErrUnavailable
    assert auth.queries == 2
    assert len(auth.begin_ops) == 1, "guard 没过就不该再发 Begin"
    assert guard_calls == 2


async def test_begin_player_guarded_runs_guard_even_without_authority() -> None:
    """auth 未部署时 guard 仍要跑:它是调用方自己的前置条件,与 owner 是否部署无关。

    ★ 变异:把 `auth is None` 分支改成直接 return —— 本用例红。
    """
    target = target_of(pending_record(POD, UID, 1))
    ran = False

    async def guard() -> None:
        nonlocal ran
        ran = True
        raise errcode.PandoraError(errcode.ErrInvalidState, "precondition failed")

    with pytest.raises(errcode.PandoraError):
        await owner_begin_player_guarded(None, 1001, OWNER_TYPE_HUB, target, BUDGET, guard)
    assert ran


async def test_begin_player_budget_exhaustion_is_fail_closed() -> None:
    """预算耗尽 = 失败,必须上抛而不是当成成功。

    ★ 变异:把 `async with asyncio.timeout(budget_sec)` 去掉 ——
      本用例会挂在 sleep 上直到超过 pytest 超时,或断言不到异常,红。
    """
    target = target_of(pending_record(POD, UID, 1))

    class SlowAuthority(ScriptedOwnerAuthority):
        async def query_owner(self, player_id: int) -> OwnerRecordView:
            await asyncio.sleep(5.0)
            return OwnerRecordView()

    with pytest.raises((TimeoutError, errcode.PandoraError)):
        await owner_begin_player(SlowAuthority(), 1001, OWNER_TYPE_HUB, target, 0.05)


async def test_begin_player_does_not_swallow_cancellation() -> None:
    """★ CancelledError 必须穿透:被宽 except 吞掉的话,停机时这条路径不退出,
    优雅排空失效(而且会被误报成一次"Begin 失败")。

    ★ 变异:把 `except asyncio.CancelledError: raise` 删掉 ——
      CancelledError 会被当成普通失败包装,本用例红。
    """
    target = target_of(pending_record(POD, UID, 1))

    class CancellingAuthority(ScriptedOwnerAuthority):
        async def query_owner(self, player_id: int) -> OwnerRecordView:
            raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await owner_begin_player(CancellingAuthority(), 1001, OWNER_TYPE_HUB, target, BUDGET)


async def test_begin_player_weak_paths_report_before_raising(warns: _WarnRecorder) -> None:
    """强 Begin 失败时必须先打一条带 pod/uid 的告警再上抛 —— 只抛不打的话,
    调用方那层只看得到"这次进场被拒",查不出是哪台实例、卡在哪一步。

    ★ 变异:把 owner_begin_failed 那条 warning 删掉 —— 本用例红。
    """
    target = target_of(pending_record(POD, UID, 1))
    auth = ScriptedOwnerAuthority(begin_err=errcode.PandoraError(errcode.ErrUnavailable, "boom"))

    with pytest.raises(errcode.PandoraError):
        await owner_begin_player(auth, 1001, OWNER_TYPE_HUB, target, BUDGET)

    assert "owner_begin_failed" in warns.names()
    fields = dict(warns.events[-1][1])
    assert (fields["player_id"], fields["pod"], fields["instance_uid"]) == (1001, POD, UID)


# ══════════════════════════════════════════════════════════════════════════════
# exact 判定
# ══════════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "field,bad",
    [
        ("pod_name", "hub-9"),
        ("instance_uid", "uid-other"),
        ("instance_epoch", 2),
        ("assignment_or_allocation_id", "a2"),
        ("release_track", "canary"),
    ],
)
def test_record_exactly_targets_rejects_any_field_drift(field: str, bad: object) -> None:
    """五格 identity 任意一格不同都算"不是这个归属"。

    ★ 变异:从 owner_record_exactly_targets 里删掉任意一行比较 —— 对应那组参数红。
    """
    rec = pending_record(POD, UID, 3)
    good = target_of(rec)
    drifted = type(good)(**{**{f.name: getattr(good, f.name) for f in good.__dataclass_fields__.values()}, field: bad})  # type: ignore[attr-defined]
    # 先确认"坏值"真的和合法值不同 —— 否则这组参数测的是同一个值。
    assert getattr(drifted, field) != getattr(good, field)

    assert owner_record_exactly_targets(rec, OWNER_TYPE_HUB, good)
    assert not owner_record_exactly_targets(rec, OWNER_TYPE_HUB, drifted)


def test_record_exactly_targets_rejects_anchorless_records() -> None:
    """epoch=0 / operation_id 空 / 类型不符 / phase 不在有效集 —— 都判否。

    这四格是 fencing 与幂等的锚点,缺一格就没法证明"这台 DS 有权控制该玩家"。

    ★ 变异:删掉 `rec.owner_epoch > 0` 或 phase 白名单 —— 对应断言红。
    """
    rec = pending_record(POD, UID, 3)
    tgt = target_of(rec)

    import dataclasses

    assert owner_record_exactly_targets(rec, OWNER_TYPE_HUB, tgt)
    assert not owner_record_exactly_targets(dataclasses.replace(rec, owner_epoch=0), OWNER_TYPE_HUB, tgt)
    assert not owner_record_exactly_targets(dataclasses.replace(rec, operation_id=""), OWNER_TYPE_HUB, tgt)
    assert not owner_record_exactly_targets(rec, OWNER_TYPE_BATTLE, tgt)
    assert not owner_record_exactly_targets(dataclasses.replace(rec, phase=0), OWNER_TYPE_HUB, tgt)
    # ADMITTED 也算有效 phase(幂等重放会看到它)。
    assert owner_record_exactly_targets(
        dataclasses.replace(rec, phase=OWNER_PHASE_ADMITTED), OWNER_TYPE_HUB, tgt
    )


def test_record_exactly_targets_uses_go_whitespace_table() -> None:
    """operation_id 的"是否为空"必须按 **Go 的** 空白表判,不能用 str.strip()。

    两栈的空白字符表不一致:U+001C(FS)在 Python 里 `isspace()` 为真、
    Go 的 `unicode.IsSpace` 为假。用 str.strip() 的话,同一条记录在 Go 侧被判
    "有 operation_id、可以交付",在 Python 侧被判"空、拒绝交付" —— 混跑期同一个
    玩家在两栈得到相反结论。

    ★ 变异:把 owner_record_exactly_targets 里的 placement.go_trim_space 换成
      `rec.operation_id.strip()` —— 本用例红。
    """
    weird = "\x1c"
    # 先证明这个字符确实能把两套实现分开(否则本用例测不出任何东西)。
    assert weird.strip() == "", "选的字符在 Python 侧不算空白,换一个"
    assert placement.go_trim_space(weird) == weird, "选的字符在 Go 侧算空白,换一个"

    import dataclasses

    rec = dataclasses.replace(pending_record(POD, UID, 3), operation_id=weird)
    assert owner_record_exactly_targets(rec, OWNER_TYPE_HUB, target_of(rec))


# ══════════════════════════════════════════════════════════════════════════════
# census:弱依赖的三条分流
# ══════════════════════════════════════════════════════════════════════════════


async def test_census_admit_barrier_not_open_is_silent_and_not_cached(
    warns: _WarnRecorder,
) -> None:
    """屏障未开(retry_after_ms>0)是**预期中的 WAIT**:不计失败、不告警、不写缓存。

    计进失败告警的话,每次正常迁移都会刷一片假告警,真故障反而被淹掉;
    写进缓存的话,屏障真开了也再没人补 Admit。

    ★ 变异:把 `if retry_after > 0: continue` 改成不生效 —— 缓存断言仍绿
      (失败路径同样不写缓存),但 warns 断言会红。这条日志断言是本用例
      唯一能分辨"静默 WAIT"和"计失败"的判据,不能删。
    """
    barrier = errcode.PandoraError(errcode.ErrOwnerBarrierNotOpen, "barrier")
    auth = ScriptedOwnerAuthority(
        records={1001: pending_record(POD, UID, 5)},
        admit_err=barrier,
        admit_retry_after_ms=1500,
    )
    admitted: dict[str, float] = {}

    await owner_admit_census_weak(auth, admitted, [1001], OWNER_TYPE_HUB, POD, UID, BUDGET)

    assert auth.admits == 1
    assert f"{UID}|1001" not in admitted, "屏障未开不能写准入缓存,否则下轮不再补"
    assert "owner_admit_census_weak_failed" not in warns.names()

    # 下一轮仍会重试。
    await owner_admit_census_weak(auth, admitted, [1001], OWNER_TYPE_HUB, POD, UID, BUDGET)
    assert auth.admits == 2
    assert "owner_admit_census_weak_failed" not in warns.names()


async def test_census_real_admit_failure_does_report(warns: _WarnRecorder) -> None:
    """反面对照:retry_after=0 的**真**失败必须计数并汇总告警。

    没有这条对照,上一个用例的 "not in warns" 可能只是因为录制器根本没接上 ——
    一条永远为真的断言。

    ★ 变异:把 finally 里的汇总告警删掉 —— 本用例红。
    """
    auth = ScriptedOwnerAuthority(
        records={1001: pending_record(POD, UID, 5)},
        admit_err=errcode.PandoraError(errcode.ErrUnavailable, "owner down"),
        admit_retry_after_ms=0,
    )
    await owner_admit_census_weak(auth, {}, [1001], OWNER_TYPE_HUB, POD, UID, BUDGET)

    assert "owner_admit_census_weak_failed" in warns.names()
    fields = dict(warns.events[-1][1])
    assert fields["admit_failed"] == 1
    assert fields["sample_player_id"] == 1001


async def test_census_admit_target_is_taken_from_record() -> None:
    """代提交的 target 取**记录自身**字段,并带上记录的 epoch / operation_id。

    自己拼一个 target 的话,owner 侧的 exact 全等校验必然失败(identity mismatch),
    census 兜底就永远补不上。

    ★ 变异:把 target 改成用 self_pod/self_uid 之外的任意常量 —— 本用例红。
    """
    rec = pending_record(POD, UID, 5)
    auth = ScriptedOwnerAuthority(records={1001: rec})
    await owner_admit_census_weak(auth, {}, [1001], OWNER_TYPE_HUB, POD, UID, BUDGET)

    assert auth.admit_targets == [target_of(rec)]
    assert auth.admit_args == [(1001, rec.owner_epoch, rec.operation_id)]


async def test_census_skips_admitted_and_other_phases() -> None:
    """已 ADMITTED → 只写缓存不再 Admit;非 PENDING/ADMITTED 的 phase → 什么都不做。

    ★ 变异:把 `if rec.phase != OWNER_PHASE_PENDING: continue` 删掉 ——
      phase=0 的记录会被 Admit,本用例红。
    """
    import dataclasses

    rec = pending_record(POD, UID, 5)
    auth = ScriptedOwnerAuthority(
        records={
            1001: dataclasses.replace(rec, phase=OWNER_PHASE_ADMITTED),
            2002: dataclasses.replace(rec, phase=0),
        }
    )
    admitted: dict[str, float] = {}
    await owner_admit_census_weak(
        auth, admitted, [1001, 2002], OWNER_TYPE_HUB, POD, UID, BUDGET
    )

    assert auth.admits == 0
    assert f"{UID}|1001" in admitted, "已 ADMITTED 要写缓存,避免每轮重查"
    assert f"{UID}|2002" not in admitted, "phase 说不准就不能标成已准入"


async def test_census_heals_drift_when_mirror_still_points_here() -> None:
    """归属镜像仍指向本实例、owner 记录却没跟上 = 签票点失败留下的漂移,补一次 Begin。

    这条自愈**没有别的重试点** —— 删了 owner 记录就长期漂移。

    ★ 变异:把 resolve_target 分支整段删掉 —— begins 停在 0,红。
    """
    elsewhere = pending_record("hub-9", "uid-elsewhere", 5)
    auth = ScriptedOwnerAuthority(records={1001: elsewhere})
    here = OwnerTargetView(
        pod_name=POD,
        instance_uid=UID,
        instance_epoch=1,
        assignment_or_allocation_id="a1",
        release_track="stable",
    )
    # 先确认"记录指向别处"这个前提确实成立。
    assert elsewhere.pod_name != here.pod_name

    async def resolve(player_id: int) -> tuple[OwnerTargetView, bool]:
        return here, True

    await owner_admit_census_weak(
        auth, {}, [1001], OWNER_TYPE_HUB, POD, UID, BUDGET, resolve
    )

    assert auth.begins == 1
    assert auth.begin_targets == [here]


async def test_census_does_not_heal_real_migration() -> None:
    """镜像也指向别处 = 真实迁移,绝不干预(干预就是把玩家从新实例抢回来)。

    ★ 变异:把 `tgt.pod_name == self_pod and tgt.instance_uid == self_uid` 去掉 ——
      本用例红。
    """
    auth = ScriptedOwnerAuthority(records={1001: pending_record("hub-9", "uid-elsewhere", 5)})

    async def resolve(player_id: int) -> tuple[OwnerTargetView, bool]:
        return OwnerTargetView(pod_name="hub-9", instance_uid="uid-elsewhere"), True

    await owner_admit_census_weak(
        auth, {}, [1001], OWNER_TYPE_HUB, POD, UID, BUDGET, resolve
    )
    assert auth.begins == 0

    # resolve 说"查不到"时同样不干预。
    async def unknown(player_id: int) -> tuple[OwnerTargetView, bool]:
        return OwnerTargetView(pod_name=POD, instance_uid=UID), False

    await owner_admit_census_weak(
        auth, {}, [1001], OWNER_TYPE_HUB, POD, UID, BUDGET, unknown
    )
    assert auth.begins == 0


async def test_census_heal_guard_blocks_when_target_changed_before_begin() -> None:
    """自愈的 guard:Query 之后 Begin 之前镜像变了,就不能再写 owner。

    ★ 变异:把 _census_heal_guard 里的 `current != tgt` 判断删掉 ——
      begins 会变成 1,红。
    """
    auth = ScriptedOwnerAuthority(records={1001: pending_record("hub-9", "uid-elsewhere", 5)})
    first = OwnerTargetView(
        pod_name=POD, instance_uid=UID, instance_epoch=1, assignment_or_allocation_id="a1"
    )
    second = OwnerTargetView(
        pod_name=POD, instance_uid=UID, instance_epoch=1, assignment_or_allocation_id="a2"
    )
    # 先确认这两个 target 确实不同(相同的话 guard 永远不会拦,用例测不出东西)。
    assert first != second

    calls = 0

    async def resolve(player_id: int) -> tuple[OwnerTargetView, bool]:
        nonlocal calls
        calls += 1
        return (first, True) if calls == 1 else (second, True)

    await owner_admit_census_weak(
        auth, {}, [1001], OWNER_TYPE_HUB, POD, UID, BUDGET, resolve
    )

    assert auth.begins == 0, "guard 没过就不能写 owner"


async def test_census_query_failure_does_not_break_the_batch() -> None:
    """弱依赖:一个玩家 Query 失败不能中断整批,也不能写缓存。

    census 挂在心跳上,让它整批失败等于让 owner 的抖动把整台 DS 判掉线。

    ★ 变异:把 query 的 except 分支改成 raise —— 本用例红。
    """
    auth = ScriptedOwnerAuthority(
        records={2002: pending_record(POD, UID, 6)},
        query_err=None,
    )

    class FlakyAuthority(ScriptedOwnerAuthority):
        async def query_owner(self, player_id: int) -> OwnerRecordView:
            self.queries += 1
            if player_id == 1001:
                raise errcode.PandoraError(errcode.ErrUnavailable, "flaky")
            return self.records.get(player_id, OwnerRecordView())

    auth = FlakyAuthority(records={2002: pending_record(POD, UID, 6)})
    admitted: dict[str, float] = {}

    await owner_admit_census_weak(
        auth, admitted, [1001, 2002], OWNER_TYPE_HUB, POD, UID, BUDGET
    )

    assert auth.admits == 1, "第二个玩家必须照常处理"
    assert f"{UID}|1001" not in admitted


async def test_census_budget_exhaustion_stops_early(warns: _WarnRecorder) -> None:
    """预算耗尽就停,剩下的玩家等下一轮心跳(census 每 ~5s 一轮,自然收敛)。

    不停的话,owner 变慢时 census 会把心跳本身拖过 heartbeat_timeout ——
    "为了对账"把 DS 判掉线。

    ★ 变异:把 `if time.monotonic() >= deadline: return` 删掉 ——
      两个玩家都会被处理,本用例红。
    ★ 变异:把汇总告警从 finally 挪回函数末尾 —— 提前 return 时告警丢失,
      下面的 warns 断言红(这正是 Go 用 defer 的原因)。
    """

    class SlowAuthority(ScriptedOwnerAuthority):
        async def query_owner(self, player_id: int) -> OwnerRecordView:
            self.queries += 1
            await asyncio.sleep(0.05)
            raise errcode.PandoraError(errcode.ErrUnavailable, "slow owner")

    auth = SlowAuthority(
        records={1001: pending_record(POD, UID, 5), 2002: pending_record(POD, UID, 6)}
    )
    await owner_admit_census_weak(
        auth, {}, [1001, 2002], OWNER_TYPE_HUB, POD, UID, 0.01
    )

    assert auth.queries == 1, "预算只够第一个玩家"
    assert "owner_admit_census_weak_failed" in warns.names(), "提前 return 也要汇总告警"


async def test_census_no_authority_is_noop() -> None:
    """auth 未部署时整函数 no-op(连剪枝都不做:没有权威就没有"准入"这回事)。

    ★ 变异:把 `if auth is None: return` 删掉 —— 会在 None 上调方法,红。
    """
    admitted = {f"{UID}|1001": time.monotonic()}
    await owner_admit_census_weak(None, admitted, [], OWNER_TYPE_HUB, POD, UID, BUDGET)
    assert f"{UID}|1001" in admitted


async def test_census_does_not_swallow_cancellation() -> None:
    """★ census 的宽 except 同样必须放行 CancelledError。

    ★ 变异:把 query 分支的 `except asyncio.CancelledError: raise` 删掉 ——
      取消会被当成一次 query_failed 吞掉,本用例红。
    """

    class CancellingAuthority(ScriptedOwnerAuthority):
        async def query_owner(self, player_id: int) -> OwnerRecordView:
            raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await owner_admit_census_weak(
            CancellingAuthority(), {}, [1001], OWNER_TYPE_HUB, POD, UID, BUDGET
        )


# ══════════════════════════════════════════════════════════════════════════════
# 租约双写门
# ══════════════════════════════════════════════════════════════════════════════


async def test_renew_owner_lease_gate_nil_renewer_is_noop() -> None:
    """未配 owner_addr → no-op,现网行为不变。

    ★ 变异:把 `if renewer is None: return` 删掉 —— AttributeError,红。
    """
    await olease.renew_owner_lease_gate(None, True, "p", "u", 1, "stable")


async def test_renew_owner_lease_gate_passes_identity_through() -> None:
    """成功路径必须把完整 identity 原样传下去(少一格就是 fencing 少一道锁)。

    ★ 变异:把 release_track 传成常量 "" —— 本用例红。
    """
    renewer = FakeLeaseRenewer()
    await olease.renew_owner_lease_gate(renewer, False, "pod-1", "uid-1", 3, "canary")

    assert renewer.calls == 1
    assert (renewer.pod, renewer.uid, renewer.epoch, renewer.track) == (
        "pod-1",
        "uid-1",
        3,
        "canary",
    )


async def test_renew_owner_lease_gate_weak_failure_does_not_break_heartbeat() -> None:
    """migrate 弱依赖:双写失败只告警,心跳照常成功。

    这一档是拿"暂时的一致性缺口"换可用性 —— owner 刚上线时它自己的抖动不该
    把一堆健康 DS 判成掉线;缺口由再入屏障双门兜底。

    ★ 变异:把 `if required: raise` 改成无条件 raise —— 本用例红。
    """
    renewer = FakeLeaseRenewer(errcode.PandoraError(errcode.ErrUnavailable, "owner down"))
    await olease.renew_owner_lease_gate(renewer, False, "p", "u", 1, "")
    assert renewer.calls == 1


async def test_renew_owner_lease_gate_required_failure_fails_heartbeat() -> None:
    """contract 强依赖:续租失败 → 心跳失败 → DS 自我 fencing。

    ★ 变异:把 required 分支的 raise 去掉 —— 本用例红,而线上后果是
      "DS 以为租约还在、owner 那边已过期"的窗口,同一玩家可在两台 DS 可玩。
    """
    renewer = FakeLeaseRenewer(errcode.PandoraError(errcode.ErrUnavailable, "owner down"))
    with pytest.raises(errcode.PandoraError):
        await olease.renew_owner_lease_gate(renewer, True, "p", "u", 1, "")


async def test_renew_owner_lease_gate_does_not_swallow_cancellation() -> None:
    """★ 弱依赖档也不能吞 CancelledError(停机时心跳路径必须能退出)。

    ★ 变异:删掉 `except asyncio.CancelledError: raise` —— 取消被当成弱失败
      静默放行,本用例红。
    """
    renewer = FakeLeaseRenewer(asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await olease.renew_owner_lease_gate(renewer, False, "p", "u", 1, "")


# ══════════════════════════════════════════════════════════════════════════════
# gRPC 客户端:响应码 → 异常 + 证据
# ══════════════════════════════════════════════════════════════════════════════


class _FakeChannel:
    """只实现 unary_unary 的假 channel,够生成的 Stub 用。"""

    def __init__(self, handlers: dict[str, object]) -> None:
        self._handlers = handlers

    def unary_unary(self, method: str, **_kw: object) -> object:
        return self._handlers[method.rsplit("/", 1)[-1]]


def make_client(**handlers: object) -> GrpcOwnerLeaseRenewer:
    """造一个不连网的客户端;未提供的方法调用即报错(暴露多余往返)。"""

    async def _unexpected(request: object, timeout: float | None = None) -> object:
        raise AssertionError("本用例不该调用这个 RPC")

    full = {
        name: handlers.get(name, _unexpected)
        for name in ("QueryOwner", "BeginTransition", "Admit", "RenewInstanceLease", "ReleaseOwner")
    }
    return GrpcOwnerLeaseRenewer(channel=_FakeChannel(full))


async def test_client_query_owner_maps_record_and_rejects_non_ok() -> None:
    """QueryOwner:OK 映射成 View;非 OK 抛业务码(绝不返回空记录冒充"无归属")。

    ★ 变异:把非 OK 分支改成 `return record_view(...)` —— 第二段红,而线上后果
      是"查不到"被当成"没有归属",第二台 DS 被放进来。
    """
    captured: list[object] = []

    async def ok(request: object, timeout: float | None = None) -> object:
        captured.append(request)
        return ownerpb.QueryOwnerResponse(
            code=errcode_pb2.OK,
            record=ownerpb.OwnerRecord(
                owner_epoch=7,
                owner_type=ownerpb.OWNER_TYPE_HUB,
                phase=ownerpb.OWNER_PHASE_ADMITTED,
                target=ownerpb.OwnerTarget(
                    pod_name=POD,
                    instance_uid=UID,
                    instance_epoch=2,
                    assignment_or_allocation_id="a1",
                    release_track="canary",
                ),
                operation_id="op-1",
                admit_not_before_ms=1234,
            ),
        )

    view = await make_client(QueryOwner=ok).query_owner(1001)
    assert view == OwnerRecordView(
        owner_epoch=7,
        owner_type=int(ownerpb.OWNER_TYPE_HUB),
        phase=int(ownerpb.OWNER_PHASE_ADMITTED),
        pod_name=POD,
        instance_uid=UID,
        instance_epoch=2,
        assignment_or_allocation_id="a1",
        release_track="canary",
        operation_id="op-1",
        admit_not_before_ms=1234,
    )
    assert captured[0].player_id == 1001  # type: ignore[attr-defined]

    async def denied(request: object, timeout: float | None = None) -> object:
        return ownerpb.QueryOwnerResponse(code=errcode_pb2.ERR_OWNER_IDENTITY_MISMATCH)

    with pytest.raises(errcode.PandoraError) as ei:
        await make_client(QueryOwner=denied).query_owner(1001)
    assert ei.value.code == int(errcode_pb2.ERR_OWNER_IDENTITY_MISMATCH)


async def test_client_begin_sends_source_revision_outside_target() -> None:
    """source_revision 走**顶层**字段,不进 OwnerTarget。

    塞进 target 的话,Admit 那边的 exact 全等判定会因为版本号变化而
    把"同一归属的重复 Admit"判成 identity mismatch。

    ★ 变异:把 target_proto 改成也写 source_revision —— OwnerTarget 里根本没有
      这个字段,构造即 ValueError,红。
    """
    seen: list[ownerpb.BeginTransitionRequest] = []

    async def begin(request: ownerpb.BeginTransitionRequest, timeout: float | None = None) -> object:
        seen.append(request)
        return ownerpb.BeginTransitionResponse(
            code=errcode_pb2.OK,
            record=ownerpb.OwnerRecord(owner_epoch=8, operation_id="op-minted"),
        )

    target = OwnerTargetView(
        pod_name=POD,
        instance_uid=UID,
        instance_epoch=1,
        assignment_or_allocation_id="a1",
        release_track="stable",
        source_revision=srev.compose(3, 5),
    )
    # ★ 先确认这个版本号确实不是 LEGACY 哨兵,否则下面断言的是默认值。
    assert target.source_revision != srev.LEGACY

    await make_client(BeginTransition=begin).begin_transition(
        1001, 7, "", OWNER_TYPE_HUB, target
    )

    req = seen[0]
    assert req.source_revision == target.source_revision
    assert req.expect_epoch == 7
    assert req.operation_id == ""
    assert "source_revision" not in {f.name for f, _ in req.target.ListFields()}


async def test_client_begin_carries_current_record_on_conflict() -> None:
    """EPOCH_CONFLICT 时权威当前记录必须随异常带回(§9.23 的 query-first 重查依据)。

    ★ 变异:把 `err.current_record = view` 删掉 —— 本用例红,而线上后果是
      调用方只拿到一个错误码,查不出"是谁抢走了"。
    """

    async def conflict(request: object, timeout: float | None = None) -> object:
        return ownerpb.BeginTransitionResponse(
            code=errcode_pb2.ERR_OWNER_EPOCH_CONFLICT,
            record=ownerpb.OwnerRecord(
                owner_epoch=42,
                target=ownerpb.OwnerTarget(pod_name="hub-9", instance_uid="uid-winner"),
            ),
        )

    with pytest.raises(errcode.PandoraError) as ei:
        await make_client(BeginTransition=conflict).begin_transition(
            1001, 7, "", OWNER_TYPE_HUB, OwnerTargetView(pod_name=POD, instance_uid=UID)
        )
    assert errcode.as_code(ei.value) == errcode.ErrOwnerEpochConflict
    record = ei.value.current_record
    assert isinstance(record, OwnerRecordView)
    assert (record.owner_epoch, record.pod_name) == (42, "hub-9")


async def test_client_admit_carries_retry_after_on_barrier() -> None:
    """屏障未开时 retry_after_ms 必须随异常带回,census 才能分流成静默 WAIT。

    ★ 变异:把 `err.retry_after_ms = ...` 删掉 —— retry_after 变 0,
      census 会把每次正常迁移都计成 admit_failed 刷告警,本用例红。
    """

    async def barrier(request: object, timeout: float | None = None) -> object:
        return ownerpb.AdmitResponse(
            code=errcode_pb2.ERR_OWNER_BARRIER_NOT_OPEN, retry_after_ms=1700
        )

    with pytest.raises(errcode.PandoraError) as ei:
        await make_client(Admit=barrier).admit(
            1001, 7, "op-1", OwnerTargetView(pod_name=POD, instance_uid=UID)
        )
    assert ei.value.retry_after_ms == 1700

    async def ok(request: object, timeout: float | None = None) -> object:
        return ownerpb.AdmitResponse(code=errcode_pb2.OK)

    assert (
        await make_client(Admit=ok).admit(
            1001, 7, "op-1", OwnerTargetView(pod_name=POD, instance_uid=UID)
        )
        == 0
    )


async def test_client_renew_lease_uses_placement_lease_seconds() -> None:
    """租约长度必须同源于 placement,不能在本文件里另写一个数字。

    两边各写一份的话,DS 自我 fencing 的窗口就和 owner 侧算屏障用的窗口不一致 ——
    §9.22 那条"旧 DS 最晚停止可玩 < 新 DS 最早开始可玩"直接失效。

    ★ 变异:把 lease_seconds 改成任意字面量 —— 本用例红。
    """
    seen: list[ownerpb.RenewInstanceLeaseRequest] = []

    async def renew(
        request: ownerpb.RenewInstanceLeaseRequest, timeout: float | None = None
    ) -> object:
        seen.append(request)
        return ownerpb.RenewInstanceLeaseResponse(code=errcode_pb2.OK, lease_deadline_ms=1)

    await make_client(RenewInstanceLease=renew).renew_instance_lease(POD, UID, 3, "canary")

    assert seen[0].lease_seconds == placement.DS_FENCE_LEASE_MAX_SECONDS
    assert seen[0].target.pod_name == POD
    assert seen[0].target.instance_epoch == 3
    assert seen[0].target.release_track == "canary"

    async def denied(request: object, timeout: float | None = None) -> object:
        return ownerpb.RenewInstanceLeaseResponse(code=errcode_pb2.ERR_OWNER_LEASE_REGRESSED)

    with pytest.raises(errcode.PandoraError):
        await make_client(RenewInstanceLease=denied).renew_instance_lease(POD, UID, 3, "")


@pytest.mark.parametrize(
    "player_id,instance_epoch",
    [
        (1 << 64, 0),  # player_id 越 uint64
        (-1, 0),  # 负数
        (1001, 1 << 32),  # instance_epoch 越 uint32
        (1001, -1),
    ],
)
async def test_client_rejects_out_of_range_unsigned_values(
    player_id: int, instance_epoch: int
) -> None:
    """越界的无符号值必须在**发送前**被拦成 ErrInvalidArg。

    Go 的 uint64/uint32 由类型系统兜底,Python 的 int 无界:不拦的话
    protobuf 序列化时抛的是一个既没有业务码、也说不清哪个字段的 ValueError。

    ★ 变异:把 _check_uint 的范围判断删掉 —— 抛的变成 ValueError,本用例红。
    """
    # 先确认这些值确实越界(而不是被 parametrize 写错成合法值)。
    assert player_id < 0 or player_id > (1 << 64) - 1 or instance_epoch < 0 or instance_epoch > (1 << 32) - 1

    async def never(request: object, timeout: float | None = None) -> object:
        raise AssertionError("越界值不该被发出去")

    client = make_client(QueryOwner=never, Admit=never)
    with pytest.raises(errcode.PandoraError) as ei:
        await client.admit(
            player_id, 1, "op", OwnerTargetView(instance_epoch=instance_epoch)
        )
    assert ei.value.code == errcode.ErrInvalidArg


async def test_client_close_does_not_touch_injected_channel() -> None:
    """注入的 channel 归调用方所有,close() 不能顺手关掉别人的连接。

    ★ 变异:把 `if self._owns_channel:` 去掉 —— 假 channel 没有 close 方法,
      AttributeError,红。
    """
    await make_client().close()
