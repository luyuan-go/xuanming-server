"""`ds_allocator` owner 三件套回归测试。

覆盖模块与其**对照真相**(行为口径一律以 Go 源码为准,不以 Python 实现"看起来该
怎样"为准):

    pandorapy/services/ds_allocator/owner_authority.py
        ← services/battle/ds_allocator/internal/biz/owner_authority.go
    pandorapy/services/ds_allocator/owner_lease.py
        ← services/battle/ds_allocator/internal/biz/owner_lease.go
    pandorapy/services/ds_allocator/orphan_gameserver.py
        ← services/battle/ds_allocator/internal/biz/orphan_gameserver.go

## 本文件盯死的是 §9 不变量 22

「同一玩家同一时刻最多只能在一个可玩 DS」。这条不变量在 battle 侧的全部落点就是
owner_authority.py 的四个入口,而它们**放行的代价是双 DS**,不是一次失败的请求。
所以这里的每条断言都对着一个具体的放行后果写:

  - `owner_epoch` 是 CAS 的期望值,必须取**刚读到的**权威记录 —— 自铸 / 推算 /
    复用旧值都等于把条件更新退化成盲写,旧 epoch 的写会覆盖已经胜出的新归属。
  - lease / 屏障不确定(`retry_after_ms > 0`、Query 失败、回读不可达)时一律
    fail-closed,**绝不**当作"没有归属"或"已经准入"继续。
  - `PENDING → ADMITTED` 的 Admit 必须带**记录自身**的 epoch + operation_id,
    这就是 CAS;拿本地推算的值去 Admit 只会静默 no-op,而调用方以为准入成功了。
  - 重复提交(并发同 target no-op)必须由 `created=False` 表达 —— 它决定这份归属
    **不进**本次的回滚集合;写反了就会把别人赖以交付 READY 的 grant 撤掉。
  - 孤儿 GS 回收的删除对象必须 exact 到 `name + uid + allocation_id`,且只在
    连续观察 + 台账可证出身之后 —— 这里错一格删的是**载人 DS**。

## 依赖策略:全内存替身,不碰 Redis / k8s / gRPC

owner 权威、Agones 编排、Redis 台账全部用记录调用序列的替身。本文件测的是
**调用方的判定与顺序**,不是 owner 服务或 k8s 的行为;把真依赖搭进来只会让这批
安全断言因为外部抖动而假红,那是最快让人把它们删掉的方式。

★ 每条用例 docstring 的 `★ 变异:` 一行都**真跑过**(改坏产品代码 → 确认本条红 →
  改回 → 确认绿),不是照着代码猜的。
"""

from __future__ import annotations

import asyncio

import pytest

from pandorapy import errcode, placement
from pandorapy.services.ds_allocator import orphan_gameserver as OG
from pandorapy.services.ds_allocator import orphan_reclaim
from pandorapy.services.ds_allocator import owner_authority as OA
from pandorapy.services.ds_allocator import owner_lease as OL
from pandorapy.services.ds_allocator.agones_allocator import AllocatedGameServerInfo
from pandorapy.services.ds_allocator.clients import OwnerRecordView, OwnerTargetView

# ── 常量 ────────────────────────────────────────────────────────────────────

P1 = 800_000_000_000_000_001
P2 = 800_000_000_000_000_002
P3 = 800_000_000_000_000_003

POD = "pandora-battle-7f9c-abcde"
UID = "gs-uid-A"
ALLOC = "5a1f1b7e-4a1e-4a3c-9f4e-2b7c1d8e6a05"
TRACK = "stable"
EPOCH = 7

# 两个 canonical UUIDv4,用作权威回传的 operation_id。
OP_OTHER = "9c3b8e21-6d4f-4a7b-8e15-2f6a9c0d3b47"
OP_OTHER2 = "1b2c3d4e-5f60-4a71-8b92-c3d4e5f60718"

TARGET = OwnerTargetView(
    pod_name=POD,
    instance_uid=UID,
    instance_epoch=EPOCH,
    assignment_or_allocation_id=ALLOC,
    release_track=TRACK,
)


def _rec(**over) -> OwnerRecordView:
    """构造一份"逐格 exact 指向 TARGET"的权威记录,再按需覆盖某一格。"""
    base = dict(
        owner_epoch=11,
        owner_type=OA.OWNER_TYPE_BATTLE,
        phase=OA.OWNER_PHASE_PENDING,
        pod_name=POD,
        instance_uid=UID,
        instance_epoch=EPOCH,
        assignment_or_allocation_id=ALLOC,
        release_track=TRACK,
        operation_id=OP_OTHER,
        admit_not_before_ms=0,
    )
    base.update(over)
    return OwnerRecordView(**base)


def _boom(msg: str = "owner unavailable") -> errcode.PandoraError:
    return errcode.PandoraError(errcode.ErrUnavailable, msg)


def _conflict(current: OwnerRecordView) -> errcode.PandoraError:
    err = errcode.PandoraError(errcode.ErrOwnerEpochConflict, "epoch conflict")
    err.current_record = current
    return err


def _wait(retry_after_ms: int) -> errcode.PandoraError:
    """屏障未开的 WAIT:owner 侧把 `retry_after_ms` 挂在异常上(clients.py:298)。"""
    err = errcode.PandoraError(errcode.ErrUnavailable, "admit barrier closed")
    err.retry_after_ms = retry_after_ms
    return err


# ── owner 权威替身 ──────────────────────────────────────────────────────────


def _pop(seq: list):
    """取下一个脚本项:多于一项时逐个消费,只剩一项时**粘住**(便于表达稳态)。"""
    item = seq[0] if len(seq) == 1 else seq.pop(0)
    if isinstance(item, BaseException):
        raise item
    if callable(item):
        return item()
    return item


class _Auth:
    """`OwnerAuthority` 替身:按玩家排队回放结果,并**逐参数**记录调用。

    调用记录是本文件大部分断言的落点 —— 只断言"返回值对"压不住 CAS 期望值取错、
    Admit 用了推算的 epoch、回滚撤了别人的 grant 这类缺陷,它们全都表现为
    "返回值照样对,参数错了"。
    """

    def __init__(self) -> None:
        self.query: dict[int, list] = {}
        self.begin: dict[int, list] = {}
        self.admit_script: dict[int, list] = {}
        self.release_script: dict[int, list] = {}
        self.query_calls: list[int] = []
        self.begin_calls: list[tuple] = []
        self.admit_calls: list[tuple] = []
        self.release_calls: list[tuple] = []

    async def query_owner(self, player_id: int) -> OwnerRecordView:
        self.query_calls.append(player_id)
        return _pop(self.query.get(player_id, [OwnerRecordView()]))

    async def begin_transition(
        self,
        player_id: int,
        expect_epoch: int,
        operation_id: str,
        owner_type: int,
        target: OwnerTargetView,
    ) -> OwnerRecordView:
        self.begin_calls.append((player_id, expect_epoch, operation_id, owner_type, target))
        script = self.begin.get(player_id)
        if script is None:
            # 缺省:权威照本次请求写入并原样回传 operation(= 真实创建)。
            return _rec(operation_id=operation_id, owner_epoch=expect_epoch + 1)
        item = script[0] if len(script) == 1 else script.pop(0)
        if isinstance(item, BaseException):
            raise item
        if callable(item):
            return item(operation_id, expect_epoch)
        return item

    async def admit(
        self, player_id: int, owner_epoch: int, operation_id: str, target: OwnerTargetView
    ) -> int:
        self.admit_calls.append((player_id, owner_epoch, operation_id, target))
        return _pop(self.admit_script.get(player_id, [0]))

    async def release_owner(self, player_id: int, owner_epoch: int, operation_id: str) -> None:
        self.release_calls.append((player_id, owner_epoch, operation_id))
        _pop(self.release_script.get(player_id, [None]))


# ════════════════════════════════════════════════════════════════════════════
# owner_record_exactly_targets —— 滚动升级期唯一的 exact 门
# ════════════════════════════════════════════════════════════════════════════


def test_exactly_targets_all_fields_match():
    """逐格全等时放行,且 `target.source_revision` 不参与比对。

    守的是:exact 门不能把"来源版本"混进 exact 身份。source_revision 会变,把它算进
    身份会让同一归属的重复 Admit 变成 identity mismatch —— 玩家在正常重试里被拒准入。

    ★ 变异:把 `owner_authority.py` 的 `owner_record_exactly_targets` 返回式里加一条
      `and rec.instance_epoch == target.source_revision` → 本条红。
    """
    t = OwnerTargetView(
        pod_name=POD,
        instance_uid=UID,
        instance_epoch=EPOCH,
        assignment_or_allocation_id=ALLOC,
        release_track=TRACK,
        source_revision=99,
    )
    assert OA.owner_record_exactly_targets(_rec(), OA.OWNER_TYPE_BATTLE, t) is True


def test_exactly_targets_rejects_instance_epoch_drift():
    """`instance_epoch` 不同 = 实例代次翻转 / 灾备接管 → **不是**同一目标。

    守的是 §9.22「instance epoch 变化必须递增 owner_epoch」。漏比这一格会把本应发生
    的 owner 迁移误判成"同 target no-op"直接跳过 —— 玩家的归属还挂在上一代实例上,
    新实例却已经开始交付 READY,这就是双 DS。
    (Go 侧这一格是后补的:hub_allocator 复审 P1-3 加了、battle 侧一直没加。)

    ★ 变异:删掉 `owner_record_exactly_targets` 里
      `and rec.instance_epoch == target.instance_epoch` 这一行 → 本条红。
    """
    assert OA.owner_record_exactly_targets(_rec(instance_epoch=EPOCH + 1), OA.OWNER_TYPE_BATTLE, TARGET) is False


def test_exactly_targets_rejects_zero_owner_epoch():
    """`owner_epoch == 0` = 权威压根没写过这条记录 → 不是 exact。

    守的是:零值不得冒充"已归属"。放行等于把一份空记录当成"这台 DS 已被授权",
    Release 时又因 epoch=0 匹配不上而静默 no-op,残留永远清不掉。

    ★ 变异:把 `rec.owner_epoch > 0` 改成 `rec.owner_epoch >= 0` → 本条红。
    """
    assert OA.owner_record_exactly_targets(_rec(owner_epoch=0), OA.OWNER_TYPE_BATTLE, TARGET) is False


@pytest.mark.parametrize(
    "bad_op",
    [
        "",
        "9C3B8E21-6D4F-4A7B-8E15-2F6A9C0D3B47",  # 大写:uuid.UUID 接受但会改写
        "{9c3b8e21-6d4f-4a7b-8e15-2f6a9c0d3b47}",  # 花括号
        "urn:uuid:9c3b8e21-6d4f-4a7b-8e15-2f6a9c0d3b47",
        "9c3b8e21-6d4f-1a7b-8e15-2f6a9c0d3b47",  # v1 不是 v4
        "not-a-uuid",
    ],
)
def test_exactly_targets_requires_canonical_operation_id(bad_op: str):
    """`operation_id` 必须是 canonical 小写 UUIDv4,不是"非空即可"。

    守的是 §9.23 端到端幂等键。非 canonical 写法意味着写者没按协议铸号:同一次进场
    用不同写法重试会被当成**两个** operation → 重复占座 / 重复分配 DS / 第二个 owner;
    而拿这种值去 `ReleaseOwner` 只会静默 no-op(看起来回滚成功,残留还在)。

    ★ 变异:把 `placement.valid_operation_id(rec.operation_id)` 改成
      `rec.operation_id != ""` → 本条(除空串外的 5 个参数)红。
    """
    assert OA.owner_record_exactly_targets(_rec(operation_id=bad_op), OA.OWNER_TYPE_BATTLE, TARGET) is False


@pytest.mark.parametrize("phase", [0, 3, -1, 99])
def test_exactly_targets_rejects_non_live_phase(phase: int):
    """只有 `PENDING` / `ADMITTED` 算"活着的归属"。

    守的是:UNSPECIFIED / 已释放 / 未来新增的阶段值都不能被当成"这台 DS 拥有该玩家"。
    放行会让一份**已释放**的记录继续证明归属,旧 DS 于是永不停玩。

    ★ 变异:把 `rec.phase in (OWNER_PHASE_PENDING, OWNER_PHASE_ADMITTED)` 改成
      `rec.phase != 0` → 本条 phase=3/99 两个参数红。
    """
    assert OA.owner_record_exactly_targets(_rec(phase=phase), OA.OWNER_TYPE_BATTLE, TARGET) is False


def test_exactly_targets_rejects_owner_type_drift():
    """owner_type 不同(HUB vs BATTLE)→ 不是本次 target。

    守的是:玩家已经被 Hub own 时,battle 侧不得把那份记录当成自己的授权凭据。

    ★ 变异:删掉 `and rec.owner_type == owner_type` → 本条红。
    """
    assert OA.owner_record_exactly_targets(_rec(), OA.OWNER_TYPE_HUB, TARGET) is False


@pytest.mark.parametrize(
    "field,value",
    [
        ("pod_name", "pandora-battle-other"),
        ("instance_uid", "gs-uid-B"),
        ("assignment_or_allocation_id", "3f2a1c0d-1111-4222-8333-444455556666"),
        ("release_track", "canary"),
    ],
)
def test_exactly_targets_rejects_any_identity_drift(field: str, value: str):
    """exact 身份的每一格都必须比。

    守的是滚动升级硬门:旧 owner binary 会把"同物理实例、不同 allocation"当成 no-op,
    RPC 返回成功却把**旧** target 原样带回。少比任何一格,调用方就会拿着一份指向旧
    allocation / 旧轨的记录去交付 READY(§9.21 同一对局固定 release track 就此断掉)。

    ★ 变异:删掉 `and rec.release_track == target.release_track` → 本条
      release_track 参数红。
    """
    assert OA.owner_record_exactly_targets(_rec(**{field: value}), OA.OWNER_TYPE_BATTLE, TARGET) is False


# ════════════════════════════════════════════════════════════════════════════
# begin_one_player —— CAS 期望值 / fail-closed / 回读判定
# ════════════════════════════════════════════════════════════════════════════


async def test_begin_uses_queried_epoch_as_cas_expectation():
    """`expect_epoch` 必须是**刚 Query 到的** owner_epoch,不是 0、不是推算值。

    守的是「PENDING → ADMITTED 必须 CAS / 条件更新」链条的上游:BeginTransition 的
    条件就是 expect_epoch。传错(尤其是传 0 或传本地缓存)等于把条件更新退化成盲写,
    并发写者互相覆盖 → 同一玩家出现两个 owner。

    ★ 变异:把 `begin_one_player` 里
      `await auth.begin_transition(player_id, rec.owner_epoch, ...)` 的第二个实参
      改成 `0` → 本条红。
    """
    auth = _Auth()
    auth.query[P1] = [_rec(owner_epoch=42)]
    got, created = await OA.begin_one_player(auth, P1, OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    assert len(auth.begin_calls) == 1
    pid, expect_epoch, op, otype, tgt = auth.begin_calls[0]
    assert (pid, expect_epoch, otype, tgt) == (P1, 42, OA.OWNER_TYPE_BATTLE, TARGET)
    assert placement.valid_operation_id(op) is True
    assert (got.owner_epoch, created) == (43, True)


async def test_begin_query_failure_never_reaches_begin():
    """Query 失败必须原样上抛,且**一次 Begin 都不许发**。

    守的是 §9.22「查询不可判定 → UNKNOWN,不得冒充 OFFLINE / 空闲」。把查询失败当成
    "这个玩家没有归属"继续写,正是把第二台 DS 放进来的那条路径 —— 旧 DS 可能还在跑。

    ★ 变异:在 `begin_one_player` 里把 `rec = await auth.query_owner(player_id)` 包成
      `try/except` 并回退 `rec = OwnerRecordView()` → 本条红。
    """
    auth = _Auth()
    auth.query[P1] = [_boom("redis down")]
    with pytest.raises(errcode.PandoraError) as ei:
        await OA.begin_one_player(auth, P1, OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    assert ei.value.code == errcode.ErrUnavailable
    assert auth.begin_calls == []
    assert auth.query_calls == [P1]  # 也没有多余的回读


async def test_begin_epoch_conflict_does_not_readback_or_retry():
    """EPOCH_CONFLICT 立即上抛:不回读、不拿旧 target 重试。

    守的是「旧 epoch 的写一律拒」。冲突恰恰可能说明**更新的归属已经胜出**;第二次写
    旧 target 会把 winner 回滚,玩家被从新 DS 抢回旧 DS。正确动作是让调用方重走整条
    分配链并重读 allocation。

    ★ 变异:把 `if errcode.as_code(exc) == errcode.ErrOwnerEpochConflict: raise` 这两行
      删掉(让冲突也走回读分支)→ 本条红(query 会被调用 2 次)。
    """
    auth = _Auth()
    winner = _rec(owner_epoch=99, pod_name="pandora-battle-winner")
    auth.query[P1] = [_rec(owner_epoch=11)]
    auth.begin[P1] = [_conflict(winner)]
    with pytest.raises(errcode.PandoraError) as ei:
        await OA.begin_one_player(auth, P1, OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    assert ei.value.code == errcode.ErrOwnerEpochConflict
    assert ei.value.current_record is winner
    assert auth.query_calls == [P1]  # 无回读
    assert len(auth.begin_calls) == 1  # 无重试


async def test_begin_rejects_non_exact_success_from_stale_binary():
    """Begin "成功"但回传的不是本次 target → `ErrInvalidState`,不得当成写成功。

    守的是滚动升级期旧 owner binary 的 no-op 回传:它返回 nil error,却把旧 allocation
    的记录原样带回。只看错误码就会拿着指向旧实例的记录交付 READY。

    ★ 变异:把 `if not owner_record_exactly_targets(got, owner_type, target):` 整段
      删掉(直接 `return got, got.operation_id == requested_operation`)→ 本条红。
    """
    auth = _Auth()
    stale = _rec(assignment_or_allocation_id="0000ffff-1111-4222-8333-444455556666")
    auth.query[P1] = [_rec()]
    auth.begin[P1] = [stale]
    with pytest.raises(errcode.PandoraError) as ei:
        await OA.begin_one_player(auth, P1, OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    assert ei.value.code == errcode.ErrInvalidState
    assert ei.value.current_record is stale


async def test_begin_readback_exact_with_requested_operation_counts_as_created():
    """Begin 回包丢失 + 回读 exact + operation 是**本次铸的** → 判定为本次创建。

    守的是"回包丢失不等于未提交"。判成未提交的后果:上层会去 cleanup allocation/Pod,
    而权威里那份 PENDING 归属指向的正是刚被删掉的实例 —— login 的 query-first 会把这
    台死 Pod 当 exact target 下发,玩家反复 Travel 到不存在的实例。

    ★ 变异:把回读分支的
      `return observed, observed.operation_id == requested_operation` 改成
      `return observed, False` → 本条红(created 变 False)。
    """
    auth = _Auth()
    minted: list[str] = []

    def _capture(operation_id: str, expect_epoch: int):
        minted.append(operation_id)
        raise _boom("begin reply lost")

    auth.query[P1] = [_rec(owner_epoch=11), lambda: _rec(operation_id=minted[0], owner_epoch=12)]
    auth.begin[P1] = [_capture]

    got, created = await OA.begin_one_player(auth, P1, OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    assert created is True
    assert got.operation_id == minted[0]
    assert auth.query_calls == [P1, P1]  # 初查 + 独立预算回读


async def test_begin_readback_exact_with_other_operation_is_not_created():
    """回读 exact 但 operation 是**别人**写的 → `created=False`(重复提交的幂等表达)。

    守的是"重复提交同一 target 不得产生第二个 owner,也不得让本次调用把别人的 grant
    纳入自己的回滚集合"。写成 True 的后果:本批失败回滚时会把另一个 binder 正赖以
    交付 READY 的归属 Release 掉 —— 玩家被从一台正常的 DS 上踢下来。

    ★ 变异:把回读分支的
      `return observed, observed.operation_id == requested_operation` 改成
      `return observed, True` → 本条红。
    """
    auth = _Auth()
    auth.query[P1] = [_rec(owner_epoch=11), _rec(operation_id=OP_OTHER, owner_epoch=12)]
    auth.begin[P1] = [_boom("begin reply lost")]

    got, created = await OA.begin_one_player(auth, P1, OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    assert created is False
    assert got.operation_id == OP_OTHER


async def test_begin_readback_unreachable_raises_outcome_unknown():
    """Begin 失败 + 回读也不可达 → `OwnerBeginOutcomeUnknown`(对外仍是 UNAVAILABLE)。

    守的是"结果未知 ≠ 未提交"。上层据此保留 allocation/Pod 与本批已写 owner;判成普通
    失败去 cleanup,就会留下一份指向已删除 Pod 的归属,而 owner 侧没有任何归属记录的
    TTL / 回收路径 —— 残留不会自己消失,只能人工清。

    ★ 变异:把回读失败分支里的 `cause=OWNER_BEGIN_OUTCOME_UNKNOWN` 删掉 → 本条红。
    """
    auth = _Auth()
    auth.query[P1] = [_rec(owner_epoch=11), _boom("readback down")]
    auth.begin[P1] = [_boom("begin reply lost")]

    with pytest.raises(errcode.PandoraError) as ei:
        await OA.begin_one_player(auth, P1, OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    assert ei.value.code == errcode.ErrUnavailable
    assert OA.is_owner_begin_outcome_unknown(ei.value) is True


async def test_begin_readback_shows_other_target_raises_original_error():
    """回读显示归属已不是本次 target → 抛**原始** Begin 错误,而不是 outcome-unknown。

    守的是补偿方向:此时"本 allocation 不再拥有该玩家"是确定事实,允许正常回滚 / 清理。
    误标成 outcome-unknown 会让上层永久保留一台其实该删的 Pod,占位泄漏。

    ★ 变异:把 `raise begin_err`(回读非 exact 分支)改成
      `raise errcode.PandoraError(errcode.ErrUnavailable, "x", cause=OWNER_BEGIN_OUTCOME_UNKNOWN)`
      → 本条红。
    """
    auth = _Auth()
    auth.query[P1] = [_rec(owner_epoch=11), _rec(pod_name="pandora-battle-other")]
    auth.begin[P1] = [_boom("begin reply lost")]

    with pytest.raises(errcode.PandoraError) as ei:
        await OA.begin_one_player(auth, P1, OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    assert ei.value.code == errcode.ErrUnavailable
    assert OA.is_owner_begin_outcome_unknown(ei.value) is False


async def test_begin_timeout_mid_flight_still_reads_back():
    """预算在 **Begin 途中**耗尽 = 与回包丢失同类,必须回读判定。

    守的是:超时不能证明服务端没 commit。直接判失败去 cleanup 的后果同上一条 —— 一份
    指向已删除 Pod 的 PENDING 残留。

    ★ 变异:把 `except TimeoutError as exc:` 分支里的 `if not begin_attempted: raise`
      改成无条件 `raise` → 本条红(回读不会发生)。
    """
    auth = _Auth()

    async def _slow_begin(*_a, **_k):
        await asyncio.sleep(5)

    auth.query[P1] = [_rec(owner_epoch=11), _rec(operation_id=OP_OTHER, owner_epoch=12)]
    auth.begin_calls_orig = auth.begin_transition

    async def _begin(player_id, expect_epoch, operation_id, owner_type, target):
        auth.begin_calls.append((player_id, expect_epoch, operation_id, owner_type, target))
        await _slow_begin()

    auth.begin_transition = _begin  # type: ignore[method-assign]

    got, created = await OA.begin_one_player(auth, P1, OA.OWNER_TYPE_BATTLE, TARGET, 0.05)
    assert created is False
    assert got.operation_id == OP_OTHER
    assert auth.query_calls == [P1, P1]


async def test_begin_timeout_before_begin_propagates_without_readback():
    """预算在 **Query 途中**耗尽 → 一次 Begin 都没发出,不存在"可能已提交",直接上抛。

    守的是"不要给根本没发生的写做回读判定":多一次回读只会把一个确定的失败拖成不确定,
    上层于是保留本该清理的 allocation。

    ★ 变异:把 `if not begin_attempted: raise` 删掉 → 本条红(会多一次 query)。
    """
    auth = _Auth()

    async def _slow_query(player_id: int):
        auth.query_calls.append(player_id)
        await asyncio.sleep(5)

    auth.query_owner = _slow_query  # type: ignore[method-assign]
    with pytest.raises(TimeoutError):
        await OA.begin_one_player(auth, P1, OA.OWNER_TYPE_BATTLE, TARGET, 0.05)
    assert auth.begin_calls == []
    assert auth.query_calls == [P1]


# ════════════════════════════════════════════════════════════════════════════
# is_owner_begin_outcome_unknown —— cause 链判定
# ════════════════════════════════════════════════════════════════════════════


def test_outcome_unknown_walks_cause_chain():
    """必须沿 `cause` 链判,不能只看最外层。

    守的是 `errcode.NewCause` 的语义(外层 code 给客户端、内层 cause 给控制流)。只看
    最外层等于把这个区分丢掉:outcome-unknown 会被当普通失败去回滚 —— 正是本模块最不
    能发生的那件事。

    ★ 变异:把 `is_owner_begin_outcome_unknown` 的 while 循环体改成只判一次
      (`return isinstance(exc, OwnerBeginOutcomeUnknown)`)→ 本条红。
    """
    inner = errcode.PandoraError(
        errcode.ErrUnavailable, "readback failed", cause=OA.OWNER_BEGIN_OUTCOME_UNKNOWN
    )
    outer = errcode.PandoraError(errcode.ErrUnavailable, "wrapped", cause=inner)
    assert OA.is_owner_begin_outcome_unknown(outer) is True
    assert OA.is_owner_begin_outcome_unknown(inner) is True
    assert OA.is_owner_begin_outcome_unknown(_boom()) is False
    assert OA.is_owner_begin_outcome_unknown(None) is False


def test_outcome_unknown_chain_is_bounded():
    """病态自环 cause 链不得把判定卡死。

    守的是有界性:一个自引用的 cause 会让沿链遍历变成死循环,而这段代码跑在分配失败的
    补偿路径上 —— 卡死等于整台 DS 的分配链停摆。
    """
    a = errcode.PandoraError(errcode.ErrUnavailable, "a")
    a.cause = a
    assert OA.is_owner_begin_outcome_unknown(a) is False


# ════════════════════════════════════════════════════════════════════════════
# owner_begin_players —— fail-closed + 精确回滚
# ════════════════════════════════════════════════════════════════════════════


async def test_begin_players_all_success_no_release():
    """全员成功时零 Release。

    守的是"补偿只撤销本次**未交付**的写"(验收底线第 4 条)。成功路径上多一次 Release
    就是把刚写好的归属自己抹掉。

    ★ 变异:把 `if failure is None: return` 改成 `if failure is None: pass` 并让其后的
      `await rollback_owner_begins(auth, granted)` 无条件执行 → 本条红。
    """
    auth = _Auth()
    await OA.owner_begin_players(auth, [P1, P2, P3], OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    assert auth.release_calls == []
    assert [c[0] for c in auth.begin_calls] == [P1, P2, P3]


async def test_begin_players_nil_auth_and_empty_roster_are_noop():
    """`auth is None` / 空名单 → 纯 no-op。

    守的是:owner 未部署属部署形态问题,不在本函数收敛;而空名单不该触发任何 RPC。
    """
    await OA.owner_begin_players(None, [P1], OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    auth = _Auth()
    await OA.owner_begin_players(auth, [], OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    assert auth.query_calls == [] and auth.begin_calls == []


async def test_begin_players_rolls_back_in_reverse_with_exact_triples():
    """部分失败 → 逆序精确回滚,且 `(player, epoch, operation)` 三元组取**权威回传值**。

    守的是两件事:
      ① fail-closed —— 任一玩家写不进权威就整体失败,绝不把 READY 交付出去(否则玩家
         可能同时被两台 DS 认领);
      ② 精确回滚 —— 调用方紧接着就会删掉那台 Pod,不回滚就留下一批"归属指向已删除
         实例"的 PENDING 记录,且没有任何路径能清掉它们(owner 侧无归属 TTL)。
      epoch/operation 若自铸或推算,Release 会因不全等而静默 no-op —— 看起来回滚成功了,
      残留还在。

    ★ 变异:把 `rollback_owner_begins` 里的 `for grant in reversed(granted):` 改成
      `for grant in granted:` → 本条红(顺序断言不符)。
    """
    auth = _Auth()
    auth.query[P1] = [_rec(owner_epoch=10)]
    auth.query[P2] = [_rec(owner_epoch=20)]
    auth.query[P3] = [_rec(owner_epoch=30), _rec(pod_name="pandora-battle-elsewhere")]
    auth.begin[P3] = [_boom("owner down")]

    with pytest.raises(errcode.PandoraError):
        await OA.owner_begin_players(auth, [P1, P2, P3], OA.OWNER_TYPE_BATTLE, TARGET, 5.0)

    # 逆序;epoch = 权威回传的 expect_epoch+1;operation = 权威回传的那一个。
    op1 = auth.begin_calls[0][2]
    op2 = auth.begin_calls[1][2]
    assert auth.release_calls == [(P2, 21, op2), (P1, 11, op1)]


async def test_begin_players_never_rolls_back_someone_elses_grant():
    """并发同 target no-op(`created=False`)的记录**不进**本次回滚集合。

    守的是"重复提交不得产生第二个 owner,也不得误撤别人的 grant"。P2 的记录是另一个
    binder 写的、正被它用来交付 READY;把它 Release 掉 = 把玩家从一台正常 DS 上踢下来。

    ★ 变异:把 `owner_begin_players` 里的 `if created:` 改成 `if True:` → 本条红。
    """
    auth = _Auth()
    auth.query[P1] = [_rec(owner_epoch=10)]
    auth.query[P2] = [_rec(owner_epoch=20)]
    # P2 的 Begin 返回既有记录(operation 不是本次铸的)= 并发同 target no-op。
    auth.begin[P2] = [_rec(owner_epoch=21, operation_id=OP_OTHER)]
    auth.query[P3] = [_rec(owner_epoch=30), _rec(pod_name="pandora-battle-elsewhere")]
    auth.begin[P3] = [_boom("owner down")]

    with pytest.raises(errcode.PandoraError):
        await OA.owner_begin_players(auth, [P1, P2, P3], OA.OWNER_TYPE_BATTLE, TARGET, 5.0)

    assert [c[0] for c in auth.release_calls] == [P1]
    assert auth.release_calls[0][2] != OP_OTHER


async def test_begin_players_outcome_unknown_keeps_every_grant():
    """`OwnerBeginOutcomeUnknown` 是唯一**不许回滚**的失败。

    守的是:当前玩家可能已提交而回读又不可达。回滚半批再留下一个未知提交,只会主动
    制造更难恢复的不一致;保留整批则可由 claim loser 在 owner 恢复后只读验证全员 exact
    后收敛。

    ★ 变异:把 `if outcome_unknown: raise failure` 这两行删掉(让它继续往下走回滚)
      → 本条红。
    """
    auth = _Auth()
    auth.query[P1] = [_rec(owner_epoch=10)]
    auth.query[P2] = [_rec(owner_epoch=20), _boom("readback down")]
    auth.begin[P2] = [_boom("begin reply lost")]

    with pytest.raises(errcode.PandoraError) as ei:
        await OA.owner_begin_players(auth, [P1, P2], OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    assert OA.is_owner_begin_outcome_unknown(ei.value) is True
    assert auth.release_calls == []


async def test_begin_players_rollback_failure_does_not_mask_original_error():
    """回滚是 best-effort:它自己失败也不得盖掉调用方拿到的原始错误。

    守的是排障可用性 —— 上层按错误码决定"重试 / 放弃 / 保留 Pod",拿到一个回滚错误会
    判错方向。owner 此时本就不可用,重试也写不进去,下次分配的 CAS 覆盖仍是兜底。

    ★ 变异:把 `rollback_owner_begins` 里 `except BaseException as exc:  # best-effort`
      改成 `raise` → 本条红(抛出的会变成回滚错误)。
    """
    auth = _Auth()
    auth.query[P1] = [_rec(owner_epoch=10)]
    auth.query[P2] = [_rec(owner_epoch=20), _rec(pod_name="pandora-battle-elsewhere")]
    auth.begin[P2] = [errcode.PandoraError(errcode.ErrInvalidState, "the original failure")]
    auth.release_script[P1] = [_boom("release also down")]

    with pytest.raises(errcode.PandoraError) as ei:
        await OA.owner_begin_players(auth, [P1, P2], OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    assert ei.value.code == errcode.ErrInvalidState
    assert ei.value.msg == "the original failure"
    assert [c[0] for c in auth.release_calls] == [P1]


async def test_begin_players_budget_exhaustion_is_failure_not_skip():
    """超预算 = **整体失败 + 回滚**,不是"跳过剩余玩家"。

    守的是:一局里部分玩家有归属、部分没有,比整局失败重来更难收敛,还会让 Admit 侧
    看到半截状态。

    ★ 变异:把 `if remaining <= 0:` 分支里的 `failure = TimeoutError(...)` + `break`
      改成裸 `break` → 本条红(不再抛异常)。
    """
    auth = _Auth()
    auth.query[P1] = [_rec(owner_epoch=10)]
    with pytest.raises(TimeoutError):
        await OA.owner_begin_players(auth, [P1, P2], OA.OWNER_TYPE_BATTLE, TARGET, -1.0)
    assert auth.begin_calls == []


# ════════════════════════════════════════════════════════════════════════════
# owner_verify_players_exact —— claim loser 的零副作用只读门
# ════════════════════════════════════════════════════════════════════════════


async def test_verify_exact_is_read_only():
    """只读门:零 Begin / 零 Release。

    守的是"同一批玩家的多个并发 binder 不得互相撤销"。loser 若也跑 Begin/Release,
    per-player 的写无法原子提交整批,它会把 winner 已经依赖的 grant 当成自己的补偿对象。

    ★ 变异:在 `owner_verify_players_exact` 的循环里补一句
      `await auth.begin_transition(player_id, rec.owner_epoch, placement.new_operation_id(), owner_type, target)`
      → 本条红。
    """
    auth = _Auth()
    auth.query[P1] = [_rec()]
    auth.query[P2] = [_rec()]
    await OA.owner_verify_players_exact(auth, [P1, P2], OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    assert auth.begin_calls == [] and auth.release_calls == []
    assert auth.query_calls == [P1, P2]


async def test_verify_exact_fails_closed_on_any_drift():
    """roster 里任一玩家漂移即 fail-closed(`ErrUnavailable`),并**立即停止**后续查询。

    守的是 §9.22:归属未全量定案就交付 READY = 玩家可能同时被两台 DS 认领。

    ★ 变异:把 `raise errcode.PandoraError(...)` 改成 `continue` → 本条红。
    """
    auth = _Auth()
    auth.query[P1] = [_rec()]
    auth.query[P2] = [_rec(instance_uid="gs-uid-B")]
    with pytest.raises(errcode.PandoraError) as ei:
        await OA.owner_verify_players_exact(auth, [P1, P2, P3], OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    assert ei.value.code == errcode.ErrUnavailable
    assert auth.query_calls == [P1, P2]  # P3 未被查询


async def test_verify_exact_propagates_query_failure():
    """Query 失败原样上抛,不得降级成"这个玩家不 exact"或"就当 exact"。

    守的是 UNKNOWN 不冒充确定值 —— 两个方向都错:当成 exact 会放行双 DS;当成 not-exact
    只是碰巧安全,但会把一个可重试的抖动伪装成永久漂移。
    """
    auth = _Auth()
    auth.query[P1] = [_boom("owner down")]
    with pytest.raises(errcode.PandoraError):
        await OA.owner_verify_players_exact(auth, [P1], OA.OWNER_TYPE_BATTLE, TARGET, 5.0)


async def test_verify_exact_noop_without_auth_or_players():
    """`auth is None` / 空名单 → no-op。"""
    await OA.owner_verify_players_exact(None, [P1], OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    auth = _Auth()
    await OA.owner_verify_players_exact(auth, [], OA.OWNER_TYPE_BATTLE, TARGET, 5.0)
    assert auth.query_calls == []


# ════════════════════════════════════════════════════════════════════════════
# owner_release_abandoned_players_weak —— 只清理属于自己的残留
# ════════════════════════════════════════════════════════════════════════════


async def test_release_abandoned_compare_deletes_with_record_values():
    """释放必须带**刚读到的** `owner_epoch + operation_id`(compare-delete)。

    守的是 §9.23「迟到 Logout 只能删自己」。带本地推算的值会被 owner 侧判不匹配而静默
    no-op —— 玩家的归属永远停在一台已被删除的 Pod 上,恢复查询反复把死目标下发给客户端。

    ★ 变异:把 `await auth.release_owner(player_id, rec.owner_epoch, rec.operation_id)`
      的第二个实参改成 `rec.owner_epoch - 1` → 本条红。
    """
    auth = _Auth()
    auth.query[P1] = [_rec(owner_epoch=77, operation_id=OP_OTHER)]
    await OA.owner_release_abandoned_players_weak(auth, [P1], POD, UID, 5.0)
    assert auth.release_calls == [(P1, 77, OP_OTHER)]


async def test_release_abandoned_skips_players_already_migrated():
    """exact 身份门:记录已指向别的 pod / uid / 别的 owner 类型 → **跳过**,不得释放。

    守的是"精确只清理属于自己 epoch 的残留"。玩家已被迁到新 DS 时误删,等于把一份
    **活着的**归属抹掉:新 DS 上的玩家立刻失去授权,而 login 会把他当无归属重新进场。

    ★ 变异:把 exact 身份门的三条判据里
      `or rec.instance_uid != self_uid` 删掉 → 本条 `uid 换代` 参数红。
    """
    auth = _Auth()
    auth.query[P1] = [_rec(pod_name="pandora-battle-new")]  # 已迁到新 pod
    auth.query[P2] = [_rec(instance_uid="gs-uid-B")]  # 同名 pod、UID 换代
    auth.query[P3] = [_rec(owner_type=OA.OWNER_TYPE_HUB)]  # 已回 Hub
    await OA.owner_release_abandoned_players_weak(auth, [P1, P2, P3], POD, UID, 5.0)
    assert auth.release_calls == []


async def test_release_abandoned_requires_self_identity():
    """`self_pod` / `self_uid` 任一为空 → 整体 no-op,连 Query 都不发。

    守的是:身份不全就没有"哪些残留属于我"的判据。空串会在 exact 门里和记录的空值
    互相匹配,把毫不相干玩家的归属删掉。

    ★ 变异:把入口 `or self_pod == "" or self_uid == ""` 删掉 → 本条红。
    """
    auth = _Auth()
    auth.query[P1] = [_rec()]
    await OA.owner_release_abandoned_players_weak(auth, [P1], "", UID, 5.0)
    await OA.owner_release_abandoned_players_weak(auth, [P1], POD, "", 5.0)
    await OA.owner_release_abandoned_players_weak(None, [P1], POD, UID, 5.0)
    assert auth.query_calls == [] and auth.release_calls == []


async def test_release_abandoned_is_weak_and_continues_after_failures():
    """弱依赖:单个玩家 Query / Release 失败只计数,**继续**处理后面的玩家。

    守的是判弃补偿链的收敛性 —— 一个玩家的抖动不该让同局其他人的归属残留下来
    (他们会一直被下发死目标)。

    ★ 变异:把 query 失败分支的 `continue` 改成 `return` → 本条红。
    """
    auth = _Auth()
    auth.query[P1] = [_boom("query down")]
    auth.query[P2] = [_rec(owner_epoch=55, operation_id=OP_OTHER2)]
    auth.query[P3] = [_rec(owner_epoch=66, operation_id=OP_OTHER)]
    auth.release_script[P2] = [_boom("release down")]
    await OA.owner_release_abandoned_players_weak(auth, [P1, P2, P3], POD, UID, 5.0)
    assert auth.query_calls == [P1, P2, P3]
    assert auth.release_calls == [(P2, 55, OP_OTHER2), (P3, 66, OP_OTHER)]


# ════════════════════════════════════════════════════════════════════════════
# owner_admit_census_weak —— PENDING → ADMITTED 的 CAS 与屏障
# ════════════════════════════════════════════════════════════════════════════


async def test_admit_census_uses_record_epoch_operation_and_target():
    """Admit 必须带记录自身的 `owner_epoch + operation_id + target`。

    守的是「`PENDING → ADMITTED` 是 CAS / 条件更新,不是先查后写」。三者任一自铸或
    推算,owner 侧的全等校验就会拒(静默 no-op),而调用方以为准入成功 —— 玩家在
    权威里永远停在 PENDING,重连恢复链会把他当"尚未准入"反复重放。

    ★ 变异:把 `await auth.admit(player_id, rec.owner_epoch, rec.operation_id, target)`
      的第二个实参改成 `rec.owner_epoch + 1` → 本条红。
    """
    auth = _Auth()
    rec = _rec(owner_epoch=31, operation_id=OP_OTHER, phase=OA.OWNER_PHASE_PENDING)
    auth.query[P1] = [rec]
    admitted: dict[str, float] = {}
    await OA.owner_admit_census_weak(
        auth, admitted, [P1], OA.OWNER_TYPE_BATTLE, POD, UID, 5.0
    )
    assert auth.admit_calls == [
        (
            P1,
            31,
            OP_OTHER,
            OwnerTargetView(
                pod_name=POD,
                instance_uid=UID,
                instance_epoch=EPOCH,
                assignment_or_allocation_id=ALLOC,
                release_track=TRACK,
            ),
        )
    ]
    assert f"{UID}|{P1}" in admitted


async def test_admit_census_barrier_closed_is_not_cached():
    """`admit_not_before` 屏障未开(`retry_after_ms > 0`)→ **不得**写进已准入缓存。

    这条直接守「屏障未开时新 DS 不得进入 ADMITTED」。缓存下去的后果是:本实例此后每轮
    心跳都命中缓存而跳过 Query→Admit,于是**永远**不再尝试准入 —— 玩家在权威里停在
    PENDING,而本地却把他当已准入继续服务;屏障保护的"旧 DS 最晚停止 < 新 DS 最早开始"
    时序就此失效。

    ★ 变异:把 admit 异常分支里 `if retry_after > 0: continue` 改成
      `if retry_after > 0: admitted[key] = now; continue` → 本条红。
    """
    auth = _Auth()
    auth.query[P1] = [_rec(phase=OA.OWNER_PHASE_PENDING)]
    auth.admit_script[P1] = [_wait(1500)]
    admitted: dict[str, float] = {}
    await OA.owner_admit_census_weak(
        auth, admitted, [P1], OA.OWNER_TYPE_BATTLE, POD, UID, 5.0
    )
    assert admitted == {}
    assert len(auth.admit_calls) == 1

    # 下一轮心跳必须**重新**尝试(这正是"不缓存"的目的)。
    auth.admit_script[P1] = [0]
    await OA.owner_admit_census_weak(
        auth, admitted, [P1], OA.OWNER_TYPE_BATTLE, POD, UID, 5.0
    )
    assert list(admitted) == [f"{UID}|{P1}"]
    assert len(auth.admit_calls) == 2


async def test_admit_census_failure_is_not_cached_either():
    """Admit 普通失败(无 retry_after)同样不得缓存,下轮继续重试。

    守的是:失败被缓存 = 这名玩家的准入永远不会再被提交,与屏障那条同样后果。

    ★ 变异:把 `admitted[key] = now` 从 try 块内移到 `except` 之后(无条件执行)
      → 本条红。
    """
    auth = _Auth()
    auth.query[P1] = [_rec(phase=OA.OWNER_PHASE_PENDING)]
    auth.admit_script[P1] = [_boom("admit down")]
    admitted: dict[str, float] = {}
    await OA.owner_admit_census_weak(
        auth, admitted, [P1], OA.OWNER_TYPE_BATTLE, POD, UID, 5.0
    )
    assert admitted == {}


async def test_admit_census_skips_records_pointing_elsewhere():
    """记录不指向本实例(pod / uid / owner_type 任一不同)→ 不 Admit、不缓存。

    守的是"本实例只能断言自己确实在服务的玩家"。对一份指向别的 DS 的 PENDING 记录代
    提交 Admit,等于替另一台 DS 打开准入 —— 屏障还没到点时就是双 DS。

    ★ 变异:把这三条判据里 `or rec.pod_name != self_pod` 删掉 → 本条 P1 参数红。
    """
    auth = _Auth()
    auth.query[P1] = [_rec(pod_name="pandora-battle-other")]
    auth.query[P2] = [_rec(instance_uid="gs-uid-B")]
    auth.query[P3] = [_rec(owner_type=OA.OWNER_TYPE_HUB)]
    admitted: dict[str, float] = {}
    await OA.owner_admit_census_weak(
        auth, admitted, [P1, P2, P3], OA.OWNER_TYPE_BATTLE, POD, UID, 5.0
    )
    assert auth.admit_calls == []
    assert admitted == {}


async def test_admit_census_already_admitted_needs_no_rpc():
    """记录已是 ADMITTED → 只写缓存,不再发 Admit(重复提交必须幂等且零额外写)。"""
    auth = _Auth()
    auth.query[P1] = [_rec(phase=OA.OWNER_PHASE_ADMITTED)]
    admitted: dict[str, float] = {}
    await OA.owner_admit_census_weak(
        auth, admitted, [P1], OA.OWNER_TYPE_BATTLE, POD, UID, 5.0
    )
    assert auth.admit_calls == []
    assert list(admitted) == [f"{UID}|{P1}"]


async def test_admit_census_cache_hit_skips_query():
    """缓存命中 → 本轮零 RPC(心跳每 ~5s 一轮,不能每轮全员重查)。"""
    auth = _Auth()
    admitted = {f"{UID}|{P1}": OA.time.monotonic()}
    await OA.owner_admit_census_weak(
        auth, admitted, [P1], OA.OWNER_TYPE_BATTLE, POD, UID, 5.0
    )
    assert auth.query_calls == [] and auth.admit_calls == []


async def test_admit_census_prunes_before_empty_roster_early_return():
    """剪枝必须发生在"本轮无玩家就早退"**之前**。

    守的是一条很隐蔽的路径:最后一名玩家离场时 census 为空,若此时早退,他的 admitted
    项就永久残留。等他回流本实例(owner epoch 已推进、新一份 PENDING)时会被缓存命中
    误吞,跳过 Query→Admit —— **新纪元的 Admit 永远不会提交**,玩家在权威里停在 PENDING。

    ★ 变异:把 `if not players: return` 这两行移到剪枝循环**之前** → 本条红。
    """
    auth = _Auth()
    admitted = {f"{UID}|{P1}": 1.0, f"{UID}|{P2}": 2.0, "other-uid|999": 3.0}
    await OA.owner_admit_census_weak(
        auth, admitted, [], OA.OWNER_TYPE_BATTLE, POD, UID, 5.0
    )
    # 本实例前缀的项全被剪掉;别的(已销毁)实例的项交给 TTL sweep,这里不动。
    assert admitted == {"other-uid|999": 3.0}


async def test_admit_census_prunes_departed_players_only():
    """剪枝只清"本实例上已离场"的项,不碰仍在场的、也不碰别的实例的。"""
    auth = _Auth()
    now = OA.time.monotonic()
    admitted = {f"{UID}|{P1}": now, f"{UID}|{P2}": now, "other-uid|777": now}
    await OA.owner_admit_census_weak(
        auth, admitted, [P1], OA.OWNER_TYPE_BATTLE, POD, UID, 5.0
    )
    assert set(admitted) == {f"{UID}|{P1}", "other-uid|777"}


async def test_admit_census_noop_without_auth():
    """`auth is None` → 连剪枝都不做(owner 未部署,整条链不启用)。"""
    admitted = {f"{UID}|{P1}": 1.0}
    await OA.owner_admit_census_weak(
        None, admitted, [], OA.OWNER_TYPE_BATTLE, POD, UID, 5.0
    )
    assert admitted == {f"{UID}|{P1}": 1.0}


# ════════════════════════════════════════════════════════════════════════════
# sweep_stale_owner_admitted —— §9.18 进程内容器有界
# ════════════════════════════════════════════════════════════════════════════


def test_sweep_stale_admitted_removes_old_and_corrupt_values():
    """老化项与**类型坏掉**的项都要删。

    守的是有界性不建立在"理论上不会发生"之上:一个非 float 的值会让它永远通不过时间
    比较,那一项就成了永久驻留的泄漏点。Battle DS 打完即销毁、InstanceUID 永不复用 ——
    不老化回收会随累计对局数单调增长,长压测下 OOM。

    ★ 变异:把 `if not isinstance(value, float) or value < cutoff` 改成
      `if value < cutoff` → 本条红(坏值项会因 TypeError 或残留而不符)。
    """
    admitted = {
        "uid-a|1": 100.0,  # 老:删
        "uid-b|2": 900.0,  # 新:留
        "uid-c|3": "corrupt",  # 类型坏:删
        "uid-d|4": None,  # 类型坏:删
        "uid-e|5": True,  # bool 不是 float:删
    }
    OA.sweep_stale_owner_admitted(admitted, cutoff=500.0)
    assert admitted == {"uid-b|2": 900.0}


def test_sweep_stale_admitted_keeps_boundary_value():
    """恰好等于 cutoff 的项保留(`<` 不是 `<=`),与 Go 的 `t.Before(cutoff)` 一致。"""
    admitted = {"uid-a|1": 500.0}
    OA.sweep_stale_owner_admitted(admitted, cutoff=500.0)
    assert admitted == {"uid-a|1": 500.0}


# ════════════════════════════════════════════════════════════════════════════
# owner_lease —— 心跳返回前的租约双写门
# ════════════════════════════════════════════════════════════════════════════


class _Renewer:
    def __init__(self, exc: BaseException | None = None) -> None:
        self.exc = exc
        self.calls: list[tuple] = []

    async def renew_instance_lease(
        self, pod_name: str, instance_uid: str, instance_epoch: int, release_track: str
    ) -> None:
        self.calls.append((pod_name, instance_uid, instance_epoch, release_track))
        if self.exc is not None:
            raise self.exc


async def test_lease_gate_noop_without_renewer():
    """`renewer is None` → no-op(未配 owner_addr,migrate 前行为不变)。"""
    await OL.renew_owner_lease_gate(None, True, POD, UID, EPOCH, TRACK)


async def test_lease_gate_forwards_exact_instance_identity():
    """续租必须带**完整** exact 实例身份四元组。

    守的是屏障计算的输入正确性:玩家级 owner lease 由实例租约派生,`BeginTransition`
    的 `admit_not_before` 按它算。少一格 / 传错一格,权威侧就会给另一台实例算出偏早的
    屏障 —— 同一玩家可以同时在两台 DS 可玩。
    """
    r = _Renewer()
    await OL.renew_owner_lease_gate(r, False, POD, UID, EPOCH, TRACK)
    assert r.calls == [(POD, UID, EPOCH, TRACK)]


async def test_lease_gate_required_true_propagates_failure():
    """contract 档(`required=True`):续租失败 = **心跳失败**。

    守的是时序闭合:DS 拿不到心跳响应就不会延长本地租约,连续失败按 fence 契约自我
    fencing。吞掉这个错误 = 权威侧租约滞后、DS 却照常继续玩 —— 屏障提前打开时就是双 DS。

    ★ 变异:把 `if required: raise` 删掉 → 本条红。
    """
    boom = _boom("owner lease rpc failed")
    r = _Renewer(boom)
    with pytest.raises(errcode.PandoraError) as ei:
        await OL.renew_owner_lease_gate(r, True, POD, UID, EPOCH, TRACK)
    assert ei.value is boom


async def test_lease_gate_required_false_swallows_failure():
    """migrate 档(`required=False`):失败只告警,心跳照常成功。

    守的是可用性方向:owner 刚上线时它自己的抖动不该把整批对局的心跳打挂 —— 那会让一堆
    健康 Battle DS 因"心跳超时"被判弃(§9.4 段位回滚),是拿玩家已打完的一局去换一个还
    没真正被依赖的一致性。由旧 `last_heartbeat_ms` 再入门双门并行兜底。

    ★ 变异:把 `except BaseException as exc:` 分支里 `if required: raise` 改成无条件
      `raise` → 本条红。
    """
    r = _Renewer(_boom("owner down"))
    await OL.renew_owner_lease_gate(r, False, POD, UID, EPOCH, TRACK)
    assert len(r.calls) == 1


async def test_lease_gate_never_swallows_cancellation():
    """`CancelledError` 必须穿透,即便在弱依赖档。

    守的是优雅停机(AGENTS §7 硬性要求):被宽 except 吞掉的话,停机时这条心跳路径不会
    退出,排空失效;而且一次正常取消会被记成一条**假的**「owner 降级」告警。

    ★ 变异:把 `except asyncio.CancelledError: raise` 这两行删掉 → 本条红。
    """
    r = _Renewer(asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await OL.renew_owner_lease_gate(r, False, POD, UID, EPOCH, TRACK)


# ════════════════════════════════════════════════════════════════════════════
# orphan_gameserver —— 只删"能证明属于本权威、且连续观察无人"的 GS
# ════════════════════════════════════════════════════════════════════════════

GS_A = "pandora-battle-aaa"
GS_B = "pandora-battle-bbb"
UID_A = "gs-uid-aaa"
UID_B = "gs-uid-bbb"
ALLOC_A = "aaaaaaaa-1111-4222-8333-444455556666"
ALLOC_B = "bbbbbbbb-1111-4222-8333-444455556666"


class _Battle:
    __slots__ = ("ds_pod_name", "gameserver_uid", "allocation_id")

    def __init__(self, pod: str = "", uid: str = "", alloc: str = "") -> None:
        self.ds_pod_name = pod
        self.gameserver_uid = uid
        self.allocation_id = alloc


class _Repo:
    """`BattleRecordSource` 替身。`get_battle` 支持按 match_id 注入异常。"""

    def __init__(self, battles: dict[int, object] | None = None) -> None:
        self.battles = battles or {}
        self.range_exc: BaseException | None = None
        self.get_exc: dict[int, BaseException] = {}
        self.get_calls: list[int] = []

    async def range_active_battles(self) -> list[int]:
        if self.range_exc is not None:
            raise self.range_exc
        return list(self.battles)

    async def get_battle(self, match_id: int):  # noqa: ANN201
        self.get_calls.append(match_id)
        if match_id in self.get_exc:
            raise self.get_exc[match_id]
        return self.battles.get(match_id)


class _Reconciler:
    def __init__(self, gs_list: list[AllocatedGameServerInfo] | None = None) -> None:
        self.gs_list = gs_list or []
        self.list_exc: BaseException | None = None
        self.delete_result: bool | BaseException = True
        self.delete_calls: list[tuple[str, str, str]] = []
        self.list_calls = 0

    async def list_allocated_game_servers(self) -> list[AllocatedGameServerInfo]:
        self.list_calls += 1
        if self.list_exc is not None:
            raise self.list_exc
        return list(self.gs_list)

    async def delete_allocated_game_server_exact(
        self, name: str, uid: str, expected_allocation_id: str
    ) -> bool:
        self.delete_calls.append((name, uid, expected_allocation_id))
        if isinstance(self.delete_result, BaseException):
            raise self.delete_result
        return self.delete_result


class _Ledger:
    def __init__(self, known: set[str] | None = None) -> None:
        self.known = known or set()
        self.contains_exc: BaseException | None = None
        self.prune_calls: list[int] = []
        self.contains_calls: list[str] = []

    async def record_allocation_ledger(self, allocation_id: str, at_ms: int) -> None:
        self.known.add(allocation_id)

    async def allocation_ledger_contains(self, allocation_id: str) -> bool:
        self.contains_calls.append(allocation_id)
        if self.contains_exc is not None:
            raise self.contains_exc
        return allocation_id in self.known

    async def prune_allocation_ledger(self, before_ms: int) -> int:
        self.prune_calls.append(before_ms)
        return 0


def _gs(name: str, uid: str, alloc: str = "", deleting: bool = False) -> AllocatedGameServerInfo:
    return AllocatedGameServerInfo(
        name=name, uid=uid, fleet="pandora-battle", allocation_id=alloc, deleting=deleting
    )


def _janitor(reconciler, ledger, repo, **kw) -> OG.OrphanGameServerJanitor:
    return OG.OrphanGameServerJanitor(reconciler, ledger, repo, **kw)


def _reclaim_counter(result: str) -> float:
    return OG.ORPHAN_GS_RECLAIM_COUNTER.labels(result)._value.get()


# ── 引用收集 ────────────────────────────────────────────────────────────────


async def test_collect_refs_takes_all_three_dimensions():
    """三维引用(pod_name / uid / allocation_id)必须全收。

    守的是:分配记录这三个字段由**不同写入路径**分别填,存在只有 uid 或只有
    allocation_id 的记录。少收一维 = 把这些**活跃对局**误判成孤儿 —— 误判方向是删,
    正是本清扫唯一不可接受的错误方向。

    ★ 变异:删掉 `if battle.gameserver_uid: refs.uids.add(...)` 这两行 → 本条红。
    """
    repo = _Repo(
        {
            1: _Battle(pod=GS_A),
            2: _Battle(uid=UID_B),
            3: _Battle(alloc=ALLOC_A),
        }
    )
    refs = await OG.collect_battle_game_server_refs(repo)
    assert refs.pod_names == {GS_A}
    assert refs.uids == {UID_B}
    assert refs.allocation_ids == {ALLOC_A}


async def test_collect_refs_is_fail_closed_on_read_error():
    """任一条记录读失败 → **整轮报错**,不得返回部分引用集。

    守的是防误删①「证据不可得 = 不删」。部分引用集会把仍被引用的 GS 判成孤儿。

    ★ 变异:把 `battle = await repo.get_battle(match_id)` 包 try/except 后 `continue`
      → 本条红。
    """
    repo = _Repo({1: _Battle(pod=GS_A), 2: _Battle(pod=GS_B)})
    repo.get_exc[2] = _boom("redis down")
    with pytest.raises(errcode.PandoraError):
        await OG.collect_battle_game_server_refs(repo)


async def test_collect_refs_treats_released_record_as_normal():
    """Range 与 Get 之间记录被正常释放(返回 None)**不是**错误,继续观察即可。"""
    repo = _Repo({1: None, 2: _Battle(pod=GS_B)})
    refs = await OG.collect_battle_game_server_refs(repo)
    assert refs.pod_names == {GS_B}


# ── 启用门与节流 ────────────────────────────────────────────────────────────


async def test_janitor_disabled_without_reconciler_or_ledger():
    """缺 reconciler 或缺 ledger → 整体禁用(没有出身证明就没有删除权)。

    ★ 变异:把 `enabled` 改成 `return self._reconciler is not None` → 本条红。
    """
    rec = _Reconciler([_gs(GS_A, UID_A, ALLOC_A)])
    j1 = _janitor(rec, None, _Repo())
    j2 = _janitor(None, _Ledger(), _Repo())
    assert j1.enabled is False and j2.enabled is False
    await j1.reconcile_if_due(0.0, 0)
    await j2.reconcile_if_due(0.0, 0)
    assert rec.list_calls == 0


async def test_janitor_throttles_to_reconcile_interval():
    """节流:`ORPHAN_GS_RECONCILE_INTERVAL_SEC` 内至多一轮。

    守的是控制面成本(每轮一次 GS LIST + 全量记录读)。sweep 每 5s tick,不节流就是
    12 倍无谓压力。

    ★ 变异:把 `now_sec - self._last_reconcile_sec < ORPHAN_GS_RECONCILE_INTERVAL_SEC`
      的 `<` 改成 `>` → 本条红。
    """
    rec = _Reconciler([])
    j = _janitor(rec, _Ledger(), _Repo())
    await j.reconcile_if_due(0.0, 0)
    await j.reconcile_if_due(59.0, 59_000)
    assert rec.list_calls == 1
    await j.reconcile_if_due(60.0, 60_000)
    assert rec.list_calls == 2


# ── 四重防误删 ──────────────────────────────────────────────────────────────


async def test_reconcile_does_nothing_when_gs_list_unavailable():
    """GS 清单读不到 → 本轮什么都不做(防误删①)。

    ★ 变异:把 `except Exception as exc: ...; return` 的 `return` 删掉 → 本条红
      (会继续往下拉权威引用并修剪台账)。
    """
    rec = _Reconciler()
    rec.list_exc = _boom("apiserver down")
    ledger = _Ledger()
    repo = _Repo()
    j = _janitor(rec, ledger, repo)
    await j.reconcile(0.0, 0)
    assert repo.get_calls == [] and ledger.prune_calls == [] and rec.delete_calls == []


async def test_reconcile_resets_observation_window_when_authority_unreadable():
    """权威引用不可得 → 全部候选的观察起点**重置为当前时刻**(防误删①后半条)。

    守的是"连续每轮都有证据核验"严格成立。不重置的后果:Redis 抖动 10 分钟(期间一轮
    证据都没拿到),抖动恢复后所有候选**立刻**满足"已观察 10 分钟"而被删 —— 那 10 分钟
    里没有任何证据支持它们无人。

    ★ 变异:把 `self._reclaimer.reset_all_observations(now_sec)` 这行删掉 → 本条红。
    """
    rec = _Reconciler([_gs(GS_A, UID_A, ALLOC_A)])
    ledger = _Ledger({ALLOC_A})
    repo = _Repo()
    j = _janitor(rec, ledger, repo)

    await j.reconcile(0.0, 0)  # t=0 首见登记
    repo.range_exc = _boom("redis down")
    await j.reconcile(700.0, 700_000)  # 证据中断 → 观察起点重置到 700
    assert rec.delete_calls == []

    repo.range_exc = None
    await j.reconcile(800.0, 800_000)  # 距重置仅 100s < 600s
    assert rec.delete_calls == []

    await j.reconcile(1301.0, 1_301_000)  # 距重置 601s
    assert rec.delete_calls == [(GS_A, UID_A, ALLOC_A)]


async def test_reconcile_first_sighting_only_registers():
    """首见只登记不删(防误删②:候选期跨轮观察)。

    ★ 变异:把 `orphan_reclaim.OrphanReclaimer.plan_round` 里
      `if first is None: self._first_seen[key] = now_sec; continue` 的 `continue` 删掉
      → 本条红。
    """
    rec = _Reconciler([_gs(GS_A, UID_A, ALLOC_A)])
    j = _janitor(rec, _Ledger({ALLOC_A}), _Repo())
    await j.reconcile(0.0, 0)
    assert rec.delete_calls == []


async def test_reconcile_deletes_with_exact_triple_after_window():
    """满足全部条件后,删除必须 exact 到 `(name, uid, allocation_id)`。

    守的是防误删③:UID + resourceVersion 双 precondition 的服务端复核靠这三格锚定。
    按名字删会误杀同名重建出来的**新**实例(它可能已经载人)。

    ★ 变异:把 `delete_allocated_game_server_exact(gs.name, gs.uid, gs.allocation_id)`
      的第二个实参改成 `""` → 本条红。
    """
    rec = _Reconciler([_gs(GS_A, UID_A, ALLOC_A)])
    j = _janitor(rec, _Ledger({ALLOC_A}), _Repo())
    await j.reconcile(0.0, 0)
    await j.reconcile(601.0, 601_000)
    assert rec.delete_calls == [(GS_A, UID_A, ALLOC_A)]


async def test_reconcile_never_touches_referenced_gameserver():
    """有权威引用的 GS 永不进候选,观察多久都不删。

    ★ 变异:把 `orphan_reclaim.GameServerRefs.references` 里
      `if gs.uid and gs.uid in self.uids: return True` 删掉 → 本条红。
    """
    rec = _Reconciler([_gs(GS_A, UID_A, ALLOC_A)])
    repo = _Repo({1: _Battle(uid=UID_A)})  # 只有 uid 一维引用
    j = _janitor(rec, _Ledger({ALLOC_A}), repo)
    await j.reconcile(0.0, 0)
    await j.reconcile(601.0, 601_000)
    await j.reconcile(1_202.0, 1_202_000)
    assert rec.delete_calls == []


async def test_reconcile_skips_gameserver_already_deleting():
    """已带 deletionTimestamp 的 GS 不再处理(删除已受理,处于终止宽限)。"""
    rec = _Reconciler([_gs(GS_A, UID_A, ALLOC_A, deleting=True)])
    j = _janitor(rec, _Ledger({ALLOC_A}), _Repo())
    await j.reconcile(0.0, 0)
    await j.reconcile(601.0, 601_000)
    assert rec.delete_calls == []


async def test_reconcile_keeps_gameserver_without_allocation_label():
    """无 `allocation-id` label → UNPROVABLE 保留不删(防误删④ bootstrap 边界)。

    守的是:手工 GSA / 台账上线前的存量拿不出出身证明,拿不到证据就不删。

    ★ 变异:把 `plan_round` 里 `if not gs.allocation_id:` 那段 UNPROVABLE 分支删掉
      → 本条红。
    """
    rec = _Reconciler([_gs(GS_A, UID_A, alloc="")])
    ledger = _Ledger()
    j = _janitor(rec, ledger, _Repo())
    before = _reclaim_counter(orphan_reclaim.RESULT_UNPROVABLE)
    await j.reconcile(0.0, 0)
    await j.reconcile(601.0, 601_000)
    assert rec.delete_calls == []
    assert ledger.contains_calls == []  # 无 label 的候选不查台账(没有 key 可查)
    assert _reclaim_counter(orphan_reclaim.RESULT_UNPROVABLE) == before + 1


async def test_reconcile_keeps_gameserver_missing_from_ledger():
    """台账**查无** → UNPROVABLE 保留不删(疑似权威视图分裂)。

    守的是 P0「权威视图与被清扫集群零绑定」的整改:一个读到空 / 错配 Redis 的进程台账
    必然为空 ⇒ 一台都删不掉。这条闸是机制性的,不靠配置纪律。

    ★ 变异:把 `plan_round` 里 `if gs.allocation_id not in ledger_allocation_ids:` 改成
      `if False:`(或直接删掉这段)→ 本条红。
    """
    rec = _Reconciler([_gs(GS_A, UID_A, ALLOC_A)])
    ledger = _Ledger(set())  # 台账里没有这一条
    j = _janitor(rec, ledger, _Repo())
    before = _reclaim_counter(orphan_reclaim.RESULT_UNPROVABLE)
    await j.reconcile(0.0, 0)
    await j.reconcile(601.0, 601_000)
    assert rec.delete_calls == []
    assert ledger.contains_calls == [ALLOC_A, ALLOC_A]
    assert _reclaim_counter(orphan_reclaim.RESULT_UNPROVABLE) == before + 1


async def test_reconcile_ledger_check_failure_is_not_view_split_signal():
    """台账**查不了** ≠ 台账**查无**:保留候选、下轮重试,且不计进 unprovable 告警。

    守的是信号纯度:unprovable 是"疑似权威视图分裂"的告警,把一次 Redis 抖动混进去会
    把真信号淹掉 —— 而这个告警正是防误删④唯一的人工入口。

    ★ 变异:把 `if gs.allocation_id in check_failed: continue` 这两行删掉 → 本条红
      (unprovable 计数会 +1)。
    """
    rec = _Reconciler([_gs(GS_A, UID_A, ALLOC_A)])
    ledger = _Ledger({ALLOC_A})
    j = _janitor(rec, ledger, _Repo())
    await j.reconcile(0.0, 0)
    ledger.contains_exc = _boom("redis timeout")
    before = _reclaim_counter(orphan_reclaim.RESULT_UNPROVABLE)
    await j.reconcile(601.0, 601_000)
    assert rec.delete_calls == []
    assert _reclaim_counter(orphan_reclaim.RESULT_UNPROVABLE) == before

    # 候选被保留:台账恢复后同一轮观察窗仍然有效,立刻可回收。
    ledger.contains_exc = None
    await j.reconcile(602.0, 602_000)
    assert rec.delete_calls == [(GS_A, UID_A, ALLOC_A)]


# ── 删除结果回填 ────────────────────────────────────────────────────────────


async def test_reclaim_skipped_invalidates_candidate_for_a_full_window():
    """exact 复核失效(返回 False)→ 作废候选,**重新观察满一个完整窗口**。

    守的是本文件最危险的一格:复核失效说明这台 GS 的 resourceVersion 刚变过,很可能
    **已经被重新分配出去了**。不作废的话,首见时间还停在很久以前,下一轮阈值早已满足,
    会立刻再发一次删除 —— 而这次它可能已经载人了(§9「绝不删 Allocated GameServer」,
    已有两次事故)。

    ★ 变异:把 `_reclaim_one` 里 `not deleted` 分支的
      `self._reclaimer.on_reclaim_outcome(gs, orphan_reclaim.RESULT_SKIPPED)` 改成
      `... RESULT_FAILED` → 本条红(会立刻重删)。
    """
    rec = _Reconciler([_gs(GS_A, UID_A, ALLOC_A)])
    rec.delete_result = False
    j = _janitor(rec, _Ledger({ALLOC_A}), _Repo())
    await j.reconcile(0.0, 0)
    await j.reconcile(601.0, 601_000)
    assert len(rec.delete_calls) == 1

    await j.reconcile(602.0, 602_000)  # 重新登记首见
    await j.reconcile(700.0, 700_000)  # 距重新登记仅 98s
    assert len(rec.delete_calls) == 1


async def test_reclaim_failed_keeps_candidate_for_idempotent_retry():
    """删除调用本身失败 → **保留**候选(首见时间不变),下轮幂等重试。

    守的是与上一条**相反**的方向:调用没打出去,对象状态未知但也未被证伪,继续观察是
    安全的。写反的后果:一次网络抖动就把观察窗口整个重置,孤儿永远回收不掉,Fleet 容量
    被永久锁死。

    ★ 变异:把 `except Exception` 分支的 `RESULT_FAILED` 改成 `RESULT_SKIPPED`
      → 本条红。
    """
    rec = _Reconciler([_gs(GS_A, UID_A, ALLOC_A)])
    rec.delete_result = _boom("delete rpc failed")
    j = _janitor(rec, _Ledger({ALLOC_A}), _Repo())
    await j.reconcile(0.0, 0)
    await j.reconcile(601.0, 601_000)
    assert len(rec.delete_calls) == 1
    await j.reconcile(602.0, 602_000)
    assert len(rec.delete_calls) == 2


async def test_candidate_key_includes_uid_so_name_reuse_restarts_window():
    """首见表的键是 `name/uid`,**不能只用 name**。

    守的是名字复用:GameServer 名字会被复用(同名 GS 删掉后 Fleet 重建可能拿到同一个
    名字),但 UID 必定是新的。只用 name 的后果 —— 重建出来的**活着的** GS 直接继承前世
    的观察起点,第一轮对账就满足"已连续观察 10 分钟"而被删。

    ★ 变异:把 `orphan_reclaim._key` 改成 `return gs.name` → 本条红。
    """
    rec = _Reconciler([_gs(GS_A, UID_A, ALLOC_A)])
    ledger = _Ledger({ALLOC_A, ALLOC_B})
    j = _janitor(rec, ledger, _Repo())
    await j.reconcile(0.0, 0)  # 老实例 uid=A 首见
    rec.gs_list = [_gs(GS_A, UID_B, ALLOC_B)]  # 同名重建,UID 换代
    await j.reconcile(601.0, 601_000)
    assert rec.delete_calls == []


async def test_reclaim_caps_attempts_per_round():
    """单轮删除尝试封顶(爆炸半径,不是性能参数)。

    守的是:万一还存在未知的误删路径,封顶把"一轮删光整个 Fleet"压成"每轮最多 N 台",
    留出人工发现并止损的窗口。

    ★ 变异:把 `plan_round` 里 `if attempts >= self._max_per_round or (...)` 的
      `attempts >= self._max_per_round` 去掉 → 本条红。
    """
    gs_list = [_gs(f"pandora-battle-{i}", f"uid-{i}", f"alloc-{i}") for i in range(5)]
    rec = _Reconciler(gs_list)
    ledger = _Ledger({f"alloc-{i}" for i in range(5)})
    j = _janitor(rec, ledger, _Repo(), max_per_round=2)
    await j.reconcile(0.0, 0)
    await j.reconcile(601.0, 601_000)
    assert len(rec.delete_calls) == 2


async def test_ledger_prune_uses_retention_cutoff():
    """台账修剪 cutoff = `now_ms - 保留期`(§9.24 有界性)。

    ★ 变异:把 `cutoff_ms = now_ms - ORPHAN_GS_LEDGER_RETENTION_SEC * 1000` 改成
      `cutoff_ms = now_ms` → 本条红(会把 7 天内的台账全清,防误删④失效)。
    """
    rec = _Reconciler([_gs(GS_A, UID_A, ALLOC_A)])
    ledger = _Ledger({ALLOC_A})
    j = _janitor(rec, ledger, _Repo())
    now_ms = 1_800_000_000_000
    await j.reconcile(0.0, now_ms)
    assert ledger.prune_calls == [now_ms - OG.ORPHAN_GS_LEDGER_RETENTION_SEC * 1000]


async def test_ledger_prune_failure_does_not_block_the_round():
    """修剪失败只告警:它是容量卫生,不是本轮判定的前提。"""
    rec = _Reconciler([_gs(GS_A, UID_A, ALLOC_A)])

    class _BadLedger(_Ledger):
        async def prune_allocation_ledger(self, before_ms: int) -> int:
            raise _boom("prune failed")

    ledger = _BadLedger({ALLOC_A})
    j = _janitor(rec, ledger, _Repo())
    await j.reconcile(0.0, 0)
    await j.reconcile(601.0, 601_000)
    assert rec.delete_calls == [(GS_A, UID_A, ALLOC_A)]


async def test_reclaim_after_is_clamped_to_floor():
    """观察阈值有**下限**(5min),误配的短阈值必须被代码拦住。

    守的是判定链①唯一的时长依据:它要覆盖票据硬上限 180s + ready_wait 120s + 跨多轮
    对账与控制面观察余量。谁在 yaml 里填了 30s(手滑 / 照抄别的 sweep 项),不钳制的话
    进程会安静地按 30s 回收 —— 一台刚分配、玩家正拿着有效票据在进的 DS 就可能在进场
    途中被删。误配不能靠纪律拦。

    ★ 变异:把 `orphan_reclaim._clamp_reclaim_after` 改成 `return sec` → 本条红。
    """
    rec = _Reconciler([_gs(GS_A, UID_A, ALLOC_A)])
    j = _janitor(rec, _Ledger({ALLOC_A}), _Repo(), reclaim_after_sec=30)
    assert j.reclaim_after_sec == orphan_reclaim.RECLAIM_AFTER_FLOOR_SEC
    await j.reconcile(0.0, 0)
    await j.reconcile(31.0, 31_000)
    assert rec.delete_calls == []
