"""owner 权威测试 —— §9.22 的不变量。

这是全仓正确性最敏感的服务:它决定「玩家同一时刻只能在一个可操作 DS」。
测试重点全部围绕**写错了不报错、只在脑裂时才显形**的那些点:

  1. ★ admit_not_before 屏障的分流与余量(核心时序不等式的兑现点)
  2. ★ operation_id 必须 canonical —— 只校验"能解析"会让幂等键失效
  3. ★ exact 实例身份:同名 Pod 重建后必须**不相等**
  4. ★ lease 秒数硬钳制 —— 配置无法放大脑裂窗口
  5. ★ hub_source_revision 只前进永不清零
  6. Admit 拒绝原因必须拆成单一枚举(一个 if 收敛 N 个条件 = 线上查不出真因)
"""

from __future__ import annotations

import uuid

import pytest

from pandorapy import errcode, placement
from pandorapy.services.owner import biz as obiz
from pandorapy.services.owner import data as odata


def _target(**overrides) -> odata.OwnerTarget:
    base = {
        "pod_name": "battle-abc",
        "instance_uid": "uid-1",
        "instance_epoch": 3,
        "assignment_or_allocation_id": "alloc-9",
        "release_track": "stable",
    }
    base.update(overrides)
    return odata.OwnerTarget(**base)


class FakeRepo:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.record = odata.OwnerRecord()
        self.renew_lease_seconds: list[int] = []

    async def query(self, player_id: int) -> odata.OwnerRecord:
        self.calls.append(("query", player_id))
        return self.record

    async def begin_transition(
        self, player_id, expect_epoch, operation_id, owner_type, target,
        source_revision, skew_margin_seconds,
    ):
        self.calls.append(
            ("begin", player_id, operation_id, owner_type, source_revision, skew_margin_seconds)
        )
        return self.record

    async def admit(self, player_id, owner_epoch, operation_id, target):
        self.calls.append(("admit", player_id, owner_epoch, operation_id))
        return self.record, 0

    async def renew_instance_lease(self, target, lease_seconds):
        self.renew_lease_seconds.append(lease_seconds)
        return 12345

    async def release(self, player_id, owner_epoch, operation_id):
        self.calls.append(("release", player_id, owner_epoch, operation_id))
        return self.record

    async def sweep_transition_log(self, retention_days, batch):
        return 0


class Cfg:
    log_retention_days = 90


def _uc(repo=None) -> obiz.OwnerUsecase:
    return obiz.OwnerUsecase(repo or FakeRepo(), Cfg())


# ── ★ 1. admit_not_before 屏障 ───────────────────────────────────────────────


def test_battle_predecessor_waits_for_lease_plus_margin() -> None:
    """★ 旧 owner=BATTLE → 屏障 = max(now, 旧租约截止) + 余量。

    对局 DS 可能已失联但仍在跑(玩家还能操作、还能产生业务写)。必须等它的租约
    确定过期再加时钟/网络余量,新 DS 才能开始可玩 ——
    这就是「旧 DS 最晚停止可玩时间 < 新 DS 最早开始可玩时间」。
    """
    now = 1_000_000
    lease_deadline = now + 8_000  # 租约还有 8 秒
    barrier = odata.compute_admit_not_before_ms(
        odata.OWNER_TYPE_BATTLE, lease_deadline, now
    )
    expected = lease_deadline + placement.DS_FENCE_SKEW_MARGIN_SECONDS * 1000
    assert barrier == expected


def test_battle_predecessor_with_expired_lease_still_gets_margin() -> None:
    """★ 旧租约**已过期**时仍必须加余量。

    如果只把余量加在 lease 上(`lease + margin`),lease 已过期时 max 取的是 now,
    屏障就等于 now —— 零等待。时钟漂移/网络在途那几秒的保护整个消失。
    """
    now = 1_000_000
    lease_deadline = now - 50_000  # 早就过期
    barrier = odata.compute_admit_not_before_ms(
        odata.OWNER_TYPE_BATTLE, lease_deadline, now
    )
    assert barrier == now + placement.DS_FENCE_SKEW_MARGIN_SECONDS * 1000
    assert barrier > now, "旧租约过期时屏障退化成零等待"


@pytest.mark.parametrize(
    "old_type", [odata.OWNER_TYPE_HUB, odata.OWNER_TYPE_NONE]
)
def test_hub_or_none_predecessor_opens_barrier_immediately(old_type: int) -> None:
    """★ 旧 owner=HUB 或无 → 屏障 = now,不等待。

    Hub 是协作迁移:双写由 epoch fencing 拦(旧 epoch 的写一律拒),
    双可玩由客户端单连接拆链拦。这里等待没有收益,只会让每次进大厅卡 27 秒。
    """
    now = 1_000_000
    assert odata.compute_admit_not_before_ms(old_type, now + 99_000, now) == now


def test_fence_constants_match_go_protocol() -> None:
    """★ fence 常量是**正确性常量不是调优参数**,必须与 Go / UE 三方同值。

    调小会重新打开「一名玩家同时存在于两台 DS」的窗口。
    """
    assert placement.DS_FENCE_LEASE_MAX_SECONDS == 20
    assert placement.DS_FENCE_SKEW_MARGIN_SECONDS == 7
    assert placement.DS_FENCE_REENTRY_BARRIER_SECONDS == 27


def test_locator_ttl_lower_bound_holds() -> None:
    """player_locator TTL / hub heartbeat_timeout 必须 ≥ 再入屏障(默认 30s ≥ 27s)。"""
    assert 30 >= placement.DS_FENCE_REENTRY_BARRIER_SECONDS


# ── ★ 2. operation_id canonical ──────────────────────────────────────────────


def test_valid_operation_id_accepts_canonical_uuid4() -> None:
    for _ in range(20):
        assert placement.valid_operation_id(str(uuid.uuid4()))


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "not-a-uuid",
        "00000000-0000-0000-0000-000000000000",  # nil
        str(uuid.uuid1()),  # v1 不是 v4
        "{123e4567-e89b-42d3-a456-426614174000}",  # 花括号
        "urn:uuid:123e4567-e89b-42d3-a456-426614174000",  # URN 前缀
        "123E4567-E89B-42D3-A456-426614174000",  # 大写
    ],
)
def test_valid_operation_id_rejects_non_canonical(bad: str) -> None:
    """★ 只接受 **canonical 原样**,不接受"能解析就行"。

    `uuid.UUID(s)` 会接受大写 / 花括号 / urn: 前缀并归一化。若只校验"能解析",
    同一次进场用不同写法重试会被当成**两个不同 operation**,幂等键失效 ——
    于是重复占座、重复分配 DS、产生第二个 owner(§9.23)。
    """
    assert not placement.valid_operation_id(bad)


async def test_begin_mints_operation_id_when_empty() -> None:
    """★ 空 operation → **权威铸造**。

    调用方(allocator 签票点 / READY 交付点)无法自己保证稳定:它们每次投递现铸
    一个 UUID,同一次进场的重连、重复交付、心跳自愈会写出不同 operation。
    """
    repo = FakeRepo()
    uc = _uc(repo)
    await uc.begin_transition(1001, 0, "", odata.OWNER_TYPE_HUB, _target())
    minted = repo.calls[0][2]
    assert placement.valid_operation_id(minted), f"权威铸出了非 canonical 的 {minted!r}"


async def test_begin_rejects_non_canonical_operation_id() -> None:
    uc = _uc()
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.begin_transition(1001, 0, "NOT-A-UUID", odata.OWNER_TYPE_HUB, _target())
    assert exc.value.code == errcode.ErrOwnerInvalidOperation


async def test_admit_requires_operation_id() -> None:
    """★ Admit 的 operation_id **不允许为空**(与 BeginTransition 不同)。

    准入是对"某一次已发起的迁移"的确认,没有 operation 就无从确认是哪一次。
    """
    uc = _uc()
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.admit(1001, 5, "", _target())
    assert exc.value.code == errcode.ErrOwnerInvalidOperation


# ── ★ 3. exact 实例身份 ─────────────────────────────────────────────────────


def test_same_name_pod_rebuild_is_not_equal() -> None:
    """★ 同名 Pod 重建后**必须不相等** —— Agones 下 Pod 名会被复用。

    若判等只看 pod_name,旧实例的迟到写会被当成当前实例的写放行。
    """
    a = _target(instance_uid="uid-1", instance_epoch=3)
    rebuilt = _target(instance_uid="uid-2", instance_epoch=4)  # 同名不同实例
    assert a != rebuilt
    assert a.pod_name == rebuilt.pod_name  # 名字确实一样


@pytest.mark.parametrize(
    "missing",
    ["pod_name", "instance_uid", "assignment_or_allocation_id", "release_track"],
)
def test_target_incomplete_when_any_field_blank(missing: str) -> None:
    assert not _target(**{missing: ""}).complete()
    assert not _target(**{missing: "   "}).complete(), "全空白应视为缺失"


def test_target_incomplete_when_instance_epoch_zero() -> None:
    assert not _target(instance_epoch=0).complete()


async def test_begin_rejects_incomplete_target() -> None:
    uc = _uc()
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.begin_transition(
            1001, 0, "", odata.OWNER_TYPE_HUB, _target(instance_uid="")
        )
    assert exc.value.code == errcode.ErrOwnerInvalidOperation
    assert "target identity incomplete" in exc.value.msg


# ── ★ 4. lease 秒数硬钳制 ───────────────────────────────────────────────────


async def test_lease_seconds_clamped_to_protocol_max() -> None:
    """★ 配置/调用方**无法放大**脑裂窗口。"""
    repo = FakeRepo()
    uc = _uc(repo)
    await uc.renew_instance_lease(_target(), 9999)
    assert repo.renew_lease_seconds == [placement.DS_FENCE_LEASE_MAX_SECONDS]


async def test_lease_seconds_under_max_passes_through() -> None:
    repo = FakeRepo()
    uc = _uc(repo)
    await uc.renew_instance_lease(_target(), 10)
    assert repo.renew_lease_seconds == [10]


async def test_renew_rejects_zero_lease() -> None:
    uc = _uc()
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.renew_instance_lease(_target(), 0)
    assert exc.value.code == errcode.ErrInvalidArg


async def test_renew_allows_zero_instance_epoch() -> None:
    """续租允许 instance_epoch=0 —— hub 凭据不携带实例纪元,uid 全局唯一已足够。

    纪元守卫在数据层只对"双方都非零且不同"拒。
    """
    repo = FakeRepo()
    uc = _uc(repo)
    assert await uc.renew_instance_lease(_target(instance_epoch=0), 10) == 12345


async def test_renew_requires_pod_and_uid() -> None:
    uc = _uc()
    for bad in (_target(pod_name=""), _target(instance_uid="")):
        with pytest.raises(errcode.PandoraError) as exc:
            await uc.renew_instance_lease(bad, 10)
        assert exc.value.code == errcode.ErrOwnerInvalidOperation


# ── ★ 5. Admit 拒绝原因必须单一枚举 ──────────────────────────────────────────


def test_admit_mismatch_reason_is_single_valued() -> None:
    """★ 一个 if 收敛了 N 个条件的,必须拆成 N 个 reason(§11.3 R2)。

    否则线上只看到「准入被拒」,不知道是 epoch 老了、operation 换了、
    还是打到了别的实例 —— 而这三者的处置完全不同。
    """
    target = _target()
    op = str(uuid.uuid4())
    rec = odata.OwnerRecord(
        player_id=1001,
        owner_epoch=5,
        phase=odata.OWNER_PHASE_PENDING,
        target=target,
        operation_id=op,
    )
    assert odata.admit_mismatch_reason(False, rec, 5, op, target) == "owner_record_absent"
    assert odata.admit_mismatch_reason(True, rec, 4, op, target) == "owner_epoch_mismatch"
    assert (
        odata.admit_mismatch_reason(True, rec, 5, str(uuid.uuid4()), target)
        == "operation_id_mismatch"
    )
    assert (
        odata.admit_mismatch_reason(True, rec, 5, op, _target(instance_uid="other"))
        == "target_instance_mismatch"
    )
    assert odata.admit_mismatch_reason(True, rec, 5, op, target) == ""


def test_admitted_phase_is_idempotent_not_mismatch() -> None:
    """已 ADMITTED 是**幂等重放**,不是不匹配 —— ACK 丢失后重放必须返回同一结果。"""
    target = _target()
    op = str(uuid.uuid4())
    rec = odata.OwnerRecord(
        owner_epoch=5, phase=odata.OWNER_PHASE_ADMITTED, target=target, operation_id=op
    )
    assert odata.admit_mismatch_reason(True, rec, 5, op, target) == ""


# ── ★ 6. 审计 detail ────────────────────────────────────────────────────────


def test_transition_detail_carries_exact_identity_not_just_pod() -> None:
    """★ 审计必须带 uid / epoch / 分配 ID / 屏障时刻,不能只写 pod_name。

    Agones 下 Pod 名会复用,同名 Pod 重建后两行 detail 完全一样;而 locator 的
    join key 是 assignment_id、allocator 是 allocation_id —— 只有 pod 名时,
    「这次 owner 迁移对应哪次 hub assignment / 哪局对局」永远接不上。
    """
    detail = odata.transition_detail(_target(), admit_not_before_ms=1730000000000)
    for token in ("uid=uid-1", "iepoch=3", "aid=alloc-9", "track=stable", "anb=1730000000000"):
        assert token in detail


def test_transition_detail_is_clamped_to_column_width() -> None:
    """★ 必须在应用侧钳制到列宽。

    sql_mode 含 STRICT_TRANS_TABLES,超长写入是 Error 1406 而**不是截断** ——
    那会让一次本该成功的 owner 迁移因为审计字段太长而整事务失败。
    审计流水缺一截可以接受,玩家进不去场景不可以。
    """
    huge = odata.transition_detail(
        _target(pod_name="p" * 900, assignment_or_allocation_id="a" * 900),
        admit_not_before_ms=1,
        from_pod="f" * 900,
    )
    assert len(huge) <= odata.TRANSITION_DETAIL_MAX_LEN


# ── 屏障等待 ────────────────────────────────────────────────────────────────


def test_barrier_wait_ms_never_negative() -> None:
    rec = odata.OwnerRecord(admit_not_before_ms=1000)
    assert odata.barrier_wait_ms(rec, 900) == 100
    assert odata.barrier_wait_ms(rec, 1000) == 0
    assert odata.barrier_wait_ms(rec, 9999) == 0


def test_barrier_error_carries_retry_after() -> None:
    """屏障未开返回带 retry_after 的错误 —— 调用方退避重查,**保留 session 与原 operation**。"""
    err = odata.barrier_not_open_error(2500)
    assert err.code == errcode.ErrOwnerBarrierNotOpen
    assert "2500" in err.msg


# ── 参数校验 ────────────────────────────────────────────────────────────────


async def test_query_rejects_zero_player() -> None:
    uc = _uc()
    with pytest.raises(errcode.PandoraError, match="player_id required"):
        await uc.query(0)


@pytest.mark.parametrize("owner_type", [0, 3, -1, 99])
async def test_begin_rejects_invalid_owner_type(owner_type: int) -> None:
    uc = _uc()
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.begin_transition(1001, 0, "", owner_type, _target())
    assert exc.value.code == errcode.ErrOwnerInvalidOperation


async def test_admit_requires_player_and_epoch() -> None:
    uc = _uc()
    op = str(uuid.uuid4())
    for pid, epoch in ((0, 5), (1001, 0)):
        with pytest.raises(errcode.PandoraError) as exc:
            await uc.admit(pid, epoch, op, _target())
        assert exc.value.code == errcode.ErrInvalidArg


async def test_release_requires_canonical_operation() -> None:
    uc = _uc()
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.release(1001, 5, "garbage")
    assert exc.value.code == errcode.ErrOwnerInvalidOperation


async def test_begin_passes_skew_margin_from_placement() -> None:
    """★ 余量必须来自 placement 单一来源,不能各服务自己抄一份。"""
    repo = FakeRepo()
    uc = _uc(repo)
    await uc.begin_transition(1001, 0, "", odata.OWNER_TYPE_HUB, _target())
    assert repo.calls[0][5] == placement.DS_FENCE_SKEW_MARGIN_SECONDS
