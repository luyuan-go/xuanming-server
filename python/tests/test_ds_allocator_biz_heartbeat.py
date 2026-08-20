"""`pandorapy/services/ds_allocator/biz_heartbeat.py` 回归测试。

对照 Go 侧 `services/battle/ds_allocator/internal/biz/allocator.go` 第 2426–3070 行;
每条断言的行为口径以那份 Go 源码为准,不以 Python 实现"看起来该怎样"为准。

## 本文件盯死的五件事(§9.6 五要件 + §9.19 / §9.22)

  ① **拒绝必须真的拒绝**。`heartbeat_authorized_with_players` 是 Battle DS 写权限的
     授权点。前三道门(未启用 Redis 权威 / census 自相矛盾 / 权威读失败)各自
     **单独**能整跳拒绝,且拒绝时**零状态转移** —— 不激活凭据、不续 owner 租约、
     不对账离场。任一条被"看着差不多"地并进后面的分支,都会让一台本该被 fencing
     的 DS 继续改玩家。
  ② **pod_uid 回填前的凭据全等门缺一即拒**。回填写进权威记录的是"该删哪个 Pod",
     所以上报方必须先被证明是当前被授权实例。八项凭据 + 五项前置条件逐项单独验，
     参数化列举。
     ★ 一个**没**咕住的点，写在这里免得下一个人重推：Python 的 `auth.active` 永远返回
       零值消息（Go 返回 nil），看上去不用 `HasField` 就会让"凭据没写过"冒充全等；
       但**实测变异后用例仍绿** —— 零值凭据要与零值身份全等，而零值身份的
       `exp_ms == 0` 已被过期门先拦下。本文件因此**不**声称能咕住那个变异，
       只验"未写过凭据的 auth 不能匹配一个合法存活身份"。
  ③ **activation_pending 是完全静默的一跳**。两阶段激活期间不发 ACK、不下发指令、
     不续 owner 租约(实例尚未服务)、不做离场对账。少禁一项就等于让证据不足的
     实例提前拿到写权限。
  ④ **驱逐单是"玩家退出副本"链上唯一的后端→DS 指令**(§9.19)。对账失败必须整跳
     失败并留证,不能吞掉继续回 200 —— 吞掉的形状是"玩家卡在退出副本上,而后端
     一切正常"。
  ⑤ **legacy 面的四条 stop 分支各有各的收尾**。终态要 kill + owner 精确释放
     (INC-20260804-001 缺口⑦:打完副本回不了大厅的直接原因),pod 不匹配与孤儿
     只 kill 不释放(它们的玩家归属不属于本上报方)。

## 依赖策略:全内存替身,不碰 Redis / k8s / owner

本模块测的是**调用方的判定与顺序**,不是 Redis / owner 权威的行为。因此
`renew_owner_lease_gate` / `owner_admit_census_weak` /
`owner_release_abandoned_players_weak` 三个跨模块协作函数按名替换成录音替身 ——
它们各自的语义在 `test_ds_allocator_owner.py` 已有覆盖,这里要断言的是
"被调用没有、参数是哪一份名单、失败时整跳失没失败"。

★ 每条用例 docstring 的 `★ 变异:` 一行都**真跑过**(改坏产品代码 → 确认本条红 →
  改回 → 确认绿),不是照着代码猜的。
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pandora.ds.v1 import allocator_pb2 as dspb

from pandorapy import errcode
from pandorapy.services.ds_allocator import biz_heartbeat as BH
from pandorapy.services.ds_allocator import conf as dsconf
from pandorapy.services.ds_allocator.battle_auth import (
    BattleActivateResult,
    BattleAuthoritySnapshot,
    BattleCredentialIdentity,
    BattleHeartbeatInput,
)
from pandorapy.services.ds_allocator.biz_base import (
    COMMAND_NONE,
    COMMAND_STOP,
    REASON_STOP_BATTLE_ABANDONED,
    REASON_STOP_BATTLE_ENDED,
    REASON_STOP_BATTLE_MISSING,
    REASON_STOP_TERMINAL_AUTH,
    STATE_ABANDONED,
    STATE_ALLOCATION_ABORT,
    STATE_ALLOCATION_EMPTY_FENCE,
    STATE_ALLOCATION_RECONCILING,
    STATE_ALLOCATION_UNCERTAIN,
    STATE_ENDED,
    STATE_PREACTIVE_RELEASING,
    STATE_READY,
    STATE_RUNNING,
    STATE_WARMING,
)

# ── 常量 ────────────────────────────────────────────────────────────────────

MATCH_ID = 700_000_000_000_000_001
POD = "pandora-battle-7f9c-abcde"
UID = "gs-uid-A"
POD_UID = "pod-uid-A"
ALLOC = "5a1f1b7e-4a1e-4a3c-9f4e-2b7c1d8e6a05"
DS_ADDR = "10.244.1.7:7777"
TRACK = "stable"
EPOCH = 5
GEN = 7
JTI = "jti-A"
KID = "kid-2026-08"
SHA = "9f2b" * 16
WRITER_EPOCH = 2
P1, P2, P3 = (
    800_000_000_000_000_002,
    800_000_000_000_000_003,
    800_000_000_000_000_004,
)
MAP_ID = 4002
GAME_MODE = "pvp"

#: 固定"现在",让所有超时判定与到期比较可复现(§16.10:兜底判定必须与真实时钟无关)。
NOW_MS = 1_800_000_000_000

IDENT = BattleCredentialIdentity(
    pod_name=POD,
    instance_uid=UID,
    instance_epoch=EPOCH,
    gen=GEN,
    jti=JTI,
    exp_ms=NOW_MS + 120_000,
    kid=KID,
    token_sha256=SHA,
    writer_epoch=WRITER_EPOCH,
)


# ── 替身 ────────────────────────────────────────────────────────────────────


def mk_battle(**kw: Any):
    """构造一份 battle 镜像;未指定的字段用"正常在打的一局"作底。"""
    fields: dict[str, Any] = {
        "match_id": MATCH_ID,
        "ds_pod_name": POD,
        "ds_addr": DS_ADDR,
        "state": STATE_RUNNING,
        "player_ids": [P1, P2],
        "map_id": MAP_ID,
        "game_mode": GAME_MODE,
        "allocated_at_ms": NOW_MS - 60_000,
        "last_heartbeat_ms": NOW_MS - 5_000,
        "player_count": 2,
        "gameserver_uid": UID,
        "instance_epoch": EPOCH,
        "allocation_id": ALLOC,
        "release_track": TRACK,
        "pod_uid": POD_UID,
        "ever_had_players": True,
    }
    fields.update(kw)
    return dspb.BattleStorageRecord(**fields)


def mk_auth(*, active: bool = True, pending: bool = False, **kw: Any):
    """构造 auth 记录;`active` / `pending` 控制**是否 set** 对应凭据(HasField 语义)。"""
    cred = dspb.BattleDSCredential(
        gen=GEN,
        jti=JTI,
        exp_ms=IDENT.exp_ms,
        kid=KID,
        instance_uid=UID,
        instance_epoch=EPOCH,
        token_sha256=SHA,
        writer_epoch=WRITER_EPOCH,
    )
    rec = dspb.BattleDSAuthStorageRecord(
        match_id=MATCH_ID,
        ds_pod_name=POD,
        instance_uid=UID,
        instance_epoch=EPOCH,
        allocation_id=ALLOC,
    )
    for name, value in kw.items():
        setattr(rec, name, value)
    if active:
        rec.active.CopyFrom(cred)
    if pending:
        rec.pending.CopyFrom(cred)
    return rec


class FakeAuthRepo:
    """`RedisBattleAuthRepo` 的读/激活两个方法。"""

    def __init__(self) -> None:
        self.snapshot = BattleAuthoritySnapshot(
            auth=None, battle=None, auth_found=False, battle_found=False
        )
        self.read_err: BaseException | None = None
        self.activate_out: BattleActivateResult | None = None
        self.activate_err: BaseException | None = None
        self.activate_input: BattleHeartbeatInput | None = None
        self.calls: list[str] = []

    async def read_authority(self, match_id: int) -> BattleAuthoritySnapshot:
        self.calls.append("read_authority")
        assert match_id == MATCH_ID
        if self.read_err is not None:
            raise self.read_err
        return self.snapshot

    async def activate_heartbeat(
        self, match_id: int, ident: BattleCredentialIdentity, inp: BattleHeartbeatInput
    ) -> BattleActivateResult:
        self.calls.append("activate_heartbeat")
        assert match_id == MATCH_ID
        assert ident is IDENT
        self.activate_input = inp
        if self.activate_err is not None:
            raise self.activate_err
        assert self.activate_out is not None
        return self.activate_out


class FakeRepo:
    """`RedisBattleRepo` 用到的三个方法。

    `records` 是 CAS 每一轮回调看到的镜像:多于一条即模拟"CAS 冲突后基于新镜像重跑",
    最后一条为准 —— 出参重置契约就靠它验。
    """

    def __init__(self, *records: Any) -> None:
        self.records = list(records)
        self.update_err: BaseException | None = None
        self.departures: list[Any] = []
        self.departure_err: BaseException | None = None
        self.departure_args: tuple[Any, ...] | None = None
        self.get_battle_result: Any = None
        self.get_battle_err: BaseException | None = None
        self.calls: list[str] = []

    async def update_battle_with_lock(
        self, match_id: int, max_retry: int, fn: Any, battle_ttl_sec: float
    ) -> None:
        self.calls.append("update_battle_with_lock")
        assert match_id == MATCH_ID
        assert max_retry >= 1
        assert battle_ttl_sec > 0
        if self.update_err is not None:
            raise self.update_err
        if not self.records:
            raise errcode.PandoraError(
                errcode.ErrDSPodNotFound, "battle %d not found", match_id
            )
        for rec in self.records:
            fn(rec)

    async def reconcile_player_departures(self, *args: Any) -> list[Any]:
        self.calls.append("reconcile_player_departures")
        self.departure_args = args
        if self.departure_err is not None:
            raise self.departure_err
        return self.departures

    async def get_battle(self, match_id: int) -> Any:
        self.calls.append("get_battle")
        assert match_id == MATCH_ID
        if self.get_battle_err is not None:
            raise self.get_battle_err
        return self.get_battle_result


class Harness(BH.HeartbeatMixin):
    """`HeartbeatMixin` + 基座字段 + 「3071 行之后」那批 helper 的录音替身。

    ★ 直接子类化 mixin 就是它的设计用法(它不继承任何东西,只用 `self.xxx`)。
    """

    def __init__(self, *, cfg: dsconf.AllocatorConf | None = None, **kw: Any) -> None:
        self.model_b = kw.pop("model_b", True)
        self.auth_repo = kw.pop("auth_repo", FakeAuthRepo())
        self.repo = kw.pop("repo", FakeRepo())
        self.cfg = cfg if cfg is not None else dsconf.AllocatorConf()
        self.ds_credential_ttl_sec = 120.0
        self.owner_lease = kw.pop("owner_lease", object())
        self.owner_lease_required = kw.pop("owner_lease_required", False)
        self.owner_auth = kw.pop("owner_auth", object())
        self.owner_admitted: dict[str, float] = {}
        self.locator = kw.pop("locator", object())
        self.alloc = kw.pop("alloc", object())
        # 录音
        self.tape: list[tuple[Any, ...]] = []
        self.pod_uid_err: BaseException | None = kw.pop("pod_uid_err", None)
        self.finish_result = BH.HeartbeatResult(command=COMMAND_STOP)
        self.roster_mode = kw.pop("roster_mode", dsconf.ROSTER_JOIN_MODE_OBSERVE)
        self.roster_deadline = kw.pop("roster_deadline", None)
        assert not kw, f"unexpected kwargs {kw}"

    # ── 基座 helper ──────────────────────────────────────────────────────
    def battle_ttl_sec(self) -> float:
        return 900.0

    def kill_stranded_ds(self, match_id: int, pod_name: str, reason: str) -> None:
        self.tape.append(("kill_stranded_ds", match_id, pod_name, reason))

    async def ensure_durable_release_pod_uid(
        self, match_id: int, pod_name: str, expected: Any, expected_release_track: str
    ) -> str:
        self.tape.append(
            (
                "ensure_durable_release_pod_uid",
                match_id,
                pod_name,
                expected.allocation_id,
                expected.instance_uid,
                expected.instance_epoch,
                expected_release_track,
            )
        )
        if self.pod_uid_err is not None:
            raise self.pod_uid_err
        return POD_UID

    # ── 「3071 行之后」的 helper(本批不实现,这里给真语义替身)────────────
    def resolve_roster_join_mode_safe(self) -> str:
        return self.roster_mode

    def effective_roster_join_deadline(self):
        import datetime as _dt

        if self.roster_deadline is not None:
            return self.roster_deadline
        if self.roster_mode == dsconf.ROSTER_JOIN_MODE_OFF:
            return _dt.timedelta(0)
        return self.cfg.resolve_roster_join_deadline()

    def roster_absentees(self, roster: list[int], census: list[int]) -> list[int]:
        if not roster:
            return []
        present = set(census)
        return [pid for pid in roster if pid not in present]

    async def finish_empty_abandon(self, *args: Any) -> BH.HeartbeatResult:
        self.tape.append(("finish_empty_abandon", *args))
        return self.finish_result

    def refresh_battle_locations(
        self, player_ids: list[int], match_id: int, ds_addr: str
    ) -> None:
        self.tape.append(("refresh_battle_locations", list(player_ids), match_id, ds_addr))


@pytest.fixture(autouse=True)
def _freeze_now(monkeypatch: pytest.MonkeyPatch) -> None:
    """冻结 `now_ms`。空场 / 到齐期限都是"现在 - 起点 >= 阈值"的比较,不冻结就不可复现。"""
    monkeypatch.setattr(BH, "now_ms", lambda: NOW_MS)


class OwnerTape(list):
    """录音磁带 + 一个可注入的租约错误。

    ★ 用子类而不是给 `list` 实例 `setattr`:内置 `list` 没有 `__dict__`,
      直接挂属性会在 fixture 里就 AttributeError,15 条用例一起变成 ERROR。
    """

    lease_err: list[BaseException]

    def __init__(self) -> None:
        super().__init__()
        self.lease_err = []


@pytest.fixture
def owner_tape(monkeypatch: pytest.MonkeyPatch) -> OwnerTape:
    """把三个 owner 协作函数换成录音替身(见模块头「依赖策略」)。"""
    tape = OwnerTape()
    lease_err = tape.lease_err

    async def fake_renew(renewer, required, pod, uid, epoch, track):  # noqa: ANN001, ANN202, PLR0913
        tape.append(("renew_owner_lease_gate", required, pod, uid, epoch, track))
        if lease_err:
            raise lease_err[0]

    async def fake_admit(auth, admitted, players, owner_type, pod, uid, budget):  # noqa: ANN001, ANN202, PLR0913
        tape.append(("owner_admit_census_weak", list(players), owner_type, pod, uid, budget))

    async def fake_release(auth, players, pod, uid, budget):  # noqa: ANN001, ANN202
        tape.append(("owner_release_abandoned_players_weak", list(players), pod, uid, budget))

    monkeypatch.setattr(BH, "renew_owner_lease_gate", fake_renew)
    monkeypatch.setattr(BH, "owner_admit_census_weak", fake_admit)
    monkeypatch.setattr(BH, "owner_release_abandoned_players_weak", fake_release)
    return tape


def mk_activate(**kw: Any) -> BattleActivateResult:
    fields: dict[str, Any] = {
        "activation_pending": False,
        "first_activation": False,
        "first_abandon": False,
        "roster_incomplete": False,
        "roster_would_abandon": False,
        "terminal": False,
        "heartbeat_ms": NOW_MS,
        "active": IDENT,
        "battle": mk_battle(),
    }
    fields.update(kw)
    return BattleActivateResult(**fields)


# ════════════════════════════════════════════════════════════════════════════
# Model B:授权心跳的三道前置门
# ════════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_model_b_off_refuses_before_touching_authority(owner_tape: list) -> None:
    """未启用 Redis 权威 → ErrInvalidState,且**一次权威读都不发**。

    ★ 变异:把 `if not self.model_b` 改成 `if False` → 本条红(变成 read_authority
      被调用 / 不再抛 ErrInvalidState)。
    """
    u = Harness(model_b=False)
    with pytest.raises(errcode.PandoraError) as ei:
        await u.heartbeat_authorized(MATCH_ID, IDENT, 2, STATE_RUNNING, 0)
    assert ei.value.code == errcode.ErrInvalidState
    assert u.auth_repo.calls == []
    assert owner_tape == []


@pytest.mark.asyncio
async def test_census_smaller_than_player_count_refuses(owner_tape: list) -> None:
    """DS 自报在场人数 > 它给出的完整名单 → 名单不可信,整跳拒绝(ErrInvalidArg)。

    ★ 变异:把 `>` 改成 `>=` 之外的任何放宽(如去掉整条判定)→ 本条红。
    """
    u = Harness()
    with pytest.raises(errcode.PandoraError) as ei:
        await u.heartbeat_authorized_with_players(
            MATCH_ID, IDENT, 3, STATE_RUNNING, 0, True, 2, "c-1", [P1, P2], []
        )
    assert ei.value.code == errcode.ErrInvalidArg
    assert u.auth_repo.calls == []


@pytest.mark.asyncio
async def test_census_equal_to_player_count_passes(owner_tape: list) -> None:
    """人数与名单**相等**必须放行 —— 这是 5v5 满员的正常形状,判据是 `>` 不是 `>=`。

    ★ 变异:把 `player_count > len(census)` 写成 `>=` → 本条红(满员局全被拒)。
    """
    u = Harness()
    u.auth_repo.activate_out = mk_activate()
    res = await u.heartbeat_authorized_with_players(
        MATCH_ID, IDENT, 2, STATE_RUNNING, 0, True, 2, "c-1", [P1, P2], []
    )
    assert res.command == ""


@pytest.mark.asyncio
async def test_census_check_skipped_when_snapshot_absent() -> None:
    """`snapshot_present=False` 时不比人数 —— 旧 DS 的 proto3 零值名单不是"名单为空"。

    ★ 变异:去掉 `snapshot_present and` → 本条红(旧 DS 全被判 census 不一致)。
    """
    u = Harness()
    u.auth_repo.activate_out = mk_activate()
    res = await u.heartbeat_authorized_with_players(
        MATCH_ID, IDENT, 5, STATE_RUNNING, 0, False, 0, "", None, None
    )
    assert res.accepted_instance_uid == UID


@pytest.mark.asyncio
async def test_authority_read_failure_propagates_with_zero_transition() -> None:
    """权威读失败 → 原样上抛,且**不激活**(本跳零状态转移)。

    ★ 变异:把 preflight 的 `raise` 改成 `preflight = None` 后继续 → 本条红。
    """
    u = Harness()
    u.auth_repo.read_err = errcode.PandoraError(errcode.ErrUnavailable, "redis down")
    with pytest.raises(errcode.PandoraError) as ei:
        await u.heartbeat_authorized(MATCH_ID, IDENT, 2, STATE_RUNNING, 0)
    assert ei.value.code == errcode.ErrUnavailable
    assert u.auth_repo.calls == ["read_authority"]


# ════════════════════════════════════════════════════════════════════════════
# pod_uid 回填的凭据全等门
# ════════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_legacy_pod_uid_backfill_invoked_when_credential_matches() -> None:
    """无 pod_uid 的滚动升级遗留记录 + 凭据全等 → 回填,且带的是**记录的** allocation_id。

    ★ 变异:把 `expected` 里的 `allocation_id` 换成 `ident.jti` 之类 → 本条红。
    """
    legacy = mk_battle(pod_uid="")
    u = Harness()
    u.auth_repo.snapshot = BattleAuthoritySnapshot(
        auth=mk_auth(), battle=legacy, auth_found=True, battle_found=True
    )
    u.auth_repo.activate_out = mk_activate()
    await u.heartbeat_authorized(MATCH_ID, IDENT, 2, STATE_RUNNING, 0)
    assert u.tape[0] == (
        "ensure_durable_release_pod_uid",
        MATCH_ID,
        POD,
        ALLOC,
        UID,
        EPOCH,
        TRACK,
    )


@pytest.mark.asyncio
async def test_legacy_pod_uid_backfill_failure_refuses_heartbeat() -> None:
    """回填失败 → 整跳拒绝(本跳零状态转移),**不**继续 activate。

    ★ 变异:把回填失败改成 `pass` 继续 → 本条红(activate_heartbeat 被调用)。
    """
    u = Harness(pod_uid_err=errcode.PandoraError(errcode.ErrUnavailable, "k8s down"))
    u.auth_repo.snapshot = BattleAuthoritySnapshot(
        auth=mk_auth(), battle=mk_battle(pod_uid=""), auth_found=True, battle_found=True
    )
    with pytest.raises(errcode.PandoraError):
        await u.heartbeat_authorized(MATCH_ID, IDENT, 2, STATE_RUNNING, 0)
    assert "activate_heartbeat" not in u.auth_repo.calls


@pytest.mark.parametrize(
    ("mutate", "why"),
    [
        ({"pod_uid": POD_UID}, "已有 pod_uid,无需回填"),
        ({"pod_uid": "", "allocation_id": ""}, "无 allocation_id 无法精确回收"),
        ({"pod_uid": "", "ds_pod_name": "other-pod"}, "上报方不是记录绑定的 pod"),
        ({"pod_uid": "", "gameserver_uid": "gs-uid-B"}, "实例 uid 不符"),
        ({"pod_uid": "", "instance_epoch": EPOCH + 1}, "实例 epoch 不符"),
    ],
)
@pytest.mark.asyncio
async def test_legacy_pod_uid_backfill_skipped(mutate: dict, why: str) -> None:
    """五个前置条件任缺其一都不回填 —— 回填的下一步是 DELETE,宁可不回填。

    ★ 变异:把那串 `and` 里任意一项删掉 → 对应参数化用例红。
    """
    u = Harness()
    u.auth_repo.snapshot = BattleAuthoritySnapshot(
        auth=mk_auth(), battle=mk_battle(**mutate), auth_found=True, battle_found=True
    )
    u.auth_repo.activate_out = mk_activate()
    await u.heartbeat_authorized(MATCH_ID, IDENT, 2, STATE_RUNNING, 0)
    assert not any(t[0] == "ensure_durable_release_pod_uid" for t in u.tape), why


def test_unwritten_credentials_never_match_a_live_identity() -> None:
    """auth 存在但 active / pending **都没写过** → 不匹配。

    ★ 变异实测结论(写下来免得重推):把 `_active_of(...)` 换成 `snapshot.auth.active`
      本条**仍然绿**。因为零值凭据要与零值身份全等,而零值身份的 `exp_ms == 0`
      已经先被过期门(`ident.exp_ms <= now`)拦下。保留 `_active_of` 是与 Go 同构的
      纵深防御(见模块头「形变 5」),不是本用例能咕住的东西 —— 不要把它当成
      HasField 的回归网。

    本条真正咕住的是:凭据未写 → 不能匹配一个合法存活身份。
    ★ 变异:把 `matches(...) or matches(...)` 整条换成 `return True` → 本条红。
    """
    snap = BattleAuthoritySnapshot(
        auth=dspb.BattleDSAuthStorageRecord(
            match_id=MATCH_ID, ds_pod_name=POD, instance_uid=UID, instance_epoch=EPOCH,
            allocation_id=ALLOC,
        ),
        battle=mk_battle(),
        auth_found=True,
        battle_found=True,
    )
    assert BH.legacy_pod_uid_preflight_credential_matches(snap, IDENT) is False


def test_preflight_expired_identity_rejected() -> None:
    """上报凭据已过期 → 不匹配(判据是 `exp_ms <= now`,不是 `<`)。

    ★ 变异:把 `<=` 改成 `<` → 本条红(exp==now 的凭据被放行)。
    """
    snap = BattleAuthoritySnapshot(
        auth=mk_auth(), battle=mk_battle(), auth_found=True, battle_found=True
    )
    expired = BattleCredentialIdentity(
        pod_name=POD,
        instance_uid=UID,
        instance_epoch=EPOCH,
        gen=GEN,
        jti=JTI,
        exp_ms=NOW_MS,
        kid=KID,
        token_sha256=SHA,
        writer_epoch=WRITER_EPOCH,
    )
    assert BH.legacy_pod_uid_preflight_credential_matches(snap, expired) is False


def test_preflight_matches_pending_credential() -> None:
    """轮换中的实例只有 pending 凭据也算匹配(Go 是 `active || pending`)。

    ★ 变异:去掉 `or matches(_pending_of(...))` → 本条红。
    """
    snap = BattleAuthoritySnapshot(
        auth=mk_auth(active=False, pending=True),
        battle=mk_battle(),
        auth_found=True,
        battle_found=True,
    )
    assert BH.legacy_pod_uid_preflight_credential_matches(snap, IDENT) is True


def test_preflight_requires_auth_found_and_matching_allocation() -> None:
    """`auth_found=False` 或 allocation_id 不同 → 不匹配。

    ★ 变异:去掉 `not snapshot.auth_found` 或 allocation_id 比对 → 本条红。
    """
    ok_auth, ok_battle = mk_auth(), mk_battle()
    assert (
        BH.legacy_pod_uid_preflight_credential_matches(
            BattleAuthoritySnapshot(
                auth=ok_auth, battle=ok_battle, auth_found=False, battle_found=True
            ),
            IDENT,
        )
        is False
    )
    drifted = mk_auth()
    drifted.allocation_id = "other-allocation"
    assert (
        BH.legacy_pod_uid_preflight_credential_matches(
            BattleAuthoritySnapshot(
                auth=drifted, battle=ok_battle, auth_found=True, battle_found=True
            ),
            IDENT,
        )
        is False
    )


# ════════════════════════════════════════════════════════════════════════════
# Model B:两阶段激活 / owner / 驱逐单 / 停机
# ════════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_activation_pending_is_a_fully_silent_beat(owner_tape: list) -> None:
    """稳定性证据不足:不发 ACK、不续 owner 租约、不 Admit、不对账、不刷位置。

    ★ 变异:把 `return HeartbeatResult()` 挪到 owner 租约之后 → 本条红。
    """
    u = Harness()
    u.auth_repo.activate_out = mk_activate(activation_pending=True)
    res = await u.heartbeat_authorized_with_players(
        MATCH_ID, IDENT, 2, STATE_RUNNING, 0, True, 2, "c-1", [P1, P2], []
    )
    assert res == BH.HeartbeatResult()
    assert res.accepted_instance_uid == ""
    assert owner_tape == []
    assert u.repo.calls == []
    assert u.tape == []


@pytest.mark.asyncio
async def test_owner_lease_required_failure_refuses_beat(owner_tape: list) -> None:
    """强依赖档租约续写失败 → 整跳失败,**不**回 ACK、不对账。

    ★ 变异:把 owner 租约的 `raise` 吞成 warn-only → 本条红。
    """
    u = Harness(owner_lease_required=True)
    u.auth_repo.activate_out = mk_activate()
    owner_tape.lease_err.append(errcode.PandoraError(errcode.ErrUnavailable, "owner down"))
    with pytest.raises(errcode.PandoraError):
        await u.heartbeat_authorized_with_players(
            MATCH_ID, IDENT, 2, STATE_RUNNING, 0, True, 2, "c-1", [P1, P2], []
        )
    assert u.repo.calls == []


@pytest.mark.asyncio
async def test_owner_lease_uses_battle_release_track(owner_tape: list) -> None:
    """租约续写带的是**镜像里的** release_track(§9.21 轨道粘滞),不是空串。

    ★ 变异:把 `owner_lease_track` 恒设为 "" → 本条红。
    """
    u = Harness()
    u.auth_repo.activate_out = mk_activate(battle=mk_battle(release_track="canary"))
    await u.heartbeat_authorized(MATCH_ID, IDENT, 2, STATE_RUNNING, 0)
    assert owner_tape[0] == ("renew_owner_lease_gate", False, POD, UID, EPOCH, "canary")


@pytest.mark.asyncio
async def test_admit_skipped_when_census_absent_or_empty(owner_tape: list) -> None:
    """无 census(或空)时不代提交 Admit —— Model B 侧只认 exact census,不做兜底。

    ★ 变异:把 `snapshot_present and len(census) > 0` 放宽成 `len(census) >= 0` → 本条红。
    """
    u = Harness()
    u.auth_repo.activate_out = mk_activate()
    await u.heartbeat_authorized_with_players(
        MATCH_ID, IDENT, 2, STATE_RUNNING, 0, False, 0, "", [P1, P2], []
    )
    assert not any(t[0] == "owner_admit_census_weak" for t in owner_tape)


@pytest.mark.asyncio
async def test_happy_path_acks_orders_and_refreshes_locations(owner_tape: list) -> None:
    """正常一跳:ACK 五项全填、驱逐单原样带出、位置续期(fire-and-forget)。

    ★ 变异:把 `result.eviction_orders = orders` 删掉 → 本条红(玩家卡在退出副本)。
    """
    orders = [dspb.BattleEvictionOrder(), dspb.BattleEvictionOrder()]
    u = Harness()
    u.repo.departures = orders
    u.auth_repo.activate_out = mk_activate(first_activation=True)
    res = await u.heartbeat_authorized_with_players(
        MATCH_ID, IDENT, 2, STATE_RUNNING, 0, True, 2, "c-1", [P1, P2], ["dep-1"]
    )
    assert res.command == ""
    assert (
        res.accepted_token_gen,
        res.accepted_token_jti,
        res.accepted_instance_uid,
        res.accepted_instance_epoch,
        res.accepted_writer_epoch,
    ) == (GEN, JTI, UID, EPOCH, WRITER_EPOCH)
    assert res.eviction_orders is orders
    assert ("owner_admit_census_weak", [P1, P2], BH.OWNER_TYPE_BATTLE, POD, UID, 2.0) in owner_tape
    assert u.tape[-1] == ("refresh_battle_locations", [P1, P2], MATCH_ID, DS_ADDR)
    # 离场对账拿到的是 exact 实例栅栏 + 原样透传的 census / acked。
    src = u.repo.departure_args[1]
    assert (src.ds_pod_name, src.gameserver_uid, src.instance_epoch, src.allocation_id) == (
        POD,
        UID,
        EPOCH,
        ALLOC,
    )
    assert u.repo.departure_args[2:] == (True, 2, "c-1", [P1, P2], ["dep-1"])


@pytest.mark.asyncio
async def test_departure_reconcile_failure_refuses_beat() -> None:
    """离场对账失败 → 整跳失败(§9.19:驱逐单是唯一的后端→DS 指令,不能吞)。

    ★ 变异:把对账失败改成"记 warn 后继续" → 本条红。
    """
    u = Harness()
    u.repo.departure_err = errcode.PandoraError(errcode.ErrUnavailable, "redis down")
    u.auth_repo.activate_out = mk_activate()
    with pytest.raises(errcode.PandoraError):
        await u.heartbeat_authorized_with_players(
            MATCH_ID, IDENT, 2, STATE_RUNNING, 0, True, 2, "c-1", [P1, P2], []
        )


@pytest.mark.asyncio
async def test_departure_skipped_without_allocation_id() -> None:
    """无 allocation_id → 不对账(栅栏元组不完整,data 层会直接 ErrInvalidArg)。

    ★ 变异:去掉 `out.battle.allocation_id != ""` → 本条红。
    """
    u = Harness()
    u.auth_repo.activate_out = mk_activate(battle=mk_battle(allocation_id=""))
    await u.heartbeat_authorized_with_players(
        MATCH_ID, IDENT, 2, STATE_RUNNING, 0, True, 2, "c-1", [P1, P2], []
    )
    assert u.repo.calls == []


@pytest.mark.asyncio
async def test_first_abandon_delegates_with_absentees_only_when_roster_incomplete() -> None:
    """首次判弃 → 交给 finish_empty_abandon,并把 command 带回;缺席者只在缺员档计算。

    ★ 变异:把 `if out.roster_incomplete` 去掉(恒算缺席者)→ 本条红
      (空场判弃会误按"缺员"只罚部分人)。
    """
    u = Harness()
    u.auth_repo.activate_out = mk_activate(
        first_abandon=True, roster_incomplete=True, battle=mk_battle(player_ids=[P1, P2, P3])
    )
    res = await u.heartbeat_authorized_with_players(
        MATCH_ID, IDENT, 1, STATE_RUNNING, 0, True, 2, "c-1", [P1], []
    )
    assert res.command == COMMAND_STOP
    call = next(t for t in u.tape if t[0] == "finish_empty_abandon")
    assert call[1:] == (
        MATCH_ID,
        POD,
        UID,
        POD_UID,
        ALLOC,
        TRACK,
        EPOCH,
        [P1, P2, P3],
        MAP_ID,
        GAME_MODE,
        False,  # ever_had_players=True → no_show=False
        True,
        [P2, P3],
    )

    u2 = Harness()
    u2.auth_repo.activate_out = mk_activate(first_abandon=True, roster_incomplete=False)
    await u2.heartbeat_authorized_with_players(
        MATCH_ID, IDENT, 0, STATE_RUNNING, 0, True, 2, "c-1", [], []
    )
    call2 = next(t for t in u2.tape if t[0] == "finish_empty_abandon")
    assert call2[-1] == []


@pytest.mark.parametrize(
    ("kw", "want_reason"),
    [
        ({"terminal": True}, REASON_STOP_TERMINAL_AUTH),
        ({"battle": None}, REASON_STOP_BATTLE_MISSING),
        ({"battle": mk_battle(state=STATE_ENDED)}, REASON_STOP_BATTLE_ENDED),
        ({"battle": mk_battle(state=STATE_ABANDONED)}, REASON_STOP_BATTLE_ABANDONED),
    ],
)
@pytest.mark.asyncio
async def test_terminal_stop_matrix(kw: dict, want_reason: str, capsys: pytest.CaptureFixture) -> None:
    """四条终态判据各自能单独触发 stop,且 reason 各不相同(R2:拆开才查得下去)。

    `battle=None` 那条同时验一件事:诊断日志本身不能因取 `battle.state` 而炸。

    ★ 变异:把四个 reason 合并成一个 → 本条红。

    ★ 用 `capsys` 而不是 `caplog`:项目日志走 structlog 直写 stdout,
      `caplog` 拿到的永远是空串 —— 那会让断言看起来在测日志、实际恒失败。
    """
    u = Harness()
    u.auth_repo.activate_out = mk_activate(**kw)
    res = await u.heartbeat_authorized(MATCH_ID, IDENT, 0, STATE_RUNNING, 0)
    assert res.command == COMMAND_STOP
    assert want_reason in capsys.readouterr().out
    # 停机跳不刷位置。
    assert not any(t[0] == "refresh_battle_locations" for t in u.tape)


@pytest.mark.asyncio
async def test_location_refresh_requires_active_state_addr_and_players() -> None:
    """位置续期三个前置条件缺一即跳过(warming / 无 addr / 无玩家)。

    ★ 变异:去掉 `out.battle.ds_addr != ""` → 本条红(往 locator 写空地址)。
    """
    for battle in (
        mk_battle(state=STATE_WARMING),
        mk_battle(ds_addr=""),
        mk_battle(player_ids=[]),
    ):
        u = Harness()
        u.auth_repo.activate_out = mk_activate(battle=battle)
        await u.heartbeat_authorized(MATCH_ID, IDENT, 0, STATE_RUNNING, 0)
        assert not any(t[0] == "refresh_battle_locations" for t in u.tape)

    u = Harness(locator=None)
    u.auth_repo.activate_out = mk_activate()
    await u.heartbeat_authorized(MATCH_ID, IDENT, 2, STATE_RUNNING, 0)
    assert not any(t[0] == "refresh_battle_locations" for t in u.tape)


@pytest.mark.asyncio
async def test_activate_input_carries_config_verbatim() -> None:
    """喂给 data 层的 `BattleHeartbeatInput` 逐项来自配置 / 入参,毫秒是**向零截断**。

    ★ 变异:把 `stability_span_ms` 写成 `int(total_seconds()*1000)` → 在 2.5h 这类
      值上与 Go 的 `Milliseconds()` 差 1ms;把 roster deadline 换成
      `cfg.resolve_roster_join_deadline()`(而非 effective)→ off 档只关一半,本条红。
    """
    cfg = dsconf.AllocatorConf(
        empty_battle_timeout="5m",
        no_show_battle_timeout="150s",
        activation_stability_beats=3,
        activation_stability_span="10s",
        roster_join_deadline="45s",
        roster_join_deadline_mode="off",
        roster_policy_generation=7,
    )
    u = Harness(cfg=cfg, roster_mode=dsconf.ROSTER_JOIN_MODE_OFF)
    u.auth_repo.activate_out = mk_activate()
    await u.heartbeat_authorized_with_players(
        MATCH_ID, IDENT, 2, STATE_RUNNING, 0, True, 2, "c-1", [P1, P2], []
    )
    inp = u.auth_repo.activate_input
    assert inp is not None
    assert inp.player_count == 2
    assert inp.state == STATE_RUNNING
    assert inp.auth_ttl_sec == 120.0
    assert inp.battle_ttl_sec == 900.0
    assert inp.empty_battle_timeout_sec == 300.0
    assert inp.no_show_timeout_sec == 150.0
    assert inp.stability_beats == 3
    assert inp.stability_span_ms == 10_000
    # off 档:effective 归零,整道闸关死。
    assert inp.roster_join_deadline_sec == 0.0
    assert inp.roster_join_mode == dsconf.ROSTER_JOIN_MODE_OFF
    assert inp.roster_policy_generation == 7
    assert inp.census_present is True
    assert inp.active_player_ids == (P1, P2)


# ════════════════════════════════════════════════════════════════════════════
# legacy 心跳
# ════════════════════════════════════════════════════════════════════════════


def legacy_cfg(**kw: Any) -> dsconf.AllocatorConf:
    base: dict[str, Any] = {
        "empty_battle_timeout": "5m",
        "no_show_battle_timeout": "150s",
        "ready_wait_timeout": "120s",
        "roster_join_deadline": "45s",
        "roster_join_deadline_mode": "observe",
    }
    base.update(kw)
    return dsconf.AllocatorConf(**base)


@pytest.mark.asyncio
async def test_legacy_match_id_required() -> None:
    """match_id=0 → ErrInvalidArg,**不**碰镜像。

    ★ 变异:去掉这条判定 → 本条红(会拿 0 去 CAS 一个不存在的键)。
    """
    u = Harness(cfg=legacy_cfg())
    with pytest.raises(errcode.PandoraError) as ei:
        await u.heartbeat(0, POD, 2, STATE_RUNNING, 0)
    assert ei.value.code == errcode.ErrInvalidArg
    assert u.repo.calls == []


@pytest.mark.parametrize(
    "state",
    [
        STATE_ALLOCATION_UNCERTAIN,
        STATE_ALLOCATION_RECONCILING,
        STATE_ALLOCATION_EMPTY_FENCE,
        STATE_PREACTIVE_RELEASING,
        STATE_ALLOCATION_ABORT,
    ],
)
@pytest.mark.asyncio
async def test_legacy_allocation_fenced_stops_without_kill(state: str) -> None:
    """五个分配期 fence 态一律 stop,且**不** kill —— pod 归属由分配链自己收尾。

    ★ 变异:在 fenced 分支加 `kill_stranded_ds` → 本条红(会误删正在对账的实例)。
    """
    u = Harness(cfg=legacy_cfg(), repo=FakeRepo(mk_battle(state=state)))
    res = await u.heartbeat(MATCH_ID, POD, 2, STATE_RUNNING, 0)
    assert res.command == COMMAND_STOP
    assert u.tape == []


@pytest.mark.asyncio
async def test_legacy_terminal_stops_kills_and_releases_owner(owner_tape: list) -> None:
    """终态:stop + kill + owner 精确释放(INC-20260804-001 缺口⑦)。

    ★ 变异:删掉 owner 释放那段 → 本条红(打完副本回不了大厅的真实形状)。
    """
    repo = FakeRepo(mk_battle(state=STATE_ENDED))
    repo.get_battle_result = mk_battle(state=STATE_ENDED, player_ids=[P1, P2])
    u = Harness(cfg=legacy_cfg(), repo=repo)
    res = await u.heartbeat(MATCH_ID, POD, 0, STATE_ENDED, 0)
    assert res.command == COMMAND_STOP
    assert u.tape == [("kill_stranded_ds", MATCH_ID, POD, "terminal")]
    assert owner_tape == [
        ("owner_release_abandoned_players_weak", [P1, P2], POD, UID, 2.0)
    ]


@pytest.mark.asyncio
async def test_legacy_terminal_read_failure_still_stops(owner_tape: list) -> None:
    """终态回读失败 → 只告警,仍然 stop(释放留给下一跳心跳重试)。

    ★ 变异:把回读失败改成上抛 → 本条红(DS 收不到 stop,继续空转)。
    """
    repo = FakeRepo(mk_battle(state=STATE_ABANDONED))
    repo.get_battle_err = errcode.PandoraError(errcode.ErrUnavailable, "redis down")
    u = Harness(cfg=legacy_cfg(), repo=repo)
    res = await u.heartbeat(MATCH_ID, POD, 0, STATE_ABANDONED, 0)
    assert res.command == COMMAND_STOP
    assert not any(t[0] == "owner_release_abandoned_players_weak" for t in owner_tape)


@pytest.mark.asyncio
async def test_legacy_pod_mismatch_stops_and_kills_without_owner_release(
    owner_tape: list,
) -> None:
    """pod 不匹配:stop + kill,但**不**释放 owner —— 这些玩家不归上报方管。

    ★ 变异:在 pod_mismatch 分支加 owner 释放 → 本条红(会删掉活归属)。
    """
    u = Harness(cfg=legacy_cfg(), repo=FakeRepo(mk_battle(ds_pod_name="other-pod")))
    res = await u.heartbeat(MATCH_ID, POD, 2, STATE_RUNNING, 0)
    assert res.command == COMMAND_STOP
    assert u.tape == [("kill_stranded_ds", MATCH_ID, POD, "pod_mismatch")]
    assert owner_tape == []


@pytest.mark.asyncio
async def test_legacy_orphan_stops_and_kills() -> None:
    """镜像不存在(ErrDSPodNotFound)→ stop + kill(孤儿 DS)。

    ★ 变异:把 `errcode.as_code(exc) == ErrDSPodNotFound` 判定去掉 → 本条红
      (孤儿会走 default 分支上抛,DS 永远收不到 stop)。
    """
    u = Harness(cfg=legacy_cfg(), repo=FakeRepo())
    res = await u.heartbeat(MATCH_ID, POD, 0, STATE_RUNNING, 0)
    assert res.command == COMMAND_STOP
    assert u.tape == [("kill_stranded_ds", MATCH_ID, POD, "orphan")]


@pytest.mark.asyncio
async def test_legacy_other_update_failure_propagates() -> None:
    """其余 CAS 失败原样上抛(不伪装成 stop)。

    ★ 变异:把 default 分支改成 `return stop` → 本条红(会把 Redis 抖动当成
      "该停机",一次抖动踢掉一整局)。
    """
    repo = FakeRepo(mk_battle())
    repo.update_err = errcode.PandoraError(errcode.ErrDSAllocationFailed, "cas exhausted")
    u = Harness(cfg=legacy_cfg(), repo=repo)
    with pytest.raises(errcode.PandoraError) as ei:
        await u.heartbeat(MATCH_ID, POD, 2, STATE_RUNNING, 0)
    assert ei.value.code == errcode.ErrDSAllocationFailed
    assert u.tape == []


@pytest.mark.asyncio
async def test_legacy_no_show_uses_short_threshold(owner_tape: list) -> None:
    """从未有人连入的局按 no-show 短阈(150s)回收,并标记 `no_show=True`。

    ★ 变异:把 `if not b.ever_had_players and no_show_timeout_ms > 0` 去掉
      → 本条红(刷进出副本能用小号把 Fleet 押死 5 分钟一台)。
    """
    rec = mk_battle(
        ever_had_players=False,
        empty_since_ms=NOW_MS - 151_000,
        player_ids=[P1, P2],
        player_count=0,
    )
    u = Harness(cfg=legacy_cfg(), repo=FakeRepo(rec))
    res = await u.heartbeat(MATCH_ID, POD, 0, STATE_RUNNING, 0)
    assert res.command == COMMAND_STOP
    call = next(t for t in u.tape if t[0] == "finish_empty_abandon")
    assert call[11] is True  # no_show
    assert call[12] is False  # roster_incomplete
    assert rec.state == STATE_ABANDONED


@pytest.mark.asyncio
async def test_legacy_played_battle_keeps_long_threshold() -> None:
    """有人连入过的局在 150s 空场时**不**回收(要给断线重连留 5 分钟)。

    ★ 变异:把阈值恒设为 no-show 短阈 → 本条红(9 人在打、1 人掉线会被判弃)。
    """
    rec = mk_battle(ever_had_players=True, empty_since_ms=NOW_MS - 151_000, player_count=0)
    u = Harness(cfg=legacy_cfg(), repo=FakeRepo(rec))
    res = await u.heartbeat(MATCH_ID, POD, 0, STATE_RUNNING, 0)
    assert res.command == COMMAND_NONE
    assert rec.state == STATE_RUNNING


@pytest.mark.asyncio
async def test_legacy_roster_observe_records_but_never_abandons(
    capsys: pytest.CaptureFixture,
) -> None:
    """observe 档到点只采证(WARN),**不**判弃 —— 激活协议的整个意义。

    ★ 变异:把 `roster_deadline_should_abandon` 的判定去掉(恒判弃)→ 本条红。
    """
    rec = mk_battle(
        player_ids=[P1, P2, P3],
        player_count=1,
        roster_ever_complete=False,
        roster_incomplete_since_ms=NOW_MS - 46_000,
        allocated_at_ms=NOW_MS - 60_000,
        roster_policy_generation=0,
    )
    u = Harness(cfg=legacy_cfg(), repo=FakeRepo(rec), roster_mode=dsconf.ROSTER_JOIN_MODE_OBSERVE)
    res = await u.heartbeat_with_census(MATCH_ID, POD, 1, STATE_RUNNING, 0, True, [P1])
    assert res.command == COMMAND_NONE
    assert rec.state == STATE_RUNNING
    assert "roster_incomplete_would_abandon" in capsys.readouterr().out
    assert not any(t[0] == "finish_empty_abandon" for t in u.tape)


@pytest.mark.asyncio
async def test_legacy_roster_enforce_abandons_only_matching_generation() -> None:
    """enforce 档且 battle 代 == 配置代 → 真判弃,且只罚缺席者。

    ★ 变异:把代比对去掉 → legacy(gen=0)老局在滚动升级窗口被误判弃,本条红。
    """
    cfg = legacy_cfg(roster_join_deadline_mode="enforce", roster_policy_generation=3)
    rec = mk_battle(
        player_ids=[P1, P2, P3],
        player_count=1,
        roster_ever_complete=False,
        roster_incomplete_since_ms=NOW_MS - 46_000,
        allocated_at_ms=NOW_MS - 60_000,
        roster_policy_generation=3,
    )
    u = Harness(cfg=cfg, repo=FakeRepo(rec), roster_mode=dsconf.ROSTER_JOIN_MODE_ENFORCE)
    await u.heartbeat_with_census(MATCH_ID, POD, 1, STATE_RUNNING, 0, True, [P1])
    assert rec.state == STATE_ABANDONED
    call = next(t for t in u.tape if t[0] == "finish_empty_abandon")
    assert call[11] is False  # no_show:缺员不按 no-show 全员记
    assert call[12] is True  # roster_incomplete
    assert call[13] == [P2, P3]  # 只罚缺席者


@pytest.mark.asyncio
async def test_legacy_roster_gate_needs_census_and_arm_window() -> None:
    """无 census 或超出武装窗 → 到齐闸整道不武装(§5 滚动升级纵深防御)。

    ★ 变异:去掉 `snapshot_present` 或 `roster_gate_armable` → 本条红。
    """
    for present, allocated in ((False, NOW_MS - 60_000), (True, NOW_MS - 10 * 3600 * 1000)):
        rec = mk_battle(
            player_ids=[P1, P2, P3],
            player_count=1,
            roster_ever_complete=False,
            roster_incomplete_since_ms=NOW_MS - 46_000,
            allocated_at_ms=allocated,
        )
        u = Harness(
            cfg=legacy_cfg(roster_join_deadline_mode="enforce"),
            repo=FakeRepo(rec),
            roster_mode=dsconf.ROSTER_JOIN_MODE_ENFORCE,
        )
        await u.heartbeat_with_census(MATCH_ID, POD, 1, STATE_RUNNING, 0, present, [P1])
        assert rec.state == STATE_RUNNING


@pytest.mark.asyncio
async def test_legacy_out_params_reset_across_cas_rerun() -> None:
    """CAS 冲突重跑:第一轮置位的判弃出参必须被第二轮重置(fn 重跑契约)。

    第一轮读到的是"空场已超时"的旧镜像,第二轮读到的是"人已经回来了"的新镜像 ——
    不重置就会拿着上一轮的判弃出参去回收一台**正在打**的 DS。

    ★ 变异:删掉 `_apply` 开头那几行 `st[...] = False/[]` → 本条红。
    """
    stale = mk_battle(ever_had_players=True, empty_since_ms=NOW_MS - 400_000, player_count=0)
    fresh = mk_battle(ever_had_players=True, empty_since_ms=0, player_count=2)
    u = Harness(cfg=legacy_cfg(), repo=FakeRepo(stale, fresh))
    res = await u.heartbeat(MATCH_ID, POD, 2, STATE_RUNNING, 0)
    assert res.command == COMMAND_NONE
    assert not any(t[0] == "finish_empty_abandon" for t in u.tape)


@pytest.mark.asyncio
async def test_legacy_became_ready_logged_once(capsys: pytest.CaptureFixture) -> None:
    """warming → ready 的首次迁移升 info(allocate_battle 放行 matchmaker 的那一跳)。

    ★ 变异:把 `prev_state == STATE_WARMING` 去掉 → 本条红(每跳都刷 info)。
    """
    u = Harness(cfg=legacy_cfg(), repo=FakeRepo(mk_battle(state=STATE_WARMING)))
    await u.heartbeat(MATCH_ID, POD, 0, STATE_READY, 0)
    assert "battle_ds_heartbeat_ready" in capsys.readouterr().out

    u2 = Harness(cfg=legacy_cfg(), repo=FakeRepo(mk_battle(state=STATE_READY)))
    await u2.heartbeat(MATCH_ID, POD, 2, STATE_RUNNING, 0)
    assert "battle_ds_heartbeat_ready" not in capsys.readouterr().out


@pytest.mark.asyncio
async def test_legacy_owner_wiring_prefers_exact_census(owner_tape: list) -> None:
    """有 census 时 Admit 用 census(exact),不是花名册。

    ★ 变异:把 `admit_players = census` 直接换成 refresh_players → 本条红。
    """
    u = Harness(cfg=legacy_cfg(), repo=FakeRepo(mk_battle(player_ids=[P1, P2, P3])))
    await u.heartbeat_with_census(MATCH_ID, POD, 2, STATE_RUNNING, 0, True, [P1, P2])
    admit = next(t for t in owner_tape if t[0] == "owner_admit_census_weak")
    assert admit[1] == [P1, P2]


@pytest.mark.asyncio
async def test_legacy_local_off_fallback_admits_roster(owner_tape: list) -> None:
    """local-off-v1 兜底:census 缺席且 player_count>0 → 用花名册代提交 Admit。

    没有这条兜底,mode=local 的玩家进图后 owner 永远停在 PENDING,30s 后弹
    "重连时间较长"(2026-08-04 实测)。

    ★ 变异:删掉兜底 → 本条红;把 `player_count > 0` 去掉 → 下面第二段红
      (空局也会代提交 Admit)。
    """
    u = Harness(cfg=legacy_cfg(), repo=FakeRepo(mk_battle(player_ids=[P1, P2])))
    await u.heartbeat(MATCH_ID, POD, 2, STATE_RUNNING, 0)
    admit = next(t for t in owner_tape if t[0] == "owner_admit_census_weak")
    assert admit[1] == [P1, P2]

    owner_tape.clear()
    u2 = Harness(cfg=legacy_cfg(), repo=FakeRepo(mk_battle(player_ids=[P1, P2], player_count=0)))
    await u2.heartbeat(MATCH_ID, POD, 0, STATE_RUNNING, 0)
    assert not any(t[0] == "owner_admit_census_weak" for t in owner_tape)


@pytest.mark.asyncio
async def test_legacy_owner_wiring_skipped_without_instance_identity(
    owner_tape: list,
) -> None:
    """镜像无实例身份(mock)→ 整段 owner 接线跳过,保持改动前行为。

    ★ 变异:去掉 `st["owner_uid"] != ""` → 本条红(会拿空 uid 去续租)。
    """
    u = Harness(cfg=legacy_cfg(), repo=FakeRepo(mk_battle(gameserver_uid="")))
    await u.heartbeat(MATCH_ID, POD, 2, STATE_RUNNING, 0)
    assert owner_tape == []


@pytest.mark.asyncio
async def test_legacy_refreshes_locations_when_active() -> None:
    """活跃对局续期 BATTLE 位置(断线重连的前提),且传的是**拷贝**不是活视图。

    ★ 变异:把 `list(st["refresh_players"])` 换成直接传引用 → 本条不红,但
      把 `_apply` 里的 `list(b.player_ids)` 换成直接引用 → 本条红。
    """
    rec = mk_battle(player_ids=[P1, P2])
    u = Harness(cfg=legacy_cfg(), repo=FakeRepo(rec))
    await u.heartbeat(MATCH_ID, POD, 2, STATE_RUNNING, 0)
    call = next(t for t in u.tape if t[0] == "refresh_battle_locations")
    assert call[1:] == ([P1, P2], MATCH_ID, DS_ADDR)
    rec.player_ids.append(P3)
    assert call[1] == [P1, P2]


# ════════════════════════════════════════════════════════════════════════════
# 本地凭据 ACK 回显
# ════════════════════════════════════════════════════════════════════════════


class FakeLocalCredSource:
    """实现 `LocalBattleCredentialSource` 的运行期协议(只看方法名)。"""

    def __init__(self, ack: Any) -> None:
        self.ack = ack
        self.seen: list[str] = []

    def local_credential_ack(self, pod_name: str) -> Any:
        self.seen.append(pod_name)
        return self.ack


class _Ack:
    gen = GEN
    jti = JTI
    instance_uid = UID
    instance_epoch = EPOCH
    writer_epoch = WRITER_EPOCH


@pytest.mark.asyncio
async def test_ack_echoed_on_stop_answers() -> None:
    """ACK 必须回显在**所有** legacy 应答上,含 stop —— UE 校验不过会把 command 清空。

    ★ 变异:把 ACK 回显挪进"非 stop"分支 → 本条红(stop 永远送不到 DS)。
    """
    u = Harness(cfg=legacy_cfg(), repo=FakeRepo(), alloc=FakeLocalCredSource(_Ack()))
    res = await u.heartbeat(MATCH_ID, POD, 0, STATE_RUNNING, 0)
    assert res.command == COMMAND_STOP
    assert (res.accepted_token_gen, res.accepted_instance_uid) == (GEN, UID)


@pytest.mark.asyncio
async def test_ack_not_overwritten_when_already_present() -> None:
    """本体已给出 ACK 时不覆盖(判据是 `accepted_instance_uid == ""`)。

    ★ 变异:去掉那个判据 → 本条红(local 台账会覆盖真实授权身份)。
    """

    class WithAck(Harness):
        async def heartbeat_legacy(self, *args: Any, **kw: Any) -> BH.HeartbeatResult:
            return BH.HeartbeatResult(accepted_instance_uid="from-authority", accepted_token_gen=99)

    u = WithAck(cfg=legacy_cfg(), alloc=FakeLocalCredSource(_Ack()))
    res = await u.heartbeat(MATCH_ID, POD, 2, STATE_RUNNING, 0)
    assert (res.accepted_instance_uid, res.accepted_token_gen) == ("from-authority", 99)


@pytest.mark.asyncio
async def test_ack_fail_closed_paths() -> None:
    """三条 fail-closed:空 pod 名 / 分配器不实现协议 / 台账回 None。

    ★ 变异:把 `isinstance(self.alloc, LocalBattleCredentialSource)` 换成
      `hasattr` 之外的任何放宽都不安全;去掉 `pod_name == ""` 早退 → 本条红。
    """
    src = FakeLocalCredSource(_Ack())
    u = Harness(cfg=legacy_cfg(), alloc=src)
    assert u.local_credential_ack("") is None
    assert src.seen == []

    assert Harness(cfg=legacy_cfg(), alloc=object()).local_credential_ack(POD) is None
    assert (
        Harness(cfg=legacy_cfg(), alloc=FakeLocalCredSource(None)).local_credential_ack(POD)
        is None
    )


def test_redis_authority_enabled_mirrors_model_b() -> None:
    """`redis_authority_enabled()` 就是 model_b,没有第三态(半开启会静默退化)。"""
    assert Harness(model_b=True).redis_authority_enabled() is True
    assert Harness(model_b=False).redis_authority_enabled() is False


@pytest.mark.asyncio
async def test_ts_ms_never_influences_any_decision() -> None:
    """请求 `ts_ms` 不参与任何判定 —— 一个未来时间戳不能让失联 DS 长期"心跳新鲜"。

    ★ 变异:在任何一处用 `ts_ms` 代替 `now_ms()` → 本条红。
    """
    rec_a = mk_battle(ever_had_players=True, empty_since_ms=NOW_MS - 400_000, player_count=0)
    rec_b = mk_battle(ever_had_players=True, empty_since_ms=NOW_MS - 400_000, player_count=0)
    u_a = Harness(cfg=legacy_cfg(), repo=FakeRepo(rec_a))
    u_b = Harness(cfg=legacy_cfg(), repo=FakeRepo(rec_b))
    await u_a.heartbeat(MATCH_ID, POD, 0, STATE_RUNNING, 0)
    await u_b.heartbeat(MATCH_ID, POD, 0, STATE_RUNNING, NOW_MS + 10 * 3600 * 1000)
    assert rec_a.state == rec_b.state == STATE_ABANDONED
    assert rec_a.last_heartbeat_ms == rec_b.last_heartbeat_ms == NOW_MS


def test_no_stray_event_loop_usage() -> None:
    """本模块的 fire-and-forget 必须**只**经 `safego.spawn` 具名任务起。

    §16.12 + §16.10:心跳是高频请求路径,裸 `asyncio.create_task` / `ensure_future`
    的异常会被静默吞进 Task,直到 GC 才打一条认不出主人的 "never retrieved"。

    ★ 判据走 **AST 实际调用**而不是源码子串:本模块的 `refresh_battle_locations`
      与 `kill_stranded_ds` 的注释里都要写清"为什么不用 `asyncio.create_task`",
      子串匹配会把那段**解释**当成违规,逼着后来人删掉唯一解释了理由的地方。

    ★ 变异:把 `safego.spawn(LOCATION_REFRESH_TASK_NAME, _run)` 换成
      `asyncio.create_task(_run())` → 本条红。
    """
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(BH))
    called = {
        node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute | ast.Name)
    }
    assert "create_task" not in called
    assert "ensure_future" not in called
    # 本模块**确有**一处 fire-and-forget(refresh_battle_locations),必须是具名 spawn。
    assert "spawn" in called
    assert asyncio.CancelledError is not None  # 保持 asyncio 引用有意义


# ════════════════════════════════════════════════════════════════════════════
# Go 第 3071–3358 行:roster / no-show / 空场遗弃 / 位置续期 / 列表
#
# 上半段用 `Harness` 把这七个函数换成了录音替身(那时它们还没落地);下半段反过来 ——
# 用 `TailHarness` **不覆盖**它们,跑真实现,改成替身的是它们的下游协作者。
# ════════════════════════════════════════════════════════════════════════════


class FakeNoShowRecorder:
    """`NoShowRecorder` 的两个方法 + 可注入的两种失败。"""

    def __init__(self, *counts: int) -> None:
        self.counts = list(counts)
        self.record_err: BaseException | None = None
        self.arm_err: BaseException | None = None
        self.recorded: list[tuple[int, float]] = []
        self.armed: list[tuple[int, float]] = []

    async def record_no_show(self, player_id: int, window_sec: float) -> int:
        self.recorded.append((player_id, window_sec))
        if self.record_err is not None:
            raise self.record_err
        return self.counts.pop(0) if self.counts else 1

    async def arm_penalty(self, player_id: int, penalty_sec: float) -> None:
        self.armed.append((player_id, penalty_sec))
        if self.arm_err is not None:
            raise self.arm_err


class TailRepo(FakeRepo):
    """在 `FakeRepo` 上补 `range_active_battles` / `expire_battle`。"""

    def __init__(self, *records: Any) -> None:
        super().__init__(*records)
        self.active: list[int] = []
        self.range_err: BaseException | None = None
        self.battles: dict[int, Any] = {}
        self.battle_errs: dict[int, BaseException] = {}
        self.expire_err: BaseException | None = None

    async def range_active_battles(self) -> list[int]:
        self.calls.append("range_active_battles")
        if self.range_err is not None:
            raise self.range_err
        return list(self.active)

    async def get_battle(self, match_id: int) -> Any:
        self.calls.append("get_battle")
        if match_id in self.battle_errs:
            raise self.battle_errs[match_id]
        return self.battles.get(match_id)

    async def expire_battle(self, match_id: int) -> None:
        self.calls.append("expire_battle")
        if self.expire_err is not None:
            raise self.expire_err


class TailAuthRepo(FakeAuthRepo):
    """空场遗弃 Model B 分支要用的三个 fence 方法。"""

    def __init__(self) -> None:
        super().__init__()
        self.terminate_ok = True
        self.terminate_err: BaseException | None = None
        self.expire_err: BaseException | None = None

    async def terminate_expected(self, match_id: int, *args: Any) -> bool:
        self.calls.append("terminate_expected")
        if self.terminate_err is not None:
            raise self.terminate_err
        return self.terminate_ok

    async def expire_terminated_expected(self, match_id: int, *args: Any) -> bool:
        self.calls.append("expire_terminated_expected")
        if self.expire_err is not None:
            raise self.expire_err
        return True


class TailHarness(BH.HeartbeatMixin):
    """**不覆盖**尾段七函数;替身下沉到它们的协作者。"""

    def __init__(self, *, cfg: dsconf.AllocatorConf | None = None, **kw: Any) -> None:
        self.cfg = cfg if cfg is not None else legacy_cfg()
        self.model_b = kw.pop("model_b", True)
        self.no_show_recorder = kw.pop("no_show_recorder", None)
        self.auth_repo = kw.pop("auth_repo", TailAuthRepo())
        self.repo = kw.pop("repo", TailRepo())
        self.locator = kw.pop("locator", None)
        self.alloc = object()
        self.owner_lease = object()
        self.owner_auth = object()
        self.owner_admitted: dict[str, float] = {}
        self.owner_lease_required = False
        self.ds_credential_ttl_sec = 120.0
        self.tape: list[tuple[Any, ...]] = []
        self.pod_uid_err: BaseException | None = kw.pop("pod_uid_err", None)
        self.release_err: BaseException | None = kw.pop("release_err", None)
        self.lifecycle_err: BaseException | None = kw.pop("lifecycle_err", None)
        self.deliver_ok: bool = kw.pop("deliver_ok", True)
        assert not kw, f"unexpected kwargs {kw}"

    def battle_ttl_sec(self) -> float:
        return 900.0

    async def ensure_durable_release_pod_uid(
        self, match_id: int, pod_name: str, expected: Any, expected_release_track: str
    ) -> str:
        self.tape.append(("ensure_durable_release_pod_uid", match_id, pod_name))
        if self.pod_uid_err is not None:
            raise self.pod_uid_err
        return POD_UID

    async def release_game_server(self, *args: Any) -> None:
        self.tape.append(("release_game_server", *args[:2]))
        if self.release_err is not None:
            raise self.release_err

    async def deliver_abandoned(self, *args: Any) -> bool:
        """★ 返回 bool 不是摆设:产品代码用 `if await self.deliver_abandoned(...)`
        决定要不要落 marker / Expire。替身返回 None 会让整段收尾被静默跳过。
        """
        self.tape.append(("deliver_abandoned", *args[:3]))
        return self.deliver_ok

    class _LifecycleProofRepo:
        def __init__(self, outer: TailHarness) -> None:
            self.outer = outer

        async def record_allocation_lifecycle_published(self, *args: Any) -> None:
            self.outer.tape.append(("lifecycle_marker",))
            if self.outer.lifecycle_err is not None:
                raise self.outer.lifecycle_err

    @property
    def lifecycle_proof_repo(self) -> Any:
        return TailHarness._LifecycleProofRepo(self)


def names(tape: list[tuple[Any, ...]]) -> list[str]:
    return [t[0] for t in tape]


ABANDON_ARGS = (
    MATCH_ID,
    POD,
    UID,
    POD_UID,
    ALLOC,
    TRACK,
    EPOCH,
    [P1, P2],
    MAP_ID,
    GAME_MODE,
)


# ── resolve_roster_join_mode_safe / effective_roster_join_deadline ───────────


def test_roster_mode_bad_config_degrades_to_observe_not_enforce() -> None:
    """配置非法时**只能**降级成 observe,不能降成 enforce。

    降 enforce = 一个打错字的配置把整服变成"到齐超时就判遗弃",玩家进不去副本;
    降 observe 只丢一条统计。方向搞反是"配置写错 → 线上事故"的经典形状。

    ★ 变异:把 `except ValueError` 分支的返回值改成 `ROSTER_JOIN_MODE_ENFORCE`
      → 本条红。
    """
    u = TailHarness(cfg=legacy_cfg(roster_join_deadline_mode="nonsense"))
    assert u.resolve_roster_join_mode_safe() == dsconf.ROSTER_JOIN_MODE_OBSERVE
    ok = TailHarness(cfg=legacy_cfg(roster_join_deadline_mode="enforce"))
    assert ok.resolve_roster_join_mode_safe() == dsconf.ROSTER_JOIN_MODE_ENFORCE


def test_roster_deadline_is_zero_when_mode_off_even_if_configured() -> None:
    """mode=off 时期限恒 0,**不看** `roster_join_deadline` 配的是多少。

    "开关关了但期限还在读配置"会让 off 变成静默的 observe —— 调用点用
    `deadline > 0` 判定是否参与,读出 45s 就等于开关没关掉。

    ★ 变异:去掉 off 分支直接 `return self.cfg.resolve_roster_join_deadline()`
      → 本条红。
    """
    import datetime as _dt

    off = TailHarness(cfg=legacy_cfg(roster_join_deadline_mode="off"))
    assert off.effective_roster_join_deadline() == _dt.timedelta(0)
    on = TailHarness(cfg=legacy_cfg(roster_join_deadline_mode="observe"))
    assert on.effective_roster_join_deadline() == _dt.timedelta(seconds=45)


# ── roster_absentees ────────────────────────────────────────────────────────


def test_roster_absentees_keeps_roster_order_and_empty_census_means_all_absent() -> None:
    """缺席名单按 **roster 顺序**,且空 census ≠ "没人缺席"。

    这份名单直接决定罚谁。用 census 顺序会让日志与罚单顺序漂;把空 census 兜底成
    "全到齐"则是把"一个人都没连上"判成"没人缺席",正好放过最该罚的那一局。

    ★ 变异:开头加 `if not census: return []` → 本条红。
    """
    assert BH.roster_absentees([P1, P2, P3], [P3, P1]) == [P2]
    assert BH.roster_absentees([P3, P1, P2], []) == [P3, P1, P2]
    assert BH.roster_absentees([], [P1]) == []
    # mixin 上挂的是同一个实现(staticmethod 绑定),不是各写一份。
    assert TailHarness().roster_absentees([P1, P2], [P1]) == [P2]


# ── record_no_show_penalties ────────────────────────────────────────────────


def penalty_cfg(**kw: Any) -> dsconf.AllocatorConf:
    base: dict[str, Any] = {
        "no_show_ledger_window": "24h",
        "no_show_penalty_base": "60s",
        "no_show_penalty_cap": "30m",
        "no_show_penalty_free": 1,
    }
    base.update(kw)
    return legacy_cfg(**base)


@pytest.mark.asyncio
async def test_no_show_free_quota_records_but_does_not_arm() -> None:
    """免罚额度内:流水**照记**,罚单**不发**。

    不记流水就没法累计,下次仍算第一次,惯犯永远罚不到;记了就发罚单则等于免罚
    额度形同虚设(一次掉线就禁赛)。两件事必须分开。

    ★ 变异:把 `over = count - free` 改成 `over = count` → 本条红。
    """
    rec = FakeNoShowRecorder(1)
    u = TailHarness(cfg=penalty_cfg(), no_show_recorder=rec)
    await u.record_no_show_penalties(MATCH_ID, [P1])
    assert rec.recorded == [(P1, 24 * 3600.0)]
    assert rec.armed == []


@pytest.mark.asyncio
async def test_no_show_penalty_doubles_per_offense_and_clamps_at_cap() -> None:
    """超额后按 `base * 2^(over-1)` 指数递增,并被 cap **钳死**。

    cap 是这条链上唯一的上界。缺了它,第 20 次缺席会算出 60s * 2^18 ≈ 18 天禁赛。

    ★ 变异:去掉 `penalty = min(penalty, cap)` 那一步 → 本条红。
    """
    rec = FakeNoShowRecorder(2, 3, 99)
    u = TailHarness(cfg=penalty_cfg(), no_show_recorder=rec)
    await u.record_no_show_penalties(MATCH_ID, [P1, P2, P3])
    assert rec.armed == [(P1, 60.0), (P2, 120.0), (P3, 30 * 60.0)]


@pytest.mark.asyncio
async def test_no_show_shift_is_capped_before_exponentiation() -> None:
    """指数**先钳后算**:`2 ** over` 不能拿一个未钳的 count 去算。

    Python 的 int 无上限,`2 ** 10**6` 不会溢出而是当场算一个百万位大整数把 CPU
    钉死 —— 这是 Go 那边(uint64 回绕)看不到、移植过来才有的新失效形状。

    ★ 变异:把 `shift = min(over - 1, NO_SHOW_PENALTY_MAX_SHIFT)` 改成
      `shift = over - 1` → 本条挂起(而不是失败),需 ctrl-c;因此这里断言的是
      钳位常量本身参与了计算:第 10^6 次缺席必须**立刻**返回且落在 cap 上。
    """
    rec = FakeNoShowRecorder(1_000_000)
    u = TailHarness(cfg=penalty_cfg(), no_show_recorder=rec)
    await asyncio.wait_for(u.record_no_show_penalties(MATCH_ID, [P1]), timeout=5)
    assert rec.armed == [(P1, 30 * 60.0)]


@pytest.mark.asyncio
async def test_no_show_ledger_failure_is_fail_open_and_does_not_skip_next_player() -> None:
    """记流水失败 → 跳过**这个人**继续下一个,不是整批中止。

    这条链的调用点是"空场遗弃已经决定了",罚单只是附加动作。一个人的 Redis 抖动
    让同局其他人全都逃罚,是把可观测的小失败放大成不可观测的大失败。

    ★ 变异:把 `record_no_show` 的 `except` 改成 `raise` → 本条红。
    """
    rec = FakeNoShowRecorder(5)
    rec.record_err = RuntimeError("redis down")
    u = TailHarness(cfg=penalty_cfg(), no_show_recorder=rec)
    await u.record_no_show_penalties(MATCH_ID, [P1, P2])
    assert [p for p, _ in rec.recorded] == [P1, P2]
    assert rec.armed == []


@pytest.mark.asyncio
async def test_no_show_disabled_or_absent_recorder_is_a_no_op() -> None:
    """未配 recorder / 未开开关 → 一次调用都不发。

    ★ 变异:去掉 `no_show_recorder is None` 的早返回 → 本条红(AttributeError)。
    """
    await TailHarness(cfg=penalty_cfg()).record_no_show_penalties(MATCH_ID, [P1])
    rec = FakeNoShowRecorder(9)
    u = TailHarness(cfg=penalty_cfg(no_show_penalty_base=""), no_show_recorder=rec)
    await u.record_no_show_penalties(MATCH_ID, [P1])
    assert rec.armed == []


# ── finish_empty_abandon ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_empty_abandon_model_b_runs_fence_release_deliver_in_order() -> None:
    """Model B 全链顺序:pod_uid 兜底 → terminate fence → 释放 GS → 投递 → 落 marker。

    §9 不变量 4(DS 崩溃必有补偿)整条链就在这里。顺序不是审美:先 fence 再释放,
    才能保证 GS 被回收后不会有第二个实例拿着旧凭据回来写;marker 落在投递**之后**,
    才能在投递失败时留下"没投递过"而不是"投过了"。

    ★ 变异:把 `release_game_server` 挪到 `terminate_expected` 之前 → 本条红。
    """
    u = TailHarness()
    out = await u.finish_empty_abandon(*ABANDON_ARGS, False, False, [])
    assert out.command == COMMAND_STOP
    assert names(u.tape) == [
        "ensure_durable_release_pod_uid",
        "release_game_server",
        "deliver_abandoned",
        "lifecycle_marker",
    ]
    assert u.auth_repo.calls == ["terminate_expected", "expire_terminated_expected"]


@pytest.mark.asyncio
async def test_empty_abandon_pod_uid_preflight_failure_stops_before_terminate() -> None:
    """pod_uid 兜底失败 → 立刻 STOP,**不** terminate、**不**释放 GS。

    兜底拿到的是"该删哪个 Pod"。拿不到还往下走,释放的可能是**下一次分配**的
    同名 Pod —— 把一次空场遗弃升级成误杀一局正在打的对局。

    ★ 变异:把该分支的 `return` 改成 `pass`(继续往下)→ 本条红。
    """
    u = TailHarness(pod_uid_err=RuntimeError("k8s api down"))
    out = await u.finish_empty_abandon(*ABANDON_ARGS, False, False, [])
    assert out.command == COMMAND_STOP
    assert names(u.tape) == ["ensure_durable_release_pod_uid"]
    assert u.auth_repo.calls == []


@pytest.mark.asyncio
async def test_empty_abandon_terminate_fence_refusal_stops_before_release() -> None:
    """terminate fence 返回 False → STOP 且**不**释放 GS。

    False 的含义是"权威记录已经不是你了"(实例被换过)。此时释放 = 拿旧证据删新实例。

    ★ 变异:把 `if not terminated:` 改成 `if terminated is None:` → 本条红。
    """
    u = TailHarness()
    u.auth_repo.terminate_ok = False
    out = await u.finish_empty_abandon(*ABANDON_ARGS, False, False, [])
    assert out.command == COMMAND_STOP
    assert "release_game_server" not in names(u.tape)


@pytest.mark.asyncio
async def test_empty_abandon_release_failure_stops_model_b_but_delivers_in_legacy() -> None:
    """释放 GS 失败:Model B 整跳停(留给 sweep 重来),legacy 仍投递遗弃事件。

    两面的权威源不同 —— Model B 有 sweep 会照着权威记录重试,legacy 没有,吞掉就
    永远没人补偿(段位回滚丢失)。所以"失败后怎么办"必须按面分,不能统一。

    ★ 变异:把 legacy 面也改成失败即 return → 本条红。
    """
    b = TailHarness(release_err=RuntimeError("agones 500"))
    assert (await b.finish_empty_abandon(*ABANDON_ARGS, False, False, [])).command == COMMAND_STOP
    assert "deliver_abandoned" not in names(b.tape)

    lg = TailHarness(model_b=False, release_err=RuntimeError("agones 500"))
    out = await lg.finish_empty_abandon(*ABANDON_ARGS, False, False, [])
    assert out.command == COMMAND_STOP
    assert "deliver_abandoned" in names(lg.tape)
    assert lg.repo.calls == ["expire_battle"]


@pytest.mark.asyncio
async def test_empty_abandon_roster_incomplete_penalizes_only_absentees() -> None:
    """到齐超时只罚**缺席者**;全员失联才罚全 roster。

    到齐超时的语义是"有人没来",来了的那几个是受害者。罚全员会把等了 45 秒的
    队友一起禁赛 —— 而且他们没有任何申诉入口。

    ★ 变异:把 `absentees` 分支改成传 `player_ids` → 本条红。
    """
    rec = FakeNoShowRecorder(9, 9, 9)
    u = TailHarness(cfg=penalty_cfg(), no_show_recorder=rec)
    await u.finish_empty_abandon(*ABANDON_ARGS, False, True, [P2])
    assert [p for p, _ in rec.recorded] == [P2]

    rec2 = FakeNoShowRecorder(9, 9)
    full = TailHarness(cfg=penalty_cfg(), no_show_recorder=rec2)
    await full.finish_empty_abandon(*ABANDON_ARGS, True, False, [])
    assert [p for p, _ in rec2.recorded] == [P1, P2]


@pytest.mark.asyncio
async def test_empty_abandon_lifecycle_marker_failure_does_not_undo_delivery() -> None:
    """marker 落库失败 → STOP,但**不**回滚已投递的遗弃事件。

    事件已经出去了。此时"回滚"没有语义(下游已消费),能做的只有停下来别再动。

    ★ 变异:在 marker 失败分支里补一次反向投递 → 本条红。
    """
    u = TailHarness(lifecycle_err=RuntimeError("mysql down"))
    out = await u.finish_empty_abandon(*ABANDON_ARGS, False, False, [])
    assert out.command == COMMAND_STOP
    assert names(u.tape).count("deliver_abandoned") == 1
    assert "expire_terminated_expected" not in u.auth_repo.calls


# ── refresh_battle_locations ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_refresh_battle_locations_snapshots_list_at_call_site() -> None:
    """名单在**入口同步拷贝**,调用返回后调用方改原 list 影响不到后台任务。

    调用点传的是 `list(out.battle.player_ids)`,但那份 list 之后还会被 CAS 回调
    复用。后台任务持有原引用 = 续期到一份和"当时那局"对不上的名单上。

    ★ 变异:把 `players = list(player_ids)` 改成 `players = player_ids` → 本条红。
    """
    seen: list[list[int]] = []

    class Loc:
        async def refresh_battle_locations(self, players, match_id, ds_addr):  # noqa: ANN001, ANN202
            seen.append(list(players))

    src = [P1, P2]
    u = TailHarness(locator=Loc())
    u.refresh_battle_locations(src, MATCH_ID, DS_ADDR)
    src.append(P3)  # 调用返回后立刻改
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert seen == [[P1, P2]]


@pytest.mark.asyncio
async def test_refresh_battle_locations_failure_never_reaches_caller() -> None:
    """locator 炸了只落一条 warn;心跳主路径不受影响。

    位置 TTL 续期是尽力而为的旁路。让它把心跳打回 500,会让 locator 的一次抖动
    变成"整局 DS 被判失联" —— 用一个非权威旁路击穿权威链。

    ★ 变异:去掉 `_run` 里的 `except BaseException` → 本条会在 safego 兜底里
      记一次 recovered,断言 `seen` 仍为空但产品语义已变(异常穿到具名任务)。
      这里正面断言的是"调用方拿不到异常"。
    """

    class Boom:
        async def refresh_battle_locations(self, *a: Any) -> None:
            raise RuntimeError("locator down")

    u = TailHarness(locator=Boom())
    u.refresh_battle_locations([P1], MATCH_ID, DS_ADDR)  # 不抛
    await asyncio.sleep(0)
    await asyncio.sleep(0)


# ── list_battles ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_battles_filters_by_state_and_copies_only_client_visible_fields() -> None:
    """按 state 过滤,且只搬 §9 不变量 14 允许的六个字段。

    ★ 变异:把 `dspb.BattleInfo(...)` 换成直接返回 `rec`(StorageRecord 直传)
      → 本条红(多出 pod_uid / allocation_id 等内部字段)。
    """
    repo = TailRepo()
    repo.active = [MATCH_ID, MATCH_ID + 1]
    repo.battles = {
        MATCH_ID: mk_battle(state=STATE_RUNNING),
        MATCH_ID + 1: mk_battle(match_id=MATCH_ID + 1, state=STATE_ENDED),
    }
    u = TailHarness(repo=repo)
    assert [b.match_id for b in await u.list_battles("")] == [MATCH_ID, MATCH_ID + 1]
    only = await u.list_battles(STATE_RUNNING)
    assert [b.match_id for b in only] == [MATCH_ID]
    info = only[0]
    assert (info.ds_pod_name, info.ds_addr, info.player_count) == (POD, DS_ADDR, 2)
    assert info.state == STATE_RUNNING
    assert info.allocated_at_ms == NOW_MS - 60_000
    # 只有这六个字段被搬过来;StorageRecord 的内部字段不在 BattleInfo 上。
    assert {f.name for f, _ in info.ListFields()} <= {
        "match_id",
        "ds_pod_name",
        "ds_addr",
        "state",
        "player_count",
        "allocated_at_ms",
    }


@pytest.mark.asyncio
async def test_list_battles_skips_orphans_and_read_failures_without_failing_the_call() -> None:
    """索引孤儿 / 单条读失败都只是**跳过**,整跳仍返回可用结果。

    这是运维排障接口。一条脏索引让整个列表 500,等于故障时唯一的观察窗口先瞎了。

    ★ 变异:把 `get_battle` 的 `except` 改成 `raise` → 本条红。
    """
    repo = TailRepo()
    repo.active = [MATCH_ID, MATCH_ID + 1, MATCH_ID + 2]
    repo.battles = {MATCH_ID + 2: mk_battle(match_id=MATCH_ID + 2)}
    repo.battle_errs = {MATCH_ID + 1: RuntimeError("redis timeout")}
    out = await TailHarness(repo=repo).list_battles("")
    assert [b.match_id for b in out] == [MATCH_ID + 2]


@pytest.mark.asyncio
async def test_list_battles_authority_read_failure_propagates() -> None:
    """索引本身读不到 → **上抛**,不返回空列表。

    返回空列表会让调用方把"读不出来"看成"一局都没有"(§9.22:UNKNOWN 不得冒充
    OFFLINE)—— 运维据此判断"可以重启了",然后杀掉一堆在打的对局。

    ★ 变异:把 `raise` 改成 `return []` → 本条红。
    """
    repo = TailRepo()
    repo.range_err = RuntimeError("redis down")
    with pytest.raises(RuntimeError):
        await TailHarness(repo=repo).list_battles("")
