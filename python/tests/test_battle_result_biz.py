"""battle_result 业务层 —— 结算判定、出箱组装、发布器语义、保留期追平。

这些用例守的都是"错了不报错"的不变量:

  · ABANDONED 短路漏了 → 伪造 outcome 的 DS 能借补偿语义改段位;
  · canonical 覆盖漏了 → DS 伪报 game_mode 就能把不计分的局变成计分的;
  · Elo 用错舍入 → 同一场对局两栈算出差 1 分的 delta,两边都不报错;
  · 出箱发布器把"失败中断"改成"跳过继续" → 同玩家段位事件乱序;
  · 掉落发布器把"单行 continue"改成"整批中断" → 一个玩家背包满卡住全服掉落。
"""

from __future__ import annotations

import asyncio
import dataclasses

import pytest
from pandora.battle.v1 import battle_pb2
from pandora.config.v1 import level_pb2
from pandora.player.v1 import player_pb2
from structlog.testing import capture_logs

from pandorapy import dbguard, errcode
from pandorapy.services.battle_result import biz as bbiz
from pandorapy.services.battle_result import catalog as bcat
from pandorapy.services.battle_result import conf as bconf
from pandorapy.services.battle_result import repo as brepo
from pandorapy.services.battle_result import roster as brost


# ── 测试替身 ─────────────────────────────────────────────────────────────────


class FakeRepo:
    """只记录调用的仓储替身。真事务语义由 test_battle_result_repo.py 打真库验。"""

    def __init__(self, *, already: bool = False, settle: brepo.ProgressSettleInfo | None = None):
        self.saved: list[tuple] = []
        self.already = already
        self.settle = settle or brepo.ProgressSettleInfo()
        self.save_error: BaseException | None = None
        self.outbox_rows: list[brepo.OutboxRecord] = []
        self.deleted_outbox: list[int] = []
        self.delete_outbox_error: BaseException | None = None
        self.drop_rows: list[brepo.DropOutboxRecord] = []
        self.deleted_drop: list[int] = []
        self.match_release_rows: list[brepo.MatchReleaseRecord] = []
        self.deferred: list[tuple[int, int]] = []
        self.deleted_match_release: list[int] = []
        self.sweeps: list[tuple[str, dbguard.Mode, int, int]] = []
        self.battle_sweep_plan: list[brepo.SweepOutcome] = []
        self.stale = 0

    async def save_result(self, result, outbox, drop_outbox, terminal_release, final_seq):
        if self.save_error is not None:
            raise self.save_error
        self.saved.append((result, outbox, list(drop_outbox or []), terminal_release, final_seq))
        return self.already, self.settle

    async def fetch_outbox(self, limit):
        return self.outbox_rows[:limit]

    async def delete_outbox(self, outbox_id):
        if self.delete_outbox_error is not None:
            raise self.delete_outbox_error
        self.deleted_outbox.append(outbox_id)
        self.outbox_rows = [r for r in self.outbox_rows if r.id != outbox_id]

    async def fetch_drop_outbox(self, limit):
        return self.drop_rows[:limit]

    async def delete_drop_outbox(self, outbox_id):
        self.deleted_drop.append(outbox_id)
        self.drop_rows = [r for r in self.drop_rows if r.id != outbox_id]

    async def fetch_match_release_outbox(self, limit, now_ms):
        return self.match_release_rows[:limit]

    async def defer_match_release_outbox(self, outbox_id, next_ms):
        self.deferred.append((outbox_id, next_ms))

    async def delete_match_release_outbox(self, outbox_id):
        self.deleted_match_release.append(outbox_id)

    async def sweep_expired_battles(self, mode, cutoff_ms, batch):
        self.sweeps.append(("battles", mode, cutoff_ms, batch))
        if self.battle_sweep_plan:
            return self.battle_sweep_plan.pop(0)
        return brepo.SweepOutcome(mode=mode)

    async def sweep_settled_progress(self, mode, cutoff_ms, batch):
        self.sweeps.append(("progress", mode, cutoff_ms, batch))
        return brepo.SweepOutcome(mode=mode)

    async def count_stale_unsettled_progress(self, cutoff_ms):
        return self.stale


class FakeMMR:
    def __init__(self, by_player: dict[int, int] | None = None, error: BaseException | None = None):
        self.by_player = by_player or {}
        self.error = error
        self.calls: list[tuple[int, str]] = []

    async def get_mmr(self, player_id: int, rating_pool: str) -> int:
        self.calls.append((player_id, rating_pool))
        if self.error is not None:
            raise self.error
        return self.by_player.get(player_id, 1500)


class FakePusher:
    def __init__(self, fail_on: int | None = None):
        self.sent: list[int] = []
        self.fail_on = fail_on

    async def push_player_update(self, player_id: int, payload: bytes) -> None:
        if self.fail_on is not None and player_id == self.fail_on:
            raise errcode.PandoraError(errcode.ErrInternal, "kafka down")
        self.sent.append(player_id)


class FakeGranter:
    def __init__(self, *, instances_error: BaseException | None = None):
        self.items: list[tuple] = []
        self.instances: list[tuple] = []
        self.instances_error = instances_error

    async def grant_items(self, player_id, items, gold_amount, key):
        # ★ 位置参数照抄真实 granter 的形状:金币是 grant_items 的**第三个**入参,不是
        #   另一个方法 —— 用 *args 收会让"金币走了独立发放链"这类回归悄悄漏过。
        self.items.append(
            (player_id, [(i.item_config_id, i.count) for i in items], gold_amount, key)
        )

    async def grant_instances(self, player_id, ids, key):
        if self.instances_error is not None:
            raise self.instances_error
        self.instances.append((player_id, list(ids), key))


class FakeMail:
    def __init__(self):
        self.sent: list[tuple] = []

    async def send_overflow_mail(self, player_id, ids, key):
        self.sent.append((player_id, list(ids), key))


class FakeReleaser:
    def __init__(self, fail_on: int | None = None):
        self.released: list[int] = []
        self.fail_on = fail_on

    async def release_match(self, match_id, player_ids):
        if self.fail_on is not None and match_id == self.fail_on:
            raise errcode.PandoraError(errcode.ErrInternal, "matchmaker down")
        self.released.append(match_id)


class FakeCatalog:
    """按 id 给出 (equipment, droppable)。未登记的 id 一律 None(fail-closed)。"""

    def __init__(self, table: dict[int, tuple[bool, bool]]):
        self.table = table

    def lookup(self, item_config_id: int):
        got = self.table.get(item_config_id)
        if got is None:
            return None
        equipment, droppable = got
        return bcat.BattleItemDefinition(
            equipment=equipment, battle_usable=False, droppable=droppable, max_stack=1
        )


def _cfg(**kw) -> bconf.BattleConf:
    b = bconf.BattleConf(**kw)
    cfg = bconf.Config(battle=b)
    cfg.apply_defaults()
    return cfg.battle


def _result(match_id=1001, outcome=battle_pb2.BATTLE_OUTCOME_NORMAL, winner=0, players=((1, 0), (2, 1))):
    return battle_pb2.BattleResult(
        match_id=match_id,
        started_at_ms=1,
        ended_at_ms=2,
        winner_team=winner,
        outcome=outcome,
        ds_pod_name="battle-pod-1",
        game_mode="pvp",
        map_id=7,
        stats=[battle_pb2.PlayerStats(player_id=pid, team=team) for pid, team in players],
    )


def _uc(repo=None, mmr=None, pusher=None, releaser=None, cfg=None) -> bbiz.BattleResultUsecase:
    return bbiz.BattleResultUsecase(
        repo or FakeRepo(), mmr or FakeMMR(), pusher, releaser, cfg or _cfg()
    )


# ── Elo ──────────────────────────────────────────────────────────────────────


def test_elo_symmetric_when_equal_averages() -> None:
    """两队均分相等 → expected=0.5 → 胜 +K/2、负 -K/2。K=32 时是 ±16。"""
    assert bbiz.elo_deltas(1500, 1500, 32, bbiz.WINNER_TEAM_A) == (16, -16)
    assert bbiz.elo_deltas(1500, 1500, 32, bbiz.WINNER_TEAM_B) == (-16, 16)
    assert bbiz.elo_deltas(1500, 1500, 32, bbiz.WINNER_TEAM_DRAW) == (0, 0)


def test_elo_draw_is_default_branch() -> None:
    """winner_team 取到任何非 0/1 的值都按平局处理(Go 的 default 分支)。"""
    assert bbiz.elo_deltas(1500, 1500, 32, 99) == (0, 0)


def test_elo_favors_underdog() -> None:
    """低分队赢拿得多、高分队赢拿得少 —— 这是 Elo 的定义,反了就是把符号写错了。"""
    strong_wins, _ = bbiz.elo_deltas(1800, 1400, 32, bbiz.WINNER_TEAM_A)
    _, weak_wins = bbiz.elo_deltas(1800, 1400, 32, bbiz.WINNER_TEAM_B)
    assert 0 < strong_wins < 16
    assert weak_wins > 16


def test_round_half_away_from_zero_not_bankers() -> None:
    """★ Python 内建 round 是银行家舍入,Go 的 math.Round 不是。

    round(0.5)==0 / round(-0.5)==0(内建)vs 1 / -1(Go)。用错会让某些对局的
    delta 差 1 分,而两栈都不报错。
    """
    assert bbiz._round_half_away_from_zero(0.5) == 1  # noqa: SLF001
    assert bbiz._round_half_away_from_zero(-0.5) == -1  # noqa: SLF001
    assert bbiz._round_half_away_from_zero(1.5) == 2  # noqa: SLF001
    assert bbiz._round_half_away_from_zero(2.5) == 3  # noqa: SLF001 —— 内建会给 2


@pytest.mark.parametrize(
    ("team", "winner", "want"),
    [(0, 0, "win"), (1, 0, "lose"), (0, 1, "lose"), (1, 1, "win"), (0, 2, "draw"), (1, 9, "draw")],
)
def test_reason_for_team(team: int, winner: int, want: str) -> None:
    assert bbiz.reason_for_team(team, winner) == want


# ── 结算主路径 ───────────────────────────────────────────────────────────────


async def test_normal_settlement_writes_outbox_and_mmr() -> None:
    repo = FakeRepo()
    mmr = FakeMMR()
    uc = _uc(repo, mmr)
    already = await uc.report_result(_result(), 0)
    assert already is False
    result, outbox, drops, tr, seq = repo.saved[0]
    assert seq == 0 and tr is None and drops == []
    # MMR 覆盖 DS 上报值(不变量 §6):两队均分相等 → A +16 / B -16。
    assert [s.mmr_delta for s in result.stats] == [16, -16]
    assert [r.player_id for r in outbox] == [1, 2]
    evt = player_pb2.PlayerUpdateEvent()
    evt.ParseFromString(outbox[0].payload)
    assert evt.match_id == 1001
    assert evt.mmr_delta == 16
    assert evt.reason == "win"
    assert evt.rating_pool == "default"  # legacy 路径归一到默认池


async def test_missing_match_id_rejected_without_side_effects() -> None:
    repo = FakeRepo()
    uc = _uc(repo)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.report_result(battle_pb2.BattleResult(), 0)
    assert errcode.as_code(ei.value) == errcode.ErrInvalidArg
    assert repo.saved == []


async def test_empty_stats_rejected() -> None:
    repo = FakeRepo()
    uc = _uc(repo)
    bad = _result()
    del bad.stats[:]
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.report_result(bad, 0)
    assert errcode.as_code(ei.value) == errcode.ErrInvalidArg
    assert repo.saved == []


async def test_outcome_unspecified_defaults_to_normal() -> None:
    repo = FakeRepo()
    uc = _uc(repo)
    await uc.report_result(_result(outcome=battle_pb2.BATTLE_OUTCOME_UNSPECIFIED), 0)
    assert repo.saved[0][0].outcome == battle_pb2.BATTLE_OUTCOME_NORMAL


# ── ★ ABANDONED 短路(不变量 ③)────────────────────────────────────────────


async def test_abandoned_zeroes_mmr_and_never_touches_reader(monkeypatch) -> None:
    """outcome==ABANDONED → delta 全 0,且**根本不调** settlement_runs_elo / MMR reader。

    漏掉短路的后果:伪造 Outcome=ABANDONED 的 DS 能借补偿语义走进计分分支。
    这里用"调了就炸"来证明它真的没被调到 —— 只断言 delta==0 证明不了这一点
    (不计分的局 delta 也是 0)。
    """

    def _boom(*a, **kw):
        raise AssertionError("ABANDONED 不得进入计分判据")

    monkeypatch.setattr(brost, "settlement_runs_elo", _boom)
    repo = FakeRepo()
    mmr = FakeMMR()
    uc = _uc(repo, mmr)
    await uc.report_result(_result(outcome=battle_pb2.BATTLE_OUTCOME_ABANDONED), 0)
    assert mmr.calls == []
    assert [s.mmr_delta for s in repo.saved[0][0].stats] == [0, 0]


async def test_abandoned_produces_no_drop_outbox() -> None:
    """ABANDONED 是崩溃补偿,不产出掉落 —— 否则崩一局就白送一份掉落。"""
    repo = FakeRepo()
    uc = _uc(repo)
    uc.set_battle_item_catalog(FakeCatalog({10001: (True, True)}))
    res = _result(outcome=battle_pb2.BATTLE_OUTCOME_ABANDONED)
    res.stats[0].dropped_item_config_ids.append(10001)
    await uc.report_result(res, 0)
    assert repo.saved[0][2] == []


async def test_handle_abandoned_writes_zero_delta_outbox() -> None:
    repo = FakeRepo()
    uc = _uc(repo)
    await uc.handle_abandoned(2002, [7, 8], 5, "pve_coop", 0)
    result, outbox, drops, tr, seq = repo.saved[0]
    assert result.outcome == battle_pb2.BATTLE_OUTCOME_ABANDONED
    assert result.winner_team == bbiz.WINNER_TEAM_DRAW
    assert drops == [] and tr is None and seq == 0
    assert [s.mmr_delta for s in result.stats] == [0, 0]
    evt = player_pb2.PlayerUpdateEvent()
    evt.ParseFromString(outbox[0].payload)
    assert evt.reason == "abandon"
    assert evt.rating_pool == "default"


async def test_handle_abandoned_requires_match_id() -> None:
    repo = FakeRepo()
    uc = _uc(repo)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.handle_abandoned(0, [1], 0, "", 0)
    assert errcode.as_code(ei.value) == errcode.ErrInvalidArg
    assert repo.saved == []


async def test_handle_abandoned_ts_defaults_to_now() -> None:
    """ts_ms<=0 时补当前时间:留 0 会让 player.update 的 ts_ms 恒 0,消费侧排序全乱。"""
    repo = FakeRepo()
    uc = _uc(repo)
    await uc.handle_abandoned(2003, [7], 0, "", 0)
    assert repo.saved[0][0].ended_at_ms > 0


# ── 授权路径:canonical 覆盖 + 名单 + 计分判据 ───────────────────────────────


def _proof(**kw) -> brepo.TerminalReleaseRecord:
    base = dict(
        match_id=1001,
        allocation_id="alloc-1",
        ds_pod_name="battle-pod-1",
        gameserver_uid="uid-1",
        instance_epoch=1,
        auth_gen=1,
        auth_jti="jti-1",
        auth_exp_ms=9_999_999_999_999,
        auth_kid="kid-1",
        auth_token_sha256="a" * 64,
        auth_writer_epoch=bbiz.DS_AUTH_WRITER_EPOCH_V2,
        authorized_at_ms=1,
        player_ids=[1, 2],
        game_mode="pve_coop",
        map_id=42,
        rating_mode=level_pb2.LEVEL_RATING_MODE_NONE,
        rating_pool="  arena  ",
    )
    base.update(kw)
    return brepo.TerminalReleaseRecord(**base)


async def test_authorized_result_overrides_untrusted_fields(monkeypatch) -> None:
    """★ canonical game_mode/map_id 在**任何副作用之前**覆盖 DS 请求体(§9.6)。

    DS 报的是 game_mode="pvp"/map_id=7,权威快照是 "pve_coop"/42 —— 落库必须是后者。
    """
    repo = FakeRepo()
    mmr = FakeMMR()
    uc = _uc(repo, mmr)
    await uc.report_authorized_result(_result(), _proof(), 0)
    saved = repo.saved[0][0]
    assert saved.game_mode == "pve_coop"
    assert saved.map_id == 42


async def test_rating_mode_none_skips_elo_entirely() -> None:
    """rating_mode=NONE → delta 全 0 且**完全不触碰 MMR reader**。"""
    repo, mmr = FakeRepo(), FakeMMR()
    uc = _uc(repo, mmr)
    await uc.report_authorized_result(_result(), _proof(), 0)
    assert mmr.calls == []
    assert [s.mmr_delta for s in repo.saved[0][0].stats] == [0, 0]


async def test_rating_mode_elo_uses_normalized_pool() -> None:
    """rating_pool 必须归一(去空白)后既用于读 MMR、也写进出箱事件 —— 两侧必须同值。

    写 "arena" 读 "  arena  " 的话玩家的分记进了另一份段位,而两边都不报错。
    """
    repo, mmr = FakeRepo(), FakeMMR()
    uc = _uc(repo, mmr)
    await uc.report_authorized_result(
        _result(), _proof(rating_mode=level_pb2.LEVEL_RATING_MODE_ELO), 0
    )
    assert {pool for _, pool in mmr.calls} == {"arena"}
    evt = player_pb2.PlayerUpdateEvent()
    evt.ParseFromString(repo.saved[0][1][0].payload)
    assert evt.rating_pool == "arena"


async def test_legacy_fallback_basis_is_logged_as_warning() -> None:
    """★ rating_mode 未定格 → 回落旧口径 **且必须打 WARN**。

    段位改动不可逆:只返 bool 不打日志的话,「这一局到底按什么算的」事后没有任何痕迹。
    事件名与字段是 Loki 上筛这类局的唯一入口,字符串对不上查询就空了。
    """
    repo, mmr = FakeRepo(), FakeMMR()
    uc = _uc(repo, mmr)
    proof = _proof(
        rating_mode=level_pb2.LEVEL_RATING_MODE_UNSPECIFIED, game_mode="casual_5v5"
    )
    with capture_logs() as logs:
        await uc.report_authorized_result(_result(), proof, 0)
    warn = [e for e in logs if e["event"] == "battle_rating_basis_legacy_fallback"]
    assert len(warn) == 1
    assert warn[0]["log_level"] == "warning"
    assert warn[0]["basis"] == brost.BASIS_LEGACY_CANONICAL_GAME_MODE
    assert warn[0]["run_elo"] is True
    assert warn[0]["match_id"] == 1001
    # 未定格但 canonical game_mode 是 pve_coop → 不计分,basis 是另一个值。
    repo2, mmr2 = FakeRepo(), FakeMMR()
    uc2 = _uc(repo2, mmr2)
    with capture_logs() as logs2:
        await uc2.report_authorized_result(
            _result(), _proof(rating_mode=level_pb2.LEVEL_RATING_MODE_UNSPECIFIED), 0
        )
    warn2 = [e for e in logs2 if e["event"] == "battle_rating_basis_legacy_fallback"]
    assert warn2[0]["basis"] == brost.BASIS_LEGACY_CANONICAL_PVE_COOP
    assert warn2[0]["run_elo"] is False
    assert mmr2.calls == []


async def test_frozen_basis_is_debug_not_warning() -> None:
    """定格了 rating_mode 的局不打 WARN —— 否则告警面被正常局淹没,真回落的局反而看不见。"""
    uc = _uc(FakeRepo(), FakeMMR())
    with capture_logs() as logs:
        await uc.report_authorized_result(
            _result(), _proof(rating_mode=level_pb2.LEVEL_RATING_MODE_ELO), 0
        )
    assert not [e for e in logs if e["event"] == "battle_rating_basis_legacy_fallback"]
    assert [e for e in logs if e["event"] == "battle_rating_basis"]


async def test_legacy_no_canonical_still_runs_elo() -> None:
    """terminal_release=None(legacy kafka / 内部直调)保持历史行为:照算 Elo,basis 不告警。"""
    repo, mmr = FakeRepo(), FakeMMR()
    uc = _uc(repo, mmr)
    with capture_logs() as logs:
        await uc.report_result(_result(), 0)
    assert mmr.calls  # 真读了 MMR
    assert not [e for e in logs if e["event"] == "battle_rating_basis_legacy_fallback"]


async def test_roster_mismatch_rejects_whole_settlement() -> None:
    """名单对不上 → 整场拒绝:不落库、不发段位、不发掉落,并留 WARN 取证。"""
    repo = FakeRepo()
    uc = _uc(repo)
    with capture_logs() as logs:
        with pytest.raises(brost.RosterRejected) as ei:
            await uc.report_authorized_result(_result(), _proof(player_ids=[1, 999]), 0)
    assert errcode.as_code(ei.value) == errcode.ErrUnauthorized
    assert repo.saved == []
    rej = [e for e in logs if e["event"] == "battle_result_roster_rejected"]
    assert rej and rej[0]["reason"] == brost.REJECT_OUTSIDER
    assert rej[0]["sample_player_id"] == 2


async def test_authorized_abandoned_is_rejected() -> None:
    """走了完整鉴权却报 ABANDONED = 想借补偿语义绕过 MMR / 掉落规则,整场拒。"""
    repo = FakeRepo()
    uc = _uc(repo)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.report_authorized_result(
            _result(outcome=battle_pb2.BATTLE_OUTCOME_ABANDONED), _proof(), 0
        )
    assert errcode.as_code(ei.value) == errcode.ErrInvalidArg
    assert repo.saved == []


async def test_mmr_read_failure_falls_back_to_base_without_blocking() -> None:
    """player 不可用只回退 base_mmr,**不阻断落库** —— 整场结算失败的代价大得多。"""
    repo = FakeRepo()
    mmr = FakeMMR(error=RuntimeError("player down"))
    uc = _uc(repo, mmr)
    await uc.report_result(_result(), 0)
    assert repo.saved  # 照样落库
    assert [s.mmr_delta for s in repo.saved[0][0].stats] == [16, -16]


# ── 终态回收证明校验 ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        ({"match_id": 0}, "missing_match_id"),
        ({"match_id": 9}, "match_id_mismatch"),
        ({"allocation_id": ""}, "missing_allocation_id"),
        ({"ds_pod_name": ""}, "missing_pod"),
        ({"ds_pod_name": "other-pod"}, "pod_mismatch"),
        ({"gameserver_uid": ""}, "missing_gameserver_uid"),
        ({"instance_epoch": 0}, "missing_instance_epoch"),
        ({"auth_gen": 0}, "missing_auth_gen"),
        ({"auth_jti": ""}, "missing_jti"),
        ({"auth_exp_ms": 0}, "invalid_exp"),
        ({"auth_kid": ""}, "missing_kid"),
        ({"auth_token_sha256": ""}, "missing_token_sha"),
        ({"auth_writer_epoch": 1}, "writer_epoch_mismatch"),
        ({"authorized_at_ms": 0}, "invalid_authorized_at"),
        ({"authorized_at_ms": 9_999_999_999_999}, "authorized_at_not_before_exp"),
        ({"released_at_ms": 5}, "already_released"),
        ({"player_ids": []}, "empty_roster"),
    ],
)
def test_prepare_terminal_release_reasons(mutate: dict, reason: str) -> None:
    """★ 19 个子条件必须拆成 19 个 reason(§11.3 R2)。

    塌成一句话的话,pod / jti / exp / writer_epoch 哪一项对不上完全查不出来 ——
    而这些正是僵尸 DS / 伪造凭据的判别信号。顺序也是契约:同一份坏证明在两栈
    必须报同一个第一原因。
    """
    got_reason, exc = bbiz.prepare_terminal_release(_result(), _proof(**mutate), 15.0)
    assert got_reason == reason
    assert exc is not None
    assert errcode.as_code(exc) == errcode.ErrUnauthorized


@pytest.mark.parametrize("grace", [4.9, 121.0])
def test_prepare_terminal_release_grace_out_of_range(grace: float) -> None:
    """grace 越界是**纯配置错误**:每一场结算都会失败,必须能一眼认出来。"""
    reason, exc = bbiz.prepare_terminal_release(_result(), _proof(), grace)
    assert reason == "grace_out_of_range"
    assert errcode.as_code(exc) == errcode.ErrInvalidState


def test_prepare_terminal_release_ok_freezes_fields() -> None:
    rec = _proof()
    reason, exc = bbiz.prepare_terminal_release(_result(), rec, 15.0)
    assert reason == "" and exc is None
    assert rec.release_after_ms > 0
    # 只允许 phase1 worker 经 MySQL CAS 推进 / MySQL writer owns created_at_ms。
    assert rec.released_at_ms == 0
    assert rec.created_at_ms == 0


def test_prepare_terminal_release_future_authorization() -> None:
    rec = _proof(authorized_at_ms=9_999_999_999_998)
    reason, exc = bbiz.prepare_terminal_release(_result(), rec, 15.0)
    assert reason == "authorized_in_future"
    assert errcode.as_code(exc) == errcode.ErrUnauthorized


# ── 掉落出箱组装 ─────────────────────────────────────────────────────────────


def _drop_uc(catalog: dict[int, tuple[bool, bool]], **cfgkw) -> bbiz.BattleResultUsecase:
    uc = _uc(cfg=_cfg(**cfgkw))
    uc.set_battle_item_catalog(FakeCatalog(catalog))
    return uc


async def test_drop_outbox_routes_equipment_and_stack() -> None:
    """装备走 instance 路由、可堆叠走 stack 路由,两份 CSV **首次入箱即冻结**。"""
    repo = FakeRepo()
    uc = bbiz.BattleResultUsecase(repo, FakeMMR(), None, None, _cfg())
    uc.set_battle_item_catalog(FakeCatalog({10001: (True, True), 10002: (False, True)}))
    res = _result()
    res.stats[0].dropped_item_config_ids.extend([10001, 10002, 10002])
    await uc.report_result(res, 0)
    drops = repo.saved[0][2]
    assert len(drops) == 1
    assert drops[0].item_config_ids == [10001, 10002, 10002]
    assert drops[0].instance_item_config_ids == [10001]
    assert drops[0].stack_item_config_ids == [10002, 10002]


async def test_drop_outbox_filters_unknown_and_non_droppable() -> None:
    """未知 ID / 不可掉落 ID 一律 fail-closed 过滤,并留下 sample 供排障。"""
    repo = FakeRepo()
    uc = bbiz.BattleResultUsecase(repo, FakeMMR(), None, None, _cfg())
    uc.set_battle_item_catalog(FakeCatalog({10001: (True, False)}))  # 存在但不可掉落
    res = _result()
    res.stats[0].dropped_item_config_ids.extend([10001, 99999, 99999])
    with capture_logs() as logs:
        await uc.report_result(res, 0)
    assert repo.saved[0][2] == []
    filtered = [e for e in logs if e["event"] == "battle_drop_all_filtered"]
    assert filtered and filtered[0]["distinct_item_ids"] == 2
    assert filtered[0]["sample_item_config_id"] == 10001


async def test_drop_outbox_truncates_at_max_per_player() -> None:
    """★ 超限**截断并 Warn**,不是让整场结算失败 —— 异常 DS 不该打挂全场。"""
    repo = FakeRepo()
    uc = bbiz.BattleResultUsecase(repo, FakeMMR(), None, None, _cfg(max_drop_per_player=3))
    uc.set_battle_item_catalog(FakeCatalog({10001: (True, True)}))
    res = _result()
    res.stats[0].dropped_item_config_ids.extend([10001] * 10)
    with capture_logs() as logs:
        await uc.report_result(res, 0)
    assert len(repo.saved[0][2][0].item_config_ids) == 3
    trunc = [e for e in logs if e["event"] == "battle_drop_truncated"]
    assert trunc and trunc[0]["reported"] == 10 and trunc[0]["kept"] == 3


async def test_drop_outbox_falls_back_to_whitelist_without_catalog() -> None:
    """未注入 catalog 时回落旧 drop_whitelist(只声明装备)—— 兼容旧单测/旧部署。"""
    repo = FakeRepo()
    uc = bbiz.BattleResultUsecase(
        repo, FakeMMR(), None, None, _cfg(drop_whitelist=[10001])
    )
    res = _result()
    res.stats[0].dropped_item_config_ids.extend([10001, 10002])
    await uc.report_result(res, 0)
    drops = repo.saved[0][2]
    assert drops[0].item_config_ids == [10001]
    assert drops[0].instance_item_config_ids == [10001]


async def test_drops_suppressed_logs_audit_only(monkeypatch) -> None:
    """水位 >0 时结算掉落只作审计 —— 这条 INFO 是"为什么这局没走结算掉落"的唯一解释。"""
    repo = FakeRepo(settle=brepo.ProgressSettleInfo(stream_existed=True, last_applied_seq=9, drops_suppressed=True))
    uc = bbiz.BattleResultUsecase(repo, FakeMMR(), None, None, _cfg())
    uc.set_battle_item_catalog(FakeCatalog({10001: (True, True)}))
    res = _result()
    res.stats[0].dropped_item_config_ids.append(10001)
    with capture_logs() as logs:
        await uc.report_result(res, 9)
    assert [e for e in logs if e["event"] == "battle_drop_suppressed_by_progress"]


# ── 本局金币:钳位 → 出箱(结算侧;发放侧在「掉落发布器」一节)────────────────


async def test_reported_gold_is_clamped_in_place_before_persisting() -> None:
    """★ DS 上报的金币先钳上限,而且**钳完写回 result**。

    战绩表(battle_player_stats)与钱包发放读的是**同一份** stats:只在出箱侧钳的话,
    战报会写着"本局 999 亿金币"而钱包只加了 100 万,玩家会认为系统吞了收益。
    钳位只截断不拒整场 —— 拒了会连带段位 / 任务 / 掉落一起丢,代价远大于少发点钱。
    """
    repo = FakeRepo()
    uc = _uc(repo, cfg=_cfg(max_gold_per_player=100))
    res = _result()
    res.stats[0].gold = 10_000
    res.stats[1].gold = 40
    with capture_logs() as logs:
        await uc.report_result(res, 0)
    # 写回 result 本体 —— 落库的战绩与出箱读到同一个数。
    assert (res.stats[0].gold, res.stats[1].gold) == (100, 40)
    saved_result = repo.saved[0][0]
    assert saved_result.stats[0].gold == 100
    drops = {d.player_id: d.currency_amount for d in repo.saved[0][2]}
    assert drops == {1: 100, 2: 40}
    trunc = [e for e in logs if e["event"] == "battle_gold_truncated"]
    assert trunc and trunc[0]["reported"] == 10_000 and trunc[0]["kept"] == 100


async def test_gold_cap_default_is_used_when_unconfigured() -> None:
    """留空 = 100 万上限,不是"无上限" —— 任何构造路径都必须带闸(§9.6)。"""
    assert _cfg().max_battle_gold_per_player() == 1_000_000
    # ★ 负数不能当限额:Go 侧该字段无符号,负数表示不出来;Python 没有类型保护,
    #   把 -1 当上限会让每个玩家的金币都被钳成 -1 并写进战绩表与钱包。
    assert _cfg(max_gold_per_player=-1).max_battle_gold_per_player() == 1_000_000


async def test_gold_only_player_still_gets_an_outbox_row() -> None:
    """★ 没掉落但有金币 → 仍要出一条只带货币的出箱行。

    旧判据(`没有掉落就 continue`)会把这条整行丢掉 —— 而"这局没爆装备只赚了钱"
    恰恰是最常见的一局,表现是金币静默蒸发、零报错。
    """
    repo = FakeRepo()
    uc = _uc(repo)
    res = _result()
    res.stats[0].gold = 77
    await uc.report_result(res, 0)
    rows = repo.saved[0][2]
    assert [(r.player_id, r.item_config_ids, r.currency_amount) for r in rows] == [(1, [], 77)]


async def test_gold_survives_when_every_reported_drop_is_filtered() -> None:
    """掉落全被白名单过滤 ≠ 金币也该丢:两者是**独立**收益来源。"""
    repo = FakeRepo()
    uc = _uc(repo)
    uc.set_battle_item_catalog(FakeCatalog({}))
    res = _result()
    res.stats[0].dropped_item_config_ids.append(9999)
    res.stats[0].gold = 55
    with capture_logs() as logs:
        await uc.report_result(res, 0)
    assert [(r.player_id, r.currency_amount) for r in repo.saved[0][2]] == [(1, 55)]
    assert [e for e in logs if e["event"] == "battle_drop_all_filtered"]


async def test_zero_gold_player_produces_no_row() -> None:
    """没掉落也没金币的玩家不产出出箱行(否则每场结算凭空多出空行)。"""
    repo = FakeRepo()
    await _uc(repo).report_result(_result(), 0)
    assert repo.saved[0][2] == []


# ── 进度对账 ─────────────────────────────────────────────────────────────────


def test_reconcile_progress_no_stream_no_seq_is_silent() -> None:
    with capture_logs() as logs:
        bbiz.reconcile_progress(1, 0, brepo.ProgressSettleInfo())
    assert logs == []


def test_reconcile_progress_stream_missing_warns() -> None:
    """DS 声称走了实时通道但服务端无水位 = 全部批次丢失或伪造 final_seq。"""
    with capture_logs() as logs:
        bbiz.reconcile_progress(1, 5, brepo.ProgressSettleInfo())
    assert logs[0]["event"] == "progress_reconcile_stream_missing"
    assert logs[0]["log_level"] == "warning"


def test_reconcile_progress_gap_warns_but_does_not_repair() -> None:
    """尾窗丢失只告警**不自动补**:补等于凭空造出服务端根本没有的事实。"""
    info = brepo.ProgressSettleInfo(stream_existed=True, last_applied_seq=3)
    with capture_logs() as logs:
        bbiz.reconcile_progress(1, 5, info)
    assert logs[0]["event"] == "progress_reconcile_gap"
    assert logs[0]["applied_seq"] == 3


def test_reconcile_progress_ok() -> None:
    info = brepo.ProgressSettleInfo(stream_existed=True, last_applied_seq=5)
    with capture_logs() as logs:
        bbiz.reconcile_progress(1, 5, info)
    assert logs[0]["event"] == "progress_reconcile_ok"


# ── player.update 出箱发布器 ────────────────────────────────────────────────


async def test_outbox_publish_deletes_after_send() -> None:
    repo = FakeRepo()
    repo.outbox_rows = [
        brepo.OutboxRecord(id=1, player_id=11, payload=b"a"),
        brepo.OutboxRecord(id=2, player_id=12, payload=b"b"),
    ]
    pusher = FakePusher()
    uc = _uc(repo, pusher=pusher)
    assert await uc.publish_outbox_batch() == 2
    assert pusher.sent == [11, 12]
    assert repo.deleted_outbox == [1, 2]


async def test_outbox_publish_failure_interrupts_batch() -> None:
    """★ 段位事件按 player_id 保序:失败必须**中断本轮**,不能跳过继续。

    跳过继续会让同一玩家后续的 player.update 先于失败那条投出去 = 段位算错。
    """
    repo = FakeRepo()
    repo.outbox_rows = [
        brepo.OutboxRecord(id=1, player_id=11, payload=b"a"),
        brepo.OutboxRecord(id=2, player_id=12, payload=b"b"),
        brepo.OutboxRecord(id=3, player_id=13, payload=b"c"),
    ]
    pusher = FakePusher(fail_on=12)
    uc = _uc(repo, pusher=pusher)
    with pytest.raises(errcode.PandoraError):
        await uc.publish_outbox_batch()
    assert pusher.sent == [11]
    assert repo.deleted_outbox == [1]  # 失败行与其后的行都保留


async def test_outbox_publish_delete_failure_surfaces() -> None:
    """投出去但删不掉 → 抛错让本轮中断;下轮重投同一事件(下游幂等吸收)。"""
    repo = FakeRepo()
    repo.outbox_rows = [brepo.OutboxRecord(id=1, player_id=11, payload=b"a")]
    repo.delete_outbox_error = errcode.PandoraError(errcode.ErrInternal, "db down")
    uc = _uc(repo, pusher=FakePusher())
    with pytest.raises(errcode.PandoraError):
        await uc.publish_outbox_batch()


async def test_outbox_without_pusher_warns_when_backlog_exists() -> None:
    """kafka 未配但出箱有积压 → 必须有信号。

    没有它的话「打完段位没变」会被当成结算 bug 排查,而真因是部署缺 kafka。
    """
    repo = FakeRepo()
    repo.outbox_rows = [brepo.OutboxRecord(id=1, player_id=11, payload=b"a")]
    uc = _uc(repo, pusher=None)
    # 进程级限流窗口是模块单例,先清掉上一个用例可能留下的状态。
    bbiz._outbox_no_pusher_log = type(bbiz._outbox_no_pusher_log)()  # noqa: SLF001
    with capture_logs() as logs:
        assert await uc.publish_outbox_batch() == 0
    assert [e for e in logs if e["event"] == "outbox_pending_without_pusher"]


async def test_outbox_without_pusher_silent_when_empty() -> None:
    """出箱空时不该告警 —— 没配 kafka 且没积压是合法的 dev 形态。"""
    uc = _uc(FakeRepo(), pusher=None)
    bbiz._outbox_no_pusher_log = type(bbiz._outbox_no_pusher_log)()  # noqa: SLF001
    with capture_logs() as logs:
        assert await uc.publish_outbox_batch() == 0
    assert [e for e in logs if e["event"] == "outbox_pending_without_pusher"] == []


# ── 掉落发布器 ───────────────────────────────────────────────────────────────


def _drop_row(**kw) -> brepo.DropOutboxRecord:
    base = dict(
        id=1, match_id=1001, player_id=11, item_config_ids=[10001],
        stack_item_config_ids=[], instance_item_config_ids=[10001],
    )
    base.update(kw)
    return brepo.DropOutboxRecord(**base)


async def test_drop_publish_uses_frozen_route_and_key() -> None:
    repo = FakeRepo()
    repo.drop_rows = [_drop_row()]
    granter = FakeGranter()
    uc = _uc(repo)
    uc.set_instance_granter(granter)
    assert await uc.publish_drop_batch() == 1
    assert granter.instances == [(11, [10001], "battle_drop:1001:11")]
    assert repo.deleted_drop == [1]


async def test_drop_publish_splits_key_when_both_routes_present() -> None:
    """两路都有时必须用**不同子键**:同键会让 inventory 把第二次调用当成重放吞掉。"""
    repo = FakeRepo()
    repo.drop_rows = [
        _drop_row(
            item_config_ids=[10001, 10002, 10002],
            stack_item_config_ids=[10002, 10002],
            instance_item_config_ids=[10001],
        )
    ]
    granter = FakeGranter()
    uc = _uc(repo)
    uc.set_instance_granter(granter)
    await uc.publish_drop_batch()
    assert granter.items == [(11, [(10002, 2)], 0, "battle_drop:1001:11:stack")]
    assert granter.instances == [(11, [10001], "battle_drop:1001:11:instance")]


async def test_drop_publish_single_row_failure_does_not_block_others() -> None:
    """★ 掉落无跨玩家保序需求:单行失败只 continue。

    改成整批中断会让某玩家背包满阻塞全服其他玩家的掉落。
    """
    repo = FakeRepo()
    repo.drop_rows = [_drop_row(id=1, player_id=11), _drop_row(id=2, player_id=12)]
    granter = FakeGranter(instances_error=errcode.PandoraError(errcode.ErrInternal, "inv down"))
    uc = _uc(repo)
    uc.set_instance_granter(granter)
    assert await uc.publish_drop_batch() == 0
    assert repo.deleted_drop == []  # 两行都保留下轮重试
    # 两行都被尝试过(没有在第一行就中断)。
    assert len(repo.drop_rows) == 2


async def test_drop_publish_overflow_goes_to_mail_with_same_key() -> None:
    """背包满 → 转邮件,且用**同一把幂等键**(直发链与邮件领取链共享 → 至多一次)。"""
    repo = FakeRepo()
    repo.drop_rows = [_drop_row()]
    granter = FakeGranter(
        instances_error=errcode.PandoraError(errcode.ErrInventoryCapacityFull, "full")
    )
    mail = FakeMail()
    uc = _uc(repo)
    uc.set_instance_granter(granter)
    uc.set_mail_sender(mail)
    assert await uc.publish_drop_batch() == 1
    assert mail.sent == [(11, [10001], "battle_drop:1001:11")]
    assert repo.deleted_drop == [1]


async def test_drop_publish_non_capacity_error_never_mails() -> None:
    """★ 只有**背包满**才允许转邮件。任何错误都转的话,inventory 抖一下就白送装备。"""
    repo = FakeRepo()
    repo.drop_rows = [_drop_row()]
    granter = FakeGranter(instances_error=errcode.PandoraError(errcode.ErrInternal, "down"))
    mail = FakeMail()
    uc = _uc(repo)
    uc.set_instance_granter(granter)
    uc.set_mail_sender(mail)
    assert await uc.publish_drop_batch() == 0
    assert mail.sent == []
    assert repo.deleted_drop == []


async def test_drop_publish_rejects_row_without_frozen_route() -> None:
    """两份路由 CSV 都空 = 首次入箱时的冻结丢了,保留行并告警,绝不猜。"""
    repo = FakeRepo()
    repo.drop_rows = [_drop_row(stack_item_config_ids=[], instance_item_config_ids=[])]
    uc = _uc(repo)
    uc.set_instance_granter(FakeGranter())
    assert await uc.publish_drop_batch() == 0
    assert repo.deleted_drop == []


async def test_drop_publisher_disabled_without_granter() -> None:
    uc = _uc(FakeRepo())
    assert await uc.publish_drop_batch() == 0


def test_aggregate_stack_grants_sorted_and_counted() -> None:
    grants = bbiz.aggregate_stack_grants([10002, 10001, 10002])
    assert [(g.item_config_id, g.count) for g in grants] == [(10001, 1), (10002, 2)]


def test_aggregate_stack_grants_rejects_zero_and_empty() -> None:
    with pytest.raises(errcode.PandoraError):
        bbiz.aggregate_stack_grants([])
    with pytest.raises(errcode.PandoraError):
        bbiz.aggregate_stack_grants([0])


async def test_gold_and_stack_items_share_one_grant_and_one_key() -> None:
    """★ 金币与可堆叠道具合并成**一次** grant_items:一个幂等键、一个 inventory 事务。

    拆成两次调用就会出现"道具到了钱没到"(或反过来),而两次调用各自重试还会
    让重试窗口不一致。纯金币行也走同一条路径。
    """
    repo = FakeRepo()
    repo.drop_rows = [
        _drop_row(
            item_config_ids=[10002], stack_item_config_ids=[10002],
            instance_item_config_ids=[], currency_amount=88,
        )
    ]
    granter = FakeGranter()
    uc = _uc(repo)
    uc.set_instance_granter(granter)
    assert await uc.publish_drop_batch() == 1
    assert granter.items == [(11, [(10002, 1)], 88, "battle_drop:1001:11")]
    assert granter.instances == []


async def test_gold_only_row_is_delivered_not_rejected_as_routeless() -> None:
    """纯金币行(两份路由都空)不是"无冻结路由",必须照发。"""
    repo = FakeRepo()
    repo.drop_rows = [
        _drop_row(item_config_ids=[], instance_item_config_ids=[], currency_amount=42)
    ]
    granter = FakeGranter()
    uc = _uc(repo)
    uc.set_instance_granter(granter)
    assert await uc.publish_drop_batch() == 1
    assert granter.items == [(11, [], 42, "battle_drop:1001:11")]


async def test_gold_plus_equipment_uses_split_keys() -> None:
    """★ 金币 + 装备(可堆叠为空)也必须分叉幂等键。

    分叉判据是"这一行会不会真的发两次调用",不是"stacks 和 instances 都非空" ——
    按后者判,这一行的 GrantItems 与 GrantInstances 会拿到同一把键,inventory 端把
    第二次当重放吞掉,**装备静默不发**。
    """
    repo = FakeRepo()
    repo.drop_rows = [
        _drop_row(
            item_config_ids=[10001], stack_item_config_ids=[],
            instance_item_config_ids=[10001], currency_amount=15,
        )
    ]
    granter = FakeGranter()
    uc = _uc(repo)
    uc.set_instance_granter(granter)
    assert await uc.publish_drop_batch() == 1
    assert granter.items == [(11, [], 15, "battle_drop:1001:11:stack")]
    assert granter.instances == [(11, [10001], "battle_drop:1001:11:instance")]


async def test_empty_row_without_gold_is_still_rejected() -> None:
    """三样全空的行仍是脏数据,必须 ErrInvalidState —— 别为了让金币过而放软这条。"""
    repo = FakeRepo()
    repo.drop_rows = [_drop_row(item_config_ids=[], instance_item_config_ids=[])]
    granter = FakeGranter()
    uc = _uc(repo)
    uc.set_instance_granter(granter)
    with capture_logs() as logs:
        assert await uc.publish_drop_batch() == 0
    assert [e for e in logs if e["event"] == "drop_grant_failed"]
    assert repo.deleted_drop == []


# ── 实时进度通道:发放形状 ───────────────────────────────────────────────────


async def test_progress_stack_grant_passes_zero_gold() -> None:
    """★ 实时进度通道**只发道具不发金币**,金币位必须固定传 0。

    金币是结算路径(_build_drop_outbox)的收益;这里若跟着传非 0,同一局的金币会被
    实时通道与结算路径**各发一次**。这条同时钉住 grant_items 的入参形状:金币是它的
    第三个位置参数,改签名时这条会红,不会静默把参数错位成幂等键。
    """
    from pandorapy.services.battle_result import progress_repo as bprepo

    class _Repo(FakeRepo):
        def __init__(self):
            super().__init__()
            self.deleted_progress: list[int] = []

        async def delete_progress_outbox(self, outbox_id):
            self.deleted_progress.append(outbox_id)

    repo = _Repo()
    granter = FakeGranter()
    uc = _uc(repo)
    uc.set_instance_granter(granter)
    rec = bprepo.ProgressOutboxRecord(
        id=7, match_id=1001, seq=3, player_id=11,
        kind=int(bprepo.ProgressGrantKind.STACK), item_config_ids=[10002, 10002],
    )
    assert await uc.process_progress_record(rec) is None
    assert granter.items == [(11, [(10002, 2)], 0, "progress:1001:3:11:stack")]
    assert repo.deleted_progress == [7]


# ── 撮合状态释放发布器 ───────────────────────────────────────────────────────


def _release_row(**kw) -> brepo.MatchReleaseRecord:
    base = dict(id=1, operation_id="op-1", match_id=1001, player_ids=[1, 2], attempt_count=0)
    base.update(kw)
    return brepo.MatchReleaseRecord(**base)


async def test_match_release_deletes_only_on_success() -> None:
    repo = FakeRepo()
    repo.match_release_rows = [_release_row()]
    releaser = FakeReleaser()
    uc = _uc(repo, releaser=releaser)
    assert await uc.publish_match_release_batch() == 1
    assert releaser.released == [1001]
    assert repo.deleted_match_release == [1]
    assert repo.deferred == []


async def test_match_release_failure_defers_not_deletes() -> None:
    """★ **明确成功才删行**。未知即删的后果:残留 claim 永不释放,玩家回 Hub 恒撞 4002。"""
    repo = FakeRepo()
    repo.match_release_rows = [_release_row(attempt_count=2)]
    uc = _uc(repo, releaser=FakeReleaser(fail_on=1001))
    with pytest.raises(errcode.PandoraError):
        await uc.publish_match_release_batch()
    assert repo.deleted_match_release == []
    assert len(repo.deferred) == 1
    assert repo.deferred[0][0] == 1


@pytest.mark.parametrize(
    ("attempt", "want"), [(0, 1.0), (1, 2.0), (3, 8.0), (6, 60.0), (10, 60.0)]
)
def test_match_release_retry_delay(attempt: int, want: float) -> None:
    """指数退避封顶 60s(Go: 1s·2^min(n,6),再 cap 1min)。"""
    assert bbiz.match_release_retry_delay(attempt) == want


async def test_match_release_disabled_without_releaser() -> None:
    assert await _uc(FakeRepo()).publish_match_release_batch() == 0


# ── 保留期清理 ───────────────────────────────────────────────────────────────


async def test_retention_drains_until_short_batch() -> None:
    """★ 小批量**循环删到追平**。只删单批的话 200 场/小时追不平生产流入,积压只增不减。"""
    repo = FakeRepo()
    repo.battle_sweep_plan = [
        brepo.SweepOutcome(mode=dbguard.Mode.DELETE, matched=200, deleted=200, truncated=True),
        brepo.SweepOutcome(mode=dbguard.Mode.DELETE, matched=200, deleted=200, truncated=True),
        brepo.SweepOutcome(mode=dbguard.Mode.DELETE, matched=7, deleted=7, truncated=False),
    ]
    uc = _uc(repo)
    with capture_logs() as logs:
        await uc.sweep_retention_once()
    purged = [e for e in logs if e["event"] == "battle_retention_battles_purged"]
    assert purged and purged[0]["matches"] == 407
    assert purged[0]["retention_days"] == 180


async def test_retention_report_only_runs_single_round() -> None:
    """report_only 下只跑一轮:那一轮已经数出全量规模(不受 batch 截断),再循环是空转。"""
    repo = FakeRepo()
    uc = _uc(repo, cfg=_cfg(retention_mode="report_only"))
    await uc.sweep_retention_once()
    battle_sweeps = [s for s in repo.sweeps if s[0] == "battles"]
    assert len(battle_sweeps) == 1
    assert battle_sweeps[0][1] is dbguard.Mode.REPORT_ONLY


async def test_retention_purge_failure_stops_that_kind_only() -> None:
    """一类失败只记日志继续下一类:清理彼此独立、幂等,下一轮自然重试。"""

    class BoomRepo(FakeRepo):
        async def sweep_expired_battles(self, mode, cutoff_ms, batch):
            raise RuntimeError("db down")

    repo = BoomRepo()
    uc = _uc(repo)
    with capture_logs() as logs:
        await uc.sweep_retention_once()
    assert [e for e in logs if e["event"] == "battle_retention_purge_failed"]
    assert [s for s in repo.sweeps if s[0] == "progress"]  # 第二类照跑


async def test_stale_unsettled_progress_alerts_but_never_purges() -> None:
    """陈年未结算水位 = 补偿链 bug 证据:永不自动清理,但必须**持续告警**。

    不告警的话"保留证据待排查"会退化成静默永久保留,§9.24 的有界承诺落空。
    """
    repo = FakeRepo()
    repo.stale = 3
    uc = _uc(repo)
    with capture_logs() as logs:
        await uc.sweep_retention_once()
    alert = [e for e in logs if e["event"] == "battle_retention_stale_unsettled_progress"]
    assert alert and alert[0]["count"] == 3
    assert alert[0]["log_level"] == "error"


async def test_retention_cutoff_uses_configured_days() -> None:
    """cutoff 必须由 history_retention_days 推 —— 写死天数会让配置形同虚设。"""
    repo = FakeRepo()
    uc = _uc(repo, cfg=_cfg(history_retention_days=30))
    await uc.sweep_retention_once()
    import time as _time

    cutoff = repo.sweeps[0][2]
    expect = int((_time.time() - 30 * 86400) * 1000)
    assert abs(cutoff - expect) < 5000


# ── 跨 region 结算口径(纯函数)──────────────────────────────────────────────


def test_settlement_key_canonical_form() -> None:
    """与 player 的 mmr_history 唯一键 (player_id, match_id) 同维度;口径漂移会让重投去重失效。"""
    assert bbiz.settlement_key(1001, 42) == "1001:42"


def test_distinct_regions_sorted_and_deduped() -> None:
    owners = [
        bbiz.SettlementOwner(1, 3, 1),
        bbiz.SettlementOwner(2, 1, 2),
        bbiz.SettlementOwner(3, 3, 4),
    ]
    assert bbiz.distinct_settlement_regions(owners) == [1, 3]
    assert bbiz.cross_region_settlement(owners) is True
    assert bbiz.distinct_settlement_regions([]) == []
    assert bbiz.cross_region_settlement([bbiz.SettlementOwner(1, 2, 3)]) is False


# ── 取消穿透 ─────────────────────────────────────────────────────────────────


async def test_cancellation_propagates_through_settlement() -> None:
    """★ 取消必须穿透到调用方,不能被"映射成业务码"吞掉(§9.16 排空在途)。"""
    repo = FakeRepo()
    repo.save_error = asyncio.CancelledError()
    uc = _uc(repo)
    with pytest.raises(asyncio.CancelledError):
        await uc.report_result(_result(), 0)


async def test_cancellation_propagates_through_mmr_read() -> None:
    uc = _uc(FakeRepo(), FakeMMR(error=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        await uc.report_result(_result(), 0)


# ── 查询 ─────────────────────────────────────────────────────────────────────


async def test_get_match_result_requires_match_id() -> None:
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(FakeRepo()).get_match_result(0)
    assert errcode.as_code(ei.value) == errcode.ErrInvalidArg


async def test_list_player_history_requires_player_id() -> None:
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(FakeRepo()).list_player_history(0, 10, 0)
    assert errcode.as_code(ei.value) == errcode.ErrInvalidArg


# ── 幂等命中 ─────────────────────────────────────────────────────────────────


async def test_idempotent_hit_returns_true_and_logs_pod_pair() -> None:
    """幂等命中良性,但「同 pod 重试」与「换 pod 的僵尸 DS 重放」必须能区分(后者是 fencing 事件)。"""
    repo = FakeRepo(already=True)
    uc = _uc(repo, FakeMMR())
    with capture_logs() as logs:
        assert await uc.report_authorized_result(_result(), _proof(), 0) is True
    hit = [e for e in logs if e["event"] == "battle_result_idempotent_hit"]
    assert hit
    assert hit[0]["ds_pod_name"] == "battle-pod-1"
    assert hit[0]["credential_pod"] == "battle-pod-1"


def test_settlement_owner_is_frozen() -> None:
    """落点结构不可变:可变的话调用方一改就把观测口径改了,而它是纯观测信号。"""
    o = bbiz.SettlementOwner(1, 2, 3)
    with pytest.raises(dataclasses.FrozenInstanceError):
        o.region_id = 9  # type: ignore[misc]
