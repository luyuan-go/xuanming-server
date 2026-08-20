"""战绩名单校验与计分判据测试。

重点:
  1. ★ 集合比对 —— 顺序不是权威信号,但重复/遗漏/外人全拒
  2. ★ 每个拒绝分支一个独立 reason(一个 if 收敛 8 个条件 = 线上查不出真因)
  3. ★ 计分判据必须与 Go settlementRunsElo **逐分支同向**(方向反了 = 排位局白打)
"""

from __future__ import annotations

import pytest

from pandorapy import errcode
from pandorapy.services.battle_result import roster


AUTH = [1001, 1002, 1003]


def _check(reported, authoritative=None):
    return roster.validate_authorized_roster(
        reported, AUTH if authoritative is None else authoritative
    )


# ── ★ 集合比对 ─────────────────────────────────────────────────────────────


def test_same_set_different_order_passes() -> None:
    """★ 顺序不是权威信号 —— DS 上报的 stat 顺序无意义,不能据此拒绝。

    按顺序比会让正常战绩偶发被拒(整场不落库、不发段位、不发掉落)。
    """
    assert _check([1003, 1001, 1002]) == ("", 0)
    assert _check([1001, 1002, 1003]) == ("", 0)


def test_exact_match_passes() -> None:
    assert _check(list(AUTH)) == ("", 0)


def test_missing_player_rejected() -> None:
    """遗漏 → 数量对不上。"""
    with pytest.raises(roster.RosterRejected) as exc:
        _check([1001, 1002])
    assert exc.value.reason == roster.REJECT_COUNT_MISMATCH
    assert exc.value.code == errcode.ErrUnauthorized


def test_extra_player_rejected() -> None:
    with pytest.raises(roster.RosterRejected) as exc:
        _check([1001, 1002, 1003, 1004])
    assert exc.value.reason == roster.REJECT_COUNT_MISMATCH


def test_outsider_rejected_with_sample_id() -> None:
    """★ 外人 —— 不在权威名单里的玩家想拿段位 / 掉落。

    sample_player_id 必须带上,否则线上只知道"混进了外人",不知道是谁。
    """
    with pytest.raises(roster.RosterRejected) as exc:
        _check([1001, 1002, 9999])  # 数量对,但 9999 不在名单
    assert exc.value.reason == roster.REJECT_OUTSIDER
    assert exc.value.sample_player_id == 9999


def test_duplicate_reported_rejected() -> None:
    """★ 重复 stat —— 否则同一玩家会拿两份结算。"""
    with pytest.raises(roster.RosterRejected) as exc:
        _check([1001, 1001, 1002])
    assert exc.value.reason == roster.REJECT_REPORTED_DUP
    assert exc.value.sample_player_id == 1001


def test_zero_player_id_rejected() -> None:
    with pytest.raises(roster.RosterRejected) as exc:
        _check([1001, 1002, 0])
    assert exc.value.reason == roster.REJECT_REPORTED_ZERO_ID


# ── ★ 权威名单自身的合法性(先判)────────────────────────────────────────────


def test_empty_authority_rejected() -> None:
    """权威名单为空 → 拒。空名单去比对任何东西都没意义。"""
    with pytest.raises(roster.RosterRejected) as exc:
        _check([1001], authoritative=[])
    assert exc.value.reason == roster.REJECT_AUTHORITY_EMPTY


def test_authority_with_zero_id_rejected() -> None:
    """★ 权威名单自己脏 → 拒,而且 reason 要能区分是**权威**脏还是上报脏。

    两者的处置完全不同:前者是上游 bug,后者可能是 DS 被改。
    """
    with pytest.raises(roster.RosterRejected) as exc:
        _check([1001, 1002], authoritative=[1001, 0])
    assert exc.value.reason == roster.REJECT_AUTHORITY_ZERO_ID


def test_authority_with_duplicate_rejected() -> None:
    with pytest.raises(roster.RosterRejected) as exc:
        _check([1001, 1002], authoritative=[1001, 1001])
    assert exc.value.reason == roster.REJECT_AUTHORITY_DUP
    assert exc.value.sample_player_id == 1001


def test_nil_result_rejected() -> None:
    with pytest.raises(roster.RosterRejected) as exc:
        _check(None)
    assert exc.value.reason == roster.REJECT_NIL_RESULT


def test_all_reasons_are_distinct() -> None:
    """★ 8 个拒绝分支必须是 8 个不同的 reason(§11.3 R2)。

    合并成一个的话线上只看到"名单不匹配",而"数量对不上 / 有重复 / 混进外人"
    的处置完全不同。
    """
    reasons = {
        roster.REJECT_NIL_RESULT,
        roster.REJECT_AUTHORITY_EMPTY,
        roster.REJECT_COUNT_MISMATCH,
        roster.REJECT_AUTHORITY_ZERO_ID,
        roster.REJECT_AUTHORITY_DUP,
        roster.REJECT_REPORTED_ZERO_ID,
        roster.REJECT_REPORTED_DUP,
        roster.REJECT_OUTSIDER,
    }
    assert len(reasons) == 8


# ── ★ 计分判据:必须与 Go settlementRunsElo 逐分支同向 ─────────────────────


def test_explicit_rating_mode_decides() -> None:
    """★ 分支①:显式定格的 rating_mode 是权威,压过一切旧口径。"""
    assert roster.should_apply_rating(roster.RATING_MODE_ELO)
    assert not roster.should_apply_rating(roster.RATING_MODE_NONE)


def test_unspecified_rating_mode_falls_back_to_legacy_and_still_rates() -> None:
    """★ 这一条是两栈最容易分叉的地方,方向反了不会有任何报错。

    rating_mode 未定格 = 滚动升级期的旧 matchmaker / 旧批次表。Go
    (battle_result.go:359-364)在这里**保守回落旧口径并照算 Elo**:
    这些对局在 rating_mode 列上线前本来就在算分,缺一个字段就跳过计分,
    受害的是正在打排位的玩家 —— 一整局白打,而且没有任何日志说"这局没算分"。

    混跑期两栈算出不同段位是最坏的失败方向(段位改动不可逆),
    所以这里断言的是 Go 的真值 True,而不是"看起来更安全"的 False。
    """
    assert roster.should_apply_rating(roster.RATING_MODE_UNSPECIFIED)
    assert roster.should_apply_rating(roster.RATING_MODE_UNSPECIFIED, "")


def test_no_canonical_snapshot_still_rates() -> None:
    """★ 分支③:legacy kafka / 内部直调压根没有 canonical 快照。

    Go 在 terminalRelease == nil 时直接 return true("legacy_no_canonical",
    battle_result.go:348-352)。

    ⚠️ 本条测的是**便捷入口**的行为:它只有 (rating_mode, game_mode) 两个参数,
    两个都传空值时走的其实是"有快照但未定格"那条兜底,结论同样是"照算"。
    真正对应 Go 那个 nil 分支的是 `settlement_runs_elo(None, ...)`,basis 也不同
    (`legacy_no_canonical` vs `legacy_canonical_game_mode`)——
    两者的区分见 test_no_canonical_is_distinct_from_unspecified_rating_mode。
    """
    assert roster.should_apply_rating(0, "")
    assert roster.settlement_runs_elo(None, "")[0]


def test_legacy_pve_coop_is_the_only_exclusion() -> None:
    """旧局兜底里唯一不计分的取值就是 pve_coop,别的池名一律照算。

    这条路径随旧局一起退役,但不能提前改向:在途旧局还要按上线前的口径结算。
    """
    assert not roster.should_apply_rating(
        roster.RATING_MODE_UNSPECIFIED, roster.CANONICAL_GAME_MODE_PVE_COOP
    )
    assert roster.should_apply_rating(roster.RATING_MODE_UNSPECIFIED, "ranked_5v5")
    assert roster.should_apply_rating(roster.RATING_MODE_UNSPECIFIED, "casual_5v5")


def test_rating_mode_wins_over_legacy_game_mode() -> None:
    """权威判据存在时,旧的 game_mode 兜底不参与决策。"""
    # rating_mode 说不计,即使 game_mode 看起来像排位
    assert not roster.should_apply_rating(roster.RATING_MODE_NONE, "ranked_5v5")
    # rating_mode 说计,即使 game_mode 是 pve_coop
    assert roster.should_apply_rating(
        roster.RATING_MODE_ELO, roster.CANONICAL_GAME_MODE_PVE_COOP
    )


def test_new_pool_must_be_stopped_by_rating_mode_not_by_pool_name() -> None:
    """★ 「新池静默按排位改段位」由**定格 rating_mode** 拦住,不是靠池名黑名单。

    这是判据从池名迁到关卡表列的全部理由:池名会不断新增,黑名单永远追不上;
    而新关卡只要把 rating_mode 定成 NONE 就绝不计分。
    """
    assert not roster.should_apply_rating(roster.RATING_MODE_NONE, "casual_5v5")


# ── ★ 与 Go 逐字一致(判据取自 Go 源码,不是抄一份到测试里)────────────────


def test_reject_reasons_match_go_source(repo_root) -> None:
    """★ 8 个 reason 必须与 Go 的常量**逐字一致**,一个不多一个不少。

    模块头声称"与 Go 逐字一致"却零断言 —— 那只是一句注释。
    reason 是**运维 grep 的判据**:线上按 `reason="count_mismatch"` 查战绩被拒的原因,
    名字漂移会让同一条查询在 Go 副本有结果、Python 副本恒空,
    而"没有结果"最容易被读成"没发生过"。

    这里还断言**集合相等**而不是包含:Python 多出一个 Go 没有的 reason 同样是漂移
    (运维手册里不会有它),少一个则说明有条拒绝路径没被拆开。
    """
    import re

    src = (
        repo_root / "services" / "battle" / "battle_result" / "internal" / "biz"
        / "battle_result.go"
    ).read_text(encoding="utf-8")
    block = re.search(r"rosterRejectNilResult.*?\n\)", src, re.S)
    assert block, "没在 Go 源码里找到 roster reject 常量块"
    go_reasons = set(re.findall(r'=\s*"([a-z_]+)"', block.group(0)))
    assert go_reasons, "常量块解析为空"

    py_reasons = {
        v
        for k, v in vars(roster).items()
        if k.startswith("REJECT_") and isinstance(v, str)
    }
    assert py_reasons == go_reasons, (
        f"reason 集合与 Go 不一致\n只在 Python:{py_reasons - go_reasons}"
        f"\n只在 Go:{go_reasons - py_reasons}"
    )


def test_reject_messages_match_go_source(repo_root) -> None:
    """★ 5 条错误文案同样必须逐字一致 —— 它们会原样出现在客户端与日志里。"""
    src = (
        repo_root / "services" / "battle" / "battle_result" / "internal" / "biz"
        / "battle_result.go"
    ).read_text(encoding="utf-8")
    for msg in (
        roster._MSG_MISMATCH,  # noqa: SLF001
        roster._MSG_AUTHORITY_INVALID,  # noqa: SLF001
        roster._MSG_REPORTED_INVALID,  # noqa: SLF001
        roster._MSG_REPORTED_DUP,  # noqa: SLF001
        roster._MSG_OUTSIDER,  # noqa: SLF001
    ):
        assert msg in src, f"Go 侧找不到这条文案:{msg!r} —— 两栈的拒绝提示已经分叉"


def test_rating_decision_matches_go_settlement_runs_elo(repo_root) -> None:
    """★ 直接从 Go 源码里读 settlementRunsElo 的真值表,再逐条喂给 Python。

    上一版这里是把常量抄进测试 —— 于是实现抄错方向时测试跟着抄错,永远绿。
    改成从 Go 解析:Go 改了方向而 Python 没跟,这条会红。

    解析的是五个 return 的布尔值,键即 Go 的判据来源(basis)字符串:
    nil 快照 / rating_mode=NONE / rating_mode=ELO / 旧口径 pve_coop / 旧口径其余。
    """
    import re

    src_go = (
        repo_root / "services" / "battle" / "battle_result" / "internal" / "biz"
        / "battle_result.go"
    ).read_text(encoding="utf-8")
    head = src_go.index("func settlementRunsElo(")
    body = src_go[head : src_go.index("\n}\n", head)]
    # Go 的每条 return 都自带 basis 字符串,正好当真值表的键。
    basis = {
        name: flag
        for flag, name in re.findall(r'return (true|false), "([a-z_]+)"', body)
    }
    assert set(basis) == {
        "legacy_no_canonical",
        "rating_mode_none",
        "rating_mode_elo",
        "legacy_canonical_pve_coop",
        "legacy_canonical_game_mode",
    }, f"Go 的判据来源集合变了:{sorted(basis)}"

    def go_says(name: str) -> bool:
        return basis[name] == "true"

    assert roster.should_apply_rating(0, "") is go_says("legacy_no_canonical")
    assert roster.should_apply_rating(roster.RATING_MODE_NONE) is go_says(
        "rating_mode_none"
    )
    assert roster.should_apply_rating(roster.RATING_MODE_ELO) is go_says(
        "rating_mode_elo"
    )
    assert roster.should_apply_rating(
        roster.RATING_MODE_UNSPECIFIED, roster.CANONICAL_GAME_MODE_PVE_COOP
    ) is go_says("legacy_canonical_pve_coop")
    assert roster.should_apply_rating(
        roster.RATING_MODE_UNSPECIFIED, "ranked_5v5"
    ) is go_says("legacy_canonical_game_mode")


def test_rating_mode_values_match_generated_pb2() -> None:
    """★ 三个 RATING_MODE_* 必须就是 proto 生成物本身,不是手抄的字面量。

    手抄一份等价常量在改枚举那天才会暴露,而那时错的是玩家段位。
    """
    from pandora.config.v1 import level_pb2

    assert roster.RATING_MODE_UNSPECIFIED == level_pb2.LEVEL_RATING_MODE_UNSPECIFIED
    assert roster.RATING_MODE_NONE == level_pb2.LEVEL_RATING_MODE_NONE
    assert roster.RATING_MODE_ELO == level_pb2.LEVEL_RATING_MODE_ELO


# ── 判据来源(basis)必须一起返回 ─────────────────────────────────────────
#
# Go 的 settlementRunsElo 返回 (bool, basis)，调用方对回落旧口径的局打
# `battle_rating_basis_legacy_fallback`（WARN）。只返 bool 的话，**旧口径兜底的局
# 会静默结算** —— 段位改动不可逆，事后想追"这一局按什么算的"没有任何痕迹。


def test_basis_strings_match_go_verbatim() -> None:
    """basis 会进日志，是筛"回落旧口径的局"的唯一凭据。字符串对不上 = 查询恒空。"""
    assert roster.BASIS_LEGACY_NO_CANONICAL == "legacy_no_canonical"
    assert roster.BASIS_RATING_MODE_NONE == "rating_mode_none"
    assert roster.BASIS_RATING_MODE_ELO == "rating_mode_elo"
    assert roster.BASIS_LEGACY_CANONICAL_PVE_COOP == "legacy_canonical_pve_coop"
    assert roster.BASIS_LEGACY_CANONICAL_GAME_MODE == "legacy_canonical_game_mode"


@pytest.mark.parametrize(
    ("rating_mode", "game_mode", "want_run", "want_basis"),
    [
        # ① canonical 显式定格
        (roster.RATING_MODE_ELO, "", True, "rating_mode_elo"),
        (roster.RATING_MODE_NONE, "", False, "rating_mode_none"),
        (roster.RATING_MODE_ELO, "pve_coop", True, "rating_mode_elo"),   # 定格优先于旧口径
        # ② 有快照但未定格 → 旧口径兜底
        (roster.RATING_MODE_UNSPECIFIED, "pve_coop", False, "legacy_canonical_pve_coop"),
        (roster.RATING_MODE_UNSPECIFIED, "pvp", True, "legacy_canonical_game_mode"),
        (roster.RATING_MODE_UNSPECIFIED, "", True, "legacy_canonical_game_mode"),
        # ③ 连快照都没有（Go 的 terminalRelease == nil）
        (None, "", True, "legacy_no_canonical"),
        (None, "pve_coop", True, "legacy_no_canonical"),   # 无快照时不看 game_mode
    ],
)
def test_settlement_runs_elo_matches_go_branch_for_branch(
    rating_mode, game_mode: str, want_run: bool, want_basis: str
) -> None:
    assert roster.settlement_runs_elo(rating_mode, game_mode) == (want_run, want_basis)


def test_no_canonical_is_distinct_from_unspecified_rating_mode() -> None:
    """★ "没有快照" 与 "有快照但 rating_mode 未定格" 是两件事，basis 必须不同。

    合并成一个 0 的话，两类局在日志里长得一样，追不出到底是哪条兜底路径。
    """
    _, no_snapshot = roster.settlement_runs_elo(None, "pvp")
    _, unspecified = roster.settlement_runs_elo(roster.RATING_MODE_UNSPECIFIED, "pvp")
    assert no_snapshot != unspecified


def test_legacy_fallback_set_covers_exactly_the_two_warn_cases() -> None:
    """Go 只对这两个 basis 打 WARN，其余打 DEBUG。多一个少一个都会改变告警面。"""
    assert roster.LEGACY_FALLBACK_BASES == {
        "legacy_canonical_pve_coop", "legacy_canonical_game_mode"
    }


def test_should_apply_rating_still_agrees_with_the_tuple_form() -> None:
    """薄封装不能与本体分叉。"""
    for rm in (roster.RATING_MODE_ELO, roster.RATING_MODE_NONE, roster.RATING_MODE_UNSPECIFIED):
        for gm in ("", "pvp", "pve_coop"):
            assert roster.should_apply_rating(rm, gm) == roster.settlement_runs_elo(rm, gm)[0]
