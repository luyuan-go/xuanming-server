"""开局在线闸测试。

重点是那条**与 §9.22 相反**的代价方向:
    这个闸管开局体验不管归属判定,所以 UNKNOWN 一律放行 ——
    「宁可放过一个,不可误杀在线的」。

外加:
  - 缺席名单必须走结构化通道(客户端要能点名)
  - 依赖故障与 UNKNOWN 是两条不同的分支
  - game_mode 校验防跨池改票
"""

from __future__ import annotations

import pytest

from pandorapy import errcode
from pandorapy.services.matchmaker import presence_gate as gate

NOW = 1_760_000_000_000
GRACE = 60_000  # 60s


# ── ★ UNKNOWN 一律放行 ─────────────────────────────────────────────────────


def test_no_baseline_is_treated_as_present() -> None:
    """★ 这是本文件最重要的一条。

    没有离开基线的三种来源:从没上过线 / 已超保留期 / **Hub DS 整台挂掉时压根没上报**。
    第三种最要命:若把"查不到"当成"离线",整个大厅的人都开不了局 ——
    而他们其实都在线。
    """
    offline, _ = gate.absent_beyond([1, 2, 3], {}, GRACE, NOW)
    assert offline == []


def test_zero_or_negative_baseline_is_treated_as_present() -> None:
    """脏数据(基线 <= 0)同样放行,不当成"很久以前离开"。"""
    offline, _ = gate.absent_beyond([1, 2], {1: 0, 2: -1}, GRACE, NOW)
    assert offline == []


def test_recently_left_is_within_grace() -> None:
    """刚离开的人可能正在重连 —— grace 窗内不算缺席。"""
    offline, _ = gate.absent_beyond([1], {1: NOW - 30_000}, GRACE, NOW)
    assert offline == []


def test_long_absent_is_rejected() -> None:
    """确实离开超过 grace 才算缺席。"""
    offline, longest = gate.absent_beyond([1], {1: NOW - 120_000}, GRACE, NOW)
    assert offline == [1]
    assert longest == 120_000


def test_mixed_roster_only_flags_the_absent() -> None:
    last_seen = {
        1: NOW - 5_000,    # 刚离开
        2: NOW - 300_000,  # 很久
        # 3 没有基线 → 放行
    }
    offline, longest = gate.absent_beyond([1, 2, 3], last_seen, GRACE, NOW)
    assert offline == [2]
    assert longest == 300_000


def test_boundary_exactly_at_grace_is_absent() -> None:
    """恰好等于 grace 判缺席(< window 才放行)。"""
    offline, _ = gate.absent_beyond([1], {1: NOW - GRACE}, GRACE, NOW)
    assert offline == [1]


# ── ★ 缺席名单走结构化通道 ─────────────────────────────────────────────────


def test_offline_error_carries_player_ids() -> None:
    """★ 光靠 error 文本客户端点不了名。

    队长只看到"有队员不在大厅"却不知道该等谁 —— 名单必须能被程序读到。
    """
    with pytest.raises(gate.MemberOfflineError) as exc:
        gate.ensure_all_present(
            [1001, 1002], {1001: NOW - 300_000}, grace_ms=GRACE, now_ms=NOW
        )
    assert exc.value.absent_player_ids == [1001]
    assert exc.value.code == errcode.ErrMatchMemberOffline


def test_offline_error_is_still_a_pandora_error() -> None:
    """错误码语义不变 —— as_code 照常解析(携带名单是额外信息,不改语义)。"""
    err = gate.MemberOfflineError([1], 60.0)
    assert errcode.as_code(err) == errcode.ErrMatchMemberOffline


# ── 闸的开关 ────────────────────────────────────────────────────────────────


def test_zero_grace_disables_gate() -> None:
    """grace <= 0 → 闸关闭,不做任何判定。"""
    gate.ensure_all_present([1], {1: NOW - 999_999}, grace_ms=0, now_ms=NOW)


def test_empty_roster_passes() -> None:
    gate.ensure_all_present([], {}, grace_ms=GRACE, now_ms=NOW)


def test_zero_player_ids_are_filtered() -> None:
    """player_id=0 是占位,不参与判定。"""
    gate.ensure_all_present([0, 0], {}, grace_ms=GRACE, now_ms=NOW)


def test_all_present_passes() -> None:
    gate.ensure_all_present(
        [1, 2], {1: NOW - 1_000, 2: NOW - 2_000}, grace_ms=GRACE, now_ms=NOW
    )


# ── ★ 依赖故障是另一条分支 ─────────────────────────────────────────────────


def test_dependency_failure_fail_open_passes() -> None:
    """★ 依赖故障与 UNKNOWN 是**两回事**。

    UNKNOWN:查通了,这个玩家没有离开基线 → 当他在线
    依赖故障:压根没查通 → 由配置决定
    """
    gate.presence_gate_unavailable("get_last_seen", RuntimeError("redis down"), fail_open=True)


def test_dependency_failure_fail_closed_raises() -> None:
    with pytest.raises(errcode.PandoraError) as exc:
        gate.presence_gate_unavailable(
            "get_last_seen", RuntimeError("redis down"), fail_open=False
        )
    assert exc.value.code == errcode.ErrUnavailable
    assert "locator unavailable" in exc.value.msg


# ── ★ game_mode 跨池防护 ───────────────────────────────────────────────────


def test_cross_pool_write_rejected() -> None:
    """★ 防止被路由到默认 PVP 实例的冷客户端去改一张 canonical PVE 票据。

    不校验的后果:玩家排在一个池里却被另一个池撮合,**而且不报错**。
    """
    with pytest.raises(errcode.PandoraError) as exc:
        gate.require_local_game_mode("pve_coop", "pvp")
    assert exc.value.code == errcode.ErrInvalidState


def test_same_pool_passes() -> None:
    gate.require_local_game_mode("pvp", "pvp")


def test_empty_stored_mode_passes_for_rolling_upgrade() -> None:
    """★ 空值**只**为滚动升级期的旧记录放行(那时还没有 game_mode 字段)。

    每个新写者都会持久化 canonical 命名空间,所以这条豁免会随旧记录过期而失效。
    """
    gate.require_local_game_mode("", "pvp")
    gate.require_local_game_mode("", "pve_coop")
