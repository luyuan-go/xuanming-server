"""fence / lease 时间线校验测试。

这一组是**落 allocator 代码之前的前置验证**:两个 allocator 的正确性全部活在
这条时间线上,不验时序就落码 = 把一堆看起来合理的数字堆在一起然后祈祷。

每条不等式破了都**没有运行期信号** —— 服务照常启动、心跳照常上报,
只在某次网络分区时表现为"一个玩家同时在两台 DS"或"整个集群长期无写者"。
"""

from __future__ import annotations


from pandorapy import fence_timeline as ft
from pandorapy import placement


# ── ★ 整条时间线自洽 ───────────────────────────────────────────────────────


def test_current_timeline_has_no_violations() -> None:
    """★ 当前这套常量整体自洽。

    这是本文件的主断言:任何人改动五处常量里的**任何一个**,
    只要破坏了不等式,这条就会红 —— 而不是等到线上分区时才发现。
    """
    violations = ft.check_timeline()
    assert not violations, "时间线不自洽:\n" + "\n".join(
        f"  [{v.name}] {v.detail}" for v in violations
    )


# ── ★ 核心不等式 ───────────────────────────────────────────────────────────


def test_core_inequality_holds() -> None:
    """★ 旧 DS 最晚停止可玩时间 < 新 DS 最早开始可玩时间(§9.22)。

    这是整个 fence 体系存在的唯一目的。
    """
    old_stop = ft.old_ds_latest_stop_sec()
    new_start = ft.new_ds_earliest_start_sec()
    assert ft.core_inequality_holds(old_ds_stop_sec=old_stop, new_ds_start_sec=new_start)
    # 而且余量必须正好是偏差预算(不多不少 —— 多了白等,少了不安全)
    assert new_start - old_stop == placement.DS_FENCE_SKEW_MARGIN_SECONDS


def test_core_inequality_detects_violation() -> None:
    """反向验证:不等式函数确实会判否(不是恒真的摆设)。"""
    assert not ft.core_inequality_holds(old_ds_stop_sec=30, new_ds_start_sec=27)
    assert not ft.core_inequality_holds(old_ds_stop_sec=27, new_ds_start_sec=27)


# ── ★ 逐条不等式:每条都用"破坏它"来证明检查有效 ─────────────────────────


def test_barrier_derivation_is_checked(monkeypatch) -> None:
    """★ 屏障必须是派生值 —— 两处各写一个数会漂移。"""
    monkeypatch.setattr(placement, "DS_FENCE_REENTRY_BARRIER_SECONDS", 25)
    names = {v.name for v in ft.check_timeline()}
    assert "reentry_barrier_derivation" in names


def test_skew_margin_budget_is_checked(monkeypatch) -> None:
    """★ 余量必须覆盖三项预算:心跳在途 4s + 检测粒度 1s + 时钟漂移 ≥2s。

    2026-07-18 从 5 提到 7 的原因正是:原值被前两项恰好占满,**时钟漂移零预留**。
    ds_allocator 写 last_heartbeat_ms 与 login 读 now() 是两台机器的时钟。
    """
    monkeypatch.setattr(placement, "DS_FENCE_SKEW_MARGIN_SECONDS", 5)
    monkeypatch.setattr(placement, "DS_FENCE_REENTRY_BARRIER_SECONDS", 25)
    names = {v.name for v in ft.check_timeline()}
    assert "skew_margin_budget" in names


def test_locator_ttl_below_barrier_is_checked(monkeypatch) -> None:
    """★ locator TTL < 屏障 → presence 先蒸发而旧 DS 未 fencing 完。"""
    monkeypatch.setattr(ft, "LOCATOR_TTL_SEC", 20)
    names = {v.name for v in ft.check_timeline()}
    assert "locator_ttl" in names


def test_hub_heartbeat_below_barrier_is_checked(monkeypatch) -> None:
    monkeypatch.setattr(ft, "HUB_HEARTBEAT_TIMEOUT_SEC", 20)
    names = {v.name for v in ft.check_timeline()}
    assert "hub_heartbeat_timeout" in names


def test_activation_timeout_must_exceed_lease_ttl(monkeypatch) -> None:
    """★ 激活超时 <= lease TTL 时,**长期无主完全静默**。

    激活期间本副本已当选并占着 leader key,但还没对外宣告持有。
    钩子阻塞时:本副本不可写 + 占着 key 不让位 + 失败计数一次都不加
    (计数只在 err != nil 分支)→ 整个集群无写者,而 degraded 恒为 false。
    """
    monkeypatch.setattr(ft, "WRITER_ACTIVATION_TIMEOUT_SEC", 10)
    names = {v.name for v in ft.check_timeline()}
    assert "activation_timeout" in names


def test_escalate_threshold_must_exceed_lease_ttl(monkeypatch) -> None:
    """★ 告警阈值太低 → 一次正常的崩溃接任就告警 → 告警被当噪音忽略。

    这条是"告警疲劳"的机械防线:阈值必须大于正常接任耗时。
    """
    monkeypatch.setattr(ft, "WRITER_CAMPAIGN_ESCALATE_AFTER", 3)  # 3×2s = 6s < 15s
    names = {v.name for v in ft.check_timeline()}
    assert "escalate_threshold" in names


def test_local_safety_window_must_be_positive(monkeypatch) -> None:
    """★ 安全余量 >= TTL 时副本永远认为自己已失主 —— 永远不可写。"""
    monkeypatch.setattr(ft, "WRITER_HOLD_SAFETY_MARGIN_SEC", 15)
    names = {v.name for v in ft.check_timeline()}
    assert "writer_local_window" in names


def test_battle_timeout_must_be_shorter_than_hub(monkeypatch) -> None:
    """★ Battle 判弃必须比 Hub 快 —— 玩家在等对局结果,大厅挂了还能玩一会儿。"""
    monkeypatch.setattr(ft, "BATTLE_HEARTBEAT_TIMEOUT_SEC", 60)
    names = {v.name for v in ft.check_timeline()}
    assert "heartbeat_ordering" in names


# ── 常量本身(三方同值)─────────────────────────────────────────────────────


def test_constants_match_go_and_ue() -> None:
    """这几个数是跨仓契约(Go / Python / UE 三方),改一处就要三处一起改。"""
    assert placement.DS_FENCE_LEASE_MAX_SECONDS == 20
    assert placement.DS_FENCE_SKEW_MARGIN_SECONDS == 7
    assert placement.DS_FENCE_REENTRY_BARRIER_SECONDS == 27
    assert ft.WRITER_LEASE_TTL_SEC == 15
    assert ft.WRITER_HOLD_SAFETY_MARGIN_SEC == 3
    assert ft.WRITER_ACTIVATION_TIMEOUT_SEC == 30


def test_writer_local_window_is_twelve_seconds() -> None:
    """lease TTL 15s - 余量 3s = 本地窗口 12s(注释里写死的推导结果)。"""
    assert ft.WRITER_LEASE_TTL_SEC - ft.WRITER_HOLD_SAFETY_MARGIN_SEC == 12


def test_activation_timeout_is_two_lease_ttls() -> None:
    """30s = 2× lease TTL —— 足够慢 etcd 完成,又让无主在租约兜底窗口的同数量级内可观测。"""
    assert ft.WRITER_ACTIVATION_TIMEOUT_SEC == 2 * ft.WRITER_LEASE_TTL_SEC


def test_escalate_window_is_thirty_seconds() -> None:
    """15 次 × 2s 退避 ≈ 30s 无主,超过 lease TTL 兜底接任窗口即异常。"""
    assert ft.WRITER_CAMPAIGN_ESCALATE_AFTER * ft.WRITER_RECAMPAIGN_BACKOFF_SEC == 30


# ── 时间线可读性(供人核对)──────────────────────────────────────────────────


def test_timeline_can_be_rendered_for_humans() -> None:
    """把时间线打出来 —— 这些数字之间的关系比单个数字重要。"""
    lines = [
        f"t=0s     旧 DS 最后一次成功心跳",
        f"t={ft.BATTLE_HEARTBEAT_TIMEOUT_SEC}s    Battle DS 判弃(段位回滚)",
        f"t={placement.DS_FENCE_LEASE_MAX_SECONDS}s    旧 DS 自我 fencing(关输入/Kick/销毁 Pawn)  ← 最晚停止可玩",
        f"t={placement.DS_FENCE_REENTRY_BARRIER_SECONDS}s    服务端再入屏障打开                        ← 最早开始可玩",
        f"t={ft.HUB_HEARTBEAT_TIMEOUT_SEC}s    Hub DS 判超时 / locator presence 蒸发",
    ]
    rendered = "\n".join(lines)
    assert "最晚停止可玩" in rendered
    assert "最早开始可玩" in rendered
    # 顺序必须严格递增
    marks = [
        ft.BATTLE_HEARTBEAT_TIMEOUT_SEC,
        placement.DS_FENCE_LEASE_MAX_SECONDS,
        placement.DS_FENCE_REENTRY_BARRIER_SECONDS,
        ft.HUB_HEARTBEAT_TIMEOUT_SEC,
    ]
    assert marks == sorted(marks), f"时间线刻度不单调:{marks}"
