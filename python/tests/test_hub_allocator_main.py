"""hub_allocator 启动闸链的可测部分 —— 覆盖 `main.py` 新接的两道闸的判定逻辑
与 `http.py` 的写者健康投影。

范围刻意限定在**不需要 etcd / Redis / gRPC** 的纯判定:
  · `_check_writer_lease_deploy_strategy`:部署策略机械门禁的三档;
  · `_writer_identity`:hostname/pid 可观测标签;
  · `HUB_AUTH_FENCE_FEATURES_MODEL_B`:与 `dsauthfence` 的 V3 策略集**精确相等**;
  · `_watch_fence_lost` / `_noop_on_elected`:失租 → 退出的传导、warmup 空钩子;
  · `http.WriterHealthHolder`:未注入租约时 fail-closed 到 enabled=false。
"""

from __future__ import annotations

import asyncio
import os

import pytest

from pandorapy import dsauthfence
from pandorapy import writerlease
from pandorapy.services.hub_allocator import conf as hconf
from pandorapy.services.hub_allocator import http as hhttp
from pandorapy.services.hub_allocator import main as hmain


class _RecordingLogger:
    """只记事件名与字段的假 logger(结构化日志的 kwargs 全留下,便于断字段名)。"""

    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict]] = []

    def info(self, event: str, **kw: object) -> None:
        self.records.append(("info", event, dict(kw)))

    def warning(self, event: str, **kw: object) -> None:
        self.records.append(("warning", event, dict(kw)))

    def error(self, event: str, **kw: object) -> None:
        self.records.append(("error", event, dict(kw)))

    def events(self) -> list[str]:
        return [event for _, event, _ in self.records]


@pytest.fixture(autouse=True)
def _clean_strategy_env(monkeypatch: pytest.MonkeyPatch):
    """两个 env 都必须显式清掉:CI runner 若恰好跑在 k8s 里,
    `KUBERNETES_SERVICE_HOST` 会让"非 k8s"那一档永远测不到。"""
    monkeypatch.delenv(hmain.DEPLOY_STRATEGY_ENV, raising=False)
    monkeypatch.delenv(hmain.KUBERNETES_SERVICE_HOST_ENV, raising=False)


# ── 部署策略机械门禁 ─────────────────────────────────────────────────────────


def test_strategy_rollingupdate_with_enforce_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(hmain.DEPLOY_STRATEGY_ENV, "RollingUpdate")
    logger = _RecordingLogger()
    assert hmain._check_writer_lease_deploy_strategy(logger, hconf.WRITER_LEASE_ENFORCE)
    assert logger.events() == ["hub_writer_lease_strategy_checked"]


@pytest.mark.parametrize("mode", [hconf.WRITER_LEASE_WARMUP, hconf.WRITER_LEASE_OFF])
def test_strategy_rollingupdate_without_enforce_blocks(
    monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    """RollingUpdate × 非 enforce = 滚动重叠期无保护双写 → 必须 fail-closed。"""
    monkeypatch.setenv(hmain.DEPLOY_STRATEGY_ENV, "RollingUpdate")
    logger = _RecordingLogger()
    assert not hmain._check_writer_lease_deploy_strategy(logger, mode)
    level, event, fields = logger.records[-1]
    assert (level, event) == ("error", "hub_writer_lease_rollingupdate_without_enforce")
    assert fields["strategy"] == "RollingUpdate"
    assert fields["mode"] == mode


def test_strategy_match_is_case_insensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    """Go 用 EqualFold;annotation 大小写漂移不能变成"绕过门禁"。"""
    monkeypatch.setenv(hmain.DEPLOY_STRATEGY_ENV, "rollingUPDATE")
    logger = _RecordingLogger()
    assert not hmain._check_writer_lease_deploy_strategy(logger, hconf.WRITER_LEASE_OFF)
    assert logger.events() == ["hub_writer_lease_rollingupdate_without_enforce"]


def test_strategy_recreate_passes_in_any_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(hmain.DEPLOY_STRATEGY_ENV, "Recreate")
    logger = _RecordingLogger()
    assert hmain._check_writer_lease_deploy_strategy(logger, hconf.WRITER_LEASE_OFF)
    assert logger.events() == ["hub_writer_lease_strategy_checked"]


def test_strategy_missing_in_managed_k8s_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    """受管 k8s 内 annotation 缺失 → fail-closed(清单回归必须炸,不能靠人看日志)。"""
    monkeypatch.setenv(hmain.KUBERNETES_SERVICE_HOST_ENV, "10.96.0.1")
    logger = _RecordingLogger()
    assert not hmain._check_writer_lease_deploy_strategy(logger, hconf.WRITER_LEASE_ENFORCE)
    assert logger.events() == ["hub_writer_lease_strategy_annotation_missing"]


def test_strategy_missing_outside_k8s_only_warns() -> None:
    """本机裸跑 / dev:阻断会把开发环境一起打死,只告警。"""
    logger = _RecordingLogger()
    assert hmain._check_writer_lease_deploy_strategy(logger, hconf.WRITER_LEASE_OFF)
    level, event, _ = logger.records[-1]
    assert (level, event) == ("warning", "hub_writer_lease_strategy_unknown")


def test_strategy_blank_env_treated_as_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """全空白 annotation 等价缺失(Go 侧 TrimSpace);受管 k8s 内同样 fail-closed。"""
    monkeypatch.setenv(hmain.DEPLOY_STRATEGY_ENV, "   ")
    monkeypatch.setenv(hmain.KUBERNETES_SERVICE_HOST_ENV, "  10.96.0.1  ")
    logger = _RecordingLogger()
    assert not hmain._check_writer_lease_deploy_strategy(logger, hconf.WRITER_LEASE_ENFORCE)
    assert logger.events() == ["hub_writer_lease_strategy_annotation_missing"]


# ── capability feature 契约 ──────────────────────────────────────────────────


def test_model_b_features_equal_required_policy_v3_set() -> None:
    """etcd 侧是**精确相等**比较:多一个 / 少一个 / 拼错都会让 capability 注册不上。"""
    expected = dsauthfence.REQUIRED_POLICY_V3_FEATURES["hub_allocator"]
    assert hmain.HUB_AUTH_FENCE_FEATURES_MODEL_B == expected
    assert dsauthfence.equal_feature_set(hmain.HUB_AUTH_FENCE_FEATURES_MODEL_B, expected)


def test_model_b_features_are_not_the_v2_set() -> None:
    """V3 多出 `hub-successor-lease-v1`;拿 V2 集去注册会让继任租约那一档失去策略依据。"""
    v2 = dsauthfence.REQUIRED_POLICY_V2_FEATURES["hub_allocator"]
    assert hmain.HUB_AUTH_FENCE_FEATURES_MODEL_B != v2
    assert "hub-successor-lease-v1" in hmain.HUB_AUTH_FENCE_FEATURES_MODEL_B


# ── 可观测身份 ───────────────────────────────────────────────────────────────


def test_writer_identity_is_hostname_slash_pid() -> None:
    ident = hmain._writer_identity()
    host, sep, pid = ident.rpartition("/")
    assert sep == "/"
    assert pid == str(os.getpid())
    # hostname 允许为空(见 `_writer_identity` 的 OSError 分支),但不能带路径分隔符
    # ——否则 `rpartition("/")` 切出来的 pid 段就不是 pid 了。
    assert "/" not in host


def test_writer_identity_survives_hostname_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """hostname 取不到时只丢标签,绝不能把启动闸链打断。"""

    def _boom() -> str:
        raise OSError("no hostname")

    monkeypatch.setattr(hmain.socket, "gethostname", _boom)
    assert hmain._writer_identity() == f"/{os.getpid()}"


# ── 失租守望 / warmup 空钩子 ─────────────────────────────────────────────────


class _FakeFence:
    """只暴露 `_watch_fence_lost` 用到的两个面。"""

    def __init__(self) -> None:
        self.lost = asyncio.Event()
        self._reason = ""

    def set_lost(self, reason: str) -> None:
        self._reason = reason
        self.lost.set()

    def lost_reason(self) -> str:
        return self._reason


@pytest.mark.asyncio
async def test_watch_fence_lost_exits_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """失租 → 进程退出。这是 capability 失效到 fail-stop 之间唯一的传导路径。"""
    exited: list[bool] = []
    monkeypatch.setattr(hmain, "_exit_process", lambda: exited.append(True))
    fence = _FakeFence()
    task = asyncio.create_task(hmain._watch_fence_lost(fence))
    await asyncio.sleep(0)
    assert not exited, "未失租前不得退出"
    fence.set_lost(dsauthfence.LOST_REASON_REQUIRED_WATCH_CLOSED)
    await asyncio.wait_for(task, timeout=1.0)
    assert exited == [True]


@pytest.mark.asyncio
async def test_noop_on_elected_returns_none() -> None:
    """warmup 档的空钩子必须**立刻**返回:它占着 leader key 的每一毫秒都是无写者窗口。"""
    assert await asyncio.wait_for(hmain._noop_on_elected(7), timeout=1.0) is None


# ── /healthz/writer 健康投影 ─────────────────────────────────────────────────


class _FakeLease:
    def __init__(self, snap: writerlease.HealthSnapshot) -> None:
        self._snap = snap

    def health(self) -> writerlease.HealthSnapshot:
        return self._snap


def test_writer_health_unset_is_fail_closed() -> None:
    """租约未注入 → enabled=false 且不算 degraded;**不能**伪装成"扫得好好的"。"""
    holder = hhttp.WriterHealthHolder()
    body = holder.snapshot()
    assert body["enabled"] is False
    assert body["held"] is False
    assert body["token"] == 0
    assert body["degraded"] is False
    assert holder.degraded() is False
    # omitempty:没有档位 / 没有错误原文时不出这些键。
    assert "mode" not in body
    assert "last_campaign_err" not in body
    assert "last_activation_err" not in body
    assert hhttp.WRITER_LEASE_ENABLED._value.get() == 0
    assert hhttp.WRITER_HELD._value.get() == 0


def test_writer_health_held_projects_gauges() -> None:
    holder = hhttp.WriterHealthHolder()
    holder.set(
        _FakeLease(writerlease.HealthSnapshot(held=True, token=42)),
        hconf.WRITER_LEASE_ENFORCE,
    )
    body = holder.snapshot()
    assert body["enabled"] is True
    assert body["mode"] == hconf.WRITER_LEASE_ENFORCE
    assert body["held"] is True
    assert body["token"] == 42
    assert body["degraded"] is False
    assert hhttp.WRITER_LEASE_ENABLED._value.get() == 1
    assert hhttp.WRITER_HELD._value.get() == 1
    assert hhttp.WRITER_TOKEN._value.get() == 42


def test_writer_health_degraded_surfaces_errors() -> None:
    """两组计数分开上报:运维看到 degraded 才知道该查 etcd 还是查激活钩子。"""
    snap = writerlease.HealthSnapshot(
        consecutive_campaign_errs=writerlease.CAMPAIGN_ESCALATE_AFTER,
        last_campaign_err="etcd unreachable",
        consecutive_activation_errs=3,
        last_activation_err="advance timeout",
    )
    holder = hhttp.WriterHealthHolder()
    holder.set(_FakeLease(snap), hconf.WRITER_LEASE_WARMUP)
    body = holder.snapshot()
    assert body["degraded"] is True
    assert holder.degraded() is True
    assert body["last_campaign_err"] == "etcd unreachable"
    assert body["last_activation_err"] == "advance timeout"
    assert body["consecutive_activation_errs"] == 3
    assert hhttp.WRITER_DEGRADED._value.get() == 1
    assert hhttp.WRITER_CAMPAIGN_ERRORS._value.get() == writerlease.CAMPAIGN_ESCALATE_AFTER
