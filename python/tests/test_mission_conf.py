"""mission 配置:默认值必须与 Go 的 Defaults() 逐字段相同。

咬住的是那一类"两边都不报错"的缺陷:同一份 yaml 在 Go 副本和 Python 副本上跑出
不同行为,面板上只看到"某些副本任务不涨/奖励不发",查到最后是一个默认值。
"""

from __future__ import annotations

import pathlib

import pytest

from pandorapy.services.mission import conf as mconf

GO_CONF = "services/social/mission/internal/conf/conf.go"


def _cfg(**mission_kwargs) -> mconf.Config:
    """构造一份**除被测项外全部合法**的配置,再填默认值。"""
    cfg = mconf.Config.model_validate(
        {
            "node": {"node_id": 1},
            "config_table": {"dir": "x"},
            "mission": {"allow_noop_reward": True, **mission_kwargs},
        }
    )
    cfg.apply_defaults()
    return cfg


def test_defaults_match_go() -> None:
    cfg = _cfg()
    m = cfg.mission
    assert m.max_active_missions == 50
    assert m.max_facts_per_report == 64
    assert m.reward_retry_interval_td().total_seconds() == 60
    assert m.reward_retry_grace_td().total_seconds() == 120
    assert m.reward_retry_batch == 200
    assert m.push_publish_interval_td().total_seconds() == 1
    assert m.push_publish_batch == 128
    assert m.reward_log_retention_days == 90
    assert m.receipt_retention_days == 90
    assert m.sweep_interval_td().total_seconds() == 300
    assert m.sweep_batch == 500
    assert cfg.server.grpc.addr == ":20019"
    assert cfg.server.http.addr == ":21019"


def test_receipt_cleanup_defaults_off() -> None:
    """§9.24:收据清理**默认关**。

    开着会怎样:上游 battle_progress_outbox 的重试没有总期限,删收据后迟到重放会把
    同一批事实**双计**进任务进度 —— 玩家进度凭空多一截,而两端都不报错。
    """
    assert _cfg().mission.receipt_cleanup_enabled is False


def test_allow_noop_reward_defaults_off() -> None:
    """默认 False:漏配发奖下游时拒启,而不是静默以「发奖恒失败」启动。"""
    cfg = mconf.Config.model_validate({"config_table": {"dir": "x"}})
    cfg.apply_defaults()
    assert cfg.mission.allow_noop_reward is False


@pytest.mark.parametrize("bad", [-1, 0])
def test_defaults_use_le_zero_predicate(bad: int) -> None:
    """判据是 `<= 0` 而不是 `== 0` —— 与 Go 逐字相同。

    写成 `== 0` 会让 `max_facts_per_report: -1` 原样带着跑:一切事实上报都撞
    `len(facts) > -1` 被判 ERR_INVALID_ARG,而 Go 副本一切正常。
    """
    assert _cfg(max_facts_per_report=bad).mission.max_facts_per_report == 64
    assert _cfg(sweep_batch=bad).mission.sweep_batch == 500


def test_validate_startup_requires_configtable_dir() -> None:
    cfg = mconf.Config.model_validate({"mission": {"allow_noop_reward": True}})
    cfg.apply_defaults()
    with pytest.raises(ValueError, match="config_table.dir"):
        cfg.validate_startup()


def test_validate_startup_requires_reward_downstreams() -> None:
    """未开 allow_noop_reward 时 inventory / player 地址缺一不可。"""
    cfg = mconf.Config.model_validate({"config_table": {"dir": "x"}})
    cfg.apply_defaults()
    with pytest.raises(ValueError, match="inventory_addr"):
        cfg.validate_startup()

    cfg = mconf.Config.model_validate(
        {"config_table": {"dir": "x"}, "mission": {"inventory_addr": "a:1"}}
    )
    cfg.apply_defaults()
    with pytest.raises(ValueError, match="player_addr"):
        cfg.validate_startup()


def test_retention_mode_typo_is_rejected_at_startup() -> None:
    """拼错必须拒启:静默回落 report_only = 运维以为开了清理、实际一行没删。"""
    cfg = _cfg(retention_mode="delet")
    with pytest.raises(ValueError):
        cfg.mission.validate_retention_mode()
    # 但**运行期取值**恒回落 report_only —— 绝不能因为配错就去删数据。
    assert cfg.mission.retention_mode_parsed().value == "report_only"


@pytest.mark.parametrize(
    ("raw", "want"),
    [("", "off"), ("off", "off"), ("OFF", "off"), ("enforce", "enforce"), ("  Enforce ", "enforce")],
)
def test_push_writer_lease_mode_normalized(raw: str, want: str) -> None:
    cfg = _cfg(push_writer_lease={"mode": raw})
    assert cfg.mission.push_writer_lease.resolve_mode() == want


def test_push_writer_lease_mode_typo_raises() -> None:
    """取值不认识**报错而非猜**:静默退回 off 会让滚动重叠期两个发布器并发。"""
    cfg = _cfg(push_writer_lease={"mode": "enfore"})
    with pytest.raises(ValueError, match="enfore"):
        cfg.mission.push_writer_lease.resolve_mode()


def test_dev_yaml_loads(repo_root: pathlib.Path) -> None:
    """Python 版必须能吃**同一份** yaml —— 迁移期运维只维护一份配置。"""
    path = repo_root / "services/social/mission/etc/mission-dev.yaml"
    cfg = mconf.Config.load(path)
    cfg.validate_startup()
    assert cfg.server.grpc.addr == ":20019"
    assert cfg.config_table.dir
    # kafka / session_gate 必须被**建模**而不是落进 model_extra:
    # 落进 extra 的话 yaml 里配了 broker、Python 侧永远静默不发。
    assert cfg.kafka.brokers
    assert cfg.session_gate.require is False


def test_go_defaults_are_still_the_same(repo_root: pathlib.Path) -> None:
    """直接对着 Go 源码断言,Go 侧改了默认值这里当场变红。"""
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")
    for needle in (
        "c.Mission.MaxActiveMissions = 50",
        "c.Mission.MaxFactsPerReport = 64",
        "c.Mission.RewardRetryBatch = 200",
        "c.Mission.PushPublishBatch = 128",
        "c.Mission.RewardLogRetentionDays = 90",
        "c.Mission.ReceiptRetentionDays = 90",
        "c.Mission.SweepBatch = 500",
        'c.Server.Grpc.Addr = ":20019"',
        'c.Server.Http.Addr = ":21019"',
    ):
        assert needle in src, f"Go 侧默认值已变,Python conf.py 需同步: {needle}"
