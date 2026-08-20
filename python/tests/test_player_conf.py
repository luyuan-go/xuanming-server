"""player 配置默认值与 Go 的 `Defaults()` 对拍。

★ 为什么这些用例值得单独写:默认值分叉是**两边都不报错**的缺陷 —— 同一份 yaml 喂两个
  实现,行为不同而日志全绿。player 这里混用了三种判据符号(`<=0` / `<0` / `==""`),
  抄错任何一个都落在这一类。

★ 判据符号本身也钉住:`mmr_floor` 是 `< 0` 而不是 `<= 0`,写成 `<= 0` 不会有任何表现
  (floor=0 兜成 0),直到有人配 `mmr_floor: 0` 想表达"不设下限"——那时也没有表现。
  所以只能靠"读 Go 源码 + 钉测试"来防。
"""

from __future__ import annotations

import datetime as _dt
import pathlib

import pytest

from pandorapy import dbguard, kafka_topics
from pandorapy.services.player import conf as pconf

DEV_YAML = (
    pathlib.Path(__file__).resolve().parents[2]
    / "services"
    / "account"
    / "player"
    / "etc"
    / "player-dev.yaml"
)


def _empty() -> pconf.Config:
    """全零配置 + apply_defaults —— 对应 Go 侧 `var cfg conf.Config; cfg.Defaults()`。"""
    cfg = pconf.Config()
    cfg.apply_defaults()
    return cfg


def test_defaults_match_go() -> None:
    cfg = _empty()
    assert cfg.player.base_mmr == 1500
    assert cfg.player.mmr_floor == 0
    assert cfg.player.default_nickname_prefix == "Player_"
    assert cfg.player.max_nickname_len == 32
    assert cfg.player.consume_topics == [kafka_topics.TOPIC_PLAYER_UPDATE]
    assert cfg.server.grpc.addr == ":20002"
    assert cfg.server.http.addr == ":21002"


def test_base_mmr_uses_le_zero_predicate() -> None:
    """Go: `if c.Player.BaseMMR <= 0`。负值也要被兜成 1500。"""
    cfg = pconf.Config.model_validate({"player": {"base_mmr": -5}})
    cfg.apply_defaults()
    assert cfg.player.base_mmr == 1500


def test_mmr_floor_uses_lt_zero_predicate() -> None:
    """Go: `if c.Player.MMRFloor < 0`(**不是** <=0)。

    floor=0 是合法且常用的配置(dev 就写 0);写成 `<= 0` 表面上结果一样,但语义变成
    "0 被当成没配" —— 将来 default 若不再是 0,这条就会静默改变行为。
    """
    cfg = pconf.Config.model_validate({"player": {"mmr_floor": -3}})
    cfg.apply_defaults()
    assert cfg.player.mmr_floor == 0

    cfg2 = pconf.Config.model_validate({"player": {"mmr_floor": 7}})
    cfg2.apply_defaults()
    assert cfg2.player.mmr_floor == 7


def test_effective_accessors_match_go() -> None:
    cfg = _empty()
    assert cfg.player.max_exp_per_grant_effective() == 1_000_000
    assert cfg.player.push_outbox_interval_sec() == 1.0
    assert cfg.player.push_outbox_batch_effective() == 128


@pytest.mark.parametrize(
    ("configured", "expect_days"),
    [
        ("", 7),  # 未配 → 7 天
        ("1h", 7),  # 低于下限 → 钳到 7 天(防手滑把幂等窗清穿)
        ("168h", 7),  # 恰好 7 天
        ("240h", 10),  # 正常放大
        ("10000h", 90),  # 高于 §9.24 硬上限 → 钳到 90 天
    ],
)
def test_exp_history_retention_clamp(configured: str, expect_days: int) -> None:
    cfg = pconf.Config.model_validate({"player": {"exp_history_retention": configured}})
    cfg.apply_defaults()
    assert cfg.player.exp_history_retention_effective() == _dt.timedelta(days=expect_days)


@pytest.mark.parametrize(
    ("days", "expect"),
    [(0, 90), (-1, 90), (10, 30), (30, 30), (60, 60), (90, 90), (365, 90)],
)
def test_history_retention_clamp(days: int, expect: int) -> None:
    """先钳**天数整数**再乘 24h —— 与 Go 逐字同序(Go 里先乘会溢出成负数误落 floor)。"""
    cfg = pconf.Config.model_validate({"player": {"history_retention_days": days}})
    cfg.apply_defaults()
    assert cfg.player.history_retention_effective() == _dt.timedelta(days=expect)


def test_retention_two_gates() -> None:
    """两道闸:总闸与本组前置条件**都开**才删。任一没开都只报告。"""
    both = pconf.Config.model_validate(
        {
            "player": {
                "retention_mode": "delete",
                "exp_history_cleanup_enabled": True,
                "history_cleanup_enabled": True,
            }
        }
    )
    both.apply_defaults()
    assert both.player.exp_history_retention_mode() is dbguard.Mode.DELETE
    assert both.player.history_retention_mode() is dbguard.Mode.DELETE

    # 总闸开、前置没确认 → 降级 report_only(**不是**不跑 janitor:那样待清理量会
    # 整个不可见,库在无人知晓的情况下涨)。
    gate_only = pconf.Config.model_validate({"player": {"retention_mode": "delete"}})
    gate_only.apply_defaults()
    assert gate_only.player.exp_history_retention_mode() is dbguard.Mode.REPORT_ONLY
    assert gate_only.player.history_retention_mode() is dbguard.Mode.REPORT_ONLY

    # 前置确认了但总闸没开 → 同样只报告。
    pre_only = pconf.Config.model_validate(
        {"player": {"exp_history_cleanup_enabled": True, "history_cleanup_enabled": True}}
    )
    pre_only.apply_defaults()
    assert pre_only.player.exp_history_retention_mode() is dbguard.Mode.REPORT_ONLY


def test_cleanup_flags_default_false() -> None:
    """§9.24 登记表:player 的五张只增表清理**默认 report_only 且还有第二道闸**。"""
    cfg = _empty()
    assert cfg.player.exp_history_cleanup_enabled is False
    assert cfg.player.history_cleanup_enabled is False
    assert cfg.player.retention_mode_parsed() is dbguard.Mode.REPORT_ONLY


def test_retention_mode_typo_is_fail_fast_but_runtime_falls_back() -> None:
    """拼错必须启动期报错;但运行期取值回落 report_only(绝不能猜成 delete)。"""
    cfg = pconf.Config.model_validate({"player": {"retention_mode": "delet"}})
    cfg.apply_defaults()
    with pytest.raises(ValueError):
        cfg.player.validate_retention_mode()
    assert cfg.player.retention_mode_parsed() is dbguard.Mode.REPORT_ONLY


@pytest.mark.parametrize("alias", ["", "report", "report-only", "report_only"])
def test_retention_mode_accepts_go_aliases(alias: str) -> None:
    """Go 的 ParseMode 认四个写法。少认两个 = 一份在 Go 上跑得好好的 yaml 起不来。"""
    cfg = pconf.Config.model_validate({"player": {"retention_mode": alias}})
    cfg.apply_defaults()
    cfg.player.validate_retention_mode()
    assert cfg.player.retention_mode_parsed() is dbguard.Mode.REPORT_ONLY


def test_push_writer_lease_mode_resolution() -> None:
    assert pconf.PushWriterLeaseConf().resolve_mode() == "off"
    assert pconf.PushWriterLeaseConf(mode="off").resolve_mode() == "off"
    assert pconf.PushWriterLeaseConf(mode="ENFORCE").resolve_mode() == "enforce"
    with pytest.raises(ValueError):
        # 拼错**报错而非猜** —— 猜成 off 会静默退回无保护并发发布。
        pconf.PushWriterLeaseConf(mode="enfore").resolve_mode()


def test_ds_auth_defaults_match_go() -> None:
    """mode / secret 留空即"不启用",**不填默认**;其余字段有默认。"""
    cfg = _empty()
    assert cfg.ds_auth.mode == ""
    assert cfg.ds_auth.secret == ""
    assert cfg.ds_auth.authority_mode == "legacy"
    assert cfg.ds_auth.issuer == "pandora-ds-control"
    assert cfg.ds_auth.audience == "pandora-ds"
    assert cfg.ds_auth.battle_token_ttl == "4h"
    assert cfg.ds_auth.hub_token_ttl == "24h"
    assert cfg.ds_auth.active_heartbeat_max_age == "30s"


@pytest.mark.skipif(not DEV_YAML.is_file(), reason="dev yaml 不在(仓库裁剪过)")
def test_loads_the_same_dev_yaml_as_go() -> None:
    """同一份 etc/player-dev.yaml 必须能被 Python 版直接吃下。"""
    cfg = pconf.Config.load(str(DEV_YAML))
    assert cfg.server.grpc.addr == ":20002"
    assert cfg.server.http.addr == ":21002"
    assert cfg.player.consume_topics == ["pandora.player.update"]
    assert cfg.player.loadout_customize_enabled is True
    assert cfg.player.experience_enabled is True
    assert cfg.kafka.group_id == "pandora-player"
    assert cfg.ds_auth.mode == "permissive"
    # dev 档两道闸都开(本地数据可弃,为了覆盖真删代码路径)。
    assert cfg.player.exp_history_retention_mode() is dbguard.Mode.DELETE
