"""battle_result 配置 —— 默认值 / 钳位 / 校验闸,逐条对着 Go 的 conf.go 断言。

为什么这些必须有测试:默认值分叉的后果**不是报错**,是同一份 yaml 在 Go 和 Python 上
跑出不同行为而两边都不报错。本服尤其危险的两处:

  ① `retention_mode` 留空 = **delete**(与 dbguard 全局默认相反)。抄错成 report_only 的话
     Python 副本永远不删战报、Go 副本在删 —— 库涨不涨取决于请求落到哪个副本。
  ② `history_retention_days` 的 [30,180] 双向钳位。缺上限钳位 → 配 365 原样生效,
     库按 365 天涨;缺下限钳位 → 把 180 写成 18 会**不可逆**删掉玩家还看得见的战报。
"""

from __future__ import annotations

import pathlib

import pytest

from pandorapy import dbguard, kafka_topics
from pandorapy.services.battle_result import conf as bconf


def _cfg(**battle) -> bconf.Config:
    """构造一份只填了 battle 段的配置并跑 apply_defaults。"""
    cfg = bconf.Config.model_validate({"battle": battle})
    cfg.apply_defaults()
    return cfg


# ── 默认值 ───────────────────────────────────────────────────────────────────


def test_defaults_match_go() -> None:
    """空配置的每个默认值都与 Go 的 Defaults() 同值。"""
    cfg = _cfg()
    b = cfg.battle
    assert b.elo_k_factor == 32
    assert b.base_mmr == 1500
    assert b.consume_topics == [
        kafka_topics.TOPIC_BATTLE_RESULT,
        kafka_topics.TOPIC_DS_LIFECYCLE,
    ]
    assert b.outbox_publish_interval_td().total_seconds() == 2
    assert b.outbox_batch_size == 128
    assert b.terminal_release_interval_td().total_seconds() == 2
    assert b.terminal_release_batch_size == 128
    assert b.terminal_release_grace_td().total_seconds() == 15
    assert b.drop_publish_interval_td().total_seconds() == 2
    assert b.drop_batch_size == 128
    assert b.history_retention_days == 180
    assert b.retention_sweep_interval_td().total_seconds() == 3600
    assert b.retention_sweep_batch == 200
    assert cfg.server.grpc.addr == ":20022"
    assert cfg.server.http.addr == ":21022"
    # ds_auth.Defaults()
    assert cfg.ds_auth.authority_mode == "legacy"
    assert cfg.ds_auth.issuer == "pandora-ds-control"
    assert cfg.ds_auth.audience == "pandora-ds"
    assert cfg.ds_auth.active_heartbeat_max_age_td().total_seconds() == 30
    # mode / secret 留空即"不启用",**刻意不填默认**。
    assert cfg.ds_auth.mode == ""
    assert cfg.ds_auth.secret == ""


@pytest.mark.parametrize(
    ("field", "bad", "want"),
    [
        ("elo_k_factor", -1, 32),
        ("elo_k_factor", 0, 32),
        ("base_mmr", 0, 1500),
        ("outbox_batch_size", -5, 128),
        ("drop_batch_size", 0, 128),
        ("terminal_release_batch_size", 0, 128),
        ("retention_sweep_batch", 0, 200),
    ],
)
def test_non_positive_falls_back(field: str, bad: int, want: int) -> None:
    """判据是 `<= 0`(本服没有 inventory 那种"负值 = 显式关闭"的字段)。"""
    cfg = _cfg(**{field: bad})
    assert getattr(cfg.battle, field) == want


# ── 保留期钳位(本服独有,两侧都会造成不可逆后果)────────────────────────────


@pytest.mark.parametrize(
    ("configured", "want"),
    [
        (0, 180),  # 未配 → 上限
        (365, 180),  # ★ 超上限钳回:配 365 不得原样生效
        (180, 180),
        (90, 90),
        (30, 30),
        (18, 30),  # ★ 低于下限钳到 30:本服是真删,写错一个数量级不可逆
        (1, 30),
    ],
)
def test_history_retention_days_clamped(configured: int, want: int) -> None:
    assert _cfg(history_retention_days=configured).battle.history_retention_days == want


# ── retention_mode:本服留空 = delete ────────────────────────────────────────


def test_retention_mode_empty_is_delete() -> None:
    """★ 本服特例:留空即真删,与 dbguard 全局默认(report_only)相反。"""
    assert _cfg().battle.retention_mode_parsed() is dbguard.Mode.DELETE


@pytest.mark.parametrize("raw", ["report_only", "report", "report-only"])
def test_retention_mode_report_aliases(raw: str) -> None:
    """Go 的 ParseMode 认这三个别名;少认一个会让一份在 Go 上跑得好的 yaml 在这里拒启。"""
    assert _cfg(retention_mode=raw).battle.retention_mode_parsed() is dbguard.Mode.REPORT_ONLY


def test_retention_mode_explicit_delete() -> None:
    assert _cfg(retention_mode="delete").battle.retention_mode_parsed() is dbguard.Mode.DELETE


def test_retention_mode_typo_rejected_at_startup() -> None:
    """拼错必须 fail-fast,**不能**静默回落 —— 否则六个月口径静默失效,库继续涨。"""
    cfg = _cfg(retention_mode="delet")
    with pytest.raises(ValueError):
        cfg.battle.validate_retention_mode()
    # 二重保险:真走到 parsed() 时回落到"不删"(任何不确定都不删)。
    assert cfg.battle.retention_mode_parsed() is dbguard.Mode.REPORT_ONLY


# ── 掉落条数上限 ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("configured", "want"),
    [(0, 32), (-1, 32), (10, 10), (46, 46), (100, 46)],
)
def test_max_drops_per_player(configured: int, want: int) -> None:
    """硬上限 46 = VARCHAR(512) 能装下的最大条数;放开会让整场结算 insert 回滚。"""
    b = bconf.BattleConf(max_drop_per_player=configured)
    assert b.max_drops_per_player() == want


@pytest.mark.parametrize(
    ("configured", "want"),
    [(0, 10), (5, 5), (46, 46), (999, 46)],
)
def test_max_pickup_count_per_fact(configured: int, want: int) -> None:
    """★ 判据是 `== 0` 而不是 `> 0`,且再夹紧到 46(与 Go 逐字一致)。"""
    assert bconf.BattleConf(max_pickup_count_per_fact=configured).max_pickup_count_per_fact_or_default() == want


def test_progress_accessors_defaults() -> None:
    """*_or_default 必须在**任何**构造路径上都有安全上限(含直建 BattleConf)。"""
    b = bconf.BattleConf()
    assert b.max_progress_batch_or_default() == 256
    assert b.max_progress_seq_per_match_or_default() == 100_000
    assert b.max_kill_count_per_fact_or_default() == 100
    assert b.max_progress_exp_per_match_or_default() == 1_000_000
    assert b.max_progress_items_per_match_or_default() == 500
    assert b.max_progress_exp_per_player_or_default() == 200_000
    assert b.max_progress_items_per_player_or_default() == 100
    assert b.max_progress_kills_per_player_or_default() == 1000
    assert b.progress_publish_interval_or_default().total_seconds() == 1
    assert b.progress_batch_size_or_default() == 128


# ── Model-B 入口收敛闸 ───────────────────────────────────────────────────────


def _redis_authority_cfg(**battle) -> bconf.Config:
    base = {
        "ds_allocator_addr": "127.0.0.1:20020",
        "terminal_release_grace": "15s",
        "consume_topics": [kafka_topics.TOPIC_DS_LIFECYCLE],
    }
    base.update(battle)
    cfg = bconf.Config.model_validate(
        {
            "battle": base,
            "ds_auth": {
                "mode": "enforce",
                "authority_mode": "redis",
                "active_heartbeat_max_age": "30s",
                "fence": {
                    "etcd_endpoints": ["127.0.0.1:2379"],
                    "keyset_revision": "r1",
                },
            },
        }
    )
    cfg.apply_defaults()
    return cfg


def test_redis_authority_ok() -> None:
    cfg = _redis_authority_cfg()
    cfg.ds_auth.validate_redis_fence()
    cfg.validate_redis_authority_ingress()


def test_redis_authority_forbids_unauthenticated_topic() -> None:
    """★ 这是安全闸:battle.result 消息没有可核验凭据,继续订阅 = 绕过授权的第二结算入口。"""
    cfg = _redis_authority_cfg(
        consume_topics=[kafka_topics.TOPIC_DS_LIFECYCLE, kafka_topics.TOPIC_BATTLE_RESULT]
    )
    with pytest.raises(ValueError, match="forbids unauthenticated topic"):
        cfg.validate_redis_authority_ingress()


def test_redis_authority_requires_ds_allocator_addr() -> None:
    cfg = _redis_authority_cfg(ds_allocator_addr="")
    with pytest.raises(ValueError, match="ds_allocator_addr"):
        cfg.validate_redis_authority_ingress()


@pytest.mark.parametrize("grace", ["4s", "3m"])
def test_redis_authority_grace_range(grace: str) -> None:
    """grace 落在 [5s,2m] 之外 = 配置错误,会让**每一场**正常结算失败而监控面全绿。"""
    cfg = _redis_authority_cfg(terminal_release_grace=grace)
    with pytest.raises(ValueError, match=r"\[5s,2m\]"):
        cfg.validate_redis_authority_ingress()


def test_legacy_authority_skips_ingress_checks() -> None:
    """legacy 档下这道闸整条不生效(否则 dev yaml 会被自己的 prod 规则拒掉)。"""
    cfg = _cfg(consume_topics=[kafka_topics.TOPIC_BATTLE_RESULT])
    cfg.validate_redis_authority_ingress()  # 不抛


# ── fence 配置闸 ─────────────────────────────────────────────────────────────


def test_fence_requires_enforce_mode() -> None:
    cfg = bconf.Config.model_validate(
        {"ds_auth": {"mode": "permissive", "authority_mode": "redis"}}
    )
    cfg.apply_defaults()
    with pytest.raises(ValueError, match="requires mode=enforce"):
        cfg.ds_auth.validate_redis_fence()


def test_fence_requires_endpoints_and_revision() -> None:
    cfg = bconf.Config.model_validate(
        {"ds_auth": {"mode": "enforce", "authority_mode": "redis"}}
    )
    cfg.apply_defaults()
    with pytest.raises(ValueError, match="fence.etcd_endpoints"):
        cfg.ds_auth.validate_redis_fence()

    cfg = bconf.Config.model_validate(
        {
            "ds_auth": {
                "mode": "enforce",
                "authority_mode": "redis",
                "fence": {"etcd_endpoints": ["127.0.0.1:2379"]},
            }
        }
    )
    cfg.apply_defaults()
    with pytest.raises(ValueError, match="keyset_revision"):
        cfg.ds_auth.validate_redis_fence()


def test_legacy_skips_fence_checks() -> None:
    cfg = _cfg()
    cfg.ds_auth.validate_redis_fence()  # 不抛


# ── 已删除键必须拒启 ─────────────────────────────────────────────────────────


def test_removed_monster_exp_key_rejected() -> None:
    """`monster_exp` 已于 2026-08-04 从配置移除(数值权威改为 role_level 表)。

    残留该键必须**拒绝启动**而不是静默忽略:静默忽略会让运维以为经验还按 yaml 走,
    而实际读的是配置表 —— 两处数值不一致时没人会想到去看 yaml。
    """
    with pytest.raises(Exception):  # noqa: B017 —— pydantic ValidationError
        bconf.Config.model_validate({"battle": {"monster_exp": {"2001": 40}}})


# ── 真实 yaml ────────────────────────────────────────────────────────────────


def test_loads_repo_dev_yaml(repo_root: pathlib.Path) -> None:
    """能用**与 Go 同一份** etc/battle_result-dev.yaml 起来 —— 这是本批次的交付判据。"""
    path = repo_root / "services/battle/battle_result/etc/battle_result-dev.yaml"
    cfg = bconf.Config.load(str(path))
    assert cfg.server.grpc.addr == ":20022"
    assert cfg.server.http.addr == ":21022"
    assert cfg.node.mysql_client.dsn.startswith("pandora:")
    assert cfg.kafka.brokers == ["127.0.0.1:9093"]
    assert cfg.kafka.group_id == "pandora-battle-result"
    assert cfg.battle.history_retention_days == 180
    assert cfg.battle.retention_mode_parsed() is dbguard.Mode.DELETE
    # dev 是 legacy 档 —— main.py 的 authority_mode 闸只拦 redis。
    assert not cfg.ds_auth.authority_mode_redis()
    cfg.ds_auth.validate_redis_fence()
    cfg.validate_redis_authority_ingress()
    cfg.battle.validate_retention_mode()


def test_loads_repo_prod_example_yaml(repo_root: pathlib.Path) -> None:
    """prod 样例是 authority_mode=redis + 只订阅 ds.lifecycle,三道闸都要过。

    ★ 这份 yaml 在 Python 上会被 main.py 的闸⑦拒启(Model-B 未实现),
      但**配置层校验必须先全过** —— 否则真正的原因会被一个假的配置错误盖住。
    """
    path = repo_root / "services/battle/battle_result/etc/battle_result-prod.yaml.example"
    cfg = bconf.Config.load(str(path))
    assert cfg.ds_auth.authority_mode_redis()
    assert cfg.battle.consume_topics == [kafka_topics.TOPIC_DS_LIFECYCLE]
    cfg.ds_auth.validate_redis_fence()
    cfg.validate_redis_authority_ingress()
    cfg.battle.validate_retention_mode()
