"""player 服务入口(main.py)的启动闸。

覆盖的是"起不来 / 起错了"这一族缺陷 —— 它们全都**不会**在业务测试里露头,而且事件名
本身就是契约(Loki 告警和运维手册按它建),改一个字等于静默失去那条告警的覆盖。

★ 用例按 **Go main.go 的闸序**排列。顺序也是契约:配置表闸必须在 MySQL 之前,
  否则"表目录没挂上"会先表现为一条 MySQL 连接错误,排查方向整个歪掉。

★ 无 MySQL / kafka 的机器上也必须能跑:这里只覆盖**触碰外部依赖之前**的闸,以及两个
  纯函数闸(部署策略机械门禁、消费者构建)。需要真库的部分在 repo 层测试里。
"""

from __future__ import annotations

import pathlib

import pytest

from pandorapy import kafka_topics
from pandorapy import log as plog
from pandorapy.services.player import conf as pconf
from pandorapy.services.player import main as pmain

GO_MAIN = "services/account/player/cmd/player/main.go"


class _Recorder:
    """把 structlog 事件名收集下来 —— 断言"哪道闸命中",不只是"exit 1"。"""

    def __init__(self) -> None:
        self.events: list[str] = []

    def __getattr__(self, _level):  # noqa: ANN001
        def emit(event, **_kw):  # noqa: ANN001, ANN003
            self.events.append(event)

        return emit


def _run(yaml_path: pathlib.Path) -> tuple[int, list[str]]:
    recorder = _Recorder()
    real_setup, real_get = plog.setup, plog.get
    plog.setup = lambda *_a, **_k: recorder
    plog.get = lambda *_a, **_k: recorder
    try:
        code = pmain.main(["-conf", str(yaml_path)])
    finally:
        plog.setup, plog.get = real_setup, real_get
    return code, recorder.events


def _yaml(tmp_path: pathlib.Path, body: str) -> pathlib.Path:
    path = tmp_path / "player.yaml"
    path.write_text(body, encoding="utf-8")
    return path


# 除被测项外必须合法的最小骨架(§7:省了某项会先被前面的闸拦下,测的就不是目标闸了)。
_BASE = "node:\n  node_id: 1\n"


# ── 闸序与事件名必须与 Go 源码对齐 ───────────────────────────────────────────


def test_gate_event_names_exist_in_go_source(repo_root: pathlib.Path) -> None:
    """★ 事件名逐字从 Go 源码里找,而不是抄一份常量。

    抄一份的话 Go 改了名字这个测试照样绿(它验的是"我抄的等于我抄的")。
    """
    src = (repo_root / GO_MAIN).read_text(encoding="utf-8")
    for event in (
        "abs_conf_path_failed",
        "config_load_failed",
        "config_scan_failed",
        "cellroute_init_failed",
        "player_retention_mode_invalid",
        "configtable_dir_required",
        "configtable_load_failed",
        "configtable_load_warning",
        "player_level_exp_loaded",
        "mysql_dsn_required",
        "mysql_connected",
        "mysql_strict_mode_required",
        "player_experience_schema_invalid",
        "player_equipment_schema_invalid",
        "player_experience_level_invalid",
        "instance_ownership_checker_grpc",
        "instance_ownership_checker_missing",
        "ds_auth_guard_init_failed",
        "ds_callback_guard_ready",
        "player_push_producer_init_failed",
        "player_push_producer_ready",
        "player_push_writer_lease_mode_invalid",
        "player_push_writer_lease_rollingupdate_without_enforce",
        "player_push_writer_lease_strategy_checked",
        "player_push_writer_lease_strategy_annotation_missing",
        "player_push_writer_lease_strategy_unknown",
        "player_push_writer_lease_endpoints_missing",
        "player_push_writer_lease_start_failed",
        "player_push_writer_lease_started",
        "player_push_writer_lease_disabled",
        "kafka_brokers_empty",
        "consume_topics_empty",
        "unknown_consume_topic_skipped",
        "dlq_producer_init_failed",
        "kafka_consumer_new_failed",
        "kafka_consumer_ready",
        "no_valid_consumer",
        "service_ready",
        "app_run_failed",
    ):
        assert f'"{event}"' in src, f"Go 源码里找不到事件名 {event}"


def test_python_main_mentions_every_gate_event() -> None:
    """Python 侧也必须逐条出现同名事件 —— 漏一条 = Loki 上那条告警对 Python 副本失效。"""
    src = pathlib.Path(pmain.__file__).read_text(encoding="utf-8")
    for event in (
        "abs_conf_path_failed",
        "config_load_failed",
        "config_scan_failed",
        "cellroute_init_failed",
        "player_retention_mode_invalid",
        "configtable_dir_required",
        "configtable_load_failed",
        "mysql_dsn_required",
        "mysql_connect_failed",
        "mysql_strict_mode_required",
        "player_experience_schema_invalid",
        "player_equipment_schema_invalid",
        "player_experience_level_invalid",
        "ds_auth_guard_init_failed",
        "session_gate_redis_failed",
        "session_gate_endpoint_required",
        "player_push_writer_lease_mode_invalid",
        "player_push_writer_lease_rollingupdate_without_enforce",
        "player_push_writer_lease_strategy_annotation_missing",
        "player_push_writer_lease_endpoints_missing",
        "player_push_writer_lease_start_failed",
        "kafka_brokers_empty",
        "consume_topics_empty",
        "dlq_producer_init_failed",
        "kafka_consumer_new_failed",
        "no_valid_consumer",
        "app_run_failed",
    ):
        assert f'"{event}"' in src, f"main.py 里找不到事件名 {event}"


# ── 逐条闸 ───────────────────────────────────────────────────────────────────


def test_missing_conf_file_exits_nonzero(tmp_path: pathlib.Path) -> None:
    code, events = _run(tmp_path / "nope.yaml")
    assert code == 1
    assert "config_load_failed" in events


def test_cell_route_mode_is_rejected(tmp_path: pathlib.Path) -> None:
    """配了多 Cell 但 Python 只实现单 Cell → 拒启(不是忽略 + WARN)。

    忽略的后果是所有玩家静默落在单 Cell 上,与配置意图不符且零信号。事件名沿用 Go 的
    cellroute_init_failed —— 同一个失败在两个实现上要能被同一条告警抓到。
    """
    path = _yaml(tmp_path, _BASE + 'cell_route:\n  mode: "static"\n')
    code, events = _run(path)
    assert code == 1
    assert "cellroute_init_failed" in events


def test_retention_mode_typo_is_rejected_before_touching_mysql(
    tmp_path: pathlib.Path,
) -> None:
    """★ 闸序:retention_mode 校验在**触碰 MySQL 之前**。

    拼错一个字母会让运维以为开了清理、实际一行没删。放到 MySQL 之后就得先连上库才报,
    而配置错误应该暴露在发布阶段。
    """
    path = _yaml(tmp_path, _BASE + 'player:\n  retention_mode: "delet"\n')
    code, events = _run(path)
    assert code == 1
    assert "player_retention_mode_invalid" in events
    assert "mysql_dsn_required" not in events  # 还没走到 MySQL 那道


def test_configtable_dir_required(tmp_path: pathlib.Path) -> None:
    """player 强依赖玩家等级经验表,**不保留 YAML 兜底曲线**。

    一份可能与客户端漂移的兜底数值参与升级结算,比拒掉一次启动危险得多。
    """
    path = _yaml(tmp_path, _BASE)
    code, events = _run(path)
    assert code == 1
    assert "configtable_dir_required" in events


def test_configtable_load_failure_is_fatal(tmp_path: pathlib.Path) -> None:
    path = _yaml(tmp_path, _BASE + f'config_table:\n  dir: "{tmp_path.as_posix()}/nope"\n')
    code, events = _run(path)
    assert code == 1
    assert "configtable_load_failed" in events


def test_mysql_dsn_required_after_configtable(
    tmp_path: pathlib.Path, configtable_dist: pathlib.Path
) -> None:
    """★ 闸序:配置表先于 MySQL。

    反过来的话,"表目录没挂上"会先表现为一条 MySQL 连接错误,排查方向整个歪掉。
    """
    if not configtable_dist.is_dir():
        pytest.skip("configtable/dist 不在")
    path = _yaml(tmp_path, _BASE + f'config_table:\n  dir: "{configtable_dist.as_posix()}"\n')
    code, events = _run(path)
    assert code == 1
    assert "player_level_exp_loaded" in events  # 配置表这道过了
    assert "mysql_dsn_required" in events
    assert "service_ready" not in events


# ── 部署策略机械门禁(纯函数,可直接测)───────────────────────────────────


def test_rollingupdate_without_enforce_is_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    """★ RollingUpdate × 非 enforce = 滚动重叠期两个发布器并发。

    PlayerExperienceEvent 携带**绝对值快照**,旧快照后到会覆盖新的 —— 玩家看到等级
    经验条倒退,而事件里没有 revision、ts_ms 是各副本墙钟不足以判序。
    """
    monkeypatch.setenv("PANDORA_DEPLOY_STRATEGY", "RollingUpdate")
    recorder = _Recorder()
    assert pmain._check_push_lease_deploy_strategy(recorder, "off") is False
    assert "player_push_writer_lease_rollingupdate_without_enforce" in recorder.events


def test_rollingupdate_with_enforce_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PANDORA_DEPLOY_STRATEGY", "RollingUpdate")
    recorder = _Recorder()
    assert pmain._check_push_lease_deploy_strategy(recorder, "enforce") is True
    assert "player_push_writer_lease_strategy_checked" in recorder.events


def test_recreate_strategy_passes_even_without_enforce(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """单副本 Recreate 下 mode=off 是合法形态 —— 不能一刀切要求 enforce。"""
    monkeypatch.setenv("PANDORA_DEPLOY_STRATEGY", "Recreate")
    recorder = _Recorder()
    assert pmain._check_push_lease_deploy_strategy(recorder, "off") is True


def test_managed_k8s_without_annotation_is_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    """受管 k8s 内必须注入策略 annotation,否则无法机械校验 RollingUpdate×非 enforce。"""
    monkeypatch.delenv("PANDORA_DEPLOY_STRATEGY", raising=False)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    recorder = _Recorder()
    assert pmain._check_push_lease_deploy_strategy(recorder, "enforce") is False
    assert "player_push_writer_lease_strategy_annotation_missing" in recorder.events


def test_bare_metal_is_warn_not_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    """★ 方向:非 k8s(本机裸跑 / dev)是 **WARN 放行**,改成 fail-fast 会让 dev 起不来。"""
    monkeypatch.delenv("PANDORA_DEPLOY_STRATEGY", raising=False)
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    recorder = _Recorder()
    assert pmain._check_push_lease_deploy_strategy(recorder, "off") is True
    assert "player_push_writer_lease_strategy_unknown" in recorder.events


# ── 消费者构建(纯函数,可直接测)────────────────────────────────────────


def _cfg(**player_overrides) -> pconf.Config:
    cfg = pconf.Config.model_validate({"player": player_overrides})
    cfg.apply_defaults()
    return cfg


def test_kafka_brokers_empty_is_fatal() -> None:
    """player 不消费 player.update 就无法做幂等 UpdateMMR —— 这是强依赖,不是弱依赖。"""
    recorder = _Recorder()
    assert pmain._build_consumers(_cfg(), object(), recorder) is None
    assert "kafka_brokers_empty" in recorder.events


def test_consume_topics_empty_is_fatal() -> None:
    cfg = _cfg()
    cfg.kafka.brokers = ["127.0.0.1:9093"]
    cfg.player.consume_topics = []
    recorder = _Recorder()
    assert pmain._build_consumers(cfg, object(), recorder) is None
    assert "consume_topics_empty" in recorder.events


def test_unknown_topic_is_skipped_then_no_valid_consumer() -> None:
    """★ 未知 topic 是 **WARN 跳过**(不是拒启);但全部无效后必须 fail-fast。

    只做前者会让 `consume_topics: ["typo"]` 静默起来且一条 MMR 都不消费。
    """
    cfg = _cfg()
    cfg.kafka.brokers = ["127.0.0.1:9093"]
    cfg.player.consume_topics = ["pandora.player.typo"]
    recorder = _Recorder()
    assert pmain._build_consumers(cfg, object(), recorder) is None
    assert "unknown_consume_topic_skipped" in recorder.events
    assert "no_valid_consumer" in recorder.events


def test_dlq_topic_naming_matches_infra_spec() -> None:
    assert (
        kafka_topics.build_dlq_topic(kafka_topics.TOPIC_PLAYER_UPDATE)
        == "pandora.dlq.player.update"
    )


def test_experience_topic_is_not_player_update() -> None:
    """★ §21:经验事件绝不能发 pandora.player.update。

    旧 player 副本消费该 topic 时不看 event_type header,会把经验事件按 MMR 解码
    **污染段位**。这条常量写错没有任何运行期信号 —— 只有段位莫名其妙地变。
    """
    assert kafka_topics.TOPIC_PLAYER_EXPERIENCE == "pandora.player.experience"
    assert kafka_topics.TOPIC_PLAYER_EXPERIENCE != kafka_topics.TOPIC_PLAYER_UPDATE


def test_dlq_retry_policy_matches_go() -> None:
    """infra.md §4.4「失败 3 次进 DLQ」;间隔 500ms 与 Go 的 dlqRetryBackoff 同值。"""
    assert pmain.DLQ_MAX_RETRIES == 3
    assert pmain.DLQ_RETRY_BACKOFF_SEC == 0.5


def test_push_lease_election_name_matches_go(repo_root: pathlib.Path) -> None:
    """★ 选举名必须逐字相同。

    改了它,滚动升级期新旧副本会各自在**不同的 key 前缀**下选举,两个都当选,
    单写者保证凭空消失且零报错。
    """
    src = (repo_root / GO_MAIN).read_text(encoding="utf-8")
    assert f'"{pmain.PUSH_LEASE_ELECTION}"' in src
