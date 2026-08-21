"""team 服务入口(main.py)的启动闸 + conf 默认值 parity。

覆盖的是"起不来 / 起错了"这一族缺陷 —— 它们全都**不会**在业务测试里露头,而且事件名
本身就是契约(Loki 告警和运维手册按它建),改一个字等于静默失去那条告警的覆盖。

★ 用例按 **Go main.go 的闸序**排列。顺序也是契约:join_policy / offline_leave 两道
  配置校验必须在 Redis 之前,否则"配置写错了"会先表现成一条 Redis 连接错误,
  排查方向整个歪掉。

★ 无 Redis / kafka 的机器上也必须能跑:这里只覆盖**触碰外部依赖之前**的闸。
"""

from __future__ import annotations

import pathlib

import pytest

from pandorapy import log as plog
from pandorapy.services.team import conf as tconf
from pandorapy.services.team import main as tmain

GO_MAIN = "services/matchmaking/team/cmd/team/main.go"
GO_CONF = "services/matchmaking/team/internal/conf/conf.go"

# Go main.go 里逐字出现的启动事件名。Python 侧必须一条不少地也出现 ——
# 漏一条 = Loki 上那条告警对 Python 副本失效,而两边都不报错。
SHARED_GATE_EVENTS = (
    "service_starting",
    "abs_conf_path_failed",
    "config_load_failed",
    "config_scan_failed",
    "team_join_policy_invalid",
    "team_offline_leave_config_invalid",
    "redis_endpoint_required",
    "redis_ping_failed",
    "redis_connected",
    "kafka_producer_required_but_unavailable",
    "kafka_producer_ready",
    "kafka_producer_disabled_dev_only",
    "team_rate_quota_ready",
    "match_resume_signer_init_failed",
    "match_resume_signer_missing",
    "match_client_ready",
    "matchmaker_addr_empty",
    "offline_watch_init_failed",
    "offline_leave_without_kafka",
    "offline_watch_consumer_init_failed",
    "offline_leave_enabled",
    "offline_leave_disabled",
    "cellroute_init_failed",
    "ds_auth_guard_init_failed",
    "match_call_replay_store_init_failed",
    "match_call_verifier_init_failed",
    "match_call_verifier_ready",
    "match_call_verifier_disabled",
    "ds_callback_guard_ready",
    "service_ready",
    "app_run_failed",
)

# Python 侧**独有**的三条:Go 用 `Must*` 直接 panic(MustProvideSnowflakeN /
# sessiongate.MustBuild / grpcserver.MustNewServer),没有事件名。Python 走
# 「打事件 + return 1」而不是抛裸异常 —— 方向相同(都拒启),但多了可 grep 的证据。
# 刻意不去 Go 源码里找它们,那会让本测试永远红。
PY_ONLY_GATE_EVENTS = (
    "snowflake_init_failed",
    "session_gate_init_failed",
    "grpc_server_init_failed",
)


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
        code = tmain.main(["-conf", str(yaml_path)])
    finally:
        plog.setup, plog.get = real_setup, real_get
    return code, recorder.events


def _yaml(tmp_path: pathlib.Path, body: str) -> pathlib.Path:
    path = tmp_path / "team.yaml"
    path.write_text(body, encoding="utf-8")
    return path


# 除被测项外必须合法的最小骨架(§7:省了 node_id 会先被 snowflake 号段闸拦下,
# 测的就不是目标闸了)。
_BASE = "node:\n  node_id: 1\n"
# 带 Redis 端点的骨架:用于测「Redis 之后」的闸序断言(端点填了才走得到 ping)。
_BASE_REDIS = _BASE + '  redis_client:\n    host: "127.0.0.1:1"\n'


# ── 事件名契约 ───────────────────────────────────────────────────────────────


def test_gate_event_names_exist_in_go_source(repo_root: pathlib.Path) -> None:
    """★ 事件名逐字从 Go 源码里找,而不是抄一份常量。

    抄一份的话 Go 改了名字这个测试照样绿(它验的是"我抄的等于我抄的")。
    """
    src = (repo_root / GO_MAIN).read_text(encoding="utf-8")
    for event in SHARED_GATE_EVENTS:
        assert f'"{event}"' in src, f"Go 源码里找不到事件名 {event}"


def test_python_main_mentions_every_gate_event() -> None:
    """Python 侧也必须逐条出现同名事件。

    只看代码：注释/docstring 里写着事件名不代表真的打了这条日志
    （理由见 tests/srcprobe.py）。
    """
    from tests.srcprobe import module_code_text

    src = module_code_text(tmain)
    for event in SHARED_GATE_EVENTS + PY_ONLY_GATE_EVENTS:
        assert f'"{event}"' in src, f"main.py 里找不到事件名 {event}"


# ── 逐条闸 ───────────────────────────────────────────────────────────────────


def test_missing_conf_file_exits_nonzero(tmp_path: pathlib.Path) -> None:
    code, events = _run(tmp_path / "nope.yaml")
    assert code == 1
    assert "config_load_failed" in events


def test_broken_yaml_reports_config_load_not_scan(tmp_path: pathlib.Path) -> None:
    """yaml 语法坏 = Go 的 `c.Load()` 失败 = config_load_failed。

    落到 config_scan_failed 会把排查方向指向"模型定义不对",而其实是 yaml 写坏了。
    """
    path = _yaml(tmp_path, "node:\n  node_id: [unclosed\n")
    code, events = _run(path)
    assert code == 1
    assert "config_load_failed" in events
    assert "config_scan_failed" not in events


def test_cell_route_mode_is_rejected(tmp_path: pathlib.Path) -> None:
    """配了多 Cell 但 Python 只实现单 Cell → 拒启(不是忽略 + WARN)。

    忽略的后果是所有玩家静默落在单 Cell 上,与配置意图不符且零信号。事件名沿用 Go 的
    cellroute_init_failed —— 同一个失败在两个实现上要能被同一条告警抓到。
    """
    path = _yaml(tmp_path, _BASE + 'cell_route:\n  mode: "static"\n')
    code, events = _run(path)
    assert code == 1
    assert "cellroute_init_failed" in events


def test_join_policy_typo_is_rejected_before_touching_redis(
    tmp_path: pathlib.Path,
) -> None:
    """★ 闸序:join_policy 校验在**触碰 Redis 之前**。

    拼错一个字母(如 "aproval")若被猜成 open,会让全服队伍对任何人敞开 —— 静默的
    权限放大。放到 Redis 之后就得先连上库才报,而配置错误应该暴露在发布阶段。
    """
    path = _yaml(tmp_path, _BASE_REDIS + 'team:\n  join_policy: "aproval"\n')
    code, events = _run(path)
    assert code == 1
    assert "team_join_policy_invalid" in events
    assert "redis_ping_failed" not in events  # 还没走到 Redis 那道


@pytest.mark.parametrize(
    ("body", "why"),
    [
        (
            'team:\n  offline_leave:\n    enabled: true\n  matchmaker_addr: "127.0.0.1:1"\n',
            "缺 locator_addr:没有判定依据,功能静默不生效",
        ),
        (
            'team:\n  offline_leave:\n    enabled: true\n  locator_addr: "127.0.0.1:1"\n',
            "缺 matchmaker_addr:没有对局闸门,有拆掉正在打的队伍的风险",
        ),
    ],
)
def test_offline_leave_missing_dependency_is_rejected(
    tmp_path: pathlib.Path, body: str, why: str
) -> None:
    """开了离线自动退队却缺依赖地址 → 拒启,而不是半截接线跑起来。"""
    path = _yaml(tmp_path, _BASE_REDIS + body)
    code, events = _run(path)
    assert code == 1, why
    assert "team_offline_leave_config_invalid" in events
    assert "redis_ping_failed" not in events


def test_offline_leave_disabled_needs_no_dependency(tmp_path: pathlib.Path) -> None:
    """关闭态不校验依赖 —— 否则「不用这个功能」的部署反而被配置校验卡住。"""
    path = _yaml(tmp_path, _BASE + "team:\n  offline_leave:\n    enabled: false\n")
    code, events = _run(path)
    assert code == 1
    assert "team_offline_leave_config_invalid" not in events
    assert "redis_endpoint_required" in events  # 停在下一道闸


def test_redis_endpoint_required(tmp_path: pathlib.Path) -> None:
    """队伍状态的唯一权威在 Redis,端点缺失一律拒启。"""
    path = _yaml(tmp_path, _BASE)
    code, events = _run(path)
    assert code == 1
    assert "redis_endpoint_required" in events


def test_redis_cluster_addrs_only_passes_endpoint_gate(tmp_path: pathlib.Path) -> None:
    """★ 只填 addrs(Cluster / Sentinel)也算配了端点。

    按 `host == ""` 单条判会把纯 Cluster 部署拒在门外,而 yaml 明明是合法的。
    """
    path = _yaml(
        tmp_path,
        _BASE + '  redis_client:\n    addrs: ["127.0.0.1:1"]\n',
    )
    code, events = _run(path)
    assert code == 1
    assert "redis_endpoint_required" not in events
    assert "redis_ping_failed" in events  # 端点闸放行,停在连不通那道


def test_static_node_id_zero_is_rejected(tmp_path: pathlib.Path) -> None:
    """★ node_id=0 是 UE DS 本地发号器的保留号,用它发号会与 DS 本地铸的 ID 逐位相同。

    漏配 node_id 时默认值恰好是 0 —— 不拦的话"忘了配"会稳定落进最危险的那一格,
    且只在数据层面表现为重号。本闸在 Redis ping **之后**(与 Go 同序),所以这里
    只能靠"不是被 redis 拦下的"来间接确认;真正的判据是 snowflake_init_failed。
    """
    from pandorapy import snowflake_etcd

    with pytest.raises(ValueError, match="node_id=0"):
        # 直接打发号器的号段闸:main 里那条链要先连上 Redis 才走得到,
        # 而本用例要验的是"0 会被拒",不是"Redis 通不通"。
        import asyncio

        asyncio.run(
            snowflake_etcd.provide_node([], "team", 0, "static", on_lost=lambda _h: None)
        )


# ── conf 默认值必须与 Go 的 Defaults() 逐字段相同 ────────────────────────────


def test_defaults_match_go_source(repo_root: pathlib.Path) -> None:
    """★ 默认值从 Go 源码里抓,而不是抄一份常量再自比。

    默认值分叉 = 同一份 yaml 喂两个实现行为不同,而**两边都不报错**。
    """
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")
    for snippet in (
        "c.Team.InviteTTL = config.Duration(60 * time.Second)",
        "c.Team.DisbandedRetention = config.Duration(5 * time.Minute)",
        "c.Team.ActiveTTL = config.Duration(60 * time.Minute)",
        "c.Team.MaxMembers = 5",
        "c.Team.OptimisticRetry = 3",
        'c.Team.InvitePushMode = "dual"',
        "c.Team.MaxPendingInvites = 10",
        "c.Team.JoinPolicy = JoinPolicyApproval",
        "c.Team.MaxOpenTeamsPerQuery = 10",
        "c.Team.MaxApplicationsPerTeam = 10",
        "c.Team.RateQuotaPerMin = 12",
        "c.Team.ApplyTTL = config.Duration(120 * time.Second)",
        "c.Team.OfflineLeave.Threshold = config.Duration(180 * time.Second)",
        "c.Team.OfflineLeave.CheckInterval = config.Duration(15 * time.Second)",
        "c.Team.OfflineLeave.Budget = 200",
        "c.Team.OfflineLeave.KafkaPartitions = 3",
        'c.Server.Grpc.Addr = ":20010"',
        'c.Server.Http.Addr = ":21010"',
    ):
        assert snippet in src, f"Go Defaults() 里找不到 {snippet}"


def test_python_defaults_equal_go_defaults() -> None:
    """空 yaml 下 Python 的每个默认值都要等于上面那批 Go 常量。"""
    cfg = tconf.Config()
    cfg.apply_defaults()
    t = cfg.team
    assert t.invite_ttl_td().total_seconds() == 60
    assert t.disbanded_retention_td().total_seconds() == 5 * 60
    assert t.active_ttl_td().total_seconds() == 60 * 60
    assert t.max_members == 5
    assert t.optimistic_retry == 3
    assert t.invite_push_mode == "dual"
    assert t.max_pending_invites == 10
    assert t.join_policy == tconf.JOIN_POLICY_APPROVAL
    assert t.max_open_teams_per_query == 10
    assert t.max_applications_per_team == 10
    assert t.rate_quota_per_min == 12
    assert t.apply_ttl_td().total_seconds() == 120
    assert t.offline_leave.threshold_td().total_seconds() == 180
    assert t.offline_leave.check_interval_td().total_seconds() == 15
    assert t.offline_leave.budget == 200
    assert t.offline_leave.kafka_partitions == 3
    assert cfg.server.grpc.addr == ":20010"
    assert cfg.server.http.addr == ":21010"


def test_negative_values_are_not_defaulted() -> None:
    """★ 判据是 `== 0` 而不是 `<= 0` —— 连符号都要跟 Go 一样。

    Go 的 `config.Duration` 是纳秒整数,`max_members: -1` / `rate_quota_per_min: -1`
    **不会**被 Defaults 覆盖(前者让队伍恒"满",后者是"关闭频率配额"的显式取值)。
    Python 若写成 `<= 0`,同一份 yaml 会让 Python 得到 5 / 12 而 Go 得到 -1,
    **两边都不报错**。
    """
    cfg = tconf.Config.model_validate(
        {"team": {"max_members": -1, "rate_quota_per_min": -1, "invite_ttl": "-1s"}}
    )
    cfg.apply_defaults()
    assert cfg.team.max_members == -1
    assert cfg.team.rate_quota_per_min == -1
    assert cfg.team.invite_ttl_td().total_seconds() == -1


def test_kafka_configured_treats_blank_broker_as_unconfigured() -> None:
    """★ `brokers: [""]` 在 Go 侧判为未配置(纯 RPC 本地调试模式)。

    按 `len(brokers) > 0` 判会去建一个连不上的 producer 然后 fail-fast ——
    同一份 yaml 两边行为相反。
    """
    assert not tconf.KafkaConf(brokers=[""]).configured()
    assert not tconf.KafkaConf(brokers=["  "]).configured()
    assert tconf.KafkaConf(brokers=["127.0.0.1:9093"]).configured()


def test_dev_yaml_loads(repo_root: pathlib.Path) -> None:
    """★ 与 Go 共用同一份 etc/team-dev.yaml,必须能原样加载。

    这条是「同一份 yaml 喂两个实现」的最低门槛:任何新增字段落进 model_extra 被静默
    忽略,都会先在这里表现为某个断言对不上。
    """
    path = repo_root / "services" / "matchmaking" / "team" / "etc" / "team-dev.yaml"
    cfg = tconf.Config.load(str(path))
    assert cfg.server.grpc.addr == ":20010"
    assert cfg.server.http.addr == ":21010"
    assert cfg.team.matchmaker_addr == "127.0.0.1:20011"
    assert cfg.team.locator_addr == "127.0.0.1:20006"
    assert cfg.team.offline_leave.enabled is True
    assert cfg.team.match_call_auth_require is True
    # 两把密钥必须不同 —— 共用等于让任一方能冒充另一方(见 conf.py 字段注释)。
    assert cfg.team.match_call_auth_secret != cfg.team.match_resume_auth_secret
    assert cfg.kafka.configured()
    assert cfg.ds_auth.mode == "permissive"
    cfg.validate_join_policy()
    cfg.validate_offline_leave()
