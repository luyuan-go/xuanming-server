"""hub_allocator 配置 —— 默认值 / 机械下限 / 启动闸,逐条对着 Go 源码断言。

为什么这些必须有测试:默认值分叉的后果**不是报错**,是同一份 yaml 在 Go 和 Python 上
跑出不同行为而两边都不报错。本服最危险的三处:

  ① `heartbeat_timeout` 的 27s 机械下限(§9.22 再入屏障)。抄丢了 → 配 5s 原样生效,
     分区的旧 Hub 还没自我 fencing,玩家已经被改派到新 Hub = 一人两大厅。
  ② `transfer_cooldown` 等字段的 `== 0` 判据。写宽成 `<= 0` → 写着 `-1s`(显式关闭)
     的 yaml 被兜回 10s:运维以为闸关了、实际开着。
  ③ `reservation_ttl` 的 `[DSTicket TTL + 15s, assignment_ttl]` 区间。下限破了会让
     玩家拿着合法票被判成没座位,而票据日志全绿。

另有一类缺陷本仓已多次踩到:**配了但不读**。所以有一条测试把真实 dev yaml 载入后
断言**没有任何字段落进 model_extra**。
"""

from __future__ import annotations

import datetime as _dt
import pathlib

import pytest
from pydantic import BaseModel

from pandorapy import dsauth, placement
from pandorapy.services.hub_allocator import conf as hconf

GO_CONF = "services/battle/hub_allocator/internal/conf/conf.go"
GO_MAIN = "services/battle/hub_allocator/cmd/hub_allocator/main.go"
GO_PKG_CONFIG = "pkg/config/config.go"
GO_PKG_AUTH_TICKET = "pkg/auth/dsticket.go"
GO_PKG_AUTH_PROFILE = "pkg/auth/ds_local_profile.go"
DEV_YAML = "services/battle/hub_allocator/etc/hub_allocator-dev.yaml"


def _cfg(**overrides) -> hconf.Config:
    """构造一份配置并跑 apply_defaults(默认给足 Redis 端点,免得每条用例重复写)。"""
    raw: dict = {"node": {"redis_client": {"host": "127.0.0.1:6380"}}}
    raw.update(overrides)
    cfg = hconf.Config.model_validate(raw)
    cfg.apply_defaults()
    return cfg


def _sec(value: str) -> float:
    return hconf.pconfig.parse_duration(value).total_seconds()


# ── 默认值:与 Go 的 Defaults() 逐个同值 ─────────────────────────────────────


def test_defaults_match_go() -> None:
    """空配置(只填 redis)的每个默认值都与 Go 的 Defaults() 同值。"""
    cfg = _cfg()
    h = cfg.hub

    # mode 留空 + agones.enabled=false → mock(legacy 推导)。
    assert cfg.mode == hconf.MODE_MOCK

    assert _sec(h.heartbeat_timeout) == 30
    assert _sec(h.sweep_interval) == 5
    assert _sec(h.shard_ttl) == 30 * 60
    assert _sec(h.assignment_ttl) == 30 * 60
    assert _sec(h.reservation_ttl) == 3 * 60 + 15
    assert h.default_region == "global"
    assert h.default_capacity == 500
    assert h.optimistic_retry == 3
    assert h.mock_shard_count == 3
    assert h.mock_hub_addr_host == "127.0.0.1"
    assert h.mock_hub_port_base == 7777
    assert h.players_per_hub == 500
    assert h.min_replicas == 1
    assert h.max_replicas == 20
    assert h.migrate_grace_seconds == 30
    assert h.consolidation_batch == 50
    assert _sec(h.transfer_cooldown) == 10

    # 刻意**不填默认**的字段(填了等于替运维做决定)。
    assert h.owner_addr == ""
    assert h.owner_lease_required is False
    assert h.locator_addr == ""
    assert h.writer_lease_mode == ""
    assert h.autoscale_enabled is False
    assert h.consolidation_enabled is False

    a = cfg.agones
    assert a.api_server == "https://kubernetes.default.svc"
    assert a.namespace == "default"
    assert a.token_path == "/var/run/secrets/kubernetes.io/serviceaccount/token"
    assert a.ca_path == "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    assert _sec(a.list_timeout) == 5
    assert a.fleet_name == ""  # 不填默认:mode=agones 时必填,填个默认会指向错的 Fleet

    # ds_auth.Defaults()
    assert cfg.ds_auth.authority_mode == "legacy"
    assert cfg.ds_auth.issuer == "pandora-ds-control"
    assert cfg.ds_auth.audience == "pandora-ds"
    assert _sec(cfg.ds_auth.battle_token_ttl) == 4 * 3600
    assert _sec(cfg.ds_auth.hub_token_ttl) == 24 * 3600
    assert _sec(cfg.ds_auth.active_heartbeat_max_age) == 30
    # mode / secret 留空即"不启用",**刻意不填默认**。
    assert cfg.ds_auth.mode == ""
    assert cfg.ds_auth.secret == ""

    assert cfg.server.grpc.addr == ":20021"
    assert cfg.server.http.addr == ":21021"


def test_defaults_match_go_source(repo_root: pathlib.Path) -> None:
    """直接对着 Go 源码断言默认值字面量 —— 改 Go 不改这里会当场变红。

    只有这条能挡住「Go 改了默认值、Python 没跟」这类漂移:上面那条测的是
    Python 自己和自己一致。
    """
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")
    for literal in (
        "c.Hub.HeartbeatTimeout = config.Duration(30 * time.Second)",
        "c.Hub.SweepInterval = config.Duration(5 * time.Second)",
        "c.Hub.ShardTTL = config.Duration(30 * time.Minute)",
        "c.Hub.AssignmentTTL = config.Duration(30 * time.Minute)",
        "c.Hub.ReservationTTL = config.Duration(auth.DSTicketMaxTTL + 15*time.Second)",
        'c.Hub.DefaultRegion = "global"',
        "c.Hub.DefaultCapacity = 500",
        "c.Hub.OptimisticRetry = 3",
        "c.Hub.MockShardCount = 3",
        'c.Hub.MockHubAddrHost = "127.0.0.1"',
        "c.Hub.MockHubPortBase = 7777",
        "c.Hub.PlayersPerHub = 500",
        "c.Hub.MinReplicas = 1",
        "c.Hub.MaxReplicas = 20",
        "c.Hub.MigrateGraceSeconds = 30",
        "c.Hub.ConsolidationBatch = 50",
        "c.Hub.TransferCooldown = config.Duration(10 * time.Second)",
        'c.Agones.APIServer = "https://kubernetes.default.svc"',
        'c.Agones.Namespace = "default"',
        "c.Agones.ListTimeout = config.Duration(5 * time.Second)",
        "c.LocalHub.Port = 7777",
        'c.LocalHub.LogDir = "run/dev/logs/ds"',
        'c.Server.Grpc.Addr = ":20021"',
        'c.Server.Http.Addr = ":21021"',
    ):
        assert literal in src, f"Go 默认值已变或被删: {literal}"


def test_go_uses_equals_zero_not_le_zero(repo_root: pathlib.Path) -> None:
    """★ 判据符号是语义的一部分:Go 的 hub 段**全部**用 `== 0` / `== ""`。

    `transfer_cooldown` 的字段注释明写「<=0 视为不限流」——负值是"显式关闭"。
    这条测试锁死「Go 侧没有偷偷改成 <= 0」,否则 Python 这边照抄的 `== 0`
    就成了新的分叉源。
    """
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")
    defaults = src[src.index("func (c *Config) Defaults()") :]
    # Defaults() 里唯一允许的非 `== 0` 比较是两条交叉/下限判定。
    allowed = {
        "if c.Hub.HeartbeatTimeout.Std() < placement.DSFenceReentryBarrier {",
        "if c.Hub.MaxReplicas < c.Hub.MinReplicas {",
    }
    for line in defaults.splitlines():
        stripped = line.strip()
        if not stripped.startswith("if "):
            continue
        if "<=" in stripped or (" < " in stripped and stripped not in allowed):
            pytest.fail(f"Go Defaults() 出现未登记的宽判据: {stripped}")


def test_negative_durations_are_preserved_not_defaulted() -> None:
    """★ 负值 = 显式关闭,必须原样保留。

    写成 `<= 0` 判据的话,一份写着 `transfer_cooldown: "-1s"` 的 yaml 会被兜回
    10s:运维以为关掉了切线防刷闸、实际它还开着,而两边都不报错。
    """
    cfg = _cfg(hub={"transfer_cooldown": "-1s"})
    assert cfg.hub.transfer_cooldown == "-1s"
    assert cfg.hub.transfer_cooldown_td().total_seconds() == -1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("min_replicas", -1),
        ("max_replicas", -5),
        ("players_per_hub", -1),
        ("consolidation_batch", -10),
        ("optimistic_retry", -1),
        ("default_capacity", -1),
        ("mock_shard_count", -1),
        ("migrate_grace_seconds", -1),
    ],
)
def test_negative_ints_are_preserved_not_defaulted(field: str, value: int) -> None:
    """整型同理:Go 是 `== 0`,负值原样流进算式。

    写宽判据会把「显式的荒谬值」悄悄修正成默认值,于是配置错误再也不会在任何
    地方暴露 —— 这比让它以荒谬值跑起来更难查。
    """
    cfg = _cfg(hub={field: value})
    got = getattr(cfg.hub, field)
    if field == "max_replicas":
        # max_replicas=-5 会先保留,再被 `max < min` 的交叉钳制抬到 min_replicas=1。
        assert got == 1
    else:
        assert got == value


# ── §9.22 机械下限:再入屏障 ─────────────────────────────────────────────────


def test_heartbeat_timeout_floor_is_reentry_barrier() -> None:
    """★ 正确性下限而非调优参数:配低会重新打开「一人两 Hub」窗口。"""
    barrier = placement.DS_FENCE_REENTRY_BARRIER_SECONDS
    assert barrier == 27, "屏障常量变了,下面的期望值要跟着复核"

    for configured in ("1s", "5s", "26s"):
        cfg = _cfg(hub={"heartbeat_timeout": configured})
        assert _sec(cfg.hub.heartbeat_timeout) == barrier, (
            f"{configured} 未被抬到再入屏障 —— 分区的旧 Hub 还没自我 fencing,"
            f"玩家已被改派到新 Hub"
        )

    # 等于和高于下限的原样保留(抬回是下限,不是钉死)。
    assert _sec(_cfg(hub={"heartbeat_timeout": "27s"}).hub.heartbeat_timeout) == 27
    assert _sec(_cfg(hub={"heartbeat_timeout": "45s"}).hub.heartbeat_timeout) == 45


def test_heartbeat_floor_references_shared_constant() -> None:
    """下限**引用** placement,不手抄 27。

    抄一份副本会让「Hub 与 Battle 共用同一个屏障」这句话变成假的:真常量被调小时
    本模块照旧按自己那份没被改的副本兜底 —— 全绿,而脑裂窗口已经开了。
    """
    assert hconf.HEARTBEAT_TIMEOUT_FLOOR == _dt.timedelta(
        seconds=placement.DS_FENCE_REENTRY_BARRIER_SECONDS
    )


def test_go_floor_also_references_placement(repo_root: pathlib.Path) -> None:
    """Go 侧也必须引用 placement 常量(两边同源才谈得上"不漂移")。"""
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")
    assert "placement.DSFenceReentryBarrier" in src


def test_reentry_barrier_matches_go(repo_root: pathlib.Path) -> None:
    """Python 的屏障派生值必须与 Go 的 pkg/placement 同值。"""
    src = (repo_root / "pkg" / "placement" / "placement.go").read_text(encoding="utf-8")
    assert "DSFenceLeaseMaxSeconds = 20" in src
    assert "DSFenceSkewMarginSeconds = 7" in src
    assert placement.DS_FENCE_LEASE_MAX_SECONDS == 20
    assert placement.DS_FENCE_SKEW_MARGIN_SECONDS == 7


# ── 交叉钳制:max_replicas >= min_replicas ───────────────────────────────────


@pytest.mark.parametrize(
    ("min_r", "max_r", "want_max"),
    [
        (0, 0, 20),  # 都不配 → 各自默认
        (5, 0, 20),  # max 不配 → 默认 20(> 5,不触发钳制)
        (30, 0, 30),  # max 默认 20 < min 30 → 抬到 30
        (5, 3, 5),  # 显式倒挂 → 抬到 min
        (5, 9, 9),  # 正常 → 原样
    ],
)
def test_max_replicas_clamped_to_min(min_r: int, max_r: int, want_max: int) -> None:
    """不钳的话 scaler 每轮把期望副本数钳到低于保底值,"开服保底 N 个大厅"静默失效。"""
    cfg = _cfg(hub={"min_replicas": min_r, "max_replicas": max_r})
    assert cfg.hub.max_replicas == want_max


# ── mode 推导 ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw_mode", "agones_enabled", "want"),
    [
        ("", False, hconf.MODE_MOCK),
        ("", True, hconf.MODE_AGONES),
        ("  LOCAL ", False, hconf.MODE_LOCAL),  # 归一化:trim + lower
        ("Agones", False, hconf.MODE_AGONES),  # 显式 mode 覆盖 legacy 开关
        ("mock", True, hconf.MODE_MOCK),  # 显式 mode 优先于 agones.enabled
    ],
)
def test_mode_resolution(raw_mode: str, agones_enabled: bool, want: str) -> None:
    cfg = _cfg(mode=raw_mode, agones={"enabled": agones_enabled})
    assert cfg.mode == want


# ── local_hub 段 ────────────────────────────────────────────────────────────


def test_local_hub_inherits_region_and_capacity_from_hub() -> None:
    """★ 顺序契约:local_hub 的兜底读的是**已填过默认值**的 hub 段。

    顺序颠倒会让本机 Hub 分片落在空 region / 0 容量上,AssignHub 只返回
    「无可用分片」,没有任何日志指向配置顺序。
    """
    cfg = _cfg(hub={"default_region": "cn", "default_capacity": 300})
    assert cfg.local_hub.region == "cn"
    assert cfg.local_hub.capacity == 300

    # 显式值不被覆盖。
    cfg2 = _cfg(hub={"default_region": "cn"}, local_hub={"region": "us", "capacity": 8})
    assert cfg2.local_hub.region == "us"
    assert cfg2.local_hub.capacity == 8


@pytest.mark.parametrize(
    ("raw", "want"),
    [
        ("", hconf.LAUNCHER_PACKAGED),
        ("packaged", hconf.LAUNCHER_PACKAGED),
        ("  EDITOR ", hconf.LAUNCHER_EDITOR),
        ("Editor", hconf.LAUNCHER_EDITOR),
        ("editorr", hconf.LAUNCHER_PACKAGED),  # 非法 → 归一到 packaged(与 Go 一致,不报错)
    ],
)
def test_launcher_normalization(raw: str, want: str) -> None:
    """launcher 不是安全档位:打错字的后果在启动日志里直接可见,故 Go 选择归一而非 fatal。"""
    assert _cfg(local_hub={"launcher": raw}).local_hub.launcher == want


def test_launcher_env_override(monkeypatch) -> None:
    monkeypatch.setenv(hconf.ENV_DS_LAUNCHER, "editor")
    assert _cfg(local_hub={"launcher": "packaged"}).local_hub.launcher == "editor"


def test_advertise_host_env_wins_over_yaml(monkeypatch) -> None:
    """内网测试服要用局域网 IP,启动脚本探测后注入,优先级高于 yaml 写死值。"""
    monkeypatch.setenv(hconf.ENV_DS_ADVERTISE_HOST, "192.168.1.7")
    assert _cfg(local_hub={"advertise_host": "127.0.0.1"}).local_hub.advertise_host == (
        "192.168.1.7"
    )
    monkeypatch.delenv(hconf.ENV_DS_ADVERTISE_HOST)
    assert _cfg().local_hub.advertise_host == "127.0.0.1"


def test_executable_env_fallback_when_path_missing(monkeypatch, tmp_path) -> None:
    """★ 判据是「为空**或不存在**」:仓库 yaml 写死的是本开发机路径,换机器后
    那条路径依然非空、只是不存在 —— 只判空会让策划机拿着一条指向虚无的路径去 exec。
    """
    real_exe = tmp_path / "PandoraServer.exe"
    real_exe.write_text("stub", encoding="utf-8")
    real_dir = tmp_path / "WindowsServer"
    real_dir.mkdir()

    monkeypatch.setenv(hconf.ENV_DS_EXE, str(real_exe))
    monkeypatch.setenv(hconf.ENV_DS_DIR, str(real_dir))

    cfg = _cfg(local_hub={"executable_path": "Z:/definitely/missing/PandoraServer.exe"})
    assert cfg.local_hub.executable_path == str(real_exe)
    assert cfg.local_hub.working_dir == str(real_dir)

    # 已存在的路径不被环境变量覆盖(dev 机上 F:\ 路径存在时保持原值)。
    existing = tmp_path / "other.exe"
    existing.write_text("stub", encoding="utf-8")
    cfg2 = _cfg(local_hub={"executable_path": str(existing)})
    assert cfg2.local_hub.executable_path == str(existing)


def test_env_expansion_undefined_becomes_empty(monkeypatch) -> None:
    """Go 的 os.ExpandEnv 把未定义变量展开成**空串**,不是原样保留。

    Python 的 os.path.expandvars 行为相反 —— 用错会让同一份 yaml 在两栈里
    得到不同字符串的路径。
    """
    monkeypatch.delenv("PANDORA_DS_ROOT", raising=False)
    monkeypatch.delenv(hconf.ENV_DS_EXE, raising=False)
    cfg = _cfg(local_hub={"executable_path": "${PANDORA_DS_ROOT}/a/b.exe"})
    assert "PANDORA_DS_ROOT" not in cfg.local_hub.executable_path

    monkeypatch.setenv("PANDORA_DS_ROOT", "F:/work")
    cfg2 = _cfg(local_hub={"executable_path": "${PANDORA_DS_ROOT}/a/b.exe"})
    assert cfg2.local_hub.executable_path == hconf._from_slash("F:/work/a/b.exe")


def test_editor_cvar_arg_matches_go(repo_root: pathlib.Path) -> None:
    """editor 形态的 CVar 覆盖参数必须与 Go 逐字一致 —— 差一个字符,PIE 客户端
    连未 cook 的 editor DS 会被 MissingLevelPackage 踢掉并进入秒级无限重连。
    """
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")
    assert f'EditorLauncherCVarArg = "{hconf.EDITOR_LAUNCHER_CVAR_ARG}"' in src


# ── writer_lease_mode:安全档位 fail-fast ───────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "want"),
    [("", "enforce"), ("enforce", "enforce"), ("warmup", "warmup"), ("off", "off")],
)
def test_writer_lease_mode_ok(raw: str, want: str) -> None:
    assert hconf.HubConf(writer_lease_mode=raw).resolve_writer_lease_mode() == want


@pytest.mark.parametrize("raw", ["Enforce", " enforce", "ENFORCE", "warm", "disabled"])
def test_writer_lease_mode_rejects_non_exact(raw: str) -> None:
    """★ 不 strip 不 lower,与 Go 的 switch 逐字一致。

    "更稳"的归一化会让 `"Enforce"` 在 Python 上正常启动、在 Go 上启动即拒 ——
    同一份 yaml 两个结果,而这是个安全档位。
    """
    with pytest.raises(ValueError, match="writer_lease_mode"):
        hconf.HubConf(writer_lease_mode=raw).resolve_writer_lease_mode()


# ── 启动闸(对应 main.go 的 fail-fast)──────────────────────────────────────


def _agones_cfg(**hub_over) -> hconf.Config:
    """一份能过闸的 agones 配置骨架(RS256 票 + 独立 DS 密钥)。"""
    hub = {"assignment_ttl": "30m", "reservation_ttl": "3m15s"}
    hub.update(hub_over)
    return _cfg(
        mode="agones",
        hub=hub,
        agones={"enabled": True, "fleet_name": "pandora-hub"},
        jwt={"secret": "player-secret-key-at-least-32-bytes!!"},
        ds_ticket={"private_key_file": "/etc/keys/dsticket.pem", "active_kid": "k1"},
        ds_auth={"mode": "enforce", "secret": "ds-callback-secret-32-bytes-min!!!!"},
    )


def test_agones_requires_ds_ticket_v2() -> None:
    """B1:缺 v2 私钥就退回 legacy HS256 票,而 legacy 票**不绑 DS 实例** ——
    一张票能在 Fleet 里任何一台 Hub 上用,§9.22 的 exact 绑定整个失效且无信号。
    """
    cfg = _cfg(mode="agones", agones={"enabled": True, "fleet_name": "f"})
    with pytest.raises(ValueError, match="agones_requires_ds_ticket_v2"):
        cfg.validate_conf()


def test_redis_endpoint_required() -> None:
    """空 host 会让 Python 侧去连 127.0.0.1:6379 —— 连上一个**无关的**本机 Redis,
    进程照常起来,而所有玩家归属写进了错的库。
    """
    cfg = hconf.Config.model_validate({"mode": "mock"})
    cfg.apply_defaults()
    with pytest.raises(ValueError, match="redis_endpoint_required"):
        cfg.validate_conf()

    # addrs(Cluster / Sentinel)也算配了。
    cfg2 = hconf.Config.model_validate(
        {"mode": "mock", "node": {"redis_client": {"addrs": ["127.0.0.1:7000"]}}}
    )
    cfg2.apply_defaults()
    cfg2.validate_conf()


def test_redis_authority_requires_agones() -> None:
    """非 agones 配 redis 权威 = 配了一套根本不会被装配的授权状态机。"""
    cfg = _cfg(
        mode="local",
        ds_auth={"mode": "enforce", "secret": "s" * 32, "authority_mode": "redis"},
    )
    with pytest.raises(ValueError, match="ds_auth_redis_authority_requires_agones"):
        cfg.validate_conf()


def test_redis_authority_requires_fence() -> None:
    """Redis 单一权威缺机械 fence = 授权权威自己先脑裂。"""
    base = {
        "mode": "enforce",
        "secret": "ds-callback-secret-32-bytes-min!!!!",
        "authority_mode": "redis",
    }
    cfg = _agones_cfg()
    cfg.ds_auth = hconf.DSAuthConf.model_validate(base)
    cfg.ds_auth.apply_defaults()
    with pytest.raises(ValueError, match="fence.etcd_endpoints"):
        cfg.validate_conf()

    cfg.ds_auth.fence.etcd_endpoints = ["127.0.0.1:2380"]
    with pytest.raises(ValueError, match="keyset_revision"):
        cfg.validate_conf()

    cfg.ds_auth.fence.keyset_revision = "r1"
    cfg.ds_auth.active_heartbeat_max_age = "0s"
    with pytest.raises(ValueError, match="active_heartbeat_max_age"):
        cfg.validate_conf()


def test_ds_auth_mode_typo_is_fatal() -> None:
    """`mode: "enfroce"` 若静默回落成 off,Heartbeat 的令牌门整个不生效而日志全绿。"""
    cfg = _cfg(ds_auth={"mode": "enfroce", "secret": "s" * 32})
    with pytest.raises(ValueError, match="ds_auth.mode invalid"):
        cfg.validate_conf()


def test_ds_auth_enforce_requires_secret() -> None:
    cfg = _cfg(mode="mock", ds_auth={"mode": "enforce"})
    with pytest.raises(ValueError, match="ds_auth_guard_init_failed"):
        cfg.validate_conf()


def test_ds_auth_empty_additional_secret_is_fatal() -> None:
    """空条目是轮换清单事故:静默过滤会让运维以为旧密钥仍被接受、实则轮换断档。"""
    cfg = _cfg(ds_auth={"mode": "off", "secret": "s" * 32, "additional_secrets": [""]})
    with pytest.raises(ValueError, match="additional_secrets"):
        cfg.validate_conf()


@pytest.mark.parametrize(
    ("field", "value", "pattern"),
    [
        ("battle_token_ttl", "30m", "battle_token_ttl"),
        ("hub_token_ttl", "59m", "hub_token_ttl"),
    ],
)
def test_ds_auth_token_ttl_floor(field: str, value: str, pattern: str) -> None:
    """令牌**不续期**的路径 TTL 太短 = 跑到一半回调被全拒,而配置没人动过。"""
    cfg = _cfg(ds_auth={"mode": "off", "secret": "s" * 32, field: value})
    with pytest.raises(ValueError, match=pattern):
        cfg.validate_conf()


def test_ds_auth_ttl_floor_skipped_when_disabled() -> None:
    """未启用(secret 空 + mode off)时零/负 TTL 无害 —— 不会签发,直接放行。"""
    cfg = _cfg(hub={"writer_lease_mode": "off"})
    cfg.ds_auth.battle_token_ttl = "1s"
    cfg.ds_auth.hub_token_ttl = "1s"
    cfg.validate_conf()


# ── reservation_ttl 区间(Model B)──────────────────────────────────────────


def _model_b(**hub_over) -> hconf.Config:
    cfg = _agones_cfg(**hub_over)
    cfg.ds_auth.authority_mode = "redis"
    cfg.ds_auth.fence.etcd_endpoints = ["127.0.0.1:2380"]
    cfg.ds_auth.fence.keyset_revision = "r1"
    cfg.ds_ticket.ttl = "120s"
    return cfg


def test_model_b_authority_requires_all_three() -> None:
    """三者齐备才启用:少任何一个都退回 legacy —— 不能只判 authority_mode。"""
    cfg = _model_b()
    assert cfg.model_b_authority() is True

    cfg.ds_auth.mode = "permissive"
    assert cfg.model_b_authority() is False

    cfg2 = _model_b()
    cfg2.mode = hconf.MODE_MOCK
    assert cfg2.model_b_authority() is False


@pytest.mark.parametrize(
    ("reservation", "ok"),
    [
        ("2m14s", False),  # < 120s + 15s
        ("2m15s", True),  # 恰好等于下限
        ("3m15s", True),  # 默认值
        ("30m", True),  # 恰好等于 assignment_ttl
        ("31m", False),  # > assignment_ttl
    ],
)
def test_reservation_ttl_window(reservation: str, ok: bool) -> None:
    """下限破了:票还有效、座位预留先过期 → 玩家拿合法票被判没座位,票据日志全绿。
    上限破了:预留活得比归属久,归属过期后座位仍被占,容量被幽灵座位吃掉。
    """
    cfg = _model_b(reservation_ttl=reservation)
    if ok:
        cfg.validate_conf()
    else:
        with pytest.raises(ValueError, match="hub_reservation_ttl_invalid"):
            cfg.validate_conf()


def test_reservation_lower_bound_uses_effective_ticket_ttl() -> None:
    """★ ds_ticket.ttl 留空时下限必须按**签发器实际生效的 120s** 算,不是按裸 0。

    按裸 0 算的话下限退化成 15s,闸看着在、其实拦不住任何东西。
    """
    cfg = _model_b(reservation_ttl="1m")
    cfg.ds_ticket.ttl = ""  # 留空 → 签发器取默认 120s
    assert cfg.ds_ticket.effective_ttl_td().total_seconds() == 120
    with pytest.raises(ValueError, match="hub_reservation_ttl_invalid"):
        cfg.validate_conf()


def test_reservation_window_not_checked_outside_model_b() -> None:
    """legacy 授权门下 Go 不做这条校验 —— 多加会让一份 Go 能起的 yaml 在这里拒启。"""
    cfg = _agones_cfg(reservation_ttl="1s")
    assert cfg.model_b_authority() is False
    cfg.validate_conf()


def test_ds_ticket_constants_match_go(repo_root: pathlib.Path) -> None:
    """DSTicket 的机械上限 / 默认值 / 验票 leeway 必须与 Go 同值。"""
    ticket_src = (repo_root / GO_PKG_AUTH_TICKET).read_text(encoding="utf-8")
    assert "DSTicketDefaultTTL = 2 * time.Minute" in ticket_src
    assert "DSTicketMaxTTL     = 3 * time.Minute" in ticket_src
    assert hconf.DS_TICKET_MAX_TTL == _dt.timedelta(minutes=3)
    assert hconf.DSTicketConf().effective_ttl_td() == _dt.timedelta(minutes=2)

    main_src = (repo_root / GO_MAIN).read_text(encoding="utf-8")
    assert "const dsVerifierMaxLeeway = 15 * time.Second" in main_src
    assert hconf.DS_TICKET_VERIFIER_MAX_LEEWAY == _dt.timedelta(seconds=15)


# ── 玩家面 / DS 面密钥不相交(P0)──────────────────────────────────────────


def test_secret_overlap_fatal_under_enforce() -> None:
    """任一交叉 = 泄露一面即可伪造另一面(拿玩家 JWT 密钥签 DS 回调令牌,
    就能冒充任意 Hub DS 上报心跳、改玩家归属)。
    """
    cfg = _agones_cfg()
    cfg.ds_auth.secret = cfg.jwt.secret
    with pytest.raises(ValueError, match="jwt_ds_auth_secret_overlap"):
        cfg.validate_conf()


def test_secret_overlap_is_warning_when_not_enforce() -> None:
    """dev 模板两面共用同一把公开 dev 密钥,硬拒会打断本地一键启动(§14)。"""
    cfg = _cfg(
        mode="mock",
        jwt={"secret": "shared-dev-secret-at-least-32-bytes!!"},
        ds_auth={"mode": "off", "secret": "shared-dev-secret-at-least-32-bytes!!"},
    )
    warns = cfg.validate_conf()
    assert any("jwt_ds_auth_secret_overlap" in w for w in warns)


def test_secret_overlap_checks_additional_secrets() -> None:
    """轮换期的 additional 也在集合里 —— 只比主密钥会漏掉「旧密钥仍跨两面」。"""
    cfg = _agones_cfg()
    cfg.jwt.additional_secrets = ["rotating-old-key-at-least-32-bytes!!"]
    cfg.ds_auth.additional_secrets = ["rotating-old-key-at-least-32-bytes!!"]
    with pytest.raises(ValueError, match="jwt_ds_auth_secret_overlap"):
        cfg.validate_conf()


def test_secret_overlap_skipped_when_ds_secret_empty() -> None:
    """Go 用 `cfg.DSAuth.Secret != ""` 守住整段检查 —— 两面都没配时不该报交叉。"""
    cfg = _cfg(mode="mock", jwt={"secret": ""}, ds_auth={"secret": ""})
    assert not [w for w in cfg.validate_conf() if "overlap" in w]


# ── mode=local 的 local-off-v1 姿态 ─────────────────────────────────────────


def _local_cfg(**ds_over) -> hconf.Config:
    ds = {"mode": "off", "authority_mode": "legacy", "secret": "ds-secret-32-bytes-min!!!!!!!!!!!"}
    ds.update(ds_over)
    return _cfg(mode="local", jwt={"secret": "player-secret-32-bytes-min!!!!!!!"}, ds_auth=ds)


def test_local_off_v1_happy_path() -> None:
    assert _local_cfg().validate_conf() == []


@pytest.mark.parametrize(
    "ds_over",
    [
        {"mode": "permissive"},  # guard 必须 off:本地链路没有 Redis pending/ACK
        {"mode": "enforce"},
        {"authority_mode": "redis"},  # Model B 的本地权威没有实现
        {"secret": ""},  # 即便 guard=off,secret 仍必填(签 Model-B tuple 经 env 下发)
    ],
)
def test_local_off_v1_profile_gate(ds_over: dict) -> None:
    """姿态不对时进程若照常起来,每台本机 Hub DS 会永远停在 staged ——
    玩家登录后一直等不到大厅(§9.19「无人驱动的静默等待」)。
    """
    cfg = _local_cfg(**ds_over)
    with pytest.raises(ValueError):
        cfg.validate_conf()


def test_local_off_v1_hub_token_ttl_floor() -> None:
    """local-off-v1 没有 annotation 轮换,UE 到 exp 会主动清空 active ——
    一次性凭据必须覆盖整段调试会话(12h),不能按 guard=off 跳过。
    """
    cfg = _local_cfg(hub_token_ttl="11h")
    with pytest.raises(ValueError, match="below local session minimum"):
        cfg.validate_conf()
    assert _local_cfg(hub_token_ttl="12h").validate_conf() == []


def test_local_profile_constants_match_go(repo_root: pathlib.Path) -> None:
    src = (repo_root / GO_PKG_AUTH_PROFILE).read_text(encoding="utf-8")
    assert 'DSLocalProfileOffV1 = "local-off-v1"' in src
    assert "DSLocalHubMinTokenTTL = 12 * time.Hour" in src
    assert hconf.DS_LOCAL_PROFILE_OFF_V1 == "local-off-v1"
    assert hconf.DS_LOCAL_HUB_MIN_TOKEN_TTL == _dt.timedelta(hours=12)


def test_ds_auth_ttl_floors_match_go(repo_root: pathlib.Path) -> None:
    src = (repo_root / GO_PKG_CONFIG).read_text(encoding="utf-8")
    assert "dsAuthMinBattleTokenTTL = time.Hour" in src
    assert "dsAuthMinHubTokenTTL    = time.Hour" in src
    assert hconf.DS_AUTH_MIN_BATTLE_TOKEN_TTL == _dt.timedelta(hours=1)
    assert hconf.DS_AUTH_MIN_HUB_TOKEN_TTL == _dt.timedelta(hours=1)


# ── 告警(非致命,但不说出来运维就以为功能在跑)────────────────────────────


def test_autoscale_inert_under_mock_warns() -> None:
    """Mock 是拓扑-only 不实现 scaler:开了也不会运行,不告警的话运维会以为
    自动扩容在跑,直到某天大厅满员也没扩出第二个实例。
    """
    cfg = _cfg(mode="mock", hub={"autoscale_enabled": True})
    assert any("autoscale_inert_under_mock" in w for w in cfg.validate_conf())
    # agones 下不告警。
    assert not [
        w for w in _agones_cfg(autoscale_enabled=True).validate_conf()
        if "autoscale_inert" in w
    ]


def test_consolidation_without_kafka_warns() -> None:
    cfg = _agones_cfg(consolidation_enabled=True)
    assert any("kafka.brokers" in w for w in cfg.validate_conf())


def test_kafka_configured_ignores_blank_brokers() -> None:
    """ConfigMap 渲染出 `brokers: [""]` 时长度是 1 而实际一个 broker 都没有。"""
    assert hconf.KafkaConf(brokers=[""]).configured() is False
    assert hconf.KafkaConf(brokers=["  "]).configured() is False
    assert hconf.KafkaConf(brokers=["127.0.0.1:9093"]).configured() is True


# ── 真实 dev yaml:同一份配置喂两个实现 ──────────────────────────────────────


def _collect_extras(model: BaseModel, path: str = "") -> dict[str, list[str]]:
    """递归收集所有 pydantic 子模型的 model_extra 键。"""
    found: dict[str, list[str]] = {}
    extra = model.model_extra or {}
    if extra:
        found[path or "<root>"] = sorted(extra)
    for name, value in model.__dict__.items():
        if isinstance(value, BaseModel):
            found.update(_collect_extras(value, f"{path}.{name}" if path else name))
    return found


def test_dev_yaml_has_no_unmodeled_fields(repo_root: pathlib.Path) -> None:
    """★ 「配了但不读」是本仓的高频缺陷(conn_max_lifetime / ProducerConf 都踩过)。

    落进 model_extra 的字段**配了不生效且零信号**。这条断言真实 dev yaml 的每个
    字段都有归宿;新增字段没跟着建模会当场变红,而不是等某天线上行为对不上。
    """
    cfg = hconf.Config.load(str(repo_root / DEV_YAML))
    extras = _collect_extras(cfg)
    assert extras == {}, f"以下字段落进了 model_extra(配了不生效): {extras}"


def test_dev_yaml_values(repo_root: pathlib.Path) -> None:
    """dev yaml 载入后的关键取值 —— 与 Go 读同一份文件必须得到同一套结论。"""
    cfg = hconf.Config.load(str(repo_root / DEV_YAML))

    assert cfg.mode == hconf.MODE_LOCAL
    assert cfg.server.grpc.addr == ":20021"
    assert cfg.server.http.addr == ":21021"
    assert cfg.node.redis_client.host == "127.0.0.1:6380"

    h = cfg.hub
    assert _sec(h.heartbeat_timeout) == 30
    assert _sec(h.reservation_ttl) == 3 * 60 + 15
    assert h.default_region == "cn"
    assert h.default_capacity == 500
    # yaml 显式写 7776(不是默认 7777)—— 抄错默认值会让这条变红。
    assert h.mock_hub_port_base == 7776
    assert h.owner_addr == "127.0.0.1:20017"
    assert h.owner_lease_required is False
    assert h.locator_addr == "127.0.0.1:20006"
    assert h.resolve_writer_lease_mode() == "enforce"
    assert h.autoscale_enabled is False
    assert h.consolidation_enabled is False

    assert cfg.agones.enabled is False
    assert cfg.agones.fleet_name == "pandora-hub"
    assert cfg.agones.insecure_skip_tls_verify is False

    assert cfg.local_hub.launcher == hconf.LAUNCHER_PACKAGED
    assert cfg.local_hub.port == 7777
    # 未配 → 继承 hub 段(不是全局默认 global/500)。
    assert cfg.local_hub.region == "cn"
    assert cfg.local_hub.capacity == 500
    assert cfg.local_hub.log_dir == "run/dev/logs/ds"
    assert cfg.local_hub.extra_env["PANDORA_DS_ALLOCATOR_TLS"] == "0"
    assert "-AssetGatherSync=false" in cfg.local_hub.extra_args

    assert cfg.ds_auth.guard_mode() is dsauth.Mode.OFF
    assert cfg.ds_auth.authority_mode == "legacy"
    assert cfg.ds_auth.signer_enabled() is True

    assert cfg.kafka.configured() is True
    assert cfg.kafka.idempotent is True
    assert cfg.kafka.dial_timeout_ms() == 2000

    assert cfg.session_gate.require is False
    # 提前置 true = 旧 Hub DS 上所有玩家无法进入大厅,所以 dev 必须是 false。
    assert cfg.session_gate.require_ticket_sjti is False


def test_dev_yaml_passes_gates_but_warns_on_dev_shared_secret(
    repo_root: pathlib.Path,
) -> None:
    """dev 一键启动必须能起来(§14「默认路径不许坏」),但共用密钥要有告警。"""
    cfg = hconf.Config.load(str(repo_root / DEV_YAML))
    warns = cfg.validate_conf()
    assert any("jwt_ds_auth_secret_overlap" in w for w in warns), warns


def test_dev_yaml_env_overrides_do_not_break_load(
    repo_root: pathlib.Path, monkeypatch, tmp_path
) -> None:
    """策划机场景:yaml 里写死的 F:\\ 路径不存在,一键脚本注入的路径必须生效。"""
    exe = tmp_path / "PandoraServer.exe"
    exe.write_text("stub", encoding="utf-8")
    monkeypatch.setenv(hconf.ENV_DS_EXE, str(exe))
    monkeypatch.setenv(hconf.ENV_DS_DIR, str(tmp_path))
    cfg = hconf.Config.load(str(repo_root / DEV_YAML))
    if not pathlib.Path("F:\\work\\Packages").exists():
        assert cfg.local_hub.executable_path == str(exe)
        assert cfg.local_hub.working_dir == str(tmp_path)


# ── 正则纪律 ────────────────────────────────────────────────────────────────


def test_env_regex_uses_absolute_anchors_or_none() -> None:
    """本模块的正则不得用 `^`/`$`:Python 的 `$` 会匹配"末尾换行之前",
    末尾带换行的串会被放过(2026-08-18 在 auction 幂等键上真踩过)。
    """
    pattern = hconf._ENV_REF_RE.pattern
    assert "^" not in pattern.replace("[^", "")  # 排除字符类里的取反
    assert "$" not in pattern.replace("\\$", "")  # 排除转义的字面 $


def test_duration_str_round_trips() -> None:
    """兜底值写回 yaml 串后必须能再解析回同一个时长(排障时两栈对比生效配置)。"""
    for td in (
        hconf.DEFAULT_RESERVATION_TTL,
        hconf.DEFAULT_SHARD_TTL,
        hconf.DEFAULT_DS_AUTH_HUB_TOKEN_TTL,
        hconf.DEFAULT_TRANSFER_COOLDOWN,
        hconf.HEARTBEAT_TIMEOUT_FLOOR,
    ):
        assert hconf.pconfig.parse_duration(hconf._duration_str(td)) == td
    assert hconf._duration_str(hconf.DEFAULT_RESERVATION_TTL) == "3m15s"
