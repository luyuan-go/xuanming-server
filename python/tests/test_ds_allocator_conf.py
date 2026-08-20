"""ds_allocator 配置 —— 默认值 / 判据符号 / 三态解析 / 机械护栏 / 校验闸,逐条对着 Go 断言。

为什么这些必须有测试:配置层的分叉**不报错**,只让同一份 yaml 在两栈上跑出不同行为。
本服最危险的三处:

  ① `Defaults()` 全用 `== 0`,因为有六个字段用**负值表示「显式关闭整道闸」**。
     写成 `<= 0` 的后果:一份写着 `roster_join_deadline: "-1s"` 的 yaml,运维以为
     关掉了到齐期限,实际 Python 副本按 45s 照判弃 —— 而两边都不报错。
  ② `no_show_battle_timeout` / `roster_join_deadline` / `orphan_gs_reclaim_after`
     **刻意不在 Defaults() 里赋值**。一旦提前兜底,resolve 就分不清「没配」与
     「配了默认值」,下限护栏(60s / 30s / 5m)整条失效 —— 那三条护栏各自守着
     「玩家进不去场景」(§9.20 红线)。
  ③ `agones.capacity_warn_ratio` 是全文件唯一用 `<= 0 || > 1` 的判据。顺手统一成
     `== 0` 会让 `1.5` 原样生效:比例永远达不到,Fleet 打满时零告警。
"""

from __future__ import annotations

import datetime as _dt
import os
import pathlib

import pytest

from pandorapy import fence_timeline, placement
from pandorapy.services.ds_allocator import conf as dconf
from pandorapy.services.ds_allocator import orphan_reclaim

GO_CONF = "services/battle/ds_allocator/internal/conf/conf.go"
GO_MAIN = "services/battle/ds_allocator/cmd/ds_allocator/main.go"
DEV_YAML = "services/battle/ds_allocator/etc/ds_allocator-dev.yaml"

# 会影响 apply_defaults 的环境变量。用例必须在**确定**的环境里跑:
# 开发机上恰好设了 PANDORA_DS_EXE 就会让默认值用例莫名变红/变绿。
_DS_ENV_KEYS = (
    "PANDORA_DS_LAUNCHER",
    "PANDORA_DS_UPROJECT",
    "PANDORA_DS_EXE",
    "PANDORA_DS_DIR",
    "PANDORA_DS_ADVERTISE_HOST",
)


@pytest.fixture(autouse=True)
def _clean_ds_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _DS_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _cfg(**raw) -> dconf.Config:
    """构造配置并跑 apply_defaults(不跑 validate —— 与 Go main 的分段一致)。"""
    cfg = dconf.Config.model_validate(raw)
    cfg.apply_defaults()
    return cfg


def _alloc(**allocator) -> dconf.AllocatorConf:
    """只构造 allocator 段(测 resolve_* 时不需要整棵树)。"""
    return dconf.AllocatorConf(**allocator)


# ── 默认值:与 Go 的 Defaults() 逐个同值 ──────────────────────────────────────


def test_defaults_match_go() -> None:
    cfg = _cfg()
    a = cfg.allocator

    # mode 留空 + 两个 enabled 都关 → mock(离线兜底)。
    assert cfg.mode == dconf.MODE_MOCK
    # launcher 缺省归一到 packaged(现状行为,旧配置零改动)。
    assert cfg.local_ds.launcher == dconf.LAUNCHER_PACKAGED

    assert a.heartbeat_timeout_td() == _dt.timedelta(seconds=15)
    assert a.activation_stability_beats == 3
    assert a.activation_stability_span_td() == _dt.timedelta(seconds=10)
    assert a.sweep_interval_td() == _dt.timedelta(seconds=5)
    assert a.battle_ttl_td() == _dt.timedelta(hours=2)
    assert a.ready_wait_timeout_td() == _dt.timedelta(seconds=10)
    assert a.empty_battle_timeout_td() == _dt.timedelta(minutes=5)
    assert a.no_show_ledger_window_td() == _dt.timedelta(minutes=10)
    assert a.no_show_penalty_base_td() == _dt.timedelta(seconds=30)
    assert a.no_show_penalty_cap_td() == _dt.timedelta(minutes=5)
    assert a.no_show_penalty_free == 1
    assert a.mock_ds_addr_host == "127.0.0.1"
    assert a.mock_ds_port_base == 30000
    assert a.mock_ds_port_range == 1000
    # 留空即 enforce / observe(档位默认不在 Defaults 里填,由 resolve 归一)。
    assert a.resolve_writer_lease_mode() == dconf.WRITER_LEASE_ENFORCE
    assert a.resolve_roster_join_mode() == dconf.ROSTER_JOIN_MODE_OBSERVE

    ag = cfg.agones
    assert ag.api_server == "https://kubernetes.default.svc"
    assert ag.namespace == "default"
    assert ag.token_path == "/var/run/secrets/kubernetes.io/serviceaccount/token"
    assert ag.ca_path == "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    assert ag.allocate_timeout_td() == _dt.timedelta(seconds=5)
    assert ag.capacity_watch_interval_td() == _dt.timedelta(seconds=30)
    assert ag.capacity_warn_ratio == 0.8

    ld = cfg.local_ds
    assert ld.advertise_host == "127.0.0.1"
    assert ld.port_base == 7777
    assert ld.port_range == 100
    assert ld.log_dir == "run/dev/logs/ds"

    assert cfg.server.grpc.addr == ":20020"
    assert cfg.server.http.addr == ":21020"

    # ds_auth.Defaults()
    assert cfg.ds_auth.authority_mode == "legacy"
    assert cfg.ds_auth.issuer == "pandora-ds-control"
    assert cfg.ds_auth.audience == "pandora-ds"
    assert cfg.ds_auth.battle_token_ttl_td() == _dt.timedelta(hours=4)
    assert cfg.ds_auth.hub_token_ttl_td() == _dt.timedelta(hours=24)
    assert cfg.ds_auth.active_heartbeat_max_age_td() == _dt.timedelta(seconds=30)
    # mode / secret 留空即「不启用」,刻意不填默认。
    assert cfg.ds_auth.mode == ""
    assert cfg.ds_auth.secret == ""


def test_three_fields_deliberately_have_no_default() -> None:
    """★ 这三项**不得**在 apply_defaults 里被填。

    填了会让 resolve_* 分不清「没配」与「配了默认值」,于是:
      - no_show:下限 60s 与「跟随 empty」两条判定同时失效;
      - roster:负值关闭语义消失(0 与负都变成 45s);
      - orphan:5m 下限失效,手滑配的短阈值会原样生效,删掉正在进人的 DS。
    """
    a = _cfg().allocator
    assert a.no_show_battle_timeout == ""
    assert a.roster_join_deadline == ""
    assert a.orphan_gs_reclaim_after == ""


def test_heartbeat_default_references_fence_timeline() -> None:
    """15s 不是随手写的数字,而是 §9 不变量 4 的 Battle 心跳超时。

    引用 fence_timeline 而不是抄一份:那个模块断言「Battle 超时 < Hub 超时」
    (对局判弃必须比大厅快,玩家在等结果)。抄一份的话改坏 fence_timeline 时
    本服务默认值不跟着变,而两处都不报错。
    """
    assert dconf.DEFAULT_HEARTBEAT_TIMEOUT == _dt.timedelta(
        seconds=fence_timeline.BATTLE_HEARTBEAT_TIMEOUT_SEC
    )
    assert not fence_timeline.check_timeline(), "fence 时间线自身已不自洽"


def test_orphan_reclaim_constants_are_referenced_not_copied() -> None:
    """回收阈值/下限必须**引用** orphan_reclaim(它是判定链的实现处)。"""
    assert dconf.DEFAULT_ORPHAN_GS_RECLAIM_AFTER == _dt.timedelta(
        seconds=orphan_reclaim.DEFAULT_RECLAIM_AFTER_SEC
    )
    assert dconf.ORPHAN_GS_RECLAIM_AFTER_FLOOR == _dt.timedelta(
        seconds=orphan_reclaim.RECLAIM_AFTER_FLOOR_SEC
    )


def test_fence_constants_are_referenced_not_copied() -> None:
    """owner 实例租约秒数是 §9.22 正确性常量,不可配置也不可抄。"""
    assert dconf.DS_FENCE_LEASE_MAX_SECONDS == placement.DS_FENCE_LEASE_MAX_SECONDS
    assert (
        dconf.DS_FENCE_REENTRY_BARRIER_SECONDS
        == placement.DS_FENCE_REENTRY_BARRIER_SECONDS
    )


# ── 判据符号:负值 = 显式关闭,必须原样保留 ──────────────────────────────────


@pytest.mark.parametrize(
    "field",
    [
        "empty_battle_timeout",
        "no_show_ledger_window",
        "no_show_penalty_base",
        "no_show_penalty_cap",
        "heartbeat_timeout",
        "sweep_interval",
        "battle_ttl",
        "ready_wait_timeout",
        "activation_stability_span",
    ],
)
def test_negative_duration_survives_defaults(field: str) -> None:
    """★ 判据必须是 `== 0`:负值原样保留,绝不被兜回默认。

    这些字段里 empty_battle_timeout / no_show_ledger_window / no_show_penalty_base
    的负值就是「关掉这道闸」。兜回默认 = 配置写着关、实际开着,而两边都不报错。
    其余几项虽然没有「关闭」语义,判据也必须同符号 —— 一旦有人把它们改成 `<= 0`,
    上面三项迟早会被一起改。
    """
    cfg = _cfg(allocator={field: "-1s"})
    assert getattr(cfg.allocator, field + "_td")() == _dt.timedelta(seconds=-1)


def test_negative_penalty_free_survives_defaults() -> None:
    """负值 = 0 次免罚(首次即罚)的严格档,判据只能是 `== 0`。

    兜回 1 的后果:运维配了严格档,实际首次 no-show 仍然免罚 —— 占位刷子每个号
    都能白刷一次,而配置面看起来严格档已生效。
    """
    assert _cfg(allocator={"no_show_penalty_free": -1}).allocator.no_show_penalty_free == -1


@pytest.mark.parametrize(
    ("configured", "want"),
    [
        (0.0, 0.8),  # 未配 → 默认
        (-0.1, 0.8),  # 域外(负)
        (1.5, 0.8),  # ★ 域外(>1)必须回默认;原样生效 = 预警永不触发
        (1.0, 1.0),  # 上界闭区间
        (0.5, 0.5),
    ],
)
def test_capacity_warn_ratio_is_the_only_range_criteria(
    configured: float, want: float
) -> None:
    """全文件唯一一处 `<= 0 || > 1`,与 Go 逐字一致。"""
    assert _cfg(agones={"capacity_warn_ratio": configured}).agones.capacity_warn_ratio == want


# ── mode / launcher 归一化 ───────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "want"),
    [
        ({}, dconf.MODE_MOCK),
        ({"mode": "  AGONES "}, dconf.MODE_AGONES),  # trim + 小写
        ({"mode": "local"}, dconf.MODE_LOCAL),
        ({"agones": {"enabled": True}}, dconf.MODE_AGONES),  # legacy 推导
        ({"local_ds": {"enabled": True}}, dconf.MODE_LOCAL),
        # 显式 mode 优先于 legacy enabled(两者矛盾时以 mode 为权威)。
        ({"mode": "mock", "agones": {"enabled": True}}, dconf.MODE_MOCK),
        # 两个 enabled 同时为 true 时按 agones 优先(与 Go 的 switch 顺序一致)。
        (
            {"agones": {"enabled": True}, "local_ds": {"enabled": True}},
            dconf.MODE_AGONES,
        ),
    ],
)
def test_mode_resolution(raw: dict, want: str) -> None:
    assert _cfg(**raw).mode == want


@pytest.mark.parametrize(
    ("raw", "want"),
    [
        ("", dconf.LAUNCHER_PACKAGED),
        ("packaged", dconf.LAUNCHER_PACKAGED),
        ("  EDITOR ", dconf.LAUNCHER_EDITOR),
        # ★ 非法值归一到 packaged(**不报错**)—— 与 Go 一致。
        # 这与 writer_lease_mode / roster mode 的「拼错即拒」相反,别顺手统一:
        # launcher 选错只是慢一点/快一点,不改变任何安全判定。
        ("editorr", dconf.LAUNCHER_PACKAGED),
    ],
)
def test_launcher_normalization(raw: str, want: str) -> None:
    assert _cfg(local_ds={"launcher": raw}).local_ds.launcher == want


def test_editor_local_widens_timeouts_only_when_unset() -> None:
    """editor 形态放宽 300s/120s,**仅当对应字段留空**;显式配置永远优先。

    不放宽的后果:editor DS 要加载编辑器模块 + 读未 cook 的散装资产,本机三个 UE
    进程并存时启动超过 120s,AllocateBattle 判 ready 超时 → PVE 对局恒 FAILED(实测)。
    放宽了却顶掉显式配置的后果同样糟:运维按 packaged 调的阈值被静默改大。
    """
    cfg = _cfg(mode="local", local_ds={"launcher": "editor"})
    assert cfg.allocator.ready_wait_timeout_td() == _dt.timedelta(seconds=300)
    assert cfg.allocator.heartbeat_timeout_td() == _dt.timedelta(seconds=120)

    explicit = _cfg(
        mode="local",
        local_ds={"launcher": "editor"},
        allocator={"ready_wait_timeout": "42s", "heartbeat_timeout": "43s"},
    )
    assert explicit.allocator.ready_wait_timeout_td() == _dt.timedelta(seconds=42)
    assert explicit.allocator.heartbeat_timeout_td() == _dt.timedelta(seconds=43)


@pytest.mark.parametrize(
    "raw",
    [
        {"mode": "local", "local_ds": {"launcher": "packaged"}},  # packaged 不放宽
        {"mode": "agones", "local_ds": {"launcher": "editor"}},  # 非 local 不放宽
        {"mode": "mock", "local_ds": {"launcher": "editor"}},
    ],
)
def test_editor_widening_scope(raw: dict) -> None:
    """放宽只作用于 `mode=local 且 launcher=editor`;线上默认值一字不动。"""
    cfg = _cfg(**raw)
    assert cfg.allocator.ready_wait_timeout_td() == _dt.timedelta(seconds=10)
    assert cfg.allocator.heartbeat_timeout_td() == _dt.timedelta(seconds=15)


def test_launcher_env_overrides_yaml_and_drives_widening(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PANDORA_DS_LAUNCHER 让一键脚本免改 yaml 切形态。

    ★ 顺序陷阱:env 归一化必须**早于**超时默认值。晚了的话 env 切到 editor 却仍按
    packaged 的 10s/15s 兜底 —— 一键脚本切 editor 后每局必超时,而 yaml 一个字没错。
    """
    monkeypatch.setenv("PANDORA_DS_LAUNCHER", "editor")
    cfg = _cfg(mode="local", local_ds={"launcher": "packaged"})
    assert cfg.local_ds.launcher == dconf.LAUNCHER_EDITOR
    assert cfg.allocator.ready_wait_timeout_td() == _dt.timedelta(seconds=300)


# ── 路径与 env 兜底(跨机器移植路径)────────────────────────────────────────


def test_exe_env_fallback_only_when_path_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """yaml 路径存在时不得被脚本注入值顶掉;不存在时才回退。"""
    real = tmp_path / "PandoraServer.exe"
    real.write_text("x", encoding="utf-8")
    monkeypatch.setenv("PANDORA_DS_EXE", str(tmp_path / "injected.exe"))
    monkeypatch.setenv("PANDORA_DS_DIR", str(tmp_path / "injected-dir"))

    kept = _cfg(local_ds={"executable_path": str(real), "working_dir": "keep-me"})
    assert kept.local_ds.executable_path == str(real)
    # ★ working_dir 只在**确实用了** env exe 时才跟着换。
    assert kept.local_ds.working_dir == "keep-me"

    fell_back = _cfg(local_ds={"executable_path": str(tmp_path / "nope.exe")})
    assert fell_back.local_ds.executable_path == dconf.from_slash(
        str(tmp_path / "injected.exe")
    )
    assert fell_back.local_ds.working_dir == dconf.from_slash(
        str(tmp_path / "injected-dir")
    )


def test_advertise_host_env_beats_yaml(monkeypatch: pytest.MonkeyPatch) -> None:
    """内网测试服要用局域网 IP,远程策划客户端才连得到战斗 DS。

    没有这条覆盖 = 每次开内网场都要改仓库配置,或者所有人拿到 127.0.0.1 连不上。
    """
    monkeypatch.setenv("PANDORA_DS_ADVERTISE_HOST", "192.168.1.7")
    assert _cfg(local_ds={"advertise_host": "127.0.0.1"}).local_ds.advertise_host == "192.168.1.7"


def test_expand_env_go_matches_go_semantics(monkeypatch: pytest.MonkeyPatch) -> None:
    """Go 的 os.ExpandEnv 把**未定义**变量替换成空串;Python 的 expandvars 保留原文。

    不自己实现的话,两栈对 `${PANDORA_DS_ROOT}/Packages/...` 打进日志的路径不同,
    排障时对不上(最终都走 env 兜底,所以不会炸,只会让人查错方向)。
    """
    monkeypatch.setenv("PANDORA_TEST_ROOT", "F:/work")
    assert dconf.expand_env_go("${PANDORA_TEST_ROOT}/Packages") == "F:/work/Packages"
    assert dconf.expand_env_go("$PANDORA_TEST_ROOT/x") == "F:/work/x"
    assert dconf.expand_env_go("${PANDORA_TEST_UNSET}/Packages") == "/Packages"
    # 反斜杠绝对路径不含 $,原样保留。
    assert dconf.expand_env_go("F:\\work\\Packages") == "F:\\work\\Packages"


def test_from_slash_is_platform_faithful() -> None:
    assert dconf.from_slash("a/b/c") == os.sep.join(["a", "b", "c"])


# ── no-show 三态解析(镜像 Go 的 conf_no_show_timeout_test.go)────────────────

_EMPTY = _dt.timedelta(minutes=5)


@pytest.mark.parametrize(
    ("empty", "no_show", "want"),
    [
        # 未配置取默认 150s(DSTicket TTL 120s + 30s 余量)
        # ★ 这一栏刻意写**字面量**而不是引用 dconf 常量:引用的话「常量被改小 +
        #   resolve 照常工作」会自洽通过 —— 用例就只在证明代码等于它自己。
        ("5m", "", _dt.timedelta(seconds=150)),
        ("5m", "90s", _dt.timedelta(seconds=90)),
        # 低于下限被钳到 60s —— 手滑配 1s 不能让正在 travel 的玩家进不去
        ("5m", "1s", _dt.timedelta(seconds=60)),
        # 高于 empty 被钳到 empty —— no-show 不该比普通空场还晚回收
        ("5m", "10m", _EMPTY),
        # 负值 = 显式禁用差异化,退回单阈值(改动前行为)
        ("5m", "-1s", _EMPTY),
        # empty 本身禁用时跟随禁用,不自作主张开启回收
        ("-1s", "90s", _dt.timedelta(seconds=-1)),
        # empty 为 0(未配)时跟随,交由 Defaults 填默认值
        ("", "90s", _dt.timedelta(0)),
    ],
)
def test_resolve_no_show_timeout(empty: str, no_show: str, want: _dt.timedelta) -> None:
    got = _alloc(empty_battle_timeout=empty, no_show_battle_timeout=no_show)
    assert got.resolve_no_show_timeout() == want


@pytest.mark.parametrize(
    "no_show", ["-1h", "-1s", "", "1s", "90s", "1h"]
)
def test_resolve_no_show_never_silently_zero(no_show: str) -> None:
    """empty 已启用时,无论 no-show 怎么配,结果都必须落在 (0, empty]。

    返回 0 会让下游 `timeout > 0` 判定永假 ⇒ no-show 局**永不回收**,比改动前更糟:
    刷进出副本的外挂能用小号把整个 Fleet 押死,正常玩家进不去(§9.20)。
    """
    got = _alloc(empty_battle_timeout="5m", no_show_battle_timeout=no_show).resolve_no_show_timeout()
    assert _dt.timedelta(0) < got <= _EMPTY


# ── 到齐期限:解析 / 武装窗 / 判定谓词 ───────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "want"),
    [
        # 同上:字面量,不引用被测常量。
        ("", _dt.timedelta(seconds=45)),  # 0 = 默认 45s
        ("-1s", _dt.timedelta(0)),  # 负 = 显式关闭整道闸
        ("10s", _dt.timedelta(seconds=30)),  # 低于 30s 下限被抬回
        ("60s", _dt.timedelta(seconds=60)),
        # ★ 刻意**不**用 empty_battle_timeout 做上限(no-show 那条才用)。
        ("30m", _dt.timedelta(minutes=30)),
    ],
)
def test_resolve_roster_join_deadline(raw: str, want: _dt.timedelta) -> None:
    assert _alloc(roster_join_deadline=raw, empty_battle_timeout="5m").resolve_roster_join_deadline() == want


def test_roster_arm_window_derivation() -> None:
    """武装窗 = ready_wait + deadline + 30s 余量,自 allocated_at 起算。

    它是滚动升级的纵深防御:旧副本手里已到齐过的局没有 roster_ever_complete 记忆,
    被新副本接手时光看该标记会把「局中掉线」误判成「开局没到齐」,45s 后判弃一场
    **正在打的对局**。窗口太小 → 冷启动慢的正常局被提前放过(漏防);
    太大 → 覆盖整局,退回上面那个更危险的状态。
    """
    a = _alloc(ready_wait_timeout="300s", roster_join_deadline="45s")
    assert a.resolve_roster_join_arm_window() == _dt.timedelta(seconds=300 + 45 + 30)

    # ready_wait 的兜底是 120s(推导专用),**不是** Defaults 填的 10s。
    fallback = _alloc(roster_join_deadline="45s")
    assert fallback.resolve_roster_join_arm_window() == _dt.timedelta(seconds=120 + 45 + 30)
    assert dconf.ARM_WINDOW_DEFAULT_READY_WAIT != dconf.DEFAULT_READY_WAIT_TIMEOUT

    # 闸关着时窗口为 0(不武装)。
    assert _alloc(roster_join_deadline="-1s").resolve_roster_join_arm_window() == _dt.timedelta(0)


@pytest.mark.parametrize(
    ("mode", "cfg_gen", "battle_gen", "want"),
    [
        (dconf.ROSTER_JOIN_MODE_ENFORCE, 1, 1, True),
        (dconf.ROSTER_JOIN_MODE_OBSERVE, 1, 1, False),  # observe 只采证
        (dconf.ROSTER_JOIN_MODE_OFF, 1, 1, False),
        (dconf.ROSTER_JOIN_MODE_ENFORCE, 0, 0, False),  # gen=0 永久豁免
        (dconf.ROSTER_JOIN_MODE_ENFORCE, 2, 1, False),  # 旧代局不执行
        (dconf.ROSTER_JOIN_MODE_ENFORCE, 1, 2, False),  # 更新代也不执行(必须相等)
        (dconf.ROSTER_JOIN_MODE_ENFORCE, 1, 0, False),  # legacy 局永不执行
    ],
)
def test_roster_deadline_should_abandon(
    mode: str, cfg_gen: int, battle_gen: int, want: bool
) -> None:
    """★ `battle_gen == cfg_gen` 必须是**相等**不是 `>=`。

    写成 `>=` 会让旧代局重新入选:回滚(只翻 mode)之后再激活时,那些已累计过计时的
    旧局会立刻被判弃 —— 而策略代机制存在的全部意义就是让旧局自然排空。
    """
    assert dconf.roster_deadline_should_abandon(mode, cfg_gen, battle_gen) is want


# ── 档位归一化:拼错必须 fail-fast ──────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "want"),
    [
        ("", dconf.WRITER_LEASE_ENFORCE),
        ("  ENFORCE ", dconf.WRITER_LEASE_ENFORCE),
        ("warmup", dconf.WRITER_LEASE_WARMUP),
        ("off", dconf.WRITER_LEASE_OFF),
    ],
)
def test_writer_lease_mode_normalized(raw: str, want: str) -> None:
    assert _alloc(writer_lease_mode=raw).resolve_writer_lease_mode() == want


def test_writer_lease_mode_typo_rejected() -> None:
    """拼错不得静默退化成 off:那会让多副本各跑一份心跳扫描,同一局被并行判弃。"""
    with pytest.raises(ValueError, match="writer_lease_mode"):
        _alloc(writer_lease_mode="enforcee").resolve_writer_lease_mode()


@pytest.mark.parametrize(
    ("raw", "want"),
    [
        ("", dconf.ROSTER_JOIN_MODE_OBSERVE),
        ("  OBSERVE ", dconf.ROSTER_JOIN_MODE_OBSERVE),
        ("enforce", dconf.ROSTER_JOIN_MODE_ENFORCE),
        ("off", dconf.ROSTER_JOIN_MODE_OFF),
    ],
)
def test_roster_join_mode_normalized(raw: str, want: str) -> None:
    assert _alloc(roster_join_deadline_mode=raw).resolve_roster_join_mode() == want


def test_roster_join_mode_typo_rejected() -> None:
    """拼错一个字母不该悄悄改变「判不判弃一场正在打的对局」。"""
    with pytest.raises(ValueError, match="roster_join_deadline_mode"):
        _alloc(roster_join_deadline_mode="enfroce").resolve_roster_join_mode()


# ── no-show 记罚的消费端钳制 ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("window", "base", "want"),
    [
        ("10m", "30s", True),
        ("-1s", "30s", False),  # 窗口负 → 整个记罚关闭
        ("10m", "-1s", False),  # 基数负 → 同上
        ("", "", True),  # 都走默认(10m / 30s)
    ],
)
def test_no_show_penalty_enabled(window: str, base: str, want: bool) -> None:
    """判据是 `window <= 0 或 base <= 0`(Go: biz/allocator.go recordNoShowPenalties)。

    写成 `== 0` 会让「配成负值 = 显式关掉记罚」失效,占位刷子照样被罚。
    """
    cfg = _cfg(allocator={"no_show_ledger_window": window, "no_show_penalty_base": base})
    cfg.apply_defaults()
    assert cfg.allocator.no_show_penalty_enabled() is want


@pytest.mark.parametrize(("raw", "want"), [(-1, 0), (-100, 0), (0, 1), (1, 1), (3, 3)])
def test_resolve_no_show_penalty_free(raw: int, want: int) -> None:
    """负值 = 严格档「首次即罚」,必须在消费端钳成 0。

    忘了钳的后果与注释语义**相反**:`count - free` 因负 free 变大,首次 no-show
    就按第 2 档(base×2)罚 —— 想要更严格反而先放过再加倍。
    注:`0 → 1` 是 Defaults() 干的(判据 `== 0`),这里连起来验证两段拼接后的最终值。
    """
    cfg = _cfg(allocator={"no_show_penalty_free": raw})
    cfg.apply_defaults()
    assert cfg.allocator.resolve_no_show_penalty_free() == want


# ── 激活稳定性门 ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("beats", "span", "want"),
    [
        (3, "10s", True),  # 默认档
        (1, "", False),  # beats<=1 且 span<=0 → 整道关(仅供测试/回退)
        (0, "-1s", False),
        (3, "", True),  # ★ 只按拍数判仍然生效 —— 判据是 and 不是 or
        (1, "10s", True),  # 只按跨度判同理
    ],
)
def test_activation_stability_gate(beats: int, span: str, want: bool) -> None:
    """门关闭的判据是 `beats<=1 **且** span<=0`。

    写成 `or` 会让「beats=3 + span 未配」被整道关掉:DS 在 PostLoadMapWithWorld
    回调里的首拍即激活并放行 ds_addr,而游戏线程可能还在阻塞 —— 客户端连上一个
    不回包的 DS(INC-20260727-001 第三 P0)。
    """
    assert _alloc(
        activation_stability_beats=beats, activation_stability_span=span
    ).activation_stability_gate_enabled() is want


# ── Agones map_fleets 路由 ───────────────────────────────────────────────────


def test_dedicated_fleet_lookup() -> None:
    ag = dconf.AgonesConf(
        fleet_name="pandora-battle",
        map_fleets=[
            {"map_id": 7, "fleet_name": "songlin"},
            {"map_id": 8, "fleet_name": "", "canary_fleet_name": "artic-canary"},
        ],
    )
    assert ag.dedicated_fleet_for(7) == "songlin"
    assert ag.dedicated_fleet_for(0) == ""  # map_id=0 = 未指定
    assert ag.dedicated_fleet_for(99) == ""  # 未配置 → 走通用池
    # ★ fleet_name 为空的条目被 dedicated_fleet_for **跳过**,但 for_track 会在
    #   map_id 命中的第一条上直接 return —— 两个判据刻意不同,合并会让两栈选出不同 Fleet。
    assert ag.dedicated_fleet_for(8) == ""
    assert ag.dedicated_fleet_for_track(8, "stable") == ""
    assert ag.dedicated_fleet_for_track(8, "canary") == "artic-canary"
    assert ag.dedicated_fleet_for_track(7, "canary") == ""  # 未配 canary 专属池


# ── 孤儿 GameServer 回收阈值 ────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "want_sec"),
    [
        ("", 600),  # 未配 → 10m(字面量,不引用被测常量)
        ("-1s", 600),  # 负值同样按默认(Go 的判据是 `<= 0`)
        ("30s", 300),  # ★ 手滑配 30s 被抬到 5m 下限
        ("10m", 600),
        ("1h", 3600),
    ],
)
def test_orphan_reclaim_clamp(raw: str, want_sec: int) -> None:
    """不钳制的后果:一台刚分配、玩家正拿着有效票据在进的 DS 会在进场途中被删。"""
    got = _alloc(orphan_gs_reclaim_after=raw).resolve_orphan_gs_reclaim_after()
    assert got == _dt.timedelta(seconds=want_sec)


# ── 生产授权权威判定 ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "want"),
    [
        ({}, False),
        ({"ds_auth": {"authority_mode": "redis"}}, True),
        ({"mode": "agones", "ds_auth": {"mode": "enforce"}}, True),
        ({"mode": "agones", "ds_auth": {"mode": "permissive"}}, False),
        ({"mode": "local", "ds_auth": {"mode": "enforce"}}, False),
        # ★ 这一条对大小写不敏感(trim + lower),而 authority_mode_redis() 是精确比较
        #   —— Go 侧的既有不一致,原样搬过来。
        ({"ds_auth": {"authority_mode": " Redis "}}, True),
    ],
)
def test_requires_reliable_lifecycle_publication(raw: dict, want: bool) -> None:
    assert _cfg(**raw).requires_reliable_lifecycle_publication() is want


def test_authority_mode_redis_is_exact_match_unlike_the_other_predicate() -> None:
    """把 Go 的不一致钉死成用例:哪天有人「顺手统一」,这里会变红并被迫先读报告。"""
    cfg = _cfg(ds_auth={"authority_mode": "Redis"})
    assert cfg.requires_reliable_lifecycle_publication() is True
    assert cfg.ds_auth.authority_mode_redis() is False


# ── Validate 闸:逐条 ───────────────────────────────────────────────────────

_ABORT_SECRET = "pandora-dev-allocation-abort-auth-key-v1!"  # 40 bytes ≥ 32
_DS_SECRET = "pandora-dev-jwt-secret-change-me-32!"


def _prod_raw(**over) -> dict:
    """一份「生产授权权威」的最小合法配置(Agones + enforce legacy 灰度)。"""
    raw = {
        "mode": "agones",
        "ds_auth": {"mode": "enforce", "secret": _DS_SECRET},
        "kafka": {"brokers": ["127.0.0.1:9093"]},
        "locator_addr": "127.0.0.1:20006",
    }
    raw.update(over)
    return raw


def test_lifecycle_publication_requires_brokers() -> None:
    """缺 broker = abandoned 事件发不出去 → match 永不释放、段位不回滚,而日志全绿。"""
    _cfg(**_prod_raw()).validate_lifecycle_publication_config()  # 不抛

    with pytest.raises(ValueError, match="kafka.brokers"):
        _cfg(**_prod_raw(kafka={"brokers": []})).validate_lifecycle_publication_config()

    # ★ 空白 broker 不算已配置:ConfigMap 渲染出 [""] 时长度是 1 而实际一个都没有。
    with pytest.raises(ValueError, match="kafka.brokers"):
        _cfg(**_prod_raw(kafka={"brokers": ["  "]})).validate_lifecycle_publication_config()

    # 非生产授权模式(dev local/off)不要求 broker。
    _cfg(kafka={"brokers": []}).validate_lifecycle_publication_config()


def test_battle_departure_requires_locator() -> None:
    """locator_addr 是局内唯一的路由信号;留空不会报错,只会让 presence 静默蒸发。"""
    _cfg(**_prod_raw()).validate_battle_departure_config()
    with pytest.raises(ValueError, match="locator_addr"):
        _cfg(**_prod_raw(locator_addr="   ")).validate_battle_departure_config()
    _cfg(locator_addr="").validate_battle_departure_config()  # 非生产模式放行


def _redis_authority_raw(**allocator) -> dict:
    alloc = {
        "allocation_abort_auth_secret": _ABORT_SECRET,
        "allocation_abort_auth_audience": "ds-allocator:battle-allocation-abort",
    }
    alloc.update(allocator)
    return {
        "mode": "agones",
        "kafka": {"brokers": ["127.0.0.1:9093"]},
        "locator_addr": "127.0.0.1:20006",
        "ds_auth": {
            "mode": "enforce",
            "secret": _DS_SECRET,
            "authority_mode": "redis",
            "active_heartbeat_max_age": "30s",
            "fence": {"etcd_endpoints": ["127.0.0.1:2379"], "keyset_revision": "r1"},
        },
        "allocator": alloc,
    }


def test_allocation_abort_auth_gate() -> None:
    """撤销任意分配的破坏性端点:密钥缺失 / 太短 / 与 ds_auth 同钥一律拒启。"""
    _cfg(**_redis_authority_raw()).validate_allocation_abort_auth_config()

    with pytest.raises(ValueError, match="allocation_abort_auth_secret"):
        _cfg(**_redis_authority_raw(allocation_abort_auth_secret="short")).validate_allocation_abort_auth_config()

    with pytest.raises(ValueError, match="allocation_abort_auth_audience"):
        _cfg(**_redis_authority_raw(allocation_abort_auth_audience="")).validate_allocation_abort_auth_config()

    # ★ 与 ds_auth.secret 同钥 = 每台持 DS 回调令牌的 DS 都能冒充 Matchmaker 撤销分配。
    with pytest.raises(ValueError, match="independent trust-domain"):
        _cfg(**_redis_authority_raw(allocation_abort_auth_secret=_DS_SECRET)).validate_allocation_abort_auth_config()

    # legacy / local 模式不设这道闸(RPC 本身仍 fail-closed)。
    _cfg().validate_allocation_abort_auth_config()


def test_local_map_source_gate() -> None:
    """两者皆空不是「回退默认图」,而是每一局都失败且失败得很晚(玩家只看到排队中)。"""
    with pytest.raises(ValueError, match="mode=local"):
        _cfg(mode="local").validate_local_map_source_config()

    _cfg(mode="local", config_table={"dir": "../../../configtable/dist"}).validate_local_map_source_config()
    _cfg(mode="local", local_ds={"loader_map": "/Game/Entry/Level/Lvl_Server_Entry"}).validate_local_map_source_config()
    # 空白串不算配置。
    with pytest.raises(ValueError, match="mode=local"):
        _cfg(mode="local", local_ds={"loader_map": "   "}).validate_local_map_source_config()
    # 非 local 模式不要求(关卡由 DS 侧 Loader 查同一张表)。
    _cfg(mode="agones").validate_local_map_source_config()


def test_roster_enforce_requires_generation() -> None:
    """enforce + gen=0 是自相矛盾:判定谓词对 gen=0 恒 false,写下 enforce 的人以为开了闸。"""
    with pytest.raises(ValueError, match="roster_policy_generation"):
        _cfg(
            allocator={"roster_join_deadline_mode": "enforce", "roster_policy_generation": 0}
        ).validate_roster_join_deadline_config()

    _cfg(
        allocator={"roster_join_deadline_mode": "enforce", "roster_policy_generation": 1}
    ).validate_roster_join_deadline_config()
    # observe / off 不受 generation 约束。
    _cfg(allocator={"roster_join_deadline_mode": "observe"}).validate_roster_join_deadline_config()
    _cfg(allocator={"roster_join_deadline_mode": "off"}).validate_roster_join_deadline_config()


def test_negative_roster_generation_rejected_at_load() -> None:
    """负 generation 会让判定谓词恒 false —— 与 enforce+0 同一种失效形状,换个入口。"""
    with pytest.raises(Exception):
        dconf.Config.model_validate({"allocator": {"roster_policy_generation": -1}})


def test_ds_auth_redis_fence_gate() -> None:
    _cfg(**_redis_authority_raw()).ds_auth.validate_redis_fence()

    raw = _redis_authority_raw()
    raw["ds_auth"] = dict(raw["ds_auth"], mode="permissive")
    with pytest.raises(ValueError, match="requires mode=enforce"):
        _cfg(**raw).ds_auth.validate_redis_fence()

    raw = _redis_authority_raw()
    raw["ds_auth"] = dict(raw["ds_auth"], fence={"keyset_revision": "r1"})
    with pytest.raises(ValueError, match="fence.etcd_endpoints"):
        _cfg(**raw).ds_auth.validate_redis_fence()

    raw = _redis_authority_raw()
    raw["ds_auth"] = dict(raw["ds_auth"], fence={"etcd_endpoints": ["x"]})
    with pytest.raises(ValueError, match="keyset_revision"):
        _cfg(**raw).ds_auth.validate_redis_fence()


def test_ds_auth_ttl_floor() -> None:
    """令牌不续期 ⇒ TTL 太短不会在签发时报错,而是让对局跑到一半回调被全拒。"""
    cfg = _cfg(ds_auth={"battle_token_ttl": "10m"})
    cfg.ds_auth.validate_ttls(enabled=False)  # 未启用时零/短 TTL 无害
    with pytest.raises(ValueError, match="battle_token_ttl"):
        cfg.ds_auth.validate_ttls(enabled=True)

    _cfg().ds_auth.validate_ttls(enabled=True)  # 默认 4h / 24h 过闸


def test_battle_token_ttl_must_cover_battle_ttl() -> None:
    """固定下限(1h)拦不住这个:battle_ttl 是可配的,4h 令牌盖不住 8h 镜像 TTL。"""
    cfg = _cfg(allocator={"battle_ttl": "8h"})
    cfg.validate_battle_token_ttl_vs_battle_ttl(signer_enabled=False)  # 不签发就不判
    with pytest.raises(ValueError, match="battle_token_ttl"):
        cfg.validate_battle_token_ttl_vs_battle_ttl(signer_enabled=True)

    _cfg().validate_battle_token_ttl_vs_battle_ttl(signer_enabled=True)  # 2h + 15m < 4h


# ── 真实 yaml:一个字段都不许掉进 model_extra ────────────────────────────────


def _collect_extras(model, path: str = "") -> dict[str, list[str]]:
    """递归收集整棵配置树上落进 `model_extra` 的字段名。

    ★ 为什么这条测试值得单独存在:pydantic 的 extra="allow" 让未建模字段**安静地**
    进入 model_extra —— yaml 里配了、Python 侧当没看见。这是本项目的高频缺陷形状
    (redis.addrs / snowflake.node_id_source / grpc.max_conn_age 都这么丢过)。
    模型必须 allow(Go 侧加字段时 Python 版不能起不来),所以只能用测试把它钉住。
    """
    from pydantic import BaseModel as _BM

    found: dict[str, list[str]] = {}
    extra = getattr(model, "model_extra", None) or {}
    if extra:
        found[path or "<root>"] = sorted(extra)
    for name in type(model).model_fields:
        value = getattr(model, name)
        child = f"{path}.{name}" if path else name
        if isinstance(value, _BM):
            found.update(_collect_extras(value, child))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, _BM):
                    found.update(_collect_extras(item, f"{child}[{i}]"))
    return found


def test_dev_yaml_has_no_unmodeled_field(repo_root: pathlib.Path) -> None:
    """载入 Go 与 Python 共用的那份 yaml,断言没有字段被静默忽略。

    白名单为空 —— 这份 yaml 里的每一个 key 都必须有对应模型字段。
    """
    cfg = dconf.Config.load(repo_root / DEV_YAML)
    assert _collect_extras(cfg) == {}


def test_dev_yaml_effective_values(repo_root: pathlib.Path) -> None:
    """dev yaml 的关键取值必须被真正读到(不是「加载成功」就算数)。

    ★ 这里同时锁住那条**跨文件**的时序链:
      server.grpc.timeout(330s) > allocator.ready_wait_timeout(300s)。
      反过来的话,DS 还在冷启动、AllocateBattle 就被 gRPC 层砍断 —— 每局必失败,
      而两个数字单看都很合理。
    """
    cfg = dconf.Config.load(repo_root / DEV_YAML)
    assert cfg.mode == dconf.MODE_LOCAL
    assert cfg.allocator.ready_wait_timeout_td() == _dt.timedelta(seconds=300)
    assert cfg.allocator.heartbeat_timeout_td() == _dt.timedelta(seconds=120)
    assert cfg.allocator.resolve_writer_lease_mode() == dconf.WRITER_LEASE_ENFORCE
    assert cfg.allocator.resolve_roster_join_mode() == dconf.ROSTER_JOIN_MODE_OBSERVE
    assert cfg.allocator.resolve_roster_join_deadline() == _dt.timedelta(seconds=45)
    assert cfg.config_table.dir == "../../../configtable/dist"
    assert cfg.locator_addr == "127.0.0.1:20006"
    assert cfg.kafka.configured()
    assert cfg.local_ds.extra_env["PANDORA_DS_ALLOCATOR_ADDR"] == "127.0.0.1:8444"
    assert "-NoZenAutoLaunch=127.0.0.1:8558" in cfg.local_ds.extra_args

    grpc = cfg.server.grpc
    assert grpc.timeout_td() > cfg.allocator.ready_wait_timeout_td()
    # GOAWAY 宽限必须盖过在途 AllocateBattle,否则滚动更新会砍断在途分配。
    assert grpc.max_conn_age_grace_td() >= grpc.timeout_td()


def test_dev_yaml_passes_all_gates(repo_root: pathlib.Path) -> None:
    """仓库里这份 dev 配置必须整体过闸 —— 否则 Python 版一启动就拒。"""
    dconf.Config.load(repo_root / DEV_YAML).validate_conf()


# ── 防漂移:直接对 Go 源码断言 ───────────────────────────────────────────────


def test_go_defaults_use_equals_zero(repo_root: pathlib.Path) -> None:
    """★ 判据符号在 Go 侧一旦被改宽,这条会变红,提醒 Python 侧同步。

    盯住三个用负值表达「显式关闭」的字段:它们在 Defaults() 里必须是 `== 0`。
    """
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")
    for field in ("EmptyBattleTimeout", "NoShowLedgerWindow", "NoShowPenaltyBase"):
        assert f"if c.Allocator.{field} == 0 {{" in src, f"{field} 的判据不再是 == 0"
    assert "if c.Allocator.NoShowPenaltyFree == 0 {" in src
    # 唯一的区间判据。
    assert "if c.Agones.CapacityWarnRatio <= 0 || c.Agones.CapacityWarnRatio > 1 {" in src


def test_go_floor_constants_match(repo_root: pathlib.Path) -> None:
    """机械护栏的数值与 Go 常量逐个对齐(它们守的都是「玩家进不去场景」)。"""
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")
    assert "DefaultNoShowBattleTimeout = 150 * time.Second" in src
    assert "NoShowTimeoutFloor = 60 * time.Second" in src
    assert "DefaultRosterJoinDeadline = 45 * time.Second" in src
    assert "RosterJoinDeadlineFloor = 30 * time.Second" in src
    assert "DefaultReadyWaitTimeout = 120 * time.Second" in src
    assert "RosterJoinArmSlack = 30 * time.Second" in src
    assert dconf.DEFAULT_NO_SHOW_BATTLE_TIMEOUT == _dt.timedelta(seconds=150)
    assert dconf.NO_SHOW_TIMEOUT_FLOOR == _dt.timedelta(seconds=60)
    assert dconf.DEFAULT_ROSTER_JOIN_DEADLINE == _dt.timedelta(seconds=45)
    assert dconf.ROSTER_JOIN_DEADLINE_FLOOR == _dt.timedelta(seconds=30)
    assert dconf.ARM_WINDOW_DEFAULT_READY_WAIT == _dt.timedelta(seconds=120)
    assert dconf.ROSTER_JOIN_ARM_SLACK == _dt.timedelta(seconds=30)


def test_go_main_gate_event_names_exist(repo_root: pathlib.Path) -> None:
    """六条闸的事件名必须真实存在于 Go main —— 它们是 Loki 告警与运维手册的入口。

    Python 版 main.py 必须逐条单独调用并打同名事件;归成一个
    `config_validation_failed` 会让排障方向从第一步就错。
    """
    src = (repo_root / GO_MAIN).read_text(encoding="utf-8")
    for event in (
        "ds_auth_fence_config_invalid",
        "ds_lifecycle_config_invalid",
        "battle_departure_config_invalid",
        "allocation_abort_auth_config_invalid",
        "local_map_source_config_invalid",
        "roster_join_deadline_config_invalid",
        "ds_auth_ttl_invalid",
        "ds_auth_battle_token_ttl_too_small_vs_battle_ttl",
        # 本模块**没有**搬的两条(依赖装配结果),写 main.py 的人必须自己接。
        "battle_model_b_invalid_activation",
        "local_battle_auth_profile_invalid",
    ):
        assert event in src, f"Go main 里找不到事件名 {event}"


def test_editor_cvar_arg_matches_go(repo_root: pathlib.Path) -> None:
    """抄错一个字母 = editor DS 的秒级无限重连循环回来了(2026-08-18 实测)。"""
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")
    assert f'EditorLauncherCVarArg = "{dconf.EDITOR_LAUNCHER_CVAR_ARG}"' in src
