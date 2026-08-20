"""battle_result 的启动闸与 service 层边界。

覆盖的是「起不来 / 起错了 / 拒错了」这一族缺陷 —— 它们都不会在业务测试里露头:

  - 启动闸漏掉或**顺序**不同:同一份坏配置在两栈报不同的第一个错误,排障从此对不上;
  - 事件名漂移:Loki 上按事件名建的告警静默失去覆盖;
  - DS 回调面的鉴权码用错:enforce 档下 DS 成批被拒 / 或伪造令牌被放行;
  - ReportProgress 未实现却回了 OK:DS 以为事实已入账 → 真正丢经验和掉落。

★ 启动闸测试**刻意只覆盖 MySQL 之前那一段**(闸①~⑧)。它们不需要任何外部依赖,
  正好是"同一份 yaml 在两栈第一个报什么错"最容易分叉的地方。
  MySQL 之后的闸由 test_battle_result_repo.py 打真库覆盖。
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import re
import subprocess
import sys

import pytest
from pandora.battle.v1 import battle_pb2
from pandora.common.v1 import errcode_pb2
from structlog.testing import capture_logs

from pandorapy import dsauth, errcode
from pandorapy.services.battle_result import biz as bbiz
from pandorapy.services.battle_result import main as bmain
from pandorapy.services.battle_result import repo as brepo
from pandorapy.services.battle_result import service as bsvc

GO_MAIN = "services/battle/battle_result/cmd/battle_result/main.go"
GO_CONF = "services/battle/battle_result/internal/conf/conf.go"
DEV_YAML = "services/battle/battle_result/etc/battle_result-dev.yaml"

_MIN_YAML = """
server:
  grpc: {addr: ":20022"}
  http: {addr: ":21022"}
node:
  mysql_client:
    dsn: "u:p@tcp(127.0.0.1:3307)/pandora_battle"
kafka:
  brokers: ["127.0.0.1:9093"]
  group_id: "pandora-battle-result"
config_table:
  dir: "../configtable/dist"
battle: {}
ds_auth:
  mode: "off"
  authority_mode: "legacy"
"""


def _write(tmp_path: pathlib.Path, body: str, name: str = "c.yaml") -> str:
    p = tmp_path / name
    p.write_text(body, encoding="utf-8")
    return str(p)


def _run_main(conf_path: str, _capsys=None) -> tuple[int, list[dict]]:  # noqa: ANN001
    """在**子进程**里跑 main 并解析它 stdout 上的结构化日志。返回 (exit_code, 事件列表)。

    ★ 为什么必须起子进程,两条理由都踩过:

      ① structlog 的 `capture_logs` 用不了 —— `plog.setup()` 会重装整条处理器链,
         把 capture_logs 挂上去的那条换掉,于是它恒返回空列表,测试会误以为
         "一条日志都没打"(第一版就这么假红过)。

      ② 在**本进程**里调 `main()` 会让 `plog.setup()` 把 structlog 的 PrintLogger
         绑到 pytest 当时那个被 capsys 接管的 stdout 上;capsys 在用例结束后关掉它,
         **后续所有用例**打日志就炸 `ValueError: I/O operation on closed file`。
         这是典型的"跨用例共享资源"污染 —— 第二版也踩过。

    解析 stdout 还有个好处:验的正是 Loki 真会采到的那一份,而不是测试专用的旁路。
    """
    env = dict(os.environ)
    py_root = pathlib.Path(__file__).resolve().parents[1]
    env["PYTHONPATH"] = os.pathsep.join([str(py_root), str(py_root / "gen")])
    env["PYTHONIOENCODING"] = "utf-8"
    proc = subprocess.run(
        [sys.executable, "-m", "pandorapy.services.battle_result.main", "-conf", conf_path],
        capture_output=True, text=True, encoding="utf-8", env=env, cwd=str(py_root), timeout=60,
    )
    events: list[dict] = []
    for line in (proc.stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return proc.returncode, events


def _events(logs: list[dict]) -> list[str]:
    # 启动闸日志的事件名在 `msg` 字段(与 Go 的 zap `"msg", "xxx"` 同口径)。
    return [e.get("msg", e.get("event", "")) for e in logs]


# ── 命令行 ───────────────────────────────────────────────────────────────────


def test_conf_flag_is_single_dash() -> None:
    """★ 单横线 `-conf`:run_services.ps1 / start.ps1 / K8s manifest 里全是这个形式。

    改成 argparse 惯常的 `--conf` 会让 Python 版无法被同一条命令行拉起。
    """
    assert bmain._parse_args([]).conf == "etc/battle_result-dev.yaml"  # noqa: SLF001
    assert bmain._parse_args(["-conf", "x.yaml"]).conf == "x.yaml"  # noqa: SLF001


# ── 启动闸 ───────────────────────────────────────────────────────────────────


def test_gate_config_load_failed(tmp_path: pathlib.Path, capsys) -> None:  # noqa: ANN001
    code, logs = _run_main(str(tmp_path / "nope.yaml"), capsys)
    assert code == 1
    assert "config_load_failed" in _events(logs)


def test_gate_config_scan_failed_on_unsupported_cell_route(tmp_path: pathlib.Path, capsys) -> None:  # noqa: ANN001
    """cell_route.mode 非空 = Python 侧只实现单 Cell → 拒启。

    静默按单 Cell 跑的话玩家会被路由到错的 cell 且不报错(§14)。
    """
    path = _write(tmp_path, _MIN_YAML + '\ncell_route:\n  mode: "keyspace"\n')
    code, logs = _run_main(path, capsys)
    assert code == 1
    assert "config_scan_failed" in _events(logs)


def test_gate_config_scan_failed_on_removed_monster_exp(tmp_path: pathlib.Path, capsys) -> None:  # noqa: ANN001
    """残留的 monster_exp 键必须拒启,不能静默忽略(数值权威已改为 role_level 表)。"""
    path = _write(tmp_path, _MIN_YAML.replace("battle: {}", "battle:\n  monster_exp: {2001: 40}"))
    code, logs = _run_main(path, capsys)
    assert code == 1
    assert "config_scan_failed" in _events(logs)


def test_gate_retention_mode_invalid(tmp_path: pathlib.Path, capsys) -> None:  # noqa: ANN001
    path = _write(
        tmp_path, _MIN_YAML.replace("battle: {}", 'battle:\n  retention_mode: "delet"')
    )
    code, logs = _run_main(path, capsys)
    assert code == 1
    assert "battle_retention_mode_invalid" in _events(logs)


def test_gate_ds_auth_fence_config_invalid(tmp_path: pathlib.Path, capsys) -> None:  # noqa: ANN001
    """authority_mode=redis 但 mode 不是 enforce → fence 配置闸先拒。"""
    path = _write(
        tmp_path,
        _MIN_YAML.replace('  mode: "off"\n  authority_mode: "legacy"',
                          '  mode: "permissive"\n  authority_mode: "redis"'),
    )
    code, logs = _run_main(path, capsys)
    assert code == 1
    assert "ds_auth_fence_config_invalid" in _events(logs)


def test_gate_ingress_invalid_before_authority_gate(tmp_path: pathlib.Path, capsys) -> None:  # noqa: ANN001
    """★ 顺序即契约:redis 权威 + 订阅了无凭据 topic 时,先报 ingress 闸。

    这道闸在 Go 里是闸⑤、Python 专有的 authority 闸是闸⑦ —— 顺序反了会让
    同一份坏 yaml 在两栈报不同的第一个错误。
    """
    body = _MIN_YAML.replace(
        '  mode: "off"\n  authority_mode: "legacy"',
        '  mode: "enforce"\n  authority_mode: "redis"\n'
        '  active_heartbeat_max_age: "30s"\n'
        '  fence:\n    etcd_endpoints: ["127.0.0.1:2379"]\n    keyset_revision: "r1"',
    ).replace(
        "battle: {}",
        "battle:\n"
        '  ds_allocator_addr: "127.0.0.1:20020"\n'
        "  consume_topics:\n"
        '    - "pandora.battle.result"\n'
        '    - "pandora.ds.lifecycle"',
    )
    code, logs = _run_main(_write(tmp_path, body), capsys)
    assert code == 1
    events = _events(logs)
    assert "battle_result_ingress_invalid" in events
    assert "battle_authority_mode_unsupported" not in events


def test_gate_authority_mode_unsupported(tmp_path: pathlib.Path, capsys) -> None:  # noqa: ANN001
    """★ Python 专有闸:Model-B 未实现 → **拒绝启动**而不是 WARN 放行。

    带病启动的两条后果都是静默的:结算 DS 的 pod 永不回收(没有 terminal release
    worker)、失租副本仍能结算(没有 fence)。
    """
    body = _MIN_YAML.replace(
        '  mode: "off"\n  authority_mode: "legacy"',
        '  mode: "enforce"\n  authority_mode: "redis"\n'
        '  active_heartbeat_max_age: "30s"\n'
        '  fence:\n    etcd_endpoints: ["127.0.0.1:2379"]\n    keyset_revision: "r1"',
    ).replace(
        "battle: {}",
        "battle:\n"
        '  ds_allocator_addr: "127.0.0.1:20020"\n'
        '  consume_topics: ["pandora.ds.lifecycle"]',
    )
    code, logs = _run_main(_write(tmp_path, body), capsys)
    assert code == 1
    assert "battle_authority_mode_unsupported" in _events(logs)


def test_gate_retention_before_authority(tmp_path: pathlib.Path, capsys) -> None:  # noqa: ANN001
    """两处同时非法时,先报**共享**的那道闸(Go 也有的闸⑥)。

    Python 专有闸排在共享闸之后,是为了让"两栈第一个报什么错"在所有共享闸上一致。
    """
    body = _MIN_YAML.replace(
        '  mode: "off"\n  authority_mode: "legacy"',
        '  mode: "enforce"\n  authority_mode: "redis"\n'
        '  active_heartbeat_max_age: "30s"\n'
        '  fence:\n    etcd_endpoints: ["127.0.0.1:2379"]\n    keyset_revision: "r1"',
    ).replace(
        "battle: {}",
        "battle:\n"
        '  ds_allocator_addr: "127.0.0.1:20020"\n'
        '  consume_topics: ["pandora.ds.lifecycle"]\n'
        '  retention_mode: "delet"',
    )
    code, logs = _run_main(_write(tmp_path, body), capsys)
    assert code == 1
    events = _events(logs)
    assert "battle_retention_mode_invalid" in events
    assert "battle_authority_mode_unsupported" not in events


def test_gate_mysql_dsn_required(tmp_path: pathlib.Path, capsys) -> None:  # noqa: ANN001
    """结算落库不可降级:DSN 缺失必须拒启,而不是"先起来再说"。"""
    body = _MIN_YAML.replace(
        '    dsn: "u:p@tcp(127.0.0.1:3307)/pandora_battle"', '    dsn: ""'
    )
    code, logs = _run_main(_write(tmp_path, body), capsys)
    assert code == 1
    assert "mysql_dsn_required" in _events(logs)


def test_service_starting_is_first_event(tmp_path: pathlib.Path, capsys) -> None:  # noqa: ANN001
    """第一条永远是 service_starting(带 conf 路径)—— 起不来时它是唯一的定位起点。"""
    _, logs = _run_main(str(tmp_path / "nope.yaml"), capsys)
    assert logs[0]["msg"] == "service_starting"
    assert "conf" in logs[0]


# ── 与 Go 源码的事件名 parity ────────────────────────────────────────────────


def test_gate_event_names_exist_in_go_main(repo_root: pathlib.Path) -> None:
    """★ 事件名逐字来自 Go 源码,**不是抄一份常量再自比**。

    抄一份的话 Go 改了名这个测试照样绿(它验的是"我抄的等于我抄的")。
    """
    src = (repo_root / GO_MAIN).read_text(encoding="utf-8")
    py = (
        pathlib.Path(bmain.__file__).parent / "main.py"
    ).read_text(encoding="utf-8")
    shared = [
        "config_load_failed",
        "config_scan_failed",
        "ds_auth_fence_config_invalid",
        "battle_result_ingress_invalid",
        "battle_retention_mode_invalid",
        "mysql_dsn_required",
        "mysql_strict_mode_required",
        "mysql_connected",
        "mmr_reader_grpc",
        "mmr_reader_static",
        "player_update_producer_init_failed",
        "player_update_producer_ready",
        "kafka_brokers_empty",
        "battle_recovery_outbox_schema_invalid",
        "battle_progress_schema_invalid",
        "match_releaser_grpc",
        "match_releaser_disabled",
        "drop_granter_grpc",
        "drop_granter_disabled",
        "configtable_dir_required",
        "configtable_load_failed",
        "configtable_load_warning",
        "configtable_loaded",
        "drop_overflow_mail_grpc",
        "drop_overflow_mail_disabled",
        "mission_outbox_schema_check_failed",
        "mission_forward_disabled",
        "ds_auth_guard_init_failed",
        "ds_callback_guard_ready",
        "consume_topics_empty",
        "unknown_consume_topic_skipped",
        "dlq_producer_init_failed",
        "kafka_consumer_new_failed",
        "kafka_consumer_ready",
        "no_valid_consumer",
        "service_starting",
        "service_ready",
        "app_run_failed",
    ]
    missing_in_go = [n for n in shared if f'"{n}"' not in src]
    missing_in_py = [n for n in shared if f'"{n}"' not in py]
    assert not missing_in_go, f"Go main.go 里找不到这些事件名(清单已过时?):{missing_in_go}"
    assert not missing_in_py, f"Python main.py 缺这些事件名:{missing_in_py}"


def test_python_only_gate_is_documented(repo_root: pathlib.Path) -> None:
    """Python 专有的那道闸**必须**不在 Go 里,且在模块头写明理由。

    没有这条约束的话,"顺手加一道闸"会悄悄让两栈行为分叉而没人记录。
    """
    src = (repo_root / GO_MAIN).read_text(encoding="utf-8")
    py = pathlib.Path(bmain.__file__).read_text(encoding="utf-8")
    assert '"battle_authority_mode_unsupported"' not in src
    assert "battle_authority_mode_unsupported" in py
    assert "诚实标注的两处差异" in py
    # 进度通道未实现的启动期判据。
    assert "battle_progress_channel_not_implemented" in py


def test_retention_bounds_match_go_source(repo_root: pathlib.Path) -> None:
    """保留期上下限从 Go 源码抓出来比,不抄一份。"""
    from pandorapy.services.battle_result import conf as bconf

    src = (repo_root / GO_CONF).read_text(encoding="utf-8")
    assert re.search(r"HistoryRetentionMaxDays\s*=\s*180", src)
    assert re.search(r"HistoryRetentionMinDays\s*=\s*30", src)
    assert bconf.HISTORY_RETENTION_MAX_DAYS == 180
    assert bconf.HISTORY_RETENTION_MIN_DAYS == 30
    # 每玩家掉落硬上限同源。
    assert re.search(r"maxDropPerPlayerHardCap\s*=\s*46", src)
    assert bconf.MAX_DROP_PER_PLAYER_HARD_CAP == 46


def test_dlq_retry_policy_matches_go(repo_root: pathlib.Path) -> None:
    """DLQ 重试档位与 Go 同值(infra.md §4.4「失败 3 次进 DLQ」)。"""
    src = (repo_root / GO_MAIN).read_text(encoding="utf-8")
    assert re.search(r"dlqMaxRetries\s*=\s*3", src)
    assert re.search(r"dlqRetryBackoff\s*=\s*500 \* time\.Millisecond", src)
    assert bmain.DLQ_MAX_RETRIES == 3
    assert bmain.DLQ_RETRY_BACKOFF_SEC == 0.5


# ── service 层 ───────────────────────────────────────────────────────────────


class FakeContext:
    """最小 grpc.aio.ServicerContext 替身:守卫只用到 invocation_metadata()。"""

    def __init__(self, metadata: dict[str, str] | None = None) -> None:
        self._md = list((metadata or {}).items())

    def invocation_metadata(self):  # noqa: ANN201
        return self._md


class FakeUsecase:
    def __init__(self, *, already=False, result=None, history=None, error=None):
        self.already = already
        self.result = result
        self.history = history or []
        self.error = error
        self.calls: list[tuple] = []

    async def report_result(self, result, final_seq):
        self.calls.append(("report_result", result.match_id, final_seq))
        if self.error is not None:
            raise self.error
        return self.already

    async def get_match_result(self, match_id):
        self.calls.append(("get", match_id))
        if self.error is not None:
            raise self.error
        return self.result

    async def list_player_history(self, player_id, limit, before_ms):
        self.calls.append(("list", player_id, limit, before_ms))
        if self.error is not None:
            raise self.error
        return self.history


def _svc(uc=None) -> bsvc.BattleResultService:
    return bsvc.BattleResultService(uc or FakeUsecase())


def _req(match_id=1001, players=2) -> battle_pb2.ReportResultRequest:
    return battle_pb2.ReportResultRequest(
        result=battle_pb2.BattleResult(
            match_id=match_id,
            ds_pod_name="battle-pod-1",
            stats=[battle_pb2.PlayerStats(player_id=i + 1, team=i) for i in range(players)],
        ),
        final_progress_seq=0,
    )


async def test_report_result_ok_and_already_recorded() -> None:
    uc = FakeUsecase(already=True)
    resp = await _svc(uc).ReportResult(_req(), FakeContext())
    assert resp.code == errcode_pb2.OK
    assert resp.already_recorded is True


async def test_report_result_missing_result_rejected_before_auth() -> None:
    """缺 result / match_id 在**鉴权之前**就拒:守卫的 scope 需要 match_id 才有意义。"""
    uc = FakeUsecase()
    resp = await _svc(uc).ReportResult(battle_pb2.ReportResultRequest(), FakeContext())
    assert resp.code == errcode_pb2.ERR_INVALID_ARG
    assert uc.calls == []


async def test_report_result_maps_business_error_to_inband_code() -> None:
    """★ 业务失败必须是 `Response(code=...)` + gRPC OK,不是 abort。

    改成 abort 会让 DS 走到完全不同的错误分支(它对 in-band code 的处置是"重试同一 match_id")。
    """
    uc = FakeUsecase(error=errcode.PandoraError(errcode.ErrBattleResultDBWrite, "db"))
    resp = await _svc(uc).ReportResult(_req(), FakeContext())
    assert resp.code == errcode.ErrBattleResultDBWrite


async def test_report_result_cancellation_propagates() -> None:
    """取消必须穿透,不能被映射成业务码返回一个"正常应答"(§9.16)。"""
    uc = FakeUsecase(error=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await _svc(uc).ReportResult(_req(), FakeContext())


async def test_report_result_logs_first_hop() -> None:
    """链路第一站留痕:没有它,「打完没结算」分不清是 DS 没上报还是被某道门拒了。"""
    with capture_logs() as logs:
        await _svc().ReportResult(_req(), FakeContext())
    first = [e for e in logs if e["event"] == "battle_result_received"]
    assert first and first[0]["match_id"] == 1001 and first[0]["players"] == 2


# ── DS 回调守卫 ─────────────────────────────────────────────────────────────


def _guard(mode: str, secret: str = "x" * 32) -> dsauth.DSCallbackGuard:
    from pandorapy.services.battle_result import conf as bconf

    cfg = bconf.DSAuthConf(mode=mode, secret=secret)
    cfg.apply_defaults()
    guard = dsauth.guard_from_conf(cfg)
    assert guard is not None
    return guard


def _token(secret: str, **claims) -> str:
    import jwt as pyjwt

    payload = {
        "iss": "pandora-ds-control",
        "aud": "pandora-ds",
        "sub": "battle-pod-1",
        "ds_type": "battle",
        "match_id": 1001,
        "jti": "j1",
        "exp": 9_999_999_999,
    }
    payload.update(claims)
    return pyjwt.encode(payload, secret, algorithm="HS256")


async def test_guard_off_lets_everything_through() -> None:
    """mode=off → guard 为 None,不校验(与 Go 的 dsGuard==nil 等价)。"""
    from pandorapy.services.battle_result import conf as bconf

    cfg = bconf.DSAuthConf(mode="off")
    cfg.apply_defaults()
    svc = _svc()
    svc.set_ds_callback_guard(dsauth.guard_from_conf(cfg))
    resp = await svc.ReportResult(_req(), FakeContext())
    assert resp.code == errcode_pb2.OK


async def test_guard_enforce_rejects_ds_gateway_without_token() -> None:
    uc = FakeUsecase()
    svc = _svc(uc)
    svc.set_ds_callback_guard(_guard("enforce"))
    ctx = FakeContext({"x-pandora-ds-gateway": "1"})
    resp = await svc.ReportResult(_req(), ctx)
    assert resp.code == errcode_pb2.ERR_UNAUTHORIZED
    assert uc.calls == []  # 未鉴权一律不落库


async def test_guard_enforce_rejects_internal_direct_without_token() -> None:
    """★ require_token=True:纯 DS 回调,**无标记无令牌**的东西向直连也一律拒。

    这道设置堵的是"被攻破的业务 Pod 绕过 Envoy 直连 :20022"的旁路 —— 少了它,
    任何能连到业务端口的进程都能伪造任意一场结算。
    """
    uc = FakeUsecase()
    svc = _svc(uc)
    svc.set_ds_callback_guard(_guard("enforce"))
    resp = await svc.ReportResult(_req(), FakeContext())
    assert resp.code == errcode_pb2.ERR_UNAUTHORIZED
    assert uc.calls == []


async def test_guard_enforce_rejects_cross_match_token() -> None:
    """★ 令牌 match_id 必须等于上报的 match_id:防拿 A 局令牌伪造 B 局结算。"""
    secret = "x" * 32
    svc = _svc()
    svc.set_ds_callback_guard(_guard("enforce", secret))
    ctx = FakeContext(
        {"x-pandora-ds-gateway": "1", "authorization": "Bearer " + _token(secret, match_id=2002)}
    )
    resp = await svc.ReportResult(_req(match_id=1001), ctx)
    assert resp.code == errcode_pb2.ERR_PERMISSION_DENY


async def test_guard_enforce_rejects_wrong_ds_type() -> None:
    """hub 令牌不得用于战斗结算(域隔离)。"""
    secret = "x" * 32
    svc = _svc()
    svc.set_ds_callback_guard(_guard("enforce", secret))
    ctx = FakeContext(
        {"x-pandora-ds-gateway": "1", "authorization": "Bearer " + _token(secret, ds_type="hub")}
    )
    resp = await svc.ReportResult(_req(), ctx)
    assert resp.code == errcode_pb2.ERR_PERMISSION_DENY


async def test_guard_enforce_accepts_matching_token() -> None:
    secret = "x" * 32
    uc = FakeUsecase()
    svc = _svc(uc)
    svc.set_ds_callback_guard(_guard("enforce", secret))
    ctx = FakeContext(
        {"x-pandora-ds-gateway": "1", "authorization": "Bearer " + _token(secret)}
    )
    resp = await svc.ReportResult(_req(), ctx)
    assert resp.code == errcode_pb2.OK
    assert uc.calls[0][0] == "report_result"


async def test_guard_permissive_warns_but_lets_through() -> None:
    """灰度观察期:完整验签 + 范围校验,失败只 warn 放行。"""
    uc = FakeUsecase()
    svc = _svc(uc)
    svc.set_ds_callback_guard(_guard("permissive"))
    with capture_logs() as logs:
        resp = await svc.ReportResult(_req(), FakeContext({"x-pandora-ds-gateway": "1"}))
    assert resp.code == errcode_pb2.OK
    assert [e for e in logs if e["event"] == "ds_callback_auth_permissive_reject"]


async def test_guard_rejection_leaves_audit_log() -> None:
    """★ 拒绝码是业务码(access log 只记 DEBUG),必须在拒绝点显式留证。

    没有它的话「一台 DS 上全部玩家的回调成批被拒」在服务端零日志。
    """
    svc = _svc()
    svc.set_ds_callback_guard(_guard("enforce"))
    with capture_logs() as logs:
        await svc.ReportResult(_req(), FakeContext({"x-pandora-ds-gateway": "1"}))
    rej = [e for e in logs if e["event"] == "ds_auth_rejected"]
    assert rej
    assert rej[0]["rpc"] == "ReportResult"
    assert rej[0]["stage"] == "check_credential"
    assert rej[0]["match_id"] == 1001
    assert rej[0]["reported_pod"] == "battle-pod-1"


# ── ReportProgress(Python 侧未实现)────────────────────────────────────────


def _progress_req(match_id=1001, events=1) -> battle_pb2.ReportProgressRequest:
    return battle_pb2.ReportProgressRequest(
        match_id=match_id,
        events=[battle_pb2.BattleProgressEvent(seq=i + 1) for i in range(events)],
    )


async def test_report_progress_returns_invalid_state_not_ok() -> None:
    """★ 必须回 ERR_INVALID_STATE(与 Go 在 progress_enabled=false 时**同一个码**)。

    回 OK 会让 DS 以为事实已入账 —— 那才是真正会丢经验和掉落的选择;
    回 ERR_NOT_IMPLEMENTED 会让 DS 走到没有约定处置的分支。
    """
    with capture_logs() as logs:
        resp = await _svc().ReportProgress(_progress_req(), FakeContext())
    assert resp.code == errcode_pb2.ERR_INVALID_STATE
    assert resp.acked_seq == 0  # 带非零 acked_seq = "服务端已处理过这个 seq",这里没有
    assert [e for e in logs if e["event"] == "battle_progress_channel_unavailable"]


async def test_report_progress_still_runs_auth_chain_first() -> None:
    """★ 未实现也要先跑鉴权:跳过的话"伪造令牌的 DS"和"合法 DS 撞上未实现"
    在日志里长一个样,而前者是安全信号。
    """
    svc = _svc()
    svc.set_ds_callback_guard(_guard("enforce"))
    with capture_logs() as logs:
        resp = await svc.ReportProgress(_progress_req(), FakeContext({"x-pandora-ds-gateway": "1"}))
    assert resp.code == errcode_pb2.ERR_UNAUTHORIZED  # 鉴权拒,不是未实现拒
    assert [e for e in logs if e["event"] == "ds_auth_rejected"]
    assert not [e for e in logs if e["event"] == "battle_progress_channel_unavailable"]


async def test_report_progress_empty_batch_rejected() -> None:
    resp = await _svc().ReportProgress(battle_pb2.ReportProgressRequest(match_id=1), FakeContext())
    assert resp.code == errcode_pb2.ERR_INVALID_ARG


# ── 查询 RPC ─────────────────────────────────────────────────────────────────


async def test_get_match_result_not_found_is_distinct_from_ok() -> None:
    """"查不到"必须是 ERR_NOT_FOUND;回 OK+空 result 会让调用方分不清"没结算"和"零战绩"。"""
    resp = await _svc(FakeUsecase(result=None)).GetMatchResult(
        battle_pb2.GetMatchResultRequest(match_id=1), FakeContext()
    )
    assert resp.code == errcode_pb2.ERR_NOT_FOUND


async def test_get_match_result_ok() -> None:
    res = battle_pb2.BattleResult(match_id=7)
    resp = await _svc(FakeUsecase(result=res)).GetMatchResult(
        battle_pb2.GetMatchResultRequest(match_id=7), FakeContext()
    )
    assert resp.code == errcode_pb2.OK
    assert resp.result.match_id == 7


async def test_get_match_result_requires_match_id() -> None:
    resp = await _svc().GetMatchResult(battle_pb2.GetMatchResultRequest(), FakeContext())
    assert resp.code == errcode_pb2.ERR_INVALID_ARG


async def test_list_player_history_passes_cursor_through() -> None:
    uc = FakeUsecase(history=[battle_pb2.BattleResult(match_id=3)])
    resp = await _svc(uc).ListPlayerHistory(
        battle_pb2.ListPlayerHistoryRequest(player_id=9, limit=5, before_ms=123), FakeContext()
    )
    assert resp.code == errcode_pb2.OK
    assert [r.match_id for r in resp.results] == [3]
    assert uc.calls[0] == ("list", 9, 5, 123)


async def test_list_player_history_requires_player_id() -> None:
    resp = await _svc().ListPlayerHistory(
        battle_pb2.ListPlayerHistoryRequest(), FakeContext()
    )
    assert resp.code == errcode_pb2.ERR_INVALID_ARG


# ── 装配件 ───────────────────────────────────────────────────────────────────


def test_player_update_pusher_keys_by_player_id() -> None:
    """kafka key = player_id(不变量 §9 同玩家事件保序)。key 用别的值会让保序失效。"""

    class FakeProducer:
        def __init__(self):
            self.sent = []

        async def send_raw(self, key, payload):
            self.sent.append((key, payload))

    prod = FakeProducer()
    asyncio.get_event_loop_policy()
    pusher = bmain.PlayerUpdatePusher(prod)
    asyncio.run(pusher.push_player_update(4242, b"pb"))
    assert prod.sent == [("4242", b"pb")]


def test_grpc_service_full_name() -> None:
    """反射注册用的全名必须与 proto 一致,否则 grpcurl 联调找不到服务。"""
    assert bsvc.GRPC_SERVICE_FULL_NAME == "pandora.battle.v1.BattleResultService"
    assert (
        battle_pb2.DESCRIPTOR.services_by_name["BattleResultService"].full_name
        == bsvc.GRPC_SERVICE_FULL_NAME
    )


def test_all_four_rpcs_are_implemented() -> None:
    """proto 里的 4 个方法必须都在 servicer 上有实现(不是继承的 Unimplemented)。"""
    methods = [
        m.name for m in battle_pb2.DESCRIPTOR.services_by_name["BattleResultService"].methods
    ]
    assert sorted(methods) == ["GetMatchResult", "ListPlayerHistory", "ReportProgress", "ReportResult"]
    for name in methods:
        assert name in bsvc.BattleResultService.__dict__, f"{name} 未在 service 层实现"


def test_usecase_and_repo_share_settle_info_type() -> None:
    """biz 与 repo 用同一个 ProgressSettleInfo —— 各写一份会让掉落抑制判据漂移。"""
    assert bbiz.brepo.ProgressSettleInfo is brepo.ProgressSettleInfo
