"""matchmaker 的启动闸、service 层身份闸与 kafka 推送适配。

覆盖的是「起不来 / 起错了 / 拒错了 / 推丢了」这一族缺陷 —— 它们一条都不会在
业务测试里露头,而每一条的线上表现都极难倒查:

  - 启动闸漏掉或**顺序**不同:同一份坏配置在两栈报不同的"第一个错误",排障对不上;
  - 事件名漂移:Loki 上按事件名建的告警静默失去覆盖;
  - 系统面 RPC 忘了拒玩家 JWT:任何登录玩家都能用任意 match_id 摧毁他人的在局状态;
  - `push_to_players` 部分失败**不抛异常**:READY 推送失败被当成成功,match 被移出
    active ZSET、补推循环不再重试,非队长成员永远停在大厅而日志一片正常。

★ 启动闸测试**刻意只覆盖 Redis 之前那一段**(闸①~⑥)。它们不需要任何外部依赖,
  正好是"同一份 yaml 在两栈第一个报什么错"最容易分叉的地方。
  Redis 之后的闸(⑦~⑱)由本地起真进程验证,不进 CI(见 test_startup_gate_order 注释)。
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import json
import os
import pathlib
import subprocess
import sys

import pytest
from pandora.common.v1 import errcode_pb2 as commonpb
from pandora.match.v1 import match_pb2 as matchpb

from pandorapy import errcode
from pandorapy import interceptors
from pandorapy import internalrpcauth
from pandorapy.services.matchmaker import conf as mconf
from pandorapy.services.matchmaker import main as mmain
from pandorapy.services.matchmaker import service as msvc
from pandorapy.services.matchmaker.presence_gate import MemberOfflineError

# 与 dev yaml 无关的最小可启动配置。刻意把 Redis 端点指向一个**不会被连**的洞:
# 本文件的用例全部在 Redis 闸之前就该失败,连上了反而说明某道闸漏了。
_MIN_YAML = """
server:
  grpc: {addr: ":29011"}
  http: {addr: ":29111"}
node:
  node_id: 1
  redis_client:
    host: "127.0.0.1:6399"
match:
  team_addr: "127.0.0.1:20010"
  match_resume_auth_secret: "pandora-test-match-resume-auth-key!!"
  match_resume_auth_audience: "matchmaker:test"
  map_id: 6
  game_mode: "5v5_ranked"
jwt:
  issuer: "pandora-login"
  audience: "pandora-client"
  secret: "pandora-test-jwt-secret-change-me!!!"
"""


def _write(tmp_path: pathlib.Path, body: str, name: str = "c.yaml") -> str:
    p = tmp_path / name
    p.write_text(body, encoding="utf-8")
    return str(p)


def _run_main(conf_path: str) -> tuple[int, list[dict]]:
    """在**子进程**里跑 main 并解析 stdout 上的结构化日志。返回 (exit_code, 事件列表)。

    ★ 为什么必须起子进程(两条都是别的服务踩过的):

      ① structlog 的 `capture_logs` 用不了 —— `plog.setup()` 会重装整条处理器链,
         把 capture_logs 挂上去的那条换掉,于是它恒返回空列表,测试会误以为
         "一条日志都没打"。
      ② 在**本进程**里调 `main()` 会让 `plog.setup()` 把 PrintLogger 绑到 pytest
         当时那个被 capsys 接管的 stdout 上;capsys 在用例结束后关掉它,
         **后续所有用例**打日志就炸 —— 典型的跨用例共享资源污染。

    解析 stdout 还有个好处:验的正是 Loki 真会采到的那一份,不是测试专用旁路。
    """
    env = dict(os.environ)
    py_root = pathlib.Path(__file__).resolve().parents[1]
    env["PYTHONPATH"] = os.pathsep.join([str(py_root), str(py_root / "gen")])
    env["PYTHONIOENCODING"] = "utf-8"
    proc = subprocess.run(
        [sys.executable, "-m", "pandorapy.services.matchmaker.main", "-conf", conf_path],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        cwd=str(py_root),
        timeout=90,
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
    assert mmain._parse_args([]).conf == "etc/matchmaker-dev.yaml"  # noqa: SLF001
    assert mmain._parse_args(["-conf", "x.yaml"]).conf == "x.yaml"  # noqa: SLF001


# ── 启动闸 ①~⑥ ──────────────────────────────────────────────────────────────


def test_gate_config_load_failed_on_missing_file(tmp_path: pathlib.Path) -> None:
    code, logs = _run_main(str(tmp_path / "nope.yaml"))
    assert code == 1
    assert "config_load_failed" in _events(logs)


def test_gate_config_load_failed_on_broken_yaml(tmp_path: pathlib.Path) -> None:
    """★ yaml 语法错必须是 config_load_failed,**不是** config_scan_failed。

    Go 的 `c.Load()` 覆盖"读文件 + 解析 yaml"两步,两者失败同一个事件名;
    scan 那个名字在 Go 侧的含义是"结构/校验不过"。分错了会让排障的人去看模型定义,
    而问题在 yaml 有没有写坏。
    """
    code, logs = _run_main(_write(tmp_path, "match:\n  team_addr: [oops\n"))
    assert code == 1
    assert "config_load_failed" in _events(logs)


def test_gate_cellroute_init_failed_on_unsupported_mode(tmp_path: pathlib.Path) -> None:
    """cell_route.mode 非空 = Python 侧只实现单 Cell → 拒启。

    静默按单 Cell 跑的话玩家会被路由到错的 cell 且**不报错**(§14)。
    事件名与 Go 的 cellroute_init_failed 相同(告警按事件名建),只是位置比 Go 早。
    """
    path = _write(tmp_path, _MIN_YAML + '\ncell_route:\n  mode: "keyspace"\n')
    code, logs = _run_main(path)
    assert code == 1
    assert "cellroute_init_failed" in _events(logs)


def test_gate_config_validation_failed_on_missing_team_addr(tmp_path: pathlib.Path) -> None:
    """team_addr 留空且没显式 allow_missing_team → 拒启。

    留空的后果是**静默**的:StartMatch 不再校验队伍 + 对局结束不复位准备状态
    (INC-20260813-001 的两个根因),两者都不报错、不打 ERROR。
    """
    path = _write(tmp_path, _MIN_YAML.replace('team_addr: "127.0.0.1:20010"', 'team_addr: ""'))
    code, logs = _run_main(path)
    assert code == 1
    assert "config_validation_failed" in _events(logs)


def test_allow_missing_team_is_the_explicit_escape_hatch(tmp_path: pathlib.Path) -> None:
    """写了 allow_missing_team=true 就放行 —— 否则骨架档根本跑不起来。

    只验配置层(不起进程):跑到这一步说明它过了闸④,而不是被 team_addr 拦下。
    """
    body = _MIN_YAML.replace(
        'team_addr: "127.0.0.1:20010"', 'team_addr: ""\n  allow_missing_team: true'
    )
    cfg = mconf.Config.load(_write(tmp_path, body))
    cfg.validate_conf()  # 不抛即通过


def test_gate_config_validation_failed_on_shared_trust_domain_key(
    tmp_path: pathlib.Path,
) -> None:
    """★ resume 密钥与玩家 JWT 同钥 = 任何一个登录玩家都能签出内部凭据。

    这是"静默塌缩信任域"的典型形状:配置看起来完全正常,而
    ResolvePlayerMatchContext(能读出任意玩家的 battle 票)对所有玩家敞开。
    """
    body = _MIN_YAML.replace(
        'match_resume_auth_secret: "pandora-test-match-resume-auth-key!!"',
        'match_resume_auth_secret: "pandora-test-jwt-secret-change-me!!!"',
    )
    code, logs = _run_main(_write(tmp_path, body))
    assert code == 1
    assert "config_validation_failed" in _events(logs)


def test_gate_configtable_load_failed_on_missing_dir(tmp_path: pathlib.Path) -> None:
    """config_table.dir 配了却加载不了 = 启动强依赖失败,fail-closed。"""
    body = _MIN_YAML + '\nconfig_table:\n  dir: "no/such/configtable/dir"\n'
    code, logs = _run_main(_write(tmp_path, body))
    assert code == 1
    assert "configtable_load_failed" in _events(logs)


def test_gate_configtable_disabled_is_a_warning_not_a_failure(
    tmp_path: pathlib.Path,
) -> None:
    """★ 未配 dir 只 WARN,**不得**改成 fail-fast。

    方向错了会把"可降级增强"升级成"可用性事故":不带配置表的部署本来就跑得起来
    (StartMatch 跳过 map_id 表校验,历史行为)。
    走到 redis_endpoint_required 说明它确实越过了闸⑤。
    """
    body = _MIN_YAML.replace('    host: "127.0.0.1:6399"', "    host: \"\"")
    code, logs = _run_main(_write(tmp_path, body))
    assert code == 1
    names = _events(logs)
    assert "configtable_disabled" in names
    assert "redis_endpoint_required" in names


def test_startup_gate_order_configtable_before_redis(tmp_path: pathlib.Path) -> None:
    """★ 顺序即契约:同时坏掉时先报关卡表,再轮到 Redis。

    顺序漂移的代价不是"报错不好看":运维手册与告警按"第一个错误"分诊,
    两栈第一个错不同,同一份坏配置在 Go 上说"表加载失败"、Python 上说"Redis 没配",
    排障从此对不上。
    """
    body = (
        _MIN_YAML.replace('    host: "127.0.0.1:6399"', "    host: \"\"")
        + '\nconfig_table:\n  dir: "no/such/configtable/dir"\n'
    )
    code, logs = _run_main(_write(tmp_path, body))
    assert code == 1
    names = _events(logs)
    assert "configtable_load_failed" in names
    # Redis 那道闸根本不该被走到 —— 走到了说明 configtable 变成了非阻断。
    assert "redis_endpoint_required" not in names


# ── conf 默认值 / 判据符号 ────────────────────────────────────────────────────


def test_negative_duration_means_gate_disabled_not_default(tmp_path: pathlib.Path) -> None:
    """★ 判据必须是 `== 0` 而不是 `<= 0`。

    五个字段用**负值表达「显式关闭整道闸」**。写成 `<= 0` 的话,同一份写着
    `start_presence_grace: "-1s"` 的 yaml 会被 Python 兜回 30s ——
    运维以为关掉了在线闸,实际它还开着,而**两边都不报错**。
    """
    body = _MIN_YAML.replace(
        'map_id: 6',
        'map_id: 6\n  start_presence_grace: "-1s"\n'
        '  queue_absence_reap_after: "-1s"\n  start_match_cooldown: "-1s"\n'
        '  match_form_cooldown: "-1s"\n  no_capacity_requeue_delay: "-1s"',
    )
    cfg = mconf.Config.load(_write(tmp_path, body))
    m = cfg.match
    minus_one = _dt.timedelta(seconds=-1)
    assert m.start_presence_grace_td() == minus_one
    assert m.queue_absence_reap_after_td() == minus_one
    assert m.start_match_cooldown_td() == minus_one
    assert m.match_form_cooldown_td() == minus_one
    assert m.no_capacity_requeue_delay_td() == minus_one


@pytest.mark.parametrize(
    ("raw", "want"),
    [(0, mconf.DEFAULT_TEAM_SIZE), (-3, 1), (999, mconf.MAX_LEVEL_TEAM_SIZE), (5, 5)],
)
def test_team_size_is_clamped(tmp_path: pathlib.Path, raw: int, want: int) -> None:
    """team_size 先兜默认(`== 0`)再钳 [1, 50]。

    撮合按 need=side_count×team_size 预分配票据列表:负值会让**每张票都凑不满**
    (队列永远不成局,而且毫无错误日志),巨值会 OOM。
    """
    cfg = mconf.Config.load(
        _write(tmp_path, _MIN_YAML.replace("map_id: 6", f"map_id: 6\n  team_size: {raw}"))
    )
    assert cfg.match.team_size == want


def test_deprecated_enable_solo_match_is_merged_into_walk_in(tmp_path: pathlib.Path) -> None:
    """旧键用 OR **并入**而非覆盖。

    漏迁移的部署若被静默判成 false,PVE 实例会从「单人/整队直进副本」退化成
    「排队等对手撮合」,而 PVE 侧根本没有单边成局逻辑 —— 玩家永远等不到人。
    """
    cfg = mconf.Config.load(
        _write(tmp_path, _MIN_YAML.replace("map_id: 6", "map_id: 6\n  enable_solo_match: true"))
    )
    assert cfg.match.walk_in is True
    # 旧字段值不清空:main.py 靠它打 deprecated_config_key 迁移进度信号。
    assert cfg.match.enable_solo_match is True


def test_dev_yaml_loads_and_validates(repo_root: pathlib.Path) -> None:
    """两份真 yaml 都能被 Python 版加载并通过校验 —— 迁移期只维护一份配置。"""
    etc = repo_root / "services" / "matchmaking" / "matchmaker" / "etc"
    for name in ("matchmaker-dev.yaml", "matchmaker-pve.yaml"):
        cfg = mconf.Config.load(str(etc / name))
        cfg.validate_conf()
        assert cfg.server.grpc.addr
        assert cfg.match.game_mode


# ── service 层:身份闸 ───────────────────────────────────────────────────────


class FakeContext:
    """最小 ServicerContext:只需要 invocation_metadata()。"""

    def __init__(self, player_id: int = 0, extra: dict[str, str] | None = None) -> None:
        md: list[tuple[str, str]] = []
        if player_id:
            md.append((interceptors.METADATA_KEY_PLAYER_ID, str(player_id)))
        for k, v in (extra or {}).items():
            md.append((k, v))
        self._md = tuple(md)

    def invocation_metadata(self):  # noqa: ANN201
        return self._md


class FakeSnowflake:
    """确定性发号器 —— 随机 ID 验不出"service 层用的是自己铸的号"。"""

    def __init__(self, start: int = 7000) -> None:
        self._next = start

    def generate(self) -> int:
        self._next += 1
        return self._next


class FakeUsecase:
    """记录调用参数的 usecase 替身。"""

    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.start_raises: BaseException | None = None
        self.resolve_result = matchpb.ResolvePlayerMatchContextResponse(
            state=matchpb.PLAYER_MATCH_CONTEXT_STATE_UNSPECIFIED
        )

    async def start_match(self, ticket_id, team_id, captain_id, map_id, entry_mode):  # noqa: ANN001
        self.calls.append(("start", ticket_id, team_id, captain_id, map_id, entry_mode))
        if self.start_raises is not None:
            raise self.start_raises
        return 9001

    async def cancel_match(self, player_id):  # noqa: ANN001
        self.calls.append(("cancel", player_id))

    async def confirm_match(self, player_id, match_id, accept):  # noqa: ANN001
        self.calls.append(("confirm", player_id, match_id, accept))

    async def get_match_progress(self, caller, handle):  # noqa: ANN001
        self.calls.append(("progress", caller, handle))
        return matchpb.MatchProgress(match_id=handle or 1)

    async def release_match(self, match_id, player_ids):  # noqa: ANN001
        self.calls.append(("release", match_id, list(player_ids)))

    async def resolve_player_match_context(self, player_id):  # noqa: ANN001
        self.calls.append(("resolve", player_id))
        return self.resolve_result


class AcceptAllAuth:
    async def verify(self, metadata, full_method, subject) -> None:  # noqa: ANN001
        return None


class RaisingAuth:
    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    async def verify(self, metadata, full_method, subject) -> None:  # noqa: ANN001
        raise self._exc


def _svc(uc: FakeUsecase, auth=AcceptAllAuth()) -> msvc.MatchService:  # noqa: ANN001,B008
    return msvc.MatchService(uc, FakeSnowflake(), auth)


def test_start_match_requires_caller_identity() -> None:
    """caller==0 → ERR_UNAUTHORIZED,且**一个副作用都不能发生**。

    Envoy 已在路由层 require JWT;这道兜底管的是内网直连 —— 少了它,集群里
    任何 Pod 都能以任意身份把任意队伍拉进撮合。
    """
    uc = FakeUsecase()
    resp = asyncio.run(
        _svc(uc).StartMatch(matchpb.StartMatchRequest(team_id=5), FakeContext(0))
    )
    assert resp.code == commonpb.ERR_UNAUTHORIZED
    assert uc.calls == []


def test_start_match_uses_caller_not_request_body() -> None:
    """★ R5:captain 恒等于鉴权上下文身份,请求体里的 team_id 只是透传。

    ticket_id 必须来自 service 层自己的 snowflake(不是客户端给的),
    否则客户端可以指定一个已存在的 ticket_id 去覆盖别人的票。
    """
    uc = FakeUsecase()
    resp = asyncio.run(
        _svc(uc).StartMatch(
            matchpb.StartMatchRequest(team_id=5, map_id=6, entry_mode=1), FakeContext(42)
        )
    )
    assert resp.code == commonpb.OK
    assert resp.match_id == 9001
    kind, ticket_id, team_id, captain_id, map_id, entry_mode = uc.calls[0]
    assert (kind, team_id, captain_id, map_id, entry_mode) == ("start", 5, 42, 6, 1)
    assert ticket_id == 7001  # service 层铸的号,不是请求体带的


def test_start_match_absent_players_cross_the_wire() -> None:
    """★ 4011 的缺席名单必须走**结构化字段**。

    只拼在服务端 error 文本里的话,队长看不出该等谁(INC-20260813-001 行动项)。
    """
    uc = FakeUsecase()
    uc.start_raises = MemberOfflineError([101, 202], 30.0)
    resp = asyncio.run(
        _svc(uc).StartMatch(matchpb.StartMatchRequest(team_id=5), FakeContext(42))
    )
    assert resp.code == errcode.ErrMatchMemberOffline
    assert list(resp.absent_player_ids) == [101, 202]


def test_cancel_match_client_path_ignores_request_body_player_id() -> None:
    """客户端路径:只能取消自己的排队,请求体里伪造的 player_id 无效。"""
    uc = FakeUsecase()
    resp = asyncio.run(
        _svc(uc).CancelMatch(matchpb.CancelMatchRequest(player_id=777), FakeContext(42))
    )
    assert resp.code == commonpb.OK
    assert uc.calls == [("cancel", 42)]


def test_cancel_match_internal_path_uses_request_body_player_id() -> None:
    """★ caller==0 是**内部路径**(team 离队/踢人联动撤票),按 req.player_id 取消。

    写成"一律取 caller"会让 team 的联动撤票整条静默失效:成员被踢出队伍后票据
    还在队列里,他会被拉进一场自己已不在队的对局。
    """
    uc = FakeUsecase()
    resp = asyncio.run(
        _svc(uc).CancelMatch(matchpb.CancelMatchRequest(player_id=777), FakeContext(0))
    )
    assert resp.code == commonpb.OK
    assert uc.calls == [("cancel", 777)]


def test_cancel_match_without_any_identity_is_rejected() -> None:
    uc = FakeUsecase()
    resp = asyncio.run(
        _svc(uc).CancelMatch(matchpb.CancelMatchRequest(), FakeContext(0))
    )
    assert resp.code == commonpb.ERR_UNAUTHORIZED
    assert uc.calls == []


def test_confirm_match_requires_match_id() -> None:
    uc = FakeUsecase()
    resp = asyncio.run(
        _svc(uc).ConfirmMatch(matchpb.ConfirmMatchRequest(accept=True), FakeContext(42))
    )
    assert resp.code == commonpb.ERR_INVALID_ARG
    assert uc.calls == []


def test_get_match_progress_allows_zero_match_id() -> None:
    """match_id==0 是重连兜底(换设备丢句柄),不是非法参数 —— biz 按 caller 反查。"""
    uc = FakeUsecase()
    resp = asyncio.run(
        _svc(uc).GetMatchProgress(matchpb.GetMatchProgressRequest(), FakeContext(42))
    )
    assert resp.code == commonpb.OK
    assert uc.calls == [("progress", 42, 0)]


def test_release_match_rejects_player_jwt() -> None:
    """★ 系统面:带玩家 JWT 一律拒。

    不拒的话任何登录玩家都能用任意 match_id 摧毁他人的在局撮合状态
    (删票据 / claim / match)—— griefing 且绕过不变量 §1。
    """
    uc = FakeUsecase()
    resp = asyncio.run(
        _svc(uc).ReleaseMatch(matchpb.ReleaseMatchRequest(match_id=9), FakeContext(42))
    )
    assert resp.code == commonpb.ERR_PERMISSION_DENY
    assert uc.calls == []


def test_release_match_requires_match_id() -> None:
    uc = FakeUsecase()
    resp = asyncio.run(
        _svc(uc).ReleaseMatch(matchpb.ReleaseMatchRequest(), FakeContext(0))
    )
    assert resp.code == commonpb.ERR_INVALID_ARG
    assert uc.calls == []


def test_release_match_internal_path_ok() -> None:
    uc = FakeUsecase()
    resp = asyncio.run(
        _svc(uc).ReleaseMatch(
            matchpb.ReleaseMatchRequest(match_id=9, player_ids=[1, 2]), FakeContext(0)
        )
    )
    assert resp.code == commonpb.OK
    assert uc.calls == [("release", 9, [1, 2])]


def test_resolve_rejects_player_jwt() -> None:
    uc = FakeUsecase()
    resp = asyncio.run(
        _svc(uc).ResolvePlayerMatchContext(
            matchpb.ResolvePlayerMatchContextRequest(player_id=1), FakeContext(42)
        )
    )
    assert resp.code == commonpb.ERR_PERMISSION_DENY
    assert uc.calls == []


def test_resolve_without_auth_is_unavailable_never_open() -> None:
    """★ 未配验签器 → 恒 ERR_UNAVAILABLE,**绝不放行**。

    "没配就不验"会把一个能读出任意玩家 battle 票的接口对整个集群网络开放。
    """
    uc = FakeUsecase()
    resp = asyncio.run(
        msvc.MatchService(uc, FakeSnowflake(), None).ResolvePlayerMatchContext(
            matchpb.ResolvePlayerMatchContextRequest(player_id=1), FakeContext(0)
        )
    )
    assert resp.code == commonpb.ERR_UNAVAILABLE
    assert uc.calls == []


def test_resolve_replay_store_unavailable_is_retryable_not_denied() -> None:
    """★ 重放存储不可用 → ERR_UNAVAILABLE(可重试),不是 PERMISSION_DENY。

    映射成越权的话,Redis 抖一下就会让 login 的冷重连整块失败,而且看上去像
    "密钥配错了" —— 排障方向完全反了。
    """
    uc = FakeUsecase()
    svc = _svc(uc, RaisingAuth(internalrpcauth.ErrUnavailable("redis down")))
    resp = asyncio.run(
        svc.ResolvePlayerMatchContext(
            matchpb.ResolvePlayerMatchContextRequest(player_id=1), FakeContext(0)
        )
    )
    assert resp.code == commonpb.ERR_UNAVAILABLE
    assert uc.calls == []


def test_resolve_bad_signature_is_permission_deny() -> None:
    uc = FakeUsecase()
    svc = _svc(uc, RaisingAuth(internalrpcauth.ErrUnauthorized("bad sig")))
    resp = asyncio.run(
        svc.ResolvePlayerMatchContext(
            matchpb.ResolvePlayerMatchContextRequest(player_id=1), FakeContext(0)
        )
    )
    assert resp.code == commonpb.ERR_PERMISSION_DENY
    assert uc.calls == []


def test_resolve_success_sets_ok_code() -> None:
    uc = FakeUsecase()
    resp = asyncio.run(
        _svc(uc).ResolvePlayerMatchContext(
            matchpb.ResolvePlayerMatchContextRequest(player_id=55), FakeContext(0)
        )
    )
    assert resp.code == commonpb.OK
    assert uc.calls == [("resolve", 55)]


# ── MultiCallerVerifier ─────────────────────────────────────────────────────


class CountingVerifier:
    """记录被调次数的验签器替身 —— 用来证明 nonce **没有**被别人先消费掉。"""

    def __init__(self, caller: str) -> None:
        self.caller = caller
        self.calls = 0

    async def verify(self, metadata, full_method, subject) -> None:  # noqa: ANN001
        self.calls += 1


def test_multi_caller_routes_by_caller_identity() -> None:
    login = CountingVerifier("login")
    team = CountingVerifier("team")
    mv = msvc.MultiCallerVerifier(login, team)
    asyncio.run(
        mv.verify({internalrpcauth.CALLER_METADATA_KEY: "team"}, "/m", 1)
    )
    assert (login.calls, team.calls) == (0, 1)


def test_multi_caller_unknown_caller_consumes_no_nonce() -> None:
    """★ 未知 caller 直接拒,且**一个验签器都不调**。

    "挨个试一遍"是错的:每个 verify 都会**消费 nonce**,第一个验签器会把一份合法
    凭据的 nonce 先吃掉,第二个再验就成了"重放" —— 于是 team 的每次合法调用都被拒,
    而日志上看起来是重放攻击。
    """
    login = CountingVerifier("login")
    team = CountingVerifier("team")
    mv = msvc.MultiCallerVerifier(login, team)
    with pytest.raises(internalrpcauth.ErrUnauthorized):
        asyncio.run(mv.verify({internalrpcauth.CALLER_METADATA_KEY: "ghost"}, "/m", 1))
    with pytest.raises(internalrpcauth.ErrUnauthorized):
        asyncio.run(mv.verify({}, "/m", 1))
    assert (login.calls, team.calls) == (0, 0)


def test_multi_caller_rejects_duplicate_caller() -> None:
    """两把钥匙挂同一个 caller = 其中一把永远不生效,而且没有任何信号。"""
    with pytest.raises(ValueError, match="duplicate caller"):
        msvc.MultiCallerVerifier(CountingVerifier("login"), CountingVerifier("login"))


def test_multi_caller_requires_at_least_one() -> None:
    with pytest.raises(ValueError, match="at least one"):
        msvc.MultiCallerVerifier()


# ── kafka 推送适配 ───────────────────────────────────────────────────────────


class FakeProducer:
    def __init__(self, result) -> None:  # noqa: ANN001
        self.result = result
        self.seen: list[tuple] = []

    async def push_to_players(self, caller_player_id, to_player_ids, payload):  # noqa: ANN001
        self.seen.append((caller_player_id, list(to_player_ids), payload))
        return self.result

    async def close(self) -> None:
        return None


def test_pusher_passes_caller_zero_through_unchanged() -> None:
    """★ 推送原则 3 的**例外**:caller_player_id=0 = 发给所有人含发起方。

    适配层若"顺手"把 caller 改成收件人,组队里非队长成员唯一的 READY 通道会永久
    静默丢失 —— 他们没有 match_id、不能轮询,推送是唯一渠道。
    """
    p = FakeProducer((1, None))
    sent = asyncio.run(mmain.KafkaMatchPusher(p).push_match_progress(0, [42], b"x"))
    assert sent == 1
    assert p.seen == [(0, [42], b"x")]


def test_pusher_turns_partial_failure_into_an_exception() -> None:
    """★ `push_to_players` 部分失败**不抛异常**,只返回 (sent, last_err)。

    直接把 producer 当 pusher 用的话,`push_ready_strict` 会把交付失败当成成功:
    match 被移出 active ZSET、补推循环不再重试,非队长成员永远收不到 READY,
    而日志一片正常。这一层就是为了修这个语义差。
    """
    p = FakeProducer((0, ConnectionError("broker down")))
    with pytest.raises(ConnectionError):
        asyncio.run(mmain.KafkaMatchPusher(p).push_match_progress(0, [42], b"x"))


def test_pusher_zero_delivered_is_a_failure() -> None:
    """没报错但一个都没送到,同样必须当失败 —— 否则重试驱动同样被关掉。"""
    p = FakeProducer((0, None))
    with pytest.raises(ConnectionError):
        asyncio.run(mmain.KafkaMatchPusher(p).push_match_progress(0, [42], b"x"))


# ── biz 装配:两个 mixin 都必须挂上 ──────────────────────────────────────────


def test_usecase_exposes_every_rpc_entrypoint() -> None:
    """★ `MatchUsecase` 必须同时挂 MatchRpcMixin 与 MatchLoopMixin。

    漏挂 MatchRpcMixin 是**完全静默的**:类照常构造、进程照常起来、单测照常全绿
    (没有哪条测试碰得到那五个方法),而线上表现是除 StartMatch 外每个 RPC 都回
    ERR_UNKNOWN —— service 层的宽 except 把 AttributeError 压成 in-band 错误码,
    gRPC status 还是 OK,access log 记的是 rpc_ok。2026-08-19 起真进程 e2e
    打过一遍这五个方法才发现,所以这条断言必须留在 CI 里。
    """
    from pandorapy.services.matchmaker import biz as mbiz

    for name in (
        "start_match",                    # biz 本体
        "cancel_match",                   # ↓ MatchRpcMixin
        "confirm_match",
        "release_match",
        "get_match_progress",
        "resolve_player_match_context",
        "run_match_loop",                 # ↓ MatchLoopMixin
        "match_tick_once",
    ):
        assert callable(getattr(mbiz.MatchUsecase, name, None)), f"MatchUsecase 缺 {name}"


def test_unexpected_exception_is_logged_not_silently_mapped(capsys) -> None:  # noqa: ANN001
    """★ 非 PandoraError → ERR_UNKNOWN 时必须留一条日志。

    不留的话整条链路零错误痕迹:in-band code + OK status + access log 记 rpc_ok,
    客户端只看到一个 1。业务失败(PandoraError)照旧不打 —— 那是正常分支。
    """
    from structlog.testing import capture_logs

    class Boom(FakeUsecase):
        async def get_match_progress(self, caller, handle):  # noqa: ANN001
            raise RuntimeError("wiring gone")

    with capture_logs() as logs:
        resp = asyncio.run(
            _svc(Boom()).GetMatchProgress(
                matchpb.GetMatchProgressRequest(match_id=3), FakeContext(42)
            )
        )
    assert resp.code == errcode.ErrUnknown
    assert "match_rpc_internal_error" in [e.get("event") for e in logs]

    # 业务失败不打这条(否则每个正常的 4001 都会刷屏)。
    class Biz(FakeUsecase):
        async def get_match_progress(self, caller, handle):  # noqa: ANN001
            raise errcode.PandoraError(errcode.ErrMatchNotFound, "nope")

    with capture_logs() as logs2:
        resp2 = asyncio.run(
            _svc(Biz()).GetMatchProgress(
                matchpb.GetMatchProgressRequest(match_id=3), FakeContext(42)
            )
        )
    assert resp2.code == errcode.ErrMatchNotFound
    assert "match_rpc_internal_error" not in [e.get("event") for e in logs2]
