"""login 的配置默认值、29 道启动闸、REST 口径与 service 层身份边界。

覆盖的是「起不来 / 起错了 / 谁都能变成谁」这一族缺陷 —— 它们都不会在普通业务
测试里露头:

  - conf 默认值与 Go 的 Defaults() 分叉:同一份 yaml 喂两个实现行为不同,两边都不报错
    (最阴的一条:`login_fail_limit: -1` 在 Go 是"显式关闭失败配额",若 Python 写成
     `<= 0` 判据就会被兜成 5 —— 运维以为关了,Python 副本仍在锁账号)
  - 启动闸漏掉或顺序不同:同一份坏配置在两栈上报**不同的第一个错误**,
    Loki 上按事件名建的告警从此对不上
  - 安全开关静默变形:缺 Redis 时 session_generation_enforce 只会变成"永不强制"
  - REST JSON 口径漂移:`code: 0`(OK)字段消失 / snake_case,客户端解不出来而两边不报错
  - service 层身份边界搞反:账号态与玩家态混用 = 拿自己的 token 进别人的号

默认值 parity 刻意**从 Go 源码里读**而不是抄一份:抄一份的话,Go 改了默认值这个
测试照样绿(它验的是"我抄的值等于我抄的值")。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import pathlib
import re

import pytest

from mysqlfixture import MYSQL_DSN as DSN
from mysqlfixture import skip_only_if_mysql_is_down

from pandora.common.v1 import errcode_pb2
from pandora.login.v1 import login_pb2

from pandorapy import dsticket as pdsticket
from pandorapy import errcode
from pandorapy import log as plog
from pandorapy.services.login import conf as lconf
from pandorapy.services.login import main as lmain
from pandorapy.services.login import passwd as lpasswd
from pandorapy.services.login import rest as lrest
from pandorapy.services.login import service as lsvc

GO_CONF = "services/account/login/internal/conf/conf.go"
GO_MAIN = "services/account/login/cmd/login/main.go"
GO_SERVICE = "services/account/login/internal/service/login.go"
GO_PROTO = "proto/pandora/login/v1/login.proto"
DEV_YAML = "services/account/login/etc/login-dev.yaml"
ACCOUNT_DDL = "deploy/mysql-init/02-account-tables.sql"


# 合法的最小 yaml 骨架。★ node_id 必须给:省了会先被 snowflake 静态号段闸拦下
# (0 是 UE DS 本地发号器的保留号),于是被测的那道闸根本走不到,而测试照样"红得对"。
_MIN_NODE = "node:\n  node_id: 1\n"
# HS256 密钥必须 >= 32 字节,否则会先被 auth_signer_init_failed 拦下。
_DEV_SECRET = "pandora-dev-jwt-secret-change-me-32!"


# ── 配置:默认值与 Go 逐个对齐 ───────────────────────────────────────────────


def test_defaults_match_go_source(repo_root: pathlib.Path) -> None:
    """每个默认值都从 Go 源码抓出来比对(含判据符号)。"""
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")

    cfg = lconf.Config()
    cfg.apply_defaults()

    # 端口:Envoy cluster / run_services.ps1 端口检查 / K8s Service 都钉在这两个数上。
    assert cfg.server.grpc.addr == ":20001"
    assert cfg.server.http.addr == ":21001"
    assert '":20001"' in src and '":21001"' in src

    assert cfg.login.session_token_ttl_td().total_seconds() == 24 * 3600
    assert cfg.login.ds_ticket_ttl_td().total_seconds() == 5 * 60
    assert cfg.login.device_retention_days == 90
    assert cfg.login.player_no_start == 1
    assert cfg.login.mock_hub_ds_addr == "127.0.0.1:7777"
    assert cfg.login.login_fail_limit == 5
    assert cfg.login.login_fail_window_td().total_seconds() == 15 * 60
    assert cfg.login.login_fail_lock_td().total_seconds() == 5 * 60
    assert cfg.login.jwt.issuer == "pandora-login"
    assert cfg.login.jwt.audience == "pandora-client"
    assert cfg.login.jwt.secret == _DEV_SECRET

    for literal in (
        "24 * time.Hour",
        "5 * time.Minute",
        "127.0.0.1:7777",
        "pandora-login",
        "pandora-client",
        _DEV_SECRET,
    ):
        assert literal in src, literal
    assert re.search(r"DeviceRetentionDays\s*(<=|==)\s*0", src)
    assert re.search(r"PlayerNoStart\s*==\s*0", src)


def test_jwt_ttls_default_from_already_defaulted_login_ttls() -> None:
    """顺序即契约:jwt.session_ttl 的默认值取自**已经兜好底的** login.session_token_ttl。

    两句调换的后果:没写 session_token_ttl 的 yaml 会让 jwt.session_ttl 停在 0,
    签出来的 token 生下来就过期 —— 而配置层没有任何信号。
    """
    cfg = lconf.Config()
    cfg.apply_defaults()
    assert cfg.login.jwt.session_ttl == cfg.login.session_token_ttl
    assert cfg.login.jwt.ds_ticket_ttl == cfg.login.ds_ticket_ttl


def test_negative_login_fail_limit_is_preserved(repo_root: pathlib.Path) -> None:
    """`== 0` 判据(不是 `<= 0`):负值 = 显式关闭失败配额,必须原样保留。

    写成 `<= 0` 的后果:运维以为关了失败配额,Python 副本仍在锁账号,而两边都不报错。
    """
    cfg = lconf.Config.model_validate({"login": {"login_fail_limit": -1}})
    cfg.apply_defaults()
    assert cfg.login.login_fail_limit == -1
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")
    assert re.search(r"LoginFailLimit\s*==\s*0", src), "Go 侧改成 <=0 了,Python 必须跟着改"


def test_dev_yaml_loads_and_validates(repo_root: pathlib.Path) -> None:
    """Go 与 Python 读的是**同一份** etc/login-dev.yaml,不另建配置文件。"""
    cfg = lconf.Config.load(str(repo_root / DEV_YAML))
    cfg.validate_conf()
    assert cfg.server.grpc.addr == ":20001"
    assert cfg.server.http.addr == ":21001"
    assert cfg.login.jwt.audience == "pandora-client"
    # dev 档不启 capability fence(authority_mode=legacy 且不要求归属绑定)。
    _, enabled = cfg.capability_fence()
    assert enabled is False


@pytest.mark.parametrize(
    ("patch", "needle"),
    [
        # v2 签发启用却没指定 active_kid:轮换窗口内无法机械确认"这个副本用的是哪把私钥"。
        ({"ds_ticket": {"private_key_file": "k.pem"}}, "active_kid"),
        # 校验器启用却缺 keyset_revision:两个副本可能各自加载了不同的重叠期 JWKS。
        ({"ds_ticket": {"jwks_file": "j.json", "active_kid": "k1"}}, "keyset_revision"),
        # 归属绑定是 fail-closed 门,缺任一前提都会静默变形成"永远放行"。
        ({"require_hub_assignment_binding": True}, "redis_client"),
    ],
)
def test_validate_conf_rejects_half_configured_gates(patch: dict, needle: str) -> None:
    cfg = lconf.Config.model_validate({"login": patch})
    cfg.apply_defaults()
    with pytest.raises(ValueError, match=needle):
        cfg.validate_conf()


def test_authority_mode_typo_is_rejected() -> None:
    """拼错的 authority_mode 会被 `== "redis"` 判成 false → 静默退回 legacy。

    运维以为开了 Model B,实际在线入场权威门根本没装,而启动日志全绿。
    """
    cfg = lconf.Config.model_validate({"ds_auth": {"authority_mode": "reids"}})
    cfg.apply_defaults()
    with pytest.raises(ValueError, match="authority_mode"):
        cfg.validate_conf()


# ── 启动闸 ────────────────────────────────────────────────────────────────


class _Recorder:
    """把 structlog 换成事件名记录器。闸本身**不 mock** —— 验的就是它们真被执行到。"""

    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    def __getattr__(self, level: str):  # noqa: ANN204
        def emit(event: str = "", **_kw: object) -> None:
            self.events.append((level, event))

        return emit

    def names(self) -> list[str]:
        return [e for _, e in self.events]


@contextlib.contextmanager
def _capture():  # noqa: ANN202
    rec = _Recorder()
    real_setup, real_get = plog.setup, plog.get
    plog.setup = lambda *_a, **_k: rec  # type: ignore[assignment]
    plog.get = lambda *_a, **_k: rec  # type: ignore[assignment]
    try:
        yield rec
    finally:
        plog.setup, plog.get = real_setup, real_get


def _run(yaml_text: str, tmp_path: pathlib.Path) -> tuple[int, list[str]]:
    path = tmp_path / "login.yaml"
    path.write_text(yaml_text, encoding="utf-8")
    with _capture() as rec:
        rc = lmain.main(["-conf", str(path)])
    return rc, rec.names()


async def _run_async(yaml_text: str, tmp_path: pathlib.Path) -> tuple[int, list[str]]:
    """异步用例专用:直接 await _main_async。

    不能复用同步的 `_run` —— 它走 `lmain.main()` → `asyncio.run()`,而异步用例里
    已经有一个运行中的事件循环,`asyncio.run` 会抛 RuntimeError,被 main 的兜底
    翻译成 app_run_failed。那时测试红得**看起来像被测的闸没触发**,而真因是夹具。
    """
    path = tmp_path / "login.yaml"
    path.write_text(yaml_text, encoding="utf-8")
    with _capture() as rec:
        rc = await lmain._main_async(lmain._parse_args(["-conf", str(path)]))
    return rc, rec.names()


def test_missing_conf_file_is_config_load_failed(tmp_path: pathlib.Path) -> None:
    """Go 的 c.Load() 覆盖"读文件 + 解析"两步,两者都归 config_load_failed。

    分错的后果:Loki 上按事件名建的告警对不上 = 静默失去覆盖。
    """
    with _capture() as rec:
        rc = lmain.main(["-conf", str(tmp_path / "nope.yaml")])
    assert rc == 1
    assert rec.names() == ["service_starting", "config_load_failed"]


def test_cell_route_mode_reports_its_own_event(tmp_path: pathlib.Path) -> None:
    """cell_route.mode 非空 → 拒启,且事件名必须是 cellroute_init_failed。

    折成 config_scan_failed 的话,排障的人照着事件名去查配置结构,而真凶是
    「Python 侧只实现单 Cell」。事件名错比没有事件名更难查。
    """
    rc, names = _run(_MIN_NODE + 'cell_route:\n  mode: "static"\n', tmp_path)
    assert rc == 1
    assert "cellroute_init_failed" in names
    assert "config_scan_failed" not in names


def test_config_validation_gate_fires_before_snowflake(tmp_path: pathlib.Path) -> None:
    """闸序:cfg.Validate()(main.go:89)在 snowflake(main.go:96)之前。

    顺序不同 = 同一份坏配置在两栈上报不同的第一个错误。
    """
    rc, names = _run(
        # node_id 故意也是非法的(0):若顺序反了,先报的会是 snowflake_init_failed。
        'ds_auth:\n  authority_mode: "reids"\n',
        tmp_path,
    )
    assert rc == 1
    assert "config_validation_failed" in names
    assert "snowflake_init_failed" not in names


def test_snowflake_static_node_id_zero_is_rejected(tmp_path: pathlib.Path) -> None:
    """node_id=0 是 UE DS 本地发号器的保留号:用它铸的 ID 会与 DS 本地铸的逐位相同。

    漏配 node_id 时 pydantic 默认值恰好是 0 —— 不拦的话"忘了配"稳定落进最危险的那一格,
    且只在数据层面表现为重号,运行期零信号。
    """
    rc, names = _run("node:\n  node_id: 0\n", tmp_path)
    assert rc == 1
    assert "snowflake_init_failed" in names


def test_short_jwt_secret_is_rejected(tmp_path: pathlib.Path) -> None:
    """HS256 密钥 < 32 字节不会让任何东西报错,只是让签名可被暴力破解 ——
    伪造一个 sub=任意 player_id 的 token 就能冒充任何玩家。"""
    rc, names = _run(_MIN_NODE + 'login:\n  jwt:\n    secret: "short"\n', tmp_path)
    assert rc == 1
    assert "auth_signer_init_failed" in names


def test_mysql_dsn_required(tmp_path: pathlib.Path) -> None:
    """账号库是强依赖:没有它 login 连"这个账号存不存在"都答不了。"""
    rc, names = _run(_MIN_NODE, tmp_path)
    assert rc == 1
    assert "mysql_dsn_required" in names
    # 顺序:JWT 装配在 MySQL 之前(与 Go 同),所以两条 auth 闸必须都没触发。
    assert "auth_signer_init_failed" not in names


def test_model_b_config_is_valid_but_main_still_refuses(tmp_path: pathlib.Path) -> None:
    """Model B(ds_auth.authority_mode=redis)是一份**合法配置**,拒绝发生在 main。

    这个区分要紧:如果拒绝发生在 conf 层,那就是"这份 yaml 写错了";发生在 main
    才是"这份 yaml 是对的,只是本实现不具备这个能力"。前者会误导运维去改配置。
    """
    raw = {
        "node": {"node_id": 1, "redis_client": {"host": "127.0.0.1:6380"}},
        "login": {
            "require_hub_assignment_binding": True,
            "hub": {"addr": "127.0.0.1:20021"},
            "locator": {"addr": "127.0.0.1:20006"},
            "hub_assignment_fence": {
                "etcd_endpoints": ["127.0.0.1:2379"],
                "keyset_revision": "r1",
            },
        },
        "ds_auth": {
            "mode": "enforce",
            "authority_mode": "redis",
            "fence": {
                "etcd_endpoints": ["127.0.0.1:2379"],
                "keyset_revision": "r1",
            },
        },
    }
    cfg = lconf.Config.model_validate(raw)
    cfg.apply_defaults()
    cfg.validate_conf()  # 合法:两把 fence 一致、四个前提齐备
    assert cfg.ds_auth.authority_mode_redis() is True
    assert cfg.capability_fence()[1] is True

    # 而 main 侧必须有两道 fail-closed,且都排在 gRPC server 构造之前 ——
    # 排在之后的话进程会先开始监听,k8s 判 Ready、流量切过来,再退出。
    # 同样去掉模块 docstring 再比(理由见 test_gate_order_matches_go)。
    src = pathlib.Path(lmain.__file__).read_text(encoding="utf-8").split('"""', 2)[2]
    for gate in ("model_b_requires_ds_ticket_v2_signer", "ds_admission_authority_incomplete"):
        assert src.index(f'"{gate}"') < src.index("build_grpc_server"), gate
    assert src.index('"login_ds_auth_fence_acquire_failed"') < src.index("pserver.run(")


def test_ds_admission_incomplete_is_fail_closed() -> None:
    """Model B 权威三件套残缺 → True(拒启)。

    ★ 这一支只在 `ds_auth.authority_mode=redis` 才走到,dev 与集成测试都到不了。
    本轮真的写出过 `ds_guard.mode()`(而 `mode` 是 property)——TypeError 会在
    **生产切 Model B 的那一刻**才炸。所以它必须是可直接调用、可断言的函数。
    """
    from pandorapy import dsauth as _dsauth

    class _Guard:
        def __init__(self, mode: _dsauth.Mode) -> None:
            self._m = mode

        @property
        def mode(self) -> _dsauth.Mode:
            return self._m

    rdb = object()
    # 三件齐 → 放行
    assert lmain.ds_admission_incomplete(_Guard(_dsauth.Mode.ENFORCE), rdb) is False
    # 缺 guard / guard 不是 enforce / 缺 Redis,任一 → 拒
    assert lmain.ds_admission_incomplete(None, rdb) is True
    assert lmain.ds_admission_incomplete(_Guard(_dsauth.Mode.PERMISSIVE), rdb) is True
    assert lmain.ds_admission_incomplete(_Guard(_dsauth.Mode.OFF), rdb) is True
    assert lmain.ds_admission_incomplete(_Guard(_dsauth.Mode.ENFORCE), None) is True


def test_passwd_backend_gate_exists_and_refuses_to_degrade() -> None:
    """缺 bcrypt 时唯一"能跑"的降级形态是跳过密码校验 = 任何密码登任何账号。

    所以本模块在缺包时**不提供任何可用路径**。这里直接验 require_backend 的方向。
    """
    assert lpasswd.AVAILABLE, "本环境应已装 bcrypt(pyproject 依赖)"
    src = pathlib.Path(lmain.__file__).read_text(encoding="utf-8")
    assert "passwd_backend_required" in src
    assert "lpasswd.require_backend()" in src


def test_bcrypt_roundtrip_and_bad_hash_is_not_a_server_fault() -> None:
    """脏哈希串归"这次没通过",不升级成 500 —— 否则玩家看到的是"服务器错误",
    而正确的排查方向是"这一行数据坏了"。"""
    h = lpasswd.hash_password("client-digest")
    assert h.startswith("$2")
    assert lpasswd.verify(h, "client-digest") is True
    assert lpasswd.verify(h, "other") is False
    assert lpasswd.verify("not-a-bcrypt-hash", "client-digest") is False
    with pytest.raises(errcode.PandoraError):
        lpasswd.hash_password("x" * 73)  # bcrypt 上限 72 字节:截断会让两个密码等价


# ── 闸的事件名与顺序:对着 Go 源码做机械核对 ────────────────────────────────


# login/main.py 当前的 fail-fast 事件名条数。见下面用例里"为什么要有下限"那段。
_MIN_FAIL_FAST_EVENTS = 28


def test_fail_fast_event_names_exist_in_go_main(repo_root: pathlib.Path) -> None:
    r"""Python 侧每一个 fail-fast 事件名都必须在 Go 的 main.go 里逐字存在。

    不这么核的话,某天有人"顺手"把 mysql_init_failed 改成 mysql_connect_failed,
    Loki 上按 Go 的名字建的告警就静默失去覆盖 —— 而两栈各自的测试都绿。
    Go 侧没有对应的检查,这条是**唯一**的守门人。

    ★ 两处必须一起有,少一处这道门就能悄悄归零(2026-08-19 修)

      ① 正则**接收者无关**。原判据写死 `logger\.error(`,而本文件被测的
         login/main.py 第 189、219 行自己就写着 `log = plog.get()` ——
         全仓 `log` 与 `logger` 两个名字并存(剔除 main.py 后 112 : 94),
         995 行还有一处 `plog.get().exception(...)`。新加一条闸时顺手写成
         `log.error("xxx")` 或 `plog.get().error("xxx")`,那个事件名从第一天起就在门外。
      ② **数量下限**,不只是非空。原来 `re.findall` 抓不到就是零次迭代、零断言、绿;
         而只补 `assert py_events` 仍盖不住"把 28 个几乎相同的 fail-fast 块收敛成一个
         `_fail(event, err)` 助手"——正则剩 1 条、非空、照样绿,27 个名字失去核对。
         实测:一次纯风格重构 + 漂移一个事件名 → 整份 test_login_main.py 37 passed。

    ⚠️ 盖不到的一条:login/main.py:278 是 `logger.error(exc.event, ...)`,事件名是
       运行期变量,任何基于源码正则的门都够不着它。别以为这条门是全覆盖。
    """
    go_src = (repo_root / GO_MAIN).read_text(encoding="utf-8")
    py_src = pathlib.Path(lmain.__file__).read_text(encoding="utf-8")
    # 判据**只认 `.error("<小写事件名>")` 这个形状,不看接收者**:写死 `logger\.`
    # 会漏掉 `log.error(` / `plog.get().error(` 这些同样真实的写法(login/main.py
    # 第 189、219 行自己就是 `log = plog.get()`,995 行是 `plog.get().exception(...)`)。
    # 实测放宽后今天仍抓到 28 个、零新增,不引入误红。
    py_events = re.findall(r'\.error\(\s*"([a-z0-9_]+)"', py_src)
    # 下限按当前实际条数钉死:少了说明日志调用形状变了,该跟着改正则,
    # 而不是把这行删掉 —— 这道门是 Loki 告警规则唯一的防漂移锚点。
    assert len(py_events) >= _MIN_FAIL_FAST_EVENTS, (
        f"只抓到 {len(py_events)} 个 fail-fast 事件名(应 >= {_MIN_FAIL_FAST_EVENTS})—— "
        f"多半是 login/main.py 的日志调用形状变了(换了 logger 的名字、或把这些块收敛成了"
        f"一个助手函数),正则要跟着改。抓不到就是零次迭代、零断言、绿,"
        f"而 Loki 上按 Go 事件名建的告警此刻已经失去覆盖。"
    )

    # Python 独有的几条,每条都有明确理由(不是漂移):
    python_only = {
        # bcrypt 在 Go 里编译进二进制,不可能缺;Python 侧是外部包。
        "passwd_backend_required",
        # Go 侧这条是 mustBuildMatchResolver 里的 panic(fmt.Sprintf(...)),没有事件名。
        # Python 不能用裸异常代替 —— 那条 traceback 不进结构化日志。
        "match_resume_auth_secret_invalid",
        # Go 侧对应 etcdnode.MustProvideSnowflake 的 klog.Errorf + os.Exit(1)
        # (在 pkg/snowflake/etcdnode/provider.go,不在 main.go),同样没有事件名。
        # 这两个名字是 Python 侧 12 个已迁服务的**统一约定**:与一个根本不存在的
        # 事件名对齐没有意义,而 fleet 内部一致才让告警规则只写一条。
        "snowflake_init_failed",
        "snowflake_nodeid_acquire_failed",
    }
    for name in py_events:
        if name in python_only:
            continue
        assert name in go_src, f"{name} 不在 Go main.go 里(事件名漂移或写错)"


def test_gate_order_matches_go() -> None:
    """关键闸的**相对顺序**必须与 Go 的**调用序**一致。

    顺序不同的后果不是"报错顺序好看不好看":同一份坏配置在两栈上报不同的第一个
    错误,值班的人照着 Loki 上的事件名去查,查到的是另一件事。

    ★ 判据用 Python 源码里的出现顺序,而**不能**拿 Go 源码的文本顺序去比:
    Go 的 mustBuildAccountRepo / mustBuildRedisRepos 等辅助函数都写在 main() 之下,
    文本顺序与调用顺序相反。下面这份序列是照着 main.go 的**调用图**读出来的
    (main.go:70 config → :96 snowflake → :108/:113 auth → :120 mustBuildAccountRepo
    → :190 mustBuildRedisRepos → 四个客户端 → v2 → :243 session enforce → ds_auth)。
    """
    # ★ 必须**去掉模块 docstring** 再比:头注释里也按同样顺序列了一遍闸,
    # 连着 docstring 一起 index() 的话,命中的全是注释里的位置 ——
    # 代码顺序改了、注释没改,这个用例照样绿(它验的是注释与注释一致)。
    py_src = pathlib.Path(lmain.__file__).read_text(encoding="utf-8").split('"""', 2)[2]

    ordered = [
        "config_load_failed",
        "config_scan_failed",
        "config_validation_failed",
        "auth_signer_init_failed",
        "auth_verifier_init_failed",
        "mysql_dsn_required",
        "mysql_init_failed",
        "mysql_strict_mode_required",
        "mysql_schema_check_failed",
        "account_backend_not_tidb",
        "account_collation_semantics_mismatch",
        "player_no_sweeper_disabled",
        "redis_ping_failed",
        "session_enforce_requires_redis_sessions",
        "ds_auth_guard_init_failed",
        "ds_admission_authority_incomplete",
    ]
    positions = [py_src.index(f'"{name}"') for name in ordered]
    assert positions == sorted(positions), (
        "闸序与 Go 调用序不符;第一个错位的是 "
        + ordered[next(i for i in range(1, len(positions)) if positions[i] < positions[i - 1])]
    )


def test_background_loops_all_go_through_safego() -> None:
    """裸 create_task 的协程抛异常后异常只躺在 Task 里:进程照跑、health 照答
    SERVING、日志零行 —— 那条循环已经死了却没人知道。"""
    src = pathlib.Path(lmain.__file__).read_text(encoding="utf-8")
    for name in ("login_device_sweep", "login_player_no_sweep", "db_capacity_guard"):
        assert f'safego.loop("{name}"' in src or f'safego.run_once("{name}"' in src, name
    assert "asyncio.create_task(" not in src


def test_cancelled_error_is_never_swallowed() -> None:
    """每一处 `except BaseException` 之前必须先 `except asyncio.CancelledError: raise`。

    吞掉取消的后果:该停的停不下来,§9.16 的「先摘流量 → 再排空在途」失效;
    启动路径上则是 Ctrl-C 被翻译成某道闸的失败,报出假的失败原因。
    """
    for mod in (lmain, lsvc, lrest, lpasswd):
        src = pathlib.Path(mod.__file__).read_text(encoding="utf-8")
        lines = src.splitlines()
        for i, line in enumerate(lines):
            if "except BaseException" not in line:
                continue
            # 窗口取 8 行:惯用写法是 `except asyncio.CancelledError:` +
            # 几行"为什么必须穿透"的注释 + `raise`,注释行数不固定。
            window = "\n".join(lines[max(0, i - 8) : i])
            assert "except asyncio.CancelledError" in window, (
                f"{mod.__name__}:{i + 1} 的 except BaseException 前面没有取消穿透"
            )


# ── REST:JSON 口径必须与 Kratos 逐字一致 ────────────────────────────────────


def test_rest_paths_match_proto_annotations(repo_root: pathlib.Path) -> None:
    """10 个 REST 路径逐条抄自 proto 的 google.api.http 注解,不是自己编的。"""
    from fastapi import FastAPI

    proto = (repo_root / GO_PROTO).read_text(encoding="utf-8")
    annotated = set(re.findall(r'post:\s*"([^"]+)"', proto))

    class _Stub:
        """只提供方法名,handler 不会被调用 —— 本用例只看路由表。"""

        def __getattr__(self, _name: str):  # noqa: ANN204
            async def _never(*_a: object, **_k: object) -> None:
                raise AssertionError("路由表用例不该调用 handler")

            return _never

    app = FastAPI()
    lrest.register(app, _Stub())
    registered = {
        r.path for r in app.routes if getattr(r, "methods", None) == {"POST"}
    }
    assert registered == annotated
    assert len(registered) == 10


def test_rest_json_emits_unpopulated_and_camel_case() -> None:
    """三项口径任意一项不同,客户端都会解不出来而**两边都不报错**:

      - 不 EmitUnpopulated:`code: 0`(OK)整个字段消失,而成功恰好是最常见的那种;
      - 用 snake_case:`session_token` vs `sessionToken`,客户端拿到空 token。
    """
    out = lrest._to_json(login_pb2.LoginResponse(code=errcode_pb2.OK, player_id=7))
    assert out["code"] == "OK"  # 枚举按名字(protojson 规则,两栈同)
    assert "sessionToken" in out and out["sessionToken"] == ""  # EmitUnpopulated
    assert "session_token" not in out  # 非 UseProtoNames
    assert out["playerId"] == "7"  # int64 在 protojson 里是字符串,两栈同规则


async def test_rest_discards_unknown_fields() -> None:
    """客户端多带一个新字段时老服务端不能 400 —— 否则滚动升级期全挂。"""

    class _Req:
        async def body(self) -> bytes:
            return b'{"account":"a","brandNewField":1}'

    msg = await lrest._read_request(_Req(), login_pb2.LoginRequest())
    assert msg is not None
    assert msg.account == "a"


async def test_rest_round_trip_carries_identity_headers() -> None:
    """一条真实的 REST 往返:HTTP 头 → metadata 形状 → servicer → protojson。

    只验路由表不够 —— 身份头搬运是这一层唯一的实质逻辑,搬丢了的表现是
    「REST 打过来一律 ERR_UNAUTHORIZED」,而 gRPC 面完全正常。
    """
    from fastapi import FastAPI
    from starlette.requests import Request

    class _Stub:
        async def get_player_no(self, player_id: int) -> int:
            assert player_id == 5, "身份头没搬进来"
            return 7

    app = FastAPI()
    lrest.register(app, lsvc.LoginService(_Stub(), object()))
    route = next(r for r in app.routes if getattr(r, "path", "") == "/v1/player-no/get")

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/v1/player-no/get",
        "headers": [(b"x-pandora-player-id", b"5")],
        "query_string": b"",
    }

    async def receive() -> dict:
        return {"type": "http.request", "body": b"{}", "more_body": False}

    resp = await route.endpoint(Request(scope, receive))
    assert resp.status_code == 200
    body = json.loads(resp.body)
    # code=OK 必须**出现在 body 里**(EmitUnpopulated),playerNo 是 camelCase 字符串。
    assert body == {"code": "OK", "playerNo": "7"}


async def test_rest_bad_json_is_400_not_a_business_code() -> None:
    """body 都没解开 = 服务端没执行任何业务判定。伪装成业务码会让客户端把
    "我发的 JSON 坏了"当成"服务端拒绝了我的请求",排查方向完全相反。"""

    class _Req:
        async def body(self) -> bytes:
            return b"{not json"

    assert await lrest._read_request(_Req(), login_pb2.LoginRequest()) is None
    resp = lrest._bad_request("x")
    assert resp.status_code == 400


# ── service 层:身份边界 ─────────────────────────────────────────────────────


class _Ctx:
    """最小 ServicerContext 替身,只提供 invocation_metadata()。"""

    def __init__(self, **headers: str) -> None:
        self._md = tuple((k.replace("_", "-"), v) for k, v in headers.items())

    def invocation_metadata(self):  # noqa: ANN201
        return self._md


class _StubLogin:
    def __init__(self) -> None:
        self.seen_account_id: int | None = None
        self.seen_client_ip: str | None = None

    async def list_account_roles(self, account_id: int):  # noqa: ANN201
        self.seen_account_id = account_id
        if account_id == 0:
            raise errcode.PandoraError(errcode.ErrUnauthorized, "no account")
        return []

    async def get_player_no(self, player_id: int) -> int:
        return 42 if player_id else 0

    async def login(self, account, password_hash, device_id, defer, client_ip=""):  # noqa: ANN001, ANN201
        self.seen_client_ip = client_ip
        raise errcode.PandoraError(errcode.ErrLoginAccountNotFound, "nope")


async def test_account_state_never_falls_back_to_player_header() -> None:
    """账号态身份**只能**来自 x-pandora-account-id。

    回退去读 x-pandora-player-id 的后果:一张玩家 SessionToken 也能列角色 / 换角色,
    「账号态」这层隔离整个被拆掉。
    """
    stub = _StubLogin()
    svc = lsvc.LoginService(stub, object())
    ctx = _Ctx(**{"x_pandora_player_id": "12345"})
    resp = await svc.ListAccountRoles(login_pb2.ListAccountRolesRequest(), ctx)
    assert stub.seen_account_id == 0
    assert resp.code == errcode_pb2.ERR_UNAUTHORIZED

    ctx2 = _Ctx(**{"x_pandora_account_id": "999"})
    await svc.ListAccountRoles(login_pb2.ListAccountRolesRequest(), ctx2)
    assert stub.seen_account_id == 999


async def test_business_failure_is_in_band_code_not_grpc_error() -> None:
    """业务失败必须是 `Response(code=...)` + gRPC status OK。

    改成 abort() 的话客户端会走到完全不同的错误分支(重试 / 弹窗 / 回登录页),
    而服务端日志一切正常 —— 迁移里最容易悄悄改掉的语义。
    """
    svc = lsvc.LoginService(_StubLogin(), object())
    resp = await svc.Login(login_pb2.LoginRequest(account="a"), _Ctx())
    assert resp.code == errcode_pb2.ERR_LOGIN_ACCOUNT_NOT_FOUND
    assert resp.session_token == ""


async def test_client_ip_comes_only_from_trusted_header() -> None:
    """未经 Envoy 的直连拿不到 IP → 失败配额的 IP 维度自动关闭。

    **不能**回落到 socket peer:那在 Envoy 后面恒等于网关 IP,会把整个集群的失败
    并成一个维度,一个人爆破就锁死所有人。
    """
    stub = _StubLogin()
    svc = lsvc.LoginService(stub, object())
    await svc.Login(login_pb2.LoginRequest(account="a"), _Ctx())
    assert stub.seen_client_ip == ""
    await svc.Login(
        login_pb2.LoginRequest(account="a"), _Ctx(**{"x_pandora_client_ip": "1.2.3.4"})
    )
    assert stub.seen_client_ip == "1.2.3.4"


async def test_get_player_no_requires_auth_and_zero_is_not_an_error() -> None:
    """code=OK 且 player_no=0 = 「仍在补号窗口内」,不是错误。

    把它当错误的话客户端会停止轮询,编号永远显示"生成中"。
    """
    svc = lsvc.LoginService(_StubLogin(), object())
    denied = await svc.GetPlayerNo(login_pb2.GetPlayerNoRequest(), _Ctx())
    assert denied.code == errcode_pb2.ERR_UNAUTHORIZED

    ok = await svc.GetPlayerNo(
        login_pb2.GetPlayerNoRequest(), _Ctx(**{"x_pandora_player_id": "5"})
    )
    assert ok.code == errcode_pb2.OK
    assert ok.player_no == 42


async def test_get_register_no_delegates_to_get_player_no() -> None:
    """兼容入口委托实现:两条路径的判定永远不会分叉。

    删除它之前必须先证明旧客户端已排空 —— 不能在滚动升级窗口内原地收缩 RPC。
    """
    svc = lsvc.LoginService(_StubLogin(), object())
    res = await svc.GetRegisterNo(
        login_pb2.GetRegisterNoRequest(), _Ctx(**{"x_pandora_player_id": "5"})
    )
    assert res.code == errcode_pb2.OK
    assert res.register_no == 42


async def test_issue_ds_ticket_hub_battle_routes_through_biz_authority() -> None:
    """hub / battle 必须走 biz 的路由权威,**不能**落到通用 `issue_ds_ticket`。

      hub    → `resolve_hub_endpoint_from_match`(locator 租约 + match 三态门),
               并把地址回给客户端;走通用签票 = 自签一张 allocator 没登记过的票,
               Hub DS 一律拒 = "登录成功进不去"。
      battle → `resolve_battle_endpoint`(roster 权威门);走通用签票 = 谁报一个
               match_id 谁就能拿到那局的进场票。且 battle **不回地址**:客户端
               此刻已连着那台 DS,回地址只会给它一个被旧值覆盖的机会。
    """

    generic_called = False

    class _Gate:
        async def require_current_session_token(self, *_a: object) -> None:
            return None

        async def resolve_hub_endpoint_from_match(
            self, player_id: int, source_match_id: int, sess_jti: str
        ):  # noqa: ANN202
            assert (player_id, source_match_id) == (5, 1)
            del sess_jti
            return "hub-ds:7777", "hub-ticket", 0

        async def resolve_battle_endpoint(
            self, player_id: int, match_id: int, sess_jti: str
        ):  # noqa: ANN202
            assert (player_id, match_id) == (5, 1)
            del sess_jti
            return "battle-ds:8888", "battle-ticket", 0

    class _Ticket:
        async def issue_ds_ticket(self, *_a: object):  # noqa: ANN202
            nonlocal generic_called
            generic_called = True
            return "t", 0

    svc = lsvc.LoginService(_Gate(), _Ticket())

    hub = await svc.IssueDSTicket(
        login_pb2.IssueDSTicketRequest(ds_type="hub", target_id=1),
        _Ctx(**{"x_pandora_player_id": "5"}),
    )
    assert hub.code == errcode_pb2.OK
    assert hub.ticket == "hub-ticket"
    assert hub.hub_ds_addr == "hub-ds:7777"

    battle = await svc.IssueDSTicket(
        login_pb2.IssueDSTicketRequest(ds_type="battle", target_id=1),
        _Ctx(**{"x_pandora_player_id": "5"}),
    )
    assert battle.code == errcode_pb2.OK
    assert battle.ticket == "battle-ticket"
    assert battle.hub_ds_addr == "", "battle 分支绝不回地址"

    assert generic_called is False, "hub/battle 不得落到通用签票路径"


async def test_issue_ds_ticket_delivery_fence_withholds_ticket() -> None:
    """交付终检失败必须**扣留**已签的票 —— 三条分支都要有。

    预检通过后、签票期间会话可能已被新登录轮换。票已签但从未离开服务端 =
    旧在途请求未取得可用票据;漏掉这一步,被顶设备就拿到一张能进场的票。
    """

    class _Gate:
        def __init__(self) -> None:
            self.calls = 0

        async def require_current_session_token(self, *_a: object) -> None:
            self.calls += 1
            if self.calls > 1:  # 第一次是预检,第二次是交付终检
                raise errcode.PandoraError(errcode.ErrUnauthorized, "session rotated")

        async def resolve_hub_endpoint_from_match(self, *_a: object):  # noqa: ANN202
            return "hub-ds:7777", "hub-ticket", 0

        async def resolve_battle_endpoint(self, *_a: object):  # noqa: ANN202
            return "battle-ds:8888", "battle-ticket", 0

    class _Ticket:
        async def issue_ds_ticket(self, *_a: object):  # noqa: ANN202
            return "generic-ticket", 0

    for ds_type in ("hub", "battle", "other"):
        svc = lsvc.LoginService(_Gate(), _Ticket())
        resp = await svc.IssueDSTicket(
            login_pb2.IssueDSTicketRequest(ds_type=ds_type, target_id=1),
            _Ctx(**{"x_pandora_player_id": "5"}),
        )
        assert resp.code == errcode_pb2.ERR_UNAUTHORIZED, ds_type
        assert resp.ticket == "", ds_type
        assert resp.hub_ds_addr == "", ds_type


def test_login_response_double_writes_register_no_and_player_no(
    repo_root: pathlib.Path,
) -> None:
    """#13 register_no(旧客户端/JSON)与 #14 player_no(新客户端)必须双写同值。

    排空前收缩任一个,对应客户端上编号直接变 0(永远显示"生成中"),而服务端零错误。
    """
    src = pathlib.Path(lsvc.__file__).read_text(encoding="utf-8")
    assert "register_no=res.player_no" in src and "player_no=res.player_no" in src
    go_src = (repo_root / GO_SERVICE).read_text(encoding="utf-8")
    assert "RegisterNo: res.PlayerNo" in go_src and "PlayerNo:   res.PlayerNo" in go_src


# ── 真起一遍进程(需要 MySQL)────────────────────────────────────────────────


def _account_ddl(repo_root: pathlib.Path) -> list[str]:
    """从生产 DDL 里取建表语句 —— 抄一份简化 schema 的话,列形状探针会在一个
    与线上不同形状的表上跑绿,而那正是这些探针要抓的东西。"""
    raw = (repo_root / ACCOUNT_DDL).read_text(encoding="utf-8")
    body = "\n".join(
        line for line in raw.splitlines() if not line.lstrip().startswith("--")
    )
    # ★ 只在**行尾**的分号处切,不能裸 `split(";")`:
    # accounts.account_id 的 COMMENT 串里就带一个分号
    # ('账号身份 ID(snowflake);NULL=旧二进制注册尚未补铸'),裸切会把一条 DDL
    # 劈成两半 → 1064 语法错。这类"夹具自己坏了"的失败最费时间,因为它长得像被测代码坏了。
    return [
        stmt.strip()
        for stmt in re.split(r";[ \t]*(?:\r?\n|$)", body)
        if stmt.strip() and not stmt.strip().upper().startswith("USE")
    ]


@pytest.fixture()
async def account_db(repo_root: pathlib.Path):  # noqa: ANN201
    """建一个带完整账号库 schema 的独占测试库,返回可用 DSN。"""
    asyncmy = pytest.importorskip("asyncmy")
    import mysqlfixture as mf

    cfg = mf.parse_go_dsn(DSN, default_db="")
    # * 只有"MySQL 没起来"才允许跳过 —— 夹具自己的代码(ensure_database / cfg 取键 /
    #   create_pool 形参)抛的异常必须原样冒红。判据、放行集与 CI 文案契约
    #   见 tests/mysqlfixture.py 的 skip_only_if_mysql_is_down。
    with skip_only_if_mysql_is_down(cfg, "login 账号库测试"):
        await mf.ensure_database(asyncmy, cfg)
        conn = await asyncio.wait_for(
            asyncmy.connect(
                host=cfg["host"], port=cfg["port"], user=cfg["user"],
                password=cfg["password"], db=cfg["db"], autocommit=True,
            ),
            timeout=8.0,
        )
    try:
        async with conn.cursor() as cur:
            for stmt in _account_ddl(repo_root):
                await cur.execute(stmt)
    finally:
        with contextlib.suppress(Exception):
            await conn.ensure_closed()
    yield (
        f"{cfg['user']}:{cfg['password']}@tcp({cfg['host']}:{cfg['port']})"
        f"/{cfg['db']}?parseTime=true&loc=UTC&charset=utf8mb4"
    )


def _boot_yaml(dsn: str) -> str:
    """能起进程的最小配置。

    刻意把两个强制门关掉:本用例不起 Redis,而 session_generation_enforce=true +
    无 Redis 正是 ㉕ 那道闸要拒的组合(由另一个用例覆盖)。
    """
    return (
        "server:\n"
        '  grpc:\n    addr: ":0"\n'
        '  http:\n    addr: ":0"\n'
        "node:\n"
        "  node_id: 1\n"
        f'  mysql_client:\n    dsn: "{dsn}"\n'
        "login:\n"
        "  session_generation_enforce: false\n"
        "  require_ticket_sjti: false\n"
        f'  jwt:\n    secret: "{_DEV_SECRET}"\n'
    )


async def test_boots_to_service_ready(account_db: str, tmp_path: pathlib.Path) -> None:
    """真起一遍:所有 DB 侧闸(严格模式 / 六张表 / 两处列形状 / 补号计数器)全部
    在**生产 DDL** 上跑过,然后监听端口并打 service_ready。

    这条用例是"能起进程"的唯一硬证据 —— 前面那些用例验的都是"起不来时报得对"。
    """
    path = tmp_path / "login.yaml"
    path.write_text(_boot_yaml(account_db), encoding="utf-8")

    ready = asyncio.Event()
    rec = _Recorder()
    real_setup, real_get = plog.setup, plog.get

    class _Watch(_Recorder):
        def __getattr__(self, level: str):  # noqa: ANN204
            inner = _Recorder.__getattr__(rec, level)

            def emit(event: str = "", **kw: object) -> None:
                inner(event, **kw)
                if event == "service_ready":
                    ready.set()

            return emit

    watch = _Watch()
    plog.setup = lambda *_a, **_k: watch  # type: ignore[assignment]
    plog.get = lambda *_a, **_k: watch  # type: ignore[assignment]
    task = asyncio.create_task(
        lmain._main_async(lmain._parse_args(["-conf", str(path)]))
    )
    try:
        # ★ 等**自己的可观测条件** + deadline,不是固定 sleep:
        # 固定 sleep 在慢机器上假红、在快机器上白等,而且掩盖真实启动耗时回归。
        done, _ = await asyncio.wait(
            [asyncio.create_task(ready.wait()), task],
            timeout=30.0,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task.done() and task.exception() is not None:
            raise AssertionError(
                f"启动中抛异常;事件序列={rec.names()}"
            ) from task.exception()
        assert ready.is_set(), f"未达 service_ready;事件序列={rec.names()}"
        assert not task.done(), f"启动后立刻退出:{rec.names()}"
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
        plog.setup, plog.get = real_setup, real_get

    names = rec.names()
    # 六张表 / 列形状 / 严格模式全过 = 一条 mysql_schema_check_failed 都不该有。
    assert "mysql_schema_check_failed" not in names
    assert "mysql_strict_mode_required" not in names
    assert "player_no_sweeper_disabled" not in names
    # 无 Redis 的部署形态:必须显式告警,而不是静默当作"会话权威已就绪"。
    assert "redis_disabled_in_config" in names
    assert "login_fail_quota_disabled" in names


async def test_ds_ticket_v2_is_refused_not_silently_downgraded(
    account_db: str, tmp_path: pathlib.Path
) -> None:
    """v2(RS256)未实现 → 拒启。**绝不能**静默签 HS256 顶替。

    静默降级的后果:DS 侧只认 RS256,票签出来了但一律被拒,表现为"全服进不去场景"
    而启动日志全绿 —— 没有任何一条日志会指向票据算法。

    ★ 这道闸在 Go 里排在 MySQL / Redis / 四个客户端**之后**(main.go 的
    v2Verifier 段),所以这里必须带上真实 DSN,否则会先停在 mysql_dsn_required,
    被测的闸根本走不到而测试照样"红得对"。
    """
    yaml_text = _boot_yaml(account_db) + (
        "  ds_ticket:\n"
        '    private_key_file: "k.pem"\n'
        '    active_kid: "kid-1"\n'
    )
    rc, names = await _run_async(yaml_text, tmp_path)
    assert rc == 1
    # jwks_file 为空 → 命中 Go 的第二道闸(signer 必须配 verifier)。
    assert "ds_ticket_v2_signer_requires_verifier" in names
    assert "service_ready" not in names


async def test_ds_ticket_v2_assembles_then_requires_hub_allocator(
    account_db: str, tmp_path: pathlib.Path
) -> None:
    """v2 真装配 —— 用真钥匙对跑完 verifier + signer,再命中 ㉗ hub_allocator 闸。

    ★ 这条用例的价值在于**证明装配真的发生了**:
      - `ds_ticket_v2_verifier_ready` 只有在 JWKS 解析 + revision/kid 对账都通过后才打;
      - `ds_ticket_v2_requires_hub_allocator` 排在 `new_ds_ticket_signer_from_conf`
        **之后**,所以它出现 = 私钥也真读进去并构造出了签发器。
      两条都在 = 不可能是"配了就拒启"的旧桩顶替。
    ★ 拒启的理由本身也是硬约束:v2 档下 login 回退自签的 HS256 hub 票会被 v2 DS
      全拒,那是"启动日志全绿但全服进不去大厅"的半完成配置。
    """
    private_pem, pub, kid = pdsticket.generate_ds_ticket_key_pair()
    key_path = tmp_path / "ds_ticket.pem"
    key_path.write_bytes(private_pem)
    jwks_path = tmp_path / "ds_ticket_jwks.json"
    jwks_path.write_bytes(pdsticket.marshal_ds_ticket_jwks(7, kid, pub))

    yaml_text = _boot_yaml(account_db) + (
        "  ds_ticket:\n"
        f'    private_key_file: "{key_path.as_posix()}"\n'
        f'    jwks_file: "{jwks_path.as_posix()}"\n'
        f'    active_kid: "{kid}"\n'
        '    keyset_revision: "7"\n'
    )
    rc, names = await _run_async(yaml_text, tmp_path)
    assert rc == 1
    assert "ds_ticket_v2_verifier_ready" in names
    assert "ds_ticket_v2_requires_hub_allocator" in names
    assert "ds_ticket_v2_verifier_init_failed" not in names
    assert "ds_ticket_v2_signer_init_failed" not in names
    assert "service_ready" not in names


async def test_ds_ticket_v2_keyset_revision_mismatch_is_refused(
    account_db: str, tmp_path: pathlib.Path
) -> None:
    """配置写的 revision 与 JWKS 文件里的不一致 → 启动即失败。

    挡的是"换了键没换文件"(或反过来)这类半完成发布 —— 它在运行期的表现是
    随机一部分票验不过,而两边日志都正常。
    """
    _pem, pub, kid = pdsticket.generate_ds_ticket_key_pair()
    jwks_path = tmp_path / "ds_ticket_jwks.json"
    jwks_path.write_bytes(pdsticket.marshal_ds_ticket_jwks(7, kid, pub))

    yaml_text = _boot_yaml(account_db) + (
        "  ds_ticket:\n"
        f'    jwks_file: "{jwks_path.as_posix()}"\n'
        f'    active_kid: "{kid}"\n'
        '    keyset_revision: "8"\n'
    )
    rc, names = await _run_async(yaml_text, tmp_path)
    assert rc == 1
    assert "ds_ticket_v2_verifier_init_failed" in names
    assert "ds_ticket_v2_verifier_ready" not in names


async def test_session_enforce_without_redis_is_refused(
    account_db: str, tmp_path: pathlib.Path
) -> None:
    """★ 安全开关不能静默变形为"永不强制"。

    两个强制门都以「Redis 会话权威存在」为前提;缺 Redis 时开关只会变成装饰,
    而 yaml 上还写着 true —— 运维以为顶号收口已经生效。
    """
    yaml_text = _boot_yaml(account_db).replace(
        "  session_generation_enforce: false", "  session_generation_enforce: true"
    )
    rc, names = await _run_async(yaml_text, tmp_path)
    assert rc == 1
    assert "session_enforce_requires_redis_sessions" in names
    assert "service_ready" not in names
