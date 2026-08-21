"""进程外壳层 —— access log / Kill-Switch / 超时 / 指标口径 / 连接老化。

这一层对应 Go 的 `pkg/grpcserver` 默认 middleware 链 + `pkg/middleware`。
它整体缺失时**业务测试全绿**:RPC 照常返回正确结果,只是关不掉、超不了时、
指标对不上、日志里什么都没有。2026-08-19 审计正是在这一层抓到五项缺口。

判据全部取「可观察的外部事实」:真起 grpc.aio server 对打、读真实的 Go 源码、
断言真的 metric 名 —— 不比对 Python 自己的字面量。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import pathlib
import re
from unittest import mock

import grpc
import pytest
from pandora.dialogue.v1 import dialogue_pb2, dialogue_pb2_grpc

from structlog.testing import capture_logs

from pandorapy import config as pconfig
from pandorapy import errcode
from pandorapy import interceptors as pintercept
from pandorapy import killswitch
from pandorapy import metrics
from pandorapy import server as pserver

PLAYER_HEADER = pintercept.METADATA_KEY_PLAYER_ID
_METHOD = "/pandora.dialogue.v1.DialogueService/StartDialogue"


class _SlowService(dialogue_pb2_grpc.DialogueServiceServicer):
    """一个可控慢的 servicer —— 用来验超时闸,不掺任何业务逻辑。"""

    def __init__(self, delay: float = 0.0, code: int = 0) -> None:
        self.delay = delay
        self.code = code
        self.calls = 0

    async def StartDialogue(self, request, context):  # noqa: N802, ANN001
        self.calls += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        return dialogue_pb2.StartDialogueResponse(code=self.code)


async def _serve(service, conf: pconfig.GrpcConf):
    server = pserver.build_grpc_server(conf, auth_required=False)
    dialogue_pb2_grpc.add_DialogueServiceServicer_to_server(service, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    return server, channel


def _access_events(captured_out: str) -> list[dict]:
    """从渲染后的日志里挑出 access log 事件。

    ★ 为什么不用 structlog 的 capture_logs:它把**整条处理器链**换掉了,
    而 trace_id / player_id 是由 pandorapy 自己的 `_add_ctx_fields` processor
    从 ContextVar 注入的 —— 换掉链之后那个 processor 不跑,字段永远看不到。
    用 capture_logs 断言"有没有 trace_id"会**恒假**,把"字段真的没打"和
    "用错了捕获方式"混成一回事。所以这里读真实渲染出来的 JSON 行。
    """
    events = []
    for line in captured_out.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        if ev.get("msg") in ("rpc_ok", "rpc_slow", "rpc_failed", "rpc_inband_error"):
            events.append(ev)
    return events


@pytest.fixture
def debug_logging():
    """把日志级别摆到 debug 并在用例后还原。

    plog.setup() 是**进程级**的:别的用例调过之后级别就留在那儿了。
    DEBUG 级的断言不自己摆级别 = 结果取决于用例执行顺序。
    """
    from pandorapy import log as plog

    plog.setup("test", level="debug")
    yield
    plog.setup("test", level="info")


@pytest.fixture(autouse=True)
def _clean_killswitch():
    """每个用例前后都把全局关停状态清干净 —— 它是进程级单例,漏清会污染别的用例。"""
    killswitch.set_default(None)
    killswitch.clear_features()
    yield
    killswitch.set_default(None)
    killswitch.clear_features()


# ── ★ Kill-Switch:三级匹配 ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("rules", "expect_off", "why"),
    [
        ({}, False, "无规则必须放行"),
        ({_METHOD.lstrip("/"): "改单个 RPC"}, True, "精确 method"),
        ({"pandora.dialogue.v1.DialogueService/*": ""}, True, "整服通配"),
        ({"*": ""}, True, "全局维护"),
        ({"pandora.other.v1.OtherService/*": ""}, False, "别的服务的整服通配不该命中"),
    ],
)
def test_killswitch_matching_levels(rules, expect_off, why) -> None:
    """★ 四级匹配缺一不可(对应 Go 的 Manager.Disabled 判定顺序)。

    早先只做「精确 method」一级。后果不是"少一点能力":运维照手册写下
    `<service>/*` 想关掉整个服务,规则加进去了、接口也返回成功,而 Python 副本
    一条都没挡住 —— 关停这种"出事时才用"的能力,失效恰好只在出事时才被发现。
    """
    m = killswitch.Manager()
    m.replace(dict(rules))
    off, reason = m.disabled(_METHOD)
    assert off is expect_off, why
    if expect_off:
        assert reason, "关停必须给出原因(客户端要拿它提示「维护中」)"


def test_killswitch_feature_group_expands_to_registered_operations() -> None:
    """feature 组必须按**代码注册的成员**展开,不是按名字前缀猜。"""
    killswitch.register_feature("trade", [_METHOD])
    m = killswitch.Manager()
    m.replace({"feature/trade": "交易玩法维护"})

    off, reason = m.disabled(_METHOD)
    assert off and reason == "交易玩法维护"
    assert m.disabled("/pandora.dialogue.v1.DialogueService/EndDialogue") == (False, "")


def test_register_feature_merges_instead_of_overwriting() -> None:
    """同名 feature 重复注册必须**合并**。

    一个玩法的 RPC 天然分散在多个服务,覆盖语义会让后注册的服务把前面的挤掉,
    而且不报错 —— 关停时只挡住一部分,最难发现。
    """
    killswitch.register_feature("trade", ["/a.Svc/One"])
    killswitch.register_feature("trade", ["/a.Svc/Two"])
    assert killswitch.feature_contains("trade", "a.Svc/One")
    assert killswitch.feature_contains("trade", "a.Svc/Two")


# ── ★ Kill-Switch:真的接进了拦截器链 ────────────────────────────────────────


async def test_killswitch_actually_blocks_the_rpc() -> None:
    """★ 关停必须**挡在业务 handler 之前**,而不是跑完再丢弃结果。

    这条守的是"接线"而不是"匹配逻辑":killswitch 模块此前整份迁过来了、
    规则也能解析,但**没有任何拦截器查它**(模块注释却写着"拦截器用它")。
    表现是运维加了规则、接口返回成功,而流量一条没挡住、副作用照样发生。

    判据取 `service.calls` —— 只看返回码的话,一个"跑完了但返回错误"的实现
    也能骗过测试。
    """
    m = killswitch.Manager()
    m.replace({_METHOD.lstrip("/"): "维护中"})
    killswitch.set_default(m)

    service = _SlowService()
    server, channel = await _serve(service, pconfig.GrpcConf())
    try:
        stub = dialogue_pb2_grpc.DialogueServiceStub(channel)
        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await stub.StartDialogue(
                dialogue_pb2.StartDialogueRequest(npc_id=1),
                metadata=((PLAYER_HEADER, "1001"),),
            )
        assert exc.value.code() == grpc.StatusCode.UNAVAILABLE
        assert "维护中" in exc.value.details()
        assert service.calls == 0, "业务 handler 仍被执行了 —— 关停没挡在它之前"
    finally:
        await channel.close()
        await server.stop(grace=None)


async def test_killswitch_never_blocks_health_check() -> None:
    """★ 全局关停不得挡住健康检查。

    挡住了等于把整个 Pod 从 Endpoints 里摘掉 —— 那不是"关一个 RPC",
    那是下线整个副本,而运维以为自己只是临时关了个接口。
    """
    from grpc_health.v1 import health_pb2, health_pb2_grpc

    m = killswitch.Manager()
    m.replace({"*": "全服维护"})
    killswitch.set_default(m)

    server, channel = await _serve(_SlowService(), pconfig.GrpcConf())
    try:
        stub = health_pb2_grpc.HealthStub(channel)
        resp = await stub.Check(health_pb2.HealthCheckRequest(service=""))
        assert resp.status == health_pb2.HealthCheckResponse.SERVING
    finally:
        await channel.close()
        await server.stop(grace=None)


async def test_killswitch_fails_open_when_unconfigured() -> None:
    """未装配 Manager 时一律放行 —— 绝不因关停组件自身故障把全服打死。"""
    killswitch.set_default(None)
    service = _SlowService()
    server, channel = await _serve(service, pconfig.GrpcConf())
    try:
        stub = dialogue_pb2_grpc.DialogueServiceStub(channel)
        resp = await stub.StartDialogue(
            dialogue_pb2.StartDialogueRequest(npc_id=1),
            metadata=((PLAYER_HEADER, "1001"),),
        )
        assert resp.code == 0
        assert service.calls == 1
    finally:
        await channel.close()
        await server.stop(grace=None)


# ── ★ 服务端单请求超时 ──────────────────────────────────────────────────────


async def test_server_timeout_aborts_slow_handler() -> None:
    """★ `server.grpc.timeout` 必须真的作用到 gRPC server。

    它在 21 份 yaml 里都配了(dev 15s),而 Python 侧此前**解析了但从未生效** ——
    一个慢 handler 会一直占着连接和 event loop 槽,配置文件看起来却是有保护的。
    """
    service = _SlowService(delay=5.0)
    server, channel = await _serve(service, pconfig.GrpcConf(timeout="300ms"))
    try:
        stub = dialogue_pb2_grpc.DialogueServiceStub(channel)
        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await stub.StartDialogue(
                dialogue_pb2.StartDialogueRequest(npc_id=1),
                metadata=((PLAYER_HEADER, "1001"),),
            )
        assert exc.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
    finally:
        await channel.close()
        await server.stop(grace=None)


async def test_server_timeout_does_not_touch_fast_handler() -> None:
    """正常请求不受影响(超时是上限,不是固定等待)。"""
    service = _SlowService(delay=0.0)
    server, channel = await _serve(service, pconfig.GrpcConf(timeout="5s"))
    try:
        stub = dialogue_pb2_grpc.DialogueServiceStub(channel)
        resp = await stub.StartDialogue(
            dialogue_pb2.StartDialogueRequest(npc_id=1),
            metadata=((PLAYER_HEADER, "1001"),),
        )
        assert resp.code == 0
    finally:
        await channel.close()
        await server.stop(grace=None)


# ── ★ BBR 自适应限流的装配 ─────────────────────────────────────────────────


def _chain_types(grpc_conf: pconfig.GrpcConf) -> list[str]:
    """取出 server 实际拿到的拦截器类名序列。

    ★ 断言**真实的链**,不是"构造成功了"。grpc.aio.server 对拦截器列表不做任何
    校验,漏挂一个照样构造成功 —— 而"漏挂限流"与"挂了但不生效"在外部完全同形。

    调用方必须是 async 用例:grpc.aio.server() 在构造时就要拿当前事件循环,
    同步用例里会 RuntimeError('There is no current event loop')。
    """
    seen: list[str] = []
    real = grpc.aio.server

    def spy(*args: object, **kwargs: object) -> object:
        for itc in kwargs.get("interceptors", ()) or ():
            seen.append(type(itc).__name__)
        return real(*args, **kwargs)

    with mock.patch.object(grpc.aio, "server", spy):
        pserver.build_grpc_server(grpc_conf)
    return seen


async def test_rate_limit_off_by_default_leaves_chain_untouched() -> None:
    """dev 默认关:链上不该出现限流拦截器。"""
    chain = _chain_types(pconfig.GrpcConf())
    assert "RateLimitInterceptor" not in chain


async def test_enable_rate_limit_actually_wires_bbr() -> None:
    """★ `enable_rate_limit=true` 必须真的挂上 BBR。

    这条曾经断言的是 `pytest.raises(NotImplementedError)` —— 那时 Python 侧确实
    没实现,fail-fast 是对的。但它不是可选项:`gen_cluster_config.ps1 -Prod`
    对 12 个 unary session-gate 服务 + login + push **机械强制**置 true(契约测试
    gen_cluster_prod_ratelimit_contract_test.ps1 锁定),也就是说这 14 个服务
    只要不实现 BBR 就一个都切不了 Python。现已移植(pandorapy/bbr.py)。
    """
    chain = _chain_types(pconfig.GrpcConf(enable_rate_limit=True))
    assert "RateLimitInterceptor" in chain


async def test_rate_limit_sits_inside_observability_and_outside_killswitch() -> None:
    """★ 链序必须与 Go 的 Metrics → [RateLimit] → KillSwitch 一致。

    位置不是风格问题,两侧都错得很具体:
      · 跑到 Observability **外面** → 被丢的请求不进 pandora_rpc_total,
        过载时"到底丢了多少"这个唯一要看的数直接消失。
      · 跑到 KillSwitch **里面** → 已被运维关停的 RPC 还要先过一遍限流统计,
        关停语义被限流的丢弃盖住。
    """
    chain = _chain_types(pconfig.GrpcConf(enable_rate_limit=True))
    assert chain.index("ObservabilityInterceptor") < chain.index("RateLimitInterceptor")
    assert chain.index("RateLimitInterceptor") < chain.index("KillSwitchInterceptor")


def test_conn_age_options_are_exactly_what_grpc_gets() -> None:
    """★ 断言**真实的 option 列表**,不是"server 构造成功了"。

    原先这条只断言 duration 解析 + `assert server is not None`,并在注释里写
    "非法 option 名会当场报错"当作间接判据 —— 那句话**是错的**(复核实测):

        grpc.aio.server(options=[("grpc.totally_bogus_option_name", 1)])   # 静默接受
        grpc.aio.server(options=[("grpc.max_connection_age_grace_millis", 1)])  # 也接受

    所以把 option 名拼错一个字母、或整段删掉,那条用例结构上不可能变红。
    现在 option 计算被提成纯函数,直接比对列表。
    """
    assert pserver.conn_age_options(pconfig.GrpcConf()) == [], "不配 = 关,不该塞任何 option"

    assert pserver.conn_age_options(
        pconfig.GrpcConf(max_conn_age="15m", max_conn_age_grace="360s")
    ) == [
        ("grpc.max_connection_age_ms", 900_000),
        ("grpc.max_connection_age_grace_ms", 360_000),
    ]


def test_grace_defaults_to_30s_when_only_max_age_is_set(repo_root: pathlib.Path) -> None:
    """★ 只配 max_conn_age 时 grace 必须兜底 30s —— 判据取自 Go 源码。

    grpc core 的默认是**无限宽限**:不兜底的话"达龄"永远不会真正断开老连接,
    滚动更新时流量滚不到新副本,而现象与"根本没开这个功能"一模一样。
    """
    src = (repo_root / "pkg" / "grpcserver" / "grpcserver.go").read_text(encoding="utf-8")
    assert "30 * time.Second" in src, "Go 的 grace 兜底值变了?请同步"

    opts = dict(pserver.conn_age_options(pconfig.GrpcConf(max_conn_age="15m")))
    assert opts["grpc.max_connection_age_grace_ms"] == 30_000


# ── ★ 指标口径与 Go 一致 ────────────────────────────────────────────────────


def test_rpc_metric_names_match_go_source(repo_root: pathlib.Path) -> None:
    """★ 指标名与 label 必须与 Go 侧**同名同 label**,判据直接读 Go 源码。

    Python 侧原先用的是 grpc_server_*,而 Go 用 pandora_rpc_*。后果是灰度期按
    pandora_* 建的告警对 Python 实例是 **NoData 而不是告警** —— 静默失去覆盖。

    抄一份字面量到测试里是没用的(Go 改了照样绿),所以这里去解析真的 Go 文件。
    """
    src = (repo_root / "pkg" / "middleware" / "metrics.go").read_text(encoding="utf-8")

    def go_metric(var: str) -> tuple[str, list[str]]:
        block = re.search(rf"{var}\s*=\s*prometheus\.New\w+Vec\((.*?)\}}\)", src, re.S)
        assert block, f"没在 Go 源码里找到 {var}"
        body = block.group(1)
        ns = re.search(r'Namespace:\s*"([^"]+)"', body)
        sub = re.search(r'Subsystem:\s*"([^"]+)"', body)
        name = re.search(r'Name:\s*"([^"]+)"', body)
        assert ns and sub and name
        labels = re.findall(r'"(\w+)"', body[body.index("[]string{"):])
        return f"{ns.group(1)}_{sub.group(1)}_{name.group(1)}", labels

    dur_name, dur_labels = go_metric("rpcDurationSeconds")
    tot_name, tot_labels = go_metric("rpcTotal")

    assert metrics.RPC_DURATION._name == dur_name, (  # noqa: SLF001
        f"直方图名与 Go 不一致:Python={metrics.RPC_DURATION._name} Go={dur_name}"  # noqa: SLF001
    )
    assert metrics.RPC_TOTAL._name == tot_name.removesuffix("_total")  # noqa: SLF001
    assert list(metrics.RPC_DURATION._labelnames) == dur_labels  # noqa: SLF001
    assert list(metrics.RPC_TOTAL._labelnames) == tot_labels  # noqa: SLF001


def test_histogram_buckets_match_go(repo_root: pathlib.Path) -> None:
    """直方图分桶必须与 Go 的 StandardBuckets 逐值一致。

    桶边界不一致 = 两栈的 P99 **不可比**,而那正是灰度时唯一要看的数。
    """
    src = (repo_root / "pkg" / "metrics" / "metrics.go").read_text(encoding="utf-8")
    m = re.search(r"ExponentialBuckets\(([\d.]+),\s*(\d+),\s*(\d+)\)", src)
    assert m, "没在 pkg/metrics 里找到 ExponentialBuckets"
    start, factor, count = float(m.group(1)), int(m.group(2)), int(m.group(3))
    expected = tuple(start * (factor**i) for i in range(count))
    assert metrics.STANDARD_BUCKETS == pytest.approx(expected)


def test_short_service_label_matches_go_shape() -> None:
    """service label 取全限定名的**最后一段**(与 Go 的 splitOperation 一致)。

    label 值不一致的话,同一块面板上两栈会各占一行,"对比两个实现"无从谈起。
    """
    assert metrics.short_service("pandora.login.v1.LoginService") == "LoginService"
    assert metrics.short_service("") == "unknown"


def test_code_label_buckets_match_go(repo_root: pathlib.Path) -> None:
    """code label 的取值集合必须是 Go codeLabel 的那几个。"""
    src = (repo_root / "pkg" / "middleware" / "metrics.go").read_text(encoding="utf-8")
    body = src[src.index("func codeLabel") :]
    go_values = set(re.findall(r'return "(\w+)"', body))
    py_values = {
        metrics.code_label(None),
        metrics.code_label(grpc.StatusCode.UNAUTHENTICATED),
        metrics.code_label(grpc.StatusCode.DEADLINE_EXCEEDED),
        metrics.code_label(grpc.StatusCode.INVALID_ARGUMENT),
        metrics.code_label(grpc.StatusCode.INTERNAL),
    }
    assert py_values <= go_values, f"Python 造了 Go 没有的 code 取值:{py_values - go_values}"


# ── ★ access log ────────────────────────────────────────────────────────────


def test_slow_threshold_env_var_matches_go(repo_root: pathlib.Path, monkeypatch) -> None:
    """慢请求阈值的环境变量名必须与 Go 同名(LOG_SLOW_RPC_MS),默认值也一致。

    名字不同 = 运维按手册设了变量而 Python 副本纹丝不动,而且**不报错**。
    """
    src = (repo_root / "pkg" / "middleware" / "logging.go").read_text(encoding="utf-8")
    m = re.search(r'os\.Getenv\("(\w+)"\)', src)
    assert m, "没在 Go 的 logging.go 里找到环境变量名"
    var = m.group(1)
    assert var == "LOG_SLOW_RPC_MS"

    monkeypatch.delenv(var, raising=False)
    assert pintercept.slow_rpc_threshold_ms() == 500
    monkeypatch.setenv(var, "120")
    assert pintercept.slow_rpc_threshold_ms() == 120
    monkeypatch.setenv(var, "垃圾值")
    assert pintercept.slow_rpc_threshold_ms() == 500, "非法值必须回落默认,不能崩"


def test_access_log_events_exist_in_go_source(repo_root: pathlib.Path) -> None:
    """★ 四个事件名必须与 Go 侧一字不差 —— 它们是运维手册的排障入口。

    事件名漂移 = `docs/ops/player-journey-log-map.md` 里的 LogQL 对 Python 副本恒空,
    而面板不会告诉你"这里本来该有数据"。
    """
    src = (repo_root / "pkg" / "middleware" / "logging.go").read_text(encoding="utf-8")
    for event in ("rpc_ok", "rpc_slow", "rpc_failed", "rpc_inband_error"):
        assert f'"{event}"' in src, f"Go 侧没有事件 {event} —— 是不是名字对错了?"

    py = (
        pathlib.Path(pintercept.__file__).read_text(encoding="utf-8")
    )
    for event in ("rpc_ok", "rpc_slow", "rpc_failed", "rpc_inband_error"):
        assert f'"{event}"' in py, f"Python 侧不产出 {event}"


def test_inband_server_fault_codes_match_go(repo_root: pathlib.Path) -> None:
    """★ 判定"服务端故障"的码集合必须与 Go 的 IsServerFault 逐个一致。

    Go 那是四个**具名码**的 switch,不是数值区间。按区间猜的话,一大批正常的
    业务拒绝码会被误升成 ERROR,把真故障淹掉;反过来漏了就会让真故障以
    rpc_ok(DEBUG)在生产 info 级下彻底静默。
    """
    src = (repo_root / "pkg" / "errcode" / "errcode.go").read_text(encoding="utf-8")
    body = src[src.index("func IsServerFault") :]
    names = re.findall(r"\bErr\w+", body.split("default:")[0])
    expected = {getattr(errcode, n) for n in names}
    assert expected, "没解析出 Go 的 IsServerFault 码集合"
    assert pintercept._SERVER_FAULT_CODES == expected  # noqa: SLF001


async def test_inband_server_fault_is_logged_as_error() -> None:
    """in-band 返回服务端故障码时必须升 ERROR,不能记成 rpc_ok。

    否则它在生产 info 级下彻底静默 —— transport 没错、body 里却是内部故障,
    这正是 §16「禁止吞掉故障」点名的形状。

    ★ 用 structlog 自己的 capture_logs,不用 caplog / capsys:
      - caplog 抓 stdlib logging 的 handler,而 structlog 不经过它 —— 恒空;
      - capsys 抓 stdout,但 structlog 的 logger 在 setup() 时就**捕获了当时的
        sys.stdout**,别的用例先跑过 setup 的话,本用例的 capsys 换上去的那个流
        它根本不认。表现正是"单跑绿、全量跑红",极易被当成 flaky 忽略掉。
    """
    service = _SlowService(code=errcode.ErrInternal)
    server, channel = await _serve(service, pconfig.GrpcConf())
    try:
        stub = dialogue_pb2_grpc.DialogueServiceStub(channel)
        with capture_logs() as logs:
            resp = await stub.StartDialogue(
                dialogue_pb2.StartDialogueRequest(npc_id=1),
                metadata=((PLAYER_HEADER, "1001"),),
            )
        assert resp.code == errcode.ErrInternal
        events = [e["event"] for e in logs]
        assert "rpc_inband_error" in events, f"服务端故障被记成了普通成功:{events}"
        assert "rpc_ok" not in events
    finally:
        await channel.close()
        await server.stop(grace=None)


async def test_successful_rpc_logs_rpc_ok(debug_logging) -> None:
    """成功请求打 rpc_ok。

    ★ 它是 **DEBUG** 级(与 Go 一致):高 QPS 下 rpc_ok 是最大噪音源,生产 info 级
    下刻意不出;要全量 access log 时设 LOG_LEVEL=debug 打开。
    所以本用例必须**显式**把级别摆到 debug —— 否则它是否通过取决于前面哪个用例
    最后调过 plog.setup(),表现为"单跑绿、全量跑红"。
    """
    server, channel = await _serve(_SlowService(), pconfig.GrpcConf())
    try:
        stub = dialogue_pb2_grpc.DialogueServiceStub(channel)
        with capture_logs() as logs:
            await stub.StartDialogue(
                dialogue_pb2.StartDialogueRequest(npc_id=1),
                metadata=((PLAYER_HEADER, "1001"),),
            )
        assert "rpc_ok" in [e["event"] for e in logs]
    finally:
        await channel.close()
        await server.stop(grace=None)


async def test_slow_rpc_logs_rpc_slow(monkeypatch) -> None:
    """耗时超阈值升 WARN rpc_slow —— 生产 info 级下也看得到慢在哪个 op。"""
    monkeypatch.setenv("LOG_SLOW_RPC_MS", "50")
    service = _SlowService(delay=0.2)
    server, channel = await _serve(service, pconfig.GrpcConf())
    try:
        stub = dialogue_pb2_grpc.DialogueServiceStub(channel)
        with capture_logs() as logs:
            await stub.StartDialogue(
                dialogue_pb2.StartDialogueRequest(npc_id=1),
                metadata=((PLAYER_HEADER, "1001"),),
            )
        slow = [e for e in logs if e["event"] == "rpc_slow"]
        assert slow, f"没打 rpc_slow:{[e['event'] for e in logs]}"
        assert slow[0]["slow_threshold_ms"] == 50
        assert slow[0]["latency_ms"] >= 50
    finally:
        await channel.close()
        await server.stop(grace=None)


async def test_run_emits_runtime_info_so_no_main_can_forget_it() -> None:
    """★ 运行时标识由 `pserver.run` 统一打,不靠每个 main 各自记得调一行。

    这条守的是**接线的结构性**,不是指标本身:实测 owner/main.py 就没有调过
    dialogue 那份(它是后写的)。凡是"每个新 main 都记得调"的约定都靠不住 ——
    漏掉不报错,只是那个副本在面板上无法与 Go 副本区分。

    判据是真的把 run() 跑起来,再从 registry 里读指标。
    """
    from prometheus_client import generate_latest

    ready = asyncio.Event()
    server = pserver.build_grpc_server(pconfig.GrpcConf())
    # ★ 端口由 run() 自己绑(它内部会 add_insecure_port + normalize)。
    # 测试里先绑一次再把地址传进去 = 绑两次,第二次拿不到端口,on_ready 永不触发。
    task = asyncio.create_task(
        pserver.run(
            service_name="runwired",
            grpc_server=server,
            grpc_addr="127.0.0.1:0",
            on_ready=ready.set,
        )
    )
    try:
        await asyncio.wait_for(ready.wait(), timeout=20)
        out = generate_latest().decode("utf-8")
        assert any(
            ln.startswith("pandora_runtime_info") and "runwired" in ln
            for ln in out.splitlines()
        ), "run() 起来了但没有 runtime info —— 该副本在面板上无法与 Go 区分"
    finally:
        # ★ 必须**先停 server 再取消任务**,顺序反了会把 pytest 吊死。
        #
        # run() 此刻阻塞在 `await stop.wait()`。直接 cancel 会让取消从那里抛出,
        # 优雅停机段(health 转 NOT_SERVING + grpc_server.stop)**整段被跳过** ——
        # grpc.aio server 保持 started,失败路径上的 traceback 又持有它的引用活到
        # event loop 关闭之后,`Server.__del__` 触发 "Event loop is closed" 并卡住进程。
        #
        # 实测(变异复现):把 pserver.run 里的 set_runtime_info 换成 pass,
        # 本用例打完结果**不退出**,90s 超时被杀;基线是 2.77s 干净退出。
        # 后果是这条用例真红时 CI 拿到的是 **job 超时**而不是一条 FAILED,
        # 同进程后面的测试文件也不再跑 —— 红被降级成"挂住",最难归因。
        with contextlib.suppress(Exception):
            await server.stop(grace=None)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


def test_unsupported_config_section_is_rejected_at_load_time() -> None:
    """★ 配了 Python 侧没实现的功能段,**加载配置就拒**,不靠 main 记得调。

    cell_route 是典型:配上去是为了改变路由行为,忽略掉之后系统行为与配置意图
    不一致**而且不报错** —— 运维看着 yaml 以为 cell 路由生效了,实际所有玩家
    都落在单 Cell 上。闸挂在 pydantic 校验器上,任何服务加载配置都躲不过。
    """
    pconfig.BaseConf()  # 不配就正常

    with pytest.raises(NotImplementedError, match="cell_route"):
        pconfig.BaseConf.model_validate({"cell_route": {"mode": "static"}})


def _sample(name: str, labels: dict) -> float:
    from prometheus_client import REGISTRY

    return REGISTRY.get_sample_value(name, labels) or 0.0


async def test_inband_code_dimension_is_actually_recorded() -> None:
    """★ `pandora_rpc_inband_total` 必须真的在涨。

    本仓的业务失败是 **in-band**(response.code),所以 `pandora_rpc_total` 的
    code label 与 Go 一样恒为 "ok" —— 灰度期"Python 副本哪个业务码在涨"
    **只有这一族**能回答。它静默消失了没有替代信号。

    这三族(inband / panics / canceled)是本批次新增的产物,原本零断言:
    把 `.inc()` 换成 `pass` 不会有任何用例变红。
    """
    labels = {
        "service": "DialogueService",
        "method": "StartDialogue",
        "errcode": str(errcode.ErrInternal),
    }
    before = _sample("pandora_rpc_inband_total", labels)

    server, channel = await _serve(_SlowService(code=errcode.ErrInternal), pconfig.GrpcConf())
    try:
        stub = dialogue_pb2_grpc.DialogueServiceStub(channel)
        await stub.StartDialogue(
            dialogue_pb2.StartDialogueRequest(npc_id=1),
            metadata=((PLAYER_HEADER, "1001"),),
        )
    finally:
        await channel.close()
        await server.stop(grace=None)

    assert _sample("pandora_rpc_inband_total", labels) == before + 1, (
        "业务错误码维度没被记录 —— 灰度期无法回答「Python 副本哪个业务码在涨」"
    )


async def test_panic_and_cancel_are_counted_separately() -> None:
    """★ 未捕获异常与"被取消"必须分开计数。

    grpc.aio 用**取消**终止超时的 handler。混进 panics 会让每一次正常的客户端
    deadline 都表现成"服务端在 panic",弱网 / 高峰时把真正的未捕获异常淹掉。
    """
    panic_labels = {
        "service": "DialogueService",
        "method": "StartDialogue",
        "exc_type": "RuntimeError",
    }
    cancel_labels = {"service": "DialogueService", "method": "StartDialogue"}
    before_panic = _sample("pandora_rpc_panics_total", panic_labels)
    before_cancel = _sample("pandora_rpc_canceled_total", cancel_labels)

    class _Boom(dialogue_pb2_grpc.DialogueServiceServicer):
        async def StartDialogue(self, request, context):  # noqa: N802, ANN001
            raise RuntimeError("boom")

    server, channel = await _serve(_Boom(), pconfig.GrpcConf())
    try:
        stub = dialogue_pb2_grpc.DialogueServiceStub(channel)
        with pytest.raises(grpc.aio.AioRpcError):
            await stub.StartDialogue(
                dialogue_pb2.StartDialogueRequest(npc_id=1),
                metadata=((PLAYER_HEADER, "1001"),),
            )
    finally:
        await channel.close()
        await server.stop(grace=None)

    assert _sample("pandora_rpc_panics_total", panic_labels) == before_panic + 1, (
        "未捕获异常没有计进 panics"
    )
    # ★ 取消是**另一族**:这次调用没有被取消,它必须**纹丝不动**。
    # 之前这里写成了 `_sample(...) == _sample(...)` —— 同一个调用跟自己比,
    # 恒真。恒真式断言是最难发现的假测试:它长得像在断言什么。
    assert _sample("pandora_rpc_canceled_total", cancel_labels) == before_cancel, (
        "未捕获异常被同时计进了 canceled —— 两族混在一起,deadline 风暴会淹掉真 panic"
    )


def test_runtime_info_metric_distinguishes_the_two_stacks() -> None:
    """★ 灰度期必须能在**指标**上分辨 Go 副本与 Python 副本。

    pandora_rpc_* 上没有任何维度能区分两者 —— 面板上只有一条混合曲线,
    而"Python 版慢不慢 / 错得多不多"是灰度期唯一要回答的问题。
    `service_ready` 日志里有 runtime 字段,但**日志回答不了指标问题**。

    用恒为 1 的 info 指标 + label(Prometheus 标准做法,同 kube_*_info),
    面板按 instance join 即可分栈,而不必给每个业务指标都加 label
    (那会让所有历史曲线的 label 集合变化,旧告警全部失配)。
    """
    from prometheus_client import generate_latest

    metrics.set_runtime_info("shelltest")
    out = generate_latest().decode("utf-8")
    lines = [
        ln
        for ln in out.splitlines()
        if ln.startswith("pandora_runtime_info") and "shelltest" in ln
    ]
    assert lines, "没有 pandora_runtime_info —— 灰度期无法在指标上分辨两个实现"
    assert 'runtime="python"' in lines[0]
    assert lines[0].endswith("1.0")


def test_access_log_op_matches_go_operation_shape(repo_root: pathlib.Path) -> None:
    """★ op 的形状必须与 Go 的 transport.Operation() 一致:**带前导斜杠**。

    Kratos 的 Operation 是 "/pandora.x.v1.XService/Method"。少一个斜杠不会报错,
    只会让按 op 精确匹配的 LogQL / 面板在 Python 副本上**全部落空** ——
    而"查不到"最容易被读成"没发生过"。

    判据从 Go 源码取:Go 侧 proto 生成的 OperationXxx 常量就是带斜杠的字面量。
    """
    import re

    src = (repo_root / "pkg" / "middleware" / "logging.go").read_text(encoding="utf-8")
    assert "tr.Operation()" in src, "Go 的 op 不再取自 transport.Operation()?"

    gen = repo_root / "proto" / "gen" / "go" / "pandora" / "dialogue" / "v1"
    grpc_files = list(gen.glob("*_grpc.pb.go"))
    assert grpc_files, "找不到 dialogue 的 gRPC 生成物"
    text = grpc_files[0].read_text(encoding="utf-8")
    # 生成物里的 FullMethodName 常量就是 transport.Operation() 的取值来源。
    names = re.findall(r'_FullMethodName\s*=\s*"([^"]+)"', text)
    assert names, "生成物里没有 FullMethodName 常量"
    assert all(n.startswith("/") for n in names), f"Go 的 method name 不带斜杠?{names[:2]}"
    assert _METHOD in names, (
        f"本用例用的 {_METHOD} 不在生成物里 —— proto 改过了?现有:{names[:3]}"
    )


async def test_access_log_carries_transport_and_slashed_op(debug_logging, capsys) -> None:
    """access log 的 op 带斜杠、且带 transport 字段(两栈同一条 LogQL 才查得到)。"""
    server, channel = await _serve(_SlowService(), pconfig.GrpcConf())
    try:
        stub = dialogue_pb2_grpc.DialogueServiceStub(channel)
        await stub.StartDialogue(
            dialogue_pb2.StartDialogueRequest(npc_id=1),
            metadata=((PLAYER_HEADER, "1001"),),
        )
    finally:
        await channel.close()
        await server.stop(grace=None)

    events = _access_events(capsys.readouterr().out)
    assert events, "没打 access log"
    assert events[0]["op"] == _METHOD, f"op 形状不对:{events[0]['op']!r}"
    assert events[0].get("transport") == "grpc", "缺 transport 字段"


# ── ★ 被拒的 RPC:code 与 trace_id 都必须落在 access log 里 ─────────────────


async def test_aborted_rpc_records_real_status_not_ok() -> None:
    """★ transport 级拒绝必须记成对应的 code,**不能记成 "ok"**。

    这条抓的是一个真实缺陷:`grpc.aio.AbortError` **没有** code()/details()
    (实测它的属性只有 add_note / args / with_traceback)。从异常上 getattr 取
    恒得到 None → code_label(None) = "ok" → 每一次 401/403 都被记成成功,
    err 还是空串。而这条路径正是"登录一直被拒"这类问题的唯一信号。

    只把 code_label 当纯函数测是抓不到的 —— 纯函数本身没错,错的是喂给它的东西。
    所以判据必须取**真实 abort 过的 RPC**。
    """
    from prometheus_client import REGISTRY

    class _Denying(dialogue_pb2_grpc.DialogueServiceServicer):
        async def StartDialogue(self, request, context):  # noqa: N802, ANN001
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "nope")

    def total(code: str) -> float:
        v = REGISTRY.get_sample_value(
            "pandora_rpc_total",
            {"service": "DialogueService", "method": "StartDialogue", "code": code},
        )
        return v or 0.0

    before_unauth, before_ok = total("unauthorized"), total("ok")
    server, channel = await _serve(_Denying(), pconfig.GrpcConf())
    try:
        stub = dialogue_pb2_grpc.DialogueServiceStub(channel)
        with pytest.raises(grpc.aio.AioRpcError):
            await stub.StartDialogue(
                dialogue_pb2.StartDialogueRequest(npc_id=1),
                metadata=((PLAYER_HEADER, "1001"),),
            )
    finally:
        await channel.close()
        await server.stop(grace=None)

    assert total("unauthorized") == before_unauth + 1, (
        "PERMISSION_DENIED 没有记成 unauthorized —— code 多半是从异常上取的(取不到)"
    )
    assert total("ok") == before_ok, "被拒的 RPC 被记成了 ok"


async def test_access_log_carries_trace_id(debug_logging, capsys) -> None:
    """★ access log 四事件必须带 trace_id。

    trace_id 是把一次请求在多个服务里的日志串起来的**唯一**手段。缺了它不会报错,
    只是 Loki 里那几条日志永远关联不上 —— 看起来像"这个服务没参与这次请求"。

    这条同时钉住拦截器**顺序**:绑定是 contextvars,作用域只在绑定它的那层。
    绑在内层(如 Auth)的话,它的 finally 一 reset,外层 access log 才开始打,
    于是四个事件全都没有 trace_id。Go 把 Trace() 排在 Logging() 之前正是此因。
    """
    server, channel = await _serve(_SlowService(), pconfig.GrpcConf())
    try:
        stub = dialogue_pb2_grpc.DialogueServiceStub(channel)
        await stub.StartDialogue(
            dialogue_pb2.StartDialogueRequest(npc_id=1),
            metadata=(
                (PLAYER_HEADER, "1001"),
                (pintercept.METADATA_KEY_TRACE_ID, "trace-abc-123"),
            ),
        )
    finally:
        await channel.close()
        await server.stop(grace=None)

    access = _access_events(capsys.readouterr().out)
    assert access, "没打 access log"
    assert access[0].get("trace_id") == "trace-abc-123", (
        f"access log 没带 trace_id(拿到 {access[0].get('trace_id')!r}) —— "
        f"多半是 trace 绑定在内层拦截器,外层打日志时已经 reset"
    )
    assert access[0].get("player_id") == 1001, "access log 没带 player_id"


async def test_failed_rpc_log_carries_trace_id_and_real_code(debug_logging, capsys) -> None:
    """被拒的那条 rpc_failed 同样要带 trace_id 与真实 code。"""
    class _Denying(dialogue_pb2_grpc.DialogueServiceServicer):
        async def StartDialogue(self, request, context):  # noqa: N802, ANN001
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "nope")

    server, channel = await _serve(_Denying(), pconfig.GrpcConf())
    try:
        stub = dialogue_pb2_grpc.DialogueServiceStub(channel)
        with pytest.raises(grpc.aio.AioRpcError):
            await stub.StartDialogue(
                dialogue_pb2.StartDialogueRequest(npc_id=1),
                metadata=(
                    (PLAYER_HEADER, "1001"),
                    (pintercept.METADATA_KEY_TRACE_ID, "trace-xyz-789"),
                ),
            )
    finally:
        await channel.close()
        await server.stop(grace=None)

    failed = [e for e in _access_events(capsys.readouterr().out) if e["msg"] == "rpc_failed"]
    assert failed, "被拒的 RPC 没打 rpc_failed"
    assert failed[0].get("trace_id") == "trace-xyz-789"
    # ★ code 是**数值**(与 Go 的 errors.Code(err) 同类型),名字在 reason 里。
    # 同名字段两栈值域不相交的话,按 code 过滤的面板只能命中一半流量,
    # 而"少一半"看起来跟"这段时间没错"一模一样。
    assert failed[0]["code"] == grpc.StatusCode.PERMISSION_DENIED.value[0], (
        f"rpc_failed 的 code 是 {failed[0]['code']!r} —— 没从 context 取到真实状态码"
    )
    assert failed[0]["reason"] == "PERMISSION_DENIED"
    assert "nope" in failed[0]["err"], "err 是空的 —— details 也没取到"
