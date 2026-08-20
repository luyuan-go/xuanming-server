"""gRPC 健康服务 + trace 头的契约测试。

这一批盯的都是**同一类故障**:服务本身完全正常、业务 RPC 全部可用、日志一行
ERROR 都没有,但线上是坏的 —— 因为坏的是"k8s 怎么看这个 Pod""日志怎么串起来",
不是业务逻辑。这类故障没有任何业务测试会发现,只能靠这里的机械判据。

2026-08-18 实测抓到的三个(都发生在同一个服务上,而它 592 个测试全绿):

  ① 根本没注册 grpc.health.v1.Health
     → deploy/k8s/services/services.yaml 里 22 个服务的 readinessProbe 全是
       `grpc: { port: 2000x }`,k8s 原生探针调的就是 Check("")。
       Python 答 UNIMPLEMENTED → **22 个服务全都永远不会 Ready**。

  ② 注册了但用了同步版 HealthServicer
     → 挂在 grpc.aio 上 Check 返回非 awaitable,请求以 UNKNOWN 失败。
       现象与①完全一样(探针失败),但根因不同。

  ③ trace_id 取错头(x-request-id 而不是 x-pandora-trace-id)
     → Python 服务的日志与 Go 服务、与 UE 客户端的 trace 完全串不起来。
       每条日志都在、格式都对、就是关联不上。
"""

from __future__ import annotations

import asyncio
import inspect

import grpc
import pytest
from grpc_health.v1 import health_pb2, health_pb2_grpc

from pandorapy import config as pconfig
from pandorapy import interceptors as pintercept
from pandorapy import server as pserver

# ── ★ ① 健康服务必须随 server 构造自动注册 ────────────────────────────────


@pytest.mark.asyncio
async def test_build_grpc_server_registers_health() -> None:
    """★ 注册必须发生在 build_grpc_server 里,不能靠各服务 main.py 自己记得调。

    忘了调的后果是 Pod 永不 Ready 且**服务日志全绿** —— 这种失败模式必须靠机制
    排除。这条测试就是那个机制的守卫:有人把注册挪回 main.py,这里立刻红。
    """
    # 必须是 async 用例:grpc.aio.server() 在没有运行中事件循环时构造会抛
    # "There is no current event loop"(Python 3.12+ 起 get_event_loop 不再隐式建)。
    # 真实服务都在 asyncio.run 里构造,所以这只是测试侧的约束。
    srv = pserver.build_grpc_server(pconfig.GrpcConf(addr=":0"))
    assert getattr(srv, pserver._HEALTH_ATTR, None) is not None, (
        "build_grpc_server 没有注册健康服务 —— k8s readinessProbe 会永远失败"
    )


@pytest.mark.asyncio
async def test_health_servicer_is_async_variant() -> None:
    """★ 必须是 _async 版。

    `grpc_health.v1.health.HealthServicer` 是同步实现,挂到 grpc.aio server 上时
    Check 返回普通对象而非 awaitable,请求以 UNKNOWN 失败 —— 而 k8s 那边看到的
    仍然只是"探测失败",根因完全不可见。
    """
    srv = pserver.build_grpc_server(pconfig.GrpcConf(addr=":0"))
    servicer = getattr(srv, pserver._HEALTH_ATTR)
    assert inspect.iscoroutinefunction(type(servicer).Check), (
        "健康服务用了同步版 HealthServicer,在 grpc.aio 上会以 UNKNOWN 失败"
    )


# ── ★ 端到端:真起 server,真发 Check ──────────────────────────────────────


async def _serve(auth_required: bool = False):
    srv = pserver.build_grpc_server(pconfig.GrpcConf(addr=":0"), auth_required=auth_required)
    port = srv.add_insecure_port("127.0.0.1:0")
    await srv.start()
    await pserver._health_set_serving(srv)
    return srv, port


async def _check(port: int, service: str = ""):
    async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch:
        return await asyncio.wait_for(
            health_pb2_grpc.HealthStub(ch).Check(health_pb2.HealthCheckRequest(service=service)), 5
        )


@pytest.mark.asyncio
async def test_health_check_serving_end_to_end() -> None:
    """空 service 名答 SERVING —— k8s 原生 gRPC 探针查的就是它。"""
    srv, port = await _serve()
    try:
        assert (await _check(port)).status == health_pb2.HealthCheckResponse.SERVING
    finally:
        await srv.stop(grace=None)


@pytest.mark.asyncio
async def test_health_check_unknown_service_is_not_found() -> None:
    """未登记的 service 名答 NOT_FOUND —— 与 grpc-go 的 health.Server 同语义。"""
    srv, port = await _serve()
    try:
        with pytest.raises(grpc.aio.AioRpcError) as ei:
            await _check(port, "nope.Svc")
        assert ei.value.code() == grpc.StatusCode.NOT_FOUND
    finally:
        await srv.stop(grace=None)


@pytest.mark.asyncio
async def test_health_check_survives_auth_required_interceptor() -> None:
    """★ 健康检查不能被鉴权拦截器挡住。

    探针不带 x-pandora-player-id;若 AuthRequired 对健康服务也生效,
    Pod 永远 NotReady —— 而且现象同样是"探测失败",看不出是鉴权拦的。
    """
    srv, port = await _serve(auth_required=True)
    try:
        assert (await _check(port)).status == health_pb2.HealthCheckResponse.SERVING
    finally:
        await srv.stop(grace=None)


# ── ★ ②「先摘流量 → 再排空」的顺序 ────────────────────────────────────────


@pytest.mark.asyncio
async def test_shutdown_flips_health_to_not_serving() -> None:
    """停机时健康状态必须翻 NOT_SERVING(对应 Kratos 的 health.Shutdown())。"""
    srv, port = await _serve()
    try:
        await pserver._health_enter_shutdown(srv)
        assert (await _check(port)).status == health_pb2.HealthCheckResponse.NOT_SERVING
    finally:
        await srv.stop(grace=None)


def test_shutdown_order_health_before_grpc_stop() -> None:
    """★ 顺序:翻 NOT_SERVING 必须**先于** grpc stop。

    Kratos 的 Server.Stop 就是这个顺序(先 health.Shutdown 再 GracefulStop)。
    这半拍是 §9.16「先摘流量 → 再排空在途」的机制本体:
    先答 NOT_SERVING 让 k8s 把本 Pod 摘出 Endpoints,在途请求再用 grace 期做完。
    反过来 = 排空期间 k8s 仍在往这台送新请求,滚动更新必然掉请求。

    用源码顺序断言而不是运行期时序 —— 后者要靠 sleep 抓那半拍,属 §16.10 禁止项。
    """
    src = inspect.getsource(pserver.run)
    i_health = src.index("_health_enter_shutdown(grpc_server)")
    i_stop = src.index("grpc_server.stop(")
    assert i_health < i_stop, "停机时先 stop 后翻健康状态 —— 排空期间仍会收到新流量"


# ── ★ ③ trace_id 口径 ────────────────────────────────────────────────────


def test_trace_metadata_key_matches_go() -> None:
    """★ 必须是 x-pandora-trace-id,不是 Envoy 的 x-request-id。

    客户端面(:8443)的 envoy 只无条件剥离 player-id / jwt-payload 两个**身份**头,
    x-pandora-trace-id 原样透传 —— UE 客户端 / DS 自带的 trace_id 要被全链采纳。
    取错头不会报任何错,只是日志再也串不起来。
    """
    assert pintercept.METADATA_KEY_TRACE_ID == "x-pandora-trace-id"
    assert pintercept.MAX_TRACE_ID_LEN == 64


@pytest.mark.parametrize(
    "value,ok",
    [
        ("0123456789abcdef", True),
        ("a-b_C-9", True),
        ("f" * 64, True),
        ("f" * 65, False),  # 超 MAX_TRACE_ID_LEN
        ("", False),
        ("abc\n", False),  # ★ 换行:会把一条日志拆成多条,可伪造他服务日志行
        ("abc\ndef", False),
        ("a b", False),
        ("a.b", False),
        ("中文", False),
        ("\x00", False),
    ],
)
def test_trace_id_safety_gate(value: str, ok: bool) -> None:
    r"""入站 trace_id 安全闸 —— 对应 Go 的 isSafeTraceID。

    ★ "abc\n" 这条是关键:正则写成 `^...$` 时 Python 的 `$` 匹配"末尾**或末尾
    换行之前**",换行注入就能过闸。必须用 `\A...\Z`。
    """
    assert pintercept.is_safe_trace_id(value) is ok


@pytest.mark.asyncio
async def test_trace_id_echoed_and_generated() -> None:
    """合规入站值原样回显;缺失 / 不合规则服务端生成并回显。

    "没有就不打"是不行的 —— 无 trace_id 的日志在 Loki 里无法与同一次请求的
    其它服务日志关联,而排障恰恰最需要这种请求。
    """
    from pandora.dialogue.v1 import dialogue_pb2 as dpb
    from pandora.dialogue.v1 import dialogue_pb2_grpc as dgrpc

    class _Svc(dgrpc.DialogueServiceServicer):
        async def StartDialogue(self, request, context):  # noqa: N802
            return dpb.StartDialogueResponse()

    srv = pserver.build_grpc_server(pconfig.GrpcConf(addr=":0"))
    dgrpc.add_DialogueServiceServicer_to_server(_Svc(), srv)
    port = srv.add_insecure_port("127.0.0.1:0")
    await srv.start()
    await pserver._health_set_serving(srv)
    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch:
            stub = dgrpc.DialogueServiceStub(ch)
            key = pintercept.METADATA_KEY_TRACE_ID

            async def trace_of(md):
                call = stub.StartDialogue(dpb.StartDialogueRequest(npc_id=1), metadata=md)
                await call
                return dict((k, v) for k, v in await call.initial_metadata()).get(key)

            assert await trace_of(((key, "client-trace-abc"),)) == "client-trace-abc"

            generated = await trace_of(())
            assert generated and pintercept.is_safe_trace_id(generated)

            # 超长入站值必须被丢弃并重新生成,不能原样回显
            bad = "x" * 200
            regenerated = await trace_of(((key, bad),))
            assert regenerated != bad
            assert pintercept.is_safe_trace_id(regenerated)
    finally:
        await srv.stop(grace=None)


# ── ★ 拦截器不得比 grpcio 本体更严 ────────────────────────────────────────


@pytest.mark.asyncio
async def test_interceptor_tolerates_sync_handler() -> None:
    """★ 拦截器必须容忍**同步** handler。

    grpcio 自己是 `if isawaitable(x): x = await x`,即同步 handler 合法。
    拦截器无条件 await 就比本体更严,任何第三方同步 unary servicer(健康检查、
    以后可能接的 channelz / 自定义诊断)挂进来都会以 UNKNOWN 失败。
    """

    def sync_handler(request, context):
        return b"pong"

    assert await pintercept._call_handler(sync_handler, b"ping", None) == b"pong"

    async def async_handler(request, context):
        return b"pong"

    assert await pintercept._call_handler(async_handler, b"ping", None) == b"pong"
