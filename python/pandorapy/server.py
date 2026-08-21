"""双 server 启动骨架 —— 一个进程同时跑 grpc.aio 和 FastAPI/uvicorn。

这是"Kratos 那一层"在 Python 侧的另一半替代物。对应 Go 侧:

    app := kratos.New(
        kratos.Name(serviceName),
        kratos.Logger(logger),
        kratos.Server(grpcSrv, httpSrv),   ← 两个 transport 平行跑
    )
    app.Run()

拓扑(与现状完全一致,端口号都不变):

                        ┌── :2001x  gRPC ──→ grpc.aio  ← Envoy 打这里
    一个进程 ───────────┤
                        └── :2101x  HTTP ──→ FastAPI   ← Prometheus 抓这里

    21 个服务现在每个都有 internal/server/grpc.go + internal/server/http.go,
    这里就是那两个文件合起来的对应物。**不是串联** —— 两个 server 互不经过对方。

为什么 gRPC 不用 FastAPI 扛:
    FastAPI 是 ASGI/HTTP 框架,说不了 h2c gRPC(HTTP/2 分帧 + grpc-status trailer +
    grpc-timeout)。让它扛 RPC 就得手写 gRPC-Web 分帧和 trailer,而客户端是 UE C++、
    中间还夹着 Envoy 的 grpc_web filter,三方任一理解偏差都要在 UE 里打断点比对字节。
    grpcio 底下是官方的 C 版 gRPC core —— 和 Go 侧 google.golang.org/grpc 同一份,
    所以这些细节不用自己承担。
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import grpc
import uvicorn
from fastapi import FastAPI
# ★ 必须是 _async 版:grpc_health.v1.health.HealthServicer 是**同步**实现,
# 挂到 grpc.aio server 上时 Check 返回的是普通对象而不是 awaitable,
# 请求会以 UNKNOWN 失败(2026-08-18 实测),而 k8s 探针只看见"探测失败"。
from grpc_health.v1 import _async as grpc_health_aio
from grpc_health.v1 import health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from pandorapy import config as pconfig
from pandorapy import interceptors as pintercept
from pandorapy import log as plog
from pandorapy import metrics
from pandorapy import safego as psafego


def conn_age_options(grpc_conf: pconfig.GrpcConf) -> list[tuple[str, Any]]:
    """连接老化相关的 grpc option。提成**纯函数**是为了能被断言。

    ★ 原先这段内联在 build_grpc_server 里,用例只能断言"server 构造成功",
    而那句话什么都证明不了 —— grpcio 对**拼错的 option 名静默接受**
    (实测 `("grpc.totally_bogus_option_name", 1)` 照样构造成功)。
    于是把 option 名写错一个字母、或整段删掉,用例都不会红。

    作用与 Go 侧 pkg/grpcserver 的 keepalive.ServerParameters 一致:
      max_conn_age       达龄 GOAWAY 让客户端重拨,滚动更新时流量才能滚到新副本
      max_conn_age_grace 达龄后给在途请求的收尾宽限
    """
    options: list[tuple[str, Any]] = []
    max_age_sec = grpc_conf.max_conn_age_td().total_seconds()
    if max_age_sec <= 0:
        return options  # 不配 = 关,行为与未接此功能前一致
    options.append(("grpc.max_connection_age_ms", int(max_age_sec * 1000)))

    # ★ 配了 max_conn_age 却没配 grace 时,Go 强制兜底 30s
    # (pkg/grpcserver/grpcserver.go:81)。必须照做:grpc core 的默认是
    # **无限宽限**,不兜底的话"达龄"永远不会真正断开老连接,滚动更新时
    # 流量滚不到新副本 —— 而这与"根本没开这个功能"表现一模一样。
    #
    # ds_allocator 实配 grace=360s,盖过 330s 的 AllocateBattle 在途调用;
    # 没有它,GOAWAY 会**砍断正在等 DS ready 的分配**。
    grace_sec = grpc_conf.max_conn_age_grace_td().total_seconds()
    if grace_sec <= 0:
        grace_sec = 30.0
    options.append(("grpc.max_connection_age_grace_ms", int(grace_sec * 1000)))
    return options


def build_grpc_server(
    grpc_conf: pconfig.GrpcConf,
    *,
    auth_required: bool = False,
    extra_interceptors: Sequence[grpc.aio.ServerInterceptor] = (),
) -> grpc.aio.Server:
    """构造 gRPC server(未注册 servicer、未 start)。

    对应 Go 侧 pkg/grpcserver.MustNewServer(cfg.Server, pmw.AuthOptional())。
    """
    options: list[tuple[str, Any]] = conn_age_options(grpc_conf)

    # 拦截器顺序(与 Kratos 默认 middleware 链 Trace → Logging → Metrics → [RateLimit]
    # → KillSwitch 同序,见 pkg/grpcserver/grpcserver.go:44)。
    # 实测语义:列表里**第一个是最外层**,它拿到的 continuation 会跑后面的。
    #
    #   ① trace/身份  最外层 —— 后面每一条日志(含 access log)都要落在它的
    #                 contextvars 作用域内。放在里层的话,它的 finally 一 reset,
    #                 外层 access log 才开始打 → 四个事件全都没有 trace_id
    #   ② 可观测      记录后面每一道拒绝(含 auth 401、限流、关停、超时)
    #   ③ 限流        BBR 过载丢负载。**在可观测之内**,被丢的请求才进得了指标;
    #                 **在关停之外**,与 Go 同序
    #   ④ 关停        在鉴权之前 —— 服务已关停时不该再做鉴权工作,而且必须
    #                 **在业务 handler 之前**返回,否则副作用照样发生
    #   ⑤ 超时        给业务 handler 套 deadline
    #   ⑥ 鉴权
    chain: list[grpc.aio.ServerInterceptor] = [
        pintercept.TraceInterceptor(),
        pintercept.ObservabilityInterceptor(),
    ]
    # 第 4 层:BBR 自适应限流(过载保护)。dev 默认关,prod 由
    # tools/scripts/gen_cluster_config.ps1 -Prod 机械置 true(14 个客户端面服务)。
    #
    # 无阈值可配 —— BBR 按 CPU / inflight / RT 自己判断过载,这也是它区别于
    # Envoy 那道 local_ratelimit(固定令牌桶)的地方:后者挡的是未鉴权洪水,
    # 这里保的是"进程别被自己的负载压垮"。两道闸不可互相替代。
    if grpc_conf.enable_rate_limit:
        chain.append(pintercept.RateLimitInterceptor())
    chain.append(pintercept.KillSwitchInterceptor())
    timeout = grpc_conf.timeout_td().total_seconds()
    if timeout > 0:
        chain.append(pintercept.TimeoutInterceptor(timeout))
    chain.append(pintercept.AuthInterceptor(required=auth_required))
    chain.extend(extra_interceptors)
    server = grpc.aio.server(interceptors=chain, options=options)
    # 健康服务随 server 构造一起注册 —— 见 _register_health 的说明。
    _register_health(server)
    return server


# grpc.aio.Server 上挂健康服务的属性名。做成"构造 server 时自动注册"而不是
# "各服务 main.py 自己记得调" —— 忘了调的后果是 Pod 永远 NotReady 且服务本身
# 日志全绿(见 _register_health 的说明),这种失败模式必须靠机制排除,不能靠纪律。
_HEALTH_ATTR = "_pandora_health"


def _register_health(server: grpc.aio.Server) -> grpc_health_aio.HealthServicer:
    """注册 grpc.health.v1.Health,对应 Kratos 内置的 `health.NewServer()`。

    ★ 这不是可选的可观测装饰,是 **k8s 能否把流量给这个 Pod 的开关**。

    `deploy/k8s/services/services.yaml` 里 22 个服务的 readinessProbe 全是
    `grpc: { port: 2000x }` —— k8s 原生 gRPC 探针调的就是本服务的
    `Check(service="")`。不注册它的后果(2026-08-18 实测):

        Go 版      Check("")  → SERVING
        Python 版  Check("")  → UNIMPLEMENTED  → 探针判失败 → Pod 永远 NotReady

    而进程本身完全健康、业务 RPC 全部可用、日志一行 ERROR 都没有 ——
    与「uvicorn 绑成 IPv6-only 导致 /metrics 抓不到」同一类静默故障。
    """
    servicer = grpc_health_aio.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(servicer, server)
    setattr(server, _HEALTH_ATTR, servicer)
    return servicer


async def _health_set_serving(server: grpc.aio.Server) -> None:
    """把整进程健康状态置 SERVING —— 对应 Kratos `Start()` 里的 `s.health.Resume()`。

    空 service 名 = 整个进程,k8s 原生 gRPC 探针查的就是它;
    HealthServicer 对未登记的名字返回 NOT_FOUND,所以必须显式 set。
    """
    servicer = getattr(server, _HEALTH_ATTR, None)
    if servicer is not None:
        await servicer.set("", health_pb2.HealthCheckResponse.SERVING)


async def _health_enter_shutdown(server: grpc.aio.Server) -> None:
    """把健康状态翻成 NOT_SERVING —— 对应 Kratos `Stop()` 里的 `s.health.Shutdown()`。

    ★ 时序:必须**先于** `grpc_server.stop(grace=...)`。

    Kratos 的 `Server.Stop` 就是这个顺序(transport/grpc/server.go:241 先 Shutdown、
    再 GracefulStop)。这半拍是 §9.16「先摘流量→再排空在途」的机制本体:
    先答 NOT_SERVING 让 k8s 把本 Pod 摘出 Endpoints,已经在途的请求再用 grace 期做完。
    顺序反过来 = 排空期间 k8s 仍在往这台送新请求,滚动更新必然掉请求。
    """
    servicer: grpc_health_aio.HealthServicer | None = getattr(server, _HEALTH_ATTR, None)
    if servicer is None:
        return
    # enter_graceful_shutdown 把**所有**已登记 service 置为 NOT_SERVING 并锁定后续 set,
    # 语义与 grpc-go 的 health.Server.Shutdown() 一致。
    with contextlib.suppress(Exception):
        await servicer.enter_graceful_shutdown()


def enable_reflection(server: grpc.aio.Server, service_full_names: Sequence[str]) -> None:
    """按配置开 reflection。

    对应 Go 侧 cfg.Server.Grpc.EnableReflection —— dev 开(grpcurl list 能用),
    prod 零值 false = 关,少暴露一个攻击面。调用方负责判断开关,这里只负责注册。
    """
    reflection.enable_server_reflection(
        [*service_full_names, reflection.SERVICE_NAME], server
    )


def build_http_app(service_name: str) -> FastAPI:
    """构造 HTTP app。

    20 个服务里它只承载 /metrics + /healthz —— 对应 Go 侧那 20 份
    `internal/server/http.go`(注释都写着「仅 /metrics」)。
    只有 login 一个服务需要在这上面加 10 个 REST 路由(login.proto 的 http 注解)。
    """
    # docs 默认关:这是运维面而不是公开 API,openapi 会把内部路由结构暴露出去。
    # PANDORA_HTTP_DOCS=1 单独打开(dev 联调)。
    #
    # ★ 这个开关原先只写在注释里、**从没实现** —— 注释承诺一个不存在的能力,
    # 比不写更糟:照着做的人会以为是自己环境的问题,去查 uvicorn / Envoy。
    docs_on = os.getenv("PANDORA_HTTP_DOCS", "").strip() in ("1", "true", "True", "yes")
    app = FastAPI(
        title=f"pandora-{service_name}",
        docs_url="/docs" if docs_on else None,
        redoc_url="/redoc" if docs_on else None,
        openapi_url="/openapi.json" if docs_on else None,
    )
    # /metrics 不套业务中间件 —— 与 Go 侧注释一致:「纯 Prometheus,不经过 Pandora
    # middleware,避免 trace/log 污染监控」。
    app.add_route("/metrics", metrics.metrics_endpoint, methods=["GET"])

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        """存活探针。K8s liveness/readiness 用;Go 侧靠 Kratos 内置健康检查。"""
        return {"status": "ok", "service": service_name}

    return app


def normalize_grpc_addr(addr: str) -> str:
    """把 Go 风格的裸端口 ":20013" 转成 grpcio 能接受的形式。

    实测差异(2026-08-18):
        Go 的 net.Listen("tcp", ":20013") 接受裸端口,含义是"监听所有接口、双栈"。
        grpcio 的 add_insecure_port(":20013") 直接失败:
            Failed to add port to server: Unparsable name: :20013
            RuntimeError: Failed to bind to address :20013

    而 21 份 etc/*.yaml 里写的**全都是**裸端口形式(":20013"),这些 yaml 要同时喂给
    Go 版和 Python 版,不能改。所以在这里做归一化,而不是去改配置。

    选 "[::]:" 而不是 "0.0.0.0:":前者是双栈(IPv4 + IPv6),与 Go 的 ":port" 语义一致;
    用 0.0.0.0 会只监听 IPv4,若客户端或 Envoy 走 IPv6 回环就连不上。
    """
    if addr.startswith(":"):
        return f"[::]{addr}"
    return addr


def _addr_to_host_port(addr: str, default_port: int) -> tuple[str, int]:
    """把 yaml 的 ":21013" / "0.0.0.0:21013" 拆成 uvicorn 需要的 (host, port)。

    裸端口的 host 取 "0.0.0.0" 而**不是** "::" —— 这里和 normalize_grpc_addr 的选择相反,
    是实测出来的差异(2026-08-18):

        grpcio  add_insecure_port("[::]:20013")  → 双栈,netstat 同时出现
                                                   0.0.0.0:20013 和 [::]:20013
        uvicorn host="::"                        → **仅 IPv6**,netstat 只有 [::]:21013,
                                                   curl http://127.0.0.1:21013 直接 connection refused

    为什么这个差异必须按 IPv4 收口:
        HTTP 端口唯一的消费者是 Prometheus 抓取和 K8s liveness/readiness 探针,
        它们在集群里走 IPv4。绑成 IPv6-only 的后果是 /metrics 抓不到 →
        **Grafana 面板静默变空**,而服务本身完全健康、日志一切正常,没有任何报错。
        这跟日志字段漂移是同一类故障,只是入口不同。

    IPv6-only 环境需要时,在 yaml 里显式写 "[::]:21013",这里会原样透传。
    """
    if not addr:
        return "0.0.0.0", default_port
    host, _, port = addr.rpartition(":")
    return (host or "0.0.0.0"), int(port)


async def run(
    *,
    service_name: str,
    grpc_server: grpc.aio.Server,
    grpc_addr: str,
    http_app: FastAPI | None = None,
    http_addr: str = "",
    http_default_port: int = 0,
    on_ready: Callable[[], None] | None = None,
    background: Sequence[Callable[[], Awaitable[None]]] = (),
) -> None:
    """启动两个 server + 后台任务,阻塞到收到 SIGINT/SIGTERM。

    background 是需要随进程生命周期存活的协程(如 dialogue 的会话过期清理),
    对应 Go 侧那些 `go runXxx(ctx)` 的 goroutine。它们会在收到停止信号时被取消。
    """
    logger = plog.get()

    # ★ 运行时标识在这里打,而不是让每个 main 各自记得调。
    #
    # 灰度期同一个服务会有 Go 副本和 Python 副本同时在线,而 pandora_rpc_* 上没有
    # 任何维度能区分两者 —— 面板只有一条混合曲线,"Python 版慢不慢"这个灰度期
    # 唯一要回答的问题无法回答。放进 run() 是因为**每个服务都必须经过它**:
    # 漏打在结构上不可能发生(对照:owner/main.py 就没有调过 dialogue 那份)。
    metrics.set_runtime_info(service_name)

    # 归一化:yaml 里是 Go 风格裸端口 ":20013",grpcio 不接受,见 normalize_grpc_addr。
    grpc_server.add_insecure_port(normalize_grpc_addr(grpc_addr))
    await grpc_server.start()
    # 对应 Kratos Start() 里的 health.Resume() —— 必须在 start 之后,
    # 否则探针可能在端口还没监听时就被答 SERVING。
    await _health_set_serving(grpc_server)

    tasks: list[asyncio.Task] = []
    http_server: uvicorn.Server | None = None
    if http_app is not None:
        host, port = _addr_to_host_port(http_addr, http_default_port)
        http_server = uvicorn.Server(
            uvicorn.Config(
                http_app,
                host=host,
                port=port,
                # log_config=None:不让 uvicorn 装自己的 logging 配置,
                # 否则它会覆盖 structlog 的 handler,HTTP 侧日志字段口径就跟 Go 对不上了。
                log_config=None,
                access_log=False,
                lifespan="on",
            )
        )
        # 同样过 safego：不变量是"`tasks` 里每一个都被兜底",靠结构而不是靠记性。
        # （bind 失败这一条 uvicorn 自己会 sys.exit，是响亮的；兜底管的是它中途死掉。）
        tasks.append(psafego.supervise("http", asyncio.create_task(http_server.serve(), name="http")))

    for entry in background:
        # 必须过 safego：裸 create_task 的协程抛异常后异常只躺在 Task 里,
        # 进程照跑、health 照答 SERVING、日志零行 —— 那条循环已经死了却没人知道。
        # Go 侧这些 `go runXxx(ctx)` 全部包在 safego.Go/Loop 里,口径见 pkg/safego。
        #
        # ★ 点位名:接受 `(name, factory)` 二元组,或裸 callable。
        # 原先一律取 `factory.__name__` —— 而各服务传的十有八九是 **lambda**,
        # 于是 `pandora_safego_panic_recovered_total{name}` 这个 label 全服都是
        # `<lambda>`,等于没有点位名:告警只能告诉你"有个后台协程死了",
        # 不能告诉你**是哪一条**。Go 侧每个 safego.Go/Loop 都显式传名字。
        name, factory = entry if isinstance(entry, tuple) else (None, entry)
        if not name:
            name = getattr(factory, "__name__", "") or "bg"
            if name == "<lambda>":
                # 这里刻意**不** raise:点位名是可观测性问题,不该把它升级成启动失败
                # (那等于用一次可用性事故去换一个 label)。改成启动期 WARN + 一条
                # 机械检查(tests/test_service_layer_contract.py)在 CI 上拦住。
                name = "bg_anonymous"
                plog.get().warning(
                    "background_task_anonymous",
                    service=service_name,
                    hint="server.run(background=[...]) 收到匿名 lambda;"
                    "请改写成 (name, factory) 二元组,否则 panic 计数与日志里"
                    "只有 bg_anonymous,出事时看不出死的是哪条后台循环",
                )
        tasks.append(psafego.spawn(name, factory))

    if on_ready is not None:
        on_ready()

    stop = asyncio.Event()

    def _request_stop(*_args: object) -> None:
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            # Linux(prod)走这条:回调由事件循环调度,可以安全碰 asyncio 对象。
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, ValueError, OSError, RuntimeError):
            # Windows 的 ProactorEventLoop 不支持 add_signal_handler,退化到 signal.signal。
            #
            # ⚠️ 这里原本写成 `for ... else:` —— for 没有 break,所以 else **恒执行**,
            # 等于在每个平台上都用 signal.signal 覆盖掉刚装好的 asyncio 处理器,
            # 注释说的"Windows 退化"实际是"无条件覆盖"。后果是 SIGTERM 回调在
            # 信号上下文里直接动 asyncio.Event,而不是由事件循环调度 —— 而 SIGTERM
            # 正是滚动更新的优雅停机入口(§9.16 先摘流量再排空),这条路径出问题
            # 表现为"发布期间偶发客户端错误",不会指向 server.py。
            with contextlib.suppress(ValueError, OSError):
                signal.signal(sig, _request_stop)

    await stop.wait()
    logger.info("service_stopping", service=service_name)

    # 优雅停机顺序,与 Kratos 的 app.Stop 一致 —— 每一步的先后都有后果:
    #   ① 健康状态翻 NOT_SERVING → k8s 把本 Pod 摘出 Endpoints(停止送**新**请求)
    #   ② grpc stop(grace) → 已在途的请求做完
    #   ③ 停 HTTP,④ 取消后台任务
    # ①②不能对调:先 stop 再翻状态 = 排空期间 k8s 还在往这台送流量(§9.16)。
    await _health_enter_shutdown(grpc_server)
    await grpc_server.stop(grace=5.0)
    if http_server is not None:
        http_server.should_exit = True
    for task in tasks:
        task.cancel()
    for task in tasks:
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
    logger.info("service_stopped", service=service_name)
