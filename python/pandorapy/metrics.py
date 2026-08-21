"""Prometheus 指标 —— 对应 Go 侧 pkg/metrics(60 行,自定义指标很少)。

Grafana 面板认的是**指标名 + label 名**,和日志字段口径同理:名字漂移 = 面板静默变空。
Go 侧现有自定义指标只有 pandora_db_table_rows / _budget 两族(dbguard 用),
dialogue 不产生自定义业务指标,所以本模块当前只负责:
  - 暴露一个 /metrics ASGI app,挂到 FastAPI 上(端口 2100x,与 Go 侧一致)
  - 提供 RPC 层通用指标,供拦截器统一打点

⚠️ 刻意**不**用 prometheus_client 的默认全局 REGISTRY 之外的自定义 registry:
   默认 registry 自带 process_* / python_gc_* 采集器,是排查 Python 侧内存/GC 问题
   (迁移期最可能出问题的地方)的免费信息源,不要为了"干净"把它关掉。
"""

from __future__ import annotations

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from starlette.requests import Request
from starlette.responses import Response

# ── RPC 层通用指标 ────────────────────────────────────────────────────────────
#
# ★ 名字与 label 必须与 Go 侧 pkg/middleware/metrics.go **逐字一致**。
#
# 这里原先用的是 grpc_server_*(Prometheus/gRPC 生态惯例),理由写的是"同名指标便于
# 对比两个实现"—— 那个前提在本仓**不成立**:Go 侧根本没有 grpc_server_* 这族指标,
# 它用的是 pandora_rpc_total / pandora_rpc_duration_seconds。后果是灰度期按
# pandora_* 建的告警对 Python 实例是 **NoData 而不是告警** —— 静默失去覆盖,
# 比"指标名不好看"严重得多。
#
# Go 侧定义(pkg/middleware/metrics.go:24-38):
#     pandora_rpc_duration_seconds{service,method,code}  Histogram, ExponentialBuckets(0.001,2,12)
#     pandora_rpc_total{service,method,code}             Counter
# 其中 service 是 proto 服务名的**最后一段**(LoginService),不是全限定名。

# 与 Go 的 pmetrics.StandardBuckets 逐值一致:ExponentialBuckets(0.001, 2, 12)。
# 直方图桶边界不一致 = 两栈的 P99 不可比,而这正是灰度时唯一要看的数。
STANDARD_BUCKETS = tuple(0.001 * (2**i) for i in range(12))

RPC_DURATION = Histogram(
    "pandora_rpc_duration_seconds",
    "RPC handler duration in seconds.",
    ["service", "method", "code"],
    buckets=STANDARD_BUCKETS,
)
RPC_TOTAL = Counter(
    "pandora_rpc_total",
    "RPC call count.",
    ["service", "method", "code"],
)

# ── 以下三个是 Python 侧**独有**的补充指标 ────────────────────────────────────
#
# Go 侧没有对应物,所以刻意用不同的 metric 名,不去污染上面那两个 —— 一族指标
# 只能有一个语义来源,否则跨栈对比时会把"Python 多打的那部分"算进共同口径。

RPC_INBAND_TOTAL = Counter(
    "pandora_rpc_inband_total",
    "按业务 errcode 分的 RPC 完成数(Go 侧只把它打进 access log,没有对应指标)",
    # 本仓的业务失败走 response.code(errcode),gRPC status 基本恒为 OK ——
    # 上面那族 code label 与 Go 一样只能看到 "ok",这一族才能看出是哪个业务错。
    ["service", "method", "errcode"],
)
RPC_PANIC = Counter(
    "pandora_rpc_panics_total",
    "gRPC handler 未捕获异常数",
    ["service", "method", "exc_type"],
)
RPC_CANCELED = Counter(
    "pandora_rpc_canceled_total",
    "gRPC handler 被取消数(客户端超时 / 断连)",
    # ★ 必须与 panics 分开计。grpc.aio 用**取消**来终止超时的 handler,
    # 混进 panics 会让每一次正常的客户端 deadline 都表现成"服务端在 panic",
    # 弱网 / 高峰时把真正的未捕获异常淹掉。
    ["service", "method"],
)
RATELIMIT_DROPPED = Counter(
    "pandora_ratelimit_dropped_total",
    "BBR 自适应限流丢弃的请求数",
    # 被丢的请求同时也会计进 pandora_rpc_total{code=resource_exhausted}
    # (RateLimit 拦截器在 Observability 内层,与 Go 侧链序一致)。但那个 label
    # 混着**所有**来源的 RESOURCE_EXHAUSTED —— 出事时第一个问题是"到底是不是
    # BBR 在丢",拿混合曲线答不了,所以这里单独计一份归因明确的。
    #
    # 只计数、不打日志:过载时每丢一条就写一行日志,等于在最不该增加负载的时刻
    # 给进程加 I/O,会把 BBR 本身变成放大器。Go 侧同样一行日志都不打。
    ["service", "method"],
)


# ── 运行时标识 ──────────────────────────────────────────────────────────────

RUNTIME_INFO = Gauge(
    "pandora_runtime_info",
    "进程运行时标识(恒为 1 的 info 指标,label 才是内容)",
    ["service", "runtime"],
)


def set_runtime_info(service: str, runtime: str = "python") -> None:
    """标记本进程的实现语言。各服务 main 在启动时调一次。

    ★ 为什么需要它:灰度期同一个服务会有 Go 副本和 Python 副本**同时在线**,
    而 pandora_rpc_* 上没有任何维度能区分两者 —— 面板上只能看到一条混合曲线,
    "Python 版慢不慢 / 错得多不多"这个灰度期唯一要回答的问题**无法回答**。

    用恒为 1 的 info 指标 + label 是 Prometheus 的标准做法(同 kube_*_info):
    面板按 instance join 一下就能把两栈分开,而不必给每个业务指标都加一个 label
    (那会让所有历史曲线的 label 集合变化,旧告警全部失配)。

    `service_ready` 日志里已经有同名的 `runtime` 字段,两边口径一致。
    """
    RUNTIME_INFO.labels(service, runtime).set(1)


def short_service(service: str) -> str:
    """把全限定服务名收成 Go 侧的 label 形态:pandora.login.v1.LoginService → LoginService。

    对应 Go 的 splitOperation。label 值不一致的话,同一块面板上两栈会各占一行
    (service="LoginService" vs service="pandora.login.v1.LoginService"),
    "对比两个实现"就无从谈起。
    """
    return service.rsplit(".", 1)[-1] if service else "unknown"


def code_label(grpc_code: object | None) -> str:
    """把 gRPC status 归到 Go 的 code 桶。对应 Go 的 codeLabel。

    ★ 语义要与 Go 对齐:Go 取的是 **transport error**,而本仓业务失败是 in-band
    (response.code),transport err 恒 nil —— 所以 Go 侧业务失败也记 "ok"。
    这里照做,不"改进"。业务维度看 RPC_INBAND_TOTAL,别把两族混起来。
    """
    if grpc_code is None:
        return "ok"
    name = getattr(grpc_code, "name", str(grpc_code))
    if name == "OK":
        return "ok"
    if name in ("UNAUTHENTICATED", "PERMISSION_DENIED"):
        return "unauthorized"
    if name == "DEADLINE_EXCEEDED":
        return "timeout"
    if name in (
        "INVALID_ARGUMENT",
        "NOT_FOUND",
        "ALREADY_EXISTS",
        "FAILED_PRECONDITION",
        "OUT_OF_RANGE",
        "RESOURCE_EXHAUSTED",
        "CANCELLED",
        "ABORTED",
    ):
        return "client_err"
    if name in ("INTERNAL", "UNAVAILABLE", "UNKNOWN", "DATA_LOSS", "UNIMPLEMENTED"):
        return "server_err"
    return "other"


async def metrics_endpoint(_request: Request) -> Response:
    """/metrics 处理器。对应 Go 侧 srv.Handle("/metrics", metrics.MustHandler())。

    Go 侧注释明确写了「纯 Prometheus,不经过 Pandora middleware,避免 trace/log 污染监控」
    —— 这里同样直接挂 route,不套业务中间件。
    """
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
