"""grpc.aio 服务端拦截器 —— 对应 Go 侧 pkg/middleware 的 Kratos middleware 链。

这是"Kratos 那一层"在 Python 侧的替代物之一。Kratos 提供的是一套 middleware 约定 +
现成件;grpcio 只给 ServerInterceptor 原语,所以约定要自己定。本文件就是那份约定。

对齐的 Go 侧行为:
    pmw.AuthOptional()  —— 从 Envoy jwt_authn 注入的 x-pandora-player-id 头读 player_id,
                           有就注入 ctx,没有就放过(登录前 RPC 也要能调)。
    pmw.AuthRequired()  —— 同上但缺 player_id 直接 401。
    plog.With(ctx)      —— 让本请求后续所有日志自动带 player_id / trace_id。
    safego.Run          —— panic 兜底:单个请求崩不能把进程带走。

身份来源的安全前提(与 Go 侧完全一致,不可放宽):
    x-pandora-player-id 是 **Envoy 验签 JWT 之后重写**的头,入站时被无条件剥离。
    因此在客户端面(:8443)它是可信的。直连内网端口联调时没有网关注入 → player_id=0,
    按匿名处理,由业务层 fail-closed。
    ⚠️ 绝不能改成"请求体里带 player_id 就信" —— R5 已经把 proto 里的 player_id 字段
       整批 reserved 掉了,正是为了消灭伪造他人身份的路径。
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import os
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import grpc

from pandorapy import errcode
from pandorapy import errcode_grpc
from pandorapy import killswitch
from pandorapy import log as plog
from pandorapy import metrics

# Envoy jwt_authn 验签后注入的玩家身份头(全后端统一用这个头当玩家身份)。
METADATA_KEY_PLAYER_ID = "x-pandora-player-id"
# Envoy 验签成功后重写的 JWT payload 头(会话现行性门用,dialogue 暂不需要)。
METADATA_KEY_JWT_PAYLOAD = "x-pandora-jwt-payload"
# 链路追踪头。★ 必须是 x-pandora-trace-id,**不是** Envoy 的 x-request-id ——
# 客户端面(:8443)的 envoy 只无条件剥离 player-id / jwt-payload 两个**身份**头,
# x-pandora-trace-id 是原样透传的,UE 客户端 / DS 自带的 trace_id 要被全链采纳。
# 取错头 = Python 服务的日志与 Go 服务、与客户端的 trace 全部串不起来(且不报错)。
METADATA_KEY_TRACE_ID = "x-pandora-trace-id"

# 入站 trace_id 长度上限,对应 Go 的 middleware.MaxTraceIDLen。
# UUID 是 36 字符,留一倍余量给「客户端本地 trace + 序号」这类拼接格式。
MAX_TRACE_ID_LEN = 64
# 允许字符集 = ASCII 字母数字 + '-' + '_'(覆盖 UUID / hex / base64url 风格)。
_SAFE_TRACE_ID_RE = re.compile(r"\A[A-Za-z0-9_-]+\Z")


def is_safe_trace_id(v: str) -> bool:
    r"""入站 trace_id 是否可直接采纳 —— 对应 Go 的 isSafeTraceID。

    trace_id 不是信任边界(不参与鉴权、不做 metric label),采纳外部值正是跨进程
    串联所必需;但取值本身必须先过闸,否则一个畸形客户端就能往全服日志里灌任意
    长度 / 任意字节的内容:

      - 换行会让按行切分的日志管道把一条日志拆成多条,**可伪造出看似来自其它服务的日志行**;
      - 控制字符与多字节序列会打穿下游解析器;
      - 无界长度直接把日志卷撑爆。

    ⚠️ 正则用 `\A...\Z` 而不是 `^...$`:Python 的 `$` 匹配"末尾**或末尾换行之前**",
    写成 `$` 时 "abc
" 会被判为合法,换行注入这条闸就白设了(2026-08-18 在
    auction 幂等键上踩过同一个坑)。
    """
    if not v or len(v) > MAX_TRACE_ID_LEN:
        return False
    return _SAFE_TRACE_ID_RE.match(v) is not None


def _metadata_get(metadata: Any, key: str) -> str:
    """从 invocation_metadata 取一个值。grpcio 给的是 (key, value) 元组序列。"""
    if not metadata:
        return ""
    for entry in metadata:
        # grpcio 的 metadata key 已经是小写(HTTP/2 规范要求),这里仍做一次归一化
        # 以防被 Envoy 或测试夹具改过大小写。
        if entry[0].lower() == key:
            value = entry[1]
            return value.decode() if isinstance(value, bytes) else str(value)
    return ""


def extract_player_id(context: grpc.aio.ServicerContext) -> int:
    """从 metadata 提取 player_id。取不到 / 非法 → 0(匿名)。

    对应 Go 的 pkg/middleware.extractPlayerID。非法值按 0 处理而不是报错:
    Envoy 注入的头永远合法,能走到"非法"分支说明是直连联调,按匿名更合理。
    """
    raw = _metadata_get(context.invocation_metadata(), METADATA_KEY_PLAYER_ID)
    if not raw:
        return 0
    try:
        value = int(raw)
    except ValueError:
        return 0
    return value if value > 0 else 0


# ── access log(对应 Go 侧 pkg/middleware/logging.go)────────────────────────
#
# 字段口径按 docs/design/infra.md §11:{ts, level, service, trace_id, player_id,
# op, latency_ms, code, err}。四个事件名是运维手册
# docs/ops/player-journey-log-map.md 的排障入口:
#
#     rpc_ok            DEBUG  成功(高 QPS 下最大噪音源,生产 info 级不出)
#     rpc_slow          WARN   耗时 >= LOG_SLOW_RPC_MS(默认 500ms)
#     rpc_failed        ERROR  transport 层失败
#     rpc_inband_error  ERROR  transport 无错,但 in-band code 是服务端 / 基础设施故障
#                              —— 不升级的话它会被记成 rpc_ok(DEBUG),在生产 info 级
#                              下彻底静默(§16 禁止吞掉故障)
#
# ★ 缺了这一层的后果不是"日志少几条":灰度期按 rpc_slow 看只看得到 Go 副本,
#   会直接得出「慢只在 Go 上」的错误结论。


def slow_rpc_threshold_ms() -> int:
    """慢请求阈值。环境变量名与 Go 的 slowRPCThresholdMs **同名**:LOG_SLOW_RPC_MS。"""
    raw = os.getenv("LOG_SLOW_RPC_MS", "")
    if raw:
        try:
            ms = int(raw)
        except ValueError:
            ms = 0
        if ms > 0:
            return ms
    return 500


# 服务端 / 基础设施故障码。**逐个列举**,与 Go 的 errcode.IsServerFault 同一组
# (pkg/errcode/errcode.go:397 是 switch 四个具名码,不是数值区间 ——
# 按区间猜会把一大批正常的业务拒绝码误升成 ERROR,把真故障淹掉)。
# in-band 返回这些码时必须升 ERROR,否则会以 rpc_ok(DEBUG)在生产 info 级静默。
_SERVER_FAULT_CODES = frozenset(
    {
        errcode.ErrUnknown,
        errcode.ErrInternal,
        errcode.ErrTimeout,
        errcode.ErrUnavailable,
    }
)


def _is_server_fault(code: int) -> bool:
    return code in _SERVER_FAULT_CODES


def _latency_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def _observe(service: str, method: str, started: float, code_label: str) -> None:
    """记一次 RPC 的时延与计数(与 Go 的 Metrics middleware 同 label)。"""
    elapsed = time.perf_counter() - started
    metrics.RPC_DURATION.labels(service, method, code_label).observe(elapsed)
    metrics.RPC_TOTAL.labels(service, method, code_label).inc()


# Go 的 access log 带 transport 字段(kind + 方向)。Python 侧目前只承载 server 端
# unary,故恒为 "grpc";留这个字段是为了两栈的 LogQL 可以用同一条查询。
_TRANSPORT = "grpc"


def _access_log_ok(op: str, started: float, code: int) -> None:
    ms = _latency_ms(started)
    log = plog.get()
    if _is_server_fault(code):
        # handler 以 in-band code 返回了服务端 / 基础设施故障(没有 transport 错误)。
        # 不单独升级的话它会落进下面的 rpc_ok(DEBUG),生产 info 级下看不见。
        log.error("rpc_inband_error", transport=_TRANSPORT, op=op, latency_ms=ms, code=code)
        return
    slow = slow_rpc_threshold_ms()
    if ms >= slow:
        log.warning(
            "rpc_slow", transport=_TRANSPORT, op=op, latency_ms=ms,
            code=code, slow_threshold_ms=slow,
        )
        return
    log.debug("rpc_ok", transport=_TRANSPORT, op=op, latency_ms=ms, code=code)


def _access_log_failed(
    op: str, started: float, *, code: int, reason: str, err: str
) -> None:
    """rpc_failed。字段形状对齐 Go:{transport, op, latency_ms, code, reason, err}。

    ★ `code` 是**数值**,不是状态名字符串 —— Go 侧是 `errors.Code(err)`(int)。
    同名字段两栈值域不相交的话,按 code 过滤的面板 / LogQL 只能命中一半流量,
    而"少一半"看起来跟"这段时间没错"一模一样。
    ⚠️ 两栈的数值**体系**仍不同:Go 是 Kratos 的 HTTP 式码(401/500…),
    Python 没有 Kratos 层,用的是 gRPC 状态码数值。名字放 `reason` 里,
    跨栈对比按 reason 更可靠。
    """
    plog.get().error(
        "rpc_failed", transport=_TRANSPORT, op=op,
        latency_ms=_latency_ms(started), code=code, reason=reason, err=err,
    )


def _split_method(full_method: str) -> tuple[str, str]:
    """把 /pandora.dialogue.v1.DialogueService/StartDialogue 拆成 (service, method)。

    用于指标 label。拆不开时整串当 method,免得指标里出现空 label。
    """
    trimmed = full_method.lstrip("/")
    if "/" in trimmed:
        service, _, method = trimmed.partition("/")
        return service, method
    return "", trimmed


# gRPC 保留命名空间 —— health / reflection / channelz 都在这下面。
# 业务 proto 全部是 pandora.*,不会相撞。
_INFRA_METHOD_PREFIX = "/grpc."


def is_infrastructure_method(method: str) -> bool:
    """是否是 gRPC 基础设施服务(健康检查 / 反射 / channelz)。

    这些不是业务 RPC:调用方是 k8s 探针和运维工具,不带玩家身份,
    也不应计入业务鉴权口径。
    """
    return bool(method) and method.startswith(_INFRA_METHOD_PREFIX)


async def _call_handler(inner: Any, request: Any, context: Any) -> Any:
    """调用被包装的 handler,兼容**同步**实现。

    grpcio 自己在 `_finish_handler_with_unary_response` 里是 `if isawaitable(x): x = await x`,
    即同步 handler 合法。而拦截器一旦无条件 `await inner(...)`,就比 grpcio 本体更严 ——
    任何第三方同步 unary servicer 挂进来都会以 UNKNOWN 失败。

    2026-08-18 实测的代价:`grpc_health.v1.health.HealthServicer`(同步版)被这样打挂,
    表现是 k8s readinessProbe 一直失败、Pod 永不 Ready,而业务 RPC 全部正常、
    日志里只有一条 TypeError —— 没人会把它和"Pod 不 Ready"联系起来。
    健康服务本身已改用 _async 版,这里是防下一个第三方 servicer 再踩。
    """
    result = inner(request, context)
    if inspect.isawaitable(result):
        return await result
    return result


class TraceInterceptor(grpc.aio.ServerInterceptor):
    """链路 ID 与身份的日志绑定。对应 Go 侧 pkg/middleware.Trace()。

    ★ 它必须是**链上最外层的一个**,原因与 Go 把 `Trace()` 排在 `Logging()` 之前一样:

        绑定是 contextvars,作用域只在本拦截器的 try 内。原先这段代码住在
        AuthInterceptor(最内层),它的 finally 一 reset,**外层的 access log 才开始打**
        —— 于是 rpc_ok / rpc_slow / rpc_failed 四个事件**全都没有 trace_id**,
        而 trace_id 正是把一次请求在多个服务里的日志串起来的唯一手段。
        字段缺失不会报错,只是 Loki 里那几条日志永远关联不上,排障时看起来像
        "这个服务没参与这次请求"。

    入站 trace_id 过安全闸(长度 + 字符集),不合规或缺失就自己生成并经回程头交还。
    """

    async def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        handler = await continuation(handler_call_details)
        if handler is None or handler.request_streaming or handler.response_streaming:
            return handler
        if is_infrastructure_method(handler_call_details.method):
            return handler

        inner = handler.unary_unary

        async def wrapper(request: Any, context: grpc.aio.ServicerContext) -> Any:
            player_id = extract_player_id(context)
            # 对应 Go trace.go 的 server 分支:入站值过安全闸 → 不合规/缺失就自己生成。
            # ★ 不能"没有就不打" —— 那样一条无 trace_id 的日志在 Loki 里无法与同一次
            # 请求的其它服务日志关联,而排障恰恰最需要这种请求。生成的 UUID 至少能把
            # **本服务内**这次请求的所有日志串起来,并经回程头交还给调用方。
            trace_id = _metadata_get(context.invocation_metadata(), METADATA_KEY_TRACE_ID)
            if not is_safe_trace_id(trace_id):
                # str(uuid4()) 而不是 .hex:Go 用 uuid.NewString(),是带连字符的
                # 36 字符形式。同一条链路上 Go 服务与 Python 服务生成的 trace_id
                # 长得不一样会让人以为是两条链路,没必要制造这个差异。
                trace_id = str(uuid.uuid4())
            # 回程头:对应 Go 的 tr.ReplyHeader().Set(MetadataKeyTraceID, traceID)。
            # 在 handler 之前发,与 Go 侧「handler 尚未开始组装响应」的时点一致。
            with contextlib.suppress(Exception):
                await context.send_initial_metadata(((METADATA_KEY_TRACE_ID, trace_id),))

            tokens = [plog.bind_trace_id(trace_id)]
            if player_id:
                tokens.append(plog.bind_player_id(player_id))
            try:
                return await _call_handler(inner, request, context)
            finally:
                # contextvars 的 Token 必须逆序 reset,否则嵌套调用会串上下文。
                for token in reversed(tokens):
                    token.var.reset(token)

        return grpc.unary_unary_rpc_method_handler(
            wrapper,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


class KillSwitchInterceptor(grpc.aio.ServerInterceptor):
    """RPC 级临时关停。对应 Go 侧 pkg/middleware.KillSwitch()。

    ★ 这个拦截器此前**根本不存在**:killswitch 模块整份迁过来了、规则也能解析,
    但没有任何地方查它 —— 模块注释还写着"拦截器用它"。表现是运维照手册加了
    关停规则、接口返回成功,而流量一条没挡住。关停是"出事时才用"的能力,
    这种失效恰好只在出事时才被发现。

    fail-open 铁律:未装配 Manager / 规则源不可用 → 一律放行。
    绝不因 Kill-Switch 自身故障把全服打死(与 Go 同)。

    基础设施方法(grpc.health / reflection)不过关停 —— 关掉健康检查会让 k8s
    把整个 Pod 摘掉,那不是"关一个 RPC",那是下线整个副本。
    """

    async def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        method = handler_call_details.method
        if is_infrastructure_method(method):
            return await continuation(handler_call_details)

        is_off, reason = killswitch.disabled(method)
        if not is_off:
            return await continuation(handler_call_details)

        async def denied(request: Any, context: grpc.aio.ServicerContext) -> Any:
            plog.get().warning(
                "rpc_killswitch_denied",
                # op 与 access log 同口径:**带**前导斜杠(= Go 的 transport.Operation())。
                # 同一次请求的两条日志 op 长得不一样,按 op 关联时会漏掉其中一条。
                op=method if method.startswith("/") else f"/{method}",
                reason=reason,
            )
            # ★ 状态码从**错误码**推导,不硬编码。
            # 硬编码的问题不是"写错了" —— 是它与 killswitch 模块声明的
            # ErrServiceDisabled 各写一份,改了一处另一处不跟着变而且不报错
            # (本模块头注释与 disabled_error() 曾经就是这么对不上的)。
            err = killswitch.disabled_error(reason)
            # ★ 消息用 Go 的形态 `errcode=13 <reason>`(str(PandoraError) 就是它),
            # 而不是裸 reason:
            #
            # Go 侧 `*errcode.Error` **刻意不实现** GRPCStatus()(pkg/errcode/grpc.go 头注释:
            # 加上会静默改变全部服务 unary 路径的线上错误形态),所以 Go 的 killswitch 拒绝
            # 经 Kratos 默认映射出去,**状态码与这里不一定相同**,但消息里那段
            # "errcode=13 …" 是两栈共有的。灰度期客户端要判"是不是被关停",
            # 应当认 message 里的 errcode 而不是 gRPC 状态码 —— 后者两栈可能不一致。
            # (Go 侧哪天把这条路接上 ToGRPCError,两边就收敛了。)
            await errcode_grpc.abort_with(context, err)

        handler = await continuation(handler_call_details)
        if handler is None:
            return handler
        if handler.request_streaming or handler.response_streaming:
            # ★ 流式不能在这里包:下面用 unary_unary_rpc_method_handler 重建,
            # 会把一个 stream handler **重包成 unary**,客户端收到的是协议级错误
            # 而不是"维护中"。Go 侧同理:stream 走的是单独的
            # KillSwitchStreamCheck(由 handler 第一行自己调),不在 middleware 链里。
            # 全项目唯一的流是 push.Subscribe,迁到它时按 Go 的做法在 handler 里查。
            return handler
        # 关停必须**在业务 handler 之前**返回,而不是让它跑完再丢弃结果 ——
        # 后者等于没关(副作用照样发生)。
        return grpc.unary_unary_rpc_method_handler(
            denied,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


class TimeoutInterceptor(grpc.aio.ServerInterceptor):
    """服务端单请求超时上限。对应 Go 侧 pkg/grpcserver 按 cfg.Timeout 设的 deadline。

    ★ `server.grpc.timeout` 在 21 份 yaml 里都配了(dev 是 15s),Python 侧此前
    **解析了但从未作用到任何地方** —— 一个慢 handler 会一直占着连接和 event loop 槽,
    而配置文件看起来是有保护的。

    与客户端自带的 deadline 取**更短**的那个:客户端可以要求更快失败,
    但不能要求比服务端上限更久。
    """

    __slots__ = ("_timeout_sec",)

    def __init__(self, timeout_sec: float) -> None:
        self._timeout_sec = timeout_sec

    async def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        handler = await continuation(handler_call_details)
        if handler is None or handler.request_streaming or handler.response_streaming:
            return handler
        if self._timeout_sec <= 0:
            return handler
        if is_infrastructure_method(handler_call_details.method):
            return handler

        inner = handler.unary_unary
        limit = self._timeout_sec

        async def wrapper(request: Any, context: grpc.aio.ServicerContext) -> Any:
            remaining = context.time_remaining()
            budget = min(limit, remaining) if remaining is not None else limit
            try:
                return await asyncio.wait_for(
                    _call_handler(inner, request, context), timeout=budget
                )
            except TimeoutError:
                # 用 DEADLINE_EXCEEDED 而不是 in-band 业务码:这是**传输层**的
                # 超时约定,客户端(含 Envoy / UE)按 gRPC status 判重试。
                await context.abort(
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                    f"server timeout after {budget:.3f}s",
                )

        return grpc.unary_unary_rpc_method_handler(
            wrapper,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


class AuthInterceptor(grpc.aio.ServerInterceptor):
    """把 Envoy 注入的 player_id 绑到日志上下文。

    required=False 对应 Go 的 AuthOptional(dialogue 用的就是这个);
    required=True 对应 AuthRequired,缺 player_id 直接 UNAUTHENTICATED。

    为什么 dialogue 用 Optional 而 service 层还要再查一次 callerID==0:
    Envoy jwt_authn 已在路由层 require JWT,拦截器不重复拒绝;但直连内网端口联调
    没有网关,业务层那道 `player_id == 0 → ERR_UNAUTHORIZED` 是兜底。两层都要在。
    """

    def __init__(self, *, required: bool = False) -> None:
        self._required = required

    async def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        handler = await continuation(handler_call_details)
        if handler is None:
            return handler
        # ★ 基础设施服务不过业务鉴权。
        # k8s 的 readinessProbe(`grpc: { port: 2000x }`)调 grpc.health.v1.Health/Check
        # 时**不可能**带 x-pandora-player-id,鉴权一挡 Pod 就永远 NotReady,
        # 而现象只是"探测失败",看不出是鉴权拦的(2026-08-18 被本仓测试当场抓到)。
        # 判据用 "/grpc." 前缀:health / reflection / channelz 都在这个保留命名空间下,
        # 业务 proto 全部是 pandora.* —— 两者不可能相撞。
        if is_infrastructure_method(handler_call_details.method):
            return handler
        # 只包 unary-unary:本仓库 208 个 RPC 里 207 个是 unary,唯一的流是
        # push.Subscribe。流式路径的身份提取走的是另一套(Go 侧注释也说明了
        # stream 不跑 unary middleware 链),等迁 push 时单独接,不在这里凑。
        if handler.request_streaming or handler.response_streaming:
            return handler

        inner = handler.unary_unary
        required = self._required

        async def wrapper(request: Any, context: grpc.aio.ServicerContext) -> Any:
            # ★ 这里**只做鉴权判定**。trace_id / player_id 的日志绑定已经上移到
            # TraceInterceptor(最外层)—— 原因见那个类的注释:绑在这里的话,
            # 外层 access log 打的时候 contextvars 已经被 reset 了。
            player_id = extract_player_id(context)
            if required and player_id == 0:
                await context.abort(
                    grpc.StatusCode.UNAUTHENTICATED, "missing or invalid player_id"
                )
            return await _call_handler(inner, request, context)

        return grpc.unary_unary_rpc_method_handler(
            wrapper,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


class ObservabilityInterceptor(grpc.aio.ServerInterceptor):
    """指标 + panic 兜底 —— 对应 Go 侧的 recovery/metrics middleware + safego。

    panic 兜底为什么必须有:
        Go 里一个 handler panic 会被 Kratos recovery middleware 拦住,只毁这一个请求。
        Python 里未捕获异常会被 grpcio 转成 UNKNOWN 返回给客户端,**并且**堆栈只在
        grpcio 内部打印 —— 不走我们的 structlog,于是 Loki 里什么都看不到。
        这正是迁 Python 最大的新风险(类型错误从编译期挪到运行期),必须在这里收口:
        统一打成结构化日志 + 计数,才能在 Grafana/Sentry 里看见。
    """

    async def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        handler = await continuation(handler_call_details)
        if handler is None or handler.request_streaming or handler.response_streaming:
            return handler

        service, method = _split_method(handler_call_details.method)
        inner = handler.unary_unary

        short_service = metrics.short_service(service)
        # ★ op 带前导斜杠,与 Go 的 transport.Operation() 逐字一致
        # (Kratos 的 Operation 形如 "/pandora.dialogue.v1.DialogueService/StartDialogue")。
        # 少一个斜杠 = 按 op 精确匹配的 LogQL / 面板在 Python 副本上全部落空。
        op = f"/{service}/{method}"

        async def wrapper(request: Any, context: grpc.aio.ServicerContext) -> Any:
            started = time.perf_counter()
            try:
                response = await _call_handler(inner, request, context)
            except asyncio.CancelledError:
                # ★ 必须排在兜底之前,而且**不算 panic**。
                # CancelledError 不是 Exception 的子类,所以它会一路落进下面的
                # `except BaseException` 兜底;而 grpc.aio 正是用**取消**来终止
                # 超时的 handler —— 于是每一次正常的客户端 deadline 都会产生一条
                # 带完整堆栈的 ERROR + 一次 RPC_PANIC。弱网 / 高峰的 deadline 风暴
                # 会造出「Python 在 panic」的假告警,并把真正的未捕获异常淹掉。
                # Go 侧 ctx 取消是普通 error,不会进 Recovery。
                metrics.RPC_CANCELED.labels(short_service, method).inc()
                _observe(short_service, method, started, "client_err")
                _access_log_failed(
                    op, started,
                    code=grpc.StatusCode.CANCELLED.value[0],
                    reason=grpc.StatusCode.CANCELLED.name,
                    err="client canceled",
                )
                raise
            except grpc.aio.AbortError:
                # context.abort 的正常控制流(如 AuthRequired 的 401),不是故障。
                # 但它**是**一次失败的 RPC,必须进 access log 与指标,否则
                # 「登录一直 401」这类问题在 Python 副本上完全看不见。
                #
                # ★ 状态码要从 **context** 读,不是从异常读:实测
                # `grpc.aio.AbortError` 的属性只有 add_note / args / with_traceback,
                # **没有** code() / details()。原先写的 getattr(exc, "code", ...)
                # 恒取到 None → code_label(None) = "ok" → 所有 transport 级拒绝
                # 都被记成成功,err 还是空串。而这条路径正是 401/403 风暴的唯一信号。
                status = context.code() if hasattr(context, "code") else None
                label = metrics.code_label(status)
                _observe(short_service, method, started, label)
                _access_log_failed(
                    op,
                    started,
                    code=(status.value[0] if status is not None else 0),
                    reason=getattr(status, "name", label),
                    err=(context.details() if hasattr(context, "details") else "") or "",
                )
                raise
            except BaseException as exc:  # noqa: BLE001 —— 兜底就是要抓全部
                metrics.RPC_PANIC.labels(short_service, method, type(exc).__name__).inc()
                _observe(short_service, method, started, "server_err")
                plog.get().exception(
                    "rpc_handler_unhandled_exception",
                    grpc_service=service,
                    grpc_method=method,
                    exc_type=type(exc).__name__,
                )
                _access_log_failed(
                    op, started,
                    code=grpc.StatusCode.UNKNOWN.value[0],
                    reason="UNKNOWN",
                    err=repr(exc),
                )
                raise

            # 走到这里 = 没有 transport 错误。与 Go 侧 codeLabel 一致记 "ok" ——
            # 本仓的业务失败是 in-band(response.code),Go 那边同样记 "ok"。
            _observe(short_service, method, started, "ok")
            # 业务错误码从 response.code 取(本仓库的失败语义在 body 里,不在 gRPC status)。
            code = int(getattr(response, "code", 0) or 0)
            metrics.RPC_INBAND_TOTAL.labels(short_service, method, str(code)).inc()
            _access_log_ok(op, started, code)
            return response

        return grpc.unary_unary_rpc_method_handler(
            wrapper,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
