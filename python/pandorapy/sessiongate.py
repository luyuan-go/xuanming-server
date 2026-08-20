"""会话现行性门 —— 对应 Go 的 `pkg/sessiongate` + `pkg/middleware/session.go`。

**这是 Python 侧覆盖面最广的一个缺口**：Go 有 13 个服务在各自的
`internal/server/grpc.go` 里挂 `pmw.SessionCurrent`，push 是第 14 处（形态不同，见文末）。

**缺了会怎样**：Envoy 的 `jwt_authn` 只验签名与 `exp`，验签只证明"曾经登录过"，
不证明"没被顶号"。顶号后旧 token 在 exp 前（默认 24h）仍能过网关；业务服务若只信
`x-pandora-player-id`，旧设备就保留了全部按 player_id 定向的能力（好友申请 / 交易 /
背包 / 组队）。这就是 INC-20260722-004 的形状，而且**完全静默** ——
旧设备的每一次请求看起来都是一次正常的、鉴权通过的调用。

判定矩阵逐条对齐 Go（`pkg/middleware/session.go:25-45`），每一行的方向都是设计决定：

| 情形 | 处置 | 为什么 |
|---|---|---|
| 无 metadata / 无证据头 | **放行** | 非客户端面请求（内部调用、DS 回调、dev 直连）。客户端面必带该头（jwt_authn 对全部玩家服务路由 require），缺头即非客户端面；`require` 档也不改这一行 —— 强行拒会切断全部内部调用 |
| 证据头存在但不可解 / player_id 缺失 | `ErrUnauthorized` | 证据残缺不可能来自正常网关 |
| gate 未装配 | `require` ? `ErrUnavailable` : 放行 | 强制档不允许在"无法判定现行性"的装配下对客户端面开门 |
| 权威查询失败 | `ErrUnavailable` | §9.22：禁止把"查不了"当"仍现行" |
| 无会话（登出 / 过期） | `ErrUnauthorized` | 客户端允许自动换新会话 |
| jti ≠ 当前一代 | `ErrSessionSuperseded` → **ABORTED** | 见下 |

`ErrSessionSuperseded` 刻意映射到 gRPC `ABORTED` 而不是 `UNAUTHENTICATED`：网关对
自然过期的 token 也产 `UNAUTHENTICATED`，客户端据此分不出"允许自动换新"还是"另一台
设备已登录"。被顶设备把顶号当过期去自动完整 Login，会**反顶**新设备形成互踢循环
（INC-20260722-004 R4 P0）。映射见 `errcode_grpc.py`。

**诚实边界**（Go 侧原话）：本门是"请求进入时"的现行性判定；判定通过后、业务写提交前
发生的会话轮换无法被跨存储（Redis 会话 vs MySQL 业务）原子拦截，残余窗口是毫秒级在途
请求，由 login 侧副作用终检进一步收窄。

⚠️ **push 是第 14 处，形态不同**：`Subscribe` 是 server stream，流式的会话门必须
在 service 层手写（Go：`service/push.go:66-104` + `biz/push.go:145-211`，含 30s 看门狗与
`sessionFailClose=3`；Python：`services/push/service.py` + `services/push/biz.py` 三道闸）。
迁 push 的人去找中间件会找不到 —— 那一处必须单独实现，不能指望这里的拦截器。

但“**unary 拦截器链对 stream 不生效**”这句 Go 侧的原话，**搬到 Python 就是错的**：
Go 的 `pmw.SessionCurrent` 是 Kratos unary middleware，只在 `unaryServerInterceptor` 里被
消费，stream 走独立的 `streamMiddleware`（全仓从未装配），所以对 stream 是**真** no-op
—— `push/internal/server/grpc.go:37` 才敢把它挂在只有 stream RPC 的 push server 上。
grpcio 不同：`ServerInterceptor.intercept_service` 对**每一个**方法都会被调用，stream
也不例外。本拦截器跳过流式靠的是 `intercept_service` 里那道**显式护栏**，而不是
框架不调用它；护栏删掉的后果不是“门失效”，而是合法会话的 Subscribe 当场 TypeError。
"""

from __future__ import annotations

import base64
import binascii
import dataclasses
import json
from typing import Protocol

import grpc

from pandorapy import errcode, errcode_grpc
from pandorapy import log as plog

# Envoy `forward_payload_header` 注入的验签后 payload。与 interceptors.METADATA_KEY_JWT_PAYLOAD
# 同名 —— 这里重新声明而不是 import，是为了让本模块不依赖拦截器模块的加载顺序；
# 两处不一致会让门恒放行，所以下面有一条测试把它们钉在一起。
METADATA_KEY_JWT_PAYLOAD = "x-pandora-jwt-payload"
METADATA_KEY_PLAYER_ID = "x-pandora-player-id"

# gRPC 基础设施方法：健康检查 / 反射 / channelz。探针不可能带会话证据，
# 挡住它们 = Pod 永不 Ready（§5.2.2 已经踩过一次同形状的坑）。
_INFRA_PREFIX = "/grpc."


@dataclasses.dataclass(frozen=True)
class SessionPayloadClaims:
    """从 Envoy 验签后 payload 头提取的会话声明子集。对应 Go 的 SessionPayloadClaims。"""

    jti: str = ""
    exp_ms: int = 0


def parse_jwt_payload_claims(payload: str) -> SessionPayloadClaims:
    """解析 base64url JSON，取 jti + exp。对应 Go 的 `ParseJWTPayloadClaims`。

    纯函数。**任何解码 / 结构异常都返回零值**，与"头缺失"同一语义 ——
    禁止向调用方暴露半解析结果（半个 claims 会让上层以为拿到了证据）。
    """
    if not payload:
        return SessionPayloadClaims()
    raw: bytes | None = None
    for decoder in (_b64url_nopad, _b64url_pad):
        try:
            raw = decoder(payload)
            break
        except (binascii.Error, ValueError):
            continue
    if raw is None:
        return SessionPayloadClaims()
    try:
        claims = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return SessionPayloadClaims()
    if not isinstance(claims, dict):
        return SessionPayloadClaims()
    jti = claims.get("jti")
    exp = claims.get("exp")
    return SessionPayloadClaims(
        jti=jti if isinstance(jti, str) else "",
        # JWT 的 exp 是**秒**级；Go 转成毫秒后再交给上层。
        exp_ms=int(exp) * 1000 if isinstance(exp, (int, float)) else 0,
    )


def _b64url_nopad(payload: str) -> bytes:
    """Envoy 输出无 padding（Go 用 base64.RawURLEncoding）。"""
    pad = "=" * (-len(payload) % 4)
    return base64.urlsafe_b64decode(payload + pad)


def _b64url_pad(payload: str) -> bytes:
    """兼容带 padding 的实现差异（Go 的第二次尝试）。"""
    return base64.urlsafe_b64decode(payload)


class Gate(Protocol):
    """查询玩家当前会话代际（jti）。对应 Go 的 sessiongate.Gate。"""

    async def current_jti(self, player_id: int) -> tuple[str, bool]:
        """返回 (jti, found)。found=False = 无会话（已登出 / 过期）。

        **权威不可达必须抛 `PandoraError(ErrUnavailable)`**，不得返回 (\"\", False) ——
        那会把"查不到"变成"确实没有会话"，两者对上层是相反的指令。
        """
        ...


class RedisGate:
    """基于共享 Redis 的 Gate。rdb 必须与 login 的会话权威指向同一 Redis。"""

    __slots__ = ("_rdb",)

    def __init__(self, rdb) -> None:  # noqa: ANN001
        self._rdb = rdb

    @staticmethod
    def session_key(player_id: int) -> str:
        """`pandora:sess:<player_id>`。key 格式与 login 是硬契约，改一个字符门就恒失效。"""
        return f"pandora:sess:{player_id}"

    async def current_jti(self, player_id: int) -> tuple[str, bool]:
        try:
            jti = await self._rdb.hget(self.session_key(player_id), "jti")
        except Exception as exc:  # noqa: BLE001
            # ★ 不可达 → 抛。返回"无会话"会让门把不确定当成结论（§9.22 明令禁止）。
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "session authority unavailable: %s", exc
            ) from exc
        if jti is None:
            return "", False
        text = jti.decode() if isinstance(jti, bytes) else str(jti)
        return text, text != ""


class SessionCurrentInterceptor(grpc.aio.ServerInterceptor):
    """会话现行性拦截器。对应 Go 的 `pmw.SessionCurrent(gate, require)`。

    与本仓其它 RPC 的错误约定**不同**：这里走 gRPC status（`context.abort`）而不是
    in-band `response.code`。这是刻意跟 Go 对齐的 —— 客户端要能用状态码本身区分
    ABORTED（被顶号，转交互登录）/ UNAUTHENTICATED（过期，可自动换新）/
    UNAVAILABLE（依赖故障，退避重连）；塞进 body 的话流式与 raw 路径读不到。
    """

    __slots__ = ("_gate", "_require")

    def __init__(self, gate: Gate | None, require: bool = False) -> None:
        self._gate = gate
        self._require = require

    async def intercept_service(self, continuation, handler_call_details):  # noqa: ANN001
        method = handler_call_details.method
        if method.startswith(_INFRA_PREFIX):
            return await continuation(handler_call_details)

        md = dict(handler_call_details.invocation_metadata or ())
        raw = md.get(METADATA_KEY_JWT_PAYLOAD, "")
        if not raw:
            # 非客户端面请求（内部调用 / DS 回调 / dev 直连）。require 档也放行 ——
            # 强行拒会切断全部东西向调用。
            return await continuation(handler_call_details)

        handler = await continuation(handler_call_details)
        if handler is None:
            return handler
        if handler.request_streaming or handler.response_streaming:
            # ★ 流式必须原样返回，不能往下走。下面用 `unary_unary_rpc_method_handler`
            # 重建 handler，会把 stream handler 的 cardinality **抹成 unary**：
            # `response_streaming` 从 True 变 False，而 `handler.unary_unary` 是 None
            # —— 于是**会话完全合法**的那条路径（`_passthrough`）当场
            # `TypeError: 'NoneType' object is not callable`，客户端只看到 UNKNOWN。
            # 注意方向：abort 分支反而正常工作，所以这道门在流式上是
            # “只会拒、不会放”：它精确地杀掉本该放行的合法会话，比“门失效”更难查。
            #
            # 为什么 Go 侧不需要这一句：Go 的 `pmw.SessionCurrent` 是 Kratos unary
            # middleware，由 `kgrpc.Middleware(...)` 装配，只在
            # `transport/grpc/interceptor.go` 的 `unaryServerInterceptor` 里被消费；
            # stream 走独立的 `s.streamMiddleware`（只有 `kgrpc.StreamMiddleware(...)`
            # 能填，全仓调用 0 次）。所以 Go 那边 middleware 对 stream 是真 no-op ——
            # `services/runtime/push/internal/server/grpc.go:37` **确实**把
            # SessionCurrent 挂在了只有 stream RPC 的 push server 上，那是无害摆设。
            # grpcio 不是这个语义：`ServerInterceptor.intercept_service` 对**每一个**
            # 方法都会被调用，包括 stream。任何一次“照 Go 对齐接线”的忠实迁移都会踩。
            #
            # `return handler` = 放行未过门的流，看着像洞，但精确等于 Go 现行语义：
            # 流式的会话门在 push service/biz 层手写（`services/push/service.py` 取 claims
            # → `biz.authorize_and_register` 建流门 + 30s 看门狗 + 逐帧 fence）。
            # 删掉本判断 = 用 TypeError 换掉那三道门，不是“多守一层”。
            return handler

        claims = parse_jwt_payload_claims(raw)
        player_id = _player_id_of(md)
        gate, require = self._gate, self._require

        async def deny(request, context: grpc.aio.ServicerContext):  # noqa: ANN001
            if not claims.jti or player_id == 0:
                # 头在但残缺：正常网关路径不可能出现，按证据不可信 fail-closed。
                return await errcode_grpc.abort_with(context, errcode.PandoraError(
                    errcode.ErrUnauthorized, "session payload malformed or player identity missing"
                ))
            if gate is None:
                if require:
                    return await errcode_grpc.abort_with(context, errcode.PandoraError(
                        errcode.ErrUnavailable,
                        "session authority not wired; request rejected (fail-closed)",
                    ))
                return await _passthrough(handler, request, context)
            try:
                cur, found = await gate.current_jti(player_id)
            except errcode.PandoraError as exc:
                # 权威不可达：fail-closed（§9.22 禁止把"查不了"当"仍现行"）。
                return await errcode_grpc.abort_with(context, exc)
            if not found:
                return await errcode_grpc.abort_with(context, errcode.PandoraError(
                    errcode.ErrUnauthorized, "session expired or logged out; login again"
                ))
            if cur != claims.jti:
                plog.get().warning(
                    "session_superseded_rejected", player_id=player_id, op=method.lstrip("/")
                )
                return await errcode_grpc.abort_with(context, errcode.PandoraError(
                    errcode.ErrSessionSuperseded, "session superseded by a newer login"
                ))
            return await _passthrough(handler, request, context)

        return grpc.unary_unary_rpc_method_handler(
            deny,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


async def _passthrough(handler, request, context):  # noqa: ANN001
    """放行到真正的 handler。兼容同步 servicer（grpcio 本体也是这么判的）。"""
    import inspect

    res = handler.unary_unary(request, context)
    if inspect.isawaitable(res):
        return await res
    return res


def _player_id_of(md: dict) -> int:
    raw = md.get(METADATA_KEY_PLAYER_ID, "")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def must_build(rdb, require: bool) -> Gate | None:  # noqa: ANN001
    """按已建好的 Redis 客户端构造 Gate。对应 Go 的 `sessiongate.MustBuild` 的后半。

    `rdb is None`（未配会话权威端点）时：

      - `require=True`  → **抛异常拒启**。prod 强制档漏配会话权威是部署错误，
        不允许服务在"无法判定会话现行性"的状态下对客户端面开门（fail-closed）。
      - `require=False` → 返回 `None`：dev 直连联调，拦截器对无证据请求本就放行。

    Go 那边还负责按 `node.redis_client` 建客户端并做启动期 Ping；Python 侧把建客户端
    留给调用方（各服务 `main.py` 已经要建自己的 Redis），这里只管**装配决策**这一半。
    """
    if rdb is None:
        if require:
            raise RuntimeError(
                "sessiongate.must_build: session authority redis endpoint required "
                "(set node.redis_client.host or addrs); require=True 拒绝在缺席时启动"
            )
        return None
    return RedisGate(rdb)
