"""会话现行性门的判定矩阵 —— 逐行对齐 Go 的 `pkg/middleware/session.go:25-45`。

矩阵里**每一行的方向都是设计决定**，写反了都不报错：

  - 无证据头 → 放行（不是拒！内部调用 / DS 回调 / dev 直连都不带该头，
    拒了会切断全部东西向调用；客户端面必带，缺头即非客户端面）
  - 权威查不到 → `UNAVAILABLE` 而**不是**当成"无会话"（§9.22 禁止把不确定当结论）
  - 顶号 → `ABORTED` 而**不是** `UNAUTHENTICATED`（后者与自然过期无法区分，
    被顶设备会自动 Login 反顶新设备形成互踢循环，INC-20260722-004 R4 P0）
  - `require=True` 且 gate 未装配 → 拒（fail-closed）

还有一条不在矩阵里但同样致命：**基础设施方法必须放行**。探针不可能带会话证据，
挡住 `/grpc.health.v1.Health/Check` 就是 Pod 永不 Ready —— §5.2.2 已经踩过同形状的坑。
"""

from __future__ import annotations

import base64
import json

import grpc
import pytest

from pandorapy import errcode, errcode_grpc, interceptors, sessiongate


# ── payload 解析 ─────────────────────────────────────────────────────────

def _payload(**claims) -> str:
    raw = json.dumps(claims).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def test_metadata_key_matches_the_interceptor_module() -> None:
    """两处各写一份常量，不一致会让门**恒放行**且没有任何信号。"""
    assert sessiongate.METADATA_KEY_JWT_PAYLOAD == interceptors.METADATA_KEY_JWT_PAYLOAD


def test_parses_jti_and_converts_exp_to_millis() -> None:
    c = sessiongate.parse_jwt_payload_claims(_payload(jti="abc", exp=1700000000))
    assert c.jti == "abc"
    assert c.exp_ms == 1700000000 * 1000, "JWT 的 exp 是秒级，Go 侧转成毫秒后才交上层"


def test_padded_base64_is_accepted() -> None:
    """Envoy 输出无 padding，但带 padding 的实现也要能解（Go 的第二次尝试）。"""
    raw = json.dumps({"jti": "x"}).encode()
    assert sessiongate.parse_jwt_payload_claims(base64.urlsafe_b64encode(raw).decode()).jti == "x"


@pytest.mark.parametrize("bad", ["", "!!!not-base64!!!", _payload(), "e30"])
def test_malformed_payload_yields_zero_value_not_half_parsed(bad: str) -> None:
    """任何异常都返回零值，与"头缺失"同一语义 —— 半个 claims 会让上层以为拿到了证据。"""
    c = sessiongate.parse_jwt_payload_claims(bad)
    assert c.jti == "" and c.exp_ms == 0


def test_non_string_jti_is_rejected() -> None:
    assert sessiongate.parse_jwt_payload_claims(_payload(jti=123)).jti == ""


# ── Gate ────────────────────────────────────────────────────────────────

class _FakeRedis:
    def __init__(self, value=None, raise_exc=None) -> None:
        self.value = value
        self.raise_exc = raise_exc
        self.keys: list[str] = []

    async def hget(self, key, field):  # noqa: ANN001
        self.keys.append(key)
        if self.raise_exc:
            raise self.raise_exc
        return self.value


async def test_session_key_format_is_the_login_contract() -> None:
    """key 格式与 login 是硬契约，改一个字符门就恒失效（永远查不到会话）。"""
    assert sessiongate.RedisGate.session_key(10086) == "pandora:sess:10086"


async def test_gate_returns_not_found_for_missing_session() -> None:
    jti, found = await sessiongate.RedisGate(_FakeRedis(None)).current_jti(1)
    assert jti == "" and found is False


async def test_gate_raises_unavailable_when_authority_is_down() -> None:
    """★ 不可达必须抛，不能返回"无会话" —— 那两者对上层是相反的指令。"""
    gate = sessiongate.RedisGate(_FakeRedis(raise_exc=RuntimeError("conn refused")))
    with pytest.raises(errcode.PandoraError) as exc:
        await gate.current_jti(1)
    assert exc.value.code == errcode.ErrUnavailable


# ── 拦截器矩阵 ───────────────────────────────────────────────────────────

class _FakeCall:
    def __init__(self, method: str, metadata: dict) -> None:
        self.method = method
        self.invocation_metadata = tuple(metadata.items())


class _FakeContext:
    def __init__(self) -> None:
        self.aborted: tuple | None = None

    async def abort(self, code, details):  # noqa: ANN001
        self.aborted = (code, details)
        raise grpc.RpcError(details)


class _StubGate:
    def __init__(self, jti: str | None, exc: Exception | None = None) -> None:
        self.jti, self.exc = jti, exc

    async def current_jti(self, player_id: int):
        if self.exc:
            raise self.exc
        if self.jti is None:
            return "", False
        return self.jti, True


async def _invoke(interceptor, *, method="/pandora.team.v1.TeamService/GetTeam",
                  metadata=None, handler_ran=None):
    called = {"ran": False}

    async def real_handler(request, context):  # noqa: ANN001
        called["ran"] = True
        return "OK"

    async def continuation(details):  # noqa: ANN001
        return grpc.unary_unary_rpc_method_handler(real_handler)

    h = await interceptor.intercept_service(continuation, _FakeCall(method, metadata or {}))
    ctx = _FakeContext()
    try:
        result = await h.unary_unary(None, ctx)
    except grpc.RpcError:
        result = None
    return result, ctx, called["ran"]


_GOOD_MD = {
    sessiongate.METADATA_KEY_JWT_PAYLOAD: _payload(jti="cur", exp=1),
    sessiongate.METADATA_KEY_PLAYER_ID: "1001",
}


async def test_infrastructure_methods_bypass_the_gate() -> None:
    """★ 探针不可能带会话证据。挡住它 = Pod 永不 Ready，而现象只是"探测失败"。"""
    i = sessiongate.SessionCurrentInterceptor(_StubGate("other"), require=True)
    _r, ctx, ran = await _invoke(i, method="/grpc.health.v1.Health/Check", metadata={})
    assert ran and ctx.aborted is None


async def test_no_evidence_header_passes_through_even_when_required() -> None:
    """★ 无证据 = 非客户端面（内部调用 / DS 回调）。拒了会切断全部东西向调用。"""
    i = sessiongate.SessionCurrentInterceptor(_StubGate("cur"), require=True)
    _r, ctx, ran = await _invoke(i, metadata={})
    assert ran and ctx.aborted is None


async def test_current_session_passes() -> None:
    i = sessiongate.SessionCurrentInterceptor(_StubGate("cur"))
    r, ctx, ran = await _invoke(i, metadata=_GOOD_MD)
    assert ran and r == "OK" and ctx.aborted is None


async def test_superseded_session_is_aborted_not_unauthenticated() -> None:
    """★ 顶号必须回 ABORTED。

    回 UNAUTHENTICATED 的话客户端分不出"过期"与"被顶"，会自动 Login 反顶新设备，
    两台设备互踢（INC-20260722-004 R4 P0）。
    """
    i = sessiongate.SessionCurrentInterceptor(_StubGate("newer-jti"))
    _r, ctx, ran = await _invoke(i, metadata=_GOOD_MD)
    assert not ran, "顶号的旧会话被放进业务 handler 了"
    assert ctx.aborted[0] == grpc.StatusCode.ABORTED
    assert errcode_grpc.grpc_code(errcode.ErrSessionSuperseded) == grpc.StatusCode.ABORTED


async def test_expired_or_logged_out_is_unauthenticated() -> None:
    """自然过期允许客户端自动换新会话，所以与顶号必须可判别。"""
    i = sessiongate.SessionCurrentInterceptor(_StubGate(None))
    _r, ctx, ran = await _invoke(i, metadata=_GOOD_MD)
    assert not ran and ctx.aborted[0] == grpc.StatusCode.UNAUTHENTICATED


async def test_authority_unavailable_is_fail_closed() -> None:
    """★ §9.22：禁止把"查不了"当"仍现行"。"""
    exc = errcode.PandoraError(errcode.ErrUnavailable, "redis down")
    i = sessiongate.SessionCurrentInterceptor(_StubGate("cur", exc=exc))
    _r, ctx, ran = await _invoke(i, metadata=_GOOD_MD)
    assert not ran, "权威查不到却把请求放进去了"
    assert ctx.aborted[0] == grpc.StatusCode.UNAVAILABLE


async def test_malformed_evidence_is_rejected() -> None:
    """头在但残缺：正常网关路径不可能出现，按证据不可信 fail-closed。"""
    md = {sessiongate.METADATA_KEY_JWT_PAYLOAD: "garbage",
          sessiongate.METADATA_KEY_PLAYER_ID: "1001"}
    i = sessiongate.SessionCurrentInterceptor(_StubGate("cur"))
    _r, ctx, ran = await _invoke(i, metadata=md)
    assert not ran and ctx.aborted[0] == grpc.StatusCode.UNAUTHENTICATED


async def test_missing_player_id_is_rejected() -> None:
    md = {sessiongate.METADATA_KEY_JWT_PAYLOAD: _payload(jti="cur")}
    i = sessiongate.SessionCurrentInterceptor(_StubGate("cur"))
    _r, ctx, ran = await _invoke(i, metadata=md)
    assert not ran and ctx.aborted[0] == grpc.StatusCode.UNAUTHENTICATED


async def test_unwired_gate_passes_when_optional_but_fails_closed_when_required() -> None:
    optional = sessiongate.SessionCurrentInterceptor(None, require=False)
    _r, ctx, ran = await _invoke(optional, metadata=_GOOD_MD)
    assert ran and ctx.aborted is None

    required = sessiongate.SessionCurrentInterceptor(None, require=True)
    _r, ctx2, ran2 = await _invoke(required, metadata=_GOOD_MD)
    assert not ran2 and ctx2.aborted[0] == grpc.StatusCode.UNAVAILABLE


# ── 流式护栏 ──────────────────────────────────────────

async def _intercept_stream(interceptor, *, kind: str, metadata=None,
                            method="/pandora.push.v1.PushService/Subscribe"):
    """把一个**真流式** handler 喂给拦截器，返回（原 handler, 拦截器返回的 handler）。"""

    async def server_stream(request, context):  # noqa: ANN001
        yield "frame"

    async def client_stream(request_iterator, context):  # noqa: ANN001
        return "OK"

    real = (grpc.unary_stream_rpc_method_handler(server_stream) if kind == "server"
            else grpc.stream_unary_rpc_method_handler(client_stream))

    async def continuation(details):  # noqa: ANN001
        return real

    got = await interceptor.intercept_service(continuation, _FakeCall(method, metadata or {}))
    return real, got


async def test_server_stream_handler_is_returned_untouched() -> None:
    """★ 流式必须原样返回，不能被重包成 unary。

    没有这道护栏时，`unary_unary_rpc_method_handler` 会把 stream handler 的
    cardinality 抹平：`response_streaming` True → False、`unary_stream` 丢失、
    `unary_unary` 为 None。grpcio 的 `ServerInterceptor.intercept_service` 对**每一个**
    方法都会被调用（不像 Kratos 的 unary middleware 对 stream 是真 no-op），
    所以这里不能靠“框架不会调用我”偷懒。
    """
    i = sessiongate.SessionCurrentInterceptor(_StubGate("cur"), require=True)
    real, got = await _intercept_stream(i, kind="server", metadata=_GOOD_MD)
    assert got is real, "流式 handler 被拦截器换掉了"
    assert got.response_streaming is True
    assert got.unary_unary is None


async def test_client_stream_handler_is_returned_untouched() -> None:
    """`request_streaming` 一侧单独钉一道：只判 `response_streaming` 会漏掉 client stream。"""
    i = sessiongate.SessionCurrentInterceptor(_StubGate("cur"), require=True)
    real, got = await _intercept_stream(i, kind="client", metadata=_GOOD_MD)
    assert got is real
    assert got.request_streaming is True
    assert got.unary_unary is None


async def test_legal_session_stream_still_serves_frames() -> None:
    """★ 失败方向是反的，写清楚以免下一个人又搞反：

    abort 分支（证据残缺 / 权威不可达 / 无会话 / 被顶）在流式上反而能跑；
    真正炸的是**会话完全合法**的放行路径 —— `_passthrough` 去取
    `handler.unary_unary`，而重包后它是 None，当场
    `TypeError: 'NoneType' object is not callable`，客户端只看到 UNKNOWN。
    即这道门在流式上是“只会拒、不会放”：精确地杀掉本该放行的合法会话。
    """
    i = sessiongate.SessionCurrentInterceptor(_StubGate("cur"), require=True)
    _real, got = await _intercept_stream(i, kind="server", metadata=_GOOD_MD)
    frames = [f async for f in got.unary_stream(None, _FakeContext())]
    assert frames == ["frame"]


# ── 装配决策 ─────────────────────────────────────────────────────────────

def test_must_build_refuses_to_start_without_authority_when_required() -> None:
    """prod 强制档漏配会话权威是部署错误，必须拒启而不是静默降级成"永不强制"。"""
    with pytest.raises(RuntimeError, match="session authority"):
        sessiongate.must_build(None, require=True)


def test_must_build_returns_none_in_dev() -> None:
    assert sessiongate.must_build(None, require=False) is None
