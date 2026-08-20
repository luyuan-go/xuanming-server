"""login 的 10 个 REST 端点 —— 对应 Go 侧 `internal/server/http.go` 里那一行
`loginv1.RegisterLoginServiceHTTPServer(srv, svc)`(Kratos protoc-gen-go-http 生成)。

login 是**全仓唯一**带 `google.api.http` 注解的服务;其余 20 个服务的 HTTP 面只有
/metrics + /healthz。路径与方法逐个抄自 proto 注解,不是自己编的:

    POST /v1/login                Login
    POST /v1/logout               Logout
    POST /v1/ds/ticket/issue      IssueDSTicket
    POST /v1/ds/ticket/verify     VerifyDSTicket
    POST /v1/player-no/get        GetPlayerNo
    POST /v1/register-no/get      GetRegisterNo      (旧客户端兼容入口)
    POST /v1/account/roles/list   ListAccountRoles
    POST /v1/role/enter           EnterRole
    POST /v1/role/select          SelectRole
    POST /v1/resume/context       GetResumeContext

★ JSON 口径必须与 Kratos 逐字一致,否则客户端解不出字段而两边都不报错:
    出参 protojson + `EmitUnpopulated: true`   → always_print_fields_with_no_presence=True
    出参字段名 **lowerCamelCase**(Kratos 没开 UseProtoNames)→ preserving_proto_field_name=False
    入参 `DiscardUnknown: true`                → ignore_unknown_fields=True
  三项任意一项不同的后果:
    - 不 EmitUnpopulated:`code: 0`(OK)会整个字段消失,客户端把"没有 code"
      当成解析失败或未知错误 —— 而**成功**响应恰好是最常见的那一种。
    - 用 snake_case:`session_token` vs `sessionToken`,客户端拿到空 token。
    - 不 DiscardUnknown:客户端多带一个新字段,老服务端直接 400,滚动升级期全挂。

★ int64 在 protojson 里是**字符串**(两栈同规则,不是 Python 特有)。
  客户端已按此解析,别"顺手"改成数字。

★ 这一层**不做鉴权判定**。身份仍然来自 Envoy 注入的三个头(见 service.py 头注释),
  这里只是把 HTTP header 原样搬进一个 metadata 形状交给 servicer ——
  判定只有一处真源,HTTP 与 gRPC 两条入口不会分叉出两套结论。
"""

from __future__ import annotations

from typing import Any, Callable

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from google.protobuf import json_format
from google.protobuf.message import Message
from pandora.login.v1 import login_pb2

from pandorapy.services.login import service as lsvc

# 只把身份相关的受信头搬进 metadata。刻意是**白名单**而不是"全搬":
# 全搬会把客户端自带的任意头(包括伪造的 x-pandora-player-id —— HTTP 面若未经
# Envoy 剥离就直达)一并送进鉴权判定。白名单让"新增一个可信头"必须是一次显式改动。
_FORWARDED_HEADERS = (
    "x-pandora-player-id",
    lsvc.METADATA_KEY_ACCOUNT_ID,
    lsvc.METADATA_KEY_CLIENT_IP,
    "x-pandora-jwt-payload",
    "x-pandora-trace-id",
)


class _HTTPContext:
    """把 HTTP 请求头伪装成 grpc.aio.ServicerContext 的最小面。

    servicer 只用到 `invocation_metadata()`(身份三件套 + trace id)。
    刻意做成最小面而不是一个"什么都有"的假 context:
    多给一个方法,servicer 里哪天新用了它就会在 HTTP 面上走出与 gRPC 面**不同**的
    行为,而两边测试都绿。少给,则第一次用到时当场 AttributeError。
    """

    __slots__ = ("_md",)

    def __init__(self, headers) -> None:  # noqa: ANN001 —— starlette Headers
        self._md = tuple(
            (key, headers[key]) for key in _FORWARDED_HEADERS if key in headers
        )

    def invocation_metadata(self):  # noqa: ANN201
        return self._md


def _to_json(msg: Message) -> dict[str, Any]:
    return json_format.MessageToDict(
        msg,
        always_print_fields_with_no_presence=True,  # Kratos EmitUnpopulated
        preserving_proto_field_name=False,  # Kratos 未开 UseProtoNames → camelCase
    )


async def _read_request(request: Request, req_msg: Message) -> Message | None:
    """把 body 解析进 req_msg。解析失败返回 None(调用方回 400)。

    空 body 视作空消息:`GetPlayerNoRequest` 之类没有字段的请求,客户端常常
    干脆不发 body。Kratos 侧同样接受(protojson 解 "{}" 与解空是一回事)。
    """
    raw = await request.body()
    text = raw.decode("utf-8").strip() if raw else ""
    if not text:
        return req_msg
    try:
        json_format.Parse(text, req_msg, ignore_unknown_fields=True)  # Kratos DiscardUnknown
    except (json_format.ParseError, UnicodeDecodeError, ValueError):
        return None
    return req_msg


def _bad_request(detail: str) -> Response:
    """对应 Kratos 的编解码错误响应(HTTP 400)。

    刻意**不**把它伪装成 `{"code": ERR_INVALID_ARG}` 的业务响应:body 都没解开,
    服务端根本没执行任何业务判定。伪装成业务码会让客户端把"我发的 JSON 坏了"
    当成"服务端拒绝了我的请求",排查方向完全相反。
    """
    return JSONResponse(
        status_code=400,
        content={"code": 400, "reason": "CODEC", "message": detail},
    )


def _bind(
    app: FastAPI,
    path: str,
    request_type: Callable[[], Message],
    handler,  # noqa: ANN001 —— servicer 的 async 方法
    name: str,
) -> None:
    async def endpoint(request: Request) -> Response:
        req = await _read_request(request, request_type())
        if req is None:
            return _bad_request(f"invalid json body for {name}")
        resp = await handler(req, _HTTPContext(request.headers))
        # ★ 业务失败在 body 的 code 里,HTTP 状态恒 200 —— 与 gRPC 面
        # 「业务失败也回 status OK」同一条纪律。改成按 code 映射 HTTP 状态码
        # 会让客户端的 HTTP 错误分支(重试 / 断线提示)在纯业务拒绝时被触发。
        return JSONResponse(status_code=200, content=_to_json(resp))

    endpoint.__name__ = f"http_{name}"
    app.add_api_route(path, endpoint, methods=["POST"], name=name)


def register(app: FastAPI, svc: lsvc.LoginService) -> None:
    """把 10 个 REST 路由挂到 build_http_app 返回的 app 上。

    不设这一层会怎样:Go 版 login 的 :21001 上有这 10 个路由,Python 版没有。
    运营工具 / 联调脚本 / 任何走 REST 的调用方在灰度到 Python 副本时收到 404,
    而 gRPC 面一切正常 —— 表现为"时好时坏",取决于打到哪个副本。
    """
    _bind(app, "/v1/login", login_pb2.LoginRequest, svc.Login, "Login")
    _bind(app, "/v1/logout", login_pb2.LogoutRequest, svc.Logout, "Logout")
    _bind(
        app,
        "/v1/ds/ticket/issue",
        login_pb2.IssueDSTicketRequest,
        svc.IssueDSTicket,
        "IssueDSTicket",
    )
    _bind(
        app,
        "/v1/ds/ticket/verify",
        login_pb2.VerifyDSTicketRequest,
        svc.VerifyDSTicket,
        "VerifyDSTicket",
    )
    _bind(
        app,
        "/v1/player-no/get",
        login_pb2.GetPlayerNoRequest,
        svc.GetPlayerNo,
        "GetPlayerNo",
    )
    _bind(
        app,
        "/v1/register-no/get",
        login_pb2.GetRegisterNoRequest,
        svc.GetRegisterNo,
        "GetRegisterNo",
    )
    _bind(
        app,
        "/v1/account/roles/list",
        login_pb2.ListAccountRolesRequest,
        svc.ListAccountRoles,
        "ListAccountRoles",
    )
    _bind(app, "/v1/role/enter", login_pb2.EnterRoleRequest, svc.EnterRole, "EnterRole")
    _bind(
        app, "/v1/role/select", login_pb2.SelectRoleRequest, svc.SelectRole, "SelectRole"
    )
    _bind(
        app,
        "/v1/resume/context",
        login_pb2.GetResumeContextRequest,
        svc.GetResumeContext,
        "GetResumeContext",
    )
