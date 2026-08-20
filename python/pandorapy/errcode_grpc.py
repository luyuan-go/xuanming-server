"""业务错误码 → gRPC 标准状态码。对应 Go 侧 `pkg/errcode/grpc.go`。

单独一个模块而不是并进 `errcode.py`：那份是**生成物**（`tools/gen_errcode.py` 从 Go 源码
产出），手写内容进去下次重跑就没了（§7.4）。Go 侧同样是 `errcode` 包里单独一个 `grpc.go`。

**为什么需要显式转换**：`PandoraError` 不带 gRPC 状态，raw / stream 路径
（如 push 的 `Subscribe`，不经 unary 拦截器链）把它原样抛出去，客户端一律收到
`UNKNOWN` —— UE 无法区分"会话已失效（换新 session 才能恢复）"与"依赖故障
（退避重连即可）"，过期会话每秒重连形成风暴。所以流 / raw 路径的最终返回值必须过一次转换。

刻意做成**显式助手**而不是给 `PandoraError` 加一个自动生效的钩子：后者会静默改变全部
服务 unary 路径的线上错误形态，影响面无法逐点验证（§15.2 最小复杂度）。
"""

from __future__ import annotations

import grpc

from pandorapy import errcode

# 公共错误码段 → gRPC 标准码。业务段（≥1000）不逐一映射，归入 UNKNOWN 并保留
# "errcode=<n>" 消息文本供客户端解析业务语义 —— 与 Go 的 default 分支一致。
_CODE_MAP: dict[int, grpc.StatusCode] = {
    errcode.OK: grpc.StatusCode.OK,
    errcode.ErrTimeout: grpc.StatusCode.DEADLINE_EXCEEDED,
    errcode.ErrInvalidArg: grpc.StatusCode.INVALID_ARGUMENT,
    errcode.ErrNotFound: grpc.StatusCode.NOT_FOUND,
    errcode.ErrAlreadyExists: grpc.StatusCode.ALREADY_EXISTS,
    errcode.ErrPermissionDeny: grpc.StatusCode.PERMISSION_DENIED,
    errcode.ErrUnauthorized: grpc.StatusCode.UNAUTHENTICATED,
    # ★ 顶号刻意**不**映射 UNAUTHENTICATED。
    # 网关(Envoy jwt_authn)对自然过期 token 也产 UNAUTHENTICATED，客户端据此
    # 分不出"允许自动换新"还是"另一设备已登录"。被顶设备把顶号当过期去自动完整
    # Login，会反顶新设备形成**互踢循环**(INC-20260722-004 R4 P0)。
    # ABORTED = "被并发的更新登录中止"，本工程没有别的码映射到它、网关也不会产生，
    # 所以客户端可以用它唯一判别顶号并转交互式登录。
    errcode.ErrSessionSuperseded: grpc.StatusCode.ABORTED,
    errcode.ErrRateLimited: grpc.StatusCode.RESOURCE_EXHAUSTED,
    errcode.ErrUnavailable: grpc.StatusCode.UNAVAILABLE,
    errcode.ErrServiceDisabled: grpc.StatusCode.UNAVAILABLE,
    errcode.ErrCanceled: grpc.StatusCode.CANCELLED,
}


def grpc_code(code: int) -> grpc.StatusCode:
    """对应 Go 的 `errcode.GRPCCode`。未映射的一律 UNKNOWN，不伪造语义。"""
    return _CODE_MAP.get(code, grpc.StatusCode.UNKNOWN)


async def abort_with(context: grpc.aio.ServicerContext, exc: errcode.PandoraError) -> None:
    """用标准 gRPC 状态中止本次调用。对应 Go 的 `ToGRPCError` + 直接返回。

    消息保留 `errcode=<n> <msg>` 原文，客户端解析业务语义时仍能拿到具体码。
    """
    await context.abort(grpc_code(exc.code), str(exc))
