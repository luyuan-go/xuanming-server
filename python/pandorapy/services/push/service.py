"""push RPC 入口层 —— 对应 Go 侧 internal/service/push.go。

唯一的 RPC `Subscribe` 是 **server stream**,形态与本仓其余 20 个服务都不同:

★ **grpcio 的 unary 拦截器对它一律不生效。**
  `pandorapy.server.build_grpc_server` 挂的那条链(Trace / 可观测 / 关停 /
  超时 / 鉴权)以及 `sessiongate.SessionCurrentInterceptor`,内部返回的都是
  `grpc.unary_unary_rpc_method_handler` —— 流式 RPC 根本不经过它们。
  所以下面这四件事必须在本文件**手写**,不能指望中间件:

    ① panic 兜底      —— 不写的话一次 latent 异常会怎样?grpc.aio 会把它变成
                          UNKNOWN 关掉这一条流,进程不崩(比 Go 好),但客户端拿到
                          UNKNOWN 无法区分"该换会话"还是"该退避重连"→ 重连风暴。
    ② 关停开关        —— 不写的话 killswitch 规则里写 Subscribe 完全不生效,
                          而"出事时才用"的能力失效恰好只在出事时才被发现。
    ③ player_id 提取  —— 不写的话恒为 0,kafka 定向推送无法路由到本 stream
                          (Go 侧 2026-06-08 正是踩了这一条)。
    ④ 会话现行性门    —— 见 biz.py 头注释的三道闸。

★ 错误一律经 `errcode_grpc` 映射成标准 gRPC status,**不走 in-band code**。
  这一处与本仓「业务失败返回 Response(code=...)、status 恒 OK」的通则**相反**,
  是刻意与 Go 对齐的:server stream 没有 response body 可放 code,
  客户端只能靠状态码区分 ABORTED(被顶号,转交互登录)/ UNAUTHENTICATED(过期,
  可自动换新)/ UNAVAILABLE(依赖故障,退避重连)。原样抛 PandoraError 的话
  客户端一律收到 UNKNOWN —— 过期会话每秒重连形成风暴。
"""

from __future__ import annotations

import asyncio

from pandora.push.v1 import push_pb2_grpc

from pandorapy import errcode, errcode_grpc, interceptors, killswitch
from pandorapy import log as plog
from pandorapy import safego, sessiongate
from pandorapy.services.push import biz as pbiz

# 关停开关查询用的 operation 名。
#
# 为什么写成常量而不是从 context 取:grpc.aio 的 `ServicerContext` **没有**
# 公开的"当前 full method"接口(Go 那边 `transport.FromServerContext(ctx).Operation()`
# 是有的)。push 只有这一个 RPC,写死是唯一能真正生效的做法;
# 从私有属性里挖会在 grpcio 升级时静默失效 —— 而"关停失效"只在出事时才被发现。
SUBSCRIBE_OPERATION = "/pandora.push.v1.PushService/Subscribe"


class PushService(push_pb2_grpc.PushServiceServicer):
    """实现 pushv1.PushServiceServicer。"""

    __slots__ = ("_uc",)

    def __init__(self, uc: pbiz.PushUsecase) -> None:
        self._uc = uc

    async def Subscribe(self, request, context):  # noqa: N802, ANN001
        """客户端长连接订阅(server stream)。

        流程:
          1. 关停开关(手写,见模块头注释②)
          2. 从 metadata 取 Envoy 注入的 player_id(手写,③)
          3. 会话现行性门 + 注册连接(同玩家锁内原子,④)
          4. 跑写者循环:补推 → 唤醒/轮询拉取投递
          5. 退出时反注册
        """
        logger = plog.get()
        player_id = 0
        token = None
        try:
            # ── ② 关停开关 ────────────────────────────────────────────────
            disabled, reason = killswitch.disabled(SUBSCRIBE_OPERATION)
            if disabled:
                return await errcode_grpc.abort_with(
                    context, killswitch.disabled_error(reason)
                )

            # ── ③ player_id:Envoy jwt_authn 注入的 x-pandora-player-id 头 ──
            # 0 = 匿名(直连 :20014 联调时正常;生产必经网关,恒非 0)。
            player_id = interceptors.extract_player_id(context)
            token = plog.bind_player_id(player_id) if player_id > 0 else None

            # ── ④ 会话身份:Envoy 验签后重写的 payload 头(入站无条件剥离,
            #      客户端无法伪造)。解析失败等同"没带",由 require 档裁决。
            claims = sessiongate.parse_jwt_payload_claims(
                _metadata_get(context, sessiongate.METADATA_KEY_JWT_PAYLOAD)
            )
            sess = pbiz.SessionInfo(jti=claims.jti, exp_ms=claims.exp_ms)

            try:
                # 校验与注册必须**原子** —— 分离执行的 TOCTOU 会让旧会话反过来
                # 顶掉新设备连接(biz.authorize_and_register 头注释)。
                slot = await self._uc.authorize_and_register(
                    player_id, sess, context.write
                )
            except errcode.PandoraError as exc:
                logger.warning(
                    "push_subscribe_rejected", player_id=player_id, err=str(exc)
                )
                return await errcode_grpc.abort_with(context, exc)

            logger.info(
                "push_stream_open",
                player_id=player_id,
                last_seen_ms=request.last_seen_ms,
                online_total=self._uc.conns.size(),
            )
            try:
                err = await self._uc.run_subscribe_stream(
                    slot, player_id, request.last_seen_ms, sess
                )
            finally:
                # 反注册必须在 finally:漏掉的话被顶号/异常退出的流会永久留在索引里,
                # 该玩家的唤醒信号从此发给一条死流 —— 真连接只能等 30s 兜底轮询。
                self._uc.conns.unregister(player_id, slot)
            if err is None:
                return None
            if isinstance(err, errcode.PandoraError):
                return await errcode_grpc.abort_with(context, err)
            # 非业务错误(write 失败等):流本身通常已经断了,状态码给 UNAVAILABLE
            # 让客户端退避重连,而不是留一个 UNKNOWN 让它立刻重拨。
            return await errcode_grpc.abort_with(
                context,
                errcode.PandoraError(errcode.ErrUnavailable, "push stream aborted: %s", err),
            )
        except asyncio.CancelledError:
            # ★ 必须在任何宽 except 之前放行。grpc.aio **正是用取消**终止在途
            # handler(客户端断连 / 停机排空);吞掉它 = 把正常断连映射成业务错误,
            # 而且流不会真的结束,排空永远等不到。
            raise
        except BaseException as exc:  # noqa: BLE001 —— ① panic 兜底
            if safego.recovered("push_subscribe", exc):
                return await errcode_grpc.abort_with(
                    context,
                    errcode.PandoraError(errcode.ErrInternal, "subscribe stream panic"),
                )
            raise
        finally:
            if token is not None:
                # contextvars 的 Token 必须由它自己的 var reset(与拦截器同法)。
                token.var.reset(token)


def _metadata_get(context, key: str) -> str:  # noqa: ANN001
    """从 invocation_metadata 取一个值(grpcio 给的是 (key, value) 元组序列)。"""
    for entry in context.invocation_metadata() or ():
        if entry[0].lower() == key:
            value = entry[1]
            return value.decode() if isinstance(value, bytes) else str(value)
    return ""
