"""mail gRPC service 层 —— 对应 Go 侧 internal/service/mail.go。

鉴权边界(两类 RPC,方向相反,**不能统一**):

    玩家 RPC(ListMail / ReadMail / ClaimMail / DeleteMail)
        player_id 一律取鉴权上下文(R5:override 请求体字段,那些字段号已在 proto 里
        reserved)。caller_id == 0 → ERR_UNAUTHORIZED。

    系统 RPC(SendSystemMail / SendGuildMail / SendPersonalMail /
             GetClaimableAttachments / MarkMailClaimed)
        合法调用者是**内网直连**的后端(运营工具、battle_result 背包满转邮件、
        owner DS 领取链),它们不经 Envoy 因而没有 JWT 注入 → caller_id == 0。
        反过来:caller_id > 0 说明请求来自客户端 → 一律 ERR_PERMISSION_DENY。
        Envoy 侧对这几个 path 另有 403,这里是服务层兜底(双保险)——
        少了它,一旦网关路由配错,玩家就能自助给自己发带附件的邮件。

⚠️ 返回值形态必须和 Go 侧一致:业务失败**不**抛 gRPC 错误,而是返回
   `Response(code=ErrXxx)` 且 gRPC status 为 OK。客户端读的是 body 里的 code。
   若改成 context.abort(),客户端会走到完全不同的错误分支 —— 这是最容易在迁移中
   悄悄改掉的语义。ClaimMail 更进一步:**失败时也带 attachments**
   (已领过的那封要把"你领过这些"回给客户端),丢了它客户端只能显示一个错误码。
"""

from __future__ import annotations

import time

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.mail.v1 import mail_pb2, mail_pb2_grpc

from pandorapy import errcode, interceptors


def now_ms() -> int:
    """当前毫秒。对应 Go 的 nowMs()。"""
    return int(time.time() * 1000)


class MailService(mail_pb2_grpc.MailServiceServicer):
    """实现 MailServiceServicer。对应 Go 的 service.MailService。"""

    __slots__ = ("_uc", "_snowflake")

    def __init__(self, usecase, snowflake) -> None:  # noqa: ANN001
        self._uc = usecase
        # snowflake 只需要 generate() —— 用最小接口而不是具体类型,
        # 与 Go 侧 snowflakeGen interface 同样的意图(测试可注入假发号器)。
        self._snowflake = snowflake

    # ── 玩家面 ───────────────────────────────────────────────────────────

    async def ListMail(  # noqa: N802 —— gRPC 生成的方法名,必须照抄
        self, request: mail_pb2.ListMailRequest, context: grpc.aio.ServicerContext
    ) -> mail_pb2.ListMailResponse:
        pid = _caller_id(context)
        if pid == 0:
            return mail_pb2.ListMailResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            mails, next_cursor = await self._uc.list_mail(
                pid, now_ms(), request.cursor, request.limit
            )
        except Exception as exc:  # noqa: BLE001 —— 与 Go 侧一样把 error 映射成 code
            return mail_pb2.ListMailResponse(code=errcode.as_code(exc))
        return mail_pb2.ListMailResponse(
            code=errcode_pb2.OK, mails=mails, next_cursor=next_cursor
        )

    async def ReadMail(  # noqa: N802
        self, request: mail_pb2.ReadMailRequest, context: grpc.aio.ServicerContext
    ) -> mail_pb2.ReadMailResponse:
        pid = _caller_id(context)
        if pid == 0:
            return mail_pb2.ReadMailResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            await self._uc.read_mail(pid, request.mail_id)
        except Exception as exc:  # noqa: BLE001
            return mail_pb2.ReadMailResponse(code=errcode.as_code(exc))
        return mail_pb2.ReadMailResponse(code=errcode_pb2.OK)

    async def ClaimMail(  # noqa: N802
        self, request: mail_pb2.ClaimMailRequest, context: grpc.aio.ServicerContext
    ) -> mail_pb2.ClaimMailResponse:
        pid = _caller_id(context)
        if pid == 0:
            return mail_pb2.ClaimMailResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            atts = await self._uc.claim_mail(pid, request.mail_id, now_ms())
        except Exception as exc:  # noqa: BLE001
            # ★ 失败也回附件:Go 侧 ClaimMail 在"已领过"这条路径上返回 (atts, err),
            # 客户端据此显示"你领过这些"。biz 把它挂在异常上(见 _already_claimed),
            # 这里必须取出来,否则那份信息静默丢失。
            return mail_pb2.ClaimMailResponse(
                code=errcode.as_code(exc), attachments=getattr(exc, "attachments", None) or []
            )
        return mail_pb2.ClaimMailResponse(code=errcode_pb2.OK, attachments=atts)

    async def DeleteMail(  # noqa: N802
        self, request: mail_pb2.DeleteMailRequest, context: grpc.aio.ServicerContext
    ) -> mail_pb2.DeleteMailResponse:
        pid = _caller_id(context)
        if pid == 0:
            return mail_pb2.DeleteMailResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            await self._uc.delete_mail(pid, request.mail_id)
        except Exception as exc:  # noqa: BLE001
            return mail_pb2.DeleteMailResponse(code=errcode.as_code(exc))
        return mail_pb2.DeleteMailResponse(code=errcode_pb2.OK)

    # ── 运营 / 内网发送(系统接口)────────────────────────────────────────

    async def SendSystemMail(  # noqa: N802
        self, request: mail_pb2.SendSystemMailRequest, context: grpc.aio.ServicerContext
    ) -> mail_pb2.SendSystemMailResponse:
        code = _system_only(context)
        if code != errcode_pb2.OK:
            return mail_pb2.SendSystemMailResponse(code=code)
        try:
            mail_id = await self._uc.send_system_mail(
                self._snowflake.generate(),
                request.title,
                request.body,
                request.attachments,
                request.start_ms,
                request.end_ms,
                now_ms(),
            )
        except Exception as exc:  # noqa: BLE001
            return mail_pb2.SendSystemMailResponse(code=errcode.as_code(exc))
        return mail_pb2.SendSystemMailResponse(code=errcode_pb2.OK, mail_id=mail_id)

    async def SendGuildMail(  # noqa: N802
        self, request: mail_pb2.SendGuildMailRequest, context: grpc.aio.ServicerContext
    ) -> mail_pb2.SendGuildMailResponse:
        code = _system_only(context)
        if code != errcode_pb2.OK:
            return mail_pb2.SendGuildMailResponse(code=code)
        try:
            mail_id = await self._uc.send_guild_mail(
                self._snowflake.generate(),
                request.guild_id,
                request.title,
                request.body,
                request.attachments,
                request.start_ms,
                request.end_ms,
                now_ms(),
            )
        except Exception as exc:  # noqa: BLE001
            return mail_pb2.SendGuildMailResponse(code=errcode.as_code(exc))
        return mail_pb2.SendGuildMailResponse(code=errcode_pb2.OK, mail_id=mail_id)

    async def SendPersonalMail(  # noqa: N802
        self, request: mail_pb2.SendPersonalMailRequest, context: grpc.aio.ServicerContext
    ) -> mail_pb2.SendPersonalMailResponse:
        code = _system_only(context)
        if code != errcode_pb2.OK:
            return mail_pb2.SendPersonalMailResponse(code=code)
        try:
            mail_id = await self._uc.send_personal_mail(
                self._snowflake.generate(),
                request.to_player_id,
                request.title,
                request.body,
                request.attachments,
                request.expire_ms,
                now_ms(),
                request.instance_grant_key,
            )
        except Exception as exc:  # noqa: BLE001
            return mail_pb2.SendPersonalMailResponse(code=errcode.as_code(exc))
        return mail_pb2.SendPersonalMailResponse(code=errcode_pb2.OK, mail_id=mail_id)

    # ── DS 三段式领取(系统接口:owner DS 内网直连)───────────────────────

    async def GetClaimableAttachments(  # noqa: N802
        self,
        request: mail_pb2.GetClaimableAttachmentsRequest,
        context: grpc.aio.ServicerContext,
    ) -> mail_pb2.GetClaimableAttachmentsResponse:
        code = _system_only(context)
        if code != errcode_pb2.OK:
            return mail_pb2.GetClaimableAttachmentsResponse(code=code)
        # ★ 这里的 player_id 取**请求体**而不是鉴权上下文:调用方是 DS,
        #   它代玩家发起,自己没有玩家身份(_system_only 已保证它不是客户端)。
        if request.player_id == 0 or request.mail_id == 0:
            return mail_pb2.GetClaimableAttachmentsResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            items, claim_key, already = await self._uc.get_claimable_attachments(
                request.player_id, request.mail_id, now_ms()
            )
        except Exception as exc:  # noqa: BLE001
            return mail_pb2.GetClaimableAttachmentsResponse(code=errcode.as_code(exc))
        return mail_pb2.GetClaimableAttachmentsResponse(
            code=errcode_pb2.OK, items=items, claim_key=claim_key, already_claimed=already
        )

    async def MarkMailClaimed(  # noqa: N802
        self, request: mail_pb2.MarkMailClaimedRequest, context: grpc.aio.ServicerContext
    ) -> mail_pb2.MarkMailClaimedResponse:
        code = _system_only(context)
        if code != errcode_pb2.OK:
            return mail_pb2.MarkMailClaimedResponse(code=code)
        if request.player_id == 0 or request.mail_id == 0:
            return mail_pb2.MarkMailClaimedResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            await self._uc.mark_mail_claimed(request.player_id, request.mail_id)
        except Exception as exc:  # noqa: BLE001
            return mail_pb2.MarkMailClaimedResponse(code=errcode.as_code(exc))
        return mail_pb2.MarkMailClaimedResponse(code=errcode_pb2.OK)


# ── 辅助 ──────────────────────────────────────────────────────────────────────


def _caller_id(context: grpc.aio.ServicerContext) -> int:
    """取鉴权上下文里的 player_id。对应 Go 的 callerID(ctx)。

    直接从 metadata 取而不是读 contextvars:身份判定走同一个真源(Envoy 注入的头)
    更不容易出错 —— 万一拦截器没挂上,这里会拿到 0 并 fail-closed,
    而不是拿到上一个请求残留的值。
    """
    return interceptors.extract_player_id(context)


def _system_only(context: grpc.aio.ServicerContext) -> int:
    """系统接口鉴权:经 Envoy 的客户端(caller_id > 0)一律拒。对应 Go 的 systemOnly。"""
    if _caller_id(context) != 0:
        return errcode_pb2.ERR_PERMISSION_DENY
    return errcode_pb2.OK
