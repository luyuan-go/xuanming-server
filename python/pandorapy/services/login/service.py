"""login gRPC service 层 —— 对应 Go 侧 `internal/service/login.go`。

三条纪律,逐条来自 Go 那份文件的头注释与实现:

  ① **业务失败不抛 gRPC 错误**。10 个 RPC 全部返回 `XxxResponse(code=ErrXxx)` 且
     gRPC status = OK,客户端只读 body 里的 code。改成 `context.abort()` 会让
     UE 客户端走到完全不同的错误分支(它按 code 分支做重试 / 弹窗 / 回登录页),
     而两边日志都不会报错 —— 这是迁移里最容易悄悄改掉的语义。

  ② **身份一律取鉴权上下文,请求体里的同名字段只当"选哪个"**。
       player_id  ← x-pandora-player-id      (Envoy 玩家态 provider 验签后注入)
       account_id ← x-pandora-account-id     (Envoy **账号态** provider 注入)
       sess jti   ← x-pandora-jwt-payload    (Envoy 验签后重写的 payload)
     三个头在客户端面 :8443 入站时被 Envoy 无条件剥离,不可伪造。
     EnterRole 的 `player_id` 是"进哪个角色"而**不是**"我是谁" —— biz 层按
     account_roles 台账回查归属。少了这一步,任何人拿自己的账号 token 填别人的
     player_id 就能进别人的号。

  ③ **交付终检(delivery fence)**。SelectRole / IssueDSTicket 在"预检通过"与
     "票据写出"之间存在 TOCTOU:这段时间里会话可能已被新登录轮换。Go 在响应写出
     前再复核一次现行性,失败则扣留票据(票据从未离开服务端 = 未取得)。
     顺序、次数都照搬 —— 少一次终检,被顶号的旧设备仍能拿到一张可用的进场票。

诚实边界(见文件末与 main.py 的 honest_gaps):
  · IssueDSTicket 的 ds_type=hub / battle 两支走**路由权威**而不是本地自签:
      - hub    → `LoginUsecase.resolve_hub_endpoint_from_match`(locator presence +
                 matchmaker 三态门 + Hub 分配链);
      - battle → `LoginUsecase.resolve_battle_endpoint`(player↔match roster 权威门)。
    绝不用"签一张票 + 空地址"顶替:hub 会让客户端拿到 allocator 没登记过的自签票
    (Hub DS 一律拒,表现是"登录成功但进不去"),battle 会退化成"谁报一个 match_id
    谁就能拿到那局的进场票"。
  · VerifyDSTicket 有 off/legacy 与 Redis admission 两支,由 main 的
    `ds_auth.authority_mode` **显式**决定(`set_redis_ds_admission_authority`),
    不按票据形态猜。admission 支的固定线性顺序是
    ① DS Bearer 验签 + pod scope → ② Redis active 权威 → ③ 票内 binding 比对 →
    ④ 原子 marker 消费,任何一步前移都会让"拿错 Pod 的票"消耗掉一次性资源。
"""

from __future__ import annotations

import asyncio

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.login.v1 import login_pb2, login_pb2_grpc

from pandorapy import errcode
from pandorapy import dsauth
from pandorapy import internalrpcauth
from pandorapy import interceptors
from pandorapy import log as plog
from pandorapy import sessiongate
from pandorapy.services.login import biz as lbiz
from pandorapy.services.login import dsticket as ldsticket

GRPC_SERVICE_FULL_NAME = "pandora.login.v1.LoginService"
INTERNAL_GRPC_SERVICE_FULL_NAME = "pandora.login.v1.LoginInternalService"
RESOLVE_PLAYER_NOS_METHOD = "/pandora.login.v1.LoginInternalService/ResolvePlayerNos"
MAX_RESOLVE_PLAYER_NOS = 32

# Envoy 账号态 provider 注入的账号身份头(Go: pkg/middleware.MetadataKeyAccountID)。
METADATA_KEY_ACCOUNT_ID = "x-pandora-account-id"
# Envoy 注入的受信客户端 IP(Go: service.clientIPFromHeader)。入站同名头被无条件
# 剥离,所以后端看到的值只可能出自 Envoy 自己。未经 Envoy 的直连为空 ——
# 登录失败 Quota 的 IP 维度自动关闭(**不是**回落到 socket peer:那在 Envoy 后面
# 恒等于网关 IP,会把整个集群的失败并成一个维度,一个人爆破锁死所有人)。
METADATA_KEY_CLIENT_IP = "x-pandora-client-ip"


def _md_get(context: grpc.aio.ServicerContext, key: str) -> str:
    """从 invocation_metadata 取一个头。取不到返回空串。

    与 pandorapy.interceptors._metadata_get 同形。刻意不复用那个私有函数:
    共享件本轮冻结,而这里多两个 login 专用的头(account_id / client_ip)。
    """
    md = context.invocation_metadata()
    if not md:
        return ""
    for entry in md:
        if entry[0].lower() == key:
            value = entry[1]
            return (value.decode() if isinstance(value, bytes) else str(value)).strip()
    return ""


def _internal_metadata_dict(context: grpc.aio.ServicerContext) -> dict[str, str]:
    """internalrpcauth 的单值 metadata 视图；重复键强制变成验签失败。"""
    out: dict[str, str] = {}
    duplicates: set[str] = set()
    for key, value in context.invocation_metadata() or ():
        if isinstance(value, bytes):
            continue
        normalized = key.lower()
        if normalized in out:
            duplicates.add(normalized)
        else:
            out[normalized] = str(value)
    for key in duplicates:
        out[key] = ""
    return out


def _caller_id(context: grpc.aio.ServicerContext) -> int:
    """玩家态身份。对应 Go 的 `ctx.Value(plog.CtxKeyPlayerID)`。"""
    return interceptors.extract_player_id(context)


def _account_id(context: grpc.aio.ServicerContext) -> int:
    """账号态身份。对应 Go 的 `middleware.AccountIDFromContext`。

    ★ 刻意**不**回退去读 x-pandora-player-id:那会让一张玩家 SessionToken 也能
    列角色 / 换角色,等于把「账号态」这层隔离整个拆掉。取不到就是 0 → 拒。
    """
    raw = _md_get(context, METADATA_KEY_ACCOUNT_ID)
    if not raw:
        return 0
    try:
        value = int(raw)
    except ValueError:
        return 0
    return value if value > 0 else 0


def _session_jti(context: grpc.aio.ServicerContext) -> str:
    """会话 JWT 的 jti。对应 Go 的 `middleware.SessionJTIFromContext`。

    取不到返回 ""(直连内网端口联调 / DS 面无 jwt_authn),由 biz 侧按
    require_hub_assignment_binding 决定 fail-closed 还是兼容放行 ——
    判定在 biz,这里只负责如实取值,不在传输层擅自兜底。
    """
    return sessiongate.parse_jwt_payload_claims(
        _md_get(context, sessiongate.METADATA_KEY_JWT_PAYLOAD)
    ).jti


def _client_ip(context: grpc.aio.ServicerContext) -> str:
    return _md_get(context, METADATA_KEY_CLIENT_IP)


def _roles_to_proto(views: list[lbiz.AccountRoleView]) -> list[login_pb2.AccountRole]:
    """biz 选角视图 → proto。对应 Go 的 accountRolesToProto。

    §5 第 11 条:由服务端按最小数据单位填充,不把存储行原样外抛。
    """
    return [
        login_pb2.AccountRole(
            player_id=v.player_id,
            role_name=v.role_name,
            role_id=v.role_id,
            player_no=v.player_no,
            level=v.level,
            last_login_at_ms=v.last_login_at_ms,
            slot=v.slot,
        )
        for v in views
    ]


def _resume_to_proto(r: lbiz.ResumeContextResult) -> login_pb2.ResumeContext:
    """对应 Go 的 resumeContextToProto。

    ★ owner_epoch / retry_after_ms / entry_state / wait_reason 四项必须带上:
    R11 复审「架构 P0」正是因为它们被 biz 取回后静默丢弃,客户端拿不到 §9.23 要求的
    (state, exact target, owner_epoch) 三元组,幂等 no-op 无从判定。
    """
    return login_pb2.ResumeContext(
        route=r.route,
        match_id=r.match_id,
        match_stage=r.match_stage,
        game_mode=r.game_mode,
        map_id=r.map_id,
        placement_state=r.placement_state,
        operation_id=r.operation_id,
        ds_pod_name=r.ds_pod_name,
        ds_instance_uid=r.ds_instance_uid,
        ds_instance_epoch=r.ds_instance_epoch,
        hub_assignment_id=r.hub_assignment_id,
        allocation_id=r.allocation_id,
        release_track=r.release_track,
        owner_epoch=r.owner_epoch,
        retry_after_ms=r.retry_after_ms,
        entry_state=r.entry_state,
        wait_reason=r.wait_reason,
    )


def _claims_to_proto(c: ldsticket.DSTicketClaims) -> login_pb2.DSTicket:
    """已验签的票据 claims → proto。对应 Go 的 VerifyDSTicket 返回体。

    ★ `ds_instance_epoch` / `allocation_id` / `release_track` 是 v2 票的 exact 实例绑定
      与 §9.21 轨道粘滞三项:漏填,DS 侧拿到的 claims 里它们恒零/恒空,两道门在 DS 上
      被整体跳过,而两边日志全绿。
    """
    return login_pb2.DSTicket(
        player_id=c.player_id,
        match_id=c.match_id,
        issued_at_ms=c.issued_at_ms,
        expires_at_ms=c.expires_at_ms,
        ds_type=c.ds_type,
        jti=c.jti,
        region_id=c.region_id,
        cell_id=c.cell_id,
        role_id=c.role_id,
        ds_pod_name=c.ds_pod_name,
        ds_instance_uid=c.ds_instance_uid,
        ds_protocol_epoch=c.ds_protocol_epoch,
        ds_credential_gen=c.ds_credential_gen,
        ds_credential_jti=c.ds_credential_jti,
        hub_assignment_id=c.hub_assignment_id,
        ds_writer_epoch=c.ds_writer_epoch,
        dst_ver=c.version,
        ds_instance_epoch=c.ds_instance_epoch,
        allocation_id=c.allocation_id,
        release_track=c.release_track,
        source_match_id=c.source_match_id,
    )


class LoginInternalService(login_pb2_grpc.LoginInternalServiceServicer):
    """只供受 internalrpcauth 保护的集群内调用方读取展示编号。"""

    __slots__ = ("_reader", "_verifier")

    def __init__(self, reader, verifier: internalrpcauth.Verifier | None) -> None:  # noqa: ANN001
        self._reader = reader
        self._verifier = verifier

    async def ResolvePlayerNos(  # noqa: N802
        self,
        request: login_pb2.ResolvePlayerNosRequest,
        context: grpc.aio.ServicerContext,
    ) -> login_pb2.ResolvePlayerNosResponse:
        raw_ids = [int(player_id) for player_id in request.player_ids]
        if (
            not raw_ids
            or len(raw_ids) > MAX_RESOLVE_PLAYER_NOS
            or any(player_id <= 0 for player_id in raw_ids)
        ):
            return login_pb2.ResolvePlayerNosResponse(
                code=errcode_pb2.ERR_INVALID_ARG
            )
        player_ids = sorted(set(raw_ids))
        if len(player_ids) > MAX_RESOLVE_PLAYER_NOS:
            return login_pb2.ResolvePlayerNosResponse(
                code=errcode_pb2.ERR_INVALID_ARG
            )

        # 鉴权绑定规范化后的确定性 protobuf。subject 取排序后的首 ID 只用于提供非零
        # 追踪锚；整批不可篡改性来自 payload digest，不是 subject。
        canonical_request = login_pb2.ResolvePlayerNosRequest(player_ids=player_ids)
        payload = canonical_request.SerializeToString(deterministic=True)
        if self._verifier is None:
            plog.get().warning(
                "resolve_player_nos_auth_unavailable", member_count=len(player_ids)
            )
            return login_pb2.ResolvePlayerNosResponse(
                code=errcode_pb2.ERR_UNAVAILABLE
            )
        try:
            await self._verifier.verify_with_payload(
                _internal_metadata_dict(context),
                RESOLVE_PLAYER_NOS_METHOD,
                player_ids[0],
                payload,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            code = errcode_pb2.ERR_PERMISSION_DENY
            if isinstance(exc, internalrpcauth.ErrUnavailable):
                code = errcode_pb2.ERR_UNAVAILABLE
            plog.get().warning(
                "resolve_player_nos_auth_rejected",
                subject=player_ids[0],
                member_count=len(player_ids),
                code=int(code),
                err=str(exc),
            )
            return login_pb2.ResolvePlayerNosResponse(code=code)

        try:
            values = await self._reader.resolve_player_nos(player_ids)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return login_pb2.ResolvePlayerNosResponse(code=errcode.as_code(exc))
        return login_pb2.ResolvePlayerNosResponse(
            code=errcode_pb2.OK,
            entries=[
                login_pb2.ResolvedPlayerNo(
                    player_id=player_id, player_no=int(values.get(player_id, 0))
                )
                for player_id in player_ids
            ],
        )


class LoginService(login_pb2_grpc.LoginServiceServicer):
    """实现 LoginServiceServicer。对应 Go 的 `service.LoginService`。"""

    __slots__ = (
        "_login",
        "_ticket",
        "_redis_ds_admission",
        "_ds_guard",
        "_admission_checker",
    )

    def __init__(self, login_uc: lbiz.LoginUsecase, ticket_uc: lbiz.TicketUsecase) -> None:
        self._login = login_uc
        self._ticket = ticket_uc
        # 与 VerifyDSTicket 的 Go 契约相同：legacy/off 默认保留本机直连；只有 main
        # 显式启用 Redis admission 后，DS-only 读取才进入 Bearer + active 权威分支。
        self._redis_ds_admission = False
        self._ds_guard = None
        self._admission_checker = None

    def set_redis_ds_admission_authority(self, guard, checker) -> None:  # noqa: ANN001
        """启用 DS Bearer credential + Redis active 权威门。"""
        self._redis_ds_admission = True
        self._ds_guard = guard
        self._admission_checker = checker

    # ── 账号面 ───────────────────────────────────────────────────────────

    async def Login(  # noqa: N802 —— gRPC 生成的方法名,必须照抄
        self, request: login_pb2.LoginRequest, context: grpc.aio.ServicerContext
    ) -> login_pb2.LoginResponse:
        try:
            res = await self._login.login(
                request.account,
                request.password_hash,
                request.device_id,
                request.defer_role_entry,
                client_ip=_client_ip(context),
            )
        except Exception as exc:  # noqa: BLE001 —— 与 Go 一样把 error 映射成 code
            return login_pb2.LoginResponse(code=errcode.as_code(exc))
        return login_pb2.LoginResponse(
            code=errcode_pb2.OK,
            player_id=res.player_id,
            session_token=res.session_token,
            hub_ds_addr=res.hub_ds_addr,
            hub_ticket=res.hub_ticket,
            region_id=res.region_id,
            cell_id=res.cell_id,
            # 账号层(两步登录):defer_role_entry=true 时只有这几项非空。
            account_token=res.account_token,
            account_id=res.account_id,
            account_token_exp_ms=res.account_token_exp_ms,
            roles=_roles_to_proto(res.roles),
            # 断线重连:命中时非空,客户端直连 battle DS;未命中为零值,走 hub。
            battle_ds_addr=res.battle_ds_addr,
            battle_ticket=res.battle_ticket,
            match_id=res.match_id,
            selected_role_id=res.selected_role_id,
            resume_context=_resume_to_proto(res.resume),
            # ★ register_no(#13,旧客户端/JSON)与 player_no(#14,新客户端)必须
            # **双写同值**:旧调用方排空前收缩任何一个,对应客户端上编号直接变 0
            # (显示「生成中」永不结束),而服务端零错误。
            register_no=res.player_no,
            player_no=res.player_no,
        )

    async def ListAccountRoles(  # noqa: N802
        self,
        request: login_pb2.ListAccountRolesRequest,
        context: grpc.aio.ServicerContext,
    ) -> login_pb2.ListAccountRolesResponse:
        del request  # 请求体为空:身份只能来自账号态 JWT,不接受自报
        try:
            roles = await self._login.list_account_roles(_account_id(context))
        except Exception as exc:  # noqa: BLE001
            return login_pb2.ListAccountRolesResponse(code=errcode.as_code(exc))
        return login_pb2.ListAccountRolesResponse(
            code=errcode_pb2.OK, roles=_roles_to_proto(roles)
        )

    async def EnterRole(  # noqa: N802
        self, request: login_pb2.EnterRoleRequest, context: grpc.aio.ServicerContext
    ) -> login_pb2.EnterRoleResponse:
        try:
            res = await self._login.enter_role(
                _account_id(context),
                request.player_id,
                request.device_id,
                client_ip=_client_ip(context),
            )
        except Exception as exc:  # noqa: BLE001
            return login_pb2.EnterRoleResponse(code=errcode.as_code(exc))
        return login_pb2.EnterRoleResponse(
            code=errcode_pb2.OK,
            player_id=res.player_id,
            session_token=res.session_token,
            hub_ds_addr=res.hub_ds_addr,
            hub_ticket=res.hub_ticket,
            region_id=res.region_id,
            cell_id=res.cell_id,
            battle_ds_addr=res.battle_ds_addr,
            battle_ticket=res.battle_ticket,
            match_id=res.match_id,
            selected_role_id=res.selected_role_id,
            resume_context=_resume_to_proto(res.resume),
            player_no=res.player_no,
        )

    async def Logout(  # noqa: N802
        self, request: login_pb2.LogoutRequest, context: grpc.aio.ServicerContext
    ) -> login_pb2.LogoutResponse:
        del context
        try:
            await self._login.logout(request.session_token)
        except Exception as exc:  # noqa: BLE001
            return login_pb2.LogoutResponse(code=errcode.as_code(exc))
        return login_pb2.LogoutResponse(code=errcode_pb2.OK)

    # ── 选角 / 编号 ──────────────────────────────────────────────────────

    async def SelectRole(  # noqa: N802
        self, request: login_pb2.SelectRoleRequest, context: grpc.aio.ServicerContext
    ) -> login_pb2.SelectRoleResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            plog.get().warning("select_role_no_player_id")
            return login_pb2.SelectRoleResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        caller_jti = _session_jti(context)
        # 会话现行性门(2026-07-18):封"顶号后旧设备仍可 SelectRole 拿 hub 票"。
        # 必须在任何业务写之前 —— 权威缺失 / 过期 / 读失败都要零签票副作用地返回。
        try:
            await self._login.require_current_session_jti(player_id, caller_jti)
        except Exception as exc:  # noqa: BLE001
            return login_pb2.SelectRoleResponse(code=errcode.as_code(exc))
        try:
            addr, ticket, _ = await self._login.select_role(
                player_id, request.role_id, caller_jti
            )
        except Exception as exc:  # noqa: BLE001
            return login_pb2.SelectRoleResponse(code=errcode.as_code(exc))
        # 交付终检(R5 复审 P0-5):预检通过后、角色落库 + 签票期间会话可能已被新登录
        # 轮换。失败则扣留票据(票据从未离开服务端 = 未取得)。
        # 诚实边界与 Go 同:角色行**已落库**且跨 Redis/MySQL 无法原子回卷,残余仅是
        # 一次可被覆盖的选角写,不构成进场能力。
        try:
            await self._login.require_current_session_jti(player_id, _session_jti(context))
        except Exception as exc:  # noqa: BLE001
            plog.get().warning("select_role_delivery_fenced", player_id=player_id)
            return login_pb2.SelectRoleResponse(code=errcode.as_code(exc))
        return login_pb2.SelectRoleResponse(
            code=errcode_pb2.OK, hub_ds_addr=addr, hub_ticket=ticket
        )

    async def GetPlayerNo(  # noqa: N802
        self, request: login_pb2.GetPlayerNoRequest, context: grpc.aio.ServicerContext
    ) -> login_pb2.GetPlayerNoResponse:
        del request  # 请求体为空:只能查自己
        player_id = _caller_id(context)
        if player_id == 0:
            # ⚠️ 该 path 必须列在 envoy.yaml 的 jwt_authn rules 里:未列到的 path
            # 默认放行不验签,上游拿不到 x-pandora-player-id,这里会一律拒。
            plog.get().warning("get_player_no_no_player_id")
            return login_pb2.GetPlayerNoResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        try:
            no = await self._login.get_player_no(player_id)
        except Exception as exc:  # noqa: BLE001
            return login_pb2.GetPlayerNoResponse(code=errcode.as_code(exc))
        # code=OK 且 player_no=0 表示「仍在补号窗口内」,不是错误。
        return login_pb2.GetPlayerNoResponse(code=errcode_pb2.OK, player_no=no)

    async def GetRegisterNo(  # noqa: N802
        self, request: login_pb2.GetRegisterNoRequest, context: grpc.aio.ServicerContext
    ) -> login_pb2.GetRegisterNoResponse:
        """已发布客户端的兼容入口;新调用方必须用 GetPlayerNo。

        删除前须先证明旧客户端已排空 —— 不能在滚动升级窗口内原地收缩 RPC。
        实现委托给 GetPlayerNo(与 Go 逐字同):两条路径的判定永远不会分叉。
        """
        del request
        res = await self.GetPlayerNo(login_pb2.GetPlayerNoRequest(), context)
        return login_pb2.GetRegisterNoResponse(
            code=res.code, register_no=res.player_no
        )

    async def ResolvePlayerNosForDS(  # noqa: N802
        self,
        request: login_pb2.ResolvePlayerNosRequest,
        context: grpc.aio.ServicerContext,
    ) -> login_pb2.ResolvePlayerNosResponse:
        """供 DS listener 批量读取角色展示编号；身份键始终是 player_id。"""
        raw_ids = [int(player_id) for player_id in request.player_ids]
        if (
            not raw_ids
            or len(raw_ids) > MAX_RESOLVE_PLAYER_NOS
            or any(player_id <= 0 for player_id in raw_ids)
        ):
            return login_pb2.ResolvePlayerNosResponse(
                code=errcode_pb2.ERR_INVALID_ARG
            )
        player_ids = sorted(set(raw_ids))

        if self._redis_ds_admission:
            if (
                self._ds_guard is None
                or self._admission_checker is None
                or self._ds_guard.mode is not dsauth.Mode.ENFORCE
            ):
                plog.get().error(
                    "resolve_player_nos_for_ds_rejected",
                    reason="ds_admission_guard_not_enforced",
                )
                return login_pb2.ResolvePlayerNosResponse(
                    code=errcode_pb2.ERR_UNAVAILABLE
                )
            _, credential, guard_code = self._ds_guard.check_credential(
                context, dsauth.DSScope(require_token=True)
            )
            if guard_code != 0:
                return login_pb2.ResolvePlayerNosResponse(code=guard_code)
            if credential is None or not credential.pod:
                return login_pb2.ResolvePlayerNosResponse(
                    code=errcode_pb2.ERR_UNAUTHORIZED
                )
            try:
                admission = await self._admission_checker.check_active(
                    credential.pod, credential
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                return login_pb2.ResolvePlayerNosResponse(code=errcode.as_code(exc))
            if credential.ds_type == "battle":
                admitted = set(admission.player_ids)
                if any(player_id not in admitted for player_id in player_ids):
                    return login_pb2.ResolvePlayerNosResponse(
                        code=errcode_pb2.ERR_PERMISSION_DENY
                    )

        if self._login is None:
            return login_pb2.ResolvePlayerNosResponse(
                code=errcode_pb2.ERR_UNAVAILABLE
            )
        try:
            values = await self._login.resolve_player_nos(player_ids)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return login_pb2.ResolvePlayerNosResponse(code=errcode.as_code(exc))
        return login_pb2.ResolvePlayerNosResponse(
            code=errcode_pb2.OK,
            entries=[
                login_pb2.ResolvedPlayerNo(
                    player_id=player_id, player_no=int(values.get(player_id, 0))
                )
                for player_id in player_ids
            ],
        )

    async def GetResumeContext(  # noqa: N802
        self,
        request: login_pb2.GetResumeContextRequest,
        context: grpc.aio.ServicerContext,
    ) -> login_pb2.GetResumeContextResponse:
        del context
        try:
            out = await self._login.get_resume_context(request.session_token)
        except Exception as exc:  # noqa: BLE001
            return login_pb2.GetResumeContextResponse(code=errcode.as_code(exc))
        return login_pb2.GetResumeContextResponse(
            code=errcode_pb2.OK, context=_resume_to_proto(out)
        )

    # ── DS 票据 ──────────────────────────────────────────────────────────

    async def IssueDSTicket(  # noqa: N802
        self,
        request: login_pb2.IssueDSTicketRequest,
        context: grpc.aio.ServicerContext,
    ) -> login_pb2.IssueDSTicketResponse:
        player_id = _caller_id(context)
        if player_id == 0:
            plog.get().warning("ds_ticket_issue_no_player_id")
            return login_pb2.IssueDSTicketResponse(code=errcode_pb2.ERR_UNAUTHORIZED)
        # session 现行性门(P0 修复 2026-07-15):JWT 验签只证明"曾登录过",顶号后
        # 旧 token 在 exp 前仍验得过。这里确认 token 是**当前一代**,
        # 防止旧设备继续给自己签 hub/battle 票造成双在场。
        try:
            await self._login.require_current_session_token(
                player_id, request.session_token
            )
        except Exception as exc:  # noqa: BLE001
            return login_pb2.IssueDSTicketResponse(code=errcode.as_code(exc))

        ds_type = request.ds_type
        caller_jti = _session_jti(context)

        # ★ 交付终检(Go 的 fenceTicketDelivery,三条分支共用)。
        # 预检通过后、分配 / 签票期间会话可能已被新登录轮换(检查与副作用之间的 TOCTOU)。
        # 票已签但从未离开服务端 = 旧在途请求未取得可用票据。
        async def _fence_delivery() -> Exception | None:
            try:
                await self._login.require_current_session_token(
                    player_id, request.session_token
                )
            except Exception as exc:  # noqa: BLE001
                plog.get().warning(
                    "ds_ticket_delivery_fenced", player_id=player_id, ds_type=ds_type
                )
                return exc
            return None

        if ds_type == ldsticket.DS_TYPE_HUB:
            # target_id 历史上携带来源 match;**现在仅作日志参考** —— 路由权威是
            # locator 租约 + match 三态门(biz._guard_hub_route_against_active_battle)。
            # 信客户端报的那个 = 让客户端自己声明"我这局打完了",直接绕开双在场门。
            try:
                addr, ticket, _ = await self._login.resolve_hub_endpoint_from_match(
                    player_id, request.target_id, caller_jti
                )
            except Exception as exc:  # noqa: BLE001
                return login_pb2.IssueDSTicketResponse(code=errcode.as_code(exc))
            fenced = await _fence_delivery()
            if fenced is not None:
                return login_pb2.IssueDSTicketResponse(code=errcode.as_code(fenced))
            return login_pb2.IssueDSTicketResponse(
                code=errcode_pb2.OK, ticket=ticket, hub_ds_addr=addr
            )

        if ds_type == ldsticket.DS_TYPE_BATTLE:
            # 地址与票都只来自 roster 权威门(biz.resolve_battle_endpoint):
            # 调用方给的 match_id 只是"要哪一局"的选择,不是成员资格证明。
            # ★ 与 Go 同:battle 分支**不回 addr** —— 客户端此刻已连着那台 DS
            # (重连场景),回地址只会给它一个可被旧值覆盖的机会。
            try:
                _addr, ticket, _ = await self._login.resolve_battle_endpoint(
                    player_id, request.target_id, caller_jti
                )
            except Exception as exc:  # noqa: BLE001
                return login_pb2.IssueDSTicketResponse(code=errcode.as_code(exc))
            fenced = await _fence_delivery()
            if fenced is not None:
                return login_pb2.IssueDSTicketResponse(code=errcode.as_code(fenced))
            return login_pb2.IssueDSTicketResponse(code=errcode_pb2.OK, ticket=ticket)

        try:
            ticket, _ = await self._ticket.issue_ds_ticket(
                player_id, ds_type, request.target_id, caller_jti
            )
        except Exception as exc:  # noqa: BLE001
            return login_pb2.IssueDSTicketResponse(code=errcode.as_code(exc))
        fenced = await _fence_delivery()
        if fenced is not None:
            return login_pb2.IssueDSTicketResponse(code=errcode.as_code(fenced))
        return login_pb2.IssueDSTicketResponse(code=errcode_pb2.OK, ticket=ticket)

    async def VerifyDSTicket(  # noqa: N802
        self,
        request: login_pb2.VerifyDSTicketRequest,
        context: grpc.aio.ServicerContext,
    ) -> login_pb2.VerifyDSTicketResponse:
        """票据兑换点。两档:off/legacy 直验;Redis authority 先过 DS 权威门。

        ★ Redis 档的四步顺序是**契约**,不能重排:
          ① DS Bearer 验签 + 请求 pod scope → ② Redis active 权威 →
          ③ 玩家票 claims 与 caller binding 精确比对 → ④ 原子 MarkUsedByAdmission。
        把 ④ 提前 = 一次绑定不符的重放就把合法票的 jti 烧掉,玩家再也进不去。
        ③④ 在 `TicketUsecase.verify_ds_ticket_for_admission` 内部完成。
        """
        log = plog.get()
        if self._redis_ds_admission:
            # 这些是**DS 调用方级**拒绝:一台 DS 的凭据 / active 权威漂移会让它上面
            # **所有玩家**的核销整体失败。而 ErrUnauthorized 不算 server fault →
            # access log 只记 rpc_ok(DEBUG),toProtoCode 又只回 code 不回 message,
            # 不在这里显式打,拒绝原因在任何日志级别都不存在。
            if not request.ds_pod_name:
                # ds_pod_name 是 Guard 的范围输入;空值**不能**退化成"不校验 pod"。
                log.warning("verify_ds_ticket_rejected", reason="ds_pod_name_empty")
                return login_pb2.VerifyDSTicketResponse(code=errcode_pb2.ERR_INVALID_ARG)
            if self._ds_guard is None or self._admission_checker is None:
                log.error(
                    "verify_ds_ticket_rejected",
                    reason="ds_admission_guard_not_wired",
                    ds_pod=request.ds_pod_name,
                )
                return login_pb2.VerifyDSTicketResponse(code=errcode_pb2.ERR_UNAVAILABLE)

            _, credential, guard_code = self._ds_guard.check_credential(
                context, dsauth.DSScope(pod=request.ds_pod_name, require_token=True)
            )
            if guard_code != 0:
                log.warning(
                    "verify_ds_ticket_rejected",
                    reason="ds_credential_rejected",
                    ds_pod=request.ds_pod_name,
                    code=int(guard_code),
                    hint=(
                        "DS callback credential 验签/scope 失败:该 DS 上所有玩家的"
                        "核销都会同型失败,查凭据轮换"
                    ),
                )
                return login_pb2.VerifyDSTicketResponse(code=guard_code)
            if credential is None:
                log.warning(
                    "verify_ds_ticket_rejected",
                    reason="ds_credential_missing",
                    ds_pod=request.ds_pod_name,
                )
                return login_pb2.VerifyDSTicketResponse(code=errcode_pb2.ERR_UNAUTHORIZED)

            try:
                admission = await self._admission_checker.check_active(
                    request.ds_pod_name, credential
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                log.warning(
                    "verify_ds_ticket_rejected",
                    reason="ds_admission_not_active",
                    ds_pod=request.ds_pod_name,
                    admission_id=request.admission_id,
                    err=str(exc),
                    hint=(
                        "DS credential 与 Redis active 权威漂移(轮换半途/心跳超时/"
                        "投影翻转):该 DS 上所有玩家 travel 会被拒"
                    ),
                )
                return login_pb2.VerifyDSTicketResponse(code=errcode.as_code(exc))

            try:
                claims = await self._ticket.verify_ds_ticket_for_admission(
                    request.ticket,
                    request.ds_pod_name,
                    request.admission_id,
                    admission,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                return login_pb2.VerifyDSTicketResponse(code=errcode.as_code(exc))
        else:
            # off/legacy 完整保留既有内部 Verify 语义与单次 JTI SETNX。
            try:
                claims = await self._ticket.verify_ds_ticket(
                    request.ticket, request.ds_pod_name
                )
            except Exception as exc:  # noqa: BLE001
                return login_pb2.VerifyDSTicketResponse(code=errcode.as_code(exc))

        # 兑换点会话复核在 TicketUsecase 内部完成(顺序是契约:验签 → 会话门 →
        # jti 消费;会话门若放到 jti 消费之后,被顶设备的一次重放会把合法票的 jti
        # 先烧掉,新设备再也进不去)。
        return login_pb2.VerifyDSTicketResponse(
            code=errcode_pb2.OK, claims=_claims_to_proto(claims)
        )
