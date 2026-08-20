"""matchmaker gRPC service 层 —— 对应 Go 侧 internal/service/{match,configtable_admin}.go。

职责只有三件,业务判定一律下沉 biz(第二份判定就是漂移的来源):
  - 从鉴权上下文取 player_id(**忽略请求体里的 player_id**,R5)
  - proto Request/Response ↔ biz 入参/出参互转
  - errcode.Code → commonv1.ErrCode 1:1 映射(数值相同)

★ 6 个 RPC 全"已受理型"(协议原则 3):业务失败一律返回 `Response(code=...)` 且
  gRPC status 保持 **OK**,与 Go 逐字一致 —— 调用方按 code 分支,不靠 gRPC status 猜。
  Go 侧本服务没有任何一个 RPC 走 gRPC status,所以这里也没有例外。

★ 三种方向不同的身份闸,逐个对着 Go 抄,**不许统一成一种**:

    客户端面(StartMatch / ConfirmMatch / GetMatchProgress)
        caller==0 → ERR_UNAUTHORIZED。Envoy jwt_authn 已在路由层 require JWT,
        这里是兜底:少了它,内网任何 Pod 都能以任意身份开局。

    双向面(CancelMatch)
        caller!=0 走客户端路径(只能取消自己的排队,请求体 player_id 被忽略);
        caller==0 是**内部路径**(team 离队/踢人联动撤票),按 req.player_id 取消。
        写成"一律取 caller"会让 team 的联动撤票整条失效:成员被踢出队伍后
        票据还在队列里,他会被拉进一场自己已不在队的对局。

    系统面(ReleaseMatch / ResolvePlayerMatchContext)
        caller!=0 一律拒(ERR_PERMISSION_DENY)。经 Envoy 进来的客户端请求一定带
        jwt_authn 注入的 x-pandora-player-id;不拒的话任何登录玩家都能用任意
        match_id 摧毁他人在局撮合状态(删票据/claim/match,griefing + 绕过不变量 §1),
        或按 player_id 读别人的对局上下文(含 battle 票)。

★ ResolvePlayerMatchContext 另有 **request-bound 内部验签**:caller==0 只能证明
  「不带玩家 JWT」,而 :8444 没有 jwt_authn,集群内任何 Pod 都满足。签名把
  caller 身份 + 精确方法 + subject + 时间戳 + nonce 绑在一起,nonce 在共享 Redis
  里原子消费一次。两个合法调用方(login / team)各持**独立**密钥 —— 共用一把
  等于两者可以互相冒充。

★ 每处宽 except 之前必须先放行 asyncio.CancelledError:它在 3.8+ 是 BaseException,
  而 grpc.aio 正是**用取消**终止在途 handler。吞掉 = 停机时把取消映射成 in-band
  业务错误码返回一个正常响应,客户端每次滚动更新都会收到一批假失败。
"""

from __future__ import annotations

import asyncio

from pandora.common.v1 import errcode_pb2 as commonpb
from pandora.config.v1 import configtable_pb2 as cfgpb
from pandora.config.v1 import configtable_pb2_grpc as cfggrpc
from pandora.match.v1 import match_pb2 as matchpb
from pandora.match.v1 import match_pb2_grpc as matchgrpc

from pandorapy import errcode
from pandorapy import internalrpcauth
from pandorapy import interceptors as pintercept
from pandorapy import log as plog
from pandorapy.protoenum import enum_name
from pandorapy.services.matchmaker import catalog as mcat
from pandorapy.services.matchmaker.presence_gate import MemberOfflineError

GRPC_SERVICE_FULL_NAME = "pandora.match.v1.MatchService"
CONFIGTABLE_GRPC_SERVICE_FULL_NAME = "pandora.config.v1.ConfigTableAdminService"

# 与 Go 的 matchv1.MatchService_ResolvePlayerMatchContext_FullMethodName 逐字相同。
# 签名把方法名也绑进去:少了它,一份为 A 方法签的凭据可以拿去调 B 方法。
RESOLVE_FULL_METHOD = "/pandora.match.v1.MatchService/ResolvePlayerMatchContext"


def _to_proto_code(exc: BaseException) -> int:
    """errcode → proto enum(1:1 数值映射)。对应 Go 的 toProtoCode。"""
    return errcode.as_code(exc)


def _code_of(rpc: str, exc: BaseException, **fields: object) -> int:
    """映射错误码,顺带把**非业务异常**打出来。

    ★ 这条日志 Go 侧没有,是 Python 专有的必要补丁,不是装饰:
    `errcode.as_code` 对任何非 PandoraError 一律返回 ERR_UNKNOWN,而本层把它当成
    in-band code 正常返回 —— 于是 access log 记的是 `rpc_ok`,gRPC status 是 OK,
    **整条链路零错误痕迹**,客户端只看到一个 1。
    真实踩过:`MatchUsecase` 漏挂 MatchRpcMixin 时,5 个 RPC 全程返回 ERR_UNKNOWN
    而服务端日志一行都没有(见 biz.MatchUsecase 的类注释)。
    业务失败(PandoraError)照旧不打 —— 那是正常分支,打了就是刷屏。
    """
    code = errcode.as_code(exc)
    if code == errcode.ErrUnknown:
        plog.get().warning(
            "match_rpc_internal_error",
            rpc=rpc,
            err_type=type(exc).__name__,
            err=str(exc),
            **fields,
        )
    return code


def _metadata_dict(context) -> dict[str, str]:  # noqa: ANN001
    """把 grpc metadata 摊成 dict,供 internalrpcauth.Verifier 消费。

    gRPC 保证 key 已小写;`-bin` 头与本协议无关,直接跳过。
    重复 key 取最后一个 —— 与 Go 的 `single()`(len!=1 即视为缺失)在畸形请求上
    有细微差异,但两边都会验签失败,方向一致。
    """
    out: dict[str, str] = {}
    for key, value in context.invocation_metadata() or ():
        if isinstance(value, bytes):
            continue
        out[key.lower()] = value
    return out


class MultiCallerVerifier:
    """按 caller 身份路由到各自验签器 —— 对应 Go 的
    `internalrpcauth.MultiCallerVerifier`(Python 共享件里还没有,见 shared_files_needed)。

    ★ 为什么不能"挨个试一遍":每个 Verifier 的 verify 都会**消费 nonce**。
    逐个试的话,第一个验签器会把这份合法凭据的 nonce 先吃掉,第二个再验就成了
    "重放" —— 于是 team 的每一次合法调用都被拒,而日志上看是重放攻击。
    按 caller 精确路由,未知 caller 直接拒且**不消费任何 nonce**。
    """

    __slots__ = ("_by_caller",)

    def __init__(self, *verifiers: internalrpcauth.Verifier) -> None:
        by_caller: dict[str, internalrpcauth.Verifier] = {}
        for v in verifiers:
            if v is None:
                raise ValueError("internal RPC multi-caller verifier: nil verifier")
            caller = v.caller
            if not caller:
                raise ValueError(
                    "internal RPC multi-caller verifier: empty caller identity"
                )
            if caller in by_caller:
                raise ValueError(
                    f"internal RPC multi-caller verifier: duplicate caller {caller!r}"
                )
            by_caller[caller] = v
        if not by_caller:
            raise ValueError(
                "internal RPC multi-caller verifier: at least one verifier is required"
            )
        self._by_caller = by_caller

    async def verify(
        self, metadata: dict[str, str], full_method: str, subject: int
    ) -> None:
        caller = metadata.get(internalrpcauth.CALLER_METADATA_KEY, "")
        verifier = self._by_caller.get(caller) if caller else None
        if verifier is None:
            raise internalrpcauth.ErrUnauthorized("unknown internal caller")
        await verifier.verify(metadata, full_method, subject)


class MatchService(matchgrpc.MatchServiceServicer):
    """实现 pandora.match.v1.MatchService 的 6 个 RPC。"""

    __slots__ = ("_uc", "_sf", "_resume_auth")

    def __init__(self, usecase, ticket_sf, resume_auth) -> None:  # noqa: ANN001
        self._uc = usecase
        # ★ ticket_id 与 match_id 是两个**独立 ID 空间**,各持一个发号器。
        # 两者共用同一 nodeID(同一把 lease),会发出逐位相同的 ID ——
        # 禁止把两个空间的 ID 放进同一个 dict / 唯一键比较。
        self._sf = ticket_sf
        # None = 未配 resume 密钥。ResolvePlayerMatchContext 恒返回 ERR_UNAVAILABLE,
        # 绝不放行:那是一个"按 player_id 读任意玩家对局上下文(含 battle 票)"的接口。
        self._resume_auth = resume_auth

    # ── 辅助 ───────────────────────────────────────────────────────────────

    @staticmethod
    def _caller_id(context) -> int:  # noqa: ANN001
        """从鉴权上下文取 player_id。对应 Go 的 callerID(ctx)。"""
        return pintercept.extract_player_id(context)

    @staticmethod
    def _rejected(rpc: str, reason: str, **fields: object) -> None:
        """记录一次被身份闸拒掉的调用。

        这类分支以 in-band code + OK status 返回,统一 access log 会记成 rpc_ok ——
        不单独打一条的话,线上对越权尝试**零可见性**。事件名与 Go 逐字相同。
        """
        plog.get().warning("match_rpc_rejected", rpc=rpc, reason=reason, **fields)

    # ── RPC 1:StartMatch(客户端面)──────────────────────────────────────

    async def StartMatch(self, request, context):  # noqa: N802
        captain_id = self._caller_id(context)
        if captain_id == 0:
            self._rejected(
                "StartMatch",
                "missing_caller_identity",
                team_id=request.team_id,
                map_id=request.map_id,
            )
            return matchpb.StartMatchResponse(code=commonpb.ERR_UNAUTHORIZED)

        # team_id==0 是合法的**单人入口**(单排撮合 / 单人进副本),名单即身份本人。
        # 成员解析与人数校验统一在 biz.resolve_members,这里不重复判定。
        #
        # entry_mode 是玩家的**选择**(§17.2),不是权威数据:能不能这么进由 biz 按
        # 关卡表 fail-closed 判定,这里只透传。
        ticket_id = self._sf.generate()
        try:
            match_id = await self._uc.start_match(
                ticket_id,
                request.team_id,
                captain_id,
                request.map_id,
                request.entry_mode,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 具体被哪道门拒(gate/reason)由 biz 打 match_start_rejected,这里不重复。
            resp = matchpb.StartMatchResponse(
                code=_code_of("StartMatch", exc, team_id=request.team_id, map_id=request.map_id)
            )
            if isinstance(exc, MemberOfflineError):
                # 4011(在线闸):把被判缺席的成员**结构化过线**,客户端才能点名
                # 「XX 不在大厅」。只拼在 error 文本里的话队长看不出该等谁
                # (INC-20260813-001 行动项)。
                resp.absent_player_ids.extend(exc.absent_player_ids)
            return resp
        return matchpb.StartMatchResponse(code=commonpb.OK, match_id=match_id)

    # ── RPC 2:CancelMatch(客户端 + 内部双向面)───────────────────────────

    async def CancelMatch(self, request, context):  # noqa: N802
        player_id = self._caller_id(context)
        if player_id == 0:
            # 内部路径(team 离队/踢人联动撤票,不经 Envoy)才会走到这里。
            player_id = request.player_id
        if player_id == 0:
            self._rejected("CancelMatch", "missing_caller_identity")
            return matchpb.CancelMatchResponse(code=commonpb.ERR_UNAUTHORIZED)
        # 内部路径没有 JWT,player_id 不会自动进日志上下文,手写(§11.3 R3):
        # 不写的话「谁的票被谁撤了」在日志里断在服务边界上。
        token = plog.bind_player_id(player_id)
        try:
            await self._uc.cancel_match(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return matchpb.CancelMatchResponse(
                code=_code_of("CancelMatch", exc, player_id=player_id)
            )
        finally:
            token.var.reset(token)
        return matchpb.CancelMatchResponse(code=commonpb.OK)

    # ── RPC 3:ConfirmMatch(客户端面)────────────────────────────────────

    async def ConfirmMatch(self, request, context):  # noqa: N802
        player_id = self._caller_id(context)
        if player_id == 0:
            self._rejected(
                "ConfirmMatch", "missing_caller_identity", match_id=request.match_id
            )
            return matchpb.ConfirmMatchResponse(code=commonpb.ERR_UNAUTHORIZED)
        if request.match_id == 0:
            self._rejected(
                "ConfirmMatch", "missing_match_id", accept=request.accept
            )
            return matchpb.ConfirmMatchResponse(code=commonpb.ERR_INVALID_ARG)
        try:
            await self._uc.confirm_match(player_id, request.match_id, request.accept)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return matchpb.ConfirmMatchResponse(
                code=_code_of("ConfirmMatch", exc, match_id=request.match_id)
            )
        return matchpb.ConfirmMatchResponse(code=commonpb.OK)

    # ── RPC 4:GetMatchProgress(客户端面)────────────────────────────────

    async def GetMatchProgress(self, request, context):  # noqa: N802
        """match_id 可为 0:重新登录 / 换设备丢句柄时 biz 按 caller 反查本人票据。

        成员资格鉴权下沉 biz(caller 必须是该 match/ticket 成员,否则按"不存在"处理),
        防外挂拉别人的对局进度 —— 里面带着 battle_ticket。
        """
        caller = self._caller_id(context)
        if caller == 0:
            self._rejected(
                "GetMatchProgress",
                "missing_caller_identity",
                match_id=request.match_id,
            )
            return matchpb.GetMatchProgressResponse(code=commonpb.ERR_UNAUTHORIZED)
        try:
            progress = await self._uc.get_match_progress(caller, request.match_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return matchpb.GetMatchProgressResponse(
                code=_code_of("GetMatchProgress", exc, match_id=request.match_id)
            )
        return matchpb.GetMatchProgressResponse(
            code=commonpb.OK, progress=progress
        )

    # ── RPC 5:ReleaseMatch(系统面)──────────────────────────────────────

    async def ReleaseMatch(self, request, context):  # noqa: N802
        """battle_result 结算落库后调用。幂等:重复调用 / 已释放均返回 OK。"""
        caller = self._caller_id(context)
        if caller != 0:
            self._rejected(
                "ReleaseMatch",
                "player_jwt_not_allowed",
                match_id=request.match_id,
                player_id=caller,
            )
            return matchpb.ReleaseMatchResponse(code=commonpb.ERR_PERMISSION_DENY)
        if request.match_id == 0:
            self._rejected(
                "ReleaseMatch",
                "missing_match_id",
                players=len(request.player_ids),
            )
            return matchpb.ReleaseMatchResponse(code=commonpb.ERR_INVALID_ARG)
        try:
            await self._uc.release_match(request.match_id, list(request.player_ids))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "match_release_rejected",
                reason="usecase_failed",
                code=_to_proto_code(exc),
                match_id=request.match_id,
                players=len(request.player_ids),
                err=str(exc),
            )
            return matchpb.ReleaseMatchResponse(code=_to_proto_code(exc))
        return matchpb.ReleaseMatchResponse(code=commonpb.OK)

    # ── RPC 6:ResolvePlayerMatchContext(系统面 + 验签)───────────────────

    async def ResolvePlayerMatchContext(self, request, context):  # noqa: N802
        caller = self._caller_id(context)
        if caller != 0:
            self._rejected(
                "ResolvePlayerMatchContext",
                "player_jwt_not_allowed",
                player_id=caller,
            )
            return matchpb.ResolvePlayerMatchContextResponse(
                code=commonpb.ERR_PERMISSION_DENY
            )
        if request.player_id == 0:
            self._rejected("ResolvePlayerMatchContext", "missing_player_id")
            return matchpb.ResolvePlayerMatchContextResponse(
                code=commonpb.ERR_INVALID_ARG
            )
        if self._resume_auth is None:
            # 未配密钥 → 恒 UNAVAILABLE。**绝不放行**:这是一个能读出任意玩家
            # battle 票的接口,"没配就不验"等于把它对整个集群网络开放。
            self._rejected("ResolvePlayerMatchContext", "resume_auth_unconfigured")
            return matchpb.ResolvePlayerMatchContextResponse(
                code=commonpb.ERR_UNAVAILABLE
            )
        # 内部面无 JWT,player_id 必须手写进日志上下文(§11.3 R3)。
        # ⚠️ 必须放在上面那道 caller!=0 之后:那道门判的就是上下文里有没有 player_id。
        token = plog.bind_player_id(request.player_id)
        try:
            try:
                await self._resume_auth.verify(
                    _metadata_dict(context), RESOLVE_FULL_METHOD, request.player_id
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                code = commonpb.ERR_PERMISSION_DENY
                reason = "resume_auth_signature_rejected"
                if isinstance(exc, internalrpcauth.ErrUnavailable):
                    # 重放存储不可用 → 说不清是不是重放,按不确定回,让调用方重试
                    # 而不是当成越权(当成越权会让 login 的冷重连整块失败)。
                    code = commonpb.ERR_UNAVAILABLE
                    reason = "resume_auth_replay_store_unavailable"
                plog.get().warning(
                    "resolve_match_context_service_auth_rejected",
                    reason=reason,
                    player_id=request.player_id,
                    replay_authority_unavailable=code == commonpb.ERR_UNAVAILABLE,
                )
                return matchpb.ResolvePlayerMatchContextResponse(code=code)

            try:
                resolved = await self._uc.resolve_player_match_context(
                    request.player_id
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "resolve_match_context_failed",
                    reason="resolve_unavailable",
                    code=_to_proto_code(exc),
                    player_id=request.player_id,
                    err=str(exc),
                )
                return matchpb.ResolvePlayerMatchContextResponse(
                    code=_to_proto_code(exc)
                )
            resolved.code = commonpb.OK
            plog.get().info(
                "resolve_match_context_ok",
                player_id=request.player_id,
                state=enum_name(matchpb.PlayerMatchContextState, resolved.state),
                stage=enum_name(matchpb.PlayerMatchResumeStage, resolved.stage),
                ticket_id=resolved.ticket_id,
                match_id=resolved.match_id,
                map_id=resolved.map_id,
                game_mode=resolved.game_mode,
                has_battle_ticket=resolved.battle_ticket != "",
            )
            return resolved
        finally:
            token.var.reset(token)


class ConfigTableAdminService(cfggrpc.ConfigTableAdminServiceServicer):
    """配置表热更受控入口 —— 对应 Go 的 service.ConfigTableAdminService。

    ★ 只在 config_table.dir 配置时注册(store 已完成启动首载)。

    ★ 纵深防御:带玩家身份的调用一律拒。Envoy 无本 service 的路由,
      这里是第二道 —— 少了它,一个能打到内部端口的玩家就能触发全服配置重载。

    ★ 失败一律返回 in-band ERR_INVALID_STATE + detail,**保留旧表**:
      半批切换比不切换危险得多(关卡表与别的表互相对不上)。
    """

    __slots__ = ("_store",)

    def __init__(self, store: mcat.Store) -> None:
        self._store = store

    async def ReloadConfigTable(self, request, context):  # noqa: N802
        if pintercept.extract_player_id(context) != 0:
            plog.get().warning(
                "match_rpc_rejected",
                rpc="ReloadConfigTable",
                reason="player_jwt_not_allowed",
                expect_version=request.expect_version,
            )
            return cfgpb.ReloadConfigTableResponse(
                code=commonpb.ERR_PERMISSION_DENY,
                detail="player-facing calls are not allowed",
            )
        if self._store is None or not self._store.active_dir:
            return cfgpb.ReloadConfigTableResponse(
                code=commonpb.ERR_INVALID_STATE,
                active_version=0,
                detail="config table store is not initialized",
            )

        # ★ 整段热更持锁:load + 单调检查 + 切换必须是一个原子段。
        #   current_version **读在锁内** —— 读在 await 之前会让并发 reload
        #   拿过期基准做单调判断,版本静默回退且两次都报成功。
        async with self._store.reload_mutex:
            current_version = self._store.tables.version
            try:
                # 加载走线程池:json 解析 + checksum 是纯 CPU/阻塞 IO,
                # 直接在事件循环里跑会把同进程的撮合循环和所有在途 RPC 一起卡住。
                result = await asyncio.to_thread(
                    mcat.load_tables, self._store.active_dir, request.expect_version
                )
                # ★ 批次级校验器(含"默认 map_id 必须是战斗类关卡")在**切换前**跑:
                # 只在启动时查的话,一个坏批次热更后所有走默认 map_id 的 StartMatch
                # 会全部失败,而热更本身报的是成功。
                self._store.run_validators(result.tables)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().error(
                    "configtable_reload_failed",
                    dir=self._store.active_dir,
                    expect_version=request.expect_version,
                    err=str(exc),
                )
                return cfgpb.ReloadConfigTableResponse(
                    code=commonpb.ERR_INVALID_STATE,
                    active_version=current_version,
                    detail=str(exc),
                )
            for warning in result.warnings:
                plog.get().warning("configtable_reload_warning", warning=warning)

            if result.version == current_version:
                # 版本未变 = no-op,仍返回 OK:重复触发热更不该被当成失败。
                return cfgpb.ReloadConfigTableResponse(
                    code=commonpb.OK,
                    active_version=result.version,
                    reloaded=False,
                    detail="version unchanged, no-op",
                )
            if result.version < current_version:
                # ★ 版本单调闸(防回退),对齐 Go pkg/configtable/store.go。
                # 缺了它不是"多热更一次":回退之后内存里生效的是旧批次而运维视图上是
                # 新版本号,此后所有人对"现在生效的是哪批"的判断都是错的。
                plog.get().warning(
                    "configtable_reload_rejected_rollback",
                    active_version=result.version,
                    current_version=current_version,
                    hint="active 版本低于当前生效版本,拒绝回退;要回滚请重新发布更大的版本号",
                )
                return cfgpb.ReloadConfigTableResponse(
                    code=commonpb.ERR_INVALID_STATE,
                    active_version=current_version,
                    detail=(
                        f"active 版本 {result.version} 低于当前生效 {current_version},拒绝回退"
                    ),
                )
            self._store.replace(result.tables)
            plog.get().info(
                "configtable_reloaded",
                version=result.version,
                reloaded=True,
                levels=result.tables.level_count(),
            )
            return cfgpb.ReloadConfigTableResponse(
                code=commonpb.OK,
                active_version=result.version,
                reloaded=True,
                detail="; ".join(result.warnings),
            )
