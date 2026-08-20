"""hub_allocator 的 gRPC service 层 —— 对应 Go 侧
`services/battle/hub_allocator/internal/service/hub.go`。

职责只有三件,一件都不多:

  - 实现 `hubv1.HubAllocatorService` 的 10 个 RPC;
  - proto Request/Response ↔ biz 入参/出参互转;
  - `errcode` → `commonv1.ErrCode` 1:1 映射(数值相同,`errcode.as_code` 即 Go 的 `errcode.As`)。

**调用方不是玩家客户端**:login 调 AssignHub、Hub DS 调 Heartbeat / Acknowledge*,
因此不从鉴权上下文取 player_id,由上游在请求体里显式传入。

例外:`ListHubLines` / `TransferToLine` 是玩家侧 RPC(经 Envoy :8443 客户端面,
jwt_authn 注入 `x-pandora-player-id`),player_id **一律取自鉴权上下文**(JWT sub 权威),
不信请求体 —— 请求体里压根没有 player_id 字段,这一点在 proto 层就钉死了。

═══════════════════════════════════════════════════════════════════════════════
与 Go 的形变(逐条都有理由)
═══════════════════════════════════════════════════════════════════════════════

1. **`ctx` 不存在**。Go 用 `plog.With(ctx)` 展开链路字段;Python 用 `plog.get()` +
   contextvars(拦截器已绑 trace_id / player_id)。`plog.WithTeamID` / `WithMatchID`
   在 Python 侧**尚无公开 binder**(`pandorapy/log.py` 的 `_team_id` / `_match_id`
   是私有 ContextVar,只有 `bind_player_id` / `bind_trace_id` 有公开入口),因此
   AssignHub 的两条日志用 `_join_fields()` 把 `team_id` / `match_id` 显式补进字段。
   零值不补 —— Go 的 `plog.With(ctx)` 取不到 ctx value 时不加字段,补一个 `team_id=0`
   会让按该字段过滤的 Loki 查询把无队伍的请求也捞进来。

2. **DS 回调凭据守卫是注入的 Protocol**。Go 的 `pmw.DSCallbackGuard.CheckHubCredential`
   返回 `(claims, cred, err)`;本仓 `pandorapy/dsauth.py` 的 `DSCallbackGuard` 目前
   **只有** `check(context, scope) -> int`(无 Model B 凭据抽取)。这里声明
   `HubCredentialGuard` 协议作为注入口,**不在 service 层实现验签** —— 验签属于
   共享守卫件,写在这儿就变成第二份实现,而两份验签逻辑漂移的后果是"一边拒一边放"。

3. **`(值, error)` → 返回值 + 抛异常**;`toProtoCode(err)` → `errcode.as_code(exc)`。
   每个 `except BaseException` 前**必须**先放行 `CancelledError`:grpc.aio 正是用取消
   终止在途 handler,吞掉它会把优雅停机变成"给客户端回一批假的业务失败,而在途请求
   并没有真的排空"。

4. **`hubCredentialFromGuard` 不需要**。Go 那个函数把 `pmw.VerifiedCredential` 翻成
   `biz.HubCredential`;Python 侧守卫协议直接产出 `biz.HubCredential`,少一层转换就
   少一处"漏抄一个字段"的机会(Go 那边 7 个字段全靠手抄)。
"""

from __future__ import annotations

import asyncio
import time
from typing import Protocol, runtime_checkable

import grpc
from pandora.common.v1 import errcode_pb2 as commonpb
from pandora.hub.v1 import allocator_pb2 as hubpb
from pandora.hub.v1 import allocator_pb2_grpc as hubgrpc

from pandorapy import dsauth, errcode, interceptors
from pandorapy import log as plog
from pandorapy import sessiongate
from pandorapy.services.hub_allocator import biz

# DS 回调令牌里的 ds_type。对应 Go 的 `auth.DSTypeHub`。
# 与 `battle_result/service.py` 的 `DS_TYPE_BATTLE` 同一约定:本仓 `dsauth` 没有导出
# 这两个字面量,而从 `services/login/dsticket` 借用会让 hub_allocator 依赖 login 包。
DS_TYPE_HUB = "hub"

# ── 日志 reason 枚举 ─────────────────────────────────────────────────────────
#
# infra.md §11.3 R2:一个 if 收敛了 N 个条件的,必须拆成 N 个 reason;snake_case 常量,
# 不是自由文本。这里全是**入参 / 凭据门**的拒绝原因 —— 它们返回的 ERR_INVALID_ARG /
# ERR_UNAUTHORIZED 都不是 `IsServerFault`,access log 只会记成 rpc_ok(DEBUG),
# 生产 info 级完全不可见,必须由本层自己打出来。

REASON_NOT_MODEL_B_AUTHORITY = "not_model_b_authority"
REASON_MISSING_PLAYER_ID = "missing_player_id"
REASON_MISSING_ASSIGNMENT_ID = "missing_assignment_id"
REASON_MISSING_POD = "missing_pod"
REASON_MISSING_ADMISSION_ID = "missing_admission_id"
REASON_MISSING_ADMISSION_SEQ = "missing_admission_seq"
REASON_NO_MODEL_B_CREDENTIAL = "no_model_b_credential"
REASON_CREDENTIAL_CHECK_FAILED = "credential_check_failed"
REASON_LEGACY_CREDENTIAL_UNDER_MODEL_B = "legacy_credential_under_model_b"
REASON_NO_SESSION_PLAYER_ID = "no_session_player_id"
REASON_BIZ_REJECTED = "biz_rejected"
REASON_UNKNOWN = "unknown"
# ledger 未能把 reservation 转成 connected owner。它返回 code=OK + admitted=false,
# access log 记 rpc_ok(DEBUG),不打就彻底看不见。
REASON_LEDGER_NOT_ADMITTED = "ledger_not_admitted"
# placement 路由体系已硬切删除,旧调用方残留。
REASON_SERVICE_DISABLED = "service_disabled"

# 准入 / 离场的两条通道标签(`channel` 字段):同一个事件名下区分 Model B 与 local
# 旧面,排障时不用反推部署形态。
CHANNEL_MODEL_B = "model_b"
CHANNEL_LOCAL = "local"

# biz 错误码 → 稳定 snake_case reason。用于"同一个错误码两种截然不同处置"场景下的
# 日志分流(如 ErrHubNoAvailable 既可能是真没容量、也可能是 CAS 并发耗尽 —— 后者在
# biz 侧另有 hub_assign_cas_exhausted)。
#
# 写成表而不是 if 链:Go 那边是一个 switch,漏一个 case 会静默落到 default;
# 表的键是 errcode 常量本身,拼错的名字在 import 期就是 NameError。
_BIZ_REJECT_REASONS: dict[int, str] = {
    errcode.ErrInvalidArg: "invalid_arg",
    errcode.ErrUnauthorized: "unauthorized",
    errcode.ErrInvalidState: "invalid_state",
    errcode.ErrUnavailable: "upstream_unavailable",
    errcode.ErrSessionSuperseded: "session_superseded",
    errcode.ErrLocatorConflict: "locator_conflict",
    errcode.ErrHubNoAvailable: "hub_no_available",
    errcode.ErrHubTransferFailed: "hub_transfer_failed",
    errcode.ErrHubLineFull: "hub_line_full",
    errcode.ErrHubTransferCooldown: "hub_transfer_cooldown",
    errcode.ErrHubTransferNotInHub: "hub_transfer_not_in_hub",
    errcode.ErrOwnerBarrierNotOpen: "owner_barrier_not_open",
    errcode.ErrOwnerEpochConflict: "owner_epoch_conflict",
    errcode.ErrOwnerIdentityMismatch: "owner_identity_mismatch",
}


# ── 辅助 ─────────────────────────────────────────────────────────────────────


def _to_proto_code(exc: BaseException) -> int:
    """`pkg/errcode` → proto enum(数值相同)。对应 Go 的 `toProtoCode`。

    返回 int 而不是 `commonpb.ErrCode`:proto3 是开放枚举,int 可直接赋给枚举字段,
    而包一层 wrapper 反而会在遇到本副本还不认识的码时抛 ValueError(§protoenum)。
    """
    return errcode.as_code(exc)


def _biz_reject_reason(exc: BaseException) -> str:
    """biz 上抛的错误 → 稳定 reason。对应 Go 的 `bizRejectReason`。"""
    return _BIZ_REJECT_REASONS.get(errcode.as_code(exc), REASON_BIZ_REJECTED)


def _model_b_admission_arg_reason(
    model_b_authority: bool,
    player_id: int,
    assignment_id: str,
    pod: str,
    admission_id: str,
    admission_seq: int,
) -> str:
    """把 AcknowledgeAdmission / AcknowledgeDeparture 里那个**一个 INVALID_ARG 收敛
    6 个条件**的入参门拆回唯一枚举 reason(R2)。对应 Go 的 `modelBAdmissionArgReason`。

    判定顺序刻意与 `if` 的短路顺序逐字一致:调用方只在已经判定要拒绝时才调用它,
    因此这里返回的就是"第一个不满足的条件",与拒绝原因严格同源。

    为什么必须区分:`model_b_authority` 配错(部署形态,要改配置重启)与 DS 没带
    admission_seq(DS 版本不对,要重打包)处置方案完全相反,而 DS 侧只认 code==0
    才出队,非 0 按 1s 周期无限重试(2026-08-06 事故),再犯时必须能从后端日志
    一眼分出是哪一个字段缺失。
    """
    if not model_b_authority:
        return REASON_NOT_MODEL_B_AUTHORITY
    if player_id == 0:
        return REASON_MISSING_PLAYER_ID
    if assignment_id == "":
        return REASON_MISSING_ASSIGNMENT_ID
    if pod == "":
        return REASON_MISSING_POD
    if admission_id == "":
        return REASON_MISSING_ADMISSION_ID
    if admission_seq == 0:
        return REASON_MISSING_ADMISSION_SEQ
    return REASON_UNKNOWN


def _local_admission_arg_reason(player_id: int, assignment_id: str, pod: str) -> str:
    """mode=local 通道(只校验三元组)的同款拆分。对应 Go 的 `localAdmissionArgReason`。"""
    if player_id == 0:
        return REASON_MISSING_PLAYER_ID
    if assignment_id == "":
        return REASON_MISSING_ASSIGNMENT_ID
    if pod == "":
        return REASON_MISSING_POD
    return REASON_UNKNOWN


def _md_get(context: grpc.aio.ServicerContext, key: str) -> str:
    """取一个 gRPC metadata 头(key 由 HTTP/2 规范保证小写,仍归一化一次防夹具改过)。"""
    try:
        md = context.invocation_metadata()
    except Exception:  # noqa: BLE001 —— 测试桩 / 无 metadata 的调用
        return ""
    if not md:
        return ""
    for entry in md:
        if str(entry[0]).lower() == key:
            value = entry[1]
            return value.decode() if isinstance(value, bytes) else str(value)
    return ""


def _ds_bearer_token(context: grpc.aio.ServicerContext) -> str:
    """本次 DS 回调携带的 Bearer 令牌。对应 Go 的 `pmw.DSBearerToken(ctx)`。

    只用于把心跳的在场名单转发给 player_locator 时**原样代传**调用方身份 ——
    hub_allocator 自己没有 locator 的写权限,借的是 DS 的。

    ★ 取到的值**禁止**进日志 / 持久化(biz 侧 `refresh_hub_presence` 有同款注释):
      它是一张可重放的有效凭据,落一次盘就等于把 DS 身份泄漏成静态密钥。
    """
    raw = _md_get(context, dsauth.AUTHORIZATION_HEADER).strip()
    prefix = "bearer "
    if len(raw) > len(prefix) and raw[: len(prefix)].lower() == prefix:
        return raw[len(prefix) :].strip()
    return ""


def _session_jti(context: grpc.aio.ServicerContext) -> str:
    """请求方会话 JWT 的 jti。对应 Go 的 `pmw.SessionJTIFromContext(ctx)`。

    Go 侧 biz 在 `TransferHub` / `transferToLineInner` 里自己从 ctx 捞;Python 的 biz
    不持有 gRPC context,改由本层读出后显式传参(`biz_rpc.py` 模块头形变 ①)。

    取不到返回 ""(直连内网端口联调 / DS 面无 jwt_authn),**由 biz 决定** fail-closed
    还是兼容放行 —— 传输层只负责如实取值,不擅自兜底。
    """
    return sessiongate.parse_jwt_payload_claims(
        _md_get(context, sessiongate.METADATA_KEY_JWT_PAYLOAD)
    ).jti


def _join_fields(team_id: int, source_match_id: int) -> dict[str, int]:
    """AssignHub 的 R3 join key(见模块头形变 ①)。

    Go 把 team_id / source_match_id 写进 ctx,本请求后续每条 `plog.With(ctx)` 自动带上;
    Python 侧无公开 binder,这里显式补进字段。**零值不补** —— Go 的 `plog.With(ctx)`
    对取不到的 ctx value 不加字段,补 0 会让按该字段过滤的查询把无队伍 / 非回流的
    请求也捞进来。
    """
    fields: dict[str, int] = {}
    if team_id != 0:
        fields["team_id"] = team_id
    if source_match_id != 0:
        # Go 用的是 plog.WithMatchID,落到日志上的字段名是 match_id 而不是 source_match_id。
        fields["match_id"] = source_match_id
    return fields


def _elapsed_ms(started_at: float) -> int:
    """跨阶段耗时。对应 Go 的 `time.Since(startedAt).Milliseconds()`。

    单调时钟:壁钟回拨会让耗时变成负数,而这几条日志正是 §11.3 验收判据 5「慢在哪」
    的唯一数据源。
    """
    return int((time.monotonic() - started_at) * 1000)


def _log_admission_outcome(  # noqa: PLR0913 —— 与 Go 的 logAdmissionOutcome 逐参对应
    player_id: int,
    assignment_id: str,
    pod: str,
    admission_id: str,
    admission_seq: int,
    channel: str,
    admitted: bool,
    started_at: float,
) -> None:
    """把准入结果分成两条稳定事件。对应 Go 的 `logAdmissionOutcome`。

    `admitted=True` 是不可逆阶段推进(R1,Info);`admitted=False` 是"OK 包里的拒绝"
    —— ledger 未能把 reservation 转成 connected owner,它不带错误码、access log
    完全看不见,但 DS 侧会据此**拒开 spawn gate**(R2,Warn)。
    """
    if not admitted:
        plog.get().warning(
            "hub_admission_rejected",
            player_id=player_id,
            hub_assignment_id=assignment_id,
            ds_pod=pod,
            admission_id=admission_id,
            admission_seq=admission_seq,
            channel=channel,
            reason=REASON_LEDGER_NOT_ADMITTED,
            elapsed_ms=_elapsed_ms(started_at),
        )
        return
    plog.get().info(
        "hub_admitted",
        player_id=player_id,
        hub_assignment_id=assignment_id,
        ds_pod=pod,
        admission_id=admission_id,
        admission_seq=admission_seq,
        channel=channel,
        elapsed_ms=_elapsed_ms(started_at),
    )


@runtime_checkable
class HubCredentialGuard(Protocol):
    """Model B hub 令牌验签 + 凭据抽取。对应 Go 的 `pmw.DSCallbackGuard.CheckHubCredential`。

    返回 `(token_gen, cred, code)`:

      - `token_gen`:legacy 令牌代际(Go 的 `claims.Gen()`);无已验签令牌时 0。
        Go 返回整个 `*auth.DSCallbackClaims`,但**唯一用途**就是 `claimsGen(claims)`
        —— 本仓 `dsauth.DSCallbackClaims` 没有 `gen` 字段,回一个 int 比先造一个
        只为取一格的 claims 类型更诚实。
      - `cred`:Model B 令牌(带 ds_uid / ds_epoch / ds_gen / jti)抽出的凭据;
        legacy 令牌或 off / permissive 档为 `None`。
      - `code`:0 = 放行;非 0 = 应回给调用方的 in-band ErrCode。

    ★ 返回码而不是抛异常,与 `dsauth.DSCallbackGuard.check` 同一约定 —— 本层一律以
      in-band code 回应,抛异常会让它逐出 handler,调用方拿到 gRPC UNKNOWN 而不是
      可解析的业务码。
    """

    def check_hub_credential(
        self, context: grpc.aio.ServicerContext, scope: dsauth.DSScope
    ) -> tuple[int, biz.HubCredential | None, int]: ...


class HubService(hubgrpc.HubAllocatorServiceServicer):
    """实现 `pandora.hub.v1.HubAllocatorService` 的 10 个 RPC。对应 Go 的 `HubService`。

    业务失败一律走 **in-band code**(返回 `response.code`,gRPC status 保持 OK),
    与 Go 侧逐字一致 —— 调用方按 code 分支,不靠 gRPC status 猜。
    """

    __slots__ = ("_uc", "_ds_guard", "_model_b_authority", "_local_admission")

    def __init__(self, uc: biz.HubUsecase) -> None:
        self._uc = uc
        # DS 回调令牌守卫(审核 P1 #1);None 等价 ds_auth.mode=off。
        self._ds_guard: HubCredentialGuard | None = None
        # Model B「Redis 唯一授权权威」总开关(main 在 ds_auth.authority_mode=redis
        # + agones + enforce 时置 True)。置 True 后心跳**必须**携带 Model B 凭据;
        # 仅带 legacy 令牌(ds_gen 但无 uid/epoch/jti)→ 直接拒 ErrUnauthorized,
        # 不给旧令牌借心跳保活 / 翻 ready(审核二轮 CE1/CE2:彻底删除 Redis 授权下
        # 的 legacy 心跳回退分支)。
        self._model_b_authority = False
        # mode=local(local-off-v1)专用准入通道。仅 main 在 local-off-v1 profile
        # 校验通过后置 True;与 `_model_b_authority` 互斥(agones 恒 False)。
        # 置 True 后 AcknowledgeAdmission 走 `acknowledge_local_admission`(归属记录
        # 复核 + 与 Model B 同一个 owner Admit 完成点),否则本机 DS 会在 Admission ACK
        # 上被拒并踢玩家;AcknowledgeDeparture 同样改走 `acknowledge_local_departure`
        # —— 进场与离场必须成对,只补一半的话离场 ACK 恒 INVALID_ARG,DS 的 Departure
        # 队列每秒重试且永不出队。
        self._local_admission = False

    # ── 装配 ─────────────────────────────────────────────────────────────

    def set_ds_callback_guard(self, guard: HubCredentialGuard | None) -> None:
        """注入 DS 回调令牌守卫(可选依赖,main 在 ds_auth 已配时调用)。"""
        self._ds_guard = guard

    def set_model_b_authority(self, enabled: bool) -> None:
        """开启 Model B「Redis 唯一授权权威」(见字段注释;仅 authority_mode=redis 时)。"""
        self._model_b_authority = enabled

    def set_local_admission(self, enabled: bool) -> None:
        """开启 mode=local 专用准入通道(见字段注释;仅 conf 为 local 时)。"""
        self._local_admission = enabled

    def _check_hub_credential(
        self, context: grpc.aio.ServicerContext, pod: str
    ) -> tuple[int, biz.HubCredential | None, int]:
        """hub 令牌的 sub(pod)必须等于上报的 `hub_pod_name`。

        防拿 A 分片令牌冒充 B 分片心跳 / 伪造在场玩家列表。`require_token=True`:
        纯 DS 回调,没有合法的东西向无令牌调用者,enforce 下无令牌直连一律拒
        (堵旁路,审核 P1)。

        守卫未注入 = Go 的 `dsGuard == nil`,等价 mode=off:放行且无凭据。
        """
        if self._ds_guard is None:
            return 0, None, 0
        return self._ds_guard.check_hub_credential(
            context,
            dsauth.DSScope(ds_type=DS_TYPE_HUB, pod=pod, require_token=True),
        )

    # ── RPC 1:AssignHub ─────────────────────────────────────────────────

    async def AssignHub(self, request, context):  # noqa: N802
        """为玩家分配大厅 DS 分片(login 登录成功后调)。"""
        started_at = time.monotonic()
        if request.player_id == 0:
            plog.get().warning(
                "hub_assign_rejected",
                player_id=request.player_id,
                region=request.region,
                reason=REASON_MISSING_PLAYER_ID,
                code=commonpb.ERR_INVALID_ARG,
            )
            return hubpb.AssignHubResponse(code=commonpb.ERR_INVALID_ARG)
        # R3 join key:team_id / source_match_id 在这里解析成功后补进本请求的日志字段
        # (Go 是写进 ctx 由 plog.With 自动展开,见模块头形变 ①)。player_id 走
        # AuthOptional 面不会自动带,一律逐条手写。
        join = _join_fields(request.team_id, request.source_match_id)
        try:
            # source_match_id:login 三态门证明原对局终局后透传的 Battle→Hub 回流 fence,
            # 盖进 hub 票据 claim(内部控制面调用,信任链与 role_id 同源)。
            # session_jti(R6 复审 P0-3):login 透传的请求方会话 jti,盖进本次 hub
            # 票据的 sjti claim。
            res = await self._uc.assign_hub(
                request.player_id,
                request.region,
                request.team_id,
                request.role_id,
                request.source_match_id,
                request.session_jti,
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透。CancelledError 在 3.8+ 是 BaseException,会被下面那条
            # 宽 except 吞掉 —— 而 grpc.aio 正是**用取消**终止在途 handler。
            # 吞掉的后果:优雅停机时把取消映射成业务错误码并返回正常响应,
            # 客户端每次滚动更新都收到一批假的业务失败,而在途请求也没有真的排空。
            raise
        except BaseException as exc:  # noqa: BLE001 —— 与 Go 同:全部转 in-band code
            code = _to_proto_code(exc)
            plog.get().warning(
                "hub_assign_failed",
                player_id=request.player_id,
                region=request.region,
                role_id=request.role_id,
                has_session_jti=request.session_jti != "",
                reason=_biz_reject_reason(exc),
                code=code,
                elapsed_ms=_elapsed_ms(started_at),
                err=str(exc),
                **join,
            )
            return hubpb.AssignHubResponse(code=code)
        # R1 阶段推进:AssignHub 返回 = 玩家拿到了进 Hub 的权威路由 + 票据。biz 侧
        # hub_assigned 只覆盖"新占座"分支,幂等重签(已有归属)分支不打 —— 这里补一条
        # 覆盖全部成功出口,并带上跨阶段耗时(§11.3 验收判据 5「慢在哪」)。
        plog.get().info(
            "hub_assign_ok",
            player_id=request.player_id,
            region=request.region,
            role_id=request.role_id,
            ds_pod=res.hub_pod_name,
            shard_id=res.shard_id,
            has_ticket=res.hub_ticket != "",
            elapsed_ms=_elapsed_ms(started_at),
            **join,
        )
        return hubpb.AssignHubResponse(
            code=commonpb.OK,
            hub_ds_addr=res.hub_ds_addr,
            hub_ticket=res.hub_ticket,
            hub_pod_name=res.hub_pod_name,
            shard_id=res.shard_id,
        )

    # ── RPC 2:ReleaseHub ────────────────────────────────────────────────

    async def ReleaseHub(self, request, context):  # noqa: N802
        """玩家离开大厅(登出 / 进战斗)。"""
        if request.player_id == 0:
            plog.get().warning(
                "hub_release_rejected",
                player_id=request.player_id,
                reason=REASON_MISSING_PLAYER_ID,
                code=commonpb.ERR_INVALID_ARG,
            )
            return hubpb.ReleaseHubResponse(code=commonpb.ERR_INVALID_ARG)
        try:
            await self._uc.release_hub(request.player_id)
        except asyncio.CancelledError:
            # ★ 取消必须穿透(理由见 AssignHub 同位注释)。
            raise
        except BaseException as exc:  # noqa: BLE001
            code = _to_proto_code(exc)
            plog.get().warning(
                "hub_release_failed",
                player_id=request.player_id,
                reason=_biz_reject_reason(exc),
                code=code,
                err=str(exc),
            )
            return hubpb.ReleaseHubResponse(code=code)
        return hubpb.ReleaseHubResponse(code=commonpb.OK)

    # ── RPC 3:EnsureHubDepartureForBattle(恒拒的死接口)─────────────────

    async def EnsureHubDepartureForBattle(self, request, context):  # noqa: N802
        """placement 路由体系已删除(硬切),旧调用方一律拒绝。

        `ErrServiceDisabled` 不是 `IsServerFault`,access log 落 DEBUG。若还有旧调用方
        在打它(混版部署 / 未升级的 battle 侧),线上必须看得见 —— 所以这里打 Warn。
        """
        plog.get().warning(
            "hub_departure_for_battle_rejected",
            player_id=request.player_id,
            reason=REASON_SERVICE_DISABLED,
            code=errcode.ErrServiceDisabled,
        )
        return hubpb.EnsureHubDepartureForBattleResponse(code=errcode.ErrServiceDisabled)

    # ── RPC 4:TransferHub ───────────────────────────────────────────────

    async def TransferHub(self, request, context):  # noqa: N802
        """跨分片传送(玩家点传送点;后端内部 / DS 调用,不经客户端面)。"""
        if request.player_id == 0:
            plog.get().warning(
                "hub_transfer_rejected",
                player_id=request.player_id,
                target_hub_id=request.target_hub_id,
                reason=REASON_MISSING_PLAYER_ID,
                code=commonpb.ERR_INVALID_ARG,
            )
            return hubpb.TransferHubResponse(code=commonpb.ERR_INVALID_ARG)
        try:
            res = await self._uc.transfer_hub(
                request.player_id, request.target_hub_id, _session_jti(context)
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透(理由见 AssignHub 同位注释)。
            raise
        except BaseException as exc:  # noqa: BLE001
            code = _to_proto_code(exc)
            plog.get().warning(
                "hub_transfer_failed",
                player_id=request.player_id,
                target_hub_id=request.target_hub_id,
                reason=_biz_reject_reason(exc),
                code=code,
                err=str(exc),
            )
            return hubpb.TransferHubResponse(code=code)
        return hubpb.TransferHubResponse(
            code=commonpb.OK,
            new_hub_ds_addr=res.new_hub_ds_addr,
            new_hub_ticket=res.new_hub_ticket,
        )

    # ── RPC 5:ListHubs ──────────────────────────────────────────────────

    async def ListHubs(self, request, context):  # noqa: N802
        """列出分片负载(运维 / 调试)。

        biz 已把 `HubShardStorageRecord` 收敛成 `HubInfo` 最小视图(§5.11):
        令牌代际 / gameserver uid / writer epoch / 各类计数**一律不外发**。
        """
        try:
            hubs = await self._uc.list_hubs(request.region)
        except asyncio.CancelledError:
            # ★ 取消必须穿透(理由见 AssignHub 同位注释)。
            raise
        except BaseException as exc:  # noqa: BLE001
            code = _to_proto_code(exc)
            # R4:成功侧不打(GM / 运维轮询),失败侧必须打 —— 它与 AssignHub 共用
            # list_shards,这里失败往往就是"没有可用 hub"的同源证据。
            plog.get().warning(
                "hub_list_failed",
                region=request.region,
                reason=_biz_reject_reason(exc),
                code=code,
                err=str(exc),
            )
            return hubpb.ListHubsResponse(code=code)
        plog.get().debug("hub_list_ok", region=request.region, hubs=len(hubs))
        return hubpb.ListHubsResponse(code=commonpb.OK, hubs=hubs)

    # ── RPC 6:Heartbeat ─────────────────────────────────────────────────

    async def Heartbeat(self, request, context):  # noqa: N802, C901, PLR0911 —— 与 Go 同形状
        """处理大厅 DS 心跳上报(Hub DS 每 5s 调)。"""
        if request.hub_pod_name == "":
            # R4:心跳是高频路径,成功侧一条都不打;失败侧必须打 —— 一台 DS 报不上心跳
            # 就永远出不了 warming、不可被 AssignHub 选中,玩家侧只表现为 ErrHubNoAvailable。
            plog.get().warning(
                "hub_heartbeat_rejected",
                pod=request.hub_pod_name,
                player_count=request.player_count,
                state=request.state,
                reason=REASON_MISSING_POD,
                code=commonpb.ERR_INVALID_ARG,
            )
            return hubpb.HeartbeatResponse(code=commonpb.ERR_INVALID_ARG)
        # Model B 令牌(带 ds_uid / ds_epoch / ds_gen / jti)→ 非空 cred,走
        # heartbeat_with_credential 的 promote 线性化点(§7);legacy 令牌(仅 ds_gen)
        # → cred=None,走原代际门路径,取 token_gen 透传。off / permissive 下两者均空。
        token_gen, cred, code = self._check_hub_credential(context, request.hub_pod_name)
        if code != 0:
            plog.get().warning(
                "hub_heartbeat_rejected",
                pod=request.hub_pod_name,
                state=request.state,
                reason=REASON_CREDENTIAL_CHECK_FAILED,
                code=code,
            )
            return hubpb.HeartbeatResponse(code=code)
        if cred is None and self._model_b_authority:
            # Model B 权威下**删除 legacy 回退**(审核二轮 CE1/CE2):仅带 legacy 令牌
            # (无 Model B 凭据)的心跳一律拒,不给旧令牌借心跳保活 / 翻 ready。
            # off / permissive 不会进此分支(那时不是 Model B)。
            plog.get().warning(
                "hub_heartbeat_rejected",
                pod=request.hub_pod_name,
                state=request.state,
                token_gen=token_gen,
                reason=REASON_LEGACY_CREDENTIAL_UNDER_MODEL_B,
                code=commonpb.ERR_UNAUTHORIZED,
                hint="DS 只带 legacy 令牌(无 uid/epoch/jti);该 pod 永远翻不到 ready,需重发凭据/重打 DS 包",
            )
            return hubpb.HeartbeatResponse(code=commonpb.ERR_UNAUTHORIZED)
        player_ids = list(request.player_ids)
        try:
            if cred is not None:
                # Model B 权威模式:走 activate_heartbeat 单事务线性化点(stale fail-closed)。
                res = await self._uc.heartbeat_with_credential(
                    request.hub_pod_name,
                    request.player_count,
                    player_ids,
                    request.max_players,
                    request.state,
                    request.ts_ms,
                    cred,
                )
            else:
                res = await self._uc.heartbeat(
                    request.hub_pod_name,
                    request.player_count,
                    request.state,
                    request.ts_ms,
                    token_gen,
                )
        except asyncio.CancelledError:
            # ★ 取消必须穿透(理由见 AssignHub 同位注释)。
            raise
        except BaseException as exc:  # noqa: BLE001
            code = _to_proto_code(exc)
            # R4:心跳成功侧一条不打;失败侧必须打。biz 内部只对部分分支(拓扑未确认)
            # 有日志,activate_heartbeat 的 stale / 相位锁定 fail-closed 返回
            # ErrUnauthorized —— 它不是 IsServerFault,access log 只记 rpc_ok(DEBUG),
            # 线上完全不可见,而它正是"这台 DS 永远翻不到 ready → ErrHubNoAvailable"的根因。
            plog.get().warning(
                "hub_heartbeat_failed",
                ds_pod=request.hub_pod_name,
                state=request.state,
                player_count=request.player_count,
                model_b=cred is not None,
                reason=_biz_reject_reason(exc),
                code=code,
                err=str(exc),
            )
            return hubpb.HeartbeatResponse(code=code)
        # R4:心跳是 5s/台 的高频路径,成功侧一律 Debug。但下发控制指令(drain / stop)
        # 与驱逐单是不可逆的阶段推进(R1),只在非空时升 Info —— 它们只在排空 / 整合 /
        # 顶号清退时出现,频率与分片数同阶,不会冲走 WARN。
        if res.command != "" or res.eviction_orders:
            plog.get().info(
                "hub_heartbeat_command_issued",
                ds_pod=request.hub_pod_name,
                command=res.command,
                grace_seconds=res.grace_seconds,
                eviction_orders=len(res.eviction_orders),
                instance_uid=res.accepted_instance_uid,
                writer_epoch=res.accepted_writer_epoch,
            )
        else:
            plog.get().debug(
                "hub_heartbeat_ok",
                ds_pod=request.hub_pod_name,
                state=request.state,
                player_count=request.player_count,
                accepted_gen=res.accepted_token_gen,
                instance_uid=res.accepted_instance_uid,
            )
        # 在线保活:把心跳捎带的在场 player_ids 转发 locator 续 HUB 位置 TTL
        # (biz 内 fire-and-forget + 独立超时,locator 抖动不拖慢心跳响应)。
        self._uc.refresh_hub_presence(
            request.hub_pod_name, player_ids, _ds_bearer_token(context)
        )
        return hubpb.HeartbeatResponse(
            code=commonpb.OK,
            command=res.command,
            grace_seconds=res.grace_seconds,
            accepted_token_gen=res.accepted_token_gen,
            accepted_token_jti=res.accepted_token_jti,
            accepted_instance_uid=res.accepted_instance_uid,
            accepted_protocol_epoch=res.accepted_protocol_epoch,
            accepted_writer_epoch=res.accepted_writer_epoch,
            eviction_orders=[
                hubpb.HubEvictionOrder(
                    player_id=order.player_id,
                    assignment_id=order.assignment_id,
                    admission_id=order.admission_id,
                    admission_seq=order.admission_seq,
                    source_instance_uid=order.source_instance_uid,
                    source_protocol_epoch=order.source_protocol_epoch,
                    source_writer_epoch=order.source_writer_epoch,
                    cleanup_assignment_id=order.cleanup_assignment_id,
                )
                for order in res.eviction_orders
            ],
        )

    # ── RPC 7:AcknowledgeAdmission ──────────────────────────────────────

    async def AcknowledgeAdmission(self, request, context):  # noqa: N802, C901, PLR0911
        """只接受 :8444 DS callback credential;请求里的 player / assignment 仍须由
        Redis reservation + 当前 active instance identity 二次核验。
        """
        started_at = time.monotonic()
        # local-off-v1:没有 Redis 授权面可消费 reservation,也没有 DS 回调令牌可验
        # (ds_auth.mode=off → 守卫恒返回 cred=None)。走下面的 Model B 分支只会得到
        # INVALID_ARG / UNAUTHORIZED,而 DS 侧把这两个码当"明确拒绝" → FailAdmission
        # → KickPlayer,玩家刚连上大厅就被踢。改走 legacy 准入:仍复核归属三元组,
        # 并推进与 Model B 同一个 owner Admit 完成点。
        if not self._model_b_authority and self._local_admission:
            if request.player_id == 0 or request.assignment_id == "" or request.hub_pod_name == "":
                plog.get().warning(
                    "hub_admission_rejected",
                    player_id=request.player_id,
                    hub_assignment_id=request.assignment_id,
                    ds_pod=request.hub_pod_name,
                    channel=CHANNEL_LOCAL,
                    reason=_local_admission_arg_reason(
                        request.player_id, request.assignment_id, request.hub_pod_name
                    ),
                    code=commonpb.ERR_INVALID_ARG,
                )
                return hubpb.AcknowledgeAdmissionResponse(code=commonpb.ERR_INVALID_ARG)
            try:
                result = await self._uc.acknowledge_local_admission(
                    request.player_id, request.assignment_id, request.hub_pod_name
                )
            except asyncio.CancelledError:
                # ★ 取消必须穿透(理由见 AssignHub 同位注释)。
                raise
            except BaseException as exc:  # noqa: BLE001
                code = _to_proto_code(exc)
                plog.get().warning(
                    "hub_admission_failed",
                    player_id=request.player_id,
                    hub_assignment_id=request.assignment_id,
                    ds_pod=request.hub_pod_name,
                    channel=CHANNEL_LOCAL,
                    reason=_biz_reject_reason(exc),
                    code=code,
                    elapsed_ms=_elapsed_ms(started_at),
                    err=str(exc),
                )
                return hubpb.AcknowledgeAdmissionResponse(code=code)
            _log_admission_outcome(
                request.player_id,
                request.assignment_id,
                request.hub_pod_name,
                "",
                0,
                CHANNEL_LOCAL,
                result.admitted,
                started_at,
            )
            return hubpb.AcknowledgeAdmissionResponse(
                code=commonpb.OK, admitted=result.admitted
            )
        if (
            not self._model_b_authority
            or request.player_id == 0
            or request.assignment_id == ""
            or request.hub_pod_name == ""
            or request.admission_id == ""
            or request.admission_seq == 0
        ):
            # 一个 INVALID_ARG 收敛 6 个条件 → 拆回唯一枚举 reason(R2)。DS 侧只认
            # code==0 才出队,非 0 按 1s 周期无限重试(2026-08-06 事故),必须能一眼
            # 分出是哪个字段。
            plog.get().warning(
                "hub_admission_rejected",
                player_id=request.player_id,
                hub_assignment_id=request.assignment_id,
                ds_pod=request.hub_pod_name,
                admission_id=request.admission_id,
                admission_seq=request.admission_seq,
                channel=CHANNEL_MODEL_B,
                reason=_model_b_admission_arg_reason(
                    self._model_b_authority,
                    request.player_id,
                    request.assignment_id,
                    request.hub_pod_name,
                    request.admission_id,
                    request.admission_seq,
                ),
                code=commonpb.ERR_INVALID_ARG,
            )
            return hubpb.AcknowledgeAdmissionResponse(code=commonpb.ERR_INVALID_ARG)
        _, cred, code = self._check_hub_credential(context, request.hub_pod_name)
        if code != 0:
            plog.get().warning(
                "hub_admission_rejected",
                player_id=request.player_id,
                hub_assignment_id=request.assignment_id,
                ds_pod=request.hub_pod_name,
                admission_id=request.admission_id,
                admission_seq=request.admission_seq,
                channel=CHANNEL_MODEL_B,
                reason=REASON_CREDENTIAL_CHECK_FAILED,
                code=code,
            )
            return hubpb.AcknowledgeAdmissionResponse(code=code)
        if cred is None:
            plog.get().warning(
                "hub_admission_rejected",
                player_id=request.player_id,
                hub_assignment_id=request.assignment_id,
                ds_pod=request.hub_pod_name,
                admission_id=request.admission_id,
                admission_seq=request.admission_seq,
                channel=CHANNEL_MODEL_B,
                reason=REASON_NO_MODEL_B_CREDENTIAL,
                code=commonpb.ERR_UNAUTHORIZED,
            )
            return hubpb.AcknowledgeAdmissionResponse(code=commonpb.ERR_UNAUTHORIZED)
        try:
            result = await self._uc.acknowledge_admission(
                request.player_id,
                request.assignment_id,
                request.hub_pod_name,
                request.admission_id,
                request.admission_seq,
                request.session_jti,
                cred,
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透(理由见 AssignHub 同位注释)。
            raise
        except BaseException as exc:  # noqa: BLE001
            code = _to_proto_code(exc)
            plog.get().warning(
                "hub_admission_failed",
                player_id=request.player_id,
                hub_assignment_id=request.assignment_id,
                ds_pod=request.hub_pod_name,
                admission_id=request.admission_id,
                admission_seq=request.admission_seq,
                channel=CHANNEL_MODEL_B,
                instance_uid=cred.instance_uid,
                writer_epoch=cred.writer_epoch,
                has_session_jti=request.session_jti != "",
                reason=_biz_reject_reason(exc),
                code=code,
                elapsed_ms=_elapsed_ms(started_at),
                err=str(exc),
            )
            return hubpb.AcknowledgeAdmissionResponse(code=code)
        _log_admission_outcome(
            request.player_id,
            request.assignment_id,
            request.hub_pod_name,
            request.admission_id,
            request.admission_seq,
            CHANNEL_MODEL_B,
            result.admitted,
            started_at,
        )
        return hubpb.AcknowledgeAdmissionResponse(code=commonpb.OK, admitted=result.admitted)

    # ── RPC 8:AcknowledgeDeparture ──────────────────────────────────────

    async def AcknowledgeDeparture(self, request, context):  # noqa: N802, C901, PLR0911
        """exact 删除当前 admission owner。

        Conflict 返回 OK + departed=false,让旧连接停止重试且不影响已接管的新连接。
        """
        started_at = time.monotonic()
        # local-off-v1:与 AcknowledgeAdmission 同源的 legacy 离场通道。走下面 Model B
        # 分支只会恒返回 INVALID_ARG(与请求内容无关),而 DS 侧 Departure 队列只认
        # code=0 才出队,结果是每秒一次的永久重试刷屏 + 队列项永不释放(2026-08-06 实测)。
        if not self._model_b_authority and self._local_admission:
            if request.player_id == 0 or request.assignment_id == "" or request.hub_pod_name == "":
                plog.get().warning(
                    "hub_departure_rejected",
                    player_id=request.player_id,
                    hub_assignment_id=request.assignment_id,
                    ds_pod=request.hub_pod_name,
                    channel=CHANNEL_LOCAL,
                    reason=_local_admission_arg_reason(
                        request.player_id, request.assignment_id, request.hub_pod_name
                    ),
                    code=commonpb.ERR_INVALID_ARG,
                )
                return hubpb.AcknowledgeDepartureResponse(code=commonpb.ERR_INVALID_ARG)
            try:
                result = await self._uc.acknowledge_local_departure(
                    request.player_id, request.assignment_id, request.hub_pod_name
                )
            except asyncio.CancelledError:
                # ★ 取消必须穿透(理由见 AssignHub 同位注释)。
                raise
            except BaseException as exc:  # noqa: BLE001
                code = _to_proto_code(exc)
                plog.get().warning(
                    "hub_departure_failed",
                    player_id=request.player_id,
                    hub_assignment_id=request.assignment_id,
                    ds_pod=request.hub_pod_name,
                    channel=CHANNEL_LOCAL,
                    reason=_biz_reject_reason(exc),
                    code=code,
                    elapsed_ms=_elapsed_ms(started_at),
                    err=str(exc),
                )
                return hubpb.AcknowledgeDepartureResponse(code=code)
            plog.get().info(
                "hub_departed",
                player_id=request.player_id,
                hub_assignment_id=request.assignment_id,
                ds_pod=request.hub_pod_name,
                channel=CHANNEL_LOCAL,
                departed=result.departed,
                conflict=False,
                elapsed_ms=_elapsed_ms(started_at),
            )
            return hubpb.AcknowledgeDepartureResponse(
                code=commonpb.OK, departed=result.departed
            )
        if (
            not self._model_b_authority
            or request.player_id == 0
            or request.assignment_id == ""
            or request.hub_pod_name == ""
            or request.admission_id == ""
            or request.admission_seq == 0
        ):
            plog.get().warning(
                "hub_departure_rejected",
                player_id=request.player_id,
                hub_assignment_id=request.assignment_id,
                ds_pod=request.hub_pod_name,
                admission_id=request.admission_id,
                admission_seq=request.admission_seq,
                channel=CHANNEL_MODEL_B,
                reason=_model_b_admission_arg_reason(
                    self._model_b_authority,
                    request.player_id,
                    request.assignment_id,
                    request.hub_pod_name,
                    request.admission_id,
                    request.admission_seq,
                ),
                code=commonpb.ERR_INVALID_ARG,
            )
            return hubpb.AcknowledgeDepartureResponse(code=commonpb.ERR_INVALID_ARG)
        _, cred, code = self._check_hub_credential(context, request.hub_pod_name)
        if code != 0:
            plog.get().warning(
                "hub_departure_rejected",
                player_id=request.player_id,
                hub_assignment_id=request.assignment_id,
                ds_pod=request.hub_pod_name,
                admission_id=request.admission_id,
                admission_seq=request.admission_seq,
                channel=CHANNEL_MODEL_B,
                reason=REASON_CREDENTIAL_CHECK_FAILED,
                code=code,
            )
            return hubpb.AcknowledgeDepartureResponse(code=code)
        if cred is None:
            plog.get().warning(
                "hub_departure_rejected",
                player_id=request.player_id,
                hub_assignment_id=request.assignment_id,
                ds_pod=request.hub_pod_name,
                admission_id=request.admission_id,
                admission_seq=request.admission_seq,
                channel=CHANNEL_MODEL_B,
                reason=REASON_NO_MODEL_B_CREDENTIAL,
                code=commonpb.ERR_UNAUTHORIZED,
            )
            return hubpb.AcknowledgeDepartureResponse(code=commonpb.ERR_UNAUTHORIZED)
        try:
            result = await self._uc.acknowledge_departure(
                request.player_id,
                request.assignment_id,
                request.hub_pod_name,
                request.admission_id,
                request.admission_seq,
                cred,
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透(理由见 AssignHub 同位注释)。
            raise
        except BaseException as exc:  # noqa: BLE001
            code = _to_proto_code(exc)
            plog.get().warning(
                "hub_departure_failed",
                player_id=request.player_id,
                hub_assignment_id=request.assignment_id,
                ds_pod=request.hub_pod_name,
                admission_id=request.admission_id,
                admission_seq=request.admission_seq,
                channel=CHANNEL_MODEL_B,
                instance_uid=cred.instance_uid,
                writer_epoch=cred.writer_epoch,
                reason=_biz_reject_reason(exc),
                code=code,
                elapsed_ms=_elapsed_ms(started_at),
                err=str(exc),
            )
            return hubpb.AcknowledgeDepartureResponse(code=code)
        # R1 阶段推进与 hub_admitted 成对。conflict=true 是旧连接晚到的 Logout(已被新
        # admission 接管),对旧连接是"停止重试"、对新连接零影响,带字段区分。
        plog.get().info(
            "hub_departed",
            player_id=request.player_id,
            hub_assignment_id=request.assignment_id,
            ds_pod=request.hub_pod_name,
            admission_id=request.admission_id,
            admission_seq=request.admission_seq,
            channel=CHANNEL_MODEL_B,
            instance_uid=cred.instance_uid,
            departed=result.departed,
            conflict=result.conflict,
            elapsed_ms=_elapsed_ms(started_at),
        )
        return hubpb.AcknowledgeDepartureResponse(code=commonpb.OK, departed=result.departed)

    # ── RPC 9:ListHubLines(玩家侧)───────────────────────────────────────

    async def ListHubLines(self, request, context):  # noqa: N802
        """列出玩家当前 region 可切换的大厅线路。

        player_id 取自鉴权上下文(JWT sub 权威),请求体里没有该字段。
        """
        player_id = interceptors.extract_player_id(context)
        if player_id == 0:
            plog.get().warning(
                "hub_list_lines_rejected",
                region=request.region,
                reason=REASON_NO_SESSION_PLAYER_ID,
                code=commonpb.ERR_UNAUTHORIZED,
            )
            return hubpb.ListHubLinesResponse(code=commonpb.ERR_UNAUTHORIZED)
        try:
            views = await self._uc.list_hub_lines_for_player(player_id, request.region)
        except asyncio.CancelledError:
            # ★ 取消必须穿透(理由见 AssignHub 同位注释)。
            raise
        except BaseException as exc:  # noqa: BLE001
            code = _to_proto_code(exc)
            # 切线 UI 的数据源。它与 AssignHub 共用 routable_shard_views:这里报错 =
            # 玩家看到空线路列表,与 ErrHubNoAvailable 同根。错误码多为 ErrInvalidState
            # (非 ServerFault)。
            plog.get().warning(
                "hub_list_lines_failed",
                player_id=player_id,
                region=request.region,
                reason=_biz_reject_reason(exc),
                code=code,
                err=str(exc),
            )
            return hubpb.ListHubLinesResponse(code=code)
        # `HubLineView` 已是客户端可见结构(刻意不含 pod 名 / DS 地址,§9 不变量 14),
        # 这里只做 1:1 摊平,不再补任何内部拓扑字段。
        lines = [
            hubpb.HubLine(
                line_no=v.line_no,
                shard_id=v.shard_id,
                player_count=v.player_count,
                capacity=v.capacity,
                is_full=v.is_full,
                is_current=v.is_current,
            )
            for v in views
        ]
        plog.get().debug(
            "hub_list_lines_ok", player_id=player_id, region=request.region, lines=len(lines)
        )
        return hubpb.ListHubLinesResponse(code=commonpb.OK, lines=lines)

    # ── RPC 10:TransferToLine(玩家侧)─────────────────────────────────────

    async def TransferToLine(self, request, context):  # noqa: N802
        """玩家主动切换到指定线路(换实例)。player_id 取自鉴权上下文。"""
        started_at = time.monotonic()
        player_id = interceptors.extract_player_id(context)
        if player_id == 0:
            plog.get().warning(
                "hub_line_transfer_rejected",
                target_shard_id=request.target_shard_id,
                reason=REASON_NO_SESSION_PLAYER_ID,
                code=commonpb.ERR_UNAUTHORIZED,
            )
            return hubpb.TransferToLineResponse(code=commonpb.ERR_UNAUTHORIZED)
        try:
            res = await self._uc.transfer_to_line_for_player(
                player_id, request.target_shard_id, _session_jti(context)
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透(理由见 AssignHub 同位注释)。
            raise
        except BaseException as exc:  # noqa: BLE001
            code = _to_proto_code(exc)
            # 全部拒绝码(ErrHubLineFull / ErrHubTransferCooldown /
            # ErrHubTransferNotInHub / ErrInvalidState)都不是 IsServerFault →
            # access log 只记 DEBUG,必须在这里打。
            plog.get().warning(
                "hub_line_transfer_failed",
                player_id=player_id,
                target_shard_id=request.target_shard_id,
                reason=_biz_reject_reason(exc),
                code=code,
                elapsed_ms=_elapsed_ms(started_at),
                err=str(exc),
            )
            return hubpb.TransferToLineResponse(code=code)
        plog.get().info(
            "hub_line_transferred",
            player_id=player_id,
            target_shard_id=request.target_shard_id,
            new_shard_id=res.new_shard_id,
            line_no=res.line_no,
            elapsed_ms=_elapsed_ms(started_at),
        )
        return hubpb.TransferToLineResponse(
            code=commonpb.OK,
            new_hub_ds_addr=res.new_hub_ds_addr,
            new_hub_ticket=res.new_hub_ticket,
            new_shard_id=res.new_shard_id,
            line_no=res.line_no,
        )


__all__ = ["DS_TYPE_HUB", "HubCredentialGuard", "HubService"]
