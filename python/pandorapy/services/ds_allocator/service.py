"""ds_allocator 的 gRPC service 层 —— 对应 Go 侧
`services/battle/ds_allocator/internal/service/allocator.go`(444 行)与
`internal/server/grpc.go`(36 行,并入本模块末尾的 `register`)。

职责(与 Go 逐条对应,不多不少):
  - 实现 `dsv1.DSAllocatorServiceServicer` 的 7 个 RPC;
  - proto Request/Response ↔ biz 入参/出参互转;
  - `errcode` → `commonv1.ErrCode` 的 1:1 映射(数值相同);
  - 拒绝路径的 reason 日志(infra.md §11.3 R2)。

**本层不做业务判定**。所有 fencing / 权威读写 / 补偿都在 `biz_*`,这里只负责
"把 proto 摊平成 biz 的形状,再把 biz 的错误折成 in-band code"。

═══════════════════════════════════════════════════════════════════════════════
与 Go 的形变(每条都是被迫的,不是口味)
═══════════════════════════════════════════════════════════════════════════════

形变 1:**没有 ctx**。Go 全程透传 `context.Context`,并用 `plog.WithMatchID(ctx, …)`
    把 join key 挂进 ctx,让 biz/data 层日志自动带 `match_id`。Python 侧 `plog` 只
    公开了 `bind_player_id` / `bind_trace_id` 两个 binder(`_match_id` 是私有
    ContextVar,没有公开写入口),所以本层**每条日志显式写 `match_id=`**。
    ★ 不要为此去动 `pandorapy/log.py` 加一个公开 binder:那会改变全仓 20 个服务
      共享件的表面,收益只有本文件少写几个 kwarg。

形变 2:**DS 守卫用 in-band code 而非 error**。Go 的
    `middleware.DSCallbackGuard.CheckBattleCredential` 返回 `(claims, verified, error)`;
    本仓共享件 `pandorapy.dsauth.DSCallbackGuard.check_credential` 返回
    `(claims, credential, code)` —— **第三项是 int 码,不是异常**。
    ★ 千万别在这里包 `try/except`:那道门根本不抛,包了等于永远不进 except,
      而 code!=0 的分支若被写成 `pass` 就是一道空转的鉴权门(dsauth.py 的 `check`
      文档里点名了这个失败模式)。

形变 3:**guard 为 None 时的语义**。Go 的 `s.dsGuard` 是指针,`nil` 时
    `Check`/`CheckBattleCredential` 是 nil 方法调用 —— 实际上 Go 侧 `DSCallbackGuard`
    的方法对 nil receiver 做了放行处理(等价 mode=off)。Python 没有 nil receiver,
    所以本层显式判 `self._ds_guard is None → 放行`,与 `dsauth.guard_from_conf`
    在 mode=off 时返回 None 的约定闭合。

形变 4:**`VerifyWithPayload` 是 async**。`internalrpcauth.Verifier.verify_with_payload`
    要 await(它要消费共享 Redis nonce)。Go 的 `errors.Is(err, ErrUnavailable)`
    在这里是 `isinstance(exc, internalrpcauth.ErrUnavailable)`。

形变 5:**ListBattles 有读取侧上限**。Go 侧 `ListBattles` 无上限(active 集合可达
    千级,一次全量回)。§9 不变量 18 要求列表接口必须有读取侧单次返回上限,故
    Python 侧在 `biz_base.list_battles` 里按 `LIST_BATTLES_MAX` 截断并打
    `battle_list_truncated`。这是**有意的语义加严**,不是移植漏项 —— 本接口是
    运维/调试面,截断不影响任何玩家路径。
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Protocol, runtime_checkable

from pandora.common.v1 import errcode_pb2 as commonpb
from pandora.config.v1 import configtable_pb2_grpc as cfggrpc
from pandora.ds.v1 import allocator_pb2 as dspb
from pandora.ds.v1 import allocator_pb2_grpc as dsgrpc
from pandora.gm.v1 import gm_pb2_grpc as gmgrpc

from pandorapy import battleabort, errcode, internalrpcauth, placement
from pandorapy import log as plog
from pandorapy.services.ds_allocator.battle_auth import (
    BattleCredentialIdentity,
    BattleExpectedInstance,
    BattleResultAuthorizationProof,
)

# DS 令牌里的 ds_type。对应 Go 的 `auth.DSTypeBattle`。
#
# ★ 为什么在这里写字面量而不是从共享件 import:`pandorapy.dsauth` 只做校验侧,
#   它不认识"battle"这个业务常量(签发方是 Go)。hub_allocator 的 service.py 同因
#   在本地声明了 `DS_TYPE_HUB`。两处写死同一份词表是**签发/校验跨语言契约**的一部分,
#   改动必须同时改 Go 的 pkg/auth。
DS_TYPE_BATTLE = "battle"

# ── 拒绝 / 降级 reason 枚举(infra.md §11.3 R2)────────────────────────────────
#
# 为什么必须由业务代码自己打(照抄 Go 头注释的理由):本 service 每个 RPC 都把错误
# 折进响应 Code 后**返回正常响应**,access log 只看得到 rpc_ok;即使抛错,
# ErrInvalidArg / ErrUnauthorized / ErrInvalidState 与全部 fencing 码(>999)也都不属
# `errcode.is_server_fault`,线上默认 info 级下一条都不出。也就是说:不在这里打,
# "matchmaker 说分配失败 / DS 说心跳被拒"在后端日志里**完全没有痕迹**。
#
# ★ 这批字符串是跨语言固定词表(运维照着建 Loki 查询),**逐字节**与 Go 相同,
#   不许改写、不许"顺手统一成更好的措辞"。
REASON_MATCH_ID_REQUIRED = "match_id_required"
REASON_PLAYER_ID_REQUIRED = "player_id_required"
REASON_COMBAT_FACTION_INVALID = "combat_factions_invalid"
REASON_USECASE_FAILED = "usecase_failed"
REASON_RELEASE_REASON_INVALID = "release_reason_not_allowed"
REASON_RELEASE_AUTH_EXP_MISSING = "release_auth_exp_missing"
REASON_ABORT_REQUEST_INCOMPLETE = "abort_request_incomplete"
REASON_ABORT_VERIFIER_UNSET = "abort_verifier_not_configured"
REASON_ABORT_AUTH_UNAVAILABLE = "abort_auth_unavailable"
REASON_ABORT_AUTH_DENIED = "abort_auth_denied"
REASON_DS_CREDENTIAL_REJECTED = "ds_credential_rejected"
REASON_DS_CREDENTIAL_INCOMPLETE = "ds_credential_incomplete"
REASON_SERVICE_DISABLED = "service_disabled"

# 与 Go 的 `dsv1.DSAllocatorService_AbortPreactiveBattle_FullMethodName` 同值。
#
# ★ 它进 `internalrpcauth` 的待签串。写错一个字符 = matchmaker 签的名与本服务验的名
#   不同 → **全部** abort 补偿验签失败,warming 实例只能等 sweep 兜底,而日志上看是
#   "签名不认"(像被攻击),不是"名字写错了"。
ABORT_PREACTIVE_BATTLE_FULL_METHOD = (
    "/pandora.ds.v1.DSAllocatorService/AbortPreactiveBattle"
)

# DS 心跳允许自报的生命周期状态白名单(§9.6:DS 写权限有范围)。
#
# abandoned 是**后端专属判决**(空场超时 / 补偿),绝不接受 DS 自报 —— 否则被攻破 /
# 出 bug 的 DS 可直接把对局推成 abandoned,进而触发 no-show 记罚给玩家铸造处罚,
# 越过后端自己的超时证据。内部 allocation_* fence 态同理只属后端。
DS_REPORTABLE_STATES = frozenset({"warming", "ready", "running", "ended"})


# ── 依赖注入口(Protocol)──────────────────────────────────────────────────────


@runtime_checkable
class BattleCredentialGuard(Protocol):
    """DS 回调令牌守卫的**结构化**接口。`pandorapy.dsauth.DSCallbackGuard` 满足它。

    ★ 用 Protocol 而不是直接标注 `dsauth.DSCallbackGuard`:测试要能塞一个只实现这两个
      方法的假守卫,而不必构造真的 HS256 验签器(那需要一把 32 字节密钥 + 真令牌)。
    ★ 两个方法都返回 **in-band code**(0=放行),不抛异常 —— 见模块头形变 2。
    """

    def check(self, context: Any, scope: Any) -> int: ...

    def check_credential(
        self, context: Any, scope: Any
    ) -> tuple[Any | None, Any | None, int]: ...


# ── 辅助 ──────────────────────────────────────────────────────────────────────


def _to_proto_code(exc: BaseException | None) -> int:
    """`pkg/errcode` → proto enum(数值相同)。对应 Go 的 `toProtoCode`。"""
    return errcode.as_code(exc)


def _elapsed_ms(started_at: float) -> int:
    """单调时钟耗时(毫秒,向零截断)。对应 Go 的 `time.Since(startedAt).Milliseconds()`。

    ★ 必须 `time.monotonic()`:`time.time()` 在 NTP 回拨时会算出负耗时,而这条数字
      正是"玩家进副本卡了多久"的唯一后端证据。
    """
    return int((time.monotonic() - started_at) * 1000)


def combat_faction_map(records: Any) -> dict[int, int] | None:
    """请求侧阵营列表 → match-local 映射。对应 Go 的 `combatFactionMap`。

    ★ 与 `biz_allocate.combat_faction_map_from_records` **不是同一个函数**,别合并:
      那个校验的是**已落盘**记录(要求与 canonical roster 同序同长),这个校验的是
      **入参**(只要求 player_id 非 0 且不重复,顺序/覆盖由 biz 再判)。把入参喂给
      落盘校验器会把合法的乱序请求判成非法。

    Returns:
        `None` = 本次请求没带阵营快照(滚动升级中的旧 matchmaker)。

    Raises:
        ValueError: player_id 缺失或重复。消息逐字与 Go 相同。
    """
    if not records:
        return None
    factions: dict[int, int] = {}
    for record in records:
        if record is None or record.player_id == 0:
            raise ValueError("player combat faction requires player_id")
        if record.player_id in factions:
            raise ValueError("duplicate player combat faction")
        factions[record.player_id] = record.combat_faction_id
    return factions


def sanitize_reported_state(state: str, *, match_id: int) -> str:
    """把 DS 上报的 state 收敛到白名单。对应 Go 的 `sanitizeReportedState`。

    非白名单值(含 abandoned / 乱码)归一化为空串 = 「本跳不更新 state」,两条心跳
    路径都已按 `if state != ""` 处理。于是 abandoned 的唯一来源仍是后端超时判决。
    """
    if state == "":
        return ""
    if state in DS_REPORTABLE_STATES:
        return state
    plog.get().warning(
        "ds_reported_state_rejected",
        match_id=match_id,
        state=state,
        hint="abandoned/internal states are backend-only (§9.6); ignoring DS self-report",
    )
    return ""


# ── AllocatorService ─────────────────────────────────────────────────────────


class AllocatorService(dsgrpc.DSAllocatorServiceServicer):
    """实现 `dsv1.DSAllocatorService` 的 7 个 RPC。对应 Go 的 `AllocatorService`。

    调用方是**后端内部**(matchmaker 调 AllocateBattle/ReleaseBattle/AbortPreactiveBattle、
    战斗 DS 调 Heartbeat、login 代玩家调 ResolveBattleTarget),不是玩家客户端,
    因此不从 context 取 player_id。
    """

    __slots__ = ("_uc", "_ds_guard", "_abort_auth")

    def __init__(self, uc: Any) -> None:
        self._uc = uc
        # nil 等价 mode=off(未配置 ds_auth 的部署零改动)。
        self._ds_guard: BattleCredentialGuard | None = None
        # Matchmaker 专用、绑定完整载荷的破坏性 RPC 验签器;未注入即恒 UNAVAILABLE。
        self._abort_auth: internalrpcauth.Verifier | None = None

    def set_ds_callback_guard(self, guard: BattleCredentialGuard | None) -> None:
        """注入 DS 回调令牌守卫(可选依赖,main 在 ds_auth 已配时调用)。

        对应 Go 的 `SetDSCallbackGuard`。
        """
        self._ds_guard = guard

    def set_allocation_abort_verifier(
        self, verifier: internalrpcauth.Verifier | None
    ) -> None:
        """注入独立的 matchmaker→allocator 服务间验签器。

        对应 Go 的 `SetAllocationAbortVerifier`。它与玩家 JWT、placement proof、
        login resume auth、DS 回调凭据**刻意无关** —— 四套凭据各管一段,任何一套
        被复用都会让爆炸半径外溢。
        """
        self._abort_auth = verifier

    # ── RPC 1:AllocateBattle ─────────────────────────────────────────────

    async def AllocateBattle(self, request, context):  # noqa: N802, ANN001, ARG002
        """为 match 申请战斗 DS(matchmaker 全员确认后调)。"""
        match_id = request.match_id
        if match_id == 0:
            plog.get().warning(
                "battle_allocate_rejected", reason=REASON_MATCH_ID_REQUIRED
            )
            return dspb.AllocateBattleResponse(code=commonpb.ERR_INVALID_ARG)
        try:
            combat_faction_by_player = combat_faction_map(request.player_combat_factions)
        except ValueError as exc:
            plog.get().warning(
                "battle_allocate_rejected",
                reason=REASON_COMBAT_FACTION_INVALID,
                match_id=match_id,
                factions=len(request.player_combat_factions),
                err=str(exc),
            )
            return dspb.AllocateBattleResponse(code=commonpb.ERR_INVALID_ARG)
        # 阶段耗时(§11.3 判据 5「慢在哪」):AllocateBattle 是整条进入链上唯一会阻塞到
        # DS 冷启动的调用(Agones 分配 + ready 等待,生产档上界 120s)。boundary 是唯一
        # 能算出整段耗时的位置 —— biz 内部各段日志各自只覆盖一小截。
        started_at = time.monotonic()
        try:
            # rating_mode 原样透传:allocator 不解释本局算不算段位,只把 matchmaker 定格
            # 的值存进 canonical BattleStorageRecord 供 battle_result 结算时取用。
            res = await self._uc.allocate_battle_with_combat_factions(
                match_id,
                list(request.player_ids),
                combat_faction_by_player,
                request.map_id,
                request.game_mode,
                request.rating_mode,
                request.rating_pool,
            )
        except asyncio.CancelledError:
            # ★ 必须紧邻宽 except 之上:取消是停机/客户端断连的控制流,不是"分配失败"。
            #   吞掉它会让 graceful shutdown 卡在一个已经没人要的分配上。
            raise
        except BaseException as exc:  # noqa: BLE001
            code = _to_proto_code(exc)
            plog.get().warning(
                "battle_allocate_rejected",
                reason=REASON_USECASE_FAILED,
                match_id=match_id,
                code=code,
                players=len(request.player_ids),
                map_id=request.map_id,
                game_mode=request.game_mode,
                elapsed_ms=_elapsed_ms(started_at),
                err=str(exc),
                hint=(
                    "玩家进不去副本:按 match_id 上溯同 trace_id 的 gameserver_allocate_failed /"
                    " battle_ready_wait_* / battle_abandoned_* 定位断点"
                ),
            )
            return dspb.AllocateBattleResponse(code=code)
        plog.get().info(
            "battle_allocate_ok",
            match_id=match_id,
            pod=res.ds_pod_name,
            ds_addr=res.ds_addr,
            uid=res.gameserver_uid,
            epoch=res.instance_epoch,
            allocation_id=res.allocation_id,
            release_track=res.release_track,
            players=len(request.player_ids),
            map_id=request.map_id,
            elapsed_ms=_elapsed_ms(started_at),
        )
        return dspb.AllocateBattleResponse(
            code=commonpb.OK,
            ds_addr=res.ds_addr,
            ds_pod_name=res.ds_pod_name,
            allocated_at_ms=res.allocated_at_ms,
            # DSTicket v2 实例绑定(方案 B):与 ds_addr 同源同快照,matchmaker 签票用。
            gameserver_uid=res.gameserver_uid,
            instance_epoch=res.instance_epoch,
            allocation_id=res.allocation_id,
            release_track=res.release_track,
        )

    # ── RPC 2:ResolveBattleTarget ────────────────────────────────────────

    async def ResolveBattleTarget(self, request, context):  # noqa: N802, ANN001, ARG002
        """只读返回当前可重连目标并核验 roster 成员。

        它与 `AllocateBattle` 分离,确保重签票据**永远不会**产生 GameServerAllocation 副作用。
        """
        # R2:一个 if 收敛了两个条件,必须拆成两个 reason —— 否则"重连重签被拒"查得到、
        # "缺的是 match_id 还是 player_id"查不到。
        if request.match_id == 0:
            plog.get().warning(
                "battle_target_resolve_rejected",
                reason=REASON_MATCH_ID_REQUIRED,
                player_id=request.player_id,
            )
            return dspb.ResolveBattleTargetResponse(code=commonpb.ERR_INVALID_ARG)
        if request.player_id == 0:
            plog.get().warning(
                "battle_target_resolve_rejected",
                reason=REASON_PLAYER_ID_REQUIRED,
                match_id=request.match_id,
            )
            return dspb.ResolveBattleTargetResponse(code=commonpb.ERR_INVALID_ARG)
        # R3 重连链 join key:本面由 login 代玩家调用,context 无玩家 JWT,player_id
        # 必须手写进日志上下文,否则整条重连日志定位不到人。
        #
        # ★ 不 reset:`plog.bind_player_id` 的 Token 由拦截器统一回收(见 log.py 注释),
        #   且 grpc.aio 每个 handler 跑在自己的 Task 上下文副本里,不会串到别的请求。
        plog.bind_player_id(request.player_id)
        try:
            res = await self._uc.resolve_battle_target(
                request.match_id, request.player_id
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            code = _to_proto_code(exc)
            plog.get().warning(
                "battle_target_resolve_rejected",
                reason=REASON_USECASE_FAILED,
                match_id=request.match_id,
                code=code,
                err=str(exc),
                hint=(
                    "玩家重连拿不到 battle 目标,将退化为回大厅;"
                    "判定依据见同 trace_id 的 battle_target_*"
                ),
            )
            return dspb.ResolveBattleTargetResponse(code=code)
        plog.get().info(
            "battle_target_resolve_ok",
            match_id=request.match_id,
            pod=res.ds_pod_name,
            ds_addr=res.ds_addr,
            uid=res.gameserver_uid,
            epoch=res.instance_epoch,
            allocation_id=res.allocation_id,
            release_track=res.release_track,
        )
        return dspb.ResolveBattleTargetResponse(
            code=commonpb.OK,
            ds_addr=res.ds_addr,
            ds_pod_name=res.ds_pod_name,
            allocated_at_ms=res.allocated_at_ms,
            gameserver_uid=res.gameserver_uid,
            instance_epoch=res.instance_epoch,
            allocation_id=res.allocation_id,
            release_track=res.release_track,
        )

    # ── RPC 3:ReleaseBattle ──────────────────────────────────────────────

    async def ReleaseBattle(self, request, context):  # noqa: N802, ANN001, ARG002, C901, PLR0911
        """回收战斗 DS(对局结束 / 异常)。"""
        match_id = request.match_id
        if match_id == 0:
            plog.get().warning(
                "battle_release_rejected",
                reason=REASON_MATCH_ID_REQUIRED,
                release_reason=request.reason,
            )
            return dspb.ReleaseBattleResponse(code=commonpb.ERR_INVALID_ARG)
        if self._uc.redis_authority_enabled():
            # R2:原来一个 if 收敛了两件完全不同的事(调用方用了不被允许的 reason /
            # 结算证明缺凭据过期时刻),拆成两个 reason —— 前者是调用方走错口,
            # 后者是 battle_result 的授权证明没带齐,排查方向不同。
            if request.reason not in ("completed", "completed-finalize"):
                plog.get().warning(
                    "battle_release_rejected",
                    reason=REASON_RELEASE_REASON_INVALID,
                    match_id=match_id,
                    release_reason=request.reason,
                    pod=request.ds_pod_name,
                    hint=(
                        "Model B 正常结算只接受 completed / completed-finalize;"
                        "abandoned 由内部 sweep 回收"
                    ),
                )
                return dspb.ReleaseBattleResponse(code=commonpb.ERR_INVALID_ARG)
            if request.auth_exp_ms <= 0:
                plog.get().warning(
                    "battle_release_rejected",
                    reason=REASON_RELEASE_AUTH_EXP_MISSING,
                    match_id=match_id,
                    release_reason=request.reason,
                    pod=request.ds_pod_name,
                    uid=request.gameserver_uid,
                    epoch=request.instance_epoch,
                )
                return dspb.ReleaseBattleResponse(code=commonpb.ERR_INVALID_ARG)
            expected = BattleExpectedInstance(
                allocation_id=request.allocation_id,
                instance_uid=request.gameserver_uid,
                instance_epoch=request.instance_epoch,
            )
            proof = BattleResultAuthorizationProof(
                credential=BattleCredentialIdentity(
                    pod_name=request.ds_pod_name,
                    instance_uid=request.gameserver_uid,
                    instance_epoch=request.instance_epoch,
                    gen=request.auth_gen,
                    jti=request.auth_jti,
                    exp_ms=request.auth_exp_ms,
                    kid=request.auth_kid,
                    token_sha256=request.auth_token_sha256,
                    writer_epoch=request.auth_writer_epoch,
                ),
                authorized_at_ms=request.authorized_at_ms,
            )
            try:
                if request.reason == "completed":
                    await self._uc.release_battle_expected(
                        match_id, request.reason, request.ds_pod_name, expected, proof
                    )
                else:
                    await self._uc.finalize_battle_release_expected(
                        match_id, request.ds_pod_name, expected, proof
                    )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                return self._release_failed(request, exc)
        else:
            try:
                await self._uc.release_battle(match_id, request.reason)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                return self._release_failed(request, exc)
        return dspb.ReleaseBattleResponse(code=commonpb.OK)

    def _release_failed(self, request, exc: BaseException):  # noqa: ANN001, ANN202
        """两条回收路径共用的失败日志 + 应答。

        ★ 抽出来只是为了不把同一段 10 行日志抄两遍;字段与 Go 的单一 `if err != nil`
          分支逐字相同,**没有**新增或省略任何一项。
        """
        code = _to_proto_code(exc)
        plog.get().warning(
            "battle_release_rejected",
            reason=REASON_USECASE_FAILED,
            match_id=request.match_id,
            code=code,
            release_reason=request.reason,
            pod=request.ds_pod_name,
            uid=request.gameserver_uid,
            epoch=request.instance_epoch,
            allocation_id=request.allocation_id,
            err=str(exc),
            hint="DS 未被回收:调用方会重试,持续失败则 pod 占位泄漏,查同 match_id 的 sweep 链",
        )
        return dspb.ReleaseBattleResponse(code=code)

    # ── RPC 4:AbortPreactiveBattle ───────────────────────────────────────

    async def AbortPreactiveBattle(self, request, context):  # noqa: N802, ANN001, C901
        """已签名的分配 saga 补偿入口。

        鉴权绑定 **canonical 全量请求体**并消费一枚共享 Redis nonce,之后 usecase 才
        允许 fence Redis 或触碰 Kubernetes。
        """
        abort_request = battleabort.Request(
            match_id=request.match_id,
            operation_id=request.allocation_operation_id,
            target=placement.Target(
                pod_name=request.ds_pod_name,
                instance_uid=request.gameserver_uid,
                instance_epoch=request.instance_epoch,
                allocation_id=request.allocation_id,
                release_track=request.release_track,
            ),
        )
        target = abort_request.target
        if not abort_request.complete():
            plog.get().warning(
                "battle_allocation_abort_rejected",
                reason=REASON_ABORT_REQUEST_INCOMPLETE,
                match_id=abort_request.match_id,
                operation_id=abort_request.operation_id,
                pod=target.pod_name,
                uid=target.instance_uid,
                epoch=target.instance_epoch,
                allocation_id=target.allocation_id,
                release_track=target.release_track,
            )
            return dspb.AbortPreactiveBattleResponse(code=commonpb.ERR_INVALID_ARG)
        if self._abort_auth is None:
            # Error 级(不是 Warn):这是**部署缺件**,不是调用方错误。整条补偿链停摆。
            plog.get().error(
                "battle_allocation_abort_rejected",
                reason=REASON_ABORT_VERIFIER_UNSET,
                match_id=abort_request.match_id,
                operation_id=abort_request.operation_id,
                hint="matchmaker 的分配 saga 补偿无法执行,warming 实例只能等 sweep 兜底回收",
            )
            return dspb.AbortPreactiveBattleResponse(code=commonpb.ERR_UNAVAILABLE)
        try:
            await self._abort_auth.verify_with_payload(
                _metadata_dict(context),
                ABORT_PREACTIVE_BATTLE_FULL_METHOD,
                abort_request.match_id,
                abort_request.canonical(),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # R2:验签失败的两种结局(依赖不可用 / 签名不认)错误码不同、处置也不同,
            # 分两个 reason。
            if isinstance(exc, internalrpcauth.ErrUnavailable):
                plog.get().warning(
                    "battle_allocation_abort_rejected",
                    reason=REASON_ABORT_AUTH_UNAVAILABLE,
                    match_id=abort_request.match_id,
                    operation_id=abort_request.operation_id,
                    err=str(exc),
                )
                return dspb.AbortPreactiveBattleResponse(code=commonpb.ERR_UNAVAILABLE)
            plog.get().warning(
                "battle_allocation_abort_rejected",
                reason=REASON_ABORT_AUTH_DENIED,
                match_id=abort_request.match_id,
                operation_id=abort_request.operation_id,
                pod=target.pod_name,
                err=str(exc),
            )
            return dspb.AbortPreactiveBattleResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            await self._uc.abort_preactive_battle(abort_request)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            code = _to_proto_code(exc)
            plog.get().warning(
                "battle_allocation_abort_rejected",
                reason=REASON_USECASE_FAILED,
                match_id=abort_request.match_id,
                code=code,
                operation_id=abort_request.operation_id,
                pod=target.pod_name,
                allocation_id=target.allocation_id,
                err=str(exc),
            )
            return dspb.AbortPreactiveBattleResponse(code=code)
        return dspb.AbortPreactiveBattleResponse(code=commonpb.OK)

    # ── RPC 5:EnsurePlayerDeparture(已硬切删除)──────────────────────────

    async def EnsurePlayerDeparture(self, request, context):  # noqa: N802, ANN001, ARG002
        """placement 路由体系已删除(硬切),旧调用方一律拒绝。

        ★ 这里**必须**打日志:已删除的 RPC 仍被调用 = 有一个没跟上硬切的旧调用方。
          不打的话,调用方那边只看到一个业务码,而这一侧完全无痕,没人知道"谁还在调"。
        """
        plog.get().warning(
            "battle_departure_rpc_rejected",
            reason=REASON_SERVICE_DISABLED,
            match_id=request.match_id,
            player_id=request.player_id,
            pod=request.ds_pod_name,
            hint="placement 路由体系已硬切删除;调用方需改走 Heartbeat 携带 census 的离场闭环",
        )
        return dspb.EnsurePlayerDepartureResponse(code=errcode.ErrServiceDisabled)

    # ── RPC 6:Heartbeat ──────────────────────────────────────────────────

    async def Heartbeat(self, request, context):  # noqa: N802, ANN001, C901, PLR0911, PLR0912
        """处理战斗 DS 心跳上报(DS 每 5s 调)。"""
        match_id = request.match_id
        if match_id == 0:
            plog.get().warning(
                "battle_heartbeat_rejected",
                reason=REASON_MATCH_ID_REQUIRED,
                pod=request.ds_pod_name,
            )
            return dspb.HeartbeatResponse(code=commonpb.ERR_INVALID_ARG)
        reported_state = sanitize_reported_state(request.state, match_id=match_id)
        if self._uc.redis_authority_enabled():
            # Model B 必须同时绑定 match+pod,并要求完整 JWT credential;legacy/None
            # credential 不允许在 permissive 语义下回退,任何 Redis 副作用前即拒绝。
            _, verified, code = self._check_credential(
                context,
                _ds_scope(
                    match_id=match_id, pod=request.ds_pod_name, require_token=True
                ),
            )
            if code != 0:
                # DS 凭据被拒时后端此前完全无痕(fencing 码 >999 与 ErrUnauthorized 在
                # access log 里都只落 rpc_ok=DEBUG),排查只能靠 DS 侧日志。
                # 这一条是"DS 明明在跑却像没心跳"类故障的唯一后端证据。
                plog.get().warning(
                    "battle_heartbeat_rejected",
                    reason=REASON_DS_CREDENTIAL_REJECTED,
                    match_id=match_id,
                    code=code,
                    pod=request.ds_pod_name,
                    state=request.state,
                    player_count=request.player_count,
                    hint="该 DS 的心跳不会推进任何权威状态,15s 后会被 sweep 判弃",
                )
                return dspb.HeartbeatResponse(code=code)
            if verified is None or verified.exp_ms <= 0:
                plog.get().warning(
                    "battle_heartbeat_rejected",
                    reason=REASON_DS_CREDENTIAL_INCOMPLETE,
                    match_id=match_id,
                    pod=request.ds_pod_name,
                    verified=verified is not None,
                )
                return dspb.HeartbeatResponse(code=commonpb.ERR_UNAUTHORIZED)
            ident = BattleCredentialIdentity(
                pod_name=verified.pod,
                instance_uid=verified.instance_uid,
                instance_epoch=verified.protocol_epoch,
                gen=verified.gen,
                jti=verified.jti,
                exp_ms=verified.exp_ms,
                kid=verified.kid,
                token_sha256=verified.token_sha256,
                writer_epoch=verified.writer_epoch,
            )
            try:
                res = await self._uc.heartbeat_authorized_with_players(
                    match_id,
                    ident,
                    request.player_count,
                    reported_state,
                    request.ts_ms,
                    request.active_player_snapshot_present,
                    request.player_census_capability_version,
                    request.player_census_id,
                    list(request.active_player_ids),
                    list(request.acknowledged_departure_ids),
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                return self._heartbeat_failed(request, reported_state, exc)
        else:
            # Legacy/off 灰度路径保持既有范围语义;Model B 开启后不会落到这里。
            code = self._check(
                context, _ds_scope(match_id=match_id, require_token=True)
            )
            if code != 0:
                plog.get().warning(
                    "battle_heartbeat_rejected",
                    reason=REASON_DS_CREDENTIAL_REJECTED,
                    match_id=match_id,
                    code=code,
                    pod=request.ds_pod_name,
                    authority="legacy",
                )
                return dspb.HeartbeatResponse(code=code)
            try:
                # census 一并透传:legacy 面据此续 owner 实例租约并代提交在场玩家 Admit
                # (不传则 owner 恒 PENDING、租约恒过期,客户端永远等不到 STABLE)。
                res = await self._uc.heartbeat_with_census(
                    match_id,
                    request.ds_pod_name,
                    request.player_count,
                    reported_state,
                    request.ts_ms,
                    request.active_player_snapshot_present,
                    list(request.active_player_ids),
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                return self._heartbeat_failed(request, reported_state, exc)
        # R4:心跳是高频路径,成功侧一律 debug(LOG_LEVEL=debug 可对单 pod 临时全开)。
        # 有它才能回答"这台 DS 到底有没有在上报、上报的在场人数是多少"。
        plog.get().debug(
            "battle_heartbeat_accepted",
            match_id=match_id,
            pod=request.ds_pod_name,
            state=reported_state,
            player_count=request.player_count,
            census_present=request.active_player_snapshot_present,
            census=len(request.active_player_ids),
            command=res.command,
            eviction_orders=len(res.eviction_orders),
        )
        return dspb.HeartbeatResponse(
            code=commonpb.OK,
            command=res.command,
            accepted_token_gen=res.accepted_token_gen,
            accepted_token_jti=res.accepted_token_jti,
            accepted_instance_uid=res.accepted_instance_uid,
            accepted_instance_epoch=res.accepted_instance_epoch,
            accepted_writer_epoch=res.accepted_writer_epoch,
            eviction_orders=res.eviction_orders,
        )

    def _heartbeat_failed(self, request, reported_state: str, exc: BaseException):  # noqa: ANN001, ANN202
        """两条心跳路径共用的失败日志 + 应答(字段与 Go 单一分支逐字相同)。"""
        code = _to_proto_code(exc)
        plog.get().warning(
            "battle_heartbeat_rejected",
            reason=REASON_USECASE_FAILED,
            match_id=request.match_id,
            code=code,
            pod=request.ds_pod_name,
            state=reported_state,
            player_count=request.player_count,
            census_present=request.active_player_snapshot_present,
            census=len(request.active_player_ids),
            err=str(exc),
        )
        return dspb.HeartbeatResponse(code=code)

    # ── RPC 7:ListBattles ────────────────────────────────────────────────

    async def ListBattles(self, request, context):  # noqa: N802, ANN001, ARG002
        """列出当前战斗实例(运维 / 调试面)。

        ★ 返回的是 biz 组装好的 `BattleInfo`(6 个字段的最小视图),**不是**
          `BattleStorageRecord`(§9 不变量 14:权威快照绝不原样外露 —— 那里面有
          roster、凭据代际、writer_epoch、pod_uid 等一整套 fencing 事实)。
        ★ 单次返回上限由 `biz_base.LIST_BATTLES_MAX` 兜住(§9 不变量 18)。
        """
        try:
            battles = await self._uc.list_battles(request.state_filter)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            code = _to_proto_code(exc)
            plog.get().warning(
                "battle_list_rejected",
                reason=REASON_USECASE_FAILED,
                code=code,
                state_filter=request.state_filter,
                err=str(exc),
            )
            return dspb.ListBattlesResponse(code=code)
        return dspb.ListBattlesResponse(code=commonpb.OK, battles=battles)

    # ── DS 守卫的两个薄封装(统一 None-guard 语义)────────────────────────

    def _check(self, context, scope) -> int:  # noqa: ANN001
        """`guard.check` 的 None 安全封装。None guard 等价 mode=off(放行)。"""
        if self._ds_guard is None:
            return 0
        return self._ds_guard.check(context, scope)

    def _check_credential(self, context, scope):  # noqa: ANN001, ANN202
        """`guard.check_credential` 的 None 安全封装。

        ★ None guard 返回 `(None, None, 0)` —— 放行但**没有** credential。
          Model B 分支随后的 `verified is None` 判据会把它拒成
          `ds_credential_incomplete`,与 Go 侧 nil guard 的结局一致:
          开了 Redis 权威却没配 ds_auth,心跳一律拒,而不是无凭据放行。
        """
        if self._ds_guard is None:
            return None, None, 0
        return self._ds_guard.check_credential(context, scope)


# ── 与 dsauth 的胶水 ─────────────────────────────────────────────────────────


def _ds_scope(*, match_id: int, pod: str = "", require_token: bool = False):  # noqa: ANN202
    """构造 `dsauth.DSScope`。延迟 import 见函数体注释。"""
    from pandorapy import dsauth  # noqa: PLC0415 —— 见下

    # ★ 为什么延迟 import:`pandorapy.dsauth` 依赖 PyJWT。本模块的其余部分(以及
    #   `register`)在没有 ds_auth 配置的部署里也要能被 import;把 jwt 抬到模块顶层
    #   会让"未配 ds_auth 的服务"因为缺一个只有验签才用得到的三方包而起不来。
    return dsauth.DSScope(
        ds_type=DS_TYPE_BATTLE,
        match_id=match_id,
        pod=pod,
        require_token=require_token,
    )


def _metadata_dict(context) -> dict[str, str]:  # noqa: ANN001
    """把 grpc metadata 摊成 dict,供 `internalrpcauth.Verifier` 消费。

    与 `services/matchmaker/service.py` 的同名函数逐字相同(那边的注释解释了为什么
    `-bin` 头直接跳过、以及重复 key 取最后一个与 Go `single()` 的差异)。
    """
    out: dict[str, str] = {}
    for key, value in context.invocation_metadata() or ():
        if isinstance(value, bytes):
            continue
        out[key.lower()] = value
    return out


# ── 服务注册(对应 Go 的 internal/server/grpc.go)────────────────────────────


def register(
    server,  # noqa: ANN001 —— grpc.aio.Server
    svc: AllocatorService,
    gm_svc: Any,
    ct_admin: Any = None,
) -> list[str]:
    """把三个 servicer 注册到 gRPC server。对应 Go 的 `NewGRPCServer`。

    Args:
        server: `grpc.aio.Server`。
        svc: `AllocatorService`(7 个 RPC)。
        gm_svc: `gm.GmService`(3 个 RPC)。
        ct_admin: 配置表热更 admin(1 个 RPC);**None 时不注册** —— 与 Go 的
            `if ctAdmin != nil` 逐字对应。没配置表的部署不该暴露一个恒失败的
            reload 端点。

    Returns:
        已注册的 service full name 列表,供调用方喂给 `server.enable_reflection`。
        ★ 返回而不是在这里开 reflection:开不开由 `cfg.Server.Grpc.EnableReflection`
          决定(dev 开、prod 关),那是 main 的判断,不是注册函数的。
    """
    dsgrpc.add_DSAllocatorServiceServicer_to_server(svc, server)
    gmgrpc.add_GmServiceServicer_to_server(gm_svc, server)
    names = ["pandora.ds.v1.DSAllocatorService", "pandora.gm.v1.GmService"]
    if ct_admin is not None:
        cfggrpc.add_ConfigTableAdminServiceServicer_to_server(ct_admin, server)
        names.append("pandora.config.v1.ConfigTableAdminService")
    return names


__all__ = [
    "ABORT_PREACTIVE_BATTLE_FULL_METHOD",
    "DS_REPORTABLE_STATES",
    "DS_TYPE_BATTLE",
    "AllocatorService",
    "BattleCredentialGuard",
    "combat_faction_map",
    "register",
    "sanitize_reported_state",
]
