"""login 的出向 gRPC 客户端 —— 对应 Go 侧 `internal/data/{locator,hub,player}_client.go`。

三条出向依赖的强弱**刻意不同**,弄反任何一条都会造成方向性错误:

    locator  弱依赖(addr 空 → 不调);但 require_hub_assignment_binding=true 时
             它是 Hub 分配前的**权威门**,LOGIN_PENDING 写失败必须 fail-closed。
    hub      弱依赖(addr 空 → 回退自签 hub 票 + mock_hub_ds_addr);
             ds_ticket v2 启用时**升级为强依赖**(v2 下自签 HS256 hub 票会被 DS 全拒,
             属半完成配置,main 直接拒启)。
    player   纯弱依赖(addr 空 → 不播种,角色名回落 player 默认前缀名)。
             对端还没滚上 EnsureProfile 时必须把 gRPC `Unimplemented` 翻成
             ErrNotImplemented —— 「这个版本没有」与「暂时不可用」必须能被调用方区分开,
             否则要么当故障刷告警,要么当可重试白白重试到超时(§9.21)。
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.hub.v1 import allocator_pb2, allocator_pb2_grpc
from pandora.locator.v1 import locator_pb2, locator_pb2_grpc
from pandora.match.v1 import match_pb2, match_pb2_grpc
from pandora.owner.v1 import owner_pb2, owner_pb2_grpc
from pandora.player.v1 import player_pb2, player_pb2_grpc

from pandorapy import errcode
from pandorapy import internalrpcauth

#: matchmaker 只读权威的 gRPC full method 名。它是 internalrpcauth 签名载荷的一部分,
#: 与 matchmaker 侧校验用的字符串**必须逐字相同**(签的是方法名,不是 stub 对象);
#: 写错不会报 "method not found",只会全量 ERR_PERMISSION_DENY。
MATCH_RESOLVE_PLAYER_MATCH_CONTEXT_METHOD = (
    "/pandora.match.v1.MatchService/ResolvePlayerMatchContext"
)

# locator 在登录链上的独立子预算。prod 登录总 deadline 5s,MySQL/Redis 基线已占 1~2s,
# 链上还有 matchmaker 探测与 AssignHub;locator 慢化(GC/failover)时若无子预算,
# 单腿就能吃光整条链让登录整体超时。取值 = locator 健康 P99(几十 ms)的 ~40 倍保守值。
LOCATOR_PROBE_TIMEOUT_SEC = 2.0

# 写者继任短重试(Go: assignHubMaxAttempts / assignHubRetryBackoff)。
# hub_allocator 是单写者,正常滚动交接(主动 Resign)为亚秒级,登录路径就地短退避即可
# 吸收;崩溃接任(lease TTL 到期,数秒~15s)**不在登录里等**,重试耗尽把 ErrUnavailable
# 交回 biz(回退自签或报错)—— 在登录里死等会把「一个副本交接」放大成「全服登录卡住」。
ASSIGN_HUB_MAX_ATTEMPTS = 3
ASSIGN_HUB_RETRY_BACKOFF_SEC = 0.15


@dataclasses.dataclass(slots=True)
class BattleLocation:
    """玩家当前 BATTLE 位置的最小快照(断线重连检测用)。

    只有 state==BATTLE 且 match_id!=0 且 battle_pod!="" 才认定 in_battle;
    其余一律 False —— presence 是 30s TTL 投影,**key miss 不能证明玩家已离开旧 DS**
    (§9.22),所以 in_battle=False 只是"没看见",不是"确定不在"。
    """

    in_battle: bool = False
    match_id: int = 0
    battle_addr: str = ""
    presence_state: int = 0


class GrpcLocationNotifier:
    """login → player_locator。"""

    __slots__ = ("_channel", "_stub")

    def __init__(self, addr: str) -> None:
        # insecure:内网直连,与 Go 的 grpcclient.MustDialInsecure 一致。
        self._channel = grpc.aio.insecure_channel(addr)
        self._stub = locator_pb2_grpc.PlayerLocatorServiceStub(self._channel)

    async def close(self) -> None:
        with contextlib.suppress(Exception):
            await self._channel.close()

    async def notify_login_pending(self, player_id: int, device_id: str) -> None:
        """SetLocation(state=LOGIN_PENDING)。

        device_id 当前不参与 LOGIN_PENDING(与 Go 同,参数保留便于以后扩展)。
        """
        del device_id
        req = locator_pb2.SetLocationRequest(
            player_id=player_id,
            location=locator_pb2.Location(
                state=locator_pb2.LOCATION_STATE_LOGIN_PENDING
            ),
        )
        try:
            resp = await self._stub.SetLocation(req, timeout=LOCATOR_PROBE_TIMEOUT_SEC)
        except asyncio.CancelledError:
            raise
        except grpc.aio.AioRpcError as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, "locator SetLocation rpc: %s", exc
            ) from exc
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                int(resp.code), "locator SetLocation code=%d", int(resp.code)
            )

    async def get_battle_location(self, player_id: int) -> BattleLocation:
        try:
            resp = await self._stub.GetLocation(
                locator_pb2.GetLocationRequest(player_id=player_id),
                timeout=LOCATOR_PROBE_TIMEOUT_SEC,
            )
        except asyncio.CancelledError:
            raise
        except grpc.aio.AioRpcError as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, "locator GetLocation rpc: %s", exc
            ) from exc
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                int(resp.code), "locator GetLocation code=%d", int(resp.code)
            )
        loc = resp.location
        if (
            loc.state != locator_pb2.LOCATION_STATE_BATTLE
            or loc.match_id == 0
            or not loc.battle_pod
        ):
            return BattleLocation(presence_state=int(loc.state), match_id=int(loc.match_id))
        return BattleLocation(
            in_battle=True,
            match_id=int(loc.match_id),
            battle_addr=loc.battle_pod,
            presence_state=int(loc.state),
        )


@dataclasses.dataclass(slots=True)
class HubAssignment:
    hub_ds_addr: str = ""
    hub_ticket: str = ""
    hub_pod_name: str = ""
    shard_id: int = 0


class GrpcHubAssigner:
    """login → hub_allocator。hub_allocator 是 hub 票据的**唯一签发权威**。"""

    __slots__ = ("_channel", "_stub")

    def __init__(self, addr: str) -> None:
        # ★ round_robin:hub_allocator 是单写者,普通 ClusterIP 直连被 L4 钉在某一 Pod,
        # 落到非-writer 副本就永远拿 ErrWriterSuperseded(可重试)。生产 addr 配成
        # dns:///hub-allocator-headless.<ns>.svc:<port>(headless Service,DNS 返回全部
        # Pod IP)时,下面的就地重试每次轮到不同副本,数次内命中当前 writer;
        # dev 单静态 addr(passthrough 单后端)下退化为单后端,行为不变。
        self._channel = grpc.aio.insecure_channel(
            addr, options=[("grpc.lb_policy_name", "round_robin")]
        )
        self._stub = allocator_pb2_grpc.HubAllocatorServiceStub(self._channel)

    async def close(self) -> None:
        with contextlib.suppress(Exception):
            await self._channel.close()

    async def assign_hub(
        self,
        player_id: int,
        region: str,
        team_id: int,
        role_id: int,
        source_match_id: int,
        session_jti: str,
    ) -> HubAssignment:
        req = allocator_pb2.AssignHubRequest(
            player_id=player_id,
            region=region,
            team_id=team_id,
            role_id=role_id,
            source_match_id=source_match_id,
            session_jti=session_jti,
        )
        last_err: BaseException | None = None
        for attempt in range(ASSIGN_HUB_MAX_ATTEMPTS):
            if attempt > 0:
                await asyncio.sleep(ASSIGN_HUB_RETRY_BACKOFF_SEC)
            try:
                resp = await self._stub.AssignHub(req)
            except asyncio.CancelledError:
                raise
            except grpc.aio.AioRpcError as exc:
                if exc.code() == grpc.StatusCode.UNAVAILABLE:
                    last_err = errcode.PandoraError(
                        errcode.ErrUnavailable,
                        "hub_allocator AssignHub transport unavailable: %s",
                        exc,
                    )
                    continue
                raise errcode.PandoraError(
                    errcode.ErrInternal, "hub_allocator AssignHub rpc: %s", exc
                ) from exc
            if resp.code != errcode_pb2.OK:
                code_err = errcode.PandoraError(
                    int(resp.code), "hub_allocator AssignHub code=%d", int(resp.code)
                )
                if int(resp.code) == errcode.ErrUnavailable:
                    last_err = code_err
                    continue
                raise code_err
            return HubAssignment(
                hub_ds_addr=resp.hub_ds_addr,
                hub_ticket=resp.hub_ticket,
                hub_pod_name=resp.hub_pod_name,
                shard_id=int(resp.shard_id),
            )
        raise last_err if last_err is not None else errcode.PandoraError(
            errcode.ErrUnavailable, "hub_allocator AssignHub exhausted retries"
        )


@dataclasses.dataclass(slots=True)
class SeededProfile:
    created: bool = False
    nickname: str = ""
    level: int = 0


class GrpcProfileSeeder:
    """login → player。把账号名播种成**全服显示名**(players.nickname)。

    只在 LoginResponse 里下发一个名字是不够的:那只有本人看得见,别人在头顶铭牌 /
    队伍面板 / 聊天 / 好友 / 公会 / 排行榜里看到的仍是 player 的默认前缀名。
    """

    __slots__ = ("_channel", "_stub")

    def __init__(self, addr: str) -> None:
        self._channel = grpc.aio.insecure_channel(addr)
        self._stub = player_pb2_grpc.PlayerServiceStub(self._channel)

    async def close(self) -> None:
        with contextlib.suppress(Exception):
            await self._channel.close()

    async def ensure_profile(
        self, player_id: int, nickname: str, *, timeout_sec: float
    ) -> SeededProfile:
        try:
            resp = await self._stub.EnsureProfile(
                player_pb2.EnsureProfileRequest(player_id=player_id, nickname=nickname),
                timeout=timeout_sec,
            )
        except asyncio.CancelledError:
            raise
        except grpc.aio.AioRpcError as exc:
            if exc.code() == grpc.StatusCode.UNIMPLEMENTED:
                # §9.21 弱依赖降级的关键一步:对端**这个版本**还没有这个 method
                # (滚动升级期的预期状态),重试永远不会成功。
                raise errcode.PandoraError(
                    errcode.ErrNotImplemented,
                    "player EnsureProfile not implemented on peer version",
                ) from exc
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "player EnsureProfile rpc: %s", exc
            ) from exc
        if resp.code != errcode_pb2.OK:
            # 业务码原样透出(尤其 ERR_PLAYER_NICKNAME_TAKEN):调用方按码分流日志与降级。
            raise errcode.PandoraError(
                int(resp.code),
                "player EnsureProfile code=%d player_id=%d",
                int(resp.code),
                player_id,
            )
        return SeededProfile(
            created=bool(resp.created),
            nickname=resp.effective_nickname,
            level=int(resp.level),
        )


# ── owner 权威(§9.23 query-first)────────────────────────────────────────────
#
# login 只需要 Query + Release(登出释放,compare-delete 自己);Begin / Admit 归
# hub_allocator(签票统一出口)与 ds_allocator(READY 交付 / census)。

# 单次 owner 调用上限。登出链路是弱依赖,快速失败放行;进场查询失败按 §9.22 走
# WAIT/UNKNOWN —— 两条路径都不能因为 owner 慢而把请求整条挂住。
OWNER_RPC_TIMEOUT_SEC = 2.0


@dataclasses.dataclass(slots=True)
class OwnerPlacementView:
    """§9.23 query-first 所需的完整 owner placement 快照。

    owner_type / phase 用整数对齐 owner.proto 枚举
    (owner_type 0 none / 1 hub / 2 battle;phase 1 pending / 2 admitted)。
    """

    owner_epoch: int = 0
    owner_type: int = 0
    phase: int = 0
    pod_name: str = ""
    instance_uid: str = ""
    instance_epoch: int = 0
    assignment_or_allocation_id: str = ""
    release_track: str = ""
    operation_id: str = ""
    admit_not_before_ms: int = 0
    lease_deadline_ms: int = 0


class GrpcOwnerClient:
    """login → owner 权威。Query(query-first 路由 + 登出 compare 依据)+ Release。"""

    __slots__ = ("_channel", "_stub")

    def __init__(self, addr: str) -> None:
        self._channel = grpc.aio.insecure_channel(addr)
        self._stub = owner_pb2_grpc.OwnerServiceStub(self._channel)

    async def close(self) -> None:
        with contextlib.suppress(Exception):
            await self._channel.close()

    async def query_owner_placement(self, player_id: int) -> OwnerPlacementView:
        """读完整 owner placement 快照。

        查询失败由调用方按 **UNKNOWN** 处理(禁冒充 OFFLINE):presence key miss
        不能证明玩家已离开旧 DS,更不能授权进入另一台 DS(§9.22)。
        """
        resp = await self._stub.QueryOwner(
            owner_pb2.QueryOwnerRequest(player_id=player_id), timeout=OWNER_RPC_TIMEOUT_SEC
        )
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                int(resp.code), "owner query rejected player=%d", player_id
            )
        rec = resp.record
        tgt = rec.target
        return OwnerPlacementView(
            owner_epoch=int(rec.owner_epoch),
            owner_type=int(rec.owner_type),
            phase=int(rec.phase),
            pod_name=tgt.pod_name,
            instance_uid=tgt.instance_uid,
            instance_epoch=int(tgt.instance_epoch),
            assignment_or_allocation_id=tgt.assignment_or_allocation_id,
            release_track=tgt.release_track,
            operation_id=rec.operation_id,
            admit_not_before_ms=int(rec.admit_not_before_ms),
            lease_deadline_ms=int(rec.lease_deadline_ms),
        )

    async def release_owner(self, player_id: int, owner_epoch: int, operation_id: str) -> None:
        """释放(epoch + operation 匹配才生效;迟到调用在 owner 侧幂等 no-op)。"""
        resp = await self._stub.ReleaseOwner(
            owner_pb2.ReleaseOwnerRequest(
                player_id=player_id, owner_epoch=owner_epoch, operation_id=operation_id
            ),
            timeout=OWNER_RPC_TIMEOUT_SEC,
        )
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                int(resp.code), "owner release rejected player=%d", player_id
            )


# ── matchmaker 只读耐久权威 ─────────────────────────────────────────────────
#
# 存在的理由(Go `internal/data/match_client.go` 头注释,P0 修复 2026-07-15):
# locator 是**presence 投影**(30s TTL,会蒸发);matchmaker 的 player claim + match
# 记录才是"玩家是否属于一场活跃对局"的**耐久事实**(claim 由 ReleaseMatch 显式释放)。
# presence 未命中 BATTLE 时再查一次这里,封两个窗口:
#   · READY 与 locator 投影之间(notifyBattle 之前 / 失败)把玩家误路由回 Hub;
#   · locator TTL 恰好蒸发但对局仍活跃 → Hub/Battle 双在场。

#: 登录链上 matchmaker 探测的**独立子预算**(Go `matchResolveTimeout`)。
#: 每次非战斗登录都同步查一次本 RPC(封上述窗口的必要查询,不得移除);matchmaker 是
#: 压测下最繁忙的服务,其 P99 慢化若无子预算会吃光 prod 登录 5s deadline。
#: ★ 超时**不得**降级成 presence-only:那等于用一次抖动换一次双在场。到期只是让失败
#: 提前、客户端更快进入退避重试,fail-closed 与否由 biz 按 profile 决定。
#: 取值:只读 Redis 记录查询,健康 P99 几十 ms,取保守偏大值 3s;待实测复核。
MATCH_RESOLVE_TIMEOUT_SEC = 3.0


@dataclasses.dataclass(slots=True)
class PlayerMatchAuthority:
    """`ResolvePlayerMatchContext` 的最小 client 视角产出 —— Go `data.PlayerMatchAuthority`。

    `state` 是 matchmaker 三态:
      UNSPECIFIED = 读取错误 / 索引漂移(fail-closed,B1 下可重试)
      NONE        = 明确无活跃撮合 / 对局
      ACTIVE      = 有活跃 claim(排队 / 确认 / 分配 / READY)
    """

    state: int = 0
    stage: int = 0
    match_id: int = 0
    battle_ds_addr: str = ""
    #: 撮合命名空间的 canonical 值(如 5v5_ranked / pve_coop),来自 matchmaker 持久记录。
    #: 冷启动客户端要用它恢复 x-pandora-game-mode 路由头;**绝不允许 login 按 PVE/PVP 猜**。
    game_mode: str = ""
    #: 本局副本编号(g_关卡.xlsx 关卡 id);0 = 未指定 / 默认。缺失时客户端有地图名反查兜底,
    #: 不 fail-closed。
    map_id: int = 0


class GrpcMatchContextResolver:
    """login → matchmaker 只读权威(零副作用,不改任何撮合状态)。"""

    __slots__ = ("_channel", "_stub", "_signer")

    def __init__(self, addr: str, signer: internalrpcauth.Signer | None = None) -> None:
        self._channel = grpc.aio.insecure_channel(addr)
        self._stub = match_pb2_grpc.MatchServiceStub(self._channel)
        # signer:login→matchmaker 内部东西向鉴权(pkg/internalrpcauth)。matchmaker 侧
        # ResolvePlayerMatchContext 强制校验 login 服务身份 HMAC + Redis nonce 防重放。
        # None = 不签名(仅容忍 matchmaker 未启用 resume auth 的裸 dev 环境;启用环境会被
        # ERR_PERMISSION_DENY 拒),main 装配时对 addr 已配但 secret 缺失打启动告警。
        self._signer = signer

    async def close(self) -> None:
        with contextlib.suppress(Exception):
            await self._channel.close()

    async def resolve_player_match_context(self, player_id: int) -> PlayerMatchAuthority:
        """每次调用签一份新鲜的 request-bound 凭证(方法 + player_id + 时间戳 + 一次性 nonce)。"""
        metadata = None
        if self._signer is not None:
            try:
                metadata = self._signer.sign_metadata(
                    MATCH_RESOLVE_PLAYER_MATCH_CONTEXT_METHOD, player_id
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "sign matchmaker resume-auth credential: %s",
                    exc,
                ) from exc
        try:
            resp = await self._stub.ResolvePlayerMatchContext(
                match_pb2.ResolvePlayerMatchContextRequest(player_id=player_id),
                timeout=MATCH_RESOLVE_TIMEOUT_SEC,
                metadata=metadata,
            )
        except asyncio.CancelledError:
            raise
        except grpc.aio.AioRpcError as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, "matchmaker ResolvePlayerMatchContext rpc: %s", exc
            ) from exc
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                int(resp.code),
                "matchmaker ResolvePlayerMatchContext code=%d",
                int(resp.code),
            )
        return PlayerMatchAuthority(
            state=int(resp.state),
            stage=int(resp.stage),
            match_id=int(resp.match_id),
            battle_ds_addr=resp.battle_ds_addr,
            game_mode=resp.game_mode,
            map_id=int(resp.map_id),
        )
