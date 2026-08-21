"""matchmaker 的出站依赖适配 —— 对应 Go 侧 internal/data/{team_reader,locator_client}.go
与 internal/biz/ds_stub.go。

三条依赖的**强度各不相同**,搞混任何一条都会制造事故形状:

  team(TeamReader)      组票是正确性路径:BeginTeamMatch 在 team 自己的乐观锁里
                        原子完成「校验 + 冻结名单 + 消费 ready」。绝不能退回只读
                        GetTeam —— 那与 team 的自动摘人分属两把锁,凑不出共同
                        线性化点,于是存在「读到名单 → 那个人被摘走 → 才建票」
                        的窗口(人在票里却已不在队里,被拉进一场自己不在场的对局)。

  locator(位置上报)     Notify* 是**弱依赖**:失败仅 Warn 不阻断撮合(上报晚一拍
                        不影响撮合正确性)。而前置查询 IsInBattle 默认 **fail-closed**
                        (见 biz.ensure_none_in_battle):locator 已注入却查不通时拒绝
                        入队,只有 dev 显式 battle_gate_fail_open=true 才降级放行。
                        本文件只负责透传结果 / 错误,**不吞错误** —— 吞掉就等于替
                        调用方做了 fail-open 的决定。

  ds_allocator          真实分配链(GrpcDSAllocator)是正确性路径:调 AllocateBattle 拉一台
                        battle DS,再由 matchmaker **自己**签 battle DSTicket ——
                        不变量 §3「票据是进场权威的唯一搬运通道」+ §9.6「DS 不可信」
                        决定了票不能由 DS 或分配器自签。
                        StubDSAllocator **只在 ds_allocator_addr 留空时**装配;两者的
                        选择只看这一个配置,绝不因签发器建不起来而互相降级
                        (签发器三档装配见 main.py 闸⑫)。
"""

from __future__ import annotations

import asyncio
import time
import uuid

import grpc

from pandora.common.v1 import errcode_pb2 as commonpb
from pandora.config.v1 import level_pb2 as levelpb
from pandora.ds.v1 import allocator_pb2 as dspb
from pandora.ds.v1 import allocator_pb2_grpc as dsgrpc
from pandora.locator.v1 import locator_pb2 as locatorpb
from pandora.locator.v1 import locator_pb2_grpc as locatorgrpc
from pandora.team.v1 import team_pb2 as teampb
from pandora.team.v1 import team_pb2_grpc as teamgrpc

from pandorapy import battleabort
from pandorapy import dsmetadata
from pandorapy import dsticket as pdsticket
from pandorapy import errcode
from pandorapy import internalrpcauth
from pandorapy import log as plog
from pandorapy import placement
from pandorapy import releasetrack
from pandorapy.services.login import dsticket as ldsticket

# team 侧会把 lease 钳到 [2s,15s];5s 是保守值,只需覆盖
# 「BeginTeamMatch 返回 → 本次 StartMatch 把 claim 落地」这一小段。
# 取大一点的代价只是「摘人多等一会儿」,取小了才会真漏窗口。
ROSTER_LOCK_LEASE_MS = 5_000

# 内网直连 gRPC 的默认超时(Go 侧 pkg/grpcclient.DefaultTimeout = 15s)。
DEFAULT_RPC_TIMEOUT_SEC = 15.0

# ds_allocator 的 abort RPC 全限定名。与 Go 的
# `dsv1.DSAllocatorService_AbortPreactiveBattle_FullMethodName` 逐字一致 ——
# 它是签名体的一部分,写错一个字符的后果是**每一次补偿都验签失败**,
# 而那时 DS 已经拉起来了(泄漏一台机器,且 match 卡在 ABORTING)。
ABORT_PREACTIVE_BATTLE_FULL_METHOD = (
    "/pandora.ds.v1.DSAllocatorService/AbortPreactiveBattle"
)


def roster_lock_operation_id(team_id: int, captain_id: int) -> str:
    """本次组票的**稳定** operation id。

    同一个 (team, captain) 的并发 / 重试拿到同一个 id,于是 team 侧按幂等续租而不是
    判冲突(§9.23:响应丢失后的重试必须继续同一个 operation,不能竞争创建第二个)。
    **刻意不掺时间戳** —— 掺了就会让自己的重试变成"另一次组票"而互相顶掉。
    """
    return f"startmatch:{team_id}:{captain_id}"


class GrpcTeamReader:
    """team 服务 gRPC 客户端。对应 Go 的 data.GrpcTeamReader。"""

    __slots__ = ("_channel", "_stub", "_signer", "_timeout")

    def __init__(
        self,
        team_addr: str,
        signer: internalrpcauth.Signer | None,
        *,
        timeout_sec: float = DEFAULT_RPC_TIMEOUT_SEC,
    ) -> None:
        self._channel = grpc.aio.insecure_channel(team_addr)
        self._stub = teamgrpc.TeamServiceStub(self._channel)
        self._signer = signer
        self._timeout = timeout_sec

    async def close(self) -> None:
        await self._channel.close()

    def _metadata(self, full_method: str, subject: int) -> list[tuple[str, str]]:
        """给出站调用签 request-bound 凭据(A-13)。

        signer 为 None = 未配密钥,**照常发出**:team 侧按其 match_call_auth_require
        决定观察放行还是拒。上线顺序是「两边配密钥 → 观察 → 翻 require」,
        每一步单独都安全(§9.21 不靠发布顺序)。
        """
        if self._signer is None:
            return []
        return self._signer.sign_metadata(full_method, subject)

    async def get_team(self, team_id: int) -> tuple[teampb.Team | None, bool]:
        """只读快照。非 OK code 一律压成 (None, False),由调用方决定怎么处理。"""
        resp = await self._stub.GetTeam(
            teampb.GetTeamRequest(team_id=team_id), timeout=self._timeout
        )
        if resp.code != commonpb.OK or not resp.HasField("team"):
            return None, False
        return resp.team, True

    async def begin_team_match(
        self, team_id: int, captain_id: int, operation_id: str, lease_ms: int, require_ready: bool
    ) -> tuple[teampb.Team, int]:
        """在 team 的乐观锁内原子完成「校验 + 冻结名单 + 消费 ready」。

        与 get_team 不同,这里**不把非 OK code 压成"没找到"**:组票拿不到锁是一个
        需要调用方区分对待的结果(队伍不 READY / 不是队长 / 正被另一次组票占着),
        压成"没找到"会让 StartMatch 报一个误导性的错误,也会把可重试的竞争说成终态。

        返回 (消费前的队伍快照, 被消费掉的那一代 ready_generation)。
        """
        metadata = self._metadata(
            "/pandora.team.v1.TeamService/BeginTeamMatch", team_id
        )
        resp = await self._stub.BeginTeamMatch(
            teampb.BeginTeamMatchRequest(
                team_id=team_id,
                captain_id=captain_id,
                operation_id=operation_id,
                lease_ms=lease_ms,
                require_ready=require_ready,
            ),
            metadata=metadata,
            timeout=self._timeout,
        )
        if resp.code != commonpb.OK:
            raise errcode.PandoraError(
                int(resp.code), "team.BeginTeamMatch code=%d team=%d", int(resp.code), team_id
            )
        return resp.team, resp.ready_generation

    async def end_team_match(
        self, team_id: int, player_ids: list[int], expected_ready_generation: int
    ) -> None:
        """对局结束复位队伍准备状态。幂等:已复位 / 队伍已解散 / 成员已离队均返回 OK。"""
        metadata = self._metadata("/pandora.team.v1.TeamService/EndTeamMatch", team_id)
        resp = await self._stub.EndTeamMatch(
            teampb.EndTeamMatchRequest(
                team_id=team_id,
                player_ids=player_ids,
                expected_ready_generation=expected_ready_generation,
            ),
            metadata=metadata,
            timeout=self._timeout,
        )
        if resp.code != commonpb.OK:
            raise errcode.PandoraError(
                int(resp.code), "team.EndTeamMatch code=%d team=%d", int(resp.code), team_id
            )


class GrpcLocationNotifier:
    """player_locator gRPC 客户端。对应 Go 的 data.GrpcLocationNotifier。

    状态权属(不变量 §1):matchmaker 是 MATCHING / BATTLE 两态的权威(它掌握撮合
    生命周期);HUB 由 hub DS 上报,故撮合失败 / 取消时 matchmaker **不回写 HUB**
    (交回 hub DS)—— 回写会与 hub DS 抢同一个位置字段的写者身份。
    """

    __slots__ = ("_channel", "_stub", "_timeout")

    def __init__(self, locator_addr: str, *, timeout_sec: float = DEFAULT_RPC_TIMEOUT_SEC) -> None:
        self._channel = grpc.aio.insecure_channel(locator_addr)
        self._stub = locatorgrpc.PlayerLocatorServiceStub(self._channel)
        self._timeout = timeout_sec

    async def close(self) -> None:
        await self._channel.close()

    async def _set_location(self, player_id: int, loc: locatorpb.Location) -> None:
        resp = await self._stub.SetLocation(
            locatorpb.SetLocationRequest(player_id=player_id, location=loc),
            timeout=self._timeout,
        )
        if resp.code != commonpb.OK:
            raise errcode.PandoraError(
                int(resp.code), "locator SetLocation player=%d code=%d", player_id, int(resp.code)
            )

    async def notify_matching(self, player_ids: list[int], match_id: int) -> None:
        """逐玩家 best-effort:单个失败继续其余,最后抛首个错误供调用方记 Warn。"""
        first_err: BaseException | None = None
        for pid in player_ids:
            try:
                await self._set_location(
                    pid,
                    locatorpb.Location(
                        state=locatorpb.LOCATION_STATE_MATCHING, match_id=match_id
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                first_err = first_err or exc
        if first_err is not None:
            raise first_err

    async def notify_battle(self, player_ids: list[int], match_id: int, battle_pod: str) -> None:
        first_err: BaseException | None = None
        for pid in player_ids:
            try:
                await self._set_location(
                    pid,
                    locatorpb.Location(
                        state=locatorpb.LOCATION_STATE_BATTLE,
                        match_id=match_id,
                        battle_pod=battle_pod,
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                first_err = first_err or exc
        if first_err is not None:
            raise first_err

    async def is_in_battle(self, player_id: int) -> bool:
        """玩家当前是否正处于 battle DS 中(不变量 §1 一人一 DS)。

        判据是三者同时成立:state==BATTLE 且 match_id!=0 且 battle_pod!=""。
        只判 state 会把"刚写了半条 BATTLE 记录"也算成在战斗中。
        查询失败**抛错不吞**:fail-open / fail-closed 的决定权在 biz(按配置)。
        """
        resp = await self._stub.GetLocation(
            locatorpb.GetLocationRequest(player_id=player_id), timeout=self._timeout
        )
        if resp.code != commonpb.OK:
            raise errcode.PandoraError(
                int(resp.code), "locator GetLocation player=%d code=%d", player_id, int(resp.code)
            )
        loc = resp.location
        return bool(
            loc.state == locatorpb.LOCATION_STATE_BATTLE
            and loc.match_id != 0
            and loc.battle_pod
        )

    async def find_offline_players(self, player_ids: list[int]) -> list[int]:
        """批量找出已离线的玩家(响应 map 里缺席 或 state==OFFLINE)。

        ❗ 这份判据对「已成局 match 的成员」结构性失效(INC-20260724-001):
        他们在 locator 里是 MATCHING 态,而 locator 只对 HUB 态续期 ——
        成局后 30s 内零真阳性、超过 30s 全员必然假阳性。因此调用方
        (biz.find_offline_members / liveness_sweep_once)默认整道关闭
        (liveness_gate_enabled=false),不要以为"查得到就能用"。
        """
        if not player_ids:
            return []
        resp = await self._stub.BatchGetLocation(
            locatorpb.BatchGetLocationRequest(player_ids=player_ids), timeout=self._timeout
        )
        if resp.code != commonpb.OK:
            raise errcode.PandoraError(
                int(resp.code), "locator BatchGetLocation code=%d", int(resp.code)
            )
        locations = resp.locations
        offline: list[int] = []
        for pid in player_ids:
            loc = locations.get(pid)
            if loc is None or loc.state == locatorpb.LOCATION_STATE_OFFLINE:
                offline.append(pid)
        return offline


class BattleAllocation:
    """一次 battle DS 分配的结果。对应 Go 的 internal/model.BattleAllocation。

    Target 各字段是**票据绑定**的输入:少一个,票据就从"绑死唯一 DS 实例"退化成
    "任何一台 battle DS 都能兑",所以 complete_battle 是 fail-closed 的必检项。
    """

    __slots__ = (
        "address",
        "pod_name",
        "instance_uid",
        "instance_epoch",
        "allocation_id",
        "release_track",
    )

    def __init__(
        self,
        address: str = "",
        pod_name: str = "",
        instance_uid: str = "",
        instance_epoch: int = 0,
        allocation_id: str = "",
        release_track: str = "",
    ) -> None:
        self.address = address
        self.pod_name = pod_name
        self.instance_uid = instance_uid
        self.instance_epoch = instance_epoch
        self.allocation_id = allocation_id
        self.release_track = release_track

    def complete_battle(self) -> bool:
        return bool(
            self.pod_name and self.instance_uid and self.instance_epoch and self.allocation_id
        )


class StubDSAllocator:
    """DSAllocator 打桩实现 —— 对应 Go 的 biz.StubDSAllocator。

    **只在 ds_allocator_addr 留空时装配**(与 Go 逐字同):那是明确声明"本进程不连
    真实分配器"的本地骨架联调档。ds_allocator_addr 非空却回落到本桩是绝不允许的 ——
    那会给玩家发一个 127.0.0.1:7777 的假地址和一张假票,而服务端日志全绿。
    """

    __slots__ = ("mock_addr",)

    def __init__(self, addr: str = "") -> None:
        self.mock_addr = addr or "127.0.0.1:7777"

    async def allocate_battle_with_combat_factions(
        self,
        match_id: int,
        player_ids: list[int],
        combat_faction_by_player: dict[int, int],
        map_id: int,
    ) -> BattleAllocation:
        """桩不需要真的投递阵营,但**必须**按 fail-closed 校验它。

        Go 侧曾经在分配器不支持阵营时降级调用不带阵营的接口并只打一条 Warn ——
        那条路会产出「有名单、无阵营」的分配,DS 侧只能退化成每人一个独立阵营:
        队友互相能打,而且对局照常进行、照常结算,错误完全不可见。
        本桩照抄那次修复后的形状,让本地环境和生产暴露同一类错误。
        """
        if not combat_faction_by_player:
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "stub allocator: combat factions required for match %d",
                match_id,
            )
        for pid in player_ids:
            if pid not in combat_faction_by_player:
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "stub allocator: player %d missing combat faction",
                    pid,
                )
        return BattleAllocation(
            address=self.mock_addr,
            pod_name=f"mock-battle-{match_id}",
            instance_uid=f"mock-uid-{match_id}",
            instance_epoch=1,
            allocation_id=f"mock-allocation-{match_id}",
            release_track="stable",
        )

    async def abort_battle_allocation(
        self, match_id: int, operation_id: str, allocation: BattleAllocation
    ) -> None:
        return None

    async def sign_battle_tickets(
        self, match_id: int, player_ids: list[int], allocation: BattleAllocation
    ) -> dict[int, str]:
        return {pid: f"mock-ticket-{match_id}-{pid}" for pid in player_ids}

    async def sign_battle_ticket(
        self, player_id: int, match_id: int, allocation: BattleAllocation
    ) -> str:
        """带纳秒后缀模拟「每次新 jti」—— 换设备 / 掉线重连每次都拿新票,
        不会撞 DS 侧 jti 一次性防重放。"""
        return f"mock-ticket-{match_id}-{player_id}-{time.time_ns()}"


def new_operation_id() -> str:
    """分配 / saga 的幂等键(UUIDv4)。跨 matchmaker 与 placement 重试稳定。"""
    return str(uuid.uuid4())


class GrpcDSAllocator:
    """真实分配链 —— 对应 Go 的 `data.GrpcDSAllocator`。

    职责边界(与 Go 逐条同):ds_allocator 服务只负责「拉一个 DS pod」并回填实例身份,
    **不签票据**;票由 matchmaker 这一侧签(§9.6:DS 不可信,派生数值与凭据都由可信
    后端产出)。所以本类同时握着 gRPC 客户端与两档 battle 票签发器。

    两档签发器**互斥**且由启动装配二选一(见 main.py 的闸⑫):
      · `v2` 非空 → RS256 实例绑定票(生产唯一形态,§9.22 exact 实例绑定);
      · `legacy` 非空 → HS256 本机联调档 local-off-v1,UE DS 被硬锁在 HS256LocalOff
        分支且不交叉接受 v2 票。
    两个都为空 = 装配漏了,签票时 fail-closed 拒 —— 绝不静默签一张验不过的票。
    """

    __slots__ = (
        "_channel",
        "_stub",
        "_legacy",
        "_v2",
        "_abort_auth",
        "_map_id",
        "_game_mode",
        "_timeout",
        "_sess_gate",
        "_tables",
    )

    def __init__(
        self,
        ds_allocator_addr: str,
        legacy_signer: ldsticket.DSTicketSigner | None,
        v2_signer: pdsticket.DSTicketSigner | None,
        abort_auth: internalrpcauth.Signer | None,
        map_id: int,
        game_mode: str,
        allocate_timeout_sec: float,
    ) -> None:
        # AllocateBattle 的超时必须覆盖「agones allocate + ready_wait」整段预算,
        # 不能用 15s 的通用默认值 —— 用默认值的表现是「DS 其实拉起来了,但 matchmaker
        # 已经超时判失败并把玩家退回队列」,于是泄漏一台机器且玩家莫名重排。
        self._timeout = (
            allocate_timeout_sec if allocate_timeout_sec > 0 else DEFAULT_RPC_TIMEOUT_SEC
        )
        self._channel = grpc.aio.insecure_channel(ds_allocator_addr)
        self._stub = dsgrpc.DSAllocatorServiceStub(self._channel)
        self._legacy = legacy_signer
        self._v2 = v2_signer
        self._abort_auth = abort_auth
        self._map_id = map_id
        self._game_mode = game_mode
        self._sess_gate = None
        self._tables = None

    def set_session_gate(self, gate) -> None:  # noqa: ANN001
        """注入会话现行性权威(启动期、撮合循环开跑前)。对应 Go 的 SetSessionGate。

        非 None 时 READY 批签的每张 battle 票都带上该玩家当前会话 jti(sjti claim),
        Login 兑换点复核现行性 —— 被新登录顶掉的旧设备即使还留着 READY 推送里的票
        也无法入场(R7 复审 P0-2)。
        """
        self._sess_gate = gate

    def set_config_tables(self, store) -> None:  # noqa: ANN001
        """注入配置表容器(启动期,可选)。对应 Go 的 SetConfigTables。

        用 setter 而非构造参数:未启用配置表的调用点不必被迫改签名。
        """
        self._tables = store

    async def close(self) -> None:
        await self._channel.close()

    # ── 关卡表定格 ────────────────────────────────────────────────────────

    def _level_row(self, map_id: int):  # noqa: ANN202
        """取关卡表当前批次的这一行;拿不到一律 None(不抛)。

        每次都经 store 取**当前**批次,于是天然读到热更后的表(与 biz 同型)。
        """
        if self._tables is None:
            return None
        tb = self._tables.tables
        if tb is None:
            return None
        return tb.by_id(map_id)

    def _rating_mode_for_map(self, map_id: int) -> int:
        """取本局计分模式(关卡表 rating_mode 列)。对应 Go 的 ratingModeForMap。

        这是「算不算段位」的**定格点**:结果写进 AllocateBattleRequest → canonical
        BattleStorageRecord,battle_result 结算只认那个定格值,不在结算那一刻重查表
        (热更改本列会改写正在打的那一局的规则)。

        ★ 拿不到就返回 UNSPECIFIED(0),**绝不猜 ELO**:未启用配置表 / 表未加载 /
        行不存在时,0 让 battle_result 回落到本列上线前的旧口径(canonical pve_coop
        不计分、其余算 Elo),与不带本字段的旧 matchmaker 行为逐字节一致
        (§9.21 共存窗口双向兼容)。猜 ELO 会给合作副本玩家扣段位,而改段位不可逆 ——
        方向必须偏向"少算",不能偏向"乱算"。
        """
        row = self._level_row(map_id)
        if row is None:
            return levelpb.LEVEL_RATING_MODE_UNSPECIFIED
        return row.rating_mode

    def _rating_pool_for_map(self, map_id: int) -> str:
        """取某副本的段位池,与 _rating_mode_for_map 同一口径。对应 Go 的 ratingPoolForMap。

        ★ 这里刻意**不做归一化**(不把空串变成 "default"):归一化只在真正落账那一侧
        做一次,这样 canonical 记录里的空值仍能被识别为"未定格",与
        rating_mode=UNSPECIFIED 一起构成"这一局出自旧 matchmaker / 旧批次表"的证据。
        若在这里就填成 "default",事后再也分不清是"策划配了 default 池"还是"根本没配"。
        """
        row = self._level_row(map_id)
        if row is None:
            return ""
        return row.rating_pool

    # ── 分配 ──────────────────────────────────────────────────────────────

    async def allocate_battle_with_combat_factions(
        self,
        match_id: int,
        player_ids: list[int],
        combat_faction_by_player: dict[int, int],
        map_id: int,
    ) -> BattleAllocation:
        """把 matchmaker 权威的 MatchMember.side 完整下发给 allocator。

        映射按 player_id canonical 化;多个玩家 / 队伍可共享 faction,也允许 faction>1。
        阵营缺失一律拒(不降级调用不带阵营的接口)—— 降级会产出「有名单、无阵营」的
        分配,DS 侧只能退化成每人一个独立阵营:队友互相能打,而且对局照常进行、
        照常结算,错误完全不可见。
        """
        try:
            canonical_players, _ = dsmetadata.canonical_combat_factions(
                player_ids, combat_faction_by_player
            )
        except ValueError as exc:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "invalid combat factions: %s", exc
            ) from exc
        factions = [
            dspb.BattlePlayerCombatFaction(
                player_id=pid, combat_faction_id=combat_faction_by_player[pid]
            )
            for pid in canonical_players
        ]
        return await self._allocate_battle(match_id, canonical_players, factions, map_id)

    async def _allocate_battle(
        self,
        match_id: int,
        player_ids: list[int],
        combat_factions: list,
        map_id: int,
    ) -> BattleAllocation:
        # map_id 非 0 时按局透传给 ds_allocator 选副本地图;为 0(旧客户端 / 未选)
        # 时回退到静态默认,保持向后兼容。
        effective_map_id = map_id or self._map_id
        # 计分模式按 effective_map_id 定格(与 game_mode / map_id 同一时刻、同一请求)。
        rating_mode = self._rating_mode_for_map(effective_map_id)
        if rating_mode == levelpb.LEVEL_RATING_MODE_UNSPECIFIED:
            # 战斗类关卡的本列应当显式填。落到这里只有两种情况:滚动升级期还在用旧批次表,
            # 或策划漏填这一列 —— 两者都会让本局回落旧口径(pve_coop 不计分 / 其余算 Elo)。
            # 是"要去查表"的信号,不是错误:分配照常继续(§15.2 不为观测新增失败模式)。
            plog.get().warning(
                "battle_rating_mode_unconfigured",
                match_id=match_id,
                map_id=effective_map_id,
                game_mode=self._game_mode,
                hint="关卡表 g_关卡.xlsx「计分模式」列未填,本局按旧口径结算(pve_coop 不计分 / 其余算 Elo)",
            )
        # 段位池与计分模式同一刻定格:ELO 局必须带上池,否则结算只能兜底进 default 池,
        # 表现为"这张图的分和别的图混在一起算"(加载期校验已拒此配置,这里是纵深防御)。
        rating_pool = self._rating_pool_for_map(effective_map_id)
        if rating_mode == levelpb.LEVEL_RATING_MODE_ELO and rating_pool == "":
            plog.get().warning(
                "battle_rating_pool_missing_for_elo",
                match_id=match_id,
                map_id=effective_map_id,
                game_mode=self._game_mode,
                hint="本图要算段位却没填「段位池」列,本局分会记进 default 池;查关卡表该行",
            )
        resp = await self._stub.AllocateBattle(
            dspb.AllocateBattleRequest(
                match_id=match_id,
                player_ids=player_ids,
                map_id=effective_map_id,
                game_mode=self._game_mode,
                player_combat_factions=combat_factions,
                rating_mode=rating_mode,
                rating_pool=rating_pool,
            ),
            timeout=self._timeout,
        )
        # ★ 保留 allocator 的权威错误分类,**不要**压成 ErrDSAllocationFailed。
        # 尤其 ERR_UNAVAILABLE 意味着外部分配结果 UNKNOWN(例如 commit/fence 回包丢了);
        # 压平会让 match worker 把这一局标 FAILED 并把玩家退回队列,而此刻 Battle DS
        # 可能已经存在 —— 那就是同一批玩家的第二场对局。
        if resp.code != commonpb.OK:
            raise errcode.PandoraError(
                resp.code,
                "ds_allocator returned code=%d for match %d",
                resp.code,
                match_id,
            )
        if not resp.ds_addr:
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "ds_allocator returned OK with empty addr for match %d",
                match_id,
            )
        _require_complete_battle_target(
            resp.ds_pod_name,
            resp.gameserver_uid,
            resp.instance_epoch,
            resp.allocation_id,
            resp.release_track,
            match_id,
        )
        return BattleAllocation(
            address=resp.ds_addr,
            pod_name=resp.ds_pod_name,
            instance_uid=resp.gameserver_uid,
            instance_epoch=resp.instance_epoch,
            allocation_id=resp.allocation_id,
            release_track=resp.release_track,
        )

    async def abort_battle_allocation(
        self, match_id: int, operation_id: str, allocation: BattleAllocation
    ) -> None:
        """调 allocator 的破坏性补偿 RPC,用新鲜 nonce + 对 canonical 全量请求体的签名。

        **不复用**玩家 JWT、Login resume 或 DS callback 密钥 —— 那几把钥匙的持有者
        范围比"能拆一台 DS"宽得多。
        """
        if self._abort_auth is None or allocation is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle allocation abort service auth unavailable for match %d",
                match_id,
            )
        request = battleabort.Request(
            match_id=match_id,
            operation_id=operation_id,
            target=placement.Target(
                pod_name=allocation.pod_name,
                instance_uid=allocation.instance_uid,
                instance_epoch=allocation.instance_epoch,
                allocation_id=allocation.allocation_id,
                release_track=allocation.release_track,
            ),
        )
        if not request.complete():
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "complete battle allocation abort tuple required for match %d",
                match_id,
            )
        try:
            metadata = self._abort_auth.sign_metadata_with_payload(
                ABORT_PREACTIVE_BATTLE_FULL_METHOD, match_id, request.canonical()
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "sign battle allocation abort for match %d: %s",
                match_id,
                exc,
            ) from exc
        resp = await self._stub.AbortPreactiveBattle(
            dspb.AbortPreactiveBattleRequest(
                match_id=match_id,
                allocation_operation_id=operation_id,
                ds_pod_name=allocation.pod_name,
                gameserver_uid=allocation.instance_uid,
                instance_epoch=allocation.instance_epoch,
                allocation_id=allocation.allocation_id,
                release_track=allocation.release_track,
            ),
            metadata=metadata,
            timeout=self._timeout,
        )
        if resp.code != commonpb.OK:
            raise errcode.PandoraError(
                resp.code,
                "ds_allocator abort returned code=%d for match %d",
                resp.code,
                match_id,
            )

    # ── 签票 ──────────────────────────────────────────────────────────────

    async def sign_battle_tickets(
        self, match_id: int, player_ids: list[int], allocation: BattleAllocation
    ) -> dict[int, str]:
        """整批签票。**任一张失败即整批失败**(与 Go 逐字同)。

        不做"能签几张算几张":部分成功会让一部分人进场、另一部分人拿不到票卡在
        READY,而对局照常开始 —— 那是 §9.19 的玩家卡死形态。
        """
        tickets: dict[int, str] = {}
        for player_id in player_ids:
            tickets[player_id] = await self.sign_battle_ticket(
                player_id, match_id, allocation
            )
        return tickets

    async def sign_battle_ticket(
        self, player_id: int, match_id: int, allocation: BattleAllocation
    ) -> str:
        """只使用 READY match 持久化的 exact target 签票。

        不允许降级 legacy HMAC 票 —— 唯一例外是 Windows 本机联调档 local-off-v1,
        此时 main 只注入 legacy signer、不注入 v2(两者互斥,见 main.py 闸⑫),
        因为同机的 ds_allocator / UE DS 被硬锁在 HS256LocalOff 档、不交叉接受 v2 票。

        R7 复审 P0-2:sess_gate 非 None 时读玩家当前会话 jti 签进 sjti claim。
          · 权威不可达 → fail-closed 拒签(票不能在"无法判定会话"时盲签);
          · 无会话(已登出 / 过期) → 拒签:没有现行会话就不存在合法的入场交付对象,
            重登后的重连链(login 的 battle 重连三态门)会用新会话重签。
        """
        if allocation is None or not allocation.complete_battle():
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "complete target required, player %d match %d",
                player_id,
                match_id,
            )
        if self._v2 is None and self._legacy is None:
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "no battle ticket signer configured, player %d match %d",
                player_id,
                match_id,
            )
        sess_jti = ""
        if self._sess_gate is not None:
            try:
                jti, found = await self._sess_gate.current_jti(player_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "session authority unavailable while signing battle ticket, "
                    "player %d match %d: %s",
                    player_id,
                    match_id,
                    exc,
                ) from exc
            if not found:
                raise errcode.PandoraError(
                    errcode.ErrUnauthorized,
                    "player %d has no current session; battle ticket withheld, match %d",
                    player_id,
                    match_id,
                )
            sess_jti = jti
        if self._v2 is None:
            # local-off-v1:UE 侧走 HS256LocalOff 分支,只校验 player/match/exp。
            try:
                token, _ = self._legacy.sign(
                    player_id,
                    ldsticket.DS_TYPE_BATTLE,
                    match_id=match_id,
                    jti=str(uuid.uuid4()),
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrDSAllocationFailed,
                    "sign local-off-v1 battle ticket for player %d match %d failed: %s",
                    player_id,
                    match_id,
                    exc,
                ) from exc
            return token
        target = pdsticket.DSTicketTarget(
            ds_pod_name=allocation.pod_name,
            ds_instance_uid=allocation.instance_uid,
            ds_instance_epoch=allocation.instance_epoch,
            release_track=allocation.release_track,
            match_id=match_id,
            allocation_id=allocation.allocation_id,
            session_jti=sess_jti,
        )
        try:
            # region_id / cell_id 恒 0:与 Go 的 `g.v2.SignBattleTicket(playerID, 0, 0, ...)`
            # 逐字同 —— battle 票不带分区维度。
            token, _ = self._v2.sign_battle_ticket(
                player_id, 0, 0, str(uuid.uuid4()), target
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "sign bound v2 battle ticket for player %d match %d failed: %s",
                player_id,
                match_id,
                exc,
            ) from exc
        return token


def _require_complete_battle_target(
    pod_name: str,
    gameserver_uid: str,
    instance_epoch: int,
    allocation_id: str,
    release_track: str,
    match_id: int,
) -> None:
    """三个实例字段 + allocation_id + release_track 缺一即拒。

    对应 Go 的 `battleTargetFromFields`。旧 ds_allocator / 降级路径会回一个"能连上但
    没有身份"的分配 —— 放过去的后果是签出一张不带 exact 实例绑定的 v2 票,
    §9.22 当场失效(任何一台 battle DS 都能兑),而两边日志全绿。
    """
    if (
        not pod_name
        or not gameserver_uid
        or instance_epoch == 0
        or not allocation_id
        or release_track not in (releasetrack.STABLE, releasetrack.CANARY)
    ):
        raise errcode.PandoraError(
            errcode.ErrDSAllocationFailed,
            "ds_allocator 未回填完整 DS 目标(pod=%r uid=%r epoch=%d alloc=%r track=%r),"
            "无法签 v2 票, match %d",
            pod_name,
            gameserver_uid,
            instance_epoch,
            allocation_id,
            release_track,
            match_id,
        )
