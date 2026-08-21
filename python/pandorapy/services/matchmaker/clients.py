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
