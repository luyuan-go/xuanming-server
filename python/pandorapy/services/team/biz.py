"""team 服务的业务逻辑层 —— 对应 Go 侧 internal/biz/team.go。

设计原则(协议铁律 4 原则):
 1. 立即完成型:RPC 在 biz 内完成状态机迁移 + redis 写 + kafka push 后立即返回
 2. push 不发 caller:push_team_update 的 caller_player_id != 0 时不发给发起者自身
 3. kafka key = player_id(不变量 §9):push_to_players 已保证
 4. WATCH/MULTI/EXEC 乐观锁:所有写路径走 `ready_generation.update_team`,
    冲突重试 optimistic_retry 次

状态机合法迁移(见 proto/pandora/team/v1/team.proto):
    FORMING   → READY(全员 ready)
    READY     → FORMING(任一成员 leave/kick/掉线软化/对局开始消费)
    DISBANDED → 任何写操作都拒绝(ErrTeamWrongState)

★ **所有队伍写都必须走 `ready_generation.update_team`**,不许直连
  `repo.update_with_lock`。它在锁内比较 ready 意图指纹并推进代际;绕过去的写会让
  代际停在旧值 → EndTeamMatch 的 CAS 照样通过 → 幂等保护形同虚设,**而且所有测试
  照样绿**(INC-20260813-001 的失效形状)。
"""

from __future__ import annotations

import asyncio
import time

from pandora.team.v1 import team_pb2

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import logwindow
from pandorapy.services.team import conf as tconf
from pandorapy.services.team import diag
from pandorapy.services.team import metrics as tmetrics
from pandorapy.services.team import ready_generation as rg
from pandorapy.services.team.offline_leave import OfflineLeaveMixin

# ── 常量 ─────────────────────────────────────────────────────────────────────

STATE_FORMING = diag.STATE_FORMING
STATE_READY = diag.STATE_READY
STATE_DISBANDED = diag.STATE_DISBANDED

# GetMyTeam 在线续期的节流间隔:同一玩家至多每 15 分钟续一次。
# 客户端轮询周期(秒级)远小于它,active_ttl(60 分钟)远大于它,续期不会断流。
TOUCH_INTERVAL_SEC = 15 * 60.0

# 候选超取倍数。索引是非权威投影,候选里可能混着已满 / 已开打 / 已解散的队伍,
# 复核会刷掉一部分;只取 limit 条会导致「明明有队伍却返回不足 limit 条」。
OPEN_CANDIDATE_FACTOR = 3
# 单次候选读取的硬上限,保证单次 Redis 读与复核开销有界
# (limit 已被 max_open_teams_per_query 钳住,这里是第二道闸)。
OPEN_CANDIDATE_MAX = 64

# player name / player_no 都是展示弱依赖。权威服务整体不可达时每次
# 队伍读都会降级，日志必须限流，否则一次依赖故障会按组队界面轮询 QPS 刷屏。
PLAYER_NAME_RESOLVE_LOG_WINDOW_MS = 5000
PLAYER_NO_RESOLVE_LOG_WINDOW_MS = 5000


def now_ms() -> int:
    return int(time.time() * 1000)


def has_member(team, player_id: int) -> bool:  # noqa: ANN001
    return any(m.player_id == player_id for m in team.members)


def member_index(members, player_id: int) -> int:  # noqa: ANN001
    for i, m in enumerate(members):
        if m.player_id == player_id:
            return i
    return -1


def remove_member(team, player_id: int) -> None:  # noqa: ANN001
    """从 repeated 字段里摘掉一个成员(原地改,保持 proto 语义)。"""
    kept = [m for m in team.members if m.player_id != player_id]
    del team.members[:]
    team.members.extend(kept)


def all_ready(members) -> bool:  # noqa: ANN001
    """空名单返回 False —— 与 Go 一致。

    ★ 若写成「空即全 ready」,一支刚被摘空的队伍会被判成 READY 并进撮合,
    matchmaker 拿到 0 人 roster。
    """
    if len(members) == 0:
        return False
    return all(m.ready for m in members)


def member_ids(team) -> list[int]:  # noqa: ANN001
    return [m.player_id for m in team.members]


def clone_team(team):  # noqa: ANN001, ANN201
    out = team_pb2.TeamStorageRecord()
    out.CopyFrom(team)
    return out


def clone_members(members):  # noqa: ANN001, ANN201
    """深拷贝成员列表。

    BeginTeamMatch 的收据要存一份消费前名单,而**同一份**名单还要返回给 matchmaker
    建票。两者共享同一批对象会让「存进队伍记录的快照」与「调用方手里的快照」互为别名 ——
    那种别名不会立刻出错,只会在将来某次看似无关的改动里静默串味。
    """
    out = []
    for m in members:
        c = team_pb2.TeamMemberStorageRecord()
        c.CopyFrom(m)
        out.append(c)
    return out


def is_open_for_recruit(team) -> bool:  # noqa: ANN001
    """判定队伍是否「正在招募」(会出现在 ListOpenTeams 结果里)。

    口径:**只要没满员且没解散就还在招募**,FORMING 与 READY 一视同仁。

    为什么 READY 也算(2026-08-05 修订,原先只认 FORMING):READY 只表示「当前成员都
    点了准备」,不表示「已经开打」。而 StartMatch 要求队伍必须 READY 才能开局,于是
    原口径下「想被人搜到就不能准备、想开局就必须准备」互斥 —— 单人队长点了准备就从
    招募列表消失,谁也搜不到他。

    那「已经在排队 / 在打的队伍别被打扰」由谁保证?**不由本函数**:MATCHING / IN_BATTLE
    在本服务没有写入点,队伍从提交票据到打完全程停在 READY,靠 state 根本分不出来。
    真正的闸门是入队路径的 `ensure_team_not_committed_to_match`(查 matchmaker 权威
    claim),以及 ListOpenTeams 复核时的同一道闸门。

    这是**唯一**的满员/解散判定口径:写索引和读复核都调它,避免「写进去的和读出来的
    不是同一套标准」。
    """
    if team is None:
        return False
    if team.state not in (STATE_FORMING, STATE_READY):
        return False
    count = len(team.members)
    return 0 < count < team.max_size


class TeamUsecase(OfflineLeaveMixin):
    """team 业务逻辑核心。"""

    def __init__(self, repo, pusher, cfg: tconf.TeamConf) -> None:  # noqa: ANN001
        self.repo = repo
        # pusher 可为 None(显式无 kafka 开发模式)→ 所有推送静默跳过。
        self.pusher = pusher
        self.cfg = cfg

        # 确定性 region/cell 路由器。None:单 Cell / dev / 阶段 1~2 不分片,
        # 队伍 region 分布观测退化为不打日志(行为不变)。
        self.router = None
        # 「离队/踢人 → 撤销 matchmaker 票据」联动。None(未配 matchmaker_addr)→ 不联动。
        self.match_canceler = None
        # 申请/邀请频率配额(anti-abuse §6 第 6 项)。None = 不限。
        self.rate_quota = None
        # 「入队闸门」:队伍已提交对局时拒绝新人进来。None → 跳过闸门;那种部署下
        # 根本没有匹配链路,不存在被对局占住的队伍,与 match_canceler 的弱依赖口径一致。
        self.match_commitment = None
        # 「离线成员自动退队」的读路径兜底入口(见 offline_leave.py)。None → no-op。
        self.presence = None
        # 客户端视图的 player_no 批量解析器。None / 解析失败 → 统一填 0，绝不让
        # 展示弱依赖阻断建队、入队或查询；player_id 始终保留为身份字段。
        self.player_no_resolver = None
        self._player_no_resolve_log = logwindow.Window()
        # nickname 只从 player 域权威批量读取；未配或失败时留空。存储
        # TeamMemberStorageRecord.nickname 不是名字权威，禁止回退使用。
        self.player_name_resolver = None
        self._player_name_resolve_log = logwindow.Window()

        # 每个玩家上次 GetMyTeam 续期队伍 TTL 的时刻(节流,避免每次轮询都敲 EXPIRE)。
        # 多实例部署下各实例独立节流,最坏多几次 EXPIRE,无正确性影响。
        # 内存上限:_maybe_sweep_last_touch 每 TOUCH_INTERVAL_SEC 清一次过期条目,
        # 常驻规模 ≈ 最近 2×interval 内轮询过 GetMyTeam 的活跃玩家数,不随 DAU 永久增长。
        self._last_touch: dict[int, float] = {}
        self._last_touch_sweep_at = 0.0

    # ── 可选依赖注入(全部 nil-safe,与 Go 的 SetXxx 同口径)──────────────────

    def set_cell_router(self, router) -> None:  # noqa: ANN001
        self.router = router

    def set_rate_quota(self, quota) -> None:  # noqa: ANN001
        self.rate_quota = quota

    def set_match_canceler(self, canceler) -> None:  # noqa: ANN001
        self.match_canceler = canceler

    def set_match_commitment_reader(self, reader) -> None:  # noqa: ANN001
        self.match_commitment = reader

    def set_presence_inspector(self, presence) -> None:  # noqa: ANN001
        self.presence = presence

    def set_player_no_resolver(self, resolver) -> None:  # noqa: ANN001
        self.player_no_resolver = resolver

    def set_player_name_resolver(self, resolver) -> None:  # noqa: ANN001
        self.player_name_resolver = resolver

    # ── 小工具 ──────────────────────────────────────────────────────────────

    def invite_ttl_ms(self) -> int:
        return int(self.cfg.invite_ttl_td().total_seconds() * 1000)

    def active_ttl_sec(self) -> float:
        return self.cfg.active_ttl_td().total_seconds()

    def max_open_teams(self) -> int:
        return self.cfg.max_open_teams_per_query if self.cfg.max_open_teams_per_query > 0 else 10

    def max_applications(self) -> int:
        return (
            self.cfg.max_applications_per_team
            if self.cfg.max_applications_per_team > 0
            else 10
        )

    def join_policy(self) -> str:
        return self.cfg.join_policy_effective()

    def join_policy_proto(self) -> int:
        if self.join_policy() == tconf.JOIN_POLICY_OPEN:
            return team_pb2.TEAM_JOIN_POLICY_OPEN
        return team_pb2.TEAM_JOIN_POLICY_APPROVAL

    async def _update_team(self, team_id: int, fn, stamp=None) -> None:  # noqa: ANN001
        """biz 层**唯一**允许的队伍写入口(见模块头)。"""
        await rg.update_team(
            self.repo,
            team_id,
            fn,
            optimistic_retry=self.cfg.optimistic_retry,
            ttl=self.active_ttl_sec(),
            stamp=stamp,
        )

    async def _allow_action(self, action: str, player_id: int) -> None:
        """频率配额门:窗内超额抛 ErrRateLimited(先于一切副作用)。

        quota 未注入 / 判定失败(Redis 抖动)一律放行(**fail-open**,anti-abuse §2 铁律:
        限流是背压门不是权威门,判不出来不得卡玩家)。
        """
        if self.rate_quota is None:
            return
        # ★ 读 `(ok, exc)` 而不是等异常：`allow()` 从不抛，原先那段
        #   `except ... : log("team_rate_quota_check_failed")` 是**不可达死代码**，
        #   于是这个 Loki 键在代码里存在、永远不会触发。
        ok, quota_exc = await self.rate_quota.allow(action, player_id)
        if quota_exc is not None:
            # 背压门判定失败一律放行（§9.20：限流不得卡玩家）。
            plog.get().warning(
                "team_rate_quota_check_failed",
                action=action,
                player_id=player_id,
                err=str(quota_exc),
                fail_open=True,
            )
            return
        if not ok:
            plog.get().warning(
                "team_rate_quota_rejected",
                reason=diag.REASON_RATE_LIMITED,
                action=action,
                player_id=player_id,
            )
            raise errcode.PandoraError(
                errcode.ErrRateLimited, "team %s rate limited, retry later", action
            )

    # ── CreateTeam ──────────────────────────────────────────────────────────

    async def create_team(self, team_id: int, player_id: int):  # noqa: ANN201
        """创建队伍,player_id 为队长。前置条件:player_id 不在任何队伍中。

        写序铁律:**先写队伍主体,后 claim_player 声明归属**。主体先落地时索引尚未指向它
        (team_id 是 Snowflake 新发,返回前无人可见),故不存在「索引已指向、主体还没写」
        的 in-flight 窗口 —— 这是 `_claim_player_healing_orphan` 把「主体不存在」判为
        真孤儿的安全前提。若倒过来先 claim 后写主体,并发的 heal 会把 in-flight claim
        误判孤儿并 CAS 删掉,同一玩家可能同时出现在两支队伍(违反不变量 §1)。
        """
        ttl = self.active_ttl_sec()
        ts = now_ms()
        team = team_pb2.TeamStorageRecord(
            team_id=team_id,
            captain_id=player_id,
            state=STATE_FORMING,
            members=[team_pb2.TeamMemberStorageRecord(player_id=player_id)],
            created_at_ms=ts,
            updated_at_ms=ts,
            max_size=self.cfg.max_members,
        )

        # 1. 先写队伍主体(此时索引不指向它,对全世界不可见)。
        try:
            await self.repo.create(team, ttl)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_create_rejected",
                reason=diag.REASON_STORE_WRITE_FAILED,
                team_id=team_id,
                captain_id=player_id,
                err=str(exc),
            )
            raise

        # 2. 原子声明玩家归属(SETNX),保证不变量 §1。孤儿索引会自愈,不误拦成 3004。
        try:
            await self._claim_player_healing_orphan(player_id, team_id, ttl)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 声明失败(玩家真在其他队)→ 回滚删掉自己刚写的主体,避免残留无主队伍。
            try:
                await self.repo.delete_team(team_id)
            except asyncio.CancelledError:
                raise
            except BaseException:  # noqa: BLE001 —— 回滚失败靠 TTL 自然回收
                pass
            plog.get().warning(
                "team_create_rejected",
                reason=diag.REASON_ALREADY_IN_TEAM,
                team_id=team_id,
                captain_id=player_id,
                rolled_back=True,
                err=str(exc),
            )
            raise

        # 3. push 给队长自己(创建者收到快照确认)
        await self.push_update(
            0, [player_id], team, team_pb2.TEAM_UPDATE_REASON_MEMBER_JOINED, 0
        )
        # 新建队伍只有队长一人、状态 FORMING → 立即进入"开放招募"索引,让别人能找到它。
        await self.sync_open_index(team, team.map_id)

        plog.get().info(
            "team_created",
            team_id=team_id,
            captain_id=player_id,
            state=int(team.state),
            max_size=team.max_size,
        )
        diag.log_team_state_changed(
            team_id,
            diag.STATE_UNSPECIFIED,
            team.state,
            "create",
            team.ready_generation,
            len(team.members),
        )
        await self.log_team_composition(team)
        return team

    # ── Invite / AcceptInvite ───────────────────────────────────────────────

    async def invite(
        self, invite_id: int, team_id: int, inviter_id: int, target_player_id: int
    ):  # noqa: ANN201
        """邀请目标玩家加入队伍。inviter 必须在该队伍中。"""
        # 频率配额(anti-abuse §6 第 6 项):按发起方计,先于一切读写。
        await self._allow_action("invite", inviter_id)

        try:
            team, found = await self.repo.get(team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            self._log_invite_rejected(
                diag.REASON_STORE_READ_FAILED,
                team_id,
                inviter_id,
                target_player_id,
                invite_id,
                exc,
            )
            raise
        if not found:
            self._log_invite_rejected(
                diag.REASON_TEAM_NOT_FOUND, team_id, inviter_id, target_player_id, invite_id
            )
            raise errcode.PandoraError(
                errcode.ErrTeamNotFound, "team %d not found", team_id
            )
        if team.state == STATE_DISBANDED:
            self._log_invite_rejected(
                diag.REASON_TEAM_DISBANDED, team_id, inviter_id, target_player_id, invite_id
            )
            raise errcode.PandoraError(
                errcode.ErrTeamWrongState, "team %d disbanded", team_id
            )
        if not has_member(team, inviter_id):
            self._log_invite_rejected(
                diag.REASON_INVITER_NOT_MEMBER,
                team_id,
                inviter_id,
                target_player_id,
                invite_id,
            )
            raise errcode.PandoraError(
                errcode.ErrTeamNotFound, "player %d not in team %d", inviter_id, team_id
            )
        if len(team.members) >= team.max_size:
            self._log_invite_rejected(
                diag.REASON_TEAM_FULL, team_id, inviter_id, target_player_id, invite_id
            )
            raise errcode.PandoraError(
                errcode.ErrTeamFull,
                "team %d is full (%d/%d)",
                team_id,
                len(team.members),
                team.max_size,
            )

        try:
            await self.repo.set_invite(
                invite_id,
                team_id,
                inviter_id,
                target_player_id,
                self.cfg.invite_ttl_td().total_seconds(),
                self.cfg.max_pending_invites,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 这里同时收敛「被邀请人 pending 上限已满(3008)」与「Redis 写失败」两种,
            # 用错误码把它们区分开:上限是产品语义的拒绝,写失败是故障。
            reason = diag.REASON_INVITE_STORE_FAILED
            if errcode.as_code(exc) == errcode.ErrTeamInvitePendingLimit:
                reason = diag.REASON_INVITE_PENDING_LIMIT
            self._log_invite_rejected(
                reason, team_id, inviter_id, target_player_id, invite_id, exc
            )
            raise

        # push 邀请给 target(不发给 inviter —— 原则 2)。
        # 老客户端只认 TeamUpdateEvent(reason=INVITE_SENT),新客户端只认独立
        # TeamInviteEvent(已不再从 TeamUpdateEvent 读邀请)。灰度共存期靠"双发"喂饱两代:
        #   dual(默认)  两条都发。老客户端 legacy 弹框、把独立事件误解成 TeamUpdateEvent
        #                → InviteId/TeamId=0 → 护栏不过 → 只多一次无害快照不误弹;
        #                新客户端忽略 legacy、只在独立事件弹框。**各弹一次不双弹**。
        #   dedicated    只发独立事件(全量铺完新客户端后用)。
        #   legacy       只发旧事件(回退用)。
        mode = self.cfg.invite_push_mode
        if mode == "legacy":
            await self.push_update(
                inviter_id,
                [target_player_id],
                team,
                team_pb2.TEAM_UPDATE_REASON_INVITE_SENT,
                invite_id,
            )
        elif mode == "dedicated":
            await self.push_invite(inviter_id, target_player_id, team_id, invite_id)
        else:  # "dual"(含空串)
            await self.push_invite(inviter_id, target_player_id, team_id, invite_id)
            await self.push_update(
                inviter_id,
                [target_player_id],
                team,
                team_pb2.TEAM_UPDATE_REASON_INVITE_SENT,
                invite_id,
            )

        plog.get().info(
            "team_invite_sent",
            team_id=team_id,
            inviter_id=inviter_id,
            target_player_id=target_player_id,
            invite_id=invite_id,
            push_mode=mode,
            members=len(team.members),
        )
        return team

    def _log_invite_rejected(
        self,
        reason: str,
        team_id: int,
        inviter_id: int,
        target_player_id: int,
        invite_id: int,
        err: BaseException | None = None,
    ) -> None:
        """收口 Invite 的全部拒绝分支(§11.3 R2)。

        单独一个助手而不是逐处写 warning:邀请有 6 个互不相同的拒绝原因,分散写迟早
        出现字段名不一致(team/team_id、target/target_player_id),看板就聚合不起来。
        """
        plog.get().warning(
            "team_invite_rejected",
            reason=reason,
            team_id=team_id,
            inviter_id=inviter_id,
            target_player_id=target_player_id,
            invite_id=invite_id,
            err=None if err is None else str(err),
        )

    def _log_accept_invite_rejected(
        self,
        reason: str,
        team_id: int,
        player_id: int,
        invite_id: int,
        err: BaseException | None = None,
    ) -> None:
        """收口 AcceptInvite 令牌校验的拒绝分支。

        这四种情况在协议上是同一个错误码(3005 邀请已失效),客户端提示也一样;
        只有 reason 能分出「令牌真过期了」/「点了发给别人的邀请」/「令牌与队伍对不上」/
        「Redis 读失败」。
        """
        plog.get().warning(
            "team_accept_invite_rejected",
            reason=reason,
            team_id=team_id,
            player_id=player_id,
            invite_id=invite_id,
            err=None if err is None else str(err),
        )

    async def accept_invite(self, invite_id: int, team_id: int, player_id: int):  # noqa: ANN201
        # 1. 若提供 invite_id,校验令牌
        if invite_id != 0:
            try:
                inv, found = await self.repo.get_invite(invite_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                self._log_accept_invite_rejected(
                    diag.REASON_INVITE_LOOKUP_FAILED, team_id, player_id, invite_id, exc
                )
                raise
            if not found:
                self._log_accept_invite_rejected(
                    diag.REASON_INVITE_NOT_FOUND, team_id, player_id, invite_id
                )
                raise errcode.PandoraError(
                    errcode.ErrTeamInviteExpired,
                    "invite %d expired or not found",
                    invite_id,
                )
            if inv.target_player_id != player_id:
                self._log_accept_invite_rejected(
                    diag.REASON_INVITE_TARGET_MISMATCH, team_id, player_id, invite_id
                )
                raise errcode.PandoraError(
                    errcode.ErrTeamInviteExpired, "invite %d target mismatch", invite_id
                )
            if inv.team_id != team_id:
                self._log_accept_invite_rejected(
                    diag.REASON_INVITE_TEAM_MISMATCH, team_id, player_id, invite_id
                )
                raise errcode.PandoraError(
                    errcode.ErrTeamInviteExpired, "invite %d team mismatch", invite_id
                )

        # 2. 走共用入队事务(内部先 claim_player 保不变量 §1,再改成员表)。
        result = await self.join_team(team_id, player_id)

        # 删 invite 令牌(同时释放被邀请人 pending 索引配额)
        if invite_id != 0:
            try:
                await self.repo.delete_invite(invite_id, player_id)
            except asyncio.CancelledError:
                raise
            except BaseException:  # noqa: BLE001 —— best-effort,令牌自带 TTL
                pass

        await self.push_update(
            player_id,
            member_ids(result),
            result,
            team_pb2.TEAM_UPDATE_REASON_MEMBER_JOINED,
            0,
        )
        # 人数变了 → 可能从"招募中"变成"已满",同步开放队伍索引(非权威投影)。
        await self.sync_open_index(result, result.map_id)

        plog.get().info(
            "team_accept_invite",
            team_id=team_id,
            player_id=player_id,
            invite_id=invite_id,
            new_state=int(result.state),
            members=len(result.members),
            ready_generation=result.ready_generation,
        )
        await self.log_team_composition(result)
        return result

    # ── 共用入队事务 ────────────────────────────────────────────────────────

    async def join_team(self, team_id: int, player_id: int):  # noqa: ANN201
        """「把 player_id 加进 team_id」的**唯一**入队事务,由三条路径共用:
        accept_invite(接受邀请)、apply_to_team(open 策略直接入队)、
        handle_team_application(队长同意)。

        为什么必须共用一份:入队是不变量 §1 的关键写路径,顺序铁律是**先 claim_player
        原子声明归属,后改成员表** —— 两个并发入队路径若各写一份,迟早有一条忘了先声明,
        同一玩家就会同时出现在两支队伍。共用后新增入队入口不可能绕过它。
        """
        ttl = self.active_ttl_sec()

        # 入队闸门:队伍一旦提交过对局,成员名单已被 matchmaker 冻结进票据,此刻放人
        # 进来 = 他不在票据里、也没有 match claim,永远不会被拉进这一局,只能占着队伍
        # 名额干等(§9.20)。必须在 claim_player 之前判,避免拒绝后还要回滚归属。
        await self.ensure_team_not_committed_to_match(team_id)

        await self._claim_player_healing_orphan(player_id, team_id, ttl)

        # prev_state / reject_reason 只用于日志:锁内看到的最后一次取值即本次提交的依据
        # (乐观锁重试时会被覆盖成最后一轮的值,正是我们要记的那一轮)。
        box: dict = {"result": None, "prev_state": 0, "reject": ""}

        def _mutate(team) -> None:  # noqa: ANN001
            box["prev_state"] = team.state
            if team.state == STATE_DISBANDED:
                box["reject"] = diag.REASON_TEAM_DISBANDED
                raise errcode.PandoraError(
                    errcode.ErrTeamWrongState, "team %d disbanded", team_id
                )
            if len(team.members) >= team.max_size:
                box["reject"] = diag.REASON_TEAM_FULL
                raise errcode.PandoraError(errcode.ErrTeamFull, "team %d full", team_id)
            if has_member(team, player_id):
                box["reject"] = diag.REASON_ALREADY_IN_TEAM
                raise errcode.PandoraError(
                    errcode.ErrTeamAlreadyInTeam,
                    "player %d already in team %d",
                    player_id,
                    team_id,
                )
            box["reject"] = ""

            team.members.append(
                team_pb2.TeamMemberStorageRecord(player_id=player_id)
            )
            team.updated_at_ms = now_ms()

            # 入队后**重算**就绪态,而不是只处理 FORMING→READY 一个方向:新成员默认
            # ready=false,加入一支已经 READY 的队伍(邀请路径可达)必须把队伍打回 FORMING。
            # 否则会留下「state=READY 但有人没准备」的脏态,而 matchmaker 只校验
            # state==READY、不逐人校验 ready,会带着没准备的人开局。
            team.state = STATE_READY if all_ready(team.members) else STATE_FORMING
            box["result"] = clone_team(team)

        try:
            await self._update_team(team_id, _mutate)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            try:
                await self.repo.delete_player_index_if_matches(player_id, team_id)
            except asyncio.CancelledError:
                raise
            except BaseException as derr:  # noqa: BLE001
                plog.get().warning(
                    "team_join_rollback_index_delete_failed",
                    reason=diag.REASON_STORE_WRITE_FAILED,
                    player_id=player_id,
                    team_id=team_id,
                    err=str(derr),
                )
            reason = box["reject"]
            if not reason:
                # 锁内没给出 reason 的只剩「乐观锁重试耗尽 / Redis 故障」两类,按错误码区分。
                reason = diag.REASON_STORE_WRITE_FAILED
                if errcode.as_code(exc) == errcode.ErrTeamConcurrent:
                    reason = diag.REASON_OPTIMISTIC_RETRY_EXHAUSTED
            plog.get().warning(
                "team_join_rejected",
                reason=reason,
                team_id=team_id,
                player_id=player_id,
                team_state=int(box["prev_state"]),
                err=str(exc),
            )
            raise

        result = box["result"]
        plog.get().info(
            "team_member_joined",
            team_id=team_id,
            player_id=player_id,
            prev_state=int(box["prev_state"]),
            new_state=int(result.state),
            members=len(result.members),
            max_size=result.max_size,
            ready_generation=result.ready_generation,
        )
        diag.log_team_state_changed(
            team_id,
            box["prev_state"],
            result.state,
            "join",
            result.ready_generation,
            len(result.members),
        )
        # player index 已由 claim_player 在锁前原子写入,此处无需再写。
        return result

    async def ensure_team_not_committed_to_match(self, team_id: int) -> None:
        """入队闸门:队伍已被一场对局占住时拒绝新人进来。

        判据取 matchmaker 的权威 claim 而不是 team.state —— MATCHING / IN_BATTLE 在本
        服务没有写入点,排队和战斗期间 state 都是 READY。查队长一人即可代表整队:
        StartMatch 由队长发起,队长的 start 索引先于成员 claim 写入,成员 claim 与队长
        同属一张票据,ReleaseMatch 也整票释放。

        **fail-closed**:matchmaker 不可达或返回 UNKNOWN 一律拒绝入队。放进一支已开打的
        队伍会让玩家既进不去这一局又占着队伍名额(§9.20),比让他重试一次严重得多。

        残留窗口:本检查与随后写成员表之间存在毫秒级 TOCTOU(跨服务无法原子)。后果有界 ——
        挤进来的人没有 match claim,绝不会被拉进那一局;他只是留在大厅的队伍里,随时可离队。
        """
        if self.match_commitment is None:
            return
        try:
            team, found = await self.repo.get(team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_join_gate_rejected",
                reason=diag.REASON_STORE_READ_FAILED,
                team_id=team_id,
                err=str(exc),
            )
            raise
        if not found:
            plog.get().warning(
                "team_join_gate_rejected",
                reason=diag.REASON_TEAM_NOT_FOUND,
                team_id=team_id,
            )
            raise errcode.PandoraError(
                errcode.ErrTeamNotFound, "team %d not found", team_id
            )

        try:
            committed = await self.is_team_committed_to_match(team)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # fail-closed 的降级必须带 reason:线上这条码是 ErrUnavailable,access log 里
            # 只会看到一次失败调用,分不出「matchmaker 挂了」与「队伍真在打」。
            plog.get().warning(
                "team_join_gate_rejected",
                reason=diag.REASON_MATCH_COMMITMENT_UNKNOWN,
                team_id=team_id,
                captain_id=team.captain_id,
                fail_closed=True,
                err=str(exc),
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "team %d match commitment unknown, join rejected",
                team_id,
                cause=exc,
            ) from exc
        if committed:
            plog.get().warning(
                "team_join_gate_rejected",
                reason=diag.REASON_MATCH_COMMITTED,
                team_id=team_id,
                captain_id=team.captain_id,
                team_state=int(team.state),
            )
            raise errcode.PandoraError(
                errcode.ErrTeamWrongState,
                "team %d already committed to a match",
                team_id,
            )

    async def is_team_committed_to_match(self, team) -> bool:  # noqa: ANN001
        """读「这支队伍是否已被一场对局占住」,复用调用方已取到的队伍记录。

        未注入 reader(骨架联调 / 未配 matchmaker_addr)时恒为 False,与历史行为一致。
        """
        if self.match_commitment is None or team is None or team.captain_id == 0:
            return False
        return await self.match_commitment.is_player_committed_to_match(team.captain_id)

    # ── LeaveTeam / Kick / SetReady ─────────────────────────────────────────

    async def leave_team(self, team_id: int, player_id: int):  # noqa: ANN201
        """玩家主动离队。

        匹配联动:若该成员正在排队/确认期(matchmaker 持有其 claim),离队后 best-effort
        撤销整张匹配票据(队伍人数已变,票据快照不再成立)。
        """
        disbanded_ttl = self.cfg.disbanded_retention_td().total_seconds()
        box: dict = {"result": None, "prev_state": 0, "prev_captain": 0, "reject": ""}

        def _mutate(team) -> None:  # noqa: ANN001
            box["prev_state"] = team.state
            box["prev_captain"] = team.captain_id
            if team.state == STATE_DISBANDED:
                box["reject"] = diag.REASON_TEAM_DISBANDED
                raise errcode.PandoraError(
                    errcode.ErrTeamWrongState, "team %d disbanded", team_id
                )
            if not has_member(team, player_id):
                box["reject"] = diag.REASON_NOT_MEMBER
                raise errcode.PandoraError(
                    errcode.ErrTeamNotFound,
                    "player %d not in team %d",
                    player_id,
                    team_id,
                )
            box["reject"] = ""

            remove_member(team, player_id)
            team.updated_at_ms = now_ms()

            if len(team.members) == 0:
                team.state = STATE_DISBANDED  # 队伍空 → 解散
            else:
                if team.captain_id == player_id:
                    team.captain_id = team.members[0].player_id  # 队长离队 → 转移
                if team.state == STATE_READY:
                    team.state = STATE_FORMING  # READY 下有人离开 → 回 FORMING
            box["result"] = clone_team(team)

        try:
            await self._update_team(team_id, _mutate)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            reason = box["reject"]
            if not reason:
                reason = diag.REASON_STORE_WRITE_FAILED
                if errcode.as_code(exc) == errcode.ErrTeamConcurrent:
                    reason = diag.REASON_OPTIMISTIC_RETRY_EXHAUSTED
            plog.get().warning(
                "team_leave_rejected",
                reason=reason,
                team_id=team_id,
                player_id=player_id,
                team_state=int(box["prev_state"]),
                err=str(exc),
            )
            raise

        result = box["result"]
        # 删 player index。CAS:仅当索引仍指向本队才删,防误删玩家并发加入新队的归属。
        try:
            await self.repo.delete_player_index_if_matches(player_id, team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_leave_delete_player_index_failed",
                reason=diag.REASON_STORE_WRITE_FAILED,
                player_id=player_id,
                team_id=team_id,
                err=str(exc),
            )

        # 匹配联动:离队成员若正在排队/确认期 → 撤销整张票据(best-effort,不阻断离队)
        await self.cancel_matchmaking(team_id, player_id)
        # 人数/状态变了 → 同步开放队伍索引(解散或满员时会被摘掉)。
        await self.sync_open_index(result, result.map_id)

        if result.state == STATE_DISBANDED:
            await self.refresh_disbanded_ttl(team_id, disbanded_ttl)
            # 队伍没了,残留的入队申请再无人能处理 → 顺手清掉,不留到 TTL(best-effort)。
            try:
                await self.repo.delete_applications(team_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "team_disband_delete_applications_failed",
                    reason=diag.REASON_STORE_WRITE_FAILED,
                    team_id=team_id,
                    err=str(exc),
                )
            await self.push_update(
                player_id,
                member_ids(result),
                result,
                team_pb2.TEAM_UPDATE_REASON_DISBANDED,
                0,
            )
        else:
            await self.push_update(
                player_id,
                member_ids(result),
                result,
                team_pb2.TEAM_UPDATE_REASON_MEMBER_LEFT,
                0,
            )

        plog.get().info(
            "team_leave",
            team_id=team_id,
            player_id=player_id,
            prev_state=int(box["prev_state"]),
            new_state=int(result.state),
            remaining=len(result.members),
            # 队长离队会把权限转给第一个成员,而「我怎么突然成队长了 / 谁把图改了」
            # 没有这条就无从解释。
            captain_transferred=(
                box["prev_captain"] == player_id and result.state != STATE_DISBANDED
            ),
            new_captain_id=result.captain_id,
            ready_generation=result.ready_generation,
        )
        diag.log_team_state_changed(
            team_id,
            box["prev_state"],
            result.state,
            "leave",
            result.ready_generation,
            len(result.members),
        )
        return result

    async def kick(self, team_id: int, captain_id: int, target_player_id: int):  # noqa: ANN201
        """队长踢人。匹配联动同 leave_team。"""
        box: dict = {"result": None, "prev_state": 0, "reject": ""}

        def _mutate(team) -> None:  # noqa: ANN001
            box["prev_state"] = team.state
            if team.state == STATE_DISBANDED:
                box["reject"] = diag.REASON_TEAM_DISBANDED
                raise errcode.PandoraError(
                    errcode.ErrTeamWrongState, "team %d disbanded", team_id
                )
            if team.captain_id != captain_id:
                box["reject"] = diag.REASON_NOT_CAPTAIN
                raise errcode.PandoraError(
                    errcode.ErrTeamNotCaptain,
                    "player %d is not captain of team %d",
                    captain_id,
                    team_id,
                )
            if captain_id == target_player_id:
                box["reject"] = diag.REASON_CAPTAIN_SELF_KICK
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "captain cannot kick themselves"
                )
            if not has_member(team, target_player_id):
                box["reject"] = diag.REASON_NOT_MEMBER
                raise errcode.PandoraError(
                    errcode.ErrTeamNotFound,
                    "player %d not in team %d",
                    target_player_id,
                    team_id,
                )
            box["reject"] = ""

            remove_member(team, target_player_id)
            team.updated_at_ms = now_ms()
            if team.state == STATE_READY:
                team.state = STATE_FORMING  # READY 下踢人 → 回 FORMING
            box["result"] = clone_team(team)

        try:
            await self._update_team(team_id, _mutate)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            reason = box["reject"]
            if not reason:
                reason = diag.REASON_STORE_WRITE_FAILED
                if errcode.as_code(exc) == errcode.ErrTeamConcurrent:
                    reason = diag.REASON_OPTIMISTIC_RETRY_EXHAUSTED
            plog.get().warning(
                "team_kick_rejected",
                reason=reason,
                team_id=team_id,
                captain_id=captain_id,
                target_player_id=target_player_id,
                team_state=int(box["prev_state"]),
                err=str(exc),
            )
            raise

        result = box["result"]
        try:
            await self.repo.delete_player_index_if_matches(target_player_id, team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_kick_delete_player_index_failed",
                reason=diag.REASON_STORE_WRITE_FAILED,
                player_id=target_player_id,
                team_id=team_id,
                err=str(exc),
            )

        await self.cancel_matchmaking(team_id, target_player_id)
        # 人数变了(满员队踢掉一人后重新开放招募)→ 同步开放队伍索引。
        await self.sync_open_index(result, result.map_id)

        # push 给剩余成员 + 被踢者(不发给 captain —— 原则 2)
        recipients = member_ids(result) + [target_player_id]
        await self.push_update(
            captain_id, recipients, result, team_pb2.TEAM_UPDATE_REASON_MEMBER_KICKED, 0
        )

        plog.get().info(
            "team_kick",
            team_id=team_id,
            captain_id=captain_id,
            target_player_id=target_player_id,
            prev_state=int(box["prev_state"]),
            new_state=int(result.state),
            remaining=len(result.members),
            ready_generation=result.ready_generation,
        )
        diag.log_team_state_changed(
            team_id,
            box["prev_state"],
            result.state,
            "kick",
            result.ready_generation,
            len(result.members),
        )
        return result

    async def set_ready(
        self, team_id: int, player_id: int, ready: bool, hero_id: int
    ):  # noqa: ANN201
        """设置玩家 ready 状态,并可选更换英雄。"""
        box: dict = {"result": None, "prev_state": 0, "prev_ready": False, "reject": ""}

        def _mutate(team) -> None:  # noqa: ANN001
            box["prev_state"] = team.state
            if team.state == STATE_DISBANDED:
                box["reject"] = diag.REASON_TEAM_DISBANDED
                raise errcode.PandoraError(
                    errcode.ErrTeamWrongState, "team %d disbanded", team_id
                )
            if team.state not in (STATE_FORMING, STATE_READY):
                box["reject"] = diag.REASON_STATE_NOT_ALLOWED
                raise errcode.PandoraError(
                    errcode.ErrTeamWrongState,
                    "team %d state %d not allows set_ready",
                    team_id,
                    int(team.state),
                )
            idx = member_index(team.members, player_id)
            if idx < 0:
                box["reject"] = diag.REASON_NOT_MEMBER
                raise errcode.PandoraError(
                    errcode.ErrTeamNotFound,
                    "player %d not in team %d",
                    player_id,
                    team_id,
                )
            box["reject"] = ""
            box["prev_ready"] = team.members[idx].ready

            team.members[idx].ready = ready
            if hero_id > 0:
                team.members[idx].hero_id = hero_id
            team.updated_at_ms = now_ms()

            if ready and all_ready(team.members):
                team.state = STATE_READY  # 全员 ready → 切 READY
            elif not ready and team.state == STATE_READY:
                team.state = STATE_FORMING  # 任一成员取消 ready → 回 FORMING
            box["result"] = clone_team(team)

        try:
            await self._update_team(team_id, _mutate)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            reason = box["reject"]
            if not reason:
                reason = diag.REASON_STORE_WRITE_FAILED
                if errcode.as_code(exc) == errcode.ErrTeamConcurrent:
                    reason = diag.REASON_OPTIMISTIC_RETRY_EXHAUSTED
            plog.get().warning(
                "team_set_ready_rejected",
                reason=reason,
                team_id=team_id,
                player_id=player_id,
                want_ready=ready,
                team_state=int(box["prev_state"]),
                err=str(exc),
            )
            raise

        result = box["result"]
        reason_enum = (
            team_pb2.TEAM_UPDATE_REASON_HERO_CHANGED
            if hero_id > 0
            else team_pb2.TEAM_UPDATE_REASON_MEMBER_READY
        )
        await self.push_update(player_id, member_ids(result), result, reason_enum, 0)
        # FORMING ↔ READY 会改变「是否还在招募」(只有未满且未散才进列表)→ 同步索引。
        await self.sync_open_index(result, result.map_id)

        plog.get().info(
            "team_set_ready",
            team_id=team_id,
            player_id=player_id,
            prev_ready=box["prev_ready"],
            ready=ready,
            hero_id=hero_id,
            prev_state=int(box["prev_state"]),
            new_state=int(result.state),
            ready_count=diag.ready_count(result.members),
            members=len(result.members),
            # ready_generation 是 EndTeamMatch 复位 CAS 的凭据,必须同框。
            ready_generation=result.ready_generation,
        )
        diag.log_team_state_changed(
            team_id,
            box["prev_state"],
            result.state,
            "set_ready",
            result.ready_generation,
            len(result.members),
        )
        return result

    # ── 只读路径 ────────────────────────────────────────────────────────────

    async def get_team(self, team_id: int):  # noqa: ANN201
        """读取队伍快照(只读,不走 WATCH)。"""
        try:
            team, found = await self.repo.get(team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_get_rejected",
                reason=diag.REASON_STORE_READ_FAILED,
                team_id=team_id,
                err=str(exc),
            )
            raise
        if not found:
            plog.get().warning(
                "team_get_rejected",
                reason=diag.REASON_TEAM_NOT_FOUND,
                team_id=team_id,
            )
            raise errcode.PandoraError(
                errcode.ErrTeamNotFound, "team %d not found", team_id
            )
        return team

    async def get_player_team_id(self, player_id: int) -> tuple[int, bool]:
        """只反查玩家当前队伍编号(内部只读,DS 出生编制专用)。

        刻意不复用 get_my_team:那条路径带三个「玩家本人正在看队伍」的副作用 ——
        续 TTL、清脏索引、离线成员观察。DS 每有一名玩家进场就会调一次,把它当成玩家
        心跳会让已被抛弃的队伍被反复续命。

        判定以**成员表**为准,不以索引为准。索引是派生投影:退队 / 被踢 / 离线清扫都会
        走 CAS 删它,但那一步删失败时只打一条 warn(best-effort,不回滚已生效的退队)。
        于是存在残留窗口 —— 索引还指向队伍 T,T 也确实存在且未解散,只是这名玩家早已
        不在 T 的成员表里。只校验「T 存在且未解散」会把一个已经退队的人当成 T 的队友
        发给 DS,而 DS 只在进场时查这一次、整场不再纠正(§9.22)。
        """
        try:
            team_id, found = await self.repo.get_player_team_id(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_player_team_lookup_failed",
                reason=diag.REASON_STORE_READ_FAILED,
                player_id=player_id,
                err=str(exc),
            )
            raise
        if not found:
            # 这是 DS 出生编制的**权威判定结果**,一名玩家一次进场只有这一条。
            plog.get().info(
                "team_player_team_resolved",
                player_id=player_id,
                has_team=False,
                reason=diag.REASON_NO_TEAM,
            )
            return 0, False

        try:
            team, found = await self.repo.get(team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_player_team_lookup_failed",
                reason=diag.REASON_STORE_READ_FAILED,
                player_id=player_id,
                team_id=team_id,
                err=str(exc),
            )
            raise
        if not found or team.state == STATE_DISBANDED:
            reason = diag.REASON_TEAM_DISBANDED if found else diag.REASON_TEAM_NOT_FOUND
            plog.get().info(
                "team_player_team_resolved",
                player_id=player_id,
                has_team=False,
                stale_team_id=team_id,
                reason=reason,
            )
            return 0, False
        if not has_member(team, player_id):
            # 索引残留(删索引那一步失败过)。按无队伍处理:**错的归属比缺失的归属更糟** ——
            # 前者让路人整场显示成队友,后者只是逐档回落到阵营矩阵。
            # 与本函数其余分支同样「只判定不清理」,自愈仍留给 get_my_team 的 CAS 删索引。
            plog.get().warning(
                "team_player_index_points_to_non_member",
                reason=diag.REASON_INDEX_NON_MEMBER,
                player_id=player_id,
                team_id=team_id,
                team_state=int(team.state),
                members=len(team.members),
            )
            plog.get().info(
                "team_player_team_resolved",
                player_id=player_id,
                has_team=False,
                stale_team_id=team_id,
                reason=diag.REASON_INDEX_NON_MEMBER,
            )
            return 0, False
        plog.get().info(
            "team_player_team_resolved",
            player_id=player_id,
            has_team=True,
            team_id=team_id,
            team_state=int(team.state),
            members=len(team.members),
            captain_id=team.captain_id,
        )
        return team_id, True

    async def get_my_team(self, player_id: int):  # noqa: ANN201
        """查询玩家当前所在队伍(只读,登录后进大厅时调用)。

        返回 (record, has_team)。索引命中但队伍记录已过期/已解散时,顺手清掉脏索引
        (否则玩家会被 claim_player SETNX 挡住无法再建队,不变量 §1 的残留侧漏洞)。
        """
        try:
            team_id, found = await self.repo.get_player_team_id(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_my_team_rejected",
                reason=diag.REASON_STORE_READ_FAILED,
                player_id=player_id,
                stage="player_index",
                err=str(exc),
            )
            raise
        if not found:
            # R4:没队伍是最常见的正常态,且本接口是轮询路径 —— 只能 Debug。
            plog.get().debug(
                "team_my_team_resolved",
                player_id=player_id,
                has_team=False,
                reason=diag.REASON_NO_TEAM,
            )
            return None, False

        try:
            team, found = await self.repo.get(team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_my_team_rejected",
                reason=diag.REASON_STORE_READ_FAILED,
                player_id=player_id,
                team_id=team_id,
                stage="team_record",
                err=str(exc),
            )
            raise
        if not found or team.state == STATE_DISBANDED:
            # TTL 竞态残留:索引还在但队伍已没/已解散 → 按无队伍处理并清索引。
            # CAS:仅当索引仍指向该孤儿 team_id 才删,防误删并发建队/入队刚写入的新归属。
            try:
                await self.repo.delete_player_index_if_matches(player_id, team_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "team_stale_player_index_cleanup_failed",
                    reason=diag.REASON_STORE_WRITE_FAILED,
                    player_id=player_id,
                    team_id=team_id,
                    err=str(exc),
                )
            else:
                # 清索引是不可逆推进(玩家从此可以重新建队)。这条只在真的清掉脏索引
                # 那一次触发,不是每轮都出,不违反 R4。
                plog.get().info(
                    "team_stale_player_index_healed",
                    player_id=player_id,
                    stale_team_id=team_id,
                    reason=diag.REASON_TEAM_NOT_FOUND,
                )
            return None, False

        # 在线心跳:玩家仍在轮询自己的队伍 → 续期(节流,best-effort)。
        # 只在 get_my_team(本人+索引校验过)续,get_team(任意 team_id)绝不续,
        # 防旁人反复读把已抛弃队伍永久续命;disbanded 分支已在上方 return,不续。
        await self._maybe_touch_team(team, player_id)
        # 离线成员兜底观察(功能关闭时为 no-op)。
        await self.inspect_team_presence(team)
        plog.get().debug(
            "team_my_team_resolved",
            player_id=player_id,
            has_team=True,
            team_id=team_id,
            team_state=int(team.state),
            members=len(team.members),
            captain_id=team.captain_id,
            ready_count=diag.ready_count(team.members),
            ready_generation=team.ready_generation,
        )
        return team, True

    async def list_pending_invites(self, player_id: int):  # noqa: ANN201
        """查询发给 player_id 的未过期 pending 邀请(拉取兜底,只读)。

        为什么存在:邀请令牌的唯一权威在 Redis,kafka→push 推送只是投影(§9-22)。
        此前 invite_id 只能从推送获得,推送链路任一环丢帧邀请就静默失效到 TTL 过期。
        """
        try:
            invites = await self.repo.list_pending_invites(
                player_id, self.cfg.max_pending_invites
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_list_invites_rejected",
                reason=diag.REASON_STORE_READ_FAILED,
                player_id=player_id,
                limit=self.cfg.max_pending_invites,
                err=str(exc),
            )
            raise
        plog.get().debug(
            "team_list_invites",
            player_id=player_id,
            invites=len(invites),
            limit=self.cfg.max_pending_invites,
        )
        return invites

    # ── 找队伍:列表 / 申请 / 审批 ───────────────────────────────────────────

    async def set_team_map(self, team_id: int, captain_id: int, map_id: int):  # noqa: ANN201
        """队长设置本队目标关卡(招募展示 + ListOpenTeams 的 map_id 筛选依据)。

        不校验 map_id 是否在关卡表内:本字段只是招募标签,真正的准入判定在进入链
        (StartMatch 已有 ERR_MATCH_INVALID_MAP),在这里再判一次就是第二份判定
        (§17.3 明确禁止)。
        """
        box: dict = {"result": None, "prev_map": 0, "prev_state": 0, "reject": ""}

        def _mutate(team) -> None:  # noqa: ANN001
            box["prev_state"] = team.state
            if team.state == STATE_DISBANDED:
                box["reject"] = diag.REASON_TEAM_DISBANDED
                raise errcode.PandoraError(
                    errcode.ErrTeamWrongState, "team %d disbanded", team_id
                )
            if team.captain_id != captain_id:
                box["reject"] = diag.REASON_NOT_CAPTAIN
                raise errcode.PandoraError(
                    errcode.ErrTeamNotCaptain,
                    "player %d is not captain of team %d",
                    captain_id,
                    team_id,
                )
            if team.state not in (STATE_FORMING, STATE_READY):
                # 已进撮合/战斗的队伍改目标关卡没有意义(本次对局的图早已定死)。
                box["reject"] = diag.REASON_STATE_NOT_ALLOWED
                raise errcode.PandoraError(
                    errcode.ErrTeamWrongState,
                    "team %d state %d not allows set_map",
                    team_id,
                    int(team.state),
                )
            box["reject"] = ""
            box["prev_map"] = team.map_id
            team.map_id = map_id
            team.updated_at_ms = now_ms()
            box["result"] = clone_team(team)

        try:
            await self._update_team(team_id, _mutate)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            reason = box["reject"]
            if not reason:
                reason = diag.REASON_STORE_WRITE_FAILED
                if errcode.as_code(exc) == errcode.ErrTeamConcurrent:
                    reason = diag.REASON_OPTIMISTIC_RETRY_EXHAUSTED
            plog.get().warning(
                "team_set_map_rejected",
                reason=reason,
                team_id=team_id,
                captain_id=captain_id,
                map_id=map_id,
                team_state=int(box["prev_state"]),
                err=str(exc),
            )
            raise

        result = box["result"]
        # 换桶:先摘旧 map 分桶再写新分桶(由 repo.sync_open_team 内部按 prev_map_id 处理)。
        await self.sync_open_index(result, box["prev_map"])
        await self.push_update(
            captain_id,
            member_ids(result),
            result,
            team_pb2.TEAM_UPDATE_REASON_MAP_CHANGED,
            0,
        )
        plog.get().info(
            "team_set_map",
            team_id=team_id,
            captain_id=captain_id,
            prev_map_id=box["prev_map"],
            map_id=map_id,
            team_state=int(result.state),
            members=len(result.members),
            open_for_recruit=is_open_for_recruit(result),
        )
        return result

    async def list_open_teams(self, map_id: int, limit: int):  # noqa: ANN201, C901
        """列出正在招募的队伍(只读)。

        两段式:先从非权威索引取候选,再逐条回权威队伍记录复核。复核不通过(已满 /
        已开打 / 已解散 / 记录已没 / map 已改)的候选顺手从索引剔除(best-effort 自愈)。
        因此索引脏不会让玩家看到一支实际进不去的队伍,最坏只是这一次少返几条。

        单条 get 失败(网络抖 / 该条记录 proto 损坏)只跳过该候选并计数告警,不整单失败:
        这里返回的是候选展示列表,跳过一条不授权任何东西;真正的准入判定在 apply_to_team,
        那条路径是 fail-closed 的。索引本身读失败则照常抛错,由客户端退避重试。
        """
        max_teams = self.max_open_teams()
        if limit <= 0 or limit > max_teams:
            limit = max_teams

        candidate_limit = min(limit * OPEN_CANDIDATE_FACTOR, OPEN_CANDIDATE_MAX)
        try:
            candidates = await self.repo.list_open_team_ids(map_id, candidate_limit)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_open_list_rejected",
                reason=diag.REASON_STORE_READ_FAILED,
                map_id=map_id,
                limit=limit,
                err=str(exc),
            )
            raise

        policy = self.join_policy_proto()
        out = []
        skipped = 0
        # 就地聚合:候选逐条打日志会在「找队伍」高频轮询下把同文件的 WARN 冲走。
        skipped_read_err = skipped_not_open = 0
        skipped_committed = skipped_commit_unkn = 0

        for team_id in candidates:
            if len(out) >= limit:
                break
            try:
                team, found = await self.repo.get(team_id)
            except asyncio.CancelledError:
                raise
            except BaseException:  # noqa: BLE001
                skipped += 1
                skipped_read_err += 1
                continue
            if not found or not is_open_for_recruit(team) or (map_id > 0 and team.map_id != map_id):
                skipped_not_open += 1
                # 索引脏了。剔除时用权威记录里的 map_id(记录已没就用查询用的 map_id),
                # 保证摘的是它真正挂着的那个分桶。
                bucket = team.map_id if found else map_id
                try:
                    await self.repo.remove_open_team_candidate(team_id, bucket)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    plog.get().warning(
                        "team_open_index_prune_failed",
                        reason=diag.REASON_STORE_WRITE_FAILED,
                        team_id=team_id,
                        map_id=bucket,
                        err=str(exc),
                    )
                continue

            # 已提交对局的队伍不该出现在招募列表里。is_open_for_recruit 分不出这一档,
            # 必须查 matchmaker 权威 claim —— 与入队闸门同一份判定,避免「列表说能进、
            # 点了却被拒」。只对**已通过前面全部复核、即将返回**的候选查,RPC 次数有界。
            #
            # 与入队闸门的关键差异是**失败方向相反**:这里查不到只跳过本条(少列一支队伍),
            # 不整体报错 —— 列表是加速器,不是准入判定。
            committed = False
            commit_err: BaseException | None = None
            try:
                committed = await self.is_team_committed_to_match(team)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                commit_err = exc
            if commit_err is not None or committed:
                if commit_err is not None:
                    skipped_commit_unkn += 1
                    plog.get().warning(
                        "team_open_list_commitment_unknown",
                        reason=diag.REASON_MATCH_COMMITMENT_UNKNOWN,
                        team_id=team_id,
                        captain_id=team.captain_id,
                        err=str(commit_err),
                    )
                else:
                    skipped_committed += 1
                    try:
                        # 已确认在对局中 → 顺手摘索引自愈,下次浏览就不用再查它。
                        await self.repo.remove_open_team_candidate(team_id, team.map_id)
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:  # noqa: BLE001
                        plog.get().warning(
                            "team_open_index_prune_failed",
                            reason=diag.REASON_STORE_WRITE_FAILED,
                            team_id=team_id,
                            map_id=team.map_id,
                            err=str(exc),
                        )
                skipped += 1
                continue

            out.append(
                team_pb2.OpenTeamBrief(
                    team_id=team.team_id,
                    captain_id=team.captain_id,
                    member_count=len(team.members),
                    max_size=team.max_size,
                    map_id=team.map_id,
                    created_at_ms=team.created_at_ms,
                    join_policy=policy,
                )
            )

        if skipped > 0:
            plog.get().warning(
                "team_open_list_candidates_skipped",
                map_id=map_id,
                skipped=skipped,
                returned=len(out),
                candidates=len(candidates),
                skipped_read_err=skipped_read_err,
                skipped_not_open=skipped_not_open,
                skipped_committed=skipped_committed,
                skipped_commitment_unknown=skipped_commit_unkn,
                hint="候选被复核刷掉:读队伍失败 / 已满或已解散 / 已在对局中 / 匹配状态不确定",
            )
        elif skipped_not_open > 0:
            # 索引指向已满 / 已解散的队伍是常态自愈(不是故障),留 Debug 即可。
            plog.get().debug(
                "team_open_list_candidates_pruned",
                map_id=map_id,
                returned=len(out),
                candidates=len(candidates),
                skipped_not_open=skipped_not_open,
            )

        # 只富化最终返回的队长；候选循环中的脏/不可加入队伍不会产生额外展示查询。
        player_names, player_nos = await self._resolve_player_display_fields(
            (brief.captain_id for brief in out),
            projection="open_team_captains",
        )
        for brief in out:
            brief.captain_nickname = player_names.get(int(brief.captain_id), "")
            brief.captain_player_no = player_nos.get(int(brief.captain_id), 0)
        return out

    async def apply_to_team(self, team_id: int, applicant_id: int):  # noqa: ANN201
        """申请加入队伍。返回 (joined, team, expires_at_ms)。

        前置校验读的是权威队伍记录(不是索引):队伍存在、未解散、正在招募、申请人不在队内。
        open 路径复用 join_team(与接受邀请同一入队事务,保不变量 §1)。
        """
        # 频率配额(anti-abuse §6 第 6 项):按申请人计,先于一切读写。
        await self._allow_action("apply", applicant_id)

        try:
            team, found = await self.repo.get(team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            self._log_apply_rejected(
                diag.REASON_STORE_READ_FAILED, team_id, applicant_id, 0, exc
            )
            raise
        if not found:
            self._log_apply_rejected(
                diag.REASON_TEAM_NOT_FOUND, team_id, applicant_id, 0
            )
            raise errcode.PandoraError(
                errcode.ErrTeamNotFound, "team %d not found", team_id
            )
        if team.state == STATE_DISBANDED:
            self._log_apply_rejected(
                diag.REASON_TEAM_DISBANDED, team_id, applicant_id, team.state
            )
            raise errcode.PandoraError(
                errcode.ErrTeamWrongState, "team %d disbanded", team_id
            )
        if has_member(team, applicant_id):
            self._log_apply_rejected(
                diag.REASON_ALREADY_IN_TEAM, team_id, applicant_id, team.state
            )
            raise errcode.PandoraError(
                errcode.ErrTeamAlreadyInTeam,
                "player %d already in team %d",
                applicant_id,
                team_id,
            )
        if not is_open_for_recruit(team):
            # 满员与状态不对分开报,客户端才能给出正确提示(而不是笼统的"进不去")。
            if len(team.members) >= team.max_size:
                self._log_apply_rejected(
                    diag.REASON_TEAM_FULL, team_id, applicant_id, team.state
                )
                raise errcode.PandoraError(
                    errcode.ErrTeamFull,
                    "team %d is full (%d/%d)",
                    team_id,
                    len(team.members),
                    team.max_size,
                )
            self._log_apply_rejected(
                diag.REASON_TEAM_NOT_RECRUITING, team_id, applicant_id, team.state
            )
            raise errcode.PandoraError(
                errcode.ErrTeamWrongState,
                "team %d state %d not recruiting",
                team_id,
                int(team.state),
            )

        # open 策略:当场入队。上面的读只是快速失败,真正的满员/重复入队判定在 join_team
        # 的 WATCH/MULTI/EXEC 事务内重做一遍(读到写之间队伍可能已被别人填满)。
        if self.join_policy() == tconf.JOIN_POLICY_OPEN:
            result = await self.join_team(team_id, applicant_id)
            await self.push_update(
                applicant_id,
                member_ids(result),
                result,
                team_pb2.TEAM_UPDATE_REASON_MEMBER_JOINED,
                0,
            )
            await self.sync_open_index(result, result.map_id)
            plog.get().info(
                "team_apply_joined_open",
                team_id=team_id,
                player_id=applicant_id,
                join_policy=tconf.JOIN_POLICY_OPEN,
                new_state=int(result.state),
                members=len(result.members),
            )
            await self.log_team_composition(result)
            return True, result, 0

        # approval 策略:写申请令牌,等队长审批。
        try:
            expires_at_ms = await self.repo.claim_application(
                team_id,
                applicant_id,
                self.cfg.apply_ttl_td().total_seconds(),
                self.max_applications(),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            reason = diag.REASON_APPLICATION_STORE_FAILED
            if errcode.as_code(exc) == errcode.ErrTeamApplyPendingLimit:
                reason = diag.REASON_APPLICATION_PENDING_LIMIT
            self._log_apply_rejected(reason, team_id, applicant_id, team.state, exc)
            raise

        # 推送只是「去重查申请列表」的提示(§9.22 权威是 ListTeamApplications),丢帧最多
        # 延迟队长看到申请,不丢申请。只发队长,不打扰其他队员。
        await self.push_update(
            applicant_id,
            [team.captain_id],
            team,
            team_pb2.TEAM_UPDATE_REASON_APPLICATION_RECEIVED,
            0,
        )
        plog.get().info(
            "team_apply_pending",
            team_id=team_id,
            player_id=applicant_id,
            captain_id=team.captain_id,
            join_policy=tconf.JOIN_POLICY_APPROVAL,
            expires_at_ms=expires_at_ms,
        )
        return False, None, expires_at_ms

    def _log_apply_rejected(
        self,
        reason: str,
        team_id: int,
        applicant_id: int,
        team_state: int,
        err: BaseException | None = None,
    ) -> None:
        """收口 apply_to_team 的全部拒绝分支。

        team_state 一并打出来:满员与「不在招募」回的是不同错误码,但玩家看到的都是
        「进不去」。
        """
        plog.get().warning(
            "team_apply_rejected",
            reason=reason,
            team_id=team_id,
            player_id=applicant_id,
            team_state=int(team_state),
            err=None if err is None else str(err),
        )

    async def list_team_applications(self, team_id: int, captain_id: int):  # noqa: ANN201
        """队长查本队待处理入队申请(只读)。申请人名单不对普通成员开放。"""
        try:
            team, found = await self.repo.get(team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_list_applications_rejected",
                reason=diag.REASON_STORE_READ_FAILED,
                team_id=team_id,
                captain_id=captain_id,
                err=str(exc),
            )
            raise
        if not found:
            plog.get().warning(
                "team_list_applications_rejected",
                reason=diag.REASON_TEAM_NOT_FOUND,
                team_id=team_id,
                captain_id=captain_id,
            )
            raise errcode.PandoraError(
                errcode.ErrTeamNotFound, "team %d not found", team_id
            )
        if team.captain_id != captain_id:
            plog.get().warning(
                "team_list_applications_rejected",
                reason=diag.REASON_NOT_CAPTAIN,
                team_id=team_id,
                captain_id=captain_id,
                actual_captain_id=team.captain_id,
            )
            raise errcode.PandoraError(
                errcode.ErrTeamNotCaptain,
                "player %d is not captain of team %d",
                captain_id,
                team_id,
            )
        return await self.repo.list_applications(team_id, self.max_applications())

    async def handle_team_application(
        self, team_id: int, captain_id: int, applicant_id: int, accept: bool
    ):  # noqa: ANN201
        """队长同意 / 拒绝一份入队申请。

        定序:先用 take_application 原子取走令牌,再决定做什么。这保证同一份申请只被处理
        一次(队长连点两次"同意"→ 第二次拿不到令牌,返回 3010,不会重复入队);也保证
        "同意"与"拒绝"竞争时只有一方生效。

        已知取舍:accept 路径若在取走令牌后入队失败(期间队伍被填满 / 申请人已加入别队),
        令牌**不放回**。理由是「放回一份队长已经处理过的申请」会让队长再看到一次幽灵申请,
        比让申请人重新申请更糟(§3 宁可 fail-closed 拒一次,也不写出不自洽的状态)。
        """
        try:
            team, found = await self.repo.get(team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            self._log_handle_application_rejected(
                diag.REASON_STORE_READ_FAILED,
                team_id,
                captain_id,
                applicant_id,
                accept,
                exc,
            )
            raise
        if not found:
            self._log_handle_application_rejected(
                diag.REASON_TEAM_NOT_FOUND, team_id, captain_id, applicant_id, accept
            )
            raise errcode.PandoraError(
                errcode.ErrTeamNotFound, "team %d not found", team_id
            )
        if team.state == STATE_DISBANDED:
            self._log_handle_application_rejected(
                diag.REASON_TEAM_DISBANDED, team_id, captain_id, applicant_id, accept
            )
            raise errcode.PandoraError(
                errcode.ErrTeamWrongState, "team %d disbanded", team_id
            )
        if team.captain_id != captain_id:
            self._log_handle_application_rejected(
                diag.REASON_NOT_CAPTAIN, team_id, captain_id, applicant_id, accept
            )
            raise errcode.PandoraError(
                errcode.ErrTeamNotCaptain,
                "player %d is not captain of team %d",
                captain_id,
                team_id,
            )

        try:
            taken = await self.repo.take_application(team_id, applicant_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            self._log_handle_application_rejected(
                diag.REASON_STORE_WRITE_FAILED,
                team_id,
                captain_id,
                applicant_id,
                accept,
                exc,
            )
            raise
        if not taken:
            # 队长连点两次"同意"的第二次会走到这里(令牌已被第一次取走)。没有这条日志时,
            # 3010 在线上只落 access log 的 DEBUG,「我明明点了同意他却没进来」完全查不出。
            self._log_handle_application_rejected(
                diag.REASON_APPLICATION_NOT_FOUND,
                team_id,
                captain_id,
                applicant_id,
                accept,
            )
            raise errcode.PandoraError(
                errcode.ErrTeamApplyNotFound,
                "application of player %d to team %d not found or expired",
                applicant_id,
                team_id,
            )

        if not accept:
            # 拒绝:令牌已消耗、配额已释放,队伍状态不变。不给申请人发推送 ——
            # 申请人的等待本来就是有界的(令牌 TTL),到期即恢复可申请(§15.3)。
            #
            # msg 保持历史名不改(§11.3 msg 稳定不变);注意它表达的是「队长拒了这份申请」
            # 这个**业务结果**,与本方法入参/权限被拒的 team_handle_application_rejected
            # 不是一回事。
            plog.get().info(
                "team_application_rejected",
                team_id=team_id,
                captain_id=captain_id,
                applicant_id=applicant_id,
                accept=False,
            )
            return team

        try:
            result = await self.join_team(team_id, applicant_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # join_team 内部已按分支打了带 reason 的拒绝日志;这里补一条「令牌已消费但
            # 没进成」的收尾 —— 那是本方法唯一的已知取舍(令牌不放回),不打就成了静默丢申请。
            plog.get().warning(
                "team_application_accept_failed",
                reason=diag.REASON_JOIN_AFTER_APPLICATION_CONSUMED,
                team_id=team_id,
                captain_id=captain_id,
                applicant_id=applicant_id,
                hint="令牌已消费且不放回(避免幽灵申请),申请人需重新申请",
                err=str(exc),
            )
            raise

        await self.push_update(
            captain_id,
            member_ids(result),
            result,
            team_pb2.TEAM_UPDATE_REASON_MEMBER_JOINED,
            0,
        )
        await self.sync_open_index(result, result.map_id)
        plog.get().info(
            "team_application_accepted",
            team_id=team_id,
            captain_id=captain_id,
            applicant_id=applicant_id,
            new_state=int(result.state),
            members=len(result.members),
        )
        await self.log_team_composition(result)
        return result

    def _log_handle_application_rejected(
        self,
        reason: str,
        team_id: int,
        captain_id: int,
        applicant_id: int,
        accept: bool,
        err: BaseException | None = None,
    ) -> None:
        """收口 handle_team_application 的拒绝分支。

        accept 一起打:同一个 3010 在「同意」与「拒绝」两条路径上的含义完全不同。
        """
        plog.get().warning(
            "team_handle_application_rejected",
            reason=reason,
            team_id=team_id,
            captain_id=captain_id,
            applicant_id=applicant_id,
            accept=accept,
            err=None if err is None else str(err),
        )

    # ── 归属声明 + 孤儿自愈 ─────────────────────────────────────────────────

    async def _claim_player_healing_orphan(
        self, player_id: int, team_id: int, ttl_sec: float
    ) -> None:
        """原子声明 player→team 归属(SETNX,不变量 §1),并对孤儿索引自愈。

        判孤儿的**安全前提**(缺一不可):所有写路径都先写队伍主体、后写/改索引 ——
          - create_team:先 create 主体再 claim(本函数),因此「索引指向 X 但 X 主体
            不在」永远不会是另一个 create_team 的 in-flight 中间态;
          - accept_invite:claim 时目标队伍主体必已存在(邀请的前提)。
        若有人改成「先 claim 后写主体」,本函数会把 in-flight claim 误判孤儿并删掉,
        造成同一玩家进两支队伍 —— 违反不变量 §1,**绝对禁止**。
        """
        exist_team_id, claimed = await self.repo.claim_player(
            player_id, team_id, ttl_sec
        )
        if claimed:
            return

        # 声明失败:核对现有队伍是否真实存在。存在且未解散 = 真冲突。
        exist_team, found = await self.repo.get(exist_team_id)
        if found and exist_team.state != STATE_DISBANDED:
            # 真冲突(玩家确在他队):排查「为什么玩家进不去队」时需要知道他当前卡在哪支队,
            # 而 3004 不是 IsServerFault,线上落 rpc_ok=DEBUG 完全不可见,故必须自己打 WARN。
            plog.get().warning(
                "team_claim_conflict",
                reason=diag.REASON_ALREADY_IN_TEAM,
                player_id=player_id,
                want_team_id=team_id,
                existing_team_id=exist_team_id,
                existing_state=int(exist_team.state),
            )
            raise errcode.PandoraError(
                errcode.ErrTeamAlreadyInTeam,
                "player %d already in team %d",
                player_id,
                exist_team_id,
            )

        # 孤儿索引:队伍主体已没/已解散。CAS 清掉脏索引(仅当仍指向该 team_id)后重试声明。
        try:
            await self.repo.delete_player_index_if_matches(player_id, exist_team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_orphan_player_index_cleanup_failed",
                reason=diag.REASON_STORE_WRITE_FAILED,
                player_id=player_id,
                team_id=exist_team_id,
                err=str(exc),
            )
            raise
        plog.get().info(
            "team_orphan_player_index_healed",
            player_id=player_id,
            stale_team_id=exist_team_id,
            want_team_id=team_id,
        )

        retry_team_id, claimed = await self.repo.claim_player(
            player_id, team_id, ttl_sec
        )
        if not claimed:
            # 清理与重试之间有人抢先真建队 → 诚实报冲突(自愈后仍撞上,属并发竞争)。
            plog.get().warning(
                "team_claim_conflict_after_heal",
                reason=diag.REASON_ALREADY_IN_TEAM,
                player_id=player_id,
                want_team_id=team_id,
                existing_team_id=retry_team_id,
            )
            raise errcode.PandoraError(
                errcode.ErrTeamAlreadyInTeam,
                "player %d already in team %d",
                player_id,
                retry_team_id,
            )

    # ── 匹配联动 ────────────────────────────────────────────────────────────

    async def cancel_matchmaking(self, team_id: int, player_id: int) -> None:
        """成员离开队伍(主动离队 / 被踢)后 best-effort 撤销其匹配票据。

        不撤销时票据里仍含已离队成员,成局会把他拉进战斗;其残留 claim 也会阻塞他加入的
        新队伍 StartMatch(4002)。

        弱依赖语义:
          - canceler 为 None(未配 matchmaker_addr)→ 跳过,行为与历史一致;
          - ErrMatchNotFound(4001)= 该成员本就没在排队,常态,静默;
          - 其余错误仅 warn 不阻断离队(残留票据由确认期超时 / TTL 兜底回收)。
        """
        if self.match_canceler is None:
            return
        try:
            await self.match_canceler.cancel_match(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            if errcode.as_code(exc) == errcode.ErrMatchNotFound:
                return  # 未在排队,常态
            plog.get().warning(
                "team_cancel_matchmaking_failed",
                reason=diag.REASON_MATCHMAKER_CANCEL_FAILED,
                team_id=team_id,
                player_id=player_id,
                err=str(exc),
                hint="票据仍含已离队成员,靠确认期超时 / TTL 兜底回收",
            )
            return
        # 撤票是跨服务的不可逆推进(整张票据连同队友一起退回队列)。
        plog.get().info(
            "team_matchmaking_cancelled_on_leave", team_id=team_id, player_id=player_id
        )

    # ── push 辅助 ───────────────────────────────────────────────────────────

    async def push_update(
        self,
        caller_player_id: int,
        to_player_ids: list[int],
        team,  # noqa: ANN001
        reason: int,
        invite_id: int,
    ) -> None:
        """把 TeamUpdateEvent 序列化后经 kafka 扇出。pusher 为 None 时直接跳过。

        每个接收方**单独序列化**一条 TeamUpdateEvent,使 to_player_id 字段精确标识接收方。
        kafka key = player_id(不变量 §9)由 push_to_players 内部保证;它同时排除
        caller_player_id(原则 2)。
        """
        if self.pusher is None or not to_player_ids:
            return
        ts = now_ms()
        proto_team = await self.team_to_proto(team)
        for pid in to_player_ids:
            event = team_pb2.TeamUpdateEvent(
                team=proto_team,
                by_player_id=caller_player_id,
                to_player_id=pid,  # 每条消息精确标识接收方,客户端可直接读取
                ts_ms=ts,
                reason=reason,
                invite_id=invite_id,
            )
            try:
                payload = event.SerializeToString()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "team_push_marshal_failed",
                    reason=diag.REASON_PUSH_MARSHAL_FAILED,
                    team_id=team.team_id,
                    to_player_id=pid,
                    update_reason=int(reason),
                    err=str(exc),
                )
                continue
            try:
                _sent, err = await self.pusher.push_to_players(
                    caller_player_id, [pid], payload, 0
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                err = exc
            if err is not None:
                if invite_id != 0:
                    # legacy 路径承载邀请的推送丢了 → 计入邀请推送失败指标(可告警);
                    # 被邀请人靠 ListMyPendingInvites 拉取兜底。
                    tmetrics.INVITE_PUSH_FAILED.labels(path="legacy").inc()
                # ⚠️ update_reason 不能挂在 "reason" 字段上:§11.3 R2 规定 reason =
                # 枚举化拒绝原因,撞名会让按 reason 聚合把两类混在一起。
                plog.get().warning(
                    "team_push_failed",
                    reason=diag.REASON_PUSH_PRODUCE_FAILED,
                    team_id=team.team_id,
                    to_player_id=pid,
                    update_reason=int(reason),
                    err=str(err),
                    hint="推送是加速器不是权威;客户端靠 GetMyTeam / ListMyPendingInvites 拉取兜底",
                )

    async def push_invite(
        self, inviter_id: int, target_player_id: int, team_id: int, invite_id: int
    ) -> None:
        """构造独立的 TeamInviteEvent 并以 event_type=INVITE(=1)推送给被邀请人。"""
        if self.pusher is None or target_player_id == 0:
            return
        # ts 与 expires 用同一个基准,避免两次取时产生偏差。
        ts = now_ms()
        event = team_pb2.TeamInviteEvent(
            team_id=team_id,
            invite_id=invite_id,
            inviter_id=inviter_id,
            to_player_id=target_player_id,
            ts_ms=ts,
            expires_at_ms=ts + self.invite_ttl_ms(),
        )
        try:
            payload = event.SerializeToString()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 序列化失败只记告警;邀请令牌已落库,不能把推送弱依赖反向变成业务失败。
            tmetrics.INVITE_PUSH_FAILED.labels(path="dedicated").inc()
            plog.get().warning(
                "team_invite_marshal_failed",
                reason=diag.REASON_PUSH_MARSHAL_FAILED,
                team_id=team_id,
                to_player_id=target_player_id,
                invite_id=invite_id,
                err=str(exc),
            )
            return
        event_type = int(team_pb2.TEAM_PUSH_EVENT_TYPE_INVITE)
        try:
            _sent, err = await self.pusher.push_to_players(
                inviter_id, [target_player_id], payload, event_type
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            err = exc
        if err is not None:
            tmetrics.INVITE_PUSH_FAILED.labels(path="dedicated").inc()
            plog.get().warning(
                "team_invite_push_failed",
                reason=diag.REASON_PUSH_PRODUCE_FAILED,
                team_id=team_id,
                to_player_id=target_player_id,
                invite_id=invite_id,
                err=str(err),
                hint="被邀请人靠 ListMyPendingInvites 拉取兜底",
            )

    # ── 索引 / TTL 辅助 ─────────────────────────────────────────────────────

    async def sync_open_index(self, team, prev_map_id: int) -> None:  # noqa: ANN001
        """把队伍在开放招募索引里的存在性同步为当前权威状态。

        索引是非权威投影(§9.22):写失败只告警,**绝不回滚已提交的队伍状态机迁移** ——
        为了一个用来"找候选"的加速结构而回退已经落地的入队/离队,才是真的破坏正确性。

        score 取「现在 + active_ttl」,与队伍 key 的 TTL 同源:队伍 key 到期消失时索引
        成员恰好也过期,ZREMRANGEBYSCORE 一扫即净,不留悬挂。
        """
        if team is None or team.team_id == 0:
            return
        ttl = self.active_ttl_sec()
        expires_at_ms = now_ms() + int(ttl * 1000)
        open_flag = is_open_for_recruit(team)
        try:
            await self.repo.sync_open_team(
                team.team_id, team.map_id, prev_map_id, open_flag, expires_at_ms, ttl
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_open_index_sync_failed",
                reason=diag.REASON_STORE_WRITE_FAILED,
                team_id=team.team_id,
                map_id=team.map_id,
                prev_map_id=prev_map_id,
                open=open_flag,
                err=str(exc),
            )

    async def refresh_disbanded_ttl(self, team_id: int, ttl_sec: float) -> None:
        """用短 TTL 刷新已解散队伍的 key。单条 EXPIRE 即可,无需再走一轮空写。"""
        try:
            await self.repo.expire_team(team_id, ttl_sec)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_refresh_disbanded_ttl_failed",
                reason=diag.REASON_STORE_WRITE_FAILED,
                team_id=team_id,
                err=str(exc),
            )

    async def _maybe_touch_team(self, team, player_id: int) -> None:  # noqa: ANN001
        """在线心跳保活:玩家仍在轮询自己的队伍 → 续期队伍与索引 TTL。

        同时刷新开放队伍索引的 score:索引成员的 score 是「索引项最晚存活时刻」,与队伍
        key 的 TTL 同源。只续队伍 key 而不续索引,会让一支持续在线、持续招募的队伍在
        active_ttl 后被 ZREMRANGEBYSCORE 当成过期项清掉,从"获取队伍"列表里静默消失
        (队伍还活着但没人找得到)。节流间隔(15min)远小于 active_ttl(60min)。
        """
        now = time.monotonic()
        last = self._last_touch.get(player_id)
        if last is not None and now - last < TOUCH_INTERVAL_SEC:
            return
        self._last_touch[player_id] = now
        self._sweep_last_touch(now)
        try:
            await self.repo.touch_team(team.team_id, player_id, self.active_ttl_sec())
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_touch_failed",
                reason=diag.REASON_STORE_WRITE_FAILED,
                player_id=player_id,
                team_id=team.team_id,
                err=str(exc),
                hint="队伍与开放索引未续期,持续失败会让在线队伍被 active_ttl 误回收",
            )
        await self.sync_open_index(team, team.map_id)

    def _sweep_last_touch(self, now: float) -> None:
        """惰性清扫已过节流窗口的条目,防止长跑进程内存随历史活跃玩家数无界增长。

        删除「距上次续期 ≥ interval」的条目与直接不存在等价(下次 Load 反正会放行续期),
        行为不变。每 interval 一次全量扫描 O(活跃玩家数),可忽略。
        """
        if now - self._last_touch_sweep_at < TOUCH_INTERVAL_SEC:
            return
        self._last_touch_sweep_at = now
        stale = [k for k, v in self._last_touch.items() if now - v >= TOUCH_INTERVAL_SEC]
        for k in stale:
            self._last_touch.pop(k, None)

    # ── 分片观测(nil-safe)──────────────────────────────────────────────────

    async def log_team_composition(self, team) -> None:  # noqa: ANN001
        """router 注入后,把一次成员变更后的 region 分布打成观测日志。

        仅可观测,不改队伍路径:队伍 redis 按 owner cell 分片 / battle DS 跨 region 放置
        属基础设施。router 为 None(单 Cell)时不打,行为不变。
        """
        if self.router is None or team is None:
            return
        regions: list[int] = []
        for m in team.members:
            if m.player_id == 0:
                continue
            try:
                loc = self.router.route(m.player_id)
            except Exception:  # noqa: BLE001 —— 尽力解析,不阻断
                continue
            regions.append(loc.region_id)
        distinct = sorted(set(regions))
        plog.get().debug(
            "team_composition_routing",
            team_id=team.team_id,
            captain_id=team.captain_id,
            member_count=len(team.members),
            region_count=len(distinct),
            cross_region=len(distinct) > 1,
            # 队伍锚定队长 owner cell(TeamShardKey=captain_id);**不取 team_id**
            # (snowflake 与落点无关,误用会让队伍与队长 owner 数据落不同 cell)。
            shard_key=str(team.captain_id),
        )

    # ── 类型转换 ────────────────────────────────────────────────────────────

    async def _resolve_player_display_fields(
        self,
        player_ids,  # noqa: ANN001
        *,
        projection: str,
        team_id: int = 0,
    ) -> tuple[dict[int, str], dict[int, int]]:
        """并发批量读取客户端视图需要的权威名字和编号。

        输入先按原顺序去零、去重；两个依赖各最多调用一次，任一失败只让对应字段
        留空。取消必须继续向上传播，不能伪装成一次成功的降级响应。
        """
        unique_ids: list[int] = []
        seen_ids: set[int] = set()
        for raw_player_id in player_ids:
            player_id = int(raw_player_id)
            if player_id <= 0 or player_id in seen_ids:
                continue
            seen_ids.add(player_id)
            unique_ids.append(player_id)

        async def _resolve_player_nos() -> dict[int, int]:
            if self.player_no_resolver is None or not unique_ids:
                return {}
            try:
                values = await self.player_no_resolver.resolve_player_nos(unique_ids)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 展示弱依赖必须 fail-soft
                ok, streak = self._player_no_resolve_log.admit(
                    now_ms(), PLAYER_NO_RESOLVE_LOG_WINDOW_MS
                )
                if ok:
                    plog.get().warning(
                        "team_player_no_resolve_failed",
                        projection=projection,
                        team_id=team_id,
                        player_count=len(unique_ids),
                        streak=streak,
                        err=str(exc),
                    )
                return {}
            else:
                failed_total, _ = self._player_no_resolve_log.recovered()
                if failed_total > 0:
                    plog.get().info(
                        "team_player_no_resolve_recovered", failed_total=failed_total
                    )
                return values

        async def _resolve_player_names() -> dict[int, str]:
            if self.player_name_resolver is None or not unique_ids:
                return {}
            try:
                values = await self.player_name_resolver.resolve_player_names(unique_ids)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 展示弱依赖必须 fail-soft
                ok, streak = self._player_name_resolve_log.admit(
                    now_ms(), PLAYER_NAME_RESOLVE_LOG_WINDOW_MS
                )
                if ok:
                    plog.get().warning(
                        "team_player_name_resolve_failed",
                        projection=projection,
                        team_id=team_id,
                        player_count=len(unique_ids),
                        streak=streak,
                        err=str(exc),
                    )
                return {}
            else:
                failed_total, _ = self._player_name_resolve_log.recovered()
                if failed_total > 0:
                    plog.get().info(
                        "team_player_name_resolve_recovered", failed_total=failed_total
                    )
                return values

        # 两个展示弱依赖无数据依赖，并发把队伍读/推送额外延迟限制为
        # max(name, no)，而不是 name + no；两侧都各自严格单 batch。
        player_names, player_nos = await asyncio.gather(
            _resolve_player_names(), _resolve_player_nos()
        )
        return player_names, player_nos

    async def team_to_proto(self, r):  # noqa: ANN001, ANN201
        """把存储快照 TeamStorageRecord 转成客户端可见结构 Team(不变量 §9.14)。

        join_policy 不是存储字段,而是**每次组装时从服务端配置派生**(§9.11 / §9.22):
        改配置即时对全服生效,也不存在「队伍里存的策略」与配置漂移的问题。因此本转换
        必须是 TeamUsecase 的方法而不是自由函数 —— 离开 usecase 就拿不到权威配置,
        只能填 UNSPECIFIED,客户端就会一直按保守策略渲染。
        """
        if r is None:
            return None

        player_names, player_nos = await self._resolve_player_display_fields(
            (member.player_id for member in r.members),
            projection="team_members",
            team_id=int(r.team_id),
        )
        return team_pb2.Team(
            team_id=r.team_id,
            captain_id=r.captain_id,
            members=[
                team_pb2.TeamMember(
                    player_id=m.player_id,
                    player_no=player_nos.get(int(m.player_id), 0),
                    nickname=player_names.get(int(m.player_id), ""),
                    mmr=m.mmr,
                    ready=m.ready,
                    hero_id=m.hero_id,
                )
                for m in r.members
            ],
            state=r.state,
            created_at_ms=r.created_at_ms,
            max_size=r.max_size,
            map_id=r.map_id,
            join_policy=self.join_policy_proto(),
        )

    async def team_applications_to_proto(
        self, team_id: int, records  # noqa: ANN001
    ) -> list[team_pb2.TeamApplication]:
        """把申请存储记录投影为队长可见视图，不把展示字段写回 Redis。"""
        player_names, player_nos = await self._resolve_player_display_fields(
            (record.player_id for record in records if record is not None),
            projection="team_applications",
            team_id=team_id,
        )
        return [
            team_pb2.TeamApplication(
                player_id=record.player_id,
                expires_at_ms=record.expires_at_ms,
                nickname=player_names.get(int(record.player_id), ""),
                player_no=player_nos.get(int(record.player_id), 0),
            )
            for record in records
            if record is not None
        ]
