"""guild 业务逻辑层 —— 对应 Go 侧 internal/biz/{guild,group,sweep}.go。

职责:
  - 公会:创建 / 申请 / 审批 / 退会 / 踢人 / 解散 / 转让会长 / 任命官员 / 查询
  - 临时群:建群 / 拉人 / 退群 / 踢人 / 解散 / 转让群主 / 查询
  - 成员变更经 kafka `pandora.guild.event` → push 推给接收方(**弱依赖**,None 静默跳过)

关键规则(每条都对应一个"改了不报错"的坑):

  ① LEADER / OWNER 不能直接退会 / 退群 —— 必须先转让或解散。
     放开的话公会 / 群会变成**无主**:没人能审批、没人能解散,只能人工改库。
  ② 推送原则 2:通知**不回发操作者本人**(申请通知发给会长 / 官员;审批结果发给申请人)。
     唯一例外是解散 —— 那是全员事件,会长自己也要收到。
  ③ 申请列表的 nickname / player_no 从各自权威按需投影，不写回公会库。
  ④ RPC 只回客户端可见结构(§9.14):Guild / GuildMember / GuildJoinRequest,
     不回存储行。

★ 缓存是**显式弱依赖**:读失败只掉命中率(权威读走 MySQL),写后删失败靠短 TTL 兜底。
  但"弱依赖"不等于"可以不打日志" —— 它仍是 Redis 健康度的信号,所以保留、限流。
"""

from __future__ import annotations

import time
from typing import Protocol

from pandora.group.v1 import group_pb2
from pandora.guild.v1 import guild_pb2

from pandorapy import errcode, logwindow
from pandorapy import log as plog
from pandorapy.services import player_display
from pandorapy.services.guild.rows import (
    GUILD_ROLE_LEADER,
    GUILD_ROLE_MEMBER,
    GUILD_ROLE_OFFICER,
    GroupRow,
    GuildRow,
)

# 缓存降级日志的最小重打间隔(毫秒),与 Go 的 guildCacheLogWindowMs 同值。
#
# GetGuild / GetMyGuild 是「全服共享热 key」的高 QPS 只读入口:Redis 一抖就按请求量
# 刷屏,而且一次请求可能叠 get + 回填 set 两条。读 / 写两侧分开计数便于区分故障面;
# 刻意**不按 key 分桶**(guild_id / player_id 是高基数,分桶会随实体数无界增长)。
GUILD_CACHE_LOG_WINDOW_MS = 5000

# 分页上限(decision-revisit-list-pagination.md)。
DEFAULT_PAGE_LIMIT = 50
MAX_PAGE_LIMIT = 100


def _now_ms() -> int:
    return int(time.time() * 1000)


def clamp_limit(limit: int) -> int:
    """limit<=0 → 默认 50;>100 → 钳到 100。§9.18 读取侧上限。"""
    if limit <= 0:
        return DEFAULT_PAGE_LIMIT
    if limit > MAX_PAGE_LIMIT:
        return MAX_PAGE_LIMIT
    return limit


class GuildEventPusher(Protocol):
    """把公会事件发到 kafka。kafka key = to_player_id(§9.9 同接收方保序)。"""

    async def push_guild_event(self, to_player_id: int, evt: guild_pb2.GuildEvent) -> None: ...


class ActionRateQuota(Protocol):
    """申请类写入的 per-player 频率配额(anti-abuse §6 第 6 项)。

    ★ 返回 `(是否放行, 故障)`,与 Go 的 `Allow(...) (bool, error)` 同形 ——
    **不是**裸 bool。Protocol 运行期不做检查,照着 `-> bool` 写实现的话测试照样绿,
    而 `_allow_action` 的 `ok, quota_exc = await ...` 会在每次申请上抛
    `TypeError: cannot unpack non-iterable bool object`。
    """

    async def allow(self, action: str, subject: int) -> tuple[bool, Exception | None]: ...


def to_guild_view(g: GuildRow) -> guild_pb2.Guild:
    """存储行 → 客户端可见 Guild(§9.14)。"""
    return guild_pb2.Guild(
        guild_id=g.guild_id,
        name=g.name,
        leader_id=g.leader_id,
        member_count=g.member_count,
        max_members=g.max_members,
        created_ms=g.created_ms,
    )


def to_group_view(g: GroupRow) -> group_pb2.Group:
    return group_pb2.Group(
        group_id=g.group_id,
        name=g.name,
        owner_id=g.owner_id,
        member_count=g.member_count,
        max_members=g.max_members,
        created_ms=g.created_ms,
    )


def dedup_exclude(ids: list[int], exclude: int) -> list[int]:
    """去重并排除 exclude(建群初始成员清洗)。保持原始顺序。"""
    seen: set[int] = set()
    out: list[int] = []
    for i in ids:
        if i == 0 or i == exclude or i in seen:
            continue
        seen.add(i)
        out.append(i)
    return out


class GuildUsecase:
    """公会业务逻辑核心。对应 Go 的 `biz.GuildUsecase`。

    cache / pusher / rate_quota 均允许为 None(弱依赖未配置时降级)。
    """

    __slots__ = (
        "_repo",
        "_cache",
        "_pusher",
        "_cfg",
        "_cache_ttl_sec",
        "_rate_quota",
        "_cache_read_log",
        "_cache_write_log",
        "_player_name_resolver",
        "_player_no_resolver",
    )

    def __init__(self, repo, cache, pusher, cfg) -> None:  # noqa: ANN001
        self._repo = repo
        self._cache = cache
        self._pusher = pusher
        self._cfg = cfg
        self._cache_ttl_sec = cfg.cache_ttl_td().total_seconds()
        self._rate_quota: ActionRateQuota | None = None
        self._cache_read_log = logwindow.Window()
        self._cache_write_log = logwindow.Window()
        self._player_name_resolver: player_display.PlayerNameResolver | None = None
        self._player_no_resolver: player_display.PlayerNoResolver | None = None

    def set_rate_quota(self, quota: ActionRateQuota | None) -> None:
        """注入频率配额(可选;不注入 = 不限,dev 无 Redis 联调兼容)。"""
        self._rate_quota = quota

    def set_player_name_resolver(
        self, resolver: player_display.PlayerNameResolver | None
    ) -> None:
        """注入 player 角色昵称权威；未配置时仅省略该展示投影。"""
        self._player_name_resolver = resolver

    def set_player_no_resolver(
        self, resolver: player_display.PlayerNoResolver | None
    ) -> None:
        """注入 login 玩家编号权威；未配置时仅省略该展示投影。"""
        self._player_no_resolver = resolver

    # ── 频率配额门 ─────────────────────────────────────────────────────────

    async def _allow_action(self, action: str, player_id: int) -> None:
        """窗内超额抛 ErrRateLimited(**先于一切副作用**);判定本身出错 fail-open。

        ★ fail-open 是刻意的:这是**背压**门,不是权威门。Redis 抖一下就把所有人的
        入会申请拒掉,比放过几个超频请求糟得多 —— 总量闸(每公会 pending 200)
        仍在事务里守着,不会因为这里放行而被突破。
        """
        if self._rate_quota is None:
            return
        # ★ 读 `(ok, exc)` 而不是等异常:`allow()` 从不抛,原先那段
        #   `except ...: log("guild_rate_quota_check_failed")` 是**不可达死代码**,
        #   于是这个 Loki 键在代码里存在、永远不会触发。
        ok, quota_exc = await self._rate_quota.allow(action, player_id)
        if quota_exc is not None:
            # 背压门判定失败一律放行(§9.20:限流不得卡玩家)。
            plog.get().warning(
                "guild_rate_quota_check_failed",
                action=action,
                player_id=player_id,
                err=str(quota_exc),
                fail_open=True,
            )
            return
        if not ok:
            plog.get().warning("guild_rate_quota_rejected", action=action, player_id=player_id)
            raise errcode.PandoraError(
                errcode.ErrRateLimited, "guild %s rate limited, retry later", action
            )

    # ── 写路径 ─────────────────────────────────────────────────────────────

    async def create_guild(self, player_id: int, name: str, new_guild_id: int) -> int:
        name = name.strip()
        if not name:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "guild name required")
        # ★ 按 **rune 数**而不是字节数:中文公会名一个字 3 字节,按字节算等于只让写 8 个字。
        if len(name) > self._cfg.max_name_len:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "guild name too long")
        await self._repo.create_guild(
            new_guild_id, player_id, name, self._cfg.max_guild_members
        )
        # 写后删:创建者成为会长 → 失效其 member 反查缓存。
        # 不删的话,旧的「所属某公会」反查残留会让 GetMyGuild 在 TTL 内继续返回旧公会。
        await self._invalidate_member(player_id)
        return new_guild_id

    async def apply_join(self, player_id: int, guild_id: int, new_request_id: int) -> int:
        if guild_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "guild_id required")
        # 频率配额按申请人计,**先于一切读写** —— 放在后面就等于先付了读库的代价。
        await self._allow_action("apply", player_id)
        m = await self._repo.get_member(player_id)
        if m is not None:
            raise errcode.PandoraError(
                errcode.ErrGuildAlreadyInGuild,
                "player %d already in guild %d",
                player_id,
                m.guild_id,
            )
        g = await self._repo.get_guild(guild_id)
        if g is None:
            raise errcode.PandoraError(errcode.ErrGuildNotFound, "guild %d not found", guild_id)

        request_id, _reused = await self._repo.create_join_request(
            new_request_id, guild_id, player_id, self._cfg.max_pending_requests_per_guild
        )
        # 推送:通知会长 / 官员有人申请(原则 2:不发申请人本人)。
        await self._fanout_to_managers(
            guild_id,
            guild_pb2.GuildEvent(
                type=guild_pb2.GUILD_EVENT_TYPE_JOIN_APPLIED,
                guild_id=guild_id,
                actor_id=player_id,
                guild_name=g.name,
            ),
        )
        return request_id

    async def approve_join(self, approver_id: int, request_id: int) -> None:
        if request_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "request_id required")
        rq = await self._repo.get_request(request_id)
        if rq is None:
            raise errcode.PandoraError(
                errcode.ErrGuildRequestInvalid, "request %d not found", request_id
            )
        approved = await self._repo.approve_join(
            request_id, approver_id, self._cfg.max_guild_members
        )
        if not approved:
            raise errcode.PandoraError(
                errcode.ErrGuildRequestInvalid, "request %d not pending", request_id
            )
        # 写后删:成员数++ → 失效公会资料;申请人新入会 → 失效其 member 反查。
        await self._invalidate_guild(rq.guild_id)
        await self._invalidate_member(rq.player_id)
        g = await self._repo.get_guild(rq.guild_id)
        await self._push(
            rq.player_id,
            guild_pb2.GuildEvent(
                type=guild_pb2.GUILD_EVENT_TYPE_JOIN_APPROVED,
                guild_id=rq.guild_id,
                actor_id=approver_id,
                guild_name=g.name if g else "",
            ),
        )

    async def reject_join(self, approver_id: int, request_id: int) -> None:
        if request_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "request_id required")
        rq = await self._repo.get_request(request_id)
        if rq is None:
            raise errcode.PandoraError(
                errcode.ErrGuildRequestInvalid, "request %d not found", request_id
            )
        rejected = await self._repo.reject_join(request_id, approver_id)
        if not rejected:
            raise errcode.PandoraError(
                errcode.ErrGuildRequestInvalid, "request %d not pending", request_id
            )
        await self._push(
            rq.player_id,
            guild_pb2.GuildEvent(
                type=guild_pb2.GUILD_EVENT_TYPE_JOIN_REJECTED,
                guild_id=rq.guild_id,
                actor_id=approver_id,
            ),
        )

    async def leave_guild(self, player_id: int) -> None:
        m = await self._repo.get_member(player_id)
        if m is None:
            raise errcode.PandoraError(
                errcode.ErrGuildNotMember, "player %d not in any guild", player_id
            )
        if m.role == GUILD_ROLE_LEADER:
            raise errcode.PandoraError(
                errcode.ErrGuildNotLeader, "leader must transfer or disband before leaving"
            )
        await self._repo.remove_member(m.guild_id, player_id)
        await self._invalidate_guild(m.guild_id)
        await self._invalidate_member(player_id)

    async def kick_member(self, operator_id: int, target_id: int) -> None:
        """踢人。LEADER 可踢任意非会长成员;OFFICER 只能踢普通成员。

        这里的判定是**给出友好错误码**用的;权威复核在 repo 事务里
        (持父行锁复读职位),两处都要有 —— 少了 biz 这层会让越权尝试拿到
        含糊的错误,少了 repo 那层则是真的能越权。
        """
        if operator_id == target_id:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "cannot kick self")
        op = await self._repo.get_member(operator_id)
        if op is None:
            raise errcode.PandoraError(
                errcode.ErrGuildNotMember, "operator %d not in any guild", operator_id
            )
        target = await self._repo.get_member(target_id)
        if target is None or target.guild_id != op.guild_id:
            raise errcode.PandoraError(
                errcode.ErrGuildNotMember, "target %d not in operator's guild", target_id
            )
        if target.role == GUILD_ROLE_LEADER:
            raise errcode.PandoraError(errcode.ErrGuildNoPermission, "cannot kick the leader")
        if op.role == GUILD_ROLE_LEADER:
            pass  # 可踢 officer / member
        elif op.role == GUILD_ROLE_OFFICER:
            if target.role != GUILD_ROLE_MEMBER:
                raise errcode.PandoraError(
                    errcode.ErrGuildNoPermission, "officer can only kick members"
                )
        else:
            raise errcode.PandoraError(errcode.ErrGuildNoPermission, "member cannot kick")
        await self._repo.kick_member(op.guild_id, operator_id, target_id)
        await self._invalidate_guild(op.guild_id)
        await self._invalidate_member(target_id)
        g = await self._repo.get_guild(op.guild_id)
        await self._push(
            target_id,
            guild_pb2.GuildEvent(
                type=guild_pb2.GUILD_EVENT_TYPE_KICKED,
                guild_id=op.guild_id,
                actor_id=operator_id,
                guild_name=g.name if g else "",
            ),
        )

    async def disband_guild(self, leader_id: int) -> None:
        m = await self._repo.get_member(leader_id)
        if m is None:
            raise errcode.PandoraError(
                errcode.ErrGuildNotMember, "player %d not in any guild", leader_id
            )
        if m.role != GUILD_ROLE_LEADER:
            raise errcode.PandoraError(errcode.ErrGuildNotLeader, "only leader can disband")
        g = await self._repo.get_guild(m.guild_id)
        guild_name = g.name if g else ""
        # 成员集合由解散事务在持父行锁时读取并原子返回 —— 与删除同事务,
        # 不会漏掉"快照后并发批准的新成员"(漏掉的那个会被删却收不到通知)。
        deleted_members = await self._repo.disband_guild(m.guild_id, leader_id)
        await self._invalidate_guild(m.guild_id)
        for pid in deleted_members:
            await self._invalidate_member(pid)
        # 通知全体成员(**含会长本人** —— 解散是全员事件,例外于原则 2)。
        for pid in deleted_members:
            await self._push(
                pid,
                guild_pb2.GuildEvent(
                    type=guild_pb2.GUILD_EVENT_TYPE_DISBANDED,
                    guild_id=m.guild_id,
                    actor_id=leader_id,
                    guild_name=guild_name,
                ),
            )

    async def transfer_leader(self, leader_id: int, target_id: int) -> None:
        if leader_id == target_id:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "cannot transfer to self")
        m = await self._repo.get_member(leader_id)
        if m is None or m.role != GUILD_ROLE_LEADER:
            raise errcode.PandoraError(errcode.ErrGuildNotLeader, "only leader can transfer")
        await self._repo.transfer_leader(m.guild_id, leader_id, target_id)
        # 写后删:leader_id 变了 → 失效公会资料(成员集合不变,member 反查无需动)。
        await self._invalidate_guild(m.guild_id)
        g = await self._repo.get_guild(m.guild_id)
        guild_name = g.name if g else ""
        members = await self._repo.list_members(m.guild_id, 0, 0)
        for mem in members:
            await self._push(
                mem.player_id,
                guild_pb2.GuildEvent(
                    type=guild_pb2.GUILD_EVENT_TYPE_LEADER_CHANGED,
                    guild_id=m.guild_id,
                    actor_id=target_id,
                    guild_name=guild_name,
                ),
            )

    async def set_officer(self, leader_id: int, target_id: int, is_officer: bool) -> None:
        if leader_id == target_id:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "cannot set officer on self")
        m = await self._repo.get_member(leader_id)
        if m is None or m.role != GUILD_ROLE_LEADER:
            raise errcode.PandoraError(errcode.ErrGuildNotLeader, "only leader can set officer")
        target = await self._repo.get_member(target_id)
        if target is None or target.guild_id != m.guild_id:
            raise errcode.PandoraError(
                errcode.ErrGuildNotMember, "target %d not in guild", target_id
            )
        if target.role == GUILD_ROLE_LEADER:
            raise errcode.PandoraError(errcode.ErrGuildNoPermission, "target is leader")
        role = GUILD_ROLE_OFFICER if is_officer else GUILD_ROLE_MEMBER
        # ★ 与 Go 一致:SetRole 之后**不动缓存** —— 职位不在 Guild 快照里
        #   (快照只有 guild_id/name/leader_id/member_count/max_members/created_ms),
        #   多删一次缓存只是白掉命中率。
        await self._repo.set_role(m.guild_id, leader_id, target_id, role)

    # ── 读路径(cache-aside)──────────────────────────────────────────────

    async def get_guild(self, guild_id: int) -> guild_pb2.Guild:
        if self._cache is not None:
            try:
                cached = await self._cache.get_guild(guild_id)
            except Exception as exc:  # noqa: BLE001 —— 缓存弱依赖,记录后回落 MySQL
                self._warn_cache_read("guild_cache_get_failed", guild_id=guild_id, err=str(exc))
            else:
                self._note_cache_read_recovered()
                if cached is not None:
                    return to_guild_view(cached)
        g = await self._repo.get_guild(guild_id)
        if g is None:
            raise errcode.PandoraError(errcode.ErrGuildNotFound, "guild %d not found", guild_id)
        await self._fill_guild_cache(g)
        return to_guild_view(g)

    async def get_my_guild(self, player_id: int) -> guild_pb2.Guild | None:
        """不在任何公会返回 None(service 回 OK + 空 guild,不是错误)。"""
        if self._cache is not None:
            try:
                guild_id = await self._cache.get_member_guild_id(player_id)
            except Exception as exc:  # noqa: BLE001
                self._warn_cache_read(
                    "guild_member_cache_get_failed", player_id=player_id, err=str(exc)
                )
                guild_id = 0
            if guild_id:
                try:
                    cached = await self._cache.get_guild(guild_id)
                except Exception as exc:  # noqa: BLE001
                    self._warn_cache_read(
                        "guild_cache_get_failed", guild_id=guild_id, err=str(exc)
                    )
                else:
                    if cached is not None:
                        return to_guild_view(cached)
                # member 命中但 info miss / 读故障 → 落到权威读取并重建缓存。
        g = await self._repo.get_my_guild(player_id)
        if g is None:
            # 权威说不在任何公会 → 清掉可能残留的陈旧反查缓存(写路径删失败时的自愈)。
            await self._invalidate_member(player_id)
            return None
        await self._fill_guild_cache(g)
        await self._fill_member_cache(player_id, g.guild_id)
        return to_guild_view(g)

    async def get_player_guild_id(self, player_id: int) -> int:
        """反查玩家当前公会编号(内部只读,DS 出生编制专用)。返回 0 = 不在任何公会。

        ★ 刻意**不复用 get_my_guild**:那条是玩家面板的 cache-aside 读,
          member→guild 反查命中即返回、不回源权威。同一次陈旧读在两条路径上的
          代价差一个量级 —— 玩家面板读到旧值只是晚一拍,下次拉取自愈;
          而 DS 只在进场时查这一次,陈旧值一旦写到实体上就会复制给全场并
          **整场不再纠正**(会友被显示成路人,或反过来)。
          写路径删缓存失败时仅告警、靠 TTL 兜底,所以"删缓存基本都成功"
          不足以让 DS 这一侧也吃缓存。

        权威读到的结果顺手回填缓存,玩家面板那条路径照常受益。
        """
        g = await self._repo.get_my_guild(player_id)
        if g is None:
            await self._invalidate_member(player_id)
            return 0
        await self._fill_guild_cache(g)
        await self._fill_member_cache(player_id, g.guild_id)
        return g.guild_id

    async def list_members(
        self, guild_id: int, cursor: int, limit: int
    ) -> tuple[list[guild_pb2.GuildMember], int]:
        limit = clamp_limit(limit)
        rows = await self._repo.list_members(guild_id, cursor, limit)
        out = [
            guild_pb2.GuildMember(
                player_id=m.player_id, role=m.role, joined_ms=m.joined_ms
            )
            for m in rows
        ]
        # 满页才给下一页游标;不满页 next=0 表示到底了。
        next_cursor = rows[-1].player_id if len(rows) == limit else 0
        return out, next_cursor

    async def list_join_requests(
        self, requester_id: int, cursor: int, limit: int
    ) -> tuple[list[guild_pb2.GuildJoinRequest], int]:
        """列公会挂起申请并投影公开昵称/编号。

        requester 须为该公会 LEADER / OFFICER，按 request_id 游标分页；展示权威
        任一失败只省略对应字段，审批身份始终是 request_id。
        """
        limit = clamp_limit(limit)
        m = await self._repo.get_member(requester_id)
        if m is None:
            raise errcode.PandoraError(
                errcode.ErrGuildNotMember, "player %d not in any guild", requester_id
            )
        if m.role not in (GUILD_ROLE_LEADER, GUILD_ROLE_OFFICER):
            raise errcode.PandoraError(
                errcode.ErrGuildNoPermission, "only leader/officer can list requests"
            )
        rows = await self._repo.list_pending_requests(m.guild_id, cursor, limit)
        names, numbers = await player_display.resolve_player_display(
            [row.player_id for row in rows],
            self._player_name_resolver,
            self._player_no_resolver,
            service="guild",
        )
        out = [
            guild_pb2.GuildJoinRequest(
                request_id=rq.request_id,
                guild_id=rq.guild_id,
                from_player_id=rq.player_id,
                from_nickname=names.get(rq.player_id, ""),
                created_ms=rq.created_ms,
                from_player_no=numbers.get(rq.player_id, 0),
            )
            for rq in rows
        ]
        next_cursor = rows[-1].request_id if len(rows) == limit else 0
        return out, next_cursor

    # ── 保留期清理(§9.24)──────────────────────────────────────────────────

    async def sweep_terminal_join_requests(self) -> None:
        """跑一轮终态申请清理(至多一批 sweep_batch),由调用方 ticker 驱动。

        mode 默认 report_only:待清理量由 `dbguard.sweep_table` 统一 WARN 告警,
        这里只在**真删**时补一条 INFO —— 两条都打会让 report_only 档每 5 分钟
        刷一对重复日志。
        """
        try:
            out = await self._repo.sweep_terminal_join_requests(
                self._cfg.retention_mode_parsed(),
                self._cfg.request_retention_days,
                self._cfg.sweep_batch,
            )
        except Exception as exc:  # noqa: BLE001
            plog.get().warning("guild_request_sweep_failed", err=str(exc))
            return
        if out.deleted > 0:
            plog.get().info(
                "guild_request_swept",
                deleted=out.deleted,
                retention_days=self._cfg.request_retention_days,
            )

    # ── 推送(弱依赖)───────────────────────────────────────────────────────

    async def _fanout_to_managers(self, guild_id: int, evt: guild_pb2.GuildEvent) -> None:
        """把事件推给公会的会长 + 官员,**排除 evt.actor_id 本人**(原则 2)。"""
        if self._pusher is None:
            return
        try:
            members = await self._repo.list_members(guild_id, 0, 0)
        except Exception as exc:  # noqa: BLE001
            plog.get().warning("guild_fanout_managers_failed", guild_id=guild_id, err=str(exc))
            return
        for m in members:
            if m.role not in (GUILD_ROLE_LEADER, GUILD_ROLE_OFFICER):
                continue
            if m.player_id == evt.actor_id:
                continue
            await self._push(m.player_id, evt)

    async def _push(self, to_player_id: int, evt: guild_pb2.GuildEvent) -> None:
        """发一条公会事件给接收方。pusher 为 None / 发送失败**只 warn**(弱依赖)。

        ★ 每个接收方都要新建一条带 to_player_id 的事件,不能改 evt 再复用:
        proto message 是可变对象,原地改 to_player_id 会污染扇出循环里下一个人的那条。
        """
        if self._pusher is None or to_player_id == 0:
            return
        e = guild_pb2.GuildEvent(
            type=evt.type,
            guild_id=evt.guild_id,
            to_player_id=to_player_id,
            actor_id=evt.actor_id,
            guild_name=evt.guild_name,
        )
        try:
            await self._pusher.push_guild_event(to_player_id, e)
        except Exception as exc:  # noqa: BLE001
            plog.get().warning(
                "guild_push_failed", to_player_id=to_player_id, type=int(evt.type), err=str(exc)
            )

    # ── 缓存维护(写后删 / 读回填,全部弱依赖)───────────────────────────────

    def _warn_cache_read(self, event: str, **fields: object) -> None:
        admit, streak = self._cache_read_log.admit(_now_ms(), GUILD_CACHE_LOG_WINDOW_MS)
        if admit:
            plog.get().warning(event, streak=streak, **fields)

    def _warn_cache_write(self, event: str, **fields: object) -> None:
        admit, streak = self._cache_write_log.admit(_now_ms(), GUILD_CACHE_LOG_WINDOW_MS)
        if admit:
            plog.get().warning(event, streak=streak, **fields)

    def _note_cache_read_recovered(self) -> None:
        n, _ = self._cache_read_log.recovered()
        if n > 0:
            plog.get().info("guild_cache_read_recovered", failed_total=n)

    async def _invalidate_guild(self, guild_id: int) -> None:
        if self._cache is None or guild_id == 0:
            return
        try:
            await self._cache.del_guild(guild_id)
        except Exception as exc:  # noqa: BLE001
            plog.get().warning("guild_cache_del_failed", guild_id=guild_id, err=str(exc))

    async def _invalidate_member(self, player_id: int) -> None:
        if self._cache is None or player_id == 0:
            return
        try:
            await self._cache.del_member(player_id)
        except Exception as exc:  # noqa: BLE001
            self._warn_cache_write(
                "guild_member_cache_del_failed", player_id=player_id, err=str(exc)
            )

    async def _fill_guild_cache(self, g: GuildRow | None) -> None:
        if self._cache is None or g is None:
            return
        try:
            await self._cache.set_guild(g, self._cache_ttl_sec)
        except Exception as exc:  # noqa: BLE001
            # 读 miss 回填在热读路径上:Redis 宕机时 get 失败紧跟 set 也失败,
            # 同一请求叠两条 → 限流。
            self._warn_cache_write("guild_cache_set_failed", guild_id=g.guild_id, err=str(exc))
        else:
            n, _ = self._cache_write_log.recovered()
            if n > 0:
                plog.get().info("guild_cache_write_recovered", failed_total=n)

    async def _fill_member_cache(self, player_id: int, guild_id: int) -> None:
        if self._cache is None:
            return
        try:
            await self._cache.set_member_guild_id(player_id, guild_id, self._cache_ttl_sec)
        except Exception as exc:  # noqa: BLE001
            self._warn_cache_write(
                "guild_member_cache_set_failed", player_id=player_id, err=str(exc)
            )


class GroupUsecase:
    """临时群业务逻辑核心。对应 Go 的 `biz.GroupUsecase`。

    与公会的三点差异(照抄 Go,别"顺手统一"):
      - **多归属**:玩家可同时在多个群(上限 max_groups_per_player)
      - 只有 OWNER / MEMBER 两级,没有 officer;**成员也能拉人**
      - 群聊是即时频道,不落聊天历史;成员变更 MVP **不单独推送**
        (客户端拉 ListMyGroups 兜底)—— 所以这里没有 pusher
    """

    __slots__ = ("_repo", "_cfg")

    def __init__(self, repo, cfg) -> None:  # noqa: ANN001
        self._repo = repo
        self._cfg = cfg

    async def create_group(
        self, owner_id: int, name: str, member_ids: list[int], new_group_id: int
    ) -> int:
        name = name.strip()
        if not name:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "group name required")
        if len(name) > self._cfg.max_name_len:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "group name too long")
        members = dedup_exclude(list(member_ids), owner_id)
        await self._repo.create_group(
            new_group_id,
            owner_id,
            members,
            name=name,
            max_members=self._cfg.max_group_members,
            max_groups_per_player=self._cfg.max_groups_per_player,
        )
        return new_group_id

    async def invite_to_group(self, operator_id: int, group_id: int, target_id: int) -> None:
        """拉人入群。操作者须为群成员;目标已在群则幂等成功。"""
        if group_id == 0 or target_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "group_id and target_id required"
            )
        if await self._repo.get_group_member(group_id, operator_id) is None:
            raise errcode.PandoraError(
                errcode.ErrGroupNotMember, "operator %d not in group %d", operator_id, group_id
            )
        # operator_id 传下去:repo 会在事务内持群行锁**再复核一次**邀请者仍在群内。
        await self._repo.add_member(
            group_id,
            target_id,
            operator_id=operator_id,
            max_members=self._cfg.max_group_members,
            max_groups_per_player=self._cfg.max_groups_per_player,
        )

    async def leave_group(self, player_id: int, group_id: int) -> None:
        m = await self._repo.get_group_member(group_id, player_id)
        if m is None:
            raise errcode.PandoraError(
                errcode.ErrGroupNotMember, "player %d not in group %d", player_id, group_id
            )
        if m.role == group_pb2.GROUP_ROLE_OWNER:
            raise errcode.PandoraError(
                errcode.ErrGroupNotOwner, "owner must transfer or disband before leaving"
            )
        await self._repo.remove_member(group_id, player_id)

    async def kick_from_group(self, owner_id: int, group_id: int, target_id: int) -> None:
        if owner_id == target_id:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "cannot kick self")
        owner = await self._repo.get_group_member(group_id, owner_id)
        if owner is None or owner.role != group_pb2.GROUP_ROLE_OWNER:
            raise errcode.PandoraError(errcode.ErrGroupNotOwner, "only owner can kick")
        if await self._repo.get_group_member(group_id, target_id) is None:
            raise errcode.PandoraError(
                errcode.ErrGroupNotMember, "target %d not in group %d", target_id, group_id
            )
        await self._repo.kick_member(group_id, owner_id, target_id)

    async def disband_group(self, owner_id: int, group_id: int) -> None:
        owner = await self._repo.get_group_member(group_id, owner_id)
        if owner is None or owner.role != group_pb2.GROUP_ROLE_OWNER:
            raise errcode.PandoraError(errcode.ErrGroupNotOwner, "only owner can disband")
        await self._repo.disband_group(group_id, owner_id)

    async def transfer_owner(self, owner_id: int, group_id: int, target_id: int) -> None:
        if owner_id == target_id:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "cannot transfer to self")
        owner = await self._repo.get_group_member(group_id, owner_id)
        if owner is None or owner.role != group_pb2.GROUP_ROLE_OWNER:
            raise errcode.PandoraError(errcode.ErrGroupNotOwner, "only owner can transfer")
        await self._repo.transfer_owner(group_id, owner_id, target_id)

    async def get_group(self, group_id: int) -> group_pb2.Group:
        g = await self._repo.get_group(group_id)
        if g is None:
            raise errcode.PandoraError(errcode.ErrGroupNotFound, "group %d not found", group_id)
        return to_group_view(g)

    async def list_group_members(self, group_id: int) -> list[group_pb2.GroupMember]:
        rows = await self._repo.list_group_members(group_id)
        return [
            group_pb2.GroupMember(player_id=m.player_id, role=m.role, joined_ms=m.joined_ms)
            for m in rows
        ]

    async def list_my_groups(self, player_id: int) -> list[group_pb2.Group]:
        rows = await self._repo.list_my_group_rows(player_id)
        return [to_group_view(g) for g in rows]
