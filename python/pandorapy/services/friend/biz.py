"""friend 业务逻辑层 —— 对应 Go 侧 internal/biz/{friend,recommend,sweep}.go。

职责(docs/design/go-services.md §2.4):
  - 好友请求 / 接受 / 拒绝 / 列表 / 删好友 / 拉黑 / 推荐
  - 好友图落 pandora_social(MySQL **强依赖**,repo.MySQLFriendRepo)
  - 好友请求 / 接受经 kafka pandora.friend.event → push 推给接收方(**弱依赖**)
  - ListFriends / RecommendFriends 经 player_locator 填在线状态(**弱依赖**,查不到按离线)

关键规则(与 Go 逐条对应):
  - 不能加自己;任一方向拉黑则不能加好友(ERR_FRIEND_BLOCKED)
  - 已是好友再加 → ERR_FRIEND_ALREADY_ADDED
  - 只有请求的 target 本人能接受 / 拒绝(R5:player_id 来自 JWT ctx)
  - **推送原则 2**:好友请求通知发给 target,接受通知发给 requester,都不发给操作者自己
  - 拒绝**不推送**给 requester(避免"被拒绝"的尴尬,业界惯例)

★ 预检与权威的分工:本层的 is_blocked / are_friends / count_friends 全是
  **fail-fast 预检,不是权威**——权威判定在 repo 的守卫锁事务内重做一遍。
  删掉预检不会错(只是多开一次事务),把预检**当成**权威才会错。

★ 分片观测(Go 的 SetCellRouter / logFriendshipSharding)已于 2026-08-20 补齐:
  `cell_route` 装配层落地后,`mode: static` / `etcd` 会真正建出 Router 并经
  `set_cell_router` 注入;`mode` 为空(单 Cell)时 router 恒为 None,整条不执行,
  行为与迁移前逐字节相同。纯逻辑在 `sharding.py`。
"""

from __future__ import annotations

import asyncio
import time
from typing import Protocol

from pandora.friend.v1 import friend_pb2

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy.protoenum import enum_name
from pandorapy.services import player_display
from pandorapy.services.friend import conf as fconf
from pandorapy.services.friend import repo as frepo
from pandorapy.services.friend import sharding as fshard


def now_ms() -> int:
    return int(time.time() * 1000)


class FriendEventPusher(Protocol):
    """把好友事件发到 kafka(main 注入 kafkax 适配器;弱依赖,None 时静默跳过)。"""

    async def push_friend_event(self, to_player_id: int, evt) -> None:  # noqa: ANN001
        ...


class OnlineStatusReader(Protocol):
    """查好友在线状态(弱依赖,None 时全部按离线)。"""

    async def batch_online(self, player_ids: list[int]) -> dict:
        ...


class ActionRateQuota(Protocol):
    """申请类写入的 per-player 频率配额(实现见 pandorapy.redisx.ActionQuota)。

    ★ 返回 `(是否放行, 故障)`,与 Go 的 `Allow(...) (bool, error)` 同形 ——
    **不是**裸 bool。这一行不是装饰:Protocol 在运行期不做任何检查,所以照着它
    写一个 `-> bool` 的实现,测试全绿、类型检查全绿,而 `_allow_action` 里的
    `ok, quota_exc = await ...` 会在**每一次**申请上抛
    `TypeError: cannot unpack non-iterable bool object` —— 服务照常起、
    RPC 全返 ErrInternal。
    """

    async def allow(self, action: str, subject: int) -> tuple[bool, Exception | None]:
        ...


class FriendUsecase:
    """friend 业务逻辑核心。对应 Go 的 biz.FriendUsecase。"""

    __slots__ = (
        "_repo",
        "_pusher",
        "_online",
        "_cfg",
        "_quota",
        "_strategies",
        "_router",
        "_player_name_resolver",
        "_player_no_resolver",
    )

    def __init__(
        self,
        repo: frepo.MySQLFriendRepo,
        pusher: FriendEventPusher | None,
        online: OnlineStatusReader | None,
        cfg: fconf.FriendConf,
    ) -> None:
        self._repo = repo
        self._pusher = pusher  # 弱依赖,可为 None
        self._online = online  # 弱依赖,可为 None
        self._cfg = cfg
        self._quota: ActionRateQuota | None = None
        self._player_name_resolver: player_display.PlayerNameResolver | None = None
        self._player_no_resolver: player_display.PlayerNoResolver | None = None
        # 分片部署时由 main 经 set_cell_router 注入;单 Cell 时恒 None(不打观测日志)。
        self._router = None
        # 策略链:未知名忽略;全被忽略(或名单为空)→ 回落 [mutual, random]。
        # 与 Go 的 buildStrategies 逐条一致。
        known = {
            "mutual": self._repo.recommend_by_mutual,
            "random": self._repo.recommend_random,
        }
        picked = [known[n] for n in cfg.strategies() if n in known]
        self._strategies = picked or [known["mutual"], known["random"]]

    def set_rate_quota(self, quota: ActionRateQuota | None) -> None:
        """注入频率配额(可选;不注入 = 不限,dev 无 Redis 联调兼容)。"""
        self._quota = quota

    def set_cell_router(self, router) -> None:
        """注入确定性 region/cell 路由器(对应 Go 的 `SetCellRouter`)。

        只用于分片落点**观测**:不注入 = 单 Cell,建边路径一字不改。
        """
        self._router = router

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

    # ── 频率配额门 ───────────────────────────────────────────────────────────

    async def _allow_action(self, action: str, player_id: int) -> None:
        """窗内超额抛 ErrRateLimited(**先于一切副作用**);判定失败 fail-open。

        限流是背压门不是权威门:Redis 抖动时放行,总比把玩家挡在门外好(§9.20)。
        """
        if self._quota is None:
            return
        # ★ 读 `(ok, exc)` 而不是等异常：`allow()` 从不抛，原先那段
        #   `except ... : log("friend_rate_quota_check_failed")` 是**不可达死代码**，
        #   于是这个 Loki 键在代码里存在、永远不会触发。
        ok, quota_exc = await self._quota.allow(action, player_id)
        if quota_exc is not None:
            # 背压门判定失败一律放行（§9.20：限流不得卡玩家）。
            plog.get().warning(
                "friend_rate_quota_check_failed",
                action=action,
                player_id=player_id,
                err=str(quota_exc),
                fail_open=True,
            )
            return
        if not ok:
            plog.get().warning(
                "friend_rate_quota_rejected", action=action, player_id=player_id
            )
            raise errcode.PandoraError(
                errcode.ErrRateLimited, "friend %s rate limited, retry later", action
            )

    # ── 写路径 ───────────────────────────────────────────────────────────────

    async def add_friend(
        self, requester_id: int, target_id: int, new_request_id: int
    ) -> int:
        """发起好友请求,返回 request_id。

        new_request_id 是 service 用 snowflake 预生成的 ID(复用既有 pending 时被丢弃)。
        """
        if requester_id == 0 or target_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "requester / target required")
        if requester_id == target_id:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "cannot add self as friend")

        # 频率配额按发起方计,**先于一切读写** —— 放在后面就等于"刷不进去但照样打库"。
        await self._allow_action("request", requester_id)

        # 以下三条都是 fail-fast 预检(非权威),权威复核在 create_request 的守卫锁内。
        if await self._repo.is_blocked(requester_id, target_id):
            raise errcode.PandoraError(
                errcode.ErrFriendBlocked, "blocked between %d and %d", requester_id, target_id
            )
        if await self._repo.are_friends(requester_id, target_id):
            raise errcode.PandoraError(
                errcode.ErrFriendAlreadyAdded, "already friends: %d-%d", requester_id, target_id
            )
        if self._cfg.max_friends > 0:
            count = await self._repo.count_friends(requester_id)
            if count >= self._cfg.max_friends:
                raise errcode.PandoraError(
                    errcode.ErrFriendLimit,
                    "friend limit reached: %d (max %d)",
                    requester_id,
                    self._cfg.max_friends,
                )

        request_id, _created = await self._repo.create_request(
            new_request_id, requester_id, target_id, self._cfg.max_incoming_requests
        )

        # 推送原则 2:好友请求通知发给**接收方** target。
        await self._push_event(
            target_id,
            friend_pb2.FriendEvent(
                by_player_id=requester_id,
                to_player_id=target_id,
                request_id=request_id,
                reason=friend_pb2.FRIEND_EVENT_REASON_REQUEST_RECEIVED,
                ts_ms=now_ms(),
            ),
        )
        return request_id

    async def accept_friend(self, player_id: int, request_id: int) -> None:
        """接受好友请求。player 必须是请求的 target 本人(R5)。"""
        if player_id == 0 or request_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player / request_id required")

        row = await self._repo.get_request(request_id)
        # 预检(fail-fast,非权威):不存在 / 不是发给本人 / 已非 pending → 直接报找不到,
        # 免开事务。★ 一律 NOT_FOUND 而不是 UNAUTHORIZED:后者等于告诉调用方
        # "这条申请确实存在",可用来探测他人社交关系。
        if row is None or row[2] != player_id or row[3] != frepo.REQUEST_STATUS_PENDING:
            raise errcode.PandoraError(
                errcode.ErrFriendNotFound, "no acceptable request: %d", request_id
            )

        try:
            requester_id, _target_id = await self._repo.accept_request(
                request_id, player_id, self._cfg.max_friends
            )
        except errcode.PandoraError as exc:
            if exc.code == errcode.ErrFriendNotFound:
                # CAS/TOCTOU 失败方:预检通过、到取守卫锁之间行被并发改写
                # (Block 置 rejected / 另一次 accept / 重新申请轮换了 request_id)。
                # 对客户端仍是 NOT_FOUND,但留一条 DEBUG 证据区分"真不存在"与
                # "并发竞争丢了工作" —— 没有它,这两种形状在日志里一模一样。
                plog.get().debug(
                    "friend_accept_lost", request_id=request_id, player_id=player_id
                )
            raise

        # 分片落点观测(router 未注入时整条不执行)。位置与 Go 一致:
        # 建边事务**已提交**之后、推送之前。
        fshard.log_friendship_sharding(self._router, request_id, requester_id, player_id)

        # 推送原则 2:接受通知发给**发起方** requester。
        await self._push_event(
            requester_id,
            friend_pb2.FriendEvent(
                by_player_id=player_id,
                to_player_id=requester_id,
                request_id=request_id,
                reason=friend_pb2.FRIEND_EVENT_REASON_REQUEST_ACCEPTED,
                ts_ms=now_ms(),
            ),
        )

    async def reject_friend(self, player_id: int, request_id: int) -> None:
        """拒绝好友请求。player 必须是 target 本人;**不推送**给 requester。

        pending→rejected 后该 requester 仍可再次发起(会换新 request_id)。
        """
        if player_id == 0 or request_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player / request_id required")

        row = await self._repo.get_request(request_id)
        if row is None or row[2] != player_id or row[3] != frepo.REQUEST_STATUS_PENDING:
            raise errcode.PandoraError(
                errcode.ErrFriendNotFound, "no rejectable request: %d", request_id
            )
        await self._repo.reject_request(request_id, player_id)

    async def remove_friend(self, player_id: int, target_id: int) -> None:
        """删好友(双向边,幂等)。不动黑名单。"""
        if player_id == 0 or target_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player / target required")
        if player_id == target_id:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "cannot remove self")
        await self._repo.remove_friend(player_id, target_id)

    async def block(self, player_id: int, target_id: int) -> None:
        """拉黑(同时删好友关系 + 作废两人之间的 pending 申请,见 repo.block)。"""
        if player_id == 0 or target_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player / target required")
        if player_id == target_id:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "cannot block self")
        await self._repo.block(player_id, target_id, self._cfg.max_blocks)

    async def unblock(self, player_id: int, target_id: int) -> None:
        """取消拉黑(幂等)。**不自动恢复好友关系**,玩家需重新加好友。"""
        if player_id == 0 or target_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player / target required")
        if player_id == target_id:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "cannot unblock self")
        await self._repo.unblock(player_id, target_id)

    # ── 读路径 ───────────────────────────────────────────────────────────────

    async def list_friend_requests(self, player_id: int) -> list:
        """列"发给本人且仍 pending"的申请。离线玩家错过 kafka push 后靠它补拉。

        friend 不持有昵称/编号真源；读取时分别向 player/login 有界批量投影。
        任一权威失败只留空对应字段，客户端按 昵称 → player_no → player_id 回退。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        rows = await self._repo.list_incoming_requests(player_id)
        names, numbers = await player_display.resolve_player_display(
            [row[1] for row in rows],
            self._player_name_resolver,
            self._player_no_resolver,
            service="friend",
        )
        return [
            friend_pb2.FriendRequestInfo(
                request_id=row[0],
                from_player_id=row[1],
                from_nickname=names.get(row[1], ""),
                created_ms=row[2],
                from_player_no=numbers.get(row[1], 0),
            )
            for row in rows
        ]

    async def list_friends(self, player_id: int) -> list:
        """列好友。nickname 留空(同上);is_online / last_seen_ms 经 locator 填,查不到按离线。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        rows = await self._repo.list_friends(player_id)
        online = await self._batch_online([r[0] for r in rows])
        out = []
        for friend_id, since_ms in rows:
            info = friend_pb2.FriendInfo(player_id=friend_id, since_ms=since_ms)
            status = online.get(friend_id)
            if status is not None:
                info.is_online = status.online
                info.last_seen_ms = status.last_seen_ms
            out.append(info)
        return out

    async def list_blocks(self, player_id: int) -> list:
        """列本人拉黑的人。nickname 留空(§5.8)。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        rows = await self._repo.list_blocks(player_id)
        return [
            friend_pb2.BlockInfo(player_id=blocked_id, since_ms=since_ms)
            for blocked_id, since_ms in rows
        ]

    async def recommend_friends(self, player_id: int, limit: int, exclude) -> list:  # noqa: ANN001
        """推荐好友:按策略链依次召回,凑够 limit 即止。

        服务端无状态:刷新靠客户端把已展示的 id 放进 exclude。
        ★ exclude 永远带上自己(否则会把自己推荐给自己),每选中一批就追加,
          避免后续策略重复推同一个人。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if limit <= 0:
            limit = self._cfg.recommend_limit
        if limit <= 0:
            limit = fconf.RECOMMEND_MAX_LIMIT
        if limit > fconf.RECOMMEND_MAX_LIMIT:
            limit = fconf.RECOMMEND_MAX_LIMIT

        ex = [int(x) for x in (exclude or [])]
        ex.append(player_id)

        rows: list[tuple[int, int]] = []
        for candidates in self._strategies:
            if len(rows) >= limit:
                break
            picked = await candidates(player_id, ex, limit - len(rows))
            for candidate_id, mutual in picked:
                rows.append((candidate_id, mutual))
                ex.append(candidate_id)

        online = await self._batch_online([r[0] for r in rows])
        out = []
        for candidate_id, mutual in rows:
            info = friend_pb2.RecommendedFriendInfo(
                player_id=candidate_id, mutual_friend_count=mutual
            )
            status = online.get(candidate_id)
            if status is not None:
                info.is_online = status.online
                info.last_seen_ms = status.last_seen_ms
            out.append(info)
        return out

    async def _batch_online(self, ids: list[int]) -> dict:
        """弱依赖在线态查询:未注入 reader / 空列表 → 空 dict(全部按离线)。"""
        if self._online is None or not ids:
            return {}
        return await self._online.batch_online(ids)

    # ── 保留期清理(§9.24)────────────────────────────────────────────────────

    async def sweep_terminal_requests(self) -> None:
        """跑一轮保留期清理(各至多一批 sweep_batch),由 main 的 ticker 驱动。

        两件事各自独立兜错 —— 一件失败不该让另一件本轮也不做:
          - 终态好友请求(accepted/rejected/expired,超 request_retention_days);
          - 关系对守卫行(friend_pair_guards,超 pair_guard_retention_days:
            pair 守卫随社交图 O(n²) 累积无上界,§9.24 不豁免)。
        mode 默认 report_only:待清理量由 dbguard.sweep_table 统一 WARN,
        这里只在**真删发生**时补一条业务 INFO(与 Go 的 out.Cleaned() 同)。
        """
        logger = plog.get()
        try:
            out = await self._repo.sweep_terminal_requests_before(
                self._cfg.retention_mode_parsed(),
                self._cfg.request_retention_days,
                self._cfg.sweep_batch,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 —— 单轮失败只丢本轮,下轮继续
            logger.warning("friend_request_sweep_failed", err=str(exc))
        else:
            if out.deleted > 0:
                logger.info(
                    "friend_request_swept",
                    deleted=out.deleted,
                    retention_days=self._cfg.request_retention_days,
                )

        try:
            deleted = await self._repo.delete_pair_guards_before(
                self._cfg.pair_guard_retention_days, self._cfg.sweep_batch
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("friend_pair_guard_sweep_failed", err=str(exc))
        else:
            if deleted > 0:
                logger.info(
                    "friend_pair_guard_swept",
                    deleted=deleted,
                    retention_days=self._cfg.pair_guard_retention_days,
                )

    # ── 推送(弱依赖)─────────────────────────────────────────────────────────

    async def _push_event(self, to_player_id: int, evt) -> None:  # noqa: ANN001
        """pusher 为 None 或发送失败只 warn,**不影响主流程成功**。

        好友关系已经落库了 —— 因为一条通知发不出去就把整个操作报失败,会让玩家
        重复点"添加",而每次都真的成功。离线补拉走 ListFriendRequests。
        """
        if self._pusher is None:
            return
        try:
            await self._pusher.push_friend_event(to_player_id, evt)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            plog.get().warning(
                "friend_event_push_failed",
                to_player_id=to_player_id,
                reason=enum_name(friend_pb2.FriendEventReason, evt.reason),
                err=str(exc),
            )
