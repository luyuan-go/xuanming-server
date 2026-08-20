"""chat 业务逻辑层 —— 对应 Go 侧 internal/biz/chat.go。

五频道:世界(WORLD)/ 队伍(TEAM)/ 私聊(PRIVATE)/ 公会(GUILD)/ 临时群(GROUP)

★ 三条最容易在移植中改错的规则:

1. **推送原则 2**:队伍 / 私聊 / 公会 / 群只发**收件方**,不回发自己
   (客户端本地回显己方消息)。世界频道是广播,是原则 2 的唯一例外。
   写错方向不会报错,只会让客户端收到自己发的消息两次。

2. **成员解析失败必须诚实报错,不能假成功**:
   team / guild / group 服务不可达时返回 ErrUnavailable 让客户端重试。
   若返回 message_id 假装成功,发送者会以为已送达而**消息静默丢失**,
   而且成员身份校验被整个跳过(非成员也能在该频道说话)。
   这与"弱依赖未配置时降级"是两回事 —— 未配置是部署形态,不可达是故障。

3. **限流一律 fail-open**:限流是背压手段而非权威门,判定失败(Redis 抖动)放行。
   §9.22 的 fail-closed 只约束权威写决策,不约束背压。

其它:
  - 客户端不能发 SYSTEM / UNSPECIFIED 频道 → ErrChatChannelInvalid
  - 公会 / 群聊历史不落库(即时频道,离线不补发;用户 2026-06-27 确认)
  - sender_nickname 留空:客户端按 sender_id 解析展示名(§5.8 最小数据单位)
"""

from __future__ import annotations

import asyncio
from typing import Protocol

from pandora.chat.v1 import chat_pb2

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import snowflake

_C = chat_pb2.ChatChannel

# 客户端允许发送的频道集合。SYSTEM / UNSPECIFIED 不在内。
_CLIENT_CHANNELS = frozenset(
    {
        _C.CHAT_CHANNEL_WORLD,
        _C.CHAT_CHANNEL_TEAM,
        _C.CHAT_CHANNEL_PRIVATE,
        _C.CHAT_CHANNEL_GUILD,
        _C.CHAT_CHANNEL_GROUP,
    }
)


def now_ms() -> int:
    import time

    return int(time.time() * 1000)


class ChatPusher(Protocol):
    """把聊天推送事件发到 kafka(弱依赖,None 时静默跳过)。

    五个方法对应五个 topic;key 由适配器按收件方 player_id 设置(世界频道 key 空)。
    """

    async def push_private(self, to_player_id: int, evt) -> None: ...
    async def push_team(self, to_player_id: int, evt) -> None: ...
    async def push_world(self, evt) -> None: ...
    async def push_guild(self, to_player_id: int, evt) -> None: ...
    async def push_group(self, to_player_id: int, evt) -> None: ...


class MemberReader(Protocol):
    """解析队伍 / 公会 / 群成员名单。返回 (成员列表, 是否存在)。"""

    async def members(self, container_id: int) -> tuple[list[int], bool]: ...


class ChatUsecase:
    """chat 业务逻辑核心。对应 Go 的 biz.ChatUsecase。"""

    __slots__ = (
        "_repo",
        "_pusher",
        "_team",
        "_guild",
        "_group",
        "_cfg",
        "_router",
        "_world_limiter",
        "_channel_limiter",
    )

    def __init__(self, repo, pusher, team, guild, group, cfg) -> None:
        self._repo = repo
        self._pusher = pusher  # 弱依赖,可为 None
        self._team = team
        self._guild = guild
        self._group = group
        self._cfg = cfg
        self._router = None
        self._world_limiter = None
        self._channel_limiter = None

    def set_cell_router(self, router) -> None:
        self._router = router

    def set_world_rate_limiter(self, limiter) -> None:
        self._world_limiter = limiter

    def set_channel_rate_limiter(self, limiter) -> None:
        self._channel_limiter = limiter

    # ── 限流门(一律 fail-open)───────────────────────────────────────────────

    async def _allow_channel(self, channel: str, sender_id: int) -> None:
        """非世界频道冷却。窗内拒绝抛 ErrRateLimited(**零副作用,先于落库/推送**)。

        limiter 未注入 / 冷却 <=0 / 判定失败一律放行。
        """
        if self._channel_limiter is None:
            return
        cooldown = self._cfg.non_world_cooldown_td()
        if cooldown.total_seconds() <= 0:
            return
        try:
            allowed = await self._channel_limiter.allow_channel(channel, sender_id, cooldown)
        except Exception as exc:  # noqa: BLE001
            plog.get().warning(
                "chat_channel_ratelimit_check_failed",
                channel=channel,
                sender_id=sender_id,
                err=str(exc),
            )
            return
        if not allowed:
            raise errcode.PandoraError(
                errcode.ErrRateLimited, "%s chat cooldown, retry after %s", channel, cooldown
            )

    # ── SendMessage ──────────────────────────────────────────────────────────

    async def send_message(
        self, sender_id: int, channel: int, target_id: int, content: str, new_message_id: int
    ) -> int:
        """发一条聊天消息。sender_id 由 service 从鉴权上下文得到(R5)。"""
        if sender_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "sender required")

        if channel not in _CLIENT_CHANNELS:
            raise errcode.PandoraError(
                errcode.ErrChatChannelInvalid, "channel %d not allowed from client", channel
            )

        content = content.strip()
        if not content:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "empty content")
        # ★ 长度按 **Unicode 码点**计,不是字节 —— Go 用 utf8.RuneCountInString。
        # 用 len(bytes) 会让中文用户的可发长度只有 1/3,而且不报错、只是"发不出去"。
        rune_count = len(content)
        if rune_count > self._cfg.max_content_len:
            raise errcode.PandoraError(
                errcode.ErrChatMessageTooLong,
                "content too long: %d > %d",
                rune_count,
                self._cfg.max_content_len,
            )
        content = self._mask_sensitive(content)

        msg = chat_pb2.ChatMessage(
            message_id=new_message_id,
            sender_id=sender_id,
            channel=channel,
            target_id=target_id,
            content=content,
            send_time_ms=now_ms(),
            # sender_nickname 留空,客户端按 sender_id 解析(最小数据单位)
        )

        # 非世界频道冷却:统一在分发点、**一切副作用之前**判定,按频道独立占窗。
        if channel == _C.CHAT_CHANNEL_PRIVATE:
            await self._allow_channel("private", sender_id)
            return await self._send_private(msg)
        if channel == _C.CHAT_CHANNEL_TEAM:
            await self._allow_channel("team", sender_id)
            return await self._send_fanout(
                msg, sender_id, self._team, "team", self._push_team
            )
        if channel == _C.CHAT_CHANNEL_GUILD:
            await self._allow_channel("guild", sender_id)
            return await self._send_fanout(
                msg, sender_id, self._guild, "guild", self._push_guild
            )
        if channel == _C.CHAT_CHANNEL_GROUP:
            await self._allow_channel("group", sender_id)
            return await self._send_fanout(
                msg, sender_id, self._group, "group", self._push_group
            )
        return await self._send_world(msg)

    async def _send_private(self, msg) -> int:
        """私聊:必须有 target,落库(离线历史)+ 推送给接收方(原则 2)。"""
        if msg.target_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "private chat requires target_id")
        if msg.target_id == msg.sender_id:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "cannot private chat self")

        # 落库**强依赖**:私聊历史不可丢(MySQL 失败则整条失败,让客户端重试)。
        await self._repo.save_private(msg)

        # 推送**弱依赖**:失败只 warn(消息已落库,接收方上线 PullHistory 兜底)。
        if self._pusher is not None:
            evt = chat_pb2.ChatPushEvent(message=msg, to_player_id=msg.target_id)
            try:
                await self._pusher.push_private(msg.target_id, evt)
            except Exception as exc:  # noqa: BLE001
                plog.get().warning(
                    "chat_private_push_failed",
                    to_player_id=msg.target_id,
                    message_id=msg.message_id,
                    err=str(exc),
                )

        self._log_private_routing(msg.sender_id, msg.target_id)
        return msg.message_id

    async def _send_fanout(self, msg, sender_id: int, reader, kind: str, push) -> int:
        """队伍 / 公会 / 群的共同形状:解析成员 → 校验发送者在内 → 逐个推送(排除自己)。

        Go 侧是三个几乎相同的函数(sendTeam/sendGuild/sendGroup);这里合成一个 ——
        三者的差异只有"读哪个服务"和"推哪个 topic",没有语义差异。
        日志事件名仍按 kind 拼出与 Go 完全相同的字符串(chat_team_degraded 等),
        运维手册和 LogQL 查询不受影响。
        """
        container_id = msg.target_id
        if container_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "%s chat requires target_id", kind
            )
        if reader is None or self._pusher is None:
            # 弱依赖**未配置**:不报错,返回 message_id(客户端本地回显),记一条 warn。
            # 注意这与"服务不可达"是两回事 —— 未配置是部署形态,见下面的 except。
            plog.get().warning(
                f"chat_{kind}_degraded",
                **{f"{kind}_id": container_id},
                hint=f"{kind} reader / pusher not configured, {kind} chat fan-out skipped",
            )
            return msg.message_id

        try:
            members, found = await reader.members(container_id)
        except Exception as exc:  # noqa: BLE001
            # ★ 服务暂时不可达:**诚实报错让客户端重试**。
            # 不能假成功 —— 成员无法解析则没有任何人收到消息,返回 message_id 会让
            # 发送者以为已送达(消息静默丢失),且成员身份校验被跳过。
            plog.get().warning(
                f"chat_{kind}_resolve_failed",
                **{f"{kind}_id": container_id},
                err=str(exc),
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "%s %d members unavailable, retry later",
                kind,
                container_id,
            ) from exc
        if not found:
            raise errcode.PandoraError(
                errcode.ErrChatChannelInvalid, "%s %d not found", kind, container_id
            )

        # 发送者必须是成员才能在该频道说话。
        if sender_id not in members:
            raise errcode.PandoraError(
                errcode.ErrChatChannelInvalid,
                "sender %d not in %s %d",
                sender_id,
                kind,
                container_id,
            )

        # 失败**批末汇总一条**而不是逐成员打 —— kafka 一挂,单条群消息会刷 50 行
        # (群成员上限 50,§9.18)。但这条又是"接收方漏推"的唯一信号(消息已发、
        # RPC 返回成功、access log 记 rpc_ok),不能删。
        failed = 0
        first_err: str | None = None
        sample_to = 0
        for member in members:
            if member == sender_id:
                continue  # ★ 原则 2:不回发自己
            evt = chat_pb2.ChatPushEvent(message=msg, to_player_id=member)
            try:
                await push(member, evt)
            except Exception as exc:  # noqa: BLE001
                failed += 1
                if first_err is None:
                    first_err, sample_to = str(exc), member
        if failed > 0:
            plog.get().warning(
                f"chat_{kind}_push_failed",
                **{f"{kind}_id": container_id},
                members=len(members),
                failed=failed,
                sample_to_player_id=sample_to,
                first_err=first_err,
            )
        return msg.message_id

    async def _push_team(self, to_player_id: int, evt) -> None:
        await self._pusher.push_team(to_player_id, evt)

    async def _push_guild(self, to_player_id: int, evt) -> None:
        await self._pusher.push_guild(to_player_id, evt)

    async def _push_group(self, to_player_id: int, evt) -> None:
        await self._pusher.push_group(to_player_id, evt)

    async def _send_world(self, msg) -> int:
        """世界频道:广播(to_player_id=0,push 服务 Broadcast,原则 2 例外)。

        冷却**前置**:广播成本 ≈ 发送速率 × 全服在线数,必须在生产侧压掉刷屏;
        冷却期内直接 ErrRateLimited,**不产生任何 kafka 写**。
        """
        if self._world_limiter is not None:
            cooldown = self._cfg.world_cooldown_td()
            if cooldown.total_seconds() > 0:
                try:
                    allowed = await self._world_limiter.allow_world(msg.sender_id, cooldown)
                except Exception as exc:  # noqa: BLE001
                    plog.get().warning(
                        "chat_world_ratelimit_check_failed",
                        sender_id=msg.sender_id,
                        err=str(exc),
                    )
                else:
                    if not allowed:
                        raise errcode.PandoraError(
                            errcode.ErrRateLimited,
                            "world chat cooldown, retry after %s",
                            cooldown,
                        )
        if self._pusher is None:
            plog.get().warning("chat_world_degraded", hint="pusher not configured")
            return msg.message_id
        evt = chat_pb2.ChatPushEvent(message=msg, to_player_id=0)
        try:
            await self._pusher.push_world(evt)
        except Exception as exc:  # noqa: BLE001
            plog.get().warning(
                "chat_world_push_failed", message_id=msg.message_id, err=str(exc)
            )
        return msg.message_id

    # ── PullHistory ──────────────────────────────────────────────────────────

    async def pull_history(
        self, player_id: int, channel: int, peer_id: int, limit: int, before_ms: int
    ) -> list:
        """拉私聊历史。只有 PRIVATE 有持久化历史;其余频道返回空。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if channel != _C.CHAT_CHANNEL_PRIVATE:
            return []  # 即时频道不持久化,无历史可拉
        if peer_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "peer_id required for private history"
            )
        if limit <= 0 or limit > self._cfg.history_limit:
            limit = self._cfg.history_limit
        return await self._repo.list_private(player_id, peer_id, limit, before_ms)

    # ── 辅助 ─────────────────────────────────────────────────────────────────

    def _mask_sensitive(self, content: str) -> str:
        """把命中的敏感词整词替换为**等长** `*`。

        等长很重要:长度变了会让"内容长度校验"和实际展示对不上。
        列表为空直接返回原文;这只是最小化屏蔽,真正风控由独立服务接管。
        """
        if not self._cfg.sensitive_words:
            return content
        out = content
        for word in self._cfg.sensitive_words:
            if not word:
                continue
            out = out.replace(word, "*" * len(word))
        return out

    def _log_private_routing(self, sender_id: int, target_id: int) -> None:
        """观测私聊的跨 region 投递落点。router 为 None(单 Cell)→ 不打。"""
        if self._router is None:
            return
        try:
            sender = self._router.route(sender_id)
            target = self._router.route(target_id)
        except Exception:  # noqa: BLE001
            plog.get().debug("chat_private_route_failed", sender_id=sender_id)
            return
        plog.get().debug(
            "chat_private_placement",
            sender_region=sender.region_id,
            target_region=target.region_id,
            cross_region=sender.region_id != target.region_id,
        )

    # ── 私聊历史保留期清理(§9.24)──────────────────────────────────────────

    async def sweep_history(self, now_ms: int) -> None:
        """跑一轮私聊历史清理(至多一批 sweep_batch)—— 对应 Go 的 SweepHistory。

        不设这道清理的后果:chat_private_messages 每条私聊一行、只增不删,
        随消息量无界线性增长,最终把 pandora_social 撑爆(§9 不变量 24)。

        ★ 按雪花 message_id 的**时间段单调性**把"发送时间早于 cutoff"翻译成
        "message_id < min_id_at(cutoff)" —— 时间条件变成**主键范围**条件,
        无需给 send_time_ms 另建索引,也不会因为索引缺失而全表扫。

        ★ 默认 report_only(只统计不删):自动删生产数据不可逆,由人确认后显式开启。
        多副本各自跑、无锁 —— DELETE 幂等,并发只多花空批。
        """
        cutoff_sec = (now_ms - self._cfg.history_retention_days * 86_400_000) // 1000
        max_id = snowflake.min_id_at(cutoff_sec)
        if max_id == 0:
            # cutoff 早于雪花 Epoch(服务上线未满保留期),无可清理。
            # ★ 这个提前返回不能省:min_id_at 回落 0 时 `message_id < 0` 恒不命中,
            # 行为上无害,但每轮都会白跑一条 COUNT/DELETE。
            return
        try:
            out = await self._repo.sweep_messages_before(
                self._cfg.retention_mode_parsed(), max_id, self._cfg.sweep_batch
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
            # 吞掉之后取消就**不再传播**,停机时这条循环不会真的停,
            # §9.16 的「先摘流量 → 再排空在途」失效。
            raise
        except BaseException as exc:  # noqa: BLE001 —— 单轮失败只丢本轮,下轮继续
            plog.get().warning("chat_history_sweep_failed", err=str(exc))
            return
        if out is not None and out.deleted > 0:
            plog.get().info(
                "chat_history_swept",
                deleted=out.deleted,
                retention_days=self._cfg.history_retention_days,
            )
