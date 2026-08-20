"""chat 数据层 —— 对应 Go 侧 internal/data(chat_repo.go + 限流器)。

库表(deploy/mysql-init/06-social-tables.sql,pandora_social 库):

    chat_private_messages  私聊消息(message_id PK = snowflake)

**只有私聊(PRIVATE)落库**支持离线 PullHistory;世界 / 队伍 / 公会 / 群是即时频道,
不持久化(用户 2026-06-27 确认:公会/群离线消息不补发)。
表是结构化列直接映射(§5.9 关系型表不强制 proto bytes blob)。

限流器为什么必须跨副本一致:
    chat 可水平扩展,单进程内存限流会被多副本摊薄 —— 3 个副本就等于把冷却放宽 3 倍。
    用 Redis SET NX PX 原子占窗。
"""

from __future__ import annotations

import datetime as _dt
from typing import Protocol

from pandora.chat.v1 import chat_pb2
from redis.asyncio.client import Redis

from pandorapy import dbguard, errcode, redisx

DEFAULT_DB = "pandora_social"


class PrivateRepo(Protocol):
    """私聊历史的数据层抽象。biz 只依赖此协议,不依赖数据库。"""

    async def save_private(self, msg) -> None: ...
    async def list_private(
        self, player_id: int, peer_id: int, limit: int, before_ms: int
    ) -> list: ...
    async def sweep_messages_before(
        self, mode: dbguard.Mode, max_message_id: int, limit: int
    ) -> dbguard.Outcome: ...


class MySQLPrivateRepo:
    """基于 asyncmy / aiomysql 的 PrivateRepo 实现。

    SQL 与 Go 侧**逐字一致**(除了占位符 ? → %s,这是驱动差异不是语义差异)。
    列顺序也保持一致,避免 scan 时错位。
    """

    __slots__ = ("_pool", "_db")

    def __init__(self, pool, db: str = DEFAULT_DB) -> None:  # noqa: ANN001
        self._pool = pool
        self._db = db

    async def save_private(self, msg) -> None:
        """落一条私聊消息。message_id 由 biz 用 snowflake 预生成。"""
        sql = (
            "INSERT INTO chat_private_messages "
            "(message_id, sender_id, receiver_id, content, send_time_ms) "
            "VALUES (%s, %s, %s, %s, %s)"
        )
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(
                    sql,
                    (msg.message_id, msg.sender_id, msg.target_id, msg.content, msg.send_time_ms),
                )
                await conn.commit()
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "save private msg %d: %s", msg.message_id, exc
            ) from exc

    async def list_private(
        self, player_id: int, peer_id: int, limit: int, before_ms: int
    ) -> list:
        """拉两人之间的私聊历史,按发送时间倒序。

        before_ms > 0 时只取 send_time_ms < before_ms 的(翻页游标);=0 取最新。
        `%s = 0 OR send_time_ms < %s` 这个写法照抄 Go 侧 —— 一条 SQL 覆盖两种情况,
        不做条件拼接(拼 SQL 是注入面)。
        """
        sql = (
            "SELECT message_id, sender_id, receiver_id, content, send_time_ms "
            "FROM chat_private_messages "
            "WHERE ((sender_id = %s AND receiver_id = %s) OR (sender_id = %s AND receiver_id = %s)) "
            "  AND (%s = 0 OR send_time_ms < %s) "
            "ORDER BY send_time_ms DESC "
            "LIMIT %s"
        )
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(
                    sql, (player_id, peer_id, peer_id, player_id, before_ms, before_ms, limit)
                )
                rows = await cur.fetchall()
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "list private %d-%d: %s", player_id, peer_id, exc
            ) from exc

        out = []
        for message_id, sender_id, receiver_id, content, send_time_ms in rows:
            out.append(
                chat_pb2.ChatMessage(
                    message_id=message_id,
                    sender_id=sender_id,
                    channel=chat_pb2.ChatChannel.CHAT_CHANNEL_PRIVATE,
                    target_id=receiver_id,
                    content=content,
                    send_time_ms=send_time_ms,
                )
            )
        return out

    async def sweep_messages_before(
        self, mode: dbguard.Mode, max_message_id: int, limit: int
    ) -> dbguard.Outcome:
        """保留期清理(§9.24)。**默认 report_only:只统计不删**。

        雪花 message_id 时间段单调 → 按主键范围操作,无需时间列索引;多副本并发安全。
        """
        async with self._pool.acquire() as conn:
            try:
                return await dbguard.sweep_table(
                    conn,
                    mode,
                    self._db,
                    "chat_private_messages",
                    "message_id < %s",
                    limit,
                    max_message_id,
                )
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "sweep private history: %s", exc
                ) from exc


def _world_cooldown_key(player_id: int) -> str:
    """世界频道冷却 key,与 Go 的 data.worldCooldownKey 逐字一致。

    **刻意不走 pandora:rl:* 规范**:这是先于该规范存在的历史 key,Go 侧保留原样以避免
    滚动升级窗口内新旧进程双键并行。迁移期 Go 副本与 Python 副本同时在线,key 只要有一
    个字符不同,同一玩家就在两侧各占一个窗 —— 实际冷却被放宽一倍,而两边日志都显示
    "限流生效中";按 pandora:chat:world:cd:* 建的巡检也会对 Python 副本整体失明。
    """
    return f"pandora:chat:world:cd:{player_id}"


class RedisRateLimiter:
    """世界频道 + 非世界频道的跨副本冷却。对应 Go 的 data.RedisWorldRateLimiter。

    同一个结构体同时提供两个接口(与 Go 侧一致):
      - allow_world(player_id, cooldown)
      - allow_channel(channel, player_id, cooldown)

    SET NX PX 原子占窗:第一个抢到 key 的放行,窗口内其余全拒。
    判定失败(Redis 抖动)由**调用方** fail-open —— 限流是背压手段而非权威门,
    依赖故障时牺牲限流保聊天可用(§9.22 的 fail-closed 只约束权威写决策)。
    """

    __slots__ = ("_rdb",)

    def __init__(self, rdb: Redis) -> None:
        self._rdb = rdb

    async def allow_world(self, player_id: int, cooldown: _dt.timedelta) -> bool:
        return await self._occupy(_world_cooldown_key(player_id), cooldown)

    async def allow_channel(
        self, channel: str, player_id: int, cooldown: _dt.timedelta
    ) -> bool:
        # 按频道独立占窗:队聊不占私聊的窗(与 Go 侧一致)。
        # key 走 redisx.rl_key 而不是手拼字面量 —— Go 侧同样调 redisx.RLKey,
        # 共用同一个构造器才能保证规范以后调整时两栈一起动,不会只改一边。
        return await self._occupy(redisx.rl_key("chat", channel, player_id), cooldown)

    async def _occupy(self, key: str, cooldown: _dt.timedelta) -> bool:
        ms = int(cooldown.total_seconds() * 1000)
        if ms <= 0:
            return True
        # nx=True 保证只有第一个请求能建 key;px 让它自动过期 = 冷却窗结束。
        acquired = await self._rdb.set(key, b"1", nx=True, px=ms)
        return bool(acquired)
