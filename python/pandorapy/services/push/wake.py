"""跨 Pod 投递唤醒信号 —— 对应 Go 侧 internal/data/wake.go。

背景:投递缓冲写入(consumer)与连接写者(Subscribe)可能不在同一 Pod
(滚动重叠 / 多副本)。本 Pod 写入走进程内唤醒零等待;跨 Pod 此前只有 30s 兜底轮询
—— 消息不丢,但可能延迟近 30s,与 push p99 <200ms 验收口径不符。

本模块用 Redis pub/sub 把唤醒信号跨 Pod 广播:消费侧写完缓冲后**无条件** PUBLISH
一条 player_id;各 Pod 订阅同一 channel,收到后对本地连接管理器做一次 send_to
(本地没这个玩家 = 廉价 no-op)。

★ 契约(不能悄悄"加强"):
  - 信号是 **best-effort 加速器**。publish 失败 / 订阅断连期间丢的信号由既有 30s
    兜底轮询收敛,**交付正确性不依赖本通道**(帧本体恒在投递缓冲);
  - fire-and-forget,无 ACK、无重放;
  - channel 是集群级广播:每 Pod 都收到全量信号,按本地连接表过滤。
    信号体只有十进制 player_id(~10 字节)。
"""

from __future__ import annotations

import asyncio
import contextlib

from pandorapy import log as plog

# 跨 Pod 唤醒信号的 Redis pub/sub channel(infra.md 键位登记)。
# ★ 与 Go 侧**逐字一致**:改一个字符,两个实现就订阅到两个不同的 channel,
#   跨 Pod 唤醒静默失效 —— 表现只是"偶尔推送慢 30s",没有任何错误。
WAKE_CHANNEL = "pandora:push:wake"

# 订阅断连后的重建间隔。与 Go 的固定 1s 退避同值。
RESUBSCRIBE_BACKOFF_SEC = 1.0


class RedisWakeSignal:
    """唤醒信号发布端(与投递缓冲同一 Redis)。"""

    __slots__ = ("_rdb",)

    def __init__(self, rdb) -> None:  # noqa: ANN001
        self._rdb = rdb

    async def publish_wake(self, player_id: int) -> None:
        """广播一条玩家唤醒信号。失败由调用方记日志,**不重试** —— 兜底轮询保证最终交付。"""
        await self._rdb.publish(WAKE_CHANNEL, str(player_id).encode())


async def run_wake_subscriber(rdb, on_wake) -> None:  # noqa: ANN001
    """订阅唤醒信号并对每条调用 on_wake(player_id)。阻塞运行直到被取消。

    订阅断连(Redis 抖动 / failover)按固定 1s 退避重建 —— 断连窗口内丢失的信号
    由兜底轮询收敛,重建后**无需回放**(帧本体在缓冲里,信号只是加速器)。
    on_wake 在订阅协程上执行,必须快速且不阻塞(ConnectionManager.send_to 满足)。
    """
    logger = plog.get()
    while True:
        pubsub = rdb.pubsub()
        try:
            await pubsub.subscribe(WAKE_CHANNEL)
            logger.info("push_wake_subscriber_started", channel=WAKE_CHANNEL)
            while True:
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if msg is None:
                    continue
                payload = msg.get("data")
                text = (
                    payload.decode(errors="replace")
                    if isinstance(payload, bytes)
                    else str(payload)
                )
                try:
                    player_id = int(text)
                except ValueError:
                    player_id = 0
                if player_id <= 0:
                    logger.warning("push_wake_bad_payload", payload=text)
                    continue
                on_wake(player_id)
        except asyncio.CancelledError:
            # 停机:取消必须穿透,否则 server.run() 的排空段等不到这条循环结束。
            # (连接由下面的 finally 关掉。)
            raise
        except BaseException as exc:  # noqa: BLE001 —— 订阅断连不该让整条循环死掉
            logger.warning(
                "push_wake_subscriber_disconnected_retry",
                backoff="1s", err=str(exc),
            )
        finally:
            with contextlib.suppress(Exception):
                closer = getattr(pubsub, "aclose", None) or getattr(pubsub, "close", None)
                if closer is not None:
                    res = closer()
                    if asyncio.iscoroutine(res):
                        await res
        await asyncio.sleep(RESUBSCRIBE_BACKOFF_SEC)
