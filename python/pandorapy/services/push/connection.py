"""玩家长连接索引 —— 对应 Go 侧 internal/biz/connection.go。

投递模型(2026-07-22 审计 v2,拉取式投递):Redis 投递缓冲(offline.py)是**唯一**
定序与投递权威。连接写者(biz.run_subscribe_stream)按「唤醒信号 + 兜底轮询」
从缓冲 Range(>游标) 拉取投递;本模块只负责**索引 + 传信号**,不传帧本体。

★ 单写者不变量:每条 stream 只有写者协程调 `context.write`。
  `send_to` / `broadcast` 都是**非阻塞**投递(唤醒信号去重、广播箱有界满即丢),
  慢客户端最多卡住自己的写者协程,绝不阻塞 kafka handler。
  写错方向的后果:若让 kafka handler 直接 write,一个慢客户端会把整个 partition
  的消费停住 —— 全服该 topic 的推送一起卡。

★ 与 Go 的一处实现差异(语义相同):Go 用 `chan struct{}` size-1 做去重唤醒,
  Python 用 `asyncio.Event` —— Event 天然就是"置位即合并",写者醒来后 clear()。
  两者都满足"写者每次醒来都会把缓冲拉到空,信号无需计数"。
"""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

# 每连接广播箱容量。与 Go 的 broadcastQueueSize 同值。
# 广播是**丢失容忍**的(离线不补推、不入投递缓冲),满即丢并计数。
BROADCAST_QUEUE_SIZE = 64


class StreamSlot:
    """一条玩家 stream 的槽位:写句柄 + 唤醒信号 + 广播箱 + 关流开关。

    `write` 只准写者协程调用(单写者不变量)。
    """

    __slots__ = ("write", "notify", "bcast", "closed", "close_reason")

    def __init__(self, write: Callable[[object], Awaitable[None]]) -> None:
        self.write = write
        # 唤醒信号:置位即合并(对应 Go 的 size-1 chan)。
        self.notify = asyncio.Event()
        # 广播帧箱(不入投递缓冲,无游标;满即丢)。
        self.bcast: asyncio.Queue = asyncio.Queue(maxsize=BROADCAST_QUEUE_SIZE)
        # 关流开关:顶号 / 会话看门狗裁决 / 主动收流都置它。
        # 对应 Go 的 `cancel func()`(取消 Subscribe 的 subCtx)。
        self.closed = asyncio.Event()
        # 关流原因(看门狗写入的 PandoraError;None = 正常关闭)。
        self.close_reason: BaseException | None = None

    def wake(self) -> None:
        """非阻塞置唤醒信号。"""
        self.notify.set()

    def cancel(self) -> None:
        """请求关闭本流(顶号 / 看门狗)。写者在下一次等待处观察到。"""
        self.closed.set()


class ConnectionManager:
    """player_id → StreamSlot 索引。对应 Go 的 ConnectionManager。

    ⚠️ 这里**没有加锁**,而 Go 用了 sync.RWMutex。理由:asyncio 单线程事件循环下,
    本类的所有方法都不含 await,天然原子(不会在方法中途被别的协程插入)。
    加锁反而会引入"必须 await 才能读索引"的传染性。
    ★ 但这条前提很脆:**本类的任何方法都不得变成 async / 中途 await**,
      一旦有了 await 点,顶号与注册就会重新出现交错窗口。
    """

    __slots__ = ("_by_player",)

    def __init__(self) -> None:
        self._by_player: dict[int, StreamSlot] = {}

    def register(self, player_id: int, write: Callable[[object], Awaitable[None]]) -> StreamSlot:
        """建槽并入索引;已存在则关掉旧槽(顶号语义)。返回新槽给调用方持有。"""
        slot = StreamSlot(write)
        old = self._by_player.get(player_id)
        if old is not None:
            old.cancel()
        self._by_player[player_id] = slot
        return slot

    def unregister(self, player_id: int, slot: StreamSlot) -> None:
        """移除索引 —— **仅当当前槽就是传入的槽**。

        这个条件不是防御性写法:顶号场景下旧流的 finally 会在新流注册之后才跑,
        无条件 delete 会把**新流**从索引里删掉 —— 新连接从此收不到任何唤醒,
        只能等 30s 兜底轮询,而日志里一切正常。
        """
        if self._by_player.get(player_id) is slot:
            del self._by_player[player_id]

    def send_to(self, player_id: int) -> bool:
        """唤醒该玩家的写者去缓冲拉新帧(帧本体已在缓冲,这里只传信号)。

        返回是否在线,**仅作观测**:不在线不是错误 —— 缓冲已有,重连/轮询恢复。
        """
        slot = self._by_player.get(player_id)
        if slot is None:
            return False
        slot.wake()
        return True

    def broadcast(self, frame) -> tuple[int, int]:  # noqa: ANN001
        """给**本 Pod** 全部在线玩家投一帧广播。返回 (入箱数, 丢弃数),不打日志。

        广播 topic 每 Pod 独立 consumer group(见 consumer.py),全 Pod 都消费到
        同一条,所以这里只发本 Pod 的连接。
        """
        sent = 0
        failed = 0
        for slot in list(self._by_player.values()):
            try:
                slot.bcast.put_nowait(frame)
            except asyncio.QueueFull:
                failed += 1
            else:
                sent += 1
        return sent, failed

    def size(self) -> int:
        """当前在线 stream 数(给 /metrics + 调试用)。"""
        return len(self._by_player)
