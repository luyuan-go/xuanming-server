"""成交 / 流转事件外发 —— 对应 Go 侧 cmd/auction/main.go 里的 auctionEventPusher。

  成交 → pandora.auction.match,kafka key = match_id(同一成交保序,不变量 §9.9)
  流转 → pandora.auction.audit,kafka key = order_id(同一挂单保序)

★ 两者的**依赖强度不同**,别当成一类:
    match  由 MySQL outbox 保证至少一次。broker 暂时不可用时 producer 可以延迟重建,
           但 outbox marker **绝不清除** —— 清了就是确定性丢事件。
    audit  弱依赖。队列满 / 发送失败只告警丢弃,绝不能反压撮合、market 锁或资产补偿。

★ match producer 支持**懒重建**:启动时 broker 不通不阻塞启动(只 WARN),
  第一次真的要发时再建。串行化只护"构造"这一步,不持状态锁做网络连接 ——
  否则 match broker 初始化会把健康的 audit 路径一起卡住。
"""

from __future__ import annotations

import asyncio

from pandorapy import kafkax
from pandorapy import log as plog


def build_topic(domain: str, event: str) -> str:
    """kafka topic 构造 —— 对应 Go 的 `pkg/config.BuildTopic`。

    ⚠️ 本该从 `pandorapy.kafka_topics` 取常量,但 auction 的两个 topic 在 Go 侧
    也是**运行期拼**的(cmd/auction/main.go 用 config.BuildTopic),
    `pkg/kafkax/topics.go` 里没有对应常量,所以生成出来的 kafka_topics.py 里也没有。
    这里照抄同一个拼法而不是写死字面量,是为了让"两边拼法一致"这件事有唯一落点。
    正解是 Go 侧补 TopicAuctionMatch / TopicAuctionAudit 常量后重跑生成器
    —— 见交付说明的 shared_files_needed。
    """
    return f"pandora.{domain}.{event}"


MATCH_TOPIC = build_topic("auction", "match")
AUDIT_TOPIC = build_topic("auction", "audit")


class AuctionEventPusher:
    """把成交 / 订单流转发 kafka。match 可懒重建,audit 是纯弱依赖。"""

    __slots__ = ("_conf", "_match", "_audit", "_closed", "_match_init_lock")

    def __init__(
        self,
        producer_conf: kafkax.ProducerConf,
        match_producer: kafkax.KeyOrderedProducer | None,
        audit_producer: kafkax.KeyOrderedProducer | None,
    ) -> None:
        self._conf = producer_conf
        self._match = match_producer
        self._audit = audit_producer
        self._closed = False
        self._match_init_lock = asyncio.Lock()

    async def push_match(self, event) -> None:  # noqa: ANN001 —— auctionv1.AuctionMatchEvent
        producer = await self._match_producer()
        await producer.send(str(event.match_id), event)

    async def push_audit(self, order) -> None:  # noqa: ANN001 —— auctionv1.AuctionOrder
        producer = self._audit
        if self._closed or producer is None:
            # audit 是弱依赖:没有 producer 就静默跳过(与 Go 的 `return nil` 同)。
            return
        await producer.send(str(order.order_id), order)

    async def _match_producer(self) -> kafkax.KeyOrderedProducer:
        if self._closed:
            raise RuntimeError("auction event pusher closed")
        if self._match is not None:
            return self._match
        # 只串行 producer 构造,不在持锁期间做别的事。
        async with self._match_init_lock:
            if self._closed:
                raise RuntimeError("auction event pusher closed")
            if self._match is not None:
                return self._match
            producer = await asyncio.to_thread(
                kafkax.KeyOrderedProducer, self._conf, MATCH_TOPIC
            )
            if self._closed:
                await producer.close()
                raise RuntimeError("auction event pusher closed")
            self._match = producer
            plog.get().info("kafka_producer_ready", topic=MATCH_TOPIC, mode="reconnect")
            return producer

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        match, audit = self._match, self._audit
        self._match, self._audit = None, None
        for producer in (match, audit):
            if producer is None:
                continue
            try:
                await producer.close()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 关不掉不该盖住停机流程
                plog.get().warning("kafka_producer_close_failed", err=str(exc))
