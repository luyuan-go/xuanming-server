"""push kafka 消费者 —— 对应 Go 侧 internal/biz/consumer.go。

一个 topic 一个消费者,定向 topic 共享 `kafka.group_id`。

★ 两个**判错了两边都不报错**的分叉,逐条对齐 Go:

  ① 广播类 topic(`kafka_topics.is_broadcast_topic`)必须走 `broadcast()`,
     不能按 player_id key 路由。广播消息的 kafka key 是**空的** —— 拿空 key 去
     解析 player_id 必然失败,消息被当 invalid key 投 DLQ / 丢弃,
     **全服公告静默不达**,而生产侧一切正常。

  ② 广播 topic 必须 ① 每 Pod 独立 group(共享 group 只有一个 Pod 消费到,
     其他 Pod 上的在线玩家收不到 —— 广播不入缓冲,补不回来);
     ② `initial_offset=latest`(fresh group 不回放历史公告刷屏);
     ③ **关闭 offset 提交**。第 ③ 条最容易漏:group 名按 hostname 派生,
     Pod 同名重启(StatefulSet / 主机名复用)时若存在 committed offset,
     客户端会**忽略 latest 从旧位点续读**,把停机窗口积压的广播整段重放给
     全部在线连接。纯实时消费者不留位点,每次启动恒从 latest 开始,
     停机窗口内的广播**有意丢弃**(这是显式契约,不是缺陷)。

★ 交付路径(审计 v2):
     ① 单 Lua 原子「分配游标 + 入投递缓冲」(Redis 单点定序,跨 Pod / 跨 topic
        并发安全,无进程锁);失败**拒 ack** → kafkax 按 RetryPolicy 重试 → DLQ;
     ② 唤醒本 Pod 连接写者 + **无条件**发跨 Pod 唤醒信号。
   kafka 重投会给同一业务事件分配新游标 → 可能重复投递,
   at-least-once 诚实契约(push.proto),业务侧幂等 / 按业务 ID 判重。
"""

from __future__ import annotations

import asyncio
import os
import re
import time

from prometheus_client import Counter

from pandora.push.v1 import push_pb2

from pandorapy import errcode, kafka_topics, kafkax
from pandorapy import cellroute
from pandorapy import log as plog
from pandorapy import logwindow

# 依赖降级日志的最小重打间隔(毫秒)。与 Go 的 degradeLogWindowMs 同值。
DEGRADE_LOG_WINDOW_MS = 5000

# kafka 消费失败处理:业务瞬时错误进程内重试 N 次(间隔 backoff)后投 DLQ
# (infra.md §4.4,与 Go 的 dlqMaxRetries / dlqRetryBackoff 同值)。
DLQ_MAX_RETRIES = 3
DLQ_RETRY_BACKOFF_SEC = 0.5

# 与 Go 的 kafkax.HeaderEventType 同名(生成侧写这个 header,消费侧按它选 message)。
HEADER_EVENT_TYPE = "event_type"

# kafka key 的合法形状:全 ASCII 十进制数字(对齐 Go 的 strconv.ParseUint)。
_ASCII_DIGITS = re.compile(r"[0-9]+")

# 对应 Go 的 biz.OfflineAppendFailed。
# label 只有 topic(≤13,低基数);player_id 永远不能进 label(§12)。
# 告警阈值:rate(...[5m]) > 0 即告警 —— 正常应恒为 0,>0 意味着这一帧
# 没有任何持久化,客户端按 last_seen_ms 重连也补不回来。
OFFLINE_APPEND_FAILED = Counter(
    "pandora_push_offline_append_failed_total",
    "push 服务把帧写入 redis 投递缓冲失败的总次数(应恒为 0,> 0 即需要告警)",
    ["topic"],
)


def parse_event_type_header(headers) -> int:  # noqa: ANN001
    """解析 event_type header。缺失 / 空值 → 0;存在但非法 → 抛 ValueError。

    ★ 非法值**不得降级为 legacy 0**(Go R5 复审 P2-3):把新事件按旧 message 路由,
    客户端会用错误的 proto 解析 payload —— 字段可能凑巧对上,表现为误弹提示 /
    污染缓存,而不是一个干脆的解析失败。producer 写坏 header 是 bug,
    毒丸进 DLQ 暴露并留证。
    """
    raw = _header_str(headers, HEADER_EVENT_TYPE)
    if not raw:
        return 0
    # 与 kafka key 同理:Go 用 ParseUint(s,10,32),Python 的 int() 宽得多。
    # 这里按 ASCII 十进制 + uint32 范围逐条对齐,不让"两边判定相反"的缝存在。
    if not _ASCII_DIGITS.fullmatch(raw):
        raise ValueError(f"malformed {HEADER_EVENT_TYPE} header {raw!r}")
    value = int(raw)
    if value > 0xFFFFFFFF:
        raise ValueError(f"{HEADER_EVENT_TYPE} header out of uint32 range: {raw!r}")
    return value


def _header_str(headers, key: str) -> str:  # noqa: ANN001
    for entry in headers or ():
        name = entry[0]
        if (name.decode() if isinstance(name, bytes) else str(name)) != key:
            continue
        value = entry[1]
        if value is None:
            return ""
        return value.decode(errors="replace") if isinstance(value, bytes) else str(value)
    return ""


def broadcast_group_id(base_group_id: str) -> str:
    """广播 topic 的 per-Pod group 名。与 Go 的 `groupID + "-bcast-" + hostname` 同。

    hostname 取不到时用纳秒兜底 —— 与 Go 的 `anon-%d` 分支同,宁可每次重启换个
    group 名(纯实时消费本就不留位点),也不能让两个 Pod 撞同一个 group。
    """
    host = ""
    try:
        host = os.uname().nodename  # type: ignore[attr-defined]
    except AttributeError:
        host = os.environ.get("COMPUTERNAME", "") or ""
    if not host:
        try:
            import socket

            host = socket.gethostname()
        except OSError:
            host = ""
    if not host:
        host = f"anon-{time.time_ns()}"
    return f"{base_group_id}-bcast-{host}"


class PushKafkaConsumer:
    """包装一个 topic 的消费循环。对应 Go 的 biz.KafkaConsumer。"""

    __slots__ = (
        "topic", "broadcast", "_conns", "_offline", "_consumer", "_wake",
        "_wake_fail_log", "_buffer_fail_log", "_bcast_dropped_log",
        "_router", "_self_region", "_self_cell", "_route_unknown_log",
    )

    def __init__(
        self,
        *,
        brokers: list[str],
        group_id: str,
        topic: str,
        conns,  # noqa: ANN001  ConnectionManager
        offline,  # noqa: ANN001  RedisOfflineCache
        dlq=None,  # noqa: ANN001  kafkax.KeyOrderedProducer
        consumer_factory=None,  # noqa: ANN001  测试注入
    ) -> None:
        if conns is None:
            raise ValueError("conns (ConnectionManager) must not be None")
        if offline is None:
            raise ValueError("offline (RedisOfflineCache) must not be None")
        if not topic:
            raise ValueError("topic must not be empty")

        self.topic = topic
        self.broadcast = kafka_topics.is_broadcast_topic(topic)
        self._conns = conns
        self._offline = offline
        self._wake = None
        # 依赖降级日志限流(模式 C)。handle 是**每条消息**执行一次的热路径:
        # Redis / pub-sub 一挂就按消息速率刷屏(delivery 那条还要乘重试次数)。
        # 每实例(= 每 topic)各一份,无 key ⇒ 内存天然有界。
        self._wake_fail_log = logwindow.Window()
        self._buffer_fail_log = logwindow.Window()
        self._bcast_dropped_log = logwindow.Window()
        # cell 归属:router 为 None = 单 Cell,本实例拥有全部玩家(历史行为)。
        # 分片部署时由 main 经 `set_cell_ownership` 注入 —— 用 setter 而非构造参数,
        # 避免单 Cell 阶段所有调用点被迫改签名(与 Go 侧 SetCellOwnership 同形)。
        self._router = None
        self._self_region = 0
        self._self_cell = 0
        self._route_unknown_log = logwindow.Window()

        initial_offset = "earliest"  # 定向 topic:组内断点续传
        if self.broadcast:
            group_id = broadcast_group_id(group_id)
            initial_offset = "latest"

        self._consumer = kafkax.KeyOrderedConsumer(
            kafkax.ConsumerConf(
                brokers=tuple(brokers),
                topic=topic,
                group_id=group_id,
                retry=kafkax.RetryPolicy(
                    max_retries=DLQ_MAX_RETRIES, backoff_sec=DLQ_RETRY_BACKOFF_SEC
                ),
                initial_offset=initial_offset,
                # ★ 见模块头注释②③:广播 per-Pod group 必须关 offset 提交。
                disable_offset_commit=self.broadcast,
            ),
            self.handle,
            dlq=dlq,
            consumer_factory=consumer_factory,
        )

    def set_wake_publisher(self, wake) -> None:  # noqa: ANN001
        """注入跨 Pod 唤醒信号发布端(main 装配;None-safe)。"""
        self._wake = wake

    def set_cell_ownership(
        self, router, self_region: int, self_cell: int
    ) -> None:  # noqa: ANN001  cellroute.Router | None
        """注入确定性 region/cell 路由器 + 本实例所在 cell 身份。

        对应 Go 的 `KafkaConsumer.SetCellOwnership`。None-safe:不调用 / router 传 None
        时(单 Cell / dev),消费者拥有全部玩家,handle 不做归属判定。
        """
        self._router = router
        self._self_region = self_region
        self._self_cell = self_cell

    def _owns_player(self, player_id: int) -> tuple[tuple[int, int], bool, bool]:
        """一名玩家是否归本 push 实例所在 cell 所有。

        返回 `((owner_region, owner_cell), owned, known)`:
          - router 为 None / player_id 为 0 / 路由失败 → `((0,0), True, False)`:
            视为本实例拥有(不阻断交付),known=False 表示归属未知 / 不适用。
          - 否则 known=True,owned = (玩家 owner region/cell == 本实例 region/cell)。

        ★ 三值而非两值是关键:"不归我"与"不知道归谁"处置**相反**
          (前者毒丸留证,后者 fail-open + 告警)。合成一个布尔会让路由表拖动时
          全量消息被毒丸进 DLQ —— 把"表没铺好"升级成"推送全断"。
        """
        if self._router is None or player_id == 0:
            return (0, 0), True, False
        try:
            loc = self._router.route(player_id)
        except cellroute.CellRouteError:
            return (0, 0), True, False
        owned = loc.region_id == self._self_region and loc.cell_id == self._self_cell
        return (loc.region_id, loc.cell_id), owned, True

    async def run(self) -> None:
        await self._consumer.run()

    def stop(self) -> None:
        self._consumer.stop()

    # ── 单条消息处理 ────────────────────────────────────────────────────
    async def handle(self, msg) -> None:  # noqa: ANN001, C901 —— 与 Go 同为线性分支
        """处理一条 kafka 消息。

        返回约定(kafkax.KeyOrderedConsumer 的三档):
          - 正常返回          → ack
          - raise kafkax.poison(...) → 毒丸,跳过重试直投 DLQ 留证
          - raise 其它异常     → 可重试,耗尽后投 DLQ
        """
        logger = plog.get()

        # ── 广播类 topic:key 为空,给本 Pod 全部在线玩家投递 ──────────────
        if self.broadcast:
            try:
                event_type = parse_event_type_header(msg.headers)
            except ValueError as exc:
                logger.warning(
                    "kafka_push_invalid_event_type",
                    topic=msg.topic, partition=msg.partition, offset=msg.offset, err=str(exc),
                )
                raise kafkax.poison(exc) from exc
            # ts_ms 置 0:客户端用所有帧的最大 ts_ms 推进恢复游标,广播若携带 kafka
            # 时间戳会**永久越过**较小的玩家专属游标,导致定向帧被补推跳过。
            # 广播不参与游标体系;客户端 max(cursor, 0) 恒 no-op。
            frame = push_pb2.PushFrame(
                topic=msg.topic,
                payload=msg.value or b"",
                ts_ms=0,
                trace_id=_header_str(msg.headers, "trace_id"),
                event_type=event_type,
            )
            sent, failed = self._conns.broadcast(frame)
            if failed > 0:
                total = self._bcast_dropped_log.add_extra(failed)
                ok, msgs = self._bcast_dropped_log.admit(
                    int(time.time() * 1000), DEGRADE_LOG_WINDOW_MS
                )
                if ok:
                    logger.warning(
                        "push_broadcast_partial_dropped",
                        topic=msg.topic, sent=sent, dropped=failed,
                        dropped_total=total, msgs_with_drop=msgs,
                    )
            else:
                n, dropped = self._bcast_dropped_log.recovered()
                if n > 0:
                    logger.info(
                        "push_broadcast_drop_recovered",
                        topic=msg.topic, msgs_with_drop=n, dropped_total=dropped,
                    )
            return

        # ── 1. 取 player_id(不变量 §9:key 必须是 player_id 序列化字符串)──
        key = msg.key
        key_text = key.decode(errors="replace") if isinstance(key, bytes) else ("" if key is None else str(key))
        # ★ 判据是「全 ASCII 十进制数字」而不是 `int(key_text)` 能不能过。
        # Python 的 int() 比 Go 的 strconv.ParseUint **宽**得多:接受前后空白、
        # 下划线分隔("1_0" → 10)、Unicode 数字("١٢")、正负号。宽在这里不是宽容
        # 而是分叉:Go 副本判它毒丸留证,Python 副本却把它当成某个**别的** player_id
        # 的消息写进那个人的缓冲 —— 一条消息投给了错的玩家,两边都不报错。
        if not _ASCII_DIGITS.fullmatch(key_text):
            logger.warning(
                "kafka_push_invalid_key",
                topic=msg.topic, partition=msg.partition, offset=msg.offset,
                key=key_text, err="kafka key is not a decimal player_id",
            )
            raise kafkax.poison(f"invalid kafka key {key_text!r} (topic={msg.topic})")
        player_id = int(key_text)
        # player_id=0 不是合法业务 ID(Snowflake 恒非 0,§9.11):key="0" 能过解析,
        # 不拦的话会写进 player 0 的缓冲并 ACK = **静默吞掉一条定向消息**。
        # producer 用零值 key 是 bug,毒丸留证暴露它。
        if player_id == 0:
            logger.warning(
                "kafka_push_zero_player_key",
                topic=msg.topic, partition=msg.partition, offset=msg.offset,
            )
            raise kafkax.poison(f"kafka key resolves to player_id=0 (topic={msg.topic})")

        # ── 2. cell 归属:非本 cell 玩家的消息**毒丸投 DLQ,不本地处理** ────
        #
        # 为什么不是"告警 + 照常交付"(2026-07-22 Go 侧审计已改过一轮):本 cell 的
        # Redis 投递缓冲对**连接所在的那个 cell** 不可见。照常交付 = 写错缓存 + ACK
        # = 静默丢一条定向消息。毒丸留证才能由基础设施 / 人工重投到 owner cell。
        #
        # ⚠️ 诚实标注(与 Go 同):这里**不是**跨 cell 消息通道,没有自动转投。
        # 业务生产者按 player_id 路由到正确 cell 的 kafka 集群是**部署面契约**;
        # 本判定只是错配的兜底暴露(DLQ 告警 = 生产者路由或 cell 表配置有 bug)。
        # 单 Cell 部署(当前唯一形态)router 为 None,不判定,行为与历史一致。
        owner, owned, known = self._owns_player(player_id)
        if known and not owned:
            logger.error(
                "push_player_not_owned_poisoned",
                player_id=player_id, topic=msg.topic,
                self_region=self._self_region, self_cell=self._self_cell,
                owner_region=owner[0], owner_cell=owner[1],
            )
            raise kafkax.poison(
                f"player {player_id} owned by region={owner[0]} cell={owner[1]}, "
                f"not self region={self._self_region} cell={self._self_cell}"
            )
        if not known and self._router is not None:
            # known=False 在此**只可能**是 route 失败(router 已注入且 player_id 非 0,
            # 零值在上一道闸就毒丸了)。归属 UNKNOWN 却 fail-open 落到本 cell 的投递
            # 缓冲:若本实例不是真 owner,连接不在本 cell = 静默误投并最终丢失。
            # 与相邻的 not_owned 毒丸分支(有日志 + DLQ 留证)形成盲区,故显式 WARN。
            # 行为未改(仍 fail-open 不阻断交付);改 fail-closed 需按 §9.22 单独拍板。
            ok, streak = self._route_unknown_log.admit(
                int(time.time() * 1000), DEGRADE_LOG_WINDOW_MS
            )
            if ok:
                logger.warning(
                    "push_player_owner_unknown_fail_open",
                    player_id=player_id, topic=msg.topic,
                    partition=msg.partition, offset=msg.offset,
                    self_region=self._self_region, self_cell=self._self_cell,
                    streak=streak,
                    hint="cellroute 路由表抖动/缺失 → 归属未知仍投本 cell,多 Cell 下可能误投",
                )

        # ── 3. 构 PushFrame ────────────────────────────────────────────────
        try:
            event_type = parse_event_type_header(msg.headers)
        except ValueError as exc:
            logger.warning(
                "kafka_push_invalid_event_type",
                topic=msg.topic, partition=msg.partition, offset=msg.offset,
                player_id=player_id, err=str(exc),
            )
            raise kafkax.poison(exc) from exc
        # ts_ms 初值为 kafka 消息时间,assign_and_buffer 会把它重铸为该玩家的投递
        # 游标(原始事件时间由业务 payload 自带)。
        frame = push_pb2.PushFrame(
            topic=msg.topic,
            payload=msg.value or b"",
            ts_ms=_msg_timestamp_ms(msg),
            trace_id=_header_str(msg.headers, "trace_id"),
            event_type=event_type,
        )

        # ── 4. 交付 ────────────────────────────────────────────────────────
        try:
            cursor = await self._offline.assign_and_buffer(
                player_id, frame, int(time.time() * 1000)
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 指标逐次 Inc(告警走指标不走日志行数);日志按窗口限流:本条会被
            # kafkax 按 RetryPolicy 重试,每次重试都再走一遍 handle,
            # Redis 一挂就是「消息速率 ×(1+MaxRetries)」行 Error。
            OFFLINE_APPEND_FAILED.labels(msg.topic).inc()
            ok, streak = self._buffer_fail_log.admit(
                int(time.time() * 1000), DEGRADE_LOG_WINDOW_MS
            )
            if ok:
                logger.error(
                    "push_delivery_buffer_failed",
                    topic=msg.topic, player_id=player_id,
                    code=errcode.ErrPushOfflineCorrupted, streak=streak, err=str(exc),
                )
            raise errcode.PandoraError(
                errcode.ErrPushOfflineCorrupted, "delivery buffer failed: %s", exc
            ) from exc
        n, _ = self._buffer_fail_log.recovered()
        if n > 0:
            logger.info("push_delivery_buffer_recovered", topic=msg.topic, failed_total=n)

        # 关键低频事件的正向交付台账:match.progress 每玩家每局个位数条,
        # 「READY 到底送没送到这个玩家」必须能按 player_id 正查,
        # 不能只靠「无 ERROR」反证。其余 topic 维持成功零日志。
        if msg.topic == kafka_topics.TOPIC_MATCH_PROGRESS:
            logger.info(
                "push_match_progress_buffered",
                player_id=player_id, event_type=event_type, cursor=cursor,
                event_trace_id=frame.trace_id,
            )

        # ── 5. 唤醒:先本地快路径,再**无条件**发跨 Pod 信号 ────────────────
        # 不能以「本地有 slot」抑制跨 Pod 信号 —— 本地 slot 可能是半死连接 /
        # 已被顶号的陈旧残留(send_to 只看索引,不验证连接活性),
        # 真持有新连接的 Pod 会被迫等 30s 兜底轮询。
        self._conns.send_to(player_id)
        if self._wake is not None:
            try:
                await self._wake.publish_wake(player_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— best-effort,失败不影响 ack
                ok, streak = self._wake_fail_log.admit(
                    int(time.time() * 1000), DEGRADE_LOG_WINDOW_MS
                )
                if ok:
                    logger.warning(
                        "push_wake_publish_failed",
                        topic=msg.topic, player_id=player_id, streak=streak, err=str(exc),
                    )
            else:
                n, _ = self._wake_fail_log.recovered()
                if n > 0:
                    logger.info(
                        "push_wake_publish_recovered", topic=msg.topic, failed_total=n
                    )


def _msg_timestamp_ms(msg) -> int:  # noqa: ANN001
    """kafka 消息时间(毫秒)。kafka-python 的 `timestamp` 已经是毫秒。"""
    ts = getattr(msg, "timestamp", None)
    return int(ts) if ts else 0
