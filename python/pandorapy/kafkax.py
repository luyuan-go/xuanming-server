"""Kafka 封装 —— 对应 Go 侧 pkg/kafkax(consistent.go / producer.go / consumer.go)。

一致性哈希分区器必须与 Go **逐位一致**,这是本模块最硬的约束:
    不变量 §9.9 要求 "kafka topic key = 业务实体 ID",目的是让同一玩家 / 同一对局的
    事件有序。有序性由"同一 key 恒落同一 partition"保证 —— Kafka 只在单 partition 内
    保证顺序。

    迁移期 Go 版和 Python 版会同时生产同一批 topic。如果两边的 key→partition 映射
    不同,同一个玩家的事件会被劈到两个 partition:
      - 消费侧看到的事件顺序错乱(后发生的先到)
      - 而且**不报错** —— 只是偶发的状态错乱,极难定位
    所以这里逐行照抄 Go 的算法,并有交叉验证测试(tests/test_kafkax_parity.py)
    直接对比两边的完整路由表。

Go 侧算法(pkg/kafkax/consistent.go):
    replicaCount = 20(每 partition 20 个虚拟节点)
    replicaKey   = 8 字节大端 = partition(int32 4 字节) ++ replicaIdx(低 32 位 4 字节)
    hash         = FNV-1a 32 位
    路由         = 环上第一个 >= keyHash 的节点,越界回绕到 0

客户端选型:kafka-python(2026-08-18 改判,原先选的是 confluent-kafka)
    实测 kafka-python 3.x 两个月内发了 6 个版本,修的是 rebalance stuck partition、
    leader epoch fencing 这类协议层硬骨头;且纯 Python 无 C 扩展 —— 开发机是
    Windows + 360 主动防御,二进制 wheel 正是它最爱拦的东西(已有 namecheck 的前例)。
"""

from __future__ import annotations

import bisect
import threading

# 与 Go 侧 NewConsistent 的默认值一致。改这个数会让整张路由表重排,
# 等于把所有 key 重新分配 partition —— 迁移期两边必须同值。
DEFAULT_REPLICA_COUNT = 20

# FNV-1a 32 位参数(Go 的 hash/fnv.New32a)。
_FNV_32A_OFFSET = 0x811C9DC5
_FNV_32A_PRIME = 0x01000193
_UINT32_MASK = 0xFFFFFFFF


def fnv1a_32(data: bytes) -> int:
    """FNV-1a 32 位 —— 与 Go 的 hash/fnv.New32a() 逐字节等价。

    刻意不用 Python 内置 hash():那个带随机化种子,进程间都不一致,更不用说跨语言。
    """
    h = _FNV_32A_OFFSET
    for byte in data:
        h ^= byte
        h = (h * _FNV_32A_PRIME) & _UINT32_MASK
    return h


def _gen_replica_key(partition: int, replica_idx: int) -> bytes:
    """生成虚拟节点的 hash 输入 —— 对应 Go 的 genReplicaKey。

    Go 那边是手工位移拼 8 个字节(大端),partition 是 int32、replicaIdx 是 int 但
    只取低 32 位。这里用 to_bytes 得到同样的字节序列。
    signed=True 是为了在 partition 为负时也与 Go 的 byte(int32) 截断行为一致
    (实际 partition 非负,但对齐算法不留"只在正常输入下相同"的缝)。
    """
    return (partition & _UINT32_MASK).to_bytes(4, "big") + (
        replica_idx & _UINT32_MASK
    ).to_bytes(4, "big")


class Consistent:
    """key → partition 的稳定路由表(一致性哈希)。对应 Go 的 kafkax.Consistent。

    读多写少,用 RLock 保护(Go 侧是 sync.RWMutex)。
    """

    __slots__ = ("_ring", "_sorted_hashes", "_replica_count", "_partitions", "_lock")

    def __init__(self, replica_count: int = DEFAULT_REPLICA_COUNT) -> None:
        self._replica_count = replica_count if replica_count > 0 else DEFAULT_REPLICA_COUNT
        self._ring: dict[int, int] = {}
        self._sorted_hashes: list[int] = []
        self._partitions: set[int] = set()
        self._lock = threading.RLock()

    def add_partition(self, partition: int) -> None:
        """加入一个 partition。重复添加是 no-op(与 Go 一致)。"""
        with self._lock:
            if partition in self._partitions:
                return
            for idx in range(self._replica_count):
                hash_val = fnv1a_32(_gen_replica_key(partition, idx))
                # 注意:Go 那边 ring[hashVal] = partition 会**覆盖**已有条目(哈希碰撞时
                # 后添加的赢),且 sortedHashes 会留下重复值。这里如实照抄该行为,
                # 不"优化"成去重 —— 去重会让碰撞情况下的路由结果与 Go 不同。
                self._ring[hash_val] = partition
                self._sorted_hashes.append(hash_val)
            self._partitions.add(partition)
            self._sorted_hashes.sort()

    def get_partition(self, key: str) -> tuple[int, bool]:
        """把 key 路由到 partition。环为空返回 (0, False),与 Go 的 (0, false) 一致。"""
        with self._lock:
            if not self._ring:
                return 0, False
            key_hash = fnv1a_32(key.encode())
            # bisect_left 等价于 Go 的 sort.Search(需要第一个 >= keyHash 的位置)
            idx = bisect.bisect_left(self._sorted_hashes, key_hash)
            if idx == len(self._sorted_hashes):
                idx = 0  # 回绕,与 Go 一致
            return self._ring[self._sorted_hashes[idx]], True

    def partition_count(self) -> int:
        with self._lock:
            return len(self._partitions)

    def partitions(self) -> list[int]:
        with self._lock:
            return sorted(self._partitions)


def partitioner_for(consistent: Consistent):
    """把 Consistent 适配成 kafka-python 的 partitioner 回调。

    kafka-python 的签名是 partitioner(key_bytes, all_partitions, available_partitions)。
    我们无视它给的 partition 列表而用自己的环 —— 因为环必须与 Go 侧完全一致,
    不能受"当前哪些 partition 可用"影响(可用性变化会让同一 key 漂移到别的 partition,
    破坏 §9.9 的有序性保证)。

    ⚠️ 反过来说:partition 列表变化时必须显式重建环并让 Go / Python 两边同时生效,
    这与 Go 侧的约束相同,不是 Python 引入的新问题。
    """

    def _partition(key_bytes, all_partitions, available_partitions):  # noqa: ANN001
        if key_bytes is None:
            # 无 key = 不要求有序,交回 kafka-python 的默认轮询。
            return None
        key = key_bytes.decode() if isinstance(key_bytes, bytes) else str(key_bytes)
        partition, ok = consistent.get_partition(key)
        if not ok:
            return None
        return partition

    return _partition


# ─────────────────────────── 生产者 ───────────────────────────────────────────
#
# 对应 Go 侧 pkg/kafkax/producer.go 的 KeyOrderedProducer。
#
# **与 Go 的一个结构性差异,必须显式处理**:Go 用 SyncProducer,阻塞的是一条 goroutine
# (廉价);Python 这边所有服务都跑在**单个事件循环**上,直接阻塞会把整个进程停住 ——
# 一次 broker 抖动就能让本进程的全部 RPC 一起超时。所以下面每个发送都过
# `asyncio.to_thread`,把阻塞留在线程池里。这不是"优化",是不这么做就会把
# 一个局部故障放大成进程级故障。

import asyncio  # noqa: E402
import dataclasses  # noqa: E402

from pandorapy import log as _plog  # noqa: E402
from pandorapy import metrics as _pmetrics  # noqa: E402

# push 域内事件类型判别键的 kafka header 名。对应 Go 的 kafkax.HeaderEventType。
# 缺省(老 producer 不填)→ consumer 读到 0 → 客户端按该 topic 的旧事件解析(向后兼容),
# 所以 event_type=0 时**不写** header,不能改成"总是写 0"。
HEADER_EVENT_TYPE = "event_type"

# Go 侧 NewKeyOrderedProducer 的默认值(producer.go:73/128)。改这两个要两边同改。
_DEFAULT_PARTITION_CNT = 4
_DEFAULT_RETRY_MAX = 3


def player_key(player_id: int) -> str:
    """玩家维度的 kafka key。对应 Go 的 strconv.FormatUint(pid, 10)。

    ★ 这是 §9.9「同一玩家事件有序」的**唯一**载体:key 决定 partition,partition 内才有序。
    格式变一个字符(补零 / 加前缀 / 十六进制)就会算出别的 partition ——
    同一玩家的事件被打散到多个 partition 后**乱序**,而两侧都不报错。
    任务域已经证过这条链的后果:后环事实提前到达 = 静默永久丢失。
    """
    return str(player_id)


@dataclasses.dataclass(frozen=True)
class ProducerConf:
    """对应 Go 的 config.KafkaConfig 里 producer 用到的那部分。"""

    brokers: tuple[str, ...] = ()
    partition_cnt: int = 0        # <=0 → 4
    retry_max: int = 0            # <=0 → 3
    retry_backoff_ms: int = 0
    compression_type: str = ""    # none|gzip|snappy|lz4|zstd
    # ★ Go 的真实默认是 **False**(结构体零值)。pkg/config/config.go:239 的注释写着
    # "默认 true",但全仓没有任何代码给它填值,producer.go:85 直穿。照注释写 True
    # 会让没显式配 idempotent 的 yaml 上两栈的**重复消息语义**不同,而两边都不报错。
    idempotent: bool = False
    dial_timeout_ms: int = 0
    read_timeout_ms: int = 0
    # Go 的 buildProducerConfig 会用它(producer.go:68-70);ProducerConf 此前**根本没有
    # 这个字段**,于是 yaml 里配了 write_timeout 在 Python 侧连"被忽略"都算不上。
    write_timeout_ms: int = 0



def producer_conf_from(kafka_conf) -> ProducerConf:  # noqa: ANN001
    """把服务 conf 里的 kafka 段翻译成 `ProducerConf` —— **唯一映射点**。

    `kafka_conf` 只需具备 Go `config.KafkaConfig` 的同名字段
    （brokers / partition_cnt / retry_max / retry_backoff / channel_buffer /
    compression_type / idempotent / dial_timeout / read_timeout / write_timeout），
    duration 字段用字符串（"5s"）即可，这里统一过 `config.parse_duration`。

    ★ 为什么必须收敛成一个函数：这段映射原先在每个服务的 `main.py` 里手抄一遍
    （13 份），而且**统一漏掉同样的字段** —— `retry_backoff` 建了模、
    `ProducerConf` 有字段、`kafkax` 也会用，就是没人传；`write_timeout` 更彻底，
    ProducerConf 里根本没有这个字段。手抄 13 遍的必然结果就是 13 份各漏各的，
    而漏掉的后果全是"配了不生效且不报错"。

    新服务一律走这里；发现 Go 又加了字段，改这一个函数。
    """
    from pandorapy.config import parse_duration

    def _ms(raw) -> int:  # noqa: ANN001
        return int(parse_duration(getattr(kafka_conf, raw, "") or "").total_seconds() * 1000)

    return ProducerConf(
        brokers=tuple(getattr(kafka_conf, "brokers", ()) or ()),
        partition_cnt=int(getattr(kafka_conf, "partition_cnt", 0) or 0),
        retry_max=int(getattr(kafka_conf, "retry_max", 0) or 0),
        retry_backoff_ms=_ms("retry_backoff"),
        compression_type=str(getattr(kafka_conf, "compression_type", "") or ""),
        idempotent=bool(getattr(kafka_conf, "idempotent", False)),
        dial_timeout_ms=_ms("dial_timeout"),
        read_timeout_ms=_ms("read_timeout"),
        write_timeout_ms=_ms("write_timeout"),
    )

class KeyOrderedProducer:
    """按 key 一致性哈希路由的生产者。对应 Go 的 KeyOrderedProducer。

    与 Go 逐条对齐的地方(每条都是"改了不报错"的):
      - **显式指定 partition**,而不是把 key 交给客户端库自己算 —— 两个库的默认
        哈希不同(kafka-python 是 murmur2,Go 侧走我们自己的一致性环),交出去就分叉。
      - `acks=all` + `max_in_flight_requests_per_connection=1`(Go 的
        RequiredAcks=WaitForAll + Net.MaxOpenRequests=1):后者是**保序前提**,
        >1 时重试会让同 partition 内的消息换序。
      - 批量扇出中单个目标失败**不中断整批**,末尾汇总一条 WARN。
        逐条打的话,hub 500 人广播时 broker 一抖就是 500 行。
    """

    __slots__ = ("_topic", "_conf", "_consistent", "_producer", "_closed", "_sent", "_failed")

    def __init__(self, conf: ProducerConf, topic: str, *, producer_factory=None) -> None:
        self._topic = topic
        self._conf = conf
        self._closed = False
        self._sent = 0
        self._failed = 0

        cnt = conf.partition_cnt if conf.partition_cnt > 0 else _DEFAULT_PARTITION_CNT
        self._consistent = Consistent()
        for i in range(cnt):
            self._consistent.add_partition(i)

        if producer_factory is not None:      # 测试注入
            self._producer = producer_factory()
            return

        from kafka import KafkaProducer  # 延迟导入:没配 kafka 的服务不该被它拖住启动

        kw = {
            "bootstrap_servers": list(conf.brokers),
            "acks": "all",
            "retries": conf.retry_max if conf.retry_max > 0 else _DEFAULT_RETRY_MAX,
            # ★ 保序前提,见类注释。
            "max_in_flight_requests_per_connection": 1,
            "enable_idempotence": conf.idempotent,
        }
        if conf.compression_type and conf.compression_type != "none":
            kw["compression_type"] = conf.compression_type
        if conf.retry_backoff_ms > 0:
            kw["retry_backoff_ms"] = conf.retry_backoff_ms
        if conf.dial_timeout_ms > 0:
            # Go 的 Net.DialTimeout 是**建连**上界,对应 kafka-python 的 bootstrap_timeout_ms。
            # 原先映射到 request_timeout_ms 是错位的:那管的是单次请求,不是建连;
            # 而且会被下面的读写超时覆写 —— 于是 dial_timeout 配了等于没配,
            # 正是本函数 docstring 反复强调要消灭的"配了不生效且不报错"。
            kw["bootstrap_timeout_ms"] = conf.dial_timeout_ms
        # Go 的 buildProducerConfig 把这两个分别设进 Net.ReadTimeout / Net.WriteTimeout
        # (producer.go:65-70)。kafka-python **没有** socket 级读写超时:3.0.11 的
        # KafkaProducer.DEFAULT_CONFIG 里根本没有 socket_timeout_ms,传进去 KafkaProducer
        # 直接抛 "Unrecognized configs" —— 2026-08-22 实测 team / matchmaker /
        # matchmaker_pve / push / player_locator 五个服务因此**启动即退**(producer 是
        # 强依赖),hub_allocator / ds_allocator 则退化成静默丢消息。
        # 语义最接近的是 request_timeout_ms(单次请求的等待上界),取两者中**更大**的
        # 那个 —— 取小的会让写大批次时莫名超时,而 Go 侧同一份配置不会。
        sock = max(conf.read_timeout_ms, conf.write_timeout_ms)
        if sock > 0:
            kw["request_timeout_ms"] = sock
        self._producer = KafkaProducer(**kw)

    # ── 单条发送 ──────────────────────────────────────────────────────────
    async def send(self, key: str, msg) -> None:
        """序列化 proto 后按 key 发送。对应 Go 的 Send。"""
        await self.send_raw(key, msg.SerializeToString())

    async def send_raw(self, key: str, payload: bytes, event_type: int = 0) -> None:
        """发原始字节。event_type != 0 时额外写 `event_type` header。

        对应 Go 的 SendRaw / SendRawWithEventType —— 合成一个函数是因为 Go 那两个
        除了 header 之外逐行相同,分成两个只会多一处漂移点。
        """
        if self._closed:
            raise RuntimeError("producer closed")
        partition, ok = self._consistent.get_partition(key)
        if not ok:
            raise RuntimeError("no partition (hash ring empty)")

        headers = [(HEADER_EVENT_TYPE, str(event_type).encode())] if event_type else None
        try:
            await asyncio.to_thread(self._send_blocking, key, payload, partition, headers)
        except Exception:
            self._record_send(False)
            raise
        self._record_send(True)

    async def send_raw_with_headers(self, key: str, payload: bytes, headers) -> None:
        """发原始字节并**原样携带一组 header**。对应 Go 的 SendRawWithHeaders。

        ★ 这个方法此前**不存在**,而 `KeyOrderedConsumer._to_dlq` 一直在调它
        (kafkax.py:572)。后果不是"DLQ 少个功能":`_to_dlq` 里的
        `except BaseException` 会把 AttributeError 当成"DLQ 投递失败" ——
        于是走到 `kafka_dlq_send_failed_will_not_ack` 分支 **不 ack**,
        该 partition 从此永远卡在这条毒丸上,消费组看起来活着、lag 一直涨。
        换句话说:**任何配了 DLQ 的消费者,一旦遇到第一条毒丸就永久停摆**。
        push 的 13 个 topic 全部配 DLQ,是第一个真正会踩到它的服务。

        headers 是 [(name, bytes)] 列表(消费侧 `_to_dlq` 已经拼好原 header +
        三个 dlq-src-* 溯源 header),这里不再追加 event_type ——
        原 header 里若有 event_type,重复追加会让回放时解出两个值。
        """
        if self._closed:
            raise RuntimeError("producer closed")
        partition, ok = self._consistent.get_partition(key)
        if not ok:
            raise RuntimeError("no partition (hash ring empty)")
        try:
            await asyncio.to_thread(
                self._send_blocking, key, payload, partition, list(headers or [])
            )
        except Exception:
            self._record_send(False)
            raise
        self._record_send(True)

    def _send_blocking(self, key: str, payload: bytes, partition: int, headers) -> None:
        fut = self._producer.send(
            self._topic,
            value=payload,
            key=key.encode(),
            partition=partition,
            headers=headers,
        )
        fut.get(timeout=30)   # 与 Go 的 SyncProducer 语义一致:确认后才算发出去

    # ── 批量扇出 ──────────────────────────────────────────────────────────
    async def push_to_players(
        self,
        caller_player_id: int,
        to_player_ids,
        payload: bytes,
        event_type: int = 0,
    ) -> tuple[int, BaseException | None]:
        """按玩家扇出,返回 (成功数, 最后一个错误)。对应 Go 的 PushToPlayers*。

        ★ **`caller_player_id == 0` 表示不排除任何人**(推送原则 3 的例外,
        如 `pandora.match.progress`:stage 异步变化必须发给所有人含发起方)。
        写成"总是排除 caller"或"caller 为 0 时排除 player_id 0"都会让某一类推送
        少发一个人,而客户端只表现为"偶尔没收到"。

        部分失败不抛异常:一个坏目标不该阻断同批其他玩家(与 Go 同)。
        """
        sent = 0
        failed = 0
        first_err: BaseException | None = None
        last_err: BaseException | None = None
        sample_pid = 0
        targets = 0
        for pid in to_player_ids:
            targets += 1
            if pid == caller_player_id:
                continue
            try:
                await self.send_raw(player_key(pid), payload, event_type)
            except BaseException as exc:  # noqa: BLE001 —— 单个目标失败要继续
                failed += 1
                last_err = exc
                if first_err is None:
                    first_err, sample_pid = exc, pid
                continue
            sent += 1
        self._log_push_batch_failure(targets, sent, failed, sample_pid, event_type, first_err)
        return sent, last_err

    def _log_push_batch_failure(
        self, targets: int, sent: int, failed: int, sample_pid: int,
        event_type: int, first_err: BaseException | None,
    ) -> None:
        """批尾汇总一条。failed==0 不打;event_type==0 时不带该字段(与 Go 同)。"""
        if failed == 0:
            return
        fields = {
            "topic": self._topic, "targets": targets, "sent": sent, "failed": failed,
            "sample_player_id": sample_pid, "first_err": str(first_err),
        }
        if event_type:
            fields["event_type"] = event_type
        _plog.get().warning("push_to_players_send_failed", **fields)

    # ── 生命周期 ──────────────────────────────────────────────────────────
    def _record_send(self, ok: bool) -> None:
        """两条发送路径(send_raw / send_raw_with_headers)共用的记账点。

        对应 Go 的 KeyOrderedProducer.recordSend,收敛成一处的理由相同:
        「成功/失败恰好记一次」是这里唯一的正确性要求,分散写就是给漏记留缝。
        """
        if ok:
            self._sent += 1
            _pmetrics.KAFKA_PRODUCE_TOTAL.labels(self._topic, "ok").inc()
            return
        self._failed += 1
        _pmetrics.KAFKA_PRODUCE_TOTAL.labels(self._topic, "error").inc()

    def stats(self) -> tuple[int, int]:
        return self._sent, self._failed

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await asyncio.to_thread(self._producer.close)
        _plog.get().info(
            "kafka_producer_closed", topic=self._topic, success=self._sent, error=self._failed
        )


# ─────────────────────────── 消费者 ───────────────────────────────────────────
#
# 对应 Go 侧 pkg/kafkax/consumer.go 的 KeyOrderedConsumer。
#
# 三条"错了不报错"的契约,逐条对齐:
#
#  ① **毒丸绕过重试**:解码失败 / 格式非法这类确定性错误,重试多少次都一样。
#     不绕过的话该分区会一直卡在同一条消息上,后面的消息永远消费不到 —— 而
#     消费组看起来是"活着的",只是 lag 一直涨。
#  ② **handler 异常必须归一化成毒丸**:确定性异常任其展开会崩掉消费循环,重启后
#     重放同 offset → 同异常 → CrashLoop,该分区**永久卡死**。投 DLQ + ack 是唯一
#     能前进的路。(Go 侧注释原话:并发 map 写是 runtime fatal,recover 兜不住,不在此列。)
#  ③ **DLQ 投递失败时不得 ack**。这一条的方向最容易写反:ack 了就等于"处理不了、
#     也没留证、还告诉 broker 我收到了" —— 事件静默消失。三态必须是:
#     没配 DLQ → 丢弃并 ack(loss-tolerant,但要打 DROPPED 让它可观测);
#     DLQ 成功 → ack;DLQ 失败 → **不 ack**,等重投。


class LazyProducer:
    """惰性生产者。对应 Go 的 kafkax.LazyProducer(pkg/kafkax/lazy.go)。

    **要解决的缺陷**(两栈同构):`KeyOrderedProducer.__init__` 会直接构造
    `KafkaProducer(...)`,而 kafka-python 在构造期就做 bootstrap 连接,broker 不可达时
    抛 `NoBrokersAvailable`。在装配期一次性构造 producer 的服务上,这造成一个静默且
    **不可自愈**的状态:

        启动时 Kafka 恰好不可用 → producer 构造抛异常 → 只打一条 WARN、pusher 保持 None
        → 出箱发布器 `if pusher is None: return` 直接退出、**连任务都不起**
        → Kafka 后来恢复也不会补发,出箱只增不减,**必须重启进程**才排空。

    而出箱堆积此前没有任何告警规则,所以这一档在生产上是完全静默的。

    改惰性后:首次投递才连,broker 不可达时 send_raw 抛错 → 发布器按既有的
    「投递失败 → 中断本轮保序」路径退出本轮 → 下一拍重试 → Kafka 恢复即自动排空。

    **不改变的语义边界**:「brokers 未配 = 推送刻意禁用」那一档不归本类管 —— 调用方仍在
    装配期判 `cfg.kafka.brokers` 为空后根本不构造 LazyProducer,pusher 保持 None。
    本类只覆盖「配了 brokers 但此刻连不上」,也就是原来会永久卡死的那一档。

    并发安全:用 `asyncio.Lock` 串行化建连(本类只在事件循环里被 await,不需要线程锁);
    建连本身仍走 `asyncio.to_thread`,不阻塞事件循环。
    """

    __slots__ = ("_conf", "_topic", "_producer", "_closed", "_lock")

    def __init__(self, conf: ProducerConf, topic: str) -> None:
        self._conf = conf
        self._topic = topic
        self._producer: KeyOrderedProducer | None = None
        self._closed = False
        self._lock = asyncio.Lock()

    @property
    def topic(self) -> str:
        return self._topic

    async def _get(self) -> KeyOrderedProducer:
        """返回已就绪的底层生产者,必要时建连。

        建连失败**不缓存失败态** —— 下一次投递会再试一次,这正是「Kafka 恢复即自愈」
        的来源。与 Go 的 LazyProducer.producer() 双检结构等价。
        """
        if self._closed:
            raise RuntimeError(f"kafkax: lazy producer closed (topic={self._topic})")
        if self._producer is not None:
            return self._producer
        async with self._lock:
            # 等锁期间可能已被别的调用方建好或已关闭。
            if self._closed:
                raise RuntimeError(f"kafkax: lazy producer closed (topic={self._topic})")
            if self._producer is not None:
                return self._producer
            # KafkaProducer 构造是阻塞 I/O,必须挪出事件循环(同本模块顶部的线程池纪律)。
            producer = await asyncio.to_thread(KeyOrderedProducer, self._conf, self._topic)
            if self._closed:
                # 建连期间被 close 了:把刚建好的客户端还回去,不能泄漏。
                await producer.close()
                raise RuntimeError(f"kafkax: lazy producer closed (topic={self._topic})")
            self._producer = producer
            return producer

    async def send_raw(self, key: str, payload: bytes, event_type: int = 0) -> None:
        """见 KeyOrderedProducer.send_raw;producer 未就绪时先按需建连。"""
        producer = await self._get()
        await producer.send_raw(key, payload, event_type)

    async def close(self) -> None:
        """关闭底层生产者(若已建连)。从未建连时是无害 no-op。"""
        if self._closed:
            return
        self._closed = True
        producer, self._producer = self._producer, None
        if producer is not None:
            await producer.close()


# 消费主循环的重连退避。与 Go 侧 consumeBackoffMin / consumeBackoffMax 逐值一致
# (200ms → 30s 指数翻倍,任何一次 poll 正常返回都立刻复位)。
#
# 没有退避时,broker 挂掉会让循环变成不受控的忙循环:单核打满 + 日志以每秒数千条的
# 速度刷满磁盘,把真正有用的上下文冲出保留窗口。而 dev 永远试不出来这一档 ——
# 那几个消费型服务在 broker 不可达时是启动期 fail-fast,根本活不到进主循环。
_CONSUME_BACKOFF_MIN_SEC = 0.2
_CONSUME_BACKOFF_MAX_SEC = 30.0


class PoisonError(Exception):
    """不可重试的毒丸(解码失败 / 格式非法)。对应 Go 的 kafkax.PoisonError。"""

    __slots__ = ("cause",)

    def __init__(self, cause: BaseException | str | None = None) -> None:
        self.cause = cause
        super().__init__("poison message" if cause is None else f"poison message: {cause}")


def poison(err: BaseException | str) -> PoisonError:
    """把错误包成毒丸。对应 Go 的 kafkax.Poison(err)。"""
    return PoisonError(err)


@dataclasses.dataclass(frozen=True)
class RetryPolicy:
    """业务瞬时错误的进程内重试。零值 = 不重试(与 Go 的零值语义一致)。"""

    max_retries: int = 0
    backoff_sec: float = 0.0  # <=0 视为 0.2s(Go: 200ms)


@dataclasses.dataclass(frozen=True)
class ConsumerConf:
    brokers: tuple[str, ...] = ()
    topic: str = ""
    group_id: str = ""
    retry: RetryPolicy = dataclasses.field(default_factory=RetryPolicy)
    # "earliest" = 历史保序消费(§14.2 默认);"latest" = 只收新消息
    # (每 Pod 独立 group 的广播消费:fresh group 不得回放全部留存广播)。
    initial_offset: str = "earliest"
    # 关闭 offset 提交。广播 per-Pod group 若复用了带已提交 offset 的 group 名
    # (Pod 同名重启 / StatefulSet),客户端会**忽略 initial_offset 从旧 committed
    # offset 续读**,把积压广播整段重放给全部在线连接。true = 纯实时消费者。
    disable_offset_commit: bool = False
    poll_timeout_ms: int = 1000


class KeyOrderedConsumer:
    """通用消费者。`run()` 设计成交给 `server.run(background=[...])` 驱动。

    与 Go 的一处实现差异(语义相同):Go 用 sarama 的 MarkMessage 标记进度;
    这里按 partition 维护"连续成功的最高 offset"并只提交它 —— 一旦某条判定为
    不 ack,该 partition 的水位就停在它前面,与"不 Mark"等价。
    """

    __slots__ = ("_conf", "_handler", "_dlq", "_consumer", "_stopped", "_consumer_factory")

    def __init__(self, conf, handler, *, dlq=None, consumer_factory=None) -> None:
        if handler is None:
            raise ValueError("handler required")
        self._conf = conf
        self._handler = handler
        self._dlq = dlq
        self._stopped = False
        self._consumer_factory = consumer_factory
        self._consumer = None

    def _build(self):
        if self._consumer_factory is not None:
            return self._consumer_factory()
        from kafka import KafkaConsumer

        return KafkaConsumer(
            self._conf.topic,
            bootstrap_servers=list(self._conf.brokers),
            group_id=self._conf.group_id,
            enable_auto_commit=False,
            auto_offset_reset=self._conf.initial_offset or "earliest",
        )

    async def run(self) -> None:
        """消费循环,直到 stop()。异常由 safego 兜底(server.run 已接线)。"""
        self._consumer = self._build()
        logger = _plog.get()
        logger.info(
            "kafka_consumer_started", topic=self._conf.topic, group_id=self._conf.group_id
        )
        backoff = _CONSUME_BACKOFF_MIN_SEC
        try:
            while not self._stopped:
                try:
                    batch = await asyncio.to_thread(
                        self._consumer.poll, self._conf.poll_timeout_ms
                    )
                except asyncio.CancelledError:
                    raise  # 取消必须穿透,否则停机时循环退不出去
                except BaseException as exc:  # noqa: BLE001
                    # poll 抛错此前会**直接逃出 run()**,消费循环就此死掉(safego 记一条就没了),
                    # 消费组看着还在、lag 一路涨。改成记数 + 退避 + 继续,与 Go 侧
                    # Start() 里 Consume 返错的处理逐条对齐(pkg/kafkax/consumer.go)。
                    _pmetrics.KAFKA_CONSUME_LOOP_ERROR_TOTAL.labels(
                        self._conf.topic, self._conf.group_id
                    ).inc()
                    logger.error(
                        "kafka_consume_loop_failed",
                        topic=self._conf.topic, group=self._conf.group_id,
                        backoff=backoff, err=str(exc),
                        hint="broker 不可达 / rebalance 失败;本条持续刷 = Kafka 侧问题,不是业务 handler 的问题",
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, _CONSUME_BACKOFF_MAX_SEC)
                    continue
                backoff = _CONSUME_BACKOFF_MIN_SEC
                if not batch:
                    continue
                for tp, messages in batch.items():
                    await self._consume_partition(tp, messages)
        finally:
            await asyncio.to_thread(self._consumer.close)
            logger.info("kafka_consumer_stopped", topic=self._conf.topic)

    async def _consume_partition(self, tp, messages) -> None:
        """顺序处理一个 partition 的一批消息,只提交连续成功的水位。"""
        commit_upto = None
        # 分区滞后:highwater 是随 fetch 一起带回来的本地缓存值,读它不产生额外 broker
        # 请求。空闲分区没有消息也就没有本回调,值会停在最后一次观测 —— 判「消费卡住」
        # 必须配合 pandora_kafka_consume_total 的 rate 一起看(与 Go 侧同一口径)。
        #
        # ★ 整段被 getattr 兜住:**指标绝不能反过来打断消费**。highwater 只在真的
        # KafkaConsumer 上有,consumer_factory 注入的替身(测试桩、将来可能的其它来源)
        # 没有它;少一条 gauge 是可接受的,为了一条 gauge 让整条消费链抛 AttributeError
        # 不可接受。tp.partition 同理走 getattr —— 替身的 tp 未必是 TopicPartition。
        if messages:
            highwater_fn = getattr(self._consumer, "highwater", None)
            partition = getattr(tp, "partition", None)
            if callable(highwater_fn) and partition is not None:
                highwater = highwater_fn(tp)
                if highwater is not None:
                    lag = highwater - messages[-1].offset - 1
                    if lag >= 0:
                        _pmetrics.KAFKA_CONSUMER_LAG.labels(
                            self._conf.topic, self._conf.group_id, str(partition)
                        ).set(lag)
        for msg in messages:
            if self._stopped:
                break
            ok = await self._process_message(msg)
            if not ok:
                # ★ 停在这里:后面的消息本轮不处理,水位也不推进 —— 等重投。
                # 继续往下会跳过这一条,而它正是"不可丢"的那一类。
                break
            commit_upto = msg.offset
        if commit_upto is not None and not self._conf.disable_offset_commit:
            await asyncio.to_thread(self._commit, tp, commit_upto)

    def _commit(self, tp, offset: int) -> None:
        from kafka import OffsetAndMetadata

        self._consumer.commit({tp: OffsetAndMetadata(offset + 1, None, -1)})

    async def _process_message(self, msg) -> bool:
        """返回是否应 ack。三档语义逐条对齐 Go 的 processMessage。"""
        logger = _plog.get()
        err = await self._call_handler(msg)
        if err is None:
            self._count(_pmetrics.KAFKA_RESULT_OK)
            return True

        if isinstance(err, PoisonError):
            self._count(_pmetrics.KAFKA_RESULT_POISON)
            logger.error(
                "kafka_poison_message", topic=msg.topic, partition=msg.partition,
                offset=msg.offset, key=_key_str(msg), err=str(err),
            )
            return await self._to_dlq(msg)

        backoff = self._conf.retry.backoff_sec if self._conf.retry.backoff_sec > 0 else 0.2
        for attempt in range(1, self._conf.retry.max_retries + 1):
            if self._stopped:
                return False
            await asyncio.sleep(backoff)
            err = await self._call_handler(msg)
            if err is None:
                self._count(_pmetrics.KAFKA_RESULT_OK)
                return True
            if isinstance(err, PoisonError):
                self._count(_pmetrics.KAFKA_RESULT_POISON)
                logger.error(
                    "kafka_poison_on_retry", attempt=attempt, topic=msg.topic,
                    partition=msg.partition, offset=msg.offset,
                    key=_key_str(msg), err=str(err),
                )
                return await self._to_dlq(msg)
            self._count(_pmetrics.KAFKA_RESULT_RETRY)
            logger.warning(
                "kafka_handler_retry_failed", attempt=attempt,
                max_retries=self._conf.retry.max_retries, topic=msg.topic,
                partition=msg.partition, offset=msg.offset, key=_key_str(msg), err=str(err),
            )
        self._count(_pmetrics.KAFKA_RESULT_EXHAUSTED)
        logger.error(
            "kafka_handler_retries_exhausted", topic=msg.topic, partition=msg.partition,
            offset=msg.offset, key=_key_str(msg), err=str(err),
            # 堆栈只在这里打一次:重试期间每轮都打会把同一个 bug 刷 N 遍,
            # 而真正需要它的时刻是"确定要进 DLQ 了"。
            stack=self._stack_of(err) if err is not None else "",
        )
        return await self._to_dlq(msg)

    async def _call_handler(self, msg):
        """执行 handler,捕获一切异常并作为"错误"返回。对应 Go 的 callHandler。

        ② 号契约的本体在这里:**任何异常都不许逃出去**。逃出去 = 消费循环崩 →
        重启重放同 offset → 同异常 → 该分区永久卡死(CrashLoop)。

        ⚠️ **与 Go 的一处语言性差异,必须知道**:Go 的 handler 用**返回值**表达
        可重试错误、用 **panic** 表达意外故障,`callHandler` 把 panic 归一成毒丸
        (跳过重试直投 DLQ)。Python 里两者都是"抛异常",分不开。
        所以这里的约定是:

            raise PoisonError / kafkax.poison(...)  → 毒丸,跳过重试
            raise 其它任何异常                        → 可重试(等价于 Go 的返回 error)

        代价是:一个**确定性**的业务 bug 会先白跑完 max_retries 次再进 DLQ,
        而 Go 那边 panic 是直接进 DLQ 的。两条关键性质都保住了(不逃逸、最终进 DLQ),
        只是多花几次重试。反过来把"其它异常"当毒丸的话,**所有瞬时故障都不会重试** ——
        下游抖一下就整条进 DLQ,那个代价大得多。
        """
        try:
            res = self._handler(msg)
            if asyncio.iscoroutine(res):
                await res
        except PoisonError as exc:
            return exc
        except asyncio.CancelledError:
            raise  # 取消必须穿透,否则停机时循环退不出去
        except BaseException as exc:  # noqa: BLE001 —— 兜底就是要抓全部
            return exc
        return None

    @staticmethod
    def _stack_of(exc: BaseException) -> str:
        import traceback

        return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))

    async def _to_dlq(self, msg) -> bool:
        """返回是否应 ack。③ 号契约的三档就在这里。"""
        logger = _plog.get()
        if self._dlq is None:
            # 没配 DLQ:消息被丢弃并 ack。必须显式记"丢弃"——上游日志写的是"→ DLQ",
            # 运维照此去 DLQ 会白找。
            self._count(_pmetrics.KAFKA_RESULT_DLQ_DROPPED)
            logger.warning(
                "kafka_message_dropped_no_dlq", topic=msg.topic, partition=msg.partition,
                offset=msg.offset, key=_key_str(msg),
            )
            return True

        # 原样保留全部原 header(event_type 等,否则回放时被当 legacy 解码)+ 三个溯源 header。
        headers = list(msg.headers or [])
        headers += [
            ("dlq-src-topic", str(msg.topic).encode()),
            ("dlq-src-partition", str(msg.partition).encode()),
            ("dlq-src-offset", str(msg.offset).encode()),
        ]
        try:
            await self._dlq.send_raw_with_headers(_key_str(msg), msg.value, headers)
        except BaseException as exc:  # noqa: BLE001
            # ★ 不 ack。ack 了就是"处理不了、没留证、还说收到了"= 事件静默消失。
            _pmetrics.KAFKA_DLQ_TOTAL.labels(
                self._conf.topic, self._conf.group_id, "failed"
            ).inc()
            logger.error(
                "kafka_dlq_send_failed_will_not_ack", topic=msg.topic, partition=msg.partition,
                offset=msg.offset, key=_key_str(msg), err=str(exc),
            )
            return False
        # ⚠️ 全仓没有 DLQ 消费者/回放器,本指标是 DLQ 唯一的发现手段(同 Go 侧 DLQTotal)。
        _pmetrics.KAFKA_DLQ_TOTAL.labels(self._conf.topic, self._conf.group_id, "ok").inc()
        logger.warning(
            "kafka_message_moved_to_dlq", topic=msg.topic, partition=msg.partition,
            offset=msg.offset, key=_key_str(msg),
        )
        return True

    def _count(self, result: str) -> None:
        """记一次消息处理结果。与 Go 的 ConsumeTotal.WithLabelValues(...).Inc() 一一对应。"""
        _pmetrics.KAFKA_CONSUME_TOTAL.labels(
            self._conf.topic, self._conf.group_id, result
        ).inc()

    def stop(self) -> None:
        self._stopped = True


def _key_str(msg) -> str:
    k = getattr(msg, "key", None)
    if k is None:
        return ""
    return k.decode(errors="replace") if isinstance(k, bytes) else str(k)
