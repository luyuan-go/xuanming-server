// Package kafkax — Producer
//
// 来源:抽自 mmorpg/go/login/internal/kafka/key_ordered_producer.go,
// **剥业务依赖**(db_proto / consistent 内部包路径),保留:
//   - SyncProducer + idempotent 配置
//   - 一致性哈希按 key 路由 partition
//   - 内置 payloadPool 减少 GC
//
// **W1-D2 阶段**:不实现 retry queue + plainProducer + DLQ,留 W2 battle_result 时再补。
package kafkax

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	klog "github.com/go-kratos/kratos/v2/log"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/config"
)

// ProducerMeta 是发送消息的元数据,用于 payload 回收。
type ProducerMeta struct {
	producer *KeyOrderedProducer
	payload  []byte
}

// KeyOrderedProducer 是基于 SyncProducer 的 key-ordered 幂等生产者。
// 同一 key 永远落同一个 partition,partition 内 sarama 保序。
type KeyOrderedProducer struct {
	producer     sarama.SyncProducer
	client       sarama.Client
	topic        string
	partitionCnt int
	mu           sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
	consistent   *Consistent
	closed       bool
	payloadPool  sync.Pool

	successCount int64
	errorCount   int64
}

// buildProducerConfig 从 config.KafkaConfig 构造 sarama 生产者配置。
//
// sarama.NewConfig() 已把三个 Net 超时初始化为 30s,且 Validate() 强制三者都 > 0。
// 仅当 yaml 显式给出正值时才覆盖,缺省字段保留 sarama 默认,避免把未配置字段写成 0
// 触发 "Net.DialTimeout/ReadTimeout/WriteTimeout must be > 0" 而导致 producer 构造失败。
// producer 是否属于启动强依赖由各服务入口决定；配置了 brokers 的 team / matchmaker 会在该
// 构造失败时拒绝 Ready（team 防 Invite 通知永久静默丢失，matchmaker 防组队非队长成员唯一的
// match.progress READY 通道永久静默丢失），其他调用方按各自恢复语义处理。
func buildProducerConfig(cfg config.KafkaConfig) *sarama.Config {
	c := sarama.NewConfig()
	c.Version = sarama.V3_6_0_0
	if d := cfg.DialTimeout.Std(); d > 0 {
		c.Net.DialTimeout = d
	}
	if d := cfg.ReadTimeout.Std(); d > 0 {
		c.Net.ReadTimeout = d
	}
	if d := cfg.WriteTimeout.Std(); d > 0 {
		c.Net.WriteTimeout = d
	}
	c.Producer.Return.Successes = true
	c.Producer.Return.Errors = true
	c.Producer.Retry.Max = cfg.RetryMax
	if c.Producer.Retry.Max <= 0 {
		c.Producer.Retry.Max = 3
	}
	if d := cfg.RetryBackoff.Std(); d > 0 {
		c.Producer.Retry.Backoff = d
	}
	c.Producer.RequiredAcks = sarama.WaitForAll
	if cfg.ChannelBuffer > 0 {
		c.ChannelBufferSize = cfg.ChannelBuffer
	}
	c.Producer.Compression = cfg.ParseCompression()
	c.Producer.Idempotent = cfg.Idempotent
	c.Net.MaxOpenRequests = 1
	return c
}

// NewKeyOrderedProducer 用 config.KafkaConfig + topic 创建生产者。
func NewKeyOrderedProducer(cfg config.KafkaConfig, topic string) (*KeyOrderedProducer, error) {
	c := buildProducerConfig(cfg)

	if err := c.Validate(); err != nil {
		klog.Errorf("[kafkax] invalid sarama config: %v", err)
		return nil, err
	}

	client, err := sarama.NewClient(cfg.Brokers, c)
	if err != nil {
		klog.Errorf("[kafkax] new client failed: %v", err)
		return nil, fmt.Errorf("new client: %w", err)
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		_ = client.Close()
		klog.Errorf("[kafkax] new sync producer failed: %v", err)
		return nil, fmt.Errorf("new sync producer: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &KeyOrderedProducer{
		producer:     producer,
		client:       client,
		topic:        topic,
		partitionCnt: int(cfg.PartitionCnt),
		ctx:          ctx,
		cancel:       cancel,
		consistent:   NewConsistent(),
		payloadPool: sync.Pool{
			New: func() any { return make([]byte, 0, 1024) },
		},
	}
	if p.partitionCnt <= 0 {
		p.partitionCnt = 4
	}

	// 初始 partition 注入哈希环
	for i := int32(0); i < int32(p.partitionCnt); i++ {
		p.consistent.AddPartition(i)
	}

	klog.Infof("[kafkax] producer ready: topic=%s partitions=%d idempotent=%v",
		topic, p.partitionCnt, cfg.Idempotent)
	return p, nil
}

// Send 把 proto 消息序列化后,按 key 路由到 partition 发送。
//
// 调用方:`producer.Send(ctx, "M_xxx", &BattleResult{...})`
func (p *KeyOrderedProducer) Send(ctx context.Context, key string, msg proto.Message) error {
	if p.isClosed() {
		return fmt.Errorf("producer closed")
	}

	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal proto: %w", err)
	}

	partition, ok := p.consistent.GetPartition(key)
	if !ok {
		return fmt.Errorf("no partition (hash ring empty)")
	}

	pm := &sarama.ProducerMessage{
		Topic:     p.topic,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(payload),
		Partition: partition,
		Timestamp: time.Now(),
	}

	_, _, err = p.producer.SendMessage(pm)
	if err != nil {
		p.recordSend(false)
		return fmt.Errorf("send: %w", err)
	}
	p.recordSend(true)
	return nil
}

// SendRaw 直接发字节(不序列化)。
func (p *KeyOrderedProducer) SendRaw(ctx context.Context, key string, payload []byte) error {
	if p.isClosed() {
		return fmt.Errorf("producer closed")
	}

	partition, ok := p.consistent.GetPartition(key)
	if !ok {
		return fmt.Errorf("no partition")
	}

	pm := &sarama.ProducerMessage{
		Topic:     p.topic,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(payload),
		Partition: partition,
		Timestamp: time.Now(),
	}

	if _, _, err := p.producer.SendMessage(pm); err != nil {
		p.recordSend(false)
		return fmt.Errorf("send: %w", err)
	}
	p.recordSend(true)
	return nil
}

// HeaderEventType 是 push 域内事件类型判别键的 kafka header 名。
// push consumer 从此 header 读出值填入 PushFrame.event_type;
// 缺省(老 producer 不填)→ consumer 读到 0 → 客户端按该 topic 的旧事件解析(向后兼容)。
const HeaderEventType = "event_type"

// SendRawWithEventType 与 SendRaw 相同,额外在 kafka header 里带上 event_type
// (push 域内多事件类型路由的判别键,详见 pandora/push/v1/push.proto)。
// eventType=0 时不写 header(等价于 SendRaw),避免给现网旧事件平白加 header。
// 参数中 key 是 Kafka 分区键(玩家推送时为十进制 player_id),payload 是业务事件原始 protobuf
// 字节,eventType 是当前 topic 域内的事件枚举值;ctx 保留与 SendRaw 一致的调用契约,
// 当前同步 Sarama 发送不会读取它。
func (p *KeyOrderedProducer) SendRawWithEventType(ctx context.Context, key string, payload []byte, eventType uint32) error {
	// 关闭后的 producer 不再接收新消息,避免把发送误报为成功或写入已关闭的客户端。
	if p.isClosed() {
		return fmt.Errorf("producer closed")
	}

	// partition 是一致性哈希为 key 选出的固定分区;ok=false 表示当前分区环无法给出路由。
	partition, ok := p.consistent.GetPartition(key)
	if !ok {
		return fmt.Errorf("no partition")
	}

	// pm 是交给 Sarama 的完整消息;显式写入分区保证同一 key 的消息保持分区内顺序。
	pm := &sarama.ProducerMessage{
		Topic:     p.topic,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(payload),
		Partition: partition,
		Timestamp: time.Now(),
	}
	// 0 是每个 topic 的旧事件兼容值,因此只有非零类型才需要增加判别 header。
	if eventType != 0 {
		pm.Headers = []sarama.RecordHeader{{
			Key:   []byte(HeaderEventType),
			Value: []byte(strconv.FormatUint(uint64(eventType), 10)),
		}}
	}

	// SendMessage 同步返回 broker 发送结果;失败只累计 errorCount,绝不能累计成功数。
	if _, _, err := p.producer.SendMessage(pm); err != nil {
		p.recordSend(false)
		return fmt.Errorf("send: %w", err)
	}
	// 只有 Sarama 确认发送成功后才累计 successCount。
	p.recordSend(true)
	return nil
}

// SendRawWithHeaders 与 SendRaw 相同,额外原样携带调用方给定的 kafka headers。
// DLQ 投递用:保留原消息全部 header(event_type 等)+ 溯源 header,回放不丢判别信息。
func (p *KeyOrderedProducer) SendRawWithHeaders(ctx context.Context, key string, payload []byte, headers []sarama.RecordHeader) error {
	if p.isClosed() {
		return fmt.Errorf("producer closed")
	}
	partition, ok := p.consistent.GetPartition(key)
	if !ok {
		return fmt.Errorf("no partition")
	}
	pm := &sarama.ProducerMessage{
		Topic:     p.topic,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(payload),
		Partition: partition,
		Timestamp: time.Now(),
		Headers:   headers,
	}
	if _, _, err := p.producer.SendMessage(pm); err != nil {
		p.recordSend(false)
		return fmt.Errorf("send: %w", err)
	}
	p.recordSend(true)
	return nil
}

// PushToPlayers 把同一份 payload 按 player_id 路由分发到 N 个玩家(W3 ④,2026-06-05)。
//
// 这是 push 推送的统一入口,**业务服必须走本方法**,review 时只看一处:
//
//  1. 自动排除 callerPlayerID(原则 2:发起方不收自己触发的 push,看 RPC response 即可)
//     - 例外:已受理型 RPC(MatchProgressEvent 等)需要发给所有人含发起方,
//     传 callerPlayerID = 0 跳过排除
//
//  2. 每个目标 player_id 用 SendRaw 发一次,kafka key = strconv.FormatUint(playerID, 10),
//     一致性哈希保证同玩家事件落同一 partition,partition 内 sarama 保序(不变量 §9)
//
//  3. 失败累加+continue 不阻断:某玩家发失败不能影响其他玩家;返回 (sent, lastErr),
//     调用方决定是否汇报(不上抛业务错误)。本层只在**批结束后**汇总一条失败日志
//     (topic 维度 + failed/targets 分布,见 logPushBatchFailure);业务侧另记自己的
//     上下文(team_id / match_id / reason),两头各一条、都不按人数刷屏。
//
// 调用示例(team 服务广播队员变更):
//
//	memberIDs := []uint64{1001, 1002, 1003}
//	payload, _ := proto.Marshal(&teamv1.TeamUpdateEvent{...})
//	producer.PushToPlayers(ctx, callerID, memberIDs, payload)
func (p *KeyOrderedProducer) PushToPlayers(
	ctx context.Context,
	callerPlayerID uint64,
	toPlayerIDs []uint64,
	payload []byte,
) (sent int, lastErr error) {
	var failed int
	var firstErr error
	var samplePID uint64
	for _, pid := range toPlayerIDs {
		if pid == callerPlayerID {
			// 原则 2:不发给发起方;callerPlayerID=0 时该条件永不满足 → 全发(原则 3 例外)
			continue
		}
		if err := p.SendRaw(ctx, strconv.FormatUint(pid, 10), payload); err != nil {
			failed++
			if firstErr == nil {
				firstErr, samplePID = err, pid
			}
			lastErr = err
			continue
		}
		sent++
	}
	p.logPushBatchFailure(len(toPlayerIDs), sent, failed, samplePID, 0, firstErr)
	return sent, lastErr
}

// logPushBatchFailure 在扇出循环结束后按批汇总一条失败日志(模式 C:循环内逐个打会按
// 名单人数刷屏——本函数的契约就是「hub 广播 500 人/实例」的统一入口,broker 抖动时
// 逐条打 = 500 行/次广播)。聚合行比逐条更有用:直接给出「N 个目标里失败 M 个」。
// failed==0 时不打;eventType==0 时不带该字段(与 PushToPlayers 语义一致)。
func (p *KeyOrderedProducer) logPushBatchFailure(targets, sent, failed int, samplePID uint64, eventType uint32, firstErr error) {
	if failed == 0 {
		return
	}
	if eventType == 0 {
		klog.Warnf("[kafkax] push_to_players send_failed topic=%s targets=%d sent=%d failed=%d sample_player_id=%d first_err=%v",
			p.topic, targets, sent, failed, samplePID, firstErr)
		return
	}
	klog.Warnf("[kafkax] push_to_players send_failed topic=%s targets=%d sent=%d failed=%d sample_player_id=%d event_type=%d first_err=%v",
		p.topic, targets, sent, failed, samplePID, eventType, firstErr)
}

// PushToPlayersWithEventType 与 PushToPlayers 相同(排除 caller、按 player_id 路由分发),
// 额外给每条消息带上 event_type header(push 域内多事件类型路由判别键)。
// eventType=0 时行为等价于 PushToPlayers(不写 header)。
// callerPlayerID=0 表示不排除任何人;toPlayerIDs 是候选接收者,payload 与 eventType 对所有
// 接收者相同。返回的 sent 是成功发送人数,lastErr 是本批最后一次发送错误(部分成功仍继续)。
func (p *KeyOrderedProducer) PushToPlayersWithEventType(
	ctx context.Context,
	callerPlayerID uint64,
	toPlayerIDs []uint64,
	payload []byte,
	eventType uint32,
) (sent int, lastErr error) {
	var failed int
	var firstErr error
	var samplePID uint64
	// pid 是当前处理的目标玩家 ID,同时会被编码成 Kafka key 以维持玩家内事件顺序。
	for _, pid := range toPlayerIDs {
		// 发起方通过 RPC 响应获得结果,默认不重复接收自己触发的推送。
		if pid == callerPlayerID {
			continue
		}
		// 单个玩家发送失败只累加并继续,避免一个坏目标阻断同批其他玩家;
		// 失败详情按批汇总(见 logPushBatchFailure),不在循环内逐条打日志。
		if err := p.SendRawWithEventType(ctx, strconv.FormatUint(pid, 10), payload, eventType); err != nil {
			failed++
			if firstErr == nil {
				firstErr, samplePID = err, pid
			}
			lastErr = err
			continue
		}
		// sent 只统计已由底层 producer 确认成功的目标数。
		sent++
	}
	p.logPushBatchFailure(len(toPlayerIDs), sent, failed, samplePID, eventType, firstErr)
	return sent, lastErr
}

// Close 优雅关闭。
func (p *KeyOrderedProducer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	p.cancel()
	if err := p.producer.Close(); err != nil {
		klog.Errorf("[kafkax] producer close: %v", err)
	}
	if err := p.client.Close(); err != nil {
		klog.Errorf("[kafkax] client close: %v", err)
	}

	klog.Infof("[kafkax] producer closed: topic=%s success=%d error=%d",
		p.topic, atomic.LoadInt64(&p.successCount), atomic.LoadInt64(&p.errorCount))
	return nil
}

// Stats 返回成功 / 失败计数(累计)。
func (p *KeyOrderedProducer) Stats() (success, errCount int64) {
	return atomic.LoadInt64(&p.successCount), atomic.LoadInt64(&p.errorCount)
}

// recordSend 是四条发送路径(Send / SendRaw / SendRawWithEventType / SendRawWithHeaders)
// 共用的记账点:进程内累计计数 + prometheus 指标。
//
// 收敛成一处的原因:此前四条路径各自内联 atomic.AddInt64,加指标就要在八个位置各贴一行,
// 而"成功/失败必须恰好记一次"是这里唯一的正确性要求 —— 分散写就是给漏记留缝
// (Stats 的进程内计数本身也只在 Close 日志里露过一次面,长期没有外部可见性)。
func (p *KeyOrderedProducer) recordSend(ok bool) {
	if ok {
		atomic.AddInt64(&p.successCount, 1)
		ProduceTotal.WithLabelValues(p.topic, produceResultOK).Inc()
		return
	}
	atomic.AddInt64(&p.errorCount, 1)
	ProduceTotal.WithLabelValues(p.topic, produceResultError).Inc()
}

func (p *KeyOrderedProducer) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}
