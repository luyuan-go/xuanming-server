// Package kafkax — Consumer
//
// 来源:抽自 mmorpg/go/db/internal/kafka/key_ordered_consumer.go,**剥业务依赖**
// (db_proto.DBTask / proto_sql / proto2mysql / dynamicpb / scene-specific cache key),
// 保留:
//   - sarama ConsumerGroup + per-partition worker
//   - Handler 接口让业务自己处理消息(不再绑定 DBTask schema)
//
// **失败处理策略(per-consumer)**:见 ConsumerConfig.RetryPolicy / DLQ。
//   - handler 返回 nil          → ack。
//   - handler 返回 PoisonError   → 直接进 DLQ(解码/毒丸,重试无意义),DLQ 成功后 ack。
//   - handler 返回其它 error     → 业务瞬时错误,按 RetryPolicy 进程内有限重试;
//     重试耗尽后进 DLQ。DLQ 投递成功才 ack;DLQ 未配置(loss-tolerant 消费者)则 log 后 ack;
//     DLQ 投递失败则**不 ack**,结束本次 claim 重新 join 后从未提交 offset 重放(at-least-once,
//     battle_result / player.update 等不可丢事件靠此不丢)。
package kafkax

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	klog "github.com/go-kratos/kratos/v2/log"
)

// Handler 是消息处理函数,由业务实现。
//
// 返回 nil → ack。返回非 nil → 按消费者 RetryPolicy / DLQ 策略处理(见 package 文档)。
// 解码失败 / 永久性错误请用 Poison(err) 包装返回,消费者会跳过重试直接进 DLQ。
type Handler func(ctx context.Context, msg *sarama.ConsumerMessage) error

// PoisonError 标记不可重试的「毒丸」消息(解码失败 / 格式非法等)。
// handler 返回它(或 Poison(err))→ 消费者跳过重试,直接投 DLQ。
type PoisonError struct{ Err error }

func (e *PoisonError) Error() string {
	if e.Err == nil {
		return "poison message"
	}
	return "poison message: " + e.Err.Error()
}

func (e *PoisonError) Unwrap() error { return e.Err }

// Poison 把 err 包装成不可重试的毒丸错误。
func Poison(err error) error { return &PoisonError{Err: err} }

func isPoison(err error) bool {
	var p *PoisonError
	return errors.As(err, &p)
}

// RetryPolicy 控制业务瞬时错误的进程内重试。零值 = 不重试(MaxRetries=0)。
type RetryPolicy struct {
	// MaxRetries 是业务瞬时错误的进程内重试次数(不含首次)。<=0 表示不重试,首次失败即进 DLQ。
	MaxRetries int
	// Backoff 是每次重试前的固定退避。<=0 视为 200ms。
	Backoff time.Duration
}

// DLQProducer 是死信队列投递抽象(kafkax.KeyOrderedProducer 直接满足)。
// key 用原消息 key 保序;payload 为原始 bytes;headers 携带原消息全部 header
// (含 event_type)+ 溯源 header(原 topic/partition/offset)——否则回放 DLQ 时
// 无 header 消息会再次被当 legacy 解码,且无法定位原始来源(审计 P2)。
type DLQProducer interface {
	SendRaw(ctx context.Context, key string, payload []byte) error
	SendRawWithHeaders(ctx context.Context, key string, payload []byte, headers []sarama.RecordHeader) error
}

// KeyOrderedConsumer 是 Pandora 通用 Kafka 消费者。
type KeyOrderedConsumer struct {
	consumer       sarama.ConsumerGroup
	topic          string
	groupID        string
	partitionCount int32
	handler        Handler
	retry          RetryPolicy
	dlq            DLQProducer
	workers        map[int32]*worker
	wg             *sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
}

// ConsumerConfig 是消费者配置。
type ConsumerConfig struct {
	Brokers        []string
	Topic          string
	GroupID        string
	PartitionCount int32
	Version        sarama.KafkaVersion // 默认 V3_6_0_0
	// RetryPolicy 控制业务瞬时错误的进程内重试。零值 = 不重试。
	RetryPolicy RetryPolicy
	// DLQ 非 nil 时,重试耗尽 / 毒丸消息投递到死信队列;为 nil 时退化为「log + ack」(loss-tolerant 消费者)。
	DLQ DLQProducer
	// InitialOffset 新 consumer group 首次消费的起点:0(缺省)= OffsetOldest(历史保序
	// 消费,§14.2 默认行为不变);sarama.OffsetNewest = 只收新消息(每 Pod 独立 group 的
	// 广播消费用:fresh group 不得回放全部留存广播)。
	InitialOffset int64
	// DisableOffsetCommit 关闭 offset 提交(审计 R4 P1:广播 per-Pod group 若复用了
	// 带已提交 offset 的 group 名——Pod 重启同名/StatefulSet——sarama 会**忽略 Initial
	// 从旧 committed offset 续读**,把积压广播整段重放给全部在线连接)。true = 纯实时
	// 消费者:group 永无 committed offset,每次启动必从 InitialOffset(Newest)开始,
	// 停机窗口内的消息**有意丢弃**(广播契约:不承诺离线补投,定向帧才走投递缓冲)。
	DisableOffsetCommit bool
}

// NewKeyOrderedConsumer 创建消费者。
func NewKeyOrderedConsumer(cfg ConsumerConfig, handler Handler) (*KeyOrderedConsumer, error) {
	if handler == nil {
		return nil, errors.New("handler must not be nil")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("groupID must not be empty")
	}

	c := sarama.NewConfig()
	if cfg.Version == (sarama.KafkaVersion{}) {
		c.Version = sarama.V3_6_0_0
	} else {
		c.Version = cfg.Version
	}
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	if cfg.InitialOffset != 0 {
		c.Consumer.Offsets.Initial = cfg.InitialOffset
	}
	if cfg.DisableOffsetCommit {
		c.Consumer.Offsets.AutoCommit.Enable = false
	}
	c.Consumer.Return.Errors = true

	cg, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, c)
	if err != nil {
		return nil, fmt.Errorf("new consumer group: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &KeyOrderedConsumer{
		consumer:       cg,
		topic:          cfg.Topic,
		groupID:        cfg.GroupID,
		partitionCount: cfg.PartitionCount,
		handler:        handler,
		retry:          cfg.RetryPolicy,
		dlq:            cfg.DLQ,
		workers:        make(map[int32]*worker),
		wg:             &sync.WaitGroup{},
		ctx:            ctx,
		cancel:         cancel,
	}, nil
}

// 消费主循环的重连退避(2026-08-24)。
//
// 原实现在 Consume 返错后**原地立即重试且每轮打一条日志**:broker 挂掉时(免Docker
// Windows 链已有 8 类 broker 自杀故障)会变成不受控的忙循环 —— 单核打满、日志以每秒
// 数千条的速度刷满磁盘,把真正有用的上下文冲出保留窗口。而 dev 永远试不出来这一档:
// 那三个消费型服务在 broker 不可达时是启动期 os.Exit(1),根本活不到进主循环。
//
// 退避从 consumeBackoffMin 起指数翻倍到 consumeBackoffMax;**任何一次 Consume 正常返回
// 都立刻复位**(正常 rebalance 也会让 Consume 返回 nil,不能把它算成失败)。
const (
	consumeBackoffMin = 200 * time.Millisecond
	consumeBackoffMax = 30 * time.Second
)

// Start 启动消费循环。
func (k *KeyOrderedConsumer) Start() {
	k.wg.Add(1)
	go func() {
		defer k.wg.Done()
		backoff := consumeBackoffMin
		for {
			select {
			case <-k.ctx.Done():
				return
			default:
			}
			if err := k.consumer.Consume(k.ctx, []string{k.topic}, k); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				ConsumeLoopErrorTotal.WithLabelValues(k.topic, k.groupID).Inc()
				klog.Errorw("msg", "kafka_consume_loop_failed",
					"topic", k.topic, "group", k.groupID,
					"backoff", backoff.String(), "err", err,
					"hint", "broker 不可达 / rebalance 失败;本条持续刷 = Kafka 侧问题,不是业务 handler 的问题")
				// ctx 感知的退避:停机时立刻退出,不空等一个完整退避周期。
				timer := time.NewTimer(backoff)
				select {
				case <-k.ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				if backoff *= 2; backoff > consumeBackoffMax {
					backoff = consumeBackoffMax
				}
				continue
			}
			backoff = consumeBackoffMin
		}
	}()

	k.wg.Add(1)
	go func() {
		defer k.wg.Done()
		for {
			select {
			case <-k.ctx.Done():
				return
			case err, ok := <-k.consumer.Errors():
				if !ok {
					return
				}
				klog.Errorf("[kafkax] consumer error: %v", err)
			}
		}
	}()

	klog.Infof("[kafkax] consumer started: topic=%s group=%s", k.topic, k.groupID)
}

// Close 优雅关闭。
func (k *KeyOrderedConsumer) Close() error {
	k.cancel()
	if err := k.consumer.Close(); err != nil {
		klog.Errorf("[kafkax] close: %v", err)
	}
	k.wg.Wait()
	klog.Infof("[kafkax] consumer closed: topic=%s", k.topic)
	return nil
}

// ==================== sarama.ConsumerGroupHandler 接口实现 ====================

func (k *KeyOrderedConsumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (k *KeyOrderedConsumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (k *KeyOrderedConsumer) ConsumeClaim(
	sess sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			// 分区滞后:高水位是 sarama 随 fetch 一起带回来的,读它不产生额外 broker 请求,
			// 故可逐条刷新。空闲分区没有消息也就没有回调,值会停在最后一次观测 —— 判「消费
			// 卡住」必须配合 ConsumeTotal 的 rate 一起看(见 metrics.go ConsumerLag 注释)。
			if lag := claim.HighWaterMarkOffset() - msg.Offset - 1; lag >= 0 {
				ConsumerLag.WithLabelValues(k.topic, k.groupID,
					strconv.FormatInt(int64(msg.Partition), 10)).Set(float64(lag))
			}
			if k.processMessage(sess.Context(), msg) {
				sess.MarkMessage(msg, "")
				continue
			}
			// 未 ack(DLQ 投递失败 / 无补偿通道且消息不可丢):结束本次 claim,
			// rejoin 后从未提交 offset 重放(at-least-once)。
			klog.Errorf("[kafkax] message not acked, rejoin to replay topic=%s partition=%d offset=%d",
				msg.Topic, msg.Partition, msg.Offset)
			return nil
		case <-sess.Context().Done():
			return nil
		}
	}
}

// processMessage 处理单条消息,返回是否应 ack(MarkMessage)。
//
//   - handler 成功            → true(ack)。
//   - 毒丸(PoisonError)      → 投 DLQ,DLQ 成功 true / 失败 false。
//   - 业务瞬时错误            → 进程内重试 RetryPolicy.MaxRetries 次;成功 true;耗尽后投 DLQ。
func (k *KeyOrderedConsumer) processMessage(ctx context.Context, msg *sarama.ConsumerMessage) bool {
	err := k.callHandler(ctx, msg)
	if err == nil {
		ConsumeTotal.WithLabelValues(k.topic, k.groupID, consumeResultOK).Inc()
		return true
	}
	if isPoison(err) {
		ConsumeTotal.WithLabelValues(k.topic, k.groupID, consumeResultPoison).Inc()
		klog.Errorf("[kafkax] poison message → DLQ topic=%s partition=%d offset=%d key=%s: %v",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), err)
		return k.toDLQ(ctx, msg)
	}

	backoff := k.retry.Backoff
	if backoff <= 0 {
		backoff = 200 * time.Millisecond
	}
	for attempt := 1; attempt <= k.retry.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		err = k.callHandler(ctx, msg)
		if err == nil {
			ConsumeTotal.WithLabelValues(k.topic, k.groupID, consumeResultOK).Inc()
			return true
		}
		if isPoison(err) {
			ConsumeTotal.WithLabelValues(k.topic, k.groupID, consumeResultPoison).Inc()
			klog.Errorf("[kafkax] poison on retry %d → DLQ topic=%s partition=%d offset=%d key=%s: %v",
				attempt, msg.Topic, msg.Partition, msg.Offset, string(msg.Key), err)
			return k.toDLQ(ctx, msg)
		}
		ConsumeTotal.WithLabelValues(k.topic, k.groupID, consumeResultRetry).Inc()
		klog.Warnf("[kafkax] handler retry %d/%d failed topic=%s partition=%d offset=%d key=%s: %v",
			attempt, k.retry.MaxRetries, msg.Topic, msg.Partition, msg.Offset, string(msg.Key), err)
	}
	ConsumeTotal.WithLabelValues(k.topic, k.groupID, consumeResultExhausted).Inc()
	klog.Errorf("[kafkax] handler retries exhausted → DLQ topic=%s partition=%d offset=%d key=%s: %v",
		msg.Topic, msg.Partition, msg.Offset, string(msg.Key), err)
	return k.toDLQ(ctx, msg)
}

// callHandler 执行 handler 并把可恢复 panic 归一化为毒丸(压测审核【必修-6】/P2-1)。
//
// 解码成功后深层业务的确定性 panic 若任其展开,会崩掉 sarama 消费 goroutine 乃至进程;
// 重启后重放同 offset → 同 panic → CrashLoop,该分区消费永久卡死。按毒丸投 DLQ + ack
// 是唯一能前进的路:消息进 DLQ 留证待人工回放,分区继续消费。并发 map 写是 runtime
// fatal throw,recover 兜不住,不在此列。
func (k *KeyOrderedConsumer) callHandler(ctx context.Context, msg *sarama.ConsumerMessage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			klog.Errorf("[kafkax] handler panic → poison topic=%s partition=%d offset=%d key=%s: %v\n%s",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), r, debug.Stack())
			err = Poison(fmt.Errorf("handler panic: %v", r))
		}
	}()
	return k.handler(ctx, msg)
}

// toDLQ 把消息投递到死信队列。返回是否应 ack。
//   - DLQ 未配置 → log 后 ack(loss-tolerant 消费者沿用旧「log + ack」行为)。
//   - DLQ 投递成功 → ack。
//   - DLQ 投递失败 → 不 ack(由 ConsumeClaim 重放,保证不可丢事件不丢)。
func (k *KeyOrderedConsumer) toDLQ(ctx context.Context, msg *sarama.ConsumerMessage) bool {
	if k.dlq == nil {
		// 未配 DLQ 的 loss-tolerant 消费者:消息被丢弃并 ack。调用点日志说"→ DLQ"实则无 DLQ,
		// 运维照此去 DLQ 会白找;此处显式记真实去向(消息被丢弃),让"丢弃的 kafka 消息"可观测。
		ConsumeTotal.WithLabelValues(k.topic, k.groupID, consumeResultDLQDrop).Inc()
		klog.Warnf("[kafkax] message DROPPED (no DLQ configured) topic=%s partition=%d offset=%d key=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key))
		return true // 无 DLQ 通道:沿用旧行为 log + ack
	}
	// 原样保留全部原消息 header(event_type 等,回放不再被当 legacy)+ 溯源 header。
	headers := make([]sarama.RecordHeader, 0, len(msg.Headers)+3)
	for _, h := range msg.Headers {
		if h != nil {
			headers = append(headers, *h)
		}
	}
	headers = append(headers,
		sarama.RecordHeader{Key: []byte("dlq-src-topic"), Value: []byte(msg.Topic)},
		sarama.RecordHeader{Key: []byte("dlq-src-partition"), Value: []byte(strconv.FormatInt(int64(msg.Partition), 10))},
		sarama.RecordHeader{Key: []byte("dlq-src-offset"), Value: []byte(strconv.FormatInt(msg.Offset, 10))},
	)
	if err := k.dlq.SendRawWithHeaders(ctx, string(msg.Key), msg.Value, headers); err != nil {
		DLQTotal.WithLabelValues(k.topic, k.groupID, "failed").Inc()
		klog.Errorf("[kafkax] DLQ send failed (will not ack) topic=%s partition=%d offset=%d key=%s: %v",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), err)
		return false
	}
	// ⚠️ 全仓没有 DLQ 消费者/回放器,本指标是 DLQ 唯一的发现手段(见 metrics.go DLQTotal 注释)。
	DLQTotal.WithLabelValues(k.topic, k.groupID, "ok").Inc()
	klog.Warnf("[kafkax] message moved to DLQ topic=%s partition=%d offset=%d key=%s",
		msg.Topic, msg.Partition, msg.Offset, string(msg.Key))
	return true
}

// worker 占位(W2 实现 per-partition worker 队列时启用)。
type worker struct {
	partition int32
}
