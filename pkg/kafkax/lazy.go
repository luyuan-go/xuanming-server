// Package kafkax —— 惰性生产者(2026-08-24)。
//
// **要解决的缺陷**:NewKeyOrderedProducer 在**构造期**就 sarama.NewClient 拨号
// (producer.go),broker 不可达即返错。在装配期一次性构造 producer 的服务上,这造成一个
// 静默且不可自愈的状态:
//
//	启动时 Kafka 恰好不可用 → producer 构造失败 → 只打一条 Warn、pusher 保持 nil
//	→ 出箱发布器 if pusher == nil { return } 直接退出、**goroutine 根本没起**
//	→ Kafka 后来恢复也不会补发,出箱只增不减,**必须重启进程**才恢复。
//
// 而出箱堆积此前没有任何告警规则,所以这一档在生产上是完全静默的。
//
// LazyProducer 把拨号推迟到**首次投递**:broker 不可达时 Send* 返错,调用方(发布器)按
// 既有的「投递失败 → 中断本轮保序」路径退出本轮,下一拍重试;Kafka 恢复后出箱自动排空,
// 不需要重启。构造本身不做任何网络 IO,故装配期永远成功。
//
// **不改变的语义边界**:「brokers 未配 = 推送刻意禁用」那一档不归本类型管 —— 调用方仍在
// 装配期判 len(cfg.Brokers) == 0 后根本不构造 LazyProducer,pusher 保持 nil,发布器照旧
// 退出。LazyProducer 只覆盖「配了 brokers 但此刻连不上」这一档,这也正是原来会永久卡死的那档。
//
// **先例**:services/economy/auction/cmd/auction/main.go 的 auctionEventPusher.matchProducer
// 已经用同一手法(双检 + 独立拨号锁),本类型把它提取出来共用(§15.5「抽象必须消除真实重复」)。
// auction 侧维持原状不动:它的成交事件另有 MySQL 行标志出箱补偿,与本次要修的缺陷不是一回事,
// 顺手迁移会把两件事的验证搅在一起。
package kafkax

import (
	"context"
	"fmt"
	"sync"

	"github.com/luyuancpp/pandora/pkg/config"
)

// LazyProducer 是 KeyOrderedProducer 的惰性构造包装:首次投递时才拨号,失败可重试。
//
// 并发安全。方法集刻意只覆盖当前真实调用点(SendRaw / SendRawWithEventType / Close),
// 不预铺 Send / Stats 等尚无调用方的转发(§15.3「拒绝预设性复杂化」)。
type LazyProducer struct {
	cfg   config.KafkaConfig
	topic string

	// mu 只保护字段读写,**绝不持它做网络 IO**:拨号可能长时间阻塞,
	// 持状态锁拨号会让所有调用方一起卡住。
	mu     sync.Mutex
	p      *KeyOrderedProducer
	closed bool

	// dialMu 串行化拨号,避免首次投递并发时打出一串重复的 sarama 客户端。
	dialMu sync.Mutex
}

// NewLazyProducer 构造惰性生产者。**不做任何网络 IO**,故不返回 error ——
// 装配期不可能因 broker 不可达而失败,这正是本类型存在的意义。
func NewLazyProducer(cfg config.KafkaConfig, topic string) *LazyProducer {
	return &LazyProducer{cfg: cfg, topic: topic}
}

// Topic 返回本生产者绑定的 topic(装配期打日志用)。
func (l *LazyProducer) Topic() string { return l.topic }

// SendRaw 见 KeyOrderedProducer.SendRaw;producer 未就绪时先按需构造。
func (l *LazyProducer) SendRaw(ctx context.Context, key string, payload []byte) error {
	p, err := l.producer()
	if err != nil {
		return err
	}
	return p.SendRaw(ctx, key, payload)
}

// SendRawWithEventType 见 KeyOrderedProducer.SendRawWithEventType;producer 未就绪时先按需构造。
func (l *LazyProducer) SendRawWithEventType(ctx context.Context, key string, payload []byte, eventType uint32) error {
	p, err := l.producer()
	if err != nil {
		return err
	}
	return p.SendRawWithEventType(ctx, key, payload, eventType)
}

// producer 返回已就绪的底层生产者,必要时拨号构造。
//
// 双检结构与 auction 的 matchProducer 逐字同构:先在 mu 下快路径读,未就绪则放开 mu、
// 取 dialMu 串行拨号,拨号成功后再取 mu 写回。拨号失败**不缓存失败态** —— 下一次投递
// 会再试一次,这正是「Kafka 恢复即自愈」的来源。
func (l *LazyProducer) producer() (*KeyOrderedProducer, error) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, fmt.Errorf("kafkax: lazy producer closed (topic=%s)", l.topic)
	}
	if l.p != nil {
		p := l.p
		l.mu.Unlock()
		return p, nil
	}
	l.mu.Unlock()

	l.dialMu.Lock()
	defer l.dialMu.Unlock()

	// 进 dialMu 后重新确认:等锁期间可能已被别的调用方构造好或已关闭。
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, fmt.Errorf("kafkax: lazy producer closed (topic=%s)", l.topic)
	}
	if l.p != nil {
		p := l.p
		l.mu.Unlock()
		return p, nil
	}
	l.mu.Unlock()

	p, err := NewKeyOrderedProducer(l.cfg, l.topic)
	if err != nil {
		return nil, fmt.Errorf("kafkax: connect producer (topic=%s): %w", l.topic, err)
	}

	l.mu.Lock()
	if l.closed {
		// 拨号期间被 Close 了:把刚建好的客户端还回去,不能泄漏。
		l.mu.Unlock()
		_ = p.Close()
		return nil, fmt.Errorf("kafkax: lazy producer closed (topic=%s)", l.topic)
	}
	l.p = p
	l.mu.Unlock()
	return p, nil
}

// Close 关闭底层生产者(若已构造)。关闭后所有投递永久返错,不会再拨号。
// 未曾构造过时是无害的 no-op —— 装配期 defer Close 可以无条件写。
func (l *LazyProducer) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	p := l.p
	l.p = nil
	l.mu.Unlock()

	if p == nil {
		return nil
	}
	return p.Close()
}
