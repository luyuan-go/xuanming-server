package kafkax

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
)

// unreachableCfg 指向一个保证连不上的 broker。
//
// 刻意用 127.0.0.1:1 而不是 RFC 5737 的黑洞地址(如 192.0.2.1):黑洞地址不回 RST,
// 每次拨号都要等满 DialTimeout,再乘上 sarama 的 Metadata.Retry,整组用例会拖到几十秒。
// 回环 1 端口立刻回 ECONNREFUSED,用例既快又不依赖任何外部网络状态。
func unreachableCfg() config.KafkaConfig {
	return config.KafkaConfig{
		Brokers:      []string{"127.0.0.1:1"},
		DialTimeout:  config.Duration(200 * time.Millisecond),
		WriteTimeout: config.Duration(200 * time.Millisecond),
	}
}

// TestLazyProducerDoesNotDialOnConstruction 钉住本类型存在的**唯一理由**:
// 装配期不做网络 IO。这条一旦回归(比如有人"顺手"在 NewLazyProducer 里预热连接),
// 就又会退回「启动时 Kafka 不可用 → pusher 恒 nil → 发布器不起 goroutine → 必须重启」
// 那个静默不可自愈的老缺陷。
func TestLazyProducerDoesNotDialOnConstruction(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		lp := NewLazyProducer(unreachableCfg(), TopicPlayerExperience)
		if lp == nil {
			t.Error("NewLazyProducer 返回 nil")
			return
		}
		if got := lp.Topic(); got != TopicPlayerExperience {
			t.Errorf("Topic() = %q, want %q", got, TopicPlayerExperience)
		}
	}()

	// 构造若真去拨号,会被 DialTimeout 卡住;这里给足余量仍应瞬时返回。
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("NewLazyProducer 阻塞了 —— 说明构造期在拨号,违反惰性契约")
	}
}

// TestLazyProducerSendRetriesAfterFailure 钉住「失败不缓存、下次还能再试」:
// 这是「Kafka 恢复即自愈」的来源。若实现把首次拨号失败记成永久态,出箱就再也排不空。
func TestLazyProducerSendRetriesAfterFailure(t *testing.T) {
	lp := NewLazyProducer(unreachableCfg(), TopicMissionUpdate)
	defer func() { _ = lp.Close() }()

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		err := lp.SendRaw(ctx, "1001", []byte("payload"))
		if err == nil {
			t.Fatalf("第 %d 次 SendRaw 应因 broker 不可达而失败", i+1)
		}
		if !strings.Contains(err.Error(), "connect producer") {
			t.Fatalf("第 %d 次 SendRaw 的错误应指明是连接失败,实际: %v", i+1, err)
		}
	}
}

// TestLazyProducerSendRawWithEventTypeFailsCleanly 覆盖 player 经验推送走的那条方法,
// 确认它与 SendRaw 同样把连接失败当可重试错误返回,而不是 panic 或静默成功。
func TestLazyProducerSendRawWithEventTypeFailsCleanly(t *testing.T) {
	lp := NewLazyProducer(unreachableCfg(), TopicPlayerExperience)
	defer func() { _ = lp.Close() }()

	if err := lp.SendRawWithEventType(context.Background(), "1001", []byte("payload"), 1); err == nil {
		t.Fatal("SendRawWithEventType 应因 broker 不可达而失败")
	}
}

// TestLazyProducerCloseWithoutDialIsNoop 装配期 defer Close 是无条件写的,
// 从未构造过底层 producer 时必须是无害 no-op(否则会在正常停机路径上报假错)。
func TestLazyProducerCloseWithoutDialIsNoop(t *testing.T) {
	lp := NewLazyProducer(unreachableCfg(), TopicTeamUpdate)
	if err := lp.Close(); err != nil {
		t.Fatalf("未拨号时 Close 应为 no-op,实际返回: %v", err)
	}
	// 幂等:重复 Close 不报错。
	if err := lp.Close(); err != nil {
		t.Fatalf("重复 Close 应幂等,实际返回: %v", err)
	}
	// 关闭后投递必须永久失败,且不再拨号。
	if err := lp.SendRaw(context.Background(), "1", []byte("x")); err == nil {
		t.Fatal("Close 之后 SendRaw 应返错")
	}
}

// TestLazyProducerConcurrentSendDoesNotRace 并发首投不应打出一串重复客户端,也不应 data race
// (跑 -race 时有效)。broker 不可达,故这里只验证并发路径不炸、错误正常返回。
func TestLazyProducerConcurrentSendDoesNotRace(t *testing.T) {
	lp := NewLazyProducer(unreachableCfg(), TopicChatPrivate)
	defer func() { _ = lp.Close() }()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = lp.SendRaw(context.Background(), "1001", []byte("payload"))
		}()
	}
	wg.Wait()
}
