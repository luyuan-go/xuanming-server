// Package kafkax —— Kafka 侧 prometheus 指标(2026-08-24)。
//
// 补这一组的原因:此前全仓 **0 个 Kafka 指标**。producer 只有进程内的
// KeyOrderedProducer.Stats()(全仓无业务调用方),consumer 的重试 / 毒丸 / DLQ 全部只落
// printf 风格日志,Grafana 里一条 Kafka 规则都没有。后果是「Kafka 半死不活」这一档完全
// 不可观测:broker 不可达时业务侧只表现为「推送没到」,而出箱堆积、DLQ 投递、消费空转
// 都查不到数,只能靠人翻日志。docs/design/infra.md §10 早就把 pandora_kafka_consumer_lag
// 举成示例指标,但代码里一直不存在。
//
// 命名规范(docs/design/infra.md §10):pandora_kafka_<metric>{<labels>}
// 强制 label service / instance 由抓取端加,代码不写。
//
// **基数**:topic ≤ 22(pkg/kafkax/topics.go 18 个常量 + 4 个 config.BuildTopic 现拼)、
// group 4 个、partition 现网 4 个、result/reason 为有限枚举 —— 全部低基数。
// 禁止把 player_id / match_id 放进 label(§10 硬规)。
package kafkax

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/luyuancpp/pandora/pkg/metrics"
)

// produce 结果枚举(ProduceTotal 的 result label)。
const (
	produceResultOK    = "ok"
	produceResultError = "error"
)

// consume 结果枚举(ConsumeTotal 的 result label)。语义与 package 文档的失败处理三档对齐。
const (
	consumeResultOK        = "ok"          // handler 返 nil → ack
	consumeResultPoison    = "poison"      // handler 返 PoisonError → 跳过重试直投 DLQ
	consumeResultRetry     = "retry"       // 瞬时错,进程内重试一次(每次重试各计一次)
	consumeResultExhausted = "exhausted"   // 重试耗尽 → 转 DLQ
	consumeResultDLQDrop   = "dlq_dropped" // 未配 DLQ 的 loss-tolerant 消费者:log 后 ack 丢弃
)

// ProduceTotal 统计每个 topic 的投递结果。
//
// result=error 的常见来源:broker 不可达、分区 leader 选举中、消息超过 max.message.bytes。
// 业务影响按 topic 分档:出箱型(player.update / player.experience / mission.update /
// auction.match)失败只是延迟,下轮重投不丢;best-effort 型(chat.* / *.audit)失败即永久丢事件。
// 告警建议:出箱型 rate>0 持续 5m 即告警;best-effort 型看比率。
var ProduceTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "pandora_kafka_produce_total",
		Help: "kafka 投递总次数,按 topic 与结果(ok/error)分。",
	},
	[]string{"topic", "result"},
)

// ConsumeTotal 统计每个消费组的消息处理结果。
//
// 除 ok 外任何一档 rate>0 都值得看:poison 表示解码/毒丸(通常是双端协议漂移),
// exhausted 表示下游持续不可用,dlq_dropped 表示消息被**真丢**了(仅 loss-tolerant 消费者)。
var ConsumeTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "pandora_kafka_consume_total",
		Help: "kafka 消息处理总次数,按 topic / 消费组 / 结果分。",
	},
	[]string{"topic", "group", "result"},
)

// DLQTotal 统计投进死信队列的消息数。
//
// ⚠️ 全仓**没有任何 DLQ 消费者 / 回放器**,infra.md §4.4 承诺的「人工介入」目前没有任何
// 触发机制 —— 这个指标是 DLQ 唯一的发现手段,rate>0 就必须有人去看 DLQ topic。
// result=failed 时消息**未被 ack**,会在 rejoin 后重放(at-least-once 的兜底路径)。
var DLQTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "pandora_kafka_dlq_total",
		Help: "投递到 DLQ 的消息数,按原 topic / 消费组 / 结果(ok/failed)分。",
	},
	[]string{"topic", "group", "result"},
)

// ConsumerLag 是分区级消费滞后(高水位 - 当前 offset - 1),在每条消息处理后刷新。
//
// 这是 docs/design/infra.md §10 点名的指标名。取值来自 sarama 的
// ConsumerGroupClaim.HighWaterMarkOffset(),无额外 broker 请求,故可逐条刷新。
//
// **边界**:空闲分区不会刷新(没有消息就没有回调),值会停在最后一次观测。判「消费卡住」
// 要配合 ConsumeTotal 的 rate 一起看,不能只看本指标。
var ConsumerLag = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "pandora_kafka_consumer_lag",
		Help: "kafka 分区消费滞后条数(高水位 - 已处理 offset - 1),按 topic / 消费组 / 分区分。",
	},
	[]string{"topic", "group", "partition"},
)

// ConsumeLoopErrorTotal 统计消费主循环 Consume() 的返错次数。
//
// 这条与 ConsumeTotal 是两回事:ConsumeTotal 数的是「拿到消息之后」,本指标数的是
// **根本没拿到消息**——broker 不可达、rebalance 失败、协调者不可用。broker 自杀时
// (免Docker Windows 链已有 8 类)本指标会持续飙升,而 ConsumeTotal 完全静止,这个组合
// 是「Kafka 死了」与「业务处理失败」最快的区分判据。
var ConsumeLoopErrorTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "pandora_kafka_consume_loop_error_total",
		Help: "消费主循环 Consume() 返错次数(broker 不可达 / rebalance 失败),按 topic / 消费组分。",
	},
	[]string{"topic", "group"},
)

func init() {
	metrics.Register(ProduceTotal)
	metrics.Register(ConsumeTotal)
	metrics.Register(DLQTotal)
	metrics.Register(ConsumerLag)
	metrics.Register(ConsumeLoopErrorTotal)
}
