// Package config 提供 Pandora 服务的通用配置结构。
//
// 设计:
//   - 基础字段(Server/Node/Redis/Kafka/Snowflake/Locker/Registry/Timeouts)集中放 Base
//   - 各服务的 internal/conf/conf.go 嵌入 Base 并加业务字段
//   - 配置加载用 Kratos config(W2 file source,W3+ 接 etcd)
//
// 跟之前 go-zero 版本的区别(2026-06-04 重写):
//   - 删 zrpc.RpcServerConf 嵌入,改 Pandora 自定义 Server 结构
//   - go-zero LogConf 改 zap(详见 pkg/log/log.go)
//   - 字段保留 mmorpg 拷过来的语义,但风格按 Kratos 惯例(yaml + protobuf 都能映射)
//
// 文件加载示例(各服务 main.go):
//
//	c := kconfig.New(kconfig.WithSource(file.NewSource("./etc/login-dev.yaml")))
//	if err := c.Load(); err != nil { panic(err) }
//	var cfg config.Base
//	if err := c.Scan(&cfg); err != nil { panic(err) }
package config

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"

	"github.com/luyuancpp/pandora/pkg/cellroute"
)

// Base 是所有 Pandora 服务的通用配置基类。
//
// 各业务服务 internal/conf/conf.go 模板:
//
//	type Config struct {
//	    config.Base `yaml:",inline"`                       // 公共
//	    BusinessKnob int    `yaml:"business_knob" json:"business_knob"` // 业务私有
//	}
type Base struct {
	// Server 监听配置(gRPC + HTTP)
	Server Server `yaml:"server" json:"server"`

	// Node 节点级配置(redis 客户端、session 超时等)
	Node NodeConfig `yaml:"node" json:"node"`

	// Snowflake 全局 ID 生成参数
	Snowflake SnowflakeConf `yaml:"snowflake,omitempty" json:"snowflake,omitempty"`

	// Locker 分布式锁默认 TTL
	Locker LockerConf `yaml:"locker,omitempty" json:"locker,omitempty"`

	// Registry 服务注册发现(W2 用 file 配置,W3+ 接 etcd)
	Registry RegistryConf `yaml:"registry,omitempty" json:"registry,omitempty"`

	// Timeouts 各种通用超时
	Timeouts TimeoutConf `yaml:"timeouts,omitempty" json:"timeouts,omitempty"`

	// Kafka 生产者/消费者通用配置
	Kafka KafkaConfig `yaml:"kafka,omitempty" json:"kafka,omitempty"`

	// KillSwitch RPC 级临时关停(Kill-Switch)
	KillSwitch KillSwitchConf `yaml:"killswitch,omitempty" json:"killswitch,omitempty"`

	// CellRoute 蜂窝扩容部署拓扑(scale-cellular-20m.md):mode 空=单 Cell 不路由,
	// static/etcd=多 Cell。各服务 main 用 cellroute.BuildRouter(cfg.CellRoute) 装配。
	CellRoute cellroute.RouterConfig `yaml:"cell_route,omitempty" json:"cell_route,omitempty"`

	// ConfigTable 策划配置表加载(pkg/configtable,不变量 §9.15 标准热更流水线)。
	// dir 非空 = 启用:启动强依赖加载该目录(active 批次,含 manifest.json),失败进程退出
	// (fail-closed);热更经 ConfigTableAdminService.ReloadConfigTable 重读同一目录,
	// 失败保留旧表。dir 为空 = 不启用,行为与未接配置表前完全一致。
	ConfigTable ConfigTableConf `yaml:"config_table,omitempty" json:"config_table,omitempty"`

	// SessionGate 客户端面会话现行性门(R5 复审 P0-1,INC-20260722-004):
	// 所有经 Envoy :8443 jwt_authn 的玩家 RPC 必须校验 payload jti == login 会话权威
	// (pandora:sess)当前一代,顶号后旧 JWT 立即失去全部按 player_id 定向能力。
	// 权威端点复用 node.redis_client;require 由 prod 生成器机械置 true。
	// 各服务 main 用 sessiongate.MustBuild + pmw.SessionCurrent 装配。
	SessionGate SessionGateConf `yaml:"session_gate,omitempty" json:"session_gate,omitempty"`
}

// SessionGateConf 会话现行性门参数(pkg/sessiongate + pkg/middleware.SessionCurrent)。
type SessionGateConf struct {
	// Require true = 强制档(prod):权威端点漏配拒启;gate 未装配时携带会话证据的请求
	// 一律 fail-closed 拒绝。false = dev 宽松档(无 Redis 时直连联调可跳过现行性判定;
	// 但 gate 已装配时无论档位,权威查询失败/顶号/登出都照常拒绝)。
	Require bool `yaml:"require,omitempty" json:"require,omitempty"`

	// RequireTicketSJTI 仅 hub_allocator 使用(R7 收口,滚动发布分阶段激活):
	// true = AcknowledgeAdmission 对票据缺 sjti(会话绑定)硬拒;false(默认)= 兼容档,
	// 空 sjti 告警放行(跳过现行性判定,行为与旧 DS/旧票一致),非空 sjti 仍全量复核。
	// 激活前提(顺序硬约束):全 fleet Hub DS 已升级到会转发 sjti 的版本、旧 DS 已排空,
	// 且等满一个票据最大 TTL(旧签发面残票自然过期)后再置 true。提前开启 = 旧 DS 上
	// 所有玩家无法进入大厅。
	RequireTicketSJTI bool `yaml:"require_ticket_sjti,omitempty" json:"require_ticket_sjti,omitempty"`
}

// ConfigTableConf 配置表加载参数。
type ConfigTableConf struct {
	// Dir active 批次目录(含 manifest.json + 各表 json),见 docs/design/config-table-hotreload.md §4。
	Dir string `yaml:"dir,omitempty" json:"dir,omitempty"`
}

// Server Kratos 风格的 server 监听配置(替代 go-zero zrpc.RpcServerConf)。
type Server struct {
	Grpc Grpc `yaml:"grpc" json:"grpc"`
	Http Http `yaml:"http,omitempty" json:"http,omitempty"` // 可选,W2 大部分服务只暴露 gRPC
}

// Grpc gRPC server 监听。
//
// EnableReflection(W3 ③,2026-06-05):
//
//   - true:保留 Kratos 默认的 grpc.reflection 注册(grpcurl list 可用,便于联调)
//
//   - false(默认):pkg/grpcserver.MustNewServer 会加 kgrpc.DisableReflection() 关掉
//
//     prod 默认不写本字段(零值 false)= 关 reflection,避免攻击面额外暴露。
//     dev yaml 显式写 enable_reflection: true 打开。
type Grpc struct {
	Network          string   `yaml:"network,omitempty" json:"network,omitempty"`                     // 默认 "tcp"
	Addr             string   `yaml:"addr" json:"addr"`                                               // 例 ":20001"
	Timeout          Duration `yaml:"timeout,omitempty" json:"timeout,omitempty"`                     // 默认 1s
	EnableReflection bool     `yaml:"enable_reflection,omitempty" json:"enable_reflection,omitempty"` // dev:true; prod:false(默认)
	EnableRateLimit  bool     `yaml:"enable_rate_limit,omitempty" json:"enable_rate_limit,omitempty"` // 第4层 BBR 自适应限流;dev:false; prod:true
	// MaxConnAge > 0 时开启 gRPC 服务端连接轮换(keepalive MaxConnectionAge):
	// 长连接达龄后优雅 GOAWAY,客户端重拨自然滚到新副本 —— 滚动更新/扩容时流量能
	// 切到新 Pod的关键兜底(zero-downtime-update.md §6.2)。0(不写)= 关,行为不变。
	MaxConnAge      Duration `yaml:"max_conn_age,omitempty" json:"max_conn_age,omitempty"`             // 建议 15m
	MaxConnAgeGrace Duration `yaml:"max_conn_age_grace,omitempty" json:"max_conn_age_grace,omitempty"` // GOAWAY 后在途 RPC 排空宽限;不写默认 30s
}

// Http HTTP server 监听(给 protoc-gen-go-http 生成的 handler 用)。
type Http struct {
	Network string   `yaml:"network,omitempty" json:"network,omitempty"`
	Addr    string   `yaml:"addr" json:"addr"` // 例 ":21001"
	Timeout Duration `yaml:"timeout,omitempty" json:"timeout,omitempty"`
}

// NodeConfig 节点级配置。
type NodeConfig struct {
	// NodeId 是 snowflake 发号器的 node 段(机器编号),不是玩家选区。
	// 同一服务类型的多个副本必须各自唯一(否则发重号,违反 CLAUDE.md §9 不变量 11);
	// 不同服务类型之间可复用同一值。单副本 / dev 默认填 1;多副本走 snowflake.node_id_source=etcd 自动分配。
	NodeId           uint32    `yaml:"node_id" json:"node_id"`
	SessionExpireMin uint32    `yaml:"session_expire_min,omitempty" json:"session_expire_min,omitempty"` // 默认 1440 (24h)
	RedisClient      RedisConf `yaml:"redis_client" json:"redis_client"`
	MySQLClient      MySQLConf `yaml:"mysql_client,omitempty" json:"mysql_client,omitempty"`             // W3 ② 起接 mysql 的服务用
	LeaseTTL         int64     `yaml:"lease_ttl,omitempty" json:"lease_ttl,omitempty"`                   // 秒,默认 10
	MaxLoginDuration Duration  `yaml:"max_login_duration,omitempty" json:"max_login_duration,omitempty"` // 默认 24h
	LogoutGraceTime  Duration  `yaml:"logout_grace_time,omitempty" json:"logout_grace_time,omitempty"`   // 默认 5m
}

// MySQLConf MySQL 客户端配置(W3 ②,2026-06-05)。
//
// DSN 示例(login 服务连 pandora_account 库):
//
//	pandora:pandora_dev_pwd@tcp(127.0.0.1:3307)/pandora_account?parseTime=true&loc=UTC&charset=utf8mb4&collation=utf8mb4_0900_ai_ci
//
// W3 ⑥(2026-06-05):duration 字段改用 config.Duration 包装类型,yaml 可写 "30m"/"3s" 字符串。
type MySQLConf struct {
	DSN string `yaml:"dsn" json:"dsn"`
	// TLSCAFile / TLSServerName 必须成对配置。留空保持本地开发明文连接行为；
	// 中心 MySQL 配置时由 mysqlx 用系统根证书 + 本文件 CA 做严格主机名校验。
	TLSCAFile       string   `yaml:"tls_ca_file,omitempty" json:"tls_ca_file,omitempty"`
	TLSServerName   string   `yaml:"tls_server_name,omitempty" json:"tls_server_name,omitempty"`
	MaxOpenConns    int      `yaml:"max_open_conns,omitempty" json:"max_open_conns,omitempty"`
	MaxIdleConns    int      `yaml:"max_idle_conns,omitempty" json:"max_idle_conns,omitempty"`
	ConnMaxLifetime Duration `yaml:"conn_max_lifetime,omitempty" json:"conn_max_lifetime,omitempty"`
	// ConnMaxIdleTime 空闲连接最长存活;留空 = 0(database/sql 默认:不因空闲回收,只受
	// ConnMaxLifetime 限制)。压测前审核门禁-B:高并发登录后回落期给空闲连接设上限,
	// 及时释放池占用、避免长时间持有对端已关的陈旧连接。opt-in,默认不改变现有行为。
	ConnMaxIdleTime Duration `yaml:"conn_max_idle_time,omitempty" json:"conn_max_idle_time,omitempty"`
	PingTimeout     Duration `yaml:"ping_timeout,omitempty" json:"ping_timeout,omitempty"`

	// Shards 是分库 DSN 列表。留空 = 单库(用 DSN)。配置 >=2 个 DSN = 分库模式,
	// 由 mysqlx.NewShardSet 按 snowflake 业务 ID 路由(shard = id % len(Shards))。
	// DAU 200万 / 千万注册量级下,单 MySQL 实例的写吞吐与单表行数都会触顶,按 player_id
	// 水平分库分表;分片数一旦定稿不可随意改(rehash 代价高),详见 docs/design/scale-cellular-20m.md。
	// 池参数(MaxOpenConns 等)每个分片各自套用本结构的同名字段。
	Shards []string `yaml:"shards,omitempty" json:"shards,omitempty"`
}

// RedisConf Redis 客户端配置。
//
// W3 ⑥(2026-06-05):duration 字段改用 config.Duration,yaml 可写 "2s"/"30s" 字符串。
type RedisConf struct {
	Host         string   `yaml:"host" json:"host"`
	Password     string   `yaml:"password,omitempty" json:"password,omitempty"`
	DB           uint32   `yaml:"db,omitempty" json:"db,omitempty"`
	DefaultTTL   Duration `yaml:"default_ttl,omitempty" json:"default_ttl,omitempty"`
	DialTimeout  Duration `yaml:"dial_timeout,omitempty" json:"dial_timeout,omitempty"`
	ReadTimeout  Duration `yaml:"read_timeout,omitempty" json:"read_timeout,omitempty"`
	WriteTimeout Duration `yaml:"write_timeout,omitempty" json:"write_timeout,omitempty"`

	// 连接池参数(压测前审核门禁-C:让运维可按核数/并发调优,消除 Redis 抖动下池耗尽拖垮
	// 登录的盲区)。全部留空 = 0 → 沿用 go-redis 默认(PoolSize=10×GOMAXPROCS、
	// PoolTimeout=ReadTimeout+1s、MinIdleConns=0),不改变现有行为;opt-in。
	// login/session/locator 等 Redis 强依赖的在线关键服务应在 yaml 显式设定,并配 read/write_timeout。
	PoolSize     int      `yaml:"pool_size,omitempty" json:"pool_size,omitempty"`
	MinIdleConns int      `yaml:"min_idle_conns,omitempty" json:"min_idle_conns,omitempty"`
	PoolTimeout  Duration `yaml:"pool_timeout,omitempty" json:"pool_timeout,omitempty"`

	// Addrs 是 Redis Cluster / Sentinel 多节点地址。
	//
	// 留空 = 单实例模式(用 Host)。MasterName 为空时,Addrs 只有 1 个仍是 standalone,
	// Addrs >1 才构造 ClusterClient(go-redis UniversalClient 按原始地址数量选型)。
	// DAU 200万 / 高 CCU 阶段单 Redis 必然成为吞吐与连接数单点,改配 Addrs 上 Redis Cluster,
	// 分片键用业务 ID(player_id / team_id);详见 docs/design/scale-cellular-20m.md。
	Addrs []string `yaml:"addrs,omitempty" json:"addrs,omitempty"`

	// MasterName 非空 = 走 Sentinel 故障转移(FailoverClient);为空且 Addrs 多节点 = Cluster。
	MasterName string `yaml:"master_name,omitempty" json:"master_name,omitempty"`

	// MaintNotifications 控制 go-redis 的 CLIENT MAINT_NOTIFICATIONS 能力探测。
	//
	// 取值:"disabled" / "auto" / "enabled";留空 = "disabled"(项目默认)。
	// 自建 Redis(本地 / k8s 内 Redis 7.x)不支持该云厂商维护通知,默认关闭探测,
	// 避免 go-redis 启动时打印 "maintnotifications disabled due to handshake error" 噪音日志。
	// 仅当接 Redis Cloud / Enterprise 需要无缝故障转移时,才显式设为 "auto" / "enabled"。
	// 由 pkg/redisx.NewClient 解析,非法值安全回退到 disabled。
	MaintNotifications string `yaml:"maint_notifications,omitempty" json:"maint_notifications,omitempty"`
}

// KafkaConfig Kafka 生产/消费通用配置。
//
// W3 ⑥(2026-06-05):duration 字段改用 config.Duration,yaml 可写 "5s"/"100ms" 字符串。
type KafkaConfig struct {
	Brokers          []string `yaml:"brokers" json:"brokers"`
	GroupID          string   `yaml:"group_id,omitempty" json:"group_id,omitempty"`
	PartitionCnt     int32    `yaml:"partition_cnt,omitempty" json:"partition_cnt,omitempty"`         // 默认 4
	InitialPartition int      `yaml:"initial_partition,omitempty" json:"initial_partition,omitempty"` // 默认 4
	DialTimeout      Duration `yaml:"dial_timeout,omitempty" json:"dial_timeout,omitempty"`
	ReadTimeout      Duration `yaml:"read_timeout,omitempty" json:"read_timeout,omitempty"`
	WriteTimeout     Duration `yaml:"write_timeout,omitempty" json:"write_timeout,omitempty"`
	RetryMax         int      `yaml:"retry_max,omitempty" json:"retry_max,omitempty"`
	RetryBackoff     Duration `yaml:"retry_backoff,omitempty" json:"retry_backoff,omitempty"`
	ChannelBuffer    int      `yaml:"channel_buffer,omitempty" json:"channel_buffer,omitempty"`
	SyncInterval     Duration `yaml:"sync_interval,omitempty" json:"sync_interval,omitempty"`
	StatsInterval    Duration `yaml:"stats_interval,omitempty" json:"stats_interval,omitempty"`
	// CompressionType: "none" | "gzip" | "snappy" | "lz4" | "zstd"(默认 none)
	// 用 string 比 int 更人类可读,内部用 ParseCompression 转换。
	CompressionType string `yaml:"compression_type,omitempty" json:"compression_type,omitempty"`
	Idempotent      bool   `yaml:"idempotent,omitempty" json:"idempotent,omitempty"`               // 默认 true
	MaxOpenRequests int    `yaml:"max_open_requests,omitempty" json:"max_open_requests,omitempty"` // idempotent=true 时必须为 1
	RetentionMs     int64  `yaml:"retention_ms,omitempty" json:"retention_ms,omitempty"`           // 默认 7 天
}

// ParseCompression 把 yaml 里的字符串转成 sarama 类型。
// 不识别的值返回 sarama.CompressionNone(不报错,日志由调用方打)。
func (k KafkaConfig) ParseCompression() sarama.CompressionCodec {
	switch k.CompressionType {
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	case "lz4":
		return sarama.CompressionLZ4
	case "zstd":
		return sarama.CompressionZSTD
	case "", "none":
		return sarama.CompressionNone
	default:
		return sarama.CompressionNone
	}
}

// SnowflakeConf 雪花算法参数。
//
// ⚠️ 位布局与 Epoch 是 pkg/snowflake 的**编译期常量**([time:32][node:17][step:15],
// Epoch=1781161165),不是配置项——历史上这里曾有 epoch/node_bits/step_bits 字段,
// 但从未被任何代码消费,且 yaml 里的旧值(1773446400)与代码常量早已漂移;为防有人
// 改 yaml 以为能生效(改 Epoch 会让新旧 ID 时间段错位、批量重号),已整体删除。
// 真要改布局只能改 pkg/snowflake 常量并全量重编译,且上线后不可再改(历史 ID 已定型)。
type SnowflakeConf struct {
	// NodeIDSource 决定 snowflake nodeID 来源:
	//   - ""/"static":用 node.node_id 静态分配(单副本 / dev 默认)。
	//   - "etcd":进入 k8s 多副本动态扩缩阶段,用 etcd Lease 自动抢占 nodeID(同服务内唯一、跨服务可复用)。
	//
	// 接线只需一行(static / etcd 两态由本字段驱动,fencing 退出已内置):
	//	sf, sfCloser, err := etcdnode.ProvideSnowflake(ctx, serviceName, cfg.Node.NodeId, cfg.Snowflake)
	// static 服务不引入 etcd 依赖;改 etcd 时该服务 go.mod 需 Codex 补 etcdnode require + go mod tidy。
	// 详见 docs/design/infra.md §8.1 与 docs/design/scale-cellular-20m.md。
	NodeIDSource string `yaml:"node_id_source,omitempty" json:"node_id_source,omitempty"`

	// Etcd* 给 NodeIDSource="etcd" 用。
	EtcdEndpoints   []string `yaml:"etcd_endpoints,omitempty" json:"etcd_endpoints,omitempty"`
	EtcdPrefix      string   `yaml:"etcd_prefix,omitempty" json:"etcd_prefix,omitempty"`               // 默认 "/pandora/snowflake/node/"
	EtcdServiceName string   `yaml:"etcd_service_name,omitempty" json:"etcd_service_name,omitempty"`   // 留空用服务名;隔离各服务 nodeID 空间
	EtcdLeaseTTLSec int64    `yaml:"etcd_lease_ttl_sec,omitempty" json:"etcd_lease_ttl_sec,omitempty"` // 默认 15
}

// LockerConf 分布式锁默认 TTL。
type LockerConf struct {
	AccountLockTTL uint32 `yaml:"account_lock_ttl,omitempty" json:"account_lock_ttl,omitempty"` // 秒,默认 10
	PlayerLockTTL  uint32 `yaml:"player_lock_ttl,omitempty" json:"player_lock_ttl,omitempty"`
}

// RegistryConf 服务注册发现。
type RegistryConf struct {
	Etcd EtcdRegistryConf `yaml:"etcd,omitempty" json:"etcd,omitempty"`
}

// KillSwitchConf RPC 级临时关停(Kill-Switch)配置。
//
// 出现重大问题想临时关某个 service / RPC、修好再开,不发版不重启、秒级热生效。
// 由 pkg/svc.BaseContext 在装配时翻译成 killswitch.Config 并启动开关源,
// pkg/middleware.KillSwitch() 在 gRPC server 链上拦截命中规则的 RPC。
type KillSwitchConf struct {
	// Enabled 为 false 时不启用 Kill-Switch(全放行)。
	Enabled bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`

	// Source 开关源:"file"(dev 默认,改 yaml 即生效)/ "etcd"(prod,集中多实例一致)。
	// etcd 源需服务在 main 里 blank import pkg/killswitch/etcdkv。
	Source string `yaml:"source,omitempty" json:"source,omitempty"`

	// FilePath file 源监听的 yaml(默认 "etc/killswitch.yaml")。
	FilePath string `yaml:"file_path,omitempty" json:"file_path,omitempty"`

	// Etcd* 给 etcd 源用。
	EtcdEndpoints   []string `yaml:"etcd_endpoints,omitempty" json:"etcd_endpoints,omitempty"`
	EtcdPrefix      string   `yaml:"etcd_prefix,omitempty" json:"etcd_prefix,omitempty"` // 默认 "/pandora/killswitch/"
	EtcdDialTimeout Duration `yaml:"etcd_dial_timeout,omitempty" json:"etcd_dial_timeout,omitempty"`

	// FailClosed 控制源构造失败时的行为。
	// 零值 false = fail-open(放行,Kill-Switch 自身故障绝不拖垮服务,推荐默认)。
	// true = fail-closed(源建不起来则 main fatal,仅在你要求"开关系统必须在线"时用)。
	FailClosed bool `yaml:"fail_closed,omitempty" json:"fail_closed,omitempty"`
}

// EtcdRegistryConf etcd 注册中心(W3+ 接入)。
type EtcdRegistryConf struct {
	Hosts       []string `yaml:"hosts" json:"hosts"`
	Key         string   `yaml:"key,omitempty" json:"key,omitempty"`                   // service path,默认按服务名构造
	DialTimeout Duration `yaml:"dial_timeout,omitempty" json:"dial_timeout,omitempty"` // 默认 5s
}

// TimeoutConf 各种公共超时。
type TimeoutConf struct {
	EtcdDialTimeout         Duration `yaml:"etcd_dial_timeout,omitempty" json:"etcd_dial_timeout,omitempty"`
	ServiceDiscoveryTimeout Duration `yaml:"service_discovery_timeout,omitempty" json:"service_discovery_timeout,omitempty"`
	TaskWaitTimeout         Duration `yaml:"task_wait_timeout,omitempty" json:"task_wait_timeout,omitempty"`
	RoleCacheExpire         Duration `yaml:"role_cache_expire,omitempty" json:"role_cache_expire,omitempty"`
}

// DSAuthConf 是「DS→后端回调服务令牌」配置(审核 P1 #1,2026-07-10 拍板落地)。
//
// 背景:DS 面网关 :8444 只有方法白名单 + 网络隔离,回调方法(Heartbeat / ReportResult /
// SetLocation / PollCommands …)此前不认证调用者身份。本机制由 ds_allocator / hub_allocator
// 在分配 / 发现 DS 时签发短期 JWT 服务令牌(aud=pandora-ds,绑定 match_id / pod),经
// GameServer annotation(Agones)或 PANDORA_DS_TOKEN env(local)下发给 DS;DS 回调时带
// `authorization: Bearer <token>`,四个被回调服务(ds_allocator / hub_allocator /
// player_locator / battle_result)按 Mode 校验并做范围绑定(详见
// docs/design/decision-revisit-ds-callback-auth.md)。
//
// 四个服务共用本结构:签发侧(两个 allocator)用到 TTL 字段,校验侧只用 Mode/Issuer/Audience/Secret。
type DSAuthConf struct {
	// Mode 校验模式(灰度开关,CLAUDE.md §14:默认关不破坏现有行为,开启分支是完整实现):
	//   ""/"off"      → 不校验(默认;UE DS 侧尚未携带令牌前必须保持 off)
	//   "permissive"  → 校验并记 warn 日志,但不拒绝(灰度观察期)
	//   "enforce"     → 经 DS 面网关(x-pandora-ds-gateway)进来的回调必须带有效且范围匹配的令牌
	Mode string `yaml:"mode,omitempty" json:"mode,omitempty"`

	// Issuer JWT iss(默认 "pandora-ds-control");签发/校验双方必须一致。
	Issuer string `yaml:"issuer,omitempty" json:"issuer,omitempty"`

	// Audience JWT aud(默认 "pandora-ds");与玩家 SessionToken(pandora-client)严格分域,
	// 玩家令牌/DSTicket 不可能通过 DS 回调校验,反之亦然。
	Audience string `yaml:"audience,omitempty" json:"audience,omitempty"`

	// Secret 是 DS callback 域的 HS256 密钥(≥32 字节)，与玩家 jwt.secret keyset 必须无交集；
	// 本地开发也使用独立的稳定 dev key，确保 Model-B 域隔离不会只在线上才生效。
	// 留空 = 本服务不启用 DS 回调令牌(签发侧不签,校验侧视同 mode=off)。
	Secret string `yaml:"secret,omitempty" json:"secret,omitempty"`

	// AdditionalSecrets 是**仅用于校验**的额外可接受密钥(不用于签发),支持 DS 回调令牌不停服轮换
	// (审核 P1 #3)。签发始终用 Secret;校验时按 token 头 kid 路由,无 / 未知 kid 时依次尝试
	// Secret + AdditionalSecrets。三段式滚动轮换(①各服务先加新密钥进 additional → ②主密钥翻新、
	// 旧密钥进 additional → ③清空 additional)保证新旧副本共存期两把密钥都被接受,无 401 断档。
	// 每把仍须 ≥32 字节。默认空:单密钥,行为与历史一致。
	AdditionalSecrets []string `yaml:"additional_secrets,omitempty" json:"additional_secrets,omitempty"`

	// BattleTokenTTL 战斗 DS 令牌有效期(默认 4h,覆盖最长对局 + 重连窗口;战斗 DS 一局一销毁,不续期)。
	BattleTokenTTL Duration `yaml:"battle_token_ttl,omitempty" json:"battle_token_ttl,omitempty"`

	// HubTokenTTL 大厅 DS 令牌有效期(默认 24h;Hub DS 常驻,hub_allocator 在剩余寿命 < 1/3 时
	// 重签并重新 patch annotation 续期)。
	HubTokenTTL Duration `yaml:"hub_token_ttl,omitempty" json:"hub_token_ttl,omitempty"`

	// ActiveHeartbeatMaxAge 是非心跳业务写 RPC 读取 Redis active credential 时允许的最大
	// 服务端接收心跳年龄。只认 auth record 的 server receive time，不认 DS 上报 ts。
	ActiveHeartbeatMaxAge Duration `yaml:"active_heartbeat_max_age,omitempty" json:"active_heartbeat_max_age,omitempty"`

	// AuthorityMode 是 Hub DS 令牌**授权权威模式**(Model B,decision-revisit-ds-callback-auth §7)。
	// 仅 hub_allocator 读取,且仅在 agones + mode=enforce 下生效:
	//   ""/"legacy" → 沿用「令牌代际镜像门」(Redis INCR 代际写镜像 CurrentTokenGen,心跳精确匹配;默认,已验证)
	//   "redis"     → 「Redis 唯一授权权威 + active/pending 两阶段令牌状态机」:allocator 签发 pending 凭据
	//                  经 annotation 投递,DS 首个合法 pending 心跳在 authRepo 上原子激活;AssignHub 授权终态门
	//                  要求分片授权记录已激活。开启后 legacy 代际镜像门自动关闭(Model B 取代之)。
	// 非 agones/非 enforce 下本字段无效(legacy 与 redis 均退化为不校验)。
	AuthorityMode string `yaml:"authority_mode,omitempty" json:"authority_mode,omitempty"`

	// Fence 是 authority_mode=redis + mode=enforce 的机械激活栅栏。
	// etcd required_writer_epoch 必须先显式 bootstrap；服务启动线性读取并注册带租约 capability，
	// 失租、required 删除/回退/超过本二进制支持版本时进程立即退出。
	Fence DSAuthFenceConf `yaml:"fence,omitempty" json:"fence,omitempty"`
}

// DSAuthFenceConf 只含非敏感控制面配置；Pod UID 与镜像 digest 必须由 Downward API 注入环境变量，
// 不能从可伪造的 hostname/tag 回退。
type DSAuthFenceConf struct {
	EtcdEndpoints   []string `yaml:"etcd_endpoints,omitempty" json:"etcd_endpoints,omitempty"`
	EtcdPrefix      string   `yaml:"etcd_prefix,omitempty" json:"etcd_prefix,omitempty"`
	EtcdLeaseTTLSec int64    `yaml:"etcd_lease_ttl_sec,omitempty" json:"etcd_lease_ttl_sec,omitempty"`
	EtcdDialTimeout Duration `yaml:"etcd_dial_timeout,omitempty" json:"etcd_dial_timeout,omitempty"`
	KeysetRevision  string   `yaml:"keyset_revision,omitempty" json:"keyset_revision,omitempty"`
}

// AuthorityModeRedis 返回是否启用 Model B「Redis 唯一授权权威」模式(§7)。
func (c *DSAuthConf) AuthorityModeRedis() bool {
	return c.AuthorityMode == "redis"
}

// ValidateRedisFence 验证 Redis 单一权威不能在缺失机械 fence 时启动。
func (c *DSAuthConf) ValidateRedisFence() error {
	if !c.AuthorityModeRedis() {
		return nil
	}
	if c.Mode != "enforce" {
		return fmt.Errorf("ds_auth: authority_mode=redis requires mode=enforce")
	}
	if len(c.Fence.EtcdEndpoints) == 0 {
		return fmt.Errorf("ds_auth: authority_mode=redis requires fence.etcd_endpoints")
	}
	if c.Fence.KeysetRevision == "" {
		return fmt.Errorf("ds_auth: authority_mode=redis requires immutable fence.keyset_revision")
	}
	if c.Fence.EtcdLeaseTTLSec < 0 {
		return fmt.Errorf("ds_auth: fence.etcd_lease_ttl_sec must be positive or zero(default)")
	}
	if c.Fence.EtcdDialTimeout.Std() < 0 {
		return fmt.Errorf("ds_auth: fence.etcd_dial_timeout must be positive or zero(default)")
	}
	if c.ActiveHeartbeatMaxAge.Std() <= 0 {
		return fmt.Errorf("ds_auth: authority_mode=redis requires positive active_heartbeat_max_age")
	}
	return nil
}

// Defaults 把零值填默认(Mode/Secret 留空即"不启用",不填默认)。
func (c *DSAuthConf) Defaults() {
	if c.AuthorityMode == "" {
		c.AuthorityMode = "legacy"
	}
	if c.Issuer == "" {
		c.Issuer = "pandora-ds-control"
	}
	if c.Audience == "" {
		c.Audience = "pandora-ds"
	}
	if c.BattleTokenTTL == 0 {
		c.BattleTokenTTL = Duration(4 * time.Hour)
	}
	if c.HubTokenTTL == 0 {
		c.HubTokenTTL = Duration(24 * time.Hour)
	}
	if c.ActiveHeartbeatMaxAge == 0 {
		c.ActiveHeartbeatMaxAge = Duration(30 * time.Second)
	}
}

// DSTicketConf 是「玩家 DSTicket v2(RS256 非对称,方案 B)」信任域配置
// (docs/design/decision-revisit-player-jwt-key-rotation.md §7 拍板)。
//
// 与 SessionToken(HS256,pandora-client)、DS 回调令牌(HS256,pandora-ds)严格分域:
// iss=pandora-dsticket,aud=pandora-game-ds,alg 固定 RS256。私钥只存在于签发侧
// (login / hub_allocator / matchmaker)的 K8s Secret;DS Fleet 只持有公钥 JWKS,
// 机械检查禁止任何私钥/oct 材料进入 Fleet(§7.5)。
//
// 留空 PrivateKeyFile = 本服务不启用 v2 签发,沿用 legacy HS256 DSTicket(dev/local-off 不变)。
type DSTicketConf struct {
	// PrivateKeyFile 签发侧 RSA 私钥 PEM 路径(≥2048 位,K8s Secret 挂载,0400)。
	PrivateKeyFile string `yaml:"private_key_file,omitempty" json:"private_key_file,omitempty"`

	// ActiveKid 签发侧必填:期望的活跃 kid(RFC 7638 指纹)。与私钥指纹不符时启动失败,
	// 用于轮换窗口内机械确认「该副本用的是预期那把键」。
	ActiveKid string `yaml:"active_kid,omitempty" json:"active_kid,omitempty"`

	// TTL 票据有效期,默认 120s,机械上限 180s(auth.DSTicketMaxTTL,DS 校验侧同样强制)。
	TTL Duration `yaml:"ttl,omitempty" json:"ttl,omitempty"`

	// JWKSFile 校验侧公钥 JWKS 路径(仅 login 在线授权端点等服务侧校验用;DS 侧走
	// PANDORA_DSTICKET_JWKS_FILE env)。留空 = 本服务不启用 v2 校验。
	JWKSFile string `yaml:"jwks_file,omitempty" json:"jwks_file,omitempty"`

	// KeysetRevision 可选:期望的 keyset revision。设置后与 JWKS 文件内 revision 不符时启动失败。
	KeysetRevision string `yaml:"keyset_revision,omitempty" json:"keyset_revision,omitempty"`
}

// SignerEnabled 返回本服务是否启用 v2 签发。
func (c *DSTicketConf) SignerEnabled() bool { return c.PrivateKeyFile != "" }

// VerifierEnabled 返回本服务是否启用 v2 校验。
func (c *DSTicketConf) VerifierEnabled() bool { return c.JWKSFile != "" }

// DS 回调令牌 TTL 的启动期最小值。这两把令牌在关键路径上都存在「不续期」窗口:
//   - 战斗 DS 令牌:一局一签、永不续期(战斗 DS 一局一销毁),TTL 必须覆盖「最长对局 + 重连窗口」,
//     否则对局跑到一半令牌过期,battle_result 等回调被全拒、赛果无法结算。
//   - 大厅 DS 令牌:仅 agones 模式在剩余寿命 <1/3 时重签续期;local 模式(宿主 exec Hub DS)一次性
//     签发后永不续期。TTL 太短会让常驻 Hub 运行中途令牌过期、Hub 回调被拒。
//
// 因此最小值不能只防「签发即过期」(1 分钟),必须防「运行中途过期」——按各自不续期场景设可用下限。
// 低于下限属明显误配,启动即 fatal,而非等线上 DS 回调被莫名全拒才排查(审核 P1:非续期令牌 TTL 下限过松)。
const (
	dsAuthMinBattleTokenTTL = time.Hour // 战斗令牌不续期:粗粒度下限(精确关联 battle_ttl 的校验在 ds_allocator main)
	dsAuthMinHubTokenTTL    = time.Hour // 大厅令牌 local 模式不续期:至少覆盖一段常驻会话(local+enforce 更严的 12h 下限在 hub_allocator main)
)

// Validate 启动期校验签发侧 TTL 的正值与最小值(仅在本服务启用 DS 回调令牌时才有意义)。
//
// enabled=true 由调用方按“本服务是否签发/校验 DS 回调令牌”传入(Secret 非空 或 Mode!=off)。
// 未启用时零/负 TTL 无害(不会签发),直接放行;启用时 TTL 低于对应不续期下限 → error。
// 校验侧(只用 Mode/Secret)不受影响:BattleTokenTTL/HubTokenTTL 只在签发侧读取。
func (c *DSAuthConf) Validate(enabled bool) error {
	if !enabled {
		return nil
	}
	if c.BattleTokenTTL.Std() < dsAuthMinBattleTokenTTL {
		return fmt.Errorf("ds_auth: battle_token_ttl=%s too small (need >= %s; 战斗令牌不续期,须覆盖最长对局+重连窗口)", c.BattleTokenTTL.Std(), dsAuthMinBattleTokenTTL)
	}
	if c.HubTokenTTL.Std() < dsAuthMinHubTokenTTL {
		return fmt.Errorf("ds_auth: hub_token_ttl=%s too small (need >= %s; 大厅令牌 local 模式不续期,须覆盖一段常驻会话)", c.HubTokenTTL.Std(), dsAuthMinHubTokenTTL)
	}
	return nil
}

// BuildTopic 按 docs/design/infra.md §4 规范构造 kafka topic。
//
//	BuildTopic("battle", "result")          → "pandora.battle.result"
//	BuildTopic("login",  "event")           → "pandora.login.event"
func BuildTopic(domain, event string) string {
	return fmt.Sprintf("pandora.%s.%s", domain, event)
}

// BuildDLQTopic 构造死信队列 topic(infra.md §4.4)。
//
//	BuildDLQTopic("pandora.battle.result") → "pandora.dlq.battle.result"
func BuildDLQTopic(originalTopic string) string {
	const prefix = "pandora."
	if len(originalTopic) > len(prefix) && originalTopic[:len(prefix)] == prefix {
		return "pandora.dlq." + originalTopic[len(prefix):]
	}
	return "pandora.dlq." + originalTopic
}
