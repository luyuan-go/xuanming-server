// Package conf 是 trade 服务的私有配置结构(2026-06-16)。
package conf

import (
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
)

// Config 是 trade 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Trade TradeConf `yaml:"trade" json:"trade"`
}

// TradeConf 是 trade 服务私有配置。
type TradeConf struct {
	// OrderTTL 订单 Redis key 存活时长(默认 "10m")。应 > OrderExpire,
	// 给已结算 / 已取消订单留一段查询窗口(ListMyOrders / 客户端回看)。
	OrderTTL config.Duration `yaml:"order_ttl,omitempty" json:"order_ttl,omitempty"`

	// OrderExpire 订单从创建到自动过期的时长(默认 "5m")。
	// 超时未完成两阶段确认的订单在被访问时惰性置 EXPIRED。
	OrderExpire config.Duration `yaml:"order_expire,omitempty" json:"order_expire,omitempty"`

	// OptimisticRetry WATCH/MULTI/EXEC 乐观锁最大重试次数(默认 3)。
	// 耗尽 → ErrTradeLockFailed。
	OptimisticRetry int `yaml:"optimistic_retry,omitempty" json:"optimistic_retry,omitempty"`

	// MaxItemsPerOrder 单订单最大物品条目数(默认 20)。
	MaxItemsPerOrder int `yaml:"max_items_per_order,omitempty" json:"max_items_per_order,omitempty"`

	// MaxTradePrice 单笔交易金额上限(默认 1_000_000_000,与拍卖 MaxPrice 同量级)。
	//
	// 2026-08-22 货币无符号化时补:此前 trade 只有 `price < 0` 一道闸,**没有任何上界**。
	// price 改 uint64 后那道闸恒为 false,等于完全没有校验 —— 一个 price=-1 的旧请求
	// 会被解成 1.8e19 并直接落 Redis、再送 inventory 结算。上界闸是这条链唯一的防线。
	MaxTradePrice uint64 `yaml:"max_trade_price,omitempty" json:"max_trade_price,omitempty"`

	// RateQuotaPerMin 下单/撤单的 per-player 每分钟频率配额(anti-abuse §6 第 6 项;
	// 默认 20;负值 = 关闭)。与 MaxOrdersPerPlayer 总量闸正交:总量限「同时挂多少」,
	// 本值限「刷多快」,挡「下单-撤单-再下单」的托管写 + 流水行放大循环。窗口固定 1 分钟。
	RateQuotaPerMin int `yaml:"rate_quota_per_min,omitempty" json:"rate_quota_per_min,omitempty"`

	// MaxOrdersPerPlayer 单玩家同时参与(买方或卖方)的订单总数上限(默认 200,不变量 §18)。
	// 写入侧硬上限:CreateOrder 用 Lua 原子预留双方反查索引名额,满 → 先清理已过期/已终态
	// 的死成员重试一次,仍满返 ERR_TRADE_ORDER_LIMIT(7006)。同时把 ListMyOrders 的 SMEMBERS
	// 全量读兕定在几百以内(读取侧单次返回上限达标)。
	MaxOrdersPerPlayer int `yaml:"max_orders_per_player,omitempty" json:"max_orders_per_player,omitempty"`

	// InventoryAddr inventory 服务 gRPC 直连地址(host:port,内网 insecure)。
	// 配置后 trade 结算走真实 inventory P2P 原子对转(GrpcResourceLedger);
	// 留空 + allow_noop_ledger=true 才退回 NoopResourceLedger,否则 fail-fast。
	InventoryAddr string `yaml:"inventory_addr,omitempty" json:"inventory_addr,omitempty"`

	// AllowNoopLedger 显式允许在没有接入真实资源账本时退回 NoopResourceLedger(占位,结算永远成功、
	// 不真实扣转背包 / 货币)。默认 false:未接真实账本即 fail-fast,防止生产漏配后仍以「成交不扣减」静默启动
	// (审计:trade 静默 Noop 结算降级)。真实账本(inventory P2P 原子对转 RPC)接入前,仅联调 / 单测显式置 true。
	AllowNoopLedger bool `yaml:"allow_noop_ledger,omitempty" json:"allow_noop_ledger,omitempty"`
}

// Defaults 填默认值,防止 yaml 缺字段时零值引发非预期行为。
func (c *Config) Defaults() {
	if c.Trade.OrderTTL <= 0 {
		c.Trade.OrderTTL = config.Duration(10 * time.Minute)
	}
	if c.Trade.OrderExpire <= 0 {
		c.Trade.OrderExpire = config.Duration(5 * time.Minute)
	}
	if c.Trade.OptimisticRetry <= 0 {
		c.Trade.OptimisticRetry = 3
	}
	if c.Trade.MaxItemsPerOrder <= 0 {
		c.Trade.MaxItemsPerOrder = 20
	}
	if c.Trade.RateQuotaPerMin == 0 {
		c.Trade.RateQuotaPerMin = 20
	}
	if c.Trade.MaxOrdersPerPlayer <= 0 {
		c.Trade.MaxOrdersPerPlayer = 200
	}
	// 0 = 未配置 → 取默认。刻意**不**把 0 当成"不限价":一个漏配的字段不该等于拆掉唯一的价格防线。
	if c.Trade.MaxTradePrice == 0 {
		c.Trade.MaxTradePrice = 1_000_000_000
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20012"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21012"
	}
}
