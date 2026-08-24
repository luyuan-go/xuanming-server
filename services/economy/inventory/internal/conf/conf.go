// Package conf 是 inventory 服务的私有配置结构(W5 ③,2026-06-18)。
package conf

import (
	"fmt"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/dbguard"
)

// Config 是 inventory 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Inventory InventoryConf `yaml:"inventory" json:"inventory"`

	// Bag 背包域(pandora.bag.v1,bag-domain.md;phase 1 由 inventory 进程承载)。
	// bag.dsn 为空 = 未启用(不注册 BagService,现网行为不变,安全默认)。
	Bag BagConf `yaml:"bag,omitempty" json:"bag,omitempty"`

	// DSAuth DS 回调令牌校验(bag phase 2 五要件①):owner DS 直写背包域时经 :8444
	// 携带 DS 凭据令牌,BagService 验签抽取 pod/uid 与 owner 记录 target 全等校验。
	// mode 默认 off(dev);生产 enforce(与 battle_result/hub_allocator 同一密钥体系)。
	DSAuth config.DSAuthConf `yaml:"ds_auth,omitempty" json:"ds_auth,omitempty"`
}

// BagSectionCapacityRule 后端驻留段容量配置(仓库 / 活动段;未配置的段 fail-closed 拒写)。
type BagSectionCapacityRule struct {
	// BagType 段类型(1 仓库 / 100+ 活动段;随身组 0/2/3 容量归 DS,不在此配)。
	BagType uint32 `yaml:"bag_type" json:"bag_type"`
	// Capacity 段格子容量(>0)。
	Capacity uint32 `yaml:"capacity" json:"capacity"`
}

// BagItemStackRule 可堆叠道具的单格堆叠上限覆盖(未列道具用 default_max_stack)。
type BagItemStackRule struct {
	// ItemConfigID 配置表道具 ID(uint32,§12)。
	ItemConfigID uint32 `yaml:"item_config_id" json:"item_config_id"`
	// MaxStack 单格堆叠上限(>0)。
	MaxStack uint32 `yaml:"max_stack" json:"max_stack"`
}

// BagCapacityTier 一档扩容(阶梯价;第 N 档 = tiers[N-1])。
type BagCapacityTier struct {
	// Slots 本档增加的格数(>0)。
	Slots uint32 `yaml:"slots" json:"slots"`
	// PriceGold 本档价格(金币,>0)。
	PriceGold int64 `yaml:"price_gold" json:"price_gold"`
}

// BagCapacityPurchaseRule 某段的容量购买规则(bag-domain.md §5.3,2026-07-22 拍板)。
// 未登记的段不可购买;价格 / 档位 / 封顶全在服务端(数值不信 DS,§9.6)。
// 正式数值走 §9.15 配置表管线(与客户端展示表同源导出);接入前由本配置承载。
type BagCapacityPurchaseRule struct {
	// BagType 可买段(当前拍板:0 身上 / 1 仓库;装备栏/临时格/活动段不可买)。
	BagType uint32 `yaml:"bag_type" json:"bag_type"`
	// MaxExtra 已购增量硬上限(§9.18 有界;须 ≥ 全部档位 slots 之和)。
	MaxExtra uint32 `yaml:"max_extra" json:"max_extra"`
	// Tiers 阶梯档位(按序购买;购罄即不可再买)。
	Tiers []BagCapacityTier `yaml:"tiers" json:"tiers"`
}

// BagConf 是背包域配置(bag-domain.md §4/§5;五要件④额度也在此)。
type BagConf struct {
	// DSN pandora_bag 库连接串。空 = 背包域未启用(安全默认,不注册 BagService)。
	DSN string `yaml:"dsn,omitempty" json:"dsn,omitempty"`
	// 背包库是独立 DSN，但必须复用 pkg/mysqlx 的严格 TLS 与连接池契约；中心策划档
	// 渲染同名字段，本地留空保持既有行为。
	TLSCAFile       string          `yaml:"tls_ca_file,omitempty" json:"tls_ca_file,omitempty"`
	TLSServerName   string          `yaml:"tls_server_name,omitempty" json:"tls_server_name,omitempty"`
	MaxOpenConns    int             `yaml:"max_open_conns,omitempty" json:"max_open_conns,omitempty"`
	MaxIdleConns    int             `yaml:"max_idle_conns,omitempty" json:"max_idle_conns,omitempty"`
	ConnMaxLifetime config.Duration `yaml:"conn_max_lifetime,omitempty" json:"conn_max_lifetime,omitempty"`
	ConnMaxIdleTime config.Duration `yaml:"conn_max_idle_time,omitempty" json:"conn_max_idle_time,omitempty"`
	PingTimeout     config.Duration `yaml:"ping_timeout,omitempty" json:"ping_timeout,omitempty"`

	// OwnerAddr owner 服务 gRPC 地址(host:port;五要件② owner 授权,phase 2 写权威切换)。
	// 背包域启用时必填:LoadBag/AppendJournal/SaveCheckpoint 逐调校验
	// 「调用方声称的 (player, owner_epoch) 确实是当前 ADMITTED owner 且租约在效」。
	// 留空仅当 allow_unverified_owner=true(纯本地/单测,生产禁止)。
	OwnerAddr string `yaml:"owner_addr,omitempty" json:"owner_addr,omitempty"`

	// AllowUnverifiedOwner 跳过 owner 授权(仅 dev/单测;缺省 false = fail-closed)。
	// true 时背包写路径退化为 phase 1 的"存储侧 epoch 单调 CAS"防线,不得用于生产。
	AllowUnverifiedOwner bool `yaml:"allow_unverified_owner,omitempty" json:"allow_unverified_owner,omitempty"`

	// MaxJournalBatch 单次 AppendJournal 最大条数(默认 64,超限拒)。
	MaxJournalBatch int `yaml:"max_journal_batch,omitempty" json:"max_journal_batch,omitempty"`

	// MaxItemsPerOp 单条 op 的物品列表上限(默认 64,超限拒)。
	MaxItemsPerOp int `yaml:"max_items_per_op,omitempty" json:"max_items_per_op,omitempty"`

	// HourlyJournalQuota 单玩家每小时流水条数封顶(默认 2000;<=0 = 不限,仅测试用)。
	HourlyJournalQuota int64 `yaml:"hourly_journal_quota,omitempty" json:"hourly_journal_quota,omitempty"`

	// SectionCapacities 后端驻留段容量表(未配置段拒写;默认只含仓库 200)。
	SectionCapacities []BagSectionCapacityRule `yaml:"section_capacities,omitempty" json:"section_capacities,omitempty"`

	// DefaultMaxStack 可堆叠道具默认单格堆叠上限(默认 99,与 UE MyBag::DefaultMaxStackSize
	// 同值)。后端驻留段(仓库/活动段)由服务端权威拆堆:同 config 按上限分格存放,容量按
	// 拆堆后格子数校验(bag-domain.md §5.2,2026-07-22 拍板);随身组堆叠权威在 owner DS。
	// 正式数据源是 §9.15 配置表热更管线的道具表(与客户端 CfgItem 同源导出);接入前由本
	// 配置承载,接入后本字段仅作缺表兜底。
	DefaultMaxStack uint32 `yaml:"default_max_stack,omitempty" json:"default_max_stack,omitempty"`

	// ItemMaxStacks 按道具覆盖堆叠上限(未列道具用 DefaultMaxStack)。
	ItemMaxStacks []BagItemStackRule `yaml:"item_max_stacks,omitempty" json:"item_max_stacks,omitempty"`

	// CapacityPurchases 容量购买规则表(§5.3;默认按 2026-07-22 拍板:身上 100+100、
	// 仓库 200+300)。空列表显式配置 = 全部不可买。
	CapacityPurchases []BagCapacityPurchaseRule `yaml:"capacity_purchases,omitempty" json:"capacity_purchases,omitempty"`

	// JournalRetentionDays bag_journal 保留天数(默认 90,§9.24;checkpoint 覆盖位之前的
	// 流水仅审计用,重放依赖 covered_seq 之后的尾部,保留期远大于 checkpoint 周期)。
	JournalRetentionDays int `yaml:"journal_retention_days,omitempty" json:"journal_retention_days,omitempty"`

	// LegacyMigrationEnabled 旧 inventory 存量迁移作业开关(默认 false = 关)。
	// 只准在 contract 阶段、旧写路径(GrantItems/UseItem/SellItem/escrow)全部冻结后开启
	// (decision-revisit-bag-replay-semantics.md D5);提前开启会造成迁移快照与在写旧表漂移。
	// 作业幂等(bag_migration 一玩家一行永久闸)、可断点续跑、多副本并发安全,关门即停。
	LegacyMigrationEnabled bool `yaml:"legacy_migration_enabled,omitempty" json:"legacy_migration_enabled,omitempty"`

	// MigrationBatch 迁移作业单轮枚举玩家数(默认 200)。
	MigrationBatch int `yaml:"migration_batch,omitempty" json:"migration_batch,omitempty"`
}

// MySQLClientConf 把 bag 独立连接配置一次性翻译成共享 mysqlx seam，避免只传 DSN
// 时静默丢掉中心 MySQL 的 TLS 身份或小池参数。
func (bc BagConf) MySQLClientConf() config.MySQLConf {
	return config.MySQLConf{
		DSN:             bc.DSN,
		TLSCAFile:       bc.TLSCAFile,
		TLSServerName:   bc.TLSServerName,
		MaxOpenConns:    bc.MaxOpenConns,
		MaxIdleConns:    bc.MaxIdleConns,
		ConnMaxLifetime: bc.ConnMaxLifetime,
		ConnMaxIdleTime: bc.ConnMaxIdleTime,
		PingTimeout:     bc.PingTimeout,
	}
}

// SectionCapacityOf 返回某段容量(0 = 未配置,调用侧 fail-closed)。
func (bc *BagConf) SectionCapacityOf(bagType uint32) uint32 {
	for i := range bc.SectionCapacities {
		if bc.SectionCapacities[i].BagType == bagType {
			return bc.SectionCapacities[i].Capacity
		}
	}
	return 0
}

// ItemMaxStackOf 返回某道具单格堆叠上限(覆盖表优先,否则 DefaultMaxStack;
// 0 = 配置非法,调用侧 fail-closed 拒写,不静默无限合并)。
func (bc *BagConf) ItemMaxStackOf(itemConfigID uint32) uint32 {
	for i := range bc.ItemMaxStacks {
		if bc.ItemMaxStacks[i].ItemConfigID == itemConfigID {
			return bc.ItemMaxStacks[i].MaxStack
		}
	}
	return bc.DefaultMaxStack
}

// CapacityPurchaseRuleOf 返回某段的容量购买规则(不存在 → nil = 不可购买)。
func (bc *BagConf) CapacityPurchaseRuleOf(bagType uint32) *BagCapacityPurchaseRule {
	for i := range bc.CapacityPurchases {
		if bc.CapacityPurchases[i].BagType == bagType {
			return &bc.CapacityPurchases[i]
		}
	}
	return nil
}

// Validate 校验背包域配置(启动 fail-fast;非法堆叠/容量配置不允许带病上线)。
//   - section_capacities:bag_type 不重复,capacity > 0
//   - item_max_stacks:item_config_id 非 0 不重复,max_stack > 0
//   - capacity_purchases:仅 0/1 可买(§5.3 拍板)、bag_type 有 base、档位合法、
//     档位 slots 总和 ≤ max_extra
func (bc *BagConf) Validate() error {
	seenSec := make(map[uint32]struct{}, len(bc.SectionCapacities))
	for i := range bc.SectionCapacities {
		r := &bc.SectionCapacities[i]
		if _, dup := seenSec[r.BagType]; dup {
			return fmt.Errorf("bag.section_capacities[%d]: duplicate bag_type %d", i, r.BagType)
		}
		seenSec[r.BagType] = struct{}{}
		if r.Capacity == 0 {
			return fmt.Errorf("bag.section_capacities[%d]: capacity must be > 0 (bag_type %d)", i, r.BagType)
		}
	}
	seenItem := make(map[uint32]struct{}, len(bc.ItemMaxStacks))
	for i := range bc.ItemMaxStacks {
		r := &bc.ItemMaxStacks[i]
		if r.ItemConfigID == 0 {
			return fmt.Errorf("bag.item_max_stacks[%d]: item_config_id must not be 0", i)
		}
		if _, dup := seenItem[r.ItemConfigID]; dup {
			return fmt.Errorf("bag.item_max_stacks[%d]: duplicate item_config_id %d", i, r.ItemConfigID)
		}
		seenItem[r.ItemConfigID] = struct{}{}
		if r.MaxStack == 0 {
			return fmt.Errorf("bag.item_max_stacks[%d]: max_stack must be > 0 (item %d)", i, r.ItemConfigID)
		}
	}
	seenPurchase := make(map[uint32]struct{}, len(bc.CapacityPurchases))
	for i := range bc.CapacityPurchases {
		r := &bc.CapacityPurchases[i]
		// §5.3 拍板:仅身上(0)/仓库(1)可买;装备栏/临时格/活动段(代际清空)不可买。
		if r.BagType != 0 && r.BagType != 1 {
			return fmt.Errorf("bag.capacity_purchases[%d]: bag_type %d not purchasable (§5.3 只准 0/1)", i, r.BagType)
		}
		if _, dup := seenPurchase[r.BagType]; dup {
			return fmt.Errorf("bag.capacity_purchases[%d]: duplicate bag_type %d", i, r.BagType)
		}
		seenPurchase[r.BagType] = struct{}{}
		if bc.SectionCapacityOf(r.BagType) == 0 {
			return fmt.Errorf("bag.capacity_purchases[%d]: bag_type %d has no base capacity in section_capacities", i, r.BagType)
		}
		if len(r.Tiers) == 0 {
			return fmt.Errorf("bag.capacity_purchases[%d]: tiers required (bag_type %d)", i, r.BagType)
		}
		var totalSlots uint64
		for j := range r.Tiers {
			t := &r.Tiers[j]
			if t.Slots == 0 {
				return fmt.Errorf("bag.capacity_purchases[%d].tiers[%d]: slots must be > 0", i, j)
			}
			if t.PriceGold <= 0 {
				return fmt.Errorf("bag.capacity_purchases[%d].tiers[%d]: price_gold must be > 0", i, j)
			}
			totalSlots += uint64(t.Slots)
		}
		if totalSlots > uint64(r.MaxExtra) {
			return fmt.Errorf("bag.capacity_purchases[%d]: tiers total slots %d exceed max_extra %d (bag_type %d)",
				i, totalSlots, r.MaxExtra, r.BagType)
		}
	}
	return nil
}

// InventoryConf 是 inventory 服务私有配置。
//
// 道具的类型 / 堆叠 / 售价 / 局内可消费一律来自与 UE 同源的 configtable item 表
// (`cmd/inventory/configtable.go` 的 biz.ItemCatalog 适配器),本结构不再承载任何
// 策划数值 —— 曾经的 `item_rules` YAML 表已随 ca8f6304 整块删除,不要再加回来。
type InventoryConf struct {
	// Capacity 是装备实例背包格子容量(W5 ④)。<=0 = 未启用实例背包(GrantInstances 拒),
	// 安全默认。分配格 / 发放实例时按此上限校验(超出 → ErrInventoryCapacityFull)。
	Capacity int32 `yaml:"capacity,omitempty" json:"capacity,omitempty"`

	// IdentifyRules / DefaultIdentifyRule 只保留给未装配 ItemCatalog 的旧单元测试兼容。
	// 正式进程强制加载 configtable equipment_affix；这两项即使出现在 YAML 也不会参与生产鉴定。
	IdentifyRules []IdentifyRule `yaml:"identify_rules,omitempty" json:"identify_rules,omitempty"`

	// Deprecated:仅兼容未装配配置表 Catalog 的测试。
	DefaultIdentifyRule *IdentifyRule `yaml:"default_identify_rule,omitempty" json:"default_identify_rule,omitempty"`

	// ── 货币(多币种,2026-08-22)──

	// SellCurrencyKind 是**出售道具**统一的结算币种(pandora.common.v1.CurrencyKind)。
	// 留空 / 0 → 金币(1)。
	//
	// 为什么是服务端配置而不是道具表一列:当前没有"卖某道具得钻石"的确认需求,
	// 给策划源表加列要动 xlsx 版式 + 客户端 DataTable + 生成物四处,属预设性复杂化(§15.3)。
	// 真出现需求时加表列即可,RPC 契约不用改(响应里已带 CurrencyAmount.kind)。
	SellCurrencyKind int32 `yaml:"sell_currency_kind,omitempty" json:"sell_currency_kind,omitempty"`

	// MaxCurrencyPerGrant 是**单次发放**某一币种的数量上限(§9.6 五要件④ 额度)。
	// 留空 / 0 → 默认 1e12。
	//
	// 它拦的不是玩家(玩家无法直接调 GrantItems),而是**上游算错**:战后结算、活动、
	// 补偿脚本一旦把倍率或单位算错,没有这道闸就会把天文数字灌进经济体,而且事后无法区分
	// "正常大额"与"算错"。超限返回 ErrInvalidArg 并留日志,比事后查账便宜得多。
	MaxCurrencyPerGrant uint64 `yaml:"max_currency_per_grant,omitempty" json:"max_currency_per_grant,omitempty"`

	// ── NPC 商店(服务端权威购买,2026-08-22)──

	// MaxShopUnitsPerPurchase 是单次商店购买的最高份数(留空 / 0 → 默认 999)。
	// 份数是价格乘数,不设上限时一次请求就能顶到溢出闸;有上限则错误码更精确
	// (ERR_INVENTORY_NOT_PURCHASABLE 而不是含糊的参数溢出)。
	MaxShopUnitsPerPurchase uint32 `yaml:"max_shop_units_per_purchase,omitempty" json:"max_shop_units_per_purchase,omitempty"`

	// ── 保留期清理(CLAUDE.md §9 不变量 24:只增表必须有界)──

	// SweepInterval 保留期清理轮询间隔(默认 5m)。多副本各自跑,DELETE 幂等无需锁
	// (对齐 mail sweep / leaderboard 补扫模式)。
	SweepInterval config.Duration `yaml:"sweep_interval,omitempty" json:"sweep_interval,omitempty"`

	// SweepBatch 每轮每表清理行数上限(默认 500,小批量防长事务锁表;积压跨轮摊平)。
	SweepBatch int `yaml:"sweep_batch,omitempty" json:"sweep_batch,omitempty"`

	// LedgerRetentionDays inventory_ledger 幂等流水保留天数(默认 90,§9.24 上限也是 90)。
	// 必须远大于一切发放/使用/出售/结算的重试窗口(分钟级);另须 ≥ mail 邮件最长可领窗口
	// (mail 发送侧已把邮件寿命钳到 claim_retention_days=180 内,且 claim 行存活 ≥ 可领窗口,
	// 重复领取先被 mail 自身 claim 记录挡住,不依赖本流水永久兜底)。
	LedgerRetentionDays int `yaml:"ledger_retention_days,omitempty" json:"ledger_retention_days,omitempty"`

	// EscrowRetentionDays auction_escrow 已关闭(closed)托管行保留天数(默认 90)。
	// 只删 closed 行;active 行永不清理(EnsureAuctionEscrow 核对遗留订单依赖其存在性)。
	EscrowRetentionDays int `yaml:"escrow_retention_days,omitempty" json:"escrow_retention_days,omitempty"`

	// RetentionModeRaw 是保留期清理的执行模式(§9.24)。
	//
	// **留空 / "report_only" = 默认:只统计待清理量并 WARN 告警,一行都不删。**
	// "delete" = 真删。「因为数据大了就自动删」不可接受:清理条件/保留期/幂等窗口任一处
	// 配错都会静默删掉不该删的玩家数据且删完才发现,所以默认不删,由人确认后显式开启。
	// 注意:道具过期、邮件失效那类**业务语义**删除不受本开关约束(东西本来就该没了)。
	RetentionModeRaw string `yaml:"retention_mode,omitempty" json:"retention_mode,omitempty"`
}

// IdentifyAttrRoll 是鉴定属性池里的一条候选属性(值在 [Min,Max] 均匀 roll)。
type IdentifyAttrRoll struct {
	// AttrID 属性配置 ID(uint32,§12)。
	AttrID uint32 `yaml:"attr_id" json:"attr_id"`
	// Min / Max 该属性 roll 的闭区间(Min<=Max;均为该属性的数值)。
	Min int64 `yaml:"min" json:"min"`
	Max int64 `yaml:"max" json:"max"`
}

// IdentifyRule 是某配置装备鉴定时的随机属性规则(W5 ④)。
//
// 鉴定 = 从 Pool 里不放回抽 AttrCount 条属性,每条在 [Min,Max] 均匀 roll 数值。
// AttrCount>len(Pool) 时取 len(Pool)(全出);Pool 空 = 无属性(只置 identified)。
type IdentifyRule struct {
	// ItemConfigID 配置表道具 ID(uint32,§12)。
	ItemConfigID uint32 `yaml:"item_config_id" json:"item_config_id"`
	// AttrCount 鉴定产出的属性条数(>0)。
	AttrCount int `yaml:"attr_count" json:"attr_count"`
	// Pool 候选属性池。
	Pool []IdentifyAttrRoll `yaml:"pool" json:"pool"`
}

// Defaults 填默认值。
func (c *Config) Defaults() {
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20015"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21015"
	}
	if c.Inventory.SweepInterval <= 0 {
		c.Inventory.SweepInterval = config.Duration(5 * time.Minute)
	}
	if c.Inventory.SweepBatch <= 0 {
		c.Inventory.SweepBatch = 500
	}
	if c.Inventory.LedgerRetentionDays <= 0 {
		c.Inventory.LedgerRetentionDays = 90
	}
	if c.Inventory.EscrowRetentionDays <= 0 {
		c.Inventory.EscrowRetentionDays = 90
	}
	if c.Bag.MaxJournalBatch <= 0 {
		c.Bag.MaxJournalBatch = 64
	}
	if c.Bag.MaxItemsPerOp <= 0 {
		c.Bag.MaxItemsPerOp = 64
	}
	if c.Bag.HourlyJournalQuota == 0 {
		c.Bag.HourlyJournalQuota = 2000
	}
	if len(c.Bag.SectionCapacities) == 0 {
		// base 容量(§5.3):身上 100(LoadBag 权威下发随身段容量)+ 仓库 200。
		c.Bag.SectionCapacities = []BagSectionCapacityRule{{BagType: 0, Capacity: 100}, {BagType: 1, Capacity: 200}}
	}
	if c.Bag.CapacityPurchases == nil {
		// 按 2026-07-22 拍板建议:身上 10 档 ×10 格(100→200),第 N 档 100N 金币;
		// 仓库 15 档 ×20 格(200→500),第 N 档 200N 金币。正式数值走配置表管线后覆盖。
		carry := BagCapacityPurchaseRule{BagType: 0, MaxExtra: 100}
		for n := int64(1); n <= 10; n++ {
			carry.Tiers = append(carry.Tiers, BagCapacityTier{Slots: 10, PriceGold: 100 * n})
		}
		warehouse := BagCapacityPurchaseRule{BagType: 1, MaxExtra: 300}
		for n := int64(1); n <= 15; n++ {
			warehouse.Tiers = append(warehouse.Tiers, BagCapacityTier{Slots: 20, PriceGold: 200 * n})
		}
		c.Bag.CapacityPurchases = []BagCapacityPurchaseRule{carry, warehouse}
	}
	if c.Bag.DefaultMaxStack == 0 {
		c.Bag.DefaultMaxStack = 99
	}
	if c.Bag.MigrationBatch <= 0 {
		c.Bag.MigrationBatch = 200
	}
	if c.Bag.JournalRetentionDays <= 0 {
		c.Bag.JournalRetentionDays = 90
	}
}

// IdentifyRuleOf 返回某装备的鉴定随机属性规则(不存在 → nil,鉴定退化为只置 identified 无属性)。
func (ic *InventoryConf) IdentifyRuleOf(itemConfigID uint32) *IdentifyRule {
	for i := range ic.IdentifyRules {
		if ic.IdentifyRules[i].ItemConfigID == itemConfigID {
			return &ic.IdentifyRules[i]
		}
	}
	return ic.DefaultIdentifyRule
}

// Validate 校验服务私有规则(启动时调,非法配置直接 fail-fast)。
//
// 道具的 usable / sellable / 售价不在此校验:它们来自 configtable item 表,
// 由 `pkg/configtable` 的整批校验器把关(缺表 / checksum / 外键异常一律拒启)。
func (ic *InventoryConf) Validate() error {
	// 校验鉴定规则表(W5 ④):item_config_id 非 0 不重复;attr_count>0;pool 每条 min<=max。
	seenID := make(map[uint32]struct{}, len(ic.IdentifyRules))
	for i := range ic.IdentifyRules {
		r := &ic.IdentifyRules[i]
		if r.ItemConfigID == 0 {
			return fmt.Errorf("identify_rules[%d]: item_config_id must not be 0", i)
		}
		if _, dup := seenID[r.ItemConfigID]; dup {
			return fmt.Errorf("identify_rules[%d]: duplicate item_config_id %d", i, r.ItemConfigID)
		}
		seenID[r.ItemConfigID] = struct{}{}
		if r.AttrCount <= 0 {
			return fmt.Errorf("identify_rules[%d]: attr_count must be > 0 (got %d)", i, r.AttrCount)
		}
		seenAttr := make(map[uint32]struct{}, len(r.Pool))
		for j := range r.Pool {
			p := &r.Pool[j]
			if p.AttrID == 0 {
				return fmt.Errorf("identify_rules[%d].pool[%d]: attr_id must not be 0", i, j)
			}
			if _, dup := seenAttr[p.AttrID]; dup {
				return fmt.Errorf("identify_rules[%d].pool[%d]: duplicate attr_id %d", i, j, p.AttrID)
			}
			seenAttr[p.AttrID] = struct{}{}
			if p.Min > p.Max {
				return fmt.Errorf("identify_rules[%d].pool[%d]: min %d must be <= max %d", i, j, p.Min, p.Max)
			}
		}
	}
	if r := ic.DefaultIdentifyRule; r != nil {
		if r.ItemConfigID != 0 {
			return fmt.Errorf("default_identify_rule.item_config_id must be 0 (got %d)", r.ItemConfigID)
		}
		if err := validateIdentifyPool("default_identify_rule", r); err != nil {
			return err
		}
	}
	return nil
}

func validateIdentifyPool(path string, r *IdentifyRule) error {
	if r.AttrCount <= 0 {
		return fmt.Errorf("%s.attr_count must be > 0 (got %d)", path, r.AttrCount)
	}
	if len(r.Pool) == 0 {
		return fmt.Errorf("%s.pool must not be empty", path)
	}
	seen := make(map[uint32]struct{}, len(r.Pool))
	for i := range r.Pool {
		p := &r.Pool[i]
		if p.AttrID == 0 {
			return fmt.Errorf("%s.pool[%d]: attr_id must not be 0", path, i)
		}
		if _, dup := seen[p.AttrID]; dup {
			return fmt.Errorf("%s.pool[%d]: duplicate attr_id %d", path, i, p.AttrID)
		}
		seen[p.AttrID] = struct{}{}
		if p.Min > p.Max {
			return fmt.Errorf("%s.pool[%d]: min %d must be <= max %d", path, i, p.Min, p.Max)
		}
	}
	return nil
}

// RetentionMode 返回生效的保留期清理模式(启动已 Validate;解析失败回落 ModeReportOnly
// —— 任何不确定都不删)。
func (ic *InventoryConf) RetentionMode() dbguard.Mode {
	m, err := dbguard.ParseMode(ic.RetentionModeRaw)
	if err != nil {
		return dbguard.ModeReportOnly
	}
	return m
}

// ValidateRetentionMode 供 main 启动 fail-fast:写了无法识别的模式必须拒启,
// 而不是静默按 report_only 跑(运维以为开了删除、实际没开,库继续涨且无人知)。
func (ic *InventoryConf) ValidateRetentionMode() error {
	_, err := dbguard.ParseMode(ic.RetentionModeRaw)
	return err
}
