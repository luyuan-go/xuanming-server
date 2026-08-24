// Package biz 是 inventory 服务的业务逻辑层(W5 ③,2026-06-18)。
//
// 职责(docs/design/go-services.md 服务清册 economy 域;inventory 暂无专章,§2.9 是 trade):
//   - 背包道具持有 / 货币余额读
//   - 系统驱动幂等发放(GrantItems:战后掉落 / 活动 / 购买到账)
//   - 大厅态道具使用(UseItem:开箱 / 经验书)与出售换金币(SellItem)
//
// 边界(ds-arch.md §0.1):战斗内即时用道具 / 出装 / 购买道具走 UE GAS,不经 gRPC。
//
// 关键不变量(CLAUDE.md §9.7):发放 / 扣减必须原子 + 幂等键;校验数量在 data 层
// SELECT ... FOR UPDATE 锁行内做,避免并发超扣。usable / sellable 规则在 biz 层用配置裁决。
package biz

import (
	"context"
	"fmt"
	"math/rand"
	"sort"

	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"

	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/data"
)

// snowflakeGen 是 snowflake.Node 的最小接口(生成装备实例 instance_id,W5 ④)。
//
// GenerateInto 是批量预留:一次 CAS 拿整段,替代按件数循环调用 Generate。
// 语义与逐个 Generate 完全一致(严格递增、互不重复、可与 Generate 混用),
// 只保证「递增 + 唯一」,不保证相邻连续(跨秒会有空洞)。
type snowflakeGen interface {
	Generate() uint64
	GenerateInto(dst []uint64)
}

// ItemDefinition 是 inventory 业务真正需要的道具表最小投影。
// 展示字段不进入本域；规则每次现查同一个热更 Store，避免 YAML 与 UE 配置表漂移。
type ItemDefinition struct {
	Equipment bool
	// LobbyUsable 只有存在服务端大厅效果处理器时才可为真。真实 item.usable
	// 当前表示 UE GAS 局内使用，因此配置表适配器必须保持本字段为 false。
	LobbyUsable bool
	// BattleUsable 表示可由可信战斗进度事实触发持久扣减；它不赋予玩家 JWT
	// 直接调用大厅 UseItem 的权限。
	BattleUsable  bool
	SellUnitPrice int64
	MaxStack      uint32
}

// ItemCatalog 是策划道具表的只读热更视图。ok=false 必须 fail-closed，未知 ID 不能入账。
type ItemCatalog interface {
	Lookup(itemConfigID uint32) (def ItemDefinition, ok bool)
}

// IdentifyAttrDefinition 是配置表鉴定池的一条候选词条。Weight 参与加权不放回抽取；
// Min/Max 是服务端最终写入实例的闭区间，客户端不得自行 roll。
type IdentifyAttrDefinition struct {
	AttrID uint32
	Weight int64
	Min    int64
	Max    int64
}

// IdentifyDefinition 是某件装备当前热更批次对应的鉴定规则。
type IdentifyDefinition struct {
	AttrCount int
	Pool      []IdentifyAttrDefinition
}

// IdentifyCatalog 是 ItemCatalog 的可选深接口。生产配置表适配器实现它，使鉴定在每次
// 请求时读取同一个原子 Store；只实现 ItemCatalog 的旧单测仍走 YAML 兼容规则。
type IdentifyCatalog interface {
	IdentifyRule(itemConfigID uint32) (rule IdentifyDefinition, ok bool)
}

// InventoryUsecase 是 inventory 服务业务逻辑核心。
type InventoryUsecase struct {
	repo data.InventoryRepo
	cfg  conf.InventoryConf

	// router 是确定性 region/cell 路由器(scale-cellular-20m.md §4.2)。
	// 可为 nil:单 Cell / dev / 阶段 1~2 不分片,拍卖成交跨分片对转落点观测退化为不打日志
	// (行为不变)。分片部署时由 main 经 SetCellRouter 注入,SettleAuctionMatch 成功后额外打一条
	// 跨分片对转落点观测(买卖双方跨 Cell → 拆 Kafka 结算出箱幂等消费)。nil-safe。
	router *cellroute.Router

	// sf 生成装备实例 instance_id(W5 ④)。可为 nil:未装配时 GrantInstances 返回
	// ErrInvalidArg(实例背包未启用),不影响堆叠计数背包(GrantItems/UseItem/SellItem)。
	// 由 main 经 SetSnowflake 注入(与 SetCellRouter 一致,避免旧调用点被迫改签名)。
	sf snowflakeGen

	// randIntn 返回 [0,n) 的随机整数(装备鉴定 roll 随机属性用,W5 ④)。
	// 默认全局 math/rand;测试经 SetRandSource 注入确定性序列。n<=0 时约定返回 0。
	randIntn func(n int64) int64

	// catalog 是道具规则的唯一权威(生产 = configtable item 表适配器)；未注入即 fail-closed。
	catalog ItemCatalog

	// shops 是 NPC 商店定价的唯一权威(生产 = configtable shop 表适配器)。
	// 未注入 → GetShop / PurchaseShopItem 一律拒:宁可商店打不开,也不能用兜底价格扣钱。
	shops ShopCatalog
}

// NewInventoryUsecase 构造。
func NewInventoryUsecase(repo data.InventoryRepo, cfg conf.InventoryConf) *InventoryUsecase {
	return &InventoryUsecase{
		repo:     repo,
		cfg:      cfg,
		randIntn: defaultRandIntn,
	}
}

// defaultRandIntn 是默认随机源(全局 math/rand;n<=0 返回 0)。
func defaultRandIntn(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return rand.Int63n(n)
}

// SetSnowflake 注入装备实例 ID 生成器(W5 ④)。nil-safe:不调用时实例背包禁用。
func (u *InventoryUsecase) SetSnowflake(sf snowflakeGen) { u.sf = sf }

// SetItemCatalog 注入策划道具表。启用后未知 ID、堆叠/实例模型错路由全部 fail-closed。
func (u *InventoryUsecase) SetItemCatalog(c ItemCatalog) { u.catalog = c }

// itemDefinition 现查道具表当前批次。catalog 未注入时 fail-closed —— 生产路径
// main.go 已强制注入(缺 config_table.dir 直接拒启),这里不再保留 YAML 兜底规则:
// 一份可能与 UE 漂移的兜底数值参与扣减 / 入账,比拒掉一次请求危险得多。
func (u *InventoryUsecase) itemDefinition(itemConfigID uint32) (ItemDefinition, bool) {
	if u.catalog == nil {
		return ItemDefinition{}, false
	}
	return u.catalog.Lookup(itemConfigID)
}

// SetRandSource 注入鉴定 roll 随机源(测试用确定性序列;不调用则用默认 math/rand)。
func (u *InventoryUsecase) SetRandSource(fn func(n int64) int64) {
	if fn != nil {
		u.randIntn = fn
	}
}

// SetCellRouter 注入确定性 region/cell 路由器(scale-cellular-20m.md §4.2 两级架构)。
//
// nil-safe:不调用 / 传 nil 时(单 Cell / dev / 阶段 1~2),SettleAuctionMatch 不做跨分片对转落点
// 观测,行为与历史一致。用 setter 而非构造参数,避免单 Cell 阶段调用点被迫改签名(与 matchmaker /
// auction / battle_result / friend / chat / trade / dialogue 一致)。Router 内部读路径无锁,并发安全。
func (u *InventoryUsecase) SetCellRouter(r *cellroute.Router) {
	u.router = r
}

// GetInventory 读玩家背包(全部币种余额 + 道具堆叠)。
func (u *InventoryUsecase) GetInventory(ctx context.Context, playerID uint64) (data.Balances, []data.ItemStack, error) {
	if playerID == 0 {
		return nil, nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	return u.repo.GetInventory(ctx, playerID)
}

// GetInventoryFull 读玩家完整背包(货币 + 堆叠道具 + 容量 + 装备实例,W5 ④)。
// capacity 来自服务配置(所有玩家同一基础容量;<=0 表示未启用实例背包)。
// 未启用实例背包时不读 player_item_instance 表(既有库可能尚未迁移出该表,
// 避免 GetInventory 全量报内部错;启用时 main 启动期已做 schema 检查)。
func (u *InventoryUsecase) GetInventoryFull(ctx context.Context, playerID uint64) (data.Balances, []data.ItemStack, int32, []data.ItemInstance, error) {
	if playerID == 0 {
		return nil, nil, 0, nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	balances, items, err := u.repo.GetInventory(ctx, playerID)
	if err != nil {
		return nil, nil, 0, nil, err
	}
	if u.cfg.Capacity <= 0 {
		return balances, items, 0, nil, nil
	}
	instances, ierr := u.repo.ListInstances(ctx, playerID)
	if ierr != nil {
		return nil, nil, 0, nil, ierr
	}
	return balances, items, u.cfg.Capacity, instances, nil
}

// GrantInstances 幂等发放装备实例(系统驱动:掉落 / 活动 / 购买到账,W5 ④)。
// 为每个 itemConfigID 用 snowflake 生成一件独立实例(默认未鉴定);容量满 → ErrInventoryCapacityFull。
func (u *InventoryUsecase) GrantInstances(ctx context.Context, playerID uint64, itemConfigIDs []uint32, idempotencyKey string) ([]data.ItemInstance, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if idempotencyKey == "" {
		return nil, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	if len(itemConfigIDs) == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "nothing to grant")
	}
	// 这里**刻意没有**按"uint64 最坏 20 位"反推的单批件数上闸(2026-08-24 删)。
	// 那道闸比现实严(现网雪花 17 位,实际能发 13 件,它只放 11 件),更致命的是它挡在
	// data 的幂等回放之前:已提交成功的旧批次再也回放不了,而掉落出箱 / 邮件领取 /
	// 任务补扫都是永不放弃的重试者 —— 拒一次就是永久卡住的行(货已发、行清不掉)。
	// detail 列宽由 data.GrantInstances 按**实际编码长度**在回放之后判定,那里天然对回放安全。
	for _, id := range itemConfigIDs {
		if id == 0 {
			return nil, errcode.New(errcode.ErrInvalidArg, "item_config_id required")
		}
		if u.catalog != nil {
			def, ok := u.itemDefinition(id)
			if !ok {
				return nil, errcode.New(errcode.ErrInvalidArg, "unknown item_config_id: %d", id)
			}
			if !def.Equipment {
				return nil, errcode.New(errcode.ErrInvalidArg, "stackable item %d must use GrantItems", id)
			}
		}
	}
	if u.sf == nil {
		return nil, errcode.New(errcode.ErrInvalidArg, "instance inventory not enabled (no id generator)")
	}
	// 一次 CAS 预留整段,替代循环调用 Generate len(itemConfigIDs) 次。
	// 产出严格递增且唯一,但不保证连续(跨秒有空洞);此处只按下标一一对应取用。
	instanceIDs := make([]uint64, len(itemConfigIDs))
	u.sf.GenerateInto(instanceIDs)
	detail := fmt.Sprintf("grant_inst n=%d", len(itemConfigIDs))
	insts, already, err := u.repo.GrantInstances(ctx, playerID, instanceIDs, itemConfigIDs, u.cfg.Capacity, idempotencyKey, detail)
	if err != nil {
		return nil, err
	}
	if already {
		plog.With(ctx).Infow("msg", "grant_instances_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey, "count", len(insts))
	}
	return insts, nil
}

// IdentifyItem 鉴定一件未鉴定装备实例:服务端权威 roll 随机属性(反作弊,不变量 §6)后落库。
// 幂等:已鉴定 → 回放已落定属性(不重复 roll)。无鉴定规则 → 只置 identified 无属性(不阻断)。
func (u *InventoryUsecase) IdentifyItem(ctx context.Context, playerID, instanceID uint64) (data.ItemInstance, error) {
	if playerID == 0 {
		return data.ItemInstance{}, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if instanceID == 0 {
		return data.ItemInstance{}, errcode.New(errcode.ErrInvalidArg, "instance_id required")
	}
	// 先锁读实例拿到 item_config_id 才能查规则 roll —— 但为避免两次读,直接把 roll 结果传给
	// 数据层,由数据层在 FOR UPDATE 里裁决"已鉴定则回放、否则落定"。roll 依赖 item_config_id,
	// 而此处尚未读到实例。折中:数据层先读实例;若已鉴定回放,否则回调 roll。这里改为两段式:
	// 1) 读实例(不锁)拿 config_id 预 roll;2) 数据层 FOR UPDATE 再判(已鉴定则忽略预 roll)。
	// 由于 roll 结果只在"未鉴定→落定"分支被采用,预读到 config 变更的概率为零(实例 config 不可变)。
	insts, lerr := u.repo.ListInstances(ctx, playerID)
	if lerr != nil {
		return data.ItemInstance{}, lerr
	}
	var configID uint32
	found := false
	for i := range insts {
		if insts[i].InstanceID == instanceID {
			configID = insts[i].ItemConfigID
			found = true
			break
		}
	}
	if !found {
		return data.ItemInstance{}, errcode.New(errcode.ErrInventoryItemNotFound, "instance not found player=%d id=%d", playerID, instanceID)
	}
	if u.catalog != nil {
		def, ok := u.itemDefinition(configID)
		if !ok || !def.Equipment {
			return data.ItemInstance{}, errcode.New(errcode.ErrInvalidArg,
				"instance config is not a configured equipment: %d", configID)
		}
	}
	attrs := u.rollIdentifyAttrs(configID)
	if u.catalog != nil && len(attrs) == 0 {
		return data.ItemInstance{}, errcode.New(errcode.ErrInvalidState,
			"equipment identify rule unavailable: %d", configID)
	}
	inst, already, err := u.repo.IdentifyInstance(ctx, playerID, instanceID, attrs)
	if err != nil {
		return data.ItemInstance{}, err
	}
	if already {
		plog.With(ctx).Infow("msg", "identify_item_idempotent_hit",
			"player_id", playerID, "instance_id", instanceID)
	}
	return inst, nil
}

// rollIdentifyAttrs 按配置从属性池不放回抽 AttrCount 条,每条在 [Min,Max] 均匀 roll 数值。
// 无规则 / 空池 → 返回 nil；接入配置表的生产路径由 IdentifyItem fail-closed，
// 防止把装备永久写成 identified=true 但零词条。
func (u *InventoryUsecase) rollIdentifyAttrs(itemConfigID uint32) []data.ItemAttribute {
	var definition IdentifyDefinition
	if catalog, ok := u.catalog.(IdentifyCatalog); ok {
		var found bool
		definition, found = catalog.IdentifyRule(itemConfigID)
		if !found {
			return nil
		}
	} else {
		rule := u.cfg.IdentifyRuleOf(itemConfigID)
		if rule == nil {
			return nil
		}
		definition.AttrCount = rule.AttrCount
		definition.Pool = make([]IdentifyAttrDefinition, 0, len(rule.Pool))
		for _, p := range rule.Pool {
			// 旧 YAML 没有权重；兼容路径按等权处理。
			definition.Pool = append(definition.Pool, IdentifyAttrDefinition{
				AttrID: p.AttrID, Weight: 1, Min: p.Min, Max: p.Max,
			})
		}
	}
	if definition.AttrCount <= 0 || definition.AttrCount > len(definition.Pool) {
		return nil
	}

	// 每轮按剩余候选的权重抽一条并移除，严格加权不放回。配置加载门已限制
	// 候选数/总权重/数值范围；这里仍逐次校验，防自定义 Catalog 绕过门禁。
	remaining := append([]IdentifyAttrDefinition(nil), definition.Pool...)
	out := make([]data.ItemAttribute, 0, definition.AttrCount)
	for len(out) < definition.AttrCount {
		var totalWeight int64
		for _, candidate := range remaining {
			if candidate.AttrID == 0 || candidate.Weight <= 0 ||
				candidate.Min < 0 || candidate.Max < candidate.Min ||
				totalWeight > int64(^uint64(0)>>1)-candidate.Weight {
				return nil
			}
			totalWeight += candidate.Weight
		}
		if totalWeight <= 0 {
			return nil
		}

		draw := u.randIntn(totalWeight)
		chosen := len(remaining) - 1
		for i, candidate := range remaining {
			if draw < candidate.Weight {
				chosen = i
				break
			}
			draw -= candidate.Weight
		}
		candidate := remaining[chosen]
		span := candidate.Max - candidate.Min + 1
		if span <= 0 {
			return nil
		}
		out = append(out, data.ItemAttribute{
			AttrID: candidate.AttrID,
			Value:  candidate.Min + u.randIntn(span),
		})
		remaining = append(remaining[:chosen], remaining[chosen+1:]...)
	}
	return out
}

// MoveInstance 移动一件装备实例到新格子(纯大厅整理,不影响属性,W5 ④)。
func (u *InventoryUsecase) MoveInstance(ctx context.Context, playerID, instanceID uint64, toSlot int32) (data.ItemInstance, error) {
	if playerID == 0 {
		return data.ItemInstance{}, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if instanceID == 0 {
		return data.ItemInstance{}, errcode.New(errcode.ErrInvalidArg, "instance_id required")
	}
	if u.cfg.Capacity <= 0 {
		return data.ItemInstance{}, errcode.New(errcode.ErrInventorySlotOccupied, "instance inventory disabled")
	}
	return u.repo.MoveInstance(ctx, playerID, instanceID, toSlot, u.cfg.Capacity)
}

// DiscardInstance 丢弃一件装备实例(从背包永久删除,W5 ④)。幂等:已丢弃 → OK no-op。
func (u *InventoryUsecase) DiscardInstance(ctx context.Context, playerID, instanceID uint64) error {
	if playerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if instanceID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "instance_id required")
	}
	return u.repo.DiscardInstance(ctx, playerID, instanceID)
}

// CheckInstancesOwned 精确校验唯一实例归属。instance_id 与 item_config_id 必须同时匹配，
// 返回请求集合中匹配的权威实例快照（去重、按 instance_id 升序）。
const MaxCheckInstancesOwned = 64

func (u *InventoryUsecase) CheckInstancesOwned(ctx context.Context, playerID uint64, queries []data.InstanceOwnershipQuery) ([]data.ItemInstance, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if len(queries) == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "instances required")
	}
	if len(queries) > MaxCheckInstancesOwned {
		return nil, errcode.New(errcode.ErrInvalidArg,
			"too many instances: %d > %d", len(queries), MaxCheckInstancesOwned)
	}
	seen := make(map[uint64]struct{}, len(queries))
	for _, q := range queries {
		if q.InstanceID == 0 || q.ItemConfigID == 0 {
			return nil, errcode.New(errcode.ErrInvalidArg, "instance_id and item_config_id required")
		}
		if _, duplicate := seen[q.InstanceID]; duplicate {
			return nil, errcode.New(errcode.ErrInvalidArg, "duplicate instance_id: %d", q.InstanceID)
		}
		seen[q.InstanceID] = struct{}{}
		if u.catalog != nil {
			def, ok := u.itemDefinition(q.ItemConfigID)
			if !ok || !def.Equipment {
				return nil, errcode.New(errcode.ErrInvalidArg,
					"instance query config is not configured equipment: %d", q.ItemConfigID)
			}
		}
	}
	return u.repo.CheckInstancesOwned(ctx, playerID, queries)
}

// GrantItems 幂等发放道具 + 多币种货币(系统驱动,idempotency_key 防重复入账)。
//
// currencies 的校验刻意逐条做,而不是"总额不为负"一刀切:
// 旧实现写的是 `if gold < 0`,把 gold 改成无符号后该判断会**恒为 false**(编译器不报错,
// go vet 默认也不报),等于闸门被静默拆掉。无符号世界里"防负数"的正确形态是
// 在**类型边界**校验(service 层从 proto 转进来时判),内层改判"是否为 0 / 是否重复 / 是否超单笔上限"。
func (u *InventoryUsecase) GrantItems(ctx context.Context, playerID uint64, items []data.ItemGrant, currencies data.Balances, idempotencyKey string) (data.Balances, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if idempotencyKey == "" {
		return nil, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	if len(items) == 0 && len(currencies) == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "nothing to grant")
	}
	for kind, amount := range currencies {
		if verr := data.ValidateCurrencyKind(kind); verr != nil {
			return nil, verr
		}
		if amount == 0 {
			return nil, errcode.New(errcode.ErrInvalidArg, "grant amount must be positive: kind=%d", int32(kind))
		}
		if amount > u.maxCurrencyPerGrant() {
			// 单笔发放上限(§9.6 五要件④ 额度):防止上游算错把天文数字灌进经济体。
			return nil, errcode.New(errcode.ErrInvalidArg,
				"grant amount exceeds per-call limit: kind=%d amount=%d limit=%d",
				int32(kind), amount, u.maxCurrencyPerGrant())
		}
	}
	for _, it := range items {
		if it.ItemConfigID == 0 {
			return nil, errcode.New(errcode.ErrInvalidArg, "item_config_id required")
		}
		if it.Count <= 0 {
			return nil, errcode.New(errcode.ErrInvalidArg, "count must be positive: item=%d", it.ItemConfigID)
		}
		if u.catalog != nil {
			def, ok := u.itemDefinition(it.ItemConfigID)
			if !ok {
				return nil, errcode.New(errcode.ErrInvalidArg, "unknown item_config_id: %d", it.ItemConfigID)
			}
			if def.Equipment {
				return nil, errcode.New(errcode.ErrInvalidArg, "equipment item %d must use GrantInstances", it.ItemConfigID)
			}
		}
	}
	detail := fmt.Sprintf("grant items=%d gold=%d", len(items), currencies.Get(data.CurrencyGold))
	newBalances, already, err := u.repo.GrantItems(ctx, playerID, items, currencies, idempotencyKey, detail)
	if err != nil {
		return nil, err
	}
	if already {
		plog.With(ctx).Infow("msg", "grant_items_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey, "gold", newBalances.Get(data.CurrencyGold))
	}
	return newBalances, nil
}

// UseItem 大厅态使用消耗品(不可大厅使用 → ErrInventoryItemNotUsable;数量不足 → ErrInventoryInsufficient)。
func (u *InventoryUsecase) UseItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey string) (int64, error) {
	if playerID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if itemConfigID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "item_config_id required")
	}
	if count <= 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "count must be positive")
	}
	if idempotencyKey == "" {
		return 0, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	def, ok := u.itemDefinition(itemConfigID)
	if !ok || !def.LobbyUsable || def.Equipment {
		return 0, errcode.New(errcode.ErrInventoryItemNotUsable, "item not usable in lobby: %d", itemConfigID)
	}
	detail := fmt.Sprintf("use item=%d count=%d", itemConfigID, count)
	remaining, already, err := u.repo.UseItem(ctx, playerID, itemConfigID, count, idempotencyKey, detail)
	if err != nil {
		return 0, err
	}
	if already {
		plog.With(ctx).Infow("msg", "use_item_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey, "item", itemConfigID, "remaining", remaining)
	}
	return remaining, nil
}

// ConsumeBattleItem 按可信 DS 进度事实持久扣减局内消耗品。此能力只供系统 RPC，
// 与大厅 UseItem 分离，避免把 item.usable 误解为“扣掉即完成效果”。实际回血/增益由 UE GAS
// 在 ReportProgress ACK 后执行；这里仅保证后端资产不会在重登后复活。
func (u *InventoryUsecase) ConsumeBattleItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey string) (int64, error) {
	if playerID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if itemConfigID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "item_config_id required")
	}
	if count <= 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "count must be positive")
	}
	if idempotencyKey == "" {
		return 0, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	def, ok := u.itemDefinition(itemConfigID)
	if !ok || !def.BattleUsable || def.Equipment {
		return 0, errcode.New(errcode.ErrInventoryItemNotUsable,
			"item not consumable in battle: %d", itemConfigID)
	}
	detail := fmt.Sprintf("battle consume item=%d count=%d", itemConfigID, count)
	remaining, already, err := u.repo.ConsumeBattleItem(ctx, playerID, itemConfigID, count, idempotencyKey, detail)
	if err != nil {
		return 0, err
	}
	if already {
		plog.With(ctx).Infow("msg", "consume_battle_item_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey,
			"item", itemConfigID, "remaining", remaining)
	}
	return remaining, nil
}

// DiscardBattleItem 按可信 DS 进度事实持久丢弃可堆叠物品。装备实例没有可靠的
// phase0 DS Guid ↔ inventory instance_id 映射，本接口明确只接收非装备配置。
func (u *InventoryUsecase) DiscardBattleItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey string) (int64, error) {
	if playerID == 0 || itemConfigID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "player_id and item_config_id required")
	}
	if count <= 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "count must be positive")
	}
	if idempotencyKey == "" {
		return 0, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	def, ok := u.itemDefinition(itemConfigID)
	if !ok || def.Equipment {
		return 0, errcode.New(errcode.ErrInvalidArg,
			"battle discard only supports configured stackable item: %d", itemConfigID)
	}
	detail := fmt.Sprintf("battle discard item=%d count=%d", itemConfigID, count)
	remaining, already, err := u.repo.DiscardBattleItem(ctx, playerID, itemConfigID, count, idempotencyKey, detail)
	if err != nil {
		return 0, err
	}
	if already {
		plog.With(ctx).Infow("msg", "discard_battle_item_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey,
			"item", itemConfigID, "remaining", remaining)
	}
	return remaining, nil
}

// DiscardItem 幂等丢弃可堆叠物品。装备实例必须走 DiscardInstance，避免按配置 ID
// 随机删掉同配置但不同词条的实例。
func (u *InventoryUsecase) DiscardItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey string) (int64, error) {
	if playerID == 0 || itemConfigID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "player_id and item_config_id required")
	}
	if count <= 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "count must be positive")
	}
	if idempotencyKey == "" {
		return 0, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	if u.catalog != nil {
		def, ok := u.itemDefinition(itemConfigID)
		if !ok || def.Equipment {
			return 0, errcode.New(errcode.ErrInvalidArg,
				"item %d is not configured stackable", itemConfigID)
		}
	}
	detail := fmt.Sprintf("discard item=%d count=%d", itemConfigID, count)
	remaining, already, err := u.repo.DiscardItem(ctx, playerID, itemConfigID, count, idempotencyKey, detail)
	if err != nil {
		return 0, err
	}
	if already {
		plog.With(ctx).Infow("msg", "discard_item_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey,
			"item", itemConfigID, "remaining", remaining)
	}
	return remaining, nil
}

// SellInstance 原子出售唯一装备实例。售价由该实例的真实 item_config_id 查同源表，
// repo 在事务里重新锁实例并拒绝 bound，删除与金币入账/ledger 同成同败。
func (u *InventoryUsecase) SellInstance(ctx context.Context, playerID, instanceID uint64, itemConfigID uint32, idempotencyKey string) (data.SaleOutcome, error) {
	if playerID == 0 || instanceID == 0 || itemConfigID == 0 {
		return data.SaleOutcome{}, errcode.New(errcode.ErrInvalidArg, "player_id, instance_id and item_config_id required")
	}
	if idempotencyKey == "" {
		return data.SaleOutcome{}, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	kind := u.sellCurrencyKind()
	def, ok := u.itemDefinition(itemConfigID)
	amount := uint64(0)
	if ok && (u.catalog == nil || def.Equipment) && def.SellUnitPrice > 0 {
		amount = uint64(def.SellUnitPrice)
	}
	// 即使热更后配置已删/禁售也要进入 repo：同 key 的首次成功结果必须从 ledger
	// 回放。amount=0 的首次请求由 repo 在 claim 后回滚为 NotSellable，不会建脏流水。
	//
	// detail 格式保持与多币种改造前逐字节一致(`gold=%d`):升级前写下的流水行靠
	// legacySaleLedgerMatches 解析这个格式来判定"是否同一次出售",格式一变,
	// 老玩家用老 key 重试就会被误判成 ErrInventoryIdempotencyConflict。
	detail := fmt.Sprintf("sell instance=%d item=%d gold=%d", instanceID, itemConfigID, amount)
	outcome, already, err := u.repo.SellInstance(ctx, playerID, instanceID, itemConfigID, kind, amount, idempotencyKey, detail)
	if err != nil {
		return data.SaleOutcome{}, err
	}
	if already {
		plog.With(ctx).Infow("msg", "sell_instance_idempotent_hit",
			"player_id", playerID, "instance_id", instanceID,
			"idempotency_key", idempotencyKey, "earned", outcome.Earned,
			"balances", describeCurrencies(outcome.Balances))
	}
	return outcome, nil
}

// SellItem 出售可堆叠道具换货币(不可出售 → ErrInventoryNotSellable;数量不足 → ErrInventoryInsufficient)。
func (u *InventoryUsecase) SellItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey string) (data.SaleOutcome, error) {
	if playerID == 0 {
		return data.SaleOutcome{}, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if itemConfigID == 0 {
		return data.SaleOutcome{}, errcode.New(errcode.ErrInvalidArg, "item_config_id required")
	}
	if count <= 0 {
		return data.SaleOutcome{}, errcode.New(errcode.ErrInvalidArg, "count must be positive")
	}
	if idempotencyKey == "" {
		return data.SaleOutcome{}, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	kind := u.sellCurrencyKind()
	def, ok := u.itemDefinition(itemConfigID)
	amount := uint64(0)
	overflow := false
	if ok && !def.Equipment && def.SellUnitPrice > 0 {
		amount, ok = data.SafeMulCurrency(uint64(def.SellUnitPrice), uint64(count))
		overflow = !ok || amount == 0
		if overflow {
			amount = 0
		}
	}
	// detail 格式保持与多币种改造前逐字节一致,理由见 SellInstance。
	detail := fmt.Sprintf("sell item=%d count=%d gold=%d", itemConfigID, count, amount)
	outcome, already, err := u.repo.SellItem(ctx, playerID, itemConfigID, count, kind, amount, idempotencyKey, detail)
	if err != nil {
		if overflow && errcode.As(err) == errcode.ErrInventoryNotSellable {
			// 溢出与"本来就不可出售"在 repo 里都表现为 amount==0;这里用调用侧记下的
			// overflow 标志把它翻回更准确的错误码,免得策划以为是自己没配售价。
			return data.SaleOutcome{}, errcode.New(errcode.ErrInvalidArg,
				"sell amount overflow item=%d price=%d count=%d", itemConfigID, def.SellUnitPrice, count)
		}
		return data.SaleOutcome{}, err
	}
	if already {
		plog.With(ctx).Infow("msg", "sell_item_idempotent_hit",
			"player_id", playerID, "idempotency_key", idempotencyKey, "item", itemConfigID,
			"remaining", outcome.Remaining, "earned", outcome.Earned)
	}
	return outcome, nil
}

// SettleAuctionMatch 原子结算一笔拍卖成交(系统驱动,幂等键基于 match_id)。
//
// 在 inventory 一个本地事务里从双方 escrow 消费完成资产对转:
//   - 卖家:从卖单 escrow 消费 quantity 个 itemConfigID 交付买家,卖家加 quantity*unitPrice 金币;
//   - 买家:从买单 escrow 消费 quantity*unitPrice 金币付给卖家,买家加 quantity 个 itemConfigID。
//
// 资产已在 FreezeForOrder 冻结进 escrow,成交不会因余额不足失败。
// 幂等键 = "auction:settle:<match_id>",同一成交重复结算只生效一次(不变量 §9.2 / §9.7)。
func (u *InventoryUsecase) SettleAuctionMatch(ctx context.Context, matchID, sellerID, buyerID, sellOrderID, buyOrderID uint64, itemConfigID uint32, quantity int64, kind data.CurrencyKind, unitPrice uint64) error {
	if matchID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	if sellerID == 0 || buyerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "seller_id / buyer_id required")
	}
	if sellOrderID == 0 || buyOrderID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "sell_order_id / buy_order_id required")
	}
	if sellerID == buyerID {
		// 自成交净额为零且会让同一玩家同 key 写两条流水冲突;视为非法,撮合侧应避免自撮合。
		return errcode.New(errcode.ErrInvalidArg, "seller and buyer must differ: %d", sellerID)
	}
	if itemConfigID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "item_config_id required")
	}
	if quantity <= 0 {
		return errcode.New(errcode.ErrInvalidArg, "quantity must be positive")
	}
	// unitPrice 是 uint64:`== 0` 已是完整的"必须为正"校验(负数在类型层不可能)。
	if unitPrice == 0 {
		return errcode.New(errcode.ErrInvalidArg, "unit_price must be positive")
	}
	if verr := data.ValidateCurrencyKind(kind); verr != nil {
		return verr
	}
	totalGold, ok := data.SafeMulCurrency(unitPrice, uint64(quantity))
	if !ok || totalGold == 0 {
		return errcode.New(errcode.ErrInvalidArg, "settle amount overflow match=%d price=%d qty=%d", matchID, unitPrice, quantity)
	}
	idempotencyKey := fmt.Sprintf("auction:settle:%d", matchID)
	detail := fmt.Sprintf("auction settle match=%d item=%d qty=%d gold=%d", matchID, itemConfigID, quantity, totalGold)
	already, err := u.repo.SettleAuctionMatch(ctx, matchID, sellerID, buyerID, sellOrderID, buyOrderID, itemConfigID, quantity, kind, totalGold, idempotencyKey, detail)
	if err != nil {
		return err
	}
	if already {
		plog.With(ctx).Infow("msg", "auction_settle_idempotent_hit",
			"match_id", matchID, "seller_id", sellerID, "buyer_id", buyerID, "item", itemConfigID, "qty", quantity, "gold", totalGold)
	}
	// 分片:成交对转成功后观测本笔的跨分片落点(买卖双方背包跨 Cell → 跨分片对转,
	// 拆 Kafka 结算出箱幂等消费)。router 为 nil(单 Cell)→ 不打。
	u.logAuctionSettlementRouting(ctx, matchID, sellerID, buyerID)
	return nil
}

// SettlePlayerTrade 原子结算一笔玩家间点对点交易(系统驱动,幂等键基于 order_id)。
//
// 在 inventory 一个本地事务里从双方活跃余额扣转完成对转(无 escrow 预冻结):
//   - 卖家:交付 sellerItems 给买家,收 buyerItems + price 金币;
//   - 买家:交付 buyerItems + price 金币给卖家,收 sellerItems。
//
// 与拍卖不同,P2P 无预冻结,任一方资产不足 → ErrInventoryInsufficient,整笔回滚(成交失败)。
// 幂等键 = "trade:settle:<order_id>",同一订单重复结算只生效一次(不变量 §9.7)。
func (u *InventoryUsecase) SettlePlayerTrade(ctx context.Context, orderID, sellerID, buyerID uint64, sellerItems, buyerItems []data.ItemGrant, kind data.CurrencyKind, price uint64) error {
	if orderID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "order_id required")
	}
	if sellerID == 0 || buyerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "seller_id / buyer_id required")
	}
	if sellerID == buyerID {
		// 自交易净额为零且会让同一玩家同 key 写两条流水冲突;视为非法。
		return errcode.New(errcode.ErrInvalidArg, "seller and buyer must differ: %d", sellerID)
	}
	// price 是 uint64,不可能为负;真正要校验的是"有钱就必须说清楚是哪种钱"。
	if price > 0 {
		if verr := data.ValidateCurrencyKind(kind); verr != nil {
			return verr
		}
	}
	if len(sellerItems) == 0 && len(buyerItems) == 0 && price == 0 {
		return errcode.New(errcode.ErrInvalidArg, "nothing to settle")
	}
	validate := func(items []data.ItemGrant, side string) error {
		for _, it := range items {
			if it.ItemConfigID == 0 {
				return errcode.New(errcode.ErrInvalidArg, "%s item_config_id required", side)
			}
			if it.Count <= 0 {
				return errcode.New(errcode.ErrInvalidArg, "%s count must be positive: item=%d", side, it.ItemConfigID)
			}
		}
		return nil
	}
	if err := validate(sellerItems, "seller"); err != nil {
		return err
	}
	if err := validate(buyerItems, "buyer"); err != nil {
		return err
	}
	idempotencyKey := fmt.Sprintf("trade:settle:%d", orderID)
	detail := fmt.Sprintf("trade settle order=%d seller_items=%d buyer_items=%d price=%d",
		orderID, len(sellerItems), len(buyerItems), price)
	already, err := u.repo.SettlePlayerTrade(ctx, orderID, sellerID, buyerID, sellerItems, buyerItems, kind, price, idempotencyKey, detail)
	if err != nil {
		return err
	}
	if already {
		plog.With(ctx).Infow("msg", "trade_settle_idempotent_hit",
			"order_id", orderID, "seller_id", sellerID, "buyer_id", buyerID, "price", price)
	}
	return nil
}

// FreezeForOrder 拍卖挂单冻结资产(系统驱动,幂等键 = order_id)。
//
// SELL:冻结 quantity 个 itemConfigID(卖家下架道具);BUY:冻结 quantity*unitPrice 金币(买家锁价)。
// 资产移入 escrow,挂单期间不可被别处消耗;道具 / 金币不足 → ErrInventoryInsufficient。
func (u *InventoryUsecase) FreezeForOrder(ctx context.Context, playerID, orderID uint64, side EscrowSide, itemConfigID uint32, quantity int64, currencyKind data.CurrencyKind, unitPrice uint64) error {
	if playerID == 0 || orderID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id / order_id required")
	}
	if itemConfigID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "item_config_id required")
	}
	if quantity <= 0 {
		return errcode.New(errcode.ErrInvalidArg, "quantity must be positive")
	}
	// unitPrice 是 uint64:`== 0` 已是完整校验(负数在类型层不可能)。
	if unitPrice == 0 {
		return errcode.New(errcode.ErrInvalidArg, "unit_price must be positive")
	}
	var (
		kind         data.EscrowKind
		frozenAmount uint64
	)
	switch side {
	case EscrowSideSell:
		kind = data.EscrowKindItem
	case EscrowSideBuy:
		kind = data.EscrowKindCurrency
		if verr := data.ValidateCurrencyKind(currencyKind); verr != nil {
			return verr
		}
		g, ok := data.SafeMulCurrency(unitPrice, uint64(quantity))
		if !ok || g == 0 {
			return errcode.New(errcode.ErrInvalidArg, "freeze amount overflow order=%d price=%d qty=%d", orderID, unitPrice, quantity)
		}
		frozenAmount = g
	default:
		return errcode.New(errcode.ErrInvalidArg, "unknown escrow side %d", side)
	}
	already, err := u.repo.FreezeForOrder(ctx, playerID, orderID, kind, itemConfigID, quantity, currencyKind, frozenAmount)
	if err != nil {
		return err
	}
	if already {
		plog.With(ctx).Infow("msg", "auction_freeze_idempotent_hit",
			"player_id", playerID, "order_id", orderID, "side", side, "item", itemConfigID, "qty", quantity)
	}
	return nil
}

// EnsureAuctionEscrow 为旧版本已进入订单状态机、但可能没有成功冻结资产的订单补齐 escrow。
// 已有满足剩余量的 active escrow 只校验不重复扣；缺失时由 repo 在一个 MySQL 事务中补冻并建行。
func (u *InventoryUsecase) EnsureAuctionEscrow(
	ctx context.Context,
	playerID, orderID uint64,
	side EscrowSide,
	itemConfigID uint32,
	remainingQuantity uint64,
	currencyKind data.CurrencyKind,
	unitPrice uint64,
) error {
	if playerID == 0 || orderID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id / order_id required")
	}
	if itemConfigID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "item_config_id required")
	}
	if remainingQuantity == 0 || unitPrice == 0 {
		return errcode.New(errcode.ErrInvalidArg, "remaining_quantity / unit_price must be positive")
	}
	// 数量仍走有符号 int64(见 CLAUDE.md §5.12 例外:quantity 一族参与 Remaining() 减法,
	// 不随货币一起改无符号),所以这里只钳数量,**不再钳价格** ——
	// 价格从入口到落库全程 uint64,没有任何一处强转回 int64,自然也不需要"防强转变负"的闸。
	// 旧实现同时钳了两者;若只删闸不删强转,超过 MaxInt64 的价格会被转成负数,
	// 一路穿到扣款处让 `have < n` 恒 false,把扣钱变成加钱(测绘报告 R5)。
	const maxInt64AsUint64 = uint64(^uint64(0) >> 1)
	if remainingQuantity > maxInt64AsUint64 {
		return errcode.New(errcode.ErrInvalidArg,
			"ensure escrow quantity exceeds int64 order=%d remaining=%d", orderID, remainingQuantity)
	}

	var kind data.EscrowKind
	switch side {
	case EscrowSideSell:
		kind = data.EscrowKindItem
	case EscrowSideBuy:
		kind = data.EscrowKindCurrency
		if verr := data.ValidateCurrencyKind(currencyKind); verr != nil {
			return verr
		}
		if _, ok := data.SafeMulCurrency(unitPrice, remainingQuantity); !ok {
			return errcode.New(errcode.ErrInvalidArg,
				"ensure escrow amount overflow order=%d price=%d remaining=%d",
				orderID, unitPrice, remainingQuantity)
		}
	default:
		return errcode.New(errcode.ErrInvalidArg, "unknown escrow side %d", side)
	}

	already, err := u.repo.EnsureAuctionEscrow(ctx, playerID, orderID, kind, itemConfigID,
		int64(remainingQuantity), currencyKind, unitPrice)
	if err != nil {
		return err
	}
	if already {
		plog.With(ctx).Infow("msg", "auction_ensure_escrow_idempotent_hit",
			"player_id", playerID, "order_id", orderID, "side", side,
			"item", itemConfigID, "remaining", remainingQuantity)
	}
	return nil
}

// ReleaseEscrow 退还某挂单 escrow 残余资产到玩家活跃余额(撤单 / 过期 / 完全成交后,幂等键 = order_id)。
func (u *InventoryUsecase) ReleaseEscrow(ctx context.Context, playerID, orderID uint64) error {
	if playerID == 0 || orderID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id / order_id required")
	}
	already, err := u.repo.ReleaseEscrow(ctx, playerID, orderID)
	if err != nil {
		return err
	}
	if already {
		plog.With(ctx).Infow("msg", "auction_release_noop", "player_id", playerID, "order_id", orderID)
	}
	return nil
}

// EscrowSide 是 biz 层冻结方向(对齐 proto EscrowSide:SELL=1 / BUY=2)。
type EscrowSide int32

const (
	// EscrowSideSell 卖单:冻道具。
	EscrowSideSell EscrowSide = 1
	// EscrowSideBuy 买单:冻金币。
	EscrowSideBuy EscrowSide = 2
)

// safeMulInt64 做溢出安全的 int64 乘法(a,b 均已保证为正)。溢出返回 (0, false)。
func safeMulInt64(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	c := a * b
	if c/b != a {
		return 0, false
	}
	return c, true
}

// MaxCheckItemsOwned 是一次 CheckItemsOwned 可查询的道具配置数上限(§9.18 读取侧上限)。
//
// 唯一调用方 player.SetEquipment 一次最多提交装备部位数量个配置(个位数),
// 取 64 留足余量;超限直接拒而不是静默截断——截断会把「未查」伪装成「未持有」,
// 让拥有权校验在超长请求下静默放行或静默误拒。
const MaxCheckItemsOwned = 64

// CheckItemsOwned 批量查询玩家持有情况,返回入参集合中**确实持有**的子集(去重,升序)。
//
// 「持有」= 可堆叠计数 > 0(player_items)或存在该配置的装备实例(player_item_instance),
// 两条路任一成立即算持有:装备走实例模型,消耗品走堆叠计数,调用方不必关心底层形态。
//
// 实现刻意复用既有 GetInventory / ListInstances 而不新写一条定向 SQL(§15.2 最少复杂度):
// 单玩家背包被容量与堆叠行数有界,而本接口只在大厅态改出战预设时调用,属低频路径。
func (u *InventoryUsecase) CheckItemsOwned(ctx context.Context, playerID uint64, itemConfigIDs []uint32) ([]uint32, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if len(itemConfigIDs) == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "item_config_ids required")
	}
	if len(itemConfigIDs) > MaxCheckItemsOwned {
		return nil, errcode.New(errcode.ErrInvalidArg,
			"too many item_config_ids: %d > %d", len(itemConfigIDs), MaxCheckItemsOwned)
	}
	want := make(map[uint32]bool, len(itemConfigIDs))
	for _, id := range itemConfigIDs {
		if id == 0 {
			return nil, errcode.New(errcode.ErrInvalidArg, "item_config_id required")
		}
		want[id] = true
	}

	_, items, err := u.repo.GetInventory(ctx, playerID)
	if err != nil {
		return nil, err
	}
	owned := make(map[uint32]bool, len(want))
	for _, it := range items {
		if it.Count > 0 && want[it.ItemConfigID] {
			owned[it.ItemConfigID] = true
		}
	}

	// 未启用实例背包时不读 player_item_instance(既有库可能尚未迁移出该表,
	// 与 GetInventoryFull 同一条件),此时装备类道具只能靠堆叠计数判定。
	if u.cfg.Capacity > 0 {
		instances, ierr := u.repo.ListInstances(ctx, playerID)
		if ierr != nil {
			return nil, ierr
		}
		for _, inst := range instances {
			if want[inst.ItemConfigID] {
				owned[inst.ItemConfigID] = true
			}
		}
	}

	result := make([]uint32, 0, len(owned))
	for id := range owned {
		result = append(result, id)
	}
	// map 遍历顺序不稳定,定序返回,让调用方与用例拿到确定结果。
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}
