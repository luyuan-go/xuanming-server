// currency.go — 多币种货币与 NPC 商店的业务规则(2026-08-22)。
//
// 本文件只放"规则",不放 SQL:余额怎么加减、锁怎么拿、幂等怎么落,全在 data 层。
// 这里负责的是:币种取哪个、价格从哪张表读、份数上限、溢出闸、以及"不可出售 / 不在售"
// 这类由配置裁决的业务判定。
package biz

import (
	"context"
	"fmt"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"

	"github.com/luyuancpp/pandora/services/economy/inventory/internal/data"
)

const (
	// defaultMaxCurrencyPerGrant 单次发放某币种的默认上限(1e12)。
	// 取值依据:即便全服每人每天入账 100 万,单笔也到不了 1e12;
	// 真到这个量级只可能是上游算错(见 conf.MaxCurrencyPerGrant 注释)。
	defaultMaxCurrencyPerGrant uint64 = 1_000_000_000_000

	// defaultMaxShopUnitsPerPurchase 单次商店购买的默认份数上限。
	defaultMaxShopUnitsPerPurchase uint32 = 999
)

// sellCurrencyKind 返回出售道具的结算币种(配置留空 → 金币)。
func (u *InventoryUsecase) sellCurrencyKind() data.CurrencyKind {
	if u.cfg.SellCurrencyKind == 0 {
		return data.CurrencyGold
	}
	return data.CurrencyKind(u.cfg.SellCurrencyKind)
}

// maxCurrencyPerGrant 返回单次发放某币种的上限。
func (u *InventoryUsecase) maxCurrencyPerGrant() uint64 {
	if u.cfg.MaxCurrencyPerGrant == 0 {
		return defaultMaxCurrencyPerGrant
	}
	return u.cfg.MaxCurrencyPerGrant
}

// maxShopUnitsPerPurchase 返回单次商店购买份数上限。
func (u *InventoryUsecase) maxShopUnitsPerPurchase() uint32 {
	if u.cfg.MaxShopUnitsPerPurchase == 0 {
		return defaultMaxShopUnitsPerPurchase
	}
	return u.cfg.MaxShopUnitsPerPurchase
}

// ── NPC 商店 ────────────────────────────────────────────────────────────────

// ShopEntry 是商店表投影出来的一档商品(biz 只取自己要用的列,不把整行配置外露)。
type ShopEntry struct {
	ItemConfigID uint32
	CountPerUnit uint32
	CurrencyKind data.CurrencyKind
	UnitPrice    uint64
	SortOrder    uint32
}

// ShopCatalog 是商店表的只读热更视图(生产实现 = configtable shop 表适配器)。
//
// 与 ItemCatalog 同一纪律:未注入即 fail-closed —— 一份可能与客户端漂移的兜底价格
// 参与扣钱,比拒掉一次购买危险得多。
type ShopCatalog interface {
	// ListShop 返回某商店的全部在售商品(按 sort_order、再按 item_config_id 升序)。
	// ok=false 表示该商店不存在。
	ListShop(shopID uint32) (entries []ShopEntry, ok bool)
	// LookupShopEntry 精确取某商店里某道具的档位。ok=false 表示该商店不卖这个。
	LookupShopEntry(shopID, itemConfigID uint32) (entry ShopEntry, ok bool)
}

// SetShopCatalog 注入商店表。不注入 → GetShop / PurchaseShopItem 一律拒。
func (u *InventoryUsecase) SetShopCatalog(c ShopCatalog) { u.shops = c }

// GetShop 读某商店的权威价目表(客户端据此渲染,与扣费同源)。
func (u *InventoryUsecase) GetShop(ctx context.Context, shopID uint32) ([]ShopEntry, error) {
	if shopID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "shop_id required")
	}
	if u.shops == nil {
		return nil, errcode.New(errcode.ErrInternal, "shop catalog not configured")
	}
	entries, ok := u.shops.ListShop(shopID)
	if !ok {
		return nil, errcode.New(errcode.ErrInvalidArg, "shop not found: %d", shopID)
	}
	return entries, nil
}

// PurchaseShopItem 服务端权威的 NPC 商店购买。
//
// 定价链:商店表 (shop_id, item_config_id) → unit_price × unit_count = 总价。
// **客户端不传价格**,所以没有"客户端报价与服务端不一致"这个失败模式(§17.2)。
//
// 装备类商品每份生成一件独立未鉴定实例:与掉落 / 活动发放同一套实例模型,
// 买来的装备照样要鉴定,不走捷径。
func (u *InventoryUsecase) PurchaseShopItem(
	ctx context.Context,
	playerID uint64,
	shopID, itemConfigID, unitCount uint32,
	idempotencyKey string,
) (data.PurchaseOutcome, error) {
	if playerID == 0 {
		return data.PurchaseOutcome{}, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if shopID == 0 || itemConfigID == 0 {
		return data.PurchaseOutcome{}, errcode.New(errcode.ErrInvalidArg, "shop_id and item_config_id required")
	}
	if unitCount == 0 {
		return data.PurchaseOutcome{}, errcode.New(errcode.ErrInvalidArg, "unit_count must be positive")
	}
	if unitCount > u.maxShopUnitsPerPurchase() {
		return data.PurchaseOutcome{}, errcode.New(errcode.ErrInventoryNotPurchasable,
			"unit_count exceeds per-purchase limit: got=%d limit=%d", unitCount, u.maxShopUnitsPerPurchase())
	}
	if idempotencyKey == "" {
		return data.PurchaseOutcome{}, errcode.New(errcode.ErrInvalidArg, "idempotency_key required")
	}
	if u.shops == nil {
		return data.PurchaseOutcome{}, errcode.New(errcode.ErrInternal, "shop catalog not configured")
	}

	entry, ok := u.shops.LookupShopEntry(shopID, itemConfigID)
	if !ok {
		// 商店不存在、或该商店不卖这件东西 —— 对客户端是同一件事:你不能在这里买它。
		// 刻意不区分,避免把"哪些商店存在"变成可探测信息。
		return data.PurchaseOutcome{}, errcode.New(errcode.ErrInventoryNotPurchasable,
			"item not on sale shop=%d item=%d", shopID, itemConfigID)
	}
	if verr := data.ValidateCurrencyKind(entry.CurrencyKind); verr != nil {
		return data.PurchaseOutcome{}, verr
	}
	if entry.UnitPrice == 0 || entry.CountPerUnit == 0 {
		// 表加载期已校验过(shop.proto 要求 required + >=1),走到这里说明适配器有 bug。
		return data.PurchaseOutcome{}, errcode.New(errcode.ErrInternal,
			"invalid shop entry shop=%d item=%d price=%d count=%d",
			shopID, itemConfigID, entry.UnitPrice, entry.CountPerUnit)
	}

	totalCost, ok := data.SafeMulCurrency(entry.UnitPrice, uint64(unitCount))
	if !ok {
		return data.PurchaseOutcome{}, errcode.New(errcode.ErrInvalidArg,
			"purchase cost overflow shop=%d item=%d price=%d units=%d",
			shopID, itemConfigID, entry.UnitPrice, unitCount)
	}

	def, defOK := u.itemDefinition(itemConfigID)
	if !defOK {
		// 商店表的外键校验保证道具存在;这里查不到 = 两张表批次漂移,fail-closed。
		return data.PurchaseOutcome{}, errcode.New(errcode.ErrInternal,
			"shop item missing from item table shop=%d item=%d", shopID, itemConfigID)
	}

	totalUnits, ok := data.SafeMulCurrency(uint64(entry.CountPerUnit), uint64(unitCount))
	if !ok || totalUnits == 0 {
		return data.PurchaseOutcome{}, errcode.New(errcode.ErrInvalidArg,
			"purchase quantity overflow shop=%d item=%d", shopID, itemConfigID)
	}

	req := data.PurchaseRequest{
		ShopID:         shopID,
		ItemConfigID:   itemConfigID,
		UnitCount:      unitCount,
		Kind:           entry.CurrencyKind,
		TotalCost:      totalCost,
		IsEquipment:    def.Equipment,
		Capacity:       u.cfg.Capacity,
		IdempotencyKey: idempotencyKey,
	}
	if def.Equipment {
		if u.sf == nil {
			return data.PurchaseOutcome{}, errcode.New(errcode.ErrInvalidArg,
				"instance inventory disabled: cannot purchase equipment item=%d", itemConfigID)
		}
		if u.cfg.Capacity <= 0 {
			return data.PurchaseOutcome{}, errcode.New(errcode.ErrInventoryCapacityFull,
				"instance inventory disabled (capacity<=0) player=%d", playerID)
		}
		// 这里**刻意没有**按"uint64 最坏 20 位"反推的份数闸(2026-08-24 删)。
		// 那道闸比现实严(现网雪花 17 位,shop=1/item=6002 实际能买 11 份,它只放 9 份),
		// 且挡在 data 的幂等回放之前 —— 已成交订单的重试会被拒死(钱已扣、货已发,
		// 客户端永远拿不到成功回包)。detail 列宽改由 data.claimPurchaseLedger 按
		// **实际编码长度**在回放之后判,超长的新请求照样返回 ErrInventoryNotPurchasable。
		// 每件一个雪花 ID;批量预留一次拿整段,避免逐件 CAS。
		ids := make([]uint64, totalUnits)
		u.sf.GenerateInto(ids)
		req.InstanceIDs = ids
	} else {
		req.TotalItems = int64(totalUnits)
	}

	outcome, already, err := u.repo.PurchaseShopItem(ctx, playerID, req)
	if err != nil {
		return data.PurchaseOutcome{}, err
	}
	if already {
		plog.With(ctx).Infow("msg", "shop_purchase_idempotent_hit",
			"player_id", playerID, "shop_id", shopID, "item", itemConfigID,
			"idempotency_key", idempotencyKey, "cost", outcome.Cost)
	}
	return outcome, nil
}

// ── proto ↔ data 的货币转换(service 层用;放 biz 是为了让转换规则只有一份)────

// BalancesFromProto 把上行的 CurrencyAmount 列表转成 data.Balances。
//
// 这是**唯一**的类型边界:proto 的 amount 已是 uint64(不可能为负),所以这里校验的是
// 语义合法性 —— 币种必须已知、数量必须为正、同一币种不得重复出现。
// 重复币种必须拒而不是相加:客户端/上游发出 {GOLD:10, GOLD:20} 时,"相加成 30"和
// "取后者 20"都是猜,猜错就是发错钱。
func BalancesFromProto(list []*commonv1.CurrencyAmount) (data.Balances, error) {
	out := make(data.Balances, len(list))
	for _, it := range list {
		kind := it.GetKind()
		if verr := data.ValidateCurrencyKind(kind); verr != nil {
			return nil, verr
		}
		if it.GetAmount() == 0 {
			return nil, errcode.New(errcode.ErrInvalidArg, "currency amount must be positive: kind=%d", int32(kind))
		}
		if _, dup := out[kind]; dup {
			return nil, errcode.New(errcode.ErrInvalidArg, "duplicated currency kind: %d", int32(kind))
		}
		out[kind] = it.GetAmount()
	}
	return out, nil
}

// BalancesToProto 把 data.Balances 转成按 kind 升序的下行列表(只含非零项)。
func BalancesToProto(b data.Balances) []*commonv1.CurrencyAmount {
	return b.Sorted()
}

// CurrencyAmountProto 组一个单币种下行值(balance / earned / cost 用)。
func CurrencyAmountProto(kind data.CurrencyKind, amount uint64) *commonv1.CurrencyAmount {
	return &commonv1.CurrencyAmount{Kind: kind, Amount: amount}
}

// describeCurrencies 生成人读摘要(日志用)。
func describeCurrencies(b data.Balances) string {
	out := ""
	for i, it := range b.Sorted() {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprintf("%d:%d", int32(it.GetKind()), it.GetAmount())
	}
	if out == "" {
		return "none"
	}
	return out
}
