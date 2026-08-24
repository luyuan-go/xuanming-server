package main

import (
	"fmt"

	"github.com/luyuancpp/pandora/pkg/configtable"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"

	"github.com/luyuancpp/pandora/services/economy/inventory/internal/biz"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
)

// inventoryCatalogFromStore 每次读取 Store 当前批次，Use/Sell/Grant/Identify 与热更后的
// item 表保持同源。未知 ID 一律返回 ok=false，由 biz fail-closed。
type inventoryCatalogFromStore struct{ store *configtable.Store }

func (c inventoryCatalogFromStore) Lookup(itemConfigID uint32) (biz.ItemDefinition, bool) {
	tables := c.store.Tables()
	if tables == nil || tables.Item == nil {
		return biz.ItemDefinition{}, false
	}
	row, ok := tables.Item.ByID(itemConfigID)
	if !ok {
		return biz.ItemDefinition{}, false
	}
	return biz.ItemDefinition{
		Equipment: row.GetType() == configpb.ItemType_ITEM_TYPE_EQUIPMENT,
		// item.usable 的真实语义是局内 UE GAS 可消费。大厅没有效果派发器，必须
		// fail-closed；BattleUsable 只授权可信战斗事实走内部持久扣减 RPC。
		LobbyUsable:   false,
		BattleUsable:  row.GetUsable(),
		SellUnitPrice: int64(row.GetSellPrice()),
		MaxStack:      row.GetMaxStackSize(),
	}, true
}

// ListShop 返回某商店的在售商品(热更后下一次读商店立即生效)。
// 商店不存在 / 无在售商品 → ok=false,由 biz 翻成"商店不可用"。
func (c inventoryCatalogFromStore) ListShop(shopID uint32) ([]biz.ShopEntry, bool) {
	tables := c.store.Tables()
	if tables == nil || tables.Shop == nil {
		return nil, false
	}
	rows := tables.Shop.ShopEntriesOf(shopID)
	if len(rows) == 0 {
		return nil, false
	}
	out := make([]biz.ShopEntry, 0, len(rows))
	for _, r := range rows {
		out = append(out, shopEntryOf(r))
	}
	return out, true
}

// LookupShopEntry 精确取某商店里某道具的档位(购买定价的唯一来源)。
func (c inventoryCatalogFromStore) LookupShopEntry(shopID, itemConfigID uint32) (biz.ShopEntry, bool) {
	tables := c.store.Tables()
	if tables == nil || tables.Shop == nil {
		return biz.ShopEntry{}, false
	}
	row, ok := tables.Shop.ShopEntryOf(shopID, itemConfigID)
	if !ok {
		return biz.ShopEntry{}, false
	}
	return shopEntryOf(row), true
}

func shopEntryOf(row *configpb.ShopRow) biz.ShopEntry {
	return biz.ShopEntry{
		ItemConfigID: row.GetItemConfigId(),
		CountPerUnit: row.GetCountPerUnit(),
		CurrencyKind: row.GetCurrencyKind(),
		UnitPrice:    row.GetUnitPrice(),
		SortOrder:    row.GetSortOrder(),
	}
}

// IdentifyRule 每次从 Store 当前原子批次读取 item→pool→候选行，热更后下一次鉴定立即
// 使用新规则；已经鉴定并落库的实例不会重 roll。
func (c inventoryCatalogFromStore) IdentifyRule(itemConfigID uint32) (biz.IdentifyDefinition, bool) {
	tables := c.store.Tables()
	if tables == nil || tables.Item == nil || tables.EquipmentAffix == nil {
		return biz.IdentifyDefinition{}, false
	}
	item, ok := tables.Item.ByID(itemConfigID)
	if !ok || item.GetType() != configpb.ItemType_ITEM_TYPE_EQUIPMENT || item.GetIdentifyPoolId() == 0 {
		return biz.IdentifyDefinition{}, false
	}
	rows := tables.EquipmentAffix.ListByPoolId(item.GetIdentifyPoolId())
	if len(rows) == 0 {
		return biz.IdentifyDefinition{}, false
	}
	rule := biz.IdentifyDefinition{AttrCount: int(rows[0].GetAttrCount()), Pool: make([]biz.IdentifyAttrDefinition, 0, len(rows))}
	for _, row := range rows {
		rule.Pool = append(rule.Pool, biz.IdentifyAttrDefinition{
			AttrID: row.GetAttrId(), Weight: int64(row.GetWeight()),
			Min: row.GetMinValue(), Max: row.GetMaxValue(),
		})
	}
	return rule, true
}

// validateInventoryTables 是启动和热更共用的整批门禁。鉴定池和 item 同源发布，
// 不再依赖 YAML 默认池；任何装备缺池、池内语义漂移或未知玩法属性都会拒绝整批切换。
func validateInventoryTables(_ conf.InventoryConf) func(*configtable.Tables) error {
	return func(t *configtable.Tables) error {
		if t == nil || t.Item == nil || t.RoleAttrMap == nil || t.EquipmentAffix == nil ||
			t.EquipmentAttr == nil {
			return fmt.Errorf("item / role_attr_map / equipment_affix / equipment_attr tables required")
		}

		// 装备基础属性表(装备属性表.xlsx)与鉴定词条互补:词条是 per-instance 随机 roll、
		// 走本服务发放;基础属性是 per-config 纯配置、由 DS 与客户端各自查表。服务端不消费
		// 那 6 个数值,但装备域的表漂移门禁在本服务,所以引用完整性一并挡在这里
		// (装备ID 必须是真实装备、部位/品质不得与道具表分叉)。
		if err := configtable.ValidateEquipmentAttrCrossTables(t.EquipmentAttr, t.Item); err != nil {
			return err
		}

		// NPC 商店定价门禁:买入价必须严格高于回收价,否则「买进立刻卖出」就是无限刷钱。
		// 买和卖的权威都在本服务,所以这道闸也必须在本服务的整批门禁里 ——
		// 放在购买期只会挡住个别商品,配错的表照样上线,而刷钱的正是那些"没报错"的档。
		// Shop 表未加载(旧批次 / 未配商店)时跳过:商店打不开是可接受的降级,
		// 但只要表在,就必须整批可信。
		if t.Shop != nil {
			if err := configtable.ValidateShopCrossTables(t.Shop, t.Item); err != nil {
				return err
			}
		}

		// 当前战斗属性映射只为这三类定义了权威单位。新增属性必须先实现 UE 应用/
		// 卸载对账，再改这里放行，不能让配表悄悄造出“只显示不生效”的词条。
		allowedAttrs := map[uint32]string{3: "Atk", 7: "MoveSpeedRate", 9: "Defense"}
		for id, codeName := range allowedAttrs {
			row, ok := t.RoleAttrMap.ByID(id)
			if !ok || row.GetCodeName() != codeName {
				return fmt.Errorf("role_attr_map id %d must be %q for equipment gameplay semantics", id, codeName)
			}
		}

		type poolStats struct {
			attrCount uint32
			attrs     map[uint32]struct{}
			total     int64
		}
		pools := make(map[uint32]*poolStats)
		for _, row := range t.EquipmentAffix.All() {
			if _, ok := allowedAttrs[row.GetAttrId()]; !ok {
				return fmt.Errorf("equipment_affix row %d attr_id %d has no gameplay apply/reconcile semantics",
					row.GetId(), row.GetAttrId())
			}
			stats := pools[row.GetPoolId()]
			if stats == nil {
				stats = &poolStats{attrCount: row.GetAttrCount(), attrs: make(map[uint32]struct{})}
				pools[row.GetPoolId()] = stats
			}
			if stats.attrCount != row.GetAttrCount() {
				return fmt.Errorf("equipment_affix pool %d has inconsistent attr_count %d/%d",
					row.GetPoolId(), stats.attrCount, row.GetAttrCount())
			}
			if _, duplicate := stats.attrs[row.GetAttrId()]; duplicate {
				return fmt.Errorf("equipment_affix pool %d duplicates attr_id %d", row.GetPoolId(), row.GetAttrId())
			}
			stats.attrs[row.GetAttrId()] = struct{}{}
			stats.total += int64(row.GetWeight())
			if stats.total > 1_000_000 {
				return fmt.Errorf("equipment_affix pool %d total weight exceeds 1000000", row.GetPoolId())
			}
		}
		for poolID, stats := range pools {
			if stats.attrCount == 0 || int(stats.attrCount) > len(stats.attrs) {
				return fmt.Errorf("equipment_affix pool %d attr_count %d exceeds unique candidates %d",
					poolID, stats.attrCount, len(stats.attrs))
			}
		}

		referenced := make(map[uint32]struct{})
		for _, item := range t.Item.All() {
			if item.GetType() != configpb.ItemType_ITEM_TYPE_EQUIPMENT {
				continue
			}
			poolID := item.GetIdentifyPoolId()
			if _, ok := pools[poolID]; !ok {
				return fmt.Errorf("equipment item %d references missing identify pool %d", item.GetId(), poolID)
			}
			referenced[poolID] = struct{}{}
		}
		for poolID := range pools {
			if _, ok := referenced[poolID]; !ok {
				return fmt.Errorf("equipment_affix pool %d is orphaned (no item references it)", poolID)
			}
		}
		return nil
	}
}

// itemMaxStacksFromTables 把同源 item.max_stack_size 投影给后端驻留背包段。
// BagConf 目前是启动快照，因此 reload 后改变堆叠上限需滚动重启 inventory；规则值本身
// 不再手抄 YAML，启动时始终取当前发布批次。
func itemMaxStacksFromTables(t *configtable.Tables) []conf.BagItemStackRule {
	out := make([]conf.BagItemStackRule, 0, t.Item.Count())
	for _, row := range t.Item.All() {
		out = append(out, conf.BagItemStackRule{ItemConfigID: row.GetId(), MaxStack: row.GetMaxStackSize()})
	}
	return out
}
