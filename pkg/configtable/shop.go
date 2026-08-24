package configtable

import (
	"fmt"
	"sort"

	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// shop.go — ShopTable 手写伴生文件。
// 首次由 configtable-gen 创建(仅当文件不存在),此后归人维护,生成器不再覆盖。
// 表私有的逐行业务校验写在 validateShopRow;域方法(业务语义查询)也加在本文件。

// validateShopRow 逐行业务校验(生成的 newShopTable 调用;
// 主键非零/唯一已由生成代码兜住,类型/必填/枚举已由生成器在导表阶段校验,
// 这里只写服务端仍须 fail-closed 的业务约束)。
//
// 这些校验之所以必须在**加载期整批拒**而不是购买时逐次拒:一张配错的商店表
// 会让整个商店页签在玩家面前半死不活(有的商品能买有的报错),
// 而加载期拒批次会保留上一份正确配置继续服务(§9.15 加载成功才切换)。
func validateShopRow(row *configpb.ShopRow) error {
	if row.GetShopId() == 0 {
		return fmt.Errorf("shop_id 必须 > 0")
	}
	if row.GetItemConfigId() == 0 {
		return fmt.Errorf("道具ID 必须 > 0")
	}
	if row.GetCountPerUnit() == 0 {
		return fmt.Errorf("每份数量 必须 >= 1")
	}
	if row.GetUnitPrice() == 0 {
		// 0 价商品等于免费发放,却绕过了发放审计链(GrantItems 的幂等流水与额度闸)。
		// 要送东西请走活动 / 邮件。
		return fmt.Errorf("单价 必须 > 0(免费发放请走活动/邮件,不要配 0 价商品)")
	}
	switch row.GetCurrencyKind() {
	case commonv1.CurrencyKind_CURRENCY_KIND_GOLD,
		commonv1.CurrencyKind_CURRENCY_KIND_DIAMOND,
		commonv1.CurrencyKind_CURRENCY_KIND_HONOR:
	default:
		// UNSPECIFIED / 未知值一律拒,**不回退成金币**:静默回退会让配错的商品
		// 按金币扣钱,是不可观测的经济事故(currency.proto)。
		return fmt.Errorf("货币类型 非法: %d", int32(row.GetCurrencyKind()))
	}
	return nil
}

// ShopEntriesOf 返回某商店的全部在售商品,按「排序 → 道具ID」升序。
//
// 排序在服务端定死而不是让客户端自己排:商品顺序是策划的展示意图,
// 两端各排一次迟早会因为并列项的处理差异而不一致。
func (t *ShopTable) ShopEntriesOf(shopID uint32) []*configpb.ShopRow {
	rows := t.ListByShopId(shopID)
	if len(rows) == 0 {
		return nil
	}
	out := append([]*configpb.ShopRow(nil), rows...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].GetSortOrder() != out[j].GetSortOrder() {
			return out[i].GetSortOrder() < out[j].GetSortOrder()
		}
		return out[i].GetItemConfigId() < out[j].GetItemConfigId()
	})
	return out
}

// ShopEntryOf 精确取某商店里某道具的档位。
//
// 同一商店里同一道具配了多档(例如"单买"与"10 个装")时**返回 false**(fail-closed):
// 购买请求只带 item_config_id,无法区分档位,返回第一档就成了"服务端替玩家选一档",
// 两档价格不同时等于随机扣钱。这种配法由 ValidateShopCrossTables 在加载期整批拒掉,
// 本函数的 fail-closed 只是兜底 —— 真出现说明门禁被绕过了。
func (t *ShopTable) ShopEntryOf(shopID, itemConfigID uint32) (*configpb.ShopRow, bool) {
	var found *configpb.ShopRow
	for _, r := range t.ListByShopId(shopID) {
		if r.GetItemConfigId() != itemConfigID {
			continue
		}
		if found != nil {
			return nil, false
		}
		found = r
	}
	return found, found != nil
}

// ValidateShopCrossTables 校验商店表自身与它对道具表的跨表不变量。
//
// # 约束一:同一商店内同一道具不得配多档
//
// 购买请求只带 (shop_id, item_config_id),没有档位标识。配了两档且价格不同时,
// 服务端无法判断玩家要买哪一档 —— ShopEntryOf 只能 fail-closed,
// 结果是**这个道具在这个商店里永远买不了**,而表面上看表是"配了的",没有任何报错。
// 打包商品(如"药水×10")应当用独立的道具 ID,不要给同一道具配两个单价。
//
// # 约束二:买入价必须严格高于回收价
//
// 商店买入价在 shop 表,出售回收价在道具表 `sell_price`,两者相互独立、谁都能单独改。
// 一旦某档买入价 <= 回收总价,玩家「买进 → 立刻卖出」就净赚或不亏,
// 而买和卖现在**都是服务端权威且都可反复执行**,这就是一条严格闭合的无限刷钱循环。
//
// 两条都必须挡在**加载期**而不是购买期:
//   - 购买期挡只会让个别商品报错,配错的表照样上线,而刷钱的正是那些"没报错"的档;
//   - 加载期整批拒会保留上一份正确配置继续服务(§9.15 加载成功才切换),
//     策划立刻看到失败原因,线上一分钱都刷不出去。
//
// 比较口径是**同一份**的总价:买 1 份得 count_per_unit 个,卖掉这些能得
// count_per_unit × sell_price,所以判据是 unit_price > count_per_unit × sell_price。
//
// 约束二只对**同币种可比**的情况生效:回收统一结算金币(inventory 的 currency.sell_kind,
// 默认金币),所以非金币计价的商品之间不存在直接套利路径,跳过。
// 真要做跨币种兑换那是汇率设计,不能靠这条不变量兜。
func ValidateShopCrossTables(shops *ShopTable, items *ItemTable) error {
	if shops == nil || items == nil {
		return fmt.Errorf("shop / item tables required")
	}

	seen := make(map[[2]uint32]uint32, shops.Count())
	for _, row := range shops.All() {
		key := [2]uint32{row.GetShopId(), row.GetItemConfigId()}
		if prev, dup := seen[key]; dup {
			return fmt.Errorf(
				"商店 %d 的道具 %d 配了多档(shop 行 %d 与 %d):购买请求无法区分档位,"+
					"该道具会永远买不了;打包商品请用独立道具 ID",
				row.GetShopId(), row.GetItemConfigId(), prev, row.GetId())
		}
		seen[key] = row.GetId()

		item, ok := items.ByID(row.GetItemConfigId())
		if !ok {
			// 外键校验理论上已挡住,这里是 fail-closed 兜底。
			return fmt.Errorf("shop %d(商店 %d)引用的道具 %d 在道具表中不存在",
				row.GetId(), row.GetShopId(), row.GetItemConfigId())
		}
		if row.GetCurrencyKind() != commonv1.CurrencyKind_CURRENCY_KIND_GOLD {
			continue // 非金币计价与金币回收不可比,见上方说明
		}
		recycle := uint64(item.GetSellPrice()) * uint64(row.GetCountPerUnit())
		if row.GetUnitPrice() <= recycle {
			return fmt.Errorf(
				"shop %d(商店 %d,道具 %d %s)买入价 %d <= 回收总价 %d(=%d×%d)"+
					":买进立刻卖出即可刷钱,买入价必须严格高于回收价",
				row.GetId(), row.GetShopId(), row.GetItemConfigId(), item.GetName(),
				row.GetUnitPrice(), recycle, item.GetSellPrice(), row.GetCountPerUnit())
		}
	}
	return nil
}
