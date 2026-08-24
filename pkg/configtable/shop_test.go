package configtable

import (
	"strings"
	"testing"

	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// shopFixture 造一张最小可用的商店表 + 道具表。
//
// 用合成数据而不是真实 dist:真表测试(realdist_test.go)证明的是"当前数据干净",
// 证明不了"闸门本身有效" —— 后者只能靠故意配错来验。两者都要有。
func shopFixture(t *testing.T, sellPrice uint32, rows []*configpb.ShopRow) (*ShopTable, *ItemTable) {
	t.Helper()
	items, err := newItemTable(&configpb.ItemTableData{Rows: []*configpb.ItemRow{{
		Id: 10001, Name: "压缩口粮", Type: configpb.ItemType_ITEM_TYPE_CONSUMABLE,
		MaxStackSize: 20, SellPrice: sellPrice,
	}}})
	if err != nil {
		t.Fatalf("build item table: %v", err)
	}
	shops, err := newShopTable(&configpb.ShopTableData{Rows: rows})
	if err != nil {
		t.Fatalf("build shop table: %v", err)
	}
	return shops, items
}

func shopRow(id, shopID, itemID, perUnit uint32, price uint64) *configpb.ShopRow {
	return &configpb.ShopRow{
		Id: id, ShopId: shopID, ItemConfigId: itemID, CountPerUnit: perUnit,
		CurrencyKind: commonv1.CurrencyKind_CURRENCY_KIND_GOLD, UnitPrice: price,
	}
}

// TestValidateShopCrossTables_RejectsArbitragePricing 钉死反套利闸。
//
// 失败模式**没有任何运行期信号**:买进立刻卖出的每一笔单独看都合法、都成功、都记流水,
// 异常只体现在总量上,日志和监控都发现不了。所以只能在加载期用配置校验挡。
func TestValidateShopCrossTables_RejectsArbitragePricing(t *testing.T) {
	const sell = uint32(100)

	t.Run("买价高于回收价放行", func(t *testing.T) {
		shops, items := shopFixture(t, sell, []*configpb.ShopRow{shopRow(1, 1, 10001, 1, 101)})
		if err := ValidateShopCrossTables(shops, items); err != nil {
			t.Fatalf("买价 101 > 回收 100,应放行: %v", err)
		}
	})

	t.Run("买价等于回收价必须拒", func(t *testing.T) {
		// 等价不亏,配合任何一点产出就能刷 —— 判据必须是**严格**大于。
		shops, items := shopFixture(t, sell, []*configpb.ShopRow{shopRow(1, 1, 10001, 1, 100)})
		mustShopErr(t, ValidateShopCrossTables(shops, items), "刷钱")
	})

	t.Run("买价低于回收价必须拒", func(t *testing.T) {
		shops, items := shopFixture(t, sell, []*configpb.ShopRow{shopRow(1, 1, 10001, 1, 99)})
		mustShopErr(t, ValidateShopCrossTables(shops, items), "刷钱")
	})

	t.Run("按每份数量折算回收总价", func(t *testing.T) {
		// 10 个装:回收总价 = 10×100 = 1000。单价 999 看着"比 100 大得多",实则亏本卖。
		// 漏乘 count_per_unit 是这条闸最容易写错的地方。
		shops, items := shopFixture(t, sell, []*configpb.ShopRow{shopRow(1, 1, 10001, 10, 999)})
		mustShopErr(t, ValidateShopCrossTables(shops, items), "刷钱")
	})

	t.Run("非金币计价跳过", func(t *testing.T) {
		// 回收统一结算金币,钻石价与金币回收不可比,不存在直接套利路径。
		r := shopRow(1, 1, 10001, 1, 1)
		r.CurrencyKind = commonv1.CurrencyKind_CURRENCY_KIND_DIAMOND
		shops, items := shopFixture(t, sell, []*configpb.ShopRow{r})
		if err := ValidateShopCrossTables(shops, items); err != nil {
			t.Fatalf("非金币计价不该被金币回收价判定: %v", err)
		}
	})
}

// TestValidateShopCrossTables_RejectsDuplicateEntry 钉死"同商店同道具多档"。
//
// 这条的后果同样是静默的:表看着配了,ShopEntryOf 却只能 fail-closed,
// 玩家看到的是"这个东西永远买不了",而服务端没有任何异常。
func TestValidateShopCrossTables_RejectsDuplicateEntry(t *testing.T) {
	shops, items := shopFixture(t, 100, []*configpb.ShopRow{
		shopRow(1, 1, 10001, 1, 400),
		shopRow(2, 1, 10001, 10, 3600), // 同商店同道具第二档
	})
	mustShopErr(t, ValidateShopCrossTables(shops, items), "配了多档")

	// 不同商店卖同一道具是合法的(各卖各价)。
	ok, items2 := shopFixture(t, 100, []*configpb.ShopRow{
		shopRow(1, 1, 10001, 1, 400),
		shopRow(2, 2, 10001, 1, 500),
	})
	if err := ValidateShopCrossTables(ok, items2); err != nil {
		t.Fatalf("不同商店卖同一道具应放行: %v", err)
	}
}

func mustShopErr(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatal("应当被拒,但校验通过了")
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("错误信息应含 %q,实际: %v", want, err)
	}
}
