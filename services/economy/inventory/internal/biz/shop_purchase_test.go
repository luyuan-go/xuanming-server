// shop_purchase_test.go — NPC 商店购买业务规则单测(2026-08-22 多币种改造新增链路)。
//
// 覆盖的是**无符号化之后最容易静默出错的几处**:
//   - 单价 × 份数的溢出闸(uint64 乘法不会报错,只会回绕);
//   - 币种 fail-closed(UNSPECIFIED 绝不回退成金币,否则配错表就按金币扣钱);
//   - 幂等重放回放**同一笔扣费额**而不是 0(响应丢失后客户端要看到同样的账);
//   - 份数上限与"商店不卖这个"的错误码区分。
package biz

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"

	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/data"
)

// mapShopCatalog 是 ShopCatalog 的内存实现:shopID → itemConfigID → 档位。
type mapShopCatalog map[uint32]map[uint32]ShopEntry

func (c mapShopCatalog) ListShop(shopID uint32) ([]ShopEntry, bool) {
	m, ok := c[shopID]
	if !ok {
		return nil, false
	}
	out := make([]ShopEntry, 0, len(m))
	for _, e := range m {
		out = append(out, e)
	}
	return out, true
}

func (c mapShopCatalog) LookupShopEntry(shopID, itemConfigID uint32) (ShopEntry, bool) {
	m, ok := c[shopID]
	if !ok {
		return ShopEntry{}, false
	}
	e, ok := m[itemConfigID]
	return e, ok
}

// newShopUC 构造带商店表 + 道具表 + 实例背包的 usecase。
// 6001 可堆叠(每份 10 个,金币 100/份);6002 是装备(每份 1 件,钻石 50/份)。
func newShopUC(repo *fakeRepo, shops mapShopCatalog) *InventoryUsecase {
	uc := NewInventoryUsecase(repo, conf.InventoryConf{Capacity: 4})
	uc.SetItemCatalog(mapItemCatalog{
		6001: {MaxStack: 99},
		6002: {Equipment: true, MaxStack: 1},
	})
	uc.SetSnowflake(&seqGen{})
	uc.SetShopCatalog(shops)
	return uc
}

func defaultShops() mapShopCatalog {
	return mapShopCatalog{
		1: {
			6001: {ItemConfigID: 6001, CountPerUnit: 10, CurrencyKind: data.CurrencyGold, UnitPrice: 100},
			6002: {ItemConfigID: 6002, CountPerUnit: 1, CurrencyKind: data.CurrencyDiamond, UnitPrice: 50},
		},
	}
}

// seedWallet 直接往假仓钱包塞钱(绕开 GrantItems 的单笔上限,专注测购买本身)。
func seedWallet(repo *fakeRepo, playerID uint64, kind data.CurrencyKind, amount uint64) {
	repo.setBalance(playerID, kind, amount)
}

func TestPurchaseShopItem_StackableChargesAndGrants(t *testing.T) {
	repo := newFakeRepo()
	uc := newShopUC(repo, defaultShops())
	seedWallet(repo, 100, data.CurrencyGold, 1000)
	ctx := context.Background()

	out, err := uc.PurchaseShopItem(ctx, 100, 1, 6001, 3, "buy-1")
	if err != nil {
		t.Fatalf("购买: %v", err)
	}
	// 3 份 × 单价 100 = 300;3 份 × 每份 10 个 = 30 个道具。
	if out.Cost != 300 || out.Kind != data.CurrencyGold {
		t.Fatalf("扣费额/币种不符: cost=%d kind=%v", out.Cost, out.Kind)
	}
	if got := out.Balances.Get(data.CurrencyGold); got != 700 {
		t.Fatalf("扣费后余额=%d want=700", got)
	}
	if len(out.Items) != 1 || out.Items[0].ItemConfigID != 6001 || out.Items[0].Count != 30 {
		t.Fatalf("入包道具不符: %+v", out.Items)
	}
	if repo.items[100][6001] != 30 {
		t.Fatalf("背包实际数量=%d want=30", repo.items[100][6001])
	}
}

// TestPurchaseShopItem_IdempotentReplaysSameCost 幂等重放必须回放**同一笔扣费额**。
// 只断言"没有二次扣钱"是不够的:Cost 回放成 0 时余额同样不会变,但客户端会显示
// "这次没花钱",与首次响应自相矛盾。
func TestPurchaseShopItem_IdempotentReplaysSameCost(t *testing.T) {
	repo := newFakeRepo()
	uc := newShopUC(repo, defaultShops())
	seedWallet(repo, 100, data.CurrencyGold, 1000)
	ctx := context.Background()

	first, err := uc.PurchaseShopItem(ctx, 100, 1, 6001, 2, "buy-dup")
	if err != nil {
		t.Fatalf("首次购买: %v", err)
	}
	replay, err := uc.PurchaseShopItem(ctx, 100, 1, 6001, 2, "buy-dup")
	if err != nil {
		t.Fatalf("重放: %v", err)
	}
	if replay.Cost != first.Cost || replay.Cost != 200 {
		t.Fatalf("重放扣费额必须等于首次 200: first=%d replay=%d", first.Cost, replay.Cost)
	}
	if got := replay.Balances.Get(data.CurrencyGold); got != 800 {
		t.Fatalf("重放余额快照=%d want=800", got)
	}
	// 发货事实也必须回放,不能重复发货。
	if len(replay.Items) != 1 || replay.Items[0].Count != 20 {
		t.Fatalf("重放发货事实不符: %+v", replay.Items)
	}
	if repo.items[100][6001] != 20 {
		t.Fatalf("重放不得二次发货: 背包=%d want=20", repo.items[100][6001])
	}
	if got := repo.goldOf(100); got != 800 {
		t.Fatalf("重放不得二次扣费: 余额=%d want=800", got)
	}
}

// TestPurchaseShopItem_NonGoldCurrencyIsolated 钻石商品只动钻石,不碰金币。
func TestPurchaseShopItem_NonGoldCurrencyIsolated(t *testing.T) {
	repo := newFakeRepo()
	uc := newShopUC(repo, defaultShops())
	seedWallet(repo, 100, data.CurrencyGold, 1000)
	seedWallet(repo, 100, data.CurrencyDiamond, 200)
	ctx := context.Background()

	out, err := uc.PurchaseShopItem(ctx, 100, 1, 6002, 2, "buy-eq")
	if err != nil {
		t.Fatalf("购买装备: %v", err)
	}
	if out.Kind != data.CurrencyDiamond || out.Cost != 100 {
		t.Fatalf("应扣钻石 100: kind=%v cost=%d", out.Kind, out.Cost)
	}
	if got := repo.balanceOf(100, data.CurrencyDiamond); got != 100 {
		t.Fatalf("钻石余额=%d want=100", got)
	}
	if got := repo.goldOf(100); got != 1000 {
		t.Fatalf("钻石消费不得动金币: gold=%d want=1000", got)
	}
	// 装备按份生成独立未鉴定实例(与掉落 / 活动发放同一实例模型)。
	if len(out.Instances) != 2 {
		t.Fatalf("应生成 2 件实例, got %d", len(out.Instances))
	}
	for _, inst := range out.Instances {
		if inst.Identified {
			t.Fatalf("买来的装备必须未鉴定: %+v", inst)
		}
		if inst.ItemConfigID != 6002 {
			t.Fatalf("实例 config 不符: %+v", inst)
		}
	}
}

// TestPurchaseShopItem_Rejections 各类拒绝路径:全部不得扣钱。
func TestPurchaseShopItem_Rejections(t *testing.T) {
	ctx := context.Background()

	t.Run("商店不卖这个", func(t *testing.T) {
		repo := newFakeRepo()
		uc := newShopUC(repo, defaultShops())
		seedWallet(repo, 100, data.CurrencyGold, 1000)
		if _, err := uc.PurchaseShopItem(ctx, 100, 1, 9999, 1, "k"); errcode.As(err) != errcode.ErrInventoryNotPurchasable {
			t.Fatalf("未上架应 ErrInventoryNotPurchasable, got %v", err)
		}
		if repo.goldOf(100) != 1000 {
			t.Fatalf("拒绝不得扣钱: %d", repo.goldOf(100))
		}
	})

	t.Run("商店不存在与不上架同码", func(t *testing.T) {
		repo := newFakeRepo()
		uc := newShopUC(repo, defaultShops())
		// 刻意不区分:区分开会把"哪些商店存在"变成可探测信息。
		if _, err := uc.PurchaseShopItem(ctx, 100, 777, 6001, 1, "k"); errcode.As(err) != errcode.ErrInventoryNotPurchasable {
			t.Fatalf("商店不存在应同码, got %v", err)
		}
	})

	t.Run("份数超单次上限", func(t *testing.T) {
		repo := newFakeRepo()
		uc := newShopUC(repo, defaultShops())
		seedWallet(repo, 100, data.CurrencyGold, 1<<40)
		// 默认上限 999。
		if _, err := uc.PurchaseShopItem(ctx, 100, 1, 6001, 1000, "k"); errcode.As(err) != errcode.ErrInventoryNotPurchasable {
			t.Fatalf("超份数上限应拒, got %v", err)
		}
		if repo.goldOf(100) != 1<<40 {
			t.Fatalf("拒绝不得扣钱: %d", repo.goldOf(100))
		}
	})

	t.Run("份数为0", func(t *testing.T) {
		repo := newFakeRepo()
		uc := newShopUC(repo, defaultShops())
		if _, err := uc.PurchaseShopItem(ctx, 100, 1, 6001, 0, "k"); errcode.As(err) != errcode.ErrInvalidArg {
			t.Fatalf("份数 0 应 ErrInvalidArg, got %v", err)
		}
	})

	t.Run("余额不足", func(t *testing.T) {
		repo := newFakeRepo()
		uc := newShopUC(repo, defaultShops())
		seedWallet(repo, 100, data.CurrencyGold, 99)
		if _, err := uc.PurchaseShopItem(ctx, 100, 1, 6001, 1, "k"); errcode.As(err) != errcode.ErrInventoryInsufficient {
			t.Fatalf("余额不足应拒, got %v", err)
		}
		if repo.goldOf(100) != 99 || repo.items[100][6001] != 0 {
			t.Fatalf("余额不足必须整笔回滚: gold=%d items=%d", repo.goldOf(100), repo.items[100][6001])
		}
	})

	t.Run("未配置商店表fail-closed", func(t *testing.T) {
		repo := newFakeRepo()
		uc := NewInventoryUsecase(repo, conf.InventoryConf{Capacity: 4})
		uc.SetItemCatalog(mapItemCatalog{6001: {MaxStack: 99}})
		// 不注入 ShopCatalog:绝不能"没有表就按道具表售价当买入价"兜底。
		if _, err := uc.PurchaseShopItem(ctx, 100, 1, 6001, 1, "k"); errcode.As(err) != errcode.ErrInternal {
			t.Fatalf("未注入商店表应 fail-closed, got %v", err)
		}
		if _, err := uc.GetShop(ctx, 1); errcode.As(err) != errcode.ErrInternal {
			t.Fatalf("未注入商店表 GetShop 应 fail-closed, got %v", err)
		}
	})

	t.Run("未知币种绝不回退成金币", func(t *testing.T) {
		repo := newFakeRepo()
		shops := defaultShops()
		e := shops[1][6001]
		e.CurrencyKind = 0 // UNSPECIFIED:配错表的典型形态
		shops[1][6001] = e
		uc := newShopUC(repo, shops)
		seedWallet(repo, 100, data.CurrencyGold, 1000)
		if _, err := uc.PurchaseShopItem(ctx, 100, 1, 6001, 1, "k"); errcode.As(err) != errcode.ErrInvalidArg {
			t.Fatalf("未知币种应 ErrInvalidArg, got %v", err)
		}
		if repo.goldOf(100) != 1000 {
			t.Fatalf("未知币种绝不能按金币扣: %d", repo.goldOf(100))
		}
	})

	t.Run("单价乘份数溢出", func(t *testing.T) {
		repo := newFakeRepo()
		shops := defaultShops()
		e := shops[1][6001]
		e.UnitPrice = data.MaxCurrencyAmount // 再乘任何 >1 的份数都越界
		shops[1][6001] = e
		uc := newShopUC(repo, shops)
		seedWallet(repo, 100, data.CurrencyGold, data.MaxCurrencyAmount)
		if _, err := uc.PurchaseShopItem(ctx, 100, 1, 6001, 2, "k"); errcode.As(err) != errcode.ErrInvalidArg {
			t.Fatalf("总价溢出应拒(不得回绕成小额), got %v", err)
		}
		if repo.goldOf(100) != data.MaxCurrencyAmount {
			t.Fatalf("溢出拒绝不得扣钱: %d", repo.goldOf(100))
		}
	})
}

// TestGetShopReturnsAuthoritativePriceList GetShop 与扣费同源:返回的单价就是实扣单价。
func TestGetShopReturnsAuthoritativePriceList(t *testing.T) {
	repo := newFakeRepo()
	uc := newShopUC(repo, defaultShops())
	ctx := context.Background()

	entries, err := uc.GetShop(ctx, 1)
	if err != nil {
		t.Fatalf("GetShop: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("应返回 2 档商品, got %d", len(entries))
	}
	byID := map[uint32]ShopEntry{}
	for _, e := range entries {
		byID[e.ItemConfigID] = e
	}
	if got := byID[6001]; got.UnitPrice != 100 || got.CurrencyKind != data.CurrencyGold || got.CountPerUnit != 10 {
		t.Fatalf("6001 档位不符: %+v", got)
	}
	if got := byID[6002]; got.UnitPrice != 50 || got.CurrencyKind != data.CurrencyDiamond {
		t.Fatalf("6002 档位不符: %+v", got)
	}
	// 展示价与实扣价必须一致(§17.3:客户端本地算价会随改表静默漂移)。
	seedWallet(repo, 100, data.CurrencyGold, 1000)
	out, err := uc.PurchaseShopItem(ctx, 100, 1, 6001, 1, "same-source")
	if err != nil {
		t.Fatalf("购买: %v", err)
	}
	if out.Cost != byID[6001].UnitPrice {
		t.Fatalf("实扣 %d 与展示单价 %d 不一致", out.Cost, byID[6001].UnitPrice)
	}
	if _, err := uc.GetShop(ctx, 777); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("不存在的商店应 ErrInvalidArg, got %v", err)
	}
}
