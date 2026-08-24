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

// newShopUCCap 同 newShopUC,但允许指定实例背包容量(测 detail 列宽边界时要买到十几件,
// 默认容量 4 会先撞背包满,断言就测不到想测的那道闸)。
func newShopUCCap(repo *fakeRepo, shops mapShopCatalog, capacity int32) *InventoryUsecase {
	return newShopUCCapDigits(repo, shops, capacity, 0)
}

// newShopUCCapDigits 再多一个"雪花 id 位数"旋钮:detail 列宽闸按**实际编码长度**判,
// 而默认的 seqGen 只产个位数 id,用它永远撞不到列宽 —— 测边界必须给真实宽度的 id
// (digits=0 表示沿用 seqGen,给不关心 id 宽度的老用例用)。
func newShopUCCapDigits(repo *fakeRepo, shops mapShopCatalog, capacity int32, digits int) *InventoryUsecase {
	uc := NewInventoryUsecase(repo, conf.InventoryConf{Capacity: capacity})
	uc.SetItemCatalog(mapItemCatalog{
		6001: {MaxStack: 99},
		6002: {Equipment: true, MaxStack: 1},
	})
	if digits > 0 {
		uc.SetSnowflake(newDigitsGen(digits))
	} else {
		uc.SetSnowflake(&seqGen{})
	}
	uc.SetShopCatalog(shops)
	return uc
}

// maxEquipUnits 求本档商品一次最多能买几份:直接问生产的编码闸 data.PurchaseDetailFits,
// 不在测试里另抄一套长度公式。注意**份数自己也编在 detail 里**(位数变多会挤占
// instance_id 的预算),所以要连份数一起代入试算。6002 每份 1 件,故份数 == 件数。
func maxEquipUnits(t *testing.T, shopID, itemConfigID uint32, digits int) uint32 {
	t.Helper()
	best := uint32(0)
	for n := uint32(1); n <= 99; n++ {
		if data.PurchaseDetailFits(data.PurchaseRequest{
			ShopID: shopID, ItemConfigID: itemConfigID, UnitCount: n,
			InstanceIDs: idsOfDigits(int(n), digits),
		}) {
			best = n
		}
	}
	if best == 0 {
		t.Fatalf("列容量必须允许至少买 1 件")
	}
	return best
}

// TestPurchaseShopItem_EquipmentUnitsBoundedByLedgerDetail 钉死"一次能买几件装备"这道闸。
//
// 事故背景(2026-08-24):每件装备的雪花 instance_id 都要写进 inventory_ledger.detail
// (VARCHAR(255),幂等回放"到底发了什么"的唯一事实源)。买太多必然撞列宽,
// INSERT 报 Error 1406 被包成 ErrInternal —— 玩家和策划都看不出这是列容量的事。
// 现在由 data.claimPurchaseLedger 按**实际编码长度**判:装得下的份数照买,
// 再多一份返回 ErrInventoryNotPurchasable(与 maxShopUnitsPerPurchase 同口径),且一分钱不扣。
//
// 口径:现网 17 位雪花下,shop=1 / item=6002 一次能买 11 份。
// 上一版在 biz 按 uint64 最坏 20 位反推只放 9 份,把今天能成的 10、11 份改判为拒,
// 是线上功能回退,已删(取舍见 data/inventory_repo.go 的容量预算段)。
func TestPurchaseShopItem_EquipmentUnitsBoundedByLedgerDetail(t *testing.T) {
	ctx := context.Background()
	maxUnits := maxEquipUnits(t, 1, 6002, snowflakeDigitsToday)
	if maxUnits != 11 {
		t.Fatalf("17 位雪花下 shop=1/item=6002 应能买 11 份, got %d", maxUnits)
	}

	t.Run("恰好装得下", func(t *testing.T) {
		repo := newFakeRepo()
		// 容量给够,确保这里只可能被 detail 列宽或 nothing 挡住,不会误撞背包满。
		uc := newShopUCCapDigits(repo, defaultShops(), int32(maxUnits)+4, snowflakeDigitsToday)
		seedWallet(repo, 100, data.CurrencyDiamond, 10000)

		out, err := uc.PurchaseShopItem(ctx, 100, 1, 6002, maxUnits, "buy-max")
		if err != nil {
			t.Fatalf("恰好装得下的份数必须能买: units=%d err=%v", maxUnits, err)
		}
		if len(out.Instances) != int(maxUnits) {
			t.Fatalf("应发 %d 件, got %d", maxUnits, len(out.Instances))
		}
		if got := repo.balanceOf(100, data.CurrencyDiamond); got != 10000-50*uint64(maxUnits) {
			t.Fatalf("扣费额不符: 余额=%d", got)
		}
	})

	t.Run("再多一件被业务错误码拒", func(t *testing.T) {
		repo := newFakeRepo()
		uc := newShopUCCapDigits(repo, defaultShops(), int32(maxUnits)+4, snowflakeDigitsToday)
		seedWallet(repo, 100, data.CurrencyDiamond, 10000)

		_, err := uc.PurchaseShopItem(ctx, 100, 1, 6002, maxUnits+1, "buy-over")
		if errcode.As(err) != errcode.ErrInventoryNotPurchasable {
			// 这里最怕的就是 ErrInternal:那说明闸没生效,又退化成撞列宽。
			t.Fatalf("超 detail 列宽应 ErrInventoryNotPurchasable, got %v", err)
		}
		if got := repo.balanceOf(100, data.CurrencyDiamond); got != 10000 {
			t.Fatalf("被拒不得扣钱: 余额=%d want=10000", got)
		}
		if len(repo.instMap(100)) != 0 {
			t.Fatalf("被拒不得发货: 实例数=%d", len(repo.instMap(100)))
		}
	})
}

// TestPurchaseShopItem_ReplayNeverBlockedByDetailGate 购买侧的"闸必须排在回放之后"。
//
// 与发放侧同构(见 biz/inventory_test.go 的 TestGrantInstances_ReplayNeverBlockedByDetailGate):
// 重试时 biz 会重新生成一批雪花 id,雪花位数涨上去后按新 id 重算 detail 会超列宽。
// 闸若前置成"超长即拒",这笔**钱已扣、货已发**的订单就永远拿不到成功回包,
// 客户端只会一直重试下去。
//
// 回退检验:把 fakeRepo.purchaseShopItemTx 的列宽闸挪到幂等命中判断之前,本用例立刻变红。
func TestPurchaseShopItem_ReplayNeverBlockedByDetailGate(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepo()
	maxUnits := maxEquipUnits(t, 1, 6002, snowflakeDigitsToday)
	seedWallet(repo, 100, data.CurrencyDiamond, 10000)
	const key = "buy-retry"
	capacity := int32(maxUnits) + 4

	first, err := newShopUCCapDigits(repo, defaultShops(), capacity, snowflakeDigitsToday).
		PurchaseShopItem(ctx, 100, 1, 6002, maxUnits, key)
	if err != nil {
		t.Fatalf("恰好达到上限的份数必须能买: units=%d err=%v", maxUnits, err)
	}
	spent := 10000 - 50*uint64(maxUnits)

	// 雪花涨到 20 位后重试:新 id 重算 detail 必然超列宽,但这笔早已成交,必须回放。
	if data.PurchaseDetailFits(data.PurchaseRequest{
		ShopID: 1, ItemConfigID: 6002, UnitCount: maxUnits,
		InstanceIDs: idsOfDigits(int(maxUnits), 20),
	}) {
		t.Fatalf("用例前提不成立:20 位 id 的 %d 份本应超列宽", maxUnits)
	}
	replayed, err := newShopUCCapDigits(repo, defaultShops(), capacity, 20).
		PurchaseShopItem(ctx, 100, 1, 6002, maxUnits, key)
	if err != nil {
		t.Fatalf("已成交订单在雪花涨位后仍必须能回放(否则客户端永远收不到成功回包): %v", err)
	}
	assertSameInstanceIDs(t, first.Instances, replayed.Instances)
	if replayed.Cost != first.Cost {
		t.Fatalf("回放必须回放首次扣费额: got=%d want=%d", replayed.Cost, first.Cost)
	}
	if got := repo.balanceOf(100, data.CurrencyDiamond); got != spent {
		t.Fatalf("回放不得二次扣费: 余额=%d want=%d", got, spent)
	}
	if n := len(repo.instMap(100)); n != int(maxUnits) {
		t.Fatalf("回放不得重复发货: 实例数=%d want=%d", n, maxUnits)
	}
}

// TestPurchaseShopItem_CapacityFullRollsBackCharge 背包满 → 整笔回滚,余额一分没少。
//
// 这条是购买链的原子性本体(§9.7):生产靠单事务 defer tx.Rollback() 撤销已扣的钱。
// 此前假仓在这条路径上**与真实现相反**(先改余额 map、容量满直接 return 不还原),
// 所以这个用例既测业务不变量,也把假仓钉在"失败必回滚"上。
func TestPurchaseShopItem_CapacityFullRollsBackCharge(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepo()
	uc := newShopUCCap(repo, defaultShops(), 4) // 实例背包只有 4 格
	seedWallet(repo, 100, data.CurrencyDiamond, 1000)

	_, err := uc.PurchaseShopItem(ctx, 100, 1, 6002, 5, "buy-full")
	if errcode.As(err) != errcode.ErrInventoryCapacityFull {
		t.Fatalf("背包装不下应 ErrInventoryCapacityFull, got %v", err)
	}
	if got := repo.balanceOf(100, data.CurrencyDiamond); got != 1000 {
		t.Fatalf("背包满必须整笔回滚扣费: 余额=%d want=1000", got)
	}
	if n := len(repo.instMap(100)); n != 0 {
		t.Fatalf("失败购买不得留下半批实例: %d 件", n)
	}
	// 幂等流水也不能留:失败的一笔留下 ledger 行,重试会被当成"已处理"直接回放空结果。
	if _, ok := repo.ledger[keyOf(100, "buy-full")]; ok {
		t.Fatalf("失败购买不得落幂等流水")
	}
	// 回滚后清出格子再买,必须能正常成交(证明上一笔没留下任何残留)。
	out, err := uc.PurchaseShopItem(ctx, 100, 1, 6002, 4, "buy-fit")
	if err != nil {
		t.Fatalf("回滚后重买应成功: %v", err)
	}
	if len(out.Instances) != 4 || repo.balanceOf(100, data.CurrencyDiamond) != 800 {
		t.Fatalf("重买结果不符: 件数=%d 余额=%d", len(out.Instances), repo.balanceOf(100, data.CurrencyDiamond))
	}
}

// TestPurchaseShopItem_SameKeyDifferentRequestConflicts 同幂等键 + 不同请求指纹 = 反作弊冲突。
//
// 这是 data/shop_purchase.go claimPurchaseLedger 的分支:同一个 key 被拿去买"另一笔",
// 绝不能静默当成 no-op 回放(那等于白送一笔),也不能重新执行(那等于凭一个 key 刷两次)。
// 指纹刻意不含价格,所以"策划改了价"不会误判成冲突 —— 这里改的是份数,是真的换了一笔请求。
func TestPurchaseShopItem_SameKeyDifferentRequestConflicts(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepo()
	uc := newShopUC(repo, defaultShops())
	seedWallet(repo, 100, data.CurrencyGold, 1000)

	if _, err := uc.PurchaseShopItem(ctx, 100, 1, 6001, 2, "same-key"); err != nil {
		t.Fatalf("首次购买: %v", err)
	}

	// ① 换份数(2 → 3):指纹变了。
	if _, err := uc.PurchaseShopItem(ctx, 100, 1, 6001, 3, "same-key"); errcode.As(err) != errcode.ErrInventoryIdempotencyConflict {
		t.Fatalf("同键不同份数应判冲突, got %v", err)
	}
	// ② 换商品(6001 → 6002):指纹同样要变。
	seedWallet(repo, 100, data.CurrencyDiamond, 500)
	if _, err := uc.PurchaseShopItem(ctx, 100, 1, 6002, 2, "same-key"); errcode.As(err) != errcode.ErrInventoryIdempotencyConflict {
		t.Fatalf("同键不同商品应判冲突, got %v", err)
	}
	// 冲突路径一分钱不动、一件货不发(首次的 2 份 × 100 = 200 已扣,之后不得再变)。
	if got := repo.goldOf(100); got != 800 {
		t.Fatalf("冲突不得再扣金币: %d want=800", got)
	}
	if got := repo.balanceOf(100, data.CurrencyDiamond); got != 500 {
		t.Fatalf("冲突不得扣钻石: %d want=500", got)
	}
	if repo.items[100][6001] != 20 {
		t.Fatalf("冲突不得二次发货: 背包=%d want=20", repo.items[100][6001])
	}
	if n := len(repo.instMap(100)); n != 0 {
		t.Fatalf("冲突不得发装备实例: %d 件", n)
	}
	// 原样重放(同键同请求)仍必须正常回放首次结果 —— 冲突判定不能误伤真正的重试。
	replay, err := uc.PurchaseShopItem(ctx, 100, 1, 6001, 2, "same-key")
	if err != nil {
		t.Fatalf("同键同请求重放应成功: %v", err)
	}
	if replay.Cost != 200 || replay.Items[0].Count != 20 {
		t.Fatalf("重放结果不符: cost=%d items=%+v", replay.Cost, replay.Items)
	}
}
