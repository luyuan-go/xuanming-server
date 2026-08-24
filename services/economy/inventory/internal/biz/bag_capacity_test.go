// bag_capacity_test.go — 容量购买用例单测(§5.3;假仓 + 假扣费器)。
package biz

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"

	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/data"
)

func newCapacityUsecaseForTest(gold uint64) (*BagUsecase, *fakeBagRepo, *fakeCapacityCharger) {
	repo := &fakeBagRepo{}
	cfg := conf.BagConf{
		SectionCapacities: []conf.BagSectionCapacityRule{{BagType: 0, Capacity: 100}, {BagType: 1, Capacity: 10}},
		CapacityPurchases: []conf.BagCapacityPurchaseRule{{
			BagType: 0, MaxExtra: 20,
			Tiers: []conf.BagCapacityTier{{Slots: 10, PriceGold: 100}, {Slots: 10, PriceGold: 200}},
		}},
		AllowUnverifiedOwner: true,
	}
	uc := NewBagUsecase(repo, cfg)
	charger := &fakeCapacityCharger{gold: gold}
	uc.SetCapacityCharger(charger)
	return uc, repo, charger
}

// TestPurchaseCapacityHappyPath 顺序买两档:档数推进、按档扣费、有效容量 = base+extra。
func TestPurchaseCapacityHappyPath(t *testing.T) {
	uc, _, charger := newCapacityUsecaseForTest(1000)
	ctx := context.Background()

	res, err := uc.PurchaseCapacity(ctx, 7, 1, 0, DSCallerIdentity{})
	if err != nil {
		t.Fatalf("第一档购买: %v", err)
	}
	if res.Purchases != 1 || res.Extra != 10 || res.EffectiveCapacity != 110 || res.Cost != 100 || res.Balance != 900 {
		t.Fatalf("第一档结果不符: %+v", res)
	}
	// 币种必须显式回报:格容扣费当前恒金币,静默回退成 0(UNSPECIFIED)会让下行无法渲染。
	if res.CurrencyKind != data.CurrencyGold {
		t.Fatalf("第一档币种应为金币: %v", res.CurrencyKind)
	}
	res, err = uc.PurchaseCapacity(ctx, 7, 1, 0, DSCallerIdentity{})
	if err != nil {
		t.Fatalf("第二档购买: %v", err)
	}
	if res.Purchases != 2 || res.Extra != 20 || res.EffectiveCapacity != 120 || res.Cost != 200 || res.Balance != 700 {
		t.Fatalf("第二档结果不符: %+v", res)
	}
	if res.CurrencyKind != data.CurrencyGold {
		t.Fatalf("第二档币种应为金币: %v", res.CurrencyKind)
	}
	if charger.charges != 2 {
		t.Fatalf("应恰好扣费两次: %d", charger.charges)
	}
	// 档位购罄 → ErrBagCapacityMaxed,不再扣费。
	if _, err := uc.PurchaseCapacity(ctx, 7, 1, 0, DSCallerIdentity{}); errcode.As(err) != errcode.ErrBagCapacityMaxed {
		t.Fatalf("购罄应拒: %v", err)
	}
	if charger.charges != 2 {
		t.Fatalf("购罄路径不得扣费: %d", charger.charges)
	}
}

// TestPurchaseCapacityCrashBetweenStepsConverges 扣费后落位前崩溃:同档重试
// → 扣费幂等回放零扣费 → 落位补应用,收敛且总扣费一次。
func TestPurchaseCapacityCrashBetweenStepsConverges(t *testing.T) {
	uc, repo, charger := newCapacityUsecaseForTest(1000)
	ctx := context.Background()

	// 模拟第一次调用在 ①扣费成功后、②落位前崩溃:预置扣费痕迹,不置落位状态。
	charger.chargedTiers = map[string]bool{capChargeKey(0, 1): true}
	charger.gold = 900 // 首次已扣 100

	res, err := uc.PurchaseCapacity(ctx, 7, 1, 0, DSCallerIdentity{})
	if err != nil {
		t.Fatalf("崩溃后重试应收敛: %v", err)
	}
	// Cost=0 是 CapacityPurchaseResult 的**既定语义**(bag.go:`if !already { res.Cost = charged }`,
	// 类型注释写明"幂等重放返回当前状态,Cost=0"),不是漏改:
	// ChargeBagCapacity 只回 (already, remainingBalance),data 层没把首次扣费额回放出来。
	// 与 SellItem.Earned(靠 ledger.result_currency_delta 回放同一金额)口径不同,见报告。
	if res.Purchases != 1 || res.Extra != 10 || res.Cost != 0 {
		t.Fatalf("重试应补落位且零扣费: %+v", res)
	}
	// 余额快照必须仍是首次扣费后的值(900),不能因为回放就丢成 0。
	if res.Balance != 900 {
		t.Fatalf("重试应回放首次扣费后的余额: %+v", res)
	}
	if repo.capPurchases[0] != 1 {
		t.Fatalf("落位未补齐: %+v", repo.capPurchases)
	}
	if charger.charges != 0 {
		t.Fatalf("重试不得再扣费: %d", charger.charges)
	}
}

// TestPurchaseCapacityRejections 金币不足 / 不可买段 / 未装配扣费器 / 授权失败。
func TestPurchaseCapacityRejections(t *testing.T) {
	ctx := context.Background()

	t.Run("金币不足", func(t *testing.T) {
		uc, repo, _ := newCapacityUsecaseForTest(50)
		if _, err := uc.PurchaseCapacity(ctx, 7, 1, 0, DSCallerIdentity{}); errcode.As(err) != errcode.ErrInventoryInsufficient {
			t.Fatalf("金币不足应拒: %v", err)
		}
		if repo.capPurchases[0] != 0 {
			t.Fatalf("扣费失败不得落位: %+v", repo.capPurchases)
		}
	})

	t.Run("不可买段", func(t *testing.T) {
		uc, _, _ := newCapacityUsecaseForTest(1000)
		if _, err := uc.PurchaseCapacity(ctx, 7, 1, 2, DSCallerIdentity{}); errcode.As(err) != errcode.ErrBagSectionNotAllowed {
			t.Fatalf("装备栏不可买应拒: %v", err)
		}
	})

	t.Run("未装配扣费器fail-closed", func(t *testing.T) {
		uc, _, _ := newCapacityUsecaseForTest(1000)
		uc.SetCapacityCharger(nil)
		if _, err := uc.PurchaseCapacity(ctx, 7, 1, 0, DSCallerIdentity{}); errcode.As(err) != errcode.ErrUnavailable {
			t.Fatalf("未装配应 fail-closed: %v", err)
		}
	})

	t.Run("owner授权失败不扣费", func(t *testing.T) {
		uc, _, charger := newCapacityUsecaseForTest(1000)
		uc.SetOwnerAuthorizer(&fakeOwnerAuthorizer{err: errcode.New(errcode.ErrBagEpochFenced, "fenced")})
		if _, err := uc.PurchaseCapacity(ctx, 7, 1, 0, DSCallerIdentity{}); errcode.As(err) != errcode.ErrBagEpochFenced {
			t.Fatalf("授权失败应拒: %v", err)
		}
		if charger.charges != 0 {
			t.Fatalf("授权失败不得扣费: %d", charger.charges)
		}
	})
}

// TestCarryEffectiveCapacities LoadBag 权威容量:只含配置了 base 的随身段,含已购增量。
func TestCarryEffectiveCapacities(t *testing.T) {
	uc, repo, _ := newCapacityUsecaseForTest(1000)
	repo.capExtra = map[uint32]uint32{0: 30}
	repo.capPurchases = map[uint32]uint32{0: 3}

	views, err := uc.CarryEffectiveCapacities(context.Background(), 7)
	if err != nil {
		t.Fatalf("CarryEffectiveCapacities: %v", err)
	}
	// 配置里随身组只有 bag_type 0 有 base(2/3 未配置 → 不下发,DS 沿用客户端配置)。
	if len(views) != 1 || views[0].BagType != 0 || views[0].Capacity != 130 {
		t.Fatalf("有效容量不符: %+v", views)
	}
}
