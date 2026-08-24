// bag_capacity_mysql_test.go — 容量购买真 MySQL 集成测试(§5.3;双库:trade 扣费 + bag 落位)。
// 门控:PANDORA_TEST_MYSQL_DSN。
package data

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
	bagv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/bag/v1"
)

// TestBagCapacityPurchase_MySQL 扣费幂等 / 指纹冲突 / 余额不足 / 落位档数 CAS /
// 有效容量进真实 journal 判定 / GetSections 有效容量。
func TestBagCapacityPurchase_MySQL(t *testing.T) {
	inv := openInventoryMySQLFixture(t)
	bag := openBagMySQLFixture(t)
	invRepo := NewMySQLInventoryRepo(inv.db)
	bagRepo := NewMySQLBagRepo(bag.db)
	ctx := context.Background()
	const player = uint64(401)

	mustExec(t, inv.db, `INSERT INTO player_wallet (player_id, currency_kind, amount) VALUES (?, 1, 1000)`, player)

	t.Run("扣费幂等与冲突", func(t *testing.T) {
		already, remaining, err := invRepo.ChargeBagCapacity(ctx, player, BagWarehouseType, 1, 20, CurrencyGold, 200)
		if err != nil || already || remaining != 800 {
			t.Fatalf("首扣: already=%v remaining=%d err=%v", already, remaining, err)
		}
		// 同档重试:幂等回放零扣费。
		already, remaining, err = invRepo.ChargeBagCapacity(ctx, player, BagWarehouseType, 1, 20, CurrencyGold, 200)
		if err != nil || !already || remaining != 800 {
			t.Fatalf("重试应回放: already=%v remaining=%d err=%v", already, remaining, err)
		}
		// 同档不同参数(配置漂移)→ 指纹冲突。
		if _, _, err := invRepo.ChargeBagCapacity(ctx, player, BagWarehouseType, 1, 20, CurrencyGold, 999); errcode.As(err) != errcode.ErrInventoryIdempotencyConflict {
			t.Fatalf("配置漂移应指纹冲突: %v", err)
		}
		// 余额不足。
		if _, _, err := invRepo.ChargeBagCapacity(ctx, player, BagWarehouseType, 2, 20, CurrencyGold, 5000); errcode.As(err) != errcode.ErrInventoryInsufficient {
			t.Fatalf("余额不足应拒: %v", err)
		}
	})

	t.Run("落位档数CAS", func(t *testing.T) {
		extra, purchases, applied, err := bagRepo.ApplyCapacityPurchase(ctx, player, BagWarehouseType, 1, 20, 300)
		if err != nil || !applied || extra != 20 || purchases != 1 {
			t.Fatalf("首落位: extra=%d purchases=%d applied=%v err=%v", extra, purchases, applied, err)
		}
		// 同档重试幂等回放。
		extra, purchases, applied, err = bagRepo.ApplyCapacityPurchase(ctx, player, BagWarehouseType, 1, 20, 300)
		if err != nil || applied || extra != 20 || purchases != 1 {
			t.Fatalf("重试应回放: extra=%d purchases=%d applied=%v err=%v", extra, purchases, applied, err)
		}
		// 乱序档 fail-closed。
		if _, _, _, err := bagRepo.ApplyCapacityPurchase(ctx, player, BagWarehouseType, 5, 20, 300); errcode.As(err) != errcode.ErrInvalidState {
			t.Fatalf("乱序档应拒: %v", err)
		}
		// 超硬上限 fail-closed。
		if _, _, _, err := bagRepo.ApplyCapacityPurchase(ctx, player, BagWarehouseType, 2, 300, 300); errcode.As(err) != errcode.ErrBagCapacityMaxed {
			t.Fatalf("超硬上限应拒: %v", err)
		}
	})

	t.Run("有效容量进journal判定与GetSections", func(t *testing.T) {
		// base 1 格 + 已购 20 → 两种 config(2 格)入仓库应成功;第 22 格应拒。
		oneSlot := func(bagType uint32) uint32 {
			if bagType == BagWarehouseType {
				return 1
			}
			return 0
		}
		if _, err := bagRepo.AppendJournal(ctx, player, 1, []*bagv1.BagJournalEntry{
			mkClaimEntry(1, "cap-eff-1", BagWarehouseType, 0, stackItem(50001, 1), stackItem(50002, 1)),
		}, oneSlot, bagTestMaxStack, 0); err != nil {
			t.Fatalf("有效容量(1+20)内入仓应成功: %v", err)
		}
		// 再塞 19 种新 config → 总 21 格 = 恰满;第 22 种拒。
		batch := []*bagv1.BagItem{}
		for i := uint32(0); i < 19; i++ {
			batch = append(batch, stackItem(51000+i, 1))
		}
		if _, err := bagRepo.AppendJournal(ctx, player, 1, []*bagv1.BagJournalEntry{
			mkClaimEntry(2, "cap-eff-2", BagWarehouseType, 0, batch...),
		}, oneSlot, bagTestMaxStack, 0); err != nil {
			t.Fatalf("填满有效容量应成功: %v", err)
		}
		if _, err := bagRepo.AppendJournal(ctx, player, 1, []*bagv1.BagJournalEntry{
			mkClaimEntry(3, "cap-eff-3", BagWarehouseType, 0, stackItem(52000, 1)),
		}, oneSlot, bagTestMaxStack, 0); errcode.As(err) != errcode.ErrBagCapacityFull {
			t.Fatalf("超出有效容量应拒: %v", err)
		}
		// GetSections 返回有效容量(1+20)。
		secs, err := bagRepo.GetSections(ctx, player, []uint32{BagWarehouseType}, oneSlot)
		if err != nil || len(secs) != 1 || secs[0].GetCapacity() != 21 {
			t.Fatalf("GetSections 有效容量应为 21: %+v err=%v", secs, err)
		}
	})

	t.Run("已购状态读取", func(t *testing.T) {
		extra, purchases, err := bagRepo.GetCapacityState(ctx, player, BagWarehouseType)
		if err != nil || extra != 20 || purchases != 1 {
			t.Fatalf("状态读取: extra=%d purchases=%d err=%v", extra, purchases, err)
		}
		// 无行段 = 0/0。
		extra, purchases, err = bagRepo.GetCapacityState(ctx, player, 0)
		if err != nil || extra != 0 || purchases != 0 {
			t.Fatalf("无行应 0/0: extra=%d purchases=%d err=%v", extra, purchases, err)
		}
	})
}
