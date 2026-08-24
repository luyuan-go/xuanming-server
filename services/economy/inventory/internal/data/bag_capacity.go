// bag_capacity.go — 玩家背包容量购买(bag-domain.md §5.3,2026-07-22 拍板)。
//
// 有效容量 = base(bag_type 配置)+ extra(bag_capacity 行,玩家资产,单调只增)。
// 购买 = 跨库幂等两步 saga,幂等身份 = (player_id, bag_type, 第 tier 档):
//
//	① trade 库:ChargeBagCapacity —— inventory_ledger 幂等(key=bagcap:{bag}:{tier},
//	   指纹钳死档参数)+ deductGoldTx 扣费,同 key 重试 / 双击并发只扣一次;
//	② bag 库:ApplyCapacityPurchase —— 锁 bag_meta 行(串行化该玩家背包写;购买不是
//	   owner 写者,不 CAS 不推进 epoch,越权防线在五要件② owner 授权)+ 档数 CAS:
//	   purchases == tier-1 才应用,>= tier 幂等回放当前值。
//
// 两步间崩溃:重试同 tier → ① ledger 回放不再扣费 → ② 补应用,收敛;
// 不存在"扣钱未到账"的终态。价格 / 档位 / 封顶全在服务端配置(§9.6 数值不信 DS)。
package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"github.com/luyuancpp/pandora/pkg/errcode"
	bagv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/bag/v1"
)

// bagCapacityChargeOp 是容量购买在 inventory_ledger 的 op 标识。
const bagCapacityChargeOp = "buy_capacity"

// BagCapacityChargeKey 生成购买扣费幂等键(每玩家 uk(player_id, key) 下按段×档唯一)。
func BagCapacityChargeKey(bagType, tier uint32) string {
	return fmt.Sprintf("bagcap:%d:%d", bagType, tier)
}

// ChargeBagCapacity 购买扣费(trade 库单事务;①):
// 首次 → 扣 priceGold 并记账;同 key 重试(already=true)→ 零扣费,返回首次后的余额快照。
// 同 key 不同档参数(配置漂移)→ ErrInventoryIdempotencyConflict;余额不足 → ErrInventoryInsufficient。
func (r *MySQLInventoryRepo) ChargeBagCapacity(ctx context.Context, playerID uint64, bagType, tier, slots uint32, kind CurrencyKind, price uint64) (already bool, remainingBalance uint64, err error) {
	if verr := ValidateCurrencyKind(kind); verr != nil {
		return false, 0, verr
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, 0, errcode.New(errcode.ErrInternal, "begin charge tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	key := BagCapacityChargeKey(bagType, tier)
	// 指纹沿用旧格式(gold=<price>)当且仅当币种是金币:格容购买在多币种之前只可能是金币,
	// 换格式会让存量流水在同 key 重试时被误判成 ErrInventoryIdempotencyConflict(同 GrantFingerprint)。
	fingerprint := hashHex(fmt.Sprintf("bagcap|%d|%d|%d|%d|%d", playerID, bagType, tier, slots, price))
	if kind != CurrencyGold {
		fingerprint = hashHex(fmt.Sprintf("bagcap|%d|%d|%d|%d|%d|cur=%d", playerID, bagType, tier, slots, price, int32(kind)))
	}
	detail := fmt.Sprintf("buy capacity bag=%d tier=%d slots=%d gold=%d", bagType, tier, slots, price)
	hit, snap, lerr := claimLedger(ctx, tx, playerID, key, bagCapacityChargeOp, fingerprint, detail)
	if lerr != nil {
		return false, 0, lerr
	}
	if hit {
		// 幂等回放:钱已在首次扣过,直接提交(保持只读)返回首次余额快照。
		if cerr := tx.Commit(); cerr != nil {
			return false, 0, errcode.New(errcode.ErrInternal, "commit charge replay player=%d: %v", playerID, cerr)
		}
		return true, snap.Balances.Get(kind), nil
	}

	remaining, derr := deductCurrencyTx(ctx, tx, playerID, kind, price)
	if derr != nil {
		return false, 0, derr
	}
	balances, rerr := readBalancesTx(ctx, tx, playerID)
	if rerr != nil {
		return false, 0, rerr
	}
	if uerr := updateLedgerResult(ctx, tx, playerID, key, int64(slots), balances, Balances{kind: price}); uerr != nil {
		return false, 0, uerr
	}
	if cerr := tx.Commit(); cerr != nil {
		return false, 0, errcode.New(errcode.ErrInternal, "commit charge player=%d: %v", playerID, cerr)
	}
	return false, remaining, nil
}

// ApplyCapacityPurchase 容量落位(bag 库单事务;②):档数 CAS 幂等。
// 返回应用后(或幂等回放的当前)extra / purchases;applied=false 表示本次未新增(回放)。
func (r *MySQLBagRepo) ApplyCapacityPurchase(ctx context.Context, playerID uint64, bagType, tier, slots, maxExtra uint32) (extra, purchases uint32, applied bool, err error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, false, errcode.New(errcode.ErrInternal, "begin apply tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 锁 bag_meta 行:与 journal / 迁移落位共用同一每玩家串行化锚点(不 CAS epoch)。
	if _, ierr := tx.ExecContext(ctx,
		`INSERT IGNORE INTO bag_meta (player_id, owner_epoch, last_journal_seq) VALUES (?, 0, 0)`, playerID); ierr != nil {
		return 0, 0, false, errcode.New(errcode.ErrInternal, "ensure bag_meta player=%d: %v", playerID, ierr)
	}
	var epochIgnored uint64
	if qerr := tx.QueryRowContext(ctx,
		`SELECT owner_epoch FROM bag_meta WHERE player_id = ? FOR UPDATE`, playerID).Scan(&epochIgnored); qerr != nil {
		return 0, 0, false, errcode.New(errcode.ErrInternal, "lock bag_meta player=%d: %v", playerID, qerr)
	}

	if _, ierr := tx.ExecContext(ctx,
		`INSERT IGNORE INTO bag_capacity (player_id, bag_type, extra, purchases) VALUES (?, ?, 0, 0)`,
		playerID, bagType); ierr != nil {
		return 0, 0, false, errcode.New(errcode.ErrInternal, "ensure bag_capacity player=%d bag=%d: %v", playerID, bagType, ierr)
	}
	if qerr := tx.QueryRowContext(ctx,
		`SELECT extra, purchases FROM bag_capacity WHERE player_id = ? AND bag_type = ? FOR UPDATE`,
		playerID, bagType).Scan(&extra, &purchases); qerr != nil {
		return 0, 0, false, errcode.New(errcode.ErrInternal, "lock bag_capacity player=%d bag=%d: %v", playerID, bagType, qerr)
	}

	if purchases >= tier {
		// 本档已应用(同档重试 / 双击并发的第二腿):幂等回放当前值。
		if cerr := tx.Commit(); cerr != nil {
			return 0, 0, false, errcode.New(errcode.ErrInternal, "commit apply replay player=%d: %v", playerID, cerr)
		}
		return extra, purchases, false, nil
	}
	if purchases != tier-1 {
		// tier 恒 = 服务端读到的 purchases+1,乱序只可能来自异常调用方,fail-closed。
		return 0, 0, false, errcode.New(errcode.ErrInvalidState,
			"capacity tier out of order player=%d bag=%d tier=%d purchases=%d", playerID, bagType, tier, purchases)
	}
	newExtra := uint64(extra) + uint64(slots)
	if newExtra > uint64(maxExtra) {
		// 已扣费但配置收缩到装不下(运营中途改配置):fail-closed 报警,凭 ledger 行排障;
		// 正常路径在扣费前已按同一配置预检,不会走到这里。
		return 0, 0, false, errcode.New(errcode.ErrBagCapacityMaxed,
			"extra %d+%d exceeds max_extra %d player=%d bag=%d (config shrank after charge?)",
			extra, slots, maxExtra, playerID, bagType)
	}
	if _, uerr := tx.ExecContext(ctx,
		`UPDATE bag_capacity SET extra = ?, purchases = ? WHERE player_id = ? AND bag_type = ?`,
		uint32(newExtra), tier, playerID, bagType); uerr != nil {
		return 0, 0, false, errcode.New(errcode.ErrInternal, "apply capacity player=%d bag=%d: %v", playerID, bagType, uerr)
	}
	if cerr := tx.Commit(); cerr != nil {
		return 0, 0, false, errcode.New(errcode.ErrInternal, "commit apply player=%d: %v", playerID, cerr)
	}
	return uint32(newExtra), tier, true, nil
}

// GetCapacityState 读某段已购状态(无行 = 0/0;购买用例定档 + LoadBag/GetSections 展示)。
func (r *MySQLBagRepo) GetCapacityState(ctx context.Context, playerID uint64, bagType uint32) (extra, purchases uint32, err error) {
	qerr := r.db.QueryRowContext(ctx,
		`SELECT extra, purchases FROM bag_capacity WHERE player_id = ? AND bag_type = ?`,
		playerID, bagType).Scan(&extra, &purchases)
	if errors.Is(qerr, sql.ErrNoRows) {
		return 0, 0, nil
	}
	if qerr != nil {
		return 0, 0, errcode.New(errcode.ErrInternal, "read bag_capacity player=%d bag=%d: %v", playerID, bagType, qerr)
	}
	return extra, purchases, nil
}

// readCapacityExtraTx 事务内读某段已购增量(AppendJournal 有效容量判定用;无行 = 0)。
func readCapacityExtraTx(ctx context.Context, tx *sql.Tx, playerID uint64, bagType uint32) (uint32, error) {
	var extra uint32
	err := tx.QueryRowContext(ctx,
		`SELECT extra FROM bag_capacity WHERE player_id = ? AND bag_type = ?`, playerID, bagType).Scan(&extra)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "read capacity extra player=%d bag=%d: %v", playerID, bagType, err)
	}
	return extra, nil
}

// collectBackendBagTypes 收集一批 journal entries 触及的后端驻留段类型
// (AppendJournal 事务内有效容量预取:主段 + 转移目标段 + 使用产出段)。
func collectBackendBagTypes(entries []*bagv1.BagJournalEntry) []uint32 {
	seen := map[uint32]bool{}
	add := func(bagType uint32) {
		if IsBackendResidentBagType(bagType) {
			seen[bagType] = true
		}
	}
	for _, entry := range entries {
		add(entry.GetBagType())
		switch op := entry.GetOp().(type) {
		case *bagv1.BagJournalEntry_Transfer:
			add(op.Transfer.GetToBagType())
		case *bagv1.BagJournalEntry_Consume:
			add(op.Consume.GetProduceBagType())
		}
	}
	out := make([]uint32, 0, len(seen))
	for bagType := range seen {
		out = append(out, bagType)
	}
	return out
}

// effectiveCapacityFn 把 base 容量回调包装成"base + 已购增量"的有效容量回调。
// base 为 0(未配置段)保持 0,fail-closed 语义不变;和溢出钳制到 uint32 上限。
func effectiveCapacityFn(base BagSectionCapacity, extras map[uint32]uint32) BagSectionCapacity {
	return func(bagType uint32) uint32 {
		b := base(bagType)
		if b == 0 {
			return 0
		}
		eff := uint64(b) + uint64(extras[bagType])
		if eff > math.MaxUint32 {
			eff = math.MaxUint32
		}
		return uint32(eff)
	}
}
