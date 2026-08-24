// inventory_test.go — InventoryUsecase 业务逻辑单测(W5 ③,2026-06-18)。
//
// 用内存版 fakeRepo 复刻 MySQL 幂等 / 扣减语义,无需真 DB;
// 验证 usable / sellable 规则裁决、幂等键去重、数量不足拦截。
package biz

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/data"
)

// ledgerEntry 复刻 MySQL inventory_ledger 一行:记录首次执行的请求指纹 + 结果快照。
//
// 多币种改造后一行要记两件事(对齐 result_currencies / result_currency_delta 两列):
//   - snapBalances:首次执行后的**全币种余额**快照;
//   - snapDelta:本次操作的**货币变动额**。缺了它,幂等重放算不出"这一笔当初是多少钱"
//     (中间可能已有别的收支),SellItem.Earned / PurchaseOutcome.Cost 就只能瞎编。
type ledgerEntry struct {
	fingerprint   string
	snapRemaining int64
	snapBalances  data.Balances
	snapDelta     data.Balances
}

// escrowEntry 复刻 MySQL auction_escrow 一行(挂单托管资产)。
// 货币托管从"只能冻金币"变成"冻某一种货币":currencyKind + frozenAmount(无符号)。
type escrowEntry struct {
	kind         data.EscrowKind
	itemConfigID uint32
	frozenQty    int64
	currencyKind data.CurrencyKind
	frozenAmount uint64
	closed       bool
}

// fakeRepo 是 data.InventoryRepo 的内存实现(复刻 MySQL 幂等 / 扣减 / 指纹快照 / escrow 语义)。
type fakeRepo struct {
	escrowMu sync.Mutex
	// wallet 复刻 player_wallet(player_id, currency_kind) → amount:
	// 一玩家一币种一项,**没有项 = 该币种余额 0**(不预建)。
	wallet map[uint64]data.Balances
	items  map[uint64]map[uint32]int64
	ledger map[string]ledgerEntry  // key=playerID|idempotencyKey
	escrow map[string]*escrowEntry // key=playerID|order:<orderID>

	// 装备实例(W5 ④):instances[playerID][instanceID]=inst;instGrant 复刻 grant_inst 幂等。
	instances map[uint64]map[uint64]*data.ItemInstance
	instGrant map[string]instGrantEntry // key=playerID|idempotencyKey

	// purchases 复刻商店购买流水 detail 承载的"首次到底发了什么"
	// (生产是 purchaseDetail/parsePurchaseDetail;这里直接存结构体,语义等价)。
	purchases map[string]purchaseFacts // key=playerID|idempotencyKey

	// 邮件 transfer 托管(2026-07-22):xferEscrow 复刻 mail_transfer_escrow 行,
	// xferLedger 复刻 escrow_out / transfer_claim 幂等流水(指纹比对)。
	xferEscrow map[uint64]*xferEscrowRow // instance_id → 托管行
	xferLedger map[string]string         // key=playerID|idempotencyKey → fingerprint
}

// xferEscrowRow 复刻 mail_transfer_escrow 一行(实例数据 + 归属上下文)。
type xferEscrowRow struct {
	inst           data.ItemInstance
	sourcePlayerID uint64
	toPlayerID     uint64
}

// instGrantEntry 复刻 grant_inst 幂等流水:指纹 + 首次发放的 instance_id 列表(回放用)。
type instGrantEntry struct {
	fingerprint string
	ids         []uint64
}

// purchaseFacts 是一次商店购买首次执行的发货事实(回放用;无法从请求重算,
// 因为每份数量是热更配置,改表后重算会得到与首次不同的数量)。
type purchaseFacts struct {
	totalItems  int64
	instanceIDs []uint64
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		wallet:     map[uint64]data.Balances{},
		items:      map[uint64]map[uint32]int64{},
		ledger:     map[string]ledgerEntry{},
		escrow:     map[string]*escrowEntry{},
		instances:  map[uint64]map[uint64]*data.ItemInstance{},
		instGrant:  map[string]instGrantEntry{},
		purchases:  map[string]purchaseFacts{},
		xferEscrow: map[uint64]*xferEscrowRow{},
		xferLedger: map[string]string{},
	}
}

func keyOf(pid uint64, k string) string {
	return string(rune(pid)) + "|" + k
}

func escrowKeyOf(pid, orderID uint64) string {
	return keyOf(pid, fmt.Sprintf("order:%d", orderID))
}

// ── 钱包原语(复刻 data/currency.go 的三条无符号硬纪律)────────────────────────
//
// ① 减法先比较后相减,永不下溢;② 加法有 MaxCurrencyAmount 上限,越界拒绝不回绕;
// ③ 乘法用 data.SafeMulCurrency。假仓刻意照抄这三条:如果假仓比生产宽松,
//    单测就永远测不出生产的溢出闸,等于把闸门测没了。

// balanceOf 读某玩家某币种余额(无项 = 0)。
func (f *fakeRepo) balanceOf(playerID uint64, kind data.CurrencyKind) uint64 {
	return f.wallet[playerID].Get(kind)
}

// setBalance 写绝对值(调用方已算好),0 余额删项,与"没有这一行"语义等价。
func (f *fakeRepo) setBalance(playerID uint64, kind data.CurrencyKind, amount uint64) {
	if f.wallet[playerID] == nil {
		f.wallet[playerID] = data.Balances{}
	}
	if amount == 0 {
		delete(f.wallet[playerID], kind)
		return
	}
	f.wallet[playerID][kind] = amount
}

// snapshotBalances 拷一份余额快照:落进 ledger 的快照必须与后续变动解耦,
// 直接存 map 引用会让"回放首次结果"变成"回放当前状态",幂等断言就永远是假绿。
func (f *fakeRepo) snapshotBalances(playerID uint64) data.Balances {
	out := data.Balances{}
	for k, v := range f.wallet[playerID] {
		if v != 0 {
			out[k] = v
		}
	}
	return out
}

// addCurrency 复刻 addCurrencyTx:越过 MaxCurrencyAmount 拒绝入账,不回绕。
func (f *fakeRepo) addCurrency(playerID uint64, kind data.CurrencyKind, n uint64) error {
	if verr := data.ValidateCurrencyKind(kind); verr != nil {
		return verr
	}
	if n == 0 {
		return nil
	}
	have := f.balanceOf(playerID, kind)
	if have > data.MaxCurrencyAmount-n {
		return errcode.New(errcode.ErrInventoryCurrencyOverflow,
			"currency overflow player=%d kind=%d have=%d add=%d", playerID, int32(kind), have, n)
	}
	f.setBalance(playerID, kind, have+n)
	return nil
}

// deductCurrency 复刻 deductCurrencyTx:**先比较后相减**,不足即拒,减法永不下溢。
func (f *fakeRepo) deductCurrency(playerID uint64, kind data.CurrencyKind, n uint64) error {
	if verr := data.ValidateCurrencyKind(kind); verr != nil {
		return verr
	}
	if n == 0 {
		return nil
	}
	have := f.balanceOf(playerID, kind)
	if have < n {
		return errcode.New(errcode.ErrInventoryInsufficient,
			"insufficient currency player=%d kind=%d need=%d have=%d", playerID, int32(kind), n, have)
	}
	f.setBalance(playerID, kind, have-n)
	return nil
}

// applyCurrencies 按 kind 升序批量入账(复刻 applyCurrencyDeltasTx 的固定锁序)。
func (f *fakeRepo) applyCurrencies(playerID uint64, deltas data.Balances) error {
	for _, it := range deltas.Sorted() {
		if err := f.addCurrency(playerID, it.GetKind(), it.GetAmount()); err != nil {
			return err
		}
	}
	return nil
}

// goldOf 是断言用简写:多数用例只关心金币这一种。
func (f *fakeRepo) goldOf(playerID uint64) uint64 {
	return f.balanceOf(playerID, data.CurrencyGold)
}

// goldBalances 组一份"只有金币"的入账额(替代旧的裸 int64 gold 参数)。
func goldBalances(n uint64) data.Balances {
	if n == 0 {
		return nil
	}
	return data.Balances{data.CurrencyGold: n}
}

func (f *fakeRepo) GetInventory(_ context.Context, playerID uint64) (data.Balances, []data.ItemStack, error) {
	var out []data.ItemStack
	for id, c := range f.items[playerID] {
		if c > 0 {
			out = append(out, data.ItemStack{ItemConfigID: id, Count: c})
		}
	}
	return f.snapshotBalances(playerID), out, nil
}

func (f *fakeRepo) GrantItems(_ context.Context, playerID uint64, items []data.ItemGrant, currencies data.Balances, idempotencyKey, _ string) (data.Balances, bool, error) {
	gk := keyOf(playerID, idempotencyKey)
	fp := data.GrantFingerprint(items, currencies)
	if e, ok := f.ledger[gk]; ok {
		if e.fingerprint != fp {
			return nil, false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "idempotency conflict")
		}
		return e.snapBalances, true, nil
	}
	if f.items[playerID] == nil {
		f.items[playerID] = map[uint32]int64{}
	}
	for _, it := range items {
		f.items[playerID][it.ItemConfigID] += it.Count
	}
	if err := f.applyCurrencies(playerID, currencies); err != nil {
		return nil, false, err
	}
	snap := f.snapshotBalances(playerID)
	f.ledger[gk] = ledgerEntry{fingerprint: fp, snapBalances: snap, snapDelta: copyBalances(currencies)}
	return snap, false, nil
}

// copyBalances 深拷一份变动额(同样不能存调用方的 map 引用)。
func copyBalances(b data.Balances) data.Balances {
	if len(b) == 0 {
		return nil
	}
	out := make(data.Balances, len(b))
	for k, v := range b {
		out[k] = v
	}
	return out
}

func (f *fakeRepo) UseItem(_ context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey, _ string) (int64, bool, error) {
	gk := keyOf(playerID, idempotencyKey)
	fp := data.UseFingerprint(itemConfigID, count)
	if e, ok := f.ledger[gk]; ok {
		if e.fingerprint != fp {
			return 0, false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "idempotency conflict")
		}
		return e.snapRemaining, true, nil
	}
	have := f.items[playerID][itemConfigID]
	if have == 0 {
		return 0, false, errcode.New(errcode.ErrInventoryItemNotFound, "not found")
	}
	if have < count {
		return 0, false, errcode.New(errcode.ErrInventoryInsufficient, "insufficient")
	}
	f.items[playerID][itemConfigID] = have - count
	f.ledger[gk] = ledgerEntry{fingerprint: fp, snapRemaining: have - count}
	return have - count, false, nil
}

func (f *fakeRepo) DiscardItem(_ context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey, _ string) (int64, bool, error) {
	gk := keyOf(playerID, idempotencyKey)
	fp := data.DiscardFingerprint(itemConfigID, count)
	if e, ok := f.ledger[gk]; ok {
		if e.fingerprint != fp {
			return 0, false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "idempotency conflict")
		}
		return e.snapRemaining, true, nil
	}
	have := f.items[playerID][itemConfigID]
	if have == 0 {
		return 0, false, errcode.New(errcode.ErrInventoryItemNotFound, "not found")
	}
	if have < count {
		return 0, false, errcode.New(errcode.ErrInventoryInsufficient, "insufficient")
	}
	remaining := have - count
	f.items[playerID][itemConfigID] = remaining
	f.ledger[gk] = ledgerEntry{fingerprint: fp, snapRemaining: remaining}
	return remaining, false, nil
}

func (f *fakeRepo) ConsumeBattleItem(_ context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey, _ string) (int64, bool, error) {
	gk := keyOf(playerID, idempotencyKey)
	fp := data.BattleConsumeFingerprint(itemConfigID, count)
	if e, ok := f.ledger[gk]; ok {
		if e.fingerprint != fp {
			return 0, false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "idempotency conflict")
		}
		return e.snapRemaining, true, nil
	}
	have := f.items[playerID][itemConfigID]
	if have == 0 {
		return 0, false, errcode.New(errcode.ErrInventoryItemNotFound, "not found")
	}
	if have < count {
		return 0, false, errcode.New(errcode.ErrInventoryInsufficient, "insufficient")
	}
	remaining := have - count
	f.items[playerID][itemConfigID] = remaining
	f.ledger[gk] = ledgerEntry{fingerprint: fp, snapRemaining: remaining}
	return remaining, false, nil
}

func (f *fakeRepo) DiscardBattleItem(_ context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey, _ string) (int64, bool, error) {
	gk := keyOf(playerID, idempotencyKey)
	fp := data.BattleDiscardFingerprint(itemConfigID, count)
	if e, ok := f.ledger[gk]; ok {
		if e.fingerprint != fp {
			return 0, false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "idempotency conflict")
		}
		return e.snapRemaining, true, nil
	}
	have := f.items[playerID][itemConfigID]
	if have == 0 {
		return 0, false, errcode.New(errcode.ErrInventoryItemNotFound, "not found")
	}
	if have < count {
		return 0, false, errcode.New(errcode.ErrInventoryInsufficient, "insufficient")
	}
	remaining := have - count
	f.items[playerID][itemConfigID] = remaining
	f.ledger[gk] = ledgerEntry{fingerprint: fp, snapRemaining: remaining}
	return remaining, false, nil
}

func (f *fakeRepo) SellItem(_ context.Context, playerID uint64, itemConfigID uint32, count int64, kind data.CurrencyKind, amount uint64, idempotencyKey, _ string) (data.SaleOutcome, bool, error) {
	gk := keyOf(playerID, idempotencyKey)
	fp := data.SellFingerprint(itemConfigID, count)
	if e, ok := f.ledger[gk]; ok {
		if e.fingerprint != fp {
			return data.SaleOutcome{}, false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "idempotency conflict")
		}
		// 回放:剩余数量 / 余额 / 本次入账额全部来自首次执行的快照,不重算。
		return data.SaleOutcome{
			Remaining: e.snapRemaining,
			Balances:  e.snapBalances,
			Earned:    e.snapDelta.Get(kind),
			Kind:      kind,
		}, true, nil
	}
	// amount 是 uint64:`== 0` 就是完整的"不可出售"判定。
	// 旧写法 `gold <= 0` 在无符号下会退化成同一个 `== 0`,留着只会让人误以为还在防负数。
	if amount == 0 {
		return data.SaleOutcome{}, false, errcode.New(errcode.ErrInventoryNotSellable, "not sellable")
	}
	have := f.items[playerID][itemConfigID]
	if have == 0 {
		return data.SaleOutcome{}, false, errcode.New(errcode.ErrInventoryItemNotFound, "not found")
	}
	if have < count {
		return data.SaleOutcome{}, false, errcode.New(errcode.ErrInventoryInsufficient, "insufficient")
	}
	f.items[playerID][itemConfigID] = have - count
	if err := f.addCurrency(playerID, kind, amount); err != nil {
		return data.SaleOutcome{}, false, err
	}
	snap := f.snapshotBalances(playerID)
	delta := data.Balances{kind: amount}
	f.ledger[gk] = ledgerEntry{fingerprint: fp, snapRemaining: have - count, snapBalances: snap, snapDelta: delta}
	return data.SaleOutcome{Remaining: have - count, Balances: snap, Earned: amount, Kind: kind}, false, nil
}

func (f *fakeRepo) SettleAuctionMatch(_ context.Context, _, sellerID, buyerID, sellOrderID, buyOrderID uint64, itemConfigID uint32, quantity int64, kind data.CurrencyKind, totalAmount uint64, idempotencyKey, _ string) (bool, error) {
	fp := data.AuctionSettleFingerprint(sellerID, buyerID, itemConfigID, quantity, kind, totalAmount)
	sk := keyOf(sellerID, idempotencyKey)
	bk := keyOf(buyerID, idempotencyKey)
	// 幂等命中:任一方流水已存(指纹一致)→ already 回放;指纹不一致 → 冲突。
	if e, ok := f.ledger[sk]; ok {
		if e.fingerprint != fp {
			return false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "idempotency conflict")
		}
		return true, nil
	}
	if e, ok := f.ledger[bk]; ok {
		if e.fingerprint != fp {
			return false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "idempotency conflict")
		}
		return true, nil
	}
	// 从双方 escrow 消费(资产已在 FreezeForOrder 冻结)。
	se := f.escrow[escrowKeyOf(sellerID, sellOrderID)]
	if se == nil || se.closed || se.kind != data.EscrowKindItem || se.frozenQty < quantity {
		return false, errcode.New(errcode.ErrInventoryInsufficient, "seller item escrow insufficient")
	}
	be := f.escrow[escrowKeyOf(buyerID, buyOrderID)]
	if be == nil || be.closed || be.kind != data.EscrowKindCurrency || be.frozenAmount < totalAmount {
		return false, errcode.New(errcode.ErrInventoryInsufficient, "buyer currency escrow insufficient")
	}
	// 币种必须与托管一致:用金币托管去付钻石成交会凭空造币,fail-closed。
	if be.currencyKind != kind {
		return false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "escrow currency kind mismatch")
	}
	se.frozenQty -= quantity
	be.frozenAmount -= totalAmount
	// 入账对手:卖家加货币,买家加道具。
	if err := f.addCurrency(sellerID, kind, totalAmount); err != nil {
		return false, err
	}
	if f.items[buyerID] == nil {
		f.items[buyerID] = map[uint32]int64{}
	}
	f.items[buyerID][itemConfigID] += quantity
	f.ledger[sk] = ledgerEntry{fingerprint: fp}
	f.ledger[bk] = ledgerEntry{fingerprint: fp}
	return false, nil
}

func (f *fakeRepo) SettlePlayerTrade(_ context.Context, _, sellerID, buyerID uint64, sellerItems, buyerItems []data.ItemGrant, kind data.CurrencyKind, price uint64, idempotencyKey, _ string) (bool, error) {
	fp := data.PlayerTradeSettleFingerprint(sellerID, buyerID, sellerItems, buyerItems, kind, price)
	sk := keyOf(sellerID, idempotencyKey)
	bk := keyOf(buyerID, idempotencyKey)
	if e, ok := f.ledger[sk]; ok {
		if e.fingerprint != fp {
			return false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "idempotency conflict")
		}
		return true, nil
	}
	if e, ok := f.ledger[bk]; ok {
		if e.fingerprint != fp {
			return false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "idempotency conflict")
		}
		return true, nil
	}
	// 校验双方活跃余额足够(无 escrow,直接从活跃背包 / 金币扣转)。
	for _, it := range sellerItems {
		if f.items[sellerID][it.ItemConfigID] < it.Count {
			return false, errcode.New(errcode.ErrInventoryInsufficient, "seller item insufficient")
		}
	}
	for _, it := range buyerItems {
		if f.items[buyerID][it.ItemConfigID] < it.Count {
			return false, errcode.New(errcode.ErrInventoryInsufficient, "buyer item insufficient")
		}
	}
	// price 是 uint64:`> 0` 判的是"这笔要不要动钱",不是"防负数"。
	if price > 0 && f.balanceOf(buyerID, kind) < price {
		return false, errcode.New(errcode.ErrInventoryInsufficient, "buyer currency insufficient")
	}
	if f.items[sellerID] == nil {
		f.items[sellerID] = map[uint32]int64{}
	}
	if f.items[buyerID] == nil {
		f.items[buyerID] = map[uint32]int64{}
	}
	// 卖家交付 sellerItems → 买家;买家交付 buyerItems → 卖家;买家付 price 金币 → 卖家。
	for _, it := range sellerItems {
		f.items[sellerID][it.ItemConfigID] -= it.Count
		f.items[buyerID][it.ItemConfigID] += it.Count
	}
	for _, it := range buyerItems {
		f.items[buyerID][it.ItemConfigID] -= it.Count
		f.items[sellerID][it.ItemConfigID] += it.Count
	}
	if price > 0 {
		if err := f.deductCurrency(buyerID, kind, price); err != nil {
			return false, err
		}
		if err := f.addCurrency(sellerID, kind, price); err != nil {
			return false, err
		}
	}
	f.ledger[sk] = ledgerEntry{fingerprint: fp}
	f.ledger[bk] = ledgerEntry{fingerprint: fp}
	return false, nil
}

func (f *fakeRepo) FreezeForOrder(_ context.Context, playerID, orderID uint64, kind data.EscrowKind, itemConfigID uint32, quantity int64, currencyKind data.CurrencyKind, frozenAmount uint64) (bool, error) {
	ek := escrowKeyOf(playerID, orderID)
	if _, ok := f.escrow[ek]; ok {
		return true, nil // 幂等:已冻结。
	}
	switch kind {
	case data.EscrowKindItem:
		if f.items[playerID] == nil || f.items[playerID][itemConfigID] < quantity {
			return false, errcode.New(errcode.ErrInventoryInsufficient, "freeze item insufficient")
		}
		f.items[playerID][itemConfigID] -= quantity
		f.escrow[ek] = &escrowEntry{kind: kind, itemConfigID: itemConfigID, frozenQty: quantity}
	case data.EscrowKindCurrency:
		if err := f.deductCurrency(playerID, currencyKind, frozenAmount); err != nil {
			return false, err
		}
		f.escrow[ek] = &escrowEntry{
			kind: kind, itemConfigID: itemConfigID,
			currencyKind: currencyKind, frozenAmount: frozenAmount,
		}
	default:
		return false, errcode.New(errcode.ErrInvalidArg, "unknown escrow kind")
	}
	return false, nil
}

func (f *fakeRepo) EnsureAuctionEscrow(_ context.Context, playerID, orderID uint64, kind data.EscrowKind, itemConfigID uint32, remainingQuantity int64, currencyKind data.CurrencyKind, unitPrice uint64) (bool, error) {
	f.escrowMu.Lock()
	defer f.escrowMu.Unlock()

	ek := escrowKeyOf(playerID, orderID)
	if e := f.escrow[ek]; e != nil {
		if e.closed || e.kind != kind || e.itemConfigID != itemConfigID {
			return false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "escrow identity conflict")
		}
		switch kind {
		case data.EscrowKindItem:
			if e.frozenAmount != 0 {
				return false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "item escrow carries currency")
			}
			if e.frozenQty < remainingQuantity {
				return false, errcode.New(errcode.ErrInventoryInsufficient, "item escrow short")
			}
		case data.EscrowKindCurrency:
			required, ok := data.SafeMulCurrency(unitPrice, uint64(remainingQuantity))
			if !ok || e.frozenQty != 0 || e.currencyKind != currencyKind {
				return false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "currency escrow malformed")
			}
			if e.frozenAmount < required {
				return false, errcode.New(errcode.ErrInventoryInsufficient, "currency escrow short")
			}
		}
		return true, nil
	}

	switch kind {
	case data.EscrowKindItem:
		if f.items[playerID] == nil || f.items[playerID][itemConfigID] < remainingQuantity {
			return false, errcode.New(errcode.ErrInventoryInsufficient, "ensure item insufficient")
		}
		f.items[playerID][itemConfigID] -= remainingQuantity
		f.escrow[ek] = &escrowEntry{kind: kind, itemConfigID: itemConfigID, frozenQty: remainingQuantity}
	case data.EscrowKindCurrency:
		required, ok := data.SafeMulCurrency(unitPrice, uint64(remainingQuantity))
		if !ok {
			return false, errcode.New(errcode.ErrInvalidArg, "ensure currency overflow")
		}
		if err := f.deductCurrency(playerID, currencyKind, required); err != nil {
			return false, err
		}
		f.escrow[ek] = &escrowEntry{
			kind: kind, itemConfigID: itemConfigID,
			currencyKind: currencyKind, frozenAmount: required,
		}
	default:
		return false, errcode.New(errcode.ErrInvalidArg, "unknown escrow kind")
	}
	return false, nil
}

func (f *fakeRepo) ReleaseEscrow(_ context.Context, playerID, orderID uint64) (bool, error) {
	e := f.escrow[escrowKeyOf(playerID, orderID)]
	if e == nil || e.closed {
		return true, nil // 幂等 no-op。
	}
	if e.kind == data.EscrowKindItem && e.frozenQty > 0 {
		if f.items[playerID] == nil {
			f.items[playerID] = map[uint32]int64{}
		}
		f.items[playerID][e.itemConfigID] += e.frozenQty
	}
	if e.kind == data.EscrowKindCurrency && e.frozenAmount > 0 {
		if err := f.addCurrency(playerID, e.currencyKind, e.frozenAmount); err != nil {
			return false, err
		}
	}
	e.frozenQty, e.frozenAmount, e.closed = 0, 0, true
	return false, nil
}

// ── 装备实例(W5 ④)内存实现,复刻 player_item_instance 语义 ──

func (f *fakeRepo) instMap(playerID uint64) map[uint64]*data.ItemInstance {
	if f.instances[playerID] == nil {
		f.instances[playerID] = map[uint64]*data.ItemInstance{}
	}
	return f.instances[playerID]
}

func (f *fakeRepo) ListInstances(_ context.Context, playerID uint64) ([]data.ItemInstance, error) {
	m := f.instances[playerID]
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]data.ItemInstance, 0, len(ids))
	for _, id := range ids {
		out = append(out, *m[id])
	}
	return out, nil
}

func (f *fakeRepo) CheckInstancesOwned(_ context.Context, playerID uint64, queries []data.InstanceOwnershipQuery) ([]data.ItemInstance, error) {
	var out []data.ItemInstance
	for _, q := range queries {
		if inst := f.instances[playerID][q.InstanceID]; inst != nil && inst.ItemConfigID == q.ItemConfigID {
			out = append(out, *inst)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })
	return out, nil
}

func (f *fakeRepo) instancesByIDs(playerID uint64, ids []uint64) []data.ItemInstance {
	m := f.instances[playerID]
	sorted := append([]uint64(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	out := make([]data.ItemInstance, 0, len(sorted))
	for _, id := range sorted {
		if inst, ok := m[id]; ok {
			out = append(out, *inst)
		}
	}
	return out
}

func (f *fakeRepo) lowestFreeSlot(playerID uint64, capacity int32) (int32, bool) {
	occ := map[int32]struct{}{}
	for _, inst := range f.instances[playerID] {
		if inst.SlotIndex >= 0 {
			occ[inst.SlotIndex] = struct{}{}
		}
	}
	for s := int32(0); s < capacity; s++ {
		if _, taken := occ[s]; !taken {
			return s, true
		}
	}
	return -1, false
}

func (f *fakeRepo) GrantInstances(_ context.Context, playerID uint64, instanceIDs []uint64, itemConfigIDs []uint32, capacity int32, idempotencyKey, _ string) ([]data.ItemInstance, bool, error) {
	gk := keyOf(playerID, idempotencyKey)
	fp := data.GrantInstancesFingerprint(itemConfigIDs)
	if e, ok := f.instGrant[gk]; ok {
		if e.fingerprint != fp {
			return nil, false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "idempotency conflict")
		}
		return f.instancesByIDs(playerID, e.ids), true, nil
	}
	if capacity <= 0 {
		return nil, false, errcode.New(errcode.ErrInventoryCapacityFull, "instance inventory disabled")
	}
	m := f.instMap(playerID)
	if len(m)+len(instanceIDs) > int(capacity) {
		return nil, false, errcode.New(errcode.ErrInventoryCapacityFull, "capacity full")
	}
	out := make([]data.ItemInstance, 0, len(instanceIDs))
	for i, id := range instanceIDs {
		slot, ok := f.lowestFreeSlot(playerID, capacity)
		if !ok {
			return nil, false, errcode.New(errcode.ErrInventoryCapacityFull, "no free slot")
		}
		inst := &data.ItemInstance{InstanceID: id, ItemConfigID: itemConfigIDs[i], SlotIndex: slot}
		m[id] = inst
		out = append(out, *inst)
	}
	f.instGrant[gk] = instGrantEntry{fingerprint: fp, ids: append([]uint64(nil), instanceIDs...)}
	return out, false, nil
}

func (f *fakeRepo) IdentifyInstance(_ context.Context, playerID, instanceID uint64, attrs []data.ItemAttribute) (data.ItemInstance, bool, error) {
	inst, ok := f.instances[playerID][instanceID]
	if !ok {
		return data.ItemInstance{}, false, errcode.New(errcode.ErrInventoryItemNotFound, "instance not found")
	}
	if inst.Identified {
		return *inst, true, nil
	}
	inst.Identified = true
	inst.Attributes = attrs
	return *inst, false, nil
}

func (f *fakeRepo) MoveInstance(_ context.Context, playerID, instanceID uint64, toSlot, capacity int32) (data.ItemInstance, error) {
	if toSlot < 0 || toSlot >= capacity {
		return data.ItemInstance{}, errcode.New(errcode.ErrInventorySlotOccupied, "slot out of range")
	}
	inst, ok := f.instances[playerID][instanceID]
	if !ok {
		return data.ItemInstance{}, errcode.New(errcode.ErrInventoryItemNotFound, "instance not found")
	}
	if inst.SlotIndex == toSlot {
		return *inst, nil
	}
	for id, other := range f.instances[playerID] {
		if id != instanceID && other.SlotIndex == toSlot {
			return data.ItemInstance{}, errcode.New(errcode.ErrInventorySlotOccupied, "slot occupied")
		}
	}
	inst.SlotIndex = toSlot
	return *inst, nil
}

// 保留期清理:biz 单测不模拟时间,默认 no-op(行为断言见 sweep_test.go 的 recording 替身)。
func (f *fakeRepo) SweepLedgerBefore(_ context.Context, mode dbguard.Mode, _, _ int) (dbguard.Outcome, error) {
	return dbguard.Outcome{Mode: mode}, nil
}

func (f *fakeRepo) SweepClosedEscrowBefore(_ context.Context, mode dbguard.Mode, _, _ int) (dbguard.Outcome, error) {
	return dbguard.Outcome{Mode: mode}, nil
}

func (f *fakeRepo) DiscardInstance(_ context.Context, playerID, instanceID uint64) error {
	if inst := f.instances[playerID][instanceID]; inst != nil && inst.Bound {
		return errcode.New(errcode.ErrInventoryInstanceBound, "bound")
	}
	delete(f.instances[playerID], instanceID)
	return nil
}

func (f *fakeRepo) SellInstance(_ context.Context, playerID, instanceID uint64, itemConfigID uint32, kind data.CurrencyKind, amount uint64, idempotencyKey, _ string) (data.SaleOutcome, bool, error) {
	gk := keyOf(playerID, idempotencyKey)
	fp := data.SellInstanceFingerprint(instanceID, itemConfigID)
	if e, ok := f.ledger[gk]; ok {
		if e.fingerprint != fp {
			return data.SaleOutcome{}, false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "idempotency conflict")
		}
		return data.SaleOutcome{
			Remaining: e.snapRemaining, // 实例出售恒 0
			Balances:  e.snapBalances,
			Earned:    e.snapDelta.Get(kind),
			Kind:      kind,
		}, true, nil
	}
	// 同 SellItem:uint64 下 `== 0` 即"不可出售"的完整判定。
	if amount == 0 {
		return data.SaleOutcome{}, false, errcode.New(errcode.ErrInventoryNotSellable, "not sellable")
	}
	inst := f.instances[playerID][instanceID]
	if inst == nil {
		return data.SaleOutcome{}, false, errcode.New(errcode.ErrInventoryItemNotFound, "not found")
	}
	if inst.Bound {
		return data.SaleOutcome{}, false, errcode.New(errcode.ErrInventoryInstanceBound, "bound")
	}
	if inst.ItemConfigID != itemConfigID {
		return data.SaleOutcome{}, false, errcode.New(errcode.ErrInvalidArg, "instance config mismatch")
	}
	delete(f.instances[playerID], instanceID)
	if err := f.addCurrency(playerID, kind, amount); err != nil {
		return data.SaleOutcome{}, false, err
	}
	snap := f.snapshotBalances(playerID)
	f.ledger[gk] = ledgerEntry{
		fingerprint: fp, snapBalances: snap, snapDelta: data.Balances{kind: amount},
	}
	return data.SaleOutcome{Balances: snap, Earned: amount, Kind: kind}, false, nil
}

// PurchaseShopItem 复刻 data/shop_purchase.go 的单事务语义:
// 扣费 → 入包(堆叠计数 / 装备实例)→ 落快照;幂等命中回放首次的扣费额与发货结果。
func (f *fakeRepo) PurchaseShopItem(_ context.Context, playerID uint64, req data.PurchaseRequest) (data.PurchaseOutcome, bool, error) {
	if verr := data.ValidateCurrencyKind(req.Kind); verr != nil {
		return data.PurchaseOutcome{}, false, verr
	}
	// TotalCost 是 uint64:`== 0` 就是"总价必须为正"的完整校验。
	if req.TotalCost == 0 {
		return data.PurchaseOutcome{}, false, errcode.New(errcode.ErrInvalidArg, "purchase total cost must be positive")
	}
	gk := keyOf(playerID, req.IdempotencyKey)
	fp := data.PurchaseFingerprint(req.ShopID, req.ItemConfigID, req.UnitCount)
	if e, ok := f.ledger[gk]; ok {
		if e.fingerprint != fp {
			return data.PurchaseOutcome{}, false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "idempotency conflict")
		}
		// 回放不重扣费也不重发货:发货事实取首次执行落下的 purchaseFacts。
		facts := f.purchases[gk]
		out := data.PurchaseOutcome{
			Balances: e.snapBalances,
			Cost:     e.snapDelta.Get(req.Kind),
			Kind:     req.Kind,
		}
		if facts.totalItems > 0 {
			out.Items = []data.ItemGrant{{ItemConfigID: req.ItemConfigID, Count: facts.totalItems}}
		}
		if len(facts.instanceIDs) > 0 {
			out.Instances = f.instancesByIDs(playerID, facts.instanceIDs)
		}
		return out, true, nil
	}

	if err := f.deductCurrency(playerID, req.Kind, req.TotalCost); err != nil {
		return data.PurchaseOutcome{}, false, err
	}

	var (
		outItems     []data.ItemGrant
		outInstances []data.ItemInstance
		facts        purchaseFacts
	)
	if req.IsEquipment {
		if req.Capacity <= 0 {
			return data.PurchaseOutcome{}, false, errcode.New(errcode.ErrInventoryCapacityFull, "instance inventory disabled")
		}
		m := f.instMap(playerID)
		if len(m)+len(req.InstanceIDs) > int(req.Capacity) {
			return data.PurchaseOutcome{}, false, errcode.New(errcode.ErrInventoryCapacityFull, "capacity full")
		}
		for _, id := range req.InstanceIDs {
			slot, ok := f.lowestFreeSlot(playerID, req.Capacity)
			if !ok {
				return data.PurchaseOutcome{}, false, errcode.New(errcode.ErrInventoryCapacityFull, "no free slot")
			}
			inst := &data.ItemInstance{InstanceID: id, ItemConfigID: req.ItemConfigID, SlotIndex: slot}
			m[id] = inst
			outInstances = append(outInstances, *inst)
		}
		facts.instanceIDs = append([]uint64(nil), req.InstanceIDs...)
	} else {
		if req.TotalItems <= 0 {
			return data.PurchaseOutcome{}, false, errcode.New(errcode.ErrInvalidArg, "purchase item count must be positive")
		}
		if f.items[playerID] == nil {
			f.items[playerID] = map[uint32]int64{}
		}
		f.items[playerID][req.ItemConfigID] += req.TotalItems
		outItems = []data.ItemGrant{{ItemConfigID: req.ItemConfigID, Count: req.TotalItems}}
		facts.totalItems = req.TotalItems
	}

	snap := f.snapshotBalances(playerID)
	f.ledger[gk] = ledgerEntry{
		fingerprint: fp, snapBalances: snap, snapDelta: data.Balances{req.Kind: req.TotalCost},
	}
	f.purchases[gk] = facts
	return data.PurchaseOutcome{
		Balances: snap, Cost: req.TotalCost, Kind: req.Kind,
		Items: outItems, Instances: outInstances,
	}, false, nil
}

// ── 邮件 transfer 托管(2026-07-22)内存实现,复刻 mail_transfer_escrow 事务搬移语义 ──

func (f *fakeRepo) EscrowOutInstances(_ context.Context, sourcePlayerID, toPlayerID uint64, instanceIDs []uint64, escrowKey, _ string) ([]data.EscrowedInstance, bool, error) {
	gk := keyOf(sourcePlayerID, escrowKey)
	fp := data.EscrowOutFingerprint(toPlayerID, instanceIDs)
	toSnapshot := func(row *xferEscrowRow) data.EscrowedInstance {
		return data.EscrowedInstance{
			InstanceID:     row.inst.InstanceID,
			ItemConfigID:   row.inst.ItemConfigID,
			Identified:     row.inst.Identified,
			Attributes:     row.inst.Attributes,
			SourcePlayerID: row.sourcePlayerID,
			ToPlayerID:     row.toPlayerID,
		}
	}
	if stored, ok := f.xferLedger[gk]; ok {
		if stored != fp {
			return nil, false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "idempotency conflict")
		}
		out := make([]data.EscrowedInstance, 0, len(instanceIDs))
		for _, id := range instanceIDs {
			row, ok := f.xferEscrow[id]
			if !ok {
				return nil, false, errcode.New(errcode.ErrInventoryItemNotFound, "escrow replay row missing")
			}
			out = append(out, toSnapshot(row))
		}
		return out, true, nil
	}
	// 先全量校验再搬移(复刻 MySQL 事务整批回滚语义)。
	for _, id := range instanceIDs {
		inst, ok := f.instances[sourcePlayerID][id]
		if !ok {
			return nil, false, errcode.New(errcode.ErrInventoryItemNotFound, "instance not found")
		}
		if inst.Bound {
			return nil, false, errcode.New(errcode.ErrInventoryInstanceBound, "bound instance not transferable")
		}
	}
	out := make([]data.EscrowedInstance, 0, len(instanceIDs))
	for _, id := range instanceIDs {
		inst := f.instances[sourcePlayerID][id]
		row := &xferEscrowRow{inst: *inst, sourcePlayerID: sourcePlayerID, toPlayerID: toPlayerID}
		f.xferEscrow[id] = row
		delete(f.instances[sourcePlayerID], id)
		out = append(out, toSnapshot(row))
	}
	f.xferLedger[gk] = fp
	return out, false, nil
}

func (f *fakeRepo) ClaimTransferInstances(_ context.Context, toPlayerID uint64, items []data.TransferClaimItem, capacity int32, idempotencyKey, _ string) (bool, error) {
	gk := keyOf(toPlayerID, idempotencyKey)
	fp := data.TransferClaimFingerprint(items)
	if stored, ok := f.xferLedger[gk]; ok {
		if stored != fp {
			return false, errcode.New(errcode.ErrInventoryIdempotencyConflict, "idempotency conflict")
		}
		return true, nil
	}
	for _, it := range items {
		row, ok := f.xferEscrow[it.InstanceID]
		if !ok || row.toPlayerID != toPlayerID || row.inst.ItemConfigID != it.ItemConfigID {
			return false, errcode.New(errcode.ErrInventoryItemNotFound, "escrow row missing/mismatch")
		}
	}
	if capacity <= 0 || len(f.instances[toPlayerID])+len(items) > int(capacity) {
		return false, errcode.New(errcode.ErrInventoryCapacityFull, "capacity full")
	}
	for _, it := range items {
		row := f.xferEscrow[it.InstanceID]
		inst := row.inst
		slot, ok := f.lowestFreeSlot(toPlayerID, capacity)
		if !ok {
			return false, errcode.New(errcode.ErrInventoryCapacityFull, "no free slot")
		}
		inst.SlotIndex = slot
		f.instMap(toPlayerID)[it.InstanceID] = &inst
		delete(f.xferEscrow, it.InstanceID)
	}
	f.xferLedger[gk] = fp
	return false, nil
}

func (f *fakeRepo) ReleaseTransferEscrow(_ context.Context, instanceIDs []uint64) (int, error) {
	released := 0
	for _, id := range instanceIDs {
		row, ok := f.xferEscrow[id]
		if !ok {
			continue
		}
		inst := row.inst
		inst.SlotIndex = -1 // 复刻 slot NULL(未分配格)入包
		f.instMap(row.sourcePlayerID)[id] = &inst
		delete(f.xferEscrow, id)
		released++
	}
	return released, nil
}

func (f *fakeRepo) ConsumeTransferEscrow(_ context.Context, toPlayerID uint64, instanceIDs []uint64) (int, error) {
	consumed := 0
	for _, id := range instanceIDs {
		row, ok := f.xferEscrow[id]
		if !ok {
			continue
		}
		if row.toPlayerID != toPlayerID {
			return 0, errcode.New(errcode.ErrInventoryItemNotFound, "escrow not destined to player")
		}
		delete(f.xferEscrow, id)
		consumed++
	}
	return consumed, nil
}

// newUC 构造只做堆叠道具的 usecase。规则来自注入的 catalog(生产是 configtable item
// 表适配器),这里用假表覆盖本文件用到的全部配置 ID:2001 大厅可用、3001 单价 10 可卖,
// 其余仅作可堆叠道具存在。LobbyUsable 在生产适配器里恒 false,此处置真是为了单独驱动
// UseItem 自身的分支,两者的契约差异由 cmd/inventory 的 catalog 测试守住。
func newUC(repo data.InventoryRepo) *InventoryUsecase {
	uc := NewInventoryUsecase(repo, conf.InventoryConf{})
	uc.SetItemCatalog(mapItemCatalog{
		2001: {LobbyUsable: true, MaxStack: 99},
		3001: {SellUnitPrice: 10, MaxStack: 99},
		7001: {MaxStack: 99},
		8002: {MaxStack: 99},
	})
	return uc
}

func TestGrantItems_Idempotent(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	first, err := uc.GrantItems(context.Background(), 100, []data.ItemGrant{{ItemConfigID: 2001, Count: 3}}, goldBalances(50), "drop-m1")
	if err != nil {
		t.Fatalf("first grant err: %v", err)
	}
	if first.Get(data.CurrencyGold) != 50 {
		t.Fatalf("first grant gold want 50, got %d", first.Get(data.CurrencyGold))
	}
	second, err := uc.GrantItems(context.Background(), 100, []data.ItemGrant{{ItemConfigID: 2001, Count: 3}}, goldBalances(50), "drop-m1")
	if err != nil {
		t.Fatalf("second grant err: %v", err)
	}
	if second.Get(data.CurrencyGold) != 50 {
		t.Fatalf("idempotent grant should not double-add gold, want 50, got %d", second.Get(data.CurrencyGold))
	}
	if repo.items[100][2001] != 3 {
		t.Fatalf("idempotent grant should not double-add items, want 3, got %d", repo.items[100][2001])
	}
}

func TestGrantItems_Validation(t *testing.T) {
	uc := newUC(newFakeRepo())
	if _, err := uc.GrantItems(context.Background(), 100, nil, nil, "k"); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("nothing to grant should be ErrInvalidArg, got %v", err)
	}
	if _, err := uc.GrantItems(context.Background(), 100, []data.ItemGrant{{ItemConfigID: 2001, Count: 0}}, nil, "k"); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("non-positive count should be ErrInvalidArg, got %v", err)
	}
	if _, err := uc.GrantItems(context.Background(), 100, nil, goldBalances(5), ""); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("empty key should be ErrInvalidArg, got %v", err)
	}
}

func TestUseItem_NotUsable(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	// 3001 是 sellable 但非 usable。
	if _, err := uc.GrantItems(context.Background(), 100, []data.ItemGrant{{ItemConfigID: 3001, Count: 5}}, nil, "g1"); err != nil {
		t.Fatalf("grant err: %v", err)
	}
	_, err := uc.UseItem(context.Background(), 100, 3001, 1, "use1")
	if errcode.As(err) != errcode.ErrInventoryItemNotUsable {
		t.Fatalf("non-usable item should be ErrInventoryItemNotUsable, got %v", err)
	}
}

func TestUseItem_Insufficient(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	if _, err := uc.GrantItems(context.Background(), 100, []data.ItemGrant{{ItemConfigID: 2001, Count: 1}}, nil, "g1"); err != nil {
		t.Fatalf("grant err: %v", err)
	}
	_, err := uc.UseItem(context.Background(), 100, 2001, 5, "use1")
	if errcode.As(err) != errcode.ErrInventoryInsufficient {
		t.Fatalf("over-use should be ErrInventoryInsufficient, got %v", err)
	}
}

func TestUseItem_Success(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	if _, err := uc.GrantItems(context.Background(), 100, []data.ItemGrant{{ItemConfigID: 2001, Count: 3}}, nil, "g1"); err != nil {
		t.Fatalf("grant err: %v", err)
	}
	remaining, err := uc.UseItem(context.Background(), 100, 2001, 2, "use1")
	if err != nil {
		t.Fatalf("use err: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("after use 2 of 3, remaining want 1, got %d", remaining)
	}
}

func TestSellItem_NotSellable(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	if _, err := uc.GrantItems(context.Background(), 100, []data.ItemGrant{{ItemConfigID: 2001, Count: 5}}, nil, "g1"); err != nil {
		t.Fatalf("grant err: %v", err)
	}
	_, err := uc.SellItem(context.Background(), 100, 2001, 1, "sell1")
	if errcode.As(err) != errcode.ErrInventoryNotSellable {
		t.Fatalf("non-sellable item should be ErrInventoryNotSellable, got %v", err)
	}
}

func TestSellItem_SuccessGivesGold(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	if _, err := uc.GrantItems(context.Background(), 100, []data.ItemGrant{{ItemConfigID: 3001, Count: 5}}, nil, "g1"); err != nil {
		t.Fatalf("grant err: %v", err)
	}
	out, err := uc.SellItem(context.Background(), 100, 3001, 2, "sell1")
	if err != nil {
		t.Fatalf("sell err: %v", err)
	}
	if out.Remaining != 3 {
		t.Fatalf("after sell 2 of 5, remaining want 3, got %d", out.Remaining)
	}
	// 余额是"卖完之后玩家有多少",Earned 是"这一笔挣了多少",两者是不同的事实,分别断言。
	if got := out.Balances.Get(data.CurrencyGold); got != 20 {
		t.Fatalf("sell 2 @ 10 should leave 20 gold balance, got %d", got)
	}
	if out.Earned != 20 {
		t.Fatalf("sell 2 @ 10 should earn 20, got %d", out.Earned)
	}
	if out.Kind != data.CurrencyGold {
		t.Fatalf("结算币种应为金币: %v", out.Kind)
	}
}

func TestSellItem_Idempotent(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	if _, err := uc.GrantItems(context.Background(), 100, []data.ItemGrant{{ItemConfigID: 3001, Count: 5}}, nil, "g1"); err != nil {
		t.Fatalf("grant err: %v", err)
	}
	if _, err := uc.SellItem(context.Background(), 100, 3001, 2, "sell1"); err != nil {
		t.Fatalf("first sell err: %v", err)
	}
	out, err := uc.SellItem(context.Background(), 100, 3001, 2, "sell1")
	if err != nil {
		t.Fatalf("second sell err: %v", err)
	}
	if out.Remaining != 3 || out.Balances.Get(data.CurrencyGold) != 20 {
		t.Fatalf("idempotent sell should not double-apply, want remaining=3 gold=20, got %+v", out)
	}
	// 回放的 Earned 必须是**当初那一笔的金额**(20),不是 0:
	// 客户端可能因响应丢失重试,拿到 0 会显示"这次白卖了"。
	if out.Earned != 20 {
		t.Fatalf("幂等重放 earned 必须回放首次金额 20, got %d", out.Earned)
	}
}

func TestGrantItems_IdempotencyConflict(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	if _, err := uc.GrantItems(context.Background(), 100, []data.ItemGrant{{ItemConfigID: 2001, Count: 3}}, goldBalances(50), "drop-m1"); err != nil {
		t.Fatalf("first grant err: %v", err)
	}
	// 同 idempotency_key 不同请求参数 → 冲突,而非静默回放旧结果。
	_, err := uc.GrantItems(context.Background(), 100, []data.ItemGrant{{ItemConfigID: 2001, Count: 999}}, goldBalances(50), "drop-m1")
	if errcode.As(err) != errcode.ErrInventoryIdempotencyConflict {
		t.Fatalf("same key different request should be ErrInventoryIdempotencyConflict, got %v", err)
	}
	if repo.items[100][2001] != 3 {
		t.Fatalf("conflict must not apply second request, want 3, got %d", repo.items[100][2001])
	}
}

func TestSellItem_ReplayReturnsSnapshot(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	if _, err := uc.GrantItems(context.Background(), 100, []data.ItemGrant{{ItemConfigID: 3001, Count: 5}}, nil, "g1"); err != nil {
		t.Fatalf("grant err: %v", err)
	}
	if _, err := uc.SellItem(context.Background(), 100, 3001, 2, "sell1"); err != nil {
		t.Fatalf("first sell err: %v", err)
	}
	// 首次卖后再卖 1 个(不同 key),改变当前库存/金币;随后回放 sell1 必须返回首次快照,而非当前状态。
	if _, err := uc.SellItem(context.Background(), 100, 3001, 1, "sell2"); err != nil {
		t.Fatalf("second sell err: %v", err)
	}
	out, err := uc.SellItem(context.Background(), 100, 3001, 2, "sell1")
	if err != nil {
		t.Fatalf("replay sell err: %v", err)
	}
	// 当前真实金币已是 30(20+10),回放必须返回首次的 20 —— 这正是 result_currencies
	// 存整份余额快照而不是"当前余额"的原因。
	if out.Remaining != 3 || out.Balances.Get(data.CurrencyGold) != 20 {
		t.Fatalf("replay must return first-time snapshot remaining=3 gold=20, got %+v", out)
	}
	if out.Earned != 20 {
		t.Fatalf("replay earned 必须是首次那一笔 20, got %d", out.Earned)
	}
	// 反证快照没被当前状态污染:当前余额确实已经涨到 30。
	if got := repo.goldOf(100); got != 30 {
		t.Fatalf("当前真实金币应为 30(两次出售累计), got %d", got)
	}
}

func TestSettleAuctionMatch_Success(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	ctx := context.Background()
	// 卖家(10)持 5 个道具 7001;买家(20)持 1000 金币。
	if _, err := uc.GrantItems(ctx, 10, []data.ItemGrant{{ItemConfigID: 7001, Count: 5}}, nil, "seed-seller"); err != nil {
		t.Fatalf("seed seller err: %v", err)
	}
	if _, err := uc.GrantItems(ctx, 20, nil, goldBalances(1000), "seed-buyer"); err != nil {
		t.Fatalf("seed buyer err: %v", err)
	}
	// 卖家挂单冻结 3 个道具(sell order 501);买家出价冻结 3*100 金币(buy order 601)。
	if err := uc.FreezeForOrder(ctx, 10, 501, EscrowSideSell, 7001, 3, data.CurrencyGold, 100); err != nil {
		t.Fatalf("freeze seller err: %v", err)
	}
	if err := uc.FreezeForOrder(ctx, 20, 601, EscrowSideBuy, 7001, 3, data.CurrencyGold, 100); err != nil {
		t.Fatalf("freeze buyer err: %v", err)
	}
	// 冻结后活跃余额已扣减。
	if repo.items[10][7001] != 2 {
		t.Fatalf("after freeze seller active item want 2, got %d", repo.items[10][7001])
	}
	if repo.goldOf(20) != 700 {
		t.Fatalf("after freeze buyer active gold want 700, got %d", repo.goldOf(20))
	}
	// 成交:卖家交付 3 个 @ 单价 100 = 300 金币。
	if err := uc.SettleAuctionMatch(ctx, 999, 10, 20, 501, 601, 7001, 3, data.CurrencyGold, 100); err != nil {
		t.Fatalf("settle err: %v", err)
	}
	if repo.items[20][7001] != 3 {
		t.Fatalf("buyer item want 3, got %d", repo.items[20][7001])
	}
	if repo.goldOf(10) != 300 {
		t.Fatalf("seller gold want 300, got %d", repo.goldOf(10))
	}
	// 买家金币 = 700(冻结后剩余),300 已从 escrow 付给卖家。
	if repo.goldOf(20) != 700 {
		t.Fatalf("buyer gold want 700, got %d", repo.goldOf(20))
	}
}

func TestSettleAuctionMatch_Idempotent(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	ctx := context.Background()
	if _, err := uc.GrantItems(ctx, 10, []data.ItemGrant{{ItemConfigID: 7001, Count: 5}}, nil, "seed-seller"); err != nil {
		t.Fatalf("seed seller err: %v", err)
	}
	if _, err := uc.GrantItems(ctx, 20, nil, goldBalances(1000), "seed-buyer"); err != nil {
		t.Fatalf("seed buyer err: %v", err)
	}
	if err := uc.FreezeForOrder(ctx, 10, 501, EscrowSideSell, 7001, 3, data.CurrencyGold, 100); err != nil {
		t.Fatalf("freeze seller err: %v", err)
	}
	if err := uc.FreezeForOrder(ctx, 20, 601, EscrowSideBuy, 7001, 3, data.CurrencyGold, 100); err != nil {
		t.Fatalf("freeze buyer err: %v", err)
	}
	if err := uc.SettleAuctionMatch(ctx, 999, 10, 20, 501, 601, 7001, 3, data.CurrencyGold, 100); err != nil {
		t.Fatalf("first settle err: %v", err)
	}
	// 重复结算同一 match_id:资产不可二次转移。
	if err := uc.SettleAuctionMatch(ctx, 999, 10, 20, 501, 601, 7001, 3, data.CurrencyGold, 100); err != nil {
		t.Fatalf("idempotent settle err: %v", err)
	}
	if repo.items[20][7001] != 3 || repo.goldOf(10) != 300 || repo.goldOf(20) != 700 {
		t.Fatalf("idempotent settle must not double-transfer: buyerItem=%d sellerGold=%d buyerGold=%d",
			repo.items[20][7001], repo.goldOf(10), repo.goldOf(20))
	}
}

func TestSettlePlayerTrade_OK(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	ctx := context.Background()
	// 卖家 10 持有 5 个 7001;买家 20 持有 1000 金币 + 2 个 8002(回付道具)。
	if _, err := uc.GrantItems(ctx, 10, []data.ItemGrant{{ItemConfigID: 7001, Count: 5}}, nil, "seed-seller"); err != nil {
		t.Fatalf("seed seller err: %v", err)
	}
	if _, err := uc.GrantItems(ctx, 20, []data.ItemGrant{{ItemConfigID: 8002, Count: 2}}, goldBalances(1000), "seed-buyer"); err != nil {
		t.Fatalf("seed buyer err: %v", err)
	}
	// 交易:卖家给 3 个 7001;买家给 2 个 8002 + 300 金币。
	err := uc.SettlePlayerTrade(ctx, 12345, 10, 20,
		[]data.ItemGrant{{ItemConfigID: 7001, Count: 3}},
		[]data.ItemGrant{{ItemConfigID: 8002, Count: 2}}, data.CurrencyGold, 300)
	if err != nil {
		t.Fatalf("settle err: %v", err)
	}
	if repo.items[10][7001] != 2 || repo.items[20][7001] != 3 {
		t.Fatalf("item 7001 transfer wrong: seller=%d buyer=%d", repo.items[10][7001], repo.items[20][7001])
	}
	if repo.items[20][8002] != 0 || repo.items[10][8002] != 2 {
		t.Fatalf("item 8002 transfer wrong: buyer=%d seller=%d", repo.items[20][8002], repo.items[10][8002])
	}
	if repo.goldOf(10) != 300 || repo.goldOf(20) != 700 {
		t.Fatalf("gold transfer wrong: seller=%d buyer=%d", repo.goldOf(10), repo.goldOf(20))
	}
}

func TestSettlePlayerTrade_Insufficient(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	ctx := context.Background()
	// 卖家只有 1 个,交易要给 3 个 → 不足。
	if _, err := uc.GrantItems(ctx, 10, []data.ItemGrant{{ItemConfigID: 7001, Count: 1}}, nil, "seed-seller"); err != nil {
		t.Fatalf("seed seller err: %v", err)
	}
	if _, err := uc.GrantItems(ctx, 20, nil, goldBalances(1000), "seed-buyer"); err != nil {
		t.Fatalf("seed buyer err: %v", err)
	}
	err := uc.SettlePlayerTrade(ctx, 12345, 10, 20,
		[]data.ItemGrant{{ItemConfigID: 7001, Count: 3}}, nil, data.CurrencyGold, 300)
	if errcode.As(err) != errcode.ErrInventoryInsufficient {
		t.Fatalf("want ErrInventoryInsufficient, got %v", err)
	}
}

func TestSettlePlayerTrade_Idempotent(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	ctx := context.Background()
	if _, err := uc.GrantItems(ctx, 10, []data.ItemGrant{{ItemConfigID: 7001, Count: 5}}, nil, "seed-seller"); err != nil {
		t.Fatalf("seed seller err: %v", err)
	}
	if _, err := uc.GrantItems(ctx, 20, nil, goldBalances(1000), "seed-buyer"); err != nil {
		t.Fatalf("seed buyer err: %v", err)
	}
	settle := func() error {
		return uc.SettlePlayerTrade(ctx, 12345, 10, 20,
			[]data.ItemGrant{{ItemConfigID: 7001, Count: 3}}, nil, data.CurrencyGold, 300)
	}
	if err := settle(); err != nil {
		t.Fatalf("first settle err: %v", err)
	}
	if err := settle(); err != nil {
		t.Fatalf("idempotent settle err: %v", err)
	}
	// 重复结算同一 order_id:资产不可二次转移。
	if repo.items[10][7001] != 2 || repo.items[20][7001] != 3 || repo.goldOf(10) != 300 || repo.goldOf(20) != 700 {
		t.Fatalf("idempotent settle must not double-transfer: sellerItem=%d buyerItem=%d sellerGold=%d buyerGold=%d",
			repo.items[10][7001], repo.items[20][7001], repo.goldOf(10), repo.goldOf(20))
	}
}

func TestFreezeForOrder_ItemInsufficient(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	ctx := context.Background()
	// 卖家只有 1 个,挂 3 个 → 冻结失败(挂单阶段就拦下,不会进簿)。
	if _, err := uc.GrantItems(ctx, 10, []data.ItemGrant{{ItemConfigID: 7001, Count: 1}}, nil, "seed-seller"); err != nil {
		t.Fatalf("seed seller err: %v", err)
	}
	if err := uc.FreezeForOrder(ctx, 10, 501, EscrowSideSell, 7001, 3, data.CurrencyGold, 100); errcode.As(err) != errcode.ErrInventoryInsufficient {
		t.Fatalf("freeze item insufficient should be ErrInventoryInsufficient, got %v", err)
	}
	// 失败后活跃余额未被扣。
	if repo.items[10][7001] != 1 {
		t.Fatalf("active item must be untouched on freeze failure, got %d", repo.items[10][7001])
	}
}

func TestFreezeForOrder_GoldInsufficient(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	ctx := context.Background()
	if _, err := uc.GrantItems(ctx, 20, nil, goldBalances(100), "seed-buyer"); err != nil {
		t.Fatalf("seed buyer err: %v", err)
	}
	// 出价冻结需要 300,只有 100 → 失败。
	if err := uc.FreezeForOrder(ctx, 20, 601, EscrowSideBuy, 7001, 3, data.CurrencyGold, 100); errcode.As(err) != errcode.ErrInventoryInsufficient {
		t.Fatalf("freeze gold insufficient should be ErrInventoryInsufficient, got %v", err)
	}
	if repo.goldOf(20) != 100 {
		t.Fatalf("active gold must be untouched on freeze failure, got %d", repo.goldOf(20))
	}
}

func TestFreezeForOrder_Idempotent(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	ctx := context.Background()
	if _, err := uc.GrantItems(ctx, 10, []data.ItemGrant{{ItemConfigID: 7001, Count: 5}}, nil, "seed-seller"); err != nil {
		t.Fatalf("seed seller err: %v", err)
	}
	if err := uc.FreezeForOrder(ctx, 10, 501, EscrowSideSell, 7001, 3, data.CurrencyGold, 100); err != nil {
		t.Fatalf("first freeze err: %v", err)
	}
	// 重复冻结同一 order:只扣一次。
	if err := uc.FreezeForOrder(ctx, 10, 501, EscrowSideSell, 7001, 3, data.CurrencyGold, 100); err != nil {
		t.Fatalf("idempotent freeze err: %v", err)
	}
	if repo.items[10][7001] != 2 {
		t.Fatalf("idempotent freeze must deduct once: active item want 2, got %d", repo.items[10][7001])
	}
}

func TestEnsureAuctionEscrow_ExistingActiveIsValidatedWithoutRefreeze(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	ctx := context.Background()
	if _, err := uc.GrantItems(ctx, 10, []data.ItemGrant{{ItemConfigID: 7001, Count: 5}}, nil, "seed-seller"); err != nil {
		t.Fatalf("seed seller: %v", err)
	}
	if err := uc.FreezeForOrder(ctx, 10, 501, EscrowSideSell, 7001, 4, data.CurrencyGold, 100); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if err := uc.EnsureAuctionEscrow(ctx, 10, 501, EscrowSideSell, 7001, 3, data.CurrencyGold, 100); err != nil {
		t.Fatalf("ensure existing: %v", err)
	}
	if got := repo.items[10][7001]; got != 1 {
		t.Fatalf("已有 escrow 不得再次扣活跃道具: got=%d want=1", got)
	}
	if got := repo.escrow[escrowKeyOf(10, 501)].frozenQty; got != 4 {
		t.Fatalf("已有 escrow 余量不得被 ensure 改写: got=%d want=4", got)
	}
}

func TestEnsureAuctionEscrow_MissingEscrowFreezesRemainingAssets(t *testing.T) {
	t.Run("sell", func(t *testing.T) {
		repo := newFakeRepo()
		uc := newUC(repo)
		if _, err := uc.GrantItems(context.Background(), 10,
			[]data.ItemGrant{{ItemConfigID: 7001, Count: 5}}, nil, "seed-seller"); err != nil {
			t.Fatalf("seed seller: %v", err)
		}
		if err := uc.EnsureAuctionEscrow(context.Background(), 10, 501,
			EscrowSideSell, 7001, 3, data.CurrencyGold, 100); err != nil {
			t.Fatalf("ensure sell: %v", err)
		}
		if got := repo.items[10][7001]; got != 2 {
			t.Fatalf("active item=%d want=2", got)
		}
		if got := repo.escrow[escrowKeyOf(10, 501)].frozenQty; got != 3 {
			t.Fatalf("frozen item=%d want=3", got)
		}
	})

	t.Run("buy", func(t *testing.T) {
		repo := newFakeRepo()
		uc := newUC(repo)
		if _, err := uc.GrantItems(context.Background(), 20, nil, goldBalances(1000), "seed-buyer"); err != nil {
			t.Fatalf("seed buyer: %v", err)
		}
		if err := uc.EnsureAuctionEscrow(context.Background(), 20, 601,
			EscrowSideBuy, 7001, 3, data.CurrencyGold, 100); err != nil {
			t.Fatalf("ensure buy: %v", err)
		}
		if got := repo.goldOf(20); got != 700 {
			t.Fatalf("active gold=%d want=700", got)
		}
		if got := repo.escrow[escrowKeyOf(20, 601)].frozenAmount; got != 300 {
			t.Fatalf("frozen gold=%d want=300", got)
		}
	})
}

func TestEnsureAuctionEscrow_RejectsInsufficientMismatchAndClosed(t *testing.T) {
	t.Run("missing-insufficient", func(t *testing.T) {
		repo := newFakeRepo()
		uc := newUC(repo)
		if _, err := uc.GrantItems(context.Background(), 10,
			[]data.ItemGrant{{ItemConfigID: 7001, Count: 1}}, nil, "seed"); err != nil {
			t.Fatalf("seed: %v", err)
		}
		err := uc.EnsureAuctionEscrow(context.Background(), 10, 501,
			EscrowSideSell, 7001, 2, data.CurrencyGold, 100)
		if errcode.As(err) != errcode.ErrInventoryInsufficient {
			t.Fatalf("want ErrInventoryInsufficient, got %v", err)
		}
		if _, ok := repo.escrow[escrowKeyOf(10, 501)]; ok {
			t.Fatal("补冻失败不得留下 escrow")
		}
		if got := repo.items[10][7001]; got != 1 {
			t.Fatalf("补冻失败不得扣活跃资产: got=%d", got)
		}
	})

	t.Run("identity-mismatch", func(t *testing.T) {
		repo := newFakeRepo()
		uc := newUC(repo)
		repo.escrow[escrowKeyOf(10, 501)] = &escrowEntry{
			kind: data.EscrowKindItem, itemConfigID: 7001, frozenQty: 3,
		}
		err := uc.EnsureAuctionEscrow(context.Background(), 10, 501,
			EscrowSideSell, 7002, 2, data.CurrencyGold, 100)
		if errcode.As(err) != errcode.ErrInventoryIdempotencyConflict {
			t.Fatalf("want ErrInventoryIdempotencyConflict, got %v", err)
		}
	})

	t.Run("closed", func(t *testing.T) {
		repo := newFakeRepo()
		uc := newUC(repo)
		repo.escrow[escrowKeyOf(10, 501)] = &escrowEntry{
			kind: data.EscrowKindItem, itemConfigID: 7001, closed: true,
		}
		err := uc.EnsureAuctionEscrow(context.Background(), 10, 501,
			EscrowSideSell, 7001, 1, data.CurrencyGold, 100)
		if errcode.As(err) != errcode.ErrInventoryIdempotencyConflict {
			t.Fatalf("want ErrInventoryIdempotencyConflict, got %v", err)
		}
	})

	t.Run("existing-short", func(t *testing.T) {
		repo := newFakeRepo()
		uc := newUC(repo)
		repo.escrow[escrowKeyOf(10, 501)] = &escrowEntry{
			kind: data.EscrowKindItem, itemConfigID: 7001, frozenQty: 1,
		}
		err := uc.EnsureAuctionEscrow(context.Background(), 10, 501,
			EscrowSideSell, 7001, 2, data.CurrencyGold, 100)
		if errcode.As(err) != errcode.ErrInventoryInsufficient {
			t.Fatalf("want ErrInventoryInsufficient, got %v", err)
		}
	})
}

func TestEnsureAuctionEscrow_ConcurrentIdempotent(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	ctx := context.Background()
	if _, err := uc.GrantItems(ctx, 10,
		[]data.ItemGrant{{ItemConfigID: 7001, Count: 5}}, nil, "seed"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const workers = 16
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- uc.EnsureAuctionEscrow(ctx, 10, 501, EscrowSideSell, 7001, 5, data.CurrencyGold, 100)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent ensure: %v", err)
		}
	}
	if got := repo.items[10][7001]; got != 0 {
		t.Fatalf("并发 ensure 只能扣一次: active=%d want=0", got)
	}
	if got := repo.escrow[escrowKeyOf(10, 501)].frozenQty; got != 5 {
		t.Fatalf("并发 ensure 只能创建一份 escrow: frozen=%d want=5", got)
	}
}

func TestReleaseEscrow_RefundsRemaining(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	ctx := context.Background()
	if _, err := uc.GrantItems(ctx, 10, []data.ItemGrant{{ItemConfigID: 7001, Count: 5}}, nil, "seed-seller"); err != nil {
		t.Fatalf("seed seller err: %v", err)
	}
	// 冻 3 个道具(活跃剩 2)。
	if err := uc.FreezeForOrder(ctx, 10, 501, EscrowSideSell, 7001, 3, data.CurrencyGold, 100); err != nil {
		t.Fatalf("freeze err: %v", err)
	}
	// 撤单退还 → 活跃恢复 5。
	if err := uc.ReleaseEscrow(ctx, 10, 501); err != nil {
		t.Fatalf("release err: %v", err)
	}
	if repo.items[10][7001] != 5 {
		t.Fatalf("after release active item want 5, got %d", repo.items[10][7001])
	}
	// 重复退还幂等:不二次返还。
	if err := uc.ReleaseEscrow(ctx, 10, 501); err != nil {
		t.Fatalf("idempotent release err: %v", err)
	}
	if repo.items[10][7001] != 5 {
		t.Fatalf("idempotent release must not double-refund, got %d", repo.items[10][7001])
	}
}

// ── 装备实例 / 鉴定单测(W5 ④)──

// seqGen 是确定性 instance_id 生成器(测试用,从 base 递增)。
type seqGen struct{ next uint64 }

func (g *seqGen) Generate() uint64 { g.next++; return g.next }

// GenerateInto 与真实 snowflake.Node 同语义:逐个取,保证严格递增且唯一。
func (g *seqGen) GenerateInto(dst []uint64) {
	for i := range dst {
		dst[i] = g.Generate()
	}
}

// newInstanceUC 构造带实例背包(容量 4)+ 鉴定规则(道具 5001 从 3 属性池抽 2 条)的 usecase,
// 注入确定性 snowflake + 确定性随机源(randIntn 恒返 0 → 每轮选剩余第一条、数值取下界),便于断言。
func newInstanceUC() *InventoryUsecase {
	uc := NewInventoryUsecase(newFakeRepo(), conf.InventoryConf{
		Capacity: 4,
		IdentifyRules: []conf.IdentifyRule{{
			ItemConfigID: 5001,
			AttrCount:    2,
			Pool: []conf.IdentifyAttrRoll{
				{AttrID: 101, Min: 10, Max: 20},
				{AttrID: 102, Min: 5, Max: 5},
				{AttrID: 103, Min: 1, Max: 100},
			},
		}},
	})
	uc.SetSnowflake(&seqGen{})
	uc.SetRandSource(func(int64) int64 { return 0 })
	return uc
}

func TestGrantInstances_AssignsSlotsAndCapacity(t *testing.T) {
	uc := newInstanceUC()
	ctx := context.Background()
	insts, err := uc.GrantInstances(ctx, 100, []uint32{5001, 5002}, "drop-m1")
	if err != nil {
		t.Fatalf("grant instances err: %v", err)
	}
	if len(insts) != 2 {
		t.Fatalf("want 2 instances, got %d", len(insts))
	}
	if insts[0].SlotIndex != 0 || insts[1].SlotIndex != 1 {
		t.Fatalf("want slots 0,1, got %d,%d", insts[0].SlotIndex, insts[1].SlotIndex)
	}
	if insts[0].Identified {
		t.Fatalf("newly granted instance must be unidentified")
	}
	// 容量 4:再发 3 件 → 超容量拒。
	if _, err := uc.GrantInstances(ctx, 100, []uint32{5001, 5001, 5001}, "drop-m2"); errcode.As(err) != errcode.ErrInventoryCapacityFull {
		t.Fatalf("over-capacity grant should be ErrInventoryCapacityFull, got %v", err)
	}
}

func TestGrantInstances_Idempotent(t *testing.T) {
	uc := newInstanceUC()
	ctx := context.Background()
	first, err := uc.GrantInstances(ctx, 100, []uint32{5001}, "drop-m1")
	if err != nil {
		t.Fatalf("first grant err: %v", err)
	}
	second, err := uc.GrantInstances(ctx, 100, []uint32{5001}, "drop-m1")
	if err != nil {
		t.Fatalf("replay grant err: %v", err)
	}
	if len(second) != 1 || second[0].InstanceID != first[0].InstanceID {
		t.Fatalf("idempotent grant must replay same instance, first=%d second=%v", first[0].InstanceID, second)
	}
	// 只发一件,不重复创建。
	_, _, capacity, instances, _ := uc.GetInventoryFull(ctx, 100)
	if capacity != 4 || len(instances) != 1 {
		t.Fatalf("want capacity 4 and 1 instance, got cap=%d n=%d", capacity, len(instances))
	}
}

func TestIdentifyItem_RollsAttributesAndIdempotent(t *testing.T) {
	uc := newInstanceUC()
	ctx := context.Background()
	insts, err := uc.GrantInstances(ctx, 100, []uint32{5001}, "drop-m1")
	if err != nil {
		t.Fatalf("grant err: %v", err)
	}
	id := insts[0].InstanceID
	got, err := uc.IdentifyItem(ctx, 100, id)
	if err != nil {
		t.Fatalf("identify err: %v", err)
	}
	if !got.Identified {
		t.Fatalf("identified flag must be set")
	}
	// 旧 YAML 兼容规则按等权加权不放回；randIntn 恒 0 时依次取 pool[0]/pool[1]，
	// 每条数值取区间下界(101→10,102→5)。
	if len(got.Attributes) != 2 {
		t.Fatalf("want 2 rolled attrs, got %d", len(got.Attributes))
	}
	if got.Attributes[0].AttrID != 101 || got.Attributes[0].Value != 10 {
		t.Fatalf("attr0 want {101,10}, got %+v", got.Attributes[0])
	}
	if got.Attributes[1].AttrID != 102 || got.Attributes[1].Value != 5 {
		t.Fatalf("attr1 want {102,5}, got %+v", got.Attributes[1])
	}
	// 幂等:再次鉴定回放同属性,不 re-roll。
	again, err := uc.IdentifyItem(ctx, 100, id)
	if err != nil {
		t.Fatalf("re-identify err: %v", err)
	}
	if len(again.Attributes) != 2 || again.Attributes[0].Value != 10 {
		t.Fatalf("re-identify must replay, got %+v", again.Attributes)
	}
}

func TestIdentifyItem_NotFound(t *testing.T) {
	uc := newInstanceUC()
	if _, err := uc.IdentifyItem(context.Background(), 100, 99999); errcode.As(err) != errcode.ErrInventoryItemNotFound {
		t.Fatalf("identify missing instance should be ErrInventoryItemNotFound, got %v", err)
	}
}

func TestMoveInstance_SlotOccupied(t *testing.T) {
	uc := newInstanceUC()
	ctx := context.Background()
	insts, err := uc.GrantInstances(ctx, 100, []uint32{5001, 5002}, "drop-m1")
	if err != nil {
		t.Fatalf("grant err: %v", err)
	}
	// insts[0] 在格 0,insts[1] 在格 1。把 insts[1] 移到格 0(被占)→ 拒。
	if _, err := uc.MoveInstance(ctx, 100, insts[1].InstanceID, 0); errcode.As(err) != errcode.ErrInventorySlotOccupied {
		t.Fatalf("move onto occupied slot should be ErrInventorySlotOccupied, got %v", err)
	}
	// 移到空格 2 → OK。
	moved, err := uc.MoveInstance(ctx, 100, insts[1].InstanceID, 2)
	if err != nil {
		t.Fatalf("move to free slot err: %v", err)
	}
	if moved.SlotIndex != 2 {
		t.Fatalf("want slot 2, got %d", moved.SlotIndex)
	}
}

func TestDiscardInstance_Idempotent(t *testing.T) {
	uc := newInstanceUC()
	ctx := context.Background()
	insts, err := uc.GrantInstances(ctx, 100, []uint32{5001}, "drop-m1")
	if err != nil {
		t.Fatalf("grant err: %v", err)
	}
	id := insts[0].InstanceID
	if err := uc.DiscardInstance(ctx, 100, id); err != nil {
		t.Fatalf("discard err: %v", err)
	}
	// 再次丢弃 no-op。
	if err := uc.DiscardInstance(ctx, 100, id); err != nil {
		t.Fatalf("idempotent discard err: %v", err)
	}
	_, _, _, instances, _ := uc.GetInventoryFull(ctx, 100)
	if len(instances) != 0 {
		t.Fatalf("want 0 instances after discard, got %d", len(instances))
	}
}

func TestReleaseEscrow_BuyerPriceImprovement(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	ctx := context.Background()
	if _, err := uc.GrantItems(ctx, 10, []data.ItemGrant{{ItemConfigID: 7001, Count: 5}}, nil, "seed-seller"); err != nil {
		t.Fatalf("seed seller err: %v", err)
	}
	if _, err := uc.GrantItems(ctx, 20, nil, goldBalances(1000), "seed-buyer"); err != nil {
		t.Fatalf("seed buyer err: %v", err)
	}
	// 卖家挂卖单单价 80;买家出价单价 100 冻 3*100=300 金币(活跃剩 700)。
	if err := uc.FreezeForOrder(ctx, 10, 501, EscrowSideSell, 7001, 3, data.CurrencyGold, 80); err != nil {
		t.Fatalf("freeze seller err: %v", err)
	}
	if err := uc.FreezeForOrder(ctx, 20, 601, EscrowSideBuy, 7001, 3, data.CurrencyGold, 100); err != nil {
		t.Fatalf("freeze buyer err: %v", err)
	}
	// 成交价 = 被动卖单价 80。买家实付 3*80=240,escrow 残余 300-240=60。
	if err := uc.SettleAuctionMatch(ctx, 999, 10, 20, 501, 601, 7001, 3, data.CurrencyGold, 80); err != nil {
		t.Fatalf("settle err: %v", err)
	}
	if repo.goldOf(10) != 240 {
		t.Fatalf("seller gold want 240, got %d", repo.goldOf(10))
	}
	// 买单完全成交后退还价差 60 → 买家活跃金币 700+60=760。
	if err := uc.ReleaseEscrow(ctx, 20, 601); err != nil {
		t.Fatalf("release buyer err: %v", err)
	}
	if repo.goldOf(20) != 760 {
		t.Fatalf("buyer gold after price-improvement refund want 760, got %d", repo.goldOf(20))
	}
}

func TestSettleAuctionMatch_Validation(t *testing.T) {
	uc := newUC(newFakeRepo())
	ctx := context.Background()
	if err := uc.SettleAuctionMatch(ctx, 0, 10, 20, 501, 601, 7001, 1, data.CurrencyGold, 1); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("zero match_id should be ErrInvalidArg, got %v", err)
	}
	if err := uc.SettleAuctionMatch(ctx, 1, 10, 10, 501, 601, 7001, 1, data.CurrencyGold, 1); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("self-trade should be ErrInvalidArg, got %v", err)
	}
	if err := uc.SettleAuctionMatch(ctx, 1, 10, 20, 0, 601, 7001, 1, data.CurrencyGold, 1); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("zero sell_order_id should be ErrInvalidArg, got %v", err)
	}
	if err := uc.SettleAuctionMatch(ctx, 1, 10, 20, 501, 601, 7001, 0, data.CurrencyGold, 1); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("zero quantity should be ErrInvalidArg, got %v", err)
	}
	if err := uc.SettleAuctionMatch(ctx, 1, 10, 20, 501, 601, 7001, 1, data.CurrencyGold, 0); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("zero unit_price should be ErrInvalidArg, got %v", err)
	}
}
