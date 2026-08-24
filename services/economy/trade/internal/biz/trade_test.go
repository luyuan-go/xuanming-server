// trade_test.go — TradeUsecase 业务逻辑单测(2026-06-16)。
//
// 用内存版 fakeRepo / fakeLedger / fakeAudit 复刻 Redis + 账本 + kafka 语义,无需真依赖。
// 覆盖:挂单 → 买方确认 → 卖方确认结算闭环 + 自交易 / 空物品 / 越权确认 / 顺序错误 /
// 取消 / 终态再取消 / 结算不足 → FAILED / 过期 / ListMyOrders(activeOnly)。
package biz

import (
	"context"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/errcode"
	tradev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/trade/v1"
	"github.com/luyuancpp/pandora/services/economy/trade/internal/conf"
)

// ── fakes ─────────────────────────────────────────────────────────────────────

type fakeRepo struct {
	orders  map[uint64]*tradev1.Order
	players map[uint64]map[uint64]bool // player → set(order_id)
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{orders: map[uint64]*tradev1.Order{}, players: map[uint64]map[uint64]bool{}}
}

func (f *fakeRepo) addIndex(playerID, orderID uint64) {
	if f.players[playerID] == nil {
		f.players[playerID] = map[uint64]bool{}
	}
	f.players[playerID][orderID] = true
}

func (f *fakeRepo) CreateOrder(_ context.Context, order *tradev1.Order, _ time.Duration) error {
	f.orders[order.GetOrderId()] = order
	return nil
}

func (f *fakeRepo) DeleteOrder(_ context.Context, orderID uint64) error {
	delete(f.orders, orderID)
	return nil
}

func (f *fakeRepo) ReserveOrderSlot(_ context.Context, playerID, orderID uint64, maxOrders int, _ time.Duration) (bool, error) {
	if f.players[playerID][orderID] {
		return true, nil // 幂等
	}
	if len(f.players[playerID]) >= maxOrders {
		return false, nil
	}
	f.addIndex(playerID, orderID)
	return true, nil
}

func (f *fakeRepo) ReleaseOrderSlot(_ context.Context, playerID, orderID uint64) error {
	delete(f.players[playerID], orderID)
	return nil
}

func (f *fakeRepo) GetOrder(_ context.Context, orderID uint64) (*tradev1.Order, bool, error) {
	o, ok := f.orders[orderID]
	return o, ok, nil
}

func (f *fakeRepo) UpdateWithLock(_ context.Context, orderID uint64, _ int, fn func(*tradev1.Order) error, _ time.Duration) error {
	o, ok := f.orders[orderID]
	if !ok {
		return errcode.New(errcode.ErrTradeOrderNotFound, "order %d not found", orderID)
	}
	if err := fn(o); err != nil {
		return err
	}
	return nil
}

func (f *fakeRepo) ListPlayerOrderIDs(_ context.Context, playerID uint64) ([]uint64, error) {
	var ids []uint64
	for id := range f.players[playerID] {
		ids = append(ids, id)
	}
	return ids, nil
}

type fakeLedger struct {
	fail bool
}

func (f *fakeLedger) Settle(_ context.Context, _ *tradev1.Order, _ uint64) error {
	if f.fail {
		return errcode.New(errcode.ErrTradeInsufficient, "insufficient")
	}
	return nil
}

type fakeAudit struct{ count int }

func (f *fakeAudit) PushAudit(_ context.Context, _ *tradev1.Order) error {
	f.count++
	return nil
}

type seqSF struct{ n uint64 }

func (s *seqSF) Generate() uint64 { s.n++; return s.n }

// ── helpers ───────────────────────────────────────────────────────────────────

func newUC(repo *fakeRepo, ledger ResourceLedger) (*TradeUsecase, *fakeAudit) {
	audit := &fakeAudit{}
	cfg := conf.TradeConf{
		OrderTTL:         config.Duration(10 * time.Minute),
		OrderExpire:      config.Duration(5 * time.Minute),
		OptimisticRetry:  3,
		MaxItemsPerOrder: 20,
		// MaxTradePrice 必须显式给:price 改 uint64 后 CreateOrder 的价格闸是**上界** ——
		// 零值 0 会把"任何正价"都判成越界(而不是"不限价")。生产由 Config.Defaults()
		// 填 1e9,但那是 *Config 上的方法,直建 TradeConf 的测试拿不到。
		MaxTradePrice: 1_000_000_000,
	}
	return NewTradeUsecase(repo, ledger, audit, &seqSF{}, cfg), audit
}

func items() []*tradev1.TradeItem {
	return []*tradev1.TradeItem{{ItemConfigId: 1001, Count: 1}}
}

func wantCode(t *testing.T, err error, code errcode.Code) {
	t.Helper()
	if errcode.As(err) != code {
		t.Fatalf("want code %d, got err=%v (code=%d)", code, err, errcode.As(err))
	}
}

// ── 测试 ───────────────────────────────────────────────────────────────────────

// TestCreateOrder_PriceUpperBound 价格闸从"下界防负数"改成"上界防天文数字"后的回归。
//
// 为什么必须有这条:price 是 uint64,旧写法 `price < 0` 恒为 false,编译器与 go vet 都不报,
// 闸门被静默拆掉也没有任何测试会红。这里钉死三件事:
//
//	① 恰好等于上限 → 放行(闸是 `>` 不是 `>=`,别把合法边界值也拒了);
//	② 超过上限 → ErrInvalidArg,且**零副作用**(不落订单);
//	③ 一个 price=-1 的老客户端请求在 uint64 里解成 MaxUint64 → 必须被同一道闸挡下,
//	   而不是一路落库再送进 SettlePlayerTrade。
func TestCreateOrder_PriceUpperBound(t *testing.T) {
	const maxPrice = uint64(1_000_000_000)

	t.Run("恰好等于上限放行", func(t *testing.T) {
		repo := newFakeRepo()
		uc, _ := newUC(repo, &fakeLedger{})
		if _, err := uc.CreateOrder(context.Background(), 1, 2, items(), nil, maxPrice); err != nil {
			t.Fatalf("边界值应放行: %v", err)
		}
	})

	t.Run("超上限拒绝且零副作用", func(t *testing.T) {
		repo := newFakeRepo()
		uc, _ := newUC(repo, &fakeLedger{})
		_, err := uc.CreateOrder(context.Background(), 1, 2, items(), nil, maxPrice+1)
		wantCode(t, err, errcode.ErrInvalidArg)
		if len(repo.orders) != 0 {
			t.Fatalf("越界不得落订单: %d", len(repo.orders))
		}
	})

	t.Run("负价在uint64里解成天文数字也必须拒", func(t *testing.T) {
		repo := newFakeRepo()
		uc, _ := newUC(repo, &fakeLedger{})
		// int64(-1) 按 uint64 解读 = 18446744073709551615。
		_, err := uc.CreateOrder(context.Background(), 1, 2, items(), nil, ^uint64(0))
		wantCode(t, err, errcode.ErrInvalidArg)
		if len(repo.orders) != 0 {
			t.Fatalf("越界不得落订单: %d", len(repo.orders))
		}
	})

	t.Run("price为0是合法的纯以物易物", func(t *testing.T) {
		repo := newFakeRepo()
		uc, _ := newUC(repo, &fakeLedger{})
		// 上界闸不能顺手把 0 也拒掉:0 = 双方只换道具不付钱,是既有合法形态。
		if _, err := uc.CreateOrder(context.Background(), 1, 2, items(), items(), 0); err != nil {
			t.Fatalf("零价以物易物应放行: %v", err)
		}
	})
}

func TestCreateOrder_OK(t *testing.T) {
	repo := newFakeRepo()
	uc, _ := newUC(repo, &fakeLedger{})
	id, err := uc.CreateOrder(context.Background(), 1, 2, items(), nil, 100)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	o := repo.orders[id]
	if o.GetState() != tradev1.OrderState_ORDER_STATE_PENDING {
		t.Fatalf("want PENDING, got %s", o.GetState())
	}
	if o.GetSellerId() != 1 || o.GetBuyerId() != 2 {
		t.Fatalf("wrong parties: %+v", o)
	}
}

func TestCreateOrder_Self(t *testing.T) {
	uc, _ := newUC(newFakeRepo(), &fakeLedger{})
	_, err := uc.CreateOrder(context.Background(), 1, 1, items(), nil, 100)
	wantCode(t, err, errcode.ErrInvalidArg)
}

func TestCreateOrder_NoItems(t *testing.T) {
	uc, _ := newUC(newFakeRepo(), &fakeLedger{})
	_, err := uc.CreateOrder(context.Background(), 1, 2, nil, nil, 100)
	wantCode(t, err, errcode.ErrInvalidArg)
}

func TestTwoPhaseConfirm_OK(t *testing.T) {
	repo := newFakeRepo()
	uc, _ := newUC(repo, &fakeLedger{})
	id, _ := uc.CreateOrder(context.Background(), 1, 2, items(), nil, 100)

	// 买方先确认 → BUYER_CONFIRMED
	st, err := uc.ConfirmOrder(context.Background(), 2, id)
	if err != nil {
		t.Fatalf("buyer confirm err: %v", err)
	}
	if st != tradev1.OrderState_ORDER_STATE_BUYER_CONFIRMED {
		t.Fatalf("want BUYER_CONFIRMED, got %s", st)
	}

	// 卖方确认 → 结算 → COMPLETED
	st, err = uc.ConfirmOrder(context.Background(), 1, id)
	if err != nil {
		t.Fatalf("seller confirm err: %v", err)
	}
	if st != tradev1.OrderState_ORDER_STATE_COMPLETED {
		t.Fatalf("want COMPLETED, got %s", st)
	}
}

func TestConfirm_SellerBeforeBuyer(t *testing.T) {
	repo := newFakeRepo()
	uc, _ := newUC(repo, &fakeLedger{})
	id, _ := uc.CreateOrder(context.Background(), 1, 2, items(), nil, 100)

	// 卖方在买方之前确认 → 顺序错误
	_, err := uc.ConfirmOrder(context.Background(), 1, id)
	wantCode(t, err, errcode.ErrTradeWrongState)
}

func TestConfirm_Outsider(t *testing.T) {
	repo := newFakeRepo()
	uc, _ := newUC(repo, &fakeLedger{})
	id, _ := uc.CreateOrder(context.Background(), 1, 2, items(), nil, 100)

	_, err := uc.ConfirmOrder(context.Background(), 99, id)
	wantCode(t, err, errcode.ErrUnauthorized)
}

func TestConfirm_SettleInsufficient(t *testing.T) {
	repo := newFakeRepo()
	uc, _ := newUC(repo, &fakeLedger{fail: true})
	id, _ := uc.CreateOrder(context.Background(), 1, 2, items(), nil, 100)
	_, _ = uc.ConfirmOrder(context.Background(), 2, id) // buyer

	st, err := uc.ConfirmOrder(context.Background(), 1, id) // seller → settle fails
	wantCode(t, err, errcode.ErrTradeInsufficient)
	if st != tradev1.OrderState_ORDER_STATE_FAILED {
		t.Fatalf("want FAILED, got %s", st)
	}
	if repo.orders[id].GetState() != tradev1.OrderState_ORDER_STATE_FAILED {
		t.Fatalf("order not persisted as FAILED: %s", repo.orders[id].GetState())
	}
}

func TestCancel_OK(t *testing.T) {
	repo := newFakeRepo()
	uc, _ := newUC(repo, &fakeLedger{})
	id, _ := uc.CreateOrder(context.Background(), 1, 2, items(), nil, 100)

	if err := uc.CancelOrder(context.Background(), 2, id); err != nil {
		t.Fatalf("cancel err: %v", err)
	}
	if repo.orders[id].GetState() != tradev1.OrderState_ORDER_STATE_CANCELED {
		t.Fatalf("want CANCELED, got %s", repo.orders[id].GetState())
	}
}

func TestCancel_Terminal(t *testing.T) {
	repo := newFakeRepo()
	uc, _ := newUC(repo, &fakeLedger{})
	id, _ := uc.CreateOrder(context.Background(), 1, 2, items(), nil, 100)
	_, _ = uc.ConfirmOrder(context.Background(), 2, id)
	_, _ = uc.ConfirmOrder(context.Background(), 1, id) // COMPLETED

	err := uc.CancelOrder(context.Background(), 1, id)
	wantCode(t, err, errcode.ErrTradeWrongState)
}

func TestConfirm_Expired(t *testing.T) {
	repo := newFakeRepo()
	uc, _ := newUC(repo, &fakeLedger{})
	id, _ := uc.CreateOrder(context.Background(), 1, 2, items(), nil, 100)
	// 手动把订单改成已过期。
	repo.orders[id].ExpiresAtMs = time.Now().Add(-time.Minute).UnixMilli()

	state, err := uc.ConfirmOrder(context.Background(), 2, id)
	wantCode(t, err, errcode.ErrTradeOrderExpired)
	// 返回状态应为 EXPIRED(惰性过期)。
	if state != tradev1.OrderState_ORDER_STATE_EXPIRED {
		t.Fatalf("want returned state EXPIRED, got %s", state)
	}
	// 关键:访问过期订单时必须把 EXPIRED 持久化回 repo(此前只在内存置位不写回,
	// 订单状态停留在 PENDING,过期态永远落不了库)。
	if got := repo.orders[id].GetState(); got != tradev1.OrderState_ORDER_STATE_EXPIRED {
		t.Fatalf("want repo order state EXPIRED after Confirm, got %s", got)
	}
}

func TestListMyOrders_ActiveOnly(t *testing.T) {
	repo := newFakeRepo()
	uc, _ := newUC(repo, &fakeLedger{})
	id1, _ := uc.CreateOrder(context.Background(), 1, 2, items(), nil, 100)
	id2, _ := uc.CreateOrder(context.Background(), 1, 3, items(), nil, 200)
	_ = uc.CancelOrder(context.Background(), 1, id2) // id2 终态

	all, _, err := uc.ListMyOrders(context.Background(), 1, false, 0, 0)
	if err != nil {
		t.Fatalf("list err: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("want 2 all, got %d", len(all))
	}

	active, _, _ := uc.ListMyOrders(context.Background(), 1, true, 0, 0)
	if len(active) != 1 || active[0].GetOrderId() != id1 {
		t.Fatalf("want 1 active (id %d), got %+v", id1, active)
	}
}

func TestConfirm_NotFound(t *testing.T) {
	uc, _ := newUC(newFakeRepo(), &fakeLedger{})
	_, err := uc.ConfirmOrder(context.Background(), 1, 999)
	wantCode(t, err, errcode.ErrTradeOrderNotFound)
}

// ── 订单配额上限(不变量 §18)────────────────────────────────────────────────

// newCappedUC 构造小配额 UC,便于测上限。
func newCappedUC(repo *fakeRepo, maxOrders int) *TradeUsecase {
	cfg := conf.TradeConf{
		OrderTTL:           config.Duration(10 * time.Minute),
		OrderExpire:        config.Duration(5 * time.Minute),
		OptimisticRetry:    3,
		MaxItemsPerOrder:   20,
		MaxOrdersPerPlayer: maxOrders,
		MaxTradePrice:      1_000_000_000, // 同 newUC:零值等于禁掉一切正价交易。
	}
	uc := NewTradeUsecase(repo, &fakeLedger{}, &fakeAudit{}, &seqSF{}, cfg)
	return uc
}

// 受害者(买方)配额被恶意刷满 → 后续对其挂单被拒 7006;发起方自身名额同步回滚,主体不残留。
func TestCreateOrder_BuyerQuotaExceeded(t *testing.T) {
	repo := newFakeRepo()
	uc := newCappedUC(repo, 2)
	ctx := context.Background()

	// 攻击者 10、11 各对受害者 2 挂一单,占满受害者配额(max=2)。
	if _, err := uc.CreateOrder(ctx, 10, 2, items(), nil, 1); err != nil {
		t.Fatalf("order 1: %v", err)
	}
	if _, err := uc.CreateOrder(ctx, 11, 2, items(), nil, 1); err != nil {
		t.Fatalf("order 2: %v", err)
	}

	// 第三单必须被拒(受害者活跃订单全在,清理不出名额)。
	_, err := uc.CreateOrder(ctx, 12, 2, items(), nil, 1)
	wantCode(t, err, errcode.ErrTradeOrderLimit)
	// 回滚断言:发起方 12 的名额已释放、失败订单主体已删。
	if len(repo.players[12]) != 0 {
		t.Fatalf("seller 12 slot must be rolled back, got %v", repo.players[12])
	}
	if len(repo.orders) != 2 {
		t.Fatalf("aborted order body must be deleted, got %d orders", len(repo.orders))
	}
}

// 配额满但成员是终态/已回收订单 → 惰性清理后放行(受害者取消恶意单即可自愈)。
func TestCreateOrder_QuotaPrunesDeadSlots(t *testing.T) {
	repo := newFakeRepo()
	uc := newCappedUC(repo, 2)
	ctx := context.Background()

	id1, _ := uc.CreateOrder(ctx, 10, 2, items(), nil, 1)
	id2, _ := uc.CreateOrder(ctx, 11, 2, items(), nil, 1)
	_ = uc.CancelOrder(ctx, 2, id1)  // 终态(受害者取消恶意单)
	delete(repo.orders, id2)         // 主体被 Redis TTL 回收的残留成员

	// 配额名义上已满(2 个成员),但全是死成员 → 清理后成功。
	id3, err := uc.CreateOrder(ctx, 12, 2, items(), nil, 1)
	if err != nil {
		t.Fatalf("expected prune to free quota: %v", err)
	}
	if !repo.players[2][id3] {
		t.Fatal("new order must occupy buyer slot after prune")
	}
	if repo.players[2][id2] {
		t.Fatal("reclaimed order slot must be pruned")
	}
}

// ── INC-20260722-001 回归:结算意图先落库 + Cancel 结算围栏 + 幂等恢复 ─────────────

// scriptedLedger 可编排结算账本:errs 依次弹出(耗尽后成功);applyBeforeErr 模拟
// "已入账但回包丢失/超时";applied 复刻 order_id 幂等键(重复 Settle 不二次入账)。
type scriptedLedger struct {
	errs          []error
	applyBeforeErr bool   // true:弹出 err 前仍完成入账(超时窗口:资产已动)
	applied       bool
	settles       int
	applies       int
	onSettle      func() // Settle 执行中回调(模拟结算窗口内的并发事件)
}

func (s *scriptedLedger) Settle(_ context.Context, _ *tradev1.Order, _ uint64) error {
	s.settles++
	if s.onSettle != nil {
		s.onSettle()
	}
	var err error
	if len(s.errs) > 0 {
		err = s.errs[0]
		s.errs = s.errs[1:]
	}
	apply := err == nil || (s.applyBeforeErr && errcode.As(err) != errcode.ErrTradeInsufficient)
	if apply && !s.applied {
		s.applied = true
		s.applies++
	}
	return err
}

// confirmedOrder 建一笔已买方确认(BUYER_CONFIRMED)的订单,返回 orderID。
func confirmedOrder(t *testing.T, uc *TradeUsecase) uint64 {
	t.Helper()
	ctx := context.Background()
	orderID, err := uc.CreateOrder(ctx, 1, 2, items(), nil, 100)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := uc.ConfirmOrder(ctx, 2, orderID); err != nil {
		t.Fatalf("buyer confirm: %v", err)
	}
	return orderID
}

// 事故场景本体:结算执行中并发 Cancel。旧实现 Settle 在 WATCH 回调内先执行,Cancel
// 可把权威终态提交成 CANCELED 而资产已转移(撕裂)。新实现结算意图 SELLER_CONFIRMED
// 先落库,窗口内 Cancel 必须被 fail-closed 拒绝,终态只能是 COMPLETED。
func TestConfirmOrder_ConcurrentCancelDuringSettlementCannotTear(t *testing.T) {
	repo := newFakeRepo()
	ledger := &scriptedLedger{}
	uc, _ := newUC(repo, ledger)
	ctx := context.Background()
	orderID := confirmedOrder(t, uc)

	var cancelErr error
	ledger.onSettle = func() {
		cancelErr = uc.CancelOrder(ctx, 2, orderID) // 买方在结算窗口内并发取消
	}
	st, err := uc.ConfirmOrder(ctx, 1, orderID)
	if err != nil || st != tradev1.OrderState_ORDER_STATE_COMPLETED {
		t.Fatalf("seller confirm: st=%s err=%v", st, err)
	}
	wantCode(t, cancelErr, errcode.ErrTradeWrongState)
	if ledger.applies != 1 {
		t.Fatalf("settle applies=%d want=1", ledger.applies)
	}
	o, _, _ := repo.GetOrder(ctx, orderID)
	if o.GetState() != tradev1.OrderState_ORDER_STATE_COMPLETED {
		t.Fatalf("终态=%s,资产已结算的订单绝不能是 CANCELED(撕裂)", o.GetState())
	}
}

// 瞬时结算失败:意图态留库(不置 FAILED、不回滚),Cancel 拒,重试幂等收敛 COMPLETED。
func TestConfirmOrder_TransientSettleFailureConvergesOnRetry(t *testing.T) {
	repo := newFakeRepo()
	ledger := &scriptedLedger{errs: []error{errcode.New(errcode.ErrInternal, "inventory timeout")}}
	uc, _ := newUC(repo, ledger)
	ctx := context.Background()
	orderID := confirmedOrder(t, uc)

	st, err := uc.ConfirmOrder(ctx, 1, orderID)
	wantCode(t, err, errcode.ErrUnavailable)
	if st != tradev1.OrderState_ORDER_STATE_SELLER_CONFIRMED {
		t.Fatalf("瞬时失败后应停留 SELLER_CONFIRMED,得 %s", st)
	}
	// 结算围栏:意图落库后 Cancel / 过期清理一律拒。
	wantCode(t, uc.CancelOrder(ctx, 2, orderID), errcode.ErrTradeWrongState)

	// 买方重试也能驱动恢复(不只卖方)。
	st, err = uc.ConfirmOrder(ctx, 2, orderID)
	if err != nil || st != tradev1.OrderState_ORDER_STATE_COMPLETED {
		t.Fatalf("retry confirm: st=%s err=%v", st, err)
	}
	if ledger.settles != 2 || ledger.applies != 1 {
		t.Fatalf("settles=%d applies=%d want 2/1", ledger.settles, ledger.applies)
	}
}

// 超时但已入账(回包丢失):重试 Settle 幂等命中,不双发,收敛 COMPLETED。
func TestConfirmOrder_AppliedButLostResponseConvergesOnce(t *testing.T) {
	repo := newFakeRepo()
	ledger := &scriptedLedger{
		errs:           []error{errcode.New(errcode.ErrInternal, "response lost")},
		applyBeforeErr: true, // 第一次调用已入账,但响应丢了
	}
	uc, _ := newUC(repo, ledger)
	ctx := context.Background()
	orderID := confirmedOrder(t, uc)

	if _, err := uc.ConfirmOrder(ctx, 1, orderID); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("lost response want ErrUnavailable, got %v", err)
	}
	st, err := uc.ConfirmOrder(ctx, 1, orderID)
	if err != nil || st != tradev1.OrderState_ORDER_STATE_COMPLETED {
		t.Fatalf("retry: st=%s err=%v", st, err)
	}
	if ledger.applies != 1 {
		t.Fatalf("资产必须恰好入账一次,applies=%d", ledger.applies)
	}
}

// SELLER_CONFIRMED 不过期:结算意图落库后哪怕 expires_at_ms 已过,访问路径也不得置
// EXPIRED(资产可能已转移,置 EXPIRED 即撕裂),重试确认继续收敛 COMPLETED。
func TestConfirmOrder_SellerConfirmedNeverExpires(t *testing.T) {
	repo := newFakeRepo()
	ledger := &scriptedLedger{errs: []error{errcode.New(errcode.ErrInternal, "boom")}}
	uc, _ := newUC(repo, ledger)
	ctx := context.Background()
	orderID := confirmedOrder(t, uc)

	if _, err := uc.ConfirmOrder(ctx, 1, orderID); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("want transient, got %v", err)
	}
	// 把订单时间推成已过期。
	o, _, _ := repo.GetOrder(ctx, orderID)
	o.ExpiresAtMs = nowMs() - 1

	// 列表路径不得把它惰性置 EXPIRED。
	if _, _, err := uc.ListMyOrders(ctx, 1, false, 0, 10); err != nil {
		t.Fatalf("list: %v", err)
	}
	if o.GetState() != tradev1.OrderState_ORDER_STATE_SELLER_CONFIRMED {
		t.Fatalf("列表惰性过期不得动结算中订单,得 %s", o.GetState())
	}
	// 确认路径继续驱动结算而不是判过期。
	st, err := uc.ConfirmOrder(ctx, 1, orderID)
	if err != nil || st != tradev1.OrderState_ORDER_STATE_COMPLETED {
		t.Fatalf("drive after expiry: st=%s err=%v", st, err)
	}
}

// 结算余额/物品不足:资产未动(inventory 原子拒),SELLER_CONFIRMED → FAILED 终态。
func TestConfirmOrder_InsufficientFromIntentMarksFailed(t *testing.T) {
	repo := newFakeRepo()
	ledger := &scriptedLedger{errs: []error{errcode.New(errcode.ErrTradeInsufficient, "no gold")}}
	uc, _ := newUC(repo, ledger)
	ctx := context.Background()
	orderID := confirmedOrder(t, uc)

	st, err := uc.ConfirmOrder(ctx, 1, orderID)
	wantCode(t, err, errcode.ErrTradeInsufficient)
	if st != tradev1.OrderState_ORDER_STATE_FAILED {
		t.Fatalf("want FAILED, got %s", st)
	}
	if ledger.applies != 0 {
		t.Fatalf("insufficient 不得入账,applies=%d", ledger.applies)
	}
	o, _, _ := repo.GetOrder(ctx, orderID)
	if o.GetState() != tradev1.OrderState_ORDER_STATE_FAILED {
		t.Fatalf("落库终态=%s want FAILED", o.GetState())
	}
}
