package data

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	inventoryv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/inventory/v1"
)

// capturingInventoryClient 在**真实请求构造点之下**拦截出参。
//
// 这一层的位置是本测试的全部意义:auction 既有的 `trackLedger` 假实现挂在
// biz.SettlementLedger 接口边界上 —— 也就是 GrpcInventoryLedger **之上**,
// 于是"请求体少填了一个字段"这类 bug 它永远碰不到(2026-08-22 实际漏过一次:
// EnsureAuctionEscrow 忘填 CurrencyKind,导致 BUY 侧补冻恒返 ERR_INVALID_ARG,
// 而全部单测照样绿)。要挡住这类缺陷,假实现必须换到 gRPC client 这一层。
type capturingInventoryClient struct {
	inventoryv1.InventoryServiceClient // 未使用的方法走内嵌接口,nil 调用即 panic(测试里不该走到)

	freeze *inventoryv1.FreezeForOrderRequest
	ensure *inventoryv1.EnsureAuctionEscrowRequest
	settle *inventoryv1.SettleAuctionMatchRequest
}

func (c *capturingInventoryClient) FreezeForOrder(_ context.Context, in *inventoryv1.FreezeForOrderRequest, _ ...grpc.CallOption) (*inventoryv1.FreezeForOrderResponse, error) {
	c.freeze = in
	return &inventoryv1.FreezeForOrderResponse{Code: commonv1.ErrCode_OK}, nil
}

func (c *capturingInventoryClient) EnsureAuctionEscrow(_ context.Context, in *inventoryv1.EnsureAuctionEscrowRequest, _ ...grpc.CallOption) (*inventoryv1.EnsureAuctionEscrowResponse, error) {
	c.ensure = in
	return &inventoryv1.EnsureAuctionEscrowResponse{Code: commonv1.ErrCode_OK}, nil
}

func (c *capturingInventoryClient) SettleAuctionMatch(_ context.Context, in *inventoryv1.SettleAuctionMatchRequest, _ ...grpc.CallOption) (*inventoryv1.SettleAuctionMatchResponse, error) {
	c.settle = in
	return &inventoryv1.SettleAuctionMatchResponse{Code: commonv1.ErrCode_OK}, nil
}

// TestSettlementRequestsAlwaysCarryCurrencyKind 锁死:凡是会走到 inventory 货币侧的请求,
// 币种必须显式填,不能留 UNSPECIFIED。
//
// inventory 对未知币种一律 fail-closed(currency.proto:"绝不回退成金币"),
// 所以漏填不是"退化成默认行为",而是那条路径**永久失败**。
// 又因为 Ensure 只在"旧订单缺 escrow 需要补冻"时才走,平时根本不触发,
// 漏填可以在线上潜伏很久 —— 必须由测试而不是靠跑到来发现。
func TestSettlementRequestsAlwaysCarryCurrencyKind(t *testing.T) {
	const (
		playerID  = uint64(1001)
		orderID   = uint64(2002)
		itemCfgID = uint32(10001)
	)

	t.Run("Freeze", func(t *testing.T) {
		cap := &capturingInventoryClient{}
		g := &GrpcInventoryLedger{cli: cap}
		if err := g.Freeze(context.Background(), playerID, orderID, SideBuy, itemCfgID, 3, 100); err != nil {
			t.Fatalf("Freeze: %v", err)
		}
		assertKindSet(t, cap.freeze.GetCurrencyKind())
	})

	t.Run("Ensure", func(t *testing.T) {
		cap := &capturingInventoryClient{}
		g := &GrpcInventoryLedger{cli: cap}
		if err := g.Ensure(context.Background(), playerID, orderID, SideBuy, itemCfgID, 3, 100); err != nil {
			t.Fatalf("Ensure: %v", err)
		}
		assertKindSet(t, cap.ensure.GetCurrencyKind())
	})

	t.Run("Settle", func(t *testing.T) {
		cap := &capturingInventoryClient{}
		g := &GrpcInventoryLedger{cli: cap}
		m := &MatchRecord{
			MatchID: 3003, SellerID: playerID, BuyerID: playerID + 1,
			SellOrderID: orderID, BuyOrderID: orderID + 1,
			ItemConfigID: itemCfgID, Quantity: 3, Price: 100,
		}
		if err := g.Settle(context.Background(), m); err != nil {
			t.Fatalf("Settle: %v", err)
		}
		assertKindSet(t, cap.settle.GetCurrencyKind())
	})
}

func assertKindSet(t *testing.T, kind commonv1.CurrencyKind) {
	t.Helper()
	if kind == commonv1.CurrencyKind_CURRENCY_KIND_UNSPECIFIED {
		t.Fatalf("币种未填(UNSPECIFIED):inventory 侧会 fail-closed 拒掉整条路径,不会回退成金币")
	}
}
