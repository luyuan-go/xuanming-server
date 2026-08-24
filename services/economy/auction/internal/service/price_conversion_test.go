// price_conversion_test.go — 上行 uint64 单价 → 撮合内部 int64 的唯一转换点回归
// (2026-08-22 货币无符号化)。
//
// 为什么值得单独钉一条:撮合簿的价格**刻意仍是有符号**(买盘用 `-price` 编 ZSET score
// 表达"最高价优先",§5.12 例外①),所以 uint64→int64 的窄化是这条链上唯一一处
// 有符号/无符号边界。窄化如果不判上界,超过 MaxInt64 的价格会静默变成**负数**流进撮合:
// 负价既击穿价格优先级,也会让下游一切"price > 0"的假设失效,而编译器与 go vet 都不报。
package service

import (
	"context"
	"math"
	"testing"

	plog "github.com/luyuancpp/pandora/pkg/log"
	auctionv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/auction/v1"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
)

// authedCtx 造一个已通过 JWT 的调用上下文(callerID 从 ctx 读)。
func authedCtx(playerID uint64) context.Context {
	return context.WithValue(context.Background(), plog.CtxKeyPlayerID, playerID)
}

func TestPriceToInternalRejectsAboveInt64(t *testing.T) {
	cases := []struct {
		name  string
		price uint64
		want  int64
		ok    bool
	}{
		{name: "零价", price: 0, want: 0, ok: true},
		{name: "普通价", price: 12345, want: 12345, ok: true},
		{name: "恰好 MaxInt64", price: math.MaxInt64, want: math.MaxInt64, ok: true},
		{name: "MaxInt64+1 必须拒", price: uint64(math.MaxInt64) + 1, ok: false},
		// int64(-1) 按 uint64 解读的值:老客户端 / 被篡改请求的典型形态。
		{name: "MaxUint64 必须拒", price: math.MaxUint64, ok: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := priceToInternal(tc.price)
			if ok != tc.ok {
				t.Fatalf("ok=%v want=%v (price=%d)", ok, tc.ok, tc.price)
			}
			if ok && got != tc.want {
				t.Fatalf("got=%d want=%d", got, tc.want)
			}
			if !ok && got != 0 {
				t.Fatalf("拒绝时必须返回 0 而不是窄化后的负数, got=%d", got)
			}
			// 转换结果绝不能是负数:负价会让 ZSET score 的价格优先级整体错乱。
			if got < 0 {
				t.Fatalf("转换结果不得为负: %d", got)
			}
		})
	}
}

// TestPlaceOrderAndBidRejectOversizedPriceBeforeUsecase 越界价格必须在**触达 usecase 之前**
// 被挡下(svc 用 nil usecase 构造:一旦漏判就会 panic,而不是悄悄放行)。
func TestPlaceOrderAndBidRejectOversizedPriceBeforeUsecase(t *testing.T) {
	svc := NewAuctionService(nil)
	ctx := authedCtx(1001)

	resp, err := svc.PlaceOrder(ctx, &auctionv1.PlaceOrderRequest{
		MarketId: 1, ItemConfigId: 7001, Quantity: 1,
		Price: math.MaxUint64, IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("PlaceOrder transport err: %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG {
		t.Fatalf("PlaceOrder 越界价格应返回 ERR_INVALID_ARG, got %v", resp.GetCode())
	}

	bidResp, err := svc.Bid(ctx, &auctionv1.BidRequest{
		MarketId: 1, ItemConfigId: 7001, Quantity: 1,
		Price: math.MaxUint64, IdempotencyKey: "k2",
	})
	if err != nil {
		t.Fatalf("Bid transport err: %v", err)
	}
	if bidResp.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG {
		t.Fatalf("Bid 越界价格应返回 ERR_INVALID_ARG, got %v", bidResp.GetCode())
	}
}
