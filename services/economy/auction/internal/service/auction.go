// Package service 是 auction 服务的 gRPC service 层(2026-06-19)。
//
// 职责:
//   - 实现 auctionv1.AuctionServiceServer
//   - 从 ctx 取 JWT player_id(R5:override request,防伪造他人身份)
//   - proto Request/Response ↔ biz 入参/出参互转
//   - errcode.Code → commonv1.ErrCode 1:1 映射
//
// 协议原则(R5):PlaceOrder 的 seller、Bid 的 buyer、Cancel/List 的 player 一律以 ctx 中的
// JWT player_id 为准,忽略请求体里的对应字段;player_id=0 → ERR_UNAUTHORIZED。
package service

import (
	"context"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	auctionv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/auction/v1"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"

	"github.com/luyuancpp/pandora/services/economy/auction/internal/biz"
	"github.com/luyuancpp/pandora/services/economy/auction/internal/data"
)

// AuctionService 实现 auctionv1.AuctionServiceServer。
type AuctionService struct {
	auctionv1.UnimplementedAuctionServiceServer
	uc *biz.AuctionUsecase
}

// NewAuctionService 构造。
func NewAuctionService(uc *biz.AuctionUsecase) *AuctionService {
	return &AuctionService{uc: uc}
}

// priceToInternal 把上行的 uint64 单价转成撮合引擎内部使用的 int64。
//
// 为什么撮合引擎的价格**刻意仍是有符号**:订单簿用 ZSET score 表达价格优先级,
// 买盘靠 `-float64(price)` 编成负分才能"升序取到最高价"(data/book.go scoreOf)。
// 这正是 CLAUDE.md §5.12 例外① 说的"语义上要参与取负的数值",改无符号会让 `-price`
// 直接回绕成天文数字,价格优先级整体错乱。
//
// 因此符号转换只发生在**这一个入口**,且必须先判上界:超出 int64 的价格在这里就拒掉,
// 绝不能让它转成负数流进撮合(负价会击穿 biz 的 `price <= 0` 闸之外的一切假设)。
// biz 侧另有 MaxPrice(默认 1e9)做业务上界,本函数只负责"不让类型转换本身出错"。
func priceToInternal(price uint64) (int64, bool) {
	const maxInt64 = uint64(^uint64(0) >> 1)
	if price > maxInt64 {
		return 0, false
	}
	return int64(price), true
}

// PlaceOrder 卖家挂单。seller 以 JWT ctx 为准(R5)。
func (s *AuctionService) PlaceOrder(ctx context.Context, req *auctionv1.PlaceOrderRequest) (*auctionv1.PlaceOrderResponse, error) {
	ownerID := callerID(ctx)
	if ownerID == 0 {
		return &auctionv1.PlaceOrderResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	price, ok := priceToInternal(req.GetPrice())
	if !ok {
		return &auctionv1.PlaceOrderResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	order, err := s.uc.PlaceOrder(ctx, ownerID, req.GetMarketId(), req.GetItemConfigId(), req.GetQuantity(), price, req.GetIdempotencyKey())
	if err != nil {
		return &auctionv1.PlaceOrderResponse{Code: toProtoCode(err)}, nil
	}
	return &auctionv1.PlaceOrderResponse{
		Code:           commonv1.ErrCode_OK,
		OrderId:        order.GetOrderId(),
		Status:         order.GetStatus(),
		FilledQuantity: order.GetFilledQuantity(),
	}, nil
}

// Bid 买家出价。buyer 以 JWT ctx 为准(R5)。
func (s *AuctionService) Bid(ctx context.Context, req *auctionv1.BidRequest) (*auctionv1.BidResponse, error) {
	ownerID := callerID(ctx)
	if ownerID == 0 {
		return &auctionv1.BidResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	price, ok := priceToInternal(req.GetPrice())
	if !ok {
		return &auctionv1.BidResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	order, err := s.uc.Bid(ctx, ownerID, req.GetMarketId(), req.GetItemConfigId(), req.GetQuantity(), price, req.GetIdempotencyKey())
	if err != nil {
		return &auctionv1.BidResponse{Code: toProtoCode(err)}, nil
	}
	return &auctionv1.BidResponse{
		Code:           commonv1.ErrCode_OK,
		OrderId:        order.GetOrderId(),
		Status:         order.GetStatus(),
		FilledQuantity: order.GetFilledQuantity(),
	}, nil
}

// CancelOrder 撤单。player 以 JWT ctx 为准(R5)。
func (s *AuctionService) CancelOrder(ctx context.Context, req *auctionv1.CancelOrderRequest) (*auctionv1.CancelOrderResponse, error) {
	ownerID := callerID(ctx)
	if ownerID == 0 {
		return &auctionv1.CancelOrderResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetMarketId() == 0 || req.GetOrderId() == 0 {
		return &auctionv1.CancelOrderResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if err := s.uc.CancelOrder(ctx, ownerID, req.GetMarketId(), req.GetOrderId()); err != nil {
		return &auctionv1.CancelOrderResponse{Code: toProtoCode(err)}, nil
	}
	return &auctionv1.CancelOrderResponse{Code: commonv1.ErrCode_OK}, nil
}

// ListMarket 看市场订单簿。
func (s *AuctionService) ListMarket(ctx context.Context, req *auctionv1.ListMarketRequest) (*auctionv1.ListMarketResponse, error) {
	if callerID(ctx) == 0 {
		return &auctionv1.ListMarketResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	orders, err := s.uc.ListMarket(ctx, req.GetMarketId(), data.Side(req.GetSide()), int(req.GetLimit()))
	if err != nil {
		return &auctionv1.ListMarketResponse{Code: toProtoCode(err)}, nil
	}
	return &auctionv1.ListMarketResponse{Code: commonv1.ErrCode_OK, Orders: orders}, nil
}

// ListMyOrders 看自己的挂单 / 出价。player 以 JWT ctx 为准(R5)。
func (s *AuctionService) ListMyOrders(ctx context.Context, req *auctionv1.ListMyOrdersRequest) (*auctionv1.ListMyOrdersResponse, error) {
	ownerID := callerID(ctx)
	if ownerID == 0 {
		return &auctionv1.ListMyOrdersResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	orders, next, hasMore, err := s.uc.ListMyOrders(ctx, ownerID, req.GetActiveOnly(),
		req.GetCursorOrderId(), int(req.GetLimit()))
	if err != nil {
		return &auctionv1.ListMyOrdersResponse{Code: toProtoCode(err)}, nil
	}
	return &auctionv1.ListMyOrdersResponse{
		Code: commonv1.ErrCode_OK, Orders: orders, NextCursorOrderId: next, HasMore: hasMore,
	}, nil
}

// ── 辅助 ──────────────────────────────────────────────────────────────────────

// callerID 从 ctx 取 JWT 注入的 player_id。
func callerID(ctx context.Context) uint64 {
	id, _ := ctx.Value(plog.CtxKeyPlayerID).(uint64)
	return id
}

// toProtoCode 把 pkg/errcode 1:1 映射成 proto enum(数值相同)。
func toProtoCode(err error) commonv1.ErrCode {
	return commonv1.ErrCode(errcode.As(err))
}
