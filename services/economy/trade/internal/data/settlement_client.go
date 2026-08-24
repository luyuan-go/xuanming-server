// settlement_client.go 实现 biz.ResourceLedger:把一笔玩家间交易经 gRPC 交给 inventory
// 服务做「卖↔买双方资产原子对转 + order_id 幂等」(不变量 §9.7)。
//
// 接线(对齐 auction/settlement_client、chat/team_reader 直连模式):
//   - main.go 用 pkg/grpcclient.MustDialInsecure 拨号(内网 insecure;无 JWT → inventory 侧 callerID==0,
//     SettlePlayerTrade 是系统接口只认内网直连);inventory_addr 未配且 allow_noop_ledger=true 时 main
//     才退回 NoopResourceLedger,否则 fail-fast。
//   - 道具一律用 item_config_id(uint32)对齐 inventory 可堆叠模型;Order.Items = 卖家交付,
//     Order.BuyerItems = 买家交付,Order.Price = 买家付卖家金币。
package data

import (
	"context"

	"google.golang.org/grpc"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	inventoryv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/inventory/v1"
	tradev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/trade/v1"
)

// GrpcResourceLedger 用 inventory 服务 gRPC client 实现 biz.ResourceLedger。
type GrpcResourceLedger struct {
	conn *grpc.ClientConn
	cli  inventoryv1.InventoryServiceClient
}

// NewGrpcResourceLedger 直连 inventory 服务 endpoint(host:port,内网 insecure)。
func NewGrpcResourceLedger(inventoryAddr string) *GrpcResourceLedger {
	conn := grpcclient.MustDialInsecure(inventoryAddr)
	return &GrpcResourceLedger{conn: conn, cli: inventoryv1.NewInventoryServiceClient(conn)}
}

// Close 关闭底层连接。
func (g *GrpcResourceLedger) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// Settle 调 inventory.SettlePlayerTrade 完成本笔交易的资产对转(幂等键 = order_id)。
//
//   - inventory 返回 OK              → nil(结算成功 / 幂等回放)
//   - 返回 ERR_INVENTORY_INSUFFICIENT → ErrTradeInsufficient(任一方道具 / 金币不足,订单置 FAILED)
//   - 其它非 OK code                 → 原样透传该错误码(便于上游定位)
//   - gRPC 传输错误                  → 原样返回(结算中止,订单不置 COMPLETED)
func (g *GrpcResourceLedger) Settle(ctx context.Context, order *tradev1.Order, idempotencyKey uint64) error {
	resp, err := g.cli.SettlePlayerTrade(ctx, &inventoryv1.SettlePlayerTradeRequest{
		OrderId:     idempotencyKey,
		SellerId:    order.GetSellerId(),
		BuyerId:     order.GetBuyerId(),
		SellerItems: toItemGrants(order.GetItems()),
		BuyerItems:  toItemGrants(order.GetBuyerItems()),
		// P2P 交易当前只用金币计价。显式给 kind:inventory 对 UNSPECIFIED 一律 fail-closed,
		// **不会**回退成金币(currency.proto)。price=0 的纯物物交换也带 kind,语义更清楚。
		PriceAmount: &commonv1.CurrencyAmount{
			Kind:   commonv1.CurrencyKind_CURRENCY_KIND_GOLD,
			Amount: order.GetPrice(),
		},
	})
	if err != nil {
		return err
	}
	switch resp.GetCode() {
	case commonv1.ErrCode_OK:
		return nil
	case commonv1.ErrCode_ERR_INVENTORY_INSUFFICIENT:
		return errcode.New(errcode.ErrTradeInsufficient,
			"trade settle insufficient order=%d seller=%d buyer=%d",
			idempotencyKey, order.GetSellerId(), order.GetBuyerId())
	default:
		// inventory 返回未预期结算码(非 OK / 非 INSUFFICIENT):可能是永久错误(如 INVALID_ARG),
		// 但上游 driveSettlement 会把它当"瞬时可重试"→ 客户端无限重试 Confirm。独立 WARN 以区分。
		plog.With(ctx).Warnw("msg", "trade_settle_unexpected_code",
			"order_id", idempotencyKey, "code", int32(resp.GetCode()),
			"seller_id", order.GetSellerId(), "buyer_id", order.GetBuyerId())
		return errcode.New(errcode.Code(resp.GetCode()),
			"trade settle failed order=%d code=%d", idempotencyKey, int32(resp.GetCode()))
	}
}

// toItemGrants 把 trade 道具(item_config_id + count)转成 inventory.ItemGrant。
func toItemGrants(items []*tradev1.TradeItem) []*inventoryv1.ItemGrant {
	if len(items) == 0 {
		return nil
	}
	out := make([]*inventoryv1.ItemGrant, 0, len(items))
	for _, it := range items {
		out = append(out, &inventoryv1.ItemGrant{
			ItemConfigId: it.GetItemConfigId(),
			Count:        int64(it.GetCount()),
		})
	}
	return out
}
