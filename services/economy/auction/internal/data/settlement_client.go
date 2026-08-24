// settlement_client.go 实现 biz.SettlementLedger:把一笔撮合成交经 gRPC 交给 inventory
// 服务做「卖↔买双方资产原子对转 + match_id 幂等」(不变量 §9.2 / §9.7)。
//
// 接线(对齐 chat/team_reader、friend/locator_client 直连模式):
//   - main.go 用 pkg/grpcclient.MustDialInsecure 拨号(内网 insecure;无 JWT → inventory 侧 callerID==0,
//     SettleAuctionMatch 是系统接口只认内网直连);inventory_addr 未配且 allow_noop_settlement=true 时 main 才退回 NoopSettlementLedger,否则 fail-fast。
//   - 成交价(被动挂单价)= MatchRecord.Price 作为单价传给 inventory,总价由 inventory 端溢出安全乘。
package data

import (
	"context"

	"google.golang.org/grpc"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	inventoryv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/inventory/v1"
)

// GrpcInventoryLedger 用 inventory 服务 gRPC client 实现 biz.SettlementLedger。
type GrpcInventoryLedger struct {
	conn *grpc.ClientConn
	cli  inventoryv1.InventoryServiceClient
}

// NewGrpcInventoryLedger 直连 inventory 服务 endpoint(host:port,内网 insecure)。
func NewGrpcInventoryLedger(inventoryAddr string) *GrpcInventoryLedger {
	conn := grpcclient.MustDialInsecure(inventoryAddr)
	return &GrpcInventoryLedger{conn: conn, cli: inventoryv1.NewInventoryServiceClient(conn)}
}

// Close 关闭底层连接。
func (g *GrpcInventoryLedger) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// Freeze 调 inventory.FreezeForOrder 在挂单 / 出价时把资产冻进 escrow(幂等键 = order_id)。
//
//   - side=SideSell → 冻 quantity 个道具;side=SideBuy → 冻 quantity*price 金币。
//   - 返回 OK                        → nil(冻结成功 / 幂等回放)
//   - 返回 ERR_INVENTORY_INSUFFICIENT → ErrAuctionInsufficient(挂单方资产不足,挂单即失败)
//   - 其它非 OK code                 → 原样透传
func (g *GrpcInventoryLedger) Freeze(ctx context.Context, playerID, orderID uint64, side Side, itemConfigID uint32, quantity, price int64) error {
	resp, err := g.cli.FreezeForOrder(ctx, &inventoryv1.FreezeForOrderRequest{
		PlayerId:     playerID,
		OrderId:      orderID,
		Side:         inventoryv1.EscrowSide(side),
		ItemConfigId: itemConfigID,
		Quantity:     quantity,
		UnitPrice:    uint64(price),
		// 拍卖行当前只用金币计价。显式传而不是留 UNSPECIFIED:
		// inventory 侧对未知币种一律 fail-closed,**不会**回退成金币(currency.proto)。
		CurrencyKind: commonv1.CurrencyKind_CURRENCY_KIND_GOLD,
	})
	if err != nil {
		return err
	}
	switch resp.GetCode() {
	case commonv1.ErrCode_OK:
		return nil
	case commonv1.ErrCode_ERR_INVENTORY_INSUFFICIENT:
		return errcode.New(errcode.ErrAuctionInsufficient,
			"auction freeze insufficient player=%d order=%d", playerID, orderID)
	default:
		return errcode.New(errcode.Code(resp.GetCode()),
			"auction freeze failed player=%d order=%d code=%d", playerID, orderID, int32(resp.GetCode()))
	}
}

// Ensure 调 inventory.EnsureAuctionEscrow 验证或补冻 legacy 活跃订单的剩余托管。
func (g *GrpcInventoryLedger) Ensure(
	ctx context.Context, playerID, orderID uint64, side Side, itemConfigID uint32, remaining, price int64,
) error {
	if remaining <= 0 || price <= 0 {
		return errcode.New(errcode.ErrInvalidArg, "invalid ensure escrow remaining=%d price=%d", remaining, price)
	}
	resp, err := g.cli.EnsureAuctionEscrow(ctx, &inventoryv1.EnsureAuctionEscrowRequest{
		PlayerId: playerID, OrderId: orderID, Side: inventoryv1.EscrowSide(side),
		ItemConfigId: itemConfigID, RemainingQuantity: uint64(remaining), UnitPrice: uint64(price),
		// 与本文件其余三个构造点同口径:inventory 对未知币种一律 fail-closed,**不会**回退成金币。
		// 漏填等于给 BUY 侧补冻钉死一个 ERR_INVALID_ARG —— 而这条路径只在
		// "旧订单缺 escrow 需要补冻" 时才走,平时不触发,所以漏了也不会立刻暴露。
		CurrencyKind: commonv1.CurrencyKind_CURRENCY_KIND_GOLD,
	})
	if err != nil {
		return err
	}
	switch resp.GetCode() {
	case commonv1.ErrCode_OK:
		return nil
	case commonv1.ErrCode_ERR_INVENTORY_INSUFFICIENT:
		return errcode.New(errcode.ErrAuctionInsufficient,
			"auction ensure escrow insufficient player=%d order=%d", playerID, orderID)
	default:
		return errcode.New(errcode.Code(resp.GetCode()),
			"auction ensure escrow failed player=%d order=%d code=%d", playerID, orderID, int32(resp.GetCode()))
	}
}

// Settle 调 inventory.SettleAuctionMatch 完成本笔成交的资产对转(幂等键 = match_id)。
//
//   - inventory 返回 OK              → nil(结算成功 / 幂等回放)
//   - 返回 ERR_INVENTORY_INSUFFICIENT → ErrAuctionInsufficient(买家金币 / 卖家道具不足)
//   - 其它非 OK code                 → 原样透传该错误码(便于上游定位)
//   - gRPC 传输错误                  → 原样返回(撮合中止,剩余不挂簿)
func (g *GrpcInventoryLedger) Settle(ctx context.Context, m *MatchRecord) error {
	resp, err := g.cli.SettleAuctionMatch(ctx, &inventoryv1.SettleAuctionMatchRequest{
		MatchId:      m.MatchID,
		SellerId:     m.SellerID,
		BuyerId:      m.BuyerID,
		SellOrderId:  m.SellOrderID,
		BuyOrderId:   m.BuyOrderID,
		ItemConfigId: m.ItemConfigID,
		Quantity:     m.Quantity,
		UnitPrice:    uint64(m.Price),
		CurrencyKind: commonv1.CurrencyKind_CURRENCY_KIND_GOLD,
	})
	if err != nil {
		return err
	}
	switch resp.GetCode() {
	case commonv1.ErrCode_OK:
		return nil
	case commonv1.ErrCode_ERR_INVENTORY_INSUFFICIENT:
		return errcode.New(errcode.ErrAuctionInsufficient,
			"auction settle insufficient match=%d seller=%d buyer=%d", m.MatchID, m.SellerID, m.BuyerID)
	default:
		return errcode.New(errcode.Code(resp.GetCode()),
			"auction settle failed match=%d code=%d", m.MatchID, int32(resp.GetCode()))
	}
}

// Release 调 inventory.ReleaseEscrow 退还某挂单 escrow 残余(撤单 / 过期 / 完全成交后,幂等键 = order_id)。
//
//   - 返回 OK         → nil(退还成功 / 幂等回放,已 closed 也算成功)
//   - 其它非 OK code  → 原样透传(上层记 Error 告警,资产仍冻结待补退)
func (g *GrpcInventoryLedger) Release(ctx context.Context, playerID, orderID uint64) error {
	resp, err := g.cli.ReleaseEscrow(ctx, &inventoryv1.ReleaseEscrowRequest{
		PlayerId: playerID,
		OrderId:  orderID,
	})
	if err != nil {
		return err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return errcode.New(errcode.Code(resp.GetCode()),
			"auction release failed player=%d order=%d code=%d", playerID, orderID, int32(resp.GetCode()))
	}
	return nil
}
