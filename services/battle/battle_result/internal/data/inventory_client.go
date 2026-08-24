// inventory_client.go — battle_result 调 inventory.GrantInstances 把战斗装备掉落落库(W5 ④,2026-07-08)。
//
// 接线对齐 mail/trade/auction 的 gRPC granter:内网 insecure 直连,无 JWT(系统接口)。
// inventory.GrantInstances 要求 callerID==0(系统内部直连),故必须走 MustDialInsecure。
// 幂等键 = battle_drop:{match_id}:{player_id},同对局同玩家的掉落只入账一次(资产不变量)。
package data

import (
	"context"

	"google.golang.org/grpc"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	inventoryv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/inventory/v1"
)

// GrpcInstanceGranter 用 inventory 服务 gRPC client 实现 biz.InstanceGranter。
type GrpcInstanceGranter struct {
	conn *grpc.ClientConn
	cli  inventoryv1.InventoryServiceClient
}

// StackGrant 是 inventory.GrantItems 的最小跨层投影。
type StackGrant struct {
	ItemConfigID uint32
	Count        int64
}

// NewGrpcInstanceGranter 直连 inventory 服务 endpoint(host:port,内网 insecure)。
func NewGrpcInstanceGranter(inventoryAddr string) *GrpcInstanceGranter {
	conn := grpcclient.MustDialInsecure(inventoryAddr)
	return &GrpcInstanceGranter{conn: conn, cli: inventoryv1.NewInventoryServiceClient(conn)}
}

// Close 关闭底层连接。
func (g *GrpcInstanceGranter) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// GrantInstances 调 inventory.GrantInstances 幂等发放战斗装备掉落;
// 返回非 OK 透传错误(发布器保留出箱行下轮重试),gRPC 错误原样返回。
func (g *GrpcInstanceGranter) GrantInstances(ctx context.Context, playerID uint64, itemConfigIDs []uint32, idempotencyKey string) error {
	resp, err := g.cli.GrantInstances(ctx, &inventoryv1.GrantInstancesRequest{
		PlayerId:       playerID,
		ItemConfigIds:  itemConfigIDs,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return errcode.New(errcode.Code(resp.GetCode()), "inventory grant instances code=%d", resp.GetCode())
	}
	return nil
}

// GrantItems 把可堆叠战利品按配置 ID 聚合后写入计数背包。
func (g *GrpcInstanceGranter) GrantItems(ctx context.Context, playerID uint64, items []StackGrant, goldAmount uint64, idempotencyKey string) error {
	grants := make([]*inventoryv1.ItemGrant, 0, len(items))
	for _, it := range items {
		grants = append(grants, &inventoryv1.ItemGrant{ItemConfigId: it.ItemConfigID, Count: it.Count})
	}
	var currencies []*commonv1.CurrencyAmount
	if goldAmount > 0 {
		// 战斗产出目前只有金币。显式给 kind:inventory 对 UNSPECIFIED fail-closed,不回退。
		currencies = []*commonv1.CurrencyAmount{{
			Kind:   commonv1.CurrencyKind_CURRENCY_KIND_GOLD,
			Amount: goldAmount,
		}}
	}
	resp, err := g.cli.GrantItems(ctx, &inventoryv1.GrantItemsRequest{
		PlayerId: playerID, Items: grants, Currencies: currencies, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return errcode.New(errcode.Code(resp.GetCode()), "inventory grant items code=%d", resp.GetCode())
	}
	return nil
}

// ConsumeBattleItem 持久扣减已经可信进度事实确认的局内消耗。
func (g *GrpcInstanceGranter) ConsumeBattleItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey string) error {
	resp, err := g.cli.ConsumeBattleItem(ctx, &inventoryv1.ConsumeBattleItemRequest{
		PlayerId: playerID, ItemConfigId: itemConfigID, Count: count, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return errcode.New(errcode.Code(resp.GetCode()), "inventory consume battle item code=%d", resp.GetCode())
	}
	return nil
}

// DiscardBattleItem 持久扣减可信进度事实确认的副本内堆叠物丢弃。
func (g *GrpcInstanceGranter) DiscardBattleItem(ctx context.Context, playerID uint64, itemConfigID uint32, count int64, idempotencyKey string) error {
	resp, err := g.cli.DiscardBattleItem(ctx, &inventoryv1.DiscardBattleItemRequest{
		PlayerId: playerID, ItemConfigId: itemConfigID, Count: count, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return errcode.New(errcode.Code(resp.GetCode()), "inventory discard battle item code=%d", resp.GetCode())
	}
	return nil
}
