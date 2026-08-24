// shop.go — NPC 商店 gRPC 处理器(2026-08-22)。
//
// 鉴权边界:两个都是**客户端 RPC**。
//   - GetShop 只读价目表,不涉及任何玩家资产,因此不校验调用者身份 ——
//     价目表本来就是要在客户端上展示的公开信息,加鉴权只会让 DS / 工具查价变麻烦。
//   - PurchaseShopItem 动玩家钱包,一律以 Envoy 注入的调用者身份为准
//     (callerPlayerID),**不信任请求体 player_id**,防止替别人花钱 / 给自己刷货。
package service

import (
	"context"

	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	inventoryv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/inventory/v1"

	"github.com/luyuancpp/pandora/services/economy/inventory/internal/biz"
)

// GetShop 读某个 NPC 商店的权威价目表。
//
// 客户端拿它渲染商品列表与单价,于是**展示口径与扣费口径同源**:
// 旧的客户端本地商店从道具表读 SellPrice 当买入价,与服务端毫无关系,
// 改表后两边会静默漂移(§17.3)。
func (s *InventoryService) GetShop(ctx context.Context, req *inventoryv1.GetShopRequest) (*inventoryv1.GetShopResponse, error) {
	entries, err := s.uc.GetShop(ctx, req.GetShopId())
	if err != nil {
		return &inventoryv1.GetShopResponse{Code: toProtoCode(err)}, nil
	}
	out := make([]*inventoryv1.ShopEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, &inventoryv1.ShopEntry{
			ItemConfigId: e.ItemConfigID,
			CountPerUnit: e.CountPerUnit,
			CurrencyKind: e.CurrencyKind,
			UnitPrice:    e.UnitPrice,
			SortOrder:    e.SortOrder,
		})
	}
	return &inventoryv1.GetShopResponse{
		Code:    commonv1.ErrCode_OK,
		ShopId:  req.GetShopId(),
		Entries: out,
	}, nil
}

// PurchaseShopItem 向 NPC 商店购买道具(服务端权威扣费 + 入包,原子且幂等)。
func (s *InventoryService) PurchaseShopItem(ctx context.Context, req *inventoryv1.PurchaseShopItemRequest) (*inventoryv1.PurchaseShopItemResponse, error) {
	playerID, code := callerPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &inventoryv1.PurchaseShopItemResponse{Code: code}, nil
	}
	outcome, err := s.uc.PurchaseShopItem(ctx, playerID,
		req.GetShopId(), req.GetItemConfigId(), req.GetUnitCount(), req.GetIdempotencyKey())
	if err != nil {
		return &inventoryv1.PurchaseShopItemResponse{Code: toProtoCode(err)}, nil
	}
	items := make([]*inventoryv1.ItemGrant, 0, len(outcome.Items))
	for _, it := range outcome.Items {
		items = append(items, &inventoryv1.ItemGrant{ItemConfigId: it.ItemConfigID, Count: it.Count})
	}
	return &inventoryv1.PurchaseShopItemResponse{
		Code:             commonv1.ErrCode_OK,
		Balance:          biz.CurrencyAmountProto(outcome.Kind, outcome.Balances.Get(outcome.Kind)),
		Cost:             biz.CurrencyAmountProto(outcome.Kind, outcome.Cost),
		GrantedItems:     items,
		GrantedInstances: toProtoInstances(outcome.Instances),
	}, nil
}
