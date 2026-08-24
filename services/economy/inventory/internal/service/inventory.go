// Package service 是 inventory 服务的 gRPC service 层(W5 ③,2026-06-18)。
//
// 职责:
//   - 实现 inventoryv1.InventoryServiceServer 接口
//   - proto Request/Response ↔ biz 入参/出参互转
//   - errcode.Code → commonv1.ErrCode 1:1 映射
//
// 鉴权边界(2026-06-17 安全审查修复):
//   - 客户端 RPC(GetInventory / UseItem / SellItem):以 Envoy jwt_authn 注入的调用者身份为准
//     (pmw.PlayerIDFromContext),**不信任请求体 player_id**;请求体 player_id 与调用者不一致直接拒,
//     防止伪造 player_id 读 / 用 / 卖他人背包。
//   - 系统 RPC(GrantItems:战后掉落 / 活动 / 购买到账):只允许后端内部直连(无 JWT,callerID==0);
//     带玩家 JWT 的客户端调用一律拒绝,杜绝玩家自助发道具。并且不在 Envoy 暴露 GrantItems 路由。
package service

import (
	"context"

	"github.com/luyuancpp/pandora/pkg/errcode"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	inventoryv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/inventory/v1"

	"github.com/luyuancpp/pandora/services/economy/inventory/internal/biz"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/data"
)

// callerPlayerID 取经鉴权的调用者身份并校验请求体 player_id 一致性(客户端 RPC 用)。
//
//	未鉴权(callerID==0,直连内网无网关注入) → ERR_UNAUTHORIZED
//	请求体 player_id 与调用者不一致           → ERR_PERMISSION_DENY
//
// 返回权威 player_id(= 调用者身份),后续业务一律用它,不信任 req.PlayerId。
func callerPlayerID(ctx context.Context, reqPlayerID uint64) (uint64, commonv1.ErrCode) {
	callerID := pmw.PlayerIDFromContext(ctx)
	if callerID == 0 {
		return 0, commonv1.ErrCode_ERR_UNAUTHORIZED
	}
	if reqPlayerID != 0 && reqPlayerID != callerID {
		return 0, commonv1.ErrCode_ERR_PERMISSION_DENY
	}
	return callerID, commonv1.ErrCode_OK
}

// InventoryService 实现 inventoryv1.InventoryServiceServer。
type InventoryService struct {
	inventoryv1.UnimplementedInventoryServiceServer
	uc *biz.InventoryUsecase
}

// NewInventoryService 构造。
func NewInventoryService(uc *biz.InventoryUsecase) *InventoryService {
	return &InventoryService{uc: uc}
}

// GetInventory 读玩家背包(货币 + 道具堆叠 + 容量 + 装备实例)。以调用者身份为准。
func (s *InventoryService) GetInventory(ctx context.Context, req *inventoryv1.GetInventoryRequest) (*inventoryv1.GetInventoryResponse, error) {
	playerID, code := callerPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &inventoryv1.GetInventoryResponse{Code: code}, nil
	}
	balances, items, capacity, instances, err := s.uc.GetInventoryFull(ctx, playerID)
	if err != nil {
		return &inventoryv1.GetInventoryResponse{Code: toProtoCode(err)}, nil
	}
	out := make([]*inventoryv1.ItemStack, 0, len(items))
	for _, it := range items {
		out = append(out, &inventoryv1.ItemStack{ItemConfigId: it.ItemConfigID, Count: it.Count})
	}
	return &inventoryv1.GetInventoryResponse{
		Code: commonv1.ErrCode_OK,
		Inventory: &inventoryv1.Inventory{
			PlayerId:   playerID,
			Items:      out,
			Capacity:   capacity,
			Instances:  toProtoInstances(instances),
			Currencies: biz.BalancesToProto(balances),
		},
	}, nil
}

// GrantItems 幂等发放道具 + 货币(系统接口,仅后端内部可调)。
func (s *InventoryService) GrantItems(ctx context.Context, req *inventoryv1.GrantItemsRequest) (*inventoryv1.GrantItemsResponse, error) {
	// 系统接口:经 Envoy 的客户端调用必带 JWT(callerID>0)→ 一律拒绝,杜绝玩家自助发道具。
	// 合法调用者是后端内部服务直连(无 x-pandora-player-id 头 → callerID==0)。
	if pmw.PlayerIDFromContext(ctx) != 0 {
		return &inventoryv1.GrantItemsResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	if req.GetPlayerId() == 0 {
		return &inventoryv1.GrantItemsResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	items := make([]data.ItemGrant, 0, len(req.GetItems()))
	for _, it := range req.GetItems() {
		items = append(items, data.ItemGrant{ItemConfigID: it.GetItemConfigId(), Count: it.GetCount()})
	}
	currencies, cerr := biz.BalancesFromProto(req.GetCurrencies())
	if cerr != nil {
		return &inventoryv1.GrantItemsResponse{Code: toProtoCode(cerr)}, nil
	}
	balances, err := s.uc.GrantItems(ctx, req.GetPlayerId(), items, currencies, req.GetIdempotencyKey())
	if err != nil {
		return &inventoryv1.GrantItemsResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.GrantItemsResponse{Code: commonv1.ErrCode_OK, Currencies: biz.BalancesToProto(balances)}, nil
}

// UseItem 大厅态使用消耗品。以调用者身份为准。
func (s *InventoryService) UseItem(ctx context.Context, req *inventoryv1.UseItemRequest) (*inventoryv1.UseItemResponse, error) {
	playerID, code := callerPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &inventoryv1.UseItemResponse{Code: code}, nil
	}
	remaining, err := s.uc.UseItem(ctx, playerID, req.GetItemConfigId(), req.GetCount(), req.GetIdempotencyKey())
	if err != nil {
		return &inventoryv1.UseItemResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.UseItemResponse{Code: commonv1.ErrCode_OK, Remaining: remaining}, nil
}

// ConsumeBattleItem 按可信战斗进度事实持久扣减局内消耗品（系统接口）。
func (s *InventoryService) ConsumeBattleItem(ctx context.Context, req *inventoryv1.ConsumeBattleItemRequest) (*inventoryv1.ConsumeBattleItemResponse, error) {
	if pmw.PlayerIDFromContext(ctx) != 0 {
		return &inventoryv1.ConsumeBattleItemResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	remaining, err := s.uc.ConsumeBattleItem(ctx, req.GetPlayerId(), req.GetItemConfigId(), req.GetCount(), req.GetIdempotencyKey())
	if err != nil {
		return &inventoryv1.ConsumeBattleItemResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.ConsumeBattleItemResponse{Code: commonv1.ErrCode_OK, Remaining: remaining}, nil
}

// DiscardBattleItem 按可信战斗进度事实持久丢弃堆叠物（系统接口）。
func (s *InventoryService) DiscardBattleItem(ctx context.Context, req *inventoryv1.DiscardBattleItemRequest) (*inventoryv1.DiscardBattleItemResponse, error) {
	if pmw.PlayerIDFromContext(ctx) != 0 {
		return &inventoryv1.DiscardBattleItemResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	remaining, err := s.uc.DiscardBattleItem(ctx, req.GetPlayerId(), req.GetItemConfigId(), req.GetCount(), req.GetIdempotencyKey())
	if err != nil {
		return &inventoryv1.DiscardBattleItemResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.DiscardBattleItemResponse{Code: commonv1.ErrCode_OK, Remaining: remaining}, nil
}

// SellItem 出售道具换金币。以调用者身份为准。
func (s *InventoryService) SellItem(ctx context.Context, req *inventoryv1.SellItemRequest) (*inventoryv1.SellItemResponse, error) {
	playerID, code := callerPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &inventoryv1.SellItemResponse{Code: code}, nil
	}
	outcome, err := s.uc.SellItem(ctx, playerID, req.GetItemConfigId(), req.GetCount(), req.GetIdempotencyKey())
	if err != nil {
		return &inventoryv1.SellItemResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.SellItemResponse{
		Code:      commonv1.ErrCode_OK,
		Remaining: outcome.Remaining,
		Balance:   biz.CurrencyAmountProto(outcome.Kind, outcome.Balances.Get(outcome.Kind)),
		Earned:    biz.CurrencyAmountProto(outcome.Kind, outcome.Earned),
	}, nil
}

// DiscardItem 丢弃可堆叠物品。以调用者身份为准。
func (s *InventoryService) DiscardItem(ctx context.Context, req *inventoryv1.DiscardItemRequest) (*inventoryv1.DiscardItemResponse, error) {
	playerID, code := callerPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &inventoryv1.DiscardItemResponse{Code: code}, nil
	}
	remaining, err := s.uc.DiscardItem(ctx, playerID, req.GetItemConfigId(), req.GetCount(), req.GetIdempotencyKey())
	if err != nil {
		return &inventoryv1.DiscardItemResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.DiscardItemResponse{Code: commonv1.ErrCode_OK, Remaining: remaining}, nil
}

// SettleAuctionMatch 原子结算拍卖成交(系统接口,仅后端内部直连)。
//
// 鉴权同 GrantItems:经 Envoy 的客户端调用必带 JWT(callerID>0)→ 一律拒绝,杜绝玩家自助结算
// 套现 / 刷道具;合法调用者是 auction 服务内网直连(无 x-pandora-player-id 头 → callerID==0)。
func (s *InventoryService) SettleAuctionMatch(ctx context.Context, req *inventoryv1.SettleAuctionMatchRequest) (*inventoryv1.SettleAuctionMatchResponse, error) {
	if pmw.PlayerIDFromContext(ctx) != 0 {
		return &inventoryv1.SettleAuctionMatchResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	err := s.uc.SettleAuctionMatch(ctx,
		req.GetMatchId(), req.GetSellerId(), req.GetBuyerId(), req.GetSellOrderId(), req.GetBuyOrderId(),
		req.GetItemConfigId(), req.GetQuantity(), req.GetCurrencyKind(), req.GetUnitPrice())
	if err != nil {
		return &inventoryv1.SettleAuctionMatchResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.SettleAuctionMatchResponse{Code: commonv1.ErrCode_OK}, nil
}

// SettlePlayerTrade 原子结算玩家间点对点交易(系统接口,仅后端内部直连)。
//
// 鉴权同 SettleAuctionMatch:经 Envoy 的客户端调用(callerID>0)一律拒绝;合法调用者是
// trade 服务内网直连(无 x-pandora-player-id 头 → callerID==0)。
func (s *InventoryService) SettlePlayerTrade(ctx context.Context, req *inventoryv1.SettlePlayerTradeRequest) (*inventoryv1.SettlePlayerTradeResponse, error) {
	if pmw.PlayerIDFromContext(ctx) != 0 {
		return &inventoryv1.SettlePlayerTradeResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	toGrants := func(items []*inventoryv1.ItemGrant) []data.ItemGrant {
		out := make([]data.ItemGrant, 0, len(items))
		for _, it := range items {
			out = append(out, data.ItemGrant{ItemConfigID: it.GetItemConfigId(), Count: it.GetCount()})
		}
		return out
	}
	err := s.uc.SettlePlayerTrade(ctx,
		req.GetOrderId(), req.GetSellerId(), req.GetBuyerId(),
		toGrants(req.GetSellerItems()), toGrants(req.GetBuyerItems()),
		req.GetPriceAmount().GetKind(), req.GetPriceAmount().GetAmount())
	if err != nil {
		return &inventoryv1.SettlePlayerTradeResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.SettlePlayerTradeResponse{Code: commonv1.ErrCode_OK}, nil
}

// FreezeForOrder 挂单冻结资产(系统接口,仅后端内部直连)。鉴权同 SettleAuctionMatch:
// 经 Envoy 的客户端调用(callerID>0)一律拒绝,合法调用者是 auction 服务内网直连。
func (s *InventoryService) FreezeForOrder(ctx context.Context, req *inventoryv1.FreezeForOrderRequest) (*inventoryv1.FreezeForOrderResponse, error) {
	if pmw.PlayerIDFromContext(ctx) != 0 {
		return &inventoryv1.FreezeForOrderResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	err := s.uc.FreezeForOrder(ctx,
		req.GetPlayerId(), req.GetOrderId(), biz.EscrowSide(req.GetSide()),
		req.GetItemConfigId(), req.GetQuantity(), req.GetCurrencyKind(), req.GetUnitPrice())
	if err != nil {
		return &inventoryv1.FreezeForOrderResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.FreezeForOrderResponse{Code: commonv1.ErrCode_OK}, nil
}

// EnsureAuctionEscrow 补齐旧版本遗留订单的 escrow(系统接口,仅后端内部直连)。
// 经 Envoy 的玩家调用(callerID>0)一律拒绝；合法调用者是 auction 持久补偿器的内网直连。
func (s *InventoryService) EnsureAuctionEscrow(ctx context.Context, req *inventoryv1.EnsureAuctionEscrowRequest) (*inventoryv1.EnsureAuctionEscrowResponse, error) {
	if pmw.PlayerIDFromContext(ctx) != 0 {
		return &inventoryv1.EnsureAuctionEscrowResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	err := s.uc.EnsureAuctionEscrow(ctx,
		req.GetPlayerId(), req.GetOrderId(), biz.EscrowSide(req.GetSide()),
		req.GetItemConfigId(), req.GetRemainingQuantity(), req.GetCurrencyKind(), req.GetUnitPrice())
	if err != nil {
		return &inventoryv1.EnsureAuctionEscrowResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.EnsureAuctionEscrowResponse{Code: commonv1.ErrCode_OK}, nil
}

// ReleaseEscrow 退还挂单 escrow 残余(系统接口,仅后端内部直连)。鉴权同 SettleAuctionMatch。
func (s *InventoryService) ReleaseEscrow(ctx context.Context, req *inventoryv1.ReleaseEscrowRequest) (*inventoryv1.ReleaseEscrowResponse, error) {
	if pmw.PlayerIDFromContext(ctx) != 0 {
		return &inventoryv1.ReleaseEscrowResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	err := s.uc.ReleaseEscrow(ctx, req.GetPlayerId(), req.GetOrderId())
	if err != nil {
		return &inventoryv1.ReleaseEscrowResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.ReleaseEscrowResponse{Code: commonv1.ErrCode_OK}, nil
}

// ── 装备实例 / 鉴定(W5 ④)──

// toProtoInstance 把 data.ItemInstance 转 proto(slot=-1 保留,客户端识别未分配格)。
func toProtoInstance(in data.ItemInstance) *inventoryv1.ItemInstance {
	attrs := make([]*inventoryv1.ItemAttribute, 0, len(in.Attributes))
	for _, a := range in.Attributes {
		attrs = append(attrs, &inventoryv1.ItemAttribute{AttrId: a.AttrID, Value: a.Value})
	}
	return &inventoryv1.ItemInstance{
		InstanceId:   in.InstanceID,
		ItemConfigId: in.ItemConfigID,
		Identified:   in.Identified,
		Attributes:   attrs,
		SlotIndex:    in.SlotIndex,
		Bound:        in.Bound,
	}
}

func toProtoInstances(in []data.ItemInstance) []*inventoryv1.ItemInstance {
	out := make([]*inventoryv1.ItemInstance, 0, len(in))
	for _, inst := range in {
		out = append(out, toProtoInstance(inst))
	}
	return out
}

// GrantInstances 幂等发放装备实例(系统接口,仅后端内部直连;鉴权同 GrantItems)。
func (s *InventoryService) GrantInstances(ctx context.Context, req *inventoryv1.GrantInstancesRequest) (*inventoryv1.GrantInstancesResponse, error) {
	if pmw.PlayerIDFromContext(ctx) != 0 {
		return &inventoryv1.GrantInstancesResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	if req.GetPlayerId() == 0 {
		return &inventoryv1.GrantInstancesResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	insts, err := s.uc.GrantInstances(ctx, req.GetPlayerId(), req.GetItemConfigIds(), req.GetIdempotencyKey())
	if err != nil {
		return &inventoryv1.GrantInstancesResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.GrantInstancesResponse{Code: commonv1.ErrCode_OK, Instances: toProtoInstances(insts)}, nil
}

// IdentifyItem 鉴定一件未鉴定装备实例。以调用者身份为准(不信任请求体 player_id)。
func (s *InventoryService) IdentifyItem(ctx context.Context, req *inventoryv1.IdentifyItemRequest) (*inventoryv1.IdentifyItemResponse, error) {
	playerID, code := callerPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &inventoryv1.IdentifyItemResponse{Code: code}, nil
	}
	inst, err := s.uc.IdentifyItem(ctx, playerID, req.GetInstanceId())
	if err != nil {
		return &inventoryv1.IdentifyItemResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.IdentifyItemResponse{Code: commonv1.ErrCode_OK, Instance: toProtoInstance(inst)}, nil
}

// DiscardInstance 丢弃一件装备实例。以调用者身份为准。
func (s *InventoryService) DiscardInstance(ctx context.Context, req *inventoryv1.DiscardInstanceRequest) (*inventoryv1.DiscardInstanceResponse, error) {
	playerID, code := callerPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &inventoryv1.DiscardInstanceResponse{Code: code}, nil
	}
	if err := s.uc.DiscardInstance(ctx, playerID, req.GetInstanceId()); err != nil {
		return &inventoryv1.DiscardInstanceResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.DiscardInstanceResponse{Code: commonv1.ErrCode_OK}, nil
}

// SellInstance 出售唯一装备实例。以调用者身份为准；instance/config 同时校验。
func (s *InventoryService) SellInstance(ctx context.Context, req *inventoryv1.SellInstanceRequest) (*inventoryv1.SellInstanceResponse, error) {
	playerID, code := callerPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &inventoryv1.SellInstanceResponse{Code: code}, nil
	}
	outcome, err := s.uc.SellInstance(ctx, playerID, req.GetInstanceId(), req.GetItemConfigId(), req.GetIdempotencyKey())
	if err != nil {
		return &inventoryv1.SellInstanceResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.SellInstanceResponse{
		Code:    commonv1.ErrCode_OK,
		Balance: biz.CurrencyAmountProto(outcome.Kind, outcome.Balances.Get(outcome.Kind)),
		Earned:  biz.CurrencyAmountProto(outcome.Kind, outcome.Earned),
	}, nil
}

// MoveInstance 移动一件装备实例到新格子。以调用者身份为准。
func (s *InventoryService) MoveInstance(ctx context.Context, req *inventoryv1.MoveInstanceRequest) (*inventoryv1.MoveInstanceResponse, error) {
	playerID, code := callerPlayerID(ctx, req.GetPlayerId())
	if code != commonv1.ErrCode_OK {
		return &inventoryv1.MoveInstanceResponse{Code: code}, nil
	}
	if _, err := s.uc.MoveInstance(ctx, playerID, req.GetInstanceId(), req.GetToSlotIndex()); err != nil {
		return &inventoryv1.MoveInstanceResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.MoveInstanceResponse{Code: commonv1.ErrCode_OK}, nil
}

func toProtoCode(err error) commonv1.ErrCode {
	return commonv1.ErrCode(errcode.As(err))
}

// CheckItemsOwned 批量拥有权查询(系统接口,仅后端内部可调)。
//
// 调用方是 player 服务的 SetEquipment 拥有权校验。与 GrantItems 同一条鉴权口径:
// 经 Envoy 的客户端调用必带 JWT(callerID>0)→ 一律拒,防玩家自查/探测他人背包;
// 合法调用者是后端内部服务直连(无 x-pandora-player-id 头 → callerID==0)。
// 路由层还有一道 Envoy 精确 403 兜底,双保险。
func (s *InventoryService) CheckItemsOwned(ctx context.Context, req *inventoryv1.CheckItemsOwnedRequest) (*inventoryv1.CheckItemsOwnedResponse, error) {
	if pmw.PlayerIDFromContext(ctx) != 0 {
		return &inventoryv1.CheckItemsOwnedResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	owned, err := s.uc.CheckItemsOwned(ctx, req.GetPlayerId(), req.GetItemConfigIds())
	if err != nil {
		return &inventoryv1.CheckItemsOwnedResponse{Code: toProtoCode(err)}, nil
	}
	return &inventoryv1.CheckItemsOwnedResponse{
		Code:               commonv1.ErrCode_OK,
		OwnedItemConfigIds: owned,
	}, nil
}

func toInstanceOwnershipQueries(in []*inventoryv1.InstanceOwnershipQuery) []data.InstanceOwnershipQuery {
	out := make([]data.InstanceOwnershipQuery, 0, len(in))
	for _, q := range in {
		out = append(out, data.InstanceOwnershipQuery{
			InstanceID: q.GetInstanceId(), ItemConfigID: q.GetItemConfigId(),
		})
	}
	return out
}

// CheckInstancesOwned 精确实例拥有权查询（系统接口）。
func (s *InventoryService) CheckInstancesOwned(ctx context.Context, req *inventoryv1.CheckInstancesOwnedRequest) (*inventoryv1.CheckInstancesOwnedResponse, error) {
	if pmw.PlayerIDFromContext(ctx) != 0 {
		return &inventoryv1.CheckInstancesOwnedResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	owned, err := s.uc.CheckInstancesOwned(ctx, req.GetPlayerId(), toInstanceOwnershipQueries(req.GetInstances()))
	if err != nil {
		return &inventoryv1.CheckInstancesOwnedResponse{Code: toProtoCode(err)}, nil
	}
	ownedIDs := make([]uint64, 0, len(owned))
	for _, inst := range owned {
		ownedIDs = append(ownedIDs, inst.InstanceID)
	}
	return &inventoryv1.CheckInstancesOwnedResponse{
		Code: commonv1.ErrCode_OK, OwnedInstanceIds: ownedIDs, OwnedInstances: toProtoInstances(owned),
	}, nil
}
