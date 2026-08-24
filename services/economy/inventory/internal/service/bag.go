// bag.go — 背包域 gRPC service 层(pandora.bag.v1;phase 1 由 inventory 进程承载)。
//
// 鉴权边界(五要件①身份,bag-domain.md §2;phase 2 已接):
//   - 全部 RPC 是内部系统接口:调用方 = owner DS(经 :8444 DS 面直连,无玩家 JWT →
//     callerID==0);带玩家 JWT 的客户端调用一律拒(Envoy /pandora.bag.v1/ 前缀 403 双保险);
//   - 五要件①:DSCallbackGuard 验签 DS 凭据令牌抽取 pod/uid(SetDSGuard 注入;
//     mode=off/dev 时无身份,由授权器按契约 fail-closed 或 epoch 证明降级);
//   - 五要件②:biz 层逐写查询 owner authority,record.target 与调用方身份全等校验;
//     fencing(③)/ 额度(④)/ 审计(⑤)在 biz/data 层。
package service

import (
	"context"

	"github.com/luyuancpp/pandora/pkg/auth"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	bagv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/bag/v1"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/services/economy/inventory/internal/biz"
)

// BagService 实现 bagv1.BagServiceServer。
type BagService struct {
	bagv1.UnimplementedBagServiceServer
	uc *biz.BagUsecase

	// dsGuard DS 回调令牌守卫(五要件①;nil = ds_auth off,dev)。
	dsGuard *pmw.DSCallbackGuard
}

// NewBagService 构造。
func NewBagService(uc *biz.BagUsecase) *BagService {
	return &BagService{uc: uc}
}

// SetDSGuard 注入 DS 回调令牌守卫(main 按 ds_auth 配置装配;nil-safe)。
func (s *BagService) SetDSGuard(g *pmw.DSCallbackGuard) {
	s.dsGuard = g
}

// rejectClientCaller 系统接口守卫:带玩家 JWT 的调用(callerID>0)一律拒。
func rejectClientCaller(ctx context.Context) bool {
	return pmw.PlayerIDFromContext(ctx) != 0
}

// resolveDSCaller 验签抽取调用方 DS 身份(五要件①)。guard nil/off/permissive 放行
// 时返回零值身份(不冒充"已证明");验签失败/范围不符 → error(handler 翻译业务码)。
// scope 不限定 hub/battle(两类 owner DS 都可写背包域;②的 target 全等才是硬闸)。
func (s *BagService) resolveDSCaller(ctx context.Context) (biz.DSCallerIdentity, error) {
	if s.dsGuard == nil {
		return biz.DSCallerIdentity{}, nil
	}
	_, cred, err := s.dsGuard.CheckCredential(ctx, pmw.DSScope{RequireToken: true})
	if err != nil {
		return biz.DSCallerIdentity{}, err
	}
	if cred == nil {
		return biz.DSCallerIdentity{}, nil
	}
	if cred.DSType != auth.DSTypeHub && cred.DSType != auth.DSTypeBattle {
		return biz.DSCallerIdentity{}, nil
	}
	return biz.DSCallerIdentity{Pod: cred.Pod, UID: cred.InstanceUID}, nil
}

// LoadBag 加载随身组(owner DS checkout)。
func (s *BagService) LoadBag(ctx context.Context, req *bagv1.LoadBagRequest) (*bagv1.LoadBagResponse, error) {
	if rejectClientCaller(ctx) {
		return &bagv1.LoadBagResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	caller, gerr := s.resolveDSCaller(ctx)
	if gerr != nil {
		return &bagv1.LoadBagResponse{Code: toProtoCode(gerr)}, nil
	}
	snapshot, tail, lastSeq, err := s.uc.LoadBag(ctx, req.GetPlayerId(), req.GetOwnerEpoch(), caller)
	if err != nil {
		return &bagv1.LoadBagResponse{Code: toProtoCode(err)}, nil
	}
	resp := &bagv1.LoadBagResponse{Code: commonv1.ErrCode_OK, LastJournalSeq: lastSeq}
	// 随身段权威有效容量(§5.3):base + 已购增量;checkpoint 内 capacity 仅回显不作数。
	capacities, cerr := s.uc.CarryEffectiveCapacities(ctx, req.GetPlayerId())
	if cerr != nil {
		return &bagv1.LoadBagResponse{Code: toProtoCode(cerr)}, nil
	}
	for _, c := range capacities {
		resp.EffectiveCapacities = append(resp.EffectiveCapacities,
			&bagv1.BagEffectiveCapacity{BagType: c.BagType, Capacity: c.Capacity})
	}
	if len(snapshot) > 0 {
		record := &bagv1.BagStorageRecord{}
		if uerr := proto.Unmarshal(snapshot, record); uerr != nil {
			return &bagv1.LoadBagResponse{Code: commonv1.ErrCode_ERR_INTERNAL}, nil
		}
		resp.Snapshot = record
	}
	for _, row := range tail {
		entry := &bagv1.BagJournalEntry{}
		if uerr := proto.Unmarshal(row.Payload, entry); uerr != nil {
			return &bagv1.LoadBagResponse{Code: commonv1.ErrCode_ERR_INTERNAL}, nil
		}
		resp.Tail = append(resp.Tail, entry)
	}
	return resp, nil
}

// AppendJournal 追加流水(同步入账;前缀确认)。
func (s *BagService) AppendJournal(ctx context.Context, req *bagv1.AppendJournalRequest) (*bagv1.AppendJournalResponse, error) {
	if rejectClientCaller(ctx) {
		return &bagv1.AppendJournalResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	caller, gerr := s.resolveDSCaller(ctx)
	if gerr != nil {
		return &bagv1.AppendJournalResponse{Code: toProtoCode(gerr)}, nil
	}
	acked, err := s.uc.AppendJournal(ctx, req.GetPlayerId(), req.GetOwnerEpoch(), req.GetEntries(), caller)
	if err != nil {
		return &bagv1.AppendJournalResponse{Code: toProtoCode(err)}, nil
	}
	return &bagv1.AppendJournalResponse{Code: commonv1.ErrCode_OK, AckedSeq: acked}, nil
}

// SaveCheckpoint 保存随身组快照。
func (s *BagService) SaveCheckpoint(ctx context.Context, req *bagv1.SaveCheckpointRequest) (*bagv1.SaveCheckpointResponse, error) {
	if rejectClientCaller(ctx) {
		return &bagv1.SaveCheckpointResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	record := req.GetSnapshot()
	if record == nil {
		return &bagv1.SaveCheckpointResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	// 先用 protobuf 的无分配尺寸计算挡住超限载荷，避免为已知会被 biz 拒绝的
	// checkpoint 再分配一份 >256KiB 的序列化缓冲。biz 仍保留同一字节闸作第二道防线。
	if proto.Size(record) > biz.BagCheckpointMaxBytes {
		return &bagv1.SaveCheckpointResponse{Code: commonv1.ErrCode_ERR_BAG_QUOTA_EXCEEDED}, nil
	}
	blob, merr := proto.Marshal(record)
	if merr != nil {
		return &bagv1.SaveCheckpointResponse{Code: commonv1.ErrCode_ERR_INTERNAL}, nil
	}
	caller, gerr := s.resolveDSCaller(ctx)
	if gerr != nil {
		return &bagv1.SaveCheckpointResponse{Code: toProtoCode(gerr)}, nil
	}
	if err := s.uc.SaveCheckpoint(ctx, req.GetPlayerId(), req.GetOwnerEpoch(), record, blob, req.GetCoveredJournalSeq(), caller); err != nil {
		return &bagv1.SaveCheckpointResponse{Code: toProtoCode(err)}, nil
	}
	return &bagv1.SaveCheckpointResponse{Code: commonv1.ErrCode_OK}, nil
}

// PurchaseCapacity 购买容量扩容(§5.3;owner DS 发起,价格/档位/封顶服务端权威)。
func (s *BagService) PurchaseCapacity(ctx context.Context, req *bagv1.PurchaseCapacityRequest) (*bagv1.PurchaseCapacityResponse, error) {
	if rejectClientCaller(ctx) {
		return &bagv1.PurchaseCapacityResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	caller, gerr := s.resolveDSCaller(ctx)
	if gerr != nil {
		return &bagv1.PurchaseCapacityResponse{Code: toProtoCode(gerr)}, nil
	}
	res, err := s.uc.PurchaseCapacity(ctx, req.GetPlayerId(), req.GetOwnerEpoch(), req.GetBagType(), caller)
	if err != nil {
		return &bagv1.PurchaseCapacityResponse{Code: toProtoCode(err)}, nil
	}
	return &bagv1.PurchaseCapacityResponse{
		Code:              commonv1.ErrCode_OK,
		Purchases:         res.Purchases,
		Extra:             res.Extra,
		EffectiveCapacity: res.EffectiveCapacity,
		Cost:              biz.CurrencyAmountProto(res.CurrencyKind, res.Cost),
		Balance:           biz.CurrencyAmountProto(res.CurrencyKind, res.Balance),
	}, nil
}

// GetSections 读后端驻留段(仓库/活动段;活动段按 current generation 过滤)。
func (s *BagService) GetSections(ctx context.Context, req *bagv1.GetSectionsRequest) (*bagv1.GetSectionsResponse, error) {
	if rejectClientCaller(ctx) {
		return &bagv1.GetSectionsResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	sections, err := s.uc.GetSections(ctx, req.GetPlayerId(), req.GetBagTypes())
	if err != nil {
		return &bagv1.GetSectionsResponse{Code: toProtoCode(err)}, nil
	}
	return &bagv1.GetSectionsResponse{Code: commonv1.ErrCode_OK, Sections: sections}, nil
}
