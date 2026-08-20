package service

import (
	"context"
	"errors"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/playerno"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"
	"github.com/luyuancpp/pandora/services/account/login/internal/biz"
)

// MaxResolvePlayerNosBatch 限制一次内部查询的原始输入数量。当前 team 默认上限为 5；保留 32
// 给可配置玩法扩容。先按原始长度限界再去重，避免重复 ID 放大解析与签名成本。
const MaxResolvePlayerNosBatch = playerno.ResolveBatchLimit

type playerNoRequestVerifier interface {
	VerifyWithPayload(ctx context.Context, fullMethod string, subject uint64, payload []byte) error
}

// LoginInternalService 实现不经客户端 Envoy 暴露的账号域内部读取接口。
type LoginInternalService struct {
	loginv1.UnimplementedLoginInternalServiceServer
	uc       *biz.PlayerNoResolveUsecase
	verifier playerNoRequestVerifier
}

func NewLoginInternalService(uc *biz.PlayerNoResolveUsecase, verifier playerNoRequestVerifier) *LoginInternalService {
	return &LoginInternalService{uc: uc, verifier: verifier}
}

// ResolvePlayerNos 只接受 player_id，并把规范化后的完整请求体绑定到 team 服务身份签名。
// 账号库查询失败会返回错误码；team 调用方按展示弱依赖把该错误降级成 player_no=0。
func (s *LoginInternalService) ResolvePlayerNos(
	ctx context.Context,
	req *loginv1.ResolvePlayerNosRequest,
) (*loginv1.ResolvePlayerNosResponse, error) {
	playerIDs, ok := normalizePlayerNoIDs(req.GetPlayerIds())
	if !ok {
		return &loginv1.ResolvePlayerNosResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	canonicalReq := &loginv1.ResolvePlayerNosRequest{PlayerIds: playerIDs}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonicalReq)
	if err != nil {
		plog.With(ctx).Errorw("msg", "resolve_player_nos_marshal_failed", "count", len(playerIDs), "err", err)
		return &loginv1.ResolvePlayerNosResponse{Code: commonv1.ErrCode_ERR_INTERNAL}, nil
	}
	if s == nil || s.verifier == nil {
		plog.With(ctx).Warnw("msg", "resolve_player_nos_auth_unavailable", "count", len(playerIDs))
		return &loginv1.ResolvePlayerNosResponse{Code: commonv1.ErrCode_ERR_UNAVAILABLE}, nil
	}
	if err := s.verifier.VerifyWithPayload(ctx,
		loginv1.LoginInternalService_ResolvePlayerNos_FullMethodName, playerIDs[0], payload); err != nil {
		code := commonv1.ErrCode_ERR_PERMISSION_DENY
		if errors.Is(err, internalrpcauth.ErrUnavailable) {
			code = commonv1.ErrCode_ERR_UNAVAILABLE
		}
		plog.With(ctx).Warnw("msg", "resolve_player_nos_auth_rejected", "count", len(playerIDs),
			"code", code, "err", err)
		return &loginv1.ResolvePlayerNosResponse{Code: code}, nil
	}
	return resolvePlayerNosFromAuthority(ctx, s.uc, playerIDs), nil
}

// resolvePlayerNosFromAuthority 是 Team internal RPC 与 DS-only RPC 共用的单批权威读取。
// 调用方必须先完成各自的身份校验与 normalizePlayerNoIDs；查询失败绝不返回部分 entries。
func resolvePlayerNosFromAuthority(
	ctx context.Context,
	uc *biz.PlayerNoResolveUsecase,
	playerIDs []uint64,
) *loginv1.ResolvePlayerNosResponse {
	if uc == nil {
		return &loginv1.ResolvePlayerNosResponse{Code: commonv1.ErrCode_ERR_UNAVAILABLE}
	}
	resolved, err := uc.ResolvePlayerNos(ctx, playerIDs)
	if err != nil {
		plog.With(ctx).Warnw("msg", "resolve_player_nos_query_failed", "count", len(playerIDs), "err", err)
		return &loginv1.ResolvePlayerNosResponse{Code: toProtoCode(err)}
	}
	entries := make([]*loginv1.ResolvedPlayerNo, 0, len(playerIDs))
	for _, playerID := range playerIDs {
		entries = append(entries, &loginv1.ResolvedPlayerNo{
			PlayerId: playerID,
			PlayerNo: resolved[playerID],
		})
	}
	return &loginv1.ResolvePlayerNosResponse{Code: commonv1.ErrCode_OK, Entries: entries}
}

func normalizePlayerNoIDs(raw []uint64) ([]uint64, bool) {
	if len(raw) == 0 || len(raw) > MaxResolvePlayerNosBatch {
		return nil, false
	}
	seen := make(map[uint64]struct{}, len(raw))
	ids := make([]uint64, 0, len(raw))
	for _, playerID := range raw {
		if playerID == 0 {
			return nil, false
		}
		if _, ok := seen[playerID]; ok {
			continue
		}
		seen[playerID] = struct{}{}
		ids = append(ids, playerID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, len(ids) != 0
}
