package service

import (
	"context"
	"errors"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/playername"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
	"github.com/luyuancpp/pandora/services/account/player/internal/biz"
)

// MaxResolvePlayerNamesBatch 限制内部名称解析请求的原始 player_ids 数量。
const MaxResolvePlayerNamesBatch = playername.ResolveBatchLimit

type playerNameUsecase interface {
	GetPlayerNames(context.Context, []uint64) ([]biz.PlayerName, error)
}

type playerNameRequestVerifier interface {
	VerifyWithPayload(context.Context, string, uint64, []byte) error
}

// PlayerInternalService 实现不经客户端或 DS Envoy 暴露的 player 域内部读取接口。
type PlayerInternalService struct {
	playerv1.UnimplementedPlayerInternalServiceServer
	uc       playerNameUsecase
	verifier playerNameRequestVerifier
}

func NewPlayerInternalService(uc playerNameUsecase, verifier playerNameRequestVerifier) *PlayerInternalService {
	return &PlayerInternalService{uc: uc, verifier: verifier}
}

// ResolvePlayerNames 只接受 player_id，并把规范化后的完整请求绑定到
// team/friend/guild 各自独立的内部服务身份签名。
func (s *PlayerInternalService) ResolvePlayerNames(
	ctx context.Context,
	req *playerv1.GetPlayerNamesRequest,
) (*playerv1.GetPlayerNamesResponse, error) {
	playerIDs, ok := normalizePlayerNameIDs(req.GetPlayerIds())
	if !ok {
		return &playerv1.GetPlayerNamesResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	canonicalReq := &playerv1.GetPlayerNamesRequest{PlayerIds: playerIDs}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonicalReq)
	if err != nil {
		plog.With(ctx).Errorw("msg", "resolve_player_names_marshal_failed", "count", len(playerIDs), "err", err)
		return &playerv1.GetPlayerNamesResponse{Code: commonv1.ErrCode_ERR_INTERNAL}, nil
	}
	if s == nil || s.verifier == nil {
		plog.With(ctx).Warnw("msg", "resolve_player_names_auth_unavailable", "count", len(playerIDs))
		return &playerv1.GetPlayerNamesResponse{Code: commonv1.ErrCode_ERR_UNAVAILABLE}, nil
	}
	if err := s.verifier.VerifyWithPayload(ctx,
		playerv1.PlayerInternalService_ResolvePlayerNames_FullMethodName, playerIDs[0], payload); err != nil {
		code := commonv1.ErrCode_ERR_PERMISSION_DENY
		if errors.Is(err, internalrpcauth.ErrUnavailable) {
			code = commonv1.ErrCode_ERR_UNAVAILABLE
		}
		plog.With(ctx).Warnw("msg", "resolve_player_names_auth_rejected", "count", len(playerIDs),
			"code", code, "err", err)
		return &playerv1.GetPlayerNamesResponse{Code: code}, nil
	}
	if s.uc == nil {
		return &playerv1.GetPlayerNamesResponse{Code: commonv1.ErrCode_ERR_UNAVAILABLE}, nil
	}
	names, err := s.uc.GetPlayerNames(ctx, playerIDs)
	if err != nil {
		plog.With(ctx).Warnw("msg", "resolve_player_names_query_failed", "count", len(playerIDs), "err", err)
		return &playerv1.GetPlayerNamesResponse{Code: toProtoCode(err)}, nil
	}
	requested := make(map[uint64]struct{}, len(playerIDs))
	for _, playerID := range playerIDs {
		requested[playerID] = struct{}{}
	}
	resolved := make(map[uint64]string, len(names))
	for _, name := range names {
		if _, ok := requested[name.PlayerID]; ok {
			resolved[name.PlayerID] = name.Nickname
		}
	}
	out := make([]*playerv1.PlayerName, 0, len(resolved))
	for _, playerID := range playerIDs {
		if nickname, ok := resolved[playerID]; ok {
			out = append(out, &playerv1.PlayerName{PlayerId: playerID, Nickname: nickname})
		}
	}
	return &playerv1.GetPlayerNamesResponse{Code: commonv1.ErrCode_OK, Names: out}, nil
}

func normalizePlayerNameIDs(raw []uint64) ([]uint64, bool) {
	if len(raw) == 0 || len(raw) > MaxResolvePlayerNamesBatch {
		return nil, false
	}
	seen := make(map[uint64]struct{}, len(raw))
	playerIDs := make([]uint64, 0, len(raw))
	for _, playerID := range raw {
		if playerID == 0 {
			return nil, false
		}
		if _, ok := seen[playerID]; ok {
			continue
		}
		seen[playerID] = struct{}{}
		playerIDs = append(playerIDs, playerID)
	}
	sort.Slice(playerIDs, func(i, j int) bool { return playerIDs[i] < playerIDs[j] })
	return playerIDs, len(playerIDs) != 0
}
