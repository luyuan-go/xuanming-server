package playerdisplay

import (
	"context"
	"sort"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	"github.com/luyuancpp/pandora/pkg/playername"
	"github.com/luyuancpp/pandora/pkg/playerno"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
)

const (
	playerNameResolveTimeout = 250 * time.Millisecond
	playerNoResolveTimeout   = 250 * time.Millisecond
)

type GrpcPlayerNameResolver struct {
	conn   *grpc.ClientConn
	client playerv1.PlayerInternalServiceClient
	signer *internalrpcauth.Signer
}

func NewGrpcPlayerNameResolver(addr string, signer *internalrpcauth.Signer) *GrpcPlayerNameResolver {
	conn := grpcclient.MustDialInsecure(addr)
	return &GrpcPlayerNameResolver{
		conn: conn, client: playerv1.NewPlayerInternalServiceClient(conn), signer: signer,
	}
}

func newGrpcPlayerNameResolver(client playerv1.PlayerInternalServiceClient, signer *internalrpcauth.Signer) *GrpcPlayerNameResolver {
	return &GrpcPlayerNameResolver{client: client, signer: signer}
}

func (r *GrpcPlayerNameResolver) Close() error {
	if r != nil && r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

func (r *GrpcPlayerNameResolver) ResolvePlayerNames(ctx context.Context, rawPlayerIDs []uint64) (map[uint64]string, error) {
	playerIDs, ok := canonicalDisplayIDs(rawPlayerIDs, playername.ResolveBatchLimit)
	if !ok {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_ids must contain 1..%d non-zero ids", playername.ResolveBatchLimit)
	}
	if r == nil || r.client == nil || r.signer == nil {
		return nil, errcode.New(errcode.ErrUnavailable, "player name resolver not configured")
	}
	req := &playerv1.GetPlayerNamesRequest{PlayerIds: playerIDs}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(req)
	if err != nil {
		return nil, errcode.NewCause(errcode.ErrInternal, err, "marshal ResolvePlayerNames request")
	}
	callCtx, cancel := context.WithTimeout(ctx, playerNameResolveTimeout)
	defer cancel()
	signedCtx, err := r.signer.SignContextWithPayload(callCtx,
		playerv1.PlayerInternalService_ResolvePlayerNames_FullMethodName, playerIDs[0], payload)
	if err != nil {
		return nil, errcode.NewCause(errcode.ErrInternal, err, "sign ResolvePlayerNames request")
	}
	resp, err := r.client.ResolvePlayerNames(signedCtx, req)
	if err != nil {
		return nil, errcode.NewCause(errcode.ErrUnavailable, err, "player ResolvePlayerNames rpc")
	}
	if resp == nil || resp.GetCode() != commonv1.ErrCode_OK {
		if resp == nil {
			return nil, errcode.New(errcode.ErrUnavailable, "player ResolvePlayerNames empty response")
		}
		return nil, errcode.New(errcode.Code(resp.GetCode()), "player ResolvePlayerNames code=%d", resp.GetCode())
	}
	requested := idSet(playerIDs)
	result := make(map[uint64]string, len(resp.GetNames()))
	for _, entry := range resp.GetNames() {
		if entry == nil {
			continue
		}
		if _, ok := requested[entry.GetPlayerId()]; ok {
			result[entry.GetPlayerId()] = entry.GetNickname()
		}
	}
	return result, nil
}

type GrpcPlayerNoResolver struct {
	conn   *grpc.ClientConn
	client loginv1.LoginInternalServiceClient
	signer *internalrpcauth.Signer
}

func NewGrpcPlayerNoResolver(addr string, signer *internalrpcauth.Signer) *GrpcPlayerNoResolver {
	conn := grpcclient.MustDialInsecure(addr)
	return &GrpcPlayerNoResolver{
		conn: conn, client: loginv1.NewLoginInternalServiceClient(conn), signer: signer,
	}
}

func newGrpcPlayerNoResolver(client loginv1.LoginInternalServiceClient, signer *internalrpcauth.Signer) *GrpcPlayerNoResolver {
	return &GrpcPlayerNoResolver{client: client, signer: signer}
}

func (r *GrpcPlayerNoResolver) Close() error {
	if r != nil && r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

func (r *GrpcPlayerNoResolver) ResolvePlayerNos(ctx context.Context, rawPlayerIDs []uint64) (map[uint64]uint64, error) {
	playerIDs, ok := canonicalDisplayIDs(rawPlayerIDs, playerno.ResolveBatchLimit)
	if !ok {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_ids must contain 1..%d non-zero ids", playerno.ResolveBatchLimit)
	}
	if r == nil || r.client == nil || r.signer == nil {
		return nil, errcode.New(errcode.ErrUnavailable, "player_no resolver not configured")
	}
	req := &loginv1.ResolvePlayerNosRequest{PlayerIds: playerIDs}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(req)
	if err != nil {
		return nil, errcode.NewCause(errcode.ErrInternal, err, "marshal ResolvePlayerNos request")
	}
	callCtx, cancel := context.WithTimeout(ctx, playerNoResolveTimeout)
	defer cancel()
	signedCtx, err := r.signer.SignContextWithPayload(callCtx,
		loginv1.LoginInternalService_ResolvePlayerNos_FullMethodName, playerIDs[0], payload)
	if err != nil {
		return nil, errcode.NewCause(errcode.ErrInternal, err, "sign ResolvePlayerNos request")
	}
	resp, err := r.client.ResolvePlayerNos(signedCtx, req)
	if err != nil {
		return nil, errcode.NewCause(errcode.ErrUnavailable, err, "login ResolvePlayerNos rpc")
	}
	if resp == nil || resp.GetCode() != commonv1.ErrCode_OK {
		if resp == nil {
			return nil, errcode.New(errcode.ErrUnavailable, "login ResolvePlayerNos empty response")
		}
		return nil, errcode.New(errcode.Code(resp.GetCode()), "login ResolvePlayerNos code=%d", resp.GetCode())
	}
	requested := idSet(playerIDs)
	result := make(map[uint64]uint64, len(resp.GetEntries()))
	for _, entry := range resp.GetEntries() {
		if entry == nil {
			continue
		}
		if _, ok := requested[entry.GetPlayerId()]; ok {
			result[entry.GetPlayerId()] = entry.GetPlayerNo()
		}
	}
	return result, nil
}

func canonicalDisplayIDs(raw []uint64, limit int) ([]uint64, bool) {
	if len(raw) == 0 || len(raw) > limit {
		return nil, false
	}
	seen := make(map[uint64]struct{}, len(raw))
	result := make([]uint64, 0, len(raw))
	for _, playerID := range raw {
		if playerID == 0 {
			return nil, false
		}
		if _, ok := seen[playerID]; ok {
			continue
		}
		seen[playerID] = struct{}{}
		result = append(result, playerID)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, len(result) != 0
}

func idSet(playerIDs []uint64) map[uint64]struct{} {
	result := make(map[uint64]struct{}, len(playerIDs))
	for _, playerID := range playerIDs {
		result[playerID] = struct{}{}
	}
	return result
}
