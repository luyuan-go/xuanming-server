package data

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
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
)

// playerNameResolveTimeout 是 team 组装展示投影时 player 权威读取的独立短预算。
const playerNameResolveTimeout = 250 * time.Millisecond

const maxPlayerNameResolveBatch = playername.ResolveBatchLimit

type GrpcPlayerNameResolver struct {
	conn   *grpc.ClientConn
	client playerv1.PlayerInternalServiceClient
	signer *internalrpcauth.Signer
}

func NewGrpcPlayerNameResolver(addr string, signer *internalrpcauth.Signer) *GrpcPlayerNameResolver {
	conn := grpcclient.MustDialInsecure(addr)
	return &GrpcPlayerNameResolver{
		conn:   conn,
		client: playerv1.NewPlayerInternalServiceClient(conn),
		signer: signer,
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

func (r *GrpcPlayerNameResolver) ResolvePlayerNames(ctx context.Context, playerIDs []uint64) (map[uint64]string, error) {
	ids, ok := canonicalPlayerNameIDs(playerIDs)
	if !ok {
		return nil, errcode.New(errcode.ErrInvalidArg,
			"player_ids must contain 1..%d non-zero ids", maxPlayerNameResolveBatch)
	}
	if r == nil || r.client == nil || r.signer == nil {
		return nil, errcode.New(errcode.ErrUnavailable, "player name resolver not configured")
	}
	req := &playerv1.GetPlayerNamesRequest{PlayerIds: ids}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(req)
	if err != nil {
		return nil, errcode.NewCause(errcode.ErrInternal, err, "marshal ResolvePlayerNames request")
	}
	callCtx, cancel := context.WithTimeout(ctx, playerNameResolveTimeout)
	defer cancel()
	signedCtx, err := r.signer.SignContextWithPayload(callCtx,
		playerv1.PlayerInternalService_ResolvePlayerNames_FullMethodName, ids[0], payload)
	if err != nil {
		return nil, errcode.NewCause(errcode.ErrInternal, err, "sign ResolvePlayerNames request")
	}
	resp, err := r.client.ResolvePlayerNames(signedCtx, req)
	if err != nil {
		return nil, errcode.NewCause(errcode.ErrUnavailable, err, "player ResolvePlayerNames rpc")
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return nil, errcode.New(errcode.Code(resp.GetCode()),
			"player ResolvePlayerNames code=%d", resp.GetCode())
	}
	requested := make(map[uint64]struct{}, len(ids))
	for _, playerID := range ids {
		requested[playerID] = struct{}{}
	}
	result := make(map[uint64]string, len(resp.GetNames()))
	for _, name := range resp.GetNames() {
		if name == nil {
			continue
		}
		if _, ok := requested[name.GetPlayerId()]; !ok {
			continue
		}
		result[name.GetPlayerId()] = name.GetNickname()
	}
	return result, nil
}

func canonicalPlayerNameIDs(raw []uint64) ([]uint64, bool) {
	if len(raw) == 0 || len(raw) > maxPlayerNameResolveBatch {
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
