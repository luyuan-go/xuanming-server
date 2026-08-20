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
	"github.com/luyuancpp/pandora/pkg/playerno"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"
)

// playerNoResolveTimeout 是 team 组装展示投影时 login 权威读取的独立短预算。
// 这是弱依赖：超时后 team 快照照常返回，player_no 降级为 0。
const playerNoResolveTimeout = 250 * time.Millisecond

const maxPlayerNoResolveBatch = playerno.ResolveBatchLimit

type GrpcPlayerNoResolver struct {
	conn   *grpc.ClientConn
	client loginv1.LoginInternalServiceClient
	signer *internalrpcauth.Signer
}

func NewGrpcPlayerNoResolver(addr string, signer *internalrpcauth.Signer) *GrpcPlayerNoResolver {
	conn := grpcclient.MustDialInsecure(addr)
	return &GrpcPlayerNoResolver{
		conn:   conn,
		client: loginv1.NewLoginInternalServiceClient(conn),
		signer: signer,
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

func (r *GrpcPlayerNoResolver) ResolvePlayerNos(ctx context.Context, playerIDs []uint64) (map[uint64]uint64, error) {
	ids, ok := canonicalPlayerNoIDs(playerIDs)
	if !ok {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_ids must contain 1..%d non-zero ids", maxPlayerNoResolveBatch)
	}
	if r == nil || r.client == nil || r.signer == nil {
		return nil, errcode.New(errcode.ErrUnavailable, "login player_no resolver not configured")
	}
	req := &loginv1.ResolvePlayerNosRequest{PlayerIds: ids}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(req)
	if err != nil {
		return nil, errcode.NewCause(errcode.ErrInternal, err, "marshal ResolvePlayerNos request")
	}
	callCtx, cancel := context.WithTimeout(ctx, playerNoResolveTimeout)
	defer cancel()
	signedCtx, err := r.signer.SignContextWithPayload(callCtx,
		loginv1.LoginInternalService_ResolvePlayerNos_FullMethodName, ids[0], payload)
	if err != nil {
		return nil, errcode.NewCause(errcode.ErrInternal, err, "sign ResolvePlayerNos request")
	}
	resp, err := r.client.ResolvePlayerNos(signedCtx, req)
	if err != nil {
		return nil, errcode.NewCause(errcode.ErrUnavailable, err, "login ResolvePlayerNos rpc")
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return nil, errcode.New(errcode.Code(resp.GetCode()), "login ResolvePlayerNos code=%d", resp.GetCode())
	}
	requested := make(map[uint64]struct{}, len(ids))
	for _, playerID := range ids {
		requested[playerID] = struct{}{}
	}
	result := make(map[uint64]uint64, len(ids))
	for _, entry := range resp.GetEntries() {
		if entry == nil {
			continue
		}
		if _, ok := requested[entry.GetPlayerId()]; !ok {
			continue
		}
		result[entry.GetPlayerId()] = entry.GetPlayerNo()
	}
	return result, nil
}

func canonicalPlayerNoIDs(raw []uint64) ([]uint64, bool) {
	if len(raw) == 0 || len(raw) > maxPlayerNoResolveBatch {
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
