package data

import (
	"context"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
)

type fakePlayerInternalClient struct {
	resp     *playerv1.GetPlayerNamesResponse
	err      error
	calls    int
	gotCtx   context.Context
	gotReq   *playerv1.GetPlayerNamesRequest
	deadline time.Time
}

func TestGrpcPlayerNameResolverRejectsInvalidRawBatchBeforeRPC(t *testing.T) {
	signer, err := internalrpcauth.NewSigner(
		"team-player-name-client-test-0123456789abcdef", "team", "player:name")
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakePlayerInternalClient{resp: &playerv1.GetPlayerNamesResponse{Code: commonv1.ErrCode_OK}}
	resolver := newGrpcPlayerNameResolver(fake, signer)
	over := make([]uint64, maxPlayerNameResolveBatch+1)
	for i := range over {
		over[i] = 11 // 重复项也必须先按 raw 长度拒绝，不能靠去重绕过限界。
	}
	for name, ids := range map[string][]uint64{
		"empty": nil,
		"zero":  {11, 0},
		"over":  over,
	} {
		t.Run(name, func(t *testing.T) {
			got, resolveErr := resolver.ResolvePlayerNames(context.Background(), ids)
			if errcode.As(resolveErr) != errcode.ErrInvalidArg || got != nil {
				t.Fatalf("got=%v err=%v", got, resolveErr)
			}
		})
	}
	if fake.calls != 0 {
		t.Fatalf("invalid input reached RPC %d times", fake.calls)
	}
}

func TestGrpcPlayerNameResolverPropagatesInBandFailureWithoutPartialNames(t *testing.T) {
	signer, err := internalrpcauth.NewSigner(
		"team-player-name-client-test-0123456789abcdef", "team", "player:name")
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakePlayerInternalClient{resp: &playerv1.GetPlayerNamesResponse{
		Code:  commonv1.ErrCode_ERR_UNAVAILABLE,
		Names: []*playerv1.PlayerName{{PlayerId: 11, Nickname: "must-not-leak"}},
	}}
	resolver := newGrpcPlayerNameResolver(fake, signer)

	got, resolveErr := resolver.ResolvePlayerNames(context.Background(), []uint64{11})

	if errcode.As(resolveErr) != errcode.ErrUnavailable || got != nil {
		t.Fatalf("got=%v err=%v", got, resolveErr)
	}
}

func (f *fakePlayerInternalClient) ResolvePlayerNames(
	ctx context.Context,
	req *playerv1.GetPlayerNamesRequest,
	_ ...grpc.CallOption,
) (*playerv1.GetPlayerNamesResponse, error) {
	f.calls++
	f.gotCtx = ctx
	f.gotReq = proto.Clone(req).(*playerv1.GetPlayerNamesRequest)
	f.deadline, _ = ctx.Deadline()
	return f.resp, f.err
}

func TestGrpcPlayerNameResolverUsesOneSignedCanonicalBatchWithShortDeadline(t *testing.T) {
	const secret = "team-player-name-client-test-0123456789abcdef"
	const audience = "player:name"
	signer, err := internalrpcauth.NewSigner(secret, "team", audience)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakePlayerInternalClient{resp: &playerv1.GetPlayerNamesResponse{
		Code: commonv1.ErrCode_OK,
		Names: []*playerv1.PlayerName{
			{PlayerId: 11, Nickname: "Alice"},
			{PlayerId: 22, Nickname: "Bob"},
			{PlayerId: 99, Nickname: "unrequested"},
		},
	}}
	resolver := newGrpcPlayerNameResolver(fake, signer)
	started := time.Now()

	got, err := resolver.ResolvePlayerNames(context.Background(), []uint64{22, 11, 22})

	if err != nil {
		t.Fatalf("ResolvePlayerNames: %v", err)
	}
	if fake.calls != 1 || !reflect.DeepEqual(fake.gotReq.GetPlayerIds(), []uint64{11, 22}) {
		t.Fatalf("calls=%d request=%v, want one sorted unique batch", fake.calls, fake.gotReq.GetPlayerIds())
	}
	if !reflect.DeepEqual(got, map[uint64]string{11: "Alice", 22: "Bob"}) {
		t.Fatalf("resolved=%v", got)
	}
	if fake.deadline.IsZero() || fake.deadline.Sub(started) > playerNameResolveTimeout+50*time.Millisecond {
		t.Fatalf("deadline=%v, want independent timeout <= %v", fake.deadline, playerNameResolveTimeout)
	}

	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(fake.gotReq)
	if err != nil {
		t.Fatal(err)
	}
	md, _ := metadata.FromOutgoingContext(fake.gotCtx)
	incoming := metadata.NewIncomingContext(context.Background(), md)
	verifier, err := internalrpcauth.NewVerifier(secret, "team", audience, 30*time.Second,
		&resolverReplayFake{seen: map[string]struct{}{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.VerifyWithPayload(incoming,
		playerv1.PlayerInternalService_ResolvePlayerNames_FullMethodName, 11, payload); err != nil {
		t.Fatalf("payload-bound signature verify: %v", err)
	}
}
