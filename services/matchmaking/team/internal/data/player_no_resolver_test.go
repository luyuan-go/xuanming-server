package data

import (
	"context"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"
)

type fakeLoginInternalClient struct {
	resp     *loginv1.ResolvePlayerNosResponse
	err      error
	calls    int
	gotCtx   context.Context
	gotReq   *loginv1.ResolvePlayerNosRequest
	deadline time.Time
}

func (f *fakeLoginInternalClient) ResolvePlayerNos(
	ctx context.Context,
	req *loginv1.ResolvePlayerNosRequest,
	_ ...grpc.CallOption,
) (*loginv1.ResolvePlayerNosResponse, error) {
	f.calls++
	f.gotCtx = ctx
	f.gotReq = proto.Clone(req).(*loginv1.ResolvePlayerNosRequest)
	f.deadline, _ = ctx.Deadline()
	return f.resp, f.err
}

type resolverReplayFake struct{ seen map[string]struct{} }

func (f *resolverReplayFake) Consume(_ context.Context, key string, _ time.Duration) (bool, error) {
	if _, ok := f.seen[key]; ok {
		return false, nil
	}
	f.seen[key] = struct{}{}
	return true, nil
}

func TestGrpcPlayerNoResolverUsesOneSignedCanonicalBatchWithShortDeadline(t *testing.T) {
	const secret = "team-player-no-client-test-0123456789abcdef"
	const audience = "login:player-no"
	signer, err := internalrpcauth.NewSigner(secret, "team", audience)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeLoginInternalClient{resp: &loginv1.ResolvePlayerNosResponse{
		Code: commonv1.ErrCode_OK,
		Entries: []*loginv1.ResolvedPlayerNo{
			{PlayerId: 11, PlayerNo: 100011},
			{PlayerId: 22, PlayerNo: 100022},
		},
	}}
	resolver := newGrpcPlayerNoResolver(fake, signer)
	started := time.Now()

	got, err := resolver.ResolvePlayerNos(context.Background(), []uint64{22, 11, 22})

	if err != nil {
		t.Fatalf("ResolvePlayerNos: %v", err)
	}
	if fake.calls != 1 || !reflect.DeepEqual(fake.gotReq.GetPlayerIds(), []uint64{11, 22}) {
		t.Fatalf("calls=%d request=%v, want one sorted unique batch", fake.calls, fake.gotReq.GetPlayerIds())
	}
	if got[11] != 100011 || got[22] != 100022 {
		t.Fatalf("resolved=%v", got)
	}
	if fake.deadline.IsZero() || fake.deadline.Sub(started) > playerNoResolveTimeout+50*time.Millisecond {
		t.Fatalf("deadline=%v, want independent timeout <= %v", fake.deadline, playerNoResolveTimeout)
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
		loginv1.LoginInternalService_ResolvePlayerNos_FullMethodName, 11, payload); err != nil {
		t.Fatalf("payload-bound signature verify: %v", err)
	}
}
