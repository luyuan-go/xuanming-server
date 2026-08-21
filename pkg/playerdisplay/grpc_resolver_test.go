package playerdisplay

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
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
)

type friendPlayerNameClientFake struct {
	ctx      context.Context
	req      *playerv1.GetPlayerNamesRequest
	deadline time.Time
}

func (f *friendPlayerNameClientFake) ResolvePlayerNames(ctx context.Context, req *playerv1.GetPlayerNamesRequest, _ ...grpc.CallOption) (*playerv1.GetPlayerNamesResponse, error) {
	f.ctx = ctx
	f.req = proto.Clone(req).(*playerv1.GetPlayerNamesRequest)
	f.deadline, _ = ctx.Deadline()
	return &playerv1.GetPlayerNamesResponse{Code: commonv1.ErrCode_OK, Names: []*playerv1.PlayerName{
		{PlayerId: 11, Nickname: "Alice"}, {PlayerId: 99, Nickname: "unrequested"},
	}}, nil
}

type friendPlayerNoClientFake struct {
	ctx      context.Context
	req      *loginv1.ResolvePlayerNosRequest
	deadline time.Time
}

func (f *friendPlayerNoClientFake) ResolvePlayerNos(ctx context.Context, req *loginv1.ResolvePlayerNosRequest, _ ...grpc.CallOption) (*loginv1.ResolvePlayerNosResponse, error) {
	f.ctx = ctx
	f.req = proto.Clone(req).(*loginv1.ResolvePlayerNosRequest)
	f.deadline, _ = ctx.Deadline()
	return &loginv1.ResolvePlayerNosResponse{Code: commonv1.ErrCode_OK, Entries: []*loginv1.ResolvedPlayerNo{
		{PlayerId: 11, PlayerNo: 10011}, {PlayerId: 99, PlayerNo: 10099},
	}}, nil
}

type friendResolverReplayFake struct{ seen map[string]struct{} }

func (f *friendResolverReplayFake) Consume(_ context.Context, key string, _ time.Duration) (bool, error) {
	if _, ok := f.seen[key]; ok {
		return false, nil
	}
	f.seen[key] = struct{}{}
	return true, nil
}

func TestPlayerDisplayResolversUseFriendPayloadBoundIdentity(t *testing.T) {
	const nameSecret = "friend-name-resolver-client-test-key-0123456789"
	const noSecret = "friend-no-resolver-client-test-key-012345678901"
	nameSigner, err := internalrpcauth.NewSigner(nameSecret, "friend", "player:name")
	if err != nil {
		t.Fatal(err)
	}
	noSigner, err := internalrpcauth.NewSigner(noSecret, "friend", "login:player-no")
	if err != nil {
		t.Fatal(err)
	}
	nameClient := &friendPlayerNameClientFake{}
	noClient := &friendPlayerNoClientFake{}
	nameStarted := time.Now()
	names, err := newGrpcPlayerNameResolver(nameClient, nameSigner).ResolvePlayerNames(context.Background(), []uint64{22, 11, 22})
	if err != nil {
		t.Fatalf("ResolvePlayerNames: %v", err)
	}
	noStarted := time.Now()
	numbers, err := newGrpcPlayerNoResolver(noClient, noSigner).ResolvePlayerNos(context.Background(), []uint64{22, 11, 22})
	if err != nil {
		t.Fatalf("ResolvePlayerNos: %v", err)
	}
	if !reflect.DeepEqual(nameClient.req.GetPlayerIds(), []uint64{11, 22}) ||
		!reflect.DeepEqual(noClient.req.GetPlayerIds(), []uint64{11, 22}) {
		t.Fatalf("canonical requests name=%v no=%v", nameClient.req.GetPlayerIds(), noClient.req.GetPlayerIds())
	}
	if !reflect.DeepEqual(names, map[uint64]string{11: "Alice"}) ||
		!reflect.DeepEqual(numbers, map[uint64]uint64{11: 10011}) {
		t.Fatalf("filtered projection names=%v numbers=%v", names, numbers)
	}
	if nameClient.deadline.IsZero() || nameClient.deadline.Sub(nameStarted) > playerNameResolveTimeout+50*time.Millisecond {
		t.Fatalf("name deadline=%v, want <=%v", nameClient.deadline, playerNameResolveTimeout)
	}
	if noClient.deadline.IsZero() || noClient.deadline.Sub(noStarted) > playerNoResolveTimeout+50*time.Millisecond {
		t.Fatalf("player_no deadline=%v, want <=%v", noClient.deadline, playerNoResolveTimeout)
	}
	verifyResolverSignature(t, nameClient.ctx, nameClient.req, nameSecret, "friend", "player:name",
		playerv1.PlayerInternalService_ResolvePlayerNames_FullMethodName)
	verifyResolverSignature(t, noClient.ctx, noClient.req, noSecret, "friend", "login:player-no",
		loginv1.LoginInternalService_ResolvePlayerNos_FullMethodName)
}

func TestPlayerDisplayResolverAcceptsGuildCallerSigner(t *testing.T) {
	const secret = "guild-name-resolver-client-test-key-01234567890"
	signer, err := internalrpcauth.NewSigner(secret, "guild", "player:name")
	if err != nil {
		t.Fatal(err)
	}
	client := &friendPlayerNameClientFake{}
	if _, err := newGrpcPlayerNameResolver(client, signer).ResolvePlayerNames(context.Background(), []uint64{11}); err != nil {
		t.Fatalf("ResolvePlayerNames: %v", err)
	}
	verifyResolverSignature(t, client.ctx, client.req, secret, "guild", "player:name",
		playerv1.PlayerInternalService_ResolvePlayerNames_FullMethodName)
}

func verifyResolverSignature(t *testing.T, outgoing context.Context, request proto.Message, secret, caller, audience, method string) {
	t.Helper()
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	md, _ := metadata.FromOutgoingContext(outgoing)
	verifier, err := internalrpcauth.NewVerifier(secret, caller, audience, 30*time.Second,
		&friendResolverReplayFake{seen: map[string]struct{}{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.VerifyWithPayload(metadata.NewIncomingContext(context.Background(), md), method, 11, payload); err != nil {
		t.Fatalf("payload-bound %s signature: %v", caller, err)
	}
}
