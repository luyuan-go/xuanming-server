package service

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
	"github.com/luyuancpp/pandora/services/account/player/internal/biz"
)

const playerNameAuthTestSecret = "player-name-resolve-auth-test-0123456789abcdef"
const playerNameAuthTestAudience = "player:name"
const resolvePlayerNamesFullMethod = "/pandora.player.v1.PlayerInternalService/ResolvePlayerNames"

func TestResolvePlayerNamesGeneratedGRPCContract(t *testing.T) {
	if got := playerv1.PlayerInternalService_ResolvePlayerNames_FullMethodName; got != resolvePlayerNamesFullMethod {
		t.Fatalf("full method=%q, want %q", got, resolvePlayerNamesFullMethod)
	}
}

type playerNameUsecaseFake struct {
	names []biz.PlayerName
	err   error
	calls [][]uint64
}

func (f *playerNameUsecaseFake) GetPlayerNames(_ context.Context, playerIDs []uint64) ([]biz.PlayerName, error) {
	f.calls = append(f.calls, append([]uint64(nil), playerIDs...))
	return f.names, f.err
}

type playerNameReplayFake struct {
	seen map[string]struct{}
}

type unavailablePlayerNameVerifier struct{}

func (unavailablePlayerNameVerifier) VerifyWithPayload(context.Context, string, uint64, []byte) error {
	return internalrpcauth.ErrUnavailable
}

func (f *playerNameReplayFake) Consume(_ context.Context, key string, _ time.Duration) (bool, error) {
	if _, ok := f.seen[key]; ok {
		return false, nil
	}
	f.seen[key] = struct{}{}
	return true, nil
}

func newPlayerInternalNameService(t *testing.T, uc *playerNameUsecaseFake) (*PlayerInternalService, *internalrpcauth.Signer) {
	t.Helper()
	verifier, err := internalrpcauth.NewVerifier(playerNameAuthTestSecret, "team", playerNameAuthTestAudience,
		30*time.Second, &playerNameReplayFake{seen: map[string]struct{}{}})
	if err != nil {
		t.Fatal(err)
	}
	signer, err := internalrpcauth.NewSigner(playerNameAuthTestSecret, "team", playerNameAuthTestAudience)
	if err != nil {
		t.Fatal(err)
	}
	return NewPlayerInternalService(uc, verifier), signer
}

func signedPlayerNameContext(t *testing.T, signer *internalrpcauth.Signer, playerIDs []uint64) context.Context {
	t.Helper()
	req := &playerv1.GetPlayerNamesRequest{PlayerIds: playerIDs}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	out, err := signer.SignContextWithPayload(context.Background(), resolvePlayerNamesFullMethod, playerIDs[0], payload)
	if err != nil {
		t.Fatal(err)
	}
	md, _ := metadata.FromOutgoingContext(out)
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestResolvePlayerNamesUsesOneAuthenticatedCanonicalBatch(t *testing.T) {
	uc := &playerNameUsecaseFake{names: []biz.PlayerName{{PlayerID: 11, Nickname: "Alice"}}}
	svc, signer := newPlayerInternalNameService(t, uc)
	ctx := signedPlayerNameContext(t, signer, []uint64{11, 22})

	resp, err := svc.ResolvePlayerNames(ctx, &playerv1.GetPlayerNamesRequest{PlayerIds: []uint64{22, 11, 22}})

	if err != nil || resp.GetCode() != commonv1.ErrCode_OK {
		t.Fatalf("response=%+v err=%v", resp, err)
	}
	if len(uc.calls) != 1 || !reflect.DeepEqual(uc.calls[0], []uint64{11, 22}) {
		t.Fatalf("usecase batches=%v, want one sorted unique batch", uc.calls)
	}
	if len(resp.GetNames()) != 1 || resp.GetNames()[0].GetPlayerId() != 11 || resp.GetNames()[0].GetNickname() != "Alice" {
		t.Fatalf("names=%+v", resp.GetNames())
	}
}

func TestResolvePlayerNamesRejectsInvalidRawBoundsBeforeUsecase(t *testing.T) {
	uc := &playerNameUsecaseFake{}
	svc, _ := newPlayerInternalNameService(t, uc)
	over := make([]uint64, MaxResolvePlayerNamesBatch+1)
	for i := range over {
		over[i] = uint64(i + 1)
	}
	for name, ids := range map[string][]uint64{
		"empty": nil,
		"zero":  {11, 0},
		"over":  over,
	} {
		t.Run(name, func(t *testing.T) {
			resp, err := svc.ResolvePlayerNames(context.Background(), &playerv1.GetPlayerNamesRequest{PlayerIds: ids})
			if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG {
				t.Fatalf("response=%+v err=%v", resp, err)
			}
		})
	}
	if len(uc.calls) != 0 {
		t.Fatalf("invalid input reached usecase: %v", uc.calls)
	}
}

func TestResolvePlayerNamesRequiresPayloadBoundTeamIdentity(t *testing.T) {
	uc := &playerNameUsecaseFake{}
	svc, signer := newPlayerInternalNameService(t, uc)
	req := &playerv1.GetPlayerNamesRequest{PlayerIds: []uint64{11, 22}}

	unsigned, err := svc.ResolvePlayerNames(context.Background(), req)
	if err != nil || unsigned.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
		t.Fatalf("unsigned response=%+v err=%v", unsigned, err)
	}
	tampered, err := svc.ResolvePlayerNames(signedPlayerNameContext(t, signer, []uint64{11, 33}), req)
	if err != nil || tampered.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
		t.Fatalf("tampered response=%+v err=%v", tampered, err)
	}
	if len(uc.calls) != 0 {
		t.Fatalf("unauthorized request reached usecase: %v", uc.calls)
	}
}

func TestResolvePlayerNamesReplayIsRejected(t *testing.T) {
	uc := &playerNameUsecaseFake{}
	svc, signer := newPlayerInternalNameService(t, uc)
	ctx := signedPlayerNameContext(t, signer, []uint64{11})
	req := &playerv1.GetPlayerNamesRequest{PlayerIds: []uint64{11}}
	first, err := svc.ResolvePlayerNames(ctx, req)
	if err != nil || first.GetCode() != commonv1.ErrCode_OK {
		t.Fatalf("first response=%+v err=%v", first, err)
	}
	second, err := svc.ResolvePlayerNames(ctx, req)
	if err != nil || second.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
		t.Fatalf("replay response=%+v err=%v", second, err)
	}
	if len(uc.calls) != 1 {
		t.Fatalf("usecase calls=%d, want one before replay rejection", len(uc.calls))
	}
}

func TestResolvePlayerNamesVerifierUnavailableIsRetryable(t *testing.T) {
	uc := &playerNameUsecaseFake{}
	svc := NewPlayerInternalService(uc, unavailablePlayerNameVerifier{})
	resp, err := svc.ResolvePlayerNames(context.Background(),
		&playerv1.GetPlayerNamesRequest{PlayerIds: []uint64{11}})
	if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_UNAVAILABLE {
		t.Fatalf("response=%+v err=%v", resp, err)
	}
	if len(uc.calls) != 0 {
		t.Fatalf("unavailable verifier reached usecase: %v", uc.calls)
	}
}

func TestResolvePlayerNamesQueryFailureReturnsNoPartialNames(t *testing.T) {
	uc := &playerNameUsecaseFake{
		names: []biz.PlayerName{{PlayerID: 11, Nickname: "must-not-leak"}},
		err:   errors.New("mysql unavailable"),
	}
	svc, signer := newPlayerInternalNameService(t, uc)
	resp, err := svc.ResolvePlayerNames(signedPlayerNameContext(t, signer, []uint64{11}),
		&playerv1.GetPlayerNamesRequest{PlayerIds: []uint64{11}})
	if err != nil || resp.GetCode() == commonv1.ErrCode_OK || len(resp.GetNames()) != 0 {
		t.Fatalf("response=%+v err=%v", resp, err)
	}
	if len(uc.calls) != 1 {
		t.Fatalf("usecase calls=%d, want one", len(uc.calls))
	}
}
