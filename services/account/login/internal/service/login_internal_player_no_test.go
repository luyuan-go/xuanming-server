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
	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"
	"github.com/luyuancpp/pandora/services/account/login/internal/biz"
)

const playerNoAuthTestSecret = "player-no-resolve-auth-test-0123456789abcdef"
const playerNoAuthTestAudience = "login:player-no"

type playerNoBatchReaderFake struct {
	values map[uint64]uint64
	err    error
	calls  [][]uint64
}

func (f *playerNoBatchReaderFake) ResolvePlayerNos(_ context.Context, playerIDs []uint64) (map[uint64]uint64, error) {
	f.calls = append(f.calls, append([]uint64(nil), playerIDs...))
	return f.values, f.err
}

type playerNoReplayFake struct {
	seen map[string]struct{}
	err  error
}

func (f *playerNoReplayFake) Consume(_ context.Context, key string, _ time.Duration) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	if _, ok := f.seen[key]; ok {
		return false, nil
	}
	f.seen[key] = struct{}{}
	return true, nil
}

type unavailablePlayerNoVerifier struct{}

func (unavailablePlayerNoVerifier) VerifyWithPayload(context.Context, string, uint64, []byte) error {
	return internalrpcauth.ErrUnavailable
}

func newPlayerNoInternalService(t *testing.T, reader *playerNoBatchReaderFake) (*LoginInternalService, *internalrpcauth.Signer) {
	t.Helper()
	replay := &playerNoReplayFake{seen: map[string]struct{}{}}
	verifier, err := internalrpcauth.NewVerifier(playerNoAuthTestSecret, "team", playerNoAuthTestAudience,
		30*time.Second, replay)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := internalrpcauth.NewSigner(playerNoAuthTestSecret, "team", playerNoAuthTestAudience)
	if err != nil {
		t.Fatal(err)
	}
	return NewLoginInternalService(biz.NewPlayerNoResolveUsecase(reader), verifier), signer
}

func signedPlayerNoContext(t *testing.T, signer *internalrpcauth.Signer, playerIDs []uint64) context.Context {
	t.Helper()
	req := &loginv1.ResolvePlayerNosRequest{PlayerIds: playerIDs}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	out, err := signer.SignContextWithPayload(context.Background(),
		loginv1.LoginInternalService_ResolvePlayerNos_FullMethodName, playerIDs[0], payload)
	if err != nil {
		t.Fatal(err)
	}
	md, _ := metadata.FromOutgoingContext(out)
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestResolvePlayerNosUsesOneBoundedBatch(t *testing.T) {
	reader := &playerNoBatchReaderFake{values: map[uint64]uint64{11: 100011, 22: 100022}}
	svc, signer := newPlayerNoInternalService(t, reader)
	ctx := signedPlayerNoContext(t, signer, []uint64{11, 22})

	resp, err := svc.ResolvePlayerNos(ctx, &loginv1.ResolvePlayerNosRequest{PlayerIds: []uint64{11, 22}})

	if err != nil || resp.GetCode() != commonv1.ErrCode_OK {
		t.Fatalf("response=%+v err=%v", resp, err)
	}
	if len(reader.calls) != 1 || !reflect.DeepEqual(reader.calls[0], []uint64{11, 22}) {
		t.Fatalf("repo batches=%v, want one [11 22] query", reader.calls)
	}
	if len(resp.GetEntries()) != 2 || resp.GetEntries()[0].GetPlayerNo() != 100011 || resp.GetEntries()[1].GetPlayerNo() != 100022 {
		t.Fatalf("entries=%+v", resp.GetEntries())
	}
}

func TestResolvePlayerNosCanonicalizesDuplicates(t *testing.T) {
	reader := &playerNoBatchReaderFake{values: map[uint64]uint64{11: 100011, 22: 100022}}
	svc, signer := newPlayerNoInternalService(t, reader)
	ctx := signedPlayerNoContext(t, signer, []uint64{11, 22})

	resp, err := svc.ResolvePlayerNos(ctx,
		&loginv1.ResolvePlayerNosRequest{PlayerIds: []uint64{22, 11, 22, 11}})

	if err != nil || resp.GetCode() != commonv1.ErrCode_OK {
		t.Fatalf("response=%+v err=%v", resp, err)
	}
	if len(reader.calls) != 1 || !reflect.DeepEqual(reader.calls[0], []uint64{11, 22}) {
		t.Fatalf("repo batches=%v, want one sorted unique batch", reader.calls)
	}
}

func TestResolvePlayerNosRejectsInvalidBoundsBeforeRepo(t *testing.T) {
	reader := &playerNoBatchReaderFake{}
	svc, _ := newPlayerNoInternalService(t, reader)

	for name, ids := range map[string][]uint64{
		"empty": {},
		"zero":  {11, 0},
		"over":  make([]uint64, MaxResolvePlayerNosBatch+1),
	} {
		t.Run(name, func(t *testing.T) {
			if name == "over" {
				for i := range ids {
					ids[i] = uint64(i + 1)
				}
			}
			resp, err := svc.ResolvePlayerNos(context.Background(), &loginv1.ResolvePlayerNosRequest{PlayerIds: ids})
			if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG {
				t.Fatalf("response=%+v err=%v", resp, err)
			}
		})
	}
	if len(reader.calls) != 0 {
		t.Fatalf("invalid input reached repo: %v", reader.calls)
	}
}

func TestResolvePlayerNosRequiresPayloadBoundServiceIdentity(t *testing.T) {
	reader := &playerNoBatchReaderFake{}
	svc, signer := newPlayerNoInternalService(t, reader)
	req := &loginv1.ResolvePlayerNosRequest{PlayerIds: []uint64{11, 22}}

	unsigned, err := svc.ResolvePlayerNos(context.Background(), req)
	if err != nil || unsigned.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
		t.Fatalf("unsigned response=%+v err=%v", unsigned, err)
	}

	tamperedCtx := signedPlayerNoContext(t, signer, []uint64{11, 33})
	tampered, err := svc.ResolvePlayerNos(tamperedCtx, req)
	if err != nil || tampered.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
		t.Fatalf("tampered response=%+v err=%v", tampered, err)
	}
	if len(reader.calls) != 0 {
		t.Fatalf("unauthorized request reached repo: %v", reader.calls)
	}
}

func TestResolvePlayerNosVerifierUnavailableIsRetryable(t *testing.T) {
	reader := &playerNoBatchReaderFake{}
	svc := NewLoginInternalService(biz.NewPlayerNoResolveUsecase(reader), unavailablePlayerNoVerifier{})

	resp, err := svc.ResolvePlayerNos(context.Background(),
		&loginv1.ResolvePlayerNosRequest{PlayerIds: []uint64{11}})

	if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_UNAVAILABLE {
		t.Fatalf("response=%+v err=%v", resp, err)
	}
	if len(reader.calls) != 0 {
		t.Fatalf("unavailable verifier reached repo: %v", reader.calls)
	}
}

func TestResolvePlayerNosRepoFailureDoesNotReturnPartialData(t *testing.T) {
	reader := &playerNoBatchReaderFake{err: errors.New("mysql unavailable")}
	svc, signer := newPlayerNoInternalService(t, reader)
	ctx := signedPlayerNoContext(t, signer, []uint64{11})

	resp, err := svc.ResolvePlayerNos(ctx, &loginv1.ResolvePlayerNosRequest{PlayerIds: []uint64{11}})

	if err != nil || resp.GetCode() == commonv1.ErrCode_OK || len(resp.GetEntries()) != 0 {
		t.Fatalf("response=%+v err=%v", resp, err)
	}
	if len(reader.calls) != 1 {
		t.Fatalf("repo calls=%d, want one", len(reader.calls))
	}
}
