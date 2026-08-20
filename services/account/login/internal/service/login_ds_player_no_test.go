package service

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"
	"github.com/luyuancpp/pandora/services/account/login/internal/biz"
)

func TestResolvePlayerNosForDSGeneratedGRPCContract(t *testing.T) {
	const want = "/pandora.login.v1.LoginService/ResolvePlayerNosForDS"
	if got := loginv1.LoginService_ResolvePlayerNosForDS_FullMethodName; got != want {
		t.Fatalf("full method=%q, want %q", got, want)
	}
}

func TestResolvePlayerNosForDSUsesAuthenticatedActiveCredentialAndOneBatch(t *testing.T) {
	checker := &activeAdmissionChecker{}
	reader := &playerNoBatchReaderFake{values: map[uint64]uint64{1001: 700001, 1002: 0}}
	svc, signer, _ := newAdmissionService(t, checker)
	svc.SetPlayerNoResolveUsecase(biz.NewPlayerNoResolveUsecase(reader))
	credential, err := signer.SignBattleCredential(9001, "battle-1", "uid-b", 4, 8, "credential-jti", time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	resp, rpcErr := svc.ResolvePlayerNosForDS(
		admissionServiceContext(credential.Token),
		&loginv1.ResolvePlayerNosRequest{PlayerIds: []uint64{1002, 1001, 1002}},
	)

	if rpcErr != nil || resp.GetCode() != commonv1.ErrCode_OK {
		t.Fatalf("response=%+v rpcErr=%v", resp, rpcErr)
	}
	if checker.calls.Load() != 1 {
		t.Fatalf("active checker calls=%d, want 1", checker.calls.Load())
	}
	if len(reader.calls) != 1 || !reflect.DeepEqual(reader.calls[0], []uint64{1001, 1002}) {
		t.Fatalf("repo batches=%v, want one sorted unique batch", reader.calls)
	}
	if got := resp.GetEntries(); len(got) != 2 || got[0].GetPlayerId() != 1001 || got[0].GetPlayerNo() != 700001 || got[1].GetPlayerId() != 1002 || got[1].GetPlayerNo() != 0 {
		t.Fatalf("entries=%+v, want authoritative value and normal zero", got)
	}
}

func TestResolvePlayerNosForDSLegacyAllowsBoundedDevDirectRead(t *testing.T) {
	reader := &playerNoBatchReaderFake{values: map[uint64]uint64{11: 800011}}
	svc := NewLoginService(nil, nil)
	svc.SetPlayerNoResolveUsecase(biz.NewPlayerNoResolveUsecase(reader))

	resp, err := svc.ResolvePlayerNosForDS(context.Background(),
		&loginv1.ResolvePlayerNosRequest{PlayerIds: []uint64{11}})

	if err != nil || resp.GetCode() != commonv1.ErrCode_OK || len(resp.GetEntries()) != 1 || resp.GetEntries()[0].GetPlayerNo() != 800011 {
		t.Fatalf("response=%+v err=%v", resp, err)
	}
	if len(reader.calls) != 1 || !reflect.DeepEqual(reader.calls[0], []uint64{11}) {
		t.Fatalf("repo batches=%v, want one [11] batch", reader.calls)
	}
}

func TestResolvePlayerNosForDSRawBoundsAndZero(t *testing.T) {
	reader := &playerNoBatchReaderFake{}
	svc := NewLoginService(nil, nil)
	svc.SetPlayerNoResolveUsecase(biz.NewPlayerNoResolveUsecase(reader))
	over := make([]uint64, MaxResolvePlayerNosBatch+1)
	for i := range over {
		over[i] = uint64(i + 1)
	}
	for name, ids := range map[string][]uint64{
		"empty": nil,
		"zero":  {11, 0},
		"over":  over,
	} {
		t.Run(name, func(t *testing.T) {
			resp, err := svc.ResolvePlayerNosForDS(context.Background(),
				&loginv1.ResolvePlayerNosRequest{PlayerIds: ids})
			if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG {
				t.Fatalf("response=%+v err=%v", resp, err)
			}
		})
	}
	if len(reader.calls) != 0 {
		t.Fatalf("invalid input reached repo: %v", reader.calls)
	}

	atLimit := make([]uint64, MaxResolvePlayerNosBatch)
	for i := range atLimit {
		atLimit[i] = uint64(i + 1)
	}
	resp, err := svc.ResolvePlayerNosForDS(context.Background(),
		&loginv1.ResolvePlayerNosRequest{PlayerIds: atLimit})
	if err != nil || resp.GetCode() != commonv1.ErrCode_OK || len(resp.GetEntries()) != MaxResolvePlayerNosBatch {
		t.Fatalf("at-limit response=%+v err=%v", resp, err)
	}
	if len(reader.calls) != 1 || len(reader.calls[0]) != MaxResolvePlayerNosBatch {
		t.Fatalf("at-limit repo batches=%v", reader.calls)
	}
}

func TestResolvePlayerNosForDSRedisAuthorityFailsClosedBeforeRepo(t *testing.T) {
	for _, tc := range []struct {
		name    string
		checker *activeAdmissionChecker
		wire    bool
		want    commonv1.ErrCode
	}{
		{name: "missing-wiring", checker: &activeAdmissionChecker{}, want: commonv1.ErrCode_ERR_UNAVAILABLE},
		{name: "missing-bearer", checker: &activeAdmissionChecker{}, wire: true, want: commonv1.ErrCode_ERR_UNAUTHORIZED},
		{name: "inactive", checker: &activeAdmissionChecker{err: errcode.New(errcode.ErrUnavailable, "redis unavailable")}, wire: true, want: commonv1.ErrCode_ERR_UNAVAILABLE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &playerNoBatchReaderFake{}
			if !tc.wire {
				svc := NewLoginService(nil, nil)
				svc.SetPlayerNoResolveUsecase(biz.NewPlayerNoResolveUsecase(reader))
				svc.redisDSAdmission = true
				resp, err := svc.ResolvePlayerNosForDS(context.Background(),
					&loginv1.ResolvePlayerNosRequest{PlayerIds: []uint64{1001}})
				if err != nil || resp.GetCode() != tc.want {
					t.Fatalf("response=%+v err=%v", resp, err)
				}
			} else {
				svc, _, _ := newAdmissionService(t, tc.checker)
				svc.SetPlayerNoResolveUsecase(biz.NewPlayerNoResolveUsecase(reader))
				resp, err := svc.ResolvePlayerNosForDS(admissionServiceContext(""),
					&loginv1.ResolvePlayerNosRequest{PlayerIds: []uint64{1001}})
				if tc.name == "inactive" {
					_, signer, _ := newAdmissionService(t, tc.checker)
					credential, signErr := signer.SignBattleCredential(9001, "battle-1", "uid-b", 4, 8, "credential-jti", time.Hour)
					if signErr != nil {
						t.Fatal(signErr)
					}
					resp, err = svc.ResolvePlayerNosForDS(admissionServiceContext(credential.Token),
						&loginv1.ResolvePlayerNosRequest{PlayerIds: []uint64{1001}})
				}
				if err != nil || resp.GetCode() != tc.want {
					t.Fatalf("response=%+v err=%v", resp, err)
				}
			}
			if len(reader.calls) != 0 {
				t.Fatalf("rejected request reached repo: %v", reader.calls)
			}
		})
	}
}

func TestResolvePlayerNosForDSBattleCredentialCannotReadOutsideAdmission(t *testing.T) {
	reader := &playerNoBatchReaderFake{}
	svc, signer, _ := newAdmissionService(t, &activeAdmissionChecker{})
	svc.SetPlayerNoResolveUsecase(biz.NewPlayerNoResolveUsecase(reader))
	credential, err := signer.SignBattleCredential(9001, "battle-1", "uid-b", 4, 8, "credential-jti", time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	resp, rpcErr := svc.ResolvePlayerNosForDS(admissionServiceContext(credential.Token),
		&loginv1.ResolvePlayerNosRequest{PlayerIds: []uint64{9999}})

	if rpcErr != nil || resp.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
		t.Fatalf("response=%+v rpcErr=%v", resp, rpcErr)
	}
	if len(reader.calls) != 0 {
		t.Fatalf("out-of-admission request reached repo: %v", reader.calls)
	}
}

func TestResolvePlayerNosForDSRepoFailureReturnsNoPartialEntries(t *testing.T) {
	reader := &playerNoBatchReaderFake{
		values: map[uint64]uint64{1001: 700001},
		err:    errors.New("mysql unavailable"),
	}
	svc, signer, _ := newAdmissionService(t, &activeAdmissionChecker{})
	svc.SetPlayerNoResolveUsecase(biz.NewPlayerNoResolveUsecase(reader))
	credential, err := signer.SignBattleCredential(9001, "battle-1", "uid-b", 4, 8, "credential-jti", time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	resp, rpcErr := svc.ResolvePlayerNosForDS(admissionServiceContext(credential.Token),
		&loginv1.ResolvePlayerNosRequest{PlayerIds: []uint64{1001}})

	if rpcErr != nil || resp.GetCode() == commonv1.ErrCode_OK || len(resp.GetEntries()) != 0 {
		t.Fatalf("response=%+v rpcErr=%v", resp, rpcErr)
	}
	if len(reader.calls) != 1 {
		t.Fatalf("repo calls=%d, want one", len(reader.calls))
	}
}
