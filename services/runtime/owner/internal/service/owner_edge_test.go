package service

import (
	"context"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	ownerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/owner/v1"

	"github.com/luyuancpp/pandora/services/runtime/owner/internal/biz"
	"github.com/luyuancpp/pandora/services/runtime/owner/internal/conf"
	"github.com/luyuancpp/pandora/services/runtime/owner/internal/data"
)

type ownerServiceRepo struct {
	calls int

	queryRec      data.OwnerRecord
	queryErr      error
	beginRec      data.OwnerRecord
	beginErr      error
	admitRec      data.OwnerRecord
	admitRetry    int64
	admitErr      error
	renewDeadline int64
	renewErr      error
	releaseRec    data.OwnerRecord
	releaseErr    error
}

func (r *ownerServiceRepo) Query(context.Context, uint64) (data.OwnerRecord, error) {
	r.calls++
	return r.queryRec, r.queryErr
}

func (r *ownerServiceRepo) BeginTransition(context.Context, uint64, uint64, string, int8, data.OwnerTarget, uint64, time.Duration) (data.OwnerRecord, error) {
	r.calls++
	return r.beginRec, r.beginErr
}

func (r *ownerServiceRepo) Admit(context.Context, uint64, uint64, string, data.OwnerTarget) (data.OwnerRecord, int64, error) {
	r.calls++
	return r.admitRec, r.admitRetry, r.admitErr
}

func (r *ownerServiceRepo) RenewInstanceLease(context.Context, data.OwnerTarget, time.Duration) (int64, error) {
	r.calls++
	return r.renewDeadline, r.renewErr
}

func (r *ownerServiceRepo) Release(context.Context, uint64, uint64, string, time.Duration) (data.OwnerRecord, error) {
	r.calls++
	return r.releaseRec, r.releaseErr
}

func (r *ownerServiceRepo) SweepTransitionLog(context.Context, time.Duration, int) (int64, error) {
	r.calls++
	return 0, nil
}

func newOwnerServiceForTest(repo data.OwnerRepo) *OwnerService {
	return NewOwnerService(biz.NewOwnerUsecase(repo, conf.OwnerConf{}))
}

func TestOwnerServiceRejectsEveryClientCallableMethodBeforeBusinessLayer(t *testing.T) {
	ctx := plog.WithPlayerID(context.Background(), 1001)
	repo := &ownerServiceRepo{}
	svc := newOwnerServiceForTest(repo)

	tests := []struct {
		name string
		call func() (commonv1.ErrCode, error)
	}{
		{
			name: "QueryOwner",
			call: func() (commonv1.ErrCode, error) {
				resp, err := svc.QueryOwner(ctx, &ownerv1.QueryOwnerRequest{PlayerId: 1001})
				return resp.GetCode(), err
			},
		},
		{
			name: "BeginTransition",
			call: func() (commonv1.ErrCode, error) {
				resp, err := svc.BeginTransition(ctx, &ownerv1.BeginTransitionRequest{})
				return resp.GetCode(), err
			},
		},
		{
			name: "Admit",
			call: func() (commonv1.ErrCode, error) {
				resp, err := svc.Admit(ctx, &ownerv1.AdmitRequest{})
				return resp.GetCode(), err
			},
		},
		{
			name: "RenewInstanceLease",
			call: func() (commonv1.ErrCode, error) {
				resp, err := svc.RenewInstanceLease(ctx, &ownerv1.RenewInstanceLeaseRequest{})
				return resp.GetCode(), err
			},
		},
		{
			name: "ReleaseOwner",
			call: func() (commonv1.ErrCode, error) {
				resp, err := svc.ReleaseOwner(ctx, &ownerv1.ReleaseOwnerRequest{})
				return resp.GetCode(), err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.call()
			if err != nil {
				t.Fatalf("transport err=%v", err)
			}
			if got != commonv1.ErrCode_ERR_PERMISSION_DENY {
				t.Fatalf("code=%v，期望 ERR_PERMISSION_DENY", got)
			}
		})
	}
	if repo.calls != 0 {
		t.Fatalf("客户端调用触达了 %d 次 owner 权威数据层", repo.calls)
	}
}

func TestOwnerServiceKeepsConflictAndBarrierRecoveryEvidence(t *testing.T) {
	target := data.OwnerTarget{
		PodName:                  "battle-1",
		InstanceUID:              "uid-1",
		InstanceEpoch:            7,
		AssignmentOrAllocationID: "allocation-1",
		ReleaseTrack:             "canary",
	}
	record := data.OwnerRecord{
		PlayerID:         1001,
		OwnerEpoch:       9,
		OwnerType:        data.OwnerTypeBattle,
		Phase:            data.OwnerPhasePending,
		Target:           target,
		OperationID:      "6f9619ff-8b86-4d01-b42d-00cf4fc964ff",
		AdmitNotBeforeMs: 123456,
		LeaseDeadlineMs:  123999,
		UpdatedAtMs:      120000,
	}
	repo := &ownerServiceRepo{
		beginRec:   record,
		beginErr:   errcode.New(errcode.ErrOwnerEpochConflict, "epoch conflict"),
		admitRec:   record,
		admitRetry: 456,
		admitErr:   errcode.New(errcode.ErrOwnerBarrierNotOpen, "barrier not open"),
	}
	svc := newOwnerServiceForTest(repo)
	ctx := context.Background()
	protoTarget := toProtoTarget(target)

	beginResp, err := svc.BeginTransition(ctx, &ownerv1.BeginTransitionRequest{
		PlayerId:    1001,
		ExpectEpoch: 8,
		OperationId: record.OperationID,
		OwnerType:   ownerv1.OwnerType_OWNER_TYPE_BATTLE,
		Target:      protoTarget,
	})
	if err != nil {
		t.Fatalf("BeginTransition transport err=%v", err)
	}
	if beginResp.GetCode() != commonv1.ErrCode_ERR_OWNER_EPOCH_CONFLICT {
		t.Fatalf("BeginTransition code=%v", beginResp.GetCode())
	}
	assertOwnerProtoRecord(t, beginResp.GetRecord(), record)

	admitResp, err := svc.Admit(ctx, &ownerv1.AdmitRequest{
		PlayerId:    1001,
		OwnerEpoch:  9,
		OperationId: record.OperationID,
		Target:      protoTarget,
	})
	if err != nil {
		t.Fatalf("Admit transport err=%v", err)
	}
	if admitResp.GetCode() != commonv1.ErrCode_ERR_OWNER_BARRIER_NOT_OPEN || admitResp.GetRetryAfterMs() != 456 {
		t.Fatalf("Admit code/retry=%v/%d", admitResp.GetCode(), admitResp.GetRetryAfterMs())
	}
	assertOwnerProtoRecord(t, admitResp.GetRecord(), record)
}

func TestOwnerServiceDoesNotLeakRecordsForGenericFailures(t *testing.T) {
	record := data.OwnerRecord{PlayerID: 1001, OwnerEpoch: 4}
	repo := &ownerServiceRepo{
		beginRec: record,
		beginErr: errcode.New(errcode.ErrUnavailable, "storage unavailable"),
		admitRec: record,
		admitErr: errcode.New(errcode.ErrOwnerEpochConflict, "stale admit"),
	}
	svc := newOwnerServiceForTest(repo)
	op := "6f9619ff-8b86-4d01-b42d-00cf4fc964ff"
	target := &ownerv1.OwnerTarget{
		PodName: "hub-1", InstanceUid: "uid-1", InstanceEpoch: 1,
		AssignmentOrAllocationId: "assignment-1", ReleaseTrack: "stable",
	}

	beginResp, beginErr := svc.BeginTransition(context.Background(), &ownerv1.BeginTransitionRequest{
		PlayerId: 1001, OperationId: op, OwnerType: ownerv1.OwnerType_OWNER_TYPE_HUB, Target: target,
	})
	if beginErr != nil {
		t.Fatalf("BeginTransition transport err=%v", beginErr)
	}
	if beginResp.GetCode() != commonv1.ErrCode_ERR_UNAVAILABLE || beginResp.GetRecord() != nil {
		t.Fatalf("普通 Begin 失败不应携带权威记录: code=%v record=%+v", beginResp.GetCode(), beginResp.GetRecord())
	}

	admitResp, admitErr := svc.Admit(context.Background(), &ownerv1.AdmitRequest{
		PlayerId: 1001, OwnerEpoch: 4, OperationId: op, Target: target,
	})
	if admitErr != nil {
		t.Fatalf("Admit transport err=%v", admitErr)
	}
	if admitResp.GetCode() != commonv1.ErrCode_ERR_OWNER_EPOCH_CONFLICT || admitResp.GetRecord() != nil {
		t.Fatalf("非屏障 Admit 失败不应携带权威记录: code=%v record=%+v", admitResp.GetCode(), admitResp.GetRecord())
	}
	if admitResp.GetRetryAfterMs() != 0 {
		t.Fatalf("非 BARRIER_NOT_OPEN 不得携带 retry_after，got=%d", admitResp.GetRetryAfterMs())
	}
}

func assertOwnerProtoRecord(t *testing.T, got *ownerv1.OwnerRecord, want data.OwnerRecord) {
	t.Helper()
	if got == nil {
		t.Fatal("缺少 owner record")
	}
	if got.GetPlayerId() != want.PlayerID || got.GetOwnerEpoch() != want.OwnerEpoch ||
		int8(got.GetOwnerType()) != want.OwnerType || int8(got.GetPhase()) != want.Phase ||
		got.GetOperationId() != want.OperationID || got.GetAdmitNotBeforeMs() != want.AdmitNotBeforeMs ||
		got.GetLeaseDeadlineMs() != want.LeaseDeadlineMs || got.GetUpdatedAtMs() != want.UpdatedAtMs {
		t.Fatalf("record 字段漂移: got=%+v want=%+v", got, want)
	}
	if target := fromProtoTarget(got.GetTarget()); !target.Equal(want.Target) {
		t.Fatalf("target 字段漂移: got=%+v want=%+v", target, want.Target)
	}
}
