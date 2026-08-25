package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"

	"github.com/luyuancpp/pandora/services/runtime/owner/internal/conf"
	"github.com/luyuancpp/pandora/services/runtime/owner/internal/data"
)

type ownerEdgeRepo struct {
	queryCalls   int
	beginCalls   int
	admitCalls   int
	renewCalls   int
	releaseCalls int
	releaseSkew  time.Duration
	sweepCalls   int

	queryPlayerID  uint64
	admitPlayerID  uint64
	admitEpoch     uint64
	admitOp        string
	admitTarget    data.OwnerTarget
	releasePlayer  uint64
	releaseEpoch   uint64
	releaseOp      string
	sweepRetention time.Duration
	sweepBatch     int

	queryRecord data.OwnerRecord
	queryErr    error
	admitRecord data.OwnerRecord
	admitRetry  int64
	admitErr    error
	releaseRec  data.OwnerRecord
	releaseErr  error
	sweepCount  int64
	sweepErr    error
}

func (r *ownerEdgeRepo) Query(_ context.Context, playerID uint64) (data.OwnerRecord, error) {
	r.queryCalls++
	r.queryPlayerID = playerID
	return r.queryRecord, r.queryErr
}

func (r *ownerEdgeRepo) BeginTransition(context.Context, uint64, uint64, string, int8, data.OwnerTarget, uint64, time.Duration) (data.OwnerRecord, error) {
	r.beginCalls++
	return data.OwnerRecord{}, nil
}

func (r *ownerEdgeRepo) Admit(_ context.Context, playerID, ownerEpoch uint64, operationID string, target data.OwnerTarget) (data.OwnerRecord, int64, error) {
	r.admitCalls++
	r.admitPlayerID = playerID
	r.admitEpoch = ownerEpoch
	r.admitOp = operationID
	r.admitTarget = target
	return r.admitRecord, r.admitRetry, r.admitErr
}

func (r *ownerEdgeRepo) RenewInstanceLease(context.Context, data.OwnerTarget, time.Duration) (int64, error) {
	r.renewCalls++
	return 0, nil
}

func (r *ownerEdgeRepo) Release(_ context.Context, playerID, ownerEpoch uint64, operationID string, skewMargin time.Duration) (data.OwnerRecord, error) {
	r.releaseCalls++
	r.releasePlayer = playerID
	r.releaseEpoch = ownerEpoch
	r.releaseOp = operationID
	r.releaseSkew = skewMargin
	return r.releaseRec, r.releaseErr
}

func (r *ownerEdgeRepo) SweepTransitionLog(_ context.Context, retention time.Duration, batch int) (int64, error) {
	r.sweepCalls++
	r.sweepRetention = retention
	r.sweepBatch = batch
	return r.sweepCount, r.sweepErr
}

func (r *ownerEdgeRepo) totalCalls() int {
	return r.queryCalls + r.beginCalls + r.admitCalls + r.renewCalls + r.releaseCalls + r.sweepCalls
}

func TestOwnerUsecaseInvalidInputsDoNotReachRepository(t *testing.T) {
	tests := []struct {
		name string
		want errcode.Code
		call func(*OwnerUsecase) error
	}{
		{
			name: "查询缺少玩家",
			want: errcode.ErrInvalidArg,
			call: func(uc *OwnerUsecase) error {
				_, err := uc.Query(context.Background(), 0)
				return err
			},
		},
		{
			name: "准入缺少玩家",
			want: errcode.ErrInvalidArg,
			call: func(uc *OwnerUsecase) error {
				_, _, err := uc.Admit(context.Background(), 0, 1, validOp, validTarget())
				return err
			},
		},
		{
			name: "准入缺少epoch",
			want: errcode.ErrInvalidArg,
			call: func(uc *OwnerUsecase) error {
				_, _, err := uc.Admit(context.Background(), 1, 0, validOp, validTarget())
				return err
			},
		},
		{
			name: "准入operation非法",
			want: errcode.ErrOwnerInvalidOperation,
			call: func(uc *OwnerUsecase) error {
				_, _, err := uc.Admit(context.Background(), 1, 1, "bad-operation", validTarget())
				return err
			},
		},
		{
			name: "准入目标不完整",
			want: errcode.ErrOwnerInvalidOperation,
			call: func(uc *OwnerUsecase) error {
				_, _, err := uc.Admit(context.Background(), 1, 1, validOp, data.OwnerTarget{PodName: "hub-1"})
				return err
			},
		},
		{
			name: "续租缺少实例身份",
			want: errcode.ErrOwnerInvalidOperation,
			call: func(uc *OwnerUsecase) error {
				_, err := uc.RenewInstanceLease(context.Background(), data.OwnerTarget{}, 1)
				return err
			},
		},
		{
			name: "续租秒数为零",
			want: errcode.ErrInvalidArg,
			call: func(uc *OwnerUsecase) error {
				target := validTarget()
				target.AssignmentOrAllocationID = ""
				_, err := uc.RenewInstanceLease(context.Background(), target, 0)
				return err
			},
		},
		{
			name: "释放缺少玩家",
			want: errcode.ErrInvalidArg,
			call: func(uc *OwnerUsecase) error {
				_, err := uc.Release(context.Background(), 0, 1, validOp)
				return err
			},
		},
		{
			name: "释放operation非法",
			want: errcode.ErrOwnerInvalidOperation,
			call: func(uc *OwnerUsecase) error {
				_, err := uc.Release(context.Background(), 1, 1, "bad-operation")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &ownerEdgeRepo{}
			uc := NewOwnerUsecase(repo, conf.OwnerConf{})
			err := tt.call(uc)
			if got := errcode.As(err); got != tt.want {
				t.Fatalf("错误码=%d，期望=%d，err=%v", got, tt.want, err)
			}
			if calls := repo.totalCalls(); calls != 0 {
				t.Fatalf("校验失败产生了 %d 次数据层调用", calls)
			}
		})
	}
}

func TestOwnerUsecaseForwardsExactAuthorityArgumentsAndErrors(t *testing.T) {
	sentinel := errors.New("repository unavailable")
	record := data.OwnerRecord{PlayerID: 88, OwnerEpoch: 9, OperationID: validOp}
	target := validTarget()
	repo := &ownerEdgeRepo{
		queryRecord: record,
		admitRecord: record,
		admitRetry:  321,
		admitErr:    sentinel,
		releaseRec:  record,
		releaseErr:  sentinel,
		sweepCount:  17,
		sweepErr:    sentinel,
	}
	uc := NewOwnerUsecase(repo, conf.OwnerConf{LogRetentionDays: 90})
	ctx := context.Background()

	got, err := uc.Query(ctx, 88)
	if err != nil || got != record || repo.queryPlayerID != 88 {
		t.Fatalf("Query 未原样转发/返回: record=%+v err=%v player=%d", got, err, repo.queryPlayerID)
	}

	got, retry, err := uc.Admit(ctx, 88, 9, validOp, target)
	if !errors.Is(err, sentinel) || got != record || retry != 321 {
		t.Fatalf("Admit 未保留记录/retry/error: record=%+v retry=%d err=%v", got, retry, err)
	}
	if repo.admitPlayerID != 88 || repo.admitEpoch != 9 || repo.admitOp != validOp || !repo.admitTarget.Equal(target) {
		t.Fatalf("Admit 参数漂移: player=%d epoch=%d op=%q target=%+v", repo.admitPlayerID, repo.admitEpoch, repo.admitOp, repo.admitTarget)
	}

	got, err = uc.Release(ctx, 88, 9, validOp)
	if !errors.Is(err, sentinel) || got != record {
		t.Fatalf("Release 未保留记录/error: record=%+v err=%v", got, err)
	}
	if repo.releasePlayer != 88 || repo.releaseEpoch != 9 || repo.releaseOp != validOp {
		t.Fatalf("Release 参数漂移: player=%d epoch=%d op=%q", repo.releasePlayer, repo.releaseEpoch, repo.releaseOp)
	}

	count, err := uc.RunTransitionLogSweep(ctx, 500)
	if !errors.Is(err, sentinel) || count != 17 {
		t.Fatalf("Sweep 未保留 count/error: count=%d err=%v", count, err)
	}
	if repo.sweepRetention != 90*24*time.Hour || repo.sweepBatch != 500 {
		t.Fatalf("Sweep 参数错误: retention=%v batch=%d", repo.sweepRetention, repo.sweepBatch)
	}
}
