// owner_test.go — owner biz 校验层单测(假仓;状态机语义由 data 侧集成测试覆盖)。
package biz

import (
	"context"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/placement"

	"github.com/luyuancpp/pandora/services/runtime/owner/internal/conf"
	"github.com/luyuancpp/pandora/services/runtime/owner/internal/data"
)

// fakeOwnerRepo 记录调用的假仓。
type fakeOwnerRepo struct {
	beginCalls int
	lastMargin time.Duration
	lastOp     string
	renewCalls int
	lastLease  time.Duration
}

func (f *fakeOwnerRepo) Query(context.Context, uint64) (data.OwnerRecord, error) {
	return data.OwnerRecord{}, nil
}

func (f *fakeOwnerRepo) BeginTransition(_ context.Context, _, _ uint64, operationID string, _ int8, _ data.OwnerTarget, _ uint64, margin time.Duration) (data.OwnerRecord, error) {
	f.beginCalls++
	f.lastMargin = margin
	f.lastOp = operationID
	return data.OwnerRecord{}, nil
}

func (f *fakeOwnerRepo) Admit(context.Context, uint64, uint64, string, data.OwnerTarget) (data.OwnerRecord, int64, error) {
	return data.OwnerRecord{}, 0, nil
}

func (f *fakeOwnerRepo) RenewInstanceLease(_ context.Context, _ data.OwnerTarget, lease time.Duration) (int64, error) {
	f.renewCalls++
	f.lastLease = lease
	return 0, nil
}

func (f *fakeOwnerRepo) Release(context.Context, uint64, uint64, string, time.Duration) (data.OwnerRecord, error) {
	return data.OwnerRecord{}, nil
}

func (f *fakeOwnerRepo) SweepTransitionLog(context.Context, time.Duration, int) (int64, error) {
	return 0, nil
}

const validOp = "6f9619ff-8b86-4d01-b42d-00cf4fc964ff"

func validTarget() data.OwnerTarget {
	return data.OwnerTarget{PodName: "hub-1", InstanceUID: "uid-1", InstanceEpoch: 1,
		AssignmentOrAllocationID: "a-1", ReleaseTrack: "stable"}
}

func TestOwnerBeginValidation(t *testing.T) {
	repo := &fakeOwnerRepo{}
	uc := NewOwnerUsecase(repo, conf.OwnerConf{})
	ctx := context.Background()

	cases := []struct {
		what   string
		player uint64
		op     string
		otype  int8
		target data.OwnerTarget
		code   errcode.Code
	}{
		{"player=0", 0, validOp, data.OwnerTypeHub, validTarget(), errcode.ErrInvalidArg},
		{"非 UUIDv4 operation", 1, "not-a-uuid", data.OwnerTypeHub, validTarget(), errcode.ErrOwnerInvalidOperation},
		{"owner_type=none", 1, validOp, data.OwnerTypeNone, validTarget(), errcode.ErrOwnerInvalidOperation},
		{"目标身份不完整", 1, validOp, data.OwnerTypeHub, data.OwnerTarget{PodName: "p"}, errcode.ErrOwnerInvalidOperation},
	}
	for _, c := range cases {
		if _, err := uc.BeginTransition(ctx, c.player, 0, c.op, c.otype, c.target, 0); errcode.As(err) != c.code {
			t.Fatalf("%s: 期望 %d,实际 %v", c.what, c.code, err)
		}
	}
	if repo.beginCalls != 0 {
		t.Fatalf("校验失败不得触达数据层: %d", repo.beginCalls)
	}

	// 合法请求放行,margin 固定来自 pkg/placement(正确性常量单一来源)。
	if _, err := uc.BeginTransition(ctx, 1, 0, validOp, data.OwnerTypeHub, validTarget(), 0); err != nil {
		t.Fatalf("合法 Begin 应放行: %v", err)
	}
	wantMargin := time.Duration(placement.DSFenceSkewMarginSeconds) * time.Second
	if repo.beginCalls != 1 || repo.lastMargin != wantMargin {
		t.Fatalf("margin 应为 placement 常量 %v,实际 %v", wantMargin, repo.lastMargin)
	}
}

// 空 operation_id = 由权威铸造(§9.23 稳定 operation 的默认形态)。
// 调用方(两个 allocator)无法自己保证稳定——它们每次投递现铸一个 UUID,同一次进场的
// 重连/重复交付/心跳自愈会写出不同 operation。改由权威铸造后,同 exact 实例的重复投递
// 在数据层原样返回既有记录,只有真实迁移才会用到这里铸的新值。
func TestOwnerBeginMintsOperationWhenEmpty(t *testing.T) {
	repo := &fakeOwnerRepo{}
	uc := NewOwnerUsecase(repo, conf.OwnerConf{})
	ctx := context.Background()

	if _, err := uc.BeginTransition(ctx, 1, 0, "", data.OwnerTypeHub, validTarget(), 0); err != nil {
		t.Fatalf("空 operation 应放行并由权威铸造: %v", err)
	}
	if repo.beginCalls != 1 {
		t.Fatalf("应触达数据层一次,实际 %d", repo.beginCalls)
	}
	// 铸出来的必须能过 placement 的 canonical UUIDv4 校验——否则重放/审计/客户端续用
	// 三处都会在后续环节被同一个校验挡掉。
	if !placement.ValidOperationID(repo.lastOp) {
		t.Fatalf("铸造的 operation 必须是 canonical UUIDv4,实际 %q", repo.lastOp)
	}

	// 每次真实迁移铸的是新值(稳定性由数据层的同实例 no-op 保证,不是靠这里复用同一个)。
	first := repo.lastOp
	if _, err := uc.BeginTransition(ctx, 1, 0, "", data.OwnerTypeHub, validTarget(), 0); err != nil {
		t.Fatalf("第二次空 operation: %v", err)
	}
	if repo.lastOp == first {
		t.Fatalf("每次铸造应产生新 UUID,两次都是 %q", first)
	}

	// 非空仍按显式幂等键原样透传(响应丢失后的原样重试路径)。
	if _, err := uc.BeginTransition(ctx, 1, 0, validOp, data.OwnerTypeHub, validTarget(), 0); err != nil {
		t.Fatalf("显式 operation 应放行: %v", err)
	}
	if repo.lastOp != validOp {
		t.Fatalf("显式 operation 不得被改写: got=%q want=%q", repo.lastOp, validOp)
	}
}

func TestOwnerRenewClamp(t *testing.T) {
	repo := &fakeOwnerRepo{}
	uc := NewOwnerUsecase(repo, conf.OwnerConf{})
	ctx := context.Background()

	// 续租秒数超协议上限被硬钳制(调用方无法放大脑裂窗口)。
	if _, err := uc.RenewInstanceLease(ctx, validTarget(), placement.DSFenceLeaseMaxSeconds*10); err != nil {
		t.Fatalf("续租应放行: %v", err)
	}
	wantLease := time.Duration(placement.DSFenceLeaseMaxSeconds) * time.Second
	if repo.lastLease != wantLease {
		t.Fatalf("lease 应被钳制到 %v,实际 %v", wantLease, repo.lastLease)
	}
	// 实例身份不完整拒。
	if _, err := uc.RenewInstanceLease(ctx, data.OwnerTarget{}, 5); errcode.As(err) != errcode.ErrOwnerInvalidOperation {
		t.Fatalf("身份不完整应拒: %v", err)
	}
}
