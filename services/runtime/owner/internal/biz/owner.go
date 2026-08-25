// Package biz — owner 权威用例(owner-authority.md §3)。
//
// 职责:入参形状校验(operation UUIDv4 / 目标身份完整性 / 类型合法性)→ 委托 OwnerRepo
// (CAS / 屏障 / 幂等全部在数据层事务内)。fence 常量单一来源 pkg/placement:
// skew margin 是 admit_not_before 的余量项,lease 秒数钳制 ≤ DSFenceLeaseMaxSeconds。
package biz

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/placement"

	"github.com/luyuancpp/pandora/services/runtime/owner/internal/conf"
	"github.com/luyuancpp/pandora/services/runtime/owner/internal/data"
)

// 入参形状拒绝 reason 枚举(§11.3 R2)。
//
// 这些错误码(ErrInvalidArg / ErrOwnerInvalidOperation)均不属 IsServerFault,
// access log 只记 DEBUG —— 不在业务侧自己打,线上完全看不到谁在用垃圾参数
// 敲 owner 权威,现象只剩下「这玩家归属一直推不动」。
const (
	reasonPlayerIDRequired    = "player_id_required"
	reasonOwnerEpochRequired  = "owner_epoch_required"
	reasonOperationIDInvalid  = "operation_id_not_uuid_v4"
	reasonOwnerTypeInvalid    = "owner_type_invalid"
	reasonTargetIncomplete    = "target_identity_incomplete"
	reasonInstanceIncomplete  = "instance_identity_incomplete"
	reasonLeaseSecondsMissing = "lease_seconds_required"
)

// logRejected 记录一次 owner 用例层的副作用前拒绝。
// ⚠️ 内部调用面 plog.With(ctx) 不会自动注入 player_id,必须手写。
func logRejected(ctx context.Context, rpc, reason string, playerID uint64, extra ...any) {
	kvs := []any{"msg", "owner_request_rejected",
		"rpc", rpc, "reason", reason, "player_id", playerID}
	plog.With(ctx).Warnw(append(kvs, extra...)...)
}

// OwnerUsecase owner 权威用例。
type OwnerUsecase struct {
	repo data.OwnerRepo
	cfg  conf.OwnerConf
}

// NewOwnerUsecase 构造。
func NewOwnerUsecase(repo data.OwnerRepo, cfg conf.OwnerConf) *OwnerUsecase {
	return &OwnerUsecase{repo: repo, cfg: cfg}
}

// Query 读当前 owner 记录(调用方查询失败一律按 UNKNOWN 处理,§9.22)。
func (u *OwnerUsecase) Query(ctx context.Context, playerID uint64) (data.OwnerRecord, error) {
	if playerID == 0 {
		logRejected(ctx, "Query", reasonPlayerIDRequired, playerID)
		return data.OwnerRecord{}, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	return u.repo.Query(ctx, playerID)
}

// BeginTransition 发起 owner 迁移(CAS;幂等键 = (player, exact 实例) 或调用方显式 operation)。
//
// operation_id 语义(§9.23「一次真实进场 / owner 迁移使用一个稳定 operation_id」):
//   - **空 = 由本权威铸造**。这是默认形态:调用方(allocator 签票点 / READY 交付点)无法自己
//     保证稳定——它们每次投递现铸一个 UUID,同一次进场的重连、重复交付、心跳自愈会写出
//     不同 operation,幂等键失效。改由权威铸造后,同 exact 实例的重复投递在数据层原样返回
//     既有记录(含原 operation_id),真实迁移才铸新的,operation 自然贯穿整条进场链。
//   - **非空 = 调用方持显式幂等键**(响应丢失后原样重试),必须是 canonical UUIDv4。
//
// sourceRevision 是 Hub assignment 的来源版本(INC-20260818-003)。0 = 调用方尚未滚上
// 本协议(兼容窗);owner_type=BATTLE 时无意义,数据层会忽略。本层刻意**不**校验它的
// 大小关系 —— 判定必须与写入落在同一个行锁事务里,放在这里做等于又造一个 TOCTOU。
func (u *OwnerUsecase) BeginTransition(ctx context.Context, playerID, expectEpoch uint64, operationID string, ownerType int8, target data.OwnerTarget, sourceRevision uint64) (data.OwnerRecord, error) {
	if playerID == 0 {
		logRejected(ctx, "BeginTransition", reasonPlayerIDRequired, playerID,
			"operation_id", operationID, "owner_type", ownerType, "req_pod", target.PodName)
		return data.OwnerRecord{}, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if operationID != "" && !placement.ValidOperationID(operationID) {
		logRejected(ctx, "BeginTransition", reasonOperationIDInvalid, playerID,
			"operation_id", operationID, "expect_epoch", expectEpoch)
		return data.OwnerRecord{}, errcode.New(errcode.ErrOwnerInvalidOperation, "operation_id must be canonical UUIDv4")
	}
	if ownerType != data.OwnerTypeHub && ownerType != data.OwnerTypeBattle {
		logRejected(ctx, "BeginTransition", reasonOwnerTypeInvalid, playerID,
			"owner_type", ownerType, "operation_id", operationID,
			"expect_owner_type_hub", data.OwnerTypeHub, "expect_owner_type_battle", data.OwnerTypeBattle)
		return data.OwnerRecord{}, errcode.New(errcode.ErrOwnerInvalidOperation, "owner_type must be HUB or BATTLE")
	}
	if !target.Complete() {
		// exact 实例身份不全 = 无法做 §9.22 的 exact 绑定。逐项打出哪一项缺了。
		logRejected(ctx, "BeginTransition", reasonTargetIncomplete, playerID,
			"operation_id", operationID, "owner_type", ownerType,
			"req_pod", target.PodName, "req_instance_uid", target.InstanceUID,
			"req_instance_epoch", target.InstanceEpoch,
			"req_assignment_id", target.AssignmentOrAllocationID,
			"req_release_track", target.ReleaseTrack)
		return data.OwnerRecord{}, errcode.New(errcode.ErrOwnerInvalidOperation, "target identity incomplete")
	}
	// 空 operation → 权威铸造。真实迁移时被写入记录并从此贯穿本次进场链;若数据层判定为
	// 同 exact 实例的重复投递(no-op 原样返回既有记录),这个新铸的值直接丢弃,不落库。
	if operationID == "" {
		operationID = uuid.NewString()
	}
	return u.repo.BeginTransition(ctx, playerID, expectEpoch, operationID, ownerType, target,
		sourceRevision, time.Duration(placement.DSFenceSkewMarginSeconds)*time.Second)
}

// Admit 准入提交(屏障 + exact 身份;幂等重放)。
func (u *OwnerUsecase) Admit(ctx context.Context, playerID, ownerEpoch uint64, operationID string, target data.OwnerTarget) (data.OwnerRecord, int64, error) {
	if playerID == 0 || ownerEpoch == 0 {
		reason := reasonPlayerIDRequired
		if playerID != 0 {
			reason = reasonOwnerEpochRequired
		}
		logRejected(ctx, "Admit", reason, playerID,
			"owner_epoch", ownerEpoch, "operation_id", operationID, "req_pod", target.PodName)
		return data.OwnerRecord{}, 0, errcode.New(errcode.ErrInvalidArg, "player_id/owner_epoch required")
	}
	if !placement.ValidOperationID(operationID) {
		logRejected(ctx, "Admit", reasonOperationIDInvalid, playerID,
			"owner_epoch", ownerEpoch, "operation_id", operationID)
		return data.OwnerRecord{}, 0, errcode.New(errcode.ErrOwnerInvalidOperation, "operation_id must be canonical UUIDv4")
	}
	if !target.Complete() {
		logRejected(ctx, "Admit", reasonTargetIncomplete, playerID,
			"owner_epoch", ownerEpoch, "operation_id", operationID,
			"req_pod", target.PodName, "req_instance_uid", target.InstanceUID,
			"req_instance_epoch", target.InstanceEpoch,
			"req_assignment_id", target.AssignmentOrAllocationID,
			"req_release_track", target.ReleaseTrack)
		return data.OwnerRecord{}, 0, errcode.New(errcode.ErrOwnerInvalidOperation, "target identity incomplete")
	}
	return u.repo.Admit(ctx, playerID, ownerEpoch, operationID, target)
}

// RenewInstanceLease 实例租约续期(allocator 心跳代写;秒数钳制到协议上限)。
func (u *OwnerUsecase) RenewInstanceLease(ctx context.Context, target data.OwnerTarget, leaseSeconds uint32) (int64, error) {
	// 续租要求 pod + uid;instance_epoch 允许 0(hub 凭据不携带实例纪元,uid 全局唯一已足,
	// 纪元守卫在数据层只对"双方都非零且不同"拒)。分配 ID 是玩家维度信息,租约是实例级。
	if target.PodName == "" || target.InstanceUID == "" {
		logRejected(ctx, "RenewInstanceLease", reasonInstanceIncomplete, 0,
			"req_pod", target.PodName, "req_instance_uid", target.InstanceUID,
			"req_instance_epoch", target.InstanceEpoch, "lease_seconds", leaseSeconds)
		return 0, errcode.New(errcode.ErrOwnerInvalidOperation, "instance identity incomplete")
	}
	if leaseSeconds == 0 {
		logRejected(ctx, "RenewInstanceLease", reasonLeaseSecondsMissing, 0,
			"req_pod", target.PodName, "req_instance_uid", target.InstanceUID)
		return 0, errcode.New(errcode.ErrInvalidArg, "lease_seconds required")
	}
	if leaseSeconds > placement.DSFenceLeaseMaxSeconds {
		// 协议上限硬钳制(§8 fence 契约):配置/调用方无法放大脑裂窗口。
		// 钳制是静默改写调用方意图,不可见就会把「租约比我要的短」误归因为续租掉链。
		plog.With(ctx).Warnw("msg", "owner_lease_seconds_clamped",
			"reason", "lease_seconds_over_protocol_max",
			"req_pod", target.PodName, "req_instance_uid", target.InstanceUID,
			"req_lease_seconds", leaseSeconds, "max_lease_seconds", placement.DSFenceLeaseMaxSeconds)
		leaseSeconds = placement.DSFenceLeaseMaxSeconds
	}
	return u.repo.RenewInstanceLease(ctx, target, time.Duration(leaseSeconds)*time.Second)
}

// Release 显式释放(登出/终局;迟到调用幂等 no-op)。
func (u *OwnerUsecase) Release(ctx context.Context, playerID, ownerEpoch uint64, operationID string) (data.OwnerRecord, error) {
	if playerID == 0 {
		logRejected(ctx, "Release", reasonPlayerIDRequired, playerID,
			"owner_epoch", ownerEpoch, "operation_id", operationID)
		return data.OwnerRecord{}, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if !placement.ValidOperationID(operationID) {
		logRejected(ctx, "Release", reasonOperationIDInvalid, playerID,
			"owner_epoch", ownerEpoch, "operation_id", operationID)
		return data.OwnerRecord{}, errcode.New(errcode.ErrOwnerInvalidOperation, "operation_id must be canonical UUIDv4")
	}
	// skewMargin 与 BeginTransition 同源同值:释放 BATTLE 归属时数据层要用同一个公式
	// 把再入屏障盖进 admit_not_before 留存(INC-20260824-003)。两处取值必须一致,
	// 否则「释放算出来的屏障」与「迁移算出来的屏障」会在同一条时间线上打架。
	return u.repo.Release(ctx, playerID, ownerEpoch, operationID,
		time.Duration(placement.DSFenceSkewMarginSeconds)*time.Second)
}

// RunTransitionLogSweep 周期清理审计流水(§9.24)。
func (u *OwnerUsecase) RunTransitionLogSweep(ctx context.Context, batch int) (int64, error) {
	retention := time.Duration(u.cfg.LogRetentionDays) * 24 * time.Hour
	return u.repo.SweepTransitionLog(ctx, retention, batch)
}
