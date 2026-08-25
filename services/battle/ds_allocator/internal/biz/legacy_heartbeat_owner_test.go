// legacy_heartbeat_owner_test.go — legacy 战斗心跳的 owner 接线回归(2026-08-04)。
//
// 背景:legacy 面心跳既不续 owner 实例租约、也不代提交在场玩家 Admit,于是玩家已 travel
// 进战斗 DS、连接也建好了,owner 记录却恒 PENDING、实例租约恒过期。login 的
// applyOwnerPlacement 只在「ADMITTED 且租约剩余 > 安全余量」才报 STABLE,客户端因此恒收到
// "post-travel owner target is still PENDING",撑到 30s deadline 弹兜底面板,进不去副本。
// 修复 = HeartbeatWithCensus 用战斗记录的 exact 实例身份续租 + 按真实在场名单 Admit。
package biz

import (
	"context"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// recordingLeaseRenewer 记录 owner 实例租约续写调用。
type recordingLeaseRenewer struct {
	calls int
	pod   string
	uid   string
	epoch uint32
	track string
}

func (r *recordingLeaseRenewer) RenewInstanceLease(_ context.Context,
	podName, instanceUID string, instanceEpoch uint32, releaseTrack string) error {
	r.calls++
	r.pod, r.uid, r.epoch, r.track = podName, instanceUID, instanceEpoch, releaseTrack
	return nil
}

// legacyOwnerFixture 装配一个已 ready 的本地面对局(带 exact 实例身份)。
func legacyOwnerFixture(t *testing.T, matchID uint64, playerIDs []uint64) (
	*AllocatorUsecase, *recordingLeaseRenewer, string,
) {
	t.Helper()
	cfg := testCfg()
	alloc := &localIdentityAllocator{
		MockGameServerAllocator: NewMockGameServerAllocator(cfg),
		uid:                     "uid-legacy-battle",
		epoch:                   1,
	}
	uc, repo, _ := newUsecaseWithAlloc(t, alloc)
	lease := &recordingLeaseRenewer{}
	uc.SetOwnerLeaseRenewer(lease, false)

	res := allocateReady(t, uc, repo, matchID, playerIDs, 7, "pve_coop")
	if res.GameserverUID == "" {
		t.Fatal("夹具前置:分配必须已回填实例身份")
	}
	return uc, lease, res.DSPodName
}

// legacy 心跳必须以战斗记录的 exact 实例身份续写 owner 实例租约。
func TestLegacyBattleHeartbeat_RenewsOwnerInstanceLease(t *testing.T) {
	const matchID uint64 = 91001
	uc, lease, pod := legacyOwnerFixture(t, matchID, []uint64{2001})
	before := lease.calls

	if _, err := uc.HeartbeatWithCensus(context.Background(), matchID, pod, 1, "running",
		time.Now().UnixMilli(), true, []uint64{2001}); err != nil {
		t.Fatalf("HeartbeatWithCensus err: %v", err)
	}
	if lease.calls <= before {
		t.Fatal("legacy 战斗心跳必须续写 owner 实例租约")
	}
	if lease.pod != pod || lease.uid != "uid-legacy-battle" || lease.epoch != 1 {
		t.Fatalf("续租身份必须取自战斗记录, got %+v", lease)
	}
}

// census 缺席 + 确有人连入(playerCount>0)时,按花名册兜底代提交 Admit。
//
// 为什么必须兜底(而不是"没 census 就不 Admit"):local-off-v1 下 census **结构上
// 不可能成立**(UE 要求 dst_ver==2 且带实例绑定的战斗票,而本档位刻意只签 HS256
// legacy 票、legacy 票又带不了绑定)。不兜底 = owner 永远 PENDING = 玩家进了副本却
// 永远等不到 STABLE、30s 后弹兜底面板(2026-08-04 实测)。
func TestLegacyBattleHeartbeat_NoCensusFallsBackToRoster(t *testing.T) {
	const matchID uint64 = 91002
	uc, lease, pod := legacyOwnerFixture(t, matchID, []uint64{2002})
	admits := &countingOwnerAuthority{}
	uc.SetOwnerAuthority(admits)
	before := lease.calls

	if _, err := uc.HeartbeatWithCensus(context.Background(), matchID, pod, 1, "running",
		time.Now().UnixMilli(), false, nil); err != nil {
		t.Fatalf("HeartbeatWithCensus err: %v", err)
	}
	if lease.calls <= before {
		t.Fatal("无 census 时仍必须续写实例租约")
	}
	if admits.queries == 0 {
		t.Fatal("census 缺席且确有人连入时必须按花名册兜底代提交 Admit")
	}
}

// 空场(playerCount==0)不得兜底 Admit:没人连进来就没有可准入的对象,
// 只续租(实例确实在服务)。这条守住"不替一个不存在的在场者宣称已准入"。
func TestLegacyBattleHeartbeat_EmptyBattleNeverAdmits(t *testing.T) {
	const matchID uint64 = 91003
	uc, lease, pod := legacyOwnerFixture(t, matchID, []uint64{2003})
	admits := &countingOwnerAuthority{}
	uc.SetOwnerAuthority(admits)
	before := lease.calls

	if _, err := uc.Heartbeat(context.Background(), matchID, pod, 0, "running", time.Now().UnixMilli()); err != nil {
		t.Fatalf("Heartbeat err: %v", err)
	}
	if lease.calls <= before {
		t.Fatal("空场心跳同样必须续写实例租约")
	}
	if admits.queries != 0 {
		t.Fatalf("空场不得兜底 Admit, queries=%d", admits.queries)
	}
}

// countingOwnerAuthority 只计数,不产生真实归属副作用。
type countingOwnerAuthority struct {
	queries int
	admits  int
}

func (c *countingOwnerAuthority) QueryOwner(context.Context, uint64) (data.OwnerRecordView, error) {
	c.queries++
	return data.OwnerRecordView{}, nil
}

func (c *countingOwnerAuthority) BeginTransition(context.Context, uint64, uint64, string, int8,
	data.OwnerTargetView) (data.OwnerRecordView, error) {
	return data.OwnerRecordView{}, nil
}

func (c *countingOwnerAuthority) Admit(context.Context, uint64, uint64, string,
	data.OwnerTargetView) (int64, error) {
	c.admits++
	return 0, nil
}

func (c *countingOwnerAuthority) ReleaseOwner(context.Context, uint64, uint64, string) error {
	return nil
}

// releaseRecordingOwnerAuthority 记录 owner 释放调用(QueryOwner 返回指向本实例的
// BATTLE/ADMITTED 记录,使 exact 身份门成立)。
type releaseRecordingOwnerAuthority struct {
	pod      string
	uid      string
	released []uint64
}

func (r *releaseRecordingOwnerAuthority) QueryOwner(_ context.Context, _ uint64) (data.OwnerRecordView, error) {
	return data.OwnerRecordView{
		OwnerType: ownerTypeBattle, Phase: ownerPhaseAdmitted, OwnerEpoch: 7,
		OperationID: "op-1", PodName: r.pod, InstanceUID: r.uid,
	}, nil
}

func (r *releaseRecordingOwnerAuthority) BeginTransition(context.Context, uint64, uint64, string, int8,
	data.OwnerTargetView) (data.OwnerRecordView, error) {
	return data.OwnerRecordView{}, nil
}

func (r *releaseRecordingOwnerAuthority) Admit(context.Context, uint64, uint64, string,
	data.OwnerTargetView) (int64, error) {
	return 0, nil
}

func (r *releaseRecordingOwnerAuthority) ReleaseOwner(_ context.Context, playerID, _ uint64, _ string) error {
	r.released = append(r.released, playerID)
	return nil
}

// legacy 正常结算回收必须释放 owner,否则玩家打完副本回不了大厅
// (owner 恒指向已销毁 DS,客户端判 incomplete owner identity 直到 30s 超时)。
func TestLegacyReleaseBattle_ReleasesOwner(t *testing.T) {
	const matchID uint64 = 92001
	uc, _, pod := legacyOwnerFixture(t, matchID, []uint64{3001})
	auth := &releaseRecordingOwnerAuthority{pod: pod, uid: "uid-legacy-battle"}
	uc.SetOwnerAuthority(auth)

	if err := uc.ReleaseBattle(context.Background(), matchID, "settled"); err != nil {
		t.Fatalf("ReleaseBattle err: %v", err)
	}
	if len(auth.released) != 1 || auth.released[0] != 3001 {
		t.Fatalf("正常结算必须释放本局玩家的 owner 归属, got %v", auth.released)
	}
}

// 归属已指向别的实例时不得误删(exact 身份门)。
func TestLegacyReleaseBattle_SkipsOwnerPointingElsewhere(t *testing.T) {
	const matchID uint64 = 92002
	uc, _, pod := legacyOwnerFixture(t, matchID, []uint64{3002})
	// owner 记录指向另一台实例:本局回收不得动它。
	auth := &releaseRecordingOwnerAuthority{pod: pod + "-other", uid: "uid-somewhere-else"}
	uc.SetOwnerAuthority(auth)

	if err := uc.ReleaseBattle(context.Background(), matchID, "settled"); err != nil {
		t.Fatalf("ReleaseBattle err: %v", err)
	}
	if len(auth.released) != 0 {
		t.Fatalf("归属指向别处时不得释放, got %v", auth.released)
	}
}

// 终态心跳(对局已 ended)必须释放 owner —— 这是 legacy 正常结算的**真实**收口点。
//
// 教训:上一版把释放接在 ReleaseBattle 上,单测同样全绿,但真机计数显示
// ReleaseBattle 在这条流程里一次都不会被调用,玩家照样回不了大厅。
// 本测试直接驱动「记录已 ended → 再来一跳心跳」这条真实路径,不测那个不会被走到的函数。
// 2026-08-24 改判:owner 释放上移到「首次迁入 ended 的那一跳」。
//
// 本用例此前断言「第一跳不释放、第二跳才释放」,依据是 allocator.go 里那句
// 「DS 转 ended 并继续上报心跳」—— 该前提**不成立**。UE DS 侧:`state="ended"` 一局只由
// SendEndedHeartbeatAndReturnToHub 发一次;它的 ACK 回调 HandleEndedHeartbeatComplete 在
// 毫秒级里就 StopBattleHeartbeat();而周期心跳的 state 恒为 "running"
// (SetBattleHeartbeatState 唯一调用点恒传 "running")、tick 5s。⇒ **第二跳物理上不可能到达。**
//
// 于是按旧断言写出来的实现,在生产上等价于「永不释放」:owner 停在 BATTLE 指向一台已结算的
// DS,客户端连查 3 拍拿不到完整 TARGET → authority_entry_terminal → 打完一局被踢回登录
// (2026-08-24 实测,两个客户端同时复现)。本文件 sweepOnce 的 ended 分支注释早就写着
// 「无第二跳」,两处注释一直互相矛盾 —— sweep 那条才是对的。
//
// ★ 变异:把释放挪回 errHeartbeatTerminal 分支 → 第一段断言红。
func TestLegacyTerminalHeartbeat_ReleasesOwner(t *testing.T) {
	const matchID uint64 = 93001
	uc, _, pod := legacyOwnerFixture(t, matchID, []uint64{4001})
	ctx := context.Background()
	auth := &releaseRecordingOwnerAuthority{pod: pod, uid: "uid-legacy-battle"}
	uc.SetOwnerAuthority(auth)

	// 首次迁入 ended 的那一跳:当场释放 owner,且**不**下 stop ——
	// DS 正是靠这跳的成功响应确认 ended ACK,回 stop 会改动握手语义
	// (且 UE 侧 ended 那跳的回调根本不读 Result.Command,回了也是静默丢弃)。
	res, err := uc.Heartbeat(ctx, matchID, pod, 1, "ended", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("ended 心跳 err: %v", err)
	}
	if res.Command == commandStop {
		t.Fatal("首次 ended 心跳不得下发 stop:DS 靠这跳的成功响应确认 ended ACK")
	}
	if len(auth.released) != 1 || auth.released[0] != 4001 {
		t.Fatalf("首次迁入 ended 的那一跳必须释放本局玩家 owner 归属, got %v", auth.released)
	}

	// 第二跳在真实链路上不会到达(见上),但补偿重试 / 孤儿 DS 仍可能撞进来:
	// 此时走终态分支 → stop + kill,释放幂等重复一次不误伤。
	res2, err := uc.Heartbeat(ctx, matchID, pod, 1, "ended", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("终态心跳 err: %v", err)
	}
	if res2.Command != commandStop {
		t.Fatalf("终态心跳应下发 stop, got %q", res2.Command)
	}
}

// 终态心跳同样受 exact 身份门保护:归属指向别处时不得误删。
func TestLegacyTerminalHeartbeat_SkipsOwnerPointingElsewhere(t *testing.T) {
	const matchID uint64 = 93002
	uc, _, pod := legacyOwnerFixture(t, matchID, []uint64{4002})
	ctx := context.Background()
	auth := &releaseRecordingOwnerAuthority{pod: pod + "-other", uid: "uid-somewhere-else"}
	uc.SetOwnerAuthority(auth)

	for i := 0; i < 2; i++ {
		if _, err := uc.Heartbeat(ctx, matchID, pod, 1, "ended", time.Now().UnixMilli()); err != nil {
			t.Fatalf("心跳 err: %v", err)
		}
	}
	if len(auth.released) != 0 {
		t.Fatalf("归属指向别处时不得释放, got %v", auth.released)
	}
}
