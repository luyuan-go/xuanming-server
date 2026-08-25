// Package biz 是 ds_allocator 服务的业务逻辑层(W4 ②,2026-06-06)。
//
// 职责(docs/design/go-services.md §2.11):战斗 DS 调度。
//   - AllocateBattle:matchmaker 全员确认后调,申请战斗 DS pod → 写 Redis 镜像 → 回 ds_addr
//   - ReleaseBattle:对局结束/异常,回收 DS pod + 删镜像
//   - Heartbeat:DS 每 5s 主动上报(单向 unary,架构决策 2026-06-03),刷新 last_heartbeat_ms
//   - ListBattles:运维/调试查询当前战斗实例
//   - RunHeartbeatSweep:后台扫描 active ZSET,15s 没心跳 → 标记 abandoned + 回收(不变量 §4)
//   - 空场兜底:Heartbeat 内检测对局活跃但 player_count==0 持续超 EmptyBattleTimeout →
//     标记 abandoned + 回收(全员掉线未归/客户端从未连入,防 DS 空转;2026-07-06)
//
// 关键不变量:
//   - AllocateBattle 幂等(同 match_id 已有镜像 → 直接回已分配地址,不重复 Allocate)
//   - 心跳超时 → abandoned + 发 ds.lifecycle 补偿事件;投递成功才移出 active,
//     失败保留在 active 下一轮重试(W4 ⑧ 可靠补偿,不变量 §4)
package biz

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/battleabort"
	"github.com/luyuancpp/pandora/pkg/dsmetadata"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/placement"
	"github.com/luyuancpp/pandora/pkg/releasetrack"
	"github.com/luyuancpp/pandora/pkg/safego"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// errHeartbeatTerminal 是 Heartbeat 在镜像已是终态(ended/abandoned)时,
// 从乐观锁回调里返回的哨兵错误:中止写回(不刷新 LastHeartbeatMs / TTL / active score),
// 由 Heartbeat 捕获后转成 stop 指令。保证 abandoned 后 DS 继续心跳不会推迟补偿重试、
// 不会刷新 BattleTTL 上界(W4 ⑧ Codex 复审 P1)。
var errHeartbeatTerminal = errors.New("heartbeat on terminal battle")

// errHeartbeatPodMismatch:Heartbeat 上报的 DsPodName 与镜像里记录的不一致(旧 DS / 孤儿 DS /
// 重分配后残留的上一个 pod)。从乐观锁回调返回此哨兵 → 不写回该镜像,并令上报方停机,
// 避免污染新对局的状态(LastHeartbeatMs / state / player_count)。
var errHeartbeatPodMismatch = errors.New("heartbeat pod mismatch")

// errHeartbeatAllocationFenced 表示该 match 正处于 GSA 结果未知或外部 release
// 尚未确认的永久墓碑。即使同版本副本仍跑 legacy 配置，也必须零写入返回 stop，
// 不能让旧 DS 心跳把 state 改回 running 并恢复 TTL。
var errHeartbeatAllocationFenced = errors.New("heartbeat on fenced allocation")

// errReadyWaitTimeout:AllocateBattle 等待 DS ready 心跳超时的哨兵,由 waitBattleReady 返回,
// 调用方据此走回收 pod + 删镜像 + 返回 ErrDSAllocationFailed 的清理路径。
var errReadyWaitTimeout = errors.New("ready wait timeout")

// errBattleWaitOwnershipLost:wait 提前终止且该分配的回收/接管所有权已属他人
// (sweep 已判弃或已 purge、allocation 被新分配取代、权威不可授权)。owner 见到本哨兵
// **不得**再执行 cleanupAllocatedBattle——sweep 的 fenced 回收链可能正在途,并发第二路
// FencePreactiveRelease/ReleaseExpected 只有幂等最终一致保证,没有单次调用保证
// (复审必修:回收所有权必须可识别)。随 errcode.NewCause 附于 ErrDSAllocationFailed。
var errBattleWaitOwnershipLost = errors.New("battle wait ownership lost")

// 战斗 DS 状态常量(对应 proto string state 字段)。
const (
	stateAllocating            = "allocating"
	stateAllocationUncertain   = data.BattleStateAllocationUncertain
	stateAllocationReconciling = data.BattleStateAllocationReconcileReleasePending
	stateAllocationEmptyFence  = data.BattleStateAllocationReconcileEmptyTombstone
	statePreactiveReleasing    = data.BattleStatePreactiveReleasePending
	stateAllocationAbort       = data.BattleStateAllocationAbortPending
	stateWarming               = "warming"
	stateReady                 = "ready"
	stateRunning               = "running"
	stateEnded                 = "ended"
	stateAbandoned             = "abandoned"
)

// Heartbeat 响应控制指令常量。
const (
	commandNone = ""
	commandStop = "stop" // 通知孤儿 DS(无对应镜像)自行停机
)

// 乐观锁重试次数(心跳/状态更新冲突)。
const updateMaxRetry = 3

// readyPollInterval 是 AllocateBattle 等待 DS ready 心跳时轮询 Redis 镜像的间隔。
// 1s 足够:DS 心跳 5s 一跳,ready 等待窗口 10s,1s 轮询既不漏判也不给 Redis 添压。
// 用 var 而非 const,便于单测把它调小以避免慢测(见 allocator_test.go init)。
var readyPollInterval = 1 * time.Second

// activeIndexReconcileInterval bounds the cost of rebuilding the derived
// active ZSET from canonical battle records. The first sweep always runs it;
// later sweeps repeat it so a standalone ZSET write loss cannot strand a
// permanent recovery tombstone forever.
var activeIndexReconcileInterval = 30 * time.Second

// detachedCleanupTimeout 是 ready 等待失败后回收 pod + 删镜像的独立 ctx 预算。
// ready 等待失败的常见原因正是入站 ctx 被取消/超时,复用它做 Release/DeleteBattle 会立刻
// 失败,留下 warming 镜像 + 已分配 pod 泄漏;故清理用一个与入站 ctx 解耦的短超时 ctx。
const detachedCleanupTimeout = 5 * time.Second

// locationRefreshTimeout 是心跳后异步续期玩家 BATTLE 位置的独立 ctx 预算(断线重连,弱依赖)。
// 短超时防 locator 卡死时后台续期 goroutine 泄漏。
const locationRefreshTimeout = 3 * time.Second

// DSLifecyclePusher 发 pandora.ds.lifecycle 事件(W4 ③,2026-06-06)。
//
// 心跳超时标记 abandoned 后,由它把 DSLifecycleEvent{phase=ABANDONED} 发给 battle_result
// 做玩家段位回滚补偿(不变量 §4 DS 崩溃必有补偿)。
//
// W4 ⑧:投递失败不再静默丢——sweepOnce 把对局保留在 active ZSET,下一轮 sweep 重试,
// 直到投递成功或镜像 TTL 过期;配合 battle_result 幂等消费构成 at-least-once 闭环。
// 实现可在内部失败时返回 error(由 sweepOnce 触发重试)。
type DSLifecyclePusher interface {
	PublishLifecycle(ctx context.Context, evt *dsv1.DSLifecycleEvent) error
}

// LocationRefresher 续期玩家 BATTLE 位置 TTL(断线重连,docs/design/battle-reconnect.md §2.2)。
//
// 心跳成功且对局处于 ready/running 时,ds_allocator 用它把该对局玩家的位置刷新为 BATTLE
// (同 match_id 续期,BATTLE→BATTLE),使玩家整局在线期间 login 都能检测到"在战斗中",
// 从而支持中途掉线重登直连回原 battle DS。
//
// 由 player_locator gRPC 客户端实现;可为 nil(未配 locator_addr → 不续期,弱依赖,
// 不影响心跳 / 对局,仅长对局中途重登可能因位置过期退化为回大厅)。
type LocationRefresher interface {
	RefreshBattleLocations(ctx context.Context, playerIDs []uint64, matchID uint64, dsAddr string) error
}

// AllocatorUsecase 是 ds_allocator 业务逻辑核心。
type AllocatorUsecase struct {
	repo      data.BattleRepo
	alloc     GameServerAllocator
	cfg       conf.AllocatorConf
	lifecycle DSLifecyclePusher // 可为 nil；仅显式 local/off 开发配置允许 best-effort 降级
	// lifecycleRequired 在 Redis authority / 生产 enforce 路径为 true。即便启动装配
	// 被未来改坏，nil publisher 也只能保留 active outbox 重试，绝不能把 abandoned
	// 当作已恢复并 Expire 掉 Battle fence。
	lifecycleRequired bool

	// noShowRecorder no-show 记账 → 进入侧退避(anti-abuse §6 第 8 项)。可为 nil(不记罚)。
	noShowRecorder NoShowRecorder

	// ownerLease / ownerLeaseRequired:owner 权威实例租约双写(owner-authority.md
	// migrate ⑥;nil = 未启用;required 语义见 biz/owner_lease.go renewOwnerLeaseGate)。
	ownerLease         OwnerLeaseRenewer
	ownerLeaseRequired bool

	// ownerAuth / ownerAdmitted:owner 迁移弱依赖调用面 + census 已准入缓存
	// (owner-authority.md migrate ②/③;见 biz/owner_authority.go)。
	ownerAuth     OwnerAuthority
	ownerAdmitted sync.Map
	locator       LocationRefresher // 可为 nil(未配 locator_addr 时不续期 BATTLE 位置)

	// Model B 仅在 agones+enforce+authority_mode=redis 时由 main 注入。Redis authRepo 是
	// 唯一授权权威；K8s annotation 只投递 pending 凭据。
	authRepo                 data.BattleAuthRepo
	abortRepo                data.BattleAllocationAbortRepo
	lifecycleProofRepo       data.BattleAllocationLifecycleRepo
	authoritativeAlloc       AuthoritativeGameServerAllocator
	dsSigner                 BattleCredentialSigner
	dsCredentialTTL          time.Duration
	modelB                   bool
	releasePolicy            releasetrack.Policy
	activeIndexReconciler    data.BattleActiveIndexReconciler
	lastActiveIndexReconcile time.Time

	// sweepDeferUntil 是 sweep 队头公平性的**进程内**退避表(INC-20260724-001)。
	// key=match_id,value=(登记退避时观察到的 state, 可再次尝试的最早时刻)。
	// 只有 sweepOnce 读写,而 sweepOnce 只由 RunHeartbeatSweep 单协程驱动
	// (与 lastActiveIndexReconcile 同一并发域),故不加锁。
	//
	// 它是**纯调度提示、非权威**:不参与准入 / 归属 / 扣减任何决策,丢失只会让某项早一轮
	// 被重试(方向安全),因此不构成 §9.22 意义上的影子状态。权威仍是 active ZSET。
	// 为什么不写进 ZSET score,见 sweepOnce 内的三条硬约束注释。
	sweepDeferUntil map[uint64]sweepDeferral

	// sweepLease:心跳扫描循环的领导权来源(nil = 未启用继任租约,本副本无条件扫描)。
	// 只在启动装配期写入,之后只读。作用域**仅限 sweepOnce**,不参与任何 RPC 写路径 ——
	// 见 SetSweepWriterLease 的边界说明。
	sweepLease SweepWriterLease
	// sweepLeaseHeld:上一轮观察到的领导权,只由 RunHeartbeatSweep 单协程读写,
	// 用于把"接任 / 让位"打成状态跃迁日志而不是每 tick 刷屏。
	sweepLeaseHeld bool

	// killOrphanOnStop:心跳判定某 DS 该停机(orphan / pod_mismatch / 终态)时,是否由后端主动
	// 回收该 pod。local 模式打开——本机 UE DS 没有 Agones,收到 stop 指令不会自杀,残留进程会
	// 幽灵般占着监听端口污染下一局;打开后 stop 时异步调 alloc.Release kill 掉它。Agones 模式默认
	// 关闭:孤儿 GameServer 由 Agones 生命周期回收,避免 Redis 抖动误判 orphan 时误删正常 pod。
	killOrphanOnStop bool

	// 孤儿 Allocated GameServer 对账清扫(biz/orphan_gameserver.go;2026-08-03)。
	// orphanGSReconciler 由构造期对 alloc 的接口断言注入(仅 Agones 实现);
	// allocationLedger 由构造期对 repo 的接口断言注入(权威台账,防误删④的
	// 「本权威出身证明」;二者任一缺失则清扫整体禁用)。
	// 其余两个字段与 sweepDeferUntil 同一并发域(仅 sweepOnce 单协程读写,不加锁),
	// 且同为进程内非权威调度提示:丢失只是把回收推迟一个观察期,方向安全。
	orphanGSReconciler    OrphanGameServerReconciler
	allocationLedger      BattleAllocationLedger
	lastOrphanGSReconcile time.Time
	orphanGSFirstSeen     map[string]time.Time
}

// BattleCredentialSigner 把 Allocator 可见能力收窄为 DS callback 凭据签发；生产实现是
// *auth.DSCallbackSigner，无法从该字段调用玩家 Session/DSTicket 签发方法。
type BattleCredentialSigner interface {
	SignBattleCredential(matchID uint64, pod, instanceUID string, epoch uint32, gen uint64, jti string, ttl time.Duration) (auth.HubCredentialResult, error)
}

// NewAllocatorUsecase 构造 AllocatorUsecase。
func NewAllocatorUsecase(repo data.BattleRepo, alloc GameServerAllocator, cfg conf.AllocatorConf) *AllocatorUsecase {
	u := &AllocatorUsecase{repo: repo, alloc: alloc, cfg: cfg}
	if reconciler, ok := repo.(data.BattleActiveIndexReconciler); ok {
		u.activeIndexReconciler = reconciler
	}
	if orphan, ok := alloc.(OrphanGameServerReconciler); ok {
		u.orphanGSReconciler = orphan // 仅 Agones 分配器实现;local/mock 自动禁用
	}
	if ledger, ok := repo.(BattleAllocationLedger); ok {
		u.allocationLedger = ledger // 孤儿清扫的权威台账(防误删④);无台账则清扫禁用
	}
	return u
}

// SetLifecyclePusher 注入 ds.lifecycle 事件发送器(main 在 Kafka 就绪时调用)。
func (u *AllocatorUsecase) SetLifecyclePusher(p DSLifecyclePusher) { u.lifecycle = p }

// SetLifecyclePusherRequired 把生产发布策略注入业务层。Redis authority 在
// EnableRedisAuthority 内还会无条件打开此门，避免调用方漏配。
func (u *AllocatorUsecase) SetLifecyclePusherRequired(required bool) {
	u.lifecycleRequired = required
}

// ValidateLifecyclePusherReady 是启动装配的最后一道门。配置宣称可靠发布时，只有
// 非 nil publisher 才允许启动 sweep/RPC。
func (u *AllocatorUsecase) ValidateLifecyclePusherReady() error {
	if u.lifecycleRequired && u.lifecycle == nil {
		return errcode.New(errcode.ErrInvalidState,
			"reliable ds.lifecycle publisher is required before allocator startup")
	}
	return nil
}

// SetLocationRefresher 注入 BATTLE 位置续期器(main 在 locator_addr 已配时调用,弱依赖)。
func (u *AllocatorUsecase) SetLocationRefresher(r LocationRefresher) { u.locator = r }

// NoShowRecorder no-show 记账 → 进入侧退避的写者(实现:data.RedisNoShowRecorder)。
// 背压非权威门:错误只 Warn,绝不阻断判弃收尾(anti-abuse §2 铁律)。
type NoShowRecorder interface {
	// RecordNoShow 记一次 no-show,返回窗口内累计次数(含本次)。
	RecordNoShow(ctx context.Context, playerID uint64, window time.Duration) (int64, error)
	// ArmPenalty 布设进入侧退避窗(matchmaker StartMatch 读取执行)。
	ArmPenalty(ctx context.Context, playerID uint64, d time.Duration) error
}

// SetNoShowRecorder 注入 no-show 记罚器(可选;不注入 = 不记罚,dev 联调兼容)。
func (u *AllocatorUsecase) SetNoShowRecorder(r NoShowRecorder) { u.noShowRecorder = r }

// SetReleaseTrackPolicy 在启动期注入 match 级确定性 cohort 策略。
func (u *AllocatorUsecase) SetReleaseTrackPolicy(p releasetrack.Policy) { u.releasePolicy = p }

// EnableRedisAuthority 打开 Battle Model B。依赖必须一次完整注入；任何缺失都拒绝启动，
// 禁止出现“配置说 redis authority、实际悄悄回退 legacy”的半开启状态。
func (u *AllocatorUsecase) EnableRedisAuthority(
	repo data.BattleAuthRepo,
	signer BattleCredentialSigner,
	tokenTTL time.Duration,
) error {
	a, ok := u.alloc.(AuthoritativeGameServerAllocator)
	abortRepo, abortOK := repo.(data.BattleAllocationAbortRepo)
	lifecycleProofRepo, lifecycleProofOK := repo.(data.BattleAllocationLifecycleRepo)
	battleStrictWriter, battleStrictOK := u.repo.(data.StrictModelBBattleStorage)
	authStrictWriter, authStrictOK := repo.(data.StrictModelBBattleStorage)
	if repo == nil || signer == nil || tokenTTL <= 0 || !ok || !abortOK || !lifecycleProofOK ||
		u.activeIndexReconciler == nil || !battleStrictOK || !authStrictOK {
		return errcode.New(errcode.ErrInvalidState,
			"battle Model B requires auth/abort/lifecycle repo, signer, positive ttl, authoritative Agones allocator, canonical active-index reconciler and strict storage writers")
	}
	// Irreversible and deliberately last among capability checks: after the
	// startup all-master preflight succeeds, no Model-B RPC/worker can become
	// visible until both battle-writing repository views enforce the same
	// continuous storage invariant.
	battleStrictWriter.EnableStrictModelBWrites()
	authStrictWriter.EnableStrictModelBWrites()
	if !battleStrictWriter.StrictModelBWritesEnabled() || !authStrictWriter.StrictModelBWritesEnabled() {
		return errcode.New(errcode.ErrInvalidState,
			"battle Model B strict storage write gate did not activate")
	}
	u.authRepo = repo
	u.abortRepo = abortRepo
	u.lifecycleProofRepo = lifecycleProofRepo
	u.authoritativeAlloc = a
	u.dsSigner = signer
	u.dsCredentialTTL = tokenTTL
	u.modelB = true
	u.lifecycleRequired = true
	return nil
}

// SetKillOrphanOnStop 打开「心跳 stop 时主动回收该 DS」(main 在 mode=local 时调用)。
// 见 killOrphanOnStop 字段说明:local 模式的 UE DS 收到 stop 不自杀,需后端主动 kill 防幽灵占端口。
func (u *AllocatorUsecase) SetKillOrphanOnStop(v bool) { u.killOrphanOnStop = v }

// killStrandedDS 在心跳判定某 DS 该停机时,异步回收其 pod(local 模式防幽灵 DS 占端口)。
// fire-and-forget:请求 ctx 不得逃逸进 goroutine(§16.7),用 plog.Detach(只复制 trace_id
// 等日志字段,满足不变量 §8)+ 短超时,不给心跳响应加尾延迟。
// killOrphanOnStop 关闭(Agones 模式)时为 no-op,pod 回收交 Agones 生命周期。
func (u *AllocatorUsecase) killStrandedDS(ctx context.Context, matchID uint64, podName, reason string) {
	if !u.killOrphanOnStop || podName == "" {
		return
	}
	go func() {
		kctx, cancel := context.WithTimeout(plog.Detach(ctx), detachedCleanupTimeout)
		defer cancel()
		if err := u.alloc.Release(kctx, podName); err != nil {
			plog.With(kctx).Warnw("msg", "kill_stranded_ds_failed", "match_id", matchID, "pod", podName, "reason", reason, "err", err)
			return
		}
		plog.With(kctx).Infow("msg", "kill_stranded_ds", "match_id", matchID, "pod", podName, "reason", reason)
	}()
}

func (u *AllocatorUsecase) battleTTL() time.Duration { return u.cfg.BattleTTL.Std() }

// readyWaitTimeout 是 AllocateBattle 等待 DS ready 心跳的最长时间(默认 10s)。
func (u *AllocatorUsecase) readyWaitTimeout() time.Duration { return u.cfg.ReadyWaitTimeout.Std() }

// 分配 / 就绪等待 / 心跳 / 回收链路的拒绝 reason 枚举(infra.md §11.3 R2)。
//
// 全部取自**已经存在的判定分支**,一个分支一个常量:凡一个 if 里收敛了多个条件的,
// 都按条件拆开。"进不去副本"的三类根因(没有可用 GameServer / Agones 分配失败 /
// roster 没到齐被判弃)必须能靠 reason 直接分开统计,不能只看到一句"分配失败"。
//
// snake_case 常量,稳定不变(日志系统按 msg+reason 聚合)。
const (
	// AllocateBattle 入参与 claim 阶段。
	reasonAllocMatchIDRequired = "match_id_required"
	reasonAllocRosterInvalid   = "roster_invalid"
	reasonAllocFactionsInvalid = "combat_factions_invalid"
	reasonAllocClaimFailed     = "claim_write_failed"
	reasonAllocTrackInvalid    = "release_track_invalid"
	reasonAllocFinalizeLost    = "finalize_claim_lost"
	// 幂等重试 / claim 输家(awaitExistingAllocation)。
	reasonAwaitClaimMissing       = "claim_record_missing"
	reasonAwaitReleaseUnconfirmed = "preactive_release_unconfirmed"
	// waitBattleReady 的提前失败(每条对应一个既有 return,不合并)。
	reasonWaitAuthorityPurged   = "authority_purged"
	reasonWaitAllocSuperseded   = "allocation_superseded"
	reasonWaitAuthOutsideGrace  = "auth_missing_outside_grace"
	reasonWaitAuthProvisionDead = "auth_provision_stalled"
	reasonWaitStateReclaimed    = "state_reclaimed"
	reasonWaitAuthPhaseFenced   = "auth_phase_fenced"
	reasonWaitRecordGone        = "battle_record_gone"
	reasonWaitCtxDone           = "caller_ctx_done"
	// 心跳链路。
	reasonHeartbeatModelBOff          = "redis_authority_disabled"
	reasonHeartbeatCensusInconsistent = "player_count_exceeds_census"
	reasonHeartbeatAuthorityRead      = "authority_read_failed"
	reasonHeartbeatActivateRejected   = "activate_rejected"
	reasonHeartbeatOwnerLease         = "owner_lease_renew_failed"
	reasonHeartbeatDepartureReconcile = "departure_reconcile_failed"
	reasonHeartbeatUpdateFailed       = "battle_update_failed"
	// 心跳 stop 指令(Model B),按判定依据分档。
	reasonStopTerminalAuth    = "auth_terminating"
	reasonStopBattleMissing   = "battle_record_missing"
	reasonStopBattleEnded     = "battle_ended"
	reasonStopBattleAbandoned = "battle_abandoned"
	// 回收 / 释放。
	reasonReleaseTupleIncomplete = "expected_tuple_incomplete"
	reasonReleasePodUIDMissing   = "expected_pod_uid_missing"
	// 重连重签的只读权威查询(ResolveBattleTarget)。
	reasonResolveArgsInvalid          = "resolve_args_invalid"
	reasonResolveAuthorityOff         = "read_only_authority_unavailable"
	reasonResolveAuthorityRead        = "authority_read_failed"
	reasonResolveNotAuthorized        = "target_not_authorized"
	reasonResolveProjectionIncomplete = "target_projection_incomplete"
	// ReleaseBattle / ReleaseBattleExpected / FinalizeBattleReleaseExpected。
	reasonReleaseModelBUnsupported = "model_b_requires_expected_tuple"
	reasonReleaseFenced            = "allocation_fence_requires_reconcile"
	reasonReleaseAuthorityOff      = "redis_authority_required"
	reasonReleaseProofIncomplete   = "terminal_proof_incomplete"
	reasonReleaseIdentityChanged   = "stable_identity_changed"
	reasonReleaseAuthorityReread   = "terminal_authority_reread_failed"
	reasonReleaseSnapshotStale     = "terminal_snapshot_incomplete"
	reasonFinalizeProofIncomplete  = "finalize_proof_incomplete"
	reasonFinalizeTombstonePending = "terminal_tombstone_not_confirmed"
	// AbortPreactiveBattle(matchmaker 分配 saga 补偿)。
	reasonAbortRequestIncomplete    = "abort_request_incomplete"
	reasonAbortAuthorityOff         = "abort_requires_redis_authority"
	reasonAbortPreflightFailed      = "abort_preflight_failed"
	reasonAbortFenceFailed          = "abort_fence_failed"
	reasonAbortAckCleanupPending    = "abort_ack_cleanup_pending"
	reasonAbortFenceIdentityMissing = "abort_fence_pod_authority_missing"
	reasonAbortLifecyclePending     = "abort_lifecycle_publish_pending"
	reasonAbortLifecycleMarker      = "abort_lifecycle_marker_pending"
	reasonAbortCompletionPending    = "abort_completion_pending"
	// Pod UID 持久化前置(所有精确回收的共同前提)。
	reasonPodUIDAuthorityRead   = "pod_uid_preflight_authority_read_failed"
	reasonPodUIDIdentityChanged = "pod_uid_preflight_identity_changed"
	reasonPodUIDResolveFailed   = "pod_uid_resolve_failed"
	reasonPodUIDBackfillFailed  = "pod_uid_backfill_failed"
	reasonPodUIDVerifyFailed    = "pod_uid_backfill_verify_failed"
	// 派生 active 索引重建。
	reasonActiveIndexUnavailable = "active_index_reconciler_unavailable"
	reasonActiveIndexRebuild     = "active_index_rebuild_failed"
)

// ── RPC 1:AllocateBattle ──────────────────────────────────────────────────────

// AllocateResult 是 AllocateBattle 的出参。
//
// GameserverUID / InstanceEpoch / AllocationID(DSTicket v2,方案 B):matchmaker 签 battle 票
// 时把票绑死到唯一 DS 实例。三者与 DSAddr 来自同一权威快照(Redis BattleStorageRecord),
// 不得从其它时点/其它源拼凑,否则地址与票据可能指向不同实例。
type AllocateResult struct {
	DSAddr        string
	DSPodName     string
	AllocatedAtMs int64
	GameserverUID string
	InstanceEpoch uint32
	AllocationID  string
	ReleaseTrack  string
}

func allocateResultFromBattle(b *dsv1.BattleStorageRecord) *AllocateResult {
	if b == nil {
		return nil
	}
	return &AllocateResult{
		DSAddr: b.GetDsAddr(), DSPodName: b.GetDsPodName(), AllocatedAtMs: b.GetAllocatedAtMs(),
		GameserverUID: b.GetGameserverUid(), InstanceEpoch: b.GetInstanceEpoch(),
		AllocationID: b.GetAllocationId(), ReleaseTrack: b.GetReleaseTrack(),
	}
}

func ownerTargetFromAllocateResult(res *AllocateResult) data.OwnerTargetView {
	if res == nil {
		return data.OwnerTargetView{}
	}
	return data.OwnerTargetView{
		PodName:                  res.DSPodName,
		InstanceUID:              res.GameserverUID,
		InstanceEpoch:            res.InstanceEpoch,
		AssignmentOrAllocationID: res.AllocationID,
		ReleaseTrack:             res.ReleaseTrack,
	}
}

// ResolveBattleTarget 是重连重签的只读权威查询。它只读同一 Redis auth+projection
// 快照，不创建 allocation claim、不调用 Agones、不刷新 TTL/heartbeat/index。
func (u *AllocatorUsecase) ResolveBattleTarget(
	ctx context.Context,
	matchID, playerID uint64,
) (*AllocateResult, error) {
	if matchID == 0 || playerID == 0 {
		plog.With(ctx).Warnw("msg", "battle_target_refused", "reason", reasonResolveArgsInvalid,
			"match_id", matchID, "player_id", playerID)
		return nil, errcode.New(errcode.ErrInvalidArg, "match_id and player_id required")
	}
	if u == nil || !u.modelB || u.authRepo == nil {
		// 这条以前完全静默:重连拿不到目标 → 玩家被退化成回大厅,而后端连"是因为
		// 本副本没开 Redis 权威"都看不出来(legacy 副本在滚动共存期就会走到这里)。
		plog.With(ctx).Warnw("msg", "battle_target_refused", "reason", reasonResolveAuthorityOff,
			"match_id", matchID, "player_id", playerID,
			"hint", "本副本未启用 Redis 权威(legacy/滚动共存),只读重连查询不可用")
		return nil, errcode.New(errcode.ErrUnavailable, "battle read-only authority unavailable")
	}
	snapshot, err := u.authRepo.ReadAuthority(ctx, matchID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "battle_target_refused", "reason", reasonResolveAuthorityRead,
			"match_id", matchID, "player_id", playerID, "err", err,
			"hint", "权威读失败按 UNKNOWN fail-closed(§9.22),客户端应退避重查而非默认回大厅")
		return nil, err
	}
	ready, reason := snapshot.ReadyAuthorized(
		time.Now().UnixMilli(), u.cfg.HeartbeatTimeout.Std().Milliseconds())
	battle := snapshot.Battle
	if !ready || battle == nil || !slices.Contains(battle.GetPlayerIds(), playerID) {
		// ReadyAuthorized 内部把近十条判据收敛成一个字符串 reason,这里必须原样带出来
		// (auth-missing / phase-not-active / heartbeat-stale / projection-mismatch …),
		// 否则"玩家重连被拒"只剩一个 PermissionDeny,分不清是对局没就绪还是人不在名单里。
		inRoster := battle != nil && slices.Contains(battle.GetPlayerIds(), playerID)
		plog.With(ctx).Warnw("msg", "battle_target_refused", "reason", reasonResolveNotAuthorized,
			"match_id", matchID, "player_id", playerID, "ready", ready,
			"ready_reason", reason, "in_roster", inRoster, "state", battle.GetState(),
			"pod", battle.GetDsPodName(), "allocation_id", battle.GetAllocationId(),
			"hint", "ready_reason 是 ReadyAuthorized 的具体判据;in_roster=false 说明该玩家不属这局")
		return nil, errcode.New(errcode.ErrPermissionDeny,
			"battle target not authorized for reconnect (reason=%s)", reason)
	}
	if battle.GetDsAddr() == "" || battle.GetDsPodName() == "" || battle.GetGameserverUid() == "" ||
		battle.GetInstanceEpoch() == 0 || battle.GetAllocationId() == "" ||
		(battle.GetReleaseTrack() != auth.ReleaseTrackStable && battle.GetReleaseTrack() != auth.ReleaseTrackCanary) {
		// 逐字段带出"哪一项缺了":重签 v2 战斗票要求 exact 实例绑定齐全,缺任一项
		// matchmaker/login 都签不出票,玩家表现为"重连一直失败"。
		plog.With(ctx).Errorw("msg", "battle_target_refused", "reason", reasonResolveProjectionIncomplete,
			"match_id", matchID, "player_id", playerID, "ds_addr", battle.GetDsAddr(),
			"pod", battle.GetDsPodName(), "uid", battle.GetGameserverUid(),
			"epoch", battle.GetInstanceEpoch(), "allocation_id", battle.GetAllocationId(),
			"release_track", battle.GetReleaseTrack(),
			"hint", "投影已 ready 却缺 exact 实例绑定字段,重签票据不可能成功;查 finalize/凭据投递链")
		return nil, errcode.New(errcode.ErrUnavailable, "battle target projection incomplete")
	}
	// R1:重连重签是一次**路由判定结果**(玩家将被指回这台 exact 实例)。低频、每次重连
	// 至多一条,是"玩家为什么被送回某台 DS"的唯一后端证据。
	plog.With(ctx).Infow("msg", "battle_target_resolved", "match_id", matchID, "player_id", playerID,
		"pod", battle.GetDsPodName(), "ds_addr", battle.GetDsAddr(), "uid", battle.GetGameserverUid(),
		"epoch", battle.GetInstanceEpoch(), "allocation_id", battle.GetAllocationId(),
		"release_track", battle.GetReleaseTrack(), "state", battle.GetState())
	return allocateResultFromBattle(battle), nil
}

// AllocateBattle 为 match 申请战斗 DS。
//
// 关键:Agones Allocated(pod 被分配)≠ 战斗 DS Ready。DS 进程要先读到 pandora.dev/match-id
// 才能在 PreLogin 放行客户端票据。所以这里不再一拿到 pod 就回 ds_addr,而是:
//
//	Allocate → CreateBattle(state=warming) → 轮询等 DS Heartbeat 上报正确 match_id/pod 且
//	进入 ready/running → 回 ds_addr;ReadyWaitTimeout 内没等到 → 回收 pod + 删镜像 + 分配失败。
//
// 用 Redis 镜像轮询(而非内存 channel):Heartbeat RPC 可能落到另一个 ds_allocator pod,
// 只有共享的 Redis 镜像能跨 pod 观察到 DS 的就绪心跳。
//
// 幂等:同 match_id 已有镜像时——ready/running 且有有效心跳 → 直接回;warming → 继续等 ready;
// 终态/不可用 → 返回分配失败(绝不把 ds_addr 回给 matchmaker)。
// AllocateBattle 是不带阵营快照 / 不带计分模式的旧口径入口(dev / 单测 / 兼容路径)。
// ratingMode 留 UNSPECIFIED = battle_result 按旧口径结算,见 BattleStorageRecord.rating_mode。
func (u *AllocatorUsecase) AllocateBattle(ctx context.Context, matchID uint64, playerIDs []uint64, mapID uint32, gameMode string) (*AllocateResult, error) {
	return u.AllocateBattleWithCombatFactions(ctx, matchID, playerIDs, nil, mapID, gameMode,
		configpb.LevelRatingMode_LEVEL_RATING_MODE_UNSPECIFIED, "")
}

// AllocateBattleWithCombatFactions 为 match 申请战斗 DS，并持久化完整的 match-local
// 玩家阵营快照。空映射仅兼容滚动升级中的旧 matchmaker；非空映射必须精确覆盖 roster。
//
// ratingMode 是 matchmaker 成局时按 map_id 从关卡表定格的「本局算不算段位」。本服务
// **只原样存进 canonical BattleStorageRecord,不解释、不校验、不下发 DS**:判定权在
// battle_result(§9.6 数值不信 DS,派生数值只在结算服务算)。0=UNSPECIFIED 是合法值
// (旧 matchmaker / 旧批次表),结算侧按旧口径兜底(§9.21)。
func (u *AllocatorUsecase) AllocateBattleWithCombatFactions(
	ctx context.Context,
	matchID uint64,
	playerIDs []uint64,
	combatFactionByPlayer map[uint64]uint32,
	mapID uint32,
	gameMode string,
	ratingMode configpb.LevelRatingMode,
	ratingPool string,
) (*AllocateResult, error) {
	if matchID == 0 {
		plog.With(ctx).Warnw("msg", "battle_allocate_refused", "reason", reasonAllocMatchIDRequired)
		return nil, errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	canonicalPlayers, _, rosterErr := dsmetadata.CanonicalRoster(playerIDs)
	if rosterErr != nil {
		plog.With(ctx).Warnw("msg", "battle_allocate_refused", "reason", reasonAllocRosterInvalid,
			"match_id", matchID, "players", len(playerIDs), "err", rosterErr)
		return nil, errcode.New(errcode.ErrInvalidArg, "invalid battle roster: %v", rosterErr)
	}
	playerIDs = canonicalPlayers
	var combatFactions []*dsv1.BattlePlayerCombatFaction
	if len(combatFactionByPlayer) > 0 {
		canonicalFactionPlayers, _, factionErr := dsmetadata.CanonicalCombatFactions(
			playerIDs, combatFactionByPlayer)
		if factionErr != nil {
			plog.With(ctx).Warnw("msg", "battle_allocate_refused", "reason", reasonAllocFactionsInvalid,
				"match_id", matchID, "players", len(playerIDs),
				"factions", len(combatFactionByPlayer), "err", factionErr,
				"hint", "阵营快照与 roster 对不上;DS 侧 ResolveCampForSpawn 解不出权威阵营会 RejectSpawn")
			return nil, errcode.New(errcode.ErrInvalidArg, "invalid battle combat factions: %v", factionErr)
		}
		playerIDs = canonicalFactionPlayers
		combatFactions = combatFactionRecords(playerIDs, combatFactionByPlayer)
		// 脱离调用方 map，避免 RPC 返回前外部复用/修改 map 造成 TOCTOU 或数据竞争。
		combatFactionByPlayer = make(map[uint64]uint32, len(combatFactions))
		for _, record := range combatFactions {
			combatFactionByPlayer[record.GetPlayerId()] = record.GetCombatFactionId()
		}
	} else {
		plog.With(ctx).Warnw("msg", "battle_combat_factions_legacy_missing", "match_id", matchID)
	}
	desiredReleaseTrack := u.releasePolicy.Select(matchID)

	// 单 key SET NX claim 是并发 AllocateBattle 的线性化点。只有持有本次 allocation_id
	// 的赢家才允许调用外部 Agones；输家只观察同一权威记录，绝不再分配第二个 Pod。
	claimAt := time.Now().UnixMilli()
	allocationID := uuid.NewString()
	claim := &dsv1.BattleStorageRecord{
		MatchId:              matchID,
		State:                stateAllocating,
		PlayerIds:            append([]uint64(nil), playerIDs...),
		MapId:                mapID,
		GameMode:             gameMode,
		RatingMode:           ratingMode,
		RatingPool:           ratingPool,
		AllocatedAtMs:        claimAt,
		LastHeartbeatMs:      claimAt,
		PlayerCount:          int32(len(playerIDs)),
		AllocationId:         allocationID,
		PlayerCombatFactions: cloneCombatFactionRecords(combatFactions),
		// 分配时冻结到齐期限策略代(observe→enforce 激活协议的执行依据,proto 字段注释)。
		RosterPolicyGeneration: u.cfg.RosterPolicyGeneration,
	}
	claimed, existing, err := u.repo.ClaimBattle(ctx, claim, u.battleTTL())
	if err != nil {
		// claim 是整条分配链的第一步。这一步失败时此前一行日志都没有,matchmaker 只看到
		// 一个错误码 —— "分配不到 DS"到底是没到 Agones 还是 Agones 没货,查不出来。
		plog.With(ctx).Warnw("msg", "battle_allocate_refused", "reason", reasonAllocClaimFailed,
			"match_id", matchID, "allocation_id", allocationID, "players", len(playerIDs),
			"map_id", mapID, "err", err,
			"hint", "还没走到 Agones:Redis 权威写失败,与 Fleet 容量无关")
		return nil, err
	}
	if !claimed {
		if !sameBattleAllocationRequest(existing, claim) {
			plog.With(ctx).Errorw("msg", "battle_allocation_idempotency_snapshot_mismatch",
				"match_id", matchID, "allocation_id", existing.GetAllocationId())
			return nil, errcode.New(errcode.ErrUnavailable,
				"battle %d allocation request conflicts with existing snapshot", matchID)
		}
		return u.awaitExistingAllocation(ctx, matchID, existing)
	}
	// allocation 台账(孤儿清扫防误删④,data/allocation_ledger.go):claim 赢家在任何
	// 外部 GSA POST 前把 allocation_id 记入本权威台账;孤儿清扫只准回收「台账能证明
	// 出身于本权威」的 GS。写失败不阻断分配(可用性优先)——代价只是本次分配若泄漏,
	// 清扫因台账查无而保留不删,方向安全。
	if u.allocationLedger != nil {
		if lerr := u.allocationLedger.RecordAllocationLedger(ctx, allocationID, claimAt); lerr != nil {
			plog.With(ctx).Warnw("msg", "battle_allocation_ledger_record_failed",
				"match_id", matchID, "allocation_id", allocationID, "err", lerr)
		}
	}

	var authoritative *data.AuthoritativeGameServerAllocation
	var podName, addr string
	actualReleaseTrack := desiredReleaseTrack
	if u.modelB {
		// 外部 GSA POST 前先把 claim CAS 成永久 allocation_uncertain。该 fence 是
		// “是否允许 POST”的唯一线性化点；失败/响应未知时绝不能访问 K8s。
		fenced, fenceErr := u.repo.FenceBattleAllocation(ctx, matchID, allocationID)
		if fenceErr != nil || !fenced {
			plog.With(ctx).Errorw("msg", "gameserver_preallocation_fence_failed",
				"match_id", matchID, "allocation_id", allocationID,
				"fenced", fenced, "err", fenceErr)
			return nil, errcode.New(errcode.ErrUnavailable,
				"battle %d allocation fence unavailable", matchID)
		}
		authoritative, err = u.authoritativeAlloc.AllocateAuthoritative(
			ctx, matchID, allocationID, playerIDs, combatFactionByPlayer, mapID, gameMode, desiredReleaseTrack)
		if authoritative != nil {
			podName, addr = authoritative.PodName, authoritative.Addr
		}
	} else {
		// legacy 本地面:DS 进程在 Allocate 内 exec、env 那一刻定型,所以权威准入元数据
		// (roster/allocation-id/release-track/combat-factions)必须**先**登记。生产走 Agones
		// annotation,这里是它在 mode=local 的等价投递通道(见 localBattleRosterSink 注释)。
		// combatFactionByPlayer 已在上方经 dsmetadata.CanonicalCombatFactions 校验并重建为
		// 独立 map(非空时),此处原样交给 sink,由它再深拷贝一份存进台账。
		// 双重隔离:!u.modelB + 类型断言(Agones/Mock 不实现),生产恒不进。
		if sink, ok := u.alloc.(localBattleRosterSink); ok {
			sink.SetPendingBattleRoster(matchID, playerIDs, combatFactionByPlayer, allocationID, desiredReleaseTrack)
		}
		podName, addr, actualReleaseTrack, err = u.alloc.Allocate(ctx, matchID, mapID, gameMode, desiredReleaseTrack)
	}
	if err != nil {
		// R2:reason 必须能把"Fleet 没有空闲 GameServer"(要扩容)与"控制面调用失败"
		// (要查 k8s / 网络)分开——两者的处置完全不同,而以前它们共用一条日志。
		allocReason := "control_plane_failed"
		if errcode.As(err) == errcode.ErrDSNoAvailable {
			allocReason = "no_available_gameserver"
		}
		plog.With(ctx).Errorw("msg", "gameserver_allocate_failed", "match_id", matchID,
			"reason", allocReason, "allocation_id", allocationID, "map_id", mapID,
			"game_mode", gameMode, "release_track", desiredReleaseTrack, "err", err)
		if u.modelB {
			// POST transport、响应解析、严格 GET 任一步失败都可能对应“已应用但结果迟到”。
			// 永久 uncertain claim 是唯一安全结果：不自动 Release/Delete、不恢复 TTL，
			// 后续同 match 重试只能 fail-closed，绝不产生第二次 POST。
			plog.With(ctx).Errorw("msg", "gameserver_allocation_uncertain_retained",
				"match_id", matchID, "allocation_id", allocationID, "pod", podName)
			return nil, errcode.New(errcode.ErrUnavailable,
				"battle %d gameserver allocation result uncertain", matchID)
		}
		u.cleanupAllocatedBattle(ctx, matchID, allocationID, podName, authoritative)
		return nil, errcode.New(errcode.ErrDSAllocationFailed, "allocate ds for match %d failed", matchID)
	}
	if u.modelB && (authoritative == nil || authoritative.PodName == "" || authoritative.Addr == "" ||
		authoritative.InstanceUID == "" || authoritative.PodUID == "" || authoritative.ResourceVersion == "" ||
		authoritative.AllocationID != allocationID || !releasetrack.Valid(authoritative.ReleaseTrack)) {
		// data 层虽已做严格 GET，这里仍在副作用边界复核完整身份，防错误实现/测试桩
		// 以 nil error 绕过 UID/RV/allocation_id 确认。保持永久 uncertain，不做清理。
		plog.With(ctx).Errorw("msg", "gameserver_authoritative_identity_incomplete",
			"match_id", matchID, "allocation_id", allocationID, "allocation", authoritative)
		return nil, errcode.New(errcode.ErrUnavailable,
			"battle %d gameserver identity unavailable", matchID)
	}

	now := time.Now().UnixMilli()
	if authoritative != nil {
		actualReleaseTrack = authoritative.ReleaseTrack
	}
	if !releasetrack.Valid(actualReleaseTrack) {
		plog.With(ctx).Errorw("msg", "battle_allocate_refused", "reason", reasonAllocTrackInvalid,
			"match_id", matchID, "allocation_id", allocationID, "pod", podName,
			"desired_track", desiredReleaseTrack, "actual_track", actualReleaseTrack,
			"hint", "§9.21 灰度轨道粘滞被破坏,已分配的 pod 会被立即回收")
		u.cleanupAllocatedBattle(ctx, matchID, allocationID, podName, authoritative)
		return nil, errcode.New(errcode.ErrDSAllocationFailed,
			"allocator returned invalid actual release_track %q", actualReleaseTrack)
	}
	battle := &dsv1.BattleStorageRecord{
		MatchId:              matchID,
		DsPodName:            podName,
		DsAddr:               addr,
		State:                stateWarming, // 等 DS 心跳确认 ready 才回 matchmaker;不把 Agones Allocated 当成 ready
		PlayerIds:            playerIDs,
		MapId:                mapID,
		GameMode:             gameMode,
		RatingMode:           ratingMode,
		RatingPool:           ratingPool,
		AllocatedAtMs:        now,
		LastHeartbeatMs:      now, // 仅作 sweep 宽限基准;ready 判定要求 LastHeartbeatMs 严格大于此(即真实心跳)
		PlayerCount:          int32(len(playerIDs)),
		AllocationId:         allocationID,
		ReleaseTrack:         actualReleaseTrack,
		PlayerCombatFactions: cloneCombatFactionRecords(combatFactions),
		// 同 claim:策略代随分配冻结,永不改写(回滚/再激活靠配置代前进,不回填存量局)。
		RosterPolicyGeneration: u.cfg.RosterPolicyGeneration,
	}
	if authoritative != nil {
		battle.GameserverUid = authoritative.InstanceUID
		battle.PodUid = authoritative.PodUID
	} else if !u.modelB {
		// legacy 本地面回填 exact 实例身份(2026-08-04):不回填则 gameserver_uid /
		// instance_epoch 恒零,matchmaker 判「未回填完整 DS 目标」拒签 v2 战斗票、对局
		// 直接 FAILED,玩家永远进不去副本。身份取自拉起该进程时生成、且已签进其 DS 回调
		// 令牌的同一组值(见 data.LocalGameServerAllocator.LocalInstanceIdentity),
		// 与 DS 自报身份天然相等。
		//
		// 双重机械隔离(与 hub 侧 localTicketBinding 同手法):①!u.modelB;
		// ②接口断言——AgonesGameServerAllocator 不实现 localInstanceIdentitySource,
		// 生产恒 false。任一门不过都保持旧行为(留零值),Agones 路径零变更。
		if src, ok := u.alloc.(localInstanceIdentitySource); ok {
			if uid, epoch, found := src.LocalInstanceIdentity(podName); found {
				battle.GameserverUid = uid
				battle.InstanceEpoch = epoch
			}
		}
	}
	var finalized bool
	if u.modelB {
		finalized, err = u.repo.FinalizeFencedBattleAllocation(ctx, battle, u.battleTTL())
	} else {
		finalized, err = u.repo.FinalizeBattleAllocation(ctx, battle, u.battleTTL())
	}
	if err != nil || !finalized {
		if u.modelB {
			// UID/RV 已确认但 Redis finalize 失败/响应未知时仍不自动释放：事务可能已把
			// persistent uncertain 成功改成 warming。后续只能由权威读回/审计收敛，
			// 本请求绝不能凭本地结果删除 claim 或触碰 K8s。
			plog.With(ctx).Errorw("msg", "gameserver_fenced_finalize_unavailable",
				"match_id", matchID, "allocation_id", allocationID,
				"pod", podName, "finalized", finalized, "err", err)
			return nil, errcode.New(errcode.ErrUnavailable,
				"battle %d allocation finalize unavailable", matchID)
		}
		// legacy 路径仍可按 allocation_id 清理；它没有 POST 结果未知的 persistent fence。
		plog.With(ctx).Warnw("msg", "battle_allocate_refused", "reason", reasonAllocFinalizeLost,
			"match_id", matchID, "allocation_id", allocationID, "pod", podName,
			"finalized", finalized, "err", err,
			"hint", "claim 已不属本次分配(被回收或被新分配取代),刚拿到的 pod 会被回收")
		u.cleanupAllocatedBattle(ctx, matchID, allocationID, podName, authoritative)
		if err != nil {
			return nil, err
		}
		return nil, errcode.New(errcode.ErrDSAllocationFailed,
			"battle %d allocation claim no longer owned", matchID)
	}
	if u.modelB {
		if err := u.provisionBattleCredential(ctx, matchID, allocationID, authoritative); err != nil {
			plog.With(ctx).Errorw("msg", "battle_credential_provision_failed", "match_id", matchID,
				"pod", podName, "uid", authoritative.InstanceUID, "err", err)
			u.cleanupAllocatedBattle(ctx, matchID, allocationID, podName, authoritative)
			return nil, errcode.New(errcode.ErrDSAllocationFailed,
				"battle %d credential delivery failed", matchID)
		}
	}

	// R1:这是本次分配唯一一条"pod 已定、身份已定格、开始等 DS 就绪"的阶段推进。
	// 缺了它就无法证明"分配到底有没有走到 Agones 这一步"。线上默认 info 级下
	// Debugw 一条都不出,故升 Infow(每 match 至多一条)。
	plog.With(ctx).Infow("msg", "battle_warming", "match_id", matchID, "pod", podName,
		"ds_addr", addr, "players", len(playerIDs), "allocation_id", allocationID,
		"uid", battle.GameserverUid, "release_track", actualReleaseTrack,
		"ready_wait", u.readyWaitTimeout().String())

	// 等 DS 用正确 match_id/pod 的心跳上报 ready/running,后端才把 ds_addr 回给 matchmaker。
	res, werr := u.waitBattleReady(ctx, matchID, podName, allocationID)
	if werr != nil {
		if errors.Is(werr, errReadyWaitTimeout) {
			return nil, u.failReadyWaitTimeout(ctx, matchID, allocationID, podName, authoritative, true)
		}
		if errors.Is(werr, errBattleWaitOwnershipLost) {
			// 回收/接管所有权已属 sweep 或新分配:owner 放弃 cleanup,避免与在途的
			// fenced 回收链并发第二路 FencePreactiveRelease/ReleaseExpected
			//(复审必修:reclaimed/purged/superseded outcome 可识别,owner 跳过)。
			plog.With(ctx).Infow("msg", "battle_ready_wait_ownership_lost",
				"match_id", matchID, "allocation_id", allocationID, "err", werr)
			return nil, werr
		}
		// 入站 ctx 取消/超时或 repo 出错等非超时失败:本次刚分配的 pod 由本调用持有,
		// 用独立 cleanup ctx 回收 pod + 删 warming 镜像,避免泄漏(入站 ctx 多半已失效)。
		u.cleanupAllocatedBattle(ctx, matchID, allocationID, podName, authoritative)
		return nil, werr
	}

	// owner 归属定案(owner-authority.md ②;contract 阶段=强依赖):READY 交付前逐玩家
	// Begin(BATTLE)。**写不进 owner 就不交付 READY**——交付即意味着客户端会拿票进这台
	// Battle DS,归属没定案就放行等于让玩家可能同时被两台 DS 认领(§9.22 / 底线第 3 条)。
	// 失败按分配失败处理:既有补偿链回收 claim 与 pod,撮合重试,客户端按 §9.23 退避重查。
	//
	// 预算 5s:一局最多 10 人,每人单轮 Query+Begin；EPOCH_CONFLICT fail-closed 交外层
	// 重走分配链并重读 allocation。串行执行,按本地 TiDB 往返量级留足余量。
	if err := ownerBeginPlayers(ctx, u.ownerAuth, playerIDs, ownerTypeBattle,
		ownerTargetFromAllocateResult(res), 5*time.Second); err != nil {
		if errors.Is(err, errOwnerBeginOutcomeUnknown) {
			// Begin 可能已在 owner 提交，只是回包与判定 Query 都失败。此时回收 Pod
			// 会把可能已落库的 PENDING owner 变成永久死目标；保留同 allocation 的
			// 技术 READY，让 owner 恢复后的 claim loser 只读 exact+postcheck 收敛。
			plog.With(ctx).Errorw("msg", "battle_owner_begin_outcome_unknown_retained",
				"match_id", matchID, "allocation_id", allocationID, "pod", res.DSPodName,
				"players", len(playerIDs), "err", err,
				"hint", "不交付 READY,不 rollback owner,不 cleanup allocation/pod;等待权威恢复后只读收敛")
			return nil, err
		}
		plog.With(ctx).Warnw("msg", "battle_ready_refused_owner_begin_failed",
			"match_id", matchID, "pod", res.DSPodName, "players", len(playerIDs), "err", err,
			"hint", "归属未定案不交付 READY(§9.22);走既有分配失败补偿链")
		u.cleanupAllocatedBattle(ctx, matchID, allocationID, podName, authoritative)
		return nil, err
	}

	// R1:分配链的终点(owner 已定案、ds_addr 即将回给 matchmaker),与 battle_warming
	// 成对。两条都有 ts 就能算出"等 DS 冷启动花了多久"(判据 5)。
	plog.With(ctx).Infow("msg", "battle_ready_after_heartbeat", "match_id", matchID, "pod", podName,
		"ds_addr", addr, "allocation_id", allocationID, "uid", res.GameserverUID,
		"epoch", res.InstanceEpoch, "players", len(playerIDs))
	return res, nil
}

func combatFactionRecords(
	canonicalPlayers []uint64,
	combatFactionByPlayer map[uint64]uint32,
) []*dsv1.BattlePlayerCombatFaction {
	if len(combatFactionByPlayer) == 0 {
		return nil
	}
	records := make([]*dsv1.BattlePlayerCombatFaction, 0, len(canonicalPlayers))
	for _, playerID := range canonicalPlayers {
		records = append(records, &dsv1.BattlePlayerCombatFaction{
			PlayerId: playerID, CombatFactionId: combatFactionByPlayer[playerID],
		})
	}
	return records
}

func cloneCombatFactionRecords(records []*dsv1.BattlePlayerCombatFaction) []*dsv1.BattlePlayerCombatFaction {
	if len(records) == 0 {
		return nil
	}
	out := make([]*dsv1.BattlePlayerCombatFaction, 0, len(records))
	for _, record := range records {
		out = append(out, &dsv1.BattlePlayerCombatFaction{
			PlayerId: record.GetPlayerId(), CombatFactionId: record.GetCombatFactionId(),
		})
	}
	return out
}

func combatFactionMapFromRecords(
	playerIDs []uint64,
	records []*dsv1.BattlePlayerCombatFaction,
) (map[uint64]uint32, error) {
	if len(records) == 0 {
		return nil, nil
	}
	canonicalPlayers, _, err := dsmetadata.CanonicalRoster(playerIDs)
	if err != nil || !slices.Equal(playerIDs, canonicalPlayers) || len(records) != len(playerIDs) {
		return nil, errors.New("combat faction records must align with canonical battle roster")
	}
	factions := make(map[uint64]uint32, len(records))
	for i, record := range records {
		if record == nil || record.GetPlayerId() != playerIDs[i] {
			return nil, errors.New("combat faction records are not in canonical roster order")
		}
		if record.GetCombatFactionId() > dsmetadata.MaxCombatFactionID {
			return nil, errors.New("combat faction_id exceeds DS camp range")
		}
		factions[record.GetPlayerId()] = record.GetCombatFactionId()
	}
	if _, _, err := dsmetadata.CanonicalCombatFactions(canonicalPlayers, factions); err != nil {
		return nil, err
	}
	return factions, nil
}

// sameBattleAllocationRequest 判定「同 match 的重试请求与已落定的 claim 是否同一次分配」。
//
// 刻意**不比 rating_mode**:①它不是身份/安全字段(身份是 roster + map_id + game_mode,
// 三者都比了),而是随 map_id 确定的本局元数据;②共存窗口里旧 matchmaker 不带本字段、
// 新 matchmaker 带,同一 match 的 ACK-loss 重试会让该值从 0 变成显式值——把它纳入比对
// 会把正常重试判成"快照冲突"直接分配失败,玩家进不去场景(§9.21 / §9.20)。
// 落定口径按 claim 赢家:后到的重试不改已定格的值。
func sameBattleAllocationRequest(existing, expected *dsv1.BattleStorageRecord) bool {
	if existing == nil || expected == nil || existing.GetMatchId() != expected.GetMatchId() ||
		existing.GetMapId() != expected.GetMapId() || existing.GetGameMode() != expected.GetGameMode() ||
		!slices.Equal(existing.GetPlayerIds(), expected.GetPlayerIds()) {
		return false
	}
	a, b := existing.GetPlayerCombatFactions(), expected.GetPlayerCombatFactions()
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].GetPlayerId() != b[i].GetPlayerId() ||
			a[i].GetCombatFactionId() != b[i].GetCombatFactionId() {
			return false
		}
	}
	return true
}

const (
	battleTokenAnnotationKey       = "pandora.dev/ds-token"
	battleTokenExpAnnotationKey    = "pandora.dev/ds-token-exp-ms"
	battleTokenGenAnnotationKey    = "pandora.dev/ds-token-gen"
	battleTokenJTIAnnotationKey    = "pandora.dev/ds-token-jti"
	battleInstanceUIDAnnotationKey = "pandora.dev/ds-instance-uid"
	battleInstanceEpochKey         = "pandora.dev/ds-instance-epoch"
	battleWriterEpochKey           = "pandora.dev/ds-writer-epoch"
	battleTokenKidKey              = "pandora.dev/ds-token-kid"
	battleTokenHashKey             = "pandora.dev/ds-token-sha256"
)

// provisionBattleCredential 完成 Redis stage→K8s 条件投递→Redis delivered CAS。任何半失败
// 都不会产生 active；cleanup 只按 expected allocation_id 撤销本轮 warming 实例。
func (u *AllocatorUsecase) provisionBattleCredential(
	ctx context.Context,
	matchID uint64,
	allocationID string,
	allocation *data.AuthoritativeGameServerAllocation,
) error {
	if allocation == nil {
		return errcode.New(errcode.ErrInvalidState, "missing authoritative gameserver allocation")
	}
	seed, err := u.authRepo.PrepareCredential(ctx, data.BattleAuthorityBinding{
		MatchID:             matchID,
		AllocationID:        allocationID,
		PodName:             allocation.PodName,
		InstanceUID:         allocation.InstanceUID,
		RequiredWriterEpoch: data.BattleDSWriterEpochV2,
		AuthTTL:             u.dsCredentialTTL,
		BattleTTL:           u.battleTTL(),
	})
	if err != nil {
		return err
	}
	allocation.InstanceEpoch = seed.InstanceEpoch
	jti := uuid.NewString()
	signed, err := u.dsSigner.SignBattleCredential(
		matchID, allocation.PodName, allocation.InstanceUID,
		seed.InstanceEpoch, seed.Gen, jti, u.dsCredentialTTL)
	if err != nil {
		return err
	}
	if signed.ExpMs <= 0 {
		return errcode.New(errcode.ErrInvalidState, "battle credential signer returned invalid exp")
	}
	credential := &dsv1.BattleDSCredential{
		Gen:           seed.Gen,
		Jti:           jti,
		ExpMs:         uint64(signed.ExpMs),
		Kid:           signed.Kid,
		InstanceUid:   allocation.InstanceUID,
		InstanceEpoch: seed.InstanceEpoch,
		TokenSha256:   signed.TokenSHA256,
		WriterEpoch:   signed.WriterEpoch,
	}
	if _, err := u.authRepo.StagePending(ctx, data.BattleStageInput{
		MatchID: matchID, AllocationID: allocationID,
		Credential: credential, AuthTTL: u.dsCredentialTTL,
	}); err != nil {
		return err
	}
	annotations := map[string]string{
		battleTokenAnnotationKey:       signed.Token,
		battleTokenExpAnnotationKey:    strconv.FormatUint(credential.ExpMs, 10),
		battleTokenGenAnnotationKey:    strconv.FormatUint(credential.Gen, 10),
		battleTokenJTIAnnotationKey:    credential.Jti,
		battleInstanceUIDAnnotationKey: credential.InstanceUid,
		battleInstanceEpochKey:         strconv.FormatUint(uint64(credential.InstanceEpoch), 10),
		battleWriterEpochKey:           strconv.FormatUint(uint64(credential.WriterEpoch), 10),
		battleTokenKidKey:              credential.Kid,
		battleTokenHashKey:             credential.TokenSha256,
	}
	rv, err := u.authoritativeAlloc.DeliverCredential(ctx, allocation, annotations)
	if err != nil {
		return err
	}
	if err := u.authRepo.MarkDelivered(
		ctx, matchID, allocationID, credential, rv, u.dsCredentialTTL); err != nil {
		// Redis 响应不确定时只认权威 read-back；不以本地 expected 或 K8s 镜像猜测成功。
		snapshot, readErr := u.authRepo.ReadAuthority(ctx, matchID)
		if readErr != nil || !battlePendingDelivered(snapshot, allocationID, credential, rv) {
			return err
		}
	}
	return nil
}

func battlePendingDelivered(
	snapshot data.BattleAuthoritySnapshot,
	allocationID string,
	expected *dsv1.BattleDSCredential,
	rv string,
) bool {
	a := snapshot.Auth
	p := a.GetPending()
	return snapshot.AuthFound && a.GetAllocationId() == allocationID && a.GetDeliveredRv() == rv &&
		p.GetGen() == expected.GetGen() && p.GetJti() == expected.GetJti() &&
		p.GetExpMs() == expected.GetExpMs() && p.GetKid() == expected.GetKid() &&
		p.GetInstanceUid() == expected.GetInstanceUid() && p.GetInstanceEpoch() == expected.GetInstanceEpoch() &&
		p.GetTokenSha256() == expected.GetTokenSha256() && p.GetWriterEpoch() == expected.GetWriterEpoch()
}

// awaitExistingAllocation 处理 claim 输家/幂等重试。调用方不拥有 allocation_id，故等待
// 失败时只返回错误，绝不清理记录或释放 Pod；资源生命周期只由 claim 赢家或 sweep 管理。
func (u *AllocatorUsecase) awaitExistingAllocation(
	ctx context.Context,
	matchID uint64,
	existing *dsv1.BattleStorageRecord,
) (*AllocateResult, error) {
	if existing == nil {
		plog.With(ctx).Warnw("msg", "battle_allocate_refused", "reason", reasonAwaitClaimMissing,
			"match_id", matchID,
			"hint", "claim 输家但读不回赢家的记录:记录已被回收或 TTL 过期,matchmaker 需重试")
		return nil, errcode.New(errcode.ErrDSAllocationFailed, "battle %d allocation claim missing", matchID)
	}
	if existing.State == stateAllocationUncertain || existing.State == stateAllocationReconciling {
		// 该状态表示 GSA POST 可能迟到应用。调用方不等待、不清理、不查删 K8s；
		// 只返回暂不可用，永久 claim 继续阻止同 match 第二次 POST。
		plog.With(ctx).Warnw("msg", "allocate_idempotent_uncertain",
			"match_id", matchID, "allocation_id", existing.AllocationId)
		return nil, errcode.New(errcode.ErrUnavailable,
			"battle %d allocation result requires explicit reconciliation", matchID)
	}
	if existing.State == statePreactiveReleasing || existing.State == stateAllocationAbort {
		// 外部 UID 条件删除尚未得到明确成功；永久 release fence 必须继续
		// 阻止本请求发第二次 GSA POST。回收可由幂等 sweep 重试，但安全不依赖重试。
		// 这条分支此前完全静默:玩家进不去而后端日志里连"为什么不给分配"都没有。
		plog.With(ctx).Warnw("msg", "battle_allocate_refused", "reason", reasonAwaitReleaseUnconfirmed,
			"match_id", matchID, "state", existing.State, "allocation_id", existing.GetAllocationId(),
			"pod", existing.GetDsPodName(),
			"hint", "上一次分配的 GameServer 回收未确认,永久 fence 阻止本次再分配;等 sweep 收敛后重试")
		return nil, errcode.New(errcode.ErrUnavailable,
			"battle %d preactive gameserver release is not confirmed", matchID)
	}
	if !u.modelB && battleReadyForPod(existing, existing.DsPodName, matchID, existing.AllocatedAtMs) {
		// R1:这是一次**路由判定结果**(本次重试直接复用已就绪的实例,不再分配)。
		// 它是"同一个 match 为什么两次拿到同一个 ds_addr"的唯一证据,不能只在 Debug。
		plog.With(ctx).Infow("msg", "allocate_idempotent_hit", "match_id", matchID,
			"ds_addr", existing.DsAddr, "state", existing.State, "allocation_id", existing.AllocationId,
			"pod", existing.GetDsPodName())
		return u.verifyExistingReadyDelivery(ctx, matchID, existing.GetPlayerIds(),
			allocateResultFromBattle(existing))
	}
	if existing.State != stateAllocating && existing.State != stateWarming &&
		(!u.modelB || (existing.State != stateReady && existing.State != stateRunning)) {
		plog.With(ctx).Warnw("msg", "allocate_idempotent_unusable", "match_id", matchID, "state", existing.State)
		return nil, errcode.New(errcode.ErrDSAllocationFailed,
			"battle %d in state %s, not allocatable", matchID, existing.State)
	}
	plog.With(ctx).Debugw("msg", "allocate_idempotent_wait", "match_id", matchID,
		"pod", existing.DsPodName, "state", existing.State, "allocation_id", existing.AllocationId)
	res, err := u.waitBattleReady(ctx, matchID, existing.DsPodName, existing.AllocationId)
	if err != nil {
		if errors.Is(err, errReadyWaitTimeout) {
			return nil, u.failReadyWaitTimeout(ctx, matchID, existing.AllocationId, existing.DsPodName, nil, false)
		}
		return nil, err
	}
	return u.verifyExistingReadyDelivery(ctx, matchID, existing.GetPlayerIds(), res)
}

// verifyExistingReadyDelivery 是 claim loser / 幂等重试返回 READY 前的双重只读门：
// 先确认全部玩家 owner 已被唯一 claim winner exact bind，再 one-shot 重读 allocation 的
// READY 快照，堵住 owner Query 期间 allocation 被回收/替换的 TOCTOU。loser 不拥有本次
// allocation，故任一失败只返回 ErrUnavailable，绝不 Begin/Release/cleanup。
func (u *AllocatorUsecase) verifyExistingReadyDelivery(ctx context.Context, matchID uint64,
	players []uint64, res *AllocateResult) (*AllocateResult, error) {
	target := ownerTargetFromAllocateResult(res)
	if err := ownerVerifyPlayersExact(ctx, u.ownerAuth, players, ownerTypeBattle, target, 5*time.Second); err != nil {
		plog.With(ctx).Warnw("msg", "battle_ready_refused_owner_not_exact",
			"match_id", matchID, "allocation_id", target.AssignmentOrAllocationID,
			"pod", target.PodName, "players", len(players), "err", err,
			"hint", "claim loser 只读等待唯一 winner 完成 owner bind,不触发 Begin/Release/cleanup")
		return nil, errcode.New(errcode.ErrUnavailable,
			"battle %d owner binding not ready: %v", matchID, err)
	}
	if err := u.verifyReadyAllocationSnapshot(ctx, matchID, res); err != nil {
		plog.With(ctx).Warnw("msg", "battle_ready_refused_postcheck_failed",
			"match_id", matchID, "allocation_id", target.AssignmentOrAllocationID,
			"pod", target.PodName, "err", err,
			"hint", "owner 校验后 allocation 已漂移或不再 READY;claim loser fail-closed 且不清理")
		return nil, err
	}
	return res, nil
}

func (u *AllocatorUsecase) verifyReadyAllocationSnapshot(
	ctx context.Context, matchID uint64, expected *AllocateResult,
) error {
	if expected == nil {
		return errcode.New(errcode.ErrUnavailable, "battle %d ready result missing", matchID)
	}
	var current *dsv1.BattleStorageRecord
	if u.modelB {
		if u.authRepo == nil {
			return errcode.New(errcode.ErrUnavailable, "battle %d authority repo missing", matchID)
		}
		snapshot, err := u.authRepo.ReadAuthority(ctx, matchID)
		if err != nil {
			return errcode.New(errcode.ErrUnavailable,
				"battle %d ready authority postcheck failed: %v", matchID, err)
		}
		ready, reason := snapshot.ReadyAuthorized(
			time.Now().UnixMilli(), u.cfg.HeartbeatTimeout.Std().Milliseconds())
		if !ready {
			return errcode.New(errcode.ErrUnavailable,
				"battle %d no longer ready after owner check: %s", matchID, reason)
		}
		current = snapshot.Battle
	} else {
		battle, found, err := u.repo.GetBattle(ctx, matchID)
		if err != nil {
			return errcode.New(errcode.ErrUnavailable,
				"battle %d ready projection postcheck failed: %v", matchID, err)
		}
		if !found || !battleReadyForPod(battle, expected.DSPodName, matchID, battle.GetAllocatedAtMs()) {
			return errcode.New(errcode.ErrUnavailable,
				"battle %d no longer ready after owner check", matchID)
		}
		current = battle
	}
	got := allocateResultFromBattle(current)
	if current == nil || current.GetMatchId() != matchID || got == nil || *got != *expected {
		return errcode.New(errcode.ErrUnavailable,
			"battle %d allocation target changed after owner check", matchID)
	}
	return nil
}

// battleReadyForPod 判定 DS 是否已用 Heartbeat 确认 ready:pod/match 对得上、有分配后的真实心跳
// (LastHeartbeatMs 严格大于 allocatedAtMs)、状态进入 ready 或 running。
// 当前 UE 侧上报的是 running(不一定先发 ready),所以后端先把 running 也视为可进入状态。
func battleReadyForPod(b *dsv1.BattleStorageRecord, podName string, matchID uint64, allocatedAtMs int64) bool {
	return b != nil &&
		b.MatchId == matchID &&
		b.DsPodName == podName &&
		b.LastHeartbeatMs > allocatedAtMs &&
		(b.State == stateReady || b.State == stateRunning)
}

// battleWaitStateProgressable 是 wait 可继续前进的状态**白名单**(复审 P1-3):
// allocating/warming 是在途,ready/running 即将命中 ready 判定。其余一切状态
// (ended/abandoned、preactive_release_pending、allocation_abort_pending、
// allocation_uncertain/reconciling/empty_fence、以及任何未知新状态)都已由回收/对账链
// 接管或属 fail-closed 墓碑——继续等待只会白耗 ready_wait,owner 也绝不能对其 cleanup。
func battleWaitStateProgressable(state string) bool {
	switch state {
	case stateAllocating, stateWarming, stateReady, stateRunning:
		return true
	}
	return false
}

// waitBattleReady 轮询 Redis 镜像直到 DS 心跳确认 ready,或 ReadyWaitTimeout 超时(返回 errReadyWaitTimeout)。
// 提前失败分两类:回收/接管所有权已属他人的一律携带 errBattleWaitOwnershipLost(owner 不得
// 再 cleanup);其余错误 owner 照旧 cleanup。
func (u *AllocatorUsecase) waitBattleReady(ctx context.Context, matchID uint64, podName, allocationID string) (*AllocateResult, error) {
	// waitStart 只用于日志的 waited_ms(§11.3 判据 5「慢在哪」):同一条 msg 上
	// 「等了多久才失败」区分得出"一进来就被判死"与"等满了 ready_wait"。
	// 不参与任何判定——上界仍只由 deadline 决定。
	waitStart := time.Now()
	deadline := waitStart.Add(u.readyWaitTimeout())
	ticker := time.NewTicker(readyPollInterval)
	defer ticker.Stop()
	// auth 缺失的累计观察时长(单调):owner 完成 provision 后才进入 wait,正常路径永不缺
	// auth;只有 joiner 会在 Claim→Prepare、Finalize→provision 在途窗口短暂看到缺 auth。
	var authMissingSince time.Time
	for {
		if u.modelB {
			snapshot, err := u.authRepo.ReadAuthority(ctx, matchID)
			if err != nil {
				// 基础设施抖动容忍到下个 tick(与下方 legacy 分支同语义,2026-08-04)。
				//
				// 本循环要等 DS 冷启动,每 tick 读一次 Redis 授权面。原先任意一次读错误就
				// 整局放弃,而 Redis 读超时抛的是**原始错误**(非 errcode),上层判
				// ErrUnknown → matchmaker ds_allocate_failed → 回滚 owner Begin → 玩家进不去。
				// 生产 Redis 主从切换、网络毛刺、GC 停顿任一下都会触发;k8s 里概率低于本机,
				// 但「一次抖动打掉整局分配」是结构性脆弱,不是概率问题。
				//
				// **只容忍这一类**:ReadAuthority 的传输/基础设施错误。下面全部 return 都是
				// **权威判定**(battle 键已 purge、分配被取代、auth 相位 TERMINATING/QUARANTINED、
				// 状态不可推进),它们是确定性事实,必须立即失败 —— 混进来一起吞会把
				// 「本分配已失效」拖成空转满 ready_wait(那正是历史上 141.85s 的根因)。
				// 容忍不会变成无限等待:循环末尾的 deadline 检查仍是唯一上界。
				//
				// ⚠️ 本分支是**生产路径**(Model B = authority_mode=redis,强制 agones+enforce)。
				// 行为变更:单次读失败由「立即失败」改为「重试到 deadline」。真集群未验证,
				// 见 PENDING-VERIFICATION 清单。
				plog.With(ctx).Warnw("msg", "battle_ready_wait_authority_read_transient",
					"match_id", matchID, "pod", podName, "err", err,
					"hint", "基础设施抖动,下个 tick 重读;deadline 仍是唯一上界")
				if !time.Now().Before(deadline) {
					return nil, errReadyWaitTimeout
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-ticker.C:
				}
				continue
			}
			b := snapshot.Battle
			// battle 键是分配的第一条持久痕迹:wait 上下文里它消失(无论 auth 是否残留)
			// 只可能是判死回收链已 purge 或 TTL 到期,本分配不可能再 ready,立即失败让
			// matchmaker 重试,不得空转满 ready_wait(实测 141.85s 总耗时的根因)。
			if !snapshot.BattleFound {
				// 以下每条 return 都曾是**完全静默**的:玩家进不去副本时,后端只留下
				// 上层一句自由文本 err,分不清"记录被回收""被新分配取代""授权被隔离"
				// ——三者的排查方向完全不同。每条按 §11.3 R2 补一个枚举 reason。
				plog.With(ctx).Warnw("msg", "battle_ready_wait_aborted", "reason", reasonWaitAuthorityPurged,
					"match_id", matchID, "pod", podName, "allocation_id", allocationID,
					"auth_found", snapshot.AuthFound, "waited_ms", time.Since(waitStart).Milliseconds(),
					"hint", "battle 键已被判死回收链 purge 或 TTL 到期,本分配不可能再 ready")
				return nil, errcode.NewCause(errcode.ErrDSAllocationFailed, errBattleWaitOwnershipLost,
					"battle %d authority purged while waiting ready (auth_found=%t)",
					matchID, snapshot.AuthFound)
			}
			if allocationID != "" && b.AllocationId != allocationID {
				plog.With(ctx).Warnw("msg", "battle_ready_wait_aborted", "reason", reasonWaitAllocSuperseded,
					"match_id", matchID, "pod", podName, "allocation_id", allocationID,
					"current_allocation_id", b.AllocationId, "state", b.State,
					"waited_ms", time.Since(waitStart).Milliseconds())
				return nil, errcode.NewCause(errcode.ErrDSAllocationFailed, errBattleWaitOwnershipLost,
					"battle %d allocation superseded", matchID)
			}
			// Auth/Battle 非对称处理(复审必修):
			//   - 缺 auth 且状态在 allocating/warming 之外:与 AbandonIfStale 的
			//     allocation-grace 契约同判,不可授权,立即失败;
			//   - 缺 auth 且在 grace 状态:只允许 joiner 的 provisioning 在途窗口,上界
			//     取 HeartbeatTimeout(15s)——窗口实际成本 = GSA POST(≤agones
			//     allocate_timeout 5s)+ DeliverCredential PATCH(≤5s)+ Redis 若干往返,
			//     15s 是保守上界;超界即 provision 已死(半失败残留),不得复用完整
			//     冷加载宽限空等 120s,fail-fast 交 sweep 按 grace 回收。
			if !snapshot.AuthFound {
				if b.State != stateAllocating && b.State != stateWarming {
					plog.With(ctx).Warnw("msg", "battle_ready_wait_aborted", "reason", reasonWaitAuthOutsideGrace,
						"match_id", matchID, "pod", podName, "allocation_id", allocationID,
						"state", b.State, "waited_ms", time.Since(waitStart).Milliseconds())
					return nil, errcode.NewCause(errcode.ErrDSAllocationFailed, errBattleWaitOwnershipLost,
						"battle %d missing auth outside allocation grace while waiting ready: state=%s",
						matchID, b.State)
				}
				if authMissingSince.IsZero() {
					authMissingSince = time.Now()
				} else if time.Since(authMissingSince) > u.cfg.HeartbeatTimeout.Std() {
					plog.With(ctx).Warnw("msg", "battle_ready_wait_aborted", "reason", reasonWaitAuthProvisionDead,
						"match_id", matchID, "pod", podName, "allocation_id", allocationID,
						"state", b.State, "auth_missing_ms", time.Since(authMissingSince).Milliseconds(),
						"grace", u.cfg.HeartbeatTimeout.Std().String(),
						"hint", "凭据投递(Redis stage→K8s PATCH→delivered CAS)半失败残留,交 sweep 按 grace 回收")
					return nil, errcode.NewCause(errcode.ErrDSAllocationFailed, errBattleWaitOwnershipLost,
						"battle %d auth provisioning stalled beyond grace while waiting ready: state=%s",
						matchID, b.State)
				}
			} else {
				authMissingSince = time.Time{}
			}
			// 状态白名单(复审 P1-3):终态、abort/uncertain/release 各类墓碑与未知状态
			// 一律视为回收/对账链已接管,立即携带所有权哨兵失败。
			if !battleWaitStateProgressable(b.State) {
				plog.With(ctx).Warnw("msg", "battle_ready_wait_aborted", "reason", reasonWaitStateReclaimed,
					"match_id", matchID, "pod", podName, "allocation_id", allocationID,
					"state", b.State, "authority", "redis",
					"waited_ms", time.Since(waitStart).Milliseconds(),
					"hint", "该分配已被回收/对账链接管(终态或各类墓碑),owner 不再 cleanup")
				return nil, errcode.NewCause(errcode.ErrDSAllocationFailed, errBattleWaitOwnershipLost,
					"battle %d reclaimed while waiting ready: state=%s", matchID, b.State)
			}
			// auth 相位门:TERMINATING/QUARANTINED 是回收/隔离链写下的永久 fence——即使
			// battle 投影仍是 warming/ready/running(半状态),该分配也不可能再被授权 ready。
			if snapshot.AuthFound {
				if phase := snapshot.Auth.GetPhase(); phase == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING ||
					phase == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_QUARANTINED {
					plog.With(ctx).Warnw("msg", "battle_ready_wait_aborted", "reason", reasonWaitAuthPhaseFenced,
						"match_id", matchID, "pod", podName, "allocation_id", allocationID,
						"phase", phase.String(), "state", b.State,
						"waited_ms", time.Since(waitStart).Milliseconds(),
						"hint", "battle 投影还是 warming/ready 的半状态,但授权已被永久 fence,不可能再放行")
					return nil, errcode.NewCause(errcode.ErrDSAllocationFailed, errBattleWaitOwnershipLost,
						"battle %d auth fenced while waiting ready: phase=%s state=%s",
						matchID, phase.String(), b.State)
				}
			}
			ready, _ := snapshot.ReadyAuthorized(
				time.Now().UnixMilli(), u.cfg.HeartbeatTimeout.Std().Milliseconds())
			if ready && (podName == "" || b.DsPodName == podName) {
				return allocateResultFromBattle(b), nil
			}
		} else {
			b, found, err := u.repo.GetBattle(ctx, matchID)
			if err != nil {
				// 基础设施抖动容忍到下个 tick(legacy/local 面,2026-08-04)。
				//
				// 本循环要等 DS 冷启动(editor 形态可达 60s+),每 tick 读一次 Redis。原先
				// 任意一次读错误就整局放弃,而 Redis 读超时抛的是**原始错误**(非 errcode),
				// 上层判 ErrUnknown(code=1)→ matchmaker ds_allocate_failed → 回滚 owner Begin
				// → 玩家被弹回大厅。本机同时跑 UE 编辑器 + 多个 DS + 21 个服务时,单次读
				// 偶发超过 read_timeout 即触发(2026-08-04 实测:AllocateBattle 59.6s code=1,
				// 紧接着的重试 allocate_idempotent_hit 就成功了 —— 证明 DS 其实好好的)。
				//
				// **只容忍这一类**:GetBattle 的传输/基础设施错误。下面三个 return 是
				// **权威判定**(记录已被回收、分配被取代、状态不可推进),它们是确定性事实,
				// 必须立即失败,绝不能一起吞掉 —— 那会把「本分配已失效」拖成空转满 ready_wait。
				// 容忍不会变成无限等待:循环末尾的 deadline 检查(见下方 !time.Now().Before)
				// 仍是唯一上界,到点照常 errReadyWaitTimeout。
				//
				// 作用域:仅 legacy 分支。上面的 Model B 分支**刻意不动** —— 它是生产路径,
				// 同样的容忍是对的但属行为变更,须真集群验证后另行评估(见事故档案行动项)。
				plog.With(ctx).Warnw("msg", "battle_ready_wait_read_transient",
					"match_id", matchID, "pod", podName, "err", err,
					"hint", "基础设施抖动,下个 tick 重读;deadline 仍是唯一上界")
				if !time.Now().Before(deadline) {
					return nil, errReadyWaitTimeout
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-ticker.C:
				}
				continue
			}
			// wait 由 finalize 之后进入,镜像消失只可能是回收/TTL,同 modelB 立即失败。
			if !found {
				plog.With(ctx).Warnw("msg", "battle_ready_wait_aborted", "reason", reasonWaitRecordGone,
					"match_id", matchID, "pod", podName, "allocation_id", allocationID,
					"authority", "legacy", "waited_ms", time.Since(waitStart).Milliseconds())
				return nil, errcode.NewCause(errcode.ErrDSAllocationFailed, errBattleWaitOwnershipLost,
					"battle %d record gone while waiting ready", matchID)
			}
			if allocationID != "" && b.AllocationId != allocationID {
				plog.With(ctx).Warnw("msg", "battle_ready_wait_aborted", "reason", reasonWaitAllocSuperseded,
					"match_id", matchID, "pod", podName, "allocation_id", allocationID,
					"current_allocation_id", b.AllocationId, "authority", "legacy",
					"waited_ms", time.Since(waitStart).Milliseconds())
				return nil, errcode.NewCause(errcode.ErrDSAllocationFailed, errBattleWaitOwnershipLost,
					"battle %d allocation superseded", matchID)
			}
			// legacy 同用白名单:滚动共存期也可能读到 modelB 写下的墓碑状态,一律不等。
			if !battleWaitStateProgressable(b.State) {
				plog.With(ctx).Warnw("msg", "battle_ready_wait_aborted", "reason", reasonWaitStateReclaimed,
					"match_id", matchID, "pod", podName, "allocation_id", allocationID,
					"state", b.State, "authority", "legacy",
					"waited_ms", time.Since(waitStart).Milliseconds())
				return nil, errcode.NewCause(errcode.ErrDSAllocationFailed, errBattleWaitOwnershipLost,
					"battle %d reclaimed while waiting ready: state=%s", matchID, b.State)
			}
			if found && (podName == "" || b.DsPodName == podName) &&
				battleReadyForPod(b, b.DsPodName, matchID, b.AllocatedAtMs) {
				return allocateResultFromBattle(b), nil
			}
		}
		if !time.Now().Before(deadline) {
			return nil, errReadyWaitTimeout
		}
		select {
		case <-ctx.Done():
			// 调用方(matchmaker)先放弃了。以前这条只返回 ctx.Err(),上层原样透传,
			// 日志里既看不出"是谁先撤的",也看不出已经等了多久。
			plog.With(ctx).Warnw("msg", "battle_ready_wait_aborted", "reason", reasonWaitCtxDone,
				"match_id", matchID, "pod", podName, "allocation_id", allocationID,
				"waited_ms", time.Since(waitStart).Milliseconds(),
				"ready_wait", u.readyWaitTimeout().String(), "err", ctx.Err(),
				"hint", "入站 ctx 已取消/超时,owner 会用 detached ctx 回收 pod 与 warming 镜像")
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// failReadyWaitTimeout 处理 ready 等待超时:回收 pod + 删镜像,返回 ErrDSAllocationFailed
// (绝不把 ds_addr 回给 matchmaker,否则客户端连上 match_id 仍为 0 的 DS 会被 PreLogin 拒)。
func (u *AllocatorUsecase) failReadyWaitTimeout(
	ctx context.Context,
	matchID uint64,
	allocationID, podName string,
	allocation *data.AuthoritativeGameServerAllocation,
	owner bool,
) error {
	plog.With(ctx).Warnw("msg", "battle_ready_wait_timeout", "match_id", matchID, "pod", podName,
		"allocation_id", allocationID, "ready_wait", u.readyWaitTimeout().String(), "owner", owner,
		"hint", "DS 在等待窗口内没上报 ready/running 心跳:查该 pod 的 DS 进程日志与镜像关卡表;"+
			"owner=true 时本调用会回收 pod 并删 warming 镜像")
	if owner {
		u.cleanupAllocatedBattle(ctx, matchID, allocationID, podName, allocation)
	}
	return errcode.New(errcode.ErrDSAllocationFailed, "battle %d ds not ready within wait timeout", matchID)
}

// cleanupAllocatedBattle 用与入站 ctx 解耦的独立 ctx 执行
// 永久 release fence → UID 条件回收 → 明确成功后 purge。
// 入站 ctx 在 ready 等待失败时多半已被取消/超时,直接复用它做 Release/DeleteBattle 会立刻
// 失败,从而留下 warming 镜像 + 已分配 pod 泄漏;故这里用 plog.Detach 出一个短超时 ctx
// 兜底回收(§16.7:detached 路径只复制日志字段,不携带请求级 transport)。
func (u *AllocatorUsecase) cleanupAllocatedBattle(
	ctx context.Context,
	matchID uint64,
	allocationID, podName string,
	allocation *data.AuthoritativeGameServerAllocation,
) {
	cleanupCtx, cancel := context.WithTimeout(plog.Detach(ctx), detachedCleanupTimeout)
	defer cancel()
	if u.modelB {
		// 先把 auth+battle 同槽锁成永久 pre-active release fence，再触碰 K8s。
		// 旧实现先 DeleteExpected 后 ReleaseExpected：DELETE 超时会留下“Redis 已无 claim、
		// GSA 仍可能活着”的窗口，下一请求可对同 match 发第二次 POST。
		expected := data.BattleExpectedInstance{AllocationID: allocationID}
		if allocation != nil {
			expected.InstanceUID = allocation.InstanceUID
			expected.InstanceEpoch = allocation.InstanceEpoch
		}
		fenced, ferr := u.authRepo.FencePreactiveReleaseExpected(cleanupCtx, matchID, expected)
		if ferr != nil || !fenced {
			plog.With(ctx).Warnw("msg", "ready_wait_cleanup_fence_failed", "match_id", matchID,
				"allocation_id", allocationID, "fenced", fenced, "err", ferr)
			return
		}
		if podName == "" || allocation == nil {
			plog.With(ctx).Warnw("msg", "ready_wait_cleanup_identity_missing", "match_id", matchID,
				"allocation_id", allocationID)
			return
		}
		if rerr := u.releaseFencedPreactiveGameServer(cleanupCtx, matchID, podName, allocation); rerr != nil {
			// ReleaseExpected timeout/unknown 必须保留永久 fence；不得 purge。
			plog.With(ctx).Warnw("msg", "ready_wait_cleanup_release_unconfirmed", "match_id", matchID,
				"pod", podName, "err", rerr)
			return
		}
		purged, perr := u.authRepo.PurgePreactiveReleasedExpected(cleanupCtx, matchID, expected)
		if perr != nil || !purged {
			plog.With(ctx).Warnw("msg", "ready_wait_cleanup_purge_failed", "match_id", matchID,
				"allocation_id", allocationID, "purged", purged, "err", perr)
		}
		return
	}

	deleted, derr := u.repo.DeleteBattleIfAllocationMatches(cleanupCtx, matchID, allocationID, podName)
	if derr != nil {
		plog.With(ctx).Warnw("msg", "ready_wait_cleanup_delete_failed", "match_id", matchID, "err", derr)
	}
	if !deleted || podName == "" {
		return
	}
	if rerr := u.releaseGameServer(cleanupCtx, matchID, podName, allocation); rerr != nil {
		plog.With(ctx).Warnw("msg", "ready_wait_cleanup_release_failed", "match_id", matchID, "pod", podName, "err", rerr)
	}
}

func (u *AllocatorUsecase) releaseGameServer(
	ctx context.Context,
	matchID uint64,
	podName string,
	allocation *data.AuthoritativeGameServerAllocation,
) error {
	if !u.modelB {
		rerr := u.alloc.Release(ctx, podName)
		if rerr == nil {
			// 删除权留证(项目红线 never-delete-allocated-gameserver-20260803):
			// 每次真正回收 DS 都必须留下「删了什么」。legacy 面按 pod 名回收,没有
			// UID precondition,更需要能事后核对删的是不是该删的那台。
			plog.With(ctx).Infow("msg", "battle_gameserver_released", "match_id", matchID,
				"pod", podName, "authority", "legacy", "precondition", "pod_name_only")
		}
		return rerr
	}
	if matchID == 0 || allocation == nil || allocation.InstanceUID == "" ||
		allocation.InstanceEpoch == 0 || allocation.AllocationID == "" || podName == "" ||
		allocation.PodName != podName {
		// allocation 可能为 nil,字段单独取,不在日志表达式里解引用。
		var uid, allocID, allocPod string
		var epoch uint32
		if allocation != nil {
			uid, allocID, allocPod, epoch =
				allocation.InstanceUID, allocation.AllocationID, allocation.PodName, allocation.InstanceEpoch
		}
		plog.With(ctx).Warnw("msg", "battle_gameserver_release_refused", "reason", reasonReleaseTupleIncomplete,
			"match_id", matchID, "pod", podName, "has_allocation", allocation != nil,
			"uid", uid, "epoch", epoch, "allocation_id", allocID, "allocation_pod", allocPod,
			"hint", "身份不全一律不删(宁可占位):这正是防误删 exact 身份门在起作用")
		return errcode.New(errcode.ErrInvalidState,
			"battle Model B release requires complete expected GameServer tuple")
	}
	if allocation.PodUID == "" {
		podUID, err := u.ensureDurableReleasePodUID(ctx, matchID, podName,
			data.BattleExpectedInstance{
				AllocationID: allocation.AllocationID, InstanceUID: allocation.InstanceUID,
				InstanceEpoch: allocation.InstanceEpoch,
			}, allocation.ReleaseTrack)
		if err != nil {
			return err
		}
		allocation.PodUID = podUID
	}
	if allocation.PodUID == "" {
		plog.With(ctx).Warnw("msg", "battle_gameserver_release_refused", "reason", reasonReleasePodUIDMissing,
			"match_id", matchID, "pod", podName, "uid", allocation.InstanceUID,
			"epoch", allocation.InstanceEpoch, "allocation_id", allocation.AllocationID,
			"hint", "拿不到 Pod UID 就没有 DELETE precondition,绝不按名字删")
		return errcode.New(errcode.ErrInvalidState,
			"battle Model B release requires durable expected Pod UID")
	}
	if err := u.authoritativeAlloc.ReleaseExpected(ctx, allocation); err != nil {
		return err
	}
	// 删除权留证(项目红线:绝不误删 Allocated GameServer)。这一条是"删了什么、
	// 依据是什么"的权威记录:UID+PodUID 双 precondition 已在 data 层生效,这里把
	// 生效的那组身份完整落盘,事后能逐字段核对删的是不是该删的实例。
	plog.With(ctx).Infow("msg", "battle_gameserver_released", "match_id", matchID,
		"pod", podName, "uid", allocation.InstanceUID, "pod_uid", allocation.PodUID,
		"epoch", allocation.InstanceEpoch, "allocation_id", allocation.AllocationID,
		"release_track", allocation.ReleaseTrack, "authority", "redis",
		"precondition", "gs_uid+pod_uid")
	// 外部 UID 条件回收明确成功后先写 durable teardown proof，再允许
	// 上层 purge/expire battle+auth。proof 写失败必须整体返错保留永久
	// release fence；后续重试 ReleaseExpected(404 幂等成功)可补齐证明。
	return u.repo.RecordInstanceTeardown(ctx, matchID, data.BattleDepartureSource{
		DSPodName: podName, GameServerUID: allocation.InstanceUID,
		InstanceEpoch: allocation.InstanceEpoch, AllocationID: allocation.AllocationID,
		PodUID: allocation.PodUID,
	})
}

// releaseFencedPreactiveGameServer handles the crash window after the exact
// GameServer+Pod identity was durably finalized but before PrepareCredential
// assigned an instance epoch. FencePreactiveReleaseExpected is the mandatory
// Redis linearization point before this method.  Epoch zero has never admitted
// a DS or minted a ticket, so it must be physically released and purged without
// fabricating a credentialed instance-teardown proof.
func (u *AllocatorUsecase) releaseFencedPreactiveGameServer(
	ctx context.Context,
	matchID uint64,
	podName string,
	allocation *data.AuthoritativeGameServerAllocation,
) error {
	if allocation == nil || allocation.InstanceEpoch != 0 {
		return u.releaseGameServer(ctx, matchID, podName, allocation)
	}
	if !u.modelB || matchID == 0 || podName == "" || allocation.PodName != podName ||
		allocation.InstanceUID == "" || allocation.AllocationID == "" {
		return errcode.New(errcode.ErrInvalidState,
			"precredential release requires exact fenced GameServer tuple")
	}
	if allocation.PodUID == "" {
		podUID, err := u.ensureDurableReleasePodUID(ctx, matchID, podName,
			data.BattleExpectedInstance{
				AllocationID: allocation.AllocationID, InstanceUID: allocation.InstanceUID,
				InstanceEpoch: 0,
			}, allocation.ReleaseTrack)
		if err != nil {
			return err
		}
		allocation.PodUID = podUID
	}
	if allocation.PodUID == "" {
		return errcode.New(errcode.ErrInvalidState,
			"precredential release requires durable expected Pod UID")
	}
	return u.authoritativeAlloc.ReleaseExpected(ctx, allocation)
}

// reconcilePreactiveRelease 幂等完成永久 pre-active release fence → UID 条件删除 → purge。
// 返回 false 只表示当前仍不可确认回收；墓碑保持永久，安全性不依赖下一轮一定执行。
// preactiveReleaseOutcome 是 preactive/bootstrap 回收单次推进的结构化结果(复审 P1-2):
// 调用方据此决定是否登记 allocation-bound 退避,不再用单一 bool 把「不适用」与
// 「外部未确认」混为一谈。
type preactiveReleaseOutcome int

const (
	// preactiveReleaseSkipped:身份不完整/fence 判定不适用(如 ACTIVE 赢家),零外部副作用,
	// 无需退避——下轮状态自然分流。
	preactiveReleaseSkipped preactiveReleaseOutcome = iota
	// preactiveReleaseUnconfirmed:preflight/release/purge 任一步结果未确认,重试前应按
	// exact allocation 退避,防控制面持续故障时每轮重复外部调用占队头。
	preactiveReleaseUnconfirmed
	// preactiveReleaseCompleted:释放+purge 全链明确成功。
	preactiveReleaseCompleted
)

func (u *AllocatorUsecase) reconcilePreactiveRelease(
	ctx context.Context,
	battle *dsv1.BattleStorageRecord,
) preactiveReleaseOutcome {
	if battle == nil || battle.GetAllocationId() == "" || battle.GetGameserverUid() == "" {
		return preactiveReleaseSkipped
	}
	expected := data.BattleExpectedInstance{
		AllocationID: battle.GetAllocationId(), InstanceUID: battle.GetGameserverUid(),
		InstanceEpoch: battle.GetInstanceEpoch(),
	}
	podUID, preflightErr := u.ensureDurableReleasePodUID(
		ctx, battle.GetMatchId(), battle.GetDsPodName(), expected, battle.GetReleaseTrack())
	if preflightErr != nil {
		plog.With(ctx).Warnw("msg", "preactive_release_pod_uid_preflight_failed",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", preflightErr)
		return preactiveReleaseUnconfirmed
	}
	battle.PodUid = podUID
	fenced, err := u.authRepo.FencePreactiveReleaseExpected(ctx, battle.GetMatchId(), expected)
	if err != nil {
		plog.With(ctx).Warnw("msg", "preactive_release_fence_failed", "match_id", battle.GetMatchId(),
			"allocation_id", battle.GetAllocationId(), "fenced", fenced, "err", err)
		return preactiveReleaseUnconfirmed
	}
	if !fenced {
		// fence 明确拒绝(身份不符/已激活赢家/状态不适用):零副作用,属不适用而非未确认。
		plog.With(ctx).Debugw("msg", "preactive_release_fence_not_applicable",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId())
		return preactiveReleaseSkipped
	}
	allocation := &data.AuthoritativeGameServerAllocation{
		PodName: battle.GetDsPodName(), InstanceUID: battle.GetGameserverUid(),
		PodUID: battle.GetPodUid(), InstanceEpoch: battle.GetInstanceEpoch(),
		AllocationID: battle.GetAllocationId(),
	}
	if err := u.releaseFencedPreactiveGameServer(ctx, battle.GetMatchId(), battle.GetDsPodName(), allocation); err != nil {
		plog.With(ctx).Warnw("msg", "preactive_release_unconfirmed", "match_id", battle.GetMatchId(),
			"allocation_id", battle.GetAllocationId(), "err", err)
		return preactiveReleaseUnconfirmed
	}
	purged, err := u.authRepo.PurgePreactiveReleasedExpected(ctx, battle.GetMatchId(), expected)
	if err != nil || !purged {
		plog.With(ctx).Warnw("msg", "preactive_release_purge_failed", "match_id", battle.GetMatchId(),
			"allocation_id", battle.GetAllocationId(), "purged", purged, "err", err)
		return preactiveReleaseUnconfirmed
	}
	return preactiveReleaseCompleted
}

// publishReconciledAllocationAbandoned is the durable handoff from a
// physically-confirmed pre-credential allocation cleanup to battle_result.
// The Redis battle record remains permanent until Kafka acknowledges the
// ABANDONED event; only then may it receive terminal retention TTL and leave
// the active recovery index.
func (u *AllocatorUsecase) publishReconciledAllocationAbandoned(
	ctx context.Context,
	battle *dsv1.BattleStorageRecord,
) bool {
	if battle == nil || battle.GetMatchId() == 0 || battle.GetState() != stateAbandoned ||
		battle.GetAllocationId() == "" || battle.GetInstanceEpoch() != 0 || len(battle.GetPlayerIds()) == 0 {
		return false
	}
	snapshot, err := u.authRepo.ReadAuthority(ctx, battle.GetMatchId())
	if err != nil || snapshot.AuthFound || !snapshot.BattleFound || snapshot.Battle == nil ||
		snapshot.Battle.GetState() != stateAbandoned ||
		snapshot.Battle.GetAllocationId() != battle.GetAllocationId() ||
		snapshot.Battle.GetGameserverUid() != battle.GetGameserverUid() {
		plog.With(ctx).Warnw("msg", "allocation_uncertain_terminal_snapshot_rejected",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
		return false
	}
	battle = snapshot.Battle
	if !u.deliverAbandoned(ctx, battle.GetMatchId(), battle.GetDsPodName(), battle.GetGameserverUid(),
		battle.GetPlayerIds(), battle.GetMapId(), battle.GetGameMode()) {
		return false
	}
	if battle.GetGameserverUid() == "" {
		// Empty LIST is enough to release the players, but not enough to forget
		// cleanup authority: the original timed-out POST may apply after that
		// LIST. Persist Kafka ACK as a separate tombstone state and keep polling
		// the allocation_id forever (until a future explicit quiescence proof).
		repo, ok := u.repo.(data.AllocationUncertainRepo)
		if !ok {
			return false
		}
		marked, err := repo.MarkAllocationUncertainEmptyLifecyclePublished(ctx,
			battle.GetMatchId(), battle.GetAllocationId())
		if err != nil || !marked {
			plog.With(ctx).Warnw("msg", "allocation_uncertain_empty_tombstone_failed",
				"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(),
				"marked", marked, "err", err)
			return false
		}
		// Delay the next cleanup pass by one heartbeat window without changing
		// the permanent battle record or pretending this is a DS heartbeat.
		if err := u.repo.TouchActive(ctx, battle.GetMatchId(), time.Now().UnixMilli()); err != nil {
			plog.With(ctx).Warnw("msg", "allocation_uncertain_empty_tombstone_index_failed",
				"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
			return false
		}
		plog.With(ctx).Infow("msg", "allocation_uncertain_empty_tombstone_retained",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId())
		return true
	}
	if err := u.repo.ExpireBattle(ctx, battle.GetMatchId(), u.battleTTL()); err != nil {
		plog.With(ctx).Warnw("msg", "allocation_uncertain_terminal_expire_failed",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
		return false
	}
	plog.With(ctx).Infow("msg", "allocation_uncertain_terminal_delivered",
		"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId())
	return true
}

// resumeReconciledAllocationAbandoned re-confirms physical absence after a
// crash between the durable ABANDONED CAS and lifecycle publication. The
// repeat release is exact and idempotent (UID+Pod UID, or allocation_id label
// when the original POST never produced an object).
func (u *AllocatorUsecase) resumeReconciledAllocationAbandoned(
	ctx context.Context,
	battle *dsv1.BattleStorageRecord,
) bool {
	if battle == nil || battle.GetState() != stateAbandoned || battle.GetInstanceEpoch() != 0 ||
		battle.GetAllocationId() == "" {
		return false
	}
	snapshot, err := u.authRepo.ReadAuthority(ctx, battle.GetMatchId())
	if err != nil || snapshot.AuthFound || !snapshot.BattleFound || snapshot.Battle == nil ||
		snapshot.Battle.GetState() != stateAbandoned ||
		snapshot.Battle.GetAllocationId() != battle.GetAllocationId() {
		return false
	}
	battle = snapshot.Battle
	allocation := &data.AuthoritativeGameServerAllocation{AllocationID: battle.GetAllocationId()}
	if battle.GetGameserverUid() != "" {
		if battle.GetDsPodName() == "" || battle.GetPodUid() == "" {
			return false
		}
		allocation.PodName = battle.GetDsPodName()
		allocation.InstanceUID = battle.GetGameserverUid()
		allocation.PodUID = battle.GetPodUid()
		allocation.ReleaseTrack = battle.GetReleaseTrack()
	}
	if err := u.authoritativeAlloc.ReleaseExpected(ctx, allocation); err != nil {
		plog.With(ctx).Warnw("msg", "allocation_uncertain_terminal_release_unconfirmed",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
		return false
	}
	return u.publishReconciledAllocationAbandoned(ctx, battle)
}

// resumeEmptyAllocationTombstone continuously cleans the immutable
// allocation_id after the players' terminal lifecycle has already been
// published. It intentionally never expires/deletes the tombstone: a delayed
// original POST has no credential and cannot admit players, but must still be
// reaped whenever it becomes visible.
func (u *AllocatorUsecase) resumeEmptyAllocationTombstone(
	ctx context.Context,
	battle *dsv1.BattleStorageRecord,
) bool {
	if battle == nil || battle.GetState() != stateAllocationEmptyFence ||
		battle.GetAllocationId() == "" || battle.GetInstanceEpoch() != 0 ||
		battle.GetDsPodName() != "" || battle.GetGameserverUid() != "" || battle.GetPodUid() != "" {
		return false
	}
	snapshot, err := u.authRepo.ReadAuthority(ctx, battle.GetMatchId())
	if err != nil || snapshot.AuthFound || !snapshot.BattleFound || snapshot.Battle == nil ||
		snapshot.Battle.GetState() != stateAllocationEmptyFence ||
		snapshot.Battle.GetAllocationId() != battle.GetAllocationId() {
		return false
	}
	if err := u.authoritativeAlloc.ReleaseExpected(ctx,
		&data.AuthoritativeGameServerAllocation{AllocationID: battle.GetAllocationId()}); err != nil {
		plog.With(ctx).Warnw("msg", "allocation_uncertain_empty_tombstone_cleanup_failed",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
		return false
	}
	if err := u.repo.TouchActive(ctx, battle.GetMatchId(), time.Now().UnixMilli()); err != nil {
		plog.With(ctx).Warnw("msg", "allocation_uncertain_empty_tombstone_reindex_failed",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
		return false
	}
	return true
}

// reconcileAllocationUncertain turns an unknown GSA POST into a bounded,
// restart-safe terminal cancellation without ever issuing a second POST:
// allocation_id LIST -> authoritative empty or one exact GS+Pod -> permanent
// exact release fence -> confirmed physical absence -> durable ABANDONED ->
// lifecycle outbox. Ambiguous/API-unknown results leave the original permanent
// fence and active index untouched for the next sweep.
func (u *AllocatorUsecase) reconcileAllocationUncertain(
	ctx context.Context,
	battle *dsv1.BattleStorageRecord,
) bool {
	if battle == nil || !u.modelB || u.authRepo == nil || u.authoritativeAlloc == nil {
		return false
	}
	repo, repoOK := u.repo.(data.AllocationUncertainRepo)
	resolver, resolverOK := u.authoritativeAlloc.(UncertainGameServerAllocationResolver)
	if !repoOK || !resolverOK {
		plog.With(ctx).Errorw("msg", "allocation_uncertain_reconciler_unavailable_fail_closed",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId())
		return false
	}

	switch battle.GetState() {
	case stateAllocationUncertain:
		combatFactionByPlayer, factionErr := combatFactionMapFromRecords(
			battle.GetPlayerIds(), battle.GetPlayerCombatFactions())
		if factionErr != nil {
			plog.With(ctx).Errorw("msg", "allocation_uncertain_combat_factions_invalid_fail_closed",
				"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", factionErr)
			return false
		}
		allocation, found, err := resolver.ResolveAllocationByID(ctx, battle.GetMatchId(),
			battle.GetAllocationId(), battle.GetPlayerIds(), combatFactionByPlayer,
			battle.GetMapId(), battle.GetGameMode())
		if err != nil {
			plog.With(ctx).Warnw("msg", "allocation_uncertain_resolve_failed_will_retry",
				"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
			return false
		}
		if !found {
			// DeleteCollection+LIST is repeated even after a read-only empty result.
			// This closes a timeout-late-apply window before publishing terminal.
			allocation = &data.AuthoritativeGameServerAllocation{AllocationID: battle.GetAllocationId()}
			if err := u.authoritativeAlloc.ReleaseExpected(ctx, allocation); err != nil {
				plog.With(ctx).Warnw("msg", "allocation_uncertain_empty_release_unconfirmed",
					"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
				return false
			}
			completed, err := repo.CompleteAllocationUncertainRelease(ctx, battle.GetMatchId(),
				battle.GetAllocationId(), "")
			if err != nil || !completed {
				plog.With(ctx).Warnw("msg", "allocation_uncertain_empty_terminal_cas_failed",
					"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(),
					"completed", completed, "err", err)
				return false
			}
		} else {
			fenced, err := repo.FenceAllocationUncertainRelease(ctx, battle.GetMatchId(),
				battle.GetAllocationId(), allocation)
			if err != nil || !fenced {
				plog.With(ctx).Warnw("msg", "allocation_uncertain_exact_release_fence_failed",
					"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(),
					"fenced", fenced, "err", err)
				return false
			}
			if err := u.authoritativeAlloc.ReleaseExpected(ctx, allocation); err != nil {
				plog.With(ctx).Warnw("msg", "allocation_uncertain_exact_release_unconfirmed",
					"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
				return false
			}
			completed, err := repo.CompleteAllocationUncertainRelease(ctx, battle.GetMatchId(),
				battle.GetAllocationId(), allocation.InstanceUID)
			if err != nil || !completed {
				plog.With(ctx).Warnw("msg", "allocation_uncertain_exact_terminal_cas_failed",
					"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(),
					"completed", completed, "err", err)
				return false
			}
		}
	case stateAllocationReconciling:
		if battle.GetAllocationId() == "" || battle.GetDsPodName() == "" ||
			battle.GetGameserverUid() == "" || battle.GetPodUid() == "" || battle.GetInstanceEpoch() != 0 {
			return false
		}
		allocation := &data.AuthoritativeGameServerAllocation{
			PodName: battle.GetDsPodName(), InstanceUID: battle.GetGameserverUid(),
			PodUID: battle.GetPodUid(), AllocationID: battle.GetAllocationId(),
			ReleaseTrack: battle.GetReleaseTrack(),
		}
		if err := u.authoritativeAlloc.ReleaseExpected(ctx, allocation); err != nil {
			plog.With(ctx).Warnw("msg", "allocation_uncertain_exact_release_resume_failed",
				"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
			return false
		}
		completed, err := repo.CompleteAllocationUncertainRelease(ctx, battle.GetMatchId(),
			battle.GetAllocationId(), battle.GetGameserverUid())
		if err != nil || !completed {
			return false
		}
	default:
		return false
	}

	terminal, found, err := u.repo.GetBattle(ctx, battle.GetMatchId())
	if err != nil || !found || terminal.GetState() != stateAbandoned {
		return false
	}
	return u.publishReconciledAllocationAbandoned(ctx, terminal)
}

// ── RPC 2:ReleaseBattle ───────────────────────────────────────────────────────

// ReleaseBattle 回收战斗 DS。幂等:镜像不存在视为已释放,返回成功。
func (u *AllocatorUsecase) ReleaseBattle(ctx context.Context, matchID uint64, reason string) error {
	if matchID == 0 {
		plog.With(ctx).Warnw("msg", "battle_release_refused", "reason", reasonAllocMatchIDRequired,
			"release_reason", reason, "authority", "legacy")
		return errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	if u.modelB {
		// Model-B 禁止 match_id-only 回读“当前实例”后删除：旧请求可能误杀同 match
		// 重建出的新 UID。正常结算必须走 ReleaseBattleExpected 的 MySQL outbox 证明；
		// abandoned 由内部 sweep 已持有同事务 expected tuple，不经过本 RPC。
		plog.With(ctx).Warnw("msg", "battle_release_refused", "reason", reasonReleaseModelBUnsupported,
			"match_id", matchID, "release_reason", reason,
			"hint", "有调用方还在用 match_id-only 回收;Model B 必须走 ReleaseBattleExpected 的 expected tuple")
		return errcode.New(errcode.ErrInvalidArg,
			"battle %d Model-B release requires terminal outbox expected tuple", matchID)
	}
	battle, found, err := u.repo.GetBattle(ctx, matchID)
	if err != nil {
		return err
	}
	if !found {
		plog.With(ctx).Debugw("msg", "release_idempotent_miss", "match_id", matchID, "reason", reason)
		return nil
	}
	if battle.State == stateAllocationUncertain || battle.State == stateAllocationReconciling ||
		battle.State == stateAllocationEmptyFence ||
		battle.State == statePreactiveReleasing || battle.State == stateAllocationAbort {
		// 即使当前副本仍以 legacy 配置运行，也不能清理由另一个 Model-B writer
		// 写下的永久 POST fence。混跑期间最多返回不可用，绝不 Release/Delete。
		plog.With(ctx).Warnw("msg", "battle_release_refused", "reason", reasonReleaseFenced,
			"match_id", matchID, "release_reason", reason, "state", battle.State,
			"pod", battle.GetDsPodName(), "allocation_id", battle.GetAllocationId(),
			"hint", "该 match 被另一个 Model-B writer 永久 fence 住,legacy 副本只读跳过,等 sweep 对账")
		return errcode.New(errcode.ErrUnavailable,
			"battle %d allocation/release result requires explicit reconciliation", matchID)
	}
	if err := u.alloc.Release(ctx, battle.DsPodName); err != nil {
		plog.With(ctx).Warnw("msg", "gameserver_release_failed", "match_id", matchID, "pod", battle.DsPodName, "err", err)
	}
	// owner 精确释放(legacy 正常结算路径,2026-08-04 补;INC-20260804-001 缺口⑦)。
	//
	// 与 abort / abandon 两条路完全同因:READY 交付前的强 Begin 已把这批玩家的归属写成
	// BATTLE/本实例,而本函数此前只做「杀 DS + 删对局记录」,**从不动 owner**。
	// 后果:对局正常结算(含 PVE 主动退出)后,owner 仍是 BATTLE/ADMITTED 指向一台刚被
	// 销毁的 DS,login 的 §9.23 query-first 一直把玩家指回去;而 match 记录已删,
	// 客户端拿到的 TARGET 缺 match_id,判「incomplete owner identity」反复重试到 30s
	// deadline —— 玩家打完副本回不了大厅(2026-08-04 实测)。
	// 释放后恢复查询才会落到「无归属 → 首次进场链 → Hub」。
	//
	// 复用与 abort/abandon 同一把释放:三条安全边界(时序在 GameServer 回收之后、
	// exact 身份门只动仍指向本实例的记录、compare-delete 按 epoch 拒陈旧释放)由
	// ownerReleaseAbandonedPlayersWeak 自身保证;玩家已被分到别处时双重跳过,不误伤。
	// 实例身份取自战斗记录的 gameserver_uid(缺口③修复后 legacy 面才有值;为空则
	// 该函数自身的守卫会整体跳过,退化为改动前行为)。
	// 必须在 DeleteBattle **之前**取名单与身份 —— 删完就取不到了。
	ownerReleaseAbandonedPlayersWeak(ctx, u.ownerAuth, battle.GetPlayerIds(),
		battle.GetDsPodName(), battle.GetGameserverUid(), 2*time.Second)
	if err := u.repo.DeleteBattle(ctx, matchID); err != nil {
		return err
	}
	// R1:对局记录被删除是不可逆状态推进,且与 battle_warming / battle_ready_after_heartbeat
	// 成对收尾。缺了它就无法证明"这局到底是正常回收的还是被 sweep 判弃的"。
	plog.With(ctx).Infow("msg", "battle_released", "match_id", matchID, "pod", battle.DsPodName,
		"reason", reason, "state", battle.State, "players", len(battle.GetPlayerIds()),
		"allocation_id", battle.GetAllocationId(), "authority", "legacy")
	return nil
}

// AbortPreactiveBattle compensates a Matchmaker allocation saga only after a
// payload-authenticated service RPC has supplied the exact operation and DS
// tuple. The Redis fence+journal is committed before Kubernetes is touched;
// every unknown result remains retryable and the permanent journal recognizes
// an ACK-loss retry even after bounded battle/auth records expire.
func (u *AllocatorUsecase) AbortPreactiveBattle(ctx context.Context, request battleabort.Request) error {
	if !request.Complete() {
		plog.With(ctx).Warnw("msg", "battle_allocation_abort_refused", "reason", reasonAbortRequestIncomplete,
			"match_id", request.MatchID, "operation_id", request.OperationID,
			"pod", request.Target.PodName, "uid", request.Target.InstanceUID,
			"epoch", request.Target.InstanceEpoch, "allocation_id", request.Target.AllocationID,
			"release_track", request.Target.ReleaseTrack)
		return errcode.New(errcode.ErrInvalidArg, "complete battle allocation abort request required")
	}
	if !u.modelB || u.abortRepo == nil || u.authoritativeAlloc == nil {
		plog.With(ctx).Warnw("msg", "battle_allocation_abort_refused", "reason", reasonAbortAuthorityOff,
			"match_id", request.MatchID, "operation_id", request.OperationID,
			"hint", "本副本未启用 Redis 权威;matchmaker 的分配 saga 补偿只能等 sweep 兜底回收")
		return errcode.New(errcode.ErrInvalidState,
			"battle allocation abort requires Redis Model-B authority")
	}

	// Do not create the permanent ABORT fence for a legacy active record until
	// its exact Pod UID is durable.  If battle/auth are already gone, defer to
	// the permanent abort journal below so ACK-loss replay still works.
	preflight, err := u.authRepo.ReadAuthority(ctx, request.MatchID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "battle_allocation_abort_refused", "reason", reasonAbortPreflightFailed,
			"match_id", request.MatchID, "operation_id", request.OperationID,
			"pod", request.Target.PodName, "err", err)
		return err
	}
	if preflight.BattleFound {
		if _, err := u.ensureDurableReleasePodUID(ctx, request.MatchID, request.Target.PodName,
			data.BattleExpectedInstance{
				AllocationID: request.Target.AllocationID, InstanceUID: request.Target.InstanceUID,
				InstanceEpoch: request.Target.InstanceEpoch,
			}, request.Target.ReleaseTrack); err != nil {
			plog.With(ctx).Warnw("msg", "battle_allocation_abort_refused", "reason", reasonAbortPreflightFailed,
				"match_id", request.MatchID, "operation_id", request.OperationID,
				"pod", request.Target.PodName, "allocation_id", request.Target.AllocationID, "err", err,
				"hint", "pod_uid 未持久化前不得创建 ABORT fence;详因见同 trace_id 的 battle_pod_uid_preflight_refused")
			return err
		}
	}
	fence, err := u.abortRepo.FenceAllocationAbortExpected(ctx, request)
	if err != nil {
		plog.With(ctx).Warnw("msg", "battle_allocation_abort_refused", "reason", reasonAbortFenceFailed,
			"match_id", request.MatchID, "operation_id", request.OperationID,
			"pod", request.Target.PodName, "allocation_id", request.Target.AllocationID, "err", err)
		return err
	}
	if fence.Released {
		completed, completeErr := u.abortRepo.CompleteAllocationAbortExpected(
			ctx, request, u.dsCredentialTTL, u.battleTTL())
		if completeErr != nil {
			plog.With(ctx).Warnw("msg", "battle_allocation_abort_refused", "reason", reasonAbortAckCleanupPending,
				"match_id", request.MatchID, "operation_id", request.OperationID, "err", completeErr)
			return completeErr
		}
		if !completed {
			plog.With(ctx).Warnw("msg", "battle_allocation_abort_refused", "reason", reasonAbortAckCleanupPending,
				"match_id", request.MatchID, "operation_id", request.OperationID,
				"hint", "ACK-loss 重放已认出 RELEASED,但保留期清理未确认;调用方同 request 幂等重试")
			return errcode.New(errcode.ErrUnavailable,
				"battle %d allocation abort ACK cleanup pending", request.MatchID)
		}
		return nil
	}
	battle := fence.Battle
	if battle == nil || battle.GetMatchId() != request.MatchID ||
		battle.GetDsPodName() != request.Target.PodName || battle.GetPodUid() == "" {
		plog.With(ctx).Errorw("msg", "battle_allocation_abort_refused", "reason", reasonAbortFenceIdentityMissing,
			"match_id", request.MatchID, "operation_id", request.OperationID,
			"want_pod", request.Target.PodName, "got_pod", battle.GetDsPodName(),
			"got_pod_uid", battle.GetPodUid(), "allocation_id", request.Target.AllocationID,
			"hint", "fence 已写却拿不到 exact pod 权威,不得发 K8s 删除;人工对账")
		return errcode.New(errcode.ErrInvalidState,
			"battle %d allocation abort fence lacks exact pod authority", request.MatchID)
	}
	allocation := &data.AuthoritativeGameServerAllocation{
		PodName: request.Target.PodName, InstanceUID: request.Target.InstanceUID,
		PodUID: battle.GetPodUid(), InstanceEpoch: request.Target.InstanceEpoch,
		AllocationID: request.Target.AllocationID, ReleaseTrack: request.Target.ReleaseTrack,
	}
	if err := u.releaseGameServer(ctx, request.MatchID, request.Target.PodName, allocation); err != nil {
		return err
	}
	if !u.deliverAbandoned(ctx, request.MatchID, request.Target.PodName, request.Target.InstanceUID,
		battle.GetPlayerIds(), battle.GetMapId(), battle.GetGameMode()) {
		plog.With(ctx).Warnw("msg", "battle_allocation_abort_refused", "reason", reasonAbortLifecyclePending,
			"match_id", request.MatchID, "operation_id", request.OperationID,
			"pod", request.Target.PodName, "players", len(battle.GetPlayerIds()),
			"hint", "GameServer 已回收但 ABANDONED 补偿事件未投递,玩家段位回滚会延后;sweep 会重试")
		return errcode.New(errcode.ErrUnavailable,
			"battle %d allocation abort lifecycle publish pending", request.MatchID)
	}
	// owner 精确释放(§9.23 取消/失败路径;2026-07-29 补)。
	//
	// 判弃的这台实例正在被回收,但 READY 交付前的强 Begin 已经把这批玩家的归属写成
	// BATTLE/本实例。不释放的话 query-first 会一直把玩家指回一台死 DS:客户端 Travel 失败
	// → 重试 → 拿到同一个 TARGET → 再失败,正是验收底线第 1 条要防的「只能杀进程恢复」。
	// (abandon sweep 那条路有同样的释放,但它由心跳超时驱动;abort 是 saga 主动判弃,
	// 走不到那里,此前是纯缺口。)
	//
	// 复用 abandon sweep 的同一把释放:exact 身份门(记录仍指向本实例才动) + compare-delete,
	// 只撤销本次未交付的归属。若玩家已被分到别处(epoch/operation 已推进),
	// 门与 compare-delete 双重跳过,不误伤——验收底线第 4 条。
	ownerReleaseAbandonedPlayersWeak(ctx, u.ownerAuth, battle.GetPlayerIds(),
		request.Target.PodName, request.Target.InstanceUID, 2*time.Second)
	if err := u.lifecycleProofRepo.RecordAllocationLifecyclePublished(
		ctx, request.MatchID, request.Target); err != nil {
		plog.With(ctx).Warnw("msg", "battle_allocation_abort_refused", "reason", reasonAbortLifecycleMarker,
			"match_id", request.MatchID, "operation_id", request.OperationID,
			"allocation_id", request.Target.AllocationID, "err", err)
		return errcode.NewCause(errcode.ErrUnavailable, err,
			"battle %d allocation abort lifecycle marker pending", request.MatchID)
	}
	completed, err := u.abortRepo.CompleteAllocationAbortExpected(
		ctx, request, u.dsCredentialTTL, u.battleTTL())
	if err != nil {
		plog.With(ctx).Warnw("msg", "battle_allocation_abort_refused", "reason", reasonAbortCompletionPending,
			"match_id", request.MatchID, "operation_id", request.OperationID, "err", err)
		return err
	}
	if !completed {
		plog.With(ctx).Warnw("msg", "battle_allocation_abort_refused", "reason", reasonAbortCompletionPending,
			"match_id", request.MatchID, "operation_id", request.OperationID,
			"allocation_id", request.Target.AllocationID,
			"hint", "外部副作用已全部完成,仅永久日志置 RELEASED 未确认;同 request 幂等重试")
		return errcode.New(errcode.ErrUnavailable,
			"battle %d allocation abort completion pending", request.MatchID)
	}
	plog.With(ctx).Infow("msg", "battle_allocation_abort_completed",
		"match_id", request.MatchID, "operation_id", request.OperationID,
		"allocation_id", request.Target.AllocationID)
	return nil
}

// ReleaseBattleExpected 是 Model-B 正常结算 phase1 服务端回收入口。严格顺序：
//  1. MySQL 持久 proof 与当前 Redis stable identity 做 terminal+receipt 原子 CAS；
//  2. 用 Kubernetes GameServer UID delete precondition 回收；
//
// 本方法绝不恢复 Redis TTL。battle_result 必须先把成功 durable CAS 为 released_at_ms，
// 再调用 FinalizeBattleReleaseExpected；DB ACK 长期失败时永久墓碑不会先消失。
// 任一步 timeout/unknown 都返回 error；pending outbox 以同 tuple 幂等重试。
func (u *AllocatorUsecase) ReleaseBattleExpected(
	ctx context.Context,
	matchID uint64,
	reason, podName string,
	expected data.BattleExpectedInstance,
	proof data.BattleResultAuthorizationProof,
) error {
	if !u.modelB || u.authRepo == nil || u.authoritativeAlloc == nil {
		plog.With(ctx).Warnw("msg", "battle_release_refused", "reason", reasonReleaseAuthorityOff,
			"match_id", matchID, "pod", podName, "release_reason", reason,
			"hint", "本副本未启用 Redis 权威/权威分配器,正常结算回收不可用;battle_result 会持续重试")
		return errcode.New(errcode.ErrInvalidState, "battle terminal release requires Redis authority")
	}
	if matchID == 0 || reason != "completed" || podName == "" ||
		proof.Credential.PodName != podName || proof.Credential.InstanceUID != expected.InstanceUID ||
		proof.Credential.InstanceEpoch != expected.InstanceEpoch {
		// 逐项带出“哪一项对不上”:这是 battle_result outbox 与 allocator 权威之间唯一的
		// 身份握手,对不上就永远收不了 DS(pod 泄漏),但错误码只是一个 InvalidArg。
		plog.With(ctx).Warnw("msg", "battle_release_refused", "reason", reasonReleaseProofIncomplete,
			"match_id", matchID, "pod", podName, "release_reason", reason,
			"want_pod", podName, "got_proof_pod", proof.Credential.PodName,
			"want_uid", expected.InstanceUID, "got_proof_uid", proof.Credential.InstanceUID,
			"want_epoch", expected.InstanceEpoch, "got_proof_epoch", proof.Credential.InstanceEpoch,
			"allocation_id", expected.AllocationID)
		return errcode.New(errcode.ErrInvalidArg, "battle terminal release proof is incomplete")
	}
	// Rolling-upgrade gate: pod_uid was added after the first Model-B records.
	// It must be durably present before the terminal CAS.  Legacy records may
	// only be backfilled from exact K8s GameServer/allocation/owned-Pod reads;
	// missing or same-name replacement objects remain retryable and do not
	// create a permanent TERMINATING fence.
	if _, err := u.ensureDurableReleasePodUID(ctx, matchID, podName, expected, ""); err != nil {
		return err
	}
	terminated, err := u.authRepo.TerminateResultExpected(ctx, matchID, expected, proof)
	if err != nil {
		return err
	}
	if !terminated {
		plog.With(ctx).Warnw("msg", "battle_release_refused", "reason", reasonReleaseIdentityChanged,
			"match_id", matchID, "pod", podName, "uid", expected.InstanceUID,
			"epoch", expected.InstanceEpoch, "allocation_id", expected.AllocationID,
			"hint", "终态 CAS 未命中:该 match 的 stable 身份已变(同名 Pod 重建/已被判弃/已回收)")
		return errcode.New(errcode.ErrDSAllocationFailed,
			"battle %d stable identity changed before terminal release", matchID)
	}
	snapshot, err := u.authRepo.ReadAuthority(ctx, matchID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "battle_release_refused", "reason", reasonReleaseAuthorityReread,
			"match_id", matchID, "pod", podName, "allocation_id", expected.AllocationID, "err", err)
		return errcode.NewCause(errcode.ErrUnavailable, err,
			"battle %d terminal authority reread failed", matchID)
	}
	if !exactTerminatedReleaseSnapshot(snapshot, matchID, podName, expected) {
		plog.With(ctx).Warnw("msg", "battle_release_refused", "reason", reasonReleaseSnapshotStale,
			"match_id", matchID, "pod", podName, "uid", expected.InstanceUID,
			"epoch", expected.InstanceEpoch, "allocation_id", expected.AllocationID,
			"auth_found", snapshot.AuthFound, "battle_found", snapshot.BattleFound,
			"state", snapshot.Battle.GetState(), "phase", snapshot.Auth.GetPhase().String(),
			"hint", "终态 CAS 已成功但重读不自洽,本轮不碰 K8s;outbox 同 tuple 幂等重试")
		return errcode.New(errcode.ErrUnavailable,
			"battle %d terminal authority snapshot is incomplete or changed", matchID)
	}
	allocation := &data.AuthoritativeGameServerAllocation{
		PodName: podName, InstanceUID: expected.InstanceUID,
		InstanceEpoch: expected.InstanceEpoch, AllocationID: expected.AllocationID,
		PodUID: snapshot.Battle.GetPodUid(), ReleaseTrack: snapshot.Battle.GetReleaseTrack(),
	}
	if err := u.releaseGameServer(ctx, matchID, podName, allocation); err != nil {
		plog.With(ctx).Warnw("msg", "terminal_gameserver_release_unconfirmed",
			"match_id", matchID, "allocation_id", expected.AllocationID, "pod", podName, "err", err)
		return errcode.NewCause(errcode.ErrUnavailable, err,
			"battle %d terminal gameserver release not confirmed", matchID)
	}
	// owner 精确释放(Model B 正常结算收口;2026-08-04,INC-20260804-001 缺口⑦-B)。
	//
	// 与 legacy 面同因、同性质:全仓 owner 释放此前只接了「登出 / 判弃 / saga 中止」三条
	// **异常或主动离场**路径,**对局正常打完这条路一个都没接** —— 本函数(Model B 正常结算
	// 唯一收口)此前对 owner 权威零动作。后果:玩家正常结束对局后,owner 记录仍是
	// BATTLE/ADMITTED 指向一台刚被 K8s 删除的 GameServer;login 的 §9.23 query-first 会
	// 一直把玩家指回该实例,客户端 Travel 到不存在的 Pod → 重试 → 拿到同一 TARGET,
	// 正是 ownerReleaseAbandonedPlayersWeak 注释里点名的「比不接 owner 更糟」。
	// (local 面已由真实客户端两轮实测复现;本条为同一缺陷在 Model B 的对应位置,
	//  **尚未在真集群验证** —— 见事故档案 §10 行动项。)
	//
	// 时序:必须在 releaseGameServer **成功之后**。该调用是 K8s UID precondition 删除,
	// 返回 nil 即「本实例回收已确认」,满足安全边界①(提前释放会在旧 DS 可能仍在跑时
	// 放行新归属 = 双 DS)。上面任一步失败都已 return,走不到这里。
	// 边界②exact 身份门(只动仍指向本 pod+uid 的 BATTLE 记录)与 ③compare-delete(按
	// owner_epoch 拒陈旧释放)由 ownerReleaseAbandonedPlayersWeak 自身保证;玩家若已被
	// 分到别处(epoch/pod/uid 已变)双重跳过,不误伤活归属。
	//
	// 弱依赖:失败只告警,不改变本函数的返回值。phase-1 已完成的事实(Redis 终态 CAS +
	// GameServer 已删)不能因为 owner 抖动而回退成"未完成",否则 battle_result 的 outbox
	// 会重放一次已经删过 GameServer 的回收。owner 未释放的兜底是 login 侧
	// InspectBattleRoute 的 Terminal→Hub 旧门,以及玩家登出时的释放。
	ownerReleaseAbandonedPlayersWeak(ctx, u.ownerAuth, snapshot.Battle.GetPlayerIds(),
		podName, expected.InstanceUID, 2*time.Second)
	plog.With(ctx).Infow("msg", "battle_terminal_release_phase1_completed",
		"match_id", matchID, "allocation_id", expected.AllocationID,
		"pod", podName, "uid", expected.InstanceUID)
	return nil
}

func (u *AllocatorUsecase) ensureDurableReleasePodUID(
	ctx context.Context,
	matchID uint64,
	podName string,
	expected data.BattleExpectedInstance,
	expectedReleaseTrack string,
) (string, error) {
	snapshot, err := u.authRepo.ReadAuthority(ctx, matchID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "battle_pod_uid_preflight_refused", "reason", reasonPodUIDAuthorityRead,
			"match_id", matchID, "pod", podName, "allocation_id", expected.AllocationID, "err", err)
		return "", errcode.NewCause(errcode.ErrUnavailable, err,
			"battle %d release preflight authority read failed", matchID)
	}
	if !exactReleaseIdentitySnapshot(snapshot, matchID, podName, expected, expectedReleaseTrack) {
		// pod_uid 是所有精确回收(结算 / 判弃 / abort)的共同前提,拿不到就一台 DS 都收不回。
		// 带出期望与实际,才能一眼分出是"同名 Pod 重建"还是"记录已被推进到别的实例"。
		plog.With(ctx).Warnw("msg", "battle_pod_uid_preflight_refused", "reason", reasonPodUIDIdentityChanged,
			"match_id", matchID, "want_pod", podName, "got_pod", snapshot.Battle.GetDsPodName(),
			"want_uid", expected.InstanceUID, "got_uid", snapshot.Battle.GetGameserverUid(),
			"want_epoch", expected.InstanceEpoch, "got_epoch", snapshot.Battle.GetInstanceEpoch(),
			"want_allocation_id", expected.AllocationID, "got_allocation_id", snapshot.Battle.GetAllocationId(),
			"want_release_track", expectedReleaseTrack, "got_release_track", snapshot.Battle.GetReleaseTrack(),
			"battle_found", snapshot.BattleFound, "state", snapshot.Battle.GetState())
		return "", errcode.New(errcode.ErrDSAllocationFailed,
			"battle %d release preflight identity changed", matchID)
	}
	if snapshot.Battle.GetPodUid() != "" {
		return snapshot.Battle.GetPodUid(), nil
	}
	podUID, err := u.authoritativeAlloc.ResolveExpectedPodUID(ctx,
		&data.AuthoritativeGameServerAllocation{
			PodName: podName, InstanceUID: expected.InstanceUID,
			InstanceEpoch: expected.InstanceEpoch, AllocationID: expected.AllocationID,
		})
	if err != nil || podUID == "" {
		plog.With(ctx).Warnw("msg", "battle_pod_uid_preflight_refused", "reason", reasonPodUIDResolveFailed,
			"match_id", matchID, "pod", podName, "uid", expected.InstanceUID,
			"epoch", expected.InstanceEpoch, "allocation_id", expected.AllocationID,
			"resolved_pod_uid", podUID, "err", err,
			"hint", "legacy 记录缺 pod_uid 且从 K8s 精确回读不到;回收保持可重试,不创建永久 TERMINATING fence")
		return "", errcode.NewCause(errcode.ErrUnavailable, err,
			"battle %d legacy pod UID exact preflight failed", matchID)
	}
	err = u.repo.UpdateBattleKeepTTL(ctx, matchID, updateMaxRetry, func(battle *dsv1.BattleStorageRecord) error {
		if !exactReleaseBattleIdentity(battle, matchID, podName, expected, expectedReleaseTrack) {
			return errcode.New(errcode.ErrDSAllocationFailed,
				"battle %d changed during pod UID backfill", matchID)
		}
		switch battle.GetPodUid() {
		case "":
			battle.PodUid = podUID
		case podUID:
		default:
			return errcode.New(errcode.ErrDSAllocationFailed,
				"battle %d pod UID changed during backfill", matchID)
		}
		return nil
	})
	if err != nil {
		plog.With(ctx).Warnw("msg", "battle_pod_uid_preflight_refused", "reason", reasonPodUIDBackfillFailed,
			"match_id", matchID, "pod", podName, "pod_uid", podUID,
			"allocation_id", expected.AllocationID, "err", err)
		return "", errcode.NewCause(errcode.ErrUnavailable, err,
			"battle %d legacy pod UID durable backfill failed", matchID)
	}
	verified, err := u.authRepo.ReadAuthority(ctx, matchID)
	if err != nil || !exactReleaseIdentitySnapshot(verified, matchID, podName, expected, expectedReleaseTrack) ||
		verified.Battle.GetPodUid() != podUID {
		plog.With(ctx).Warnw("msg", "battle_pod_uid_preflight_refused", "reason", reasonPodUIDVerifyFailed,
			"match_id", matchID, "pod", podName, "want_pod_uid", podUID,
			"got_pod_uid", verified.Battle.GetPodUid(), "allocation_id", expected.AllocationID, "err", err)
		return "", errcode.NewCause(errcode.ErrUnavailable, err,
			"battle %d legacy pod UID backfill verification failed", matchID)
	}
	// R4:本函数在 sweep 每轮、每次心跳前置都可能被调用,成功侧只能 Debugw。
	// 但"回填过一次 pod_uid"是 legacy 记录被补齐的证据,查回收链时要看得到。
	plog.With(ctx).Debugw("msg", "battle_pod_uid_backfilled", "match_id", matchID,
		"pod", podName, "pod_uid", podUID, "allocation_id", expected.AllocationID)
	return podUID, nil
}

func exactReleaseBattleIdentity(
	battle *dsv1.BattleStorageRecord,
	matchID uint64,
	podName string,
	expected data.BattleExpectedInstance,
	expectedReleaseTrack string,
) bool {
	return battle != nil && battle.GetMatchId() == matchID &&
		battle.GetAllocationId() == expected.AllocationID && battle.GetDsPodName() == podName &&
		battle.GetGameserverUid() == expected.InstanceUID &&
		battle.GetInstanceEpoch() == expected.InstanceEpoch &&
		releasetrack.Valid(battle.GetReleaseTrack()) &&
		(expectedReleaseTrack == "" || battle.GetReleaseTrack() == expectedReleaseTrack) &&
		releaseStateAllowsPodUIDBackfill(battle.GetState())
}

func releaseStateAllowsPodUIDBackfill(state string) bool {
	switch state {
	case stateWarming, stateReady, stateRunning, stateEnded, stateAbandoned,
		statePreactiveReleasing, stateAllocationAbort:
		return true
	default:
		return false
	}
}

func exactReleaseIdentitySnapshot(
	snapshot data.BattleAuthoritySnapshot,
	matchID uint64,
	podName string,
	expected data.BattleExpectedInstance,
	expectedReleaseTrack string,
) bool {
	if !snapshot.BattleFound || !exactReleaseBattleIdentity(
		snapshot.Battle, matchID, podName, expected, expectedReleaseTrack) {
		return false
	}
	// auth may have naturally expired before result relay; TerminateResultExpected
	// has a proof-bound reconstruction path.  When present it must still bind the
	// exact stable allocation.
	return !snapshot.AuthFound || (snapshot.Auth != nil && snapshot.Auth.GetMatchId() == matchID &&
		snapshot.Auth.GetAllocationId() == expected.AllocationID && snapshot.Auth.GetDsPodName() == podName &&
		snapshot.Auth.GetInstanceUid() == expected.InstanceUID &&
		snapshot.Auth.GetInstanceEpoch() == expected.InstanceEpoch)
}

func exactTerminatedReleaseSnapshot(
	snapshot data.BattleAuthoritySnapshot,
	matchID uint64,
	podName string,
	expected data.BattleExpectedInstance,
) bool {
	return snapshot.AuthFound && snapshot.Auth != nil && snapshot.BattleFound && snapshot.Battle != nil &&
		exactReleaseBattleIdentity(snapshot.Battle, matchID, podName, expected, "") &&
		snapshot.Battle.GetState() == stateEnded && snapshot.Battle.GetPodUid() != "" &&
		snapshot.Auth.GetMatchId() == matchID && snapshot.Auth.GetAllocationId() == expected.AllocationID &&
		snapshot.Auth.GetDsPodName() == podName && snapshot.Auth.GetInstanceUid() == expected.InstanceUID &&
		snapshot.Auth.GetInstanceEpoch() == expected.InstanceEpoch &&
		snapshot.Auth.GetPhase() == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING
}

// FinalizeBattleReleaseExpected 是 durable released_at_ms 之后的 phase2。它只校验同一
// proof 的 Redis terminal/receipt 墓碑并恢复 TTL，绝不调用 Kubernetes。若上一次
// finalize 响应丢失且 TTL 已把三键全部清空，则按幂等成功返回。
func (u *AllocatorUsecase) FinalizeBattleReleaseExpected(
	ctx context.Context,
	matchID uint64,
	podName string,
	expected data.BattleExpectedInstance,
	proof data.BattleResultAuthorizationProof,
) error {
	if !u.modelB || u.authRepo == nil {
		plog.With(ctx).Warnw("msg", "battle_release_refused", "reason", reasonReleaseAuthorityOff,
			"match_id", matchID, "pod", podName, "release_reason", "completed-finalize")
		return errcode.New(errcode.ErrInvalidState, "battle terminal finalize requires Redis authority")
	}
	if matchID == 0 || podName == "" || proof.Credential.PodName != podName ||
		proof.Credential.InstanceUID != expected.InstanceUID ||
		proof.Credential.InstanceEpoch != expected.InstanceEpoch {
		plog.With(ctx).Warnw("msg", "battle_release_refused", "reason", reasonFinalizeProofIncomplete,
			"match_id", matchID, "pod", podName,
			"want_uid", expected.InstanceUID, "got_proof_uid", proof.Credential.InstanceUID,
			"want_epoch", expected.InstanceEpoch, "got_proof_epoch", proof.Credential.InstanceEpoch,
			"allocation_id", expected.AllocationID)
		return errcode.New(errcode.ErrInvalidArg, "battle terminal finalize proof is incomplete")
	}
	expired, err := u.authRepo.ExpireResultTerminatedExpected(
		ctx, matchID, expected, proof, u.battleTTL())
	if err != nil {
		plog.With(ctx).Warnw("msg", "battle_release_refused", "reason", reasonFinalizeTombstonePending,
			"match_id", matchID, "pod", podName, "allocation_id", expected.AllocationID, "err", err)
		return err
	}
	if !expired {
		plog.With(ctx).Warnw("msg", "battle_release_refused", "reason", reasonFinalizeTombstonePending,
			"match_id", matchID, "pod", podName, "uid", expected.InstanceUID,
			"epoch", expected.InstanceEpoch, "allocation_id", expected.AllocationID,
			"hint", "永久墓碑未能恢复有界 TTL(身份不匹配或已被推进);outbox 会同 tuple 重试")
		return errcode.New(errcode.ErrUnavailable,
			"battle %d terminal tombstone retention not confirmed", matchID)
	}
	plog.With(ctx).Infow("msg", "battle_terminal_release_finalized",
		"match_id", matchID, "allocation_id", expected.AllocationID,
		"pod", podName, "uid", expected.InstanceUID)
	return nil
}

// ── RPC 3:Heartbeat ───────────────────────────────────────────────────────────

// HeartbeatResult 是 Heartbeat 的出参(下发给 DS 的控制指令)。
type HeartbeatResult struct {
	Command               string
	AcceptedTokenGen      uint64
	AcceptedTokenJTI      string
	AcceptedInstanceUID   string
	AcceptedInstanceEpoch uint32
	AcceptedWriterEpoch   uint32
	EvictionOrders        []*dsv1.BattleEvictionOrder
}

// RedisAuthorityEnabled 供 service/gm 选择严格 Model B 路径；开启后不存在 legacy fallback。
func (u *AllocatorUsecase) RedisAuthorityEnabled() bool { return u != nil && u.modelB }

// HeartbeatAuthorized 是 Model B 唯一心跳入口。pending 激活、active 幂等续命、battle 投影
// 与服务端接收时间在 Redis 同槽一次 EXEC 完成；请求 ts_ms 不参与任何授权/TTL 判断。
func (u *AllocatorUsecase) HeartbeatAuthorized(
	ctx context.Context,
	matchID uint64,
	id data.BattleCredentialIdentity,
	playerCount int32,
	state string,
	tsMs int64,
) (*HeartbeatResult, error) {
	return u.HeartbeatAuthorizedWithPlayers(ctx, matchID, id, playerCount, state, tsMs,
		false, 0, "", nil, nil)
}

// HeartbeatAuthorizedWithPlayers 是 Battle→Hub 物理离场闭环心跳。只有
// snapshotPresent=true 的新 DS 才能用完整可信 active-player 快照提交
// departure；旧 DS 的 proto3 零值始终 fail-closed，但仍可收到 order 等待升级/
// source UID teardown。
func (u *AllocatorUsecase) HeartbeatAuthorizedWithPlayers(
	ctx context.Context,
	matchID uint64,
	id data.BattleCredentialIdentity,
	playerCount int32,
	state string,
	_ int64,
	snapshotPresent bool,
	censusCapabilityVersion uint32,
	censusID string,
	activePlayerIDs []uint64,
	acknowledgedDepartureIDs []string,
) (*HeartbeatResult, error) {
	if !u.modelB {
		plog.With(ctx).Warnw("msg", "battle_heartbeat_refused", "reason", reasonHeartbeatModelBOff,
			"match_id", matchID, "pod", id.PodName)
		return nil, errcode.New(errcode.ErrInvalidState, "battle Redis authority is not enabled")
	}
	if snapshotPresent && playerCount > int32(len(activePlayerIDs)) {
		plog.With(ctx).Warnw("msg", "battle_heartbeat_refused", "reason", reasonHeartbeatCensusInconsistent,
			"match_id", matchID, "pod", id.PodName, "player_count", playerCount,
			"census", len(activePlayerIDs),
			"hint", "DS 自报在场人数比它给出的完整名单还多,名单不可信,整跳拒绝")
		return nil, errcode.New(errcode.ErrInvalidArg,
			"battle heartbeat player_count=%d exceeds complete owner census=%d",
			playerCount, len(activePlayerIDs))
	}
	// Rolling Model-B writers may encounter an active record written before
	// pod_uid existed.  Backfill on the first heartbeat, before
	// ActivateHeartbeat can atomically turn an empty battle into ABANDONED and
	// make its authority permanent.  A missing/recreated K8s object rejects this
	// heartbeat with zero Redis state transition.
	preflightSnapshot, preflightReadErr := u.authRepo.ReadAuthority(ctx, matchID)
	if preflightReadErr != nil {
		plog.With(ctx).Warnw("msg", "battle_heartbeat_refused", "reason", reasonHeartbeatAuthorityRead,
			"match_id", matchID, "pod", id.PodName, "err", preflightReadErr,
			"hint", "Redis 权威读失败,本跳零状态转移;连续失败会走满 15s 心跳超时被判弃")
		return nil, preflightReadErr
	}
	if preflightSnapshot.BattleFound && preflightSnapshot.Battle != nil &&
		preflightSnapshot.Battle.GetPodUid() == "" &&
		preflightSnapshot.Battle.GetAllocationId() != "" &&
		preflightSnapshot.Battle.GetDsPodName() == id.PodName &&
		preflightSnapshot.Battle.GetGameserverUid() == id.InstanceUID &&
		preflightSnapshot.Battle.GetInstanceEpoch() == id.InstanceEpoch &&
		legacyPodUIDPreflightCredentialMatches(preflightSnapshot, id) {
		if _, err := u.ensureDurableReleasePodUID(ctx, matchID, id.PodName,
			data.BattleExpectedInstance{
				AllocationID: preflightSnapshot.Battle.GetAllocationId(),
				InstanceUID:  id.InstanceUID, InstanceEpoch: id.InstanceEpoch,
			}, preflightSnapshot.Battle.GetReleaseTrack()); err != nil {
			plog.With(ctx).Warnw("msg", "battle_heartbeat_refused", "reason", reasonPodUIDResolveFailed,
				"match_id", matchID, "pod", id.PodName, "uid", id.InstanceUID,
				"epoch", id.InstanceEpoch, "err", err,
				"hint", "滚动升级遗留的无 pod_uid 记录回填失败,本跳零状态转移;详因见同 trace_id 的 battle_pod_uid_preflight_refused")
			return nil, err
		}
	}
	out, err := u.authRepo.ActivateHeartbeat(ctx, matchID, id, data.BattleHeartbeatInput{
		PlayerCount:        playerCount,
		State:              state,
		AuthTTL:            u.dsCredentialTTL,
		BattleTTL:          u.battleTTL(),
		EmptyBattleTimeout: u.cfg.EmptyBattleTimeout.Std(),
		NoShowTimeout:      u.cfg.ResolveNoShowTimeout(),
		StabilityBeats:     u.cfg.ActivationStabilityBeats,
		StabilitySpanMs:    u.cfg.ActivationStabilitySpan.Std().Milliseconds(),
		// 花名册到齐期限(INC-20260813-001)。census 未上报时 data 层整道跳过 ——
		// 拿 player_count 硬猜会把每一局都判成缺员。
		RosterJoinDeadline:  u.effectiveRosterJoinDeadline(),
		RosterJoinArmWindow: u.cfg.ResolveRosterJoinArmWindow(),
		CensusPresent:       snapshotPresent,
		ActivePlayerIDs:     activePlayerIDs,
		// observe→enforce 激活档与策略代(共享谓词 conf.RosterDeadlineShouldAbandon)。
		RosterJoinMode:         u.resolveRosterJoinModeSafe(),
		RosterPolicyGeneration: u.cfg.RosterPolicyGeneration,
	})
	if err != nil {
		// 这是 Model B 心跳唯一的授权/fencing 判定点,原来失败**一条日志都没有**:
		// errBattleAuthStale 是 ErrUnauthorized,access log 只落 rpc_ok=DEBUG。
		// 「DS 在跑但后端认为它没心跳」这类故障以前在后端完全查不出来。
		// 具体是哪一条 fencing 规则拒的,见 data 层同 trace_id 的 battle_ds_auth_rejected。
		plog.With(ctx).Warnw("msg", "battle_heartbeat_refused", "reason", reasonHeartbeatActivateRejected,
			"match_id", matchID, "pod", id.PodName, "uid", id.InstanceUID, "epoch", id.InstanceEpoch,
			"gen", id.Gen, "writer_epoch", id.WriterEpoch, "state", state,
			"player_count", playerCount, "code", int32(errcode.As(err)), "err", err)
		return nil, err
	}
	if out.RosterWouldAbandon {
		// observe 采证 / 代不匹配豁免(与 legacy 路径同一条 WARN,enforce 翻闸前的唯一现场依据)。
		plog.With(ctx).Warnw("msg", "roster_incomplete_would_abandon",
			"match_id", matchID, "pod", id.PodName, "mode", u.resolveRosterJoinModeSafe(),
			"config_policy_generation", u.cfg.RosterPolicyGeneration,
			"deadline_ms", u.cfg.ResolveRosterJoinDeadline().Milliseconds(), "authority", "redis",
			"hint", "mode=enforce 且 battle 代==配置代 才会真判弃;见 decision-revisit §5")
	}
	if out.ActivationPending {
		// 两阶段激活(INC-20260727-001 第三 P0):稳定性证据不足,不发 ACK、不下发指令、
		// 不续 owner 租约(实例尚未服务)、不做离场对账;DS 每 tick 幂等重试 staged 心跳,
		// waitBattleReady 因 auth 仍 BOOTSTRAP 不会放行 ds_addr。
		plog.With(ctx).Debugw("msg", "battle_ds_activation_pending",
			"match_id", matchID, "pod", id.PodName, "gen", id.Gen)
		return &HeartbeatResult{}, nil
	}
	// owner 权威实例租约双写(owner-authority.md migrate ⑥):必须在心跳响应返回前完成,
	// 失败语义(弱/强依赖)见 renewOwnerLeaseGate。
	ownerLeaseTrack := ""
	if out.Battle != nil {
		ownerLeaseTrack = out.Battle.GetReleaseTrack()
	}
	if lerr := renewOwnerLeaseGate(ctx, u.ownerLease, u.ownerLeaseRequired,
		id.PodName, id.InstanceUID, id.InstanceEpoch, ownerLeaseTrack); lerr != nil {
		// 只有 required(contract 强依赖)档会走到这里;弱依赖档由 renewOwnerLeaseGate
		// 内部按窗口限流 Warn 后放行。强依赖失败 = 心跳整跳失败 = DS 会自我 fencing 停玩,
		// 是"玩家突然被踢出副本"的直接原因,必须显式留证。
		plog.With(ctx).Warnw("msg", "battle_heartbeat_refused", "reason", reasonHeartbeatOwnerLease,
			"match_id", matchID, "pod", id.PodName, "uid", id.InstanceUID,
			"epoch", id.InstanceEpoch, "release_track", ownerLeaseTrack, "err", lerr,
			"hint", "owner 权威租约续写是强依赖档;DS 拿不到响应会按 §9.22 自我 fencing")
		return nil, lerr
	}
	// owner 迁移准入代提交(owner-authority.md migrate ③,近似:授权 census 即准入证据;
	// contract 阶段移交 DS Admission 链)。弱依赖,失败/屏障未开都不影响心跳。
	if snapshotPresent && len(activePlayerIDs) > 0 {
		ownerAdmitCensusWeak(ctx, u.ownerAuth, &u.ownerAdmitted, activePlayerIDs,
			ownerTypeBattle, id.PodName, id.InstanceUID, 2*time.Second)
	}
	result := &HeartbeatResult{
		AcceptedTokenGen:      out.Active.Gen,
		AcceptedTokenJTI:      out.Active.JTI,
		AcceptedInstanceUID:   out.Active.InstanceUID,
		AcceptedInstanceEpoch: out.Active.InstanceEpoch,
		AcceptedWriterEpoch:   out.Active.WriterEpoch,
	}
	if out.FirstActivation {
		plog.With(ctx).Infow("msg", "battle_ds_credential_activated", "match_id", matchID,
			"pod", id.PodName, "uid", id.InstanceUID, "epoch", id.InstanceEpoch,
			"gen", id.Gen, "jti", id.JTI, "writer_epoch", id.WriterEpoch)
	}
	if out.Battle != nil && out.Battle.GetAllocationId() != "" {
		orders, reconcileErr := u.repo.ReconcilePlayerDepartures(ctx, matchID,
			data.BattleDepartureSource{
				DSPodName: id.PodName, GameServerUID: id.InstanceUID,
				InstanceEpoch: id.InstanceEpoch, AllocationID: out.Battle.GetAllocationId(),
			}, snapshotPresent, censusCapabilityVersion, censusID,
			activePlayerIDs, acknowledgedDepartureIDs)
		if reconcileErr != nil {
			plog.With(ctx).Warnw("msg", "battle_heartbeat_refused", "reason", reasonHeartbeatDepartureReconcile,
				"match_id", matchID, "pod", id.PodName, "census_present", snapshotPresent,
				"census", len(activePlayerIDs), "acked_departures", len(acknowledgedDepartureIDs),
				"err", reconcileErr,
				"hint", "Battle→Hub 物理离场对账失败,驱逐单发不出去,玩家可能卡在"+
					"「退出副本」上;下一跳心跳会重试")
			return nil, reconcileErr
		}
		result.EvictionOrders = orders
		if len(orders) > 0 {
			// R4:心跳是高频路径,成功侧 Debugw。驱逐单是"玩家退出副本"链上唯一
			// 从后端流向 DS 的指令,查"点了退出没反应"必须能看到它到底发没发出去。
			plog.With(ctx).Debugw("msg", "battle_eviction_orders_issued", "match_id", matchID,
				"pod", id.PodName, "orders", len(orders), "census", len(activePlayerIDs))
		}
	}
	if out.FirstAbandon && out.Battle != nil {
		// 缺员判弃时只罚缺席者:census 就在手边,直接按它与 roster 求差集,
		// 不必让 data 层把名单再回传一遍(它已经用同一份输入做过判定)。
		var absentees []uint64
		if out.RosterIncomplete {
			absentees = rosterAbsentees(out.Battle.PlayerIds, activePlayerIDs)
		}
		finished := u.finishEmptyAbandon(ctx, matchID, out.Battle.DsPodName,
			out.Battle.GameserverUid, out.Battle.PodUid, out.Battle.AllocationId,
			out.Battle.ReleaseTrack, out.Battle.InstanceEpoch,
			out.Battle.PlayerIds, out.Battle.MapId, out.Battle.GameMode,
			!out.Battle.GetEverHadPlayers(), out.RosterIncomplete, absentees)
		result.Command = finished.Command
		return result, nil
	}
	if out.Terminal || out.Battle == nil || out.Battle.State == stateEnded || out.Battle.State == stateAbandoned {
		// R2:一个 if 收敛了四个条件,按判定依据拆成四个 reason —— "DS 被叫停"是
		// 玩家侧「突然被踢回大厅」的直接上游,只知道停了、不知道凭什么停,查不下去。
		stopReason := reasonStopTerminalAuth
		switch {
		case out.Battle == nil:
			stopReason = reasonStopBattleMissing
		case out.Battle.State == stateEnded:
			stopReason = reasonStopBattleEnded
		case out.Battle.State == stateAbandoned:
			stopReason = reasonStopBattleAbandoned
		}
		plog.With(ctx).Warnw("msg", "battle_heartbeat_stop_commanded", "reason", stopReason,
			"match_id", matchID, "pod", id.PodName, "uid", id.InstanceUID,
			"epoch", id.InstanceEpoch, "state", out.Battle.GetState())
		result.Command = commandStop
		return result, nil
	}
	if u.locator != nil && (out.Battle.State == stateReady || out.Battle.State == stateRunning) &&
		out.Battle.DsAddr != "" && len(out.Battle.PlayerIds) > 0 {
		u.refreshBattleLocations(ctx, out.Battle.PlayerIds, matchID, out.Battle.DsAddr)
	}
	return result, nil
}

func legacyPodUIDPreflightCredentialMatches(
	snapshot data.BattleAuthoritySnapshot,
	id data.BattleCredentialIdentity,
) bool {
	if !snapshot.AuthFound || snapshot.Auth == nil || snapshot.Battle == nil ||
		snapshot.Auth.GetMatchId() != snapshot.Battle.GetMatchId() ||
		snapshot.Auth.GetAllocationId() != snapshot.Battle.GetAllocationId() ||
		snapshot.Auth.GetDsPodName() != id.PodName || snapshot.Auth.GetInstanceUid() != id.InstanceUID ||
		snapshot.Auth.GetInstanceEpoch() != id.InstanceEpoch || id.ExpMs <= uint64(time.Now().UnixMilli()) {
		return false
	}
	matches := func(c *dsv1.BattleDSCredential) bool {
		return c != nil && c.GetGen() == id.Gen && c.GetJti() == id.JTI && c.GetExpMs() == id.ExpMs &&
			c.GetKid() == id.Kid && c.GetInstanceUid() == id.InstanceUID &&
			c.GetInstanceEpoch() == id.InstanceEpoch && c.GetTokenSha256() == id.TokenSHA256 &&
			c.GetWriterEpoch() == id.WriterEpoch
	}
	return matches(snapshot.Auth.GetActive()) || matches(snapshot.Auth.GetPending())
}

// Heartbeat 处理 DS 上报(单向 unary,DS 每 5s 调)。刷新 last_heartbeat_ms + 状态。
// 镜像不存在(孤儿 DS)→ 返回 stop 指令让其自行停机。
//
// 已是终态(ended/abandoned)的镜像:直接返回 stop,且**不写回记录**——不刷新
// LastHeartbeatMs / TTL,也不重新 ZAdd active。否则 abandoned 后仍在心跳的 DS(pod
// release 失败 / 延迟终止)会不断推迟 sweep 补偿重试并刷新 BattleTTL 上界,使 active
// 重新可能无限堆积(W4 ⑧ Codex 复审 P1)。
func (u *AllocatorUsecase) Heartbeat(ctx context.Context, matchID uint64, podName string, playerCount int32, state string, tsMs int64) (*HeartbeatResult, error) {
	return u.HeartbeatWithCensus(ctx, matchID, podName, playerCount, state, tsMs, false, nil)
}

// HeartbeatWithCensus 是带在场名单的 legacy 心跳(2026-08-04)。
//
// 相对 Heartbeat 多做两件事,**与 Model B 心跳(HeartbeatAuthorizedWithPlayers)同语义**:
// ①续写 owner 权威实例租约;②对在场玩家代提交 owner Admit。
//
// 为什么必须补:legacy 面此前两件都不做,于是玩家 travel 进战斗 DS、连接也建好了,
// owner 记录却永远停在 PENDING、实例租约永远过期 —— login 的 applyOwnerPlacement 只在
// 「ADMITTED 且租约剩余 > 安全余量」才报 STABLE,客户端因此恒收到
// "post-travel owner target is still PENDING",撑到 30s deadline 弹兜底面板,进不去副本
// (2026-08-04 mode=local 实测,与 hub 侧同一形状的洞)。
//
// 实例身份取自战斗记录的 gameserver_uid / instance_epoch —— 与签票时写进 owner 目标的
// 是同一份权威快照,Admit 的 exact 全等校验因此自洽;取不出(mock 镜像无身份)则两件都
// 不做,保持旧行为。census 只用 DS 实际上报的在场名单,不拿 roster 充数:roster 里可能有
// 尚未连入的玩家,拿它 Admit 等于替一个还没到场的玩家宣称"已准入"(§9.22 禁冒充状态)。
//
// 线上隔离:本函数只在 !RedisAuthorityEnabled() 时被 service 层调用,Model B 恒走
// HeartbeatAuthorizedWithPlayers,生产路径零变更。
func (u *AllocatorUsecase) HeartbeatWithCensus(ctx context.Context, matchID uint64, podName string,
	playerCount int32, state string, tsMs int64, snapshotPresent bool, activePlayerIDs []uint64,
) (*HeartbeatResult, error) {
	// ACK 必须在**进入本体之前**快照:终态 / pod_mismatch / 孤儿三条分支都会调
	// killStrandedDS,而它是异步 goroutine —— 本体返回后再读台账,进程记录可能已被 Release
	// 删掉,同一条 stop 应答带不带 ACK 就成了竞态。先快照后回显,应答与调用时刻的授权事实一致。
	ack, hasACK := u.localCredentialACK(podName)
	res, err := u.heartbeatLegacy(ctx, matchID, podName, playerCount, state, tsMs,
		snapshotPresent, activePlayerIDs)
	if err != nil {
		return nil, err
	}
	// 回显在**所有** legacy 应答上,含 stop/drain:UE 校验不过时会把 Command 连同驱逐单
	// 一起清空(见 data.LocalGameServerAllocator.LocalCredentialACK 注释),
	// 而"让这台 DS 停机"恰恰是最需要送达的一条。
	if hasACK && res != nil && res.AcceptedInstanceUID == "" {
		res.AcceptedTokenGen = ack.Gen
		res.AcceptedTokenJTI = ack.JTI
		res.AcceptedInstanceUID = ack.InstanceUID
		res.AcceptedInstanceEpoch = ack.InstanceEpoch
		res.AcceptedWriterEpoch = ack.WriterEpoch
	}
	return res, nil
}

// localCredentialACK 取本机战斗 DS 的完整凭据身份(mode=local 才有)。
//
// 手法与边界同 hub 侧 HubUsecase.applyLocalCredentialACK:值取自本进程签发、经 env 下发给
// 该 DS 的同一份凭据(不是凭空构造),且严格 fail-closed —— pod 不在台账或五元组不全时
// 一律不回显,绝不糊半截 ACK。
//
// 线上隔离(双重机械门):①只有 !RedisAuthorityEnabled() 的 legacy 心跳会走到这里;
// ②localBattleCredentialSource 仅 data.LocalGameServerAllocator 实现,Agones / Mock
// 分配器类型断言直接不成立,两条路径的应答逐字节不变。
func (u *AllocatorUsecase) localCredentialACK(podName string) (data.BattleCredentialIdentity, bool) {
	if podName == "" {
		return data.BattleCredentialIdentity{}, false
	}
	src, ok := u.alloc.(localBattleCredentialSource)
	if !ok {
		return data.BattleCredentialIdentity{}, false
	}
	return src.LocalCredentialACK(podName)
}

// heartbeatLegacy 是 legacy 心跳本体(镜像 CAS + owner 接线 + 位置续期)。
// 凭据 ACK 回显由唯一调用方 HeartbeatWithCensus 统一收口,避免每条 return 各补一次。
func (u *AllocatorUsecase) heartbeatLegacy(ctx context.Context, matchID uint64, podName string,
	playerCount int32, state string, tsMs int64, snapshotPresent bool, activePlayerIDs []uint64,
) (*HeartbeatResult, error) {
	if matchID == 0 {
		plog.With(ctx).Warnw("msg", "battle_heartbeat_refused", "reason", reasonAllocMatchIDRequired,
			"pod", podName, "authority", "legacy")
		return nil, errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	now := time.Now().UnixMilli()
	// owner 接线所需的 exact 实例身份(CAS 内捕获,与本轮写回的镜像同一快照)。
	var ownerUID, ownerTrack string
	var ownerEpoch uint32

	var becameReady bool
	// 正常结算收口(2026-08-24,见下方 becameEnded 消费点)。本跳心跳把 State 首次写成
	// ended 时置位,同一 CAS 内捕获释放 owner 所需的 exact 实例身份。
	var becameEnded bool
	var endedPod, endedUID string
	var endedPlayers []uint64
	// 断线重连(docs/design/battle-reconnect.md §2.2):捕获对局在 ready/running 时的玩家名单 +
	// ds_addr,心跳成功后续期这些玩家的 BATTLE 位置 TTL。回调可能因 CAS 冲突重跑,故每轮重置。
	var refreshActive bool
	var refreshAddr string
	var refreshPlayers []uint64
	// 空场兜底(2026-07-06):对局活跃但 player_count==0 持续超阈 → 判 abandoned
	// (全员掉线未归 / 客户端从未连入,DS 空转烧资源)。主路径是 DS 侧空场计时器自结算
	// (agones-dev.md §2.4),这里是后端保险。
	//
	// 双阈值(2026-08-07,anti-abuse-scene-entry.md §3.2.1):阈值按 EverHadPlayers 二选一 ——
	// 有人连入过的局要给断线重连留路(默认 5m,远大于 ~30s 重连窗);从未连入的局
	// 根本没人要回来,只需覆盖「DS 报 ready → travel + 连接 + Admission」(默认 150s)。
	var emptyAbandoned bool
	var abandonNoShow bool
	var abandonPod string
	var abandonUID string
	var abandonPodUID string
	var abandonAllocationID string
	var abandonReleaseTrack string
	var abandonInstanceEpoch uint32
	var abandonPlayers []uint64
	var abandonMapID uint32
	var abandonGameMode string
	// 花名册到齐期限(INC-20260813-001):空场两档都只管「一个人都没有」,
	// 拦不住「来了但没来齐」。缺席者单列,判弃时只罚他们。
	var abandonRosterIncomplete bool
	var abandonAbsentees []uint64
	// observe 档的采证出参:到点但本局不允许判弃(mode/代不匹配)时置位,CAS 外打 WARN。
	var rosterWouldAbandon bool
	var rosterWouldAbandonAbsentees []uint64
	var rosterBattleGen uint64
	emptyTimeoutMs := u.cfg.EmptyBattleTimeout.Std().Milliseconds()
	noShowTimeoutMs := u.cfg.ResolveNoShowTimeout().Milliseconds()
	// off 档归零 = 整道闸关死(不记事实、不判弃);与 Model B 路径同一口径(见 helper)。
	rosterDeadlineMs := u.effectiveRosterJoinDeadline().Milliseconds()
	rosterArmWindowMs := u.cfg.ResolveRosterJoinArmWindow().Milliseconds()
	rosterMode := u.resolveRosterJoinModeSafe()
	err := u.repo.UpdateBattleWithLock(ctx, matchID, updateMaxRetry, func(b *dsv1.BattleStorageRecord) error {
		// CAS 冲突重跑时以最后一轮为准,每轮重置出参标记
		refreshActive = false
		becameEnded = false
		endedPod = ""
		endedUID = ""
		endedPlayers = nil
		emptyAbandoned = false
		abandonNoShow = false
		abandonRosterIncomplete = false
		abandonAbsentees = nil
		rosterWouldAbandon = false
		rosterWouldAbandonAbsentees = nil
		rosterBattleGen = 0
		if b.State == stateAllocationUncertain || b.State == stateAllocationReconciling ||
			b.State == stateAllocationEmptyFence ||
			b.State == statePreactiveReleasing || b.State == stateAllocationAbort {
			return errHeartbeatAllocationFenced
		}
		// 已是终态(ended/abandoned):中止写回(哨兵错误),不刷新 TTL/active,令 DS 停机
		if b.State == stateEnded || b.State == stateAbandoned {
			return errHeartbeatTerminal
		}
		// podName 校验:镜像已绑定某个 pod,但上报方是另一个 pod(旧 DS / 孤儿 DS / 重分配残留)→
		// 不写回该镜像,令上报方停机,避免污染新对局(防进错对局的 DS 刷 state/心跳)。
		if b.DsPodName != "" && podName != "" && b.DsPodName != podName {
			return errHeartbeatPodMismatch
		}
		prevState := b.State
		b.LastHeartbeatMs = now
		b.PlayerCount = playerCount
		if state != "" {
			b.State = state
		}
		// warming → ready/running:DS 首次确认就绪,这一跳让 AllocateBattle 得以放行 matchmaker。
		if prevState == stateWarming && (b.State == stateReady || b.State == stateRunning) {
			becameReady = true
		}
		// ready/running → ended:DS 上报对局正常结算的那一跳。**这一跳就是收口点。**
		//
		// 为什么不能等下一跳:owner 释放此前只挂在下方 `errHeartbeatTerminal` 分支上,
		// 而那个分支的触发条件是「心跳到达时记录**已经**是 ended」(见上方 stateEnded 判定
		// 先于 `b.State = state` 写入)—— 也就是要**第二跳**。可 UE DS 侧:
		//   · `state="ended"` 全仓只有 SendEndedHeartbeatAndReturnToHub 发,一局只发一次;
		//   · 周期心跳的 CurrentBattleState 永远是 "running"(SetBattleHeartbeatState 唯一
		//     调用点恒传 "running"),周期 tick 5s;
		//   · ended 的 ACK 回调 HandleEndedHeartbeatComplete 是毫秒级,里面立刻
		//     StopBattleHeartbeat()。
		// ⇒ **第二跳在物理上不可能到达**,该分支在正常结算路径上一次都不会触发。
		// 后果:owner 永远停在 BATTLE 指向一台已结算的 DS,客户端连查 3 次拿不到完整落点
		// → authority_entry_terminal → 打完一局被踢回登录(2026-08-24 Python 栈实测,
		// 两个客户端同时复现;Go 与 Python 此处逐行同构,同源同错)。
		// 本文件 sweepOnce 的 ended 分支注释早已写明「无第二跳」,与该分支自己的注释互相
		// 矛盾 —— sweep 那条才符合实测,另一条已在下方标注更正。
		//
		// exactly-once:上方 stateEnded/stateAbandoned 已返回 errHeartbeatTerminal,故走到
		// 这里 prevState 必非终态,本条件天然只在首次迁入 ended 时成立;CAS 重跑由闭包顶部
		// 的重置保证不串轮。
		//
		// 安全边界(★ 2026-08-24 重写,原文的论证是错的,见 INC-20260824-003):
		//
		// 原文写的是「边界①(须在实例回收确认后才释放)是为 abandoned 写的;ended 不同,
		// 该实例结构上不可能再接纳任何人」。**这个论证站不住**:边界① 防的不是「旧 DS 会
		// 不会再接纳**新**玩家」,而是「旧 DS 是否还持有**旧**玩家」——而释放这个动作本身
		// 会把再入屏障的唯一判据(owner_type=BATTLE + instance_uid)抹掉,屏障塌成 0。
		// 换句话说,当时这里不是「豁免了一道门」,是把门本身删了。
		//
		// 现在成立的理由是另一条:owner 侧的 Release 已改为在释放 BATTLE 归属时把
		// max(now, 本实例租约截止)+skew **盖进 admit_not_before 留存**,BeginTransition
		// 取 max 认回来(owner_repo.go)。屏障不再随归属指针消失,因此**任何时刻释放都不会
		// 让围栏消失**,本调用点也就不再需要「回收已确认」这道前置门。
		// 边界②exact 身份门与③compare-delete 仍由 helper 自身保证。
		//
		// ⚠️ 这条依赖是硬的:若哪天把 owner 的屏障留存改掉,这里必须同步退回「回收确认后
		// 才释放」,否则每一局正常结算都会打开一个脑裂窗口。
		if b.State == stateEnded {
			becameEnded = true
			endedPod = b.GetDsPodName()
			endedUID = b.GetGameserverUid()
			// ★ 必须整片复制:repeated 容器属于本轮事务对象,CAS 重跑会换成新对象。
			endedPlayers = append([]uint64(nil), b.GetPlayerIds()...)
		}
		// 空场跟踪:活跃对局无人 → 盖 EmptySinceMs 起计时;有人回来 → 清零;
		// 持续空场超阈 → 同一 CAS 内直接写 abandoned(与心跳写回原子,无额外竞态窗口)。
		if b.State == stateReady || b.State == stateRunning {
			// 阈值取 EverHadPlayers ? empty : no-show —— EverHadPlayers 一经置位永不清零,
			// 因此只要有一个玩家进来过,本局就永久享受长阈值(5v5 里「9 人在打、1 人掉线」
			// 绝不会走 no-show)。noShowTimeoutMs<=0 回退长阈值:未配差异化时退化成改动前行为,
			// 绝不能让阈值变 0 导致 no-show 局永不回收(fail-safe:宁可回收晚,不可不回收)。
			activeTimeoutMs := emptyTimeoutMs
			if !b.EverHadPlayers && noShowTimeoutMs > 0 {
				activeTimeoutMs = noShowTimeoutMs
			}
			switch {
			case playerCount > 0:
				b.EmptySinceMs = 0
				b.EverHadPlayers = true
			case b.EmptySinceMs == 0:
				b.EmptySinceMs = now
			case activeTimeoutMs > 0 && now-b.EmptySinceMs >= activeTimeoutMs:
				b.State = stateAbandoned
				emptyAbandoned = true
				abandonNoShow = !b.EverHadPlayers
				abandonPod = b.DsPodName
				abandonUID = b.GameserverUid
				abandonPodUID = b.PodUid
				abandonAllocationID = b.AllocationId
				abandonReleaseTrack = b.ReleaseTrack
				abandonInstanceEpoch = b.InstanceEpoch
				abandonPlayers = append([]uint64(nil), b.PlayerIds...)
				abandonMapID = b.MapId
				abandonGameMode = b.GameMode
			}

			// 花名册到齐期限(INC-20260813-001)。与上面的空场两档并列而不是嵌套:
			// 那两档看 player_count==0,本档看 census 与 roster 的差集 —— 事故当天
			// player_count 恒为 5(非 0),空场计时器一次都没起过,3v2 照常打完。
			//
			// 只在**开局阶段**生效(!RosterEverComplete):曾经到齐过的局此后任何掉线
			// 都交回 EmptyBattleTimeout,否则局中掉线会在 deadline 后判弃一场正打着的对局。
			//
			// 武装窗(§5 滚动升级):roster_ever_complete 是**新版**副本才写的记忆,
			// 旧副本手里已经到齐过的局不会有它。滚动升级时那种局一旦被新副本接手,
			// 光看标记会把「局中掉线」误判成「开局没到齐」,45s 后判弃一场**正在打的对局**。
			// 时间窗只保护已经过窗的老局；窗口内旧副本见过到齐、新副本接手时正好缺人的局
			// 仍可能被误武装。它是纵深防御,发布仍须 observe → policy generation enforce。
			if !emptyAbandoned && !b.RosterEverComplete && rosterDeadlineMs > 0 && snapshotPresent &&
				rosterGateArmable(b.AllocatedAtMs, now, rosterArmWindowMs) {
				absent := rosterAbsentees(b.PlayerIds, activePlayerIDs)
				switch {
				case len(absent) == 0:
					// 全员同时在场过一次就永久豁免。
					b.RosterEverComplete = true
					b.RosterIncompleteSinceMs = 0
				case b.RosterIncompleteSinceMs == 0:
					b.RosterIncompleteSinceMs = now
				case now-b.RosterIncompleteSinceMs >= rosterDeadlineMs:
					// 到点。能不能**真**判弃由激活档 + 策略代把关(共享谓词,与 data 层同一份):
					// observe 只采证;enforce 也只对「当前配置代创建」的局动手 ——
					// legacy(gen=0)与旧代局永不执行,滚动升级窗口的误判弃在机制上不可达。
					if !conf.RosterDeadlineShouldAbandon(rosterMode, u.cfg.RosterPolicyGeneration, b.GetRosterPolicyGeneration()) {
						rosterWouldAbandon = true
						rosterWouldAbandonAbsentees = absent
						rosterBattleGen = b.GetRosterPolicyGeneration()
						break
					}
					b.State = stateAbandoned
					emptyAbandoned = true
					abandonRosterIncomplete = true
					abandonNoShow = false // 罚只记缺席者,不按 no-show 全员记
					abandonAbsentees = absent
					abandonPod = b.DsPodName
					abandonUID = b.GameserverUid
					abandonPodUID = b.PodUid
					abandonAllocationID = b.AllocationId
					abandonReleaseTrack = b.ReleaseTrack
					abandonInstanceEpoch = b.InstanceEpoch
					abandonPlayers = append([]uint64(nil), b.PlayerIds...)
					abandonMapID = b.MapId
					abandonGameMode = b.GameMode
				}
			}
		}
		// 对局活跃(ready/running,且未被空场超时判弃):记下玩家名单 + ds_addr,供心跳后续期 BATTLE 位置。
		if b.State == stateReady || b.State == stateRunning {
			refreshActive = true
			refreshAddr = b.DsAddr
			refreshPlayers = append(refreshPlayers[:0], b.PlayerIds...)
			ownerUID, ownerEpoch, ownerTrack = b.GameserverUid, b.InstanceEpoch, b.ReleaseTrack
		}
		return nil
	}, u.battleTTL())

	if err != nil {
		switch {
		case errors.Is(err, errHeartbeatAllocationFenced):
			plog.With(ctx).Warnw("msg", "heartbeat_allocation_fenced_stop", "match_id", matchID, "pod", podName)
			return &HeartbeatResult{Command: commandStop}, nil
		case errors.Is(err, errHeartbeatTerminal):
			// 终态 DS:不写回、通知停机,补偿重试与 TTL 上界不受影响
			plog.With(ctx).Infow("msg", "heartbeat_terminal_stop", "match_id", matchID, "pod", podName)
			u.killStrandedDS(ctx, matchID, podName, "terminal")
			// owner 精确释放(legacy 正常结算的**真实**收口点,2026-08-04;INC-20260804-001 缺口⑦)。
			//
			// ⚠️ 2026-08-24 更正:下面这段「DS 转 ended 并继续上报心跳」的前提**不成立**。
			// UE DS 在 ended terminal ACK 的毫秒级回调里就 StopBattleHeartbeat,而周期心跳
			// 的 state 恒为 "running"、tick 5s —— 第二跳物理上不可能到达,本分支在正常结算
			// 路径上一次都不会触发(实测 heartbeat_terminal_stop 零条)。正常结算的真实收口点
			// 已上移到写回成功后的 becameEnded 分支。本分支保留:它仍覆盖「终态后仍有心跳
			// 到达」的情形(补偿重试、孤儿 DS、未来改回持续心跳的 DS),释放是幂等的
			// (exact 身份门 + compare-delete),重复调用不会误伤。
			//
			// 为什么在这里而不是 ReleaseBattle:legacy 面对局正常结束(含 PVE 主动退出)后,
			// battle_result 记账 + outbox 投递完成,DS 转 ended 并继续上报心跳,由**本分支**
			// 判终态并回收 DS —— `ReleaseBattle` 在这条流程里**一次都不会被调用**(实测计数 0)。
			// 此前把释放接在 ReleaseBattle 上是接错了位置,owner 因此仍停在 BATTLE/ADMITTED
			// 指向一台已销毁的 DS:login 的 query-first 一直把玩家指回去,而对局记录已终态,
			// 客户端拿到的 TARGET 缺 match_id → `incomplete owner identity` → 判弃回登录,
			// 玩家打完副本回不了大厅(2026-08-04 两轮实测)。
			// ⚠️ 2026-08-24 订正:原文"撞 30s 线"是错的。判弃不由时间窗触发,而由**连续次数**
			// 触发 —— `IncompleteTargetStreakLimit = 3`(MyDsRecoveryCoordinator.h),按退避
			// min(8,2^clamp(n,0,3))*rand(0.75,1.25) 折算约 2.25~3.75s;`AuthorityWaitWindowSeconds`
			// 今天是 300.0 不是 30。估算窗口时别再按 30s 算。
			//
			// 时序满足 ownerReleaseAbandonedPlayersWeak 的安全边界①:killStrandedDS 已发起
			// 本实例回收,此后释放不会在旧 DS 仍可服务时放行新归属。边界②exact 身份门与
			// ③compare-delete 由该函数自身保证 —— 玩家已被分到别处时双重跳过,不误伤。
			// 记录读取失败或无实例身份时整体跳过(退化为改动前行为),绝不用半截身份去删归属。
			//
			// **作用域是刻意的,不是漏了隔离门(2026-08-04 用户拍板)**:本分支的唯一门是
			// service 层的 `!RedisAuthorityEnabled()`,没有像 localTicketBinding /
			// localInstanceIdentitySource / localBattleRosterSink 那样再叠一道类型断言。
			// 后果:标准生产姿态(authority_mode=redis ⇒ 强制 agones+enforce,见 main.go
			// battle_model_b_invalid_activation 门)恒走 HeartbeatAuthorizedWithPlayers,
			// 本段不可达。
			//
			// ⚠️ 2026-08-24 订正(INC-20260824-003 §8):原文称「**Agones + legacy
			// authority_mode 的灰度部署会执行到这里**,那正是我们要的」——**该形态并不存在
			// 可达路径**,这段是为死代码写的辩护:
			//   · legacy 面 `battle.GameserverUid` 只在 modelB(authoritative != nil)或
			//     `u.alloc.(localInstanceIdentitySource)`(即 mode=local)才回填;
			//   · Agones + legacy 两者皆不成立 ⇒ uid 恒空 ⇒ 若开了 owner,`ownerBeginPlayers`
			//     在 READY 交付前就会因 target 不完整失败(OwnerTarget.Complete 要求 uid 非空),
			//     玩家**根本进不了战斗**;若没开 owner 则 `auth == nil`,helper 首行早退。
			// ⇒ 两种情况下本段都不对应任何真实生效路径。作用域实际被限制在 mode=local。
			// 将来若要给 Agones + legacy 回填 gameserver_uid,必须先重新评估 INC-20260824-003
			// 的屏障坍塌(那会把它从 dev-only 变成生产可达)。
			if terminal, found, gerr := u.repo.GetBattle(ctx, matchID); gerr == nil && found {
				ownerReleaseAbandonedPlayersWeak(ctx, u.ownerAuth, terminal.GetPlayerIds(),
					terminal.GetDsPodName(), terminal.GetGameserverUid(), 2*time.Second)
			} else if gerr != nil {
				plog.With(ctx).Warnw("msg", "terminal_owner_release_read_failed",
					"match_id", matchID, "pod", podName, "err", gerr,
					"hint", "owner 未释放,玩家可能回不了大厅;下一跳心跳会重试")
			}
			return &HeartbeatResult{Command: commandStop}, nil
		case errors.Is(err, errHeartbeatPodMismatch):
			// pod 不匹配:不写回镜像,令旧/孤儿 DS 停机(防污染新对局)
			plog.With(ctx).Warnw("msg", "heartbeat_pod_mismatch", "match_id", matchID, "pod", podName)
			u.killStrandedDS(ctx, matchID, podName, "pod_mismatch")
			return &HeartbeatResult{Command: commandStop}, nil
		case errcode.As(err) == errcode.ErrDSPodNotFound:
			// 孤儿 DS:无镜像,通知停机
			plog.With(ctx).Warnw("msg", "heartbeat_orphan_ds", "match_id", matchID, "pod", podName)
			u.killStrandedDS(ctx, matchID, podName, "orphan")
			return &HeartbeatResult{Command: commandStop}, nil
		default:
			// 上面三条哨兵之外的失败(乐观锁重试耗尽、Redis 不可用等)以前直接透传,
			// 后端零日志。它会让本跳心跳不落库,连续发生就是 15s 判弃的前因。
			plog.With(ctx).Warnw("msg", "battle_heartbeat_refused", "reason", reasonHeartbeatUpdateFailed,
				"match_id", matchID, "pod", podName, "state", state,
				"player_count", playerCount, "authority", "legacy",
				"code", int32(errcode.As(err)), "err", err)
			return nil, err
		}
	}
	if becameReady {
		// R1:warming → ready/running 的首次迁移,是"DS 起来了"的唯一凭证,也是
		// AllocateBattle 得以放行 matchmaker 的那一跳。每对局至多一条,升 Infow。
		plog.With(ctx).Infow("msg", "battle_ds_heartbeat_ready", "match_id", matchID,
			"pod", podName, "state", state, "player_count", playerCount, "authority", "legacy")
	}
	if rosterWouldAbandon {
		// observe 采证 / 代不匹配豁免:到点了但本局不允许真判弃。这条 WARN 是激活协议的
		// 唯一现场依据 —— enforce 翻闸前,运维靠它判断 45s 会误伤多少「本可成立」的局
		// (每 5s 心跳一条,持续到人补齐或对局被其它闸收走;量大本身就是「别翻 enforce」的证据)。
		plog.With(ctx).Warnw("msg", "roster_incomplete_would_abandon",
			"match_id", matchID, "pod", podName, "mode", rosterMode,
			"battle_policy_generation", rosterBattleGen,
			"config_policy_generation", u.cfg.RosterPolicyGeneration,
			"absent_players", rosterWouldAbandonAbsentees,
			"deadline_ms", rosterDeadlineMs, "authority", "legacy",
			"hint", "mode=enforce 且 battle 代==配置代 才会真判弃;见 decision-revisit §5")
	}
	if becameEnded {
		// 正常结算收口(2026-08-24):对局已终态,把仍指向本实例的 BATTLE 归属释放掉,
		// 玩家的恢复查询才会落到「无归属 → 首次进场链 → Hub」。
		//
		// **同步调用**:释放要发生在本跳心跳响应返回之前 —— DS 一拿到 ended ACK 就
		// StopBattleHeartbeat,成功路径上**再没有第二跳**,这是唯一一次机会。
		//
		// ⚠️ 2026-08-24 订正,原注释这里给的两条理由都不准确:
		//   · 「客户端结算后紧接着的 GetResumeContext」—— 正常结算主路径**不走**
		//     GetResumeContext:terminal ACK → ClientPandoraBattleSettledReturnToHub →
		//     UMyMatchModel::ReturnToHubDs → IssueDSTicketScoped("hub", fence),而那道门
		//     (guardHubRouteAgainstActiveBattle)读的是 locator presence + battle 投影,
		//     **全程不读 owner**。GetResumeContext 只是失败/重启后的兜底路。
		//     ⇒ 本释放修的是**兜底路**(以及释放后新 Begin 的屏障口径),不是主路。
		//   · 「fail-closed 只等 3 拍,约 6~10s」—— 3 拍对(IncompleteTargetStreakLimit=3),
		//     秒数错:按退避 min(8,2^clamp(n,0,3))*rand(0.75,1.25) 折算约 2.25~3.75s。
		//
		// 弱依赖:该函数内部失败只告警。sweepOnce 的 ended 分支会再释放一次,但那要晚一个
		// HeartbeatTimeout(dev 档 120s)—— **体验口径上救不回本次**,只保证最终收敛。
		//
		// ★ 不动本跳的返回值:DS 正是靠这跳的成功响应确认 ended ACK。而且 UE 侧 ended 那跳
		//   的回调 HandleEndedHeartbeatComplete 根本不读 Result.Command,回 stop 也会被
		//   静默丢弃 —— 不能把 stop 当作生效通道。
		plog.With(ctx).Infow("msg", "battle_ended_owner_release", "match_id", matchID,
			"pod", endedPod, "players", len(endedPlayers), "authority", "legacy")
		ownerReleaseAbandonedPlayersWeak(ctx, u.ownerAuth, endedPlayers,
			endedPod, endedUID, 2*time.Second)
	}
	if emptyAbandoned {
		// 空场 / 缺员超时判弃:回收 pod + 投递补偿 + 移出 active,回 stop 指令令 DS 停机。
		return u.finishEmptyAbandon(ctx, matchID, abandonPod, abandonUID,
			abandonPodUID, abandonAllocationID, abandonReleaseTrack, abandonInstanceEpoch,
			abandonPlayers, abandonMapID, abandonGameMode, abandonNoShow,
			abandonRosterIncomplete, abandonAbsentees), nil
	}
	// owner 权威实例租约双写 + 在场玩家准入代提交(与 Model B 心跳同语义,见函数注释)。
	// 时序同 owner-authority.md §4:必须在心跳响应返回前完成租约续写。
	// 身份取不出(mock 镜像无实例身份)则整段跳过,保持旧行为。
	if refreshActive && ownerUID != "" {
		if lerr := renewOwnerLeaseGate(ctx, u.ownerLease, u.ownerLeaseRequired,
			podName, ownerUID, ownerEpoch, ownerTrack); lerr != nil {
			return nil, lerr
		}
		// 弱依赖:失败/屏障未开都不影响心跳(下一跳重试)。
		// 优先用 DS 真实上报的在场名单(exact);拿不到时按下面的 local-off-v1 兜底。
		admitPlayers := activePlayerIDs
		if !snapshotPresent || len(admitPlayers) == 0 {
			// local-off-v1 兜底:该档位下 census **结构上不可能成立** —— UE 的
			// BuildCompleteBattlePlayerCensus 要求每个 owner 的 claims 满足
			// IsCompleteBattleOwnerClaims(dst_ver==2 且带 ds_pod/ds_uid/ds_epoch/allocation_id),
			// 而本档位刻意只签 HS256 legacy 战斗票(GrpcDSAllocator.SignBattleTicket 注释:
			// UE DS 硬锁 HS256LocalOff、不交叉接受 v2),legacy 战斗票又**带不了**实例绑定
			// (pkg/auth.signDSTicket 明令 binding 只许 hub 票用)。三者叠加 = census 永远缺席
			// = owner 永远停在 PENDING = 客户端进图后永远等不到 STABLE,30s 后弹"重连时间较长"
			// (2026-08-04 实测:副本内其实能正常战斗,只是入场确认不了)。
			//
			// 兜底用「本对局花名册」代替在场名单,并要求 playerCount>0(DS 确认确有人连入)。
			// 这是**近似**,与 Model B 的 census(exact)有别:名册里可能有还在 travel 的玩家,
			// 会被提前判 ADMITTED。可接受的理由:①owner 侧 Admit 仍做 exact 身份 CAS
			// (pod/uid/epoch/operation_id 全等)+ admit_not_before 屏障,归属指向别处的玩家
			// 一律被拒,不会凭空造出第二个 owner;②Begin 早已把归属搬到本实例,旧 DS 此刻
			// 已无归属,提前 ADMITTED 不产生双 DS;③本路径只在 legacy(非 Model B)心跳上,
			// 生产恒走 HeartbeatAuthorizedWithPlayers 的 exact census,一字不受影响。
			// 一旦 local 档位将来能签带绑定的战斗票,census 自然非空,兜底自动让位。
			if playerCount > 0 {
				admitPlayers = refreshPlayers
			}
		}
		if len(admitPlayers) > 0 {
			ownerAdmitCensusWeak(ctx, u.ownerAuth, &u.ownerAdmitted, admitPlayers,
				ownerTypeBattle, podName, ownerUID, 2*time.Second)
		}
	}
	// 断线重连(docs/design/battle-reconnect.md §2.2):对局活跃时续期玩家 BATTLE 位置 TTL,
	// 使玩家整局在线期间 login 都能检测到"在战斗中",支持中途掉线重登直连回原 battle DS。
	if u.locator != nil && refreshActive && refreshAddr != "" && len(refreshPlayers) > 0 {
		u.refreshBattleLocations(ctx, refreshPlayers, matchID, refreshAddr)
	}
	return &HeartbeatResult{Command: commandNone}, nil
}

// rosterAbsentees 返回 roster 里没出现在 census 中的玩家(保持 roster 原序,便于日志比对)。
//
// resolveRosterJoinModeSafe 取到齐期限激活档;配置非法时滑向 observe(只采证,绝不判弃)。
// main 启动时已 fail-fast,此兜底正常不可达 —— 留它是因为本函数在判弃路径上,
// 不可达分支的失败方向也必须是安全侧。
func (u *AllocatorUsecase) resolveRosterJoinModeSafe() string {
	mode, err := u.cfg.ResolveRosterJoinMode()
	if err != nil {
		return conf.RosterJoinModeObserve
	}
	return mode
}

// effectiveRosterJoinDeadline 是「激活档折算后的」到齐期限:off 档归零(整道闸关死,
// 连事实都不记)。legacy 与 Model B 两条心跳路径必须用同一个口径,否则 off 只关一半。
func (u *AllocatorUsecase) effectiveRosterJoinDeadline() time.Duration {
	if u.resolveRosterJoinModeSafe() == conf.RosterJoinModeOff {
		return 0
	}
	return u.cfg.ResolveRosterJoinDeadline()
}

// 输入语义严格:census 必须是 DS 上报的**真实在场名单**,不是 roster 也不是数量。
// 拿数量判只能得到「少了几个」,说不出少的是谁 —— 而「只罚缺席者、不罚在场的人」
// 恰恰要求点名到人。调用方负责保证 census 确实存在(snapshotPresent),
// 本函数不替它兜底:census 缺席时返回「全员缺席」会把每一局都判弃。
func rosterAbsentees(roster, census []uint64) []uint64 {
	if len(roster) == 0 {
		return nil
	}
	present := make(map[uint64]struct{}, len(census))
	for _, id := range census {
		present[id] = struct{}{}
	}
	var absent []uint64
	for _, id := range roster {
		if _, ok := present[id]; !ok {
			absent = append(absent, id)
		}
	}
	return absent
}

// recordNoShowPenalties 对 no-show 判弃的 roster 全员记账,并按温和档指数退避布罚:
// 记账窗口(默认 10min)内首 NoShowPenaltyFree(默认 1)次免罚,之后 base×2^k 封顶 cap
// (默认 30s→60s→120s→240s→5min)。全程 fail-open:任何 Redis 错误只 Warn,
// 绝不阻断判弃收尾(记罚是背压,判弃回收才是正确性)。
// 惩罚的执行点在 matchmaker StartMatch(读 noshowcd 键拒绝 + 可见倒计时)。
func (u *AllocatorUsecase) recordNoShowPenalties(ctx context.Context, matchID uint64, playerIDs []uint64) {
	if u.noShowRecorder == nil {
		return
	}
	window := u.cfg.NoShowLedgerWindow.Std()
	base := u.cfg.NoShowPenaltyBase.Std()
	if window <= 0 || base <= 0 {
		return
	}
	penaltyCap := u.cfg.NoShowPenaltyCap.Std()
	free := int64(u.cfg.NoShowPenaltyFree)
	if free < 0 {
		free = 0 // 负值 = 严格档:首次即罚(conf 注释契约)
	}
	for _, pid := range playerIDs {
		count, err := u.noShowRecorder.RecordNoShow(ctx, pid, window)
		if err != nil {
			plog.With(ctx).Warnw("msg", "noshow_ledger_record_failed",
				"match_id", matchID, "player_id", pid, "err", err)
			continue
		}
		over := count - free
		if over <= 0 {
			continue // 免罚额度内(偶发一次不惩罚,温和档核心)
		}
		// base << (over-1),指数移位钳到 62 位内防溢出;cap 兜底。
		shift := over - 1
		if shift > 8 {
			shift = 8 // 30s<<8 = 128min,远超任何合理 cap,再大没有意义
		}
		penalty := base << shift
		if penaltyCap > 0 && penalty > penaltyCap {
			penalty = penaltyCap
		}
		if err := u.noShowRecorder.ArmPenalty(ctx, pid, penalty); err != nil {
			plog.With(ctx).Warnw("msg", "noshow_penalty_arm_failed",
				"match_id", matchID, "player_id", pid, "err", err)
			continue
		}
		// 拒绝与惩罚走日志定位玩家(§4.4:player_id 绝不能进 metrics label)。
		plog.With(ctx).Warnw("msg", "noshow_penalty_armed",
			"match_id", matchID, "player_id", pid, "window_count", count, "penalty", penalty.String())
	}
}

// finishEmptyAbandon 完成空场超时判弃的收尾(abandoned 已在心跳 CAS 内写入镜像):
// 回收 pod + 投递 ds.lifecycle{ABANDONED} 补偿事件 + 移出 active,并令 DS 停机。
//
// 投递失败的重试闭环(不变量 §4 可靠补偿):投递失败时对局保留在 active；Model B
// 的 TERMINATING 两键保持永久，直到外部 release 与 lifecycle 都明确成功后才恢复
// 有界 TTL；legacy 仍以原 BattleTTL 为天然上界。
func (u *AllocatorUsecase) finishEmptyAbandon(
	ctx context.Context,
	matchID uint64,
	podName, instanceUID, podUID, allocationID, releaseTrack string,
	instanceEpoch uint32,
	playerIDs []uint64,
	mapID uint32,
	gameMode string,
	noShow bool,
	rosterIncomplete bool,
	absentees []uint64,
) *HeartbeatResult {
	// reason 区分三档:no_show = 从头到尾没人连入(可能是刷进出副本的滥用,也可能是
	// 客户端进场链路断了);all_disconnected = 有人打过但全员掉线未归;
	// roster_incomplete = 来了但没来齐(INC-20260813-001)。
	// 三者的排查方向完全不同,日志必须能分开统计(否则滥用会被当成“玩家网络差”)。
	reason, timeout := "all_disconnected", u.cfg.EmptyBattleTimeout.Std()
	switch {
	case rosterIncomplete:
		reason, timeout = "roster_incomplete", u.cfg.ResolveRosterJoinDeadline()
	case noShow:
		reason, timeout = "no_show", u.cfg.ResolveNoShowTimeout()
	}
	plog.With(ctx).Warnw("msg", "battle_abandoned_empty_timeout",
		"match_id", matchID, "pod", podName, "reason", reason, "empty_timeout", timeout.String(),
		"roster", len(playerIDs), "absentees", absentees)
	switch {
	case rosterIncomplete:
		// **只罚缺席者**。在场那几位是受害者:他们按时连进来了,局却因为别人没到而作废,
		// 再给他们记一笔退避等于让「队友掉线」变成自己的惩罚(§6 第 8 项的温和档精神)。
		u.recordNoShowPenalties(ctx, matchID, absentees)
	case noShow:
		// no-show 记账 → 进入侧退避(§6 第 8 项)。放在判弃 CAS 已提交之后的收口点:
		// FirstAbandon/emptyAbandoned 保证每局至多进入一次,天然不重复记账。
		// 只罚 no_show——all_disconnected 是「打过但掉线」,可能是网络问题,不记。
		u.recordNoShowPenalties(ctx, matchID, playerIDs)
	}
	var expected *data.AuthoritativeGameServerAllocation
	if u.modelB {
		fence := data.BattleExpectedInstance{
			AllocationID: allocationID, InstanceUID: instanceUID, InstanceEpoch: instanceEpoch,
		}
		resolvedPodUID, preflightErr := u.ensureDurableReleasePodUID(
			ctx, matchID, podName, fence, releaseTrack)
		if preflightErr != nil {
			plog.With(ctx).Warnw("msg", "empty_abandon_pod_uid_preflight_failed",
				"match_id", matchID, "pod", podName, "err", preflightErr)
			return &HeartbeatResult{Command: commandStop}
		}
		podUID = resolvedPodUID
		terminated, terr := u.authRepo.TerminateExpected(
			ctx, matchID, fence, stateAbandoned, u.dsCredentialTTL, u.battleTTL())
		if !terminated {
			plog.With(ctx).Warnw("msg", "empty_abandon_terminate_fence_failed", "match_id", matchID,
				"pod", podName, "err", terr)
			return &HeartbeatResult{Command: commandStop}
		}
		if terr != nil {
			plog.With(ctx).Warnw("msg", "empty_abandon_index_cleanup_failed", "match_id", matchID, "err", terr)
		}
		expected = &data.AuthoritativeGameServerAllocation{
			PodName: podName, InstanceUID: instanceUID, AllocationID: allocationID,
			PodUID: podUID, InstanceEpoch: instanceEpoch, ReleaseTrack: releaseTrack,
		}
	}
	if rerr := u.releaseGameServer(ctx, matchID, podName, expected); rerr != nil {
		plog.With(ctx).Warnw("msg", "empty_abandon_release_failed", "match_id", matchID, "pod", podName, "err", rerr)
		if u.modelB {
			// 永久 TERMINATING fence 留在 Redis；不得继续投递/Expire 后让墓碑消失。
			return &HeartbeatResult{Command: commandStop}
		}
	}
	if u.deliverAbandoned(ctx, matchID, podName, instanceUID, playerIDs, mapID, gameMode) {
		if u.modelB {
			target := placement.Target{
				PodName: podName, InstanceUID: instanceUID, InstanceEpoch: instanceEpoch,
				AllocationID: allocationID, ReleaseTrack: releaseTrack,
			}
			if err := u.lifecycleProofRepo.RecordAllocationLifecyclePublished(ctx, matchID, target); err != nil {
				plog.With(ctx).Warnw("msg", "empty_abandon_lifecycle_marker_failed",
					"match_id", matchID, "allocation_id", allocationID, "err", err)
				return &HeartbeatResult{Command: commandStop}
			}
			fence := data.BattleExpectedInstance{
				AllocationID: allocationID, InstanceUID: instanceUID, InstanceEpoch: instanceEpoch,
			}
			if expired, eerr := u.authRepo.ExpireTerminatedExpected(
				ctx, matchID, fence, u.dsCredentialTTL, u.battleTTL()); eerr != nil || !expired {
				plog.With(ctx).Warnw("msg", "empty_abandon_expire_failed", "match_id", matchID,
					"expired", expired, "err", eerr)
			}
		} else if eerr := u.repo.ExpireBattle(ctx, matchID, u.battleTTL()); eerr != nil {
			plog.With(ctx).Warnw("msg", "empty_abandon_expire_failed", "match_id", matchID, "err", eerr)
		}
	}
	return &HeartbeatResult{Command: commandStop}
}

// refreshBattleLocations 异步续期一批玩家的 BATTLE 位置 TTL(断线重连,弱依赖)。
//
// fire-and-forget:不给心跳响应加尾延迟;用 plog.Detach(只复制 trace_id 等日志字段,
// 满足不变量 §8 写带 trace_id,同时剥离心跳 RPC 的取消与 server transport——下游是
// 挂 Trace middleware 的 gRPC client,不得继承入站请求 transport)+ 短超时防 locator
// 卡死泄漏 goroutine。失败只 Warn,绝不影响心跳 / 对局。
func (u *AllocatorUsecase) refreshBattleLocations(ctx context.Context, playerIDs []uint64, matchID uint64, dsAddr string) {
	players := append([]uint64(nil), playerIDs...) // 拷贝,脱离调用方切片复用
	go func() {
		rctx, cancel := context.WithTimeout(plog.Detach(ctx), locationRefreshTimeout)
		defer cancel()
		if err := u.locator.RefreshBattleLocations(rctx, players, matchID, dsAddr); err != nil {
			plog.With(rctx).Warnw("msg", "refresh_battle_locations_failed", "match_id", matchID, "err", err)
		}
	}()
}

// ── RPC 4:ListBattles ─────────────────────────────────────────────────────────

// ListBattles 列出当前战斗实例,stateFilter 非空时按 state 过滤。
func (u *AllocatorUsecase) ListBattles(ctx context.Context, stateFilter string) ([]*dsv1.BattleInfo, error) {
	matchIDs, err := u.repo.RangeActiveBattles(ctx)
	if err != nil {
		plog.With(ctx).Warnw("msg", "battle_list_refused", "reason", reasonHeartbeatAuthorityRead,
			"state_filter", stateFilter, "err", err)
		return nil, err
	}
	out := make([]*dsv1.BattleInfo, 0, len(matchIDs))
	// 读失败 / 索引残留在循环里被静默 continue 掉。逐条打会刷屏(active 集合可达千级),
	// 故按 R4 聚合:只记数量 + 首错 + 一个样本,循环后一条。
	var readFailed, indexOrphan int
	var firstErr error
	var sampleMatchID uint64
	for _, mid := range matchIDs {
		b, found, gerr := u.repo.GetBattle(ctx, mid)
		if gerr != nil || !found {
			if gerr != nil {
				readFailed++
				if firstErr == nil {
					firstErr, sampleMatchID = gerr, mid
				}
			} else {
				indexOrphan++
				if sampleMatchID == 0 {
					sampleMatchID = mid
				}
			}
			continue
		}
		if stateFilter != "" && b.State != stateFilter {
			continue
		}
		out = append(out, &dsv1.BattleInfo{
			MatchId:       b.MatchId,
			DsPodName:     b.DsPodName,
			DsAddr:        b.DsAddr,
			State:         b.State,
			PlayerCount:   b.PlayerCount,
			AllocatedAtMs: b.AllocatedAtMs,
		})
	}
	if readFailed > 0 || indexOrphan > 0 {
		plog.With(ctx).Warnw("msg", "battle_list_partial", "active", len(matchIDs),
			"returned", len(out), "read_failed", readFailed, "index_orphan", indexOrphan,
			"state_filter", stateFilter, "sample_match_id", sampleMatchID, "first_err", firstErr,
			"hint", "index_orphan = active ZSET 有项但权威镜像已不在(TTL 到期/已删),由 sweep 自愈")
	}
	return out, nil
}

// ── 后台心跳超时扫描 ──────────────────────────────────────────────────────────

// RunHeartbeatSweep 启动后台心跳超时扫描,直到 ctx 取消(不变量 §4)。
// SweepWriterLease 是心跳扫描循环的领导权来源(*writerlease.Lease 满足)。
//
// **边界(必须理解后再扩大用途)**:本接口只用来给 sweepOnce 去重,不是防脑裂手段,
// 因此 sweep 的写**不需要**像 hub_allocator 那样把 fencing token 带进存储事务。
// 两者的差别是结构性的:
//   - hub_allocator 的写携带**进程内累积的账本意图**(assignment / 容量),迟到的旧写者
//     会把陈旧意图写进权威,故必须在同事务比较单调 token;
//   - ds_allocator 的 sweep **不携带跨轮次权威状态**:每轮从 Redis 重读 authority 快照,
//     判据只是"当前 last_heartbeat_ms 是否超时",写是同一事务内的 CAS。一个迟到的旧
//     leader 做的是与新 leader **逐字相同的重算**,不存在"陈旧意图"这一类错误。它仅有的
//     进程内状态(sweepDeferUntil / ownerAdmitted)已被显式定义为非权威调度提示与可重建
//     缓存(见各自注释与 INC-20260724-001 整改结论),丢失或重复只影响一次多余往返。
//
// 所以这里选举的收益是**降载与去重**,防脑裂由既有的按 match 凭据 CAS 承担 —— 这正是
// §9.21「单写者循环共享 leader election」与「可并行 worker 用幂等 CAS」的分界线。
// 若将来 sweep 开始携带跨轮次权威意图,必须同步升级为存储级 fencing,不能只靠本接口。
type SweepWriterLease interface {
	// Current 返回 (fencing token, 是否持有领导权)。本接口只消费第二个返回值。
	Current() (uint64, bool)
}

// SetSweepWriterLease 注入心跳扫描的领导权来源;nil = 不启用(本副本无条件扫描)。
// 只允许在启动装配期调用(RunHeartbeatSweep 之前)。
func (u *AllocatorUsecase) SetSweepWriterLease(l SweepWriterLease) { u.sweepLease = l }

// sweepIsLeader 判定本轮是否由本副本执行扫描,并把领导权跃迁打成日志。
// 未注入租约时恒为 true(单副本 Recreate 的历史形态)。
func (u *AllocatorUsecase) sweepIsLeader(ctx context.Context) bool {
	if u.sweepLease == nil {
		return true
	}
	token, held := u.sweepLease.Current()
	if held != u.sweepLeaseHeld {
		u.sweepLeaseHeld = held
		if held {
			plog.With(ctx).Infow("msg", "heartbeat_sweep_leadership_acquired", "token", token)
		} else {
			// 让位不是故障:热备副本继续服务 Heartbeat / AllocateBattle,只是不扫描。
			// 真正的异常是"全集群无人持有",由 writerlease Health()/指标暴露。
			plog.With(ctx).Infow("msg", "heartbeat_sweep_leadership_released",
				"hint", "本副本转热备,仅暂停心跳超时扫描;RPC 路径不受影响")
		}
	}
	return held
}

func (u *AllocatorUsecase) RunHeartbeatSweep(ctx context.Context) {
	ticker := time.NewTicker(u.cfg.SweepInterval.Std())
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "heartbeat_sweep_started",
		"interval", u.cfg.SweepInterval.String(), "timeout", u.cfg.HeartbeatTimeout.String(),
		"writer_lease_wired", u.sweepLease != nil)
	// Recover the derived index immediately on process start. Waiting for the
	// first ticker would leave a lost permanent tombstone invisible for a full
	// sweep interval after every restart.
	//
	// 接了继任租约时首扫必须过领导权门:竞选是异步的,启动瞬间通常尚未当选,此时
	// 无条件首扫等于在滚动升级重叠窗口里多一个并发扫描者。跳过无损 —— 当选后的
	// 第一个 tick 就会补上,间隔仅一个 SweepInterval(默认 5s)。
	if u.sweepIsLeader(ctx) {
		if err := u.sweepOnce(ctx); err != nil {
			plog.With(ctx).Warnw("msg", "heartbeat_initial_sweep_failed", "err", err)
		}
	}
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "heartbeat_sweep_stopped")
			return
		case <-ticker.C:
			u.heartbeatSweepTick(ctx)
		}
	}
}

// heartbeatSweepTick 单个清扫 tick(独立函数使 recover 作用域恰为一轮)。
//
// panic 兜底(压测审核【必修-6】同类点位):此前 tick 内 latent panic 崩整进程 =
// DS 崩溃补偿链(§9 不变量 4)停摆、abandoned 对局无人回收。recover 后跳过本轮,
// 下 tick 继续;sweep 幂等可重入。并发 map 写是 runtime fatal,recover 兜不住。
func (u *AllocatorUsecase) heartbeatSweepTick(ctx context.Context) {
	defer safego.Recover(ctx, "ds_heartbeat_sweep")
	// 压测前审核 P1:census 准入缓存按 last-touch TTL 清死实例项(对齐 hub_allocator)。
	// 纯本地内存卫生,不经存储、不依赖 writer 身份,故每 tick 无条件先执行——否则本副本
	// 处理过心跳的已销毁 Battle 实例(UID 永不复用)留下的 admitted 项永不回收,长压测 OOM。
	// 活实例项每心跳 census 续期,仅超 TTL 未续期(实例已销毁)的项被清。
	sweepStaleOwnerAdmitted(&u.ownerAdmitted, time.Now().Add(-ownerAdmittedStaleTTL))
	// 领导权门排在本地内存卫生**之后**:热备副本照样服务 Heartbeat,其 census 缓存
	// 必须继续老化回收,否则热备期越长内存越涨(§9.18)。
	if !u.sweepIsLeader(ctx) {
		return
	}
	if err := u.sweepOnce(ctx); err != nil {
		plog.With(ctx).Warnw("msg", "heartbeat_sweep_failed", "err", err)
	}
}

// sweepOnce 扫描一次:last_heartbeat_ms 早于阈值的战斗 → 标记 abandoned + 回收 + 可靠补偿。
//
// W4 ⑧ 可靠补偿(不变量 §4 DS 崩溃必有补偿):
// 把 active ZSET 自身当作补偿事件的「outbox」——abandoned 的对局在 ds.lifecycle 事件
// 成功投递前**不移出 active**,故下一轮 sweep 会再次命中并重试投递;只有投递成功(或显式
// local/off 开发配置的 best-effort 回退)才 ExpireBattle 移出 active。配合 battle_result 幂等消费
// (不变量 §2),整条补偿链是 at-least-once 闭环,可穿越 Kafka 临时不可用。
//
// legacy 天然上界靠 UpdateBattleKeepTTL(KEEPTTL)。Model B 则在任何外部 Release 前
// 把 TERMINATING auth+battle 置为永久，只有 ReleaseExpected 与 lifecycle 投递都明确
// 成功后才由 ExpireTerminatedExpected 恢复有界 TTL；未知结果宁可不可用也不丢 fence。
// sweepDeferral 是一次队头退避的记录。state 参与失效校验:记录状态一变(被并发 RPC 推进、
// 或已进入 §9.4 补偿链、或 abort fence / auth quarantine 要求立即对账)立即作废退避,
// 不让调度优化拖慢真正的终态收敛。
type sweepDeferral struct {
	state string
	until time.Time
}

// warmingProbeDeferPrefix 是 warming 判死 probe 失败的退避键前缀,后接 allocation_id:
// 退避绑定到被探测的那次分配身份,同 match_id 的新分配(不同 allocation_id)键不同,
// 天然不继承旧退避;记录状态真正变化时也会因 state 不匹配立即作废(sweepDeferralActive)。
const warmingProbeDeferPrefix = "warming-probe:"

// releasePendingDeferPrefix 是 abandoned 外部回收**任何未确认结果**(删除宽限、
// GET/DELETE 超时、控制面持续故障、pod UID preflight 失败)后的退避键前缀,后接
// allocation_id(复审必修:不只 deletion-pending,普通 5s 超时同样会让最老项每轮吃满
// 预算,INC-20260724-001 队头饥饿必须全类根除)。宽限/故障期内重试只会空转,按分配
// 身份让出队头;进程内表、不写 ZSET score,重启即清空(重试由 DELETE 幂等兜底)。
const releasePendingDeferPrefix = "release-pending:"

// noteSweepDeferral 登记一次队头退避。退避窗口复用既有 HeartbeatTimeout(默认 15s
// = 3 个 SweepInterval),不新增配置项。
func (u *AllocatorUsecase) noteSweepDeferral(ctx context.Context, matchID uint64, state string, now time.Time) {
	backoff := u.cfg.HeartbeatTimeout.Std()
	if backoff <= 0 {
		backoff = 15 * time.Second
	}
	if u.sweepDeferUntil == nil {
		u.sweepDeferUntil = make(map[uint64]sweepDeferral)
	}
	u.sweepDeferUntil[matchID] = sweepDeferral{state: state, until: now.Add(backoff)}
	plog.With(ctx).Debugw("msg", "allocation_sweep_head_of_line_deferred",
		"match_id", matchID, "state", state, "backoff", backoff.String())
}

// sweepDeferralActive 判定该项本轮是否让出队头。到期、或状态已变(见 sweepDeferral.state
// 的作用)即作废并立即重试。
func (u *AllocatorUsecase) sweepDeferralActive(matchID uint64, state string, now time.Time) bool {
	d, ok := u.sweepDeferUntil[matchID]
	if !ok {
		return false
	}
	if d.state != state || !now.Before(d.until) {
		delete(u.sweepDeferUntil, matchID)
		return false
	}
	return true
}

// pruneSweepDeferrals 清掉已到期的退避项,防表随历史 match_id 无界增长
// (§9.18 内存容器有界纪律;与同包 sweepStaleOwnerAdmitted 同源)。
// 表容量上界 = 一个退避窗口内被判为「卡住墓碑」的对局数,远小于 active 集合。
func (u *AllocatorUsecase) pruneSweepDeferrals(now time.Time) {
	for matchID, d := range u.sweepDeferUntil {
		if !now.Before(d.until) {
			delete(u.sweepDeferUntil, matchID)
		}
	}
}

// stuckReconcileState 列出「靠重试收敛、外部依赖不可用时会原地打转」的 sweep 状态。
// 这些项在每轮被处理前先退避,让出队头,避免饿死队尾的 §9.4 abandoned 补偿
// (INC-20260724-001)。
//
// 刻意**不含** stateAbandoned+epoch==0 的 resume 分支:那条本身就是 §9.4 补偿的最后一棒,
// 让它保持最高优先级重试;它不产生外部 GSA POST,单次耗时也远小于上面几种。
func stuckReconcileState(state string) bool {
	switch state {
	case stateAllocationUncertain, stateAllocationReconciling, stateAllocationEmptyFence,
		statePreactiveReleasing, stateAllocationAbort:
		return true
	}
	return false
}

// sweepRoundBudget 返回单轮 sweep 的墙钟预算。
//
// 上界口径(可断言):预算只在**开始下一项之前**检查,故单轮实际上界是
// `SweepInterval + 单项最坏耗时`,不是 `SweepInterval`——单项最坏 ≈
// LIST + getPod + DELETE + waitExpectedInstanceGone,各自受 allocate_timeout 约束。
// 写清这一点是为了让验收断言写得出来,而不是放宽成无意义的"大概不会太久"。
func (u *AllocatorUsecase) sweepRoundBudget() time.Duration {
	if d := u.cfg.SweepInterval.Std(); d > 0 {
		return d
	}
	return 5 * time.Second
}

func (u *AllocatorUsecase) sweepOnce(ctx context.Context) error {
	if err := u.reconcileActiveIndexIfDue(ctx); err != nil {
		return err
	}
	// 双阈值同一 now 计算(data.BattleStaleCutoffs):ACTIVE 用业务心跳超时,warming
	// 冷加载用 ready 等待超时(大图 ServerTravel→GameMode BeginPlay 前无业务心跳是
	// 正常行为,与 AllocateBattle 在途的 waitBattleReady 同一口径)。RangeStaleBattles
	// 仍用 activeCutoff 粗筛派生 ZSET(score 语义=last_heartbeat_ms),状态对应的
	// 二次核验交给 AbandonIfStale 在 WATCH 事务内按权威快照完成——外层读到的
	// inflight.State 会与首次 ActivateHeartbeat 并发,不得据其选单一阈值(TOCTOU)。
	now := time.Now()
	activeCutoff := now.Add(-u.cfg.HeartbeatTimeout.Std()).UnixMilli()
	warmingCutoff := now.Add(-u.readyWaitTimeout()).UnixMilli()
	stale, err := u.repo.RangeStaleBattles(ctx, activeCutoff)
	if err != nil {
		return err
	}
	roundStart := time.Now()
	budget := u.sweepRoundBudget()
	u.pruneSweepDeferrals(roundStart)
	processed := 0
	for _, mid := range stale {
		// 单轮墙钟预算(INC-20260724-001):控制面持续超时时单项可耗时数十秒,
		// 无预算的一轮会把下一 tick 直接叠上来,且队尾的 §9.4 abandoned 补偿被无限推后。
		// processed > 0 保证每轮至少推进一项(预算再小也不会活锁);未处理项留在 active
		// ZSET,下一 tick 继续(outbox 语义,进程重启即恢复)。
		if processed > 0 && time.Since(roundStart) >= budget {
			plog.With(ctx).Warnw("msg", "allocation_sweep_round_budget_exhausted",
				"processed", processed, "deferred_to_next_tick", len(stale)-processed,
				"budget", budget.String(), "elapsed_ms", time.Since(roundStart).Milliseconds())
			break
		}
		processed++
		// 先于 authority-mode 分支识别永久 fence：这样同版本但仍跑 legacy 配置的
		// writer 也只读跳过，不能把 uncertain 改成 abandoned 后 Release/Delete。
		inflight, found, readErr := u.repo.GetBattle(ctx, mid)
		if readErr != nil {
			plog.With(ctx).Warnw("msg", "allocation_sweep_read_failed",
				"match_id", mid, "err", readErr)
			continue
		}
		// sweep 队头公平性(INC-20260724-001):下面几种是「永久墓碑 / 靠重试收敛」状态,
		// 外部依赖(k8s/Agones 控制面)持续不可用时它们永远收敛不了,而其 active ZSET
		// score 不变 ⇒ 恒排在 RangeStaleBattles(无 LIMIT、score 升序)结果最前面,
		// 串行吃掉整轮预算,把队尾 abandoned 对局的 §9.4 补偿无限推后
		// (事故当天控制面超时约 40s 期间正是此形态)。故让它们本轮让出队头。
		//
		// ⚠️ 退避记在**进程内**,绝不能写进 active ZSET score —— 三条硬约束:
		//   ① score 的权威语义是 last_heartbeat_ms(data/battle.go:106 接口注释),
		//      挪作调度时间戳会让「score ≤ 阈值 ⇔ 心跳超时」判据失效;
		//   ② data/battle_abort.go:289(abort fence)与 data/battle_auth.go:1150
		//      (auth quarantine)用 score==0 表示「下一轮 sweep 必须立即对账/回收」,
		//      data/battle_active_reconciler.go:143 专门用 ZADD NX 保护该哨兵 ——
		//      写退避会把这个高危态的立即对账降级成延迟对账;
		//   ③ TouchActive 是无条件 ZADD(data/battle.go:935,无 NX/XX/GT),
		//      迟到的退避写会把已被 RemoveActive 的终态项复活回补偿 outbox,重复投递 lifecycle。
		// 进程内表还顺带保住了 RunHeartbeatSweep 开头「重启即扫」的既定意图(重启即清空)。
		if found && stuckReconcileState(inflight.GetState()) {
			deferKey := inflight.GetState()
			if inflight.GetState() == statePreactiveReleasing && inflight.GetAllocationId() != "" {
				// preactive 墓碑与 release-pending 家族共用 allocation 键(复审 P1-2):
				// abandoned→fence 成功→release 未确认会发生 abandoned→pending 的跨状态
				// 迁移,若仍按状态键退避,上一轮登记的退避会被状态变化误判失效,
				// 回到每轮重复外部调用;allocation_id 不随该迁移变化。
				deferKey = releasePendingDeferPrefix + inflight.GetAllocationId()
			}
			if u.sweepDeferralActive(mid, deferKey, roundStart) {
				continue
			}
			u.noteSweepDeferral(ctx, mid, deferKey, roundStart)
		}
		if found && (inflight.State == stateAllocationUncertain ||
			inflight.State == stateAllocationReconciling) {
			if u.modelB {
				u.reconcileAllocationUncertain(ctx, inflight)
			} else {
				plog.With(ctx).Debugw("msg", "allocation_uncertain_retained_legacy_writer",
					"match_id", mid, "allocation_id", inflight.AllocationId)
			}
			continue
		}
		if found && inflight.State == stateAllocationEmptyFence {
			if u.modelB {
				u.resumeEmptyAllocationTombstone(ctx, inflight)
			}
			// Old writers do not know this state and must remain read-only.
			continue
		}
		if u.modelB && found && inflight.State == stateAbandoned && inflight.GetInstanceEpoch() == 0 {
			// allocation_uncertain reconciliation committed ABANDONED but crashed
			// before Kafka ACK/Expire. Reconfirm exact physical absence, then resume
			// the durable lifecycle handoff.
			// §9.4 最后一棒仍保持高优先重试,但外部确认失败后按分配身份让出队头一个
			// 退避窗口(复审必修:控制面持续故障时 epoch=0 resume 的物理确认 GET 同样
			// 会每轮吃满预算;15s 退避只延后重试节奏,outbox 可靠补偿语义不变)。
			if inflight.GetAllocationId() != "" &&
				u.sweepDeferralActive(mid, releasePendingDeferPrefix+inflight.GetAllocationId(), roundStart) {
				continue
			}
			if !u.resumeReconciledAllocationAbandoned(ctx, inflight) && inflight.GetAllocationId() != "" {
				u.noteSweepDeferral(ctx, mid, releasePendingDeferPrefix+inflight.GetAllocationId(), roundStart)
			}
			continue
		}
		if found && inflight.State == statePreactiveReleasing {
			// cleanup 在外部 DELETE 响应未知后留下的永久墓碑。重试只会再次
			// 做 UID 条件 Release 并在明确成功后 purge，不会再次 GSA POST。
			if u.modelB {
				u.reconcilePreactiveRelease(ctx, inflight)
			}
			continue
		}
		if found && inflight.State == stateAllocationAbort {
			if !u.modelB || u.abortRepo == nil {
				continue
			}
			request, released, journalFound, journalErr := u.abortRepo.ReadAllocationAbort(ctx, mid)
			if journalErr != nil || !journalFound {
				plog.With(ctx).Warnw("msg", "allocation_abort_journal_unavailable",
					"match_id", mid, "found", journalFound, "err", journalErr)
				continue
			}
			if abortErr := u.AbortPreactiveBattle(ctx, request); abortErr != nil {
				plog.With(ctx).Warnw("msg", "allocation_abort_reconcile_pending",
					"match_id", mid, "released", released, "err", abortErr)
			}
			continue
		}
		if u.modelB && found && inflight.State == stateAllocating {
			// Model B 只有 FenceBattleAllocation 成功把 state 改成
			// allocation_uncertain 后才允许 GSA POST。仍为 allocating 的陈旧
			// claim 机械证明外部副作用尚未开始，可按 allocation_id 直接撤销；
			// 无需、也不能伪造一个缺 UID 的外部 release。
			deleted, deleteErr := u.repo.DeleteBattleIfAllocationMatches(
				ctx, mid, inflight.GetAllocationId(), inflight.GetDsPodName())
			if deleteErr != nil {
				plog.With(ctx).Warnw("msg", "model_b_prepost_claim_delete_failed",
					"match_id", mid, "allocation_id", inflight.GetAllocationId(),
					"deleted", deleted, "err", deleteErr)
			}
			continue
		}
		if u.modelB && found && inflight.GetPodUid() == "" &&
			inflight.GetDsPodName() != "" && inflight.GetGameserverUid() != "" &&
			inflight.GetInstanceEpoch() > 0 && inflight.GetAllocationId() != "" {
			resolvedPodUID, preflightErr := u.ensureDurableReleasePodUID(
				ctx, mid, inflight.GetDsPodName(), data.BattleExpectedInstance{
					AllocationID: inflight.GetAllocationId(), InstanceUID: inflight.GetGameserverUid(),
					InstanceEpoch: inflight.GetInstanceEpoch(),
				}, inflight.GetReleaseTrack())
			if preflightErr != nil {
				plog.With(ctx).Warnw("msg", "model_b_stale_pod_uid_preflight_failed",
					"match_id", mid, "pod", inflight.GetDsPodName(), "err", preflightErr)
				continue
			}
			inflight.PodUid = resolvedPodUID
		}
		if u.modelB {
			cutoffs := data.BattleStaleCutoffs{
				ActiveHeartbeatMs:  activeCutoff,
				WarmingHeartbeatMs: warmingCutoff,
			}
			// warming 冷加载宽限的 Agones 加速出口(SDK health ping 判死接入):exact 实例
			// 已被编排层权威确认死亡(GameServer+关联 Pod UID 双确认消失,或 Agones 判
			// Unhealthy)时放弃时间宽限,本轮即交事务判弃,不再空等 ready_wait 到期。
			// 三道防线:
			//   ① probe 是 advisory 只读:任何读失败仅回退时间界(fail-closed 到慢路径,
			//      绝不据此直接 Release),并按分配身份记一次队头退避(复用 sweepDeferral,
			//      INC-20260724-001 纪律:控制面持续超时的 probe 不得跨轮占住队头;时间
			//      兜底判弃不受退避影响,同 match 的新分配身份不同、不继承旧退避)。
			//   ② 判死 forfeit 绑定被探测实例的 exact 身份,由 AbandonIfStale 在事务内
			//      精确核验(防 ABA:probe 挂起期间 A 被清理、同 match 新分配 B 就位,
			//      A 的判死绝不能杀 B)。
			//   ③ 与首次 ActivateHeartbeat 的竞态仍由 WATCH 单赢家保证——激活先赢时
			//      Active 分支按业务心跳阈值重判,本轮不弃。
			if found && inflight.GetState() == stateWarming && inflight.GetAllocationId() != "" &&
				inflight.GetDsPodName() != "" && inflight.GetGameserverUid() != "" {
				if prober, ok := u.authoritativeAlloc.(WarmingInstanceProber); ok {
					probeDeferState := warmingProbeDeferPrefix + inflight.GetAllocationId()
					if !u.sweepDeferralActive(mid, probeDeferState, roundStart) {
						gone, perr := prober.ProbeExpectedInstanceGone(ctx,
							inflight.GetDsPodName(), inflight.GetGameserverUid(), inflight.GetPodUid())
						switch {
						case perr != nil:
							u.noteSweepDeferral(ctx, mid, probeDeferState, roundStart)
							plog.With(ctx).Debugw("msg", "warming_instance_probe_unavailable",
								"match_id", mid, "pod", inflight.GetDsPodName(), "err", perr)
						case gone:
							cutoffs.WarmingForfeit = &data.BattleWarmingForfeit{
								Instance: data.BattleExpectedInstance{
									AllocationID:  inflight.GetAllocationId(),
									InstanceUID:   inflight.GetGameserverUid(),
									InstanceEpoch: inflight.GetInstanceEpoch(),
								},
								HeartbeatMs: time.Now().UnixMilli(),
							}
							plog.With(ctx).Warnw("msg", "warming_instance_confirmed_dead_forfeit_grace",
								"match_id", mid, "pod", inflight.GetDsPodName(),
								"allocation_id", inflight.GetAllocationId())
						}
					}
				}
			}
			out, aerr := u.authRepo.AbandonIfStale(
				ctx, mid, cutoffs, u.dsCredentialTTL, u.battleTTL())
			if aerr != nil {
				// Redis 权威不可读/状态不一致时 fail-closed：绝不凭派生 ZSET 直接 Release。
				// 仅 battle 已随 TTL 消失时可安全清残留索引。
				if _, found, gerr := u.repo.GetBattle(ctx, mid); gerr == nil && !found {
					_ = u.repo.RemoveActive(ctx, mid)
				}
				plog.With(ctx).Warnw("msg", "model_b_sweep_authority_check_failed",
					"match_id", mid, "err", aerr)
				continue
			}
			b := out.Battle
			if !out.Abandoned && !out.AlreadyTerminal {
				// 只是跨 slot ZSET score 陈旧；用事务快照里的服务端 auth heartbeat 修索引。
				if b != nil {
					_ = u.repo.TouchActive(ctx, mid, b.LastHeartbeatMs)
				}
				continue
			}
			if b == nil {
				continue
			}
			if b.State == stateEnded {
				_ = u.repo.RemoveActive(ctx, mid)
				continue
			}
			if b.State != stateAbandoned {
				continue
			}
			// 外部回收已进入删除宽限的项让出队头(退避期内不重复 terminate/release;
			// deliverAbandoned 本就门控在 release 确认之后,退避只是把必然失败的重试
			// 推迟到宽限结束,不延后任何可行的补偿)。
			if u.sweepDeferralActive(mid, releasePendingDeferPrefix+b.GetAllocationId(), roundStart) {
				continue
			}
			if !out.AuthFound || !out.ActiveFound {
				// Prepare 前崩溃的分配也必须先进入永久 release fence。旧实现
				// Release→Delete 虽顺序较安全，但有限 TTL 仍会在 Release 响应未知后
				// 自行开放第二次 POST；统一走同一个 fenced 回收状态机。
				// bootstrap/no-active 分支同样按结构化结果退避(复审 P1-2):任何未确认
				// 结果都按 exact allocation 让出队头,防控制面故障时每轮重复外部调用。
				switch u.reconcilePreactiveRelease(ctx, b) {
				case preactiveReleaseCompleted:
					plog.With(ctx).Infow("msg", "model_b_inflight_reconciled",
						"match_id", mid, "allocation_id", b.AllocationId)
				case preactiveReleaseUnconfirmed:
					u.noteSweepDeferral(ctx, mid, releasePendingDeferPrefix+b.GetAllocationId(), roundStart)
				case preactiveReleaseSkipped:
				}
				continue
			}
			// abandoned 的外部回收按 UID/allocation_id 幂等执行到确认成功；失败必须保留
			// active outbox，不能先投补偿并移出索引后永久泄漏 GameServer。
			expectedInstance := data.BattleExpectedInstance{
				AllocationID: b.GetAllocationId(), InstanceUID: b.GetGameserverUid(),
				InstanceEpoch: b.GetInstanceEpoch(),
			}
			resolvedPodUID, preflightErr := u.ensureDurableReleasePodUID(
				ctx, mid, b.GetDsPodName(), expectedInstance, b.GetReleaseTrack())
			if preflightErr != nil {
				// 控制面读失败同样按分配身份退避,不得每轮重试吃满预算(队头饥饿全类根除)。
				u.noteSweepDeferral(ctx, mid, releasePendingDeferPrefix+b.GetAllocationId(), roundStart)
				plog.With(ctx).Warnw("msg", "model_b_sweep_pod_uid_preflight_failed",
					"match_id", mid, "pod", b.GetDsPodName(), "err", preflightErr)
				continue
			}
			b.PodUid = resolvedPodUID
			terminated, terminateErr := u.authRepo.TerminateExpected(
				ctx, mid, expectedInstance, stateAbandoned, u.dsCredentialTTL, u.battleTTL())
			if !terminated {
				plog.With(ctx).Warnw("msg", "model_b_sweep_terminate_fence_failed",
					"match_id", mid, "pod", b.DsPodName, "err", terminateErr)
				continue
			}
			if terminateErr != nil {
				plog.With(ctx).Warnw("msg", "model_b_sweep_index_cleanup_failed",
					"match_id", mid, "err", terminateErr)
			}
			if rerr := u.releaseGameServer(ctx, mid, b.DsPodName, &data.AuthoritativeGameServerAllocation{
				PodName: b.DsPodName, InstanceUID: b.GameserverUid, AllocationID: b.AllocationId,
				PodUID: b.PodUid, InstanceEpoch: b.InstanceEpoch,
			}); rerr != nil {
				// 任何未确认的 release 结果都按分配身份退避(不只 deletion-pending):
				// 宽限内/控制面故障中的重试只会空转占队头,队尾 §9.4 补偿必须照常推进;
				// 退避到期后的重试经 DELETE 幂等 + 双对象消失确认闭合,outbox 语义不变。
				u.noteSweepDeferral(ctx, mid, releasePendingDeferPrefix+b.GetAllocationId(), roundStart)
				if errors.Is(rerr, data.ErrReleaseDeletionPending) {
					plog.With(ctx).Debugw("msg", "model_b_sweep_release_grace_pending",
						"match_id", mid, "pod", b.DsPodName, "allocation_id", b.GetAllocationId())
					continue
				}
				plog.With(ctx).Warnw("msg", "model_b_sweep_release_failed",
					"match_id", mid, "pod", b.DsPodName, "err", rerr)
				continue
			}
			if out.Abandoned {
				plog.With(ctx).Infow("msg", "battle_abandoned_heartbeat_timeout",
					"match_id", mid, "pod", b.DsPodName, "authority", "redis")
			}
			if u.deliverAbandoned(ctx, mid, b.DsPodName, b.GameserverUid, b.PlayerIds, b.MapId, b.GameMode) {
				target := placement.Target{
					PodName: b.DsPodName, InstanceUID: b.GameserverUid,
					InstanceEpoch: b.InstanceEpoch, AllocationID: b.AllocationId,
					ReleaseTrack: b.ReleaseTrack,
				}
				if markerErr := u.lifecycleProofRepo.RecordAllocationLifecyclePublished(
					ctx, mid, target); markerErr != nil {
					plog.With(ctx).Warnw("msg", "model_b_sweep_lifecycle_marker_failed",
						"match_id", mid, "allocation_id", b.AllocationId, "err", markerErr)
					continue
				}
				if expired, eerr := u.authRepo.ExpireTerminatedExpected(
					ctx, mid, expectedInstance, u.dsCredentialTTL, u.battleTTL()); eerr != nil || !expired {
					plog.With(ctx).Warnw("msg", "model_b_sweep_expire_failed", "match_id", mid,
						"expired", expired, "err", eerr)
				}
			}
			continue
		}
		var podName string
		var instanceUID string // owner 释放的 exact 身份门用(见 deliverAbandoned)
		var staleIndexOnly bool
		var endedSkip bool
		var firstAbandon bool // 本次成功事务是否执行了 →abandoned 的首次迁移(全局恰好一次,见闭包内注释)
		var playerIDs []uint64
		var mapID uint32
		var gameMode string
		// KEEPTTL:标记 abandoned / 每轮重试不刷新 battle key TTL,保证 BattleTTL 是补偿重试上界。
		lerr := u.repo.UpdateBattleKeepTTL(ctx, mid, updateMaxRetry, func(b *dsv1.BattleStorageRecord) error {
			// 出参每轮重置:CAS 冲突时闭包基于重新 GET 的最新镜像整体重跑,以最后一次成功事务为准
			// (BattleRepo.UpdateBattleWithLock 的 fn 重跑契约)。
			staleIndexOnly = false
			endedSkip = false
			firstAbandon = false
			// active ZSET 是跨 slot 派生索引。心跳可能已成功更新权威 record，但后续
			// ZADD 失败，留下旧 score；必须在任何终态写/Release 前重新核验 record。
			// UpdateBattleKeepTTL 成功后会以该真实时间补写 ZSET，故这里只修索引、零副作用。
			// 阈值同样在事务闭包内按最新镜像状态选择:warming 冷加载用 ready 等待阈值。
			cutoff := activeCutoff
			if b.State == stateWarming {
				cutoff = warmingCutoff
			}
			if b.LastHeartbeatMs > cutoff {
				staleIndexOnly = true
				return nil
			}
			if b.State == stateEnded {
				endedSkip = true      // 正常结算,移出 active 不补偿
				podName = b.DsPodName // 捕获用于 local 幽灵 DS 收尾(见下方 killStrandedDS)
				// 2026-08-24:同时捕获 exact 实例身份与花名册 —— 下方 ended 分支要拿它们补一次
				// owner 释放。**不捕获就是静默 no-op**:ownerReleaseAbandonedPlayersWeak 开头对
				// selfUID=="" / players 为空整体早退,加了释放也等于没加。
				instanceUID = b.GetGameserverUid()
				playerIDs = append([]uint64(nil), b.GetPlayerIds()...)
				return nil
			}
			// firstAbandon 仅在本事务把状态从非 abandoned 首次写成 abandoned 时为 true。
			// WATCH CAS 保证该迁移跨副本/跨 sweep 轮次全局只成功一次:并发副本撞 TxFailedErr
			// 重跑后读到 abandoned → false;补偿重试轮次读到 abandoned → false。
			// 因此下方 Release 恰好执行一次,不存在 double-release。
			firstAbandon = b.State != stateAbandoned
			b.State = stateAbandoned
			podName = b.DsPodName
			instanceUID = b.GameserverUid
			playerIDs = b.PlayerIds
			mapID = b.MapId
			gameMode = b.GameMode
			return nil
		})
		if lerr != nil {
			if errcode.As(lerr) == errcode.ErrDSPodNotFound {
				_ = u.repo.RemoveActive(ctx, mid) // 镜像 TTL 过期:清理残留 active(补偿重试的天然上界)
				continue
			}
			plog.With(ctx).Warnw("msg", "sweep_lock_failed", "match_id", mid, "err", lerr)
			continue
		}
		if staleIndexOnly {
			plog.With(ctx).Infow("msg", "sweep_repaired_stale_index", "match_id", mid)
			continue
		}
		if endedSkip {
			// 正常结算的 DS 收尾回收(2026-07-03):battle_result 不再在结算响应路径直杀 DS(那会抢在
			// DS 通知客户端回大厅之前把它杀掉),DS 生命周期归此处兜底。DS 发完 ended 心跳后即
			// StopBattleHeartbeat(无第二跳,心跳终态 killStrandedDS 永不触发),且 local 模式 DS 的
			// Agones Shutdown 是 no-op → 进程不会自退。故这里在 ended 且失联(≥HeartbeatTimeout 未心跳,
			// 此时 DS 早已通知客户端回大厅)时主动 taskkill,防幽灵 DS 占端口耗尽端口池。
			// killOrphanOnStop 门控:仅 local 打开;Agones 关(DS 已自身 Shutdown,pod 交 Fleet 回收)。
			u.killStrandedDS(ctx, mid, podName, "ended")
			// owner 释放的**兜底重试**(2026-08-24)。主收口在心跳侧:DS 上报 ended 的那一跳
			// 写回成功后即释放(本文件 becameEnded 分支)。这里再补一次,覆盖那一跳的释放失败
			// (owner 抖动 / 超预算)—— 否则没有任何人重试,玩家结算后永远回不了大厅。
			// 幂等:exact 身份门(pod+uid+BATTLE)+ compare-delete 保证已释放 / 已改派的玩家
			// 一律跳过。时序上比心跳侧晚一个 HeartbeatTimeout,救不了体验,只保证最终收敛。
			plog.With(ctx).Infow("msg", "ended_owner_release_retry", "match_id", mid,
				"pod", podName, "players", len(playerIDs))
			ownerReleaseAbandonedPlayersWeak(ctx, u.ownerAuth, playerIDs,
				podName, instanceUID, 2*time.Second)
			_ = u.repo.RemoveActive(ctx, mid)
			continue
		}
		// 仅首次迁移 abandoned 的赢家事务回收 pod(并发副本 / 补偿重试轮次 firstAbandon=false 跳过,
		// 不会重复 Release;Release 本身对已消失的 GameServer 幂等,双重保险)。
		if firstAbandon {
			if rerr := u.alloc.Release(ctx, podName); rerr != nil {
				plog.With(ctx).Warnw("msg", "sweep_release_failed", "match_id", mid, "pod", podName, "err", rerr)
			}
			plog.With(ctx).Infow("msg", "battle_abandoned_heartbeat_timeout", "match_id", mid, "pod", podName)
		}
		// 投递 abandoned 补偿事件:成功(或显式 local/off 开发回退)才移出 active;
		// 失败则保留在 active,下一轮 sweep 重试(可靠补偿,不变量 §4)。
		if u.deliverAbandoned(ctx, mid, podName, instanceUID, playerIDs, mapID, gameMode) {
			// 终态镜像保留一段供查询,移出 active 不再扫描
			if eerr := u.repo.ExpireBattle(ctx, mid, u.battleTTL()); eerr != nil {
				plog.With(ctx).Warnw("msg", "sweep_expire_failed", "match_id", mid, "err", eerr)
			}
		}
	}
	// R4 就地聚合:sweep 每 SweepInterval(默认 5s)跑一轮,逐项打 INFO 会把同文件里的
	// WARN 拒绝冲走。整轮只出一条 Debugw,足以回答"扫了几个、处理了几个、耗时多少"
	// (§11.3 判据 5),异常侧(预算耗尽 / 各类失败)另有独立 WARN,不受本条级别影响。
	if len(stale) > 0 {
		plog.With(ctx).Debugw("msg", "allocation_sweep_round_done", "stale", len(stale),
			"processed", processed, "deferred", len(stale)-processed,
			"elapsed_ms", time.Since(roundStart).Milliseconds(), "budget", budget.String())
	}
	// 孤儿 Allocated GameServer 对账清扫(biz/orphan_gameserver.go):按分钟节流,
	// 只处理「无任何权威记录引用」的 GS,与上面按记录驱动的判弃链互不重叠。
	u.reconcileOrphanGameServersIfDue(ctx, time.Now())
	return nil
}

func (u *AllocatorUsecase) reconcileActiveIndexIfDue(ctx context.Context) error {
	if u.activeIndexReconciler == nil {
		if u.modelB {
			plog.With(ctx).Errorw("msg", "battle_active_index_refused", "reason", reasonActiveIndexUnavailable,
				"hint", "Model B 下缺派生索引重建能力,丢失的永久墓碑无人发现;整轮 sweep fail-closed")
			return errcode.New(errcode.ErrInvalidState,
				"canonical battle active-index reconciler unavailable")
		}
		return nil
	}
	now := time.Now()
	if !u.lastActiveIndexReconcile.IsZero() && activeIndexReconcileInterval > 0 &&
		now.Sub(u.lastActiveIndexReconcile) < activeIndexReconcileInterval {
		return nil
	}
	if err := u.activeIndexReconciler.ReconcileBattleActiveIndex(ctx, 256); err != nil {
		plog.With(ctx).Warnw("msg", "battle_active_index_refused", "reason", reasonActiveIndexRebuild,
			"elapsed_ms", time.Since(now).Milliseconds(), "err", err,
			"hint", "重建失败 = 本轮 sweep 整体不执行(fail-closed),判弃补偿链顺延一个周期")
		return errcode.NewCause(errcode.ErrUnavailable, err,
			"rebuild canonical battle active index")
	}
	// R1：派生 active 索引重建是§11.3 点名要求留 INFO 的不可逆推进之一（它决定了
	// 哪些对局能被心跳超时扫描看到）。频率固定为 activeIndexReconcileInterval（30s），
	// 不会刷屏；无它则“判弃链从什么时候开始漏扫”无法从日志回答。
	plog.With(ctx).Infow("msg", "battle_active_index_reconciled",
		"interval", activeIndexReconcileInterval.String(),
		"elapsed_ms", time.Since(now).Milliseconds())
	u.lastActiveIndexReconcile = now
	return nil
}

// deliverAbandoned 发 DSLifecycleEvent{phase=ABANDONED} 给 battle_result 做玩家段位回滚补偿。
//
// 返回值语义(给 sweepOnce 决定是否移出 active):
//   - true  → 可移出 active:已成功投递,或显式 local/off 开发配置未接 Kafka。
//   - false → 投递失败,保留在 active 下一轮 sweep 重试(可靠补偿,不变量 §4)。
//
// 生产 required 但 publisher 意外为 nil 时必须返回 false：这是一道独立于 main 启动
// 校验的 fail-closed 保险，禁止 abandoned 在没有 match release / exit proof 时被过期。
func (u *AllocatorUsecase) deliverAbandoned(ctx context.Context, matchID uint64, podName, instanceUID string, playerIDs []uint64, mapID uint32, gameMode string) bool {
	// owner 权威释放(INC-20260729-002 P0-B1)与 lifecycle 投递同点执行:本函数的所有调用点
	// 都已门控在「被判弃实例的 GameServer 回收已确认」之后,正是唯一既安全(旧 DS 已消失,
	// 释放不会开出双 DS)又必经(补偿链的唯一收口)的位置。
	//
	// 放在 lifecycle 投递**之前**:lifecycle 失败会返回 false 让 sweep 下一轮整体重试,
	// 而 owner 释放本身幂等(compare-delete,已释放的记录第二次是 no-op),重跑无副作用;
	// 反过来若放在 return true 之后,lifecycle 长时间不可用时玩家的 owner 记录会一直
	// 指向已删除的 Pod,这正是本条要根治的状态。
	// 预算 2s 与 ownerAdmitCensusWeak 同口径:本函数在 sweep 轮内被调用,而 sweep 单轮
	// 墙钟预算 = SweepInterval(默认 5s,见 sweepRoundBudget),owner 抖动时不得吃满它。
	ownerReleaseAbandonedPlayersWeak(ctx, u.ownerAuth, playerIDs, podName, instanceUID, 2*time.Second)

	if u.lifecycle == nil {
		if u.lifecycleRequired {
			plog.With(ctx).Errorw("msg", "ds_lifecycle_publisher_missing_fail_closed",
				"match_id", matchID, "hint", "retain active recovery outbox; restart with a healthy Kafka producer")
			return false
		}
		plog.With(ctx).Warnw("msg", "ds_lifecycle_disabled_dev_best_effort",
			"match_id", matchID, "hint", "local/off development only; no battle_result recovery event will be produced")
		return true
	}
	evt := &dsv1.DSLifecycleEvent{
		MatchId:   matchID,
		DsPodName: podName,
		Phase:     dsv1.DSLifecyclePhase_DS_LIFECYCLE_PHASE_ABANDONED,
		PlayerIds: playerIDs,
		MapId:     mapID,
		GameMode:  gameMode,
		TsMs:      time.Now().UnixMilli(),
	}
	if err := u.lifecycle.PublishLifecycle(ctx, evt); err != nil {
		// 保留在 active,下轮 sweep 重试(穿越 Kafka 临时不可用)
		plog.With(ctx).Warnw("msg", "ds_lifecycle_publish_failed_will_retry", "match_id", matchID, "err", err)
		return false
	}
	plog.With(ctx).Debugw("msg", "ds_lifecycle_published", "match_id", matchID)
	return true
}

// rosterGateArmable 判断「花名册到齐期限」这道闸此刻还允不允许被武装。
//
// 判据是本局的年龄(自 allocated_at 起算)而不是任何进程内状态 —— 它必须对**任何副本**
// 给出同一答案,滚动升级中接手心跳的新副本才不会对一局老 battle 做出与前任不同的判断。
//
// allocated_at 缺失(旧记录 / mock)时返回 false:拿不到年龄就不武装。这是刻意的 fail-safe
// 方向 —— 漏防一局缺员局,远好过误判弃一局正在打的对局。
func rosterGateArmable(allocatedAtMs, nowMs, armWindowMs int64) bool {
	if armWindowMs <= 0 || allocatedAtMs <= 0 {
		return false
	}
	age := nowMs - allocatedAtMs
	return age >= 0 && age <= armWindowMs
}
