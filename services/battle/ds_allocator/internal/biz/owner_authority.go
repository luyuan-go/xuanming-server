// owner_authority.go — owner 归属接线(owner-authority.md;contract 阶段,2026-07-29)。
//
//   - ② READY 交付前逐玩家 **强** BeginTransition(BATTLE):把"这批玩家将由该 Battle 实例
//     own"写进 owner 权威(E+1/PENDING/屏障);**写不进即拒绝本次交付**(§9.22 fail-closed);
//   - ③ 授权心跳 census 首见玩家代提交 Admit(仍是近似:census 来自绑定 exact 实例身份的
//     授权心跳,是"该实例正在服务该玩家"的证据;DS Admission 链原生提交后本近似退役)。
//     census 侧刻意保持弱:它是周期性重试点,不该让一个玩家的自愈失败打挂整台 DS 的心跳。
package biz

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/placement"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// owner 类型常量(对齐 owner.proto OwnerType;biz 不依赖生成代码)。
const (
	ownerTypeHub    int8 = 1
	ownerTypeBattle int8 = 2
)

// owner 阶段常量(对齐 owner.proto OwnerPhase)。
const (
	ownerPhasePending  int8 = 1
	ownerPhaseAdmitted int8 = 2
)

// errOwnerBeginOutcomeUnknown 表示 Begin 的服务端提交结果无法判定。调用方必须保留
// allocation/Pod 与本批已写 owner，不能按普通失败回滚或清理；否则可能留下指向死 Pod
// 的 owner。它只作为进程内控制流 cause，对客户端仍统一暴露 ErrUnavailable。
var errOwnerBeginOutcomeUnknown = errors.New("owner Begin outcome unknown")

// ownerBeginReadbackBudget 是 Begin 非 epoch-conflict 失败后的独立判定预算。Begin 的
// 入站 ctx 可能正因 transport deadline 失效，故回读必须 detach；2s 与 QueryOwner 单次
// RPC 的默认上界同量级，足以确认 requested operation 是否已提交。
const ownerBeginReadbackBudget = 2 * time.Second

// ownerAdmittedStaleTTL 是 census 已准入缓存项(ownerAdmitted,key=instanceUID|playerID)的
// 最大保鲜期。活实例每次心跳 census 对在场玩家续期 last-touch;超过本值未续期 = 其所属 Battle
// 实例已销毁(UID 不再心跳),项由 sweepStaleOwnerAdmitted 清除,防缓存随历史实例 UID 无界增长
// (压测前审核 P1;§9.18 客户端触发型内存容器有界)。取值远大于心跳/census 周期,活实例项绝不误清。
//
// 与 hub_allocator 同名机制一致(hub 复审 P1-5):Battle DS 打完即销毁、InstanceUID 永不复用,
// admitted 项若不老化回收会随累计对局数单调增长,长压测下 OOM。
const ownerAdmittedStaleTTL = 5 * time.Minute

// sweepStaleOwnerAdmitted 删除 last-touch 早于 cutoff 的 census 准入缓存项。
// 值意外非 time.Time(理论不达,所有写入点均写 time.Time)也删除,保证 fail-safe 有界。
// best-effort:被清项若玩家仍在场,下一轮 census 会重新 Query→Admit 补回(至多一次多余往返)。
func sweepStaleOwnerAdmitted(admitted *sync.Map, cutoff time.Time) {
	admitted.Range(func(k, v any) bool {
		if t, ok := v.(time.Time); !ok || t.Before(cutoff) {
			admitted.Delete(k)
		}
		return true
	})
}

// OwnerAuthority 是 owner 权威的调用面(Query/Begin/Admit/Release)。
// 由 data.GrpcOwnerLeaseRenewer 实现(与租约续写共用连接);可为 nil(未启用)。
//
// BeginTransition 回传权威最终记录:真实写时其 owner_epoch + operation_id 是精确回滚的
// 唯一凭据;同 target no-op 时是既有记录;EPOCH_CONFLICT 时是权威当前记录。
// 契约见 owner.proto BeginTransitionResponse。
type OwnerAuthority interface {
	QueryOwner(ctx context.Context, playerID uint64) (data.OwnerRecordView, error)
	BeginTransition(ctx context.Context, playerID, expectEpoch uint64, operationID string, ownerType int8, target data.OwnerTargetView) (data.OwnerRecordView, error)
	Admit(ctx context.Context, playerID, ownerEpoch uint64, operationID string, target data.OwnerTargetView) (int64, error)
	ReleaseOwner(ctx context.Context, playerID, ownerEpoch uint64, operationID string) error
}

// SetOwnerAuthority 注入 owner 权威调用面(nil-safe)。
func (u *AllocatorUsecase) SetOwnerAuthority(a OwnerAuthority) {
	u.ownerAuth = a
}

// beginOnePlayer 为单个玩家把 owner 权威推进到 target(contract 阶段:强依赖)。
//
// 同实例收敛已下沉到 owner 的行锁事务:
//   - 本地 Query→比对→Begin 是先查再存(§9.22 明令禁止),判定与写入不在同一线性化点;
//   - DS 批量 Begin 需要精确补偿 provenance,故每玩家传显式 UUIDv4 operation。真实写会
//     原样回传该 operation；并发同 target no-op 返回既有 operation,调用方据此绝不把别人
//     已写的记录纳入自己的 rollback。同 target 重投递不会覆盖既有 operation,稳定性不变。
//
// 顺带修掉一个洞:本文件原先的同实例判定只比 pod+uid,**不含 instance_epoch**
// (hub_allocator 侧复审 P1-3 已加、这边一直没加)。§9.22 要求 instance epoch 变化
// (实例代次翻转 / 灾备接管)必须递增 owner_epoch——旧判定会把它误当同目标跳过,
// 漏掉本应发生的 owner 迁移。权威侧的判定含 instance_epoch,下沉后自动补齐。
//
// Query 仍要发:取 CAS 期望值 expect_epoch。与 Begin 之间被别的写者推进属 CAS 设计内
// 竞争,权威回 EPOCH_CONFLICT。这里不能拿原 allocation/target 重查 epoch 后盲重试:
// conflict 可能正是更新的归属已经胜出,第二次写旧 target 会把 winner 回滚。强调用方
// 重走整条分配链并重读 allocation。
//
// 成功也不能只看 nil error:滚动升级期间旧 owner binary 可能把同物理实例、不同 allocation
// 当作 no-op,原样回传旧记录。只有回传记录与本次完整 target exact 且带有效 fencing/幂等
// 锚点,调用方才可把它纳入精确回滚凭据并继续交付 READY。
//
// 非 EPOCH_CONFLICT 的 RPC 错误也不能直接当作未提交：服务端可能已经 commit requested
// operation，只是回包丢失。此时用独立 ctx one-shot Query 回读；exact+requested operation
// 证明本次创建，exact+其他 operation 证明既有同 target 已收敛。若回读也失败则返回
// errOwnerBeginOutcomeUnknown，要求上层保留 allocation/Pod，绝不能猜测性 cleanup。
func beginOnePlayer(ctx context.Context, auth OwnerAuthority, playerID uint64,
	ownerType int8, target data.OwnerTargetView) (data.OwnerRecordView, bool, error) {
	requestedOperation := uuid.NewString()
	rec, qerr := auth.QueryOwner(ctx, playerID)
	if qerr != nil {
		// 查询不可判定 → UNKNOWN,绝不当作"无归属"继续(§9.22 禁冒充 OFFLINE/空闲)。
		return data.OwnerRecordView{}, false, qerr
	}
	got, berr := auth.BeginTransition(ctx, playerID, rec.OwnerEpoch, requestedOperation, ownerType, target)
	if berr != nil {
		if errcode.As(berr) == errcode.ErrOwnerEpochConflict {
			return got, false, berr
		}

		readbackCtx, cancel := context.WithTimeout(plog.Detach(ctx), ownerBeginReadbackBudget)
		observed, readbackErr := auth.QueryOwner(readbackCtx, playerID)
		cancel()
		if readbackErr != nil {
			return got, false, errcode.NewCause(errcode.ErrUnavailable, errOwnerBeginOutcomeUnknown,
				"owner Begin outcome unknown player=%d: begin=%v readback=%v",
				playerID, berr, readbackErr)
		}
		if ownerRecordExactlyTargets(observed, ownerType, target) {
			return observed, observed.OperationID == requestedOperation, nil
		}
		// 回读明确显示当前 owner 不是本次 target：无论 Begin 未提交，还是提交后已被
		// 更新的 writer 覆盖，本 allocation 都不再拥有该玩家，可按原错误正常补偿。
		return observed, false, berr
	}
	if !ownerRecordExactlyTargets(got, ownerType, target) {
		return got, false, errcode.New(errcode.ErrInvalidState,
			"owner Begin returned a non-exact target player=%d", playerID)
	}
	return got, got.OperationID == requestedOperation, nil
}

func ownerRecordExactlyTargets(rec data.OwnerRecordView, ownerType int8, target data.OwnerTargetView) bool {
	return rec.OwnerEpoch > 0 && placement.ValidOperationID(rec.OperationID) && rec.OwnerType == ownerType &&
		(rec.Phase == ownerPhasePending || rec.Phase == ownerPhaseAdmitted) &&
		rec.PodName == target.PodName && rec.InstanceUID == target.InstanceUID &&
		rec.InstanceEpoch == target.InstanceEpoch &&
		rec.AssignmentOrAllocationID == target.AssignmentOrAllocationID &&
		rec.ReleaseTrack == target.ReleaseTrack
}

// ownerVerifyPlayersExact 是 claim loser / 幂等重试交付 READY 前的只读门。
//
// loser 不能再跑 Begin:同一批玩家的多个并发 binder 会让一个调用把另一个调用已经依赖的
// grant 当成自己的补偿对象 Release,per-player Begin/Release 无法原子提交整批。这里只 Query
// 并要求 roster 每个玩家都已 exact 指向 winner 的完整 Battle target；任一缺失/漂移即
// fail-closed,等待唯一 claim winner 完成 bind 或由外层重试。函数零 Begin/Release 副作用。
func ownerVerifyPlayersExact(ctx context.Context, auth OwnerAuthority, players []uint64,
	ownerType int8, target data.OwnerTargetView, budget time.Duration) error {
	if auth == nil || len(players) == 0 {
		return nil
	}
	budgetCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	for _, playerID := range players {
		rec, err := auth.QueryOwner(budgetCtx, playerID)
		if err != nil {
			return err
		}
		if !ownerRecordExactlyTargets(rec, ownerType, target) {
			return errcode.New(errcode.ErrUnavailable,
				"owner target not exact for ready delivery player=%d", playerID)
		}
	}
	return nil
}

// ownerBeginGrant 是本次调用已成功写进权威的一份归属,用于部分失败时精确回滚。
// epoch/operation 必须取权威回传的新记录:Release 要求两者与记录全等才生效。
type ownerBeginGrant struct {
	PlayerID    uint64
	OwnerEpoch  uint64
	OperationID string
}

// ownerBeginRollbackBudget 是部分失败后回滚已写入归属的独立预算。
// 取值依据:回滚至多 len(players)-1 次(一局最多 10 人 → ≤9 次)单趟 owner Release RPC,
// 与 Begin 侧同量级往返;3s 留足余量,又不至于让分配失败路径长时间挂着。待实测复核。
const ownerBeginRollbackBudget = 3 * time.Second

// rollbackOwnerBegins 精确撤销本次未交付的 Begin。
//
// 为什么必须回滚,而不是"等下次分配的 CAS 自然覆盖":BeginTransition 是纯 CAS、不校验
// admit 屏障,下次分配确实能覆盖——**前提是还有下次**。玩家若在本次失败后离线,记录会
// 永久停在 PENDING 指向一台马上被 cleanupAllocatedBattle 删掉的 Pod;而 login 的
// query-first(account/login/internal/biz/owner_query.go 的 applyOwnerPlacement)在屏障
// 已开时会把它翻译成 TARGET+PENDING,把这台死 Pod 当 exact target 下发给客户端。客户端
// 按 §9.23 重查拿到同一个死目标,重登也一样——玩家有"返回登录"出口却回不去游戏,
// 只能靠运维手工清记录。owner 侧没有任何归属记录的 TTL/回收(唯一的 sweep 是
// RunTransitionLogSweep,清的是审计流水),所以这个残留不会自己消失。
//
// 只撤销本函数刚写进去的 (player, epoch, operation) 三元组,不动别的:符合验收底线第 4 条
// "补偿只允许精确撤销本次未交付的写,绝不回退玩家已获得的东西"。若期间记录已被别的写者
// 推进(玩家已被分到别处),epoch/operation 不再匹配,Release 按迟到调用幂等 no-op,不误伤。
//
// 独立 ctx:调用方的 budgetCtx 多半已因超时失效,拿它回滚等于不回滚。用 plog.Detach
// 保住 trace_id(§9.8 所有写都要带 trace_id)又不继承请求级 transport(§16.7)。
//
// best-effort:回滚失败只告警不上抛。此时 owner 权威本就不可用,重试也写不进去,而把
// 回滚的错误盖掉调用方的原始错误会让上层误判失败原因;下次分配的 CAS 覆盖仍是兜底。
func rollbackOwnerBegins(ctx context.Context, auth OwnerAuthority, granted []ownerBeginGrant) {
	if len(granted) == 0 {
		return
	}
	rbCtx, cancel := context.WithTimeout(plog.Detach(ctx), ownerBeginRollbackBudget)
	defer cancel()
	var failed int
	var firstErr error
	// 逆序释放:与写入顺序相反,先撤最后写进去的。
	for i := len(granted) - 1; i >= 0; i-- {
		g := granted[i]
		if err := auth.ReleaseOwner(rbCtx, g.PlayerID, g.OwnerEpoch, g.OperationID); err != nil {
			failed++
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if failed > 0 {
		plog.With(ctx).Warnw("msg", "owner_begin_rollback_incomplete",
			"granted", len(granted), "release_failed", failed, "first_err", firstErr,
			"hint", "残留归属指向已回收实例;下次分配的 CAS 覆盖兜底,持续出现须人工核查")
		return
	}
	plog.With(ctx).Infow("msg", "owner_begin_rolled_back",
		"granted", len(granted), "hint", "已精确撤销本次未交付的归属写")
}

// ownerBeginPlayers 批量强 Begin(contract 阶段):**任一玩家写不进 owner 权威即整体失败**。
//
// 为什么从"告警放行"改成 fail-closed:owner 是归属的唯一权威(§9.22),写不进去就无法证明
// "这台 DS 有权控制该玩家"。此时把 READY 交付出去 = 玩家可能同时被两台 DS 认领,
// 直接踩验收底线第 3 条(宁可 fail-closed 拒绝一次操作,也不写出不自洽的数据)。
//
// 拒绝不会把玩家卡死(底线第 1 条):分配失败后撮合按既有补偿链回收 claim,
// 客户端按 §9.23 退避重查,owner 恢复即自动重新分配。
//
// auth == nil = owner_addr 未配置(owner 服务未部署),属部署形态问题,不在本函数收敛。
//
// 超预算即失败,而不是 migrate 阶段的"跳过剩余玩家":一局里部分玩家有归属、部分没有,
// 比整局失败重来更难收敛,也会让 Admit 侧看到半截状态。
//
// **可判定的整体失败必须连同已写入的部分一起撤销**:本函数串行逐玩家写,失败点之前的玩家归属
// 已经落进权威,而调用方紧接着就会 cleanupAllocatedBattle 把那台 Pod 删掉。不回滚就会
// 留下一批"归属指向已删除实例"的 PENDING 记录,且没有任何路径能清掉它们
// (详见 rollbackOwnerBegins 的注释)。回滚是 best-effort,不改变本函数返回的原始错误。
//
// 唯一例外是 errOwnerBeginOutcomeUnknown：当前玩家可能已提交，而回读也不可达。此时既
// 不回滚此前 grants，也要求调用方不清理 allocation/Pod。保留整批技术 READY/PENDING
// 虽会 fail-closed 暂停交付，但 owner 恢复后 claim loser 可只读验证全员 exact 后收敛；
// 回滚半批再留下一个未知提交只会主动制造更难恢复的不一致。
func ownerBeginPlayers(ctx context.Context, auth OwnerAuthority, players []uint64,
	ownerType int8, target data.OwnerTargetView, budget time.Duration) error {
	if auth == nil || len(players) == 0 {
		return nil
	}
	budgetCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	granted := make([]ownerBeginGrant, 0, len(players))
	for i, playerID := range players {
		rec, created, err := beginOnePlayer(budgetCtx, auth, playerID, ownerType, target)
		if err != nil {
			outcomeUnknown := errors.Is(err, errOwnerBeginOutcomeUnknown)
			plog.With(ctx).Warnw("msg", "owner_begin_failed",
				"players", len(players), "failed_at", i, "player_id", playerID, "err", err,
				"pod", target.PodName, "instance_uid", target.InstanceUID,
				"granted_before_failure", len(granted),
				"outcome_unknown", outcomeUnknown,
				"hint", "contract 强依赖:归属未全量定案即拒绝交付;outcome unknown 必须保留 grants/allocation/pod")
			if outcomeUnknown {
				return err
			}
			rollbackOwnerBegins(ctx, auth, granted)
			return err
		}
		if created {
			granted = append(granted, ownerBeginGrant{
				PlayerID:    playerID,
				OwnerEpoch:  rec.OwnerEpoch,
				OperationID: rec.OperationID,
			})
		}
	}
	return nil
}

// ownerReleaseAbandonedPlayersWeak 判弃对局后释放仍指向该实例的 owner 记录
// (INC-20260729-002 P0-B1;弱依赖,同 Begin/Admit)。
//
// 为什么必须有:§9.4 的 abandoned 补偿链原本被当作「玩家解放出口」(UE 侧
// PandoraAgonesHealthPinger.h 的设计注释明写这一点),但它只做了 lifecycle 事件 +
// battle_result 记账 + match 释放,**从没动过 owner 权威**。全仓 ReleaseOwner 的唯一
// 调用点是 login 登出。后果:一旦 login 的 owner_query_first 打开,判弃后的恢复查询会
// 一直返回 TARGET(已删除的 battle Pod),客户端按 §9.23 反复 Travel 到不存在的实例,
// 比不接 owner 更糟。释放后恢复查询才会落到「无归属 → 首次进场链 → Hub」。
//
// 安全边界:
//
//	① **时序**(★ 2026-08-24 已由 owner 侧不变量取代,见下)。原文是:「只能在被判弃实例
//	   的 GameServer 回收已确认之后调用;提前释放会在旧 DS 可能仍在跑时放行新归属 =
//	   双 DS(§9.22)」。
//	② **exact 身份**:只释放「记录仍指向本次被判弃的 pod+uid 且类型为 BATTLE」的玩家。
//	   玩家已被迁到新 DS(epoch 已推进、pod/uid 已变)时必须跳过,否则误删活归属。
//	③ **compare-delete**:带 Query 读到的 owner_epoch + operation_id 调用,owner 侧按
//	   epoch 比对拒绝陈旧释放(同 §9.23「迟到 Logout 只能删自己」)。
//
// ★ 边界① 为什么不再是调用方的责任(INC-20260824-003,2026-08-24):
//
// 边界① 真正要守的是 pkg/placement §「旧 DS 最晚停止可玩时间 < 新 DS 最早开始可玩时间」,
// 而这条不等式在服务端的**唯一**执行点是 owner 的再入屏障 admit_not_before。问题在于:
// 屏障此前只能从「当前归属指针」推导(owner_type=BATTLE + instance_uid),而 Release 的
// UPDATE 恰恰清空这两列 —— **释放本身就是删掉屏障的判据**,屏障随即塌成 0。
// 于是边界① 实际上是在用「调用方记得晚点再释放」来替代一条本该由权威保证的不变量;
// 而 login 登出释放(判据只有 owner_type != 0)从来就没遵守过它。
//
// 已改为:owner 的 Release 在释放 BATTLE 归属时,按与 BeginTransition 同一公式算出
// max(now, 本实例租约截止)+skew 并**盖进 admit_not_before 留存**,BeginTransition 再取
// max 认回来(owner_repo.go 两处)。屏障从此是**玩家这一行的留存事实**,不再是归属指针的
// 派生量 —— 什么时候释放都不会让围栏消失。
//
// ⇒ 本函数的调用点不再需要「回收已确认」这道前置门来保证 §9.22 的正确性;边界②③ 仍然
//   必须遵守(它们管的是「删哪一条」,与屏障无关)。若将来把屏障留存逻辑改掉或绕过,
//   边界① 立刻恢复为硬要求 —— 两者是同一条不变量的两种实现,只能有一个在岗。
//
// 失败只告警:owner 未启用 / 抖动时,login 侧 InspectBattleRoute 的
// abandoned→(过再入屏障)→Terminal→Hub 旧门仍能让玩家收敛,不影响正确性。
func ownerReleaseAbandonedPlayersWeak(ctx context.Context, auth OwnerAuthority, players []uint64,
	selfPod, selfUID string, budget time.Duration) {
	if auth == nil || len(players) == 0 || selfPod == "" || selfUID == "" {
		return
	}
	budgetCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	var queryFailed, releaseFailed, released, skipped int
	var firstErr error
	var samplePlayer uint64
	noteFail := func(playerID uint64, err error) {
		if firstErr == nil {
			firstErr, samplePlayer = err, playerID
		}
	}
	defer func() {
		// 成功条数也要可观测:判弃是低频事件,一条汇总不会刷屏,而「释放了几个/跳过几个」
		// 正是排查「玩家为什么还回不去 Hub」时第一眼要看的数。
		plog.With(ctx).Infow("msg", "owner_release_abandoned_weak",
			"players", len(players), "released", released, "skipped_not_self", skipped,
			"query_failed", queryFailed, "release_failed", releaseFailed,
			"pod", selfPod, "sample_player_id", samplePlayer, "first_err", firstErr)
	}()
	for i, playerID := range players {
		if budgetCtx.Err() != nil {
			plog.With(ctx).Warnw("msg", "owner_release_abandoned_budget_exhausted",
				"players", len(players), "done", i, "remaining_players", len(players)-i,
				"hint", "migrate 弱依赖;login InspectBattleRoute 旧门仍能收敛")
			return
		}
		rec, err := auth.QueryOwner(budgetCtx, playerID)
		if err != nil {
			queryFailed++
			noteFail(playerID, err)
			continue
		}
		// ② exact 身份门:只有记录仍指向本次被判弃的实例才允许释放。
		if rec.OwnerType != ownerTypeBattle || rec.PodName != selfPod || rec.InstanceUID != selfUID {
			skipped++
			continue
		}
		// ③ compare-delete:epoch/operation 取自刚读到的记录,owner 侧再校验一次。
		if rerr := auth.ReleaseOwner(budgetCtx, playerID, rec.OwnerEpoch, rec.OperationID); rerr != nil {
			releaseFailed++
			noteFail(playerID, rerr)
			continue
		}
		released++
	}
}

// ownerAdmitCensusWeak census 首见玩家代提交 Admit(migrate 近似;弱依赖)。
//
// admitted 缓存 key = instanceUID|playerID(进程内 best-effort:重启后重查一轮即收敛);
// 仅当记录确实指向本实例(pod+uid 同 && 类型同 && PENDING)才 Admit,目标取记录自身字段
// (Admit 的 exact 全等校验由 owner 侧执行;pod/uid 是本调用方独立断言的部分)。
// 屏障未开(retryAfter>0)→ 本轮跳过,下次心跳重试;其余失败告警跳过。
//
// 缓存有界(压测前审核 P1,对齐 hub_allocator):
//   - 值存 time.Time(last-touch):命中即续期(接近过期才写,降 sync.Map 写争用),活实例
//     项恒新鲜;仅已销毁 Battle 实例(UID 不再心跳续期)的项会老化超 TTL,由后台
//     sweepStaleOwnerAdmitted 清除,防缓存随累计对局的历史 InstanceUID 无界增长导致 OOM。
//   - 按本实例 census 剪枝:玩家离开本实例(不再出现在 census)即删除其缓存项,与 TTL 兜底
//     互补(TTL 清死实例项,剪枝清活实例上已离场玩家项)。
func ownerAdmitCensusWeak(ctx context.Context, auth OwnerAuthority, admitted *sync.Map,
	players []uint64, ownerType int8, selfPod, selfUID string, budget time.Duration) {
	if auth == nil {
		return
	}
	// 先按本实例 census 剪枝:present 为本轮 census 的本实例 key 集合;删除带 selfUID| 前缀
	// 但已不在 present 的项(该玩家已离开本实例)。只触本实例前缀,死实例项交给 TTL sweep。
	present := make(map[string]struct{}, len(players))
	for _, playerID := range players {
		present[selfUID+"|"+fmt.Sprintf("%d", playerID)] = struct{}{}
	}
	admitted.Range(func(k, _ any) bool {
		key, ok := k.(string)
		if ok && strings.HasPrefix(key, selfUID+"|") {
			if _, in := present[key]; !in {
				admitted.Delete(key)
			}
		}
		return true
	})
	if len(players) == 0 {
		return // 剪枝已完成;本轮无玩家可代提交 Admit。
	}
	budgetCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	// 模式 C:本函数由**每次心跳**(每 ~5s/对局)对全部在场玩家调用,owner 抖动时逐玩家
	// 打 Warn = 并发对局数 × 玩家数 / 5s 条刷屏。批末汇总一条(弱依赖,双门兜底)。
	var queryFailed, admitFailed int
	var firstErr error
	var samplePlayer uint64
	noteFail := func(playerID uint64, err error) {
		if firstErr == nil {
			firstErr, samplePlayer = err, playerID
		}
	}
	defer func() {
		if queryFailed+admitFailed == 0 {
			return
		}
		plog.With(ctx).Warnw("msg", "owner_admit_census_weak_failed",
			"players", len(players), "query_failed", queryFailed, "admit_failed", admitFailed,
			"sample_player_id", samplePlayer, "first_err", firstErr,
			"hint", "migrate 弱依赖,再入屏障双门兜底")
	}()
	now := time.Now()
	for _, playerID := range players {
		key := selfUID + "|" + fmt.Sprintf("%d", playerID)
		if v, ok := admitted.Load(key); ok {
			// 命中即续期(接近过期才写):活实例项恒新鲜,仅死实例项会老化被 sweep 清。
			if t, isTime := v.(time.Time); !isTime || now.Sub(t) > ownerAdmittedStaleTTL/2 {
				admitted.Store(key, now)
			}
			continue
		}
		if budgetCtx.Err() != nil {
			return // 预算耗尽:剩余玩家下次心跳继续(census 每 ~5s 一轮,自然收敛)。
		}
		rec, err := auth.QueryOwner(budgetCtx, playerID)
		if err != nil {
			queryFailed++
			noteFail(playerID, err)
			continue
		}
		if rec.OwnerType != ownerType || rec.PodName != selfPod || rec.InstanceUID != selfUID {
			continue // 记录不指向本实例(迁移中/漂移),不是本实例可断言的准入。
		}
		if rec.Phase == ownerPhaseAdmitted {
			admitted.Store(key, now)
			continue
		}
		if rec.Phase != ownerPhasePending {
			continue
		}
		target := data.OwnerTargetView{
			PodName: rec.PodName, InstanceUID: rec.InstanceUID, InstanceEpoch: rec.InstanceEpoch,
			AssignmentOrAllocationID: rec.AssignmentOrAllocationID, ReleaseTrack: rec.ReleaseTrack,
		}
		retryAfter, aerr := auth.Admit(budgetCtx, playerID, rec.OwnerEpoch, rec.OperationID, target)
		switch {
		case aerr == nil:
			admitted.Store(key, now)
		case retryAfter > 0:
			// 屏障未开:预期中的 WAIT,下次心跳重试,不告警刷屏。
		default:
			admitFailed++
			noteFail(playerID, aerr)
		}
	}
}
