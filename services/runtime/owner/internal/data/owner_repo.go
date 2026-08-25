// Package data — owner 权威数据层(pandora_owner 库,owner-authority.md §2/§3)。
//
// 一致性核心(§9.22):owner_record 行是每玩家的串行化锚点——所有 transition 先
// SELECT ... FOR UPDATE 锁该行,epoch 单调 CAS、admit_not_before 计算(同事务 FOR UPDATE
// 读旧实例租约行,取 CAS 线性化点观察值)、PENDING→ADMITTED 推进全部在同一事务内完成。
// 锁序固定 owner_record → ds_instance_lease,Renew 只锁 lease 行,无环无死锁。
//
// SQL 写法 TiDB 安全:只锁存在行 + 条件更新,不依赖间隙锁(生产 TiDB / dev 单机 MySQL 同构)。
package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/placement"
)

// OwnerType 取值(对齐 owner.proto OwnerType)。
const (
	OwnerTypeNone   int8 = 0
	OwnerTypeHub    int8 = 1
	OwnerTypeBattle int8 = 2
)

// OwnerPhase 取值(对齐 owner.proto OwnerPhase)。
const (
	OwnerPhaseNone     int8 = 0
	OwnerPhasePending  int8 = 1
	OwnerPhaseAdmitted int8 = 2
)

// renewSlowProbeThreshold 是续租分段计时的打印阈值。
//
// 为什么要这个探针:owner 的 RPC 延迟是「中位 1ms、P90 65ms、最大 19397ms」的极端长尾,
// 而外部指标已逐条排除行锁争用(Innodb_row_lock_waits 仅 4 次,慢调用却有 148 次)、
// fsync(直连实测提交 ≤51ms)、连接池饥饿(MaxOpen=32 远未打满)与基础设施
// (同集群 player_locator / team / login 慢调用为 0)。剩余可能都在本函数内部,
// 而从进程外无法再分辨是卡在取连接、SQL 还是提交。
//
// 取 300ms:中位是 1ms,正常调用不可能触发;而观测到的长尾都在秒级,不会漏。
const renewSlowProbeThreshold = 300 * time.Millisecond

// transition 审计 op 取值。
const (
	transitionOpBegin   int8 = 1
	transitionOpAdmit   int8 = 2
	transitionOpRelease int8 = 3
)

// barrierWaitInfoThresholdMs 是「屏障未开」从 Debug 升 Info 的剩余等待阈值。
//
// 为什么不整条 Debug(2026-08-13 可诊断性审计):线上默认 info 级,整条 Debug =
// 「玩家匹配好了却进不去」最常见的落点永远查不到。也不能整条 Info:调用方按
// wait_ms 轮询,BATTLE→* 迁移的屏障可长达 ~27s,短剩余量的收尾轮询会刷屏。
// 取 5s:HUB 旧 owner 分支屏障恒为 0(不会触发),BATTLE 分支的实质等待必然远超它。
const barrierWaitInfoThresholdMs int64 = 5000

// transitionDetailMaxLen 是 owner_transition_log.detail 列宽(VARCHAR(512))。
//
// 必须在 Go 侧钳制:sql_mode 含 STRICT_TRANS_TABLES(§9.24),超长写入是 Error 1406
// 而不是截断——那会让一次**本该成功的 owner 迁移**因为审计字段太长而整事务失败。
// 审计流水信息缺一截可以接受,玩家进不去场景不可以。
const transitionDetailMaxLen = 512

// transitionDetail 把一次迁移的 exact 实例身份编码进审计流水的 detail 列。
//
// 为什么不能只写 pod_name(原实现):Agones 下 Pod 名会被复用,同名 Pod 重建后两行
// detail 完全一样;而 locator 侧的 join key 是 assignment_id、allocator 侧是
// allocation_id / match_id —— 只有 pod 名时,「这次 owner 迁移对应哪次 hub assignment /
// 哪局对局」永远接不上。admit_not_before_ms 则是屏障时刻的唯一持久证据。
//
// 格式是给人读的 key=value 串,**没有任何读取方**(全仓仅本文件写入),
// 因此不构成对外契约;新增字段只往后追加。
func transitionDetail(t OwnerTarget, admitNotBeforeMs int64, fromPod string) string {
	s := fmt.Sprintf("pod=%s uid=%s iepoch=%d aid=%s track=%s anb=%d",
		t.PodName, t.InstanceUID, t.InstanceEpoch, t.AssignmentOrAllocationID, t.ReleaseTrack, admitNotBeforeMs)
	if fromPod != "" {
		s += " from_pod=" + fromPod
	}
	if len(s) > transitionDetailMaxLen {
		s = s[:transitionDetailMaxLen]
	}
	return s
}

// admitMismatchReason 把 Admit 那个「任一项不匹配都拒」的合取条件拆成单一枚举 reason
// (§11.3 R2:一个 if 收敛了 N 个条件的,必须拆成 N 个 reason)。判定顺序与 if 内一致。
func admitMismatchReason(found bool, rec OwnerRecord, ownerEpoch uint64, operationID string, target OwnerTarget) string {
	switch {
	case !found:
		return "record_absent"
	case rec.OwnerEpoch != ownerEpoch:
		return "epoch_mismatch"
	case rec.OperationID != operationID:
		return "operation_mismatch"
	case rec.OwnerType == OwnerTypeNone:
		return "owner_type_none"
	case !rec.Target.Equal(target):
		return "target_mismatch"
	}
	return "unknown"
}

// releaseNoopReason 同上,拆 Release 的迟到 no-op 合取条件。
func releaseNoopReason(found bool, rec OwnerRecord, ownerEpoch uint64, operationID string) string {
	switch {
	case !found:
		return "record_absent"
	case rec.OwnerEpoch != ownerEpoch:
		return "epoch_mismatch"
	case rec.OperationID != operationID:
		return "operation_mismatch"
	case rec.OwnerType == OwnerTypeNone:
		return "already_released"
	}
	return "unknown"
}

// logTransitionNoop 记录 BeginTransition 的两条 no-op 收敛分支(§11.3 R1)。
//
// 为什么这也算「阶段推进」:allocator 反复投递同一实例而玩家实际卡住时,现象是
// 「Begin 一直返回 OK 但 epoch 不动」。成功路径与 no-op 路径若都不打日志,
// 「权威认为已经在这台了」与「权威根本没收到请求」在日志上完全无法区分。
// 每次真实进场至多几条(重连 / 重复交付),不属高频路径。
func logTransitionNoop(ctx context.Context, kind string, playerID uint64, rec OwnerRecord, reqTarget OwnerTarget) {
	plog.With(ctx).Infow("msg", "owner_transition_noop",
		"player_id", playerID, "kind", kind,
		"owner_epoch", rec.OwnerEpoch, "owner_type", rec.OwnerType, "phase", rec.Phase,
		"operation_id", rec.OperationID,
		"pod", rec.Target.PodName, "instance_uid", rec.Target.InstanceUID,
		"instance_epoch", rec.Target.InstanceEpoch,
		"cur_assignment_id", rec.Target.AssignmentOrAllocationID,
		"req_assignment_id", reqTarget.AssignmentOrAllocationID,
		"release_track", rec.Target.ReleaseTrack,
		"admit_not_before_ms", rec.AdmitNotBeforeMs,
		"lease_deadline_ms", rec.LeaseDeadlineMs)
}

// OwnerTarget exact DS 实例身份(对齐 pkg/placement.Target 语义;同名换实例不相等)。
type OwnerTarget struct {
	PodName                  string
	InstanceUID              string
	InstanceEpoch            uint32
	AssignmentOrAllocationID string
	ReleaseTrack             string
}

// Equal 四元组 + 分配 ID + 轨道全等(§9.22 exact 匹配)。
func (t OwnerTarget) Equal(o OwnerTarget) bool {
	return t.PodName == o.PodName && t.InstanceUID == o.InstanceUID &&
		t.InstanceEpoch == o.InstanceEpoch &&
		t.AssignmentOrAllocationID == o.AssignmentOrAllocationID &&
		t.ReleaseTrack == o.ReleaseTrack
}

// Complete 实例身份完整性(pod/uid/epoch/track/分配 ID 全非空)。
func (t OwnerTarget) Complete() bool {
	return strings.TrimSpace(t.PodName) != "" && strings.TrimSpace(t.InstanceUID) != "" &&
		t.InstanceEpoch > 0 && strings.TrimSpace(t.AssignmentOrAllocationID) != "" &&
		strings.TrimSpace(t.ReleaseTrack) != ""
}

// OwnerRecord 每玩家 owner 权威记录(LeaseDeadlineMs 为派生字段:同事务读实例租约)。
type OwnerRecord struct {
	PlayerID         uint64
	OwnerEpoch       uint64
	OwnerType        int8
	Phase            int8
	Target           OwnerTarget
	OperationID      string
	AdmitNotBeforeMs int64
	LeaseDeadlineMs  int64
	UpdatedAtMs      int64

	// HubSourceRevision 该玩家的 **Hub 来源版本高水位**(INC-20260818-003)。
	//
	// 它与 OwnerEpoch 是两个不同维度,别混:
	//   - OwnerEpoch 回答「谁**后**提交」——由 Owner 自己在 CAS 时 +1;
	//   - HubSourceRevision 回答「谁的**来源**更新」——由 hub_allocator 在真正改变
	//     target 的 assignment CAS 上领号,Owner 只负责比较与持久化。
	// 事故反例里旧 binary 恰好能拿到合法的 expect_epoch(它先 Begin 后 CAS),
	// 所以只靠 epoch 挡不住它;能挡住的只有来源版本。
	//
	// **只前进,永不清零**:Release 与 BATTLE 迁移都不动它。清零等于「打完一局回大厅」
	// 就把门重新对 legacy(0)敞开,滚动窗口里的旧写者随即又能写进来。
	HubSourceRevision uint64
}

// OwnerRepo 是 owner 权威数据层抽象。
type OwnerRepo interface {
	// Query 读当前记录(无行返回 epoch=0/none;附带派生 lease 截止)。
	Query(ctx context.Context, playerID uint64) (OwnerRecord, error)

	// BeginTransition CAS expect_epoch → epoch+1/PENDING/newTarget;admit_not_before 按旧
	// owner 类型分流:旧 owner=BATTLE → 同事务读旧实例租约,= max(now, 旧 deadline)+skewMargin
	// (失联对局 DS 的双可玩/迟到写风险);旧 owner=HUB 或无 → now(协作迁移,双写由 epoch
	// fencing 拦,双可玩由客户端单连接拆链拦;详见实现处举证)。
	// 同 (player, operationID) 幂等重放。expect 不符 → ErrOwnerEpochConflict(附当前记录)。
	BeginTransition(ctx context.Context, playerID, expectEpoch uint64, operationID string, ownerType int8, target OwnerTarget, sourceRevision uint64, skewMargin time.Duration) (OwnerRecord, error)

	// Admit 屏障开 + epoch/operation/实例全等 → PENDING→ADMITTED;已 ADMITTED 幂等重放。
	// 屏障未开 → ErrOwnerBarrierNotOpen(retryAfterMs>0)。
	Admit(ctx context.Context, playerID, ownerEpoch uint64, operationID string, target OwnerTarget) (rec OwnerRecord, retryAfterMs int64, err error)

	// RenewInstanceLease 实例租约续期(deadline 只前进;实例纪元不符拒)。返回生效截止。
	RenewInstanceLease(ctx context.Context, target OwnerTarget, lease time.Duration) (int64, error)

	// Release epoch+operation 匹配 → 置 none(epoch 保留);不匹配(迟到)幂等 no-op 返回当前。
	// 释放 BATTLE 归属时**必须把再入屏障盖进 admit_not_before 留存**(INC-20260824-003):
	// 屏障的判据是 owner_type=BATTLE + instance_uid,而本操作正要抹掉这两列,不留存等于
	// 亲手把屏障降到 0。skewMargin 与 BeginTransition 同源同值。
	Release(ctx context.Context, playerID, ownerEpoch uint64, operationID string, skewMargin time.Duration) (OwnerRecord, error)

	// SweepTransitionLog 删除超保留期审计行(有界批量)。
	SweepTransitionLog(ctx context.Context, retention time.Duration, batch int) (int64, error)
}

// MySQLOwnerRepo 基于 database/sql 的实现(生产连 TiDB,dev 连单机 MySQL;DDL 同构)。
type MySQLOwnerRepo struct {
	db *sql.DB
	// rejectLegacySourceRevision 见 SetRejectLegacySourceRevision。默认 false = 兼容窗。
	rejectLegacySourceRevision bool
}

// NewMySQLOwnerRepo 构造。
func NewMySQLOwnerRepo(db *sql.DB) *MySQLOwnerRepo {
	return &MySQLOwnerRepo{db: db}
}

func nowUnixMs() int64 { return time.Now().UnixMilli() }

// scanRecordRow 读 owner_record 行(锁定与否由调用 SQL 决定)。无行 → zero 记录 + false。
func scanRecordRow(row *sql.Row, playerID uint64) (OwnerRecord, bool, error) {
	rec := OwnerRecord{PlayerID: playerID}
	err := row.Scan(&rec.OwnerEpoch, &rec.OwnerType, &rec.Phase,
		&rec.Target.PodName, &rec.Target.InstanceUID, &rec.Target.InstanceEpoch,
		&rec.Target.AssignmentOrAllocationID, &rec.Target.ReleaseTrack,
		&rec.OperationID, &rec.AdmitNotBeforeMs, &rec.HubSourceRevision, &rec.UpdatedAtMs)
	if errors.Is(err, sql.ErrNoRows) {
		return rec, false, nil
	}
	if err != nil {
		return rec, false, errcode.New(errcode.ErrInternal, "scan owner_record player=%d: %v", playerID, err)
	}
	return rec, true, nil
}

const selectRecordCols = `SELECT owner_epoch, owner_type, phase, pod_name, instance_uid, instance_epoch,
 assignment_or_allocation_id, release_track, operation_id, admit_not_before_ms, hub_source_revision, updated_at_ms
 FROM owner_record WHERE player_id = ?`

// lockRecordTx 确保并锁定 owner_record 行(无行则建 epoch=0/none 再锁)。
func lockRecordTx(ctx context.Context, tx *sql.Tx, playerID uint64) (OwnerRecord, error) {
	const ins = `INSERT IGNORE INTO owner_record (player_id, updated_at_ms) VALUES (?, ?)`
	if _, ierr := tx.ExecContext(ctx, ins, playerID, nowUnixMs()); ierr != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "ensure owner_record player=%d: %v", playerID, ierr)
	}
	rec, found, err := scanRecordRow(tx.QueryRowContext(ctx, selectRecordCols+` FOR UPDATE`, playerID), playerID)
	if err != nil {
		return OwnerRecord{}, err
	}
	if !found {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "owner_record vanished after ensure player=%d", playerID)
	}
	return rec, nil
}

// readLeaseDeadline 读实例租约截止(forUpdate 决定是否锁行;无行返回 0)。
func readLeaseDeadline(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, instanceUID string, forUpdate bool) (int64, error) {
	if strings.TrimSpace(instanceUID) == "" {
		return 0, nil
	}
	query := `SELECT lease_deadline_ms FROM ds_instance_lease WHERE instance_uid = ?`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var deadline int64
	err := q.QueryRowContext(ctx, query, instanceUID).Scan(&deadline)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "read lease uid=%s: %v", instanceUID, err)
	}
	return deadline, nil
}

func appendTransitionLog(ctx context.Context, tx *sql.Tx, playerID, fromEpoch, toEpoch uint64, op int8, operationID, detail string) error {
	const ins = `INSERT INTO owner_transition_log (player_id, from_epoch, to_epoch, op, operation_id, detail)
VALUES (?, ?, ?, ?, ?, ?)`
	if _, err := tx.ExecContext(ctx, ins, playerID, fromEpoch, toEpoch, op, operationID, detail); err != nil {
		return errcode.New(errcode.ErrInternal, "append transition log player=%d: %v", playerID, err)
	}
	return nil
}

func (r *MySQLOwnerRepo) Query(ctx context.Context, playerID uint64) (OwnerRecord, error) {
	// 读事务:record + 派生 lease 两读同快照(§9.22 状态按语义拆开,查询不落缓存)。
	//
	// ⚠️ 不能用 sql.TxOptions{ReadOnly: true}(2026-07-27 审计,实测):go-sql-driver 会把它
	// 翻成 `START TRANSACTION READ ONLY`,而 TiDB 在默认 tidb_enable_noop_functions=OFF 下
	// 直接返回 `Error 1235: function READ ONLY has only noop implementation in tidb now`。
	// owner 生产被 -Prod 机械注入 require_tidb: true 强制连 TiDB,而 Query 是 owner 唯一读路径
	// —— 保留 ReadOnly 等于 owner 一上生产就 100% 读失败(dev 走单机 MySQL 所以永远测不出来)。
	// 普通事务同样满足这里需要的「两读同快照」:TiDB 按 start_ts 取快照,InnoDB 按 RR 读视图。
	// 不要改用 DSN 打开 tidb_enable_noop_functions 绕过 —— 那是让 TiDB 假装接受语义它并不实现。
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "begin query tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	rec, found, serr := scanRecordRow(tx.QueryRowContext(ctx, selectRecordCols, playerID), playerID)
	if serr != nil {
		return OwnerRecord{}, serr
	}
	if !found {
		return OwnerRecord{PlayerID: playerID}, nil // epoch=0 / none(从未有 owner)
	}
	if rec.OwnerType != OwnerTypeNone {
		deadline, derr := readLeaseDeadline(ctx, tx, rec.Target.InstanceUID, false)
		if derr != nil {
			return OwnerRecord{}, derr
		}
		rec.LeaseDeadlineMs = deadline
	}
	if cerr := tx.Commit(); cerr != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "commit query player=%d: %v", playerID, cerr)
	}
	return rec, nil
}

// sourceRevisionDecision 是来源版本闸门的判定结果。reason 只进日志与错误文案:
// 五种拒因在排障时含义完全不同(旧写者 vs 迟到写 vs 铸号重复),合并成一句会白丢线索。
type sourceRevisionDecision struct {
	allow bool
	// advance 表示本次放行**应当**把高水位推进到 incoming。
	//
	// 它是判定的**声明**,不是执行:写入路径刻意用 `max(旧水位, incoming)` 而不是
	// `if advance { 水位 = incoming }`。两者在当前矩阵下等价,但取 max 在矩阵将来被扩写时
	// 更安全 —— 万一有人加出一条 allow=true / advance=false 却 incoming>高水位 的分支,
	// max 仍然把水位顶上去(水位越高越安全),按 advance 分支则会把水位留在低位,
	// 等于给更旧的来源留了一道缝。所以本字段目前只被判定矩阵的测试与排障读。
	advance bool
	reason  string // 稳定的机器可读原因(日志 join key)
}

// classifySourceRevision 是 INC-20260818-003 的判定矩阵本体(纯函数,与 DB 无关)。
//
//	highWater  = 该玩家已见过的最大 Hub 来源版本(0 = 从未见过带版本的写者)
//	incoming   = 本次 Begin 携带的版本(0 = legacy,调用方尚未滚上本协议)
//	sameTarget = 本次 target 与记录里的 target 是否全等
//	rejectLegacy = 全局 legacy 拒绝门(分阶段发布最后一步才打开)
//
// 判定表(顺序即优先级):
//
//	incoming=0 且 rejectLegacy         → 拒:旧写者已宣称排空,不该再有 legacy 请求
//	incoming=0 且 highWater>0          → 拒:该玩家见过版本,legacy 永久出局(逐玩家自动生效)
//	incoming=0 且 highWater=0          → 放行、不推进:兼容窗内的正常旧写者
//	incoming<highWater                 → 拒:来源更旧(事故反例里迟到的 R1/R2 就落在这)
//	incoming=highWater 且 sameTarget   → 放行、不推进:同一来源的重复投递,幂等
//	incoming=highWater 且 !sameTarget  → 拒:同一个版本号不可能产出两个 target,
//	                                        出现即说明铸号被复制(两个写者共用了同一任期)
//	incoming>highWater                 → 放行并推进:唯一的正常前进路径
//
// 注意「放行」不等于「一定会写」:放行只是过了本闸,后面还有 epoch CAS 与各条 no-op 分支。
func classifySourceRevision(highWater, incoming uint64, sameTarget, rejectLegacy bool) sourceRevisionDecision {
	if incoming == placement.SourceRevisionLegacy {
		switch {
		case rejectLegacy:
			return sourceRevisionDecision{reason: "legacy_rejected_globally"}
		case highWater > 0:
			return sourceRevisionDecision{reason: "legacy_after_versioned"}
		default:
			return sourceRevisionDecision{allow: true, reason: "legacy_compat_window"}
		}
	}
	switch {
	case incoming < highWater:
		return sourceRevisionDecision{reason: "older_than_high_water"}
	case incoming == highWater && sameTarget:
		return sourceRevisionDecision{allow: true, reason: "same_revision_same_target"}
	case incoming == highWater:
		return sourceRevisionDecision{reason: "same_revision_different_target"}
	default:
		return sourceRevisionDecision{allow: true, advance: true, reason: "advances_high_water"}
	}
}

// SetRejectLegacySourceRevision 打开 / 关闭**全局** legacy(revision=0)拒绝门。
//
// 它是 INC-20260818-003 §3 分阶段发布的最后一步,只有在**证明旧 hub_allocator 已排空**
// 之后才允许打开;打开后任何不带来源版本的 Begin 一律被拒。默认关闭 = 兼容窗行为。
//
// 逐玩家的那条规则(见过非零版本就永久拒 legacy)不受本开关控制,它从第一个新写者写下
// 第一个非零版本起就自动生效 —— 那条不需要人来拍时机,所以不该做成开关。
func (r *MySQLOwnerRepo) SetRejectLegacySourceRevision(v bool) { r.rejectLegacySourceRevision = v }

func (r *MySQLOwnerRepo) BeginTransition(ctx context.Context, playerID, expectEpoch uint64, operationID string, ownerType int8, target OwnerTarget, sourceRevision uint64, skewMargin time.Duration) (OwnerRecord, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "begin transition tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	rec, lerr := lockRecordTx(ctx, tx, playerID)
	if lerr != nil {
		return OwnerRecord{}, lerr
	}

	// ── 来源版本闸门(INC-20260818-003)────────────────────────────────────────
	//
	// 必须在**所有**后续分支之前:事故反例里旧 binary 手上握着一个**合法**的 expect_epoch
	// (它先 Begin 后 CAS),所以 epoch 检查放不倒它;能判定"谁的来源更新"的只有本闸。
	// 比较与提交在同一行锁事务内,不存在读到旧水位再写的窗口。
	//
	// **只对 HUB 生效**:来源版本由 hub_allocator 领号,BATTLE 迁移不带号也不动水位。
	// 若这里对 BATTLE 也比较,battle 的 revision=0 会被"见过非零就拒 legacy"那条挡下,
	// 玩家将永远进不了战斗 —— 这是本改动最容易踩的一脚。
	if ownerType == OwnerTypeHub {
		decision := classifySourceRevision(rec.HubSourceRevision, sourceRevision,
			rec.Target.Equal(target), r.rejectLegacySourceRevision)
		if !decision.allow {
			plog.With(ctx).Warnw("msg", "owner_source_revision_rejected",
				"player_id", playerID, "reason", decision.reason,
				"incoming_revision", sourceRevision, "high_water", rec.HubSourceRevision,
				"current_epoch", rec.OwnerEpoch, "expect_epoch", expectEpoch,
				"operation_id", operationID, "same_target", rec.Target.Equal(target),
				"hint", "来源更旧的 hub assignment 被拒;调用方应重查自身 assignment,不要拿更大的 epoch 重试")
			return rec, errcode.New(errcode.ErrOwnerSourceRevisionStale,
				"stale hub source revision player=%d incoming=%d high_water=%d reason=%s",
				playerID, sourceRevision, rec.HubSourceRevision, decision.reason)
		}
	}

	// 高水位推进不能被下面两条 no-op 早退分支吞掉(INC-20260818-003 复审)。
	//
	// hub 侧 backfillSourceRevision 把存量 legacy(0)补成 R 时,target 一个字节都不变,
	// 于是这次 Begin 必然落到 idempotent_replay 或 same_target 的 return —— 而推水位的
	// 代码在它们**下游**(见下方 newHighWater),永远走不到。结果是 assignment 侧已经
	// 有号、owner 侧水位永久停在 0,「某玩家见过非零版本就永久拒 legacy」这条逐玩家
	// 防线对这批玩家从不 arm,只剩全局开关一道保护。
	//
	// 闸门在上面已经判过 allow,这里只负责落库,不重复判定。
	advanceHubSourceRevision := func() error {
		if ownerType != OwnerTypeHub || sourceRevision <= rec.HubSourceRevision {
			return nil
		}
		if _, uerr := tx.ExecContext(ctx,
			`UPDATE owner_record SET hub_source_revision = ?, updated_at_ms = ? WHERE player_id = ?`,
			sourceRevision, nowUnixMs(), playerID); uerr != nil {
			return errcode.New(errcode.ErrInternal,
				"advance hub source revision player=%d: %v", playerID, uerr)
		}
		rec.HubSourceRevision = sourceRevision
		return nil
	}

	// 幂等重放:同 operation 且记录就是本次 Begin 的结果(epoch=expect+1 / 目标全等)。
	// 响应丢失后的原样重试拿回同一结果,不再推进 epoch(§9.23 端到端幂等)。
	// operationID 为空时本分支不适用(空 = 调用方未持显式幂等键,交由下面的同实例收敛)。
	if operationID != "" && rec.OperationID == operationID && rec.OwnerEpoch == expectEpoch+1 &&
		rec.OwnerType == ownerType && rec.Target.Equal(target) {
		deadline, derr := readLeaseDeadline(ctx, tx, rec.Target.InstanceUID, false)
		if derr != nil {
			return OwnerRecord{}, derr
		}
		rec.LeaseDeadlineMs = deadline
		if aerr := advanceHubSourceRevision(); aerr != nil {
			return OwnerRecord{}, aerr
		}
		if cerr := tx.Commit(); cerr != nil {
			return OwnerRecord{}, errcode.New(errcode.ErrInternal, "commit replay begin player=%d: %v", playerID, cerr)
		}
		logTransitionNoop(ctx, "idempotent_replay", playerID, rec, target)
		return rec, nil
	}

	// 同 exact owner 身份的重复投递:在本行锁事务内收敛为 no-op,原样返回既有记录
	// (不推进 epoch、不改 phase、不覆盖 operation_id)。
	//
	// 这段判定原先在调用方(两个 allocator 的 decideOwnerBegin:Query → 本地比对 → Begin)。
	// 移进事务的理由:
	//   ① **operation_id 必须稳定**(§9.23「一次真实进场 / owner 迁移使用一个稳定
	//      operation_id」)。调用方每次 Begin 现铸 uuid.NewString(),同一次进场的重复
	//      投递(重连、重复交付、心跳自愈)会写出不同 operation,幂等键形同虚设。改由权威
	//      在同目标时原样返回既有记录后,operation_id 天然贯穿整条链,客户端也能从
	//      ResumeContext 拿到同一个值续用。
	//   ② **判定与写入落在同一线性化点**。调用方那份判定是建议性的:Query 与 Begin 之间
	//      记录可能已变,expectEpoch 随之作废,只能靠 EPOCH_CONFLICT 兜底重查——而"目标
	//      已经是它"本就该是 no-op,不该先冲突再重来。
	//
	// assignment_or_allocation_id 是票据/准入所绑定的归属版本,release_track 也是 exact
	// 身份的一部分；二者任一变化都必须走下方 epoch CAS。只按物理实例做 no-op 会产生两类
	// 安全问题：
	//   - 新 assignment 直接覆写旧 epoch 后，迟到的旧 Begin 可再把它回滚；UUID 本身无序，
	//     没有办法判定哪次“刷新”更新；
	//   - BATTLE allocation 或 release track 的变化会继承旧 epoch/ADMITTED phase，让旧票与
	//     新归属共享 fencing 版本。
	// 因而 no-op 必须要求完整 Target.Equal。物理实例未变但 assignment/track 变化仍是一次
	// 真实 owner 身份迁移：epoch+1、PENDING、新 operation；旧 epoch 随即失效。
	if rec.OwnerType == ownerType && rec.Target.Equal(target) &&
		(rec.Phase == OwnerPhasePending || rec.Phase == OwnerPhaseAdmitted) {
		deadline, derr := readLeaseDeadline(ctx, tx, rec.Target.InstanceUID, false)
		if derr != nil {
			return OwnerRecord{}, derr
		}
		rec.LeaseDeadlineMs = deadline
		if aerr := advanceHubSourceRevision(); aerr != nil {
			return OwnerRecord{}, aerr
		}
		if cerr := tx.Commit(); cerr != nil {
			return OwnerRecord{}, errcode.New(errcode.ErrInternal, "commit same-instance begin player=%d: %v", playerID, cerr)
		}
		logTransitionNoop(ctx, "same_target", playerID, rec, target)
		return rec, nil
	}

	if rec.OwnerEpoch != expectEpoch {
		// CAS 期望不符:附当前记录返回,调用方重查再决策(禁盲重试推进 epoch)。
		// 单次冲突是 §9.23 query-first 正常竞争(故 INFO);同一 player 高频冲突 = 两个调用方
		// 在抢 owner 迁移,靠此可观测频率与双方 epoch(否则 in-band 业务码被 access log 记 DEBUG)。
		plog.With(ctx).Infow("msg", "owner_epoch_conflict",
			"player_id", playerID, "expect_epoch", expectEpoch, "current_epoch", rec.OwnerEpoch,
			"operation_id", operationID)
		return rec, errcode.New(errcode.ErrOwnerEpochConflict,
			"epoch conflict player=%d expect=%d current=%d", playerID, expectEpoch, rec.OwnerEpoch)
	}

	// 到这里必定是**真实迁移**(要写新记录),operation_id 不能为空:空 operation 会让后续
	// Admit 的 exact 校验、客户端续用与审计流水同时失去锚点。biz 层保证非空(空则铸新),
	// 本行是防止绕过 biz 的内部调用写出无锚点记录的兜底。
	if operationID == "" {
		return OwnerRecord{}, errcode.New(errcode.ErrOwnerInvalidOperation,
			"operation_id required for a real transition player=%d", playerID)
	}

	// admit_not_before:按旧 owner 类型分流(2026-08-03,实测事故:大厅每次进战斗都要干等
	// ~27s 屏障,客户端 30s 权威等待窗口随之耗尽,玩家看到的是"匹配没反应")。
	//
	// ① 旧 owner 是 **BATTLE**:保守屏障 = max(now, 旧实例租约截止) + 时钟/网络余量。
	//    失联战斗 DS 上该玩家的 Pawn 仍可能被模拟(AI/其他玩家仍与之交互),且 DS 是受信
	//    写者(§9.6)可能有 journal 迟到写在途——"双可玩 + 迟到写"风险真实存在,必须等旧
	//    实例本地自 fencing 的最晚时刻(其租约截止)过去后才许新 DS 接管。
	//
	// ② 旧 owner 是 **HUB**:屏障 = now,不等实例租约。举证(§15.4):
	//    - 双写防护不靠屏障:hub 对该玩家的权威写全部走 §9.6 五要件,本 CAS 提交后
	//      owner_epoch 已 +1,旧 epoch 的写与时间无关地被下游拒(五要件③ fencing);
	//    - "双可玩"防护不靠屏障:玩家只有一个客户端,travel 去新 DS 时旧 hub 连接被
	//      客户端协调器主动拆除,hub 侧 Pawn 随连接断开清退;大厅 Pawn 无对局语义,
	//      不存在"残留 Pawn 继续演化影响权威态"(对局内那种风险正是 ① 保守的原因);
	//    - hub 与后端分区也不改变上述两条(fencing 在写入侧、连接归属在客户端侧)。
	//    等实例租约唯一"防住"的是——该实例上其它逻辑仍自认持有玩家——而那正是 epoch
	//    fencing 的职责。hub 实例租约被 allocator 持续代续(整机级,承载数百玩家,永不
	//    过期),对 HUB 旧 owner 等它 = 恒定 ~27s 纯延迟、零安全收益,且违反验收底线
	//    第 1 条(无收益的强制等待)。margin 也一并省去:本分支不依赖旧 DS 本地自
	//    fencing 时钟,Admit 的屏障判定与本处写入同库同钟,无跨机偏移可补。
	//
	// 无旧 owner → 无需屏障(没有要围栏的旧 DS)。
	now := nowUnixMs()
	admitNotBefore := now
	// 屏障算成多少、走的哪一支、读到的旧租约截止是多少 —— 这三件事此前只活在本函数栈上,
	// 落库后连表都还原不出(2026-08-03「匹配没反应」事故的根因正是这个值算大了)。
	// 提两个局部变量出来只为把它们打进日志,分流逻辑本身一字未动。
	barrierSource := "no_old_battle_owner"
	var oldLeaseDeadlineMs int64
	if rec.OwnerType == OwnerTypeBattle && rec.Target.InstanceUID != "" {
		oldDeadline, derr := readLeaseDeadline(ctx, tx, rec.Target.InstanceUID, true)
		if derr != nil {
			return OwnerRecord{}, derr
		}
		base := now
		if oldDeadline > base {
			base = oldDeadline
		}
		admitNotBefore = base + skewMargin.Milliseconds()
		barrierSource = "battle_old_lease"
		oldLeaseDeadlineMs = oldDeadline
	}
	// **屏障必须跨 Release 存活**(INC-20260824-003,2026-08-24)。
	//
	// 上面这套分流的判据是「**当前**记录还指向哪台 BATTLE 实例」。可 Release 的 UPDATE
	// 恰好把 owner_type / instance_uid 抹空 —— 于是「先释放、再迁移」这条顺序会让本函数
	// 落到 `no_old_battle_owner`,屏障塌成 now,而那台旧战斗 DS 可能仍活着。
	// 这不是某一条调用链的疏忽:登出释放(login 侧判据只有 owner_type != 0)对 BATTLE 归属
	// 一视同仁,对局中登出再重登早就走在这条路上。
	//
	// 修法是把屏障**从「归属指针的派生量」改成「玩家这一行的留存事实」**:Release 时把
	// 算好的屏障盖进 admit_not_before(见本文件 Release),这里再取 max 认回来。
	// 于是「谁持有归属」与「何时才允许再次可玩」彻底解耦,安全性不再依赖
	// 「记得在实例回收之后才释放」这种调用方纪律。
	//
	// 取 max 而不是直接采用:①屏障只前进,陈旧留存值(早已过期)天然失效,不会永久卡人;
	// ②旧 owner 仍是 BATTLE 时上面算出的值已含本实例租约,与留存值取大者仍然正确。
	if rec.AdmitNotBeforeMs > admitNotBefore {
		admitNotBefore = rec.AdmitNotBeforeMs
		barrierSource = "retained_released_battle"
	}

	// 高水位只前进,且**只有 HUB 迁移能推它**。
	//
	// 取 max 而不是直接赋值:闸门已经保证 sourceRevision >= 高水位,但兼容窗内合法的
	// legacy(0)也会走到这里 —— 直接赋值会把已经建立起来的水位打回 0,等于亲手把门重新
	// 对旧写者敞开。BATTLE 分支原样带走旧值(Release 的 UPDATE 不含本列,天然保留)。
	newHighWater := rec.HubSourceRevision
	if ownerType == OwnerTypeHub && sourceRevision > newHighWater {
		newHighWater = sourceRevision
	}

	newRec := OwnerRecord{
		PlayerID:          playerID,
		OwnerEpoch:        rec.OwnerEpoch + 1,
		OwnerType:         ownerType,
		Phase:             OwnerPhasePending,
		Target:            target,
		OperationID:       operationID,
		AdmitNotBeforeMs:  admitNotBefore,
		HubSourceRevision: newHighWater,
		UpdatedAtMs:       now,
	}
	const upd = `UPDATE owner_record SET owner_epoch = ?, owner_type = ?, phase = ?, pod_name = ?,
 instance_uid = ?, instance_epoch = ?, assignment_or_allocation_id = ?, release_track = ?,
 operation_id = ?, admit_not_before_ms = ?, hub_source_revision = ?, updated_at_ms = ? WHERE player_id = ?`
	if _, uerr := tx.ExecContext(ctx, upd, newRec.OwnerEpoch, newRec.OwnerType, newRec.Phase,
		newRec.Target.PodName, newRec.Target.InstanceUID, newRec.Target.InstanceEpoch,
		newRec.Target.AssignmentOrAllocationID, newRec.Target.ReleaseTrack,
		newRec.OperationID, newRec.AdmitNotBeforeMs, newRec.HubSourceRevision,
		newRec.UpdatedAtMs, playerID); uerr != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "cas owner_record player=%d: %v", playerID, uerr)
	}
	if aerr := appendTransitionLog(ctx, tx, playerID, rec.OwnerEpoch, newRec.OwnerEpoch,
		transitionOpBegin, operationID, transitionDetail(target, admitNotBefore, rec.Target.PodName)); aerr != nil {
		return OwnerRecord{}, aerr
	}
	newDeadline, derr := readLeaseDeadline(ctx, tx, target.InstanceUID, false)
	if derr != nil {
		return OwnerRecord{}, derr
	}
	newRec.LeaseDeadlineMs = newDeadline
	if cerr := tx.Commit(); cerr != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "commit begin player=%d: %v", playerID, cerr)
	}
	// §9.1 的唯一权威事实点(§11.3 R1):epoch 何时 N→N+1、从哪台 DS 换到哪台、
	// 屏障算成多久走的哪一支。这些只在 MySQL 行里而行会被下一次迁移覆盖 ——
	// 「玩家卡在旧 DS / 同时在两个 DS」时的时间线,缺了这条就无法还原。
	// 频次 = 每玩家每次真实进场一条(重复投递已在上面两条 no-op 分支收敛),非高频。
	plog.With(ctx).Infow("msg", "owner_transition_begun",
		"player_id", playerID,
		"from_epoch", rec.OwnerEpoch, "to_epoch", newRec.OwnerEpoch,
		"operation_id", operationID,
		"from_owner_type", rec.OwnerType, "owner_type", ownerType,
		"from_pod", rec.Target.PodName, "from_instance_uid", rec.Target.InstanceUID,
		"from_instance_epoch", rec.Target.InstanceEpoch,
		"from_assignment_id", rec.Target.AssignmentOrAllocationID,
		"to_pod", target.PodName, "to_instance_uid", target.InstanceUID,
		"to_instance_epoch", target.InstanceEpoch,
		"assignment_or_allocation_id", target.AssignmentOrAllocationID,
		"release_track", target.ReleaseTrack,
		"barrier_source", barrierSource,
		"admit_not_before_ms", admitNotBefore,
		"barrier_wait_ms", admitNotBefore-now,
		"old_lease_deadline_ms", oldLeaseDeadlineMs,
		"skew_margin_ms", skewMargin.Milliseconds(),
		"lease_deadline_ms", newDeadline)
	return newRec, nil
}

func (r *MySQLOwnerRepo) Admit(ctx context.Context, playerID, ownerEpoch uint64, operationID string, target OwnerTarget) (OwnerRecord, int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return OwnerRecord{}, 0, errcode.New(errcode.ErrInternal, "begin admit tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	rec, found, serr := scanRecordRow(tx.QueryRowContext(ctx, selectRecordCols+` FOR UPDATE`, playerID), playerID)
	if serr != nil {
		return OwnerRecord{}, 0, serr
	}
	if !found || rec.OwnerEpoch != ownerEpoch || rec.OperationID != operationID ||
		rec.OwnerType == OwnerTypeNone || !rec.Target.Equal(target) {
		// fail-closed:任何一项不匹配都拒(旧 epoch / 换代实例 / 伪造 operation 都进不来)。
		// owner fencing 的核心拒绝点(§9.22),恰是要能查到的脑裂 / stale writer 信号 → WARN。
		//
		// 两侧的 exact 实例身份必须并排打出来(§11.3 R3):这条日志是「玩家同时在两个 DS」
		// 的核心证据,只报一个 target_match=false 等于只知道「不匹配」,查不出是哪台 DS
		// 在试图接管、它自认是哪个 assignment。
		plog.With(ctx).Warnw("msg", "owner_admit_identity_mismatch",
			"player_id", playerID, "found", found,
			"reason", admitMismatchReason(found, rec, ownerEpoch, operationID, target),
			"req_epoch", ownerEpoch, "current_epoch", rec.OwnerEpoch,
			"req_op", operationID, "current_op", rec.OperationID,
			"target_match", rec.Target.Equal(target), "owner_type", rec.OwnerType,
			"cur_phase", rec.Phase,
			"req_pod", target.PodName, "req_instance_uid", target.InstanceUID,
			"req_instance_epoch", target.InstanceEpoch,
			"req_assignment_id", target.AssignmentOrAllocationID,
			"req_release_track", target.ReleaseTrack,
			"cur_pod", rec.Target.PodName, "cur_instance_uid", rec.Target.InstanceUID,
			"cur_instance_epoch", rec.Target.InstanceEpoch,
			"cur_assignment_id", rec.Target.AssignmentOrAllocationID,
			"cur_release_track", rec.Target.ReleaseTrack,
			"cur_updated_at_ms", rec.UpdatedAtMs)
		return rec, 0, errcode.New(errcode.ErrOwnerIdentityMismatch,
			"admit identity mismatch player=%d epoch=%d op=%s", playerID, ownerEpoch, operationID)
	}
	if rec.Phase == OwnerPhaseAdmitted {
		// 幂等重放:Admission 回包丢失 → 原样返回,不再分配、不创建第二 owner(§9.23)。
		if cerr := tx.Commit(); cerr != nil {
			return OwnerRecord{}, 0, errcode.New(errcode.ErrInternal, "commit replay admit player=%d: %v", playerID, cerr)
		}
		// 与 owner_admitted 分开命名:排查时必须能区分「本次真的完成了交接」与
		// 「回包丢了在重放」,否则看到 OK 却不知道玩家是刚进去还是早就进去了。
		plog.With(ctx).Infow("msg", "owner_admit_replayed",
			"player_id", playerID, "owner_epoch", ownerEpoch, "operation_id", operationID,
			"owner_type", rec.OwnerType, "pod", rec.Target.PodName,
			"instance_uid", rec.Target.InstanceUID, "instance_epoch", rec.Target.InstanceEpoch,
			"assignment_or_allocation_id", rec.Target.AssignmentOrAllocationID,
			"admitted_at_ms", rec.UpdatedAtMs)
		return rec, 0, nil
	}
	now := nowUnixMs()
	if now < rec.AdmitNotBeforeMs {
		// 屏障未开:WAIT 语义(§9.23),带剩余毫秒退避重试;安全优先但不永久卡(watchdog 驱动)。
		//
		// 级别按剩余等待量分档(2026-08-13 可诊断性审计,§11.3 R1):
		// 原实现整条 Debug,而线上默认 info —— 「玩家匹配好了却进不去」最常见的落点
		// 于是永不出现,运维只看到玩家反复 Admit 拿不到 ADMITTED,分不清是屏障没开
		// 还是身份不匹配。剩余 ≥ 阈值时升 Info(真的在等),收尾轮询仍留 Debug 不刷屏。
		waitMs := rec.AdmitNotBeforeMs - now
		h := plog.With(ctx)
		kvs := []any{"msg", "owner_admit_barrier_wait",
			"player_id", playerID, "owner_epoch", ownerEpoch, "operation_id", operationID,
			"owner_type", rec.OwnerType, "pod", rec.Target.PodName,
			"instance_uid", rec.Target.InstanceUID, "instance_epoch", rec.Target.InstanceEpoch,
			"assignment_or_allocation_id", rec.Target.AssignmentOrAllocationID,
			"admit_not_before_ms", rec.AdmitNotBeforeMs, "wait_ms", waitMs,
			"elapsed_since_begin_ms", now - rec.UpdatedAtMs}
		if waitMs >= barrierWaitInfoThresholdMs {
			h.Infow(kvs...)
		} else {
			h.Debugw(kvs...)
		}
		return rec, waitMs, errcode.New(errcode.ErrOwnerBarrierNotOpen,
			"admit barrier not open player=%d wait_ms=%d", playerID, waitMs)
	}
	const upd = `UPDATE owner_record SET phase = ?, updated_at_ms = ? WHERE player_id = ? AND owner_epoch = ?`
	if _, uerr := tx.ExecContext(ctx, upd, OwnerPhaseAdmitted, now, playerID, ownerEpoch); uerr != nil {
		return OwnerRecord{}, 0, errcode.New(errcode.ErrInternal, "admit update player=%d: %v", playerID, uerr)
	}
	admitNotBefore := rec.AdmitNotBeforeMs
	if aerr := appendTransitionLog(ctx, tx, playerID, rec.OwnerEpoch, rec.OwnerEpoch,
		transitionOpAdmit, operationID, transitionDetail(target, admitNotBefore, "")); aerr != nil {
		return OwnerRecord{}, 0, aerr
	}
	rec.Phase = OwnerPhaseAdmitted
	rec.UpdatedAtMs = now
	deadline, derr := readLeaseDeadline(ctx, tx, rec.Target.InstanceUID, false)
	if derr != nil {
		return OwnerRecord{}, 0, derr
	}
	rec.LeaseDeadlineMs = deadline
	if cerr := tx.Commit(); cerr != nil {
		return OwnerRecord{}, 0, errcode.New(errcode.ErrInternal, "commit admit player=%d: %v", playerID, cerr)
	}
	// §9.23 的服务端完成点(§11.3 R1):「玩家到底进没进新 DS」此前只能靠 DS 侧日志反推,
	// 屏障实际等了多久也算不出来(判据⑤)。barrier_waited_ms 就是从屏障时刻到提交的实测值。
	plog.With(ctx).Infow("msg", "owner_admitted",
		"player_id", playerID, "owner_epoch", ownerEpoch, "operation_id", operationID,
		"owner_type", rec.OwnerType,
		"pod", rec.Target.PodName, "instance_uid", rec.Target.InstanceUID,
		"instance_epoch", rec.Target.InstanceEpoch,
		"assignment_or_allocation_id", rec.Target.AssignmentOrAllocationID,
		"release_track", rec.Target.ReleaseTrack,
		"admit_not_before_ms", admitNotBefore,
		"barrier_waited_ms", now-admitNotBefore,
		"lease_deadline_ms", deadline)
	return rec, 0, nil
}

func (r *MySQLOwnerRepo) RenewInstanceLease(ctx context.Context, target OwnerTarget, lease time.Duration) (int64, error) {
	// 分段计时:owner 的 RPC 延迟呈现「中位 1ms、最大 19s」的长尾,而外部指标全部排除了
	// 行锁争用(Innodb_row_lock_waits 只有个位数)、fsync(实测 ≤51ms)、连接池饥饿
	// (MaxOpen=32 远未打满)与基础设施(同集群其它服务慢调用为 0)。剩下的可能都在本函数
	// 内部,而从外部无法再分辨。这里按阶段留时,只在超阈值时打印,不刷屏。
	stageStart := time.Now()
	var tBegin, tSelect, tCommit time.Duration

	tx, err := r.db.BeginTx(ctx, nil)
	tBegin = time.Since(stageStart)
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "begin renew tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	defer func() {
		total := time.Since(stageStart)
		if total < renewSlowProbeThreshold {
			return
		}
		// database/sql 的池等待是独立于本次调用的全局计数,一并带出:WaitCount 增长
		// 说明卡在拿连接而不是卡在 SQL,两者的修法完全不同。
		st := r.db.Stats()
		plog.With(ctx).Warnw("msg", "owner_renew_lease_slow",
			"instance_uid", target.InstanceUID, "pod", target.PodName,
			"total_ms", total.Milliseconds(),
			"begin_tx_ms", tBegin.Milliseconds(),
			"select_for_update_ms", tSelect.Milliseconds(),
			"commit_ms", tCommit.Milliseconds(),
			"pool_in_use", st.InUse, "pool_idle", st.Idle,
			"pool_wait_count", st.WaitCount, "pool_wait_ms", st.WaitDuration.Milliseconds())
	}()

	now := nowUnixMs()
	newDeadline := now + lease.Milliseconds()

	var (
		storedEpoch    uint32
		storedDeadline int64
	)
	selStart := time.Now()
	qerr := tx.QueryRowContext(ctx,
		`SELECT instance_epoch, lease_deadline_ms FROM ds_instance_lease WHERE instance_uid = ? FOR UPDATE`,
		target.InstanceUID).Scan(&storedEpoch, &storedDeadline)
	tSelect = time.Since(selStart)
	switch {
	case errors.Is(qerr, sql.ErrNoRows):
		const ins = `INSERT INTO ds_instance_lease (instance_uid, pod_name, instance_epoch, release_track, lease_deadline_ms, updated_at_ms)
VALUES (?, ?, ?, ?, ?, ?)`
		if _, ierr := tx.ExecContext(ctx, ins, target.InstanceUID, target.PodName, target.InstanceEpoch,
			target.ReleaseTrack, newDeadline, now); ierr != nil {
			return 0, errcode.New(errcode.ErrInternal, "insert lease uid=%s: %v", target.InstanceUID, ierr)
		}
	case qerr != nil:
		return 0, errcode.New(errcode.ErrInternal, "lock lease uid=%s: %v", target.InstanceUID, qerr)
	default:
		// 实例纪元守卫:双方都带纪元且不同 → 换代实例不得续旧行(fail-closed)。
		// 请求 0 = 调用方无纪元语义(hub 凭据不携带实例纪元,uid 全局唯一已足);
		// 存量 0 且请求非零 → 首次补齐纪元(battle 侧续租升级旧行)。
		if storedEpoch != 0 && target.InstanceEpoch != 0 && storedEpoch != target.InstanceEpoch {
			// 换代实例试图续旧租约行的 fencing 拒绝(§8):可能是 Pod 复用 uid 或 epoch 回退。
			// 续租链断裂本只能靠玩家掉线反推,这里显式 WARN 留证。
			plog.With(ctx).Warnw("msg", "owner_lease_epoch_regressed",
				"instance_uid", target.InstanceUID, "pod", target.PodName,
				"stored_epoch", storedEpoch, "req_epoch", target.InstanceEpoch)
			return 0, errcode.New(errcode.ErrOwnerLeaseRegressed,
				"instance epoch mismatch uid=%s stored=%d req=%d", target.InstanceUID, storedEpoch, target.InstanceEpoch)
		}
		if storedEpoch == 0 && target.InstanceEpoch != 0 {
			if _, uerr := tx.ExecContext(ctx,
				`UPDATE ds_instance_lease SET instance_epoch = ? WHERE instance_uid = ?`,
				target.InstanceEpoch, target.InstanceUID); uerr != nil {
				return 0, errcode.New(errcode.ErrInternal, "backfill lease epoch uid=%s: %v", target.InstanceUID, uerr)
			}
		}
		if now > storedDeadline && storedDeadline > 0 {
			// 失租:上一次 deadline 已经过去了才来续。这是 owner 侧**唯一**的失租证据 ——
			// §9.22 的安全论证(旧 DS 最晚停止可玩时间 < 新 DS 最早可玩时间)整个建立在
			// lease 时间线上,而续租成功是热路径不能逐条打,不打这一刻就永远回答不了
			// 「某实例的租约什么时候断过、断了多久」。仅在真的断过时打,天然低频。
			plog.With(ctx).Warnw("msg", "owner_lease_lapsed",
				"instance_uid", target.InstanceUID, "pod", target.PodName,
				"instance_epoch", target.InstanceEpoch, "stored_epoch", storedEpoch,
				"prev_deadline_ms", storedDeadline, "lapsed_ms", now-storedDeadline,
				"new_deadline_ms", newDeadline)
		}
		if newDeadline <= storedDeadline {
			// deadline 只前进:乱序/迟到续租幂等返回现值,不回退。
			newDeadline = storedDeadline
		} else {
			const upd = `UPDATE ds_instance_lease SET lease_deadline_ms = ?, updated_at_ms = ? WHERE instance_uid = ?`
			if _, uerr := tx.ExecContext(ctx, upd, newDeadline, now, target.InstanceUID); uerr != nil {
				return 0, errcode.New(errcode.ErrInternal, "renew lease uid=%s: %v", target.InstanceUID, uerr)
			}
		}
	}
	commitStart := time.Now()
	cerr := tx.Commit()
	tCommit = time.Since(commitStart)
	if cerr != nil {
		return 0, errcode.New(errcode.ErrInternal, "commit renew uid=%s: %v", target.InstanceUID, cerr)
	}
	return newDeadline, nil
}

func (r *MySQLOwnerRepo) Release(ctx context.Context, playerID, ownerEpoch uint64, operationID string, skewMargin time.Duration) (OwnerRecord, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "begin release tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	rec, found, serr := scanRecordRow(tx.QueryRowContext(ctx, selectRecordCols+` FOR UPDATE`, playerID), playerID)
	if serr != nil {
		return OwnerRecord{}, serr
	}
	if !found || rec.OwnerEpoch != ownerEpoch || rec.OperationID != operationID || rec.OwnerType == OwnerTypeNone {
		// 迟到 Release(旧 epoch / 旧 operation / 已释放):幂等 no-op,只能"compare-delete 自己"。
		if cerr := tx.Commit(); cerr != nil {
			return OwnerRecord{}, errcode.New(errcode.ErrInternal, "commit noop release player=%d: %v", playerID, cerr)
		}
		// no-op 时 owner 记录仍指向那台已死的 DS,玩家「卡在旧 DS」直到下一次 BeginTransition,
		// 而 RPC 返回 OK + 当前记录、access log 记 rpc_ok(DEBUG)—— 排查时完全看不出
		// 释放请求到过、被以什么理由拒了(login 登出释放与 allocator 回滚/终局释放都走这条)。
		plog.With(ctx).Warnw("msg", "owner_release_noop",
			"player_id", playerID,
			"reason", releaseNoopReason(found, rec, ownerEpoch, operationID),
			"found", found,
			"req_epoch", ownerEpoch, "current_epoch", rec.OwnerEpoch,
			"req_operation_id", operationID, "current_operation_id", rec.OperationID,
			"current_owner_type", rec.OwnerType, "current_phase", rec.Phase,
			"current_pod", rec.Target.PodName, "current_instance_uid", rec.Target.InstanceUID,
			"current_instance_epoch", rec.Target.InstanceEpoch,
			"current_assignment_id", rec.Target.AssignmentOrAllocationID,
			"current_updated_at_ms", rec.UpdatedAtMs)
		return rec, nil
	}
	now := nowUnixMs()
	released := rec.Target // Target 下面会被清空,先留一份给审计流水与日志
	// **再入屏障必须在释放前算好并留存**(INC-20260824-003,2026-08-24)。
	//
	// 本 UPDATE 抹掉的 owner_type / instance_uid 正是 BeginTransition 计算屏障的**唯一
	// 判据**。不留存的后果:任何「先释放、后迁移」的顺序都会让下一次 Begin 走
	// `no_old_battle_owner`、屏障 = now,而那台旧战斗 DS 可能仍活着(玩家 Pawn 仍被模拟、
	// 仍有 journal 迟到写在途)——即 §9.22 的核心时序不等式被反转。
	// 这不是某一条链的疏忽:登出释放对 BATTLE 归属一视同仁,该顺序早已存在。
	//
	// 因此这里按与 BeginTransition **同一个公式**算出屏障并盖进 admit_not_before:
	// 释放只放弃「归属」,不赦免「旧实例仍可能可玩的那段时间」。
	// 只对 BATTLE 归属计算:HUB 归属的屏障按设计恒为 now(协作迁移,双写由 epoch fencing
	// 拦、双可玩由客户端单连接拆链拦),给它盖屏障等于每次进大厅白卡 27 秒。
	retainedBarrier := rec.AdmitNotBeforeMs
	barrierSource := "kept_previous"
	if rec.OwnerType == OwnerTypeBattle && rec.Target.InstanceUID != "" {
		oldDeadline, derr := readLeaseDeadline(ctx, tx, rec.Target.InstanceUID, true)
		if derr != nil {
			return OwnerRecord{}, derr
		}
		base := now
		if oldDeadline > base {
			base = oldDeadline
		}
		// 只前进:同一玩家连续两次释放时不把已建立的屏障往回调。
		if stamped := base + skewMargin.Milliseconds(); stamped > retainedBarrier {
			retainedBarrier = stamped
			barrierSource = "released_battle_lease"
		}
	}
	// ⚠️ 列清单里**刻意没有** hub_source_revision(INC-20260818-003):释放归属不该把
	// 来源版本高水位一起抹掉。抹掉的后果是「打完一局 / 掉一次线」就把该玩家的门重新对
	// legacy(0)敞开,滚动窗口里的旧写者随即又能写进来。以后往这条 UPDATE 加列时,
	// 别顺手把它补上 —— 它不在这里是结论,不是遗漏。
	const upd = `UPDATE owner_record SET owner_type = ?, phase = ?, pod_name = '', instance_uid = '',
 instance_epoch = 0, assignment_or_allocation_id = '', release_track = '', admit_not_before_ms = ?,
 updated_at_ms = ? WHERE player_id = ? AND owner_epoch = ?`
	if _, uerr := tx.ExecContext(ctx, upd, OwnerTypeNone, OwnerPhaseNone,
		retainedBarrier, now, playerID, ownerEpoch); uerr != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "release update player=%d: %v", playerID, uerr)
	}
	if aerr := appendTransitionLog(ctx, tx, playerID, rec.OwnerEpoch, rec.OwnerEpoch,
		transitionOpRelease, operationID, transitionDetail(released, rec.AdmitNotBeforeMs, "")); aerr != nil {
		return OwnerRecord{}, aerr
	}
	releasedType := rec.OwnerType
	rec.OwnerType = OwnerTypeNone
	rec.Phase = OwnerPhaseNone
	rec.Target = OwnerTarget{}
	rec.LeaseDeadlineMs = 0
	rec.AdmitNotBeforeMs = retainedBarrier
	rec.UpdatedAtMs = now
	if cerr := tx.Commit(); cerr != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "commit release player=%d: %v", playerID, cerr)
	}
	// 释放是三个不可逆推进点的最后一个(§11.3 R1):没有它就无法证明玩家是「被正常放开」
	// 还是「记录还挂在旧 DS 上」——两者在 owner_record 上都表现为 owner_type=none/仍有值,
	// 而时间线只在这条日志与审计流水里。
	plog.With(ctx).Infow("msg", "owner_released",
		"player_id", playerID, "owner_epoch", ownerEpoch, "operation_id", operationID,
		// 留存屏障要能对账:「释放后下一次 Begin 为什么还等 / 为什么不等」的唯一现场依据
		// (INC-20260824-003)。缺了它,屏障塌成 0 与屏障正常留存在日志里完全同形。
		"retained_admit_not_before_ms", retainedBarrier,
		"retained_barrier_source", barrierSource,
		"retained_barrier_remaining_ms", retainedBarrier-now,
		"released_owner_type", releasedType,
		"pod", released.PodName, "instance_uid", released.InstanceUID,
		"instance_epoch", released.InstanceEpoch,
		"assignment_or_allocation_id", released.AssignmentOrAllocationID,
		"release_track", released.ReleaseTrack)
	return rec, nil
}

func (r *MySQLOwnerRepo) SweepTransitionLog(ctx context.Context, retention time.Duration, batch int) (int64, error) {
	if batch <= 0 || retention <= 0 {
		return 0, nil
	}
	res, err := r.db.ExecContext(ctx,
		`DELETE FROM owner_transition_log WHERE created_at < (NOW() - INTERVAL ? SECOND) LIMIT ?`,
		int64(retention.Seconds()), batch)
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "sweep transition log: %v", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}
