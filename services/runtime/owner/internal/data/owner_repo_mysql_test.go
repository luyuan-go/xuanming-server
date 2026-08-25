// owner_repo_mysql_test.go — owner 权威状态机真实 MySQL 集成测试(owner-authority.md §5)。
//
// PANDORA_TEST_MYSQL_DSN 必须是不带 database 的专用测试实例 DSN(同 inventory/bag 模式)。
// 测试只创建/删除名称严格匹配 pandora_owner_it_* 的随机临时库;未设置时明确 Skip。
//
// 覆盖(§16 风险对应验证):
//   - 首迁移无屏障 + Begin/Admit 幂等重放(响应丢失安全);
//   - epoch CAS 冲突(并发双迁移一胜一败);
//   - admit_not_before = CAS 时点旧租约观察值 + 余量;早到 Admit 拒;Begin 后旧实例续租
//     不回写已算屏障;
//   - Admit exact 身份全等(旧 epoch / 换实例 UID 拒);
//   - 同物理实例 assignment 换发也推进 epoch/operation 并回到 PENDING；旧 epoch/assignment
//     不得回滚或准入(2026-08-17 resume↔票据分叉死循环回归);
//   - Release 幂等(迟到旧 operation no-op);
//   - 实例租约只前进 + 实例纪元不符拒。
package data

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/placement"
)

const ownerMySQLTestTimeout = 15 * time.Second

// ownerMySQLSetupTimeout 是 fixture 建库 + 建表(DDL)与清理的预算,独立于断言用的
// ownerMySQLTestTimeout。DDL 比测试体里的小 DML 重一个量级(CREATE DATABASE 要落目录),
// 而测试可能跑在被代理的链路上(kubectl port-forward / 远端联调库),实测单条 CREATE
// DATABASE 可达十余秒——原实现用**同一个 15s ctx** 覆盖 ping + 建库 + 建表三步,任一步
// 稍慢就把后面两步的预算吃光,失败还表现为「schema 初始化超时」这种误导性信息。
// 放宽只影响等待上限,不改变任何断言:逻辑错依旧失败。
const ownerMySQLSetupTimeout = 120 * time.Second

var ownerMySQLTestDBPattern = regexp.MustCompile(`^pandora_owner_it_[0-9]+_[0-9a-f]{12}$`)

type ownerMySQLFixture struct {
	admin   *sql.DB
	db      *sql.DB
	dbName  string
	created bool
}

func openOwnerMySQLFixture(t *testing.T) *ownerMySQLFixture {
	t.Helper()
	serverDSN := strings.TrimSpace(os.Getenv("PANDORA_TEST_MYSQL_DSN"))
	if serverDSN == "" {
		t.Skip("未设置 PANDORA_TEST_MYSQL_DSN,跳过 owner 权威真 MySQL 集成测试")
	}
	cfg, err := mysql.ParseDSN(serverDSN)
	if err != nil {
		t.Fatalf("解析 PANDORA_TEST_MYSQL_DSN: %v", err)
	}
	if cfg.DBName != "" {
		t.Fatalf("PANDORA_TEST_MYSQL_DSN 禁止带 database,got=%q", cfg.DBName)
	}
	cfg.MultiStatements = true
	cfg.ParseTime = true
	cfg.Timeout = 5 * time.Second
	// 驱动级读写上限按 DDL 预算取(建库/建表是本套最重的语句);断言用的小 DML 远低于此,
	// 放宽只影响卡死时的等待上限,不改变任何判定。
	cfg.ReadTimeout = ownerMySQLSetupTimeout
	cfg.WriteTimeout = ownerMySQLSetupTimeout

	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开 MySQL 管理连接: %v", err)
	}
	f := &ownerMySQLFixture{admin: admin}
	t.Cleanup(func() { f.cleanup(t) })
	ctx, cancel := context.WithTimeout(context.Background(), ownerMySQLSetupTimeout)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("已设置测试 DSN 但 MySQL 不可达: %v", err)
	}

	var randBytes [6]byte
	if _, err := rand.Read(randBytes[:]); err != nil {
		t.Fatalf("生成随机库名: %v", err)
	}
	f.dbName = fmt.Sprintf("pandora_owner_it_%d_%s", time.Now().UnixNano(), hex.EncodeToString(randBytes[:]))
	if !ownerMySQLTestDBPattern.MatchString(f.dbName) {
		t.Fatalf("随机测试库名未通过安全校验: %q", f.dbName)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+f.dbName+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		t.Fatalf("创建随机测试库 %s: %v", f.dbName, err)
	}
	f.created = true

	if _, err := admin.ExecContext(ctx, readOwnerMySQLSchema(t, f.dbName)); err != nil {
		t.Fatalf("初始化 owner schema: %v", err)
	}
	testCfg := cfg.Clone()
	testCfg.DBName = f.dbName
	db, err := sql.Open("mysql", testCfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开测试库连接: %v", err)
	}
	f.db = db
	return f
}

func (f *ownerMySQLFixture) cleanup(t *testing.T) {
	t.Helper()
	// DROP DATABASE 与建库同级重,用 setup 预算(原来复用 15s 断言预算,慢链路上必然误报)。
	ctx, cancel := context.WithTimeout(context.Background(), ownerMySQLSetupTimeout)
	defer cancel()
	if f.db != nil {
		_ = f.db.Close()
	}
	if f.created && ownerMySQLTestDBPattern.MatchString(f.dbName) {
		if _, err := f.admin.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+f.dbName+"`"); err != nil {
			t.Errorf("删除随机测试库 %s: %v", f.dbName, err)
		}
	}
	_ = f.admin.Close()
}

func readOwnerMySQLSchema(t *testing.T, dbName string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("定位测试文件路径失败")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(thisFile),
		"..", "..", "..", "..", "..", "deploy", "mysql-init", "15-owner-tables.sql"))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 owner schema %s: %v", path, err)
	}
	schema := string(b)
	const needle = "USE `pandora_owner`;"
	if strings.Count(schema, needle) != 1 {
		t.Fatalf("owner schema USE 锚点数量异常: %d", strings.Count(schema, needle))
	}
	return strings.Replace(schema, needle, "USE `"+dbName+"`;", 1)
}

func testTarget(uid string) OwnerTarget {
	return OwnerTarget{
		PodName: "hub-" + uid, InstanceUID: uid, InstanceEpoch: 1,
		AssignmentOrAllocationID: "assign-" + uid, ReleaseTrack: "stable",
	}
}

const testOpA = "6f9619ff-8b86-4d01-b42d-00cf4fc964ff"
const testOpB = "7f9619ff-8b86-4d01-b42d-00cf4fc964aa"
const testOpC = "8f9619ff-8b86-4d01-b42d-00cf4fc964bb"

// testSkewMargin 与 biz 传给 BeginTransition / Release 的是同一个常量口径
// (placement.DSFenceSkewMarginSeconds)。Release 用它把再入屏障盖进 admit_not_before
// 留存(INC-20260824-003),两处取值必须一致。
const testSkewMargin = time.Duration(placement.DSFenceSkewMarginSeconds) * time.Second

func TestOwnerRepoMySQL(t *testing.T) {
	f := openOwnerMySQLFixture(t)
	repo := NewMySQLOwnerRepo(f.db)
	ctx := context.Background()

	t.Run("FirstTransitionNoBarrierAndIdempotentReplay", func(t *testing.T) {
		const player = 301
		target := testTarget("uid-a")
		rec, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeHub, target, 0, 5*time.Second)
		if err != nil || rec.OwnerEpoch != 1 || rec.Phase != OwnerPhasePending {
			t.Fatalf("首迁移: %+v err=%v", rec, err)
		}
		if rec.AdmitNotBeforeMs > time.Now().UnixMilli() {
			t.Fatalf("首迁移(无旧 owner)不应有屏障: admit=%d", rec.AdmitNotBeforeMs)
		}
		// Begin 幂等重放:同 operation 原样返回,epoch 不再推进。
		replay, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeHub, target, 0, 5*time.Second)
		if err != nil || replay.OwnerEpoch != 1 {
			t.Fatalf("Begin 重放: %+v err=%v", replay, err)
		}
		// Admit 成功 + 幂等重放。
		admitted, retry, err := repo.Admit(ctx, player, 1, testOpA, target)
		if err != nil || retry != 0 || admitted.Phase != OwnerPhaseAdmitted {
			t.Fatalf("Admit: %+v retry=%d err=%v", admitted, retry, err)
		}
		again, _, err := repo.Admit(ctx, player, 1, testOpA, target)
		if err != nil || again.Phase != OwnerPhaseAdmitted {
			t.Fatalf("Admit 重放: %+v err=%v", again, err)
		}
	})

	// 同 exact owner 目标的重复投递必须收敛为 no-op,且 **operation_id 必须保持不变**。
	// 这是 §9.23「一次真实进场用一个稳定 operation_id」的权威侧落点:调用方过去每次
	// 投递现铸 UUID,同一次进场的重连/重复交付/心跳自愈会写出不同 operation,幂等键失效。
	t.Run("SameInstanceRedeliveryKeepsOperationAndEpoch", func(t *testing.T) {
		const player = 310
		target := testTarget("uid-same")
		first, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeHub, target, 0, 5*time.Second)
		if err != nil || first.OwnerEpoch != 1 || first.OperationID != testOpA {
			t.Fatalf("首迁移: %+v err=%v", first, err)
		}

		// ① 换一个 operation 重复投递同一 exact 目标 → 原样返回,epoch 不动,operation 不被覆盖。
		again, err := repo.BeginTransition(ctx, player, 0, testOpB, OwnerTypeHub, target, 0, 5*time.Second)
		if err != nil {
			t.Fatalf("同实例重复投递不应报错: %v", err)
		}
		if again.OwnerEpoch != 1 {
			t.Fatalf("同实例重复投递不得推进 epoch: got=%d want=1", again.OwnerEpoch)
		}
		if again.OperationID != testOpA {
			t.Fatalf("operation_id 必须稳定: got=%q want=%q(调用方传的 %q 不得覆盖)",
				again.OperationID, testOpA, testOpB)
		}

		// ② 过期 expectEpoch + 同一 exact 目标 → 仍是 no-op,不得报 EPOCH_CONFLICT。
		//    「目标已经是它」本就该幂等,不该先冲突再让调用方重查。
		stale, err := repo.BeginTransition(ctx, player, 0, testOpC, OwnerTypeHub, target, 0, 5*time.Second)
		if err != nil || stale.OwnerEpoch != 1 || stale.OperationID != testOpA {
			t.Fatalf("过期 expect + 同实例应幂等 no-op: %+v err=%v", stale, err)
		}

		// ③ 物理实例相同但 assignment 换发仍是 exact owner 身份变化：必须推进 epoch、
		//    回到 PENDING 并使用本次 operation。否则旧票与新票共享 fencing 版本，且迟到旧
		//    Begin 可把新值回滚。
		refreshed := target
		refreshed.AssignmentOrAllocationID = "assign-uid-same-v2"
		seat, err := repo.BeginTransition(ctx, player, 1, testOpB, OwnerTypeHub, refreshed, 0, 5*time.Second)
		if err != nil || seat.OwnerEpoch != 2 || seat.OperationID != testOpB || seat.Phase != OwnerPhasePending {
			t.Fatalf("assignment 换发必须是真实迁移: %+v err=%v", seat, err)
		}
		if seat.Target.AssignmentOrAllocationID != refreshed.AssignmentOrAllocationID {
			t.Fatalf("assignment 换发必须跟进最新签发值: got=%q want=%q",
				seat.Target.AssignmentOrAllocationID, refreshed.AssignmentOrAllocationID)
		}

		// ④ 新 owner Admit 后的 exact 重投递不得再推进 epoch/回退 phase。
		if _, _, aerr := repo.Admit(ctx, player, 2, testOpB, refreshed); aerr != nil {
			t.Fatalf("Admit(换发后目标): %v", aerr)
		}
		afterAdmit, err := repo.BeginTransition(ctx, player, 1, testOpC, OwnerTypeHub, refreshed, 0, 5*time.Second)
		if err != nil || afterAdmit.Phase != OwnerPhaseAdmitted || afterAdmit.OwnerEpoch != 2 ||
			afterAdmit.OperationID != testOpB {
			t.Fatalf("ADMITTED 后重复投递不得回退 phase: %+v err=%v", afterAdmit, err)
		}

		// ⑤ 换物理实例继续推进 epoch。
		moved, err := repo.BeginTransition(ctx, player, 2, testOpC, OwnerTypeHub, testTarget("uid-moved"), 0, 0)
		if err != nil || moved.OwnerEpoch != 3 || moved.OperationID != testOpC {
			t.Fatalf("换实例应真实迁移: %+v err=%v", moved, err)
		}
	})

	// 2026-08-17 现网死循环回归(INC 事故形状):allocator 在同一实例上换发 assignment
	// (旧归属记录蒸发 / Release 后原地新铸 uuid)后,owner 记录是 GetResumeContext 的唯一
	// 数据源——若不跟进,resume 恒下发旧 id、票据/DS ACK 恒携带新 id,客户端把每个 ACK 判成
	// owner_target_mismatch 丢弃,每 45s 重取票再撞,永不收敛。本用例钉住服务端一半的收敛
	// 判据:换发后的 Begin 必须以新 epoch/operation 让记录与 Query 视图收敛到新值，且
	// 新旧 assignment 的 Admit 一放一拒。
	t.Run("AssignmentRefreshConvergesResumeView", func(t *testing.T) {
		const player = 313
		target := testTarget("uid-refresh")
		if _, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeHub, target, 0, 0); err != nil {
			t.Fatalf("首迁移: %v", err)
		}
		if _, _, err := repo.Admit(ctx, player, 1, testOpA, target); err != nil {
			t.Fatalf("admit: %v", err)
		}
		// 同一实例上换发 assignment,签票路径携新值重复 Begin。
		reissued := target
		reissued.AssignmentOrAllocationID = "assign-uid-refresh-v2"
		rec, err := repo.BeginTransition(ctx, player, 1, testOpB, OwnerTypeHub, reissued, 0, 0)
		if err != nil {
			t.Fatalf("换发 Begin: %v", err)
		}
		if rec.OwnerEpoch != 2 || rec.Phase != OwnerPhasePending || rec.OperationID != testOpB {
			t.Fatalf("换发必须推进 epoch/operation 并回到 PENDING: %+v", rec)
		}
		if rec.Target.AssignmentOrAllocationID != reissued.AssignmentOrAllocationID {
			t.Fatalf("记录未跟进新 assignment: %+v", rec.Target)
		}
		// Query(resume 数据源)必须已看到新值——客户端收敛判据的服务端一半。
		view, err := repo.Query(ctx, player)
		if err != nil || view.Target.AssignmentOrAllocationID != reissued.AssignmentOrAllocationID {
			t.Fatalf("Query 应看到换发后的 assignment: %+v err=%v", view.Target, err)
		}
		// DS ACK 携新 epoch/assignment 的 Admit 通过;携旧 epoch/assignment 的被拒
		// (§9.3 换发即旧票作废,正是期望的 fencing)。
		if _, _, err := repo.Admit(ctx, player, 2, testOpB, reissued); err != nil {
			t.Fatalf("换发后 Admit(新 assignment)应通过: %v", err)
		}
		if _, _, err := repo.Admit(ctx, player, 1, testOpA, target); errcode.As(err) != errcode.ErrOwnerIdentityMismatch {
			t.Fatalf("换发后 Admit(旧 assignment)应拒: %v", err)
		}
	})

	// assignment 是 Hub 票据的归属版本。即使物理 DS 实例未变，旧 owner epoch 的迟到
	// Begin 也不能把已经换发的新 assignment 写回旧值；否则旧票会被重新写回 owner
	// 权威，ResumeContext 与当前 allocator 记录再次分叉。
	t.Run("AssignmentRefreshRejectsStaleEpochRollback", func(t *testing.T) {
		const player = 314
		original := testTarget("uid-refresh-stale")
		if _, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeHub, original, 0, 0); err != nil {
			t.Fatalf("首迁移: %v", err)
		}

		reissued := original
		reissued.AssignmentOrAllocationID = "assign-uid-refresh-stale-v2"
		if _, err := repo.BeginTransition(ctx, player, 1, testOpB, OwnerTypeHub, reissued, 0, 0); err != nil {
			t.Fatalf("换发 Begin: %v", err)
		}

		// 模拟首迁移时代的请求在换发后迟到。它携带的 expect_epoch=0 已过期，必须被
		// CAS 拒绝，不能仅因 pod/uid/instance_epoch 相同就改写 assignment。
		if _, err := repo.BeginTransition(ctx, player, 0, testOpC, OwnerTypeHub, original, 0, 0); errcode.As(err) != errcode.ErrOwnerEpochConflict {
			t.Fatalf("迟到旧 epoch Begin 必须拒绝，不能回滚 assignment: %v", err)
		}
		view, err := repo.Query(ctx, player)
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		if view.Target.AssignmentOrAllocationID != reissued.AssignmentOrAllocationID {
			t.Fatalf("迟到请求回滚了 assignment: got=%q want=%q",
				view.Target.AssignmentOrAllocationID, reissued.AssignmentOrAllocationID)
		}
	})

	// exact target 规则不只服务 Hub：Battle allocation 与 release track 同样是票据/写入
	// fencing 身份。物理实例相同也不得继承旧 epoch/ADMITTED phase。
	t.Run("BattleAllocationAndTrackChangesAdvanceEpoch", func(t *testing.T) {
		const player = 315
		target := testTarget("uid-battle-exact")
		target.AssignmentOrAllocationID = "allocation-v1"
		first, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeBattle, target, 0, 0)
		if err != nil || first.OwnerEpoch != 1 {
			t.Fatalf("首 Battle owner: %+v err=%v", first, err)
		}
		if _, _, err := repo.Admit(ctx, player, 1, testOpA, target); err != nil {
			t.Fatalf("Admit v1: %v", err)
		}

		allocationV2 := target
		allocationV2.AssignmentOrAllocationID = "allocation-v2"
		second, err := repo.BeginTransition(ctx, player, 1, testOpB, OwnerTypeBattle, allocationV2, 0, 0)
		if err != nil || second.OwnerEpoch != 2 || second.Phase != OwnerPhasePending || second.OperationID != testOpB {
			t.Fatalf("allocation 变化必须推进 epoch: %+v err=%v", second, err)
		}

		canary := allocationV2
		canary.ReleaseTrack = "canary"
		third, err := repo.BeginTransition(ctx, player, 2, testOpC, OwnerTypeBattle, canary, 0, 0)
		if err != nil || third.OwnerEpoch != 3 || third.Phase != OwnerPhasePending || third.OperationID != testOpC {
			t.Fatalf("release track 变化必须推进 epoch: %+v err=%v", third, err)
		}
	})

	// §9.22:「Pod UID / instance epoch 变化或灾备接管都必须递增 owner_epoch」。
	// 同 pod+uid 但 instance_epoch 已推进(实例代次翻转 / 灾备接管)**不是**同一实例,
	// 必须走真实迁移。这条判据原先在 allocator 的 decideOwnerBegin(复审 P1-3 专门加的),
	// 随判定下沉到权威一并搬来,覆盖不能丢。
	t.Run("InstanceEpochBumpIsRealMigration", func(t *testing.T) {
		const player = 312
		target := testTarget("uid-epoch")
		first, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeHub, target, 0, 0)
		if err != nil || first.OwnerEpoch != 1 {
			t.Fatalf("首迁移: %+v err=%v", first, err)
		}
		// 同 pod + 同 uid,仅 instance_epoch 前进 → 必须推进 owner_epoch 并换 operation。
		bumped := target
		bumped.InstanceEpoch = target.InstanceEpoch + 1
		moved, err := repo.BeginTransition(ctx, player, 1, testOpB, OwnerTypeHub, bumped, 0, 0)
		if err != nil {
			t.Fatalf("instance epoch 前进应发起真实迁移: %v", err)
		}
		if moved.OwnerEpoch != 2 || moved.OperationID != testOpB {
			t.Fatalf("instance epoch 变化必须递增 owner_epoch(§9.22): %+v", moved)
		}
	})

	// 真实迁移路径不接受空 operation_id(绕过 biz 的内部调用兜底;空 operation 会让
	// Admit exact 校验、客户端续用与审计流水同时失去锚点)。
	t.Run("RealTransitionRejectsEmptyOperation", func(t *testing.T) {
		const player = 311
		_, err := repo.BeginTransition(ctx, player, 0, "", OwnerTypeHub, testTarget("uid-empty"), 0, 0)
		if errcode.As(err) != errcode.ErrOwnerInvalidOperation {
			t.Fatalf("空 operation 的真实迁移必须拒绝,得到 err=%v", err)
		}
	})

	t.Run("EpochConflictAndConcurrentBegin", func(t *testing.T) {
		const player = 302
		target := testTarget("uid-b")
		if _, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeHub, target, 0, 0); err != nil {
			t.Fatalf("首迁移: %v", err)
		}
		// 过期期望 → 冲突并附当前记录。
		cur, err := repo.BeginTransition(ctx, player, 0, testOpB, OwnerTypeHub, testTarget("uid-b2"), 0, 0)
		if errcode.As(err) != errcode.ErrOwnerEpochConflict || cur.OwnerEpoch != 1 {
			t.Fatalf("epoch 冲突: %+v err=%v", cur, err)
		}
		// 并发双迁移(同 expect):恰好一胜一冲突。
		var wg sync.WaitGroup
		errs := make([]error, 2)
		ops := []string{testOpB, testOpC}
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				_, errs[idx] = repo.BeginTransition(ctx, player, 1, ops[idx], OwnerTypeBattle,
					testTarget(fmt.Sprintf("uid-b-race-%d", idx)), 0, 0)
			}(i)
		}
		wg.Wait()
		wins, conflicts := 0, 0
		for _, e := range errs {
			switch {
			case e == nil:
				wins++
			case errcode.As(e) == errcode.ErrOwnerEpochConflict:
				conflicts++
			default:
				t.Fatalf("并发 Begin 意外错误: %v", e)
			}
		}
		if wins != 1 || conflicts != 1 {
			t.Fatalf("并发双迁移应恰好一胜一冲突: wins=%d conflicts=%d", wins, conflicts)
		}
	})

	t.Run("BarrierFromOldLeaseObservedAtCAS", func(t *testing.T) {
		// 保守屏障只对旧 owner=BATTLE 生效(2026-08-03 分流:失联对局 DS 的双可玩/迟到写
		// 风险场景,如战斗实例替换/重连改派)。旧 owner=HUB 的路径见下一个用例。
		const player = 303
		oldTarget := testTarget("uid-c-old")
		newTarget := testTarget("uid-c-new")
		if _, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeBattle, oldTarget, 0, 0); err != nil {
			t.Fatalf("旧 owner 建立: %v", err)
		}
		if _, _, err := repo.Admit(ctx, player, 1, testOpA, oldTarget); err != nil {
			t.Fatalf("旧 owner admit: %v", err)
		}
		// 旧实例续租 10s → 屏障应 ≥ 旧租约截止 + 余量。
		oldDeadline, err := repo.RenewInstanceLease(ctx, oldTarget, 10*time.Second)
		if err != nil {
			t.Fatalf("旧实例续租: %v", err)
		}
		const margin = 5 * time.Second
		rec, err := repo.BeginTransition(ctx, player, 1, testOpB, OwnerTypeBattle, newTarget, 0, margin)
		if err != nil {
			t.Fatalf("迁移到新 owner: %v", err)
		}
		wantMin := oldDeadline + margin.Milliseconds()
		if rec.AdmitNotBeforeMs < wantMin {
			t.Fatalf("屏障应 ≥ 旧租约+余量: admit=%d want>=%d", rec.AdmitNotBeforeMs, wantMin)
		}
		// 早到 Admit 拒(带 retry_after)。
		_, retry, err := repo.Admit(ctx, player, 2, testOpB, newTarget)
		if errcode.As(err) != errcode.ErrOwnerBarrierNotOpen || retry <= 0 {
			t.Fatalf("早到 Admit 应拒: retry=%d err=%v", retry, err)
		}
		// Begin 后旧实例继续续租,不回写已算屏障(取 CAS 时点观察值)。
		if _, err := repo.RenewInstanceLease(ctx, oldTarget, 20*time.Second); err != nil {
			t.Fatalf("Begin 后旧实例续租: %v", err)
		}
		after, err := repo.Query(ctx, player)
		if err != nil || after.AdmitNotBeforeMs != rec.AdmitNotBeforeMs {
			t.Fatalf("屏障被后续续租改写: before=%d after=%d err=%v", rec.AdmitNotBeforeMs, after.AdmitNotBeforeMs, err)
		}
		// 身份不匹配拒:旧 epoch / 换实例 UID。
		if _, _, err := repo.Admit(ctx, player, 1, testOpA, oldTarget); errcode.As(err) != errcode.ErrOwnerIdentityMismatch {
			t.Fatalf("旧 epoch Admit 应拒: %v", err)
		}
		forged := newTarget
		forged.InstanceUID = "uid-c-forged"
		if _, _, err := repo.Admit(ctx, player, 2, testOpB, forged); errcode.As(err) != errcode.ErrOwnerIdentityMismatch {
			t.Fatalf("换实例 Admit 应拒: %v", err)
		}
	})

	t.Run("HubOriginMigrationHasNoBarrier", func(t *testing.T) {
		// 2026-08-03 屏障分流回归:旧 owner=HUB(大厅)迁出是玩家在线主动触发的协作迁移,
		// 双写由 owner_epoch fencing 拦、双可玩由客户端单连接拆链拦,屏障必须为 now——
		// 修复前这里会取 max(now, hub 实例租约截止)+margin,而 hub 实例租约被 allocator
		// 整机级持续代续(永不过期),导致每次"大厅→战斗"都干等 ~27s,客户端 30s 权威
		// 等待窗口随之耗尽,玩家现象是"匹配没反应"(实测 2026-08-03 02:02 事故)。
		const player = 306
		hubTarget := testTarget("uid-h-hub")
		battleTarget := testTarget("uid-h-battle")
		if _, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeHub, hubTarget, 0, 0); err != nil {
			t.Fatalf("hub owner 建立: %v", err)
		}
		if _, _, err := repo.Admit(ctx, player, 1, testOpA, hubTarget); err != nil {
			t.Fatalf("hub admit: %v", err)
		}
		// hub 实例租约续到远未来(模拟 allocator 整机级持续代续):它不得进入屏障计算。
		if _, err := repo.RenewInstanceLease(ctx, hubTarget, 60*time.Second); err != nil {
			t.Fatalf("hub 实例续租: %v", err)
		}
		const margin = 5 * time.Second
		beforeMs := time.Now().UnixMilli()
		rec, err := repo.BeginTransition(ctx, player, 1, testOpB, OwnerTypeBattle, battleTarget, 0, margin)
		if err != nil {
			t.Fatalf("hub→battle 迁移: %v", err)
		}
		// 屏障 = CAS 时点 now:不含 hub 租约(60s 后),也不叠 margin(见实现处举证)。
		// 上界取"调用前后本地时钟窗口 + 1s 容差",防跨进程时钟抖动导致偶发红。
		afterMs := time.Now().UnixMilli()
		if rec.AdmitNotBeforeMs < beforeMs || rec.AdmitNotBeforeMs > afterMs+1000 {
			t.Fatalf("HUB 旧 owner 的屏障应为 now(不等租约不叠余量): admit=%d 窗口=[%d,%d]",
				rec.AdmitNotBeforeMs, beforeMs, afterMs+1000)
		}
		// 立即 Admit 必须成功——这就是"点匹配立刻能进"的服务端语义。
		if _, retry, err := repo.Admit(ctx, player, 2, testOpB, battleTarget); err != nil {
			t.Fatalf("hub→battle 应立即可 Admit(无屏障): retry=%d err=%v", retry, err)
		}
	})

	t.Run("ReleaseIdempotentLateNoop", func(t *testing.T) {
		const player = 304
		target := testTarget("uid-d")
		if _, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeHub, target, 0, 0); err != nil {
			t.Fatalf("建立: %v", err)
		}
		if _, _, err := repo.Admit(ctx, player, 1, testOpA, target); err != nil {
			t.Fatalf("admit: %v", err)
		}
		rec, err := repo.Release(ctx, player, 1, testOpA, testSkewMargin)
		if err != nil || rec.OwnerType != OwnerTypeNone || rec.OwnerEpoch != 1 {
			t.Fatalf("释放: %+v err=%v", rec, err)
		}
		// 迟到 Release(旧 operation)幂等 no-op。
		late, err := repo.Release(ctx, player, 1, testOpB, testSkewMargin)
		if err != nil || late.OwnerType != OwnerTypeNone {
			t.Fatalf("迟到释放应 no-op: %+v err=%v", late, err)
		}
	})

	t.Run("ReleasedBattleRetainsBarrier", func(t *testing.T) {
		// INC-20260824-003 回归:**释放 BATTLE 归属不得把再入屏障一起抹掉**。
		//
		// 屏障的唯一判据是 owner_type=BATTLE + instance_uid,而 Release 的 UPDATE 正要清空
		// 这两列。修复前:释放之后的下一次 Begin 落到 `no_old_battle_owner`、屏障 = now,
		// 而那台旧战斗 DS 可能仍活着(Pawn 仍被模拟、journal 迟到写在途)——§9.22 的核心
		// 时序不等式「旧 DS 最晚停止可玩 < 新 DS 最早开始可玩」被反转。
		// 这条路径不是边角:login 登出释放对 BATTLE 归属一视同仁(判据只有 owner_type != 0),
		// 「对局中登出 → 立刻重登」早就走在上面。
		//
		// ★ 变异(两处任一,都会让本用例红):
		//   ① 删掉 Release 里盖 admit_not_before 的那段(退回原 UPDATE 列清单);
		//   ② 删掉 BeginTransition 里 `rec.AdmitNotBeforeMs > admitNotBefore` 的取 max。
		const player = 307
		battleTarget := testTarget("uid-r-battle")
		hubTarget := testTarget("uid-r-hub")
		if _, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeBattle, battleTarget, 0, 0); err != nil {
			t.Fatalf("battle owner 建立: %v", err)
		}
		if _, _, err := repo.Admit(ctx, player, 1, testOpA, battleTarget); err != nil {
			t.Fatalf("battle admit: %v", err)
		}
		leaseDeadline, err := repo.RenewInstanceLease(ctx, battleTarget, 20*time.Second)
		if err != nil {
			t.Fatalf("战斗实例续租: %v", err)
		}
		const margin = 5 * time.Second
		wantMin := leaseDeadline + margin.Milliseconds()

		released, err := repo.Release(ctx, player, 1, testOpA, margin)
		if err != nil {
			t.Fatalf("释放: %v", err)
		}
		// ① 释放照常放弃归属……
		if released.OwnerType != OwnerTypeNone || released.Target.InstanceUID != "" {
			t.Fatalf("释放后应无归属: %+v", released)
		}
		// ……但必须把屏障算好留下,而不是连同判据一起清零。
		if released.AdmitNotBeforeMs < wantMin {
			t.Fatalf("释放必须留存屏障: admit=%d want>=%d(旧租约=%d 余量=%dms)",
				released.AdmitNotBeforeMs, wantMin, leaseDeadline, margin.Milliseconds())
		}
		// ② 留存值必须真的落库,不能只活在返回值里。
		persisted, err := repo.Query(ctx, player)
		if err != nil || persisted.AdmitNotBeforeMs != released.AdmitNotBeforeMs {
			t.Fatalf("留存屏障未落库: persisted=%d released=%d err=%v",
				persisted.AdmitNotBeforeMs, released.AdmitNotBeforeMs, err)
		}
		// ③ 释放后的下一次迁移此时**已无旧 owner**,正是修复前塌成 now 的那一步。
		//    这里刻意传 margin=0:屏障必须完全由留存值顶住,不靠本次余量。
		next, err := repo.BeginTransition(ctx, player, 1, testOpB, OwnerTypeHub, hubTarget, 0, 0)
		if err != nil {
			t.Fatalf("释放后迁移: %v", err)
		}
		if next.AdmitNotBeforeMs < wantMin {
			t.Fatalf("释放后 Begin 屏障塌陷: admit=%d want>=%d", next.AdmitNotBeforeMs, wantMin)
		}
		// ④ 屏障必须**真生效**,不只是存下来的一个数:未开时 Admit 须拒并带 retry_after。
		if _, retry, aerr := repo.Admit(ctx, player, 2, testOpB, hubTarget); errcode.As(aerr) != errcode.ErrOwnerBarrierNotOpen || retry <= 0 {
			t.Fatalf("留存屏障未开时 Admit 应拒: retry=%d err=%v", retry, aerr)
		}
	})

	t.Run("LeaseMonotonicAndEpochGuard", func(t *testing.T) {
		target := testTarget("uid-e")
		first, err := repo.RenewInstanceLease(ctx, target, 10*time.Second)
		if err != nil {
			t.Fatalf("首续: %v", err)
		}
		shorter, err := repo.RenewInstanceLease(ctx, target, 1*time.Second)
		if err != nil || shorter < first {
			t.Fatalf("deadline 不得回退: first=%d shorter=%d err=%v", first, shorter, err)
		}
		replaced := target
		replaced.InstanceEpoch = 2
		if _, err := repo.RenewInstanceLease(ctx, replaced, 5*time.Second); errcode.As(err) != errcode.ErrOwnerLeaseRegressed {
			t.Fatalf("实例纪元不符续租应拒: %v", err)
		}
	})
}
