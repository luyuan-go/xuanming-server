// inventory_retention_mysql_test.go — 保留期清理的真实 MySQL 集成测试(2026-07-21,§9.24)。
//
// 复用 inventory_repo_mysql_test.go 的随机临时库夹具(PANDORA_TEST_MYSQL_DSN 未设置时 Skip)。
// 覆盖:超期删 / 未超期留、单批 limit 有界、escrow 只删 closed(active 超期也不删)、
// 删后迟到 ReleaseEscrow 幂等 no-op。
package data

import (
	"context"
	"database/sql"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"testing"
)

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %s: %v", q, err)
	}
}

func countRows(t *testing.T, db *sql.DB, q string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", q, err)
	}
	return n
}

func TestInventoryRetentionSweep_MySQL(t *testing.T) {
	f := openInventoryMySQLFixture(t)
	repo := NewMySQLInventoryRepo(f.db)
	ctx := context.Background()

	// ReportOnlyDeletesNothing 是本文件最重要的用例:**默认模式下一行都不能少**。
	// 它守住 2026-07-22 用户指令"不能因为数据大了就删我的数据",回归它 = 回归数据安全。
	t.Run("ReportOnlyDeletesNothing", func(t *testing.T) {
		mustExec(t, f.db, `INSERT INTO inventory_ledger(player_id,idempotency_key,op,request_fingerprint,detail,created_at) VALUES
			(9,'ro-1','grant','fp','', DATE_SUB(NOW(), INTERVAL 91 DAY)),
			(9,'ro-2','use','fp','', DATE_SUB(NOW(), INTERVAL 500 DAY))`)
		mustExec(t, f.db, `INSERT INTO auction_escrow(player_id,order_id,kind,item_config_id,frozen_qty,frozen_amount,status,created_at,updated_at) VALUES
			(9,91,1,7001,0,0,2, DATE_SUB(NOW(), INTERVAL 500 DAY), DATE_SUB(NOW(), INTERVAL 400 DAY))`)

		// 极端条件:保留期给 0 天(一切都"超期"),report-only 仍必须一行不删。
		out, err := repo.SweepLedgerBefore(ctx, dbguard.ModeReportOnly, 0, 100)
		if err != nil {
			t.Fatalf("report-only ledger: %v", err)
		}
		if out.Deleted != 0 {
			t.Fatalf("report_only 模式删了 %d 行 —— 违反「只报告不删」", out.Deleted)
		}
		if out.Matched < 2 {
			t.Fatalf("report_only 应报出待清理量 >=2, got %d", out.Matched)
		}
		if got := countRows(t, f.db, `SELECT COUNT(*) FROM inventory_ledger WHERE player_id=9`); got != 2 {
			t.Fatalf("report_only 后流水行=%d,必须仍为 2(一行不少)", got)
		}

		eout, eerr := repo.SweepClosedEscrowBefore(ctx, dbguard.ModeReportOnly, 0, 100)
		if eerr != nil {
			t.Fatalf("report-only escrow: %v", eerr)
		}
		if eout.Deleted != 0 {
			t.Fatalf("report_only 模式删了 %d 行 escrow", eout.Deleted)
		}
		if got := countRows(t, f.db, `SELECT COUNT(*) FROM auction_escrow WHERE player_id=9`); got != 1 {
			t.Fatalf("report_only 后 escrow 行=%d,必须仍为 1", got)
		}

		// 清掉本用例数据,避免影响后续 delete 模式用例的计数。
		mustExec(t, f.db, `DELETE FROM inventory_ledger WHERE player_id=9`)
		mustExec(t, f.db, `DELETE FROM auction_escrow WHERE player_id=9`)
	})

	t.Run("DeleteModeRemovesOnlyExpired", func(t *testing.T) {
		mustExec(t, f.db, `INSERT INTO inventory_ledger(player_id,idempotency_key,op,request_fingerprint,detail,created_at) VALUES
			(1,'old-1','grant','fp','', DATE_SUB(NOW(), INTERVAL 91 DAY)),
			(1,'old-2','use','fp','', DATE_SUB(NOW(), INTERVAL 120 DAY)),
			(1,'fresh','grant','fp','', DATE_SUB(NOW(), INTERVAL 89 DAY))`)

		out, err := repo.SweepLedgerBefore(ctx, dbguard.ModeDelete, 90, 100)
		if err != nil || out.Deleted != 2 {
			t.Fatalf("delete 模式: deleted=%d err=%v, want 2", out.Deleted, err)
		}
		if got := countRows(t, f.db, `SELECT COUNT(*) FROM inventory_ledger WHERE player_id=1`); got != 1 {
			t.Fatalf("剩余流水行=%d want=1(只留未超期)", got)
		}
		var key string
		if err := f.db.QueryRow(`SELECT idempotency_key FROM inventory_ledger WHERE player_id=1`).Scan(&key); err != nil || key != "fresh" {
			t.Fatalf("留存行 key=%q err=%v, want fresh", key, err)
		}
	})

	t.Run("DeleteModeBatchLimitBounded", func(t *testing.T) {
		mustExec(t, f.db, `INSERT INTO inventory_ledger(player_id,idempotency_key,op,request_fingerprint,detail,created_at) VALUES
			(3,'b1','grant','fp','', DATE_SUB(NOW(), INTERVAL 100 DAY)),
			(3,'b2','grant','fp','', DATE_SUB(NOW(), INTERVAL 100 DAY)),
			(3,'b3','grant','fp','', DATE_SUB(NOW(), INTERVAL 100 DAY))`)

		out, err := repo.SweepLedgerBefore(ctx, dbguard.ModeDelete, 90, 2)
		if err != nil || out.Deleted != 2 || !out.Truncated {
			t.Fatalf("第一批: deleted=%d truncated=%v err=%v, want 2/true(limit 有界)", out.Deleted, out.Truncated, err)
		}
		out, err = repo.SweepLedgerBefore(ctx, dbguard.ModeDelete, 90, 2)
		if err != nil || out.Deleted != 1 || out.Truncated {
			t.Fatalf("第二批: deleted=%d truncated=%v err=%v, want 1/false(积压摊平后收敛)", out.Deleted, out.Truncated, err)
		}
		out, err = repo.SweepLedgerBefore(ctx, dbguard.ModeDelete, 90, 2)
		if err != nil || out.Deleted != 0 {
			t.Fatalf("清空后: deleted=%d err=%v, want 0(幂等空批)", out.Deleted, err)
		}
	})

	t.Run("DeleteModeEscrowOnlyClosedExpired", func(t *testing.T) {
		// order 21: closed 超期 → 删;order 22: closed 未超期 → 留;
		// order 23: active 超期 400 天 → 永不删(遗留 OPEN/PARTIAL 订单核对依赖其存在)。
		mustExec(t, f.db, `INSERT INTO auction_escrow(player_id,order_id,kind,item_config_id,frozen_qty,frozen_amount,status,created_at,updated_at) VALUES
			(2,21,1,7001,0,0,2, DATE_SUB(NOW(), INTERVAL 100 DAY), DATE_SUB(NOW(), INTERVAL 91 DAY)),
			(2,22,1,7001,0,0,2, DATE_SUB(NOW(), INTERVAL 100 DAY), DATE_SUB(NOW(), INTERVAL 10 DAY)),
			(2,23,1,7001,5,0,1, DATE_SUB(NOW(), INTERVAL 400 DAY), DATE_SUB(NOW(), INTERVAL 400 DAY))`)

		out, err := repo.SweepClosedEscrowBefore(ctx, dbguard.ModeDelete, 90, 100)
		if err != nil || out.Deleted != 1 {
			t.Fatalf("SweepClosedEscrowBefore(delete): deleted=%d err=%v, want 1", out.Deleted, err)
		}
		if got := countRows(t, f.db, `SELECT COUNT(*) FROM auction_escrow WHERE player_id=2 AND order_id=21`); got != 0 {
			t.Fatal("closed 超期行未被删除")
		}
		if got := countRows(t, f.db, `SELECT COUNT(*) FROM auction_escrow WHERE player_id=2 AND order_id=22`); got != 1 {
			t.Fatal("closed 未超期行不应被删除")
		}
		if got := countRows(t, f.db, `SELECT COUNT(*) FROM auction_escrow WHERE player_id=2 AND order_id=23`); got != 1 {
			t.Fatal("active 行无论多老都不得清理")
		}

		// 删后迟到 ReleaseEscrow:ErrNoRows → already=true no-op,fail-safe 不报错不退资产。
		already, rerr := repo.ReleaseEscrow(ctx, 2, 21)
		if rerr != nil || !already {
			t.Fatalf("迟到 ReleaseEscrow: already=%v err=%v, want already no-op", already, rerr)
		}
	})
}
