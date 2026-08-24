// inventory_repo_mysql_test.go — EnsureAuctionEscrow 的真实 MySQL 8.4 集成测试。
//
// PANDORA_TEST_MYSQL_DSN 必须是不带 database 的专用测试实例 DSN。测试只会创建并删除名称严格匹配
// pandora_inventory_it_* 的随机临时库；未设置时明确 Skip，设置后连接/DDL/事务失败一律硬失败。
package data

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
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
)

const inventoryMySQLTestTimeout = 15 * time.Second

var inventoryMySQLTestDBPattern = regexp.MustCompile(`^pandora_inventory_it_[0-9]+_[0-9a-f]{12}$`)

type inventoryMySQLFixture struct {
	admin   *sql.DB
	db      *sql.DB
	dbName  string
	created bool
}

func TestInventoryEnsureAuctionEscrow_MySQL(t *testing.T) {
	f := openInventoryMySQLFixture(t)
	repo := NewMySQLInventoryRepo(f.db)

	t.Run("ExistingActiveValidatedWithoutRefreeze", func(t *testing.T) {
		seedItem(t, f.db, 101, 7001, 10)
		if _, err := repo.FreezeForOrder(context.Background(), 101, 1001,
			EscrowKindItem, 7001, 5, CurrencyGold, 0); err != nil {
			t.Fatalf("预冻 existing escrow: %v", err)
		}
		already, err := repo.EnsureAuctionEscrow(context.Background(), 101, 1001,
			EscrowKindItem, 7001, 3, CurrencyGold, 100)
		if err != nil || !already {
			t.Fatalf("校验 existing escrow: already=%v err=%v", already, err)
		}
		if got := queryItemCount(t, f.db, 101, 7001); got != 5 {
			t.Fatalf("existing ensure 不得二次扣道具: got=%d want=5", got)
		}
		e := queryEscrow(t, f.db, 101, 1001)
		if e.kind != int8(EscrowKindItem) || e.itemConfigID != 7001 || e.frozenQty != 5 || e.status != escrowStatusActive {
			t.Fatalf("existing escrow 被意外改变: %+v", e)
		}
	})

	t.Run("MissingEscrowAtomicallyFreezes", func(t *testing.T) {
		seedItem(t, f.db, 102, 7002, 5)
		already, err := repo.EnsureAuctionEscrow(context.Background(), 102, 1002,
			EscrowKindItem, 7002, 3, CurrencyGold, 80)
		if err != nil || already {
			t.Fatalf("补建 sell escrow: already=%v err=%v", already, err)
		}
		if got := queryItemCount(t, f.db, 102, 7002); got != 2 {
			t.Fatalf("补冻后 active item=%d want=2", got)
		}
		if got := queryEscrow(t, f.db, 102, 1002).frozenQty; got != 3 {
			t.Fatalf("补冻 item escrow=%d want=3", got)
		}

		seedGold(t, f.db, 103, 1000)
		already, err = repo.EnsureAuctionEscrow(context.Background(), 103, 1003,
			EscrowKindCurrency, 7003, 4, CurrencyGold, 125)
		if err != nil || already {
			t.Fatalf("补建 buy escrow: already=%v err=%v", already, err)
		}
		if got := queryGold(t, f.db, 103); got != 500 {
			t.Fatalf("补冻后 active gold=%d want=500", got)
		}
		e := queryEscrow(t, f.db, 103, 1003)
		if e.frozenAmount != 500 {
			t.Fatalf("补冻 currency escrow=%d want=500", e.frozenAmount)
		}
		// 币种必须落库:漏写会让退还 / 成交按默认币种走,静默串币。
		if e.currencyKind != int32(CurrencyGold) {
			t.Fatalf("escrow currency_kind=%d want=%d", e.currencyKind, int32(CurrencyGold))
		}

		// 非金币托管:整条链(扣、落库、退还)都必须按传入币种走,不得回退成金币。
		seedCurrency(t, f.db, 113, CurrencyDiamond, 900)
		already, err = repo.EnsureAuctionEscrow(context.Background(), 113, 1013,
			EscrowKindCurrency, 7013, 3, CurrencyDiamond, 200)
		if err != nil || already {
			t.Fatalf("补建钻石 buy escrow: already=%v err=%v", already, err)
		}
		if got := queryCurrency(t, f.db, 113, CurrencyDiamond); got != 300 {
			t.Fatalf("补冻后钻石余额=%d want=300", got)
		}
		if got := queryGold(t, f.db, 113); got != 0 {
			t.Fatalf("钻石托管不得动金币: gold=%d want=0", got)
		}
		e = queryEscrow(t, f.db, 113, 1013)
		if e.frozenAmount != 600 || e.currencyKind != int32(CurrencyDiamond) {
			t.Fatalf("钻石 escrow=%+v want amount=600 kind=%d", e, int32(CurrencyDiamond))
		}
		if _, rerr := repo.ReleaseEscrow(context.Background(), 113, 1013); rerr != nil {
			t.Fatalf("退还钻石托管: %v", rerr)
		}
		if got := queryCurrency(t, f.db, 113, CurrencyDiamond); got != 900 {
			t.Fatalf("退还必须回到钻石余额: got=%d want=900", got)
		}
		if got := queryGold(t, f.db, 113); got != 0 {
			t.Fatalf("退还不得凭空加金币: gold=%d want=0", got)
		}
	})

	t.Run("InsufficientRollsBackEscrowAndAssets", func(t *testing.T) {
		seedItem(t, f.db, 104, 7004, 1)
		already, err := repo.EnsureAuctionEscrow(context.Background(), 104, 1004,
			EscrowKindItem, 7004, 2, CurrencyGold, 100)
		if already || errcode.As(err) != errcode.ErrInventoryInsufficient {
			t.Fatalf("不足应明确失败: already=%v err=%v", already, err)
		}
		if got := queryItemCount(t, f.db, 104, 7004); got != 1 {
			t.Fatalf("失败后 active item=%d want=1", got)
		}
		var rows int
		if err := f.db.QueryRow(`SELECT COUNT(*) FROM auction_escrow WHERE player_id=104 AND order_id=1004`).Scan(&rows); err != nil {
			t.Fatalf("查询失败后的 escrow: %v", err)
		}
		if rows != 0 {
			t.Fatalf("失败后不得留下 escrow 行: rows=%d", rows)
		}
	})

	t.Run("MismatchClosedAndShortReturnDeterministicErrors", func(t *testing.T) {
		seedItem(t, f.db, 105, 7005, 5)
		if _, err := repo.FreezeForOrder(context.Background(), 105, 1005,
			EscrowKindItem, 7005, 3, CurrencyGold, 0); err != nil {
			t.Fatalf("预冻 mismatch escrow: %v", err)
		}
		if _, err := repo.EnsureAuctionEscrow(context.Background(), 105, 1005,
			EscrowKindItem, 7999, 2, CurrencyGold, 100); errcode.As(err) != errcode.ErrInventoryIdempotencyConflict {
			t.Fatalf("item mismatch want conflict, got %v", err)
		}
		if _, err := repo.EnsureAuctionEscrow(context.Background(), 105, 1005,
			EscrowKindCurrency, 7005, 2, CurrencyGold, 100); errcode.As(err) != errcode.ErrInventoryIdempotencyConflict {
			t.Fatalf("kind mismatch want conflict, got %v", err)
		}
		if _, err := f.db.Exec(`UPDATE auction_escrow SET frozen_qty=1 WHERE player_id=105 AND order_id=1005`); err != nil {
			t.Fatalf("制造 short escrow: %v", err)
		}
		if _, err := repo.EnsureAuctionEscrow(context.Background(), 105, 1005,
			EscrowKindItem, 7005, 2, CurrencyGold, 100); errcode.As(err) != errcode.ErrInventoryInsufficient {
			t.Fatalf("short escrow want insufficient, got %v", err)
		}
		if _, err := repo.ReleaseEscrow(context.Background(), 105, 1005); err != nil {
			t.Fatalf("关闭 escrow: %v", err)
		}
		if _, err := repo.EnsureAuctionEscrow(context.Background(), 105, 1005,
			EscrowKindItem, 7005, 1, CurrencyGold, 100); errcode.As(err) != errcode.ErrInventoryIdempotencyConflict {
			t.Fatalf("closed escrow want conflict, got %v", err)
		}
	})

	t.Run("ConcurrentSameRequestFreezesExactlyOnce", func(t *testing.T) {
		seedItem(t, f.db, 106, 7006, 5)
		const workers = 16
		start := make(chan struct{})
		errs := make(chan error, workers)
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ctx, cancel := context.WithTimeout(context.Background(), inventoryMySQLTestTimeout)
				defer cancel()
				_, err := repo.EnsureAuctionEscrow(ctx, 106, 1006,
					EscrowKindItem, 7006, 5, CurrencyGold, 100)
				errs <- err
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Errorf("并发同请求 ensure: %v", err)
			}
		}
		if got := queryItemCount(t, f.db, 106, 7006); got != 0 {
			t.Fatalf("并发同请求只能扣一次: active=%d want=0", got)
		}
		e := queryEscrow(t, f.db, 106, 1006)
		if e.frozenQty != 5 || e.status != escrowStatusActive {
			t.Fatalf("并发同请求 escrow=%+v want qty=5 active", e)
		}
	})

	t.Run("ConcurrentConflictingRequestNeverTreats1062AsSuccess", func(t *testing.T) {
		seedItem(t, f.db, 107, 7101, 5)
		seedItem(t, f.db, 107, 7102, 5)
		start := make(chan struct{})
		type result struct {
			item uint32
			err  error
		}
		results := make(chan result, 2)
		var wg sync.WaitGroup
		for _, itemID := range []uint32{7101, 7102} {
			itemID := itemID
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ctx, cancel := context.WithTimeout(context.Background(), inventoryMySQLTestTimeout)
				defer cancel()
				_, err := repo.EnsureAuctionEscrow(ctx, 107, 1007,
					EscrowKindItem, itemID, 5, CurrencyGold, 100)
				results <- result{item: itemID, err: err}
			}()
		}
		close(start)
		wg.Wait()
		close(results)

		var succeeded uint32
		for got := range results {
			if got.err == nil {
				if succeeded != 0 {
					t.Fatalf("冲突请求不应都成功: first=%d second=%d", succeeded, got.item)
				}
				succeeded = got.item
				continue
			}
			if errcode.As(got.err) != errcode.ErrInventoryIdempotencyConflict {
				t.Fatalf("冲突 loser item=%d 应返回 conflict, got %v", got.item, got.err)
			}
		}
		if succeeded == 0 {
			t.Fatal("冲突请求应恰有一个创建成功")
		}
		e := queryEscrow(t, f.db, 107, 1007)
		if e.itemConfigID != succeeded || e.frozenQty != 5 {
			t.Fatalf("escrow=%+v, want winner item=%d qty=5", e, succeeded)
		}
		for _, itemID := range []uint32{7101, 7102} {
			want := int64(5)
			if itemID == succeeded {
				want = 0
			}
			if got := queryItemCount(t, f.db, 107, itemID); got != want {
				t.Fatalf("item=%d active=%d want=%d", itemID, got, want)
			}
		}
	})
}

func openInventoryMySQLFixture(t *testing.T) *inventoryMySQLFixture {
	t.Helper()
	serverDSN := strings.TrimSpace(os.Getenv("PANDORA_TEST_MYSQL_DSN"))
	if serverDSN == "" {
		t.Skip("未设置 PANDORA_TEST_MYSQL_DSN，跳过 inventory 真 MySQL 集成测试")
	}
	cfg, err := mysql.ParseDSN(serverDSN)
	if err != nil {
		t.Fatalf("解析 PANDORA_TEST_MYSQL_DSN: %v", err)
	}
	if cfg.DBName != "" {
		t.Fatalf("PANDORA_TEST_MYSQL_DSN 禁止带 database，got=%q", cfg.DBName)
	}
	cfg.MultiStatements = true
	cfg.ParseTime = true
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = inventoryMySQLTestTimeout
	cfg.WriteTimeout = inventoryMySQLTestTimeout

	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开 MySQL 管理连接: %v", err)
	}
	f := &inventoryMySQLFixture{admin: admin}
	t.Cleanup(func() { f.cleanup(t) })
	ctx, cancel := context.WithTimeout(context.Background(), inventoryMySQLTestTimeout)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("已设置测试 DSN 但 MySQL 不可达: %v", err)
	}

	f.dbName = randomInventoryMySQLTestDBName(t)
	if !inventoryMySQLTestDBPattern.MatchString(f.dbName) {
		t.Fatalf("随机测试库名未通过安全校验: %q", f.dbName)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+f.dbName+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		t.Fatalf("创建随机测试库 %s: %v", f.dbName, err)
	}
	f.created = true

	schema := readInventoryMySQLSchema(t, f.dbName)
	if _, err := admin.ExecContext(ctx, schema); err != nil {
		t.Fatalf("初始化 inventory schema: %v", err)
	}
	testCfg := cfg.Clone()
	testCfg.DBName = f.dbName
	db, err := sql.Open("mysql", testCfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开随机测试库: %v", err)
	}
	f.db = db
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(32)
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("连接随机测试库: %v", err)
	}
	var selected string
	if err := db.QueryRowContext(ctx, `SELECT DATABASE()`).Scan(&selected); err != nil || selected != f.dbName {
		t.Fatalf("当前数据库校验失败: selected=%q want=%q err=%v", selected, f.dbName, err)
	}
	return f
}

func (f *inventoryMySQLFixture) cleanup(t *testing.T) {
	t.Helper()
	if f.db != nil {
		if err := f.db.Close(); err != nil {
			t.Errorf("关闭 inventory 测试库连接: %v", err)
		}
	}
	if f.created {
		if !inventoryMySQLTestDBPattern.MatchString(f.dbName) {
			t.Errorf("拒绝 DROP 非预期测试库名 %q", f.dbName)
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), inventoryMySQLTestTimeout)
			_, err := f.admin.ExecContext(ctx, "DROP DATABASE `"+f.dbName+"`")
			cancel()
			if err != nil {
				t.Errorf("删除随机测试库 %s: %v", f.dbName, err)
			}
		}
	}
	if f.admin != nil {
		if err := f.admin.Close(); err != nil {
			t.Errorf("关闭 MySQL 管理连接: %v", err)
		}
	}
}

func randomInventoryMySQLTestDBName(t *testing.T) string {
	t.Helper()
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("生成随机测试库后缀: %v", err)
	}
	return fmt.Sprintf("pandora_inventory_it_%d_%s", time.Now().UnixMilli(), hex.EncodeToString(b))
}

func readInventoryMySQLSchema(t *testing.T, dbName string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("无法定位 inventory_repo_mysql_test.go")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "..",
		"deploy", "mysql-init", "08-inventory-tables.sql"))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 inventory schema %s: %v", path, err)
	}
	schema := string(b)
	const needle = "USE `pandora_trade`;"
	if strings.Count(schema, needle) != 1 {
		t.Fatalf("inventory schema USE 锚点数量异常: %d", strings.Count(schema, needle))
	}
	return strings.Replace(schema, needle, "USE `"+dbName+"`;", 1)
}

type mysqlEscrowRow struct {
	kind         int8
	itemConfigID uint32
	frozenQty    int64
	frozenAmount uint64
	currencyKind int32
	status       int8
}

func seedItem(t *testing.T, db *sql.DB, playerID uint64, itemID uint32, count int64) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO player_items(player_id,item_config_id,count) VALUES(?,?,?)`,
		playerID, itemID, count); err != nil {
		t.Fatalf("seed item player=%d item=%d: %v", playerID, itemID, err)
	}
}

// seedGold 往多币种钱包塞金币行(currency_kind=1)。
// 表是 player_wallet 而不是 legacy player_currency:后者是单列 gold 的存量只读表,
// 新装库(本 fixture 就是从 08-inventory-tables.sql 建的)根本不建它。
func seedGold(t *testing.T, db *sql.DB, playerID uint64, gold uint64) {
	t.Helper()
	seedCurrency(t, db, playerID, CurrencyGold, gold)
}

// seedCurrency 往钱包塞任意币种(多币种回归用)。
func seedCurrency(t *testing.T, db *sql.DB, playerID uint64, kind CurrencyKind, amount uint64) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO player_wallet(player_id,currency_kind,amount) VALUES(?,?,?)
ON DUPLICATE KEY UPDATE amount=VALUES(amount)`, playerID, int32(kind), amount); err != nil {
		t.Fatalf("seed currency player=%d kind=%d: %v", playerID, int32(kind), err)
	}
}

func queryItemCount(t *testing.T, db *sql.DB, playerID uint64, itemID uint32) int64 {
	t.Helper()
	var got int64
	err := db.QueryRow(`SELECT count FROM player_items WHERE player_id=? AND item_config_id=?`,
		playerID, itemID).Scan(&got)
	if errors.Is(err, sql.ErrNoRows) {
		return 0 // 堆叠扣空即删行(2026-07-22):无行 = 持有 0,语义一致。
	}
	if err != nil {
		t.Fatalf("query item player=%d item=%d: %v", playerID, itemID, err)
	}
	return got
}

// queryItemRowExists 断言行物理存在性(扣空即删行回归专用)。
func queryItemRowExists(t *testing.T, db *sql.DB, playerID uint64, itemID uint32) bool {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM player_items WHERE player_id=? AND item_config_id=?`,
		playerID, itemID).Scan(&n); err != nil {
		t.Fatalf("count item rows player=%d item=%d: %v", playerID, itemID, err)
	}
	return n > 0
}

// TestUseItemEmptiedRowDeleted 堆叠道具用尽即删行(2026-07-22 用户要求):
// 扣到 0 时 player_items 行物理删除(不留 count=0 死行);再发放同 config 走 upsert 重建。
func TestUseItemEmptiedRowDeleted(t *testing.T) {
	f := openInventoryMySQLFixture(t)
	repo := NewMySQLInventoryRepo(f.db)
	ctx := context.Background()

	const player, item = 901, 7901
	if _, _, err := repo.GrantItems(ctx, player, []ItemGrant{{ItemConfigID: item, Count: 3}}, nil, "g1", ""); err != nil {
		t.Fatalf("发放: %v", err)
	}
	remaining, _, err := repo.UseItem(ctx, player, item, 3, "u1", "")
	if err != nil || remaining != 0 {
		t.Fatalf("用尽: remaining=%d err=%v", remaining, err)
	}
	if queryItemRowExists(t, f.db, player, item) {
		t.Fatal("扣空后行必须物理删除,不得留 count=0 死行")
	}
	// 幂等重放:同 key 重试仍返回快照 0,不复活行。
	if remaining, already, err := repo.UseItem(ctx, player, item, 3, "u1", ""); err != nil || !already || remaining != 0 {
		t.Fatalf("幂等重放: remaining=%d already=%v err=%v", remaining, already, err)
	}
	// 再发放同 config:upsert 重建行,行为不变。
	if _, _, err := repo.GrantItems(ctx, player, []ItemGrant{{ItemConfigID: item, Count: 2}}, nil, "g2", ""); err != nil {
		t.Fatalf("重建发放: %v", err)
	}
	if got := queryItemCount(t, f.db, player, item); got != 2 {
		t.Fatalf("重建后应为 2,实际 %d", got)
	}
}

func TestCheckInstancesOwnedExactPair_MySQL(t *testing.T) {
	f := openInventoryMySQLFixture(t)
	repo := NewMySQLInventoryRepo(f.db)
	attrs, err := encodeInstanceAttrs(context.Background(), "player_item_instance", []ItemAttribute{
		{AttrID: 21, Value: 37}, {AttrID: 8, Value: 12},
	})
	if err != nil {
		t.Fatalf("encode attrs: %v", err)
	}
	// 9001 是绑定实例：bound 只禁止出售/丢弃/转移，不影响本人穿戴，exact ownership
	// 不得把它过滤掉；鉴定状态/词条也必须按原值返回。
	if _, err := f.db.Exec(`INSERT INTO player_item_instance
(instance_id, player_id, item_config_id, identified, attributes, slot_index, bound) VALUES
(9001, 701, 10003, 1, ?, 0, 1), (9002, 701, 10003, 0, NULL, 1, 0),
(9003, 702, 10003, 0, NULL, 0, 0)`, attrs); err != nil {
		t.Fatalf("seed instances: %v", err)
	}
	owned, err := repo.CheckInstancesOwned(context.Background(), 701, []InstanceOwnershipQuery{
		{InstanceID: 9001, ItemConfigID: 10003}, // exact
		{InstanceID: 9002, ItemConfigID: 10027}, // config mismatch
		{InstanceID: 9003, ItemConfigID: 10003}, // owner mismatch
		{InstanceID: 9999, ItemConfigID: 10003}, // missing
	})
	if err != nil {
		t.Fatalf("CheckInstancesOwned: %v", err)
	}
	if len(owned) != 1 || owned[0].InstanceID != 9001 || owned[0].ItemConfigID != 10003 ||
		!owned[0].Identified || !owned[0].Bound || len(owned[0].Attributes) != 2 ||
		owned[0].Attributes[0] != (ItemAttribute{AttrID: 21, Value: 37}) ||
		owned[0].Attributes[1] != (ItemAttribute{AttrID: 8, Value: 12}) {
		t.Fatalf("owned detail not preserved: %+v", owned)
	}
}

// TestItemClosureTransactions_MySQL 锁住本轮道具闭环新增的四条真实 MySQL 事务：
// 战斗消耗、战斗丢弃与大厅丢弃都必须只扣一次并回放首次 remaining；实例出售必须把
// 删除实例、金币入账与幂等流水放在同一事务里，绑定/入账失败时完整回滚。
func TestItemClosureTransactions_MySQL(t *testing.T) {
	f := openInventoryMySQLFixture(t)
	repo := NewMySQLInventoryRepo(f.db)
	ctx := context.Background()

	type stackMutation func(context.Context, uint64, uint32, int64, string, string) (int64, bool, error)
	stackCases := []struct {
		name   string
		player uint64
		item   uint32
		key    string
		op     string
		call   stackMutation
	}{
		{name: "ConsumeBattleItem", player: 801, item: 10001, key: "battle-consume-801", op: "battle_consume", call: repo.ConsumeBattleItem},
		{name: "DiscardBattleItem", player: 802, item: 10006, key: "battle-discard-802", op: "battle_discard", call: repo.DiscardBattleItem},
		{name: "DiscardItem", player: 803, item: 10007, key: "lobby-discard-803", op: "discard", call: repo.DiscardItem},
	}
	for _, tc := range stackCases {
		t.Run(tc.name, func(t *testing.T) {
			seedItem(t, f.db, tc.player, tc.item, 5)
			remaining, already, err := tc.call(ctx, tc.player, tc.item, 2, tc.key, "mysql closure")
			if err != nil || already || remaining != 3 {
				t.Fatalf("首次扣减: remaining=%d already=%v err=%v", remaining, already, err)
			}

			// 模拟服务端已提交但调用方丢失回包：同一请求重试只回放首次快照，不再次扣减。
			remaining, already, err = tc.call(ctx, tc.player, tc.item, 2, tc.key, "retry after lost response")
			if err != nil || !already || remaining != 3 {
				t.Fatalf("丢回包重放: remaining=%d already=%v err=%v", remaining, already, err)
			}
			if got := queryItemCount(t, f.db, tc.player, tc.item); got != 3 {
				t.Fatalf("重放后只能扣一次: got=%d want=3", got)
			}
			if got := queryLedgerOp(t, f.db, tc.player, tc.key); got != tc.op {
				t.Fatalf("ledger op=%q want=%q", got, tc.op)
			}

			// 同 key 改数量必须冲突，且不能改变库存。
			if _, _, err := tc.call(ctx, tc.player, tc.item, 1, tc.key, "conflicting retry"); errcode.As(err) != errcode.ErrInventoryIdempotencyConflict {
				t.Fatalf("同 key 改数量应冲突, got %v", err)
			}
			if got := queryItemCount(t, f.db, tc.player, tc.item); got != 3 {
				t.Fatalf("冲突重试不得扣库存: got=%d want=3", got)
			}
		})
	}

	t.Run("SellInstance", func(t *testing.T) {
		const (
			player        = uint64(804)
			instance      = uint64(98001)
			boundInstance = uint64(98002)
			item          = uint32(10003)
			key           = "sell-instance-804"
		)
		seedGold(t, f.db, player, 10)
		seedInstance(t, f.db, player, instance, item, false)
		out, already, err := repo.SellInstance(ctx, player, instance, item, CurrencyGold, 180, key, "mysql closure")
		if err != nil || already || out.Balances.Get(CurrencyGold) != 190 {
			t.Fatalf("首次出售: outcome=%+v already=%v err=%v", out, already, err)
		}
		// 余额 190 = 期初 10 + 本笔 180;两个数字分别断言,免得"入账额"被余额掩盖。
		if out.Earned != 180 || out.Kind != CurrencyGold {
			t.Fatalf("首次出售入账额/币种: earned=%d kind=%v", out.Earned, out.Kind)
		}
		if queryInstanceExists(t, f.db, player, instance) {
			t.Fatal("出售成功后实例必须删除")
		}
		if got := queryGold(t, f.db, player); got != 190 {
			t.Fatalf("出售金币=%d want=190", got)
		}
		if got := queryLedgerOp(t, f.db, player, key); got != "sell_inst" {
			t.Fatalf("ledger op=%q want=sell_inst", got)
		}

		// 实例已删除也能靠 ledger 回放首次金币快照，不重复加钱。
		out, already, err = repo.SellInstance(ctx, player, instance, item, CurrencyGold, 180, key, "retry after lost response")
		if err != nil || !already || out.Balances.Get(CurrencyGold) != 190 {
			t.Fatalf("丢回包重放: outcome=%+v already=%v err=%v", out, already, err)
		}
		// 回放的入账额必须仍是 180(来自 ledger 的 result_currency_delta),不是 0。
		if out.Earned != 180 {
			t.Fatalf("丢回包重放 earned=%d want=180", out.Earned)
		}
		if got := queryGold(t, f.db, player); got != 190 {
			t.Fatalf("重放不得重复加钱: got=%d want=190", got)
		}
		out, already, err = repo.SellInstance(ctx, player, instance, item, CurrencyGold, 181, key, "retry after price reload")
		if err != nil || !already || out.Balances.Get(CurrencyGold) != 190 || out.Earned != 180 {
			t.Fatalf("同客户端意图热更售价必须回放首次结果: outcome=%+v already=%v err=%v", out, already, err)
		}

		seedInstance(t, f.db, player, boundInstance, item, true)
		if _, _, err := repo.SellInstance(ctx, player, boundInstance, item, CurrencyGold, 180, "sell-bound-804", ""); errcode.As(err) != errcode.ErrInventoryInstanceBound {
			t.Fatalf("绑定实例必须拒绝出售, got %v", err)
		}
		if !queryInstanceExists(t, f.db, player, boundInstance) {
			t.Fatal("绑定保护失败后实例必须保留")
		}
		if got := queryGold(t, f.db, player); got != 190 {
			t.Fatalf("绑定保护失败不得加钱: got=%d want=190", got)
		}
		if got := queryLedgerCount(t, f.db, player, "sell-bound-804"); got != 0 {
			t.Fatalf("绑定保护失败必须回滚 ledger: got=%d", got)
		}
	})

	t.Run("SellItemPriceReloadReplaysFirstResult", func(t *testing.T) {
		const (
			player = uint64(806)
			item   = uint32(10008)
			key    = "sell-stack-806"
		)
		seedItem(t, f.db, player, item, 5)
		seedGold(t, f.db, player, 10)
		out, already, err := repo.SellItem(ctx, player, item, 2, CurrencyGold, 100, key,
			"sell item=10008 count=2 gold=100")
		if err != nil || already || out.Remaining != 3 || out.Balances.Get(CurrencyGold) != 110 || out.Earned != 100 {
			t.Fatalf("首次出售 outcome=%+v already=%v err=%v", out, already, err)
		}
		out, already, err = repo.SellItem(ctx, player, item, 2, CurrencyGold, 200, key,
			"sell item=10008 count=2 gold=200")
		if err != nil || !already || out.Remaining != 3 || out.Balances.Get(CurrencyGold) != 110 {
			t.Fatalf("价格热更回放 outcome=%+v already=%v err=%v", out, already, err)
		}
		// 回放必须回首次那笔 100,而不是按新价 200 重算 —— 否则客户端重试一次就"多挣了"。
		if out.Earned != 100 {
			t.Fatalf("价格热更回放 earned=%d want=100(不得按新价重算)", out.Earned)
		}
		if got := queryItemCount(t, f.db, player, item); got != 3 {
			t.Fatalf("热更重试不得二扣 item=%d", got)
		}
		if got := queryGold(t, f.db, player); got != 110 {
			t.Fatalf("热更重试不得按新价二加 gold=%d", got)
		}
	})

	// 非金币出售:入账必须落到指定币种,不得静默串到金币。
	t.Run("SellItemNonGoldCurrency", func(t *testing.T) {
		const (
			player = uint64(816)
			item   = uint32(10018)
			key    = "sell-stack-816-diamond"
		)
		seedItem(t, f.db, player, item, 5)
		seedGold(t, f.db, player, 7)
		out, already, err := repo.SellItem(ctx, player, item, 2, CurrencyDiamond, 50, key,
			"sell item=10018 count=2 gold=50")
		if err != nil || already || out.Earned != 50 || out.Kind != CurrencyDiamond {
			t.Fatalf("钻石出售 outcome=%+v already=%v err=%v", out, already, err)
		}
		if got := queryCurrency(t, f.db, player, CurrencyDiamond); got != 50 {
			t.Fatalf("钻石余额=%d want=50", got)
		}
		if got := queryGold(t, f.db, player); got != 7 {
			t.Fatalf("钻石出售不得动金币: gold=%d want=7", got)
		}
		// 余额快照必须含全部币种(不是只含本次那一种),否则客户端刷新后会看到钱少了。
		if out.Balances.Get(CurrencyGold) != 7 || out.Balances.Get(CurrencyDiamond) != 50 {
			t.Fatalf("余额快照应含全部币种: %+v", out.Balances)
		}
	})

	t.Run("LegacySaleLedgerUsesPersistedFirstPrice", func(t *testing.T) {
		const (
			stackPlayer    = uint64(807)
			stackItem      = uint32(10009)
			stackKey       = "legacy-sell-stack-807"
			instancePlayer = uint64(808)
			instanceID     = uint64(98008)
			instanceItem   = uint32(10003)
			instanceKey    = "legacy-sell-instance-808"
		)
		// 模拟旧版本已按旧价完成并丢失响应：资产状态和 ledger 快照均已提交。
		seedItem(t, f.db, stackPlayer, stackItem, 3)
		seedGold(t, f.db, stackPlayer, 110)
		if _, err := f.db.Exec(`INSERT INTO inventory_ledger
(player_id,idempotency_key,op,request_fingerprint,detail,result_remaining,result_gold)
VALUES(?,?,?,?,?,?,?)`, stackPlayer, stackKey, "sell",
			legacySellFingerprint(stackItem, 2, 100), "sell item=10009 count=2 gold=100", 3, 110); err != nil {
			t.Fatalf("seed legacy stack ledger: %v", err)
		}
		out, already, err := repo.SellItem(ctx, stackPlayer, stackItem, 2, CurrencyGold, 200, stackKey,
			"sell item=10009 count=2 gold=200")
		if err != nil || !already || out.Remaining != 3 || out.Balances.Get(CurrencyGold) != 110 {
			t.Fatalf("legacy stack replay outcome=%+v already=%v err=%v", out, already, err)
		}
		// 老行没有 result_currency_delta 列,入账额只能从 detail 的 `gold=100` 恢复;
		// 恢复不出来就会静默变成 0,所以这条断言是 legacySaleEarned 的唯一守门。
		if out.Earned != 100 {
			t.Fatalf("legacy stack replay earned=%d want=100(须从老行 detail 恢复)", out.Earned)
		}
		if queryItemCount(t, f.db, stackPlayer, stackItem) != 3 || queryGold(t, f.db, stackPlayer) != 110 {
			t.Fatal("legacy stack replay changed assets")
		}

		seedGold(t, f.db, instancePlayer, 190) // 旧实例已删除，只剩首次快照。
		if _, err := f.db.Exec(`INSERT INTO inventory_ledger
(player_id,idempotency_key,op,request_fingerprint,detail,result_remaining,result_gold)
VALUES(?,?,?,?,?,0,?)`, instancePlayer, instanceKey, "sell_inst",
			legacySellInstanceFingerprint(instanceID, instanceItem, 180),
			"sell instance=98008 item=10003 gold=180", 190); err != nil {
			t.Fatalf("seed legacy instance ledger: %v", err)
		}
		out, already, err = repo.SellInstance(ctx, instancePlayer, instanceID, instanceItem, CurrencyGold, 360, instanceKey,
			"sell instance=98008 item=10003 gold=360")
		if err != nil || !already || out.Balances.Get(CurrencyGold) != 190 || queryGold(t, f.db, instancePlayer) != 190 {
			t.Fatalf("legacy instance replay outcome=%+v already=%v err=%v", out, already, err)
		}
		if out.Earned != 180 {
			t.Fatalf("legacy instance replay earned=%d want=180(须从老行 detail 恢复)", out.Earned)
		}

		// 旧 hash 不能成为万能通行证：detail 的意图不一致时必须冲突。
		if _, err := f.db.Exec(`INSERT INTO inventory_ledger
(player_id,idempotency_key,op,request_fingerprint,detail,result_remaining,result_gold)
VALUES(?,?,?,?,?,?,?)`, 809, "legacy-malformed-809", "sell",
			legacySellFingerprint(10010, 2, 100), "sell item=10010 count=3 gold=100", 3, 100); err != nil {
			t.Fatalf("seed malformed legacy ledger: %v", err)
		}
		if _, _, err := repo.SellItem(ctx, 809, 10010, 2, CurrencyGold, 200, "legacy-malformed-809",
			"sell item=10010 count=2 gold=200"); errcode.As(err) != errcode.ErrInventoryIdempotencyConflict {
			t.Fatalf("malformed legacy detail must conflict, got %v", err)
		}
	})

	// 加法上限闸:余额已在 MaxCurrencyAmount(2^62)时再入账必须**拒绝**并整笔回滚,
	// 绝不能回绕(回绕会让首富瞬间变零元户)。错误码是 ErrInventoryCurrencyOverflow ——
	// 改造前这里是 SQL 列溢出导致的 ErrInternal,现在闸提前到应用层,是可识别的业务错误。
	t.Run("SellInstanceCurrencyOverflowRollsBack", func(t *testing.T) {
		const (
			player   = uint64(805)
			instance = uint64(98003)
			item     = uint32(10003)
		)
		seedGold(t, f.db, player, MaxCurrencyAmount)
		seedInstance(t, f.db, player, instance, item, false)
		if _, _, err := repo.SellInstance(ctx, player, instance, item, CurrencyGold, 1, "sell-overflow-805", ""); errcode.As(err) != errcode.ErrInventoryCurrencyOverflow {
			t.Fatalf("越过货币上限应拒绝入账, got %v", err)
		}
		if !queryInstanceExists(t, f.db, player, instance) {
			t.Fatal("金币入账失败必须回滚实例删除")
		}
		if got := queryGold(t, f.db, player); got != MaxCurrencyAmount {
			t.Fatalf("金币入账失败必须保留原余额: got=%d want=%d", got, MaxCurrencyAmount)
		}
		if got := queryLedgerCount(t, f.db, player, "sell-overflow-805"); got != 0 {
			t.Fatalf("金币入账失败必须回滚 ledger: got=%d", got)
		}
	})
}

func seedInstance(t *testing.T, db *sql.DB, playerID, instanceID uint64, itemID uint32, bound bool) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO player_item_instance(instance_id,player_id,item_config_id,bound) VALUES(?,?,?,?)`,
		instanceID, playerID, itemID, bound); err != nil {
		t.Fatalf("seed instance player=%d instance=%d: %v", playerID, instanceID, err)
	}
}

func queryInstanceExists(t *testing.T, db *sql.DB, playerID, instanceID uint64) bool {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM player_item_instance WHERE player_id=? AND instance_id=?`,
		playerID, instanceID).Scan(&count); err != nil {
		t.Fatalf("query instance player=%d instance=%d: %v", playerID, instanceID, err)
	}
	return count == 1
}

func queryLedgerOp(t *testing.T, db *sql.DB, playerID uint64, key string) string {
	t.Helper()
	var op string
	if err := db.QueryRow(`SELECT op FROM inventory_ledger WHERE player_id=? AND idempotency_key=?`,
		playerID, key).Scan(&op); err != nil {
		t.Fatalf("query ledger player=%d key=%s: %v", playerID, key, err)
	}
	return op
}

func queryLedgerCount(t *testing.T, db *sql.DB, playerID uint64, key string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM inventory_ledger WHERE player_id=? AND idempotency_key=?`,
		playerID, key).Scan(&count); err != nil {
		t.Fatalf("count ledger player=%d key=%s: %v", playerID, key, err)
	}
	return count
}

func queryGold(t *testing.T, db *sql.DB, playerID uint64) uint64 {
	t.Helper()
	return queryCurrency(t, db, playerID, CurrencyGold)
}

// queryCurrency 读某币种余额;**无行 = 0**(钱包不预建行,与 data 层读取口径一致)。
func queryCurrency(t *testing.T, db *sql.DB, playerID uint64, kind CurrencyKind) uint64 {
	t.Helper()
	var got uint64
	err := db.QueryRow(`SELECT amount FROM player_wallet WHERE player_id=? AND currency_kind=?`,
		playerID, int32(kind)).Scan(&got)
	if errors.Is(err, sql.ErrNoRows) {
		return 0
	}
	if err != nil {
		t.Fatalf("query currency player=%d kind=%d: %v", playerID, int32(kind), err)
	}
	return got
}

func queryEscrow(t *testing.T, db *sql.DB, playerID, orderID uint64) mysqlEscrowRow {
	t.Helper()
	var got mysqlEscrowRow
	if err := db.QueryRow(`SELECT kind,item_config_id,frozen_qty,frozen_amount,currency_kind,status
        FROM auction_escrow WHERE player_id=? AND order_id=?`, playerID, orderID).
		Scan(&got.kind, &got.itemConfigID, &got.frozenQty, &got.frozenAmount, &got.currencyKind, &got.status); err != nil {
		t.Fatalf("query escrow player=%d order=%d: %v", playerID, orderID, err)
	}
	return got
}
