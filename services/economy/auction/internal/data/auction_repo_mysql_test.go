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
	"sync/atomic"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

type twoDBTestRouter struct{ dbs [2]*sql.DB }

func (r twoDBTestRouter) ForMarket(marketID uint32) *sql.DB { return r.dbs[marketID%2] }
func (r twoDBTestRouter) ForOwner(ownerID uint64) *sql.DB   { return r.dbs[ownerID%2] }
func (r twoDBTestRouter) All() []*sql.DB                    { return []*sql.DB{r.dbs[0], r.dbs[1]} }

// TestAuctionRepo_MySQL 是 opt-in 真 MySQL 8.4 集成测试。
//
// PANDORA_TEST_MYSQL_DSN 必须是“不带 database”的测试实例 DSN；测试会创建并删除随机
// pandora_auction_it_* 数据库。账号需具备 CREATE/DROP DATABASE 以及该随机库内的
// CREATE/ALTER/INDEX/SELECT/INSERT/UPDATE/DELETE 权限。禁止指向生产实例。
func TestAuctionRepo_MySQL(t *testing.T) {
	serverDSN := strings.TrimSpace(os.Getenv("PANDORA_TEST_MYSQL_DSN"))
	if serverDSN == "" {
		t.Skip("未设置 PANDORA_TEST_MYSQL_DSN，跳过真 MySQL 8.4 集成测试")
	}
	serverCfg, err := mysql.ParseDSN(serverDSN)
	if err != nil {
		t.Fatalf("解析 PANDORA_TEST_MYSQL_DSN: %v", err)
	}
	if serverCfg.DBName != "" {
		t.Fatalf("PANDORA_TEST_MYSQL_DSN 禁止带 database，got=%q", serverCfg.DBName)
	}
	serverCfg.MultiStatements = true
	serverCfg.ParseTime = true
	serverCfg.Timeout = 5 * time.Second
	serverCfg.ReadTimeout = 10 * time.Second
	serverCfg.WriteTimeout = 10 * time.Second

	admin, err := sql.Open("mysql", serverCfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开 MySQL: %v", err)
	}
	t.Cleanup(func() {
		if err := admin.Close(); err != nil {
			t.Errorf("关闭 MySQL admin: %v", err)
		}
	})
	// 这一个预算罩住**整段** setup:Ping + 2×CREATE DATABASE + 2×全量 schema 载入,
	// 是累计值而不是单条超时 —— 每多一条 DDL,前面每条能用的时间就少一截。
	//
	// 2026-08-24 backend-dev #48 就是这么红的:同机同时在跑 UE 编译(MSBuild 38 个 worker),
	// 整场 CI 慢 6~15 倍(inventory data 6.1s→93.9s),本测试 1.38s→14.66s,
	// 前四条 DDL 吃光 10s,第二次 schema 载入直接 context deadline exceeded。
	// 代码一个字没改(那次构建与上一次只差一个 dev_all_python.ps1 的提交),纯粹是被同机负载挤死的。
	//
	// 放宽到 60s 不会让"真挂死"变得难发现:DSN 已经带了 Timeout=5s / ReadTimeout=10s /
	// WriteTimeout=10s,单条网络操作卡住会先被驱动超时打断,本预算只是兜住累计时长。
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("连接 MySQL: %v", err)
	}

	dbName := randomAuctionTestDBName(t)
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+dbName+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		t.Fatalf("创建随机测试库 %s: %v", dbName, err)
	}
	t.Cleanup(func() {
		if !regexp.MustCompile(`^pandora_auction_it_[0-9]+_[0-9a-f]{12}$`).MatchString(dbName) {
			t.Errorf("拒绝 DROP 非预期测试库名 %q", dbName)
			return
		}
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer dropCancel()
		if _, err := admin.ExecContext(dropCtx, "DROP DATABASE `"+dbName+"`"); err != nil {
			t.Errorf("删除随机测试库 %s: %v", dbName, err)
		}
	})

	schema := readAuctionSchema(t, dbName)
	if _, err := admin.ExecContext(ctx, schema); err != nil {
		t.Fatalf("初始化 auction schema: %v", err)
	}

	dbCfg := serverCfg.Clone()
	dbCfg.DBName = dbName
	db, err := sql.Open("mysql", dbCfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开随机测试库: %v", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("关闭随机测试库: %v", err)
		}
	})
	repo := NewMySQLAuctionRepo(SingleDB{DB: db})

	// 第二个随机库用于验证 owner coordinator 与 market shard 分离时的全局幂等契约。
	shardDBName := randomAuctionTestDBName(t)
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+shardDBName+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		t.Fatalf("创建第二随机测试库 %s: %v", shardDBName, err)
	}
	t.Cleanup(func() {
		if !regexp.MustCompile(`^pandora_auction_it_[0-9]+_[0-9a-f]{12}$`).MatchString(shardDBName) {
			t.Errorf("拒绝 DROP 非预期测试库名 %q", shardDBName)
			return
		}
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer dropCancel()
		if _, err := admin.ExecContext(dropCtx, "DROP DATABASE `"+shardDBName+"`"); err != nil {
			t.Errorf("删除第二随机测试库 %s: %v", shardDBName, err)
		}
	})
	if _, err := admin.ExecContext(ctx, readAuctionSchema(t, shardDBName)); err != nil {
		t.Fatalf("初始化第二 auction schema: %v", err)
	}
	shardCfg := serverCfg.Clone()
	shardCfg.DBName = shardDBName
	shardDB, err := sql.Open("mysql", shardCfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开第二随机测试库: %v", err)
	}
	shardDB.SetMaxOpenConns(16)
	shardDB.SetMaxIdleConns(16)
	t.Cleanup(func() {
		if err := shardDB.Close(); err != nil {
			t.Errorf("关闭第二随机测试库: %v", err)
		}
	})
	shardedRepo := NewMySQLAuctionRepo(twoDBTestRouter{dbs: [2]*sql.DB{db, shardDB}})

	t.Run("PendingIsolationAndAtomicReserve", func(t *testing.T) {
		truncateAuctionTables(t, db)
		now := time.Now().UnixMilli()
		pendingBest := testOrder(1001, 1, SideSell, 7, 5, 80, StatusPending, now)
		resting := testOrder(1002, 2, SideSell, 7, 6, 90, StatusPending, now+1)
		incoming := testOrder(1003, 3, SideBuy, 7, 10, 100, StatusPending, now+2)
		claimOrder(t, repo, pendingBest)
		claimOrder(t, repo, resting)
		claimOrder(t, repo, incoming)
		for _, o := range []*OrderRecord{resting, incoming} {
			if changed, err := repo.ConfirmOrderEscrow(context.Background(), 42, o.OrderID, now+3); err != nil || !changed {
				t.Fatalf("确认 escrow %d: changed=%v err=%v", o.OrderID, changed, err)
			}
		}
		if confirmed, err := repo.ConfirmOrderEscrow(context.Background(), 42, incoming.OrderID, now+3); err != nil || !confirmed {
			t.Fatalf("同毫秒重复确认 escrow 必须幂等成功: confirmed=%v err=%v", confirmed, err)
		}
		if changed, err := repo.ActivateOrder(context.Background(), 42, resting.OrderID, now+3); err != nil || !changed {
			t.Fatalf("激活 resting: changed=%v err=%v", changed, err)
		}

		best, found, err := repo.FindBestActiveOrder(context.Background(), 42, 7, SideSell, incoming.OwnerID)
		if err != nil || !found || best.OrderID != resting.OrderID {
			t.Fatalf("PENDING 不得成为 resting: best=%v found=%v err=%v", best, found, err)
		}

		m, gotIncoming, gotResting, reserved, err := repo.ReserveMatch(
			context.Background(), 42, incoming.OrderID, resting.OrderID, 9001, now+4)
		if err != nil || !reserved {
			t.Fatalf("ReserveMatch: reserved=%v err=%v", reserved, err)
		}
		if m.Quantity != 6 || m.Price != 90 || m.SettlementStatus != SettlementPending {
			t.Fatalf("match=%+v, want qty=6 price=90 pending", m)
		}
		if gotIncoming.Status != StatusPartial || gotIncoming.FilledQuantity != 6 || !gotIncoming.MatchPending {
			t.Fatalf("incoming=%+v, want PARTIAL filled=6 match_pending", gotIncoming)
		}
		if gotResting.Status != StatusFilled || !gotResting.ReleasePending {
			t.Fatalf("resting=%+v, want FILLED release_pending", gotResting)
		}
		if _, releasable, err := repo.GetReleasableOrder(context.Background(), 42, resting.OrderID); err != nil || releasable {
			t.Fatalf("待结算成交存在时不得释放: releasable=%v err=%v", releasable, err)
		}
		if completed, err := repo.CompleteMatch(context.Background(), 42, m.MatchID); err != nil || !completed {
			t.Fatalf("CompleteMatch: completed=%v err=%v", completed, err)
		}
		if _, releasable, err := repo.GetReleasableOrder(context.Background(), 42, resting.OrderID); err != nil || !releasable {
			t.Fatalf("结算完成后的终态单应可释放: releasable=%v err=%v", releasable, err)
		}
		if cleared, err := repo.ClearReleasePending(context.Background(), 42, resting.OrderID); err != nil || !cleared {
			t.Fatalf("ClearReleasePending: cleared=%v err=%v", cleared, err)
		}
	})

	t.Run("ConcurrentReserveCannotOversell", func(t *testing.T) {
		truncateAuctionTables(t, db)
		now := time.Now().UnixMilli()
		resting := testOrder(2001, 10, SideSell, 8, 10, 50, StatusPending, now)
		incomingA := testOrder(2002, 11, SideBuy, 8, 10, 60, StatusPending, now+1)
		incomingB := testOrder(2003, 12, SideBuy, 8, 10, 60, StatusPending, now+2)
		for _, o := range []*OrderRecord{resting, incomingA, incomingB} {
			claimOrder(t, repo, o)
			if changed, err := repo.ConfirmOrderEscrow(context.Background(), 42, o.OrderID, now+3); err != nil || !changed {
				t.Fatalf("确认 escrow %d: changed=%v err=%v", o.OrderID, changed, err)
			}
		}
		if changed, err := repo.ActivateOrder(context.Background(), 42, resting.OrderID, now+3); err != nil || !changed {
			t.Fatalf("激活 resting: changed=%v err=%v", changed, err)
		}

		start := make(chan struct{})
		var successes atomic.Int32
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		for i, incoming := range []*OrderRecord{incomingA, incomingB} {
			wg.Add(1)
			go func(i int, incoming *OrderRecord) {
				defer wg.Done()
				<-start
				_, _, _, reserved, err := repo.ReserveMatch(
					context.Background(), 42, incoming.OrderID, resting.OrderID, uint64(9100+i), now+10+int64(i))
				if err != nil {
					errs <- err
					return
				}
				if reserved {
					successes.Add(1)
				}
			}(i, incoming)
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("并发 ReserveMatch: %v", err)
		}
		if got := successes.Load(); got != 1 {
			t.Fatalf("成功 Reserve 数=%d, want=1", got)
		}
		got, found, err := repo.GetOrder(context.Background(), 42, resting.OrderID)
		if err != nil || !found || got.FilledQuantity != 10 || got.Status != StatusFilled {
			t.Fatalf("resting=%+v found=%v err=%v, want filled exactly 10", got, found, err)
		}
	})

	t.Run("OldBinaryDefaultsNeverReleaseActiveEarly", func(t *testing.T) {
		truncateAuctionTables(t, db)
		now := time.Now().UnixMilli()
		// 模拟旧二进制 INSERT：省略 release_pending/match_pending/settlement 新列。
		_, err := db.Exec(`INSERT INTO auction_orders
			(order_id,market_id,owner_id,side,item_config_id,quantity,filled_quantity,price,status,idempotency_key,created_at_ms,updated_at_ms)
			VALUES (3001,42,20,1,9,3,0,70,1,'old-binary',?,?)`, now, now)
		if err != nil {
			t.Fatalf("旧列集合 INSERT: %v", err)
		}
		o, found, err := repo.GetOrder(context.Background(), 42, 3001)
		if err != nil || !found || !o.ReleasePending {
			t.Fatalf("旧 INSERT 应从 default=1 获得 marker: order=%+v found=%v err=%v", o, found, err)
		}
		if _, releasable, err := repo.GetReleasableOrder(context.Background(), 42, 3001); err != nil || releasable {
			t.Fatalf("活跃旧单即使 marker=1 也不得提前释放: releasable=%v err=%v", releasable, err)
		}
		// 模拟旧二进制只更新 status/fill，不知道新列；marker 仍为 1，终态可被新 worker 补偿。
		if _, err := db.Exec(`UPDATE auction_orders SET filled_quantity=quantity,status=3,updated_at_ms=? WHERE order_id=3001`, now+1); err != nil {
			t.Fatalf("旧列集合 UPDATE: %v", err)
		}
		if _, releasable, err := repo.GetReleasableOrder(context.Background(), 42, 3001); err != nil || !releasable {
			t.Fatalf("旧二进制终态应可补偿释放: releasable=%v err=%v", releasable, err)
		}

		// migration 完成后仍在运行的旧 RecordMatch 会省略 event_pending；默认 1 必须让它
		// 自动进入新 outbox，覆盖旧进程在发送前退出或 Kafka 失败的窗口。
		_, err = db.Exec(`INSERT INTO auction_matches
			(match_id,market_id,sell_order_id,buy_order_id,seller_id,buyer_id,item_config_id,quantity,price,matched_at_ms)
			VALUES (3002,42,3001,3999,20,21,9,1,70,?)`, now+2)
		if err != nil {
			t.Fatalf("旧成交列集合 INSERT: %v", err)
		}
		var eventPending int
		if err := db.QueryRow(`SELECT event_pending FROM auction_matches WHERE match_id=3002`).Scan(&eventPending); err != nil || eventPending != 1 {
			t.Fatalf("旧成交必须自动登记 outbox: event_pending=%d err=%v", eventPending, err)
		}
	})

	t.Run("MatchEventWaitsForTerminalEscrowRelease", func(t *testing.T) {
		truncateAuctionTables(t, db)
		now := time.Now().UnixMilli()
		_, err := db.Exec(`INSERT INTO auction_orders
			(order_id,market_id,owner_id,side,item_config_id,quantity,filled_quantity,price,status,
			 release_pending,match_pending,escrow_verified,idempotency_key,created_at_ms,updated_at_ms)
			VALUES
			(3101,42,20,1,9,1,1,70,3,1,0,0,'event-release-sell',?,?),
			(3102,42,21,2,9,2,1,70,2,1,0,1,'event-release-buy',?,?)`, now, now, now, now)
		if err != nil {
			t.Fatalf("seed release barrier orders: %v", err)
		}
		_, err = db.Exec(`INSERT INTO auction_matches
			(match_id,market_id,sell_order_id,buy_order_id,seller_id,buyer_id,item_config_id,quantity,price,
			 matched_at_ms,settlement_status,event_pending)
			VALUES (3103,42,3101,3102,20,21,9,1,70,?,1,1)`, now+1)
		if err != nil {
			t.Fatalf("seed release barrier match: %v", err)
		}
		events, err := repo.ListPendingMatchEvents(context.Background(), 10)
		if err != nil || len(events) != 0 {
			t.Fatalf("release pending must hide event: events=%+v err=%v", events, err)
		}
		if _, err := db.Exec(`UPDATE auction_orders SET release_pending=0 WHERE order_id=3101`); err != nil {
			t.Fatalf("clear release barrier: %v", err)
		}
		// PARTIAL 模拟 migration/旧二进制的兼容 default=1；它仍有有效 escrow，不能把事件永久卡住。
		events, err = repo.ListPendingMatchEvents(context.Background(), 10)
		if err != nil || len(events) != 1 || events[0].MatchID != 3103 {
			t.Fatalf("cleared release barrier must expose event: events=%+v err=%v", events, err)
		}
	})

	t.Run("DeferredPoisonDoesNotStarveReadyRecords", func(t *testing.T) {
		truncateAuctionTables(t, db)
		now := time.Now().UnixMilli()
		for _, id := range []uint64{4001, 4002} {
			_, err := db.Exec(`INSERT INTO auction_matches
				(match_id,market_id,sell_order_id,buy_order_id,seller_id,buyer_id,item_config_id,quantity,price,matched_at_ms,settlement_status)
				VALUES (?,42,?,?,31,32,11,1,10,?,0)`, id, id+100, id+200, now+int64(id))
			if err != nil {
				t.Fatalf("插入待结算成交 %d: %v", id, err)
			}
		}
		if changed, err := repo.DeferMatchSettlement(context.Background(), 42, 4001, now+60_000); err != nil || !changed {
			t.Fatalf("退避 poison match: changed=%v err=%v", changed, err)
		}
		matches, err := repo.ListPendingMatches(context.Background(), 1)
		if err != nil || len(matches) != 1 || matches[0].MatchID != 4002 {
			t.Fatalf("待结算公平批次=%+v err=%v, want match 4002", matches, err)
		}

		for _, id := range []uint64{4401, 4402} {
			o := testOrder(id, id, SideSell, 12, 1, 10, StatusCanceled, now+int64(id))
			o.ReleasePending = true
			claimOrder(t, repo, o)
		}
		if changed, err := repo.DeferOrderRelease(context.Background(), 42, 4401, now+60_000); err != nil || !changed {
			t.Fatalf("退避 poison order: changed=%v err=%v", changed, err)
		}
		orders, err := repo.ListReleasableOrders(context.Background(), 1)
		if err != nil || len(orders) != 1 || orders[0].OrderID != 4402 {
			t.Fatalf("待释放公平批次=%+v err=%v, want order 4402", orders, err)
		}
	})

	t.Run("GlobalIdempotencyAcrossMarketShards", func(t *testing.T) {
		truncateAuctionTables(t, db)
		truncateAuctionTables(t, shardDB)
		now := time.Now().UnixMilli()
		canonical := testOrder(5001, 600, SideSell, 20, 2, 30, StatusPending, now)
		canonical.MarketID = 43 // owner=600 路由 db0，market=43 路由 db1，强制跨库。
		canonical.IdempotencyKey = "global-idem"
		got, already, err := shardedRepo.ClaimOrder(context.Background(), canonical)
		if err != nil || already || got.OrderID != canonical.OrderID {
			t.Fatalf("first cross-shard claim: got=%+v already=%v err=%v", got, already, err)
		}
		retry := *canonical
		retry.OrderID = 5002
		got, already, err = shardedRepo.ClaimOrder(context.Background(), &retry)
		if err != nil || !already || got.OrderID != canonical.OrderID {
			t.Fatalf("same fingerprint retry: got=%+v already=%v err=%v", got, already, err)
		}
		conflict := *canonical
		conflict.OrderID = 5003
		conflict.MarketID = 42
		if _, _, err := shardedRepo.ClaimOrder(context.Background(), &conflict); errcode.As(err) != errcode.ErrAuctionIdempotencyConflict {
			t.Fatalf("cross-market key reuse must conflict: err=%v code=%d", err, errcode.As(err))
		}
		var orderCount, registryCount int
		if err := shardDB.QueryRow(`SELECT COUNT(*) FROM auction_orders WHERE owner_id=600 AND idempotency_key='global-idem'`).Scan(&orderCount); err != nil {
			t.Fatalf("count market order: %v", err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM auction_idempotency_keys WHERE owner_id=600 AND idempotency_key='global-idem'`).Scan(&registryCount); err != nil {
			t.Fatalf("count coordinator claim: %v", err)
		}
		if orderCount != 1 || registryCount != 1 {
			t.Fatalf("global canonical drift: orders=%d registry=%d", orderCount, registryCount)
		}
	})

	t.Run("ConcurrentGlobalIdempotencyChoosesOneFingerprint", func(t *testing.T) {
		truncateAuctionTables(t, db)
		truncateAuctionTables(t, shardDB)
		now := time.Now().UnixMilli()
		const workers = 16
		start := make(chan struct{})
		var firstClaims atomic.Int32
		var conflicts atomic.Int32
		errs := make(chan error, workers)
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				o := testOrder(uint64(5100+i), 700, SideSell, 21, 1, 40, StatusPending, now+int64(i))
				o.MarketID = uint32(42 + i%2)
				o.IdempotencyKey = "concurrent-global-idem"
				_, already, err := shardedRepo.ClaimOrder(context.Background(), o)
				if err != nil {
					if errcode.As(err) == errcode.ErrAuctionIdempotencyConflict {
						conflicts.Add(1)
						return
					}
					errs <- err
					return
				}
				if !already {
					firstClaims.Add(1)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("concurrent global claim: %v", err)
		}
		var totalOrders, totalClaims int
		for _, physical := range []*sql.DB{db, shardDB} {
			var n int
			if err := physical.QueryRow(`SELECT COUNT(*) FROM auction_orders WHERE owner_id=700 AND idempotency_key='concurrent-global-idem'`).Scan(&n); err != nil {
				t.Fatalf("count concurrent orders: %v", err)
			}
			totalOrders += n
			if err := physical.QueryRow(`SELECT COUNT(*) FROM auction_idempotency_keys WHERE owner_id=700 AND idempotency_key='concurrent-global-idem'`).Scan(&n); err != nil {
				t.Fatalf("count concurrent claims: %v", err)
			}
			totalClaims += n
		}
		if firstClaims.Load() != 1 || conflicts.Load() != workers/2 || totalOrders != 1 || totalClaims != 1 {
			t.Fatalf("global concurrent result: first=%d conflicts=%d orders=%d claims=%d",
				firstClaims.Load(), conflicts.Load(), totalOrders, totalClaims)
		}
	})

	t.Run("OppositeShardClaimsDoNotDeadlockConnectionPools", func(t *testing.T) {
		truncateAuctionTables(t, db)
		truncateAuctionTables(t, shardDB)
		// 每个 shard 只留一条连接，复现“db0 coordinator 等 db1、db1 coordinator 等 db0”
		// 的最小条件。ClaimOrder 不得持 coordinator 事务再访问另一 shard。
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		shardDB.SetMaxOpenConns(1)
		shardDB.SetMaxIdleConns(1)
		defer func() {
			db.SetMaxOpenConns(8)
			db.SetMaxIdleConns(8)
			shardDB.SetMaxOpenConns(16)
			shardDB.SetMaxIdleConns(16)
		}()

		now := time.Now().UnixMilli()
		orders := []*OrderRecord{
			testOrder(5151, 1000, SideSell, 31, 1, 10, StatusPending, now),
			testOrder(5152, 1001, SideBuy, 32, 1, 11, StatusPending, now+1),
		}
		orders[0].MarketID, orders[0].IdempotencyKey = 43, "opposite-pool-a" // owner→db0, market→db1
		orders[1].MarketID, orders[1].IdempotencyKey = 42, "opposite-pool-b" // owner→db1, market→db0

		claimCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		start := make(chan struct{})
		errs := make(chan error, len(orders))
		var wg sync.WaitGroup
		for _, order := range orders {
			order := order
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				got, already, claimErr := shardedRepo.ClaimOrder(claimCtx, order)
				if claimErr != nil {
					errs <- claimErr
					return
				}
				if already || got.OrderID != order.OrderID {
					errs <- fmt.Errorf("unexpected claim order=%d got=%+v already=%v", order.OrderID, got, already)
				}
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for claimErr := range errs {
			t.Fatalf("opposite-shard claim failed: %v", claimErr)
		}
	})

	t.Run("LegacyOrderWithoutRegistryIsAdopted", func(t *testing.T) {
		truncateAuctionTables(t, db)
		truncateAuctionTables(t, shardDB)
		now := time.Now().UnixMilli()
		_, err := shardDB.Exec(`INSERT INTO auction_orders
			(order_id,market_id,owner_id,side,item_config_id,quantity,filled_quantity,price,status,
			 release_pending,match_pending,escrow_verified,reconcile_next_attempt_at_ms,release_next_attempt_at_ms,
			 idempotency_key,created_at_ms,updated_at_ms)
			VALUES (5201,43,800,1,22,1,0,50,1,1,0,0,0,0,'legacy-no-registry',?,?)`, now, now)
		if err != nil {
			t.Fatalf("seed legacy order: %v", err)
		}
		retry := testOrder(5299, 800, SideSell, 22, 1, 50, StatusPending, now+1)
		retry.MarketID = 43
		retry.IdempotencyKey = "legacy-no-registry"
		got, already, err := shardedRepo.ClaimOrder(context.Background(), retry)
		if err != nil || !already || got.OrderID != 5201 {
			t.Fatalf("adopt legacy canonical: got=%+v already=%v err=%v", got, already, err)
		}
		var registryOrder uint64
		if err := db.QueryRow(`SELECT order_id FROM auction_idempotency_keys
			WHERE owner_id=800 AND idempotency_key='legacy-no-registry'`).Scan(&registryOrder); err != nil || registryOrder != 5201 {
			t.Fatalf("legacy registry order=%d err=%v", registryOrder, err)
		}
	})

	t.Run("RegistryOnlyHealUsesStoredCanonicalKey", func(t *testing.T) {
		truncateAuctionTables(t, db)
		truncateAuctionTables(t, shardDB)
		now := time.Now().UnixMilli()
		_, err := db.Exec(`INSERT INTO auction_idempotency_keys
			(owner_id,idempotency_key,order_id,market_id,side,item_config_id,quantity,price,created_at_ms)
			VALUES (900,'Canonical-Key',5301,43,1,23,2,60,?)`, now)
		if err != nil {
			t.Fatalf("seed registry-only canonical: %v", err)
		}
		retry := testOrder(5399, 900, SideSell, 23, 2, 60, StatusPending, now+1)
		retry.MarketID = 43
		retry.IdempotencyKey = "canonical-key" // CI collation 等价，但字面值不同。
		got, already, err := shardedRepo.ClaimOrder(context.Background(), retry)
		if err != nil || !already || got.OrderID != 5301 || got.IdempotencyKey != "Canonical-Key" {
			t.Fatalf("heal registry-only: got=%+v already=%v err=%v", got, already, err)
		}
		var storedKey string
		if err := shardDB.QueryRow(`SELECT idempotency_key FROM auction_orders WHERE order_id=5301`).Scan(&storedKey); err != nil || storedKey != "Canonical-Key" {
			t.Fatalf("healed market key=%q err=%v", storedKey, err)
		}
	})

	t.Run("RegistryMarketFingerprintDriftFailsClosed", func(t *testing.T) {
		truncateAuctionTables(t, db)
		truncateAuctionTables(t, shardDB)
		now := time.Now().UnixMilli()
		_, err := db.Exec(`INSERT INTO auction_idempotency_keys
			(owner_id,idempotency_key,order_id,market_id,side,item_config_id,quantity,price,created_at_ms)
			VALUES (902,'drift-key',5401,43,1,24,2,70,?)`, now)
		if err != nil {
			t.Fatalf("seed drift registry: %v", err)
		}
		_, err = shardDB.Exec(`INSERT INTO auction_orders
			(order_id,market_id,owner_id,side,item_config_id,quantity,filled_quantity,price,status,idempotency_key,created_at_ms,updated_at_ms)
			VALUES (5401,43,903,1,24,2,0,70,0,'drift-key',?,?)`, now, now)
		if err != nil {
			t.Fatalf("seed drift market row: %v", err)
		}
		retry := testOrder(5499, 902, SideSell, 24, 2, 70, StatusPending, now+1)
		retry.MarketID = 43
		retry.IdempotencyKey = "drift-key"
		if _, _, err := shardedRepo.ClaimOrder(context.Background(), retry); errcode.As(err) != errcode.ErrInternal {
			t.Fatalf("registry/market drift must fail internal: err=%v code=%d", err, errcode.As(err))
		}
		var ownerID uint64
		if err := shardDB.QueryRow(`SELECT owner_id FROM auction_orders WHERE order_id=5401`).Scan(&ownerID); err != nil || ownerID != 903 {
			t.Fatalf("drift row must stay untouched: owner=%d err=%v", ownerID, err)
		}
	})

	t.Run("ShardTopologyIsPersistedAndExactMatched", func(t *testing.T) {
		if _, err := db.Exec(`DELETE FROM auction_shard_topology`); err != nil {
			t.Fatal(err)
		}
		if _, err := shardDB.Exec(`DELETE FROM auction_shard_topology`); err != nil {
			t.Fatal(err)
		}
		router := twoDBTestRouter{dbs: [2]*sql.DB{db, shardDB}}
		dsns := []string{dbCfg.FormatDSN(), shardCfg.FormatDSN()}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := ValidateShardTopology(ctx, router, ShardTopologyOptions{
			Generation: "auction-v1", DSNs: dsns,
		}); err == nil {
			t.Fatal("first 2-shard start without explicit bootstrap must fail")
		}
		if err := ValidateShardTopology(ctx, router, ShardTopologyOptions{
			Generation: "auction-v1", DSNs: dsns, AllowBootstrap: true,
		}); err != nil {
			t.Fatalf("bootstrap reviewed topology: %v", err)
		}
		if err := ValidateShardTopology(ctx, router, ShardTopologyOptions{
			Generation: "auction-v1", DSNs: dsns,
		}); err != nil {
			t.Fatalf("repeat exact topology: %v", err)
		}

		reversed := twoDBTestRouter{dbs: [2]*sql.DB{shardDB, db}}
		if err := ValidateShardTopology(ctx, reversed, ShardTopologyOptions{
			Generation: "auction-v1", DSNs: []string{dsns[1], dsns[0]}, AllowBootstrap: true,
		}); err == nil {
			t.Fatal("reordered physical shards must fail even when bootstrap remains true")
		}
		if err := ValidateShardTopology(ctx, SingleDB{DB: db}, ShardTopologyOptions{
			Generation: "auction-v1", DSNs: dsns[:1], AllowBootstrap: true,
		}); err == nil {
			t.Fatal("2->1 shard count change must fail")
		}
	})
}

func randomAuctionTestDBName(t *testing.T) string {
	t.Helper()
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("生成随机测试库名: %v", err)
	}
	return fmt.Sprintf("pandora_auction_it_%d_%s", time.Now().UnixMilli(), hex.EncodeToString(b))
}

func readAuctionSchema(t *testing.T, dbName string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("无法定位 auction_repo_mysql_test.go")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "..", "deploy", "mysql-init", "09-auction-tables.sql"))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 auction schema %s: %v", path, err)
	}
	schema := string(b)
	needle := "USE `pandora_auction`;"
	if strings.Count(schema, needle) != 1 {
		t.Fatalf("auction schema USE 锚点数量异常: %d", strings.Count(schema, needle))
	}
	return strings.Replace(schema, needle, "USE `"+dbName+"`;", 1)
}

func truncateAuctionTables(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`DELETE FROM auction_matches; DELETE FROM auction_orders; DELETE FROM auction_idempotency_keys; DELETE FROM auction_owner_guards`); err != nil {
		t.Fatalf("清空 auction 测试表: %v", err)
	}
}

func testOrder(orderID, ownerID uint64, side Side, itemID uint32, quantity, price int64, status int32, now int64) *OrderRecord {
	return &OrderRecord{
		OrderID: orderID, MarketID: 42, OwnerID: ownerID, Side: side, ItemConfigID: itemID,
		Quantity: quantity, Price: price, Status: status, IdempotencyKey: fmt.Sprintf("it-%d", orderID),
		CreatedAtMs: now, UpdatedAtMs: now,
	}
}

func claimOrder(t *testing.T, repo AuctionRepo, o *OrderRecord) {
	t.Helper()
	if _, already, err := repo.ClaimOrder(context.Background(), o); err != nil || already {
		t.Fatalf("ClaimOrder(%d): already=%v err=%v", o.OrderID, already, err)
	}
}
