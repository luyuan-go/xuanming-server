// dbcheck — 数据库无界增长检查工具(CLAUDE.md §9 不变量 24,2026-07-22)。
//
// 用途:
//  1. 上线前检查:枚举 MySQL 上全部 pandora_% 库的全部表,对照内嵌登记清单
//     (与 CLAUDE.md §9.24 只增表清单同步维护)——清单外的表、swept 表缺清理索引 → 退出码 1。
//  2. 压测断言:压前 -snapshot before.json,压后 -snapshot after.json -compare before.json,
//     断言 outbox 类表已排空(≤ -outbox-max)、无未登记表;各表增量打印供"增长 = 业务量可解释"核对。
//  3. 待清理量报告:-pending 对各 swept 表按 cutoff=now 统计"最坏情况下有多少行可清",
//     供容量评估与"是否该开删"的决策。
//
// # 本工具永不删除任何数据(2026-07-22)
//
// 早先版本有 -force-sweep -confirm=YES-DELETE 的"清理压测"能力(cutoff=now 批删到空、
// 输出 rows/s)。按用户指令「不能清理我的数据,只能打印日志」**已整块移除**,且**刻意不
// 保留同名 flag**:留着改语义会让按旧文档敲的命令静默变行为,现在旧命令直接报未定义 flag。
// 真删只由各服务配置 `retention_mode: delete` 决定(默认 report_only 只告警)。
//
// 用法:
//
//	go run ./cmd/dbcheck -dsn "root:pwd@tcp(127.0.0.1:3307)/" [-exact] [-snapshot out.json]
//	    [-compare before.json] [-outbox-max 200] [-size-check [-top-rows N]] [-pending]
//
// 登记清单口径:新增任何表必须先在 CLAUDE.md §9.24 登记再同步到本清单;
// 本工具报"未登记表"即 PR review 拒绝口径的机械化(AGENTS.md §10 红线)。
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// tableClass 是表的增长类别(与 CLAUDE.md §9.24 分类一致)。
type tableClass string

const (
	classBounded tableClass = "bounded" // 按玩家/配置有界(行数 ∝ 玩家数×常数)
	classSwept   tableClass = "swept"   // 只增表,有保留期清理任务
	classOutbox  tableClass = "outbox"  // 事务出箱:投递成功即删,稳态应接近空
	classExempt  tableClass = "exempt"  // 登记豁免(慢增长/权威闸/运营审计)
)

// indexSpec 是清理路径必需索引的名字 + 前导列(按顺序)。
// 校验列名与顺序而不只是索引名(审计 P1:同名错列的索引会让门禁误报健康);
// spec 列必须是实际索引的**前缀**(实际索引多出的尾列不影响清理扫描路径)。
type indexSpec struct {
	Name    string
	Columns []string
}

// tableEntry 是登记清单一项。
type tableEntry struct {
	Class tableClass
	// RequiredIndexes 是 swept 表清理路径必需的索引(名字 + 前导列核对)。
	RequiredIndexes []indexSpec
	// PendingWhere 是"满足清理条件"的 WHERE 片段(不含 WHERE 关键字,cutoff=now 即"一切都算超期",
	// 用于 -pending 报告各表最坏待清理规模)。**本工具只 COUNT,永不 DELETE**
	// (2026-07-22 用户指令:不能因为数据大了就删数据;真删由各服务 retention_mode=delete 决定)。
	// 空 = 条件复杂/多表事务,由各服务 sweep 自己报告(见服务日志 db_retention_pending_not_deleted)。
	PendingWhere string
}

// registry 是全库表登记清单(与 CLAUDE.md §9.24 同步;新表未登记 → 检查失败)。
var registry = map[string]map[string]tableEntry{
	"pandora_account": {
		"accounts":                   {Class: classBounded},
		"account_roles":              {Class: classBounded}, // 角色归属台账,每角色 1 行;被「账号数 × 每账号角色上限」有界
		"player_roles":               {Class: classBounded},
		"player_session_generations": {Class: classBounded}, // 每玩家 1 行(登录定序+SetRole fencing 权威),被玩家数有界
		"account_devices":            {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_last_login", Columns: []string{"last_login_at"}}}, PendingWhere: "last_login_at < DATE_SUB(NOW(), INTERVAL 0 DAY)"},
		"account_bans":               {Class: classExempt}, // 运营合规审计,量级 = 运营操作数
		"player_no_counter":          {Class: classExempt}, // 角色编号全局发号计数器,恒 1 行(发号权威闸;player-no-and-login-surge.md §3.3)
		// 同一个发号权威闸的旧名副本:000006 曾把它 RENAME 掉,破坏了滚动升级期
		// Stable/Canary 共存,000007 expand 重新建回并与 player_no_counter 双锁双写。
		// 恒 1 行,contract 迁移(所有旧 login 二进制退场后)之前不得删除。
		"register_no_counter": {Class: classExempt},
	},
	"pandora_player": {
		"players":              {Class: classBounded},
		"player_data":          {Class: classBounded}, // data_service proto2mysql 自动建表,per-player
		"player_heroes":        {Class: classBounded},
		"player_attributes":    {Class: classBounded},
		"player_equipment":     {Class: classBounded},
		"player_talents":       {Class: classBounded},
		"player_skill_cards":   {Class: classBounded}, // 每玩家每卡至多 1 行,被技能卡表行数有界
		"player_skill_slots":   {Class: classBounded}, // 每玩家至多 SkillSlotCount(4) 行
		"player_reward_claims": {Class: classBounded},
		// 段位分唯一权威:每玩家每池至多 1 行,池数被关卡表行数有界(§9.24 登记豁免)。
		"player_mmr":          {Class: classBounded},
		"player_push_outbox":  {Class: classOutbox},
		"mmr_history":         {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_created", Columns: []string{"created_at"}}}, PendingWhere: "created_at < NOW()"},
		"exp_history":         {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_created", Columns: []string{"created_at"}}}, PendingWhere: "created_at < NOW()"},
		"attr_point_grants":   {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_created", Columns: []string{"created_at"}}}, PendingWhere: "created_at < NOW()"},
		"talent_point_grants": {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_created", Columns: []string{"created_at"}}}, PendingWhere: "created_at < NOW()"},
		"skill_card_grants":   {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_created", Columns: []string{"created_at"}}}, PendingWhere: "created_at < NOW()"},
	},
	"pandora_battle": {
		"battles":                      {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_created", Columns: []string{"created_at"}}}},                // 清理按服务端 created_at(§9.6 不信 DS ended_at_ms);与 stats 同事务批删,不支持工具直删
		"battle_player_stats":          {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "uk_match_player", Columns: []string{"match_id", "player_id"}}}}, // 随 battles 批删(按 match_id 前缀)
		"battle_progress_stream":       {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_settled", Columns: []string{"settled_at_ms"}}}},
		"battle_progress_player":       {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "PRIMARY", Columns: []string{"match_id", "player_id"}}}},                   // 随 stream 批删(PK 前缀 match_id)
		"battle_progress_item_balance": {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "PRIMARY", Columns: []string{"match_id", "player_id", "item_config_id"}}}}, // 随 stream 按 match_id 成组批删
		"battle_progress_action":       {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "PRIMARY", Columns: []string{"match_id", "seq", "player_id", "kind"}}}},    // 随 stream 按 match_id 成组批删
		"player_update_outbox":         {Class: classOutbox},
		"battle_drop_outbox":           {Class: classOutbox},
		"terminal_release_outbox":      {Class: classOutbox},
		"match_release_outbox":         {Class: classOutbox},
		"battle_progress_outbox":       {Class: classOutbox},
		"battle_exit_proof_outbox":     {Class: classOutbox},
		"battle_mission_outbox":        {Class: classOutbox}, // 任务事实转发出箱:投递成功即删,稳态接近空
	},
	"pandora_social": {
		"friendships":           {Class: classBounded},
		"blocks":                {Class: classBounded},
		"friend_player_guards":  {Class: classExempt},                                                                                                                                                    // 每玩家一行写守卫(R5 P1-2,TiDB 无 gap 锁;被玩家数有界,§9.24 豁免)
		"friend_pair_guards":    {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_created", Columns: []string{"created_at"}}}, PendingWhere: "created_at < DATE_SUB(NOW(), INTERVAL 0 DAY)"}, // 每关系对一行写守卫,随社交图 O(n²) 累积 → 保留期 sweep(R9 P1;守卫行仅锁载体,删除安全)
		"friend_requests":       {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_status_updated", Columns: []string{"status", "updated_at"}}}, PendingWhere: "status <> 1 AND updated_at < DATE_SUB(NOW(), INTERVAL 0 DAY)"},
		"chat_private_messages": {Class: classSwept, PendingWhere: "message_id < 18446744073709551615"}, // 雪花 PK 范围删,无需时间索引
		"guilds":                {Class: classBounded},
		"guild_members":         {Class: classBounded},
		"guild_join_requests":   {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_status_updated", Columns: []string{"status", "updated_at"}}}, PendingWhere: "status <> 1 AND updated_at < DATE_SUB(NOW(), INTERVAL 0 DAY)"},
		"chat_groups":           {Class: classBounded},
		"chat_group_members":    {Class: classBounded},
		"player_group_counts":   {Class: classBounded},
		"sys_mail":              {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_end", Columns: []string{"end_ms"}}}, PendingWhere: "end_ms > 0 AND end_ms <= UNIX_TIMESTAMP(NOW(3))*1000"},
		"guild_mail":            {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_end", Columns: []string{"end_ms"}}}, PendingWhere: "end_ms > 0 AND end_ms <= UNIX_TIMESTAMP(NOW(3))*1000"},
		"player_mail":           {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_expire", Columns: []string{"expire_ms"}}}}, // 归档分流(未领附件先归档),不支持工具直删
		"player_mail_cursor":    {Class: classBounded},
		"player_mail_claim":     {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_mail", Columns: []string{"mail_id"}}}, PendingWhere: "mail_id < 18446744073709551615"},
		"player_mail_archive":   {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_archived", Columns: []string{"archived_at"}}}, PendingWhere: "archived_at < NOW()"},
	},
	"pandora_trade": {
		"player_wallet":        {Class: classBounded}, // 多币种钱包:每玩家每币种一行,被玩家数×币种枚举有界
		"player_currency":      {Class: classBounded}, // legacy 单币种 gold 表:expand 期仍在(000005 只搬数据不删),contract 时连同本行一起删
		"player_items":         {Class: classBounded}, // count=0 行被 uk(player,item) 有界,故意不清(§9.24 豁免注记)
		"player_item_instance": {Class: classBounded}, // 容量上限×玩家数;丢弃硬删
		"mail_transfer_escrow": {Class: classBounded}, // 在途托管行,领取/释放即删;量级=在途 transfer 邮件数(§9.24 豁免注记)
		"inventory_ledger":     {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_created", Columns: []string{"created_at"}}}, PendingWhere: "created_at < DATE_SUB(NOW(), INTERVAL 0 DAY)"},
		"auction_escrow":       {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_status_updated", Columns: []string{"status", "updated_at"}}}, PendingWhere: "status = 2 AND updated_at < DATE_SUB(NOW(), INTERVAL 0 DAY)"},
	},
	"pandora_auction": {
		"auction_orders":           {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_terminal_purge", Columns: []string{"status", "updated_at_ms"}}}, PendingWhere: "status IN (3,4,5) AND release_pending = 0 AND match_pending = 0 AND updated_at_ms < UNIX_TIMESTAMP(NOW(3))*1000"},
		"auction_matches":          {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_settled_purge", Columns: []string{"settlement_status", "event_pending", "matched_at_ms"}}}, PendingWhere: "settlement_status = 1 AND event_pending = 0 AND matched_at_ms < UNIX_TIMESTAMP(NOW(3))*1000"},
		"auction_idempotency_keys": {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_created", Columns: []string{"created_at_ms"}}}, PendingWhere: "created_at_ms < UNIX_TIMESTAMP(NOW(3))*1000"},
		"auction_owner_guards":     {Class: classExempt}, // 每 owner 一行,被玩家数有界
		"auction_shard_topology":   {Class: classBounded},
	},
	"pandora_leaderboard": {
		"leaderboard_settlement": {Class: classExempt}, // settle uk 防重复结算永久闸,每批次 1 行
		"leaderboard_snapshot":   {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_created", Columns: []string{"created_at_ms"}}}, PendingWhere: "created_at_ms < UNIX_TIMESTAMP(NOW(3))*1000"},
		"leaderboard_reward_log": {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_status_updated", Columns: []string{"status", "updated_at_ms"}}}, PendingWhere: "status = 1 AND updated_at_ms < UNIX_TIMESTAMP(NOW(3))*1000"},
	},
	"pandora_bag": {
		"bag_meta":       {Class: classBounded},
		"bag_checkpoint": {Class: classBounded},
		"bag_section":    {Class: classBounded},
		"bag_generation": {Class: classBounded},
		"bag_journal":    {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_created_at", Columns: []string{"created_at"}}}}, // checkpoint 覆盖位条件,由服务 sweep 负责
		"bag_migration":  {Class: classExempt},                                                                                         // 存量迁移幂等闸,一玩家一行永久保留(D5)
		"bag_capacity":   {Class: classBounded},                                                                                        // 已购容量增量,每玩家×可买段一行(§5.3)
	},
	"pandora_owner": {
		"owner_record":         {Class: classBounded},                                                                                                                            // 每玩家一行(§9.22 owner 权威)
		"ds_instance_lease":    {Class: classBounded},                                                                                                                            // 每 DS 实例一行
		"owner_transition_log": {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_created_at", Columns: []string{"created_at"}}}, PendingWhere: "created_at < NOW()"}, // 迁移审计流水,90 天(owner 线负责 sweep 落地)
	},
	"pandora_mission": {
		"player_mission_active": {Class: classBounded},                                                                                                                                                                                     // 每玩家 ≤ max_active_missions(50) 行(§9.18 写入侧上限)
		"player_mission_done":   {Class: classBounded},                                                                                                                                                                                     // 每玩家每任务至多 1 行,被任务表行数有界
		"mission_reward_log":    {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_status_updated", Columns: []string{"status", "updated_at_ms"}}}, PendingWhere: "status = 1 AND updated_at_ms < UNIX_TIMESTAMP(NOW(3))*1000"}, // GRANTED 90 天清;PENDING/FAILED 是补发工作集永不清(对齐 leaderboard_reward_log)
		"mission_fact_receipts": {Class: classSwept, RequiredIndexes: []indexSpec{{Name: "idx_created", Columns: []string{"created_at"}}}, PendingWhere: "created_at < NOW()"},                                                             // 清理默认关:上游 battle_progress_outbox 重试无总期限,删收据会双计(同 exp_history)
		"mission_push_outbox":   {Class: classOutbox},                                                                                                                                                                                      // 推送事务出箱:投递成功即删,稳态应接近空
		"mission_player_guards": {Class: classExempt},                                                                                                                                                                                      // 每玩家一行写守卫(TiDB 无 gap 锁;被玩家数有界,§9.24 豁免,同 friend_player_guards)
	},
	// 预留库:当前应无业务表;一旦出现新表必须先登记。
	"pandora_ops": {},
}

// ignoredTables 是所有库都允许存在的框架元表。
var ignoredTables = map[string]bool{"schema_migrations": true}

type snapshot map[string]int64 // "db.table" → rows

func main() {
	dsn := flag.String("dsn", "", "不带 database 的 MySQL DSN,如 root:pwd@tcp(127.0.0.1:3307)/")
	exact := flag.Bool("exact", false, "用 COUNT(*) 精确行数(默认 information_schema 估算;压测对比建议开)")
	snapshotOut := flag.String("snapshot", "", "把各表行数快照写入该 JSON 文件")
	compareWith := flag.String("compare", "", "与该 JSON 快照对比并输出增量")
	outboxMax := flag.Int64("outbox-max", 200, "outbox 类表允许的残留行数上限(压后断言;在途少量属正常)")
	// -force-sweep / -confirm / -batch 已于 2026-07-22 **移除**(用户指令:不能因为数据大了就删数据)。
	// 刻意不保留同名 flag:留着改语义会让按旧文档敲的命令静默变行为;现在旧命令直接报未定义 flag。
	pending := flag.Bool("pending", false, "报告各 swept 表在 cutoff=now 下的待清理行数(只 COUNT,不删任何数据)")
	sizeCheck := flag.Bool("size-check", false, "大字段体检:对登记的 blob/JSON 列跑 MAX/AVG LENGTH 与超限行数(全表扫描,勿高频)")
	topRows := flag.Int("top-rows", 0, "配合 -size-check:对超限字段列出前 N 行(主键+字节数),定位到具体玩家/记录")
	flag.Parse()

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "用法: dbcheck -dsn 'user:pwd@tcp(host:port)/' [flags];见文件头注释")
		os.Exit(2)
	}

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		fatal("open mysql: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		fatal("ping mysql: %v", err)
	}

	violations := 0

	// 1. 枚举全部 pandora_% 库的表,对照登记清单。
	actual, err := listTables(ctx, db)
	if err != nil {
		fatal("list tables: %v", err)
	}
	for dbName, tables := range actual {
		reg, dbKnown := registry[dbName]
		if !dbKnown {
			fmt.Printf("[FAIL] 未登记的库: %s(先在 CLAUDE.md §9.24 登记,再同步 dbcheck 清单)\n", dbName)
			violations++
			continue
		}
		for _, t := range tables {
			if ignoredTables[t] {
				continue
			}
			if _, ok := reg[t]; !ok {
				fmt.Printf("[FAIL] 未登记的表: %s.%s(只增表必须有保留期+清理任务并登记 §9.24)\n", dbName, t)
				violations++
			}
		}
	}

	// 1.5 登记库/表必须**真实存在**(审计 P1:只校验可见对象时,空库、连错实例或
	// 权限不全都会"零违规"误报 PASS——门禁检查不到东西 ≠ 东西没问题)。
	// 全部环境统一由 mysql-init fresh-init / migrate 建齐登记表,缺失即部署残缺。
	for dbName, reg := range registry {
		if len(reg) == 0 {
			continue // 预留空库(pandora_ops)不强制存在
		}
		present, dbExists := actual[dbName]
		if !dbExists {
			fmt.Printf("[FAIL] 登记库缺失: %s(连错实例 / 初始化未跑 / 权限不可见)\n", dbName)
			violations++
			continue
		}
		presentSet := toSet(present)
		for t := range reg {
			if !presentSet[t] {
				fmt.Printf("[FAIL] 登记表缺失: %s.%s(初始化/迁移未跑或权限不可见)\n", dbName, t)
				violations++
			}
		}
	}

	// 2. swept 表清理索引核对(表存在才查;缺表已由 1.5 记违规)。
	for dbName, reg := range registry {
		present := toSet(actual[dbName])
		for t, e := range reg {
			if e.Class != classSwept || len(e.RequiredIndexes) == 0 || !present[t] {
				continue
			}
			for _, idx := range e.RequiredIndexes {
				ok, detail, ierr := indexMatches(ctx, db, dbName, t, idx)
				if ierr != nil {
					fatal("probe index %s.%s.%s: %v", dbName, t, idx.Name, ierr)
				}
				if !ok {
					fmt.Printf("[FAIL] %s.%s 清理索引 %s 缺失或列不匹配(%s;要求前导列 %v;跑 tools/migrate 对应 *_retention_indexes)\n",
						dbName, t, idx.Name, detail, idx.Columns)
					violations++
				}
			}
		}
	}

	// 3. 行数快照。outbox 类表**强制精确 COUNT**(审计 P1:InnoDB TABLE_ROWS 是估算值,
	// 统计滞后/低估时真实积压超阈值仍可能 PASS;outbox 稳态接近空,精确计数代价可忽略)。
	// 其余大表默认估算(-exact 全表精确,压测对比建议开)。
	snap := snapshot{}
	for dbName, tables := range actual {
		for _, t := range tables {
			if ignoredTables[t] {
				continue
			}
			useExact := *exact
			if e, ok := registry[dbName][t]; ok && e.Class == classOutbox {
				useExact = true
			}
			n, cerr := rowCount(ctx, db, dbName, t, useExact)
			if cerr != nil {
				fatal("count %s.%s: %v", dbName, t, cerr)
			}
			snap[dbName+"."+t] = n
		}
	}
	if *snapshotOut != "" {
		b, _ := json.MarshalIndent(snap, "", "  ")
		if werr := os.WriteFile(*snapshotOut, b, 0o644); werr != nil {
			fatal("write snapshot: %v", werr)
		}
		fmt.Printf("[OK] 行数快照已写入 %s(%d 表)\n", *snapshotOut, len(snap))
	}

	// 4. 快照对比(压测前后):打印增量;outbox 残留断言。
	if *compareWith != "" {
		before := snapshot{}
		b, rerr := os.ReadFile(*compareWith)
		if rerr != nil {
			fatal("read compare snapshot: %v", rerr)
		}
		if jerr := json.Unmarshal(b, &before); jerr != nil {
			fatal("parse compare snapshot: %v", jerr)
		}
		keys := make([]string, 0, len(snap))
		for k := range snap {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Println("── 压测前后行数增量(核对:增长必须能用业务量解释)──")
		for _, k := range keys {
			delta := snap[k] - before[k]
			if delta != 0 {
				fmt.Printf("  %-50s %+d(%d → %d)\n", k, delta, before[k], snap[k])
			}
		}
	}

	// 5. outbox 残留断言(有无 -compare 都执行:上线前 / 压后 outbox 都不应堆积)。
	for dbName, reg := range registry {
		present := toSet(actual[dbName])
		for t, e := range reg {
			if e.Class != classOutbox || !present[t] {
				continue
			}
			if n := snap[dbName+"."+t]; n > *outboxMax {
				fmt.Printf("[FAIL] outbox 未排空: %s.%s = %d 行(> %d;投递链堵塞或下游长期不可用)\n", dbName, t, n, *outboxMax)
				violations++
			}
		}
	}

	// 5.5 大字段体检(-size-check):单行/单元素"变胖"检查,与行数检查是两个独立失控方向。
	// 全表扫描 MAX/AVG LENGTH,成本高 → 仅显式开启时跑(上线前一次 / 告警后排查 / 压测收尾)。
	if *sizeCheck {
		present := map[string]map[string]bool{}
		for dbName, tables := range actual {
			present[dbName] = toSet(tables)
		}
		fmt.Println("\n── 大字段体检(按 max/预算 比值降序;AVG 涨=全体普遍变胖=设计问题,MAX 涨=个别行畸形)──")
		for _, s := range checkBigFields(ctx, db, present) {
			target := s.budget.DB + "." + s.budget.Table + "." + s.budget.Column
			if s.err != nil {
				fmt.Printf("  [WARN] %-52s 体检失败: %v\n", target, s.err)
				continue
			}
			ratio := float64(s.maxBytes) / float64(max64(s.budget.MaxBytes, 1))
			mark := "  ok  "
			if s.overRows > 0 {
				mark = " FAIL "
				violations++
			} else if ratio >= 0.8 {
				mark = " WARN " // 逼近预算:还没坏,但这是排查成本最低的时点
			}
			fmt.Printf("  [%s] %-52s rows=%-9d max=%-9d avg=%-7d 预算=%-8d 超限行=%d\n",
				mark, target, s.rows, s.maxBytes, s.avgBytes, s.budget.MaxBytes, s.overRows)
			if s.overRows > 0 || ratio >= 0.8 {
				fmt.Printf("         ↳ %s\n", s.budget.Why)
			}
			// 定位到具体行:拿到主键后 dump 反序列化,才能知道是"哪个字段"爆了。
			if *topRows > 0 && (s.overRows > 0 || ratio >= 0.8) {
				top, terr := topLargeRows(ctx, db, s.budget, *topRows)
				if terr != nil {
					fmt.Printf("         ↳ [WARN] Top-N 定位失败: %v\n", terr)
				}
				for _, r := range top {
					fmt.Printf("         ↳ %s=%s  %d 字节\n", s.budget.PK, r.PK, r.Bytes)
				}
			}
		}
	}

	// 6. 待清理量报告(-pending):对登记了 PendingWhere 的表跑 COUNT,给出 cutoff=now 下
	//    的最坏待清理规模。**本工具只 COUNT,永不 DELETE**(2026-07-22 用户指令:不能因为
	//    数据大了就删数据)。真删只由各服务配置 retention_mode=delete 决定,服务侧另有
	//    持续 WARN 告警(db_retention_pending_not_deleted)与 pending gauge。
	if *pending {
		fmt.Println("\n── 待清理量报告(cutoff=now 上界;本工具不删任何数据)──")
		dbNames := make([]string, 0, len(registry))
		for dbName := range registry {
			dbNames = append(dbNames, dbName)
		}
		sort.Strings(dbNames)
		for _, dbName := range dbNames {
			reg := registry[dbName]
			present := toSet(actual[dbName])
			names := make([]string, 0, len(reg))
			for t := range reg {
				names = append(names, t)
			}
			sort.Strings(names)
			for _, t := range names {
				e := reg[t]
				if e.Class != classSwept || e.PendingWhere == "" || !present[t] {
					continue
				}
				var n int64
				q := "SELECT COUNT(*) FROM `" + dbName + "`.`" + t + "` WHERE " + e.PendingWhere
				if qerr := db.QueryRowContext(ctx, q).Scan(&n); qerr != nil {
					fmt.Printf("  [WARN] %-46s 统计失败: %v\n", dbName+"."+t, qerr)
					continue
				}
				fmt.Printf("  %-46s 可清理 %10d 行\n", dbName+"."+t, n)
			}
		}
		fmt.Println("  说明:以上是 cutoff=now 的**上界**(把一切都当超期);真实待清理量按各服务配置的")
		fmt.Println("       retention_days 计算,见服务日志 db_retention_pending_not_deleted 与")
		fmt.Println("       metric pandora_db_retention_pending_rows。")
	}

	if violations > 0 {
		fmt.Printf("\n[RESULT] FAIL:%d 项违规(未登记表 / 缺清理索引 / outbox 堆积)\n", violations)
		os.Exit(1)
	}
	fmt.Println("\n[RESULT] PASS:全部表已登记,清理索引齐备,outbox 无堆积")
}

func listTables(ctx context.Context, db *sql.DB) (map[string][]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES
		 WHERE TABLE_SCHEMA LIKE 'pandora\_%' AND TABLE_TYPE = 'BASE TABLE'
		 ORDER BY TABLE_SCHEMA, TABLE_NAME`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string][]string{}
	for rows.Next() {
		var d, t string
		if err := rows.Scan(&d, &t); err != nil {
			return nil, err
		}
		out[d] = append(out[d], t)
	}
	return out, rows.Err()
}

// indexMatches 校验索引存在且**前导列名与顺序**与 spec 一致(审计 P1:只查索引名
// 会让同名错列的索引通过门禁;spec 列必须是实际索引列序列的前缀,多出的尾列不影响
// 清理扫描路径)。返回 (匹配, 不匹配时的实际列描述, err)。
func indexMatches(ctx context.Context, db *sql.DB, dbName, table string, spec indexSpec) (bool, string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT COLUMN_NAME FROM information_schema.STATISTICS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ?
		 ORDER BY SEQ_IN_INDEX`, dbName, table, spec.Name)
	if err != nil {
		return false, "", err
	}
	defer func() { _ = rows.Close() }()
	var cols []string
	for rows.Next() {
		var c string
		if serr := rows.Scan(&c); serr != nil {
			return false, "", serr
		}
		cols = append(cols, c)
	}
	if rerr := rows.Err(); rerr != nil {
		return false, "", rerr
	}
	if len(cols) == 0 {
		return false, "索引不存在", nil
	}
	if len(cols) < len(spec.Columns) {
		return false, fmt.Sprintf("实际列 %v", cols), nil
	}
	for i, want := range spec.Columns {
		if cols[i] != want {
			return false, fmt.Sprintf("实际列 %v", cols), nil
		}
	}
	return true, "", nil
}

func rowCount(ctx context.Context, db *sql.DB, dbName, table string, exact bool) (int64, error) {
	var n int64
	if exact {
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+dbName+"`.`"+table+"`").Scan(&n)
		return n, err
	}
	err := db.QueryRowContext(ctx,
		`SELECT COALESCE(TABLE_ROWS, 0) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
		dbName, table).Scan(&n)
	return n, err
}

func toSet(list []string) map[string]bool {
	out := make(map[string]bool, len(list))
	for _, s := range list {
		out[s] = true
	}
	return out
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(2)
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
