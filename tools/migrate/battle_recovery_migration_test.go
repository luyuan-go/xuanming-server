package main

import (
	"io/fs"
	"strings"
	"testing"
)

func TestPandoraBattleRecoveryMigrationsStayAdditive(t *testing.T) {
	version, err := latestMigrationVersion("pandora_battle")
	if err != nil {
		t.Fatalf("latestMigrationVersion: %v", err)
	}
	if version != 11 {
		t.Fatalf("pandora_battle latest version=%d, want 11", version)
	}

	v3 := readEmbeddedMigration(t, "migrations/pandora_battle/000003_match_release_outbox.up.sql")
	if strings.Contains(v3, "battle_exit_proof_outbox") {
		t.Fatal("already-versioned 000003 must remain immutable; battle exit proof belongs to 000004")
	}
	v4 := readEmbeddedMigration(t, "migrations/pandora_battle/000004_battle_exit_proof_outbox.up.sql")
	for _, fragment := range []string{
		"CREATE TABLE IF NOT EXISTS `battle_exit_proof_outbox`",
		"UNIQUE KEY `uk_battle_exit_match_player` (`match_id`, `player_id`)",
		"KEY `idx_battle_exit_due` (`superseded_at_ms`, `next_attempt_at_ms`, `id`)",
	} {
		if !strings.Contains(v4, fragment) {
			t.Fatalf("000004 up missing contract fragment %q", fragment)
		}
	}
	down := readEmbeddedMigration(t, "migrations/pandora_battle/000004_battle_exit_proof_outbox.down.sql")
	if !strings.Contains(down, "DROP TABLE IF EXISTS `battle_exit_proof_outbox`") ||
		strings.Contains(down, "match_release_outbox") {
		t.Fatal("000004 down must roll back only battle_exit_proof_outbox")
	}

	// 000005 实时进度通道:水位表须带单场累计上限列,出箱表须带失败退避列
	// (battle_result 启动 schema gate 逐列探测,契约漂移在此拦住)。
	v5 := readEmbeddedMigration(t, "migrations/pandora_battle/000005_battle_progress.up.sql")
	for _, fragment := range []string{
		"CREATE TABLE IF NOT EXISTS `battle_progress_stream`",
		"`total_exp`",
		"`total_items`",
		"CREATE TABLE IF NOT EXISTS `battle_progress_outbox`",
		"`next_attempt_at_ms`",
		"`attempt_count`",
		"UNIQUE KEY `uk_match_seq_player_kind` (`match_id`, `seq`, `player_id`, `kind`)",
		"KEY `idx_progress_due` (`next_attempt_at_ms`, `id`)",
	} {
		if !strings.Contains(v5, fragment) {
			t.Fatalf("000005 up missing contract fragment %q", fragment)
		}
	}
	// 000005 已在共享环境执行过,保持不可变:单玩家累计表属于 000006,不准原地追加。
	if strings.Contains(v5, "battle_progress_player") {
		t.Fatal("already-versioned 000005 must remain immutable; battle_progress_player belongs to 000006")
	}

	// 000006 单玩家累计表(单场单玩家上限权威依据,失陷 DS 不能把全场额度灌给一人)。
	v6 := readEmbeddedMigration(t, "migrations/pandora_battle/000006_battle_progress_player.up.sql")
	for _, fragment := range []string{
		"CREATE TABLE IF NOT EXISTS `battle_progress_player`",
		"`total_exp`",
		"`total_items`",
		"`total_kills`",
		"PRIMARY KEY (`match_id`, `player_id`)",
	} {
		if !strings.Contains(v6, fragment) {
			t.Fatalf("000006 up missing contract fragment %q", fragment)
		}
	}

	// 000007 保留期清理索引(§9.24):存量库条件补齐,清理列必须有索引;
	// down 保持 no-op,回滚不得删掉权威表定义自带的索引。
	v7 := readEmbeddedMigration(t, "migrations/pandora_battle/000007_battle_retention_indexes.up.sql")
	for _, fragment := range []string{
		"information_schema.STATISTICS", // 条件建索引:fresh-init 已建时跳过
		"ADD KEY `idx_created` (`created_at`)",
		"ADD KEY `idx_settled` (`settled_at_ms`)",
		"ALGORITHM=INPLACE",
	} {
		if !strings.Contains(v7, fragment) {
			t.Fatalf("000007 up missing contract fragment %q", fragment)
		}
	}
	v7down := readEmbeddedMigration(t, "migrations/pandora_battle/000007_battle_retention_indexes.down.sql")
	if strings.Contains(v7down, "DROP KEY") || strings.Contains(v7down, "DROP INDEX") {
		t.Fatal("000007 down must stay no-op; dropping retention indexes diverges rolled-back schema from authoritative definition")
	}

	// 000008 停流标记(审计 P1:未知事实停流必须持久化,禁止已知批重新开流):
	// 条件加列 + INSTANT;down no-op(additive 列回滚删列丢停流审计事实)。
	v8 := readEmbeddedMigration(t, "migrations/pandora_battle/000008_battle_progress_stopped.up.sql")
	for _, fragment := range []string{
		"information_schema.COLUMNS",
		"ADD COLUMN `stopped_at_ms`",
		"ALGORITHM=INSTANT",
	} {
		if !strings.Contains(v8, fragment) {
			t.Fatalf("000008 up missing contract fragment %q", fragment)
		}
	}
	v8down := readEmbeddedMigration(t, "migrations/pandora_battle/000008_battle_progress_stopped.down.sql")
	if strings.Contains(strings.ToUpper(v8down), "DROP COLUMN") {
		t.Fatal("000008 down must stay no-op (additive column)")
	}

	// 000009 冻结 drop 路由，并为 phase0 item action 增加本场余额和 durable outcome。
	// 旧行必须按旧契约回填 instance route，不能在迁移时按热配置重新分类。
	v9 := readEmbeddedMigration(t, "migrations/pandora_battle/000009_item_action_outcome_and_drop_route.up.sql")
	for _, fragment := range []string{
		"information_schema.COLUMNS",
		"ADD COLUMN `stack_item_config_ids`",
		"ADD COLUMN `instance_item_config_ids`",
		"ADD COLUMN `item_count`",
		"ALGORITHM=INSTANT",
		"SET `instance_item_config_ids` = `item_config_ids`",
		"CREATE TABLE IF NOT EXISTS `battle_progress_item_balance`",
		"CONSTRAINT `chk_battle_progress_item_balance` CHECK (`spent_count` <= `picked_count`)",
		"CREATE TABLE IF NOT EXISTS `battle_progress_action`",
		"CONSTRAINT `chk_battle_progress_action_status` CHECK (`status` IN (0,1,2))",
	} {
		if !strings.Contains(v9, fragment) {
			t.Fatalf("000009 up missing contract fragment %q", fragment)
		}
	}
	v9down := strings.ToUpper(readEmbeddedMigration(t, "migrations/pandora_battle/000009_item_action_outcome_and_drop_route.down.sql"))
	if strings.Contains(v9down, "DROP TABLE") || strings.Contains(v9down, "DROP COLUMN") {
		t.Fatal("000009 down must stay no-op; dropping durable outcome/balance/routes destroys authority")
	}

	// 000010 任务事实转发出箱(mission.md §5.1)。**必须是独立表**:battle_progress_outbox
	// 按每玩家严格 FIFO 取行(item balance 权威要求 pickup 与 consume/discard 有序),
	// 任务行混入会让 mission 故障卡住队首、连带阻塞该玩家的掉落/经验投递 —— 把弱依赖
	// 变成强依赖。uk 到 (match,seq,player) 为止(每事件唯一,一事实一行);退避列同进度出箱。
	v10 := readEmbeddedMigration(t, "migrations/pandora_battle/000010_battle_mission_outbox.up.sql")
	for _, fragment := range []string{
		"CREATE TABLE IF NOT EXISTS `battle_mission_outbox`",
		"`category`",
		"`slot_value`",
		"`amount`",
		"UNIQUE KEY `uk_match_seq_player` (`match_id`, `seq`, `player_id`)",
		"KEY `idx_mission_due` (`next_attempt_at_ms`, `id`)",
	} {
		if !strings.Contains(v10, fragment) {
			t.Fatalf("000010 up missing contract fragment %q", fragment)
		}
	}
	// 任务出箱不得**改动**既有进度出箱表(混表即破坏上面那条故障隔离契约)。
	// 只查 DDL 不查字符串:迁移头注释正是在解释"为什么不混进 battle_progress_outbox",
	// 那段说明必须留着 —— 按词命中会把解释本身判成违规。
	for _, ddl := range []string{
		"ALTER TABLE `battle_progress_outbox`",
		"DROP TABLE IF EXISTS `battle_progress_outbox`",
		"INSERT INTO `battle_progress_outbox`",
	} {
		if strings.Contains(v10, ddl) {
			t.Fatalf("000010 must not touch battle_progress_outbox (%q); mission facts live in their own table (failure-domain isolation)", ddl)
		}
	}
	v10down := readEmbeddedMigration(t, "migrations/pandora_battle/000010_battle_mission_outbox.down.sql")
	if !strings.Contains(v10down, "DROP TABLE IF EXISTS `battle_mission_outbox`") {
		t.Fatal("000010 down must drop battle_mission_outbox")
	}
	if strings.Contains(v10down, "battle_progress_outbox") {
		t.Fatal("000010 down must roll back only battle_mission_outbox")
	}

	// 000011 战后金币收益(currency-and-shop.md):金币搭**既有**掉落出箱发放,不另起一条
	// 发放链——出箱表的幂等键 / 失败重试 / 投递成功即删 / 容量预算全部免费复用。
	// 因此本迁移只准给 battle_drop_outbox 加一列,新建"金币发放表"就是把同一笔资产
	// 拆成两条投递链(§9.22 唯一权威)。列装的是**入箱时已过服务端上限闸的既成事实**,
	// 与同表 stack_item_config_ids / instance_item_config_ids 同纪律:重试不回头现算,
	// 否则上限热改会让同一场战斗第一次与重试发出不同额度。
	v11 := readEmbeddedMigration(t, "migrations/pandora_battle/000011_battle_gold_grant.up.sql")
	for _, fragment := range []string{
		"information_schema.columns", // 条件加列:fresh-init 已建列时跳过
		"ALTER TABLE `battle_drop_outbox` ADD COLUMN `currency_amount` BIGINT UNSIGNED NOT NULL DEFAULT 0",
	} {
		if !strings.Contains(v11, fragment) {
			t.Fatalf("000011 up missing contract fragment %q", fragment)
		}
	}
	// 金币语义非负(§5.12):列型退回有符号会让"扣成负数"在非严格 sql_mode 下静默落库。
	if strings.Contains(v11, "`currency_amount` BIGINT NOT NULL") {
		t.Fatal("000011 currency_amount must stay BIGINT UNSIGNED; signed column silently accepts negative payouts")
	}
	// 存量出箱行必须留在 0:补发要走运营邮件,迁移里回填等于给升级前的对局追发金币,
	// 同一场战斗的收益口径会前后不一致。
	if strings.Contains(v11, "UPDATE `battle_drop_outbox`") {
		t.Fatal("000011 must not backfill currency_amount for pre-upgrade rows; use ops mail instead")
	}
	v11down := readEmbeddedMigration(t, "migrations/pandora_battle/000011_battle_gold_grant.down.sql")
	if !strings.Contains(v11down, "ALTER TABLE `battle_drop_outbox` DROP COLUMN `currency_amount`") {
		t.Fatal("000011 down must drop currency_amount")
	}
	if strings.Contains(v11down, "DROP TABLE") {
		t.Fatal("000011 down must roll back only the added column")
	}
}

// TestPandoraPlayerExperienceMigrationIsInitSafe 保证 pandora_player 000002 与
// deploy/mysql-init fresh-init 双路兼容:init 已建 exp 列时条件加列必须跳过而不是
// duplicate column 失败(审计 P1)。
func TestPandoraPlayerExperienceMigrationIsInitSafe(t *testing.T) {
	version, err := latestMigrationVersion("pandora_player")
	if err != nil {
		t.Fatalf("latestMigrationVersion: %v", err)
	}
	if version != 8 {
		t.Fatalf("pandora_player latest version=%d, want 8", version)
	}
	v2 := readEmbeddedMigration(t, "migrations/pandora_player/000002_experience.up.sql")
	for _, fragment := range []string{
		"information_schema.COLUMNS", // 条件加列:fresh-init 已建列时跳过
		"ALGORITHM=INSTANT",          // 在线 DDL 显式声明,不能静默退化成锁表拷贝
		"CREATE TABLE IF NOT EXISTS `exp_history`",
		"CREATE TABLE IF NOT EXISTS `player_push_outbox`",
	} {
		if !strings.Contains(v2, fragment) {
			t.Fatalf("000002 up missing contract fragment %q", fragment)
		}
	}
	if !strings.Contains(v2, "PREPARE") {
		t.Fatal("ALTER ADD COLUMN must go through conditional PREPARE/EXECUTE; bare ALTER breaks fresh-init databases")
	}

	// 000003 保留期清理索引(§9.24):四张历史/授予表条件补 idx_created;
	// down 保持 no-op,回滚不得删掉权威表定义(新版 000002 / fresh-init)自带的索引。
	v3 := readEmbeddedMigration(t, "migrations/pandora_player/000003_retention_indexes.up.sql")
	for _, table := range []string{"exp_history", "mmr_history", "attr_point_grants", "talent_point_grants"} {
		if !strings.Contains(v3, "TABLE_NAME = '"+table+"'") {
			t.Fatalf("000003 up missing conditional idx_created for table %q", table)
		}
	}
	if !strings.Contains(v3, "information_schema.STATISTICS") || !strings.Contains(v3, "ALGORITHM=INPLACE") {
		t.Fatal("000003 up must conditionally add indexes online (information_schema probe + ALGORITHM=INPLACE)")
	}
	v3down := readEmbeddedMigration(t, "migrations/pandora_player/000003_retention_indexes.down.sql")
	if strings.Contains(v3down, "DROP KEY") || strings.Contains(v3down, "DROP INDEX") {
		t.Fatal("000003 down must stay no-op; dropping idx_created diverges rolled-back v2 from authoritative v2 definition")
	}

	// 000004 把天赋实际消耗点数落列。fresh-init 已直接建列，因此升级迁移必须
	// 条件加列；存量行按升级前 cost_per_level=1 的契约回填为 level。
	v4 := readEmbeddedMigration(t, "migrations/pandora_player/000004_talent_spent_points.up.sql")
	for _, fragment := range []string{
		"information_schema.COLUMNS",
		"ADD COLUMN `spent_points`",
		"ALGORITHM=INSTANT",
		"UPDATE `player_talents` SET `spent_points` = `level` WHERE `spent_points` = 0",
	} {
		if !strings.Contains(v4, fragment) {
			t.Fatalf("000004 up missing contract fragment %q", fragment)
		}
	}
	v4down := readEmbeddedMigration(t, "migrations/pandora_player/000004_talent_spent_points.down.sql")
	if !strings.Contains(v4down, "DROP COLUMN `spent_points`") {
		t.Fatal("000004 down must drop spent_points")
	}

	// 000005 新增技能卡持有、装配与发放幂等收据三张表。发放收据属于
	// §9.24 只增表，必须随建表带 created_at 清理索引；同卡不得并发占两槽。
	v5 := readEmbeddedMigration(t, "migrations/pandora_player/000005_skill_cards.up.sql")
	for _, fragment := range []string{
		"CREATE TABLE IF NOT EXISTS `player_skill_cards`",
		"UNIQUE KEY `uk_player_card` (`player_id`, `card_id`)",
		"CREATE TABLE IF NOT EXISTS `player_skill_slots`",
		"UNIQUE KEY `uk_player_slot` (`player_id`, `slot`)",
		"UNIQUE KEY `uk_player_card_once` (`player_id`, `card_id`)",
		"CREATE TABLE IF NOT EXISTS `skill_card_grants`",
		"UNIQUE KEY `uk_player_key` (`player_id`, `idempotency_key`)",
		"KEY `idx_created` (`created_at`)",
	} {
		if !strings.Contains(v5, fragment) {
			t.Fatalf("000005 up missing contract fragment %q", fragment)
		}
	}

	// 000006 把配置级装备预设 expand 为精确唯一实例。存量行不可猜测回填，故列必须
	// nullable；新服务写入与 GetLoadout 的 fail-closed 由 player 模块测试守护。
	v6 := readEmbeddedMigration(t, "migrations/pandora_player/000006_equipment_instance_id.up.sql")
	for _, fragment := range []string{
		"information_schema.COLUMNS",
		"ADD COLUMN `instance_id` BIGINT UNSIGNED NULL",
		"ALGORITHM=INSTANT",
		"information_schema.STATISTICS",
		"ADD UNIQUE KEY `uk_player_instance` (`player_id`, `instance_id`)",
		"ALGORITHM=INPLACE",
	} {
		if !strings.Contains(v6, fragment) {
			t.Fatalf("000006 up missing contract fragment %q", fragment)
		}
	}
	v6down := readEmbeddedMigration(t, "migrations/pandora_player/000006_equipment_instance_id.down.sql")
	if !strings.Contains(v6down, "DROP INDEX `uk_player_instance`") ||
		!strings.Contains(v6down, "DROP COLUMN `instance_id`") {
		t.Fatal("000006 down must remove only its unique index and instance_id column")
	}
}

// TestRetentionIndexDownsStayNoOp 横扫全部 *_retention_indexes / 保留期索引迁移的 down:
// 一律 no-op(清理索引属权威表定义,fresh-init 自带;回滚删索引会让"fresh 建表 + 回滚"
// 的库与权威定义不一致,2026-07-22 审计 P1)。新增库照此纪律,含 DROP 即 FAIL。
func TestRetentionIndexDownsStayNoOp(t *testing.T) {
	found := 0
	dbs, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		t.Fatalf("read migrations root: %v", err)
	}
	for _, db := range dbs {
		if !db.IsDir() {
			continue
		}
		files, derr := fs.ReadDir(migrationsFS, "migrations/"+db.Name())
		if derr != nil {
			t.Fatalf("read %s: %v", db.Name(), derr)
		}
		for _, f := range files {
			if !strings.Contains(f.Name(), "retention_indexes") || !strings.HasSuffix(f.Name(), ".down.sql") {
				continue
			}
			found++
			down := readEmbeddedMigration(t, "migrations/"+db.Name()+"/"+f.Name())
			upper := strings.ToUpper(down)
			if strings.Contains(upper, "DROP KEY") || strings.Contains(upper, "DROP INDEX") {
				t.Fatalf("%s/%s must stay no-op: dropping retention indexes diverges rolled-back schema from authoritative definition", db.Name(), f.Name())
			}
		}
	}
	if found < 7 {
		t.Fatalf("expected >=7 retention_indexes down migrations, found %d (sweep glob broken?)", found)
	}
}
