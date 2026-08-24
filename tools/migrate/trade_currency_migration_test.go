package main

import (
	"strings"
	"testing"
)

const (
	tradeCurrencyUpPath   = "migrations/pandora_trade/000005_multi_currency_wallet.up.sql"
	tradeCurrencyDownPath = "migrations/pandora_trade/000005_multi_currency_wallet.down.sql"
)

// TestPandoraTradeMultiCurrencyMigrationStaysAdditive 固化 000005(单币种 gold → 多币种钱包)
// 的几条要害,并承担 pandora_trade 的**最新版本号钉子**(每套迁移集只留一处,钉在最新那条
// 迁移的契约测试里;加 000006 时这里会红,提醒把新迁移的契约补进来再挪钉子)。
//
// 钉住的不是"SQL 长这样",而是几条改了就出事、但改完仍然全绿的决定:
//
//	① 新建 player_wallet 而不是原地改 player_currency 的主键 —— TiDB 不支持 DROP 聚簇主键,
//	   走 RENAME 舞在中途被杀会留下半迁移状态。
//	② 存量搬迁必须 INSERT IGNORE + GREATEST(gold,0):覆盖写会把迁移后产生的新余额抹回旧值;
//	   负数落进 UNSIGNED 列在非严格 sql_mode 下被静默截断(§9.24 实测)。
//	③ expand 阶段不准删 legacy 表 —— contract 留给后续迁移,等确认没有副本再读旧表。
func TestPandoraTradeMultiCurrencyMigrationStaysAdditive(t *testing.T) {
	version, err := latestMigrationVersion("pandora_trade")
	if err != nil {
		t.Fatalf("latestMigrationVersion: %v", err)
	}
	if version != 5 {
		t.Fatalf("pandora_trade latest version=%d, 期望=5", version)
	}

	up := readEmbeddedMigration(t, tradeCurrencyUpPath)
	// 折叠空白后比对:建表脚本按列对齐排版,原文里的空格数是排版结果,不是契约。
	upFlat := collapseSpaces(up)
	for _, fragment := range []string{
		// ① 多币种终态表:一玩家一币种一行,"加一种货币" = 多写一个 kind 值,不加列不加表。
		"CREATE TABLE IF NOT EXISTS `player_wallet`",
		"`currency_kind` INT NOT NULL COMMENT",
		"PRIMARY KEY (`player_id`, `currency_kind`)",
		// 非负由列类型保证(§5.12);退回有符号等于把"扣款失败"变成可落库的负余额。
		"`amount` BIGINT UNSIGNED NOT NULL DEFAULT 0",
		// ② 存量金币搬迁:不覆盖、不搬负数、0 余额与"没有行"语义等价。
		"INSERT IGNORE INTO `player_wallet`",
		"GREATEST(`gold`, 0)",
		"WHERE `gold` > 0",
		// ③ 幂等结果快照 + 本次变动额。两列都是 pb,重放要原样回放首次执行的结果与增量。
		"ALTER TABLE `inventory_ledger` ADD COLUMN `result_currencies` VARBINARY(256) NULL",
		"ALTER TABLE `inventory_ledger` ADD COLUMN `result_currency_delta` VARBINARY(256) NULL",
		// ④ 买单托管从"只能冻金币"变成"冻某一种货币"。
		"CHANGE COLUMN `frozen_gold` `frozen_amount` BIGINT NOT NULL DEFAULT 0",
		"ALTER TABLE `auction_escrow` ADD COLUMN `currency_kind` INT NOT NULL DEFAULT 1",
	} {
		if !strings.Contains(upFlat, fragment) {
			t.Errorf("000005 up 缺少契约片段 %q", fragment)
		}
	}

	// 三处 ADD COLUMN 各自现查"列不存在"。复用上一处的判断结果,会让"两条 ALTER 之间被杀"
	// 留下的半迁移状态在重跑时无法自愈(同 000004 顶部注释)。
	if got := strings.Count(up, "SELECT COUNT(*) = 0"); got != 3 {
		t.Errorf("000005 up 的三处 ADD COLUMN 必须各自现查『列不存在』,实得 %d 处", got)
	}

	// result_gold 是存量行的重放依据:pb 二进制无法用 SQL 从整数转换,存量行没法就地转换,
	// 删列等于让升级前写下的流水永远重放不出首次结果(§9.7 幂等)。
	// contract 删列要等 ledger_retention_days 把存量行清完,不在本迁移。
	if strings.Contains(upFlat, "DROP COLUMN `result_gold`") {
		t.Error("000005 不得删 result_gold:存量行的幂等重放还要读它,contract 留给保留期清完后的迁移")
	}
	// expand 阶段不准删 legacy 表。只查 DDL 不查字符串:迁移头注释正是在解释
	// "contract(DROP TABLE player_currency)留给后续迁移",按词命中会把解释本身判成违规。
	for _, ddl := range []string{
		"DROP TABLE `player_currency`",
		"DROP TABLE IF EXISTS `player_currency`",
	} {
		if strings.Contains(upFlat, ddl) {
			t.Errorf("000005 是 expand 阶段,不得删 legacy 表(%q);contract 留给确认无副本读旧表之后", ddl)
		}
	}
	// UNSIGNED 列上的负结果:严格模式抛 1690、非严格模式静默截断成 0 —— 后者等于把
	// "扣款失败"变成"余额清零"。扣减一律先锁行读出、在 Go 里比较、再写绝对值,
	// SQL 里不出现 amount - ?(含本迁移与今后对它的任何改动)。
	if strings.Contains(upFlat, "`amount` - ") {
		t.Error("000005 不得在 SQL 里对 UNSIGNED 的 amount 做减法:负结果在非严格 sql_mode 下被静默截断成 0")
	}

	down := readEmbeddedMigration(t, tradeCurrencyDownPath)
	downFlat := collapseSpaces(down)
	for _, fragment := range []string{
		// 回滚只还得回金币(kind=1);其它币种在旧结构里没有容身之处,这一点写在迁移头部,
		// 这里钉住"至少金币要写回去",不能变成静默 DROP 掉整张钱包表。
		"INSERT INTO `player_currency`",
		"WHERE `currency_kind` = 1",
		"DROP TABLE IF EXISTS `player_wallet`",
		"ALTER TABLE `inventory_ledger` DROP COLUMN `result_currencies`",
		"ALTER TABLE `inventory_ledger` DROP COLUMN `result_currency_delta`",
		"CHANGE COLUMN `frozen_amount` `frozen_gold` BIGINT NOT NULL DEFAULT 0",
		"ALTER TABLE `auction_escrow` DROP COLUMN `currency_kind`",
	} {
		if !strings.Contains(downFlat, fragment) {
			t.Errorf("000005 down 缺少契约片段 %q", fragment)
		}
	}
	// 回滚只回滚本迁移新增的东西,不得顺手动 000004 的列。
	if strings.Contains(downFlat, "DROP COLUMN `attributes`") {
		t.Error("000005 down 只回滚本迁移新增的表/列,不得触碰 000004 的 attributes")
	}
}
