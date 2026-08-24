package main

import (
	"io/fs"
	"path"
	"regexp"
	"strings"
	"testing"
)

// 迁移必须同时能在 MySQL 8 与 TiDB 上执行(生产 owner / account 等库在 TiDB,
// §9.22)。本文件是**纯静态**的方言体检:不连库,只扫嵌入的迁移文本,把已经踩过的
// 坑固化成编译期就能拦下的断言。

var (
	alterTablePattern = regexp.MustCompile(`(?i)ALTER\s+TABLE`)
	dropColumnPattern = regexp.MustCompile("(?i)DROP\\s+COLUMN\\s+`?([A-Za-z0-9_]+)`?")
	addColumnPattern  = regexp.MustCompile("(?i)ADD\\s+COLUMN\\s+`?([A-Za-z0-9_]+)`?")
)

// TestMigrationsAvoidSameColumnDropAddInOneAlter 守住 2026-08-09 实测到的 TiDB 行为:
//
//	ALTER TABLE t DROP COLUMN c, ADD COLUMN c ...
//
// MySQL 8 按子句先后处理所以能过,TiDB 的 multi-schema change 却拿每个子句去和语句执行
// 前的原表结构比对,ADD 看见尚未删除的同名列 → ERROR 1060 Duplicate column name。
// 换名列(DROP a + ADD b)在一条语句里两边都可用,只有"删掉再加同一个列名"必须拆两条。
func TestMigrationsAvoidSameColumnDropAddInOneAlter(t *testing.T) {
	scanned := 0
	forEachMigrationFile(t, func(file, content string) {
		scanned++
		for _, statement := range alterStatements(content) {
			drops := columnNames(dropColumnPattern, statement)
			for added := range columnNames(addColumnPattern, statement) {
				if _, exists := drops[added]; exists {
					t.Errorf("%s: 同一条 ALTER 里既 DROP 又 ADD 列 %q，TiDB 会报 "+
						"ERROR 1060 Duplicate column name；必须拆成两条 ALTER。语句片段: %s",
						file, added, collapseSpaces(statement))
				}
			}
		}
	})
	if scanned == 0 {
		t.Fatal("没有扫到任何迁移文件，嵌入的 migrations 可能坏了")
	}
}

// TestSameColumnDropAddDetector 用 000004 修复前的原文喂给探测器，证明上面的扫描在修复前
// 会失败、修复后才通过；同时钉住反面：换名列(DROP a + ADD b)是 TiDB 允许的写法，
// pandora_leaderboard/000003 正靠它，不能被误报。
func TestSameColumnDropAddDetector(t *testing.T) {
	cases := []struct {
		name    string
		sql     string
		flagged string // 期望被判定为"同一条 ALTER 里删了又加"的列名，空 = 不该报
	}{
		{
			name:    "000004 修复前(同名列)",
			sql:     "SET @s := IF(@c = 1,\n  'ALTER TABLE `player_item_instance` DROP COLUMN `attributes`, ADD COLUMN `attributes` VARBINARY(1024) NULL COMMENT ''x'' AFTER `identified`',\n  'SELECT 1');",
			flagged: "attributes",
		},
		{
			name:    "000003 leaderboard(换名列)",
			sql:     "SET @s := IF(@c = 1,\n  'ALTER TABLE `leaderboard_reward_log` DROP COLUMN `reward_json`, ADD COLUMN `reward_pb` VARBINARY(2048) NOT NULL DEFAULT '''' AFTER `status`',\n  'SELECT 1');",
			flagged: "",
		},
		{
			name:    "拆成两条(修复后)",
			sql:     "ALTER TABLE `t` DROP COLUMN `attributes`;\nALTER TABLE `t` ADD COLUMN `attributes` VARBINARY(1024) NULL;",
			flagged: "",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			found := ""
			for _, statement := range alterStatements(stripLineComments(testCase.sql)) {
				drops := columnNames(dropColumnPattern, statement)
				for added := range columnNames(addColumnPattern, statement) {
					if _, exists := drops[added]; exists {
						found = added
					}
				}
			}
			if found != testCase.flagged {
				t.Fatalf("探测结果=%q，期望=%q", found, testCase.flagged)
			}
		})
	}
}

func forEachMigrationFile(t *testing.T, visit func(file, content string)) {
	t.Helper()
	err := fs.WalkDir(migrationsFS, "migrations", func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path.Base(p), ".sql") {
			return nil
		}
		raw, err := fs.ReadFile(migrationsFS, p)
		if err != nil {
			return err
		}
		visit(p, stripLineComments(string(raw)))
		return nil
	})
	if err != nil {
		t.Fatalf("遍历嵌入迁移: %v", err)
	}
}

// stripLineComments 去掉 `-- ...` 行注释。注释里成段解释这条规则本身(含 DROP/ADD 字样)
// 属正常,不能让说明文字触发断言。
func stripLineComments(content string) string {
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		if idx := strings.Index(line, "--"); idx >= 0 {
			lines[i] = line[:idx]
		}
	}
	return strings.Join(lines, "\n")
}

// alterStatements 取出每段 ALTER TABLE 文本。DDL 既可能是裸语句，也可能是 PREPARE 用的
// 单引号字符串字面量(条件迁移的写法)，两种都要覆盖：从 ALTER TABLE 起扫到语句分号或
// 字符串字面量的结束单引号为止（`''` 是转义，不算结束）。
func alterStatements(content string) []string {
	matches := alterTablePattern.FindAllStringIndex(content, -1)
	statements := make([]string, 0, len(matches))
	for _, match := range matches {
		statements = append(statements, alterStatementAt(content, match[0]))
	}
	return statements
}

func alterStatementAt(content string, start int) string {
	for i := start; i < len(content); i++ {
		switch content[i] {
		case ';':
			return content[start:i]
		case '\'':
			if i+1 < len(content) && content[i+1] == '\'' {
				i++ // '' 是字符串里的转义单引号
				continue
			}
			return content[start:i]
		}
	}
	return content[start:]
}

func columnNames(pattern *regexp.Regexp, statement string) map[string]struct{} {
	names := make(map[string]struct{})
	for _, match := range pattern.FindAllStringSubmatch(statement, -1) {
		names[strings.ToLower(match[1])] = struct{}{}
	}
	return names
}

func collapseSpaces(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

const (
	tradeAttributesUpPath   = "migrations/pandora_trade/000004_attributes_proto_binary.up.sql"
	tradeAttributesDownPath = "migrations/pandora_trade/000004_attributes_proto_binary.down.sql"
)

// TestTradeAttributesProtoBinaryContract 固化 000004 的两条要害:列型换成 VARBINARY，
// 以及 ADD 的执行条件必须是"现查列不存在"而不是复用 DROP 的判断 —— 两条 ALTER 之间进程
// 被杀会留下"已删未加"，只有现查才能在重跑时自愈。
// pandora_trade 的"最新版本号"钉子在 trade_currency_migration_test.go —— 每套迁移集只留
// 一处钉子,且钉在**最新那条**迁移的契约测试里:钉在旧版本的测试里,下次加迁移时红的是
// 一个名字与新迁移毫无关系的用例,排查者会先怀疑自己改坏了 000004。
func TestTradeAttributesProtoBinaryContract(t *testing.T) {
	up := readEmbeddedMigration(t, tradeAttributesUpPath)
	for _, fragment := range []string{
		"ALTER TABLE `player_item_instance` DROP COLUMN `attributes`'",
		"ALTER TABLE `player_item_instance` ADD COLUMN `attributes` VARBINARY(1024) NULL",
		"ALTER TABLE `mail_transfer_escrow` DROP COLUMN `attributes`'",
		"ALTER TABLE `mail_transfer_escrow` ADD COLUMN `attributes` VARBINARY(1024) NULL",
		"AND data_type = 'json'",
	} {
		if !strings.Contains(up, fragment) {
			t.Errorf("000004 up 缺少契约片段 %q", fragment)
		}
	}
	if strings.Count(up, "SELECT COUNT(*) = 0") != 2 {
		t.Error("000004 up 的两处 ADD 必须各自现查『列不存在』，不得复用 DROP 的判断结果")
	}

	down := readEmbeddedMigration(t, tradeAttributesDownPath)
	for _, fragment := range []string{
		"ALTER TABLE `player_item_instance` ADD COLUMN `attributes` JSON NULL",
		"ALTER TABLE `mail_transfer_escrow` ADD COLUMN `attributes` JSON NULL",
		"AND data_type = 'varbinary'",
	} {
		if !strings.Contains(down, fragment) {
			t.Errorf("000004 down 缺少契约片段 %q", fragment)
		}
	}
}
