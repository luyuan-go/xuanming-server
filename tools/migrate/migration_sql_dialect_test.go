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

// TestStripLineCommentsSkipsStringLiterals 钉住 2026-08-24 收口轮修掉的门禁盲区。
//
// expand-only 主门禁判的是 stripLineComments **之后**的正文。旧实现"整行从第一个 `--` 起
// 截断"不认字符串字面量,于是字面量里随便一个 `--`(破折号、CSV 样例、注释文案)就能把
// 同一行后面的真 DDL 一起吃掉,破坏性 DDL 对门禁完全隐形。
//
// 回退验证:把 stripLineComments 换回旧的按行截断实现,本用例第一条立刻红
// (剥完得到 []),CHANGE / DROP 两条同理。
func TestStripLineCommentsSkipsStringLiterals(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want []string
	}{
		{
			// 复核实测原样本:COMMENT 文案里带 `--`,同一行后面还有 DROP COLUMN。
			name: "字面量里的 -- 不得吃掉同行后面的 DDL",
			sql:  "SET @s := 'ALTER TABLE `t` COMMENT = ''a--b'', DROP COLUMN `x`';",
			want: []string{"DROP COLUMN"},
		},
		{
			name: "字面量里的 -- 不得吃掉同行后面的 CHANGE",
			sql:  "SET @s := 'ALTER TABLE `t` COMMENT = ''x--y'', CHANGE COLUMN `a` `b` INT';",
			want: []string{"CHANGE COLUMN"},
		},
		{
			// 反向:真正的行注释仍必须被剥掉,否则注释里成段讲解 DROP/RENAME 的迁移会全红。
			name: "真行注释仍要剥掉",
			sql:  "-- 本迁移不做 DROP COLUMN / RENAME TABLE，只加列。\nALTER TABLE `t` ADD COLUMN `c` INT;",
			want: nil,
		},
		{
			// 行注释里出现落单单引号(中文行文里很常见)不得把状态机带跑偏:
			// `--` 是在 outside 状态下识别的,注释正文里的引号根本不参与状态。
			name: "行注释里的落单单引号不影响后续剥离",
			sql:  "-- 别写成 'DROP COLUMN，见 §9.21\n-- 这一行也该被剥掉：RENAME TABLE\nALTER TABLE `t` ADD COLUMN `c` INT;",
			want: nil,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := destructiveHits(stripLineComments(testCase.sql))
			if strings.Join(got, "|") != strings.Join(testCase.want, "|") {
				t.Fatalf("剥注释后探测=%v，期望=%v；剥出来的正文=%q",
					got, testCase.want, stripLineComments(testCase.sql))
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
//
// **必须跳过字符串字面量与反引号标识符内部**(2026-08-24 收口轮补)。上一版是"整行从第一个
// `--` 起截断",不认字面量;而本仓的条件迁移把整条 DDL 装在单引号字面量里(000005 的
// PREPARE 写法),字面量里出现 `--` 是完全合法的正文。复核实测:
//
//	SET @s := 'ALTER TABLE `t` COMMENT = ''a--b'', DROP COLUMN `x`';
//	destructiveHits(原文)                    -> [DROP COLUMN]
//	destructiveHits(stripLineComments(原文)) -> []
//
// 主门禁判的正是剥完的那一份,于是**字面量里带 `--` 的破坏性 DDL 对门禁完全隐形** ——
// 而且 `--` 不必是人写的注释,COMMENT 文案里一个破折号、一段 CSV 样例就能触发。
//
// 剥掉注释正文时保留换行:别的断言(以及报错里的语句片段)按行/按分号切,吃掉换行会串行。
//
// 刻意没做的两件事:①不认 `#` 与 `/* */` 注释 —— 本仓迁移一律用 `--`,加了反而多一份要维护的
// 状态机;②单引号状态**不在换行处复位** —— MySQL 的字符串字面量本就允许跨行,复位是错的。
// 代价是全文件若出现落单的单引号,其后的 `--` 注释会被当成字面量正文保留下来 —— 那是
// **保守**方向(注释里的散文会去撞破坏性 DDL 正则,红给你看),不是漏报方向。
func stripLineComments(content string) string {
	const (
		outside    = iota
		inQuote    // 单引号字符串字面量,`''` 是转义不是结束
		inBacktick // 反引号标识符
	)

	var out strings.Builder
	out.Grow(len(content))
	state := outside
	for i := 0; i < len(content); i++ {
		c := content[i]
		switch state {
		case outside:
			if c == '-' && i+1 < len(content) && content[i+1] == '-' {
				for i < len(content) && content[i] != '\n' {
					i++
				}
				if i < len(content) {
					out.WriteByte('\n')
				}
				continue
			}
			if c == '\'' {
				state = inQuote
			} else if c == '`' {
				state = inBacktick
			}
		case inQuote:
			if c == '\'' {
				if i+1 < len(content) && content[i+1] == '\'' {
					out.WriteByte(c)
					i++
					c = content[i] // 连写的第二个单引号原样留下,状态不变
				} else {
					state = outside
				}
			}
		case inBacktick:
			if c == '`' {
				state = outside
			}
		}
		out.WriteByte(c)
	}
	return out.String()
}

// alterStatements 取出每段 ALTER TABLE 文本。DDL 既可能是裸语句，也可能是 PREPARE 用的
// 单引号字符串字面量(条件迁移的写法)，两种都要覆盖：从 ALTER TABLE 起扫到语句分号或
// 字符串字面量的结束单引号为止（`”` 是转义，不算结束）。
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
