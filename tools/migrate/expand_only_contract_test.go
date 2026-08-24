package main

// expand_only_contract_test.go — 「迁移默认只许 expand」的机械门禁(INC-20260812-001 行动项 A-2)。
//
// 存在的理由:2026-08-12 一次审查同时抓到两个**已发布**迁移做的是 contract 而不是 expand ——
// `pandora_account/000006` 用 RENAME+DROP 换掉角色编号三件套,`pandora_player/000007` 直接
// `DROP players.mmr`。迁移一执行,尚未排空的旧 Go 副本读写的对象**当场消失**,违反
// CLAUDE.md §9.16 / §9.21「删除能力必须走 expand → migrate → contract」。
//
// 这两条都是**人眼**在事后审查里发现的:此前的迁移契约测试只断言"某某片段存在"与
// fresh-init 一致性,没有任何一条断言"up.sql 不许出现 DROP / RENAME"。本文件把这道判断
// 机械化,让下一次写反的人在 `go test` 就红,而不是等迁移在生产上把旧副本打死。
//
// 判定规则:
//   - 只看 `*.up.sql`。down 迁移删掉自己刚建的对象是正常的,不在本门禁范围。
//   - 先剥掉 `-- ` 行注释(注释里成段解释这条规则本身属正常,见 stripLineComments)。
//   - 命中破坏性 DDL 的 up 迁移必须二选一:
//       ① 文件头显式标注 `-- CONTRACT:` 并写明**旧副本排空判据**(谁排空、怎么确认);
//       ② 在 grandfatheredContractMigrations 里登记(仅限门禁上线前已对 origin 暴露、
//          因而不可再修改的历史迁移),并写清它删了什么、兼容面是否已被后续 expand 补回。
//
// 反向门禁见 TestGrandfatheredContractListIsExact:allowlist 里的条目必须**确实还是**
// 破坏性迁移,否则这张表会退化成一张没人维护的永久豁免后门。

import (
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// sqlIdent 是一个列名/表名在 DDL 里的两种合法写法:反引号包裹,或裸标识符。
// 门禁必须两种都认 —— 本仓的迁移习惯全带反引号,但 MySQL 不要求,
// `docs/design/player-no-and-login-surge.md` **§3.6.3**(449-499 行)讨论改名时用的就是裸写法
// —— 具体是第 491 行「用 `RENAME COLUMN` 而非 `CHANGE old new <type>`」那句。
// (别写成 §3.6.4:那一节从 500 行才开始,讲的是 000007 expand 回补与 contract 退出条件。)
const sqlIdent = "(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_$]*)"

// destructiveRule 是一条破坏性 DDL 形态。
//
// rejectHeads 存在的理由:Go 的 regexp 是 RE2,**没有 negative lookahead**。
// 「DROP 后面跟的是对象名,而不是 TABLE / INDEX / PRIMARY 这些开启**另一种**形态的关键字」
// 这句话没法写进正则,只能在命中之后二次过滤。约定:带 rejectHeads 的正则必须恰好有两个
// 捕获组 —— 第 1 组是**显式关键字**(可省,省了就是空串),第 2 组是紧随其后的标识符。
//   - 第 1 组非空:关键字写全了,第 2 组必是对象名 → 命中。
//   - 第 2 组带反引号:被引号包起来的只可能是对象名(MySQL 里 `table` 是合法列名)→ 命中。
//   - 否则查 rejectHeads:落在表里说明这条语句属于**别的**形态,那些形态各有自己的条目,
//     这里必须放过,否则同一条语句会被 destructiveOccurrences 数两次,把条数门禁打歪。
type destructiveRule struct {
	name        string
	re          *regexp.Regexp
	rejectHeads map[string]bool
}

// dropObjectHeads / renameObjectHeads:关键字省略时,紧跟 DROP / RENAME 的裸词若是这些,
// 说明它开启的是另一种 DDL 形态,而不是「省了关键字的列名/表名」。
//
// ⚠️ 已知且**刻意保留**的盲区(2026-08-24 收口轮明写,别当成"扫过了"):
// `DROP PARTITION` / `DROP CONSTRAINT` / `DROP CHECK` 被放过后**没有**自己的条目,
// 因而本门禁完全不管这三种。理由:本仓零分区表、零 CHECK 约束,且 DROP CONSTRAINT/CHECK
// 是放松约束不是让旧副本的目标对象消失。真要用分区了,必须先给 `DROP PARTITION` 补一条,
// 而不是指望这张表挡住它。同步写在 incident §7.3 形态表下面。
var (
	dropObjectHeads = map[string]bool{
		"COLUMN": true, "TABLE": true, "TEMPORARY": true, "INDEX": true, "KEY": true,
		"PRIMARY": true, "FOREIGN": true, "CONSTRAINT": true, "CHECK": true, "PARTITION": true,
		"DATABASE": true, "SCHEMA": true, "VIEW": true, "TRIGGER": true, "PROCEDURE": true,
		"FUNCTION": true, "EVENT": true, "USER": true, "ROLE": true, "SERVER": true,
		"TABLESPACE": true, "LOGFILE": true,
	}
	renameObjectHeads = map[string]bool{
		"COLUMN": true, "INDEX": true, "KEY": true, "USER": true,
	}
)

// destructiveDDL 是「会让旧副本的 SQL 目标对象消失」的语句形态。
// 只列真正拆兼容面的动作:ADD / MODIFY COMMENT / CREATE 一律不在内。
var destructiveDDL = []destructiveRule{
	// DROP 与 RENAME 的判据同样是**语法形状**,不是关键字(2026-08-24 收口轮补)。
	// 上一轮只把 CHANGE 改成了形状判据,同一条理由**逐字**适用于 DROP 与 RENAME,却只落实了
	// CHANGE 那一行。复核拿探测器本体喂样本,下面六条当时全返回空:
	//     ALTER TABLE `t` DROP `col`;            ALTER TABLE t DROP col;
	//     ALTER TABLE `t` DROP PRIMARY KEY;      ALTER TABLE `t` DROP FOREIGN KEY `fk`;
	//     ALTER TABLE `old` RENAME TO `new`;     ALTER TABLE `old` RENAME `new`;
	// 全是"旧副本的目标对象当场消失",与 CHANGE 完全同级。
	//
	// `DROP [COLUMN] <名>`:MySQL 的 COLUMN 关键字可省(和 CHANGE 一模一样的省法)。
	{"DROP COLUMN", regexp.MustCompile(`(?i)\bDROP\s+(?:(COLUMN)\s+)?(` + sqlIdent + `)`), dropObjectHeads},
	{"DROP TABLE", regexp.MustCompile(`(?i)\bDROP\s+TABLE\b`), nil},
	{"DROP INDEX", regexp.MustCompile(`(?i)\bDROP\s+(?:INDEX|KEY)\b`), nil},
	// `DROP PRIMARY KEY` / `DROP FOREIGN KEY`:DROP 后面跟的是 PRIMARY / FOREIGN,
	// 不是 INDEX 也不是 KEY,所以旧的 `\bDROP\s+(INDEX|KEY)\b` 一条都抓不到。
	// 掉主键 = 旧副本按主键的 upsert / FOR UPDATE 当场语义变化,同级破坏。
	{"DROP PRIMARY KEY", regexp.MustCompile(`(?i)\bDROP\s+PRIMARY\s+KEY\b`), nil},
	{"DROP FOREIGN KEY", regexp.MustCompile(`(?i)\bDROP\s+FOREIGN\s+KEY\b`), nil},
	{"RENAME COLUMN", regexp.MustCompile(`(?i)\bRENAME\s+COLUMN\b`), nil},
	// `RENAME KEY old TO new` 与 `RENAME INDEX` 是同一个动作的两种拼法(和 DROP INDEX|KEY 对称)。
	{"RENAME INDEX", regexp.MustCompile(`(?i)\bRENAME\s+(?:INDEX|KEY)\b`), nil},
	// `ALTER TABLE old RENAME [TO|AS] new` 是在 ALTER 内改表名的正规写法,TO/AS 还都能省。
	// 旧的 `\bRENAME\s+TABLE\b` 只认独立的 `RENAME TABLE a TO b`,ALTER 内那三种拼法全漏。
	{"RENAME TABLE", regexp.MustCompile(`(?i)\bRENAME\s+(?:(TABLE|TO|AS)\s+)?(` + sqlIdent + `)`), renameObjectHeads},
	// CHANGE COLUMN 在兼容性上与 RENAME COLUMN **完全等价**:两者都让旧列名当场消失,
	// 还在运行的旧副本查旧列名一律报错。门禁上线时只列了 RENAME,于是 2026-08-22 的
	// pandora_trade/000005 用 `CHANGE COLUMN frozen_gold frozen_amount` 做硬切,
	// 迁移作者自己在头注释里写明"不支持混跑",门禁却一声没吭 —— 补上这一条。
	//
	// 四种写法都要命中(COLUMN 关键字可省、标识符可不带反引号,MySQL 全都接受,
	// 省了/裸写照样是硬切)。判据不是"CHANGE 后面跟什么关键字",而是 **CHANGE 的语法形状**:
	// `CHANGE [COLUMN] <旧名> <新名> <类型>` —— 两个标识符后面必须还跟得出一个类型词。
	//
	// 为什么不能用更松的写法(2026-08-24 变异实测,两个方向都踩过):
	//   - 只认「CHANGE 后面紧跟 COLUMN 关键字或反引号」(本条上一版):
	//     ALTER TABLE auction_escrow CHANGE frozen_gold frozen_amount BIGINT NOT NULL;
	//     **漏报** —— 裸标识符 + 省 COLUMN 是合法 MySQL,下一条这么写的硬切会静默放行。
	//   - 只认光秃秃的 CHANGE 一个词:
	//     ADD COLUMN `c` INT COMMENT 'change `foo` semantics' **误报** ——
	//     列注释里写英文 change 再跟一个反引号标识符是很自然的措辞。
	//     注意这类误报**剥行注释救不了**:COMMENT 是字符串字面量不是 `--` 注释,
	//     而且本仓条件迁移把整条 DDL 装在单引号字面量里(见 000005 的 PREPARE 写法),
	//     所以也不能靠"剥掉单引号字符串"来规避。
	//     要求"两个标识符 + 类型词"正好把它挡掉:`semantics` 之后没有第三个词。
	//
	// 残留的保守面(**刻意接受**,见 TestDestructiveDDLDetector 同名用例):
	// 字符串字面量里连着三个英文单词的散文 `'change this column now'` 仍会命中。
	// 同理,形状化之后 `'drop table support'` / `'rename it later'` 这类字面量散文也会命中
	// DROP / RENAME —— 判据是形状,写成 `DROP <一个词>` 就当 DDL 看。
	// 门禁的两种错法不等价 —— 误报是作者改一句注释或标 CONTRACT,漏报是生产上打死旧副本,
	// 所以宁可保守。真撞上了就把注释换个措辞,别把这条正则改松。
	{"CHANGE COLUMN", regexp.MustCompile(
		`(?i)\bCHANGE\s+(?:COLUMN\s+)?` + sqlIdent + `\s+` + sqlIdent + `\s+[A-Za-z]`), nil},
}

// contractMarker 是显式声明「本版就是 contract,我知道自己在删什么」的标记。
// drainCriterionMarker 强制同一份文件里必须写出旧副本排空判据 —— 只喊 CONTRACT
// 不写判据等于把 §9.21 的举证义务跳过去了。
const (
	contractMarker       = "-- CONTRACT:"
	drainCriterionMarker = "旧副本排空判据"
)

// grandfatheredContractMigrations 登记本门禁上线(2026-08-12)之前**已经对 origin 暴露**、
// 按 tools/migrate/README 已不可再修改的破坏性迁移。
//
// ⚠️ 这张表**只减不增**。新写的迁移一律走 expand;确实需要 contract 的走 contractMarker
// 显式标注路径,不许往这里加行。
var grandfatheredContractMigrations = map[string]string{
	"migrations/pandora_account/000005_rename_player_no.up.sql": "" +
		"RENAME COLUMN/INDEX/TABLE 把 register_no 三件套改名成 player_no。改名当时只论证了" +
		"「生产零注册路径、无存量数据」(数据无风险),没论证二进制共存无风险。兼容面已由" +
		"000007_player_no_expand_compat 重新建回并双写;INC-20260812-001。",
	"migrations/pandora_account/000006_reconcile_player_no.up.sql": "" +
		"同 000005 的收敛版:双对象库里 DROP INDEX uk_register_no / DROP COLUMN register_no / " +
		"DROP TABLE register_no_counter。兼容面已由 000007_player_no_expand_compat 补回;INC-20260812-001。",
	"migrations/pandora_player/000007_rating_pool_partition.up.sql": "" +
		"DROP players.mmr(且因列上挂着 idx_mmr,ALGORITHM=INSTANT 在 MySQL 8.4 必报 1845 → v7 dirty)。" +
		"兼容面已由 000008_rating_pool_expand_compat 补回并双写;INC-20260812-001 行动项 A-1 要求" +
		"未来的 contract 不得原样重放这条语句。",
	"migrations/pandora_leaderboard/000003_reward_proto_binary.up.sql": "" +
		"DROP COLUMN reward_json 与 ADD COLUMN reward_pb 在同一条 ALTER 里(json→pb 表示法切换)。" +
		"未经 expand 窗口;若重来应拆成加列→双写→排空→删列。",
	"migrations/pandora_trade/000004_attributes_proto_binary.up.sql": "" +
		"DROP COLUMN attributes(player_item_instance / mail_transfer_escrow 两张表),同为 " +
		"json→pb 表示法切换,同样未经 expand 窗口。",
}

// TestMigrationsAreExpandOnly 是主门禁。
func TestMigrationsAreExpandOnly(t *testing.T) {
	violations := make([]string, 0)

	forEachUpMigration(t, func(file, stripped, raw string) {
		hits := destructiveHits(stripped)
		if len(hits) == 0 {
			return
		}
		if _, ok := grandfatheredContractMigrations[file]; ok {
			return
		}
		if strings.Contains(raw, contractMarker) {
			if !strings.Contains(raw, drainCriterionMarker) {
				violations = append(violations, file+
					" 标了 "+contractMarker+" 但没写「"+drainCriterionMarker+
					"」:contract 必须写清谁排空、怎么确认旧副本归零,否则等于跳过 §9.21 的举证义务")
			}
			return
		}
		violations = append(violations, file+" 含破坏性 DDL "+strings.Join(hits, " / "))
	})

	if len(violations) == 0 {
		return
	}
	sort.Strings(violations)
	t.Fatalf("迁移默认只许 expand(CLAUDE.md §9.16/§9.21,INC-20260812-001)。\n"+
		"旧副本在滚动升级窗口内仍会读写这些对象,迁移一执行就当场打死它们。\n"+
		"正确做法:加新对象 → 新旧双写 → 确认旧副本排空 → 另立更高版本迁移收缩。\n"+
		"确实是 contract 的,在文件头写 %q 并说明「%s」。\n违规:\n  %s",
		contractMarker, drainCriterionMarker, strings.Join(violations, "\n  "))
}

// TestGrandfatheredContractListIsExact 防止 allowlist 腐化:登记过的文件必须**确实还是**
// 破坏性迁移,而且必须真实存在。历史迁移不可改,所以这两条恒该成立;一旦不成立,说明
// 有人改了不可改的文件,或者把条目留在表里当永久后门。
func TestGrandfatheredContractListIsExact(t *testing.T) {
	seen := make(map[string]bool, len(grandfatheredContractMigrations))

	forEachUpMigration(t, func(file, stripped, _ string) {
		if _, ok := grandfatheredContractMigrations[file]; !ok {
			return
		}
		seen[file] = true
		if len(destructiveHits(stripped)) == 0 {
			t.Errorf("%s 登记在 grandfathered 清单里,但已不含任何破坏性 DDL —— "+
				"要么这份不可改的历史迁移被人改过,要么该把它从清单里删掉", file)
		}
	})

	for file, reason := range grandfatheredContractMigrations {
		if !seen[file] {
			t.Errorf("grandfathered 清单里的 %s 在嵌入迁移里不存在(路径写错或文件被删):%s", file, reason)
		}
		if strings.TrimSpace(reason) == "" {
			t.Errorf("grandfathered 清单里的 %s 没写理由 —— 每条豁免都必须说明删了什么、兼容面补回了没有", file)
		}
	}
}

// TestDestructiveDDLDetector 直接喂样本给探测器。
//
// 为什么单独写:主门禁的绿色**不能**证明探测器还活着 —— 现有违规迁移要么在 grandfathered
// 名单里、要么已标 CONTRACT,两条路都在命中之后才分叉。把正则改坏(比如把 CHANGE COLUMN
// 那条删掉),主门禁照样全绿,下一条硬切迁移就此静默放行。本用例是那条正则的唯一守卫。
//
// 样本来自本仓真实踩过的形态,别当成教科书例子精简掉。
func TestDestructiveDDLDetector(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want []string // 期望命中的 destructiveDDL 名字,nil = 不该报
	}{
		{
			name: "CHANGE COLUMN 反引号形(trade/000005 原文)",
			sql:  "'ALTER TABLE `auction_escrow` CHANGE COLUMN `frozen_gold` `frozen_amount` BIGINT NOT NULL DEFAULT 0'",
			want: []string{"CHANGE COLUMN"},
		},
		{
			// MySQL 允许省略 COLUMN 关键字,省了照样是硬切,必须同样命中。
			name: "CHANGE 省略 COLUMN 关键字",
			sql:  "ALTER TABLE `auction_escrow` CHANGE `frozen_gold` `frozen_amount` BIGINT NOT NULL;",
			want: []string{"CHANGE COLUMN"},
		},
		{
			// 反引号全省 + COLUMN 全省 —— 合法 MySQL,兼容性上与上一条**完全一样**。
			// 2026-08-24 变异实测:这一形态在旧正则下返回空,下一条这么写的硬切会静默放行。
			// 不是理论形态,`docs/design/player-no-and-login-surge.md` **§3.6.3** 第 491 行讨论
			// register_no → player_no 改名时用的就是 `CHANGE old new <type>` 这个拼法。
			// (§3.6.4 从 500 行才开始,是另一回事 —— 别再引错。)
			name: "CHANGE 裸标识符 + 省 COLUMN(旧正则漏报的形态)",
			sql:  "ALTER TABLE auction_escrow CHANGE frozen_gold frozen_amount BIGINT NOT NULL;",
			want: []string{"CHANGE COLUMN"},
		},
		{
			// 真实的假阳性风险面:英文 change + 空格 + 反引号标识符,写在**列注释**里。
			//
			// 别把这个用例换成 `-- TiDB multi-schema change` 之类的行注释来"证明"没有假阳性 ——
			// 行注释本来就会被 stripLineComments 剥掉,拿它当反例等于自己给自己发合格证
			// (上一版就是这么写的,复核实测下面这条会误报)。COMMENT 是字符串字面量,
			// 剥行注释救不了它;而本仓条件迁移把整条 DDL 装在单引号字面量里(000005 的
			// PREPARE 写法),所以也不能靠"剥单引号字符串"规避 —— 只能靠正则的语法形状。
			name: "COMMENT 里 change 后跟反引号标识符(真实假阳性面)",
			sql:  "ALTER TABLE `t` ADD COLUMN `c` INT COMMENT 'change `foo` semantics';",
			want: nil,
		},
		{
			// 上一条的宽松版:注释里 change 之后连着三个英文单词,凑得出"两标识符 + 类型词"
			// 的形状,**仍会误报**。这是刻意接受的取舍,不是遗漏 ——
			// 误报的代价是作者改一句措辞或标 CONTRACT,漏报的代价是生产上打死旧副本。
			// 本用例把这个取舍钉住:谁想"顺手修掉这个假阳性",必须先证明不会带回漏报。
			name: "散文式 change + 三个词仍误报(刻意接受的保守面)",
			sql:  "ALTER TABLE `t` ADD COLUMN `c` INT COMMENT 'change this column now';",
			want: []string{"CHANGE COLUMN"},
		},
		{
			// 列注释里出现英文单词 change 属正常措辞,不能因此把纯 ADD 判成破坏性。
			name: "COMMENT 里的英文单词 change 不算 DDL",
			sql:  "ALTER TABLE `t` ADD COLUMN `c` INT NOT NULL DEFAULT 0 COMMENT 'schema change safe';",
			want: nil,
		},
		{
			name: "纯 expand(ADD COLUMN / CREATE TABLE)不该报",
			sql:  "CREATE TABLE IF NOT EXISTS `player_wallet` (`player_id` BIGINT UNSIGNED NOT NULL);",
			want: nil,
		},
		{
			name: "RENAME COLUMN 仍要命中(防改坏既有条目)",
			sql:  "ALTER TABLE `t` RENAME COLUMN `a` TO `b`;",
			want: []string{"RENAME COLUMN"},
		},

		// ↓↓↓ 2026-08-24 收口轮:DROP / RENAME 的同级盲区。
		// 上一轮只把 CHANGE 改成语法形状判据,同一条理由逐字适用于 DROP 与 RENAME,
		// 却只在 CHANGE 那一行落实了。下面六条在改之前**全部返回空**
		// (复核拿探测器本体喂样本实测),而它们全是"旧副本目标对象当场消失"。
		{
			// MySQL 的 COLUMN 关键字可省 —— 与 CHANGE 完全同理。
			name: "DROP 省略 COLUMN 关键字(反引号)",
			sql:  "ALTER TABLE `t` DROP `col`;",
			want: []string{"DROP COLUMN"},
		},
		{
			name: "DROP 省略 COLUMN 关键字(裸标识符)",
			sql:  "ALTER TABLE t DROP col;",
			want: []string{"DROP COLUMN"},
		},
		{
			// 旧正则是 `\bDROP\s+(INDEX|KEY)\b`,DROP 后面是 PRIMARY 不是 KEY,一条都抓不到。
			name: "DROP PRIMARY KEY",
			sql:  "ALTER TABLE `t` DROP PRIMARY KEY;",
			want: []string{"DROP PRIMARY KEY"},
		},
		{
			name: "DROP FOREIGN KEY",
			sql:  "ALTER TABLE `t` DROP FOREIGN KEY `fk`;",
			want: []string{"DROP FOREIGN KEY"},
		},
		{
			// ALTER 内改表名的正规写法,TO / AS 还都能省。旧的 `\bRENAME\s+TABLE\b`
			// 只认独立语句 `RENAME TABLE a TO b`,这三种拼法全漏。
			name: "ALTER TABLE ... RENAME TO",
			sql:  "ALTER TABLE `old` RENAME TO `new`;",
			want: []string{"RENAME TABLE"},
		},
		{
			name: "ALTER TABLE ... RENAME(省略 TO)",
			sql:  "ALTER TABLE `old` RENAME `new`;",
			want: []string{"RENAME TABLE"},
		},
		{
			// 反向:形状化之后不许把同一条语句数成两种形态。
			// `DROP TABLE` / `DROP INDEX` / `RENAME COLUMN` 都会撞上"省关键字"那两条正则,
			// 必须被 rejectHeads 放过,否则 destructiveOccurrences 的条数门禁会凭空多一条。
			name: "关键字形态不得被省关键字那条重复计数",
			sql: "DROP TABLE IF EXISTS `t`;\n" +
				"ALTER TABLE `t` DROP INDEX `idx`;\n" +
				"ALTER TABLE `t` RENAME COLUMN `a` TO `b`;\n" +
				"ALTER TABLE `t` RENAME INDEX `i` TO `j`;",
			want: []string{"DROP TABLE", "DROP INDEX", "RENAME COLUMN", "RENAME INDEX"},
		},
		{
			// 反引号里的对象名即使**长得像关键字**也是对象名,不能被 rejectHeads 放过。
			name: "反引号包裹的关键字同名列仍是列",
			sql:  "ALTER TABLE `t` DROP `table`;",
			want: []string{"DROP COLUMN"},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := destructiveHits(stripLineComments(testCase.sql))
			if strings.Join(got, "|") != strings.Join(testCase.want, "|") {
				t.Fatalf("探测结果=%v,期望=%v", got, testCase.want)
			}
		})
	}
}

// TestDestructiveOccurrencesCounts 守 destructiveOccurrences 的**计数**语义。
// destructiveHits 去重(每种形态最多一条),条数门禁靠的恰恰是不去重那一份 ——
// 把 FindAllString 写回 MatchString,trade/000005 的"只许一条硬切"就会退回成
// "至少一条硬切",再加第二条也不红。
func TestDestructiveOccurrencesCounts(t *testing.T) {
	sql := "ALTER TABLE `a` CHANGE COLUMN `x` `y` INT;\n" +
		"ALTER TABLE `b` CHANGE `p` `q` BIGINT;\n" +
		"ALTER TABLE `c` DROP COLUMN `z`;\n"
	if got, want := destructiveOccurrences(sql), []string{"DROP COLUMN", "CHANGE COLUMN", "CHANGE COLUMN"}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("destructiveOccurrences=%v,期望=%v", got, want)
	}
	if got, want := destructiveHits(sql), []string{"DROP COLUMN", "CHANGE COLUMN"}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("destructiveHits=%v,期望=%v(去重语义)", got, want)
	}
}

func destructiveHits(stripped string) []string {
	hits := make([]string, 0, len(destructiveDDL))
	for _, d := range destructiveDDL {
		// 不能写 d.re.MatchString:带 rejectHeads 的条目里,「正则命中」不等于「本形态命中」
		// (`DROP TABLE` 会命中 DROP COLUMN 那条正则,却该由 DROP TABLE 条目负责)。
		if d.count(stripped) > 0 {
			hits = append(hits, d.name)
		}
	}
	return hits
}

// count 返回本形态在 stripped 里的命中次数,rejectHeads 的过滤语义见 destructiveRule。
func (d destructiveRule) count(stripped string) int {
	if d.rejectHeads == nil {
		return len(d.re.FindAllString(stripped, -1))
	}
	n := 0
	for _, m := range d.re.FindAllStringSubmatch(stripped, -1) {
		keyword, ident := m[1], m[2]
		if keyword == "" && !strings.HasPrefix(ident, "`") && d.rejectHeads[strings.ToUpper(ident)] {
			continue // 这条语句属于别的形态,由别的条目负责
		}
		n++
	}
	return n
}

// destructiveOccurrences 返回 stripped 正文里**每一处**破坏性 DDL 命中(同一形态出现两次
// 就返回两条),顺序按 destructiveDDL 的声明顺序。destructiveHits 只回答"有没有",
// 想钉「破坏性 DDL 恰好只有 N 条、且就是那 N 条」的迁移契约必须用这个。
//
// 为什么单独有这个函数(2026-08-24 变异实测):trade/000005 的条数门禁原先写的是
// `strings.Count(strings.ToUpper(upDDL), "CHANGE COLUMN")` —— 字面量计数,
// 只认 7 种破坏形态里的 1 种,且只认 CHANGE 两种拼法里的 1 种。往 up.sql 末尾追加
// 一条省 COLUMN 的 CHANGE 加一条 DROP COLUMN,`go test` 照样全绿,
// 那条注释里写的"钉死数量"从来没有实现过。条数门禁一律走探测器,不许自己数字符串。
func destructiveOccurrences(stripped string) []string {
	occurrences := make([]string, 0, len(destructiveDDL))
	for _, d := range destructiveDDL {
		for i := 0; i < d.count(stripped); i++ {
			occurrences = append(occurrences, d.name)
		}
	}
	return occurrences
}

// forEachUpMigration 只遍历 up 迁移,同时把「剥注释后的正文」与「原始正文」都交给回调:
// 破坏性 DDL 要在剥注释后判(注释里讲解规则不算违规),而 CONTRACT 标记恰恰写在注释里。
func forEachUpMigration(t *testing.T, visit func(file, stripped, raw string)) {
	t.Helper()
	count := 0
	err := fs.WalkDir(migrationsFS, "migrations", func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path.Base(p), ".up.sql") {
			return nil
		}
		raw, rerr := fs.ReadFile(migrationsFS, p)
		if rerr != nil {
			return rerr
		}
		count++
		visit(p, stripLineComments(string(raw)), string(raw))
		return nil
	})
	if err != nil {
		t.Fatalf("遍历嵌入 up 迁移: %v", err)
	}
	// 解析坏掉时(比如 embed 路径变了)必须炸,不能静默零遍历后打绿。
	if count < 20 {
		t.Fatalf("只遍历到 %d 份 up 迁移,遍历八成坏了", count)
	}
}
