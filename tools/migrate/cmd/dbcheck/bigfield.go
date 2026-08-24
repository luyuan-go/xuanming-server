// bigfield.go — dbcheck 的大字段体检(§9.24 的"单行/单元素过大"侧,2026-07-22)。
//
// 与主文件(行数/索引/outbox)的分工:
//
//	main.go     管"表有多少行"——增长的**广度**失控(清理没追上写入)
//	bigfield.go 管"单行有多大"——增长的**深度**失控(blob 内部集合无界)
//
// 两者是完全独立的失控方向,只查一个必漏另一个。真实案例(本仓 2026-07-22 审计):
//
//	广度有闸、深度无闸:bag 管住了 items 条数(MaxItemsPerOp=64),没管单个 BagItem 的
//	                   attrs 条数 —— 单个"格子"可以无限胖。
//	深度有闸、广度无闸:rewardclaim 管住了单条位图大小(MaxBitIndex=128KiB),没管位图
//	                   条目数 —— 每条都不大,但条数无限,整行照样爆(已修)。
//
// 三个信号按灵敏度排序(排查时从上往下走,这就是标准 runbook):
//
//  1. AVG_ROW_LENGTH 突增   → 单行普遍变胖 = 设计性问题(某个 repeated 字段在全体玩家身上涨)
//  2. MAX(LENGTH(col)) 超标 → 个别行畸形 = 个体问题(某个玩家的数据被刷/被 bug 撑爆)
//  3. Top-N 大行的主键      → 定位到"是谁",再 dump 反序列化定位到"哪个字段"
//
// 只看 MAX 会把"全体普遍变胖"误判成"个别异常";只看 AVG 会漏掉"单个玩家被刷爆"。
// 因此本工具同时输出 AVG / MAX / 超阈值行数三个维度。
package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
)

// fieldBudget 是一个大字段的体检预算。
//
// MaxBytes 的取值口径(**关键,定错等于没定**):按**设计期望**定,不按列类型上限定。
// 例:列是 BLOB(65535) 但设计只装 16 个附件 ≈ 2KB,就写 4096;写 65535 等于没设——
// 等数据涨到 60KB 才告警时,业务语义早崩了(附件列表已经无界),而且离 MySQL 报错只剩一步。
type fieldBudget struct {
	DB     string
	Table  string
	Column string
	PK     string // 主键列名,用于 Top-N 定位到具体行;空则不做 Top-N
	// MaxBytes 单字段字节告警线(超过 = 需要人查)。
	MaxBytes int64
	// Why 说明这个数怎么推出来的 + 超了该查什么(进报告,让排查者不用回头翻代码)。
	Why string
}

// bigFields 是全库大字段登记清单(与 CLAUDE.md §9.24 大字段表同步)。
//
// 登记原则:凡 BLOB/TEXT/JSON/VARBINARY 且内容是"集合序列化"(proto repeated / JSON 数组 /
// CSV / 位图)的列都必须登记——它们才是会"越长越胖"的;定长标量列(id/时间戳/枚举)不必。
var bigFields = []fieldBudget{
	{
		DB: "pandora_player", Table: "player_reward_claims", Column: "record", PK: "player_id",
		MaxBytes: 64 * 1024,
		Why: "列是 LONGBLOB(4GB) 但设计只装配置表内的有限档位:永久来源 ≤64 条 + 活动实例 ≤256 条," +
			"单条位图按真实档位数通常 <1KB。超 64KB 说明位图条目数或单条位图异常膨胀——" +
			"查是否有客户端刷任意 source/activity_instance_id(pkg/rewardclaim 条目数上限已落地),或活动未经 EraseActivity 回收",
	},
	{
		DB: "pandora_bag", Table: "bag_section", Column: "section", PK: "player_id",
		MaxBytes: 256 * 1024,
		Why: "列是 MEDIUMBLOB(16MB) 但设计只装单段格子(仓库默认 200 格),每格约 20~200 字节。" +
			"超 256KB 说明格子数或单格 attrs 异常——查 bag_section 容量闸是否被绕过(迁移路径曾传 math.MaxUint32),以及单个 BagItem.attrs 条数",
	},
	{
		// 2026-08-11 修正:此前登记成 `bag_meta`(那张表根本没有 snapshot 列,只有
		// owner_epoch / last_journal_seq),导致 -size-check 对随身组快照**从来没体检过**。
		// bigfield_test.go 的正反双向漂移检查就是为了让这类"登记了但永远匹配不上"当场暴露。
		DB: "pandora_bag", Table: "bag_checkpoint", Column: "snapshot", PK: "player_id",
		MaxBytes: 256 * 1024,
		Why:      "随身组背包快照(pb BagStorageRecord),设计规模同 bag_section;超限排查同上",
	},
	{
		DB: "pandora_bag", Table: "bag_journal", Column: "payload", PK: "id",
		MaxBytes: 32 * 1024,
		Why: "列是 BLOB(65535),单条 journal 设计最多 128 个 item(consume 64 + produce 64)," +
			"无 attrs 时约 4.4KB。超 32KB 说明单个 item 的 attrs 无界(该字段全仓无元素数校验)——直接看 Top-N 行反序列化确认",
	},
	{
		DB: "pandora_social", Table: "player_mail", Column: "payload", PK: "mail_id",
		MaxBytes: 16 * 1024,
		Why: "列是 BLOB(65535),设计装 标题 ≤64 rune + 正文 ≤2048 rune + 附件 ≤16 条 ≈ 8KB 上限。" +
			"超 16KB 说明某个发送方绕过了 MaxTitleLen/MaxBodyLen/MaxAttachments(尤其系统发送方如战斗掉落转邮件)",
	},
	{
		DB: "pandora_trade", Table: "player_item_instance", Column: "attributes", PK: "instance_id",
		MaxBytes: 768,
		Why: "列是 VARBINARY(1024),装 pb ItemInstanceAttributesStorageRecord。词条由鉴定 roll 写入," +
			"条数受 identify_rules.attr_count 约束(通常个位数 ≈ 几十字节)。超 768(75%)即逼近列上限——查鉴定规则配置是否失控或有人直写",
	},
	{
		DB: "pandora_trade", Table: "mail_transfer_escrow", Column: "attributes", PK: "instance_id",
		MaxBytes: 768,
		Why:      "托管行原样搬运 player_item_instance.attributes(同为 VARBINARY(1024) 存 pb),阀值与源表一致;超限先查源表",
	},
	{
		DB: "pandora_trade", Table: "inventory_ledger", Column: "result_currencies", PK: "id",
		MaxBytes: 192,
		Why: "列是 VARBINARY(256),装 pb CurrencyBalancesStorageRecord(首次执行后的多币种余额快照,幂等重放原样回放)。" +
			"条目数被 CurrencyKind 枚举取值数封顶(当前 3 种,单项约十几字节),正常在百字节以内。" +
			"超 192(75%)即逼近列上限——写满会让整条流水写失败、重放拿不回首次结果,查是否新增了大量币种或有人绕过 kind 校验直写",
	},
	{
		DB: "pandora_trade", Table: "inventory_ledger", Column: "result_currency_delta", PK: "id",
		MaxBytes: 192,
		Why:      "同表 result_currencies 的变动额侧(同为 VARBINARY(256) 装同一 pb),条目数上限与阀值一致;超限先按 result_currencies 排查",
	},
	{
		DB: "pandora_leaderboard", Table: "leaderboard_reward_log", Column: "reward_pb", PK: "id",
		MaxBytes: 1536,
		Why:      "列是 VARBINARY(2048),装 pb RewardGrantStorageRecord(单档奖励明细 + 补发重放入参)。超 1536(75%)即逼近列上限,再涨会拒写整条发奖记录——查 RewardTier.items 条数是否有上限",
	},
	{
		DB: "pandora_battle", Table: "match_release_outbox", Column: "payload", PK: "id",
		MaxBytes: 768,
		Why: "列是 VARBINARY(1024),装 MatchReleaseStorageRecord(含 repeated player_ids)。" +
			"超 768(75%)即逼近列上限——队伍规模(如 5v5→10v10)变大会撑爆,超限会让整场结算的释放出箱写失败",
	},
	{
		DB: "pandora_battle", Table: "battle_exit_proof_outbox", Column: "payload", PK: "id",
		MaxBytes: 1536,
		Why:      "列是 VARBINARY(2048),装每玩家 Battle→Hub 终态 proof。超 1536(75%)即逼近列上限",
	},
	{
		DB: "pandora_mission", Table: "player_mission_active", Column: "progress", PK: "id",
		MaxBytes: 192,
		Why: "列是 VARBINARY(256),装 pb MissionProgressStorageRecord(repeated uint32 每条件槽一个)。" +
			"槽数上限 configtable.MaxMissionConditionSlots=8,满槽满值约 48 字节。" +
			"超 192(75%)即逼近列上限——查槽数上限是否被绕过(热更加条件后旧行槽数会跟着涨)",
	},
	{
		DB: "pandora_mission", Table: "mission_reward_log", Column: "reward_pb", PK: "id",
		MaxBytes: 1536,
		Why: "列是 VARBINARY(2048),装 pb MissionRewardStorageRecord(补发重放唯一入参)。" +
			"超 1536(75%)即逼近列上限,再涨会拒写整条发奖流水——查奖励表单行 item_ids 条数" +
			"(与 leaderboard_reward_log 同型同阈值)",
	},
	{
		DB: "pandora_mission", Table: "mission_push_outbox", Column: "payload", PK: "id",
		MaxBytes: 1536,
		Why: "列是 VARBINARY(2048),装 pb MissionUpdateEvent 分片。biz 侧 marshalEventChunks 已按" +
			"软上限 1800B / 每片 6 条任务分片,正常远小于此。超 1536 说明分片粒度失效——" +
			"查单条 ActiveMission 的 progress/targets 槽数是否异常",
	},
}

// fieldStat 是一个字段的体检结果。
type fieldStat struct {
	budget   fieldBudget
	rows     int64
	maxBytes int64
	avgBytes int64
	overRows int64 // 超预算的行数(不只是"有没有超",还要知道"多少行超")
	err      error
}

// checkBigFields 对登记的大字段跑体检(**全表扫描 MAX/AVG,成本高**,仅 -size-check 触发)。
// 表不存在(功能未部署)静默跳过。返回按"超限严重程度"排序的结果。
func checkBigFields(ctx context.Context, db *sql.DB, present map[string]map[string]bool) []fieldStat {
	var out []fieldStat
	for _, b := range bigFields {
		if !present[b.DB][b.Table] {
			continue
		}
		s := fieldStat{budget: b}
		q := fmt.Sprintf(
			"SELECT COUNT(*), COALESCE(MAX(LENGTH(`%s`)),0), COALESCE(AVG(LENGTH(`%s`)),0), "+
				"COALESCE(SUM(CASE WHEN LENGTH(`%s`) > ? THEN 1 ELSE 0 END),0) FROM `%s`.`%s`",
			b.Column, b.Column, b.Column, b.DB, b.Table)
		var avg sql.NullFloat64
		if err := db.QueryRowContext(ctx, q, b.MaxBytes).Scan(&s.rows, &s.maxBytes, &avg, &s.overRows); err != nil {
			s.err = err
		}
		s.avgBytes = int64(avg.Float64)
		out = append(out, s)
	}
	// 超限的排前面,其次按"离预算多近"降序——让最该看的排最上面。
	sort.SliceStable(out, func(i, j int) bool {
		ri := float64(out[i].maxBytes) / float64(max64(out[i].budget.MaxBytes, 1))
		rj := float64(out[j].maxBytes) / float64(max64(out[j].budget.MaxBytes, 1))
		return ri > rj
	})
	return out
}

// topLargeRows 定位某字段最大的 N 行(主键 + 字节数)——排查的最后一跳:定位到"是谁"。
// 拿到主键后的标准下一步:把该行 dump 出来反序列化,看是哪个 repeated 字段条数异常。
func topLargeRows(ctx context.Context, db *sql.DB, b fieldBudget, limit int) ([]struct {
	PK    string
	Bytes int64
}, error) {
	if b.PK == "" {
		return nil, nil
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		"SELECT CAST(`%s` AS CHAR), LENGTH(`%s`) AS n FROM `%s`.`%s` ORDER BY n DESC LIMIT ?",
		b.PK, b.Column, b.DB, b.Table), limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []struct {
		PK    string
		Bytes int64
	}
	for rows.Next() {
		var r struct {
			PK    string
			Bytes int64
		}
		var n sql.NullInt64
		if serr := rows.Scan(&r.PK, &n); serr != nil {
			return nil, serr
		}
		r.Bytes = n.Int64
		out = append(out, r)
	}
	return out, rows.Err()
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
