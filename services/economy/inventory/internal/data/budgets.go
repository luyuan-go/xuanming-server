// budgets.go — inventory 服务的库容量预算(CLAUDE.md §9.24,2026-07-22)。
//
// 服务启动时与周期巡检时对照本预算检查自己负责的表:超限打 ERROR 日志 + metric,
// **不阻止启动**(容量超限是"要去查的问题",不是"服务不能跑的理由";拒绝启动会把容量
// 问题升级成可用性事故,违反验收底线第 1 条)。
//
// 预算怎么定(必须能说明推导依据,不准拍脑袋):
//   - 有保留期的只增表:峰值写入速率 × 保留期 × 3 倍安全系数。超了 = 清理没追上写入,
//     或写入速率超出容量规划——两种都要人查。
//   - 按玩家有界的表:预期玩家数 × 每玩家行数 × 3。
//   - MaxAvgRowBytes:定长列之和 + blob 的**设计期望大小**(不是列类型上限)。
//     写成列类型上限等于没设——那样只有 MySQL 快报错时才告警,失去预警意义。
package data

import "github.com/luyuancpp/pandora/pkg/dbguard"

// 容量规划基数(docs/design/stress-discipline.md 口径:大厅 500 人/实例)。
// 预算按 10 万注册玩家 / 1 万日活推算,留 3 倍安全系数;真实规模变化时改这里一处。
const (
	planPlayers = 100_000 // 注册玩家量级
	planDAU     = 10_000  // 日活量级
)

// TradeBudgets 是 pandora_trade 库(货币 / 道具 / 流水 / 托管)的容量预算。
func TradeBudgets() []dbguard.TableBudget {
	return []dbguard.TableBudget{
		{
			// 多币种钱包:一玩家一币种一行(CurrencyKind 当前 3 种),故按 planPlayers×3 计,
			// 再乘既有的 3 倍余量口径(与 player_items 的 planPlayers*200*3 同形)。
			Table: "player_wallet", MaxRows: planPlayers * 3 * 3, MaxAvgRowBytes: 128,
			Note: "每玩家每币种一行;超限说明玩家量级超出容量规划或币种枚举失控",
		},
		{
			Table: "player_items", MaxRows: planPlayers * 200 * 3, MaxAvgRowBytes: 128,
			Note: "行数 ≈ 玩家数 × 持有过的道具配置种类;超限查是否有人刷任意 item_config_id",
		},
		{
			Table: "player_item_instance", MaxRows: planPlayers * 200 * 3, MaxAvgRowBytes: 512,
			Note: "行数 ≈ 玩家数 × 背包容量;avg_row 超限查 attributes(pb)词条数(应受 identify_rules.attr_count 约束)",
		},
		{
			// 保留期 90 天;按日活 1 万 × 每人每天 20 笔 × 90 天 × 3 ≈ 5400 万。
			Table: "inventory_ledger", MaxRows: planDAU * 20 * 90 * 3, MaxAvgRowBytes: 512,
			Note: "幂等流水,保留期 90 天;超限先查 inventory sweep 是否在跑(日志 inventory_sweep_ledger),再查写入速率是否异常",
		},
		{
			Table: "auction_escrow", MaxRows: planPlayers * 200 * 3, MaxAvgRowBytes: 256,
			Note: "挂单托管;active 行不清理,超限查是否有大量挂单未终结(escrow 泄漏)",
		},
	}
}

// BagBudgets 是 pandora_bag 库(背包域)的容量预算。
//
// 这里的 MaxAvgRowBytes 尤其重要:三个 blob 列都是"集合序列化",是典型的
// **深度失控**风险点(行数正常、单行越来越胖)。avg_row_bytes 突增是最早的信号。
func BagBudgets() []dbguard.TableBudget {
	return []dbguard.TableBudget{
		{Table: "bag_meta", MaxRows: planPlayers * 3, MaxAvgRowBytes: 8 * 1024,
			Note: "snapshot 是随身组快照(MEDIUMBLOB);avg_row 超 8KB 查格子数与单格 attrs 条数"},
		{Table: "bag_checkpoint", MaxRows: planPlayers * 3, MaxAvgRowBytes: 1024},
		{Table: "bag_section", MaxRows: planPlayers * 8 * 3, MaxAvgRowBytes: 16 * 1024,
			Note: "section 是单段全部格子(MEDIUMBLOB,仓库默认 200 格);avg_row 超 16KB 说明容量闸被绕过或单格 attrs 无界"},
		{
			// 保留期 90 天;按日活 1 万 × 每人每天 50 条 × 90 天 × 3 ≈ 1.35 亿(journal 写入最密集)。
			Table: "bag_journal", MaxRows: planDAU * 50 * 90 * 3, MaxAvgRowBytes: 2 * 1024,
			Note: "单条 journal payload(BLOB);avg_row 超 2KB 查单条 op 的 item 数与单个 item 的 attrs 数",
		},
		{Table: "bag_generation", MaxRows: 10_000, MaxAvgRowBytes: 256,
			Note: "每活动段一行,运营配置驱动;超限查是否有活动段只增不回收"},
	}
}

// TradeBigFields / BagBigFields 是列级字节预算(全表扫描,**不放周期路径**;
// 由 dbcheck -size-check 或表级 avg_row_bytes 告警后人工触发)。
func TradeBigFields() []dbguard.ColumnBudget {
	return []dbguard.ColumnBudget{
		{Table: "player_item_instance", Column: "attributes", MaxBytes: 768,
			Note: "鉴定词条(pb ItemInstanceAttributesStorageRecord,列 VARBINARY(1024));768=75% 预警线,超限查 identify_rules.attr_count 配置"},
		{Table: "mail_transfer_escrow", Column: "attributes", MaxBytes: 768,
			Note: "托管行原样搬运源表词条,阈值与 player_item_instance.attributes 一致"},
	}
}

func BagBigFields() []dbguard.ColumnBudget {
	return []dbguard.ColumnBudget{
		{Table: "bag_section", Column: "section", MaxBytes: 256 * 1024,
			Note: "单段格子集合;超限用 dbcheck -size-check -top-rows 定位到 player_id 再反序列化看哪个格子胖"},
		{Table: "bag_meta", Column: "snapshot", MaxBytes: 256 * 1024},
		{Table: "bag_journal", Column: "payload", MaxBytes: 32 * 1024,
			Note: "单条 journal;超限说明单个 BagItem.attrs 无界(该字段目前无元素数校验)"},
	}
}
