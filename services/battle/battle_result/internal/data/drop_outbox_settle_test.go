// drop_outbox_settle_test.go — 结算路径掉落出箱入箱判据的纯函数回归(2026-08-24 P0)。
//
// 这一层刻意与 MySQL 解耦:出事的不是 SQL,而是"哪些行该写、写什么内容"的判据本身
// (抑制条件误伤货币),而该判据此前只存在于 SaveResult 事务体内,没有任何一处能直接观测。
package data

import (
	"reflect"
	"testing"
)

// TestSettleDropOutboxRow_SuppressionOnlyCoversItems 抑制只作用于道具,货币必须照常入箱。
//
// 这是 2026-08-24 P0 的核心不变量:实时进度通道明确只发道具不发金币
// (biz/progress.go 的 GrantItems 第三实参硬编码 0),所以"本场走过实时通道"绝不能
// 成为整局金币不入箱的理由 —— 否则两处代码互相指望对方发金币,金币永久丢失且零错误日志。
func TestSettleDropOutboxRow_SuppressionOnlyCoversItems(t *testing.T) {
	t.Run("纯货币行在抑制下照常入箱", func(t *testing.T) {
		row, keep := SettleDropOutboxRow(DropOutboxRecord{PlayerID: 7, CurrencyAmount: 250}, true)
		if !keep {
			t.Fatalf("走过实时通道的对局金币被整条吞掉(P0 回归)")
		}
		if row.CurrencyAmount != 250 || row.PlayerID != 7 {
			t.Fatalf("入箱行内容不符: %+v", row)
		}
	})

	t.Run("道具+货币混合行在抑制下只剩货币", func(t *testing.T) {
		in := DropOutboxRecord{
			PlayerID:              7,
			ItemConfigIDs:         []uint32{5001, 5002},
			StackItemConfigIDs:    []uint32{5002},
			InstanceItemConfigIDs: []uint32{5001},
			CurrencyAmount:        300,
		}
		row, keep := SettleDropOutboxRow(in, true)
		if !keep {
			t.Fatalf("混合行的货币部分被吞掉")
		}
		if row.CurrencyAmount != 300 {
			t.Fatalf("货币应保留, got %d", row.CurrencyAmount)
		}
		if len(row.ItemConfigIDs) != 0 || len(row.StackItemConfigIDs) != 0 || len(row.InstanceItemConfigIDs) != 0 {
			t.Fatalf("已被实时通道发过的道具不得再入箱(双发): %+v", row)
		}
		// 入参不得被就地改写:调用方(SaveResult)后续日志/审计还要读原始行。
		if len(in.ItemConfigIDs) != 2 || len(in.InstanceItemConfigIDs) != 1 {
			t.Fatalf("入参被就地改写: %+v", in)
		}
	})

	t.Run("纯道具行在抑制下整条跳过", func(t *testing.T) {
		in := DropOutboxRecord{PlayerID: 7, ItemConfigIDs: []uint32{5001}, InstanceItemConfigIDs: []uint32{5001}}
		if _, keep := SettleDropOutboxRow(in, true); keep {
			t.Fatalf("实时通道已发过的道具必须被抑制,不得双发")
		}
	})
}

// TestSettleDropOutboxRow_KeepsRowsWithOnlyRouteColumns 只填了路由列、ItemConfigIDs 留空的行
// 也必须成行 —— 成行判据看的是**三列并集**。
//
// 2026-08-24 收口:旧判据只看 `len(ItemConfigIDs) == 0 && CurrencyAmount == 0`。
// 今天全仓唯一生产者 buildDropOutbox 维持着 ItemConfigIDs ⊇ Stack ∪ Instance,所以两种判据
// 行为完全一致 —— 但本函数是**导出**的,任何新增生产者只填 Stack/Instance 两列(比如把
// ItemConfigIDs 当"历史兼容字段"),就会被整行静默丢弃:出箱表没有行、日志没有错误、
// 玩家掉落凭空消失。上一轮只把这个陷阱写进了注释;本轮把它消除掉。
//
// 把 SettleDropOutboxRow 的判据改回只看 ItemConfigIDs,这条会变红。
func TestSettleDropOutboxRow_KeepsRowsWithOnlyRouteColumns(t *testing.T) {
	t.Run("只有 instance 列", func(t *testing.T) {
		in := DropOutboxRecord{PlayerID: 11, InstanceItemConfigIDs: []uint32{5001}}
		row, keep := SettleDropOutboxRow(in, false)
		if !keep {
			t.Fatalf("只填路由列的行被静默整条丢弃(零日志漏发)")
		}
		if !reflect.DeepEqual(row, in) {
			t.Fatalf("未抑制时应原样入箱: got=%+v want=%+v", row, in)
		}
	})

	t.Run("只有 stack 列", func(t *testing.T) {
		in := DropOutboxRecord{PlayerID: 11, StackItemConfigIDs: []uint32{5002}}
		if _, keep := SettleDropOutboxRow(in, false); !keep {
			t.Fatalf("只填路由列的行被静默整条丢弃(零日志漏发)")
		}
	})

	t.Run("抑制仍然掐掉这些行", func(t *testing.T) {
		// 并集判据只影响"该不该成行",不放松抑制:道具三列被清空后整行零收益 → 不成行。
		in := DropOutboxRecord{PlayerID: 11, StackItemConfigIDs: []uint32{5002}, InstanceItemConfigIDs: []uint32{5001}}
		if _, keep := SettleDropOutboxRow(in, true); keep {
			t.Fatalf("实时通道已发过的道具必须被抑制,不得双发")
		}
	})
}

// TestSettleDropOutboxRow_NotSuppressed 未走实时通道:内容原样入箱,零收益行仍跳过。
func TestSettleDropOutboxRow_NotSuppressed(t *testing.T) {
	in := DropOutboxRecord{
		PlayerID:              9,
		ItemConfigIDs:         []uint32{5001},
		InstanceItemConfigIDs: []uint32{5001},
		CurrencyAmount:        10,
	}
	row, keep := SettleDropOutboxRow(in, false)
	if !keep {
		t.Fatalf("未抑制的收益行必须入箱")
	}
	if !reflect.DeepEqual(row, in) {
		t.Fatalf("未抑制时应原样入箱: got=%+v want=%+v", row, in)
	}
	if _, keep := SettleDropOutboxRow(DropOutboxRecord{PlayerID: 9}, false); keep {
		t.Fatalf("零收益行不该占一行出箱")
	}
}
