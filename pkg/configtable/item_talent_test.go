// item_talent_test.go — 道具表 / 专精表的私有校验与域方法单测(2026-07-25)。
//
// 这两张表是 player.SetEquipment / SetTalents 的权威判定来源,
// 因此重点在"坏表必须拒绝加载"和"非法分配必须拒绝"两类分支。
package configtable

import (
	"strings"
	"testing"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

func TestValidateItemRow(t *testing.T) {
	base := func() *configpb.ItemRow {
		return &configpb.ItemRow{
			Id: 10001, Name: "消耗品", Type: configpb.ItemType_ITEM_TYPE_CONSUMABLE,
			MaxStackSize: 99, Usable: true, UseHealHp: 50,
		}
	}

	if err := validateItemRow(base()); err != nil {
		t.Fatalf("合法的旧版固定回血行不应报错: %v", err)
	}
	explicitFixed := base()
	explicitFixed.UseHealType = configpb.ItemHealType_ITEM_HEAL_TYPE_FIXED
	if err := validateItemRow(explicitFixed); err != nil {
		t.Fatalf("合法的显式固定回血行不应报错: %v", err)
	}
	percent := base()
	percent.UseHealType = configpb.ItemHealType_ITEM_HEAL_TYPE_MAX_HP_PERCENT
	percent.UseHealHp = 0
	percent.UseHealMaxHpPercent = 25
	if err := validateItemRow(percent); err != nil {
		t.Fatalf("合法的最大生命百分比回血行不应报错: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*configpb.ItemRow)
		expect string
	}{
		{"名称为空", func(r *configpb.ItemRow) { r.Name = "" }, "名称"},
		{"类型未填", func(r *configpb.ItemRow) { r.Type = configpb.ItemType_ITEM_TYPE_UNSPECIFIED }, "类型"},
		{"堆叠上限为 0", func(r *configpb.ItemRow) { r.MaxStackSize = 0 }, "堆叠上限"},
		{
			// 类型说是装备但没填部位 → SetEquipment 的两项校验会互相矛盾。
			"装备类型但无部位",
			func(r *configpb.ItemRow) { r.Type = configpb.ItemType_ITEM_TYPE_EQUIPMENT; r.MaxStackSize = 1 },
			"装备类不一致",
		},
		{
			"有部位但类型不是装备",
			func(r *configpb.ItemRow) { r.EquipSlot = 1; r.MaxStackSize = 1 },
			"装备类不一致",
		},
		{
			// 装备可堆叠会在合并时改写实例 guid,破坏强化 / 词条跟随。
			"装备可堆叠",
			func(r *configpb.ItemRow) {
				r.Type = configpb.ItemType_ITEM_TYPE_EQUIPMENT
				r.EquipSlot = 1
				r.MaxStackSize = 2
				r.Usable = false
				r.UseHealHp = 0
			},
			"堆叠上限必须为 1",
		},
		{"可使用但回血 0", func(r *configpb.ItemRow) { r.UseHealHp = 0 }, "使用回血量"},
		{
			"固定类型误填百分比",
			func(r *configpb.ItemRow) { r.UseHealMaxHpPercent = 20 },
			"固定回血类型",
		},
		{
			"固定回血超过客户端上限",
			func(r *configpb.ItemRow) { r.UseHealHp = maxClientFixedHealHP + 1 },
			"int32 上限",
		},
		{
			"不可使用却配置固定回血",
			func(r *configpb.ItemRow) { r.Usable = false },
			"不可使用道具",
		},
		{
			"百分比类型仍填固定值",
			func(r *configpb.ItemRow) {
				r.UseHealType = configpb.ItemHealType_ITEM_HEAL_TYPE_MAX_HP_PERCENT
				r.UseHealMaxHpPercent = 20
			},
			"固定使用回血量",
		},
		{
			"百分比为 0",
			func(r *configpb.ItemRow) {
				r.UseHealType = configpb.ItemHealType_ITEM_HEAL_TYPE_MAX_HP_PERCENT
				r.UseHealHp = 0
			},
			"1..100",
		},
		{
			"百分比超过 100",
			func(r *configpb.ItemRow) {
				r.UseHealType = configpb.ItemHealType_ITEM_HEAL_TYPE_MAX_HP_PERCENT
				r.UseHealHp = 0
				r.UseHealMaxHpPercent = 101
			},
			"1..100",
		},
		{
			"百分比回血不可使用",
			func(r *configpb.ItemRow) {
				r.UseHealType = configpb.ItemHealType_ITEM_HEAL_TYPE_MAX_HP_PERCENT
				r.UseHealHp = 0
				r.UseHealMaxHpPercent = 20
				r.Usable = false
			},
			"可使用",
		},
		{
			"未知回血类型",
			func(r *configpb.ItemRow) { r.UseHealType = configpb.ItemHealType(99) },
			"未知使用回血类型",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			row := base()
			c.mutate(row)
			err := validateItemRow(row)
			if err == nil {
				t.Fatal("应报错")
			}
			if !strings.Contains(err.Error(), c.expect) {
				t.Fatalf("错误信息应含 %q, got %v", c.expect, err)
			}
		})
	}
}

// newItemTableForTest 直接走生成的构造函数,顺带覆盖 newItemTable 的主键 / 校验链路。
func newItemTableForTest(t *testing.T, rows ...*configpb.ItemRow) *ItemTable {
	t.Helper()
	tbl, err := newItemTable(&configpb.ItemTableData{Rows: rows})
	if err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	return tbl
}

func TestItemTableSlotQueries(t *testing.T) {
	tbl := newItemTableForTest(t,
		&configpb.ItemRow{Id: 1, Name: "剑", Type: configpb.ItemType_ITEM_TYPE_EQUIPMENT, MaxStackSize: 1, EquipSlot: 1, IdentifyPoolId: 1},
		&configpb.ItemRow{Id: 2, Name: "药", Type: configpb.ItemType_ITEM_TYPE_CONSUMABLE, MaxStackSize: 99, Usable: true, UseHealHp: 10},
	)

	if !tbl.IsEquipment(1) || tbl.EquipSlotOf(1) != 1 {
		t.Fatal("1 应为部位 1 的装备")
	}
	if tbl.IsEquipment(2) || tbl.EquipSlotOf(2) != 0 {
		t.Fatal("2 不应可穿戴")
	}
	if !tbl.MatchesSlot(1, 1) {
		t.Fatal("1 应匹配部位 1")
	}
	if tbl.MatchesSlot(1, 2) {
		t.Fatal("1 不应匹配部位 2")
	}
	// 未知道具与 slot 0 都必须 fail-closed:热更缺行不能变成放行后门。
	if tbl.MatchesSlot(999, 1) {
		t.Fatal("未知道具应判不匹配")
	}
	if tbl.MatchesSlot(1, 0) {
		t.Fatal("slot 0 应判不匹配")
	}
	if tbl.IsEquipment(999) || tbl.EquipSlotOf(999) != 0 {
		t.Fatal("未知道具应判不可穿戴且部位为 0")
	}
}

func TestValidateTalentRow(t *testing.T) {
	base := func() *configpb.TalentRow {
		return &configpb.TalentRow{Id: 1, Name: "强击", MaxLevel: 5, CostPerLevel: 1}
	}
	if err := validateTalentRow(base()); err != nil {
		t.Fatalf("合法行不应报错: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*configpb.TalentRow)
		expect string
	}{
		{"名称为空", func(r *configpb.TalentRow) { r.Name = "" }, "名称"},
		{"等级上限为 0", func(r *configpb.TalentRow) { r.MaxLevel = 0 }, "等级上限"},
		{"等级上限超硬约束", func(r *configpb.TalentRow) { r.MaxLevel = MaxTalentLevel + 1 }, "超过上限"},
		{"每级消耗为 0", func(r *configpb.TalentRow) { r.CostPerLevel = 0 }, "每级消耗"},
		{"每级消耗超硬约束", func(r *configpb.TalentRow) { r.CostPerLevel = MaxTalentCostPerLevel + 1 }, "超过上限"},
		{"前置指向自身", func(r *configpb.TalentRow) { r.RequireTalentId = 1; r.RequireTalentLevel = 1 }, "指向自身"},
		{"无前置却填前置等级", func(r *configpb.TalentRow) { r.RequireTalentLevel = 2 }, "前置等级"},
		{"有前置却未填等级", func(r *configpb.TalentRow) { r.RequireTalentId = 9 }, "未填前置等级"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			row := base()
			c.mutate(row)
			err := validateTalentRow(row)
			if err == nil {
				t.Fatal("应报错")
			}
			if !strings.Contains(err.Error(), c.expect) {
				t.Fatalf("错误信息应含 %q, got %v", c.expect, err)
			}
		})
	}
}

func newTalentTableForTest(t *testing.T, rows ...*configpb.TalentRow) *TalentTable {
	t.Helper()
	tbl, err := newTalentTable(&configpb.TalentTableData{Rows: rows})
	if err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	return tbl
}

func TestTalentValidateTree(t *testing.T) {
	ok := newTalentTableForTest(t,
		&configpb.TalentRow{Id: 1, Name: "A", MaxLevel: 5, CostPerLevel: 1},
		&configpb.TalentRow{Id: 2, Name: "B", MaxLevel: 3, CostPerLevel: 1, RequireTalentId: 1, RequireTalentLevel: 3},
	)
	if err := ok.ValidateTree(); err != nil {
		t.Fatalf("合法树不应报错: %v", err)
	}

	missing := newTalentTableForTest(t,
		&configpb.TalentRow{Id: 2, Name: "B", MaxLevel: 3, CostPerLevel: 1, RequireTalentId: 99, RequireTalentLevel: 1},
	)
	if err := missing.ValidateTree(); err == nil || !strings.Contains(err.Error(), "不存在") {
		t.Fatalf("前置缺失应报错: %v", err)
	}

	// 前置要求 4 级但前置上限只有 3 级 → 该节点永远点不出来,属配置事故。
	unreachable := newTalentTableForTest(t,
		&configpb.TalentRow{Id: 1, Name: "A", MaxLevel: 3, CostPerLevel: 1},
		&configpb.TalentRow{Id: 2, Name: "B", MaxLevel: 3, CostPerLevel: 1, RequireTalentId: 1, RequireTalentLevel: 4},
	)
	if err := unreachable.ValidateTree(); err == nil || !strings.Contains(err.Error(), "永远点不出来") {
		t.Fatalf("前置等级超前置上限应报错: %v", err)
	}

	// 成环:1 依赖 2、2 依赖 1(单行自环已由 validateTalentRow 挡住,这里验跨行环)。
	cyclic := newTalentTableForTest(t,
		&configpb.TalentRow{Id: 1, Name: "A", MaxLevel: 3, CostPerLevel: 1, RequireTalentId: 2, RequireTalentLevel: 1},
		&configpb.TalentRow{Id: 2, Name: "B", MaxLevel: 3, CostPerLevel: 1, RequireTalentId: 1, RequireTalentLevel: 1},
	)
	if err := cyclic.ValidateTree(); err == nil || !strings.Contains(err.Error(), "成环") {
		t.Fatalf("依赖成环应报错: %v", err)
	}
}

func TestTalentValidateAllocation(t *testing.T) {
	tbl := newTalentTableForTest(t,
		&configpb.TalentRow{Id: 1, Name: "A", MaxLevel: 5, CostPerLevel: 1},
		&configpb.TalentRow{Id: 2, Name: "B", MaxLevel: 3, CostPerLevel: 2, RequireTalentId: 1, RequireTalentLevel: 2},
	)

	// 总消耗 = 2×1 + 2×2 = 6,不是等级和 4。
	costs, cost, err := tbl.ValidateAllocation(map[uint32]uint32{1: 2, 2: 2})
	if err != nil {
		t.Fatalf("合法分配不应报错: %v", err)
	}
	if cost != 6 {
		t.Fatalf("总消耗应为 6(按每级消耗), got %d", cost)
	}
	// 逐节点消耗要落库供读取侧还原已花点数,必须逐条对上而不只对总数。
	if costs[1] != 2 || costs[2] != 4 {
		t.Fatalf("逐节点消耗应为 {1:2, 2:4}, got %v", costs)
	}

	if _, _, err := tbl.ValidateAllocation(map[uint32]uint32{99: 1}); err == nil {
		t.Fatal("未知节点应被拒")
	}
	if _, _, err := tbl.ValidateAllocation(map[uint32]uint32{1: 6}); err == nil {
		t.Fatal("超等级上限应被拒")
	}
	if _, _, err := tbl.ValidateAllocation(map[uint32]uint32{1: 0}); err == nil {
		t.Fatal("等级 0 应被拒(应由调用方剔除)")
	}
	// 前置只看本次方案:1 只有 1 级,不满足 2 要求的 2 级。
	if _, _, err := tbl.ValidateAllocation(map[uint32]uint32{1: 1, 2: 1}); err == nil {
		t.Fatal("前置未达标应被拒")
	}
	// 完全不点前置同样要拒,否则"先点满前置再洗掉前置"能留下悬空节点。
	if _, _, err := tbl.ValidateAllocation(map[uint32]uint32{2: 1}); err == nil {
		t.Fatal("缺前置应被拒")
	}
	// 空分配是合法的"清空",总消耗 0。
	if _, cost, err := tbl.ValidateAllocation(nil); err != nil || cost != 0 {
		t.Fatalf("空分配应返回 (0, nil), got (%d, %v)", cost, err)
	}
}
