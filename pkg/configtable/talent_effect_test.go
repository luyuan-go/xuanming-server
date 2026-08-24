package configtable

import (
	"strings"
	"testing"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

func newTalentEffectTableForTest(t *testing.T, rows ...*configpb.TalentEffectRow) *TalentEffectTable {
	t.Helper()
	tbl, err := newTalentEffectTable(&configpb.TalentEffectTableData{Rows: rows})
	if err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	return tbl
}

// TestTalentEffectAttrKeyWhitelist 钉住"拼错属性键必须在加载期被拒"。
// 拼错的键在 DS 上表现为天赋点了没反应,既不报错也不崩,只能靠加载期白名单挡。
// ⚠️ UMyEntityAttrSet 增删属性时,本用例的合法键与 combatAttrKeys 须一起改。
func TestTalentEffectAttrKeyWhitelist(t *testing.T) {
	_, err := newTalentEffectTable(&configpb.TalentEffectTableData{Rows: []*configpb.TalentEffectRow{
		// "Attack" 不是 GAS 属性名(正确写法是 Atk)。
		{Id: 1, TalentId: 1, AttrKey: "Attack", ValuePerLevel: 3},
	}})
	if err == nil {
		t.Fatal("非法属性键应被拒")
	}
	if !strings.Contains(err.Error(), "attr_key") {
		t.Fatalf("错误信息应点名 attr_key 便于定位: %v", err)
	}

	// 白名单内的键全部要能过,避免白名单写漏把合法配置挡在门外。
	for key := range combatAttrKeys {
		if _, err := newTalentEffectTable(&configpb.TalentEffectTableData{Rows: []*configpb.TalentEffectRow{
			{Id: 1, TalentId: 1, AttrKey: key, ValuePerLevel: 1},
		}}); err != nil {
			t.Fatalf("白名单属性 %s 应放行: %v", key, err)
		}
	}
}

func TestTalentEffectRejectsZeroAndOversizedValue(t *testing.T) {
	if _, err := newTalentEffectTable(&configpb.TalentEffectTableData{Rows: []*configpb.TalentEffectRow{
		{Id: 1, TalentId: 1, AttrKey: "Atk", ValuePerLevel: 0},
	}}); err == nil {
		t.Fatal("每级数值为 0 的效果行应被拒(整行没有作用)")
	}
	if _, err := newTalentEffectTable(&configpb.TalentEffectTableData{Rows: []*configpb.TalentEffectRow{
		{Id: 1, TalentId: 1, AttrKey: "Hp", ValuePerLevel: MaxTalentEffectValuePerLevel + 1},
	}}); err == nil {
		t.Fatal("超上限的每级数值应被拒(疑似多打零)")
	}
	// 负数是合法设计(减益型专精),只挡绝对值超限。
	if _, err := newTalentEffectTable(&configpb.TalentEffectTableData{Rows: []*configpb.TalentEffectRow{
		{Id: 1, TalentId: 1, AttrKey: "MoveSpeedRate", ValuePerLevel: -0.05},
	}}); err != nil {
		t.Fatalf("负数加成应放行: %v", err)
	}
}

// TestTalentEffectValidateEffects 覆盖跨行约束:同一专精对同一属性配两行会让加成翻倍。
func TestTalentEffectValidateEffects(t *testing.T) {
	dup := newTalentEffectTableForTest(t,
		&configpb.TalentEffectRow{Id: 1, TalentId: 8, AttrKey: "Atk", ValuePerLevel: 2},
		&configpb.TalentEffectRow{Id: 2, TalentId: 8, AttrKey: "Atk", ValuePerLevel: 3},
	)
	if err := dup.ValidateEffects(); err == nil {
		t.Fatal("同一专精对同一属性配多行应被拒")
	}

	// 同一专精配不同属性(专精 8「全部基础属性」的正常形态)必须放行。
	ok := newTalentEffectTableForTest(t,
		&configpb.TalentEffectRow{Id: 1, TalentId: 8, AttrKey: "Atk", ValuePerLevel: 2},
		&configpb.TalentEffectRow{Id: 2, TalentId: 8, AttrKey: "Hp", ValuePerLevel: 15},
		&configpb.TalentEffectRow{Id: 3, TalentId: 1, AttrKey: "Atk", ValuePerLevel: 3},
	)
	if err := ok.ValidateEffects(); err != nil {
		t.Fatalf("同专精不同属性 / 不同专精同属性应放行: %v", err)
	}
}

func TestTalentEffectResolveBonuses(t *testing.T) {
	tbl := newTalentEffectTableForTest(t,
		&configpb.TalentEffectRow{Id: 1, TalentId: 1, AttrKey: "Atk", ValuePerLevel: 3},
		&configpb.TalentEffectRow{Id: 2, TalentId: 8, AttrKey: "Atk", ValuePerLevel: 2},
		&configpb.TalentEffectRow{Id: 3, TalentId: 8, AttrKey: "Hp", ValuePerLevel: 15},
	)

	// 专精 1 点 3 级 → Atk 9;专精 8 点 1 级 → Atk 2 + Hp 15。Atk 跨节点累加成 11。
	got := tbl.ResolveBonuses(map[uint32]uint32{1: 3, 8: 1})
	if len(got) != 2 {
		t.Fatalf("应聚合成 2 条属性加成, got %v", got)
	}
	// 结果按属性名定序:Atk 在 Hp 前。
	if got[0].AttrKey != "Atk" || got[0].Value != 11 {
		t.Fatalf("Atk 应为 3×3 + 2×1 = 11, got %v", got[0])
	}
	if got[1].AttrKey != "Hp" || got[1].Value != 15 {
		t.Fatalf("Hp 应为 15×1 = 15, got %v", got[1])
	}

	// 0 级节点与无效果行的节点都不该产出加成,也不该报错(纯解锁型节点合法)。
	if bonuses := tbl.ResolveBonuses(map[uint32]uint32{1: 0, 999: 5}); bonuses != nil {
		t.Fatalf("0 级与无效果节点不应产出加成, got %v", bonuses)
	}
}
