package configtable

import (
	"strings"
	"testing"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

func newSkillCardEffectTableForTest(t *testing.T, rows ...*configpb.SkillCardEffectRow) *SkillCardEffectTable {
	t.Helper()
	tbl, err := newSkillCardEffectTable(&configpb.SkillCardEffectTableData{Rows: rows})
	if err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	return tbl
}

// TestSkillCardEffectAttrKeyWhitelist 钉住"拼错属性键必须在加载期被拒"。
// 拼错的键在 DS 上表现为卡升了没反应,既不报错也不崩,只能靠加载期白名单挡。
// ⚠️ UMyEntityAttrSet 增删属性时,本用例的合法键与 combatAttrKeys 须一起改。
func TestSkillCardEffectAttrKeyWhitelist(t *testing.T) {
	_, err := newSkillCardEffectTable(&configpb.SkillCardEffectTableData{Rows: []*configpb.SkillCardEffectRow{
		// "Attack" 不是 GAS 属性名(正确写法是 Atk)。
		{Id: 1, CardId: 1, AttrKey: "Attack", ValuePerLevel: 3},
	}})
	if err == nil {
		t.Fatal("非法属性键应被拒")
	}
	if !strings.Contains(err.Error(), "attr_key") {
		t.Fatalf("错误信息应点名 attr_key 便于定位: %v", err)
	}

	// 白名单内的键全部要能过,避免白名单写漏把合法配置挡在门外。
	// 与 talent_effect 共用同一份 combatAttrKeys:两张表说的是同一个战斗属性集。
	for key := range combatAttrKeys {
		if _, err := newSkillCardEffectTable(&configpb.SkillCardEffectTableData{Rows: []*configpb.SkillCardEffectRow{
			{Id: 1, CardId: 1, AttrKey: key, ValuePerLevel: 1},
		}}); err != nil {
			t.Fatalf("白名单属性 %s 应放行: %v", key, err)
		}
	}
}

func TestSkillCardEffectRejectsZeroAndOversizedValue(t *testing.T) {
	if _, err := newSkillCardEffectTable(&configpb.SkillCardEffectTableData{Rows: []*configpb.SkillCardEffectRow{
		{Id: 1, CardId: 1, AttrKey: "Atk", ValuePerLevel: 0},
	}}); err == nil {
		t.Fatal("每级数值为 0 的效果行应被拒(整行没有作用)")
	}
	if _, err := newSkillCardEffectTable(&configpb.SkillCardEffectTableData{Rows: []*configpb.SkillCardEffectRow{
		{Id: 1, CardId: 1, AttrKey: "MaxHp", ValuePerLevel: MaxSkillCardEffectValuePerLevel + 1},
	}}); err == nil {
		t.Fatal("超上限的每级数值应被拒(疑似多打零)")
	}
	// 负数是合法设计(高风险高回报卡:加攻减速),只挡绝对值超限。
	if _, err := newSkillCardEffectTable(&configpb.SkillCardEffectTableData{Rows: []*configpb.SkillCardEffectRow{
		{Id: 1, CardId: 1, AttrKey: "MoveSpeedRate", ValuePerLevel: -0.05},
	}}); err != nil {
		t.Fatalf("负数加成应放行: %v", err)
	}
}

// TestSkillCardEffectValidateEffects 覆盖跨行约束:同一张卡对同一属性配两行会让加成翻倍。
func TestSkillCardEffectValidateEffects(t *testing.T) {
	dup := newSkillCardEffectTableForTest(t,
		&configpb.SkillCardEffectRow{Id: 1, CardId: 8, AttrKey: "Atk", ValuePerLevel: 2},
		&configpb.SkillCardEffectRow{Id: 2, CardId: 8, AttrKey: "Atk", ValuePerLevel: 3},
	)
	if err := dup.ValidateEffects(); err == nil {
		t.Fatal("同一张卡对同一属性配多行应被拒")
	}

	// 同一张卡配不同属性(传说卡「影袭」的正常形态)必须放行。
	ok := newSkillCardEffectTableForTest(t,
		&configpb.SkillCardEffectRow{Id: 1, CardId: 8, AttrKey: "Atk", ValuePerLevel: 4},
		&configpb.SkillCardEffectRow{Id: 2, CardId: 8, AttrKey: "MaxHp", ValuePerLevel: 30},
		&configpb.SkillCardEffectRow{Id: 3, CardId: 1, AttrKey: "Atk", ValuePerLevel: 2},
	)
	if err := ok.ValidateEffects(); err != nil {
		t.Fatalf("同卡不同属性 / 不同卡同属性应放行: %v", err)
	}
}

func TestSkillCardEffectResolveBonuses(t *testing.T) {
	tbl := newSkillCardEffectTableForTest(t,
		&configpb.SkillCardEffectRow{Id: 1, CardId: 1, AttrKey: "Atk", ValuePerLevel: 2},
		&configpb.SkillCardEffectRow{Id: 2, CardId: 8, AttrKey: "Atk", ValuePerLevel: 4},
		&configpb.SkillCardEffectRow{Id: 3, CardId: 8, AttrKey: "MaxHp", ValuePerLevel: 30},
	)

	// 卡 1 装到 3 级 → Atk 6;卡 8 装着 1 级 → Atk 4 + MaxHp 30。Atk 跨卡累加成 10。
	// 1 级卡也吃加成:等级即倍数,不做 level-1 折算(与专精同口径)。
	got := tbl.ResolveBonuses(map[uint32]uint32{1: 3, 8: 1})
	if len(got) != 2 {
		t.Fatalf("应聚合成 2 条属性加成, got %v", got)
	}
	// 结果按属性名定序:Atk 在 MaxHp 前。
	if got[0].AttrKey != "Atk" || got[0].Value != 10 {
		t.Fatalf("Atk 应为 2×3 + 4×1 = 10, got %v", got[0])
	}
	if got[1].AttrKey != "MaxHp" || got[1].Value != 30 {
		t.Fatalf("MaxHp 应为 30×1 = 30, got %v", got[1])
	}

	// 0 级(没装)与无效果行的卡都不该产出加成,也不该报错:
	// 后者是**正常态** —— 纯技能型卡只给技能不给属性。
	if bonuses := tbl.ResolveBonuses(map[uint32]uint32{1: 0, 999: 5}); bonuses != nil {
		t.Fatalf("未装配与无效果卡不应产出加成, got %v", bonuses)
	}
}
