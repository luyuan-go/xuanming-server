package configtable

import (
	"strings"
	"testing"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

func newAttrPointEffectTableForTest(t *testing.T, rows ...*configpb.AttrPointEffectRow) *AttrPointEffectTable {
	t.Helper()
	tbl, err := newAttrPointEffectTable(&configpb.AttrPointEffectTableData{Rows: rows})
	if err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	return tbl
}

// TestAttrPointEffectAttrKeyWhitelist 钉住"拼错战斗属性键必须在加载期被拒"。
// 拼错的键在 DS 上表现为这条属性加了点没反应,既不报错也不崩,只能靠加载期白名单挡。
// 白名单与专精 / 技能卡效果表共享一份(combat_attr_key.go),此处只钉本表接上了那一份。
func TestAttrPointEffectAttrKeyWhitelist(t *testing.T) {
	_, err := newAttrPointEffectTable(&configpb.AttrPointEffectTableData{Rows: []*configpb.AttrPointEffectRow{
		// "Attack" 不是 GAS 属性名(正确写法是 Atk)。
		{Id: 1, AttrPointKey: "str", AttrKey: "Attack", ValuePerPoint: 2},
	}})
	if err == nil {
		t.Fatal("非法属性键应被拒")
	}
	if !strings.Contains(err.Error(), "attr_key") {
		t.Fatalf("错误信息应点名 attr_key 便于定位: %v", err)
	}

	for _, key := range []string{"Hp", "Atk", "Defense", "MoveSpeedRate"} {
		if _, err := newAttrPointEffectTable(&configpb.AttrPointEffectTableData{Rows: []*configpb.AttrPointEffectRow{
			{Id: 1, AttrPointKey: "str", AttrKey: key, ValuePerPoint: 1},
		}}); err != nil {
			t.Fatalf("战斗属性 %s 应放行: %v", key, err)
		}
	}
}

// TestAttrPointEffectKeyCharset 钉住属性点键的字符集与长度约束。
// 落库列是 utf8mb4_0900_ai_ci(大小写不敏感),"Str" 与 "str" 会挤进同一条唯一键,
// 读回来却是先写进去的拼法 —— 表现为"同一条属性点有时加得上有时加不上"。
func TestAttrPointEffectKeyCharset(t *testing.T) {
	bad := []string{"", "Str", "st r", "力量", strings.Repeat("a", MaxAttrPointKeyLen+1)}
	for _, key := range bad {
		if _, err := newAttrPointEffectTable(&configpb.AttrPointEffectTableData{Rows: []*configpb.AttrPointEffectRow{
			{Id: 1, AttrPointKey: key, AttrKey: "Atk", ValuePerPoint: 1},
		}}); err == nil {
			t.Fatalf("非法属性点键 %q 应被拒", key)
		}
	}
	for _, key := range []string{"str", "con", "dex", "int", "men", "crit_rate2"} {
		if _, err := newAttrPointEffectTable(&configpb.AttrPointEffectTableData{Rows: []*configpb.AttrPointEffectRow{
			{Id: 1, AttrPointKey: key, AttrKey: "Atk", ValuePerPoint: 1},
		}}); err != nil {
			t.Fatalf("合法属性点键 %q 应放行: %v", key, err)
		}
	}
}

func TestAttrPointEffectRejectsZeroAndOversizedValue(t *testing.T) {
	if _, err := newAttrPointEffectTable(&configpb.AttrPointEffectTableData{Rows: []*configpb.AttrPointEffectRow{
		{Id: 1, AttrPointKey: "str", AttrKey: "Atk", ValuePerPoint: 0},
	}}); err == nil {
		t.Fatal("每点数值为 0 的效果行应被拒(整行没有作用)")
	}
	if _, err := newAttrPointEffectTable(&configpb.AttrPointEffectTableData{Rows: []*configpb.AttrPointEffectRow{
		{Id: 1, AttrPointKey: "con", AttrKey: "Hp", ValuePerPoint: MaxAttrPointEffectValuePerPoint + 1},
	}}); err == nil {
		t.Fatal("超上限的每点数值应被拒(疑似多打零)")
	}
	// 负数是合法设计(代价型属性点),只挡绝对值超限。
	if _, err := newAttrPointEffectTable(&configpb.AttrPointEffectTableData{Rows: []*configpb.AttrPointEffectRow{
		{Id: 1, AttrPointKey: "con", AttrKey: "MoveSpeedRate", ValuePerPoint: -0.005},
	}}); err != nil {
		t.Fatalf("负数加成应放行: %v", err)
	}
}

// TestAttrPointEffectValidateEffects 覆盖跨行约束:同一条属性点对同一属性配两行会让加成翻倍。
func TestAttrPointEffectValidateEffects(t *testing.T) {
	dup := newAttrPointEffectTableForTest(t,
		&configpb.AttrPointEffectRow{Id: 1, AttrPointKey: "men", AttrKey: "Hp", ValuePerPoint: 10},
		&configpb.AttrPointEffectRow{Id: 2, AttrPointKey: "men", AttrKey: "Hp", ValuePerPoint: 5},
	)
	if err := dup.ValidateEffects(); err == nil {
		t.Fatal("同一属性点对同一属性配多行应被拒")
	}

	// 同一属性点配不同属性(「精神」同时加防御与血量)必须放行。
	ok := newAttrPointEffectTableForTest(t,
		&configpb.AttrPointEffectRow{Id: 1, AttrPointKey: "men", AttrKey: "Defense", ValuePerPoint: 1},
		&configpb.AttrPointEffectRow{Id: 2, AttrPointKey: "men", AttrKey: "Hp", ValuePerPoint: 10},
		&configpb.AttrPointEffectRow{Id: 3, AttrPointKey: "con", AttrKey: "Hp", ValuePerPoint: 25},
	)
	if err := ok.ValidateEffects(); err != nil {
		t.Fatalf("同属性点不同属性 / 不同属性点同属性应放行: %v", err)
	}
}

func TestAttrPointEffectResolveBonuses(t *testing.T) {
	tbl := newAttrPointEffectTableForTest(t,
		&configpb.AttrPointEffectRow{Id: 1, AttrPointKey: "str", AttrKey: "Atk", ValuePerPoint: 2},
		&configpb.AttrPointEffectRow{Id: 2, AttrPointKey: "con", AttrKey: "Hp", ValuePerPoint: 25},
		&configpb.AttrPointEffectRow{Id: 3, AttrPointKey: "men", AttrKey: "Hp", ValuePerPoint: 10},
	)

	// 力量 3 点 → Atk 6;体质 2 点 → Hp 50;精神 1 点 → Hp 10。Hp 跨属性点累加成 60。
	got := tbl.ResolveBonuses(map[string]int32{"str": 3, "con": 2, "men": 1})
	if len(got) != 2 {
		t.Fatalf("应聚合成 2 条属性加成, got %v", got)
	}
	// 结果按属性名定序:Atk 在 Hp 前。
	if got[0].AttrKey != "Atk" || got[0].Value != 6 {
		t.Fatalf("Atk 应为 2×3 = 6, got %v", got[0])
	}
	if got[1].AttrKey != "Hp" || got[1].Value != 60 {
		t.Fatalf("Hp 应为 25×2 + 10×1 = 60, got %v", got[1])
	}

	// 0 点、负点(脏数据)与表里没有的键都不该产出加成,也不该报错。
	if bonuses := tbl.ResolveBonuses(map[string]int32{"str": 0, "con": -5, "ghost": 9}); bonuses != nil {
		t.Fatalf("0 点 / 负点 / 未知键不应产出加成, got %v", bonuses)
	}
}

// TestAttrPointEffectKeySet 钉住「本表是有哪几条属性点的权威」这一用途:
// player 服务的写入边界(AllocateAttributePoints)按它拒未知键。
func TestAttrPointEffectKeySet(t *testing.T) {
	tbl := newAttrPointEffectTableForTest(t,
		&configpb.AttrPointEffectRow{Id: 1, AttrPointKey: "men", AttrKey: "Defense", ValuePerPoint: 1},
		&configpb.AttrPointEffectRow{Id: 2, AttrPointKey: "men", AttrKey: "Hp", ValuePerPoint: 10},
		&configpb.AttrPointEffectRow{Id: 3, AttrPointKey: "con", AttrKey: "Hp", ValuePerPoint: 25},
	)
	if !tbl.HasAttrPointKey("men") || !tbl.HasAttrPointKey("con") {
		t.Fatal("表里配了的属性点键应被认出")
	}
	if tbl.HasAttrPointKey("ghost") || tbl.HasAttrPointKey("MEN") {
		t.Fatal("未配置的键 / 大写拼法不应被认出(落库列大小写不敏感,只认小写)")
	}
	keys := tbl.AttrPointKeys()
	if len(keys) != 2 || keys[0] != "con" || keys[1] != "men" {
		t.Fatalf("属性点键应去重且升序, got %v", keys)
	}
}
