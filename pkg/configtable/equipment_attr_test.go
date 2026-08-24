package configtable

import (
	"strings"
	"testing"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	"google.golang.org/protobuf/proto"
)

// newValidEquipmentAttrRow 造一行「星火法杖」形状的合法数据(与真实 dist 第一行同构)。
func newValidEquipmentAttrRow() *configpb.EquipmentAttrRow {
	return &configpb.EquipmentAttrRow{
		Id: 10066, Name: "星火法杖", Quality: 0, EquipSlot: 1,
		Icon:        "/Game/UI/Icon/ItemReal/T_Item_10066",
		HpRate:      0.07,
		DamageRate:  0.19,
		HealRate:    0,
		CritRate:    0.19,
		SkillRate:   0.11,
		ControlRate: 0.09,
		Description: "武器",
	}
}

func TestValidateEquipmentAttrRowBounds(t *testing.T) {
	if err := validateEquipmentAttrRow(newValidEquipmentAttrRow()); err != nil {
		t.Fatalf("合法行被拒: %v", err)
	}

	tests := []struct {
		name string
		edit func(*configpb.EquipmentAttrRow)
	}{
		{"名称为空", func(r *configpb.EquipmentAttrRow) { r.Name = "" }},
		{"部位为 0", func(r *configpb.EquipmentAttrRow) { r.EquipSlot = 0 }},
		{"部位越界", func(r *configpb.EquipmentAttrRow) { r.EquipSlot = MaxEquipmentAttrSlot + 1 }},
		{"品质越界", func(r *configpb.EquipmentAttrRow) { r.Quality = MaxEquipmentAttrQuality + 1 }},
		{"图标为空", func(r *configpb.EquipmentAttrRow) { r.Icon = "" }},
		{"负加成", func(r *configpb.EquipmentAttrRow) { r.HpRate = -0.01 }},
		{"小数点写错", func(r *configpb.EquipmentAttrRow) { r.DamageRate = MaxEquipmentAttrRate + 1 }},
		{"治疗列越界", func(r *configpb.EquipmentAttrRow) { r.HealRate = MaxEquipmentAttrRate + 1 }},
		{"暴击列越界", func(r *configpb.EquipmentAttrRow) { r.CritRate = MaxEquipmentAttrRate + 1 }},
		{"技能列越界", func(r *configpb.EquipmentAttrRow) { r.SkillRate = MaxEquipmentAttrRate + 1 }},
		{"控场列越界", func(r *configpb.EquipmentAttrRow) { r.ControlRate = MaxEquipmentAttrRate + 1 }},
		{"六项全 0", func(r *configpb.EquipmentAttrRow) {
			r.HpRate, r.DamageRate, r.HealRate = 0, 0, 0
			r.CritRate, r.SkillRate, r.ControlRate = 0, 0, 0
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// proto message 禁止值拷贝(CLAUDE.md §5.10):克隆而不是 row := *valid。
			row := proto.Clone(newValidEquipmentAttrRow()).(*configpb.EquipmentAttrRow)
			tt.edit(row)
			if err := validateEquipmentAttrRow(row); err == nil {
				t.Fatal("非法行被放行")
			}
		})
	}
}

// TestEquipmentAttrTableIndex 建表期的主键唯一/非零由生成代码兜住,这里钉住它确实生效。
func TestEquipmentAttrTableIndex(t *testing.T) {
	ok, err := newEquipmentAttrTable(&configpb.EquipmentAttrTableData{
		Rows: []*configpb.EquipmentAttrRow{newValidEquipmentAttrRow()},
	})
	if err != nil {
		t.Fatalf("合法整表被拒: %v", err)
	}
	if ok.Count() != 1 || !ok.Exists(10066) {
		t.Fatalf("索引未建立: count=%d exists=%v", ok.Count(), ok.Exists(10066))
	}

	dup := proto.Clone(newValidEquipmentAttrRow()).(*configpb.EquipmentAttrRow)
	if _, err := newEquipmentAttrTable(&configpb.EquipmentAttrTableData{
		Rows: []*configpb.EquipmentAttrRow{newValidEquipmentAttrRow(), dup},
	}); err == nil {
		t.Fatal("主键重复被放行")
	}
}

// equipmentAttrCrossFixture 造一对「属性表 + 道具表」批次,默认自洽。
func equipmentAttrCrossFixture(t *testing.T, editAttr func(*configpb.EquipmentAttrRow),
	editItem func(*configpb.ItemRow)) (*EquipmentAttrTable, *ItemTable) {
	t.Helper()

	attrRow := newValidEquipmentAttrRow()
	itemRow := &configpb.ItemRow{
		Id: 10066, Name: "星火法杖", Type: configpb.ItemType_ITEM_TYPE_EQUIPMENT,
		Quality: 0, MaxStackSize: 1, EquipSlot: 1, IdentifyPoolId: 1,
		Icon: "/Game/UI/Icon/ItemReal/T_Item_10066",
	}
	if editAttr != nil {
		editAttr(attrRow)
	}
	if editItem != nil {
		editItem(itemRow)
	}

	attrs, err := newEquipmentAttrTable(&configpb.EquipmentAttrTableData{
		Rows: []*configpb.EquipmentAttrRow{attrRow},
	})
	if err != nil {
		t.Fatalf("构造装备属性表失败: %v", err)
	}
	items, err := newItemTable(&configpb.ItemTableData{Rows: []*configpb.ItemRow{itemRow}})
	if err != nil {
		t.Fatalf("构造道具表失败: %v", err)
	}
	return attrs, items
}

func TestValidateEquipmentAttrCrossTables(t *testing.T) {
	attrs, items := equipmentAttrCrossFixture(t, nil, nil)
	if err := ValidateEquipmentAttrCrossTables(attrs, items); err != nil {
		t.Fatalf("自洽批次被拒: %v", err)
	}

	t.Run("道具不存在", func(t *testing.T) {
		attrs, items := equipmentAttrCrossFixture(t,
			func(r *configpb.EquipmentAttrRow) { r.Id = 19999 }, nil)
		if err := ValidateEquipmentAttrCrossTables(attrs, items); err == nil ||
			!strings.Contains(err.Error(), "不存在") {
			t.Fatalf("孤儿属性行必须被拒, err=%v", err)
		}
	})

	t.Run("部位漂移", func(t *testing.T) {
		attrs, items := equipmentAttrCrossFixture(t, nil,
			func(r *configpb.ItemRow) { r.EquipSlot = 4 })
		if err := ValidateEquipmentAttrCrossTables(attrs, items); err == nil ||
			!strings.Contains(err.Error(), "装备部位") {
			t.Fatalf("部位漂移必须被拒, err=%v", err)
		}
	})

	t.Run("品质漂移", func(t *testing.T) {
		attrs, items := equipmentAttrCrossFixture(t, nil,
			func(r *configpb.ItemRow) { r.Quality = 3 })
		if err := ValidateEquipmentAttrCrossTables(attrs, items); err == nil ||
			!strings.Contains(err.Error(), "品质") {
			t.Fatalf("品质漂移必须被拒, err=%v", err)
		}
	})

	t.Run("对应道具不是装备", func(t *testing.T) {
		// type 与 equip_slot 必须同时成立(validateItemRow),所以两列一起改成消耗品。
		attrs, items := equipmentAttrCrossFixture(t,
			func(r *configpb.EquipmentAttrRow) { r.Id = 10001 },
			func(r *configpb.ItemRow) {
				r.Id = 10001
				r.Type = configpb.ItemType_ITEM_TYPE_CONSUMABLE
				r.EquipSlot = 0
				r.IdentifyPoolId = 0
				r.MaxStackSize = 20
			})
		if err := ValidateEquipmentAttrCrossTables(attrs, items); err == nil ||
			!strings.Contains(err.Error(), "不是装备") {
			t.Fatalf("非装备道具必须被拒, err=%v", err)
		}
	})

	t.Run("缺表 fail-closed", func(t *testing.T) {
		if err := ValidateEquipmentAttrCrossTables(nil, items); err == nil {
			t.Fatal("缺属性表必须被拒")
		}
		if err := ValidateEquipmentAttrCrossTables(attrs, nil); err == nil {
			t.Fatal("缺道具表必须被拒")
		}
	})
}
