package configtable

import (
	"fmt"
	"math"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// equipment_attr.go — EquipmentAttrTable 手写伴生文件。
// 首次由 configtable-gen 创建(仅当文件不存在),此后归人维护,生成器不再覆盖。
// 表私有的逐行业务校验写在 validateEquipmentAttrRow;域方法(业务语义查询)也加在本文件。
//
// 视图结构与通用访问 API(All/ByID/Exists/Count/ByIDs/RandOne/Where/First)在
// equipment_attr_table.gen.go(tools/configtable-gen 生成,勿手改)。
//
// 服务端**不消费**这 6 个百分比(战斗数值权威在客户端 §9.6),本文件只做加载期门禁。
// 表的用途与两端口径见 proto/pandora/config/v1/equipment_attr.proto 的文件头。

const (
	// MaxEquipmentAttrSlot 装备部位上界(1臂 2手 3腿 4躯干 5头 6核心)。
	// 与 d_道具.xlsx「装备部位」列说明、客户端 UMyRoleEquipView::SlotCount 同一口径。
	MaxEquipmentAttrSlot uint32 = 6

	// MaxEquipmentAttrQuality 品质上界(0白 1绿 2蓝 3紫 4橙 5红)。
	MaxEquipmentAttrQuality uint32 = 5

	// MaxEquipmentAttrRate 单件单项加成率上界(= +1000%)。
	//
	// 不是「以后可能有更高的」,是**挡小数点打错**:策划把 0.07 写成 7 或 70,在 DS 上
	// 表现为一件白装把角色血量拉到 8 倍,没有任何报错。真实数据最大值 0.42,
	// 留两个数量级的余量后仍能一眼拦住量级错误。
	MaxEquipmentAttrRate float32 = 10
)

// validateEquipmentAttrRow 逐行业务校验(生成的 newEquipmentAttrTable 调用;
// 主键非零/唯一已由生成代码兜住,类型/必填/枚举已由生成器在导表阶段校验,
// 这里只写服务端仍须 fail-closed 的业务约束)。
//
// 与生成阶段校验重复是有意的 fail-closed:服务端不信任产物一定出自本生成器。
// 整表任一行不过即拒新版本、保留旧表(§9.15 加载失败不切换)。
func validateEquipmentAttrRow(row *configpb.EquipmentAttrRow) error {
	if row.GetName() == "" {
		return fmt.Errorf("装备名称(name)为空")
	}
	if slot := row.GetEquipSlot(); slot == 0 || slot > MaxEquipmentAttrSlot {
		return fmt.Errorf("装备部位(equip_slot)必须在 [1,%d],实为 %d", MaxEquipmentAttrSlot, slot)
	}
	if q := row.GetQuality(); q > MaxEquipmentAttrQuality {
		return fmt.Errorf("品质(quality)必须在 [0,%d],实为 %d", MaxEquipmentAttrQuality, q)
	}
	if row.GetIcon() == "" {
		return fmt.Errorf("装备图标(icon)为空")
	}

	for _, rate := range equipmentAttrRates(row) {
		if err := validateEquipmentAttrRate(rate.col, rate.value); err != nil {
			return err
		}
	}

	// 六项全 0 的行不是「这件装备没有基础属性」而是「这行白填了」:装备一定有基础属性,
	// 全 0 更可能是整行被误清空或粘错列。空行本身会被导表器跳过,能走到这里说明
	// 装备ID/名称/部位都填了,只有数值列是空的。
	if equipmentAttrAllZero(row) {
		return fmt.Errorf("六项加成(HP/伤害/治疗/暴击/技能/控场)全为 0,该装备穿上不会有任何效果")
	}
	return nil
}

// equipmentAttrRate 是一列加成率的「列名 + 取值」,只为把校验与遍历写成一处。
type equipmentAttrRate struct {
	col   string
	value float32
}

// equipmentAttrRates 按表头顺序列出六个百分比列。
// 新增数值列时改这里一处,逐行校验与全零判定自动覆盖。
func equipmentAttrRates(row *configpb.EquipmentAttrRow) []equipmentAttrRate {
	return []equipmentAttrRate{
		{"HP(hp_rate)", row.GetHpRate()},
		{"伤害(damage_rate)", row.GetDamageRate()},
		{"治疗(heal_rate)", row.GetHealRate()},
		{"暴击(crit_rate)", row.GetCritRate()},
		{"技能(skill_rate)", row.GetSkillRate()},
		{"控场(control_rate)", row.GetControlRate()},
	}
}

// validateEquipmentAttrRate 单个百分比列的取值域校验。
func validateEquipmentAttrRate(col string, value float32) error {
	if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
		return fmt.Errorf("%s 不是有限数值", col)
	}
	// 负加成不是「减益装备」这种玩法,而是漏了负号校验就会出现的配表事故:
	// 现有 120 行全部非负,真要做减益装备时应当先设计减益的展示与上限,再放开这条。
	if value < 0 {
		return fmt.Errorf("%s 为负(%v);当前不支持负加成", col, value)
	}
	if value > MaxEquipmentAttrRate {
		return fmt.Errorf("%s = %v 超上限 %v(单位是小数百分比,0.07 = +7%%;疑似小数点写错)",
			col, value, MaxEquipmentAttrRate)
	}
	return nil
}

// equipmentAttrAllZero 判断一行的六个百分比列是否全为 0。
func equipmentAttrAllZero(row *configpb.EquipmentAttrRow) bool {
	for _, rate := range equipmentAttrRates(row) {
		if rate.value != 0 {
			return false
		}
	}
	return true
}

// ValidateEquipmentAttrCrossTables 装备基础属性表的批次级跨表校验(消费服务 AddValidator 注册)。
//
// 「装备ID」就是本表主键 id,而 (excel_fk) 不允许标在 id 上(tools/configtable-gen
// discover.go 的硬约束),所以指向 item 表的引用完整性只能在这里手写。
//
// 三条 fail-closed:
//  1. 装备ID 必须存在于道具表 —— 属性行指向一个不存在的道具,穿戴时永远查不到,
//     且在 xlsx 里与「这件装备还没配属性」长得一模一样;
//  2. 该道具必须是装备(type=EQUIPMENT 且 equip_slot>0) —— 给消耗品配战斗加成是配错列;
//  3. 装备部位 / 品质 必须与道具表逐行一致。注意真实风险不是"客户端会读错部位"——
//     **本表的 equip_slot / quality 在客户端运行期根本没有消费点**:UE 侧只读 6 个百分比
//     (UMyEquipAttrStatics::BuildBaseDeltasFromRow / UMyItemViewStatics::FormatEquipBaseAttributes),
//     外观挂载与穿戴校验一律读道具表的 FCfgItem::EquipSlot
//     (MyEquipmentVisualComponent.cpp 的 fail-closed 分支比的是道具表,而且带 Warning 日志)。
//     风险是「策划以为改了本表的部位就改了这件装备的部位」——改了个没人读的副本,
//     两张表就此分叉,而加载期是唯一能挡住它的地方。
//
// 刻意**不**校验的:
//   - 名称 / 图标 / 描述文本 三列(同样是道具表冗余副本)只在逐行校验里查非空。它们不参与
//     任何判定,为一次纯文案改动拒绝整批配置、卡住服务启动,代价不成比例。
//   - 「每件装备都必须有属性行」不成立:道具表现有 151 件装备,本表只覆盖 120 件,
//     10003 与 10027..10056 这 31 件老装备刻意没有基础属性(只吃鉴定词条)。
//     反方向(属性行必须有对应装备)才是硬约束,即第 1 条。
func ValidateEquipmentAttrCrossTables(attrs *EquipmentAttrTable, items *ItemTable) error {
	if attrs == nil || items == nil {
		return fmt.Errorf("equipment_attr / item tables required")
	}
	for _, row := range attrs.All() {
		item, ok := items.ByID(row.GetId())
		if !ok {
			return fmt.Errorf("equipment_attr %d(%s)在道具表中不存在", row.GetId(), row.GetName())
		}
		if item.GetType() != configpb.ItemType_ITEM_TYPE_EQUIPMENT || item.GetEquipSlot() == 0 {
			return fmt.Errorf("equipment_attr %d(%s)对应的道具不是装备:type=%v equip_slot=%d",
				row.GetId(), row.GetName(), item.GetType(), item.GetEquipSlot())
		}
		if row.GetEquipSlot() != item.GetEquipSlot() {
			return fmt.Errorf("equipment_attr %d(%s)装备部位与道具表漂移:属性表=%d 道具表=%d",
				row.GetId(), row.GetName(), row.GetEquipSlot(), item.GetEquipSlot())
		}
		if row.GetQuality() != item.GetQuality() {
			return fmt.Errorf("equipment_attr %d(%s)品质与道具表漂移:属性表=%d 道具表=%d",
				row.GetId(), row.GetName(), row.GetQuality(), item.GetQuality())
		}
	}
	return nil
}
