package configtable

import (
	"fmt"
	"sort"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// skill_card_effect.go — SkillCardEffectTable 手写伴生文件。
// 首次由 configtable-gen 创建(仅当文件不存在),此后归人维护,生成器不再覆盖。
// 表私有的逐行业务校验写在 validateSkillCardEffectRow;域方法(业务语义查询)也加在本文件。
//
// 视图结构与通用访问 API(All/ByID/Exists/Count/ByIDs/RandOne/Where/First/ListByCardId)在
// skill_card_effect_table.gen.go(tools/configtable-gen 生成,勿手改)。
//
// 属性键白名单与专精效果表共享一份:见 combat_attr_key.go 的 combatAttrKeys。
// 两张表填的都是同一个 UE GAS 战斗属性集,各存一份必然漂移(§9.22 权威唯一)。

// MaxSkillCardEffectValuePerLevel 是单级加成绝对值上限。
//
// 口径与 MaxTalentEffectValuePerLevel 一致:与 MaxSkillCardLevel(30)相乘把单卡加成
// 钳在 30 万以内。配置是热更进来的,多打几个零会让 DS 上出现"一张卡加十亿血"的
// 不可玩局面。只挡明显手滑,不干涉数值设计(§16.5 容量边界)。
const MaxSkillCardEffectValuePerLevel = 10000

// validateSkillCardEffectRow 逐行业务校验(生成的 newSkillCardEffectTable 调用;
// 主键非零/唯一已由生成代码兜住,类型/必填/外键已由生成器在导表阶段校验)。
//
// 跨行约束(同一张卡不得对同一属性重复加成)看不到其它行,放在 ValidateEffects。
func validateSkillCardEffectRow(row *configpb.SkillCardEffectRow) error {
	if !IsCombatAttrKey(row.GetAttrKey()) {
		return fmt.Errorf("属性键(attr_key=%q)不是战斗属性集里的属性,该效果在 DS 上永远不会生效;"+
			"合法取值:%s(权威见 MyEntityAttrSet.h)", row.GetAttrKey(), CombatAttrKeysHint())
	}
	if row.GetValuePerLevel() == 0 {
		return fmt.Errorf("每级数值(value_per_level)为 0,该效果行没有任何作用")
	}
	if v := row.GetValuePerLevel(); v > MaxSkillCardEffectValuePerLevel || v < -MaxSkillCardEffectValuePerLevel {
		return fmt.Errorf("每级数值(value_per_level=%v)超出 ±%d,疑似多打了零",
			v, MaxSkillCardEffectValuePerLevel)
	}
	return nil
}

// ValidateEffects 整表跨行校验:同一张技能卡不得对同一属性配置多行。
//
// 重复行不会报错,只会让加成翻倍——是"数值莫名其妙偏高"这类问题里最难查的来源,
// 必须在加载边界整批拒绝。由各服务的 configtable 加载校验器调用
// (做法对齐 TalentEffectTable.ValidateEffects)。
func (t *SkillCardEffectTable) ValidateEffects() error {
	type key struct {
		cardID  uint32
		attrKey string
	}
	seen := make(map[key]uint32, len(t.rows))
	for _, row := range t.rows {
		k := key{cardID: row.GetCardId(), attrKey: row.GetAttrKey()}
		if firstID, dup := seen[k]; dup {
			return fmt.Errorf("技能卡 %d 对属性 %s 配了多行效果(行 %d 与行 %d),加成会翻倍",
				k.cardID, k.attrKey, firstID, row.GetId())
		}
		seen[k] = row.GetId()
	}
	return nil
}

// SkillCardAttrBonus 是一份卡槽装配换算出来的单条属性加成(展示 / 校验用聚合结果)。
type SkillCardAttrBonus struct {
	AttrKey string
	Value   float32
}

// ResolveBonuses 把一份卡槽装配(卡 ID → 该卡等级)换算成属性加成合计。
//
// 单卡加成 = 等级 × 该卡每条效果的每级数值;同一属性跨卡累加。等级 1 是获得卡时的
// 初始等级,因此 1 级卡也有加成(口径与专精一致:等级即倍数,不做 level-1 折算)。
// 结果按属性名定序,保证同一份装配每次输出一致(map 遍历顺序不稳定会让输出抖动、
// 测试随机失败)。
//
// 入参只应包含**装配在卡槽里**的卡:持有即加属性会让攒卡直接等于变强,与卡槽是
// 养成出口的设计相悖。DS 侧的入参来自 GetLoadout 的 LoadoutSkillCard(卡槽装配),
// 这条不变量由数据通道本身保证。
//
// 装配里出现本表没有效果行的卡是合法的(纯技能型卡,只给技能不给属性),跳过即可,不报错。
func (t *SkillCardEffectTable) ResolveBonuses(levels map[uint32]uint32) []SkillCardAttrBonus {
	sum := make(map[string]float32)
	for cardID, level := range levels {
		if level == 0 {
			continue
		}
		for _, row := range t.ListByCardId(cardID) {
			sum[row.GetAttrKey()] += float32(level) * row.GetValuePerLevel()
		}
	}
	if len(sum) == 0 {
		return nil
	}

	keys := make([]string, 0, len(sum))
	for k := range sum {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]SkillCardAttrBonus, 0, len(keys))
	for _, k := range keys {
		out = append(out, SkillCardAttrBonus{AttrKey: k, Value: sum[k]})
	}
	return out
}
