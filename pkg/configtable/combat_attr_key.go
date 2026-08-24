package configtable

import (
	"sort"
	"strings"
)

// combat_attr_key.go — 「战斗属性集」在服务端的校验用副本(手写,非生成)。
//
// 谁在用:专精效果表(talent_effect)与技能卡效果表(skill_card_effect)。两张表填的
// 都是同一个东西 —— UE GAS 的战斗属性名 —— 因此白名单只能有一份。曾经各存一份的做法
// 会在 MyEntityAttrSet 增删属性时只同步到其中一张表,让另一张表要么误拒合法键、
// 要么放过拼错的键,而这两种结果在 DS 上都表现为"点了/升了没反应"且全程静默。
//
// **权威在客户端**:Pandora-Client-SVN/Pandora/Source/Pandora/Public/GAS/MyEntityAttrSet.h
// 的 UMyEntityAttrSet。这里是校验用副本,不是第二份权威(§9.22)——服务端不消费这些数值
// (§9.6 战斗数值权威在客户端),但必须在导表/加载边界挡住拼错的键。
//
// ⚠️ UMyEntityAttrSet 增删属性时必须同步本文件(以及 TestCombatAttrKeyWhitelist 的注释)。
//
// === 与「属性键不存在就不应用」的关系 ===
// 运行期(DS)的口径是**逐条跳过**:UMyAttrBonusStatics 在 UMyEntityAttrSet 上按名反查
// 不到属性时跳过该条加成并打 Error,不影响同批其它加成 —— 这是"战斗属性有就用、
// 没有就不应用"。本白名单是**导表期**的另一道门,口径相反:整批拒。两者不冲突,
// 因为它们回答的是不同问题 ——
//   导表期问"策划是不是填错了",答错就得让人当场知道,静默产出一张永远不生效的表
//     才是事故(拼错 "Atak" 与"这个属性还没做"在 xlsx 里长得一模一样);
//   运行期问"这一条现在能不能落地",答不能就跳过,不能因为一条脏数据让整局没属性。
// 换句话说:表进不来是策划的编辑期反馈,表进来了但键对不上是 DS 的运行期兜底。
var combatAttrKeys = map[string]struct{}{
	"Hp":               {}, // 血量(当前值,上限见 MaxHp)
	"MaxHp":            {}, // 生命上限
	"Atk":              {}, // 攻击力
	"Defense":          {}, // 防御
	"Shield":           {}, // 护盾
	"CritChance":       {}, // 暴击率
	"CritDamage":       {}, // 暴击伤害(暴击时的额外伤害倍率,1.0 = 双倍)
	"HitChance":        {}, // 命中率
	"DodgeChance":      {}, // 闪避率
	"MoveSpeedRate":    {}, // 移动速度倍率
	"AtkSpeedRate":     {}, // 攻击速度倍率
	"SkillDamageRate":  {}, // 技能伤害加成率
	"SkillHealRate":    {}, // 技能治疗加成率
	"SkillControlRate": {}, // 技能控制时间加成率
}

// IsCombatAttrKey 判断一个属性键是否是战斗属性集里的属性(区分大小写)。
//
// 区分大小写是刻意的:DS 侧 FindPropertyByName 也区分大小写,这里放宽会让
// "hp" 通过导表却在 DS 上永远查不到属性 —— 那正是本白名单要挡的静默失效。
func IsCombatAttrKey(key string) bool {
	_, ok := combatAttrKeys[key]
	return ok
}

// CombatAttrKeysHint 返回排序后的合法属性键,用于拼进报错信息。
//
// 报错只说"这个键非法"帮不上策划:xlsx 里看不到 MyEntityAttrSet.h。把合法取值
// 一并列出来,拼错的那一个通常一眼就能对出来。
func CombatAttrKeysHint() string {
	keys := make([]string, 0, len(combatAttrKeys))
	for k := range combatAttrKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, " / ")
}
