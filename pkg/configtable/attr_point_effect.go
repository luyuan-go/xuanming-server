package configtable

import (
	"fmt"
	"sort"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// attr_point_effect.go — AttrPointEffectTable 手写伴生文件。
// 首次由 configtable-gen 创建(仅当文件不存在),此后归人维护,生成器不再覆盖。
// 表私有的逐行业务校验写在 validateAttrPointEffectRow;域方法(业务语义查询)也加在本文件。
//
// 视图结构与通用访问 API(All/ByID/Exists/Count/ByIDs/RandOne/Where/First/ListByAttrPointKey)在
// attr_point_effect_table.gen.go(tools/configtable-gen 生成,勿手改)。
//
// 属性键白名单与专精效果表、技能卡效果表共享一份:见 combat_attr_key.go 的 combatAttrKeys。
// 三张表填的都是同一个 UE GAS 战斗属性集,各存一份必然漂移(§9.22 权威唯一)。
//
// === 本表同时是「有哪几条属性点」的权威 ===
// 专精 / 技能卡的效果表只回答"这个节点加多少",节点本身有主表(talent / skill_card)。
// 属性点没有主表 —— 在本表出现之前,"玩家能往哪几条属性上加点"只存在于客户端
// UMyRoleAttrView 的构造函数里,服务端 AllocateAttributePoints 收任何非空字符串。
// 因此本表的 attr_point_key 集合就是权威集合,player 服务用 HasAttrPointKey 校验写入:
// 表里没有的键加了点也永远换不成战斗数值,等于把点数永久沉没(只能洗点找回)。

// MaxAttrPointEffectValuePerPoint 是单点加成绝对值上限。
//
// 口径与 MaxTalentEffectValuePerLevel 一致:只挡明显手滑(多打几个零),
// 不干涉数值设计(§16.5 容量边界)。配置是热更进来的,一个多打零的数值会让 DS 上
// 出现"一点体质加十亿血"的不可玩局面。
const MaxAttrPointEffectValuePerPoint = 10000

// MaxAttrPointKeyLen 是属性点键长度上限,对齐落库列 pandora_player.player_attributes.attr_key
// (VARCHAR(32))。超长的键写库会被截断(非严格模式下还是静默截断,§9.24),
// 截断后两条不同的属性点会挤进同一行,必须在导表边界就挡住。
const MaxAttrPointKeyLen = 32

// validateAttrPointEffectRow 逐行业务校验(生成的 newAttrPointEffectTable 调用;
// 主键非零/唯一已由生成代码兜住,类型/必填已由生成器在导表阶段校验)。
//
// 跨行约束(同一条属性点不得对同一属性重复加成)看不到其它行,放在 ValidateEffects。
func validateAttrPointEffectRow(row *configpb.AttrPointEffectRow) error {
	if err := validateAttrPointKey(row.GetAttrPointKey()); err != nil {
		return err
	}
	if !IsCombatAttrKey(row.GetAttrKey()) {
		return fmt.Errorf("属性键(attr_key=%q)不是战斗属性集里的属性,该效果在 DS 上永远不会生效;"+
			"合法取值:%s(权威见 MyEntityAttrSet.h)", row.GetAttrKey(), CombatAttrKeysHint())
	}
	if row.GetValuePerPoint() == 0 {
		return fmt.Errorf("每点数值(value_per_point)为 0,该效果行没有任何作用")
	}
	if v := row.GetValuePerPoint(); v > MaxAttrPointEffectValuePerPoint || v < -MaxAttrPointEffectValuePerPoint {
		return fmt.Errorf("每点数值(value_per_point=%v)超出 ±%d,疑似多打了零",
			v, MaxAttrPointEffectValuePerPoint)
	}
	return nil
}

// validateAttrPointKey 约束属性点键的字符集与长度。
//
// 只放行 [a-z0-9_]:落库列是 utf8mb4_0900_ai_ci(**大小写不敏感**),"Str" 与 "str"
// 会挤进同一条唯一键但读回来是先写进去的那个拼法,于是"同一条属性点"在协议层有两个名字、
// 在表里只匹配上其中一个。全角字符、首尾空格同理 —— 肉眼看不出来,表现为"这条属性加了点没反应"。
// 与其在运行期猜,不如在导表期只认一种拼法。
func validateAttrPointKey(key string) error {
	if key == "" {
		return fmt.Errorf("属性点键(attr_point_key)为空")
	}
	if len(key) > MaxAttrPointKeyLen {
		return fmt.Errorf("属性点键(attr_point_key=%q)超过 %d 字节,落库会被截断",
			key, MaxAttrPointKeyLen)
	}
	for _, r := range key {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			continue
		}
		return fmt.Errorf("属性点键(attr_point_key=%q)只允许小写字母 / 数字 / 下划线;"+
			"落库列大小写不敏感,大写与全角拼法会与小写键互相串行", key)
	}
	return nil
}

// ValidateEffects 整表跨行校验:同一条属性点不得对同一属性配置多行。
//
// 重复行不会报错,只会让加成翻倍——是"数值莫名其妙偏高"这类问题里最难查的来源,
// 必须在加载边界整批拒绝。由各服务的 configtable 加载校验器调用
// (做法对齐 TalentEffectTable.ValidateEffects)。
func (t *AttrPointEffectTable) ValidateEffects() error {
	type key struct {
		pointKey string
		attrKey  string
	}
	seen := make(map[key]uint32, len(t.rows))
	for _, row := range t.rows {
		k := key{pointKey: row.GetAttrPointKey(), attrKey: row.GetAttrKey()}
		if firstID, dup := seen[k]; dup {
			return fmt.Errorf("属性点 %s 对属性 %s 配了多行效果(行 %d 与行 %d),加成会翻倍",
				k.pointKey, k.attrKey, firstID, row.GetId())
		}
		seen[k] = row.GetId()
	}
	return nil
}

// HasAttrPointKey 该属性点键是否在表里(区分大小写,表内已强制小写)。
//
// player 服务的 AllocateAttributePoints 用它做写入边界校验:表里没有的键
// 在 DS 上换不出任何战斗数值,加了就是把点数沉没掉。
func (t *AttrPointEffectTable) HasAttrPointKey(key string) bool {
	return len(t.ListByAttrPointKey(key)) > 0
}

// AttrPointKeys 全部属性点键(升序去重)。
//
// 定序是为了让报错信息与展示顺序稳定:map 遍历顺序不稳定会让同一份表每次输出不同,
// 报错里的"合法取值"每次换一个顺序,读的人以为表变了。
func (t *AttrPointEffectTable) AttrPointKeys() []string {
	seen := make(map[string]struct{}, len(t.rows))
	keys := make([]string, 0, len(t.rows))
	for _, row := range t.rows {
		k := row.GetAttrPointKey()
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// AttrPointBonus 是一份属性加点换算出来的单条属性加成(展示 / 校验用聚合结果)。
type AttrPointBonus struct {
	AttrKey string
	Value   float32
}

// ResolveBonuses 把一份属性加点(属性点键 → 已分配点数)换算成属性加成合计。
//
// 单条属性点的加成 = 点数 × 该属性点每条效果的每点数值;同一战斗属性跨属性点累加
// (如"体质"与"精神"都加 Hp)。结果按属性名定序,保证同一份分配每次输出一致
// (map 遍历顺序不稳定会让下发内容抖动、测试随机失败)。
//
// 分配里出现本表没有效果行的键是合法的(旧数据 / 表还没配),跳过即可,不报错 ——
// 服务端这条读路径不该因为一条脏键就整份算不出来。写路径由 HasAttrPointKey 挡在门外。
//
// 点数为负是脏数据(落库列约束 >=0),按 0 处理:负点数会变成负加成,
// 表现为"洗点之后角色反而更弱",比少算一条更难查。
func (t *AttrPointEffectTable) ResolveBonuses(points map[string]int32) []AttrPointBonus {
	sum := make(map[string]float32)
	for pointKey, p := range points {
		if p <= 0 {
			continue
		}
		for _, row := range t.ListByAttrPointKey(pointKey) {
			sum[row.GetAttrKey()] += float32(p) * row.GetValuePerPoint()
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

	out := make([]AttrPointBonus, 0, len(keys))
	for _, k := range keys {
		out = append(out, AttrPointBonus{AttrKey: k, Value: sum[k]})
	}
	return out
}
