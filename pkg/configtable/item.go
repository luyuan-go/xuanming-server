package configtable

import (
	"fmt"
	"math"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// item.go — ItemTable 手写伴生文件。
// 首次由 configtable-gen 创建(仅当文件不存在),此后归人维护,生成器不再覆盖。
// 表私有的逐行业务校验写在 validateItemRow;域方法(业务语义查询)也加在本文件。
//
// 视图结构与通用访问 API(All/ByID/Exists/Count/ByIDs/RandOne/Where/First)在
// item_table.gen.go(tools/configtable-gen 生成,勿手改)。

const (
	maxClientFixedHealHP uint32 = 1<<31 - 1
)

// validateItemRow 逐行业务校验(生成的 newItemTable 调用;
// 主键非零/唯一已由生成代码兜住,类型/必填/枚举已由生成器在导表阶段校验)。
//
// 与生成阶段校验重复是有意的 fail-closed:服务端不信任产物一定出自本生成器。
// 整表任一行不过即拒新版本、保留旧表(§9.15 加载失败不切换),坏配置挡在加载边界。
func validateItemRow(row *configpb.ItemRow) error {
	if row.GetName() == "" {
		return fmt.Errorf("名称(name)为空")
	}
	if row.GetType() == configpb.ItemType_ITEM_TYPE_UNSPECIFIED {
		return fmt.Errorf("类型(type)未填")
	}
	if row.GetMaxStackSize() == 0 {
		return fmt.Errorf("堆叠上限(max_stack_size)为 0,该道具无法进入任何背包")
	}

	// 装备类不变量:「装备部位 > 0」与「类型 = 装备」必须同时成立。
	// 否则 player.SetEquipment 会出现互相矛盾的判定(按 type 说不是装备、按 equip_slot 说能穿),
	// 客户端 CfgItem 与服务端校验的口径也会就此分叉。
	isEquipType := row.GetType() == configpb.ItemType_ITEM_TYPE_EQUIPMENT
	hasSlot := row.GetEquipSlot() > 0
	if isEquipType != hasSlot {
		return fmt.Errorf("装备类不一致:类型(type=%v)与装备部位(equip_slot=%d)必须同时成立或同时不成立",
			row.GetType(), row.GetEquipSlot())
	}
	// 装备必须不可堆叠:堆叠合并会改写实例 guid,破坏强化 / 词条跟随。
	// 与客户端 UMyBagComponent::ServerEquipItem 的运行期校验是同一条约束,
	// 在这里挡住 = 把「客户端运行时才报错」提前成「坏表拒绝加载」。
	if hasSlot && row.GetMaxStackSize() != 1 {
		return fmt.Errorf("装备部位 > 0 的道具堆叠上限必须为 1,实为 %d", row.GetMaxStackSize())
	}
	if isEquipType != (row.GetIdentifyPoolId() > 0) {
		return fmt.Errorf("鉴定池不一致:装备必须配置 identify_pool_id>0,非装备必须为0,实为 %d",
			row.GetIdentifyPoolId())
	}

	// 可见模型必须成对配置。资源是否真实存在、角色是否具备对应 Socket/Bone 由 UE
	// LoadObject/DoesSocketExist 再次 fail-closed；服务端这里只挡住半截路径和坏变换。
	hasMesh := row.GetEquipMesh() != ""
	hasSocket := row.GetEquipSocket() != ""
	if hasMesh != hasSocket {
		return fmt.Errorf("装备模型(equip_mesh)与挂点(equip_socket)必须同时为空或同时非空")
	}
	if !isEquipType && (hasMesh || hasSocket) {
		return fmt.Errorf("非装备不得配置装备模型/挂点")
	}
	finiteBounded := func(name string, value float32, limit float64) error {
		v := float64(value)
		if math.IsNaN(v) || math.IsInf(v, 0) || math.Abs(v) > limit {
			return fmt.Errorf("%s 必须为有限数且绝对值 <= %g,实为 %v", name, limit, value)
		}
		return nil
	}
	type namedFloat struct {
		name  string
		value float32
	}
	for _, field := range []namedFloat{
		{"装备偏移X", row.GetEquipOffsetX()}, {"装备偏移Y", row.GetEquipOffsetY()}, {"装备偏移Z", row.GetEquipOffsetZ()},
	} {
		if err := finiteBounded(field.name, field.value, 100_000); err != nil {
			return err
		}
	}
	for _, field := range []namedFloat{
		{"装备旋转Yaw", row.GetEquipYaw()}, {"装备旋转Pitch", row.GetEquipPitch()}, {"装备旋转Roll", row.GetEquipRoll()},
	} {
		if err := finiteBounded(field.name, field.value, 36_000); err != nil {
			return err
		}
	}
	for _, field := range []namedFloat{
		{"装备缩放X", row.GetEquipScaleX()}, {"装备缩放Y", row.GetEquipScaleY()}, {"装备缩放Z", row.GetEquipScaleZ()},
	} {
		if err := finiteBounded(field.name, field.value, 100); err != nil {
			return err
		}
		if hasMesh && field.value <= 0 {
			return fmt.Errorf("%s 必须 > 0,实为 %v", field.name, field.value)
		}
	}

	// 回血类型与数值必须形成唯一合法组合。百分比使用独立列，使旧 DS 看到新类型时
	// use_heal_hp=0 并 fail-closed，不会把 20% 错当成固定回血 20 点。
	switch row.GetUseHealType() {
	case configpb.ItemHealType_ITEM_HEAL_TYPE_UNSPECIFIED,
		configpb.ItemHealType_ITEM_HEAL_TYPE_FIXED:
		if row.GetUseHealMaxHpPercent() != 0 {
			return fmt.Errorf("固定回血类型要求使用回血百分比(use_heal_max_hp_percent)为 0,实为 %d",
				row.GetUseHealMaxHpPercent())
		}
		if row.GetUseHealHp() > maxClientFixedHealHP {
			return fmt.Errorf("固定使用回血量(use_heal_hp=%d)超过客户端 int32 上限 %d",
				row.GetUseHealHp(), maxClientFixedHealHP)
		}
		if row.GetUsable() && row.GetUseHealHp() == 0 {
			return fmt.Errorf("可使用(usable)为真但固定使用回血量(use_heal_hp)为 0,该道具使用后无任何效果")
		}
		if !row.GetUsable() && row.GetUseHealHp() != 0 {
			return fmt.Errorf("不可使用道具不得配置固定使用回血量(use_heal_hp=%d)", row.GetUseHealHp())
		}
	case configpb.ItemHealType_ITEM_HEAL_TYPE_MAX_HP_PERCENT:
		if !row.GetUsable() {
			return fmt.Errorf("最大生命百分比回血要求可使用(usable)为真")
		}
		if row.GetUseHealHp() != 0 {
			return fmt.Errorf("最大生命百分比回血要求固定使用回血量(use_heal_hp)为 0,实为 %d",
				row.GetUseHealHp())
		}
		if percent := row.GetUseHealMaxHpPercent(); percent == 0 || percent > 100 {
			return fmt.Errorf("最大生命回血百分比(use_heal_max_hp_percent)必须在 1..100,实为 %d", percent)
		}
	default:
		return fmt.Errorf("未知使用回血类型(use_heal_type=%d),仅支持 0=未配置/旧版固定、1=固定值、2=最大生命百分比",
			row.GetUseHealType())
	}
	return nil
}

// IsEquipment 判断道具是否为可穿戴装备;行不存在返回 false(fail-closed)。
func (t *ItemTable) IsEquipment(itemConfigID uint32) bool {
	row, ok := t.byID[itemConfigID]
	return ok && row.GetEquipSlot() > 0
}

// EquipSlotOf 取道具的装备部位;行不存在或不可穿戴返回 0。
func (t *ItemTable) EquipSlotOf(itemConfigID uint32) uint32 {
	row, ok := t.byID[itemConfigID]
	if !ok {
		return 0
	}
	return row.GetEquipSlot()
}

// MatchesSlot 判断道具能否装进指定部位:必须存在、可穿戴、且部位号完全一致。
//
// 这是 player.SetEquipment「是不是装备 + 部位对不对」两项校验的唯一入口。
// 未知道具一律不匹配(fail-closed),不给热更缺行留后门。
func (t *ItemTable) MatchesSlot(itemConfigID uint32, slot uint32) bool {
	if slot == 0 {
		return false
	}
	row, ok := t.byID[itemConfigID]
	if !ok {
		return false
	}
	return row.GetEquipSlot() == slot
}
