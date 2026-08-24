# 决策:装备基础属性(装备属性表.xlsx)

**日期**:2026-08-22
**状态**:已落码(UE 编译与资产创建待用户执行,见 §7)
**源表**:`Pandora-Client-SVN/Table/道具/装备属性表.xlsx`(策划维护,SVN r2180 起在版本控制下)

---

## 1. 需求与既有基线

策划新表给 120 件装备(item id 10066..10185)各配了 6 个百分比:
`HP / 伤害 / 治疗 / 暴击 / 技能 / 控场`(0.07 = +7%)。要求穿上后**实际生效**,并在**背包装备信息里显示**。

改动前的基线(全部实测确认):

| 事实 | 证据 |
|---|---|
| 装备**基础**属性在两个仓库里完全不存在 | `FCfgItem` 只有部位 / 鉴定池 / 外观列;`Public/Table/` 下无任何 `CfgEquip*` |
| 生效的只有**随机鉴定词条**,且只认 3 个 attr_id | `UMyBagComponent::ResolveEquipmentAttributeTarget`:3→Atk、9→Defense、7→MoveSpeedRate |
| 没有 MaxHp 属性,`Hp` 被钳到配表 `CfgRoleLv->Hp` | `UMyEntityAttrSet::CheckClampAttr`,且挂在 `PreAttributeChange` 与 `PreAttributeBaseChange` 两个 hook 上 |
| 暴击是**死属性** | `UMyAtkHitExeCalc` 捕获了 CritChance/CritDamage 却在 `CalcDamage` 里一个都没读 |
| 治疗 / 技能强度 / 控场 三个概念全仓为零 | 治疗只有 Buff 表对 Hp 挂固定值;控场只存在于技能 Tips 的文案反推 |
| 新表 A~E + L 六列是 `d_道具.xlsx` 的**逐行逐字节冗余副本** | 120/120 行 name/quality/equip_slot/icon/description 完全一致 |
| 道具表有 151 件装备,新表只覆盖 120 件 | 10003 与 10027..10056 共 31 件老装备刻意没有基础属性 |

## 2. 拍板结论(用户 2026-08-22)

1. **治疗 = 技能治疗量;技能 = 技能伤害;控场 = 技能控制时间**。三者都是"施加者放大自己打出去的效果"。
2. **暴击顺带补进伤害公式**——不补的话暴击列是纯装饰。
3. **服务端登记本表并做跨表校验**,尽管服务端不消费这 6 个数值。

## 3. 数值不下发,两端各自查表

沿用专精效果表(`talent_effect`)与技能卡效果表(`skill_card_effect`)已经证明的范式:

```
player.GetLoadout  →  只送身份(item_config_id)
Battle/Hub DS      →  自己查 CfgEquipAttr,自己换算成 GAS 增量
```

**proto 零改动、RPC 零改动**。理由:

- 基础属性是 **per-config** 的纯配置,同一 item_config_id 的所有实例恒等,不存在"服务端才知道"的信息;
  下发它等于把同一份配置再复制一条通道(违反 §9.22 唯一权威);
- 战斗数值权威本就在客户端(§9.6),服务端不算任何战斗数值;
- 鉴定词条走协议是因为它是 **per-instance 随机 roll**,服务端 roll 完只有服务端知道 —— 两者不是同一类事实。

## 4. 两种百分比口径(刻意不统一)

| 列 | 落点 | 口径 |
|---|---|---|
| HP | `MaxHp` | **基础值百分比**:`CfgRoleLv->Hp × 0.07` 换成绝对值加算 |
| 伤害 | `Atk` | **基础值百分比**:`CfgRoleLv->Atk × 0.19` |
| 暴击 | `CritChance` | **效果百分比**:直接加 0.19(= +19 个百分点的暴击率) |
| 治疗 | `SkillHealRate` | **效果百分比**:直接加算 |
| 技能 | `SkillDamageRate` | **效果百分比**:直接加算 |
| 控场 | `SkillControlRate` | **效果百分比**:直接加算 |

差别是刻意的:「+7% 生命」只能相对某个基准,而「+19% 技能伤害」本身就是比例。
基准取**角色等级表裸体值**而不是 ASC 当前值 —— 当前值里已经含上一件装备加过的量,
拿它当基准会让第二件装备按被放大后的基数再加一次(复利)。

多件装备的同一项**加算**(0.07 + 0.12 = +19%),不是乘算(1.07 × 1.12)。
与专精 / 技能卡 / 鉴定词条全部同一口径,玩家把面板加起来才对得上。

## 5. 新增的 4 个 GAS 属性与它们的唯一消费点

权威定义在 `Pandora-Client-SVN/.../Public/GAS/MyEntityAttrSet.h`;服务端校验副本在
`pkg/configtable/combat_attr_key.go`(专精 / 技能卡 / 属性加点三张效果表共用,§9.22)。

| 属性 | 复制 | 唯一消费点 |
|---|---|---|
| `MaxHp` | **COND_ReplayOrOwner(只发给自己)**,且声明在 AtkSpeedRate 之后 | `CheckClampAttr` 的 Hp 上限;`UMyAttrStatics::ResolveEntityMaxHp` 统一供 5 个 UI / 道具百分比回血 |
| `SkillDamageRate` | 不复制(只在权威端裁决) | `UMyAtkHitExeCalc::CalcDamage`,放大整段技能伤害(含技能固定值) |
| `SkillHealRate` | 不复制 | `UMyEntityAttrSet::PreGameplayEffectExecute`,放大加算型正向 Hp 修改 |
| `SkillControlRate` | 不复制 | `UMyBuffDurMmc`,只放大 `UMyBuffStatics::IsControlBuff` 判真的 Buff 时长 |

### 复制口径:只发给自己,别人那边保持原样(2026-08-23 拍板)

用户口径:**「我只说我们装备的属性」「不要变人家原来的东西」** —— 装备属性是本次新增的能力,
不能因此改变别的客户端上任何既有表现。落实成两条:

1. **`MaxHp` 用 `COND_ReplayOrOwner`(同 `MoveSpeedRate`),不是 `COND_None`。**
   别的客户端收不到这一条,`UMyAttrStatics::ResolveEntityMaxHp` 就回退 `CfgRoleLv->Hp` ——
   那正是本属性存在之前的行为,逐字节一致。怪物/NPC 的 ASC 没有 owner 连接,对所有人都不发,同样保持原状。
   **代价(已向用户说明并确认)**:队友穿了加生命的装备时,你看到的他的血条分母仍是配表裸值,条会填不满。
   战斗裁决不受影响 —— 伤害与死亡判定全在权威端,那里恒有真值。
2. **`MaxHp` 声明在 `AtkSpeedRate` 之后**,不插在 `Hp` 与 `Shield` 之间。
   复制句柄(RepIndex)按属性声明顺序编号,插在中间会让 `Shield` / `MoveSpeedRate` / `AtkSpeedRate`
   的既有句柄整体后移一格 —— 那就是在动既有属性。追加到末尾则既有句柄一个不动。
   (这不能省掉协议版本隔离:旧客户端根本没有这一条,收到该句柄仍解析不了。)

其余 3 个新属性 `SkillDamageRate` / `SkillHealRate` / `SkillControlRate` **完全不复制**,
只在权威端参与裁决,连自己的客户端都收不到。

### 本次改动对「没穿新装备的人」是零变化(可逐条验证)

| 改的地方 | 不穿新装备时的行为 |
|---|---|
| `CheckClampAttr` 的 Hp 上限改读 MaxHp | `MaxHp` 初值 = `CfgRoleLv->Hp`,上限完全相同 |
| 5 处 UI / 道具回血改走 `ResolveEntityMaxHp` | 拿不到或等于配表值,输出相同 |
| `CalcDamage` 加入暴击判定 | `CritChance` 全表为 0,没有装备就永不触发 |
| `CalcDamage` 乘 `(1 + SkillDamageRate)` | 该属性裸体值 0,恒 ×1 |
| `PreGameplayEffectExecute` 放大治疗 | `SkillHealRate` 为 0 时早退,不改任何量值 |
| `UMyBuffDurMmc` 控场时长加成 | `SkillControlRate` 为 0 时原样返回 |
| `MyActionTypes` 控场判定转交共享件 | 判据逐字搬运,行为相同 |
| 两个增量结构体合并成别名 | 字段与语义不变(既有测试的按位置初始化已改成逐字段赋值) |

### 为什么 MaxHp 必须是属性

`Hp` 原先被钳到配表 `CfgRoleLv->Hp`,而那两个钳制 hook 正是 `SetNumericAttributeBase` /
`ApplyModToAttribute` 的必经之路 —— **任何加到 Hp 上的持久加成都会被立刻钳回裸表值**,
表现为"配了没反应"且零报错。天赋白名单里本就有 `Hp`,所以这条一直是潜在的静默失效。
生命上限做成属性之后,配表纪律是:**生命类加成一律配 `MaxHp`,不配 `Hp`**。

### 治疗为什么落在 PreGameplayEffectExecute

本项目**没有治疗 ExecCalc**:治疗是 Buff 配表直接对 `Hp` 挂固定值 modifier
(`FCfgBuffModifier`),那条路径上没有任何位置能读到施加者属性。
`PreGameplayEffectExecute` 是全项目唯一同时看得到「这次改了多少血」与「是谁改的」的收口。

只放大**加算型正向**修改:Override 型治疗是"把血设成某个值",按比例放大会改变语义;
道具回血走 `ApplyModToAttribute` 根本不经该回调,因此天然不吃技能治疗加成——**道具不是技能**。

### 控场判定收口

「这个 Buff 算不算控场」原先只写在技能 Tips 的文案生成里。现在收口到
`UMyBuffStatics::IsControlBuff`(眩晕标签 / 定身标签 / MoveSpeedRate 乘算且倍率<1 的减速),
Tips 与时长加成共用同一份判据,不会出现"Tips 说是控场但加成不生效"。

## 6. 暴击补进伤害公式

`暴击倍率 = 1 + CritDamage`,随机只在权威端发生(伤害 GE 由 DS 施加,客户端拿到的是复制后的 Hp)。

### 暴击伤害列(2026-08-23 已填)

改动前 `j_角色等级.xlsx` 的「暴击伤害」列只有 1011 冰法·新 与 1013 冰法2号 填了 `1`,
其余 28 行是**空**(导出后为 0),1014 悟空显式填 0 —— 也就是"两个英雄能暴、其余暴了没伤害"的
不一致状态,而不是整列空白。

2026-08-23 按**表内已有先例**统一为:**所有 `entity_type=1`(玩家英雄)的 14 行 `暴击伤害 = 1`**
(= 暴击打双倍)。取 1 而不是别的数,是因为 1011/1013 这两行是策划自己填的,项目内已有口径优先于外部惯例。

**怪物与 NPC 刻意保持空白**(2/3/2001..2101/6001/3001 雅典娜):它们的「暴击率」全表为 0 且没有装备,
`CritDamage` 永远不会被消费,填了只是噪音;真要给怪物暴击,应当先配「暴击率」再配这一列。
因此本次改表**对怪物战斗数值零影响**。

代码侧保留了那条**只报一次**的 Warning(`[暴击] 触发暴击但暴击伤害=0`):它现在是防回退的哨兵 ——
将来谁把这一列清空、或给怪物配了暴击率却没配暴击伤害,会立刻在日志里可见,而不是静默无手感。

## 7. 两条装备数值路径与"挂在哪"

改动前装备词条有两条互斥来源(`FMyEquipmentGameplaySourcePolicy`):Bag type 2 主路径、
GetLoadout 迁移 fallback。**基础属性不进这两条路径**,而是挂在
`UMyEquipmentVisualComponent::Equipment` —— 那是 `AMyEntityPlayerState::ReconcileEquipmentSources`
在两个来源之间**已经二选一之后**的那一份可见装备。

理由:基础属性只取决于 `item_config_id`,两个来源给出的结果恒等;挂在已收敛的那一份上
天然不可能双计,也不需要再走一遍互斥策略。撤销 / 重放直接复用既有的
`AppliedGameplayDeltas` 缓存,升级重基、重生、换关卡三条链一并覆盖。

两条路径的增量结构体已合并成同一个 `FMyEquipGameplayDeltas`
(`FMyLoadoutGameplayDeltas` / `FMyEquipmentGameplayDeltas` 都是它的别名):
形状本来就一致,各写一份的后果是"新增属性只改了其中一条路径",而那正是重放链最容易漏且不报错的地方。

## 8. 服务端为什么仍登记这张表

服务端**不消费**这 6 个数值。登记的唯一目的是加载期挡住策划表漂移,与 `item.proto` 登记
`equip_mesh` / `equip_socket` 等纯 UE 渲染列是同一条理由:

1. 表头改名 / 加列 / 删列在导表阶段整批拒绝,而不是等 DS 上表现异常;
2. `ValidateEquipmentAttrCrossTables`:装备ID 必须是道具表里真实存在的**装备**,
   且 `装备部位` / `品质` 与道具表逐行一致。注意真实风险**不是**"客户端会读错部位"——
   本表的 `equip_slot` / `quality` 在客户端运行期根本没有消费点(UE 侧只读 6 个百分比,
   外观挂载与穿戴校验一律读道具表的 `FCfgItem::EquipSlot`,且那条 fail-closed 分支带 Warning 日志)。
   风险是"策划以为改了本表的部位就改了这件装备的部位"——改了个没人读的副本,两张表就此分叉,
   而加载期是唯一能挡住它的地方。

刻意**不**校验的:名称 / 图标 / 描述文本三列只查非空 —— 它们不参与任何判定,
为一次纯文案改动拒绝整批配置、卡住服务启动,代价不成比例。
「每件装备都必须有属性行」也不成立(31 件老装备刻意没有)。

## 9. 待用户执行

1. **UE 编译**(项目纪律:UE 编译由用户本人执行)。
2. **创建 DataTable 资产**:编译后 `pwsh tools/scripts/equip_attr_csv.ps1` 出 CSV,
   在编辑器 `Content/Pkg/Cfg/Table/Cpp` 下 Import,行结构选 `CfgEquipAttr`,
   **资产名必须是全小写 `cfgequipattr`**。之后改表走编辑器「生成所有配置」(Alt+Y)自动重填。
3. ~~SVN 新增文件需 add~~ —— 2026-08-23 已 `svn add`(6 个新 .h/.cpp,**仅调度未提交**;提交仍由用户执行)。
4. ~~策划补 `j_角色等级.xlsx` 的「暴击伤害」列~~ —— 2026-08-23 已填(14 个玩家英雄统一为 1 = 双倍),见 §6。

## 关联

- `ds-arch.md §0.5` 开战前养成快照下发契约(为什么身份下发、数值不下发)
- `CLAUDE.md §9.6` 派生数值一律服务端计算 / 战斗数值权威在客户端
- `CLAUDE.md §9.22` 唯一权威,不重复存储影子状态
- `config-table-hotreload.md` 配置表流水线契约
- `pkg/configtable/combat_attr_key.go` 战斗属性集校验副本(增删属性必须同步)
