"""player 的配置表快照 —— 对应 Go 侧 cmd/player/main.go 里注册的整批校验器 +
pkg/configtable 的七张表伴生方法(player_level_exp / item / talent / talent_effect /
skill_card / skill_card_upgrade / skill_card_effect)。

★ 为什么 player 必须**强依赖**配置表(main.py 缺 config_table.dir 直接拒启):
  玩家等级经验曲线的唯一数值源是策划 j_玩家等级经验.xlsx。player 不保留 YAML 兜底曲线
  —— 一份可能与客户端漂移的兜底数值参与升级结算,比拒掉一次启动危险得多。

★ 加载语义逐条照抄 Go 的 Store.Load,任一失败**整批不切换**:
  manifest 缺表 / proto 名不符 / 文件缺失 / checksum 不符 / protojson 解析失败 /
  行数与 manifest 声明不符 / 逐行校验不过 / 整批校验器不过。
  manifest 未列出的 *.json 视为脏数据,**只告警不拒载**(hotreload doc §5)。

★ 复用 `pandorapy.configtable` 的 read_manifest / verify_checksum,不再抄一份:
  checksum 的字节口径(含尾换行的 LF 全字节 sha256)已经踩过一次事故,两份实现必然
  漂移,而漂移的表现是"Go 版拒载的批次 Python 版放行"。

★ 逐行校验(validate_*_row)在 Go 侧由生成代码调用,这里手写在同一处 —— 少一条的后果
  在每个函数的注释里逐条写明,都是"配错了不报错、只是玩法悄悄失效"那一类。
"""

from __future__ import annotations

import dataclasses
import pathlib

from google.protobuf import json_format

from pandora.config.v1 import item_pb2 as _item_pb2
from pandora.config.v1 import player_level_exp_pb2 as _lvl_pb2
from pandora.config.v1 import skill_card_pb2 as _card_pb2
from pandora.config.v1 import skill_card_effect_pb2 as _ceff_pb2
from pandora.config.v1 import talent_effect_pb2 as _teff_pb2
from pandora.config.v1 import talent_pb2 as _talent_pb2

from pandorapy.configtable import (
    MANIFEST_FILE_NAME,
    ConfigTableError,
    read_manifest,
    verify_checksum,
)

# ── 与 Go 逐字同值的上限常量 ─────────────────────────────────────────────────

# 玩家等级表的等级数上下限(Go: PlayerLevelExpTable.ValidateCurve)。
MIN_PLAYER_LEVELS = 2
MAX_PLAYER_LEVELS = 200
# cumulative_exp / upgrade_exp 都是 uint32 列,累计不得越界。
UINT32_MAX = (1 << 32) - 1

# 专精:等级上限与每级消耗的硬约束(Go: MaxTalentLevel / MaxTalentCostPerLevel)。
# 二者相乘把单节点总消耗钳在 10 万以内,远离 uint32 上界 —— 不钳的话,一张热更进来
# 的超大表能让总消耗在 uint32 上回绕,回绕后反而"点得起"。
MAX_TALENT_LEVEL = 100
MAX_TALENT_COST_PER_LEVEL = 1000

# 专精效果:单级加成绝对值上限(Go: MaxTalentEffectValuePerLevel)。
MAX_TALENT_EFFECT_VALUE_PER_LEVEL = 10000

# 战斗属性集(专精效果 / 技能卡效果两张表共用的 GAS 属性名白名单;Go: combatAttrKeys)。
# **权威在客户端** UMyEntityAttrSet;这里是校验用副本,不是第二份权威 ——
# 服务端不消费这些数值,但必须在加载期挡住拼错的键:attr_key 写错在 DS 上的表现是
# "这个天赋点了 / 这张卡升了完全没反应",既不报错也不崩。两张表各存一份必然漂移,只留一份。
COMBAT_ATTR_KEYS = frozenset(
    {
        "Hp",  # 当前血量;加成请配 MaxHp,写 Hp 会被 DS 侧按上限钳掉
        "MaxHp",
        "Atk",
        "Defense",
        "Shield",
        "CritChance",
        "CritDamage",
        "HitChance",
        "DodgeChance",
        "MoveSpeedRate",
        "AtkSpeedRate",
        "SkillDamageRate",
        "SkillHealRate",
        "SkillControlRate",
    }
)

# 技能卡效果:单级加成绝对值上限(Go: MaxSkillCardEffectValuePerLevel)。
MAX_SKILL_CARD_EFFECT_VALUE_PER_LEVEL = 10000

# 技能卡稀有度(Go: SkillCardRarity*)。决定走哪条升级消耗曲线。
SKILL_CARD_RARITIES = frozenset({1, 2, 3, 4})
# 等级上限的上限:每一级都要有一行消耗,填大了会让 ValidateCurves 报一堆"缺消耗行"。
MAX_SKILL_CARD_LEVEL = 30

_TABLE_PROTOS: dict[str, tuple[str, type]] = {
    "player_level_exp": (
        "pandora.config.v1.PlayerLevelExpTableData",
        _lvl_pb2.PlayerLevelExpTableData,
    ),
    "item": ("pandora.config.v1.ItemTableData", _item_pb2.ItemTableData),
    "talent": ("pandora.config.v1.TalentTableData", _talent_pb2.TalentTableData),
    "talent_effect": (
        "pandora.config.v1.TalentEffectTableData",
        _teff_pb2.TalentEffectTableData,
    ),
    "skill_card": ("pandora.config.v1.SkillCardTableData", _card_pb2.SkillCardTableData),
    "skill_card_upgrade": (
        "pandora.config.v1.SkillCardUpgradeTableData",
        _card_pb2.SkillCardUpgradeTableData,
    ),
    "skill_card_effect": (
        "pandora.config.v1.SkillCardEffectTableData",
        _ceff_pb2.SkillCardEffectTableData,
    ),
}

# player 必需的表。talent_effect / skill_card_effect **刻意不在此列**:效果表为空等于
# "天赋 / 技能卡只存数据不加数值",是合法的过渡态,不该让 player 起不来
# (§9.6 数值权威本就不在这里)。
REQUIRED_TABLES = (
    "player_level_exp",
    "item",
    "talent",
    "skill_card",
    "skill_card_upgrade",
)


# ── 已校验的原子批次 ─────────────────────────────────────────────────────────


@dataclasses.dataclass(slots=True)
class Tables:
    """一个已校验的原子批次。热更时整份替换,读侧永远看到自洽的一批。"""

    version: int
    source_rev: str
    # level → PlayerLevelExpRow(等级连续,已由 validate 保证)
    levels: dict[int, _lvl_pb2.PlayerLevelExpRow]
    items: dict[int, _item_pb2.ItemRow]
    talents: dict[int, _talent_pb2.TalentRow]
    talent_effects: list[_teff_pb2.TalentEffectRow]
    skill_cards: dict[int, _card_pb2.SkillCardRow]
    # (rarity, level) → shard_cost
    card_upgrade: dict[tuple[int, int], int]
    # 技能卡效果行(缺表 = 技能卡只给技能不加数值,是合法过渡态,故给默认空表)。
    skill_card_effects: list[_ceff_pb2.SkillCardEffectRow] = dataclasses.field(
        default_factory=list
    )

    # ── 玩家等级经验 ────────────────────────────────────────────────────

    def max_level(self) -> int:
        """当前批次的最高玩家等级 = 表行数(等级从 1 连续递增,已校验)。"""
        return len(self.levels)

    def experience_curve(self) -> list[int]:
        """AdvanceExperience 使用的 Lv1→末级曲线快照。

        curve[i] = 从 Lv(i+1) 升到 Lv(i+2) 所需级内经验;max_level = len(curve)+1。
        返回新列表 —— 调用方可安全持有到本次事务结束,不受热更换批影响。
        """
        count = len(self.levels)
        if count < MIN_PLAYER_LEVELS:
            return []
        curve: list[int] = []
        for level in range(1, count):
            row = self.levels.get(level)
            if row is None:
                return []
            curve.append(int(row.upgrade_exp))
        return curve

    # ── 道具(SetEquipment 的 isEquip / slotMatch)────────────────────────

    def matches_slot(self, item_config_id: int, slot: int) -> bool:
        """道具能否装进指定部位:必须存在、可穿戴(equip_slot>0)、且部位号完全一致。

        未知道具一律不匹配(fail-closed),不给热更缺行留后门。
        """
        if slot == 0:
            return False
        row = self.items.get(item_config_id)
        if row is None:
            return False
        return int(row.equip_slot) == slot

    # ── 专精 ────────────────────────────────────────────────────────────

    def validate_talent_allocation(self, levels: dict[int, int]) -> tuple[dict[int, int], int]:
        """校验一份完整专精分配,返回 (逐节点消耗, 总消耗)。

        ★ 全量替换语义:每次提交都是一份完整方案,因此前置关系**只看本次方案自身**,
          不看库里旧数据 —— 否则"先点满前置、再单独洗掉前置"就能留下悬空节点。
        ★ 逐节点消耗要随分配一起落库:读取侧据此还原已花点数,不再按 Σ 等级 反推
          (cost_per_level≠1 时反推会算少,玩家看到的可点数比实际多)。
        ★ **一次调用取一份表快照**(self 就是那份快照),校验与消耗必然同版本;
          分两次查表则可能跨热更边界混算(§9.15)。

        任一条不满足抛 ConfigTableError。ID 先排序再遍历 —— 保证同一份非法分配每次
        报同一条错误(dict 顺序在 Python 里稳定,但 Go 侧 map 不稳定才特意排序,
        两边报同一条错误才对得上)。
        """
        costs: dict[int, int] = {}
        total = 0
        for talent_id in sorted(levels):
            level = levels[talent_id]
            row = self.talents.get(talent_id)
            if row is None:
                raise ConfigTableError(f"专精 {talent_id} 不在配置表中")
            if level == 0:
                raise ConfigTableError(f"专精 {talent_id} 等级为 0(等级 0 应从分配中移除)")
            if level > int(row.max_level):
                raise ConfigTableError(
                    f"专精 {talent_id} 等级 {level} 超过上限 {int(row.max_level)}"
                )
            req_id = int(row.require_talent_id)
            if req_id != 0:
                got = levels.get(req_id, 0)
                if got < int(row.require_talent_level):
                    raise ConfigTableError(
                        f"专精 {talent_id} 需要前置专精 {req_id} 达到 "
                        f"{int(row.require_talent_level)} 级,本次方案只有 {got} 级"
                    )
            node_cost = level * int(row.cost_per_level)
            costs[talent_id] = node_cost
            total += node_cost
            if total > UINT32_MAX:
                raise ConfigTableError(
                    f"专精总消耗超出上限(累加到节点 {talent_id} 时已达 {total})"
                )
        return costs, total

    # ── 技能卡 ──────────────────────────────────────────────────────────

    def card_exists(self, card_id: int) -> bool:
        return card_id in self.skill_cards

    def upgrade_curve(self, card_id: int) -> tuple[dict[int, int], int]:
        """返回该卡的升级曲线(目标等级 → 碎片消耗)与等级上限。

        ★ 返回整条曲线而不是单级价钱:实际消耗取决于**事务内锁到的**当前等级,
          先读等级再算价会有 TOCTOU —— 两次并发升级都按同一级的价钱扣。
        ★ 缺档抛错而不是当 0:加载期 ValidateCurves 已挡过一道,走到这里说明热更换了
          表且表和上限对不上。缺档绝不能当免费升级。
        """
        card = self.skill_cards.get(card_id)
        if card is None:
            raise ConfigTableError(f"unknown skill card {card_id}")
        max_level = int(card.max_level)
        rarity = int(card.rarity)
        curve: dict[int, int] = {}
        for level in range(2, max_level + 1):
            cost = self.card_upgrade.get((rarity, level))
            if cost is None:
                raise ConfigTableError(
                    f"upgrade curve missing: card={card_id} rarity={rarity} level={level}"
                )
            curve[level] = cost
        return curve, max_level


@dataclasses.dataclass(slots=True)
class LoadResult:
    version: int
    source_rev: str
    warnings: list[str]
    tables: Tables


# ── 逐行校验(Go 侧由生成代码调用 validate*Row)────────────────────────────


def _validate_player_level_exp_row(row) -> None:  # noqa: ANN001
    """ID 必须与等级一致 —— 否则 by-id 索引与「第 N 级」这个语义脱钩,曲线整体错位。"""
    if int(row.id) != int(row.level):
        raise ConfigTableError(f"ID {int(row.id)} 必须与等级 {int(row.level)} 一致")


def _validate_talent_row(row) -> None:  # noqa: ANN001
    """专精逐行校验。少任一条的后果:

    name 空          → 界面上一个没名字的节点,策划自己都不知道是哪个;
    max_level=0      → 该专精永远点不出来(不报错,只是点不动);
    max_level 超限   → 总消耗可在 uint32 上回绕,回绕后玩家"点得起"任意方案;
    cost_per_level=0 → 该专精可无限点满;
    自引用前置       → 必然无解,单行即可判定,不必等环检测;
    前置 ID/等级只填一半 → 一半是"无前置却要求等级"(死条件),一半是"有前置却不要求
                        等级"(等价没前置),两种都是填错而不是省略。
    """
    if not row.name:
        raise ConfigTableError(f"专精 {int(row.id)}: 名称(name)为空")
    if int(row.max_level) == 0:
        raise ConfigTableError(f"专精 {int(row.id)}: 等级上限(max_level)为 0,该专精永远点不出来")
    if int(row.max_level) > MAX_TALENT_LEVEL:
        raise ConfigTableError(
            f"专精 {int(row.id)}: 等级上限(max_level={int(row.max_level)})超过上限 "
            f"{MAX_TALENT_LEVEL}(防总消耗溢出)"
        )
    if int(row.cost_per_level) == 0:
        raise ConfigTableError(f"专精 {int(row.id)}: 每级消耗(cost_per_level)为 0,该专精可无限点满")
    if int(row.cost_per_level) > MAX_TALENT_COST_PER_LEVEL:
        raise ConfigTableError(
            f"专精 {int(row.id)}: 每级消耗(cost_per_level={int(row.cost_per_level)})超过上限 "
            f"{MAX_TALENT_COST_PER_LEVEL}(防总消耗溢出)"
        )
    if int(row.require_talent_id) == int(row.id):
        raise ConfigTableError(f"专精 {int(row.id)}: 前置专精(require_talent_id)指向自身")
    if int(row.require_talent_id) == 0 and int(row.require_talent_level) != 0:
        raise ConfigTableError(
            f"专精 {int(row.id)}: 无前置专精却填了前置等级"
            f"(require_talent_level={int(row.require_talent_level)})"
        )
    if int(row.require_talent_id) != 0 and int(row.require_talent_level) == 0:
        raise ConfigTableError(
            f"专精 {int(row.id)}: 填了前置专精({int(row.require_talent_id)})却未填前置等级"
        )


def _validate_talent_effect_row(row) -> None:  # noqa: ANN001
    """专精效果逐行校验。attr_key 不在白名单 = 该效果在 DS 上永远不会生效且零报错。"""
    if row.attr_key not in COMBAT_ATTR_KEYS:
        raise ConfigTableError(
            f"专精效果行 {int(row.id)}: 属性键(attr_key={row.attr_key!r})不是 UE GAS 属性名,"
            "该效果在 DS 上永远不会生效;合法取值见 MyEntityAttrSet.h"
        )
    value = float(row.value_per_level)
    if value == 0:
        raise ConfigTableError(f"专精效果行 {int(row.id)}: 每级数值(value_per_level)为 0,该效果行没有任何作用")
    if value > MAX_TALENT_EFFECT_VALUE_PER_LEVEL or value < -MAX_TALENT_EFFECT_VALUE_PER_LEVEL:
        raise ConfigTableError(
            f"专精效果行 {int(row.id)}: 每级数值(value_per_level={value})超出 "
            f"±{MAX_TALENT_EFFECT_VALUE_PER_LEVEL},疑似多打了零"
        )


def _validate_skill_card_effect_row(row) -> None:  # noqa: ANN001
    """技能卡效果逐行校验。attr_key 不在白名单 = 该效果在 DS 上永远不会生效且零报错。"""
    if row.attr_key not in COMBAT_ATTR_KEYS:
        raise ConfigTableError(
            f"技能卡效果行 {int(row.id)}: 属性键(attr_key={row.attr_key!r})不是战斗属性集里的属性,"
            "该效果在 DS 上永远不会生效;合法取值见 MyEntityAttrSet.h"
        )
    value = float(row.value_per_level)
    if value == 0:
        raise ConfigTableError(
            f"技能卡效果行 {int(row.id)}: 每级数值(value_per_level)为 0,该效果行没有任何作用"
        )
    if value > MAX_SKILL_CARD_EFFECT_VALUE_PER_LEVEL or value < -MAX_SKILL_CARD_EFFECT_VALUE_PER_LEVEL:
        raise ConfigTableError(
            f"技能卡效果行 {int(row.id)}: 每级数值(value_per_level={value})超出 "
            f"±{MAX_SKILL_CARD_EFFECT_VALUE_PER_LEVEL},疑似多打了零"
        )


def _validate_skill_card_row(row) -> None:  # noqa: ANN001
    """技能卡逐行校验。非法稀有度 → 查不到升级曲线,该卡将永远无法升级(且不报错)。"""
    if int(row.rarity) not in SKILL_CARD_RARITIES:
        raise ConfigTableError(
            f"技能卡 {int(row.id)}: 稀有度(rarity={int(row.rarity)})不是合法取值"
            "(1=普通 2=稀有 3=史诗 4=传说);非法稀有度查不到升级曲线,该卡将永远无法升级"
        )
    if int(row.max_level) == 0:
        raise ConfigTableError(f"技能卡 {int(row.id)}: 等级上限(max_level)为 0;初始等级就是 1,上限至少为 1")
    if int(row.max_level) > MAX_SKILL_CARD_LEVEL:
        raise ConfigTableError(
            f"技能卡 {int(row.id)}: 等级上限(max_level={int(row.max_level)})超过 "
            f"{MAX_SKILL_CARD_LEVEL},疑似手滑"
        )


def _validate_skill_card_upgrade_row(row) -> None:  # noqa: ANN001
    """升级消耗逐行校验。shard_cost=0 等于免费升级 —— 要做免费升级请显式改需求。"""
    if int(row.rarity) not in SKILL_CARD_RARITIES:
        raise ConfigTableError(
            f"升级消耗行 {int(row.id)}: 稀有度(rarity={int(row.rarity)})不是合法取值(1~4)"
        )
    if int(row.level) < 2:
        raise ConfigTableError(
            f"升级消耗行 {int(row.id)}: 目标等级(level={int(row.level)})必须 >= 2:"
            "1 级是获得卡时的初始等级,不存在升到 1 级这回事"
        )
    if int(row.level) > MAX_SKILL_CARD_LEVEL:
        raise ConfigTableError(
            f"升级消耗行 {int(row.id)}: 目标等级(level={int(row.level)})超过 {MAX_SKILL_CARD_LEVEL}"
        )
    if int(row.shard_cost) == 0:
        raise ConfigTableError(
            f"升级消耗行 {int(row.id)}: 碎片消耗(shard_cost)为 0,等于免费升级;"
            "要做免费升级请显式改需求而不是填 0"
        )


# ── 整表跨行校验 ─────────────────────────────────────────────────────────────


def _validate_level_curve(levels: dict[int, object]) -> None:
    """玩家等级表整表不变量 —— 对应 Go 的 PlayerLevelExpTable.ValidateCurve。

    校验的是「等级从 1 连续递增 + 累计经验与逐级升级经验自洽 + 末级 upgrade_exp=0」。
    缺了它:表里跳掉 Lv7 时 experience_curve() 会返回一条短了一截的曲线,玩家在 Lv6
    就"满级"了 —— 不报错。
    """
    count = len(levels)
    if count == 0:
        raise ConfigTableError("玩家等级经验表为空")
    if count < MIN_PLAYER_LEVELS:
        raise ConfigTableError(f"玩家等级经验表至少需要 Lv1-Lv2,实为 {count} 级")
    if count > MAX_PLAYER_LEVELS:
        raise ConfigTableError(f"玩家等级经验表等级数 {count} 超过上限 {MAX_PLAYER_LEVELS}")

    expected_cumulative = 0
    for level in range(1, count + 1):
        row = levels.get(level)
        if row is None:
            raise ConfigTableError(f"玩家等级经验表缺少 Lv{level}(等级必须从 1 连续递增)")
        if int(row.level) != level:  # type: ignore[attr-defined]
            raise ConfigTableError(f"玩家等级经验表 ID={level} 的等级为 {int(row.level)}")  # type: ignore[attr-defined]
        if int(row.cumulative_exp) != expected_cumulative:  # type: ignore[attr-defined]
            raise ConfigTableError(
                f"Lv{level} 到达本级累计经验={int(row.cumulative_exp)},"  # type: ignore[attr-defined]
                f"按前级累计应为 {expected_cumulative}"
            )
        if level == count:
            if int(row.upgrade_exp) != 0:  # type: ignore[attr-defined]
                raise ConfigTableError(
                    f"末级 Lv{level} 的升级所需经验必须为 0,实为 {int(row.upgrade_exp)}"  # type: ignore[attr-defined]
                )
            continue
        if int(row.upgrade_exp) == 0:  # type: ignore[attr-defined]
            raise ConfigTableError(f"非末级 Lv{level} 的升级所需经验必须大于 0")
        expected_cumulative += int(row.upgrade_exp)  # type: ignore[attr-defined]
        if expected_cumulative > UINT32_MAX:
            raise ConfigTableError(f"Lv{level} 后累计经验 {expected_cumulative} 超过 uint32 上限")


def _validate_talent_tree(talents: dict[int, object]) -> None:
    """前置存在、前置等级不超前置上限、依赖无环 —— 对应 Go 的 TalentTable.ValidateTree。

    前置关系是**自引用外键**,生成器明确拒绝自引用 FK,所以这项只能落在加载期整表校验。
    成环的表现是玩家怎么点都点不出来(前置永远不达标),不报错。
    """
    for row in talents.values():
        req_id = int(row.require_talent_id)  # type: ignore[attr-defined]
        if req_id == 0:
            continue
        req = talents.get(req_id)
        if req is None:
            raise ConfigTableError(f"专精 {int(row.id)} 的前置专精 {req_id} 不存在")  # type: ignore[attr-defined]
        lv = int(row.require_talent_level)  # type: ignore[attr-defined]
        if lv > int(req.max_level):  # type: ignore[attr-defined]
            raise ConfigTableError(
                f"专精 {int(row.id)} 要求前置 {req_id} 达到 {lv} 级,"  # type: ignore[attr-defined]
                f"但其等级上限只有 {int(req.max_level)}(该专精永远点不出来)"  # type: ignore[attr-defined]
            )

    # 环检测:每个节点至多一条出边(require_talent_id),沿链走一定终止于 0、已判定安全的
    # 节点,或回到本次路径中的节点(成环)。访问过的节点缓存到 safe,整表 O(N)。
    safe: set[int] = set()
    for row in talents.values():
        start = int(row.id)  # type: ignore[attr-defined]
        if start in safe:
            continue
        path: set[int] = set()
        chain: list[int] = []
        cur = start
        while cur != 0 and cur not in safe:
            if cur in path:
                raise ConfigTableError(f"专精前置关系成环: {chain + [cur]}")
            path.add(cur)
            chain.append(cur)
            node = talents.get(cur)
            if node is None:
                break  # 前置缺失已在上面报过,这里只做保守终止
            cur = int(node.require_talent_id)  # type: ignore[attr-defined]
        safe.update(chain)


def _validate_talent_effects(rows: list) -> None:
    """同一专精不得对同一属性配多行 —— 重复行不报错,只会让加成翻倍。

    这是"数值莫名其妙偏高"这类问题里最难查的来源,必须在加载边界整批拒绝。
    """
    seen: dict[tuple[int, str], int] = {}
    for row in rows:
        key = (int(row.talent_id), row.attr_key)
        first = seen.get(key)
        if first is not None:
            raise ConfigTableError(
                f"专精 {key[0]} 对属性 {key[1]} 配了多行效果(行 {first} 与行 {int(row.id)}),加成会翻倍"
            )
        seen[key] = int(row.id)


def _validate_skill_card_effects(rows: list) -> None:
    """同一张技能卡不得对同一属性配多行 —— 重复行不报错,只会让加成翻倍。

    与 _validate_talent_effects 同因:是"数值莫名其妙偏高"这类问题里最难查的来源,
    必须在加载边界整批拒绝。
    """
    seen: dict[tuple[int, str], int] = {}
    for row in rows:
        key = (int(row.card_id), row.attr_key)
        first = seen.get(key)
        if first is not None:
            raise ConfigTableError(
                f"技能卡 {key[0]} 对属性 {key[1]} 配了多行效果(行 {first} 与行 {int(row.id)}),加成会翻倍"
            )
        seen[key] = int(row.id)


def _validate_card_curves(
    cards: dict[int, object], upgrade_rows: list
) -> dict[tuple[int, int], int]:
    """升级曲线三条不变量 —— 对应 Go 的 SkillCardUpgradeTable.ValidateCurves。

      ① (稀有度, 目标等级) 唯一 —— 重复行会让消耗取决于表内顺序;
      ② 每个在用稀有度的曲线从 2 级起连续铺到该稀有度的最高等级上限 —— 断档表现为
         "卡升到某级之后按钮没反应",不报错,是最难查的一类配置事故;
      ③ 消耗随等级单调不减 —— 越升越便宜几乎总是填错。

    返回 (rarity, level) → shard_cost 的索引供运行期查价。
    """
    cost_by_key: dict[tuple[int, int], int] = {}
    for row in upgrade_rows:
        key = (int(row.rarity), int(row.level))
        if key in cost_by_key:
            raise ConfigTableError(
                f"稀有度 {key[0]} 的 {key[1]} 级升级消耗配了多行,取值将取决于表内顺序"
            )
        cost_by_key[key] = int(row.shard_cost)

    # 只有卡在用的稀有度才要求有完整曲线 —— 反过来要求"曲线表里每个稀有度都得有卡"
    # 是错的(策划可以先铺曲线再加卡)。
    max_level_by_rarity: dict[int, int] = {}
    for card in cards.values():
        rarity = int(card.rarity)  # type: ignore[attr-defined]
        lvl = int(card.max_level)  # type: ignore[attr-defined]
        if lvl > max_level_by_rarity.get(rarity, 0):
            max_level_by_rarity[rarity] = lvl

    for rarity, max_level in max_level_by_rarity.items():
        prev = 0
        for level in range(2, max_level + 1):
            cost = cost_by_key.get((rarity, level))
            if cost is None:
                raise ConfigTableError(
                    f"稀有度 {rarity} 的升级曲线缺 {level} 级(该稀有度有卡的等级上限是 {max_level});"
                    f"缺档会让卡升到 {level - 1} 级后无法继续升且不报错"
                )
            if cost < prev:
                raise ConfigTableError(
                    f"稀有度 {rarity} 的升级消耗在 {level} 级下降({prev} → {cost}),疑似填错"
                )
            prev = cost
    return cost_by_key


# ── 加载 ─────────────────────────────────────────────────────────────────────


def _load_one(active: pathlib.Path, manifest, name: str, *, required: bool):  # noqa: ANN001, ANN202
    mt = manifest.tables.get(name)
    if mt is None:
        if not required:
            return None
        raise ConfigTableError(f"manifest 缺少本进程必需的表 {name!r},整批拒绝")
    expect_proto, container_cls = _TABLE_PROTOS[name]
    if mt.proto != expect_proto:
        raise ConfigTableError(
            f"{name} 表 proto 不符: 期望 {expect_proto} 实际 {mt.proto}(接错文件?)"
        )
    path = active / mt.file
    if not path.is_file():
        raise ConfigTableError(f"manifest 列出的表文件不存在: {path}")
    raw = path.read_bytes()
    verify_checksum(raw, mt.checksum)

    container = container_cls()
    try:
        # ★ ignore_unknown_fields=True —— 与 Go 侧 protojson + DiscardUnknown 一致,
        # **不是**放松校验。严格校验属于生成阶段;运行期必须容忍新增列,否则标准发布序
        # (先发配置、再滚二进制)会让尚未滚上的旧进程整批拒载,共存窗口被打穿。
        json_format.Parse(raw.decode("utf-8"), container, ignore_unknown_fields=True)
    except json_format.ParseError as exc:
        raise ConfigTableError(f"{path} protojson 解析失败: {exc}") from exc

    rows = list(container.rows)
    # ★ 判据是 `len(rows) != mt.rows`,**不是** `if mt.rows and ...`。后者会让 manifest
    # 声明 rows=0 的表整条跳过行数校验 —— 而这道校验防的正是"发布拷贝被截断":
    # 截成 0 行的表配上声明 0 行的 manifest,两边"自洽",服务照常启动,曲线整个是空的。
    if len(rows) != mt.rows:
        raise ConfigTableError(f"{name} 表行数不符: manifest 声明 {mt.rows} 实际 {len(rows)}")
    return rows


def load_tables(
    active_dir: str | pathlib.Path,
    expect_version: int = 0,
    *,
    current_max_level: int = 0,
) -> LoadResult:
    """加载 player 需要的六张表。整批 fail-closed:任一条不过都抛,不返回半个批次。

    ★ current_max_level 是**热更专用**的防降级基线(启动首载传 0)。对应 Go 侧
      main.go 校验器里的那一条:「配置热更不得缩短最高等级,否则已有高等级玩家会在
      后续入账时被错误降级」。少了它,一次误发的短表会把满级玩家在下一次 AddExperience
      时按新上限重新结算,等级凭空掉下去且不可逆。
    """
    active = pathlib.Path(active_dir)
    if not active.is_dir():
        raise ConfigTableError(f"配置表目录不存在: {active}")

    manifest = read_manifest(active)
    if expect_version and manifest.version != expect_version:
        raise ConfigTableError(f"manifest 版本不符: 期望 {expect_version} 实际 {manifest.version}")

    level_rows = _load_one(active, manifest, "player_level_exp", required=True)
    item_rows = _load_one(active, manifest, "item", required=True)
    talent_rows = _load_one(active, manifest, "talent", required=True)
    # talent_effect 缺表不拒(合法过渡态,见 REQUIRED_TABLES 注释)。
    effect_rows = _load_one(active, manifest, "talent_effect", required=False)
    card_rows = _load_one(active, manifest, "skill_card", required=True)
    upgrade_rows = _load_one(active, manifest, "skill_card_upgrade", required=True)
    # skill_card_effect 缺表不拒(合法过渡态,与 talent_effect 同一处置)。
    card_effect_rows = _load_one(active, manifest, "skill_card_effect", required=False)

    levels: dict[int, _lvl_pb2.PlayerLevelExpRow] = {}
    for row in level_rows:
        _validate_player_level_exp_row(row)
        if int(row.id) in levels:
            raise ConfigTableError(f"player_level_exp 表 id 重复: {int(row.id)}")
        levels[int(row.id)] = row

    items: dict[int, _item_pb2.ItemRow] = {}
    for row in item_rows:
        if int(row.id) in items:
            raise ConfigTableError(f"item 表 id 重复: {int(row.id)}")
        items[int(row.id)] = row

    talents: dict[int, _talent_pb2.TalentRow] = {}
    for row in talent_rows:
        _validate_talent_row(row)
        if int(row.id) in talents:
            raise ConfigTableError(f"talent 表 id 重复: {int(row.id)}")
        talents[int(row.id)] = row

    effects: list[_teff_pb2.TalentEffectRow] = []
    for row in effect_rows or []:
        _validate_talent_effect_row(row)
        effects.append(row)

    cards: dict[int, _card_pb2.SkillCardRow] = {}
    for row in card_rows:
        _validate_skill_card_row(row)
        if int(row.id) in cards:
            raise ConfigTableError(f"skill_card 表 id 重复: {int(row.id)}")
        cards[int(row.id)] = row

    for row in upgrade_rows:
        _validate_skill_card_upgrade_row(row)

    card_effects: list[_ceff_pb2.SkillCardEffectRow] = []
    for row in card_effect_rows or []:
        _validate_skill_card_effect_row(row)
        card_effects.append(row)

    # ── 整批门禁(启动与热更共用同一入口)─────────────────────────────
    _validate_level_curve(levels)
    if current_max_level and len(levels) < current_max_level:
        raise ConfigTableError(
            f"玩家最高等级不允许从 {current_max_level} 降到 {len(levels)}"
        )
    _validate_talent_tree(talents)
    _validate_talent_effects(effects)
    card_upgrade = _validate_card_curves(cards, upgrade_rows)
    _validate_skill_card_effects(card_effects)

    tables = Tables(
        version=manifest.version,
        source_rev=manifest.source_rev,
        levels=levels,
        items=items,
        talents=talents,
        talent_effects=effects,
        skill_cards=cards,
        card_upgrade=card_upgrade,
        skill_card_effects=card_effects,
    )

    listed = {MANIFEST_FILE_NAME} | {t.file for t in manifest.tables.values()}
    warnings = [
        f"active 目录存在 manifest 未列出的文件 {p.name!r}(脏数据)"
        for p in sorted(active.glob("*.json"))
        if p.name not in listed
    ]
    return LoadResult(
        version=manifest.version,
        source_rev=manifest.source_rev,
        warnings=warnings,
        tables=tables,
    )


class Store:
    """当前生效批次的持有者。热更时整份替换 `_tables`(单次赋值,读侧无锁)。

    与 Go 侧 configtable.Store 同一语义:每次查询都读**当前**批次,所以热更后的下一次
    AddExperience / SetTalents / UpgradeSkillCard 立即使用新规则;已落库的分配不被追溯改写。
    """

    __slots__ = ("_tables", "_dir")

    def __init__(self, tables: Tables, active_dir: str) -> None:
        self._tables = tables
        self._dir = active_dir

    @property
    def tables(self) -> Tables:
        return self._tables

    @property
    def active_dir(self) -> str:
        return self._dir

    def replace(self, tables: Tables) -> None:
        """原子换批(单次属性赋值 —— 读侧要么看到旧的一整批,要么看到新的一整批)。"""
        self._tables = tables
