"""player 数据层的领域结构 —— 对应 Go 侧 internal/data/player_repo.go 里的那组 struct。

它们是 biz ↔ repo 的入参/出参形状,刻意与 proto 分开:proto 是**客户端可见结构**,
库里的 spent_points / baseline / floor 这类字段不该外露(§5.11),而 proto 的
`identified` / `attributes` 又只在 GetLoadout 那一刻由 inventory 权威填充、绝不落库。
"""

from __future__ import annotations

import dataclasses


@dataclasses.dataclass(frozen=True, slots=True)
class MMRChange:
    """一次 MMR 变更请求(biz 算好语义后传给 repo 落库)。"""

    player_id: int
    idempotency_key: str  # 一般是 match_id 字符串
    # rating_pool 必须是**已归一化**的值(rating.normalize)。空串会写出一行
    # rating_pool='' 的孤儿分区,与 default 池的分永远对不上,且没有任何报错。
    rating_pool: str
    # baseline 是该池**首战**时的起算分。只在该玩家该池尚无行时生效;已有行时以库里
    # 的值为准,绝不用本字段覆盖(否则每次结算都会把老玩家的分拽回基线)。
    baseline: int
    delta: int
    reason: str
    floor: int
    inc_battle: bool
    inc_win: bool


@dataclasses.dataclass(frozen=True, slots=True)
class PlayerRating:
    """某玩家在某段位池下的一份分(list_ratings 的行视图)。"""

    rating_pool: str
    mmr: int


@dataclasses.dataclass(frozen=True, slots=True)
class AttrAllocation:
    """一次加点请求里对某属性增加的点数(只增,points>0)。"""

    key: str
    points: int


@dataclasses.dataclass(frozen=True, slots=True)
class AttrPoint:
    """某条属性的已分配点数。"""

    key: str
    points: int


@dataclasses.dataclass(frozen=True, slots=True)
class EquipmentSlot:
    """出战装备预设的一个槽位。

    instance_id=0 **仅**表示 000006 上线前的存量只读行;所有新写必须携带精确实例 ID。
    """

    slot: int
    item_config_id: int
    instance_id: int


@dataclasses.dataclass(frozen=True, slots=True)
class EquipmentAttributeSnapshot:
    """inventory 域按 exact pair 返回的一条权威鉴定词条。

    player **不保存、不重新 roll**,只在 GetLoadout 组装战斗快照时保真转发。
    """

    attr_id: int
    value: int


@dataclasses.dataclass(frozen=True, slots=True)
class OwnedEquipmentInstance:
    """inventory.CheckInstancesOwned 的权威实例快照。"""

    instance_id: int
    item_config_id: int
    identified: bool
    attributes: tuple[EquipmentAttributeSnapshot, ...] = ()


@dataclasses.dataclass(frozen=True, slots=True)
class InstanceOwnershipResult:
    """同时保留滚动升级兼容的 ID 子集与新版详情。

    旧 inventory 副本只会填 owned_instance_ids:SetEquipment 仍可 exact pair 核权,
    而 GetLoadout 必须等 owned_instances 齐全才生成战斗快照(词条缺失时 fail-closed)。
    """

    owned_instance_ids: tuple[int, ...] = ()
    owned_instances: tuple[OwnedEquipmentInstance, ...] = ()


@dataclasses.dataclass(slots=True)
class TalentLevel:
    """天赋树某节点的已点等级与该节点实际消耗的天赋点。

    spent_points = 等级 × 专精表 cost_per_level,写入时由 biz 按表算好填入、随分配一起
    落库;读取时由 repo 回填。**之所以要存而不是读时再算**:repo 层看不到配置表,只能
    按 Σ 等级 反推,而 cost_per_level≠1 时反推会把已花点数算少,玩家看到的可点数比实际
    多(写扣 6 读算 4)。存下来后写与读共用专精表这一个口径,且策划改表不会追溯改写
    老玩家已花掉的点。
    """

    talent_id: int
    level: int
    spent_points: int = 0


@dataclasses.dataclass(frozen=True, slots=True)
class SkillCard:
    """玩家持有的一张技能卡(等级 + 碎片余量)。"""

    card_id: int
    level: int
    shards: int


@dataclasses.dataclass(frozen=True, slots=True)
class SkillCardGrant:
    """一次发放中的一项(碎片可为 0 = 只解锁不给碎片)。"""

    card_id: int
    shards: int


@dataclasses.dataclass(frozen=True, slots=True)
class SkillSlot:
    """一个卡槽的装配。空槽不落行,故 card_id 恒 > 0。"""

    slot: int
    card_id: int


@dataclasses.dataclass(frozen=True, slots=True)
class ExpApply:
    """一次经验入账请求(biz 校验合法性后传入)。

    curve 第 i 项(0 基)= 从 Lv(i+1) 升到 Lv(i+2) 所需级内经验(>0);
    最高等级 = len(curve)+1(与策划 j_玩家等级经验.xlsx / 客户端 CfgPlayerLevelExp 同源)。
    """

    player_id: int
    delta: int
    reason: str
    idempotency_key: str
    curve: tuple[int, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class ExpState:
    """入账后(或幂等命中时当前)的权威经验快照。"""

    level: int = 0
    exp_in_level: int = 0
    is_max_level: bool = False
    levels_gained: int = 0


@dataclasses.dataclass(frozen=True, slots=True)
class PushOutboxRecord:
    """一条待发布的玩家推送事务出箱记录(FIFO 按 id)。"""

    id: int
    player_id: int
    event_type: int
    payload: bytes


@dataclasses.dataclass(frozen=True, slots=True)
class PlayerName:
    """「角色实体 → 显示名」的最小映射(biz 层结构,service 再翻成 proto)。"""

    player_id: int
    nickname: str


@dataclasses.dataclass(frozen=True, slots=True)
class EnsureProfileResult:
    """ensure_profile_named 的产出。"""

    # created=true 表示本次调用真的建了档(请求的 nickname 生效)。
    created: bool
    # nickname 是调用结束后该玩家**实际**的昵称(权威值)。
    nickname: str
    # level 是调用结束后的等级(刚建档为 1)。
    level: int
