"""数据层的内部结构 —— 对应 Go 侧 internal/data 的同名类型。

这些结构是 data → biz → service 的搬运形状,**不是**对外协议:
proto ↔ 内部结构的互转集中在 service.py,data 层不碰 proto(除了实例词条那一列,
它在 DB 里就是 pb 二进制)。
"""

from __future__ import annotations

import dataclasses
import enum

# ── escrow 行的类型与状态(对齐 auction_escrow 表的列语义)──


class EscrowKind(enum.IntEnum):
    """拍卖挂单托管的资产类型。数值对齐 auction_escrow.kind,不可改。"""

    ITEM = 1  # 卖单冻结道具
    GOLD = 2  # 买单冻结金币


# escrow 行状态(对齐 auction_escrow.status)。
ESCROW_STATUS_ACTIVE = 1
ESCROW_STATUS_CLOSED = 2


@dataclasses.dataclass(frozen=True, slots=True)
class ItemStack:
    """背包里某配置道具的持有堆叠。"""

    item_config_id: int
    count: int


@dataclasses.dataclass(frozen=True, slots=True)
class ItemGrant:
    """一次发放里对某配置道具增加的数量(count > 0)。"""

    item_config_id: int
    count: int


@dataclasses.dataclass(frozen=True, slots=True)
class ItemAttribute:
    """装备实例鉴定后的一条随机属性。"""

    attr_id: int
    value: int


@dataclasses.dataclass(slots=True)
class ItemInstance:
    """一件装备类道具的唯一实例(不可堆叠)。

    ★ slot_index = -1 表示**未分配格**,DB 里存 NULL。
      MySQL 的唯一键允许多个 NULL,所以多件未分配格的实例不会互相冲突 ——
      释放托管(ReleaseTransferEscrow)正是靠这条不设容量闸地把资产还回去。
      用 0 表示"未分配"会与真实的第 0 格撞车。
    """

    instance_id: int
    item_config_id: int
    identified: bool = False
    attributes: list[ItemAttribute] = dataclasses.field(default_factory=list)
    slot_index: int = -1
    bound: bool = False


@dataclasses.dataclass(frozen=True, slots=True)
class InstanceOwnershipQuery:
    """精确实例归属校验项 —— 两个字段必须**同时**匹配。

    只按 instance_id 查会让"我有这个 id 的实例"变成通过条件,
    而调用方(player.SetEquipment)真正要问的是"这个 id 的实例确实是这个配置"。
    """

    instance_id: int
    item_config_id: int


@dataclasses.dataclass(frozen=True, slots=True)
class TransferClaimItem:
    """领取核对项:instance_id 定位托管行,item_config_id 交叉核对
    (与托管行不符 = 附件漂移 / 伪造,整批拒)。"""

    instance_id: int
    item_config_id: int


@dataclasses.dataclass(slots=True)
class EscrowedInstance:
    """托管行快照(EscrowOutInstances 返回,调用方装 TransferAttachment.item)。"""

    instance_id: int
    item_config_id: int
    identified: bool
    attributes: list[ItemAttribute]
    source_player_id: int
    to_player_id: int
