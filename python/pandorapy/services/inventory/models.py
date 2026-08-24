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
    # CURRENCY(原 GOLD):买单冻结货币,**具体币种由 auction_escrow.currency_kind 记录**。
    # 改名不改值:kind 列存的仍是 2,存量行不需要迁移;改的是"这一格只能是金币"这个假设。
    CURRENCY = 2


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


# ── 多币种改造引入的结果形状(2026-08-22)──


@dataclasses.dataclass(slots=True)
class LedgerSnapshot:
    """一条幂等流水记下的首次执行结果,重放时原样返回(§9.7)。对应 Go 的 LedgerSnapshot。

        remaining  操作后该道具剩余数量(use / sell / discard 用)
        balances   操作后玩家**全部**币种余额
        delta      本次操作的货币变动额**绝对值**(方向由 op 决定:
                   sell / grant 是收入,purchase 是支出)

    ★ delta 必须单独记而不是从 balances 反推:重放时中间可能已发生别的收支,
      反推会算错("这次赚了多少"变成"这次之后总共有多少减去现在有多少")。
    """

    remaining: int = 0
    balances: dict = dataclasses.field(default_factory=dict)
    delta: dict = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(slots=True)
class SaleOutcome:
    """一次出售的结果(新执行或幂等回放共用同一形状)。对应 Go 的 SaleOutcome。"""

    remaining: int = 0  # 出售后该道具剩余数量(装备实例出售恒 0)
    balances: dict = dataclasses.field(default_factory=dict)  # 出售后全部币种余额
    earned: int = 0  # 本次实际入账额(回放时来自 ledger 的 delta 列)
    kind: int = 0


@dataclasses.dataclass(slots=True)
class PurchaseRequest:
    """biz 层算好的一次商店购买(价格已按商店表定价并过溢出闸)。对应 Go 的 PurchaseRequest。

    ★ 这里**没有** unit_price:单价只在 biz 用于算 total_cost,data 层只认总价,
      免得两层各存一份价格再漂移。
    """

    shop_id: int = 0
    item_config_id: int = 0
    unit_count: int = 0  # 购买份数
    total_items: int = 0  # 可堆叠道具的总发放数量(= 份数 × 每份数量);装备类为 0
    kind: int = 0
    total_cost: int = 0
    is_equipment: bool = False
    # instance_ids 是装备类购买预生成的雪花 ID(由 biz 批量预留),
    # 长度必须等于要发的件数;可堆叠购买为空。
    instance_ids: list[int] = dataclasses.field(default_factory=list)
    capacity: int = 0
    idempotency_key: str = ""


@dataclasses.dataclass(slots=True)
class PurchaseOutcome:
    """一次购买的结果(新执行或幂等回放共用同一形状)。对应 Go 的 PurchaseOutcome。"""

    balances: dict = dataclasses.field(default_factory=dict)  # 扣费后玩家全部币种余额
    cost: int = 0  # 本次实际扣费(回放时来自 ledger delta 列)
    kind: int = 0
    items: list[ItemGrant] = dataclasses.field(default_factory=list)
    instances: list[ItemInstance] = dataclasses.field(default_factory=list)
