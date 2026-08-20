"""背包域 journal op 应用逻辑 —— 对应 Go 侧 internal/data/bag_apply.go
(bag-domain.md §3/§5.1)。

纯段内存变换(不触 SQL,可独立单测):对**后端驻留段**(仓库 1 / 活动段 100+)执行
加 / 扣 / 转移;**随身组**(0 身上 / 2 装备栏 / 3 临时格,DS 驻留)侧只记 journal
不改 bag_section —— 内存态由 owner DS 按 ACK 应用,存储侧本体经 checkpoint 覆盖。
调用方保证:同段读改写走事务内工作副本,提交前统一落库。

★ 为什么"规划 → 容量门 → 应用"要分成三步(sectionAddItems)

  合并写法(边填边扣容量)在"填了两个堆之后发现第三格放不下"时,段已经被改脏了。
  真实调用链上 MySQL 事务会回滚,看不出问题;但纯内存单测的假仓不会回滚,
  段里会留下半个操作的痕迹。更要命的是这个差异**只在失败路径上出现**,
  平时跑一万次都是对的。所以镜像 UE FMyBag 的做法:先规划,再过容量门,最后写。

★ 计数一律走 Python 任意精度整数,但**写回 proto 前必须显式钳在 uint32 内**

  Go 那边靠 uint64 中间量 + 类型边界兜底(`limit ≤ MaxUint32` 由 uint32 配置保证)。
  Python 的配置是普通 int,写超范围的 count 会在赋值时抛 ValueError,
  逃出去就是 500 而不是业务码 —— 所以堆叠上限在入口处显式判上界。
"""

from __future__ import annotations

from collections.abc import Callable

from pandora.bag.v1 import bag_pb2

from pandorapy import errcode

# ── 段类型(对应 Go 的 BagWarehouseType / BagActivityTypeBase)────────────────

BAG_WAREHOUSE_TYPE = 1
BAG_ACTIVITY_TYPE_BASE = 100

# ── op_type 列取值(对齐 bag.proto 的 oneof 分支;对应 Go 的 BagOp* 常量)──────

BAG_OP_PICKUP_GRANT = 1
BAG_OP_MAIL_CLAIM = 2
BAG_OP_TRANSFER = 3
BAG_OP_CONSUME = 4

# proto 里 count / capacity / slot 都是 uint32,instance_id / generation 是 uint64。
# Python 没有类型边界,这两个常量就是**唯一**的边界闸。
UINT32_MAX = (1 << 32) - 1
UINT64_MAX = (1 << 64) - 1


def is_backend_resident_bag_type(bag_type: int) -> bool:
    """后端驻留段(仓库 / 活动段)。随身组 0/2/3 不落 bag_section。"""
    return bag_type == BAG_WAREHOUSE_TYPE or bag_type >= BAG_ACTIVITY_TYPE_BASE


def is_activity_bag_type(bag_type: int) -> bool:
    """活动段(代际语义仅活动段生效)。"""
    return bag_type >= BAG_ACTIVITY_TYPE_BASE


def carry_group_bag_type(bag_type: int) -> bool:
    """随身组段(0 身上 / 2 装备栏 / 3 临时格;DS 驻留,checkpoint 覆盖)。"""
    return bag_type in (0, 2, 3)


# 事务内段工作副本加载回调(append_journal 注入;含代际归一)。
SectionLoader = Callable[[int], "bag_pb2.BagSection"]
# 段容量 / 堆叠上限查询回调(biz 注入配置;0 = 未配置,fail-closed 拒写)。
CapacityFn = Callable[[int], int]
MaxStackFn = Callable[[int], int]


def apply_bag_op(
    entry: bag_pb2.BagJournalEntry,
    load: SectionLoader,
    dirty: dict[int, bool],
    capacity: CapacityFn,
    max_stack: MaxStackFn,
) -> int:
    """应用一条 journal op:改动涉及的后端驻留段并标脏,返回 op_type。

    任何校验失败整批拒(调用方回滚)。对应 Go 的 applyBagOpTx。
    """
    which = entry.WhichOneof("op")
    if which == "pickup_grant":
        _grant_into_bag_type(
            entry.bag_type, entry.generation, entry.pickup_grant.items,
            load, dirty, capacity, max_stack,
        )
        return BAG_OP_PICKUP_GRANT

    if which == "mail_claim":
        if not entry.mail_claim.claim_key:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "mail_claim requires claim_key")
        _grant_into_bag_type(
            entry.bag_type, entry.generation, entry.mail_claim.items,
            load, dirty, capacity, max_stack,
        )
        return BAG_OP_MAIL_CLAIM

    if which == "transfer":
        # 源段 = entry.bag_type(代际已在外层校验);目标段代际在此校验。
        _deduct_from_bag_type(entry.bag_type, entry.transfer.items, load, dirty)
        _grant_into_bag_type(
            entry.transfer.to_bag_type, entry.transfer.to_generation, entry.transfer.items,
            load, dirty, capacity, max_stack,
        )
        return BAG_OP_TRANSFER

    if which == "consume":
        # 后端驻留段:使用语义(§5.1),真实扣段 + 可选产出。
        # 随身组段(0/2/3):丢弃 / 扣减流水 —— 存储侧只记 journal 供崩溃恢复尾重放,
        # 段内容由 owner DS 内存应用、经 checkpoint 覆盖(_deduct_from_bag_type 对
        # 随身组本就 no-op)。随身组 consume **不许携带产出**(fail-closed,防经此
        # 开出未经审计的跨段发放通道)。
        op = entry.consume
        if not is_backend_resident_bag_type(entry.bag_type) and len(op.produce_items) > 0:
            raise errcode.PandoraError(
                errcode.ErrBagSectionNotAllowed,
                "carry-section consume must not produce bag=%d",
                entry.bag_type,
            )
        _deduct_from_bag_type(entry.bag_type, op.consume_items, load, dirty)
        if len(op.produce_items) > 0:
            _grant_into_bag_type(
                op.produce_bag_type, op.produce_generation, op.produce_items,
                load, dirty, capacity, max_stack,
            )
        return BAG_OP_CONSUME

    # 未知 op fail-closed 整批拒(旧副本遇到新 op 不得静默 ACK 丢失,§9.21 混版纪律)。
    raise errcode.PandoraError(
        errcode.ErrInvalidArg, "unknown journal op seq=%d", entry.journal_seq
    )


def _grant_into_bag_type(
    bag_type: int,
    generation: int,
    items,  # noqa: ANN001 —— RepeatedCompositeFieldContainer[BagItem]
    load: SectionLoader,
    dirty: dict[int, bool],
    capacity: CapacityFn,
    max_stack: MaxStackFn,
) -> None:
    """把物品加进目标段:后端驻留段真实入段;随身组目标只记 journal,此处 no-op。"""
    validate_bag_items(items)
    if not is_backend_resident_bag_type(bag_type):
        return
    sec = load(bag_type)
    # 活动段目标代际必须等于 current(load 已归一 sec.generation=current;fail-closed)。
    if is_activity_bag_type(bag_type) and generation != sec.generation:
        raise errcode.PandoraError(
            errcode.ErrBagGenerationMismatch,
            "target generation mismatch bag=%d want=%d current=%d",
            bag_type, generation, sec.generation,
        )
    section_cap = capacity(bag_type)
    if section_cap == 0:
        # 容量未配置 fail-closed:不允许向未登记的后端驻留段写入(配置错误不静默造格)。
        raise errcode.PandoraError(
            errcode.ErrBagSectionNotAllowed, "capacity not configured bag=%d", bag_type
        )
    section_add_items(sec, items, section_cap, max_stack)
    dirty[bag_type] = True


def _deduct_from_bag_type(
    bag_type: int,
    items,  # noqa: ANN001
    load: SectionLoader,
    dirty: dict[int, bool],
) -> None:
    """从源段扣物品:后端驻留段真实扣段;随身组源只记 journal,此处 no-op。"""
    validate_bag_items(items)
    if not is_backend_resident_bag_type(bag_type):
        return
    sec = load(bag_type)
    section_remove_items(sec, items)
    dirty[bag_type] = True


def validate_bag_items(items) -> None:  # noqa: ANN001
    """校验物品列表形状(config>0,count>0,实例 count 恒 1)。"""
    if len(items) == 0:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "items required")
    for it in items:
        if it.item_config_id == 0 or it.count == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "invalid item config=%d count=%d",
                it.item_config_id, it.count,
            )
        if it.instance_id != 0 and it.count != 1:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "instance item must have count=1 instance=%d",
                it.instance_id,
            )


def section_add_items(
    sec: bag_pb2.BagSection,
    items,  # noqa: ANN001
    capacity: int,
    max_stack_of: MaxStackFn,
) -> None:
    """向段内加物品(服务端权威建模堆叠上限,bag-domain.md §5.2)。

    可堆叠道具按 max_stack 拆堆 —— 先填既有未满同 config 堆,溢出按 max_stack 整格新开;
    实例每件独占一格。容量按条目数(格子数)校验,放不下该 item 时**在写入前整体拒**。
    """
    for it in items:
        if it.instance_id != 0:
            for exist in sec.items:
                if exist.instance_id == it.instance_id:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "duplicate instance %d in bag=%d",
                        it.instance_id, sec.bag_type,
                    )
            if len(sec.items) + 1 > capacity:
                raise errcode.PandoraError(
                    errcode.ErrBagCapacityFull,
                    "bag=%d capacity=%d full",
                    sec.bag_type, capacity,
                )
            added = sec.items.add()
            added.item_config_id = it.item_config_id
            added.count = 1
            added.slot = lowest_free_bag_slot(sec, skip_last=True)
            added.instance_id = it.instance_id
            added.identified = it.identified
            for attr in it.attrs:
                added.attrs.add(attr_id=attr.attr_id, value=attr.value)
            continue

        # 可堆叠:堆叠上限未配置 fail-closed(配置错误不静默无限合并)。
        limit = max_stack_of(it.item_config_id)
        if limit == 0:
            raise errcode.PandoraError(
                errcode.ErrBagSectionNotAllowed,
                "max_stack not configured config=%d bag=%d",
                it.item_config_id, sec.bag_type,
            )
        if limit < 0 or limit > UINT32_MAX:
            # Go 侧 max_stack 是 uint32,配置越界不可能发生;Python 的 int 无边界,
            # 越界会在写回 count 时抛 ValueError 逃出业务码通道 → 这里显式拒。
            raise errcode.PandoraError(
                errcode.ErrBagSectionNotAllowed,
                "max_stack out of uint32 range config=%d max_stack=%d",
                it.item_config_id, limit,
            )

        # ① 规划(不改段):既有未满同 config 堆可吸纳多少;超上限的脏数据堆跳过
        #    (不参与吸纳也不做减法,防下溢;资产保留,只出不进)。
        fills: list[tuple[int, int]] = []
        remaining = it.count
        for idx, exist in enumerate(sec.items):
            if remaining == 0:
                break
            if exist.instance_id != 0 or exist.item_config_id != it.item_config_id:
                continue
            cnt = exist.count
            if cnt >= limit:
                continue
            fill = min(limit - cnt, remaining)
            fills.append((idx, fill))
            remaining -= fill

        # ② 容量门:溢出部分按 max_stack 整格折算新格数,放不下在任何写入前整体拒。
        if remaining > 0:
            new_grids = (remaining + limit - 1) // limit
            if len(sec.items) + new_grids > capacity:
                raise errcode.PandoraError(
                    errcode.ErrBagCapacityFull,
                    "bag=%d capacity=%d full (need %d new slots)",
                    sec.bag_type, capacity, new_grids,
                )

        # ③ 应用:填堆(cnt+fill ≤ limit ≤ uint32 上限,写回安全)+ 整格铺开。
        for idx, fill in fills:
            sec.items[idx].count = sec.items[idx].count + fill
        while remaining > 0:
            put = min(limit, remaining)
            added = sec.items.add()
            added.item_config_id = it.item_config_id
            added.count = put
            added.slot = lowest_free_bag_slot(sec, skip_last=True)
            remaining -= put


def section_remove_items(sec: bag_pb2.BagSection, items) -> None:  # noqa: ANN001
    """从段内扣物品:实例按 instance_id 精确移除;可堆叠按 config 扣数量,扣空移格。

    不存在 / 数量不足 → ErrBagItemNotFound(整批拒,零部分应用)。
    """
    for it in items:
        if it.instance_id != 0:
            removed = False
            for idx, exist in enumerate(sec.items):
                if exist.instance_id == it.instance_id and exist.item_config_id == it.item_config_id:
                    del sec.items[idx]
                    removed = True
                    break
            if not removed:
                raise errcode.PandoraError(
                    errcode.ErrBagItemNotFound,
                    "instance %d not found in bag=%d",
                    it.instance_id, sec.bag_type,
                )
            continue

        remaining = it.count
        idx = 0
        while idx < len(sec.items) and remaining > 0:
            exist = sec.items[idx]
            if exist.instance_id != 0 or exist.item_config_id != it.item_config_id:
                idx += 1
                continue
            if exist.count > remaining:
                exist.count = exist.count - remaining
                remaining = 0
                break
            remaining -= exist.count
            del sec.items[idx]
        if remaining > 0:
            raise errcode.PandoraError(
                errcode.ErrBagItemNotFound,
                "insufficient item config=%d need=%d in bag=%d",
                it.item_config_id, it.count, sec.bag_type,
            )


def lowest_free_bag_slot(sec: bag_pb2.BagSection, *, skip_last: bool = False) -> int:
    """段内最小空闲格位(后端驻留段格位仅展示用,随身组格位归 DS checkpoint)。

    ★ skip_last 是 Python 与 Go 的一处**必须显式处理**的形状差异:
      Go 先构造好 BagItem 再 append,算 slot 时新格还不在切片里;
      protobuf-python 只能 `items.add()` 先拿到空消息再填字段,新格已在列表里
      且 slot 默认 0 —— 不排除它的话,第一次加格会把 0 号槽算成"已占用",
      整段格位从 1 开始,与 Go 落库的数据错位一格(客户端渲染直接对不上)。
    """
    used: set[int] = set()
    rows = sec.items[:-1] if skip_last else sec.items
    for it in rows:
        used.add(it.slot)
    slot = 0
    while slot in used:
        slot += 1
    return slot
