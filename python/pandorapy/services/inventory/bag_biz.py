"""背包域用例 —— 对应 Go 侧 internal/biz/bag.go(bag-domain.md)。

职责:请求形状校验(批量上限 / 段类型合法性 / 幂等键形状)→ 委托 BagRepo
(epoch fencing / generation 校验 / 幂等去重 / 同事务段变更全部在数据层事务内完成)。

五要件分工(CLAUDE.md §9.6):

    ①身份 = 传输层(内网直连,service 层拒带玩家 JWT 的调用);
    ②owner 授权 = 本层 authorize_owner 查 owner authority;
    ③fencing + ⑤审计 = 数据层;④额度 = 本层配置 + 数据层滑窗封顶。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from pandora.bag.v1 import bag_pb2

from pandorapy import errcode
from pandorapy.services.inventory import bag_apply as bapply
from pandorapy.services.inventory import currency as ccy
from pandorapy.services.inventory import conf as bconf

# checkpoint 是被攻破 DS 可写的 MEDIUMBLOB;业务上随身组最多三个段,空间恢复的绝对
# 格位上限与 UE RecoveryAbsoluteMaxGridCount 保持一致。整体字节闸再兜住 protobuf
# unknown fields 与单元素深度,避免合法行数下的单行无界增长。
# BAG_CHECKPOINT_MAX_BYTES 是 service 在序列化前就必须使用的公共字节闸。
BAG_CHECKPOINT_MAX_BYTES = 256 * 1024
_BAG_CHECKPOINT_MAX_SECTIONS = 3
_BAG_CHECKPOINT_MAX_GRID_COUNT = 4096
_BAG_MAX_ATTRS_PER_ITEM = 64
_BAG_MAX_UE_ITEM_COUNT = (1 << 31) - 1
_BAG_MAX_UE_INSTANCE_ID = (1 << 63) - 1
# 装备词条跨 DS/UE 的硬安全包络:3/9 是 flat,7 是 basis points。
# 正式 equipment_affix 当前远低于此上限;包络只防受损 DS 造未知/负数/超大值,
# 不把 checkpoint 上的历史合法 roll 绑死到当前调优区间。
_BAG_MAX_FLAT_EQUIPMENT_ATTR_VALUE = 1_000_000
_BAG_MAX_MOVE_SPEED_BASIS_POINTS = 10_000  # 单词条最多 +100% 倍率。


def valid_bag_gameplay_attr(attr_id: int, value: int) -> bool:
    if attr_id in (3, 9):  # Atk / Defense flat
        return 0 < value <= _BAG_MAX_FLAT_EQUIPMENT_ATTR_VALUE
    if attr_id == 7:  # MoveSpeedRate basis points
        return 0 < value <= _BAG_MAX_MOVE_SPEED_BASIS_POINTS
    return False


@dataclass(frozen=True, slots=True)
class DSCallerIdentity:
    """调用方 DS 凭据身份(service 层经 DSCallbackGuard 验签后注入;五要件①。

    pod 空 = 未验身份(guard off/dev),授权器按其契约 fail-closed 或降级)。
    """

    pod: str = ""
    uid: str = ""


class OwnerAuthorizer(Protocol):
    """校验并解析写授权(五要件②,查询 §9.22 owner authority)。

    判定:玩家当前 owner 记录必须 ADMITTED、租约在效、且(caller_pod 非空时)
    record.target 与调用方 DS 凭据身份(pod+uid)全等;claimed_epoch 非 0 时还须等于
    当前 epoch。返回解析出的当前 owner_epoch(0 号请求由服务端代填,DS 无需预知 epoch)。
    epoch/身份不符 / 未 ADMITTED / 失租 → ErrBagEpochFenced(调用方停写重查);
    查询失败 / UNKNOWN → ErrUnavailable(fail-closed,原批重试,禁冒充有权)。
    """

    async def authorize_owner_write(
        self, player_id: int, claimed_epoch: int, caller_pod: str, caller_uid: str
    ) -> int: ...


class CapacityCharger(Protocol):
    """容量购买扣费抽象(§5.3 两步 saga 第①步;trade 库)。同 (bag_type, tier) 重试幂等零扣费。"""

    async def charge_bag_capacity(
        self, player_id: int, bag_type: int, tier: int, slots: int, kind: int, price: int
    ) -> tuple[bool, int]: ...


@dataclass(frozen=True, slots=True)
class EffectiveCapacityView:
    """一段的有效容量(LoadBag 权威下发 / 购买响应)。"""

    bag_type: int
    capacity: int


@dataclass(frozen=True, slots=True)
class CapacityPurchaseResult:
    """一次购买的结果(幂等重放返回当前状态,cost=0)。"""

    purchases: int
    extra: int
    effective_capacity: int
    # currency_kind 本次扣费币种(格容购买当前恒为金币;结构上按币种参数化,
    # 将来改成钻石扩容只需改配置,不用改协议与代码)。
    currency_kind: int
    cost: int
    balance: int


class BagUsecase:
    """背包域用例。"""

    __slots__ = ("_repo", "_cfg", "_owner_auth", "_charger")

    def __init__(self, repo, cfg: bconf.BagConf) -> None:  # noqa: ANN001
        self._repo = repo
        self._cfg = cfg
        # 五要件② owner 授权(set_owner_authorizer 注入)。None 时仅当
        # cfg.allow_unverified_owner=True 放行(dev/单测),否则一切写路径 fail-closed 拒。
        self._owner_auth: OwnerAuthorizer | None = None
        # 容量购买扣费(set_capacity_charger 注入;None = 购买链未装配,fail-closed 拒)。
        self._charger: CapacityCharger | None = None

    def set_owner_authorizer(self, authorizer: OwnerAuthorizer | None) -> None:
        self._owner_auth = authorizer

    def set_capacity_charger(self, charger: CapacityCharger | None) -> None:
        self._charger = charger

    async def _authorize_owner(
        self, player_id: int, claimed_epoch: int, caller: DSCallerIdentity
    ) -> int:
        """对写路径统一执行五要件②并解析生效 epoch。

        LoadBag 也在列(它会单调推进 bag_meta.owner_epoch,伪造高 epoch 的加载会把真
        owner 的后续写全部围栏)。未配置授权器且显式跳过(dev)时,直接采用调用方声称的 epoch。
        """
        if self._owner_auth is None:
            if self._cfg.allow_unverified_owner:
                return claimed_epoch
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "owner authorizer unavailable (bag.owner_addr unset)"
            )
        return await self._owner_auth.authorize_owner_write(
            player_id, claimed_epoch, caller.pod, caller.uid
        )

    # ── LoadBag ───────────────────────────────────────────────────────────

    async def load_bag(
        self, player_id: int, owner_epoch: int, caller: DSCallerIdentity
    ) -> tuple[bytes, list[tuple[int, bytes]], int]:
        """加载随身组(owner DS checkout):快照 + journal 尾部 + 权威水位。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        epoch = await self._authorize_owner(player_id, owner_epoch, caller)
        return await self._repo.load_bag(player_id, epoch)

    # ── AppendJournal ─────────────────────────────────────────────────────

    async def append_journal(
        self, player_id: int, owner_epoch: int, entries, caller: DSCallerIdentity  # noqa: ANN001
    ) -> int:
        """追加流水(同步入账;落库成功操作才算完成)。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if len(entries) == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "entries required")
        if len(entries) > self._cfg.max_journal_batch:
            raise errcode.PandoraError(
                errcode.ErrBagQuotaExceeded,
                "batch size %d exceeds max %d",
                len(entries), self._cfg.max_journal_batch,
            )
        epoch = await self._authorize_owner(player_id, owner_epoch, caller)
        for entry in entries:
            if entry.journal_seq == 0:
                raise errcode.PandoraError(errcode.ErrBagSeqConflict, "journal_seq required")
            key = entry.idempotency_key
            if key == "" or len(key) > 128:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "invalid idempotency_key seq=%d", entry.journal_seq
                )
            if not _known_bag_type(entry.bag_type):
                raise errcode.PandoraError(
                    errcode.ErrBagSectionNotAllowed,
                    "unknown bag_type=%d seq=%d",
                    entry.bag_type, entry.journal_seq,
                )
            # 固定段代际恒 0(活动段代际由数据层对 current 校验)。
            if not bapply.is_activity_bag_type(entry.bag_type) and entry.generation != 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "fixed bag_type=%d must carry generation=0 seq=%d",
                    entry.bag_type, entry.journal_seq,
                )
            self._validate_op_shape(entry)
        return await self._repo.append_journal(
            player_id,
            epoch,
            entries,
            self._cfg.section_capacity_of,
            self._cfg.item_max_stack_of,
            self._cfg.hourly_journal_quota,
        )

    def _validate_op_shape(self, entry: bag_pb2.BagJournalEntry) -> None:
        """校验单条 op 的形状与目标段合法性(内容级校验在数据层应用时进行)。"""
        seq = entry.journal_seq
        which = entry.WhichOneof("op")
        if which == "pickup_grant":
            self._check_items_len(entry.pickup_grant.items, seq)
            return
        if which == "mail_claim":
            self._check_items_len(entry.mail_claim.items, seq)
            return
        if which == "transfer":
            op = entry.transfer
            if not _known_bag_type(op.to_bag_type):
                raise errcode.PandoraError(
                    errcode.ErrBagSectionNotAllowed,
                    "transfer to unknown bag_type=%d seq=%d",
                    op.to_bag_type, seq,
                )
            if op.to_bag_type == entry.bag_type:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "transfer to same bag_type=%d seq=%d",
                    entry.bag_type, seq,
                )
            self._check_items_len(op.items, seq)
            return
        if which == "consume":
            op = entry.consume
            if len(op.produce_items) > 0 and not _known_bag_type(op.produce_bag_type):
                raise errcode.PandoraError(
                    errcode.ErrBagSectionNotAllowed,
                    "produce to unknown bag_type=%d seq=%d",
                    op.produce_bag_type, seq,
                )
            self._check_items_len(op.consume_items, seq)
            if len(op.produce_items) > 0:
                self._check_items_len(op.produce_items, seq)
            return
        # 未知 op fail-closed(旧副本遇到新 op 不得静默应用/跳过,§9.21 混版纪律)。
        raise errcode.PandoraError(errcode.ErrInvalidArg, "op required seq=%d", seq)

    def _check_items_len(self, items, seq: int) -> None:  # noqa: ANN001, C901
        if len(items) == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "items required seq=%d", seq)
        if len(items) > self._cfg.max_items_per_op:
            raise errcode.PandoraError(
                errcode.ErrBagQuotaExceeded,
                "items %d exceed max %d seq=%d",
                len(items), self._cfg.max_items_per_op, seq,
            )
        seen_instances: set[int] = set()
        for item_index, item in enumerate(items):
            if item.item_config_id == 0 or item.count == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "invalid item seq=%d index=%d config=%d count=%d",
                    seq, item_index, item.item_config_id, item.count,
                )
            if item.count > _BAG_MAX_UE_ITEM_COUNT:
                raise errcode.PandoraError(
                    errcode.ErrBagQuotaExceeded,
                    "item count=%d exceeds UE int32 max seq=%d index=%d",
                    item.count, seq, item_index,
                )
            instance_id = item.instance_id
            if (instance_id == 0 and (item.identified or len(item.attrs) > 0)) or (
                not item.identified and len(item.attrs) > 0
            ):
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "item instance/identified attrs mismatch seq=%d index=%d",
                    seq, item_index,
                )
            if instance_id != 0:
                if instance_id > _BAG_MAX_UE_INSTANCE_ID:
                    raise errcode.PandoraError(
                        errcode.ErrBagQuotaExceeded,
                        "instance_id=%d exceeds UE int64 max seq=%d",
                        instance_id, seq,
                    )
                if item.count != 1:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "instance_id=%d must carry count=1 seq=%d",
                        instance_id, seq,
                    )
                if instance_id in seen_instances:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "duplicate instance_id=%d seq=%d",
                        instance_id, seq,
                    )
                seen_instances.add(instance_id)
            # ★ 深度闸:单个 item 的词条条数上限。真实教训 —— 只管住 items 条数
            #   (广度)而不管单格 attrs 条数(深度),单行照样能胖到无界(§9.24)。
            if len(item.attrs) > _BAG_MAX_ATTRS_PER_ITEM:
                raise errcode.PandoraError(
                    errcode.ErrBagQuotaExceeded,
                    "attrs=%d exceed max %d seq=%d index=%d",
                    len(item.attrs), _BAG_MAX_ATTRS_PER_ITEM, seq, item_index,
                )
            seen_attrs: set[int] = set()
            for attr_index, attr in enumerate(item.attrs):
                if not valid_bag_gameplay_attr(attr.attr_id, attr.value):
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "unsupported/out-of-range attr seq=%d item=%d attr=%d id=%d value=%d",
                        seq, item_index, attr_index, attr.attr_id, attr.value,
                    )
                if attr.attr_id in seen_attrs:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "duplicate attr_id=%d seq=%d item=%d",
                        attr.attr_id, seq, item_index,
                    )
                seen_attrs.add(attr.attr_id)

    # ── SaveCheckpoint ────────────────────────────────────────────────────

    async def save_checkpoint(
        self,
        player_id: int,
        owner_epoch: int,
        record: bag_pb2.BagStorageRecord,
        snapshot: bytes,
        covered_seq: int,
        caller: DSCallerIdentity,
    ) -> None:
        """保存随身组快照(snapshot 已由 service 层编码为 BagStorageRecord bytes;

        record 用于段类型校验:含后端驻留段整批拒 —— 它们的本体不归 checkpoint)。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        epoch = await self._authorize_owner(player_id, owner_epoch, caller)
        validate_bag_checkpoint(record, snapshot)
        await self._repo.save_checkpoint(player_id, epoch, snapshot, covered_seq)

    # ── GetSections ───────────────────────────────────────────────────────

    async def get_sections(self, player_id: int, bag_types) -> list[bag_pb2.BagSection]:  # noqa: ANN001
        """读后端驻留段(仓库/活动段;活动段按 current generation 过滤)。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if len(bag_types) == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "bag_types required")
        for bag_type in bag_types:
            if not bapply.is_backend_resident_bag_type(bag_type):
                raise errcode.PandoraError(
                    errcode.ErrBagSectionNotAllowed,
                    "bag_type=%d is not backend-resident",
                    bag_type,
                )
        return await self._repo.get_sections(
            player_id, list(bag_types), self._cfg.section_capacity_of
        )

    # ── 有效容量 / 购买(§5.3)─────────────────────────────────────────────

    async def carry_effective_capacities(self, player_id: int) -> list[EffectiveCapacityView]:
        """随身组各段有效容量(base 配置 + 已购增量)。

        只含服务端已配置 base 的段 —— 未配置的段 DS 沿用客户端配置(过渡)。
        """
        out: list[EffectiveCapacityView] = []
        for bag_type in (0, 2, 3):
            base = self._cfg.section_capacity_of(bag_type)
            if base == 0:
                continue
            extra, _purchases = await self._repo.get_capacity_state(player_id, bag_type)
            eff = min(base + extra, bapply.UINT32_MAX)
            out.append(EffectiveCapacityView(bag_type=bag_type, capacity=eff))
        return out

    async def purchase_capacity(
        self, player_id: int, owner_epoch: int, bag_type: int, caller: DSCallerIdentity
    ) -> CapacityPurchaseResult:
        """购买容量扩容(§5.3):定档 → 扣费(幂等)→ 落位(档数 CAS)。

        幂等身份 = (player, bag_type, 第 purchases+1 档);同档重试 / 双击并发收敛为
        单次扣费单次生效。两步间崩溃由同 tier 重试收敛(①回放零扣费 → ②补应用)。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        rule = self._cfg.capacity_purchase_rule_of(bag_type)
        if rule is None:
            raise errcode.PandoraError(
                errcode.ErrBagSectionNotAllowed, "bag_type=%d not purchasable", bag_type
            )
        if self._charger is None:
            raise errcode.PandoraError(errcode.ErrUnavailable, "capacity charger unavailable")
        await self._authorize_owner(player_id, owner_epoch, caller)

        extra, purchases = await self._repo.get_capacity_state(player_id, bag_type)
        tier = purchases + 1
        if tier > len(rule.tiers):
            raise errcode.PandoraError(
                errcode.ErrBagCapacityMaxed,
                "all %d tiers purchased player=%d bag=%d",
                len(rule.tiers), player_id, bag_type,
            )
        t = rule.tiers[tier - 1]
        if extra + t.slots > rule.max_extra:
            raise errcode.PandoraError(
                errcode.ErrBagCapacityMaxed,
                "extra %d+%d exceeds max_extra %d player=%d bag=%d",
                extra, t.slots, rule.max_extra, player_id, bag_type,
            )

        # 格容购买当前恒用金币。显式传而不是留 UNSPECIFIED:
        # 扣费侧对未知币种一律 fail-closed,**不会**回退成金币(currency.proto)。
        charge_kind = ccy.CURRENCY_GOLD
        already, remaining_balance = await self._charger.charge_bag_capacity(
            player_id, bag_type, tier, t.slots, charge_kind, t.price_gold
        )
        # 已扣费未落位(配置中途收缩等):错误如实上抛,凭 ledger 行 + 同 tier 重试收敛/排障。
        new_extra, new_purchases, _applied = await self._repo.apply_capacity_purchase(
            player_id, bag_type, tier, t.slots, rule.max_extra
        )

        base = self._cfg.section_capacity_of(bag_type)
        return CapacityPurchaseResult(
            purchases=new_purchases,
            extra=new_extra,
            effective_capacity=min(base + new_extra, bapply.UINT32_MAX),
            currency_kind=charge_kind,
            cost=0 if already else t.price_gold,
            balance=remaining_balance,
        )

    # ── 保留期清理(§9.24)─────────────────────────────────────────────────

    async def run_journal_sweep(self, batch: int) -> int:
        """周期清理超保留期**且已被 checkpoint 覆盖**的流水(多副本各自跑,DELETE 幂等)。

        删除资格 = 覆盖水位证明,时间阈值只是附加条件:未覆盖尾部是 LoadBag 唯一恢复
        数据,时间到期也绝不删(INC-20260722-003,谓词在 repo.sweep_journal)。
        """
        retention_seconds = self._cfg.journal_retention_days * 24 * 3600
        return await self._repo.sweep_journal(retention_seconds, batch)


def _known_bag_type(bag_type: int) -> bool:
    """已知段类型(随身组 + 后端驻留组;其余整批拒)。"""
    return bapply.carry_group_bag_type(bag_type) or bapply.is_backend_resident_bag_type(bag_type)


def validate_bag_checkpoint(  # noqa: C901, PLR0912 —— 与 Go 同为一串线性形状校验
    record: bag_pb2.BagStorageRecord, snapshot: bytes
) -> None:
    """只校验信任边界和可恢复结构,不重算 DS 权威布局/MaxStack。

    特别地,slot 可以 >= capacity、items 可以 > capacity:这是 §3.2 journal 重放后
    "临时格超容、只出不进"的合法恢复态,服务端必须原样保存,不能把玩家资产挡在场外。
    """
    if record is None:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "checkpoint snapshot required")
    if len(snapshot) > BAG_CHECKPOINT_MAX_BYTES:
        raise errcode.PandoraError(
            errcode.ErrBagQuotaExceeded,
            "checkpoint bytes %d exceed max %d",
            len(snapshot), BAG_CHECKPOINT_MAX_BYTES,
        )
    sections = record.sections
    if len(sections) > _BAG_CHECKPOINT_MAX_SECTIONS:
        raise errcode.PandoraError(
            errcode.ErrBagQuotaExceeded,
            "checkpoint sections %d exceed max %d",
            len(sections), _BAG_CHECKPOINT_MAX_SECTIONS,
        )

    seen_sections: set[int] = set()
    seen_instances: set[int] = set()
    for sec in sections:
        bag_type = sec.bag_type
        if not bapply.carry_group_bag_type(bag_type):
            raise errcode.PandoraError(
                errcode.ErrBagSectionNotAllowed,
                "checkpoint must not carry backend-resident bag_type=%d",
                bag_type,
            )
        if bag_type in seen_sections:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "duplicate checkpoint bag_type=%d", bag_type
            )
        seen_sections.add(bag_type)
        if sec.generation != 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "fixed checkpoint bag_type=%d must carry generation=0",
                bag_type,
            )
        # capacity=0 在混版滚动期是允许的:LoadBag 另行下发权威有效容量,
        # 缺省时 UE 回落到自身配置 base。
        if sec.capacity > _BAG_CHECKPOINT_MAX_GRID_COUNT:
            raise errcode.PandoraError(
                errcode.ErrBagQuotaExceeded,
                "checkpoint bag_type=%d capacity=%d exceeds max %d",
                bag_type, sec.capacity, _BAG_CHECKPOINT_MAX_GRID_COUNT,
            )
        if len(sec.items) > _BAG_CHECKPOINT_MAX_GRID_COUNT:
            raise errcode.PandoraError(
                errcode.ErrBagQuotaExceeded,
                "checkpoint bag_type=%d items=%d exceed max %d",
                bag_type, len(sec.items), _BAG_CHECKPOINT_MAX_GRID_COUNT,
            )

        seen_slots: set[int] = set()
        for item_index, item in enumerate(sec.items):
            if item.item_config_id == 0 or item.count == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "invalid checkpoint item bag_type=%d index=%d config=%d count=%d",
                    bag_type, item_index, item.item_config_id, item.count,
                )
            if item.count > _BAG_MAX_UE_ITEM_COUNT:
                raise errcode.PandoraError(
                    errcode.ErrBagQuotaExceeded,
                    "checkpoint item count=%d exceeds UE int32 max bag_type=%d index=%d",
                    item.count, bag_type, item_index,
                )
            if item.slot >= _BAG_CHECKPOINT_MAX_GRID_COUNT:
                raise errcode.PandoraError(
                    errcode.ErrBagQuotaExceeded,
                    "checkpoint slot=%d exceeds max %d bag_type=%d",
                    item.slot, _BAG_CHECKPOINT_MAX_GRID_COUNT - 1, bag_type,
                )
            if item.slot in seen_slots:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "duplicate checkpoint slot=%d bag_type=%d",
                    item.slot, bag_type,
                )
            seen_slots.add(item.slot)

            instance_id = item.instance_id
            if (instance_id == 0 and (item.identified or len(item.attrs) > 0)) or (
                not item.identified and len(item.attrs) > 0
            ):
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "checkpoint item instance/identified attrs mismatch bag_type=%d index=%d",
                    bag_type, item_index,
                )
            if instance_id != 0:
                if instance_id > _BAG_MAX_UE_INSTANCE_ID:
                    raise errcode.PandoraError(
                        errcode.ErrBagQuotaExceeded,
                        "checkpoint instance_id=%d exceeds UE int64 max",
                        instance_id,
                    )
                if item.count != 1:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "checkpoint instance_id=%d must carry count=1",
                        instance_id,
                    )
                if instance_id in seen_instances:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "duplicate checkpoint instance_id=%d",
                        instance_id,
                    )
                seen_instances.add(instance_id)
            if len(item.attrs) > _BAG_MAX_ATTRS_PER_ITEM:
                raise errcode.PandoraError(
                    errcode.ErrBagQuotaExceeded,
                    "checkpoint attrs=%d exceed max %d instance_id=%d",
                    len(item.attrs), _BAG_MAX_ATTRS_PER_ITEM, instance_id,
                )
            seen_attrs: set[int] = set()
            for attr_index, attr in enumerate(item.attrs):
                if not valid_bag_gameplay_attr(attr.attr_id, attr.value):
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "unsupported/out-of-range checkpoint attr instance_id=%d "
                        "index=%d id=%d value=%d",
                        instance_id, attr_index, attr.attr_id, attr.value,
                    )
                if attr.attr_id in seen_attrs:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "duplicate checkpoint attr_id=%d instance_id=%d",
                        attr.attr_id, instance_id,
                    )
                seen_attrs.add(attr.attr_id)
