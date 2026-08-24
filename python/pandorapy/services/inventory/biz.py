"""inventory 业务逻辑层 —— 对应 Go 侧 internal/biz/inventory.go / transfer.go / sweep.go /
inventory_sharding.go。

职责:
  - 背包道具持有 / 货币余额读
  - 系统驱动幂等发放(GrantItems / GrantInstances:战后掉落 / 活动 / 购买到账)
  - 大厅态使用(UseItem)与出售换金币(SellItem / SellInstance)
  - 拍卖 / P2P 结算与挂单托管(escrow 三件套)
  - 邮件 transfer 附件实例托管(escrow_out / claim / release / consume)

边界(ds-arch.md §0.1):战斗内即时用道具 / 出装 / 购买道具走 UE GAS,不经 gRPC。

★ 关键不变量(CLAUDE.md §9.7):发放 / 扣减必须原子 + 幂等键;
  校验数量在 data 层 `SELECT ... FOR UPDATE` 锁行内做,避免并发超扣。
  usable / sellable / 是否装备一律由 **configtable 道具表**裁决(catalog),
  未注入 catalog 即 fail-closed —— 一份可能与 UE 漂移的兜底数值参与扣减 / 入账,
  比拒掉一次请求危险得多。

★ 幂等键格式是**跨服务契约**,必须与 Go 逐字一致:
      auction:settle:<match_id>
      trade:settle:<order_id>
  它是 inventory_ledger 里的去重键。格式变一个字符,Go 版写过的那笔在 Python 版
  看来就是"没结算过" —— **重复入账**。迁移期两栈并存时这不是理论风险。
"""

from __future__ import annotations

import random

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy.services.inventory import catalog as cat
from pandorapy.services.inventory import currency as ccy
from pandorapy.services.inventory.conf import InventoryConf
from pandorapy.services.inventory.currency_biz import ShopUsecaseMixin
from pandorapy.services.inventory.models import (
    EscrowKind,
    EscrowedInstance,
    InstanceOwnershipQuery,
    ItemAttribute,
    ItemGrant,
    ItemInstance,
    ItemStack,
    SaleOutcome,
    TransferClaimItem,
)
from pandorapy.services.inventory import settle as settle_mod

# ── 请求形状上限(§9.18 读取侧 / 写入侧上限)────────────────────────────────

# 一次 CheckItemsOwned 可查询的道具配置数上限。
# 唯一调用方 player.SetEquipment 一次最多提交装备部位数量个配置(个位数),取 64 留余量;
# 超限**直接拒而不是静默截断** —— 截断会把「未查」伪装成「未持有」,
# 让拥有权校验在超长请求下静默放行或静默误拒。
MAX_CHECK_ITEMS_OWNED = 64
MAX_CHECK_INSTANCES_OWNED = 64

# 单次托管/领取/释放的实例数上限(mail 单封附件上限 16 的宽裕倍;防超大批量长事务锁表)。
MAX_TRANSFER_BATCH = 64

# 幂等键列宽(inventory_ledger.idempotency_key VARCHAR(64))。
# 超长的键写进去会在严格模式下报 1406、在非严格模式下被**静默截断** ——
# 截断后两个不同的键会撞成同一个,后一笔被当成幂等重放而静默不执行。
MAX_IDEMPOTENCY_KEY_LEN = 64


# ── 拍卖分片对账口径(scale-cellular-20m.md §4.2)────────────────────────────


def auction_leg_key(match_id: int, player_id: int, leg: str) -> str:
    """拍卖成交对转账本一条腿的幂等键口径(对账主键)。

    canonical `auction:settle:<match_id>:<player_id>:<leg>`,与现状结算幂等键
    `auction:settle:<match_id>` 同源、再细分到 (player_id, leg)。分片落地时每条腿
    在各自 owner cell 幂等写,重复消费同一腿命中唯一键只对转一次。
    纯函数、确定性;留在这里是为了让分片落地时各腿消费者有**单一口径**,不再各写一份。
    """
    return f"auction:settle:{match_id}:{player_id}:{leg}"


LEG_SELLER_DELIVER = "seller_deliver"
LEG_SELLER_RECEIVE = "seller_receive"
LEG_BUYER_PAY = "buyer_pay"
LEG_BUYER_RECEIVE = "buyer_receive"


def _default_rand_intn(n: int) -> int:
    """默认随机源。n<=0 约定返回 0(与 Go 的 defaultRandIntn 一致)。"""
    if n <= 0:
        return 0
    return random.randrange(n)


class InventoryUsecase(ShopUsecaseMixin):
    """inventory 服务业务逻辑核心。对应 Go 的 biz.InventoryUsecase。

    货币规则与 NPC 商店在 ShopUsecaseMixin(currency_biz.py),对应 Go 的 biz/currency.go。
    """

    __slots__ = ("_repo", "_cfg", "_catalog", "_sf", "_rand_intn", "_shops")

    def __init__(self, repo, cfg: InventoryConf) -> None:  # noqa: ANN001
        self._repo = repo
        self._cfg = cfg
        # catalog 是道具规则的唯一权威(生产 = configtable item 表)。未注入即 fail-closed。
        self._catalog: cat.Store | None = None
        # sf 生成装备实例 instance_id。可为 None:未装配时 GrantInstances 返回
        # ErrInvalidArg(实例背包未启用),不影响堆叠计数背包。
        self._sf = None
        # shops 是商店表的唯一权威(生产 = configtable shop 表)。未注入即 fail-closed:
        # 一份可能与客户端漂移的兜底价格参与扣钱,比拒掉一次购买危险得多。
        self._shops = None
        self._rand_intn = _default_rand_intn

    # ── 注入(用 setter 而非构造参数,避免旧调用点被迫改签名)──

    def set_item_catalog(self, catalog: cat.Store) -> None:
        self._catalog = catalog

    def set_snowflake(self, sf) -> None:  # noqa: ANN001
        self._sf = sf

    def set_rand_source(self, fn) -> None:  # noqa: ANN001
        """注入鉴定 roll 随机源(测试用确定性序列)。"""
        if fn is not None:
            self._rand_intn = fn

    def _item_definition(self, item_config_id: int) -> cat.ItemDefinition | None:
        """现查道具表当前批次。catalog 未注入时 fail-closed(返回 None)。"""
        if self._catalog is None:
            return None
        return self._catalog.lookup(item_config_id)

    # ── 读 ────────────────────────────────────────────────────────────────

    async def get_inventory(self, player_id: int) -> tuple[dict[int, int], list[ItemStack]]:
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        return await self._repo.get_inventory(player_id)

    async def get_inventory_full(
        self, player_id: int
    ) -> tuple[dict[int, int], list[ItemStack], int, list[ItemInstance]]:
        """全部币种余额 + 堆叠道具 + 容量 + 装备实例。

        ★ 未启用实例背包(capacity<=0)时**不读** player_item_instance 表:
          既有库可能尚未迁移出该表,读它会让整个 GetInventory 报内部错。
          启用时 main 启动期已做 schema 检查,不会走到"表不存在"。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        balances, items = await self._repo.get_inventory(player_id)
        if self._cfg.capacity <= 0:
            return balances, items, 0, []
        instances = await self._repo.list_instances(player_id)
        return balances, items, self._cfg.capacity, instances

    async def check_items_owned(self, player_id: int, item_config_ids) -> list[int]:  # noqa: ANN001
        """批量查询玩家持有情况,返回入参集合中**确实持有**的子集(去重,升序)。

        「持有」= 可堆叠计数 > 0(player_items)**或**存在该配置的装备实例
        (player_item_instance),两条路任一成立即算持有:装备走实例模型,
        消耗品走堆叠计数,调用方不必关心底层形态。

        实现刻意复用既有 get_inventory / list_instances 而不新写一条定向 SQL
        (§15.2 最少复杂度):单玩家背包被容量与堆叠行数有界,而本接口只在
        大厅态改出战预设时调用,属低频路径。
        """
        item_config_ids = list(item_config_ids)
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if not item_config_ids:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "item_config_ids required")
        if len(item_config_ids) > MAX_CHECK_ITEMS_OWNED:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "too many item_config_ids: %d > %d",
                len(item_config_ids),
                MAX_CHECK_ITEMS_OWNED,
            )
        want: set[int] = set()
        for cid in item_config_ids:
            if cid == 0:
                raise errcode.PandoraError(errcode.ErrInvalidArg, "item_config_id required")
            want.add(cid)

        _balances, items = await self._repo.get_inventory(player_id)
        owned = {it.item_config_id for it in items if it.count > 0 and it.item_config_id in want}

        # 未启用实例背包时不读 player_item_instance(与 get_inventory_full 同一条件)。
        if self._cfg.capacity > 0:
            for inst in await self._repo.list_instances(player_id):
                if inst.item_config_id in want:
                    owned.add(inst.item_config_id)
        # 定序返回,让调用方与用例拿到确定结果(集合遍历顺序不稳定)。
        return sorted(owned)

    async def check_instances_owned(self, player_id: int, queries) -> list[ItemInstance]:  # noqa: ANN001
        """精确校验唯一实例归属。instance_id 与 item_config_id 必须**同时**匹配。"""
        queries = list(queries)
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if not queries:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "instances required")
        if len(queries) > MAX_CHECK_INSTANCES_OWNED:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "too many instances: %d > %d",
                len(queries),
                MAX_CHECK_INSTANCES_OWNED,
            )
        seen: set[int] = set()
        for q in queries:
            if q.instance_id == 0 or q.item_config_id == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "instance_id and item_config_id required"
                )
            if q.instance_id in seen:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "duplicate instance_id: %d", q.instance_id
                )
            seen.add(q.instance_id)
            if self._catalog is not None:
                definition = self._item_definition(q.item_config_id)
                if definition is None or not definition.equipment:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "instance query config is not configured equipment: %d",
                        q.item_config_id,
                    )
        return await self._repo.check_instances_owned(player_id, queries)

    # ── 堆叠道具:发放 / 使用 / 丢弃 / 出售 ────────────────────────────────

    async def grant_items(
        self, player_id: int, items: list[ItemGrant], currencies, idempotency_key: str
    ) -> dict[int, int]:  # noqa: ANN001
        """幂等发放道具 + 多币种货币(系统驱动,idempotency_key 防重复入账)。

        返回发放后**全部币种**余额。

        ★ currencies 的校验刻意逐条做,而不是"总额不为负"一刀切。
          旧实现写的是 `if gold < 0`;协议改成 uint64 后,Go 侧那句会**恒为 false**
          (编译器与 linter 都不报),等于闸门被静默拆掉。Python 侧则相反 ——
          它不会恒为 false,而是**根本拦不住**:上游若真传了负数,dict 里就是负数,
          一路写进 UNSIGNED 列被截断。所以正确形态是在**类型边界**校验
          (service 层 balances_from_proto 判币种/正数/重复),内层改判
          "是否为 0 / 是否超单笔上限",并在 currency.add_currency_tx 再加一道显式负数闸。
        """
        currencies = dict(currencies or {})
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if not idempotency_key:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "idempotency_key required")
        if not items and not currencies:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "nothing to grant")
        for kind, amount in currencies.items():
            ccy.validate_currency_kind(kind)
            if amount == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "grant amount must be positive: kind=%d",
                    int(kind),
                )
            if amount < 0:
                # Go 侧 uint64 让这条不可能;Python 必须显式拒(见上方 docstring)。
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "grant amount must be positive: kind=%d",
                    int(kind),
                )
            if amount > self._max_currency_per_grant():
                # 单笔发放上限(§9.6 五要件④ 额度):防止上游算错把天文数字灌进经济体。
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "grant amount exceeds per-call limit: kind=%d amount=%d limit=%d",
                    int(kind),
                    amount,
                    self._max_currency_per_grant(),
                )
        for it in items:
            if it.item_config_id == 0:
                raise errcode.PandoraError(errcode.ErrInvalidArg, "item_config_id required")
            if it.count <= 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "count must be positive: item=%d", it.item_config_id
                )
            if self._catalog is not None:
                definition = self._item_definition(it.item_config_id)
                if definition is None:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg, "unknown item_config_id: %d", it.item_config_id
                    )
                if definition.equipment:
                    # 装备走实例模型:按配置 ID 堆叠会让"同配置不同词条"的两件装备合并成计数。
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "equipment item %d must use GrantInstances",
                        it.item_config_id,
                    )
        # detail 格式与多币种改造前**逐字节一致**(`gold=%d`):它是人读审计串,
        # 两栈流水混看时格式必须同源;金币之外的币种由 result_currency_delta 列承担。
        detail = f"grant items={len(items)} gold={ccy.balances_get(currencies, ccy.CURRENCY_GOLD)}"
        new_balances, already = await self._repo.grant_items(
            player_id, items, currencies, idempotency_key, detail
        )
        if already:
            plog.get().info(
                "grant_items_idempotent_hit",
                player_id=player_id,
                idempotency_key=idempotency_key,
                gold=ccy.balances_get(new_balances, ccy.CURRENCY_GOLD),
            )
        return new_balances

    async def use_item(
        self, player_id: int, item_config_id: int, count: int, idempotency_key: str
    ) -> int:
        """大厅态使用消耗品。

        ★ 判据是 `lobby_usable`,而配置表适配器把它**恒置 False**(见 catalog.py):
          item.usable 的真实语义是"局内 UE GAS 可消费",大厅没有效果派发器。
          若把 usable 直接当大厅可用,UseItem 会扣掉道具而效果一个也不发生。
          真正的局内扣减走 ConsumeBattleItem(系统接口,凭可信战斗事实)。
        """
        self._require_stack_op_args(player_id, item_config_id, count, idempotency_key)
        definition = self._item_definition(item_config_id)
        if definition is None or not definition.lobby_usable or definition.equipment:
            raise errcode.PandoraError(
                errcode.ErrInventoryItemNotUsable, "item not usable in lobby: %d", item_config_id
            )
        detail = f"use item={item_config_id} count={count}"
        remaining, already = await self._repo.use_item(
            player_id, item_config_id, count, idempotency_key, detail
        )
        if already:
            plog.get().info(
                "use_item_idempotent_hit",
                player_id=player_id,
                idempotency_key=idempotency_key,
                item=item_config_id,
                remaining=remaining,
            )
        return remaining

    async def consume_battle_item(
        self, player_id: int, item_config_id: int, count: int, idempotency_key: str
    ) -> int:
        """按可信 DS 进度事实持久扣减局内消耗品(系统接口)。

        与大厅 UseItem 分离,避免把 item.usable 误解为"扣掉即完成效果":
        实际回血/增益由 UE GAS 在 ReportProgress ACK 后执行;
        这里仅保证后端资产不会在重登后复活。
        """
        self._require_stack_op_args(player_id, item_config_id, count, idempotency_key)
        definition = self._item_definition(item_config_id)
        if definition is None or not definition.battle_usable or definition.equipment:
            raise errcode.PandoraError(
                errcode.ErrInventoryItemNotUsable,
                "item not consumable in battle: %d",
                item_config_id,
            )
        detail = f"battle consume item={item_config_id} count={count}"
        remaining, already = await self._repo.consume_battle_item(
            player_id, item_config_id, count, idempotency_key, detail
        )
        if already:
            plog.get().info(
                "consume_battle_item_idempotent_hit",
                player_id=player_id,
                idempotency_key=idempotency_key,
                item=item_config_id,
                remaining=remaining,
            )
        return remaining

    async def discard_battle_item(
        self, player_id: int, item_config_id: int, count: int, idempotency_key: str
    ) -> int:
        """按可信 DS 进度事实持久丢弃可堆叠物品(系统接口)。

        ★ 明确只接收非装备配置:装备实例没有可靠的 phase0 DS Guid ↔ instance_id 映射,
          按配置 ID 丢会随机删掉同配置但不同词条的实例。
        """
        if player_id == 0 or item_config_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "player_id and item_config_id required"
            )
        if count <= 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "count must be positive")
        if not idempotency_key:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "idempotency_key required")
        definition = self._item_definition(item_config_id)
        if definition is None or definition.equipment:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle discard only supports configured stackable item: %d",
                item_config_id,
            )
        detail = f"battle discard item={item_config_id} count={count}"
        remaining, already = await self._repo.discard_battle_item(
            player_id, item_config_id, count, idempotency_key, detail
        )
        if already:
            plog.get().info(
                "discard_battle_item_idempotent_hit",
                player_id=player_id,
                idempotency_key=idempotency_key,
                item=item_config_id,
                remaining=remaining,
            )
        return remaining

    async def discard_item(
        self, player_id: int, item_config_id: int, count: int, idempotency_key: str
    ) -> int:
        """幂等丢弃可堆叠物品(客户端面)。装备实例必须走 DiscardInstance。"""
        if player_id == 0 or item_config_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "player_id and item_config_id required"
            )
        if count <= 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "count must be positive")
        if not idempotency_key:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "idempotency_key required")
        if self._catalog is not None:
            definition = self._item_definition(item_config_id)
            if definition is None or definition.equipment:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "item %d is not configured stackable", item_config_id
                )
        detail = f"discard item={item_config_id} count={count}"
        remaining, already = await self._repo.discard_item(
            player_id, item_config_id, count, idempotency_key, detail
        )
        if already:
            plog.get().info(
                "discard_item_idempotent_hit",
                player_id=player_id,
                idempotency_key=idempotency_key,
                item=item_config_id,
                remaining=remaining,
            )
        return remaining

    async def sell_item(
        self, player_id: int, item_config_id: int, count: int, idempotency_key: str
    ) -> SaleOutcome:
        """出售道具换货币(币种由 sell_currency_kind 配置,留空 = 金币)。

        ★ 溢出的处理很绕但必须照抄:单价×数量溢出时 amount 置 0 交给 repo,
          repo 会以 ErrInventoryNotSellable 拒;此时 biz 把它**改写成 ErrInvalidArg**。
          直接在 biz 拒掉的话,同 key 的重试就拿不到 repo 的幂等回放
          —— 而首次请求可能已经成功过。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if item_config_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "item_config_id required")
        if count <= 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "count must be positive")
        if not idempotency_key:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "idempotency_key required")
        kind = self._sell_currency_kind()
        definition = self._item_definition(item_config_id)
        amount = 0
        overflow = False
        if definition is not None and not definition.equipment and definition.sell_unit_price > 0:
            amount, ok = ccy.safe_mul_currency(definition.sell_unit_price, count)
            overflow = not ok or amount == 0
            if overflow:
                amount = 0
        # detail 格式保持与多币种改造前**逐字节一致**(`gold=%d`):升级前写下的流水行靠
        # repo_sql._legacy_sale_ledger_matches 解析这个格式判定"是否同一次出售",
        # 格式一变,老玩家用老 key 重试就会被误判成 ErrInventoryIdempotencyConflict。
        detail = f"sell item={item_config_id} count={count} gold={amount}"
        try:
            outcome, already = await self._repo.sell_item(
                player_id, item_config_id, count, kind, amount, idempotency_key, detail
            )
        except errcode.PandoraError as exc:
            if overflow and exc.code == errcode.ErrInventoryNotSellable:
                # 溢出与"本来就不可出售"在 repo 里都表现为 amount==0;这里用调用侧记下的
                # overflow 标志把它翻回更准确的错误码,免得策划以为是自己没配售价。
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "sell amount overflow item=%d price=%d count=%d",
                    item_config_id,
                    definition.sell_unit_price if definition else 0,
                    count,
                ) from exc
            raise
        if already:
            plog.get().info(
                "sell_item_idempotent_hit",
                player_id=player_id,
                idempotency_key=idempotency_key,
                item=item_config_id,
                remaining=outcome.remaining,
                earned=outcome.earned,
            )
        return outcome

    @staticmethod
    def _require_stack_op_args(
        player_id: int, item_config_id: int, count: int, idempotency_key: str
    ) -> None:
        """UseItem / ConsumeBattleItem 的四条形状校验(顺序与 Go 一致)。

        顺序要紧:Go 先查 player_id、再 item_config_id、再 count、最后 idempotency_key,
        调用方按第一个错误分支排障,乱序会让同一份非法请求在两栈上报不同的原因。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if item_config_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "item_config_id required")
        if count <= 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "count must be positive")
        if not idempotency_key:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "idempotency_key required")

    # ── 装备实例 ──────────────────────────────────────────────────────────

    async def grant_instances(
        self, player_id: int, item_config_ids, idempotency_key: str
    ) -> list[ItemInstance]:  # noqa: ANN001
        """幂等发放装备实例(系统驱动:掉落 / 活动 / 购买到账)。"""
        item_config_ids = list(item_config_ids)
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if not idempotency_key:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "idempotency_key required")
        if not item_config_ids:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "nothing to grant")
        for cid in item_config_ids:
            if cid == 0:
                raise errcode.PandoraError(errcode.ErrInvalidArg, "item_config_id required")
            if self._catalog is not None:
                definition = self._item_definition(cid)
                if definition is None:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg, "unknown item_config_id: %d", cid
                    )
                if not definition.equipment:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg, "stackable item %d must use GrantItems", cid
                    )
        if self._sf is None:
            # 实例背包未启用(inventory.capacity<=0 → main 不装配 snowflake)。
            # 调用方拿到 code=4 而 gRPC 本身 rpc_ok —— 2026-08-06「战斗掉落进不了背包」
            # 的表现就是这个,判据是启动日志没有 instance_bag_enabled。
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "instance inventory not enabled (no id generator)"
            )
        # 一次 CAS 预留整段,替代循环调用 Generate len(itemConfigIDs) 次。
        # 产出严格递增且唯一,但**不保证连续**(跨秒有空洞);此处只按下标一一对应取用。
        instance_ids = [0] * len(item_config_ids)
        self._sf.generate_into(instance_ids)
        insts, already = await self._repo.grant_instances(
            player_id,
            list(instance_ids),
            item_config_ids,
            self._cfg.capacity,
            idempotency_key,
        )
        if already:
            plog.get().info(
                "grant_instances_idempotent_hit",
                player_id=player_id,
                idempotency_key=idempotency_key,
                count=len(insts),
            )
        return insts

    async def identify_item(self, player_id: int, instance_id: int) -> ItemInstance:
        """鉴定一件未鉴定装备实例:服务端权威 roll 随机属性(反作弊,§6)后落库。

        幂等:已鉴定 → 回放已落定属性(不重复 roll)。

        ★ 两段式(与 Go 同):先不锁读实例拿 item_config_id 预 roll,
          再由数据层 FOR UPDATE 裁决"已鉴定则回放、否则落定"。
          实例的 config 不可变,所以预读到过期 config 的概率为零。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if instance_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "instance_id required")
        insts = await self._repo.list_instances(player_id)
        config_id = 0
        for inst in insts:
            if inst.instance_id == instance_id:
                config_id = inst.item_config_id
                break
        if config_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInventoryItemNotFound,
                "instance not found player=%d id=%d",
                player_id,
                instance_id,
            )
        if self._catalog is not None:
            definition = self._item_definition(config_id)
            if definition is None or not definition.equipment:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "instance config is not a configured equipment: %d",
                    config_id,
                )
        attrs = self._roll_identify_attrs(config_id)
        if self._catalog is not None and not attrs:
            # ★ fail-closed:接了配置表之后,roll 不出词条**必须拒**。
            #   放行会把装备永久写成 identified=true 且零词条 —— 不可逆。
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "equipment identify rule unavailable: %d", config_id
            )
        inst, already = await self._repo.identify_instance(player_id, instance_id, attrs)
        if already:
            plog.get().info(
                "identify_item_idempotent_hit", player_id=player_id, instance_id=instance_id
            )
        return inst

    def _roll_identify_attrs(self, item_config_id: int) -> list[ItemAttribute]:
        """按配置从属性池**加权不放回**抽 attr_count 条,每条在 [min,max] 均匀 roll。

        无规则 / 空池 / 任一候选非法 → 返回空(调用方 fail-closed)。

        ★ 逐次校验候选是刻意的:配置加载门已限制候选数/总权重/数值范围,
          但自定义 Catalog(单测)能绕过门禁 —— 这里再挡一次,
          防止一个负权重或 min>max 的候选让抽取逻辑产生不可预期的值。
        """
        definition: cat.IdentifyDefinition | None = None
        if self._catalog is not None:
            definition = self._catalog.identify_rule(item_config_id)
            if definition is None:
                return []
        else:
            rule = self._cfg.identify_rule_of(item_config_id)
            if rule is None:
                return []
            definition = cat.IdentifyDefinition(
                attr_count=rule.attr_count,
                # 旧 YAML 没有权重;兼容路径按等权处理。
                pool=[
                    cat.IdentifyAttrDefinition(attr_id=p.attr_id, weight=1, min=p.min, max=p.max)
                    for p in rule.pool
                ],
            )
        if definition.attr_count <= 0 or definition.attr_count > len(definition.pool):
            return []

        remaining = list(definition.pool)
        out: list[ItemAttribute] = []
        while len(out) < definition.attr_count:
            total_weight = 0
            for c in remaining:
                if c.attr_id == 0 or c.weight <= 0 or c.min < 0 or c.max < c.min:
                    return []
                total_weight += c.weight
            if total_weight <= 0:
                return []
            draw = self._rand_intn(total_weight)
            chosen = len(remaining) - 1
            for i, c in enumerate(remaining):
                if draw < c.weight:
                    chosen = i
                    break
                draw -= c.weight
            candidate = remaining[chosen]
            span = candidate.max - candidate.min + 1
            if span <= 0:
                return []
            out.append(
                ItemAttribute(
                    attr_id=candidate.attr_id,
                    value=candidate.min + self._rand_intn(span),
                )
            )
            remaining.pop(chosen)
        return out

    async def move_instance(self, player_id: int, instance_id: int, to_slot: int) -> ItemInstance:
        """移动一件装备实例到新格子(纯大厅整理,不影响属性)。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if instance_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "instance_id required")
        if self._cfg.capacity <= 0:
            raise errcode.PandoraError(
                errcode.ErrInventorySlotOccupied, "instance inventory disabled"
            )
        return await self._repo.move_instance(player_id, instance_id, to_slot, self._cfg.capacity)

    async def discard_instance(self, player_id: int, instance_id: int) -> None:
        """丢弃一件装备实例(从背包永久删除)。幂等:已丢弃 → OK no-op。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if instance_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "instance_id required")
        await self._repo.discard_instance(player_id, instance_id)

    async def sell_instance(
        self, player_id: int, instance_id: int, item_config_id: int, idempotency_key: str
    ) -> SaleOutcome:
        """原子出售唯一装备实例。

        ★ 即使热更后配置已删 / 禁售也要进 repo:同 key 的首次成功结果必须从 ledger 回放。
          amount=0 的首次请求由 repo 在 claim 后回滚为 NotSellable,不会建脏流水。
        """
        if player_id == 0 or instance_id == 0 or item_config_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "player_id, instance_id and item_config_id required"
            )
        if not idempotency_key:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "idempotency_key required")
        kind = self._sell_currency_kind()
        definition = self._item_definition(item_config_id)
        amount = 0
        if (
            definition is not None
            and (self._catalog is None or definition.equipment)
            and definition.sell_unit_price > 0
        ):
            amount = definition.sell_unit_price
        # detail 格式保持与多币种改造前逐字节一致(`gold=%d`),理由见 sell_item。
        detail = f"sell instance={instance_id} item={item_config_id} gold={amount}"
        outcome, already = await self._repo.sell_instance(
            player_id, instance_id, item_config_id, kind, amount, idempotency_key, detail
        )
        if already:
            plog.get().info(
                "sell_instance_idempotent_hit",
                player_id=player_id,
                instance_id=instance_id,
                idempotency_key=idempotency_key,
                earned=outcome.earned,
                balances=ccy.describe_balances(outcome.balances),
            )
        return outcome

    # ── 结算 ──────────────────────────────────────────────────────────────

    async def settle_auction_match(
        self,
        match_id: int,
        seller_id: int,
        buyer_id: int,
        sell_order_id: int,
        buy_order_id: int,
        item_config_id: int,
        quantity: int,
        kind: int,
        unit_price: int,
    ) -> None:
        """原子结算一笔拍卖成交(幂等键基于 match_id)。"""
        # 入参校验 / 溢出守卫 / 幂等键格式复用 settle.py(与 auction 服务共用同一份口径,
        # 不在这里再抄一遍 —— 抄一遍就多一个会漂移的真相)。
        total_amount = settle_mod.validate_auction_settle(
            match_id=match_id,
            seller_id=seller_id,
            buyer_id=buyer_id,
            sell_order_id=sell_order_id,
            buy_order_id=buy_order_id,
            item_config_id=item_config_id,
            quantity=quantity,
            kind=kind,
            unit_price=unit_price,
        )
        idempotency_key = settle_mod.auction_settle_key(match_id)
        detail = settle_mod.auction_settle_detail(match_id, item_config_id, quantity, total_amount)
        already = await self._repo.settle_auction_match(
            match_id,
            seller_id,
            buyer_id,
            sell_order_id,
            buy_order_id,
            item_config_id,
            quantity,
            kind,
            total_amount,
            idempotency_key,
            detail,
        )
        if already:
            plog.get().info(
                "auction_settle_idempotent_hit",
                match_id=match_id,
                seller_id=seller_id,
                buyer_id=buyer_id,
                item=item_config_id,
                qty=quantity,
                gold=total_amount,
            )

    async def settle_player_trade(
        self,
        order_id: int,
        seller_id: int,
        buyer_id: int,
        seller_items: list[ItemGrant],
        buyer_items: list[ItemGrant],
        kind: int,
        price: int,
    ) -> None:
        """原子结算一笔玩家间点对点交易(幂等键基于 order_id)。

        与拍卖不同:P2P 无预冻结,任一方资产不足 → ErrInventoryInsufficient,整笔回滚。
        """
        settle_mod.validate_player_trade_settle(
            order_id=order_id,
            seller_id=seller_id,
            buyer_id=buyer_id,
            seller_items=seller_items,
            buyer_items=buyer_items,
            kind=kind,
            price=price,
        )
        idempotency_key = settle_mod.trade_settle_key(order_id)
        detail = (
            f"trade settle order={order_id} seller_items={len(seller_items)} "
            f"buyer_items={len(buyer_items)} price={price}"
        )
        already = await self._repo.settle_player_trade(
            order_id,
            seller_id,
            buyer_id,
            seller_items,
            buyer_items,
            kind,
            price,
            idempotency_key,
            detail,
        )
        if already:
            plog.get().info(
                "trade_settle_idempotent_hit",
                order_id=order_id,
                seller_id=seller_id,
                buyer_id=buyer_id,
                price=price,
            )

    # ── 拍卖托管三件套 ────────────────────────────────────────────────────

    async def freeze_for_order(
        self,
        player_id: int,
        order_id: int,
        side: int,
        item_config_id: int,
        quantity: int,
        currency_kind: int,
        unit_price: int,
    ) -> None:
        """挂单冻结资产。SELL 冻道具,BUY 冻 quantity×unit_price 个 currency_kind 货币。"""
        if player_id == 0 or order_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id / order_id required")
        if item_config_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "item_config_id required")
        if quantity <= 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "quantity must be positive")
        # unit_price 在协议里已是 uint64,`== 0` 即完整校验;Python 无类型保护,
        # 仍显式写 `<= 0` 拒负数(负单价 × 正数量 = 负总价,会把扣钱变成加钱)。
        if unit_price <= 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "unit_price must be positive")
        frozen_amount = 0
        if side == ESCROW_SIDE_SELL:
            kind = EscrowKind.ITEM
        elif side == ESCROW_SIDE_BUY:
            kind = EscrowKind.CURRENCY
            ccy.validate_currency_kind(currency_kind)
            frozen_amount, ok = ccy.safe_mul_currency(unit_price, quantity)
            if not ok or frozen_amount == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "freeze amount overflow order=%d price=%d qty=%d",
                    order_id,
                    unit_price,
                    quantity,
                )
        else:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "unknown escrow side %d", side)
        already = await self._repo.freeze_for_order(
            player_id, order_id, kind, item_config_id, quantity, currency_kind, frozen_amount
        )
        if already:
            plog.get().info(
                "auction_freeze_idempotent_hit",
                player_id=player_id,
                order_id=order_id,
                side=side,
                item=item_config_id,
                qty=quantity,
            )

    async def ensure_auction_escrow(
        self,
        player_id: int,
        order_id: int,
        side: int,
        item_config_id: int,
        remaining_quantity: int,
        currency_kind: int,
        unit_price: int,
    ) -> None:
        """为旧版本已进入订单状态机、但可能没成功冻结资产的订单补齐 escrow。

        ★ 数量仍走**有符号** int64(CLAUDE.md §5.12 例外:quantity 一族参与减法,
          不随货币一起改无符号),所以这里只钳数量,**不再钳价格** ——
          价格从入口到落库全程无符号语义,没有任何一处强转回 int64。
          旧实现同时钳了两者;若只删闸不删强转,超过 MaxInt64 的价格会被转成负数,
          一路穿到扣款处让 `have < n` 恒 false,把扣钱变成加钱(测绘报告 R5)。
        """
        if player_id == 0 or order_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id / order_id required")
        if item_config_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "item_config_id required")
        if remaining_quantity <= 0 or unit_price <= 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "remaining_quantity / unit_price must be positive"
            )
        max_int64 = (1 << 63) - 1
        if remaining_quantity > max_int64:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "ensure escrow quantity exceeds int64 order=%d remaining=%d",
                order_id,
                remaining_quantity,
            )
        if side == ESCROW_SIDE_SELL:
            kind = EscrowKind.ITEM
        elif side == ESCROW_SIDE_BUY:
            kind = EscrowKind.CURRENCY
            ccy.validate_currency_kind(currency_kind)
            _total, ok = ccy.safe_mul_currency(unit_price, remaining_quantity)
            if not ok:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "ensure escrow amount overflow order=%d price=%d remaining=%d",
                    order_id,
                    unit_price,
                    remaining_quantity,
                )
        else:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "unknown escrow side %d", side)
        already = await self._repo.ensure_auction_escrow(
            player_id,
            order_id,
            kind,
            item_config_id,
            remaining_quantity,
            currency_kind,
            unit_price,
        )
        if already:
            plog.get().info(
                "auction_ensure_escrow_idempotent_hit",
                player_id=player_id,
                order_id=order_id,
                side=side,
                item=item_config_id,
                remaining=remaining_quantity,
            )

    async def release_escrow(self, player_id: int, order_id: int) -> None:
        """退还某挂单 escrow 残余资产到活跃余额(撤单 / 过期 / 完全成交后)。"""
        if player_id == 0 or order_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id / order_id required")
        already = await self._repo.release_escrow(player_id, order_id)
        if already:
            plog.get().info("auction_release_noop", player_id=player_id, order_id=order_id)

    # ── 邮件 transfer 附件实例托管 ────────────────────────────────────────

    async def escrow_out_instances(
        self, source_player_id: int, to_player_id: int, instance_ids, escrow_key: str
    ) -> list[EscrowedInstance]:  # noqa: ANN001
        """从源玩家扣出实例并托管(发 transfer 邮件前的 saga 第一步)。

        source==to 合法(活动补发 / 切代 salvage 把玩家自己的已鉴定物经邮件送回)。
        """
        instance_ids = list(instance_ids)
        if source_player_id == 0 or to_player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "source/to player_id required")
        _validate_transfer_ids(instance_ids)
        _validate_idem_key(escrow_key)
        detail = f"escrow_out to={to_player_id} n={len(instance_ids)}"
        rows, already = await self._repo.escrow_out_instances(
            source_player_id, to_player_id, instance_ids, escrow_key, detail
        )
        if already:
            plog.get().info(
                "escrow_out_idempotent_hit",
                source_player_id=source_player_id,
                escrow_key=escrow_key,
                count=len(rows),
            )
        return rows

    async def claim_transfer_instances(
        self, to_player_id: int, items: list[TransferClaimItem], idempotency_key: str
    ) -> None:
        """领取托管实例(mail ClaimMail 专用)。"""
        if to_player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "to_player_id required")
        ids: list[int] = []
        for it in items:
            if it.item_config_id == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "item_config_id required instance=%d", it.instance_id
                )
            ids.append(it.instance_id)
        _validate_transfer_ids(ids)
        _validate_idem_key(idempotency_key)
        detail = f"transfer_claim n={len(items)}"
        already = await self._repo.claim_transfer_instances(
            to_player_id, items, self._cfg.capacity, idempotency_key, detail
        )
        if already:
            plog.get().info(
                "transfer_claim_idempotent_hit",
                to_player_id=to_player_id,
                idempotency_key=idempotency_key,
                count=len(items),
            )

    async def release_transfer_escrow(self, instance_ids) -> None:  # noqa: ANN001
        """托管释放回源玩家(发信 saga 失败补偿;行缺失 no-op 幂等)。"""
        instance_ids = list(instance_ids)
        _validate_transfer_ids(instance_ids)
        released = await self._repo.release_transfer_escrow(instance_ids)
        if released != len(instance_ids):
            # 缺行 = 已被领取或已释放,no-op 属预期;打观测日志便于审计核对,不算错。
            plog.get().info(
                "release_transfer_escrow_partial",
                requested=len(instance_ids),
                released=released,
            )

    async def consume_transfer_escrow(self, to_player_id: int, instance_ids) -> None:  # noqa: ANN001
        """消托管行不物化(bag phase 2 DS 领取链;mail.MarkMailClaimed 调)。"""
        instance_ids = list(instance_ids)
        if to_player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "to_player_id required")
        _validate_transfer_ids(instance_ids)
        consumed = await self._repo.consume_transfer_escrow(to_player_id, instance_ids)
        if consumed != len(instance_ids):
            # 缺行 = 重放已消费,幂等预期;观测日志留审计线索。
            plog.get().info(
                "consume_transfer_escrow_partial",
                to_player_id=to_player_id,
                requested=len(instance_ids),
                consumed=consumed,
            )

    # ── 保留期清理 ────────────────────────────────────────────────────────

    async def sweep_retention(self) -> None:
        """跑一轮保留期清理,每表至多一批(cfg.sweep_batch)。

        ★ 任一表失败**只记日志继续下一表**:清理彼此独立、幂等,下一轮自然重试。
          让第一张表的失败中断整轮,会导致后面的表永远不被清理而没人发现。

        report-only 下的"待清理量"告警由 dbguard.sweep_table 统一打(WARN + pending),
        这里只在真删发生时补一条业务侧 INFO。
        """
        log = plog.get()
        mode = self._cfg.retention_mode_parsed()
        try:
            out = await self._repo.sweep_ledger_before(
                mode, self._cfg.ledger_retention_days, self._cfg.sweep_batch
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("inventory_sweep_ledger_failed", err=str(exc))
        else:
            if out.deleted:
                log.info(
                    "inventory_sweep_ledger",
                    deleted=out.deleted,
                    retention_days=self._cfg.ledger_retention_days,
                )
        try:
            out = await self._repo.sweep_closed_escrow_before(
                mode, self._cfg.escrow_retention_days, self._cfg.sweep_batch
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("inventory_sweep_escrow_failed", err=str(exc))
        else:
            if out.deleted:
                log.info(
                    "inventory_sweep_escrow",
                    deleted=out.deleted,
                    retention_days=self._cfg.escrow_retention_days,
                )


# ── escrow 方向(对齐 proto EscrowSide:SELL=1 / BUY=2)────────────────────
#
# 数值取自 proto,不手抄:service 层直接把 request.side 传进来。
ESCROW_SIDE_SELL = 1
ESCROW_SIDE_BUY = 2


def _validate_transfer_ids(instance_ids: list[int]) -> None:
    """校验实例 ID 列表形状(非空 / 上限 / 无零值 / 无重复)。

    重复 ID 必须拒:同一行会被搬两次 —— 第二次搬的是"已经不在源表"的行,
    表现为静默少搬一件,而调用方看到的是成功。
    """
    if not instance_ids:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "instance_ids required")
    if len(instance_ids) > MAX_TRANSFER_BATCH:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg,
            "instance_ids %d exceed max %d",
            len(instance_ids),
            MAX_TRANSFER_BATCH,
        )
    seen: set[int] = set()
    for iid in instance_ids:
        if iid == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "instance_id required")
        if iid in seen:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "duplicate instance_id %d", iid)
        seen.add(iid)


def _validate_idem_key(key: str) -> None:
    """校验幂等键形状(非空且不超 inventory_ledger 列宽 64)。见 MAX_IDEMPOTENCY_KEY_LEN。"""
    if not key or len(key) > MAX_IDEMPOTENCY_KEY_LEN:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "invalid idempotency key")


__all__ = [
    "ESCROW_SIDE_BUY",
    "ESCROW_SIDE_SELL",
    "InstanceOwnershipQuery",
    "InventoryUsecase",
    "ItemGrant",
    "TransferClaimItem",
    "auction_leg_key",
]
