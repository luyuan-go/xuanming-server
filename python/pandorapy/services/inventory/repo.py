"""inventory 数据层(MySQL 货币 / 道具 / 幂等流水 / 拍卖托管)。

对应 Go 侧 internal/data/inventory_repo.go。库表(pandora_trade):

    player_wallet         玩家多币种钱包(PK player_id+currency_kind;见 currency.py)
    player_items          背包道具堆叠(uk player_id+item_config_id)
    player_item_instance  装备实例(见 repo_instance.py)
    inventory_ledger      发放 / 使用 / 出售 / 购买 / 结算幂等流水(uk player_id+idempotency_key)
    auction_escrow        挂单托管(uk player_id+order_id)
    mail_transfer_escrow  邮件附件实例托管(见 repo_transfer.py)

反作弊 / 一致性(不变量 §9.7):GrantItems / UseItem / SellItem / PurchaseShopItem /
两种结算全部在一个事务里先 INSERT inventory_ledger(命中 uk → 幂等已处理),再原子改
player_items / player_wallet;扣减用 `SELECT ... FOR UPDATE` 锁行 + 数量校验,
避免并发超扣。

★ 钱包是 `player_wallet` 而**不是** legacy 的 `player_currency`(单列 gold,
  只读待 contract 删除;新装库根本不建它)。货币的加减一律走 currency.py 的
  add/deduct_currency_tx,SQL 里绝不出现 `amount = amount ± %s` —— 理由见 currency.py 头注释。

★ 全服务**没有一行 Redis Lua**。原子性只有三样东西撑着:
  MySQL 本地事务、FOR UPDATE 行锁、唯一键 1062。所以本文件里真正要命的是
  **锁序**和 **1062 语义**(见 repo_sql.py 头注释),两者都不写在类型里、
  写错也不报错,只在并发压力下表现成死锁或超扣。
"""

from __future__ import annotations

from pandorapy import dbguard
from pandorapy import errcode
from pandorapy import mysqlx
from pandorapy.services.inventory import bag_migration as bmig
from pandorapy.services.inventory import currency as ccy
from pandorapy.services.inventory import fingerprint as fp
from pandorapy.services.inventory import repo_sql as rsql
from pandorapy.services.inventory.models import (
    ESCROW_STATUS_ACTIVE,
    ESCROW_STATUS_CLOSED,
    EscrowKind,
    ItemGrant,
    ItemStack,
    PurchaseOutcome,
    PurchaseRequest,
    SaleOutcome,
)
from pandorapy.services.inventory.repo_instance import (
    InstanceRepoMixin,
    grant_instances_in_tx_mixed,
    select_instances_by_ids_tx,
)
from pandorapy.services.inventory.repo_transfer import TransferRepoMixin

TRADE_DB = "pandora_trade"

# EnsureAuctionEscrow 的重试次数(与 Go 的 ensureAuctionEscrowMaxAttempts 同值)。
ENSURE_AUCTION_ESCROW_MAX_ATTEMPTS = 3

# 背包容量购买在 inventory_ledger 里的 op 标识(与 Go 的 bagCapacityChargeOp 同字面量)。
BAG_CAPACITY_CHARGE_OP = "buy_capacity"


def bag_capacity_charge_key(bag_type: int, tier: int) -> str:
    """购买扣费幂等键。对应 Go 的 BagCapacityChargeKey。

    每玩家 uk(player_id, idempotency_key) 下按 段×档 唯一 —— 与 Go **逐字节一致**,
    分叉即等于跨栈重试会重复扣费。
    """
    return f"bagcap:{bag_type}:{tier}"


_MAX_UINT64 = (1 << 64) - 1


def safe_mul_positive_uint64(a: int, b: int) -> tuple[int, bool]:
    """"两数都必须为正"的溢出安全乘法(0 也返回 False)。对应 Go 的 safeMulPositiveUint64。

    ★ 天花板取 2^64-1 而**不是** currency.MAX_CURRENCY_AMOUNT:本函数只判"乘法本身
      会不会溢出",业务上限由调用侧的 MAX_CURRENCY_AMOUNT 闸负责。两者混在一起会让
      (2^62, 2^64) 区间的中间量被误报成溢出,错误码也会从"金额超限"变成"参数非法",
      排障时指错方向。

    ★ Python 的 int 不会溢出,所以这道闸必须显式写:算得出来 ≠ 存得下,
      写进 BIGINT 列时严格模式报错、非严格模式**静默截断**(§9.24)。
    """
    if a <= 0 or b <= 0 or a > _MAX_UINT64 // b:
        return 0, False
    return a * b, True


async def _consume_item_escrow_tx(
    cur, player_id: int, order_id: int, item_config_id: int, qty: int
) -> None:  # noqa: ANN001
    """锁卖单道具 escrow 并消费 qty(成交交付)。

    escrow 不存在 / 非 item / 余量不足 → 错误(正常流程不应发生,
    escrow 充足由 FreezeForOrder 阶段保证)。
    """
    try:
        await cur.execute(
            "SELECT kind, item_config_id, frozen_qty, status FROM auction_escrow "
            "WHERE player_id = %s AND order_id = %s FOR UPDATE",
            (player_id, order_id),
        )
        row = await cur.fetchone()
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "lock item escrow player=%d order=%d: %s",
            player_id,
            order_id,
            exc,
        ) from exc
    if row is None:
        raise errcode.PandoraError(
            errcode.ErrInventoryInsufficient,
            "item escrow not found player=%d order=%d",
            player_id,
            order_id,
        )
    kind, item_id, frozen_qty, status = int(row[0]), int(row[1]), int(row[2]), int(row[3])
    if kind != EscrowKind.ITEM or item_id != item_config_id:
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "escrow kind/item mismatch player=%d order=%d kind=%d item=%d want item=%d",
            player_id,
            order_id,
            kind,
            item_id,
            item_config_id,
        )
    if status == ESCROW_STATUS_CLOSED or frozen_qty < qty:
        raise errcode.PandoraError(
            errcode.ErrInventoryInsufficient,
            "item escrow short player=%d order=%d frozen=%d need=%d",
            player_id,
            order_id,
            frozen_qty,
            qty,
        )
    try:
        await cur.execute(
            "UPDATE auction_escrow SET frozen_qty = frozen_qty - %s "
            "WHERE player_id = %s AND order_id = %s",
            (qty, player_id, order_id),
        )
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "consume item escrow player=%d order=%d: %s",
            player_id,
            order_id,
            exc,
        ) from exc


async def _consume_currency_escrow_tx(  # noqa: ANN001
    cur, player_id: int, order_id: int, want_kind: int, amount: int
) -> None:
    """锁买单货币 escrow 并消费 amount(成交付款)。对应 Go 的 consumeCurrencyEscrowTx。

    ★ 币种必须与本次结算一致:托管行冻的是哪种货币,就只能用来付哪种货币的账。
      静默按托管行的币种结算 = 用玩家冻的钻石去付一笔金币订单,不可观测的经济事故。
    """
    try:
        await cur.execute(
            "SELECT kind, frozen_amount, currency_kind, status FROM auction_escrow "
            "WHERE player_id = %s AND order_id = %s FOR UPDATE",
            (player_id, order_id),
        )
        row = await cur.fetchone()
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "lock currency escrow player=%d order=%d: %s",
            player_id,
            order_id,
            exc,
        ) from exc
    if row is None:
        raise errcode.PandoraError(
            errcode.ErrInventoryInsufficient,
            "currency escrow not found player=%d order=%d",
            player_id,
            order_id,
        )
    kind, frozen_amount = int(row[0]), int(row[1])
    currency_kind, status = int(row[2]), int(row[3])
    if kind != EscrowKind.CURRENCY:
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "escrow kind mismatch player=%d order=%d kind=%d want currency",
            player_id,
            order_id,
            kind,
        )
    if currency_kind != int(want_kind):
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "escrow currency mismatch player=%d order=%d have=%d want=%d",
            player_id,
            order_id,
            currency_kind,
            int(want_kind),
        )
    if status == ESCROW_STATUS_CLOSED or frozen_amount < amount:
        raise errcode.PandoraError(
            errcode.ErrInventoryInsufficient,
            "currency escrow short player=%d order=%d frozen=%d need=%d",
            player_id,
            order_id,
            frozen_amount,
            amount,
        )
    try:
        # 这里的 `frozen_amount - %s` 是安全的:上面刚在 FOR UPDATE 锁内断言过
        # frozen_amount >= amount,且该列是**有符号** BIGINT(不是 UNSIGNED),
        # 即便断言被绕过也只会得到负数而非静默截断成 0。
        await cur.execute(
            "UPDATE auction_escrow SET frozen_amount = frozen_amount - %s "
            "WHERE player_id = %s AND order_id = %s",
            (amount, player_id, order_id),
        )
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "consume currency escrow player=%d order=%d: %s",
            player_id,
            order_id,
            exc,
        ) from exc


class MySQLInventoryRepo(InstanceRepoMixin, TransferRepoMixin, bmig.LegacyBagSourceMixin):
    """基于 asyncmy 连接池的 inventory 仓储。对应 Go 的 MySQLInventoryRepo。

    ★ 池必须以 **autocommit=True** 建(见 main.py):与 Go 的 database/sql 默认语义一致。
      需要原子性的地方全部显式 `conn.begin()`(rsql.transaction)包起来。
    """

    __slots__ = ("_pool", "_db")

    def __init__(self, pool, db: str = TRADE_DB) -> None:  # noqa: ANN001
        self._pool = pool
        # db 只被保留期清理用到(dbguard 的 SQL 要按 schema 限定表名)。
        #
        # ⚠️ 与 Go 的一处**已知差异**:Go 的 dbguard.SweepTable 把这个值只当
        # metric label,SQL 里写的是不带库名的裸表名(跟着连接当前库走);
        # Python 的 dbguard.sweep_table 会把它拼进 SQL。生产上两者等价
        # (DSN 的库名就是 pandora_trade),但连到别名库时行为不同。
        # 做成参数而不是硬编码,是为了让数据层测试能打自己的临时库 ——
        # 硬编码会让"清理到底删没删"这条只能在生产库上验证,等于没法验。
        self._db = db

    # ── 只读 ──────────────────────────────────────────────────────────────

    async def get_inventory(self, player_id: int) -> tuple[dict[int, int], list[ItemStack]]:
        """读玩家**全部币种余额** + 道具堆叠(按 item_config_id 排序;未建档 → 空/空)。

        ★ 只读 count > 0 的行。扣空的行会被删掉(见 repo_sql.deduct_item_tx),
          但历史数据里可能还留着 count=0 的死行 —— 把它们读出来会让客户端显示
          "持有 0 个 XX"。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            # 只读路径不开事务;read_balances_tx 里只有一条 SELECT,不依赖事务语义。
            balances = await ccy.read_balances_tx(cur, player_id)
            try:
                await cur.execute(
                    "SELECT item_config_id, count FROM player_items "
                    "WHERE player_id = %s AND count > 0 ORDER BY item_config_id",
                    (player_id,),
                )
                rows = await cur.fetchall()
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "query items player=%d: %s", player_id, exc
                ) from exc
        return balances, [ItemStack(item_config_id=int(r[0]), count=int(r[1])) for r in rows or ()]

    # ── 发放 / 扣减 ────────────────────────────────────────────────────────

    async def grant_items(
        self,
        player_id: int,
        items: list[ItemGrant],
        currencies,  # noqa: ANN001 —— {kind: amount}
        idempotency_key: str,
        detail: str,
    ) -> tuple[dict[int, int], bool]:
        """幂等发放道具 + 多币种货币。返回 (发放后全部币种余额, already)。"""
        fingerprint = fp.grant_fingerprint(
            ((it.item_config_id, it.count) for it in items), currencies
        )
        async with rsql.transaction(self._pool) as cur:
            already, snap = await rsql.claim_ledger(
                cur, player_id, idempotency_key, "grant", fingerprint, detail
            )
            if already:
                return snap.balances, True
            for it in items:
                await rsql.add_item_tx(cur, player_id, it.item_config_id, it.count)
            # 按 kind 升序逐币种加钱;每笔都过 MAX_CURRENCY_AMOUNT 上限闸
            # (溢出显式报错,不回绕)。零/空 currencies 时只做一次全量读回。
            new_balances = await ccy.apply_currency_deltas_tx(cur, player_id, currencies)
            await rsql.update_ledger_result(
                cur, player_id, idempotency_key, 0, new_balances, currencies
            )
            return new_balances, False

    # ── 背包容量购买扣费(bag-domain.md §5.3 两步 saga 第①步)────────────────

    async def charge_bag_capacity(
        self, player_id: int, bag_type: int, tier: int, slots: int, kind: int, price: int
    ) -> tuple[bool, int]:
        """购买扣费(trade 库单事务)。对应 Go 的 MySQLInventoryRepo.ChargeBagCapacity。

        首次 → 扣 price 个 kind 货币并记账;同 key 重试(already=True)→ **零扣费**,
        返回首次那一刻的余额快照(不是当前余额:玩家之后花了钱,重放不能显示新余额,
        否则同一笔购买两次响应给出两个"成功但不同"的答案)。

        幂等身份 = (player_id, bag_type, 第 tier 档),与 bag 库的档数 CAS 共用 ——
        两步之间崩溃时重试同 tier:①回放零扣费 → ②补应用,不存在"扣钱未到账"终态。

        ★ key / op / 指纹三者与 Go **逐字节一致**:它们是跨栈幂等的全部依据。
          Go 版扣过一次、Python 版重试时算出别的 key,就会**再扣一次钱**。
          指纹在金币时沿用旧格式(见 fingerprint.bag_capacity_charge_fingerprint);
          detail 也保持 `gold=%d` 不变,理由同 sell(存量行靠它识别)。
        """
        ccy.validate_currency_kind(kind)
        key = bag_capacity_charge_key(bag_type, tier)
        fingerprint = fp.bag_capacity_charge_fingerprint(
            player_id, bag_type, tier, slots, price, kind
        )
        detail = f"buy capacity bag={bag_type} tier={tier} slots={slots} gold={price}"
        async with rsql.transaction(self._pool) as cur:
            already, snap = await rsql.claim_ledger(
                cur, player_id, key, BAG_CAPACITY_CHARGE_OP, fingerprint, detail
            )
            if already:
                return True, ccy.balances_get(snap.balances, kind)
            remaining = await ccy.deduct_currency_tx(cur, player_id, kind, price)
            balances = await ccy.read_balances_tx(cur, player_id)
            # result_remaining 记的是本次买到的格子数(Go 同):它是审计时"这笔钱换了什么"
            # 的唯一凭据 —— 光有余额看不出买的是哪一档。
            await rsql.update_ledger_result(
                cur, player_id, key, slots, balances, {kind: price}
            )
            return False, remaining

    async def _deduct_with_ledger(
        self,
        player_id: int,
        item_config_id: int,
        count: int,
        idempotency_key: str,
        detail: str,
        *,
        op: str,
        fingerprint: str,
    ) -> tuple[int, bool]:
        """「记流水 → 锁行扣减 → 回写结果快照」这条骨架。

        UseItem / DiscardItem / ConsumeBattleItem / DiscardBattleItem 走的是同一条,
        **区别只有 op 与指纹** —— 而这两者恰恰是审计语义与防串改的关键:
        op 让审计能区分"用掉了"和"扔掉了",指纹让同 key 不同意图被拒。
        所以它们是参数,不是可以省掉的细节。
        """
        async with rsql.transaction(self._pool) as cur:
            already, snap = await rsql.claim_ledger(
                cur, player_id, idempotency_key, op, fingerprint, detail
            )
            if already:
                return snap.remaining, True
            remaining = await rsql.deduct_item_tx(cur, player_id, item_config_id, count)
            await rsql.update_ledger_result(cur, player_id, idempotency_key, remaining, None, None)
            return remaining, False

    async def use_item(
        self, player_id: int, item_config_id: int, count: int, idempotency_key: str, detail: str
    ) -> tuple[int, bool]:
        return await self._deduct_with_ledger(
            player_id,
            item_config_id,
            count,
            idempotency_key,
            detail,
            op="use",
            fingerprint=fp.use_fingerprint(item_config_id, count),
        )

    async def discard_item(
        self, player_id: int, item_config_id: int, count: int, idempotency_key: str, detail: str
    ) -> tuple[int, bool]:
        return await self._deduct_with_ledger(
            player_id,
            item_config_id,
            count,
            idempotency_key,
            detail,
            op="discard",
            fingerprint=fp.discard_fingerprint(item_config_id, count),
        )

    async def consume_battle_item(
        self, player_id: int, item_config_id: int, count: int, idempotency_key: str, detail: str
    ) -> tuple[int, bool]:
        return await self._deduct_with_ledger(
            player_id,
            item_config_id,
            count,
            idempotency_key,
            detail,
            op="battle_consume",
            fingerprint=fp.battle_consume_fingerprint(item_config_id, count),
        )

    async def discard_battle_item(
        self, player_id: int, item_config_id: int, count: int, idempotency_key: str, detail: str
    ) -> tuple[int, bool]:
        return await self._deduct_with_ledger(
            player_id,
            item_config_id,
            count,
            idempotency_key,
            detail,
            op="battle_discard",
            fingerprint=fp.battle_discard_fingerprint(item_config_id, count),
        )

    async def sell_item(
        self,
        player_id: int,
        item_config_id: int,
        count: int,
        kind: int,
        amount: int,
        idempotency_key: str,
        detail: str,
    ) -> tuple[SaleOutcome, bool]:
        """幂等出售。返回 (SaleOutcome, already)。

        amount == 0 的首次请求在 claim 之后才拒(ErrInventoryNotSellable),整笔回滚 ——
        连 claim 行一起没有,不会留下脏流水把这个 key 永久占死。
        """
        async with rsql.transaction(self._pool) as cur:
            already, snap = await rsql.claim_sale_ledger(
                cur,
                player_id,
                idempotency_key,
                fp.sell_fingerprint(item_config_id, count),
                detail,
                op="sell",
                item_config_id=item_config_id,
                count=count,
            )
            if already:
                # 幂等命中:回放首次执行的剩余数量 + 余额 + 本次入账额。
                return (
                    SaleOutcome(
                        remaining=snap.remaining,
                        balances=snap.balances,
                        earned=ccy.balances_get(snap.delta, kind),
                        kind=kind,
                    ),
                    True,
                )
            # amount == 0 = 该道具不可出售(配置缺失 / 售价为 0)。
            #
            # ★ 判据刻意是 `== 0` 而不是旧的 `<= 0`:Go 侧 amount 已是 uint64,
            #   `<= 0` 在那边退化成 `== 0`。Python 没有类型保护,所以负数在**上游**
            #   (biz.sell_item 的 safe_mul_currency)就被拒掉;走到这里只可能是 0。
            #   写 `<= 0` 会让人以为这里还在防负数,而真正的防线不在这。
            if amount == 0:
                raise errcode.PandoraError(
                    errcode.ErrInventoryNotSellable,
                    "item not sellable player=%d item=%d",
                    player_id,
                    item_config_id,
                )
            remaining = await rsql.deduct_item_tx(cur, player_id, item_config_id, count)
            await ccy.add_currency_tx(cur, player_id, kind, amount)
            new_balances = await ccy.read_balances_tx(cur, player_id)
            await rsql.update_ledger_result(
                cur, player_id, idempotency_key, remaining, new_balances, {kind: amount}
            )
            return (
                SaleOutcome(
                    remaining=remaining, balances=new_balances, earned=amount, kind=kind
                ),
                False,
            )

    # ── NPC 商店购买(2026-08-22)────────────────────────────────────────────

    async def purchase_shop_item(
        self, player_id: int, req: PurchaseRequest
    ) -> tuple[PurchaseOutcome, bool]:
        """一个本地事务里完成扣费 + 入包 + 记账。对应 Go 的 PurchaseShopItem。

        ★ 为什么装备也走这里而不是复用 grant_instances:grant_instances 是**系统发放**
          接口,自己开事务、自己写 ledger。若购买先扣钱再调它,扣钱与发货就落在两个
          事务里 —— 中间崩溃会出现"钱扣了货没到"且无补偿。购买必须是一个事务,
          所以在这里内联实例分配(复用同一套 lock_player_instances / lowest_free_slot,
          不另起一套格子分配规则)。
        """
        ccy.validate_currency_kind(req.kind)
        if req.total_cost <= 0:
            # 商店表要求单价 > 0(shop.proto),份数也已在 biz 校验 > 0,所以总价为 0
            # 只可能是上游算错。免费发放请走活动 / 邮件,不要借商店链绕过发放审计。
            #
            # Go 侧 TotalCost 是 uint64,那边写 `== 0` 就是完整校验;这里写 `<= 0`
            # 是因为 Python 没有类型保护 —— 扣钱这一步的入口不该把"负总价"
            # 交给下游某一层去兜。
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "purchase total cost must be positive player=%d shop=%d item=%d",
                player_id,
                req.shop_id,
                req.item_config_id,
            )

        fingerprint = fp.purchase_fingerprint(req.shop_id, req.item_config_id, req.unit_count)
        detail = fp.purchase_detail(
            req.shop_id, req.item_config_id, req.unit_count, req.total_items, req.instance_ids
        )

        async with rsql.transaction(self._pool) as cur:
            already, snap, stored_detail = await self._claim_purchase_ledger(
                cur, player_id, req.idempotency_key, fingerprint, detail
            )
            if already:
                return await self._replay_purchase(cur, player_id, req, snap, stored_detail), True

            # ① 扣钱。放在入包之前:余额不足是最常见的失败,先扣可以少做无用功;
            #    且两者同事务,顺序不影响原子性。
            await ccy.deduct_currency_tx(cur, player_id, req.kind, req.total_cost)

            # ② 入包。
            out_items: list[ItemGrant] = []
            out_instances = []
            if req.is_equipment:
                out_instances = await grant_instances_in_tx_mixed(
                    cur,
                    player_id,
                    req.instance_ids,
                    [req.item_config_id] * len(req.instance_ids),
                    req.capacity,
                )
            else:
                if req.total_items <= 0:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "purchase item count must be positive player=%d item=%d",
                        player_id,
                        req.item_config_id,
                    )
                await rsql.add_item_tx(cur, player_id, req.item_config_id, req.total_items)
                out_items = [
                    ItemGrant(item_config_id=req.item_config_id, count=req.total_items)
                ]

            # ③ 结果快照落流水(余额 + 本次扣费额),供响应丢失后的重放返回同样的值。
            new_balances = await ccy.read_balances_tx(cur, player_id)
            await rsql.update_ledger_result(
                cur,
                player_id,
                req.idempotency_key,
                0,
                new_balances,
                {req.kind: req.total_cost},
            )
            return (
                PurchaseOutcome(
                    balances=new_balances,
                    cost=req.total_cost,
                    kind=req.kind,
                    items=out_items,
                    instances=out_instances,
                ),
                False,
            )

    @staticmethod
    async def _claim_purchase_ledger(  # noqa: ANN001
        cur, player_id: int, idempotency_key: str, fingerprint: str, detail: str
    ) -> tuple[bool, object, str]:
        """声明购买幂等键,并在命中时返回首次执行的结果快照与 detail。

        对应 Go 的 claimPurchaseLedger。**不能复用 claim_ledger**:回放要还原
        "发了什么",而那些事实存在 detail 里,claim_ledger 不返回 detail。
        """
        try:
            await cur.execute(
                "INSERT INTO inventory_ledger "
                "(player_id, idempotency_key, op, request_fingerprint, detail) "
                "VALUES (%s, %s, 'shop_buy', %s, %s)",
                (player_id, idempotency_key, fingerprint, detail),
            )
        except Exception as exc:  # noqa: BLE001
            if not mysqlx.is_duplicate_entry(exc):
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "insert purchase ledger player=%d key=%s: %s",
                    player_id,
                    idempotency_key,
                    exc,
                ) from exc
        else:
            return False, None, ""

        await cur.execute(
            f"SELECT op, request_fingerprint, detail, {rsql.LEDGER_RESULT_COLUMNS} "
            "FROM inventory_ledger WHERE player_id = %s AND idempotency_key = %s LIMIT 1",
            (player_id, idempotency_key),
        )
        row = await cur.fetchone()
        if row is None:
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "read purchase ledger player=%d key=%s: row vanished after duplicate",
                player_id,
                idempotency_key,
            )
        stored_op, stored_fp, stored_detail = str(row[0]), str(row[1]), str(row[2] or "")
        # op 也要核:同一个 key 被出售 / 发放用过再拿来购买,是明确的 key 复用。
        if stored_op != "shop_buy" or stored_fp != fingerprint:
            raise errcode.PandoraError(
                errcode.ErrInventoryIdempotencyConflict,
                "idempotency_key reused for different purchase player=%d key=%s",
                player_id,
                idempotency_key,
            )
        snap = rsql.scan_ledger_result(int(row[3] or 0), int(row[4] or 0), row[5], row[6])
        return True, snap, stored_detail

    @staticmethod
    async def _replay_purchase(  # noqa: ANN001
        cur, player_id: int, req: PurchaseRequest, snap, stored_detail: str
    ) -> PurchaseOutcome:
        """用首次执行落下的事实重建响应,**不重新扣费也不重新发货**。"""
        parsed = fp.parse_purchase_detail(stored_detail)
        if parsed is None:
            # 解析不出首次发货事实就 fail-closed:宁可让客户端看到内部错并由人排查,
            # 也不能编一个"发了什么"回去(那会让 UI 显示玩家其实没拿到的东西)。
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "purchase ledger detail unparsable player=%d key=%s",
                player_id,
                req.idempotency_key,
            )
        total_items, instance_ids = parsed
        out = PurchaseOutcome(
            balances=snap.balances,
            cost=ccy.balances_get(snap.delta, req.kind),
            kind=req.kind,
        )
        if instance_ids:
            out.instances = await select_instances_by_ids_tx(cur, player_id, instance_ids)
        if total_items > 0:
            out.items = [ItemGrant(item_config_id=req.item_config_id, count=total_items)]
        return out

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
        total_amount: int,
        idempotency_key: str,
        detail: str,
    ) -> bool:
        """一个本地事务里从双方 escrow 消费完成拍卖成交的卖↔买对转。返回 already。

        因卖家道具与买家货币已在 FreezeForOrder 冻结进 escrow,本步只做
        「消费 escrow + 入账对手」,不再触活跃余额扣减,故成交不会因余额不足失败。

        ★ 防死锁:对 escrow / player_items / player_wallet 的行锁全部按
          **player_id 升序**、同一玩家内「先 escrow 后入账」的总顺序获取。
          不这么做的话,两笔买卖角色对调的并发成交会以相反顺序锁同一批行 → 1213。
        """
        fingerprint = fp.auction_settle_fingerprint(
            seller_id, buyer_id, item_config_id, quantity, kind, total_amount
        )
        async with rsql.transaction(self._pool) as cur:
            # 1) 幂等流水:按 player_id 升序声明两条(同 key),避免并发交叉插入死锁。
            lo_id, hi_id = seller_id, buyer_id
            lo_op, hi_op = "auction_sell", "auction_buy"
            if buyer_id < seller_id:
                lo_id, hi_id = buyer_id, seller_id
                lo_op, hi_op = "auction_buy", "auction_sell"
            lo_already, _lo_snap = await rsql.claim_ledger(
                cur, lo_id, idempotency_key, lo_op, fingerprint, detail
            )
            hi_already, _hi_snap = await rsql.claim_ledger(
                cur, hi_id, idempotency_key, hi_op, fingerprint, detail
            )
            if lo_already or hi_already:
                # 已结算过(双方流水原子写入,正常下同真同假;
                # 异常单边脏数据也按已处理回滚,防双扣)。
                return True

            async def seller_leg() -> None:
                await _consume_item_escrow_tx(
                    cur, seller_id, sell_order_id, item_config_id, quantity
                )
                await ccy.add_currency_tx(cur, seller_id, kind, total_amount)

            async def buyer_leg() -> None:
                await _consume_currency_escrow_tx(
                    cur, buyer_id, buy_order_id, kind, total_amount
                )
                await rsql.add_item_tx(cur, buyer_id, item_config_id, quantity)

            first, second = seller_leg, buyer_leg
            if buyer_id < seller_id:
                first, second = buyer_leg, seller_leg
            await first()
            await second()
            return False

    async def settle_player_trade(
        self,
        order_id: int,
        seller_id: int,
        buyer_id: int,
        seller_items: list[ItemGrant],
        buyer_items: list[ItemGrant],
        kind: int,
        price: int,
        idempotency_key: str,
        detail: str,
    ) -> bool:
        """一个本地事务里的玩家间点对点交易对转。返回 already。

        与拍卖不同:P2P **无 escrow 预冻结**,直接从双方活跃余额扣转,
        任一方道具 / 货币不足 → ErrInventoryInsufficient,整笔回滚(成交失败)。

        ★ 腿内锁序必须全局一致:同一玩家的道具行按 item_config_id 升序
          「扣/加合并成一趟」处理(同 ID 先扣后加),货币行统一放腿尾。
          若像旧实现那样「先扣完再加」,两笔方向对调的并发交易
          (A 卖 item1 换 item2 vs A 卖 item2 换 item1)会在同一玩家的行上
          以相反顺序加锁 → InnoDB 死锁(1213)。
        """
        fingerprint = fp.player_trade_settle_fingerprint(
            seller_id,
            buyer_id,
            ((it.item_config_id, it.count) for it in seller_items),
            ((it.item_config_id, it.count) for it in buyer_items),
            kind,
            price,
        )
        async with rsql.transaction(self._pool) as cur:
            lo_id, hi_id = seller_id, buyer_id
            lo_op, hi_op = "trade_sell", "trade_buy"
            if buyer_id < seller_id:
                lo_id, hi_id = buyer_id, seller_id
                lo_op, hi_op = "trade_buy", "trade_sell"
            lo_already, _lo_snap = await rsql.claim_ledger(
                cur, lo_id, idempotency_key, lo_op, fingerprint, detail
            )
            hi_already, _hi_snap = await rsql.claim_ledger(
                cur, hi_id, idempotency_key, hi_op, fingerprint, detail
            )
            if lo_already or hi_already:
                return True

            sorted_seller = sorted(seller_items, key=lambda it: it.item_config_id)
            sorted_buyer = sorted(buyer_items, key=lambda it: it.item_config_id)

            async def item_ops(
                player_id: int, deducts: list[ItemGrant], adds: list[ItemGrant]
            ) -> None:
                """把「扣 deducts + 加 adds」按 item_config_id 升序归并成单趟。

                同一 ID 同时出现在两边时**先扣后加**(保守:先过余额校验)。
                """
                di = ai = 0
                while di < len(deducts) or ai < len(adds):
                    take_deduct = ai >= len(adds) or (
                        di < len(deducts)
                        and deducts[di].item_config_id <= adds[ai].item_config_id
                    )
                    if take_deduct:
                        await rsql.deduct_item_tx(
                            cur, player_id, deducts[di].item_config_id, deducts[di].count
                        )
                        di += 1
                        continue
                    await rsql.add_item_tx(
                        cur, player_id, adds[ai].item_config_id, adds[ai].count
                    )
                    ai += 1

            async def seller_leg() -> None:
                await item_ops(seller_id, sorted_seller, sorted_buyer)
                if price > 0:
                    await ccy.add_currency_tx(cur, seller_id, kind, price)

            async def buyer_leg() -> None:
                await item_ops(buyer_id, sorted_buyer, sorted_seller)
                if price > 0:
                    await ccy.deduct_currency_tx(cur, buyer_id, kind, price)

            first, second = seller_leg, buyer_leg
            if buyer_id < seller_id:
                first, second = buyer_leg, seller_leg
            await first()
            await second()
            return False

    # ── 拍卖托管 ──────────────────────────────────────────────────────────

    async def freeze_for_order(
        self,
        player_id: int,
        order_id: int,
        kind: EscrowKind,
        item_config_id: int,
        quantity: int,
        currency_kind: int,
        frozen_amount: int,
    ) -> bool:
        """挂单冻结资产(把活跃余额移入 escrow)。返回 already。

        幂等键 = escrow 行的 uk(player, order):命中 → 已冻结,直接 already
        (资产已扣,绝不重复扣)。
        """
        frozen_qty = quantity if kind == EscrowKind.ITEM else 0
        async with rsql.transaction(self._pool) as cur:
            duplicate = False
            try:
                await cur.execute(
                    "INSERT INTO auction_escrow "
                    "(player_id, order_id, kind, item_config_id, frozen_qty, frozen_amount, "
                    "currency_kind, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        player_id,
                        order_id,
                        int(kind),
                        item_config_id,
                        frozen_qty,
                        frozen_amount,
                        int(currency_kind),
                        ESCROW_STATUS_ACTIVE,
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                if mysqlx.is_duplicate_entry(exc):
                    duplicate = True
                else:
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "insert escrow player=%d order=%d: %s",
                        player_id,
                        order_id,
                        exc,
                    ) from exc
            if duplicate:
                return True

            # 从活跃余额扣减(不足 → ErrInventoryInsufficient,整笔回滚含 escrow 行)。
            if kind == EscrowKind.ITEM:
                await rsql.deduct_item_tx(cur, player_id, item_config_id, quantity)
            elif kind == EscrowKind.CURRENCY:
                await ccy.deduct_currency_tx(cur, player_id, currency_kind, frozen_amount)
            else:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "unknown escrow kind %d", int(kind)
                )
            return False

    async def ensure_auction_escrow(
        self,
        player_id: int,
        order_id: int,
        kind: EscrowKind,
        item_config_id: int,
        remaining_quantity: int,
        currency_kind: int,
        unit_price: int,
    ) -> bool:
        """为旧版本遗留订单补齐足够的 active escrow。返回 already。

        ★ 这里是本服务对 1062 语义最讲究的一处,逐条照抄 Go:

          并发无行时直接争用 uk_player_order 的 INSERT:
            胜者在**同一事务**里扣活跃资产并提交;
            失败者收到 1062 后**必须先回滚**(释放本事务持有的一切锁),
            再以**新事务** SELECT ... FOR UPDATE 严格核对胜者提交的整行。

          把 1062 直接当"幂等成功"会怎样:两个事务都认为自己不必扣资产,
          或者更糟 —— 失败者在没回滚的情况下去读,读到的是自己那笔未提交的脏状态。
          所以 1062 只是"转入校验路径"的信号,绝不等价于成功。
        """
        # unit_price 在协议里已是 uint64：`== 0` 就是完整的“必须为正”校验。
        # Python 没有类型保护，所以这里仍显式写 `<= 0` 拒负数：
        # 负单价乘上正数量会得到负总价，一路穿到 deduct 处让 `have < n` 恒为假，
        # 把扣钱变成加钱（测绘报告 R5 同形洞）。
        if (
            player_id == 0
            or order_id == 0
            or item_config_id == 0
            or remaining_quantity <= 0
            or unit_price <= 0
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "invalid ensure escrow player=%d order=%d kind=%d item=%d remaining=%d price=%d",
                player_id,
                order_id,
                int(kind),
                item_config_id,
                remaining_quantity,
                unit_price,
            )
        if kind not in (EscrowKind.ITEM, EscrowKind.CURRENCY):
            raise errcode.PandoraError(errcode.ErrInvalidArg, "unknown escrow kind %d", int(kind))
        required_amount = 0
        if kind == EscrowKind.CURRENCY:
            ccy.validate_currency_kind(currency_kind)
            required_amount, ok = safe_mul_positive_uint64(remaining_quantity, unit_price)
            if not ok:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "ensure escrow amount overflow order=%d remaining=%d price=%d",
                    order_id,
                    remaining_quantity,
                    unit_price,
                )

        for _attempt in range(ENSURE_AUCTION_ESCROW_MAX_ATTEMPTS):
            created, duplicate = await self._try_create_auction_escrow(
                player_id,
                order_id,
                kind,
                item_config_id,
                remaining_quantity,
                currency_kind,
                required_amount,
            )
            if created:
                return False
            if not duplicate:
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "ensure escrow neither created nor duplicate player=%d order=%d",
                    player_id,
                    order_id,
                )
            found = await self._validate_existing_auction_escrow(
                player_id,
                order_id,
                kind,
                item_config_id,
                remaining_quantity,
                currency_kind,
                required_amount,
            )
            if found:
                return True
            # active 行正常流程从不 DELETE(保留期清理只删 closed 且超期的行)。
            # 走到这里说明外部清理恰好发生在 1062 与复查之间 —— 重新竞争 INSERT,
            # 但**绝不**把不可解释的消失当成功。
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "escrow disappeared after duplicate player=%d order=%d attempts=%d",
            player_id,
            order_id,
            ENSURE_AUCTION_ESCROW_MAX_ATTEMPTS,
        )

    async def _try_create_auction_escrow(
        self,
        player_id: int,
        order_id: int,
        kind: EscrowKind,
        item_config_id: int,
        remaining_quantity: int,
        currency_kind: int,
        required_amount: int,
    ) -> tuple[bool, bool]:
        """返回 (created, duplicate)。duplicate 分支**必定已回滚**(哨兵退出)。"""
        frozen_qty = remaining_quantity if kind == EscrowKind.ITEM else 0
        frozen_amount = 0 if kind == EscrowKind.ITEM else required_amount

        async with rsql.transaction_or_value(self._pool) as holder:
            cur = holder.cur
            try:
                await cur.execute(
                    "INSERT INTO auction_escrow "
                    "(player_id, order_id, kind, item_config_id, frozen_qty, frozen_amount, "
                    "currency_kind, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        player_id,
                        order_id,
                        int(kind),
                        item_config_id,
                        frozen_qty,
                        frozen_amount,
                        int(currency_kind),
                        ESCROW_STATUS_ACTIVE,
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                if mysqlx.is_duplicate_entry(exc):
                    # ★ 用哨兵退出让 transaction_or_value 走 ROLLBACK ——
                    #   必须在复查之前把本事务的锁全放掉,否则新事务的 FOR UPDATE
                    #   会等自己持有的锁,直到锁等待超时(1205)。
                    raise rsql.RollbackAndReturn((False, True)) from exc
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "insert ensured escrow player=%d order=%d: %s",
                    player_id,
                    order_id,
                    exc,
                ) from exc

            if kind == EscrowKind.ITEM:
                await rsql.deduct_item_tx(cur, player_id, item_config_id, remaining_quantity)
            else:
                await ccy.deduct_currency_tx(cur, player_id, currency_kind, required_amount)
        if holder.rolled_back:
            return holder.value
        return True, False

    async def _validate_existing_auction_escrow(
        self,
        player_id: int,
        order_id: int,
        want_kind: EscrowKind,
        want_item_config_id: int,
        remaining_quantity: int,
        want_currency_kind: int,
        required_amount: int,
    ) -> bool:
        """FOR UPDATE 复核已存在的托管行。返回 found;不满足条件时抛业务错误。"""
        async with rsql.transaction(self._pool) as cur:
            try:
                await cur.execute(
                    "SELECT kind, item_config_id, frozen_qty, frozen_amount, currency_kind, status "
                    "FROM auction_escrow WHERE player_id = %s AND order_id = %s FOR UPDATE",
                    (player_id, order_id),
                )
                row = await cur.fetchone()
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "lock ensured escrow player=%d order=%d: %s",
                    player_id,
                    order_id,
                    exc,
                ) from exc
            if row is None:
                return False
            kind, item_config_id = int(row[0]), int(row[1])
            frozen_qty, frozen_amount = int(row[2]), int(row[3])
            currency_kind, status = int(row[4]), int(row[5])
            if status != ESCROW_STATUS_ACTIVE:
                raise errcode.PandoraError(
                    errcode.ErrInventoryIdempotencyConflict,
                    "escrow is not active player=%d order=%d status=%d",
                    player_id,
                    order_id,
                    status,
                )
            if kind != int(want_kind) or item_config_id != want_item_config_id:
                raise errcode.PandoraError(
                    errcode.ErrInventoryIdempotencyConflict,
                    "escrow identity conflict player=%d order=%d kind=%d item=%d "
                    "want_kind=%d want_item=%d",
                    player_id,
                    order_id,
                    kind,
                    item_config_id,
                    int(want_kind),
                    want_item_config_id,
                )
            if want_kind == EscrowKind.ITEM:
                if frozen_amount != 0:
                    raise errcode.PandoraError(
                        errcode.ErrInventoryIdempotencyConflict,
                        "item escrow carries gold player=%d order=%d frozen_amount=%d",
                        player_id,
                        order_id,
                        frozen_amount,
                    )
                if frozen_qty < remaining_quantity:
                    raise errcode.PandoraError(
                        errcode.ErrInventoryInsufficient,
                        "item escrow short player=%d order=%d frozen=%d need=%d",
                        player_id,
                        order_id,
                        frozen_qty,
                        remaining_quantity,
                    )
            else:
                if frozen_qty != 0:
                    raise errcode.PandoraError(
                        errcode.ErrInventoryIdempotencyConflict,
                        "currency escrow carries item quantity player=%d order=%d frozen_qty=%d",
                        player_id,
                        order_id,
                        frozen_qty,
                    )
                # 币种必须与订单一致。**绝不允许静默按托管行的币种结算**:
                # 那等于用玩家冻的钻石去付一笔金币订单,是不可观测的经济事故。
                if currency_kind != int(want_currency_kind):
                    raise errcode.PandoraError(
                        errcode.ErrInventoryIdempotencyConflict,
                        "escrow currency conflict player=%d order=%d have=%d want=%d",
                        player_id,
                        order_id,
                        currency_kind,
                        int(want_currency_kind),
                    )
                if frozen_amount < required_amount:
                    raise errcode.PandoraError(
                        errcode.ErrInventoryInsufficient,
                        "currency escrow short player=%d order=%d frozen=%d need=%d",
                        player_id,
                        order_id,
                        frozen_amount,
                        required_amount,
                    )
            return True

    async def release_escrow(self, player_id: int, order_id: int) -> bool:
        """退还某挂单 escrow 残余到活跃余额并关闭托管。返回 already。

        幂等由 escrow 行状态承担:行不存在(冻结失败的挂单从未建 escrow)或已 closed
        → already no-op(只退一次)。
        """
        async with rsql.transaction(self._pool) as cur:
            try:
                await cur.execute(
                    "SELECT kind, item_config_id, frozen_qty, frozen_amount, currency_kind, status "
                    "FROM auction_escrow WHERE player_id = %s AND order_id = %s FOR UPDATE",
                    (player_id, order_id),
                )
                row = await cur.fetchone()
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "lock escrow player=%d order=%d: %s",
                    player_id,
                    order_id,
                    exc,
                ) from exc
            if row is None:
                return True
            kind, item_config_id = int(row[0]), int(row[1])
            frozen_qty, frozen_amount = int(row[2]), int(row[3])
            currency_kind, status = int(row[4]), int(row[5])
            if status == ESCROW_STATUS_CLOSED:
                return True

            if kind == EscrowKind.ITEM and frozen_qty > 0:
                await rsql.add_item_tx(cur, player_id, item_config_id, frozen_qty)
            elif kind == EscrowKind.CURRENCY and frozen_amount > 0:
                # 退还必须按**托管行自己记的币种**,不能按当前配置或调用方口径
                # —— 冻的是什么就退什么,否则撤单会变成免费换币。
                await ccy.add_currency_tx(cur, player_id, currency_kind, frozen_amount)

            try:
                await cur.execute(
                    "UPDATE auction_escrow SET frozen_qty = 0, frozen_amount = 0, status = %s "
                    "WHERE player_id = %s AND order_id = %s",
                    (ESCROW_STATUS_CLOSED, player_id, order_id),
                )
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "close escrow player=%d order=%d: %s",
                    player_id,
                    order_id,
                    exc,
                ) from exc
            return False

    # ── 保留期清理(CLAUDE.md §9 不变量 24)────────────────────────────────

    async def sweep_ledger_before(
        self, mode: dbguard.Mode, retention_days: int, limit: int
    ) -> dbguard.Outcome:
        """处理 created_at 超保留期的幂等流水。

        **mode 默认 REPORT_ONLY:只统计待清理量并 WARN,一行都不删。**
        真删语义(仅供开启前评估):保留期必须远大于一切发放/使用/出售/结算的重试窗口
        (分钟级),行删除后同 key 重放不再被 uk 拦截,靠"对应操作早已终态"保证不重复入账。
        """
        async with self._pool.acquire() as conn:
            try:
                return await dbguard.sweep_table(
                    conn,
                    mode,
                    self._db,
                    "inventory_ledger",
                    "created_at < DATE_SUB(NOW(), INTERVAL %s DAY)",
                    limit,
                    retention_days,
                )
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(errcode.ErrInternal, "sweep ledger: %s", exc) from exc

    async def sweep_closed_escrow_before(
        self, mode: dbguard.Mode, retention_days: int, limit: int
    ) -> dbguard.Outcome:
        """处理已关闭(status=closed)且 updated_at 超保留期的托管行。

        ★ active 行**无论如何**都不在处理范围:EnsureAuctionEscrow 依赖它们的存在性
          核对 OPEN/PARTIAL 遗留订单。删了 active 行,那些订单会被判成"从没冻结过"
          而重新扣一次活跃资产。
        真删后迟到的 ReleaseEscrow 命中无行 → already no-op,fail-safe。
        """
        async with self._pool.acquire() as conn:
            try:
                return await dbguard.sweep_table(
                    conn,
                    mode,
                    self._db,
                    "auction_escrow",
                    "status = %s AND updated_at < DATE_SUB(NOW(), INTERVAL %s DAY)",
                    limit,
                    ESCROW_STATUS_CLOSED,
                    retention_days,
                )
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "sweep closed escrow: %s", exc
                ) from exc
