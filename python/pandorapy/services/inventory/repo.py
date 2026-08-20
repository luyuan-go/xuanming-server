"""inventory 数据层(MySQL 货币 / 道具 / 幂等流水 / 拍卖托管)。

对应 Go 侧 internal/data/inventory_repo.go。库表(pandora_trade):

    player_currency       玩家货币余额(PK player_id)
    player_items          背包道具堆叠(uk player_id+item_config_id)
    player_item_instance  装备实例(见 repo_instance.py)
    inventory_ledger      发放 / 使用 / 出售 / 结算幂等流水(uk player_id+idempotency_key)
    auction_escrow        挂单托管(uk player_id+order_id)
    mail_transfer_escrow  邮件附件实例托管(见 repo_transfer.py)

反作弊 / 一致性(不变量 §9.7):GrantItems / UseItem / SellItem / 两种结算全部在一个
事务里先 INSERT inventory_ledger(命中 uk → 幂等已处理),再原子改
player_items / player_currency;扣减用 `SELECT ... FOR UPDATE` 锁行 + 数量校验,
避免并发超扣。

★ 全服务**没有一行 Redis Lua**。原子性只有三样东西撑着:
  MySQL 本地事务、FOR UPDATE 行锁、唯一键 1062。所以本文件里真正要命的是
  **锁序**和 **1062 语义**(见 repo_sql.py 头注释),两者都不写在类型里、
  写错也不报错,只在并发压力下表现成死锁或超扣。
"""

from __future__ import annotations

from pandorapy import dbguard
from pandorapy import errcode
from pandorapy import mysqlx
from pandorapy.services.inventory import fingerprint as fp
from pandorapy.services.inventory import repo_sql as rsql
from pandorapy.services.inventory.models import (
    ESCROW_STATUS_ACTIVE,
    ESCROW_STATUS_CLOSED,
    EscrowKind,
    ItemGrant,
    ItemStack,
)
from pandorapy.services.inventory.repo_instance import InstanceRepoMixin
from pandorapy.services.inventory.repo_transfer import TransferRepoMixin

TRADE_DB = "pandora_trade"

# EnsureAuctionEscrow 的重试次数(与 Go 的 ensureAuctionEscrowMaxAttempts 同值)。
ENSURE_AUCTION_ESCROW_MAX_ATTEMPTS = 3

_MAX_INT64 = (1 << 63) - 1


def safe_mul_positive_int64(a: int, b: int) -> tuple[int, bool]:
    """正数 int64 乘法溢出守卫。对应 Go 的 safeMulPositiveInt64。

    不做守卫的话,Go 侧会静默回绕成负数 —— 一笔"总价为负"的成交会给买家加钱。
    Python 的 int 不会溢出,但**列是 BIGINT**:溢出的值写进去照样是错的,
    所以判据必须与 Go 一致地拒掉,而不是"Python 能算就算"。
    """
    if a <= 0 or b <= 0 or a > _MAX_INT64 // b:
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


async def _consume_gold_escrow_tx(cur, player_id: int, order_id: int, gold: int) -> None:  # noqa: ANN001
    """锁买单金币 escrow 并消费 gold(成交付款)。"""
    try:
        await cur.execute(
            "SELECT kind, frozen_gold, status FROM auction_escrow "
            "WHERE player_id = %s AND order_id = %s FOR UPDATE",
            (player_id, order_id),
        )
        row = await cur.fetchone()
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "lock gold escrow player=%d order=%d: %s",
            player_id,
            order_id,
            exc,
        ) from exc
    if row is None:
        raise errcode.PandoraError(
            errcode.ErrInventoryInsufficient,
            "gold escrow not found player=%d order=%d",
            player_id,
            order_id,
        )
    kind, frozen_gold, status = int(row[0]), int(row[1]), int(row[2])
    if kind != EscrowKind.GOLD:
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "escrow kind mismatch player=%d order=%d kind=%d want gold",
            player_id,
            order_id,
            kind,
        )
    if status == ESCROW_STATUS_CLOSED or frozen_gold < gold:
        raise errcode.PandoraError(
            errcode.ErrInventoryInsufficient,
            "gold escrow short player=%d order=%d frozen=%d need=%d",
            player_id,
            order_id,
            frozen_gold,
            gold,
        )
    try:
        await cur.execute(
            "UPDATE auction_escrow SET frozen_gold = frozen_gold - %s "
            "WHERE player_id = %s AND order_id = %s",
            (gold, player_id, order_id),
        )
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "consume gold escrow player=%d order=%d: %s",
            player_id,
            order_id,
            exc,
        ) from exc


class MySQLInventoryRepo(InstanceRepoMixin, TransferRepoMixin):
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

    async def get_inventory(self, player_id: int) -> tuple[int, list[ItemStack]]:
        """读玩家货币 + 道具堆叠(按 item_config_id 排序;未建档 → gold=0 空道具)。

        ★ 只读 count > 0 的行。扣空的行会被删掉(见 repo_sql.deduct_item_tx),
          但历史数据里可能还留着 count=0 的死行 —— 把它们读出来会让客户端显示
          "持有 0 个 XX"。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(
                    "SELECT gold FROM player_currency WHERE player_id = %s LIMIT 1", (player_id,)
                )
                gold_row = await cur.fetchone()
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "read gold player=%d: %s", player_id, exc
                ) from exc
            gold = int(gold_row[0]) if gold_row else 0
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
        return gold, [ItemStack(item_config_id=int(r[0]), count=int(r[1])) for r in rows or ()]

    # ── 发放 / 扣减 ────────────────────────────────────────────────────────

    async def grant_items(
        self,
        player_id: int,
        items: list[ItemGrant],
        gold: int,
        idempotency_key: str,
        detail: str,
    ) -> tuple[int, bool]:
        """幂等发放道具 + 货币。返回 (发放后 gold, already)。"""
        fingerprint = fp.grant_fingerprint(((it.item_config_id, it.count) for it in items), gold)
        async with rsql.transaction(self._pool) as cur:
            already, _remaining, snap_gold = await rsql.claim_ledger(
                cur, player_id, idempotency_key, "grant", fingerprint, detail
            )
            if already:
                return snap_gold, True
            for it in items:
                await rsql.add_item_tx(cur, player_id, it.item_config_id, it.count)
            # ★ gold 恒写(哪怕是 0):Go 侧也是无条件 upsert。少了这条,
            #   "只发道具不发钱"的请求不会给玩家建 player_currency 行,
            #   后续 deduct_gold_tx 看到无行会报 insufficient 而不是 have=0。
            await rsql.add_gold_tx(cur, player_id, gold)
            new_gold = await rsql.read_gold_tx(cur, player_id)
            await rsql.update_ledger_result(cur, player_id, idempotency_key, 0, new_gold)
            return new_gold, False

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
            already, snap_remaining, _gold = await rsql.claim_ledger(
                cur, player_id, idempotency_key, op, fingerprint, detail
            )
            if already:
                return snap_remaining, True
            remaining = await rsql.deduct_item_tx(cur, player_id, item_config_id, count)
            await rsql.update_ledger_result(cur, player_id, idempotency_key, remaining, 0)
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
        gold: int,
        idempotency_key: str,
        detail: str,
    ) -> tuple[int, int, bool]:
        """幂等出售。返回 (剩余数量, 出售后 gold, already)。

        gold<=0 的首次请求在 claim 之后才拒(ErrInventoryNotSellable),整笔回滚 ——
        连 claim 行一起没有,不会留下脏流水把这个 key 永久占死。
        """
        async with rsql.transaction(self._pool) as cur:
            already, snap_remaining, snap_gold = await rsql.claim_sale_ledger(
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
                return snap_remaining, snap_gold, True
            if gold <= 0:
                raise errcode.PandoraError(
                    errcode.ErrInventoryNotSellable,
                    "item not sellable player=%d item=%d",
                    player_id,
                    item_config_id,
                )
            remaining = await rsql.deduct_item_tx(cur, player_id, item_config_id, count)
            await rsql.add_gold_tx(cur, player_id, gold)
            new_gold = await rsql.read_gold_tx(cur, player_id)
            await rsql.update_ledger_result(cur, player_id, idempotency_key, remaining, new_gold)
            return remaining, new_gold, False

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
        total_gold: int,
        idempotency_key: str,
        detail: str,
    ) -> bool:
        """一个本地事务里从双方 escrow 消费完成拍卖成交的卖↔买对转。返回 already。

        因卖家道具与买家金币已在 FreezeForOrder 冻结进 escrow,本步只做
        「消费 escrow + 入账对手」,不再触活跃余额扣减,故成交不会因余额不足失败。

        ★ 防死锁:对 escrow / player_items / player_currency 的行锁全部按
          **player_id 升序**、同一玩家内「先 escrow 后入账」的总顺序获取。
          不这么做的话,两笔买卖角色对调的并发成交会以相反顺序锁同一批行 → 1213。
        """
        fingerprint = fp.auction_settle_fingerprint(
            seller_id, buyer_id, item_config_id, quantity, total_gold
        )
        async with rsql.transaction(self._pool) as cur:
            # 1) 幂等流水:按 player_id 升序声明两条(同 key),避免并发交叉插入死锁。
            lo_id, hi_id = seller_id, buyer_id
            lo_op, hi_op = "auction_sell", "auction_buy"
            if buyer_id < seller_id:
                lo_id, hi_id = buyer_id, seller_id
                lo_op, hi_op = "auction_buy", "auction_sell"
            lo_already, _lr, _lg = await rsql.claim_ledger(
                cur, lo_id, idempotency_key, lo_op, fingerprint, detail
            )
            hi_already, _hr, _hg = await rsql.claim_ledger(
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
                await rsql.add_gold_tx(cur, seller_id, total_gold)

            async def buyer_leg() -> None:
                await _consume_gold_escrow_tx(cur, buyer_id, buy_order_id, total_gold)
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
        price: int,
        idempotency_key: str,
        detail: str,
    ) -> bool:
        """一个本地事务里的玩家间点对点交易对转。返回 already。

        与拍卖不同:P2P **无 escrow 预冻结**,直接从双方活跃余额扣转,
        任一方道具 / 金币不足 → ErrInventoryInsufficient,整笔回滚(成交失败)。

        ★ 腿内锁序必须全局一致:同一玩家的道具行按 item_config_id 升序
          「扣/加合并成一趟」处理(同 ID 先扣后加),金币行统一放腿尾。
          若像旧实现那样「先扣完再加」,两笔方向对调的并发交易
          (A 卖 item1 换 item2 vs A 卖 item2 换 item1)会在同一玩家的行上
          以相反顺序加锁 → InnoDB 死锁(1213)。
        """
        fingerprint = fp.player_trade_settle_fingerprint(
            seller_id,
            buyer_id,
            ((it.item_config_id, it.count) for it in seller_items),
            ((it.item_config_id, it.count) for it in buyer_items),
            price,
        )
        async with rsql.transaction(self._pool) as cur:
            lo_id, hi_id = seller_id, buyer_id
            lo_op, hi_op = "trade_sell", "trade_buy"
            if buyer_id < seller_id:
                lo_id, hi_id = buyer_id, seller_id
                lo_op, hi_op = "trade_buy", "trade_sell"
            lo_already, _lr, _lg = await rsql.claim_ledger(
                cur, lo_id, idempotency_key, lo_op, fingerprint, detail
            )
            hi_already, _hr, _hg = await rsql.claim_ledger(
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
                    await rsql.add_gold_tx(cur, seller_id, price)

            async def buyer_leg() -> None:
                await item_ops(buyer_id, sorted_buyer, sorted_seller)
                if price > 0:
                    await rsql.deduct_gold_tx(cur, buyer_id, price)

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
        frozen_gold: int,
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
                    "(player_id, order_id, kind, item_config_id, frozen_qty, frozen_gold, status) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        player_id,
                        order_id,
                        int(kind),
                        item_config_id,
                        frozen_qty,
                        frozen_gold,
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
            elif kind == EscrowKind.GOLD:
                await rsql.deduct_gold_tx(cur, player_id, frozen_gold)
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
        if kind not in (EscrowKind.ITEM, EscrowKind.GOLD):
            raise errcode.PandoraError(errcode.ErrInvalidArg, "unknown escrow kind %d", int(kind))
        required_gold = 0
        if kind == EscrowKind.GOLD:
            required_gold, ok = safe_mul_positive_int64(remaining_quantity, unit_price)
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
                player_id, order_id, kind, item_config_id, remaining_quantity, required_gold
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
                player_id, order_id, kind, item_config_id, remaining_quantity, required_gold
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
        required_gold: int,
    ) -> tuple[bool, bool]:
        """返回 (created, duplicate)。duplicate 分支**必定已回滚**(哨兵退出)。"""
        frozen_qty = remaining_quantity if kind == EscrowKind.ITEM else 0
        frozen_gold = 0 if kind == EscrowKind.ITEM else required_gold

        async with rsql.transaction_or_value(self._pool) as holder:
            cur = holder.cur
            try:
                await cur.execute(
                    "INSERT INTO auction_escrow "
                    "(player_id, order_id, kind, item_config_id, frozen_qty, frozen_gold, status) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        player_id,
                        order_id,
                        int(kind),
                        item_config_id,
                        frozen_qty,
                        frozen_gold,
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
                await rsql.deduct_gold_tx(cur, player_id, required_gold)
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
        required_gold: int,
    ) -> bool:
        """FOR UPDATE 复核已存在的托管行。返回 found;不满足条件时抛业务错误。"""
        async with rsql.transaction(self._pool) as cur:
            try:
                await cur.execute(
                    "SELECT kind, item_config_id, frozen_qty, frozen_gold, status "
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
            frozen_qty, frozen_gold, status = int(row[2]), int(row[3]), int(row[4])
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
                if frozen_gold != 0:
                    raise errcode.PandoraError(
                        errcode.ErrInventoryIdempotencyConflict,
                        "item escrow carries gold player=%d order=%d frozen_gold=%d",
                        player_id,
                        order_id,
                        frozen_gold,
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
                        "gold escrow carries item quantity player=%d order=%d frozen_qty=%d",
                        player_id,
                        order_id,
                        frozen_qty,
                    )
                if frozen_gold < required_gold:
                    raise errcode.PandoraError(
                        errcode.ErrInventoryInsufficient,
                        "gold escrow short player=%d order=%d frozen=%d need=%d",
                        player_id,
                        order_id,
                        frozen_gold,
                        required_gold,
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
                    "SELECT kind, item_config_id, frozen_qty, frozen_gold, status "
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
            frozen_qty, frozen_gold, status = int(row[2]), int(row[3]), int(row[4])
            if status == ESCROW_STATUS_CLOSED:
                return True

            if kind == EscrowKind.ITEM and frozen_qty > 0:
                await rsql.add_item_tx(cur, player_id, item_config_id, frozen_qty)
            elif kind == EscrowKind.GOLD and frozen_gold > 0:
                await rsql.add_gold_tx(cur, player_id, frozen_gold)

            try:
                await cur.execute(
                    "UPDATE auction_escrow SET frozen_qty = 0, frozen_gold = 0, status = %s "
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
