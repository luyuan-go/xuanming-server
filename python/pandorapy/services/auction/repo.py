"""auction 数据层(MySQL 撮合权威 + 分库路由)—— 对应 Go 侧 internal/data/auction_repo.go。

MySQL 同时保存订单、成交意图和待补偿副作用。任何会消耗或释放 escrow 的外部调用,
都必须先把意图持久化,再以 match_id / order_id 调用幂等账本;进程崩溃后由后台重试。

★ 本文件里几条"写错不报错"的地方,逐条标了 ★:

  1. 条件更新的 rowcount 判定(execChanged)。MySQL 默认返回的是**改变行数**而不是
     匹配行数(go-sql-driver 同),所以"值恰好没变"会得到 0 —— ConfirmOrderEscrow
     专门为此加了回读兜底。把它删掉的后果:同一毫秒重放时订单永久卡在 PENDING。
  2. ClaimOrder 的三段协议(事务外扫描 → coordinator 单库事务登记 → 事务外补回 market
     PENDING)。**绝不能**持 coordinator 事务连接再去查/写其他 shard:双向请求会各自
     占满连接池,形成连接池环,表现是整个服务挂死而没有任何错误日志。
  3. 分片遍历一律"每分片各取一批",不是全局 remaining。用全局的话首个繁忙分片会
     **永久饿死**后续分片,而补偿计数看着一直在动。
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import time
from collections.abc import AsyncIterator, Sequence
from typing import Any

from pandorapy import errcode, mysqlx
from pandorapy.services.auction.submit import (
    SIDE_BUY,
    STATUS_CANCELED,
    STATUS_EXPIRED,
    STATUS_FILLED,
    STATUS_OPEN,
    STATUS_PARTIAL,
    STATUS_PENDING,
    OrderRecord,
)

# 成交结算状态。DDL 默认必须是 COMPLETED,使未指定该列的旧二进制保持原语义。
SETTLEMENT_PENDING = 0
SETTLEMENT_COMPLETED = 1

# 撮合权威库名(逐分片同名)。
DB_SCHEMA = "pandora_auction"


@dataclasses.dataclass(slots=True)
class MatchRecord:
    """auction_matches 一行的存储视图(成交事实 + 结算意图)。对应 Go 的 MatchRecord。"""

    match_id: int = 0
    market_id: int = 0
    sell_order_id: int = 0
    buy_order_id: int = 0
    seller_id: int = 0
    buyer_id: int = 0
    item_config_id: int = 0
    quantity: int = 0
    price: int = 0
    matched_at_ms: int = 0
    settlement_status: int = SETTLEMENT_PENDING
    settlement_next_attempt_at_ms: int = 0
    event_pending: bool = False
    event_next_attempt_at_ms: int = 0


def remaining_of(o: OrderRecord) -> int:
    """未成交剩余量。对应 Go 的 `OrderRecord.Remaining()`。"""
    return o.quantity - o.filled_quantity


def now_ms() -> int:
    return int(time.time() * 1000)


# ── 分库路由 ──────────────────────────────────────────────────────────────────
#
# Go 侧是 mysqlx.ShardSet(shard = id % N)。Python 的 pandorapy.mysqlx **没有**
# 对应实现,而 auction 是本仓唯一分库的服务,所以路由在这里落地,公式与 Go 逐字相同。
# 公式一旦定稿不可随意改:改 N 或改取模对象都会让历史数据被路由丢失(查不到 = 订单
# 凭空消失),而且不报错。


@dataclasses.dataclass(frozen=True, slots=True)
class DBShard:
    """一个不可拆分的 auction 分片连接与其实际物理 schema。"""

    pool: Any
    schema: str


class DBRouter:
    """按 market_id / owner_id 选库。对应 Go 的 data.DBRouter。"""

    __slots__ = ("_shards",)

    def __init__(
        self, pools: Sequence[Any], *, schemas: Sequence[str] | None = None
    ) -> None:
        if not pools:
            raise ValueError("auction DBRouter: 至少要有一个连接池")
        if schemas is None:
            schemas = [DB_SCHEMA] * len(pools)
        if len(schemas) != len(pools):
            raise ValueError("auction DBRouter: pools/schemas 数量必须一致")
        self._shards = tuple(
            DBShard(
                pool=pool,
                schema=mysqlx.require_mysql_identifier(schema, kind="schema"),
            )
            for pool, schema in zip(pools, schemas, strict=True)
        )

    def for_market(self, market_id: int) -> Any:
        return self._shards[market_id % len(self._shards)].pool

    def for_owner(self, owner_id: int) -> Any:
        return self._shards[owner_id % len(self._shards)].pool

    def all(self) -> tuple[Any, ...]:
        return tuple(shard.pool for shard in self._shards)

    def shards(self) -> tuple[DBShard, ...]:
        return self._shards

    def count(self) -> int:
        return len(self._shards)


# ── 事务与游标底座 ────────────────────────────────────────────────────────────


@contextlib.asynccontextmanager
async def _cursor(pool) -> AsyncIterator:  # noqa: ANN001
    """借一条连接开一个游标(autocommit 池,单语句自提交)。"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            yield cur


@contextlib.asynccontextmanager
async def _transaction(pool) -> AsyncIterator:  # noqa: ANN001
    """显式事务。正常退出 COMMIT,异常 ROLLBACK 后原样上抛。

    与 inventory 的 repo_sql.transaction 同形:池是 autocommit=True,
    需要多语句原子性的地方显式 begin()(与 Go 的 database/sql 一致)。
    """
    async with pool.acquire() as conn:
        await conn.begin()
        try:
            async with conn.cursor() as cur:
                yield cur
        except BaseException:
            # ★ 这里必须放行一切(含 CancelledError):回滚后原样上抛,不吞。
            # 不回滚就走人 = 行锁攥在手里直到连接归池被复用,后续事务自己等自己。
            with contextlib.suppress(Exception):
                await conn.rollback()
            raise
        else:
            await conn.commit()


ORDER_COLS = (
    "order_id, market_id, owner_id, side, item_config_id, quantity, filled_quantity, "
    "price, status, release_pending, match_pending, escrow_verified, "
    "reconcile_next_attempt_at_ms, release_next_attempt_at_ms, idempotency_key, "
    "created_at_ms, updated_at_ms"
)
ORDER_COLS_O = ", ".join("o." + c.strip() for c in ORDER_COLS.split(","))
MATCH_COLS = (
    "match_id, market_id, sell_order_id, buy_order_id, seller_id, buyer_id, "
    "item_config_id, quantity, price, matched_at_ms, settlement_status, "
    "settlement_next_attempt_at_ms, event_pending, event_next_attempt_at_ms"
)
MATCH_COLS_M = ", ".join("m." + c.strip() for c in MATCH_COLS.split(","))


def _scan_order(row: Sequence[Any]) -> OrderRecord:
    return OrderRecord(
        order_id=int(row[0]),
        market_id=int(row[1]),
        owner_id=int(row[2]),
        side=int(row[3]),
        item_config_id=int(row[4]),
        quantity=int(row[5]),
        filled_quantity=int(row[6]),
        price=int(row[7]),
        status=int(row[8]),
        release_pending=bool(row[9]),
        match_pending=bool(row[10]),
        escrow_verified=bool(row[11]),
        reconcile_next_attempt_at_ms=int(row[12]),
        release_next_attempt_at_ms=int(row[13]),
        idempotency_key=row[14] if isinstance(row[14], str) else bytes(row[14]).decode(),
        created_at_ms=int(row[15]),
        updated_at_ms=int(row[16]),
    )


def _scan_match(row: Sequence[Any]) -> MatchRecord:
    return MatchRecord(
        match_id=int(row[0]),
        market_id=int(row[1]),
        sell_order_id=int(row[2]),
        buy_order_id=int(row[3]),
        seller_id=int(row[4]),
        buyer_id=int(row[5]),
        item_config_id=int(row[6]),
        quantity=int(row[7]),
        price=int(row[8]),
        matched_at_ms=int(row[9]),
        settlement_status=int(row[10]),
        settlement_next_attempt_at_ms=int(row[11]),
        event_pending=bool(row[12]),
        event_next_attempt_at_ms=int(row[13]),
    )


def _same_order_fingerprint(a: OrderRecord | None, b: OrderRecord | None) -> bool:
    return (
        a is not None
        and b is not None
        and a.owner_id == b.owner_id
        and a.market_id == b.market_id
        and a.side == b.side
        and a.item_config_id == b.item_config_id
        and a.quantity == b.quantity
        and a.price == b.price
    )


def _same_canonical_order(a: OrderRecord | None, b: OrderRecord | None) -> bool:
    return (
        a is not None
        and b is not None
        and a.order_id == b.order_id
        and a.idempotency_key == b.idempotency_key
        and a.created_at_ms == b.created_at_ms
        and _same_order_fingerprint(a, b)
    )


def _idempotency_conflict(owner_id: int, key: str) -> errcode.PandoraError:
    return errcode.PandoraError(
        errcode.ErrAuctionIdempotencyConflict,
        "idempotency_key reused for different request owner=%d key=%s",
        owner_id,
        key,
    )


def _is_active_status(status: int) -> bool:
    return status in (STATUS_OPEN, STATUS_PARTIAL)


def _is_incoming_status(status: int) -> bool:
    return status == STATUS_PENDING or _is_active_status(status)


class MySQLAuctionRepo:
    """撮合权威库。biz 只依赖本类的方法名,不依赖连接池类型。"""

    __slots__ = ("_r",)

    def __init__(self, router: DBRouter) -> None:
        self._r = router

    @property
    def router(self) -> DBRouter:
        return self._r

    # ── 通用小工具 ────────────────────────────────────────────────────────

    async def _exec_changed(self, pool, sql: str, action: str, *params: Any) -> bool:  # noqa: ANN001
        """执行一条条件更新,返回"是否真的改了行"。对应 Go 的 execChanged。

        ★ rowcount 是**改变行数**不是匹配行数(MySQL 默认,go-sql-driver 亦然)。
        条件更新靠它做 CAS 判定,所以"值本来就一样"会返回 False —— 这是刻意的,
        调用方(如 ConfirmOrderEscrow)自己补回读。
        """
        try:
            async with _cursor(pool) as cur:
                await cur.execute(sql, params)
                return (cur.rowcount or 0) > 0
        except errcode.PandoraError:
            raise
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except
            # 吞掉。吞掉之后取消不再传播 —— grpc.aio 正是用取消终止在途 handler,
            # 吞了会把取消变成一个正常应答,§9.16「先摘流量 → 再排空在途」当场失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(errcode.ErrInternal, "%s: %s", action, exc) from exc

    async def _query_all(self, pool, sql: str, params: tuple, action: str) -> list[tuple]:  # noqa: ANN001
        try:
            async with _cursor(pool) as cur:
                await cur.execute(sql, params)
                return list(await cur.fetchall())
        except errcode.PandoraError:
            raise
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except
            # 吞掉。吞掉之后取消不再传播 —— grpc.aio 正是用取消终止在途 handler,
            # 吞了会把取消变成一个正常应答,§9.16「先摘流量 → 再排空在途」当场失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(errcode.ErrInternal, "%s: %s", action, exc) from exc

    async def _query_one(self, pool, sql: str, params: tuple, action: str):  # noqa: ANN001, ANN201
        try:
            async with _cursor(pool) as cur:
                await cur.execute(sql, params)
                return await cur.fetchone()
        except errcode.PandoraError:
            raise
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except
            # 吞掉。吞掉之后取消不再传播 —— grpc.aio 正是用取消终止在途 handler,
            # 吞了会把取消变成一个正常应答,§9.16「先摘流量 → 再排空在途」当场失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(errcode.ErrInternal, "%s: %s", action, exc) from exc

    # ── 幂等登记(ClaimOrder 三段协议)────────────────────────────────────

    async def claim_order(self, rec: OrderRecord) -> tuple[OrderRecord | None, bool]:
        """以 PENDING 幂等登记订单。命中 uk(owner+idem)时返回已有快照。

        ★ 三段协议(对应 Go 的同名函数,顺序不可改):
            ① 事务外扫描 legacy(不持任何连接)
            ② coordinator(owner 分片)单库事务登记不可变 canonical
            ③ 事务外幂等补回 market 分片的 PENDING 行
          绝不能持 ② 的事务连接去做 ①③ 的跨分片 I/O —— 双向请求各自占满连接池后
          会形成连接池环,整个服务挂死且**没有任何错误日志**。
        """
        coordinator = self._r.for_owner(rec.owner_id)
        try:
            async with _cursor(coordinator) as cur:
                await cur.execute(
                    "INSERT IGNORE INTO auction_owner_guards (owner_id) VALUES (%s)",
                    (rec.owner_id,),
                )
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except
            # 吞掉。吞掉之后取消不再传播 —— grpc.aio 正是用取消终止在途 handler,
            # 吞了会把取消变成一个正常应答,§9.16「先摘流量 → 再排空在途」当场失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "create owner guard %d: %s", rec.owner_id, exc
            ) from exc

        # 已登记的 registry 是 immutable 快路径,不需要持 guard 锁做跨分片 I/O。
        canonical = await self._read_idempotency_claim(
            coordinator, rec.owner_id, rec.idempotency_key
        )
        if canonical is not None:
            if not _same_order_fingerprint(canonical, rec):
                raise _idempotency_conflict(rec.owner_id, rec.idempotency_key)
            return await self._ensure_canonical_order(canonical), True

        # registry 首次 miss 才广播扫描兼容历史订单。
        legacy = await self._find_orders_by_owner_idempotency(rec.owner_id, rec.idempotency_key)
        if len(legacy) > 1:
            raise errcode.PandoraError(
                errcode.ErrAuctionIdempotencyConflict,
                "multiple legacy orders for owner=%d key=%s require reconciliation",
                rec.owner_id,
                rec.idempotency_key,
            )

        async with _transaction(coordinator) as cur:
            await cur.execute(
                "SELECT owner_id FROM auction_owner_guards WHERE owner_id = %s FOR UPDATE",
                (rec.owner_id,),
            )
            locked = await cur.fetchone()
            if locked is None:
                raise errcode.PandoraError(
                    errcode.ErrInternal, "lock owner guard %d: row missing", rec.owner_id
                )
            # 扫描期间可能已有另一个实例登记 canonical;锁内必须重读,以库内映射为准。
            canonical = await self._read_claim_with_cursor(
                cur, rec.owner_id, rec.idempotency_key
            )
            already = canonical is not None
            if canonical is not None:
                if not _same_order_fingerprint(canonical, rec):
                    raise _idempotency_conflict(rec.owner_id, rec.idempotency_key)
            elif len(legacy) == 1:
                canonical = legacy[0]
                if not _same_order_fingerprint(canonical, rec):
                    raise _idempotency_conflict(rec.owner_id, rec.idempotency_key)
                await self._insert_claim_with_cursor(cur, canonical)
                already = True
            else:
                canonical = rec
                await self._insert_claim_with_cursor(cur, canonical)

        return await self._ensure_canonical_order(canonical), already

    async def _read_idempotency_claim(
        self, pool, owner_id: int, key: str  # noqa: ANN001
    ) -> OrderRecord | None:
        row = await self._query_one(
            pool,
            "SELECT idempotency_key,order_id,market_id,side,item_config_id,quantity,"
            "price,created_at_ms FROM auction_idempotency_keys "
            "WHERE owner_id = %s AND idempotency_key = %s LIMIT 1",
            (owner_id, key),
            "read owner idempotency claim",
        )
        return _claim_row_to_record(row, owner_id)

    async def _read_claim_with_cursor(self, cur, owner_id: int, key: str) -> OrderRecord | None:  # noqa: ANN001
        await cur.execute(
            "SELECT idempotency_key,order_id,market_id,side,item_config_id,quantity,"
            "price,created_at_ms FROM auction_idempotency_keys "
            "WHERE owner_id = %s AND idempotency_key = %s LIMIT 1",
            (owner_id, key),
        )
        return _claim_row_to_record(await cur.fetchone(), owner_id)

    async def _insert_claim_with_cursor(self, cur, o: OrderRecord) -> None:  # noqa: ANN001
        await cur.execute(
            "INSERT INTO auction_idempotency_keys "
            "(owner_id,idempotency_key,order_id,market_id,side,item_config_id,"
            "quantity,price,created_at_ms) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                o.owner_id,
                o.idempotency_key,
                o.order_id,
                o.market_id,
                o.side,
                o.item_config_id,
                o.quantity,
                o.price,
                o.created_at_ms,
            ),
        )

    async def _find_orders_by_owner_idempotency(self, owner_id: int, key: str) -> list[OrderRecord]:
        sql = (
            f"SELECT {ORDER_COLS} FROM auction_orders "
            "WHERE owner_id = %s AND idempotency_key = %s ORDER BY order_id ASC LIMIT 2"
        )
        out: list[OrderRecord] = []
        for pool in self._r.all():
            rows = await self._query_all(
                pool, sql, (owner_id, key), "scan legacy owner idempotency"
            )
            out.extend(_scan_order(r) for r in rows)
            if len(out) > 1:
                break
        return out

    async def _ensure_canonical_order(self, canonical: OrderRecord) -> OrderRecord:
        """coordinator commit 后幂等补回 market PENDING,并严格校验两侧 immutable 字段。"""
        try:
            await self._insert_order_for_claim(canonical)
        except errcode.PandoraError as exc:
            if not _is_dup_error(exc):
                raise
        existing = await self._get_order_for_claim(canonical.market_id, canonical.order_id)
        if existing is None:
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "canonical market row missing after ensure owner=%d key=%s order=%d",
                canonical.owner_id,
                canonical.idempotency_key,
                canonical.order_id,
            )
        if not _same_canonical_order(existing, canonical):
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "canonical order drift owner=%d key=%s order=%d",
                canonical.owner_id,
                canonical.idempotency_key,
                canonical.order_id,
            )
        return existing

    async def _insert_order_for_claim(self, rec: OrderRecord) -> None:
        sql = (
            f"INSERT INTO auction_orders ({ORDER_COLS}) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        )
        pool = self._r.for_market(rec.market_id)
        try:
            async with _cursor(pool) as cur:
                await cur.execute(
                    sql,
                    (
                        rec.order_id,
                        rec.market_id,
                        rec.owner_id,
                        rec.side,
                        rec.item_config_id,
                        rec.quantity,
                        rec.filled_quantity,
                        rec.price,
                        rec.status,
                        int(rec.release_pending),
                        int(rec.match_pending),
                        int(rec.escrow_verified),
                        rec.reconcile_next_attempt_at_ms,
                        rec.release_next_attempt_at_ms,
                        rec.idempotency_key,
                        rec.created_at_ms,
                        rec.updated_at_ms,
                    ),
                )
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except
            # 吞掉。吞掉之后取消不再传播 —— grpc.aio 正是用取消终止在途 handler,
            # 吞了会把取消变成一个正常应答,§9.16「先摘流量 → 再排空在途」当场失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "insert order owner=%d key=%s: %s",
                rec.owner_id,
                rec.idempotency_key,
                exc,
            ) from exc

    async def _get_order_for_claim(self, market_id: int, order_id: int) -> OrderRecord | None:
        row = await self._query_one(
            self._r.for_market(market_id),
            f"SELECT {ORDER_COLS} FROM auction_orders WHERE order_id = %s LIMIT 1",
            (order_id,),
            f"read canonical order {order_id}",
        )
        return _scan_order(row) if row else None

    # ── 单条读写 ──────────────────────────────────────────────────────────

    async def get_order(self, market_id: int, order_id: int) -> tuple[OrderRecord | None, bool]:
        row = await self._query_one(
            self._r.for_market(market_id),
            f"SELECT {ORDER_COLS} FROM auction_orders WHERE order_id = %s LIMIT 1",
            (order_id,),
            f"get order {order_id}",
        )
        if not row:
            return None, False
        return _scan_order(row), True

    async def activate_order(self, market_id: int, order_id: int, updated_at_ms: int) -> bool:
        """只允许 PENDING -> OPEN,且必须 escrow_verified=1。Freeze 成功后调用。"""
        return await self._exec_changed(
            self._r.for_market(market_id),
            "UPDATE auction_orders SET status = %s, reconcile_next_attempt_at_ms = 0, "
            "updated_at_ms = %s WHERE order_id = %s AND status = %s AND escrow_verified = 1",
            "activate order",
            STATUS_OPEN,
            updated_at_ms,
            order_id,
            STATUS_PENDING,
        )

    async def confirm_order_escrow(
        self, market_id: int, order_id: int, updated_at_ms: int
    ) -> bool:
        """幂等确认 inventory 验证结果;已确认且仍可继续的订单也返回 True。撮合前必须成功。"""
        pool = self._r.for_market(market_id)
        changed = await self._exec_changed(
            pool,
            "UPDATE auction_orders SET escrow_verified = 1, "
            "match_pending = IF(status IN (%s, %s), 1, match_pending), "
            "reconcile_next_attempt_at_ms = 0, updated_at_ms = %s "
            "WHERE order_id = %s AND status IN (%s, %s, %s) AND filled_quantity < quantity",
            "confirm order escrow",
            STATUS_OPEN,
            STATUS_PARTIAL,
            updated_at_ms,
            order_id,
            STATUS_PENDING,
            STATUS_OPEN,
            STATUS_PARTIAL,
        )
        if changed:
            return True
        # ★ 同一毫秒重放时所有赋值都可能与现值相同,rowcount=0;回读确认。
        # 少了这段:Freeze 已成功且 marker 已落库后进程退出,重试会**永久卡在 PENDING**
        # —— 资产冻着、订单不进簿、玩家看不到任何东西,而日志一行异常都没有。
        o, found = await self.get_order(market_id, order_id)
        if not found or o is None:
            return False
        return (
            o.escrow_verified
            and remaining_of(o) > 0
            and o.status in (STATUS_PENDING, STATUS_OPEN, STATUS_PARTIAL)
        )

    async def reject_pending_order(
        self, market_id: int, order_id: int, updated_at_ms: int
    ) -> bool:
        """只允许 PENDING -> CANCELED,并登记 release_pending 覆盖 Freeze 结果不确定窗口。"""
        return await self._exec_changed(
            self._r.for_market(market_id),
            "UPDATE auction_orders SET status = %s, release_pending = 1, match_pending = 0, "
            "escrow_verified = 0, reconcile_next_attempt_at_ms = 0, "
            "release_next_attempt_at_ms = 0, updated_at_ms = %s "
            "WHERE order_id = %s AND status = %s",
            "reject pending order",
            STATUS_CANCELED,
            updated_at_ms,
            order_id,
            STATUS_PENDING,
        )

    async def clear_match_pending(self, market_id: int, order_id: int) -> bool:
        return await self._exec_changed(
            self._r.for_market(market_id),
            "UPDATE auction_orders SET match_pending = 0, reconcile_next_attempt_at_ms = 0 "
            "WHERE order_id = %s AND match_pending = 1 AND status IN (%s, %s)",
            "clear match pending",
            order_id,
            STATUS_OPEN,
            STATUS_PARTIAL,
        )

    async def defer_order_reconcile(
        self, market_id: int, order_id: int, next_attempt_at_ms: int
    ) -> bool:
        if next_attempt_at_ms <= 0:
            raise errcode.PandoraError(
                errcode.ErrInternal, "invalid order reconcile retry time %d", next_attempt_at_ms
            )
        return await self._exec_changed(
            self._r.for_market(market_id),
            "UPDATE auction_orders SET reconcile_next_attempt_at_ms = %s WHERE order_id = %s "
            "AND (status = %s OR (status IN (%s, %s) AND "
            "(match_pending = 1 OR escrow_verified = 0)))",
            "defer order reconcile",
            next_attempt_at_ms,
            order_id,
            STATUS_PENDING,
            STATUS_OPEN,
            STATUS_PARTIAL,
        )

    # ── 撮合 ──────────────────────────────────────────────────────────────

    async def find_best_active_order(
        self, market_id: int, item_config_id: int, side: int, exclude_owner_id: int
    ) -> tuple[OrderRecord | None, bool]:
        """从权威库选精确物品、非自己的价格-时间优先对手单。

        ★ 候选**必须**来自 MySQL,不能用 Redis ZSET:那个旧 key 只含 market_id,
        无法区分同品类里的不同 item_config_id —— 会导致跨物品成交。
        """
        order_by = "price DESC, order_id ASC" if side == SIDE_BUY else "price ASC, order_id ASC"
        row = await self._query_one(
            self._r.for_market(market_id),
            f"SELECT {ORDER_COLS} FROM auction_orders "
            "WHERE market_id = %s AND item_config_id = %s AND side = %s "
            "AND escrow_verified = 1 AND status IN (%s, %s) AND owner_id <> %s "
            f"AND filled_quantity < quantity ORDER BY {order_by} LIMIT 1",
            (market_id, item_config_id, side, STATUS_OPEN, STATUS_PARTIAL, exclude_owner_id),
            f"find best active order market={market_id} item={item_config_id} side={side}",
        )
        if not row:
            return None, False
        return _scan_order(row), True

    async def reserve_match(
        self,
        market_id: int,
        incoming_order_id: int,
        resting_order_id: int,
        match_id: int,
        matched_at_ms: int,
    ) -> tuple[MatchRecord | None, OrderRecord | None, OrderRecord | None, bool]:
        """单事务锁定两张订单、复验全部成交不变量、写 PENDING 成交意图并推进双方 filled。

        候选已失效时 reserved=False 且**不产生任何写入**(Go 同)。
        """
        if (
            incoming_order_id == 0
            or resting_order_id == 0
            or incoming_order_id == resting_order_id
        ):
            raise errcode.PandoraError(errcode.ErrInternal, "invalid reserve order ids")
        pool = self._r.for_market(market_id)
        first, second = sorted((incoming_order_id, resting_order_id))
        try:
            async with _transaction(pool) as cur:
                # ★ 按 order_id 升序锁行 —— 两笔并发操作若以相反顺序锁同一批行,
                # InnoDB 直接死锁(1213)。这个顺序不许"顺手优化"。
                await cur.execute(
                    f"SELECT {ORDER_COLS} FROM auction_orders WHERE order_id IN (%s, %s) "
                    "ORDER BY order_id ASC FOR UPDATE",
                    (first, second),
                )
                locked = [_scan_order(r) for r in await cur.fetchall()]
                if len(locked) != 2:
                    return None, None, None, False
                incoming = next((o for o in locked if o.order_id == incoming_order_id), None)
                resting = next((o for o in locked if o.order_id == resting_order_id), None)
                if not _valid_reservation(market_id, incoming, resting):
                    return None, incoming, resting, False

                qty = min(remaining_of(incoming), remaining_of(resting))
                m = _build_reserved_match(
                    match_id, incoming, resting, qty, resting.price, matched_at_ms
                )
                await cur.execute(
                    f"INSERT INTO auction_matches ({MATCH_COLS}) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        m.match_id,
                        m.market_id,
                        m.sell_order_id,
                        m.buy_order_id,
                        m.seller_id,
                        m.buyer_id,
                        m.item_config_id,
                        m.quantity,
                        m.price,
                        m.matched_at_ms,
                        SETTLEMENT_PENDING,
                        m.settlement_next_attempt_at_ms,
                        int(m.event_pending),
                        m.event_next_attempt_at_ms,
                    ),
                )
                incoming.filled_quantity += qty
                resting.filled_quantity += qty
                _advance_reserved_order(incoming, matched_at_ms)
                _advance_reserved_order(resting, matched_at_ms)
                # incoming 部分成交后必须持久续跑;否则本事务提交后进程退出会留下
                # 一张"已交叉却无人主动处理"的 PARTIAL 单。
                incoming.match_pending = remaining_of(incoming) > 0
                if remaining_of(resting) == 0:
                    resting.match_pending = False
                await _update_reserved_order(cur, incoming)
                await _update_reserved_order(cur, resting)
        except errcode.PandoraError:
            raise
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except
            # 吞掉。吞掉之后取消不再传播 —— grpc.aio 正是用取消终止在途 handler,
            # 吞了会把取消变成一个正常应答,§9.16「先摘流量 → 再排空在途」当场失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "reserve match %d: %s", match_id, exc
            ) from exc
        return m, incoming, resting, True

    async def complete_match(self, market_id: int, match_id: int) -> bool:
        return await self._exec_changed(
            self._r.for_market(market_id),
            "UPDATE auction_matches SET settlement_status = %s, "
            "settlement_next_attempt_at_ms = 0, event_pending = 1, event_next_attempt_at_ms = 0 "
            "WHERE match_id = %s AND settlement_status = %s",
            "complete match",
            SETTLEMENT_COMPLETED,
            match_id,
            SETTLEMENT_PENDING,
        )

    async def defer_match_settlement(
        self, market_id: int, match_id: int, next_attempt_at_ms: int
    ) -> bool:
        if next_attempt_at_ms <= 0:
            raise errcode.PandoraError(
                errcode.ErrInternal, "invalid match retry time %d", next_attempt_at_ms
            )
        return await self._exec_changed(
            self._r.for_market(market_id),
            "UPDATE auction_matches SET settlement_next_attempt_at_ms = %s "
            "WHERE match_id = %s AND settlement_status = %s",
            "defer match settlement",
            next_attempt_at_ms,
            match_id,
            SETTLEMENT_PENDING,
        )

    async def defer_match_event(
        self, market_id: int, match_id: int, next_attempt_at_ms: int
    ) -> bool:
        if next_attempt_at_ms <= 0:
            raise errcode.PandoraError(
                errcode.ErrInternal, "invalid match event retry time %d", next_attempt_at_ms
            )
        return await self._exec_changed(
            self._r.for_market(market_id),
            "UPDATE auction_matches SET event_next_attempt_at_ms = %s "
            "WHERE match_id = %s AND event_pending = 1 AND settlement_status = %s",
            "defer match event",
            next_attempt_at_ms,
            match_id,
            SETTLEMENT_COMPLETED,
        )

    async def clear_match_event_pending(self, market_id: int, match_id: int) -> bool:
        return await self._exec_changed(
            self._r.for_market(market_id),
            "UPDATE auction_matches SET event_pending = 0, event_next_attempt_at_ms = 0 "
            "WHERE match_id = %s AND event_pending = 1 AND settlement_status = %s",
            "clear match event pending",
            match_id,
            SETTLEMENT_COMPLETED,
        )

    # ── 终态 / 释放 ───────────────────────────────────────────────────────

    async def mark_order_terminal(
        self, market_id: int, order_id: int, status: int, updated_at_ms: int
    ) -> bool:
        if status not in (STATUS_CANCELED, STATUS_EXPIRED):
            raise errcode.PandoraError(errcode.ErrInternal, "invalid terminal status %d", status)
        return await self._exec_changed(
            self._r.for_market(market_id),
            "UPDATE auction_orders SET status = %s, release_pending = 1, match_pending = 0, "
            "escrow_verified = 0, reconcile_next_attempt_at_ms = 0, "
            "release_next_attempt_at_ms = 0, updated_at_ms = %s "
            "WHERE order_id = %s AND status IN (%s, %s)",
            "mark order terminal",
            status,
            updated_at_ms,
            order_id,
            STATUS_OPEN,
            STATUS_PARTIAL,
        )

    async def get_releasable_order(
        self, market_id: int, order_id: int
    ) -> tuple[OrderRecord | None, bool]:
        """只有不存在引用该订单的 PENDING 成交时,订单才可释放 escrow。"""
        row = await self._query_one(
            self._r.for_market(market_id),
            f"SELECT {ORDER_COLS_O} FROM auction_orders o "
            "WHERE o.order_id = %s AND o.release_pending = 1 AND o.status IN (%s, %s, %s) "
            "AND NOT EXISTS (SELECT 1 FROM auction_matches m WHERE m.settlement_status = %s "
            "AND (m.sell_order_id = o.order_id OR m.buy_order_id = o.order_id)) LIMIT 1",
            (order_id, STATUS_FILLED, STATUS_CANCELED, STATUS_EXPIRED, SETTLEMENT_PENDING),
            f"get releasable order {order_id}",
        )
        if not row:
            return None, False
        return _scan_order(row), True

    async def defer_order_release(
        self, market_id: int, order_id: int, next_attempt_at_ms: int
    ) -> bool:
        if next_attempt_at_ms <= 0:
            raise errcode.PandoraError(
                errcode.ErrInternal, "invalid order retry time %d", next_attempt_at_ms
            )
        return await self._exec_changed(
            self._r.for_market(market_id),
            "UPDATE auction_orders SET release_next_attempt_at_ms = %s "
            "WHERE order_id = %s AND release_pending = 1",
            "defer order release",
            next_attempt_at_ms,
            order_id,
        )

    async def clear_release_pending(self, market_id: int, order_id: int) -> bool:
        return await self._exec_changed(
            self._r.for_market(market_id),
            "UPDATE auction_orders SET release_pending = 0, release_next_attempt_at_ms = 0 "
            "WHERE auction_orders.order_id = %s AND auction_orders.release_pending = 1 "
            "AND auction_orders.status IN (%s, %s, %s) "
            "AND NOT EXISTS (SELECT 1 FROM auction_matches m WHERE m.settlement_status = %s "
            "AND (m.sell_order_id = auction_orders.order_id "
            "OR m.buy_order_id = auction_orders.order_id))",
            "clear release pending",
            order_id,
            STATUS_FILLED,
            STATUS_CANCELED,
            STATUS_EXPIRED,
            SETTLEMENT_PENDING,
        )

    async def repair_terminal_markers(self, market_id: int, order_id: int) -> bool:
        return await self._exec_changed(
            self._r.for_market(market_id),
            "UPDATE auction_orders SET release_pending = 1, match_pending = 0, "
            "escrow_verified = 0, reconcile_next_attempt_at_ms = 0, "
            "release_next_attempt_at_ms = 0 WHERE order_id = %s AND status IN (%s, %s, %s) "
            "AND (escrow_verified = 1 OR match_pending = 1)",
            "repair terminal markers",
            order_id,
            STATUS_FILLED,
            STATUS_CANCELED,
            STATUS_EXPIRED,
        )

    # ── 批量扫描(逐分片各取一批)──────────────────────────────────────

    async def _scan_orders_all_shards(
        self, sql: str, params: tuple, action: str
    ) -> list[OrderRecord]:
        out: list[OrderRecord] = []
        for pool in self._r.all():
            rows = await self._query_all(pool, sql, params, action)
            out.extend(_scan_order(r) for r in rows)
        return out

    async def _scan_matches_all_shards(
        self, sql: str, params: tuple, action: str
    ) -> list[MatchRecord]:
        out: list[MatchRecord] = []
        for pool in self._r.all():
            rows = await self._query_all(pool, sql, params, action)
            out.extend(_scan_match(r) for r in rows)
        return out

    async def list_pending_orders(self, limit: int) -> list[OrderRecord]:
        limit = limit if limit > 0 else 100
        return await self._scan_orders_all_shards(
            f"SELECT {ORDER_COLS} FROM auction_orders "
            "WHERE status = %s AND reconcile_next_attempt_at_ms <= %s "
            "ORDER BY reconcile_next_attempt_at_ms ASC, order_id ASC LIMIT %s",
            (STATUS_PENDING, now_ms(), limit),
            "list pending orders",
        )

    async def list_match_pending_orders(self, limit: int) -> list[OrderRecord]:
        limit = limit if limit > 0 else 100
        return await self._scan_orders_all_shards(
            f"SELECT {ORDER_COLS} FROM auction_orders "
            "WHERE match_pending = 1 AND escrow_verified = 1 AND status IN (%s, %s) "
            "AND reconcile_next_attempt_at_ms <= %s "
            "ORDER BY reconcile_next_attempt_at_ms ASC, order_id ASC LIMIT %s",
            (STATUS_OPEN, STATUS_PARTIAL, now_ms(), limit),
            "list match pending orders",
        )

    async def list_unverified_active_orders(self, limit: int) -> list[OrderRecord]:
        limit = limit if limit > 0 else 100
        return await self._scan_orders_all_shards(
            f"SELECT {ORDER_COLS} FROM auction_orders "
            "WHERE escrow_verified = 0 AND status IN (%s, %s) AND filled_quantity < quantity "
            "AND reconcile_next_attempt_at_ms <= %s "
            "ORDER BY reconcile_next_attempt_at_ms ASC, order_id ASC LIMIT %s",
            (STATUS_OPEN, STATUS_PARTIAL, now_ms(), limit),
            "list unverified active orders",
        )

    async def list_releasable_orders(self, limit: int) -> list[OrderRecord]:
        limit = limit if limit > 0 else 100
        return await self._scan_orders_all_shards(
            f"SELECT {ORDER_COLS_O} FROM auction_orders o "
            "WHERE o.release_pending = 1 AND o.status IN (%s, %s, %s) "
            "AND o.release_next_attempt_at_ms <= %s "
            "AND NOT EXISTS (SELECT 1 FROM auction_matches m WHERE m.settlement_status = %s "
            "AND (m.sell_order_id = o.order_id OR m.buy_order_id = o.order_id)) "
            "ORDER BY o.release_next_attempt_at_ms ASC, o.order_id ASC LIMIT %s",
            (
                STATUS_FILLED,
                STATUS_CANCELED,
                STATUS_EXPIRED,
                now_ms(),
                SETTLEMENT_PENDING,
                limit,
            ),
            "list releasable orders",
        )

    async def list_terminal_orders_for_repair(self, limit: int) -> list[OrderRecord]:
        limit = limit if limit > 0 else 100
        return await self._scan_orders_all_shards(
            f"SELECT {ORDER_COLS} FROM auction_orders "
            "WHERE status IN (%s, %s, %s) AND (escrow_verified = 1 OR match_pending = 1) "
            "ORDER BY order_id ASC LIMIT %s",
            (STATUS_FILLED, STATUS_CANCELED, STATUS_EXPIRED, limit),
            "list terminal marker repairs",
        )

    async def list_pending_matches(self, limit: int) -> list[MatchRecord]:
        limit = limit if limit > 0 else 100
        return await self._scan_matches_all_shards(
            f"SELECT {MATCH_COLS} FROM auction_matches "
            "WHERE settlement_status = %s AND settlement_next_attempt_at_ms <= %s "
            "ORDER BY settlement_next_attempt_at_ms ASC, matched_at_ms ASC, match_id ASC "
            "LIMIT %s",
            (SETTLEMENT_PENDING, now_ms(), limit),
            "list pending matches",
        )

    async def list_pending_match_events(self, limit: int) -> list[MatchRecord]:
        """只返回结算已完成、且关联终态订单没有 release_pending 的事件。

        ★ 这两个 NOT EXISTS 是**持久屏障**:终态 escrow 未释放前成交事件不可见。
        去掉它们不会报错,只会让下游先看到"成交了",而卖家的 escrow 还锁着。
        """
        limit = limit if limit > 0 else 100
        return await self._scan_matches_all_shards(
            f"SELECT {MATCH_COLS_M} FROM auction_matches m "
            "WHERE m.event_pending = 1 AND m.settlement_status = %s "
            "AND m.event_next_attempt_at_ms <= %s "
            "AND NOT EXISTS (SELECT 1 FROM auction_orders o WHERE o.order_id = m.sell_order_id "
            "AND o.market_id = m.market_id AND o.release_pending = 1 AND o.status IN (%s,%s,%s)) "
            "AND NOT EXISTS (SELECT 1 FROM auction_orders o WHERE o.order_id = m.buy_order_id "
            "AND o.market_id = m.market_id AND o.release_pending = 1 AND o.status IN (%s,%s,%s)) "
            "ORDER BY m.event_next_attempt_at_ms ASC, m.matched_at_ms ASC, m.match_id ASC "
            "LIMIT %s",
            (
                SETTLEMENT_COMPLETED,
                now_ms(),
                STATUS_FILLED,
                STATUS_CANCELED,
                STATUS_EXPIRED,
                STATUS_FILLED,
                STATUS_CANCELED,
                STATUS_EXPIRED,
                limit,
            ),
            "list pending match events",
        )

    # ── 列表查询 ──────────────────────────────────────────────────────────

    async def list_market_orders(self, market_id: int, side: int, limit: int) -> list[OrderRecord]:
        order_by = "price DESC, order_id ASC" if side == SIDE_BUY else "price ASC, order_id ASC"
        rows = await self._query_all(
            self._r.for_market(market_id),
            f"SELECT {ORDER_COLS} FROM auction_orders "
            "WHERE market_id = %s AND side = %s AND escrow_verified = 1 AND status IN (%s, %s) "
            f"ORDER BY {order_by} LIMIT %s",
            (market_id, side, STATUS_OPEN, STATUS_PARTIAL, limit),
            f"list market={market_id} side={side}",
        )
        return [_scan_order(r) for r in rows]

    async def list_owner_orders(
        self, owner_id: int, active_only: bool, cursor_order_id: int, limit: int
    ) -> list[OrderRecord]:
        """按全局 order_id DESC 游标分页;各分片最多读 limit 条后归并截断。

        ★ `status <> PENDING` 不能省:PENDING 是**内部恢复态**,proto 里占用
        UNSPECIFIED(0),暴露给客户端列表等于让玩家看到一张"状态未指定"的幽灵单。
        """
        limit = limit if limit > 0 else 51
        sql = (
            f"SELECT {ORDER_COLS} FROM auction_orders "
            f"WHERE owner_id = %s AND status <> {STATUS_PENDING}"
        )
        params: list[Any] = [owner_id]
        if active_only:
            sql += f" AND status IN ({STATUS_OPEN}, {STATUS_PARTIAL})"
        if cursor_order_id > 0:
            sql += " AND order_id < %s"
            params.append(cursor_order_id)
        sql += " ORDER BY order_id DESC LIMIT %s"
        params.append(limit)
        out: list[OrderRecord] = []
        for pool in self._r.all():
            rows = await self._query_all(pool, sql, tuple(params), f"list owner={owner_id}")
            out.extend(_scan_order(r) for r in rows)
        out.sort(key=lambda o: o.order_id, reverse=True)
        return out[:limit]

    async def list_owner_active_and_pending(self, owner_id: int, limit: int) -> list[OrderRecord]:
        limit = limit if limit > 0 else 201
        out: list[OrderRecord] = []
        for pool in self._r.all():
            rows = await self._query_all(
                pool,
                f"SELECT {ORDER_COLS} FROM auction_orders "
                "WHERE owner_id = %s AND status IN (%s, %s, %s) ORDER BY order_id ASC LIMIT %s",
                (owner_id, STATUS_PENDING, STATUS_OPEN, STATUS_PARTIAL, limit),
                f"list owner active slots {owner_id}",
            )
            out.extend(_scan_order(r) for r in rows)
        out.sort(key=lambda o: o.order_id)
        return out[:limit]

    async def list_expirable_orders(
        self, created_before_ms: int, limit: int
    ) -> list[OrderRecord]:
        limit = limit if limit > 0 else 200
        return await self._scan_orders_all_shards(
            f"SELECT {ORDER_COLS} FROM auction_orders "
            f"WHERE status IN ({STATUS_OPEN}, {STATUS_PARTIAL}) AND created_at_ms < %s "
            "ORDER BY created_at_ms ASC LIMIT %s",
            (created_before_ms, limit),
            f"list expirable before={created_before_ms}",
        )


# ── ClaimOrder 辅助 ───────────────────────────────────────────────────────────


def _claim_row_to_record(row: Sequence[Any] | None, owner_id: int) -> OrderRecord | None:
    """把 registry 行还原成 canonical 快照。

    ★ 必须读回**库内 canonical key 字面值**(row[0]),不能沿用本次请求的字面值:
    默认 MySQL collation 把大小写 / 尾空格视为等价,沿用会让 registry 与订单永久漂移。
    """
    if not row:
        return None
    key = row[0] if isinstance(row[0], str) else bytes(row[0]).decode()
    o = OrderRecord(
        owner_id=owner_id,
        idempotency_key=key,
        status=STATUS_PENDING,
        order_id=int(row[1]),
        market_id=int(row[2]),
        side=int(row[3]),
        item_config_id=int(row[4]),
        quantity=int(row[5]),
        price=int(row[6]),
        created_at_ms=int(row[7]),
    )
    o.updated_at_ms = o.created_at_ms
    return o


def _is_dup_error(exc: BaseException) -> bool:
    """1062 唯一键冲突判定。对应 Go 的 isDupErr(它也是按错误文本判的)。"""
    text = str(exc)
    return "1062" in text or "Duplicate entry" in text


def _valid_reservation(
    market_id: int, incoming: OrderRecord | None, resting: OrderRecord | None
) -> bool:
    """事务内复验全部成交不变量。逐条对应 Go 的 validReservation。

    少任何一条都是"成交发生了但不该发生":同 owner 自成交、跨物品成交、
    未验证 escrow 的订单参与撮合、价格不交叉却成交。全都不会报错。
    """
    if incoming is None or resting is None:
        return False
    if (
        incoming.market_id != market_id
        or resting.market_id != market_id
        or incoming.item_config_id != resting.item_config_id
        or incoming.side == resting.side
        or incoming.owner_id == resting.owner_id
        or not incoming.escrow_verified
        or not resting.escrow_verified
        or not _is_incoming_status(incoming.status)
        or not _is_active_status(resting.status)
        or remaining_of(incoming) <= 0
        or remaining_of(resting) <= 0
    ):
        return False
    if incoming.side != SIDE_BUY:  # incoming 是 SELL
        return resting.price >= incoming.price
    return resting.price <= incoming.price


def _build_reserved_match(
    match_id: int,
    incoming: OrderRecord,
    resting: OrderRecord,
    qty: int,
    price: int,
    matched_at_ms: int,
) -> MatchRecord:
    m = MatchRecord(
        match_id=match_id,
        market_id=incoming.market_id,
        item_config_id=incoming.item_config_id,
        quantity=qty,
        price=price,
        matched_at_ms=matched_at_ms,
        settlement_status=SETTLEMENT_PENDING,
    )
    if incoming.side != SIDE_BUY:  # incoming 是 SELL
        m.sell_order_id, m.seller_id = incoming.order_id, incoming.owner_id
        m.buy_order_id, m.buyer_id = resting.order_id, resting.owner_id
    else:
        m.buy_order_id, m.buyer_id = incoming.order_id, incoming.owner_id
        m.sell_order_id, m.seller_id = resting.order_id, resting.owner_id
    return m


def _advance_reserved_order(o: OrderRecord, updated_at_ms: int) -> None:
    o.updated_at_ms = updated_at_ms
    if remaining_of(o) == 0:
        o.status = STATUS_FILLED
        o.release_pending = True
        o.escrow_verified = False
        o.reconcile_next_attempt_at_ms = 0
    else:
        o.status = STATUS_PARTIAL


async def _update_reserved_order(cur, o: OrderRecord) -> None:  # noqa: ANN001
    await cur.execute(
        "UPDATE auction_orders SET filled_quantity = %s, status = %s, release_pending = %s, "
        "match_pending = %s, escrow_verified = %s, reconcile_next_attempt_at_ms = %s, "
        "release_next_attempt_at_ms = %s, updated_at_ms = %s WHERE order_id = %s",
        (
            o.filled_quantity,
            o.status,
            int(o.release_pending),
            int(o.match_pending),
            int(o.escrow_verified),
            o.reconcile_next_attempt_at_ms,
            o.release_next_attempt_at_ms,
            o.updated_at_ms,
            o.order_id,
        ),
    )
