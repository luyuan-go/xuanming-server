"""事务内的公共 SQL 底座 —— 对应 Go 侧 inventory_repo.go 里的
claimLedger / claimSaleLedger / updateLedgerResult / deduct*Tx / add*Tx。

★ 这里没有一行 Redis,也没有 Lua。inventory 的原子性**全压在 MySQL 上**:
    一个本地事务 + `SELECT ... FOR UPDATE` 锁行 + 唯一键 1062。
  真正的风险是**锁序**与 **1062 语义**,两类都不写在类型里、写错也不报错:

    锁序    并发的两笔操作若以相反顺序锁同一批行,InnoDB 直接死锁(1213)。
            所以所有多行路径一律"按 player_id 升序、同玩家内按 item_config_id 升序"
            —— 这个顺序在 Go 里是逐处手写的,这里逐处照抄,不许"顺手优化"。
    1062    唯一键冲突**不等于**"幂等成功"。它只说明"有人先插进去了",
            接下来必须去读那一行核对指纹;EnsureAuctionEscrow 更进一步:
            必须先**回滚**失败事务,再开新事务 FOR UPDATE 复核胜者提交的整行。
            把 1062 直接当成功,会出现两个事务都扣了活跃资产。

★ 事务边界:autocommit=True 的池 + 显式 `conn.begin()`(与 Go 的 database/sql 一致)。
  transaction() 正常退出即 COMMIT,抛异常即 ROLLBACK 后原样上抛。
  需要"读完就走、不提交"的地方(EnsureAuctionEscrow 的 1062 分支)用专门的
  哨兵异常退出,让 ROLLBACK 真的发生 —— 不回滚就去开第二个事务,
  第一个事务的行锁还攥在手里,自己等自己。
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import mysqlx
from pandorapy.services.inventory import currency
from pandorapy.services.inventory import fingerprint as fp
from pandorapy.services.inventory.models import LedgerSnapshot

# 事务内货币原语在 currency.py(对应 Go 的 data/currency.go)。这里再导出一次,
# 让调用方(repo.py / repo_instance.py)只 import 一个 rsql 就够 —— 与改造前
# add_gold_tx / deduct_gold_tx 就在本模块的调用形态保持一致。
add_currency_tx = currency.add_currency_tx
deduct_currency_tx = currency.deduct_currency_tx
read_balances_tx = currency.read_balances_tx
apply_currency_deltas_tx = currency.apply_currency_deltas_tx


@contextlib.asynccontextmanager
async def transaction(pool) -> AsyncIterator:  # noqa: ANN001
    """开一个事务并交出游标。正常退出 COMMIT,异常 ROLLBACK 后原样上抛。"""
    async with pool.acquire() as conn:
        await conn.begin()
        try:
            async with conn.cursor() as cur:
                yield cur
        except BaseException:
            # ★ 这里必须放行一切(含 CancelledError):回滚后原样上抛,不吞。
            with contextlib.suppress(Exception):
                await conn.rollback()
            raise
        else:
            await conn.commit()


class RollbackAndReturn(Exception):  # noqa: N818 —— 是控制流哨兵,不是错误
    """从事务里"读完就走"的哨兵:让 transaction() 走 ROLLBACK 分支再把值交出来。

    用途只有一处族:1062 之后必须先释放本事务的锁,才能开新事务去复核。
    """

    __slots__ = ("value",)

    def __init__(self, value=None) -> None:  # noqa: ANN001
        super().__init__("rollback-and-return")
        self.value = value


@contextlib.asynccontextmanager
async def transaction_or_value(pool) -> AsyncIterator:  # noqa: ANN001
    """transaction() 的变体:允许用 RollbackAndReturn 携值退出(回滚,不算错误)。

    调用方用 `holder.value` 取回携带的值(未触发哨兵时为 None)。
    """

    class _Holder:
        __slots__ = ("cur", "value", "rolled_back")

        def __init__(self) -> None:
            self.cur = None
            self.value = None
            self.rolled_back = False

    holder = _Holder()
    async with pool.acquire() as conn:
        await conn.begin()
        try:
            async with conn.cursor() as cur:
                holder.cur = cur
                yield holder
        except RollbackAndReturn as sentinel:
            with contextlib.suppress(Exception):
                await conn.rollback()
            holder.value = sentinel.value
            holder.rolled_back = True
        except BaseException:
            with contextlib.suppress(Exception):
                await conn.rollback()
            raise
        else:
            await conn.commit()


# ── 幂等流水(inventory_ledger)────────────────────────────────────────────

_INSERT_LEDGER = (
    "INSERT INTO inventory_ledger "
    "(player_id, idempotency_key, op, request_fingerprint, detail) VALUES (%s, %s, %s, %s, %s)"
)

# LEDGER_RESULT_COLUMNS 是结果快照四列的统一读取口径(与 Go 的 ledgerResultColumns 同序)。
#
# result_gold 是多币种改造之前的单币种列:新行不写它作为权威(仍**双写**以便旧副本可读),
# 老行的 result_currencies 为 NULL,此时把 result_gold 当作金币余额还原 ——
# 存量行的重放结果因此与升级前逐字节一致。
LEDGER_RESULT_COLUMNS = "result_remaining, result_gold, result_currencies, result_currency_delta"


def scan_ledger_result(
    remaining: int, legacy_gold: int, raw_balances: bytes | None, raw_delta: bytes | None
) -> LedgerSnapshot:
    """把结果四列还原成 LedgerSnapshot。对应 Go 的 scanLedgerResult。"""
    balances = currency.decode_balances(raw_balances)
    if not raw_balances and legacy_gold > 0:
        # 老行回退:升级前只可能有金币。
        balances = {currency.CURRENCY_GOLD: legacy_gold}
    return LedgerSnapshot(
        remaining=remaining, balances=balances, delta=currency.decode_balances(raw_delta)
    )


async def claim_ledger(
    cur, player_id: int, idempotency_key: str, op: str, fingerprint: str, detail: str
) -> tuple[bool, LedgerSnapshot]:  # noqa: ANN001
    """在事务里声明幂等键 + 记录请求指纹。对应 Go 的 claimLedger。

    返回 (already, snap):
      - 首次:插入成功 → already=False,空快照
      - 重复(uk 1062):读回已存指纹 + 结果快照
          指纹不一致 → ErrInventoryIdempotencyConflict(fail-closed 留证)
          指纹一致   → already=True + 首次结果快照(回放)

    ★ 为什么回放的是**首次结果快照**而不是重新读当前状态:
      重新读会随后续操作漂移 —— 玩家用了 3 个药,重试那次请求会看到"剩 7 个"
      而首次返回的是"剩 10 个",客户端两次收到不同答案却都是"成功"。
    """
    try:
        await cur.execute(_INSERT_LEDGER, (player_id, idempotency_key, op, fingerprint, detail))
    except Exception as exc:  # noqa: BLE001
        if not mysqlx.is_duplicate_entry(exc):
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "insert ledger player=%d key=%s: %s",
                player_id,
                idempotency_key,
                exc,
            ) from exc
    else:
        return False, LedgerSnapshot()

    await cur.execute(
        f"SELECT request_fingerprint, {LEDGER_RESULT_COLUMNS} FROM inventory_ledger "
        "WHERE player_id = %s AND idempotency_key = %s LIMIT 1",
        (player_id, idempotency_key),
    )
    row = await cur.fetchone()
    if row is None:
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "read ledger player=%d key=%s: row vanished after duplicate",
            player_id,
            idempotency_key,
        )
    stored_fp = str(row[0])
    if stored_fp != fingerprint:
        # 同键不同请求内容:发放/扣减/结算的完整性冲突(防 key 复用串改账),fail-closed 留证。
        plog.get().warning(
            "inventory_idempotency_conflict",
            player_id=player_id,
            idempotency_key=idempotency_key,
            op="ledger",
        )
        raise errcode.PandoraError(
            errcode.ErrInventoryIdempotencyConflict,
            "idempotency_key reused for different request player=%d key=%s",
            player_id,
            idempotency_key,
        )
    return True, scan_ledger_result(int(row[1] or 0), int(row[2] or 0), row[3], row[4])


async def claim_sale_ledger(
    cur,  # noqa: ANN001
    player_id: int,
    idempotency_key: str,
    fingerprint: str,
    detail: str,
    *,
    op: str,
    item_config_id: int = 0,
    count: int = 0,
    instance_id: int = 0,
) -> tuple[bool, LedgerSnapshot]:
    """售价热更安全的幂等声明。对应 Go 的 claimSaleLedger。

    新行只存**客户端意图**指纹(不含 gold);旧行(升级前提交的,指纹里含首次成交价)
    只有在 op、detail 里的完整首次意图、以及由首次 gold 重算的旧指纹**全部一致**时
    才允许回放。

    ★ 为什么不能拿"当前售价"去重算旧指纹:配置表热更改价之后,同一笔重试就会
      算出不同的旧指纹 → 被判成 key 复用冲突。所以价格必须从旧行的 detail 里恢复。
    """
    try:
        await cur.execute(_INSERT_LEDGER, (player_id, idempotency_key, op, fingerprint, detail))
    except Exception as exc:  # noqa: BLE001
        if not mysqlx.is_duplicate_entry(exc):
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "insert sale ledger player=%d key=%s: %s",
                player_id,
                idempotency_key,
                exc,
            ) from exc
    else:
        return False, LedgerSnapshot()

    await cur.execute(
        f"SELECT op, request_fingerprint, detail, {LEDGER_RESULT_COLUMNS} "
        "FROM inventory_ledger WHERE player_id = %s AND idempotency_key = %s LIMIT 1",
        (player_id, idempotency_key),
    )
    row = await cur.fetchone()
    if row is None:
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "read sale ledger player=%d key=%s: row vanished after duplicate",
            player_id,
            idempotency_key,
        )
    stored_op, stored_fp, stored_detail = str(row[0]), str(row[1]), str(row[2] or "")
    snap_remaining, legacy_gold = int(row[3] or 0), int(row[4] or 0)
    raw_balances, raw_delta = row[5], row[6]

    matched = stored_op == op and stored_fp == fingerprint
    if not matched and stored_op == op:
        matched = _legacy_sale_ledger_matches(
            stored_fp,
            stored_detail,
            op=op,
            item_config_id=item_config_id,
            count=count,
            instance_id=instance_id,
        )
    if not matched:
        plog.get().warning(
            "inventory_idempotency_conflict",
            player_id=player_id,
            idempotency_key=idempotency_key,
            op=op,
        )
        raise errcode.PandoraError(
            errcode.ErrInventoryIdempotencyConflict,
            "idempotency_key reused for different sale request player=%d key=%s",
            player_id,
            idempotency_key,
        )
    snap = scan_ledger_result(snap_remaining, legacy_gold, raw_balances, raw_delta)
    if not snap.delta and legacy_gold > 0 and not raw_delta:
        # 老行没有 delta 列。出售的**首次入账额**可以从 detail 里那份"人读摘要"恢复:
        # 该格式由服务端生成、且刚被 _legacy_sale_ledger_matches 严格校验过完整性,
        # 这里只是把已验证的数字取出来,不是把 detail 当业务字段用。
        #
        # 少了这一段会怎样:老行重放时 earned 恒 0 —— 客户端第二次收到"出售成功,获得 0 金币",
        # 而首次那笔其实入账了。两次响应都是成功却互相矛盾。
        earned = _legacy_sale_earned(
            stored_detail, op=op, item_config_id=item_config_id, count=count,
            instance_id=instance_id,
        )
        if earned is not None:
            snap.delta = {currency.CURRENCY_GOLD: earned}
    return True, snap


def _legacy_sale_earned(
    detail: str,
    *,
    op: str,
    item_config_id: int,
    count: int,
    instance_id: int,
) -> int | None:
    """从升级前流水的 detail 恢复首次出售入账额(仅金币)。对应 Go 的 legacySaleEarned。

    只在 _legacy_sale_ledger_matches 已判定意图完全一致后调用。
    """
    if op == "sell":
        parsed = _parse_kv(detail, "sell item={} count={} gold={}")
        if parsed is None:
            return None
        _item_id, _cnt, gold = parsed
        return gold if gold > 0 else None
    if op == "sell_inst":
        parsed = _parse_kv(detail, "sell instance={} item={} gold={}")
        if parsed is None:
            return None
        _inst_id, _item_id, gold = parsed
        return gold if gold > 0 else None
    return None


def _legacy_sale_ledger_matches(
    stored_fp: str,
    detail: str,
    *,
    op: str,
    item_config_id: int,
    count: int,
    instance_id: int,
) -> bool:
    """识别升级前提交的售卖行。对应 Go 的 legacySaleLedgerMatches。

    ★ detail 必须**完全等于**用解析出的字段重新格式化的结果(Go 用 Sscanf + 回拼比对)。
      只做前缀匹配的话,一条被拼接了额外内容的 detail 也会通过 —— 而 detail 是
      唯一能恢复"首次成交价"的地方,它一旦可被构造,旧指纹校验就形同虚设。
    """
    if op == "sell":
        parsed = _parse_kv(detail, "sell item={} count={} gold={}")
        if parsed is None:
            return False
        item_id, cnt, gold = parsed
        if detail != f"sell item={item_id} count={cnt} gold={gold}":
            return False
        if item_id != item_config_id or cnt != count or gold <= 0:
            return False
        return stored_fp == fp.legacy_sell_fingerprint(item_id, cnt, gold)
    if op == "sell_inst":
        parsed = _parse_kv(detail, "sell instance={} item={} gold={}")
        if parsed is None:
            return False
        inst_id, item_id, gold = parsed
        if detail != f"sell instance={inst_id} item={item_id} gold={gold}":
            return False
        if inst_id != instance_id or item_id != item_config_id or gold <= 0:
            return False
        return stored_fp == fp.legacy_sell_instance_fingerprint(inst_id, item_id, gold)
    return False


def _parse_kv(text: str, template: str) -> tuple[int, ...] | None:
    """按 "字面量{}字面量{}..." 模板抽出整数(等价于 Go 的 Sscanf 三个 %d)。

    解析失败一律返回 None —— 解析不出来就当"不是旧行",走冲突分支,fail-closed。
    """
    parts = template.split("{}")
    if not text.startswith(parts[0]):
        return None
    rest = text[len(parts[0]) :]
    out: list[int] = []
    for sep in parts[1:]:
        if sep:
            idx = rest.find(sep)
            if idx < 0:
                return None
            token, rest = rest[:idx], rest[idx + len(sep) :]
        else:
            token, rest = rest, ""
        try:
            out.append(int(token))
        except ValueError:
            return None
    if rest:
        return None
    return tuple(out)


async def update_ledger_result(
    cur, player_id: int, idempotency_key: str, remaining: int, balances, delta  # noqa: ANN001
) -> None:
    """把首次执行的结果快照写回流水(供后续幂等回放返回稳定值)。对应 Go 的 updateLedgerResult。

    ★ result_gold 与 result_currencies **双写**:前者是多币种改造之前的单币种列,
      滚动升级窗口里旧副本仍会去读它(§9.21 共存窗口)。双写代价是一个整数列,
      换来"新副本写的行,旧副本也能正确重放金币结果"。金币之外的币种旧副本本就
      理解不了,那种流水只可能由新功能产生,旧副本不会去重放。
      contract 阶段(确认无旧副本后)再删列并停止双写。
    """
    raw_balances = currency.encode_balances(balances)
    raw_delta = currency.encode_balances(delta)
    legacy_gold = currency.balances_get(balances, currency.CURRENCY_GOLD)
    try:
        await cur.execute(
            "UPDATE inventory_ledger SET result_remaining = %s, result_gold = %s, "
            "result_currencies = %s, result_currency_delta = %s "
            "WHERE player_id = %s AND idempotency_key = %s",
            (remaining, legacy_gold, raw_balances, raw_delta, player_id, idempotency_key),
        )
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "update ledger result player=%d key=%s: %s",
            player_id,
            idempotency_key,
            exc,
        ) from exc


# ── 道具堆叠(货币在 currency.py)──────────────────────────────────────────

_UPSERT_ITEM = (
    "INSERT INTO player_items (player_id, item_config_id, count) VALUES (%s, %s, %s) "
    "ON DUPLICATE KEY UPDATE count = count + VALUES(count)"
)


async def add_item_tx(cur, player_id: int, item_config_id: int, n: int) -> None:  # noqa: ANN001
    try:
        await cur.execute(_UPSERT_ITEM, (player_id, item_config_id, n))
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "add item player=%d item=%d: %s",
            player_id,
            item_config_id,
            exc,
        ) from exc


async def deduct_item_tx(cur, player_id: int, item_config_id: int, n: int) -> int:  # noqa: ANN001
    """锁道具行并扣减 count。返回扣减后剩余数量。

      行不存在 → ErrInventoryItemNotFound
      count < n → ErrInventoryInsufficient

    ★ 扣空即删行(2026-07-22 用户要求):不留 count=0 死行。
      读侧本就过滤 count>0,留行只会让 player_items 无界堆积;
      后续再发放同 config 走 upsert 重建行,行为不变。
    """
    try:
        await cur.execute(
            "SELECT count FROM player_items WHERE player_id = %s AND item_config_id = %s FOR UPDATE",
            (player_id, item_config_id),
        )
        row = await cur.fetchone()
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "lock item player=%d item=%d: %s",
            player_id,
            item_config_id,
            exc,
        ) from exc
    if row is None:
        raise errcode.PandoraError(
            errcode.ErrInventoryItemNotFound,
            "item not found player=%d item=%d",
            player_id,
            item_config_id,
        )
    have = int(row[0])
    if have < n:
        raise errcode.PandoraError(
            errcode.ErrInventoryInsufficient,
            "insufficient item player=%d item=%d need=%d have=%d",
            player_id,
            item_config_id,
            n,
            have,
        )
    remaining = have - n
    try:
        if remaining == 0:
            await cur.execute(
                "DELETE FROM player_items WHERE player_id = %s AND item_config_id = %s",
                (player_id, item_config_id),
            )
        else:
            await cur.execute(
                "UPDATE player_items SET count = %s WHERE player_id = %s AND item_config_id = %s",
                (remaining, player_id, item_config_id),
            )
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "deduct item player=%d item=%d: %s",
            player_id,
            item_config_id,
            exc,
        ) from exc
    return remaining
