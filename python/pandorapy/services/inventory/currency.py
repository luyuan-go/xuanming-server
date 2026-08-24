"""多币种货币数据层原语 —— 对应 Go 侧 internal/data/currency.go(2026-08-22)。

库表:`player_wallet(player_id, currency_kind) → amount BIGINT UNSIGNED`。
一个玩家每种货币至多一行;**没有行 = 该币种余额为 0**(不预建行,不占空间)。

# 为什么金额是无符号,以及无符号带来的三条硬纪律

CLAUDE.md §5.12:语义上不可能为负的整数默认无符号。货币余额恒 >= 0,列因此是
BIGINT UNSIGNED。但"存储无符号"会把原本静默的 bug 换一种形态,必须在**应用层**堵死,
不能指望 SQL:

    ① 减法下溢:`UPDATE ... SET amount = amount - %s` 在 UNSIGNED 列上,严格模式抛
       Error 1690(out of range),非严格模式**截断成 0** —— 后者等于把"扣款失败"
       变成"余额清零",是不可接受的静默数据损坏。因此本文件一律
       **先 FOR UPDATE 锁行读出余额、在 Python 里比较、再写绝对值**,
       SQL 里不出现 `amount - %s`。
    ② 加法溢出:同理会在列上溢出。所以设 MAX_CURRENCY_AMOUNT 硬上限,
       越界返回 ErrInventoryCurrencyOverflow 而不是回绕。
    ③ 乘法溢出:单价 × 数量在算出总价的那一步就可能溢出,必须用 safe_mul_uint64,
       不能等到写库才发现。

★ Python 与 Go 的差别正在这里,而且是**更危险**的方向:Go 的 uint64 至少保证
  "负数不可能存在";Python 的 int 是无限精度,一个负的 amount 会**一路畅通地写进库**,
  由列去截断。所以每一条上面的纪律在这里都必须写成显式判断 ——
  类型系统在 Python 侧一点忙都帮不上。

# 防死锁

与本包既有纪律一致:跨玩家的行锁按 player_id 升序获取;同一玩家的多币种行锁按
currency_kind 升序获取。P2P 交易 / 拍卖结算会同时锁买卖双方,顺序错就互等。
"""

from __future__ import annotations

from pandora.common.v1 import currency_pb2 as _cur_pb2

from pandorapy import errcode

# ── 币种(数值从 pb 引用,**不手抄**;本仓刚修完 13 处手抄错位)──

CURRENCY_KIND_UNSPECIFIED: int = _cur_pb2.CURRENCY_KIND_UNSPECIFIED
CURRENCY_GOLD: int = _cur_pb2.CURRENCY_KIND_GOLD
CURRENCY_DIAMOND: int = _cur_pb2.CURRENCY_KIND_DIAMOND
CURRENCY_HONOR: int = _cur_pb2.CURRENCY_KIND_HONOR

_KNOWN_KINDS = (CURRENCY_GOLD, CURRENCY_DIAMOND, CURRENCY_HONOR)

# MAX_CURRENCY_AMOUNT 是**单玩家单币种**余额硬上限(与 Go 的 MaxCurrencyAmount 同值)。
#
# 取 2^62 而不是 2^64-1:留出 2 位余量,使"余额 + 单笔入账"这类中间量在 uint64 里
# 永远算得下,不必在每个加法点再判一次中间溢出。4.6e18 远超任何合理经济体量;
# 真触顶只可能是发放逻辑失控或有人直写库,此时拒绝入账比静默回绕正确。
MAX_CURRENCY_AMOUNT: int = 1 << 62

# CURRENCY_BALANCES_MAX_BYTES 是 inventory_ledger.result_currencies 的写入侧字节闸
# (§9.24 深度上限)。币种数是枚举有界的(个位数),单条 CurrencyAmount 编码 ≤ 12 字节;
# 取列容量 VARBINARY(256),达到即说明枚举失控或有人直写。
CURRENCY_BALANCES_MAX_BYTES = 256

# 钱包表名。**不是** legacy 的 player_currency(单列 gold,只读待 contract 删除)。
WALLET_TABLE = "player_wallet"

# Balances 是 {kind: amount} 的普通 dict(只含非零项)。
# 不另立类型:Go 侧 `type Balances map[CurrencyKind]uint64` 也只是 map 的别名,
# 包一层 class 只会让 == 比较、dict 字面量构造在测试里全都要改写。
Balances = dict


def balances_get(balances, kind: int) -> int:  # noqa: ANN001
    """取某币种余额(不存在 = 0)。对应 Go 的 Balances.Get。"""
    if not balances:
        return 0
    return int(balances.get(kind, 0))


def balances_sorted(balances) -> list[tuple[int, int]]:  # noqa: ANN001
    """按 kind 升序的 (kind, amount) 列表,只含非零项。对应 Go 的 Balances.Sorted。

    ★ 顺序必须稳定:它同时决定落库 pb 字节与下行协议顺序,
      顺序一漂移,幂等指纹跟着漂移(同一笔发放算出两个指纹 → 重试被判冲突)。
    """
    if not balances:
        return []
    return [(k, int(v)) for k, v in sorted(balances.items()) if int(v) != 0]


def balances_to_amounts(balances) -> list[_cur_pb2.CurrencyAmount]:  # noqa: ANN001
    """转成按 kind 升序的 pb 列表(只含非零项)。"""
    return [_cur_pb2.CurrencyAmount(kind=k, amount=v) for k, v in balances_sorted(balances)]


def validate_currency_kind(kind: int) -> None:
    """校验币种合法。对应 Go 的 ValidateCurrencyKind。

    UNSPECIFIED 与未知值一律拒,**绝不回退成金币**:静默回退会让配错表的商品
    按金币扣钱,是不可观测的经济事故(fail-closed,§9.22)。
    """
    if int(kind) not in _KNOWN_KINDS:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "unsupported currency kind: %d", int(kind)
        )


def safe_mul_uint64(a: int, b: int) -> tuple[int, bool]:
    """溢出安全乘法(上界 MAX_CURRENCY_AMOUNT)。对应 Go 的 safeMulUint64。

    ★ 负数在这里必须显式拒:Go 的入参是 uint64(负数不可能),Python 没有这个保证,
      而 `-1 * 5 = -5` 既不溢出也"算得出来",会一路写进 UNSIGNED 列被截断成 0。
    """
    if a < 0 or b < 0:
        return 0, False
    if a == 0 or b == 0:
        return 0, True
    if a > MAX_CURRENCY_AMOUNT // b:
        return 0, False
    return a * b, True


def safe_add_uint64(a: int, b: int) -> tuple[int, bool]:
    """溢出安全加法(上界是 MAX_CURRENCY_AMOUNT 而不是 2^64-1)。对应 Go 的 safeAddUint64。"""
    if a < 0 or b < 0:
        return 0, False
    if a > MAX_CURRENCY_AMOUNT - b:
        return 0, False
    return a + b, True


def safe_mul_currency(unit_price: int, count: int) -> tuple[int, bool]:
    """给 biz 层用的溢出安全乘法(单价 × 数量 → 总价)。对应 Go 的 SafeMulCurrency。"""
    return safe_mul_uint64(unit_price, count)


def encode_balances(balances) -> bytes | None:  # noqa: ANN001
    """余额 → pb 二进制(写 inventory_ledger.result_currencies)。

    空余额 → None(列写 NULL),不写一个空 message:省字节,也便于肉眼区分
    "没记"与"记了全零"。
    """
    sorted_items = balances_sorted(balances)
    if not sorted_items:
        return None
    rec = _cur_pb2.CurrencyBalancesStorageRecord(
        balances=[_cur_pb2.CurrencyAmount(kind=k, amount=v) for k, v in sorted_items]
    )
    raw = rec.SerializeToString()
    if len(raw) > CURRENCY_BALANCES_MAX_BYTES:
        # 写入侧字节闸:非严格 sql_mode 下超长会被静默截断成半截 pb,
        # 重放时解不出来 → 幂等结果凭空变形。宁可拒写。
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "currency balances payload too large: %d > %d bytes",
            len(raw),
            CURRENCY_BALANCES_MAX_BYTES,
        )
    return raw


def decode_balances(raw: bytes | None) -> dict[int, int]:
    """pb 二进制 → 余额快照。对应 Go 的 DecodeBalances。

    NULL / 空 → 空余额(**不是错误**:老流水行或纯道具操作本就没有货币结果)。
    """
    if not raw:
        return {}
    rec = _cur_pb2.CurrencyBalancesStorageRecord()
    try:
        rec.ParseFromString(bytes(raw))
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "unmarshal currency balances: %s", exc
        ) from exc
    out: dict[int, int] = {}
    for it in rec.balances:
        if it.amount == 0:
            continue
        out[int(it.kind)] = int(it.amount)
    return out


def describe_balances(balances) -> str:  # noqa: ANN001
    """人读摘要(写进 ledger.detail / 日志,**也进指纹**)。对应 Go 的 describeBalances。

    ★ 格式是跨实现契约:非金币发放的指纹字符串里含它(见 fingerprint.grant_fingerprint),
      多一个空格就是两个不同的指纹。
    """
    items = balances_sorted(balances)
    if not items:
        return "none"
    return ",".join(f"{k}:{v}" for k, v in items)


# ── 事务内 SQL(全部要求调用方已在 rsql.transaction 里)──────────────────


async def read_balances_tx(cur, player_id: int) -> dict[int, int]:  # noqa: ANN001
    """在事务里读某玩家全部币种余额(无行 → 空)。对应 Go 的 readBalancesTx。"""
    try:
        await cur.execute(
            f"SELECT currency_kind, amount FROM {WALLET_TABLE} "
            "WHERE player_id = %s ORDER BY currency_kind",
            (player_id,),
        )
        rows = await cur.fetchall()
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "read balances player=%d: %s", player_id, exc
        ) from exc
    out: dict[int, int] = {}
    for row in rows or ():
        kind, amount = int(row[0]), int(row[1])
        if amount == 0:
            continue
        out[kind] = amount
    return out


async def lock_balance_tx(cur, player_id: int, kind: int) -> int:  # noqa: ANN001
    """锁定并读取某玩家某币种的余额行。对应 Go 的 lockBalanceTx。

    ★ 先做一次 no-op upsert 把行**物化**再锁:UNSIGNED 列上不能靠 `amount = amount + %s`,
      必须锁行读出来在代码里算;而 `SELECT ... FOR UPDATE` 对**不存在的行**不加行锁
      (TiDB 无 gap 锁,MySQL 也只加间隙锁),两个并发事务会双双看到"无行"然后各自
      INSERT,一个拿到 duplicate key 而不是被串行化。no-op upsert 让两者在同一行上排队。
    """
    try:
        await cur.execute(
            f"INSERT INTO {WALLET_TABLE} (player_id, currency_kind, amount) VALUES (%s, %s, 0) "
            "ON DUPLICATE KEY UPDATE player_id = player_id",
            (player_id, int(kind)),
        )
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "materialize currency row player=%d kind=%d: %s",
            player_id,
            int(kind),
            exc,
        ) from exc
    try:
        await cur.execute(
            f"SELECT amount FROM {WALLET_TABLE} "
            "WHERE player_id = %s AND currency_kind = %s FOR UPDATE",
            (player_id, int(kind)),
        )
        row = await cur.fetchone()
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "lock currency player=%d kind=%d: %s",
            player_id,
            int(kind),
            exc,
        ) from exc
    if row is None:
        # 刚 upsert 过必然有行;读不到只能是并发 DDL / 直写库。fail-closed。
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "lock currency player=%d kind=%d: row vanished after materialize",
            player_id,
            int(kind),
        )
    return int(row[0])


async def write_balance_tx(cur, player_id: int, kind: int, amount: int) -> None:  # noqa: ANN001
    """写**绝对**余额(调用方已在锁内算好,SQL 里不做算术)。对应 Go 的 writeBalanceTx。"""
    if amount < 0 or amount > MAX_CURRENCY_AMOUNT:
        # Go 侧靠 uint64 + 上限闸保证走不到这里;Python 没有类型保护,
        # 所以最后一道闸必须显式写出来 —— 负数写进 UNSIGNED 列会被静默截断成 0。
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "refuse to write out-of-range balance player=%d kind=%d amount=%d",
            player_id,
            int(kind),
            amount,
        )
    try:
        await cur.execute(
            f"UPDATE {WALLET_TABLE} SET amount = %s WHERE player_id = %s AND currency_kind = %s",
            (amount, player_id, int(kind)),
        )
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "write currency player=%d kind=%d: %s",
            player_id,
            int(kind),
            exc,
        ) from exc


async def add_currency_tx(cur, player_id: int, kind: int, n: int) -> int:  # noqa: ANN001
    """在事务里给玩家某币种加钱,返回加后余额。对应 Go 的 addCurrencyTx。

    n == 0 时不写库(纯道具发放的常见情形),直接读回当前值。
    越过 MAX_CURRENCY_AMOUNT → ErrInventoryCurrencyOverflow(拒绝入账,**不回绕**)。
    """
    validate_currency_kind(kind)
    if n < 0:
        # Go 的入参是 uint64,这条分支在那边不存在;Python 必须显式拒 ——
        # 负数"加钱"就是没有比较保护的扣钱。
        raise errcode.PandoraError(
            errcode.ErrInvalidArg,
            "currency add amount must not be negative player=%d kind=%d n=%d",
            player_id,
            int(kind),
            n,
        )
    have = await lock_balance_tx(cur, player_id, kind)
    if n == 0:
        return have
    nxt, ok = safe_add_uint64(have, n)
    if not ok:
        raise errcode.PandoraError(
            errcode.ErrInventoryCurrencyOverflow,
            "currency overflow player=%d kind=%d have=%d add=%d max=%d",
            player_id,
            int(kind),
            have,
            n,
            MAX_CURRENCY_AMOUNT,
        )
    await write_balance_tx(cur, player_id, kind, nxt)
    return nxt


async def deduct_currency_tx(cur, player_id: int, kind: int, n: int) -> int:  # noqa: ANN001
    """在事务里扣某币种,返回扣后余额。对应 Go 的 deductCurrencyTx。

    余额不足 → ErrInventoryInsufficient(**先比较后相减**,减法永远不下溢)。
    """
    validate_currency_kind(kind)
    if n < 0:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg,
            "currency deduct amount must not be negative player=%d kind=%d n=%d",
            player_id,
            int(kind),
            n,
        )
    have = await lock_balance_tx(cur, player_id, kind)
    if n == 0:
        return have
    if have < n:
        raise errcode.PandoraError(
            errcode.ErrInventoryInsufficient,
            "insufficient currency player=%d kind=%d need=%d have=%d",
            player_id,
            int(kind),
            n,
            have,
        )
    nxt = have - n
    await write_balance_tx(cur, player_id, kind, nxt)
    return nxt


async def apply_currency_deltas_tx(cur, player_id: int, deltas) -> dict[int, int]:  # noqa: ANN001
    """按 kind 升序批量加钱(多币种发放)。返回加后的**全量**余额快照。

    ★ 按 kind 升序是防死锁纪律的一部分:同一玩家的多币种行锁必须固定顺序获取。
      dict 的迭代顺序是插入序,不是键序 —— 直接 `for k in deltas` 会让两笔
      币种集合相同但构造顺序不同的并发发放以相反顺序锁行(1213)。
    """
    for kind, amount in balances_sorted(deltas):
        await add_currency_tx(cur, player_id, kind, amount)
    return await read_balances_tx(cur, player_id)
