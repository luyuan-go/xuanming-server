"""login 服务的数据层 —— 对应 Go 侧 `services/account/login/internal/data/`。

    MySQL  pandora_account.accounts / account_devices / account_bans /
           account_roles / player_roles / player_session_generations / player_no_counter
    Redis  pandora:sess:<player_id>     hash,TTL = session TTL     会话权威
           pandora:ticket:<jti>         string,TTL = ds_ticket_ttl  DSTicket 防重放
           pandora:rl:login:*           登录失败 Quota(账号 + IP)

★ 连接池必须以 **autocommit=True** 建(见 main.py):与 Go 的 `database/sql` 默认语义
  一致,也是本文件的前提。需要原子性的地方全部显式 `conn.begin()`;若把池建成
  autocommit=False,MySQL 会在**每条 SELECT** 上隐式开事务,单条读也会长期持有
  read view —— 而 `SweepPlayerNo` 的正确性恰恰依赖"取批扫描发生在拿计数器锁之后、
  且看得到锁前驱批次的提交"(见 sweep_player_no 的说明)。

★ SQL 逐字对着 Go 抄,包括 `WHERE` 条件与 `RowsAffected` 复核。改写"等价" SQL 是
  这一层最危险的自由发挥:`UPDATE ... AND player_no IS NULL` 少一个条件,
  第二写者就检测不出来,整批号会静默重发。
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime as _dt
import hashlib
import time

from pandorapy import dbguard
from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import redisx

# ── 常量(与 Go 逐字同值)────────────────────────────────────────────────────

# 单事务编号行数上限(对齐仓库批处理 LIMIT 500 惯例,控制事务大小)。
PLAYER_NO_BATCH_SIZE = 500
# 水位安全滞后:INSERT 打戳(语句执行)到行可见(提交)存在间隙,补号只处理打戳
# 超过该滞后的行 —— 少了它,「打戳早、提交可见晚」的行会在编号之后才出现,
# 编号全序与 created_at 全序不再一致(设计文档 §3.3)。
PLAYER_NO_WATERMARK_LAG_SEC = 10

# 无能力墓碑哨兵值:登出与失败 Login 的条件 fencing 共用。非 uuid 格式,永不与真 jti 碰撞。
SESSION_TOMBSTONE_JTI = "logged-out"

# PersistSessionJTI 遇并发错误时的有界重试次数(Go: persistMaxAttempts)。
# 真 MySQL 实测:并发首登时 `SELECT ... FOR UPDATE` 命中**不存在的行**加的是 gap 锁,
# 多个首登事务互等 INSERT intention 锁 → 1213 死锁。3 次足够:死锁一方被回滚重放时
# 对手已提交、行已存在,FOR UPDATE 退化为普通行锁。
PERSIST_MAX_ATTEMPTS = 3

_MYSQL_ERR_LOCK_WAIT_TIMEOUT = 1205
_MYSQL_ERR_DEADLOCK = 1213


def now_ms() -> int:
    return int(time.time() * 1000)


class CommitAmbiguousError(RuntimeError):
    """COMMIT 结果不确定:MySQL 可能已提交,但回包丢了。

    这是唯一必须与「确定没提交」区分开的失败:确定没提交时直接失败即一致;
    不确定时若直接失败,MySQL 可能已经带着一个**从未交付给客户端的 jti** 推进了代际,
    把仍在线的上一代会话错误地 fence 出去。调用方必须用本次 jti 读回权威把它**判定**掉,
    而不是猜(§9.22)。
    """


def _is_retryable_tx_error(exc: BaseException) -> bool:
    """1213 死锁 / 1205 锁等待超时:InnoDB 已回滚本事务,整段重放安全。"""
    args = getattr(exc, "args", ())
    if args and isinstance(args[0], int):
        return args[0] in (_MYSQL_ERR_DEADLOCK, _MYSQL_ERR_LOCK_WAIT_TIMEOUT)
    text = str(exc)
    return "1213" in text or "1205" in text


def _is_dup_error(exc: BaseException) -> bool:
    """粗略判断 MySQL 唯一键冲突,不依赖 driver 强类型(与 Go 的 isDupErr 同口径)。"""
    args = getattr(exc, "args", ())
    if args and isinstance(args[0], int) and args[0] == 1062:
        return True
    text = str(exc)
    return "1062" in text or "Duplicate entry" in text


# ── 值对象 ───────────────────────────────────────────────────────────────────


@dataclasses.dataclass(slots=True)
class AccountIdentity:
    """账号名解析出来的账号身份。

    account_id 与 player_id 是**两个层面**的东西:前者是账号身份(一个账号恒一个),
    后者在本结构里是 accounts 表的历史主键,语义已退化为「该账号的主角色指针」。
    account_id == 0 表示存量行该列仍是 NULL(旧二进制注册),需要 backfill_account_id。
    """

    account_id: int = 0
    player_id: int = 0
    password_hash: str = ""


@dataclasses.dataclass(slots=True)
class AccountRole:
    """角色归属台账的一行(account_roles)。status 0=normal,1=deleted(软删)。"""

    player_id: int = 0
    account_id: int = 0
    slot: int = 0
    role_name: str = ""
    status: int = 0
    # 该角色最近一次真正进入游戏(unix ms);0 = 从未进过。
    last_login_at_ms: int = 0
    created_at_ms: int = 0


@dataclasses.dataclass(slots=True)
class SessionGenerationLease:
    """一次 persist_session_jti 的分配结果。首登 generation=1。"""

    generation: int = 0


def _dt_to_ms(value) -> int:  # noqa: ANN001 —— driver 返回 datetime 或 None
    if value is None:
        return 0
    if isinstance(value, _dt.datetime):
        # 列是 UTC_TIMESTAMP() 写的 naive datetime;按 UTC 解释(与 Go 的 UnixMilli 同)。
        if value.tzinfo is None:
            value = value.replace(tzinfo=_dt.UTC)
        return int(value.timestamp() * 1000)
    return 0


# ── MySQL:账号仓储 ──────────────────────────────────────────────────────────


class MySQLAccountRepo:
    """pandora_account.accounts / account_devices / account_bans。"""

    __slots__ = ("_pool",)

    def __init__(self, pool) -> None:  # noqa: ANN001
        self._pool = pool

    async def find_by_account(self, account: str) -> AccountIdentity:
        q = "SELECT player_id, account_id, password_hash FROM accounts WHERE account = %s LIMIT 1"
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(q, (account,))
            row = await cur.fetchone()
        if row is None:
            raise errcode.PandoraError(
                errcode.ErrLoginAccountNotFound, f"account={account} not found"
            )
        return AccountIdentity(
            # NULL → 0,交给 biz 走 backfill_account_id 补铸(旧二进制注册的存量行)。
            account_id=int(row[1] or 0),
            player_id=int(row[0]),
            password_hash=row[2] or "",
        )

    async def find_by_account_id(self, account_id: int) -> AccountIdentity:
        """按账号身份查行(EnterRole 的封禁复核用)。

        封禁是**账号级**的,挂在 accounts.player_id(主角色指针)上,不是挂在本次要
        进入的角色上。少了这一跳,账号下的其它角色就绕过了封禁。
        """
        if account_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "find account: accountID must be > 0"
            )
        q = (
            "SELECT player_id, account_id, password_hash FROM accounts "
            "WHERE account_id = %s LIMIT 1"
        )
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(q, (account_id,))
            row = await cur.fetchone()
        if row is None:
            raise errcode.PandoraError(
                errcode.ErrLoginAccountNotFound, f"account_id={account_id} not found"
            )
        return AccountIdentity(
            account_id=int(row[1] or 0), player_id=int(row[0]), password_hash=row[2] or ""
        )

    async def create_account(
        self, account_id: int, player_id: int, account: str, bcrypt_hash: str
    ) -> None:
        q = "INSERT INTO accounts(player_id, account_id, account, password_hash) VALUES (%s, %s, %s, %s)"
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(q, (player_id, account_id, account, bcrypt_hash))
        except errcode.PandoraError:
            raise
        except Exception as exc:
            if _is_dup_error(exc):
                raise errcode.PandoraError(
                    errcode.ErrAlreadyExists, f"account={account} already exists"
                ) from exc
            raise errcode.PandoraError(
                errcode.ErrInternal, f"mysql create account: {exc}"
            ) from exc

    async def backfill_account_id(self, player_id: int, candidate: int) -> int:
        """给 account_id 仍为 NULL 的存量行补铸账号身份(滚动升级兼容窗)。

        条件写 `WHERE player_id=? AND account_id IS NULL`:并发下只有一个赢家。
        输的那个**必须回读拿最终生效的值**,绝不返回 candidate 假装成功 ——
        否则两个并发请求各自以为自己的 account_id 生效,后续按不同 account_id
        建角色台账,同一个账号裂成两个。
        """
        if player_id == 0 or candidate == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                f"backfill account_id: playerID/candidate must be > 0 (got {player_id}/{candidate})",
            )
        upd = "UPDATE accounts SET account_id = %s WHERE player_id = %s AND account_id IS NULL"
        sel = "SELECT account_id FROM accounts WHERE player_id = %s LIMIT 1"
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(upd, (candidate, player_id))
                if cur.rowcount > 0:
                    return candidate
                await cur.execute(sel, (player_id,))
                row = await cur.fetchone()
        except errcode.PandoraError:
            raise
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, f"mysql backfill account_id: {exc}"
            ) from exc
        if row is None:
            raise errcode.PandoraError(
                errcode.ErrLoginAccountNotFound, f"account player_id={player_id} not found"
            )
        current = int(row[0] or 0)
        if current <= 0:
            # 更新说"没改到"、回读又说"还是 NULL":两次读到互相矛盾的状态,不能猜。
            raise errcode.PandoraError(
                errcode.ErrInternal,
                f"backfill account_id inconsistent for player_id={player_id} "
                "(update no-op but column still NULL)",
            )
        return current

    async def check_banned(self, player_id: int, device_id: str) -> bool:
        q = (
            "SELECT COUNT(*) FROM account_bans "
            "WHERE (expires_at IS NULL OR expires_at > UTC_TIMESTAMP()) "
            "  AND ((player_id IS NOT NULL AND player_id = %s) "
            "    OR (device_id IS NOT NULL AND device_id = %s))"
        )
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(q, (player_id, device_id))
                row = await cur.fetchone()
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, f"mysql check banned: {exc}"
            ) from exc
        return bool(row and int(row[0]) > 0)

    async def touch_device(self, player_id: int, device_id: str) -> None:
        if not device_id:
            return
        q = (
            "INSERT INTO account_devices(player_id, device_id, last_login_at) "
            "VALUES (%s, %s, UTC_TIMESTAMP()) "
            "ON DUPLICATE KEY UPDATE last_login_at = UTC_TIMESTAMP()"
        )
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(q, (player_id, device_id))
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, f"mysql touch device: {exc}"
            ) from exc

    async def get_player_no(self, player_id: int) -> int:
        """读角色编号(展示专用)。NULL / 行不存在均返回 0(未分配)。

        双列冲突时**报错而不是挑一个**:player_no 与 register_no 是 expand 期的
        双写对,值不同说明有第二写者,挑任何一个都是在编造事实。
        """
        q = "SELECT player_no, register_no FROM accounts WHERE player_id = %s LIMIT 1"
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(q, (player_id,))
                row = await cur.fetchone()
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, f"mysql get player_no: {exc}"
            ) from exc
        if row is None:
            return 0
        player_no, legacy_no = row[0], row[1]
        if player_no is not None and legacy_no is not None and int(player_no) != int(legacy_no):
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "mysql get player_no: player/register 双列冲突 "
                f"player_id={player_id} player_no={player_no} register_no={legacy_no}",
            )
        if player_no is not None:
            return int(player_no)
        if legacy_no is not None:
            return int(legacy_no)
        return 0

    async def get_player_nos(self, player_ids: list[int]) -> dict[int, int]:
        """一次 SQL 批量读取角色展示编号；缺行 / 双列皆空的值由调用方按 0 处理。"""
        if not player_ids:
            return {}
        placeholders = ", ".join(["%s"] * len(player_ids))
        q = (
            "SELECT player_id, player_no, register_no FROM accounts "
            f"WHERE player_id IN ({placeholders})"
        )
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(q, tuple(player_ids))
                rows = await cur.fetchall()
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, f"mysql get player_nos: {exc}"
            ) from exc

        out: dict[int, int] = {}
        for player_id, player_no, legacy_no in rows:
            pid = int(player_id)
            if (
                player_no is not None
                and legacy_no is not None
                and int(player_no) != int(legacy_no)
            ):
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "mysql get player_nos: player/register 双列冲突 "
                    f"player_id={pid} player_no={player_no} register_no={legacy_no}",
                )
            if player_no is not None:
                out[pid] = int(player_no)
            elif legacy_no is not None:
                out[pid] = int(legacy_no)
            else:
                out[pid] = 0
        return out


# ── MySQL:已选角色(player_roles)───────────────────────────────────────────


class MySQLPlayerRoleRepo:
    """玩家当前已选的 CfgRole 职业外观(与 account_roles 正交,别混)。"""

    __slots__ = ("_pool",)

    def __init__(self, pool) -> None:  # noqa: ANN001
        self._pool = pool

    async def get_role(self, player_id: int) -> int:
        q = "SELECT role_id FROM player_roles WHERE player_id = %s LIMIT 1"
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(q, (player_id,))
                row = await cur.fetchone()
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, f"mysql get player role: {exc}"
            ) from exc
        return int(row[0]) if row else 0  # 从未选过角是合法态,不是错误

    async def set_role(
        self, player_id: int, role_id: int, expected_sess_jti: str = "", precommit=None
    ) -> None:
        """覆盖式 upsert;幂等。

        expected_sess_jti 非空时在**同一事务内** `SELECT ... FOR UPDATE` 复核
        player_session_generations.sess_jti:行锁把「角色写」与「登录轮换代际写」放进
        同一 InnoDB 串行化域 —— 新登录把新 jti 落库后,旧会话的角色写事务必然读到新
        jti 而回滚,不再有「Redis precommit 通过与 COMMIT 之间被轮换」的跨存储窗口。

        兼容窗:该玩家行不存在(部署前登录的存量会话)时退化为仅 precommit 复核。
        """
        q = (
            "INSERT INTO player_roles(player_id, role_id) VALUES (%s, %s) "
            "ON DUPLICATE KEY UPDATE role_id = VALUES(role_id)"
        )
        if not expected_sess_jti and precommit is None:
            # 两道防线都未启用:保持单语句路径(dev 裸跑),行为与 Go 一致。
            try:
                async with self._pool.acquire() as conn, conn.cursor() as cur:
                    await cur.execute(q, (player_id, role_id))
            except Exception as exc:
                raise errcode.PandoraError(
                    errcode.ErrInternal, f"mysql set player role: {exc}"
                ) from exc
            return

        async with self._pool.acquire() as conn:
            await conn.begin()
            try:
                async with conn.cursor() as cur:
                    await cur.execute(q, (player_id, role_id))
                    if expected_sess_jti:
                        await cur.execute(
                            "SELECT sess_jti FROM player_session_generations "
                            "WHERE player_id = %s FOR UPDATE",
                            (player_id,),
                        )
                        row = await cur.fetchone()
                        if row is not None and (row[0] or "") != expected_sess_jti:
                            raise errcode.PandoraError(
                                errcode.ErrSessionSuperseded,
                                "session superseded; role write rolled back",
                            )
                if precommit is not None:
                    await precommit()  # 失败 → 下面的 except 回滚,写不落地
                await conn.commit()
            except BaseException:
                # 无条件 re-raise:取消照样穿透,回滚是必须做的清理。
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise


# ── MySQL:角色归属台账(account_roles)──────────────────────────────────────

_ROLE_SELECT_COLUMNS = "player_id, account_id, slot, role_name, status, last_login_at, created_at"


def _scan_account_role(row) -> AccountRole:  # noqa: ANN001
    return AccountRole(
        player_id=int(row[0]),
        account_id=int(row[1] or 0),
        slot=int(row[2] or 0),
        role_name=row[3] or "",
        status=int(row[4] or 0),
        last_login_at_ms=_dt_to_ms(row[5]),
        created_at_ms=_dt_to_ms(row[6]),
    )


class MySQLAccountRoleRepo:
    """account_roles:「有哪些角色、归谁」。role_name 不是显示名权威(那在 player 服务)。"""

    __slots__ = ("_pool",)

    def __init__(self, pool) -> None:  # noqa: ANN001
        self._pool = pool

    async def list_by_account(self, account_id: int) -> list[AccountRole]:
        """列出账号下**可用**角色(status=0)。排序 = 选角界面的默认选中顺序。

        `(last_login_at IS NULL) ASC` 必须在最前:MySQL 里 NULL 在 DESC 排序中排最前,
        直接 `ORDER BY last_login_at DESC` 会让「从未登录过的新角色」抢占默认选中位。
        排序放 SQL 里而不是 Python 里,是为了让「默认选中第一个」在所有调用方恒等。
        """
        if account_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "list account roles: accountID must be > 0"
            )
        q = (
            f"SELECT {_ROLE_SELECT_COLUMNS} FROM account_roles "
            "WHERE account_id = %s AND status = 0 "
            "ORDER BY (last_login_at IS NULL) ASC, last_login_at DESC, slot ASC"
        )
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(q, (account_id,))
                rows = await cur.fetchall()
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, f"mysql list account roles: {exc}"
            ) from exc
        return [_scan_account_role(r) for r in rows]

    async def get_by_player(self, player_id: int) -> AccountRole:
        if player_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "get account role: playerID must be > 0"
            )
        q = f"SELECT {_ROLE_SELECT_COLUMNS} FROM account_roles WHERE player_id = %s LIMIT 1"
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(q, (player_id,))
                row = await cur.fetchone()
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, f"mysql get account role: {exc}"
            ) from exc
        if row is None:
            raise errcode.PandoraError(
                errcode.ErrLoginRoleNotFound, f"role player_id={player_id} not found"
            )
        return _scan_account_role(row)

    async def create(self, role: AccountRole) -> None:
        if role.player_id == 0 or role.account_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "create account role: playerID/accountID must be > 0 "
                f"(got {role.player_id}/{role.account_id})",
            )
        q = (
            "INSERT INTO account_roles(player_id, account_id, slot, role_name, status) "
            "VALUES (%s, %s, %s, %s, 0)"
        )
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(q, (role.player_id, role.account_id, role.slot, role.role_name))
        except errcode.PandoraError:
            raise
        except Exception as exc:
            if _is_dup_error(exc):
                raise errcode.PandoraError(
                    errcode.ErrAlreadyExists,
                    f"account role already exists (player_id={role.player_id} "
                    f"account_id={role.account_id} slot={role.slot})",
                ) from exc
            raise errcode.PandoraError(
                errcode.ErrInternal, f"mysql create account role: {exc}"
            ) from exc

    async def touch_login(self, player_id: int) -> None:
        if player_id == 0:
            return
        q = "UPDATE account_roles SET last_login_at = UTC_TIMESTAMP() WHERE player_id = %s"
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(q, (player_id,))
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, f"mysql touch role login: {exc}"
            ) from exc

    async def next_slot(self, account_id: int) -> int:
        """下一个可用槽位(= 当前最大 slot + 1;无角色时 0)。

        **含软删行一起算最大值:槽位不回收。** 回收会让「刚删的角色」与「新建的角色」
        共用同一个 (account_id, slot),历史台账无法区分,过户 / 找回时对不上账。
        """
        if account_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "next slot: accountID must be > 0"
            )
        q = "SELECT COALESCE(MAX(slot) + 1, 0) FROM account_roles WHERE account_id = %s"
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(q, (account_id,))
                row = await cur.fetchone()
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, f"mysql next role slot: {exc}"
            ) from exc
        return int(row[0] or 0) if row else 0


# ── MySQL:会话代际(player_session_generations)─────────────────────────────


class MySQLSessionGenerationRepo:
    """登录**定序权威**。MySQL 原子分配单调 generation,Redis 再按「仅更高代际可覆盖」条件写。

    首版实现是「无条件覆盖 upsert」,并发登录 A/B 各自先写 Redis 再写 MySQL 时,
    迟到的 A 会把 MySQL 回写成旧 jti(Redis=B、MySQL=A 撕裂),合法的 B 反而被
    SetRole 的代际复核拒绝。改成本形态后,任意交错下两个存储都收敛到最高代际那次登录。
    """

    __slots__ = ("_pool",)

    def __init__(self, pool) -> None:  # noqa: ANN001
        self._pool = pool

    async def persist_session_jti(self, player_id: int, jti: str) -> SessionGenerationLease:
        last_exc: BaseException | None = None
        for _ in range(PERSIST_MAX_ATTEMPTS):
            try:
                return await self._persist_once(player_id, jti)
            except CommitAmbiguousError:
                # 不是"事务已回滚"的并发错误:重放会把不确定变成确定的多推一代。原样上抛。
                raise
            except errcode.PandoraError:
                raise
            except Exception as exc:
                if not _is_retryable_tx_error(exc):
                    raise errcode.PandoraError(
                        errcode.ErrInternal, f"mysql persist session jti: {exc}"
                    ) from exc
                last_exc = exc
        # 重试耗尽返回 **ErrUnavailable**(可重试语义)而不是 ErrInternal:客户端与 UE 侧
        # 只对可重试语义做自动退避恢复;报成 ErrInternal 会让玩家卡在登录页(§9.20)。
        raise errcode.PandoraError(
            errcode.ErrUnavailable,
            f"session generation contended (deadlock/lock timeout) after "
            f"{PERSIST_MAX_ATTEMPTS} attempts; retry login: {last_exc}",
        )

    async def _persist_once(self, player_id: int, jti: str) -> SessionGenerationLease:
        """事务内「FOR UPDATE 快照旧值 → upsert(generation+1) → 读回代际 → COMMIT」。

        行 X 锁持有到 COMMIT,同事务读回的必然是本次分配的代际,并发登录在此串行化。
        """
        lease = SessionGenerationLease()
        previous_jti = ""
        async with self._pool.acquire() as conn:
            await conn.begin()
            committed = False
            try:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT sess_jti FROM player_session_generations "
                        "WHERE player_id = %s FOR UPDATE",
                        (player_id,),
                    )
                    row = await cur.fetchone()
                    if row is not None:
                        previous_jti = row[0] or ""
                    await cur.execute(
                        "INSERT INTO player_session_generations"
                        "(player_id, sess_jti, generation) VALUES (%s, %s, 1) "
                        "ON DUPLICATE KEY UPDATE generation = generation + 1, "
                        "sess_jti = VALUES(sess_jti)",
                        (player_id, jti),
                    )
                    await cur.execute(
                        "SELECT generation FROM player_session_generations WHERE player_id = %s",
                        (player_id,),
                    )
                    gen_row = await cur.fetchone()
                    if gen_row is None:
                        raise errcode.PandoraError(
                            errcode.ErrInternal,
                            "mysql read session generation: row vanished inside tx",
                        )
                    lease.generation = int(gen_row[0])
                try:
                    await conn.commit()
                    committed = True
                except Exception as exc:
                    # COMMIT 可能**已经生效**而只是回包丢了。不猜、不当成「没提交」——
                    # 事务内已读出的 generation 一并带出,供调用方条件墓碑本次未交付代际。
                    err = CommitAmbiguousError(f"commit session generation: {exc}")
                    err.generation = lease.generation  # type: ignore[attr-defined]
                    raise err from exc
            except BaseException:
                if not committed:
                    with contextlib.suppress(Exception):
                        await conn.rollback()
                raise
        if previous_jti and previous_jti != jti:
            # 顶号 / 重登轮换的**赢家侧**唯一记录:prev_sess_jti 可 join 回上一条 login_ok
            # 拿到旧设备 device_id。COMMIT 成功后才打,回滚的事务不留假轮换记录。
            plog.get().info(
                "session_generation_rotated",
                player_id=player_id,
                prev_sess_jti=previous_jti,
                sess_jti=jti,
                generation=lease.generation,
            )
        return lease

    async def load_session_generation(self, player_id: int) -> tuple[str, int, bool]:
        """读回当前代际行 —— 把不确定的 COMMIT **判定**成事实。"""
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(
                    "SELECT sess_jti, generation FROM player_session_generations "
                    "WHERE player_id = %s",
                    (player_id,),
                )
                row = await cur.fetchone()
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, f"mysql load session generation: {exc}"
            ) from exc
        if row is None:
            return "", 0, False
        return (row[0] or ""), int(row[1] or 0), True

    async def tombstone_failed_session_jti(
        self, player_id: int, failed_jti: str, generation: int
    ) -> bool:
        """行仍是失败 Login 写入的 (failed_jti, generation) 才清除能力;generation 不回退。

        并发新登录已推进时 WHERE 不命中,绝不影响赢家。**绝不恢复即时前代**:
        连续未交付登录 A→B→C 中 B 可能也从未被任何客户端持有。
        """
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(
                    "UPDATE player_session_generations SET sess_jti = %s "
                    "WHERE player_id = %s AND sess_jti = %s AND generation = %s",
                    (SESSION_TOMBSTONE_JTI, player_id, failed_jti, generation),
                )
                return cur.rowcount > 0
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, f"mysql tombstone failed session jti: {exc}"
            ) from exc

    async def tombstone_session_jti(self, player_id: int, jti: str) -> bool:
        """登出墓碑:行仍持有本次登出的 jti 才推代际改哨兵。

        条件写(CAS)而非无条件覆盖:并发新登录可能已把行推到更新代际,
        无条件墓碑会**毒化新会话**。
        """
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(
                    "UPDATE player_session_generations "
                    "SET generation = generation + 1, sess_jti = %s "
                    "WHERE player_id = %s AND sess_jti = %s",
                    (SESSION_TOMBSTONE_JTI, player_id, jti),
                )
                return cur.rowcount > 0
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, f"mysql tombstone session jti: {exc}"
            ) from exc


# ── MySQL:角色编号补号 ─────────────────────────────────────────────────────


async def ensure_player_no_counter(pool, start_no: int) -> None:  # noqa: ANN001
    """启动期探针 + 双代计数器幂等初始化 —— 对应 Go 的 EnsurePlayerNoCounter。

    探针 `SELECT player_no, register_no`:未跑 000007 expand 的存量库在此一次性失败,
    调用方**停用补号任务但不拦启动**(编号是展示功能,fail-soft)。
    初始化 `INSERT IGNORE`:计数器已存在时不改 next_no —— 起始号只在首次初始化生效,
    编号连续性不允许事后改起点。
    """
    if start_no == 0:
        start_no = 1
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute("SELECT player_no, register_no FROM accounts LIMIT 1")
                await cur.fetchone()
            except Exception as exc:
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    f"player_no 双列探针失败(pandora_account 000007 expand 未完成?): {exc}",
                ) from exc
        # READ COMMITTED:与 sweep 同口径,见 sweep_player_no 的说明。
        async with conn.cursor() as cur:
            await cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        await conn.begin()
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT IGNORE INTO player_no_counter (id, next_no) VALUES (1, %s)",
                    (start_no,),
                )
                await cur.execute(
                    "INSERT IGNORE INTO register_no_counter (id, next_no) VALUES (1, %s)",
                    (start_no,),
                )
                await cur.execute(
                    "SELECT next_no FROM player_no_counter WHERE id=1 FOR UPDATE"
                )
                player_next = int((await cur.fetchone())[0])
                await cur.execute(
                    "SELECT next_no FROM register_no_counter WHERE id=1 FOR UPDATE"
                )
                legacy_next = int((await cur.fetchone())[0])
                nxt = max(player_next, legacy_next)
                await cur.execute("UPDATE player_no_counter SET next_no=%s WHERE id=1", (nxt,))
                await cur.execute("UPDATE register_no_counter SET next_no=%s WHERE id=1", (nxt,))
            await conn.commit()
        except BaseException:
            with contextlib.suppress(Exception):
                await conn.rollback()
            raise


async def sweep_player_no(pool, batch: int = PLAYER_NO_BATCH_SIZE) -> int:  # noqa: ANN001
    """执行一批补号,返回本批处理的行数。0 = 当前无待编号行(调用方据此结束本轮 drain)。

    ★ **READ COMMITTED 是正确性要求,不是调优**:计数器行锁只串行化「写」,取批 SELECT
    的可见性由隔离级别决定。TiDB 悲观事务在 RR 下用 BeginTx 时刻的 start_ts 快照服务
    普通 SELECT —— 后到的 sweeper 在计数器锁上等前一批提交后,取批扫描**仍看不到刚提交
    的编号**,会重扫同一批行,复核 UPDATE(当前读)恒 affected=0,把正常并发误判成
    「第二写者」整批回滚。RC 下两端都是逐语句新快照,扫描必然包含锁前驱批次的提交。

    ★ 不锁账号行:player_no 只有本事务(被计数器锁串行化)写,行级 FOR UPDATE 冗余,
    且扫 uk_player_no 的 NULL 范围会产生间隙锁、反向阻塞并发注册 INSERT。改用
    `UPDATE ... AND player_no IS NULL` + `rowcount == 1` 复核兜底:复核失败说明存在
    计数器锁之外的第二写者,整批回滚 fail-closed,**绝不发出可疑号**。
    """
    if batch <= 0:
        batch = PLAYER_NO_BATCH_SIZE
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        await conn.begin()
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT next_no FROM player_no_counter WHERE id = 1 FOR UPDATE"
                )
                row = await cur.fetchone()
                if row is None:
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "player_no counter lock(计数器未初始化? 见 ensure_player_no_counter)",
                    )
                player_next = int(row[0])
                await cur.execute(
                    "SELECT next_no FROM register_no_counter WHERE id = 1 FOR UPDATE"
                )
                row = await cur.fetchone()
                if row is None:
                    raise errcode.PandoraError(
                        errcode.ErrInternal, "register_no compatibility counter lock"
                    )
                legacy_next = int(row[0])
                nxt = max(player_next, legacy_next)

                await cur.execute(
                    "SELECT player_id, COALESCE(player_no, register_no, 0), "
                    "       player_no IS NOT NULL, register_no IS NOT NULL "
                    "  FROM accounts "
                    " WHERE (player_no IS NULL OR register_no IS NULL) "
                    "   AND created_at < NOW() - INTERVAL %s SECOND "
                    " ORDER BY created_at, player_id LIMIT %s",
                    (PLAYER_NO_WATERMARK_LAG_SEC, batch),
                )
                pending = await cur.fetchall()
                if not pending:
                    await conn.rollback()  # 空转:只读事务无副作用
                    return 0

                new_assignments = 0
                for player_id, existing, player_has, legacy_has in pending:
                    assigned = int(existing or 0)
                    if not player_has and not legacy_has:
                        assigned = nxt + new_assignments
                        new_assignments += 1
                    await cur.execute(
                        "UPDATE accounts SET player_no = %s, register_no = %s "
                        " WHERE player_id = %s "
                        "   AND (player_no IS NULL OR player_no = %s) "
                        "   AND (register_no IS NULL OR register_no = %s) "
                        "   AND (player_no IS NULL OR register_no IS NULL)",
                        (assigned, assigned, player_id, assigned, assigned),
                    )
                    if cur.rowcount != 1:
                        raise errcode.PandoraError(
                            errcode.ErrInternal,
                            f"player_no assign player_id={player_id} affected={cur.rowcount}"
                            "(计数器锁外存在第二写者或双列冲突,整批回滚)",
                        )
                new_next = nxt + new_assignments
                await cur.execute(
                    "UPDATE player_no_counter SET next_no = %s WHERE id = 1", (new_next,)
                )
                await cur.execute(
                    "UPDATE register_no_counter SET next_no = %s WHERE id = 1", (new_next,)
                )
            await conn.commit()
            return len(pending)
        except BaseException:
            with contextlib.suppress(Exception):
                await conn.rollback()
            raise


async def sweep_stale_devices(  # noqa: ANN001
    pool,
    mode: dbguard.Mode,
    retention_days: int,
    limit: int,
    *,
    schema: str = "pandora_account",
) -> dbguard.Outcome:
    """account_devices 保留期清理(§9.24)。**默认 report_only:一行都不删**。

    device_id 由客户端上报,单账号可无限堆新设备行 → 按最近登录时间兜底有界:
    被删设备下次登录 touch_device upsert 自然重建,只丢历史最近登录记录,无业务语义。
    """
    async with pool.acquire() as conn:
        return await dbguard.sweep_table(
            conn,
            mode,
            schema,
            "account_devices",
            "last_login_at < DATE_SUB(NOW(), INTERVAL %s DAY)",
            limit,
            retention_days,
        )


# ── Redis:会话权威 ─────────────────────────────────────────────────────────


def sess_key(player_id: int) -> str:
    return f"pandora:sess:{player_id}"


def ticket_key(jti: str) -> str:
    return f"pandora:ticket:{jti}"


# 单 key 原子「代际比较 + 覆盖」。现存 gen 缺失 / 更小 → 覆盖并刷 TTL 返回 1;
# 更大 → 零写入返回 0(本次登录已被更新一代顶掉);
# 相等且同 jti → 返回 2(网络重试的幂等成功:第一次 EVAL 已落地、只是回包丢了);
# 相等但不同 jti → 返回 -1(完整性冲突,不能冒充幂等 —— 正常 MySQL 分配不可能产生)。
_SET_IF_NEWER_GEN = redisx.LuaScript(
    "login_sess_set_if_newer_gen",
    """
local cur = redis.call('HGET', KEYS[1], 'gen')
if cur then
	local cur_gen = tonumber(cur)
	local next_gen = tonumber(ARGV[5])
	if cur_gen > next_gen then
		return 0
	end
	if cur_gen == next_gen then
		if redis.call('HGET', KEYS[1], 'jti') == ARGV[2] then
			return 2
		end
		return -1
	end
end
redis.call('HSET', KEYS[1],
	'token', ARGV[1], 'jti', ARGV[2], 'device_id', ARGV[3], 'exp_ms', ARGV[4], 'gen', ARGV[5])
redis.call('HDEL', KEYS[1], '_rollback_token', '_rollback_jti',
	'_rollback_device_id', '_rollback_exp_ms')
redis.call('PEXPIRE', KEYS[1], ARGV[6])
return 1
""",
)

_DELETE_IF_JTI = redisx.LuaScript(
    "login_sess_delete_if_jti",
    """
if redis.call("HGET", KEYS[1], "jti") == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
""",
)

# 对结果不确定的失败代际做单调无能力墓碑。刻意按 `generation <= failedGen` fencing 而
# 不只匹配 failedJTI:C 写入结果不确定且实际未落 Redis 时,Redis 可能仍停在同样未交付
# 的 B,只匹配 C 会把 B 永久留成 current。ARGV[1] 保留 failedJTI 作调用契约 / 审计参数,
# 不参与"恢复谁"的猜测。
_FENCE_FAILED_SET = redisx.LuaScript(
    "login_sess_fence_failed_set",
    """
local cur = redis.call('HGET', KEYS[1], 'gen')
if cur and tonumber(cur) > tonumber(ARGV[2]) then
	return 0
end
redis.call('HDEL', KEYS[1], 'token', 'jti', 'device_id', 'exp_ms',
	'_rollback_token', '_rollback_jti', '_rollback_device_id', '_rollback_exp_ms')
redis.call('HSET', KEYS[1], 'gen', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
""",
)


class RedisSessionRepo:
    """pandora:sess:<player_id> hash + TTL。字段:token / jti / device_id / exp_ms / gen。"""

    __slots__ = ("_rdb",)

    def __init__(self, rdb) -> None:  # noqa: ANN001
        self._rdb = rdb

    async def set(
        self, player_id: int, token: str, jti: str, device_id: str, ttl_sec: float, gen: int
    ) -> None:
        if ttl_sec < 0.001:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                f"invalid session ttl for player {player_id}: {ttl_sec}s (minimum 1ms)",
            )
        key = sess_key(player_id)
        exp_ms = now_ms() + int(ttl_sec * 1000)
        if gen > 0:
            try:
                n = await _SET_IF_NEWER_GEN(
                    self._rdb,
                    keys=[key],
                    args=[token, jti, device_id, exp_ms, gen, int(ttl_sec * 1000)],
                )
            except Exception as exc:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable, f"redis sess set unavailable: {exc}"
                ) from exc
            n = int(n or 0)
            if n == 0:
                raise errcode.PandoraError(
                    errcode.ErrSessionSuperseded,
                    f"login superseded by a newer concurrent login "
                    f"(player {player_id} gen {gen})",
                )
            if n < 0:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    f"session generation conflict for player {player_id} gen {gen}; retry login",
                )
            return
        # gen == 0:dev 裸跑无 MySQL 定序权威,保持历史无条件覆盖;同时清掉可能残留的
        # gen 字段 —— 陈旧代际会把后续 dev 登录当成"被顶"拒绝。
        try:
            pipe = self._rdb.pipeline(transaction=True)
            pipe.hset(
                key,
                mapping={
                    "token": token,
                    "jti": jti,
                    "device_id": device_id,
                    "exp_ms": exp_ms,
                },
            )
            pipe.hdel(key, "gen")
            pipe.hdel(
                key, "_rollback_token", "_rollback_jti", "_rollback_device_id", "_rollback_exp_ms"
            )
            pipe.expire(key, int(ttl_sec))
            await pipe.execute()
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, f"redis sess set unavailable: {exc}"
            ) from exc

    async def delete(self, player_id: int) -> None:
        try:
            await self._rdb.delete(sess_key(player_id))
        except Exception as exc:
            raise errcode.PandoraError(errcode.ErrInternal, f"redis sess del: {exc}") from exc

    async def get_jti(self, player_id: int) -> tuple[str, bool]:
        try:
            jti = await self._rdb.hget(sess_key(player_id), "jti")
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, f"redis sess get jti: {exc}"
            ) from exc
        if jti is None:
            return "", False
        text = jti.decode() if isinstance(jti, bytes) else str(jti)
        return text, text != ""

    async def delete_if_jti(self, player_id: int, jti: str) -> bool:
        """CAS 删除:防止旧设备的迟到 Logout 误删新登录的 session(顶号后新设备被踢)。"""
        try:
            n = await _DELETE_IF_JTI(self._rdb, keys=[sess_key(player_id)], args=[jti])
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, f"redis sess del-if-jti: {exc}"
            ) from exc
        return int(n or 0) > 0

    async def fence_failed_set(
        self, player_id: int, jti: str, gen: int, ttl_sec: float
    ) -> bool:
        if not jti or gen == 0 or ttl_sec < 0.001:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                f"invalid failed-session fence: player {player_id} "
                f"jti_present={bool(jti)} gen {gen} ttl {ttl_sec}s",
            )
        try:
            n = await _FENCE_FAILED_SET(
                self._rdb, keys=[sess_key(player_id)], args=[jti, gen, int(ttl_sec * 1000)]
            )
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, f"redis sess fence-failed-set unavailable: {exc}"
            ) from exc
        return int(n or 0) > 0


class RedisTicketJTIRepo:
    """pandora:ticket:<jti> 短期标记。首次 SETNX 成功 → 票据可用;再次失败 → 已重放。"""

    __slots__ = ("_rdb",)

    def __init__(self, rdb) -> None:  # noqa: ANN001
        self._rdb = rdb

    async def mark_used(self, jti: str, ttl_sec: float) -> None:
        if not jti:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "empty jti")
        try:
            ok = await self._rdb.set(ticket_key(jti), 1, nx=True, ex=max(1, int(ttl_sec)))
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, f"redis ticket setnx: {exc}"
            ) from exc
        if not ok:
            raise errcode.PandoraError(
                errcode.ErrLoginTicketReplayed, f"ticket jti={jti} already used"
            )


# ── Redis:登录失败 Quota(账号 + IP)────────────────────────────────────────


def _hash_account(account: str) -> str:
    """先按认证后端的账号等价语义归一化,再哈希。

    不归一化的后果:失败配额可被大小写 / 尾空格变体绕过 —— 账号权威列是
    `utf8mb4_0900_ai_ci`(大小写不敏感 + NO PAD),`WHERE account=?` 把
    alice / Alice / ALICE / "alice " 解析为同一个 player,若各自独立计数,
    针对单一真实账号的失败预算被放大 N 倍,账号维度闸形同虚设。
    """
    norm = account.strip().lower()
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()[:16]


class RedisLoginRateLimiter:
    """登录失败 Quota。**只对凭据失败计数**;成功登录、封禁与 DB 故障都不计。

    背压非权威门:Redis 故障一律放行只 Warn(§2 铁律)。
    """

    __slots__ = ("_rdb", "_limit", "_window_sec", "_lock_sec")

    def __init__(self, rdb, limit: int, window_sec: float, lock_sec: float) -> None:  # noqa: ANN001
        self._rdb = rdb
        self._limit = limit
        self._window_sec = window_sec
        self._lock_sec = lock_sec

    async def lock_remaining(self, account: str, client_ip: str) -> tuple[float, Exception | None]:
        """账号 / IP 两维度锁定剩余的较长者(0 = 未锁)。

        两维度**独立读、各自 fail-open**:一个维度读失败绝不短路掉另一个维度已读到的锁
        —— 撞库场景下 IP 维度锁常是唯一有效防线(攻击者轮换账号名使账号锁失效),
        若账号键读失败就提前返回会把 IP 锁静默绕过。
        """
        if self._limit <= 0:
            return 0.0, None
        remain = 0.0
        first_err: Exception | None = None
        d, err = await redisx.penalty_remaining(
            self._rdb, redisx.rl_key_string("login", "lockacct", _hash_account(account))
        )
        if err is not None:
            first_err = err
        elif d > remain:
            remain = d
        if client_ip:
            d, err = await redisx.penalty_remaining(
                self._rdb, redisx.rl_key_string("login", "lockip", client_ip)
            )
            if err is not None:
                if first_err is None:
                    first_err = err
            elif d > remain:
                remain = d
        return remain, first_err

    async def record_failure(self, account: str, client_ip: str) -> Exception | None:
        """记一次凭据失败:双维度计数,任一维度达限即布锁并**清零该维度的计数**。

        不清零的后果:计数窗(默认 15m)长于锁窗(默认 5m)时,锁到期后残留的满计数会被
        单次失败 INC 回 ≥limit 重新布满锁 —— 攻击者以「每 lock 一次失败」把目标账号 / IP
        长锁到整个计数窗,且共享 NAT 出口下会连坐锁死同 IP 正常玩家(违反 §9.20)。
        """
        if self._limit <= 0 or self._window_sec <= 0:
            return None
        acct_fail_key = redisx.rl_key_string("login", "failacct", _hash_account(account))
        n, err = await redisx.incr_window(self._rdb, acct_fail_key, self._window_sec)
        if err is not None:
            return err
        if n >= self._limit:
            err = await redisx.arm_penalty(
                self._rdb,
                redisx.rl_key_string("login", "lockacct", _hash_account(account)),
                self._lock_sec,
            )
            if err is not None:
                return err
            await redisx.clear_cooldown(self._rdb, acct_fail_key)
        if not client_ip:
            return None
        ip_fail_key = redisx.rl_key_string("login", "failip", client_ip)
        ip_n, err = await redisx.incr_window(self._rdb, ip_fail_key, self._window_sec)
        if err is not None:
            return err
        if ip_n >= self._limit:
            err = await redisx.arm_penalty(
                self._rdb, redisx.rl_key_string("login", "lockip", client_ip), self._lock_sec
            )
            if err is not None:
                return err
            await redisx.clear_cooldown(self._rdb, ip_fail_key)
        return None
