"""battle_result Model-B 终态回收出箱仓储 —— 对应 Go 侧
`internal/data/terminal_release_schema.go` + `battle_repo.go` 的三个 outbox 方法。

拆成独立 mixin 与 Go 拆文件同因:这条链只在 `ds_auth.authority_mode=redis` 下存在,
混进结算仓储后没人能一眼看出"哪些 SQL 属于两阶段回收"。`MySQLBattleRepo` 继承本
mixin 拿到全部方法,签名与 Go 一一对应。

库表(tools/migrate pandora_battle 000002_terminal_release_outbox):

    terminal_release_outbox   每场至多一行(uk match_id),记录**服务端自证**的完整
                              active credential 快照 + 到期时间 + phase-1 ACK 位

两阶段(§16.2 幂等 / §16.3 部分失败):

    released_at_ms = 0   → phase 1:relay.release_terminal()(ds_allocator 做永久
                           terminal + UID-precondition delete),成功后 UPDATE 打 ACK
    released_at_ms > 0   → phase 2:relay.finalize_terminal()(只恢复同 proof 的
                           Redis 墓碑 TTL,ds_allocator 对该 reason 绝不碰 K8s),
                           成功后 DELETE 行

★ 顺序不可交换:phase 1 的 Redis CAS 一旦成功就不可逆,必须先在 MySQL 打上 durable
  ACK 才能进入 phase 2;反过来(先 finalize 再 mark)会在崩溃后重放 UID delete,
  而那时 UID 已经属于新一轮分配 —— 直接删掉正在打的另一局。
"""

from __future__ import annotations

import asyncio
import time

from pandorapy import errcode

#: 与 Go 的 `if limit <= 0 { limit = 128 }` 同值。
DEFAULT_TERMINAL_RELEASE_LIMIT = 128

#: 迁移未执行时的排障提示(与 Go 的 hint 逐字一致)。
TERMINAL_RELEASE_SCHEMA_HINT = "先执行 pandora_battle/000002_terminal_release_outbox migration"

#: 期望的表级属性(engine / collation 都小写化后比对)。
EXPECTED_TERMINAL_RELEASE_TABLE = ("innodb", "utf8mb4_0900_ai_ci")

#: 期望的列快照,顺序即 ordinal_position。
#: 元组顺序:(name, column_type, is_nullable, charset, collation, default, extra)
#:
#: ★ 为什么要精确到 charset / collation:`allocation_id` 等字段参与 fencing 比对,
#:   若被建成 `utf8mb4_0900_ai_ci`(大小写不敏感),两个只差大小写的 allocation
#:   会被 uk 判成同一行 —— 表现是"另一局的回收把本局的行顶掉",而全链零报错。
EXPECTED_TERMINAL_RELEASE_COLUMNS: tuple[tuple[str, str, str, str, str, str, str], ...] = (
    ("id", "bigint unsigned", "NO", "", "", "", "auto_increment"),
    ("match_id", "bigint unsigned", "NO", "", "", "", ""),
    ("allocation_id", "char(36)", "NO", "ascii", "ascii_bin", "", ""),
    ("ds_pod_name", "varchar(253)", "NO", "ascii", "ascii_bin", "", ""),
    ("gameserver_uid", "varchar(64)", "NO", "ascii", "ascii_bin", "", ""),
    ("instance_epoch", "int unsigned", "NO", "", "", "", ""),
    ("auth_gen", "bigint unsigned", "NO", "", "", "", ""),
    ("auth_jti", "varchar(256)", "NO", "ascii", "ascii_bin", "", ""),
    ("auth_exp_ms", "bigint", "NO", "", "", "", ""),
    ("auth_kid", "varchar(128)", "NO", "ascii", "ascii_bin", "", ""),
    ("auth_token_sha256", "char(64)", "NO", "ascii", "ascii_bin", "", ""),
    ("auth_writer_epoch", "int unsigned", "NO", "", "", "", ""),
    ("authorized_at_ms", "bigint", "NO", "", "", "", ""),
    ("release_after_ms", "bigint", "NO", "", "", "", ""),
    ("released_at_ms", "bigint", "NO", "", "", "0", ""),
    ("created_at_ms", "bigint", "NO", "", "", "", ""),
)

#: 期望的索引快照。元组顺序:(index_name, non_unique, seq_in_index, column_name)
EXPECTED_TERMINAL_RELEASE_INDEXES: tuple[tuple[str, int, int, str], ...] = (
    ("PRIMARY", 0, 1, "id"),
    ("idx_terminal_release_due", 1, 1, "release_after_ms"),
    ("idx_terminal_release_due", 1, 2, "id"),
    ("uk_terminal_release_match", 0, 1, "match_id"),
)

_TABLE_SQL = """SELECT COALESCE(engine, ''), COALESCE(table_collation, '')
FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name = 'terminal_release_outbox'"""

_COLUMNS_SQL = """SELECT column_name, column_type, is_nullable,
COALESCE(character_set_name, ''), COALESCE(collation_name, ''),
COALESCE(column_default, ''), extra
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'terminal_release_outbox'
ORDER BY ordinal_position"""

# ★ `BINARY index_name` 不能省:MySQL 默认排序对大小写不敏感,而 Go 侧就是按
#   BINARY 排的。少了它,`PRIMARY` 与自定义索引在混合大小写时的相对顺序会与
#   期望快照错位,于是**每次启动都报 schema 不符**(假红,且指向错误的列)。
_INDEXES_SQL = """SELECT index_name, non_unique, seq_in_index, column_name
FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = 'terminal_release_outbox'
ORDER BY CASE WHEN index_name = 'PRIMARY' THEN 0 ELSE 1 END,
         BINARY index_name, seq_in_index"""

_FETCH_SQL = """SELECT id, match_id, allocation_id, ds_pod_name, gameserver_uid, instance_epoch,
auth_gen, auth_jti, auth_exp_ms, auth_kid, auth_token_sha256, auth_writer_epoch,
authorized_at_ms, release_after_ms, released_at_ms, created_at_ms
FROM terminal_release_outbox
WHERE release_after_ms <= %s
ORDER BY release_after_ms ASC, id ASC
LIMIT %s"""


def _text(value: object) -> str:
    """information_schema 的列可能回 bytes(取决于连接 charset),统一成 str。"""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return "" if value is None else str(value)


class TerminalReleaseRepoMixin:
    """两阶段终态回收的 MySQL 仓储。由 `MySQLBattleRepo` 继承。

    本 mixin **不继承任何东西**、不定义 `__init__`,只使用宿主类的 `self._pool`
    (与 `ProgressRepoMixin` 同一套 MRO 组合约定)。`__slots__` 必须为空:
    加字段会与宿主类的 `__slots__` 冲突。
    """

    __slots__ = ()

    async def validate_terminal_release_schema(self) -> None:
        """Model-B 注册 capability 前的机械迁移门 —— 对应 Go 的 `ValidateTerminalReleaseSchema`。

        ★ 为什么不能只查"表在不在":半迁移实例(表建了但少一个索引、或某列被建成
          大小写不敏感)一样能 SELECT 成功,于是它会先 Ready、接下结算,直到某次
          fencing 比对静默失效才暴露 —— 那时已经有 DS 被误回收了。
        ★ 为什么放在启动而不是首个 ReportResult:结算是不可回滚的外部副作用,
          "先 Ready 再发现表缺失"等于把迁移错误变成玩家可见的战绩丢失。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:  # type: ignore[attr-defined]
            await cur.execute(_COLUMNS_SQL)
            columns = tuple(
                (
                    _text(row[0]).lower(),
                    _text(row[1]).lower(),
                    _text(row[2]).upper(),
                    _text(row[3]).lower(),
                    _text(row[4]).lower(),
                    _text(row[5]).lower(),
                    _text(row[6]).lower(),
                )
                for row in await cur.fetchall()
            )
            if columns != EXPECTED_TERMINAL_RELEASE_COLUMNS:
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "terminal release schema columns mismatch: got=%s want=%s",
                    columns,
                    EXPECTED_TERMINAL_RELEASE_COLUMNS,
                )

            await cur.execute(_TABLE_SQL)
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrInternal, "terminal release schema: table not found"
                )
            table = (_text(row[0]).lower(), _text(row[1]).lower())
            if table != EXPECTED_TERMINAL_RELEASE_TABLE:
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "terminal release schema table mismatch: got=%s want=%s",
                    table,
                    EXPECTED_TERMINAL_RELEASE_TABLE,
                )

            await cur.execute(_INDEXES_SQL)
            indexes = tuple(
                (_text(r[0]), int(r[1]), int(r[2]), _text(r[3])) for r in await cur.fetchall()
            )
            if indexes != EXPECTED_TERMINAL_RELEASE_INDEXES:
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "terminal release schema indexes mismatch: got=%s want=%s",
                    indexes,
                    EXPECTED_TERMINAL_RELEASE_INDEXES,
                )

    async def fetch_terminal_release_outbox(self, limit: int, now_ms: int) -> list:
        """按到期时间 / id 取一批待终态回收行 —— 对应 Go 的 `FetchTerminalReleaseOutbox`。"""
        from pandorapy.services.battle_result import repo as brepo  # 避免循环导入

        if limit <= 0:
            limit = DEFAULT_TERMINAL_RELEASE_LIMIT
        if now_ms <= 0:
            now_ms = int(time.time() * 1000)
        async with self._pool.acquire() as conn, conn.cursor() as cur:  # type: ignore[attr-defined]
            try:
                await cur.execute(_FETCH_SQL, (now_ms, limit))
                rows = await cur.fetchall()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "query terminal release outbox: %s", exc
                ) from exc
        out = []
        for row in rows:
            out.append(
                brepo.TerminalReleaseRecord(
                    id=int(row[0]),
                    match_id=int(row[1]),
                    allocation_id=_text(row[2]),
                    ds_pod_name=_text(row[3]),
                    gameserver_uid=_text(row[4]),
                    instance_epoch=int(row[5]),
                    auth_gen=int(row[6]),
                    auth_jti=_text(row[7]),
                    auth_exp_ms=int(row[8]),
                    auth_kid=_text(row[9]),
                    auth_token_sha256=_text(row[10]),
                    auth_writer_epoch=int(row[11]),
                    authorized_at_ms=int(row[12]),
                    release_after_ms=int(row[13]),
                    released_at_ms=int(row[14]),
                    created_at_ms=int(row[15]),
                )
            )
        return out

    async def mark_terminal_release_released(self, outbox_id: int, released_at_ms: int) -> bool:
        """持久化 phase-1 ACK —— 对应 Go 的 `MarkTerminalReleaseReleased`。

        返回 True = 本次真的把 0 推进成 released_at_ms;False = 已被并发 worker 推进
        (幂等 no-op)。`WHERE released_at_ms=0` 是这条语义的全部来源,不能去掉。
        """
        if outbox_id == 0 or released_at_ms <= 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "terminal release mark requires id/time"
            )
        async with self._pool.acquire() as conn, conn.cursor() as cur:  # type: ignore[attr-defined]
            try:
                await cur.execute(
                    "UPDATE terminal_release_outbox SET released_at_ms=%s "
                    "WHERE id=%s AND released_at_ms=0",
                    (released_at_ms, outbox_id),
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "mark terminal release released: %s", exc
                ) from exc
            return cur.rowcount == 1

    async def delete_terminal_release_outbox(self, outbox_id: int) -> None:
        """phase-2 finalize 的 ACK —— 对应 Go 的 `DeleteTerminalReleaseOutbox`。

        ★ `AND released_at_ms > 0` 是防误删的前置条件:pending 行(phase-1 还没成功)
          被删掉 = DS pod 永远不会被回收,而且没有任何记录能发现它。
        ★ affected 0 按幂等 no-op(并发 worker 已删 / 仍是 pending);>1 只可能是
          PK 失效或驱动异常,必须 fail-closed。
        """
        if outbox_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "terminal release outbox id required"
            )
        async with self._pool.acquire() as conn, conn.cursor() as cur:  # type: ignore[attr-defined]
            try:
                await cur.execute(
                    "DELETE FROM terminal_release_outbox WHERE id=%s AND released_at_ms > 0",
                    (outbox_id,),
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "delete terminal release outbox id=%d: %s",
                    outbox_id,
                    exc,
                ) from exc
            if cur.rowcount > 1:
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "delete terminal release id=%d affected=%d",
                    outbox_id,
                    cur.rowcount,
                )
