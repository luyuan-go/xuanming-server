"""公会数据层 —— 对应 Go 侧 internal/data/guild_repo.go。

库表(pandora_social):

    guilds              公会(PK guild_id snowflake,uk name)
    guild_members       公会成员(PK player_id = 单归属:玩家只属一个公会)
    guild_join_requests 加入申请(PK request_id,uk guild_id+player_id)

════ ★ 全局唯一加锁序:guilds 父行 → 子表 ════

    任一公会操作都**先**锁该公会的 guilds 行,使它成为「单公会唯一串行化闸门」:
    两个事务在拿到任何子表锁之前就会在 guilds 行相互阻塞,无法形成 hold-and-wait 环。

    这条不是"顺手统一一下":此前 Approve/Reject 走 request→guild、Disband 走
    guild→request,两条交叉就是**确定性 ABBA 死锁**(不是偶发)。
    新增写路径必须照这个方向,否则重试只是把死锁掩盖成"偶发变慢"。

════ ★ 权限复核必须在**持父行锁之后、用 FOR UPDATE 读** ════

    事务第一条语句是未锁读 `guild_id`(为了定加锁序),它已经建立了 REPEATABLE READ
    快照。若随后用普通 SELECT 读审批人角色,读到的是**取父行锁之前**的旧快照 ——
    审批人在等锁期间被并发降级 / 踢出,仍会读到旧的 leader/officer(五审 P1)。
    所以角色复核一律 `FOR UPDATE`(TiDB 下 FOR SHARE 是 noop 且默认拒绝,只能用它)。

════ ★ pending 计数是「明细 COUNT 权威 + 计数列自愈」 ════

    §9.18 的 max_pending_requests_per_guild 判定用的是 `COUNT(*) ... FOR UPDATE`,
    不是 `guilds.pending_request_count` 的值;计数列只作读优化 + 旧版兼容,
    每次都按明细**绝对值回写**。滚动窗口里旧 Pod 只改明细不改计数列,新版照样判对。
"""

from __future__ import annotations

import contextlib

from pandorapy import dbguard, errcode, mysqlx
from pandorapy import log as plog
from pandorapy.services.guild.rows import (
    GUILD_ROLE_LEADER,
    GUILD_ROLE_MEMBER,
    GUILD_ROLE_OFFICER,
    JOIN_STATUS_APPROVED,
    JOIN_STATUS_PENDING,
    JOIN_STATUS_REJECTED,
    GuildJoinRequestRow,
    GuildMemberRow,
    GuildRow,
)

# 事务遇死锁 / 锁等待超时后的额外重试次数(共 1+N 次尝试),与 Go 的 txMaxRetries 同值。
TX_MAX_RETRIES = 3

# MySQL 事务并发错误码:1213 死锁、1205 锁等待超时。二者发生时 InnoDB 已回滚本事务,
# 整段重放是安全且标准的做法。
ER_LOCK_WAIT_TIMEOUT = 1205

_GUILD_COLS = """guild_id, name, leader_id, member_count, max_members,
       CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED)"""


def _guild_row(row) -> GuildRow:  # noqa: ANN001
    return GuildRow(
        guild_id=int(row[0]),
        name=str(row[1]),
        leader_id=int(row[2]),
        member_count=int(row[3]),
        max_members=int(row[4]),
        created_ms=int(row[5] or 0),
    )


def _is_retryable(exc: BaseException) -> bool:
    args = getattr(exc, "args", ())
    return bool(args) and args[0] in (mysqlx.ER_LOCK_DEADLOCK, ER_LOCK_WAIT_TIMEOUT)


class MySQLGuildRepo:
    """公会数据层。对应 Go 的 `MySQLGuildRepo`。"""

    __slots__ = ("_pool", "_schema")

    def __init__(self, pool, schema: str = "pandora_social") -> None:  # noqa: ANN001
        self._pool = pool
        # 保留期清理的 DELETE 要写全限定表名(`schema`.`table`),Go 那边直接写死
        # "pandora_social"。这里做成参数只为一件事:数据层测试跑在**每进程独占库**上
        # (见 tests/mysqlfixture.py),写死会让 sweep 那两条用例永远打不到自己的表。
        # main 传的是 DSN 里的库名,生产上仍然是 pandora_social,行为与 Go 一致。
        self._schema = schema

    # ── 事务封装(带死锁重试)───────────────────────────────────────────────

    @contextlib.asynccontextmanager
    async def _tx_once(self):
        async with self._pool.acquire() as conn:
            await conn.begin()
            try:
                async with conn.cursor() as cur:
                    yield cur
                await conn.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise

    async def _run_tx(self, fn):  # noqa: ANN001
        """跑一个事务,遇 1213/1205 有界重试。

        ⚠️ fn **必须可安全重放** —— 它会被整体重跑,不能有事务外的副作用
        (发 kafka、调下游 RPC)。返回列表的路径必须在 fn 开头重置累加器,
        否则重试会把上一轮的元素累计进去(Go 的 DisbandGuild 就专门写了
        `deletedMembers = nil`)。
        """
        last: BaseException | None = None
        for attempt in range(TX_MAX_RETRIES + 1):
            try:
                async with self._tx_once() as cur:
                    result = await fn(cur)
                if attempt > 0:
                    plog.get().debug("guild_tx_retry_succeeded", attempts=attempt + 1)
                return result
            except Exception as exc:  # noqa: BLE001
                if not _is_retryable(exc):
                    raise
                last = exc
                # 父行是所有公会写的唯一串行化闸门,高并发下父行竞争会触发 1213/1205
                # 并静默重放。留在 debug 等于默认不上报 —— 线上只会表现为偶发变慢,
                # 看不出"又出现了一条反向取锁的新路径"。
                plog.get().warning(
                    "guild_tx_retryable_conflict",
                    attempt=attempt + 1,
                    max_retries=TX_MAX_RETRIES,
                    err=str(exc),
                )
        assert last is not None
        plog.get().warning(
            "guild_tx_retries_exhausted", attempts=TX_MAX_RETRIES + 1, err=str(last)
        )
        raise last

    # ── pending 计数对账 ───────────────────────────────────────────────────

    @staticmethod
    async def _reconcile_pending_count(cur, guild_id: int) -> int:  # noqa: ANN001
        """必须在**持有 guilds(guild_id) 父行锁之后**调用。

        复用旧版 `COUNT(*)...FOR UPDATE` 的 MySQL 索引锁,同时把明细权威值写回计数列:
        滚动窗口里旧 Pod 只改 guild_join_requests 也不会让新版信任陈旧计数。
        TiDB 没有间隙锁,但所有新版写已由父行串行。
        """
        await cur.execute(
            "SELECT COUNT(*) FROM guild_join_requests WHERE guild_id = %s AND status = %s FOR UPDATE",
            (guild_id, JOIN_STATUS_PENDING),
        )
        row = await cur.fetchone()
        count = int(row[0]) if row else 0
        # 带 `<> %s` 是为了不产生无谓的行更新(计数已正确时 rowcount=0)。
        await cur.execute(
            "UPDATE guilds SET pending_request_count = %s "
            "WHERE guild_id = %s AND pending_request_count <> %s",
            (count, guild_id, count),
        )
        return count

    # ── 建会 / 查询 ────────────────────────────────────────────────────────

    async def create_guild(
        self, new_guild_id: int, leader_id: int, name: str, max_members: int
    ) -> None:
        async def body(cur):  # noqa: ANN001
            # 单归属:创建者不能已在任何公会。
            await cur.execute(
                "SELECT 1 FROM guild_members WHERE player_id = %s LIMIT 1", (leader_id,)
            )
            if await cur.fetchone() is not None:
                raise errcode.PandoraError(
                    errcode.ErrGuildAlreadyInGuild, "player %d already in a guild", leader_id
                )
            try:
                await cur.execute(
                    "INSERT INTO guilds (guild_id, name, leader_id, member_count, max_members) "
                    "VALUES (%s, %s, %s, 1, %s)",
                    (new_guild_id, name, leader_id, max_members),
                )
            except Exception as exc:  # noqa: BLE001
                if mysqlx.is_duplicate_entry(exc):
                    # uk_name 冲突 → 业务错误码,不是 500。
                    raise errcode.PandoraError(
                        errcode.ErrGuildNameTaken, "guild name %r taken", name
                    ) from exc
                raise
            await cur.execute(
                "INSERT INTO guild_members (player_id, guild_id, role) VALUES (%s, %s, %s)",
                (leader_id, new_guild_id, GUILD_ROLE_LEADER),
            )

        await self._run_tx(body)

    async def get_guild(self, guild_id: int) -> GuildRow | None:
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                f"SELECT {_GUILD_COLS} FROM guilds WHERE guild_id = %s",  # noqa: S608
                (guild_id,),
            )
            row = await cur.fetchone()
            return _guild_row(row) if row else None

    async def get_my_guild(self, player_id: int) -> GuildRow | None:
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT g.guild_id, g.name, g.leader_id, g.member_count, g.max_members, "
                "CAST(UNIX_TIMESTAMP(g.created_at) * 1000 AS SIGNED) "
                "FROM guilds g JOIN guild_members m ON m.guild_id = g.guild_id "
                "WHERE m.player_id = %s",
                (player_id,),
            )
            row = await cur.fetchone()
            return _guild_row(row) if row else None

    async def get_member(self, player_id: int) -> GuildMemberRow | None:
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT player_id, guild_id, role, "
                "CAST(UNIX_TIMESTAMP(joined_at) * 1000 AS SIGNED) "
                "FROM guild_members WHERE player_id = %s",
                (player_id,),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            return GuildMemberRow(
                player_id=int(row[0]),
                guild_id=int(row[1]),
                role=int(row[2]),
                joined_ms=int(row[3] or 0),
            )

    async def list_members(
        self, guild_id: int, cursor: int = 0, limit: int = 0
    ) -> list[GuildMemberRow]:
        """按 player_id 升序游标分页(§9.18 读取侧)。limit<=0 = 全量(内部扇出用)。"""
        q = (
            "SELECT player_id, guild_id, role, "
            "CAST(UNIX_TIMESTAMP(joined_at) * 1000 AS SIGNED) "
            "FROM guild_members WHERE guild_id = %s AND (%s = 0 OR player_id > %s) "
            "ORDER BY player_id ASC"
        )
        args: list[object] = [guild_id, cursor, cursor]
        if limit > 0:
            q += " LIMIT %s"
            args.append(limit)
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(q, tuple(args))
            return [
                GuildMemberRow(
                    player_id=int(r[0]), guild_id=int(r[1]), role=int(r[2]), joined_ms=int(r[3] or 0)
                )
                for r in await cur.fetchall()
            ]

    # ── 加入申请 ───────────────────────────────────────────────────────────

    async def create_join_request(
        self, new_request_id: int, guild_id: int, player_id: int, max_pending: int
    ) -> tuple[int, bool]:
        """创建 / 复用加入申请。返回 (request_id, reused)。"""

        async def body(cur):  # noqa: ANN001
            # 1. 先锁 guilds 父行(全局统一加锁序,兼作与 DisbandGuild 的串行化闸门)。
            #    公会已被并发解散 → ErrGuildNotFound,避免写出指向已删公会的孤儿申请。
            await cur.execute(
                "SELECT guild_id FROM guilds WHERE guild_id = %s FOR UPDATE", (guild_id,)
            )
            if await cur.fetchone() is None:
                raise errcode.PandoraError(
                    errcode.ErrGuildNotFound, "guild %d not found", guild_id
                )

            # 2. 锁该玩家对该公会的申请行(唯一键 guild_id+player_id)。
            await cur.execute(
                "SELECT request_id, status FROM guild_join_requests "
                "WHERE guild_id = %s AND player_id = %s FOR UPDATE",
                (guild_id, player_id),
            )
            existing = await cur.fetchone()

            # 3. 以 pending 明细为权威并校正计数列。父行已 FOR UPDATE,
            #    pendingCount 与随后的 +1 在同一临界区,无幻读。
            pending = await self._reconcile_pending_count(cur, guild_id)

            if existing is None:
                if max_pending > 0 and pending >= max_pending:
                    raise errcode.PandoraError(
                        errcode.ErrGuildRequestLimit,
                        "pending join request limit reached for guild %d (max %d)",
                        guild_id,
                        max_pending,
                    )
                await cur.execute(
                    "INSERT INTO guild_join_requests (request_id, guild_id, player_id, status) "
                    "VALUES (%s, %s, %s, %s)",
                    (new_request_id, guild_id, player_id, JOIN_STATUS_PENDING),
                )
                await cur.execute(
                    "UPDATE guilds SET pending_request_count = %s WHERE guild_id = %s",
                    (pending + 1, guild_id),
                )
                return new_request_id, False

            existing_id, status = int(existing[0]), int(existing[1])
            if status == JOIN_STATUS_PENDING:
                return existing_id, True
            # 历史 rejected/approved → 复位 pending 并复用 request_id。
            # ★ 从非 pending 转 pending 同样占一格名额,所以上限要**再校验一次**;
            #   少这一次,"申满 → 全被拒 → 再全部复开"就能突破 §9.18 的闸。
            if max_pending > 0 and pending >= max_pending:
                raise errcode.PandoraError(
                    errcode.ErrGuildRequestLimit,
                    "pending join request limit reached for guild %d (max %d)",
                    guild_id,
                    max_pending,
                )
            await cur.execute(
                "UPDATE guild_join_requests SET status = %s WHERE request_id = %s",
                (JOIN_STATUS_PENDING, existing_id),
            )
            await cur.execute(
                "UPDATE guilds SET pending_request_count = %s WHERE guild_id = %s",
                (pending + 1, guild_id),
            )
            return existing_id, False

        return await self._run_tx(body)

    async def get_request(self, request_id: int) -> GuildJoinRequestRow | None:
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT request_id, guild_id, player_id, status, "
                "CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED) "
                "FROM guild_join_requests WHERE request_id = %s",
                (request_id,),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            return GuildJoinRequestRow(
                request_id=int(row[0]),
                guild_id=int(row[1]),
                player_id=int(row[2]),
                status=int(row[3]),
                created_ms=int(row[4] or 0),
            )

    async def list_pending_requests(
        self, guild_id: int, cursor: int = 0, limit: int = 0
    ) -> list[GuildJoinRequestRow]:
        """按 request_id 升序游标分页(§9.18 读取侧的 cursor 分页)。"""
        q = (
            "SELECT request_id, guild_id, player_id, status, "
            "CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED) "
            "FROM guild_join_requests WHERE guild_id = %s AND status = %s "
            "AND (%s = 0 OR request_id > %s) ORDER BY request_id ASC"
        )
        args: list[object] = [guild_id, JOIN_STATUS_PENDING, cursor, cursor]
        if limit > 0:
            q += " LIMIT %s"
            args.append(limit)
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(q, tuple(args))
            return [
                GuildJoinRequestRow(
                    request_id=int(r[0]),
                    guild_id=int(r[1]),
                    player_id=int(r[2]),
                    status=int(r[3]),
                    created_ms=int(r[4] or 0),
                )
                for r in await cur.fetchall()
            ]

    async def approve_join(self, request_id: int, approver_id: int, max_members: int) -> bool:
        """审批通过。返回 False 表示申请已被并发处理(非 pending),biz 不报成功。"""

        async def body(cur):  # noqa: ANN001
            # 未锁读申请所属公会(guild_id 是申请行的不可变列)—— 只为**定加锁序**。
            await cur.execute(
                "SELECT guild_id FROM guild_join_requests WHERE request_id = %s", (request_id,)
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGuildRequestInvalid, "request %d not found", request_id
                )
            guild_id = int(row[0])

            # 1. 锁公会父行(所有职位变更的串行化点)并读 member_count。
            await cur.execute(
                "SELECT member_count FROM guilds WHERE guild_id = %s FOR UPDATE", (guild_id,)
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGuildNotFound, "guild %d not found", guild_id
                )
            member_count = int(row[0])

            # 2. 锁申请行并复读权威状态 / 申请人。
            await cur.execute(
                "SELECT player_id, status FROM guild_join_requests WHERE request_id = %s FOR UPDATE",
                (request_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGuildRequestInvalid, "request %d not found", request_id
                )
            applicant_id, status = int(row[0]), int(row[1])
            if status != JOIN_STATUS_PENDING:
                await self._reconcile_pending_count(cur, guild_id)
                return False  # 已被并发处理;仍提交一次计数自愈

            # 3. 审批人须在该公会且为 leader/officer。
            #    ★ FOR UPDATE 而不是普通读:本事务首条是未锁读,已建立 RR 快照;
            #      普通读会拿到取父行锁**之前**的角色 —— 等锁期间被降级仍读到旧权限。
            await cur.execute(
                "SELECT role FROM guild_members WHERE player_id = %s AND guild_id = %s FOR UPDATE",
                (approver_id, guild_id),
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGuildNoPermission,
                    "approver %d not in guild %d",
                    approver_id,
                    guild_id,
                )
            approver_role = int(row[0])
            if approver_role not in (GUILD_ROLE_LEADER, GUILD_ROLE_OFFICER):
                raise errcode.PandoraError(
                    errcode.ErrGuildNoPermission, "approver %d not leader/officer", approver_id
                )

            # 4. 申请人不能已在任何公会(单归属)。
            await cur.execute(
                "SELECT 1 FROM guild_members WHERE player_id = %s LIMIT 1", (applicant_id,)
            )
            if await cur.fetchone() is not None:
                raise errcode.PandoraError(
                    errcode.ErrGuildAlreadyInGuild,
                    "applicant %d already in a guild",
                    applicant_id,
                )

            # 5. 不超员(§9.18 写入侧上限,在事务内原子校验)。
            if member_count >= max_members:
                raise errcode.PandoraError(
                    errcode.ErrGuildFull,
                    "guild %d full (%d/%d)",
                    guild_id,
                    member_count,
                    max_members,
                )

            # 6. 插成员 + 置 approved + member_count++。
            #    player_id 是主键(单归属硬约束):并发被另一个公会先批时 dup →
            #    翻译成业务错误(步骤 4 的读拦不住这种竞态,靠 PK 兜底)。
            try:
                await cur.execute(
                    "INSERT INTO guild_members (player_id, guild_id, role) VALUES (%s, %s, %s)",
                    (applicant_id, guild_id, GUILD_ROLE_MEMBER),
                )
            except Exception as exc:  # noqa: BLE001
                if mysqlx.is_duplicate_entry(exc):
                    raise errcode.PandoraError(
                        errcode.ErrGuildAlreadyInGuild,
                        "applicant %d already in a guild",
                        applicant_id,
                    ) from exc
                raise
            await cur.execute(
                "UPDATE guild_join_requests SET status = %s WHERE request_id = %s",
                (JOIN_STATUS_APPROVED, request_id),
            )
            await cur.execute(
                "UPDATE guilds SET member_count = member_count + 1 WHERE guild_id = %s",
                (guild_id,),
            )
            await self._reconcile_pending_count(cur, guild_id)
            return True

        return await self._run_tx(body)

    async def reject_join(self, request_id: int, approver_id: int) -> bool:
        async def body(cur):  # noqa: ANN001
            await cur.execute(
                "SELECT guild_id FROM guild_join_requests WHERE request_id = %s", (request_id,)
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGuildRequestInvalid, "request %d not found", request_id
                )
            guild_id = int(row[0])
            # 同 ApproveJoin 的加锁序:guilds 父行 → 申请行 → 成员行。
            await cur.execute(
                "SELECT member_count FROM guilds WHERE guild_id = %s FOR UPDATE", (guild_id,)
            )
            if await cur.fetchone() is None:
                raise errcode.PandoraError(
                    errcode.ErrGuildNotFound, "guild %d not found", guild_id
                )
            await cur.execute(
                "SELECT status FROM guild_join_requests WHERE request_id = %s FOR UPDATE",
                (request_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGuildRequestInvalid, "request %d not found", request_id
                )
            if int(row[0]) != JOIN_STATUS_PENDING:
                await self._reconcile_pending_count(cur, guild_id)
                return False
            await cur.execute(
                "SELECT role FROM guild_members WHERE player_id = %s AND guild_id = %s FOR UPDATE",
                (approver_id, guild_id),
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGuildNoPermission,
                    "approver %d not in guild %d",
                    approver_id,
                    guild_id,
                )
            if int(row[0]) not in (GUILD_ROLE_LEADER, GUILD_ROLE_OFFICER):
                raise errcode.PandoraError(
                    errcode.ErrGuildNoPermission, "approver %d not leader/officer", approver_id
                )
            await cur.execute(
                "UPDATE guild_join_requests SET status = %s WHERE request_id = %s",
                (JOIN_STATUS_REJECTED, request_id),
            )
            await self._reconcile_pending_count(cur, guild_id)
            return True

        return await self._run_tx(body)

    # ── 成员变更 ───────────────────────────────────────────────────────────

    async def remove_member(self, guild_id: int, player_id: int) -> None:
        """玩家本人退会(幂等:不存在不报错)。"""

        async def body(cur):  # noqa: ANN001
            await cur.execute(
                "SELECT leader_id FROM guilds WHERE guild_id = %s FOR UPDATE", (guild_id,)
            )
            row = await cur.fetchone()
            if row is None:
                return None  # 公会已解散 → 退会 / 踢人幂等成功
            # ★ 禁止移除现任会长:否则"退会 / 踢人"与"转让"交错会删掉**刚晋升的新会长**,
            #   留下指向已不在会的 leader_id —— 之后谁都转让不了、也解散不掉。
            if int(row[0]) == player_id:
                raise errcode.PandoraError(
                    errcode.ErrGuildNotLeader,
                    "leader %d must transfer or disband before leaving guild %d",
                    player_id,
                    guild_id,
                )
            await cur.execute(
                "DELETE FROM guild_members WHERE guild_id = %s AND player_id = %s",
                (guild_id, player_id),
            )
            if not cur.rowcount:
                return None  # 幂等:本就不在
            await cur.execute(
                "UPDATE guilds SET member_count = member_count - 1 "
                "WHERE guild_id = %s AND member_count > 0",
                (guild_id,),
            )
            return None

        await self._run_tx(body)

    async def kick_member(self, guild_id: int, operator_id: int, target_id: int) -> None:
        """踢人。操作者权限在事务内**持父行锁复核**(消除 biz 检查后被并发降级的 TOCTOU)。"""

        async def body(cur):  # noqa: ANN001
            await cur.execute(
                "SELECT guild_id FROM guilds WHERE guild_id = %s FOR UPDATE", (guild_id,)
            )
            if await cur.fetchone() is None:
                raise errcode.PandoraError(
                    errcode.ErrGuildNotFound, "guild %d not found", guild_id
                )
            await cur.execute(
                "SELECT role FROM guild_members WHERE player_id = %s AND guild_id = %s",
                (operator_id, guild_id),
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGuildNoPermission,
                    "operator %d not in guild %d",
                    operator_id,
                    guild_id,
                )
            op_role = int(row[0])
            await cur.execute(
                "SELECT role FROM guild_members WHERE player_id = %s AND guild_id = %s",
                (target_id, guild_id),
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGuildNotMember, "target %d not in guild %d", target_id, guild_id
                )
            target_role = int(row[0])
            if target_role == GUILD_ROLE_LEADER:
                raise errcode.PandoraError(
                    errcode.ErrGuildNoPermission, "cannot kick the leader"
                )
            if op_role == GUILD_ROLE_LEADER:
                pass  # leader 可踢 officer / member
            elif op_role == GUILD_ROLE_OFFICER:
                if target_role != GUILD_ROLE_MEMBER:
                    raise errcode.PandoraError(
                        errcode.ErrGuildNoPermission, "officer can only kick members"
                    )
            else:
                raise errcode.PandoraError(errcode.ErrGuildNoPermission, "member cannot kick")
            await cur.execute(
                "DELETE FROM guild_members WHERE guild_id = %s AND player_id = %s",
                (guild_id, target_id),
            )
            if not cur.rowcount:
                return None
            await cur.execute(
                "UPDATE guilds SET member_count = member_count - 1 "
                "WHERE guild_id = %s AND member_count > 0",
                (guild_id,),
            )
            return None

        await self._run_tx(body)

    async def disband_guild(self, guild_id: int, operator_id: int) -> list[int]:
        """解散公会,返回**实际被删的成员集合**。

        ★ 成员集合必须在持父行锁时读、与 DELETE 同事务:父行 FOR UPDATE 已串行化
          ApproveJoin(它也锁同一父行),所以读到的集合与随后的 DELETE 原子一致,
          不会漏掉"快照后并发批准的新成员"——漏掉的那个会被删却拿不到,
          于是缓存不失效、通知也不发,玩家客户端上公会永远还在。
        """

        async def body(cur):  # noqa: ANN001
            # ★ 每次重放都重新收集,否则重试会把上一轮的 player_id 累计进去。
            deleted: list[int] = []
            await cur.execute(
                "SELECT leader_id FROM guilds WHERE guild_id = %s FOR UPDATE", (guild_id,)
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGuildNotFound, "guild %d not found", guild_id
                )
            if int(row[0]) != operator_id:
                raise errcode.PandoraError(
                    errcode.ErrGuildNotLeader,
                    "player %d is not current leader of guild %d (concurrent transfer?)",
                    operator_id,
                    guild_id,
                )
            await cur.execute(
                "SELECT player_id FROM guild_members WHERE guild_id = %s", (guild_id,)
            )
            deleted = [int(r[0]) for r in await cur.fetchall()]
            await cur.execute("DELETE FROM guild_members WHERE guild_id = %s", (guild_id,))
            await cur.execute("DELETE FROM guild_join_requests WHERE guild_id = %s", (guild_id,))
            await cur.execute("DELETE FROM guilds WHERE guild_id = %s", (guild_id,))
            return deleted

        return await self._run_tx(body)

    async def set_role(self, guild_id: int, operator_id: int, target_id: int, role: int) -> None:
        async def body(cur):  # noqa: ANN001
            await cur.execute(
                "SELECT leader_id FROM guilds WHERE guild_id = %s FOR UPDATE", (guild_id,)
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGuildNotFound, "guild %d not found", guild_id
                )
            cur_leader = int(row[0])
            if cur_leader != operator_id:
                raise errcode.PandoraError(
                    errcode.ErrGuildNotLeader,
                    "player %d is not current leader of guild %d (concurrent transfer?)",
                    operator_id,
                    guild_id,
                )
            # 会长职位只能走 TransferLeader 改,保 leader_id 与 role 一致。
            if target_id == cur_leader:
                raise errcode.PandoraError(
                    errcode.ErrGuildNoPermission,
                    "cannot change role of current leader %d",
                    target_id,
                )
            await cur.execute(
                "UPDATE guild_members SET role = %s WHERE guild_id = %s AND player_id = %s",
                (role, guild_id, target_id),
            )
            if not cur.rowcount:
                raise errcode.PandoraError(
                    errcode.ErrGuildNotMember, "player %d not in guild %d", target_id, guild_id
                )
            return None

        await self._run_tx(body)

    async def transfer_leader(
        self, guild_id: int, old_leader_id: int, new_leader_id: int
    ) -> None:
        async def body(cur):  # noqa: ANN001
            # ★ 缺"确认旧会长仍是现任"这一步,并发两次转让会各自降旧会长、升不同目标
            #   → 双 LEADER,而 leader_id 只留最后一个。
            await cur.execute(
                "SELECT leader_id FROM guilds WHERE guild_id = %s FOR UPDATE", (guild_id,)
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGuildNotFound, "guild %d not found", guild_id
                )
            if int(row[0]) != old_leader_id:
                raise errcode.PandoraError(
                    errcode.ErrGuildNotLeader,
                    "player %d is not current leader of guild %d (concurrent transfer?)",
                    old_leader_id,
                    guild_id,
                )
            await cur.execute(
                "SELECT role FROM guild_members WHERE player_id = %s AND guild_id = %s FOR UPDATE",
                (new_leader_id, guild_id),
            )
            if await cur.fetchone() is None:
                raise errcode.PandoraError(
                    errcode.ErrGuildNotMember, "target %d not in guild %d", new_leader_id, guild_id
                )
            await cur.execute(
                "UPDATE guild_members SET role = %s WHERE guild_id = %s AND player_id = %s",
                (GUILD_ROLE_MEMBER, guild_id, old_leader_id),
            )
            await cur.execute(
                "UPDATE guild_members SET role = %s WHERE guild_id = %s AND player_id = %s",
                (GUILD_ROLE_LEADER, guild_id, new_leader_id),
            )
            await cur.execute(
                "UPDATE guilds SET leader_id = %s WHERE guild_id = %s",
                (new_leader_id, guild_id),
            )
            return None

        await self._run_tx(body)

    # ── 保留期清理(§9.24)──────────────────────────────────────────────────

    async def sweep_terminal_join_requests(
        self, mode: dbguard.Mode, retention_days: int, limit: int
    ) -> dbguard.Outcome:
        """处理终态(approved/rejected)且 updated_at 超保留期的申请行。

        **mode 默认 report_only:只统计待清理量并 WARN,一行都不删**(用户 2026-07-22 指令)。
        pending 无论如何都不在处理范围 —— 它是工作集,不是失效数据。
        条件走 idx_status_updated(status, updated_at)。
        """
        async with self._pool.acquire() as conn:
            return await dbguard.sweep_table(
                conn,
                mode,
                self._schema,
                "guild_join_requests",
                "status <> %s AND updated_at < DATE_SUB(NOW(), INTERVAL %s DAY)",
                limit,
                JOIN_STATUS_PENDING,
                retention_days,
            )
