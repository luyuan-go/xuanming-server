"""leaderboard 结算归档数据层(MySQL)—— 对应 Go 侧 internal/data/leaderboard_repo.go。

库表(deploy/mysql-init/10-leaderboard-tables.sql,pandora_leaderboard 库):

    leaderboard_settlement  结算批次头(uk settle_idempotency_key 防重复结算,§9.2)
    leaderboard_snapshot    结算 Top-N 名次快照(归档 / 对账)
    leaderboard_reward_log  逐名次发奖记录(uk grant_idempotency_key 防重复发奖,§9.7)

进行中的实时排名 / 临时榜只在 Redis(board_store.py),不落库;
MySQL 只兜结算结果 + 发奖凭证。

★ `rank` 是 MySQL 8 保留字,**每一处都必须反引号**。漏一处的表现是语法错误
  (响亮的),但如果借机改成别的列名就是静默的 —— 两栈会读写不同的列。
"""

from __future__ import annotations

import asyncio

import dataclasses

from pandorapy import dbguard, errcode, mysqlx

# 发奖状态(leaderboard_reward_log.status)。与 Go 的 RewardPending/Granted/Failed 同值。
REWARD_PENDING = 0
REWARD_GRANTED = 1
REWARD_FAILED = 2


@dataclasses.dataclass(slots=True)
class SettlementRecord:
    """leaderboard_settlement 一行的存储视图。"""

    settlement_id: int
    board_type: int
    scope: int
    scope_id: int
    period: str
    top_n: int
    settled_count: int
    settle_idem_key: str
    reset_after: bool
    created_at_ms: int


@dataclasses.dataclass(slots=True)
class SnapshotRow:
    """leaderboard_snapshot 一行。"""

    rank: int
    entity_id: int
    score: int
    created_at_ms: int


@dataclasses.dataclass(slots=True)
class RewardLogRecord:
    """leaderboard_reward_log 一行的存储视图。

    reward_payload 是 pb `RewardGrantStorageRecord` 二进制(列 reward_pb VARBINARY)。
    它既是审计明细,**也是补发时重放的权威入参** —— 重放路径不得因编码漂移解不出
    奖励,所以用 proto 二进制而非 JSON(§5.8/§9.17)。
    """

    settlement_id: int
    entity_id: int
    rank: int
    grant_idem_key: str
    status: int
    reward_payload: bytes
    created_at_ms: int
    updated_at_ms: int


_SETTLEMENT_COLS = (
    "settlement_id, board_type, scope, scope_id, period, top_n, settled_count, "
    "settle_idempotency_key, reset_after, created_at_ms"
)


def _row_to_settlement(row) -> SettlementRecord:  # noqa: ANN001
    return SettlementRecord(
        settlement_id=int(row[0]),
        board_type=int(row[1]),
        scope=int(row[2]),
        scope_id=int(row[3]),
        period=row[4] if isinstance(row[4], str) else (row[4] or b"").decode(),
        top_n=int(row[5]),
        settled_count=int(row[6]),
        settle_idem_key=row[7] if isinstance(row[7], str) else (row[7] or b"").decode(),
        reset_after=int(row[8]) != 0,
        created_at_ms=int(row[9]),
    )


def build_mark_reward_sql(grant_idem_key: str, status: int, updated_at_ms: int) -> tuple[str, list]:
    """拼发奖状态更新语句。抽出来是为了可断言(对齐 Go 的 buildMarkRewardSQL)。

    ★ **失败标记必须带 `status <> GRANTED` 守卫**(INC-20260811-001 §6 同型缺陷)。
      多副本补扫是刻意允许的(正确性靠下游幂等键,§15.3 不为此加 claim/lease),
      但无条件 UPDATE 会让 A 副本发放成功写 GRANTED 的同时,B 副本因下游瞬时不可用
      把同一行打回 FAILED。后果三层:
        - 已发放的行重回补发工作集,每轮重放一次;
        - "陈年 FAILED = 发放链有 bug" 这个审计信号被淹没;
        - 下游幂等记录过保留期(90 天)后再重放,就从"幂等吸收"变成**真重复发放**。

      成功标记仍为无条件更新:终态推进幂等,重复写 GRANTED 无害。
    """
    sql = "UPDATE leaderboard_reward_log SET status = %s, updated_at_ms = %s WHERE grant_idempotency_key = %s"
    args: list = [status, updated_at_ms, grant_idem_key]
    if status != REWARD_GRANTED:
        sql += " AND status <> %s"
        args.append(REWARD_GRANTED)
    return sql, args


def build_save_snapshot_sql(settlement_id: int, rows: list[SnapshotRow]) -> tuple[str, list]:
    """拼批量快照插入。INSERT IGNORE = 幂等回放((settlement_id, rank) 是主键)。"""
    values = ",".join(["(%s,%s,%s,%s,%s)"] * len(rows))
    sql = (
        "INSERT IGNORE INTO leaderboard_snapshot "
        "(settlement_id, `rank`, entity_id, score, created_at_ms) VALUES " + values
    )
    args: list = []
    for row in rows:
        args.extend([settlement_id, row.rank, row.entity_id, row.score, row.created_at_ms])
    return sql, args


# 结算归档库名。保留期清理的 DELETE 要写全限定表名(`db`.`table`),所以必须知道库名。
DEFAULT_SCHEMA = "pandora_leaderboard"


class MySQLLeaderboardRepo:
    """基于 asyncmy / aiomysql 连接池的结算归档仓库。对应 Go 的 MySQLLeaderboardRepo。"""

    __slots__ = ("_pool", "_schema")

    def __init__(self, pool, schema: str = DEFAULT_SCHEMA) -> None:  # noqa: ANN001
        self._pool = pool
        # ★ Go 侧把库名写死成 "pandora_leaderboard" 字面量。这里做成参数,由 main.py
        # 传**DSN 里实际连上的那个库**:两者在生产是同一个值,但 DSN 指向别的库名时
        # (联调库 / 每进程独占的测试库),写死的那份会每小时对着一个不存在的 schema
        # 报错 —— 清理静默不生效,而失败只落在一条每小时一次的 WARN 里。
        self._schema = schema or DEFAULT_SCHEMA

    # ── 结算批次 ─────────────────────────────────────────────────────────────

    async def claim_settlement(
        self, rec: SettlementRecord
    ) -> tuple[SettlementRecord, bool]:
        """幂等插入结算批次。命中 uk → (已存批次, True);首次 → (rec, False)。

        ★ 幂等靠 **唯一键冲突**,不是"先 SELECT 再 INSERT"。后者在两个副本同时
        结算同一榜时会双双查不到、双双插入、双双发奖 —— 唯一键是这里唯一的串行化点。
        """
        sql = f"INSERT INTO leaderboard_settlement ({_SETTLEMENT_COLS}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        params = (
            rec.settlement_id,
            rec.board_type,
            rec.scope,
            rec.scope_id,
            rec.period,
            rec.top_n,
            rec.settled_count,
            rec.settle_idem_key,
            1 if rec.reset_after else 0,
            rec.created_at_ms,
        )
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(sql, params)
                    await conn.commit()
                    return rec, False
                except asyncio.CancelledError:
                    # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                    # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                    #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                    #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                    # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                    raise
                except BaseException as exc:  # noqa: BLE001
                    await conn.rollback()
                    if not mysqlx.is_duplicate_entry(exc):
                        raise errcode.PandoraError(
                            errcode.ErrInternal,
                            "insert settlement key=%s: %s",
                            rec.settle_idem_key,
                            exc,
                        ) from exc
                try:
                    await cur.execute(
                        f"SELECT {_SETTLEMENT_COLS} FROM leaderboard_settlement "
                        "WHERE settle_idempotency_key = %s LIMIT 1",
                        (rec.settle_idem_key,),
                    )
                    row = await cur.fetchone()
                except asyncio.CancelledError:
                    # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                    # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                    #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                    #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                    # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                    raise
                except BaseException as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "read settlement key=%s: %s",
                        rec.settle_idem_key,
                        exc,
                    ) from exc
        if row is None:
            # 唯一键冲突了却查不到 = 同一 key 的行刚被删。绝不能当"首次结算"往下走
            # (会重复发奖),报错让调用方重试。
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "read settlement key=%s: duplicate but row missing",
                rec.settle_idem_key,
            )
        return _row_to_settlement(row), True

    # ── 名次快照 ─────────────────────────────────────────────────────────────

    async def save_snapshot(self, settlement_id: int, rows: list[SnapshotRow]) -> None:
        """批量落 Top-N 名次快照(已存在的 (settlement_id, rank) 忽略,幂等回放)。"""
        if not rows:
            return
        sql, args = build_save_snapshot_sql(settlement_id, rows)
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(sql, args)
                await conn.commit()
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                await conn.rollback()
                raise errcode.PandoraError(
                    errcode.ErrInternal, "save snapshot settlement=%d: %s", settlement_id, exc
                ) from exc

    async def load_snapshot(self, settlement_id: int) -> list[SnapshotRow]:
        """按 settlement_id 读 Top-N 名次快照(rank 升序),供幂等命中后回放 winners。

        ★ 回放**只能**取 MySQL 快照,不能回 Redis 取:首次结算若 reset_after=true
        已经把榜清空了,而且 Redis 是计算层(可 evict / TTL)—— 快照才是结算权威记录。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(
                    "SELECT `rank`, entity_id, score, created_at_ms FROM leaderboard_snapshot "
                    "WHERE settlement_id = %s ORDER BY `rank` ASC",
                    (settlement_id,),
                )
                rows = await cur.fetchall()
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "load snapshot settlement=%d: %s", settlement_id, exc
                ) from exc
        return [
            SnapshotRow(
                rank=int(r[0]), entity_id=int(r[1]), score=int(r[2]), created_at_ms=int(r[3])
            )
            for r in rows
        ]

    # ── 发奖记录 ─────────────────────────────────────────────────────────────

    async def claim_reward(self, rec: RewardLogRecord) -> bool:
        """幂等插入发奖记录。命中 uk(grant_idempotency_key)→ True(本名次已发过)。"""
        sql = (
            "INSERT INTO leaderboard_reward_log "
            "(settlement_id, entity_id, `rank`, grant_idempotency_key, status, reward_pb, "
            "created_at_ms, updated_at_ms) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        )
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(
                    sql,
                    (
                        rec.settlement_id,
                        rec.entity_id,
                        rec.rank,
                        rec.grant_idem_key,
                        rec.status,
                        rec.reward_payload,
                        rec.created_at_ms,
                        rec.updated_at_ms,
                    ),
                )
                await conn.commit()
                return False
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                await conn.rollback()
                if mysqlx.is_duplicate_entry(exc):
                    return True
                raise errcode.PandoraError(
                    errcode.ErrInternal, "insert reward_log key=%s: %s", rec.grant_idem_key, exc
                ) from exc

    async def mark_reward(self, grant_idem_key: str, status: int, updated_at_ms: int) -> None:
        """更新发奖状态(GRANTED / FAILED)。守卫条件见 build_mark_reward_sql。"""
        sql, args = build_mark_reward_sql(grant_idem_key, status, updated_at_ms)
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(sql, args)
                await conn.commit()
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                await conn.rollback()
                raise errcode.PandoraError(
                    errcode.ErrInternal, "mark reward key=%s: %s", grant_idem_key, exc
                ) from exc

    async def list_ungranted_rewards(
        self, older_than_ms: int, limit: int
    ) -> list[RewardLogRecord]:
        """列出未发成(PENDING / FAILED)且 updated_at_ms < older_than_ms 的奖励。

        older_than_ms 把"刚结算还在同步发"的批次挡在扫描外,避免与同步发奖路径
        双重发起(Grant 幂等,重叠也不双发,但会白跑一趟并制造 FAILED 噪声)。
        """
        if limit <= 0:
            limit = 100
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(
                    "SELECT settlement_id, entity_id, `rank`, grant_idempotency_key, status, "
                    "reward_pb, created_at_ms, updated_at_ms FROM leaderboard_reward_log "
                    "WHERE status <> %s AND updated_at_ms < %s ORDER BY updated_at_ms ASC LIMIT %s",
                    (REWARD_GRANTED, older_than_ms, limit),
                )
                rows = await cur.fetchall()
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "list ungranted rewards: %s", exc
                ) from exc
        out: list[RewardLogRecord] = []
        for r in rows:
            key = r[3] if isinstance(r[3], str) else (r[3] or b"").decode()
            payload = r[5]
            if isinstance(payload, str):
                payload = payload.encode("latin-1")
            out.append(
                RewardLogRecord(
                    settlement_id=int(r[0]),
                    entity_id=int(r[1]),
                    rank=int(r[2]),
                    grant_idem_key=key,
                    status=int(r[4]),
                    reward_payload=bytes(payload or b""),
                    created_at_ms=int(r[6]),
                    updated_at_ms=int(r[7]),
                )
            )
        return out

    # ── 保留期清理(§9.24)────────────────────────────────────────────────────
    #
    # snapshot / reward_log(GRANTED)随结算批次 × 名次数线性增长,超保留期批删。
    # leaderboard_settlement **故意不清**:settle_idempotency_key 的 uk 是防重复结算
    # 的永久闸 —— 删了它,超期后同 key 重放会被当成新结算**重新发一遍奖**;留着则
    # 重放 already=True + 快照已清 → 回放空 winners,fail-safe。
    # reward_log 只清 GRANTED:PENDING/FAILED 是补发扫描工作集,陈年残留属告警问题。

    async def sweep_snapshots_before(
        self, mode: dbguard.Mode, cutoff_ms: int, limit: int
    ) -> dbguard.Outcome:
        """处理 created_at_ms 超保留期的名次快照。**mode 默认 REPORT_ONLY:一行都不删**。"""
        async with self._pool.acquire() as conn:
            try:
                out = await dbguard.sweep_table(
                    conn,
                    mode,
                    self._schema,
                    "leaderboard_snapshot",
                    "created_at_ms < %s",
                    limit,
                    cutoff_ms,
                )
                await conn.commit()
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                await conn.rollback()
                raise errcode.PandoraError(
                    errcode.ErrInternal, "sweep snapshots: %s", exc
                ) from exc
        return out

    async def sweep_granted_rewards_before(
        self, mode: dbguard.Mode, cutoff_ms: int, limit: int
    ) -> dbguard.Outcome:
        """处理已发放(GRANTED)且 updated_at_ms 超保留期的发奖记录。

        PENDING / FAILED **永不进入处理范围** —— 它们是补发工作集,清掉等于把
        "还没发出去的奖"连同证据一起删了,玩家永远收不到且查无对证。
        """
        async with self._pool.acquire() as conn:
            try:
                out = await dbguard.sweep_table(
                    conn,
                    mode,
                    self._schema,
                    "leaderboard_reward_log",
                    "status = %s AND updated_at_ms < %s",
                    limit,
                    REWARD_GRANTED,
                    cutoff_ms,
                )
                await conn.commit()
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                await conn.rollback()
                raise errcode.PandoraError(
                    errcode.ErrInternal, "sweep granted rewards: %s", exc
                ) from exc
        return out
