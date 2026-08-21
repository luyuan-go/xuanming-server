"""任务域 MySQL 仓储 —— 对应 Go 侧 internal/data/mission_repo.go(库 pandora_mission)。

事务边界:mutate_player / apply_facts_tx 一次领域操作一个事务 ——
先取该玩家守卫行点锁(玩家内串行)→ FOR UPDATE 载入该玩家全部活跃/完成行 → 引擎
回调(纯函数)→ 突变 + 发奖流水 + 推送出箱同事务持久化。repo 不读配置表(规则全在 biz)。

════ ① 为什么必须有守卫行 ════

    TiDB 悲观事务没有 gap/next-key 锁,`SELECT ... WHERE player_id=? FOR UPDATE`
    只锁**已存在**的行 —— 玩家一条活跃任务都没有时该语句一把锁都不加,两个并发
    AcceptMission 各自读到空活跃集,双双通过 max_active_missions 上限(§9.18)与
    (type,sub_type) 类型互斥校验,然后各插一行。已存在行的点锁两库语义一致,所以照
    friend 域的做法:临界区入口先 `INSERT ... ON DUPLICATE KEY UPDATE pk=pk`
    建/锁守卫行,锁持有到事务结束。

════ ② 写事务必须显式 READ COMMITTED(正确性要求,不是调优)════

    2026-08-11 真 MySQL 8.4 实测抓获:24 个**不同玩家**并发 Accept(彼此不共享任何
    守卫行、任何业务行)必炸 1213,报在 upsert active。根因不是锁序 —— 守卫行确实是
    本事务第一把锁,而是 RR 下对**零行**的 FOR UPDATE 取的是「键所在的间隙」而非某
    一行:表空时全部 player_id 落进同一个 supremum 间隙,N 个事务各自拿到相容的间隙
    锁,随后各自的 INSERT 都要 insert intention → 互相挡成环。玩家彼此无关却互相
    打死,并发越高越必然。

    为什么降到 RC 安全:本域的并发正确性**从设计之初就不依赖 gap 锁**(守卫行存在
    的理由正是「TiDB 没有 gap 锁」)。限额与类型互斥的权威性来自守卫行 + 守卫锁内的
    锁定读,幂等来自 mission_fact_receipts 的唯一键,三者在 RC 下全部成立。
    前置:binlog_format=ROW(MySQL 8.4 默认)。

════ ③ 幂等 ════

    mission_fact_receipts uk(player_id, idempotency_key) + 指纹比对(同键不同内容
      fail-closed,对齐 inventory claimLedger);
    mission_reward_log uk(grant_idempotency_key) 撞键回填既有行 ID(重放安全)。
"""

from __future__ import annotations

import contextlib
import dataclasses
import time

from pandora.mission.v1 import mission_pb2

from pandorapy import dbguard, errcode, mysqlx
from pandorapy.services.mission import engine as eng

DB_LABEL = "pandora_mission"

# 见文件头 ②。
WRITE_TX_ISOLATION = "READ COMMITTED"

# 发奖流水状态(与 DDL 注释、Go 的 MarkReward 逐字对应)。
REWARD_STATUS_PENDING = 0
REWARD_STATUS_GRANTED = 1
REWARD_STATUS_FAILED = 2

# 写入侧 payload 字节闸(§9.24 深度三上限之③;①单槽 uint32 ②槽数 ≤8 由配置表兜)。
PROGRESS_PAYLOAD_MAX = 256
REWARD_PAYLOAD_MAX = 2048
PUSH_PAYLOAD_MAX = 2048

# **只读路径**(load_player → ListMissions)对完成集的单次返回上限(§9.18 读取侧)。
#
# 为什么只加在只读路径:事务路径用 state.done 判「已完成不可重复接取」与领奖 CAS ——
# 那里一旦截断,超出截断窗口的已完成任务会被判成可重新接取,**把一个展示问题升级成
# 重复发奖**。所以事务路径刻意保持全量,由配置表 MAX_MISSION_ROWS 的写入侧硬上限兜住
# 规模(§9.18 要求写入侧与读取侧上限同时存在,不是二选一)。
DONE_READ_LIMIT = 2000


class PushOutboxRacedError(RuntimeError):
    """出箱删行命中 0 行 = 另一副本已抢先投递并删除本行(多副本并发发布)。"""


@dataclasses.dataclass(slots=True)
class RewardLogEntry:
    """一条待插入的发奖流水(repo 插入后回填 id,供事务提交后立即尝试发放)。"""

    mission_config_id: int
    key: str  # mission:<player_id>:<mission_config_id>
    reward_pb: bytes
    id: int = 0


@dataclasses.dataclass(slots=True)
class RewardLogRow:
    """发奖流水一行(补扫工作集)。"""

    id: int
    player_id: int
    mission_config_id: int
    key: str
    reward_pb: bytes


@dataclasses.dataclass(slots=True)
class PushOutboxRow:
    id: int
    player_id: int
    payload: bytes


class MySQLMissionRepo:
    """基于 asyncmy / aiomysql 的任务域数据层。对应 Go 的 MySQLMissionRepo。"""

    __slots__ = ("_pool", "_schema", "_done_read_limit")

    def __init__(self, pool, schema: str = "", *, done_read_limit: int = 0) -> None:  # noqa: ANN001
        self._pool = pool
        # schema 只被保留期清理用到(dbguard.sweep_table 要写全限定表名)。
        # main 传的是 **DSN 里实际连上的库**,不是写死 pandora_mission —— TiDB 档 /
        # 测试库连的都不是它,写死会让 sweep 去删一个根本没连的库。
        self._schema = schema or DB_LABEL
        # 只为测试可注入(真造 2000 行太慢),生产恒走默认值。
        self._done_read_limit = done_read_limit if done_read_limit > 0 else DONE_READ_LIMIT

    # ── 事务 ────────────────────────────────────────────────────────────────

    @contextlib.asynccontextmanager
    async def _write_tx(self):
        """写事务:**显式 RC**。见文件头 ② —— 这是正确性要求不是调优。"""
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SET TRANSACTION ISOLATION LEVEL {WRITE_TX_ISOLATION}"
                )
            await conn.begin()
            try:
                async with conn.cursor() as cur:
                    yield cur
                await conn.commit()
            except BaseException:
                # 无条件 raise 的回滚惯用法:这里不需要 CancelledError 分支,
                # 取消照样穿透(异常原样向外抛)。
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise

    # ── 守卫行 ──────────────────────────────────────────────────────────────

    @staticmethod
    async def _acquire_player_guard(cur, player_id: int) -> None:  # noqa: ANN001
        """取该玩家守卫行的悲观点锁,持有到事务结束。见文件头 ①。

        `INSERT ... ON DUPLICATE KEY UPDATE player_id = player_id` 一条语句同时完成
        「不存在则建行、存在则锁行」;普通 `SELECT FOR UPDATE` 在行不存在时(TiDB)
        一把锁都不加。守卫行无业务数据,只是锁载体;每玩家至多 1 行(§9.24 登记豁免)。
        """
        await cur.execute(
            "INSERT INTO mission_player_guards (player_id) VALUES (%s) "
            "ON DUPLICATE KEY UPDATE player_id = player_id",
            (player_id,),
        )

    # ── 状态载入 ────────────────────────────────────────────────────────────

    async def _load_state(
        self, cur, player_id: int, for_update: bool
    ) -> eng.PlayerState:  # noqa: ANN001
        lock = " FOR UPDATE" if for_update else ""
        # 只读路径按 mission_config_id 稳定序截断,与 biz 侧排序同序。
        done_limit = "" if for_update else f" ORDER BY mission_config_id LIMIT {self._done_read_limit}"

        st = eng.PlayerState(player_id=player_id)

        await cur.execute(
            "SELECT mission_config_id, progress, accepted_at_ms "
            "FROM player_mission_active WHERE player_id = %s" + lock,
            (player_id,),
        )
        for mid, blob, accepted in await cur.fetchall():
            record = mission_pb2.MissionProgressStorageRecord()
            try:
                record.ParseFromString(bytes(blob or b""))
            except Exception as exc:  # noqa: BLE001
                # 坏行 fail-closed:静默清零会让玩家进度凭空回退(§16 不吞错)。
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "progress blob 解码失败 player=%d mission=%d: %s",
                    player_id,
                    int(mid),
                    exc,
                ) from exc
            st.active[int(mid)] = eng.ActiveMission(
                mission_config_id=int(mid),
                progress=list(record.progress),
                accepted_at_ms=int(accepted),
            )

        await cur.execute(
            "SELECT mission_config_id, reward_state, completed_at_ms "
            "FROM player_mission_done WHERE player_id = %s" + done_limit + lock,
            (player_id,),
        )
        for mid, state, completed in await cur.fetchall():
            st.done[int(mid)] = eng.DoneMission(
                mission_config_id=int(mid),
                reward_state=int(state),
                completed_at_ms=int(completed),
            )
        return st

    async def load_player(self, player_id: int) -> eng.PlayerState:
        """无锁读全量状态(ListMissions / resync 回源)。"""
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                return await self._load_state(cur, player_id, for_update=False)

    # ── 领域事务入口 ────────────────────────────────────────────────────────

    async def mutate_player(self, player_id: int, fn) -> None:  # noqa: ANN001
        """事务内 FOR UPDATE 载入 → fn 计算突变 → 同事务持久化。

        fn 抛异常时整个事务回滚(异常原样透出,业务错误码不被吞成 ErrInternal)。
        """
        async with self._write_tx() as cur:
            await self._acquire_player_guard(cur, player_id)
            st = await self._load_state(cur, player_id, for_update=True)
            mut = await _maybe_await(fn(st))
            await self._persist(cur, player_id, mut)

    async def apply_facts_tx(
        self, player_id: int, idem_key: str, fingerprint: bytes, fn
    ) -> bool:  # noqa: ANN001
        """同 mutate_player,外加事实收据幂等。返回 already(同键已存在且指纹一致)。

        锁序纪律:守卫行**恒为第一把锁**(与 mutate_player 一致),两条路径不会互相
        等成环。收据先行(同事务):撞 uk → 指纹比对 —— 一致 = 纯重放幂等吸收,
        不一致 = 同键串改账 fail-closed(inventory claimLedger 同款,§16.2)。
        """
        async with self._write_tx() as cur:
            await self._acquire_player_guard(cur, player_id)
            try:
                await cur.execute(
                    "INSERT INTO mission_fact_receipts "
                    "(player_id, idempotency_key, request_fingerprint) VALUES (%s, %s, %s)",
                    (player_id, idem_key, fingerprint),
                )
            except Exception as exc:  # noqa: BLE001
                if not mysqlx.is_duplicate_entry(exc):
                    raise
                await cur.execute(
                    "SELECT request_fingerprint FROM mission_fact_receipts "
                    "WHERE player_id = %s AND idempotency_key = %s",
                    (player_id, idem_key),
                )
                row = await cur.fetchone()
                existing = bytes(row[0]) if row and row[0] is not None else b""
                if existing != fingerprint:
                    raise errcode.PandoraError(
                        errcode.ErrMissionFactsConflict,
                        "fact key reused with different content player=%d key=%s",
                        player_id,
                        idem_key,
                    ) from exc
                # 空事务提交:无副作用。**必须提交而不是回滚** —— 回滚会把守卫行
                # 的锁一起放掉没关系,但把"已经确认是重放"这件事变成一次失败,
                # 上游会当作可重试错误一直重投。
                return True
            st = await self._load_state(cur, player_id, for_update=True)
            mut = await _maybe_await(fn(st))
            await self._persist(cur, player_id, mut)
            return False

    # ── 持久化 ──────────────────────────────────────────────────────────────

    async def _persist(self, cur, player_id: int, mut) -> None:  # noqa: ANN001
        if mut is None:
            return
        now_ms = int(time.time() * 1000)

        for am in mut.upsert_active:
            blob = mission_pb2.MissionProgressStorageRecord(
                progress=am.progress
            ).SerializeToString()
            dbguard.check_payload(
                "pandora_mission.player_mission_active.progress", blob, PROGRESS_PAYLOAD_MAX
            )
            await cur.execute(
                "INSERT INTO player_mission_active "
                "(player_id, mission_config_id, progress, accepted_at_ms) "
                "VALUES (%s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE progress = VALUES(progress)",
                (player_id, am.mission_config_id, blob, am.accepted_at_ms),
            )

        for mid in mut.delete_active:
            await cur.execute(
                "DELETE FROM player_mission_active "
                "WHERE player_id = %s AND mission_config_id = %s",
                (player_id, mid),
            )

        for dm in mut.insert_done:
            # uk 撞行 = 引擎不变量被破坏(接取校验挡了已完成任务),fail-closed 整事务回滚。
            await cur.execute(
                "INSERT INTO player_mission_done "
                "(player_id, mission_config_id, reward_state, completed_at_ms) "
                "VALUES (%s, %s, %s, %s)",
                (player_id, dm.mission_config_id, dm.reward_state, dm.completed_at_ms),
            )

        for mid in mut.claim_done:
            await cur.execute(
                "UPDATE player_mission_done SET reward_state = %s "
                "WHERE player_id = %s AND mission_config_id = %s AND reward_state = %s",
                (
                    eng.REWARD_STATE_CLAIMED,
                    player_id,
                    mid,
                    eng.REWARD_STATE_CLAIMABLE,
                ),
            )
            # FOR UPDATE 下不该出现;条件更新兜底(§16.1 TOCTOU 双保险)。
            if (cur.rowcount or 0) == 0:
                raise errcode.PandoraError(
                    errcode.ErrMissionNotClaimable,
                    "claim cas miss mission=%d player=%d",
                    mid,
                    player_id,
                )

        for entry in mut.reward_logs:
            dbguard.check_payload(
                "pandora_mission.mission_reward_log.reward_pb",
                entry.reward_pb,
                REWARD_PAYLOAD_MAX,
            )
            try:
                await cur.execute(
                    "INSERT INTO mission_reward_log "
                    "(player_id, mission_config_id, grant_idempotency_key, status, "
                    "reward_pb, created_at_ms, updated_at_ms) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        player_id,
                        entry.mission_config_id,
                        entry.key,
                        REWARD_STATUS_PENDING,
                        entry.reward_pb,
                        now_ms,
                        now_ms,
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                if not mysqlx.is_duplicate_entry(exc):
                    raise
                # 撞 uk_grant_idem = 该任务的发奖流水已存在(历史重放),回填既有行 ID,
                # 由补扫按其真实状态处置(GRANTED 行不会被重发)。
                await cur.execute(
                    "SELECT id FROM mission_reward_log WHERE grant_idempotency_key = %s",
                    (entry.key,),
                )
                row = await cur.fetchone()
                if row is None:
                    raise
                entry.id = int(row[0])
                continue
            entry.id = int(cur.lastrowid or 0)

        for payload in mut.push_payloads:
            dbguard.check_payload(
                "pandora_mission.mission_push_outbox.payload", payload, PUSH_PAYLOAD_MAX
            )
            await cur.execute(
                "INSERT INTO mission_push_outbox (player_id, payload, created_at_ms) "
                "VALUES (%s, %s, %s)",
                (player_id, payload, now_ms),
            )

    # ── 发奖补扫工作集 ──────────────────────────────────────────────────────

    async def list_ungranted_rewards(
        self, older_than_ms: int, limit: int
    ) -> list[RewardLogRow]:
        """status<>GRANTED 且 updated_at_ms 早于 grace 的行,按 id 序。"""
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id, player_id, mission_config_id, grant_idempotency_key, reward_pb "
                    "FROM mission_reward_log WHERE status <> %s AND updated_at_ms < %s "
                    "ORDER BY id LIMIT %s",
                    (REWARD_STATUS_GRANTED, older_than_ms, limit),
                )
                rows = await cur.fetchall()
        return [
            RewardLogRow(
                id=int(r[0]),
                player_id=int(r[1]),
                mission_config_id=int(r[2]),
                key=str(r[3]),
                reward_pb=bytes(r[4] or b""),
            )
            for r in rows
        ]

    async def mark_reward(self, log_id: int, granted: bool, now_ms: int) -> None:
        """更新发奖流水状态。

        ★ **GRANTED 是终态,任何副本都不得把它改回 FAILED**(§16.1/§16.4)。
        多副本补扫是刻意允许的(正确性由下游幂等键保证,不引入 claim/lease),但那
        意味着两个副本可能同时处理同一行:A 发放成功正要写 GRANTED,B 因下游瞬时不可用
        写 FAILED。若无条件覆盖,已发放的行会被打回补发工作集,然后:
          · 每轮补扫都重放它(下游幂等键吸收,但白烧配额与日志);
          · `status<>1 且超期` 的行永不收敛,"陈年 FAILED = 发放链有 bug"这个审计信号
            被噪声淹没;
          · 保留期把下游幂等记录清掉之后(90 天),再一次重放就是**真的重复发放**。
        所以失败标记带 `status <> 1` 条件更新;成功标记无条件(终态推进,重复写同值幂等)。
        命中 0 行是正常并发结果,不算错误。
        """
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                if granted:
                    await cur.execute(
                        "UPDATE mission_reward_log SET status = %s, updated_at_ms = %s "
                        "WHERE id = %s",
                        (REWARD_STATUS_GRANTED, now_ms, log_id),
                    )
                else:
                    await cur.execute(
                        "UPDATE mission_reward_log SET status = %s, updated_at_ms = %s "
                        "WHERE id = %s AND status <> %s",
                        (REWARD_STATUS_FAILED, now_ms, log_id, REWARD_STATUS_GRANTED),
                    )
            await conn.commit()

    # ── 推送出箱 ────────────────────────────────────────────────────────────

    async def fetch_push_outbox(self, limit: int) -> list[PushOutboxRow]:
        """FIFO 按 id 序取出箱行。"""
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id, player_id, payload FROM mission_push_outbox "
                    "ORDER BY id LIMIT %s",
                    (limit,),
                )
                rows = await cur.fetchall()
        return [
            PushOutboxRow(id=int(r[0]), player_id=int(r[1]), payload=bytes(r[2] or b""))
            for r in rows
        ]

    async def delete_push_outbox(self, row_id: int) -> None:
        """删一行出箱。

        命中 0 行 = **另一个副本已经投过并删掉了这一行**,也就是两个发布器正在同一张
        出箱表上打架:两边各自持有一份内存快照,投递顺序会交错,同玩家的旧进度快照
        可能在新快照之后到达客户端(progressed 是全量快照,后到即覆盖)。旧实现丢弃
        rowcount,这件事在日志与 metric 里完全不可见 —— 先让它可见,这也是验证
        「单写者是否真的生效」的唯一手段。
        """
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "DELETE FROM mission_push_outbox WHERE id = %s", (row_id,)
                )
                affected = cur.rowcount or 0
            await conn.commit()
        if affected == 0:
            raise PushOutboxRacedError(
                f"mission_push_outbox 行 {row_id} 已被其它副本删除(多副本并发发布,推送可能乱序)"
            )

    # ── 保留期清理(§9.24)──────────────────────────────────────────────────

    async def sweep_reward_log(self, mode, retention_days: int, batch: int) -> None:  # noqa: ANN001
        """只清 GRANTED 且超期;PENDING/FAILED 永不清(它们是补发工作集)。"""
        cutoff = int(time.time() * 1000) - retention_days * 86_400_000
        async with self._pool.acquire() as conn:
            await dbguard.sweep_table(
                conn,
                mode,
                self._schema,
                "mission_reward_log",
                "status = %s AND updated_at_ms < %s",
                batch,
                REWARD_STATUS_GRANTED,
                cutoff,
            )
            await conn.commit()

    async def sweep_receipts(self, mode, retention_days: int, batch: int) -> None:  # noqa: ANN001
        """收据清理(组级闸在 biz;这里只按模式执行)。"""
        async with self._pool.acquire() as conn:
            await dbguard.sweep_table(
                conn,
                mode,
                self._schema,
                "mission_fact_receipts",
                "created_at < DATE_SUB(NOW(), INTERVAL %s DAY)",
                batch,
                retention_days,
            )
            await conn.commit()


async def _maybe_await(value):  # noqa: ANN001, ANN201
    """允许 fn 是同步或协程 —— biz 的引擎回调是纯同步,发奖快照构造也是。"""
    if hasattr(value, "__await__"):
        return await value
    return value
