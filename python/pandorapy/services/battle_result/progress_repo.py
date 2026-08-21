"""battle_result 实时进度 / 任务事实出箱仓储 —— 对应 Go 侧
`internal/data/progress_repo.go` + `internal/data/mission_outbox_repo.go`。

拆成独立 mixin(而不是继续堆进 repo.py)与 Go 拆两个文件同因:实时进度是一整套
自洽的水位 + 出箱 + action outcome 机制,混进结算仓储后没人能一眼看出"哪些 SQL 属于
哪条链"。`MySQLBattleRepo` 通过继承本 mixin 拿到全部方法,调用点签名与 Go 一一对应。

库表(deploy/mysql-init/05-battle-outbox.sql / tools/migrate pandora_battle 000005+000006
+000009+000010):

    battle_progress_stream        每场进度水位(PK match_id;last_applied_seq 单调推进,
                                  settled_at_ms>0 = 已结算,stopped_at_ms>0 = 已停流)
    battle_progress_outbox        进度事实事务出箱(uk match+seq+player+kind)
    battle_progress_player        每场每玩家累计(单玩家上限判定依据)
    battle_progress_item_balance  本场 stack 拾取额度 / 支出预留
    battle_progress_action        consume/discard 的持久 outcome
    battle_mission_outbox         任务事实转发出箱(与进度出箱**分表**,故障域隔离)

幂等 / 原子(§16.1 / §16.2):水位推进(乐观 CAS:`WHERE last_applied_seq=expected
AND settled_at_ms=0 AND stopped_at_ms=0`)、单场 / 单玩家累计上限判定、进度出箱行、
item balance 预留、action 行、任务出箱行 **全在同一个 MySQL 事务**。CAS 失败(并发写者 /
已结算 / 已停流)或超限 → 整批回滚(零副作用),DS 按错误语义重试 / 丢批 / 停流。

★ 上限判定为什么必须在事务内(而不是"先读累计再判"):事务外读到的累计与水位 CAS
  分属不同快照,重试请求可能读到旧水位 + 新累计,把同批 delta 重复计入后返回**永久**
  ErrInvalidArg —— 而 DS 按契约会丢批并释放拾取认领,首请求的出箱却已提交,
  重新拾取即重复发放(审计 P1)。

★ 任务出箱与进度出箱**分表但同事务**:分表是故障域隔离(mission 不可用不该堵住该玩家
  的经验 / 掉落);同事务是不丢事实(分两次提交会留下"seq 已推进但任务事实未落箱"的
  窗口,而 DS 不会重发)。
"""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import time

from pandorapy import errcode, mysqlx

# 出箱批量默认值(与 Go 的 `if limit <= 0 { limit = 128 }` 同值)。
DEFAULT_OUTBOX_LIMIT = 128

# uint32 上界:CSV 展开计数溢出判定用(§显式判 int64/uint64 边界)。
_UINT32_MAX = 0xFFFFFFFF


class ProgressGrantKind(enum.IntEnum):
    """进度出箱行的发放类型。对应 Go 的 data.ProgressGrantKind(uint8)。

    ⚠️ 数值是**已落库的存量语义**,不是可重排的枚举:INSTANCE=2 保持旧出箱兼容。
    """

    EXP = 1  # 经验入账(player.AddExperience)
    INSTANCE = 2  # 装备掉落发放(inventory.GrantInstances)
    STACK = 3  # 可堆叠掉落发放(inventory.GrantItems)
    CONSUME_STACK = 4  # 局内消费持久扣减(inventory.ConsumeBattleItem)
    DISCARD_STACK = 5  # 副本丢弃持久扣减(inventory.DiscardBattleItem)


# 历史名称兼容别名(对应 Go 的 ProgressGrantItem = ProgressGrantInstance)。
PROGRESS_GRANT_ITEM = ProgressGrantKind.INSTANCE


class ProgressActionStatus(enum.IntEnum):
    """consume/discard 的 durable completion outcome。对应 Go 的 data.ProgressActionStatus。

    PENDING 只表示"事实与出箱已接受";只有 SUCCEEDED 才允许 ReportProgress 返回 OK。
    """

    PENDING = 0
    SUCCEEDED = 1
    FAILED = 2


@dataclasses.dataclass(slots=True)
class ProgressAction:
    """一个独立 action 事实及其最终结果。对应 Go 的 data.ProgressAction。

    item_config_id / count 同时充当**请求指纹**:同 seq 改 payload 必须拒绝。
    result_code 只在 FAILED 时非 OK。
    """

    match_id: int = 0
    seq: int = 0
    player_id: int = 0
    kind: int = 0
    item_config_id: int = 0
    count: int = 0
    status: int = ProgressActionStatus.PENDING
    result_code: int = errcode.OK


@dataclasses.dataclass(slots=True)
class ProgressOutboxRecord:
    """一条待发放的进度出箱记录。对应 Go 的 data.ProgressOutboxRecord。

    幂等键 = `progress:{match_id}:{seq}:{player_id}:{kind 名}`。
    seq:exp 行 = 批末事件 seq(批内按玩家聚合);item 行 = 该拾取事实自身的 seq
    (每事实一行,天然不超 CSV 列宽,合法掉落永不截断 —— 审计 P1)。
    """

    match_id: int = 0
    seq: int = 0
    player_id: int = 0
    kind: int = 0
    exp_delta: int = 0  # kind=EXP 时有效
    item_config_ids: list[int] = dataclasses.field(default_factory=list)
    # consume/discard 的紧凑计数;0 表示兼容旧的"重复 CSV"表示法。
    item_count: int = 0
    id: int = 0


@dataclasses.dataclass(slots=True)
class ProgressWatermark:
    """一场对局的进度水位快照。对应 Go 的 data.ProgressWatermark。"""

    # 已应用批末 seq(0 = 尚未入账任何批)。
    last_applied_seq: int = 0
    # 本场已累计入账经验(事实换算后,累计上限依据)。
    total_exp: int = 0
    # 本场已累计入账掉落件数。
    total_items: int = 0
    # 对局已结算(终局标记,迟到进度一律拒)。
    settled: bool = False
    # 实时通道已停流(未知事实 / 违纪混版的持久标记,后续进度一律拒)。
    # 无持久标记时,违纪 DS 停流后再发只含已知事实的批会被重新接受(审计 P1)。
    stopped: bool = False
    # 水位行是否存在(= 本场已走实时通道;killswitch 中途关闭不影响已开流对局)。
    existed: bool = False


@dataclasses.dataclass(slots=True)
class ProgressPlayerTotals:
    """本场单个玩家的累计入账快照。对应 Go 的 data.ProgressPlayerTotals。

    只按场累计时,失陷 DS 可以把全场额度灌给一个人(审计 P1),所以必须有单玩家维度。
    """

    total_exp: int = 0
    total_items: int = 0
    total_kills: int = 0


@dataclasses.dataclass(slots=True)
class ProgressPlayerDelta:
    """本批某玩家的新增累计(与水位 CAS 同事务 upsert)。对应 Go 的 data.ProgressPlayerDelta。"""

    player_id: int
    exp: int = 0
    items: int = 0
    kills: int = 0


@dataclasses.dataclass(slots=True)
class ProgressCaps:
    """单场 / 单场单玩家累计上限。对应 Go 的 data.ProgressCaps。

    各项必须 > 0(由 conf 的 `*_or_default()` 取值保证)。
    """

    match_exp: int
    match_items: int
    player_exp: int
    player_items: int
    player_kills: int


@dataclasses.dataclass(slots=True)
class MissionFactRecord:
    """一条待转发的任务事实(一事件一行)。对应 Go 的 data.MissionFactRecord。

    category / slot_value 对应 `pandora.mission.v1` 的 MissionConditionCategory
    与该类别的槽位1语义(杀怪=怪物配置 ID,拾取 / 使用道具=道具配置 ID);
    amount 是进度增量。幂等键 = `progress:{match_id}:{seq}:{player_id}:mission`,
    由 mission 侧 `mission_fact_receipts`(uk + 请求指纹)吸收 at-least-once 重放。
    """

    match_id: int = 0
    seq: int = 0
    player_id: int = 0
    category: int = 0
    slot_value: int = 0
    amount: int = 0
    # True 时行落库即不可投递,等局内消费扣除落定(ResolveProgressAction 置 0 或删行)。
    # 只有 USE_ITEM 类事实用得上:扣除可能以业务失败终态收场(道具不足),
    # 照发就等于让"上报根本没发生的消耗"刷「使用 N 个 X」型任务(§9.6)。
    pending_action: bool = False
    id: int = 0


def progress_kind_column(kind: int) -> int:
    """出箱行 kind 列的存储值(Go 侧是 `uint8(row.Kind)`)。"""
    return int(kind)


def progress_single_stack_fact(row: ProgressOutboxRecord) -> tuple[int, int]:
    """从 consume/discard 出箱行还原 (item_config_id, count)。

    对应 Go 的 progressSingleStackFact。两种表示法都要认:
      · 紧凑法:`item_config_ids=[X]` + `item_count=N`;
      · 旧法:`item_config_ids=[X, X, ...]`(重复 N 次)+ `item_count=0`。
    混着不同 item 的行是坏数据,直接拒 —— 放过去会让扣减额度记到错误的道具上。
    """
    ids = row.item_config_ids
    if not ids or ids[0] == 0:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "empty battle stack action")
    if row.item_count > 0:
        if len(ids) != 1:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "compact battle stack action must contain exactly one item_config_id",
            )
        return ids[0], row.item_count
    if len(ids) > _UINT32_MAX:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "battle stack action count overflows uint32"
        )
    item_id = ids[0]
    for got in ids[1:]:
        if got != item_id:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle stack action mixes item_config_ids %d and %d",
                item_id,
                got,
            )
    return item_id, len(ids)


class ProgressRepoMixin:
    """实时进度 / 任务事实出箱的仓储方法。由 `MySQLBattleRepo` 继承。

    只依赖 `self._pool`(mixin 不持有自己的状态,故 `__slots__` 为空 —— 加了字段会与
    宿主类的 `__slots__` 冲突)。
    """

    __slots__ = ()

    # ── 水位 ──────────────────────────────────────────────────────────────

    async def get_progress_watermark(self, match_id: int) -> ProgressWatermark:
        """读一场对局的进度水位。行不存在 → 零值(existed=False)。"""
        rows = await self._query(  # type: ignore[attr-defined]
            "SELECT last_applied_seq, total_exp, total_items, settled_at_ms, stopped_at_ms "
            "FROM battle_progress_stream WHERE match_id = %s LIMIT 1",
            (match_id,),
            f"query progress watermark match={match_id}",
            code=errcode.ErrUnavailable,
        )
        if not rows:
            return ProgressWatermark()
        last_seq, total_exp, total_items, settled_ms, stopped_ms = rows[0]
        return ProgressWatermark(
            last_applied_seq=int(last_seq),
            total_exp=int(total_exp),
            total_items=int(total_items),
            settled=int(settled_ms) > 0,
            stopped=int(stopped_ms) > 0,
            existed=True,
        )

    async def claim_progress_legacy(self, match_id: int) -> bool:
        """行不存在才创建停流标记(固化"本场 legacy 结算模式")。

        行已存在时**零修改**:`INSERT IGNORE` 撞 PK 即输掉认领(审计 R4 #11 ——
        不得用 upsert 把开启副本刚开的流停掉)。返回 True = 本副本赢得认领。
        """
        now_ms = int(time.time() * 1000)
        affected = await self._exec(  # type: ignore[attr-defined]
            "INSERT IGNORE INTO battle_progress_stream "
            "(match_id, last_applied_seq, total_exp, total_items, final_seq, "
            "settled_at_ms, stopped_at_ms, updated_at_ms) "
            "VALUES (%s, 0, 0, 0, 0, 0, %s, %s)",
            (match_id, now_ms, now_ms),
            f"claim progress legacy match={match_id}",
            code=errcode.ErrUnavailable,
        )
        return affected == 1

    async def mark_progress_stopped(self, match_id: int) -> None:
        """持久化停流标记(幂等:只记录首次停流时间)。

        行不存在时创建 —— 首批就含未知事实的场景也必须留标记,否则后续"只含已知事实"
        的批会重新开流。⚠️ upsert 语义,仅供**流内确定停流**(未知事实)使用;
        通道关闭固化走 `claim_progress_legacy`。
        """
        now_ms = int(time.time() * 1000)
        await self._exec(  # type: ignore[attr-defined]
            "INSERT INTO battle_progress_stream "
            "(match_id, last_applied_seq, total_exp, total_items, final_seq, "
            "settled_at_ms, stopped_at_ms, updated_at_ms) "
            "VALUES (%s, 0, 0, 0, 0, 0, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "stopped_at_ms = IF(stopped_at_ms = 0, VALUES(stopped_at_ms), stopped_at_ms), "
            "updated_at_ms = VALUES(updated_at_ms)",
            (match_id, now_ms, now_ms),
            f"mark progress stopped match={match_id}",
            code=errcode.ErrUnavailable,
        )

    # ── 入账事务 ──────────────────────────────────────────────────────────

    async def apply_progress(  # noqa: C901, PLR0912, PLR0913 —— 与 Go 同为线性事务
        self,
        match_id: int,
        expected_seq: int,
        new_seq: int,
        add_exp: int,
        add_items: int,
        player_deltas: list[ProgressPlayerDelta],
        rows: list[ProgressOutboxRecord],
        mission_rows: list[MissionFactRecord],
        caps: ProgressCaps,
    ) -> None:
        """原子推进水位、判定累计上限并写进度 / 任务出箱(同一事务)。

        · `expected_seq` 是调用方读到的水位(乐观 CAS 期望值);`new_seq` 是本批批末 seq。
        · `add_exp` / `add_items` 与水位同一 CAS 行累计。
        · `caps` 在**本事务一致快照**上判定(水位行已被本事务写锁定,并发批次被 CAS
          串行化),超限 → ErrInvalidArg 并整体回滚(零副作用)。
        · CAS 失败(并发写者抢先 / 已结算 / 已停流)→ ErrUnavailable(瞬时,DS 重试后
          按新水位去重收敛);首批 INSERT 撞 PK 同样按 ErrUnavailable 收敛。
        """
        if match_id == 0 or new_seq <= expected_seq:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "apply progress requires match/seq advance"
            )
        action_rows = sum(
            1
            for row in rows
            if row.kind
            in (ProgressGrantKind.CONSUME_STACK, ProgressGrantKind.DISCARD_STACK)
        )
        # action 行必须**独占一批**:混批会让"扣减失败整批回滚"连坐掉同批的经验 / 掉落,
        # 而 DS 侧对两者的重试语义完全不同。
        if action_rows > 0 and (
            action_rows != 1
            or len(rows) != 1
            or add_exp != 0
            or add_items != 0
            or player_deltas
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "consume/discard progress action must be one isolated outbox row",
            )

        now_ms = int(time.time() * 1000)
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            try:
                await conn.begin()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrUnavailable, "begin progress tx match=%d: %s", match_id, exc
                ) from exc
            try:
                async with conn.cursor() as cur:
                    await self._advance_watermark_tx(
                        cur, match_id, expected_seq, new_seq, add_exp, add_items, now_ms
                    )
                    await self._assert_match_caps_tx(cur, match_id, add_exp, add_items, caps)
                    await self._upsert_player_totals_tx(cur, match_id, player_deltas, now_ms)
                    await self._assert_player_caps_tx(cur, match_id, player_deltas, caps)
                    await self._insert_progress_rows_tx(cur, match_id, rows, now_ms)
                    # 任务事实出箱与水位 CAS 同事务:分开写会在两次提交之间留下
                    # "seq 已推进但事实未落箱"的窗口 —— DS 不会重发,事实永久丢失。
                    await self._insert_mission_facts_tx(cur, mission_rows, now_ms)
                await conn.commit()
            except BaseException:
                # ★ 无条件 re-raise:回滚是必须做的清理(不做会留下悬挂事务占着连接),
                # 取消照样穿透出去。
                try:
                    await conn.rollback()
                except Exception:  # noqa: BLE001 —— 回滚失败不得顶掉真正的失败原因
                    pass
                raise

    async def _advance_watermark_tx(  # noqa: ANN001, PLR0913
        self, cur, match_id: int, expected_seq: int, new_seq: int,
        add_exp: int, add_items: int, now_ms: int,
    ) -> None:
        """水位 CAS。首批 INSERT,后续 UPDATE 带 settled/stopped fencing 条件。"""
        if expected_seq == 0:
            # 已结算 / 已停流对局的行**永远存在**(save_result 落终局标记 /
            # mark_progress_stopped 落停流标记),INSERT 撞 PK 即被拒 → 调用方重读水位
            # 看到 settled/stopped,天然 fail-closed。
            try:
                await cur.execute(
                    "INSERT INTO battle_progress_stream "
                    "(match_id, last_applied_seq, total_exp, total_items, final_seq, "
                    "settled_at_ms, updated_at_ms) VALUES (%s, %s, %s, %s, 0, 0, %s)",
                    (match_id, new_seq, add_exp, add_items, now_ms),
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                if mysqlx.is_duplicate_entry(exc):
                    raise errcode.PandoraError(
                        errcode.ErrUnavailable,
                        "progress watermark contended match=%d",
                        match_id,
                    ) from exc
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "insert progress watermark match=%d: %s",
                    match_id,
                    exc,
                ) from exc
            return
        try:
            await cur.execute(
                "UPDATE battle_progress_stream SET last_applied_seq = %s, "
                "total_exp = total_exp + %s, total_items = total_items + %s, "
                "updated_at_ms = %s "
                "WHERE match_id = %s AND last_applied_seq = %s "
                "AND settled_at_ms = 0 AND stopped_at_ms = 0",
                (new_seq, add_exp, add_items, now_ms, match_id, expected_seq),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "advance progress watermark match=%d: %s",
                match_id,
                exc,
            ) from exc
        if (cur.rowcount or 0) == 0:
            # 期望水位不匹配 / 已结算 / 已停流:让调用方重读水位。已结算场景重读后
            # 会拿到明确的 ErrInvalidState(biz 判 settled),不会无限重试。
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "progress watermark moved or settled match=%d",
                match_id,
            )

    async def _assert_match_caps_tx(  # noqa: ANN001
        self, cur, match_id: int, add_exp: int, add_items: int, caps: ProgressCaps
    ) -> None:
        """单场累计上限。读回的是入账后的权威累计(水位行已被本事务写锁定)。"""
        try:
            await cur.execute(
                "SELECT total_exp, total_items FROM battle_progress_stream WHERE match_id = %s",
                (match_id,),
            )
            row = await cur.fetchone()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "read progress totals match=%d: %s", match_id, exc
            ) from exc
        if row is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "read progress totals match=%d: row vanished", match_id
            )
        cur_exp, cur_items = int(row[0]), int(row[1])
        if cur_exp > caps.match_exp:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "match %d cumulative exp %d exceeds per-match cap %d (batch +%d)",
                match_id,
                cur_exp,
                caps.match_exp,
                add_exp,
            )
        if cur_items > caps.match_items:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "match %d cumulative items %d exceeds per-match cap %d (batch +%d)",
                match_id,
                cur_items,
                caps.match_items,
                add_items,
            )

    async def _upsert_player_totals_tx(  # noqa: ANN001
        self, cur, match_id: int, deltas: list[ProgressPlayerDelta], now_ms: int
    ) -> None:
        """单玩家累计与水位同事务推进(CAS 保护下 upsert 累加无竞态)。"""
        sql = (
            "INSERT INTO battle_progress_player "
            "(match_id, player_id, total_exp, total_items, total_kills, updated_at_ms) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE total_exp = total_exp + VALUES(total_exp), "
            "total_items = total_items + VALUES(total_items), "
            "total_kills = total_kills + VALUES(total_kills), "
            "updated_at_ms = VALUES(updated_at_ms)"
        )
        for d in deltas:
            try:
                await cur.execute(
                    sql, (match_id, d.player_id, d.exp, d.items, d.kills, now_ms)
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "upsert progress player totals match=%d player=%d: %s",
                    match_id,
                    d.player_id,
                    exc,
                ) from exc

    async def _assert_player_caps_tx(  # noqa: ANN001
        self, cur, match_id: int, deltas: list[ProgressPlayerDelta], caps: ProgressCaps
    ) -> None:
        """单玩家累计上限(同一事务一致快照,理由同单场上限)。"""
        if not deltas:
            return
        placeholders = ",".join(["%s"] * len(deltas))
        try:
            await cur.execute(
                "SELECT player_id, total_exp, total_items, total_kills "
                f"FROM battle_progress_player WHERE match_id = %s AND player_id IN ({placeholders})",  # noqa: S608
                (match_id, *[d.player_id for d in deltas]),
            )
            rows = await cur.fetchall()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "query progress player totals match=%d: %s",
                match_id,
                exc,
            ) from exc
        for pid, total_exp, total_items, total_kills in rows:
            if int(total_exp) > caps.player_exp:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "match %d player %d cumulative exp %d exceeds per-player cap %d",
                    match_id,
                    int(pid),
                    int(total_exp),
                    caps.player_exp,
                )
            if int(total_items) > caps.player_items:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "match %d player %d cumulative items %d exceeds per-player cap %d",
                    match_id,
                    int(pid),
                    int(total_items),
                    caps.player_items,
                )
            if int(total_kills) > caps.player_kills:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "match %d player %d cumulative kills %d exceeds per-player cap %d",
                    match_id,
                    int(pid),
                    int(total_kills),
                    caps.player_kills,
                )

    async def _insert_progress_rows_tx(  # noqa: ANN001
        self, cur, match_id: int, rows: list[ProgressOutboxRecord], now_ms: int
    ) -> None:
        """写进度出箱行;stack pickup / action 先过 item balance 权威。"""
        from pandorapy.services.battle_result import repo as brepo  # 避免循环 import

        sql = (
            "INSERT INTO battle_progress_outbox "
            "(match_id, seq, player_id, kind, exp_delta, item_config_ids, item_count, "
            "created_at_ms) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        )
        for row in rows:
            await self._apply_item_authority_tx(cur, match_id, row, now_ms)
            try:
                await cur.execute(
                    sql,
                    (
                        match_id,
                        row.seq,
                        row.player_id,
                        progress_kind_column(row.kind),
                        row.exp_delta,
                        brepo.encode_config_ids(row.item_config_ids),
                        row.item_count,
                        now_ms,
                    ),
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "insert progress outbox match=%d player=%d: %s",
                    match_id,
                    row.player_id,
                    exc,
                ) from exc

    async def _apply_item_authority_tx(  # noqa: ANN001
        self, cur, match_id: int, row: ProgressOutboxRecord, now_ms: int
    ) -> None:
        """把 phase0 stack pickup 额度与 action 支出预留收进同一个水位事务。

        对应 Go 的 applyProgressItemAuthorityTx。**只有本场、同玩家、同 item 已接受的
        stack pickup 能提供额度** —— 进入本场前的主库存永远不会增加这里的 picked_count,
        否则 DS 可以用"消耗本来就有的道具"刷任务 / 刷掉落。
        """
        if row.kind == ProgressGrantKind.STACK:
            counts: dict[int, int] = {}
            for item_id in row.item_config_ids:
                if item_id == 0:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg, "stack pickup contains zero item_config_id"
                    )
                counts[item_id] = counts.get(item_id, 0) + 1
            if not counts:
                raise errcode.PandoraError(errcode.ErrInvalidArg, "stack pickup row is empty")
            sql = (
                "INSERT INTO battle_progress_item_balance "
                "(match_id, player_id, item_config_id, picked_count, spent_count, updated_at_ms) "
                "VALUES (%s, %s, %s, %s, 0, %s) "
                "ON DUPLICATE KEY UPDATE picked_count = picked_count + VALUES(picked_count), "
                "updated_at_ms = VALUES(updated_at_ms)"
            )
            for item_id, count in counts.items():
                try:
                    await cur.execute(
                        sql, (match_id, row.player_id, item_id, count, now_ms)
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrUnavailable,
                        "credit battle pickup balance match=%d player=%d item=%d: %s",
                        match_id,
                        row.player_id,
                        item_id,
                        exc,
                    ) from exc
            return
        if row.kind not in (
            ProgressGrantKind.CONSUME_STACK,
            ProgressGrantKind.DISCARD_STACK,
        ):
            return

        item_id, count = progress_single_stack_fact(row)
        try:
            await cur.execute(
                "UPDATE battle_progress_item_balance "
                "SET spent_count = spent_count + %s, updated_at_ms = %s "
                "WHERE match_id = %s AND player_id = %s AND item_config_id = %s "
                "AND spent_count <= picked_count AND picked_count - spent_count >= %s",
                (count, now_ms, match_id, row.player_id, item_id, count),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "reserve battle item spend match=%d player=%d item=%d: %s",
                match_id,
                row.player_id,
                item_id,
                exc,
            ) from exc
        if (cur.rowcount or 0) != 1:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle item action exceeds same-match accepted pickup balance "
                "match=%d player=%d item=%d count=%d; phase0 actions may only spend "
                "this match's stack pickups",
                match_id,
                row.player_id,
                item_id,
                count,
            )
        try:
            await cur.execute(
                "INSERT INTO battle_progress_action "
                "(match_id, seq, player_id, kind, item_config_id, count, status, "
                "result_code, created_at_ms, updated_at_ms) "
                "VALUES (%s, %s, %s, %s, %s, %s, 0, 0, %s, %s)",
                (
                    match_id,
                    row.seq,
                    row.player_id,
                    progress_kind_column(row.kind),
                    item_id,
                    count,
                    now_ms,
                    now_ms,
                ),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "insert battle progress action match=%d seq=%d player=%d: %s",
                match_id,
                row.seq,
                row.player_id,
                exc,
            ) from exc

    async def _insert_mission_facts_tx(  # noqa: ANN001
        self, cur, rows: list[MissionFactRecord], now_ms: int
    ) -> None:
        """在 apply_progress 事务内写任务出箱行。对应 Go 的 insertMissionFactsTx。

        撞 uk(match_id, seq, player_id)说明同一事件被重复展开(理论不可达:seq 去重在前),
        按幂等忽略而不是让整批失败 —— 重复插入不该把一批合法进度打回。
        """
        if not rows:
            return
        sql = (
            "INSERT IGNORE INTO battle_mission_outbox "
            "(match_id, seq, player_id, category, slot_value, amount, pending_action, "
            "created_at_ms) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        )
        for row in rows:
            try:
                await cur.execute(
                    sql,
                    (
                        row.match_id,
                        row.seq,
                        row.player_id,
                        row.category,
                        row.slot_value,
                        row.amount,
                        1 if row.pending_action else 0,
                        now_ms,
                    ),
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "insert mission outbox match=%d player=%d seq=%d: %s",
                    row.match_id,
                    row.player_id,
                    row.seq,
                    exc,
                ) from exc

    # ── 进度出箱读写 ──────────────────────────────────────────────────────

    async def fetch_progress_outbox(self, limit: int) -> list[ProgressOutboxRecord]:
        """按 id 升序取最多 limit 条**已到重试时点**的待发放进度出箱记录。

        同 (match, player) 只返回 seq/id 最早的一条:即使前序失败已被退避,后序也不能
        越过它先发 —— 保证 ItemPickup 的 Grant 一定先于后续 ItemConsume。
        `next_attempt_at_ms` 过滤 + `defer_progress_outbox` 退避,保证个别永久失败行
        (坏数据 / granter 未配)不会长期占满首批饿死后续正常行(审计 P1 队首阻塞)。
        """
        if limit <= 0:
            limit = DEFAULT_OUTBOX_LIMIT
        rows = await self._query(  # type: ignore[attr-defined]
            "SELECT cur.id, cur.match_id, cur.seq, cur.player_id, cur.kind, cur.exp_delta, "
            "cur.item_config_ids, cur.item_count FROM battle_progress_outbox cur "
            "WHERE cur.next_attempt_at_ms <= %s AND NOT EXISTS ("
            "SELECT 1 FROM battle_progress_outbox prev "
            "WHERE prev.match_id = cur.match_id AND prev.player_id = cur.player_id "
            "AND (prev.seq < cur.seq OR (prev.seq = cur.seq AND prev.id < cur.id))) "
            "ORDER BY cur.id ASC LIMIT %s",
            (int(time.time() * 1000), limit),
            "query progress outbox",
        )
        return [_scan_progress_row(r) for r in rows]

    async def fetch_progress_outbox_for_player(
        self, match_id: int, player_id: int, max_seq: int
    ) -> ProgressOutboxRecord | None:
        """忽略 `next_attempt_at_ms`,供同步 action 路径主动驱动该玩家的最早出箱。

        其它玩家完全不受影响(所以同步路径不会因为"帮别人推队列"而变慢或互相阻塞)。
        """
        rows = await self._query(  # type: ignore[attr-defined]
            "SELECT id, match_id, seq, player_id, kind, exp_delta, item_config_ids, item_count "
            "FROM battle_progress_outbox WHERE match_id = %s AND player_id = %s AND seq <= %s "
            "ORDER BY seq ASC, id ASC LIMIT 1",
            (match_id, player_id, max_seq),
            f"query player progress outbox match={match_id} player={player_id} "
            f"max_seq={max_seq}",
        )
        if not rows:
            return None
        return _scan_progress_row(rows[0])

    async def delete_progress_outbox(self, outbox_id: int) -> None:
        """删除已成功发放的进度出箱行。"""
        await self._exec(  # type: ignore[attr-defined]
            "DELETE FROM battle_progress_outbox WHERE id = %s",
            (outbox_id,),
            f"delete progress outbox id={outbox_id}",
        )

    async def defer_progress_outbox(self, outbox_id: int) -> None:
        """发放失败后指数退避(2s·2^n 封顶 5min)。

        行**永不丢弃** —— 封顶后持续告警由人工介入,at-least-once 语义不变。
        """
        await self._exec(  # type: ignore[attr-defined]
            "UPDATE battle_progress_outbox SET attempt_count = attempt_count + 1, "
            "next_attempt_at_ms = %s + LEAST(2000 * POW(2, LEAST(attempt_count, 7)), 300000) "
            "WHERE id = %s",
            (int(time.time() * 1000), outbox_id),
            f"defer progress outbox id={outbox_id}",
        )

    # ── action outcome ────────────────────────────────────────────────────

    async def get_progress_action(
        self, match_id: int, seq: int, player_id: int, kind: int
    ) -> ProgressAction | None:
        """读 consume/discard 的持久事实与权威结果。

        ★ **不存在不能解释为成功**(返回 None,调用方必须当成"还没有结果")。
        """
        rows = await self._query(  # type: ignore[attr-defined]
            "SELECT match_id, seq, player_id, kind, item_config_id, count, status, result_code "
            "FROM battle_progress_action "
            "WHERE match_id = %s AND seq = %s AND player_id = %s AND kind = %s LIMIT 1",
            (match_id, seq, player_id, progress_kind_column(kind)),
            f"query progress action match={match_id} seq={seq} player={player_id} kind={kind}",
        )
        if not rows:
            return None
        return _scan_progress_action(rows[0])

    async def resolve_progress_action(  # noqa: C901
        self, row: ProgressOutboxRecord, result_code: int
    ) -> ProgressAction:
        """原子落 action outcome 并删除对应 outbox 行。

        inventory RPC 与 battle MySQL 之间的崩溃窗由 inventory 幂等键收敛:RPC 成功后
        若本事务未提交,重试会拿到同一 RPC 结果,再完成本事务。并发执行以**首个已提交
        outcome** 为权威(`FOR UPDATE` + `status=0` 条件更新)。
        """
        item_id, count = progress_single_stack_fact(row)
        if (
            row.id == 0
            or row.match_id == 0
            or row.seq == 0
            or row.player_id == 0
            or row.kind
            not in (ProgressGrantKind.CONSUME_STACK, ProgressGrantKind.DISCARD_STACK)
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "resolve progress action requires persisted action outbox row",
            )
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            try:
                await conn.begin()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "begin resolve progress action: %s", exc
                ) from exc
            try:
                async with conn.cursor() as cur:
                    action = await self._lock_progress_action_tx(cur, row)
                    if action.item_config_id != item_id or action.count != count:
                        raise errcode.PandoraError(
                            errcode.ErrInvalidState,
                            "progress action/outbox mismatch match=%d seq=%d "
                            "stored=%d:%d row=%d:%d",
                            row.match_id,
                            row.seq,
                            action.item_config_id,
                            action.count,
                            item_id,
                            count,
                        )
                    if action.status == ProgressActionStatus.PENDING:
                        await self._settle_action_outcome_tx(
                            cur, action, result_code, count
                        )
                    await cur.execute(
                        "DELETE FROM battle_progress_outbox "
                        "WHERE id = %s AND match_id = %s AND seq = %s AND player_id = %s "
                        "AND kind = %s",
                        (
                            row.id,
                            row.match_id,
                            row.seq,
                            row.player_id,
                            progress_kind_column(row.kind),
                        ),
                    )
                    # 「使用道具」任务事实与扣除结果**同事务**落定:成功放行、失败删行。
                    # 丢弃(discard)不产任务事实,命中 0 行即可。
                    if row.kind == ProgressGrantKind.CONSUME_STACK:
                        await self._settle_mission_fact_pending_tx(
                            cur,
                            action.match_id,
                            action.seq,
                            action.player_id,
                            action.status == ProgressActionStatus.SUCCEEDED,
                        )
                await conn.commit()
            except BaseException:
                try:
                    await conn.rollback()
                except Exception:  # noqa: BLE001
                    pass
                raise
        return action

    async def _lock_progress_action_tx(  # noqa: ANN001
        self, cur, row: ProgressOutboxRecord
    ) -> ProgressAction:
        try:
            await cur.execute(
                "SELECT match_id, seq, player_id, kind, item_config_id, count, status, "
                "result_code FROM battle_progress_action "
                "WHERE match_id = %s AND seq = %s AND player_id = %s AND kind = %s FOR UPDATE",
                (row.match_id, row.seq, row.player_id, progress_kind_column(row.kind)),
            )
            found = await cur.fetchone()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "lock progress action: %s", exc
            ) from exc
        if found is None:
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "progress action outcome missing match=%d seq=%d player=%d kind=%d",
                row.match_id,
                row.seq,
                row.player_id,
                int(row.kind),
            )
        return _scan_progress_action(found)

    async def _settle_action_outcome_tx(  # noqa: ANN001
        self, cur, action: ProgressAction, result_code: int, count: int
    ) -> None:
        """PENDING → SUCCEEDED/FAILED;失败时**同事务释放** spent 额度。

        ★ 释放必须同事务:预留发生在 apply_progress。终态业务失败意味着 inventory 没有
          扣物、UE 也会保留本地物品,不释放的话玩家用新 seq 重试同一意图会被"假余额"
          永久拒绝。
        """
        if result_code == errcode.OK:
            action.status = int(ProgressActionStatus.SUCCEEDED)
            action.result_code = errcode.OK
        else:
            try:
                await cur.execute(
                    "UPDATE battle_progress_item_balance "
                    "SET spent_count = spent_count - %s, updated_at_ms = %s "
                    "WHERE match_id = %s AND player_id = %s AND item_config_id = %s "
                    "AND spent_count >= %s",
                    (
                        count,
                        int(time.time() * 1000),
                        action.match_id,
                        action.player_id,
                        action.item_config_id,
                        count,
                    ),
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "release failed progress action balance: %s", exc
                ) from exc
            affected = cur.rowcount or 0
            if affected != 1:
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "release failed progress action balance match=%d seq=%d affected=%d",
                    action.match_id,
                    action.seq,
                    affected,
                )
            action.status = int(ProgressActionStatus.FAILED)
            action.result_code = result_code
        try:
            await cur.execute(
                "UPDATE battle_progress_action SET status = %s, result_code = %s, "
                "updated_at_ms = %s "
                "WHERE match_id = %s AND seq = %s AND player_id = %s AND kind = %s "
                "AND status = 0",
                (
                    int(action.status),
                    int(action.result_code),
                    int(time.time() * 1000),
                    action.match_id,
                    action.seq,
                    action.player_id,
                    progress_kind_column(action.kind),
                ),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "update progress action outcome: %s", exc
            ) from exc

    # ── 任务事实出箱 ──────────────────────────────────────────────────────

    async def fetch_mission_outbox(self, limit: int) -> list[MissionFactRecord]:
        """取一批到期的任务事实出箱行,**同一 player_id 只返回 id 最小的一条**。

        为什么必须 FIFO(推翻早期"任务事实顺序无关"的说法):任务链前后两环的条件类别
        通常不同(「杀 5 只狼」→「收集 3 张狼皮」),后环任务在前环完成时才被自动接取。
        若"狼皮"事实先于"杀狼"事实投递,mission 侧匹配不上任何活跃任务,进度**静默丢失**
        且事实已被收据吸收、永不重放。乱序的来源就是 defer_mission_outbox:失败行退避后,
        同玩家后续行会越过它先投。改为队首阻塞后,退避的行会把同玩家后续事实一起挡住。

        **按 player_id 而不是 (match_id, player_id) 分组**:任务链是玩家维度的。
        排序键用 **id 而不是 seq**:seq 是每对局自增的,跨对局不可比;id 是插入序,
        而 apply_progress 一个事务一批、批内按 seq 升序插入,所以 id 序在对局内等价于
        seq 序,跨对局等价于对局发生序(§9.1 保证玩家同一时刻只在一个可操作 DS)。

        `pending_action=1` 的行同样占队首但不投递(等局内消费扣除落定),天然实现
        "扣除未落定就不推进使用类任务、也不让后续事实抢跑"。
        """
        if limit <= 0:
            limit = DEFAULT_OUTBOX_LIMIT
        rows = await self._query(  # type: ignore[attr-defined]
            "SELECT cur.id, cur.match_id, cur.seq, cur.player_id, cur.category, "
            "cur.slot_value, cur.amount FROM battle_mission_outbox cur "
            "WHERE cur.next_attempt_at_ms <= %s AND cur.pending_action = 0 "
            "AND NOT EXISTS (SELECT 1 FROM battle_mission_outbox prev "
            "WHERE prev.player_id = cur.player_id AND prev.id < cur.id) "
            "ORDER BY cur.id ASC LIMIT %s",
            (int(time.time() * 1000), limit),
            "query mission outbox",
        )
        return [
            MissionFactRecord(
                id=int(r[0]),
                match_id=int(r[1]),
                seq=int(r[2]),
                player_id=int(r[3]),
                category=int(r[4]),
                slot_value=int(r[5]),
                amount=int(r[6]),
            )
            for r in rows
        ]

    async def delete_mission_outbox(self, outbox_id: int) -> None:
        """投递成功后删行。"""
        await self._exec(  # type: ignore[attr-defined]
            "DELETE FROM battle_mission_outbox WHERE id = %s",
            (outbox_id,),
            f"delete mission outbox id={outbox_id}",
        )

    async def defer_mission_outbox(self, outbox_id: int) -> None:
        """投递失败后指数退避(2s·2^n 封顶 5min;行永不丢弃,与进度出箱同纪律)。"""
        await self._exec(  # type: ignore[attr-defined]
            "UPDATE battle_mission_outbox SET attempt_count = attempt_count + 1, "
            "next_attempt_at_ms = %s + LEAST(2000 * POW(2, LEAST(attempt_count, 7)), 300000) "
            "WHERE id = %s",
            (int(time.time() * 1000), outbox_id),
            f"defer mission outbox id={outbox_id}",
        )

    async def _settle_mission_fact_pending_tx(  # noqa: ANN001, PLR0913
        self, cur, match_id: int, seq: int, player_id: int, granted: bool
    ) -> None:
        """在局内消费**结果落定的同一事务**里解开 USE_ITEM 事实的 pending 闸。

        granted=True 置 0(允许投递),False 直接删行(扣除没发生 = 事实不存在)。

        必须同事务:分两次提交会留下"扣除已失败但任务事实已可投递"的窗口,足够让补扫把
        一条根本没发生的消耗推进任务进度。命中 0 行是正常的(丢弃类事实、mission_addr
        未配时压根没写行、或重复 resolve),不构成错误。
        """
        stmt = (
            "UPDATE battle_mission_outbox SET pending_action = 0 "
            "WHERE match_id = %s AND seq = %s AND player_id = %s AND pending_action = 1"
            if granted
            else "DELETE FROM battle_mission_outbox "
            "WHERE match_id = %s AND seq = %s AND player_id = %s AND pending_action = 1"
        )
        try:
            await cur.execute(stmt, (match_id, seq, player_id))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "settle mission fact pending match=%d seq=%d player=%d granted=%s: %s",
                match_id,
                seq,
                player_id,
                granted,
                exc,
            ) from exc


def _scan_progress_row(r) -> ProgressOutboxRecord:  # noqa: ANN001
    """把一行 SELECT 结果还原成出箱记录(列序与两处 SELECT 完全一致)。"""
    from pandorapy.services.battle_result import repo as brepo  # 避免循环 import

    return ProgressOutboxRecord(
        id=int(r[0]),
        match_id=int(r[1]),
        seq=int(r[2]),
        player_id=int(r[3]),
        kind=int(r[4]),
        exp_delta=int(r[5]),
        item_config_ids=brepo.decode_config_ids(r[6] or ""),
        item_count=int(r[7]),
    )


def _scan_progress_action(r) -> ProgressAction:  # noqa: ANN001
    return ProgressAction(
        match_id=int(r[0]),
        seq=int(r[1]),
        player_id=int(r[2]),
        kind=int(r[3]),
        item_config_id=int(r[4]),
        count=int(r[5]),
        status=int(r[6]),
        result_code=int(r[7]),
    )
