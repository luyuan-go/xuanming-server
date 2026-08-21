"""battle_result 数据层(MySQL)—— 对应 Go 侧 internal/data/battle_repo.go
与 internal/data/progress_repo.go 的 settleProgressStreamTx。

库表(deploy/mysql-init/03-battle-tables.sql + 05-battle-outbox.sql,pandora_battle 库):

    battles                 对局结算头(PK match_id = 幂等键,不变量 §2)
    battle_player_stats     玩家战绩 + MMR 变化(uk match_id+player_id)
    player_update_outbox    段位事件事务出箱(uk match+player)
    battle_drop_outbox      掉落发放事务出箱(uk match+player;三份冻结 CSV)
    match_release_outbox    撮合状态释放事务出箱(uk match)
    terminal_release_outbox Model-B 终态回收证明(uk match)
    battle_progress_stream  实时进度水位(PK match_id;settled_at_ms>0 = 迟到进度一律拒)

★★ 本文件最要紧的一条:`save_result` 里的 `_settle_progress_stream_tx` **必须整体在**。

    它做两件事,少任何一件都会产生"Go 已修好的 bug 被移植回来"的形状:

      ① 打终局标记(settled_at_ms>0)——僵尸 / 分区恢复的 DS 之后再上报进度一律被拒。
         漏了它:DS 崩溃重连后还能继续往一场已结算的对局里刷经验和掉落。

      ② 读回 last_applied_seq,>0 时**抑制结算路径的掉落入箱**(DropsSuppressed)。
         漏了它:一场走过实时通道的对局,掉落会被实时通道发一遍、再被结算路径发一遍 ——
         **双发**。而且这个 bug 只在"Python 副本结算 + Go 副本收过进度"的混部下出现,
         单跑 Python 永远复现不了(因为 Python 侧没有实时进度通道,水位恒 0)。

    所以哪怕 Python 侧不实现 ReportProgress,这段收口逻辑也必须一字不差地在这里。

★ 幂等重放分支(battles 撞 1062)也要跑这两件事:首笔落库若由不含进度逻辑的旧副本完成,
  水位行会一直没打终局标记(审计 P0)。Go 在 dup 分支里同样调了 settleProgressStreamTx。

★ 事务边界:asyncmy 池以 autocommit=True 建(见 main.py),单语句写直接 execute;
  需要原子性的地方显式 `conn.begin()` 包起来。save_result 与保留期批删都是显式事务。
"""

from __future__ import annotations

import asyncio
import dataclasses
import time
import uuid

from pandora.battle.v1 import battle_pb2
from pandora.match.v1 import match_pb2

from pandorapy import dbguard, errcode, mysqlx
from pandorapy import log as plog
from pandorapy.services.battle_result import progress_repo as bprogress
from pandorapy.services.battle_result import terminal_release_repo as bterminal

# 本服的权威库(容量巡检 / 保留期 metric 的 db 标签)。
BATTLE_DB = "pandora_battle"

# 缺表提示直接指向迁移产物,省得值班的人翻仓库。
RECOVERY_SCHEMA_HINT = "apply pandora_battle migration 000003_match_release_outbox"
PROGRESS_SCHEMA_HINT = (
    "apply pandora_battle migrations 000005_battle_progress + "
    "000006_battle_progress_player + 000008_battle_progress_stopped"
)
MISSION_SCHEMA_HINT = (
    "跑 tools/migrate 至 pandora_battle 000010,或把 mission_addr 置空关闭转发"
)


# ── 记录类型 ─────────────────────────────────────────────────────────────────


@dataclasses.dataclass(slots=True)
class OutboxRecord:
    """一条待发布的 player.update 事务出箱记录。对应 Go 的 data.OutboxRecord。"""

    player_id: int
    payload: bytes
    # id 仅 fetch_outbox 返回时填充(save_result 入参时忽略)。
    id: int = 0


@dataclasses.dataclass(slots=True)
class DropOutboxRecord:
    """一条待发放的战斗掉落出箱记录。对应 Go 的 data.DropOutboxRecord。

    ★ stack / instance 两份路由在**首次入箱时冻结**,发布重试绝不重算:
      重试时读热配置会让 stack 已成功、instance 失败期间的类型热更换掉 method + 幂等键,
      造成已成功那部分**双发**。
    """

    player_id: int
    item_config_ids: list[int]
    stack_item_config_ids: list[int] = dataclasses.field(default_factory=list)
    instance_item_config_ids: list[int] = dataclasses.field(default_factory=list)
    id: int = 0
    match_id: int = 0


@dataclasses.dataclass(slots=True)
class MatchReleaseRecord:
    """battle_result→matchmaker 的持久事务 outbox 行。对应 Go 的 data.MatchReleaseRecord。"""

    id: int
    operation_id: str
    match_id: int
    player_ids: list[int]
    attempt_count: int = 0
    next_attempt_at_ms: int = 0
    created_at_ms: int = 0


@dataclasses.dataclass(slots=True)
class TerminalReleaseRecord:
    """正常结算的持久终态回收证明。对应 Go 的 data.TerminalReleaseRecord。

    ★ player_ids / game_mode / map_id / rating_mode / rating_pool 五个字段
      **只活在这次授权结算里,不落 terminal_release_outbox 表**(Go 同样如此,不做迁移)。
      它们来自凭据校验时抓的 canonical BattleStorageRecord,**不是 DS 请求体字段** ——
      reportResult 会用它们覆盖不可信的请求体 game_mode/map_id,MMR 判据也只读它们。
    """

    match_id: int = 0
    allocation_id: str = ""
    ds_pod_name: str = ""
    gameserver_uid: str = ""
    instance_epoch: int = 0
    auth_gen: int = 0
    auth_jti: str = ""
    auth_exp_ms: int = 0
    auth_kid: str = ""
    auth_token_sha256: str = ""
    auth_writer_epoch: int = 0
    authorized_at_ms: int = 0
    release_after_ms: int = 0
    released_at_ms: int = 0
    created_at_ms: int = 0
    id: int = 0
    player_ids: list[int] = dataclasses.field(default_factory=list)
    game_mode: str = ""
    map_id: int = 0
    # configpb.LevelRatingMode;0 = UNSPECIFIED = "未定格"(不是"不计分")。
    rating_mode: int = 0
    rating_pool: str = ""


@dataclasses.dataclass(slots=True)
class ProgressSettleInfo:
    """SaveResult 事务内对实时进度通道的结算收口结果。对应 Go 的 data.ProgressSettleInfo。"""

    # 结算时水位行是否已存在(= 本场走过实时通道)。
    stream_existed: bool = False
    # 结算时的已应用水位(与 DS 上报 final_progress_seq 对账)。
    last_applied_seq: int = 0
    # True = 掉落发放权已归实时通道,结算路径的 dropped_item_config_ids 只作对账不再发放。
    drops_suppressed: bool = False


@dataclasses.dataclass(slots=True)
class SweepOutcome:
    """一轮保留期清理的结果。

    ★ 比共享的 `dbguard.Outcome` 多一个 `truncated` —— 而这一位正是"追平积压"循环的
      终止条件(见 biz.drain_purge)。共享件本轮禁改,所以在这里补一个本地类型,
      **不是**为了另立口径:mode/matched/deleted 三个字段语义与 dbguard.Outcome 完全一致。
    """

    mode: dbguard.Mode
    matched: int = 0
    deleted: int = 0
    truncated: bool = False


# ── CSV 编解码(与 Go 的 encodeConfigIDs / decodeConfigIDs 同口径)──────────────


def encode_config_ids(ids: list[int]) -> str:
    """把 item_config_id 列表编码成 CSV(如 "5001,5002")。"""
    return ",".join(str(i) for i in ids)


def decode_config_ids(csv: str) -> list[int]:
    """解析 CSV item_config_id。

    非法段 / 空段 / 0 一律跳过(防御性,与 Go 逐条一致)。**不抛异常** ——
    一行坏 CSV 不该让整批出箱取不出来,那会把一个玩家的坏数据放大成全服掉落停摆。
    """
    if not csv:
        return []
    out: list[int] = []
    for part in csv.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            v = int(part)
        except ValueError:
            continue
        if 0 < v <= 0xFFFFFFFF:
            out.append(v)
    return out


def _as_bytes(value) -> bytes:  # noqa: ANN001
    """把驱动返回的 payload 归一成 bytes(asyncmy 对 VARBINARY 可能给 str)。"""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8", errors="surrogateescape")
    return b"" if value is None else bytes(value)


# ── 清理条件常量 ─────────────────────────────────────────────────────────────
#
# **Count(report-only)与 SELECT 候选(delete)共用同一 where**,条件只写一遍,
# 杜绝"报告的条件"与"实删的条件"漂移。

# 依据服务端落库时间 created_at(§9.6 数值不信 DS;走 idx_created)。
_EXPIRED_BATTLES_WHERE = "created_at < FROM_UNIXTIME(%s / 1000)"
# 依据服务端结算事务打的 settled_at_ms(_settle_progress_stream_tx);走 idx_settled。
_SETTLED_PROGRESS_WHERE = "settled_at_ms > 0 AND settled_at_ms < %s"


class MySQLBattleRepo(bprogress.ProgressRepoMixin, bterminal.TerminalReleaseRepoMixin):
    """基于 asyncmy 连接池的战斗结算仓储。对应 Go 的 data.MySQLBattleRepo。

    实时进度 / 任务事实出箱的方法在 `progress_repo.ProgressRepoMixin`(与 Go 拆
    progress_repo.go / mission_outbox_repo.go 同因),Model-B 两阶段终态回收在
    `terminal_release_repo.TerminalReleaseRepoMixin`(对应 Go 的
    terminal_release_schema.go),这里继承进来 —— 对外仍是**同一个 repo 对象**,
    biz 侧调用点与 Go 一一对应。
    """

    __slots__ = ("_pool",)

    def __init__(self, pool) -> None:  # noqa: ANN001 —— asyncmy / aiomysql 池
        self._pool = pool

    # ── 启动期 schema 探测 ────────────────────────────────────────────────

    async def validate_recovery_outbox_schema(self) -> None:
        """探测 match_release / drop 两张 recovery 出箱表。

        不设这道闸会怎样:缺少 additive migration 时,**首个结算**才会在事务里炸 ——
        那时 Pod 早已 Ready、流量早已切过来,而每一场对局都会失败。
        """
        await self._probe(
            [
                "SELECT id, match_id, payload, next_attempt_at_ms, attempt_count, created_at_ms "
                "FROM match_release_outbox LIMIT 0",
                "SELECT id, match_id, player_id, item_config_ids, stack_item_config_ids, "
                "instance_item_config_ids, created_at_ms FROM battle_drop_outbox LIMIT 0",
            ],
            "battle recovery outbox schema invalid",
        )

    async def validate_progress_schema(self) -> None:
        """探测实时进度五表 + stopped_at_ms 的**列级契约**。

        为什么这道闸与 progress_enabled 开关无关:`_settle_progress_stream_tx` 在
        **每次结算**都无条件访问水位表(它是结算收口的一部分,不是进度通道的一部分)。
        缺表时不能等 Ready 后首个结算才炸(§16.4)。

        列级契约(而不只是"列存在"):停流 fencing 依赖 "缺省 0 = 未停流" 的语义,
        手工漂移出来的可空列 / 错类型 / 坏默认值同样会让这条语义失效,而探测能过。
        """
        await self._probe(
            [
                "SELECT match_id, last_applied_seq, total_exp, total_items, final_seq, "
                "settled_at_ms, stopped_at_ms, updated_at_ms FROM battle_progress_stream LIMIT 0",
                "SELECT id, match_id, seq, player_id, kind, exp_delta, item_config_ids, "
                "item_count, next_attempt_at_ms, attempt_count, created_at_ms "
                "FROM battle_progress_outbox LIMIT 0",
                "SELECT match_id, player_id, total_exp, total_items, total_kills, updated_at_ms "
                "FROM battle_progress_player LIMIT 0",
                "SELECT match_id, player_id, item_config_id, picked_count, spent_count, "
                "updated_at_ms FROM battle_progress_item_balance LIMIT 0",
                "SELECT match_id, seq, player_id, kind, item_config_id, count, status, "
                "result_code, created_at_ms, updated_at_ms FROM battle_progress_action LIMIT 0",
            ],
            "battle progress schema invalid",
        )
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'battle_progress_stream' "
                "AND COLUMN_NAME = 'stopped_at_ms'"
            )
            row = await cur.fetchone()
        if row is None:
            raise errcode.PandoraError(
                errcode.ErrInternal, "probe stopped_at_ms column contract: column missing"
            )
        data_type, is_nullable, col_default = str(row[0]), str(row[1]), row[2]
        if (
            data_type != "bigint"
            or is_nullable != "NO"
            or col_default is None
            or str(col_default) != "0"
        ):
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "battle_progress_stream.stopped_at_ms contract violated "
                "(type=%s nullable=%s default=%s, want bigint/NO/0): stop fencing semantics broken",
                data_type,
                is_nullable,
                col_default,
            )

    async def validate_mission_outbox_schema(self) -> None:
        """探测任务出箱表(000010 迁移产物)。仅在 mission_addr 已配时调用。

        未启用时不写该表,缺表不该拖垮启动;已启用而缺表则每次 ReportProgress
        都会在事务里炸 —— 整条实时进度通道不可用,必须 fail-fast。
        """
        await self._probe(
            [
                "SELECT id, match_id, seq, player_id, category, slot_value, amount, "
                "pending_action, next_attempt_at_ms, attempt_count, created_at_ms "
                "FROM battle_mission_outbox LIMIT 0"
            ],
            f"battle_mission_outbox schema 探测失败({MISSION_SCHEMA_HINT})",
        )

    async def _probe(self, queries: list[str], msg: str) -> None:
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            for query in queries:
                try:
                    await cur.execute(query)
                    await cur.fetchall()
                except asyncio.CancelledError:
                    # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                    # 吞掉之后取消就**不再传播** —— 启动路径上 Ctrl-C / 上层取消会被翻译成
                    # 某道闸的失败,报出假的失败原因。
                    raise
                except BaseException as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal, "%s: %s", msg, exc
                    ) from exc

    # ── 结算落库(唯一写权威路径)─────────────────────────────────────────

    async def save_result(
        self,
        result: battle_pb2.BattleResult,
        outbox: list[OutboxRecord],
        drop_outbox: list[DropOutboxRecord] | None,
        terminal_release: TerminalReleaseRecord | None,
        final_progress_seq: int,
    ) -> tuple[bool, ProgressSettleInfo]:
        """一场对局的全部结算副作用,**一个事务**。返回 (already_recorded, settle_info)。

        写入顺序与 Go 逐条相同(顺序本身是契约:出问题时第一个失败点必须一致):
          ① battles                 —— 撞 1062 = 幂等命中,走重放分支
          ② battle_player_stats
          ③ player_update_outbox
          ④ **_settle_progress_stream_tx**(见文件头 ★★)
          ⑤ battle_drop_outbox      —— 仅当 ④ 判定未被实时通道接管
          ⑥ terminal_release_outbox —— 仅授权同步路径
          ⑦ match_release_outbox
        """
        drop_outbox = drop_outbox or []
        match_id = result.match_id
        async with self._pool.acquire() as conn:
            await conn.begin()
            try:
                async with conn.cursor() as cur:
                    already = await self._insert_battle(cur, result)
                    if already:
                        info = await self._replay_idempotent(
                            cur, match_id, terminal_release, final_progress_seq
                        )
                        await conn.commit()
                        return True, info

                    now_ms = int(time.time() * 1000)
                    await self._insert_stats(cur, result)
                    await self._insert_player_update_outbox(cur, match_id, outbox, now_ms)

                    # ★★ 结算收口:打终局标记 + 读回水位。**不可省略**,见文件头。
                    settle_info = await self._settle_progress_stream_tx(
                        cur, match_id, final_progress_seq, now_ms
                    )

                    if not settle_info.drops_suppressed:
                        await self._insert_drop_outbox(cur, match_id, drop_outbox, now_ms)

                    if terminal_release is not None:
                        await self._insert_terminal_release(
                            cur, match_id, terminal_release, now_ms
                        )

                    player_ids = _authoritative_recovery_player_ids(terminal_release)
                    if not player_ids:
                        player_ids = [s.player_id for s in result.stats if s.player_id != 0]
                    await self._insert_match_release_outbox(cur, match_id, player_ids, now_ms)
                await conn.commit()
            except BaseException:
                # ★ 无条件 re-raise:回滚是必须做的清理(不做会留下悬挂事务占着连接),
                # 而取消照样穿透出去。这是事务回滚的标准写法。
                try:
                    await conn.rollback()
                except Exception:  # noqa: BLE001 —— 回滚失败不得顶掉真正的失败原因
                    pass
                raise
        return False, settle_info

    async def _insert_battle(self, cur, result: battle_pb2.BattleResult) -> bool:  # noqa: ANN001
        """插结算头。返回 True = 撞唯一键(幂等命中)。"""
        try:
            await cur.execute(
                "INSERT INTO battles "
                "(match_id, started_at_ms, ended_at_ms, winner_team, outcome, ds_pod_name, "
                "game_mode, map_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    result.match_id,
                    result.started_at_ms,
                    result.ended_at_ms,
                    result.winner_team,
                    int(result.outcome),
                    result.ds_pod_name,
                    result.game_mode,
                    result.map_id,
                ),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            if mysqlx.is_duplicate_entry(exc):
                return True
            raise errcode.PandoraError(
                errcode.ErrBattleResultDBWrite,
                "insert battles match=%d: %s",
                result.match_id,
                exc,
            ) from exc
        return False

    async def _replay_idempotent(  # noqa: ANN001
        self,
        cur,
        match_id: int,
        terminal_release: TerminalReleaseRecord | None,
        final_progress_seq: int,
    ) -> ProgressSettleInfo:
        """幂等重放分支:恢复可能缺失的 recovery outbox + 收口进度水位。

        ★ 名单来源顺序不能反(§9.6):Redis-authority 路径只信 checker 从 canonical
          BattleStorageRecord 带回的 roster;取不到才回读**首笔已落库**的 stats。
          **绝不信本次重复 payload** —— 重放的请求体可能来自僵尸 DS。

        ★ 收口进度水位在重放分支同样要跑(审计 P0):首笔落库若由不含进度逻辑的旧副本
          完成,水位行会一直没打终局标记,僵尸 DS 的迟到进度仍会被接受并发放。
          `_settle_progress_stream_tx` 幂等,已收口的行原样返回不改写首次标记。
        """
        player_ids = _authoritative_recovery_player_ids(terminal_release)
        if not player_ids:
            try:
                await cur.execute(
                    "SELECT player_id FROM battle_player_stats WHERE match_id = %s "
                    "ORDER BY player_id ASC",
                    (match_id,),
                )
                rows = await cur.fetchall()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrBattleResultDBWrite,
                    "load idempotent match release players match=%d: %s",
                    match_id,
                    exc,
                ) from exc
            player_ids = [int(r[0]) for r in rows]
        now_ms = int(time.time() * 1000)
        try:
            await self._insert_match_release_outbox(cur, match_id, player_ids, now_ms)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrBattleResultDBWrite,
                "restore match release outbox match=%d: %s",
                match_id,
                exc,
            ) from exc
        try:
            # 返回值刻意丢弃:Go 的重放分支返回**零值** ProgressSettleInfo,而不是刚读到的水位。
            # 重放不再产生任何掉落 / 对账副作用,返回真实水位会让调用方以为本次做了收口
            # (于是打出一条"本场掉落已由实时通道发放"的假台账)。
            await self._settle_progress_stream_tx(cur, match_id, final_progress_seq, now_ms)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrBattleResultDBWrite,
                "settle progress stream on replay match=%d: %s",
                match_id,
                exc,
            ) from exc
        return ProgressSettleInfo()

    async def _insert_stats(self, cur, result: battle_pb2.BattleResult) -> None:  # noqa: ANN001
        sql = (
            "INSERT INTO battle_player_stats "
            "(match_id, player_id, hero_id, team, kills, deaths, assists, damage_dealt, "
            "damage_taken, healing, gold, mmr_delta) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        for s in result.stats:
            try:
                await cur.execute(
                    sql,
                    (
                        result.match_id,
                        s.player_id,
                        s.hero_id,
                        s.team,
                        s.kills,
                        s.deaths,
                        s.assists,
                        s.damage_dealt,
                        s.damage_taken,
                        s.healing,
                        s.gold,
                        s.mmr_delta,
                    ),
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrBattleResultDBWrite,
                    "insert stats match=%d player=%d: %s",
                    result.match_id,
                    s.player_id,
                    exc,
                ) from exc

    async def _insert_player_update_outbox(  # noqa: ANN001
        self, cur, match_id: int, outbox: list[OutboxRecord], now_ms: int
    ) -> None:
        sql = (
            "INSERT INTO player_update_outbox (match_id, player_id, payload, created_at_ms) "
            "VALUES (%s, %s, %s, %s)"
        )
        for o in outbox:
            try:
                await cur.execute(sql, (match_id, o.player_id, o.payload, now_ms))
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrBattleResultDBWrite,
                    "insert outbox match=%d player=%d: %s",
                    match_id,
                    o.player_id,
                    exc,
                ) from exc

    async def _insert_drop_outbox(  # noqa: ANN001
        self, cur, match_id: int, drop_outbox: list[DropOutboxRecord], now_ms: int
    ) -> None:
        sql = (
            "INSERT INTO battle_drop_outbox (match_id, player_id, item_config_ids, "
            "stack_item_config_ids, instance_item_config_ids, created_at_ms) "
            "VALUES (%s, %s, %s, %s, %s, %s)"
        )
        for d in drop_outbox:
            if not d.item_config_ids:
                continue
            try:
                await cur.execute(
                    sql,
                    (
                        match_id,
                        d.player_id,
                        encode_config_ids(d.item_config_ids),
                        encode_config_ids(d.stack_item_config_ids),
                        encode_config_ids(d.instance_item_config_ids),
                        now_ms,
                    ),
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrBattleResultDBWrite,
                    "insert drop outbox match=%d player=%d: %s",
                    match_id,
                    d.player_id,
                    exc,
                ) from exc

    async def _insert_terminal_release(  # noqa: ANN001
        self, cur, match_id: int, rec: TerminalReleaseRecord, now_ms: int
    ) -> None:
        try:
            await cur.execute(
                "INSERT INTO terminal_release_outbox "
                "(match_id, allocation_id, ds_pod_name, gameserver_uid, instance_epoch, "
                "auth_gen, auth_jti, auth_exp_ms, auth_kid, auth_token_sha256, auth_writer_epoch, "
                "authorized_at_ms, release_after_ms, created_at_ms) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    match_id,
                    rec.allocation_id,
                    rec.ds_pod_name,
                    rec.gameserver_uid,
                    rec.instance_epoch,
                    rec.auth_gen,
                    rec.auth_jti,
                    rec.auth_exp_ms,
                    rec.auth_kid,
                    rec.auth_token_sha256,
                    rec.auth_writer_epoch,
                    rec.authorized_at_ms,
                    rec.release_after_ms,
                    now_ms,
                ),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrBattleResultDBWrite,
                "insert terminal release outbox match=%d allocation=%s: %s",
                match_id,
                rec.allocation_id,
                exc,
            ) from exc

    async def _insert_match_release_outbox(  # noqa: ANN001
        self, cur, match_id: int, player_ids: list[int], now_ms: int
    ) -> None:
        """写撮合状态释放出箱(ON DUPLICATE KEY 只把 next_attempt 往前拉,不覆盖 payload)。

        ★ `LEAST(next_attempt_at_ms, VALUES(next_attempt_at_ms))` 而不是直接覆盖:
          重放时若把已退避到未来的行拉成"立刻重试",会让一个长期失败的行每轮都被取出来
          打一遍 matchmaker。
        """
        record = match_pb2.MatchReleaseStorageRecord(
            operation_id=str(uuid.uuid4()),
            match_id=match_id,
            player_ids=player_ids,
            created_at_ms=now_ms,
        )
        payload = record.SerializeToString()
        try:
            await cur.execute(
                "INSERT INTO match_release_outbox "
                "(match_id, payload, next_attempt_at_ms, attempt_count, created_at_ms) "
                "VALUES (%s, %s, 0, 0, %s) "
                "ON DUPLICATE KEY UPDATE match_id = VALUES(match_id), "
                "next_attempt_at_ms = LEAST(next_attempt_at_ms, VALUES(next_attempt_at_ms))",
                (match_id, payload, now_ms),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrBattleResultDBWrite,
                "insert match release outbox match=%d: %s",
                match_id,
                exc,
            ) from exc

    # ── ★★ 实时进度通道结算收口(必须整体在,见文件头)────────────────────

    async def _settle_progress_stream_tx(  # noqa: ANN001
        self, cur, match_id: int, final_seq: int, now_ms: int
    ) -> ProgressSettleInfo:
        """在结算事务内收口实时进度通道。对应 Go 的 settleProgressStreamTx。

          ① `SELECT ... FOR UPDATE` 锁定(或创建)水位行并打终局标记(settled_at_ms>0)
             → 之后任何 ReportProgress 一律拒(僵尸 / 分区恢复 DS fencing;ABANDONED 同样收口);
          ② 返回水位信息,调用方据 last_applied_seq>0 决定是否抑制结算路径掉落发放。

        判定依据是**服务端自己的水位表**,不信 DS 声明:恶意 DS 两头上报也只有一条路径发放。

        幂等:已打过终局标记的行原样返回不再改写 —— 首次结算的 settled_at_ms / final_seq
        是权威审计值,重复结算 / battles 幂等重放都不得覆盖它。
        """
        await cur.execute(
            "SELECT last_applied_seq, settled_at_ms FROM battle_progress_stream "
            "WHERE match_id = %s FOR UPDATE",
            (match_id,),
        )
        row = await cur.fetchone()
        if row is None:
            # 本场未走实时通道:插入终局标记行,封死迟到进度(水位 0,掉落走结算路径)。
            await cur.execute(
                "INSERT INTO battle_progress_stream "
                "(match_id, last_applied_seq, final_seq, settled_at_ms, updated_at_ms) "
                "VALUES (%s, 0, %s, %s, %s)",
                (match_id, final_seq, now_ms, now_ms),
            )
            return ProgressSettleInfo()

        last_seq = int(row[0] or 0)
        settled_ms = int(row[1] or 0)
        info = ProgressSettleInfo(
            stream_existed=True,
            last_applied_seq=last_seq,
            # ★ 判据是水位 >0,不是 "progress_enabled 开着":开关中途改变不影响已开流的对局。
            drops_suppressed=last_seq > 0,
        )
        if settled_ms > 0:
            return info  # 已收口(幂等重放),不改写首次结算标记
        await cur.execute(
            "UPDATE battle_progress_stream SET settled_at_ms = %s, final_seq = %s, "
            "updated_at_ms = %s WHERE match_id = %s",
            (now_ms, final_seq, now_ms, match_id),
        )
        return info

    # ── 查询 ──────────────────────────────────────────────────────────────

    async def get_result(self, match_id: int) -> battle_pb2.BattleResult | None:
        """读一场对局结算。不存在返回 None(对应 Go 的 found=false)。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(
                    "SELECT started_at_ms, ended_at_ms, winner_team, outcome, ds_pod_name, "
                    "game_mode, map_id FROM battles WHERE match_id = %s LIMIT 1",
                    (match_id,),
                )
                head = await cur.fetchone()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "query battle match=%d: %s", match_id, exc
                ) from exc
            if head is None:
                return None
            stats = await self._load_stats(cur, match_id)
        return battle_pb2.BattleResult(
            match_id=match_id,
            started_at_ms=int(head[0] or 0),
            ended_at_ms=int(head[1] or 0),
            winner_team=int(head[2] or 0),
            outcome=int(head[3] or 0),
            ds_pod_name=head[4] or "",
            game_mode=head[5] or "",
            map_id=int(head[6] or 0),
            stats=stats,
        )

    async def list_player_history(
        self, player_id: int, limit: int, before_ms: int
    ) -> list[battle_pb2.BattleResult]:
        """倒序列出玩家战绩历史(游标分页)。

        ★ 窗口受保留期限制:ListPlayerHistory 只读 MySQL,没有冷存归档 ——
          这正是产品口径"MySQL 里最多只有最近六个月的战报"。
        """
        if limit <= 0:
            limit = 20
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(
                    "SELECT b.match_id FROM battle_player_stats s "
                    "JOIN battles b ON b.match_id = s.match_id "
                    "WHERE s.player_id = %s AND (%s <= 0 OR b.ended_at_ms < %s) "
                    "ORDER BY b.ended_at_ms DESC LIMIT %s",
                    (player_id, before_ms, before_ms, limit),
                )
                rows = await cur.fetchall()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "query history player=%d: %s", player_id, exc
                ) from exc
            match_ids = [int(r[0]) for r in rows]

        out: list[battle_pb2.BattleResult] = []
        for mid in match_ids:
            res = await self.get_result(mid)
            if res is not None:
                out.append(res)
        return out

    async def _load_stats(self, cur, match_id: int) -> list[battle_pb2.PlayerStats]:  # noqa: ANN001
        try:
            await cur.execute(
                "SELECT player_id, hero_id, team, kills, deaths, assists, damage_dealt, "
                "damage_taken, healing, gold, mmr_delta FROM battle_player_stats "
                "WHERE match_id = %s ORDER BY team, player_id",
                (match_id,),
            )
            rows = await cur.fetchall()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "query stats match=%d: %s", match_id, exc
            ) from exc
        return [
            battle_pb2.PlayerStats(
                player_id=int(r[0]),
                hero_id=int(r[1]),
                team=int(r[2]),
                kills=int(r[3]),
                deaths=int(r[4]),
                assists=int(r[5]),
                damage_dealt=int(r[6]),
                damage_taken=int(r[7]),
                healing=int(r[8]),
                gold=int(r[9]),
                mmr_delta=int(r[10]),
            )
            for r in rows
        ]

    # ── player.update 出箱 ───────────────────────────────────────────────

    async def fetch_outbox(self, limit: int) -> list[OutboxRecord]:
        """按 id 升序取待发布出箱行(**FIFO 保序** —— 同玩家段位事件不得乱序)。"""
        if limit <= 0:
            limit = 128
        rows = await self._query(
            "SELECT id, player_id, payload FROM player_update_outbox ORDER BY id ASC LIMIT %s",
            (limit,),
            "query outbox",
        )
        return [
            OutboxRecord(id=int(r[0]), player_id=int(r[1]), payload=_as_bytes(r[2])) for r in rows
        ]

    async def delete_outbox(self, outbox_id: int) -> None:
        await self._exec(
            "DELETE FROM player_update_outbox WHERE id = %s",
            (outbox_id,),
            f"delete outbox id={outbox_id}",
        )

    # ── 掉落出箱 ─────────────────────────────────────────────────────────

    async def fetch_drop_outbox(self, limit: int) -> list[DropOutboxRecord]:
        if limit <= 0:
            limit = 128
        rows = await self._query(
            "SELECT id, match_id, player_id, item_config_ids, stack_item_config_ids, "
            "instance_item_config_ids FROM battle_drop_outbox ORDER BY id ASC LIMIT %s",
            (limit,),
            "query drop outbox",
        )
        return [
            DropOutboxRecord(
                id=int(r[0]),
                match_id=int(r[1]),
                player_id=int(r[2]),
                item_config_ids=decode_config_ids(r[3] or ""),
                stack_item_config_ids=decode_config_ids(r[4] or ""),
                instance_item_config_ids=decode_config_ids(r[5] or ""),
            )
            for r in rows
        ]

    async def delete_drop_outbox(self, outbox_id: int) -> None:
        await self._exec(
            "DELETE FROM battle_drop_outbox WHERE id = %s",
            (outbox_id,),
            f"delete drop outbox id={outbox_id}",
        )

    # ── 撮合释放出箱 ─────────────────────────────────────────────────────

    async def fetch_match_release_outbox(
        self, limit: int, now_ms: int
    ) -> list[MatchReleaseRecord]:
        if limit <= 0:
            limit = 128
        if now_ms <= 0:
            now_ms = int(time.time() * 1000)
        rows = await self._query(
            "SELECT id, payload, attempt_count, next_attempt_at_ms, created_at_ms "
            "FROM match_release_outbox WHERE next_attempt_at_ms <= %s "
            "ORDER BY next_attempt_at_ms ASC, id ASC LIMIT %s",
            (now_ms, limit),
            "query match release outbox",
        )
        out: list[MatchReleaseRecord] = []
        for r in rows:
            row_id = int(r[0])
            storage = match_pb2.MatchReleaseStorageRecord()
            try:
                storage.ParseFromString(_as_bytes(r[1]))
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "decode match release outbox id=%d: %s", row_id, exc
                ) from exc
            if storage.match_id == 0 or not storage.operation_id:
                # 坏行整批停:继续跑等于拿一个不知道该释放谁的 payload 去调 matchmaker。
                raise errcode.PandoraError(
                    errcode.ErrInternal, "invalid match release outbox id=%d", row_id
                )
            out.append(
                MatchReleaseRecord(
                    id=row_id,
                    operation_id=storage.operation_id,
                    match_id=storage.match_id,
                    player_ids=list(storage.player_ids),
                    attempt_count=int(r[2] or 0),
                    next_attempt_at_ms=int(r[3] or 0),
                    created_at_ms=int(r[4] or 0),
                )
            )
        return out

    async def defer_match_release_outbox(self, outbox_id: int, next_attempt_at_ms: int) -> None:
        if outbox_id == 0 or next_attempt_at_ms <= 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "match release defer requires id/time"
            )
        affected = await self._exec(
            "UPDATE match_release_outbox SET attempt_count = attempt_count + 1, "
            "next_attempt_at_ms = %s WHERE id = %s",
            (next_attempt_at_ms, outbox_id),
            f"defer match release outbox id={outbox_id}",
        )
        if affected > 1:
            # PK 保证 >1 是结构 / 驱动异常,必须 fail-closed。
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "defer match release id=%d affected=%d",
                outbox_id,
                affected,
            )

    async def delete_match_release_outbox(self, outbox_id: int) -> None:
        if outbox_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "match release outbox id required")
        affected = await self._exec(
            "DELETE FROM match_release_outbox WHERE id = %s",
            (outbox_id,),
            f"delete match release outbox id={outbox_id}",
        )
        if affected > 1:
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "delete match release id=%d affected=%d",
                outbox_id,
                affected,
            )

    # ── 保留期清理(§9.24)────────────────────────────────────────────────

    async def sweep_expired_battles(
        self, mode: dbguard.Mode, cutoff_ms: int, batch: int
    ) -> SweepOutcome:
        """超保留期的对局(battles + battle_player_stats 同事务成组)。"""
        return await self._sweep_by_match_id(
            mode,
            anchor_table="battles",
            where=_EXPIRED_BATTLES_WHERE,
            delete_tables=["battle_player_stats", "battles"],
            cutoff_ms=cutoff_ms,
            batch=batch,
        )

    async def sweep_settled_progress(
        self, mode: dbguard.Mode, cutoff_ms: int, batch: int
    ) -> SweepOutcome:
        """已结算且超保留期的进度水位(五张表同事务成组)。

        未结算行**无论如何都不在处理范围** —— 陈年未结算 = 补偿链 bug 证据,
        另有 count_stale_unsettled_progress 持续告警,永不静默清理。
        """
        return await self._sweep_by_match_id(
            mode,
            anchor_table="battle_progress_stream",
            where=_SETTLED_PROGRESS_WHERE,
            delete_tables=[
                "battle_progress_outbox",
                "battle_progress_action",
                "battle_progress_item_balance",
                "battle_progress_player",
                "battle_progress_stream",
            ],
            cutoff_ms=cutoff_ms,
            batch=batch,
        )

    async def count_stale_unsettled_progress(self, cutoff_ms: int) -> int:
        rows = await self._query(
            "SELECT COUNT(*) FROM battle_progress_stream "
            "WHERE settled_at_ms = 0 AND updated_at_ms < %s",
            (cutoff_ms,),
            "count stale unsettled progress",
        )
        return int(rows[0][0]) if rows else 0

    async def _sweep_by_match_id(
        self,
        mode: dbguard.Mode,
        *,
        anchor_table: str,
        where: str,
        delete_tables: list[str],
        cutoff_ms: int,
        batch: int,
    ) -> SweepOutcome:
        """多表按 match_id 成组清理的统一实现(report-only 只数不删)。

        多表事务清理用不了 dbguard.sweep_table(那是单条 DELETE),但**告警口径必须一致**:
        事件名 / metric / 单位都与共享件对齐,unit="matches" —— 这里删的是"一组行"
        (一场对局的主表+子表),不是"一行"。
        """
        out = SweepOutcome(mode=mode)
        if mode is not dbguard.Mode.DELETE:
            # report-only:只数满足条件的对局数(**不受 batch 截断**,给真实积压规模)。
            rows = await self._query(
                f"SELECT COUNT(*) FROM `{anchor_table}` WHERE {where}",  # noqa: S608
                (cutoff_ms,),
                f"count purgeable {anchor_table}",
            )
            n = int(rows[0][0]) if rows else 0
            out.matched = n
            _report_pending(anchor_table, n)
            return out

        rows = await self._query(
            f"SELECT match_id FROM `{anchor_table}` WHERE {where} LIMIT %s",  # noqa: S608
            (cutoff_ms, batch),
            "select purge candidates",
        )
        ids = [int(r[0]) for r in rows]
        if not ids:
            return out
        await self._delete_by_match_ids_tx(delete_tables, ids)
        out.matched = len(ids)
        out.deleted = len(ids)
        out.truncated = batch > 0 and len(ids) >= batch
        _report_deleted(anchor_table, out.deleted)
        return out

    async def _delete_by_match_ids_tx(self, tables: list[str], ids: list[int]) -> None:
        """一个事务里按 match_id 列表依次清多张表(**子表在前,主表最后**)。"""
        placeholders = ",".join(["%s"] * len(ids))
        async with self._pool.acquire() as conn:
            await conn.begin()
            try:
                async with conn.cursor() as cur:
                    for table in tables:
                        await cur.execute(
                            f"DELETE FROM `{table}` WHERE match_id IN ({placeholders})",  # noqa: S608
                            tuple(ids),
                        )
                await conn.commit()
            except BaseException:
                # ★ 无条件 re-raise:回滚是必须做的清理,取消照样穿透。
                try:
                    await conn.rollback()
                except Exception:  # noqa: BLE001
                    pass
                raise

    # ── 低层小工具 ───────────────────────────────────────────────────────

    async def _query(
        self, sql: str, params: tuple, what: str, code: int = errcode.ErrInternal
    ) -> list:
        """单条只读 SQL。`code` 供进度水位这类**必须报 ErrUnavailable**(可重试)的
        调用点覆盖 —— Go 侧同一条 SQL 用的是 ErrUnavailable 而不是 ErrInternal,
        码不同会让 DS 把可重试的库抖动当成永久失败而丢批。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(sql, params)
                return list(await cur.fetchall())
            except asyncio.CancelledError:
                raise
            except errcode.PandoraError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(code, "%s: %s", what, exc) from exc

    async def _exec(
        self, sql: str, params: tuple, what: str, code: int = errcode.ErrInternal
    ) -> int:
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            try:
                await cur.execute(sql, params)
                return cur.rowcount or 0
            except asyncio.CancelledError:
                raise
            except errcode.PandoraError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(code, "%s: %s", what, exc) from exc


def _authoritative_recovery_player_ids(rec: TerminalReleaseRecord | None) -> list[int]:
    """recovery 出箱的名单来源:**只信 canonical roster**。

    返回空列表表示"没有权威名单",调用方才回落到已落库 stats / 本次 stats。
    """
    if rec is None or not rec.player_ids:
        return []
    return list(rec.player_ids)


def _report_pending(table: str, pending: int) -> None:
    """report_only 模式的待清理量告警。事件名与 Go dbguard.ReportPending 逐字相同。"""
    dbguard.RETENTION_PENDING.labels(BATTLE_DB, table).set(pending)
    if pending <= 0:
        return
    plog.get().warning(
        "db_retention_pending_not_deleted",
        db=BATTLE_DB,
        table=table,
        pending_rows=pending,
        unit="matches",
        mode=dbguard.Mode.REPORT_ONLY.value,
        hint="按当前配置只报告不删除(retention_mode=report_only);"
        "库会继续增长,确认清理条件无误后设 retention_mode=delete 开启实删",
    )


def _report_deleted(table: str, deleted: int) -> None:
    """delete 模式的实删台账。事件名与 Go dbguard.ReportDeleted 逐字相同。"""
    if deleted <= 0:
        return
    dbguard.RETENTION_DELETED.labels(BATTLE_DB, table).inc(deleted)
    plog.get().info(
        "db_retention_deleted", db=BATTLE_DB, table=table, deleted=deleted, unit="matches"
    )
