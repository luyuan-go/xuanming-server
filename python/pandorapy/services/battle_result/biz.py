"""battle_result 业务逻辑层 —— 对应 Go 侧 internal/biz 的
battle_result.go / mmr.go / settlement.go / retention.go / consumer.go。

职责(docs/design/go-services.md §2.13):
  - Model-B 同步 ReportResult / legacy battle.result → 幂等落库(不变量 §2,unique match_id)
  - **MMR 在此算**(Elo,DS 上报的 mmr_delta 一律被覆盖,不变量 §6)
  - 消费 pandora.ds.lifecycle 的 ABANDONED → 写 abandoned 补偿记录(不变量 §4)
  - 落库同事务写 player.update / drop / match_release 出箱 → 后台发布器可靠投递
  - 提供 GetMatchResult / ListPlayerHistory 查询

关键不变量(改任一条之前先想清楚它防的是什么):

  ① 幂等键 = match_id。SaveResult 命中唯一键即 already_recorded,不重复写。
  ② MMR 覆盖 DS 上报值:只信对局胜负 winner_team,不信 DS 给的 mmr_delta。
  ③ ABANDONED **短路**:outcome==ABANDONED 时全部 stats 的 mmr_delta 清零,
     并且**根本不调** settlement_runs_elo、不读 MMR reader。
     漏掉这条短路的后果:伪造 Outcome=ABANDONED 的 DS 能借补偿语义改玩家段位。
  ④ 计分判据只读**权威**字段(canonical rating_mode → 旧口径 game_mode → legacy),
     绝不读 DS 请求体(§9.6)。回落旧口径的局必须打
     `battle_rating_basis_legacy_fallback` WARN —— 段位改动不可逆,事后要能追。
  ⑤ 授权同步路径(terminal_release 非空)在**任何副作用之前**用 canonical
     game_mode/map_id 覆盖请求体。canonical 为空也照覆盖为空:宁可少存元数据,
     也不把不可信请求字段伪装成权威事实。

★ 本模块**不含**实时进度通道(ReportProgress / ApplyProgress / mission 转发)。
  见 service.py 的 ReportProgress 与 main.py 的启动 WARN。但结算侧的进度收口
  (repo._settle_progress_stream_tx)是完整的 —— 它决定掉落会不会双发,不能少。
"""

from __future__ import annotations

import asyncio
import dataclasses
import math
import time
import uuid

from pandora.battle.v1 import battle_pb2
from pandora.player.v1 import player_pb2

from pandorapy import dbguard, errcode, logwindow, rating, safego
from pandorapy import log as plog
from pandorapy.protoenum import enum_name
from pandorapy.services.battle_result import conf as bconf
from pandorapy.services.battle_result import progress as bprog
from pandorapy.services.battle_result import repo as brepo
from pandorapy.services.battle_result import roster as brost

# winner_team 取值(对齐 proto BattleResult.winner_team)。
WINNER_TEAM_A = 0  # A 队(team=0)胜
WINNER_TEAM_B = 1  # B 队(team=1)胜
WINNER_TEAM_DRAW = 2  # 平 / 无效

# DS callback 令牌的写者代际(auth.DSAuthWriterEpochV2)。
# 值必须与 Go 侧一致:不一致会让**每一场**授权结算被判 writer_epoch_mismatch。
DS_AUTH_WRITER_EPOCH_V2 = 2


# ── 纯函数:Elo ───────────────────────────────────────────────────────────────


def elo_deltas(avg_a: int, avg_b: int, k: int, winner_team: int) -> tuple[int, int]:
    """标准 Elo。对应 Go 的 eloDeltas。

        expected_a = 1 / (1 + 10^((avg_b - avg_a) / 400))
        score_a    = 胜 1 / 负 0 / 平 0.5
        delta      = round(K * (score - expected))

    ★ 用 `round_half_away_from_zero` 而不是 Python 内建 `round`:
      内建 round 是**银行家舍入**(round-half-to-even),Go 的 math.Round 是
      half-away-from-zero。差别只在 .5 上,而 K=32、两队均分相等时
      期望差正好是 ±16.0 这类整数还看不出来 —— 一旦均分不等,
      同一场对局在两栈会算出差 1 分的 delta,而两边都不报错。
    """
    expected_a = 1.0 / (1.0 + math.pow(10, (avg_b - avg_a) / 400.0))
    expected_b = 1.0 - expected_a

    if winner_team == WINNER_TEAM_A:
        score_a, score_b = 1.0, 0.0
    elif winner_team == WINNER_TEAM_B:
        score_a, score_b = 0.0, 1.0
    else:  # draw / invalid
        score_a, score_b = 0.5, 0.5

    return (
        _round_half_away_from_zero(k * (score_a - expected_a)),
        _round_half_away_from_zero(k * (score_b - expected_b)),
    )


def _round_half_away_from_zero(v: float) -> int:
    """与 Go 的 math.Round 同语义。"""
    return int(math.floor(v + 0.5)) if v >= 0 else int(math.ceil(v - 0.5))


def reason_for_team(team: int, winner_team: int) -> str:
    """某队玩家的 player.update reason。对应 Go 的 reasonForTeam。"""
    if winner_team not in (WINNER_TEAM_A, WINNER_TEAM_B):
        return "draw"
    return "win" if team == winner_team else "lose"


# ── 纯函数:跨 region 结算回流口径(settlement.go)──────────────────────────────


def settlement_key(match_id: int, player_id: int) -> str:
    """一名玩家在一场对局的结算回流幂等键。canonical "match_id:player_id"。

    与 player 服务 mmr_history 唯一键 (player_id, match_id) 同一维度:多 region 部署下
    overflow 对局的 player.update 可能经跨 region 桥 at-least-once 重投,
    所有路径一律用此键去重,杜绝因桥实现不同产生口径漂移。
    """
    return f"{match_id}:{player_id}"


@dataclasses.dataclass(frozen=True, slots=True)
class SettlementOwner:
    """一名玩家结算回流的落点 (region, cell)。"""

    player_id: int
    region_id: int
    cell_id: int


def distinct_settlement_regions(owners: list[SettlementOwner]) -> list[int]:
    """一组结算落点里去重后的 region 列表(**升序,确定性**)。空输入返回 []。"""
    return sorted({o.region_id for o in owners})


def cross_region_settlement(owners: list[SettlementOwner]) -> bool:
    """一场对局的结算是否跨 region(回流到 >1 个 region)。"""
    return len(distinct_settlement_regions(owners)) > 1


# ── 掉落幂等键 / 聚合 ────────────────────────────────────────────────────────


def drop_idempotency_key(match_id: int, player_id: int) -> str:
    """battle_drop:{match_id}:{player_id}。同对局同玩家的掉落只入账一次。"""
    return f"battle_drop:{match_id}:{player_id}"


def aggregate_stack_grants(item_config_ids: list[int]):  # noqa: ANN201
    """把逐件展开的可堆叠 ID 聚成 (id, count)。对应 Go 的 aggregateStackGrants。

    **按 id 升序**输出(Go 显式 sort):顺序进不了幂等键,但会进 inventory 的流水,
    两栈顺序不同会让同一次发放在流水里长得不一样,对账时看着像两笔。
    """
    from pandorapy.services.battle_result import clients as bcli

    if not item_config_ids:
        raise errcode.PandoraError(errcode.ErrInvalidState, "empty stack grant row")
    counts: dict[int, int] = {}
    for cid in item_config_ids:
        if cid == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "zero item_config_id in stack row"
            )
        counts[cid] = counts.get(cid, 0) + 1
    return [bcli.StackGrant(item_config_id=cid, count=counts[cid]) for cid in sorted(counts)]


def match_release_retry_delay(attempt: int) -> float:
    """撮合释放的指数退避(秒):1s·2^n,封顶 60s。对应 Go 的 matchReleaseRetryDelay。"""
    shift = min(attempt, 6)
    return float(min(1 << shift, 60))


# ── 出箱无 producer 的限流告警窗口 ────────────────────────────────────────────
#
# 进程级单例(与 Go 的 `var outboxNoPusherLog plog.Window` 同)。首错 + 每 60s 一条。
_outbox_no_pusher_log = logwindow.Window()


class BattleResultUsecase(bprog.ProgressMixin):
    """battle_result 业务逻辑核心。对应 Go 的 biz.BattleResultUsecase。

    实时进度通道(ReportProgress / 进度出箱发布器 / 任务事实转发器)在
    `progress.ProgressMixin` —— 与 Go 把 biz 拆成 battle_result.go + progress.go +
    mission_forward.go 同一刀口。Mixin 只依赖本类的 `_repo` / `_cfg` / `_granter` /
    `_mail_sender` / `_monster_exp` 与 `battle_item_definition()`。
    """

    def __init__(
        self,
        repo: brepo.MySQLBattleRepo,
        mmr,  # noqa: ANN001 —— 鸭子类型:必须有 async get_mmr(player_id, rating_pool)
        pusher,  # noqa: ANN001 —— async push_player_update(player_id, payload);None = kafka 未配
        releaser,  # noqa: ANN001 —— async release_match(match_id, player_ids);None = 地址未配
        cfg: bconf.BattleConf,
    ) -> None:
        self._repo = repo
        # mmr 为 None 时兜静态 reader —— 与 Go 的构造器同:调用点从此不必判空。
        if mmr is None:
            from pandorapy.services.battle_result import clients as bcli

            mmr = bcli.StaticMMRReader(cfg.base_mmr)
        self._mmr = mmr
        self._pusher = pusher
        self._releaser = releaser
        self._cfg = cfg
        # 以下用 setter 注入(与 Go 一致),避免构造签名被迫改。
        self._granter = None
        self._mail_sender = None
        self._item_catalog = None
        self._monster_exp = None
        # 实时进度通道的两个弱依赖(ProgressMixin 的 setter 写这两个字段)。
        # 必须在这里预置 None:Mixin 的 run_progress_publisher / run_mission_forwarder
        # 在 main 未调 setter 时会读它们判「是否关闭」,少了预置就是 AttributeError,
        # 而那是在**后台循环里**炸 —— 进程照跑、health 照答 SERVING。
        self._exp_granter = None
        self._mission_reporter = None
        # Model-B 终态回收 relay。None = authority_mode 非 redis(legacy)→ 发布器不启动。
        # 同样必须在这里预置:_publish_terminal_release_batch 在后台循环里读它判空。
        self._terminal_relay = None

    # ── setter ────────────────────────────────────────────────────────────

    def set_terminal_release_relay(self, relay) -> None:  # noqa: ANN001
        """Model-B 正常结算资源回收 relay(对应 Go 的 SetTerminalReleaseRelay)。

        只在 `ds_auth.authority_mode=redis` 且 schema / Redis 探测全过后注入。
        """
        self._terminal_relay = relay

    def set_instance_granter(self, granter) -> None:  # noqa: ANN001
        """inventory 掉落发放器。None / 不调 = inventory_addr 未配 → 不启动掉落发布器,
        出箱积压不丢(等地址配好重启补发)。"""
        self._granter = granter

    def set_mail_sender(self, sender) -> None:  # noqa: ANN001
        """背包满溢出转邮件。None = mail_addr 未配 → 背包满掉落留在出箱轮询重试。"""
        self._mail_sender = sender

    def set_battle_item_catalog(self, catalog) -> None:  # noqa: ANN001
        """与 UE 同源的 item/drop 热更视图(生产必须注入)。"""
        self._item_catalog = catalog

    def set_monster_exp_table(self, table) -> None:  # noqa: ANN001
        """怪物击杀经验查表(configtable role_level)。

        **非 nil-safe**:实时进度通道(report_progress)直接查它算经验。
        缺表在启动期就 fail-fast(不留到运行期的第一批进度才炸)。
        """
        self._monster_exp = table

    @property
    def cfg(self) -> bconf.BattleConf:
        return self._cfg

    # ── 道具定义 ──────────────────────────────────────────────────────────

    def battle_item_definition(self, item_config_id: int):  # noqa: ANN201
        """查一个道具在战斗域的投影;未知一律 fail-closed 返回 None。"""
        if self._item_catalog is not None:
            return self._item_catalog.lookup(item_config_id)
        # 兼容旧单测:历史 drop_whitelist 仅装装备,因此 fallback 只声明 equipment。
        if self._cfg.is_droppable(item_config_id):
            from pandorapy.services.battle_result import catalog as bcat

            return bcat.BattleItemDefinition(
                equipment=True, battle_usable=False, droppable=True, max_stack=1
            )
        return None

    # ── ReportResult ──────────────────────────────────────────────────────

    async def report_result(
        self, result: battle_pb2.BattleResult, final_progress_seq: int
    ) -> bool:
        """legacy kafka / 内部直调入口。返回 already_recorded。"""
        return await self._report_result(result, None, final_progress_seq)

    async def report_authorized_result(
        self,
        result: battle_pb2.BattleResult,
        terminal_release: brepo.TerminalReleaseRecord,
        final_progress_seq: int,
    ) -> bool:
        """Redis-authority 同步入口。terminal_release 必须来自 service 已完成
        Guard + active 校验的**服务端快照**,不从 BattleResult 请求体补值。
        """
        try:
            brost.validate_authorized_roster(
                [s.player_id for s in result.stats] if result is not None else None,
                list(terminal_release.player_ids),
            )
        except brost.RosterRejected as exc:
            # 整场结算被判 ErrUnauthorized 的闸门。ErrUnauthorized 不属 server fault,
            # access log 走 rpc_ok(DEBUG)、service 层的 ds_auth_rejected 也不覆盖 biz 返回的
            # 错误 → 不在这里留证就是全链零日志。中途掉线导致 DS 少报一个 stat、
            # 或多报一个观战/替补 ID,玩家表现为「打完什么都没有」,运维必须能区分它
            # 与「DS 根本没上报」。
            plog.get().warning(
                "battle_result_roster_rejected",
                match_id=terminal_release.match_id,
                ds_pod_name=terminal_release.ds_pod_name,
                reason=exc.reason,
                reported_players=len(result.stats) if result is not None else 0,
                authority_players=len(terminal_release.player_ids),
                sample_player_id=exc.sample_player_id,
                code=exc.code,
                err=str(exc),
                hint="DS 上报 stats 名单 ≠ canonical roster,本场结算整体拒绝"
                "(不落库、不发段位、不发掉落)",
            )
            raise
        return await self._report_result(result, terminal_release, final_progress_seq)

    async def _report_result(  # noqa: C901 —— 与 Go 同为线性结算流水
        self,
        result: battle_pb2.BattleResult | None,
        terminal_release: brepo.TerminalReleaseRecord | None,
        final_progress_seq: int,
    ) -> bool:
        started_at = time.monotonic()
        authorized = terminal_release is not None

        if result is None or result.match_id == 0:
            # ErrInvalidArg 不属 server fault,access log 记 rpc_ok(DEBUG)。legacy kafka /
            # 内部直调路径没有 service 层那条 ds_report_result_rejected,不在这里留证
            # 就是「这局根本没进结算链」零日志。
            plog.get().warning(
                "battle_result_rejected",
                reason="missing_match_id",
                authorized=authorized,
                hint="result 为空或 match_id=0,未做任何落库 / 出箱副作用",
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "match_id required")
        if not result.stats:
            # 「打完什么都没有」的一种真因:DS 报了空 stats。
            plog.get().warning(
                "battle_result_rejected",
                match_id=result.match_id,
                ds_pod_name=result.ds_pod_name,
                reason="missing_stats",
                authorized=authorized,
                outcome=enum_name(battle_pb2.BattleOutcome, result.outcome),
                hint="DS 上报 stats 为空,本场不落库(玩家无战绩 / 无段位 / 无掉落)",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "stats required for match %d", result.match_id
            )

        # ── 权威字段覆盖(§9.6 数值不信 DS)────────────────────────────────
        # 授权同步路径下 game_mode/map_id 一律以 canonical BattleStorageRecord 为准,
        # 在任何 MMR/DB/outbox 副作用**之前**覆盖。canonical 为空也照覆盖为空:
        # 宁可少存元数据,也不把不可信请求字段伪装成权威事实。
        if terminal_release is not None:
            result.game_mode = terminal_release.game_mode
            result.map_id = terminal_release.map_id

        # 正常结算:outcome 缺省补 NORMAL。
        if result.outcome == battle_pb2.BATTLE_OUTCOME_UNSPECIFIED:
            result.outcome = battle_pb2.BATTLE_OUTCOME_NORMAL

        # 本局段位池(结算入账的分区键):算 Elo 读同一池的分,出箱按同一池入账。
        rating_pool = _settlement_rating_pool(terminal_release)

        abandoned = result.outcome == battle_pb2.BATTLE_OUTCOME_ABANDONED
        if abandoned:
            # ★ 不变量 ③ ABANDONED 短路:delta 全 0,且**完全不进 settlement_runs_elo /
            #   不读 MMR reader**。ABANDONED 是补偿语义(权威路径是 ds.lifecycle →
            #   handle_abandoned,delta 全 0 不掉段);此处兜底防 battle.result 误报 /
            #   伪造 Outcome=ABANDONED 借补偿语义改玩家段位(不变量 §4/§6)。
            for s in result.stats:
                s.mmr_delta = 0
        else:
            run_elo, basis = brost.settlement_runs_elo(
                None if terminal_release is None else terminal_release.rating_mode,
                "" if terminal_release is None else terminal_release.game_mode,
            )
            if not run_elo:
                for s in result.stats:
                    s.mmr_delta = 0
            else:
                await self._assign_mmr(result, rating_pool)
            # 判据可观测:一局到底按什么算的分,事后必须能查(尤其是回落旧口径的局)。
            if basis in brost.LEGACY_FALLBACK_BASES:
                plog.get().warning(
                    "battle_rating_basis_legacy_fallback",
                    match_id=result.match_id,
                    map_id=result.map_id,
                    game_mode=result.game_mode,
                    rating_pool=rating_pool,
                    run_elo=run_elo,
                    basis=basis,
                    hint="本局 canonical rating_mode 未定格(旧 matchmaker / 旧批次表),"
                    "按旧口径结算",
                )
            else:
                plog.get().debug(
                    "battle_rating_basis",
                    match_id=result.match_id,
                    rating_pool=rating_pool,
                    run_elo=run_elo,
                    basis=basis,
                )

        if abandoned and terminal_release is not None:
            # 走了完整鉴权的同步结算却报 ABANDONED = DS 想借补偿语义绕过 MMR / 掉落规则。
            plog.get().warning(
                "battle_result_rejected",
                match_id=result.match_id,
                ds_pod_name=result.ds_pod_name,
                credential_pod=terminal_release.ds_pod_name,
                reason="abandoned_with_terminal_proof",
                players=len(result.stats),
                hint="授权同步路径不接受 ABANDONED"
                "(权威补偿路径是 ds.lifecycle → HandleAbandoned)",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "completed terminal release proof cannot settle abandoned match %d",
                result.match_id,
            )

        if terminal_release is not None:
            reason, exc = prepare_terminal_release(
                result, terminal_release, self._cfg.terminal_release_grace_td().total_seconds()
            )
            if exc is not None:
                # grace_out_of_range 是**纯配置错误**:配错一次会让**每一场**正常结算失败
                # 且监控面全绿,必须 ERROR 留证。
                plog.get().error(
                    "terminal_release_proof_rejected",
                    match_id=result.match_id,
                    allocation_id=terminal_release.allocation_id,
                    ds_pod_name=terminal_release.ds_pod_name,
                    reported_pod=result.ds_pod_name,
                    reason=reason,
                    grace_ms=int(
                        self._cfg.terminal_release_grace_td().total_seconds() * 1000
                    ),
                    code=errcode.as_code(exc),
                    err=str(exc),
                    hint="终态回收证明校验失败,本场结算整体拒绝;"
                    "reason=grace_out_of_range 时是配置错误(全服失败)",
                )
                raise exc

        try:
            outbox = self._build_outbox(result, abandoned, rating_pool)
        except errcode.PandoraError as exc:
            # 序列化失败 = 本进程 pb 定义与数据不自洽,整场段位事件一条都发不出去。
            plog.get().error(
                "battle_result_outbox_build_failed",
                match_id=result.match_id,
                players=len(result.stats),
                rating_pool=rating_pool,
                abandoned=abandoned,
                code=errcode.as_code(exc),
                err=str(exc),
                hint="player.update 出箱组装失败,本场未落库;DS 会按同一 match_id 重试",
            )
            raise

        # DS 上报的金币先**就地**钳到服务端上限,再落战绩与出箱(§9.6 数值不信 DS)。
        #
        # 必须钳完写回 result:battle_player_stats 与钱包发放读的是同一份 stats,
        # 只在出箱侧钳的话,战绩表会记着"本局 999 亿金币"而钱包只加了 100 万 ——
        # 玩家看战报会认为系统吞了收益,客服无从解释。
        self._clamp_reported_gold(result)

        # 战斗掉落出箱:正常结算才发放;ABANDONED(DS 崩溃补偿)不产出掉落。
        drop_outbox = [] if abandoned else self._build_drop_outbox(result)

        try:
            already, settle_info = await self._repo.save_result(
                result, outbox, drop_outbox, terminal_release, final_progress_seq
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # ErrBattleResultDBWrite(6003) **不在** IsServerFault 白名单里 → access log
            # 走 rpc_ok(DEBUG)。不在这里打 ERROR,线上 info 级下 MySQL 抖动 / 死锁 /
            # 字段截断导致的**整场结算失败在本服零日志**,只能从 DS 侧看到拿 6003 无限重试。
            plog.get().error(
                "battle_result_persist_failed",
                match_id=result.match_id,
                ds_pod_name=result.ds_pod_name,
                outcome=enum_name(battle_pb2.BattleOutcome, result.outcome),
                players=len(result.stats),
                outbox_rows=len(outbox),
                drop_rows=len(drop_outbox),
                has_terminal_release=authorized,
                final_progress_seq=final_progress_seq,
                duration_ms=int((time.monotonic() - started_at) * 1000),
                code=errcode.as_code(exc),
                err=str(exc),
                hint="本场结算未落库,DS 会按同一 match_id 重试;"
                "查 MySQL 侧错误(死锁/超长/schema 漂移)",
            )
            raise

        if already:
            # 幂等命中本身是良性的,但「同 pod 重试」与「换了 pod 的僵尸 DS 重放」性质完全不同
            # (后者是 fencing 事件),必须能从这一条日志上区分。
            plog.get().info(
                "battle_result_idempotent_hit",
                match_id=result.match_id,
                ds_pod_name=result.ds_pod_name,
                credential_pod="" if terminal_release is None else terminal_release.ds_pod_name,
                players=len(result.stats),
                outcome=enum_name(battle_pb2.BattleOutcome, result.outcome),
                final_progress_seq=final_progress_seq,
                authorized=authorized,
            )
            return True

        # 整条结算链**唯一**一条「这一局已落库」的状态推进日志(§11.3 R1)。
        plog.get().info(
            "battle_result_recorded",
            match_id=result.match_id,
            ds_pod_name=result.ds_pod_name,
            winner_team=result.winner_team,
            outcome=enum_name(battle_pb2.BattleOutcome, result.outcome),
            players=len(result.stats),
            rating_pool=rating_pool,
            map_id=result.map_id,
            game_mode=result.game_mode,
            drop_rows=len(drop_outbox),
            drops_suppressed=settle_info.drops_suppressed,
            final_progress_seq=final_progress_seq,
            applied_seq=settle_info.last_applied_seq,
            duration_ms=int((time.monotonic() - started_at) * 1000),
        )

        reconcile_progress(result.match_id, final_progress_seq, settle_info)
        log_drop_suppression(result.match_id, drop_outbox, settle_info.drops_suppressed)
        return False

    # ── HandleAbandoned:DS 崩溃补偿 ───────────────────────────────────────

    async def handle_abandoned(
        self, match_id: int, player_ids: list[int], map_id: int, game_mode: str, ts_ms: int
    ) -> None:
        """处理 ds_allocator 发来的 ABANDONED 事件(不变量 §4)。

        写一条 outcome=ABANDONED、mmr_delta 全 0 的补偿记录(幂等)。
        同样收口实时进度水位(final_progress_seq=0):打终局标记后,分区恢复的僵尸 DS
        再上报进度一律拒;崩溃前已入账的经验 / 掉落按需求**保留不回滚**。
        """
        if match_id == 0:
            # kafka 消费面没有 access log 兜底:不打就是「ds.lifecycle 收到了但什么都没发生」。
            plog.get().warning(
                "battle_abandoned_rejected",
                reason="missing_match_id",
                players=len(player_ids),
                map_id=map_id,
                game_mode=game_mode,
                hint="ABANDONED 事件缺 match_id,无法写补偿记录"
                "(上游 ds_allocator 发事件时漏填)",
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "match_id required")
        if ts_ms <= 0:
            ts_ms = int(time.time() * 1000)

        result = battle_pb2.BattleResult(
            match_id=match_id,
            ended_at_ms=ts_ms,
            winner_team=WINNER_TEAM_DRAW,
            outcome=battle_pb2.BATTLE_OUTCOME_ABANDONED,
            game_mode=game_mode,
            map_id=map_id,
            stats=[battle_pb2.PlayerStats(player_id=pid, mmr_delta=0) for pid in player_ids],
        )

        try:
            # ABANDONED 补偿:delta 恒 0,段位池取默认值即可(0 分入账不改任何池的分,
            # 但事件仍需一个确定的池字段,消费侧才不会因空值走不同分支)。
            outbox = self._build_outbox(result, True, rating.DEFAULT_POOL)
        except errcode.PandoraError as exc:
            plog.get().error(
                "battle_abandoned_outbox_build_failed",
                match_id=match_id,
                players=len(player_ids),
                map_id=map_id,
                game_mode=game_mode,
                code=errcode.as_code(exc),
                err=str(exc),
                hint="补偿出箱组装失败,本场未写补偿记录;ds.lifecycle 消费会重试",
            )
            raise

        try:
            already, _ = await self._repo.save_result(result, outbox, None, None, 0)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().error(
                "battle_abandoned_persist_failed",
                match_id=match_id,
                players=len(player_ids),
                map_id=map_id,
                game_mode=game_mode,
                outbox_rows=len(outbox),
                code=errcode.as_code(exc),
                err=str(exc),
                hint="ABANDONED 补偿未落库,ds.lifecycle 消费会重试;查 MySQL 侧错误",
            )
            raise
        if already:
            # 已有正常结算或已补偿过 → 不重复(不变量 §2)
            plog.get().info("abandoned_idempotent_hit", match_id=match_id)
            return
        plog.get().info(
            "battle_abandoned_recorded", match_id=match_id, players=len(player_ids)
        )

    # ── 查询 ──────────────────────────────────────────────────────────────

    async def get_match_result(self, match_id: int) -> battle_pb2.BattleResult | None:
        if match_id == 0:
            plog.get().warning("battle_result_query_rejected", reason="missing_match_id")
            raise errcode.PandoraError(errcode.ErrInvalidArg, "match_id required")
        try:
            return await self._repo.get_result(match_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 读错误不是 server fault,access log 记 rpc_ok(DEBUG) —— 不打就零日志。
            plog.get().warning(
                "battle_result_query_failed",
                match_id=match_id,
                code=errcode.as_code(exc),
                err=str(exc),
            )
            raise

    async def list_player_history(
        self, player_id: int, limit: int, before_ms: int
    ) -> list[battle_pb2.BattleResult]:
        if player_id == 0:
            plog.get().warning("battle_history_query_rejected", reason="missing_player_id")
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        try:
            return await self._repo.list_player_history(player_id, limit, before_ms)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "battle_history_query_failed",
                player_id=player_id,
                limit=limit,
                before_ms=before_ms,
                code=errcode.as_code(exc),
                err=str(exc),
            )
            raise

    # ── 辅助:MMR / 出箱组装 ──────────────────────────────────────────────

    async def _assign_mmr(self, result: battle_pb2.BattleResult, rating_pool: str) -> None:
        """按两队当前 MMR 均值算 Elo delta,写回每个 stat.mmr_delta(不变量 §6)。

        单个玩家读失败只回退 base_mmr 并 WARN,**不阻断落库** —— player 不可用时
        整场结算失败的代价远大于一场对局用兜底分算 Elo。
        """
        sum0 = n0 = sum1 = n1 = 0
        for s in result.stats:
            try:
                m = await self._mmr.get_mmr(s.player_id, rating_pool)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                m = self._cfg.base_mmr
                # match_id 必须手写:DS 回调面没有玩家 JWT,日志上下文不会自动带
                # player_id / match_id;缺了这条就接不回具体一局。
                plog.get().warning(
                    "mmr_read_failed_fallback_base",
                    match_id=result.match_id,
                    player_id=s.player_id,
                    rating_pool=rating_pool,
                    base_mmr=self._cfg.base_mmr,
                    err=str(exc),
                )
            if s.team == WINNER_TEAM_A:
                sum0 += m
                n0 += 1
            else:
                sum1 += m
                n1 += 1
        # ★ 整除(Go 的 int 除法)。用真除会让均分带小数,期望胜率随之偏移,
        #   同一场对局两栈算出不同 delta。
        avg_a = sum0 // n0 if n0 > 0 else self._cfg.base_mmr
        avg_b = sum1 // n1 if n1 > 0 else self._cfg.base_mmr
        delta_a, delta_b = elo_deltas(
            avg_a, avg_b, self._cfg.elo_k_factor, result.winner_team
        )
        for s in result.stats:
            s.mmr_delta = delta_a if s.team == WINNER_TEAM_A else delta_b

    def _build_outbox(
        self, result: battle_pb2.BattleResult, abandoned: bool, rating_pool: str
    ) -> list[brepo.OutboxRecord]:
        """把每个玩家组装成 player.update 出箱记录(待发布,与落库同事务)。"""
        recs: list[brepo.OutboxRecord] = []
        for s in result.stats:
            reason = "abandon" if abandoned else reason_for_team(s.team, result.winner_team)
            evt = player_pb2.PlayerUpdateEvent(
                player_id=s.player_id,
                match_id=result.match_id,
                mmr_delta=s.mmr_delta,
                reason=reason,
                ts_ms=result.ended_at_ms,
                # 段位池随事件带给 player 服务:消费侧按本值分区入账,不再自己猜。
                rating_pool=rating_pool,
            )
            try:
                payload = evt.SerializeToString()
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "marshal player.update player=%d: %s",
                    s.player_id,
                    exc,
                ) from exc
            recs.append(brepo.OutboxRecord(player_id=s.player_id, payload=payload))
        return recs

    def _clamp_reported_gold(self, result: battle_pb2.BattleResult) -> None:
        """把 DS 上报的每玩家金币**就地**钳到服务端上限(§9.6 数值不信 DS)。

        超限只截断不拒整场:战绩落库失败会连带段位、任务、掉落一起丢,代价远大于
        少发点钱;但每次截断都留 Warn,异常 / 越权上报的 DS 照样可发现。

        ★ 负数也必须钳成 0。Go 侧 gold 是 uint64,负数表示不出来所以那边只要管上界;
        Python 从 pb 读出来的虽也是非负,但 biz 层被单测 / 内部调用直接塞负数时,
        负金币会一路走进 GrantItems 的扣款语义 —— 下界闸不能跟着 Go 一起省掉。
        """
        max_gold = self._cfg.max_battle_gold_per_player()
        for s in result.stats:
            if s.gold < 0:
                plog.get().warning(
                    "battle_gold_negative",
                    match_id=result.match_id,
                    player_id=s.player_id,
                    reported=s.gold,
                    hint="DS 上报负金币(只可能是 bug 或伪造),已归零",
                )
                s.gold = 0
                continue
            if s.gold <= max_gold:
                continue
            plog.get().warning(
                "battle_gold_truncated",
                match_id=result.match_id,
                player_id=s.player_id,
                reported=s.gold,
                kept=max_gold,
                hint="DS 上报金币超服务端上限(配置错误或越权上报)",
            )
            s.gold = max_gold

    def _build_drop_outbox(
        self, result: battle_pb2.BattleResult
    ) -> list[brepo.DropOutboxRecord]:
        """把每个玩家的战斗掉落**与本局金币**组装成 drop 出箱记录(与落库同事务)。

        DS 不可信:逐条按同源 drop×item 过滤 DS 上报的 dropped_item_config_ids,
        item/drop 缺失一律 fail-closed。每玩家最多保留 cfg.max_drops_per_player() 条 ——
        防异常/恶意 DS 重复上报海量 ID 撑爆 VARCHAR(512) 导致**整场结算回滚**。

        ★ 金币搭的是**同一条**战后发放出箱:同一套幂等键、同一套重试、同一套审计,
          不另起一条"金币专用发放链"(§15.2 最少复杂度)。此前 PlayerStats.gold 只写进
          battle_player_stats 战绩表、**从来没有进过玩家钱包**,战后的"获得 N 金币"
          是纯展示 —— 而且不报错,查不出来。

        金币与掉落是**独立**收益来源:没掉落、或掉落全被过滤,都不能顺手把金币也丢掉。
        """
        max_drops = self._cfg.max_drops_per_player()
        recs: list[brepo.DropOutboxRecord] = []
        for s in result.stats:
            # 值已由 _clamp_reported_gold 钳过上限,这里直接用(出箱行是**已裁决的事实**)。
            gold_granted = s.gold
            reported = list(s.dropped_item_config_ids)
            if not reported:
                # 没掉落但有金币:仍要出一条只带货币的行,否则金币照旧发不出去。
                if gold_granted > 0:
                    recs.append(
                        brepo.DropOutboxRecord(
                            player_id=s.player_id,
                            item_config_ids=[],
                            currency_amount=gold_granted,
                        )
                    )
                continue
            allowed: list[int] = []
            stacks: list[int] = []
            instances: list[int] = []
            truncated = False
            # 被过滤掉的 ID 去重收集:「打完没掉落」最常见的真因就是 item/drop 表漏配某个 ID,
            # 只报「报了 N 个全被过滤」而不给 ID,排障没有落点。
            filtered_ids: set[int] = set()
            sample_filtered_id = 0
            for cid in reported:
                definition = self.battle_item_definition(cid) if cid != 0 else None
                if cid != 0 and definition is not None and definition.droppable:
                    if len(allowed) >= max_drops:
                        truncated = True
                        break
                    allowed.append(cid)
                    if definition.equipment:
                        instances.append(cid)
                    else:
                        stacks.append(cid)
                    continue
                if cid not in filtered_ids:
                    filtered_ids.add(cid)
                    if sample_filtered_id == 0:
                        sample_filtered_id = cid
            if truncated:
                plog.get().warning(
                    "battle_drop_truncated",
                    match_id=result.match_id,
                    player_id=s.player_id,
                    reported=len(reported),
                    kept=len(allowed),
                    max=max_drops,
                )
            if not allowed:
                plog.get().warning(
                    "battle_drop_all_filtered",
                    match_id=result.match_id,
                    player_id=s.player_id,
                    reported=len(reported),
                    distinct_item_ids=len(filtered_ids),
                    sample_item_config_id=sample_filtered_id,
                    hint="item/drop 表漏配该 ID(改表)或 DS 上报未授权掉落(安全信号)",
                )
                # 掉落全被过滤不代表金币也该丢:两者是独立的收益来源。
                if gold_granted > 0:
                    recs.append(
                        brepo.DropOutboxRecord(
                            player_id=s.player_id,
                            item_config_ids=[],
                            currency_amount=gold_granted,
                        )
                    )
                continue
            recs.append(
                brepo.DropOutboxRecord(
                    player_id=s.player_id,
                    item_config_ids=allowed,
                    stack_item_config_ids=stacks,
                    instance_item_config_ids=instances,
                    currency_amount=gold_granted,
                )
            )
        return recs

    # ── 后台:player.update 出箱发布器 ────────────────────────────────────

    async def run_outbox_publisher(self) -> None:
        """周期发布 player.update 出箱,直到被取消。对应 Go 的 RunOutboxPublisher。"""
        interval = self._cfg.outbox_publish_interval_td().total_seconds()
        if interval <= 0:
            interval = 2.0
        plog.get().info(
            "outbox_publisher_started",
            interval=self._cfg.outbox_publish_interval,
            batch=self._cfg.outbox_batch_size_or_default(),
        )
        try:
            await safego.loop("battle_outbox_publisher", interval, self._publish_outbox_tick)
        finally:
            plog.get().info("outbox_publisher_stopped")

    async def _publish_outbox_tick(self) -> None:
        try:
            await self.publish_outbox_batch()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # ★ 刻意不在批级日志里打"本轮投出去几条":Python 侧是抛异常出来的,
            # 到这里已经拿不到真实条数,填 0 会是**假数字**。准确的条数在行级
            # `outbox_publish_failed` 的 published_before 字段上(那才是排障要看的)。
            plog.get().warning("outbox_publish_batch_failed", err=str(exc))

    async def publish_outbox_batch(self) -> int:
        """取一批出箱记录投递,返回本轮成功投递并删除的条数。

        ★ 投递失败**立即中断本轮**(保留出箱行下轮重试)—— 段位事件按 player_id 保序,
          跳过失败行继续发后面的会让同一玩家的段位事件乱序。
        """
        if self._pusher is None:
            # kafka 未配置:出箱无法投递。出箱行已落库不丢,等 producer 可用后重启再发。
            # 积压必须可见:启动仅有一条 kafka_brokers_empty WARN,运行期若段位事件持续
            # 堆积而零信号,「打完段位没变」会被当成结算 bug 排查(实为部署缺 kafka)。
            recs = await self._repo.fetch_outbox(1)
            if recs:
                ok, streak = _outbox_no_pusher_log.admit(int(time.time() * 1000), 60_000)
                if ok:
                    plog.get().warning(
                        "outbox_pending_without_pusher",
                        streak_ticks=streak,
                        hint="kafka producer 未配置但段位出箱有积压:配置 kafka.brokers 后重启",
                    )
            return 0

        recs = await self._repo.fetch_outbox(self._cfg.outbox_batch_size_or_default())
        published = 0
        for r in recs:
            row_started = time.monotonic()
            token = plog.bind_trace_id(str(uuid.uuid4()))
            try:
                try:
                    await self._pusher.push_player_update(r.player_id, r.payload)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    # 批级失败说不出卡在谁身上;段位事件按 player_id 保序,
                    # 卡住的这一行会挡住该玩家后续所有 player.update。
                    plog.get().warning(
                        "outbox_publish_failed",
                        player_id=r.player_id,
                        outbox_id=r.id,
                        published_before=published,
                        elapsed_ms=int((time.monotonic() - row_started) * 1000),
                        code=errcode.as_code(exc),
                        err=str(exc),
                        hint="本轮中断保留出箱行下轮重试(同玩家保序);持续失败查 kafka producer",
                    )
                    raise
                try:
                    await self._repo.delete_outbox(r.id)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    # 已投递但未 ACK:下轮会重投同一事件(player 侧 mmr_history uk 幂等吸收)。
                    plog.get().warning(
                        "outbox_delete_failed",
                        player_id=r.player_id,
                        outbox_id=r.id,
                        published_before=published,
                        elapsed_ms=int((time.monotonic() - row_started) * 1000),
                        code=errcode.as_code(exc),
                        err=str(exc),
                        hint="kafka 已投递但出箱行未删除 → 下轮重投同一事件(下游幂等吸收)",
                    )
                    raise
                # 逐玩家成功台账:段位是玩家资产,「该玩家的段位事件已投出」必须能按
                # player_id 在 info 级正查,不能只靠「没有 outbox_publish_failed」反证。
                plog.get().info(
                    "player_update_delivered",
                    player_id=r.player_id,
                    outbox_id=r.id,
                    elapsed_ms=int((time.monotonic() - row_started) * 1000),
                )
                published += 1
            finally:
                token.var.reset(token)
        if published > 0:
            plog.get().debug("outbox_published", count=published)
        return published

    # ── 后台:掉落出箱发布器 ──────────────────────────────────────────────

    async def run_drop_publisher(self) -> None:
        """周期发放掉落出箱。granter 未注入 = inventory_addr 未配 → 直接返回不空转。"""
        if self._granter is None:
            plog.get().info(
                "drop_publisher_disabled",
                hint="inventory_addr 未配置 → 战斗装备掉落不发放(出箱积压不丢)",
            )
            return
        interval = self._cfg.drop_publish_interval_td().total_seconds()
        if interval <= 0:
            interval = 2.0
        plog.get().info(
            "drop_publisher_started",
            interval=self._cfg.drop_publish_interval,
            batch=self._cfg.drop_batch_size_or_default(),
        )
        try:
            await safego.loop("battle_drop_publisher", interval, self._publish_drop_tick)
        finally:
            plog.get().info("drop_publisher_stopped")

    async def _publish_drop_tick(self) -> None:
        try:
            await self.publish_drop_batch()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 同 _publish_outbox_tick:准确条数在行级日志上,批级不填假数字。
            plog.get().warning("drop_publish_batch_failed", err=str(exc))

    async def publish_drop_batch(self) -> int:
        """取一批掉落出箱行发放。

        ★ 与 player.update 出箱不同:掉落**无跨玩家保序需求** → 单行失败只 continue,
          不中断本轮 —— 否则某个玩家背包满会阻塞全服其他玩家的掉落。
        """
        if self._granter is None:
            return 0
        recs = await self._repo.fetch_drop_outbox(self._cfg.drop_batch_size_or_default())
        granted = 0
        for r in recs:
            row_started = time.monotonic()
            key = drop_idempotency_key(r.match_id, r.player_id)
            token = plog.bind_trace_id(str(uuid.uuid4()))
            try:
                try:
                    await self._deliver_drop_record(r)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    # 任一路由失败都保留整行;已成功的另一路由靠独立幂等键回放,不会重复入账。
                    plog.get().warning(
                        "drop_grant_failed",
                        match_id=r.match_id,
                        player_id=r.player_id,
                        outbox_id=r.id,
                        items=len(r.item_config_ids),
                        stack_items=len(r.stack_item_config_ids),
                        instance_items=len(r.instance_item_config_ids),
                        currency_amount=r.currency_amount,
                        idempotency_key=key,
                        elapsed_ms=int((time.monotonic() - row_started) * 1000),
                        code=errcode.as_code(exc),
                        err=str(exc),
                        hint="出箱行保留下轮重试(at-least-once);"
                        "持续失败查 inventory / mail 侧同 trace_id",
                    )
                    continue
                try:
                    await self._repo.delete_drop_outbox(r.id)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    plog.get().warning(
                        "drop_outbox_delete_failed",
                        match_id=r.match_id,
                        player_id=r.player_id,
                        outbox_id=r.id,
                        idempotency_key=key,
                        elapsed_ms=int((time.monotonic() - row_started) * 1000),
                        code=errcode.as_code(exc),
                        err=str(exc),
                        hint="掉落已发放但出箱行未删 → 下轮重发(inventory 同幂等键去重)",
                    )
                    raise
                # 资产变更必须有逐玩家台账:失败有行、成功没行时,「掉落到底发没发」
                # 在出箱行被删后只能反查 inventory。
                plog.get().info(
                    "drop_grant_delivered",
                    match_id=r.match_id,
                    player_id=r.player_id,
                    outbox_id=r.id,
                    stack_items=len(r.stack_item_config_ids),
                    instance_items=len(r.instance_item_config_ids),
                    # 金币进没进钱包必须在台账上看得见:出箱行删掉之后,
                    # "这局金币到底发没发"就只能反查 inventory 流水了。
                    currency_amount=r.currency_amount,
                    idempotency_key=key,
                    elapsed_ms=int((time.monotonic() - row_started) * 1000),
                )
                granted += 1
            finally:
                token.var.reset(token)
        if granted > 0:
            plog.get().debug("drop_outbox_granted", count=granted)
        return granted

    async def _deliver_drop_record(self, r: brepo.DropOutboxRecord) -> None:
        """按**首次入箱时冻结**的路由发放。重试绝不读 item/drop 热配置。

        重算路由的后果:stack 已成功、instance 失败期间的类型热更会换掉 method 与
        子幂等键,造成已成功那部分双发。
        """
        stacks = (
            aggregate_stack_grants(r.stack_item_config_ids) if r.stack_item_config_ids else []
        )
        instances = list(r.instance_item_config_ids)
        currency = r.currency_amount if r.currency_amount > 0 else 0
        if not stacks and not instances and currency == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "drop outbox row has no frozen route id=%d", r.id
            )
        # 金币与可堆叠道具走同一次 GrantItems:共用一个幂等键、一个事务,不会出现
        # "道具到了钱没到"。纯金币行(无掉落)也走这条路径。
        grants_items = bool(stacks) or currency > 0
        base_key = drop_idempotency_key(r.match_id, r.player_id)
        stack_key = instance_key = base_key
        # ★ 分叉判据是"**这一行会不会真的发两次调用**",不是"stacks 和 instances 都非空"。
        #   按后者判,`金币 + 装备`(stacks 空)两次调用会拿到同一把键 —— inventory 端把
        #   第二次当重放吞掉,装备静默不发。
        if grants_items and instances:
            stack_key, instance_key = base_key + ":stack", base_key + ":instance"
        if grants_items:
            await self._granter.grant_items(r.player_id, stacks, currency, stack_key)
        if not instances:
            return
        try:
            await self._granter.grant_instances(r.player_id, instances, instance_key)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # ★ 只有**实例背包满**才允许转邮件;堆叠物品已走计数模型,
            #   不应进入装备邮件链(转了会在邮件里变成一堆无法领取的实例附件)。
            if (
                self._mail_sender is not None
                and errcode.as_code(exc) == errcode.ErrInventoryCapacityFull
            ):
                try:
                    await self._mail_sender.send_overflow_mail(
                        r.player_id, instances, instance_key
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as mexc:  # noqa: BLE001
                    plog.get().warning(
                        "drop_overflow_mail_failed",
                        match_id=r.match_id,
                        player_id=r.player_id,
                        outbox_id=r.id,
                        items=len(instances),
                        idempotency_key=instance_key,
                        code=errcode.as_code(mexc),
                        err=str(mexc),
                    )
                    raise
                plog.get().info(
                    "drop_overflow_mailed",
                    match_id=r.match_id,
                    player_id=r.player_id,
                    outbox_id=r.id,
                    items=len(instances),
                    idempotency_key=instance_key,
                )
                return
            raise

    # ── 后台:撮合状态释放出箱发布器 ──────────────────────────────────────

    async def run_match_release_publisher(self) -> None:
        interval = self._cfg.outbox_publish_interval_td().total_seconds()
        if interval <= 0:
            interval = 2.0
        plog.get().info(
            "match_release_publisher_started",
            interval=self._cfg.outbox_publish_interval,
            batch=self._cfg.outbox_batch_size_or_default(),
        )
        try:
            await safego.loop(
                "battle_match_release_publisher", interval, self._publish_match_release_tick
            )
        finally:
            plog.get().info("match_release_publisher_stopped")

    async def _publish_match_release_tick(self) -> None:
        try:
            await self.publish_match_release_batch()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 同 _publish_outbox_tick:准确条数在行级 match_release_failed 上。
            plog.get().warning("match_release_batch_failed", err=str(exc))

    async def publish_match_release_batch(self) -> int:
        """释放 matchmaker 的 ticket/claim/match 状态。

        ★ **明确成功才删行**;RPC 失败或响应未知只延期重试 ——
          未知即删的后果是残留 claim 永不释放,玩家回 Hub 再匹配恒撞 4002。
        """
        if self._releaser is None:
            return 0
        now_ms = int(time.time() * 1000)
        recs = await self._repo.fetch_match_release_outbox(
            self._cfg.outbox_batch_size_or_default(), now_ms
        )
        released = 0
        first_error: BaseException | None = None
        for rec in recs:
            row_started = time.monotonic()
            token = plog.bind_trace_id(str(uuid.uuid4()))
            try:
                try:
                    await asyncio.wait_for(
                        self._releaser.release_match(rec.match_id, rec.player_ids), timeout=10.0
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    next_ms = int(
                        (time.time() + match_release_retry_delay(rec.attempt_count)) * 1000
                    )
                    plog.get().warning(
                        "match_release_failed",
                        match_id=rec.match_id,
                        outbox_id=rec.id,
                        players=len(rec.player_ids),
                        attempt=rec.attempt_count,
                        next_attempt_at_ms=next_ms,
                        elapsed_ms=int((time.monotonic() - row_started) * 1000),
                        code=errcode.as_code(exc),
                        err=str(exc),
                        hint="matchmaker 撮合状态未释放,玩家回 Hub 再匹配可能撞 "
                        "ErrMatchAlreadyMatching(4002)",
                    )
                    first_error = first_error or exc
                    try:
                        await self._repo.defer_match_release_outbox(rec.id, next_ms)
                    except asyncio.CancelledError:
                        raise
                    except BaseException as derr:  # noqa: BLE001
                        plog.get().warning(
                            "match_release_defer_failed",
                            match_id=rec.match_id,
                            outbox_id=rec.id,
                            next_attempt_at_ms=next_ms,
                            code=errcode.as_code(derr),
                            err=str(derr),
                            hint="退避写入失败:下轮 Fetch 仍会取到该行重试(不影响 at-least-once)",
                        )
                    continue
                try:
                    await self._repo.delete_match_release_outbox(rec.id)
                except asyncio.CancelledError:
                    raise
                except BaseException as derr:  # noqa: BLE001
                    plog.get().warning(
                        "match_release_delete_failed",
                        match_id=rec.match_id,
                        outbox_id=rec.id,
                        players=len(rec.player_ids),
                        elapsed_ms=int((time.monotonic() - row_started) * 1000),
                        code=errcode.as_code(derr),
                        err=str(derr),
                        hint="matchmaker 已释放但出箱行未删 → 下轮重调 ReleaseMatch"
                        "(matchmaker 侧幂等)",
                    )
                    first_error = first_error or derr
                    continue
                plog.get().info(
                    "match_release_published",
                    match_id=rec.match_id,
                    outbox_id=rec.id,
                    players=len(rec.player_ids),
                    attempt=rec.attempt_count,
                    elapsed_ms=int((time.monotonic() - row_started) * 1000),
                )
                released += 1
            finally:
                token.var.reset(token)
        if first_error is not None:
            raise first_error
        return released

    # ── 后台:Battle terminal-release 事务出箱(Model-B)────────────────────

    async def run_terminal_release_publisher(self) -> None:
        """启动正常结算资源回收 worker。对应 Go 的 `RunTerminalReleasePublisher`。

        它**只能**在 MySQL schema probe、relay 构造和 dsauth capability 获取全部成功后
        启动(main 侧的顺序保证)。单行失败保留重试、不阻塞同批其它对局;UID
        precondition 与 ds_allocator 的 Redis CAS 保证多副本 / 响应丢失幂等。
        """
        if self._terminal_relay is None:
            plog.get().info("terminal_release_publisher_disabled")
            return
        interval = self._cfg.terminal_release_interval_td().total_seconds()
        if interval <= 0:
            interval = 2.0
        plog.get().info(
            "terminal_release_publisher_started",
            interval=self._cfg.terminal_release_interval,
            batch=self._terminal_release_batch_size(),
        )
        try:
            await safego.loop(
                "battle_terminal_release_publisher",
                interval,
                self._publish_terminal_release_tick,
            )
        finally:
            plog.get().info("terminal_release_publisher_stopped")

    async def _publish_terminal_release_tick(self) -> None:
        try:
            await self.publish_terminal_release_batch()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 准确的行级原因在 terminal_release_phase1_failed / _finalize_failed /
            # _mark_failed / _delete_failed 上;这里只记"本轮整批中断"。
            plog.get().warning("terminal_release_batch_failed", err=str(exc))

    def _terminal_release_batch_size(self) -> int:
        return self._cfg.terminal_release_batch_size_or_default()

    async def publish_terminal_release_batch(self) -> int:  # noqa: C901 —— 与 Go 同为线性两阶段
        """两阶段推进一批终态回收行。返回本轮 finalize(删行)的条数。

        ★ 阶段顺序不可交换,且**每一步的失败处置都不一样**:
          - phase1 RPC 失败       → `continue`(保留原行重试;Redis/K8s unknown 绝不推进 DB)
          - phase1 mark 失败      → `raise`(中断整批:Redis CAS 已成功但 durable ACK 未知,
                                    下轮必须按库真实状态重读,不能带着错误认知继续)
          - phase2 finalize 失败  → `continue`(released 行保留;重试只校验 / Expire 同 proof)
          - phase2 delete 失败    → `raise`(同上,中断本轮)
        """
        if self._terminal_relay is None:
            return 0
        recs = await self._repo.fetch_terminal_release_outbox(
            self._terminal_release_batch_size(), int(time.time() * 1000)
        )
        finalized = 0
        for rec in recs:
            if rec.released_at_ms < 0:
                # 库里出现不可能的值 = schema 漂移 / 写入者 bug,本轮整批停止且会反复发生。
                plog.get().error(
                    "terminal_release_row_invalid",
                    match_id=rec.match_id,
                    allocation_id=rec.allocation_id,
                    pod=rec.ds_pod_name,
                    outbox_id=rec.id,
                    released_at_ms=rec.released_at_ms,
                    reason="negative_released_at_ms",
                    hint="terminal_release_outbox 行非法,本轮中断;DS pod 不会被回收,需人工排查",
                )
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "terminal release outbox id=%d has invalid released_at_ms",
                    rec.id,
                )
            row_started = time.monotonic()
            token = plog.bind_trace_id(str(uuid.uuid4()))
            try:
                if rec.released_at_ms == 0:
                    try:
                        await asyncio.wait_for(
                            self._terminal_relay.release_terminal(rec), timeout=10.0
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:  # noqa: BLE001
                        # Redis/K8s unknown 绝不能推进 DB phase;永久墓碑 / 原始行保留重试。
                        plog.get().warning(
                            "terminal_release_phase1_failed",
                            match_id=rec.match_id,
                            allocation_id=rec.allocation_id,
                            pod=rec.ds_pod_name,
                            outbox_id=rec.id,
                            elapsed_ms=int((time.monotonic() - row_started) * 1000),
                            code=errcode.as_code(exc),
                            err=str(exc),
                        )
                        continue
                    try:
                        marked = await self._repo.mark_terminal_release_released(
                            rec.id, int(time.time() * 1000)
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:  # noqa: BLE001
                        # UID delete 已成功但 durable ACK 未知:phase1 绝不 expire Redis。
                        # 下轮按 DB 真实状态重读;0 则重放 UID delete,>0 则进入 finalize。
                        plog.get().warning(
                            "terminal_release_mark_failed",
                            match_id=rec.match_id,
                            allocation_id=rec.allocation_id,
                            pod=rec.ds_pod_name,
                            outbox_id=rec.id,
                            elapsed_ms=int((time.monotonic() - row_started) * 1000),
                            code=errcode.as_code(exc),
                            err=str(exc),
                            hint="Redis terminal CAS 已成功但 MySQL durable ACK 未知,"
                            "本轮中断等下轮按库真实状态重跑",
                        )
                        raise
                    if not marked:
                        plog.get().debug(
                            "terminal_release_phase1_already_advanced",
                            match_id=rec.match_id,
                            outbox_id=rec.id,
                        )
                    else:
                        # 不可逆推进(Redis terminal/receipt CAS 已完成 + MySQL 已 durable 标记)。
                        plog.get().info(
                            "terminal_release_phase1_done",
                            match_id=rec.match_id,
                            allocation_id=rec.allocation_id,
                            pod=rec.ds_pod_name,
                            outbox_id=rec.id,
                            elapsed_ms=int((time.monotonic() - row_started) * 1000),
                        )
                    continue

                try:
                    await asyncio.wait_for(
                        self._terminal_relay.finalize_terminal(rec), timeout=10.0
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    # finalize 响应未知绝不能 DELETE released 行;重试只校验 / Expire 同 proof,
                    # 绝不再次删除 K8s。若 TTL 已自然清空全部墓碑,服务端按幂等成功返回。
                    plog.get().warning(
                        "terminal_release_finalize_failed",
                        match_id=rec.match_id,
                        allocation_id=rec.allocation_id,
                        pod=rec.ds_pod_name,
                        outbox_id=rec.id,
                        elapsed_ms=int((time.monotonic() - row_started) * 1000),
                        code=errcode.as_code(exc),
                        err=str(exc),
                    )
                    continue
                try:
                    await self._repo.delete_terminal_release_outbox(rec.id)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    # finalize 已成功但 DB delete 失败:released 行保留。下一轮只重放
                    # finalize;即使墓碑 TTL 已过、三键都不存在,也会幂等重认成功。
                    plog.get().warning(
                        "terminal_release_delete_failed",
                        match_id=rec.match_id,
                        allocation_id=rec.allocation_id,
                        pod=rec.ds_pod_name,
                        outbox_id=rec.id,
                        elapsed_ms=int((time.monotonic() - row_started) * 1000),
                        code=errcode.as_code(exc),
                        err=str(exc),
                        hint="回收已完成但出箱行未删 → 下轮重放 finalize(幂等)",
                    )
                    raise
                # 落在 DELETE 之后:先打日志再落库时,库操作失败会让日志与库状态互相矛盾,
                # 排障会按"已完成"处理(同 progress_unknown_fact_stream_stopped 的纪律)。
                plog.get().info(
                    "terminal_release_finalized",
                    match_id=rec.match_id,
                    allocation_id=rec.allocation_id,
                    pod=rec.ds_pod_name,
                    outbox_id=rec.id,
                    released_at_ms=rec.released_at_ms,
                    elapsed_ms=int((time.monotonic() - row_started) * 1000),
                )
                finalized += 1
            finally:
                token.var.reset(token)
        if finalized > 0:
            plog.get().debug("terminal_release_outbox_finalized", count=finalized)
        return finalized

    # ── 后台:保留期清理(§9.24)──────────────────────────────────────────

    async def run_retention_sweep(self) -> None:
        interval = self._cfg.retention_sweep_interval_td().total_seconds()
        if interval <= 0:
            # 防御:未过 apply_defaults 的零值配置(safego.loop 对 <=0 直接返回,
            # 那会让清理**整个不跑**而日志只有一条 invalid_interval)。
            interval = 3600.0
        await safego.loop("battle_retention_sweep", interval, self.sweep_retention_once)

    async def sweep_retention_once(self) -> None:
        """跑一轮保留期清理(每类小批量循环删到追平)。独立函数便于单测。"""
        cutoff_ms = int((time.time() - self._cfg.history_retention_days * 86400) * 1000)
        mode = self._cfg.retention_mode_parsed()

        n = await self._drain_purge("battles", mode, cutoff_ms, self._repo.sweep_expired_battles)
        if n > 0:
            plog.get().info(
                "battle_retention_battles_purged",
                matches=n,
                retention_days=self._cfg.history_retention_days,
            )
        n = await self._drain_purge(
            "progress", mode, cutoff_ms, self._repo.sweep_settled_progress
        )
        if n > 0:
            plog.get().info(
                "battle_retention_progress_purged",
                matches=n,
                retention_days=self._cfg.history_retention_days,
            )

        # 陈年未结算水位 = 结算补偿链 bug 证据:按设计永不清理,但必须持续告警暴露
        # (否则"保留证据待排查"退化成静默永久保留,§9.24 有界承诺落空)。
        try:
            stale = await self._repo.count_stale_unsettled_progress(cutoff_ms)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "battle_retention_stale_unsettled_count_failed", err=str(exc)
            )
            return
        if stale > 0:
            plog.get().error(
                "battle_retention_stale_unsettled_progress",
                count=stale,
                retention_days=self._cfg.history_retention_days,
                hint="存在超保留期未结算的进度水位行(结算补偿链 bug),永不自动清理,"
                "需人工排查对应 match 的结算链路",
            )

    async def _drain_purge(self, kind: str, mode: dbguard.Mode, cutoff_ms: int, sweep) -> int:  # noqa: ANN001
        """单类清理:小批量循环删到短批(= 积压追平)为止。

        ★ **report_only 模式下只跑一轮**:那一轮已经数出全量待清理规模(不受 batch 截断),
          再循环只是重复同一次 COUNT。循环存在的意义是"追平积压",而只报告不删时
          积压永远追不平,循环会变成每轮固定跑满的空转。
        """
        batch = self._cfg.retention_sweep_batch
        if batch <= 0:
            # 防御:未过 apply_defaults 的零值(batch=0 时 truncated 永假 → 死循环)。
            batch = 200
        total = 0
        while True:
            try:
                out = await sweep(mode, cutoff_ms, batch)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "battle_retention_purge_failed",
                    kind=kind,
                    err=str(exc),
                    purged_before_fail=total,
                )
                return total
            if mode is not dbguard.Mode.DELETE:
                return 0  # 只报告:一轮即得全量待清理规模,不删也就无所谓"追平"
            total += out.deleted
            if not out.truncated:
                return total


# ── 模块级纯逻辑 ─────────────────────────────────────────────────────────────


def _settlement_rating_pool(terminal_release: brepo.TerminalReleaseRecord | None) -> str:
    """本局的段位池(结算入账的分区键)。

    只读 canonical 定格值,**绝不读 DS 请求体、也不在结算那一刻重查关卡表** ——
    热更改表不得改写正在打的那一局的规则。空值归一到默认池:段位必须有确定落点,
    不能因为缺一个字段就把玩家这一局的分丢掉(§9.22)。
    """
    if terminal_release is None:
        return rating.DEFAULT_POOL
    return rating.normalize(terminal_release.rating_pool)


def prepare_terminal_release(
    result: battle_pb2.BattleResult | None,
    rec: brepo.TerminalReleaseRecord | None,
    grace_sec: float,
) -> tuple[str, errcode.PandoraError | None]:
    """校验并定格终态回收证明。对应 Go 的 prepareTerminalRelease。

    返回 (reason, err):reason 只供调用方打日志(§11.3 R2 —— 原 Go 实现把 19 个子条件
    塌成一句话,pod / jti / exp / writer_epoch / 配置 grace 哪一项对不上完全查不出来);
    err 与拆分前逐字节一致,控制流不变。

    **顺序即契约**:同一份坏证明在两栈必须报同一个第一原因,否则排障结论会分叉。
    """
    checks: list[tuple[str, bool]] = [
        ("nil_result", result is None),
        ("nil_proof", rec is None),
    ]
    for reason, hit in checks:
        if hit:
            return reason, _tr_incomplete()
    assert result is not None and rec is not None  # noqa: S101 —— 上面两条已排除

    ordered: list[tuple[str, bool]] = [
        ("missing_match_id", rec.match_id == 0),
        ("match_id_mismatch", rec.match_id != result.match_id),
        ("missing_allocation_id", rec.allocation_id == ""),
        ("missing_pod", rec.ds_pod_name == ""),
        ("pod_mismatch", rec.ds_pod_name != result.ds_pod_name),
        ("missing_gameserver_uid", rec.gameserver_uid == ""),
        ("missing_instance_epoch", rec.instance_epoch == 0),
        ("missing_auth_gen", rec.auth_gen == 0),
        ("missing_jti", rec.auth_jti == ""),
        ("invalid_exp", rec.auth_exp_ms <= 0),
        ("missing_kid", rec.auth_kid == ""),
        ("missing_token_sha", rec.auth_token_sha256 == ""),
        ("writer_epoch_mismatch", rec.auth_writer_epoch != DS_AUTH_WRITER_EPOCH_V2),
        ("invalid_authorized_at", rec.authorized_at_ms <= 0),
        ("authorized_at_not_before_exp", rec.authorized_at_ms >= rec.auth_exp_ms),
        ("already_released", rec.released_at_ms != 0),
        ("empty_roster", not rec.player_ids),
    ]
    for reason, hit in ordered:
        if hit:
            return reason, _tr_incomplete()

    if grace_sec < 5 or grace_sec > 120:
        return "grace_out_of_range", errcode.PandoraError(
            errcode.ErrInvalidState, "terminal release grace is outside [5s,2m]"
        )
    now_ms = int(time.time() * 1000)
    if rec.authorized_at_ms > now_ms:
        return "authorized_in_future", errcode.PandoraError(
            errcode.ErrUnauthorized, "terminal release authorization time is in the future"
        )
    rec.release_after_ms = now_ms + int(grace_sec * 1000)
    # 只允许 phase1 worker 经 MySQL CAS 推进,调用方不能伪造。
    rec.released_at_ms = 0
    # MySQL writer owns created_at_ms;调用方不能伪造。
    rec.created_at_ms = 0
    return "", None


def _tr_incomplete() -> errcode.PandoraError:
    return errcode.PandoraError(
        errcode.ErrUnauthorized, "terminal release proof is incomplete or not bound to result"
    )


def log_drop_suppression(
    match_id: int, built: list[brepo.DropOutboxRecord], drops_suppressed: bool
) -> None:
    """记录「实时进度通道已接管道具发放」这一事实。对应 Go 的 logDropSuppression。

    built 是 build_drop_outbox 的**入箱前**列表(save_result 内部才按出箱裁剪规则逐行裁),
    所以这里能算出「本来要发什么、实际被掐掉了哪一部分」。

    2026-08-24 修字段口径与 Go 对齐:这条日志原先只打 `audit_rows=len(drop_outbox)`,
    那是个**假字段** —— 它的名字承诺"被跳过的行数",实际值却是"本来要入箱的全部行数",
    而金币行根本没被跳过(实时通道不发金币,那些行照常入箱、随后 drop_grant_delivered)。
    排障的人按 audit_rows 会以为这些行都没入箱,正是「修复本身成为下一次误判的源头」。
    换成可核对的分项后,每个数字都能拿去和出箱表对上:
      built_rows            入箱裁剪前的行数
      suppressed_item_rows  真的被掐掉道具的行数
      suppressed_items      被掐掉的道具件数
      granted_currency_rows / granted_currency_total  照常入箱的金币行数与总额

    只有**真的掐掉了道具**才打:全是纯金币行时一件道具都没被抑制,打 *_suppressed_*
    又会是一次同样的名不副实。
    """
    if not drops_suppressed or not built:
        return
    suppressed_rows = 0
    suppressed_items = 0
    currency_rows = 0
    currency_total = 0
    for d in built:
        n = len(d.item_config_ids)
        if n > 0:
            suppressed_rows += 1
            suppressed_items += n
        if d.currency_amount > 0:
            currency_rows += 1
            currency_total += d.currency_amount
    if suppressed_rows == 0:
        return
    plog.get().info(
        "battle_drop_suppressed_by_progress",
        match_id=match_id,
        built_rows=len(built),
        suppressed_item_rows=suppressed_rows,
        suppressed_items=suppressed_items,
        granted_currency_rows=currency_rows,
        granted_currency_total=currency_total,
        hint="只有**道具**被抑制(已由实时进度通道逐事件发放,结算字段仅审计);"
        "金币不走实时通道,granted_currency_rows 行照常入箱,随后同 match_id 会有 "
        "drop_grant_delivered",
    )


def reconcile_progress(
    match_id: int, final_seq: int, info: brepo.ProgressSettleInfo
) -> None:
    """实时进度通道对账。对应 Go 的 reconcileProgress。

    只告警**不自动补**(realtime-progression.md §9):尾窗事件丢失时自动补会把
    "DS 崩溃前没上报的事实"凭空造出来,而服务端根本没有那些事实。
    """
    if final_seq == 0 and not info.stream_existed:
        return  # 本场未走实时通道(旧 DS / 通道关),无需对账
    if final_seq > 0 and not info.stream_existed:
        plog.get().warning(
            "progress_reconcile_stream_missing",
            match_id=match_id,
            final_seq=final_seq,
            hint="DS 声称走了实时通道但服务端无水位(全部批次丢失或伪造 final_seq)",
        )
        return
    if final_seq != info.last_applied_seq:
        plog.get().warning(
            "progress_reconcile_gap",
            match_id=match_id,
            final_seq=final_seq,
            applied_seq=info.last_applied_seq,
            hint="尾窗事件丢失(DS 崩溃/网络),只告警不自动补(realtime-progression.md §9)",
        )
        return
    plog.get().debug(
        "progress_reconcile_ok", match_id=match_id, applied_seq=info.last_applied_seq
    )
