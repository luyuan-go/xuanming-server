"""battle_result 战斗中实时进度通道业务逻辑 —— 对应 Go 侧
`internal/biz/progress.go` + `internal/biz/mission_forward.go`
(docs/design/realtime-progression.md、docs/design/mission.md §5.1)。

链路:DS 异步批量 ReportProgress(事实事件,seq 幂等)
    → 本文件:校验(roster / 上限 / 升序)→ 换算(怪物经验表 / 掉落白名单,**DS 不可信**)
    → repo.apply_progress(水位 CAS + 进度出箱 + 任务出箱同事务)
    → run_progress_publisher:出箱 worker 幂等调 player.AddExperience /
      inventory.GrantInstances / GrantItems / ConsumeBattleItem / DiscardBattleItem
    → run_mission_forwarder:任务事实出箱 worker 幂等调 mission.ReportMissionFacts。

错误语义(DS 侧行为契约,battle.proto ReportProgress 注释;**两栈必须一致**,
否则同一个 DS 在 Go / Python 副本上会做出不同处置):
  · ErrUnavailable(水位竞争 / DB 瞬时)→ DS **原批重试**;
  · ErrInvalidArg / ErrUnauthorized(坏批 / 越权)→ DS **丢批告警**,继续后续批;
  · ErrInvalidState(对局已结算 / 通道关闭 / 未知事实类型)→ DS **停流**,不得无限重试。
    未知事实 = 新 DS 对旧 Go,能力不匹配是**整场性质**的,丢批语义会造成逐批永久丢失。
    停流后果:已 ACK 部分保持有效;水位 >0 时结算掉落发放保持抑制(单一权威路径),
    停流之后的拾取 / 经验不结算兜底,本场剩余实时奖励永久丢失 —— 该场景只该出现在
    违反「Go 先行发布」纪律时,不为违纪场景做兜底。

★ §9 不变量 6(派生数值一律服务端计算):DS 只报**事实**(杀了哪只怪、几只、
  归属权重),经验数值一律由服务端查 configtable role_level 换算;掉落只认白名单。

★ §16.2(幂等与顺序):水位 seq 去重 + 出箱幂等键
  `progress:{match_id}:{seq}:{player_id}:{kind}`,下游按键去重。
  不假设 DS 只上报一次、也不假设批次不重叠。
"""

from __future__ import annotations

import asyncio
import time
import uuid

from pandora.battle.v1 import battle_pb2
from pandora.mission.v1 import mission_pb2

from pandorapy import errcode, safego
from pandorapy import log as plog
from pandorapy.services.battle_result import progress_repo as bprepo

# 任务条件类别(取 mission proto 枚举,改编号即 import 期暴露;docs/design/mission.md §5.2)。
# ★ 禁止手抄字面量:这三个值同时被 mission 服务的条件表和本服的事实展开读,
#   抄错一个数字的表现是"某类任务永远不推进",而两边日志都正常。
MISSION_CATEGORY_KILL_MONSTER = int(
    mission_pb2.MISSION_CONDITION_CATEGORY_KILL_MONSTER
)
MISSION_CATEGORY_USE_ITEM = int(mission_pb2.MISSION_CONDITION_CATEGORY_USE_ITEM)
MISSION_CATEGORY_PICKUP_ITEM = int(
    mission_pb2.MISSION_CONDITION_CATEGORY_PICKUP_ITEM
)

# 「一整份经验」对应的归属权重(千分比满值)。
EXP_SHARE_PERMILLE_FULL = 1000

# 独立于 CSV / 配置错误的最终护栏。正常上限来自 item.max_stack;
# 硬上限只防热配置异常把单次同步事务放大到不可控规模。
MAX_BATTLE_ITEM_ACTION_COUNT_HARD = 1000

# 出箱类型 → 稳定 snake_case 名(日志字段用,不参与业务判定)。
# 日志里只打数字时,排障要回来翻常量表才知道 4 是消费还是丢弃。
_KIND_NAMES = {
    int(bprepo.ProgressGrantKind.EXP): "grant_exp",
    int(bprepo.ProgressGrantKind.INSTANCE): "grant_instance",
    int(bprepo.ProgressGrantKind.STACK): "grant_stack",
    int(bprepo.ProgressGrantKind.CONSUME_STACK): "consume_stack",
    int(bprepo.ProgressGrantKind.DISCARD_STACK): "discard_stack",
}

_ACTION_STATUS_NAMES = {
    int(bprepo.ProgressActionStatus.PENDING): "pending",
    int(bprepo.ProgressActionStatus.SUCCEEDED): "succeeded",
    int(bprepo.ProgressActionStatus.FAILED): "failed",
}

# 局内 action 失败后**仍可重试**的下游错误码(对应 Go 的 isRetryableProgressActionError)。
# 不在此集合内的码是 inventory 的**确定拒绝**(道具不足等),必须落 FAILED 终态 ——
# 把确定拒绝当瞬时错误重试,会让出箱行永远退避、玩家的 claim 永远不释放。
_RETRYABLE_ACTION_CODES = frozenset(
    {
        errcode.ErrUnknown,
        errcode.ErrInternal,
        errcode.ErrTimeout,
        errcode.ErrUnavailable,
        errcode.ErrRateLimited,
        errcode.ErrCanceled,
        errcode.ErrServiceDisabled,
        errcode.ErrInventoryLockFailed,
    }
)


def progress_kind_name(kind: int) -> str:
    """把出箱类型渲染成稳定名字。对应 Go 的 progressKindName。"""
    return _KIND_NAMES.get(int(kind), "unknown")


def progress_action_status_name(status: int) -> str:
    """把 durable action 终态渲染成稳定名字。对应 Go 的 progressActionStatusName。"""
    return _ACTION_STATUS_NAMES.get(int(status), "unknown")


def progress_idempotency_key(match_id: int, seq: int, player_id: int, kind: str) -> str:
    """`progress:{match_id}:{seq}:{player_id}:{kind}`。对应 Go 的 progressIdempotencyKey。

    与 realtime-progression.md §3 / player.proto AddExperienceRequest 注释同一口径。
    ⚠️ 键的构成是**跨服务契约**:mission 的 `mission_fact_receipts`、player 的
    `exp_history`、inventory 的流水都按它去重。改一个字符 = 全部历史重放变成新事实。
    """
    return f"progress:{match_id}:{seq}:{player_id}:{kind}"


def is_retryable_progress_action_error(code: int) -> bool:
    """对应 Go 的 isRetryableProgressActionError。"""
    return code in _RETRYABLE_ACTION_CODES


class ProgressActionTerminalError(errcode.PandoraError):
    """isolated consume/discard 的 **durable terminal failure**。

    Go 在该分支写的是 `return req.Seq, errcode.New(ErrInvalidArg, ...)` ——
    失败了但 acked_seq 仍然有意义:服务端**已经明确处理过这个 seq**,
    UE 靠它释放 action claim 并保留本地物品。Python 用异常传播时必须把这个值
    显式挂在异常上(errcode.py 的「(value, err) 通则」),否则 acked_seq 静默回 0 ——
    那是「瞬时失败,原样重试」的语义,DS 会拿同一个确定拒绝无限重试。

    声明 slots 而不是随手 setattr:后者能写进去(Exception 自带 __dict__)
    但拼错一个字母不会报错,读的那侧只会永远拿到 0。
    """

    __slots__ = ("acked_seq",)

    def __init__(
        self, acked_seq: int, code: int, msg: str = "", *args: object
    ) -> None:
        super().__init__(code, msg, *args)
        self.acked_seq = int(acked_seq)


def progress_acked_seq(err: BaseException | None) -> int:
    """从异常取回 acked_seq;普通失败恒为 0(对应 Go progressResponse 的 acked)。"""
    if isinstance(err, ProgressActionTerminalError):
        return err.acked_seq
    return 0


def apply_exp_share(total: int, share_permille: int) -> int:
    """按千分比权重切一份经验,向下取整。对应 Go 的 applyExpShare。

    拆成商余两段算(等价 `total*share/1000`),Go 侧这么写是防 uint64 溢出;
    Python 整数无上界,但**保持同一算式**才能保证两栈逐位相同的取整结果 ——
    直接写 `total*share//1000` 在余项上会与 Go 出现 ±1 的差,那是玩家看得见的经验差。

    向下取整意味着小额经验 × 小权重可能归零 —— 归零的份额不产出箱行(0 额度会被
    player 拒收),这是「均分」档刻意接受的舍入损失,不是丢账。
    """
    if share_permille >= EXP_SHARE_PERMILLE_FULL:
        return total
    return (
        total // EXP_SHARE_PERMILLE_FULL * share_permille
        + total % EXP_SHARE_PERMILLE_FULL * share_permille // EXP_SHARE_PERMILLE_FULL
    )


def battle_item_reject_reason(
    item_id: int, definition, consumable: bool  # noqa: ANN001
) -> str:
    """把局内 consume / discard 的「不是合法战斗道具」判定拆成枚举 reason。

    对应 Go 的 battleItemRejectReason(§11.3 R2:一个 if 收敛的 N 个条件必须拆成
    N 个 reason)。`consumable=True` 走消费口径(要求 battle_usable),
    False 走丢弃口径(要求 droppable)。只作日志判据,**不参与控制流**。
    """
    if item_id == 0:
        return "zero_item_config_id"
    if definition is None:
        return "not_configured"
    if definition.equipment:
        return "equipment"
    if consumable and not definition.battle_usable:
        return "not_battle_usable"
    if not consumable and not definition.droppable:
        return "not_droppable"
    return "unknown"


class ProgressActionRequest:
    """一个独占整批的局内 action 请求。对应 Go 的 progressActionRequest。"""

    __slots__ = ("seq", "player_id", "kind", "item_config_id", "count")

    def __init__(  # noqa: PLR0913
        self, seq: int, player_id: int, kind: int, item_config_id: int, count: int
    ) -> None:
        self.seq = seq
        self.player_id = player_id
        self.kind = kind
        self.item_config_id = item_config_id
        self.count = count


def isolated_progress_action(
    events: "list[battle_pb2.BattleProgressEvent]",
) -> tuple[ProgressActionRequest | None, str, BaseException | None]:
    """强制 consume/discard 独占 ReportProgress 批。对应 Go 的 isolatedProgressAction。

    这样 action 的业务失败不会让同一批已接受的 pickup claim 被 UE 整批释放,
    也不存在一个 action 被拆成多个下游事务后部分成功的问题。

    返回 (action, reason, err):reason 只供调用方打日志(§11.3 R2 —— 两个拒绝条件
    「批型不合法」与「count 越硬上限」历史上都零日志,DS 侧只看到整批 InvalidArg)。
    err 非 None 时调用方必定立即返回,不读 action 的业务字段。
    """
    action: ProgressActionRequest | None = None
    for e in events:
        which = e.WhichOneof("fact")
        candidate: ProgressActionRequest | None = None
        if which == "item_consume":
            candidate = ProgressActionRequest(
                seq=e.seq,
                player_id=e.player_id,
                kind=int(bprepo.ProgressGrantKind.CONSUME_STACK),
                item_config_id=e.item_consume.item_config_id,
                count=e.item_consume.count,
            )
        elif which == "item_discard":
            candidate = ProgressActionRequest(
                seq=e.seq,
                player_id=e.player_id,
                kind=int(bprepo.ProgressGrantKind.DISCARD_STACK),
                item_config_id=e.item_discard.item_config_id,
                count=e.item_discard.count,
            )
        if candidate is not None:
            if action is not None or len(events) != 1:
                return (
                    candidate,
                    "action_not_isolated",
                    errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "consume/discard must be exactly one fact in an isolated "
                        "ReportProgress batch",
                    ),
                )
            action = candidate
    if action is not None and (
        action.count == 0 or action.count > MAX_BATTLE_ITEM_ACTION_COUNT_HARD
    ):
        return (
            action,
            "action_count_out_of_hard_range",
            errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle item action count %d out of hard range (max %d)",
                action.count,
                MAX_BATTLE_ITEM_ACTION_COUNT_HARD,
            ),
        )
    return action, "", None


class _BatchState:
    """一批事实展开过程中的累加器。

    单独成类(而不是散落的局部变量)是因为 `report_progress` 与 Go 一样是线性长函数,
    把累加器聚在一起后,「哪些量参与上限判定」一眼可数。
    """

    __slots__ = (
        "exp_by_player",
        "items_by_player",
        "kills_by_player",
        "item_rows",
        "mission_rows",
        "batch_exp",
        "batch_items",
        "skipped_facts",
        "unconfigured_monsters",
        "not_whitelisted_items",
        "sample_monster_id",
        "sample_item_id",
        "skipped_players",
        "sample_skipped_player_id",
    )

    def __init__(self) -> None:
        self.exp_by_player: dict[int, int] = {}
        self.items_by_player: dict[int, int] = {}
        self.kills_by_player: dict[int, int] = {}
        self.item_rows: list[bprepo.ProgressOutboxRecord] = []
        self.mission_rows: list[bprepo.MissionFactRecord] = []
        self.batch_exp = 0
        self.batch_items = 0
        self.skipped_facts = 0
        # 模式 C:漏配 / 非白名单事实按 config_id 去重收集(distinct 计数 + 样例 ID),
        # 批末汇总一条 —— 逐条打 Warn 会让一张漏配的经验表按
        # 「该怪每次击杀 × 每个玩家 × 每场对局」刷屏。
        self.unconfigured_monsters: set[int] = set()
        self.not_whitelisted_items: set[int] = set()
        self.sample_monster_id = 0
        self.sample_item_id = 0
        # 受影响玩家同样去重收集:DS 回调面没有玩家 JWT,日志不会自动带 player_id,
        # 少了它「同一局里只有我没加经验」定位不到人。
        self.skipped_players: set[int] = set()
        self.sample_skipped_player_id = 0

    def note_skipped_player(self, player_id: int) -> None:
        if player_id in self.skipped_players:
            return
        self.skipped_players.add(player_id)
        if self.sample_skipped_player_id == 0:
            self.sample_skipped_player_id = player_id


class ProgressMixin:
    """实时进度通道 + 任务事实转发。由 `BattleResultUsecase` 继承。

    依赖宿主提供:`_repo` / `_cfg` / `_granter` / `_mail_sender` / `_monster_exp` /
    `battle_item_definition()`。
    """

    __slots__ = ()

    # ── setter(与 Go 一致用 setter 注入,避免构造签名被迫改)────────────────

    def set_experience_granter(self, granter) -> None:  # noqa: ANN001
        """player 经验入账器。

        None / 不调 = player_addr 未配 → 经验出箱行**积压不丢**(地址配好重启补发,
        与掉落 granter 同语义)。
        """
        self._exp_granter = granter

    def set_mission_reporter(self, reporter) -> None:  # noqa: ANN001
        """任务事实转发器。

        **弱依赖**:None = mission_addr 未配 → ReportProgress 根本**不产生**任务出箱行。
        产生了却投不出去只会让出箱表无界堆积(§9.24),所以"是否转发"由本字段决定,
        而不是"照产不投"。发布顺序按 §9.21 Go 先行:先上 mission 服务,
        再给 battle_result 配地址开转发。
        """
        self._mission_reporter = reporter

    # ── ReportProgress ────────────────────────────────────────────────────

    async def report_progress(  # noqa: C901, PLR0911, PLR0912, PLR0915 —— 与 Go 同为线性长函数
        self,
        match_id: int,
        roster: list[int] | None,
        events: "list[battle_pb2.BattleProgressEvent]",
    ) -> int:
        """处理 DS 的一批进度事实事件,返回已应用水位 acked_seq。

        `roster` 是凭据检查器从权威 BattleStorageRecord 取的本场玩家名单(service 层注入);
        None = dev / guard off 模式,跳过成员校验(生产 authority_mode=redis 恒非 None)。
        """
        logger = plog.get()
        if match_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "match_id required")
        if not events:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "events required")
        max_batch = self._cfg.max_progress_batch_or_default()
        if len(events) > max_batch:
            # 批量硬上限拒收 = DS 违反上报契约 / 失陷 DS 刷量,与其它 cap 同一监控面。
            logger.error(
                "progress_cap_rejected",
                match_id=match_id,
                kind="batch_size",
                value=len(events),
                cap=max_batch,
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "batch size %d exceeds max %d",
                len(events),
                max_batch,
            )

        roster_set = None if roster is None else set(roster)
        action, action_reason, action_err = isolated_progress_action(events)
        if action_err is not None:
            kv: dict[str, object] = {
                "match_id": match_id,
                "reason": action_reason,
                "events": len(events),
            }
            if action is not None:
                kv.update(
                    seq=action.seq,
                    player_id=action.player_id,
                    kind=progress_kind_name(action.kind),
                    item_config_id=action.item_config_id,
                    count=action.count,
                    hard_max=MAX_BATTLE_ITEM_ACTION_COUNT_HARD,
                )
            kv.update(
                code=errcode.as_code(action_err),
                err=str(action_err),
                hint="consume/discard 必须独占整批且 count 在硬上限内,否则整批拒收",
            )
            logger.warning("progress_batch_rejected", **kv)
            raise action_err
        if action is not None:
            if action.seq == 0 or action.player_id == 0:
                logger.warning(
                    "progress_action_rejected",
                    match_id=match_id,
                    reason="missing_seq_or_player_id",
                    seq=action.seq,
                    player_id=action.player_id,
                    kind=progress_kind_name(action.kind),
                    path="isolated_action",
                )
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "isolated action requires seq and player_id"
                )
            if roster_set is not None and action.player_id not in roster_set:
                # 事件循环里的同一判定已经打了 progress_roster_reject,这条**前置**的
                # isolated action 分支历史上没打 —— 失陷 DS 只要把越权请求做成
                # consume/discard 独占批就能绕开这条安全告警(§9.6 owner 授权 / roster 门)。
                logger.warning(
                    "progress_roster_reject",
                    match_id=match_id,
                    player_id=action.player_id,
                    seq=action.seq,
                    kind=progress_kind_name(action.kind),
                    path="isolated_action",
                )
                raise errcode.PandoraError(
                    errcode.ErrUnauthorized,
                    "player %d not in match %d roster",
                    action.player_id,
                    match_id,
                )

        try:
            wm = await self._repo.get_progress_watermark(match_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 读不到水位 = 本批既不能接受也不能拒绝(DS 会原批重试)。不留证时,
            # 「DS 一直重试上报进度」在本服只表现为零日志。
            logger.warning(
                "progress_watermark_read_failed",
                match_id=match_id,
                events=len(events),
                code=errcode.as_code(exc),
                err=str(exc),
                hint="水位读失败,本批未入账;DS 原批重试",
            )
            raise
        # 已接受 action 的**终态回放优先于** settled/stopped 门。否则 inventory 已完成
        # 但 ReportProgress 回包丢失、随后对局结算时,UE 重试会只收到 InvalidState,
        # 永远无法确定是否应该扣本地 / 施放效果。未曾接受的 action 查不到 outcome,仍会拒。
        if action is not None and action.seq <= wm.last_applied_seq:
            return await self._complete_progress_action(match_id, action)
        if wm.settled:
            # 结算后仍收到进度 = 僵尸 / 分区恢复 DS 迟到上报(§9.22 / §9.4 fencing);
            # ErrInvalidState 是业务码不被 access log 当故障 → 显式 WARN 留证。
            logger.warning(
                "progress_rejected_settled", match_id=match_id, events=len(events)
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "match %d already settled, progress rejected",
                match_id,
            )
        if wm.stopped:
            logger.warning(
                "progress_rejected_stream_stopped",
                match_id=match_id,
                events=len(events),
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "match %d progress stream permanently stopped, rejected",
                match_id,
            )
        if not wm.existed and not self._cfg.progress_enabled:
            wm = await self._claim_legacy_or_rejoin(match_id, events)

        last_seq = wm.last_applied_seq
        state, new_seq = await self._expand_events(
            match_id, events, roster_set, last_seq
        )

        # 漏配 / 非白名单汇总(模式 C):按 config_id 去重后每批至多一条。
        if state.unconfigured_monsters or state.not_whitelisted_items:
            logger.warning(
                "progress_facts_skipped",
                match_id=match_id,
                skipped_facts=state.skipped_facts,
                affected_player_ids=len(state.skipped_players),
                sample_player_id=state.sample_skipped_player_id,
                unconfigured_monster_ids=len(state.unconfigured_monsters),
                sample_monster_config_id=state.sample_monster_id,
                not_whitelisted_item_ids=len(state.not_whitelisted_items),
                sample_item_config_id=state.sample_item_id,
                hint="怪物经验表漏配 / 掉落白名单漏项,或失陷 DS 上报未授权事实"
                "(该玩家这几笔经验/掉落永久不发)",
            )

        if new_seq == last_seq:
            # 普通旧事件是纯重放;action 必须从 durable outcome 稳定回放,
            # **不能**从"outbox 已不存在"猜成功。
            if action is not None:
                return await self._complete_progress_action(match_id, action)
            return last_seq

        await self._apply_batch(match_id, last_seq, new_seq, state, len(events))
        logger.debug(
            "battle_progress_applied",
            match_id=match_id,
            acked_seq=new_seq,
            events=len(events),
            grant_rows=len(state.item_rows) + len(state.exp_by_player),
            skipped_facts=state.skipped_facts,
            batch_exp=state.batch_exp,
            batch_items=state.batch_items,
        )
        if action is not None:
            return await self._complete_progress_action(match_id, action)
        return new_seq

    async def _claim_legacy_or_rejoin(
        self, match_id: int, events
    ) -> bprepo.ProgressWatermark:  # noqa: ANN001
        """通道关闭且本场尚未开流:固化 legacy 结算模式,或让给并发开流副本。

        每场模式以**水位行存在性**固化(§9.22 单一权威):行已存在 = 本场发放权已归
        实时通道,killswitch 中途关闭不影响进行中对局(否则"部分实时 + 结算掉落被
        整体抑制"会丢奖);行不存在时由开关决定能否开流。

        认领必须是"无行才创建"(审计 R4 #11):滚动混版下开启副本可能在本副本读水位
        之后、认领之前已创建行开流,upsert 会把那条**合法已开的流**停掉。
        """
        logger = plog.get()
        try:
            claimed = await self._repo.claim_progress_legacy(match_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error(
                "progress_claim_legacy_failed_retryable", match_id=match_id, err=str(exc)
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "progress channel disabled and legacy claim persist failed, retry: %s",
                exc,
            ) from exc
        if claimed:
            # 一次**不可逆的每局模式定格**:本局此后永远走结算发放,中途开 killswitch
            # 也不生效。§11.3 R1:不可逆状态推进打 INFO。
            logger.info(
                "progress_legacy_claimed",
                match_id=match_id,
                events=len(events),
                first_seq=events[0].seq,
                reason="progress_disabled",
                hint="本场固化为 legacy 结算发放模式,实时通道对本局永久关闭",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "realtime progress channel disabled"
            )
        # 竞态输给已存在的行:可能是开启副本已开流、已认领 legacy、或已结算 ——
        # 重读后按与首读**相同的裁决顺序**处理。
        wm = await self._repo.get_progress_watermark(match_id)
        if wm.settled:
            logger.warning(
                "progress_rejected_settled",
                match_id=match_id,
                events=len(events),
                last_seq=wm.last_applied_seq,
                path="legacy_claim_race",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "match %d already settled, progress rejected",
                match_id,
            )
        if wm.stopped:
            logger.warning(
                "progress_rejected_stream_stopped",
                match_id=match_id,
                events=len(events),
                last_seq=wm.last_applied_seq,
                path="legacy_claim_race",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "match %d progress stream permanently stopped, rejected",
                match_id,
            )
        if not wm.existed:
            # INSERT IGNORE 没生效行却不存在(并发删除 / 复制异常):按瞬时态重试收敛。
            logger.error(
                "progress_legacy_claim_raced",
                match_id=match_id,
                events=len(events),
                hint="INSERT IGNORE 未生效且水位行不存在(并发删除/复制异常),"
                "按瞬时态让 DS 重试",
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "progress legacy claim raced, retry"
            )
        logger.warning(
            "progress_disabled_replica_joins_open_stream",
            match_id=match_id,
            last_seq=wm.last_applied_seq,
            hint="通道关闭副本遇到已开流对局(滚动混版/killswitch 中途关闭),"
            "按已开流继续入账",
        )
        return wm

    async def _expand_events(  # noqa: C901, PLR0912, PLR0915 —— 与 Go 同为线性事实展开
        self,
        match_id: int,
        events,  # noqa: ANN001
        roster_set: set[int] | None,
        last_seq: int,
    ) -> tuple[_BatchState, int]:
        """事实换算(**DS 不可信**):怪物经验查配置表,拾取过白名单。

        未知怪 / 非白名单物品**跳过并汇总告警**(只丢该事实的发放,水位照常推进 ——
        坏配置不能把整条流卡死);未知事实**类型**则停流(见 default 分支)。
        """
        logger = plog.get()
        cfg = self._cfg
        max_seq_cap = cfg.max_progress_seq_per_match_or_default()
        max_kill = cfg.max_kill_count_per_fact_or_default()
        max_pickup = cfg.max_pickup_count_per_fact_or_default()
        # 任务事实转发开关:reporter 未注入(mission_addr 未配)时一行不产,
        # 避免出箱无界堆积(§9.24)。
        forward_mission = self._mission_reporter is not None

        st = _BatchState()
        prev_seq = 0
        new_seq = last_seq
        prev_applied_seq = last_seq

        def add_mission_fact(  # noqa: PLR0913
            seq: int,
            player_id: int,
            category: int,
            slot_value: int,
            amount: int,
            pending_action: bool,
        ) -> None:
            if not forward_mission or slot_value == 0 or amount == 0:
                return
            st.mission_rows.append(
                bprepo.MissionFactRecord(
                    match_id=match_id,
                    seq=seq,
                    player_id=player_id,
                    category=category,
                    slot_value=slot_value,
                    amount=amount,
                    pending_action=pending_action,
                )
            )

        for e in events:
            seq = e.seq
            if seq == 0 or seq <= prev_seq:
                logger.warning(
                    "progress_batch_rejected",
                    match_id=match_id,
                    reason="seq_not_ascending",
                    seq=seq,
                    prev_seq=prev_seq,
                    events=len(events),
                )
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "event seq must be ascending (seq=%d prev=%d)",
                    seq,
                    prev_seq,
                )
            prev_seq = seq
            if seq > max_seq_cap:
                logger.error(
                    "progress_cap_rejected",
                    match_id=match_id,
                    kind="seq",
                    seq=seq,
                    cap=max_seq_cap,
                )
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "event seq %d exceeds per-match cap %d",
                    seq,
                    max_seq_cap,
                )
            if seq <= last_seq:
                continue  # 旧事件重放(at-least-once),已入账,跳过
            if seq > prev_applied_seq + 1:
                # seq 跳号合法(DS 有界缓冲满载丢最老事件),但必须留痕:
                # 跳过的 seq 永不再来,是结算对账 gap 告警的先导信号。
                logger.warning(
                    "progress_seq_gap",
                    match_id=match_id,
                    prev_applied=prev_applied_seq,
                    next=seq,
                )
            prev_applied_seq = seq
            new_seq = seq
            player_id = e.player_id
            if player_id == 0:
                logger.warning(
                    "progress_batch_rejected",
                    match_id=match_id,
                    reason="missing_player_id",
                    seq=seq,
                    events=len(events),
                )
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "event %d missing player_id", seq
                )
            if roster_set is not None and player_id not in roster_set:
                # DS 为非本场玩家上报进度事实 = 越权 / 失陷 DS 的直接信号(§9.6)。
                logger.warning(
                    "progress_roster_reject",
                    match_id=match_id,
                    player_id=player_id,
                    seq=seq,
                    path="event",
                )
                raise errcode.PandoraError(
                    errcode.ErrUnauthorized,
                    "player %d not in match %d roster",
                    player_id,
                    match_id,
                )

            which = e.WhichOneof("fact")
            if which == "monster_kill":
                self._expand_monster_kill(
                    match_id, e, st, max_kill, add_mission_fact
                )
            elif which == "item_pickup":
                self._expand_item_pickup(
                    match_id, e, st, max_pickup, add_mission_fact
                )
            elif which == "item_consume":
                self._expand_item_consume(match_id, e, st, add_mission_fact)
            elif which == "item_discard":
                self._expand_item_discard(match_id, e, st)
            else:
                await self._stop_stream_on_unknown_fact(match_id, seq)
        return st, new_seq

    def _expand_monster_kill(  # noqa: ANN001, C901, PLR0913
        self, match_id: int, e, st: _BatchState, max_kill: int, add_mission_fact
    ) -> None:
        logger = plog.get()
        seq, player_id = e.seq, e.player_id
        fact = e.monster_kill
        cnt = fact.count
        if cnt == 0 or cnt > max_kill:
            # 兄弟闸门(seq 硬上限 / 累计上限)都以 progress_cap_rejected 打 Error;
            # 失陷 DS 刷单条巨量击杀是最直接的作弊形态,不能静默。
            logger.error(
                "progress_cap_rejected",
                match_id=match_id,
                player_id=player_id,
                seq=seq,
                kind="kill_count",
                value=cnt,
                cap=max_kill,
                monster_config_id=fact.monster_config_id,
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "kill count %d out of range (max %d)", cnt, max_kill
            )
        # 击杀计数在经验换算前累计:未配置经验的怪也计入单玩家击杀上限,
        # 失陷 DS 不能靠刷未知怪 ID 绕过反作弊额度。
        st.kills_by_player[player_id] = st.kills_by_player.get(player_id, 0) + cnt
        # 任务事实同样在经验换算前收集:「这只怪被杀了」与「这只怪配没配经验」是两件事,
        # role_level 漏配不该让杀怪类任务跟着不计数。
        add_mission_fact(
            seq, player_id, MISSION_CATEGORY_KILL_MONSTER, fact.monster_config_id, cnt, False
        )
        # 归属权重(千分比):0 / 未设置 = 整份。>1000 = 坏批(单条事实拿到超过一整份)。
        share = fact.share_permille or EXP_SHARE_PERMILLE_FULL
        if share > EXP_SHARE_PERMILLE_FULL:
            logger.error(
                "progress_cap_rejected",
                match_id=match_id,
                player_id=player_id,
                seq=seq,
                kind="share_permille",
                value=share,
                cap=EXP_SHARE_PERMILLE_FULL,
                hint="单条事实拿到超过一整份归属权重 = 坏批 / 失陷 DS",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "share_permille %d exceeds %d (seq=%d)",
                share,
                EXP_SHARE_PERMILLE_FULL,
                seq,
            )
        # 表未注入时**不能**走"跳过并推进水位"那条路 —— 水位一推进,这批击杀的经验
        # 就永久没有补救路径了(DS 重发会被 seq<=last_seq 跳过)。按可重试错误整批退回。
        if self._monster_exp is None:
            logger.error(
                "monster_exp_table_unavailable",
                match_id=match_id,
                seq=seq,
                hint="configtable role_level 未注入;经验无法换算,整批退回等待重试"
                "(绝不推进水位)",
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "monster exp table unavailable, retry batch (seq=%d)",
                seq,
            )
        monster_id = fact.monster_config_id
        exp_per, configured = self._monster_exp.kill_exp_of(
            monster_id, fact.monster_level
        )
        if not configured:
            # 表里没有这一 (角色, 等级) 行 = 配置漏项。只丢该事实的发放,水位照常推进:
            # 坏配置不能把整条流卡死(与非白名单拾取同一纪律)。
            # ★ 必须与「有行但经验配 0」区分:后者是策划有意不给经验(照常推进,
            #   只是不产出箱行),不该进漏配告警。
            st.skipped_facts += 1
            st.note_skipped_player(player_id)
            if monster_id not in st.unconfigured_monsters:
                st.unconfigured_monsters.add(monster_id)
                if st.sample_monster_id == 0:
                    st.sample_monster_id = monster_id
            return
        gained = apply_exp_share(exp_per * cnt, share)
        st.exp_by_player[player_id] = st.exp_by_player.get(player_id, 0) + gained
        st.batch_exp += gained

    def _expand_item_pickup(  # noqa: ANN001, PLR0913
        self, match_id: int, e, st: _BatchState, max_pickup: int, add_mission_fact
    ) -> None:
        logger = plog.get()
        seq, player_id = e.seq, e.player_id
        fact = e.item_pickup
        cnt = fact.count
        if cnt == 0 or cnt > max_pickup:
            logger.error(
                "progress_cap_rejected",
                match_id=match_id,
                player_id=player_id,
                seq=seq,
                kind="pickup_count",
                value=cnt,
                cap=max_pickup,
                item_config_id=fact.item_config_id,
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "pickup count %d out of range (max %d)",
                cnt,
                max_pickup,
            )
        item_id = fact.item_config_id
        definition = self.battle_item_definition(item_id)
        if item_id == 0 or definition is None or not definition.droppable:
            st.skipped_facts += 1
            st.note_skipped_player(player_id)
            if item_id not in st.not_whitelisted_items:
                st.not_whitelisted_items.add(item_id)
                if st.sample_item_id == 0:
                    st.sample_item_id = item_id
            return
        # 每拾取事实一行出箱(seq = 事实自身 seq,uk 天然唯一):单事实 count 已被夹紧到
        # CSV 列宽内,合法掉落永不截断(审计 P1;拾取低频,行数有界)。
        kind = (
            int(bprepo.ProgressGrantKind.INSTANCE)
            if definition.equipment
            else int(bprepo.ProgressGrantKind.STACK)
        )
        st.item_rows.append(
            bprepo.ProgressOutboxRecord(
                match_id=match_id,
                seq=seq,
                player_id=player_id,
                kind=kind,
                item_config_ids=[item_id] * cnt,
            )
        )
        st.batch_items += cnt
        st.items_by_player[player_id] = st.items_by_player.get(player_id, 0) + cnt
        # 拾取类任务事实只收白名单内的:非白名单拾取本就被判为可疑事实(上面已跳过发放),
        # 放它推进任务进度等于另开一条绕过白名单的计数通道。
        # 不挂 pending 闸:拾取没有 battle_progress_action 结果行可等,
        # 且"捡到了"本身就是 DS 记录的事实,发放失败属投递问题(满包转邮件 / 退避重试)。
        add_mission_fact(seq, player_id, MISSION_CATEGORY_PICKUP_ITEM, item_id, cnt, False)

    def _expand_item_consume(  # noqa: ANN001
        self, match_id: int, e, st: _BatchState, add_mission_fact
    ) -> None:
        logger = plog.get()
        seq, player_id = e.seq, e.player_id
        fact = e.item_consume
        cnt, item_id = fact.count, fact.item_config_id
        definition = self.battle_item_definition(item_id)
        if (
            item_id == 0
            or definition is None
            or definition.equipment
            or not definition.battle_usable
        ):
            # **不能跳过并 ACK**:DS 会据 ACK 最终扣本地并应用 GAS,后端若未留下消费出箱
            # 会让资产重登复活。坏事实必须整批明确拒绝。
            logger.warning(
                "progress_item_action_rejected",
                match_id=match_id,
                player_id=player_id,
                seq=seq,
                kind="consume",
                item_config_id=item_id,
                count=cnt,
                reason=battle_item_reject_reason(item_id, definition, True),
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "item %d is not configured battle consumable",
                item_id,
            )
        if (
            cnt == 0
            or definition.max_stack == 0
            or cnt > definition.max_stack
            or cnt > MAX_BATTLE_ITEM_ACTION_COUNT_HARD
        ):
            logger.warning(
                "progress_item_action_rejected",
                match_id=match_id,
                player_id=player_id,
                seq=seq,
                kind="consume",
                item_config_id=item_id,
                count=cnt,
                max_stack=definition.max_stack,
                hard_max=MAX_BATTLE_ITEM_ACTION_COUNT_HARD,
                reason="count_out_of_range",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "consume count %d out of range for item %d (max_stack %d, hard max %d)",
                cnt,
                item_id,
                definition.max_stack,
                MAX_BATTLE_ITEM_ACTION_COUNT_HARD,
            )
        st.item_rows.append(
            bprepo.ProgressOutboxRecord(
                match_id=match_id,
                seq=seq,
                player_id=player_id,
                kind=int(bprepo.ProgressGrantKind.CONSUME_STACK),
                item_config_ids=[item_id],
                item_count=cnt,
            )
        )
        # 「使用道具」类任务事实。丢弃(item_discard)**刻意不转发**:扔掉不是用掉,
        # 否则「使用 N 个 X」型任务能靠捡了再扔刷完。
        # pending_action=True:这一行在扣除落定前不可投递 —— 局内消费可能以业务失败终态
        # 收场(道具不足等),此时 inventory 一件没扣、UE 也保留本地物品,
        # 事实照发就等于让"上报根本没发生的消耗"刷完任务(§9.6 不信 DS)。
        add_mission_fact(seq, player_id, MISSION_CATEGORY_USE_ITEM, item_id, cnt, True)

    def _expand_item_discard(self, match_id: int, e, st: _BatchState) -> None:  # noqa: ANN001
        logger = plog.get()
        seq, player_id = e.seq, e.player_id
        fact = e.item_discard
        cnt, item_id = fact.count, fact.item_config_id
        definition = self.battle_item_definition(item_id)
        if (
            item_id == 0
            or definition is None
            or definition.equipment
            or not definition.droppable
        ):
            logger.warning(
                "progress_item_action_rejected",
                match_id=match_id,
                player_id=player_id,
                seq=seq,
                kind="discard",
                item_config_id=item_id,
                count=cnt,
                reason=battle_item_reject_reason(item_id, definition, False),
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle discard only supports configured droppable stackable item: %d",
                item_id,
            )
        if (
            cnt == 0
            or definition.max_stack == 0
            or cnt > definition.max_stack
            or cnt > MAX_BATTLE_ITEM_ACTION_COUNT_HARD
        ):
            logger.warning(
                "progress_item_action_rejected",
                match_id=match_id,
                player_id=player_id,
                seq=seq,
                kind="discard",
                item_config_id=item_id,
                count=cnt,
                max_stack=definition.max_stack,
                hard_max=MAX_BATTLE_ITEM_ACTION_COUNT_HARD,
                reason="count_out_of_range",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "discard count %d out of range for item %d (max_stack %d, hard max %d)",
                cnt,
                item_id,
                definition.max_stack,
                MAX_BATTLE_ITEM_ACTION_COUNT_HARD,
            )
        st.item_rows.append(
            bprepo.ProgressOutboxRecord(
                match_id=match_id,
                seq=seq,
                player_id=player_id,
                kind=int(bprepo.ProgressGrantKind.DISCARD_STACK),
                item_config_ids=[item_id],
                item_count=cnt,
            )
        )

    async def _stop_stream_on_unknown_fact(self, match_id: int, seq: int) -> None:
        """未知事实类型 = 能力不匹配(新 DS 对旧 Go),**整场停流**。

        ErrInvalidArg 的"丢批继续"语义会让 DS 逐批丢弃所有含新事实的批(永久丢失,
        审计 P1),所以改用 ErrInvalidState → DS 停流。

        停流标记持久化:防违纪 DS 用后续"只含已知事实"的批重新开流。标记失败必须
        **保持可重试**(吞掉失败直接返回终态 InvalidState 会让 DS 永久停流而库里
        没有标记,后续已知批仍可能被接受)—— 返回 ErrUnavailable,DS 原批重试 →
        再次命中未知事实 → 重试落标记,收敛后才返回停流终态。

        「已停流」日志只在标记成功后打(审计 R4 P2:先打日志再落标记,标记失败时
        日志与库状态矛盾,排障会按已停流处理)。
        """
        logger = plog.get()
        try:
            await self._repo.mark_progress_stopped(match_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error(
                "progress_mark_stopped_failed_retryable", match_id=match_id, err=str(exc)
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "unknown progress fact seq=%d and stop marker persist failed, "
                "retry batch: %s",
                seq,
                exc,
            ) from exc
        logger.error(
            "progress_unknown_fact_stream_stopped",
            match_id=match_id,
            seq=seq,
            hint="新 DS 事实类型早于 battle_result 升级放量(违反 Go 先行纪律),"
            "本场已停流,停流后实时奖励永久丢失",
        )
        raise errcode.PandoraError(
            errcode.ErrInvalidState,
            "unknown progress fact seq=%d: upgrade battle_result fleet before enabling "
            "new DS fact types (stream stopped; remaining realtime rewards for this "
            "match are permanently lost)",
            seq,
        )

    async def _apply_batch(
        self, match_id: int, last_seq: int, new_seq: int, st: _BatchState, events: int
    ) -> None:
        """聚合本批 delta 并交给 repo 的一致事务落地。

        单场 / 单玩家累计上限统一在 `apply_progress` **事务内**判定(审计 P1):
        此处若按事务外读到的水位 / player totals 先判,与水位 CAS 分属不同快照 ——
        重试请求可能读到旧水位 + 首请求已提交的新累计,把同批 delta 重复计入后永久误拒,
        而 DS 据契约丢批并释放拾取认领,首请求出箱却已提交 → 重新拾取可重复发放。
        """
        logger = plog.get()
        # 触达玩家 = 击杀 ∪ 拾取(经验只源自击杀,exp 键集 ⊆ kills 键集)。
        touched = set(st.kills_by_player) | set(st.items_by_player)
        # 按 player_id 升序(Go 显式 sort):顺序决定事务内 upsert 的加锁次序,
        # 两栈不一致会在并发批次之间制造死锁窗口。
        player_deltas = [
            bprepo.ProgressPlayerDelta(
                player_id=pid,
                exp=st.exp_by_player.get(pid, 0),
                items=st.items_by_player.get(pid, 0),
                kills=st.kills_by_player.get(pid, 0),
            )
            for pid in sorted(touched)
        ]
        cfg = self._cfg
        caps = bprepo.ProgressCaps(
            match_exp=cfg.max_progress_exp_per_match_or_default(),
            match_items=cfg.max_progress_items_per_match_or_default(),
            player_exp=cfg.max_progress_exp_per_player_or_default(),
            player_items=cfg.max_progress_items_per_player_or_default(),
            player_kills=cfg.max_progress_kills_per_player_or_default(),
        )
        rows: list[bprepo.ProgressOutboxRecord] = []
        for player_id in sorted(st.exp_by_player):
            exp = st.exp_by_player[player_id]
            if exp == 0:
                # monster_exp 显式配 0(无经验怪):不产生 0 额度出箱行
                # (player 拒收 0 会让这行永久重试)。
                continue
            rows.append(
                bprepo.ProgressOutboxRecord(
                    match_id=match_id,
                    seq=new_seq,
                    player_id=player_id,
                    kind=int(bprepo.ProgressGrantKind.EXP),
                    exp_delta=exp,
                )
            )
        rows.extend(st.item_rows)

        try:
            await self._repo.apply_progress(
                match_id,
                last_seq,
                new_seq,
                st.batch_exp,
                st.batch_items,
                player_deltas,
                rows,
                st.mission_rows,
                caps,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            if errcode.as_code(exc) == errcode.ErrInvalidArg:
                # 累计上限拒收告警(契约:拒收**并告警**,审计 P2):这是失陷 DS 刷产出的
                # 第一现场信号。biz 层的 InvalidArg 前置校验都在 apply_progress 之前,
                # 此处 InvalidArg 只来自事务内单场 / 单玩家累计上限。
                logger.error(
                    "progress_cap_rejected",
                    match_id=match_id,
                    batch_exp=st.batch_exp,
                    batch_items=st.batch_items,
                    players=len(player_deltas),
                    err=str(exc),
                )
            else:
                logger.warning(
                    "progress_apply_failed",
                    match_id=match_id,
                    expected_seq=last_seq,
                    new_seq=new_seq,
                    events=events,
                    grant_rows=len(rows),
                    mission_rows=len(st.mission_rows),
                    batch_exp=st.batch_exp,
                    batch_items=st.batch_items,
                    players=len(player_deltas),
                    code=errcode.as_code(exc),
                    err=str(exc),
                    hint="水位 CAS 失败 / 已结算 / DB 瞬时错误,本批整体回滚;DS 原批重试",
                )
            raise

    # ── 同步 action 完成路径 ──────────────────────────────────────────────

    async def _complete_progress_action(  # noqa: C901, PLR0912
        self, match_id: int, req: ProgressActionRequest
    ) -> int:
        """同步驱动该玩家到 action seq 为止的出箱,并**只从 durable outcome 回放终态**。

        对应 Go 的 completeProgressAction。普通 pickup/exp 仍是异步 ACK;
        只有 isolated consume/discard 走这里等待 inventory 事务明确完成。

        ★ 循环是**有界**的:每轮要么读到终态返回,要么真的推进一条出箱行
          (processs 成功 → outcome 落 SUCCEEDED/FAILED;失败 → defer 后返回)。
          `pending_without_outbox` 也直接返回可重试错误,不原地空转 ——
          不存在"等一会儿再看看"式的定时器掩盖(§16.10)。
        """
        logger = plog.get()
        while True:
            action = await self._get_action_or_raise(match_id, req, "outcome_read")
            if action is None:
                logger.warning(
                    "progress_action_rejected",
                    match_id=match_id,
                    seq=req.seq,
                    player_id=req.player_id,
                    kind=progress_kind_name(req.kind),
                    item_config_id=req.item_config_id,
                    count=req.count,
                    reason="outcome_row_missing",
                    hint="该 seq 从未被接受过(重放/伪造),或 battle_progress_action 行被清理",
                )
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "progress action outcome missing match=%d seq=%d player=%d kind=%d",
                    match_id,
                    req.seq,
                    req.player_id,
                    req.kind,
                )
            if action.item_config_id != req.item_config_id or action.count != req.count:
                logger.warning(
                    "progress_action_rejected",
                    match_id=match_id,
                    seq=req.seq,
                    player_id=req.player_id,
                    kind=progress_kind_name(req.kind),
                    reason="seq_payload_mismatch",
                    stored_item_config_id=action.item_config_id,
                    stored_count=action.count,
                    item_config_id=req.item_config_id,
                    count=req.count,
                    hint="同一 seq 被换了载荷重放 = DS 违约 / 失陷信号",
                )
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "progress action seq reused with different payload match=%d seq=%d "
                    "stored=%d:%d request=%d:%d",
                    match_id,
                    req.seq,
                    action.item_config_id,
                    action.count,
                    req.item_config_id,
                    req.count,
                )
            if action.status == int(bprepo.ProgressActionStatus.SUCCEEDED):
                # 局内消费 / 丢弃的**不可逆终态**(inventory 已实扣)。每玩家每 seq
                # 至多一条,不是高频路径 —— 不打的话「战斗里用了药到底扣没扣」
                # 只能查 battle_progress_action 表。
                logger.info(
                    "progress_action_completed",
                    match_id=match_id,
                    seq=req.seq,
                    player_id=req.player_id,
                    kind=progress_kind_name(req.kind),
                    item_config_id=req.item_config_id,
                    count=req.count,
                    status="succeeded",
                )
                return req.seq
            if action.status == int(bprepo.ProgressActionStatus.FAILED):
                # UE 的 mutation claim 只需区分"确定拒绝"与"可重试"。内部保留 inventory
                # 原始 result_code 供审计;线协议统一映射 InvalidArg,避免客户端把陌生的
                # 701x 业务码误当瞬时失败无限重试。
                logger.warning(
                    "progress_action_terminal_failed",
                    match_id=match_id,
                    seq=req.seq,
                    player_id=req.player_id,
                    kind=progress_kind_name(req.kind),
                    item_config_id=req.item_config_id,
                    count=req.count,
                    inventory_code=action.result_code,
                    hint="inventory 明确拒绝(道具不足等),一件没扣;UE 保留本地物品并释放 claim",
                )
                raise ProgressActionTerminalError(
                    req.seq,
                    errcode.ErrInvalidArg,
                    "battle item action terminally failed match=%d seq=%d player=%d "
                    "item=%d count=%d inventory_code=%d",
                    match_id,
                    req.seq,
                    req.player_id,
                    req.item_config_id,
                    req.count,
                    action.result_code,
                )

            row = await self._fetch_action_outbox_or_raise(match_id, req)
            if row is None:
                # 出箱缺失**绝不是成功的证据**。并发 worker 可能刚 resolve 完,
                # 所以重读一次 outcome;仍是 pending 则是不变量破坏,按可重试返回、绝不 ACK。
                action = await self._get_action_or_raise(match_id, req, "outcome_reread")
                if action is not None and action.status != int(
                    bprepo.ProgressActionStatus.PENDING
                ):
                    continue
                logger.warning(
                    "progress_action_rejected",
                    match_id=match_id,
                    seq=req.seq,
                    player_id=req.player_id,
                    kind=progress_kind_name(req.kind),
                    reason="pending_without_outbox",
                    hint="pending action 找不到出箱行(不变量破坏),按可重试返回,绝不 ACK",
                )
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "pending progress action has no outbox match=%d seq=%d; retry",
                    match_id,
                    req.seq,
                )
            try:
                await self.process_progress_record(row)
            except asyncio.CancelledError:
                raise
            except BaseException:
                await self._defer_progress_row(row.id)
                raise

    async def _get_action_or_raise(
        self, match_id: int, req: ProgressActionRequest, path: str
    ) -> bprepo.ProgressAction | None:
        try:
            return await self._repo.get_progress_action(
                match_id, req.seq, req.player_id, req.kind
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 读不到 durable outcome:UE 拿不到确定结果,会同 seq 同 payload 重试。
            plog.get().warning(
                "progress_action_lookup_failed",
                match_id=match_id,
                seq=req.seq,
                player_id=req.player_id,
                kind=progress_kind_name(req.kind),
                path=path,
                code=errcode.as_code(exc),
                err=str(exc),
            )
            raise

    async def _fetch_action_outbox_or_raise(
        self, match_id: int, req: ProgressActionRequest
    ) -> bprepo.ProgressOutboxRecord | None:
        try:
            return await self._repo.fetch_progress_outbox_for_player(
                match_id, req.player_id, req.seq
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "progress_action_lookup_failed",
                match_id=match_id,
                seq=req.seq,
                player_id=req.player_id,
                kind=progress_kind_name(req.kind),
                path="outbox_read",
                code=errcode.as_code(exc),
                err=str(exc),
            )
            raise

    async def _defer_progress_row(self, outbox_id: int) -> None:
        """推迟一条发放失败的出箱行。

        推迟失败本身只告警:下轮 fetch 仍会取到该行重试,不影响 at-least-once。
        """
        try:
            await self._repo.defer_progress_outbox(outbox_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "progress_outbox_defer_failed", id=outbox_id, err=str(exc)
            )

    # ── 后台:进度出箱发布器 ──────────────────────────────────────────────

    async def run_progress_publisher(self) -> None:
        """周期发放进度出箱。

        纪律同 run_drop_publisher:单行失败仅记录并 continue(保留出箱行下轮重试),
        不阻塞其他行;下游客户端未注入的行原样跳过积压不丢。
        """
        if self._exp_granter is None and self._granter is None:
            plog.get().info(
                "progress_publisher_disabled",
                hint="player_addr / inventory_addr 均未配置 → 进度出箱积压不丢,配置后重启补发",
            )
            return
        interval = self._cfg.progress_publish_interval_or_default().total_seconds()
        plog.get().info(
            "progress_publisher_started",
            interval=self._cfg.progress_publish_interval,
            batch=self._cfg.progress_batch_size_or_default(),
        )
        try:
            await safego.loop(
                "battle_progress_publisher", interval, self._publish_progress_tick
            )
        finally:
            plog.get().info("progress_publisher_stopped")

    async def _publish_progress_tick(self) -> None:
        try:
            await self.publish_progress_batch()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 准确条数在行级日志上,批级不填假数字。
            plog.get().warning("progress_publish_batch_failed", err=str(exc))

    async def publish_progress_batch(self) -> int:
        """取一批进度出箱行处理,返回本轮成功发放 / 落定 action 终态的条数。"""
        recs = await self._repo.fetch_progress_outbox(
            self._cfg.progress_batch_size_or_default()
        )
        processed = 0
        for r in recs:
            row_started = time.monotonic()
            token = plog.bind_trace_id(str(uuid.uuid4()))
            try:
                try:
                    await self.process_progress_record(r)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    plog.get().warning(
                        "progress_outbox_delivery_failed",
                        id=r.id,
                        match_id=r.match_id,
                        seq=r.seq,
                        player_id=r.player_id,
                        kind=progress_kind_name(r.kind),
                        exp_delta=r.exp_delta,
                        items=len(r.item_config_ids),
                        item_count=r.item_count,
                        elapsed_ms=int((time.monotonic() - row_started) * 1000),
                        code=errcode.as_code(exc),
                        err=str(exc),
                        hint="出箱行退避后下轮重试(at-least-once);"
                        "持续失败查 player / inventory 侧同 trace_id",
                    )
                    await self._defer_progress_row(r.id)
                    continue
                processed += 1
            finally:
                token.var.reset(token)
        if processed > 0:
            plog.get().debug("progress_outbox_resolved", count=processed)
        return processed

    async def process_progress_record(  # noqa: C901, PLR0912
        self, r: bprepo.ProgressOutboxRecord
    ) -> bprepo.ProgressAction | None:
        """后台 publisher 与同步 action 路径**共用的唯一投递语义**。

        action 的终态(成功或业务失败)由 repo 原子写 outcome + 删除 outbox;
        普通行成功后直接删 outbox。返回非 None 表示已持久进入终态。
        """
        from pandorapy.services.battle_result import biz as bbiz  # 避免循环 import

        logger = plog.get()
        kind = int(r.kind)
        idempotency_key = ""
        if kind == int(bprepo.ProgressGrantKind.EXP):
            if self._exp_granter is None:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable, "player_addr not configured"
                )
            idempotency_key = progress_idempotency_key(
                r.match_id, r.seq, r.player_id, "exp"
            )
            await self._exp_granter.add_experience(
                r.player_id, r.exp_delta, "monster_kill", idempotency_key
            )
        elif kind == int(bprepo.ProgressGrantKind.INSTANCE):
            if self._granter is None:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable, "inventory_addr not configured"
                )
            idempotency_key = progress_idempotency_key(
                r.match_id, r.seq, r.player_id, "item"
            )
            await self._grant_instances_or_mail(r, idempotency_key)
        elif kind == int(bprepo.ProgressGrantKind.STACK):
            if self._granter is None:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable, "inventory_addr not configured"
                )
            stacks = bbiz.aggregate_stack_grants(r.item_config_ids)
            idempotency_key = progress_idempotency_key(
                r.match_id, r.seq, r.player_id, "stack"
            )
            # 金币位固定传 0:实时进度通道**只发道具不发金币**(金币是结算路径的收益,
            # 见 biz._build_drop_outbox)。这里传非 0 会让同一局的金币被两条链各发一次。
            await self._granter.grant_items(r.player_id, stacks, 0, idempotency_key)
        elif kind in (
            int(bprepo.ProgressGrantKind.CONSUME_STACK),
            int(bprepo.ProgressGrantKind.DISCARD_STACK),
        ):
            return await self._resolve_action_record(r)
        else:
            logger.error(
                "progress_outbox_unknown_kind",
                outbox_id=r.id,
                match_id=r.match_id,
                seq=r.seq,
                player_id=r.player_id,
                kind=kind,
                hint="出箱行类型本进程不认识(旧副本读到新版本写的行),行保留退避重试",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "unknown progress outbox kind id=%d kind=%d",
                r.id,
                kind,
            )
        await self._repo.delete_progress_outbox(r.id)
        # 落在 DELETE 之后:出箱行删掉才算这笔发放彻底闭环(先打日志再删,
        # 删失败会让日志与库状态矛盾)。每玩家每 seq 至多一条。
        logger.info(
            "progress_grant_delivered",
            match_id=r.match_id,
            seq=r.seq,
            player_id=r.player_id,
            outbox_id=r.id,
            kind=progress_kind_name(r.kind),
            exp_delta=r.exp_delta,
            items=len(r.item_config_ids),
            idempotency_key=idempotency_key,
        )
        return None

    async def _grant_instances_or_mail(
        self, r: bprepo.ProgressOutboxRecord, key: str
    ) -> None:
        """装备实例发放;**只有背包满**才转邮件(同幂等键,直发链与邮件链至多一次)。"""
        try:
            await self._granter.grant_instances(r.player_id, r.item_config_ids, key)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            if (
                self._mail_sender is None
                or errcode.as_code(exc) != errcode.ErrInventoryCapacityFull
            ):
                raise
            await self._mail_sender.send_overflow_mail(
                r.player_id, r.item_config_ids, key
            )
            plog.get().info(
                "progress_grant_overflow_mailed",
                match_id=r.match_id,
                seq=r.seq,
                player_id=r.player_id,
                outbox_id=r.id,
                items=len(r.item_config_ids),
                idempotency_key=key,
                hint="背包满 → 装备掉落转个人邮件(同幂等键去重,直发链与邮件链至多一次)",
            )

    async def _resolve_action_record(
        self, r: bprepo.ProgressOutboxRecord
    ) -> bprepo.ProgressAction:
        """局内消费 / 丢弃:调 inventory,再把终态与出箱删除原子落定。"""
        if self._granter is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "inventory_addr not configured"
            )
        item_id, count = bprepo.progress_single_stack_fact(r)
        consume = int(r.kind) == int(bprepo.ProgressGrantKind.CONSUME_STACK)
        kind_name = "consume" if consume else "discard"
        key = progress_idempotency_key(r.match_id, r.seq, r.player_id, kind_name)
        result_code = errcode.OK
        try:
            if consume:
                await self._granter.consume_battle_item(
                    r.player_id, item_id, count, key
                )
            else:
                await self._granter.discard_battle_item(
                    r.player_id, item_id, count, key
                )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            code = errcode.as_code(exc)
            if is_retryable_progress_action_error(code):
                # 瞬时失败:不落终态,行保留退避重试(落 FAILED 会把可恢复的抖动
                # 变成"这次使用永久失败",玩家的道具白扣一次 claim)。
                raise
            result_code = code
        resolved = await self._repo.resolve_progress_action(r, result_code)
        # 局内扣减的持久终态已落定(出箱行同事务删除)。资产变更的逐玩家台账,
        # 出箱行随即消失,不留痕就只能反查 inventory。
        plog.get().info(
            "progress_action_resolved",
            match_id=r.match_id,
            seq=r.seq,
            player_id=r.player_id,
            outbox_id=r.id,
            kind=progress_kind_name(r.kind),
            item_config_id=item_id,
            count=count,
            status=progress_action_status_name(resolved.status),
            inventory_code=result_code,
            idempotency_key=key,
        )
        return resolved

    # ── 后台:任务事实转发出箱发布器 ──────────────────────────────────────

    async def run_mission_forwarder(self) -> None:
        """周期转发任务事实出箱(docs/design/mission.md §5.1)。

        与 run_progress_publisher 分开的理由是**故障域隔离**:任务行混进进度出箱会让
        mission 不可用卡住队首、连带阻塞该玩家的经验 / 掉落投递 —— 把弱依赖变成强依赖。

        reporter 未注入(mission_addr 未配)时直接返回:此时 report_progress 根本不产生
        任务出箱行,没有可投递的东西(转发关闭的完整语义,不是"产了不投")。
        """
        if self._mission_reporter is None:
            plog.get().info(
                "mission_forwarder_disabled",
                hint="mission_addr 未配 → 不产生任务出箱行,任务进度不受战斗事实驱动",
            )
            return
        interval = self._cfg.progress_publish_interval_or_default().total_seconds()
        plog.get().info(
            "mission_forwarder_started",
            interval=self._cfg.progress_publish_interval,
            batch=self._cfg.progress_batch_size_or_default(),
        )
        try:
            await safego.loop(
                "battle_mission_forwarder", interval, self._forward_mission_tick
            )
        finally:
            plog.get().info("mission_forwarder_stopped")

    async def _forward_mission_tick(self) -> None:
        try:
            await self.forward_mission_batch()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning("mission_forward_batch_failed", err=str(exc))

    async def forward_mission_batch(self) -> int:
        """取一批到期任务事实投递,返回本轮成功条数。

        `fetch_mission_outbox` 每玩家只返回队首一条,所以本批内**至多一条属于同一玩家**:
        单行失败退避只推迟该玩家自己的队列,其它玩家照常推进(跨玩家不互相拖)。

        语义:at-least-once + mission 侧收据表(uk + 指纹)幂等吸收;**行永不丢弃**。
        """
        if self._mission_reporter is None:
            return 0
        recs = await self._repo.fetch_mission_outbox(
            self._cfg.progress_batch_size_or_default()
        )
        forwarded = 0
        for r in recs:
            token = plog.bind_trace_id(str(uuid.uuid4()))
            try:
                key = progress_idempotency_key(
                    r.match_id, r.seq, r.player_id, "mission"
                )
                try:
                    await self._mission_reporter.report_mission_fact(
                        r.player_id, r.category, r.slot_value, r.amount, key
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    plog.get().warning(
                        "mission_fact_forward_failed",
                        id=r.id,
                        match_id=r.match_id,
                        seq=r.seq,
                        player_id=r.player_id,
                        category=r.category,
                        slot_value=r.slot_value,
                        amount=r.amount,
                        idempotency_key=key,
                        code=errcode.as_code(exc),
                        err=str(exc),
                    )
                    try:
                        await self._repo.defer_mission_outbox(r.id)
                    except asyncio.CancelledError:
                        raise
                    except BaseException as derr:  # noqa: BLE001
                        # 退避失败只告警:下轮 fetch 仍会取到该行重试,
                        # at-least-once 不受影响。
                        plog.get().warning(
                            "mission_outbox_defer_failed", id=r.id, err=str(derr)
                        )
                    continue
                try:
                    await self._repo.delete_mission_outbox(r.id)
                except asyncio.CancelledError:
                    raise
                except BaseException as derr:  # noqa: BLE001
                    # 删行失败 → 下轮重投同一事实;mission 侧收据 uk 幂等吸收,
                    # 不会重复计进度。
                    plog.get().warning(
                        "mission_outbox_delete_failed", id=r.id, err=str(derr)
                    )
                    continue
                # 任务进度是玩家可见资产(领奖依据):失败有逐行日志、成功只有聚合 count 时,
                # 「这一局的杀怪为什么没计进任务」事后无从对账(出箱行已删)。
                plog.get().info(
                    "mission_fact_delivered",
                    id=r.id,
                    match_id=r.match_id,
                    seq=r.seq,
                    player_id=r.player_id,
                    category=r.category,
                    slot_value=r.slot_value,
                    amount=r.amount,
                    idempotency_key=key,
                )
                forwarded += 1
            finally:
                token.var.reset(token)
        if forwarded > 0:
            plog.get().debug("mission_facts_forwarded", count=forwarded)
        return forwarded
