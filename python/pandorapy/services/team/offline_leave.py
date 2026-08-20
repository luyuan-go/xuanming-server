"""队员离线超时自动移出队伍 + 组票 roster fence —— 对应 Go 侧 internal/biz/offline_leave.go。

# 这条链长什么样

    Hub DS Logout ──ReportDisconnect──▶ player_locator(记 last-seen + 发离场事件)
                                              │ kafka: pandora.player.presence
                                              ▼
                              pandorapy.offlinewatch(排到期 → 到点回查 locator 权威)
                                              │
                  ┌───────────────────────────┴───────────────────────────┐
                  │ 不在线,未满 threshold                                  │ 不在线,已满 threshold
                  ▼                                                       ▼
      on_player_presence_lost                                   on_player_offline
      (取消他的准备,队伍掉出 READY,人还在队里)                    (把人摘出队伍)

# 为什么要两档(INC-20260813-001)

180s 的 threshold 是留给弱网 / 地铁 / 重连的余量,**不该缩**。但在这段时间里,
「他还留在队伍里」会被下游误读成「他有资格被拉进对局」:队长点开始匹配时,matchmaker
把一个已经不在大厅的人原样冻进票据,战斗 DS 拿到 N 人 roster 却只进来 N-1 个。

⚠️ **本档不是 INC-20260813-001 的第一根因**。那次事故的缺席者全程没有掉线 ——
他打完上一局先退出战斗,还在「结算 → 回大厅 → 重登」路上,队长 75 秒后就开了下一局。
真正的缺口是 team 侧没有 match-ended 复位路径(见 end_team_match);
本档是掉线场景的纵深防御,任何离线阈值都覆盖不到「正常玩家还没走回来」这一形态。

# 三道闸,少一道都会出事(硬档 on_player_offline)

 1. **此刻真的不在线**:由 offlinewatch 回查 locator 得出。locator 查不通一律不动作
    (§9.22 不确定不得冒充 OFFLINE),本文件不再重复判。
 2. **整支队伍没被一场对局占住**:自动摘人绝不能拆一支正在打的队伍 —— 那会让还在
    正常游戏的队友一起受影响。判定走 matchmaker 权威,读不确定就 fail-closed 重试。
 3. **这名玩家自己没被对局占住**:冗余的第二保险,防的是 locator 与 matchmaker 之间的
    短暂不一致(位置已掉、票据还在)。

# TOCTOU 已消除(2026-08-06)

matchmaker 组票走 `TeamService.BeginTeamMatch`,在 **team 自己的乐观锁内**冻结名单并
留下一把秒级自净租约;摘人在**同一把锁内**看到租约就推迟(DeferredError)。两个操作
因此只能有一个赢,窗口不再存在 —— 不是「后果收敛」,是消除。
compensate_if_committed_during_removal 作为纵深防御保留(覆盖租约已过期、而 claim 恰在
此刻落地的极窄残留),正常路径不会触发。

# 刻意不做的事

  - **正常路径不联动 cancel_matchmaking**。leave_team / kick 会撤票是因为那是玩家的主动
    操作;自动摘人只在「队伍没被对局占住」时发生,此时根本没有票可撤,调用它只会平添一次
    无谓 RPC 和一条误导性日志。唯一例外是上面那个残留窗口被命中时的补偿撤票。
  - **不处理单人队**。一个人的队伍没有队友受影响,摘掉他等于解散,不如留给 active_ttl
    自然回收 —— 玩家断线重连回来还能看到自己的队伍,少一次「队伍怎么没了」。

★ 循环依赖说明:`biz.TeamUsecase` 继承本模块的 Mixin,所以本模块**不能**在模块级
  import biz。共用的记录小工具(has_member / clone_team / …)经 `_biz()` 惰性取模块 ——
  在这里各写一份是更糟的选择:它们一旦与 biz 那份漂移(比如 remove_member 的语义),
  离队路径与自动摘人路径会走出两种成员表,而两边测试都绿。
"""

from __future__ import annotations

import asyncio
import importlib
import time

from pandora.team.v1 import team_pb2

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import offlinewatch
from pandorapy.services.team import diag
from pandorapy.services.team import metrics as tmetrics

# ── 租约钳制范围 ──────────────────────────────────────────────────────────────
#
# 只需覆盖「matchmaker 拿到名单 → ClaimPlayer 落地」这一小段。
# 下限防误配成 0(锁瞬间失效 = 等于没上锁);上限防一次异常的 Begin 把摘人挡住太久 ——
# 租约到期即自净,所以上限也是「matchmaker 崩了最多拖多久」的上界。
MATCH_LOCK_MIN_LEASE_MS = 2_000
MATCH_LOCK_MAX_LEASE_MS = 15_000

# 收据的重入窗:超过这么久之后,即使 attempt_id 与 post_ready_generation 都还对得上,
# 也不再把请求当成「同一次尝试的重试」。
#
# 为什么必须有上界:冻结之后若名单/状态都没再变,代际就停在收据记的值上。窗口兜的是
# 「重入路径不重复冻结、不推送」这层语义 —— 一次很久之后的真实点击应当走正常冻结路径
# (重清残留 ready、重新盖收据),而不是永远命中重入。
#
# 取 60s:明显大于客户端最长重试链(RPC 超时 + 断线重发,秒级),又远小于「看完结算回
# 大厅再开一局」的尺度。失败方向安全(多走一次正常冻结,不会多开一局)。
MATCH_START_RECEIPT_WINDOW_MS = 60_000

_BIZ = None


def _biz():  # noqa: ANN201
    """惰性取 biz 模块(见模块头「循环依赖说明」)。

    调用时 biz 一定已经 import 完毕(本 Mixin 的方法只可能经 TeamUsecase 实例调用),
    所以不存在半初始化模块的风险。
    """
    global _BIZ  # noqa: PLW0603
    if _BIZ is None:
        _BIZ = importlib.import_module("pandorapy.services.team.biz")
    return _BIZ


def _now_ms() -> int:
    return int(time.time() * 1000)


class _ReadyNoChange(Exception):
    """锁内的「没什么可改的」哨兵:让 update_with_lock 放弃写回。

    不是错误,外层统一映射成「幂等成功」—— 否则一个挂机离线的玩家会让他所在队伍
    每 15s 白写一次 Redis 并广播一次无意义的推送。
    """


def receipt_reentry(rec, attempt_id: str, current_gen: int, now_ms: int) -> bool:  # noqa: ANN001
    """判断本次请求是不是「同一次尝试的重试」。对应 Go 的 `receiptReentry`。

    三个条件缺一不可:
     1. attempt_id 相同 —— 是同一次尝试;
     2. 当前代际仍等于收据记下的**消费后**代际 —— 期间没有任何人动过 ready。
        动过就说明这是一次**新的**开局意图,必须重新消费,而不是拿回旧名单。
        这一条同时让 matchmaker 那个跨局复用的 operation_id((team,captain) 派生)安全:
        打完一局全队重新点准备后再开,代际已前进,不会被误判成上一局的重试;
     3. 还在重入窗内 —— 见 MATCH_START_RECEIPT_WINDOW_MS。

    时钟回拨会让 now-created 变负,按「在窗内」处理(那只是把重试窗放宽,方向仍安全)。
    """
    if rec is None or not rec.attempt_id or rec.attempt_id != attempt_id:
        return False
    if rec.post_ready_generation != current_gen:
        return False
    return now_ms - rec.created_at_ms <= MATCH_START_RECEIPT_WINDOW_MS


def roster_locked_for_match(team) -> bool:  # noqa: ANN001
    """此刻是否有未过期的组票租约。只在 team 的乐观锁**内**调用。"""
    return team.match_lock_until_ms > _now_ms()


def ready_target_indexes(team, targets) -> list[int]:  # noqa: ANN001
    """把 targets 映射成队伍内的下标;targets 为空 = 全队。

    不在队伍里的 id 直接忽略(对局期间离队是正常的)。
    """
    members = team.members
    if not targets:
        return list(range(len(members)))
    member_index = _biz().member_index
    out: list[int] = []
    for pid in targets:
        idx = member_index(members, pid)
        if idx >= 0:
            out.append(idx)
    return out


def ready_needs_clearing(team, targets) -> bool:  # noqa: ANN001
    """还有没有东西要复位。两件事任一成立就要动:

    目标成员里还有人挂着 ready,或队伍还停在 READY(理论上后者由前者推出,分开判是为了
    兜住历史脏数据 —— 一支 READY 却没人 ready 的队伍照样能让队长点开始匹配)。
    """
    if team.state == diag.STATE_READY:
        return True
    return any(team.members[i].ready for i in ready_target_indexes(team, targets))


class OfflineLeaveMixin:
    """离线自动退队 + 组票 fence。混入 `biz.TeamUsecase`。

    依赖 TeamUsecase 提供:repo / cfg / presence / match_commitment / _update_team /
    active_ttl_sec / push_update / sync_open_index / refresh_disbanded_ttl /
    cancel_matchmaking / is_team_committed_to_match。
    """

    # ── 开关 ────────────────────────────────────────────────────────────────

    def offline_leave_enabled(self) -> bool:
        """汇总「这条链是不是真的开着」。

        配置开了但依赖没注入(装配漏了)时按关处理,不会走到半截逻辑 ——
        少了 match_commitment 就没有闸②③,自动摘人会有拆掉正在打的队伍的风险。
        """
        return bool(self.cfg.offline_leave.enabled) and self.match_commitment is not None

    # ── 硬档:摘人 ──────────────────────────────────────────────────────────

    async def on_player_offline(self, player_id: int, offline_since_ms: int) -> None:
        """实现 offlinewatch.Handler:某玩家已确认离线满阈值。

        幂等:同一玩家被重复调用(事件重投、多副本各扫一遍、读路径兜底)是常态。
        玩家不在任何队伍可直接完成;队伍已不存在 / 已解散 / 已不含该玩家时,仍须用
        compare-delete 收敛旧 player→team 索引,成功后才算处理完成。

        抛异常只留给「这次没判成,下轮再来」:Redis 读失败、matchmaker 读不确定、
        写冲突或旧索引尚未清理成功。
        """
        if not self.offline_leave_enabled() or player_id == 0:
            return
        # §11.3 R3:offlinewatch 是定时任务面,没有玩家 JWT —— player_id 不进日志上下文
        # 的话,这一整条自动摘人链在日志里与该玩家的其它记录完全串不起来。
        token = plog.bind_player_id(player_id)
        try:
            await self._on_player_offline(player_id, offline_since_ms)
        finally:
            token.var.reset(token)

    async def _on_player_offline(self, player_id: int, offline_since_ms: int) -> None:
        logger = plog.get()
        try:
            team_id, found = await self.repo.get_player_team_id(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # R2:读不通就重试(绝不当成「他没队伍」)。不打日志的话,一个持续读失败的
            # 玩家会永远留在队伍里,而外部只看到"人一直没被摘掉"。
            logger.warning(
                "team_offline_leave_rejected",
                reason=diag.REASON_STORE_READ_FAILED,
                player_id=player_id,
                stage="player_index",
                err=str(exc),
            )
            raise
        if not found or team_id == 0:
            return  # 不在任何队伍,正常路径

        try:
            team, found = await self.repo.get(team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.warning(
                "team_offline_leave_rejected",
                reason=diag.REASON_STORE_READ_FAILED,
                player_id=player_id,
                team_id=team_id,
                stage="team_record",
                err=str(exc),
            )
            raise

        has_member = _biz().has_member
        if not found or team.state == diag.STATE_DISBANDED or not has_member(team, player_id):
            # 队伍主体已是终态,但 player→team 索引可能来自上一次「主体写成功、索引删除
            # 失败」的部分成功。必须精确删除仍指向旧 team_id 的索引;失败继续重试。
            await self.delete_offline_player_index(player_id, team_id)
            return
        if len(team.members) <= 1:
            # R4:单人队每轮复查都会走到这里,只能 Debug。
            logger.debug(
                "team_offline_leave_skipped",
                reason=diag.REASON_SINGLE_MEMBER_TEAM,
                player_id=player_id,
                team_id=team_id,
            )
            return

        # 闸②:整支队伍被一场对局占住 → 整轮跳过。读不确定必须 fail-closed(抛错重试),
        # 绝不能因为 matchmaker 抖一下就把一支正在打的队伍拆了。
        try:
            committed = await self.is_team_committed_to_match(team)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.warning(
                "team_offline_leave_commitment_unknown",
                reason=diag.REASON_MATCH_COMMITMENT_UNKNOWN,
                team_id=team_id,
                player_id=player_id,
                captain_id=team.captain_id,
                fail_closed=True,
                err=str(exc),
            )
            raise
        if committed:
            logger.debug(
                "team_offline_leave_skipped_match_committed",
                reason=diag.REASON_MATCH_COMMITTED,
                team_id=team_id,
                player_id=player_id,
                team_state=int(team.state),
            )
            # 对局占用是暂态,不是处理终态。抛 DeferredError 让 offlinewatch 保留到期项,
            # 票据释放后自动重查;正常返回会永久删任务,只能碰运气等下一次事件。
            raise offlinewatch.DeferredError(f"team {team_id} committed to a match")

        # 闸③:该玩家自己被对局占住(冗余保险,见模块头)。
        try:
            player_committed = await self.match_commitment.is_player_committed_to_match(
                player_id
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.warning(
                "team_offline_leave_player_commitment_unknown",
                reason=diag.REASON_PLAYER_COMMITMENT_UNKNOWN,
                team_id=team_id,
                player_id=player_id,
                fail_closed=True,
                err=str(exc),
            )
            raise
        if player_committed:
            logger.debug(
                "team_offline_leave_skipped_match_committed",
                reason=diag.REASON_PLAYER_COMMITTED,
                team_id=team_id,
                player_id=player_id,
            )
            raise offlinewatch.DeferredError(f"player {player_id} committed to a match")

        await self.remove_offline_member(team_id, player_id, offline_since_ms)

    # ── 软档:清准备 ────────────────────────────────────────────────────────

    async def on_player_presence_lost(self, player_id: int, since_ms: int) -> None:
        """实现 offlinewatch.PresenceLostHandler:成员此刻不在线,但还没满 threshold。

        # 它和 on_player_offline 的分工

            不在线,未满阈值(本方法)          → 取消他的准备,队伍掉出 READY。**人留在队伍里。**
            不在线,已满阈值(on_player_offline)→ 把人摘出队伍。

        队长因此根本点不动「开始匹配」(队伍不是 READY),不需要先撞一个错误码才知道
        出了事;玩家重连回来自己重新点准备即可 —— 与「队友取消准备」完全同一套语义,
        客户端零改动。

        # 幂等 / 无变化即无写

        本回调在玩家离线期间每轮 Observe 都会来一次。锁外先廉价判一次、锁内再判一次,
        没有实际变化就用哨兵放弃写回 —— 否则一个挂机离线的玩家会让他所在队伍每 15s
        白写一次 Redis 并广播一次无意义的推送。
        """
        if not self.offline_leave_enabled() or player_id == 0:
            return
        # R3:同 on_player_offline —— 定时任务面没有玩家 JWT,join key 必须手写。
        token = plog.bind_player_id(player_id)
        try:
            await self._on_player_presence_lost(player_id, since_ms)
        finally:
            token.var.reset(token)

    async def _on_player_presence_lost(self, player_id: int, since_ms: int) -> None:
        logger = plog.get()
        try:
            team_id, found = await self.repo.get_player_team_id(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.warning(
                "team_presence_lost_rejected",
                reason=diag.REASON_STORE_READ_FAILED,
                player_id=player_id,
                stage="player_index",
                err=str(exc),
            )
            raise  # 读不通 → 下轮重来,绝不当成「他没队伍」
        if not found or team_id == 0:
            return
        try:
            team, found = await self.repo.get(team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.warning(
                "team_presence_lost_rejected",
                reason=diag.REASON_STORE_READ_FAILED,
                player_id=player_id,
                team_id=team_id,
                stage="team_record",
                err=str(exc),
            )
            raise

        has_member = _biz().has_member
        # 终态 / 已不含该成员 / 单人队一律不动。旧 player→team 索引的收敛是
        # on_player_offline 的职责(它有 due/claim 闭环能保证重试),本方法不重复那条链。
        if (
            not found
            or team.state == diag.STATE_DISBANDED
            or not has_member(team, player_id)
            or len(team.members) <= 1
        ):
            # R4:玩家离线期间每轮 Observe 都会来一次,只能 Debug。四个条件拆开报,
            # 否则"他为什么没被取消准备"分不出是队伍没了还是他早已不在队里。
            reason = diag.REASON_TEAM_NOT_FOUND
            if found and team.state == diag.STATE_DISBANDED:
                reason = diag.REASON_TEAM_DISBANDED
            elif found and not has_member(team, player_id):
                reason = diag.REASON_NOT_MEMBER
            elif found:
                reason = diag.REASON_SINGLE_MEMBER_TEAM
            logger.debug(
                "team_presence_lost_skipped",
                reason=reason,
                player_id=player_id,
                team_id=team_id,
            )
            return

        result = await self.clear_member_ready(
            team_id, targets=[player_id], cause="presence_lost"
        )
        if result is None:
            return  # 并发路径已经改过了
        await self.publish_ready_cleared(result)
        logger.info(
            "team_presence_lost_unready",
            team_id=team_id,
            player_id=player_id,
            since_ms=since_ms,
            new_state=int(result.state),
            members=len(result.members),
            ready_count=diag.ready_count(result.members),
            ready_generation=result.ready_generation,
        )

    # ── 对局结束复位 ────────────────────────────────────────────────────────

    async def end_team_match(
        self, team_id: int, player_ids: list[int], expected_gen: int
    ) -> None:
        """对局结束后复位队伍准备状态(matchmaker 在 ReleaseMatch 时调用)。

        # 历史与现状

        这条路径为 INC-20260813-001 v2 的第一根因而生:当时 ready 是开局门槛,一局打完
        队伍仍停在 READY,队长能在队友还没回大厅时用残留 ready 立刻再开一局(缺席者被
        原样冻进票据,3v3 打成 3v2)。

        2026-08-17 起 ready 不再是全服开局门槛,本方法保留两个职责:
         1. 清掉本局成员的残留 ready 位 —— 存量客户端还在按 ready 渲染面板;
         2. 滚动升级共存窗口:旧 team 记录 / 旧 matchmaker 组合下,这是唯一的赛后复位。

        # 幂等与终态

        battle_result 的 outbox 会把 ReleaseMatch 重投到成功为止,所以本方法必然被重复
        调用:已复位过再调是零写零推送。队伍已解散 / 已不存在 / 成员已不在队,一律
        **正常返回** —— 那些都是「本就没什么可复位的」,报错只会让 outbox 永远重试下去。

        # 跨代幂等靠 expected_gen,不靠 player_ids

        光看「谁还挂着 ready」是**不够**的:ACK 丢失后玩家重新点了准备 / 离队重入 /
        队长已开新局,重投照样把新意图抹平。expected_gen 是 begin_team_match 冻结名单那
        一刻的 ready 代际,只有它仍等于当前代际才复位 —— 任何 ready 意图变更都会推进
        代际(见 ready_generation.py),重投自然落空。

        expected_gen==0 = 代际未知(滚动升级窗口的旧 matchmaker / 旧 team 记录),
        退化为「只在当前确实还挂着 ready 时复位一次」。不是跨代安全的,但严格优于完全不复位。
        """
        started = time.monotonic()
        logger = plog.get()
        if team_id == 0:
            logger.warning(
                "team_match_end_rejected",
                reason=diag.REASON_MISSING_ARG,
                players=len(player_ids),
                expected_ready_generation=expected_gen,
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "team_id required")
        if expected_gen == 0:
            # R2:滚动升级共存窗口(旧 matchmaker / 旧 team 记录没有代际)。此时复位退化成
            # 「只要还挂着 ready 就清一次」,不是跨代安全的 —— 出现即说明还有旧副本在跑,
            # 必须可见,否则事后无法解释"为什么某次复位把新点的准备抹掉了"。
            logger.warning(
                "team_match_end_legacy_generation",
                reason=diag.REASON_LEGACY_READY_REVOKED,
                team_id=team_id,
                players=len(player_ids),
                hint="调用方未回传 ready 代际(旧副本共存窗口),复位退化为非跨代安全",
            )
        try:
            result = await self.clear_member_ready(
                team_id,
                targets=list(player_ids),
                expected_gen=expected_gen,
                cause="match_end",
            )
        except offlinewatch.DeferredError:
            # 组票租约在手 = 队长已经开了**下一局**。租约是秒级自净的,交给上游 outbox
            # 下一轮重投即可(此时改 ready 会与那一局的冻结名单打架)。
            # clear_member_ready 已按分支打过带 reason 的 deferred 日志,这里只补收尾。
            logger.warning(
                "team_match_end_rejected",
                reason=diag.REASON_ROSTER_LOCKED,
                team_id=team_id,
                players=len(player_ids),
                expected_ready_generation=expected_gen,
                elapsed_ms=int((time.monotonic() - started) * 1000),
                hint="上游 outbox 会重投;返回 3007 属暂态",
            )
            raise errcode.PandoraError(
                errcode.ErrTeamConcurrent,
                "team %d roster locked by another match start",
                team_id,
            ) from None
        if result is None:
            return  # 已经复位过 / 队伍已终态:幂等成功
        await self.publish_ready_cleared(result)
        logger.info(
            "team_match_ended_unready",
            team_id=team_id,
            players=len(player_ids),
            expected_ready_generation=expected_gen,
            new_state=int(result.state),
            members=len(result.members),
            ready_count=diag.ready_count(result.members),
            ready_generation=result.ready_generation,
            elapsed_ms=int((time.monotonic() - started) * 1000),
        )

    # ── ready 复位的共同实现 ────────────────────────────────────────────────

    async def clear_member_ready(  # noqa: C901 —— 与 Go 同为一条线性判定链
        self,
        team_id: int,
        *,
        targets: list[int] | None = None,
        expected_gen: int = 0,
        cause: str = "",
    ):  # noqa: ANN201
        """「取消部分成员的准备、必要时把队伍打回 FORMING」的共同实现。

        两个调用方(掉线软化 / 对局结束复位)的差别只有触发原因与日志,**写路径必须
        完全一致** —— 一处漏了 sync_open_index 或推送,另一处的玩家就会对着一个不会
        刷新的面板点半天。

        targets 为空 = 全队。返回 None 表示「没什么可改的」,调用方按幂等成功处理。
        抛 offlinewatch.DeferredError 表示组票租约在手,调用方自行决定怎么重试。

        cause 只用于日志(§11.3 R1 的 cause 词根:presence_lost / match_end)。两个调用方
        共用同一段写路径,不带它就分不出"这次没复位"是掉线软化还是对局结束。
        **不参与任何判定** —— 加它是为了可诊断性,不是为了分叉行为。
        """
        targets = targets or []
        logger = plog.get()
        result_box: list = []
        # prev_state 只用于日志:锁内看到的最后一次取值即本次提交的依据(乐观锁重试时
        # 会被覆盖成最后一轮的值,正是我们要记的那一轮)。放在闭包外,重试不会重复打日志。
        state_box = [diag.STATE_UNSPECIFIED]
        clone_team = _biz().clone_team

        def _mutate(team) -> None:  # noqa: ANN001
            state_box[0] = team.state
            if team.state == diag.STATE_DISBANDED:
                # R4:队伍已解散是常态终态(outbox 迟到重投 / 离线玩家挂机),Debug。
                logger.debug(
                    "team_ready_clear_skipped",
                    reason=diag.REASON_TEAM_DISBANDED,
                    cause=cause,
                    team_id=team_id,
                    targets=len(targets),
                )
                raise _ReadyNoChange
            # 跨代 CAS:代际已经往前走了,说明这条复位对应的那一局早已翻篇(玩家重新点了
            # 准备 / 离队重入 / 队长开了新局)。按幂等成功处理,绝不能抹掉新意图。
            if expected_gen != 0 and team.ready_generation != expected_gen:
                # R2:这是**设计内**的幂等落空,但必须可见 —— INC-20260813-001 里
                # "打完一局还挂着 ready" 的对立面就是"复位被 CAS 挡了却没人知道"。
                # 只在 expected_gen != 0(end_team_match)时可达,不会被离线软化路径刷屏。
                logger.warning(
                    "team_ready_clear_skipped",
                    reason=diag.REASON_READY_GENERATION_MISMATCH,
                    cause=cause,
                    team_id=team_id,
                    expected_ready_generation=expected_gen,
                    current_ready_generation=team.ready_generation,
                    team_state=int(team.state),
                    members=len(team.members),
                    ready_count=diag.ready_count(team.members),
                    hint="代际已推进 = 这条复位对应的那一局早已翻篇,按幂等成功处理",
                )
                raise _ReadyNoChange
            # ★ 与 matchmaker 的共同线性化点,同 remove_offline_member:begin_team_match
            # 已经(或正在)把这份名单冻进票据,这时改 ready 会让票据里的快照与队伍打架。
            # 租约是秒级自净,推迟一轮即可。
            if roster_locked_for_match(team):
                logger.warning(
                    "team_ready_clear_deferred",
                    reason=diag.REASON_ROSTER_LOCKED,
                    cause=cause,
                    team_id=team_id,
                    lock_until_ms=team.match_lock_until_ms,
                    lock_operation_id=team.match_lock_operation_id,
                    hint="组票租约在手,秒级自净;调用方退避后重来",
                )
                raise offlinewatch.DeferredError(
                    f"team {team_id} roster locked for matchmaking"
                )
            if not ready_needs_clearing(team, targets):
                # R4:离线玩家每轮 Observe 都会走到这里,只能 Debug。
                logger.debug(
                    "team_ready_clear_skipped",
                    reason=diag.REASON_READY_ALREADY_CLEARED,
                    cause=cause,
                    team_id=team_id,
                    team_state=int(team.state),
                    targets=len(targets),
                    ready_count=diag.ready_count(team.members),
                )
                raise _ReadyNoChange
            for idx in ready_target_indexes(team, targets):
                team.members[idx].ready = False
            if team.state == diag.STATE_READY:
                # 少一个人在场 / 刚打完一局,就不再是「全员已准备」
                team.state = diag.STATE_FORMING
            team.updated_at_ms = _now_ms()
            result_box.append(clone_team(team))

        try:
            await self._update_team(team_id, _mutate)
        except _ReadyNoChange:
            return None
        except offlinewatch.DeferredError:
            raise
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            code = errcode.as_code(exc)
            if code in (errcode.ErrTeamNotFound, errcode.ErrTeamWrongState):
                # R4:队伍已没了 / 已终态是常态(outbox 迟到重投),Debug 即可。
                logger.debug(
                    "team_ready_clear_skipped",
                    reason=diag.REASON_TEAM_NOT_FOUND,
                    cause=cause,
                    team_id=team_id,
                    err=str(exc),
                )
                return None  # 队伍已没了 / 已终态:本就没什么可复位的
            if code == errcode.ErrTeamConcurrent:
                # 乐观锁重试耗尽,暂态。用 DeferredError 而非普通错误,免得正常竞争刷 Warn。
                # data 层已打过 team_update_lock_exhausted,这里只补上"是哪条业务被挡了"。
                logger.warning(
                    "team_ready_clear_deferred",
                    reason=diag.REASON_OPTIMISTIC_RETRY_EXHAUSTED,
                    cause=cause,
                    team_id=team_id,
                    targets=len(targets),
                    expected_ready_generation=expected_gen,
                )
                raise offlinewatch.DeferredError(
                    f"team {team_id} optimistic retry exhausted"
                ) from exc
            logger.warning(
                "team_ready_clear_failed",
                reason=diag.REASON_STORE_WRITE_FAILED,
                cause=cause,
                team_id=team_id,
                targets=len(targets),
                err=str(exc),
            )
            raise

        result = result_box[-1]
        # 状态机轨迹与其它写路径共用同一个 msg;cause 取调用方词根(presence_lost /
        # match_end),from == to 时 helper 自己会跳过,不会污染轨迹。
        diag.log_team_state_changed(
            team_id,
            state_box[0],
            result.state,
            cause,
            result.ready_generation,
            len(result.members),
        )
        return result

    async def publish_ready_cleared(self, result) -> None:  # noqa: ANN001
        """收口 ready 复位后的对外动作,两个调用方共用。

        复用 MEMBER_READY:这条推送表达的事实就是「成员的准备状态变了」,客户端既有的
        刷新逻辑一字不用改。刻意不新增 reason 枚举 —— 客户端在本推送到达时的动作与队友
        手动取消准备完全相同(§15.3:没有真实差异就不加协议面)。
        """
        member_ids = _biz().member_ids
        await self.push_update(
            0,
            member_ids(result),
            result,
            team_pb2.TEAM_UPDATE_REASON_MEMBER_READY,
            0,
        )
        # FORMING ↔ READY 改变「是否还在招募」→ 同步开放索引(同 set_ready)。
        await self.sync_open_index(result, result.map_id)

    # ── 摘人本体 ────────────────────────────────────────────────────────────

    async def remove_offline_member(  # noqa: C901 —— 与 Go 同为一条线性判定链
        self, team_id: int, player_id: int, offline_since_ms: int
    ) -> None:
        """把成员摘出队伍。

        核心与 leave_team 同源(同一把乐观锁、同样的队长转移 / READY 回退 / 索引清理),
        差别只有:推送原因不同、且不撤匹配票据。
        """
        logger = plog.get()
        disbanded_ttl = self.cfg.disbanded_retention_td().total_seconds()
        b = _biz()
        result_box: list = []
        # 锁内判定依据只用于日志(同 biz.py 各写路径)。
        state = {
            "reject_reason": "",
            "prev_state": diag.STATE_UNSPECIFIED,
            "prev_captain_id": 0,
            "prev_members": 0,
            "lock_until_ms": 0,
            "terminal_needs_index_cleanup": False,
        }

        def _mutate(team) -> None:  # noqa: ANN001
            state["prev_state"] = team.state
            state["prev_captain_id"] = team.captain_id
            state["prev_members"] = len(team.members)
            state["lock_until_ms"] = team.match_lock_until_ms
            # 锁内重查一遍:从上面的读到这里之间,他可能已经自己离队 / 被踢 / 队伍已解散。
            if team.state == diag.STATE_DISBANDED:
                state["terminal_needs_index_cleanup"] = True
                state["reject_reason"] = diag.REASON_TEAM_DISBANDED
                raise errcode.PandoraError(
                    errcode.ErrTeamWrongState, "team %d disbanded", team_id
                )
            if not b.has_member(team, player_id):
                state["terminal_needs_index_cleanup"] = True
                state["reject_reason"] = diag.REASON_NOT_MEMBER
                raise errcode.PandoraError(
                    errcode.ErrTeamNotFound,
                    "player %d not in team %d",
                    player_id,
                    team_id,
                )
            # 锁内再确认一次人数:并发摘人时不能把最后一个成员也摘掉(那等于自动解散队伍,
            # 超出了「移除离线队员」的授权范围)。
            if len(team.members) <= 1:
                state["reject_reason"] = diag.REASON_NO_TEAMMATE_TO_KEEP
                raise errcode.PandoraError(
                    errcode.ErrTeamWrongState, "team %d has no teammate to keep", team_id
                )
            # ★ 与 matchmaker 的共同线性化点:begin_team_match 在**同一把锁**内上的租约。
            # 看到它就说明有一次组票已经(或正在)把这份名单冻进票据 —— 这时摘人就会造出
            # 「人在票据里、却不在队伍里」。租约秒级自净,推迟重试即可,不需要任何补偿。
            if roster_locked_for_match(team):
                state["reject_reason"] = diag.REASON_ROSTER_LOCKED
                raise errcode.PandoraError(
                    errcode.ErrTeamConcurrent,
                    "team %d roster locked for matchmaking until %d",
                    team_id,
                    team.match_lock_until_ms,
                )
            state["reject_reason"] = ""

            b.remove_member(team, player_id)
            team.updated_at_ms = _now_ms()
            if team.captain_id == player_id:
                # 队长离线被摘 → 按 leave_team 的既有规则转给第一个成员,
                # 否则一支队伍会永远卡在「队长不在、没人能改图 / 审批申请」。
                team.captain_id = team.members[0].player_id
            if team.state == diag.STATE_READY:
                team.state = diag.STATE_FORMING  # 少了人就不再是「全员已准备」
            result_box.append(b.clone_team(team))

        try:
            await self._update_team(team_id, _mutate)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            reject = state["reject_reason"]
            if not reject:
                reject = diag.REASON_STORE_WRITE_FAILED
                if errcode.as_code(exc) == errcode.ErrTeamConcurrent:
                    reject = diag.REASON_OPTIMISTIC_RETRY_EXHAUSTED
            # R2:自动摘人被挡住是常态(租约竞争 / 玩家已自行离队),但"为什么没摘掉"
            # 必须查得出来 —— 否则只看到一个离线成员一直挂在队里。终态分支走 Debug
            # (每轮复查都会命中),暂态 / 故障分支走 Warn。
            if reject in (
                diag.REASON_TEAM_DISBANDED,
                diag.REASON_NOT_MEMBER,
                diag.REASON_NO_TEAMMATE_TO_KEEP,
            ):
                logger.debug(
                    "team_offline_leave_skipped",
                    reason=reject,
                    team_id=team_id,
                    player_id=player_id,
                    team_state=int(state["prev_state"]),
                    members=state["prev_members"],
                )
            else:
                logger.warning(
                    "team_offline_leave_deferred",
                    reason=reject,
                    team_id=team_id,
                    player_id=player_id,
                    team_state=int(state["prev_state"]),
                    members=state["prev_members"],
                    lock_until_ms=state["lock_until_ms"],
                    err=str(exc),
                )
            code = errcode.as_code(exc)
            # 主体不存在 / 已解散 / 已不含玩家时,仍须收敛此前读到的旧归属索引。
            # 单人队则保留其正常归属;其余错误由骨架退避后重排。
            if code == errcode.ErrTeamNotFound:
                await self.delete_offline_player_index(player_id, team_id)
                return
            if code == errcode.ErrTeamWrongState:
                if state["terminal_needs_index_cleanup"]:
                    await self.delete_offline_player_index(player_id, team_id)
                return
            if code == errcode.ErrTeamConcurrent:
                # 组票租约赢了这一轮(或乐观锁重试耗尽)。都是暂态,不是处理终态:
                # 保留到期项,租约自净后下轮重来。用 DeferredError 而非普通错误,
                # 免得每次正常竞争都刷一条 handler_failed 的 Warn。
                raise offlinewatch.DeferredError(
                    f"team {team_id} roster locked / retry exhausted"
                ) from exc
            raise

        result = result_box[-1]
        # CAS 删索引:仅当索引仍指向本队才删,防误删他并发加入新队的归属。
        # 失败时仍完成下面的开放索引 / 推送等主体写后动作,但最终抛错保留复查任务;
        # 下轮会命中 on_player_offline 的终态分支,继续精确清理这条旧索引。
        index_exc: BaseException | None = None
        try:
            await self.delete_offline_player_index(player_id, team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            index_exc = exc

        # 人数变了 → 同步开放招募索引(满员队摘掉一人后重新开放)。
        await self.sync_open_index(result, result.map_id)

        if result.state == diag.STATE_DISBANDED:
            # 理论上到不了(锁内已挡住摘最后一人),留着是为了万一将来放开该限制时不漏处理。
            await self.refresh_disbanded_ttl(team_id, disbanded_ttl)

        # caller 传 0:所有人都收到,包括被摘的那位本人 —— 他若在推送到达前恰好重连回来,
        # 能立刻知道自己已经不在队里,而不是对着一个过期的队伍界面点半天。
        await self.push_update(
            0,
            [*b.member_ids(result), player_id],
            result,
            team_pb2.TEAM_UPDATE_REASON_MEMBER_OFFLINE_LEFT,
            0,
        )

        logger.info(
            "team_offline_leave",
            team_id=team_id,
            player_id=player_id,
            offline_since_ms=offline_since_ms,
            threshold=self.cfg.offline_leave.threshold,
            prev_state=int(state["prev_state"]),
            new_state=int(result.state),
            remaining=len(result.members),
            captain_transferred=state["prev_captain_id"] == player_id,
            new_captain_id=result.captain_id,
            ready_generation=result.ready_generation,
        )
        diag.log_team_state_changed(
            team_id,
            state["prev_state"],
            result.state,
            "offline_leave",
            result.ready_generation,
            len(result.members),
        )
        await self.compensate_if_committed_during_removal(team_id, player_id)
        if index_exc is not None:
            raise index_exc

    async def delete_offline_player_index(self, player_id: int, team_id: int) -> None:
        """精确清理本次处理看到的旧 player→team 归属。

        compare-delete 在玩家已并发加入新队时是安全 no-op;存储失败必须向上传播,
        确保 offlinewatch 不会把仍有残留索引的任务误判为完成。
        """
        try:
            await self.repo.delete_player_index_if_matches(player_id, team_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_offline_leave_delete_player_index_failed",
                reason=diag.REASON_STORE_WRITE_FAILED,
                player_id=player_id,
                team_id=team_id,
                err=str(exc),
                hint="旧归属索引未收敛,玩家会被 3004 挡住建不了新队;复查任务保留,下轮重试",
            )
            raise

    async def inspect_team_presence(self, team) -> None:  # noqa: ANN001
        """读路径兜底:把完整成员列表交给 offlinewatch 的统一观察入口。

        ONLINE / OFFLINE / UNKNOWN 的分类和排期都留在 offlinewatch 内;team 不复制判定
        规则,也不在读路径上直接摘人。实际动作仍由后台复查循环执行。

        全程 best-effort:观察失败只记日志,绝不影响本次读返回 —— 组队面板打不开
        比多留一个离线成员严重得多。
        """
        if not self.offline_leave_enabled() or self.presence is None or team is None:
            return
        if team.state == diag.STATE_DISBANDED or len(team.members) <= 1:
            return
        ids = _biz().member_ids(team)
        try:
            await self.presence.observe(ids)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "team_presence_observe_failed",
                reason=diag.REASON_PRESENCE_OBSERVE_FAIL,
                team_id=team.team_id,
                members=len(ids),
                err=str(exc),
                hint="只影响离线兜底提名;本次读返回不受影响",
            )

    async def compensate_if_committed_during_removal(
        self, team_id: int, player_id: int
    ) -> None:
        """收口「检查对局占用 → 改队伍」之间的残留窗口。

        这个窗口跨服务(matchmaker 权威 + team 的 Redis 乐观锁),没法做成一个原子操作。
        窗口本身已被 begin_team_match 的锁内租约消除,这里覆盖的是「租约已过期、而 claim
        恰在此刻落地」的极窄残留:摘人成功后复核一次,发现票据确实在窗口内成立了,就走
        **与 leave_team 完全相同的补偿** —— 撤销整张票据,全队退回队列重新匹配。
        理由也和 leave_team 一样:队伍人数已经变了,票据里那份成员快照不再成立。

        为什么不在锁内复核:Redis 事务里发不了 gRPC。
        为什么不回滚摘人:那时票据已冻结,把人加回去反而制造第二种不一致。

        复核 RPC 失败时不重试也不回滚(人已经摘了),只记 error + 计数 —— 这是本路径唯一
        需要人工看一眼的残留,必须可观测,不能静默。
        """
        if self.match_commitment is None:
            return
        try:
            committed = await self.match_commitment.is_player_committed_to_match(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            tmetrics.OFFLINE_LEAVE_RACE.labels(outcome="recheck_failed").inc()
            plog.get().error(
                "team_offline_leave_race_recheck_failed",
                reason=diag.REASON_RACE_RECHECK_FAILED,
                team_id=team_id,
                player_id=player_id,
                err=str(exc),
                impact="member already removed; cannot tell whether a ticket froze him in "
                "during the window",
            )
            return
        if not committed:
            return  # 常态:窗口没被命中

        # 窗口被命中了。撤票语义与 leave_team 一致:排队中 → 全队退出队列;
        # 确认期 → 等价该玩家拒绝确认(match 失败,其余票据退回队列)。
        tmetrics.OFFLINE_LEAVE_RACE.labels(outcome="compensated").inc()
        plog.get().warning(
            "team_offline_leave_race_compensated",
            reason=diag.REASON_RACE_TICKET_FROZE,
            team_id=team_id,
            player_id=player_id,
            detail="match ticket froze this member between the gate check and the team write; "
            "cancelling it so the team rematches without him",
        )
        await self.cancel_matchmaking(team_id, player_id)

    # ── 组票 roster fence:与 matchmaker 的共同线性化点 ──────────────────────

    async def begin_team_match(  # noqa: C901 —— 与 Go 同为一条线性判定链
        self,
        team_id: int,
        captain_id: int,
        operation_id: str,
        lease_ms: int,
        require_ready: bool,
    ):  # noqa: ANN201
        """在 team 的乐观锁内原子完成
        「校验 + 冻结名单 + 清残留 ready + 上租约锁 + 返回冻结快照」。

        # ready 是不是开局门槛,由**关卡表**决定(2026-08-18 拍板:两模式按图二选一)

        require_ready 来自关卡表 ready_mode 列(matchmaker 按本次 StartMatch 的 map_id
        解析后传入):

            require_ready=True (PRE_READY)     队伍必须 State==READY 才放行 —— 面板上全员
                                               点过准备,准备态本身就是「此刻都在」的证明。
            require_ready=False(POST_CONFIRM)  FORMING 也放行,新鲜度改由撮合确认期承担。

        「带着不在场的人开局」(INC-20260813-001)在两种模式下都有防线:
          - 两种模式共有:StartMatch 在线闸(matchmaker ensureAllPresent),离开大厅超过
            宽限窗(默认 30s)的成员直接 4011 点名拒绝 —— 事故里那位退场 75s 的缺席者
            在这里就进不了票;
          - PRE_READY:队伍必须 READY;任何人掉线 / 离队 / 入队都会把队伍打回 FORMING;
          - POST_CONFIRM:撮合确认期(confirm_timeout 默认 15s)全员点「接受」才拉 DS;
            缺席者超时 / 拒绝 → match FAILED,此时 DS 尚未分配,没有任何人被拉进对局。

        方案 A 的另一半 —— 锁内冻结名单、秒级租约、收据幂等重入 —— **两种模式下都原样
        保留**:它们消除的是「组票 vs 自动摘人」的 TOCTOU 与「响应丢失重试」的重复消费,
        与 ready 无关。

        Begin 在两种模式下都清掉 ready 位并把队伍转回 FORMING。PRE_READY 下这是**必需**的:
        一次准备只授权一次开局,不清就等于队长能拿同一次准备连开两局(正是方案 A 要堵的
        形状);POST_CONFIRM 下则是为了存量客户端 —— 它们还在发 SetReady,残留 ready 不清,
        面板会在开局后继续显示「已准备」。

        # 两条路径

         1. **重入**:同一 attempt 的重试(响应丢失)。返回收据里那份冻结名单,不再重复冻结。
         2. **正常冻结**:校验通过(PRE_READY 时含 READY 闸)后留快照 → 清 ready →
            转 FORMING → 上租约 → 开收据。

        # 租约仍然要上(消除 TOCTOU 的那一半不变)

        matchmaker 原先只读 GetTeam 取名单,与本服务的自动摘人分属两把锁,凑不出共同线性
        化点。在这里上锁后,「冻结名单」与「移除离线成员」落在同一把 team 乐观锁上,
        两者只能有一个赢。

        返回 (冻结快照, lease_expires_at_ms)。
        """
        started = time.monotonic()
        logger = plog.get()
        if team_id == 0 or captain_id == 0 or not operation_id:
            # 一个 if 三个条件 → 三个 reason 分不开时,至少把三项原值一起打出来:
            # matchmaker 传空 operation_id 与传空 captain_id 的排查方向完全不同。
            logger.warning(
                "team_match_start_rejected",
                reason=diag.REASON_MISSING_ARG,
                team_id=team_id,
                captain_id=captain_id,
                operation_id=operation_id,
                lease_ms=lease_ms,
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "team_id, captain_id and operation_id required"
            )
        lease = max(MATCH_LOCK_MIN_LEASE_MS, min(int(lease_ms), MATCH_LOCK_MAX_LEASE_MS))

        b = _biz()
        # 锁内看到的判定依据只用于日志:乐观锁重试时会被覆盖成最后一轮的取值,
        # 那正是本次提交真正依据的那一轮(与 biz.py 各写路径同一套写法)。
        out = {
            "snapshot": None,
            "committed": None,
            "expires_at_ms": 0,
            "reentered": False,
            "reject_reason": "",
            "prev_state": diag.STATE_UNSPECIFIED,
            "prev_captain_id": 0,
            "prev_members": 0,
            "prev_ready": 0,
            "lock_until_ms": 0,
            "lock_owner_op": "",
        }

        def _mutate(team) -> None:  # noqa: ANN001, C901
            # 乐观锁重试会重跑整个闭包 → 上一轮的输出必须清干净,否则「前一轮判成重入、
            # 这一轮正常冻结」会带着上一轮的 snapshot/标志位污染本轮结果。
            out["snapshot"] = None
            out["reentered"] = False
            out["prev_state"] = team.state
            out["prev_captain_id"] = team.captain_id
            out["prev_members"] = len(team.members)
            out["prev_ready"] = diag.ready_count(team.members)
            out["lock_until_ms"] = team.match_lock_until_ms
            out["lock_owner_op"] = team.match_lock_operation_id
            if team.state == diag.STATE_DISBANDED:
                out["reject_reason"] = diag.REASON_TEAM_DISBANDED
                raise errcode.PandoraError(
                    errcode.ErrTeamWrongState, "team %d disbanded", team_id
                )
            now = _now_ms()

            # ── 路径 1:重入(同一 attempt 的重试)────────────────────────────
            #
            # 必须**先于**队长与 READY 校验:此刻队伍已因上一次消费转成 FORMING,任何按
            # READY 判定的门都会把自己的重试拒掉;队长若恰在重试间隙转移,再验队长同样会拒。
            # 对一次**已提交成功**的操作重新做授权判定,正是端到端幂等被打破的经典形状
            # (§9.23)。收据本身就是「这次 attempt 已被授权并提交过」的证据;
            # service 层的 verify_match_call 已把住调用方。
            rec = (
                team.match_start_receipt
                if team.HasField("match_start_receipt")
                else None
            )
            if receipt_reentry(rec, operation_id, team.ready_generation, now):
                if (
                    team.match_lock_until_ms > now
                    and team.match_lock_operation_id != operation_id
                ):
                    # 收据还有效但租约已被另一次组票拿走 —— 理论上只在收据窗内换队长
                    # 再开局时可达。不抢锁,按正常竞争退避。
                    out["reject_reason"] = diag.REASON_ROSTER_LOCKED
                    raise errcode.PandoraError(
                        errcode.ErrTeamConcurrent,
                        "team %d roster locked by operation %s until %d",
                        team_id,
                        team.match_lock_operation_id,
                        team.match_lock_until_ms,
                    )
                out["reject_reason"] = ""
                out["expires_at_ms"] = now + lease
                team.match_lock_until_ms = out["expires_at_ms"]
                team.match_lock_operation_id = operation_id
                team.updated_at_ms = now
                # 用收据重建「冻结那一刻」的队伍:当前记录的 ready 已被清空、状态已转
                # FORMING,直接返回当前记录等于把一份空 ready 的名单交给 matchmaker 建票。
                # State 置 READY 只为与首次返回的快照形状一致 —— matchmaker 不读 State。
                snap = b.clone_team(team)
                del snap.members[:]
                snap.members.extend(b.clone_members(rec.roster))
                snap.state = diag.STATE_READY
                snap.ready_generation = rec.consumed_ready_generation
                snap.ClearField("match_start_receipt")
                out["snapshot"] = snap
                out["reentered"] = True
                return

            if team.captain_id != captain_id:
                out["reject_reason"] = diag.REASON_NOT_CAPTAIN
                raise errcode.PandoraError(
                    errcode.ErrTeamNotCaptain,
                    "player %d is not captain of team %d",
                    captain_id,
                    team_id,
                )
            if (
                team.match_lock_until_ms > now
                and team.match_lock_operation_id != operation_id
            ):
                # 另一次组票的租约还没到期。这是正常竞争(并发重试 / 罕见的换 attempt),
                # 返回可重试错误 —— 几秒后租约自净,客户端重试即可。
                out["reject_reason"] = diag.REASON_ROSTER_LOCKED
                raise errcode.PandoraError(
                    errcode.ErrTeamConcurrent,
                    "team %d roster locked by operation %s until %d",
                    team_id,
                    team.match_lock_operation_id,
                    team.match_lock_until_ms,
                )

            # ── PRE_READY 图的准备门槛(关卡表 ready_mode=1)──────────────────
            #
            # 必须放在**租约冲突判定之后**:走到这里队伍可能刚被上一次冻结转成 FORMING,
            # 先判 READY 会把一个「稍后重试即可」的暂态说成「队伍未准备」这种终态 ——
            # 客户端会据此引导玩家去重新点准备,而其实几秒后租约一过什么都不用做。
            #
            # POST_CONFIRM 图(require_ready=False,含关卡表留空与旧 matchmaker 不发该
            # 字段)整道跳过,行为与本字段上线前逐字节一致。
            if require_ready and team.state != diag.STATE_READY:
                out["reject_reason"] = diag.REASON_TEAM_NOT_READY
                raise errcode.PandoraError(
                    errcode.ErrTeamWrongState,
                    "team %d not ready (state=%d, map requires pre-match ready)",
                    team_id,
                    int(team.state),
                )

            # ── 路径 2:正常冻结 ──────────────────────────────────────────────
            #
            # 先留冻结快照(matchmaker 建票的输入),再清 ready —— 顺序不能反,
            # 反了快照里的 ready 位就全是假的(PRE_READY 图靠它对账,存量客户端靠它显示)。
            out["reject_reason"] = ""
            out["snapshot"] = b.clone_team(team)
            for m in team.members:
                m.ready = False
            team.state = diag.STATE_FORMING
            out["expires_at_ms"] = now + lease
            team.match_lock_until_ms = out["expires_at_ms"]
            team.match_lock_operation_id = operation_id
            team.updated_at_ms = now

        def _stamp(team) -> None:  # noqa: ANN001
            # 盖章在代际推进**之后**:收据要记的是冻结后代际(receipt_reentry 的 CAS 依据)。
            # 手动去猜「当前值 + 1」等于复刻 update_team 内部实现,推进规则一变就静默劈叉。
            snap = out["snapshot"]
            if snap is not None and not out["reentered"]:
                rec = team.match_start_receipt
                rec.Clear()
                rec.attempt_id = operation_id
                rec.roster.extend(b.clone_members(snap.members))
                rec.consumed_ready_generation = snap.ready_generation
                rec.post_ready_generation = team.ready_generation
                rec.created_at_ms = team.updated_at_ms
            out["committed"] = b.clone_team(team)

        try:
            await self._update_team(team_id, _mutate, stamp=_stamp)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 锁内没给出 reason 的只剩「队伍不存在 / 乐观锁重试耗尽 / Redis 故障」三类。
            reject = out["reject_reason"]
            if not reject:
                code = errcode.as_code(exc)
                if code == errcode.ErrTeamNotFound:
                    reject = diag.REASON_TEAM_NOT_FOUND
                elif code == errcode.ErrTeamConcurrent:
                    reject = diag.REASON_OPTIMISTIC_RETRY_EXHAUSTED
                else:
                    reject = diag.REASON_STORE_WRITE_FAILED
            # R2:这是「队长点了开始匹配却没进匹配」的**唯一**服务端证据。matchmaker 侧
            # 只看得到一个错误码,team 侧不打就两边都断线。依据字段(队伍状态 / 人数 /
            # ready 数 / 当前租约持有者)必须同框,否则查不出到底是没准备还是被别人占着。
            logger.warning(
                "team_match_start_rejected",
                reason=reject,
                team_id=team_id,
                captain_id=captain_id,
                operation_id=operation_id,
                team_state=int(out["prev_state"]),
                actual_captain_id=out["prev_captain_id"],
                members=out["prev_members"],
                ready_count=out["prev_ready"],
                lock_until_ms=out["lock_until_ms"],
                lock_operation_id=out["lock_owner_op"],
                elapsed_ms=int((time.monotonic() - started) * 1000),
                err=str(exc),
            )
            raise

        snapshot = out["snapshot"]
        committed = out["committed"]
        if not out["reentered"]:
            # 残留 ready 已被清掉 → 队伍已转 FORMING。必须推送 + 同步招募索引,否则存量
            # 客户端面板一直显示全员已准备,且 FORMING 队伍不回到招募列表。
            await self.publish_ready_cleared(committed)
            diag.log_team_state_changed(
                team_id,
                out["prev_state"],
                committed.state,
                "match_start_consume",
                committed.ready_generation,
                len(committed.members),
            )

        # R1:上租约锁 = 这一局的名单从此刻起被冻结(不可逆推进,且是 matchmaker 建票的输入)。
        logger.info(
            "team_match_roster_locked",
            team_id=team_id,
            captain_id=captain_id,
            operation_id=operation_id,
            reentry=out["reentered"],
            lease_ms=lease,
            expires_at_ms=out["expires_at_ms"],
            members=len(snapshot.members),
            ready_count=diag.ready_count(snapshot.members),
            consumed_ready_generation=snapshot.ready_generation,
            post_ready_generation=committed.ready_generation,
            elapsed_ms=int((time.monotonic() - started) * 1000),
        )
        # 冻结那一刻每个成员的 (id, ready, hero) 三元组。INC-20260813-001(3v3 打成 3v2)
        # 事后唯一能还原"到底是谁没进去"的取证点。
        # reentry:同 attempt 重试,名单来自收据,未再次消费 ready。
        diag.log_match_roster_frozen(
            team_id, captain_id, operation_id, snapshot, out["reentered"]
        )
        return snapshot, out["expires_at_ms"]


# ── 兜底候选源:整队一起掉线时唯一能发现残留的路径 ────────────────────────────


class TeamRosterSource:
    """实现 offlinewatch.RosterSource:增量提名「有队伍的玩家」。

    # 为什么必须有它(2026-08-12 实测发现)

    排期原本只有两条触发链,它们在同一种情况下**同时失效**:
      - kafka 离场事件:依赖 Hub DS 走完 Logout 调 ReportDisconnect —— Hub DS 整机崩溃
        时不会调;
      - GetMyTeam 读路径:依赖有活人打开组队面板 —— 整队都掉线时没人打开。

    现场证据:两名玩家的 hubmeta 只有 last_alive_ms、没有 left_at_ms(印证 Hub 侧没走
    Logout),他们仍挂在队伍里,而调度队列是空的 —— 永久残留。

    last_alive_ms 解决的是「**能不能判**」,本类型解决的是「**谁来触发判**」,缺一不可。
    注意本类型只负责**提名**,判定与排期全部由 observe 走 locator 权威完成:
    它不看时间、不做任何判断,因此不存在「用本地时钟猜离线」的风险。
    """

    __slots__ = ("_repo", "_cursor", "_sweep_started_at", "_scanned_in_round", "_rounds")

    def __init__(self, repo) -> None:  # noqa: ANN001
        self._repo = repo
        self._cursor = 0
        # 以下三项只用于**观测一轮完整遍历要多久** —— 那是判断 roster_scan_batch 够不够大
        # 的唯一现场依据。纯进程内,重启即清空,不写任何存储
        # (§16.10 调度状态不得寄生到业务数据上)。
        self._sweep_started_at = 0.0
        self._scanned_in_round = 0
        self._rounds = 0

    async def next_batch(self, limit: int) -> list[int]:
        """每轮取一小段游标。

        游标只存进程内:它是纯调度提示,重启从头扫一遍即可,不写进任何权威或派生存储
        (§16.10)。

        游标回到 0 表示「本次完整遍历结束」,此时打一条 info 记录这一轮的耗时与候选数 ——
        运维据此核对「全量扫描周期 <= threshold」是否成立;不成立就得调大 roster_scan_batch,
        否则残留成员会等远超阈值的时间才被复查。
        """
        if self._sweep_started_at == 0.0:
            self._sweep_started_at = time.monotonic()
        try:
            ids, nxt = await self._repo.scan_player_index(self._cursor, limit)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # R2:扫描是「整队一起掉线」时唯一的候选来源。它持续失败 = 残留成员永远
            # 没人提名,而调用方只看到一个错误往上抛。带上游标位置,便于判断是不是
            # 卡在同一段 keyspace(而不是偶发抖动)。
            plog.get().warning(
                "team_roster_scan_failed",
                reason=diag.REASON_STORE_READ_FAILED,
                cursor=self._cursor,
                limit=limit,
                round=self._rounds,
                scanned_in_round=self._scanned_in_round,
                err=str(exc),
            )
            raise
        self._cursor = nxt
        self._scanned_in_round += len(ids)

        if nxt == 0:  # 一轮遍历结束
            self._rounds += 1
            elapsed = time.monotonic() - self._sweep_started_at
            plog.get().info(
                "team_roster_full_scan_completed",
                round=self._rounds,
                scanned=self._scanned_in_round,
                elapsed=f"{elapsed:.3f}s",
                hint="elapsed 必须明显小于 offline_leave.threshold;"
                "否则调大 offlinewatch roster_scan_batch",
            )
            self._sweep_started_at = 0.0
            self._scanned_in_round = 0
        return ids
