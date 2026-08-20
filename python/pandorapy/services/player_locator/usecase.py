"""player_locator 用例层 —— 对应 Go 侧 internal/biz/locator.go 的 LocatorUsecase。

不变量 §1(CLAUDE.md §9.1)「玩家只能在一个 Location」的执行点在这里:
redis hash 是单写者(SetLocation),覆盖语义 = 自动顶号;状态机守卫
(`guard_transition`)决定哪些覆盖是合法的。

★ 但 locator 仍然只是 **presence 投影**,不是归属权威(§9.22)。
  守卫拦的是「投影被旧连接/旧对局的迟到写顶回去」,不是「谁拥有这个玩家」。
  谁拥有玩家由 owner 服务回答;key miss **不能**当成「已离开旧 DS」。

★ 拒绝 reason 一律独立枚举(§11.3 R2):一个 if 收敛 N 个条件时必须拆成 N 个,
  否则线上只看得到「被拒了」,看不到「为什么被拒」。
"""

from __future__ import annotations

import dataclasses
import time
from typing import Any, Callable, Protocol

from prometheus_client import Counter

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy.services.player_locator import biz as lbiz
from pandorapy.services.player_locator import repo as lrepo

# SetGuarded WATCH/MULTI/EXEC 的 CAS 冲突重试次数(与 Go 的 optimisticRetry 同值)。
OPTIMISTIC_RETRY = 3

# 快速断线上报后的宽限期:真退出的玩家 ~10s 内判离线(不等满 30s 心跳 TTL);
# 窗口内重连 → PostLogin SetLocationHub 重写记录,状态自愈。
# **绝不即时置 OFFLINE**:玩家 travel 去战斗也触发 Hub Logout,靠 grace + 守卫免疫误判。
DISCONNECT_GRACE_SEC = 10.0

# last-seen 时刻的默认保留时长。必须远大于所有消费方的离线阈值(当前最长 team 的 180s)。
DEFAULT_LAST_SEEN_RETENTION_SEC = 3600.0

# ── 拒绝 reason(与 Go 逐字同名)──────────────────────────────────────────
REASON_SET_PRESENCE_STALE = "stale_hub_presence"
REASON_SET_PRESENCE_SUPERSEDED = "hub_presence_superseded_before_commit"
REASON_DISCONNECT_HUB_POD_MISSING = "hub_pod_missing"
REASON_DISCONNECT_PLAYER_ID_ZERO = "player_id_zero"
REASON_DISCONNECT_FENCE_INCOMPLETE = "hub_fence_incomplete"
REASON_DISCONNECT_FENCE_MISMATCH = "hub_fence_or_state_mismatch"
REASON_REFRESH_HUB_POD_MISSING = "hub_pod_missing"
REASON_QUERY_PLAYER_ID_ZERO = "player_id_zero"
REASON_SUBSCRIBER_ID_ZERO = "subscriber_id_zero"

# legacy 降级的 op label 取值(低基数 2 值)。
LEGACY_OP_REPORT_DISCONNECT = "report_disconnect"
LEGACY_OP_SET_LOCATION = "set_location"

# ★ 与 Go 侧 biz.HubPresenceLegacyDegraded 同名同 label。
#
# 为什么这条必须有**指标**而不只是日志:连接级 fence 是 Hub DS 与 locator 之间的
# 新协议,DS 没接上时服务端只能安全降级。降级本身是对的(滚动升级要能跑),
# 但后果是静默的 —— report_disconnect 降级时不缩 TTL、不记 last-seen、不发离场事件,
# 于是**所有按「离线满 N 秒」做决策的下游(组队自动退队等)一个都不会触发**,
# 而链路上每一环看起来都健康。这正是「测试全绿但一个人也踢不掉」的那类故障。
#
# 告警:滚动升级窗口之外 rate(...{op="report_disconnect"}[5m]) > 0 即告警。
HUB_PRESENCE_LEGACY_DEGRADED = Counter(
    "pandora_locator_hub_presence_legacy_degraded_total",
    "Hub DS 未携带连接级 presence fence 而走安全降级的次数"
    "(稳态应恒为 0;report_disconnect 降级会让离线时长类功能整体静默失效)",
    ["op"],
)


@dataclasses.dataclass(slots=True)
class LocationOutput:
    """GetLocation 的出参。"""

    state: int = 0
    hub_pod: str = ""
    shard_id: int = 0
    match_id: int = 0
    battle_pod: str = ""
    updated_at_ms: int = 0


class DepartureNotifier(Protocol):
    """把「Hub DS 观测到某玩家离开大厅」发成服务间事件(topic pandora.player.presence)。

    None = 未开启:此时 last-seen 时刻照常记录,只是没有实时触发器,消费方退化为
    「下次读到该实体时顺手复查」的兜底路径。这条降级是刻意保留的 ——
    事件流是加速器,权威始终是 locator 的查询接口。
    """

    async def notify_left_hub(self, player_id: int, left_at_ms: int, hub_pod: str) -> None:
        ...


class PresenceNotifier(Protocol):
    """presence fan-out 入口(由 PresenceHub 实现;None 表示未启用 → 纯拉)。"""

    def notify(self, player_id: int, state: int) -> None: ...
    def subscribe(self, subscriber_id: int, watched_ids: list[int]) -> None: ...
    def unsubscribe(self, subscriber_id: int) -> None: ...


def _to_data_fence(f: lbiz.HubPresenceFence) -> lrepo.HubPresenceFence:
    return lrepo.HubPresenceFence(
        assignment_id=f.assignment_id,
        admission_id=f.admission_id,
        admission_seq=f.admission_seq,
    )


def _log_set_rejected(reason: str, inp: lbiz.LocationInput, **extra: Any) -> None:
    """记录一次 SetLocation 的提前拒绝(§11.3 R2)。

    为什么必须显式打:service handler 把这些 errcode 转成 in-band Code 后返回
    正常响应,access log 只会记 rpc_ok(DEBUG);而 ErrInvalidArg /
    ErrLocatorConflict 都不在 IsServerFault 里,线上默认 info 级下**一条都看不到**
    —— 玩家 presence 写不进去的现场因此完全无痕。
    """
    plog.get().warning(
        "locator_set_rejected",
        reason=reason,
        player_id=inp.player_id,
        presence_state=inp.state,
        hub_pod=inp.hub_pod,
        battle_pod=inp.battle_pod,
        fence_match_id=inp.match_id,
        assignment_id=inp.hub_presence_fence.assignment_id,
        admission_id=inp.hub_presence_fence.admission_id,
        admission_seq=inp.hub_presence_fence.admission_seq,
        **extra,
    )


def guard_transition(
    inp: lbiz.LocationInput, incoming_fence: lrepo.HubPresenceFence
) -> Callable[[lrepo.LocationRecord, bool], None]:
    """返回 set_guarded 的状态机守卫闭包,实现不变量 §1。

    ── 一句话速记(判断分三层)────────────────────────────────────────────
      - 玩家原本没记录(not found)→ 首次上线,放行。
      - 旧状态不是对局态(OFFLINE / LOGIN_PENDING / HUB)→ 不命中,放行(普通顶号)。
      - 旧状态是对局态才拦:
          旧 = MATCHING:只拦 HUB 上报(防 hub DS 把 matchmaker 刚写的确认期冲掉);
          旧 = BATTLE(最严):
            新 BATTLE 且同 match_id → 放行(心跳续期);不同 → 拒(旧 DS 迟到心跳);
            新 MATCHING → 放行(下一局撮合);
            新 HUB → 带对的 match_id 令牌才放行(打完回大厅),否则拒;
            其余(LOGIN_PENDING 等裸登录)→ 一律拒。这就是防止断线重登把人从战斗里
            顶出去、导致 matchmaker 误判空闲、一人两处的核心洞。

    心法:旧状态越"重要"(BATTLE 最重),门卫越挑剔,只放跟这局有关的写进来。
    """

    def reject(reason: str, cur: lrepo.LocationRecord, **extra: Any) -> None:
        # 这些拒绝正是不变量 §1 的 fencing 事件。handler 把 ErrLocatorConflict 转成
        # in-band Code 后返回正常响应,access log 只记 DEBUG,故必须在拒绝点显式留证
        # —— 否则线上出「玩家被莫名踢出战斗 / 顶号」类问题时无日志可查。
        plog.get().warning(
            "locator_guard_rejected",
            reason=reason,
            player_id=inp.player_id,
            cur_state=cur.state,
            presence_state=cur.state,
            cur_match_id=cur.match_id,
            cur_hub_pod=cur.hub_pod,
            cur_battle_pod=cur.battle_pod,
            new_state=inp.state,
            fence_match_id=inp.match_id,
            hub_pod=inp.hub_pod,
            cur_updated_at_ms=cur.updated_at_ms,
            **extra,
        )

    def guard(cur: lrepo.LocationRecord, found: bool) -> None:
        if not found:
            return
        # HUB→HUB 也必须按连接代际守卫。同 Pod 秒重连时 hub_pod 不变,
        # 只有 assignment + admission seq/id 能阻止旧 SetLocation 迟到反向夺回投影。
        if cur.state == lbiz.LOCATION_STATE_HUB and inp.state == lbiz.LOCATION_STATE_HUB:
            current = cur.hub_presence_fence
            if current.is_complete():
                if incoming_fence.is_zero():
                    # 已经有带 fence 的当前连接,就不再接受不带 fence 的写覆盖它 ——
                    # 否则一台还没升级的旧 DS 能把新连接的投影降级回
                    # 「谁也说不清是哪条连接」。
                    reject(
                        "legacy_hub_write_downgrades_fenced_presence",
                        cur,
                        cur_assignment_id=current.assignment_id,
                        cur_admission_id=current.admission_id,
                        cur_admission_seq=current.admission_seq,
                        req_assignment_id="",
                        req_admission_id="",
                        req_admission_seq=0,
                    )
                    raise errcode.PandoraError(
                        errcode.ErrLocatorConflict,
                        "player %d reject legacy HUB write over fenced current presence",
                        inp.player_id,
                    )
                if not incoming_fence.is_complete():
                    reject(
                        "incomplete_hub_presence_fence",
                        cur,
                        cur_assignment_id=current.assignment_id,
                        cur_admission_id=current.admission_id,
                        cur_admission_seq=current.admission_seq,
                        req_assignment_id=incoming_fence.assignment_id,
                        req_admission_id=incoming_fence.admission_id,
                        req_admission_seq=incoming_fence.admission_seq,
                    )
                    raise errcode.PandoraError(
                        errcode.ErrLocatorConflict,
                        "player %d reject incomplete HUB presence fence",
                        inp.player_id,
                    )
                # **只在同 assignment 内定序**:秒重连落回同一 assignment 时,靠 seq
                # 单调 + 同序 admission_id 防 ABA。跨 assignment 不在这里判 ——
                # 那是 hub_allocator 的归属权威说了算(§9.22)。
                if incoming_fence.assignment_id == current.assignment_id and (
                    incoming_fence.admission_seq < current.admission_seq
                    or (
                        incoming_fence.admission_seq == current.admission_seq
                        and incoming_fence.admission_id != current.admission_id
                    )
                ):
                    reject(
                        "stale_hub_presence_generation",
                        cur,
                        cur_assignment_id=current.assignment_id,
                        cur_admission_id=current.admission_id,
                        cur_admission_seq=current.admission_seq,
                        req_assignment_id=incoming_fence.assignment_id,
                        req_admission_id=incoming_fence.admission_id,
                        req_admission_seq=incoming_fence.admission_seq,
                    )
                    raise errcode.PandoraError(
                        errcode.ErrLocatorConflict,
                        "player %d reject stale HUB admission assignment=%s "
                        "current_seq=%d incoming_seq=%d",
                        inp.player_id,
                        current.assignment_id,
                        current.admission_seq,
                        incoming_fence.admission_seq,
                    )

        if cur.state == lbiz.LOCATION_STATE_MATCHING:
            # 撮合确认期只拦可能 stale 的 hub DS 上报。
            if inp.state == lbiz.LOCATION_STATE_HUB:
                reject("stale_hub_during_matching", cur)
                raise errcode.PandoraError(
                    errcode.ErrLocatorConflict,
                    "player %d in MATCHING(match_id=%d), reject stale HUB report pod=%s",
                    inp.player_id,
                    cur.match_id,
                    inp.hub_pod,
                )
            return

        if cur.state == lbiz.LOCATION_STATE_BATTLE:
            if inp.state == lbiz.LOCATION_STATE_BATTLE:
                # 同局心跳续期放行;不同 match_id = 旧 DS / 旧 allocator 的迟到心跳,
                # 拒之以免把当前对局位置覆盖成旧对局(指向已死旧 DS,破 §1)。
                if inp.match_id != cur.match_id:
                    reject("battle_write_different_match", cur)
                    raise errcode.PandoraError(
                        errcode.ErrLocatorConflict,
                        "player %d in BATTLE(match_id=%d), reject BATTLE write for "
                        "different match_id=%d",
                        inp.player_id,
                        cur.match_id,
                        inp.match_id,
                    )
                return
            if inp.state == lbiz.LOCATION_STATE_MATCHING:
                return  # matchmaker 控制面写下一局撮合,放行
            if inp.state == lbiz.LOCATION_STATE_HUB:
                # hub 回流必须带当前战斗的 match_id 令牌。
                if inp.match_id == 0 or inp.match_id != cur.match_id:
                    reject("stale_hub_during_battle", cur)
                    raise errcode.PandoraError(
                        errcode.ErrLocatorConflict,
                        "player %d in BATTLE(match_id=%d), reject stale HUB report "
                        "pod=%s fence_match_id=%d",
                        inp.player_id,
                        cur.match_id,
                        inp.hub_pod,
                        inp.match_id,
                    )
                return
            # LOGIN_PENDING 等裸写无对局上下文,不得顶掉 active BATTLE。
            reject("bare_write_evicts_active_battle", cur)
            raise errcode.PandoraError(
                errcode.ErrLocatorConflict,
                "player %d in BATTLE(match_id=%d), reject non-battle write state=%d "
                "(bare login/reconnect cannot evict active battle)",
                inp.player_id,
                cur.match_id,
                inp.state,
            )

    return guard


class LocatorUsecase:
    """SetLocation / GetLocation / ClearLocation 等用例。"""

    def __init__(
        self,
        repo: lrepo.RedisLocationRepo,
        ttl_sec: float,
        presence: PresenceNotifier | None = None,
    ) -> None:
        self._repo = repo
        # ★ TTL 的机械下限在 biz.effective_ttl_sec 里(不是调优参数):
        # BATTLE presence 是 login/matchmaker 再入门的第一道信号,其 TTL 必须
        # ≥ DS 授权租约上限 + 偏差余量(27s)。配置调低会被机械抬回。
        self._ttl_sec = float(lbiz.effective_ttl_sec(int(ttl_sec)))
        self._presence = presence
        self._last_seen_retention_sec = DEFAULT_LAST_SEEN_RETENTION_SEC
        self._departure: DepartureNotifier | None = None

    @property
    def ttl_sec(self) -> float:
        return self._ttl_sec

    def set_last_seen_retention(self, sec: float) -> None:
        """覆盖 last-seen 保留时长(conf 注入)。

        <=0 视为不改(保持默认):配置缺字段时把保留期设成 0 会让整条链静默失效。
        """
        if sec > 0:
            self._last_seen_retention_sec = sec

    def set_departure_notifier(self, n: DepartureNotifier | None) -> None:
        self._departure = n

    # ── SetLocation ───────────────────────────────────────────────────────

    async def set_location(self, inp: lbiz.LocationInput) -> None:
        # 入参校验(每个分支一个独立 reason)。
        lbiz.validate_location_input(inp)

        resolved = _to_data_fence(inp.hub_presence_fence)
        if inp.state == lbiz.LOCATION_STATE_HUB and inp.hub_presence_fence.is_zero():
            # 旧 Hub DS 没带连接级 fence:HUB 写仍然放行(滚动升级必须能跑),但要留计数。
            # 这里只计数不打 Error —— 与 ReportDisconnect 的降级不同,写 HUB 走 legacy
            # 不会让任何功能失效,只是失去「同 pod 旧连接迟到写」的防护。
            HUB_PRESENCE_LEGACY_DEGRADED.labels(LEGACY_OP_SET_LOCATION).inc()

        # 先**只读**校验长 TTL meta,再 CAS 写 location,最后 commit meta。
        # 这样 location 已过期时仍可用长 TTL meta 挡住同 assignment 的旧 admission;
        # 而 MATCHING/BATTLE guard 拒绝时 meta 零副作用。
        if inp.state == lbiz.LOCATION_STATE_HUB:
            accepted = await self._repo.validate_hub_presence(inp.player_id, resolved)
            if not accepted:
                # 长 TTL meta 判定本次 HUB 写来自旧代连接。这是本服务
                # 「玩家秒重连后被旧连接迟到写顶回去」的唯一拦截点,必须留证。
                _log_set_rejected(REASON_SET_PRESENCE_STALE, inp)
                raise errcode.PandoraError(
                    errcode.ErrLocatorConflict,
                    "player %d reject stale HUB presence assignment=%s admission_seq=%d",
                    inp.player_id,
                    inp.hub_presence_fence.assignment_id,
                    inp.hub_presence_fence.admission_seq,
                )

        rec = lrepo.LocationRecord(
            state=inp.state,
            hub_pod=inp.hub_pod,
            shard_id=inp.shard_id,
            match_id=inp.match_id,
            battle_pod=inp.battle_pod,
            updated_at_ms=int(time.time() * 1000),
            hub_presence_fence=resolved,
        )
        # HUB 报文里的 match_id 仅作 BATTLE fence 令牌(供 guard 判定),
        # 玩家进入 HUB 后已无活跃对局,不持久化 match_id/battle_pod,免其它服务误读。
        if inp.state == lbiz.LOCATION_STATE_HUB:
            rec.match_id = 0
            rec.battle_pod = ""

        # prev* 只做观测:守卫闭包本就拿到「写之前的当前记录」,记下来是为了在写成功后
        # 判断这次是**状态迁移**(低频、必须 Info)还是**同态续期**(每次心跳一条、
        # 只能 Debug)。纯赋值,不参与任何判定。
        observed: dict[str, Any] = {
            "found": False, "state": 0, "match_id": 0, "hub_pod": ""
        }
        inner_guard = guard_transition(inp, resolved)

        def observed_guard(cur: lrepo.LocationRecord, found: bool) -> None:
            observed["found"] = found
            observed["state"] = cur.state
            observed["match_id"] = cur.match_id
            observed["hub_pod"] = cur.hub_pod
            inner_guard(cur, found)

        await self._repo.set_guarded(
            inp.player_id, rec, self._ttl_sec, OPTIMISTIC_RETRY, observed_guard
        )

        if inp.state == lbiz.LOCATION_STATE_HUB:
            committed = await self._repo.activate_hub_presence(
                inp.player_id, resolved, self._last_seen_retention_sec
            )
            if not committed:
                # validate 后 exact Disconnect 可能先缩 TTL / 写 left_at,随后本次
                # set_guarded 又把同 fence TTL 刷长;commit 会因 left_at 拒绝。
                # 此时必须再做一次 exact 收缩补偿。若位置已被更新代或对局态替换,
                # repo 层 exact guard 会零副作用。
                compensated = False
                if resolved.is_complete():
                    try:
                        await self._repo.shrink_hub_ttl(
                            inp.hub_pod, inp.player_id, resolved, DISCONNECT_GRACE_SEC
                        )
                    except errcode.PandoraError as exc:
                        # 补偿失败 = location TTL 被刷长了但 meta 拒绝确认:
                        # 玩家的 presence 会比真实在场多活一个 TTL。
                        _log_set_rejected(
                            REASON_SET_PRESENCE_SUPERSEDED, inp,
                            compensated=False, compensate_err=str(exc),
                        )
                        raise errcode.PandoraError(
                            errcode.ErrUnavailable,
                            "hub presence meta commit rejected and TTL compensation "
                            "failed player=%d",
                            inp.player_id,
                        ) from exc
                    compensated = True
                _log_set_rejected(
                    REASON_SET_PRESENCE_SUPERSEDED, inp, compensated=compensated
                )
                raise errcode.PandoraError(
                    errcode.ErrLocatorConflict,
                    "player %d hub presence superseded before meta commit",
                    inp.player_id,
                )

        # 非 HUB 的在线状态(MATCHING / BATTLE)走本路径而不是 RefreshHubLocations,
        # 必须在这里推 last_alive_ms —— 否则玩家一进战斗 meta 就不再更新,
        # 「打完一局直接退游戏」(最常见的退出方式)的离线时刻会停在很早的 Hub 阶段。
        # 已带 30s 节流,BATTLE 心跳的绝大多数调用只读不写。
        # best-effort:失败只告警,退化成「时刻偏早」,方向安全。
        if inp.state in (lbiz.LOCATION_STATE_MATCHING, lbiz.LOCATION_STATE_BATTLE):
            try:
                await self._repo.touch_alive(
                    inp.player_id, int(time.time() * 1000), self._last_seen_retention_sec
                )
            except errcode.PandoraError as exc:
                plog.get().warning(
                    "location_touch_alive_failed",
                    player_id=inp.player_id, state=inp.state, err=str(exc),
                )

        if self._presence is not None:
            self._presence.notify(inp.player_id, inp.state)

        logger = plog.get()
        # §11.3 R1 vs R4 的分界就在这里:
        #   - **状态迁移**(首次上线 / HUB↔MATCHING↔BATTLE 互切)是低频的链路阶段推进,
        #     「presence 什么时候从 HUB 变 BATTLE」只能靠它回答 → Info;
        #   - **同态续期**(下面的 location_set)挂在每玩家每次心跳上 → Debug。
        if not observed["found"] or observed["state"] != inp.state:
            logger.info(
                "location_state_changed",
                player_id=inp.player_id,
                prev_found=observed["found"],
                prev_presence_state=observed["state"],
                presence_state=inp.state,
                prev_hub_pod=observed["hub_pod"],
                hub_pod=inp.hub_pod,
                prev_match_id=observed["match_id"],
                battle_pod=inp.battle_pod,
                assignment_id=inp.hub_presence_fence.assignment_id,
                admission_seq=inp.hub_presence_fence.admission_seq,
                ttl_ms=int(self._ttl_sec * 1000),
            )
        # §11.3 R4:这条挂在**每玩家每次心跳**上,500 人/hub 下按 Info 打会把同文件里的
        # locator_guard_rejected / locator_set_rejected 彻底冲走 —— 那些才是排障要看的行。
        logger.debug(
            "location_set",
            player_id=inp.player_id,
            presence_state=inp.state,
            hub_pod=inp.hub_pod,
            battle_pod=inp.battle_pod,
            assignment_id=inp.hub_presence_fence.assignment_id,
            admission_seq=inp.hub_presence_fence.admission_seq,
            ttl_ms=int(self._ttl_sec * 1000),
        )

    # ── 查询面 ────────────────────────────────────────────────────────────

    async def get_location(self, player_id: int) -> LocationOutput:
        if player_id == 0:
            plog.get().warning(
                "locator_query_rejected",
                reason=REASON_QUERY_PLAYER_ID_ZERO, rpc="GetLocation", player_id=player_id,
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id must > 0")
        rec, found = await self._repo.get(player_id)
        logger = plog.get()
        if not found:
            # 不变量 §1:不存在等价 OFFLINE。
            # §11.3 R4:查询是高频路径,只能 Debug;但必须有 ——
            # 「重连时 locator 到底告诉调用方什么」是 key miss 被误当成
            # 「已离开旧 DS」这类事故(§9.22)的第一手证据。
            logger.debug(
                "location_queried",
                player_id=player_id, found=False,
                presence_state=lbiz.LOCATION_STATE_OFFLINE,
            )
            return LocationOutput(state=lbiz.LOCATION_STATE_OFFLINE)
        logger.debug(
            "location_queried",
            player_id=player_id, found=True, presence_state=rec.state,
            hub_pod=rec.hub_pod, battle_pod=rec.battle_pod,
            loc_match_id=rec.match_id, updated_at_ms=rec.updated_at_ms,
        )
        return LocationOutput(
            state=rec.state, hub_pod=rec.hub_pod, shard_id=rec.shard_id,
            match_id=rec.match_id, battle_pod=rec.battle_pod,
            updated_at_ms=rec.updated_at_ms,
        )

    async def batch_get_location(self, player_ids: list[int]) -> dict[int, LocationOutput]:
        """与 GetLocation 同语义,但**不给 miss 回填 OFFLINE 占位**。

        返回 map 只含 redis 命中的玩家;未在线 / 不存在的 player_id 不出现
        (调用方按缺席判离线,避免响应被大量离线占位撞胀)。
        """
        if not player_ids:
            return {}
        recs = await self._repo.batch_get(player_ids)
        return {
            pid: LocationOutput(
                state=r.state, hub_pod=r.hub_pod, shard_id=r.shard_id,
                match_id=r.match_id, battle_pod=r.battle_pod,
                updated_at_ms=r.updated_at_ms,
            )
            for pid, r in recs.items()
        }

    async def batch_get_last_seen(self, player_ids: list[int]) -> dict[int, int]:
        """批量查「最后一次被观测到离开 Hub 的时刻」(unix ms)。

        返回 map 只含有记录的玩家;缺席 = UNKNOWN。调用方必须与 BatchGetLocation
        合用:先确认此刻查不到位置(离线),再看 last-seen 判断离开了多久 ——
        单看本接口不能判离线(玩家可能已经回来了,而上一次的离开时刻还在保留期内)。
        """
        if not player_ids:
            return {}
        return await self._repo.batch_get_last_seen(player_ids)

    # ── 续期 / 断线 ───────────────────────────────────────────────────────

    async def refresh_hub_locations(self, hub_pod: str, player_ids: list[int]) -> int:
        """批量续期一批玩家的 HUB 位置 TTL(在线保活)。

        只有「state==HUB 且 hub_pod==本次上报 pod」的记录才续期(repo 层校验);
        MATCHING/BATTLE/其它 pod 的记录一律不动(不变量 §1)。
        玩家掉线 → Hub DS 停报该 id → key 自然过期 = 好友视角离线。
        不触发 presence 通知(续期不是状态变更)。
        """
        if not hub_pod:
            plog.get().warning(
                "locator_refresh_rejected",
                reason=REASON_REFRESH_HUB_POD_MISSING, requested=len(player_ids),
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "hub_pod must not be empty")
        if not player_ids:
            return 0
        refreshed = await self._repo.refresh_hub_locations(
            hub_pod, player_ids, self._ttl_sec, self._last_seen_retention_sec
        )
        # §11.3 R4:Hub DS 心跳携带(每 5s 一次、一次带全场百人),只能 Debug。
        # 差集(census 在场却没被续上的名单)由 repo 层按原因分类点名。
        plog.get().debug(
            "location_hub_refreshed",
            hub_pod=hub_pod, requested=len(player_ids), refreshed=refreshed,
            ttl_ms=int(self._ttl_sec * 1000),
        )
        return refreshed

    async def report_disconnect(
        self, hub_pod: str, player_id: int, fence: lbiz.HubPresenceFence
    ) -> bool:
        """快速断线上报:把该玩家 HUB 位置的 TTL 缩短到 grace。

        守卫在 repo 层同时核对 state、pod 与 exact connection fence;
        同 Pod 旧连接迟到也严格零副作用。不触发 presence 通知 ——
        缩 TTL 不是状态变更,真离线由 key 过期体现。
        """
        logger = plog.get()
        if not hub_pod:
            logger.warning(
                "locator_disconnect_rejected",
                reason=REASON_DISCONNECT_HUB_POD_MISSING, player_id=player_id,
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "hub_pod must not be empty")
        if player_id == 0:
            logger.warning(
                "locator_disconnect_rejected",
                reason=REASON_DISCONNECT_PLAYER_ID_ZERO, hub_pod=hub_pod,
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id must > 0")

        if fence.is_zero():
            # 滚动升级中的旧 Hub DS 无法证明具体连接代际。安全降级为等正常 presence
            # TTL,自身 RPC 仍返回 OK,不能再按同 pod 猜测并误伤秒重连的新连接。
            #
            # ⚠️ 这条降级**不是无害的**,必须能被发现:走到这里意味着本次断线上报被
            # 整条跳过 —— 不缩 TTL、不记 last-seen、不发离场事件,于是所有按
            # 「离线满 N 秒」做决策的下游(组队自动退队等)**一个都不会触发**,
            # 而链路上每一环都返回 OK、看起来健康。
            # 因此这里打 Error + 计数,而不是 Info。
            HUB_PRESENCE_LEGACY_DEGRADED.labels(LEGACY_OP_REPORT_DISCONNECT).inc()
            logger.error(
                "location_disconnect_legacy_noop",
                player_id=player_id, hub_pod=hub_pod,
                impact="no ttl shrink / no last-seen / no departure event: "
                       "offline-duration features stay silent",
                hint="Hub DS must send hub_presence_fence "
                     "(assignment_id + admission_id + admission_seq)",
            )
            return False
        if not fence.is_complete():
            logger.warning(
                "locator_disconnect_rejected",
                reason=REASON_DISCONNECT_FENCE_INCOMPLETE,
                player_id=player_id, hub_pod=hub_pod,
                req_assignment_id=fence.assignment_id,
                req_admission_id=fence.admission_id,
                req_admission_seq=fence.admission_seq,
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "hub presence fence must be complete or empty"
            )

        data_fence = _to_data_fence(fence)
        accepted, shrunk = await self._repo.shrink_hub_ttl(
            hub_pod, player_id, data_fence, DISCONNECT_GRACE_SEC
        )
        if not accepted:
            # state/pod/fence 任一不匹配:正常的迟到请求,严格不留**副作用**。
            # 「不留痕」指的是不留副作用,不是不留日志:这正是旧连接 / 旧 assignment 的
            # 迟到 Logout 被 fencing 掉的瞬间。不打这条,「断线上报到底有没有生效」
            # 在线上完全无法回答。
            logger.warning(
                "locator_disconnect_rejected",
                reason=REASON_DISCONNECT_FENCE_MISMATCH,
                player_id=player_id, hub_pod=hub_pod,
                req_assignment_id=fence.assignment_id,
                req_admission_id=fence.admission_id,
                req_admission_seq=fence.admission_seq,
                grace_ms=int(DISCONNECT_GRACE_SEC * 1000),
            )
            return False

        # location exact 守卫通过后才写 meta。若重连在两步之间推进了 meta,
        # 旧 fence 会在 record_last_seen 的单 key Lua 内被拒;反之新 Set 会原子清掉旧 left_at。
        recorded = False
        effective_at_ms = 0
        try:
            recorded, effective_at_ms = await self._repo.record_last_seen(
                player_id, data_fence, int(time.time() * 1000),
                self._last_seen_retention_sec,
            )
        except errcode.PandoraError as exc:
            # TTL 已缩但没 last-seen 只会让后续动作更保守;ReportDisconnect 本身仍是
            # best-effort 优化,不把一次 Redis 部分失败升级成玩家退出失败。
            logger.warning(
                "location_last_seen_write_failed",
                player_id=player_id, hub_pod=hub_pod, err=str(exc),
            )
        else:
            if recorded and self._departure is not None:
                try:
                    await self._departure.notify_left_hub(
                        player_id, effective_at_ms, hub_pod
                    )
                except Exception as exc:  # noqa: BLE001 —— best-effort,不阻断退出
                    logger.warning(
                        "location_departure_event_failed",
                        player_id=player_id, hub_pod=hub_pod, err=str(exc),
                    )
        logger.info(
            "location_disconnect_reported",
            player_id=player_id, hub_pod=hub_pod,
            assignment_id=fence.assignment_id, admission_seq=fence.admission_seq,
            shrunk=shrunk, last_seen_recorded=recorded,
            grace_ms=int(DISCONNECT_GRACE_SEC * 1000),
        )
        return shrunk

    async def clear_location(self, player_id: int) -> None:
        if player_id == 0:
            plog.get().warning(
                "locator_query_rejected",
                reason=REASON_QUERY_PLAYER_ID_ZERO, rpc="ClearLocation", player_id=player_id,
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id must > 0")
        await self._repo.delete(player_id)
        if self._presence is not None:
            self._presence.notify(player_id, lbiz.LOCATION_STATE_OFFLINE)
        # §11.3 R1:清位置是 presence 投影的不可逆推进(登出 / 强制下线),低频 → Info。
        plog.get().info(
            "location_cleared",
            player_id=player_id, presence_state=lbiz.LOCATION_STATE_OFFLINE,
        )

    # ── presence 订阅面(未启用时为 no-op,不报错)────────────────────────

    def subscribe_presence(self, subscriber_id: int, watched_ids: list[int]) -> None:
        if subscriber_id == 0:
            plog.get().warning(
                "locator_query_rejected",
                reason=REASON_SUBSCRIBER_ID_ZERO, rpc="SubscribePresence",
                watched_count=len(watched_ids),
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "subscriber_id must > 0")
        if self._presence is not None:
            self._presence.subscribe(subscriber_id, watched_ids)

    def unsubscribe_presence(self, subscriber_id: int) -> None:
        if subscriber_id == 0:
            plog.get().warning(
                "locator_query_rejected",
                reason=REASON_SUBSCRIBER_ID_ZERO, rpc="UnsubscribePresence",
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "subscriber_id must > 0")
        if self._presence is not None:
            self._presence.unsubscribe(subscriber_id)
