"""孤儿 Allocated GameServer 对账清扫的**编排层** —— 对应 Go 侧
`services/battle/ds_allocator/internal/biz/orphan_gameserver.go`。

判定链、四重防误删与首见表的**纯逻辑**已经落在 `orphan_reclaim.py`
(`OrphanReclaimer.plan_round` / `on_reclaim_outcome`),本模块只负责 Go 那份文件里
剩下的三件事,不重复实现判定:

    ① 从权威存储收集分配记录对 GameServer 的三维引用(fail-closed)
    ② 按 `ORPHAN_GS_RECONCILE_INTERVAL_SEC` 节流,挂在既有 sweep 循环末尾
    ③ 台账出身证明 + 执行决策(exact 复核删除)+ 回填结果 + 指标与日志

## 一句话背景

Agones **不回收** Allocated GameServer。一台 GS 若处于 Allocated 却在权威存储里没有
任何分配记录引用,它会永久占位锁死 Fleet 容量(实例:泄漏 18h 把 min=max=2 的匹配池
锁死);而"人工确认无人后删除"已两次误删载人 DS —— 人肉路径必须从流程上废除。
完整判定链见 `orphan_reclaim.py` 模块头,那里才是这套安全论证的存放处。

## 两个时钟,不是笔误

    now_sec  **单调**秒。用于观察期(`plan_round` 比的是"连续观察了多久")与节流。
             用墙钟的话一次 NTP 回拨就能让全部候选瞬间"满足观察期"而被删。
    now_ms   **墙钟**毫秒。只用于台账保留期修剪与日志里的 candidate_since ——
             台账是 Redis ZSET,score 是各副本写入时的墙钟 ms,拿单调钟去和它比毫无
             意义(两者原点不同)。

Go 侧用同一个 `time.Time` 同时承担这两件事(它既有墙钟也有单调读数),Python 没有
这种复合类型,只能显式传两个。

## 台账查询的位置(与 Go 逐格对齐)

Go 在"候选已满足观察期"之后才 `AllocationLedgerContains`,而且**在单轮删除封顶之前**
—— 于是台账查无的候选不会白白吃掉删除名额。`plan_round` 是纯同步函数,不能在里面
发 Redis 调用,所以本模块把台账查询**前置**到规划之前,只对"本轮无权威引用"的 GS
逐个查(正常情况这个集合是空的),再把结果作为 `ledger_allocation_ids` 交给
`plan_round`。判定顺序因此与 Go 完全一致:无 label → 台账查无 → 才轮到封顶。

查询**失败**与查询**查无**必须分开:前者是"证据不可得"(保留候选、下轮重试),
后者是"证据显示不是本权威分配的"(保留不删 + 告警)。混成一件事的方向是
"一次 Redis 抖动 = 疑似权威视图分裂",会把真信号淹掉。

## 调度纪律

节流靠 `_last_reconcile_sec` 这个进程内**非权威调度提示**(同 Go 的
`lastOrphanGSReconcile`):重启即清空,效果只是把回收推迟一个观察期,方向安全。
**不新建第二套 timer 状态机**(§16.10)—— 本模块没有自己的循环,由 sweep 每 tick
调用 `reconcile_if_due`。到期后的动作是"重新拉清单 + 重查权威",不是"假设上轮结论
仍然成立",故属合法兜底而非用定时器掩盖时序。
"""

from __future__ import annotations

import datetime as _dt
import time
from typing import Protocol

from prometheus_client import Counter

from pandorapy import godur
from pandorapy import log as plog
from pandorapy.services.ds_allocator import orphan_reclaim
from pandorapy.services.ds_allocator.agones_allocator import AllocatedGameServerInfo

# 对账节流间隔(秒)。Go: `orphanGSReconcileInterval = time.Minute`。
# sweep 每 `sweep_interval`(默认 5s)tick,对账每分钟至多一轮(一次 GS LIST + 全量
# 记录读)。回收时效要求以**小时**计(占位泄漏),1min 远够;更密只是白耗控制面。
ORPHAN_GS_RECONCILE_INTERVAL_SEC = 60.0

# allocation 台账保留期(秒)。Go: `orphanGSLedgerRetention = 7 * 24 * time.Hour`。
# 功能上只需 ≫ 观察阈值(10min),取 7 天给"泄漏很久才被注意到"的场景留余量;
# 超期条目每轮对账顺带 ZREMRANGEBYSCORE 清除,台账容量 = 7 天内的分配次数,有界(§9.24)。
ORPHAN_GS_LEDGER_RETENTION_SEC = 7 * 24 * 3600

# int64 下界。台账 prune 的 cutoff 会经 repo 的 `_require_int64` 校验;Python 整数不回绕
# (Go 的 int64 会),一个荒谬的 now_ms 减去 7 天可能落到 int64 之外。显式判掉,让
# "时钟坏了"在这里就报出来,而不是变成一次静默的空修剪或一个看不懂的 repo 异常。
_INT64_MIN = -(1 << 63)

ORPHAN_GS_RECLAIM_COUNTER = Counter(
    "pandora_ds_allocator_orphan_gameserver_reclaims_total",
    "孤儿 Allocated GameServer 对账处置计数"
    "(result=reclaimed 回收成功 / skipped 复核失效 / failed 删除失败 / unprovable 台账查无保留)",
    ["result"],
)


class OrphanGameServerReconciler(Protocol):
    """对账清扫需要的编排层能力。由 `agones_allocator.AgonesGameServerAllocator` 实现。
    Go: `biz.OrphanGameServerReconciler`。

    local / mock 分配器**不实现**它(它们没有 Agones Allocated 概念),清扫自动禁用。
    """

    async def list_allocated_game_servers(self) -> list[AllocatedGameServerInfo]: ...

    async def delete_allocated_game_server_exact(
        self, name: str, uid: str, expected_allocation_id: str
    ) -> bool:
        """exact 复核删除。返回 False = 复核失效(候选作废,重新观察)。"""
        ...


class BattleAllocationLedger(Protocol):
    """防误删 ④ 的权威台账能力。由 `repo.RedisBattleRepo` 实现。
    Go: `biz.BattleAllocationLedger`。

    未实现该 Protocol 的 repo 下清扫**整体禁用** —— 没有出身证明就没有删除权。
    """

    async def record_allocation_ledger(self, allocation_id: str, at_ms: int) -> None: ...

    async def allocation_ledger_contains(self, allocation_id: str) -> bool: ...

    async def prune_allocation_ledger(self, before_ms: int) -> int: ...


class BattleRecordSource(Protocol):
    """权威分配记录的读取面(`repo.RedisBattleRepo` 的两个方法)。

    `get_battle` 返回 None = 记录不存在(对应 Go 的 `found=false`)。
    """

    async def range_active_battles(self) -> list[int]: ...

    async def get_battle(self, match_id: int): ...  # noqa: ANN201 —— 见 repo.unmarshal_battle


async def collect_battle_game_server_refs(
    repo: BattleRecordSource,
) -> orphan_reclaim.GameServerRefs:
    """从权威存储收集**全部**分配记录对 GameServer 的引用。
    Go: `(*AllocatorUsecase).collectBattleGameServerRefs`。

    ★ fail-closed:任何一条记录读失败都**整轮报错**(异常上抛)。部分引用集会把仍被
      引用的 GS 误判为孤儿 —— 这是本清扫唯一不可接受的错误方向。

    ★ Range 与 Get 之间记录被正常释放(`get_battle` 返回 None)**不是**错误:释放路径
      本就会回收 GS,继续观察即可。
    """
    refs = orphan_reclaim.GameServerRefs()
    match_ids = await repo.range_active_battles()
    for match_id in match_ids:
        battle = await repo.get_battle(match_id)
        if battle is None:
            continue
        # 三维都收:分配记录的三个字段是不同写入路径分别填的,少一维就等于把这些
        # **活跃对局**误判成孤儿(详见 orphan_reclaim.GameServerRefs 的注释)。
        if battle.ds_pod_name:
            refs.pod_names.add(battle.ds_pod_name)
        if battle.gameserver_uid:
            refs.uids.add(battle.gameserver_uid)
        if battle.allocation_id:
            refs.allocation_ids.add(battle.allocation_id)
    return refs


class OrphanGameServerJanitor:
    """一轮对账的编排。Go 侧是 `AllocatorUsecase` 上的两个方法 +
    `orphanGSFirstSeen` / `lastOrphanGSReconcile` 两个字段。

    ★ 做成独立对象而不是塞进 usecase:清扫的**全部**状态(首见表、上轮时刻、已告警
      候选)都是进程内非权威调度提示。把它们和权威态放在同一个对象里,迟早会有人
      顺手拿首见表当"这台 GS 确实无人"的证据 —— 它不是证据,证据每轮都要重新取。
    """

    __slots__ = (
        "_reconciler",
        "_ledger",
        "_repo",
        "_reclaimer",
        "_last_reconcile_sec",
        "_announced_ms",
        "_round_budget_sec",
    )

    def __init__(
        self,
        reconciler: OrphanGameServerReconciler | None,
        ledger: BattleAllocationLedger | None,
        repo: BattleRecordSource,
        *,
        reclaim_after_sec: int = orphan_reclaim.DEFAULT_RECLAIM_AFTER_SEC,
        max_per_round: int = orphan_reclaim.DEFAULT_MAX_RECLAIM_PER_ROUND,
        round_budget_sec: float = orphan_reclaim.DEFAULT_ROUND_BUDGET_SEC,
    ) -> None:
        self._reconciler = reconciler
        self._ledger = ledger
        self._repo = repo
        self._reclaimer = orphan_reclaim.OrphanReclaimer(
            reclaim_after_sec=reclaim_after_sec,
            max_per_round=max_per_round,
            round_budget_sec=round_budget_sec,
        )
        self._round_budget_sec = round_budget_sec
        self._last_reconcile_sec: float | None = None
        # key → 首见的**墙钟毫秒**。两个用途:候选首见 Warn 只打一次;以及回收成功时
        # 那条 ERROR 里的 candidate_since / observed(Go 直接读它自己的 firstSeen 表,
        # 而这里那张表在 OrphanReclaimer 内部)。
        # 容量上界 = 当前候选数,每轮按候选集修剪(§9.18 进程内容器有界)。
        self._announced_ms: dict[str, int] = {}

    @property
    def enabled(self) -> bool:
        """两件能力缺一即禁用。Go: `reconcileOrphanGameServersIfDue` 的首个 return。"""
        return self._reconciler is not None and self._ledger is not None

    @property
    def reclaim_after_sec(self) -> int:
        """生效的观察阈值(已按下限钳制,见 `orphan_reclaim._clamp_reclaim_after`)。"""
        return self._reclaimer.reclaim_after_sec

    async def reconcile_if_due(self, now_sec: float, now_ms: int) -> None:
        """按节流间隔跑一轮对账。Go: `reconcileOrphanGameServersIfDue`。

        由 sweep 在**每轮末尾**调用,继承其 leader 门与单协程并发域;本方法不自建循环。
        """
        if not self.enabled:
            return
        if (
            self._last_reconcile_sec is not None
            and now_sec - self._last_reconcile_sec < ORPHAN_GS_RECONCILE_INTERVAL_SEC
        ):
            return
        self._last_reconcile_sec = now_sec
        await self.reconcile(now_sec, now_ms)

    async def reconcile(self, now_sec: float, now_ms: int) -> None:
        """执行一轮对账(四重防误删见 `orphan_reclaim.py` 模块头)。
        Go: `reconcileOrphanGameServers`。
        """
        reconciler = self._reconciler
        ledger = self._ledger
        if reconciler is None or ledger is None:
            return

        try:
            gs_list = await reconciler.list_allocated_game_servers()
        except Exception as exc:  # noqa: BLE001 —— 防误删①:证据不可得 = 本轮什么都不做
            plog.get().warning("orphan_gs_list_failed", err=str(exc))
            return

        if not gs_list:
            # Go 这里是 `clear(u.orphanGSFirstSeen)`。首见表在 OrphanReclaimer 内部,
            # 用一次空清单的 plan_round 达成同一效果(它末尾按当前候选集修剪 = 全清),
            # 而不是伸手去改别人的私有字段。
            self._reclaimer.plan_round(
                allocated=[],
                refs=orphan_reclaim.GameServerRefs(),
                ledger_allocation_ids=set(),
                now_sec=now_sec,
            )
            self._announced_ms.clear()
            return

        try:
            refs = await collect_battle_game_server_refs(self._repo)
        except Exception as exc:  # noqa: BLE001 —— 防误删①的后半条
            # 权威引用不可得 = 本轮不删,且把全部已有候选的观察起点**重置为当前时刻**:
            # 「连续每轮都有证据核验」是删除的前提,证据中断就必须重新起算。
            # 方向安全:权威抖动只会推迟回收,绝不会让未经核验的墙钟时间计入阈值。
            self._reclaimer.reset_all_observations(now_sec)
            plog.get().warning(
                "orphan_gs_authority_refs_unavailable",
                candidates_reset=self._reclaimer.observation_count(),
                err=str(exc),
            )
            return

        await self._prune_ledger(ledger, now_ms)

        infos = [_to_reclaim_info(gs) for gs in gs_list]
        fleet_by_key = {_gs_key(i): gs.fleet for i, gs in zip(infos, gs_list, strict=True)}
        # ★ 候选谓词与 `plan_round` 的入口条件必须保持一致(身份齐全 + 未进入删除宽限
        #   + 无权威引用)。之所以在这里重算一次:台账查询是 async 的,而 plan_round 是
        #   纯同步判定 —— 想让"台账查无不吃删除名额"这条 Go 顺序成立,只能把台账结果
        #   先备好再交给它。改 plan_round 的入口条件时,这里要一起改。
        candidates = [
            i for i in infos if i.name and i.uid and not i.deleting and not refs.references(i)
        ]
        self._announce_candidates(candidates, fleet_by_key, now_ms)

        provable, check_failed = await self._resolve_ledger(ledger, candidates)

        round_start = time.monotonic()
        planned = self._reclaimer.plan_round(
            allocated=infos,
            refs=refs,
            ledger_allocation_ids=provable,
            now_sec=now_sec,
            elapsed_sec=lambda: time.monotonic() - round_start,
        )

        attempts = 0
        unprovable = 0
        for decision in planned:
            gs = decision.gs
            fleet = fleet_by_key.get(_gs_key(gs), "")
            if decision.result == orphan_reclaim.RESULT_UNPROVABLE:
                if gs.allocation_id in check_failed:
                    # 证据**不可得**(台账查询本身失败),不是"证据显示查无"。候选保留、
                    # 下轮重试,且不计进 unprovable 告警(那条告警是权威视图分裂的信号)。
                    continue
                unprovable += 1
                ORPHAN_GS_RECLAIM_COUNTER.labels(orphan_reclaim.RESULT_UNPROVABLE).inc()
                if gs.allocation_id == "":
                    plog.get().warning(
                        "orphan_allocated_gs_unprovable_no_label",
                        gameserver=gs.name,
                        uid=gs.uid,
                        fleet=fleet,
                    )
                else:
                    plog.get().warning(
                        "orphan_allocated_gs_unprovable_ledger_miss",
                        gameserver=gs.name,
                        uid=gs.uid,
                        fleet=fleet,
                        allocation_id=gs.allocation_id,
                    )
                continue
            if decision.result != orphan_reclaim.RESULT_RECLAIMED:
                continue

            # 资源占用防护:`plan_round` 已按条数封顶;墙钟预算这里再拦一次,因为真正的
            # 耗时(GET 复核 + DELETE,各 ≤ allocate_timeout)只有执行侧知道。超限的候选
            # **保留首见时间**下轮继续 —— 不调 `on_reclaim_outcome` 就是"保留"。
            if attempts > 0 and time.monotonic() - round_start >= self._round_budget_sec:
                plog.get().info(
                    "orphan_gs_round_budget_exhausted",
                    attempts=attempts,
                    budget=godur.duration_string(_dt.timedelta(seconds=self._round_budget_sec)),
                )
                break
            attempts += 1
            await self._reclaim_one(reconciler, gs, fleet, now_ms)

        if unprovable > 0:
            # 台账查无的候选 ≥1 即打 ERROR:它既可能是台账上线前的存量泄漏 / 手工 GSA,
            # 也可能是「本进程读错了权威」(防误删④正在拦下一次全量误删)——两者都必须有人看。
            plog.get().error(
                "orphan_allocated_gs_unprovable_present",
                count=unprovable,
                hint=(
                    "存在无法证明出身本权威的孤儿候选,已全部保留不删;"
                    "若数量≈全部 Allocated,优先排查本进程 Redis 配置是否指向了错误/空的权威"
                    "(权威视图分裂)"
                ),
            )

        # 修剪已告警集合:不再是候选的项移出,容量上界 = 当前候选数(§9.18)。
        live = {_gs_key(i) for i in candidates}
        for key in [k for k in self._announced_ms if k not in live]:
            del self._announced_ms[key]

    async def _prune_ledger(self, ledger: BattleAllocationLedger, now_ms: int) -> None:
        """台账保留期修剪(§9.24 有界性)。只在**成功拿到权威证据**的轮次里顺带做。

        失败只告警:修剪是容量卫生,不是本轮判定的前提。
        """
        cutoff_ms = now_ms - ORPHAN_GS_LEDGER_RETENTION_SEC * 1000
        if cutoff_ms < _INT64_MIN:
            plog.get().warning(
                "orphan_gs_ledger_prune_failed",
                err=f"ledger prune cutoff underflows int64: now_ms={now_ms}",
            )
            return
        try:
            await ledger.prune_allocation_ledger(cutoff_ms)
        except Exception as exc:  # noqa: BLE001 —— 修剪失败不影响本轮判定
            plog.get().warning("orphan_gs_ledger_prune_failed", err=str(exc))

    async def _resolve_ledger(
        self, ledger: BattleAllocationLedger, candidates: list[orphan_reclaim.GameServerInfo]
    ) -> tuple[set[str], set[str]]:
        """查每个候选的出身证明,返回 `(台账可证的 allocation_id, 查询失败的 allocation_id)`。

        ★ 两个集合必须分开(见模块头):"查不到"允许告警并保留不删;"查不了"只能保留
          候选、下轮重试,绝不能冒充成"查不到"去触发权威视图分裂告警。

        ★ 无 label 的候选**不查**:它们在 `plan_round` 里就落 UNPROVABLE,查了也没有
          key 可查(`allocation_ledger_contains("")` 恒 False,平白多一次往返)。
        """
        provable: set[str] = set()
        check_failed: set[str] = set()
        for gs in candidates:
            if gs.allocation_id == "":
                continue
            try:
                known = await ledger.allocation_ledger_contains(gs.allocation_id)
            except Exception as exc:  # noqa: BLE001 —— fail-closed:证据不可得不删
                check_failed.add(gs.allocation_id)
                plog.get().warning(
                    "orphan_allocated_gs_ledger_check_failed",
                    gameserver=gs.name,
                    uid=gs.uid,
                    err=str(exc),
                )
                continue
            if known:
                provable.add(gs.allocation_id)
        return provable, check_failed

    async def _reclaim_one(
        self,
        reconciler: OrphanGameServerReconciler,
        gs: orphan_reclaim.GameServerInfo,
        fleet: str,
        now_ms: int,
    ) -> None:
        """执行一条 RECLAIMED 决策并把**实际结果**回填进首见表。

        ★ 三个分支的方向都不能写反(细节见 `OrphanReclaimer.on_reclaim_outcome`):

            failed    保留候选(首见时间不变),下轮幂等重试 —— 调用没打出去,对象状态
                      未知但也未被证伪,继续观察是安全的。
            skipped   **作废候选、重新观察满一个完整窗口** —— 复核失效说明这台 GS 的
                      resourceVersion 刚变过,很可能已经被重新分配出去了;不作废的话
                      下一轮阈值早已满足,会立刻再发一次删除,而这次它可能已经载人了。
            reclaimed 同样删键(候选已经不存在了)。
        """
        key = _gs_key(gs)
        first_ms = self._announced_ms.get(key, now_ms)
        try:
            deleted = await reconciler.delete_allocated_game_server_exact(
                gs.name, gs.uid, gs.allocation_id
            )
        except Exception as exc:  # noqa: BLE001 —— 删除失败:保留候选,下轮重试
            self._reclaimer.on_reclaim_outcome(gs, orphan_reclaim.RESULT_FAILED)
            ORPHAN_GS_RECLAIM_COUNTER.labels(orphan_reclaim.RESULT_FAILED).inc()
            plog.get().warning(
                "orphan_allocated_gs_reclaim_failed",
                gameserver=gs.name,
                uid=gs.uid,
                fleet=fleet,
                err=str(exc),
            )
            return
        if not deleted:
            self._reclaimer.on_reclaim_outcome(gs, orphan_reclaim.RESULT_SKIPPED)
            self._announced_ms.pop(key, None)
            ORPHAN_GS_RECLAIM_COUNTER.labels(orphan_reclaim.RESULT_SKIPPED).inc()
            plog.get().info(
                "orphan_allocated_gs_reclaim_skipped_recheck",
                gameserver=gs.name,
                uid=gs.uid,
                fleet=fleet,
            )
            return
        self._reclaimer.on_reclaim_outcome(gs, orphan_reclaim.RESULT_RECLAIMED)
        self._announced_ms.pop(key, None)
        ORPHAN_GS_RECLAIM_COUNTER.labels(orphan_reclaim.RESULT_RECLAIMED).inc()
        # ERROR 级(运维语义的破坏性动作必须显眼):正常运行不该出现孤儿,出现即说明
        # 某次外部释放没有闭环,值得追查来源。
        plog.get().error(
            "orphan_allocated_gs_reclaimed",
            gameserver=gs.name,
            uid=gs.uid,
            fleet=fleet,
            allocation_id=gs.allocation_id,
            candidate_since=_rfc3339(first_ms),
            observed=godur.duration_string(_dt.timedelta(milliseconds=now_ms - first_ms)),
            hint=(
                "Allocated 且连续无任何权威分配记录引用超过阈值,台账确认出身本权威,"
                "已按 UID+resourceVersion 精确回收;请追查该 GS 当初为何未被正常释放"
            ),
        )

    def _announce_candidates(
        self,
        candidates: list[orphan_reclaim.GameServerInfo],
        fleet_by_key: dict[str, str],
        now_ms: int,
    ) -> None:
        """首见候选打一条 Warn(每个 name/uid 只打一次)。Go 在 plan 循环里内联打这条。

        ★ 这条 Warn 是「有 GS 开始被观察」的唯一线索,排查泄漏来源时第一眼就要看它;
          `plan_round` 的返回值里没有"首见"这一类(它只回要处置的决策),所以由编排层
          按候选集补。
        """
        for gs in candidates:
            key = _gs_key(gs)
            if key in self._announced_ms:
                continue
            self._announced_ms[key] = now_ms
            plog.get().warning(
                "orphan_allocated_gs_candidate",
                gameserver=gs.name,
                uid=gs.uid,
                fleet=fleet_by_key.get(key, ""),
                allocation_id=gs.allocation_id,
                reclaim_after=godur.duration_string(
                    _dt.timedelta(seconds=self._reclaimer.reclaim_after_sec)
                ),
            )


def _to_reclaim_info(gs: AllocatedGameServerInfo) -> orphan_reclaim.GameServerInfo:
    """`agones_allocator` 的快照 → `orphan_reclaim` 的判定入参。

    `fleet` 只用于日志,不参与判定,故**不进**判定结构 —— 判定结构里多一个字段就多一个
    "有人拿它当判据"的入口。
    """
    return orphan_reclaim.GameServerInfo(
        name=gs.name,
        uid=gs.uid,
        allocation_id=gs.allocation_id,
        deleting=gs.deleting,
    )


def _gs_key(gs: orphan_reclaim.GameServerInfo) -> str:
    """与 `orphan_reclaim._key` 同构:`name + "/" + uid`,**不能只用 name**。

    GameServer 名字会被复用(同名 GS 删掉后 Fleet 重建可能拿到同一个名字),但 UID
    必定是新的。只用 name 的后果:重建出来的**活着的** GS 直接继承前世的观察起点,
    第一轮对账就满足"已连续观察 10 分钟"而被删。
    """
    return f"{gs.name}/{gs.uid}"


def _rfc3339(ms: int) -> str:
    """墙钟毫秒 → RFC3339(UTC)。对应 Go 的 `first.Format(time.RFC3339)`。

    Go 的 RFC3339 输出不带小数秒,故这里截到秒;时区固定 UTC(事故时间线一律 UTC)。
    """
    return (
        _dt.datetime.fromtimestamp(ms / 1000, tz=_dt.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
