"""孤儿 Allocated GameServer 对账清扫 —— 对应 Go 侧 internal/biz/orphan_gameserver.go。

# 问题

Agones 生命周期**不回收** Allocated GameServer。一台 GS 若处于 Allocated 却在权威存储里
没有任何分配记录引用(记录已释放但外部删除失败且响应丢失、手工 GSA、历史残留),
它会**永久占位**锁死 Fleet 容量(实例:泄漏 18h 把 min=max=2 的匹配池锁死)。

而「人工确认无人后删除」已**两次误删载人 DS** —— 人肉路径必须从流程上废除。

# 判定链:为什么「无记录 ⇒ 不可能有玩家」

两个半边的依据**不同,缺一不可**:

    ①「不可能再进来」由**时长数学**保证
       玩家进入战斗 DS 的唯一通道是 DSTicket,票据只在持有分配记录时签发,
       而记录**先于** GSA POST 落库。故连续 reclaim_after(默认 10min,
       ≫ 票据硬上限 180s + ready_wait 120s)每轮都查无记录的 GS,
       期间签不出、也不存在仍有效的进场票据。

    ②「不可能已在内」**不由**时长数学保证,而由**心跳停机契约**闭合
       记录消失后 DS 下一拍心跳被拒 → 停心跳并 Shutdown;心跳打不通时
       DS 按 §9.22 自我 fencing(失租即停玩)。
       ★ 谁要放松 UE DS 的「心跳被拒/失联即停机」行为,等于拆掉本清扫②的
       安全依据,必须连这份代码一起重新论证。

# 四重防误删

    ① 证据不可得 = 不删
    ② 候选期跨轮观察(首见只登记)
    ③ exact 复核删除(UID + resourceVersion 双 precondition)
    ④ 权威出身台账(让"权威视图分裂"机制上安全,不靠配置纪律)

第 ④ 层最关键:被删 GS 必须携带 allocation-id label,且该值能在**本权威**的
台账里查到。一个读到空/错配 Redis 的进程台账必然为空 ⇒ **一台都删不掉**;
真正由本权威分配后泄漏的 GS 台账必然有记录 ⇒ 照常回收。
"""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Callable

# 处置结果(与 Go 侧 metric label 一致 —— Grafana 面板按它分组)。
RESULT_RECLAIMED = "reclaimed"  # 回收成功
RESULT_SKIPPED = "skipped"  # exact 复核失效(409 等)
RESULT_FAILED = "failed"  # 删除失败
RESULT_UNPROVABLE = "unprovable"  # 台账查无,保留不删

# 连续观察多久才允许回收。必须 ≫ 票据硬上限(180s)+ ready_wait(120s)。
# 对齐 Go orphanGSReclaimAfterDefault = 10 * time.Minute。
DEFAULT_RECLAIM_AFTER_SEC = 600
# 观察阈值**下限**,对齐 Go orphanGSReclaimAfterFloor = 5 * time.Minute。
# 为什么必须钳制:阈值是判定链①唯一的时长依据,它要覆盖的不只是票据 180s +
# ready_wait 120s,还有「跨多轮对账 + 控制面观察余量」。不钳制的后果:谁在
# yaml 里填了 30s(手滑、照抄别的 sweep 项),进程会安静地按 30s 回收 ——
# 一台刚分配、玩家正拿着有效票据在进的 DS 就可能在进场途中被删。
# 误配不能靠纪律拦,必须在代码里拦。
RECLAIM_AFTER_FLOOR_SEC = 300
# 单轮删除尝试上限,对齐 Go orphanGSMaxReclaimPerRound = 3。
# 这个数字不是性能参数而是**爆炸半径**:万一还存在未知的误删路径,封顶把
# 「一轮删光整个 Fleet」压成「每分钟最多 3 台」,留出人工发现并止损的窗口。
# 泄漏回收时效以小时计,封顶只推迟不丢失,压小几乎无代价。
DEFAULT_MAX_RECLAIM_PER_ROUND = 3
# 单轮墙钟预算,对齐 Go sweepRoundBudget()(默认 sweep_interval = 5s)。
# 为什么要:对账轮与 §9.4 判弃链跑在**同一协程**,孤儿轮里每处置一台都是两次
# 外部调用(GET 复核 + DELETE)。没有预算时一轮孤儿处置能把判弃链饿死,失联
# DS 的判弃被无限推迟 —— 清扫本身就变成了新的故障源。
DEFAULT_ROUND_BUDGET_SEC = 5.0


@dataclasses.dataclass(slots=True)
class GameServerInfo:
    name: str
    uid: str
    resource_version: str = ""
    # pandora.dev/allocation-id label。缺失 = 手工 GSA / 台账上线前的存量。
    allocation_id: str = ""
    # deletionTimestamp 非空:删除已受理,处于终止宽限,无需再处理。
    deleting: bool = False


@dataclasses.dataclass(slots=True)
class GameServerRefs:
    """一轮对账中权威分配记录对 GameServer 的**全部**引用。

    三个维度缺一不可,对齐 Go battleGameServerRefs(pods / uids / allocs)。
    退化成"只按名字比对"的后果:分配记录三个字段是不同写入路径分别填的,
    存在只有 uid 或只有 allocation_id 的记录;少一维就等于把这些**活跃对局**
    误判成孤儿 —— 误判方向是删,正是本清扫唯一不可接受的错误方向。
    """

    pod_names: set[str] = dataclasses.field(default_factory=set)
    uids: set[str] = dataclasses.field(default_factory=set)
    allocation_ids: set[str] = dataclasses.field(default_factory=set)

    def references(self, gs: GameServerInfo) -> bool:
        # 空值不参与匹配:记录侧同样可能是空串,空对空会把毫不相干的 GS 判成
        # 被引用,让真正的泄漏永远回收不掉。
        if gs.name and gs.name in self.pod_names:
            return True
        if gs.uid and gs.uid in self.uids:
            return True
        if gs.allocation_id and gs.allocation_id in self.allocation_ids:
            return True
        return False


@dataclasses.dataclass(slots=True)
class Decision:
    gs: GameServerInfo
    result: str
    reason: str


class OrphanReclaimer:
    """孤儿回收的**决策**部分(纯逻辑;实际删除由编排层执行)。

    firstSeen 表是**进程内非权威调度提示**:重启即清空,效果只是把回收推迟
    一个观察期,方向安全。
    """

    __slots__ = (
        "_first_seen",
        "_reclaim_after_sec",
        "_max_per_round",
        "_round_budget_sec",
    )

    def __init__(
        self,
        *,
        reclaim_after_sec: int = DEFAULT_RECLAIM_AFTER_SEC,
        max_per_round: int = DEFAULT_MAX_RECLAIM_PER_ROUND,
        round_budget_sec: float = DEFAULT_ROUND_BUDGET_SEC,
    ) -> None:
        # 键是 "{name}/{uid}" 而不是裸 name,理由见 _key()。
        self._first_seen: dict[str, float] = {}
        self._reclaim_after_sec = _clamp_reclaim_after(reclaim_after_sec)
        self._max_per_round = max_per_round
        self._round_budget_sec = round_budget_sec

    @property
    def reclaim_after_sec(self) -> int:
        """生效的观察阈值(已按下限钳制),供编排层打日志/告警用。"""
        return self._reclaim_after_sec

    def observation_count(self) -> int:
        return len(self._first_seen)

    def reset_all_observations(self, now_sec: float) -> None:
        """★ 防误删 ①的后半条:**证据中断即重新起算**。

        权威记录读失败的轮次,把全部候选的观察起点重置为当前时刻 ——
        保证「连续每轮都有完整证据核验」严格成立,而不是让墙钟静默推进。

        不重置的后果:Redis 抖动 10 分钟(期间一轮证据都没拿到),
        抖动恢复后所有候选**立刻**满足"已观察 10 分钟"而被删 ——
        那 10 分钟里没有任何证据支持它们无人。
        """
        for key in self._first_seen:
            self._first_seen[key] = now_sec

    def plan_round(
        self,
        *,
        allocated: list[GameServerInfo],
        refs: GameServerRefs,
        ledger_allocation_ids: set[str],
        now_sec: float,
        elapsed_sec: Callable[[], float] | None = None,
    ) -> list[Decision]:
        """规划一轮的处置。返回**打算**删除/保留的决策列表。

        allocated             当前 Agones 里所有 Allocated 状态的 GS
        refs                  权威存储里分配记录对 GS 的三维引用
        ledger_allocation_ids 本权威 allocation 台账里的全部 allocation-id
        elapsed_sec           返回本轮已耗墙钟秒数的回调。编排层是边规划边执行
                              删除的(每台一次 GET 复核 + DELETE),真实耗时只有
                              它知道;缺省用本函数入口起算的单调钟(纯规划下恒
                              ≈0,等价于只有条数封顶生效)。
        """
        if elapsed_sec is None:
            started = time.monotonic()

            def elapsed_sec() -> float:
                return time.monotonic() - started

        decisions: list[Decision] = []
        attempts = 0
        candidates: set[str] = set()
        for gs in allocated:
            # 身份不全 = 做不了 exact 复核删除(要 name + UID 双 precondition);
            # 已进入终止宽限的不必再处理。两类都不进候选集,末尾修剪会清掉它们
            # 遗留的观察起点,免得一台正在删除的 GS 长期占着首见表。
            if not gs.name or not gs.uid or gs.deleting:
                continue
            key = _key(gs)
            if refs.references(gs):
                # 有权威引用:健康 Allocated,出候选(若曾入选)。
                self._first_seen.pop(key, None)
                continue

            candidates.add(key)
            first = self._first_seen.get(key)
            if first is None:
                # ★ 首见只登记,不删。
                self._first_seen[key] = now_sec
                continue
            if now_sec - first < self._reclaim_after_sec:
                continue  # 观察期未满

            # ★ 防误删 ④:必须能在**本权威**台账里证明出身。
            if not gs.allocation_id:
                decisions.append(
                    Decision(
                        gs,
                        RESULT_UNPROVABLE,
                        "GS 无 allocation-id label(手工 GSA 或台账上线前的存量)—— 保留不删",
                    )
                )
                continue
            if gs.allocation_id not in ledger_allocation_ids:
                decisions.append(
                    Decision(
                        gs,
                        RESULT_UNPROVABLE,
                        "allocation-id 不在本权威台账中 —— 疑似权威视图分裂,保留不删",
                    )
                )
                continue

            # ★ 资源占用防护:条数封顶 + 墙钟预算,超限的留在首见表下轮继续
            # (表跨轮持久,只推迟不丢失)。这里必须 continue 而不是 break ——
            # 排在后面的 GS 还要登记首见时间、还要进候选集,提前跳出会让它们
            # 每轮都被当成首见重新计时,永远等不到阈值,泄漏就再也回收不掉。
            # 预算只在**已经尝试过一次之后**才拦(attempts > 0):否则轮一开局
            # 就超预算时会一台都删不成,清扫彻底失效。
            if attempts >= self._max_per_round or (
                attempts > 0 and elapsed_sec() >= self._round_budget_sec
            ):
                continue
            attempts += 1
            decisions.append(
                Decision(gs, RESULT_RECLAIMED, "连续观察期内每轮均无引用,且台账可证出身")
            )

        # 修剪:不再是候选(GS 消失/转态/获得引用/进入删除宽限)的项移出首见表,
        # 表容量上界 = 当前候选数(§9.18 进程内容器有界纪律)。
        for key in list(self._first_seen):
            if key not in candidates:
                del self._first_seen[key]

        return decisions

    def on_reclaim_outcome(self, gs: GameServerInfo, outcome: str) -> None:
        """把**删除的实际结果**回填进首见表。编排层每执行完一条 RECLAIMED 决策必须调它。

        对应 Go orphan_gameserver.go:309-329 的三个分支。少了这一步,
        `plan_round` 就成了一个"只会说删、从不知道删没删成"的规划器:

            skipped(exact 复核失效)  → Go `delete(firstSeen, key)`,**作废候选、
              重新观察满一个完整窗口**。复核失效意味着这台 GS 的 resourceVersion 变了
              —— 它刚发生过变更,很可能**已经被重新分配出去了**。不作废的话,首见时间
              还停在很久以前,下一轮阈值早已满足,会**立刻再发一次删除** ——
              而这次它可能已经载人了。这正是本文件要防的那类事故(§9「绝不删
              Allocated GameServer」,已有两次)。
            reclaimed(删成功)        → 同样删键(候选已经不存在了)。
            failed(调用本身失败)     → **保留键**,首见时间不变,下轮按幂等重试。
              这一格与上面两格方向相反:调用没打出去,对象状态未知但也未被证伪,
              继续观察是安全的。

        方向写反的后果都不报错:该作废的没作废 = 删活着的 DS;
        该保留的删了 = 一次网络抖动就把观察窗口整个重置,孤儿永远回收不掉。
        """
        if outcome in (RESULT_RECLAIMED, RESULT_SKIPPED):
            self._first_seen.pop(_key(gs), None)
        elif outcome != RESULT_FAILED:
            raise ValueError(
                f"未知的回收结果 {outcome!r};必须是 "
                f"{RESULT_RECLAIMED}/{RESULT_SKIPPED}/{RESULT_FAILED}"
                " —— 静默忽略未知值会让首见表按错误的分支走"
            )


def _key(gs: GameServerInfo) -> str:
    """首见表的键必须是 name + "/" + uid,**不能只用 name**。

    对齐 Go orphan_gameserver.go:259。GameServer 名字会被复用:同名 GS 被删后
    Fleet 重建出的实例可能拿到同一个名字,但 UID 必定是新的。
    只用 name 的后果:重建出来的**活着的** GS 直接继承前世的观察起点,第一轮
    对账就满足"已连续观察 10 分钟"而被删 —— 删的是载人 DS,正是 CLAUDE.md §9
    「绝不删 Allocated GameServer」记着的那类事故。
    带上 UID 后,名字复用的新实例是一个全新的键,必须从头观察满一个完整窗口。
    """
    return f"{gs.name}/{gs.uid}"


def _clamp_reclaim_after(sec: int) -> int:
    """观察阈值钳制,对齐 Go orphanGSReclaimAfter()(orphan_gameserver.go:133-142)。

    0/负值(未配置)走默认;低于下限的配置钳到下限,而不是原样接受。
    """
    if sec <= 0:
        return DEFAULT_RECLAIM_AFTER_SEC
    if sec < RECLAIM_AFTER_FLOOR_SEC:
        return RECLAIM_AFTER_FLOOR_SEC
    return sec


def all_candidates_unprovable(decisions: list[Decision], allocated_count: int) -> bool:
    """★ 「查无出身的候选数 ≈ 全部 Allocated」是**权威视图分裂**的强信号。

    正常情况下泄漏的 GS 是少数;若几乎每一台都查不到出身,
    更可能是本进程的 Redis 指向了错误/空的权威,而不是集群真的全泄漏了。
    这时应当优先排查配置,而不是"想办法把它们删掉"。
    """
    if allocated_count == 0:
        return False
    unprovable = sum(1 for d in decisions if d.result == RESULT_UNPROVABLE)
    return unprovable >= allocated_count
