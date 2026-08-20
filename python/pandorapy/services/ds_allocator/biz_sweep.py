"""ds_allocator 业务层**心跳超时清扫 / 单写者 leader / 活跃索引对账 / abandoned 投递**
—— 对应 Go 侧 `services/battle/ds_allocator/internal/biz/allocator.go` 第 3359 行到文件末
(3359–4048)。

覆盖的 Go 函数(逐个对应,顺序一致):

    SetSweepWriterLease / sweepIsLeader / RunHeartbeatSweep / heartbeatSweepTick
    noteSweepDeferral / sweepDeferralActive / pruneSweepDeferrals / stuckReconcileState
    sweepRoundBudget / sweepOnce / reconcileActiveIndexIfDue / deliverAbandoned
    rosterGateArmable

最终组装(见同目录 `biz.py`):

    class AllocatorUsecase(SweepMixin, HeartbeatMixin, ReleaseMixin,
                           AllocateMixin, AllocatorUsecaseBase): ...

字段与 helper **全部**来自 `AllocatorUsecaseBase` / `ReleaseMixin` / 已落地的数据层模块,
本文件一个都不重复实现。

═══════════════════════════════════════════════════════════════════════════════
这批的失败形状:**静默停摆**,不是报错
═══════════════════════════════════════════════════════════════════════════════

RPC 出错客户端立刻看得见;后台循环死掉**没有任何人看得见** —— 进程还在、端口还在、
health 还答 SERVING,只是:

  - 心跳超时不再把失联对局标 abandoned(§9 不变量 4「DS 崩溃必有补偿」断链),
    玩家的段位回滚 / match 释放永远不发生,人卡在一局已经不存在的战斗里;
  - 派生 active ZSET 不再从权威记录重建,一次丢失的 ZADD 就让某个永久墓碑
    **永远**没人扫到(对应的 GameServer 永久占位);
  - abandoned 的 lifecycle 事件不再投递,而它正是 battle_result 侧补偿的唯一入口。

所以本模块对 `safego` 的依赖不是"顺手":**每一轮 tick 必须独立兜底**,单轮异常只
终止本轮,循环整体存活(移植要求 12 的直接适用对象)。

═══════════════════════════════════════════════════════════════════════════════
§16.10 与 INC-20260724-001:退避**只能**记在进程内
═══════════════════════════════════════════════════════════════════════════════

`sweep_defer_until` 是队头公平性的**进程内非权威调度提示**。它绝不能写进 active
ZSET 的 score —— 三条硬约束(逐条抄自 Go 注释,任何一条被破坏都不会报错):

  ① score 的权威语义是 `last_heartbeat_ms`。挪作调度时间戳会让
     「score ≤ 阈值 ⇔ 心跳超时」这条判据整体失效。
  ② abort fence 与 auth quarantine 用 **score==0** 表示「下一轮 sweep 必须立即
     对账 / 回收」,派生索引重建专门用 `ZADD NX` 保护这个哨兵。写退避会把这个
     最高危状态的**立即**对账降级成**延迟**对账。
  ③ `touch_active` 是无条件 `ZADD`(无 NX/XX/GT)。迟到的退避写会把已被
     `remove_active` 的终态项**复活**回补偿 outbox,重复投递 lifecycle。

进程内表还顺带保住了 `run_heartbeat_sweep` 开头「重启即扫」的既定意图(重启即清空)。

同一条纪律的判别口诀(§16.10):**到期后"假设成功"的是掩盖,到期后"重查权威"的
是合法兜底**。本模块的每一处到期动作都是后者:

  - 退避到期 → 重新 `get_battle` / 重发 exact DELETE 并等物理消失确认,不是
    "等够 15s 就认为回收成功了";
  - 单轮墙钟预算到点 → 未处理项**留在 active ZSET**(outbox 语义),下一 tick
    重新 `range_stale_battles`,不是"跳过就算处理过";
  - 活跃索引重建节流到点 → 从 canonical 记录**重新 SCAN**,不是复用上轮结论。

═══════════════════════════════════════════════════════════════════════════════
与 Go 的形变(逐条都有理由)
═══════════════════════════════════════════════════════════════════════════════

  1. `ticker + for/select` → `safego.loop(name, interval, fn)`。语义逐条对齐:
     首轮在第一个 tick **之后**执行、慢轮丢 tick 不堆积、单轮异常不终止循环。
     Go 的 `defer safego.Recover(ctx, "ds_heartbeat_sweep")` 由 `safego.loop` 内部的
     `run_once` 用**同名点位**提供,本模块**不再自己兜一层** —— 兜两层会让外层永远
     收不到异常,变成假的防线。

  2. **两个时钟,不是笔误**。
     `now_ms()`(墙钟毫秒)只用于两个 stale cutoff:它们要和 Redis 里各副本写入的
     `last_heartbeat_ms` 比较,必须同源。
     `time.monotonic()`(单调秒)用于单轮预算、退避到期、索引重建节流:NTP 回拨
     一次就能让整表退避瞬间"到期"或永远不到期。Go 的 `time.Time` 同时带墙钟与
     单调读数(`time.Since` 走单调),Python 没有这种复合类型,只能显式分开。

  3. `(值, error)` → 抛异常。`sweep_once` / `reconcile_active_index_if_due` 在 Go 里
     返回 error 供 tick 记日志,这里改成抛出、由 tick 的 try/except 记**同名**事件。
     用 bool 表达控制流的 `deliver_abandoned` / `sweep_is_leader` 照抄二元语义。

  4. **`TerminateExpected` 的 `(bool, error)` 在 Python 是"bool + 抛"**。Go 里
     `(true, err)` 这一组合表示"永久 fence 已提交、只是派生索引清理失败",故 Go 多打
     一条 `model_b_sweep_index_cleanup_failed`。Python 的 `terminate_expected` 对任何
     失败都抛异常、成功恒返回 True 且无残留 error,该组合**不可达** —— 因此本文件
     刻意**没有**这条事件,而不是写一段永远进不去的死代码(§15.5)。
     `expire_terminated_expected` / `read_allocation_abort` 的 `(值, error)` 同理收敛成
     `err is not None or not ok`,与 Go 的判定式逐字等价。

  5. **`u.sweepDeferUntil == nil` 的懒初始化删掉**。Go 的 map 零值是 nil,必须先
     `make`;`AllocatorUsecaseBase.__init__` 已经把它建成 `{}`,保留那段只会写出一行
     永远为假的死代码(与基座删掉 `u == nil` 判空同因)。

  6. **`BattleStaleCutoffs` 是 frozen dataclass**,不能像 Go 那样先构造再
     `cutoffs.WarmingForfeit = &...`。这里改成先把 probe 结果收进局部变量、最后**一次**
     构造 —— 求值顺序与可观察行为完全一致(probe 仍在 `abandon_if_stale` 之前发生)。

  7. **`roster_gate_armable` 不在本文件重新实现**。它已随 `battle_auth.py` 落地
     (那里是 `activate_heartbeat` 事务内的实际判据处),本模块只从那里 re-export ——
     两份副本漂移时**两边都不报错**,只会让「开局没到齐」的判弃闸在两条路径上给出
     不同答案。
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime as _dt
import time
from typing import Any

from pandora.ds.v1 import allocator_pb2 as dspb

from pandorapy import errcode, godur, placement, safego
from pandorapy import log as plog
from pandorapy.services.ds_allocator.agones_allocator import (
    AuthoritativeGameServerAllocation,
    ReleaseDeletionPendingError,
)
from pandorapy.services.ds_allocator.battle_auth import (
    BattleExpectedInstance,
    BattleStaleCutoffs,
    BattleWarmingForfeit,
    now_ms,
    roster_gate_armable,
)
from pandorapy.services.ds_allocator.biz_base import (
    ACTIVE_INDEX_RECONCILE_INTERVAL_SEC,
    REASON_ACTIVE_INDEX_REBUILD,
    REASON_ACTIVE_INDEX_UNAVAILABLE,
    STATE_ABANDONED,
    STATE_ALLOCATING,
    STATE_ALLOCATION_ABORT,
    STATE_ALLOCATION_EMPTY_FENCE,
    STATE_ALLOCATION_RECONCILING,
    STATE_ALLOCATION_UNCERTAIN,
    STATE_ENDED,
    STATE_PREACTIVE_RELEASING,
    STATE_WARMING,
    UPDATE_MAX_RETRY,
    SweepWriterLease,
)
from pandorapy.services.ds_allocator.biz_release import (
    OWNER_RELEASE_BUDGET_SEC,
    PreactiveReleaseOutcome,
)
from pandorapy.services.ds_allocator.gameserver import WarmingInstanceProber
from pandorapy.services.ds_allocator.orphan_gameserver import OrphanGameServerJanitor
from pandorapy.services.ds_allocator.owner_authority import (
    OWNER_ADMITTED_STALE_TTL_SEC,
    owner_release_abandoned_players_weak,
    sweep_stale_owner_admitted,
)

__all__ = [
    "ACTIVE_INDEX_RECONCILE_SCAN_COUNT",
    "DEFAULT_SWEEP_DEFER_BACKOFF_SEC",
    "DEFAULT_SWEEP_ROUND_BUDGET_SEC",
    "RELEASE_PENDING_DEFER_PREFIX",
    "SWEEP_TASK_NAME",
    "WARMING_PROBE_DEFER_PREFIX",
    "SweepDeferral",
    "SweepMixin",
    "roster_gate_armable",
    "stuck_reconcile_state",
]

# `safego` 的点位名。**必须与 Go 的 `safego.Recover(ctx, "ds_heartbeat_sweep")` 逐字节
# 一致**:它是 `pandora_safego_panic_recovered_total{name}` 的 label 值,两栈并存期改
# 一个字母,告警规则就只覆盖到一半的副本(而"少了一半"本身没有任何提示)。
SWEEP_TASK_NAME = "ds_heartbeat_sweep"

#: warming 判死 probe 失败的退避键前缀,后接 `allocation_id`。Go: `warmingProbeDeferPrefix`。
#:
#: 退避绑定到**被探测的那次分配身份**:同 match_id 的新分配(不同 allocation_id)键不同,
#: 天然不继承旧退避;记录状态真正变化时也会因 state 不匹配立即作废
#: (见 `SweepMixin.sweep_deferral_active`)。
WARMING_PROBE_DEFER_PREFIX = "warming-probe:"

#: abandoned 外部回收**任何未确认结果**后的退避键前缀,后接 `allocation_id`。
#: Go: `releasePendingDeferPrefix`。
#:
#: 覆盖删除宽限、GET/DELETE 超时、控制面持续故障、pod UID preflight 失败四类 ——
#: 复审必修:不只 deletion-pending,普通 5s 超时同样会让最老项每轮吃满预算,
#: INC-20260724-001 的队头饥饿必须**全类**根除。
RELEASE_PENDING_DEFER_PREFIX = "release-pending:"

#: `heartbeat_timeout` 非正时的退避窗兜底(秒)。Go: `noteSweepDeferral` 里的 `15 * time.Second`。
#: 退避窗**复用既有 HeartbeatTimeout**(默认 15s = 3 个 SweepInterval),不新增配置项。
DEFAULT_SWEEP_DEFER_BACKOFF_SEC = 15.0

#: `sweep_interval` 非正时的单轮墙钟预算兜底(秒)。Go: `sweepRoundBudget` 的 `5 * time.Second`。
DEFAULT_SWEEP_ROUND_BUDGET_SEC = 5.0

#: 派生 active 索引重建单次 SCAN 的批量。Go: `ReconcileBattleActiveIndex(ctx, 256)` 的字面量。
ACTIVE_INDEX_RECONCILE_SCAN_COUNT = 256


def _duration_ms(td: _dt.timedelta) -> int:
    """`time.Duration.Milliseconds()` 的逐位等价(**向零截断**)。

    ★ 不写成 `int(total_seconds() * 1000)`:那会在 `2.5h` 这类值上被浮点尾数咬掉
      1ms(与 `AllocatorUsecaseBase.heartbeat_timeout_ms` 同因)。这里算出来的是
      判弃阈值,边界上差 1ms 就会让"到底该不该判弃"在两栈之间给出不同答案。
    """
    return int(td / _dt.timedelta(milliseconds=1))


async def _discard_error(coro: Any) -> None:
    """对应 Go 的 `_ = f(ctx, ...)`:**显式**丢弃派生索引维护的错误。

    调用点全是派生 active ZSET 的自愈性维护(移除残留项 / 修 score)。它们失败时
    下一轮 sweep 必然重来,而在这里冒泡会中断整轮 §9.4 补偿链 —— 队尾那些**能**
    收敛的对局会被一个无关的 ZSET 抖动拖着一起不推进。

    ★ 刻意**不**打日志:Go 侧这几处就是裸 `_ =`,新增一个事件名会让两栈的 Loki
      面板对不上(移植硬性要求 8)。真正需要看见的失败都在各自的 WARN 分支里。
    ★ `CancelledError` 必须放行:它是停机控制流,吞掉会让循环退不出去。
    """
    try:
        await coro
    except asyncio.CancelledError:
        raise
    except BaseException:  # noqa: BLE001 —— 对齐 Go 的 `_ =`,见 docstring
        return


@dataclasses.dataclass(frozen=True, slots=True)
class SweepDeferral:
    """一次队头退避的记录。Go: `sweepDeferral`。

    `state` 参与失效校验:记录状态一变(被并发 RPC 推进、或已进入 §9.4 补偿链、
    或 abort fence / auth quarantine 要求立即对账)立即作废退避,不让调度优化拖慢
    真正的终态收敛。

    `until` 是**单调**秒(`time.monotonic()`),不是墙钟 —— 见模块头「两个时钟」。
    """

    state: str
    until: float


def stuck_reconcile_state(state: str) -> bool:
    """列出「靠重试收敛、外部依赖不可用时会原地打转」的 sweep 状态。Go: `stuckReconcileState`。

    这些项在每轮被处理前先退避、让出队头,避免饿死队尾的 §9.4 abandoned 补偿
    (INC-20260724-001:事故当天控制面超时约 40s,这几种状态的 active score 不变
    ⇒ 恒排在 `range_stale_battles`(无 LIMIT、score 升序)结果最前面,串行吃掉整轮预算)。

    ★ 刻意**不含** `abandoned + instance_epoch==0` 的 resume 分支:那条本身就是 §9.4
      补偿的最后一棒,要保持最高优先级重试;它不产生外部 GSA POST,单次耗时也远小于
      上面几种。把它加进来 = 把补偿链最后一步也推迟 15s。
    """
    return state in (
        STATE_ALLOCATION_UNCERTAIN,
        STATE_ALLOCATION_RECONCILING,
        STATE_ALLOCATION_EMPTY_FENCE,
        STATE_PREACTIVE_RELEASING,
        STATE_ALLOCATION_ABORT,
    )


class SweepMixin:
    """`AllocatorUsecase` 的心跳超时清扫链。对应 Go allocator.go 3359–4048。

    ★ 刻意**不**加 `__slots__`(与 `AllocatorUsecaseBase` / 其余 mixin 同因):多 mixin
      叠加时 slots 布局要在每层重复声明,漏一层就静默退回 `__dict__`,反而制造
      "以为有约束其实没有"的假象。
    """

    # 下面这些由 AllocatorUsecaseBase / ReleaseMixin 提供,这里只声明给读者看:
    #   repo / alloc / cfg / auth_repo / abort_repo / lifecycle_proof_repo
    #   authoritative_alloc / active_index_reconciler / lifecycle / lifecycle_required
    #   model_b / ds_credential_ttl_sec / owner_auth / owner_admitted
    #   sweep_lease / sweep_lease_held / sweep_defer_until / last_active_index_reconcile
    #   orphan_gs_reconciler / allocation_ledger
    #   battle_ttl_sec() / ready_wait_timeout_sec() / heartbeat_timeout_ms()
    #   kill_stranded_ds()
    #   reconcile_allocation_uncertain() / resume_empty_allocation_tombstone()
    #   resume_reconciled_allocation_abandoned() / reconcile_preactive_release()
    #   abort_preactive_battle() / ensure_durable_release_pod_uid() / release_game_server()

    # ── 领导权 ────────────────────────────────────────────────────────────

    def set_sweep_writer_lease(self, lease: SweepWriterLease | None) -> None:
        """注入心跳扫描的领导权来源;`None` = 不启用(本副本无条件扫描)。
        对应 Go 的 `SetSweepWriterLease`。

        只允许在**启动装配期**调用(`run_heartbeat_sweep` 之前);之后只读。
        """
        self.sweep_lease = lease

    def sweep_is_leader(self) -> bool:
        """判定本轮是否由本副本执行扫描,并把领导权跃迁打成日志。对应 Go 的 `sweepIsLeader`。

        未注入租约时恒 `True`(单副本 Recreate 的历史形态)。

        ★ 日志只在**跃迁**时打,不是每 tick 打:sweep 每 5s 一轮,每轮一条会把同文件里
          的 WARN 冲走(§11.3 R4)。
        """
        if self.sweep_lease is None:
            return True
        token, held = self.sweep_lease.current()
        if held != self.sweep_lease_held:
            self.sweep_lease_held = held
            if held:
                plog.get().info("heartbeat_sweep_leadership_acquired", token=token)
            else:
                # 让位不是故障:热备副本继续服务 Heartbeat / AllocateBattle,只是不扫描。
                # 真正的异常是"全集群无人持有",由 writerlease Health()/指标暴露。
                plog.get().info(
                    "heartbeat_sweep_leadership_released",
                    hint="本副本转热备,仅暂停心跳超时扫描;RPC 路径不受影响",
                )
        return held

    # ── 循环 ──────────────────────────────────────────────────────────────

    async def run_heartbeat_sweep(self) -> None:
        """启动后台心跳超时扫描,直到被取消(§9 不变量 4)。对应 Go 的 `RunHeartbeatSweep`。

        ★ 必须走 `safego.loop` 而不是自建 `while True` + `create_task`:裸 task 的异常
          会被存进 Task 对象等人来取,没人取就**零日志**地躺着 —— 进程照跑、端口照
          服务、health 照答 SERVING,而 §9.4 这条补偿链已经死了。

        `interval <= 0` 由 `safego.loop` 自身挡住(打 `safego_loop_invalid_interval` 后
        立即返回,防误配自旋),这里不重复判。
        """
        interval = self.cfg.sweep_interval_td().total_seconds()
        plog.get().info(
            "heartbeat_sweep_started",
            # Go 是 `config.Duration.String()`(= Go duration 串,如 "5s");
            # `godur.duration_string` 是它的逐字等价,不能改用原始配置串
            # ——后者可能是空("")或 "5000ms" 之类的等价写法,两栈日志就对不上了。
            interval=godur.duration_string(self.cfg.sweep_interval_td()),
            timeout=godur.duration_string(self.cfg.heartbeat_timeout_td()),
            writer_lease_wired=self.sweep_lease is not None,
        )
        # 进程启动即恢复派生索引。等第一个 tick 会让"丢失的永久墓碑"在**每次重启后**
        # 都有整整一个 sweep 间隔不可见。
        #
        # 接了继任租约时首扫必须过领导权门:竞选是异步的,启动瞬间通常尚未当选,此时
        # 无条件首扫等于在滚动升级重叠窗口里多一个并发扫描者。跳过无损 —— 当选后的
        # 第一个 tick 就会补上,间隔仅一个 SweepInterval(默认 5s)。
        if self.sweep_is_leader():
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                # ★ 必须紧邻宽 except 之上:取消是停机控制流,不是"首扫失败"。
                raise
            except BaseException as exc:  # noqa: BLE001 —— 首扫失败不阻断循环启动
                plog.get().warning("heartbeat_initial_sweep_failed", err=str(exc))
        try:
            await safego.loop(SWEEP_TASK_NAME, interval, self.heartbeat_sweep_tick)
        except asyncio.CancelledError:
            # Go 在 `<-ctx.Done()` 分支里打这条后 return;Python 必须**再抛出去**,
            # 否则停机时 `server.run()` 的 gather 等不到这个任务结束。
            plog.get().info("heartbeat_sweep_stopped")
            raise

    async def heartbeat_sweep_tick(self) -> None:
        """单个清扫 tick。对应 Go 的 `heartbeatSweepTick`。

        Go 在函数头 `defer safego.Recover(ctx, "ds_heartbeat_sweep")`,使 recover 作用域
        恰为一轮;Python 侧这层由 `safego.loop` 的 `run_once` 用**同名点位**提供,
        本函数不再自己兜底 —— 兜两层会让第二层永远收不到异常,变成假的防线。
        """
        # census 准入缓存按 last-touch TTL 清死实例项(对齐 hub_allocator)。纯本地内存
        # 卫生,不经存储、不依赖 writer 身份,故每 tick **无条件先执行** —— 否则本副本
        # 处理过心跳的已销毁 Battle 实例(UID 永不复用)留下的 admitted 项永不回收,
        # 长压测 OOM(§9.18)。活实例项每心跳 census 续期,仅超 TTL 未续期的项被清。
        #
        # ★ cutoff 必须与写入侧同一时钟:`owner_admit_census_weak` 写进去的是
        #   `time.monotonic()`。混用墙钟会让整表要么永不老化、要么被整片误清。
        sweep_stale_owner_admitted(
            self.owner_admitted, time.monotonic() - OWNER_ADMITTED_STALE_TTL_SEC
        )
        # 领导权门排在本地内存卫生**之后**:热备副本照样服务 Heartbeat,其 census 缓存
        # 必须继续老化回收,否则热备期越长内存越涨。
        if not self.sweep_is_leader():
            return
        try:
            await self.sweep_once()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 单轮失败,下 tick 继续
            plog.get().warning("heartbeat_sweep_failed", err=str(exc))

    # ── 队头退避(进程内、非权威)──────────────────────────────────────────

    def note_sweep_deferral(self, match_id: int, state: str, now: float) -> None:
        """登记一次队头退避。对应 Go 的 `noteSweepDeferral`。

        退避窗复用既有 `heartbeat_timeout`(默认 15s = 3 个 SweepInterval),不新增配置项。

        ★ 只写**进程内** `sweep_defer_until`,绝不碰 active ZSET —— 三条硬约束见模块头。
        """
        backoff = self.cfg.heartbeat_timeout_td().total_seconds()
        if backoff <= 0:
            backoff = DEFAULT_SWEEP_DEFER_BACKOFF_SEC
        self.sweep_defer_until[match_id] = SweepDeferral(state=state, until=now + backoff)
        plog.get().debug(
            "allocation_sweep_head_of_line_deferred",
            match_id=match_id,
            state=state,
            backoff=godur.duration_string(_dt.timedelta(seconds=backoff)),
        )

    def sweep_deferral_active(self, match_id: int, state: str, now: float) -> bool:
        """判定该项本轮是否让出队头。对应 Go 的 `sweepDeferralActive`。

        到期、或状态已变(见 `SweepDeferral.state`)即**就地删除**并立即重试 ——
        删除是刻意的:留着一条已失效的记录会在同一 match 后续状态回到旧值时
        (补偿链会反复经过同一状态)被误认为"还在退避窗内"。
        """
        d = self.sweep_defer_until.get(match_id)
        if d is None:
            return False
        if d.state != state or now >= d.until:
            del self.sweep_defer_until[match_id]
            return False
        return True

    def prune_sweep_deferrals(self, now: float) -> None:
        """清掉已到期的退避项。对应 Go 的 `pruneSweepDeferrals`。

        防表随历史 match_id 无界增长(§9.18 进程内容器有界纪律;与同域的
        `sweep_stale_owner_admitted` 同源)。表容量上界 = 一个退避窗口内被判为
        「卡住墓碑」的对局数,远小于 active 集合。
        """
        expired = [mid for mid, d in self.sweep_defer_until.items() if now >= d.until]
        for mid in expired:
            del self.sweep_defer_until[mid]

    def sweep_round_budget(self) -> float:
        """单轮 sweep 的墙钟预算(秒)。对应 Go 的 `sweepRoundBudget`。

        上界口径(可断言):预算只在**开始下一项之前**检查,故单轮实际上界是
        `sweep_interval + 单项最坏耗时`,不是 `sweep_interval` —— 单项最坏 ≈
        LIST + get_pod + DELETE + 等待 expected 实例消失,各自受 `allocate_timeout` 约束。
        写清这一点是为了让验收断言写得出来,而不是放宽成无意义的"大概不会太久"。
        """
        d = self.cfg.sweep_interval_td().total_seconds()
        if d > 0:
            return d
        return DEFAULT_SWEEP_ROUND_BUDGET_SEC

    # ── 主扫描 ────────────────────────────────────────────────────────────

    async def sweep_once(self) -> None:  # noqa: C901, PLR0911, PLR0912, PLR0915 —— 与 Go 逐条对应
        """扫描一次:`last_heartbeat_ms` 早于阈值的战斗 → 标 abandoned + 回收 + 可靠补偿。
        对应 Go 的 `sweepOnce`。

        **可靠补偿(§9 不变量 4)**:把 active ZSET 自身当作补偿事件的 outbox ——
        abandoned 的对局在 `ds.lifecycle` 事件成功投递前**不移出 active**,故下一轮
        sweep 会再次命中并重试投递;只有投递成功(或显式 local/off 开发配置的
        best-effort 回退)才 `expire_battle` 移出 active。配合 battle_result 幂等消费
        (不变量 2),整条补偿链是 at-least-once 闭环,可穿越 Kafka 临时不可用。

        legacy 天然上界靠 `update_battle_keep_ttl`(KEEPTTL)。Model B 则在任何外部
        Release 前把 TERMINATING auth+battle 置为**永久**,只有 release 与 lifecycle
        投递都明确成功后才由 `expire_terminated_expected` 恢复有界 TTL;未知结果宁可
        不可用也不丢 fence。

        Raises:
            权威读 / 索引重建失败原样上抛,由 `heartbeat_sweep_tick` 记
            `heartbeat_sweep_failed` 并在下一 tick 重试(Go 是 `return err`)。
        """
        await self.reconcile_active_index_if_due()
        # 双阈值同一 now 计算:ACTIVE 用业务心跳超时,warming 冷加载用 ready 等待超时
        # (大图 ServerTravel→GameMode BeginPlay 前无业务心跳是正常行为,与
        # AllocateBattle 在途的 wait_battle_ready 同一口径)。`range_stale_battles` 仍用
        # active_cutoff 粗筛派生 ZSET(score 语义 = last_heartbeat_ms),状态对应的二次
        # 核验交给 `abandon_if_stale` 在 WATCH 事务内按权威快照完成 —— 外层读到的
        # state 会与首次 `activate_heartbeat` 并发,不得据其选单一阈值(TOCTOU)。
        now_wall_ms = now_ms()
        active_cutoff = now_wall_ms - self.heartbeat_timeout_ms()
        warming_cutoff = now_wall_ms - _duration_ms(self.cfg.ready_wait_timeout_td())
        stale = await self.repo.range_stale_battles(active_cutoff)
        round_start = time.monotonic()
        budget = self.sweep_round_budget()
        self.prune_sweep_deferrals(round_start)
        processed = 0
        for mid in stale:
            # 单轮墙钟预算(INC-20260724-001):控制面持续超时时单项可耗时数十秒,无预算
            # 的一轮会把下一 tick 直接叠上来,且队尾的 §9.4 abandoned 补偿被无限推后。
            # `processed > 0` 保证每轮至少推进一项(预算再小也不会活锁);未处理项留在
            # active ZSET,下一 tick 继续(outbox 语义,进程重启即恢复)。
            if processed > 0 and time.monotonic() - round_start >= budget:
                plog.get().warning(
                    "allocation_sweep_round_budget_exhausted",
                    processed=processed,
                    deferred_to_next_tick=len(stale) - processed,
                    budget=godur.duration_string(_dt.timedelta(seconds=budget)),
                    elapsed_ms=int((time.monotonic() - round_start) * 1000),
                )
                break
            processed += 1
            # 先于 authority-mode 分支识别永久 fence:这样同版本但仍跑 legacy 配置的
            # writer 也只读跳过,不能把 uncertain 改成 abandoned 后 Release/Delete。
            try:
                inflight = await self.repo.get_battle(mid)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 单项读失败不阻断整轮
                plog.get().warning("allocation_sweep_read_failed", match_id=mid, err=str(exc))
                continue
            found = inflight is not None
            # sweep 队头公平性(INC-20260724-001):`stuck_reconcile_state` 那几种是
            # 「永久墓碑 / 靠重试收敛」状态,外部依赖(k8s/Agones 控制面)持续不可用时
            # 它们永远收敛不了,而其 active score 不变 ⇒ 恒排在结果最前面,串行吃掉
            # 整轮预算。故让它们本轮让出队头。退避记在进程内(理由见模块头三条硬约束)。
            if found and stuck_reconcile_state(inflight.state):
                defer_key = inflight.state
                if inflight.state == STATE_PREACTIVE_RELEASING and inflight.allocation_id != "":
                    # preactive 墓碑与 release-pending 家族共用 allocation 键(复审 P1-2):
                    # abandoned→fence 成功→release 未确认会发生 abandoned→pending 的跨状态
                    # 迁移,若仍按状态键退避,上一轮登记的退避会被状态变化误判失效,回到
                    # 每轮重复外部调用;allocation_id 不随该迁移变化。
                    defer_key = RELEASE_PENDING_DEFER_PREFIX + inflight.allocation_id
                if self.sweep_deferral_active(mid, defer_key, round_start):
                    continue
                self.note_sweep_deferral(mid, defer_key, round_start)
            if found and inflight.state in (
                STATE_ALLOCATION_UNCERTAIN,
                STATE_ALLOCATION_RECONCILING,
            ):
                if self.model_b:
                    await self.reconcile_allocation_uncertain(inflight)
                else:
                    plog.get().debug(
                        "allocation_uncertain_retained_legacy_writer",
                        match_id=mid,
                        allocation_id=inflight.allocation_id,
                    )
                continue
            if found and inflight.state == STATE_ALLOCATION_EMPTY_FENCE:
                if self.model_b:
                    await self.resume_empty_allocation_tombstone(inflight)
                # 旧 writer 不认识这个状态,必须保持只读。
                continue
            if (
                self.model_b
                and found
                and inflight.state == STATE_ABANDONED
                and inflight.instance_epoch == 0
            ):
                # allocation_uncertain 对账已提交 ABANDONED,但在 Kafka ACK / Expire 之前
                # 崩溃。重新确认 exact 物理消失,然后续跑那条 durable 生命周期交接。
                #
                # §9.4 最后一棒仍保持高优先重试,但外部确认失败后按分配身份让出队头一个
                # 退避窗口(复审必修:控制面持续故障时 epoch=0 resume 的物理确认 GET 同样
                # 会每轮吃满预算;15s 退避只延后重试节奏,outbox 可靠补偿语义不变)。
                if inflight.allocation_id != "" and self.sweep_deferral_active(
                    mid, RELEASE_PENDING_DEFER_PREFIX + inflight.allocation_id, round_start
                ):
                    continue
                resumed = await self.resume_reconciled_allocation_abandoned(inflight)
                if not resumed and inflight.allocation_id != "":
                    self.note_sweep_deferral(
                        mid, RELEASE_PENDING_DEFER_PREFIX + inflight.allocation_id, round_start
                    )
                continue
            if found and inflight.state == STATE_PREACTIVE_RELEASING:
                # cleanup 在外部 DELETE 响应未知后留下的永久墓碑。重试只会再次做 UID 条件
                # Release 并在明确成功后 purge,不会再次 GSA POST。
                if self.model_b:
                    await self.reconcile_preactive_release(inflight)
                continue
            if found and inflight.state == STATE_ALLOCATION_ABORT:
                if not self.model_b or self.abort_repo is None:
                    continue
                request = None
                released = False
                journal_found = False
                journal_err: BaseException | None = None
                try:
                    request, released, journal_found = await self.abort_repo.read_allocation_abort(
                        mid
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001 —— Go 的 `journalErr`
                    journal_err = exc
                if journal_err is not None or not journal_found:
                    plog.get().warning(
                        "allocation_abort_journal_unavailable",
                        match_id=mid,
                        found=journal_found,
                        err=str(journal_err) if journal_err is not None else "",
                    )
                    continue
                try:
                    await self.abort_preactive_battle(request)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001 —— 同 request 幂等重试
                    plog.get().warning(
                        "allocation_abort_reconcile_pending",
                        match_id=mid,
                        released=released,
                        err=str(exc),
                    )
                continue
            if self.model_b and found and inflight.state == STATE_ALLOCATING:
                # Model B 只有 fence 成功把 state 改成 allocation_uncertain 后才允许
                # GSA POST。仍为 allocating 的陈旧 claim **机械证明**外部副作用尚未开始,
                # 可按 allocation_id 直接撤销;无需、也不能伪造一个缺 UID 的外部 release。
                deleted = False
                try:
                    deleted = await self.repo.delete_battle_if_allocation_matches(
                        mid, inflight.allocation_id, inflight.ds_pod_name
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001 —— 下一轮重试
                    plog.get().warning(
                        "model_b_prepost_claim_delete_failed",
                        match_id=mid,
                        allocation_id=inflight.allocation_id,
                        deleted=deleted,
                        err=str(exc),
                    )
                continue
            if (
                self.model_b
                and found
                and inflight.pod_uid == ""
                and inflight.ds_pod_name != ""
                and inflight.gameserver_uid != ""
                and inflight.instance_epoch > 0
                and inflight.allocation_id != ""
            ):
                try:
                    resolved_pod_uid = await self.ensure_durable_release_pod_uid(
                        mid,
                        inflight.ds_pod_name,
                        BattleExpectedInstance(
                            allocation_id=inflight.allocation_id,
                            instance_uid=inflight.gameserver_uid,
                            instance_epoch=inflight.instance_epoch,
                        ),
                        inflight.release_track,
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001 —— 拿不到 UID 就不删,下轮重试
                    plog.get().warning(
                        "model_b_stale_pod_uid_preflight_failed",
                        match_id=mid,
                        pod=inflight.ds_pod_name,
                        err=str(exc),
                    )
                    continue
                inflight.pod_uid = resolved_pod_uid
            if self.model_b:
                await self._sweep_model_b_item(
                    mid, inflight, found, active_cutoff, warming_cutoff, round_start
                )
                continue
            await self._sweep_legacy_item(mid, active_cutoff, warming_cutoff)
        # R4 就地聚合:sweep 每 sweep_interval(默认 5s)跑一轮,逐项打 INFO 会把同文件里
        # 的 WARN 拒绝冲走。整轮只出一条 debug,足以回答"扫了几个、处理了几个、耗时多少"
        # (§11.3 判据 5),异常侧(预算耗尽 / 各类失败)另有独立 WARN,不受本条级别影响。
        if stale:
            plog.get().debug(
                "allocation_sweep_round_done",
                stale=len(stale),
                processed=processed,
                deferred=len(stale) - processed,
                elapsed_ms=int((time.monotonic() - round_start) * 1000),
                budget=godur.duration_string(_dt.timedelta(seconds=budget)),
            )
        # 孤儿 Allocated GameServer 对账清扫:按分钟节流,只处理「无任何权威记录引用」
        # 的 GS,与上面按记录驱动的判弃链互不重叠。
        await self.reconcile_orphan_game_servers_if_due(time.monotonic(), now_ms())

    async def _sweep_model_b_item(  # noqa: C901, PLR0911, PLR0912 —— 与 Go 逐条对应
        self,
        mid: int,
        inflight: Any,
        found: bool,
        active_cutoff: int,
        warming_cutoff: int,
        round_start: float,
    ) -> None:
        """Go `sweepOnce` 里 `if u.modelB { ... }` 那一整块。

        ★ 拆成独立方法**只**为了让 `continue` 有落点:Go 那块用 `continue` 跳过本项
          剩余处理,Python 在 `for` 体里同样可以,但连同上游分支一起会让单个函数超过
          200 行、每次改动都要重读全文。语义与内联完全一致(每个 `continue` → `return`)。
        """
        warming_forfeit: BattleWarmingForfeit | None = None
        # warming 冷加载宽限的 Agones 加速出口(SDK health ping 判死接入):exact 实例已被
        # 编排层权威确认死亡(GameServer + 关联 Pod UID 双确认消失,或 Agones 判 Unhealthy)
        # 时放弃时间宽限,本轮即交事务判弃,不再空等 ready_wait 到期。三道防线:
        #   ① probe 是 advisory 只读:任何读失败仅回退时间界(fail-closed 到慢路径,绝不
        #      据此直接 Release),并按分配身份记一次队头退避(INC-20260724-001 纪律:
        #      控制面持续超时的 probe 不得跨轮占住队头;时间兜底判弃不受退避影响,同
        #      match 的新分配身份不同、不继承旧退避)。
        #   ② 判死 forfeit 绑定被探测实例的 exact 身份,由 `abandon_if_stale` 在事务内精确
        #      核验(防 ABA:probe 挂起期间 A 被清理、同 match 新分配 B 就位,A 的判死绝
        #      不能杀 B)。
        #   ③ 与首次 `activate_heartbeat` 的竞态仍由 WATCH 单赢家保证 —— 激活先赢时
        #      Active 分支按业务心跳阈值重判,本轮不弃。
        if (
            found
            and inflight.state == STATE_WARMING
            and inflight.allocation_id != ""
            and inflight.ds_pod_name != ""
            and inflight.gameserver_uid != ""
            and isinstance(self.authoritative_alloc, WarmingInstanceProber)
        ):
            probe_defer_state = WARMING_PROBE_DEFER_PREFIX + inflight.allocation_id
            if not self.sweep_deferral_active(mid, probe_defer_state, round_start):
                gone = False
                perr: BaseException | None = None
                try:
                    gone = await self.authoritative_alloc.probe_expected_instance_gone(
                        inflight.ds_pod_name, inflight.gameserver_uid, inflight.pod_uid
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001 —— advisory:失败回退时间界
                    perr = exc
                if perr is not None:
                    self.note_sweep_deferral(mid, probe_defer_state, round_start)
                    plog.get().debug(
                        "warming_instance_probe_unavailable",
                        match_id=mid,
                        pod=inflight.ds_pod_name,
                        err=str(perr),
                    )
                elif gone:
                    warming_forfeit = BattleWarmingForfeit(
                        instance=BattleExpectedInstance(
                            allocation_id=inflight.allocation_id,
                            instance_uid=inflight.gameserver_uid,
                            instance_epoch=inflight.instance_epoch,
                        ),
                        heartbeat_ms=now_ms(),
                    )
                    plog.get().warning(
                        "warming_instance_confirmed_dead_forfeit_grace",
                        match_id=mid,
                        pod=inflight.ds_pod_name,
                        allocation_id=inflight.allocation_id,
                    )
        # frozen dataclass ⇒ 一次构造(形变 6)。probe 已在上面完成,求值顺序与 Go 相同。
        cutoffs = BattleStaleCutoffs(
            active_heartbeat_ms=active_cutoff,
            warming_heartbeat_ms=warming_cutoff,
            warming_forfeit=warming_forfeit,
        )
        try:
            out = await self.auth_repo.abandon_if_stale(
                mid, cutoffs, self.ds_credential_ttl_sec, self.battle_ttl_sec()
            )
        except asyncio.CancelledError:
            raise
        except BaseException as aerr:  # noqa: BLE001 —— Go 的 `aerr != nil`
            # Redis 权威不可读 / 状态不一致时 fail-closed:绝不凭派生 ZSET 直接 Release。
            # 仅 battle 已随 TTL 消失时可安全清残留索引。
            try:
                existing = await self.repo.get_battle(mid)
            except asyncio.CancelledError:
                raise
            except BaseException:  # noqa: BLE001 —— Go 的 `gerr == nil` 前置,读不到就不清
                pass
            else:
                if existing is None:
                    await _discard_error(self.repo.remove_active(mid))
            plog.get().warning(
                "model_b_sweep_authority_check_failed", match_id=mid, err=str(aerr)
            )
            return
        b = out.battle
        if not out.abandoned and not out.already_terminal:
            # 只是跨 slot ZSET score 陈旧;用事务快照里的服务端 auth heartbeat 修索引。
            if b is not None:
                await _discard_error(self.repo.touch_active(mid, b.last_heartbeat_ms))
            return
        if b is None:
            return
        if b.state == STATE_ENDED:
            await _discard_error(self.repo.remove_active(mid))
            return
        if b.state != STATE_ABANDONED:
            return
        # 外部回收已进入删除宽限的项让出队头(退避期内不重复 terminate/release;
        # `deliver_abandoned` 本就门控在 release 确认之后,退避只是把必然失败的重试
        # 推迟到宽限结束,不延后任何可行的补偿)。
        if self.sweep_deferral_active(
            mid, RELEASE_PENDING_DEFER_PREFIX + b.allocation_id, round_start
        ):
            return
        if not out.auth_found or not out.active_found:
            # Prepare 前崩溃的分配也必须先进入永久 release fence。旧实现 Release→Delete
            # 虽顺序较安全,但有限 TTL 仍会在 Release 响应未知后自行开放第二次 POST;
            # 统一走同一个 fenced 回收状态机。
            # bootstrap/no-active 分支同样按结构化结果退避(复审 P1-2):任何未确认结果
            # 都按 exact allocation 让出队头,防控制面故障时每轮重复外部调用。
            outcome = await self.reconcile_preactive_release(b)
            if outcome == PreactiveReleaseOutcome.COMPLETED:
                plog.get().info(
                    "model_b_inflight_reconciled", match_id=mid, allocation_id=b.allocation_id
                )
            elif outcome == PreactiveReleaseOutcome.UNCONFIRMED:
                self.note_sweep_deferral(
                    mid, RELEASE_PENDING_DEFER_PREFIX + b.allocation_id, round_start
                )
            # PreactiveReleaseOutcome.SKIPPED:零外部副作用,无需退避(Go 的空 case)。
            return
        # abandoned 的外部回收按 UID/allocation_id 幂等执行到确认成功;失败必须保留
        # active outbox,不能先投补偿并移出索引后永久泄漏 GameServer。
        expected_instance = BattleExpectedInstance(
            allocation_id=b.allocation_id,
            instance_uid=b.gameserver_uid,
            instance_epoch=b.instance_epoch,
        )
        try:
            resolved_pod_uid = await self.ensure_durable_release_pod_uid(
                mid, b.ds_pod_name, expected_instance, b.release_track
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 控制面读失败:退避后重试
            # 控制面读失败同样按分配身份退避,不得每轮重试吃满预算(队头饥饿全类根除)。
            self.note_sweep_deferral(
                mid, RELEASE_PENDING_DEFER_PREFIX + b.allocation_id, round_start
            )
            plog.get().warning(
                "model_b_sweep_pod_uid_preflight_failed",
                match_id=mid,
                pod=b.ds_pod_name,
                err=str(exc),
            )
            return
        b.pod_uid = resolved_pod_uid
        terminated = False
        terminate_err: BaseException | None = None
        try:
            terminated = await self.auth_repo.terminate_expected(
                mid,
                expected_instance,
                STATE_ABANDONED,
                self.ds_credential_ttl_sec,
                self.battle_ttl_sec(),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— Go 的 `terminateErr`
            terminate_err = exc
        if not terminated:
            plog.get().warning(
                "model_b_sweep_terminate_fence_failed",
                match_id=mid,
                pod=b.ds_pod_name,
                err=str(terminate_err) if terminate_err is not None else "",
            )
            return
        # ★ Go 在 `terminated == true` 之后还判一次 `terminateErr != nil` 并打
        #   `model_b_sweep_index_cleanup_failed`(fence 已提交、只是派生索引清理失败)。
        #   Python 的 `terminate_expected` 对任何失败都抛、成功恒 True 且不残留 error,
        #   该组合不可达 —— 见模块头形变 4,这里刻意不写死代码。
        try:
            await self.release_game_server(
                mid,
                b.ds_pod_name,
                AuthoritativeGameServerAllocation(
                    pod_name=b.ds_pod_name,
                    instance_uid=b.gameserver_uid,
                    allocation_id=b.allocation_id,
                    pod_uid=b.pod_uid,
                    instance_epoch=b.instance_epoch,
                ),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as rerr:  # noqa: BLE001 —— 未确认的 release 一律退避重试
            # 任何未确认的 release 结果都按分配身份退避(不只 deletion-pending):宽限内 /
            # 控制面故障中的重试只会空转占队头,队尾 §9.4 补偿必须照常推进;退避到期后的
            # 重试经 DELETE 幂等 + 双对象消失确认闭合,outbox 语义不变。
            self.note_sweep_deferral(
                mid, RELEASE_PENDING_DEFER_PREFIX + b.allocation_id, round_start
            )
            if _is_release_deletion_pending(rerr):
                plog.get().debug(
                    "model_b_sweep_release_grace_pending",
                    match_id=mid,
                    pod=b.ds_pod_name,
                    allocation_id=b.allocation_id,
                )
                return
            plog.get().warning(
                "model_b_sweep_release_failed", match_id=mid, pod=b.ds_pod_name, err=str(rerr)
            )
            return
        if out.abandoned:
            plog.get().info(
                "battle_abandoned_heartbeat_timeout",
                match_id=mid,
                pod=b.ds_pod_name,
                authority="redis",
            )
        if await self.deliver_abandoned(
            mid, b.ds_pod_name, b.gameserver_uid, list(b.player_ids), b.map_id, b.game_mode
        ):
            target = placement.Target(
                pod_name=b.ds_pod_name,
                instance_uid=b.gameserver_uid,
                instance_epoch=b.instance_epoch,
                allocation_id=b.allocation_id,
                release_track=b.release_track,
            )
            try:
                await self.lifecycle_proof_repo.record_allocation_lifecycle_published(mid, target)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 无 ACK 见证不得放开 fence
                plog.get().warning(
                    "model_b_sweep_lifecycle_marker_failed",
                    match_id=mid,
                    allocation_id=b.allocation_id,
                    err=str(exc),
                )
                return
            expired = False
            eerr: BaseException | None = None
            try:
                expired = await self.auth_repo.expire_terminated_expected(
                    mid, expected_instance, self.ds_credential_ttl_sec, self.battle_ttl_sec()
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— Go 的 `eerr`
                eerr = exc
            if eerr is not None or not expired:
                plog.get().warning(
                    "model_b_sweep_expire_failed",
                    match_id=mid,
                    expired=expired,
                    err=str(eerr) if eerr is not None else "",
                )

    async def _sweep_legacy_item(  # noqa: C901 —— 与 Go 逐条对应
        self, mid: int, active_cutoff: int, warming_cutoff: int
    ) -> None:
        """Go `sweepOnce` 尾部的 legacy(非 Model B)分支。

        与 `_sweep_model_b_item` 同因拆出;每个 `continue` → `return`,语义一致。
        """
        pod_name = ""
        instance_uid = ""  # owner 释放的 exact 身份门用(见 deliver_abandoned)
        stale_index_only = False
        ended_skip = False
        # 本次成功事务是否执行了 →abandoned 的首次迁移(全局恰好一次,见闭包内注释)。
        first_abandon = False
        player_ids: list[int] = []
        map_id = 0
        game_mode = ""

        def mutate(b: Any) -> None:
            # 出参每轮重置:CAS 冲突时闭包基于重新 GET 的最新镜像整体重跑,以最后一次
            # 成功事务为准(`_update_with_lock` 的 fn 重跑契约)。
            nonlocal pod_name, instance_uid, stale_index_only, ended_skip
            nonlocal first_abandon, player_ids, map_id, game_mode
            stale_index_only = False
            ended_skip = False
            first_abandon = False
            # active ZSET 是跨 slot 派生索引。心跳可能已成功更新权威 record,但后续 ZADD
            # 失败,留下旧 score;必须在任何终态写 / Release 前重新核验 record。
            # `update_battle_keep_ttl` 成功后会以该真实时间补写 ZSET,故这里只修索引、
            # 零副作用。阈值同样在事务闭包内按最新镜像状态选择:warming 冷加载用 ready
            # 等待阈值。
            cutoff = active_cutoff
            if b.state == STATE_WARMING:
                cutoff = warming_cutoff
            if b.last_heartbeat_ms > cutoff:
                stale_index_only = True
                return
            if b.state == STATE_ENDED:
                ended_skip = True  # 正常结算,移出 active 不补偿
                pod_name = b.ds_pod_name  # 捕获用于 local 幽灵 DS 收尾(见 kill_stranded_ds)
                return
            # first_abandon 仅在本事务把状态从非 abandoned 首次写成 abandoned 时为 True。
            # WATCH CAS 保证该迁移跨副本 / 跨 sweep 轮次全局只成功一次:并发副本撞
            # WatchError 重跑后读到 abandoned → False;补偿重试轮次读到 abandoned → False。
            # 因此下方 Release 恰好执行一次,不存在 double-release。
            first_abandon = b.state != STATE_ABANDONED
            b.state = STATE_ABANDONED
            pod_name = b.ds_pod_name
            instance_uid = b.gameserver_uid
            # ★ 必须 `list(...)` 复制:repeated 容器属于事务内那个 record 对象,CAS 重跑
            #   会换成新对象,直接持有引用会让出参指向上一次重跑的躯壳。
            player_ids = list(b.player_ids)
            map_id = b.map_id
            game_mode = b.game_mode

        # KEEPTTL:标记 abandoned / 每轮重试不刷新 battle key TTL,保证 BattleTTL 是补偿
        # 重试上界。
        try:
            await self.repo.update_battle_keep_ttl(mid, UPDATE_MAX_RETRY, mutate)
        except asyncio.CancelledError:
            raise
        except BaseException as lerr:  # noqa: BLE001 —— Go 的 `lerr != nil`
            if errcode.as_code(lerr) == errcode.ErrDSPodNotFound:
                # 镜像 TTL 过期:清理残留 active(补偿重试的天然上界)
                await _discard_error(self.repo.remove_active(mid))
                return
            plog.get().warning("sweep_lock_failed", match_id=mid, err=str(lerr))
            return
        if stale_index_only:
            plog.get().info("sweep_repaired_stale_index", match_id=mid)
            return
        if ended_skip:
            # 正常结算的 DS 收尾回收(2026-07-03):battle_result 不再在结算响应路径直杀 DS
            # (那会抢在 DS 通知客户端回大厅之前把它杀掉),DS 生命周期归此处兜底。DS 发完
            # ended 心跳后即停止心跳(无第二跳,心跳终态 kill_stranded_ds 永不触发),且
            # local 模式 DS 的 Agones Shutdown 是 no-op → 进程不会自退。故这里在 ended 且
            # 失联(≥HeartbeatTimeout 未心跳,此时 DS 早已通知客户端回大厅)时主动 taskkill,
            # 防幽灵 DS 占端口耗尽端口池。
            # `kill_orphan_on_stop` 门控:仅 local 打开;Agones 关(DS 已自身 Shutdown,
            # pod 交 Fleet 回收)。
            self.kill_stranded_ds(mid, pod_name, "ended")
            await _discard_error(self.repo.remove_active(mid))
            return
        # 仅首次迁移 abandoned 的赢家事务回收 pod(并发副本 / 补偿重试轮次 first_abandon
        # 为 False 跳过,不会重复 Release;Release 本身对已消失的 GameServer 幂等,双重保险)。
        if first_abandon:
            try:
                await self.alloc.release(pod_name)
            except asyncio.CancelledError:
                raise
            except BaseException as rerr:  # noqa: BLE001 —— 幂等,失败只告警
                plog.get().warning(
                    "sweep_release_failed", match_id=mid, pod=pod_name, err=str(rerr)
                )
            plog.get().info("battle_abandoned_heartbeat_timeout", match_id=mid, pod=pod_name)
        # 投递 abandoned 补偿事件:成功(或显式 local/off 开发回退)才移出 active;
        # 失败则保留在 active,下一轮 sweep 重试(可靠补偿,§9 不变量 4)。
        if await self.deliver_abandoned(
            mid, pod_name, instance_uid, player_ids, map_id, game_mode
        ):
            # 终态镜像保留一段供查询,移出 active 不再扫描
            try:
                await self.repo.expire_battle(mid, self.battle_ttl_sec())
            except asyncio.CancelledError:
                raise
            except BaseException as eerr:  # noqa: BLE001 —— 下一轮 sweep 重来
                plog.get().warning("sweep_expire_failed", match_id=mid, err=str(eerr))

    # ── 派生 active 索引重建 ──────────────────────────────────────────────

    async def reconcile_active_index_if_due(self) -> None:
        """按 30s 节流从 canonical 记录重建派生 active ZSET。对应 Go 的 `reconcileActiveIndexIfDue`。

        Raises:
            errcode.PandoraError(ErrInvalidState): Model B 下缺重建能力 —— 丢失的永久
                墓碑无人发现,**整轮 sweep fail-closed**。
            errcode.PandoraError(ErrUnavailable): 重建失败,本轮 sweep 整体不执行,
                判弃补偿链顺延一个周期。
        """
        if self.active_index_reconciler is None:
            if self.model_b:
                plog.get().error(
                    "battle_active_index_refused",
                    reason=REASON_ACTIVE_INDEX_UNAVAILABLE,
                    hint="Model B 下缺派生索引重建能力,丢失的永久墓碑无人发现;整轮 sweep fail-closed",
                )
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "canonical battle active-index reconciler unavailable",
                )
            return
        # ★ 单调秒。0.0 = 从未重建(Go 的 `IsZero()`),首扫必跑。
        now = time.monotonic()
        if (
            self.last_active_index_reconcile != 0.0
            and ACTIVE_INDEX_RECONCILE_INTERVAL_SEC > 0
            and now - self.last_active_index_reconcile < ACTIVE_INDEX_RECONCILE_INTERVAL_SEC
        ):
            return
        try:
            await self.active_index_reconciler.reconcile_battle_active_index(
                ACTIVE_INDEX_RECONCILE_SCAN_COUNT
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            plog.get().warning(
                "battle_active_index_refused",
                reason=REASON_ACTIVE_INDEX_REBUILD,
                elapsed_ms=int((time.monotonic() - now) * 1000),
                err=str(exc),
                hint="重建失败 = 本轮 sweep 整体不执行(fail-closed),判弃补偿链顺延一个周期",
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "rebuild canonical battle active index",
                cause=exc,
            ) from exc
        # R1:派生 active 索引重建是 §11.3 点名要求留 INFO 的不可逆推进之一(它决定了哪些
        # 对局能被心跳超时扫描看到)。频率固定 30s,不会刷屏;无它则"判弃链从什么时候开始
        # 漏扫"无法从日志回答。
        plog.get().info(
            "battle_active_index_reconciled",
            interval=godur.duration_string(
                _dt.timedelta(seconds=ACTIVE_INDEX_RECONCILE_INTERVAL_SEC)
            ),
            elapsed_ms=int((time.monotonic() - now) * 1000),
        )
        self.last_active_index_reconcile = now

    # ── abandoned 补偿投递 ────────────────────────────────────────────────

    async def deliver_abandoned(
        self,
        match_id: int,
        pod_name: str,
        instance_uid: str,
        player_ids: list[int],
        map_id: int,
        game_mode: str,
    ) -> bool:
        """发 `DSLifecycleEvent{phase=ABANDONED}` 给 battle_result 做玩家段位回滚补偿。
        对应 Go 的 `deliverAbandoned`。

        Returns:
            True  → 可移出 active:已成功投递,或显式 local/off 开发配置未接 Kafka。
            False → 投递失败,保留在 active 下一轮 sweep 重试(可靠补偿,§9 不变量 4)。

        ★ 生产 required 但 publisher 意外为 None 时**必须**返回 False:这是一道独立于
          main 启动校验的 fail-closed 保险,禁止 abandoned 在没有 match release /
          exit proof 时被过期掉。
        """
        # owner 权威释放(INC-20260729-002 P0-B1)与 lifecycle 投递同点执行:本函数的所有
        # 调用点都已门控在「被判弃实例的 GameServer 回收已确认」之后,正是唯一既安全
        # (旧 DS 已消失,释放不会开出双 DS)又必经(补偿链的唯一收口)的位置。
        #
        # 放在 lifecycle 投递**之前**:lifecycle 失败会返回 False 让 sweep 下一轮整体重试,
        # 而 owner 释放本身幂等(compare-delete,已释放的记录第二次是 no-op),重跑无副作用;
        # 反过来若放在 return True 之后,lifecycle 长时间不可用时玩家的 owner 记录会一直
        # 指向已删除的 Pod,这正是本条要根治的状态。
        #
        # 预算 2s 与 census 弱依赖同口径:本函数在 sweep 轮内被调用,而 sweep 单轮墙钟预算
        # = sweep_interval(默认 5s,见 `sweep_round_budget`),owner 抖动时不得吃满它。
        await owner_release_abandoned_players_weak(
            self.owner_auth, player_ids, pod_name, instance_uid, OWNER_RELEASE_BUDGET_SEC
        )

        if self.lifecycle is None:
            if self.lifecycle_required:
                plog.get().error(
                    "ds_lifecycle_publisher_missing_fail_closed",
                    match_id=match_id,
                    hint="retain active recovery outbox; restart with a healthy Kafka producer",
                )
                return False
            plog.get().warning(
                "ds_lifecycle_disabled_dev_best_effort",
                match_id=match_id,
                hint="local/off development only; no battle_result recovery event will be produced",
            )
            return True
        evt = dspb.DSLifecycleEvent(
            match_id=match_id,
            ds_pod_name=pod_name,
            phase=dspb.DS_LIFECYCLE_PHASE_ABANDONED,
            player_ids=player_ids,
            map_id=map_id,
            game_mode=game_mode,
            ts_ms=now_ms(),
        )
        try:
            await self.lifecycle.publish_lifecycle(evt)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 保留在 active,下轮重试
            # 保留在 active,下轮 sweep 重试(穿越 Kafka 临时不可用)
            plog.get().warning(
                "ds_lifecycle_publish_failed_will_retry", match_id=match_id, err=str(exc)
            )
            return False
        plog.get().debug("ds_lifecycle_published", match_id=match_id)
        return True

    # ── 孤儿 GameServer 对账(挂在 sweep 轮末尾)──────────────────────────

    async def reconcile_orphan_game_servers_if_due(self, now_sec: float, now_wall_ms: int) -> None:
        """按分钟节流跑一轮孤儿 Allocated GameServer 对账。
        对应 Go 的 `reconcileOrphanGameServersIfDue`(biz/orphan_gameserver.go)。

        ★ 判定链 / 四重防误删 / 首见表已经在 `orphan_gameserver.OrphanGameServerJanitor`
          里落地(那次移植刻意把它做成**独立对象**,理由见该模块头:清扫的全部状态都是
          进程内非权威调度提示,和权威态放同一个对象里迟早会有人拿首见表当"这台 GS 确实
          无人"的证据)。本方法只负责把它接到 sweep 循环上,不重复实现任何判定。

        ★ 因此 `AllocatorUsecaseBase.last_orphan_gs_reconcile` / `orphan_gs_first_seen`
          这两个 Go 镜像字段在本批**不被使用** —— 等价状态由 janitor 私有持有。刻意不去
          同步两份:两份进程内调度状态漂移时不会报错,只会让节流窗与首见时刻各说各话。

        ★ janitor **懒建**并缓存:它必须在 `enable_redis_authority` / 各 setter 跑完之后
          才拿得到最终的 reconciler / ledger(构造期能力探测的结果在基座 `__init__` 里,
          但 orphan 清扫的两项能力允许被后续装配改写)。首次 sweep 时建一次即可。
        """
        janitor = getattr(self, "_orphan_gs_janitor", None)
        if janitor is None:
            janitor = OrphanGameServerJanitor(
                self.orphan_gs_reconciler,
                self.allocation_ledger,
                self.repo,
                reclaim_after_sec=int(
                    self.cfg.resolve_orphan_gs_reclaim_after().total_seconds()
                ),
            )
            self._orphan_gs_janitor = janitor
        await janitor.reconcile_if_due(now_sec, now_wall_ms)


# ── 哨兵识别(Go 的 errors.Is)────────────────────────────────────────────────

#: `cause` 链最大追溯深度。纯防环 / 防病态深链,正常链路只有 1~2 层。
#: (与 `biz_allocate._CAUSE_CHAIN_LIMIT` / `owner_authority._CAUSE_CHAIN_LIMIT` 同值同因。)
_CAUSE_CHAIN_LIMIT = 16


def _is_release_deletion_pending(exc: BaseException | None) -> bool:
    """Go: `errors.Is(rerr, data.ErrReleaseDeletionPending)`。

    ★ 必须沿 `PandoraError.cause` 与 `__cause__` 两条链走,而不是只看最外层:
      `release_game_server` 会把底层错误包成 errcode(外层 code 给调用方、内层 cause
      给控制流)。只看最外层的后果不是"少一条 debug 日志",而是把「删除已受理、正在等
      Pod 终止宽限」错报成 `model_b_sweep_release_failed` —— 一个 WARN 级别的假告警,
      每 15s 一条,直到宽限结束。
    """
    seen = 0
    cur = exc
    while cur is not None and seen < _CAUSE_CHAIN_LIMIT:
        if isinstance(cur, ReleaseDeletionPendingError):
            return True
        nxt = getattr(cur, "cause", None)
        if nxt is None:
            nxt = cur.__cause__
        cur = nxt
        seen += 1
    return False
