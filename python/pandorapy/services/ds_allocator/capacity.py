"""战斗 DS Fleet 容量巡检 + 快到上限预警 —— 对应 Go 侧
`services/battle/ds_allocator/internal/biz/capacity.go`。

## 背景

Agones Fleet 副本数是战斗 DS 并发容量的**硬上限**,打满后 GameServerAllocation 直接
返回 UnAllocated —— 玩家匹配成功却进不了局。本巡检的全部意义是让运维在打满
**之前**就看到信号:

  - 每 `capacity_watch_interval`(默认 30s)GET 通用 Fleet + 各 `map_fleets` 专属
    Fleet 的 spec/status;
  - 暴露 `pandora_ds_allocator_fleet_{replicas,ready,allocated,usage_ratio,desired_replicas}`;
  - `allocated/replicas >= capacity_warn_ratio`(默认 0.8)→ Warn `ds_fleet_capacity_near_limit`;
  - `ready == 0`(完全打满 / Fleet 缩到 0)→ Error `ds_fleet_capacity_exhausted`;
  - 回落阈值以下 → Info `ds_fleet_capacity_recovered`。

## 降噪:日志只报**事件**,水位看 gauge

`infra.md §11`:周期任务只在有事发生时打。只在状态**变化**时打日志;持续超限时每
`REWARN_INTERVAL_SEC`(5m)重打一次 —— 既不至于 30s 一条刷屏,也不至于让长时间高
水位彻底静默。持续性的水位信号交给 Prometheus gauge(Grafana / 告警规则消费)。

## INC-20260724-001:canary 常态 desired=0 曾把真信号淹掉

未做金丝雀发布时 canary Fleet 常态 `desired=0`,旧逻辑 `ready==0` 先于一切判定
⇒ **恒判 exhausted**,每 5m 一条 Error + Grafana critical 长期 firing,把真实的
stable `ready=0` 信号淹没(事故当天 13:12:38 那条极可能就被当噪音略过了)。

修法是**两条刻意保守的边界**,一条都不能"顺手简化":

    desired_known == False   没解码到 spec.replicas → 维持旧行为照常判 exhausted。
                             不确定不得冒充"已知为 0"(§9.22)。
    stable 轨 desired == 0   运维 / 脚本误把 stable 缩到 0 → **照常** exhausted 告警。
                             那确实让新对局分配必失败,是真问题,不能因为"是故意缩的"
                             就静音。

仅 mode=agones 装配;local / mock 无 Fleet 概念,不巡检。
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import time
from typing import Callable, Protocol

from prometheus_client import Gauge

from pandorapy import godur
from pandorapy import log as plog
from pandorapy import safego
from pandorapy.services.ds_allocator import conf as dsconf
from pandorapy.services.ds_allocator.agones_allocator import AgonesError, FleetCapacity

# ── Prometheus 指标(命名规范 infra.md §10:pandora_<service>_<metric>)────────
#
# ★ 指标名 / label 名必须与 Go **逐字一致**:Grafana 面板与告警规则按它们建,
#   改一个字 = 面板静默变空、告警变 NoData,两边都不报错。
# label 只有 fleet(集群里 Fleet 数量级 = 个位数,无高基数风险)。

FLEET_REPLICAS_GAUGE = Gauge(
    "pandora_ds_allocator_fleet_replicas",
    "战斗 DS Fleet 当前总副本数(容量上限,Fleet status.replicas)",
    ["fleet"],
)
FLEET_READY_GAUGE = Gauge(
    "pandora_ds_allocator_fleet_ready",
    "战斗 DS Fleet 空闲可分配副本数(Fleet status.readyReplicas)",
    ["fleet"],
)
FLEET_ALLOCATED_GAUGE = Gauge(
    "pandora_ds_allocator_fleet_allocated",
    "战斗 DS Fleet 已被对局占用副本数(Fleet status.allocatedReplicas)",
    ["fleet"],
)
FLEET_USAGE_RATIO_GAUGE = Gauge(
    "pandora_ds_allocator_fleet_usage_ratio",
    "战斗 DS Fleet 容量占用比 allocated/replicas(0~1;replicas=0 时置 1,有意不配容量的 canary 置 0)",
    ["fleet"],
)
# ★ 只在**解码到** spec.replicas 时才 Set —— 没解码到就不写这条序列,让告警规则
#   退化回原判据继续告警,而不是被一个默认 0 值静默(§9.22:不得把 UNKNOWN 冒充
#   成确定值)。这也是为什么 FleetCapacity 要带 desired_known 而不是只带 desired。
FLEET_DESIRED_GAUGE = Gauge(
    "pandora_ds_allocator_fleet_desired_replicas",
    "战斗 DS Fleet 期望副本数(Fleet spec.replicas;未解码到时不上报该序列)",
    ["fleet"],
)

# ── 水位档 / 事件 ────────────────────────────────────────────────────────────
#
# Go 是 `capacityLevel int` 的 iota 常量。这里保持整数**且保持大小次序** ——
# `observe` 里的升档判定写的是 `level > prev`,换成字符串枚举就没有次序可比了。
CAPACITY_OK = 0  # 水位正常
CAPACITY_WARN = 1  # allocated/replicas >= warn_ratio,接近上限
CAPACITY_EXHAUSTED = 2  # ready==0,完全打满(或 Fleet 缩到 0),分配必失败

# 巡检事件:`poll_once` 据此决定打哪条日志(空串 = 不打)。
EVENT_NONE = ""
EVENT_NEAR_LIMIT = "near_limit"
EVENT_EXHAUSTED = "exhausted"
EVENT_RECOVERED = "recovered"

# 持续超限时的重复告警间隔(秒):状态不变时最多每 5m 重打一条,防刷屏。
REWARN_INTERVAL_SEC = 5 * 60.0

# `capacity_watch_interval` 的默认值(配 0 = 未配置 → 用它;配负 = 显式禁用)。
DEFAULT_WATCH_INTERVAL_SEC = dsconf.DEFAULT_CAPACITY_WATCH_INTERVAL.total_seconds()
DEFAULT_WARN_RATIO = dsconf.DEFAULT_CAPACITY_WARN_RATIO

# safego 点位名(有界枚举,不是高基数值)。与 Go 的 `safego.Run(ctx, "fleet_capacity_watch", …)` 同名。
_WATCH_TASK_NAME = "fleet_capacity_watch"


class FleetCapacityLister(Protocol):
    """GET Fleet spec/status。由 `agones_allocator.AgonesGameServerAllocator` 实现。
    Go: `biz.FleetCapacityLister`。

    ★ 返回 `(快照列表, 错误)` 而不是"要么全成功要么抛" —— 单个 Fleet 查询失败不影响
      其余。把部分成功抹成异常等于用一次抖动把已经拿到的水位全丢掉,而水位正是本
      巡检唯一的产出。
    """

    async def list_fleet_capacities(self) -> tuple[list[FleetCapacity], AgonesError | None]: ...


def deliberately_unprovisioned(c: FleetCapacity) -> bool:
    """该 Fleet 是「有意不配容量」而非「被负载打满」。Go: `deliberatelyUnprovisioned`。

    当前**只认** canary 轨 `desired_known and desired == 0` 这一种组合。
    三个条件缺任何一个就不成立,见模块头那两条保守边界。
    """
    return c.canary and c.desired_known and c.desired == 0


def usage_ratio(c: FleetCapacity) -> float:
    """`allocated/replicas`。Go: `usageRatio`。

    `replicas == 0`(Fleet 缩到 0)按 **1.0** 计 —— 零容量即满,这是要告警的状态。
    例外:有意不配容量的 canary 按 0 计;它没有"占用比"可言,记 1.0 会让 Grafana
    面板上一条常态空跑的 canary 永远顶在 100%,看板从此不可信。
    """
    if c.replicas == 0:
        if deliberately_unprovisioned(c):
            return 0.0
        return 1.0
    return float(c.allocated) / float(c.replicas)


def level_for(c: FleetCapacity, warn_ratio: float) -> int:
    """计算水位档。Go: `levelFor`。

    ★ 判定顺序不可交换:`deliberately_unprovisioned` 必须在 `ready == 0` **之前**,
      这正是 INC-20260724-001 的修复本身;倒回去就恢复了"canary 恒判 exhausted"。
    """
    if deliberately_unprovisioned(c):
        return CAPACITY_OK
    if c.ready == 0:
        return CAPACITY_EXHAUSTED
    if usage_ratio(c) >= warn_ratio:
        return CAPACITY_WARN
    return CAPACITY_OK


class CapacityWatcher:
    """周期巡检 Fleet 容量:更新指标 + 状态变化时打预警日志。Go: `biz.CapacityWatcher`。

    `_levels` / `_last_warn_at` 只由巡检协程单线程读写(与 Go 的"仅巡检 goroutine"
    同),无需锁。两张表的键是 Fleet 名 —— 来自配置的有界集合,不随流量增长。
    """

    __slots__ = ("_lister", "_interval_sec", "_warn_ratio", "_levels", "_last_warn_at", "_now")

    def __init__(
        self,
        lister: FleetCapacityLister,
        interval_sec: float,
        warn_ratio: float,
        *,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._lister = lister
        self._interval_sec = interval_sec
        self._warn_ratio = warn_ratio
        self._levels: dict[str, int] = {}
        self._last_warn_at: dict[str, float] = {}
        # ★ 默认单调钟,不是 `time.time()`:重报间隔比的是"过了多久",墙钟被 NTP
        #   回拨会让一整批 Fleet 的告警要么突然全部重打、要么长时间打不出来。
        #   Go 的 `time.Now()` 自带单调读数,`Sub` 用的就是那部分 —— monotonic 才是等价物。
        self._now = now

    @property
    def interval_sec(self) -> float:
        return self._interval_sec

    @property
    def warn_ratio(self) -> float:
        return self._warn_ratio

    async def run(self) -> None:
        """启动巡检循环,直到被取消。Go: `Run`。

        ★ **先巡一次再进循环**,不等首个 tick:服务刚起来时就要把水位摸清,
          否则一次冷启动会有一整个 interval 的盲区。
        """
        plog.get().info(
            "fleet_capacity_watch_started",
            interval=godur.duration_string(_dt.timedelta(seconds=self._interval_sec)),
            warn_ratio=self._warn_ratio,
        )
        # 单轮 panic 兜底:水位巡检是纯观测,一轮炸掉只该丢本轮,不能让整条循环退出。
        await safego.run_once(_WATCH_TASK_NAME, self.poll_once)
        try:
            while True:
                await asyncio.sleep(self._interval_sec)
                await safego.run_once(_WATCH_TASK_NAME, self.poll_once)
        except asyncio.CancelledError:
            plog.get().info("fleet_capacity_watch_stopped")
            raise

    async def poll_once(self) -> None:
        """巡检一轮:拉容量快照 → 刷指标 → 按水位状态机打事件日志。Go: `pollOnce`。

        ★ 部分 Fleet 查询失败**不影响其余**:错误只汇总打一条 Warn,已拿到的快照
          照常刷指标。返回的 caps 与 err 是两件独立的事,不要写成 `if err: return`。
        """
        caps, err = await self._lister.list_fleet_capacities()
        if err is not None:
            plog.get().warning("fleet_capacity_query_failed", err=str(err))
        for c in caps:
            ratio = usage_ratio(c)
            FLEET_REPLICAS_GAUGE.labels(c.fleet).set(float(c.replicas))
            FLEET_READY_GAUGE.labels(c.fleet).set(float(c.ready))
            FLEET_ALLOCATED_GAUGE.labels(c.fleet).set(float(c.allocated))
            FLEET_USAGE_RATIO_GAUGE.labels(c.fleet).set(ratio)
            if c.desired_known:
                FLEET_DESIRED_GAUGE.labels(c.fleet).set(float(c.desired))

            # 字段名 / 取值口径与 Go 的 kv 切片逐字一致(排障时按字段名 grep)。
            kv = {
                "fleet": c.fleet,
                "replicas": c.replicas,
                "ready": c.ready,
                "allocated": c.allocated,
                "usage_ratio": ratio,
                "warn_ratio": self._warn_ratio,
                "desired": c.desired,
                "desired_known": c.desired_known,
                "canary": c.canary,
            }
            event = self.observe(c)
            if event == EVENT_EXHAUSTED:
                plog.get().error(
                    "ds_fleet_capacity_exhausted",
                    hint="战斗 DS 无空闲副本,新对局分配必失败;立即扩 Fleet replicas",
                    **kv,
                )
            elif event == EVENT_NEAR_LIMIT:
                plog.get().warning(
                    "ds_fleet_capacity_near_limit",
                    hint="战斗 DS 并发接近容量上限,考虑扩 Fleet replicas",
                    **kv,
                )
            elif event == EVENT_RECOVERED:
                plog.get().info("ds_fleet_capacity_recovered", **kv)

    def observe(self, c: FleetCapacity) -> str:
        """更新该 Fleet 的水位状态机,返回本轮要上报的事件。Go: `observe`。

        三条规则(逐条对应 Go 的 switch,顺序不可动):

            升档(ok→warn / *→exhausted)          立即上报
            同档持续超限且距上次告警 < 重报间隔      不报
            从超限回落到 ok                        上报 recovered;首轮即 ok 不报
        """
        level = level_for(c, self._warn_ratio)
        seen = c.fleet in self._levels
        prev = self._levels.get(c.fleet, CAPACITY_OK)
        self._levels[c.fleet] = level
        now = self._now()

        if level == CAPACITY_OK:
            if seen and prev != CAPACITY_OK:
                self._last_warn_at.pop(c.fleet, None)
                return EVENT_RECOVERED
            return EVENT_NONE
        if level > prev or not seen:
            # 升档(或首轮即超限):记下告警时刻并立即上报。
            self._last_warn_at[c.fleet] = now
        elif now - self._last_warn_at.get(c.fleet, float("-inf")) < REWARN_INTERVAL_SEC:
            # ★ 缺省值取 -inf 而不是 0:Go 那边缺省是零值 time.Time(公元 1 年),
            #   `now.Sub(zero)` 是个巨大正值,必然进 default 分支重报。用 0 的话
            #   monotonic 时钟刚启动时 `now - 0` 可能小于 5m,首条告警会被吞掉。
            return EVENT_NONE
        else:
            self._last_warn_at[c.fleet] = now
        if level == CAPACITY_EXHAUSTED:
            return EVENT_EXHAUSTED
        return EVENT_NEAR_LIMIT


def new_capacity_watcher(
    lister: FleetCapacityLister,
    cfg: dsconf.AgonesConf,
    *,
    now: Callable[[], float] = time.monotonic,
) -> CapacityWatcher | None:
    """构造容量巡检器。Go: `NewCapacityWatcher`。

    `capacity_watch_interval` **为负 = 显式禁用**,返回 None(调用方跳过启动);
    为 0 = 未配置,取默认 30s。`capacity_warn_ratio` 落在 `(0, 1]` 之外取默认 0.8。

    ★ "负 = 禁用"与"0 = 默认"是两回事,不能合并成 `<= 0 用默认` ——
      那样运维就再也关不掉这个巡检了(而它会对 apiserver 持续发 GET)。
    """
    interval = cfg.capacity_watch_interval_td().total_seconds()
    if interval < 0:
        return None
    if interval == 0:
        interval = DEFAULT_WATCH_INTERVAL_SEC
    warn_ratio = cfg.capacity_warn_ratio
    if warn_ratio <= 0 or warn_ratio > 1:
        warn_ratio = DEFAULT_WARN_RATIO
    return CapacityWatcher(lister, interval, warn_ratio, now=now)
