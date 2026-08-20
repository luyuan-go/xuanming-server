"""`ds_allocator` 容量巡检 + DS 分配抽象回归测试。

覆盖模块与其**对照真相**(行为口径一律以 Go 源码为准):

    pandorapy/services/ds_allocator/capacity.py
        ← services/battle/ds_allocator/internal/biz/capacity.go
    pandorapy/services/ds_allocator/gameserver.py
        ← services/battle/ds_allocator/internal/biz/gameserver.go

## 本文件盯死的四件事

  ① **INC-20260724-001 的修复方向不可回退**。未做金丝雀发布时 canary Fleet 常态
     `desired=0`,旧逻辑 `ready==0` 先于一切判定 ⇒ 恒判 exhausted,每 5m 一条 Error
     长期 firing,把真实的 stable `ready=0` 信号**淹没**(事故当天那条极可能就被当
     噪音略过了)。修法是 `deliberately_unprovisioned` 排在 `ready == 0` 之前;
     判定顺序一交换就恢复原状。
  ② **两条刻意保守的边界一格都不能省**:`desired_known == False`(没解码到
     spec.replicas)与 **stable 轨** `desired == 0` 都必须**照常告警**。前者是
     §9.22「不确定不得冒充已知为 0」;后者是"运维误把 stable 缩到 0"这个真问题,
     不能因为"是故意缩的"就静音。
  ③ **告警状态机的三条规则顺序不可动**。升档立即报、同档 5m 内不重报、回落报
     recovered。写反的两个方向都有代价:多报 = 刷屏把真信号淹掉(正是 ① 的成因);
     少报 = 容量打满时运维一条都收不到。
  ④ **Mock 分配器必须原样回传 release track**。回传"意图轨"的后果:canary 容量回退
     (GSA 落到 stable Fleet)时会把这局记成 canary,§9.21 要求的"同一对局固定
     release track"就此断掉 —— 之后所有按轨粘滞的判定(重连、回流、灰度分流)都指错。

## 指标断言用公开 `collect()`,不摸私有字段

`FLEET_DESIRED_GAUGE` 的"未解码到就**不写**这条序列"是 §9.22 在指标面的落点:写一个
默认 0 会让 Grafana 告警规则把它当"已知为 0"而静默。要断言"没有这条序列",只能看
采集出来的样本集合。

★ 每条用例 docstring 的 `★ 变异:` 一行都**真跑过**(改坏产品代码 → 确认本条红 →
  改回 → 确认绿),不是照着代码猜的。
"""

from __future__ import annotations

import pytest

from pandorapy import errcode
from pandorapy.services.ds_allocator import capacity as CAP
from pandorapy.services.ds_allocator import gameserver as GS
from pandorapy.services.ds_allocator.agones_allocator import AgonesError, FleetCapacity
from pandorapy.services.ds_allocator.conf import AgonesConf, AllocatorConf

# ── 夹具 ────────────────────────────────────────────────────────────────────


def _cap(
    fleet: str = "pandora-battle",
    replicas: int = 10,
    ready: int = 5,
    allocated: int = 5,
    desired: int = 10,
    desired_known: bool = True,
    canary: bool = False,
) -> FleetCapacity:
    return FleetCapacity(
        fleet=fleet,
        replicas=replicas,
        ready=ready,
        allocated=allocated,
        desired=desired,
        desired_known=desired_known,
        canary=canary,
    )


class _Lister:
    """`FleetCapacityLister` 替身:回放 `(快照列表, 错误)` 二元组。"""

    def __init__(self, caps: list[FleetCapacity], err: AgonesError | None = None) -> None:
        self.caps = caps
        self.err = err
        self.calls = 0

    async def list_fleet_capacities(self) -> tuple[list[FleetCapacity], AgonesError | None]:
        self.calls += 1
        return list(self.caps), self.err


class _Clock:
    """可推进的单调钟替身(注入 `CapacityWatcher(now=...)`)。"""

    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, sec: float) -> None:
        self.t += sec


def _watcher(caps: list[FleetCapacity], warn_ratio: float = 0.8):
    clock = _Clock()
    lister = _Lister(caps)
    return CAP.CapacityWatcher(lister, 30.0, warn_ratio, now=clock), lister, clock


def _gauge_fleets(gauge) -> set[str]:  # noqa: ANN001
    """该 gauge 当前**有样本**的 fleet 集合(公开 collect 接口,不摸私有字段)。"""
    return {s.labels["fleet"] for m in gauge.collect() for s in m.samples}


def _gauge_value(gauge, fleet: str) -> float:  # noqa: ANN001
    for m in gauge.collect():
        for s in m.samples:
            if s.labels.get("fleet") == fleet:
                return s.value
    raise AssertionError(f"gauge 上没有 fleet={fleet} 的样本")


# ════════════════════════════════════════════════════════════════════════════
# deliberately_unprovisioned —— 三个条件缺一不可
# ════════════════════════════════════════════════════════════════════════════


def test_unprovisioned_requires_canary_and_known_and_zero():
    """只有 `canary and desired_known and desired == 0` 三者同时成立才算"有意不配容量"。

    守的是 INC-20260724-001 的两条保守边界。任一格放松的后果是**把真问题静音**:
      - 不看 canary  → 运维误把 **stable** 缩到 0 时不再告警,而那确实让新对局分配必失败;
      - 不看 known   → 没解码到 spec.replicas 时被当成"已知为 0",§9.22 的 UNKNOWN
                       被冒充成确定值,一次 Fleet 解码变更就能让整个容量告警体系静默。

    ★ 变异:把 `deliberately_unprovisioned` 里的 `c.desired_known and` 删掉 →
      本条 `desired_known=False` 参数红。
    """
    assert CAP.deliberately_unprovisioned(_cap(canary=True, desired=0, desired_known=True)) is True
    # 三格各缺一次:
    assert CAP.deliberately_unprovisioned(_cap(canary=False, desired=0, desired_known=True)) is False
    assert CAP.deliberately_unprovisioned(_cap(canary=True, desired=0, desired_known=False)) is False
    assert CAP.deliberately_unprovisioned(_cap(canary=True, desired=1, desired_known=True)) is False


# ════════════════════════════════════════════════════════════════════════════
# usage_ratio
# ════════════════════════════════════════════════════════════════════════════


def test_usage_ratio_normal():
    """`allocated / replicas`,普通情形。

    ★ 变异:把 `return float(c.allocated) / float(c.replicas)` 改成
      `float(c.ready) / float(c.replicas)` → 本条红。
    """
    assert CAP.usage_ratio(_cap(replicas=10, allocated=3)) == pytest.approx(0.3)


def test_usage_ratio_zero_replicas_counts_as_full():
    """`replicas == 0`(Fleet 缩到 0)按 **1.0** 计 —— 零容量即满,这是要告警的状态。

    守的是:除零不能变成"占用比 0 = 很空闲",那会让一个完全没有副本的 Fleet 在面板上
    看起来最健康。

    ★ 变异:把 `if c.replicas == 0: ... return 1.0` 改成 `return 0.0` → 本条红。
    """
    assert CAP.usage_ratio(_cap(replicas=0, allocated=0)) == 1.0


def test_usage_ratio_unprovisioned_canary_counts_as_empty():
    """有意不配容量的 canary 按 **0.0** 计。

    守的是看板可信度:它没有"占用比"可言,记 1.0 会让 Grafana 上一条常态空跑的 canary
    永远顶在 100%,看板从此不可信 —— 而"看板不可信"正是 INC-20260724-001 让真信号被
    当噪音略过的直接原因。

    ★ 变异:把 `if deliberately_unprovisioned(c): return 0.0` 删掉 → 本条红。
    """
    c = _cap(replicas=0, ready=0, allocated=0, desired=0, desired_known=True, canary=True)
    assert CAP.usage_ratio(c) == 0.0


# ════════════════════════════════════════════════════════════════════════════
# level_for —— 判定顺序就是 INC-20260724-001 的修复本身
# ════════════════════════════════════════════════════════════════════════════


def test_level_for_unprovisioned_canary_is_ok_not_exhausted():
    """未做金丝雀发布的 canary(`desired=0` 且已解码)判 **OK**,不是 exhausted。

    这条就是 INC-20260724-001 的修复:旧逻辑 `ready==0` 先于一切判定 ⇒ 恒判 exhausted,
    每 5m 一条 Error + Grafana critical 长期 firing,把真实的 stable `ready=0` 信号淹没。

    ★ 变异:把 `level_for` 里 `if deliberately_unprovisioned(c): return CAPACITY_OK`
      这两行移到 `if c.ready == 0:` **之后** → 本条红。
    """
    c = _cap(replicas=0, ready=0, allocated=0, desired=0, desired_known=True, canary=True)
    assert CAP.level_for(c, 0.8) == CAP.CAPACITY_OK


def test_level_for_stable_scaled_to_zero_still_alarms():
    """**stable 轨**被缩到 0 → 照常 EXHAUSTED。

    守的是保守边界之二:那确实让新对局分配必失败,是真问题,不能因为"是故意缩的"就静音。
    运维 / 脚本误把 stable 缩零是一个已知的手滑形状,静音等于让它无声地断掉整个战斗池。

    ★ 变异:把 `deliberately_unprovisioned` 里的 `c.canary and` 删掉 → 本条红。
    """
    c = _cap(replicas=0, ready=0, allocated=0, desired=0, desired_known=True, canary=False)
    assert CAP.level_for(c, 0.8) == CAP.CAPACITY_EXHAUSTED


def test_level_for_unknown_desired_still_alarms():
    """没解码到 `spec.replicas` 的 canary → 照常 EXHAUSTED(§9.22 不确定不冒充已知)。

    ★ 变异:把 `deliberately_unprovisioned` 里的 `c.desired_known and` 删掉 → 本条红。
    """
    c = _cap(replicas=0, ready=0, allocated=0, desired=0, desired_known=False, canary=True)
    assert CAP.level_for(c, 0.8) == CAP.CAPACITY_EXHAUSTED


def test_level_for_ready_zero_is_exhausted_even_when_ratio_low():
    """`ready == 0` 优先于占用比判定 → EXHAUSTED。

    守的是语义次序:没有空闲副本时,占用比再低也分配不出去。

    ★ 变异:把 `if c.ready == 0: return CAPACITY_EXHAUSTED` 移到占用比判定**之后**
      → 本条红。
    """
    c = _cap(replicas=10, ready=0, allocated=1)  # ratio=0.1,远低于 0.8
    assert CAP.level_for(c, 0.8) == CAP.CAPACITY_EXHAUSTED


@pytest.mark.parametrize(
    "allocated,expect",
    [
        (7, CAP.CAPACITY_OK),  # 0.7 < 0.8
        (8, CAP.CAPACITY_WARN),  # 0.8 >= 0.8:边界包含
        (9, CAP.CAPACITY_WARN),
    ],
)
def test_level_for_warn_ratio_boundary_is_inclusive(allocated: int, expect: int):
    """占用比阈值是 `>=`,不是 `>`(与 Go 逐字一致)。

    守的是预警窗口:改成 `>` 会让恰好压在阈值上的 Fleet 一条都不报,而"卡在 80%"正是
    最该被看到的那个状态。

    ★ 变异:把 `if usage_ratio(c) >= warn_ratio:` 改成 `>` → 本条 allocated=8 参数红。
    """
    c = _cap(replicas=10, ready=10 - allocated, allocated=allocated)
    assert CAP.level_for(c, 0.8) == expect


# ════════════════════════════════════════════════════════════════════════════
# CapacityWatcher.observe —— 三条规则的顺序
# ════════════════════════════════════════════════════════════════════════════


def test_observe_first_round_ok_reports_nothing():
    """首轮即 OK → 不上报(不能凭空报一条 recovered)。

    守的是降噪:每个新 Fleet 上线都刷一条"已恢复",会让这条事件失去意义。

    ★ 变异:把 `if seen and prev != CAPACITY_OK:` 的 `seen and` 删掉 → 本条红。
    """
    w, _, _ = _watcher([])
    assert w.observe(_cap(replicas=10, ready=8, allocated=2)) == CAP.EVENT_NONE


def test_observe_first_round_exhausted_reports_immediately():
    """首轮即超限 → 立即上报(`not seen` 分支)。

    守的是冷启动盲区:服务刚起来时 Fleet 就已经打满,必须马上有信号,不能等到"下一次
    升档"—— 而它已经在最高档,永远不会再升。

    ★ 变异:把 `if level > prev or not seen:` 的 `or not seen` 删掉 → 本条红。
    """
    w, _, _ = _watcher([])
    assert w.observe(_cap(replicas=10, ready=0, allocated=10)) == CAP.EVENT_EXHAUSTED


def test_observe_throttles_same_level_within_rewarn_interval():
    """同档持续超限,距上次告警 < 5m → 不重报。

    守的是 INC-20260724-001 的成因面:30s 一条 Error 会把真信号淹掉。

    ★ 变异:把 `elif now - self._last_warn_at.get(...) < REWARN_INTERVAL_SEC: return
      EVENT_NONE` 改成 `pass` → 本条红。
    """
    w, _, clock = _watcher([])
    c = _cap(replicas=10, ready=0, allocated=10)
    assert w.observe(c) == CAP.EVENT_EXHAUSTED
    clock.advance(CAP.REWARN_INTERVAL_SEC - 1)
    assert w.observe(c) == CAP.EVENT_NONE


def test_observe_rewarns_after_interval():
    """同档持续超限,距上次告警 >= 5m → 重报一条。

    守的是另一半:长时间高水位不能彻底静默,否则问题会一直躺在那里没人看。

    ★ 变异:把 `REWARN_INTERVAL_SEC` 改成一个巨大值(如 `5 * 60 * 1000.0`)→ 本条红。
    """
    w, _, clock = _watcher([])
    c = _cap(replicas=10, ready=0, allocated=10)
    assert w.observe(c) == CAP.EVENT_EXHAUSTED
    clock.advance(CAP.REWARN_INTERVAL_SEC)
    assert w.observe(c) == CAP.EVENT_EXHAUSTED


def test_observe_escalation_reports_immediately_without_throttle():
    """warn → exhausted 是**升档**,不受重报间隔限制。

    守的是及时性:从"接近上限"恶化到"完全打满"是状态质变,压在 5m 节流里等于让运维
    在最需要动作的那几分钟里什么都收不到。

    ★ 变异:把 `if level > prev or not seen:` 改成 `if not seen:` → 本条红。
    """
    w, _, clock = _watcher([])
    assert w.observe(_cap(replicas=10, ready=1, allocated=9)) == CAP.EVENT_NEAR_LIMIT
    clock.advance(1.0)  # 远小于重报间隔
    assert w.observe(_cap(replicas=10, ready=0, allocated=10)) == CAP.EVENT_EXHAUSTED


def test_observe_recovery_reports_and_clears_throttle_state():
    """回落到 OK → 上报 recovered,并**清掉**上次告警时刻。

    守的是"恢复后再次超限必须立刻重报":不清 `_last_warn_at` 的话,恢复后 1 分钟内再次
    打满会被 5m 节流吞掉 —— 而那是一次全新的故障。

    ★ 变异:把 `self._last_warn_at.pop(c.fleet, None)` 这行删掉 → 本条最后一句断言红。
    """
    w, _, clock = _watcher([])
    assert w.observe(_cap(replicas=10, ready=0, allocated=10)) == CAP.EVENT_EXHAUSTED
    clock.advance(10.0)
    assert w.observe(_cap(replicas=10, ready=8, allocated=2)) == CAP.EVENT_RECOVERED
    clock.advance(10.0)
    # 恢复后再次打满:虽然距上次告警仅 20s,仍必须立刻重报。
    assert w.observe(_cap(replicas=10, ready=0, allocated=10)) == CAP.EVENT_EXHAUSTED


def test_observe_tracks_each_fleet_independently():
    """状态机按 Fleet 分桶,一个 Fleet 的告警不得吃掉另一个的。

    守的是多 Fleet 部署(通用池 + 各 map_fleets + canary):共用一个状态会让先超限的那个
    把其余全部静音。
    """
    w, _, _ = _watcher([])
    assert w.observe(_cap(fleet="a", replicas=10, ready=0, allocated=10)) == CAP.EVENT_EXHAUSTED
    assert w.observe(_cap(fleet="b", replicas=10, ready=0, allocated=10)) == CAP.EVENT_EXHAUSTED


def test_observe_first_warn_is_not_swallowed_by_zero_default():
    """首条告警不得被"上次告警时刻缺省值"吞掉。

    守的是一个很容易写错的细节:缺省取 0 时,单调钟刚启动(`now` 很小)会让
    `now - 0 < 300` 成立,**首条告警直接消失**。Go 那边缺省是零值 `time.Time`(公元 1 年),
    `now.Sub(zero)` 必然是巨大正值;等价物是 `-inf` 而不是 0。

    ★ 变异:把 `self._last_warn_at.get(c.fleet, float("-inf"))` 的缺省改成 `0.0`,
      并把本用例的时钟起点设成 1.0 → 本条红。
    """
    lister = _Lister([])
    clock = _Clock(1.0)  # 单调钟刚启动
    w = CAP.CapacityWatcher(lister, 30.0, 0.8, now=clock)
    # 先制造一次"已 seen 且同档"的情形,逼它走重报间隔那条分支。
    c = _cap(replicas=10, ready=0, allocated=10)
    assert w.observe(c) == CAP.EVENT_EXHAUSTED
    assert w._last_warn_at["pandora-battle"] == 1.0


# ════════════════════════════════════════════════════════════════════════════
# poll_once —— 部分失败不影响其余 + desired 序列的"不写"语义
# ════════════════════════════════════════════════════════════════════════════


async def test_poll_once_processes_caps_even_when_error_present():
    """部分 Fleet 查询失败(err 非 None)时,**已拿到的快照照常刷指标**。

    守的是:`caps` 与 `err` 是两件独立的事。写成 `if err: return` 等于用一次抖动把已经
    拿到的水位全丢掉 —— 而水位正是本巡检唯一的产出。

    ★ 变异:在 `poll_once` 里把 `plog.get().warning(...)` 后面加一行 `return`
      → 本条红。
    """
    fleet = "poll-partial-fail"
    lister = _Lister([_cap(fleet=fleet, replicas=4, ready=1, allocated=3)], err=AgonesError("x"))
    w = CAP.CapacityWatcher(lister, 30.0, 0.8, now=_Clock())
    await w.poll_once()
    assert _gauge_value(CAP.FLEET_REPLICAS_GAUGE, fleet) == 4.0
    assert _gauge_value(CAP.FLEET_READY_GAUGE, fleet) == 1.0
    assert _gauge_value(CAP.FLEET_ALLOCATED_GAUGE, fleet) == 3.0
    assert _gauge_value(CAP.FLEET_USAGE_RATIO_GAUGE, fleet) == pytest.approx(0.75)


async def test_poll_once_omits_desired_series_when_not_decoded():
    """没解码到 `spec.replicas` → **不写** desired 序列(不是写 0)。

    守的是 §9.22 在指标面的落点:写一个默认 0 会让 Grafana 的
    「desired>0 且 ready==0 才 critical」规则把它当"已知为 0"而静默 —— 于是一个真正
    打满的 Fleet 因为解码失败反而不告警了。不写该序列,规则会退化回原判据继续告警。

    ★ 变异:把 `if c.desired_known:` 删掉(无条件 Set)→ 本条红。
    """
    known = "poll-desired-known"
    unknown = "poll-desired-unknown"
    lister = _Lister(
        [
            _cap(fleet=known, desired=12, desired_known=True),
            _cap(fleet=unknown, desired=0, desired_known=False),
        ]
    )
    w = CAP.CapacityWatcher(lister, 30.0, 0.8, now=_Clock())
    await w.poll_once()
    fleets = _gauge_fleets(CAP.FLEET_DESIRED_GAUGE)
    assert known in fleets
    assert unknown not in fleets
    assert _gauge_value(CAP.FLEET_DESIRED_GAUGE, known) == 12.0


# ════════════════════════════════════════════════════════════════════════════
# new_capacity_watcher —— "负 = 禁用" 与 "0 = 默认" 是两回事
# ════════════════════════════════════════════════════════════════════════════


def test_new_watcher_negative_interval_disables():
    """`capacity_watch_interval` 为负 = **显式禁用**,返回 None。

    守的是运维的关闭开关。合并成 `<= 0 用默认` 的后果:再也关不掉这个巡检,而它会对
    apiserver 持续发 GET。

    ★ 变异:把 `if interval < 0: return None` 删掉 → 本条红。
    """
    cfg = AgonesConf(capacity_watch_interval="-1s", capacity_warn_ratio=0.8)
    assert CAP.new_capacity_watcher(_Lister([]), cfg) is None


def test_new_watcher_zero_interval_takes_default():
    """`capacity_watch_interval` 为 0 / 未配 = 取默认 30s(零值代表"用默认",不是禁用)。

    ★ 变异:把 `if interval == 0: interval = DEFAULT_WATCH_INTERVAL_SEC` 删掉 → 本条红
      (interval 会是 0,巡检退化成忙循环)。
    """
    w = CAP.new_capacity_watcher(_Lister([]), AgonesConf(capacity_watch_interval=""))
    assert w is not None
    assert w.interval_sec == CAP.DEFAULT_WATCH_INTERVAL_SEC == 30.0


@pytest.mark.parametrize("bad", [0.0, -0.5, 1.5, 100.0])
def test_new_watcher_rejects_out_of_range_warn_ratio(bad: float):
    """`capacity_warn_ratio` 落在 `(0, 1]` 之外 → 取默认 0.8。

    守的是预警有效性:配成 0 会让每个 Fleet 恒判 WARN(刷屏),配成 1.5 会让 WARN 永远
    不触发(静默)。两个方向都让这道预警失去意义。

    ★ 变异:把 `if warn_ratio <= 0 or warn_ratio > 1:` 改成 `if warn_ratio < 0:`
      → 本条 0.0 / 1.5 / 100.0 三个参数红。
    """
    cfg = AgonesConf(capacity_watch_interval="30s", capacity_warn_ratio=bad)
    w = CAP.new_capacity_watcher(_Lister([]), cfg)
    assert w is not None
    assert w.warn_ratio == CAP.DEFAULT_WARN_RATIO == 0.8


@pytest.mark.parametrize("ok", [0.5, 0.8, 1.0])
def test_new_watcher_keeps_in_range_warn_ratio(ok: float):
    """`(0, 1]` 内的值原样保留 —— 含**上界 1.0**(只在完全打满时才 WARN 是合法配置)。"""
    cfg = AgonesConf(capacity_watch_interval="30s", capacity_warn_ratio=ok)
    w = CAP.new_capacity_watcher(_Lister([]), cfg)
    assert w is not None
    assert w.warn_ratio == ok


# ════════════════════════════════════════════════════════════════════════════
# MockGameServerAllocator
# ════════════════════════════════════════════════════════════════════════════


def _mock_cfg(base: int = 30000, rng: int = 1000, host: str = "127.0.0.1") -> AllocatorConf:
    return AllocatorConf(
        mock_ds_port_base=base, mock_ds_port_range=rng, mock_ds_addr_host=host
    )


async def test_mock_allocate_is_deterministic_by_match_id():
    """端口 = `base + match_id % range`,同一 match 多次分配地址稳定。

    守的是幂等场景下的可复现性(重复请求必须落回同一处,不能每次换地址)。

    ★ 变异:把 `port = self._cfg.mock_ds_port_base + (match_id % port_range)` 改成
      `+ (match_id % port_range) + 1` → 本条红。
    """
    a = GS.MockGameServerAllocator(_mock_cfg())
    pod, addr, track = await a.allocate(1234, 4002, "pve_coop", "stable")
    assert pod == "pandora-battle-1234"
    assert addr == "127.0.0.1:30234"
    assert track == "stable"
    assert await a.allocate(1234, 4002, "pve_coop", "stable") == (pod, addr, track)


async def test_mock_allocate_returns_release_track_verbatim():
    """`release_track` **原样回传**,不得改写成 "stable"。

    守的是 §9.21「同一对局固定 release track」。Mock 也要遵守"回传实际命中的轨"这条
    约定,否则用 mock 跑的灰度用例会给出一个**生产上不成立**的结论 —— 而灰度粘滞是靠
    这个返回值往下游传的。

    ★ 变异:把 `return pod_name, addr, release_track` 改成
      `return pod_name, addr, "stable"` → 本条红。
    """
    a = GS.MockGameServerAllocator(_mock_cfg())
    _, _, track = await a.allocate(7, 4002, "pvp", "canary")
    assert track == "canary"


@pytest.mark.parametrize("bad", [True, False, "1", 1.0, None])
async def test_mock_allocate_rejects_non_int_match_id(bad):  # noqa: ANN001
    """`match_id` 必须是真正的 int(`bool` 也拒)。

    守的是:`isinstance(True, int)` 在 Python 里为真,不显式排除的话 `True % 1000 == 1`
    会算出一个"看起来正常"的端口,把一个类型缺陷伪装成一次成功分配。

    ★ 变异:把 `if not isinstance(match_id, int) or isinstance(match_id, bool):` 里的
      `or isinstance(match_id, bool)` 删掉 → 本条 True/False 两个参数红。
    """
    a = GS.MockGameServerAllocator(_mock_cfg())
    with pytest.raises(errcode.PandoraError) as ei:
        await a.allocate(bad, 4002, "pvp", "stable")
    assert ei.value.code == errcode.ErrInvalidArg


@pytest.mark.parametrize("bad", [-1, 1 << 64])
async def test_mock_allocate_rejects_out_of_uint64_range(bad: int):
    """`match_id` 必须落在 uint64 域内。

    守的是跨栈一致:Go 的 `matchID % uint64(range)` 在 64 位无符号域里算,Python 整数
    **不回绕**。不判界的话同一个越界 match_id 在两栈上会算出不同的端口,而 Go 那侧压根
    走不到这里(类型系统先挡了)。

    ★ 变异:把 `if match_id < 0 or match_id > _UINT64_MAX:` 改成 `if False:` → 本条红。
    """
    a = GS.MockGameServerAllocator(_mock_cfg())
    with pytest.raises(errcode.PandoraError) as ei:
        await a.allocate(bad, 4002, "pvp", "stable")
    assert ei.value.code == errcode.ErrInvalidArg


@pytest.mark.parametrize("rng", [0, -1])
async def test_mock_allocate_rejects_non_positive_port_range(rng: int):
    """`mock_ds_port_range <= 0` → 带业务码的 `ErrInvalidArg`,不是 `ZeroDivisionError`。

    守的是可诊断性:裸 `ZeroDivisionError` 在 access log 里和"随便哪里的 bug"没有区别,
    而这里是一个说得清是哪个配置项写错了的问题。
    (Go 侧此处会直接 panic —— 这是本移植**有意**的行为增强,不是语义放宽。)

    ★ 变异:把 `if port_range <= 0: raise ...` 删掉 → 本条 rng=0 参数会变成
      ZeroDivisionError、rng=-1 会算出负端口,两者都红。
    """
    a = GS.MockGameServerAllocator(_mock_cfg(rng=rng))
    with pytest.raises(errcode.PandoraError) as ei:
        await a.allocate(1234, 4002, "pvp", "stable")
    assert ei.value.code == errcode.ErrInvalidArg


async def test_mock_release_is_noop():
    """Mock 没有真实 pod 可回收,`release` 恒 no-op。"""
    a = GS.MockGameServerAllocator(_mock_cfg())
    assert await a.release("pandora-battle-1") is None


# ════════════════════════════════════════════════════════════════════════════
# 能力边界:local 专属 Protocol 是**结构性**死代码,不是靠注释保证的
# ════════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "proto",
    [
        GS.LocalInstanceIdentitySource,
        GS.LocalBattleCredentialSource,
        GS.LocalBattleRosterSink,
    ],
)
def test_mock_allocator_does_not_implement_local_only_protocols(proto):  # noqa: ANN001
    """Mock(与生产 Agones 同)**不实现** local 专属能力。

    守的是一条结构性事实:调用方那几条 legacy 回填分支在生产(Model B)与离线 mock 下
    是**机械死代码** —— 两条路径的分配结果与心跳应答逐字节不变。一旦把这些方法合并进
    主接口(或顺手在 Mock 上补一个"返回不支持"的实现),"生产上这条分支不可达"就从结构性
    事实降级成了一句需要人去核对的注释。

    ★ 变异:给 `MockGameServerAllocator` 加一个
      `def local_instance_identity(self, pod_name): return None` → 本条第一个参数红。
    """
    assert isinstance(GS.MockGameServerAllocator(_mock_cfg()), proto) is False


def test_warming_prober_and_uncertain_resolver_are_optional_capabilities():
    """`WarmingInstanceProber` / `UncertainGameServerAllocationResolver` 也是可选能力。

    守的是 fail-closed 的默认档:不懂对账的旧写者与测试替身必须继续停在
    `allocation_uncertain` 这道永久 fence 上,而不是因为多了个方法签名就被当成"支持对账";
    不实现探测能力的分配器必须**回退到时间界**,把"探不了"当成"已死"会在 DS 还在冷加载时
    把整局判弃。
    """
    a = GS.MockGameServerAllocator(_mock_cfg())
    assert isinstance(a, GS.WarmingInstanceProber) is False
    assert isinstance(a, GS.UncertainGameServerAllocationResolver) is False
