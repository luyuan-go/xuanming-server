"""好友在线态扇出 fan-out worker —— 对应 Go 侧 internal/biz/presence.go。

落地 docs/design/friend-distributed-scaling.md §13.4 / §13.5:

    §13.4.1 只推订阅者:内存订阅倒排索引(watchedID → 订阅者集合);
             好友上线事件只推给「此刻正盯着这一行看的人」,扇出从 N 降到个位数。
    §13.4.2 去抖(debounce):变更进窗口,窗口内回退到原状态判为抖动不推。
    §13.4.3 合并(coalesce):tick(默认 1s)把同一订阅者的多条变更攒成一条批次。
    §13.4.4 降采样:只推粗粒度 PresenceStatus(在线/离线/游戏中)。
    §13.5   洪峰降级:挂 killswitch,降级时丢事件退回纯拉模式,保主链路。

架构取舍(与 Go 同):订阅倒排索引是**单实例内存态**。多实例水平扩展需把倒排索引
下沉 Redis 并让 presence 变更走 Kafka 分区到单一 fan-out 消费组 —— 列为后续。
本版默认 presence.enabled=false(§13.7 先拉后推)。

★ 与 Go 的一处实现差异(行为等价):Go 用 sync.Mutex 保护共享 map,Python 侧是
  单线程事件循环 + 全同步的临界区(collect_batches 内没有 await),天然互斥,
  因此不另加锁。推送 I/O 仍然在临界区**之外**做 —— 这一点必须保持:
  在临界区里 await 会让 Notify/Subscribe 插进来改到正在遍历的 dict。
"""

from __future__ import annotations

import dataclasses
import time
from typing import Any, Callable, Protocol

from pandora.locator.v1 import locator_pb2

from pandorapy import log as plog
from pandorapy.services.player_locator import biz as lbiz

# 粗粒度在线态 —— **直接引用 proto 生成物**,不手抄数值(本仓刚修完 13 处手抄错位)。
_P = locator_pb2.PresenceStatus
PRESENCE_UNSPECIFIED = _P.PRESENCE_STATUS_UNSPECIFIED
PRESENCE_OFFLINE = _P.PRESENCE_STATUS_OFFLINE
PRESENCE_ONLINE = _P.PRESENCE_STATUS_ONLINE
PRESENCE_IN_GAME = _P.PRESENCE_STATUS_IN_GAME

# 无配置时的默认窗口(与 Go 同值)。
DEFAULT_DEBOUNCE_WINDOW_SEC = 8.0
DEFAULT_COALESCE_TICK_SEC = 1.0


def coarse_presence(state: int) -> int:
    """细粒度 LocationState → 粗粒度在线态(§13.4.4 降采样)。"""
    if state in (lbiz.LOCATION_STATE_LOGIN_PENDING, lbiz.LOCATION_STATE_HUB):
        return PRESENCE_ONLINE
    if state in (lbiz.LOCATION_STATE_MATCHING, lbiz.LOCATION_STATE_BATTLE):
        return PRESENCE_IN_GAME
    return PRESENCE_OFFLINE  # Unspecified / Offline


@dataclasses.dataclass(slots=True)
class PresenceChangeOut:
    """推给订阅者的单条变更(→proto 由 pusher 适配)。"""

    player_id: int
    status: int
    ts_ms: int


class PresencePusher(Protocol):
    """把合并后的一批变更推给某订阅者本人(适配 kafka→push 服务)。"""

    async def push_presence(self, subscriber_id: int, changes: list[PresenceChangeOut]) -> None:
        ...


@dataclasses.dataclass(slots=True)
class _PendingChange:
    """去抖窗口内某玩家待结算的最新变更。"""

    status: int
    ts_ms: int
    deadline: float  # 首次变更时刻 + debounce 窗口;到点才结算广播


@dataclasses.dataclass(slots=True)
class _Batch:
    sub: int
    changes: list[PresenceChangeOut]


class PresenceHub:
    """presence 订阅 + 去抖 + 合并 fan-out worker。

    与 Go 的 PresenceHub 逐方法对应;`step()` 单独暴露是为了单测能用受控时钟驱动。
    """

    def __init__(
        self,
        pusher: PresencePusher | None,
        debounce_sec: float,
        tick_sec: float,
        kill_switch: Callable[[], tuple[bool, str]] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._pusher = pusher
        self._ks = kill_switch
        self._debounce = debounce_sec if debounce_sec > 0 else DEFAULT_DEBOUNCE_WINDOW_SEC
        self._tick = tick_sec if tick_sec > 0 else DEFAULT_COALESCE_TICK_SEC
        self._clock = clock or time.monotonic

        self._watchers: dict[int, set[int]] = {}  # watchedID → 订阅者集合
        self._watching: dict[int, set[int]] = {}  # subscriberID → 关注集合(清理用)
        self._pending: dict[int, _PendingChange] = {}
        self._last_sent: dict[int, int] = {}  # watchedID → 上次已广播的粗状态
        self._buffer: dict[int, dict[int, PresenceChangeOut]] = {}
        self._degraded = False

    @property
    def tick_sec(self) -> float:
        return self._tick

    # ── 订阅面 ────────────────────────────────────────────────────────────

    def subscribe(self, subscriber_id: int, watched_ids: list[int]) -> None:
        """注册订阅者关注的一批好友(§13.4.1)。

        「替换」语义:每次打开/刷新好友面板都发全量 watched_ids,覆盖旧订阅。
        subscriber_id==0 或空列表视为退订。
        """
        if subscriber_id == 0:
            return
        self._unsubscribe_locked(subscriber_id)
        if not watched_ids:
            return
        s: set[int] = set()
        for wid in watched_ids:
            if wid == 0 or wid == subscriber_id:
                continue  # 跳过非法 id 与自订阅
            s.add(wid)
            self._watchers.setdefault(wid, set()).add(subscriber_id)
        if s:
            self._watching[subscriber_id] = s

    def unsubscribe(self, subscriber_id: int) -> None:
        if subscriber_id == 0:
            return
        self._unsubscribe_locked(subscriber_id)

    def _unsubscribe_locked(self, subscriber_id: int) -> None:
        for wid in self._watching.get(subscriber_id, ()):
            ws = self._watchers.get(wid)
            if ws is not None:
                ws.discard(subscriber_id)
                if not ws:
                    del self._watchers[wid]
        self._watching.pop(subscriber_id, None)
        self._buffer.pop(subscriber_id, None)

    # ── 变更入口 ──────────────────────────────────────────────────────────

    def notify(self, player_id: int, state: int) -> None:
        """上报某玩家的(细粒度)位置变更;内部转粗粒度并进去抖窗口。

        由 usecase 在 SetLocation / ClearLocation **成功后**调用,非阻塞。
        """
        if player_id == 0:
            return
        status = coarse_presence(state)
        now = self._clock()
        cur = self._pending.get(player_id)
        if cur is not None:
            # 去抖窗口内再次变更:保留原 deadline(持续 hold),只更新到最新状态/时间。
            cur.status = status
            cur.ts_ms = int(time.time() * 1000)
            return
        self._pending[player_id] = _PendingChange(
            status=status, ts_ms=int(time.time() * 1000), deadline=now + self._debounce
        )

    # ── tick ─────────────────────────────────────────────────────────────

    async def step(self, now: float | None = None) -> None:
        """单次 tick:降级判定 → 去抖结算 → 合并 flush(推送在临界区外)。"""
        batches = self._collect_batches(self._clock() if now is None else now)
        if self._pusher is None:
            return
        # 模式 C:本 flush 由 ticker(默认 1s)驱动,对每个有缓冲变更的订阅者各推一次。
        # push 不可用时逐订阅者打 Warn = 每秒数百条同因日志 → 改为本轮累加、轮末汇总一条。
        failed = 0
        failed_changes = 0
        first_err: Any = None
        sample_sub = 0
        for b in batches:
            try:
                await self._pusher.push_presence(b.sub, b.changes)
            except Exception as exc:  # noqa: BLE001 —— 单个订阅者失败不中断整轮
                failed += 1
                failed_changes += len(b.changes)
                if first_err is None:
                    first_err, sample_sub = exc, b.sub
        if failed > 0:
            plog.get().warning(
                "presence_push_failed",
                subscribers=len(batches),
                failed=failed,
                failed_changes=failed_changes,
                sample_subscriber_id=sample_sub,
                first_err=str(first_err),
            )

    def _collect_batches(self, now: float) -> list[_Batch]:
        """临界区:降级判定 + 去抖结算 + 抽批清缓冲。**内部不得有 await**。"""
        logger = plog.get()

        # 1. 洪峰降级(§13.5):退回纯拉,丢弃在途事件。
        if self._ks is not None:
            disabled, reason = self._ks()
            if disabled:
                if not self._degraded:
                    self._degraded = True
                    logger.warning("presence_fanout_degraded", reason=reason)
                self._pending = {}
                self._buffer = {}
                return []
            if self._degraded:
                self._degraded = False
                logger.info("presence_fanout_recovered")

        # 2. 去抖结算:deadline 到点 → 取窗口内最终状态,与上次广播比对。
        for wid in list(self._pending.keys()):
            pc = self._pending[wid]
            if pc.deadline > now:
                continue  # 窗口未到,继续 hold
            del self._pending[wid]

            prev = self._last_sent.get(wid, PRESENCE_OFFLINE)  # 缺省基线 = 离线
            if pc.status == prev:
                continue  # 抖动吸收 / 无净变化:不推
            if pc.status == PRESENCE_OFFLINE:
                self._last_sent.pop(wid, None)  # 离线 = 基线,回收内存
            else:
                self._last_sent[wid] = pc.status

            change = PresenceChangeOut(player_id=wid, status=pc.status, ts_ms=pc.ts_ms)
            for sub in self._watchers.get(wid, ()):
                self._buffer.setdefault(sub, {})[wid] = change

        # 3. 抽出本 tick 要推的批次,清空缓冲(推送在临界区外做)。
        batches = [
            _Batch(sub=sub, changes=list(sb.values()))
            for sub, sb in self._buffer.items()
            if sb
        ]
        self._buffer = {}
        return batches
