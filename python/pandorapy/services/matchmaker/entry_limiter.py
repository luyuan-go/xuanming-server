"""进场侧限流适配 —— 对应 Go 侧 internal/data/entry_limiter.go
(anti-abuse-scene-entry.md §6 第 2/3/7/8 项)。

全部委托 pandorapy.redisx 的限流原语;语义边界 = **背压非权威门**(§2 铁律):
Redis 故障一律 fail-open(原语已内建 allow=True + 异常回传,biz 层 Warn 留证),
一人一票的正确性仍由 durable start operation 的 SETNX、player claim、
locator BATTLE 门兜底。把背压门做成 fail-closed 会让一次 Redis 抖动变成全服进不了场。

key 契约(登记于 docs/design/infra.md §3.2;跨服务共享的 noshow key 由
ds_allocator 写、本服务读,两端都经 redisx.rl_key 构造,**不得各自拼字符串**):

    pandora:rl:match:start:<captain_id>      StartMatch per-队长冷却
    pandora:rl:match:form:<ticket_id>        成局级冷却(含容量耗尽静默窗)
    pandora:rl:match:noshowcd:<player_id>    no-show 进入侧退避(写者 ds_allocator)

★ 刻意只按队长(captain_id)计,不按 team_id:captain_id 来自 JWT(service 层
  callerID),攻击者只能占用自己作为队长的键,天然自限;而 team_id 来自请求体、
  未经校验,若按它占坑会变成「刷任意 team_id 压制他人队伍进场」的定向骚扰原语
  —— 冷却门在成员校验**之前**,所以那道门自己会变成攻击面。
"""

from __future__ import annotations

from pandorapy import redisx

DOMAIN = "match"


class RedisEntryLimiter:
    """实现 biz 需要的 EntryRateLimiter 面。所有方法把「故障」如实交回调用方。"""

    __slots__ = ("_rdb",)

    def __init__(self, rdb) -> None:  # noqa: ANN001
        self._rdb = rdb

    async def try_start_cooldown(
        self, captain_id: int, team_id: int, window_sec: float
    ) -> tuple[bool, Exception | None]:
        """占用队长冷却窗。team_id 参数保留在签名里仅为接口稳定,不参与占坑(见模块头)。"""
        return await redisx.cooldown(
            self._rdb, redisx.rl_key(DOMAIN, "start", captain_id), window_sec
        )

    async def clear_start_cooldown(self, captain_id: int, team_id: int) -> None:
        """业务失败时释放冷却(先占坑 → 干活 → 失败释放,§9.20 立即可重试)。"""
        await redisx.clear_cooldown(self._rdb, redisx.rl_key(DOMAIN, "start", captain_id))

    async def try_form_cooldown(
        self, ticket_id: int, window_sec: float
    ) -> tuple[bool, Exception | None]:
        """成局提交前占用本票据的成局冷却窗(首次 SETNX 成功即零延迟放行)。"""
        return await redisx.cooldown(
            self._rdb, redisx.rl_key(DOMAIN, "form", ticket_id), window_sec
        )

    async def in_form_cooldown(self, ticket_id: int) -> tuple[bool, Exception | None]:
        """只读探测(撮合组队路径:组内任一票据在窗内则本轮跳过该组合)。"""
        remaining, err = await redisx.penalty_remaining(
            self._rdb, redisx.rl_key(DOMAIN, "form", ticket_id)
        )
        return remaining > 0, err

    async def arm_form_cooldown(self, ticket_id: int, window_sec: float) -> Exception | None:
        """无条件布设成局冷却窗(容量耗尽退票时用更长的静默窗**覆盖**已有窗)。"""
        return await redisx.arm_penalty(
            self._rdb, redisx.rl_key(DOMAIN, "form", ticket_id), window_sec
        )

    async def no_show_penalty_remaining(self, player_id: int) -> tuple[float, Exception | None]:
        """读 no-show 进入侧退避剩余秒数(0 = 无惩罚;写者是 ds_allocator)。"""
        return await redisx.penalty_remaining(
            self._rdb, redisx.rl_key(DOMAIN, "noshowcd", player_id)
        )
