"""auction 保留期清理 —— 对应 Go 侧 internal/data/retention.go(CLAUDE.md §9 不变量 24)。

三张只增表随挂单 / 成交量无界线性增长,逐分片批删超保留期的行:

  auction_orders            终态(FILLED/CANCELED/EXPIRED)且 release_pending=0
                            且 match_pending=0(escrow 已释放、无续跑意图)且 updated_at_ms 超期。
                            uk_owner_idem 幂等键随行删除:客户端挂单幂等键是每次请求生成的,
                            重试窗口分钟级,90 天后同 key 重放不可能是同一笔业务请求。
  auction_matches           settlement_status=COMPLETED 且 event_pending=0 且 matched_at_ms 超期
                            (结算/事件补偿只扫 PENDING 行,删已完成行不影响补偿闭环)。
  auction_idempotency_keys  created_at_ms 超期。★ 它在 **owner 分片**而 orders 在 **market 分片**,
                            无法 join 删,只能按创建时间独立清理。

**不清理**:auction_owner_guards(每 owner 一行,被玩家数有界)、
auction_shard_topology(单行 marker)。

★ mode 默认 REPORT_ONLY:只统计各表待清理量并 WARN,**一行都不删**
  (2026-07-22 用户指令:不允许"因为数据大了"自动删数据)。
  真删必须由配置显式写 `retention_mode: delete`。

★ 三类的 Count 与 Delete 共用同一个 where(dbguard.sweep_table 保证),条件只写一遍
  —— 从机制上排除"报告说 0 行、实际删了 10 万行"的条件漂移。
"""

from __future__ import annotations

import dataclasses

from pandorapy import dbguard
from pandorapy import errcode
from pandorapy.services.auction.repo import (
    SETTLEMENT_COMPLETED,
    STATUS_CANCELED,
    STATUS_EXPIRED,
    STATUS_FILLED,
    DBRouter,
)


@dataclasses.dataclass(slots=True)
class RetentionOutcome:
    """一轮三类清理的汇总(逐分片累加)。"""

    orders_matched: int = 0
    orders_deleted: int = 0
    matches_matched: int = 0
    matches_deleted: int = 0
    idem_keys_matched: int = 0
    idem_keys_deleted: int = 0

    def cleaned(self) -> bool:
        """本轮是否真的删了数据(供调用方决定日志措辞)。"""
        return self.orders_deleted + self.matches_deleted + self.idem_keys_deleted > 0

    def pending_total(self) -> int:
        """三类待清理量之和(report_only 下的积压规模)。"""
        return self.orders_matched + self.matches_matched + self.idem_keys_matched


async def sweep_retention(
    router: DBRouter, mode: dbguard.Mode, cutoff_ms: int, limit: int
) -> RetentionOutcome:
    """对所有分片跑一轮保留期清理(每分片每表至多一批 limit 行)。

    任一分片 / 表失败立即上抛(下一轮重试,不破坏幂等)。
    逐分片累加是"整个 auction 域"的口径;metric 由 sweep_table 按 db+table 打
    (分片间同 label 会互相覆盖 gauge —— 这是有意的近似:分片容量应大致均衡,
    单分片异常会被 dbcheck 逐库巡检抓到)。
    """
    out = RetentionOutcome()
    for shard in router.shards():
        async with shard.pool.acquire() as conn:
            try:
                o = await dbguard.sweep_table(
                    conn,
                    mode,
                    shard.schema,
                    "auction_orders",
                    "status IN (%s, %s, %s) AND release_pending = 0 AND match_pending = 0 "
                    "AND updated_at_ms < %s",
                    limit,
                    STATUS_FILLED,
                    STATUS_CANCELED,
                    STATUS_EXPIRED,
                    cutoff_ms,
                )
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "sweep terminal orders: %s", exc
                ) from exc
            out.orders_matched += o.matched
            out.orders_deleted += o.deleted

            try:
                m = await dbguard.sweep_table(
                    conn,
                    mode,
                    shard.schema,
                    "auction_matches",
                    "settlement_status = %s AND event_pending = 0 AND matched_at_ms < %s",
                    limit,
                    SETTLEMENT_COMPLETED,
                    cutoff_ms,
                )
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "sweep settled matches: %s", exc
                ) from exc
            out.matches_matched += m.matched
            out.matches_deleted += m.deleted

            try:
                k = await dbguard.sweep_table(
                    conn,
                    mode,
                    shard.schema,
                    "auction_idempotency_keys",
                    "created_at_ms < %s",
                    limit,
                    cutoff_ms,
                )
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "sweep idempotency keys: %s", exc
                ) from exc
            out.idem_keys_matched += k.matched
            out.idem_keys_deleted += k.deleted
    return out
