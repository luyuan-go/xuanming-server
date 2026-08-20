"""auction 的库容量预算 —— 对应 Go 侧 internal/data/budgets.go(CLAUDE.md §9.24)。

口径见 inventory/budgets.py 头注释:启动即跑一轮拿基线,之后周期巡检;
超限打 ERROR + metric,**不阻止启动**(容量超限是"要去查的问题",不是"服务不能跑的理由")。

★ 分片时预算按**单分片**给:main 逐分片各建一个巡检,各分片各自比对,
  不把总量摊在一个分片上。少了这条,两分片部署下每片都要涨到总预算才告警 ——
  等于把阈值悄悄放宽了一倍。

★ 数值必须与 Go 逐个相同。分叉的后果很隐蔽:同一个库在 Go 版上告警、Python 版不告警
  (或反过来),而两边都"正常运行"。
"""

from __future__ import annotations

from pandorapy import dbguard

# 容量规划:日活 1 万 × 每人每天 5 单。
PLAN_DAILY_ORDERS = 50_000


def auction_budgets() -> list[dbguard.TableBudget]:
    """pandora_auction **单个分片**的容量预算。"""
    return [
        dbguard.TableBudget(
            # 终态行保留 90 天:5 万单/天 × 90 × 3。
            table="auction_orders",
            max_rows=PLAN_DAILY_ORDERS * 90 * 3,
            max_avg_row_bytes=384,
            note="挂单/出价;终态行保留 90 天,超限先查 auction 保留期清理是否在跑"
            "(日志 auction_retention_swept)",
        ),
        dbguard.TableBudget(
            table="auction_matches",
            max_rows=PLAN_DAILY_ORDERS * 90 * 3,
            max_avg_row_bytes=256,
            note="成交流水;已结算行保留 90 天。堆积也可能是结算/事件补偿链堵塞"
            "(PENDING 行不清理)",
        ),
        dbguard.TableBudget(
            table="auction_idempotency_keys",
            max_rows=PLAN_DAILY_ORDERS * 90 * 3,
            max_avg_row_bytes=192,
            note="owner+key canonical 映射,保留 90 天",
        ),
        dbguard.TableBudget(
            table="auction_owner_guards",
            max_rows=100_000 * 3,
            max_avg_row_bytes=128,
            note="§9.24 登记豁免:每 owner 一行,被玩家数有界",
        ),
        dbguard.TableBudget(
            table="auction_shard_topology",
            max_rows=10,
            max_avg_row_bytes=512,
            note="单行拓扑 marker;多于 1 行说明拓扑被污染",
        ),
    ]
