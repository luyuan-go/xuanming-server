"""pandora_owner 库的容量预算(§9.24)—— 对应 Go 侧 internal/data/budgets.go。

上限值按**设计期望**定,不按列类型上限定 —— 写成列类型上限等于没设。
超预算只告警不阻断:容量超限是"要去查的问题",不是"服务不能跑的理由"
(拒绝启动会把容量问题升级成可用性事故)。
"""

from __future__ import annotations

from pandorapy.dbguard import TableBudget

# 规划口径,与 Go 侧同值。
PLAN_PLAYERS = 100_000
PLAN_DAU = 10_000


def budgets() -> list[TableBudget]:
    """owner_record / ds_instance_lease 按玩家 / DS 实例有界;
    owner_transition_log 是唯一只增流水(有 90 天保留期清理)。

    各表超限时的排查方向写在下面注释里 —— 光有一个 ERROR 日志说"超预算"
    而不说该往哪查,值班的人只能干瞪眼。
    """
    return [
        TableBudget(
            table="owner_record",
            max_rows=PLAN_PLAYERS * 3,
            max_avg_row_bytes=512,
            note="每玩家一行 owner 权威(§9.22);超限说明玩家量级超规划",
        ),
        TableBudget(
            table="ds_instance_lease",
            max_rows=100_000,
            max_avg_row_bytes=384,
            note="每 DS 实例一行;超限说明失效实例行未回收",
        ),
        # 保留 90 天:日活 1 万 × 每人每天 20 次归属迁移(登录/进场/换线/回大厅)× 90 × 3。
        TableBudget(
            table="owner_transition_log",
            max_rows=PLAN_DAU * 20 * 90 * 3,
            max_avg_row_bytes=768,
            note=(
                "归属迁移审计流水,保留 90 天;超限先查 owner sweep 是否在跑"
                "(日志 owner_transition_log_swept),再查是否有玩家在异常高频迁移(进场链抖动)"
            ),
        ),
    ]
