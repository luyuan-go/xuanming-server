"""friend 服务的库容量预算 —— 对应 Go 侧 internal/data/budgets.go(§9.24)。

只声明本服务负责的表(pandora_social 是共用库,chat 的表不在这里)。
★ 数值必须与 Go 侧逐个相同:两个实现共用同一套 Grafana 面板与 Loki 告警,
  预算不同会让"同一张表在 Go 副本上告警、在 Python 副本上不告警"。
"""

from __future__ import annotations

from pandorapy.dbguard import TableBudget

# 规划玩家量级(Go 的 planPlayers)。三张受 §9.18 写入侧上限保护的表,
# 行数上限 = 玩家数 × 每玩家上限 × 3;超限说明上限校验失效或玩家量级超规划。
PLAN_PLAYERS = 100_000


def budgets() -> list[TableBudget]:
    """好友域表的容量预算。对应 Go 的 data.Budgets()。"""
    return [
        TableBudget(
            table="friendships",
            max_rows=PLAN_PLAYERS * 200 * 3,
            max_avg_row_bytes=128,
            note="双向各一行;超限查 max_friends 上限校验是否失效(§9.18)",
        ),
        TableBudget(
            table="friend_requests",
            max_rows=PLAN_PLAYERS * 200 * 3,
            max_avg_row_bytes=192,
            note="终态行保留 90 天;超限先查 friend sweep 是否在跑",
        ),
        TableBudget(
            table="blocks",
            max_rows=PLAN_PLAYERS * 200 * 3,
            max_avg_row_bytes=128,
        ),
        TableBudget(
            table="friend_player_guards",
            max_rows=PLAN_PLAYERS * 3,
            max_avg_row_bytes=128,
            note="每玩家一行守卫",
        ),
        TableBudget(
            table="friend_pair_guards",
            max_rows=PLAN_PLAYERS * 200 * 3,
            max_avg_row_bytes=128,
            note="关系对守卫随社交图 O(n²) 累积,有保留期清理;超限查 pair guard sweep",
        ),
    ]
