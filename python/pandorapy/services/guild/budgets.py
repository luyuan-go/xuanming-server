"""guild 服务的库容量预算(§9.24)—— 对应 Go 侧 internal/data/budgets.go。

只声明**本服务负责的表**:pandora_social 是 chat/friend/guild/mail 共用库,
各服务各管自己那几张,避免四份重复巡检把 information_schema 打成热点。

超预算**只告警不阻断**:容量超限是"要去查的问题",不是"服务不能跑的理由" ——
拒绝启动会把容量问题升级成可用性事故。
"""

from __future__ import annotations

from pandorapy.dbguard import TableBudget

# 规划口径,与 Go 侧 planPlayers 同值。
PLAN_PLAYERS = 100_000


def budgets() -> list[TableBudget]:
    """公会 / 临时群表的容量预算,与 Go 的 `data.Budgets()` 逐表逐值一致。

    ★ 每张表的上限都由 §9.18 的写入侧闸兜住,所以**超限本身就是"某道闸没生效"
      的信号**,而不只是"数据涨了"。note 写的就是往哪查。
    """
    return [
        TableBudget(
            table="guilds",
            max_rows=PLAN_PLAYERS // 10 * 3,
            max_avg_row_bytes=256,
            note="公会数 ≈ 玩家数/最小成员数;超限查建会是否被刷",
        ),
        TableBudget(
            table="guild_members",
            max_rows=PLAN_PLAYERS * 3,
            max_avg_row_bytes=128,
            note="单归属:每玩家至多一行",
        ),
        TableBudget(
            table="guild_join_requests",
            max_rows=PLAN_PLAYERS * 20 * 3,
            max_avg_row_bytes=192,
            note="终态行保留 90 天;超限先查 guild sweep 是否在跑",
        ),
        TableBudget(table="chat_groups", max_rows=PLAN_PLAYERS * 50 * 3, max_avg_row_bytes=256),
        TableBudget(
            table="chat_group_members",
            max_rows=PLAN_PLAYERS * 50 * 3,
            max_avg_row_bytes=128,
            note="受 max_group_members=50 / max_groups_per_player=50 双向约束(§9.18)",
        ),
        TableBudget(
            table="player_group_counts", max_rows=PLAN_PLAYERS * 3, max_avg_row_bytes=128
        ),
    ]
