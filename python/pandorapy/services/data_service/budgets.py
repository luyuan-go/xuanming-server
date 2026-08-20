"""pandora_player 库的容量预算(§9.24)—— 对应 Go 侧 internal/data/budgets.go。

⚠️ player_data 的 schema 由 PlayerData proto **自动建表**:string 字段生成 MEDIUMTEXT(16MB)、
bytes/message 生成 MEDIUMBLOB,即 **DB 层几乎不设防**。写入侧目前也没有长度校验
(只校验 update_mask),所以 max_avg_row_bytes 是这里唯一的自动告警手段:
avg_row 突增 = 某个字符串/blob 字段在无界增长,顺着 WritePlayer 的调用方查。

超预算**只告警不阻断**:容量超限是"要去查的问题",不是"服务不能跑的理由" ——
拒绝启动会把一个容量问题升级成可用性事故。
"""

from __future__ import annotations

from pandorapy.dbguard import TableBudget

# 规划口径,与 Go 侧 planPlayers 同值。
PLAN_PLAYERS = 100_000


def budgets() -> list[TableBudget]:
    """player_data 表的容量预算。数值与 Go 侧 Budgets() 逐个相同。

    两边必须同值的理由:同一套 Grafana 面板会同时看到 Go 副本和 Python 副本打的
    db_capacity_budget_exceeded,阈值不同会让"到底超没超"这个问题按副本随机作答。
    """
    return [
        TableBudget(
            table="player_data",
            max_rows=PLAN_PLAYERS * 3,
            max_avg_row_bytes=4 * 1024,
            note=(
                "proto2mysql 自动建表(string→MEDIUMTEXT 16MB,DB 层不设防);"
                "avg_row 超 4KB 说明某字段无界增长,用 dbcheck -size-check 定位到列再查 "
                "WritePlayer 调用方"
            ),
        ),
    ]
