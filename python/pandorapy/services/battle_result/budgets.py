"""pandora_battle 库容量预算(§9.24)—— 对应 Go 侧 internal/data/budgets.go。

数值与 Go **逐个相同**。分叉的后果不是"告警早一点晚一点":两栈副本同时在跑,
一边告警一边不告警,值班的人会按"另一台没报"判定成误报。

容量规划基数:日活 1 万,每人每天 10 局,每局 10 人 → 每天约 1 万局。
保留期直接引 conf.HISTORY_RETENTION_MAX_DAYS,不另抄一份常量:
预算 = 稳态最大行数 × 3 倍余量,保留期一改预算必须同步,否则告警要么恒响要么形同虚设。

出箱表给的是**积压告警线**而非容量上限:它们投递成功即删,稳态应接近空,
堆积 = 投递链堵塞(kafka / 下游服务不可用),属告警问题不是增长问题。
"""

from __future__ import annotations

from pandorapy import dbguard
from pandorapy.services.battle_result import conf as bconf

PLAN_DAILY_MATCHES = 10_000
# 单局人数(stats / progress_player 的行放大系数)。
PLAN_PER_MATCH = 10
PLAN_RETENTION_DAYS = bconf.HISTORY_RETENTION_MAX_DAYS

# battle_result 的权威库。
BATTLE_DB = "pandora_battle"


def budgets() -> list[dbguard.TableBudget]:
    """pandora_battle 库的容量预算(与 Go 的 Budgets() 逐条同值同序)。"""
    return [
        dbguard.TableBudget(
            table="battles",
            max_rows=PLAN_DAILY_MATCHES * PLAN_RETENTION_DAYS * 3,
            max_avg_row_bytes=256,
            note=(
                "对局结算头,保留 180 天;超限先查 battle 保留期清理是否在跑"
                "(日志 battle_retention_battles_purged)"
            ),
        ),
        dbguard.TableBudget(
            table="battle_player_stats",
            max_rows=PLAN_DAILY_MATCHES * PLAN_PER_MATCH * PLAN_RETENTION_DAYS * 3,
            max_avg_row_bytes=192,
            note="随 battles 同事务批删;行数 ≈ 对局数 × 单局人数",
        ),
        dbguard.TableBudget(
            table="battle_progress_stream",
            max_rows=PLAN_DAILY_MATCHES * PLAN_RETENTION_DAYS * 3,
            max_avg_row_bytes=128,
            note=(
                "已结算行保留 180 天;未结算陈年行永不清但有 ERROR 告警"
                "(battle_retention_stale_unsettled_progress)"
            ),
        ),
        dbguard.TableBudget(
            table="battle_progress_player",
            max_rows=PLAN_DAILY_MATCHES * PLAN_PER_MATCH * PLAN_RETENTION_DAYS * 3,
            max_avg_row_bytes=128,
        ),
        dbguard.TableBudget(
            table="battle_progress_item_balance",
            max_rows=PLAN_DAILY_MATCHES * PLAN_PER_MATCH * PLAN_RETENTION_DAYS * 3 * 8,
            max_avg_row_bytes=96,
            note="phase0 每场玩家同 item pickup/支出余额;随 settled progress 成组清理",
        ),
        dbguard.TableBudget(
            table="battle_progress_action",
            max_rows=PLAN_DAILY_MATCHES * PLAN_PER_MATCH * PLAN_RETENTION_DAYS * 3 * 8,
            max_avg_row_bytes=128,
            note="consume/discard durable outcome;随 settled progress 成组清理,响应丢失回放依据",
        ),
        # ── 出箱表:积压告警线 ──
        dbguard.TableBudget(
            table="player_update_outbox",
            max_rows=200_000,
            max_avg_row_bytes=768,
            note="投递成功即删;堆积 = kafka 投递链堵塞,查 RunOutboxPublisher 日志",
        ),
        dbguard.TableBudget(
            table="battle_drop_outbox",
            max_rows=200_000,
            max_avg_row_bytes=1536,
            note="冻结完整/stack/instance 三份 CSV;堆积 = inventory GrantItems/GrantInstances 长期失败",
        ),
        dbguard.TableBudget(
            table="battle_progress_outbox",
            max_rows=500_000,
            max_avg_row_bytes=768,
            note="实时进度出箱,写入最密集;堆积 = player/inventory 发放链堵塞",
        ),
        dbguard.TableBudget(
            table="match_release_outbox",
            max_rows=200_000,
            max_avg_row_bytes=1024,
            note="payload 含 repeated player_ids(VARBINARY(1024));队伍规模变大会逼近列上限",
        ),
        dbguard.TableBudget(
            table="terminal_release_outbox", max_rows=200_000, max_avg_row_bytes=1024
        ),
        dbguard.TableBudget(
            table="battle_exit_proof_outbox", max_rows=200_000, max_avg_row_bytes=2048
        ),
    ]


# ── 列级字节预算(Go 的 BigFields)─────────────────────────────────────────
#
# ⚠️ 与 `budgets()` 的调用时机**完全不同**:表级走 information_schema(毫秒级、
# 不锁表)可以挂周期 ticker;列级是 `MAX(LENGTH(col))` **全表扫描**,只在
# ①表级 avg_row_bytes 告警后人工定位、②天级低频巡检时跑。
# 别把它接到 sweep ticker 上 —— 那会把生产库扫死。
def big_fields() -> list[dbguard.ColumnBudget]:
    """列级字节预算 —— 与 Go `data.BigFields()` 逐字段一致。

    两条都是 75% 预警线:等撞到列类型上限才发现,写入已经在失败了(严格模式下
    Error 1406),而这条巡检的意义正是**在写失败之前**给出排查窗口。
    """
    return [
        dbguard.ColumnBudget(
            table="match_release_outbox",
            column="payload",
            max_bytes=768,
            note=(
                "列是 VARBINARY(1024);768=75% 预警线。超限说明 player_ids 随队伍规模胀大,"
                "再涨会让整场结算的释放出箱写失败"
            ),
        ),
        dbguard.ColumnBudget(
            table="battle_exit_proof_outbox",
            column="payload",
            max_bytes=1536,
            note="列是 VARBINARY(2048);1536=75% 预警线",
        ),
    ]
