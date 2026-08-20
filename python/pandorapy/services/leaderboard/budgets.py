"""pandora_leaderboard 库容量预算(§9.24)—— 对应 Go 侧 internal/data/budgets.go。

数值与 Go **逐个相同**。分叉的后果不是"告警早一点晚一点":两栈副本同时在跑,
一边告警一边不告警,值班的人会按"另一台没报"判定成误报。

结算批次驱动:每天若干榜 × 若干周期,量级远小于业务流水表;
snapshot / reward_log 行数 = 批次数 × Top-N,是主要增长源。
"""

from __future__ import annotations

from pandorapy import dbguard


def budgets() -> list[dbguard.TableBudget]:
    """三张结算归档表的行数 / 平均行长预算。"""
    return [
        dbguard.TableBudget(
            # settlement 故意不清理(settle uk 是防重复结算的永久闸,每批次 1 行慢增长豁免),
            # 所以这里给一个"慢增长仍需可见"的告警线:每天 100 批 × 3 年。
            table="leaderboard_settlement",
            max_rows=100 * 365 * 3,
            max_avg_row_bytes=256,
            note="§9.24 登记豁免(永久闸,不清理);超限说明结算批次量远超预期,需人工评估是否要归档",
        ),
        dbguard.TableBudget(
            # 保留 90 天:每天 100 批 × Top-100 × 90 × 3。
            table="leaderboard_snapshot",
            max_rows=100 * 100 * 90 * 3,
            max_avg_row_bytes=128,
            note="名次快照,保留 90 天;超限先查 leaderboard 保留期清理是否在跑",
        ),
        dbguard.TableBudget(
            table="leaderboard_reward_log",
            max_rows=100 * 100 * 90 * 3,
            max_avg_row_bytes=1024,
            note=(
                "GRANTED 行保留 90 天,PENDING/FAILED 永不清(补发工作集);"
                "avg_row 超 1KB 查 reward_pb 的 items 条数(列 VARBINARY(2048),无条数上限)"
            ),
        ),
    ]


# reward_pb 列的**写入侧**字节上限(§9.24 三个上限里的第 ③ 条)。
#
# 列是 VARBINARY(2048),这里取 75% 预警线:单条 RewardItem 编码 ≤ 16 字节,
# 1536 字节已能装下上百件奖励 —— 达到即 RewardTier.items 条数失控。
#
# ★ 这是**写入侧硬阀**而不是巡检指标:超了就拒写。放行的后果是要么被 MySQL 报错,
# 要么在非严格 sql_mode 下被静默截断 —— 截断后的 reward_pb 解不出来,那条奖励
# 会被补扫永久标 FAILED,玩家的奖就卡在那里。
REWARD_PAYLOAD_MAX_BYTES = 1536
