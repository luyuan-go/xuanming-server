"""chat 服务的库容量预算 —— 对应 Go 侧 internal/data/budgets.go(§9.24)。

只声明本服务负责的表(pandora_social 是 chat / friend / guild / mail 共用库,
各服务只报自己那几张,否则同一张表会被多个服务重复告警)。

数字**不在这里现算**:逐字照抄 Go 源码的表达式,让两栈的告警阈值只有一个真值来源。
"""

from __future__ import annotations

from pandorapy import dbguard


def budgets() -> list[dbguard.TableBudget]:
    """私聊历史表的容量预算。

    口径与 Go 逐字相同:日活 1 万 × 每人每天 50 条 × 保留 90 天 × 3。
    """
    return [
        dbguard.TableBudget(
            table="chat_private_messages",
            max_rows=10_000 * 50 * 90 * 3,
            max_avg_row_bytes=768,
            note=(
                "私聊历史,保留 90 天;行数超限查 chat sweep 是否在跑(日志 chat_history_swept)"
                "与发言速率;avg_row 超限查 MaxContentLen 是否生效(content 列 VARCHAR(512))"
            ),
        )
    ]
