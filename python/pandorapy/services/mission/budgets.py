"""mission 服务的库容量预算 —— 对应 Go 侧 internal/data/budgets.go(§9.24)。

口径与 Go 逐行同值。预算不是"报警阈值",是**规模假设的书面化**:
每条 Note 写的是"超了先查什么",超限时不阻断服务(容量问题不该升级成可用性事故),
只在 db_capacity_guard 里 WARN。
"""

from __future__ import annotations

from pandorapy import dbguard

# 规划玩家数(与 Go 的 planPlayers 同值)。三倍余量是全仓统一口径。
PLAN_PLAYERS = 100_000


def budgets() -> list[dbguard.TableBudget]:
    """任务域表的容量预算(pandora_mission 独库,本服务全责)。"""
    return [
        dbguard.TableBudget(
            table="player_mission_active",
            max_rows=PLAN_PLAYERS * 50 * 3,
            max_avg_row_bytes=384,
            note="每玩家 ≤ max_active_missions(50);超限查接取上限校验是否失效(§9.18)",
        ),
        dbguard.TableBudget(
            table="player_mission_done",
            max_rows=PLAN_PLAYERS * 2000 * 3,
            max_avg_row_bytes=128,
            note="被任务表行数有界(预算按 2000 行任务表);超限查任务表规模或完成行重复",
        ),
        dbguard.TableBudget(
            table="mission_reward_log",
            max_rows=PLAN_PLAYERS * 2000 * 3,
            max_avg_row_bytes=512,
            note="GRANTED 90 天清;超限先查 mission sweep 是否在跑、PENDING 是否堆积(发放链故障)",
        ),
        dbguard.TableBudget(
            table="mission_fact_receipts",
            max_rows=PLAN_PLAYERS * 500 * 3,
            max_avg_row_bytes=256,
            note="清理默认关(同 exp_history);持续增长属预期,超限评估开启收据清理的前置条件",
        ),
        dbguard.TableBudget(
            table="mission_push_outbox",
            max_rows=100_000,
            max_avg_row_bytes=1024,
            note="出箱稳态应接近空;超限 = 发布器停转或 kafka 不可用",
        ),
        dbguard.TableBudget(
            table="mission_player_guards",
            max_rows=PLAN_PLAYERS * 3,
            max_avg_row_bytes=64,
            note="每玩家至多 1 行的写守卫(TiDB 无 gap 锁);超限只能是玩家规模超预算",
        ),
    ]


def big_fields() -> list[tuple[str, str, int]]:
    """大字段体检登记(dbcheck -size-check 用;写入侧闸在 repo 的三处 check_payload)。

    返回 (table, column, max_bytes) —— 此处只是审计口径,真正拒写在 repo。
    """
    return [
        ("player_mission_active", "progress", 256),
        ("mission_reward_log", "reward_pb", 2048),
        ("mission_push_outbox", "payload", 2048),
    ]
