"""login 服务的库容量预算(CLAUDE.md §9.24)—— 对应 Go 的
`services/account/login/internal/data/budgets.go`。

预算值逐条与 Go 同:抄一份不同的数字会让同一张表在 Go 副本上告警、Python 副本上
安静(或反过来),而两边都"没报错",容量问题因此永远查不到源头。
"""

from __future__ import annotations

from pandorapy import dbguard

# Go: const planPlayers = 100_000
PLAN_PLAYERS = 100_000


def budgets() -> list[dbguard.TableBudget]:
    """pandora_account 库的容量预算。"""
    return [
        dbguard.TableBudget(
            table="accounts",
            max_rows=PLAN_PLAYERS * 3,
            max_avg_row_bytes=256,
            note="每账号一行;超限说明注册量超容量规划(或被刷注册)",
        ),
        dbguard.TableBudget(
            table="player_roles", max_rows=PLAN_PLAYERS * 3, max_avg_row_bytes=128
        ),
        dbguard.TableBudget(
            table="player_session_generations",
            max_rows=PLAN_PLAYERS * 3,
            max_avg_row_bytes=128,
            note="每玩家一行会话代际",
        ),
        dbguard.TableBudget(
            # device_id 由客户端上报、单账号可堆多设备;保留 90 天兜底。
            table="account_devices",
            max_rows=PLAN_PLAYERS * 10 * 3,
            max_avg_row_bytes=256,
            note=(
                "行数 ≈ 玩家数 × 设备数;超限查是否有人刷任意 device_id"
                "(保留期清理已落地,日志 stale_devices_purged)"
            ),
        ),
        dbguard.TableBudget(
            table="account_bans",
            max_rows=1_000_000,
            max_avg_row_bytes=512,
            note="§9.24 登记豁免(运营合规审计,不清理);超限说明封禁量异常,需人工评估归档",
        ),
        dbguard.TableBudget(
            table="player_no_counter",
            max_rows=8,
            # max_avg_row_bytes 刻意留 0(= 不检查):单行表的 avg_row_length 由
            # information_schema 按 data_length/rows 估算,InnoDB 最小分配一个 16KB 页
            # → 恒报 16384,与真实行长(≈9 字节)无关。设任何"按 schema 推算"的值
            # 都必然误报(Go 侧实测 2026-08-10:budget=64 每轮巡检刷一条 ERROR)。
            note=(
                "角色编号全局发号计数器,恒 1 行(§9.24 登记豁免:发号权威闸,不清理);"
                "行长不设限=单行表 avg 估算恒为页大小"
            ),
        ),
    ]
