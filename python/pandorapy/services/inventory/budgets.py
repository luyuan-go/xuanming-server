"""inventory 的库容量预算 —— 对应 Go 侧 internal/data/budgets.go(CLAUDE.md §9.24)。

启动即跑一轮拿基线,之后周期巡检:超限打 ERROR 日志 + metric,**不阻止启动**
(容量超限是"要去查的问题",不是"服务不能跑的理由";拒绝启动会把容量问题
升级成可用性事故)。

预算怎么定(必须能说明推导依据,不准拍脑袋):
  - 有保留期的只增表:峰值写入速率 × 保留期 × 3 倍安全系数。超了 = 清理没追上写入,
    或写入速率超出容量规划 —— 两种都要人查。
  - 按玩家有界的表:预期玩家数 × 每玩家行数 × 3。
  - max_avg_row_bytes:定长列之和 + blob 的**设计期望大小**(不是列类型上限)。
    写成列类型上限等于没设 —— 那样只有 MySQL 快报错时才告警,失去预警意义。

★ 数值必须与 Go 逐个相同。分叉的后果很隐蔽:同一个库在 Go 版上告警、Python 版不告警
  (或反过来),而两边都"正常运行"。
"""

from __future__ import annotations

from pandorapy import dbguard

# 容量规划基数(docs/design/stress-discipline.md 口径:大厅 500 人/实例)。
# 预算按 10 万注册玩家 / 1 万日活推算,留 3 倍安全系数;真实规模变化时改这里一处。
PLAN_PLAYERS = 100_000
PLAN_DAU = 10_000

# 玩家实例背包容量(inventory.capacity 的取值依据也是它,见 conf.py)。
_PLAN_BAG_CAPACITY = 200


def trade_budgets() -> list[dbguard.TableBudget]:
    """pandora_trade 库(货币 / 道具 / 流水 / 托管)的容量预算。"""
    return [
        dbguard.TableBudget(
            # 多币种钱包:一玩家一币种一行(CurrencyKind 当前 3 种),故按 PLAN_PLAYERS×3 计,
            # 再乘既有的 3 倍余量口径(与 player_items 的 PLAN_PLAYERS*200*3 同形)。
            table="player_wallet",
            max_rows=PLAN_PLAYERS * 3 * 3,
            max_avg_row_bytes=128,
            note="每玩家每币种一行;超限说明玩家量级超出容量规划或币种枚举失控",
        ),
        dbguard.TableBudget(
            table="player_items",
            max_rows=PLAN_PLAYERS * _PLAN_BAG_CAPACITY * 3,
            max_avg_row_bytes=128,
            note="行数 ≈ 玩家数 × 持有过的道具配置种类;超限查是否有人刷任意 item_config_id",
        ),
        dbguard.TableBudget(
            table="player_item_instance",
            max_rows=PLAN_PLAYERS * _PLAN_BAG_CAPACITY * 3,
            max_avg_row_bytes=512,
            note="行数 ≈ 玩家数 × 背包容量;avg_row 超限查 attributes(pb)词条数",
        ),
        dbguard.TableBudget(
            # 保留期 90 天;按日活 1 万 × 每人每天 20 笔 × 90 天 × 3 ≈ 5400 万。
            table="inventory_ledger",
            max_rows=PLAN_DAU * 20 * 90 * 3,
            max_avg_row_bytes=512,
            note="幂等流水,保留期 90 天;超限先查 sweep 是否在跑(日志 inventory_sweep_ledger)",
        ),
        dbguard.TableBudget(
            table="auction_escrow",
            max_rows=PLAN_PLAYERS * _PLAN_BAG_CAPACITY * 3,
            max_avg_row_bytes=256,
            note="挂单托管;active 行不清理,超限查是否有大量挂单未终结(escrow 泄漏)",
        ),
    ]


def bag_budgets() -> list[dbguard.TableBudget]:
    """pandora_bag 库(背包域)的容量预算。对应 Go 的 data.BagBudgets()。

    这里的 max_avg_row_bytes 尤其重要:三个 blob 列都是"集合序列化",是典型的
    **深度失控**风险点(行数正常、单行越来越胖)。avg_row_bytes 突增是最早的信号。
    """
    return [
        dbguard.TableBudget(
            table="bag_meta",
            max_rows=PLAN_PLAYERS * 3,
            max_avg_row_bytes=8 * 1024,
            note="snapshot 是随身组快照(MEDIUMBLOB);avg_row 超 8KB 查格子数与单格 attrs 条数",
        ),
        dbguard.TableBudget(
            table="bag_checkpoint",
            max_rows=PLAN_PLAYERS * 3,
            max_avg_row_bytes=1024,
        ),
        dbguard.TableBudget(
            table="bag_section",
            max_rows=PLAN_PLAYERS * 8 * 3,
            max_avg_row_bytes=16 * 1024,
            note="section 是单段全部格子(MEDIUMBLOB,仓库默认 200 格);"
            "avg_row 超 16KB 说明容量闸被绕过或单格 attrs 无界",
        ),
        dbguard.TableBudget(
            # 保留期 90 天;按日活 1 万 × 每人每天 50 条 × 90 天 × 3 ≈ 1.35 亿
            # (journal 写入最密集)。
            table="bag_journal",
            max_rows=PLAN_DAU * 50 * 90 * 3,
            max_avg_row_bytes=2 * 1024,
            note="单条 journal payload(BLOB);avg_row 超 2KB 查单条 op 的 item 数"
            "与单个 item 的 attrs 数",
        ),
        dbguard.TableBudget(
            table="bag_generation",
            max_rows=10_000,
            max_avg_row_bytes=256,
            note="每活动段一行,运营配置驱动;超限查是否有活动段只增不回收",
        ),
    ]
