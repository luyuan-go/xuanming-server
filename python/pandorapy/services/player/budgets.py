"""player 的库容量预算 —— 对应 Go 侧 internal/data/budgets.go(§9.24)。

启动即跑一轮拿基线,之后每小时一轮:超限打 ERROR 日志 + metric,**不阻止启动**
(容量超限是"要去查的问题",不是"服务不能跑的理由";拒启会把容量问题升级成可用性事故)。

★ 数值必须与 Go 逐个相同。分叉的后果很隐蔽:同一个库在 Go 副本上告警、Python 副本不
  告警(或反过来),而两边都"正常运行" —— 值班的人会以为告警是抖动。
"""

from __future__ import annotations

from pandorapy import dbguard

# 容量规划基数(与 Go 的 planPlayers / planDAU 同值)。
PLAN_PLAYERS = 100_000
PLAN_DAU = 10_000


def budgets() -> list[dbguard.TableBudget]:
    """pandora_player 库的容量预算。"""
    return [
        dbguard.TableBudget(table="players", max_rows=PLAN_PLAYERS * 3, max_avg_row_bytes=512),
        dbguard.TableBudget(
            # 段位分唯一权威:每玩家每池 1 行。池数 = 关卡表「段位池」列的取值数,按 16 档
            # 玩法留余量(当前 2 档)。超限说明池名在失控增长(例如把池名当成 map 维度填)。
            table="player_mmr",
            max_rows=PLAN_PLAYERS * 16 * 3,
            max_avg_row_bytes=128,
            note="分池段位分;超限先查关卡表「段位池」列是否被当成每图一池填",
        ),
        dbguard.TableBudget(
            table="player_heroes", max_rows=PLAN_PLAYERS * 100 * 3, max_avg_row_bytes=128
        ),
        dbguard.TableBudget(
            table="player_attributes", max_rows=PLAN_PLAYERS * 16 * 3, max_avg_row_bytes=128
        ),
        dbguard.TableBudget(
            table="player_equipment", max_rows=PLAN_PLAYERS * 16 * 3, max_avg_row_bytes=128
        ),
        dbguard.TableBudget(
            table="player_talents", max_rows=PLAN_PLAYERS * 200 * 3, max_avg_row_bytes=128
        ),
        dbguard.TableBudget(
            # 每场对局每人一行,保留期 90 天(清理默认关):日活 1 万 × 每天 10 局 × 90 × 3。
            table="mmr_history",
            max_rows=PLAN_DAU * 10 * 90 * 3,
            max_avg_row_bytes=256,
            note="MMR 幂等历史;清理默认关(history_cleanup_enabled),超限先确认是否该开清理",
        ),
        dbguard.TableBudget(
            table="exp_history",
            max_rows=PLAN_DAU * 100 * 7 * 3,
            max_avg_row_bytes=256,
            note="经验幂等收据,保留期 7 天;超限查 exp_history_cleanup_enabled 与上游 progress 出箱重试是否有界",
        ),
        dbguard.TableBudget(
            table="attr_point_grants", max_rows=PLAN_PLAYERS * 200 * 3, max_avg_row_bytes=192
        ),
        dbguard.TableBudget(
            table="talent_point_grants", max_rows=PLAN_PLAYERS * 200 * 3, max_avg_row_bytes=192
        ),
        dbguard.TableBudget(
            # 发卡幂等收据。idempotency_key 是 VARCHAR(128)(比另外两张的 64 宽一倍,
            # 抽卡侧的键通常带批次 + 卡池 + 序号),故 avg_row 预算相应放宽。
            table="skill_card_grants",
            max_rows=PLAN_PLAYERS * 200 * 3,
            max_avg_row_bytes=256,
            note="抽卡/活动发卡幂等收据;超限查 history_cleanup_enabled 与抽卡侧幂等键是否过长",
        ),
        dbguard.TableBudget(
            table="player_skill_cards",
            max_rows=PLAN_PLAYERS * 200,
            max_avg_row_bytes=128,
            note="行数上界 = 玩家数 × 技能卡表行数;远超说明发了配置表里没有的卡",
        ),
        dbguard.TableBudget(
            table="player_skill_slots",
            max_rows=PLAN_PLAYERS * 8,
            max_avg_row_bytes=128,
            note="行数上界 = 玩家数 × 卡槽数(4);超限说明卡槽越界校验被绕过",
        ),
        dbguard.TableBudget(
            # 出箱表:投递成功即删,稳态应接近空。这里给的是"积压告警线"而非容量上限。
            table="player_push_outbox",
            max_rows=100_000,
            max_avg_row_bytes=1024,
            note="推送出箱应即时排空;行数堆积 = kafka 投递链堵塞,查 push_outbox_publisher 日志",
        ),
        dbguard.TableBudget(
            # 每玩家一行,但**单行**是 LONGBLOB(4GB):这里 avg_row 才是关键信号。
            # 设计期望:永久来源 ≤64 条 + 活动实例 ≤256 条,单条位图按真实档位数通常 <1KB。
            table="player_reward_claims",
            max_rows=PLAN_PLAYERS * 3,
            max_avg_row_bytes=8 * 1024,
            note=(
                "record 是 LONGBLOB(DB 层不设防);avg_row 超 8KB 说明位图条目数异常膨胀——"
                "查是否有客户端刷任意 source/activity_instance_id(rewardclaim 已加条目数上限),"
                "或活动未 erase_activity 回收"
            ),
        ),
    ]
