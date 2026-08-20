"""mail 服务的库容量预算(§9.24)—— 对应 Go 侧 internal/data/budgets.go。

只声明**本服务负责的表**:pandora_social 是 chat/friend/guild/mail 共用库,
各服务各管自己那几张,避免四份重复巡检把 information_schema 打成热点。

上限按**设计期望**定,不按列类型上限定 —— 写成列类型上限等于没设(等数据涨到
快撑爆才告警,业务语义早就崩了)。超预算**只告警不阻断**:容量超限是"要去查的问题",
不是"服务不能跑的理由",拒绝启动会把容量问题升级成可用性事故。

payload 是 BLOB(标题 + 正文 + 附件的集合序列化),所以 max_avg_row_bytes 是这里
最关键的信号:**行数正常但平均行长在涨 = 某个发送方绕过了逐项上限**
(尤其是系统发送方,如战斗掉落转邮件)。只看行数看不出这种。
"""

from __future__ import annotations

from pandorapy.dbguard import TableBudget

# 规划口径,与 Go 侧同值。
PLAN_PLAYERS = 100_000
PLAN_DAU = 10_000


def budgets() -> list[TableBudget]:
    """邮件相关表的容量预算,与 Go 的 data.Budgets() 逐表逐值一致。"""
    return [
        # 写扩散:日活 × 每人每天 5 封 × 过期缓冲 37 天 × 3 倍余量。
        TableBudget(
            table="player_mail",
            max_rows=PLAN_DAU * 5 * 37 * 3,
            max_avg_row_bytes=4 * 1024,
            note=(
                "写扩散收件箱;行数超限查 mail sweep 是否在跑 + MaxInboxSize 是否生效;"
                "avg_row 超 4KB 查 payload 内标题/正文/附件是否绕过逐项上限"
            ),
        ),
        TableBudget(
            table="sys_mail",
            max_rows=100_000,
            max_avg_row_bytes=4 * 1024,
            note="全服一份,运营发送驱动;行数超限说明运营发信量异常或失效邮件未清",
        ),
        TableBudget(table="guild_mail", max_rows=500_000, max_avg_row_bytes=4 * 1024),
        TableBudget(
            table="player_mail_cursor",
            max_rows=PLAN_PLAYERS * 3,
            max_avg_row_bytes=128,
            note="每玩家一行游标",
        ),
        # 领取记录保留 180 天(§9.24 登记例外:必须盖过邮件最长可领窗口)。
        TableBudget(
            table="player_mail_claim",
            max_rows=PLAN_DAU * 5 * 180 * 3,
            max_avg_row_bytes=512,
            note="领取幂等记录,保留 180 天(§9.24 登记例外);intent_payload 为 DS 领取意图 blob",
        ),
        TableBudget(
            table="player_mail_archive",
            max_rows=PLAN_DAU * 5 * 90 * 3,
            max_avg_row_bytes=4 * 1024,
            note="过期未领附件归档,保留 90 天",
        ),
    ]
