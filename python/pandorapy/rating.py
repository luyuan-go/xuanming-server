"""段位池(rating_pool)分区键规范 —— 对应 Go 侧 pkg/rating/pool.go。

存在理由(2026-08-11):产品口径「3v3 与 5v5 不共用同一份段位」要求段位按池分区存储。
分区键取值来自关卡表「段位池」列,经 matchmaker 成局定格 → ds_allocator → canonical
BattleStorageRecord → battle_result 结算 → player 分区入账,横跨四个服务。

★ 为什么必须与 Go 逐字一致:默认池名与归一化规则若两栈各写一份,漂移的后果是
**同一个玩家在两个实现眼里属于不同段位池**(Go 副本入账 "default"、Python 副本查 "")
—— 表现为「分怎么打都不涨」,而两边都不报错。

⚠️ 刻意**不维护合法池名白名单**:池名是策划在关卡表里自由填的标识符,语义只有
「同值即同一份段位」。加白名单等于每开一档玩法都要改代码发版(与 §17.1「差异进表」相悖)。
"""

from __future__ import annotations

# DEFAULT_POOL 是 rating_pool 为空时归一化到的池名。
#
# 取 "default" 而不是空串:空串在 SQL 主键、Redis key、日志里都难与"缺字段"区分,
# 排查时无法回答"这一分到底记到哪儿了"。给它一个能被 SELECT 出来的名字。
DEFAULT_POOL = "default"

# MAX_POOL_LEN 与 `player_mmr.rating_pool` 列宽 VARCHAR(32) 同源。
# 超长写入在非严格 sql_mode 下会**静默截断**(§9.24),截断后的池名与原值不同
# = 玩家的分被记进了另一份段位且无任何报错。
MAX_POOL_LEN = 32


def normalize(pool: str | None) -> str:
    """归一成存储/查询用的规范形式:去首尾空白,空则取 DEFAULT_POOL。

    ★ 只做这两件事 —— **不做大小写折叠**。池名是策划填的标识符,"PVP" 与 "pvp"
    若被折叠成同一份段位,等于替策划决定了两张图共用一份分;不折叠则它们各是一份,
    漏配一眼看得出来。

    ★ 归一化必须在**写入与读取两侧都调用**,否则会出现写 "default" / 读 "" 的分裂。
    """
    trimmed = (pool or "").strip()
    return trimmed if trimmed else DEFAULT_POOL
