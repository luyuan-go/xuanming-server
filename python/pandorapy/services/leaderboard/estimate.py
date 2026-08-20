"""排行榜名次估算 —— 对应 Go 侧 internal/data/board_store.go 的直方图部分。

设计:**头部精确 + 尾部估算**。

    精确榜(ZSET)只保留 max_size 名,超出的被截断出榜。
    但被截断的玩家仍然想知道"我大概第几" —— 全量精确排名在千万级玩家下不可行,
    所以用**分数直方图**做区间估算:

        估算名次 = 比我优的桶计数和 + 本桶的一半(桶内取中位)

    直方图是**可重建的派生状态**(不变量 §22),纯只读、无锁,不参与任何权威写。

★ 两条不能丢的约束:

1. **榜外估算名次不得落进精确区**
   直方图是约值,精确榜是精确值,两者打架时以精确榜为准。
   所以估算结果必须钳到 `ZCARD + 1` 之后 —— 否则会出现
   "榜外玩家显示第 5 名,而精确榜第 5 名是另一个人"。

2. **桶索引必须钳制**
   异常分数(负的极大值 / 溢出)会把 HASH 的 field 数撑到失控。
   正常配置远达不到 ±2^20。
"""

from __future__ import annotations

import dataclasses

# 桶索引绝对值上限。防异常分数把 field 数撑爆;正常配置远达不到。
MAX_BUCKET_IDX = 1 << 20

# 估算直方图桶宽的兜底默认值(MMR 量纲)。
DEFAULT_ESTIMATE_BUCKET_WIDTH = 25


def bucket_of(score: int, width: int) -> int:
    """分数 → 所属直方图桶。**floor 除法,负分正确**;索引钳制 ±MAX_BUCKET_IDX。

    ★ 跨语言注意:Go 的 `/` 对整数是**向零截断**,所以那边写成

        q := score / width
        if score%width != 0 && (score < 0) != (width < 0) { q-- }

    手工补了"负数向下取整"这一步。Python 的 `//` **本身就是 floor**,
    所以直接用 `//` 与 Go 的结果一致 —— 但这不是显然的,
    有 tests/test_leaderboard_estimate.py 的跨语言对拍兜着。

    写错的后果:负分玩家(MMR 可能为负)被分到错误的桶,
    估算名次系统性偏移,而且**不报错**。
    """
    if width == 0:
        width = DEFAULT_ESTIMATE_BUCKET_WIDTH  # 防除零(biz 层已兜底,这里再挡一道)
    q = score // width
    return max(-MAX_BUCKET_IDX, min(MAX_BUCKET_IDX, q))


@dataclasses.dataclass(slots=True)
class EstimateResult:
    entity_id: int
    score: int
    rank: int
    total_submitters: int


def estimate_rank(
    *,
    entity_id: int,
    score: int,
    bucket_width: int,
    histogram: dict[int, int],
    on_board_count: int,
    ascending: bool,
) -> EstimateResult:
    """用分数直方图估算未进精确榜 entity 的名次(约值)。

    ascending=True  → 分数**低**的排前面(如竞速用时)
    ascending=False → 分数**高**的排前面(如 MMR,默认)

    histogram: {桶索引: 计数}。脏 field(负计数 / 非法索引)由调用方过滤。
    """
    my_bucket = bucket_of(score, bucket_width)
    better = 0
    own = 0
    total = 0

    for idx, cnt in histogram.items():
        if cnt <= 0:
            continue  # 脏桶跳过,不影响其余桶
        total += cnt
        if idx == my_bucket:
            own = cnt
            continue
        if (ascending and idx < my_bucket) or (not ascending and idx > my_bucket):
            better += cnt

    # 桶内取中位:本桶 own 个人里,假设我在正中间。
    # (own+1)//2 而不是 own//2 —— own=1(只有我自己)时应当是 1 不是 0。
    est = better + (own + 1) // 2

    # ★ 榜外估算名次**不得落进精确区**。
    # 直方图是约值,精确榜是精确值;不钳的话会出现"榜外玩家显示第 5 名,
    # 而精确榜第 5 名是另一个人"这种自相矛盾的展示。
    est = max(est, on_board_count + 1)

    return EstimateResult(
        entity_id=entity_id, score=score, rank=est, total_submitters=total
    )
