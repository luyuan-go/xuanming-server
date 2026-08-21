"""region_affinity.py —— 两级撮合(region 内优先 + 跨 region 溢出)的核心算法,纯函数。

逐条对应 Go 的 `services/matchmaking/matchmaker/internal/biz/region_affinity.go`:

  - 等待超 T_overflow(段位越高越短)且本 region 同段位不足 → 允许跨 region 溢出;
  - 跨 region 候选评分加 RTT 亲和度惩罚(同 region 0 惩罚,永远优先同 region);
  - 一局内跨 region 玩家比例软上限(防一局横跨三区体验崩坏);

本模块只实现"算法",不接 etcd / 跨 region Kafka / 溢出池存储 —— 与 Go 同样的边界。

★ Go 侧 region_affinity.go 头部有一句「这些纯函数当前不接入 matchOnce 主循环」,
  那是**陈旧注释**:`match.go` 的 `formMatchesInPool`(约 3681 行)早已调用
  `partitionTicketsByRegion` / `leftoverRegionBucketTotals` / `selectOverflowTickets` /
  `withinCrossRegionCap`。移植时以**代码**为准,不以注释为准。

★ 确定性:所有"并列取谁"的地方都显式排序后取最小者。撮合次序不确定会让两栈
  并排跑同一批票据成出不同的局,parity 探针直接失效。
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Iterable, Sequence

from pandora.match.v1 import match_pb2 as matchpb

__all__ = [
    "CellLocation",
    "RegionMatchPolicy",
    "default_region_match_policy",
    "majority_region",
    "majority_cell_location",
    "partition_tickets_by_region",
    "leftover_region_bucket_totals",
    "select_overflow_tickets",
]

#: 一张票据 → 其 owner region 的解析器(实现用 `cellroute.Router.route(captain_id)`)。
#: 返回 0 表示"未知 / 单 Cell"(router 未配或解析失败),所有票据落同一桶 → 退化为不分区。
RegionResolver = Callable[[matchpb.MatchTicketStorageRecord], int]


@dataclasses.dataclass(frozen=True, slots=True)
class RegionMatchPolicy:
    """跨 region 撮合策略(决策文档 §2.2 的可调参数)。

    自包含,不进 `conf.MatchConf` —— 与 Go 同样:跨 region 溢出是阶段 3 才接的路径,
    先以策略结构 + 默认值形式落地算法,main 在阶段 3 装配时再从配置填充。
    """

    #: 跨 region 候选每 1ms 估计 RTT 的评分惩罚(w_rtt)。
    #: 同 region RTT 视为 0 → 0 惩罚,永远优先同 region。值越大越抗拒跨 region。
    rtt_penalty_per_ms: float

    #: 一局内跨 region(非多数 region)玩家比例软上限(百分比,0~100)。
    cross_region_ratio_cap_pct: int

    #: 最低段位的溢出等待阈值(ms)。段位越高,实际阈值越短(见 overflow_threshold_ms)。
    overflow_base_ms: int

    #: 每升一个段位档,溢出阈值缩短的 ms。高分段人稀,早点跨 region。
    overflow_shorten_per_tier_ms: int

    #: 溢出阈值下限(ms),再高段位也不低于此,避免过早跨 region。
    overflow_min_ms: int

    #: 段位桶宽度(分):溢出池按 mmr/width 分桶,避免单一全局大池热点(§2.3)。
    mmr_bucket_width: int

    #: 段位档 0(普通段)的 MMR 上界:≤ 此值算 tier 0。
    tier_base_mmr: int

    #: 每升一个段位档所需的 MMR 增量(高于 tier_base_mmr 后)。
    tier_step_mmr: int

    def mmr_bucket(self, mmr: int) -> int:
        """某 MMR 对应的段位桶编号(溢出池 key=mmr_bucket,§2.3)。

        负 MMR 归桶 0;`mmr_bucket_width <= 0` 时退化为单桶 0(配置缺省保护)。
        """
        if mmr < 0:
            mmr = 0
        if self.mmr_bucket_width <= 0:
            return 0
        # Go 是 int32 整除且被除数已非负,Python 的 // 在两个非负数上与之同结果。
        return mmr // self.mmr_bucket_width

    def mmr_tier(self, mmr: int) -> int:
        """某 MMR 的段位档(0=普通段,越大段位越高 → 溢出阈值越短)。

        `tier = max(0, (mmr - tier_base_mmr) / tier_step_mmr)`;
        `tier_step_mmr <= 0` 时恒 0(单档保护)。
        """
        if mmr <= self.tier_base_mmr or self.tier_step_mmr <= 0:
            return 0
        return (mmr - self.tier_base_mmr) // self.tier_step_mmr

    def overflow_threshold_ms(self, tier: int) -> int:
        """某段位档 tier 的溢出等待阈值(ms)。

        tier=0 为最低档,越大段位越高;阈值 = clamp(base - tier×shorten, min, base)。
        """
        if tier < 0:
            tier = 0
        th = self.overflow_base_ms - tier * self.overflow_shorten_per_tier_ms
        if th < self.overflow_min_ms:
            th = self.overflow_min_ms
        if th > self.overflow_base_ms:
            th = self.overflow_base_ms
        return th

    def should_overflow(self, wait_ms: int, tier: int, local_candidates_enough: bool) -> bool:
        """一张票据是否到了"允许跨 region 溢出"的时机。

        双条件(§2.2):等待时长已过该段位阈值 **且** 本 region 同段位候选不足成局。
        两者皆满足才放开跨 region,避免人够还跨区。
        """
        if local_candidates_enough:
            return False
        return wait_ms >= self.overflow_threshold_ms(tier)

    def candidate_score(
        self, mmr_diff: int, anchor_region: int, candidate_region: int, est_rtt_ms: int
    ) -> float:
        """给一个候选在某锚点视角下打分,分越高越优先入选。

        评分 = -|mmrDiff| - RTT 惩罚:MMR 越接近越好,跨 region RTT 越大惩罚越重。
        同 region 时 est_rtt_ms 视为 0 → 仅 MMR 决定,永远优先同 region。
        """
        if mmr_diff < 0:
            mmr_diff = -mmr_diff
        score = -float(mmr_diff)
        if candidate_region != anchor_region:
            score -= self.rtt_penalty_per_ms * float(est_rtt_ms)
        return score

    def within_cross_region_cap(self, player_regions: Sequence[int]) -> bool:
        """一局玩家的 region 分布是否满足"跨 region 比例软上限"。

        取多数 region 为本局主 region,其余(少数派)玩家即"跨 region";
        少数派占比 ≤ `cross_region_ratio_cap_pct` 才合规。空输入视为合规。
        """
        n = len(player_regions)
        if n == 0:
            return True
        major, ok = majority_region(player_regions)
        if not ok:
            return True
        minority = sum(1 for r in player_regions if r != major)
        # minority/n ≤ cap/100  ⇔  minority×100 ≤ cap×n(整数比较,免浮点)
        return minority * 100 <= self.cross_region_ratio_cap_pct * n


def default_region_match_policy() -> RegionMatchPolicy:
    """一套保守默认值(决策文档 §2.2 量级:钻石+ ~30s、普通段 ~90s)。

    数值与 Go 的 `DefaultRegionMatchPolicy()` 逐个相同 —— 两栈并排跑时策略不同
    会让同一批票据溢出时机不一致,parity 探针会报出无法归因的差异。
    """
    return RegionMatchPolicy(
        rtt_penalty_per_ms=2.0,  # 每 1ms RTT 扣 2 分(与 MMR 差同量纲)
        cross_region_ratio_cap_pct=40,  # 一局跨 region 玩家 ≤40%
        overflow_base_ms=90000,  # 普通段 90s 才溢出
        overflow_shorten_per_tier_ms=20000,  # 每高一档减 20s
        overflow_min_ms=30000,  # 不低于 30s(高分段下限)
        mmr_bucket_width=200,  # 段位桶宽 200 分
        tier_base_mmr=2000,  # ≤2000 算普通段(tier 0)
        tier_step_mmr=400,  # 每 +400 分升一档
    )


def majority_region(regions: Sequence[int]) -> tuple[int, bool]:
    """一组玩家所属 region 里的多数派(出现次数最多的 region)。

    用于 §2.2 "battle Cell 选参战玩家多数所在 region"。
    空返回 (0, False);并列时返回 region 值较小者(确定性)。
    """
    if not regions:
        return 0, False
    count: dict[int, int] = {}
    for r in regions:
        count[r] = count.get(r, 0) + 1
    # 确定性:先按 region 值排序,再取计数最大者(并列取最小 region)。
    keys = sorted(count)
    best = keys[0]
    for r in keys[1:]:
        if count[r] > count[best]:
            best = r
    return best, True


@dataclasses.dataclass(frozen=True, slots=True, order=True)
class CellLocation:
    """一名玩家的物理落点 (region, cell),用于 battle DS 放置选择。

    与 `cellroute.Location` 解耦(只取放置决策需要的两维),让放置算法是纯函数、易测。
    `order=True` + 字段顺序 (region_id, cell_id) 让排序即"按 (region, cell) 升序",
    与 Go 的 `sort.Slice` 比较函数同序。
    """

    region_id: int
    cell_id: int


def majority_cell_location(locs: Sequence[CellLocation]) -> tuple[CellLocation, bool]:
    """一组参战玩家落点里的多数派 (region, cell)。

    scale-cellular-20m.md §4.4/§5:对局在"参战玩家多数所在 region 的 Cell"拉起
    battle DS,让多数玩家就近连入,少数跨 region 玩家承担稍高 RTT;结算仍各自回
    owner cell(不变量不破)。

    空返回 (CellLocation(0, 0), False);计数并列时按 (region, cell) 升序取最小者。
    """
    if not locs:
        return CellLocation(region_id=0, cell_id=0), False
    count: dict[CellLocation, int] = {}
    for loc in locs:
        count[loc] = count.get(loc, 0) + 1
    # 确定性:先按 (region, cell) 升序排候选,再取计数最大者(并列取最小落点)。
    keys = sorted(count)
    best = keys[0]
    for loc in keys[1:]:
        if count[loc] > count[best]:
            best = loc
    return best, True


def partition_tickets_by_region(
    tickets: Sequence[matchpb.MatchTicketStorageRecord],
    region_of: RegionResolver | None,
) -> tuple[dict[int, list[matchpb.MatchTicketStorageRecord]], list[int]]:
    """把(已按 MMR 排序的)票据按 owner region 分桶,保持桶内原相对顺序。

    返回 (buckets, order):order 按 region 值升序,保证撮合次序确定性。
    `region_of` 为 None 或对所有票据恒返回同一值时落入单桶 —— 等价于单 Cell 不分区。
    """
    buckets: dict[int, list[matchpb.MatchTicketStorageRecord]] = {}
    for t in tickets:
        region = region_of(t) if region_of is not None else 0
        buckets.setdefault(region, []).append(t)
    return buckets, sorted(buckets)


def leftover_region_bucket_totals(
    leftover: Iterable[matchpb.MatchTicketStorageRecord],
    region_of: RegionResolver | None,
    bucket_of: Callable[[matchpb.MatchTicketStorageRecord], int] | None,
) -> dict[tuple[int, int], int]:
    """统计 region 内撮合**之后**的剩余票据,按 (region, MMR 桶) 分组的**总人数**。

    供 `select_overflow_tickets` 判断"本 region 同 MMR 窗口"是否够成局。

    ★ 必须用 leftover(撮合后)而非撮合前的总人数:撮合前总人数足够 ≠ 撮合后某窗口
    还够,否则会因"全 region 人多"误判本地充足,卡住本该跨 region 兜底的久等票据(§2.2)。

    ★ 累加的是 `len(t.members)`(人数)不是票数 —— need 也是总人数,量纲必须一致。

    key 用 `(region, bucket)` 元组,对应 Go 的 `regionBucketKey` 结构体。
    `bucket_of` 为 None 时所有票据落桶 0(退化为按 region 统计)。
    """
    totals: dict[tuple[int, int], int] = {}
    for t in leftover:
        region = region_of(t) if region_of is not None else 0
        bucket = bucket_of(t) if bucket_of is not None else 0
        key = (region, bucket)
        totals[key] = totals.get(key, 0) + len(t.members)
    return totals


def select_overflow_tickets(
    leftover: Sequence[matchpb.MatchTicketStorageRecord],
    region_of: RegionResolver | None,
    leftover_totals: dict[tuple[int, int], int],
    bucket_of: Callable[[matchpb.MatchTicketStorageRecord], int] | None,
    need: int,
    policy: RegionMatchPolicy,
    tier_of: Callable[[matchpb.MatchTicketStorageRecord], int] | None,
    now_ms: int,
) -> list[matchpb.MatchTicketStorageRecord]:
    """从各 region 剩余(本 region 内未成局)票据里挑出"可跨 region 溢出"的票据。

    决策(§2.2,经 `RegionMatchPolicy.should_overflow`):

      - 等待时长已过该段位溢出阈值,且本 region 同段位/MMR 窗口候选不足成局 → 放开跨 region。
      - local_enough 基于 region 内撮合**后**的 leftover、按 (region, MMR 桶) 判定:
        `leftover_totals[(region, bucket_of(t))] >= need` 才算本地充足。这样 region
        总人数够、但本轮同段位/MMR 窗口剩余不足时,久等票据仍能跨 region 兜底。
      - `tier_of` 为 None 时恒按 tier 0;`bucket_of` 为 None 时恒按桶 0。

    返回的票据保持入参(MMR 升序)顺序,供跨 region 贪心装箱复用同一撮合路径。
    """
    out: list[matchpb.MatchTicketStorageRecord] = []
    for t in leftover:
        region = region_of(t) if region_of is not None else 0
        bucket = bucket_of(t) if bucket_of is not None else 0
        local_enough = leftover_totals.get((region, bucket), 0) >= need
        tier = tier_of(t) if tier_of is not None else 0
        wait_ms = now_ms - t.enqueued_at_ms
        if policy.should_overflow(wait_ms, tier, local_enough):
            out.append(t)
    return out
