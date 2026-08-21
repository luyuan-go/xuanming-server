"""matchmaker 两级撮合(region 内优先 + 跨 region 溢出)的回归测试。

分两层:

  ① `pandorapy.services.matchmaker.region_affinity` 的纯函数 —— 逐条对应 Go 的
     `internal/biz/region_affinity_test.go`。数值断言必须与 Go 逐个相同:两栈并排跑
     同一批票据时策略不一致会让溢出时机不同,parity 探针报出无法归因的差异。

  ② 接线层 —— `MatchLoopMixin._form_matches_in_pool` 在 router 未注入 / 已注入两种
     形态下的行为,以及 `main._self_region` 的取值来源。

★ 本文件存在的直接原因(2026-08-21 两处回归):

  1) `_self_region` 曾从 `cfg.model_extra["cell_route"]` 取值。`cell_route` 升级成
     `BaseConf` 的**正式 pydantic 字段**后,pydantic 就不再把它放进 `model_extra`,
     于是该函数**恒返回 0** → 选举名恒为 `matchmaker/<mode>/r0` → 跨 region 部署的
     所有副本挤进同一个选举,非 leader region 的撮合永久停摆且零错误日志。
  2) `_form_matches_in_pool` 曾**只有**单桶贪心,靠一句"cell_route.mode 非空一律拒启"
     的注释声称不存在"配了却静默按单桶跑"的窗口 —— 那道闸后来被拆掉,注释没跟着改。

  两条都是"看代码觉得没问题、跑起来静默错"的形状,只能靠断言钉住。
"""

from __future__ import annotations

import pathlib

import pytest
from pandora.match.v1 import match_pb2 as matchpb

from pandorapy import cellroute
from pandorapy.services.matchmaker import conf as mconf
from pandorapy.services.matchmaker import main as mmain
from pandorapy.services.matchmaker import region_affinity as ra
from pandorapy.services.matchmaker.matchloop import MatchLoopMixin

# 最小可加载配置:本文件只关心 cell_route 段,其余字段只为过完整性校验。
_MIN_YAML = """
server:
  grpc: {addr: ":29011"}
  http: {addr: ":29111"}
node:
  node_id: 1
  redis_client:
    host: "127.0.0.1:6399"
match:
  team_addr: "127.0.0.1:20010"
  match_resume_auth_secret: "pandora-test-match-resume-auth-key!!"
  match_resume_auth_audience: "matchmaker:test"
  map_id: 6
  game_mode: "5v5_ranked"
jwt:
  issuer: "pandora-login"
  audience: "pandora-client"
  secret: "pandora-test-jwt-secret-change-me!!!"
"""


def _load_cfg(tmp_path: pathlib.Path, extra: str = "") -> mconf.Config:
    path = tmp_path / "c.yaml"
    path.write_text(_MIN_YAML + extra, encoding="utf-8")
    return mconf.Config.load(str(path))

# ── 构造工具 ─────────────────────────────────────────────────────────────────


def _mk_ticket(
    tid: int, size: int, avg_mmr: int, enqueued_ms: int = 0, map_id: int = 6
) -> matchpb.MatchTicketStorageRecord:
    """一张测试票据(members 人数 = size,captain_id = tid)。对应 Go 的 `mkTicket`。"""
    return matchpb.MatchTicketStorageRecord(
        ticket_id=tid,
        captain_id=tid,
        members=[
            matchpb.MatchMemberStorageRecord(player_id=tid * 100 + i) for i in range(size)
        ],
        avg_mmr=avg_mmr,
        enqueued_at_ms=enqueued_ms,
        map_id=map_id,
    )


# ══════════════════════════════════════════════════════════════════════════════
# ① 纯函数:与 Go region_affinity_test.go 逐条对应
# ══════════════════════════════════════════════════════════════════════════════


def test_default_policy_values_match_go() -> None:
    """默认参数必须与 Go 的 `DefaultRegionMatchPolicy()` **逐个字节**相同。

    这不是"复制常量的重复断言":两栈跑同一批票据时,任何一个参数不同都会让溢出时机
    错开,而症状是"偶尔多一局跨区",极难归因。钉死在这里,改动必须两边同时改。
    """
    p = ra.default_region_match_policy()
    assert p.rtt_penalty_per_ms == 2.0
    assert p.cross_region_ratio_cap_pct == 40
    assert p.overflow_base_ms == 90000
    assert p.overflow_shorten_per_tier_ms == 20000
    assert p.overflow_min_ms == 30000
    assert p.mmr_bucket_width == 200
    assert p.tier_base_mmr == 2000
    assert p.tier_step_mmr == 400


def test_overflow_threshold_ms_tier_shortens() -> None:
    p = ra.default_region_match_policy()
    low = p.overflow_threshold_ms(0)
    mid = p.overflow_threshold_ms(3)
    high = p.overflow_threshold_ms(100)
    assert low == 90000
    assert mid < low
    assert high == p.overflow_min_ms  # 极高段撞下限
    assert p.overflow_threshold_ms(-5) == low  # 负 tier 当 0


def test_should_overflow_dual_condition() -> None:
    """双条件:本地不足 **且** 等待过阈值。缺任一条都不溢出。"""
    p = ra.default_region_match_policy()
    th = p.overflow_threshold_ms(0)
    assert not p.should_overflow(th + 1000, 0, True)  # 本地够 → 等再久也不跨区
    assert not p.should_overflow(th - 1, 0, False)  # 没到阈值
    assert p.should_overflow(th, 0, False)  # 恰好到阈值即放开


def test_candidate_score_same_region_preferred() -> None:
    p = ra.default_region_match_policy()
    same = p.candidate_score(100, 1, 1, 999)  # 同 region:RTT 入参被忽略
    assert same == -100
    cross = p.candidate_score(100, 1, 2, 50)
    assert cross < same
    assert p.candidate_score(-100, 1, 1, 0) == p.candidate_score(100, 1, 1, 0)


def test_candidate_score_closer_mmr_wins_within_region() -> None:
    p = ra.default_region_match_policy()
    assert p.candidate_score(50, 1, 1, 0) > p.candidate_score(300, 1, 1, 0)


def test_majority_region() -> None:
    assert ra.majority_region([]) == (0, False)
    assert ra.majority_region([1, 1, 1, 2, 3]) == (1, True)
    # 并列取较小 region —— 确定性,不能依赖 dict 迭代序
    assert ra.majority_region([2, 2, 3, 3])[0] == 2


def test_within_cross_region_cap() -> None:
    p = ra.default_region_match_policy()  # cap 40%
    assert p.within_cross_region_cap([1] * 10)  # 0% 跨区
    assert p.within_cross_region_cap([1] * 6 + [2] * 4)  # 恰好 40% ≤ 40%
    assert not p.within_cross_region_cap([1] * 5 + [2, 2, 3, 3, 3])  # 50% > 40%
    assert p.within_cross_region_cap([])  # 空视为合规


def test_partition_tickets_by_region_groups_and_orders() -> None:
    def region_of(t: matchpb.MatchTicketStorageRecord) -> int:
        return t.captain_id % 3

    tickets = [
        _mk_ticket(3, 1, 1000),
        _mk_ticket(1, 1, 1010),
        _mk_ticket(4, 1, 1020),
        _mk_ticket(2, 1, 1030),
        _mk_ticket(6, 1, 1040),
    ]
    buckets, order = ra.partition_tickets_by_region(tickets, region_of)
    assert order == [0, 1, 2]  # region 值升序
    # 桶内保持原相对顺序(即入参的 MMR 升序),否则贪心锚点会漂
    assert [t.ticket_id for t in buckets[1]] == [1, 4]
    assert len(buckets[0]) == 2
    assert len(buckets[2]) == 1


def test_partition_tickets_by_region_none_resolver_single_bucket() -> None:
    tickets = [_mk_ticket(1, 1, 1000), _mk_ticket(2, 1, 1000)]
    buckets, order = ra.partition_tickets_by_region(tickets, None)
    assert order == [0]
    assert len(buckets[0]) == 2


def test_leftover_region_bucket_totals_counts_people_not_tickets() -> None:
    """★ 累加的是**人数**不是票数:need 也是总人数,量纲错了会判反 local_enough。"""

    def region_of(t: matchpb.MatchTicketStorageRecord) -> int:
        return t.captain_id % 2

    def bucket_of(t: matchpb.MatchTicketStorageRecord) -> int:
        return t.avg_mmr // 1000

    leftover = [
        _mk_ticket(2, 3, 500),  # region 0,桶 0,3 人
        _mk_ticket(4, 2, 800),  # region 0,桶 0,2 人
        _mk_ticket(6, 1, 1500),  # region 0,桶 1,1 人
        _mk_ticket(1, 4, 1200),  # region 1,桶 1,4 人
    ]
    totals = ra.leftover_region_bucket_totals(leftover, region_of, bucket_of)
    assert totals[(0, 0)] == 5
    assert totals[(0, 1)] == 1
    assert totals[(1, 1)] == 4


def test_select_overflow_tickets_dual_condition() -> None:
    p = ra.default_region_match_policy()

    def region_of(t: matchpb.MatchTicketStorageRecord) -> int:
        return t.captain_id % 2

    def bucket_of(t: matchpb.MatchTicketStorageRecord) -> int:
        return 0  # 单桶:退化为按 region 判定

    now = 1_000_000_000
    need = 10
    waited = _mk_ticket(2, 1, 1000, now - 100_000)  # region 0,等 100s,本区不足 → 溢出
    too_fresh = _mk_ticket(4, 1, 1000, now - 10_000)  # 等 10s < 90s → 不溢出
    enough_region = _mk_ticket(1, 1, 1000, now - 100_000)  # region 1 本区够 → 不溢出

    got = ra.select_overflow_tickets(
        [waited, too_fresh, enough_region],
        region_of,
        {(0, 0): 2, (1, 0): 12},
        bucket_of,
        need,
        p,
        None,
        now,
    )
    assert [t.ticket_id for t in got] == [2]


def test_select_overflow_tickets_region_total_enough_but_window_short() -> None:
    """核心修复:region 总人数够,但久等票据所在 MMR 窗口剩余不足 → 仍须放开溢出。

    若误用"region 内撮合**前**总人数"判 local_enough(旧 bug),会因"全 region 人多"
    把这张票卡死 —— 症状是低分段玩家排队永不成局,且没有任何错误日志。
    """
    p = ra.default_region_match_policy()
    need = 10
    now = 1_000_000_000

    def all_same_region(t: matchpb.MatchTicketStorageRecord) -> int:
        return 0

    def bucket_of(t: matchpb.MatchTicketStorageRecord) -> int:
        return p.mmr_bucket(t.avg_mmr)

    def tier_of(t: matchpb.MatchTicketStorageRecord) -> int:
        return p.mmr_tier(t.avg_mmr)

    low_waited = _mk_ticket(1, 2, 1000, now - 100_000)  # 桶 5,2 人 < need → 应溢出
    high_enough = _mk_ticket(2, 10, 3000, now - 100_000)  # 桶 15,10 人 ≥ need → 不溢出
    leftover = [low_waited, high_enough]
    totals = ra.leftover_region_bucket_totals(leftover, all_same_region, bucket_of)
    # 前置断言:region 总人数 12 ≥ need,确保走的是"窗口不足"而非"region 整体不足"。
    assert sum(totals.values()) >= need

    got = ra.select_overflow_tickets(
        leftover, all_same_region, totals, bucket_of, need, p, tier_of, now
    )
    assert [t.ticket_id for t in got] == [low_waited.ticket_id]


def test_majority_cell_location_plurality_wins() -> None:
    locs = [
        ra.CellLocation(1, 10),
        ra.CellLocation(1, 10),
        ra.CellLocation(1, 11),
        ra.CellLocation(2, 20),
    ]
    got, ok = ra.majority_cell_location(locs)
    assert ok
    assert (got.region_id, got.cell_id) == (1, 10)


def test_majority_cell_location_tie_deterministic_smallest() -> None:
    """并列取 (region, cell) 升序最小者 —— 不确定的话两栈会把 DS 拉到不同 Cell。"""
    locs = [
        ra.CellLocation(2, 20),
        ra.CellLocation(2, 20),
        ra.CellLocation(1, 10),
        ra.CellLocation(1, 10),
    ]
    got, ok = ra.majority_cell_location(locs)
    assert ok
    assert (got.region_id, got.cell_id) == (1, 10)


def test_majority_cell_location_empty_not_ok() -> None:
    got, ok = ra.majority_cell_location([])
    assert not ok
    assert (got.region_id, got.cell_id) == (0, 0)


def test_mmr_bucket_segments_by_width() -> None:
    p = ra.default_region_match_policy()  # 宽 200
    assert p.mmr_bucket(0) == 0
    assert p.mmr_bucket(199) == 0
    assert p.mmr_bucket(200) == 1
    assert p.mmr_bucket(2050) == 10
    assert p.mmr_bucket(-50) == 0  # 负 MMR 归桶 0
    # 宽度非法 → 单桶 0(配置缺省保护,不能除零炸掉整条撮合循环)
    bad = ra.RegionMatchPolicy(
        rtt_penalty_per_ms=0.0,
        cross_region_ratio_cap_pct=0,
        overflow_base_ms=0,
        overflow_shorten_per_tier_ms=0,
        overflow_min_ms=0,
        mmr_bucket_width=0,
        tier_base_mmr=0,
        tier_step_mmr=0,
    )
    assert bad.mmr_bucket(9999) == 0


def test_mmr_tier_higher_mmr_higher_tier() -> None:
    p = ra.default_region_match_policy()  # base 2000,step 400
    assert p.mmr_tier(1500) == 0
    assert p.mmr_tier(2000) == 0  # 恰好 = base 仍算普通段
    assert p.mmr_tier(2400) == 1
    assert p.mmr_tier(3300) == 3
    bad = ra.RegionMatchPolicy(
        rtt_penalty_per_ms=0.0,
        cross_region_ratio_cap_pct=0,
        overflow_base_ms=0,
        overflow_shorten_per_tier_ms=0,
        overflow_min_ms=0,
        mmr_bucket_width=1,
        tier_base_mmr=2000,
        tier_step_mmr=0,
    )
    assert bad.mmr_tier(9999) == 0  # step 非法 → 恒 0


def test_mmr_tier_feeds_shorter_overflow_threshold() -> None:
    """段位桶与溢出阈值口径打通:高分段 tier 更高 → 阈值更短(人稀,早点跨区)。"""
    p = ra.default_region_match_policy()
    low_tier = p.mmr_tier(1800)
    high_tier = p.mmr_tier(3300)
    assert high_tier > low_tier
    assert p.overflow_threshold_ms(high_tier) < p.overflow_threshold_ms(low_tier)


# ══════════════════════════════════════════════════════════════════════════════
# ② 接线层:_self_region / _form_matches_in_pool
# ══════════════════════════════════════════════════════════════════════════════


def test_self_region_reads_modeled_cell_route(tmp_path: pathlib.Path) -> None:
    """★ 回归钉子:`_self_region` 必须读 `cfg.cell_route.self_region`(正式字段)。

    它曾经读 `cfg.model_extra["cell_route"]`。`cell_route` 升级成 `BaseConf` 的正式
    pydantic 字段后 `model_extra` 里就没有它了,该函数恒返回 0 →
    `election = "matchmaker/<mode>/r0"` 对所有 region 相同 → 跨 region 部署时所有副本
    挤进同一个 leader 选举,非 leader region 的撮合**永久停摆且零错误日志**(§9.20)。

    这里顺带断言 `cell_route` 确实不在 `model_extra` 里 —— 否则本用例会在
    "读法改回 model_extra 也能过"的情况下失去保护力。
    """
    cfg = _load_cfg(
        tmp_path,
        "cell_route:\n"
        '  mode: "static"\n'
        "  self_region: 7\n"
        "  self_cell: 3\n"
        "  cells:\n"
        "    - {region_id: 7, cell_id: 3, weight: 1}\n",
    )

    assert "cell_route" not in (cfg.model_extra or {})
    assert cfg.cell_route.self_region == 7
    assert mmain._self_region(cfg) == 7  # noqa: SLF001


def test_self_region_defaults_to_zero_when_unset(tmp_path: pathlib.Path) -> None:
    """未配 cell_route → 0,与 Go 单 Cell 部署的 SelfRegion 零值一致。"""
    assert mmain._self_region(_load_cfg(tmp_path)) == 0  # noqa: SLF001


class _PoolCfg:
    """`_form_matches_in_pool` 只用到 MMR 窗口三参数,这里给一个够宽的窗口。"""

    mmr_base_window = 10_000
    mmr_widen_per_sec = 0
    mmr_max_window = 10_000


class _FakeLoop(MatchLoopMixin):
    """只装配 `_form_matches_in_pool` 这条路径所需的最小依赖。

    不构造真 `MatchUsecase`:它要 repo / kafka / allocator / snowflake 五个真依赖,
    对"票据怎么分桶"这件事没有任何信息量,只会把用例变成集成测试。
    """

    def __init__(
        self, router: object | None, team_size: int = 1, side_count: int = 2
    ) -> None:
        self.cfg = _PoolCfg()
        self.router = router
        self.region_policy = ra.default_region_match_policy()
        self._team_size = team_size
        self._side_count = side_count
        self.formed: list[list[list[matchpb.MatchTicketStorageRecord]]] = []

    def team_size_for_map(self, map_id: int) -> int:
        return self._team_size

    def side_count_for_map(self, map_id: int) -> int:
        return self._side_count

    async def form_match(
        self, sides: list[list[matchpb.MatchTicketStorageRecord]]
    ) -> None:
        self.formed.append(sides)


def _formed_ticket_ids(loop: _FakeLoop) -> list[set[int]]:
    return [
        {t.ticket_id for side in sides for t in side} for sides in loop.formed
    ]


async def test_form_matches_in_pool_single_bucket_when_router_none() -> None:
    """router 未注入(单 Cell / dev)→ 单桶贪心,与 Go router==nil 完全一致。"""
    loop = _FakeLoop(router=None)
    now = 1_000_000_000
    tickets = [_mk_ticket(i, 1, 1000, now) for i in (1, 2)]
    await loop._form_matches_in_pool(tickets, now)  # noqa: SLF001
    assert _formed_ticket_ids(loop) == [{1, 2}]


class _FakeRouter:
    """按 player_id → region 的确定性桩(cell 恒等于 region,便于同时验放置)。"""

    def __init__(self, mapping: dict[int, int], unmapped: set[int] | None = None) -> None:
        self._mapping = mapping
        self._unmapped = unmapped or set()

    def route(self, player_id: int) -> cellroute.Location:
        if player_id in self._unmapped:
            raise cellroute.CellRouteError(f"no cell for {player_id}")
        region = self._mapping.get(player_id, 0)
        return cellroute.Location(region_id=region, cell_id=region, logical_cell=0)


async def test_form_matches_in_pool_prefers_same_region() -> None:
    """★ 回归钉子:配了 cell_route 就必须真的按 region 分桶,不能静默退化成单桶。

    4 张 1 人票,region 分别 1/1/2/2,每局 2 人。两级撮合应成出**两局同区**;
    若退化成单桶贪心,MMR 升序会把 (1,2) 和 (3,4) 凑一起 → 两局都跨区。
    """
    now = 1_000_000_000
    # captain_id 1..4;region 交错排布,使"按 MMR 顺序取相邻两张"必然跨区。
    router = _FakeRouter({1: 1, 2: 2, 3: 1, 4: 2})
    loop = _FakeLoop(router=router)
    tickets = [_mk_ticket(i, 1, 1000 + i, now) for i in (1, 2, 3, 4)]

    await loop._form_matches_in_pool(tickets, now)  # noqa: SLF001

    got = _formed_ticket_ids(loop)
    assert len(got) == 2
    assert {frozenset(s) for s in got} == {frozenset({1, 3}), frozenset({2, 4})}


async def test_form_matches_in_pool_no_cross_region_before_threshold() -> None:
    """本区凑不齐、但等待没到溢出阈值 → 不跨区成局(宁可继续排队)。

    5v5(need=10):region 1 有 6 人、region 2 有 4 人,各自都凑不满一局。
    """
    now = 1_000_000_000
    router = _FakeRouter({i: 1 for i in range(1, 7)} | {i: 2 for i in range(7, 11)})
    loop = _FakeLoop(router=router, team_size=5, side_count=2)
    # 全部同 MMR 桶(1000 // 200 == 5),刚入队。
    tickets = [_mk_ticket(i, 1, 1000, now) for i in range(1, 11)]

    await loop._form_matches_in_pool(tickets, now)  # noqa: SLF001
    assert loop.formed == []


async def test_form_matches_in_pool_overflows_after_threshold() -> None:
    """本区凑不齐且已等过阈值(90s)→ 放开跨 region 溢出兜底,玩家不被无限卡住。

    与上一例同一布局(6+4),只把入队时间拨到 100s 前—— 唯一变量就是等待时长。
    少数派 4/10 = 40% 恰好压线,不超跨 region 比例上限。
    """
    now = 1_000_000_000
    router = _FakeRouter({i: 1 for i in range(1, 7)} | {i: 2 for i in range(7, 11)})
    loop = _FakeLoop(router=router, team_size=5, side_count=2)
    tickets = [_mk_ticket(i, 1, 1000, now - 100_000) for i in range(1, 11)]

    await loop._form_matches_in_pool(tickets, now)  # noqa: SLF001
    assert _formed_ticket_ids(loop) == [set(range(1, 11))]


async def test_form_matches_in_pool_overflow_still_capped_by_ratio() -> None:
    """★ 溢出贪心必须带 validate 守卫:跨区比例超上限时宁可不成局。

    5+5 均分两 region → 少数派 5/10 = 50% > 40%,即使等了 100s 也不能成局。
    漏传 validate 的话上一个用例照样绿,只有这条会红。
    """
    now = 1_000_000_000
    router = _FakeRouter({i: 1 for i in range(1, 6)} | {i: 2 for i in range(6, 11)})
    loop = _FakeLoop(router=router, team_size=5, side_count=2)
    tickets = [_mk_ticket(i, 1, 1000, now - 100_000) for i in range(1, 11)]

    await loop._form_matches_in_pool(tickets, now)  # noqa: SLF001
    assert loop.formed == []


async def test_cross_region_cap_rejects_overloaded_group() -> None:
    """跨 region 溢出仍受比例上限约束:2 人局里 1 人跨区 = 50% > 40% → 拒绝成局。

    这条同时钉住"溢出贪心必须带 validate 守卫" —— 漏传 validate 的话上一个用例
    照样绿,只有这条会红。
    """
    loop = _FakeLoop(router=_FakeRouter({1: 1, 2: 2}))
    group = [_mk_ticket(1, 1, 1000), _mk_ticket(2, 1, 1000)]
    assert not loop._within_cross_region_cap(group)  # noqa: SLF001


def test_cross_region_cap_counts_people_not_tickets() -> None:
    """★ 按**人数**展开而非票数:5 人本区队 + 1 人外区散排 = 1/6 ≈ 17% ≤ 40%,合规。

    若按票数算会得出 1/2 = 50% 而误拒 —— 组队玩家会莫名其妙排不到队。
    """
    loop = _FakeLoop(router=_FakeRouter({1: 1, 2: 2}))
    group = [_mk_ticket(1, 5, 1000), _mk_ticket(2, 1, 1000)]
    assert loop._within_cross_region_cap(group)  # noqa: SLF001


def test_ticket_region_degrades_to_zero_on_unmapped_cell() -> None:
    """路由查不到 → 返回 0(未知/单桶),不抛异常打断整条撮合循环。

    撮合是可降级的(不分区仍能成局);§9.22 要求 fail-closed 的是玩家数据写落点那类
    权威判定,不是"这局分不分区"。
    """
    loop = _FakeLoop(router=_FakeRouter({1: 1}, unmapped={9}))
    assert loop._ticket_region(_mk_ticket(1, 1, 1000)) == 1  # noqa: SLF001
    assert loop._ticket_region(_mk_ticket(9, 1, 1000)) == 0  # noqa: SLF001


def test_battle_placement_none_router_not_ok() -> None:
    """router 未配 → ok=False,调用方不带放置提示(与 Go 同)。"""
    loop = _FakeLoop(router=None)
    _, ok = loop._battle_placement([1, 2, 3])  # noqa: SLF001
    assert not ok


def test_battle_placement_single_region_all_agree() -> None:
    loop = _FakeLoop(router=_FakeRouter({11: 1, 22: 1, 33: 1}))
    got, ok = loop._battle_placement([11, 22, 33])  # noqa: SLF001
    assert ok
    assert (got.region_id, got.cell_id) == (1, 1)


def test_battle_placement_skips_unroutable_players() -> None:
    """单个玩家路由失败只跳过该玩家,不抹掉整局的就近落点(多数派本就容忍缺样本)。"""
    loop = _FakeLoop(router=_FakeRouter({11: 2, 22: 2}, unmapped={33}))
    got, ok = loop._battle_placement([11, 22, 33])  # noqa: SLF001
    assert ok
    assert (got.region_id, got.cell_id) == (2, 2)


def test_battle_placement_all_unroutable_not_ok() -> None:
    loop = _FakeLoop(router=_FakeRouter({}, unmapped={1, 2}))
    _, ok = loop._battle_placement([1, 2])  # noqa: SLF001
    assert not ok


@pytest.mark.parametrize("mode", ["static", "etcd"])
def test_valid_cell_route_modes_no_longer_rejected_at_config_load(
    tmp_path: pathlib.Path, mode: str
) -> None:
    """★ 回归钉子:合法 mode 不该再被 config 加载期拒启。

    历史上 `BaseConf` 对任何非空 `mode` 一律拒启(因为 Python 侧还没有 Router 装配);
    装配补齐后必须放行,否则多 Region 部署根本起不来。反过来,非法 mode 仍须拒启 ——
    由 `test_matchmaker_main.test_gate_cellroute_init_failed_on_unsupported_mode` 覆盖。
    """
    body = (
        "cell_route:\n"
        f'  mode: "{mode}"\n'
        "  self_region: 1\n"
        "  self_cell: 1\n"
        "  cells:\n"
        "    - {region_id: 1, cell_id: 1, weight: 1}\n"
    )
    if mode == "etcd":
        body += '  etcd_endpoints: ["127.0.0.1:2379"]\n'

    cfg = _load_cfg(tmp_path, body)
    assert cfg.cell_route.mode == mode
    assert cfg.cell_route.enabled()
