"""孤儿 GameServer 回收测试。

这是全仓**后果最重**的一段逻辑:误删一台载人 DS = 一整局玩家被踢。
项目已经因人肉路径误删过两次,所以每一重防护都要有对应的测试。

四重防误删逐条验:
  ① 证据不可得 = 不删,且**证据中断即重新起算**
  ② 候选期跨轮观察,期间任一轮出现引用立即出候选
  ③ exact 复核(本文件测决策层,复核由编排层执行)
  ④ 权威出身台账 —— 空/错配 Redis 的进程一台都删不掉

另有一组「跨语言真值」测试:阈值下限、单轮封顶、首见表键的构成、引用集维度,
全部以 services/battle/ds_allocator/internal/biz/orphan_gameserver.go 为准。
这些值抄错不会让任何功能测试变红,只会在生产里删掉活着的 DS,所以必须单独钉死。
"""

from __future__ import annotations

import pytest

from pandorapy.services.ds_allocator import orphan_reclaim as orc

NOW = 1_000_000.0
AFTER = orc.DEFAULT_RECLAIM_AFTER_SEC  # 600s


def _gs(
    name: str,
    *,
    allocation_id: str = "",
    uid: str = "",
    deleting: bool = False,
) -> orc.GameServerInfo:
    return orc.GameServerInfo(
        name=name,
        uid=uid or f"uid-{name}",
        allocation_id=allocation_id,
        deleting=deleting,
    )


def _refs(
    *,
    names: set[str] | None = None,
    uids: set[str] | None = None,
    allocs: set[str] | None = None,
) -> orc.GameServerRefs:
    return orc.GameServerRefs(
        pod_names=set(names or ()),
        uids=set(uids or ()),
        allocation_ids=set(allocs or ()),
    )


NO_REFS = _refs()


def _reclaimed(decisions) -> list[str]:
    return [d.gs.name for d in decisions if d.result == orc.RESULT_RECLAIMED]


def _unprovable(decisions) -> list[str]:
    return [d.gs.name for d in decisions if d.result == orc.RESULT_UNPROVABLE]


# ── ★ ② 候选期跨轮观察 ────────────────────────────────────────────────────


def test_first_sight_never_deletes() -> None:
    """★ 首见只登记,**绝不删**。

    一轮证据不足以证明无人 —— 记录可能刚好在这一拍之间被读到空。
    """
    r = orc.OrphanReclaimer()
    d = r.plan_round(
        allocated=[_gs("gs-1", allocation_id="a1")],
        refs=NO_REFS,
        ledger_allocation_ids={"a1"},
        now_sec=NOW,
    )
    assert _reclaimed(d) == []
    assert r.observation_count() == 1


def test_reclaim_only_after_full_observation_window() -> None:
    """观察期未满不删;满了才删。"""
    r = orc.OrphanReclaimer()
    gs = [_gs("gs-1", allocation_id="a1")]
    kw = dict(allocated=gs, refs=NO_REFS, ledger_allocation_ids={"a1"})

    r.plan_round(now_sec=NOW, **kw)
    assert _reclaimed(r.plan_round(now_sec=NOW + AFTER - 1, **kw)) == []
    assert _reclaimed(r.plan_round(now_sec=NOW + AFTER, **kw)) == ["gs-1"]


def test_reference_appearing_mid_window_drops_candidate() -> None:
    """★ 期间任一轮出现引用 → **立即出候选**,重新从头开始。

    这条防的是:一台 GS 被释放后又被新的 GSA 分配走 ——
    它现在载人了,之前累积的观察期必须作废。
    """
    r = orc.OrphanReclaimer()
    gs = [_gs("gs-1", allocation_id="a1")]
    r.plan_round(allocated=gs, refs=NO_REFS, ledger_allocation_ids={"a1"}, now_sec=NOW)

    # 中途出现引用
    r.plan_round(
        allocated=gs,
        refs=_refs(names={"gs-1"}),
        ledger_allocation_ids={"a1"},
        now_sec=NOW + 100,
    )
    assert r.observation_count() == 0, "出现引用后没有出候选"

    # 引用消失后重新开始计时 —— 此时即使早已超过原窗口也不能删
    d = r.plan_round(
        allocated=gs,
        refs=NO_REFS,
        ledger_allocation_ids={"a1"},
        now_sec=NOW + AFTER + 100,
    )
    assert _reclaimed(d) == [], "出候选后没有重新计时"


def test_referenced_gs_is_never_a_candidate() -> None:
    """有记录的 GS 与本清扫无关(由既有判弃链负责)。"""
    r = orc.OrphanReclaimer()
    for t in range(0, AFTER * 2, 60):
        d = r.plan_round(
            allocated=[_gs("gs-1", allocation_id="a1")],
            refs=_refs(names={"gs-1"}),
            ledger_allocation_ids={"a1"},
            now_sec=NOW + t,
        )
        assert _reclaimed(d) == []


# ── ★ 首见表的键 = name + "/" + uid(名字复用事故)────────────────────────


def test_name_reuse_starts_a_fresh_observation_window() -> None:
    """★ 名字复用的**新** GameServer 绝不能继承前世的观察起点。

    Fleet 重建出的实例可能拿到同一个名字,但 UID 必定是新的。首见表若按裸
    name 索引,这台刚起来、马上要被分配的活 GS 一上来就"已观察满 10 分钟",
    第一轮对账就被删 —— 删的是载人 DS(§9「绝不删 Allocated GameServer」)。

    键带上 UID 后,它是一个全新的候选,必须从头观察满一个完整窗口。
    """
    r = orc.OrphanReclaimer()
    r.plan_round(
        allocated=[_gs("gs-1", uid="uid-old", allocation_id="a-old")],
        refs=NO_REFS,
        ledger_allocation_ids={"a-old", "a-new"},
        now_sec=NOW,
    )

    # 一个窗口之后,同名但 UID 全新的 GS 出现(旧的已被删除、Fleet 重建)
    d = r.plan_round(
        allocated=[_gs("gs-1", uid="uid-new", allocation_id="a-new")],
        refs=NO_REFS,
        ledger_allocation_ids={"a-old", "a-new"},
        now_sec=NOW + AFTER,
    )
    assert _reclaimed(d) == [], "名字复用的新 GS 继承了旧观察起点,第一轮就被删"
    assert r.observation_count() == 1, "旧 UID 的候选没有被修剪掉"

    # 新 UID 自己观察满一个完整窗口后才允许回收
    assert _reclaimed(
        r.plan_round(
            allocated=[_gs("gs-1", uid="uid-new", allocation_id="a-new")],
            refs=NO_REFS,
            ledger_allocation_ids={"a-new"},
            now_sec=NOW + AFTER * 2,
        )
    ) == ["gs-1"]


# ── ★ 引用集是三维的(pod 名 ∪ UID ∪ allocation_id)────────────────────────


def test_reference_by_uid_alone_protects_gs() -> None:
    """★ 记录里只写了 gameserver uid(没写 pod 名)的对局同样是活的。

    只按名字比对的实现会把这类记录当成"查无引用",观察满窗口后删掉一台
    正在打的 DS。三个维度任一命中都必须立刻出候选。
    """
    r = orc.OrphanReclaimer()
    gs = [_gs("gs-1", uid="uid-1", allocation_id="a1")]
    kw = dict(allocated=gs, refs=_refs(uids={"uid-1"}), ledger_allocation_ids={"a1"})
    r.plan_round(now_sec=NOW, **kw)
    assert r.observation_count() == 0, "按 UID 的引用没有被认出来"
    assert _reclaimed(r.plan_round(now_sec=NOW + AFTER, **kw)) == []


def test_reference_by_allocation_id_alone_protects_gs() -> None:
    """★ 同上:记录里只留下 allocation_id 的对局也必须被认作有引用。"""
    r = orc.OrphanReclaimer()
    gs = [_gs("gs-1", uid="uid-1", allocation_id="a1")]
    kw = dict(allocated=gs, refs=_refs(allocs={"a1"}), ledger_allocation_ids={"a1"})
    r.plan_round(now_sec=NOW, **kw)
    assert r.observation_count() == 0, "按 allocation_id 的引用没有被认出来"
    assert _reclaimed(r.plan_round(now_sec=NOW + AFTER, **kw)) == []


def test_empty_fields_do_not_match_empty_refs() -> None:
    """空值不参与匹配:空对空若算命中,真正的泄漏会被永久保护、回收不掉。"""
    refs = orc.GameServerRefs(pod_names={""}, uids={""}, allocation_ids={""})
    assert not refs.references(orc.GameServerInfo(name="gs-1", uid="uid-1"))


# ── ★ 删除宽限期内的 GS 不再处理 ──────────────────────────────────────────


def test_deleting_gs_is_not_a_candidate() -> None:
    """deletionTimestamp 已置 = 删除已受理,不必再发第二次删除。

    更要紧的是它必须**离开首见表**:否则一台长期卡在终止宽限的 GS 会一直
    占着表项,把「表容量 = 当前候选数」的有界性说法废掉。
    """
    r = orc.OrphanReclaimer()
    r.plan_round(
        allocated=[_gs("gs-1", allocation_id="a1")],
        refs=NO_REFS,
        ledger_allocation_ids={"a1"},
        now_sec=NOW,
    )
    assert r.observation_count() == 1

    d = r.plan_round(
        allocated=[_gs("gs-1", allocation_id="a1", deleting=True)],
        refs=NO_REFS,
        ledger_allocation_ids={"a1"},
        now_sec=NOW + AFTER,
    )
    assert _reclaimed(d) == []
    assert _unprovable(d) == []
    assert r.observation_count() == 0, "进入删除宽限的 GS 仍留在首见表里"


def test_gs_without_identity_is_skipped() -> None:
    """name / uid 任一为空 = 做不了 exact 复核删除(双 precondition),直接跳过。"""
    r = orc.OrphanReclaimer()
    kw = dict(
        allocated=[
            orc.GameServerInfo(name="", uid="uid-1", allocation_id="a1"),
            orc.GameServerInfo(name="gs-1", uid="", allocation_id="a1"),
        ],
        refs=NO_REFS,
        ledger_allocation_ids={"a1"},
    )
    r.plan_round(now_sec=NOW, **kw)
    assert r.observation_count() == 0
    assert r.plan_round(now_sec=NOW + AFTER, **kw) == []


# ── ★ ① 证据中断即重新起算 ────────────────────────────────────────────────


def test_evidence_gap_resets_observation() -> None:
    """★ 这条最容易漏,后果最重。

    权威记录读失败的轮次必须把**全部候选的观察起点重置为当前时刻**。

    不重置的后果:Redis 抖动 10 分钟(期间一轮证据都没拿到),
    抖动恢复后所有候选**立刻**满足"已观察 10 分钟"而被删 ——
    可那 10 分钟里没有任何证据支持它们无人。
    """
    r = orc.OrphanReclaimer()
    gs = [_gs("gs-1", allocation_id="a1")]
    kw = dict(allocated=gs, refs=NO_REFS, ledger_allocation_ids={"a1"})

    r.plan_round(now_sec=NOW, **kw)
    # 模拟 Redis 抖动:整段时间拿不到权威记录 → 编排层调 reset
    r.reset_all_observations(now_sec=NOW + AFTER)
    # 恢复后立刻又过了一整个窗口的墙钟,但观察期是从 reset 那一刻重新起算的
    assert _reclaimed(r.plan_round(now_sec=NOW + AFTER + 1, **kw)) == [], (
        "证据中断后墙钟静默推进,候选被误删"
    )
    assert _reclaimed(r.plan_round(now_sec=NOW + AFTER * 2, **kw)) == ["gs-1"]


# ── ★ ④ 权威出身台账(最关键的一层)──────────────────────────────────────


def test_empty_ledger_can_delete_nothing() -> None:
    """★ 这一层让「权威视图分裂」**机制上安全**,不靠配置纪律。

    一个读到空/错配 Redis 的进程(第二套部署、宿主残留、failover 到空实例)
    台账必然为空 ⇒ **一台都删不掉**。
    """
    r = orc.OrphanReclaimer()
    gs = [_gs(f"gs-{i}", allocation_id=f"a{i}") for i in range(5)]
    kw = dict(allocated=gs, refs=NO_REFS, ledger_allocation_ids=set())  # 空台账

    r.plan_round(now_sec=NOW, **kw)
    d = r.plan_round(now_sec=NOW + AFTER, **kw)
    assert _reclaimed(d) == [], "空台账的进程删掉了 GS —— 权威视图分裂时会误删"
    assert len(_unprovable(d)) == 5


def test_gs_without_allocation_label_is_kept() -> None:
    """无 label 的 GS(手工 GSA / 台账上线前的存量)只告警不删。

    拿不到证据 = 不删,由人按 never-delete-allocated 纪律处置。
    """
    r = orc.OrphanReclaimer()
    gs = [_gs("manual-gsa")]  # 没有 allocation_id
    kw = dict(allocated=gs, refs=NO_REFS, ledger_allocation_ids={"a1"})
    r.plan_round(now_sec=NOW, **kw)
    d = r.plan_round(now_sec=NOW + AFTER, **kw)
    assert _reclaimed(d) == []
    assert _unprovable(d) == ["manual-gsa"]
    assert "无 allocation-id" in d[0].reason


def test_allocation_id_not_in_ledger_is_kept() -> None:
    """label 有但台账里查不到 → 疑似权威视图分裂,保留不删。"""
    r = orc.OrphanReclaimer()
    gs = [_gs("gs-1", allocation_id="from-another-authority")]
    kw = dict(allocated=gs, refs=NO_REFS, ledger_allocation_ids={"a1", "a2"})
    r.plan_round(now_sec=NOW, **kw)
    d = r.plan_round(now_sec=NOW + AFTER, **kw)
    assert _reclaimed(d) == []
    assert "权威视图分裂" in d[0].reason


def test_leaked_gs_from_this_authority_is_reclaimed() -> None:
    """对比:真正由本权威分配后泄漏的 GS 台账必然有记录 ⇒ 照常回收。"""
    r = orc.OrphanReclaimer()
    gs = [_gs("leaked", allocation_id="a1")]
    kw = dict(allocated=gs, refs=NO_REFS, ledger_allocation_ids={"a1"})
    r.plan_round(now_sec=NOW, **kw)
    assert _reclaimed(r.plan_round(now_sec=NOW + AFTER, **kw)) == ["leaked"]


def test_authority_split_signal() -> None:
    """★ 「查无出身的候选数 ≈ 全部 Allocated」是权威视图分裂的强信号。

    正常情况下泄漏的是少数;几乎每台都查不到出身时,更可能是**本进程配置错了**,
    而不是集群真的全泄漏了 —— 这时该排查配置,不是"想办法把它们删掉"。
    """
    r = orc.OrphanReclaimer()
    gs = [_gs(f"gs-{i}", allocation_id=f"a{i}") for i in range(4)]
    kw = dict(allocated=gs, refs=NO_REFS, ledger_allocation_ids=set())
    r.plan_round(now_sec=NOW, **kw)
    d = r.plan_round(now_sec=NOW + AFTER, **kw)
    assert orc.all_candidates_unprovable(d, allocated_count=4)


def test_partial_unprovable_is_not_a_split_signal() -> None:
    """只有个别查无出身 → 正常泄漏,不是分裂信号。"""
    r = orc.OrphanReclaimer()
    gs = [_gs("ok", allocation_id="a1"), _gs("manual")]
    kw = dict(allocated=gs, refs=NO_REFS, ledger_allocation_ids={"a1"})
    r.plan_round(now_sec=NOW, **kw)
    d = r.plan_round(now_sec=NOW + AFTER, **kw)
    assert not orc.all_candidates_unprovable(d, allocated_count=2)


# ── 资源占用防护 ────────────────────────────────────────────────────────────


def test_per_round_delete_cap_is_enforced() -> None:
    """★ 单轮删除尝试封顶 —— 防孤儿轮饿死 §9.4 判弃链。"""
    r = orc.OrphanReclaimer(max_per_round=2)
    gs = [_gs(f"gs-{i}", allocation_id=f"a{i}") for i in range(10)]
    ids = {f"a{i}" for i in range(10)}
    kw = dict(allocated=gs, refs=NO_REFS, ledger_allocation_ids=ids)
    r.plan_round(now_sec=NOW, **kw)
    d = r.plan_round(now_sec=NOW + AFTER, **kw)
    assert len(_reclaimed(d)) == 2


def test_default_cap_matches_go_blast_radius() -> None:
    """★ 单轮封顶的真值是 3(Go orphanGSMaxReclaimPerRound)。

    它是爆炸半径而不是性能参数:抄大了,万一还有未知误删路径,一轮就能多删
    几台载人 DS。默认值必须与权威侧逐字一致。
    """
    assert orc.DEFAULT_MAX_RECLAIM_PER_ROUND == 3

    r = orc.OrphanReclaimer()  # 用默认封顶
    gs = [_gs(f"gs-{i}", allocation_id=f"a{i}") for i in range(10)]
    ids = {f"a{i}" for i in range(10)}
    kw = dict(allocated=gs, refs=NO_REFS, ledger_allocation_ids=ids)
    r.plan_round(now_sec=NOW, **kw)
    assert len(_reclaimed(r.plan_round(now_sec=NOW + AFTER, **kw))) == 3


def test_round_wall_clock_budget_stops_further_attempts() -> None:
    """★ 墙钟预算:对账轮与判弃链同协程,删除不能吃满整轮。

    编排层每处置一台要跑 GET 复核 + DELETE 两次外部调用,慢起来一轮就能把
    §9.4 判弃链饿死。预算在**首次尝试之后**才拦(否则开局就超预算会一台都
    删不成),超预算的候选留表下轮继续。
    """
    # 第一台处置完就把预算耗光(编排层量到的真实耗时)
    ticks = iter([99.0] * 8)
    r = orc.OrphanReclaimer(max_per_round=10, round_budget_sec=5.0)
    gs = [_gs(f"gs-{i}", allocation_id=f"a{i}") for i in range(4)]
    ids = {f"a{i}" for i in range(4)}
    kw = dict(allocated=gs, refs=NO_REFS, ledger_allocation_ids=ids)
    r.plan_round(now_sec=NOW, **kw)
    d = r.plan_round(now_sec=NOW + AFTER, elapsed_sec=lambda: next(ticks), **kw)
    assert len(_reclaimed(d)) == 1, "超出墙钟预算后仍在继续发删除"
    assert r.observation_count() == 4, "被预算拦下的候选丢了观察起点"


def test_capped_round_still_registers_later_candidates() -> None:
    """★ 封顶用 continue 不是 break:排在后面的 GS 仍要登记首见时间。

    提前跳出的后果:每轮都在同一批候选上耗光配额,后面的 GS 轮轮被当成首见
    重新计时,永远等不到阈值 —— 泄漏再也回收不掉,清扫等于对它们失效。
    """
    r = orc.OrphanReclaimer(max_per_round=1)
    old = [_gs("gs-a", allocation_id="aa"), _gs("gs-b", allocation_id="ab")]
    ids = {"aa", "ab", "ac"}
    r.plan_round(allocated=old, refs=NO_REFS, ledger_allocation_ids=ids, now_sec=NOW)

    # 第二轮:两台老候选已满窗口(配额只够 1 台),队尾多了一台全新的 gs-c
    fresh = [*old, _gs("gs-c", allocation_id="ac")]
    d = r.plan_round(
        allocated=fresh, refs=NO_REFS, ledger_allocation_ids=ids, now_sec=NOW + AFTER
    )
    assert len(_reclaimed(d)) == 1
    assert r.observation_count() == 3, "封顶后队尾的新候选没有被登记"


def test_capped_candidates_survive_to_next_round() -> None:
    """★ 超限的候选留在首见表下轮继续 —— 表跨轮持久,**只推迟不丢失**。"""
    r = orc.OrphanReclaimer(max_per_round=2)
    gs = [_gs(f"gs-{i}", allocation_id=f"a{i}") for i in range(5)]
    ids = {f"a{i}" for i in range(5)}
    kw = dict(allocated=gs, refs=NO_REFS, ledger_allocation_ids=ids)
    r.plan_round(now_sec=NOW, **kw)
    total = 0
    for k in range(1, 5):
        total += len(_reclaimed(r.plan_round(now_sec=NOW + AFTER + k, **kw)))
    assert total >= 5 - 2, "被封顶的候选在后续轮次丢失了"


# ── 观察期取值 ──────────────────────────────────────────────────────────────


def test_reclaim_window_exceeds_ticket_and_ready_wait() -> None:
    """★ 观察期必须 ≫ 票据硬上限(180s)+ ready_wait(120s)。

    这是判定链①「不可能再进来」的时长数学依据 ——
    窗口短于它就可能删掉一台正有玩家拿着有效票据在进的 DS。
    """
    assert orc.DEFAULT_RECLAIM_AFTER_SEC == 600
    assert orc.DEFAULT_RECLAIM_AFTER_SEC > 180 + 120


def test_reclaim_after_is_clamped_to_floor() -> None:
    """★ 阈值下限 300s(Go orphanGSReclaimAfterFloor = 5min),误配必须被钳住。

    没有下限时,yaml 里一个手滑的 30s 会让进程安静地按 30s 回收 —— 票据硬
    上限就有 180s,一台玩家正在进场的 DS 会在途中被删。
    0/负值视为未配置,走默认 600s。
    """
    assert orc.RECLAIM_AFTER_FLOOR_SEC == 300
    assert orc.OrphanReclaimer(reclaim_after_sec=30).reclaim_after_sec == 300
    assert orc.OrphanReclaimer(reclaim_after_sec=0).reclaim_after_sec == 600
    assert orc.OrphanReclaimer(reclaim_after_sec=-1).reclaim_after_sec == 600
    assert orc.OrphanReclaimer(reclaim_after_sec=1800).reclaim_after_sec == 1800


def test_below_floor_config_does_not_shorten_real_observation() -> None:
    """钳制必须作用在**判定**上,不只是那个只读属性。"""
    r = orc.OrphanReclaimer(reclaim_after_sec=30)
    gs = [_gs("gs-1", allocation_id="a1")]
    kw = dict(allocated=gs, refs=NO_REFS, ledger_allocation_ids={"a1"})
    r.plan_round(now_sec=NOW, **kw)
    assert _reclaimed(r.plan_round(now_sec=NOW + 60, **kw)) == [], "误配的 30s 阈值真的生效了"
    assert _reclaimed(r.plan_round(now_sec=NOW + 300, **kw)) == ["gs-1"]


def test_gs_disappearing_from_cluster_drops_candidate() -> None:
    """GS 已经从集群消失(别的路径删掉了)→ 出候选,不留悬空条目。"""
    r = orc.OrphanReclaimer()
    r.plan_round(
        allocated=[_gs("gs-1", allocation_id="a1")],
        refs=NO_REFS,
        ledger_allocation_ids={"a1"},
        now_sec=NOW,
    )
    assert r.observation_count() == 1
    r.plan_round(
        allocated=[], refs=NO_REFS, ledger_allocation_ids={"a1"}, now_sec=NOW + 10
    )
    assert r.observation_count() == 0


# ── 删除结果必须回填(对齐 Go orphan_gameserver.go:309-329)────────────────
#
# `plan_round` 只做规划;编排层执行完删除后必须把**实际结果**告诉回收器。
# 少了这一步,规划器就成了"只会说删、从不知道删没删成"的东西,而三个分支的
# 方向各不相同、写反都不报错。


def _observe_until_ripe(r, gs, alloc_ids):
    """让一台 GS 走到"观察期已满、下一轮就会被判回收"的状态。"""
    r.plan_round(allocated=[gs], refs=NO_REFS, ledger_allocation_ids=alloc_ids, now_sec=NOW)
    ripe = NOW + r.reclaim_after_sec + 1
    d = r.plan_round(allocated=[gs], refs=NO_REFS, ledger_allocation_ids=alloc_ids, now_sec=ripe)
    assert _reclaimed(d) == [gs.name], "前置没走到可回收状态"
    return ripe


def test_skipped_outcome_restarts_the_observation_window() -> None:
    """★ 本组最重要的一条。

    exact 复核失效 = 这台 GS 的 resourceVersion 变了 = 它刚发生过变更,
    **很可能已经被重新分配出去了**。不作废候选的话,首见时间还停在很久以前,
    下一轮阈值早已满足,会**立刻再发一次删除** —— 而这次它可能已经载人了。
    (§9「绝不删 Allocated GameServer」,已有两次事故。)
    """
    r = orc.OrphanReclaimer()
    gs = _gs("gs-skip", allocation_id="a1")
    ripe = _observe_until_ripe(r, gs, {"a1"})

    r.on_reclaim_outcome(gs, orc.RESULT_SKIPPED)
    assert r.observation_count() == 0, "复核失效后候选没被作废"

    # 紧接着的下一轮:只能重新登记,**不能**再判回收
    d = r.plan_round(allocated=[gs], refs=NO_REFS, ledger_allocation_ids={"a1"}, now_sec=ripe + 1)
    assert _reclaimed(d) == [], "复核失效后立刻又发了一次删除"
    assert r.observation_count() == 1, "没有重新开始观察"


def test_failed_outcome_keeps_the_candidate_for_retry() -> None:
    """调用本身失败:对象状态未知但也未被证伪,保留首见时间下轮幂等重试。

    与上一条方向**相反** —— 这里删键才是错的:一次网络抖动就把观察窗口整个重置,
    孤儿永远回收不掉。
    """
    r = orc.OrphanReclaimer()
    gs = _gs("gs-fail", allocation_id="a1")
    ripe = _observe_until_ripe(r, gs, {"a1"})

    r.on_reclaim_outcome(gs, orc.RESULT_FAILED)
    assert r.observation_count() == 1, "调用失败却把候选作废了"

    d = r.plan_round(allocated=[gs], refs=NO_REFS, ledger_allocation_ids={"a1"}, now_sec=ripe + 1)
    assert _reclaimed(d) == [gs.name], "失败后没有继续重试"


def test_reclaimed_outcome_drops_the_key() -> None:
    r = orc.OrphanReclaimer()
    gs = _gs("gs-ok", allocation_id="a1")
    _observe_until_ripe(r, gs, {"a1"})
    r.on_reclaim_outcome(gs, orc.RESULT_RECLAIMED)
    assert r.observation_count() == 0


def test_unknown_outcome_is_rejected_not_ignored() -> None:
    """静默忽略未知值会让首见表按错误的分支走(既不作废也不保留,取决于拼写)。"""
    r = orc.OrphanReclaimer()
    with pytest.raises(ValueError, match="未知的回收结果"):
        r.on_reclaim_outcome(_gs("gs-x", allocation_id="a1"), "deleted")


def test_outcome_keys_on_name_plus_uid_not_name_alone() -> None:
    """回填也要按 name+uid 定位 —— 只按 name 会把同名新实例的观察起点一起抹掉。"""
    r = orc.OrphanReclaimer()
    old = orc.GameServerInfo(name="gs-reuse", uid="uid-old", allocation_id="a1")
    new = orc.GameServerInfo(name="gs-reuse", uid="uid-new", allocation_id="a1")
    # 必须同一轮登记：plan_round 会把不在本轮候选集里的键修剪掉，
    # 分两轮调用的话第二轮就把 old 的观察起点清了（那是修剪，不是本条要测的东西）。
    r.plan_round(allocated=[old, new], refs=NO_REFS, ledger_allocation_ids={"a1"}, now_sec=NOW)
    assert r.observation_count() == 2, "同名不同 UID 应当是两个独立候选"

    r.on_reclaim_outcome(old, orc.RESULT_RECLAIMED)
    assert r.observation_count() == 1, "按名字回填把同名新实例的观察起点也抹了"
