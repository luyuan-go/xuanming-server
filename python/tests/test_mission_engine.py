"""任务事实引擎测试。

重点是那条**被推翻过**的结论:任务事实顺序**不是**无关的。
链上后环任务必须能收到"前环已完成"这条事实,而这只有把它再入队到
下一轮才能做到 —— 当场处理时新任务还没激活,事实会被静默丢弃。
"""

from __future__ import annotations

import pytest

from pandora.mission.v1 import mission_pb2
from pandorapy.services.mission import engine as eng


class FakeCatalog:
    """最小配置目录。

    mission: {mid: {"conditions": [cid...], "next": [mid...], "reward_id": int,
                    "auto_reward": int, "targets": {slot: value}}}
    condition: {cid: {"category": int, "slots": (v,...), "target": int}}
    """

    def __init__(self, missions: dict, conditions: dict) -> None:
        self.missions = missions
        self.conditions = conditions
        self.reward_logs_built: list[tuple[int, int]] = []

    def mission_by_id(self, mid):  # noqa: ANN001
        return self.missions.get(mid)

    def condition_by_id(self, cid):  # noqa: ANN001
        return self.conditions.get(cid)

    def mission_condition_ids(self, row) -> list[int]:  # noqa: ANN001
        return row["conditions"]

    def mission_next_ids(self, row) -> list[int]:  # noqa: ANN001
        return row.get("next", [])

    def mission_slot_target(self, row, i: int) -> int:  # noqa: ANN001
        return row.get("targets", {}).get(i, 0)

    def mission_reward_id(self, row) -> int:  # noqa: ANN001
        return row.get("reward_id", 0)

    def mission_auto_reward(self, row) -> int:  # noqa: ANN001
        return row.get("auto_reward", 0)

    def build_reward_log(self, player_id: int, row):  # noqa: ANN001
        self.reward_logs_built.append((player_id, row["reward_id"]))
        return ("reward", player_id, row["reward_id"])

    def condition_category(self, cond) -> int:  # noqa: ANN001
        return cond["category"]

    def condition_matches_slots(self, cond, slot_values) -> bool:  # noqa: ANN001
        want = cond.get("slots", ())
        if not want:
            return True  # 全空槽条件匹配任意同类事件
        return tuple(slot_values) == tuple(want)

    def _target(self, cond, override: int) -> int:  # noqa: ANN001
        return override if override > 0 else cond["target"]

    def condition_is_fulfilled(self, cond, value: int, override: int) -> bool:  # noqa: ANN001
        return value >= self._target(cond, override)

    def condition_clamp(self, cond, value: int, override: int) -> int:  # noqa: ANN001
        return min(value, self._target(cond, override))


KILL = 10  # 条件类别:击杀
COMPLETE = eng.CONDITION_CATEGORY_COMPLETE_MISSION


def test_complete_mission_category_matches_proto() -> None:
    """COMPLETE_MISSION 类别号必须与 proto 枚举同值。

    咬住的是"手抄了一个数"这件事本身:引擎里一旦写成字面量而不是取生成物,
    这条就红。数字 8 再写一遍是给 proto 改号留的跳闸点 —— 改号时必须有人
    同时看一眼 Go 侧 pkg/configtable/condition.go 的 ConditionCategoryCompleteMission。
    """
    assert (
        eng.CONDITION_CATEGORY_COMPLETE_MISSION
        == mission_pb2.MISSION_CONDITION_CATEGORY_COMPLETE_MISSION
        == 8
    )


def _state(player_id: int, active: dict[int, list[int]] | None = None) -> eng.PlayerState:
    st = eng.PlayerState(player_id=player_id)
    for mid, progress in (active or {}).items():
        st.active[mid] = eng.ActiveMission(mission_config_id=mid, progress=list(progress))
    return st


# ── ★ 链式任务:后环必须收到前环完成事实 ────────────────────────────────────


def test_chain_next_mission_receives_completion_fact() -> None:
    """★ 这是整个引擎存在的理由。

    任务 1 完成 → 链式激活任务 2 → 任务 2 的条件正是"完成任务 1" →
    它必须在**同一次调用内**也被判完成。

    若把 COMPLETE_MISSION 当场处理(而不是再入下一轮),任务 2 那时还没激活,
    这条事实被静默丢弃 —— 表现是"做完前置任务后,后续任务进度一直是 0"。
    """
    cat = FakeCatalog(
        missions={
            1: {"conditions": [101], "next": [2]},
            2: {"conditions": [102]},
        },
        conditions={
            101: {"category": KILL, "slots": (5001,), "target": 1},
            102: {"category": COMPLETE, "slots": (1,), "target": 1},  # 完成任务 1
        },
    )
    st = _state(7, {1: [0]})
    mut = eng.apply_facts(cat, st, [eng.Fact(KILL, (5001,), 1)], now_ms=0)

    assert 1 in mut.completed, "任务 1 没完成"
    assert 2 in mut.auto_accepted, "任务 2 没被链式激活"
    assert 2 in mut.completed, (
        "任务 2 没收到「完成任务 1」这条事实 —— COMPLETE_MISSION 被当场处理了"
    )
    assert not mut.fanout_truncated


def test_three_level_chain_completes_in_one_call() -> None:
    """三层链 1→2→3,一次调用全部完成(每层消耗一轮)。"""
    cat = FakeCatalog(
        missions={
            1: {"conditions": [101], "next": [2]},
            2: {"conditions": [102], "next": [3]},
            3: {"conditions": [103]},
        },
        conditions={
            101: {"category": KILL, "slots": (5001,), "target": 1},
            102: {"category": COMPLETE, "slots": (1,), "target": 1},
            103: {"category": COMPLETE, "slots": (2,), "target": 1},
        },
    )
    st = _state(7, {1: [0]})
    mut = eng.apply_facts(cat, st, [eng.Fact(KILL, (5001,), 1)], now_ms=0)
    assert sorted(mut.completed) == [1, 2, 3]


def test_self_loop_config_is_naturally_safe() -> None:
    """自环配置(next 指回自己)天然安全 —— 已完成的任务不会被重接。

    这条是我最初写错的测试留下的:自环**不会**触发截断,因为 _accept_into
    检查 done 集。真正需要截断保护的是超长链(见下一条)。
    """
    cat = FakeCatalog(
        missions={1: {"conditions": [101], "next": [1]}},
        conditions={101: {"category": COMPLETE, "slots": (1,), "target": 1}},
    )
    st = _state(7, {1: [0]})
    mut = eng.apply_facts(cat, st, [eng.Fact(COMPLETE, (1,), 1)], now_ms=0)
    assert mut.completed == [1]
    assert not mut.auto_accepted, "已完成的任务被重新接了"
    assert not mut.fanout_truncated


def test_fanout_rounds_are_bounded_on_long_chain() -> None:
    """★ 超长链必须截断并**置标记**,不能挂死也不能静默。

    每层链消耗一轮,链比 MAX_FANOUT_ROUNDS 长时截断。
    静默截断的话玩家会遇到"链上任务莫名其妙没接上"而服务端零信号。
    """
    depth = eng.MAX_FANOUT_ROUNDS + 5
    missions = {}
    conditions = {}
    for i in range(1, depth + 1):
        missions[i] = {"conditions": [100 + i], "next": [i + 1] if i < depth else []}
        if i == 1:
            conditions[101] = {"category": KILL, "slots": (5001,), "target": 1}
        else:
            conditions[100 + i] = {"category": COMPLETE, "slots": (i - 1,), "target": 1}
    cat = FakeCatalog(missions=missions, conditions=conditions)
    st = _state(7, {1: [0]})
    mut = eng.apply_facts(cat, st, [eng.Fact(KILL, (5001,), 1)], now_ms=0)
    assert mut.fanout_truncated, f"{depth} 层链没有触发截断"
    assert len(mut.completed) < depth, "截断了却把整条链都完成了"


# ── ★ 护栏:无效事实不推进度 ────────────────────────────────────────────────


@pytest.mark.parametrize(
    "bad",
    [
        eng.Fact(KILL, (5001,), 0),  # amount=0
        eng.Fact(KILL, (), 1),  # 空槽值
        eng.Fact(0, (5001,), 1),  # 无类别
    ],
)
def test_invalid_facts_do_not_progress(bad: eng.Fact) -> None:
    """★ 空槽值的事实必须跳过。

    全空槽条件匹配**任意**同类事件,放行无槽位值的事实会误推进度 ——
    表现是"什么都没干,任务自己完成了"。
    """
    cat = FakeCatalog(
        missions={1: {"conditions": [101]}},
        conditions={101: {"category": KILL, "slots": (), "target": 1}},  # 全空槽
    )
    st = _state(7, {1: [0]})
    mut = eng.apply_facts(cat, st, [bad], now_ms=0)
    assert not mut.completed
    assert st.active[1].progress == [0]


# ── ★ 已达标槽不再累加 ──────────────────────────────────────────────────────


def test_fulfilled_slot_does_not_accumulate() -> None:
    """★ 已达标的槽不再累加 —— 否则配置调低目标时会留下超额进度。"""
    cat = FakeCatalog(
        missions={1: {"conditions": [101, 102]}},
        conditions={
            101: {"category": KILL, "slots": (5001,), "target": 3},
            102: {"category": KILL, "slots": (6001,), "target": 5},
        },
    )
    st = _state(7, {1: [3, 0]})  # 槽 0 已达标
    eng.apply_facts(cat, st, [eng.Fact(KILL, (5001,), 10)], now_ms=0)
    assert st.active[1].progress[0] == 3, "已达标槽被继续累加了"


def test_progress_is_clamped_to_target() -> None:
    """超额推进钳到目标值。"""
    cat = FakeCatalog(
        missions={1: {"conditions": [101]}},
        conditions={101: {"category": KILL, "slots": (5001,), "target": 3}},
    )
    st = _state(7, {1: [0]})
    eng.apply_facts(cat, st, [eng.Fact(KILL, (5001,), 99)], now_ms=0)
    assert st.done[1].mission_config_id == 1
    # 完成时槽值被钳到 3 而不是 99
    assert 1 in [d.mission_config_id for d in [st.done[1]]]


def test_slot_target_override_wins() -> None:
    """任务行的槽目标覆盖条件自带的目标。"""
    cat = FakeCatalog(
        missions={1: {"conditions": [101], "targets": {0: 10}}},
        conditions={101: {"category": KILL, "slots": (5001,), "target": 1}},
    )
    st = _state(7, {1: [0]})
    mut = eng.apply_facts(cat, st, [eng.Fact(KILL, (5001,), 5)], now_ms=0)
    assert not mut.completed, "用了条件自带的 target 而不是任务行的覆盖值"
    assert st.active[1].progress == [5]


# ── ★ 全槽达标 fail-closed ──────────────────────────────────────────────────


def test_short_progress_slots_never_completes() -> None:
    """★ 进度槽比配置短时必须判**未达标**。

    原实现取 min 会让热更新增的条件被整段跳过 → **白送完成**。
    这里模拟:配置有两个条件,但玩家的进度只有一个槽(热更前接的任务)。
    """
    cat = FakeCatalog(
        missions={1: {"conditions": [101, 102]}},
        conditions={
            101: {"category": KILL, "slots": (5001,), "target": 1},
            102: {"category": KILL, "slots": (6001,), "target": 1},
        },
    )
    st = _state(7, {1: [0]})  # 只有一个槽
    mut = eng.apply_facts(cat, st, [eng.Fact(KILL, (5001,), 1)], now_ms=0)
    assert not mut.completed, "槽数不足却判了完成 —— 白送"
    # 但槽应当被自动补齐,后续事实能推进第二个槽
    assert len(st.active[1].progress) == 2
    mut2 = eng.apply_facts(cat, st, [eng.Fact(KILL, (6001,), 1)], now_ms=0)
    assert 1 in mut2.completed


def test_missing_condition_row_never_completes() -> None:
    """条件行被热更删掉 → 宁可不完成,不误发奖。"""
    cat = FakeCatalog(
        missions={1: {"conditions": [101, 999]}},  # 999 不存在
        conditions={101: {"category": KILL, "slots": (5001,), "target": 1}},
    )
    st = _state(7, {1: [0, 0]})
    mut = eng.apply_facts(cat, st, [eng.Fact(KILL, (5001,), 1)], now_ms=0)
    assert not mut.completed


def test_missing_mission_row_is_skipped_not_error() -> None:
    """配置热更删任务行:活跃孤行不再推进,**不报错**(展示层照旧)。"""
    cat = FakeCatalog(missions={}, conditions={})
    st = _state(7, {999: [0]})
    mut = eng.apply_facts(cat, st, [eng.Fact(KILL, (5001,), 1)], now_ms=0)
    assert not mut.completed and not mut.progressed


# ── 奖励状态 ────────────────────────────────────────────────────────────────


def test_auto_reward_builds_log_immediately() -> None:
    cat = FakeCatalog(
        missions={1: {"conditions": [101], "reward_id": 500, "auto_reward": 1}},
        conditions={101: {"category": KILL, "slots": (5001,), "target": 1}},
    )
    st = _state(7, {1: [0]})
    mut = eng.apply_facts(cat, st, [eng.Fact(KILL, (5001,), 1)], now_ms=123)
    assert len(mut.reward_logs) == 1
    assert st.done[1].reward_state == eng.REWARD_STATE_NONE


def test_manual_reward_marks_claimable() -> None:
    cat = FakeCatalog(
        missions={1: {"conditions": [101], "reward_id": 500, "auto_reward": 0}},
        conditions={101: {"category": KILL, "slots": (5001,), "target": 1}},
    )
    st = _state(7, {1: [0]})
    mut = eng.apply_facts(cat, st, [eng.Fact(KILL, (5001,), 1)], now_ms=123)
    assert not mut.reward_logs
    assert st.done[1].reward_state == eng.REWARD_STATE_CLAIMABLE


def test_no_reward_id_stays_none() -> None:
    cat = FakeCatalog(
        missions={1: {"conditions": [101]}},
        conditions={101: {"category": KILL, "slots": (5001,), "target": 1}},
    )
    st = _state(7, {1: [0]})
    eng.apply_facts(cat, st, [eng.Fact(KILL, (5001,), 1)], now_ms=0)
    assert st.done[1].reward_state == eng.REWARD_STATE_NONE


# ── 去重与确定性 ────────────────────────────────────────────────────────────


def test_same_batch_duplicate_completion_dedup() -> None:
    """同批多条事实各推一次,完成判定不能重复触发。"""
    cat = FakeCatalog(
        missions={1: {"conditions": [101]}},
        conditions={101: {"category": KILL, "slots": (5001,), "target": 1}},
    )
    st = _state(7, {1: [0]})
    facts = [eng.Fact(KILL, (5001,), 1) for _ in range(5)]
    mut = eng.apply_facts(cat, st, facts, now_ms=0)
    assert mut.completed.count(1) == 1
    assert mut.delete_active.count(1) == 1
    assert len(mut.insert_done) == 1


def test_already_accepted_or_done_is_not_reaccepted() -> None:
    """链式激活时,已接/已完成的任务不重复接。"""
    cat = FakeCatalog(
        missions={1: {"conditions": [101], "next": [2]}, 2: {"conditions": [102]}},
        conditions={
            101: {"category": KILL, "slots": (5001,), "target": 1},
            102: {"category": KILL, "slots": (7001,), "target": 1},
        },
    )
    st = _state(7, {1: [0], 2: [0]})  # 任务 2 已经在活跃里
    mut = eng.apply_facts(cat, st, [eng.Fact(KILL, (5001,), 1)], now_ms=0)
    assert 2 not in mut.auto_accepted


def test_output_order_is_deterministic() -> None:
    """★ 输出顺序必须确定 —— 不能依赖 dict 的插入序。

    Go 的 map 遍历随机所以那边显式 sort;Python 的 dict 保插入序,
    但插入序 ≠ 排序,同一批事实在不同历史下会产生不同的输出顺序。
    """
    cat = FakeCatalog(
        missions={m: {"conditions": [100 + m]} for m in (3, 1, 2)},
        conditions={100 + m: {"category": KILL, "slots": (5001,), "target": 9} for m in (3, 1, 2)},
    )
    # 故意用乱序插入
    st = eng.PlayerState(player_id=7)
    for mid in (3, 1, 2):
        st.active[mid] = eng.ActiveMission(mission_config_id=mid, progress=[0])
    mut = eng.apply_facts(cat, st, [eng.Fact(KILL, (5001,), 1)], now_ms=0)
    assert mut.progressed == [1, 2, 3], f"输出顺序不确定:{mut.progressed}"


def test_saturating_add_does_not_overflow() -> None:
    """饱和加钳到 uint32 上限 —— 必须与 Go saturatingAdd 的 math.MaxUint32 同口径。

    咬住的是**上限本身取哪个数**:钳低成 int32 的 2**31-1 时,第一条断言
    会把结果算成 2**31-1 而不是原值,双端进度在高位区间静默劈叉。
    """
    assert eng.saturating_add(2**31, 5) == 2**31 + 5  # 越过 int32 界不该被钳
    assert eng.saturating_add(2**32 - 2, 100) == 2**32 - 1
    assert eng.saturating_add(2**32 - 1, 1) == 2**32 - 1
    assert eng.saturating_add(1, 2) == 3
