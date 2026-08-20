"""任务域配置表:加载语义 + 条件判定件 + 跨表校验。

这些校验全都在**加载边界**上,放过一条的后果都不是报错,而是运行期一个不报错的
错误行为(白送完成 / 永久活锁 / 发放侧 OOM)。
"""

from __future__ import annotations

import pathlib

import pytest
from pandora.config.v1 import mission_pb2 as cfgpb
from pandora.mission.v1 import mission_pb2

from pandorapy.configtable import ConfigTableError
from pandorapy.services.mission import catalog as mc


# ── CSV 列解析 ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "want"), [("", []), ("1", [1]), (" 1 , 2,3 ", [1, 2, 3])]
)
def test_parse_uint32_csv_ok(raw: str, want: list[int]) -> None:
    assert mc.parse_uint32_csv(raw) == want


@pytest.mark.parametrize("bad", ["1,,2", "1,", "a", "-1", "+1", "0x10", str(2**32)])
def test_parse_uint32_csv_rejects(bad: str) -> None:
    """口径必须与 Go 的 strconv.ParseUint(p,10,32) 一样窄。

    放过 "+1" / 空元素的后果:条件槽少一格或多一格,任务判定按错误的槽数算 ——
    加载不报错,运行期白送完成。
    """
    with pytest.raises(ConfigTableError):
        mc.parse_uint32_csv(bad)


# ── 条件判定件 ───────────────────────────────────────────────────────────────


def _cond(**kw) -> cfgpb.ConditionRow:
    kw.setdefault("id", 1)
    kw.setdefault("name", "c")
    kw.setdefault("condition_category", 1)
    return cfgpb.ConditionRow(**kw)


def test_slot_target_override_wins() -> None:
    cond = _cond(target_count=10)
    assert mc.condition_effective_target(cond, 0) == 10
    assert mc.condition_effective_target(cond, 3) == 3


def test_gt_clamp_does_not_undo_fulfillment() -> None:
    """回归:GT 条件 clamp 到 target 会造成**永久活锁**。

    target=5 的 GT 条件,进度 6 达标 → 若 clamp 到 5,再判 `5 > 5` 为假 → 任务回到
    未完成而进度写死 5;下一条事实推到 6 又被打回 5,任务再也完不成。
    正解是 clamp 到**最小达标值** target+1。
    """
    cond = _cond(target_count=5, comparison_op=mc.CONDITION_COMPARE_GT)
    clamped = mc.condition_clamp_if_fulfilled(cond, 9, 0)
    assert clamped == 6
    assert mc.condition_is_fulfilled(cond, clamped, 0) is True


def test_ge_clamp_stops_at_target() -> None:
    cond = _cond(target_count=5, comparison_op=mc.CONDITION_COMPARE_GE)
    assert mc.condition_clamp_if_fulfilled(cond, 99, 0) == 5
    assert mc.condition_clamp_if_fulfilled(cond, 3, 0) == 3


def test_empty_slots_match_any_same_category_fact() -> None:
    assert mc.condition_matches_event_slots(_cond(), [1, 2, 3]) is True


def test_missing_slot_value_is_fail_closed() -> None:
    """事实槽位数不足时,缺位的非空槽判**不命中** —— 宁可不推进,不误推。"""
    cond = _cond(slot1="1", slot2="7")
    assert mc.condition_matches_event_slots(cond, [1]) is False
    assert mc.condition_matches_event_slots(cond, [1, 7]) is True
    assert mc.condition_matches_event_slots(cond, [1, 8]) is False


@pytest.mark.parametrize(
    "op",
    [mc.CONDITION_COMPARE_LE, mc.CONDITION_COMPARE_LT, mc.CONDITION_COMPARE_EQ],
)
def test_non_monotonic_ops_are_unusable(op: int) -> None:
    _, ok = mc.condition_min_fulfilling_progress(op, 5)
    assert ok is False


def test_gt_max_uint32_has_no_solution() -> None:
    _, ok = mc.condition_min_fulfilling_progress(
        mc.CONDITION_COMPARE_GT, mc.MAX_UINT32
    )
    assert ok is False


def test_complete_mission_category_comes_from_proto() -> None:
    """类别号是跨语言线上契约,必须取生成物 —— 手抄错了只表现为链上任务进度恒 0。"""
    assert (
        mc.CONDITION_CATEGORY_COMPLETE_MISSION
        == mission_pb2.MISSION_CONDITION_CATEGORY_COMPLETE_MISSION
    )


# ── 逐行校验 ─────────────────────────────────────────────────────────────────


def _mission(**kw) -> cfgpb.MissionRow:
    kw.setdefault("id", 1)
    kw.setdefault("name", "m")
    kw.setdefault("mission_type", 1)
    kw.setdefault("condition_ids", "1")
    return cfgpb.MissionRow(**kw)


def test_mission_row_rejects_empty_conditions() -> None:
    with pytest.raises(ConfigTableError, match="永远无法完成"):
        mc.validate_mission_row(_mission(condition_ids=""))


def test_mission_row_rejects_too_many_slots() -> None:
    ids = ",".join(str(i) for i in range(1, mc.MAX_MISSION_CONDITION_SLOTS + 2))
    with pytest.raises(ConfigTableError, match="超上限"):
        mc.validate_mission_row(_mission(condition_ids=ids))


def test_mission_row_rejects_target_length_mismatch() -> None:
    with pytest.raises(ConfigTableError, match="不等"):
        mc.validate_mission_row(_mission(condition_ids="1,2", target_counts="3"))


def test_mission_row_rejects_self_loop() -> None:
    with pytest.raises(ConfigTableError, match="自环"):
        mc.validate_mission_row(_mission(id=7, next_mission_ids="7"))


def test_mission_row_rejects_auto_reward_without_reward() -> None:
    with pytest.raises(ConfigTableError, match="自动发奖"):
        mc.validate_mission_row(_mission(auto_reward=1, reward_id=0))


def test_reward_row_rejects_duplicate_items() -> None:
    with pytest.raises(ConfigTableError, match="重复"):
        mc.validate_reward_row(
            cfgpb.RewardRow(id=1, name="r", item_ids="5,5", item_counts="1,1")
        )


def test_reward_row_rejects_empty_reward() -> None:
    with pytest.raises(ConfigTableError, match="空奖励行"):
        mc.validate_reward_row(cfgpb.RewardRow(id=1, name="r"))


# ── 跨表校验 ─────────────────────────────────────────────────────────────────


def _tables(missions, conditions, rewards, *, equipment=(), items=()) -> mc.Tables:
    return mc.Tables(
        version=1,
        source_rev="t",
        missions={m.id: m for m in missions},
        conditions={c.id: c for c in conditions},
        rewards={r.id: r for r in rewards},
        equipment_items=frozenset(equipment),
        item_ids=frozenset(items) | frozenset(equipment),
    )


def test_cross_rejects_missing_condition() -> None:
    tb = _tables([_mission(condition_ids="99")], [], [])
    with pytest.raises(ConfigTableError, match="不存在的条件"):
        mc.validate_mission_cross_tables(tb)


def test_cross_rejects_non_monotonic_comparison_op() -> None:
    """LE 用作任务条件 = 进度 0 就达标 = **白送完成**,必须整批拒。"""
    tb = _tables(
        [_mission(condition_ids="1")],
        [_cond(id=1, target_count=5, comparison_op=mc.CONDITION_COMPARE_LE)],
        [],
    )
    with pytest.raises(ConfigTableError, match="不能用作任务条件"):
        mc.validate_mission_cross_tables(tb)


def test_cross_detects_indirect_cycle() -> None:
    """链环必须**加载期**拒:运行期 16 轮迭代上限只是纵深兜底,不是许可。"""
    tb = _tables(
        [
            _mission(id=1, next_mission_ids="2"),
            _mission(id=2, next_mission_ids="3"),
            _mission(id=3, next_mission_ids="1"),
        ],
        [_cond(id=1, target_count=1)],
        [],
    )
    with pytest.raises(ConfigTableError, match="成环"):
        mc.validate_mission_cross_tables(tb)


def test_cross_accepts_long_linear_chain() -> None:
    """回归:环检测若写成递归,一条 1500 长的**合法**直链会撞 Python 递归上限,
    把"配置没问题"误报成加载失败(Go 的递归没有这个限制)。"""
    n = 1500
    missions = [
        _mission(id=i, next_mission_ids=(str(i + 1) if i < n else ""))
        for i in range(1, n + 1)
    ]
    tb = _tables(missions, [_cond(id=1, target_count=1)], [])
    mc.validate_mission_cross_tables(tb)  # 不抛即通过


def test_cross_rejects_equipment_total_across_entries() -> None:
    """累计而不是逐条判:「两条各 33 件」单条都合规、合计 66 件越界。

    只判单条会让这份奖励整批过审、落进 reward_pb 快照、任务同事务置 CLAIMED,
    然后在发放侧的累计闸上**永远发不出去** —— 玩家永久损失该任务全部奖励。
    """
    half = mc.MAX_REWARD_EQUIPMENT_INSTANCES // 2 + 1
    reward = cfgpb.RewardRow(
        id=1, name="r", item_ids="101,102", item_counts=f"{half},{half}"
    )
    tb = _tables(
        [_mission(condition_ids="1")],
        [_cond(id=1, target_count=1)],
        [reward],
        equipment=(101, 102),
    )
    with pytest.raises(ConfigTableError, match="装备累计件数"):
        mc.validate_mission_cross_tables(tb)


def test_cross_rejects_too_many_mission_rows() -> None:
    missions = [
        _mission(id=i, condition_ids="1") for i in range(1, mc.MAX_MISSION_ROWS + 2)
    ]
    tb = _tables(missions, [_cond(id=1, target_count=1)], [])
    with pytest.raises(ConfigTableError, match="任务表行数"):
        mc.validate_mission_cross_tables(tb)


# ── 真批次加载 ───────────────────────────────────────────────────────────────


def test_loads_real_dist_batch(configtable_dist: pathlib.Path) -> None:
    """刻意用真数据:checksum、行数、跨表引用都是**当前线上事实**。

    造假夹具只能验代码自己,验不了"Python 版和 Go 版看到的是同一个批次"。
    """
    res = mc.load_tables(configtable_dist)
    assert res.version > 0
    assert res.tables.mission_count() > 0
    assert res.tables.condition_count() > 0
    cat = mc.Catalog(res.tables)
    for mid, row in res.tables.missions.items():
        assert cat.mission_by_id(mid) is row
        for cid in mc.mission_condition_ids(row):
            assert cat.condition_by_id(cid) is not None


def test_missing_table_is_batch_rejected(tmp_path: pathlib.Path) -> None:
    """manifest 缺本进程必需的表 → 整批拒绝(不是"少一张表就少一个功能")。"""
    (tmp_path / "manifest.json").write_text(
        '{"version":1,"tables":[{"name":"dialogue","file":"dialogue.json",'
        '"proto":"pandora.config.v1.DialogueTableData","checksum":"sha256:00","rows":0}]}',
        encoding="utf-8",
    )
    with pytest.raises(ConfigTableError, match="必需的表"):
        mc.load_tables(tmp_path)
