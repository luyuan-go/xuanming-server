"""player 配置表整批门禁 —— 每条闸都对应一类"配错了不报错"的事故。

★ 真实 dist 用来验**跨语言一致性**(checksum 字节口径、行数、proto 名):造假数据只能
  验代码自己。逐条门禁则用合成行做变异实验 —— 把闸拆掉必须变红,否则这个闸是摆设。
"""

from __future__ import annotations

import pathlib

import pytest

from pandora.config.v1 import player_level_exp_pb2 as lvl_pb
from pandora.config.v1 import skill_card_pb2 as card_pb
from pandora.config.v1 import talent_effect_pb2 as teff_pb
from pandora.config.v1 import talent_pb2 as talent_pb

from pandorapy.configtable import ConfigTableError
from pandorapy.services.player import tables as pt


# ── 真实批次 ─────────────────────────────────────────────────────────────────


def test_loads_real_dist(configtable_dist: pathlib.Path) -> None:
    """真实 dist 必须整批通过 —— 门禁平时是绿的才有人留着(§7.2)。"""
    if not configtable_dist.is_dir():
        pytest.skip("configtable/dist 不在")
    result = pt.load_tables(configtable_dist)
    assert result.version > 0
    tables = result.tables
    assert tables.max_level() >= 2
    # 曲线长度 = 等级数 - 1(Lv1→末级的每一跳)。
    assert len(tables.experience_curve()) == tables.max_level() - 1
    assert tables.items and tables.talents and tables.skill_cards and tables.card_upgrade


def test_expect_version_mismatch_rejects(configtable_dist: pathlib.Path) -> None:
    if not configtable_dist.is_dir():
        pytest.skip("configtable/dist 不在")
    with pytest.raises(ConfigTableError):
        pt.load_tables(configtable_dist, expect_version=1)


def test_hot_reload_must_not_shrink_max_level(configtable_dist: pathlib.Path) -> None:
    """★ player 特有的热更门禁:新批次不得缩短最高等级。

    少了它,一次误发的短表会让满级玩家在下一次 AddExperience 被按新上限重新结算,
    等级凭空掉下去且不可逆。
    """
    if not configtable_dist.is_dir():
        pytest.skip("configtable/dist 不在")
    current = pt.load_tables(configtable_dist).tables.max_level()
    # 声称"当前生效批次的上限比新批次高一级" → 必须拒绝换批。
    with pytest.raises(ConfigTableError, match="不允许从"):
        pt.load_tables(configtable_dist, current_max_level=current + 1)
    # 等于当前上限则放行(同批次重载是 no-op,不该被误拒)。
    pt.load_tables(configtable_dist, current_max_level=current)


def test_missing_dir_rejects(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ConfigTableError):
        pt.load_tables(tmp_path / "nope")


# ── 玩家等级曲线 ─────────────────────────────────────────────────────────────


def _levels(rows: list[tuple[int, int, int]]) -> dict[int, object]:
    """rows = [(level, upgrade_exp, cumulative_exp)]。"""
    return {
        lv: lvl_pb.PlayerLevelExpRow(
            id=lv, level=lv, upgrade_exp=up, cumulative_exp=cum
        )
        for lv, up, cum in rows
    }


def test_level_curve_happy_path() -> None:
    pt._validate_level_curve(_levels([(1, 100, 0), (2, 200, 100), (3, 0, 300)]))


def test_level_curve_rejects_gap() -> None:
    """等级跳号 → experience_curve() 会短一截,玩家在中途就"满级"了,不报错。"""
    broken = _levels([(1, 100, 0), (3, 0, 100)])
    with pytest.raises(ConfigTableError, match="缺少 Lv2"):
        pt._validate_level_curve(broken)


def test_level_curve_rejects_cumulative_drift() -> None:
    """累计经验与逐级经验必须自洽 —— 漂了说明表被手改过。"""
    with pytest.raises(ConfigTableError, match="累计经验"):
        pt._validate_level_curve(_levels([(1, 100, 0), (2, 200, 999), (3, 0, 300)]))


def test_level_curve_rejects_nonzero_last_upgrade_exp() -> None:
    with pytest.raises(ConfigTableError, match="末级"):
        pt._validate_level_curve(_levels([(1, 100, 0), (2, 50, 100)]))


def test_level_curve_rejects_zero_mid_upgrade_exp() -> None:
    """非末级 upgrade_exp=0 会让进位循环在这级**停住**(advance_experience 的第 ③ 条)。"""
    with pytest.raises(ConfigTableError, match="必须大于 0"):
        pt._validate_level_curve(_levels([(1, 0, 0), (2, 100, 0), (3, 0, 100)]))


def test_level_curve_rejects_too_few_and_too_many() -> None:
    with pytest.raises(ConfigTableError, match="至少需要"):
        pt._validate_level_curve(_levels([(1, 0, 0)]))
    with pytest.raises(ConfigTableError, match="超过上限"):
        big = [(lv, 1, lv - 1) for lv in range(1, pt.MAX_PLAYER_LEVELS + 2)]
        big[-1] = (len(big), 0, len(big) - 1)
        pt._validate_level_curve(_levels(big))


def test_level_row_id_must_equal_level() -> None:
    with pytest.raises(ConfigTableError, match="必须与等级"):
        pt._validate_player_level_exp_row(lvl_pb.PlayerLevelExpRow(id=2, level=3))


# ── 专精 ─────────────────────────────────────────────────────────────────────


def _talent(**kw) -> talent_pb.TalentRow:
    base = {"id": 1, "name": "力量", "max_level": 5, "cost_per_level": 1}
    base.update(kw)
    return talent_pb.TalentRow(**base)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"name": ""}, "名称"),
        ({"max_level": 0}, "永远点不出来"),
        ({"max_level": pt.MAX_TALENT_LEVEL + 1}, "防总消耗溢出"),
        ({"cost_per_level": 0}, "可无限点满"),
        ({"cost_per_level": pt.MAX_TALENT_COST_PER_LEVEL + 1}, "防总消耗溢出"),
        ({"require_talent_id": 1, "require_talent_level": 1}, "指向自身"),
        ({"require_talent_level": 2}, "无前置专精却填了前置等级"),
        ({"require_talent_id": 2}, "却未填前置等级"),
    ],
)
def test_talent_row_gates(kwargs: dict, match: str) -> None:
    with pytest.raises(ConfigTableError, match=match):
        pt._validate_talent_row(_talent(**kwargs))


def test_talent_tree_rejects_missing_prerequisite() -> None:
    talents = {1: _talent(id=1, require_talent_id=9, require_talent_level=1)}
    with pytest.raises(ConfigTableError, match="不存在"):
        pt._validate_talent_tree(talents)


def test_talent_tree_rejects_unreachable_prerequisite_level() -> None:
    talents = {
        1: _talent(id=1, max_level=2),
        2: _talent(id=2, require_talent_id=1, require_talent_level=5),
    }
    with pytest.raises(ConfigTableError, match="永远点不出来"):
        pt._validate_talent_tree(talents)


def test_talent_tree_rejects_cycle() -> None:
    """成环的表现是玩家怎么点都点不出来(前置永远不达标),不报错。"""
    talents = {
        1: _talent(id=1, require_talent_id=2, require_talent_level=1),
        2: _talent(id=2, require_talent_id=1, require_talent_level=1),
    }
    with pytest.raises(ConfigTableError, match="成环"):
        pt._validate_talent_tree(talents)


def _tables_with_talents(talents: dict) -> pt.Tables:
    return pt.Tables(
        version=1,
        source_rev="",
        levels={},
        items={},
        talents=talents,
        talent_effects=[],
        skill_cards={},
        card_upgrade={},
    )


def test_validate_allocation_costs_use_cost_per_level() -> None:
    """逐节点消耗 = 等级 × cost_per_level。

    ★ 这正是必须落库的原因:读取侧按 Σ 等级 反推会在 cost_per_level≠1 时算少已花点数,
      玩家看到的可点数比实际多(写扣 6 读算 2)。
    """
    tables = _tables_with_talents({1: _talent(id=1, max_level=5, cost_per_level=3)})
    costs, total = tables.validate_talent_allocation({1: 2})
    assert costs == {1: 6}
    assert total == 6


def test_validate_allocation_prerequisite_only_looks_at_this_plan() -> None:
    """全量替换语义:前置只看**本次方案**,不看库里旧数据。

    否则"先点满前置、再单独洗掉前置"就能留下悬空节点。
    """
    tables = _tables_with_talents(
        {
            1: _talent(id=1, max_level=3),
            2: _talent(id=2, max_level=3, require_talent_id=1, require_talent_level=2),
        }
    )
    with pytest.raises(ConfigTableError, match="需要前置专精"):
        tables.validate_talent_allocation({2: 1})
    tables.validate_talent_allocation({1: 2, 2: 1})


def test_validate_allocation_rejects_unknown_and_overlevel() -> None:
    tables = _tables_with_talents({1: _talent(id=1, max_level=3)})
    with pytest.raises(ConfigTableError, match="不在配置表中"):
        tables.validate_talent_allocation({99: 1})
    with pytest.raises(ConfigTableError, match="超过上限"):
        tables.validate_talent_allocation({1: 4})
    with pytest.raises(ConfigTableError, match="等级为 0"):
        tables.validate_talent_allocation({1: 0})


# ── 专精效果 ─────────────────────────────────────────────────────────────────


def test_talent_effect_attr_key_whitelist() -> None:
    """attr_key 写错在 DS 上的表现是"这个天赋点了完全没反应",既不报错也不崩。"""
    with pytest.raises(ConfigTableError, match="不是 UE GAS 属性名"):
        pt._validate_talent_effect_row(
            teff_pb.TalentEffectRow(id=1, talent_id=1, attr_key="Atack", value_per_level=1)
        )
    pt._validate_talent_effect_row(
        teff_pb.TalentEffectRow(id=1, talent_id=1, attr_key="Atk", value_per_level=1)
    )


def test_talent_effect_value_bounds() -> None:
    with pytest.raises(ConfigTableError, match="没有任何作用"):
        pt._validate_talent_effect_row(
            teff_pb.TalentEffectRow(id=1, talent_id=1, attr_key="Atk", value_per_level=0)
        )
    with pytest.raises(ConfigTableError, match="疑似多打了零"):
        pt._validate_talent_effect_row(
            teff_pb.TalentEffectRow(
                id=1,
                talent_id=1,
                attr_key="Atk",
                value_per_level=pt.MAX_TALENT_EFFECT_VALUE_PER_LEVEL + 1,
            )
        )


def test_talent_effect_duplicate_rows_double_the_bonus() -> None:
    rows = [
        teff_pb.TalentEffectRow(id=1, talent_id=8, attr_key="Atk", value_per_level=5),
        teff_pb.TalentEffectRow(id=2, talent_id=8, attr_key="Atk", value_per_level=5),
    ]
    with pytest.raises(ConfigTableError, match="加成会翻倍"):
        pt._validate_talent_effects(rows)


# ── 技能卡 ───────────────────────────────────────────────────────────────────


def _card(**kw) -> card_pb.SkillCardRow:
    base = {"id": 1, "name": "火球", "skill_id": 1, "rarity": 1, "max_level": 3}
    base.update(kw)
    return card_pb.SkillCardRow(**base)


def _upgrade(rarity: int, level: int, cost: int, row_id: int = 1) -> card_pb.SkillCardUpgradeRow:
    return card_pb.SkillCardUpgradeRow(id=row_id, rarity=rarity, level=level, shard_cost=cost)


def test_card_row_gates() -> None:
    with pytest.raises(ConfigTableError, match="稀有度"):
        pt._validate_skill_card_row(_card(rarity=9))
    with pytest.raises(ConfigTableError, match="上限至少为 1"):
        pt._validate_skill_card_row(_card(max_level=0))
    with pytest.raises(ConfigTableError, match="疑似手滑"):
        pt._validate_skill_card_row(_card(max_level=pt.MAX_SKILL_CARD_LEVEL + 1))


def test_upgrade_row_gates() -> None:
    with pytest.raises(ConfigTableError, match="不存在升到 1 级"):
        pt._validate_skill_card_upgrade_row(_upgrade(1, 1, 5))
    with pytest.raises(ConfigTableError, match="等于免费升级"):
        pt._validate_skill_card_upgrade_row(_upgrade(1, 2, 0))


def test_upgrade_curve_gap_is_rejected() -> None:
    """缺档表现为"卡升到某级之后按钮没反应",不报错 —— 最难查的一类配置事故。"""
    cards = {1: _card(max_level=4)}
    rows = [_upgrade(1, 2, 5, 1), _upgrade(1, 4, 20, 2)]  # 缺 3 级
    with pytest.raises(ConfigTableError, match="缺 3 级"):
        pt._validate_card_curves(cards, rows)


def test_upgrade_curve_duplicate_is_rejected() -> None:
    cards = {1: _card(max_level=2)}
    rows = [_upgrade(1, 2, 5, 1), _upgrade(1, 2, 7, 2)]
    with pytest.raises(ConfigTableError, match="取决于表内顺序"):
        pt._validate_card_curves(cards, rows)


def test_upgrade_curve_must_be_monotonic() -> None:
    cards = {1: _card(max_level=3)}
    rows = [_upgrade(1, 2, 10, 1), _upgrade(1, 3, 5, 2)]
    with pytest.raises(ConfigTableError, match="下降"):
        pt._validate_card_curves(cards, rows)


def test_upgrade_curve_only_requires_rarities_in_use() -> None:
    """反过来要求"曲线表里每个稀有度都得有卡"是错的(策划可以先铺曲线再加卡)。"""
    cards = {1: _card(rarity=1, max_level=2)}
    rows = [_upgrade(1, 2, 5, 1), _upgrade(4, 2, 50, 2)]  # 传说曲线暂时无卡在用
    index = pt._validate_card_curves(cards, rows)
    assert index[(1, 2)] == 5


def test_upgrade_curve_lookup_fails_closed_on_missing_level() -> None:
    """热更换表后曲线与上限对不上时**抛错而不是当免费升级**。"""
    tables = pt.Tables(
        version=1,
        source_rev="",
        levels={},
        items={},
        talents={},
        talent_effects=[],
        skill_cards={1: _card(max_level=3)},
        card_upgrade={(1, 2): 5},  # 缺 3 级
    )
    with pytest.raises(ConfigTableError, match="upgrade curve missing"):
        tables.upgrade_curve(1)
    with pytest.raises(ConfigTableError, match="unknown skill card"):
        tables.upgrade_curve(999)


# ── 道具 / 装备部位 ──────────────────────────────────────────────────────────


def test_matches_slot_fails_closed() -> None:
    from pandora.config.v1 import item_pb2

    tables = pt.Tables(
        version=1,
        source_rev="",
        levels={},
        items={
            10: item_pb2.ItemRow(id=10, name="剑", equip_slot=1),
            20: item_pb2.ItemRow(id=20, name="药", equip_slot=0),
        },
        talents={},
        talent_effects=[],
        skill_cards={},
        card_upgrade={},
    )
    assert tables.matches_slot(10, 1) is True
    assert tables.matches_slot(10, 2) is False  # 部位不符
    assert tables.matches_slot(20, 1) is False  # 不是装备
    assert tables.matches_slot(999, 1) is False  # 未知道具 fail-closed
    assert tables.matches_slot(10, 0) is False  # slot 0 = 不可穿戴
