"""player 业务层 —— 重点全在"写错了不报错"的分支上。

  1. ★ fail-closed:配置表 / inventory 校验器缺失时,写路径必须**拒绝**而不是放行
  2. ★ GetLoadout 的战斗快照复核(陈旧预设 / 词条越界 / 滚动升级期详情缺失)
  3. ★ 幂等与乐观锁:领奖冲突重试、MMR 幂等命中
  4. ★ 保留期 janitor 的 report_only **不循环**(否则每轮固定跑满的空转)
  5. ★ 发布器领导权逐轮判定,失主立刻停发(不"补完"在飞的行)
"""

from __future__ import annotations

import asyncio
import datetime as _dt

import pytest

from pandora.player.v1 import player_pb2 as ppb

from pandorapy import dbguard, errcode
from pandorapy.services.player import biz as pbiz
from pandorapy.services.player import conf as pconf
from pandorapy.services.player import models as m
from pandorapy.services.player import tables as pt


def _cfg(**overrides) -> pconf.PlayerConf:
    cfg = pconf.Config.model_validate({"player": overrides})
    cfg.apply_defaults()
    return cfg.player


def _tables(**overrides) -> pt.Tables:
    base = {
        "version": 1,
        "source_rev": "",
        "levels": {},
        "items": {},
        "talents": {},
        "talent_effects": [],
        "skill_cards": {},
        "card_upgrade": {},
    }
    base.update(overrides)
    return pt.Tables(**base)


def _store(tables: pt.Tables) -> pt.Store:
    return pt.Store(tables, "/dev/null")


class FakeRepo:
    """只实现被测路径用到的方法;没实现的被调用会直接 AttributeError(比静默返回空好)。"""

    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.profile: ppb.PlayerProfile | None = None
        self.ratings: list[m.PlayerRating] = []
        self.nicknames: dict[int, str] = {}
        self.equipment: list[m.EquipmentSlot] = []
        self.talents: tuple[list[m.TalentLevel], int] = ([], 0)
        self.attrs: tuple[list[m.AttrPoint], int] = ([], 0)
        self.active_hero = 0
        self.skill_cards: list[m.SkillCard] = []
        self.skill_slots: list[m.SkillSlot] = []
        self.reward_raw = b""
        self.reward_version = 0
        self.save_failures: list[BaseException | None] = []
        self.mmr_result = (1500, False)
        self.exp_result = (m.ExpState(level=2, exp_in_level=5, levels_gained=1), False)
        self.outbox: list[m.PushOutboxRecord] = []
        self.deleted_outbox: list[int] = []
        self.sweep_outcomes: list[dbguard.Outcome] = []

    async def ensure_profile(self, player_id, nickname, base_mmr):  # noqa: ANN001
        self.calls.append(("ensure_profile", player_id, nickname, base_mmr))
        return True

    async def get_profile(self, player_id):  # noqa: ANN001
        return self.profile

    async def list_ratings(self, player_id):  # noqa: ANN001
        return self.ratings

    async def list_nicknames(self, ids):  # noqa: ANN001
        self.calls.append(("list_nicknames", tuple(ids)))
        return {i: self.nicknames[i] for i in ids if i in self.nicknames}

    async def get_equipment(self, player_id):  # noqa: ANN001
        return self.equipment

    async def set_equipment(self, player_id, slots):  # noqa: ANN001
        self.calls.append(("set_equipment", player_id, tuple(slots)))

    async def get_talents(self, player_id):  # noqa: ANN001
        return self.talents

    async def set_talents(self, player_id, talents):  # noqa: ANN001
        self.calls.append(("set_talents", player_id, tuple(talents)))
        return 0

    async def get_attributes(self, player_id):  # noqa: ANN001
        return self.attrs

    async def get_active_hero(self, player_id):  # noqa: ANN001
        return self.active_hero

    async def get_skill_cards(self, player_id):  # noqa: ANN001
        return self.skill_cards

    async def get_skill_slots(self, player_id):  # noqa: ANN001
        return self.skill_slots

    async def set_skill_slots(self, player_id, slots):  # noqa: ANN001
        self.calls.append(("set_skill_slots", player_id, tuple(slots)))

    async def apply_mmr_change(self, change):  # noqa: ANN001
        self.calls.append(("apply_mmr_change", change))
        return self.mmr_result

    async def apply_experience(self, apply):  # noqa: ANN001
        self.calls.append(("apply_experience", apply))
        return self.exp_result

    async def load_reward_claims(self, player_id):  # noqa: ANN001
        return self.reward_raw, self.reward_version

    async def save_reward_claims(self, player_id, record, expect_version):  # noqa: ANN001
        self.calls.append(("save_reward_claims", player_id, expect_version))
        if self.save_failures:
            failure = self.save_failures.pop(0)
            if failure is not None:
                raise failure
        self.reward_raw = record
        self.reward_version = expect_version + 1

    async def fetch_push_outbox(self, limit):  # noqa: ANN001
        batch = self.outbox[:limit]
        self.outbox = self.outbox[limit:]
        return batch

    async def delete_push_outbox(self, record_id):  # noqa: ANN001
        self.deleted_outbox.append(record_id)

    # 五个 sweep 入口:登记清单测试要按名字取到它们,漏一个就是"既不删也不报"。
    async def sweep_exp_history(self, mode, cutoff, limit):  # noqa: ANN001
        return dbguard.Outcome(mode=mode, matched=0, deleted=0)

    async def sweep_mmr_history(self, mode, cutoff, limit):  # noqa: ANN001
        return dbguard.Outcome(mode=mode, matched=0, deleted=0)

    async def sweep_attr_point_grants(self, mode, cutoff, limit):  # noqa: ANN001
        return dbguard.Outcome(mode=mode, matched=0, deleted=0)

    async def sweep_talent_point_grants(self, mode, cutoff, limit):  # noqa: ANN001
        return dbguard.Outcome(mode=mode, matched=0, deleted=0)

    async def sweep_skill_card_grants(self, mode, cutoff, limit):  # noqa: ANN001
        return dbguard.Outcome(mode=mode, matched=0, deleted=0)


class FakeOwnership:
    def __init__(self, result: m.InstanceOwnershipResult | BaseException) -> None:
        self._result = result
        self.calls = 0

    async def check_instances_owned(self, player_id, equipment):  # noqa: ANN001
        self.calls += 1
        if isinstance(self._result, BaseException):
            raise self._result
        return self._result


# ── 档案 ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_ensure_profile_named_detects_silent_nickname_conflict() -> None:
    """INSERT IGNORE 下昵称撞 uk **不报错**,只是没插进去 —— 必须靠回读区分。

    谎报成功会让 login 以为播种完成、再也不重试,该玩家永远没有档案。
    """
    repo = FakeRepo()
    repo.profile = None  # 回读拿不到 = 建档确实没成
    uc = pbiz.PlayerUsecase(repo, _cfg())
    with pytest.raises(errcode.PandoraError) as excinfo:
        await uc.ensure_profile_named(7, "被占用的名字")
    assert excinfo.value.code == errcode.ErrPlayerNicknameTaken


@pytest.mark.asyncio
async def test_ensure_profile_named_empty_nickname_uses_prefix() -> None:
    repo = FakeRepo()
    repo.profile = ppb.PlayerProfile(player_id=7, nickname="Player_7", level=1)
    uc = pbiz.PlayerUsecase(repo, _cfg())
    res = await uc.ensure_profile_named(7, "   ")
    assert res.created is True
    assert res.nickname == "Player_7"
    assert ("ensure_profile", 7, "Player_7", 1500) in repo.calls


@pytest.mark.asyncio
async def test_get_player_names_dedupes_drops_zero_and_truncates() -> None:
    """去重 + 去 0 + 截断,且按**请求顺序**回填(调用方可直接顺序消费)。

    刻意不做隐式分页:服务端悄悄少返回会让调用方以为"这些人就是没有档案"。
    """
    repo = FakeRepo()
    repo.nicknames = {i: f"n{i}" for i in range(1, 500)}
    uc = pbiz.PlayerUsecase(repo, _cfg())

    names = await uc.get_player_names([3, 0, 3, 1, 2])
    assert [n.player_id for n in names] == [3, 1, 2]

    big = await uc.get_player_names(list(range(1, 400)))
    assert len(big) == pbiz.MAX_PLAYER_NAMES_PER_QUERY
    queried = [c for c in repo.calls if c[0] == "list_nicknames"][-1][1]
    assert len(queried) == pbiz.MAX_PLAYER_NAMES_PER_QUERY


@pytest.mark.asyncio
async def test_get_player_names_omits_missing_rows() -> None:
    """请求里有、响应里没有 = 该角色无档案。用空串占位会让 DS 把真名字覆盖成空。"""
    repo = FakeRepo()
    repo.nicknames = {1: "有档案"}
    uc = pbiz.PlayerUsecase(repo, _cfg())
    names = await uc.get_player_names([1, 2])
    assert [(n.player_id, n.nickname) for n in names] == [(1, "有档案")]


@pytest.mark.asyncio
async def test_get_profile_overwrites_deprecated_mmr_with_default_pool() -> None:
    """同一响应内若已有 default rating,就以它覆盖 deprecated mmr #4。

    两次仓储读取之间可能恰逢结算提交,不覆盖会让新旧客户端看到两份瞬时值。
    """
    repo = FakeRepo()
    repo.profile = ppb.PlayerProfile(player_id=1, nickname="a", level=1, mmr=1500)
    repo.ratings = [
        m.PlayerRating(rating_pool="3v3_ranked", mmr=1700),
        m.PlayerRating(rating_pool="default", mmr=1620),
    ]
    uc = pbiz.PlayerUsecase(repo, _cfg())
    profile = await uc.get_profile(1)
    assert profile.mmr == 1620
    assert [(r.rating_pool, r.mmr) for r in profile.ratings] == [
        ("3v3_ranked", 1700),
        ("default", 1620),
    ]


# ── 经验 ─────────────────────────────────────────────────────────────────────


def _exp_store() -> pt.Store:
    return _store(
        _tables(
            levels={
                1: type("R", (), {"level": 1, "upgrade_exp": 100, "cumulative_exp": 0})(),
                2: type("R", (), {"level": 2, "upgrade_exp": 200, "cumulative_exp": 100})(),
                3: type("R", (), {"level": 3, "upgrade_exp": 0, "cumulative_exp": 300})(),
            }
        )
    )


def test_decorate_experience_respects_switch_and_curve() -> None:
    repo = FakeRepo()
    uc = pbiz.PlayerUsecase(repo, _cfg(experience_enabled=False))
    uc.set_config_tables(_exp_store())
    # 功能关闭 → 不标满级,exp 原样(行为与历史一致)。
    assert uc.decorate_experience(3, 42) == (42, False)

    uc2 = pbiz.PlayerUsecase(repo, _cfg(experience_enabled=True))
    uc2.set_config_tables(_exp_store())
    assert uc2.decorate_experience(3, 42) == (0, True)  # 满级夹紧
    assert uc2.decorate_experience(2, 42) == (42, False)

    # 曲线未加载 → 同样不标满级(而不是把所有人都当满级)。
    uc3 = pbiz.PlayerUsecase(repo, _cfg(experience_enabled=True))
    assert uc3.decorate_experience(3, 42) == (42, False)


@pytest.mark.asyncio
async def test_add_experience_gate_order_matches_go() -> None:
    """参数校验在功能开关**之前** —— 否则"功能关了"会掩盖"调用方传了非法参数"。"""
    repo = FakeRepo()
    uc = pbiz.PlayerUsecase(repo, _cfg(experience_enabled=False))
    uc.set_config_tables(_exp_store())

    with pytest.raises(errcode.PandoraError) as e1:
        await uc.add_experience(1, 0, "battle", "k")
    assert e1.value.code == errcode.ErrInvalidArg  # delta=0 先被拒

    with pytest.raises(errcode.PandoraError) as e2:
        await uc.add_experience(1, 10, "battle", "k")
    assert e2.value.code == errcode.ErrPlayerFeatureDisabled


@pytest.mark.asyncio
async def test_add_experience_rejects_over_max_grant() -> None:
    """防异常 / 越权调用方一次灌满等级(player 侧最后一道兜底)。"""
    repo = FakeRepo()
    uc = pbiz.PlayerUsecase(repo, _cfg(experience_enabled=True, max_exp_per_grant=100))
    uc.set_config_tables(_exp_store())
    with pytest.raises(errcode.PandoraError) as excinfo:
        await uc.add_experience(1, 101, "battle", "k")
    assert excinfo.value.code == errcode.ErrInvalidArg


@pytest.mark.asyncio
async def test_add_experience_without_curve_is_feature_disabled() -> None:
    repo = FakeRepo()
    uc = pbiz.PlayerUsecase(repo, _cfg(experience_enabled=True))  # 未 set_config_tables
    with pytest.raises(errcode.PandoraError) as excinfo:
        await uc.add_experience(1, 10, "battle", "k")
    assert excinfo.value.code == errcode.ErrPlayerFeatureDisabled


# ── 装备预设 fail-closed ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_set_equipment_requires_feature_switch() -> None:
    repo = FakeRepo()
    uc = pbiz.PlayerUsecase(repo, _cfg(loadout_customize_enabled=False))
    with pytest.raises(errcode.PandoraError) as excinfo:
        await uc.set_equipment(1, [m.EquipmentSlot(slot=1, item_config_id=10, instance_id=99)])
    assert excinfo.value.code == errcode.ErrPlayerFeatureDisabled


@pytest.mark.asyncio
async def test_set_equipment_fails_closed_without_item_table() -> None:
    """表未加载 → 拒掉这次改装,**不放行一份没校验过的预设**。"""
    repo = FakeRepo()
    uc = pbiz.PlayerUsecase(repo, _cfg(loadout_customize_enabled=True))
    with pytest.raises(errcode.PandoraError) as excinfo:
        await uc.set_equipment(1, [m.EquipmentSlot(slot=1, item_config_id=10, instance_id=99)])
    assert excinfo.value.code == errcode.ErrInternal
    assert not any(c[0] == "set_equipment" for c in repo.calls)


@pytest.mark.asyncio
async def test_set_equipment_fails_closed_without_ownership_checker() -> None:
    """inventory 未接线 → fail-closed,而不是退化成"不校验就放行"。"""
    from pandora.config.v1 import item_pb2

    repo = FakeRepo()
    uc = pbiz.PlayerUsecase(repo, _cfg(loadout_customize_enabled=True))
    uc.set_config_tables(_store(_tables(items={10: item_pb2.ItemRow(id=10, equip_slot=1)})))
    with pytest.raises(errcode.PandoraError) as excinfo:
        await uc.set_equipment(1, [m.EquipmentSlot(slot=1, item_config_id=10, instance_id=99)])
    assert excinfo.value.code == errcode.ErrInternal


@pytest.mark.asyncio
async def test_set_equipment_rejects_slot_zero_and_duplicates() -> None:
    repo = FakeRepo()
    uc = pbiz.PlayerUsecase(repo, _cfg(loadout_customize_enabled=True))
    # slot 0 = 道具表约定的"不可穿戴",预设里出现它只会变成一条恒失败的记录。
    with pytest.raises(errcode.PandoraError, match="slot must be positive"):
        await uc.set_equipment(1, [m.EquipmentSlot(slot=0, item_config_id=10, instance_id=1)])
    with pytest.raises(errcode.PandoraError, match="duplicate slot"):
        await uc.set_equipment(
            1,
            [
                m.EquipmentSlot(slot=1, item_config_id=10, instance_id=1),
                m.EquipmentSlot(slot=1, item_config_id=11, instance_id=2),
            ],
        )
    with pytest.raises(errcode.PandoraError, match="duplicate instance_id"):
        await uc.set_equipment(
            1,
            [
                m.EquipmentSlot(slot=1, item_config_id=10, instance_id=1),
                m.EquipmentSlot(slot=2, item_config_id=11, instance_id=1),
            ],
        )


@pytest.mark.asyncio
async def test_ownership_query_failure_is_not_treated_as_not_owned() -> None:
    """★ 查询失败 ≠ 一件都没有:必须把错误原样抛出让客户端重试(§9.22)。"""
    from pandora.config.v1 import item_pb2

    repo = FakeRepo()
    uc = pbiz.PlayerUsecase(repo, _cfg(loadout_customize_enabled=True))
    uc.set_config_tables(_store(_tables(items={10: item_pb2.ItemRow(id=10, equip_slot=1)})))
    boom = errcode.PandoraError(errcode.ErrUnavailable, "inventory down")
    uc.set_instance_ownership_checker(FakeOwnership(boom))
    with pytest.raises(errcode.PandoraError) as excinfo:
        await uc.set_equipment(1, [m.EquipmentSlot(slot=1, item_config_id=10, instance_id=99)])
    assert excinfo.value.code == errcode.ErrUnavailable


@pytest.mark.asyncio
async def test_ownership_missing_instance_is_permission_deny() -> None:
    from pandora.config.v1 import item_pb2

    repo = FakeRepo()
    uc = pbiz.PlayerUsecase(repo, _cfg(loadout_customize_enabled=True))
    uc.set_config_tables(_store(_tables(items={10: item_pb2.ItemRow(id=10, equip_slot=1)})))
    uc.set_instance_ownership_checker(FakeOwnership(m.InstanceOwnershipResult()))
    with pytest.raises(errcode.PandoraError) as excinfo:
        await uc.set_equipment(1, [m.EquipmentSlot(slot=1, item_config_id=10, instance_id=99)])
    assert excinfo.value.code == errcode.ErrPermissionDeny


@pytest.mark.asyncio
async def test_ownership_unexpected_instance_is_internal() -> None:
    """inventory 回了没问过的实例 = 契约被破坏,不能当"多给了就多给了"。"""
    from pandora.config.v1 import item_pb2

    repo = FakeRepo()
    uc = pbiz.PlayerUsecase(repo, _cfg(loadout_customize_enabled=True))
    uc.set_config_tables(_store(_tables(items={10: item_pb2.ItemRow(id=10, equip_slot=1)})))
    uc.set_instance_ownership_checker(
        FakeOwnership(m.InstanceOwnershipResult(owned_instance_ids=(4242,)))
    )
    with pytest.raises(errcode.PandoraError) as excinfo:
        await uc.set_equipment(1, [m.EquipmentSlot(slot=1, item_config_id=10, instance_id=99)])
    assert excinfo.value.code == errcode.ErrInternal


# ── 战斗快照复核 ─────────────────────────────────────────────────────────────


def _loadout_uc(repo: FakeRepo, owned: m.InstanceOwnershipResult) -> pbiz.PlayerUsecase:
    from pandora.config.v1 import item_pb2

    uc = pbiz.PlayerUsecase(repo, _cfg(loadout_customize_enabled=True))
    uc.set_config_tables(_store(_tables(items={10: item_pb2.ItemRow(id=10, equip_slot=1)})))
    uc.set_instance_ownership_checker(FakeOwnership(owned))
    return uc


@pytest.mark.asyncio
async def test_loadout_rejects_legacy_preset_without_instance_id() -> None:
    """000006 前只按 item_config_id 存的旧行无法证明是哪一件实例 → 战斗快照 fail-closed。"""
    repo = FakeRepo()
    repo.equipment = [m.EquipmentSlot(slot=1, item_config_id=10, instance_id=0)]
    uc = _loadout_uc(repo, m.InstanceOwnershipResult())
    with pytest.raises(errcode.PandoraError) as excinfo:
        await uc.get_loadout(1)
    assert excinfo.value.code == errcode.ErrInvalidState


@pytest.mark.asyncio
async def test_loadout_requires_instance_details_during_rollout() -> None:
    """滚动升级期旧 inventory 只回 ID 子集 —— 那不足以生成含词条的战斗快照。"""
    repo = FakeRepo()
    repo.equipment = [m.EquipmentSlot(slot=1, item_config_id=10, instance_id=99)]
    uc = _loadout_uc(repo, m.InstanceOwnershipResult(owned_instance_ids=(99,)))
    with pytest.raises(errcode.PandoraError, match="rollout incomplete"):
        await uc.get_loadout(1)


@pytest.mark.asyncio
async def test_loadout_rejects_unsupported_or_out_of_range_attr() -> None:
    """装备词条信任边界:未知 attr / 非正数 / 超限一律 fail-closed。

    不设这道闸的话,脏库或旧副本能把不受控的增益直接注入战斗服。
    """
    repo = FakeRepo()
    repo.equipment = [m.EquipmentSlot(slot=1, item_config_id=10, instance_id=99)]

    bad_attr = m.InstanceOwnershipResult(
        owned_instance_ids=(99,),
        owned_instances=(
            m.OwnedEquipmentInstance(
                instance_id=99,
                item_config_id=10,
                identified=True,
                attributes=(m.EquipmentAttributeSnapshot(attr_id=1234, value=5),),
            ),
        ),
    )
    with pytest.raises(errcode.PandoraError, match="unsupported attr"):
        await _loadout_uc(repo, bad_attr).get_loadout(1)

    over_range = m.InstanceOwnershipResult(
        owned_instance_ids=(99,),
        owned_instances=(
            m.OwnedEquipmentInstance(
                instance_id=99,
                item_config_id=10,
                identified=True,
                attributes=(
                    m.EquipmentAttributeSnapshot(
                        attr_id=pbiz.EQUIPMENT_ATTR_MOVE_SPEED_RATE_ID,
                        value=pbiz.MAX_EQUIPMENT_RATE_BASIS_POINTS + 1,
                    ),
                ),
            ),
        ),
    )
    with pytest.raises(errcode.PandoraError, match="out of range"):
        await _loadout_uc(repo, over_range).get_loadout(1)


@pytest.mark.asyncio
async def test_loadout_happy_path_carries_card_level() -> None:
    """卡等级随槽位一起带出 —— 让 DS 再查一次持有表等于把一次读拆成两次(中间可能被改)。"""
    repo = FakeRepo()
    repo.equipment = [m.EquipmentSlot(slot=1, item_config_id=10, instance_id=99)]
    repo.skill_cards = [m.SkillCard(card_id=5, level=3, shards=2)]
    repo.skill_slots = [m.SkillSlot(slot=0, card_id=5)]
    owned = m.InstanceOwnershipResult(
        owned_instance_ids=(99,),
        owned_instances=(
            m.OwnedEquipmentInstance(
                instance_id=99,
                item_config_id=10,
                identified=True,
                attributes=(
                    m.EquipmentAttributeSnapshot(
                        attr_id=pbiz.EQUIPMENT_ATTR_ATTACK_ID, value=12
                    ),
                ),
            ),
        ),
    )
    loadout = await _loadout_uc(repo, owned).get_loadout(1)
    assert loadout.equipment[0].identified is True
    assert loadout.equipment[0].attributes[0].value == 12
    assert (loadout.skill_cards[0].slot, loadout.skill_cards[0].level) == (0, 3)


# ── 天赋 ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_set_talents_prices_each_node_from_table() -> None:
    """逐节点消耗必须由配置表算好后落库(读取侧不再按 Σ 等级 反推)。"""
    from pandora.config.v1 import talent_pb2

    repo = FakeRepo()
    uc = pbiz.PlayerUsecase(repo, _cfg(loadout_customize_enabled=True))
    uc.set_config_tables(
        _store(
            _tables(
                talents={
                    1: talent_pb2.TalentRow(id=1, name="力", max_level=5, cost_per_level=3)
                }
            )
        )
    )
    await uc.set_talents(1, [m.TalentLevel(talent_id=1, level=2)])
    priced = [c for c in repo.calls if c[0] == "set_talents"][-1][2]
    assert priced[0].spent_points == 6


@pytest.mark.asyncio
async def test_set_talents_table_error_is_invalid_arg_not_internal() -> None:
    """表判定非法 → ErrInvalidArg(玩家的锅);表**未加载** → ErrInternal(我们的锅)。"""
    from pandora.config.v1 import talent_pb2

    repo = FakeRepo()
    uc = pbiz.PlayerUsecase(repo, _cfg(loadout_customize_enabled=True))
    uc.set_config_tables(
        _store(_tables(talents={1: talent_pb2.TalentRow(id=1, name="力", max_level=1, cost_per_level=1)}))
    )
    with pytest.raises(errcode.PandoraError) as bad:
        await uc.set_talents(1, [m.TalentLevel(talent_id=1, level=9)])
    assert bad.value.code == errcode.ErrInvalidArg

    uc2 = pbiz.PlayerUsecase(repo, _cfg(loadout_customize_enabled=True))
    with pytest.raises(errcode.PandoraError) as missing:
        await uc2.set_talents(1, [m.TalentLevel(talent_id=1, level=1)])
    assert missing.value.code == errcode.ErrInternal


# ── 技能卡槽 ─────────────────────────────────────────────────────────────────


def _card_uc(repo: FakeRepo) -> pbiz.PlayerUsecase:
    from pandora.config.v1 import skill_card_pb2

    uc = pbiz.PlayerUsecase(repo, _cfg(loadout_customize_enabled=True))
    uc.set_config_tables(
        _store(
            _tables(
                skill_cards={
                    1: skill_card_pb2.SkillCardRow(id=1, name="a", rarity=1, max_level=3),
                    2: skill_card_pb2.SkillCardRow(id=2, name="b", rarity=1, max_level=3),
                },
                card_upgrade={(1, 2): 5, (1, 3): 10},
            )
        )
    )
    return uc


@pytest.mark.asyncio
async def test_set_skill_slots_clear_and_validation() -> None:
    repo = FakeRepo()
    uc = _card_uc(repo)

    # card_id=0 = 显式清空该槽,不落行。
    applied = await uc.set_skill_slots(
        1, [m.SkillSlot(slot=0, card_id=1), m.SkillSlot(slot=1, card_id=0)]
    )
    assert [(s.slot, s.card_id) for s in applied] == [(0, 1)]

    with pytest.raises(errcode.PandoraError) as out_of_range:
        await uc.set_skill_slots(1, [m.SkillSlot(slot=pbiz.SKILL_SLOT_COUNT, card_id=1)])
    assert out_of_range.value.code == errcode.ErrSkillCardSlotInvalid

    with pytest.raises(errcode.PandoraError, match="more than one slot"):
        await uc.set_skill_slots(
            1, [m.SkillSlot(slot=0, card_id=1), m.SkillSlot(slot=1, card_id=1)]
        )

    with pytest.raises(errcode.PandoraError, match="unknown skill card"):
        await uc.set_skill_slots(1, [m.SkillSlot(slot=0, card_id=999)])


@pytest.mark.asyncio
async def test_grant_skill_cards_rejects_unknown_and_duplicate() -> None:
    """发一张表里没有的卡 = 玩家背包里出现一张永远打不开、升不了、装不上的幽灵卡。"""
    repo = FakeRepo()
    uc = _card_uc(repo)
    with pytest.raises(errcode.PandoraError, match="unknown skill card"):
        await uc.grant_skill_cards(1, [m.SkillCardGrant(card_id=999, shards=1)], "k")
    with pytest.raises(errcode.PandoraError, match="duplicate card_id"):
        await uc.grant_skill_cards(
            1,
            [m.SkillCardGrant(card_id=1, shards=1), m.SkillCardGrant(card_id=1, shards=2)],
            "k",
        )


# ── MMR ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("reason", "expect"),
    [("win", (True, True)), ("lose", (True, False)), ("draw", (True, False)),
     ("abandon", (False, False)), ("rollback", (False, False)), ("", (False, False))],
)
def test_battle_flags(reason: str, expect: tuple[bool, bool]) -> None:
    assert pbiz._battle_flags(reason) == expect


@pytest.mark.asyncio
async def test_update_mmr_normalizes_pool_on_write_side() -> None:
    """归一化在**写入侧**做一次,与读侧同一函数 —— 杜绝写 default / 读 "" 的分裂。"""
    repo = FakeRepo()
    uc = pbiz.PlayerUsecase(repo, _cfg())
    await uc.update_mmr(1, 15, "win", "match-1", "   ")
    change = [c for c in repo.calls if c[0] == "apply_mmr_change"][-1][1]
    assert change.rating_pool == "default"
    assert (change.inc_battle, change.inc_win) == (True, True)
    assert change.baseline == 1500


@pytest.mark.asyncio
async def test_get_mmr_reports_baseline_with_found_false() -> None:
    class NoRowRepo(FakeRepo):
        async def get_mmr(self, player_id, pool):  # noqa: ANN001
            return 0, False

    uc = pbiz.PlayerUsecase(NoRowRepo(), _cfg())
    assert await uc.get_mmr(1, "3v3_ranked") == (1500, False)


# ── 领奖 ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_claim_reward_retries_on_version_conflict() -> None:
    repo = FakeRepo()
    repo.save_failures = [
        errcode.PandoraError(errcode.ErrPlayerVersionMismatch, "conflict"),
        None,
    ]
    uc = pbiz.PlayerUsecase(repo, _cfg())
    await uc.claim_reward(1, ppb.REWARD_SOURCE_TYPE_PERMANENT, "sign_in", 0, 3)
    saves = [c for c in repo.calls if c[0] == "save_reward_claims"]
    assert len(saves) == 2


@pytest.mark.asyncio
async def test_claim_reward_exhausts_retries() -> None:
    """★ 重试耗尽必须报出来(WARN + 明确码),否则竞争风暴与偶发冲突无法区分。"""
    repo = FakeRepo()
    repo.save_failures = [
        errcode.PandoraError(errcode.ErrPlayerVersionMismatch, "conflict")
    ] * pbiz.MAX_REWARD_CLAIM_RETRY
    uc = pbiz.PlayerUsecase(repo, _cfg())
    with pytest.raises(errcode.PandoraError) as excinfo:
        await uc.claim_reward(1, ppb.REWARD_SOURCE_TYPE_PERMANENT, "sign_in", 0, 3)
    assert excinfo.value.code == errcode.ErrPlayerVersionMismatch


@pytest.mark.asyncio
async def test_claim_reward_is_idempotent_within_stored_record() -> None:
    repo = FakeRepo()
    uc = pbiz.PlayerUsecase(repo, _cfg())
    await uc.claim_reward(1, ppb.REWARD_SOURCE_TYPE_PERMANENT, "sign_in", 0, 3)
    with pytest.raises(errcode.PandoraError) as excinfo:
        await uc.claim_reward(1, ppb.REWARD_SOURCE_TYPE_PERMANENT, "sign_in", 0, 3)
    assert excinfo.value.code == errcode.ErrRewardAlreadyClaimed
    assert await uc.get_reward_claims(1, ppb.REWARD_SOURCE_TYPE_PERMANENT, "sign_in", 0) == [3]


@pytest.mark.asyncio
async def test_claim_reward_preserves_unknown_fields() -> None:
    """★ read-modify-write 必须原样带回 unknown fields(zero-downtime §2.3)。

    金丝雀期新副本可能写了本副本不认识的字段;重建 message 会静默清掉它们。
    """
    repo = FakeRepo()
    stored = ppb.RewardClaimStorageRecord()
    stored.permanent["sign_in"] = b"\x01"
    raw = bytearray(stored.SerializeToString())
    # 手工追加一个未知字段(field 15, varint) —— 模拟新副本写下的新字段。
    raw += bytes([15 << 3 | 0, 42])
    repo.reward_raw = bytes(raw)
    repo.reward_version = 4

    uc = pbiz.PlayerUsecase(repo, _cfg())
    await uc.claim_reward(1, ppb.REWARD_SOURCE_TYPE_PERMANENT, "sign_in", 0, 5)
    assert bytes([15 << 3 | 0, 42]) in repo.reward_raw


@pytest.mark.asyncio
async def test_claim_reward_validates_source_shape() -> None:
    repo = FakeRepo()
    uc = pbiz.PlayerUsecase(repo, _cfg())
    with pytest.raises(errcode.PandoraError, match="source required"):
        await uc.claim_reward(1, ppb.REWARD_SOURCE_TYPE_PERMANENT, "", 0, 1)
    with pytest.raises(errcode.PandoraError, match="activity_instance_id required"):
        await uc.claim_reward(1, ppb.REWARD_SOURCE_TYPE_ACTIVITY, "", 0, 1)
    with pytest.raises(errcode.PandoraError, match="unknown reward source_type"):
        await uc.claim_reward(1, ppb.REWARD_SOURCE_TYPE_UNSPECIFIED, "", 0, 1)


# ── 后台循环 ─────────────────────────────────────────────────────────────────


class _Lease:
    def __init__(self, held: bool) -> None:
        self.held = held

    def current(self) -> tuple[bool, int]:
        return self.held, 1


@pytest.mark.asyncio
async def test_publish_batch_stops_at_first_delivery_failure() -> None:
    """投递失败**立即中断本轮**(保留出箱行下轮重试),保证同玩家事件按 id 顺序投递。"""
    repo = FakeRepo()
    repo.outbox = [
        m.PushOutboxRecord(id=1, player_id=7, event_type=1, payload=b"a"),
        m.PushOutboxRecord(id=2, player_id=7, event_type=1, payload=b"b"),
    ]

    class FlakyPusher:
        def __init__(self) -> None:
            self.sent: list[int] = []

        async def push_player_event(self, player_id, event_type, payload):  # noqa: ANN001
            if len(self.sent) == 1:
                raise RuntimeError("broker down")
            self.sent.append(player_id)

    uc = pbiz.PlayerUsecase(repo, _cfg())
    pusher = FlakyPusher()
    uc.set_experience_pusher(pusher)
    with pytest.raises(RuntimeError):
        await uc._publish_push_outbox_batch()
    # 第一条已删,第二条**没被删**(下轮重试)。
    assert repo.deleted_outbox == [1]


def test_push_leadership_is_evaluated_every_round() -> None:
    """失主后本副本立刻停止发布,不"补完"在飞的行 —— 那正是交错的来源。"""
    uc = pbiz.PlayerUsecase(FakeRepo(), _cfg())
    assert uc._push_is_leader() is True  # 未注入 → 无条件发布

    lease = _Lease(held=False)
    uc.set_push_writer_lease(lease)
    assert uc._push_is_leader() is False
    lease.held = True
    assert uc._push_is_leader() is True


class _SweepRecorder:
    """按脚本返回 Outcome,并记录被调用了几次。"""

    def __init__(self, outcomes: list[dbguard.Outcome]) -> None:
        self._outcomes = outcomes
        self.calls = 0

    async def __call__(self, mode, cutoff, limit):  # noqa: ANN001
        self.calls += 1
        return self._outcomes[min(self.calls - 1, len(self._outcomes) - 1)]


@pytest.mark.asyncio
async def test_report_only_sweep_does_not_loop() -> None:
    """★ report_only 下**不循环**:那一轮的 COUNT 已给出全量待清理规模。

    循环的意义是"追平积压",而只报告时积压永远追不平 —— 循环会变成每轮跑满的空转。
    """
    uc = pbiz.PlayerUsecase(FakeRepo(), _cfg())
    sweep = _SweepRecorder(
        [dbguard.Outcome(mode=dbguard.Mode.REPORT_ONLY, matched=10_000, deleted=0)]
    )
    await uc._drain_retention(
        dbguard.Mode.REPORT_ONLY, _dt.datetime(2026, 1, 1), "exp_history", sweep
    )
    assert sweep.calls == 1


@pytest.mark.asyncio
async def test_delete_sweep_drains_until_caught_up() -> None:
    """delete 档必须循环删到**短批**为止,判据是 truncated。

    ★ 罐装 Outcome 的形状必须是真 sweep_table 造得出来的:DELETE 档
      `matched == deleted` 恒成立(见 test_pkg_infra 的形状契约组)。这里曾写成
      `matched=2500, deleted=1000` —— 一个生产上永不出现的形状,于是这条用例验的是
      一条走不到的分支:带着"循环只跑一轮"的真 bug 它照样绿。
    """
    uc = pbiz.PlayerUsecase(FakeRepo(), _cfg())
    sweep = _SweepRecorder(
        [
            dbguard.Outcome(
                mode=dbguard.Mode.DELETE, matched=1000, deleted=1000, truncated=True
            ),
            dbguard.Outcome(
                mode=dbguard.Mode.DELETE, matched=1000, deleted=1000, truncated=True
            ),
            dbguard.Outcome(
                mode=dbguard.Mode.DELETE, matched=500, deleted=500, truncated=False
            ),
        ]
    )
    await uc._drain_retention(
        dbguard.Mode.DELETE, _dt.datetime(2026, 1, 1), "exp_history", sweep
    )
    assert sweep.calls == 3


class _FakeCursor:
    """按脚本吐 rowcount 的假 cursor —— 让 dbguard.sweep_table 走真实代码路径。"""

    def __init__(self, rowcounts: list[int]) -> None:
        self._rowcounts = rowcounts
        self.calls = 0
        self.rowcount = 0

    async def __aenter__(self) -> "_FakeCursor":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def execute(self, sql: str, params: object = None) -> None:  # noqa: ARG002
        self.rowcount = self._rowcounts[min(self.calls, len(self._rowcounts) - 1)]
        self.calls += 1

    async def fetchone(self) -> tuple[int]:
        return (0,)


class _FakeConn:
    def __init__(self, cur: _FakeCursor) -> None:
        self._cur = cur

    def cursor(self) -> _FakeCursor:
        return self._cur


@pytest.mark.asyncio
async def test_delete_sweep_drains_backlog_through_real_sweep_table() -> None:
    """★ 端到端钉住"Outcome 形状 ↔ 消费者判据"这条缝:不手搓 Outcome,走真 sweep_table。

    本条缺陷当年正是从这条缝漏过去的 —— 上面那个 `_SweepRecorder` 是手搓件,想编什么
    形状就编什么形状,于是消费者用了一个"只对假件成立"的判据也没人发现。这里让
    `dbguard.sweep_table` 真的跑一遍(假的只有 DB cursor),它产出的 Outcome 就是生产
    上会出现的那一种。

    场景:表里 3500 行待删、batch=1000 → 必须跑 4 轮(1000+1000+1000+500)删到追平。
    循环若在第一轮就退出,五张只增表在 retention_mode=delete 下每表每小时最多删 1000 行,
    积压永远追不平(§9.24 的容量守护形同虚设),而日志里 truncated=True 正写着还有积压。
    """
    cur = _FakeCursor([1000, 1000, 1000, 500])
    conn = _FakeConn(cur)

    async def sweep(mode, cutoff, limit):  # noqa: ANN001, ANN202
        return await dbguard.sweep_table(
            conn, mode, "pandora_player", "exp_history", "created_at < %s", limit, cutoff
        )

    uc = pbiz.PlayerUsecase(FakeRepo(), _cfg())
    await uc._drain_retention(
        dbguard.Mode.DELETE, _dt.datetime(2026, 1, 1), "exp_history", sweep
    )
    assert cur.calls == 4


@pytest.mark.asyncio
async def test_delete_sweep_stops_on_empty_batch() -> None:
    """删到 0 行也必须停 —— 判据只留 truncated 后不能出现死循环。

    `limit` 恒 >0(RETENTION_SWEEP_BATCH=1000,repo 侧还会把 <=0 兜成 1000),
    所以 deleted==0 时 truncated 必为 False。
    """
    cur = _FakeCursor([0])
    conn = _FakeConn(cur)

    async def sweep(mode, cutoff, limit):  # noqa: ANN001, ANN202
        return await dbguard.sweep_table(
            conn, mode, "pandora_player", "exp_history", "created_at < %s", limit, cutoff
        )

    uc = pbiz.PlayerUsecase(FakeRepo(), _cfg())
    await uc._drain_retention(
        dbguard.Mode.DELETE, _dt.datetime(2026, 1, 1), "exp_history", sweep
    )
    assert cur.calls == 1


@pytest.mark.asyncio
async def test_sweep_failure_does_not_spin_forever() -> None:
    """单表 sweep 失败只丢本表本轮(WARN 留证),不阻断同一轮里的其它表。"""

    async def _boom(mode, cutoff, limit):  # noqa: ANN001
        raise RuntimeError("db down")

    uc = pbiz.PlayerUsecase(FakeRepo(), _cfg())
    await uc._drain_retention(
        dbguard.Mode.DELETE, _dt.datetime(2026, 1, 1), "exp_history", _boom
    )


def test_retention_sweep_registry_covers_all_append_only_tables() -> None:
    """★ §9.24 登记表:五张只增表一张都不能漏接 —— 漏接就是"既不删也不报"。"""
    uc = pbiz.PlayerUsecase(FakeRepo(), _cfg())
    assert [t for t, _ in uc._exp_history_sweeps()] == ["exp_history"]
    assert [t for t, _ in uc._history_sweeps()] == [
        "mmr_history",
        "attr_point_grants",
        "talent_point_grants",
        "skill_card_grants",
    ]


def test_profile_shard_key_is_player_id_only() -> None:
    """分片键**不取 nickname / hero_id / 任何配置 ID**(与落点无关)。"""
    assert pbiz.profile_shard_key(123) == "123"
    assert pbiz.profile_shard_key(0) == "0"


@pytest.mark.asyncio
async def test_publisher_returns_immediately_without_pusher() -> None:
    """producer 未注入 → 出箱积压不丢,发布器直接返回(不空转)。"""
    uc = pbiz.PlayerUsecase(FakeRepo(), _cfg())
    await asyncio.wait_for(uc.run_push_outbox_publisher(), timeout=1.0)
