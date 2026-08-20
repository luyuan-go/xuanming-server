"""mission biz + service 层:接取校验、发放路由、推送分片、权限方向。

全部用内存假件,不碰真库 —— 这一层的缺陷形状是"逻辑走错分支"而不是"SQL 写错",
真库只会把跑一轮的时间从毫秒拉到秒。repo 的事务/锁序纪律由 Go 侧的
mission_guard_lock_order_mysql_test.go 钉住,那条必须在真 MySQL 上跑。
"""

from __future__ import annotations

import asyncio

import pytest
from pandora.common.v1 import errcode_pb2
from pandora.config.v1 import mission_pb2 as cfgpb
from pandora.mission.v1 import mission_pb2

from pandorapy import errcode
from pandorapy.services.mission import biz as mbiz
from pandorapy.services.mission import catalog as mc
from pandorapy.services.mission import conf as mconf
from pandorapy.services.mission import engine as eng
from pandorapy.services.mission import repo as mrepo
from pandorapy.services.mission import service as msvc


# ── 假件 ─────────────────────────────────────────────────────────────────────


class FakeRepo:
    """内存 repo:保住"事务回调抛异常 = 突变整体不落"这条语义。"""

    def __init__(self, state: eng.PlayerState | None = None) -> None:
        self.state = state or eng.PlayerState(player_id=1)
        self.persisted: list[eng.Mutation] = []
        self.receipts: dict[tuple[int, str], bytes] = {}
        self.marks: list[tuple[int, bool]] = []
        self.outbox: list[mrepo.PushOutboxRow] = []
        self.deleted: list[int] = []
        self.sweeps: list[str] = []
        self.ungranted: list[mrepo.RewardLogRow] = []
        self.next_log_id = 100
        self.raced_ids: set[int] = set()

    async def load_player(self, player_id: int) -> eng.PlayerState:
        return self.state

    async def mutate_player(self, player_id: int, fn) -> None:  # noqa: ANN001
        mut = fn(self.state)
        self._assign_ids(mut)
        self.persisted.append(mut)

    async def apply_facts_tx(self, player_id, idem_key, fingerprint, fn) -> bool:  # noqa: ANN001
        key = (player_id, idem_key)
        if key in self.receipts:
            if self.receipts[key] != fingerprint:
                raise errcode.PandoraError(
                    errcode.ErrMissionFactsConflict, "fingerprint mismatch"
                )
            return True
        self.receipts[key] = fingerprint
        mut = fn(self.state)
        self._assign_ids(mut)
        self.persisted.append(mut)
        return False

    def _assign_ids(self, mut) -> None:  # noqa: ANN001
        if mut is None:
            return
        for entry in mut.reward_logs:
            self.next_log_id += 1
            entry.id = self.next_log_id
        for payload in mut.push_payloads:
            self.outbox.append(
                mrepo.PushOutboxRow(id=len(self.outbox) + 1, player_id=1, payload=payload)
            )

    async def list_ungranted_rewards(self, older_than_ms, limit):  # noqa: ANN001
        return self.ungranted[:limit]

    async def mark_reward(self, log_id: int, granted: bool, now_ms: int) -> None:
        self.marks.append((log_id, granted))

    async def fetch_push_outbox(self, limit: int):  # noqa: ANN201
        return self.outbox[:limit]

    async def delete_push_outbox(self, row_id: int) -> None:
        if row_id in self.raced_ids:
            raise mrepo.PushOutboxRacedError(f"raced {row_id}")
        self.deleted.append(row_id)
        self.outbox = [r for r in self.outbox if r.id != row_id]

    async def sweep_reward_log(self, mode, days, batch) -> None:  # noqa: ANN001
        self.sweeps.append("reward_log")

    async def sweep_receipts(self, mode, days, batch) -> None:  # noqa: ANN001
        self.sweeps.append("receipts")


class FakeItems:
    def __init__(self, *, capacity_full: bool = False) -> None:
        self.capacity_full = capacity_full
        self.stack_calls: list[tuple[int, str, list]] = []
        self.inst_calls: list[tuple[int, str, list]] = []

    async def grant_items(self, player_id, key, items) -> None:  # noqa: ANN001
        self.stack_calls.append((player_id, key, list(items)))

    async def grant_instances(self, player_id, key, ids) -> bool:  # noqa: ANN001
        self.inst_calls.append((player_id, key, list(ids)))
        return self.capacity_full


class FakeExp:
    def __init__(self) -> None:
        self.calls: list[tuple[int, int, str]] = []

    async def add_experience(self, player_id, delta, key) -> None:  # noqa: ANN001
        self.calls.append((player_id, delta, key))


class FakeMail:
    def __init__(self) -> None:
        self.calls: list[tuple[int, list, str]] = []

    async def send_overflow_mail(self, player_id, ids, key) -> None:  # noqa: ANN001
        self.calls.append((player_id, list(ids), key))


class FakePusher:
    def __init__(self, *, fail_on: int | None = None) -> None:
        self.sent: list[tuple[int, bytes]] = []
        self.fail_on = fail_on

    async def push_mission_update(self, player_id: int, payload: bytes) -> None:
        if self.fail_on is not None and len(self.sent) == self.fail_on:
            raise RuntimeError("broker down")
        self.sent.append((player_id, payload))


class FakeContext:
    """最小 grpc context:只需要 invocation_metadata()。"""

    def __init__(self, player_id: int = 0) -> None:
        self._md = (
            (("x-pandora-player-id", str(player_id)),) if player_id else ()
        )

    def invocation_metadata(self):  # noqa: ANN201
        return self._md


# ── 夹具构造 ─────────────────────────────────────────────────────────────────


def _mission(**kw) -> cfgpb.MissionRow:
    kw.setdefault("name", "m")
    kw.setdefault("mission_type", 1)
    kw.setdefault("condition_ids", "1")
    return cfgpb.MissionRow(**kw)


def _cond(**kw) -> cfgpb.ConditionRow:
    kw.setdefault("name", "c")
    kw.setdefault("condition_category", 1)
    return cfgpb.ConditionRow(**kw)


def _tables(missions=(), conditions=(), rewards=(), equipment=(), items=()) -> mc.Tables:
    return mc.Tables(
        version=1,
        source_rev="t",
        missions={m.id: m for m in missions},
        conditions={c.id: c for c in conditions},
        rewards={r.id: r for r in rewards},
        equipment_items=frozenset(equipment),
        item_ids=frozenset(items) | frozenset(equipment),
    )


def _cfg(**kw) -> mconf.MissionConf:
    cfg = mconf.Config.model_validate(
        {"config_table": {"dir": "x"}, "mission": {"allow_noop_reward": True, **kw}}
    )
    cfg.apply_defaults()
    return cfg.mission


def _uc(tables, *, repo=None, items=None, exp=None, mail=None, pusher=None, cfg=None):  # noqa: ANN001, ANN201
    repo = repo or FakeRepo()
    uc = mbiz.MissionUsecase(
        repo,
        mc.CatalogSource(tables),
        items or FakeItems(),
        exp or FakeExp(),
        mail,
        pusher,
        cfg or _cfg(),
        now_fn=lambda: 1_000,
    )
    return uc, repo


# ── 接取校验 ─────────────────────────────────────────────────────────────────


async def test_accept_rejects_unknown_mission() -> None:
    uc, _ = _uc(_tables())
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.accept(1, 42)
    assert ei.value.code == errcode.ErrMissionConfigNotFound


async def test_accept_enforces_active_limit() -> None:
    tables = _tables([_mission(id=i) for i in (1, 2)], [_cond(id=1, target_count=1)])
    repo = FakeRepo()
    repo.state.active[1] = eng.ActiveMission(mission_config_id=1, progress=[0])
    uc, _ = _uc(tables, repo=repo, cfg=_cfg(max_active_missions=1))
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.accept(1, 2)
    assert ei.value.code == errcode.ErrMissionActiveLimit


async def test_accept_enforces_type_exclusivity() -> None:
    """同 (type, sub_type) 至多一个活跃任务;sub_type=0 不参与互斥。"""
    tables = _tables(
        [
            _mission(id=1, mission_type=3, mission_sub_type=7),
            _mission(id=2, mission_type=3, mission_sub_type=7),
            _mission(id=3, mission_type=3, mission_sub_type=0),
        ],
        [_cond(id=1, target_count=1)],
    )
    repo = FakeRepo()
    repo.state.active[1] = eng.ActiveMission(mission_config_id=1, progress=[0])
    uc, _ = _uc(tables, repo=repo)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.accept(1, 2)
    assert ei.value.code == errcode.ErrMissionTypeConflict
    # sub_type=0 的那条照常可接。
    await uc.accept(1, 3)


async def test_accept_rejects_completed_mission() -> None:
    tables = _tables([_mission(id=1)], [_cond(id=1, target_count=1)])
    repo = FakeRepo()
    repo.state.done[1] = eng.DoneMission(mission_config_id=1)
    uc, _ = _uc(tables, repo=repo)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.accept(1, 1)
    assert ei.value.code == errcode.ErrMissionAlreadyCompleted


async def test_abandon_distinguishes_not_accepted_from_completed() -> None:
    tables = _tables([_mission(id=1), _mission(id=2)], [_cond(id=1, target_count=1)])
    repo = FakeRepo()
    repo.state.done[2] = eng.DoneMission(mission_config_id=2)
    uc, _ = _uc(tables, repo=repo)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.abandon(1, 1)
    assert ei.value.code == errcode.ErrMissionNotAccepted
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.abandon(1, 2)
    assert ei.value.code == errcode.ErrMissionAlreadyCompleted


async def test_chain_accept_respects_active_limit_and_skips() -> None:
    """完成扇出的自动接链必须走**同一套**接取校验,校验不过跳过该条不阻断整批。

    不传 accept_fn 会让这条路径绕过上限:一条 next 链能把活跃任务顶到任意多条,
    而玩家自己调 AcceptMission 时上限照常生效 —— 同一个不变量两套判据。
    """
    tables = _tables(
        [
            _mission(id=1, condition_ids="1", next_mission_ids="2,3"),
            _mission(id=2, condition_ids="1"),
            _mission(id=3, condition_ids="1"),
        ],
        [_cond(id=1, target_count=1, slot1="5001")],
    )
    repo = FakeRepo()
    repo.state.active[1] = eng.ActiveMission(mission_config_id=1, progress=[0])
    uc, _ = _uc(tables, repo=repo, cfg=_cfg(max_active_missions=1))
    await uc.report_facts(1, [eng.Fact(category=1, slot_values=(5001,), amount=1)], "k1")
    assert 1 in repo.state.done
    # 任务 1 完成后活跃集为空 → 链上第一条能接进来;第二条撞上限 = 1 被跳过,
    # 但**整批不中断**(任务 1 的完成与发奖照常落库)。
    assert 2 in repo.state.active
    assert 3 not in repo.state.active
    assert 3 not in repo.state.done
    assert repo.persisted[-1].auto_accepted == [2]


# ── 事实入账 ─────────────────────────────────────────────────────────────────


async def test_report_facts_rejects_oversized_batch() -> None:
    uc, _ = _uc(_tables(), cfg=_cfg(max_facts_per_report=2))
    facts = [eng.Fact(category=1, slot_values=(1,), amount=1)] * 3
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.report_facts(1, facts, "k")
    assert ei.value.code == errcode.ErrInvalidArg


async def test_report_facts_is_idempotent_by_receipt() -> None:
    tables = _tables([_mission(id=1)], [_cond(id=1, target_count=3, slot1="5001")])
    repo = FakeRepo()
    repo.state.active[1] = eng.ActiveMission(mission_config_id=1, progress=[0])
    uc, _ = _uc(tables, repo=repo)
    fact = [eng.Fact(category=1, slot_values=(5001,), amount=1)]
    assert await uc.report_facts(1, fact, "k") is False
    assert repo.state.active[1].progress == [1]
    # 同键同内容重放:already=True 且**不再入账**。
    assert await uc.report_facts(1, fact, "k") is True
    assert repo.state.active[1].progress == [1]


async def test_report_facts_same_key_different_content_is_rejected() -> None:
    """同键串改账 fail-closed —— 放行等于允许上游用一个键刷任意进度。"""
    tables = _tables([_mission(id=1)], [_cond(id=1, target_count=9, slot1="5001")])
    repo = FakeRepo()
    repo.state.active[1] = eng.ActiveMission(mission_config_id=1, progress=[0])
    uc, _ = _uc(tables, repo=repo)
    await uc.report_facts(1, [eng.Fact(category=1, slot_values=(5001,), amount=1)], "k")
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.report_facts(
            1, [eng.Fact(category=1, slot_values=(5001,), amount=5)], "k"
        )
    assert ei.value.code == errcode.ErrMissionFactsConflict


def test_facts_fingerprint_is_stable_and_content_sensitive() -> None:
    """指纹不依赖 proto 编码:字段增删不得改变既有收据的指纹。"""
    a = [eng.Fact(category=1, slot_values=(5001,), amount=2)]
    b = [eng.Fact(category=1, slot_values=(5001,), amount=3)]
    assert mbiz.facts_fingerprint(7, a) == mbiz.facts_fingerprint(7, a)
    assert mbiz.facts_fingerprint(7, a) != mbiz.facts_fingerprint(7, b)
    assert mbiz.facts_fingerprint(7, a) != mbiz.facts_fingerprint(8, a)


# ── 领奖与发放路由 ───────────────────────────────────────────────────────────


async def test_claim_requires_claimable_state() -> None:
    tables = _tables([_mission(id=1, reward_id=1)], [_cond(id=1, target_count=1)])
    repo = FakeRepo()
    repo.state.done[1] = eng.DoneMission(
        mission_config_id=1, reward_state=eng.REWARD_STATE_NONE
    )
    uc, _ = _uc(tables, repo=repo)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.claim(1, 1)
    assert ei.value.code == errcode.ErrMissionNotClaimable


async def test_claim_marks_claimed_and_grants() -> None:
    reward = cfgpb.RewardRow(id=1, name="r", item_ids="500", item_counts="2", exp=30)
    tables = _tables(
        [_mission(id=1, reward_id=1)],
        [_cond(id=1, target_count=1)],
        [reward],
        items=(500,),
    )
    repo = FakeRepo()
    repo.state.done[1] = eng.DoneMission(
        mission_config_id=1, reward_state=eng.REWARD_STATE_CLAIMABLE
    )
    items, exp = FakeItems(), FakeExp()
    uc, _ = _uc(tables, repo=repo, items=items, exp=exp)
    await uc.claim(1, 1)
    assert repo.state.done[1].reward_state == eng.REWARD_STATE_CLAIMED
    assert repo.persisted[-1].claim_done == [1]
    # 三通道**分键**:inventory_ledger 的 uk 是 (player_id, key),同键会撞指纹冲突。
    assert items.stack_calls[0][1] == "mission:1:1:stack"
    assert exp.calls[0][2] == "quest:1:1"


async def test_equipment_route_uses_frozen_bit_not_current_table() -> None:
    """回归:发放路由必须读快照里的**冻结位**,不回读道具表。

    回读的后果:形态在两次投递之间被热更改掉,同一条奖励先后用 `:stack` 和 `:inst`
    两个不同幂等键各发一次 —— 幂等键防不住,因为不是同一个键。
    """
    items = FakeItems()
    uc, _ = _uc(_tables(), items=items)  # 当前批次里 101 **不是**装备
    row = mrepo.RewardLogRow(
        id=1,
        player_id=7,
        mission_config_id=9,
        key="mission:7:9",
        reward_pb=mission_pb2.MissionRewardStorageRecord(
            items=[
                mission_pb2.MissionRewardItem(
                    item_config_id=101, count=2, equipment=True
                )
            ]
        ).SerializeToString(),
    )
    cat = uc.catalogs.snapshot()
    await uc.grant_one(cat, row)
    assert items.inst_calls == [(7, "mission:7:9:inst", [101, 101])]
    assert items.stack_calls == []


async def test_unfrozen_legacy_row_falls_back_to_item_table() -> None:
    """冻结位缺省 = 上线前的历史行,回退读道具表(§9.17 双向兼容)。"""
    items = FakeItems()
    uc, _ = _uc(_tables(equipment=(101,)), items=items)
    record = mission_pb2.MissionRewardStorageRecord()
    it = record.items.add()
    it.item_config_id = 101
    it.count = 1  # 刻意不设 equipment
    row = mrepo.RewardLogRow(
        id=1, player_id=7, mission_config_id=9, key="k", reward_pb=record.SerializeToString()
    )
    await uc.grant_one(uc.catalogs.snapshot(), row)
    assert items.inst_calls[0][2] == [101]


async def test_capacity_full_routes_to_overflow_mail_with_same_key() -> None:
    """满包转邮件必须传**同一个 `:inst` 键**,否则直发链与邮件链各发一次。"""
    items, mail = FakeItems(capacity_full=True), FakeMail()
    uc, _ = _uc(_tables(), items=items, mail=mail)
    row = mrepo.RewardLogRow(
        id=1,
        player_id=7,
        mission_config_id=9,
        key="mission:7:9",
        reward_pb=mission_pb2.MissionRewardStorageRecord(
            items=[
                mission_pb2.MissionRewardItem(item_config_id=101, count=1, equipment=True)
            ]
        ).SerializeToString(),
    )
    await uc.grant_one(uc.catalogs.snapshot(), row)
    assert mail.calls == [(7, [101], "mission:7:9:inst")]


async def test_capacity_full_without_mail_stays_pending() -> None:
    """mail_addr 未配时满包**不得**标 GRANTED —— 装备必须留补扫,不能凭空消失。"""
    items = FakeItems(capacity_full=True)
    uc, repo = _uc(_tables(), items=items, mail=None)
    row = mrepo.RewardLogRow(
        id=1,
        player_id=7,
        mission_config_id=9,
        key="k",
        reward_pb=mission_pb2.MissionRewardStorageRecord(
            items=[
                mission_pb2.MissionRewardItem(item_config_id=101, count=1, equipment=True)
            ]
        ).SerializeToString(),
    )
    with pytest.raises(errcode.PandoraError):
        await uc.grant_one(uc.catalogs.snapshot(), row)
    assert repo.marks == [(1, False)]


async def test_oversized_equipment_snapshot_is_refused_not_expanded() -> None:
    """坏快照的装备数量必须当场拒,不能按数量分配列表(补扫每轮 OOM 一次)。"""
    items = FakeItems()
    uc, _ = _uc(_tables(), items=items)
    row = mrepo.RewardLogRow(
        id=1,
        player_id=7,
        mission_config_id=9,
        key="k",
        reward_pb=mission_pb2.MissionRewardStorageRecord(
            items=[
                mission_pb2.MissionRewardItem(
                    item_config_id=101,
                    count=mc.MAX_REWARD_EQUIPMENT_INSTANCES + 1,
                    equipment=True,
                )
            ]
        ).SerializeToString(),
    )
    with pytest.raises(errcode.PandoraError, match="越界"):
        await uc.grant_one(uc.catalogs.snapshot(), row)
    assert items.inst_calls == []


async def test_grant_one_marks_granted_on_success() -> None:
    uc, repo = _uc(_tables())
    row = mrepo.RewardLogRow(
        id=5,
        player_id=7,
        mission_config_id=9,
        key="k",
        reward_pb=mission_pb2.MissionRewardStorageRecord(exp=10).SerializeToString(),
    )
    await uc.grant_one(uc.catalogs.snapshot(), row)
    assert repo.marks == [(5, True)]


# ── 推送出箱 ─────────────────────────────────────────────────────────────────


def test_marshal_event_chunks_splits_by_count_and_size() -> None:
    """分片让"推送太大"退化成"多发几条",而不是"进度写不进去"(出箱列 2048 字节)。"""
    progressed = [
        mission_pb2.ActiveMission(mission_config_id=i, progress=[1, 2], targets=[3, 4])
        for i in range(1, 20)
    ]
    chunks = mbiz.marshal_event_chunks(
        1, progressed=progressed, completed=[], auto_accepted=[]
    )
    assert len(chunks) == 4  # 19 条 / 每片 6 条
    total = 0
    for pb in chunks:
        evt = mission_pb2.MissionUpdateEvent()
        evt.ParseFromString(pb)
        assert len(pb) <= mrepo.PUSH_PAYLOAD_MAX
        total += len(evt.progressed)
    assert total == 19


def test_marshal_event_chunks_empty_is_empty() -> None:
    assert mbiz.marshal_event_chunks(1, progressed=[], completed=[], auto_accepted=[]) == []


async def test_push_publisher_stops_batch_on_send_failure() -> None:
    """失败**中断本轮**:同玩家事件必须保序,跳过失败行再发后续行就乱序了。"""
    pusher = FakePusher(fail_on=1)
    uc, repo = _uc(_tables(), pusher=pusher)
    repo.outbox = [mrepo.PushOutboxRow(id=i, player_id=1, payload=b"x") for i in (1, 2, 3)]
    await uc._publish_push_batch()
    assert repo.deleted == [1]  # 第 2 条投递失败 → 本轮到此为止
    assert [r.id for r in repo.outbox] == [2, 3]


async def test_push_publisher_stops_on_race() -> None:
    """删行命中 0 行 = 另一副本也在发布 → 立即中断本轮(继续投只会加剧交错)。"""
    pusher = FakePusher()
    uc, repo = _uc(_tables(), pusher=pusher)
    repo.outbox = [mrepo.PushOutboxRow(id=i, player_id=1, payload=b"x") for i in (1, 2)]
    repo.raced_ids = {1}
    await uc._publish_push_batch()
    assert repo.deleted == []
    assert len(pusher.sent) == 1


async def test_push_publisher_skips_round_when_not_leader() -> None:
    """失主后本副本立刻停止发布(下一拍即生效),不"补完"在飞的行。"""

    class Lease:
        def current(self):  # noqa: ANN201
            return False, 0

    pusher = FakePusher()
    uc, repo = _uc(_tables(), pusher=pusher)
    uc.set_push_writer_lease(Lease())
    repo.outbox = [mrepo.PushOutboxRow(id=1, player_id=1, payload=b"x")]
    await uc._push_round()
    assert pusher.sent == []


async def test_no_lease_means_unconditional_publisher() -> None:
    pusher = FakePusher()
    uc, repo = _uc(_tables(), pusher=pusher)
    repo.outbox = [mrepo.PushOutboxRow(id=1, player_id=1, payload=b"x")]
    await uc._push_round()
    assert len(pusher.sent) == 1


# ── 保留期清理 ───────────────────────────────────────────────────────────────


async def test_receipt_sweep_is_off_by_default() -> None:
    """组级闸默认关:收据**连报告都不跑**(上游重试无界,删收据会双计进度)。"""
    uc, repo = _uc(_tables())
    await uc._sweep_once()
    assert repo.sweeps == ["reward_log"]


async def test_receipt_sweep_runs_when_enabled() -> None:
    uc, repo = _uc(_tables(), cfg=_cfg(receipt_cleanup_enabled=True))
    await uc._sweep_once()
    assert repo.sweeps == ["reward_log", "receipts"]


async def test_sweep_continues_after_single_table_failure() -> None:
    """单表失败只记日志继续下一表 —— 一张表的问题不该把另一张表也停掉。"""

    class BoomRepo(FakeRepo):
        async def sweep_reward_log(self, mode, days, batch) -> None:  # noqa: ANN001
            raise RuntimeError("boom")

    uc, repo = _uc(
        _tables(), repo=BoomRepo(), cfg=_cfg(receipt_cleanup_enabled=True)
    )
    await uc._sweep_once()
    assert repo.sweeps == ["receipts"]


# ── service 层 ───────────────────────────────────────────────────────────────


class StubUsecase:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def list_missions(self, player_id):  # noqa: ANN001, ANN201
        self.calls.append(("list", player_id))
        return [], []

    async def accept(self, player_id, mid):  # noqa: ANN001, ANN201
        self.calls.append(("accept", player_id, mid))
        return mission_pb2.ActiveMission(mission_config_id=mid)

    async def abandon(self, player_id, mid) -> None:  # noqa: ANN001
        self.calls.append(("abandon", player_id, mid))

    async def claim(self, player_id, mid) -> None:  # noqa: ANN001
        self.calls.append(("claim", player_id, mid))

    async def report_facts(self, player_id, facts, key) -> bool:  # noqa: ANN001
        self.calls.append(("report", player_id, len(facts), key))
        return False

    async def complete_all(self, player_id) -> int:  # noqa: ANN001
        self.calls.append(("complete_all", player_id))
        return 3


async def test_client_rpc_requires_identity() -> None:
    svc = msvc.MissionService(StubUsecase())
    resp = await svc.ListMissions(mission_pb2.ListMissionsRequest(), FakeContext(0))
    assert resp.code == errcode_pb2.ERR_UNAUTHORIZED


async def test_client_rpc_ignores_request_body_identity() -> None:
    """R5:player_id 取鉴权上下文。请求体里的身份字段一律不看。"""
    uc = StubUsecase()
    svc = msvc.MissionService(uc)
    await svc.AcceptMission(
        mission_pb2.AcceptMissionRequest(mission_config_id=5), FakeContext(77)
    )
    assert uc.calls == [("accept", 77, 5)]


async def test_system_rpc_rejects_player_identity() -> None:
    """方向与客户端 RPC **相反**:有身份就拒。写反了玩家能自己刷满任务进度。"""
    uc = StubUsecase()
    svc = msvc.MissionService(uc)
    resp = await svc.ReportMissionFacts(
        mission_pb2.ReportMissionFactsRequest(
            player_id=1,
            facts=[mission_pb2.MissionFact(condition_category=1, condition_ids=[1], amount=1)],
            idempotency_key="k",
        ),
        FakeContext(9),
    )
    assert resp.code == errcode_pb2.ERR_PERMISSION_DENY
    assert uc.calls == []

    resp = await svc.CompleteAllMissions(
        mission_pb2.CompleteAllMissionsRequest(player_id=1), FakeContext(9)
    )
    assert resp.code == errcode_pb2.ERR_PERMISSION_DENY


async def test_system_rpc_accepts_internal_caller() -> None:
    uc = StubUsecase()
    svc = msvc.MissionService(uc)
    resp = await svc.ReportMissionFacts(
        mission_pb2.ReportMissionFactsRequest(
            player_id=1,
            facts=[mission_pb2.MissionFact(condition_category=1, condition_ids=[1], amount=2)],
            idempotency_key="k",
        ),
        FakeContext(0),
    )
    assert resp.code == errcode_pb2.OK
    assert uc.calls == [("report", 1, 1, "k")]

    resp = await svc.CompleteAllMissions(
        mission_pb2.CompleteAllMissionsRequest(player_id=1), FakeContext(0)
    )
    assert resp.code == errcode_pb2.OK
    assert resp.completed_count == 3


async def test_business_error_is_in_band_not_grpc_status() -> None:
    """错误必须 in-band:改成 gRPC status 会让客户端把它当网络故障去退避重连。"""

    class BoomUsecase(StubUsecase):
        async def claim(self, player_id, mid) -> None:  # noqa: ANN001
            raise errcode.PandoraError(errcode.ErrMissionNotClaimable, "nope")

    svc = msvc.MissionService(BoomUsecase())
    resp = await svc.ClaimMissionReward(
        mission_pb2.ClaimMissionRewardRequest(mission_config_id=1), FakeContext(7)
    )
    assert resp.code == errcode_pb2.ERR_MISSION_NOT_CLAIMABLE


async def test_unknown_exception_maps_to_internal_not_ok() -> None:
    """未知异常绝不能映射成 OK —— 那会让一次库故障对客户端表现为"操作成功"。"""

    class BoomUsecase(StubUsecase):
        async def abandon(self, player_id, mid) -> None:  # noqa: ANN001
            raise RuntimeError("db down")

    svc = msvc.MissionService(BoomUsecase())
    resp = await svc.AbandonMission(
        mission_pb2.AbandonMissionRequest(mission_config_id=1), FakeContext(7)
    )
    assert resp.code == errcode_pb2.ERR_INTERNAL


async def test_zero_mission_id_is_invalid_arg() -> None:
    svc = msvc.MissionService(StubUsecase())
    for req, call in (
        (mission_pb2.AcceptMissionRequest(), svc.AcceptMission),
        (mission_pb2.AbandonMissionRequest(), svc.AbandonMission),
        (mission_pb2.ClaimMissionRewardRequest(), svc.ClaimMissionReward),
    ):
        resp = await call(req, FakeContext(7))
        assert resp.code == errcode_pb2.ERR_INVALID_ARG


# ── 启动闸:部署策略机械门禁 ─────────────────────────────────────────────────


def test_deploy_strategy_gate(monkeypatch) -> None:  # noqa: ANN001
    from pandorapy.services.mission import main as mmain

    logger = mmain.plog.get()

    # 非 k8s 且无 env:WARN 放行(阻断会把开发环境一起打死)。
    monkeypatch.delenv("PANDORA_DEPLOY_STRATEGY", raising=False)
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    assert mmain._check_push_lease_deploy_strategy(logger, "off") is True

    # 受管 k8s 内缺 annotation:fail-closed(清单回归必须炸,不能靠人看日志)。
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    assert mmain._check_push_lease_deploy_strategy(logger, "off") is False

    # RollingUpdate × 非 enforce:拒启(滚动重叠期两个发布器并发)。
    monkeypatch.setenv("PANDORA_DEPLOY_STRATEGY", "RollingUpdate")
    assert mmain._check_push_lease_deploy_strategy(logger, "off") is False
    assert mmain._check_push_lease_deploy_strategy(logger, "enforce") is True

    # Recreate × off:放行。
    monkeypatch.setenv("PANDORA_DEPLOY_STRATEGY", "Recreate")
    assert mmain._check_push_lease_deploy_strategy(logger, "off") is True


def test_main_uses_same_topic_as_go() -> None:
    from pandorapy import kafka_topics
    from pandorapy.services.mission import main as mmain

    assert kafka_topics.TOPIC_MISSION_UPDATE == "pandora.mission.update"
    assert mmain.PUSH_LEASE_ELECTION == "mission/push_publisher"


def test_required_tables_match_ddl(repo_root) -> None:  # noqa: ANN001
    """启动 schema 检查的表清单必须与建表 SQL 一致 —— 少一张就是缺表不 fail-fast。"""
    from pandorapy.services.mission import main as mmain

    ddl = (repo_root / "deploy/mysql-init/16-mission-tables.sql").read_text(
        encoding="utf-8"
    )
    for table in mmain.REQUIRED_TABLES:
        assert f"`{table}`" in ddl


def test_asyncio_cancelled_propagates_through_grant_loop() -> None:
    """取消必须穿透补扫循环:吞掉会让停机时"排空在途"永远排不空。"""

    async def _run() -> None:
        class CancelItems(FakeItems):
            async def grant_items(self, player_id, key, items) -> None:  # noqa: ANN001
                raise asyncio.CancelledError

        uc, _ = _uc(_tables(), items=CancelItems())
        row = mrepo.RewardLogRow(
            id=1,
            player_id=7,
            mission_config_id=9,
            key="k",
            reward_pb=mission_pb2.MissionRewardStorageRecord(
                items=[
                    mission_pb2.MissionRewardItem(
                        item_config_id=101, count=1, equipment=False
                    )
                ]
            ).SerializeToString(),
        )
        with pytest.raises(asyncio.CancelledError):
            await uc._grant_entries_best_effort(
                uc.catalogs.snapshot(),
                7,
                [mrepo.RewardLogEntry(mission_config_id=9, key="k", reward_pb=row.reward_pb, id=1)],
            )

    asyncio.run(_run())
