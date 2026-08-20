"""组票 roster fence + 赛后复位(INC-20260813-001)。

这两条路径的失效形状都是**静默的**:

  - `end_team_match` 不存在 → 打完一局队伍仍停在 READY,队长能在队友还没回大厅时
    用残留 ready 立刻再开一局(3v3 打成 3v2)。事故当天 team 侧**根本没有**
    match-ended 路径,而所有测试照样绿。
  - `begin_team_match` 的收据重入判错 → 响应丢失后的重试要么重复消费一次 ready、
    要么拿回一份空 ready 的名单交给 matchmaker 建票。两种都不报错。

所以这里直接盯"代际推进了没、快照里的 ready 位是不是冻结那一刻的、重投有没有被 CAS 挡住"。

★ 全程无 Redis / kafka:repo 用内存假件,pusher=None(显式无 kafka 开发模式)。
  乐观锁语义由假件如实复刻(fn 抛异常 → 放弃写回并原样冒泡)。
"""

from __future__ import annotations

import pytest
from pandora.team.v1 import team_pb2

from pandorapy import errcode
from pandorapy import offlinewatch
from pandorapy.services.team import biz as tbiz
from pandorapy.services.team import conf as tconf
from pandorapy.services.team import offline_leave as toffline

TEAM_ID = 7001
CAPTAIN = 10001
MEMBER = 10002


class FakeRepo:
    """内存队伍仓储。只实现本组用例走到的那几个方法。

    `update_with_lock` 如实复刻真实实现的关键语义:**fn 抛异常 → 不写回、原样冒泡**
    (哨兵与业务判定都靠它),而不是被吞成"重试耗尽"。
    """

    def __init__(self, team: team_pb2.TeamStorageRecord | None) -> None:
        self.team = team
        self.commits = 0
        self.open_syncs: list[tuple] = []
        self.deleted_index: list[tuple[int, int]] = []

    async def get(self, team_id: int):  # noqa: ANN201
        if self.team is None or self.team.team_id != team_id:
            return None, False
        return self.team, True

    async def update_with_lock(self, team_id, retry, fn, ttl):  # noqa: ANN001
        if self.team is None or self.team.team_id != team_id:
            raise errcode.PandoraError(
                errcode.ErrTeamNotFound, "team %d not found", team_id
            )
        # 真实实现是"改一份反序列化出来的副本,成功才 SET 回去"。这里改草稿再整体
        # 覆盖,保证 fn 抛异常时队伍**一个字节都不动** —— 直接改 self.team 的话,
        # 一个抛哨兵的用例会留下半改的记录,把后续断言变成假绿。
        draft = team_pb2.TeamStorageRecord()
        draft.CopyFrom(self.team)
        fn(draft)
        self.team = draft
        self.commits += 1

    async def sync_open_team(self, *args) -> None:  # noqa: ANN002
        self.open_syncs.append(args)

    async def expire_team(self, team_id: int, ttl_sec: float) -> None:
        pass

    async def delete_player_index_if_matches(self, player_id: int, team_id: int) -> None:
        self.deleted_index.append((player_id, team_id))

    async def get_player_team_id(self, player_id: int):  # noqa: ANN201
        if self.team is None:
            return 0, False
        for m in self.team.members:
            if m.player_id == player_id:
                return self.team.team_id, True
        return 0, False


def _cfg() -> tconf.TeamConf:
    cfg = tconf.Config()
    cfg.apply_defaults()
    return cfg.team


def _team(
    *,
    state: int = team_pb2.TEAM_STATE_READY,
    ready: bool = True,
    gen: int = 5,
    members: tuple[int, ...] = (CAPTAIN, MEMBER),
) -> team_pb2.TeamStorageRecord:
    rec = team_pb2.TeamStorageRecord(
        team_id=TEAM_ID,
        captain_id=CAPTAIN,
        state=state,
        ready_generation=gen,
        max_size=5,
        map_id=4002,
    )
    for pid in members:
        rec.members.add(player_id=pid, ready=ready, hero_id=7)
    return rec


def _uc(repo: FakeRepo) -> tbiz.TeamUsecase:
    # pusher=None:显式无 kafka 开发模式,推送整条跳过(本组验的是队伍状态机,不是推送)。
    return tbiz.TeamUsecase(repo, None, _cfg())


# ── begin_team_match:正常冻结 ───────────────────────────────────────────────


async def test_begin_freezes_roster_then_clears_ready() -> None:
    """★ 顺序不能反:先留冻结快照,再清 ready。

    反了的话快照里的 ready 位全是假的 —— PRE_READY 图靠它对账、存量客户端靠它显示,
    而 matchmaker 建票时不会发现任何异常。
    """
    repo = FakeRepo(_team())
    uc = _uc(repo)

    snapshot, expires_at_ms = await uc.begin_team_match(
        TEAM_ID, CAPTAIN, "op-1", 5000, True
    )

    # 快照 = 冻结那一刻(全员 ready)
    assert [m.ready for m in snapshot.members] == [True, True]
    assert snapshot.ready_generation == 5
    # 落库 = 已消费(ready 清空 + 转 FORMING + 代际推进 + 上租约)
    assert [m.ready for m in repo.team.members] == [False, False]
    assert repo.team.state == team_pb2.TEAM_STATE_FORMING
    assert repo.team.ready_generation == 6
    assert repo.team.match_lock_operation_id == "op-1"
    assert repo.team.match_lock_until_ms == expires_at_ms


async def test_begin_receipt_records_post_consume_generation() -> None:
    """★ 收据记的是**消费后**代际 —— 它是 receipt_reentry 的 CAS 依据。

    记成消费前的值,重入判定会恒不成立(每次重试都重新消费一次 ready)。
    """
    repo = FakeRepo(_team(gen=5))
    await _uc(repo).begin_team_match(TEAM_ID, CAPTAIN, "op-1", 5000, True)

    rec = repo.team.match_start_receipt
    assert rec.attempt_id == "op-1"
    assert rec.consumed_ready_generation == 5
    assert rec.post_ready_generation == 6 == repo.team.ready_generation
    # 冻结名单原样存进收据(重入时要拿它重建"冻结那一刻")
    assert [m.ready for m in rec.roster] == [True, True]


@pytest.mark.parametrize(
    ("lease_ms", "want"),
    [
        (0, toffline.MATCH_LOCK_MIN_LEASE_MS),  # 误配成 0 = 锁瞬间失效,必须抬到下限
        (1, toffline.MATCH_LOCK_MIN_LEASE_MS),
        (5000, 5000),
        (999_999, toffline.MATCH_LOCK_MAX_LEASE_MS),  # 上限 = matchmaker 崩了最多拖多久
    ],
)
async def test_lease_is_clamped(lease_ms: int, want: int) -> None:
    repo = FakeRepo(_team())
    _snapshot, expires = await _uc(repo).begin_team_match(
        TEAM_ID, CAPTAIN, "op-1", lease_ms, True
    )
    assert expires - repo.team.updated_at_ms == want


# ── begin_team_match:重入(响应丢失后的重试)────────────────────────────────


async def test_begin_reentry_returns_frozen_roster_without_reconsuming() -> None:
    """★ 同一 attempt 的重试必须拿回**收据里那份**名单,且不再消费一次 ready。

    直接返回当前记录 = 把一份空 ready 的名单交给 matchmaker 建票;
    重新走冻结路径 = 同一次点击消费了两次 ready(代际白涨,EndTeamMatch 的 CAS 落空)。
    """
    repo = FakeRepo(_team(gen=5))
    uc = _uc(repo)
    first, _ = await uc.begin_team_match(TEAM_ID, CAPTAIN, "op-1", 5000, True)
    gen_after_first = repo.team.ready_generation
    commits_after_first = repo.commits

    second, _ = await uc.begin_team_match(TEAM_ID, CAPTAIN, "op-1", 5000, True)

    assert [m.player_id for m in second.members] == [m.player_id for m in first.members]
    assert [m.ready for m in second.members] == [True, True]
    assert second.ready_generation == first.ready_generation
    # 重入只续租约,不再动 ready → 代际不变
    assert repo.team.ready_generation == gen_after_first
    assert repo.commits == commits_after_first + 1  # 写了(续租约),但没推进代际


async def test_begin_reentry_survives_captain_transfer() -> None:
    """★ 重入判定必须**先于**队长校验。

    对一次已提交成功的操作重新做授权判定,正是端到端幂等被打破的经典形状(§9.23):
    队长恰在重试间隙转移,重试就会被 3003 拒掉,而那次组票其实早已成功。
    """
    repo = FakeRepo(_team())
    uc = _uc(repo)
    await uc.begin_team_match(TEAM_ID, CAPTAIN, "op-1", 5000, True)
    repo.team.captain_id = MEMBER  # 队长转移

    snapshot, _ = await uc.begin_team_match(TEAM_ID, CAPTAIN, "op-1", 5000, True)
    assert [m.ready for m in snapshot.members] == [True, True]


async def test_begin_reentry_expires_after_window() -> None:
    """★ 重入窗有上界:很久以后的真实点击必须走正常冻结路径,不能永远命中重入。"""
    repo = FakeRepo(_team())
    uc = _uc(repo)
    await uc.begin_team_match(TEAM_ID, CAPTAIN, "op-1", 5000, True)
    # 把收据与租约都推到窗外(不用 sleep:直接改冻结时刻,与真实时钟无关)。
    repo.team.match_start_receipt.created_at_ms -= (
        toffline.MATCH_START_RECEIPT_WINDOW_MS + 1
    )
    repo.team.match_lock_until_ms = 0
    gen_before = repo.team.ready_generation

    # 队伍此刻是 FORMING(上一次已消费),PRE_READY 图会拒 —— 正是"走了正常冻结路径"
    # 的证据(重入路径不看 State)。
    with pytest.raises(errcode.PandoraError) as excinfo:
        await uc.begin_team_match(TEAM_ID, CAPTAIN, "op-1", 5000, True)
    assert excinfo.value.code == errcode.ErrTeamWrongState
    assert repo.team.ready_generation == gen_before


# ── begin_team_match:两种 ready 模式(关卡表 ready_mode)────────────────────


async def test_pre_ready_map_rejects_forming_team() -> None:
    """require_ready=True(PRE_READY 图):队伍不是 READY 就拒。"""
    repo = FakeRepo(_team(state=team_pb2.TEAM_STATE_FORMING, ready=False))
    with pytest.raises(errcode.PandoraError) as excinfo:
        await _uc(repo).begin_team_match(TEAM_ID, CAPTAIN, "op-1", 5000, True)
    assert excinfo.value.code == errcode.ErrTeamWrongState


async def test_post_confirm_map_accepts_forming_team() -> None:
    """require_ready=False(POST_CONFIRM 图 / 旧 matchmaker 不发该字段):FORMING 也放行。

    §9.21 共存安全:该字段上线前的调用方不发它 → False → 行为与上线前逐字节一致。
    """
    repo = FakeRepo(_team(state=team_pb2.TEAM_STATE_FORMING, ready=False))
    snapshot, _ = await _uc(repo).begin_team_match(
        TEAM_ID, CAPTAIN, "op-1", 5000, False
    )
    assert len(snapshot.members) == 2


async def test_begin_rejects_non_captain() -> None:
    repo = FakeRepo(_team())
    with pytest.raises(errcode.PandoraError) as excinfo:
        await _uc(repo).begin_team_match(TEAM_ID, MEMBER, "op-1", 5000, True)
    assert excinfo.value.code == errcode.ErrTeamNotCaptain


async def test_begin_rejects_missing_args() -> None:
    repo = FakeRepo(_team())
    with pytest.raises(errcode.PandoraError) as excinfo:
        await _uc(repo).begin_team_match(TEAM_ID, CAPTAIN, "", 5000, True)
    assert excinfo.value.code == errcode.ErrInvalidArg


async def test_begin_rejects_lease_held_by_another_operation() -> None:
    """另一次组票的租约还没到期 = 正常竞争,回可重试错误(3007),几秒后自净。"""
    repo = FakeRepo(_team())
    uc = _uc(repo)
    await uc.begin_team_match(TEAM_ID, CAPTAIN, "op-1", 5000, True)
    # 让 op-2 的重入判定不成立(代际已变),只剩租约冲突这一条。
    repo.team.ready_generation += 1
    with pytest.raises(errcode.PandoraError) as excinfo:
        await uc.begin_team_match(TEAM_ID, CAPTAIN, "op-2", 5000, False)
    assert excinfo.value.code == errcode.ErrTeamConcurrent


# ── end_team_match:INC-20260813-001 的第一根因 ──────────────────────────────


async def test_end_team_match_resets_ready_after_a_game() -> None:
    """★ 打完一局必须复位 ready —— 事故当天 team 侧根本没有这条路径。

    不复位的话队伍仍停在 READY,队长能在队友还没回大厅时用残留 ready 立刻再开一局,
    缺席者被原样冻进票据(3v3 打成 3v2)。
    """
    repo = FakeRepo(_team(gen=9))
    uc = _uc(repo)

    await uc.end_team_match(TEAM_ID, [CAPTAIN, MEMBER], 9)

    assert [m.ready for m in repo.team.members] == [False, False]
    assert repo.team.state == team_pb2.TEAM_STATE_FORMING
    assert repo.team.ready_generation == 10  # ready 意图变了 → 代际推进


async def test_end_team_match_replay_is_blocked_by_generation_cas() -> None:
    """★ outbox 重投必须被跨代 CAS 挡住,而不是把新意图抹平。

    ACK 丢失后玩家重新点了准备 / 队长已开新局 —— 光看「谁还挂着 ready」是**不够**的,
    重投照样会清掉那份新的准备。
    """
    repo = FakeRepo(_team(gen=9))
    uc = _uc(repo)
    await uc.end_team_match(TEAM_ID, [CAPTAIN, MEMBER], 9)

    # 玩家回大厅后重新点了准备(模拟:ready 位与代际都往前走)
    for m in repo.team.members:
        m.ready = True
    repo.team.state = team_pb2.TEAM_STATE_READY
    repo.team.ready_generation = 12
    commits_before = repo.commits

    await uc.end_team_match(TEAM_ID, [CAPTAIN, MEMBER], 9)  # 迟到重投,带旧代际

    assert [m.ready for m in repo.team.members] == [True, True]  # 新意图没被抹掉
    assert repo.team.ready_generation == 12
    assert repo.commits == commits_before  # 一次写都没发生


async def test_end_team_match_legacy_zero_generation_still_resets_once() -> None:
    """expected_gen=0(滚动升级窗口的旧 matchmaker)退化为「还挂着就清一次」。

    不是跨代安全的,但**严格优于完全不复位**;调用方为此打 team_match_end_legacy_generation。
    """
    repo = FakeRepo(_team(gen=9))
    await _uc(repo).end_team_match(TEAM_ID, [CAPTAIN, MEMBER], 0)
    assert [m.ready for m in repo.team.members] == [False, False]


async def test_end_team_match_is_idempotent_when_already_cleared() -> None:
    """已复位过再调 = 零写零推送(outbox 会重投到成功为止)。"""
    repo = FakeRepo(_team(state=team_pb2.TEAM_STATE_FORMING, ready=False, gen=9))
    commits_before = repo.commits
    await _uc(repo).end_team_match(TEAM_ID, [CAPTAIN, MEMBER], 9)
    assert repo.commits == commits_before


async def test_end_team_match_on_missing_team_is_success() -> None:
    """队伍已不存在 = 本就没什么可复位的,**返回成功**。

    报错只会让 outbox 永远重试下去。
    """
    repo = FakeRepo(None)
    await _uc(repo).end_team_match(TEAM_ID, [CAPTAIN], 9)  # 不抛


async def test_end_team_match_defers_while_roster_locked() -> None:
    """★ 组票租约在手 = 队长已经开了下一局,这时改 ready 会与那一局的冻结名单打架。

    回 3007(暂态)让上游 outbox 下一轮重投,而不是硬清。
    """
    repo = FakeRepo(_team(gen=9))
    uc = _uc(repo)
    await uc.begin_team_match(TEAM_ID, CAPTAIN, "op-next", 5000, True)
    gen_now = repo.team.ready_generation
    for m in repo.team.members:  # 造出"还挂着 ready"以便走到租约判定
        m.ready = True

    with pytest.raises(errcode.PandoraError) as excinfo:
        await uc.end_team_match(TEAM_ID, [CAPTAIN, MEMBER], gen_now)
    assert excinfo.value.code == errcode.ErrTeamConcurrent
    assert [m.ready for m in repo.team.members] == [True, True]


async def test_end_team_match_rejects_zero_team_id() -> None:
    repo = FakeRepo(_team())
    with pytest.raises(errcode.PandoraError) as excinfo:
        await _uc(repo).end_team_match(0, [CAPTAIN], 9)
    assert excinfo.value.code == errcode.ErrInvalidArg


# ── 离线软化 / 摘人与租约的互斥 ─────────────────────────────────────────────


class _StubCommitment:
    """matchmaker 权威读的假件。committed 为异常时表示"读不确定"。"""

    def __init__(self, committed: object = False) -> None:
        self.committed = committed
        self.calls: list[int] = []

    async def is_player_committed_to_match(self, player_id: int) -> bool:
        self.calls.append(player_id)
        if isinstance(self.committed, BaseException):
            raise self.committed
        return bool(self.committed)


def _uc_offline(repo: FakeRepo, commitment: _StubCommitment) -> tbiz.TeamUsecase:
    cfg = tconf.Config()
    cfg.team.offline_leave.enabled = True
    cfg.apply_defaults()
    uc = tbiz.TeamUsecase(repo, None, cfg.team)
    uc.set_match_commitment_reader(commitment)
    return uc


async def test_offline_leave_is_off_without_commitment_reader() -> None:
    """★ 配置开了但依赖没注入(装配漏了)一律按关处理,不走半截逻辑。

    少了 match_commitment 就没有闸②③,自动摘人会有拆掉正在打的队伍的风险。
    """
    cfg = tconf.Config()
    cfg.team.offline_leave.enabled = True
    cfg.apply_defaults()
    uc = tbiz.TeamUsecase(FakeRepo(_team()), None, cfg.team)
    assert uc.offline_leave_enabled() is False


async def test_offline_leave_removes_member_and_transfers_captain() -> None:
    repo = FakeRepo(_team(ready=False, state=team_pb2.TEAM_STATE_FORMING))
    uc = _uc_offline(repo, _StubCommitment(False))

    await uc.on_player_offline(CAPTAIN, 1)

    assert [m.player_id for m in repo.team.members] == [MEMBER]
    assert repo.team.captain_id == MEMBER  # 否则队伍永远卡在"队长不在"
    assert (CAPTAIN, TEAM_ID) in repo.deleted_index
    # ★ 探针:证明真的走过了闸②(整队占用 → 查队长)与闸③(本人占用 → 查本人)。
    # 本例里被摘的就是队长,两道闸都会查同一个 id —— 少任一道 count 就掉到 1。
    # 不加这条探针的话,"人被摘了"这个结果分辨不出闸有没有被跳过,而跳过的后果是
    # 有可能拆掉一支正在打的队伍。
    assert uc.match_commitment.calls.count(CAPTAIN) >= 2


async def test_offline_leave_defers_when_roster_locked() -> None:
    """★ 与 matchmaker 的共同线性化点:租约在手就推迟,绝不造出「人在票据里、不在队伍里」。"""
    repo = FakeRepo(_team())
    uc = _uc_offline(repo, _StubCommitment(False))
    await uc.begin_team_match(TEAM_ID, CAPTAIN, "op-1", 15000, True)

    with pytest.raises(offlinewatch.DeferredError):
        await uc.on_player_offline(MEMBER, 1)
    assert [m.player_id for m in repo.team.members] == [CAPTAIN, MEMBER]


async def test_offline_leave_fail_closed_when_commitment_unknown() -> None:
    """★ 读不确定必须 fail-closed(抛错重试),绝不能因为 matchmaker 抖一下就拆队伍。"""
    repo = FakeRepo(_team())
    boom = RuntimeError("matchmaker unreachable")
    uc = _uc_offline(repo, _StubCommitment(boom))

    with pytest.raises(RuntimeError):
        await uc.on_player_offline(MEMBER, 1)
    assert len(repo.team.members) == 2


async def test_offline_leave_skips_single_member_team() -> None:
    """单人队不动:没有队友受影响,摘掉他等于解散,留给 active_ttl 自然回收。"""
    repo = FakeRepo(_team(members=(CAPTAIN,)))
    uc = _uc_offline(repo, _StubCommitment(False))
    await uc.on_player_offline(CAPTAIN, 1)
    assert len(repo.team.members) == 1


async def test_presence_lost_clears_ready_but_keeps_member() -> None:
    """★ 软档只动准备状态,人留在队伍里 —— 留人(为了重连)与可开局是两个判断。"""
    repo = FakeRepo(_team(gen=3))
    uc = _uc_offline(repo, _StubCommitment(False))

    await uc.on_player_presence_lost(MEMBER, 1)

    assert [m.player_id for m in repo.team.members] == [CAPTAIN, MEMBER]
    assert [m.ready for m in repo.team.members] == [True, False]
    assert repo.team.state == team_pb2.TEAM_STATE_FORMING
    assert repo.team.ready_generation == 4


async def test_presence_lost_is_write_free_when_nothing_changes() -> None:
    """★ 每轮 Observe 都会来一次:没有实际变化就必须零写零推送。

    否则一个挂机离线的玩家会让他所在队伍每 15s 白写一次 Redis 并广播一次无意义推送。
    """
    repo = FakeRepo(_team(ready=False, state=team_pb2.TEAM_STATE_FORMING))
    uc = _uc_offline(repo, _StubCommitment(False))
    commits_before = repo.commits
    await uc.on_player_presence_lost(MEMBER, 1)
    assert repo.commits == commits_before
