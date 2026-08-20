"""ready 代际测试(INC-20260813-001 ①)。

这一组测的是一个**静默失效**的防线:代际漏推进时,CAS 照样通过、幂等保护形同虚设,
而所有常规测试仍然绿。所以测试必须直接盯"指纹该变的时候变了没、不该变的时候没变"。

重点:
  1. ★ ready 位 / 成员集合 / 队伍状态变化 → 必须推进代际
  2. ★ map_id / 队长 / 昵称 / 英雄变化 → **不能**推进(否则该复位的反而不复位)
  3. ★ 成员顺序变化 → **不能**推进(存储顺序会因增删而变)
  4. ★ 业务回调抛异常 → 代际不动
  5. ★ stamp 在代际推进**之后**运行
"""

from __future__ import annotations

import dataclasses

import pytest

from pandorapy.services.team import ready_generation as rg


@dataclasses.dataclass
class Member:
    player_id: int
    ready: bool = False
    nickname: str = ""
    hero_id: int = 0


@dataclasses.dataclass
class Team:
    state: int = 1
    members: list = dataclasses.field(default_factory=list)
    ready_generation: int = 0
    map_id: int = 0
    leader_id: int = 0


class FakeRepo:
    """最小 repo:直接把回调作用在持有的 team 上。"""

    def __init__(self, team: Team) -> None:
        self.team = team
        self.commits = 0

    async def update_with_lock(self, team_id, retry, fn, ttl):  # noqa: ANN001
        fn(self.team)
        self.commits += 1


def _team(*specs: tuple[int, bool], state: int = 1, gen: int = 5) -> Team:
    return Team(
        state=state,
        members=[Member(player_id=p, ready=r) for p, r in specs],
        ready_generation=gen,
    )


async def _update(team: Team, fn, stamp=None) -> Team:
    repo = FakeRepo(team)
    await rg.update_team(repo, 1, fn, optimistic_retry=3, ttl=None, stamp=stamp)
    return repo.team


# ── ★ 该推进的 ────────────────────────────────────────────────────────────


async def test_ready_flag_change_advances_generation() -> None:
    """★ ready 位变化必须推进 —— 这是代际存在的核心理由。"""
    team = await _update(_team((1, False), (2, True)), lambda t: setattr(t.members[0], "ready", True))
    assert team.ready_generation == 6


async def test_member_join_advances_generation() -> None:
    """入队 = 意图变了(新人还没准备),必须推进。"""
    team = await _update(
        _team((1, True), (2, True)),
        lambda t: t.members.append(Member(player_id=3, ready=False)),
    )
    assert team.ready_generation == 6


async def test_member_leave_advances_generation() -> None:
    """离队同理 —— 否则一条迟到的旧局释放会把"剩下的人"的新准备抹掉。"""
    team = await _update(_team((1, True), (2, True)), lambda t: t.members.pop(0))
    assert team.ready_generation == 6


async def test_state_change_advances_generation() -> None:
    """队伍状态(FORMING / READY / MATCHING)变化必须推进。"""
    team = await _update(_team((1, True), state=1), lambda t: setattr(t, "state", 2))
    assert team.ready_generation == 6


# ── ★ 不该推进的(这一组比上一组更容易写错)────────────────────────────────


async def test_map_id_change_does_not_advance() -> None:
    """★ map_id 变了**不影响**"这一局该不该被复位"。

    算进指纹会让代际无谓地涨 → 正常的 EndTeamMatch 频繁 CAS 失败 →
    **该复位的反而不复位**,而这个失败是静默的(看起来只是"偶尔没清干净")。
    """
    team = await _update(_team((1, True)), lambda t: setattr(t, "map_id", 4002))
    assert team.ready_generation == 5, "map_id 变化推进了代际"


async def test_leader_change_does_not_advance() -> None:
    team = await _update(_team((1, True)), lambda t: setattr(t, "leader_id", 999))
    assert team.ready_generation == 5


async def test_nickname_and_hero_change_do_not_advance() -> None:
    """昵称 / 英雄是展示与编成字段,与"谁准备好了"无关。"""

    def touch(t):
        t.members[0].nickname = "新名字"
        t.members[0].hero_id = 77

    team = await _update(_team((1, True)), touch)
    assert team.ready_generation == 5


async def test_member_reorder_does_not_advance() -> None:
    """★ 成员顺序变化**不能**推进。

    Members 的存储顺序会因增删而变;不排序就会把"顺序变了"误判成"意图变了",
    代际虚涨、复位失效。
    """
    team = await _update(
        _team((1, True), (2, False), (3, True)),
        lambda t: t.members.reverse(),
    )
    assert team.ready_generation == 5, "成员顺序变化推进了代际 —— 指纹没排序"


async def test_no_change_does_not_advance() -> None:
    """纯读改写但什么都没变 → 不推进。"""
    team = await _update(_team((1, True)), lambda t: None)
    assert team.ready_generation == 5


# ── ★ 异常与 stamp ─────────────────────────────────────────────────────────


async def test_callback_error_leaves_generation_untouched() -> None:
    """业务回调抛异常 → 整次写放弃,代际不动。"""

    def boom(t):
        t.members[0].ready = True
        raise RuntimeError("业务拒绝")

    team = _team((1, False))
    with pytest.raises(RuntimeError):
        await _update(team, boom)
    # FakeRepo 没有真回滚,但代际推进发生在 fn 之后 —— 抛了就到不了那一步
    assert team.ready_generation == 5


async def test_stamp_runs_after_generation_advance() -> None:
    """★ stamp 必须看到**推进之后**的代际。

    在 fn 里写收据只能靠「当前值 + 1」去猜 —— 那是在复刻内部实现,
    推进规则一变收据就静默记错一个永远对不上的值,而重入判定失效**不报错**。
    """
    seen: list[int] = []
    team = await _update(
        _team((1, False)),
        lambda t: setattr(t.members[0], "ready", True),
        stamp=lambda t: seen.append(t.ready_generation),
    )
    assert seen == [6], f"stamp 看到的代际是 {seen},不是推进后的 6"
    assert team.ready_generation == 6


async def test_stamp_runs_even_when_nothing_changed() -> None:
    """没变化时 stamp 仍然运行(它只是盖章,不依赖有没有推进)。"""
    seen: list[int] = []
    await _update(_team((1, True)), lambda t: None, stamp=lambda t: seen.append(t.ready_generation))
    assert seen == [5]


# ── ★ 跨代 CAS 判据 ────────────────────────────────────────────────────────


def test_generation_cas_rejects_stale_retry() -> None:
    """★ 重投带的 expected 对不上当前代际 → no-op。

    这正是"ACK 丢失 → 玩家重新点准备 → outbox 重投把新意图抹平"的防线。
    """
    assert rg.generation_matches(current=7, expected=7)
    assert not rg.generation_matches(current=8, expected=7)


def test_unknown_generation_degrades_to_allow_once() -> None:
    """expected==0(滚动升级窗口的旧调用方)→ 放行一次。

    ★ 不是跨代安全的,但**严格优于完全不复位**。调用方必须为此打 WARN。
    """
    assert rg.generation_matches(current=99, expected=0)


# ── 指纹本身 ────────────────────────────────────────────────────────────────


def test_fingerprint_is_order_independent() -> None:
    a = _team((3, True), (1, False), (2, True))
    b = _team((1, False), (2, True), (3, True))
    assert rg.ready_intent_fingerprint(a) == rg.ready_intent_fingerprint(b)


def test_fingerprint_distinguishes_ready_bits() -> None:
    a = _team((1, True), (2, False))
    b = _team((1, False), (2, True))
    assert rg.ready_intent_fingerprint(a) != rg.ready_intent_fingerprint(b)


def test_fingerprint_of_none_is_empty() -> None:
    assert rg.ready_intent_fingerprint(None) == ""
