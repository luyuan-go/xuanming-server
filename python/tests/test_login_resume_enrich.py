"""GetResumeContext 的撮合富化 + 首次进场链回归。

★ 这两条都是**整段没移植**的 Go 逻辑,2026-08-23 用真客户端实测暴露:

    LogMyDsRecoveryCoordinator: Error: TARGET carried an incomplete owner identity;
      retrying fail-closed: route=2 ... hub_assign=0 alloc=1 match=0
    → 连续 3 拍同 owner 身份后 reason=authority_entry_terminal,弹回登录界面

  (那行里的 hub_assign=0 是**合法**的:客户端 `HasCompleteOwnerIdentity` 要求
  hub_assignment_id 与 allocation_id 按 route 严格二选一,BATTLE 路由下前者本就该空。
  真正缺的只有 match_id 一项。)

  owner 权威**不回答 match_id**(`apply_owner_placement` 不设,Go 的
  `applyOwnerPlacement` 同样不设 —— 这是两栈共同设计,别当缺陷去改 owner)。
  Go 靠 `enrichResumeFromMatchAuthority` 从撮合权威补上,Python 侧当时零命中。

  被踢回登录后再登录还会卡死在第二个洞上:owner 说"无归属"时 Python 回一个裸
  `ResumeContextResult()`,客户端判 `incomplete authoritative entry contract` 无限重查
  —— Go 那里是 `resolveFirstEntry`(角色门 → 分配首个 Hub → 回查 TARGET)。

所以每条用例都写成"**把对应代码删掉就会红**"的形状,尤其是
`test_battle_target_gets_match_id_filled`:它就是那次"匹配进不去副本"的最小复现。
"""

from __future__ import annotations

import pytest

from pandora.login.v1 import login_pb2 as _R
from pandora.match.v1 import match_pb2

from pandorapy.services.login import biz as lbiz
from pandorapy.services.login import clients as lclients

PLAYER = 27402587133214721
SESS_JTI = "sess-jti-1"


# ── 装配 ────────────────────────────────────────────────────────────────────


class _Resolver:
    """matchmaker 只读权威的替身。`exc` 非空时模拟权威不可达。"""

    def __init__(self, ma=None, exc: BaseException | None = None) -> None:
        self.ma = ma
        self.exc = exc
        self.calls = 0

    async def resolve_player_match_context(self, player_id: int):
        self.calls += 1
        if self.exc is not None:
            raise self.exc
        return self.ma


def _ma(state=match_pb2.PLAYER_MATCH_CONTEXT_STATE_ACTIVE, *, stage=0, match_id=0,
        game_mode="", map_id=0) -> lclients.PlayerMatchAuthority:
    return lclients.PlayerMatchAuthority(
        state=state, stage=stage, match_id=match_id, game_mode=game_mode, map_id=map_id
    )


def _uc(*, resolver=None, role_repo=None) -> lbiz.LoginUsecase:
    """只装配被测路径用得到的字段。LoginUsecase 没有 __slots__,可直接 __new__。"""
    uc = lbiz.LoginUsecase.__new__(lbiz.LoginUsecase)
    uc._match_resolver = resolver
    uc._role_repo = role_repo
    uc._owner = None
    return uc


def _battle_target(**over) -> lbiz.ResumeContextResult:
    """owner 刚给出的 BATTLE TARGET:身份齐全,但**没有 match_id**(owner 不答这个)。"""
    out = lbiz.ResumeContextResult()
    out.route = _R.RESUME_ROUTE_BATTLE
    out.entry_state = _R.RESUME_ENTRY_STATE_TARGET
    out.owner_epoch = 5
    out.operation_id = "op-1"
    out.ds_pod_name = "pandora-battle-local-1"
    out.ds_instance_uid = "uid-1"
    out.ds_instance_epoch = 1
    out.release_track = "stable"
    out.allocation_id = "alloc-1"
    for k, v in over.items():
        setattr(out, k, v)
    return out


# ── 撮合富化 ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_battle_target_gets_match_id_filled():
    """owner 的 BATTLE TARGET 缺 match_id → 必须由撮合权威补齐。

    这就是"匹配成功却进不去副本"的最小复现:客户端 `HasCompleteOwnerTarget` 对
    route=BATTLE 硬要求 match_id > 0,缺了就判 TARGET 残缺并最终踢回登录。
    """
    uc = _uc(resolver=_Resolver(_ma(
        stage=match_pb2.PLAYER_MATCH_RESUME_STAGE_READY,
        match_id=27406903575379968, game_mode="pve_coop", map_id=14,
    )))
    out = await uc._enrich_resume_from_match_authority(PLAYER, _battle_target())
    assert out.match_id == 27406903575379968
    assert out.match_stage == _R.RESUME_MATCH_STAGE_READY
    assert out.game_mode == "pve_coop"
    assert out.map_id == 14
    # 归属维度一个字都不许动 —— 撮合权威不回答"归谁管"。
    assert out.route == _R.RESUME_ROUTE_BATTLE
    assert out.entry_state == _R.RESUME_ENTRY_STATE_TARGET
    assert out.owner_epoch == 5
    assert out.operation_id == "op-1"
    assert out.allocation_id == "alloc-1"
    assert out.hub_assignment_id == ""


@pytest.mark.asyncio
async def test_enrich_never_overwrites_existing_fields():
    """已有值一概不覆盖:owner / battle 重连链填的才是准的,撮合只补空缺。"""
    uc = _uc(resolver=_Resolver(_ma(
        stage=match_pb2.PLAYER_MATCH_RESUME_STAGE_QUEUED,
        match_id=999, game_mode="pvp_rank", map_id=88,
    )))
    src = _battle_target(match_id=111, match_stage=_R.RESUME_MATCH_STAGE_RUNNING,
                         game_mode="pve_coop", map_id=14)
    out = await uc._enrich_resume_from_match_authority(PLAYER, src)
    assert (out.match_id, out.game_mode, out.map_id) == (111, "pve_coop", 14)
    assert out.match_stage == _R.RESUME_MATCH_STAGE_RUNNING


@pytest.mark.asyncio
@pytest.mark.parametrize("state", [
    match_pb2.PLAYER_MATCH_CONTEXT_STATE_NONE,
    match_pb2.PLAYER_MATCH_CONTEXT_STATE_UNSPECIFIED,
])
async def test_enrich_ignores_non_active_claim(state):
    """只有活跃 claim 才可恢复;终态 / 漂移记录不得带给客户端。"""
    uc = _uc(resolver=_Resolver(_ma(state, match_id=777, game_mode="pve_coop", map_id=14)))
    out = await uc._enrich_resume_from_match_authority(PLAYER, _battle_target())
    assert out.match_id == 0 and out.game_mode == "" and out.map_id == 0


@pytest.mark.asyncio
async def test_enrich_degrades_when_authority_unavailable():
    """撮合权威不可达 = 字段缺失降级,**不得**影响 owner 已定的进场判定。"""
    uc = _uc(resolver=_Resolver(exc=RuntimeError("matchmaker down")))
    out = await uc._enrich_resume_from_match_authority(PLAYER, _battle_target())
    assert out.entry_state == _R.RESUME_ENTRY_STATE_TARGET
    assert out.route == _R.RESUME_ROUTE_BATTLE
    assert out.match_id == 0


@pytest.mark.asyncio
async def test_enrich_noop_without_resolver():
    """未配 matchmaker(裸跑)→ 原样返回,不抛。"""
    uc = _uc(resolver=None)
    out = await uc._enrich_resume_from_match_authority(PLAYER, _battle_target())
    assert out.entry_state == _R.RESUME_ENTRY_STATE_TARGET


# ── 首次进场链 ──────────────────────────────────────────────────────────────


def _first_entry_uc(*, role_id=1004, role_repo=object(), hub_exc=None,
                    owner=None, resolver=None):
    uc = _uc(resolver=resolver, role_repo=role_repo)
    calls = {"hub": 0}

    async def _load_selected_role(player_id):
        if isinstance(role_id, BaseException):
            raise role_id
        return role_id

    async def resolve_hub_endpoint(player_id, sess_jti):
        calls["hub"] += 1
        if hub_exc is not None:
            raise hub_exc
        return ("127.0.0.1:7777", "ticket", 0)

    async def _resolve_resume_from_owner(player_id):
        return owner if owner is not None else (False, lbiz.ResumeContextResult())

    uc._load_selected_role = _load_selected_role
    uc.resolve_hub_endpoint = resolve_hub_endpoint
    uc._resolve_resume_from_owner = _resolve_resume_from_owner
    return uc, calls


@pytest.mark.asyncio
async def test_first_entry_role_required_does_not_assign_hub():
    """role=0 → ROLE_REQUIRED,且**绝不**分配 Hub / 占座 / 签票。"""
    uc, calls = _first_entry_uc(role_id=0)
    out = await uc._resolve_first_entry(PLAYER, SESS_JTI)
    assert out.entry_state == _R.RESUME_ENTRY_STATE_ROLE_REQUIRED
    assert out.route == _R.RESUME_ROUTE_HUB
    assert calls["hub"] == 0


@pytest.mark.asyncio
async def test_first_entry_role_unknown_waits_not_pretends_unselected():
    """角色权威不可判定 → WAIT/ROLE_UNKNOWN,不得冒充"未选角"。"""
    uc, calls = _first_entry_uc(role_id=RuntimeError("role authority down"))
    out = await uc._resolve_first_entry(PLAYER, SESS_JTI)
    assert out.entry_state == _R.RESUME_ENTRY_STATE_WAIT
    assert out.wait_reason == _R.RESUME_WAIT_REASON_ROLE_UNKNOWN
    assert calls["hub"] == 0


@pytest.mark.asyncio
async def test_first_entry_hub_assign_failure_waits_with_retry():
    uc, _ = _first_entry_uc(hub_exc=RuntimeError("no capacity"))
    out = await uc._resolve_first_entry(PLAYER, SESS_JTI)
    assert out.entry_state == _R.RESUME_ENTRY_STATE_WAIT
    assert out.wait_reason == _R.RESUME_WAIT_REASON_NO_CAPACITY
    assert out.retry_after_ms > 0


@pytest.mark.asyncio
async def test_first_entry_assigns_then_returns_enriched_target():
    """已选角无归属 → 分配首个 Hub → 回查 owner 给 exact TARGET,并叠加撮合字段。"""
    hub_target = lbiz.ResumeContextResult()
    hub_target.route = _R.RESUME_ROUTE_HUB
    hub_target.entry_state = _R.RESUME_ENTRY_STATE_TARGET
    hub_target.owner_epoch = 4
    hub_target.hub_assignment_id = "assign-1"
    uc, calls = _first_entry_uc(
        owner=(True, hub_target),
        resolver=_Resolver(_ma(stage=match_pb2.PLAYER_MATCH_RESUME_STAGE_QUEUED,
                               match_id=42, game_mode="pve_coop", map_id=14)),
    )
    out = await uc._resolve_first_entry(PLAYER, SESS_JTI)
    assert calls["hub"] == 1
    assert out.entry_state == _R.RESUME_ENTRY_STATE_TARGET
    assert out.route == _R.RESUME_ROUTE_HUB
    assert out.hub_assignment_id == "assign-1"
    assert (out.match_id, out.game_mode, out.map_id) == (42, "pve_coop", 14)


@pytest.mark.asyncio
async def test_first_entry_owner_missing_after_assign_waits():
    """刚分配完却查不到归属 = 不自洽:fail-closed 回 WAIT,不冒充 TARGET。"""
    uc, calls = _first_entry_uc(owner=(False, lbiz.ResumeContextResult()))
    out = await uc._resolve_first_entry(PLAYER, SESS_JTI)
    assert calls["hub"] == 1
    assert out.entry_state == _R.RESUME_ENTRY_STATE_WAIT
    assert out.wait_reason == _R.RESUME_WAIT_REASON_OWNER_UNKNOWN


# ── get_resume_context 接线 ─────────────────────────────────────────────────


class _Signer:
    def verify(self, token, expect_audience=None):
        return {"sub": str(PLAYER), "jti": SESS_JTI}


def _resume_uc(owner, *, resolver=None):
    uc = _uc(resolver=resolver)
    uc._signer = _Signer()
    uc._signer_audience = lambda: "pandora-client"

    async def _resolve_resume_from_owner(player_id):
        return owner

    uc._resolve_resume_from_owner = _resolve_resume_from_owner
    return uc


@pytest.mark.asyncio
async def test_get_resume_context_enriches_battle_target():
    """轮询入口必须富化 —— 客户端 READY 之后走的正是这条,不是 Login。"""
    uc = _resume_uc(
        (True, _battle_target()),
        resolver=_Resolver(_ma(stage=match_pb2.PLAYER_MATCH_RESUME_STAGE_READY,
                               match_id=27406903575379968, game_mode="pve_coop", map_id=14)),
    )
    out = await uc.get_resume_context("tok")
    assert out.match_id == 27406903575379968
    assert out.match_stage == _R.RESUME_MATCH_STAGE_READY


@pytest.mark.asyncio
async def test_get_resume_context_wait_is_not_enriched():
    """WAIT 是"权威还没定",不该去问撮合,更不该被富化字段掩盖成可进场的样子。"""
    wait = lbiz._wait_resume(_R.RESUME_WAIT_REASON_OWNER_UNKNOWN, 800)
    resolver = _Resolver(_ma(match_id=1))
    uc = _resume_uc((True, wait), resolver=resolver)
    out = await uc.get_resume_context("tok")
    assert out.entry_state == _R.RESUME_ENTRY_STATE_WAIT
    assert resolver.calls == 0


@pytest.mark.asyncio
async def test_get_resume_context_no_ownership_runs_first_entry():
    """owner 说"无归属"时**不得**回裸 UNSPECIFIED —— 客户端会判契约残缺无限重查。"""
    uc = _resume_uc((False, lbiz.ResumeContextResult()))
    seen = {}

    async def _resolve_first_entry(player_id, sess_jti):
        seen["player_id"] = player_id
        seen["sess_jti"] = sess_jti
        out = lbiz.ResumeContextResult()
        out.route = _R.RESUME_ROUTE_HUB
        out.entry_state = _R.RESUME_ENTRY_STATE_ROLE_REQUIRED
        return out

    uc._resolve_first_entry = _resolve_first_entry
    out = await uc.get_resume_context("tok")
    assert seen == {"player_id": PLAYER, "sess_jti": SESS_JTI}
    assert out.entry_state != _R.RESUME_ENTRY_STATE_UNSPECIFIED
