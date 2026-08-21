"""`mclients.GrpcDSAllocator` —— matchmaker 侧真实 DS 分配链。

对拍对象:`services/matchmaking/matchmaker/internal/data/ds_allocator.go`。

本文件锁的是**四段容易静默出错的逻辑**,而不是 gRPC 传输本身(那由 grpc 库负责):

  ① 计分模式 / 段位池的**定格**:分配那一刻从关卡表读一次写进请求。
     拿不到必须回 UNSPECIFIED / "",**绝不猜 ELO** —— 猜错会给合作副本玩家扣段位,
     而段位是玩家资产、扣了不可逆。
  ② allocator 错误码**原样上抛**:压成 ErrDSAllocationFailed 会让 worker 把这一局标
     FAILED 并把玩家退回队列,而此刻 Battle DS 可能已经存在 → 同一批玩家开第二场。
  ③ exact 实例五要件**缺一即拒**:放过去会签出一张没有实例绑定的 v2 票,
     §9.22 当场失效(任何一台 battle DS 都能兑),且两边日志全绿。
  ④ 签票的会话现行性:权威不可达 fail-closed,无会话拒签(顶号后旧设备不得入场)。
"""

from __future__ import annotations

import datetime as _dt

import pytest
from pandora.common.v1 import errcode_pb2 as commonpb
from pandora.config.v1 import level_pb2 as levelpb
from pandora.ds.v1 import allocator_pb2 as dspb

from pandorapy import battleabort
from pandorapy import dsticket as pdsticket
from pandorapy import errcode
from pandorapy import internalrpcauth
from pandorapy import placement
from pandorapy import releasetrack
from pandorapy.services.login import dsticket as ldsticket
from pandorapy.services.matchmaker import catalog as mcat
from pandorapy.services.matchmaker import clients as mclients

pytestmark = pytest.mark.anyio

MAP_ID = 4002
GAME_MODE = "pvp"
MATCH_ID = 90210


# ── 替身 ─────────────────────────────────────────────────────────────────────


class _FakeStub:
    """只记调用 + 回预置响应。不起 gRPC server —— 本文件测的是**装填与判读**。"""

    def __init__(self, allocate_resp=None, abort_resp=None) -> None:
        self.allocate_resp = allocate_resp
        self.abort_resp = abort_resp or dspb.AbortPreactiveBattleResponse(code=commonpb.OK)
        self.allocate_reqs: list[dspb.AllocateBattleRequest] = []
        self.abort_reqs: list[tuple[dspb.AbortPreactiveBattleRequest, list]] = []

    async def AllocateBattle(self, req, timeout=None):  # noqa: ANN001, N802
        self.allocate_reqs.append(req)
        return self.allocate_resp

    async def AbortPreactiveBattle(self, req, metadata=None, timeout=None):  # noqa: ANN001, N802
        self.abort_reqs.append((req, list(metadata or ())))
        return self.abort_resp


class _FakeGate:
    """会话现行性权威替身。三种姿态各自对应一条**相反**的指令,不能混。"""

    def __init__(self, jti: str = "", found: bool = True, exc: BaseException | None = None):
        self.jti = jti
        self.found = found
        self.exc = exc
        self.calls: list[int] = []

    async def current_jti(self, player_id: int) -> tuple[str, bool]:
        self.calls.append(player_id)
        if self.exc is not None:
            raise self.exc
        return self.jti, self.found


def _ok_resp(**over) -> dspb.AllocateBattleResponse:
    fields = {
        "code": commonpb.OK,
        "ds_addr": "10.0.0.7:7777",
        "ds_pod_name": "battle-ds-abc",
        "gameserver_uid": "uid-abc",
        "instance_epoch": 3,
        "allocation_id": "alloc-abc",
        "release_track": releasetrack.STABLE,
    }
    fields.update(over)
    return dspb.AllocateBattleResponse(**fields)


def _allocation(**over) -> mclients.BattleAllocation:
    fields = {
        "address": "10.0.0.7:7777",
        "pod_name": "battle-ds-abc",
        "instance_uid": "uid-abc",
        "instance_epoch": 3,
        "allocation_id": "alloc-abc",
        "release_track": releasetrack.STABLE,
    }
    fields.update(over)
    return mclients.BattleAllocation(**fields)


def _store(**level_over) -> mcat.Store:
    """一个只含单行关卡的批次容器。"""
    fields = {"id": MAP_ID, "category": levelpb.LEVEL_CATEGORY_BATTLE}
    fields.update(level_over)
    row = levelpb.LevelRow(**fields)
    return mcat.Store(
        mcat.Tables(version=1, source_rev="test", levels={MAP_ID: row}), "test-dir"
    )


def _alloc(
    *,
    stub=None,
    v2=None,
    legacy=None,
    abort_auth=None,
    gate=None,
    store=None,
) -> mclients.GrpcDSAllocator:
    a = mclients.GrpcDSAllocator(
        "127.0.0.1:1",  # 惰性 channel:不发 RPC 就不会连,下一行立刻换掉 stub
        legacy,
        v2,
        abort_auth,
        MAP_ID,
        GAME_MODE,
        30.0,
    )
    if stub is not None:
        a._stub = stub
    if gate is not None:
        a.set_session_gate(gate)
    if store is not None:
        a.set_config_tables(store)
    return a


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


# ── ① 计分模式 / 段位池定格 ──────────────────────────────────────────────────


async def test_rating_mode_and_pool_are_frozen_from_level_table() -> None:
    stub = _FakeStub(_ok_resp())
    a = _alloc(
        stub=stub,
        store=_store(rating_mode=levelpb.LEVEL_RATING_MODE_ELO, rating_pool="ranked_5v5"),
    )
    await a.allocate_battle_with_combat_factions(MATCH_ID, [7, 42], {7: 1, 42: 2}, MAP_ID)
    req = stub.allocate_reqs[0]
    assert req.rating_mode == levelpb.LEVEL_RATING_MODE_ELO
    assert req.rating_pool == "ranked_5v5"


async def test_rating_mode_falls_back_to_unspecified_without_tables() -> None:
    """未启用配置表 → UNSPECIFIED,**不是** ELO。

    UNSPECIFIED 让 battle_result 回落到本列上线前的旧口径(canonical pve_coop 不计分、
    其余算 Elo),与不带本字段的旧 matchmaker 逐字节一致(§9.21 共存窗口双向兼容)。
    """
    stub = _FakeStub(_ok_resp())
    a = _alloc(stub=stub)  # 刻意不注入 store
    await a.allocate_battle_with_combat_factions(MATCH_ID, [7], {7: 1}, MAP_ID)
    assert stub.allocate_reqs[0].rating_mode == levelpb.LEVEL_RATING_MODE_UNSPECIFIED


async def test_rating_mode_falls_back_when_row_missing() -> None:
    """表在、但没有这一行(热更批次里删了图 / 传了别的 map)→ 同样 UNSPECIFIED。"""
    stub = _FakeStub(_ok_resp())
    a = _alloc(stub=stub, store=_store(rating_mode=levelpb.LEVEL_RATING_MODE_ELO))
    await a.allocate_battle_with_combat_factions(MATCH_ID, [7], {7: 1}, 999999)
    req = stub.allocate_reqs[0]
    assert req.map_id == 999999
    assert req.rating_mode == levelpb.LEVEL_RATING_MODE_UNSPECIFIED


async def test_rating_pool_is_not_normalized_to_default() -> None:
    """空池**保持空**。

    归一化只在落账那一侧做一次;若在这里就填 "default",事后再也分不清是
    "策划配了 default 池"还是"根本没配"。
    """
    stub = _FakeStub(_ok_resp())
    a = _alloc(stub=stub, store=_store(rating_mode=levelpb.LEVEL_RATING_MODE_ELO))
    await a.allocate_battle_with_combat_factions(MATCH_ID, [7], {7: 1}, MAP_ID)
    assert stub.allocate_reqs[0].rating_pool == ""


async def test_empty_map_id_falls_back_to_static_default() -> None:
    """map_id=0(旧客户端 / 未选)→ 回退构造期的静态默认。"""
    stub = _FakeStub(_ok_resp())
    a = _alloc(stub=stub)
    await a.allocate_battle_with_combat_factions(MATCH_ID, [7], {7: 1}, 0)
    assert stub.allocate_reqs[0].map_id == MAP_ID


# ── 请求装填 ─────────────────────────────────────────────────────────────────


async def test_combat_factions_are_canonicalized_by_player_id() -> None:
    """阵营按 player_id **升序**下发 —— 顺序来自 canonical roster,不是 dict 插入序。"""
    stub = _FakeStub(_ok_resp())
    a = _alloc(stub=stub)
    await a.allocate_battle_with_combat_factions(
        MATCH_ID, [99, 7, 42], {99: 9, 7: 3, 42: 3}, MAP_ID
    )
    req = stub.allocate_reqs[0]
    assert list(req.player_ids) == [7, 42, 99]
    assert [(f.player_id, f.combat_faction_id) for f in req.player_combat_factions] == [
        (7, 3),
        (42, 3),
        (99, 9),
    ]
    assert req.game_mode == GAME_MODE
    assert req.match_id == MATCH_ID


async def test_missing_combat_faction_is_rejected_not_downgraded() -> None:
    """阵营缺一个 → 拒。

    降级成"不带阵营"会让 DS 退化成每人一个独立阵营:队友互相能打,
    而对局照常进行、照常结算 —— 错误完全不可见。
    """
    stub = _FakeStub(_ok_resp())
    a = _alloc(stub=stub)
    with pytest.raises(errcode.PandoraError) as ei:
        await a.allocate_battle_with_combat_factions(MATCH_ID, [7, 42], {7: 1}, MAP_ID)
    assert ei.value.code == errcode.ErrInvalidArg
    assert stub.allocate_reqs == [], "形状不合法就绝不能真的去拉一台 DS"


# ── ② 错误码原样上抛 ─────────────────────────────────────────────────────────


async def test_allocator_error_code_is_preserved_not_collapsed() -> None:
    """ERR_UNAVAILABLE 必须原样上抛。

    它意味着外部分配结果 UNKNOWN(commit/fence 回包丢了);压成 ErrDSAllocationFailed
    会让 worker 标 FAILED 并把玩家退回队列,而 Battle DS 可能已经存在。
    """
    stub = _FakeStub(_ok_resp(code=commonpb.ERR_UNAVAILABLE, ds_addr=""))
    a = _alloc(stub=stub)
    with pytest.raises(errcode.PandoraError) as ei:
        await a.allocate_battle_with_combat_factions(MATCH_ID, [7], {7: 1}, MAP_ID)
    assert ei.value.code == commonpb.ERR_UNAVAILABLE
    assert ei.value.code != errcode.ErrDSAllocationFailed


async def test_ok_with_empty_addr_is_rejected() -> None:
    stub = _FakeStub(_ok_resp(ds_addr=""))
    a = _alloc(stub=stub)
    with pytest.raises(errcode.PandoraError) as ei:
        await a.allocate_battle_with_combat_factions(MATCH_ID, [7], {7: 1}, MAP_ID)
    assert ei.value.code == errcode.ErrDSAllocationFailed


# ── ③ exact 实例五要件 ───────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "missing",
    [
        {"ds_pod_name": ""},
        {"gameserver_uid": ""},
        {"instance_epoch": 0},
        {"allocation_id": ""},
        {"release_track": ""},
        {"release_track": "prod"},  # 不在 {stable, canary} 里 = 未知轨道
    ],
    ids=["pod", "uid", "epoch", "alloc", "track_empty", "track_unknown"],
)
async def test_incomplete_exact_target_is_rejected(missing: dict) -> None:
    stub = _FakeStub(_ok_resp(**missing))
    a = _alloc(stub=stub)
    with pytest.raises(errcode.PandoraError) as ei:
        await a.allocate_battle_with_combat_factions(MATCH_ID, [7], {7: 1}, MAP_ID)
    assert ei.value.code == errcode.ErrDSAllocationFailed


async def test_complete_target_is_carried_into_allocation() -> None:
    stub = _FakeStub(_ok_resp())
    a = _alloc(stub=stub)
    got = await a.allocate_battle_with_combat_factions(MATCH_ID, [7], {7: 1}, MAP_ID)
    assert got.address == "10.0.0.7:7777"
    assert got.pod_name == "battle-ds-abc"
    assert got.instance_uid == "uid-abc"
    assert got.instance_epoch == 3
    assert got.allocation_id == "alloc-abc"
    assert got.release_track == releasetrack.STABLE
    assert got.complete_battle()


# ── ④ 签票 ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def keypair() -> tuple[bytes, object, str]:
    return pdsticket.generate_ds_ticket_key_pair()


@pytest.fixture(scope="module")
def v2_signer(keypair) -> pdsticket.DSTicketSigner:
    private_pem, _pub, kid = keypair
    return pdsticket.DSTicketSigner.new(
        pdsticket.DSTicketSignerConfig(private_key_pem=private_pem, active_kid=kid)
    )


@pytest.fixture(scope="module")
def v2_verifier(keypair) -> pdsticket.DSTicketVerifier:
    """用**产品自己的验签器**读回 claims,而不是测试里手解 JWT。

    手解会让用例与实现对 claim 名分叉(实现改了 claim 名、用例还在断旧名 → 绿的假象);
    过验签器还顺带证明了这张票真的验得过。
    """
    _pem, pub, kid = keypair
    jwks = pdsticket.marshal_ds_ticket_jwks(1, kid, pub)
    return pdsticket.DSTicketVerifier.new(pdsticket.DSTicketVerifierConfig(jwks=jwks))


def _legacy_signer() -> ldsticket.DSTicketSigner:
    return ldsticket.DSTicketSigner(
        secret="x" * 32,
        issuer="pandora",
        audience="pandora-client",
        ttl=_dt.timedelta(minutes=2),
    )


async def test_sign_battle_ticket_without_any_signer_is_rejected() -> None:
    """两档都没注入 = 装配漏了。绝不静默签一张验不过的票。"""
    a = _alloc()
    with pytest.raises(errcode.PandoraError) as ei:
        await a.sign_battle_ticket(7, MATCH_ID, _allocation())
    assert ei.value.code == errcode.ErrDSAllocationFailed


async def test_sign_battle_ticket_requires_complete_target(v2_signer) -> None:
    a = _alloc(v2=v2_signer)
    with pytest.raises(errcode.PandoraError) as ei:
        await a.sign_battle_ticket(7, MATCH_ID, _allocation(instance_uid=""))
    assert ei.value.code == errcode.ErrDSAllocationFailed


async def test_v2_ticket_binds_exact_instance_and_session(v2_signer, v2_verifier) -> None:
    gate = _FakeGate(jti="sess-9", found=True)
    a = _alloc(v2=v2_signer, gate=gate)
    token = await a.sign_battle_ticket(7, MATCH_ID, _allocation())
    assert pdsticket.ds_ticket_algorithm(token) == "RS256"
    assert gate.calls == [7]
    claims = v2_verifier.verify(token)
    assert claims.player_id() == 7
    assert claims.ds_type == pdsticket.DS_TYPE_BATTLE
    assert claims.ds_pod_name == "battle-ds-abc"
    assert claims.ds_instance_uid == "uid-abc"
    assert claims.ds_instance_epoch == 3
    assert claims.allocation_id == "alloc-abc"
    assert claims.release_track == releasetrack.STABLE
    assert claims.match_id == MATCH_ID
    assert claims.sess_jti == "sess-9"
    # battle 票**不得**带 hub 侧绑定(带了说明 target 拼错了类型)。
    assert claims.hub_assignment_id == ""
    assert claims.source_match_id == 0


async def test_session_authority_unavailable_fails_closed(v2_signer) -> None:
    """权威不可达 → ErrUnavailable。

    「查不到」不能当成「确实有会话」也不能当成「确实没有」—— 票在不确定时不签。
    """
    gate = _FakeGate(exc=errcode.PandoraError(errcode.ErrUnavailable, "redis down"))
    a = _alloc(v2=v2_signer, gate=gate)
    with pytest.raises(errcode.PandoraError) as ei:
        await a.sign_battle_ticket(7, MATCH_ID, _allocation())
    assert ei.value.code == errcode.ErrUnavailable


async def test_no_current_session_withholds_ticket(v2_signer) -> None:
    """无会话(已登出 / 被顶号)→ ErrUnauthorized:不存在合法的入场交付对象。"""
    gate = _FakeGate(found=False)
    a = _alloc(v2=v2_signer, gate=gate)
    with pytest.raises(errcode.PandoraError) as ei:
        await a.sign_battle_ticket(7, MATCH_ID, _allocation())
    assert ei.value.code == errcode.ErrUnauthorized


async def test_local_off_v1_signs_hs256(v2_signer) -> None:
    """local-off-v1 档只注入 legacy:签出的必须是 HS256(UE 侧硬锁该分支)。"""
    a = _alloc(legacy=_legacy_signer())
    token = await a.sign_battle_ticket(7, MATCH_ID, _allocation())
    assert pdsticket.ds_ticket_algorithm(token) == "HS256"


async def test_sign_battle_tickets_is_all_or_nothing(v2_signer) -> None:
    """任一张失败即整批失败。

    部分成功会让一部分人进场、另一部分人拿不到票卡在 READY,而对局照常开始 ——
    那是 §9.19 的玩家卡死形态。
    """
    gate = _FakeGate(found=True, jti="s")

    calls: list[int] = []
    orig = gate.current_jti

    async def flaky(player_id: int):  # noqa: ANN202
        calls.append(player_id)
        if player_id == 42:
            raise errcode.PandoraError(errcode.ErrUnavailable, "boom")
        return await orig(player_id)

    gate.current_jti = flaky  # type: ignore[method-assign]
    a = _alloc(v2=v2_signer, gate=gate)
    with pytest.raises(errcode.PandoraError):
        await a.sign_battle_tickets(MATCH_ID, [7, 42, 99], _allocation())
    assert 99 not in calls, "第二张就炸了,不该继续签第三张"


# ── abort 补偿 ───────────────────────────────────────────────────────────────


async def test_abort_without_auth_signer_is_unavailable() -> None:
    stub = _FakeStub(_ok_resp())
    a = _alloc(stub=stub)
    with pytest.raises(errcode.PandoraError) as ei:
        await a.abort_battle_allocation(MATCH_ID, mclients.new_operation_id(), _allocation())
    assert ei.value.code == errcode.ErrUnavailable
    assert stub.abort_reqs == []


async def test_abort_signs_canonical_request_body() -> None:
    """签的是**全量 canonical 请求体**,不是只签 match_id。

    只签 match_id 的话,拿到一份合法签名就能把 target 换成另一台 DS 再拆掉它。
    """
    stub = _FakeStub(_ok_resp())
    auth = internalrpcauth.Signer("s" * 32, "matchmaker", "ds_allocator")
    a = _alloc(stub=stub, abort_auth=auth)
    op = mclients.new_operation_id()
    await a.abort_battle_allocation(MATCH_ID, op, _allocation())
    req, metadata = stub.abort_reqs[0]
    assert req.match_id == MATCH_ID
    assert req.allocation_operation_id == op
    assert req.ds_pod_name == "battle-ds-abc"
    assert req.gameserver_uid == "uid-abc"
    assert req.instance_epoch == 3
    assert req.allocation_id == "alloc-abc"
    assert req.release_track == releasetrack.STABLE
    assert metadata, "必须带鉴权 metadata"
    # 签名体确实覆盖 target:改一个字段就得到不同的 canonical 字节。
    base = battleabort.Request(
        match_id=MATCH_ID,
        operation_id=op,
        target=placement.Target(
            pod_name="battle-ds-abc",
            instance_uid="uid-abc",
            instance_epoch=3,
            allocation_id="alloc-abc",
            release_track=releasetrack.STABLE,
        ),
    )
    tampered = battleabort.Request(
        match_id=MATCH_ID,
        operation_id=op,
        target=placement.Target(
            pod_name="battle-ds-other",
            instance_uid="uid-abc",
            instance_epoch=3,
            allocation_id="alloc-abc",
            release_track=releasetrack.STABLE,
        ),
    )
    assert base.complete()
    assert base.canonical() != tampered.canonical()


async def test_abort_incomplete_target_is_rejected() -> None:
    stub = _FakeStub(_ok_resp())
    auth = internalrpcauth.Signer("s" * 32, "matchmaker", "ds_allocator")
    a = _alloc(stub=stub, abort_auth=auth)
    with pytest.raises(errcode.PandoraError) as ei:
        await a.abort_battle_allocation(
            MATCH_ID, mclients.new_operation_id(), _allocation(allocation_id="")
        )
    assert ei.value.code == errcode.ErrInvalidArg
    assert stub.abort_reqs == []


async def test_abort_propagates_allocator_error_code() -> None:
    stub = _FakeStub(
        _ok_resp(),
        abort_resp=dspb.AbortPreactiveBattleResponse(code=commonpb.ERR_UNAVAILABLE),
    )
    auth = internalrpcauth.Signer("s" * 32, "matchmaker", "ds_allocator")
    a = _alloc(stub=stub, abort_auth=auth)
    with pytest.raises(errcode.PandoraError) as ei:
        await a.abort_battle_allocation(MATCH_ID, mclients.new_operation_id(), _allocation())
    assert ei.value.code == commonpb.ERR_UNAVAILABLE
