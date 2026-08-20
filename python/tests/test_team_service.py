"""team gRPC service 层:身份门、入参门、in-band 码形态。

这一层的两类缺陷都**不会**在 biz 测试里露头:

  1. **返回形态**:Go 侧每条失败路径都是 `return &XxxResponse{Code: …}, nil`(gRPC
     status 仍是 OK)。改成 `context.abort()` 会让调用方走到完全不同的错误分支 ——
     客户端读的是 body 里的 code,abort 之后 body 是空的。
  2. **身份来源**:客户端面 RPC 必须用鉴权上下文的 player_id 覆盖请求体字段(R5)。
     用请求体的话任何人都能以别人的身份离队 / 踢人。而"功能能跑"这件事分辨不出来。

★ 不起真 gRPC server:context 用最小假件(只需要 invocation_metadata),
  usecase 用记录调用参数的假件 —— 本组验的是 service 层自己的判定,不是业务逻辑。
"""

from __future__ import annotations

import pytest
from pandora.common.v1 import errcode_pb2
from pandora.team.v1 import team_pb2

from pandorapy import errcode
from pandorapy import interceptors
from pandorapy.services.team import service as tsvc

CALLER = 10001
OTHER = 20002
TEAM_ID = 7001


class FakeContext:
    """最小 grpc.aio.ServicerContext 替身。"""

    def __init__(self, player_id: int = 0, extra: dict[str, str] | None = None) -> None:
        md: list[tuple[str, str]] = []
        if player_id:
            md.append((interceptors.METADATA_KEY_PLAYER_ID, str(player_id)))
        for k, v in (extra or {}).items():
            md.append((k, v))
        self._md = md

    def invocation_metadata(self):  # noqa: ANN201
        return self._md


class FakeSF:
    def __init__(self, value: int) -> None:
        self.value = value

    def generate(self) -> int:
        return self.value


class FakeUsecase:
    """记录 biz 调用参数;失败路径按需抛 PandoraError。"""

    def __init__(self, *, raises: BaseException | None = None) -> None:
        self.calls: list[tuple] = []
        self.raises = raises

    def _record(self, name: str, *args):  # noqa: ANN002, ANN202
        self.calls.append((name, *args))
        if self.raises is not None:
            raise self.raises

    async def team_to_proto(self, rec):  # noqa: ANN001, ANN201
        return team_pb2.Team(team_id=TEAM_ID)

    def invite_ttl_ms(self) -> int:
        return 60_000

    async def create_team(self, team_id, player_id):  # noqa: ANN001, ANN201
        self._record("create_team", team_id, player_id)
        return team_pb2.TeamStorageRecord(team_id=team_id)

    async def invite(self, invite_id, team_id, inviter_id, target):  # noqa: ANN001, ANN201
        self._record("invite", invite_id, team_id, inviter_id, target)
        return team_pb2.TeamStorageRecord(team_id=team_id)

    async def leave_team(self, team_id, player_id):  # noqa: ANN001, ANN201
        self._record("leave_team", team_id, player_id)
        return team_pb2.TeamStorageRecord(team_id=team_id)

    async def kick(self, team_id, captain_id, target):  # noqa: ANN001, ANN201
        self._record("kick", team_id, captain_id, target)
        return team_pb2.TeamStorageRecord(team_id=team_id)

    async def set_ready(self, team_id, player_id, ready, hero_id):  # noqa: ANN001, ANN201
        self._record("set_ready", team_id, player_id, ready, hero_id)
        return team_pb2.TeamStorageRecord(team_id=team_id)

    async def get_my_team(self, player_id):  # noqa: ANN001, ANN201
        self._record("get_my_team", player_id)
        return None, False

    async def get_player_team_id(self, player_id):  # noqa: ANN001, ANN201
        self._record("get_player_team_id", player_id)
        return TEAM_ID, True

    async def begin_team_match(self, team_id, captain_id, op, lease, require):  # noqa: ANN001, ANN201
        self._record("begin_team_match", team_id, captain_id, op, lease, require)
        return team_pb2.TeamStorageRecord(team_id=team_id, ready_generation=4), 123

    async def end_team_match(self, team_id, player_ids, expected_gen):  # noqa: ANN001
        self._record("end_team_match", team_id, tuple(player_ids), expected_gen)


def _svc(uc: FakeUsecase | None = None) -> tsvc.TeamService:
    return tsvc.TeamService(uc or FakeUsecase(), FakeSF(111), FakeSF(222))


# ── 身份门(R5)────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("rpc", "request_msg"),
    [
        ("CreateTeam", team_pb2.CreateTeamRequest()),
        ("Invite", team_pb2.InviteRequest(team_id=TEAM_ID, target_player_id=OTHER)),
        ("AcceptInvite", team_pb2.AcceptInviteRequest(team_id=TEAM_ID)),
        ("LeaveTeam", team_pb2.LeaveTeamRequest(team_id=TEAM_ID)),
        ("Kick", team_pb2.KickRequest(team_id=TEAM_ID, target_player_id=OTHER)),
        ("SetReady", team_pb2.SetReadyRequest(team_id=TEAM_ID, ready=True)),
        ("GetMyTeam", team_pb2.GetMyTeamRequest()),
        ("ListMyPendingInvites", team_pb2.ListMyPendingInvitesRequest()),
        ("SetTeamMap", team_pb2.SetTeamMapRequest(team_id=TEAM_ID)),
        ("ListOpenTeams", team_pb2.ListOpenTeamsRequest()),
        ("ApplyToTeam", team_pb2.ApplyToTeamRequest(team_id=TEAM_ID)),
        (
            "ListTeamApplications",
            team_pb2.ListTeamApplicationsRequest(team_id=TEAM_ID),
        ),
        (
            "HandleTeamApplication",
            team_pb2.HandleTeamApplicationRequest(
                team_id=TEAM_ID, applicant_id=OTHER, accept=True
            ),
        ),
    ],
)
async def test_client_rpcs_reject_anonymous_with_inband_code(rpc, request_msg) -> None:  # noqa: ANN001
    """★ 未登录 → `Response(code=ERR_UNAUTHORIZED)` 且**不抛 gRPC 错误**。

    Envoy jwt_authn 已在路由层 require JWT;这是兜底。抛 gRPC 错误的话客户端读不到
    body 里的 code,会走到"网络失败"分支而不是"请重新登录"。
    """
    svc = _svc()
    resp = await getattr(svc, rpc)(request_msg, FakeContext(0))
    assert resp.code == errcode_pb2.ERR_UNAUTHORIZED, rpc


async def test_get_team_needs_no_identity() -> None:
    """GetTeam 只读,team_id 即授权 —— 刻意不要求登录(与 Go 同)。

    顺手加上身份要求会让 DS / 内网工具的合法查询全部变成 401。
    """
    uc = FakeUsecase()

    async def _get_team(team_id):  # noqa: ANN001, ANN202
        uc.calls.append(("get_team", team_id))
        return team_pb2.TeamStorageRecord(team_id=team_id)

    uc.get_team = _get_team
    resp = await _svc(uc).GetTeam(team_pb2.GetTeamRequest(team_id=TEAM_ID), FakeContext(0))
    assert resp.code == errcode_pb2.OK
    assert ("get_team", TEAM_ID) in uc.calls


@pytest.mark.parametrize(
    ("rpc", "request_msg", "want_arg_index"),
    [
        ("LeaveTeam", team_pb2.LeaveTeamRequest(team_id=TEAM_ID), 2),
        ("Kick", team_pb2.KickRequest(team_id=TEAM_ID, target_player_id=OTHER), 2),
        (
            "SetReady",
            team_pb2.SetReadyRequest(team_id=TEAM_ID, ready=True, hero_id=3),
            2,
        ),
    ],
)
async def test_identity_comes_from_auth_context(rpc, request_msg, want_arg_index) -> None:  # noqa: ANN001
    """★ 身份取鉴权上下文,不取请求体(R5)。

    请求体里的 player_id 字段在这些 message 上根本不存在(协议已按 R5 删掉),
    这条验的是 service 真的把上下文里的 id 传给了 biz —— 传 0 的话每个写操作都会
    以"匿名"落到业务层。
    """
    uc = FakeUsecase()
    await getattr(_svc(uc), rpc)(request_msg, FakeContext(CALLER))
    assert uc.calls[0][want_arg_index] == CALLER


# ── 入参门 ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("rpc", "request_msg"),
    [
        ("Invite", team_pb2.InviteRequest(target_player_id=OTHER)),
        ("Invite", team_pb2.InviteRequest(team_id=TEAM_ID)),
        ("AcceptInvite", team_pb2.AcceptInviteRequest(invite_id=9)),
        ("LeaveTeam", team_pb2.LeaveTeamRequest()),
        ("Kick", team_pb2.KickRequest(target_player_id=OTHER)),
        ("Kick", team_pb2.KickRequest(team_id=TEAM_ID)),
        ("SetReady", team_pb2.SetReadyRequest(ready=True)),
        ("SetTeamMap", team_pb2.SetTeamMapRequest(map_id=4002)),
        ("ApplyToTeam", team_pb2.ApplyToTeamRequest()),
        ("ListTeamApplications", team_pb2.ListTeamApplicationsRequest()),
        (
            "HandleTeamApplication",
            team_pb2.HandleTeamApplicationRequest(applicant_id=OTHER),
        ),
        (
            "HandleTeamApplication",
            team_pb2.HandleTeamApplicationRequest(team_id=TEAM_ID),
        ),
    ],
)
async def test_missing_required_args_return_invalid_arg(rpc, request_msg) -> None:  # noqa: ANN001
    uc = FakeUsecase()
    resp = await getattr(_svc(uc), rpc)(request_msg, FakeContext(CALLER))
    assert resp.code == errcode_pb2.ERR_INVALID_ARG
    assert uc.calls == [], "入参门必须在调 biz 之前"


async def test_set_team_map_allows_zero_map_id() -> None:
    """map_id=0 合法(清空 / 未指定),不做非零校验 —— 加了会让"取消目标关卡"点不动。"""
    uc = FakeUsecase()

    async def _set(team_id, captain_id, map_id):  # noqa: ANN001, ANN202
        uc.calls.append(("set_team_map", team_id, captain_id, map_id))
        return team_pb2.TeamStorageRecord(team_id=team_id)

    uc.set_team_map = _set
    resp = await _svc(uc).SetTeamMap(
        team_pb2.SetTeamMapRequest(team_id=TEAM_ID), FakeContext(CALLER)
    )
    assert resp.code == errcode_pb2.OK
    assert uc.calls[0] == ("set_team_map", TEAM_ID, CALLER, 0)


# ── 业务失败:in-band 码,gRPC status 仍 OK ────────────────────────────────


async def test_business_failure_is_inband_not_grpc_error() -> None:
    """★ biz 抛 PandoraError → 转成 response.code,**不冒泡成 gRPC 错误**。"""
    uc = FakeUsecase(raises=errcode.PandoraError(errcode.ErrTeamFull, "full"))
    resp = await _svc(uc).LeaveTeam(
        team_pb2.LeaveTeamRequest(team_id=TEAM_ID), FakeContext(CALLER)
    )
    assert resp.code == errcode.ErrTeamFull == errcode_pb2.ERR_TEAM_FULL


async def test_get_my_team_without_team_is_ok_not_error() -> None:
    """没队伍是正常态:OK + has_team_msg=false,不用 errcode 表达。"""
    resp = await _svc().GetMyTeam(team_pb2.GetMyTeamRequest(), FakeContext(CALLER))
    assert resp.code == errcode_pb2.OK
    assert resp.has_team_msg is False


# ── system_only:三个东西向方法 ────────────────────────────────────────────


@pytest.mark.parametrize(
    ("rpc", "request_msg"),
    [
        (
            "BeginTeamMatch",
            team_pb2.BeginTeamMatchRequest(
                team_id=TEAM_ID, captain_id=CALLER, operation_id="op", lease_ms=5000
            ),
        ),
        ("EndTeamMatch", team_pb2.EndTeamMatchRequest(team_id=TEAM_ID)),
        ("GetPlayerTeam", team_pb2.GetPlayerTeamRequest(player_id=CALLER)),
    ],
)
async def test_internal_rpcs_reject_calls_carrying_player_jwt(rpc, request_msg) -> None:  # noqa: ANN001
    """★ Envoy 按整前缀路由,带玩家 JWT 的客户端同样打得到这三个方法。

    不显式拒就是对客户端开放:BeginTeamMatch 能给任意队伍上租约(反复调 = 让那支队伍
    永远开不了局),EndTeamMatch 能把任意队伍打回 FORMING,GetPlayerTeam 是查任意玩家
    在哪支队的 IDOR 口子。
    """
    uc = FakeUsecase()
    resp = await getattr(_svc(uc), rpc)(request_msg, FakeContext(CALLER))
    assert resp.code == errcode_pb2.ERR_PERMISSION_DENY
    assert uc.calls == [], "身份门必须在调 biz 之前"


async def test_begin_team_match_passes_through_captain_and_generation() -> None:
    """caller==0(内部东西向)时放行,captain_id 从请求体取并原样交给 biz 复核。

    ready_generation 必须回给 matchmaker —— 它是「这次复位对应的正是这一局」的唯一凭据,
    不回的话 EndTeamMatch 只能退化成非跨代安全的 legacy 档。
    """
    uc = FakeUsecase()
    resp = await _svc(uc).BeginTeamMatch(
        team_pb2.BeginTeamMatchRequest(
            team_id=TEAM_ID,
            captain_id=CALLER,
            operation_id="op-1",
            lease_ms=5000,
            require_ready=True,
        ),
        FakeContext(0),
    )
    assert resp.code == errcode_pb2.OK
    assert resp.lease_expires_at_ms == 123
    assert resp.ready_generation == 4
    assert uc.calls[0] == ("begin_team_match", TEAM_ID, CALLER, "op-1", 5000, True)


async def test_end_team_match_forwards_expected_generation() -> None:
    uc = FakeUsecase()
    resp = await _svc(uc).EndTeamMatch(
        team_pb2.EndTeamMatchRequest(
            team_id=TEAM_ID, player_ids=[CALLER, OTHER], expected_ready_generation=9
        ),
        FakeContext(0),
    )
    assert resp.code == errcode_pb2.OK
    assert uc.calls[0] == ("end_team_match", TEAM_ID, (CALLER, OTHER), 9)


async def test_get_player_team_requires_player_id() -> None:
    uc = FakeUsecase()
    resp = await _svc(uc).GetPlayerTeam(
        team_pb2.GetPlayerTeamRequest(), FakeContext(0)
    )
    assert resp.code == errcode_pb2.ERR_INVALID_ARG
    assert uc.calls == []


async def test_get_player_team_returns_team_id() -> None:
    uc = FakeUsecase()
    resp = await _svc(uc).GetPlayerTeam(
        team_pb2.GetPlayerTeamRequest(player_id=CALLER), FakeContext(0)
    )
    assert resp.code == errcode_pb2.OK
    assert resp.has_team is True
    assert resp.team_id == TEAM_ID


# ── DS 回调令牌门(GetPlayerTeam)──────────────────────────────────────────


class _StubGuard:
    """dsauth.DSCallbackGuard 的最小替身:记录 scope 并返回预置码。"""

    def __init__(self, code: int) -> None:
        self.code = code
        self.scopes: list = []

    def check(self, _context, scope) -> int:  # noqa: ANN001
        self.scopes.append(scope)
        return self.code


async def test_get_player_team_ds_guard_rejection_is_inband() -> None:
    """★ 守卫拒绝 → in-band 码,且**不查库**。"""
    uc = FakeUsecase()
    svc = _svc(uc)
    guard = _StubGuard(errcode.ErrUnauthorized)
    svc.set_ds_callback_guard(guard)
    resp = await svc.GetPlayerTeam(
        team_pb2.GetPlayerTeamRequest(player_id=CALLER), FakeContext(0)
    )
    assert resp.code == errcode_pb2.ERR_UNAUTHORIZED
    assert uc.calls == []


async def test_get_player_team_ds_guard_requires_token() -> None:
    """★ scope 必须是 require_token=True,且**不绑** ds_type / pod / match_id。

    require_token 少了,被攻破的业务 Pod 直连 20010 就会被当东西向内部信任;
    绑了 ds_type 则会让将来任何新玩法的合法查询变成鉴权失败,而范围收益是零
    (令牌本身已证明是 DS)。
    """
    svc = _svc()
    guard = _StubGuard(0)
    svc.set_ds_callback_guard(guard)
    await svc.GetPlayerTeam(
        team_pb2.GetPlayerTeamRequest(player_id=CALLER), FakeContext(0)
    )
    scope = guard.scopes[0]
    assert scope.require_token is True
    assert scope.ds_type == ""
    assert scope.pod == ""
    assert scope.match_id == 0
    assert scope.deny_ds is False


# ── matchmaker 调用验签的三档 ─────────────────────────────────────────────


class _StubVerifier:
    def __init__(self, exc: BaseException | None) -> None:
        self.exc = exc
        self.calls: list[tuple[str, int]] = []

    async def verify(self, _md, full_method, subject) -> None:  # noqa: ANN001
        self.calls.append((full_method, subject))
        if self.exc is not None:
            raise self.exc


async def test_match_call_auth_disabled_passes_through() -> None:
    """留空密钥 = 整道不启用,行为与接线前完全一致(滚动升级期先滚 team 不打断调用)。"""
    uc = FakeUsecase()
    resp = await _svc(uc).EndTeamMatch(
        team_pb2.EndTeamMatchRequest(team_id=TEAM_ID), FakeContext(0)
    )
    assert resp.code == errcode_pb2.OK


async def test_match_call_auth_observe_mode_logs_but_allows() -> None:
    """★ 观察期(require=false):验不过只记不拒。

    直接上强制会踩 §9.21 那个坑 —— team 先要求签名而 matchmaker 还没滚到签名版本,
    全线拒。
    """
    uc = FakeUsecase()
    svc = _svc(uc)
    verifier = _StubVerifier(RuntimeError("no credential"))
    svc.set_match_call_auth(verifier, False)
    resp = await svc.EndTeamMatch(
        team_pb2.EndTeamMatchRequest(team_id=TEAM_ID), FakeContext(0)
    )
    assert resp.code == errcode_pb2.OK
    assert verifier.calls == [(tsvc.END_TEAM_MATCH_METHOD, TEAM_ID)]
    assert uc.calls, "观察期必须真的放行到 biz,而不是「没拒也没做」"


async def test_match_call_auth_enforced_rejects() -> None:
    uc = FakeUsecase()
    svc = _svc(uc)
    svc.set_match_call_auth(_StubVerifier(RuntimeError("bad signature")), True)
    resp = await svc.BeginTeamMatch(
        team_pb2.BeginTeamMatchRequest(
            team_id=TEAM_ID, captain_id=CALLER, operation_id="op", lease_ms=5000
        ),
        FakeContext(0),
    )
    assert resp.code == errcode_pb2.ERR_PERMISSION_DENY
    assert uc.calls == []


async def test_match_call_auth_replay_store_unavailable_is_retryable() -> None:
    """★ 重放存储不可用 → ERR_UNAVAILABLE 而不是 ERR_PERMISSION_DENY。

    说不清是不是重放,就按不确定回,让调用方重试 —— 判成越权会让 matchmaker 把一次
    Redis 抖动当成"team 拒绝了我",转而走完全不同的处置。
    """
    from pandorapy import internalrpcauth

    svc = _svc()
    svc.set_match_call_auth(
        _StubVerifier(internalrpcauth.ErrUnavailable("replay store down")), True
    )
    resp = await svc.EndTeamMatch(
        team_pb2.EndTeamMatchRequest(team_id=TEAM_ID), FakeContext(0)
    )
    assert resp.code == errcode_pb2.ERR_UNAVAILABLE


async def test_match_call_auth_runs_after_system_only() -> None:
    """★ 顺序:system_only 在前,验签在后。

    反过来的话,一个带玩家 JWT 但恰好签名合法的调用会被放行 —— 而 system_only 判的
    就是"上下文里有没有 player_id"。
    """
    svc = _svc()
    verifier = _StubVerifier(None)
    svc.set_match_call_auth(verifier, True)
    resp = await svc.EndTeamMatch(
        team_pb2.EndTeamMatchRequest(team_id=TEAM_ID), FakeContext(CALLER)
    )
    assert resp.code == errcode_pb2.ERR_PERMISSION_DENY
    assert verifier.calls == [], "system_only 应当在验签之前就拦下"
