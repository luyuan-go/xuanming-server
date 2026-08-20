"""team 客户端视图里的玩家编号展示契约。

``player_id`` 仍是队伍成员身份、索引和所有写请求使用的 Snowflake ID；
``player_no`` 只在组装客户端可见 ``TeamMember`` 时从账号权威批量解析，绝不写进
Redis 的 ``TeamMemberStorageRecord``。
"""

from __future__ import annotations

import asyncio

import pytest
from pandora.team.v1 import team_pb2

from pandorapy import internalrpcauth
from pandorapy.services.team import biz as tbiz
from pandorapy.services.team import conf as tconf


CAPTAIN_ID = 8_628_135_489_420_001
MEMBER_ID = 8_628_135_489_420_002
CAPTAIN_NO = 100_001
MEMBER_NO = 100_002
AUTH_SECRET = "team-to-login-player-no-resolver-secret-v1"
RESOLVE_METHOD = "/pandora.login.v1.LoginInternalService/ResolvePlayerNos"


class _PlayerNoResolver:
    def __init__(
        self,
        values: dict[int, int] | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.values = values or {}
        self.error = error
        self.calls: list[list[int]] = []

    async def resolve_player_nos(self, player_ids: list[int]) -> dict[int, int]:
        self.calls.append(list(player_ids))
        if self.error is not None:
            raise self.error
        return dict(self.values)


class _ReplayStore:
    def __init__(self) -> None:
        self.keys: set[str] = set()

    async def consume(self, nonce_key: str, _ttl_sec: float) -> bool:
        if nonce_key in self.keys:
            return False
        self.keys.add(nonce_key)
        return True


class _Context:
    def __init__(self, metadata: list[tuple[str, str]] | None = None) -> None:
        self._metadata = metadata or []

    def invocation_metadata(self):  # noqa: ANN201
        return self._metadata


def _record() -> team_pb2.TeamStorageRecord:
    return team_pb2.TeamStorageRecord(
        team_id=7_001,
        captain_id=CAPTAIN_ID,
        state=team_pb2.TEAM_STATE_FORMING,
        members=[
            team_pb2.TeamMemberStorageRecord(player_id=CAPTAIN_ID),
            team_pb2.TeamMemberStorageRecord(player_id=MEMBER_ID),
        ],
        max_size=5,
    )


async def test_team_to_proto_batch_enriches_display_no_and_keeps_identity() -> None:
    resolver = _PlayerNoResolver(
        {CAPTAIN_ID: CAPTAIN_NO, MEMBER_ID: MEMBER_NO}
    )
    uc = tbiz.TeamUsecase(None, None, tconf.TeamConf())
    uc.set_player_no_resolver(resolver)

    team = await uc.team_to_proto(_record())

    assert resolver.calls == [[CAPTAIN_ID, MEMBER_ID]], "一支队伍只能发起一次有界批量解析"
    assert [(m.player_id, m.player_no) for m in team.members] == [
        (CAPTAIN_ID, CAPTAIN_NO),
        (MEMBER_ID, MEMBER_NO),
    ]
    assert "player_no" not in team_pb2.TeamMemberStorageRecord.DESCRIPTOR.fields_by_name


async def test_team_to_proto_player_no_failure_falls_back_without_hiding_identity() -> None:
    resolver = _PlayerNoResolver(error=RuntimeError("login unavailable"))
    uc = tbiz.TeamUsecase(None, None, tconf.TeamConf())
    uc.set_player_no_resolver(resolver)

    team = await uc.team_to_proto(_record())

    assert resolver.calls == [[CAPTAIN_ID, MEMBER_ID]]
    assert [(m.player_id, m.player_no) for m in team.members] == [
        (CAPTAIN_ID, 0),
        (MEMBER_ID, 0),
    ]


async def test_team_push_carries_resolved_player_no_in_client_projection() -> None:
    class _Pusher:
        def __init__(self) -> None:
            self.payloads: list[bytes] = []

        async def push_to_players(self, _caller, _players, payload, _event_type):  # noqa: ANN001, ANN201
            self.payloads.append(payload)
            return 1, None

    resolver = _PlayerNoResolver(
        {CAPTAIN_ID: CAPTAIN_NO, MEMBER_ID: MEMBER_NO}
    )
    pusher = _Pusher()
    uc = tbiz.TeamUsecase(None, pusher, tconf.TeamConf())
    uc.set_player_no_resolver(resolver)

    await uc.push_update(0, [CAPTAIN_ID], _record(), 0, 0)

    assert resolver.calls == [[CAPTAIN_ID, MEMBER_ID]]
    assert len(pusher.payloads) == 1
    event = team_pb2.TeamUpdateEvent.FromString(pusher.payloads[0])
    assert [(m.player_id, m.player_no) for m in event.team.members] == [
        (CAPTAIN_ID, CAPTAIN_NO),
        (MEMBER_ID, MEMBER_NO),
    ]


async def test_internal_auth_payload_binding_rejects_mutated_player_id_batch() -> None:
    """批量请求必须绑定整包；只签第一个 ID 会让其余 ID 可被中途替换。"""
    now_ms = lambda: 1_700_000_000_000  # noqa: E731
    signer = internalrpcauth.Signer(
        AUTH_SECRET, "team", "login", now_ms=now_ms
    )
    verifier = internalrpcauth.Verifier(
        AUTH_SECRET,
        "team",
        "login",
        30.0,
        _ReplayStore(),
        now_ms=now_ms,
    )
    canonical_request = b"\x0a\x02\x01\x02"
    mutated_request = b"\x0a\x02\x01\x03"
    metadata = dict(
        signer.sign_metadata_with_payload(
            RESOLVE_METHOD, CAPTAIN_ID, canonical_request
        )
    )

    with pytest.raises(internalrpcauth.ErrUnauthorized):
        await verifier.verify_with_payload(
            metadata, RESOLVE_METHOD, CAPTAIN_ID, mutated_request
        )

    # payload 不匹配不能先消费 nonce；同一凭证随后校验原始请求仍应成功。
    await verifier.verify_with_payload(
        metadata, RESOLVE_METHOD, CAPTAIN_ID, canonical_request
    )


async def test_internal_auth_replay_cancellation_propagates() -> None:
    """请求任务被取消时必须原样传播，不能伪装成 replay store 不可用。"""

    cancelled = asyncio.CancelledError("request cancelled")

    class _CancelledReplayStore:
        async def consume(self, _nonce_key: str, _ttl_sec: float) -> bool:
            raise cancelled

    now_ms = lambda: 1_700_000_000_000  # noqa: E731
    signer = internalrpcauth.Signer(
        AUTH_SECRET, "team", "login", now_ms=now_ms
    )
    verifier = internalrpcauth.Verifier(
        AUTH_SECRET,
        "team",
        "login",
        30.0,
        _CancelledReplayStore(),
        now_ms=now_ms,
    )
    payload = b"\x0a\x01\x01"
    metadata = dict(
        signer.sign_metadata_with_payload(RESOLVE_METHOD, CAPTAIN_ID, payload)
    )

    with pytest.raises(asyncio.CancelledError) as exc_info:
        await verifier.verify_with_payload(
            metadata, RESOLVE_METHOD, CAPTAIN_ID, payload
        )
    assert exc_info.value is cancelled


async def test_grpc_resolver_normalizes_once_and_signs_the_exact_batch() -> None:
    from pandora.common.v1 import errcode_pb2
    from pandora.login.v1 import login_pb2

    from pandorapy.services.team import player_no_client

    now_ms = lambda: 1_700_000_000_000  # noqa: E731
    signer = internalrpcauth.Signer(
        AUTH_SECRET, "team", "login", now_ms=now_ms
    )
    verifier = internalrpcauth.Verifier(
        AUTH_SECRET,
        "team",
        "login",
        30.0,
        _ReplayStore(),
        now_ms=now_ms,
    )

    class _Stub:
        def __init__(self) -> None:
            self.requests: list[list[int]] = []

        async def ResolvePlayerNos(self, request, *, timeout, metadata):  # noqa: N802, ANN001, ANN201
            assert timeout > 0
            self.requests.append(list(request.player_ids))
            payload = request.SerializeToString(deterministic=True)
            await verifier.verify_with_payload(
                dict(metadata), RESOLVE_METHOD, request.player_ids[0], payload
            )
            return login_pb2.ResolvePlayerNosResponse(
                code=errcode_pb2.OK,
                entries=[
                    login_pb2.ResolvedPlayerNo(
                        player_id=CAPTAIN_ID, player_no=CAPTAIN_NO
                    ),
                    login_pb2.ResolvedPlayerNo(
                        player_id=MEMBER_ID, player_no=MEMBER_NO
                    ),
                ],
            )

    stub = _Stub()
    resolver = player_no_client.GrpcPlayerNoResolver(
        "", signer, stub=stub, timeout_sec=0.5
    )

    got = await resolver.resolve_player_nos([MEMBER_ID, CAPTAIN_ID, MEMBER_ID])

    assert stub.requests == [[CAPTAIN_ID, MEMBER_ID]]
    assert got == {CAPTAIN_ID: CAPTAIN_NO, MEMBER_ID: MEMBER_NO}


async def test_grpc_resolver_rejects_empty_and_raw_batch_over_32() -> None:
    from pandorapy.services.team import player_no_client

    signer = internalrpcauth.Signer(AUTH_SECRET, "team", "login")
    resolver = player_no_client.GrpcPlayerNoResolver(
        "", signer, stub=object(), timeout_sec=0.5
    )

    with pytest.raises(ValueError):
        await resolver.resolve_player_nos([])
    with pytest.raises(ValueError):
        await resolver.resolve_player_nos([CAPTAIN_ID] * 33)


async def test_login_internal_service_verifies_payload_before_batch_read() -> None:
    from pandora.common.v1 import errcode_pb2
    from pandora.login.v1 import login_pb2

    from pandorapy.services.login import service as lsvc

    class _Reader:
        def __init__(self) -> None:
            self.calls: list[list[int]] = []

        async def resolve_player_nos(self, player_ids: list[int]) -> dict[int, int]:
            self.calls.append(list(player_ids))
            return {CAPTAIN_ID: CAPTAIN_NO, MEMBER_ID: MEMBER_NO}

    class _Verifier:
        def __init__(self) -> None:
            self.calls: list[tuple[str, int, bytes]] = []

        async def verify_with_payload(self, _metadata, method, subject, payload) -> None:  # noqa: ANN001
            self.calls.append((method, subject, payload))

    reader = _Reader()
    verifier = _Verifier()
    svc = lsvc.LoginInternalService(reader, verifier)
    request = login_pb2.ResolvePlayerNosRequest(
        player_ids=[MEMBER_ID, CAPTAIN_ID, MEMBER_ID]
    )

    response = await svc.ResolvePlayerNos(request, _Context())

    canonical = login_pb2.ResolvePlayerNosRequest(
        player_ids=[CAPTAIN_ID, MEMBER_ID]
    ).SerializeToString(deterministic=True)
    assert verifier.calls == [(RESOLVE_METHOD, CAPTAIN_ID, canonical)]
    assert reader.calls == [[CAPTAIN_ID, MEMBER_ID]]
    assert response.code == errcode_pb2.OK
    assert [(e.player_id, e.player_no) for e in response.entries] == [
        (CAPTAIN_ID, CAPTAIN_NO),
        (MEMBER_ID, MEMBER_NO),
    ]


async def test_login_internal_service_rejects_zero_and_never_reads_repo() -> None:
    from pandora.common.v1 import errcode_pb2
    from pandora.login.v1 import login_pb2

    from pandorapy.services.login import service as lsvc

    class _Reader:
        def __init__(self) -> None:
            self.called = False

        async def resolve_player_nos(self, _player_ids):  # noqa: ANN001, ANN201
            self.called = True
            return {}

    reader = _Reader()
    svc = lsvc.LoginInternalService(reader, None)

    response = await svc.ResolvePlayerNos(
        login_pb2.ResolvePlayerNosRequest(player_ids=[CAPTAIN_ID, 0]), _Context()
    )

    assert response.code == errcode_pb2.ERR_INVALID_ARG
    assert reader.called is False


async def test_login_internal_service_bounds_raw_batch_and_missing_auth_is_unavailable() -> None:
    from pandora.common.v1 import errcode_pb2
    from pandora.login.v1 import login_pb2

    from pandorapy.services.login import service as lsvc

    class _Reader:
        def __init__(self) -> None:
            self.called = False

        async def resolve_player_nos(self, _player_ids):  # noqa: ANN001, ANN201
            self.called = True
            return {}

    reader = _Reader()
    svc = lsvc.LoginInternalService(reader, None)

    empty = await svc.ResolvePlayerNos(login_pb2.ResolvePlayerNosRequest(), _Context())
    oversized = await svc.ResolvePlayerNos(
        login_pb2.ResolvePlayerNosRequest(player_ids=[CAPTAIN_ID] * 33), _Context()
    )
    unavailable = await svc.ResolvePlayerNos(
        login_pb2.ResolvePlayerNosRequest(player_ids=[CAPTAIN_ID]), _Context()
    )

    assert empty.code == errcode_pb2.ERR_INVALID_ARG
    assert oversized.code == errcode_pb2.ERR_INVALID_ARG
    assert unavailable.code == errcode_pb2.ERR_UNAVAILABLE
    assert reader.called is False


async def test_login_usecase_reads_player_nos_in_one_repo_batch() -> None:
    from pandorapy.services.login import biz as lbiz

    class _Repo:
        def __init__(self) -> None:
            self.calls: list[list[int]] = []

        async def get_player_nos(self, player_ids: list[int]) -> dict[int, int]:
            self.calls.append(list(player_ids))
            return {CAPTAIN_ID: CAPTAIN_NO, MEMBER_ID: MEMBER_NO}

    repo = _Repo()
    uc = object.__new__(lbiz.LoginUsecase)
    uc._repo = repo  # noqa: SLF001 —— 只隔离本公开方法，不构造整条登录依赖

    got = await uc.resolve_player_nos([CAPTAIN_ID, MEMBER_ID])

    assert repo.calls == [[CAPTAIN_ID, MEMBER_ID]]
    assert got == {CAPTAIN_ID: CAPTAIN_NO, MEMBER_ID: MEMBER_NO}


async def test_mysql_account_repo_reads_player_nos_with_one_in_query() -> None:
    from pandorapy.services.login import data as ldata

    class _CM:
        def __init__(self, value) -> None:  # noqa: ANN001
            self.value = value

        async def __aenter__(self):  # noqa: ANN201
            return self.value

        async def __aexit__(self, *_args):  # noqa: ANN002, ANN201
            return False

    class _Cursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[int, ...]]] = []

        async def execute(self, query: str, args: tuple[int, ...]) -> None:
            self.calls.append((query, args))

        async def fetchall(self):  # noqa: ANN201
            return [
                (CAPTAIN_ID, CAPTAIN_NO, CAPTAIN_NO),
                (MEMBER_ID, None, MEMBER_NO),
            ]

    cursor = _Cursor()

    class _Conn:
        def cursor(self):  # noqa: ANN201
            return _CM(cursor)

    class _Pool:
        def acquire(self):  # noqa: ANN201
            return _CM(_Conn())

    repo = ldata.MySQLAccountRepo(_Pool())

    got = await repo.get_player_nos([CAPTAIN_ID, MEMBER_ID])

    assert got == {CAPTAIN_ID: CAPTAIN_NO, MEMBER_ID: MEMBER_NO}
    assert len(cursor.calls) == 1
    query, args = cursor.calls[0]
    assert "WHERE player_id IN (%s, %s)" in query
    assert args == (CAPTAIN_ID, MEMBER_ID)


def test_python_main_wires_the_real_signed_rpc_chain() -> None:
    import pathlib

    from pandorapy.services.login import conf as lconf
    from pandorapy.services.login import main as lmain
    from pandorapy.services.team import main as tmain
    from pandorapy.services.team import player_no_client

    team_cfg = tconf.Config()
    team_cfg.team.player_no_resolver_addr = "127.0.0.1:20001"
    team_cfg.team.player_no_resolver_auth_secret = AUTH_SECRET
    team_cfg.apply_defaults()
    login_cfg = lconf.Config()
    login_cfg.login.player_no_resolve_auth_secret = AUTH_SECRET
    login_cfg.apply_defaults()
    assert team_cfg.team.player_no_resolver_auth_audience == "login:player-no"
    assert login_cfg.login.player_no_resolve_auth_audience == "login:player-no"
    assert player_no_client.DEFAULT_TIMEOUT_SEC == 0.25
    assert lmain.PLAYER_NO_RESOLVE_MAX_CLOCK_SKEW_SEC == 30.0
    assert (
        lmain.PLAYER_NO_RESOLVE_NONCE_PREFIX
        == "pandora:login:player-no-resolve:nonce:"
    )

    team_main = pathlib.Path(tmain.__file__).read_text(encoding="utf-8")
    login_main = pathlib.Path(lmain.__file__).read_text(encoding="utf-8")
    assert "GrpcPlayerNoResolver" in team_main
    assert "uc.set_player_no_resolver(player_no_resolver)" in team_main
    assert "internalrpcauth.RedisReplayStore" in login_main
    assert "LoginInternalService(login_uc, player_no_verifier)" in login_main
    assert "add_LoginInternalServiceServicer_to_server" in login_main


def test_player_no_resolver_config_rejects_enabled_team_larger_than_batch_bound() -> None:
    enabled = tconf.Config()
    enabled.team.player_no_resolver_addr = "127.0.0.1:20001"
    enabled.team.player_no_resolver_auth_secret = AUTH_SECRET
    enabled.team.max_members = 33
    enabled.apply_defaults()
    with pytest.raises(ValueError, match=r"max_members.*\[1,32\]"):
        enabled.validate_player_no_resolver()

    disabled = tconf.Config()
    disabled.team.max_members = 33
    disabled.apply_defaults()
    disabled.validate_player_no_resolver()

    dangling_audience = tconf.Config()
    dangling_audience.team.player_no_resolver_auth_audience = "login:player-no"
    dangling_audience.apply_defaults()
    with pytest.raises(ValueError, match="requires team.player_no_resolver_addr"):
        dangling_audience.validate_player_no_resolver()


def test_player_no_auth_audience_defaults_only_when_feature_is_configured() -> None:
    from pandorapy.services.login import conf as lconf

    team_disabled = tconf.Config()
    team_disabled.apply_defaults()
    assert team_disabled.team.player_no_resolver_auth_audience == ""

    login_disabled = lconf.Config()
    login_disabled.apply_defaults()
    assert login_disabled.login.player_no_resolve_auth_audience == ""

    login_bad = lconf.Config()
    login_bad.login.player_no_resolve_auth_audience = "login:player-no"
    login_bad.apply_defaults()
    with pytest.raises(ValueError, match="requires player_no_resolve_auth_secret"):
        login_bad.validate_conf()
