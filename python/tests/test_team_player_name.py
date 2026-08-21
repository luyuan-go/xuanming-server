"""Team 客户端视图的权威玩家名富化契约。"""

from __future__ import annotations

import asyncio
import pathlib
from types import SimpleNamespace

import pytest
from pandora.player.v1 import player_pb2
from pandora.team.v1 import team_pb2

from pandorapy import internalrpcauth
from pandorapy.services.team import biz as tbiz
from pandorapy.services.team import conf as tconf


CAPTAIN_ID = 8_628_135_489_420_001
MEMBER_ID = 8_628_135_489_420_002
AUTH_SECRET = "team-to-player-name-resolver-secret-v1"
RESOLVE_METHOD = "/pandora.player.v1.PlayerInternalService/ResolvePlayerNames"


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


def test_player_proto_exposes_internal_name_rpc() -> None:
    service = player_pb2.DESCRIPTOR.services_by_name["PlayerInternalService"]

    method = service.methods_by_name["ResolvePlayerNames"]

    assert method.input_type.full_name == "pandora.player.v1.GetPlayerNamesRequest"
    assert method.output_type.full_name == "pandora.player.v1.GetPlayerNamesResponse"


class _PlayerNameResolver:
    def __init__(
        self,
        values: dict[int, str] | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.values = values or {}
        self.error = error
        self.calls: list[list[int]] = []

    async def resolve_player_names(self, player_ids: list[int]) -> dict[int, str]:
        self.calls.append(list(player_ids))
        if self.error is not None:
            raise self.error
        return dict(self.values)


class _PlayerNoResolver:
    def __init__(self, values: dict[int, int]) -> None:
        self.values = values
        self.calls: list[list[int]] = []

    async def resolve_player_nos(self, player_ids: list[int]) -> dict[int, int]:
        self.calls.append(list(player_ids))
        return dict(self.values)


def _record() -> team_pb2.TeamStorageRecord:
    return team_pb2.TeamStorageRecord(
        team_id=7001,
        captain_id=CAPTAIN_ID,
        state=team_pb2.TEAM_STATE_FORMING,
        members=[
            team_pb2.TeamMemberStorageRecord(
                player_id=CAPTAIN_ID, nickname="forged-storage-captain"
            ),
            team_pb2.TeamMemberStorageRecord(
                player_id=MEMBER_ID, nickname="stale-storage-member"
            ),
        ],
        max_size=5,
    )


@pytest.mark.asyncio
async def test_team_to_proto_uses_one_authoritative_name_batch_and_never_storage() -> None:
    name_resolver = _PlayerNameResolver({CAPTAIN_ID: "Alice"})
    no_resolver = _PlayerNoResolver({CAPTAIN_ID: 100_001, MEMBER_ID: 100_002})
    uc = tbiz.TeamUsecase(None, None, tconf.TeamConf())
    uc.set_player_name_resolver(name_resolver)
    uc.set_player_no_resolver(no_resolver)

    team = await uc.team_to_proto(_record())

    assert name_resolver.calls == [[CAPTAIN_ID, MEMBER_ID]]
    assert [(m.player_id, m.nickname, m.player_no) for m in team.members] == [
        (CAPTAIN_ID, "Alice", 100_001),
        (MEMBER_ID, "", 100_002),
    ]


@pytest.mark.asyncio
async def test_name_and_player_no_weak_dependencies_run_concurrently() -> None:
    name_entered = asyncio.Event()
    no_entered = asyncio.Event()

    class _Names:
        async def resolve_player_names(self, _player_ids):  # noqa: ANN001, ANN201
            name_entered.set()
            await no_entered.wait()
            return {CAPTAIN_ID: "Alice"}

    class _Numbers:
        async def resolve_player_nos(self, _player_ids):  # noqa: ANN001, ANN201
            no_entered.set()
            await name_entered.wait()
            return {CAPTAIN_ID: 100_001}

    uc = tbiz.TeamUsecase(None, None, tconf.TeamConf())
    uc.set_player_name_resolver(_Names())
    uc.set_player_no_resolver(_Numbers())

    team = await asyncio.wait_for(uc.team_to_proto(_record()), timeout=0.5)

    assert team.members[0].nickname == "Alice"
    assert team.members[0].player_no == 100_001


@pytest.mark.asyncio
async def test_name_failure_is_empty_and_does_not_erase_identity_or_player_no() -> None:
    name_resolver = _PlayerNameResolver(error=RuntimeError("player unavailable"))
    no_resolver = _PlayerNoResolver({CAPTAIN_ID: 100_001, MEMBER_ID: 100_002})
    uc = tbiz.TeamUsecase(None, None, tconf.TeamConf())
    uc.set_player_name_resolver(name_resolver)
    uc.set_player_no_resolver(no_resolver)

    team = await uc.team_to_proto(_record())

    assert [(m.player_id, m.nickname, m.player_no) for m in team.members] == [
        (CAPTAIN_ID, "", 100_001),
        (MEMBER_ID, "", 100_002),
    ]


@pytest.mark.asyncio
async def test_team_projection_filters_zero_and_duplicate_ids_before_resolvers() -> None:
    record = _record()
    record.members.insert(
        1, team_pb2.TeamMemberStorageRecord(player_id=CAPTAIN_ID)
    )
    record.members.insert(2, team_pb2.TeamMemberStorageRecord(player_id=0))
    names = _PlayerNameResolver({CAPTAIN_ID: "Alice", MEMBER_ID: "Bob"})
    numbers = _PlayerNoResolver({CAPTAIN_ID: 100_001, MEMBER_ID: 100_002})
    uc = tbiz.TeamUsecase(None, None, tconf.TeamConf())
    uc.set_player_name_resolver(names)
    uc.set_player_no_resolver(numbers)

    await uc.team_to_proto(record)

    assert names.calls == [[CAPTAIN_ID, MEMBER_ID]]
    assert numbers.calls == [[CAPTAIN_ID, MEMBER_ID]]


@pytest.mark.asyncio
async def test_team_push_carries_authoritative_player_names() -> None:
    class _Pusher:
        def __init__(self) -> None:
            self.payloads: list[bytes] = []

        async def push_to_players(self, _caller, _players, payload, _event_type):  # noqa: ANN001, ANN201
            self.payloads.append(payload)
            return 1, None

    resolver = _PlayerNameResolver(
        {CAPTAIN_ID: "Alice", MEMBER_ID: "Bob"}
    )
    pusher = _Pusher()
    uc = tbiz.TeamUsecase(None, pusher, tconf.TeamConf())
    uc.set_player_name_resolver(resolver)

    await uc.push_update(0, [MEMBER_ID], _record(), 0, 0)

    assert resolver.calls == [[CAPTAIN_ID, MEMBER_ID]]
    event = team_pb2.TeamUpdateEvent.FromString(pusher.payloads[0])
    assert [member.nickname for member in event.team.members] == ["Alice", "Bob"]


@pytest.mark.asyncio
async def test_team_player_name_resolver_cancellation_propagates() -> None:
    resolver = _PlayerNameResolver(error=asyncio.CancelledError())
    uc = tbiz.TeamUsecase(None, None, tconf.TeamConf())
    uc.set_player_name_resolver(resolver)

    with pytest.raises(asyncio.CancelledError):
        await uc.team_to_proto(_record())


@pytest.mark.asyncio
async def test_grpc_name_resolver_signs_one_canonical_batch_with_250ms_budget() -> None:
    from pandora.common.v1 import errcode_pb2

    from pandorapy.services.team import player_name_client

    now_ms = lambda: 1_700_000_000_000  # noqa: E731
    signer = internalrpcauth.Signer(
        AUTH_SECRET, "team", "player:name", now_ms=now_ms
    )
    verifier = internalrpcauth.Verifier(
        AUTH_SECRET,
        "team",
        "player:name",
        30.0,
        _ReplayStore(),
        now_ms=now_ms,
    )

    class _Stub:
        def __init__(self) -> None:
            self.requests: list[list[int]] = []

        async def ResolvePlayerNames(self, request, *, timeout, metadata):  # noqa: N802, ANN001, ANN201
            assert timeout == 0.25
            self.requests.append(list(request.player_ids))
            await verifier.verify_with_payload(
                dict(metadata),
                RESOLVE_METHOD,
                request.player_ids[0],
                request.SerializeToString(deterministic=True),
            )
            return player_pb2.GetPlayerNamesResponse(
                code=errcode_pb2.OK,
                names=[
                    player_pb2.PlayerName(player_id=CAPTAIN_ID, nickname="Alice"),
                    player_pb2.PlayerName(player_id=MEMBER_ID, nickname="Bob"),
                ],
            )

    stub = _Stub()
    resolver = player_name_client.GrpcPlayerNameResolver("", signer, stub=stub)

    names = await resolver.resolve_player_names(
        [MEMBER_ID, CAPTAIN_ID, MEMBER_ID]
    )

    assert stub.requests == [[CAPTAIN_ID, MEMBER_ID]]
    assert names == {CAPTAIN_ID: "Alice", MEMBER_ID: "Bob"}


@pytest.mark.asyncio
async def test_grpc_name_resolver_rejects_invalid_raw_batches_without_rpc() -> None:
    from pandorapy.services.team import player_name_client

    signer = internalrpcauth.Signer(AUTH_SECRET, "team", "player:name")
    resolver = player_name_client.GrpcPlayerNameResolver("", signer, stub=object())

    for player_ids in ([], [CAPTAIN_ID, 0], [CAPTAIN_ID] * 33):
        with pytest.raises(ValueError):
            await resolver.resolve_player_names(player_ids)


@pytest.mark.asyncio
async def test_player_internal_service_authenticates_canonical_batch_before_one_read() -> None:
    from pandora.common.v1 import errcode_pb2

    from pandorapy.services.player import service as psvc

    class _Names:
        def __init__(self) -> None:
            self.calls: list[list[int]] = []

        async def get_player_names(self, player_ids: list[int]):  # noqa: ANN201
            self.calls.append(list(player_ids))
            return [
                SimpleNamespace(player_id=MEMBER_ID, nickname="Bob"),
                SimpleNamespace(player_id=9999, nickname="must-not-leak"),
                SimpleNamespace(player_id=CAPTAIN_ID, nickname="Alice"),
            ]

    class _Verifier:
        def __init__(self) -> None:
            self.calls: list[tuple[str, int, bytes]] = []

        async def verify_with_payload(self, _metadata, method, subject, payload) -> None:  # noqa: ANN001
            self.calls.append((method, subject, payload))

    names = _Names()
    verifier = _Verifier()
    svc = psvc.PlayerInternalService(names, verifier)
    request = player_pb2.GetPlayerNamesRequest(
        player_ids=[MEMBER_ID, CAPTAIN_ID, MEMBER_ID]
    )

    response = await svc.ResolvePlayerNames(request, _Context())

    canonical = player_pb2.GetPlayerNamesRequest(
        player_ids=[CAPTAIN_ID, MEMBER_ID]
    ).SerializeToString(deterministic=True)
    assert verifier.calls == [(RESOLVE_METHOD, CAPTAIN_ID, canonical)]
    assert names.calls == [[CAPTAIN_ID, MEMBER_ID]]
    assert response.code == errcode_pb2.OK
    assert [(item.player_id, item.nickname) for item in response.names] == [
        (CAPTAIN_ID, "Alice"),
        (MEMBER_ID, "Bob"),
    ]


@pytest.mark.asyncio
async def test_player_internal_service_rejects_bounds_and_missing_auth_before_read() -> None:
    from pandora.common.v1 import errcode_pb2

    from pandorapy.services.player import service as psvc

    class _Names:
        def __init__(self) -> None:
            self.calls: list[list[int]] = []

        async def get_player_names(self, player_ids: list[int]):  # noqa: ANN201
            self.calls.append(list(player_ids))
            return []

    names = _Names()
    svc = psvc.PlayerInternalService(names, None)
    invalid_requests = [
        player_pb2.GetPlayerNamesRequest(),
        player_pb2.GetPlayerNamesRequest(player_ids=[CAPTAIN_ID, 0]),
        player_pb2.GetPlayerNamesRequest(player_ids=[CAPTAIN_ID] * 33),
    ]

    for request in invalid_requests:
        response = await svc.ResolvePlayerNames(request, _Context())
        assert response.code == errcode_pb2.ERR_INVALID_ARG
    unavailable = await svc.ResolvePlayerNames(
        player_pb2.GetPlayerNamesRequest(player_ids=[CAPTAIN_ID]), _Context()
    )

    assert unavailable.code == errcode_pb2.ERR_UNAVAILABLE
    assert names.calls == []


@pytest.mark.asyncio
async def test_player_internal_service_query_failure_has_no_partial_names_and_cancel_propagates() -> None:
    from pandora.common.v1 import errcode_pb2

    from pandorapy import errcode
    from pandorapy.services.player import service as psvc

    class _Verifier:
        async def verify_with_payload(self, *_args) -> None:  # noqa: ANN002
            return None

    class _Names:
        def __init__(self, error: BaseException) -> None:
            self.error = error

        async def get_player_names(self, _player_ids):  # noqa: ANN001, ANN201
            raise self.error

    request = player_pb2.GetPlayerNamesRequest(player_ids=[CAPTAIN_ID])
    failed = psvc.PlayerInternalService(
        _Names(errcode.PandoraError(errcode.ErrUnavailable, "mysql unavailable")),
        _Verifier(),
    )
    response = await failed.ResolvePlayerNames(request, _Context())
    assert response.code == errcode_pb2.ERR_UNAVAILABLE
    assert list(response.names) == []

    cancelled = psvc.PlayerInternalService(
        _Names(asyncio.CancelledError()), _Verifier()
    )
    with pytest.raises(asyncio.CancelledError):
        await cancelled.ResolvePlayerNames(request, _Context())


def test_player_name_resolver_config_defaults_and_fails_closed() -> None:
    from pandorapy.services.player import conf as pconf

    team_cfg = tconf.Config()
    team_cfg.team.player_name_resolver_addr = "127.0.0.1:20002"
    team_cfg.team.player_name_resolver_auth_secret = AUTH_SECRET
    team_cfg.apply_defaults()
    team_cfg.validate_player_name_resolver()
    assert team_cfg.team.player_name_resolver_auth_audience == "player:name"

    player_cfg = pconf.Config()
    player_cfg.player.player_name_resolve_auth_secret = AUTH_SECRET
    # ★ 配了内部 RPC 鉴权就必须有 Redis 重放权威 —— 验签靠 nonce 去重挡重放,
    # 没有共享 Redis 时多副本各记各的,同一个签名在别的副本上照样能重放一次。
    # 这条在 conf.validate_player_name_resolver 里是 fail-closed 的,所以最小可用
    # 配置**必须**含 redis;不给就等于在测一个真实跑不起来的组合。
    player_cfg.node.redis_client.host = "127.0.0.1:6379"
    player_cfg.apply_defaults()
    player_cfg.validate_player_name_resolver()
    assert player_cfg.player.player_name_resolve_auth_audience == "player:name"

    # 反向钉住上面那条规则本身:有鉴权、没 Redis → 必须拒。
    no_replay = pconf.Config()
    no_replay.player.player_name_resolve_auth_secret = AUTH_SECRET
    no_replay.apply_defaults()
    with pytest.raises(ValueError, match="replay authority"):
        no_replay.validate_player_name_resolver()

    dangling_team = tconf.Config()
    dangling_team.team.player_name_resolver_auth_audience = "player:name"
    dangling_team.apply_defaults()
    with pytest.raises(ValueError, match="requires team.player_name_resolver_addr"):
        dangling_team.validate_player_name_resolver()

    dangling_player = pconf.Config()
    dangling_player.player.player_name_resolve_auth_audience = "player:name"
    dangling_player.apply_defaults()
    with pytest.raises(ValueError, match="requires player_name_resolve_auth_secret"):
        dangling_player.validate_player_name_resolver()


def test_python_mains_wire_signed_player_name_rpc_and_shared_replay() -> None:
    from pandorapy.services.player import main as pmain
    from pandorapy.services.team import main as tmain

    team_source = pathlib.Path(tmain.__file__).read_text(encoding="utf-8")
    player_source = pathlib.Path(pmain.__file__).read_text(encoding="utf-8")

    assert "GrpcPlayerNameResolver(" in team_source
    assert "uc.set_player_name_resolver(player_name_resolver)" in team_source
    assert "player_name_resolver_auth_secret" in team_source
    assert "internalrpcauth.RedisReplayStore(" in player_source
    assert '"pandora:player:name-resolve:nonce:"' in player_source
    assert "PlayerInternalService(uc, player_name_verifier)" in player_source
    assert "add_PlayerInternalServiceServicer_to_server" in player_source
