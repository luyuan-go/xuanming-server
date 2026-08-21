"""好友/公会申请列表的公开玩家展示投影。

申请表只保存不可变的 ``player_id``；列表读取时分别向 player/login 权威解析
角色昵称和 ``player_no``。两条弱依赖彼此独立，任何一条失败都不能抹掉另一条，
更不能把账号登录名下发给客户端。
"""

from __future__ import annotations

import asyncio

import pytest
from pandora.common.v1 import errcode_pb2
from pandora.friend.v1 import friend_pb2
from pandora.guild.v1 import guild_pb2
from pandora.login.v1 import login_pb2
from pandora.player.v1 import player_pb2

from pandorapy import internalrpcauth
from pandorapy.services import player_display
from pandorapy.services.friend import biz as fbiz
from pandorapy.services.friend import conf as fconf
from pandorapy.services.guild import biz as gbiz
from pandorapy.services.guild import conf as gconf
from pandorapy.services.guild import rows as grows


class _ReplayStore:
    def __init__(self) -> None:
        self.keys: set[str] = set()

    async def consume(self, nonce_key: str, _ttl_sec: float) -> bool:
        if nonce_key in self.keys:
            return False
        self.keys.add(nonce_key)
        return True


class _FriendRepo:
    def __init__(self, rows: list[tuple[int, int, int]]) -> None:
        self.rows = rows

    async def list_incoming_requests(self, _player_id: int):
        return list(self.rows)

    async def recommend_by_mutual(self, *_args):
        return []

    async def recommend_random(self, *_args):
        return []


class _GuildRepo:
    def __init__(self, rows: list[grows.GuildJoinRequestRow]) -> None:
        self.rows = rows

    async def get_member(self, player_id: int):
        return grows.GuildMemberRow(
            player_id, 700, grows.GUILD_ROLE_LEADER, 1
        )

    async def list_pending_requests(self, guild_id: int, cursor: int, limit: int):
        assert guild_id == 700
        rows = [row for row in self.rows if not cursor or row.request_id > cursor]
        return rows[:limit]


class _ConcurrencyProbe:
    def __init__(self, expected_peak: int = 4) -> None:
        self.active = 0
        self.max_active = 0
        self.expected_peak = expected_peak
        self.peak_reached = asyncio.Event()
        self.release = asyncio.Event()

    async def enter(self) -> None:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        if self.active >= self.expected_peak:
            self.peak_reached.set()
        await self.release.wait()

    def leave(self) -> None:
        self.active -= 1


class _NameResolver:
    def __init__(
        self,
        *,
        probe: _ConcurrencyProbe | None = None,
        fail_on: set[int] | None = None,
        cancel: bool = False,
    ) -> None:
        self.calls: list[list[int]] = []
        self.probe = probe
        self.fail_on = fail_on or set()
        self.cancel = cancel

    async def resolve_player_names(self, player_ids: list[int]) -> dict[int, str]:
        self.calls.append(list(player_ids))
        if self.cancel:
            raise asyncio.CancelledError
        if self.probe is not None:
            await self.probe.enter()
        try:
            if any(player_id in self.fail_on for player_id in player_ids):
                raise RuntimeError("player authority unavailable")
            return {player_id: f"角色{player_id}" for player_id in player_ids}
        finally:
            if self.probe is not None:
                self.probe.leave()


class _NoResolver:
    def __init__(
        self,
        *,
        probe: _ConcurrencyProbe | None = None,
        fail_on: set[int] | None = None,
        cancel: bool = False,
    ) -> None:
        self.calls: list[list[int]] = []
        self.probe = probe
        self.fail_on = fail_on or set()
        self.cancel = cancel

    async def resolve_player_nos(self, player_ids: list[int]) -> dict[int, int]:
        self.calls.append(list(player_ids))
        if self.cancel:
            raise asyncio.CancelledError
        if self.probe is not None:
            await self.probe.enter()
        try:
            if any(player_id in self.fail_on for player_id in player_ids):
                raise RuntimeError("login authority unavailable")
            return {player_id: 100_000 + player_id for player_id in player_ids}
        finally:
            if self.probe is not None:
                self.probe.leave()


def _friend_usecase(rows: list[tuple[int, int, int]]) -> fbiz.FriendUsecase:
    cfg = fconf.Config()
    cfg.apply_defaults()
    return fbiz.FriendUsecase(_FriendRepo(rows), None, None, cfg.friend)


def _guild_usecase(rows: list[grows.GuildJoinRequestRow]) -> gbiz.GuildUsecase:
    cfg = gconf.Config()
    cfg.apply_defaults()
    return gbiz.GuildUsecase(_GuildRepo(rows), None, None, cfg.guild)


async def test_friend_list_projects_deduped_chunked_names_and_nos_with_one_bound() -> None:
    """两个 authority 共用总并发上限 4；每批最多 32，ID 稳定去重。"""
    unique_ids = list(range(1, 71))
    # 同一玩家出现两条仅用于锁死防御性去重；输出本身仍保持申请行顺序/基数。
    request_ids = unique_ids + [1, 33]
    rows = [(index + 1, player_id, 1_000 + index) for index, player_id in enumerate(request_ids)]
    probe = _ConcurrencyProbe()
    names = _NameResolver(probe=probe)
    numbers = _NoResolver(probe=probe)
    uc = _friend_usecase(rows)
    uc.set_player_name_resolver(names)
    uc.set_player_no_resolver(numbers)

    task = asyncio.create_task(uc.list_friend_requests(900))
    await asyncio.wait_for(probe.peak_reached.wait(), timeout=1.0)
    assert probe.max_active == 4
    assert not task.done(), "第 5/6 批不应绕过总并发上限"
    probe.release.set()
    out = await task

    assert probe.max_active == 4
    assert all(0 < len(batch) <= 32 for batch in names.calls + numbers.calls)
    assert [pid for batch in names.calls for pid in batch] == unique_ids
    assert [pid for batch in numbers.calls for pid in batch] == unique_ids
    assert [item.from_player_id for item in out] == request_ids
    assert [item.from_nickname for item in out] == [f"角色{pid}" for pid in request_ids]
    assert [item.from_player_no for item in out] == [100_000 + pid for pid in request_ids]


async def test_friend_projection_is_partial_and_name_no_fail_independently() -> None:
    rows = [(player_id, player_id, player_id) for player_id in range(1, 66)]
    names = _NameResolver(fail_on={33})  # 仅第 2 个 32-ID 分块失败
    numbers = _NoResolver(fail_on={1})  # 仅第 1 个分块失败
    uc = _friend_usecase(rows)
    uc.set_player_name_resolver(names)
    uc.set_player_no_resolver(numbers)

    out = await uc.list_friend_requests(900)

    assert out[0].from_nickname == "角色1"
    assert out[0].from_player_no == 0
    assert out[32].from_nickname == ""
    assert out[32].from_player_no == 100_033
    assert out[64].from_nickname == "角色65"
    assert out[64].from_player_no == 100_065


@pytest.mark.parametrize("which", ["name", "no"])
async def test_friend_projection_propagates_cancellation(which: str) -> None:
    uc = _friend_usecase([(1, 7, 9)])
    uc.set_player_name_resolver(_NameResolver(cancel=which == "name"))
    uc.set_player_no_resolver(_NoResolver(cancel=which == "no"))
    with pytest.raises(asyncio.CancelledError):
        await uc.list_friend_requests(900)


async def test_guild_list_projects_public_name_and_player_no() -> None:
    rows = [
        grows.GuildJoinRequestRow(11, 700, 21, grows.JOIN_STATUS_PENDING, 101),
        grows.GuildJoinRequestRow(12, 700, 22, grows.JOIN_STATUS_PENDING, 102),
    ]
    uc = _guild_usecase(rows)
    uc.set_player_name_resolver(_NameResolver())
    uc.set_player_no_resolver(_NoResolver())

    out, next_cursor = await uc.list_join_requests(800, 0, 50)

    assert next_cursor == 0
    assert [(x.from_nickname, x.from_player_no) for x in out] == [
        ("角色21", 100_021),
        ("角色22", 100_022),
    ]


@pytest.mark.parametrize(
    ("caller", "name_secret", "no_secret"),
    [
        (
            "friend",
            "friend-player-name-resolver-auth-secret-v1",
            "friend-player-number-resolver-auth-secret-v1",
        ),
        (
            "guild",
            "guild-player-name-resolver-auth-secret-v1",
            "guild-player-number-resolver-auth-secret-v1",
        ),
    ],
)
async def test_grpc_display_resolvers_sign_canonical_batches_and_filter_responses(
    caller: str, name_secret: str, no_secret: str
) -> None:
    """friend/guild 两个 caller 都须精确签规范批次，并拒绝越界响应泄漏。"""
    now_ms = lambda: 1_700_000_000_000  # noqa: E731
    name_signer = internalrpcauth.Signer(
        name_secret, caller, "player:name", now_ms=now_ms
    )
    no_signer = internalrpcauth.Signer(
        no_secret, caller, "login:player-no", now_ms=now_ms
    )
    name_verifier = internalrpcauth.Verifier(
        name_secret,
        caller,
        "player:name",
        30.0,
        _ReplayStore(),
        now_ms=now_ms,
    )
    no_verifier = internalrpcauth.Verifier(
        no_secret,
        caller,
        "login:player-no",
        30.0,
        _ReplayStore(),
        now_ms=now_ms,
    )

    class _NameStub:
        def __init__(self) -> None:
            self.requests: list[list[int]] = []

        async def ResolvePlayerNames(self, request, *, timeout, metadata):  # noqa: N802, ANN001, ANN201
            assert timeout == player_display.DEFAULT_TIMEOUT_SEC == 0.25
            self.requests.append(list(request.player_ids))
            await name_verifier.verify_with_payload(
                dict(metadata),
                player_display.RESOLVE_PLAYER_NAMES_METHOD,
                request.player_ids[0],
                request.SerializeToString(deterministic=True),
            )
            return player_pb2.GetPlayerNamesResponse(
                code=errcode_pb2.OK,
                names=[
                    player_pb2.PlayerName(player_id=21, nickname="角色21"),
                    player_pb2.PlayerName(player_id=999, nickname="不可泄漏"),
                ],
            )

    class _NoStub:
        def __init__(self) -> None:
            self.requests: list[list[int]] = []

        async def ResolvePlayerNos(self, request, *, timeout, metadata):  # noqa: N802, ANN001, ANN201
            assert timeout == player_display.DEFAULT_TIMEOUT_SEC == 0.25
            self.requests.append(list(request.player_ids))
            await no_verifier.verify_with_payload(
                dict(metadata),
                player_display.RESOLVE_PLAYER_NOS_METHOD,
                request.player_ids[0],
                request.SerializeToString(deterministic=True),
            )
            return login_pb2.ResolvePlayerNosResponse(
                code=errcode_pb2.OK,
                entries=[
                    login_pb2.ResolvedPlayerNo(player_id=22, player_no=100_022),
                    login_pb2.ResolvedPlayerNo(player_id=999, player_no=999_999),
                ],
            )

    name_stub = _NameStub()
    no_stub = _NoStub()
    name_resolver = player_display.GrpcPlayerNameResolver(
        "", name_signer, stub=name_stub
    )
    no_resolver = player_display.GrpcPlayerNoResolver("", no_signer, stub=no_stub)

    names = await name_resolver.resolve_player_names([22, 21, 22])
    numbers = await no_resolver.resolve_player_nos([22, 21, 22])

    assert name_stub.requests == [[21, 22]]
    assert no_stub.requests == [[21, 22]]
    assert names == {21: "角色21"}
    assert numbers == {22: 100_022}


async def test_grpc_display_resolvers_reject_in_band_authority_failures() -> None:
    class _NameFailureStub:
        async def ResolvePlayerNames(self, _request, **_kwargs):  # noqa: N802, ANN001, ANN201
            return player_pb2.GetPlayerNamesResponse(code=errcode_pb2.ERR_UNAVAILABLE)

    class _NoFailureStub:
        async def ResolvePlayerNos(self, _request, **_kwargs):  # noqa: N802, ANN001, ANN201
            return login_pb2.ResolvePlayerNosResponse(code=errcode_pb2.ERR_UNAVAILABLE)

    name_resolver = player_display.GrpcPlayerNameResolver(
        "",
        internalrpcauth.Signer(
            "friend-player-name-resolver-auth-secret-v1",
            "friend",
            "player:name",
        ),
        stub=_NameFailureStub(),
    )
    no_resolver = player_display.GrpcPlayerNoResolver(
        "",
        internalrpcauth.Signer(
            "friend-player-number-resolver-auth-secret-v1",
            "friend",
            "login:player-no",
        ),
        stub=_NoFailureStub(),
    )

    with pytest.raises(Exception, match="ResolvePlayerNames"):
        await name_resolver.resolve_player_names([21])
    with pytest.raises(RuntimeError, match="ResolvePlayerNos"):
        await no_resolver.resolve_player_nos([21])


def test_application_contract_never_exposes_account_login_name() -> None:
    """公开申请视图只能带角色昵称/编号/ID，不得混入 accounts.account。"""
    for message in (friend_pb2.FriendRequestInfo, guild_pb2.GuildJoinRequest):
        fields = {field.name for field in message.DESCRIPTOR.fields}
        assert "from_nickname" in fields
        assert "from_player_no" in fields
        assert "from_player_id" in fields
        assert "account" not in fields
        assert "account_name" not in fields


def test_approval_identity_remains_request_id_only() -> None:
    """昵称/编号只是展示投影，审批仍只认不可变 request_id。"""
    for message in (
        friend_pb2.AcceptFriendRequest,
        friend_pb2.RejectFriendRequest,
        guild_pb2.ApproveJoinRequest,
        guild_pb2.RejectJoinRequest,
    ):
        assert {field.name for field in message.DESCRIPTOR.fields} == {"request_id"}
