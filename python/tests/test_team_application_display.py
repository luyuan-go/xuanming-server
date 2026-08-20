"""队长侧入队申请列表的权威名字/编号展示投影契约。"""

from __future__ import annotations

import asyncio

import pytest
from pandora.team.v1 import team_pb2

from pandorapy.services.team import biz as tbiz
from pandorapy.services.team import conf as tconf
from pandorapy.services.team.data import ApplicationRecord


class _NameResolver:
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


class _NoResolver:
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


@pytest.mark.asyncio
async def test_application_projection_uses_one_authoritative_batch_per_field() -> None:
    names = _NameResolver({11: "Alice"})
    numbers = _NoResolver({11: 100_011, 22: 100_022})
    uc = tbiz.TeamUsecase(None, None, tconf.TeamConf())
    uc.set_player_name_resolver(names)
    uc.set_player_no_resolver(numbers)
    records = [
        ApplicationRecord(11, 101),
        ApplicationRecord(22, 202),
        ApplicationRecord(11, 303),
    ]

    applications = await uc.team_applications_to_proto(7001, records)

    assert names.calls == [[11, 22]]
    assert numbers.calls == [[11, 22]]
    assert [
        (a.player_id, a.nickname, a.player_no, a.expires_at_ms)
        for a in applications
    ] == [
        (11, "Alice", 100_011, 101),
        (22, "", 100_022, 202),
        (11, "Alice", 100_011, 303),
    ]


@pytest.mark.asyncio
async def test_application_display_dependencies_are_concurrent_and_independent() -> None:
    name_entered = asyncio.Event()
    no_entered = asyncio.Event()

    class _Names:
        async def resolve_player_names(self, _player_ids):  # noqa: ANN001, ANN201
            name_entered.set()
            await no_entered.wait()
            return {11: "Alice"}

    class _Numbers:
        async def resolve_player_nos(self, _player_ids):  # noqa: ANN001, ANN201
            no_entered.set()
            await name_entered.wait()
            return {11: 100_011}

    uc = tbiz.TeamUsecase(None, None, tconf.TeamConf())
    uc.set_player_name_resolver(_Names())
    uc.set_player_no_resolver(_Numbers())

    applications = await asyncio.wait_for(
        uc.team_applications_to_proto(7001, [ApplicationRecord(11, 101)]),
        timeout=0.5,
    )

    assert (applications[0].nickname, applications[0].player_no) == (
        "Alice",
        100_011,
    )


@pytest.mark.asyncio
async def test_application_display_failures_are_fail_soft_and_cancel_propagates() -> None:
    records = [ApplicationRecord(11, 101)]

    uc = tbiz.TeamUsecase(None, None, tconf.TeamConf())
    uc.set_player_name_resolver(_NameResolver(error=RuntimeError("player down")))
    uc.set_player_no_resolver(_NoResolver({11: 100_011}))
    applications = await uc.team_applications_to_proto(7001, records)
    assert (applications[0].nickname, applications[0].player_no) == ("", 100_011)

    uc = tbiz.TeamUsecase(None, None, tconf.TeamConf())
    uc.set_player_name_resolver(_NameResolver({11: "Alice"}))
    uc.set_player_no_resolver(_NoResolver(error=RuntimeError("login down")))
    applications = await uc.team_applications_to_proto(7001, records)
    assert (applications[0].nickname, applications[0].player_no) == ("Alice", 0)

    uc = tbiz.TeamUsecase(None, None, tconf.TeamConf())
    uc.set_player_name_resolver(_NameResolver(error=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        await uc.team_applications_to_proto(7001, records)


@pytest.mark.asyncio
async def test_empty_application_projection_skips_resolvers() -> None:
    names = _NameResolver()
    numbers = _NoResolver()
    uc = tbiz.TeamUsecase(None, None, tconf.TeamConf())
    uc.set_player_name_resolver(names)
    uc.set_player_no_resolver(numbers)

    assert await uc.team_applications_to_proto(7001, []) == []
    assert names.calls == []
    assert numbers.calls == []


def test_enabled_display_resolvers_bound_application_batch_to_32() -> None:
    cfg = tconf.Config()
    cfg.team.player_name_resolver_addr = "player:20002"
    cfg.team.player_name_resolver_auth_secret = "team-player-name-test-secret-0123456789"
    cfg.team.player_no_resolver_addr = "login:20001"
    cfg.team.player_no_resolver_auth_secret = "team-player-no-test-secret-0123456789"
    cfg.team.max_applications_per_team = 33
    cfg.apply_defaults()

    with pytest.raises(ValueError, match=r"max_applications_per_team.*\[1,32\]"):
        cfg.validate_player_name_resolver()
    with pytest.raises(ValueError, match=r"max_applications_per_team.*\[1,32\]"):
        cfg.validate_player_no_resolver()

    cfg.team.max_applications_per_team = 10
    cfg.team.max_open_teams_per_query = 33
    with pytest.raises(ValueError, match=r"max_open_teams_per_query.*\[1,32\]"):
        cfg.validate_player_name_resolver()
    with pytest.raises(ValueError, match=r"max_open_teams_per_query.*\[1,32\]"):
        cfg.validate_player_no_resolver()


class _OpenTeamsRepo:
    def __init__(self, teams: list[team_pb2.TeamStorageRecord]) -> None:
        self.teams = {int(team.team_id): team for team in teams}

    async def list_open_team_ids(self, _map_id: int, _limit: int) -> list[int]:
        return list(self.teams)

    async def get(self, team_id: int):  # noqa: ANN201
        return self.teams[team_id], True

    async def remove_open_team_candidate(self, _team_id: int, _map_id: int) -> None:
        raise AssertionError("valid open teams must not be pruned")


@pytest.mark.asyncio
async def test_open_team_projection_resolves_each_captain_in_one_batch() -> None:
    teams = [
        team_pb2.TeamStorageRecord(
            team_id=7001,
            captain_id=11,
            state=team_pb2.TEAM_STATE_FORMING,
            members=[team_pb2.TeamMemberStorageRecord(player_id=11)],
            max_size=5,
        ),
        team_pb2.TeamStorageRecord(
            team_id=7002,
            captain_id=22,
            state=team_pb2.TEAM_STATE_FORMING,
            members=[team_pb2.TeamMemberStorageRecord(player_id=22)],
            max_size=5,
        ),
    ]
    names = _NameResolver({11: "Alice"})
    numbers = _NoResolver({11: 100_011, 22: 100_022})
    uc = tbiz.TeamUsecase(_OpenTeamsRepo(teams), None, tconf.TeamConf())
    uc.set_player_name_resolver(names)
    uc.set_player_no_resolver(numbers)

    briefs = await uc.list_open_teams(0, 10)

    assert names.calls == [[11, 22]]
    assert numbers.calls == [[11, 22]]
    assert [
        (brief.captain_id, brief.captain_nickname, brief.captain_player_no)
        for brief in briefs
    ] == [(11, "Alice", 100_011), (22, "", 100_022)]
