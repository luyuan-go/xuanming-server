"""DS 读取角色展示编号的 Login RPC 等价契约。"""

import asyncio
import hashlib
import pathlib
import time
from types import SimpleNamespace

import jwt as pyjwt
import pytest
from pandora.login.v1 import login_pb2

from pandorapy import dsauth, errcode
from pandorapy.services.login import service as lsvc


class _Context:
    def __init__(self, metadata: dict[str, str] | None = None) -> None:
        self._metadata = tuple((metadata or {}).items())

    def invocation_metadata(self):  # noqa: ANN201
        return self._metadata


class _PlayerNoReader:
    def __init__(
        self,
        values: dict[int, int] | None = None,
        exc: BaseException | None = None,
    ) -> None:
        self.values = values or {}
        self.exc = exc
        self.calls: list[list[int]] = []

    async def resolve_player_nos(self, player_ids: list[int]) -> dict[int, int]:
        self.calls.append(list(player_ids))
        if self.exc is not None:
            raise self.exc
        return dict(self.values)


class _CredentialGuard:
    mode = dsauth.Mode.ENFORCE

    def __init__(self, credential) -> None:  # noqa: ANN001
        self.credential = credential
        self.scopes: list[dsauth.DSScope] = []

    def check_credential(self, context, scope):  # noqa: ANN001, ANN201
        del context
        self.scopes.append(scope)
        return object(), self.credential, 0


class _ActiveChecker:
    def __init__(
        self,
        admitted: list[int],
        exc: BaseException | None = None,
    ) -> None:
        self.admitted = admitted
        self.exc = exc
        self.calls: list[tuple[str, object]] = []

    async def check_active(self, pod: str, credential):  # noqa: ANN001, ANN201
        self.calls.append((pod, credential))
        if self.exc is not None:
            raise self.exc
        return SimpleNamespace(player_ids=list(self.admitted))


def test_login_proto_exposes_ds_player_no_rpc() -> None:
    """DS 批量查询必须走 canonical LoginService，不暴露独立裸内部服务入口。"""
    service = login_pb2.DESCRIPTOR.services_by_name["LoginService"]

    method = service.methods_by_name["ResolvePlayerNosForDS"]

    assert method.input_type.full_name == "pandora.login.v1.ResolvePlayerNosRequest"
    assert method.output_type.full_name == "pandora.login.v1.ResolvePlayerNosResponse"


@pytest.mark.asyncio
async def test_legacy_ds_player_no_uses_one_canonical_batch_and_keeps_zero() -> None:
    """legacy/off 本机直连兼容；展示读取不把未补号的 0 当故障。"""
    reader = _PlayerNoReader({1001: 700001, 1002: 0})
    svc = lsvc.LoginService(reader, None)

    response = await svc.ResolvePlayerNosForDS(  # type: ignore[attr-defined]
        login_pb2.ResolvePlayerNosRequest(player_ids=[1002, 1001, 1002]), _Context()
    )

    assert response.code == 0
    assert reader.calls == [[1001, 1002]]
    assert [(entry.player_id, entry.player_no) for entry in response.entries] == [
        (1001, 700001),
        (1002, 0),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "player_ids",
    [[], [1001, 0], list(range(1, 34))],
    ids=["empty", "zero", "over-raw-limit"],
)
async def test_ds_player_no_rejects_raw_bounds_before_authority(
    player_ids: list[int],
) -> None:
    reader = _PlayerNoReader()
    svc = lsvc.LoginService(reader, None)

    response = await svc.ResolvePlayerNosForDS(
        login_pb2.ResolvePlayerNosRequest(player_ids=player_ids), _Context()
    )

    assert response.code == errcode.ErrInvalidArg
    assert list(response.entries) == []
    assert reader.calls == []


@pytest.mark.asyncio
async def test_ds_player_no_accepts_raw_limit_in_one_batch() -> None:
    reader = _PlayerNoReader()
    svc = lsvc.LoginService(reader, None)
    player_ids = list(range(32, 0, -1))

    response = await svc.ResolvePlayerNosForDS(
        login_pb2.ResolvePlayerNosRequest(player_ids=player_ids), _Context()
    )

    assert response.code == 0
    assert reader.calls == [list(range(1, 33))]
    assert len(response.entries) == 32


@pytest.mark.asyncio
async def test_ds_player_no_missing_reader_is_unavailable() -> None:
    svc = lsvc.LoginService(None, None)  # type: ignore[arg-type]

    response = await svc.ResolvePlayerNosForDS(
        login_pb2.ResolvePlayerNosRequest(player_ids=[1001]), _Context()
    )

    assert response.code == errcode.ErrUnavailable
    assert list(response.entries) == []


@pytest.mark.asyncio
async def test_redis_ds_player_no_uses_bearer_credential_pod_then_active_authority() -> None:
    """request 无 pod；只能使用验签 credential 的 pod 查询 active 权威。"""
    reader = _PlayerNoReader({1001: 700001, 1002: 700002})
    credential = SimpleNamespace(ds_type="battle", pod="battle-1")
    guard = _CredentialGuard(credential)
    checker = _ActiveChecker([1001, 1002])
    svc = lsvc.LoginService(reader, None)

    svc.set_redis_ds_admission_authority(guard, checker)
    response = await svc.ResolvePlayerNosForDS(
        login_pb2.ResolvePlayerNosRequest(player_ids=[1002, 1001]), _Context()
    )

    assert response.code == 0
    assert len(guard.scopes) == 1 and guard.scopes[0].require_token is True
    assert checker.calls == [("battle-1", credential)]
    assert reader.calls == [[1001, 1002]]


@pytest.mark.asyncio
@pytest.mark.parametrize("guard", [None, SimpleNamespace(mode=dsauth.Mode.PERMISSIVE)])
async def test_redis_ds_player_no_incomplete_authority_fails_closed(guard) -> None:  # noqa: ANN001
    reader = _PlayerNoReader()
    svc = lsvc.LoginService(reader, None)
    svc.set_redis_ds_admission_authority(guard, None)

    response = await svc.ResolvePlayerNosForDS(
        login_pb2.ResolvePlayerNosRequest(player_ids=[1001]), _Context()
    )

    assert response.code == errcode.ErrUnavailable
    assert list(response.entries) == []
    assert reader.calls == []


@pytest.mark.asyncio
async def test_redis_ds_player_no_requires_complete_bearer_credential() -> None:
    reader = _PlayerNoReader()
    svc = lsvc.LoginService(reader, None)
    svc.set_redis_ds_admission_authority(
        _CredentialGuard(None), _ActiveChecker([1001])
    )

    response = await svc.ResolvePlayerNosForDS(
        login_pb2.ResolvePlayerNosRequest(player_ids=[1001]), _Context()
    )

    assert response.code == errcode.ErrUnauthorized
    assert list(response.entries) == []
    assert reader.calls == []


@pytest.mark.asyncio
async def test_redis_ds_player_no_propagates_active_authority_error_code() -> None:
    reader = _PlayerNoReader()
    svc = lsvc.LoginService(reader, None)
    credential = SimpleNamespace(ds_type="battle", pod="battle-1")
    checker = _ActiveChecker(
        [1001],
        errcode.PandoraError(errcode.ErrUnavailable, "redis unavailable"),
    )
    svc.set_redis_ds_admission_authority(_CredentialGuard(credential), checker)

    response = await svc.ResolvePlayerNosForDS(
        login_pb2.ResolvePlayerNosRequest(player_ids=[1001]), _Context()
    )

    assert response.code == errcode.ErrUnavailable
    assert list(response.entries) == []
    assert reader.calls == []


@pytest.mark.asyncio
async def test_battle_ds_cannot_resolve_player_outside_active_roster() -> None:
    reader = _PlayerNoReader()
    svc = lsvc.LoginService(reader, None)
    credential = SimpleNamespace(ds_type="battle", pod="battle-1")
    svc.set_redis_ds_admission_authority(
        _CredentialGuard(credential), _ActiveChecker([1001])
    )

    response = await svc.ResolvePlayerNosForDS(
        login_pb2.ResolvePlayerNosRequest(player_ids=[9999]), _Context()
    )

    assert response.code == errcode.ErrPermissionDeny
    assert list(response.entries) == []
    assert reader.calls == []


@pytest.mark.asyncio
async def test_ds_player_no_repo_failure_returns_no_partial_entries() -> None:
    reader = _PlayerNoReader(
        {1001: 700001},
        errcode.PandoraError(errcode.ErrUnavailable, "mysql unavailable"),
    )
    svc = lsvc.LoginService(reader, None)

    response = await svc.ResolvePlayerNosForDS(
        login_pb2.ResolvePlayerNosRequest(player_ids=[1001]), _Context()
    )

    assert response.code == errcode.ErrUnavailable
    assert list(response.entries) == []
    assert reader.calls == [[1001]]


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_at", ["active", "reader"])
async def test_ds_player_no_propagates_cancellation(cancel_at: str) -> None:
    reader = _PlayerNoReader(
        exc=asyncio.CancelledError() if cancel_at == "reader" else None
    )
    svc = lsvc.LoginService(reader, None)
    if cancel_at == "active":
        credential = SimpleNamespace(ds_type="battle", pod="battle-1")
        svc.set_redis_ds_admission_authority(
            _CredentialGuard(credential),
            _ActiveChecker([1001], asyncio.CancelledError()),
        )

    with pytest.raises(asyncio.CancelledError):
        await svc.ResolvePlayerNosForDS(
            login_pb2.ResolvePlayerNosRequest(player_ids=[1001]), _Context()
        )


def test_main_wires_redis_ds_player_no_authority_before_serving() -> None:
    """未来解除 v2 signer 闸时，Redis 档不能漏掉 endpoint 的 active checker。"""
    from pandorapy.services.login import main as lmain

    source = pathlib.Path(lmain.__file__).read_text(encoding="utf-8")
    wire = "svc.set_redis_ds_admission_authority("

    assert "ldsadmission.RedisDSAdmissionChecker(" in source
    assert source.index(wire) < source.index("build_grpc_server")


@pytest.mark.asyncio
async def test_redis_active_checker_reads_one_battle_authority_snapshot() -> None:
    """active 判定一次 MGET 同槽 auth+projection，不做任何 Redis 写。"""
    from pandora.ds.v1 import allocator_pb2 as dspb
    from pandorapy.services.login import dsadmission

    now_ms = int(time.time() * 1000)
    credential = dsauth.VerifiedCredential(
        ds_type="battle",
        match_id=9001,
        pod="battle-1",
        instance_uid="uid-b",
        protocol_epoch=4,
        gen=8,
        jti="credential-jti",
        exp_ms=now_ms + 60_000,
        kid="credential-kid",
        token_sha256="a" * 64,
        writer_epoch=2,
    )
    auth = dspb.BattleDSAuthStorageRecord(
        match_id=9001,
        ds_pod_name="battle-1",
        instance_uid="uid-b",
        instance_epoch=4,
        phase=dspb.BATTLE_AUTH_PHASE_ACTIVE,
        active=dspb.BattleDSCredential(
            gen=8,
            jti="credential-jti",
            exp_ms=credential.exp_ms,
            kid="credential-kid",
            instance_uid="uid-b",
            instance_epoch=4,
            token_sha256="a" * 64,
            writer_epoch=2,
        ),
        high_water_gen=8,
        required_writer_epoch=2,
        allocation_id="allocation-1",
        last_active_heartbeat_ms=now_ms,
    )
    projection = dspb.BattleStorageRecord(
        match_id=9001,
        ds_pod_name="battle-1",
        state="running",
        player_ids=[1001, 1002],
        last_heartbeat_ms=now_ms,
        gameserver_uid="uid-b",
        instance_epoch=4,
        last_verified_gen=8,
        last_verified_jti="credential-jti",
        last_verified_writer_epoch=2,
        allocation_id="allocation-1",
        release_track="stable",
    )

    class _Redis:
        def __init__(self) -> None:
            self.calls: list[tuple[str, ...]] = []

        async def mget(self, *keys: str):  # noqa: ANN201
            self.calls.append(keys)
            return [auth.SerializeToString(), projection.SerializeToString()]

    rdb = _Redis()
    checker = dsadmission.RedisDSAdmissionChecker(
        rdb, max_active_heartbeat_age_sec=30, now=lambda: now_ms / 1000
    )

    binding = await checker.check_active("battle-1", credential)

    assert rdb.calls == [
        ("pandora:ds:auth:{9001}", "pandora:ds:battle:{9001}")
    ]
    assert binding.player_ids == [1001, 1002]


@pytest.mark.asyncio
async def test_redis_active_checker_propagates_cancellation() -> None:
    """Redis await 被取消时不得包装成可重试的 unavailable。"""
    from pandorapy.services.login import dsadmission

    class _CancelledRedis:
        async def mget(self, *keys: str):  # noqa: ANN201
            del keys
            raise asyncio.CancelledError

    checker = dsadmission.RedisDSAdmissionChecker(
        _CancelledRedis(), max_active_heartbeat_age_sec=30
    )
    credential = dsauth.VerifiedCredential(
        ds_type="battle",
        match_id=9001,
        pod="battle-1",
        instance_uid="uid-b",
        protocol_epoch=4,
        gen=8,
        jti="credential-jti",
        exp_ms=int(time.time() * 1000) + 60_000,
        kid="credential-kid",
        token_sha256="a" * 64,
        writer_epoch=2,
    )

    with pytest.raises(asyncio.CancelledError):
        await checker.check_active("battle-1", credential)


def test_ds_guard_extracts_complete_active_credential_without_gateway_marker() -> None:
    """DS 可直连业务端口，但必须带完整且有效的 Bearer credential。"""
    secret = "python-login-ds-player-no-secret-32-bytes!!"
    kid = "credential-kid"
    token = pyjwt.encode(
        {
            "iss": "pandora-ds-control",
            "aud": "pandora-ds",
            "sub": "battle-1",
            "iat": int(time.time()),
            "exp": int(time.time()) + 60,
            "jti": "credential-jti",
            "ds_type": "battle",
            "match_id": 9001,
            "ds_uid": "uid-b",
            "ds_epoch": 4,
            "ds_gen": 8,
            "ds_writer_epoch": 2,
            "ds_kid": kid,
        },
        secret,
        algorithm="HS256",
        headers={"kid": kid},
    )
    verifier = dsauth.DSCallbackVerifier(
        issuer="pandora-ds-control",
        audience="pandora-ds",
        secret=secret,
        additional_secrets=[],
    )
    guard = dsauth.DSCallbackGuard(verifier, dsauth.Mode.ENFORCE)

    _, credential, code = guard.check_credential(
        _Context({"authorization": f"Bearer {token}"}),
        dsauth.DSScope(require_token=True),
    )

    assert code == 0
    assert credential is not None
    assert (
        credential.ds_type,
        credential.match_id,
        credential.pod,
        credential.instance_uid,
        credential.protocol_epoch,
        credential.gen,
        credential.jti,
        credential.kid,
        credential.writer_epoch,
    ) == ("battle", 9001, "battle-1", "uid-b", 4, 8, "credential-jti", kid, 2)
    assert credential.exp_ms > int(time.time() * 1000)
    assert credential.token_sha256 == hashlib.sha256(token.encode()).hexdigest()
