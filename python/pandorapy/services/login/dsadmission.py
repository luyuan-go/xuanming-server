"""Login 对 DS callback credential 的 Redis active 权威校验。"""

from __future__ import annotations

import asyncio
import dataclasses
import hmac
import time

from pandora.ds.v1 import allocator_pb2 as dspb
from pandora.hub.v1 import allocator_pb2 as hubpb

from pandorapy import auth, dsauth, errcode


@dataclasses.dataclass(frozen=True, slots=True)
class DSAdmissionBinding:
    ds_type: str
    match_id: int
    pod_name: str
    instance_uid: str
    protocol_epoch: int
    credential_gen: int
    credential_jti: str
    exp_ms: int
    kid: str
    token_sha256: str
    writer_epoch: int
    allocation_id: str = ""
    release_track: str = ""
    player_ids: list[int] = dataclasses.field(default_factory=list)


def _hub_auth_key(pod: str) -> str:
    return f"pandora:hub:auth:{{{pod}}}"


def _hub_projection_key(pod: str) -> str:
    return f"pandora:hub:shard:{{{pod}}}"


def _battle_auth_key(match_id: int) -> str:
    return f"pandora:ds:auth:{{{match_id}}}"


def _battle_projection_key(match_id: int) -> str:
    return f"pandora:ds:battle:{{{match_id}}}"


def _as_bytes(value) -> bytes:  # noqa: ANN001
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise TypeError(f"unexpected redis value type {type(value).__name__}")


class RedisDSAdmissionChecker:
    """一次 MGET 读取 auth+projection，证明 callback credential 仍为 active。"""

    __slots__ = ("_rdb", "_max_age_sec", "_now")

    def __init__(
        self,
        rdb,  # noqa: ANN001
        *,
        max_active_heartbeat_age_sec: float,
        now=time.time,  # noqa: B008
    ) -> None:
        self._rdb = rdb
        self._max_age_sec = (
            max_active_heartbeat_age_sec
            if max_active_heartbeat_age_sec > 0
            else 30.0
        )
        self._now = now

    async def check_active(
        self, pod: str, credential: dsauth.VerifiedCredential
    ) -> DSAdmissionBinding:
        if (
            not pod
            or credential is None
            or credential.pod != pod
            or credential.ds_type not in ("hub", "battle")
            or not credential.instance_uid
            or credential.protocol_epoch <= 0
            or credential.gen <= 0
            or not credential.jti
            or credential.exp_ms <= 0
            or not credential.kid
            or not credential.token_sha256
            or credential.writer_epoch != auth.DS_AUTH_WRITER_EPOCH_V2
            or (credential.ds_type == "battle" and credential.match_id <= 0)
            or (credential.ds_type == "hub" and credential.match_id != 0)
        ):
            raise errcode.PandoraError(
                errcode.ErrUnauthorized,
                "ds admission credential is incomplete or scope mismatched",
            )
        if self._rdb is None or self._now is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "ds admission authority is unavailable"
            )
        if credential.ds_type == "hub":
            return await self._check_hub(pod, credential)
        return await self._check_battle(pod, credential)

    async def _mget(self, first: str, second: str) -> tuple[bytes, bytes]:
        try:
            values = await self._rdb.mget(first, second)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "read ds admission authority failed: %s", exc
            ) from exc
        if len(values) != 2 or values[0] is None or values[1] is None:
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "ds admission credential is not active"
            )
        try:
            return _as_bytes(values[0]), _as_bytes(values[1])
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "decode ds admission Redis value failed: %s", exc
            ) from exc

    async def _check_hub(
        self, pod: str, credential: dsauth.VerifiedCredential
    ) -> DSAdmissionBinding:
        auth_raw, projection_raw = await self._mget(
            _hub_auth_key(pod), _hub_projection_key(pod)
        )
        auth_rec = hubpb.HubShardAuthStorageRecord()
        projection = hubpb.HubShardStorageRecord()
        try:
            auth_rec.ParseFromString(auth_raw)
            projection.ParseFromString(projection_raw)
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "decode hub admission authority failed: %s", exc
            ) from exc
        now_ms = int(self._now() * 1000)
        max_age_ms = int(self._max_age_sec * 1000)
        active = auth_rec.active if auth_rec.HasField("active") else None
        pending_bad = (
            auth_rec.HasField("pending")
            and auth_rec.pending.writer_epoch != auth.DS_AUTH_WRITER_EPOCH_V2
        )
        if (
            now_ms <= 0
            or active is None
            or auth_rec.phase
            not in (hubpb.HUB_AUTH_PHASE_ACTIVE, hubpb.HUB_AUTH_PHASE_ROTATING)
            or auth_rec.pod_name != pod
            or auth_rec.instance_uid != credential.instance_uid
            or auth_rec.protocol_epoch != credential.protocol_epoch
            or auth_rec.required_writer_epoch != auth.DS_AUTH_WRITER_EPOCH_V2
            or pending_bad
            or auth_rec.high_water_gen < active.gen
            or not self._heartbeat_fresh(auth_rec.last_active_heartbeat_ms, now_ms, max_age_ms)
            or projection.hub_pod_name != pod
            or projection.state != "ready"
            or projection.release_track not in ("", "stable", "canary")
            or projection.gameserver_uid != credential.instance_uid
            or projection.auth_epoch != credential.protocol_epoch
            or projection.last_verified_gen != credential.gen
            or projection.last_verified_jti != credential.jti
            or projection.last_verified_writer_epoch != auth.DS_AUTH_WRITER_EPOCH_V2
            or not self._heartbeat_fresh(projection.last_heartbeat_ms, now_ms, max_age_ms)
            or not self._active_matches(active, credential, now_ms, hub=True)
        ):
            raise errcode.PandoraError(
                errcode.ErrUnauthorized,
                "hub admission credential does not match active authority",
            )
        return self._binding(credential, release_track=projection.release_track or "stable")

    async def _check_battle(
        self, pod: str, credential: dsauth.VerifiedCredential
    ) -> DSAdmissionBinding:
        auth_raw, projection_raw = await self._mget(
            _battle_auth_key(credential.match_id),
            _battle_projection_key(credential.match_id),
        )
        auth_rec = dspb.BattleDSAuthStorageRecord()
        projection = dspb.BattleStorageRecord()
        try:
            auth_rec.ParseFromString(auth_raw)
            projection.ParseFromString(projection_raw)
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "decode battle admission authority failed: %s", exc
            ) from exc
        now_ms = int(self._now() * 1000)
        max_age_ms = int(self._max_age_sec * 1000)
        active = auth_rec.active if auth_rec.HasField("active") else None
        pending_bad = (
            auth_rec.HasField("pending")
            and auth_rec.pending.writer_epoch != auth.DS_AUTH_WRITER_EPOCH_V2
        )
        if (
            now_ms <= 0
            or active is None
            or auth_rec.phase
            not in (dspb.BATTLE_AUTH_PHASE_ACTIVE, dspb.BATTLE_AUTH_PHASE_ROTATING)
            or auth_rec.match_id != credential.match_id
            or auth_rec.ds_pod_name != pod
            or not auth_rec.allocation_id
            or auth_rec.instance_uid != credential.instance_uid
            or auth_rec.instance_epoch != credential.protocol_epoch
            or auth_rec.required_writer_epoch != auth.DS_AUTH_WRITER_EPOCH_V2
            or pending_bad
            or auth_rec.high_water_gen < active.gen
            or not self._heartbeat_fresh(auth_rec.last_active_heartbeat_ms, now_ms, max_age_ms)
            or projection.match_id != credential.match_id
            or projection.allocation_id != auth_rec.allocation_id
            or projection.release_track not in ("", "stable", "canary")
            or projection.ds_pod_name != pod
            or projection.gameserver_uid != credential.instance_uid
            or projection.instance_epoch != credential.protocol_epoch
            or projection.state not in ("ready", "running")
            or not projection.player_ids
            or projection.last_verified_gen != credential.gen
            or projection.last_verified_jti != credential.jti
            or projection.last_verified_writer_epoch != auth.DS_AUTH_WRITER_EPOCH_V2
            or not self._active_matches(active, credential, now_ms, hub=False)
        ):
            raise errcode.PandoraError(
                errcode.ErrUnauthorized,
                "battle admission credential does not match active authority",
            )
        return self._binding(
            credential,
            allocation_id=projection.allocation_id,
            release_track=projection.release_track or "stable",
            player_ids=list(projection.player_ids),
        )

    @staticmethod
    def _heartbeat_fresh(value: int, now_ms: int, max_age_ms: int) -> bool:
        return value > 0 and value <= now_ms and now_ms - value <= max_age_ms

    @staticmethod
    def _active_matches(active, credential: dsauth.VerifiedCredential, now_ms: int, *, hub: bool) -> bool:  # noqa: ANN001
        epoch = active.protocol_epoch if hub else active.instance_epoch
        return (
            active.instance_uid == credential.instance_uid
            and epoch == credential.protocol_epoch
            and active.gen == credential.gen
            and active.jti == credential.jti
            and active.exp_ms == credential.exp_ms
            and active.exp_ms > now_ms
            and active.kid == credential.kid
            and active.writer_epoch == auth.DS_AUTH_WRITER_EPOCH_V2
            and active.writer_epoch == credential.writer_epoch
            and bool(active.token_sha256)
            and hmac.compare_digest(active.token_sha256, credential.token_sha256)
        )

    @staticmethod
    def _binding(
        credential: dsauth.VerifiedCredential,
        *,
        allocation_id: str = "",
        release_track: str = "",
        player_ids: list[int] | None = None,
    ) -> DSAdmissionBinding:
        return DSAdmissionBinding(
            ds_type=credential.ds_type,
            match_id=credential.match_id,
            pod_name=credential.pod,
            instance_uid=credential.instance_uid,
            protocol_epoch=credential.protocol_epoch,
            credential_gen=credential.gen,
            credential_jti=credential.jti,
            exp_ms=credential.exp_ms,
            kid=credential.kid,
            token_sha256=credential.token_sha256,
            writer_epoch=credential.writer_epoch,
            allocation_id=allocation_id,
            release_track=release_track,
            player_ids=list(player_ids or []),
        )
