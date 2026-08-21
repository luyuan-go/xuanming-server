"""Login 对 DS callback credential 的 Redis active 权威校验。"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import hmac
import json
import time

from pandora.ds.v1 import allocator_pb2 as dspb
from pandora.hub.v1 import allocator_pb2 as hubpb

from pandorapy import auth, dsauth, errcode, placement
from pandorapy import dsticket

#: DS 类型字面量的**唯一出处**取公共层(proto 里 ds_type 是裸 string,没有 enum)。
DS_TYPE_HUB = dsticket.DS_TYPE_HUB
DS_TYPE_BATTLE = dsticket.DS_TYPE_BATTLE


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

    def complete(self) -> bool:
        """Go `DSAdmissionBinding.Complete()`。

        Hub 与 Battle 的 `match_id` / `player_ids` 要求**互为反面**(hub 必须为 0/空,
        battle 必须非 0/非空):合成一条"有就行"会让一张 hub 的 admission 去证明
        battle 的票据绑定。
        """
        if self.ds_type not in (DS_TYPE_HUB, DS_TYPE_BATTLE):
            return False
        if (
            not self.pod_name
            or not self.instance_uid
            or self.protocol_epoch == 0
            or self.credential_gen == 0
            or not self.credential_jti
            or self.exp_ms <= 0
            or not self.kid
            or not self.token_sha256
            or self.writer_epoch != auth.DS_AUTH_WRITER_EPOCH_V2
        ):
            return False
        if self.ds_type == DS_TYPE_BATTLE:
            return self.match_id != 0 and len(self.player_ids) > 0
        return self.match_id == 0 and len(self.player_ids) == 0

    def admission_attempt_owner(self, admission_id: str) -> str:
        """一次 PreLoginAsync 的**稳定** owner 摘要 —— Go `AdmissionAttemptOwner`。

        ★ 刻意**不含** gen/jti/exp/kid/token_sha256:那些在普通 token 轮换时会变。
        含 ds_type/match/pod/uid/instance_epoch/writer_epoch:于是同实例平滑轮换后
        可以重认,而 UID 重建 / 跨对局 / 旧 writer **永远**不能重认。
        少放一个字段就多一类可以冒名重认的场景。
        """
        if not self.complete() or not _valid_admission_uuid_v4(admission_id):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "invalid admission attempt owner input"
            )
        return _go_json_sha256_hex(
            {
                "v": 3,
                "admission_id": admission_id,
                "ds_type": self.ds_type,
                "match_id": self.match_id,
                "pod": self.pod_name,
                "uid": self.instance_uid,
                "epoch": self.protocol_epoch,
                "writer_epoch": self.writer_epoch,
            }
        )

    def accepted_credential_hash(self) -> str:
        """首次成功准入时**完整 active tuple** 的审计绑定 —— Go `AcceptedCredentialHash`。

        它不参与 same-attempt 重认判定(那是 attempt_owner 的活),所以平滑轮换不会
        覆盖首次接受的凭据,也不会因为"响应未知 → 重试"而被误判成 replay。
        """
        if not self.complete():
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "invalid accepted credential input"
            )
        return _go_json_sha256_hex(
            {
                "v": 3,
                "ds_type": self.ds_type,
                "match_id": self.match_id,
                "pod": self.pod_name,
                "uid": self.instance_uid,
                "epoch": self.protocol_epoch,
                "gen": self.credential_gen,
                "credential_jti": self.credential_jti,
                "exp_ms": self.exp_ms,
                "kid": self.kid,
                "token_sha256": self.token_sha256,
                "writer_epoch": self.writer_epoch,
            }
        )


def _valid_admission_uuid_v4(value: str) -> bool:
    """canonical 小写 RFC4122 UUIDv4 —— 对齐 Go 里 `uuid.Parse` + 三重复核那一段。

    复用 `placement.valid_operation_id`:同一套判据(正则 + version/variant + 原样相等),
    两处各写一份迟早漂移。admission_id 与 operation_id 的形状要求本来就是同一个。
    """
    return placement.valid_operation_id(value)


def _go_json_sha256_hex(payload: dict[str, object]) -> str:
    """按 **Go `json.Marshal` 的字节形态**序列化后取 sha256 hex。

    ★ 这个摘要会被写进 Redis marker,并在灰度期被 **Go 与 Python 两个 login 副本**
    分别重算比对。字节不一致 = 同一次 admission 的重试被判成另一次准入 → 玩家在
    重连时被 `ErrLoginTicketReplayed` 拒掉,而两栈日志都"正常"。所以:

      · `separators=(",", ":")` —— Go 的 encoding/json 不产生任何多余空格;
      · `sort_keys=False` —— Go 按**结构体字段声明顺序**输出,不是字典序;
        这里的 dict 插入顺序已按 Go 的字段顺序排好,排序反而会写错;
      · `ensure_ascii=False` —— Go 输出原始 UTF-8,Python 默认会转成 \\uXXXX。
        (本摘要的入参都是 k8s 名 / uuid / hex / base64url 指纹,ASCII 且不含
        Go 会转义的 `<` `>` `&`;上游的 complete() 与 credential 校验保证了这点。)
    """
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False, sort_keys=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _hub_auth_key(pod: str) -> str:
    return f"pandora:hub:auth:{{{pod}}}"


def _hub_projection_key(pod: str) -> str:
    return f"pandora:hub:shard:{{{pod}}}"


def battle_auth_key(match_id: int) -> str:
    return f"pandora:ds:auth:{{{match_id}}}"


def battle_projection_key(match_id: int) -> str:
    return f"pandora:ds:battle:{{{match_id}}}"


def as_bytes(value) -> bytes:  # noqa: ANN001
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise TypeError(f"unexpected redis value type {type(value).__name__}")


# battle 的两个 key 模板与字节强转是**跨模块共享事实**(本文件与 battleroute.py 都要用):
# hashtag `{match_id}` 的位置决定 Redis Cluster slot,auth 与 projection 必须同槽才能
# 用一次 MGET 读到同一时刻的快照。两处各抄一份迟早漂移成跨槽读 → MGET 直接报错,
# 而单机 Redis 自测全绿。所以模板只在这里定义一次,下面的下划线名只是历史内部别名。
_battle_auth_key = battle_auth_key
_battle_projection_key = battle_projection_key
_as_bytes = as_bytes


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
