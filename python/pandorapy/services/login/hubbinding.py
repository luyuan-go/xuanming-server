"""Hub DSTicket 归属绑定校验 —— 对齐 Go `internal/data/hub_assignment_binding.go`。

★ 这个文件是补一个**已经存在的洞**,不是新功能。

移植初版的 `verify_ds_ticket` 把整段归属校验删了,只留下"验签 + 会话 + jti 消费",
并在 `del ds_pod_name` 上写了注释 `# v1 票不带 ds_pod 绑定`。**那条注释是错的** ——
`pkg/auth/jwt.go` 的六个绑定字段就在 v1 claims 里(`omitempty`,是"可以为空"不是
"没有这个字段"),而同包的 `dsticket.py` 自己就把它们全解出来了。
删掉校验的后果是 §9.3 的四条门一起塌:

  - 票内 `ds_pod` 与调用方 pod 不比对 → **A 玩家的 Hub 票能在 B 台 DS 上兑换**;
  - 不查 assignment 权威 → Transfer / Release / 同名 Pod 重建后的**旧票永久有效**;
  - 半绑定票(签发面半截升级)被当合法票放行;
  - `require_hub_assignment_binding` 栅栏形同虚设。

判据本身不难验:票据是"把已由权威算好的判定结果搬到 DS 的唯一不可伪造通道",
删掉兑换点的核对,搬运通道就退化成"谁拿到谁能用"。

关于 v2:`dsticket.py` 的解析器把 `version` 硬编码成 1,所以 Python 侧只可能走
legacy 分支。Go 的 v2 分支(`CheckCurrentB1`)这里**刻意不移植** —— 移植一条永远
走不到的分支,只会让人以为 v2 已经支持了。等 `version` 真能解出 2 再补。
"""

from __future__ import annotations

import dataclasses
import hmac
import time
from typing import Protocol

from pandora.hub.v1 import allocator_pb2 as hubpb

from pandorapy import errcode
from pandorapy.auth import DS_AUTH_WRITER_EPOCH_V2

RELEASE_TRACK_STABLE = "stable"
RELEASE_TRACK_CANARY = "canary"

#: 心跳最大年龄。Go `NewRedisHubAssignmentChecker` 里是 30s。
DEFAULT_MAX_HEARTBEAT_AGE_SEC = 30.0


def hub_player_assignment_key(player_id: int) -> str:
    return f"pandora:hub:player:{player_id}"


def hub_auth_authority_key(pod: str) -> str:
    return "pandora:hub:auth:{%s}" % pod


def hub_shard_projection_key(pod: str) -> str:
    return "pandora:hub:shard:{%s}" % pod


@dataclasses.dataclass(frozen=True, slots=True)
class HubAssignmentBinding:
    """一张 Hub 票据里携带的归属五要件(+ 灰度轨道)。"""

    pod_name: str = ""
    instance_uid: str = ""
    protocol_epoch: int = 0
    credential_gen: int = 0
    credential_jti: str = ""
    assignment_id: str = ""
    writer_epoch: int = 0
    release_track: str = ""

    def complete(self) -> bool:
        return bool(
            self.pod_name
            and self.instance_uid
            and self.protocol_epoch > 0
            and self.credential_gen > 0
            and self.credential_jti
            and self.assignment_id
            and self.writer_epoch > 0
        )

    def empty(self) -> bool:
        """★ 只表示"完全没有归属绑定"(旧格式票)。

        **任何半绑定都不是兼容旧票** —— 半绑定意味着签发面正在半截升级,比完全
        没有绑定更危险,必须拒(见 Go 同名方法上的注释)。所以这里是"全空"而不是
        "not complete"。
        """
        return not (
            self.pod_name
            or self.instance_uid
            or self.protocol_epoch
            or self.credential_gen
            or self.credential_jti
            or self.assignment_id
            or self.writer_epoch
            or self.release_track
        )


class HubAssignmentChecker(Protocol):
    async def check_current(self, player_id: int, expected: HubAssignmentBinding) -> None: ...


class RedisHubAssignmentChecker:
    """按 Redis 当前归属权威校验一张已验签的 Hub 票。

    ★ 线性化点是第一次 `GET assignment` 返回的时刻。

    assignment 与 `{pod}` 槽的 auth / shard **不同 slot**,不能用 MULTI 跨 slot 读。
    所以用双采集给等价的线性化证明:`A1 → MGET(auth, shard) → A2`,且 `A1 == A2`。
    中间被 Transfer 改过 assignment 的话第二次读必然不等,直接判票据失效。

    错误方向是这里最容易写反的地方:

      - **missing / 字段不一致** = 这张票确实无效 → `ErrLoginTicketInvalid`;
      - **Redis 故障 / proto 解不出来** = 授权权威**不可判定** → `ErrUnavailable`。

    绝不能把基础设施错误压成"鉴权通过" —— 那会让 Redis 一抖动,全部旧票在那个
    窗口里集体可用。反过来也不能压成"票据无效":一次 Redis 抖动会变成大批玩家被
    踢下线,且客户端认为该重登(§9.22 的 UNKNOWN 不得冒充 OFFLINE,同一个道理)。
    """

    __slots__ = ("_rdb", "_now", "_max_heartbeat_age_sec")

    def __init__(
        self,
        rdb,
        *,
        now=None,
        max_heartbeat_age_sec: float = DEFAULT_MAX_HEARTBEAT_AGE_SEC,
    ):
        self._rdb = rdb
        self._now = now or (lambda: time.time())
        self._max_heartbeat_age_sec = (
            max_heartbeat_age_sec if max_heartbeat_age_sec > 0 else DEFAULT_MAX_HEARTBEAT_AGE_SEC
        )

    async def check_current(self, player_id: int, expected: HubAssignmentBinding) -> None:
        await self._check_current(player_id, expected, expected, strict_assignment_credential=True)

    async def _check_current(
        self,
        player_id: int,
        stable: HubAssignmentBinding,
        active: HubAssignmentBinding,
        *,
        strict_assignment_credential: bool,
    ) -> None:
        # ── ① 票内自洽:两份期望必须同源且完整 ──────────────────────────
        if (
            player_id == 0
            or not stable.complete()
            or not active.complete()
            or stable.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
            or active.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
            or stable.pod_name != active.pod_name
            or stable.instance_uid != active.instance_uid
            or stable.protocol_epoch != active.protocol_epoch
            or stable.assignment_id != active.assignment_id
            or (
                stable.release_track
                and (
                    stable.release_track != active.release_track
                    or stable.release_track not in (RELEASE_TRACK_STABLE, RELEASE_TRACK_CANARY)
                )
            )
        ):
            raise errcode.PandoraError(
                errcode.ErrLoginTicketInvalid, "hub ticket assignment binding incomplete"
            )
        if strict_assignment_credential and (
            stable.credential_gen != active.credential_gen
            or stable.credential_jti != active.credential_jti
        ):
            raise errcode.PandoraError(
                errcode.ErrLoginTicketInvalid, "hub first admission credential changed"
            )
        if self._rdb is None:
            raise errcode.PandoraError(errcode.ErrUnavailable, "hub assignment authority unavailable")

        # ── ② A1:读 assignment 权威(线性化点) ───────────────────────────
        akey = hub_player_assignment_key(player_id)
        payload = await self._get_bytes(akey, "read hub assignment authority failed")
        if payload is None:
            raise errcode.PandoraError(errcode.ErrLoginTicketInvalid, "hub assignment not found")
        rec = self._parse(
            hubpb.HubAssignmentStorageRecord, payload, "decode hub assignment authority failed"
        )

        if (
            rec.player_id != player_id
            or rec.assignment_id != stable.assignment_id
            or rec.hub_pod_name != stable.pod_name
            or rec.hub_instance_uid != stable.instance_uid
            or rec.auth_epoch != stable.protocol_epoch
            or rec.auth_writer_epoch != stable.writer_epoch
            or (stable.release_track and rec.release_track != stable.release_track)
            or (
                strict_assignment_credential
                and (rec.auth_gen != stable.credential_gen or rec.auth_jti != stable.credential_jti)
            )
        ):
            raise errcode.PandoraError(
                errcode.ErrLoginTicketInvalid, "hub ticket no longer matches current assignment"
            )

        # ── ③ MGET:当前凭证 + 分片投影 ──────────────────────────────────
        # 入场不能只看 auth:已 draining / stopping 或投影漂移的 shard,即使
        # assignment 还没迁走也必须拒。
        try:
            values = await self._rdb.mget(
                hub_auth_authority_key(active.pod_name),
                hub_shard_projection_key(active.pod_name),
            )
        except errcode.PandoraError:
            raise
        except Exception as exc:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "read hub active authority/projection failed: %s", exc
            ) from exc
        if len(values) != 2 or values[0] is None or values[1] is None:
            raise errcode.PandoraError(
                errcode.ErrLoginTicketInvalid, "hub active authority or projection not found"
            )
        auth_rec = self._parse(
            hubpb.HubShardAuthStorageRecord, _as_bytes(values[0]), "decode hub active credential failed"
        )
        shard_rec = self._parse(
            hubpb.HubShardStorageRecord, _as_bytes(values[1]), "decode hub active projection failed"
        )

        # ── ④ A2:重读 assignment,证明 ③ 期间没被改过 ────────────────────
        payload2 = await self._get_bytes(akey, "re-read hub assignment authority failed")
        if payload2 is None or payload2 != payload:
            raise errcode.PandoraError(
                errcode.ErrLoginTicketInvalid, "hub assignment changed during validation"
            )

        # ── ⑤ 凭证仍然 active ──────────────────────────────────────────
        now_ms = int(self._now() * 1000)
        max_age_ms = int(self._max_heartbeat_age_sec * 1000)
        act = auth_rec.active if auth_rec.HasField("active") else None
        phase_ok = auth_rec.phase in (
            hubpb.HubAuthPhase.HUB_AUTH_PHASE_ACTIVE,
            hubpb.HubAuthPhase.HUB_AUTH_PHASE_ROTATING,
        )
        pending_bad = (
            auth_rec.HasField("pending") and auth_rec.pending.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
        )
        if (
            now_ms <= 0
            or active.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
            or not phase_ok
            or act is None
            or auth_rec.pod_name != active.pod_name
            or auth_rec.instance_uid != active.instance_uid
            or auth_rec.protocol_epoch != active.protocol_epoch
            or act.instance_uid != active.instance_uid
            or act.protocol_epoch != active.protocol_epoch
            or act.gen != active.credential_gen
            or act.jti != active.credential_jti
            or act.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
            or act.writer_epoch != active.writer_epoch
            or auth_rec.required_writer_epoch != DS_AUTH_WRITER_EPOCH_V2
            or pending_bad
            or auth_rec.high_water_gen < act.gen
            or act.exp_ms <= now_ms
            or not act.kid
            or not act.token_sha256
            or auth_rec.last_active_heartbeat_ms <= 0
            or auth_rec.last_active_heartbeat_ms > now_ms
            or now_ms - auth_rec.last_active_heartbeat_ms > max_age_ms
            or shard_rec.hub_pod_name != active.pod_name
            or shard_rec.state != "ready"
            or (active.release_track and shard_rec.release_track != active.release_track)
            or shard_rec.gameserver_uid != active.instance_uid
            or shard_rec.auth_epoch != active.protocol_epoch
            or shard_rec.last_verified_gen != active.credential_gen
            or shard_rec.last_verified_jti != active.credential_jti
            or shard_rec.last_verified_writer_epoch != DS_AUTH_WRITER_EPOCH_V2
            or shard_rec.last_heartbeat_ms <= 0
            or shard_rec.last_heartbeat_ms > now_ms
            or now_ms - shard_rec.last_heartbeat_ms > max_age_ms
        ):
            raise errcode.PandoraError(
                errcode.ErrLoginTicketInvalid, "hub assignment credential is no longer active"
            )

    async def _get_bytes(self, key: str, fail_msg: str) -> bytes | None:
        try:
            v = await self._rdb.get(key)
        except errcode.PandoraError:
            raise
        except Exception as exc:
            raise errcode.PandoraError(errcode.ErrUnavailable, "%s: %s", fail_msg, exc) from exc
        return None if v is None else _as_bytes(v)

    @staticmethod
    def _parse(msg_cls, payload: bytes, fail_msg: str):
        rec = msg_cls()
        try:
            rec.ParseFromString(payload)
        except Exception as exc:
            # 坏 protobuf = 权威不可判定,不是"票据无效"。
            raise errcode.PandoraError(errcode.ErrUnavailable, "%s: %s", fail_msg, exc) from exc
        return rec


def _as_bytes(v) -> bytes:
    return v if isinstance(v, bytes) else str(v).encode()


def constant_time_eq(a: str, b: str) -> bool:
    """给 token_sha256 之类的比较用,对齐 Go 的 `subtle.ConstantTimeCompare`。"""
    return hmac.compare_digest(a.encode(), b.encode())


def binding_from_claims(claims) -> HubAssignmentBinding:
    """从已验签 claims 取出归属绑定。对齐 Go `hubBindingFromClaims`。"""
    if claims is None:
        return HubAssignmentBinding()
    return HubAssignmentBinding(
        pod_name=claims.ds_pod_name,
        instance_uid=claims.ds_instance_uid,
        protocol_epoch=claims.ds_protocol_epoch,
        credential_gen=claims.ds_credential_gen,
        credential_jti=claims.ds_credential_jti,
        assignment_id=claims.hub_assignment_id,
        writer_epoch=claims.ds_writer_epoch,
        release_track=getattr(claims, "release_track", "") or "",
    )
