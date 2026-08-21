"""battle DSTicket 的 roster 权威门 —— 对应 Go `internal/data/battle_ticket_authorizer.go`。

★ 这个文件回答两个**语义完全不同**的问题,共用一份 Redis 快照但**绝不能合并**:

  ① `authorize_battle_ticket`(签票门):"能不能给这个人签这局的票?"
     不能签的原因塌成 `ErrPermissionDeny` 是**正确**的 —— 签票语义只有"给/不给"。
  ② `inspect_battle_route`(Hub 放行门):"这局是不是已经显式终局了?"
     这里 `ErrPermissionDeny` **不能**当"已终局"的证明:它同时覆盖 roster 漂移、
     非成员、记录缺失、stale 心跳 —— 那些都只能是 UNKNOWN。把 ① 的拒绝当 ② 的
     放行证明,就是 2026-07-15 那个 P0:roster 抖一下,活局里的玩家被放回大厅 →
     双归属(§9.22「locator key miss 不能证明玩家已离开旧 DS」的同构错误)。

★ 为什么"记录缺失(key miss)"是 UNKNOWN 而不是 TERMINAL:
  投影 key 消失有两种可能 —— 终局后清理,或 DS 续期失败导致 TTL 漂移(活局仍在)。
  没有版本化 placement lease 之前这两者**不可区分**,所以一律 UNKNOWN、fail-closed。

★ `abandoned` 与 `ended` 的处置不同(脑裂再入屏障,§9.22):
  `ended`     = DS 自己上报的正常终局,已按结算流程收尾,无脑裂窗口 → 立即 TERMINAL。
  `abandoned` = 心跳超时判死的**补偿性**终态,DS 可能只是与后端分区、其上玩家仍可玩。
                必须等旧 DS 的授权租约上限 + 偏差余量过去(它届时已对存量玩家自我
                fencing)才能放行,否则玩家会在两台 DS 上同时可玩。
                `last_heartbeat_ms == 0` 例外:从未有过成功心跳 = 从未取得授权租约,
                准入门从未打开,不可能有玩家在其上 → 立即 TERMINAL 安全。

本模块**只读 Redis**,不写不删不续 TTL:它是签发/放行的线性化观测点,不是状态机。
"""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import time

from pandora.ds.v1 import allocator_pb2 as dspb

from pandorapy import auth, errcode, placement
from pandorapy.services.login import dsadmission as ldsadmission

#: 投影记录的显式终态(与 ds_allocator 状态机常量一致;TerminateExpected 写入)。
#: Go: `battleStateEnded` / `battleStateAbandoned`。
BATTLE_STATE_ENDED = "ended"
BATTLE_STATE_ABANDONED = "abandoned"

#: live roster 认可的两个在局状态。Go `liveRosterDenyReason` 的 `state_not_live` 判据。
BATTLE_STATE_READY = "ready"
BATTLE_STATE_RUNNING = "running"

#: 心跳最大年龄默认值(Go `NewRedisBattleTicketAuthorizer` 的 `maxHeartbeatAge <= 0` 兜底)。
DEFAULT_MAX_HEARTBEAT_AGE_SEC = 30.0


@dataclasses.dataclass(frozen=True, slots=True)
class BattleTicketTarget:
    """本次 roster 授权读到的**同一份快照**里的可路由目标 —— Go `data.BattleTicketTarget`。

    ★ 重连必须用这里的 `ds_addr`,**不能**在证明了 projection 之后又回头用 locator 的地址:
    locator 是 30s TTL 投影,可能比 projection 陈旧一整个 Pod 生命周期,那样签出的票
    绑的是新实例、地址指向旧实例。
    """

    ds_addr: str = ""
    pod_name: str = ""
    instance_uid: str = ""
    instance_epoch: int = 0
    #: 本局分配 ID(DSTicket v2 `allocation_id` claim)。旧记录可能为空,
    #: v2 签发侧对空值 fail-closed 拒签(见 `biz.TicketUsecase._issue_battle_ds_ticket_v2`)。
    allocation_id: str = ""
    release_track: str = ""


class BattleRouteState(enum.IntEnum):
    """Hub 签票门的显式三态 —— Go `data.BattleRouteState`(取值与 iota 顺序一致)。"""

    #: 无法权威判定(记录缺失 / 非成员漂移 / stale 心跳 / Redis 错误)。
    #: 调用方在 locator 阳性 BATTLE 信号下必须 fail-closed。
    UNKNOWN = 0
    #: 玩家确属 live 对局(ready/running + 成员 + 心跳新鲜)。
    ACTIVE = 1
    #: 权威记录显式终态(ended / abandoned)—— **唯一**允许放行 Hub 的证明。
    TERMINAL = 2


def _heartbeat_fresh(value: int, now_ms: int, max_age_ms: int) -> bool:
    """Go `ticketHeartbeatFresh`:必须 >0、不得在未来、且不得超龄。

    "不得在未来"这一条不是洁癖:DS 侧时钟跑快时会写出未来心跳,若只判 `now-v <= max`
    就等于给了一张**永不过期**的新鲜证明。
    """
    return value > 0 and value <= now_ms and now_ms - value <= max_age_ms


def _battle_ticket_target(battle: dspb.BattleStorageRecord | None) -> BattleTicketTarget:
    """Go `battleTicketTarget`。"""
    if battle is None:
        return BattleTicketTarget()
    return BattleTicketTarget(
        ds_addr=battle.ds_addr,
        pod_name=battle.ds_pod_name,
        instance_uid=battle.gameserver_uid,
        instance_epoch=int(battle.instance_epoch),
        allocation_id=battle.allocation_id,
        release_track=battle.release_track,
    )


class RedisBattleTicketAuthorizer:
    """battle 签票 / Hub 放行的 Redis roster 权威。只读,无副作用。"""

    __slots__ = ("_rdb", "_require_model_b", "_now", "_max_age_sec")

    def __init__(
        self,
        rdb,  # noqa: ANN001 —— redis.asyncio 客户端(Cluster / 单机同形)
        *,
        require_model_b: bool,
        max_heartbeat_age_sec: float = DEFAULT_MAX_HEARTBEAT_AGE_SEC,
        now=time.time,  # noqa: B008
    ) -> None:
        self._rdb = rdb
        self._require_model_b = require_model_b
        self._max_age_sec = (
            max_heartbeat_age_sec
            if max_heartbeat_age_sec > 0
            else DEFAULT_MAX_HEARTBEAT_AGE_SEC
        )
        self._now = now

    # ── 签票门 ───────────────────────────────────────────────────────────

    async def authorize_battle_ticket(
        self, player_id: int, match_id: int
    ) -> BattleTicketTarget:
        """读取时刻 = 本次签发授权的线性化点。

        Redis 不可判定 → `ErrUnavailable`(可重试);
        非成员 / 空 roster / 非 live / stale / Model-B 漂移 → `ErrPermissionDeny`(绝不签票)。
        """
        if player_id == 0 or match_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "battle ticket authorization requires player and match"
            )
        if self._rdb is None or self._now is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle ticket roster authority unavailable"
            )
        if self._require_model_b:
            return await self._authorize_model_b(player_id, match_id)
        payload = await self._get(
            ldsadmission.battle_projection_key(match_id),
            "read battle ticket roster failed",
        )
        if payload is None:
            raise errcode.PandoraError(
                errcode.ErrPermissionDeny, "battle ticket target is not live"
            )
        battle = self._parse(
            dspb.BattleStorageRecord, payload, "decode battle ticket roster failed"
        )
        reason = self._live_roster_deny_reason(battle, player_id, match_id)
        if reason:
            # reason 进错误串:调用方(login 的 authorize_battle_reconnect_ticket_failed
            # ERROR)带 err 落盘,否则八个条件塌成一句话,"不在 roster / 心跳 stale /
            # 状态不对"三种完全不同的处置方向无法分诊。
            raise errcode.PandoraError(
                errcode.ErrPermissionDeny,
                "player is not authorized for battle ticket target: %s",
                reason,
            )
        return _battle_ticket_target(battle)

    # ── Hub 放行门(三态,零副作用,不签票)───────────────────────────────

    async def inspect_battle_route(self, player_id: int, match_id: int) -> BattleRouteState:
        """Go `InspectBattleRoute`。

        与 `authorize_battle_ticket` 的区别见模块头:后者把 UNKNOWN 情形折叠进
        PermissionDeny(签票语义正确),但作为 Hub 放行证明会把 roster 漂移**误判成终局**。
        """
        if player_id == 0 or match_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "battle route inspection requires player and match"
            )
        if self._rdb is None or self._now is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle route authority unavailable"
            )
        payload = await self._get(
            ldsadmission.battle_projection_key(match_id),
            "read battle route projection failed",
        )
        if payload is None:
            # 记录缺失 ≠ 终态:可能是终局后清理,也可能是 DS 续期失败导致 TTL 漂移(活局仍在)。
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle projection missing; cannot prove match %d is terminal",
                match_id,
            )
        battle = self._parse(
            dspb.BattleStorageRecord, payload, "decode battle route projection failed"
        )
        if int(battle.match_id) != match_id:
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle projection match mismatch (want %d got %d)",
                match_id,
                int(battle.match_id),
            )
        if battle.state == BATTLE_STATE_ENDED:
            return BattleRouteState.TERMINAL
        if battle.state == BATTLE_STATE_ABANDONED:
            last = int(battle.last_heartbeat_ms)
            if last > 0:
                barrier_ms = int(placement.DS_FENCE_REENTRY_BARRIER_SECONDS * 1000)
                wait = last + barrier_ms - int(self._now() * 1000)
                if wait > 0:
                    raise errcode.PandoraError(
                        errcode.ErrUnavailable,
                        "abandoned battle %d is inside the DS fence re-entry barrier "
                        "(%dms left); retry",
                        match_id,
                        wait,
                    )
            return BattleRouteState.TERMINAL
        if not self._live_roster_deny_reason(battle, player_id, match_id):
            return BattleRouteState.ACTIVE
        # 非终态且非可证明 live:running 但非成员(漂移)/ stale 心跳 / warming 等中间态。
        raise errcode.PandoraError(
            errcode.ErrUnavailable,
            "battle route not provably terminal (state=%q)",
            battle.state,
        )

    # ── Model-B(B1 k8s 档)───────────────────────────────────────────────

    async def _authorize_model_b(
        self, player_id: int, match_id: int
    ) -> BattleTicketTarget:
        try:
            values = await self._rdb.mget(
                ldsadmission.battle_auth_key(match_id),
                ldsadmission.battle_projection_key(match_id),
            )
        except asyncio.CancelledError:
            raise
        except errcode.PandoraError:
            raise
        except BaseException as exc:  # noqa: BLE001
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "read battle ticket authority failed: %s", exc
            ) from exc
        if len(values) != 2 or values[0] is None or values[1] is None:
            raise errcode.PandoraError(
                errcode.ErrPermissionDeny, "battle ticket authority is not active"
            )
        try:
            auth_raw = ldsadmission.as_bytes(values[0])
            battle_raw = ldsadmission.as_bytes(values[1])
        except TypeError as exc:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "decode battle ticket auth value failed: %s", exc
            ) from exc
        record = self._parse(
            dspb.BattleDSAuthStorageRecord, auth_raw, "decode battle ticket auth failed"
        )
        battle = self._parse(
            dspb.BattleStorageRecord, battle_raw, "decode battle ticket projection failed"
        )
        reason = self._model_b_deny_reason(record, battle, player_id, match_id)
        if reason:
            # 同 live_roster:约 25 个条件的合取,首个不满足的条件名必须可见 ——
            # 处置方向(查 matchmaker 数据 / 等心跳自愈 / 查 DS 凭据轮换)截然不同。
            raise errcode.PandoraError(
                errcode.ErrPermissionDeny,
                "battle ticket authority or roster is not routable: %s",
                reason,
            )
        return _battle_ticket_target(battle)

    # ── 判据(逐条对 Go,顺序不变;顺序即诊断价值)────────────────────────

    def _live_roster_deny_reason(
        self, battle: dspb.BattleStorageRecord | None, player_id: int, match_id: int
    ) -> str:
        """Go `liveRosterDenyReason`:返回第一个不满足的条件名(""=通过)。"""
        now_ms = int(self._now() * 1000)
        max_age_ms = int(self._max_age_sec * 1000)
        if battle is None:
            return "projection_missing"
        if int(battle.match_id) != match_id:
            return "projection_match_id_mismatch"
        if not battle.ds_pod_name:
            return "projection_ds_pod_empty"
        if not battle.ds_addr:
            return "projection_ds_addr_empty"
        if battle.state not in (BATTLE_STATE_READY, BATTLE_STATE_RUNNING):
            return "state_not_live(" + battle.state + ")"
        if not battle.player_ids:
            return "roster_empty"
        if player_id not in battle.player_ids:
            return "player_not_in_roster"
        if not _heartbeat_fresh(int(battle.last_heartbeat_ms), now_ms, max_age_ms):
            return "heartbeat_stale"
        return ""

    def _model_b_deny_reason(  # noqa: C901, PLR0911, PLR0912 —— 逐条对 Go,合并会丢诊断
        self,
        record: dspb.BattleDSAuthStorageRecord | None,
        battle: dspb.BattleStorageRecord,
        player_id: int,
        match_id: int,
    ) -> str:
        """Go `modelBDenyReason`:条件与顺序逐项等价。"""
        reason = self._live_roster_deny_reason(battle, player_id, match_id)
        if reason:
            return reason
        if record is None:
            return "auth_record_missing"
        active = record.active if record.HasField("active") else None
        now_ms = int(self._now() * 1000)
        max_age_ms = int(self._max_age_sec * 1000)
        if active is None:
            return "auth_active_credential_missing"
        if record.phase not in (
            dspb.BATTLE_AUTH_PHASE_ACTIVE,
            dspb.BATTLE_AUTH_PHASE_ROTATING,
        ):
            return "auth_phase_not_active(" + _battle_auth_phase_name(record.phase) + ")"
        if int(record.match_id) != match_id:
            return "auth_match_id_mismatch"
        if not record.allocation_id:
            return "auth_allocation_id_empty"
        if record.allocation_id != battle.allocation_id:
            return "allocation_id_mismatch"
        if not record.instance_uid or int(record.instance_epoch) == 0:
            return "auth_instance_identity_missing"
        if not battle.gameserver_uid or int(battle.instance_epoch) == 0:
            return "projection_instance_identity_missing"
        if record.ds_pod_name != battle.ds_pod_name:
            return "instance_pod_mismatch"
        if record.instance_uid != battle.gameserver_uid:
            return "instance_uid_mismatch"
        if int(record.instance_epoch) != int(battle.instance_epoch):
            return "instance_epoch_mismatch"
        if int(record.required_writer_epoch) != auth.DS_AUTH_WRITER_EPOCH_V2:
            return "required_writer_epoch_not_v2"
        if (
            record.HasField("pending")
            and int(record.pending.writer_epoch) != auth.DS_AUTH_WRITER_EPOCH_V2
        ):
            return "pending_writer_epoch_not_v2"
        if int(record.high_water_gen) < int(active.gen):
            return "high_water_below_active_gen"
        if not _heartbeat_fresh(int(record.last_active_heartbeat_ms), now_ms, max_age_ms):
            return "auth_heartbeat_stale"
        if int(battle.last_heartbeat_ms) != int(record.last_active_heartbeat_ms):
            return "heartbeat_anchor_mismatch"
        if not active.instance_uid or int(active.instance_epoch) == 0:
            return "active_instance_identity_missing"
        if active.instance_uid != record.instance_uid or int(active.instance_epoch) != int(
            record.instance_epoch
        ):
            return "active_instance_mismatch"
        if int(active.gen) == 0:
            return "active_gen_zero"
        if not active.jti:
            return "active_jti_empty"
        if int(active.exp_ms) <= now_ms:
            return "active_credential_expired"
        if not active.kid:
            return "active_kid_empty"
        if not active.token_sha256:
            return "active_token_hash_empty"
        if int(active.writer_epoch) != auth.DS_AUTH_WRITER_EPOCH_V2:
            return "active_writer_epoch_not_v2"
        if int(battle.last_verified_gen) != int(active.gen):
            return "projection_verified_gen_stale"
        if battle.last_verified_jti != active.jti:
            return "projection_verified_jti_stale"
        if int(battle.last_verified_writer_epoch) != auth.DS_AUTH_WRITER_EPOCH_V2:
            return "projection_verified_writer_epoch_not_v2"
        return ""

    # ── Redis / proto 读写(错误方向:基础设施 → Unavailable)──────────────

    async def _get(self, key: str, fail_msg: str) -> bytes | None:
        try:
            value = await self._rdb.get(key)
        except asyncio.CancelledError:
            raise
        except errcode.PandoraError:
            raise
        except BaseException as exc:  # noqa: BLE001
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "%s: %s", fail_msg, exc
            ) from exc
        if value is None:
            return None
        return ldsadmission.as_bytes(value)

    @staticmethod
    def _parse(msg_cls, payload: bytes, fail_msg: str):  # noqa: ANN001, ANN205
        rec = msg_cls()
        try:
            rec.ParseFromString(payload)
        except Exception as exc:  # noqa: BLE001 —— protobuf 解析异常类型随实现变化
            # 坏 protobuf = 权威**不可判定**,不是"票据无效"。压成后者会让一次投递
            # 事故变成大批玩家被踢下线且客户端认为该重登。
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "%s: %s", fail_msg, exc
            ) from exc
        return rec


def _battle_auth_phase_name(value: int) -> str:
    """枚举转名字。走 protoenum 而不是 `.Name()`:未知值抛 ValueError 会把**拒绝路径本身**
    炸成 500,而给 phase 加枚举值是 additive 的常规 proto 演进(§9 不变式 17)。"""
    from pandorapy.protoenum import enum_name

    return enum_name(dspb.BattleAuthPhase, value)
