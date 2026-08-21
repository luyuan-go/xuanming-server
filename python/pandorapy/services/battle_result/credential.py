"""battle_result 的 ReportResult / ReportProgress Redis active credential 终态门 ——
对应 Go 侧 `internal/service/battle_credential.go` + `internal/data/battle_auth.go`。

JWT 验签只证明「令牌由受信签发方签过」;本模块再读取 Redis 唯一授权权威
(`pandora:ds:auth:{match_id}` + `pandora:ds:battle:{match_id}` 的**同槽单命令快照**),
证明这份 (pod, GameServer UID, instance epoch, gen, jti, kid, token sha) 凭据**此刻**
仍是 active,并把 canonical 名单 / game_mode / map_id / 段位口径从**服务端快照**取出来 ——
DS 请求体里的同名字段一律不采信(§9.6)。

★ 两个 key 必须一次 MGET 读:分两次 GET 会读到"auth 是新一轮、battle 还是上一轮"的
  撕裂快照,而这条链的全部安全性就建立在两份记录互相印证上。`{match_id}` hashtag
  保证它们同 slot,MGET 在 Redis Cluster 上才合法。

★ AuthorizeResult 只读,不回写;唯一的写是 `record_battle_result` 的 receipt SET,
  而它自己也在 WATCH 里重新核验一遍权威(并发轮换 / 终止 / UID 重建 / allocation
  漂移都会零写入失败)。授权记录本身的写者只有 ds_allocator。

★ 这里必须用**墙钟**(`time.time()`):比较对象是 Redis 里存的绝对 unix 毫秒
  (`active.exp_ms` / `last_active_heartbeat_ms`),单调钟与它不在同一坐标系。

★ 与 `player_locator/hub_credential.py` 的差异(刻意):那边返回 in-band code,
  因为它的调用点是 `code = check(...); if code != 0: ...`;这边 Go 返回
  `(record, error)` 且调用点紧接着 `toProtoCode(err)`,而**返回值本身是必须的**
  (terminal release 证明),所以照 Go 抛 `errcode.PandoraError`。
"""

from __future__ import annotations

import asyncio
import dataclasses
import hmac
import time
from typing import Any, Protocol

from pandora.ds.v1 import allocator_pb2 as dspb
from redis.exceptions import WatchError

from pandorapy import dsauth, dsauthrecord, errcode
from pandorapy import log as plog
from pandorapy.auth import DS_AUTH_WRITER_EPOCH_V2, DS_TYPE_BATTLE
from pandorapy.protoenum import enum_name
from pandorapy.services.battle_result import repo as brepo

#: Go: `NewBattleCredentialStateChecker` 的 `maxAge <= 0 → 30 * time.Second`。
DEFAULT_ACTIVE_HEARTBEAT_MAX_AGE_SEC = 30.0

#: Go: `battleResultReceiptCASRetries`。
BATTLE_RESULT_RECEIPT_CAS_RETRIES = 4

#: 可结算的 battle 投影状态。Go 在三处硬编码 `"ready"` / `"running"`。
PLAYABLE_BATTLE_STATES = ("ready", "running")


def battle_auth_key(match_id: int) -> str:
    """`pandora:ds:auth:{<match_id>}`。Go: `battleAuthKey`。

    ★ 花括号是 Redis Cluster hashtag,不是格式占位符:它让本键与 battle 投影、
      result receipt 落在同一 slot,MGET / MULTI 才不会 CROSSSLOT。
    """
    return f"pandora:ds:auth:{{{match_id}}}"


def battle_key(match_id: int) -> str:
    """`pandora:ds:battle:{<match_id>}`。Go: `battleKey`。"""
    return f"pandora:ds:battle:{{{match_id}}}"


# ── 拒绝 reason 枚举(§11.3 R2)────────────────────────────────────────────────
#
# ★ 取值与 Go 的 credentialScopeReason / credentialFreshnessReason /
#   credentialAuthorityReason 返回的字符串**逐字节一致** —— 它们进 Loki 查询与
#   告警规则,漂移即查询落空。历史实现把这约 25 个子条件塌成一句
#   "battle credential does not match active authority":DS 换 pod、instance_epoch
#   轮换、battle.state 卡在 allocating、换钥后 kid 不匹配 —— 现象完全相同
#   (都是 ds_auth_rejected + code=Unauthorized),排障只能翻 Redis 人肉比对。


def credential_scope_reason(match_id: int, cred: dsauth.VerifiedCredential | None) -> str:
    """枚举「令牌本身不完整 / scope 不匹配」的具体项。返回 "" = 全部通过。"""
    if match_id == 0:
        return "missing_match_id"
    if cred is None:
        return "missing_credential"
    if cred.ds_type != DS_TYPE_BATTLE:
        return "ds_type_not_battle"
    if cred.match_id != match_id:
        return "token_match_id_mismatch"
    if cred.pod == "":
        return "token_missing_pod"
    if cred.instance_uid == "":
        return "token_missing_instance_uid"
    if cred.protocol_epoch == 0:
        return "token_missing_instance_epoch"
    if cred.gen == 0:
        return "token_missing_gen"
    if cred.jti == "":
        return "token_missing_jti"
    if cred.exp_ms <= 0:
        return "token_missing_exp"
    if cred.token_sha256 == "":
        return "token_missing_token_sha"
    if cred.kid == "":
        return "token_missing_kid"
    if cred.writer_epoch != DS_AUTH_WRITER_EPOCH_V2:
        return "token_writer_epoch_unsupported"
    return ""


def credential_freshness_reason(
    now_ms: int,
    max_age_ms: int,
    cred: dsauth.VerifiedCredential,
    rec: Any,
    active: Any,
) -> str:
    """枚举「过期 / 心跳陈旧」的具体项。

    ★ `token_expired` 与 `heartbeat_stale` 必须分开:前者是这台 DS 该去换票了
      (正常轮换),后者是这台 DS 已经不在了(僵尸)。合成一条会让"对局结算不了"
      的两种完全不同的故障看起来一模一样。
    """
    if now_ms <= 0:
        return "server_clock_invalid"
    if cred.exp_ms <= now_ms:
        return "token_expired"
    if active.exp_ms == 0:
        return "active_exp_unset"
    if now_ms >= active.exp_ms:
        return "active_expired"
    if rec.last_active_heartbeat_ms <= 0:
        return "heartbeat_missing"
    if rec.last_active_heartbeat_ms > now_ms:
        return "heartbeat_in_future"
    if now_ms - rec.last_active_heartbeat_ms > max_age_ms:
        return "heartbeat_stale"
    return ""


def credential_authority_reason(  # noqa: C901, PLR0911, PLR0912 —— 与 Go 同为逐条枚举
    match_id: int,
    cred: dsauth.VerifiedCredential,
    rec: Any,
    battle: Any,
    active: Any,
) -> str:
    """枚举「已验签令牌 ≠ Redis active 权威」的具体项。

    条件顺序与 Go 的 switch 逐条一致(聚合结果等价,但**具体命中哪条**要一样,
    否则两栈对同一份坏数据报出不同 reason,双栈并行期的对账会失效)。
    """
    if rec.match_id != match_id:
        return "record_match_id_mismatch"
    if rec.ds_pod_name != cred.pod:
        return "record_pod_mismatch"
    if rec.allocation_id == "":
        return "record_missing_allocation_id"
    if battle.match_id != match_id:
        return "battle_match_id_mismatch"
    if battle.allocation_id != rec.allocation_id:
        return "battle_allocation_id_mismatch"
    if battle.ds_pod_name != cred.pod:
        return "battle_pod_mismatch"
    if battle.state not in PLAYABLE_BATTLE_STATES:
        return "battle_state_not_playable"
    if battle.gameserver_uid != cred.instance_uid:
        return "battle_instance_uid_mismatch"
    if battle.instance_epoch != cred.protocol_epoch:
        return "battle_instance_epoch_mismatch"
    if battle.last_verified_gen != cred.gen:
        return "battle_gen_mismatch"
    if battle.last_verified_jti != cred.jti:
        return "battle_jti_mismatch"
    if battle.last_verified_writer_epoch != cred.writer_epoch:
        return "battle_writer_epoch_mismatch"
    if rec.instance_uid == "":
        return "record_missing_instance_uid"
    if rec.instance_uid != cred.instance_uid:
        return "record_instance_uid_mismatch"
    if rec.instance_epoch == 0:
        return "record_missing_instance_epoch"
    if rec.instance_epoch != cred.protocol_epoch:
        return "record_instance_epoch_mismatch"
    if active.instance_uid != cred.instance_uid:
        return "active_instance_uid_mismatch"
    if active.instance_epoch != cred.protocol_epoch:
        return "active_instance_epoch_mismatch"
    if active.gen == 0:
        return "active_missing_gen"
    if active.gen != cred.gen:
        return "active_gen_mismatch"
    if active.jti == "":
        return "active_missing_jti"
    if active.jti != cred.jti:
        return "active_jti_mismatch"
    if active.exp_ms != cred.exp_ms:
        return "active_exp_mismatch"
    if active.kid == "":
        return "active_missing_kid"
    if active.kid != cred.kid:
        return "active_kid_mismatch"
    if active.token_sha256 == "":
        return "active_missing_token_sha"
    if active.writer_epoch != DS_AUTH_WRITER_EPOCH_V2:
        return "active_writer_epoch_unsupported"
    if active.writer_epoch != cred.writer_epoch:
        return "active_writer_epoch_mismatch"
    if rec.required_writer_epoch != DS_AUTH_WRITER_EPOCH_V2:
        return "record_required_writer_epoch_unsupported"
    if rec.HasField("pending") and rec.pending.writer_epoch != DS_AUTH_WRITER_EPOCH_V2:
        return "pending_writer_epoch_unsupported"
    if rec.high_water_gen < active.gen:
        return "high_water_gen_regressed"
    # ★ 常量时间比较:token_sha256 是凭据的一部分,朴素 `!=` 会按首个不同字节
    #   提前返回,给出可测量的时序旁路(§加密比较用 hmac.compare_digest)。
    if not hmac.compare_digest(active.token_sha256, cred.token_sha256):
        return "token_sha_mismatch"
    return ""


def canonical_battle_roster(raw) -> list[int]:  # noqa: ANN001 —— repeated uint64
    """把 battle 投影里的名单规范化成升序去重列表。Go: `canonicalBattleRoster`。

    ★ 空 / 含 0 / 有重复一律 ErrUnauthorized 而不是"清洗后继续":这份名单是整场
      结算的发放依据,里面有 0 说明快照本身坏了 —— 继续算等于给一个不存在的
      player_id 发段位和掉落。
    """
    out = sorted(int(pid) for pid in raw)
    if not out:
        raise errcode.PandoraError(
            errcode.ErrUnauthorized, "battle authority roster is missing"
        )
    for i, player_id in enumerate(out):
        if player_id == 0 or (i > 0 and out[i - 1] == player_id):
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "battle authority roster is invalid"
            )
    return out


def _log_credential_reject(
    match_id: int, reason: str, cred: dsauth.VerifiedCredential | None, **kv: object
) -> None:
    """在每个拒绝点留一条带**枚举 reason** 的 WARN(§11.3 R2)。

    这些码全是业务码(ErrUnauthorized / ErrUnavailable),不属 server fault,
    access log 走 rpc_ok(DEBUG)—— 不在拒绝点打就是线上完全不可见。
    """
    fields: dict[str, object] = {"match_id": match_id, "reason": reason}
    if cred is not None:
        fields["ds_pod"] = cred.pod
        fields["cred_gen"] = cred.gen
        fields["cred_instance_epoch"] = cred.protocol_epoch
        fields["cred_jti"] = cred.jti
        fields["cred_kid"] = cred.kid
    fields.update(kv)
    plog.get().warning("battle_credential_mismatch", **fields)


# ── Redis 授权权威读端 ───────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True, slots=True)
class BattleResultCredential:
    """已验签 JWT 的完整、不可降级身份。Go: `data.BattleResultCredential`。"""

    match_id: int
    pod_name: str
    instance_uid: str
    instance_epoch: int
    gen: int
    jti: str
    exp_ms: int
    kid: str
    token_sha256: str
    writer_epoch: int


class BattleAuthReader(Protocol):
    """在任何结算副作用前读取同槽 auth+battle 的单命令快照。Go: `data.BattleAuthReader`。"""

    async def get_battle_authority(self, match_id: int) -> tuple[Any | None, Any | None, bool]:
        """返回 `(auth_record, battle_record, found)`;任一 key miss = `(None, None, False)`。

        读失败 / 解码失败必须**抛异常**:调用方据此把「凭据未激活」(fail-closed)
        与「授权权威不可用」(可重试)分开 —— 混同会让 Redis 抖动被记成
        「这台 DS 的凭据被吊销了」。
        """
        ...


def _result_authority_matches(
    a: Any, b: Any, c: BattleResultCredential, now_ms: int, max_age_ms: int
) -> bool:
    """receipt CAS 里重新核验权威。Go: `resultAuthorityMatches`。

    ★ 这不是 credential_authority_reason 的重复:那一份跑在 handler 里、用的是
      读快照;这一份跑在 WATCH 事务里、用的是**事务快照**。并发轮换恰好落在
      两者之间时,只有这一份能挡住(WATCH 会让 EXEC 失败,或这里直接判不匹配)。
    """
    active = a.active
    return (
        a.phase
        in (dspb.BATTLE_AUTH_PHASE_ACTIVE, dspb.BATTLE_AUTH_PHASE_ROTATING)
        and a.match_id == c.match_id
        and b.match_id == c.match_id
        and a.allocation_id != ""
        and a.allocation_id == b.allocation_id
        and a.ds_pod_name == c.pod_name
        and b.ds_pod_name == c.pod_name
        and a.instance_uid == c.instance_uid
        and b.gameserver_uid == c.instance_uid
        and a.instance_epoch == c.instance_epoch
        and b.instance_epoch == c.instance_epoch
        and b.state in PLAYABLE_BATTLE_STATES
        and a.last_active_heartbeat_ms > 0
        and a.last_active_heartbeat_ms <= now_ms
        and now_ms - a.last_active_heartbeat_ms <= max_age_ms
        and active.gen == c.gen
        and active.jti == c.jti
        and active.exp_ms == c.exp_ms
        and active.kid == c.kid
        and active.instance_uid == c.instance_uid
        and active.instance_epoch == c.instance_epoch
        and active.writer_epoch == DS_AUTH_WRITER_EPOCH_V2
        and active.writer_epoch == c.writer_epoch
        and active.token_sha256 != ""
        and hmac.compare_digest(active.token_sha256, c.token_sha256)
        and b.last_verified_gen == c.gen
        and b.last_verified_jti == c.jti
        and b.last_verified_writer_epoch == c.writer_epoch
        and a.required_writer_epoch == DS_AUTH_WRITER_EPOCH_V2
        and (not a.HasField("pending") or a.pending.writer_epoch == DS_AUTH_WRITER_EPOCH_V2)
        and a.high_water_gen >= c.gen
    )


def _valid_result_credential(c: BattleResultCredential, now_ms: int) -> bool:
    """Go: `validResultCredential`。"""
    return (
        c.match_id != 0
        and c.pod_name != ""
        and c.instance_uid != ""
        and c.instance_epoch != 0
        and c.gen != 0
        and c.jti != ""
        and c.exp_ms > now_ms
        and c.kid != ""
        and c.token_sha256 != ""
        and c.writer_epoch == DS_AUTH_WRITER_EPOCH_V2
    )


class RedisBattleAuthReader:
    """与 ds_allocator 共用 Redis 唯一授权记录。Go: `data.RedisBattleAuthReader`。

    同时实现 Go 的 `BattleAuthReader`(只读快照)与 `BattleResultRecorder`
    (receipt 写入)—— Go 那边也是同一个 struct 实现两个接口。
    """

    __slots__ = ("_rdb",)

    def __init__(self, rdb) -> None:  # noqa: ANN001 —— redis.asyncio 客户端
        self._rdb = rdb

    async def get_battle_authority(
        self, match_id: int
    ) -> tuple[Any | None, Any | None, bool]:
        if self._rdb is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle auth redis reader is not initialized"
            )
        if match_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "battle auth match id is zero")
        values = await self._rdb.mget(battle_auth_key(match_id), battle_key(match_id))
        if len(values) != 2 or values[0] is None or values[1] is None:
            return None, None, False
        auth_record = dspb.BattleDSAuthStorageRecord()
        battle_record = dspb.BattleStorageRecord()
        try:
            auth_record.ParseFromString(bytes(values[0]))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "unmarshal battle auth %d: %s", match_id, exc
            ) from exc
        try:
            battle_record.ParseFromString(bytes(values[1]))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "unmarshal battle projection %d: %s", match_id, exc
            ) from exc
        return auth_record, battle_record, True

    async def record_battle_result(
        self, credential: BattleResultCredential, max_heartbeat_age_sec: float
    ) -> None:
        """写 result receipt。SET 是"结算已权威接收"的线性化点。

        Go: `RecordBattleResult`。在同一 WATCH 中重新核验 active tuple 与 battle
        投影;并发轮换、终止、UID 重建或 allocation 漂移都会**零写入**失败。

        ★ receipt TTL 取 battle 投影的**剩余 PTTL**,不取 token exp:receipt 是
          "已经发生过的事"的证明,被旧 token 的 TTL 提前截断会让 ended 心跳找不到
          它,终态释放卡住(表现为对局结束后 DS 迟迟不回收)。
        ★ PTTL <= 0 直接 fail:battle 投影没有有界保留期时,receipt 写进去就是
          永不过期的垃圾键。
        """
        if self._rdb is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle result receipt authority unavailable"
            )
        if max_heartbeat_age_sec <= 0:
            max_heartbeat_age_sec = DEFAULT_ACTIVE_HEARTBEAT_MAX_AGE_SEC
        max_age_ms = int(max_heartbeat_age_sec * 1000)
        if not _valid_result_credential(credential, int(time.time() * 1000)):
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "battle result credential incomplete or expired"
            )
        a_key = battle_auth_key(credential.match_id)
        b_key = battle_key(credential.match_id)
        r_key = dsauthrecord.battle_result_receipt_key(credential.match_id)
        for _ in range(BATTLE_RESULT_RECEIPT_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key, r_key)
                    now_ms = int(time.time() * 1000)
                    values = await pipe.mget(a_key, b_key, r_key)
                    if len(values) != 3 or values[0] is None or values[1] is None:
                        raise errcode.PandoraError(
                            errcode.ErrUnauthorized, "battle authority missing"
                        )
                    auth_record = dspb.BattleDSAuthStorageRecord()
                    battle_record = dspb.BattleStorageRecord()
                    auth_record.ParseFromString(bytes(values[0]))
                    battle_record.ParseFromString(bytes(values[1]))
                    if not _result_authority_matches(
                        auth_record, battle_record, credential, now_ms, max_age_ms
                    ):
                        raise errcode.PandoraError(
                            errcode.ErrUnauthorized,
                            "battle authority changed before result receipt",
                        )
                    receipt = dsauthrecord.new_battle_result_receipt(
                        credential.match_id,
                        auth_record.allocation_id,
                        credential.pod_name,
                        credential.instance_uid,
                        credential.instance_epoch,
                        credential.gen,
                        credential.jti,
                        credential.exp_ms,
                        credential.kid,
                        credential.token_sha256,
                        credential.writer_epoch,
                        now_ms,
                    )
                    battle_ttl = await pipe.pttl(b_key)
                    if battle_ttl is None or battle_ttl <= 0:
                        raise errcode.PandoraError(
                            errcode.ErrUnavailable,
                            "battle projection has no bounded receipt retention",
                        )
                    pipe.multi()
                    if values[2] is not None:
                        existing = dsauthrecord.unmarshal_battle_result_receipt(values[2])
                        if not existing.valid(now_ms) or not existing.same_credential(receipt):
                            raise errcode.PandoraError(
                                errcode.ErrUnauthorized,
                                "battle result receipt belongs to another credential",
                            )
                        # 必须执行 EXEC 才能让 WATCH 对上述 authority 快照生效;同时把旧
                        # receipt 的保留期对齐当前 battle 生命周期。
                        pipe.pexpire(r_key, battle_ttl)
                    else:
                        pipe.set(
                            r_key,
                            dsauthrecord.marshal_battle_result_receipt(receipt),
                            px=battle_ttl,
                        )
                    await pipe.execute()
                    return
            except asyncio.CancelledError:
                raise
            except WatchError:
                # 并发写者动过被 WATCH 的三个键 → EXEC 未执行,零副作用,重来一轮。
                continue
            except errcode.PandoraError:
                # 业务判定(权威不符 / receipt 属于别的凭据 / 无保留期)是终态,不重试。
                raise
            except BaseException as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrUnavailable, "write battle result receipt failed: %s", exc
                ) from exc
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "battle result receipt concurrent retry exhausted"
        )


# ── 终态门 ───────────────────────────────────────────────────────────────────


class BattleCredentialStateChecker(Protocol):
    """证明已验签 JWT 此刻仍等于 Redis active。Go: `service.BattleCredentialStateChecker`。"""

    async def check_active(
        self, match_id: int, cred: dsauth.VerifiedCredential | None
    ) -> None: ...

    async def authorize_result(
        self, match_id: int, cred: dsauth.VerifiedCredential | None
    ) -> brepo.TerminalReleaseRecord: ...

    async def mark_result_recorded(
        self, match_id: int, cred: dsauth.VerifiedCredential | None
    ) -> None: ...


class RedisBattleCredentialStateChecker:
    """Go: `service.redisBattleCredentialStateChecker`。"""

    __slots__ = ("_reader", "_recorder", "_max_age_sec")

    def __init__(self, reader, max_age_sec: float = 0.0) -> None:  # noqa: ANN001
        self._reader = reader
        # Go: `recorder, _ := reader.(data.BattleResultRecorder)` —— 同一个对象若也
        # 实现了写端就顺带拿来用,没有就只做只读门(receipt 由出箱 relay 补)。
        self._recorder = reader if hasattr(reader, "record_battle_result") else None
        self._max_age_sec = (
            max_age_sec if max_age_sec > 0 else DEFAULT_ACTIVE_HEARTBEAT_MAX_AGE_SEC
        )

    async def check_active(
        self, match_id: int, cred: dsauth.VerifiedCredential | None
    ) -> None:
        await self.authorize_result(match_id, cred)

    async def authorize_result(  # noqa: C901 —— 与 Go 同为线性闸门链
        self, match_id: int, cred: dsauth.VerifiedCredential | None
    ) -> brepo.TerminalReleaseRecord:
        """返回只能由服务端 active 快照构造的持久 terminal-release 证明。

        `authorized_at_ms` 取 checker 本机校验时刻,绝不接受 DS 请求字段。
        """
        reason = credential_scope_reason(match_id, cred)
        if reason:
            _log_credential_reject(match_id, reason, cred)
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "battle credential is incomplete or scope mismatched"
            )
        assert cred is not None  # noqa: S101 —— scope 检查已排除 None,给类型检查器用
        if self._reader is None:
            _log_credential_reject(
                match_id,
                "authority_not_wired",
                cred,
                hint="checker 未接线(reader 为空),本进程无法证明令牌仍等于 active",
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle credential authority is unavailable"
            )
        try:
            rec, battle, found = await self._reader.get_battle_authority(match_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            _log_credential_reject(match_id, "authority_read_failed", cred, err=str(exc))
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle credential authority read failed: %s", exc
            ) from exc
        if not found or rec is None or battle is None:
            _log_credential_reject(
                match_id,
                "authority_not_found",
                cred,
                found=found,
                has_auth_record=rec is not None,
                has_battle_record=battle is not None,
                hint="Redis 里没有本局 active 凭据(已回收 / 从未写入 / 换了 authority 实例)",
            )
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "battle credential is not active"
            )
        if rec.phase not in (
            dspb.BATTLE_AUTH_PHASE_ACTIVE,
            dspb.BATTLE_AUTH_PHASE_ROTATING,
        ):
            _log_credential_reject(
                match_id,
                "phase_not_active",
                cred,
                phase=enum_name(dspb.BattleAuthPhase, rec.phase),
            )
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "battle credential phase is not active"
            )
        if not rec.HasField("active"):
            _log_credential_reject(
                match_id,
                "active_credential_missing",
                cred,
                phase=enum_name(dspb.BattleAuthPhase, rec.phase),
            )
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "battle active credential is missing"
            )
        active = rec.active
        now_ms = int(time.time() * 1000)
        max_age_ms = int(self._max_age_sec * 1000)
        reason = credential_freshness_reason(now_ms, max_age_ms, cred, rec, active)
        if reason:
            _log_credential_reject(
                match_id,
                reason,
                cred,
                now_ms=now_ms,
                token_exp_ms=cred.exp_ms,
                active_exp_ms=active.exp_ms,
                last_heartbeat_ms=rec.last_active_heartbeat_ms,
                max_heartbeat_age_ms=max_age_ms,
            )
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "battle credential expired or heartbeat stale"
            )
        reason = credential_authority_reason(match_id, cred, rec, battle, active)
        if reason:
            _log_credential_reject(
                match_id,
                reason,
                cred,
                record_pod=rec.ds_pod_name,
                battle_pod=battle.ds_pod_name,
                battle_state=battle.state,
                allocation_id=rec.allocation_id,
                active_gen=active.gen,
                high_water_gen=rec.high_water_gen,
                active_instance_epoch=active.instance_epoch,
                active_kid=active.kid,
                hint="已验签令牌与 Redis active 权威不一致(换 pod / epoch 轮换 / 换钥 / 僵尸 DS)",
            )
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "battle credential does not match active authority"
            )
        try:
            player_ids = canonical_battle_roster(battle.player_ids)
        except errcode.PandoraError as exc:
            _log_credential_reject(
                match_id,
                "canonical_roster_invalid",
                cred,
                roster_size=len(battle.player_ids),
                err=str(exc),
                hint="canonical BattleStorageRecord 名单为空 / 含 0 / 有重复,本局无法结算",
            )
            raise
        return brepo.TerminalReleaseRecord(
            match_id=match_id,
            allocation_id=rec.allocation_id,
            ds_pod_name=cred.pod,
            gameserver_uid=cred.instance_uid,
            instance_epoch=cred.protocol_epoch,
            auth_gen=cred.gen,
            auth_jti=cred.jti,
            auth_exp_ms=cred.exp_ms,
            auth_kid=cred.kid,
            auth_token_sha256=cred.token_sha256,
            auth_writer_epoch=cred.writer_epoch,
            authorized_at_ms=now_ms,
            player_ids=player_ids,
            # canonical game_mode/map_id/rating_mode 与 roster 同源:取自已通过上方精确
            # 比对的 BattleStorageRecord 快照,不做二次 Redis 查询,也绝不用 DS 请求体
            # 补值。滚动升级前的旧记录 game_mode 可能为空、rating_mode 可能是
            # UNSPECIFIED,biz 层按"canonical 未知"保守处理。
            game_mode=battle.game_mode,
            map_id=battle.map_id,
            rating_mode=int(battle.rating_mode),
            rating_pool=battle.rating_pool,
        )

    async def mark_result_recorded(
        self, match_id: int, cred: dsauth.VerifiedCredential | None
    ) -> None:
        """MySQL 幂等落库后写 result receipt;ended 心跳只消费该凭据。

        ★ 调用方**必须**把这里的失败降级成 WARN 后照常回 OK:MySQL 已把同一鉴权
          证明与战绩原子写入 terminal_release_outbox,后台 relay 会用持久证明完成
          terminal CAS + UID 回收。这里只是低延迟优化。
        """
        if self._recorder is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle result receipt writer is unavailable"
            )
        if (
            cred is None
            or match_id == 0
            or cred.match_id != match_id
            or cred.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
        ):
            raise errcode.PandoraError(
                errcode.ErrUnauthorized,
                "battle result credential writer epoch is not supported",
            )
        await self._recorder.record_battle_result(
            BattleResultCredential(
                match_id=match_id,
                pod_name=cred.pod,
                instance_uid=cred.instance_uid,
                instance_epoch=cred.protocol_epoch,
                gen=cred.gen,
                jti=cred.jti,
                exp_ms=cred.exp_ms,
                kid=cred.kid,
                token_sha256=cred.token_sha256,
                writer_epoch=cred.writer_epoch,
            ),
            self._max_age_sec,
        )


def new_battle_credential_state_checker(
    reader, max_age_sec: float = 0.0
) -> RedisBattleCredentialStateChecker:  # noqa: ANN001
    """Go: `NewBattleCredentialStateChecker`。"""
    return RedisBattleCredentialStateChecker(reader, max_age_sec)
