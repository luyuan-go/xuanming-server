"""player_locator 的 Hub DS active credential 终态门 —— 对应 Go 侧
internal/data/hub_auth.go + internal/service/hub_credential.go。

JWT 验签只证明「令牌由受信签发方签过」;本模块再读取 Redis 唯一授权权威
(`pandora:hub:auth:{pod}`,写者只有 hub_allocator),证明这份
(GameServer UID, protocol epoch, gen, jti) 凭据**此刻**仍是 active。
任一条不成立都在位置 / TTL / presence 副作用**之前** fail-closed 返回。

★ 只读,永不回写:授权状态的唯一写者仍是 hub_allocator。旧副本 read-modify-write
  会丢弃 unknown fields(不变量 §17),所以这里连 SET 的代码都不存在。

★ 失败通道用 **in-band code**,不是异常 —— 与 `pandorapy.dsauth.DSCallbackGuard.check`
  同档。理由见那边的长注释:本仓 service 层调用点一律
  `code = check(...); if code != 0: return Resp(code=...)`,没有一个包在 try 里;
  改成抛异常会让它逐出 handler,客户端拿到 UNKNOWN 而不是可解析的业务码。
  Go 侧 `CheckActive` 返回 error,但它的调用点紧接着就 `toProtoCode(err)`,
  语义等价。

★ 这里必须用**墙钟**(`time.time()`)而不是 `time.monotonic()`:比较对象是 Redis 里
  存的绝对 unix 毫秒(`active.exp_ms` / `last_active_heartbeat_ms`),单调时钟与它
  不在同一坐标系。租约本地安全截止那类「本进程还能写多久」才用单调钟,
  由 `pandorapy.dsauthfence` 负责,两者不要互相套用。
"""

from __future__ import annotations

import asyncio
import dataclasses
import hmac
import time
from typing import Any, Protocol

from pandora.hub.v1 import allocator_pb2 as hubpb

from pandorapy import dsauth
from pandorapy import errcode
from pandorapy import log as plog
from pandorapy.auth import DS_AUTH_WRITER_EPOCH_V2

# Hub 凭据终态门的拒绝 reason 枚举(§11.3 R2:一个 if 收敛 N 个条件必须拆成 N 个 reason)。
# ★ 取值与 Go 的 credReason* 常量逐字节一致 —— 它们进 Loki 查询与告警,漂移即查询落空。
CRED_REASON_INCOMPLETE_CLAIMS = "credential_claims_incomplete"
CRED_REASON_AUTHORITY_DOWN = "authority_unavailable"
CRED_REASON_AUTHORITY_READ_FAIL = "authority_read_failed"
CRED_REASON_NOT_ACTIVE = "authority_record_absent"
CRED_REASON_PHASE_NOT_ACTIVE = "authority_phase_not_active"
CRED_REASON_ACTIVE_MISSING = "authority_active_missing"
CRED_REASON_EXPIRED = "credential_expired"
CRED_REASON_HEARTBEAT_STALE = "active_heartbeat_stale"
CRED_REASON_AUTHORITY_MISMATCH = "credential_authority_mismatch"

# Go: `age := 30 * time.Second`(NewHubCredentialStateChecker 的 maxAge 缺省)。
DEFAULT_ACTIVE_HEARTBEAT_MAX_AGE_SEC = 30.0


def hub_auth_key(pod: str) -> str:
    """`pandora:hub:auth:{pod}` —— 与 hub_allocator 的 `ledger.auth_key` 同一把 key。

    ★ 花括号是 Redis Cluster hashtag(与分片镜像 / 代际计数器同 slot),不是格式占位符。
    """
    return f"pandora:hub:auth:{{{pod}}}"


class HubAuthReader(Protocol):
    """Hub 授权权威的只读端口 —— 对应 Go 的 `data.HubAuthReader`。"""

    async def get_hub_auth(self, pod: str) -> tuple[Any | None, bool]:
        """返回 `(record, found)`;key miss = `(None, False)`,**不是**异常。

        读失败 / 解码失败必须**抛异常**:调用方据此分别映射成
        「凭据未激活」(fail-closed 拒绝)与「授权权威不可用」(可重试),
        两者混同会让 Redis 抖动被记成「这台 DS 的凭据被吊销了」。
        """
        ...


class RedisHubAuthReader:
    """从与 hub_allocator 共用的 Redis 读取授权记录。Go: `data.RedisHubAuthReader`。"""

    __slots__ = ("_rdb",)

    def __init__(self, rdb) -> None:  # noqa: ANN001 —— redis.asyncio 客户端
        self._rdb = rdb

    async def get_hub_auth(self, pod: str) -> tuple[Any | None, bool]:
        if self._rdb is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "hub auth redis reader is not initialized"
            )
        if not pod:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "hub auth pod is empty")
        raw = await self._rdb.get(hub_auth_key(pod))
        if raw is None:
            return None, False
        rec = hubpb.HubShardAuthStorageRecord()
        try:
            rec.ParseFromString(bytes(raw))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "unmarshal hub auth %s: %s", pod, exc
            ) from exc
        return rec, True


@dataclasses.dataclass(frozen=True, slots=True)
class ActiveCheck:
    """终态门判定结果。`code == 0` 放行。

    `err` 是 Go 侧 `errcode.New(...)` 的 message 文本,**逐字节照抄** ——
    它会原样进 `locator_ds_callback_rejected` 的 `err` 字段,是两个实现在同一条
    Loki 查询下可比对的唯一依据。
    """

    code: int
    err: str = ""


_OK = ActiveCheck(0)


class HubCredentialStateChecker(Protocol):
    """Go: `service.HubCredentialStateChecker`。"""

    async def check_active(
        self, pod: str, cred: dsauth.VerifiedCredential | None
    ) -> ActiveCheck: ...


def authority_mismatch_reason(
    pod: str, rec, active, cred: dsauth.VerifiedCredential
) -> str:  # noqa: ANN001 —— proto message
    """把最后那个「任一项不等都拒」的合取条件拆成单一枚举 reason。

    判定顺序与下面的 if **完全一致**(只读比较,不改变任何控制流)。
    """
    if rec.pod_name != pod:
        return "record_pod_mismatch"
    if not rec.instance_uid or rec.instance_uid != cred.instance_uid:
        return "record_instance_uid_mismatch"
    if rec.protocol_epoch == 0 or rec.protocol_epoch != cred.protocol_epoch:
        return "record_protocol_epoch_mismatch"
    if rec.required_writer_epoch != DS_AUTH_WRITER_EPOCH_V2:
        return "record_required_writer_epoch_mismatch"
    if rec.HasField("pending") and rec.pending.writer_epoch != DS_AUTH_WRITER_EPOCH_V2:
        return "pending_writer_epoch_mismatch"
    if not active.instance_uid or active.instance_uid != cred.instance_uid:
        return "active_instance_uid_mismatch"
    if active.protocol_epoch == 0 or active.protocol_epoch != cred.protocol_epoch:
        return "active_protocol_epoch_mismatch"
    if active.gen == 0 or active.gen != cred.gen:
        return "active_gen_mismatch"
    if not active.jti or active.jti != cred.jti:
        return "active_jti_mismatch"
    if active.exp_ms != cred.exp_ms:
        return "active_exp_mismatch"
    if not active.kid or active.kid != cred.kid or not active.token_sha256:
        return "active_kid_or_token_digest_missing"
    if active.writer_epoch != DS_AUTH_WRITER_EPOCH_V2 or active.writer_epoch != cred.writer_epoch:
        return "active_writer_epoch_mismatch"
    if rec.high_water_gen < active.gen:
        return "high_water_gen_regressed"
    return "token_digest_mismatch"


def _log_cred_rejected(
    reason: str, pod: str, cred: dsauth.VerifiedCredential | None, **extra: Any
) -> None:
    """记录一次 Hub 凭据终态门拒绝。

    为什么必须在这里打:`check_active` 返回的是 ErrUnauthorized / ErrUnavailable,
    调用方转成 in-band Code 后 handler 返回正常响应 → access log 只记 DEBUG。
    于是「Hub DS 明明在跑,写位置却全被拒」在线上零日志,现象只有玩家在大厅里查不到。
    频次:稳态恒 0;凭据轮转窗口内短暂出现,属预期。
    """
    fields: dict[str, Any] = {"reason": reason, "hub_pod": pod}
    if cred is not None:
        fields.update(
            req_instance_uid=cred.instance_uid,
            req_protocol_epoch=cred.protocol_epoch,
            req_gen=cred.gen,
            req_jti=cred.jti,
            req_writer_epoch=cred.writer_epoch,
            req_kid=cred.kid,
            req_exp_ms=cred.exp_ms,
            req_pod=cred.pod,
        )
    fields.update(extra)
    plog.get().warning("hub_credential_rejected", **fields)


def _now_ms() -> int:
    return int(time.time() * 1000)


class RedisHubCredentialStateChecker:
    """Redis active credential 终态门。Go: `redisHubCredentialStateChecker`。"""

    __slots__ = ("_reader", "_now_ms", "_max_active_heartbeat_age_sec")

    def __init__(
        self,
        reader: HubAuthReader,
        max_active_heartbeat_age_sec: float = 0.0,
        *,
        now_ms=_now_ms,  # noqa: ANN001 —— 仅测试注入
    ) -> None:
        age = DEFAULT_ACTIVE_HEARTBEAT_MAX_AGE_SEC
        if max_active_heartbeat_age_sec > 0:
            age = max_active_heartbeat_age_sec
        self._reader = reader
        self._now_ms = now_ms
        self._max_active_heartbeat_age_sec = age

    async def check_active(  # noqa: C901 —— 与 Go 同为线性判定表
        self, pod: str, cred: dsauth.VerifiedCredential | None
    ) -> ActiveCheck:
        # Model B 下 legacy / 不完整凭据**绝不回退放行**。JWT exp 虽已由 verifier 校验,
        # 这里仍把 claim exp 与 Redis active.exp_ms 精确绑定,避免 annotation / 外部数字
        # 参与授权。
        if (
            not pod
            or cred is None
            or cred.pod != pod
            or not cred.instance_uid
            or cred.protocol_epoch == 0
            or cred.gen == 0
            or not cred.jti
            or cred.exp_ms <= 0
            or not cred.token_sha256
            or not cred.kid
            or cred.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
        ):
            _log_cred_rejected(
                CRED_REASON_INCOMPLETE_CLAIMS,
                pod,
                cred,
                required_writer_epoch=DS_AUTH_WRITER_EPOCH_V2,
            )
            return ActiveCheck(
                errcode.ErrUnauthorized, "hub credential is incomplete or scope mismatched"
            )
        if self._reader is None or self._now_ms is None:
            _log_cred_rejected(CRED_REASON_AUTHORITY_DOWN, pod, cred)
            return ActiveCheck(
                errcode.ErrUnavailable, "hub credential authority is unavailable"
            )

        try:
            rec, found = await self._reader.get_hub_auth(pod)
        except asyncio.CancelledError:
            # ★ 取消必须穿透:被下面那条宽 except 吞掉之后取消就不再传播,优雅停机时
            # 会把「取消」翻译成「授权权威读失败」这种假原因,而排空在途也没有真的发生。
            raise
        except BaseException as exc:  # noqa: BLE001
            _log_cred_rejected(CRED_REASON_AUTHORITY_READ_FAIL, pod, cred, err=str(exc))
            return ActiveCheck(
                errcode.ErrUnavailable, "hub credential authority read failed"
            )
        if not found or rec is None:
            _log_cred_rejected(CRED_REASON_NOT_ACTIVE, pod, cred, found=found)
            return ActiveCheck(errcode.ErrUnauthorized, "hub credential is not active")

        # ROTATING 表示 active+pending 并存;旧 active 在 pending 被激活前仍是权威,
        # 必须继续可用以保证零停机。其余 phase 都没有可用于普通写 RPC 的 active 权限。
        if rec.phase not in (hubpb.HUB_AUTH_PHASE_ACTIVE, hubpb.HUB_AUTH_PHASE_ROTATING):
            _log_cred_rejected(
                CRED_REASON_PHASE_NOT_ACTIVE, pod, cred, cur_phase=int(rec.phase)
            )
            return ActiveCheck(
                errcode.ErrUnauthorized, "hub credential phase is not active"
            )
        if not rec.HasField("active"):
            _log_cred_rejected(
                CRED_REASON_ACTIVE_MISSING, pod, cred, cur_phase=int(rec.phase)
            )
            return ActiveCheck(
                errcode.ErrUnauthorized, "hub credential active record is missing"
            )
        active = rec.active

        now_ms = self._now_ms()
        if now_ms <= 0 or cred.exp_ms <= now_ms or active.exp_ms == 0 or now_ms >= active.exp_ms:
            _log_cred_rejected(
                CRED_REASON_EXPIRED,
                pod,
                cred,
                now_ms=now_ms,
                cur_active_exp_ms=active.exp_ms,
            )
            return ActiveCheck(errcode.ErrUnauthorized, "hub credential has expired")

        last_heartbeat_ms = rec.last_active_heartbeat_ms
        max_heartbeat_age_ms = int(self._max_active_heartbeat_age_sec * 1000)
        if max_heartbeat_age_ms <= 0:
            max_heartbeat_age_ms = int(DEFAULT_ACTIVE_HEARTBEAT_MAX_AGE_SEC * 1000)
        # `last_heartbeat_ms > now_ms` 也拒:未来时间戳只可能来自时钟错乱或伪造,
        # 放行等于给「把心跳时间写到很远的未来」这种手法一张永久通行证。
        if (
            last_heartbeat_ms <= 0
            or last_heartbeat_ms > now_ms
            or now_ms - last_heartbeat_ms > max_heartbeat_age_ms
        ):
            _log_cred_rejected(
                CRED_REASON_HEARTBEAT_STALE,
                pod,
                cred,
                now_ms=now_ms,
                last_active_heartbeat_ms=last_heartbeat_ms,
                max_heartbeat_age_ms=max_heartbeat_age_ms,
            )
            return ActiveCheck(
                errcode.ErrUnauthorized, "hub credential active heartbeat is not fresh"
            )

        # record 顶层实例身份、active 内嵌身份和 JWT claims 三者必须完全一致。
        # high-water 小于 active.gen 表示权威记录自身不完整 / 回退,同样 fail-closed。
        digest_match = hmac.compare_digest(active.token_sha256, cred.token_sha256)
        if (
            rec.pod_name != pod
            or not rec.instance_uid
            or rec.instance_uid != cred.instance_uid
            or rec.protocol_epoch == 0
            or rec.protocol_epoch != cred.protocol_epoch
            or rec.required_writer_epoch != DS_AUTH_WRITER_EPOCH_V2
            or (rec.HasField("pending") and rec.pending.writer_epoch != DS_AUTH_WRITER_EPOCH_V2)
            or not active.instance_uid
            or active.instance_uid != cred.instance_uid
            or active.protocol_epoch == 0
            or active.protocol_epoch != cred.protocol_epoch
            or active.gen == 0
            or active.gen != cred.gen
            or not active.jti
            or active.jti != cred.jti
            or active.exp_ms != cred.exp_ms
            or not active.kid
            or active.kid != cred.kid
            or not active.token_sha256
            or active.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
            or active.writer_epoch != cred.writer_epoch
            or rec.high_water_gen < active.gen
            or not digest_match
        ):
            # 期望值(Redis active 权威)与实际值(JWT claims)并排打出:只报「不匹配」
            # 查不出是哪一代凭据在写、权威此刻认的是哪一代 —— 而那正是排查
            # 「旧 Hub 实例的迟到写为什么被 fencing 掉」需要的两个数。
            # token_sha256 只打是否相等,不打摘要本身。
            _log_cred_rejected(
                CRED_REASON_AUTHORITY_MISMATCH,
                pod,
                cred,
                detail_reason=authority_mismatch_reason(pod, rec, active, cred),
                cur_pod=rec.pod_name,
                cur_instance_uid=rec.instance_uid,
                cur_protocol_epoch=rec.protocol_epoch,
                cur_required_writer_epoch=rec.required_writer_epoch,
                cur_high_water_gen=rec.high_water_gen,
                cur_active_instance_uid=active.instance_uid,
                cur_active_protocol_epoch=active.protocol_epoch,
                cur_active_gen=active.gen,
                cur_active_jti=active.jti,
                cur_active_kid=active.kid,
                cur_active_exp_ms=active.exp_ms,
                cur_active_writer_epoch=active.writer_epoch,
                token_digest_match=digest_match,
            )
            return ActiveCheck(
                errcode.ErrUnauthorized, "hub credential does not match active authority"
            )
        return _OK


def new_hub_credential_state_checker(
    reader: HubAuthReader, max_active_heartbeat_age_sec: float = 0.0
) -> RedisHubCredentialStateChecker:
    """Go: `NewHubCredentialStateChecker`。"""
    return RedisHubCredentialStateChecker(reader, max_active_heartbeat_age_sec)
