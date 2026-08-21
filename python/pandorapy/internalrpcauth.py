"""东西向 unary RPC 的服务身份签名 —— 对应 Go 侧 pkg/internalrpcauth。

一份凭证把「调用方身份 + 精确 gRPC 方法 + 请求主体 + 时间戳 + 一次性 nonce」
一起签进 HMAC-SHA256;校验方在**共享**重放库里消费掉 nonce 之后才放行 handler。

★ 它与玩家 JWT / DS 回调令牌 / placement proof 是**四个互不相干的信任域**,
  刻意不复用密钥也不复用中间件:共用一把钥匙 = 任一方能冒充另一方。

★ 逐字节对齐 Go 的地方(每一条都是「改了不报错、只在跨语言互调时静默拒签」):
  - metadata key 名(全小写,gRPC 规范会把 key 强制小写);
  - canonical message 的**字段顺序与分隔符**(`\\n` join),多一个字段就永远对不上;
  - 协议版本串 `pandora-internal-rpc-v1` / `pandora-internal-rpc-payload-v1`;
  - 签名与 nonce 都是 **base64 raw-url**(无 padding)—— 用标准 base64 会带 `=`,
    Go 侧 DecodeString 直接失败,表现是「Python 签的 matchmaker 全拒」;
  - nonce 24 字节;时钟偏差窗 `(0,5m]`;replay TTL = 2×skew + 1s。

★ 重放库必须是**跨副本共享**的(Redis)。进程内缓存不够:同一份凭证在 skew 窗口内
  可以打到另一个副本上,而那个副本从没见过这个 nonce。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import os
import secrets
import unicodedata
from typing import Protocol

# ── metadata key(必须与 Go 逐字一致;gRPC 会把 key 小写化)────────────────────
CALLER_METADATA_KEY = "x-pandora-service-caller"
AUDIENCE_METADATA_KEY = "x-pandora-service-audience"
TIMESTAMP_METADATA_KEY = "x-pandora-service-timestamp-ms"
NONCE_METADATA_KEY = "x-pandora-service-nonce"
SIGNATURE_METADATA_KEY = "x-pandora-service-signature"
PAYLOAD_METADATA_KEY = "x-pandora-service-payload-sha256"

_PROTOCOL_VERSION = "pandora-internal-rpc-v1"
_PAYLOAD_PROTOCOL_VERSION = "pandora-internal-rpc-payload-v1"
_NONCE_BYTES = 24

MIN_SECRET_BYTES = 32
MAX_CLOCK_SKEW_LIMIT_SEC = 300.0


class InternalRPCAuthError(Exception):
    """本模块所有失败的基类。"""


class ErrUnauthorized(InternalRPCAuthError):
    """签名 / 身份 / 时间窗任一不过。对应 Go 的 ErrUnauthorized。"""


class ErrReplay(InternalRPCAuthError):
    """nonce 已被消费过。对应 Go 的 ErrReplay。"""


class ErrUnavailable(InternalRPCAuthError):
    """重放库不可用 —— **说不清是不是重放**。

    ★ 必须与 ErrUnauthorized 分开:调用方据此回 ERR_UNAVAILABLE 让对端重试,
    而不是回 ERR_PERMISSION_DENY 把一次合法调用永久判成越权
    (Go 的 service.verifyMatchCall 就是按这个区分选错误码的)。
    """


class ReplayStore(Protocol):
    """原子消费一个已签名 nonce。实现必须被受保护服务的**全部副本**共享。"""

    async def consume(self, nonce_key: str, ttl_sec: float) -> bool: ...


def validate_identity(identity: str) -> None:
    """caller / audience 身份串的形状校验(对应 Go 的 ValidateIdentity)。"""
    if (
        not identity.strip()
        or identity != identity.strip()
        or any(ch in identity for ch in "\r\n\t")
        or len(identity.encode("utf-8")) > 128
    ):
        raise ValueError(
            "identity must be a non-empty, trimmed value no longer than 128 bytes"
        )


def validate_secret(secret: str) -> None:
    """密钥策略。**绝不回显密钥本身**(对应 Go 的 ValidateSecret)。

    32 字节下限是 HMAC-SHA256 的熵下限;控制字符会在 yaml / env 往返里被吃掉或
    转义,两侧算出的 message 不同 —— 表现是「配置看起来一样但验签永远失败」。
    """
    if len(secret.encode("utf-8")) < MIN_SECRET_BYTES:
        raise ValueError("internal RPC auth secret must be at least 32 bytes")
    for ch in secret:
        if unicodedata.category(ch) == "Cc":
            raise ValueError("internal RPC auth secret contains control characters")


def _validate_secret_and_identities(secret: str, caller: str, audience: str) -> None:
    validate_secret(secret)
    try:
        validate_identity(caller)
    except ValueError as exc:
        raise ValueError(f"internal RPC caller identity is invalid: {exc}") from exc
    try:
        validate_identity(audience)
    except ValueError as exc:
        raise ValueError(f"internal RPC audience identity is invalid: {exc}") from exc


def _b64(raw: bytes) -> str:
    """base64 raw-url(**无 padding**)—— 与 Go 的 base64.RawURLEncoding 同。"""
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64_decode(text: str) -> bytes:
    pad = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + pad)


def _canonical(
    caller: str, audience: str, method: str, subject: int, timestamp_ms: int, nonce: str
) -> str:
    return "\n".join(
        [_PROTOCOL_VERSION, caller, audience, method, str(subject), str(timestamp_ms), nonce]
    )


def _canonical_payload(
    caller: str,
    audience: str,
    method: str,
    subject: int,
    timestamp_ms: int,
    nonce: str,
    payload_digest: str,
) -> str:
    return "\n".join(
        [
            _PAYLOAD_PROTOCOL_VERSION,
            caller,
            audience,
            method,
            str(subject),
            str(timestamp_ms),
            nonce,
            payload_digest,
        ]
    )


def _sign(secret: bytes, message: str) -> str:
    return _b64(hmac.new(secret, message.encode("utf-8"), hashlib.sha256).digest())


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)


class Signer:
    """出站签名器(caller 侧)。对应 Go 的 *Signer。"""

    __slots__ = ("_secret", "_caller", "_audience", "_now_ms")

    def __init__(self, secret: str, caller: str, audience: str, *, now_ms=_now_ms) -> None:
        _validate_secret_and_identities(secret, caller, audience)
        self._secret = secret.encode("utf-8")
        self._caller = caller
        self._audience = audience
        self._now_ms = now_ms

    @property
    def caller(self) -> str:
        return self._caller

    def sign_metadata(self, full_method: str, subject: int) -> list[tuple[str, str]]:
        """为一次调用铸一份新鲜凭证,返回 grpc metadata 键值对。

        ★ 每次调用**都要重签**:nonce 一次性、时间戳有窗。缓存一份重复用
        等于自己触发 ErrReplay,而那在日志里长得像「被攻击」。
        """
        return self._sign_metadata(full_method, subject, "")

    def sign_metadata_with_payload(
        self, full_method: str, subject: int, payload: bytes
    ) -> list[tuple[str, str]]:
        """签名精确请求载荷；与 Go ``SignContextWithPayload`` 逐字节兼容。"""
        if not payload:
            raise ErrUnauthorized("non-empty payload required")
        digest = _b64(hashlib.sha256(payload).digest())
        return self._sign_metadata(full_method, subject, digest)

    def _sign_metadata(
        self, full_method: str, subject: int, payload_digest: str
    ) -> list[tuple[str, str]]:
        if not full_method or subject == 0:
            raise ErrUnauthorized("full_method and non-zero subject required")
        nonce = _b64(os.urandom(_NONCE_BYTES))
        timestamp = self._now_ms()
        message = _canonical(
            self._caller, self._audience, full_method, subject, timestamp, nonce
        )
        if payload_digest:
            message = _canonical_payload(
                self._caller,
                self._audience,
                full_method,
                subject,
                timestamp,
                nonce,
                payload_digest,
            )
        metadata = [
            (CALLER_METADATA_KEY, self._caller),
            (AUDIENCE_METADATA_KEY, self._audience),
            (TIMESTAMP_METADATA_KEY, str(timestamp)),
            (NONCE_METADATA_KEY, nonce),
            (SIGNATURE_METADATA_KEY, _sign(self._secret, message)),
        ]
        if payload_digest:
            metadata.append((PAYLOAD_METADATA_KEY, payload_digest))
        return metadata


class Verifier:
    """入站校验器(被调方)。**fail-closed**:任何一项对不上一律拒。

    对应 Go 的 *Verifier。共享 replay store 是**必填** —— 没有它,一份合法凭证
    可以在时钟偏差窗内被重放到另一个副本上,而那个副本毫不知情。
    """

    __slots__ = (
        "_secret",
        "_caller",
        "_audience",
        "_max_skew_sec",
        "_replay_ttl_sec",
        "_replays",
        "_now_ms",
    )

    def __init__(
        self,
        secret: str,
        expected_caller: str,
        expected_audience: str,
        max_clock_skew_sec: float,
        replays: ReplayStore,
        *,
        now_ms=_now_ms,
    ) -> None:
        _validate_secret_and_identities(secret, expected_caller, expected_audience)
        if max_clock_skew_sec <= 0 or max_clock_skew_sec > MAX_CLOCK_SKEW_LIMIT_SEC:
            raise ValueError("internal RPC max clock skew must be within (0,5m]")
        if replays is None:
            raise ValueError("internal RPC shared replay store is required")
        self._secret = secret.encode("utf-8")
        self._caller = expected_caller
        self._audience = expected_audience
        self._max_skew_sec = max_clock_skew_sec
        # 凭证时间戳可以「未来 skew」也可以「过去 skew」,nonce 必须覆盖整段区间,
        # 再加 1s 兜毫秒边界(与 Go 逐字同)。
        self._replay_ttl_sec = 2 * max_clock_skew_sec + 1.0
        self._replays = replays
        self._now_ms = now_ms

    @property
    def caller(self) -> str:
        return self._caller

    async def verify(self, metadata: dict[str, str], full_method: str, subject: int) -> None:
        """校验并**消费** nonce。通过返回 None,否则抛上面三种异常之一。

        metadata:入站 metadata 的「key → 单值」视图。多值 / 空值一律按缺失处理
        (Go 的 single() 语义:len != 1 或空串都不接受)—— 允许多值等于允许
        攻击者追加一个自己的签名头。
        """
        await self._verify(metadata, full_method, subject, "")

    async def verify_with_payload(
        self,
        metadata: dict[str, str],
        full_method: str,
        subject: int,
        payload: bytes,
    ) -> None:
        """校验精确请求载荷，且只在载荷摘要匹配后消费 nonce。"""
        if not payload:
            raise ErrUnauthorized("non-empty payload required")
        await self._verify(
            metadata,
            full_method,
            subject,
            _b64(hashlib.sha256(payload).digest()),
        )

    async def _verify(
        self,
        metadata: dict[str, str],
        full_method: str,
        subject: int,
        expected_payload_digest: str,
    ) -> None:
        if len(self._secret) < MIN_SECRET_BYTES or not full_method or subject == 0:
            raise ErrUnauthorized("verifier not usable for this call")

        caller = metadata.get(CALLER_METADATA_KEY, "")
        if not caller or caller != self._caller:
            raise ErrUnauthorized("caller mismatch")
        audience = metadata.get(AUDIENCE_METADATA_KEY, "")
        if not audience or audience != self._audience:
            raise ErrUnauthorized("audience mismatch")
        ts_raw = metadata.get(TIMESTAMP_METADATA_KEY, "")
        try:
            timestamp = int(ts_raw)
        except ValueError as exc:
            raise ErrUnauthorized("bad timestamp") from exc
        if timestamp <= 0:
            raise ErrUnauthorized("bad timestamp")
        nonce = metadata.get(NONCE_METADATA_KEY, "")
        if not _valid_nonce(nonce):
            raise ErrUnauthorized("bad nonce")
        signature = metadata.get(SIGNATURE_METADATA_KEY, "")
        if not signature:
            raise ErrUnauthorized("missing signature")
        try:
            provided = _b64_decode(signature)
        except Exception as exc:  # noqa: BLE001
            raise ErrUnauthorized("bad signature encoding") from exc
        if len(provided) != hashlib.sha256().digest_size:
            raise ErrUnauthorized("bad signature length")

        message = _canonical(caller, audience, full_method, subject, timestamp, nonce)
        if expected_payload_digest:
            payload_digest = metadata.get(PAYLOAD_METADATA_KEY, "")
            if not payload_digest or not hmac.compare_digest(
                payload_digest, expected_payload_digest
            ):
                raise ErrUnauthorized("payload digest mismatch")
            message = _canonical_payload(
                caller,
                audience,
                full_method,
                subject,
                timestamp,
                nonce,
                payload_digest,
            )
        elif metadata.get(PAYLOAD_METADATA_KEY):
            # 不允许「带 payload 绑定的凭证」经不带 payload 的 verify 静默降级
            # (Go 侧同一条判断)。
            raise ErrUnauthorized("payload-bound credential cannot downgrade")
        expected = _b64_decode(_sign(self._secret, message))
        if not hmac.compare_digest(provided, expected):
            raise ErrUnauthorized("signature mismatch")

        delta_sec = (self._now_ms() - timestamp) / 1000.0
        if delta_sec > self._max_skew_sec or delta_sec < -self._max_skew_sec:
            raise ErrUnauthorized("timestamp outside clock skew window")

        try:
            consumed = await self._replays.consume(
                f"{caller}:{nonce}", self._replay_ttl_sec
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 说不清是不是重放 → 交给调用方回可重试码,绝不当成越权。
            raise ErrUnavailable(str(exc)) from exc
        if not consumed:
            raise ErrReplay("nonce already consumed")


class MultiCallerVerifier:
    """按已签名 caller 分派到独立 ``Verifier``，不共享调用方密钥。

    caller 只用于选择候选 verifier；真正的 caller/audience/签名/载荷/重放校验
    仍由该 verifier 完整执行。未知 caller 在触碰 replay store 之前拒绝。
    """

    __slots__ = ("_by_caller",)

    def __init__(self, *verifiers: Verifier) -> None:
        if not verifiers:
            raise ValueError("internal RPC multi-caller verifier requires at least one verifier")
        by_caller: dict[str, Verifier] = {}
        for verifier in verifiers:
            if verifier is None:
                raise ValueError("internal RPC multi-caller verifier must not be None")
            caller = verifier.caller
            if caller in by_caller:
                raise ValueError(f"internal RPC multi-caller duplicate caller: {caller}")
            by_caller[caller] = verifier
        self._by_caller = by_caller

    @property
    def callers(self) -> tuple[str, ...]:
        return tuple(self._by_caller)

    def _select(self, metadata: dict[str, str]) -> Verifier:
        caller = metadata.get(CALLER_METADATA_KEY, "")
        verifier = self._by_caller.get(caller)
        if verifier is None:
            raise ErrUnauthorized("unknown caller")
        return verifier

    async def verify(
        self, metadata: dict[str, str], full_method: str, subject: int
    ) -> None:
        await self._select(metadata).verify(metadata, full_method, subject)

    async def verify_with_payload(
        self,
        metadata: dict[str, str],
        full_method: str,
        subject: int,
        payload: bytes,
    ) -> None:
        await self._select(metadata).verify_with_payload(
            metadata, full_method, subject, payload
        )


def _valid_nonce(encoded: str) -> bool:
    if not encoded:
        return False
    try:
        return len(_b64_decode(encoded)) == _NONCE_BYTES
    except Exception:  # noqa: BLE001
        return False


class RedisReplayStore:
    """Redis 实现(SETNX + TTL)。对应 Go 的 RedisReplayStore。

    key 里放的是 `caller:nonce` 的 sha256 **十六进制**,不是原文:
    nonce 是攻击者可控的 metadata,直接拼进 key 会让它变成 Redis keyspace 的一部分,
    也会让日志 / SCAN 输出泄漏一个还能用的 nonce。
    """

    __slots__ = ("_client", "_prefix")

    DEFAULT_PREFIX = "pandora:internal-rpc:nonce:"

    def __init__(self, client, prefix: str = "") -> None:  # noqa: ANN001
        if client is None:
            raise ValueError("internal RPC Redis replay client is required")
        self._client = client
        self._prefix = prefix or self.DEFAULT_PREFIX

    async def consume(self, nonce_key: str, ttl_sec: float) -> bool:
        if not nonce_key or ttl_sec <= 0:
            raise ValueError("internal RPC Redis replay store unavailable")
        digest = hashlib.sha256(nonce_key.encode("utf-8")).hexdigest()
        ok = await self._client.set(
            self._prefix + digest, "1", px=int(ttl_sec * 1000), nx=True
        )
        return bool(ok)


def new_nonce() -> str:
    """仅供测试构造凭证用。"""
    return _b64(secrets.token_bytes(_NONCE_BYTES))
