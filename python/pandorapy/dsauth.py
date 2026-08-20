"""DS 回调服务令牌守卫 —— 对应 Go 侧 pkg/middleware/dsauth.go 的**校验侧**。

背景:DS 面网关 :8444 只有方法白名单 + NetworkPolicy 网络隔离,经它进来的调用
`x-pandora-player-id` 恒缺失(该监听器没有 jwt_authn)—— 于是「callerID==0 即后端
内部可信」在挂到该面的方法上不再成立:任何能连到 :8444、或绕过 Envoy 直连业务端口的
进程,都能按任意 player_id 拿出战快照 / 批量捞角色名。本守卫提供「调用方确实是 DS」
的那半证明。

★ 本模块是校验侧(DSCallbackVerifier / DSCallbackGuard)。**签发侧在 pandorapy.auth**
  (`Signer.sign_ds_callback` / `DSCallbackSigner`,对应 Go 的 pkg/auth);本模块只提供
  `signer_from_conf` 这个装配便利函数,与 `guard_from_conf` 对称 —— 分层和 Go 一致:
  令牌本体在 auth,按 ds_auth 配置装配在 middleware。签发方是 ds_allocator /
  hub_allocator(Go 与 Python 两栓同在),secret 三方同值 —— 两栓必须能互相验过对方
  签出的令牌,否则灰度期一台 DS 上所有玩家的回调会成批被拒。

★ 档位(config.DSAuthConf.mode),与 Go 逐字一致:
    off        → 直接放行(nil guard 等价)
    permissive → 完整验签 + 范围校验,失败只 warn 放行(灰度观察期)
    enforce    → 无有效令牌 / 范围不匹配一律拒
  **拼错的 mode 必须启动即失败**,不能静默回落 off:那等于把一道安全门悄悄关掉,
  而 yaml 上还写着 "enforce"。

★ 判定表(mode=enforce;permissive 把「拒绝」降级为 warn 放行):
    经 :8444 网关(带标记头) + 无/无效令牌            → ErrUnauthorized
    经 :8444 网关            + 令牌范围不匹配          → ErrPermissionDeny
    内部直连(无标记头)     + 无令牌                  → 放行(东西向内部调用不受影响)
    内部直连(无标记头)     + 无令牌 + require_token   → ErrUnauthorized
    任意来源                 + 带令牌但范围不匹配      → ErrPermissionDeny
    scope.deny_ds            + 经网关或带令牌          → ErrPermissionDeny
"""

from __future__ import annotations

import dataclasses
import enum
import hashlib

import jwt as pyjwt

from pandorapy import auth as pauth
from pandorapy import errcode
from pandorapy import log as plog

# 与 Go 侧一致的签名算法(pkg/auth.ALGORITHM)。
ALGORITHM = "HS256"

# HS256 密钥最小长度(RFC 7518 §3.2)。与 Go 的 auth.Config.Validate 同值 —— 放松它
# 等于允许一把能被离线爆破的密钥保护 DS 回调面。
MIN_SECRET_BYTES = 32

# Envoy 在 :8443/:8444 入站先无条件剥离同名头(防客户端/DS 伪造),再由 :8444 路由
# 重新写入。后端据此区分「DS 面进来的回调」与「集群内东西向内部调用」。
METADATA_KEY_DS_GATEWAY = "x-pandora-ds-gateway"
AUTHORIZATION_HEADER = "authorization"


class Mode(str, enum.Enum):
    OFF = "off"
    PERMISSIVE = "permissive"
    ENFORCE = "enforce"


def parse_mode(raw: str | None) -> Mode:
    """解析 ds_auth.mode;空串等价 "off",**非法值报错**(main 启动期 fail-fast)。

    不设这道闸会怎样:`mode: "enfroce"` 静默回落成 off,GetLoadout / GetPlayerNames
    的 DS 令牌门整个不生效,而 yaml 上写着 enforce、启动日志全绿。
    """
    text = (raw or "").strip().lower()
    if text in ("", "off"):
        return Mode.OFF
    if text == "permissive":
        return Mode.PERMISSIVE
    if text == "enforce":
        return Mode.ENFORCE
    raise ValueError(f"ds_auth.mode invalid: {raw!r} (want off|permissive|enforce)")


@dataclasses.dataclass(frozen=True, slots=True)
class DSScope:
    """一次回调允许的授权范围(handler 按请求参数填)。"""

    # ds_type 期望的 DS 类型(hub/battle);空不校验。
    ds_type: str = ""
    # match_id 非 0 时要求令牌 match_id 与之一致。
    match_id: int = 0
    # pod 非空时要求令牌 sub(pod 名)与之一致。
    pod: str = ""
    # deny_ds:「这种调用形态根本不该来自 DS」。命中时经网关或带令牌一律按越权处理。
    deny_ds: bool = False
    # require_token:「本回调只可能来自 DS,没有合法的东西向内部无令牌调用者」。
    # 命中时即使请求不带网关标记头,只要没有有效令牌也一律拒 —— 堵住「被攻破的
    # 业务 Pod 绕过 Envoy 直连业务端口、无标记无令牌却被当内部东西向信任」的旁路。
    require_token: bool = False


@dataclasses.dataclass(frozen=True, slots=True)
class DSCallbackClaims:
    """验签通过的 DS 回调令牌身份(只投影本仓校验侧用得到的字段)。"""

    ds_type: str
    match_id: int
    pod: str
    jti: str
    instance_uid: str = ""
    protocol_epoch: int = 0
    gen: int = 0
    exp_ms: int = 0
    kid: str = ""
    writer_epoch: int = 0


@dataclasses.dataclass(frozen=True, slots=True)
class VerifiedCredential:
    """从完整、验签通过的 Model B callback token 抽出的 active 身份。"""

    ds_type: str
    match_id: int
    pod: str
    instance_uid: str
    protocol_epoch: int
    gen: int
    jti: str
    exp_ms: int
    kid: str
    token_sha256: str
    writer_epoch: int


class DSCallbackVerifier:
    """HS256 DS 回调令牌验签器 —— 对应 Go 的 auth.DSCallbackVerifier。

    ★ 依次尝试 [主密钥, *additional_secrets]:additional 只用于校验、不用于签发,
      这是 DS 回调令牌**不停服三段式轮换**的全部机制。少了它,轮换中间那一段会
      出现「新副本签的令牌旧副本验不过」的 401 断档。
    """

    __slots__ = ("_issuer", "_audience", "_keys")

    def __init__(
        self, *, issuer: str, audience: str, secret: str, additional_secrets: list[str]
    ) -> None:
        if len(secret.encode("utf-8")) < MIN_SECRET_BYTES:
            raise ValueError(
                f"ds_auth: secret 长度 {len(secret.encode('utf-8'))} < {MIN_SECRET_BYTES} 字节"
            )
        keys = [secret]
        for i, extra in enumerate(additional_secrets):
            # ★ 空串条目是配置事故(轮换清单少写了一把却留了占位)。静默过滤会让运维
            # 以为旧密钥仍被接受、实则轮换断档 → 启动即报错(fail-closed)。
            if not extra:
                raise ValueError(
                    f"ds_auth: additional_secrets[{i}] is empty "
                    "(rotation misconfig; remove the entry or fill the key)"
                )
            if len(extra.encode("utf-8")) < MIN_SECRET_BYTES:
                raise ValueError(f"ds_auth: additional_secrets[{i}] 长度不足 {MIN_SECRET_BYTES} 字节")
            keys.append(extra)
        self._issuer = issuer
        self._audience = audience
        self._keys = tuple(keys)

    def verify(self, token: str) -> DSCallbackClaims:
        """验签并校验 iss / aud / exp。失败抛 ValueError(调用方翻成拒绝)。"""
        last: Exception | None = None
        for key in self._keys:
            try:
                payload = pyjwt.decode(
                    token,
                    key,
                    algorithms=[ALGORITHM],
                    audience=self._audience,
                    issuer=self._issuer,
                    options={"require": ["exp", "iss", "aud"]},
                )
            except pyjwt.ExpiredSignatureError as exc:
                # 过期与"签名不对"必须分开:过期就是过期,拿去逐把密钥重试白费且掩盖原因。
                raise ValueError(f"token expired: {exc}") from exc
            except pyjwt.InvalidSignatureError as exc:
                last = exc
                continue  # 可能是别的密钥签的,继续试下一把
            except pyjwt.PyJWTError as exc:
                # iss / aud / 必填 claim 不对:换密钥也救不了。
                raise ValueError(f"token invalid: {exc}") from exc
            return DSCallbackClaims(
                ds_type=str(payload.get("ds_type") or ""),
                match_id=int(payload.get("match_id") or 0),
                # Pod 就是 sub(Go: DSCallbackClaims.Pod() 返回 RegisteredClaims.Subject)。
                pod=str(payload.get("sub") or ""),
                jti=str(payload.get("jti") or ""),
                instance_uid=str(payload.get("ds_uid") or ""),
                protocol_epoch=int(payload.get("ds_epoch") or 0),
                gen=int(payload.get("ds_gen") or 0),
                exp_ms=int(float(payload.get("exp") or 0) * 1000),
                kid=str(payload.get("ds_kid") or ""),
                writer_epoch=int(payload.get("ds_writer_epoch") or 0),
            )
        raise ValueError(f"token invalid: {last}")


def _metadata_get(context, key: str) -> str:  # noqa: ANN001 —— grpc.aio.ServicerContext
    """从 gRPC metadata 取一个头(gRPC 保证 key 已小写)。"""
    try:
        md = context.invocation_metadata()
    except Exception:  # noqa: BLE001 —— 测试桩 / 无 metadata 的调用
        return ""
    if not md:
        return ""
    for item in md:
        try:
            k, v = item[0], item[1]
        except (TypeError, IndexError):
            k, v = getattr(item, "key", ""), getattr(item, "value", "")
        if str(k).lower() == key:
            return str(v)
    return ""


def _bearer_token(context) -> str:  # noqa: ANN001
    """从 authorization 头取 Bearer 令牌;无 / 非 Bearer 返回空串。"""
    raw = _metadata_get(context, AUTHORIZATION_HEADER).strip()
    if not raw:
        return ""
    prefix = "bearer "
    if len(raw) > len(prefix) and raw[: len(prefix)].lower() == prefix:
        return raw[len(prefix) :].strip()
    return ""


class DSCallbackGuard:
    """校验 DS 回调令牌 + 范围绑定。None guard 等价 mode=off(未配置服务零改动)。"""

    __slots__ = ("_verifier", "_mode")

    def __init__(self, verifier: DSCallbackVerifier | None, mode: Mode) -> None:
        if mode is not Mode.OFF and verifier is None:
            raise ValueError(f"ds_auth: mode={mode.value} requires verifier (ds_auth.secret)")
        self._verifier = verifier
        self._mode = mode

    @property
    def mode(self) -> Mode:
        return self._mode

    def check(self, context, scope: DSScope) -> int:  # noqa: ANN001, C901 —— 与 Go 同为线性判定表
        """返回 0 = 放行;非 0 = 应回给客户端的 in-band ErrCode。

        ★ 返回码而不是抛异常,是因为调用点(service 层)一律以 in-band code 回应,
          与 Go 侧 `return &Resp{Code: toProtoCode(err)}` 逐字对应。

        ⚠️ **不允许为了 "对齐 Go" 把本方法改成抛异常**。Go 那边
        `pkg/middleware/dsauth.go:161 Check(ctx, scope) error` 确实走 error 通道,
        但本仓三个调用点 —— `battle_result/service.py`、`player/service.py:133`、
        `team/service.py:718` —— 都是 `code = check(...); if code != 0: return Resp(code=...)`,
        **一个都没包在 try 里**。改成抛异常会让它逐出 handler,客户端拿到
        UNKNOWN 而不是可解析的业务码,in-band code 契约当场破掉。

        ⚠️ 全仓还有另一份 `DSCallbackGuard`:`services/guild/ds_guard.py`。
        类名、`check` 方法名、`DSScope` 参数类型名与五个字段**完全相同**,
        失败通道却**相反**(那边 enforce 档抛 `PandoraError`,调用点包在 try 里)。
        两者互换后语法完全合法、import 就能过:把本共享件塞进 guild 的
        `try/except` 调用点,那道门当场空转(无令牌东西向直连拿到权威
        guild_id),而日志照旧打 `ds_callback_auth_rejected` —— Loki 上看是拒了。
        合并两份模块可以,但**必须在同一次改动里**把 `guild/service.py:270-275`
        的 `try/except` 改写成 `code = check(...); if code != 0:` —— "删掉重复模块、
        调用点不动" 正是那个空转动作。
        """
        _, code = self.check_with_claims(context, scope)
        return code

    def check_with_claims(  # noqa: ANN001, C901
        self, context, scope: DSScope
    ) -> tuple[DSCallbackClaims | None, int]:
        """同 ``check``，并仅在真正验签且 scope 匹配时返回 claims。"""
        if self._mode is Mode.OFF:
            return None, 0
        via_gateway = _metadata_get(context, METADATA_KEY_DS_GATEWAY) != ""
        token = _bearer_token(context)

        if scope.deny_ds:
            if via_gateway or token:
                return None, self._reject(
                    errcode.ErrPermissionDeny,
                    via_gateway,
                    "call shape not allowed from ds gateway",
                )
            return None, 0

        if not token:
            if not via_gateway:
                if scope.require_token:
                    return None, self._reject(
                        errcode.ErrUnauthorized,
                        via_gateway,
                        "missing bearer token (ds-only callback rejects unauthenticated east-west call)",
                    )
                return None, 0  # 集群内东西向内部调用,不带令牌、不经网关
            return None, self._reject(
                errcode.ErrUnauthorized, via_gateway, "missing bearer token"
            )

        assert self._verifier is not None  # mode != off 时构造期已保证
        try:
            claims = self._verifier.verify(token)
        except ValueError as exc:
            return None, self._reject(
                errcode.ErrUnauthorized, via_gateway, f"token invalid: {exc}"
            )

        if scope.ds_type and claims.ds_type != scope.ds_type:
            return None, self._reject(
                errcode.ErrPermissionDeny,
                via_gateway,
                f"ds_type mismatch: token={claims.ds_type} want={scope.ds_type}",
            )
        if scope.match_id and claims.match_id != scope.match_id:
            return None, self._reject(
                errcode.ErrPermissionDeny,
                via_gateway,
                f"match_id mismatch: token={claims.match_id} req={scope.match_id}",
            )
        if scope.pod and claims.pod != scope.pod:
            return None, self._reject(
                errcode.ErrPermissionDeny,
                via_gateway,
                f"pod mismatch: token={claims.pod!r} req={scope.pod!r}",
            )
        return claims, 0

    def check_credential(  # noqa: ANN001
        self, context, scope: DSScope
    ) -> tuple[DSCallbackClaims | None, VerifiedCredential | None, int]:
        """校验回调并仅从完整 Model B claims 生成 active credential。"""
        claims, code = self.check_with_claims(context, scope)
        if code != 0 or claims is None:
            return claims, None, code
        if (
            claims.ds_type not in ("hub", "battle")
            or not claims.pod
            or not claims.instance_uid
            or claims.protocol_epoch <= 0
            or claims.gen <= 0
            or not claims.jti
            or claims.exp_ms <= 0
            or not claims.kid
            or claims.writer_epoch <= 0
            or (claims.ds_type == "battle" and claims.match_id <= 0)
            or (claims.ds_type == "hub" and claims.match_id != 0)
        ):
            return claims, None, 0
        token = _bearer_token(context)
        return (
            claims,
            VerifiedCredential(
                ds_type=claims.ds_type,
                match_id=claims.match_id,
                pod=claims.pod,
                instance_uid=claims.instance_uid,
                protocol_epoch=claims.protocol_epoch,
                gen=claims.gen,
                jti=claims.jti,
                exp_ms=claims.exp_ms,
                kid=claims.kid,
                token_sha256=hashlib.sha256(token.encode("utf-8")).hexdigest(),
                writer_epoch=claims.writer_epoch,
            ),
            0,
        )

    def _reject(self, code: int, via_gateway: bool, reason: str) -> int:
        """enforce 返回错误码;permissive 记 warn 放行(返回 0)。

        ★ enforce 拒绝同样必须落盘(§11.3):这些码非 IsServerFault,access log 只记
          DEBUG;各服务调用点并不逐点补日志,这里是所有 DS 回调鉴权拒绝的唯一必经收口
          —— DS 凭据轮换断档时,一台 DS 上全部玩家的回调会成批被拒,没有这条就只能
          靠客户端表象反推。拒绝是低频异常路径,不构成噪音。
        """
        if self._mode is Mode.ENFORCE:
            plog.get().warning(
                "ds_callback_auth_rejected", reason=reason, via_gateway=via_gateway, code=code
            )
            return code
        plog.get().warning(
            "ds_callback_auth_permissive_reject", reason=reason, via_gateway=via_gateway
        )
        return 0


def guard_from_conf(cfg) -> DSCallbackGuard | None:  # noqa: ANN001 —— config 里的 DSAuthConf 模型
    """按 ds_auth 配置构造校验守卫 —— 对应 Go 的 NewDSCallbackGuardFromConf。

      - mode=off(含空)→ None,调用点当作放行
      - mode=permissive/enforce 但 secret 未配 → **抛错**(配置矛盾,启动即 fatal
        而非静默不校验:那正是这道门要防的东西)
    """
    mode = parse_mode(cfg.mode)
    if mode is Mode.OFF:
        return None
    if not cfg.secret:
        raise ValueError(f"ds_auth: mode={mode.value} requires ds_auth.secret")
    verifier = DSCallbackVerifier(
        issuer=cfg.issuer,
        audience=cfg.audience,
        secret=cfg.secret,
        additional_secrets=list(cfg.additional_secrets),
    )
    return DSCallbackGuard(verifier, mode)


def signer_from_conf(cfg) -> pauth.DSCallbackSigner | None:  # noqa: ANN001 —— config 里的 DSAuthConf 模型
    """按 ds_auth 配置构造 DS 回调令牌**签发器** —— 对应 Go 的 `NewDSCallbackSignerFromConf`。

    secret 未配 → `None`,表示本服务不签发(调用方跳过注入)。与 `guard_from_conf`
    的 `mode=off → None` 对称,但判据不同:

      - 签发看的是 **secret**(签不签跟 mode 无关:mode 管的是本服务**验**不验;
        ds_allocator / hub_allocator 即使 mode=off 也照样要给 DS 签令牌下发,
        否则灰度期把 mode 切成 enforce 的那一刻,全部 DS 手上没有令牌 → 成批被拒);
      - 校验看的是 **mode**。

    ★ **不传 additional_secrets** —— 与 Go 一致。备用密钥只用于校验(轮换共存窗口),
      签发始终用主密钥;签发侧接受旧密钥会让轮换的第二段永远走不完。
    """
    if not cfg.secret:
        return None
    return pauth.DSCallbackSigner(
        pauth.SignerConfig(
            secret=cfg.secret.encode("utf-8"),
            issuer=cfg.issuer,
            audience=cfg.audience,
            # DS 回调面用不到账号态,但 SignerConfig.validate 要求它非空且与 audience 不同
            # (Go 那边由 Config.Defaults() 自动填同一个值)。
            account_audience=pauth.DEFAULT_ACCOUNT_AUDIENCE,
        )
    )
