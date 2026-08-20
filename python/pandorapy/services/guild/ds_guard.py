"""DS 回调令牌守卫 —— 对应 Go 侧 pkg/middleware/dsauth.go + pkg/auth 的验签部分。

★ 为什么这份代码在 guild 服务目录里,而不是 `pandorapy/` 公共层:

    Go 侧它是公共 middleware,被 7 个 DS 回调方法共用。Python 侧目前只有 guild 一个
    服务真的需要它(`GetPlayerGuild` 是 DS 出生编制反查,暴露在无 jwt_authn 的
    DS 面 :8444 上),而它牵扯 auth/config 两个禁改的共享文件。先落在服务内、
    等第二个服务要用时再上提 —— 不提前搭公共层(§15.3)。

★ **它与共享件 `pandorapy/dsauth.py` 同名同形但失败通道相反,不可互换**:

    两边都叫 `DSCallbackGuard`、都有 `check(context, scope)`、`DSScope` 五个字段
    逐字相同 —— 但共享件 `check` **返回错误码且从不抛**,本份 enforce 档
    **抛 `PandoraError`**。于是调用点写法也相反:共享件侧是
    `code = check(...); if code != 0: 拒`(battle_result / player / team 三处),
    本份是 `try: check(...) except: 拒`(`guild/service.py:270-275`)。
    互换后语法完全合法、import 就能过,但把共享件塞进本服务的 try 调用点
    = 这道门当场空转(无令牌东西向直连拿到权威 guild_id)。
    想收敛成一份可以,但**删模块与改写调用点必须在同一次改动里**。

★ 为什么不能"Python 侧没实现就当 off 继续跑":

    guild-dev.yaml 写着 `ds_auth.mode: permissive`。当成 off 跑的话,
    **观察期本该产生的 `ds_callback_auth_permissive_reject` 告警一条都不会有** ——
    运维据此判断"DS 侧是不是已经全量带上令牌了、能不能切 enforce",
    而那个判断会基于一份恒空的日志做出。切 enforce 当天全线 401。
    enforce 档更直接:fail-closed 的门被静默降级成 fail-open。

★ 判定表(mode=enforce;permissive 把每一条「拒绝」降级成 WARN 放行):

    经 :8444 网关(带标记头) + 无 / 无效令牌        → ErrUnauthorized
    经 :8444 网关            + 令牌范围不匹配        → ErrPermissionDeny
    内部直连(无标记头)     + 无令牌                → 放行(东西向内部调用)
    内部直连(无标记头)     + 无令牌 + require_token → ErrUnauthorized
    任意来源                 + 带令牌但范围不匹配    → ErrPermissionDeny
    scope.deny_ds            + 经网关或带令牌        → ErrPermissionDeny

  guild 只用 `require_token=True` 一种 scope(反查与哪台 DS、哪一局无关,
  所以不绑 type / pod / match_id)。其余分支照样实现完整 —— 只实现"用得到的
  那一支"会让下一个接线的人以为整个 scope 语义都在,而缺的那几支恰好是放行方向。
"""

from __future__ import annotations

import dataclasses
import hashlib
from typing import Any

import jwt as pyjwt

from pandorapy import errcode
from pandorapy import log as plog

# 与 Go 侧同名常量。
# Envoy 在 :8443/:8444 入站先无条件剥离同名头(防客户端 / DS 伪造),
# 再由 :8444 路由重新写入 —— 所以它能当"来自 DS 面"的可信标记。
METADATA_KEY_DS_GATEWAY = "x-pandora-ds-gateway"
AUTHORIZATION_HEADER = "authorization"

MODE_OFF = "off"
MODE_PERMISSIVE = "permissive"
MODE_ENFORCE = "enforce"

# DS 回调令牌只允许 HS256(与 Go 的 jwt.WithValidMethods 同)。
# 不锁算法 = 接受 alg:none 或攻击者选定的算法,是 JWT 最经典的绕过。
ALGORITHM = "HS256"

DS_TYPE_HUB = "hub"
DS_TYPE_BATTLE = "battle"


def parse_mode(raw: str) -> str:
    """解析 `ds_auth.mode`。空串等价 off;非法值**报错**(main fail-fast)。

    不报错而回落 off 的话,把 `enforce` 拼成 `enfroce` 会静默关掉整道鉴权门。
    """
    text = (raw or "").strip().lower()
    if text in ("", MODE_OFF):
        return MODE_OFF
    if text in (MODE_PERMISSIVE, MODE_ENFORCE):
        return text
    raise ValueError(f"ds_auth.mode invalid: {raw!r} (want off|permissive|enforce)")


def key_fingerprint(secret: str) -> str:
    """对应 Go 的 `auth.keyFingerprint`:sha256 前 8 字节的 hex。

    值必须逐字节一致 —— 轮换期签发方按这个值写 `kid` 头,校验方按它选键;
    算法不同 = 所有带 kid 的令牌都路由不到密钥。
    """
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()[:16]


@dataclasses.dataclass(frozen=True, slots=True)
class DSScope:
    """一次回调允许的授权范围 —— 对应 Go 的 `middleware.DSScope`。"""

    # 期望的 DS 类型("hub"/"battle");空 = 不校验。
    ds_type: str = ""
    # 非 0 时要求令牌 match_id 与之一致。
    match_id: int = 0
    # 非空时要求令牌 sub(pod 名)与之一致。
    pod: str = ""
    # 「这种调用形态根本不该来自 DS」。
    deny_ds: bool = False
    # 「本回调只可能来自 DS,没有合法的东西向内部无令牌调用者」。
    # 堵住"被攻破的业务 Pod 绕过 Envoy 直连业务端口、无标记无令牌却被当内部信任"。
    require_token: bool = False


class DSCallbackVerifier:
    """DS 回调令牌验签 —— 对应 Go 的 `auth.DSCallbackVerifier`。

    刻意**不复用** `pandorapy.auth.Signer.verify`:那一条要求 `sub` 与 `jti`
    必填,而 battle 回调令牌的 `sub` 是空的(签发时还不知道 Agones 会选中哪个
    GameServer)。照搬会让 battle 令牌全部验签失败,且失败原因是"缺 sub",
    看上去像密钥问题。
    """

    __slots__ = ("_issuer", "_audience", "_keys")

    def __init__(
        self, issuer: str, audience: str, secret: str, additional_secrets: list[str] | None = None
    ) -> None:
        self._issuer = issuer
        self._audience = audience
        # 主密钥在前,额外密钥只用于**校验**不用于签发 —— 这是不停服密钥轮换的全部机制。
        self._keys: list[str] = [secret, *(additional_secrets or [])]

    def verify(self, token: str) -> dict[str, Any]:
        """验签 + iss/aud/exp + ds_type 范围。失败抛 `PandoraError(ErrUnauthorized)`。"""
        if not token:
            raise errcode.PandoraError(errcode.ErrUnauthorized, "ds callback token: empty token")

        # kid 命中就只用那把(轮换期确定性选键);否则依次尝试全部。
        kid = ""
        try:
            kid = str(pyjwt.get_unverified_header(token).get("kid") or "")
        except pyjwt.PyJWTError:
            kid = ""
        candidates = [k for k in self._keys if kid and key_fingerprint(k) == kid] or self._keys

        last: BaseException | None = None
        claims: dict[str, Any] | None = None
        for key in candidates:
            try:
                claims = pyjwt.decode(
                    token,
                    key,
                    algorithms=[ALGORITHM],
                    issuer=self._issuer,
                    audience=self._audience,
                    # ★ exp 必填:没有它,签名正确的令牌就是**永久凭证**。
                    #   Go 侧是 jwt.WithExpirationRequired(),这里对齐。
                    options={"require": ["exp"]},
                )
                break
            except pyjwt.ExpiredSignatureError as exc:
                # 过期与"签名不对"必须分开:换密钥救不了过期,继续试只是白费且掩盖原因。
                raise errcode.PandoraError(
                    errcode.ErrUnauthorized, "ds callback token: %s", str(exc)
                ) from exc
            except pyjwt.InvalidSignatureError as exc:
                last = exc
                continue  # 可能是另一把密钥签的
            except pyjwt.PyJWTError as exc:
                # iss / aud / 缺 exp:换密钥也救不了。
                raise errcode.PandoraError(
                    errcode.ErrUnauthorized, "ds callback token: %s", str(exc)
                ) from exc
        if claims is None:
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "ds callback token: %s", str(last)
            ) from last

        # 与 Go 的 VerifyDSCallback 同序同判据:ds_type 决定哪一个范围字段必填。
        ds_type = str(claims.get("ds_type") or "")
        if ds_type == DS_TYPE_BATTLE:
            if int(claims.get("match_id") or 0) == 0:
                raise errcode.PandoraError(
                    errcode.ErrUnauthorized, "battle ds callback token missing match_id"
                )
        elif ds_type == DS_TYPE_HUB:
            if not str(claims.get("sub") or ""):
                raise errcode.PandoraError(
                    errcode.ErrUnauthorized, "hub ds callback token missing pod (sub)"
                )
        else:
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "ds callback token dsType invalid: %r", ds_type
            )
        return claims


def _metadata_value(context, key: str) -> str:  # noqa: ANN001
    """从 gRPC metadata 取一个头。对应 Go 的 `transport.RequestHeader().Get`。"""
    if context is None:
        return ""
    try:
        md = context.invocation_metadata() or ()
    except Exception:  # noqa: BLE001 —— 测试桩 / 非 gRPC 调用路径
        return ""
    for entry in md:
        name = getattr(entry, "key", None)
        value = getattr(entry, "value", None)
        if name is None:
            name, value = entry  # tuple 形态
        if str(name).lower() == key:
            return str(value or "").strip()
    return ""


def _bearer_token(context) -> str:  # noqa: ANN001
    """取 `authorization: Bearer xxx`;无 / 非 Bearer 返回空串(与 Go 同)。"""
    raw = _metadata_value(context, AUTHORIZATION_HEADER)
    if not raw:
        return ""
    prefix = "bearer "
    if len(raw) > len(prefix) and raw[: len(prefix)].lower() == prefix:
        return raw[len(prefix) :].strip()
    return ""


class DSCallbackGuard:
    """校验 DS 回调令牌 + 范围绑定。`None` 守卫等价 mode=off(未配置服务零改动)。"""

    __slots__ = ("_verifier", "_mode")

    def __init__(self, verifier: DSCallbackVerifier | None, mode: str) -> None:
        if mode != MODE_OFF and verifier is None:
            raise ValueError(f"ds_auth: mode={mode} requires verifier (ds_auth.secret)")
        self._verifier = verifier
        self._mode = mode

    @property
    def mode(self) -> str:
        return self._mode

    def check(self, context, scope: DSScope) -> None:  # noqa: ANN001
        """在 DS 回调 handler 顶部调用。拒绝时抛 `PandoraError`,由 service 层译成 code。"""
        self.check_with_claims(context, scope)

    def check_with_claims(self, context, scope: DSScope) -> dict[str, Any] | None:  # noqa: ANN001
        """同 `check`,额外返回**真正验签通过且范围匹配**的 claims。

        claims 非 None 仅当:请求带令牌、验签成功、且 scope 全部匹配。
        mode=off / 无令牌放行 / permissive 降级放行时为 None ——
        调用方据此区分「鉴权过的事实」与「未验证的放行」,
        禁止把 None 当作已证明。
        """
        if self._mode == MODE_OFF:
            return None
        via_gateway = _metadata_value(context, METADATA_KEY_DS_GATEWAY) != ""
        token = _bearer_token(context)

        if scope.deny_ds:
            if via_gateway or token:
                self._reject(
                    errcode.ErrPermissionDeny, via_gateway, "call shape not allowed from ds gateway"
                )
            return None

        if not token:
            if not via_gateway:
                if scope.require_token:
                    self._reject(
                        errcode.ErrUnauthorized,
                        via_gateway,
                        "missing bearer token (ds-only callback rejects unauthenticated east-west call)",
                    )
                    return None
                # 集群内东西向内部调用(不带令牌、不经网关),不受本守卫影响。
                return None
            self._reject(errcode.ErrUnauthorized, via_gateway, "missing bearer token")
            return None

        assert self._verifier is not None  # 构造期已保证 mode!=off ⇒ verifier 非 None
        try:
            claims = self._verifier.verify(token)
        except errcode.PandoraError as exc:
            self._reject(errcode.ErrUnauthorized, via_gateway, "token invalid: %s" % exc.msg)
            return None

        if scope.ds_type and str(claims.get("ds_type") or "") != scope.ds_type:
            self._reject(
                errcode.ErrPermissionDeny,
                via_gateway,
                "ds_type mismatch: token=%s want=%s" % (claims.get("ds_type"), scope.ds_type),
            )
            return None
        if scope.match_id and int(claims.get("match_id") or 0) != scope.match_id:
            self._reject(
                errcode.ErrPermissionDeny,
                via_gateway,
                "match_id mismatch: token=%s req=%d" % (claims.get("match_id"), scope.match_id),
            )
            return None
        if scope.pod and str(claims.get("sub") or "") != scope.pod:
            self._reject(
                errcode.ErrPermissionDeny,
                via_gateway,
                "pod mismatch: token=%r req=%r" % (claims.get("sub"), scope.pod),
            )
            return None
        return claims

    def _reject(self, code: int, via_gateway: bool, reason: str) -> None:
        """enforce 抛错;permissive 记 WARN 放行(**观察期的全部价值就在这条日志上**)。

        enforce 的拒绝同样必须落盘(§11.3 R2):这些码不是 server fault,
        access log 只记 rpc_ok=DEBUG;这里是所有 DS 回调鉴权拒绝的唯一必经收口。
        DS 凭据轮换断档时,一台 DS 上全部玩家的回调会成批被拒 ——
        没有这条就只能靠客户端表象反推。

        ★ **两个档必须是两个事件名,不能合并成无条件的一条**。曾经这里先无条件
        打 `ds_callback_auth_rejected`、再判 enforce 才抛,于是 permissive 放行时也打出
        enforce 的**拒绝**事件名:Loki 上写着拒了,实际把权威 guild_id 返回给了无令牌
        的调用方,排障方向从第一步就是错的。而 `guild-dev.yaml` 实配就是 `mode: permissive`
        —— 这个档的**全部价值**就是让运维据此判断 "DS 是不是已全量带上令牌、
        能不能切 enforce";跨服务并排看还会得出反向结论(team/player/battle_result 有
        permissive_reject、guild 没有 ⇒ "guild 的 DS 都带令牌了")。

        事件名与字段集逐字对齐 Go 的 `pkg/middleware/dsauth.go::reject`(:372-384) 与共享件
        `pandorapy/dsauth.py::_reject`:`code` 只在 enforce 支有 —— 放行了就没有
        "错误码" 可言,带上反而让人以为调用方收到了该码。
        """
        if self._mode == MODE_ENFORCE:
            plog.get().warning(
                "ds_callback_auth_rejected",
                reason=reason,
                via_gateway=via_gateway,
                code=int(code),
            )
            raise errcode.PandoraError(code, "ds callback auth: %s", reason)
        plog.get().warning(
            "ds_callback_auth_permissive_reject",
            reason=reason,
            via_gateway=via_gateway,
        )


def new_from_conf(cfg) -> DSCallbackGuard | None:  # noqa: ANN001
    """按 `ds_auth` 配置构造守卫 —— 对应 Go 的 `NewDSCallbackGuardFromConf`。

      - mode=off(含空)→ None,handler 侧直接放行
      - mode=permissive/enforce 但 secret 未配 → 抛错(配置矛盾,启动即 fatal 而非静默不校验)
      - additional_secrets 里有空串 → 抛错。静默过滤会让运维以为旧密钥仍被接受、
        实则轮换断档(fail-closed)。
    """
    mode = parse_mode(cfg.mode)
    if mode == MODE_OFF:
        return None
    if not cfg.secret:
        raise ValueError(f"ds_auth: mode={mode} requires ds_auth.secret")
    for i, s in enumerate(cfg.additional_secrets):
        if not s:
            raise ValueError(
                f"ds_auth: additional_secrets[{i}] is empty "
                f"(rotation misconfig; remove the entry or fill the key)"
            )
    verifier = DSCallbackVerifier(
        issuer=cfg.issuer,
        audience=cfg.audience,
        secret=cfg.secret,
        additional_secrets=list(cfg.additional_secrets),
    )
    return DSCallbackGuard(verifier, mode)
