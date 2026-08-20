"""JWT 签发 / 验签 —— 对应 Go 侧 pkg/auth/jwt.go。

★ 两条移植时最容易踩的坑,都是"写错了不报错、只在特定路径上 401 / 越权":

════ ① 账号态与玩家态的 audience 必须**严格分离** ════

    两步登录:
        第一步 → 账号态 token(sub=account_id, aud=<account_audience>)
                 只解锁 ListAccountRoles / EnterRole
        第二步 → 玩家态 token(sub=player_id,  aud=<audience>)
                 才有玩家面能力

    共用同一个 aud 的后果:账号态 token 会被当成玩家态用,而 sub 里装的是
    **account_id**。下游把它当 player_id 去查数据 —— 越权,且不报错
    (account_id 和 player_id 都是 snowflake,长得一模一样)。

════ ② 经 Envoy 校验的 token **绝不能设 kid 头** ════

    envoy.yaml 的 local_jwks 里那把 key 的 kid 是 `"pandora-dev"` ——
    **一个固定字面量,不是密钥指纹**。一旦签发时写进真实指纹,
    Envoy 会按 kid 精确找 key、找不到就直接拒 → 该入口**全线 401**。

    所以:
        经 Envoy 的(SessionToken / AccountToken)  → **不设 kid**
        只在 Go 侧验签的(DS 回调 / Hub 凭据)      → 设 kid(便于轮换)

    这个区别没有任何运行期信号会提醒你 —— 设错了就是登录全挂。
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import uuid as _uuid

import jwt as pyjwt

from pandorapy import errcode

# 与 Go 侧一致的签名算法。
ALGORITHM = "HS256"

# HS256 密钥最小长度(RFC 7518 §3.2)。与 Go 侧 auth.Config.Validate 同值。
MIN_SECRET_BYTES = 32

# 本二进制实现的 Model B writer capability。对应 Go 侧 pkg/auth/jwt.go 的
# DSAuthWriterEpochV2(uint32 = 2)。
#
# ★ 它不是"版本号下限"而是**恰等于**的门:所有校验点写的都是 `!= V2 → 拒`。
# 各处若各自手抄这个 2,某处抄错(或松成 >=)不会有任何运行期信号 ——
# 低代际的 legacy writer 被静默放行,Model B 的代际隔离就整个被拆掉。
# proto 里 writer_epoch 是裸 uint32、没有 enum,生成物里没有对应符号,
# 只能落字面量;tests/test_hub_capacity.py 有一条与 Go 源码对拍的钉子测试。
DS_AUTH_WRITER_EPOCH_V2 = 2


class TokenError(errcode.PandoraError):
    """签发 / 验签失败。

    ★ 它是 `PandoraError` 的子类,不是裸 `RuntimeError` —— 这样 `errcode.as_code`
    才能拿到真正的业务码。原先塌成 RuntimeError 时,service 层拿到的一律是
    ErrUnknown,客户端只知道"出错了",分不出"该重登"还是"该停手"。

    默认码 ErrUnauthorized;两个子类各自带更精确的码。
    """

    def __init__(self, msg: str, code: int = errcode.ErrUnauthorized) -> None:
        super().__init__(code, msg)


class TokenExpiredError(TokenError):
    """token 已过期 —— 对应 Go 的 ErrLoginTicketExpired(1010)。

    ★ 必须与"非法"分开。两者的客户端处置完全相反:
        过期 → 静默重新登录 / 刷新,属正常生命周期
        非法 → 凭据被篡改或密钥配错,要提示并**停止重试**
    合成一个之后,客户端只能一律重试 —— 密钥配错那天会变成全量客户端重试风暴。
    """

    def __init__(self, msg: str) -> None:
        super().__init__(msg, errcode.ErrLoginTicketExpired)


class TokenInvalidError(TokenError):
    """token 非法(签名 / issuer / audience / 必填 claim 不对)。对应 ErrLoginTicketInvalid(1011)。"""

    def __init__(self, msg: str) -> None:
        super().__init__(msg, errcode.ErrLoginTicketInvalid)


@dataclasses.dataclass(slots=True)
class SignerConfig:
    secret: bytes
    issuer: str
    # 玩家态受众。必须与 envoy.yaml 一致(否则 Envoy 拒)。
    audience: str
    # ★ 账号态受众。必须与 audience **不同** —— 见模块头 ①。
    account_audience: str
    # 与 Go 的 Config.Defaults() 同值(24h)。此前写的是 2h ——
    # 默认值不一致的后果是"同一份没写 session_ttl 的 yaml,两栈签出来的 token
    # 寿命差 12 倍",而灰度期两栈同时在线,玩家会遇到"有时几小时就要重登"。
    session_ttl: _dt.timedelta = _dt.timedelta(hours=24)
    account_ttl: _dt.timedelta = _dt.timedelta(minutes=10)
    # 密钥指纹。只给**不经 Envoy**的那几种 token 当 kid 用。
    key_fingerprint: str = ""
    # ★ 仅用于**校验**的额外可接受密钥(不用于签发)—— 不停服密钥轮换的载体。
    #
    # 对应 Go 的 auth.Config.AdditionalSecrets。三段式轮换:
    #   ① 各服务先把新密钥 K2 加进 additional(仍用 K1 签)→ 全量接受 K1/K2
    #   ② 主密钥翻成 K2、additional 放 K1 → 用 K2 签,仍接受 K1
    #   ③ 清空 additional 只留 K2
    # 三步都是滚动更新,共存期两把密钥都被接受,**无 401 断档**(§9 不变量 16)。
    #
    # 缺了它,换密钥只能是"同一时刻所有副本一起换" —— 而滚动更新根本做不到同一时刻,
    # 于是共存窗口里旧副本签的 token 被新副本拒:玩家批量掉线,且只在换钥那天发生。
    additional_secrets: tuple[bytes, ...] = ()

    def validate(self) -> None:
        """启动期 fail-fast。

        ★ 两个受众相同时**必须拒绝启动** —— 这是越权的直接入口,
        而它在运行期没有任何信号(两种 token 都能通过校验)。
        """
        # ★ HS256 的密钥必须 ≥ 32 字节(RFC 7518 §3.2)。
        # 与 Go 侧 auth.Config.Validate 同一道闸 —— 我第一版**漏了这条**,
        # 是 PyJWT 的 InsecureKeyLengthWarning 把它逼出来的。
        # 短密钥不会让任何东西报错,只是让签名可被暴力破解:
        # 伪造一个 sub=任意 player_id 的 token 就能冒充任何玩家。
        if len(self.secret) < MIN_SECRET_BYTES:
            raise TokenError(
                f"auth: secret 太短(得到 {len(self.secret)} 字节,"
                f"HS256 需要 >= {MIN_SECRET_BYTES} 字节)"
            )
        if not self.issuer:
            raise TokenError("auth: issuer 不能为空")
        if not self.audience or not self.account_audience:
            raise TokenError("auth: audience / account_audience 都必须配置")
        # ★ JWT 的 NumericDate 以**秒**为粒度:TTL < 1s 会在签发时就被截断成
        # "已过期",于是签出来的 token 生下来就是坏的 —— 而这在配置层没有任何信号,
        # 表现是"登录成功但立刻 401"。与 Go 的 auth.Config.Validate 同一道闸。
        one_second = _dt.timedelta(seconds=1)
        if self.session_ttl < one_second:
            raise TokenError(f"auth: session_ttl 必须 >= 1s(得到 {self.session_ttl})")
        if self.account_ttl < one_second:
            raise TokenError(f"auth: account_ttl 必须 >= 1s(得到 {self.account_ttl})")

        # ★ 备用密钥同样要过长度闸,而且不能与主密钥 / 彼此重复 ——
        # 重复不是"多写一遍没关系",它说明轮换配错了(以为在轮换,其实两边同一把),
        # 于是轮换完成后旧密钥并没有真正退役。
        for i, extra in enumerate(self.additional_secrets):
            if len(extra) < MIN_SECRET_BYTES:
                raise TokenError(
                    f"auth: additional_secrets[{i}] 太短(得到 {len(extra)} 字节,"
                    f"HS256 需要 >= {MIN_SECRET_BYTES} 字节)"
                )
            if extra == self.secret:
                raise TokenError(
                    f"auth: additional_secrets[{i}] 与主密钥相同(轮换配置错误)"
                )
            for j in range(i):
                if extra == self.additional_secrets[j]:
                    raise TokenError(
                        f"auth: additional_secrets[{i}] 与 additional_secrets[{j}] 重复"
                    )

        if self.audience == self.account_audience:
            raise TokenError(
                f"auth: account_audience 不能与 audience 相同({self.audience!r}) —— "
                f"账号态 token 会被当成玩家态使用,而 sub 里装的是 account_id(越权)"
            )


class Signer:
    """JWT 签发器。"""

    __slots__ = ("_cfg", "_now")

    def __init__(self, cfg: SignerConfig, now_fn=None) -> None:
        cfg.validate()
        self._cfg = cfg
        self._now = now_fn or (lambda: _dt.datetime.now(_dt.UTC))

    # ── 经 Envoy 校验的(不设 kid)────────────────────────────────────────

    def sign_session(self, player_id: int, jti: str = "") -> tuple[str, int]:
        """签发**玩家态** token。sub = player_id,aud = audience。

        ★ 不设 kid —— 见模块头 ②。
        """
        if player_id == 0:
            raise TokenError("auth.sign_session: player_id 必须 > 0")
        return self._sign(
            subject=str(player_id),
            audience=self._cfg.audience,
            ttl=self._cfg.session_ttl,
            jti=jti or str(_uuid.uuid4()),
            with_kid=False,
        )

    def sign_account(self, account_id: int, jti: str = "") -> tuple[str, int]:
        """签发**账号态** token(两步登录第一步的产物)。

        sub = account_id,aud = **account_audience**(与玩家态严格分离)。
        它只解锁 ListAccountRoles / EnterRole,拿不到任何玩家面能力。

        ★ 同样不设 kid —— 它也经 Envoy 校验。
        """
        if account_id == 0:
            raise TokenError("auth.sign_account: account_id 必须 > 0")
        if not jti:
            # Go 侧要求调用方传 jti(uuid v4)。这里允许缺省自动铸,
            # 但**不允许空串**通过 —— 空 jti 会让吊销无从下手。
            jti = str(_uuid.uuid4())
        return self._sign(
            subject=str(account_id),
            audience=self._cfg.account_audience,
            ttl=self._cfg.account_ttl,
            jti=jti,
            with_kid=False,
        )

    # ── 只在 Go/Python 侧验签的(设 kid,便于轮换)────────────────────────

    def sign_internal(
        self, subject: str, audience: str, ttl: _dt.timedelta, jti: str = ""
    ) -> tuple[str, int]:
        """签发**内部** token(DS 回调 / Hub 凭据)。

        ★ 这一类**设 kid** —— 它们不经 Envoy,只在服务端自己验签,
        带上指纹便于密钥轮换时区分新旧。
        """
        return self._sign(
            subject=subject,
            audience=audience,
            ttl=ttl,
            jti=jti or str(_uuid.uuid4()),
            with_kid=True,
        )

    # ── 内部 ─────────────────────────────────────────────────────────────

    def _sign(
        self, *, subject: str, audience: str, ttl: _dt.timedelta, jti: str, with_kid: bool
    ) -> tuple[str, int]:
        now = self._now()
        exp = now + ttl
        claims = {
            "iss": self._cfg.issuer,
            "sub": subject,
            "aud": [audience],  # 与 Go 的 jwt.ClaimStrings 一致(数组形式)
            "iat": int(now.timestamp()),
            "exp": int(exp.timestamp()),
            "jti": jti,
        }
        headers = {}
        if with_kid and self._cfg.key_fingerprint:
            headers["kid"] = self._cfg.key_fingerprint
        token = pyjwt.encode(
            claims, self._cfg.secret, algorithm=ALGORITHM, headers=headers or None
        )
        return token, int(exp.timestamp() * 1000)

    # ── 验签 ─────────────────────────────────────────────────────────────

    def verify(self, token: str, *, expect_audience: str) -> dict:
        """验签并校验 iss / aud / exp。

        ★ `expect_audience` 是**必填**的:调用方必须显式说明"我期望这是哪一类
        token"。给个默认值就等于让账号态 token 在玩家面被接受。
        """
        # ★ 依次尝试 [主密钥, *备用密钥]。备用密钥只用于校验,不用于签发 ——
        # 这是不停服密钥轮换的全部机制(见 SignerConfig.additional_secrets)。
        keys = (self._cfg.secret, *self._cfg.additional_secrets)
        last: pyjwt.PyJWTError | None = None
        for key in keys:
            try:
                return pyjwt.decode(
                    token,
                    key,
                    algorithms=[ALGORITHM],
                    audience=expect_audience,
                    issuer=self._cfg.issuer,
                    options={"require": ["exp", "iat", "iss", "sub", "aud", "jti"]},
                )
            except pyjwt.ExpiredSignatureError as exc:
                # ★ 过期与"签名不对"必须分开:换密钥时**不能**把已过期的旧 token
                # 也拿去逐把密钥重试(白费且掩盖原因)。过期就是过期,直接抛。
                raise TokenExpiredError(f"auth.verify: {exc}") from exc
            except pyjwt.InvalidSignatureError as exc:
                last = exc
                continue  # 可能是别的密钥签的,继续试下一把
            except pyjwt.PyJWTError as exc:
                # iss / aud / 必填 claim 不对:换密钥也救不了,直接抛。
                raise TokenInvalidError(f"auth.verify: {exc}") from exc
        raise TokenInvalidError(f"auth.verify: {last}") from last
