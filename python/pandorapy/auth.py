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
import hashlib as _hashlib
import uuid as _uuid

import jwt as pyjwt

from pandorapy import errcode

# 与 Go 侧一致的签名算法。
ALGORITHM = "HS256"

# HS256 密钥最小长度(RFC 7518 §3.2)。与 Go 侧 auth.Config.Validate 同值。
MIN_SECRET_BYTES = 32

# 账号态受众的默认值。对应 Go 侧 auth.Config.Defaults() 里的 "pandora-account"。
#
# ★ 权威副本在**本层**(= Go 的 pkg/auth):services/login/conf.py 的
#   DEFAULT_JWT_ACCOUNT_AUDIENCE 是服务层的同值副本。DS 回调面的 Signer 用不到账号态,
#   但 SignerConfig.validate 要求 account_audience 非空且与 audience 不同 ——
#   Go 那边由 Config.Defaults() 自动填上,这里给出同一个默认值,免得每个
#   DS 回调调用点各编一个字符串(编出来若恰好撞上 audience,就是启动期 fail-fast)。
DEFAULT_ACCOUNT_AUDIENCE = "pandora-account"

# DS 回调令牌(DS→后端)的 iss / aud。对应 Go 侧 pkg/auth/jwt.go 的
# DSCallbackIssuer / DSCallbackAudience。
#
# ★ 与玩家面(pandora-login → pandora-client)**严格分域**:玩家令牌拿到 DS 面用不了,
#   DS 令牌拿到玩家面也用不了。两侧签发器共用同一把密钥都不行 —— aud 校验发生在
#   签名校验的同一层,是这道分域唯一机械可拦的一环。
DS_CALLBACK_ISSUER = "pandora-ds-control"
DS_CALLBACK_AUDIENCE = "pandora-ds"

# DS 类型词表。对应 Go 侧 pkg/auth/jwt.go 的 DSTypeHub / DSTypeBattle(type DSType string)。
#
# ★ 这两个串是**跨语言签发/校验契约**:签发侧写进 ds_type claim,校验侧(dsauth.DSScope /
#   DSCallbackGuard)逐字比对。任何一侧写成 "Hub" / "battles",令牌就永远范围不匹配 ——
#   而 permissive 档下它只是 warn 放行,要到切 enforce 那天才全线拒。
DS_TYPE_HUB = "hub"
DS_TYPE_BATTLE = "battle"

# uint64 上界。Go 的 matchID / gen 是 uint64,类型系统天然挡住负数与溢出;
# Python 的 int 无界,必须在签发口显式判 —— 否则 -1 会被 JSON 编成 -1 签进令牌,
# 校验侧 int() 解出 -1,match_id 范围校验静默失配(且只在那一场对局上发生)。
UINT64_MAX = (1 << 64) - 1


def key_fingerprint(secret: bytes) -> str:
    """密钥的稳定短指纹(SHA256 前 8 字节的 hex)—— 对应 Go 的 auth.keyFingerprint。

    ★ 取前 8 字节、hex 编码,共 16 个字符。**长度和截取位置都不能改**:它同时进
      JWT 头 kid 与 ds_kid claim,Go 侧校验器按 kid 把令牌路由到对应校验密钥。
      截 16 字节的 Python 与截 8 字节的 Go 互相认不出对方的 kid,轮换期就会退化成
      "逐把密钥试"(还能过)或直接找不到 key(全线拒)。
    """
    return _hashlib.sha256(secret).hexdigest()[:16]


def _check_uint64(name: str, value: int) -> None:
    """uint64 边界闸。Go 侧由类型系统免费提供,Python 必须手动补上。

    ★ bool 是 int 的子类(True == 1),`sign_ds_callback(..., match_id=True)` 会静默签出
      match_id=1 的令牌。这类调用只会出现在参数写串的场合,放过它等于把一个错配的
      授权范围签成合法令牌。
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise TokenError(f"auth.SignDSCallback: {name} 必须是 int(得到 {type(value).__name__})")
    if value < 0 or value > UINT64_MAX:
        raise TokenError(f"auth.SignDSCallback: {name} 超出 uint64 范围(得到 {value})")


# uint32 上界。DS 实例纪元(ds_epoch)在 Go 侧是 uint32,与 match_id / gen 的 uint64 不同宽,
# 用同一个 uint64 闸放行会让一个越界的 epoch 被签进凭据,而校验侧只做相等比较不做范围检查。
UINT32_MAX = (1 << 32) - 1


def _check_uint32(name: str, value: int) -> None:
    """uint32 边界闸(同 `_check_uint64`,但宽度是 32 位)。"""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TokenError(f"auth.SignDSCallback: {name} 必须是 int(得到 {type(value).__name__})")
    if value < 0 or value > UINT32_MAX:
        raise TokenError(f"auth.SignDSCallback: {name} 超出 uint32 范围(得到 {value})")


def _check_uint_range(caller: str, name: str, value: int, bits: int) -> None:
    """任意宽度的无符号边界闸,错误前缀带调用方名(与 Go 的 `auth.SignXxx: ...` 同形)。

    与 `_check_uint32` / `_check_uint64` 的区别只在于错误信息的前缀可变 ——
    Go 侧靠类型系统免费拿到 uint32 / uint64 的宽度约束,Python 的 int 无界,
    负数或溢出值会被原样签进令牌,而验签侧只做相等比较、不做范围检查,
    于是一个越界的 epoch / gen 会静默变成"合法"凭据。

    ★ bool 是 int 的子类(`True == 1`),必须单独挡掉:参数写串时会静默签出
      值为 1 的令牌,把一个错配的授权范围签成合法凭据。
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise TokenError(f"{caller}: {name} 必须是 int(得到 {type(value).__name__})")
    if value < 0 or value > (1 << bits) - 1:
        raise TokenError(f"{caller}: {name} 超出 uint{bits} 范围(得到 {value})")


@dataclasses.dataclass(frozen=True, slots=True)
class HubCredentialResult:
    """`sign_hub_credential` 的产物 —— 对应 Go 的 `auth.HubCredentialResult`。

    除令牌串外还回显组装 `HubDSCredential` 所需的身份字段:`kid` 在**令牌头**不在
    payload、`token_sha256` 必须签完才算得出来,回显免得调用方重新解析一遍令牌。
    """

    token: str
    exp_ms: int
    #: 签发密钥指纹(= `key_fingerprint(主密钥)`)。
    kid: str
    #: 令牌串的 SHA256(hex),完整性绑定。
    token_sha256: str
    writer_epoch: int


@dataclasses.dataclass(frozen=True, slots=True)
class CredentialResult:
    """Model B 回调凭据的签发产物 —— 对应 Go 的 `auth.HubCredentialResult`。

    除令牌串外还回吐组装权威仓身份所需的三项:`kid` 在 JWT **头**里不在 payload,
    `token_sha256` 要签完才算得出,`writer_epoch` 是本二进制的 writer capability。
    让签发口一次给全,调用方就不必把刚签好的令牌再解一遍 —— 那种"解自己刚签的东西"
    的代码一旦与签发侧漂移,漂移点会落在权威仓里,而不是在签发处炸掉。
    """

    token: str
    #: 与令牌 `exp` claim(秒)换算得到的**毫秒**值,不是未截断的本地时刻。
    exp_ms: int
    kid: str
    token_sha256: str
    writer_epoch: int


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

    # ── DS 回调服务令牌(方向:DS→后端)────────────────────────────────────

    def sign_ds_callback(
        self, ds_type: str, pod: str, match_id: int, ttl: _dt.timedelta
    ) -> tuple[str, int]:
        """签发 DS 回调服务令牌 —— 对应 Go 的 `Signer.SignDSCallback`。

        方向与 DSTicket(玩家→DS 的入场票)相反:它证明回调方(Heartbeat /
        ReportResult / SetLocation / PollCommands …)确实是后端刚分配 / 发现的那个
        DS 实例,而不是集群内的伪造调用者。

        约束(与 Go 逐条同):
          - ds_type=battle:match_id 必填(授权范围 = 本场对局),pod 可空(分配时
            尚不知道 Agones 会选中哪个 GameServer)
          - ds_type=hub:pod 必填(授权范围 = 本实例),match_id 必须为 0
          - ttl 必须 > 0(battle 4h / hub 24h 由 ds_auth 配置决定,调用方传入)

        ★ 签发用的 Signer 必须以 **DS 回调专用**配置构造(iss=pandora-ds-control /
          aud=pandora-ds),不要复用玩家令牌 Signer —— 见 DSCallbackSigner。
        """
        return self.sign_ds_callback_with_gen(ds_type, pod, match_id, 0, ttl)

    def sign_ds_callback_with_gen(
        self, ds_type: str, pod: str, match_id: int, gen: int, ttl: _dt.timedelta
    ) -> tuple[str, int]:
        """同 `sign_ds_callback`,但额外把 hub 令牌代际 gen 签进 `ds_gen` claim。

        对应 Go 的 `Signer.SignDSCallbackWithGen`。

        gen=0 等价于 `sign_ds_callback`(battle 令牌 / 未启用代际门控)。gen>0 仅用于
        hub 令牌:hub_allocator 经 Redis INCR 领取严格递增的 gen 后签进来,DS 心跳原样
        回显,服务端**精确相等**比较判定是否当前代际 —— 它替代的是"按秒级 exp 分代际",
        后者在同一秒内重签会碰撞(两张不同令牌 exp 相同 → 旧令牌被误判为当前代际)。

        返回 (token, exp_ms)。exp_ms 与 Go 的 `exp.UnixMilli()` 同义:**毫秒**,
        而令牌里的 `exp` claim 是 JWT NumericDate 规定的**秒**。两个单位不同不是笔误 ——
        写反了会让调用方按 1970 年附近的时刻算续期,于是要么每次心跳都重签、要么永不重签。
        """
        # ★ 校验顺序与 Go 一致:先 ds_type 分支,再 ttl。顺序影响的是"配置写错时报哪条
        #   错误",而运维就是按这条错误去改 yaml 的。
        if ds_type == DS_TYPE_BATTLE:
            if match_id == 0:
                raise TokenError("auth.SignDSCallback: battle token requires matchID")
        elif ds_type == DS_TYPE_HUB:
            if not pod:
                raise TokenError("auth.SignDSCallback: hub token requires pod")
            if match_id != 0:
                raise TokenError("auth.SignDSCallback: hub token must not carry matchID")
        else:
            raise TokenError(f"auth.SignDSCallback: invalid dsType {ds_type!r}")
        if ttl <= _dt.timedelta(0):
            raise TokenError("auth.SignDSCallback: ttl must be > 0")
        # ★ Go 侧 matchID / gen 是 uint64,负数与溢出由类型系统挡掉;Python 的 int 无界,
        #   必须在这里显式判 —— 否则 -1 / 2**64 会被 JSON 原样签进令牌,校验侧 int()
        #   解出来照样能用,范围校验静默失配。
        _check_uint64("match_id", match_id)
        _check_uint64("gen", gen)

        now = self._now()
        exp = now + ttl
        kid = key_fingerprint(self._cfg.secret)
        # ★ 逐字段对应 Go 的 DSCallbackClaims json tag,并**照搬 omitempty**:
        #   ds_type 无 omitempty(恒在);sub / match_id / ds_gen 零值时整个 key 不出现。
        #   多写一个 "match_id": 0 不会让 hub 令牌验不过,但会让"hub 令牌不得携带
        #   match_id"这条约束在字节层失真,后续按 claim 存在性判别的代码就会分叉。
        #   注意 DS 回调令牌**没有 jti**(Go 的 RegisteredClaims.ID 未设,jti omitempty),
        #   校验侧 required claim 也只有 exp / iss / aud —— 这里补一个 jti 不会立刻出错,
        #   却会让"令牌是否带 jti"成为两栈差异。
        claims: dict[str, object] = {
            "iss": self._cfg.issuer,
            "aud": [self._cfg.audience],  # 与 Go 的 jwt.ClaimStrings 一致(数组形式)
            # JWT NumericDate 以**秒**为粒度;Go 的 jwt.NewNumericDate 按 TimePrecision
            # (默认 1s)截断,int() 对正数同样是向下截断,两栈同值。
            "iat": int(now.timestamp()),
            "exp": int(exp.timestamp()),
            "ds_type": ds_type,
            "ds_kid": kid,
        }
        if pod:
            claims["sub"] = pod
        if match_id:
            claims["match_id"] = match_id
        if gen:
            claims["ds_gen"] = gen
        # 打 kid = 主密钥指纹:令牌自描述用了哪把密钥,轮换期校验侧据此路由到对应密钥。
        # DS 回调令牌不经 Envoy jwt_authn(只由 DSCallbackGuard 校验),加 kid 头无影响 ——
        # 这与模块头 ② 说的"经 Envoy 的不设 kid"并不矛盾,正是那条规则的另一半。
        token = pyjwt.encode(
            claims, self._cfg.secret, algorithm=ALGORITHM, headers={"kid": kid}
        )
        return token, int(exp.timestamp() * 1000)

    def sign_battle_credential(
        self,
        match_id: int,
        pod: str,
        instance_uid: str,
        epoch: int,
        gen: int,
        jti: str,
        ttl: _dt.timedelta,
    ) -> CredentialResult:
        """签发 Model B **battle** 回调凭据 —— 对应 Go 的 `Signer.SignBattleCredential`。

        与 `sign_ds_callback_with_gen` 的差别不是"多签几个字段",而是**身份的粒度**:
        回调凭据的身份是 `(pod, instance_uid, instance_epoch, gen, jti, writer_epoch)`
        六元组再加 `match_id`,DS 心跳按这六项**逐字段**回显 ACK。少签任何一项,
        UE 侧 `IsComplete` 直接判不完整 → 丢掉整个 Command 与驱逐单,现场表现是
        "DS 在跑、后端指令全不生效",而两边都不报错。

        `jti` 由调用方传入(uuid v4,与其他票据签发一致);`pkg/auth` 侧不铸 uuid,
        因为凭据身份要先落进权威仓再签,铸在这里就没法保证两边同一个值。

        生产 Redis Model B 与 Windows `local-off-v1` 都走本方法,两者只在
        "是否需要 Redis pending→active ACK"上不同 —— 令牌本身必须一模一样,
        否则 local 档调出来的行为对生产不成立。
        """
        # ★ 校验顺序照抄 Go:matchID → pod → instanceUID → (epoch/gen/jti/ttl 合并一条)。
        #   顺序决定"配置写错时报哪条",而运维就是按这条错误去查的。
        if match_id == 0:
            raise TokenError("auth.SignBattleCredential: matchID must be > 0")
        if not pod:
            raise TokenError("auth.SignBattleCredential: pod must be non-empty")
        if not instance_uid:
            raise TokenError("auth.SignBattleCredential: instanceUID must be non-empty")
        if epoch == 0 or gen == 0 or not jti or ttl <= _dt.timedelta(0):
            raise TokenError("auth.SignBattleCredential: epoch/gen/jti/ttl must be non-zero")
        # Go 的 matchID/gen 是 uint64、epoch 是 uint32,越界在那边编译不出来;
        # Python 必须显式挡,否则负数 / 溢出值会被原样签进凭据身份。
        _check_uint64("match_id", match_id)
        _check_uint64("gen", gen)
        _check_uint32("instance_epoch", epoch)

        now = self._now()
        exp = now + ttl
        kid = key_fingerprint(self._cfg.secret)
        claims: dict[str, object] = {
            "iss": self._cfg.issuer,
            "sub": pod,
            "aud": [self._cfg.audience],
            "iat": int(now.timestamp()),
            # NumericDate 以**秒**为粒度;下面回传的 exp_ms 必须由这个**已截断**的
            # 秒值换算,不能用未截断的本地 exp —— 否则权威仓存的 exp 与令牌里的
            # claim 稳定差 1~999ms,严格 active-state 逐字段比较永远不等。
            "exp": int(exp.timestamp()),
            "jti": jti,
            "ds_type": DS_TYPE_BATTLE,
            "match_id": match_id,
            "ds_gen": gen,
            "ds_uid": instance_uid,
            "ds_epoch": epoch,
            "ds_writer_epoch": DS_AUTH_WRITER_EPOCH_V2,
            "ds_kid": kid,
        }
        token = pyjwt.encode(
            claims, self._cfg.secret, algorithm=ALGORITHM, headers={"kid": kid}
        )
        return CredentialResult(
            token=token,
            exp_ms=int(claims["exp"]) * 1000,  # type: ignore[arg-type]
            kid=kid,
            token_sha256=_hashlib.sha256(token.encode("utf-8")).hexdigest(),
            writer_epoch=DS_AUTH_WRITER_EPOCH_V2,
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

    # ── Model B hub 回调凭据(DS→后端,带完整实例身份)────────────────────

    def sign_hub_credential(
        self,
        pod: str,
        instance_uid: str,
        epoch: int,
        gen: int,
        jti: str,
        ttl: _dt.timedelta,
    ) -> HubCredentialResult:
        """签发 Model B 的 hub 回调凭据令牌 —— 对应 Go 的 `Signer.SignHubCredential`。

        与 `sign_ds_callback_with_gen` 相比,额外把 DS 实例身份(`instance_uid`)、
        协议纪元(`epoch`)、`jti` 签进去,使凭据身份 =
        `(instance_uid, protocol_epoch, gen, jti)` **四元组** —— 单看 gen 不安全:
        代际计数器一旦被 TTL 复位,第 1 代会与历史第 1 代撞号,而实例 UID 不会。

        生产 Redis Model B 与 Windows local-off-v1 都用本方法签发,两者只在
        "是否需要 Redis pending→active ACK"上不同(签发形态完全一致)。

        ★ 返回的 `exp_ms` 取的是**已按秒截断**的 `exp` claim 值,而不是未截断的本地
          时刻(与 Go 的 `claims.ExpiresAt.UnixMilli()` 一致)。这与
          `sign_ds_callback_with_gen` 的 `exp.UnixMilli()` **刻意不同**:hub 凭据要
          落进 Redis 授权记录并与验签后的 `claims.exp` 做**严格相等**比较,存未截断值
          会稳定差 1~999ms,于是每一次 active-state 比较都不相等 —— 表现是"凭据刚投
          递就被判 stale",而令牌本身完全合法。
        """
        # ★ 校验顺序与 Go 逐条一致:运维是按第一条报出来的错误去改配置 / 排查的。
        if not pod:
            raise TokenError("auth.SignHubCredential: pod must be non-empty")
        if not instance_uid:
            raise TokenError("auth.SignHubCredential: instanceUID must be non-empty")
        if epoch == 0:
            raise TokenError("auth.SignHubCredential: protocol epoch must be > 0")
        if gen == 0:
            raise TokenError("auth.SignHubCredential: gen must be > 0")
        if not jti:
            raise TokenError("auth.SignHubCredential: jti must be non-empty")
        if ttl <= _dt.timedelta(0):
            raise TokenError("auth.SignHubCredential: ttl must be > 0")
        # Go 的 epoch 是 uint32、gen 是 uint64,越界由类型系统挡掉;Python 的 int 无界,
        # 负数 / 溢出值会被原样签进令牌,验签侧 int() 解出来照样"合法",范围校验静默失配。
        _check_uint_range("auth.SignHubCredential", "protocol epoch", epoch, 32)
        _check_uint_range("auth.SignHubCredential", "gen", gen, 64)

        now = self._now()
        exp = now + ttl
        # NumericDate 以秒为粒度:先算出**实际会被序列化进 claim 的**秒值,
        # 再由它推导 exp_ms,保证返回值与令牌内容逐字节自洽。
        exp_sec = int(exp.timestamp())
        kid = key_fingerprint(self._cfg.secret)
        # claim 名逐字对应 Go 的 DSCallbackClaims json tag(校验侧 dsauth.DSCallbackVerifier
        # 按同一批名字解析)。match_id 在 hub 凭据上**必须缺席**(omitempty),
        # 补一个 0 会让"hub 令牌不得携带 match_id"这条约束在字节层失真。
        claims: dict[str, object] = {
            "iss": self._cfg.issuer,
            "sub": pod,
            "aud": [self._cfg.audience],
            "iat": int(now.timestamp()),
            "exp": exp_sec,
            "jti": jti,
            "ds_type": DS_TYPE_HUB,
            "ds_gen": gen,
            "ds_uid": instance_uid,
            "ds_epoch": epoch,
            "ds_writer_epoch": DS_AUTH_WRITER_EPOCH_V2,
            "ds_kid": kid,
        }
        token = pyjwt.encode(
            claims, self._cfg.secret, algorithm=ALGORITHM, headers={"kid": kid}
        )
        return HubCredentialResult(
            token=token,
            exp_ms=exp_sec * 1000,
            kid=kid,
            token_sha256=_hashlib.sha256(token.encode("utf-8")).hexdigest(),
            writer_epoch=DS_AUTH_WRITER_EPOCH_V2,
        )


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


class DSCallbackSigner:
    """只签 DS→后端回调令牌 —— 对应 Go 的 `auth.DSCallbackSigner`(pkg/auth/domains.go)。

    ★ 存在的理由是**收窄方法集**,不是加一层壳。裸 `Signer` 同时能签 session / account /
      DS 回调,"拿玩家面密钥签 DS 回调令牌"只能靠约定防;域类型把这类串域错误从运行期
      约定升级成"构造时就过不去":iss / aud 不是 DS 回调面的那一对,直接拒绝构造。

    ★ 与 Go 一样**不接 additional_secrets**:备用密钥只用于校验、绝不用于签发。
      签发侧一旦能用旧密钥签,三段式轮换的第二段(主密钥已翻新、旧密钥仅待退役)就没有
      终点 —— 旧密钥会被无限续命,而运维以为它早已退役。
    """

    __slots__ = ("_signer",)

    def __init__(self, cfg: SignerConfig, now_fn=None) -> None:
        if cfg.issuer != DS_CALLBACK_ISSUER or cfg.audience != DS_CALLBACK_AUDIENCE:
            raise TokenError(
                f"auth: DS callback signer requires issuer={DS_CALLBACK_ISSUER!r} "
                f"audience={DS_CALLBACK_AUDIENCE!r}"
            )
        self._signer = Signer(cfg, now_fn)

    def sign_ds_callback(
        self, ds_type: str, pod: str, match_id: int, ttl: _dt.timedelta
    ) -> tuple[str, int]:
        """同 `Signer.sign_ds_callback`。"""
        return self._signer.sign_ds_callback(ds_type, pod, match_id, ttl)

    def sign_ds_callback_with_gen(
        self, ds_type: str, pod: str, match_id: int, gen: int, ttl: _dt.timedelta
    ) -> tuple[str, int]:
        """同 `Signer.sign_ds_callback_with_gen`。"""
        return self._signer.sign_ds_callback_with_gen(ds_type, pod, match_id, gen, ttl)

    def sign_hub_credential(
        self,
        pod: str,
        instance_uid: str,
        epoch: int,
        gen: int,
        jti: str,
        ttl: _dt.timedelta,
    ) -> HubCredentialResult:
        """同 `Signer.sign_hub_credential`。"""
        return self._signer.sign_hub_credential(pod, instance_uid, epoch, gen, jti, ttl)

    def sign_battle_credential(
        self,
        match_id: int,
        pod: str,
        instance_uid: str,
        epoch: int,
        gen: int,
        jti: str,
        ttl: _dt.timedelta,
    ) -> CredentialResult:
        """同 `Signer.sign_battle_credential`。"""
        return self._signer.sign_battle_credential(
            match_id, pod, instance_uid, epoch, gen, jti, ttl
        )
