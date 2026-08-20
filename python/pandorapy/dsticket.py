"""DSTicket v2(方案 B):RS256 非对称签发 / 验签 + 严格 JWKS
—— 对应 Go 侧 `pkg/auth/dsticket.go` + `dsticket_conf.go` + `ds_local_profile.go`。

════ 这是**跨栈互操作件**,不是"又一个 JWT 工具" ════

    Python 签出的票必须能被 Go 的 `auth.DSTicketVerifier` **和** UE DS 侧的
    `FPandoraTicketVerifier` 验过,反之亦然。三个实现读的是同一份 JWKS、同一份
    claim 契约。任何一处偏差(claim 名少个下划线、iat 写成毫秒、aud 写成裸串而非
    数组、base64url 带了 padding)都**不会报错**,只会表现为:

        "票签出来了、DS 一律拒" → 玩家全线进不去场景,而两栈日志全绿。

    所以本文件的每个字面量都是契约的一部分,不是风格问题。

════ 契约要点(逐条对应 CLAUDE.md §9 不变量 3)════

  ① **TTL 双向强制**:默认 120s、硬上限 180s。签发侧超限**启动即拒**,验签侧
     `exp-iat > 180s` **一律拒票**。B1 是"纯本地验票"(DS 不回后端查吊销),
     吊销时延上界 = TTL + DS 侧 leeway;放长 TTL 等于把吊销手段直接删掉,
     而配置层没有任何信号。`ds_ticket_ttl: 5m` 那种值只属 legacy HS256 路径,
     **不要**照着算 v2 的安全窗口。
  ② **jti 必填**:B1 下它是唯一的吊销抓手(在线核销点按 jti 单次核销)。
     省掉它 = 一张票在 TTL 内可无限次重放。
  ③ **实例绑定一个都不能少**:
       ds_pod / ds_uid / ds_instance_epoch  → §9.22 exact 实例绑定(同名 Pod 重建后旧票自动失效)
       hub_assignment_id                    → 玩家归属版本(Transfer / Release 后旧票失效,是容量与强制迁移的执行手段)
       release_track                        → §9.21 灰度轨道粘滞(stable 票不得在 canary Pod 兑换)
       sjti                                 → §9.23 会话 fencing(旧 session 签的票在兑换点被拒)
       source_match_id                      → Battle→Hub 回流 fence(消除终局 TTL 残留导致的 4007)
  ④ **信任域隔离**:iss=pandora-dsticket、aud=pandora-game-ds、alg 固定 RS256。
     与 SessionToken(HS256 / pandora-client)、DS 回调令牌(HS256 / pandora-ds)
     交叉使用必然验签失败 —— 这是有意的。

════ 三个最容易写错、且写错了不报错的地方 ════

  ★ **时间单位**:JWT 的 iat / exp 是**秒**(NumericDate),而各处 API 回给调用方的
    `expires_at_ms` 是**毫秒**。Go 侧是 `jwt.NewNumericDate(t)`(秒)+ `exp.UnixMilli()`
    (毫秒)两条路。弄反了两栈就永远互不认票,而单栈自测全绿。

  ★ **base64url 必须无 padding**(RFC 7515 §2):JWT 三段、JWK 的 n/e、RFC 7638 指纹
    **全部**是 `base64url(无 '=')`。Python 的 `base64.urlsafe_b64decode` 在缺 padding 时
    直接抛 `binascii.Error` —— 这是移植时最常踩的坑,所以本模块统一走
    `_b64url_decode`(解码补 padding,编码去 padding)。

  ★ **`omitempty` 语义**:Go 结构体上带 `omitempty` 的 claim 在零值时**不出现在 JSON 里**。
    Python 用 dict 手工组装时若无脑全填,签出的票会比 Go 版多出 `"match_id":0` 这类键。
    对 Go/UE 的解析器无害(零值),但会让"两栈签出的票逐字节对拍"这道最有效的
    回归闸失效。本模块严格复刻 omitempty(见 `_sign` 里的 `_put_omitempty`)。

════ 本模块**不打日志** ════

    Go 侧 `pkg/auth/dsticket.go` 全文没有一行日志(它是纯函数库,调用方负责记录)。
    这里若自作主张加 event,灰度期两栈日志就对不上了 —— 而"日志 event 名与字段名
    与 Go 逐字一致"是硬性要求。所以:一行不加。
"""

from __future__ import annotations

import base64
import binascii
import dataclasses
import datetime as _dt
import hashlib
import hmac
import json
import re
import typing

import jwt as pyjwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa as _rsa

from pandorapy import errcode
from pandorapy import godur
from pandorapy import releasetrack

# ── 信任域常量(与 Go 逐字一致;改任何一个都等于换了一个信任域)────────────────

#: `iss`。Go: `auth.DSTicketIssuer`。
DS_TICKET_ISSUER = "pandora-dsticket"
#: `aud`。Go: `auth.DSTicketAudience`。
DS_TICKET_AUDIENCE = "pandora-game-ds"

#: `dst_ver` 当前值。验签侧**精确匹配**(不是 ">= 2"),旧 v1(无 dst_ver)一律拒。
DS_TICKET_VERSION_2 = 2

#: 固定签名算法。RS256 之外(含 `none` / HS256)在验签侧一律拒 —— 见 `DSTicketVerifier.verify`。
DS_TICKET_ALGORITHM = "RS256"

#: 票据默认 / 上限有效期。Go: `DSTicketDefaultTTL` / `DSTicketMaxTTL`。
DS_TICKET_DEFAULT_TTL = _dt.timedelta(minutes=2)
DS_TICKET_MAX_TTL = _dt.timedelta(minutes=3)

#: RSA 密钥最小位数(NIST SP 800-57 当前基线)。Go: `DSTicketMinRSABits`。
DS_TICKET_MIN_RSA_BITS = 2048

#: JWKS 文件解析上限(fail-closed,防投递事故)。Go: `dsTicketJWKSMaxBytes` / `dsTicketJWKSMaxKeys`。
_DS_TICKET_JWKS_MAX_BYTES = 64 * 1024
_DS_TICKET_JWKS_MAX_KEYS = 8

#: DS 类型。Go 侧是 `auth.DSType`(`pkg/auth/jwt.go:40`)的两个取值;proto 里 ds_type
#: 是裸 string(没有 enum),所以两栈都只能落字面量 —— 本模块是 Python 公共层的
#: 唯一出处,服务层请 `from pandorapy.dsticket import DS_TYPE_HUB` 而不是各自再抄一遍。
DS_TYPE_HUB = "hub"
DS_TYPE_BATTLE = "battle"

#: 灰度轨道取值。**不在本文件重复定义** —— 它已经是公共层常量,而且
#: `releasetrack.select()` 与 Go 逐位对拍过;两处各写一份字面量迟早漂移。
RELEASE_TRACK_STABLE = releasetrack.STABLE
RELEASE_TRACK_CANARY = releasetrack.CANARY

# ── 数值边界 ─────────────────────────────────────────────────────────────────
#
# ★ Python 的 int 是任意精度、**不会回绕**;Go 的 uint32 / uint64 会。
#   于是「Python 侧塞进一个 2**32 的 ds_instance_epoch」这种事,在 Python 侧毫无感觉,
#   到了 Go/UE 侧要么解析失败要么被截断成另一个实例号(→ 票被兑换到错误的 Pod)。
#   所有跨栈整型字段必须在**进出两侧**都显式判边界。
_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1

#: NumericDate(iat/exp/nbf)的可接受范围。JSON 数字过了 2**53 就不再能被双精度
#: 精确表示,两栈会读出不同的秒数;超出即判构造攻击 / 坏数据,不做"尽力而为"解释。
_MAX_NUMERIC_DATE = 1 << 53

#: `sub` 必须是**纯十进制**串(Go 的 `strconv.ParseUint` 不接受符号、空白、下划线)。
#: 用 `\A...\Z` 而不是 `^...$`:后者在多行串上会匹配任意一行,
#: `"1\n<垃圾>"` 会被判成合法 player_id。
_DECIMAL_RE = re.compile(r"\A[0-9]+\Z")

#: base64url 字母表(无 padding)。同样锚 `\A...\Z`。
_B64URL_RE = re.compile(r"\A[A-Za-z0-9_-]*\Z")

_UNIX_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_dt.UTC)

# JWK 的私钥成员。只要**出现**(哪怕是 null / 空串)就整份 keyset 拒收:
# 它意味着有人把私钥投递进了只该拿公钥的 DS Fleet,这是事故不是配置问题。
_JWK_PRIVATE_MEMBERS = ("d", "p", "q", "dp", "dq", "qi", "oth", "k")
# JWK / JWKS 允许出现的字段(对应 Go 的 `dec.DisallowUnknownFields()`,递归生效)。
_JWK_ALLOWED_MEMBERS = frozenset(("kty", "use", "alg", "kid", "n", "e", *_JWK_PRIVATE_MEMBERS))
_JWKS_ALLOWED_MEMBERS = frozenset(("revision", "active_kid", "keys"))


# ── 异常 ─────────────────────────────────────────────────────────────────────


class DSTicketConfigError(errcode.PandoraError):
    """密钥 / TTL / JWKS / 目标绑定等**配置与入参**错误。

    对应 Go 侧 `NewDSTicketSigner` / `ParseDSTicketJWKS` 返回的裸 `error`
    (那些路径都是启动期或签发期的 fail-fast,不是给客户端看的业务码)。
    """

    def __init__(self, msg: str, *args: object) -> None:
        super().__init__(errcode.ErrInvalidArg, msg, *args)


class DSTicketExpiredError(errcode.PandoraError):
    """票据已过期 —— 对应 Go 的 `errcode.ErrLoginTicketExpired`(1010)。

    ★ 必须与"非法"分开:过期是正常生命周期(客户端重新走进场链再取一张票),
    非法是密钥 / keyset 配错或票被伪造(必须停手并告警)。合成一个之后,
    换钥那天会变成全量客户端重试风暴。
    """

    def __init__(self, msg: str, *args: object) -> None:
        super().__init__(errcode.ErrLoginTicketExpired, msg, *args)


class DSTicketInvalidError(errcode.PandoraError):
    """票据非法(签名 / kid / iss / aud / 结构 / 绑定不对)—— 对应 `ErrLoginTicketInvalid`(1011)。"""

    def __init__(self, msg: str, *args: object) -> None:
        super().__init__(errcode.ErrLoginTicketInvalid, msg, *args)


# ── base64url(RFC 7515 §2:**无 padding**)───────────────────────────────────


def _b64url_encode(raw: bytes) -> str:
    """编码成 base64url 且**去掉 '='**。

    带 padding 的 JWT 段 / JWK 成员会被 Go 的 `base64.RawURLEncoding` 直接判非法
    (它把 '=' 当非法字符),表现是"Python 签的票 Go 一个都验不过"。
    """
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(text: str, *, what: str) -> bytes:
    """解码 base64url,**自动补 padding**。

    Python 的 `urlsafe_b64decode` 在长度不是 4 的倍数时抛 `binascii.Error`,
    而 JWT / JWK 里的串**恒无 padding** —— 不补就是"所有票都解不开"。

    仍然 fail-closed:非字母表字符(含 '=' 与标准 base64 的 '+' '/')、
    以及 len%4==1 这种不可能由任何编码器产出的长度,一律拒。
    """
    if not _B64URL_RE.fullmatch(text):
        raise DSTicketConfigError("%s: not base64url (RFC 4648 §5, no padding)", what)
    remainder = len(text) % 4
    if remainder == 1:
        # 4n+1 不是任何 base64 输出的合法长度;补 padding 也解不出,提前给准话。
        raise DSTicketConfigError("%s: invalid base64url length", what)
    padded = text + "=" * ((4 - remainder) % 4)
    try:
        return base64.urlsafe_b64decode(padded.encode("ascii"))
    except (binascii.Error, ValueError) as exc:
        raise DSTicketConfigError("%s decode: %s", what, exc) from exc


# ── 时间(秒 / 毫秒两条路,别混)────────────────────────────────────────────


def _unix_seconds(moment: _dt.datetime) -> int:
    """Go `jwt.NewNumericDate(t)` 的落盘形态:**截断到秒**的 Unix 时间。

    golang-jwt v5 的 `TimePrecision` 默认 `time.Second`,序列化前先
    `Truncate(time.Second)` 再取 `.Unix()` —— 所以是**下取整**,不是四舍五入。
    这里用整数除法(而不是 `int(ts.timestamp())`)以免浮点在边界上抖一秒。
    """
    return (moment - _UNIX_EPOCH) // _dt.timedelta(seconds=1)


def _unix_milli(moment: _dt.datetime) -> int:
    """Go `t.UnixMilli()`:**毫秒**。只用于回给调用方的 `expires_at_ms`,不进 JWT。"""
    return (moment - _UNIX_EPOCH) // _dt.timedelta(milliseconds=1)


# ── 整型边界 ─────────────────────────────────────────────────────────────────


def _require_uint(value: int, *, bits: int, what: str) -> int:
    """校验一个跨栈无符号整型字段。越界 / 负数 / bool 一律拒(见 `_UINT32_MAX` 注释)。"""
    # bool 是 int 的子类:`True` 会被静默当成 1 塞进 match_id。
    if isinstance(value, bool) or not isinstance(value, int):
        raise DSTicketConfigError("%s must be an integer (got %r)", what, value)
    if value < 0 or value > (1 << bits) - 1:
        raise DSTicketConfigError("%s out of uint%d range: %d", what, bits, value)
    return value


# ── claims ───────────────────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True, slots=True)
class DSTicketClaimsV2:
    """dst_ver=2 票据载荷 —— 对应 Go 的 `auth.DSTicketClaimsV2`。

    字段名是 Python 侧的(蛇形),但**序列化用的 claim key 全部写死在 `_CLAIM_*`
    常量与 `_sign` / `verify` 里**,与 Go 的 json tag 逐字节一致。
    """

    # RegisteredClaims(Go 侧是内嵌结构,JSON 上是平铺的)。
    issuer: str = ""
    subject: str = ""
    audience: tuple[str, ...] = ()
    issued_at: float | None = None  # `iat`,**秒**
    expires_at: float | None = None  # `exp`,**秒**
    not_before: float | None = None  # `nbf`,**秒**;本实现从不签发,只在验签侧校验
    jti: str = ""

    # v2 私有 claim。
    dst_ver: int = 0
    ds_type: str = ""
    ds_pod_name: str = ""
    ds_instance_uid: str = ""
    ds_instance_epoch: int = 0
    release_track: str = ""
    region_id: int = 0
    cell_id: int = 0
    role_id: int = 0
    match_id: int = 0
    allocation_id: str = ""
    hub_assignment_id: str = ""
    #: Battle→Hub 回流 fence。hub 票可带(玩家从终局对局回大厅时由签票权威盖上,
    #: Hub DS 用它调 `SetLocation(HUB, fence=source_match_id)` 过 locator 的
    #: BATTLE→HUB guard);battle 票必须 0(battle 绑定走 match_id)。
    source_match_id: int = 0
    #: 签发本票的**请求方登录会话 jti**(§9.23 会话 fencing)。
    #: 空值 = 兼容窗(matchmaker READY 批签 / allocator Transfer 重签 / 滚动升级旧票),
    #: **不得**把空值当"已验"。
    sess_jti: str = ""

    def player_id(self) -> int:
        """把 `sub` 解成 uint64。失败返回 0 —— 与 Go 的 `PlayerID()` 同语义(含失败取零)。

        ★ 不能用 `str.isdigit()`:它对 '١٢٣'(Arabic-Indic)这类 Unicode 数字返回 True,
        而 Go 的 `strconv.ParseUint` 只认 ASCII `[0-9]`。两栈对同一个 sub 解出不同的
        player_id = 把票兑换到别人头上。
        """
        if not self.subject:
            return 0
        if not _DECIMAL_RE.fullmatch(self.subject):
            return 0
        value = int(self.subject)
        if value > _UINT64_MAX:
            # Go 的 ParseUint(64 位)在这里返回 err → PlayerID() 取 0。
            return 0
        return value


@dataclasses.dataclass(frozen=True, slots=True)
class DSTicketTarget:
    """签票时的目标 DS 实例身份 —— 对应 Go 的 `auth.DSTicketTarget`。

    全部来自**受信控制面的权威快照**(hub_allocator 的 assignment 记录 /
    ds_allocator 的 ReadyAuthorized 快照),绝不允许由客户端或 DS 自报。
    """

    #: 目标实例三元组,三者必填(§9.22 exact 实例绑定)。
    ds_pod_name: str = ""
    ds_instance_uid: str = ""
    ds_instance_epoch: int = 0
    #: "stable" / "canary",**必填**。单 Fleet 部署也要显式写 stable ——
    #: 把空值解释成"隐式默认轨道"会让 §9.21 的轨道粘滞在历史数据上静默失效。
    release_track: str = ""
    #: hub 票必填(玩家当前归属版本);battle 票必空。
    hub_assignment_id: str = ""
    #: battle 票必填;hub 票必空 / 0。
    match_id: int = 0
    allocation_id: str = ""
    #: 仅 hub 票可带(见 `DSTicketClaimsV2.source_match_id`);battle 票必须 0。
    source_match_id: int = 0
    #: 请求方登录会话 jti(见 `DSTicketClaimsV2.sess_jti`)。可空(兼容窗)。
    session_jti: str = ""

    def validate(self, ds_type: str) -> None:
        """逐条对应 Go 的 `DSTicketTarget.validate`(顺序也照抄,便于两栈错误信息对拍)。"""
        if not self.ds_pod_name or not self.ds_instance_uid or self.ds_instance_epoch == 0:
            raise DSTicketConfigError(
                "auth.DSTicketTarget: pod/uid/instance_epoch must be complete"
            )
        _require_uint(
            self.ds_instance_epoch, bits=32, what="auth.DSTicketTarget: ds_instance_epoch"
        )
        if self.release_track not in (RELEASE_TRACK_STABLE, RELEASE_TRACK_CANARY):
            raise DSTicketConfigError(
                "auth.DSTicketTarget: invalid release_track %r", self.release_track
            )
        _require_uint(self.match_id, bits=64, what="auth.DSTicketTarget: match_id")
        _require_uint(self.source_match_id, bits=64, what="auth.DSTicketTarget: source_match_id")
        if ds_type == DS_TYPE_HUB:
            if not self.hub_assignment_id:
                raise DSTicketConfigError(
                    "auth.DSTicketTarget: hub ticket requires hub_assignment_id"
                )
            if self.match_id != 0 or self.allocation_id:
                raise DSTicketConfigError(
                    "auth.DSTicketTarget: hub ticket must not carry match/allocation binding"
                )
            return
        if ds_type == DS_TYPE_BATTLE:
            if self.match_id == 0 or not self.allocation_id:
                raise DSTicketConfigError(
                    "auth.DSTicketTarget: battle ticket requires match_id and allocation_id"
                )
            if self.hub_assignment_id:
                raise DSTicketConfigError(
                    "auth.DSTicketTarget: battle ticket must not carry hub_assignment_id"
                )
            if self.source_match_id != 0:
                raise DSTicketConfigError(
                    "auth.DSTicketTarget: battle ticket must not carry source_match_id "
                    "(battle binding is match_id)"
                )
            return
        raise DSTicketConfigError("auth.DSTicketTarget: invalid dsType %r", ds_type)


# ── 签发器 ───────────────────────────────────────────────────────────────────


@dataclasses.dataclass(slots=True)
class DSTicketSignerConfig:
    """RS256 签发器配置 —— 对应 Go 的 `auth.DSTicketSignerConfig`。

    与 HS256 玩家面配置(`pandorapy.auth.SignerConfig`)是**两个类型**,不是两个实例:
    编译期 / 类型期隔离,防止 Session 密钥被误接进 DSTicket 信任域。
    """

    #: RSA 私钥 PEM(PKCS#1 或 PKCS#8)。以非 root 身份从 mode 0440 的 Secret 文件读入,
    #: 绝不进 ConfigMap / 命令行参数 / 明文环境变量。
    private_key_pem: bytes = b""
    #: 期望的签发密钥指纹(RFC 7638)。**必填**且必须与私钥推导指纹一致 ——
    #: 轮换时若挂错私钥文件,启动即拒,而不是签出一堆验不过的票。
    active_kid: str = ""
    #: 票据有效期。None / 0 取 `DS_TICKET_DEFAULT_TTL`,超 `DS_TICKET_MAX_TTL` 启动即拒。
    ttl: _dt.timedelta | None = None
    issuer: str = ""
    audience: str = ""
    #: 可注入的时钟(测试)。默认 `datetime.now(UTC)`。
    now_fn: typing.Callable[[], _dt.datetime] | None = None


class DSTicketSigner:
    """签发 dst_ver=2 RS256 票据。无可变状态,线程安全。"""

    __slots__ = ("_key", "_kid", "_issuer", "_audience", "_ttl", "_now")

    def __init__(
        self,
        *,
        key: _rsa.RSAPrivateKey,
        kid: str,
        issuer: str,
        audience: str,
        ttl: _dt.timedelta,
        now_fn: typing.Callable[[], _dt.datetime],
    ) -> None:
        # 直接 new 会绕过 `new()` 里的弱密钥 / TTL / kid 三道闸,所以业务侧一律用 `new()`。
        self._key = key
        self._kid = kid
        self._issuer = issuer
        self._audience = audience
        self._ttl = ttl
        self._now = now_fn

    @classmethod
    def new(cls, cfg: DSTicketSignerConfig) -> DSTicketSigner:
        """对应 Go 的 `NewDSTicketSigner`。弱密钥 / TTL 超上限 / kid 不匹配一律**启动即拒**。"""
        key = parse_rsa_private_key_pem(cfg.private_key_pem)
        bits = key.key_size
        if bits < DS_TICKET_MIN_RSA_BITS:
            raise DSTicketConfigError(
                "auth.NewDSTicketSigner: RSA key too weak (%d bits, need >=%d)",
                bits,
                DS_TICKET_MIN_RSA_BITS,
            )
        kid = rsa_public_key_thumbprint(key.public_key())
        if not cfg.active_kid:
            raise DSTicketConfigError(
                "auth.NewDSTicketSigner: active_kid is required (implicit key selection forbidden)"
            )
        # ★ 密钥指纹比较走 `hmac.compare_digest`:`==` 的短路会泄漏"前几个字符对上了"。
        # 指纹本身不是秘密,但这是全仓统一纪律 —— 例外一旦开口就会被抄到真的比密钥处。
        if not hmac.compare_digest(cfg.active_kid, kid):
            raise DSTicketConfigError(
                "auth.NewDSTicketSigner: active_kid %r does not match private key thumbprint %r "
                "(wrong key file mounted?)",
                cfg.active_kid,
                kid,
            )
        ttl = cfg.ttl if cfg.ttl is not None else _dt.timedelta(0)
        if ttl == _dt.timedelta(0):
            ttl = DS_TICKET_DEFAULT_TTL
        if ttl < _dt.timedelta(0) or ttl > DS_TICKET_MAX_TTL:
            # 时长一律走 godur:Go 的 `%v` 打的是 "3m0s",Python 的 str(timedelta)
            # 打的是 "0:03:00" —— 灰度期两栈日志放一起比对时,这种差异要人脑换算。
            raise DSTicketConfigError(
                "auth.NewDSTicketSigner: ttl %s out of range (0, %s] "
                "— B1 纯本地验票要求短时票",
                godur.duration_string(ttl),
                godur.duration_string(DS_TICKET_MAX_TTL),
            )
        return cls(
            key=key,
            kid=kid,
            issuer=cfg.issuer or DS_TICKET_ISSUER,
            audience=cfg.audience or DS_TICKET_AUDIENCE,
            ttl=ttl,
            now_fn=cfg.now_fn or (lambda: _dt.datetime.now(_dt.UTC)),
        )

    def kid(self) -> str:
        """签发密钥指纹(RFC 7638),供启动日志与部署对账。"""
        return self._kid

    def ttl(self) -> _dt.timedelta:
        """票据有效期(调用方对齐 Redis 记录 TTL 时用)。"""
        return self._ttl

    def sign_hub_ticket(
        self,
        player_id: int,
        region_id: int,
        cell_id: int,
        role_id: int,
        jti: str,
        target: DSTicketTarget,
    ) -> tuple[str, int]:
        """签发绑定唯一 Hub DS 实例 + 玩家归属版本的 hub 票。返回 `(token, expires_at_ms)`。"""
        return self._sign(player_id, DS_TYPE_HUB, region_id, cell_id, role_id, jti, target)

    def sign_battle_ticket(
        self,
        player_id: int,
        region_id: int,
        cell_id: int,
        jti: str,
        target: DSTicketTarget,
    ) -> tuple[str, int]:
        """签发绑定唯一 Battle DS 实例 + 对局/分配 ID 的 battle 票。

        ★ 没有 role_id 参数:Go 侧 `SignBattleTicket` 硬传 0(battle 票不带角色),
        这里同样恒 0 —— 加个可选参数就等于允许 battle 票携带 role_id,
        与 Go 签出的票不同形。
        """
        return self._sign(player_id, DS_TYPE_BATTLE, region_id, cell_id, 0, jti, target)

    def _sign(
        self,
        player_id: int,
        ds_type: str,
        region_id: int,
        cell_id: int,
        role_id: int,
        jti: str,
        target: DSTicketTarget,
    ) -> tuple[str, int]:
        if player_id == 0:
            raise DSTicketConfigError("auth.DSTicketSigner: playerID must be > 0")
        _require_uint(player_id, bits=64, what="auth.DSTicketSigner: player_id")
        if not jti:
            # B1 纯本地验票下 jti 是唯一的吊销抓手,自动铸一个也不行:
            # 调用方必须能把 jti 落进核销记录,自动生成的它拿不到。
            raise DSTicketConfigError("auth.DSTicketSigner: jti must be non-empty")
        _require_uint(region_id, bits=32, what="auth.DSTicketSigner: region_id")
        _require_uint(cell_id, bits=32, what="auth.DSTicketSigner: cell_id")
        _require_uint(role_id, bits=32, what="auth.DSTicketSigner: role_id")
        target.validate(ds_type)

        now = self._now()
        exp = now + self._ttl

        # ★ claim 组装:非 omitempty 的字段**恒出现**(哪怕零值),omitempty 的字段
        # 零值时**必须不出现**。这一份 dict 的键集合就是与 Go 的字节级契约。
        claims: dict[str, object] = {
            "iss": self._issuer,
            "sub": str(player_id),
            # Go 的 `jwt.ClaimStrings` 在 `MarshalSingleStringAsArray=true`(v5 默认)下
            # 恒序列化成**数组**。写成裸字符串也能被多数库接受,但就不是同一份字节了。
            "aud": [self._audience],
            "iat": _unix_seconds(now),
            "exp": _unix_seconds(exp),
            "jti": jti,
            "dst_ver": DS_TICKET_VERSION_2,
            "ds_type": ds_type,
            "ds_pod": target.ds_pod_name,
            "ds_uid": target.ds_instance_uid,
            "ds_instance_epoch": target.ds_instance_epoch,
        }
        _put_omitempty(claims, "release_track", target.release_track)
        _put_omitempty(claims, "region_id", region_id)
        _put_omitempty(claims, "cell_id", cell_id)
        _put_omitempty(claims, "role_id", role_id)
        _put_omitempty(claims, "match_id", target.match_id)
        _put_omitempty(claims, "allocation_id", target.allocation_id)
        _put_omitempty(claims, "hub_assignment_id", target.hub_assignment_id)
        _put_omitempty(claims, "source_match_id", target.source_match_id)
        _put_omitempty(claims, "sjti", target.session_jti)

        token = pyjwt.encode(
            claims,
            self._key,
            algorithm=DS_TICKET_ALGORITHM,
            # header 只多一个 kid。PyJWT 自带 `typ:"JWT"` 且按 key 排序输出,
            # 与 Go(map 序列化天然按 key 排序)得到同一串:{"alg","kid","typ"}。
            headers={"kid": self._kid},
        )
        # ★ 返回值是**毫秒**,而 JWT 里的 exp 是**秒**。两条路,别串。
        return token, _unix_milli(exp)


def _put_omitempty(claims: dict[str, object], key: str, value: object) -> None:
    """复刻 Go 的 `json:",omitempty"`:零值(0 / 空串)不写入。"""
    if value == 0 or value == "":
        return
    claims[key] = value


# ── 算法分发(只解 header,不是鉴权结果)──────────────────────────────────────


def ds_ticket_algorithm(token: str) -> str:
    """只解析 JOSE header 以选择严格 verifier —— 对应 Go 的 `DSTicketAlgorithm`。

    ★ 返回值**只能用于分发**,不是鉴权结果:HS256 / RS256 两个分支随后都会再做
    完整的签名与 claims 校验。其余算法(含 `none`)在分发前就拒 —— 这道闸是
    legacy(HS256)与 v2(RS256)共存期防算法混淆的第一层。
    """
    if not token:
        raise DSTicketInvalidError("empty ds ticket")
    try:
        header = pyjwt.get_unverified_header(token)
    except pyjwt.PyJWTError as exc:
        raise DSTicketInvalidError("ds ticket header invalid") from exc
    alg = header.get("alg")
    if not isinstance(alg, str):
        raise DSTicketInvalidError("ds ticket header invalid")
    # 白名单,不是黑名单:黑名单挡不住 "rs256"(小写)/ "None" 这类变体。
    if alg not in ("HS256", DS_TICKET_ALGORITHM):
        raise DSTicketInvalidError("ds ticket alg %r unsupported", alg)
    return alg


# ── 验签器 ───────────────────────────────────────────────────────────────────


@dataclasses.dataclass(slots=True)
class DSTicketVerifierConfig:
    """RS256 验签器配置 —— 对应 Go 的 `auth.DSTicketVerifierConfig`。"""

    #: 公钥 keyset 文件原始内容(strict 解析,见 `parse_ds_ticket_jwks`)。
    jwks: bytes = b""
    issuer: str = ""
    audience: str = ""
    now_fn: typing.Callable[[], _dt.datetime] | None = None


class DSTicketVerifier:
    """验 dst_ver=2 RS256 票据。无可变状态,线程安全。"""

    __slots__ = ("_keys", "_issuer", "_audience", "_now")

    def __init__(
        self,
        *,
        keys: dict[str, _rsa.RSAPublicKey],
        issuer: str,
        audience: str,
        now_fn: typing.Callable[[], _dt.datetime],
    ) -> None:
        self._keys = keys
        self._issuer = issuer
        self._audience = audience
        self._now = now_fn

    @classmethod
    def new(cls, cfg: DSTicketVerifierConfig) -> DSTicketVerifier:
        """对应 Go 的 `NewDSTicketVerifier`。JWKS 解析失败 / 空 keyset **启动即拒**(fail-closed)。"""
        keys = parse_ds_ticket_jwks(cfg.jwks)
        return cls(
            keys=keys,
            issuer=cfg.issuer or DS_TICKET_ISSUER,
            audience=cfg.audience or DS_TICKET_AUDIENCE,
            now_fn=cfg.now_fn or (lambda: _dt.datetime.now(_dt.UTC)),
        )

    def _lookup_key(self, kid: str) -> _rsa.RSAPublicKey | None:
        """按 kid 取公钥。

        ★ 用 `hmac.compare_digest` 线性扫而不是 dict 下标:keyset 最多 8 把
        (`_DS_TICKET_JWKS_MAX_KEYS`),常数时间比较的代价可以忽略,而"凡与凭据
        相关的比较一律 compare_digest"这条纪律不留例外口子。
        """
        for known_kid, pub in self._keys.items():
            if hmac.compare_digest(known_kid, kid):
                return pub
        return None

    def verify(self, token: str) -> DSTicketClaimsV2:
        """验签 + 结构校验。校验项与 UE 侧 `FPandoraTicketVerifier` 的 RS256 路径一一对应:

        - alg 固定 RS256(白名单,天然拒 `none` / HS256 混淆);
        - kid 必须存在且在 keyset 内(kid 只是**选键提示**,签名仍必须验过);
        - iss / aud / exp 必须匹配且存在;iat 必须存在且 `exp-iat <= DS_TICKET_MAX_TTL`;
        - dst_ver == 2;ds_type ∈ {hub, battle};实例绑定按类型完整。
        """
        if not token:
            raise DSTicketInvalidError("empty ds ticket")

        try:
            header = pyjwt.get_unverified_header(token)
        except pyjwt.PyJWTError as exc:
            raise DSTicketInvalidError("ds ticket verify: %s", exc) from exc
        kid = header.get("kid")
        if not isinstance(kid, str) or not kid:
            raise DSTicketInvalidError(
                "ds ticket verify: error while executing keyfunc: ds ticket v2 requires kid header"
            )
        key = self._lookup_key(kid)
        if key is None:
            raise DSTicketInvalidError(
                "ds ticket verify: error while executing keyfunc: "
                "unknown kid %r (keyset stale or foreign ticket)",
                kid,
            )

        try:
            payload = pyjwt.decode(
                token,
                key,
                # ★ 单元素白名单 = 算法混淆的根治手段:把 alg 改成 HS256(拿公钥字节
                # 当 HMAC 密钥)或 none,在这里就被拒,连密钥都不会被取用。
                algorithms=[DS_TICKET_ALGORITHM],
                audience=self._audience,
                issuer=self._issuer,
                options={
                    # Go 侧 `WithExpirationRequired()` + 事后 nil 判等价于:这几个 claim 缺一不可。
                    "require": ["exp", "iat", "iss", "aud", "sub", "jti"],
                    # exp / nbf 交给下面按注入时钟判(PyJWT 只认真实系统时间,
                    # 没有 Go 的 `WithTimeFunc` —— 用系统时间会让时钟可注入的测试失效)。
                    "verify_exp": False,
                    "verify_nbf": False,
                },
            )
        # TypeError / ValueError 也要收:PyJWT 的 `int(payload["iat"])` 遇到 `"iat": []`
        # 这类构造 payload 会抛裸 TypeError,漏出去就变成 500 而不是"票非法"。
        except (pyjwt.PyJWTError, TypeError, ValueError) as exc:
            raise DSTicketInvalidError("ds ticket verify: %s", exc) from exc

        claims = _claims_from_payload(payload)

        # ── 时间三判(Go: Validator.verifyExpiresAt / verifyNotBefore / verifyIssuedAt)──
        # leeway 恒 0,与 Go 侧一致:v2 票只活 120s,给 leeway 等于变相延长票寿。
        now_sec = self._now().timestamp()
        if claims.expires_at is None or claims.issued_at is None:
            raise DSTicketInvalidError("ds ticket missing iat/exp")
        if now_sec >= claims.expires_at:
            raise DSTicketExpiredError("ds ticket expired")
        if claims.not_before is not None and now_sec < claims.not_before:
            raise DSTicketInvalidError("ds ticket verify: token is not valid yet")
        if now_sec < claims.issued_at:
            raise DSTicketInvalidError("ds ticket verify: token used before issued")

        # ── 结构校验(顺序照抄 Go,便于两栈错误对拍)────────────────────────
        if claims.dst_ver != DS_TICKET_VERSION_2:
            raise DSTicketInvalidError(
                "ds ticket dst_ver %d unsupported (want %d)",
                claims.dst_ver,
                DS_TICKET_VERSION_2,
            )
        if claims.player_id() == 0:
            raise DSTicketInvalidError("ds ticket sub not a valid player_id")
        if not claims.jti:
            raise DSTicketInvalidError("ds ticket missing jti")
        if claims.expires_at <= claims.issued_at:
            raise DSTicketInvalidError("ds ticket exp must be after iat")
        if claims.not_before is not None and claims.not_before > claims.expires_at:
            raise DSTicketInvalidError("ds ticket nbf must not be after exp")
        if claims.expires_at - claims.issued_at > DS_TICKET_MAX_TTL.total_seconds():
            # ★ 验签侧同样强制上限 —— 这就是"双向强制"。只在签发侧限制的话,
            # 任何一个配错 TTL 的签发点(或被攻破的签发点)都能签出长效 capability,
            # 而 B1 下 DS 不回后端查吊销,长票 = 吊销失效。
            raise DSTicketInvalidError(
                "ds ticket lifetime exceeds max ttl %s "
                "(long-lived capability forbidden under B1)",
                godur.duration_string(DS_TICKET_MAX_TTL),
            )

        target = DSTicketTarget(
            ds_pod_name=claims.ds_pod_name,
            ds_instance_uid=claims.ds_instance_uid,
            ds_instance_epoch=claims.ds_instance_epoch,
            release_track=claims.release_track,
            hub_assignment_id=claims.hub_assignment_id,
            match_id=claims.match_id,
            allocation_id=claims.allocation_id,
            source_match_id=claims.source_match_id,
        )
        try:
            target.validate(claims.ds_type)
        except DSTicketConfigError as exc:
            raise DSTicketInvalidError("ds ticket binding invalid: %s", exc.msg) from exc
        return claims


def _claims_from_payload(payload: dict[str, object]) -> DSTicketClaimsV2:
    """把 JSON payload 投影成 `DSTicketClaimsV2`,并做 Go 侧由 `json.Unmarshal` 天然完成的类型闸。

    ★ 这一段不能"尽力而为":Go 把 payload 反序列化进强类型结构体,
    `"ds_instance_epoch": -1` / `"match_id": "42"` / `"ds_instance_epoch": 2**40`
    都会**直接失败**。Python 的 dict 不会,`int(...)` 还会把它们"救活"成一个
    与 Go 不同的值 —— 于是同一张票两栈解出不同的实例绑定,票被兑换到错误的 Pod。
    """
    return DSTicketClaimsV2(
        issuer=_claim_str(payload, "iss"),
        subject=_claim_str(payload, "sub"),
        audience=_claim_audience(payload),
        issued_at=_claim_numeric_date(payload, "iat"),
        expires_at=_claim_numeric_date(payload, "exp"),
        not_before=_claim_numeric_date(payload, "nbf"),
        jti=_claim_str(payload, "jti"),
        dst_ver=_claim_int(payload, "dst_ver"),
        ds_type=_claim_str(payload, "ds_type"),
        ds_pod_name=_claim_str(payload, "ds_pod"),
        ds_instance_uid=_claim_str(payload, "ds_uid"),
        ds_instance_epoch=_claim_uint(payload, "ds_instance_epoch", bits=32),
        release_track=_claim_str(payload, "release_track"),
        region_id=_claim_uint(payload, "region_id", bits=32),
        cell_id=_claim_uint(payload, "cell_id", bits=32),
        role_id=_claim_uint(payload, "role_id", bits=32),
        match_id=_claim_uint(payload, "match_id", bits=64),
        allocation_id=_claim_str(payload, "allocation_id"),
        hub_assignment_id=_claim_str(payload, "hub_assignment_id"),
        source_match_id=_claim_uint(payload, "source_match_id", bits=64),
        sess_jti=_claim_str(payload, "sjti"),
    )


def _claim_str(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if value is None:
        return ""
    if not isinstance(value, str):
        # Go: `json: cannot unmarshal number into Go struct field ... of type string`。
        raise DSTicketInvalidError("ds ticket claim %s must be a string", key)
    return value


def _claim_audience(payload: dict[str, object]) -> tuple[str, ...]:
    """`aud` 可以是单串或数组(RFC 7519 §4.1.3),Go 的 `ClaimStrings` 两种都收。"""
    value = payload.get("aud")
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return tuple(typing.cast("list[str]", value))
    raise DSTicketInvalidError("ds ticket claim aud must be a string or string array")


def _claim_int(payload: dict[str, object], key: str) -> int:
    value = payload.get(key)
    if value is None:
        return 0
    if isinstance(value, bool) or not isinstance(value, int):
        raise DSTicketInvalidError("ds ticket claim %s must be an integer", key)
    return value


def _claim_uint(payload: dict[str, object], key: str, *, bits: int) -> int:
    value = _claim_int(payload, key)
    if value < 0 or value > (1 << bits) - 1:
        raise DSTicketInvalidError(
            "ds ticket claim %s out of uint%d range: %d", key, bits, value
        )
    return value


def _claim_numeric_date(payload: dict[str, object], key: str) -> float | None:
    """NumericDate:JSON 数字(**秒**,允许小数)。字符串 / bool / 越界一律拒。"""
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DSTicketInvalidError("ds ticket claim %s must be a numeric date", key)
    if not -_MAX_NUMERIC_DATE <= value <= _MAX_NUMERIC_DATE:
        raise DSTicketInvalidError("ds ticket claim %s out of range", key)
    return float(value)


# ── 严格 JWKS ────────────────────────────────────────────────────────────────


def parse_ds_ticket_jwks(data: bytes | str) -> dict[str, _rsa.RSAPublicKey]:
    """严格解析公钥 keyset —— 对应 Go 的 `ParseDSTicketJWKS`。

    任何一把 key 不合规即**整文件拒绝**(fail-closed;放行"其余几把"等于让一次
    投递事故留下半可用的 keyset,而运维以为发布成功了):

      - `kty` 必须 "RSA"(`kty=oct` = 把对称密钥当公钥投递,直接判事故);
      - `use` / `alg` 均必填且必须分别为 "sig" / "RS256";
      - `kid` 必填、全 set 内唯一、且必须等于该公钥的 RFC 7638 指纹(防 kid 张冠李戴);
      - 私钥成员(d/p/q/dp/dq/qi/oth/k)只要**出现**即拒(含空串 / null);
      - `n` >= 2048 bit;`e` 为常见公开指数;
      - keyset 非空、<= 8 把、文件 <= 64 KiB;
      - **未知字段一律拒**(对应 Go 的 `DisallowUnknownFields`)—— 多出来的字段
        意味着这份文件不是本契约生成的,继续解析等于猜。
    """
    raw = data.encode("utf-8") if isinstance(data, str) else data
    if not raw:
        raise DSTicketConfigError("jwks empty")
    if len(raw) > _DS_TICKET_JWKS_MAX_BYTES:
        raise DSTicketConfigError(
            "jwks too large (%d bytes > %d)", len(raw), _DS_TICKET_JWKS_MAX_BYTES
        )
    try:
        # json.loads 对尾随内容直接报 "Extra data",等价于 Go 的 trailing JSON value 判定。
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DSTicketConfigError("jwks parse: %s", exc) from exc
    if not isinstance(decoded, dict):
        raise DSTicketConfigError("jwks parse: top level must be an object")
    unknown = sorted(set(decoded) - _JWKS_ALLOWED_MEMBERS)
    if unknown:
        raise DSTicketConfigError("jwks parse: unknown field %r", unknown[0])

    revision = decoded.get("revision", 0)
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
        raise DSTicketConfigError("jwks revision must be >= 1")
    active_kid = decoded.get("active_kid", "")
    if not isinstance(active_kid, str) or not active_kid:
        raise DSTicketConfigError("jwks active_kid required")
    keys = decoded.get("keys")
    if keys is None or not isinstance(keys, list) or not keys:
        raise DSTicketConfigError("jwks has no keys")
    if len(keys) > _DS_TICKET_JWKS_MAX_KEYS:
        raise DSTicketConfigError(
            "jwks has %d keys (max %d)", len(keys), _DS_TICKET_JWKS_MAX_KEYS
        )

    out: dict[str, _rsa.RSAPublicKey] = {}
    for i, entry in enumerate(keys):
        if not isinstance(entry, dict):
            raise DSTicketConfigError("jwks key[%d]: must be an object", i)
        unknown_member = sorted(set(entry) - _JWK_ALLOWED_MEMBERS)
        if unknown_member:
            raise DSTicketConfigError("jwks key[%d]: unknown field %r", i, unknown_member[0])
        if entry.get("kty") != "RSA":
            raise DSTicketConfigError(
                "jwks key[%d]: kty %r rejected "
                "(only RSA public keys allowed; oct = 对称密钥投递事故)",
                i,
                entry.get("kty"),
            )
        # 只判"出现",不判值:`"d": null` 与 `"d": ""` 同样是私钥泄漏进 Fleet 的证据。
        if any(member in entry for member in _JWK_PRIVATE_MEMBERS):
            raise DSTicketConfigError(
                "jwks key[%d]: private key material present — refusing entire keyset", i
            )
        if entry.get("use") != "sig":
            raise DSTicketConfigError(
                "jwks key[%d]: use %r rejected (want sig)", i, entry.get("use")
            )
        if entry.get("alg") != DS_TICKET_ALGORITHM:
            raise DSTicketConfigError(
                "jwks key[%d]: alg %r rejected (want RS256)", i, entry.get("alg")
            )
        kid = entry.get("kid")
        if not isinstance(kid, str) or not kid:
            raise DSTicketConfigError("jwks key[%d]: kid required", i)
        if kid in out:
            raise DSTicketConfigError("jwks key[%d]: duplicate kid %r", i, kid)
        n_b64 = entry.get("n")
        e_b64 = entry.get("e")
        if not isinstance(n_b64, str) or not isinstance(e_b64, str):
            raise DSTicketConfigError("jwks key[%d]: n/e required", i)
        try:
            pub = rsa_public_key_from_jwk(n_b64, e_b64)
        except DSTicketConfigError as exc:
            raise DSTicketConfigError("jwks key[%d]: %s", i, exc.msg) from exc
        if pub.key_size < DS_TICKET_MIN_RSA_BITS:
            raise DSTicketConfigError(
                "jwks key[%d]: RSA modulus %d bits too weak (need >=%d)",
                i,
                pub.key_size,
                DS_TICKET_MIN_RSA_BITS,
            )
        got = rsa_public_key_thumbprint(pub)
        if not hmac.compare_digest(got, kid):
            raise DSTicketConfigError(
                "jwks key[%d]: kid %r does not match RFC 7638 thumbprint %r", i, kid, got
            )
        out[kid] = pub

    if active_kid not in out:
        raise DSTicketConfigError("jwks active_kid %r is not present in keys", active_kid)
    return out


def ds_ticket_jwks_metadata(data: bytes | str) -> tuple[int, str]:
    """读取并严格校验发布元数据,返回 `(revision, active_kid)` —— 对应 `DSTicketJWKSMetadata`。

    ★ 它**复用完整解析**而不是只挑两个字段读:否则部署对账会接受一份 verifier
    自己都会拒的半成品 keyset(对账绿灯、服务起不来 / 票全拒)。
    """
    parse_ds_ticket_jwks(data)
    raw = data.encode("utf-8") if isinstance(data, str) else data
    decoded = json.loads(raw.decode("utf-8"))
    return int(decoded["revision"]), str(decoded["active_kid"])


def ds_ticket_jwks_revision(data: bytes | str) -> int:
    """返回严格 keyset 的 revision —— 对应 `DSTicketJWKSRevision`。"""
    revision, _ = ds_ticket_jwks_metadata(data)
    return revision


def marshal_ds_ticket_jwks(
    revision: int, active_kid: str, *pubs: _rsa.RSAPublicKey
) -> bytes:
    """把一组 RSA 公钥编码成严格 JWKS(kid = RFC 7638 指纹)—— 对应 `MarshalDSTicketJWKS`。

    输出保证能被 `parse_ds_ticket_jwks`(以及 Go 的 `ParseDSTicketJWKS`)接受:
    2 空格缩进、字段顺序 revision/active_kid/keys、key 内 kty/use/alg/kid/n/e、
    并按 kid 排序 —— **确定性输出**,同一组 key 恒得同一份文件,便于 ConfigMap 内容对账。
    """
    if revision < 1:
        raise DSTicketConfigError("auth.MarshalDSTicketJWKS: revision must be >= 1")
    if not active_kid:
        raise DSTicketConfigError("auth.MarshalDSTicketJWKS: active_kid is required")
    if not pubs:
        raise DSTicketConfigError("auth.MarshalDSTicketJWKS: at least one key required")

    entries: list[dict[str, object]] = []
    for pub in pubs:
        if pub is None:
            raise DSTicketConfigError("auth.MarshalDSTicketJWKS: nil key")
        numbers = pub.public_numbers()
        entries.append(
            {
                "kty": "RSA",
                "use": "sig",
                "alg": DS_TICKET_ALGORITHM,
                "kid": rsa_public_key_thumbprint(pub),
                "n": _b64url_encode(_int_to_bytes(numbers.n)),
                "e": _b64url_encode(_int_to_bytes(numbers.e)),
            }
        )
    entries.sort(key=lambda item: typing.cast("str", item["kid"]))
    if not any(hmac.compare_digest(typing.cast("str", e["kid"]), active_kid) for e in entries):
        raise DSTicketConfigError(
            "auth.MarshalDSTicketJWKS: active_kid %r is not present in keys", active_kid
        )
    document = {"revision": revision, "active_kid": active_kid, "keys": entries}
    # Go 的 `json.MarshalIndent(v, "", "  ")` 不带尾随换行;Python 默认分隔符
    # 在 indent 模式下就是 `,` + 换行与 `": "`,与 Go 一致。
    return json.dumps(document, indent=2, ensure_ascii=False).encode("utf-8")


# ── RFC 7638 指纹 / 密钥编解码 ───────────────────────────────────────────────


def _int_to_bytes(value: int) -> bytes:
    """大整数 → **最小长度**大端字节串(等价 Go `big.Int.Bytes()`)。

    "最小长度"是硬要求:多一个前导 0 字节,算出来的 RFC 7638 指纹就完全不同,
    于是同一把公钥在两栈得到两个 kid → 票带的 kid 在对方 keyset 里找不到。
    """
    if value == 0:
        return b""
    return value.to_bytes((value.bit_length() + 7) // 8, "big")


def rsa_public_key_thumbprint(pub: _rsa.RSAPublicKey) -> str:
    """RFC 7638 JWK 指纹(SHA-256,base64url 无填充)—— 对应 `RSAPublicKeyThumbprint`。

    成员按**字典序** e, kty, n 构造 canonical JSON 后哈希 —— 与语言无关的稳定 kid。
    canonical JSON 里不能有任何空格(RFC 7638 §3.3),Go 侧是手工 `fmt.Sprintf` 拼的,
    这里同样手工拼:`json.dumps` 的默认分隔符带空格,拼出来的指纹会与 Go 全不一样。
    """
    numbers = pub.public_numbers()
    e = _b64url_encode(_int_to_bytes(numbers.e))
    n = _b64url_encode(_int_to_bytes(numbers.n))
    canonical = f'{{"e":"{e}","kty":"RSA","n":"{n}"}}'
    digest = hashlib.sha256(canonical.encode("utf-8")).digest()
    return _b64url_encode(digest)


def generate_ds_ticket_key_pair() -> tuple[bytes, _rsa.RSAPublicKey, str]:
    """生成 RSA-2048 密钥对,返回 `(私钥 PKCS#8 PEM, 公钥, kid)` —— 对应 `GenerateDSTicketKeyPair`。

    ★ 只供密钥工具(首次迁移的 K1 / 罕见轮换的 K2)与测试使用;**服务运行期绝不调用**
    (密钥生命周期与发布解耦:常规发布永不轮换密钥)。
    """
    key = _rsa.generate_private_key(public_exponent=65537, key_size=DS_TICKET_MIN_RSA_BITS)
    private_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pub = key.public_key()
    return private_pem, pub, rsa_public_key_thumbprint(pub)


def parse_rsa_private_key_pem(pem_bytes: bytes | str) -> _rsa.RSAPrivateKey:
    """解析 RSA 私钥 PEM(PKCS#8 `PRIVATE KEY` / PKCS#1 `RSA PRIVATE KEY`)。

    ★ 先按 **PEM block 类型**白名单判,再交给 cryptography 解:
    `load_pem_private_key` 会连 EC / Ed25519 / 加密私钥一起收下,而 Go 的
    `parseRSAPrivateKeyPEM` 只认那两种 block 类型。放宽的后果不是报错,
    而是"启动成功但签出的票 DS 验不了"(或更糟:把加密私钥当明文密钥用)。
    """
    raw = pem_bytes.encode("utf-8") if isinstance(pem_bytes, str) else pem_bytes
    if not raw:
        raise DSTicketConfigError("private key PEM empty")
    block_type = _first_pem_block_type(raw)
    if block_type is None:
        raise DSTicketConfigError("private key PEM decode failed")
    if block_type not in ("PRIVATE KEY", "RSA PRIVATE KEY"):
        raise DSTicketConfigError("unsupported PEM block type %r", block_type)
    try:
        key = serialization.load_pem_private_key(raw, password=None)
    except (ValueError, TypeError) as exc:
        raise DSTicketConfigError("parse %s: %s", block_type, exc) from exc
    if not isinstance(key, _rsa.RSAPrivateKey):
        raise DSTicketConfigError("%s key is not RSA", block_type)
    return key


def _first_pem_block_type(raw: bytes) -> str | None:
    """取**第一个** PEM block 的类型(等价 Go `pem.Decode` 只解首块的行为)。

    不用正则:PEM 是行结构,`splitlines` + 前后缀判定比一个跨行正则更难写错,
    也不用担心 `^`/`$` 在多行串上的多行语义(见文件头 `\\A...\\Z` 纪律)。
    """
    begin = b"-----BEGIN "
    end = b"-----"
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith(begin) and stripped.endswith(end):
            body = stripped[len(begin) : -len(end)]
            if body:
                return body.decode("ascii", errors="replace")
    return None


def rsa_public_key_from_jwk(n_b64: str, e_b64: str) -> _rsa.RSAPublicKey:
    """JWK 的 `n` / `e` → RSA 公钥 —— 对应 Go 的 `rsaPublicKeyFromJWK`。"""
    if not n_b64 or not e_b64:
        raise DSTicketConfigError("n/e required")
    n_bytes = _b64url_decode(n_b64, what="n")
    e_bytes = _b64url_decode(e_b64, what="e")
    if not n_bytes or n_bytes[0] == 0:
        # 前导 0 会让 RFC 7638 指纹与规范值不同(见 `_int_to_bytes`),必须在入口就拒。
        raise DSTicketConfigError("n must be minimal big-endian without leading zero")
    if not e_bytes or len(e_bytes) > 4:
        raise DSTicketConfigError("e out of range")
    e = int.from_bytes(e_bytes, "big")
    # 只接受常见公开指数(奇数、3 <= e <= 2^31-1);偶数 e 根本不可能与 φ(n) 互质,
    # 出现即是构造数据 —— 离谱指数一律拒。
    if e < 3 or e > (1 << 31) - 1 or e % 2 == 0:
        raise DSTicketConfigError("e not an acceptable public exponent")
    n = int.from_bytes(n_bytes, "big")
    try:
        return _rsa.RSAPublicNumbers(e=e, n=n).public_key()
    except ValueError as exc:
        raise DSTicketConfigError("rsa public key invalid: %s", exc) from exc


# ── 配置接线(对应 dsticket_conf.go)──────────────────────────────────────────


@dataclasses.dataclass(slots=True)
class DSTicketConf:
    """`ds_ticket:` 配置节 —— 对应 Go 的 `config.DSTicketConf`(yaml key 逐字一致)。

    留空 `private_key_file` = 本服务不启用 v2 签发,沿用 legacy HS256 DSTicket
    (dev / local-off 不变)。留空 `jwks_file` = 不启用 v2 校验。
    """

    private_key_file: str = ""
    active_kid: str = ""
    #: 票据有效期。None = 用默认 120s(**不是** 0)。
    ttl: _dt.timedelta | None = None
    jwks_file: str = ""
    #: 期望的 keyset revision(字符串,与 yaml 原样一致)。设置后与文件内不符即启动失败。
    keyset_revision: str = ""

    def signer_enabled(self) -> bool:
        return bool(self.private_key_file)

    def verifier_enabled(self) -> bool:
        return bool(self.jwks_file)


def new_ds_ticket_signer_from_conf(conf: DSTicketConf) -> DSTicketSigner:
    """按共享配置构造 v2 签发器 —— 对应 `NewDSTicketSignerFromConf`。

    ★ 集中在这里的原因:login / hub_allocator / matchmaker 三个签发点必须用**完全一致**的
    加载与校验逻辑(读私钥、kid 自检、TTL 上限)。各服务各写一份必然出现行为漂移,
    而漂移的表现是"某个签发点签的票 DS 拒",排查要跨三个服务。
    """
    if not conf.signer_enabled():
        raise DSTicketConfigError("ds_ticket: private_key_file 未配置,v2 签发未启用")
    try:
        with open(conf.private_key_file, "rb") as handle:
            pem_bytes = handle.read()
    except OSError as exc:
        raise DSTicketConfigError("ds_ticket: 读私钥文件失败: %s", exc) from exc
    return DSTicketSigner.new(
        DSTicketSignerConfig(
            private_key_pem=pem_bytes, active_kid=conf.active_kid, ttl=conf.ttl
        )
    )


def new_ds_ticket_verifier_from_conf(conf: DSTicketConf) -> DSTicketVerifier:
    """按共享配置构造 v2 校验器 —— 对应 `NewDSTicketVerifierFromConf`。

    revision / active_kid 双对账:配置写了什么、文件里是什么,不一致**启动即失败**。
    挡的是"换了键没换文件"(或反过来)这类半完成发布 —— 它在运行期的表现是
    随机一部分票验不过,而两边日志都正常。
    """
    if not conf.verifier_enabled():
        raise DSTicketConfigError("ds_ticket: jwks_file 未配置,v2 校验未启用")
    if not conf.keyset_revision or not conf.active_kid:
        raise DSTicketConfigError(
            "ds_ticket: verifier requires explicit keyset_revision and active_kid"
        )
    try:
        with open(conf.jwks_file, "rb") as handle:
            data = handle.read()
    except OSError as exc:
        raise DSTicketConfigError("ds_ticket: 读 JWKS 文件失败: %s", exc) from exc
    try:
        revision, active_kid = ds_ticket_jwks_metadata(data)
    except DSTicketConfigError as exc:
        raise DSTicketConfigError("ds_ticket: JWKS 不合规: %s", exc.msg) from exc
    if not _DECIMAL_RE.fullmatch(conf.keyset_revision):
        raise DSTicketConfigError("ds_ticket: keyset_revision 必须是正整数")
    want = int(conf.keyset_revision)
    if want < 1:
        raise DSTicketConfigError("ds_ticket: keyset_revision 必须是正整数")
    if revision != want:
        raise DSTicketConfigError(
            "ds_ticket: keyset revision 不匹配: 期望 %d, JWKS 内为 %d", want, revision
        )
    if not hmac.compare_digest(active_kid, conf.active_kid):
        raise DSTicketConfigError(
            "ds_ticket: active_kid 不匹配: 配置为 %r, JWKS 内为 %r", conf.active_kid, active_kid
        )
    return DSTicketVerifier.new(DSTicketVerifierConfig(jwks=data))


# ── 本机 DS 运行契约(对应 ds_local_profile.go)───────────────────────────────

#: allocator → UE 的本机运行契约标记。
#: ★ 它**不是授权凭据**:UE 还会机械校验 Windows、非 Agones、本地 pod 前缀
#: 以及完整 Model-B JWT scope。把它当凭据用等于本机档位下没有鉴权。
DS_LOCAL_PROFILE_ENV = "PANDORA_DS_LOCAL_PROFILE"
#: 只用于 mode=local + ds_auth.mode=off + authority=legacy。
DS_LOCAL_PROFILE_OFF_V1 = "local-off-v1"
#: 一次性 env 凭据必须覆盖的最短本地 Hub 调试会话。
#: local-off-v1 没有 annotation 轮换,UE 到 exp 会主动清空 active,
#: 所以**不能**按 Guard=off 跳过这道闸(否则调试到一半 DS 自己把玩家踢了)。
DS_LOCAL_HUB_MIN_TOKEN_TTL = _dt.timedelta(hours=12)


def validate_ds_local_profile_off_v1(
    guard_mode: str, authority_mode: str, signer_ready: bool
) -> None:
    """阻止本机 allocator 把**生产 / 灰度姿态**误标成离线 profile。

    `guard_mode` 应传已经过 middleware 解析归一化的值,故只接受精确的 "off"。
    """
    if guard_mode != "off" or authority_mode != "legacy" or not signer_ready:
        raise DSTicketConfigError(
            "%s requires guard=off authority=legacy signer_ready=true "
            "(got guard=%r authority=%r signer_ready=%s)",
            DS_LOCAL_PROFILE_OFF_V1,
            guard_mode,
            authority_mode,
            # Go 的 `%t` 打 "true"/"false";Python 的 bool 会打 "True"/"False"。
            "true" if signer_ready else "false",
        )


def validate_ds_local_hub_profile_off_v1(
    guard_mode: str, authority_mode: str, signer_ready: bool, token_ttl: _dt.timedelta
) -> None:
    """在通用 profile 门上再校验一次性 Hub 凭据寿命。"""
    validate_ds_local_profile_off_v1(guard_mode, authority_mode, signer_ready)
    if token_ttl < DS_LOCAL_HUB_MIN_TOKEN_TTL:
        raise DSTicketConfigError(
            "%s hub token ttl=%s is below local session minimum %s",
            DS_LOCAL_PROFILE_OFF_V1,
            godur.duration_string(token_ttl),
            godur.duration_string(DS_LOCAL_HUB_MIN_TOKEN_TTL),
        )
