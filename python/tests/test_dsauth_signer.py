"""DS 回调令牌**签发侧** —— 与同仓校验侧的自签自验往返对拍。

为什么这条往返测试是本文件的地基:签发与校验是两套独立写下的 claim 名 / 单位 / 编码,
两边同时写错同一处才会互相认可。而**跨栈**(Go 签 → Python 验、Python 签 → Go 验)
的失配没有任何本地信号:令牌照样签得出来、日志照样绿,只有等灰度期把 ds_auth.mode
切成 enforce 那一刻,一台 DS 上全部玩家的回调成批 401。

同仓往返能一次性抓住三类漂移(claim 名 / 时间单位 / base64 编码);跨栈那一层由
「claim 集合与 Go 的 json tag 逐字对照」这组断言钉住。
"""

from __future__ import annotations

import datetime as _dt
import hmac

import jwt as pyjwt
import pytest

from pandorapy import auth, dsauth, errcode

SECRET = "pandora-dev-jwt-secret-change-me-32!"
OTHER_SECRET = "another-dev-secret-at-least-32-bytes!!"


def _signer(
    secret: str = SECRET,
    issuer: str = auth.DS_CALLBACK_ISSUER,
    audience: str = auth.DS_CALLBACK_AUDIENCE,
    now_fn=None,
) -> auth.DSCallbackSigner:
    return auth.DSCallbackSigner(
        auth.SignerConfig(
            secret=secret.encode("utf-8"),
            issuer=issuer,
            audience=audience,
            account_audience=auth.DEFAULT_ACCOUNT_AUDIENCE,
        ),
        now_fn,
    )


def _verifier(additional: list[str] | None = None) -> dsauth.DSCallbackVerifier:
    return dsauth.DSCallbackVerifier(
        issuer=auth.DS_CALLBACK_ISSUER,
        audience=auth.DS_CALLBACK_AUDIENCE,
        secret=SECRET,
        additional_secrets=additional or [],
    )


def _payload(token: str, secret: str = SECRET) -> dict:
    """不做 aud/iss 校验地解出裸 payload —— 用来逐字对照 claim 名与 omitempty。"""
    return pyjwt.decode(
        token,
        secret,
        algorithms=[auth.ALGORITHM],
        options={"verify_aud": False},
    )


class Ctx:
    """gRPC ServicerContext 的最小桩(与 tests/test_dsauth.py 同形)。"""

    def __init__(self, **headers: str) -> None:
        self._md = tuple(headers.items())

    def invocation_metadata(self):  # noqa: ANN201
        return self._md


# ── ① 自签自验往返(本文件的地基)────────────────────────────────────────────


def test_battle_token_round_trip() -> None:
    """★ 变异:auth.py 里 `"ds_type": ds_type` 改成 `"dsType"` → 本条红。

    ★ 变异:`"match_id"` 改成 `"matchId"` → 本条红(match_id 解成 0)。
    ★ 变异:`"exp": int(exp.timestamp())` 改成毫秒 → 本条红(exp 落到公元 5 万年,
      exp_ms 与令牌 exp 对不上;而"看起来还能验过"正是单位写反最阴险的地方)。
    """
    token, exp_ms = _signer().sign_ds_callback(
        auth.DS_TYPE_BATTLE, "", 12345678901234567890, _dt.timedelta(hours=4)
    )
    claims = _verifier().verify(token)

    assert claims.ds_type == auth.DS_TYPE_BATTLE
    assert claims.match_id == 12345678901234567890
    assert claims.pod == ""  # battle 令牌签发时还不知道 Agones 选中哪个 GameServer
    assert claims.gen == 0
    # 签发返回的 exp_ms 是毫秒,令牌里的 exp 是秒 —— 校验侧按秒 ×1000 还原,
    # 两者在秒粒度上必须相等(签发侧保留了亚秒精度,故比对时向下取整到秒)。
    assert claims.exp_ms == (exp_ms // 1000) * 1000


def test_hub_token_round_trip_with_gen() -> None:
    """★ 变异:`"ds_gen"` 改成 `"gen"` → 本条红(gen 解成 0 = 代际门控静默失效)。"""
    token, _ = _signer().sign_ds_callback_with_gen(
        auth.DS_TYPE_HUB, "hub-pod-7", 0, 42, _dt.timedelta(hours=24)
    )
    claims = _verifier().verify(token)

    assert claims.ds_type == auth.DS_TYPE_HUB
    assert claims.pod == "hub-pod-7"  # Go: DSCallbackClaims.Pod() 返回 sub
    assert claims.match_id == 0
    assert claims.gen == 42


def test_round_trip_through_guard_enforce() -> None:
    """端到端:签出的令牌经 Bearer 头喂给 enforce 档 guard,范围匹配即放行。

    ★ 变异:签发时把 aud 写成 `["pandora-client"]`(玩家面)→ 本条红。
      这正是"域分离"要挡的东西,而它在签发侧毫无信号。
    """
    token, _ = _signer().sign_ds_callback(
        auth.DS_TYPE_BATTLE, "", 777, _dt.timedelta(hours=4)
    )
    guard = dsauth.DSCallbackGuard(_verifier(), dsauth.Mode.ENFORCE)
    ctx = Ctx(**{
        dsauth.METADATA_KEY_DS_GATEWAY: "1",
        dsauth.AUTHORIZATION_HEADER: f"Bearer {token}",
    })

    assert guard.check(ctx, dsauth.DSScope(ds_type=auth.DS_TYPE_BATTLE, match_id=777)) == 0
    # 范围绑定是真的在起作用:换一场对局立刻越权。
    assert (
        guard.check(ctx, dsauth.DSScope(ds_type=auth.DS_TYPE_BATTLE, match_id=778))
        == errcode.ErrPermissionDeny
    )


# ── ② claim 集合与 Go 的 json tag 逐字对照(跨栈那一层的钉子)──────────────


def test_battle_claim_set_matches_go_json_tags() -> None:
    """battle 令牌的 claim 键集合必须**恰好**是 Go DSCallbackClaims 会序列化出来的那些。

    ★ 变异:给 claims 补一个 `"jti": ...` → 本条红。
      Go 的 SignDSCallbackWithGen 不设 RegisteredClaims.ID(jti omitempty),多签一个
      jti 不会立刻验不过,却会让"令牌是否带 jti"成为两栈差异 —— 而按 jti 做重放
      去重的那一侧会突然多出/少掉一个维度。
    """
    token, _ = _signer().sign_ds_callback(
        auth.DS_TYPE_BATTLE, "", 999, _dt.timedelta(hours=4)
    )
    assert set(_payload(token)) == {"iss", "aud", "iat", "exp", "ds_type", "ds_kid", "match_id"}


def test_hub_claim_set_omits_match_id() -> None:
    """★ 变异:去掉 `if match_id:` 守卫、无条件写 match_id → 本条红。

    hub 令牌带 match_id=0 虽然验得过,但"hub 令牌不得携带 match_id"这条约束在字节层
    就失真了;下游任何按 claim **存在性**判别的代码(含未来的 Go 侧)当场分叉。
    """
    token, _ = _signer().sign_ds_callback_with_gen(
        auth.DS_TYPE_HUB, "hub-pod-1", 0, 0, _dt.timedelta(hours=24)
    )
    payload = _payload(token)
    assert set(payload) == {"iss", "sub", "aud", "iat", "exp", "ds_type", "ds_kid"}
    assert payload["ds_type"] == auth.DS_TYPE_HUB  # ds_type 无 omitempty,恒在


def test_battle_token_omits_sub_when_pod_unknown() -> None:
    """★ 变异:去掉 `if pod:` 守卫 → 本条红(sub 变成空串而不是不存在)。"""
    token, _ = _signer().sign_ds_callback(
        auth.DS_TYPE_BATTLE, "", 5, _dt.timedelta(minutes=1)
    )
    assert "sub" not in _payload(token)


def test_aud_is_array_like_go_claim_strings() -> None:
    """★ 变异:`"aud": self._cfg.audience`(裸字符串)→ 本条红。

    PyJWT 两种形态都验得过,Go 的 jwt.ClaimStrings 也都收 —— 但令牌字节不同,
    任何按原文比对 / 快照对拍的地方会分叉。照搬数组形式。
    """
    token, _ = _signer().sign_ds_callback(
        auth.DS_TYPE_BATTLE, "", 5, _dt.timedelta(minutes=1)
    )
    assert _payload(token)["aud"] == [auth.DS_CALLBACK_AUDIENCE]


# ── ③ kid:头与 claim 同源,且等于 Go 的 keyFingerprint ────────────────────


def test_kid_header_and_claim_are_go_key_fingerprint() -> None:
    """★ 变异:`key_fingerprint` 里 `[:16]` 改成 `[:32]` → 本条红。

    kid = hex(sha256(secret)[:8]) = **16 个 hex 字符**。截取长度写岔,轮换期 Go 侧
    按 kid 路由密钥时会找不到 key → 该密钥签的令牌全线拒。
    """
    token, _ = _signer().sign_ds_callback(
        auth.DS_TYPE_BATTLE, "", 5, _dt.timedelta(minutes=1)
    )
    want = auth.key_fingerprint(SECRET.encode("utf-8"))
    assert len(want) == 16

    header_kid = pyjwt.get_unverified_header(token)["kid"]
    claim_kid = _payload(token)["ds_kid"]
    # 密钥派生值的比较一律走 compare_digest(不因比较耗时泄漏指纹前缀)。
    assert hmac.compare_digest(header_kid, want)
    assert hmac.compare_digest(claim_kid, want)


def test_key_fingerprint_is_deterministic_and_key_bound() -> None:
    """指纹必须确定性(同密钥恒同值)且与密钥绑定(不同密钥必不同)。"""
    a = auth.key_fingerprint(SECRET.encode("utf-8"))
    assert hmac.compare_digest(a, auth.key_fingerprint(SECRET.encode("utf-8")))
    assert not hmac.compare_digest(a, auth.key_fingerprint(OTHER_SECRET.encode("utf-8")))


# ── ④ base64url 无 padding(JWT 规范;跨语言互操作最常见的坑)──────────────


def test_token_segments_are_unpadded_base64url() -> None:
    """★ 变异:手工给任一段补 '=' → 本条红。

    RFC 7515 §2 要求 base64url **去掉尾部 '='**。带 padding 的令牌 Go 的
    base64.RawURLEncoding 直接解不出来 —— 表现是"Python 签的令牌 Go 一律非法",
    而 Python 自己验得好好的。
    """
    token, _ = _signer().sign_ds_callback(
        auth.DS_TYPE_BATTLE, "", 5, _dt.timedelta(minutes=1)
    )
    segments = token.split(".")
    assert len(segments) == 3
    for seg in segments:
        assert "=" not in seg
        assert "+" not in seg and "/" not in seg  # url-safe 字母表


def test_header_alg_and_typ_match_go() -> None:
    """alg / typ 与 Go 的 jwt.NewWithClaims(SigningMethodHS256, ...) 一致。"""
    token, _ = _signer().sign_ds_callback(
        auth.DS_TYPE_BATTLE, "", 5, _dt.timedelta(minutes=1)
    )
    header = pyjwt.get_unverified_header(token)
    assert header["alg"] == auth.ALGORITHM == "HS256"
    assert header["typ"] == "JWT"


# ── ⑤ 时间:exp/iat 单位与 TTL 语义 ────────────────────────────────────────


def test_iat_exp_are_seconds_and_exp_ms_is_millis() -> None:
    """★ 变异:返回值改成 `int(exp.timestamp())`(秒)→ 本条红。

    令牌内 exp/iat 是**秒**(JWT NumericDate),返回给调用方的 exp_ms 是**毫秒**
    (对齐 Go 的 exp.UnixMilli())。两者混淆会让续期判据算出 1970 年附近的时刻:
    要么每次心跳都重签,要么永不重签。
    """
    # 锚在"刚刚"而不是某个写死的日历时刻:写死的未来时刻会让 iat 落在 now 之后
    # (PyJWT 直接判 ImmatureSignature),写死的过去时刻会让 exp 早已过期。
    fixed = _dt.datetime.now(_dt.UTC).replace(microsecond=0) - _dt.timedelta(seconds=1)
    token, exp_ms = _signer(now_fn=lambda: fixed).sign_ds_callback(
        auth.DS_TYPE_BATTLE, "", 5, _dt.timedelta(hours=4)
    )
    payload = _payload(token)

    assert payload["iat"] == int(fixed.timestamp())
    assert payload["exp"] == int((fixed + _dt.timedelta(hours=4)).timestamp())
    assert exp_ms == payload["exp"] * 1000


def test_expired_token_is_rejected_by_verifier() -> None:
    """TTL 到点后校验侧必须判过期(而不是"非法")—— 两者客户端处置相反。"""
    past = _dt.datetime.now(_dt.UTC) - _dt.timedelta(hours=2)
    token, _ = _signer(now_fn=lambda: past).sign_ds_callback(
        auth.DS_TYPE_BATTLE, "", 5, _dt.timedelta(minutes=1)
    )
    with pytest.raises(ValueError, match="token expired"):
        _verifier().verify(token)


# ── ⑥ 参数约束(与 Go SignDSCallbackWithGen 逐条同)──────────────────────


def test_battle_requires_match_id() -> None:
    """★ 变异:删掉 `if match_id == 0` 分支 → 本条红。

    battle 令牌的授权范围**就是** match_id;缺了它,令牌等于"任意对局通行证"。
    """
    with pytest.raises(auth.TokenError, match="battle token requires matchID"):
        _signer().sign_ds_callback(auth.DS_TYPE_BATTLE, "", 0, _dt.timedelta(hours=4))


def test_hub_requires_pod() -> None:
    with pytest.raises(auth.TokenError, match="hub token requires pod"):
        _signer().sign_ds_callback(auth.DS_TYPE_HUB, "", 0, _dt.timedelta(hours=24))


def test_hub_must_not_carry_match_id() -> None:
    """★ 变异:删掉 `if match_id != 0` 分支 → 本条红。"""
    with pytest.raises(auth.TokenError, match="hub token must not carry matchID"):
        _signer().sign_ds_callback(auth.DS_TYPE_HUB, "hub-pod-1", 9, _dt.timedelta(hours=24))


@pytest.mark.parametrize("bad", ["", "Hub", "battles", "HUB", "ds"])
def test_invalid_ds_type_is_rejected(bad: str) -> None:
    """★ 变异:把 else 分支改成 `pass`(未知 ds_type 照签)→ 本条红。

    ds_type 是跨语言词表:签成 "Hub" 的令牌在校验侧永远范围不匹配,而 permissive
    档只 warn 放行 —— 要到切 enforce 那天才全线拒。
    """
    with pytest.raises(auth.TokenError, match="invalid dsType"):
        _signer().sign_ds_callback(bad, "pod-1", 1, _dt.timedelta(hours=1))


@pytest.mark.parametrize(
    "ttl", [_dt.timedelta(0), _dt.timedelta(seconds=-1), _dt.timedelta(hours=-4)]
)
def test_non_positive_ttl_is_rejected(ttl: _dt.timedelta) -> None:
    """★ 变异:`ttl <= timedelta(0)` 改成 `< timedelta(0)` → ttl=0 这格红。

    ttl=0 会签出"生下来就过期"的令牌:DS 拿到手即全线 401,而签发侧毫无信号。
    """
    with pytest.raises(auth.TokenError, match="ttl must be > 0"):
        _signer().sign_ds_callback(auth.DS_TYPE_BATTLE, "", 5, ttl)


# ── ⑦ uint64 边界(Go 由类型系统免费提供,Python 必须手判)────────────────


@pytest.mark.parametrize("bad", [-1, -(1 << 63), 1 << 64, (1 << 64) + 1])
def test_match_id_out_of_uint64_range_is_rejected(bad: int) -> None:
    """★ 变异:删掉 `_check_uint64("match_id", ...)` → 本条红。

    -1 会被 JSON 原样签进令牌,校验侧 int() 解出 -1,范围校验静默失配 ——
    只在那一场对局上发生,且日志全绿。
    """
    with pytest.raises(auth.TokenError, match="超出 uint64 范围"):
        _signer().sign_ds_callback(auth.DS_TYPE_BATTLE, "", bad, _dt.timedelta(hours=4))


@pytest.mark.parametrize("bad", [-1, 1 << 64])
def test_gen_out_of_uint64_range_is_rejected(bad: int) -> None:
    with pytest.raises(auth.TokenError, match="超出 uint64 范围"):
        _signer().sign_ds_callback_with_gen(
            auth.DS_TYPE_HUB, "hub-pod-1", 0, bad, _dt.timedelta(hours=24)
        )


def test_uint64_max_is_accepted() -> None:
    """上界是**闭区间**:2**64-1 必须签得出来(写成 `>=` 会砍掉一个合法值)。"""
    token, _ = _signer().sign_ds_callback(
        auth.DS_TYPE_BATTLE, "", auth.UINT64_MAX, _dt.timedelta(hours=4)
    )
    assert _verifier().verify(token).match_id == auth.UINT64_MAX


def test_bool_match_id_is_rejected() -> None:
    """★ 变异:去掉 `isinstance(value, bool)` 判 → 本条红。

    bool 是 int 的子类,`match_id=True` 会静默签成 match_id=1 —— 一个错配的授权范围
    被签成完全合法的令牌。
    """
    with pytest.raises(auth.TokenError, match="必须是 int"):
        _signer().sign_ds_callback(auth.DS_TYPE_BATTLE, "", True, _dt.timedelta(hours=4))


# ── ⑧ 域隔离:DSCallbackSigner 拒绝非 DS 回调面的 iss/aud ──────────────────


@pytest.mark.parametrize(
    ("issuer", "audience"),
    [
        ("pandora-login", auth.DS_CALLBACK_AUDIENCE),
        (auth.DS_CALLBACK_ISSUER, "pandora-client"),
        ("pandora-login", "pandora-client"),
    ],
)
def test_ds_callback_signer_rejects_foreign_domain(issuer: str, audience: str) -> None:
    """★ 变异:把构造期的 iss/aud 闸删掉 → 本条红。

    这道闸把"拿玩家面配置签 DS 回调令牌"从运行期约定升级成构造期失败。少了它,
    一张 aud=pandora-client 的令牌会被 Envoy 的玩家态 provider 接受,而它的 sub 是
    **pod 名**(battle 令牌甚至没有 sub)。
    """
    with pytest.raises(auth.TokenError, match="DS callback signer requires"):
        _signer(issuer=issuer, audience=audience)


# ── ⑨ signer_from_conf(与 guard_from_conf 对称)──────────────────────────


def _conf(**kw):  # noqa: ANN202
    from pandorapy.services.ds_allocator import conf as dconf

    cfg = dconf.DSAuthConf(**kw)
    cfg.apply_defaults()
    return cfg


def test_signer_from_conf_returns_none_without_secret() -> None:
    """secret 未配 → None(本服务不签发),对应 Go 的 `(nil, nil)`。"""
    assert dsauth.signer_from_conf(_conf()) is None


def test_signer_from_conf_signs_with_conf_defaults() -> None:
    """★ 变异:`signer_from_conf` 里 issuer/audience 换成写死的玩家面值 → 本条红。"""
    signer = dsauth.signer_from_conf(_conf(secret=SECRET))
    assert signer is not None
    token, _ = signer.sign_ds_callback(
        auth.DS_TYPE_BATTLE, "", 4242, _dt.timedelta(hours=4)
    )
    assert _verifier().verify(token).match_id == 4242


def test_signer_from_conf_ignores_mode() -> None:
    """签发看 secret、校验看 mode —— 两个判据必须分开。

    ★ 变异:给 `signer_from_conf` 加上 `if parse_mode(cfg.mode) is Mode.OFF: return None`
      → 本条红。灰度期 mode=off 时若不签发,把 mode 切成 enforce 的那一刻全部 DS
      手上没有令牌 → 成批被拒。
    """
    assert dsauth.signer_from_conf(_conf(mode="off", secret=SECRET)) is not None


def test_signer_from_conf_never_signs_with_additional_secret() -> None:
    """★ 变异:把 additional_secrets 也传进 SignerConfig 并用它签 → 本条红(理论上)。

    这里的可验证形式是:签出的令牌必须能被**只持主密钥**的校验器验过,
    即签发只用主密钥。备用密钥只用于校验,签发侧一旦接受旧密钥,三段式轮换的
    第二段(主密钥已翻新、旧密钥待退役)就永远走不完 —— 旧密钥被无限续命。
    """
    signer = dsauth.signer_from_conf(
        _conf(secret=SECRET, additional_secrets=[OTHER_SECRET])
    )
    assert signer is not None
    token, _ = signer.sign_ds_callback(
        auth.DS_TYPE_BATTLE, "", 1, _dt.timedelta(hours=4)
    )
    # 只持主密钥的校验器能验过 ⇒ 签发用的是主密钥。
    assert _verifier().verify(token).match_id == 1
    # 反向:只持 OTHER_SECRET 的校验器必须验不过。
    other = dsauth.DSCallbackVerifier(
        issuer=auth.DS_CALLBACK_ISSUER,
        audience=auth.DS_CALLBACK_AUDIENCE,
        secret=OTHER_SECRET,
        additional_secrets=[],
    )
    with pytest.raises(ValueError, match="token invalid"):
        other.verify(token)


def test_rotation_window_verifier_accepts_new_key() -> None:
    """轮换共存窗口:主密钥翻新后,持 [新, 旧] 的校验器仍能验过新密钥签的令牌。

    ★ 变异:DSCallbackVerifier 里去掉 additional 循环 → 本条红。
      (该逻辑属既有校验侧,本条只做回归钉子,不改它。)
    """
    signer = dsauth.signer_from_conf(_conf(secret=OTHER_SECRET))
    assert signer is not None
    token, _ = signer.sign_ds_callback(
        auth.DS_TYPE_BATTLE, "", 8, _dt.timedelta(hours=4)
    )
    assert _verifier(additional=[OTHER_SECRET]).verify(token).match_id == 8


def test_short_secret_is_rejected_at_signer_construction() -> None:
    """HS256 密钥下限 32 字节 —— 签发侧与校验侧同一道闸。"""
    with pytest.raises(auth.TokenError, match="secret 太短"):
        _signer(secret="too-short")
