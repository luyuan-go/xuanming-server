"""JWT 签发测试。

两条坑做成机械可测:
  1. ★ 账号态与玩家态 audience 严格分离 —— 相同则拒绝启动
  2. ★ 经 Envoy 的 token 绝不能设 kid(Envoy 的 kid 是字面量,带指纹全线 401)
"""

from __future__ import annotations

import datetime as _dt

import jwt as pyjwt
import pytest

from pandorapy import auth


def _cfg(**kw) -> auth.SignerConfig:
    base = dict(
        secret=b"test-secret-0123456789-abcdefghijk",  # >= 32 字节(HS256 硬要求)
        issuer="pandora-login",
        audience="pandora-client",
        account_audience="pandora-account",
        key_fingerprint="fp-abcdef123456",
    )
    base.update(kw)
    return auth.SignerConfig(**base)


def _signer(**kw) -> auth.Signer:
    return auth.Signer(_cfg(**kw))


# ── ★ ① audience 严格分离 ─────────────────────────────────────────────────


def test_same_audience_is_rejected_at_startup() -> None:
    """★ 两个受众相同必须**拒绝启动**。

    这是越权的直接入口,而它在运行期**没有任何信号** ——
    两种 token 都能通过校验,而账号态 token 的 sub 里装的是 account_id,
    下游把它当 player_id 去查数据。account_id 和 player_id 都是 snowflake,
    长得一模一样,查出来的是**别人的数据**。
    """
    with pytest.raises(auth.TokenError, match="不能与 audience 相同"):
        auth.Signer(_cfg(account_audience="pandora-client"))


def test_account_token_has_account_audience() -> None:
    s = _signer()
    token, _ = s.sign_account(account_id=1001)
    claims = pyjwt.decode(token, options={"verify_signature": False})
    assert claims["aud"] == ["pandora-account"]
    assert claims["sub"] == "1001"


def test_session_token_has_player_audience() -> None:
    s = _signer()
    token, _ = s.sign_session(player_id=2002)
    claims = pyjwt.decode(token, options={"verify_signature": False})
    assert claims["aud"] == ["pandora-client"]
    assert claims["sub"] == "2002"


def test_account_token_rejected_on_player_audience() -> None:
    """★ 这是分离的**实际效果**:账号态 token 在玩家面验不过。

    没有这条保护的话,一个只该解锁 ListAccountRoles / EnterRole 的 token
    能调用任意玩家面接口。
    """
    s = _signer()
    token, _ = s.sign_account(account_id=1001)
    with pytest.raises(auth.TokenError):
        s.verify(token, expect_audience="pandora-client")
    # 在自己的受众下正常
    claims = s.verify(token, expect_audience="pandora-account")
    assert claims["sub"] == "1001"


def test_session_token_rejected_on_account_audience() -> None:
    """反向也要挡住:玩家态 token 不该被账号面接受。"""
    s = _signer()
    token, _ = s.sign_session(player_id=2002)
    with pytest.raises(auth.TokenError):
        s.verify(token, expect_audience="pandora-account")


def test_verify_requires_explicit_audience() -> None:
    """★ `expect_audience` 没有默认值 —— 调用方必须显式说明期望哪一类。

    给默认值就等于让账号态 token 在玩家面被接受。
    """
    import inspect

    sig = inspect.signature(auth.Signer.verify)
    param = sig.parameters["expect_audience"]
    assert param.default is inspect.Parameter.empty, "expect_audience 有默认值了"
    assert param.kind is inspect.Parameter.KEYWORD_ONLY, "应当是 keyword-only,防位置传错"


# ── ★ ② 经 Envoy 的 token 不设 kid ────────────────────────────────────────


def test_session_token_has_no_kid_header() -> None:
    """★ envoy.yaml 的 local_jwks 里 kid 是字面量 "pandora-dev",不是指纹。

    签发时写进真实指纹 → Envoy 按 kid 精确找 key、找不到就拒 → **全线 401**。
    这个错误没有任何运行期信号会提醒你,设错了就是登录全挂。
    """
    s = _signer()
    token, _ = s.sign_session(player_id=2002)
    headers = pyjwt.get_unverified_header(token)
    assert "kid" not in headers, f"SessionToken 带了 kid:{headers}"


def test_account_token_has_no_kid_header() -> None:
    """账号态同样经 Envoy 校验 → 同样不设 kid。"""
    s = _signer()
    token, _ = s.sign_account(account_id=1001)
    headers = pyjwt.get_unverified_header(token)
    assert "kid" not in headers, f"AccountToken 带了 kid:{headers}"


def test_internal_token_does_set_kid() -> None:
    """★ 对比:只在服务端验签的 token **要**设 kid(便于密钥轮换)。

    这个区别是本模块最容易搞混的地方 —— 两类都是"我们签的 token",
    但一类经 Envoy、一类不经。
    """
    s = _signer()
    token, _ = s.sign_internal(
        subject="ds-pod-1", audience="pandora-ds", ttl=_dt.timedelta(minutes=2)
    )
    headers = pyjwt.get_unverified_header(token)
    assert headers.get("kid") == "fp-abcdef123456"


def test_internal_token_without_fingerprint_omits_kid() -> None:
    """没配指纹时不写空 kid(空 kid 比不写更糟 —— 验签方会去找一把叫 "" 的 key)。"""
    s = _signer(key_fingerprint="")
    token, _ = s.sign_internal(
        subject="x", audience="y", ttl=_dt.timedelta(minutes=1)
    )
    assert "kid" not in pyjwt.get_unverified_header(token)


# ── 必填校验 ────────────────────────────────────────────────────────────────


def test_zero_ids_rejected() -> None:
    s = _signer()
    with pytest.raises(auth.TokenError, match="player_id"):
        s.sign_session(player_id=0)
    with pytest.raises(auth.TokenError, match="account_id"):
        s.sign_account(account_id=0)


def test_short_secret_rejected() -> None:
    """★ HS256 密钥必须 >= 32 字节(RFC 7518 §3.2),与 Go 侧同一道闸。

    我第一版**漏了这条** —— 是 PyJWT 的 InsecureKeyLengthWarning 逼出来的。
    短密钥不会让任何东西报错,只是让签名可被暴力破解:
    伪造一个 sub=任意 player_id 的 token 就能冒充任何玩家。
    """
    with pytest.raises(auth.TokenError, match="secret"):
        auth.Signer(_cfg(secret=b""))
    with pytest.raises(auth.TokenError, match="太短"):
        auth.Signer(_cfg(secret=b"short"))
    with pytest.raises(auth.TokenError, match="太短"):
        auth.Signer(_cfg(secret=b"a" * 31))  # 差一个字节也不行
    auth.Signer(_cfg(secret=b"a" * 32))  # 恰好 32 放行


def test_min_secret_bytes_matches_go() -> None:
    assert auth.MIN_SECRET_BYTES == 32


def test_empty_issuer_rejected() -> None:
    with pytest.raises(auth.TokenError, match="issuer"):
        auth.Signer(_cfg(issuer=""))


def test_missing_audience_rejected() -> None:
    with pytest.raises(auth.TokenError, match="audience"):
        auth.Signer(_cfg(audience=""))
    with pytest.raises(auth.TokenError, match="audience"):
        auth.Signer(_cfg(account_audience=""))


# ── claims 形状 ─────────────────────────────────────────────────────────────


def test_jti_is_always_present_and_unique() -> None:
    """jti 是**唯一的吊销手段**(B1 纯本地验票),不能缺、不能重。"""
    s = _signer()
    jtis = set()
    for _ in range(20):
        token, _ = s.sign_session(player_id=1)
        jtis.add(pyjwt.decode(token, options={"verify_signature": False})["jti"])
    assert len(jtis) == 20


def test_aud_is_array_like_go_claimstrings() -> None:
    """aud 用数组形式 —— 与 Go 的 jwt.ClaimStrings 一致。

    写成裸字符串时多数验签器也认,但 Envoy 的行为随版本不同,
    保持与 Go 完全一致最安全。
    """
    s = _signer()
    token, _ = s.sign_session(player_id=1)
    claims = pyjwt.decode(token, options={"verify_signature": False})
    assert isinstance(claims["aud"], list)


def test_expiry_matches_configured_ttl() -> None:
    fixed = _dt.datetime(2026, 8, 18, 12, 0, 0, tzinfo=_dt.UTC)
    s = auth.Signer(
        _cfg(session_ttl=_dt.timedelta(hours=2), account_ttl=_dt.timedelta(minutes=10)),
        now_fn=lambda: fixed,
    )
    _, exp_ms = s.sign_session(player_id=1)
    assert exp_ms == int((fixed + _dt.timedelta(hours=2)).timestamp() * 1000)
    _, acc_exp_ms = s.sign_account(account_id=1)
    assert acc_exp_ms == int((fixed + _dt.timedelta(minutes=10)).timestamp() * 1000)


def test_account_ttl_is_shorter_than_session() -> None:
    """账号态是**短 TTL** —— 它只用于选角那几秒,不该长期有效。"""
    cfg = _cfg()
    assert cfg.account_ttl < cfg.session_ttl


def test_expired_token_is_rejected() -> None:
    past = _dt.datetime(2020, 1, 1, tzinfo=_dt.UTC)
    s = auth.Signer(_cfg(), now_fn=lambda: past)
    token, _ = s.sign_session(player_id=1)
    live = _signer()  # 用当前时间验
    with pytest.raises(auth.TokenError):
        live.verify(token, expect_audience="pandora-client")


def test_wrong_issuer_is_rejected() -> None:
    other = auth.Signer(_cfg(issuer="someone-else"))
    token, _ = other.sign_session(player_id=1)
    with pytest.raises(auth.TokenError):
        _signer().verify(token, expect_audience="pandora-client")


def test_tampered_signature_is_rejected() -> None:
    s = _signer()
    token, _ = s.sign_session(player_id=1)
    forged = auth.Signer(_cfg(secret=b"another-secret-0000000000000000000")).sign_session(player_id=1)[0]
    with pytest.raises(auth.TokenError):
        s.verify(forged, expect_audience="pandora-client")


# ── ★ TTL 下限:签出来就过期的 token ────────────────────────────────────────


@pytest.mark.parametrize("field", ["session_ttl", "account_ttl"])
def test_sub_second_ttl_is_rejected_at_startup(field: str) -> None:
    """★ TTL < 1s 必须**启动期拒绝**。

    JWT 的 NumericDate 以**秒**为粒度 —— 0.5s 的 TTL 在签发时就被截断成"已过期",
    签出来的 token 生下来就是坏的。而配置层没有任何信号,线上表现是
    「登录成功但立刻 401」,排查方向会被完全带偏(去查 Envoy、查密钥、查时钟)。
    与 Go 的 auth.Config.Validate 同一道闸。
    """
    with pytest.raises(auth.TokenError, match=field):
        _cfg(**{field: _dt.timedelta(milliseconds=500)}).validate()


def test_exactly_one_second_ttl_is_accepted() -> None:
    """边界值 1s 必须放行 —— 闸的判据是 `< 1s`,不是 `<= 1s`。"""
    _cfg(session_ttl=_dt.timedelta(seconds=1)).validate()


# ── ★ 不停服密钥轮换 ────────────────────────────────────────────────────────


def test_rotation_accepts_tokens_signed_by_previous_key() -> None:
    """★ 轮换第二段:主密钥已翻新,旧密钥签的 token 仍必须被接受。

    缺了备用密钥,换密钥只能是"所有副本同一时刻一起换" —— 而滚动更新
    根本做不到同一时刻。共存窗口里旧副本签的 token 被新副本拒 =
    玩家批量掉线,且只在换钥那天发生(§9 不变量 16)。
    """
    old_secret = b"old-secret-0123456789-abcdefghijk"
    new_secret = b"new-secret-0123456789-abcdefghijk"

    old_signer = auth.Signer(_cfg(secret=old_secret))
    token, _ = old_signer.sign_session(player_id=1001)

    # 轮换后的副本:主密钥是新的,备用里留着旧的
    rotated = auth.Signer(_cfg(secret=new_secret, additional_secrets=(old_secret,)))
    assert rotated.verify(token, expect_audience="pandora-client")["sub"] == "1001"

    # 轮换第三段(清空 additional)之后,旧 token 才失效
    finished = auth.Signer(_cfg(secret=new_secret))
    with pytest.raises(auth.TokenInvalidError):
        finished.verify(token, expect_audience="pandora-client")


def test_additional_secret_must_not_duplicate_primary() -> None:
    """备用密钥与主密钥相同 = 轮换配错了(以为在轮换,其实两边同一把)。

    不拦的话轮换"完成"后旧密钥根本没退役,而每一步看起来都成功。
    """
    same = b"test-secret-0123456789-abcdefghijk"
    with pytest.raises(auth.TokenError, match="主密钥"):
        _cfg(secret=same, additional_secrets=(same,)).validate()


def test_additional_secret_must_pass_length_gate() -> None:
    """备用密钥同样要过 32 字节闸 —— 轮换不是绕过安全下限的后门。"""
    with pytest.raises(auth.TokenError, match="additional_secrets"):
        _cfg(additional_secrets=(b"short",)).validate()


# ── ★ 过期 vs 非法:两种错误码不能塌成一个 ──────────────────────────────────


def test_expired_and_invalid_map_to_distinct_go_errcodes() -> None:
    """★ 过期与非法必须是**不同的业务码**,且与 Go 侧取同一个值。

    两者的客户端处置完全相反:
        过期 → 静默重新登录,属正常生命周期
        非法 → 凭据被篡改或密钥配错,要提示并**停止重试**

    塌成一个(原实现是裸 RuntimeError → as_code 得到 ErrUnknown)之后,
    客户端只能一律重试 —— 密钥配错那天会变成全量客户端重试风暴。
    """
    from pandorapy import errcode

    signer = _signer()
    token, _ = signer.sign_session(player_id=1001)

    with pytest.raises(auth.TokenInvalidError) as bad:
        signer.verify(token[:-4] + "aaaa", expect_audience="pandora-client")
    assert errcode.as_code(bad.value) == errcode.ErrLoginTicketInvalid

    expired = auth.Signer(_cfg(session_ttl=_dt.timedelta(seconds=1)))
    stale, _ = expired.sign_session(player_id=1001)
    # 直接构造一个 exp 已过去的 token,避免用例真的等 1 秒
    payload = pyjwt.decode(
        stale,
        expired._cfg.secret,  # noqa: SLF001
        algorithms=["HS256"],
        audience="pandora-client",
        issuer="pandora-login",
    )
    payload["exp"] = payload["iat"] - 1
    forged = pyjwt.encode(payload, expired._cfg.secret, algorithm="HS256")  # noqa: SLF001
    with pytest.raises(auth.TokenExpiredError) as old:
        expired.verify(forged, expect_audience="pandora-client")
    assert errcode.as_code(old.value) == errcode.ErrLoginTicketExpired
    assert errcode.ErrLoginTicketExpired != errcode.ErrLoginTicketInvalid


def test_expired_token_is_not_retried_against_every_key() -> None:
    """★ 已过期的 token 不得拿去逐把备用密钥重试。

    过期与"这把密钥不对"是两回事:重试白费,而且会把真实原因(过期)
    掩盖成最后一把密钥的签名错误。
    """
    signer = auth.Signer(
        _cfg(
            session_ttl=_dt.timedelta(seconds=1),
            additional_secrets=(b"other-secret-0123456789-abcdefgh",),
        )
    )
    token, _ = signer.sign_session(player_id=1001)
    payload = pyjwt.decode(
        token,
        signer._cfg.secret,  # noqa: SLF001
        algorithms=["HS256"],
        audience="pandora-client",
        issuer="pandora-login",
    )
    payload["exp"] = payload["iat"] - 1
    forged = pyjwt.encode(payload, signer._cfg.secret, algorithm="HS256")  # noqa: SLF001
    with pytest.raises(auth.TokenExpiredError):
        signer.verify(forged, expect_audience="pandora-client")
