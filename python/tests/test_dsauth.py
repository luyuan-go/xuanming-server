"""DS 回调令牌守卫 —— 判定表逐格验证。

这是"挂在 DS 面(:8444)上的方法"的唯一身份证明。判定表写错的后果分两个方向,都很糟:
  放松 → 任何能到达 DS 网段的东西都能按任意 player_id 捞数据;
  收紧 → 正常客户端(带玩家 JWT 走 :8443)被一起拒掉。
"""

from __future__ import annotations

import datetime as _dt

import jwt as pyjwt
import pytest

from pandorapy import dsauth, errcode

SECRET = "pandora-dev-jwt-secret-change-me-32!"
OTHER_SECRET = "another-dev-secret-at-least-32-bytes!!"


class Ctx:
    def __init__(self, **headers: str) -> None:
        self._md = tuple(headers.items())

    def invocation_metadata(self):  # noqa: ANN201
        return self._md


def _token(secret: str = SECRET, **claims) -> str:
    base = {
        "iss": "pandora-ds-control",
        "aud": ["pandora-ds"],
        "sub": "hub-pod-1",
        "ds_type": "hub",
        "exp": int((_dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(hours=1)).timestamp()),
        "jti": "j1",
    }
    base.update(claims)
    return pyjwt.encode(base, secret, algorithm="HS256")


def _verifier(additional: list[str] | None = None) -> dsauth.DSCallbackVerifier:
    return dsauth.DSCallbackVerifier(
        issuer="pandora-ds-control",
        audience="pandora-ds",
        secret=SECRET,
        additional_secrets=additional or [],
    )


def _guard(mode: dsauth.Mode, additional: list[str] | None = None) -> dsauth.DSCallbackGuard:
    return dsauth.DSCallbackGuard(_verifier(additional), mode)


# ── mode 解析 ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "expect"),
    [
        ("", dsauth.Mode.OFF),
        ("off", dsauth.Mode.OFF),
        ("  PERMISSIVE ", dsauth.Mode.PERMISSIVE),
        ("enforce", dsauth.Mode.ENFORCE),
    ],
)
def test_parse_mode(raw: str, expect: dsauth.Mode) -> None:
    assert dsauth.parse_mode(raw) is expect


def test_parse_mode_rejects_typo() -> None:
    """★ 拼错必须报错。静默回落 off 会让安全门整个不生效,而 yaml 上写着 enforce。"""
    with pytest.raises(ValueError):
        dsauth.parse_mode("enfroce")


def test_guard_from_conf_requires_secret_when_enabled() -> None:
    from pandorapy.services.player import conf as pconf

    cfg = pconf.DSAuthConf(mode="enforce")
    cfg.apply_defaults()
    with pytest.raises(ValueError, match="requires ds_auth.secret"):
        dsauth.guard_from_conf(cfg)


def test_guard_from_conf_off_returns_none() -> None:
    from pandorapy.services.player import conf as pconf

    cfg = pconf.DSAuthConf()
    cfg.apply_defaults()
    assert dsauth.guard_from_conf(cfg) is None


def test_short_secret_is_rejected() -> None:
    """HS256 密钥下限与 Go 同为 32 字节 —— 放松它等于允许一把能被离线爆破的密钥。"""
    with pytest.raises(ValueError):
        dsauth.DSCallbackVerifier(
            issuer="i", audience="a", secret="too-short", additional_secrets=[]
        )


def test_empty_additional_secret_is_rejected() -> None:
    """空串条目 = 轮换清单少写了一把却留了占位。静默过滤会让轮换断档且无人察觉。"""
    with pytest.raises(ValueError, match="additional_secrets"):
        dsauth.DSCallbackVerifier(
            issuer="i", audience="pandora-ds", secret=SECRET, additional_secrets=[""]
        )


# ── 判定表 ───────────────────────────────────────────────────────────────────


def test_off_mode_passes_everything() -> None:
    guard = dsauth.DSCallbackGuard(None, dsauth.Mode.OFF)
    assert guard.check(Ctx(), dsauth.DSScope(require_token=True)) == 0


def test_gateway_without_token_is_unauthorized() -> None:
    guard = _guard(dsauth.Mode.ENFORCE)
    ctx = Ctx(**{dsauth.METADATA_KEY_DS_GATEWAY: "1"})
    assert guard.check(ctx, dsauth.DSScope()) == errcode.ErrUnauthorized


def test_internal_call_without_token_passes_unless_require_token() -> None:
    """东西向内部调用(不带标记头、不带令牌)默认放行;require_token 时才拒。"""
    guard = _guard(dsauth.Mode.ENFORCE)
    assert guard.check(Ctx(), dsauth.DSScope()) == 0
    assert guard.check(Ctx(), dsauth.DSScope(require_token=True)) == errcode.ErrUnauthorized


def test_valid_token_passes() -> None:
    guard = _guard(dsauth.Mode.ENFORCE)
    ctx = Ctx(authorization=f"Bearer {_token()}")
    assert guard.check(ctx, dsauth.DSScope(require_token=True)) == 0


def test_bearer_prefix_is_case_insensitive() -> None:
    guard = _guard(dsauth.Mode.ENFORCE)
    ctx = Ctx(authorization=f"bEaReR {_token()}")
    assert guard.check(ctx, dsauth.DSScope(require_token=True)) == 0


def test_non_bearer_authorization_counts_as_no_token() -> None:
    """玩家面用的可能是别的 scheme;非 Bearer 一律当"没带 DS 令牌"处理。"""
    guard = _guard(dsauth.Mode.ENFORCE)
    ctx = Ctx(authorization="Basic abc")
    assert guard.check(ctx, dsauth.DSScope(require_token=True)) == errcode.ErrUnauthorized


def test_wrong_secret_is_unauthorized() -> None:
    guard = _guard(dsauth.Mode.ENFORCE)
    ctx = Ctx(authorization=f"Bearer {_token(secret=OTHER_SECRET)}")
    assert guard.check(ctx, dsauth.DSScope(require_token=True)) == errcode.ErrUnauthorized


def test_additional_secret_enables_rotation_without_downtime() -> None:
    """★ 三段式轮换的全部机制:additional 只用于校验、不用于签发。

    少了它,轮换中间那一段会出现"新副本签的令牌旧副本验不过"的 401 断档。
    """
    guard = _guard(dsauth.Mode.ENFORCE, additional=[OTHER_SECRET])
    ctx = Ctx(authorization=f"Bearer {_token(secret=OTHER_SECRET)}")
    assert guard.check(ctx, dsauth.DSScope(require_token=True)) == 0


def test_expired_token_is_unauthorized() -> None:
    guard = _guard(dsauth.Mode.ENFORCE)
    past = int((_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(minutes=1)).timestamp())
    ctx = Ctx(authorization=f"Bearer {_token(exp=past)}")
    assert guard.check(ctx, dsauth.DSScope(require_token=True)) == errcode.ErrUnauthorized


def test_wrong_audience_is_unauthorized() -> None:
    """★ 玩家 SessionToken(aud=pandora-client)绝不能通过 DS 回调校验(域隔离)。"""
    guard = _guard(dsauth.Mode.ENFORCE)
    ctx = Ctx(authorization=f"Bearer {_token(aud=['pandora-client'])}")
    assert guard.check(ctx, dsauth.DSScope(require_token=True)) == errcode.ErrUnauthorized


def test_scope_mismatches_are_permission_deny() -> None:
    """范围不匹配是**越权**(PERMISSION_DENY),不是**没鉴权**(UNAUTHORIZED)。"""
    guard = _guard(dsauth.Mode.ENFORCE)
    ctx = Ctx(authorization=f"Bearer {_token()}")
    assert (
        guard.check(ctx, dsauth.DSScope(ds_type="battle", require_token=True))
        == errcode.ErrPermissionDeny
    )
    assert (
        guard.check(ctx, dsauth.DSScope(pod="other-pod", require_token=True))
        == errcode.ErrPermissionDeny
    )
    assert (
        guard.check(ctx, dsauth.DSScope(match_id=999, require_token=True))
        == errcode.ErrPermissionDeny
    )


def test_deny_ds_rejects_gateway_and_token_calls() -> None:
    """deny_ds:这种调用形态根本不该来自 DS。"""
    guard = _guard(dsauth.Mode.ENFORCE)
    assert (
        guard.check(
            Ctx(**{dsauth.METADATA_KEY_DS_GATEWAY: "1"}), dsauth.DSScope(deny_ds=True)
        )
        == errcode.ErrPermissionDeny
    )
    assert (
        guard.check(Ctx(authorization=f"Bearer {_token()}"), dsauth.DSScope(deny_ds=True))
        == errcode.ErrPermissionDeny
    )
    # 纯内部调用不受影响。
    assert guard.check(Ctx(), dsauth.DSScope(deny_ds=True)) == 0


def test_permissive_downgrades_every_rejection_to_pass() -> None:
    """★ permissive 跑**完整**验签路径,失败只 warn 放行 —— 观察期不改变现有行为。"""
    guard = _guard(dsauth.Mode.PERMISSIVE)
    assert guard.check(Ctx(), dsauth.DSScope(require_token=True)) == 0
    assert (
        guard.check(
            Ctx(authorization=f"Bearer {_token(secret=OTHER_SECRET)}"),
            dsauth.DSScope(require_token=True),
        )
        == 0
    )


def test_enabled_mode_requires_verifier() -> None:
    with pytest.raises(ValueError, match="requires verifier"):
        dsauth.DSCallbackGuard(None, dsauth.Mode.ENFORCE)
