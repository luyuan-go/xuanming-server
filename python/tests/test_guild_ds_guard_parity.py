"""guild 专属 DS 回调守卫与**共享件 `pandorapy/dsauth.py`** 的差异钉子。

为什么单独一个文件:全仓存在**两份** `DSCallbackGuard`,类名、`check` 方法名、
`DSScope` 参数类型名与五个字段完全相同,**失败通道却相反**。单看任一边都是对的,
只有把两边并排断言才能拦住"把 guild 收敛到共享件、调用点一个字不改"这个看起来
最自然的清理动作 —— 那一下会让门当场空转(无令牌东西向直连拿到权威 guild_id,
而日志照旧打拒绝事件名)。

本文件刻意不放进 `test_guild_service.py`:它钉的是**两个模块之间的契约**,
不属于 guild service 的行为。
"""

from __future__ import annotations

import time

import jwt as pyjwt
import pytest
from structlog.testing import capture_logs

from pandorapy import dsauth, errcode
from pandorapy.services.guild import conf as gconf
from pandorapy.services.guild import ds_guard as gds

SECRET = "pandora-dev-jwt-secret-change-me-32!"


class _Ctx:
    """只实现两份守卫都要的 `invocation_metadata()`。"""

    def __init__(self, **headers: str) -> None:
        self._md = tuple(headers.items())

    def invocation_metadata(self):  # noqa: ANN201
        return self._md


def _fork(mode: str) -> gds.DSCallbackGuard:
    cfg = gconf.DSAuthConf(mode=mode, secret=SECRET)
    cfg.apply_defaults()
    guard = gds.new_from_conf(cfg)
    assert guard is not None
    return guard


def _shared(mode: dsauth.Mode) -> dsauth.DSCallbackGuard:
    return dsauth.DSCallbackGuard(
        dsauth.DSCallbackVerifier(
            issuer=gconf.DEFAULT_DS_AUTH_ISSUER,
            audience=gconf.DEFAULT_DS_AUTH_AUDIENCE,
            secret=SECRET,
            additional_secrets=[],
        ),
        mode,
    )


def _ds_token(**over) -> str:  # noqa: ANN003
    now = int(time.time())
    claims = {
        "iss": gconf.DEFAULT_DS_AUTH_ISSUER,
        "sub": "hub-0",
        "aud": [gconf.DEFAULT_DS_AUTH_AUDIENCE],
        "iat": now,
        "exp": now + 600,
        "ds_type": "hub",
    }
    claims.update(over)
    return pyjwt.encode(
        claims, SECRET, algorithm="HS256", headers={"kid": gds.key_fingerprint(SECRET)}
    )


# ── 观察期日志事件名 ─────────────────────────────────────────────────────

def test_permissive_reject_uses_the_permissive_event_name() -> None:
    """★ permissive **放行**时不能打 enforce 的拒绝事件名。

    `guild-dev.yaml:65` 实配就是 `mode: permissive`。这个档的**全部价值**就是让运维看
    "DS 是不是已全量带上令牌、能不能切 enforce"。若放行时也打 `ds_callback_auth_rejected`,
    Loki 上看是拒了、实际把权威 guild_id 给了无令牌调用方,排障方向从第一步就是错的。
    """
    with capture_logs() as logs:
        _fork("permissive").check(_Ctx(), gds.DSScope(require_token=True))
    events = [e["event"] for e in logs]
    assert events == ["ds_callback_auth_permissive_reject"], events


def test_enforce_reject_keeps_the_enforce_event_name_and_code() -> None:
    """enforce 侧不能被上一条修正误伤 —— 它才是真拒绝,且必须带 code。"""
    with capture_logs() as logs:
        with pytest.raises(errcode.PandoraError):
            _fork("enforce").check(_Ctx(), gds.DSScope(require_token=True))
    assert [e["event"] for e in logs] == ["ds_callback_auth_rejected"]
    assert logs[0]["code"] == errcode.ErrUnauthorized


@pytest.mark.parametrize("mode", ["permissive", "enforce"])
def test_fork_event_names_match_the_shared_module(mode: str) -> None:
    """★ 两份守卫在同一情形下必须打**同一个**事件名。

    否则跨服务并排会得出反向结论:team/player/battle_result 有 permissive_reject、
    guild 没有 ⇒ "guild 的 DS 都带令牌了"。Go 侧只有一份
    (`pkg/middleware/dsauth.go::reject`),本来不存在这个问题。
    """
    shared_mode = dsauth.Mode.PERMISSIVE if mode == "permissive" else dsauth.Mode.ENFORCE
    with capture_logs() as fork_logs:
        try:
            _fork(mode).check(_Ctx(), gds.DSScope(require_token=True))
        except errcode.PandoraError:
            pass
    with capture_logs() as shared_logs:
        _shared(shared_mode).check(_Ctx(), dsauth.DSScope(require_token=True))
    assert [e["event"] for e in fork_logs] == [e["event"] for e in shared_logs]


# ── 失败通道相反 ─────────────────────────────────────────────────────────

def test_two_guards_have_opposite_failure_channels() -> None:
    """★ 把"不可互换"钉成可执行断言。

    共享件 `check` 返回 in-band 错误码且**从不抛**(三个调用点 battle_result /
    player / team 都不在 try 里,抛了就逃出 handler → 客户端收到 UNKNOWN);
    guild fork enforce 档**抛 `PandoraError`**(调用点包在 try 里)。
    两者互换后语法完全合法、import 就能过 —— 只有这条断言能拦住。
    """
    code = _shared(dsauth.Mode.ENFORCE).check(_Ctx(), dsauth.DSScope(require_token=True))
    assert code == errcode.ErrUnauthorized, "共享件改成抛异常会打穿三个不在 try 里的调用点"

    with pytest.raises(errcode.PandoraError):
        _fork("enforce").check(_Ctx(), gds.DSScope(require_token=True))


def test_valid_token_passes_both_guards() -> None:
    """正向基线:带合法令牌时两边都放行、都不打拒绝日志。"""
    ctx = _Ctx(authorization=f"Bearer {_ds_token()}")
    with capture_logs() as logs:
        assert _shared(dsauth.Mode.ENFORCE).check(ctx, dsauth.DSScope(require_token=True)) == 0
        _fork("enforce").check(ctx, gds.DSScope(require_token=True))
    assert logs == []
