"""申请展示跨服务鉴权的调用方隔离与配置闭包。"""

from __future__ import annotations

import pathlib

import pytest

from pandorapy import config as pconfig
from pandorapy import internalrpcauth
from pandorapy.services.friend import conf as fconf
from pandorapy.services.guild import conf as gconf
from pandorapy.services.login import conf as lconf
from pandorapy.services.player import conf as pconf


TEAM_NAME_SECRET = "team-name-internal-rpc-secret-00000001"
FRIEND_NAME_SECRET = "friend-name-internal-rpc-secret-00001"
GUILD_NAME_SECRET = "guild-name-internal-rpc-secret-000001"
TEAM_NO_SECRET = "team-number-internal-rpc-secret-000001"
FRIEND_NO_SECRET = "friend-number-internal-rpc-secret-0001"
GUILD_NO_SECRET = "guild-number-internal-rpc-secret-00001"
REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


class _ReplayStore:
    def __init__(self) -> None:
        self.consumed: set[str] = set()
        self.calls = 0

    async def consume(self, nonce_key: str, _ttl_sec: float) -> bool:
        self.calls += 1
        if nonce_key in self.consumed:
            return False
        self.consumed.add(nonce_key)
        return True


def _verifier(secret: str, caller: str, audience: str, store: _ReplayStore):
    return internalrpcauth.Verifier(
        secret,
        caller,
        audience,
        30.0,
        store,
        now_ms=lambda: 1_700_000_000_000,
    )


async def test_multi_caller_verifier_dispatches_payload_bound_credentials() -> None:
    store = _ReplayStore()
    team = _verifier(TEAM_NAME_SECRET, "team", "player:name", store)
    friend = _verifier(FRIEND_NAME_SECRET, "friend", "player:name", store)
    multi = internalrpcauth.MultiCallerVerifier(team, friend)
    payload = b"exact protobuf request"
    signer = internalrpcauth.Signer(
        FRIEND_NAME_SECRET,
        "friend",
        "player:name",
        now_ms=lambda: 1_700_000_000_000,
    )
    metadata = dict(
        signer.sign_metadata_with_payload("/pandora.Player/Resolve", 42, payload)
    )

    await multi.verify_with_payload(metadata, "/pandora.Player/Resolve", 42, payload)
    assert store.calls == 1


async def test_unknown_caller_is_rejected_before_nonce_consumption() -> None:
    store = _ReplayStore()
    signer = internalrpcauth.Signer(
        FRIEND_NAME_SECRET,
        "friend",
        "player:name",
        now_ms=lambda: 1_700_000_000_000,
    )
    payload = b"request"
    metadata = dict(signer.sign_metadata_with_payload("/svc/method", 7, payload))

    team_only = internalrpcauth.MultiCallerVerifier(
        _verifier(TEAM_NAME_SECRET, "team", "player:name", store)
    )
    with pytest.raises(internalrpcauth.ErrUnauthorized):
        await team_only.verify_with_payload(metadata, "/svc/method", 7, payload)
    assert store.calls == 0

    # 把正确 caller 加入后，同一份凭证仍应首次消费成功。
    with_friend = internalrpcauth.MultiCallerVerifier(
        _verifier(FRIEND_NAME_SECRET, "friend", "player:name", store)
    )
    await with_friend.verify_with_payload(metadata, "/svc/method", 7, payload)
    assert store.calls == 1


def test_multi_caller_verifier_rejects_empty_and_duplicate_callers() -> None:
    store = _ReplayStore()
    team = _verifier(TEAM_NAME_SECRET, "team", "player:name", store)
    with pytest.raises(ValueError, match="at least one"):
        internalrpcauth.MultiCallerVerifier()
    with pytest.raises(ValueError, match="duplicate"):
        internalrpcauth.MultiCallerVerifier(team, team)
    with pytest.raises(ValueError, match="must not be None"):
        internalrpcauth.MultiCallerVerifier(team, None)


@pytest.mark.parametrize(
    ("config_cls", "section"),
    [(fconf.Config, "friend"), (gconf.Config, "guild")],
)
def test_outbound_display_resolver_credentials_are_complete_and_independent(
    config_cls, section: str
) -> None:
    cfg = config_cls.model_validate(
        {
            section: {
                "player_name_resolver_addr": "127.0.0.1:20002",
                "player_name_resolver_auth_secret": FRIEND_NAME_SECRET,
                "player_no_resolver_addr": "127.0.0.1:20001",
                "player_no_resolver_auth_secret": FRIEND_NO_SECRET,
            }
        }
    )
    cfg.apply_defaults()
    cfg.validate_player_display_resolvers()
    target = getattr(cfg, section)
    assert target.player_name_resolver_auth_audience == "player:name"
    assert target.player_no_resolver_auth_audience == "login:player-no"

    target.player_no_resolver_auth_secret = target.player_name_resolver_auth_secret
    with pytest.raises(ValueError, match="must differ"):
        cfg.validate_player_display_resolvers()


@pytest.mark.parametrize(
    ("config_cls", "section", "overrides"),
    [
        (
            fconf.Config,
            "friend",
            {"player_name_resolver_addr": "127.0.0.1:20002"},
        ),
        (
            gconf.Config,
            "guild",
            {"player_no_resolver_auth_secret": GUILD_NO_SECRET},
        ),
    ],
)
def test_outbound_display_resolver_partial_configuration_is_rejected(
    config_cls, section: str, overrides: dict[str, str]
) -> None:
    cfg = config_cls.model_validate({section: overrides})
    cfg.apply_defaults()
    with pytest.raises(ValueError):
        cfg.validate_player_display_resolvers()


def test_login_accepts_three_callers_but_rejects_reused_player_no_key() -> None:
    cfg = lconf.Config.model_validate(
        {
            "node": {"redis_client": {"host": "127.0.0.1:6379"}},
            "login": {
                "player_no_resolve_auth_secret": TEAM_NO_SECRET,
                "friend_player_no_resolve_auth_secret": FRIEND_NO_SECRET,
                "guild_player_no_resolve_auth_secret": GUILD_NO_SECRET,
            }
        }
    )
    cfg.apply_defaults()
    cfg.validate_conf()
    assert cfg.login.player_no_resolve_auth_audience == "login:player-no"
    assert cfg.login.friend_player_no_resolve_auth_audience == "login:player-no"
    assert cfg.login.guild_player_no_resolve_auth_audience == "login:player-no"

    cfg.login.guild_player_no_resolve_auth_secret = FRIEND_NO_SECRET
    with pytest.raises(ValueError, match="reused"):
        cfg.validate_conf()


def test_player_accepts_three_callers_but_rejects_reused_player_name_key() -> None:
    cfg = pconf.Config.model_validate(
        {
            "node": {"redis_client": {"host": "127.0.0.1:6379"}},
            "player": {
                "player_name_resolve_auth_secret": TEAM_NAME_SECRET,
                "friend_player_name_resolve_auth_secret": FRIEND_NAME_SECRET,
                "guild_player_name_resolve_auth_secret": GUILD_NAME_SECRET,
            }
        }
    )
    cfg.apply_defaults()
    cfg.validate_player_name_resolvers()
    assert cfg.player.player_name_resolve_auth_audience == "player:name"
    assert cfg.player.friend_player_name_resolve_auth_audience == "player:name"
    assert cfg.player.guild_player_name_resolve_auth_audience == "player:name"

    cfg.player.guild_player_name_resolve_auth_secret = FRIEND_NAME_SECRET
    with pytest.raises(ValueError, match="reused"):
        cfg.validate_player_name_resolvers()


@pytest.mark.parametrize(
    ("config_cls", "section", "secret_field", "secret", "validate_name"),
    [
        (
            lconf.Config,
            "login",
            "friend_player_no_resolve_auth_secret",
            FRIEND_NO_SECRET,
            "validate_conf",
        ),
        (
            pconf.Config,
            "player",
            "friend_player_name_resolve_auth_secret",
            FRIEND_NAME_SECRET,
            "validate_player_name_resolvers",
        ),
    ],
)
def test_inbound_display_auth_requires_redis_replay_authority(
    config_cls, section: str, secret_field: str, secret: str, validate_name: str
) -> None:
    cfg = config_cls.model_validate({section: {secret_field: secret}})
    cfg.apply_defaults()
    with pytest.raises(ValueError, match="replay authority"):
        getattr(cfg, validate_name)()


@pytest.mark.parametrize(
    ("domain", "social_rel", "login_rel", "player_rel"),
    [
        (
            "friend",
            "services/social/friend/etc/friend-dev.yaml",
            "services/account/login/etc/login-dev.yaml",
            "services/account/player/etc/player-dev.yaml",
        ),
        (
            "friend",
            "services/social/friend/etc/friend-dev-tidb.yaml",
            "services/account/login/etc/login-dev-tidb.yaml",
            "services/account/player/etc/player-dev.yaml",
        ),
        (
            "friend",
            "services/social/friend/etc/friend-prod.yaml.example",
            "services/account/login/etc/login-prod.yaml.example",
            "services/account/player/etc/player-prod.yaml.example",
        ),
        (
            "guild",
            "services/social/guild/etc/guild-dev.yaml",
            "services/account/login/etc/login-dev.yaml",
            "services/account/player/etc/player-dev.yaml",
        ),
        (
            "guild",
            "services/social/guild/etc/guild-dev-tidb.yaml",
            "services/account/login/etc/login-dev-tidb.yaml",
            "services/account/player/etc/player-dev.yaml",
        ),
        (
            "guild",
            "services/social/guild/etc/guild-prod.yaml.example",
            "services/account/login/etc/login-prod.yaml.example",
            "services/account/player/etc/player-prod.yaml.example",
        ),
    ],
)
def test_dev_tidb_and_prod_examples_share_each_caller_credentials(
    domain: str, social_rel: str, login_rel: str, player_rel: str
) -> None:
    social_cls = fconf.Config if domain == "friend" else gconf.Config
    social = social_cls.load(str(REPO_ROOT / social_rel))
    social.validate_player_display_resolvers()
    login_raw = pconfig.load_yaml(str(REPO_ROOT / login_rel))
    # prod 模板的角色 ID 占位符与本测试无关，替换成合法样值后仍走真实 Config 校验。
    if login_rel.endswith(".example"):
        login_raw["login"]["allowed_role_ids"] = [1, 2]
    login = lconf.Config.model_validate(login_raw)
    login.apply_defaults()
    login.validate_conf()
    player = pconf.Config.load(str(REPO_ROOT / player_rel))
    player.validate_player_name_resolvers()

    social_section = getattr(social, domain)
    assert social_section.player_no_resolver_auth_secret == getattr(
        login.login, f"{domain}_player_no_resolve_auth_secret"
    )
    assert social_section.player_no_resolver_auth_audience == getattr(
        login.login, f"{domain}_player_no_resolve_auth_audience"
    )
    assert social_section.player_name_resolver_auth_secret == getattr(
        player.player, f"{domain}_player_name_resolve_auth_secret"
    )
    assert social_section.player_name_resolver_auth_audience == getattr(
        player.player, f"{domain}_player_name_resolve_auth_audience"
    )


def test_authority_mains_install_shared_multi_caller_verifier() -> None:
    from tests.srcprobe import code_text

    # 只看代码：注释/docstring 里提到这些接线不算数（理由见 tests/srcprobe.py）
    login_main = code_text(REPO_ROOT / "python/pandorapy/services/login/main.py")
    player_main = code_text(REPO_ROOT / "python/pandorapy/services/player/main.py")
    assert "internalrpcauth.MultiCallerVerifier(*verifiers)" in login_main
    assert "internalrpcauth.MultiCallerVerifier(" in player_main
    for caller in ("team", "friend", "guild"):
        assert f'"{caller}"' in login_main
        assert f'"{caller}"' in player_main
