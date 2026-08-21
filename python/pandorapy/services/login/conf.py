"""login 服务配置 —— 对应 Go 侧 services/account/login/internal/conf/conf.go。

读的是**同一份** services/account/login/etc/login-dev.yaml,不另建配置文件:
迁移期 Go 版和 Python 版并存,运维只该维护一份配置。

★ 默认值逐字段对 Go 的 `Defaults()`,**连判据符号都一样**:
    - `SessionTokenTTL == 0` / `PlayerNoStart == 0` / `LoginFailLimit == 0` 是 `== 0`,
      所以 yaml 写 `login_fail_limit: -1` 在 Go 侧**原样保留**(负值 = 显式关闭整个
      失败配额,见 LoginConf.LoginFailLimit 注释)。若 Python 写成 `<= 0` 判据,
      同一份 yaml 会让 Python 副本把 -1 兜成 5 —— 运维以为关了失败配额,
      实际 Python 副本仍在锁账号,而**两边都不报错**。
    - `DeviceRetentionDays <= 0` 才是 `<= 0`。两个符号混用是本文件最容易出错的地方,
      所以每处都在代码里写了 Go 的原符号。

★ ds_auth / ds_ticket / cell_route 三段在 Go 是 `pkg/config` 的成员。
  Python 共享的 `pandorapy.config.BaseConf` 目前只有 server/node/snowflake/config_table,
  ds_auth 与 ds_ticket 在这里建模成服务私有段(与 player / guild / inventory 同做法),
  为的是不让**配了就会改变正确性**的段落进 `model_extra` 被静默忽略:
    - ds_auth.authority_mode=redis 是 Model B 的总开关,忽略它 = 在线入场权威整条消失;
    - ds_ticket.private_key_file 非空 = v2 RS256 签发启用,忽略它 = 继续签 legacy HS256
      而 DS 侧只认 RS256,表现为"全服进不去场景"且启动日志全绿。
  cell_route 由 BaseConf 的 `assert_unsupported_sections` 统一拒启(Python 侧只实现单 Cell)。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig
from pandorapy import dbguard
from pandorapy import internalrpcauth

SERVICE_NAME = "login"

# 与 Go 的 conf.Defaults() 逐个同值。抽成常量而不是内联字面量,是为了让
# tests/test_login_conf.py 能直接对着 Go 源码断言(漂移当场变红)。
DEFAULT_GRPC_ADDR = ":20001"
DEFAULT_HTTP_ADDR = ":21001"
DEFAULT_SESSION_TOKEN_TTL = "24h"
DEFAULT_DS_TICKET_TTL = "5m"
DEFAULT_DEVICE_RETENTION_DAYS = 90
DEFAULT_PLAYER_NO_START = 1
DEFAULT_MOCK_HUB_DS_ADDR = "127.0.0.1:7777"
DEFAULT_LOGIN_FAIL_LIMIT = 5
DEFAULT_LOGIN_FAIL_WINDOW = "15m"
DEFAULT_LOGIN_FAIL_LOCK = "5m"
DEFAULT_JWT_ISSUER = "pandora-login"
DEFAULT_JWT_AUDIENCE = "pandora-client"
# ❗ dev 默认 secret,与 Go 的 conf.Defaults() 逐字一致(envoy.yaml 里放的是它的 base64url)。
DEFAULT_JWT_SECRET = "pandora-dev-jwt-secret-change-me-32!"
# 账号态受众来自 pkg/auth.Config.Defaults()(不是 login 的 conf),login 的 yaml 里
# 没有这个字段。写死在这里与 Go 同源:两个受众相同 = 账号 token 能冒充玩家 token。
DEFAULT_JWT_ACCOUNT_AUDIENCE = "pandora-account"
DEFAULT_JWT_ACCOUNT_TTL = _dt.timedelta(minutes=10)

# pkg/config.DSAuthConf.Defaults()
DEFAULT_DS_AUTH_AUTHORITY_MODE = "legacy"
DEFAULT_DS_AUTH_ISSUER = "pandora-ds-control"
DEFAULT_DS_AUTH_AUDIENCE = "pandora-ds"
DEFAULT_DS_AUTH_BATTLE_TOKEN_TTL = "4h"
DEFAULT_DS_AUTH_HUB_TOKEN_TTL = "24h"
DEFAULT_DS_AUTH_ACTIVE_HEARTBEAT_MAX_AGE = "30s"


def _is_zero(raw: str) -> bool:
    """Go 侧 `config.Duration == 0` 的判据。

    留空、写 "0s"、写 "0" 都是零值;负值(如 "-1s")**不是**零值,必须原样保留 ——
    多个服务用负数表示"显式关闭",兜成默认值等于把关掉的开关偷偷打开。
    """
    return pconfig.parse_duration(raw) == _dt.timedelta(0)


class DSAuthFenceConf(BaseModel):
    """对应 Go 的 `pkg/config.DSAuthFenceConf`。

    只含非敏感控制面配置:Pod UID 与镜像 digest 由 Downward API 注入环境变量,
    不能从可伪造的 hostname/tag 回退。
    """

    model_config = {"extra": "allow"}

    etcd_endpoints: list[str] = Field(default_factory=list)
    etcd_prefix: str = ""
    etcd_lease_ttl_sec: int = 0
    etcd_dial_timeout: str = ""
    keyset_revision: str = ""

    def dial_timeout_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.etcd_dial_timeout)

    def same_as(self, other: "DSAuthFenceConf") -> bool:
        """对应 Go 的 `sameFence`。逐字段相等 —— 两处 fence 必须是**同一把** capability 租约。

        不设这道闸会怎样:ds_auth.fence 与 login.hub_assignment_fence 指向不同 etcd
        前缀 / 不同 keyset_revision 时,同一个进程会注册两把互不相干的 capability,
        「旧 writer 激活后不能回滚接流量」这条保证在其中一把上成立、另一把上不成立,
        而两把都续约成功,日志全绿。
        """
        return (
            list(self.etcd_endpoints) == list(other.etcd_endpoints)
            and self.etcd_prefix == other.etcd_prefix
            and self.etcd_lease_ttl_sec == other.etcd_lease_ttl_sec
            and self.dial_timeout_td() == other.dial_timeout_td()
            and self.keyset_revision == other.keyset_revision
        )


class DSAuthConf(BaseModel):
    """对应 Go 的 `pkg/config.DSAuthConf`。login 既是校验侧,也是 Model B 的权威侧。"""

    model_config = {"extra": "allow"}

    mode: str = ""
    issuer: str = ""
    audience: str = ""
    secret: str = ""
    additional_secrets: list[str] = Field(default_factory=list)
    battle_token_ttl: str = ""
    hub_token_ttl: str = ""
    active_heartbeat_max_age: str = ""
    authority_mode: str = ""
    fence: DSAuthFenceConf = Field(default_factory=DSAuthFenceConf)

    def apply_defaults(self) -> None:
        """对应 Go 的 `DSAuthConf.Defaults()`。mode/secret 留空即"不启用",不填默认。"""
        if not self.authority_mode:
            self.authority_mode = DEFAULT_DS_AUTH_AUTHORITY_MODE
        if not self.issuer:
            self.issuer = DEFAULT_DS_AUTH_ISSUER
        if not self.audience:
            self.audience = DEFAULT_DS_AUTH_AUDIENCE
        if _is_zero(self.battle_token_ttl):
            self.battle_token_ttl = DEFAULT_DS_AUTH_BATTLE_TOKEN_TTL
        if _is_zero(self.hub_token_ttl):
            self.hub_token_ttl = DEFAULT_DS_AUTH_HUB_TOKEN_TTL
        if _is_zero(self.active_heartbeat_max_age):
            self.active_heartbeat_max_age = DEFAULT_DS_AUTH_ACTIVE_HEARTBEAT_MAX_AGE

    def authority_mode_redis(self) -> bool:
        return self.authority_mode == "redis"

    def active_heartbeat_max_age_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.active_heartbeat_max_age)

    def validate_redis_fence(self) -> None:
        """对应 Go 的 `DSAuthConf.ValidateRedisFence()`。

        不设这道闸会怎样:authority_mode=redis 缺 fence 时,Redis 成了唯一授权权威
        却没有任何机械手段阻止「已被替换的旧 login writer 重新起来接流量」——
        两个 writer 同时消费 DS 入场票,一人一 DS 的不变量当场破。
        """
        if not self.authority_mode_redis():
            return
        if self.mode != "enforce":
            raise ValueError("ds_auth: authority_mode=redis requires mode=enforce")
        if not self.fence.etcd_endpoints:
            raise ValueError("ds_auth: authority_mode=redis requires fence.etcd_endpoints")
        if not self.fence.keyset_revision:
            raise ValueError(
                "ds_auth: authority_mode=redis requires immutable fence.keyset_revision"
            )
        if self.fence.etcd_lease_ttl_sec < 0:
            raise ValueError(
                "ds_auth: fence.etcd_lease_ttl_sec must be positive or zero(default)"
            )
        if self.fence.dial_timeout_td() < _dt.timedelta(0):
            raise ValueError(
                "ds_auth: fence.etcd_dial_timeout must be positive or zero(default)"
            )
        if self.active_heartbeat_max_age_td() <= _dt.timedelta(0):
            raise ValueError(
                "ds_auth: authority_mode=redis requires positive active_heartbeat_max_age"
            )


class DSTicketConf(BaseModel):
    """对应 Go 的 `pkg/config.DSTicketConf` —— 玩家 DSTicket v2(RS256)信任域。

    与 SessionToken(HS256,pandora-client)、DS 回调令牌(HS256,pandora-ds)严格分域。
    留空 private_key_file = 本服务不启用 v2 签发,沿用 legacy HS256(dev 行为不变)。
    """

    model_config = {"extra": "allow"}

    private_key_file: str = ""
    active_kid: str = ""
    ttl: str = ""
    jwks_file: str = ""
    keyset_revision: str = ""

    def signer_enabled(self) -> bool:
        return self.private_key_file != ""

    def verifier_enabled(self) -> bool:
        return self.jwks_file != ""

    def ttl_td(self) -> _dt.timedelta:
        """票据有效期。零值 = 由签发器取 DSTicketDefaultTTL(120s)。

        ★ conf 层**不填默认**,与 Go 一致:默认值在 `dsticket.DSTicketSigner.new`
        里。在这里替它填一个,会让「conf 层看到的 TTL」与「签发器实际用的 TTL」
        在某次改动后悄悄分叉。
        ⚠️ 别和 `JWTConf.ds_ticket_ttl`(legacy HS256,默认 5m)混:v2 生产档是
        120s 默认 / 180s 硬上限(CLAUDE.md §9 不变量 3),按 5m 推安全窗口会错 2.5 倍。
        """
        return pconfig.parse_duration(self.ttl)


class LocatorClientConf(BaseModel):
    """login → player_locator。留空仅允许 local/off 档。"""

    model_config = {"extra": "allow"}
    addr: str = ""


class HubClientConf(BaseModel):
    """login → hub_allocator。留空 → 回退自签 hub 票据 + mock_hub_ds_addr。"""

    model_config = {"extra": "allow"}
    addr: str = ""
    region: str = ""


class MatchClientConf(BaseModel):
    """login → matchmaker 只读权威兜底。留空 → presence-only。"""

    model_config = {"extra": "allow"}
    addr: str = ""
    auth_secret: str = ""
    auth_audience: str = ""


class PlayerClientConf(BaseModel):
    """login → player(把账号名播种成全服显示名)。留空 → 不播种,弱依赖。"""

    model_config = {"extra": "allow"}
    addr: str = ""


class JWTConf(BaseModel):
    """对应 Go 的 `conf.JWTConf`。

    与 Envoy jwt_authn 的 provider 配套:issuer / audience 必须跟 envoy.yaml 一致。
    """

    model_config = {"extra": "allow"}

    issuer: str = ""
    audience: str = ""
    secret: str = ""
    # 仅用于**校验**的额外可接受密钥(不用于签发),支持玩家面 JWT 不停服密钥轮换。
    additional_secrets: list[str] = Field(default_factory=list)
    session_ttl: str = ""
    ds_ticket_ttl: str = ""

    def session_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.session_ttl)

    def ds_ticket_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.ds_ticket_ttl)


class LoginConf(BaseModel):
    """login 私有配置段。字段名与 Go 的 yaml tag 逐字一致。"""

    model_config = {"extra": "allow"}

    session_token_ttl: str = ""
    ds_ticket_ttl: str = ""
    owner_addr: str = ""
    require_tidb: bool = False
    retention_mode: str = ""
    device_retention_days: int = 0
    player_no_start: int = 0
    # team → login 的内部批量 player_no 解析接口验签。secret 留空时 handler 仍注册但
    # fail-closed 拒绝，绝不因“仅内网”而裸奔。
    player_no_resolve_auth_secret: str = ""
    player_no_resolve_auth_audience: str = ""
    session_generation_enforce: bool = False
    require_ticket_sjti: bool = False
    require_hub_assignment_binding: bool = False
    hub_assignment_fence: DSAuthFenceConf = Field(default_factory=DSAuthFenceConf)
    mock_hub_ds_addr: str = ""
    dev_skip_password: bool = False
    dev_auto_register: bool = False
    # ★ `== 0` 判据(不是 `<= 0`):**负值**表示显式关闭整个失败配额。
    login_fail_limit: int = 0
    login_fail_window: str = ""
    login_fail_lock: str = ""
    jwt: JWTConf = Field(default_factory=JWTConf)
    ds_ticket: DSTicketConf = Field(default_factory=DSTicketConf)
    locator: LocatorClientConf = Field(default_factory=LocatorClientConf)
    hub: HubClientConf = Field(default_factory=HubClientConf)
    # ★ yaml key 是 `matchmaker`,Go 结构体字段叫 Match。以 yaml 为准。
    matchmaker: MatchClientConf = Field(default_factory=MatchClientConf)
    player: PlayerClientConf = Field(default_factory=PlayerClientConf)
    allowed_role_ids: list[int] = Field(default_factory=list)
    dev_allow_any_role: bool = False

    def session_token_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.session_token_ttl)

    def ds_ticket_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.ds_ticket_ttl)

    def login_fail_window_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.login_fail_window)

    def login_fail_lock_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.login_fail_lock)

    def retention_mode_parsed(self) -> dbguard.Mode:
        """生效的清理模式。**无法识别时回落 REPORT_ONLY**(与 Go 的 RetentionMode 同)。

        与 validate_retention_mode 分工不同:这个是运行期取值(绝不能因为配错就去删
        数据),validate 那个是启动期 fail-fast。
        """
        try:
            return dbguard.parse_mode(self.retention_mode)
        except ValueError:
            return dbguard.Mode.REPORT_ONLY

    def validate_retention_mode(self) -> None:
        """对应 Go 的 `ValidateRetentionMode`,供启动 fail-fast。

        不设这道闸会怎样:`retention_mode: delet`(少个 e)会静默回落 report_only,
        运维以为开了 account_devices 清理、实际一行没删,库继续增长且启动期毫无痕迹。
        """
        dbguard.parse_mode(self.retention_mode)


class Config(pconfig.BaseConf):
    """login 服务的完整配置。对应 Go 的 `conf.Config`。"""

    login: LoginConf = Field(default_factory=LoginConf)
    ds_auth: DSAuthConf = Field(default_factory=DSAuthConf)

    def apply_defaults(self) -> None:
        """对应 Go 的 `Config.Defaults()` —— 顺序与判据符号逐字同。

        顺序本身是契约:jwt.session_ttl 的默认值取自 **已经兜好底的**
        login.session_token_ttl。两句调换,没写 session_token_ttl 的 yaml 会让
        jwt.session_ttl 停在 0 → 签出来的 token 生下来就过期。
        """
        lg = self.login
        if _is_zero(lg.session_token_ttl):  # Go: == 0
            lg.session_token_ttl = DEFAULT_SESSION_TOKEN_TTL
        if _is_zero(lg.ds_ticket_ttl):  # Go: == 0
            lg.ds_ticket_ttl = DEFAULT_DS_TICKET_TTL
        if lg.device_retention_days <= 0:  # Go: <= 0
            lg.device_retention_days = DEFAULT_DEVICE_RETENTION_DAYS
        if lg.player_no_start == 0:  # Go: == 0
            lg.player_no_start = DEFAULT_PLAYER_NO_START
        if (
            lg.player_no_resolve_auth_secret
            and not lg.player_no_resolve_auth_audience
        ):
            lg.player_no_resolve_auth_audience = "login:player-no"
        if not lg.mock_hub_ds_addr:  # Go: == ""
            lg.mock_hub_ds_addr = DEFAULT_MOCK_HUB_DS_ADDR
        if lg.login_fail_limit == 0:  # Go: == 0(负值=显式关闭,必须保留)
            lg.login_fail_limit = DEFAULT_LOGIN_FAIL_LIMIT
        if _is_zero(lg.login_fail_window):  # Go: == 0
            lg.login_fail_window = DEFAULT_LOGIN_FAIL_WINDOW
        if _is_zero(lg.login_fail_lock):  # Go: == 0
            lg.login_fail_lock = DEFAULT_LOGIN_FAIL_LOCK
        if not lg.jwt.issuer:
            lg.jwt.issuer = DEFAULT_JWT_ISSUER
        if not lg.jwt.audience:
            lg.jwt.audience = DEFAULT_JWT_AUDIENCE
        if not lg.jwt.secret:
            lg.jwt.secret = DEFAULT_JWT_SECRET
        if _is_zero(lg.jwt.session_ttl):
            lg.jwt.session_ttl = lg.session_token_ttl  # 默认跟 SessionTokenTTL 一致
        if _is_zero(lg.jwt.ds_ticket_ttl):
            lg.jwt.ds_ticket_ttl = lg.ds_ticket_ttl
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR
        self.ds_auth.apply_defaults()

    def validate_conf(self) -> None:
        """对应 Go 的 `Config.Validate()` —— 校验不能靠运行期降级修复的配置冲突。

        每条闸的"不设会怎样"见分支内注释。名字刻意不叫 `validate`:
        pydantic BaseModel 已经有同名的类方法语义,覆盖它会让模型校验行为漂移。
        """
        login = self.login
        if login.player_no_resolve_auth_secret:
            try:
                internalrpcauth.validate_secret(
                    login.player_no_resolve_auth_secret
                )
            except ValueError as exc:
                raise ValueError(
                    f"login.player_no_resolve_auth_secret: {exc}"
                ) from exc
            try:
                internalrpcauth.validate_identity(
                    login.player_no_resolve_auth_audience
                )
            except ValueError as exc:
                raise ValueError(
                    f"login.player_no_resolve_auth_audience: {exc}"
                ) from exc
        elif login.player_no_resolve_auth_audience:
            raise ValueError(
                "login.player_no_resolve_auth_audience requires "
                "player_no_resolve_auth_secret"
            )

        ds_ticket = login.ds_ticket
        if ds_ticket.signer_enabled() and not ds_ticket.active_kid:
            # 不设会怎样:轮换窗口内无法机械确认"这个副本用的是预期那把私钥",
            # 换错键签出来的票 DS 全拒,而 login 侧一切正常。
            raise ValueError("login.ds_ticket signer requires explicit active_kid")
        if ds_ticket.verifier_enabled() and (
            not ds_ticket.active_kid or not ds_ticket.keyset_revision
        ):
            raise ValueError(
                "login.ds_ticket verifier requires explicit active_kid and keyset_revision"
            )
        if self.ds_auth.authority_mode not in ("", "legacy", "redis"):
            # 不设会怎样:拼错的 authority_mode 会被 `== "redis"` 判成 false,
            # 静默退回 legacy —— 运维以为开了 Model B,实际权威门根本没装。
            raise ValueError(
                f"ds_auth.authority_mode invalid: {self.ds_auth.authority_mode!r} "
                "(want legacy|redis)"
            )
        self.ds_auth.validate_redis_fence()
        if self.ds_auth.authority_mode_redis():
            if not self.login.require_hub_assignment_binding:
                raise ValueError(
                    "ds_auth.authority_mode=redis requires "
                    "login.require_hub_assignment_binding=true"
                )
            if not self.ds_auth.fence.same_as(self.login.hub_assignment_fence):
                raise ValueError(
                    "login ds_auth.fence and hub_assignment_fence must be identical "
                    "(single capability lease)"
                )
        if self.login.require_hub_assignment_binding:
            # 这四条是同一件事的四个前提:归属绑定是 fail-closed 门,
            # 缺任何一个,门都会静默变形成"永远放行"(见各 hint)。
            if not self.node.redis_client.host and not self.node.redis_client.addrs:
                raise ValueError(
                    "login.require_hub_assignment_binding=true requires node.redis_client"
                )
            if not self.login.hub.addr:
                raise ValueError(
                    "login.require_hub_assignment_binding=true requires login.hub.addr"
                )
            if not self.login.locator.addr:
                raise ValueError(
                    "login.require_hub_assignment_binding=true requires login.locator.addr"
                )
            if (
                not self.login.hub_assignment_fence.etcd_endpoints
                or not self.login.hub_assignment_fence.keyset_revision
            ):
                raise ValueError(
                    "login.require_hub_assignment_binding=true requires "
                    "login.hub_assignment_fence etcd endpoints/keyset revision"
                )
        self.login.validate_retention_mode()

    def capability_fence(self) -> tuple[DSAuthFenceConf, bool]:
        """对应 Go 的 `Config.CapabilityFence()`。

        Redis admission 与 Hub assignment fence 同时开启时 validate_conf 已要求二者
        完全一致,因此 main 只 Acquire **一次** —— 注册两把租约会让"唯一 writer"
        这条保证分裂成两把各自成立的租约,两把都续上时脑裂无人察觉。
        """
        if self.ds_auth.authority_mode_redis():
            return self.ds_auth.fence, True
        if self.login.require_hub_assignment_binding:
            return self.login.hub_assignment_fence, True
        return DSAuthFenceConf(), False

    @classmethod
    def load(cls, path: str) -> "Config":
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
