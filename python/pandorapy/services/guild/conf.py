"""guild 服务私有配置 —— 对应 Go 侧 services/social/guild/internal/conf/conf.go。

读的是**同一份** services/social/guild/etc/guild-dev.yaml,不另建配置文件:
迁移期 Go 版和 Python 版并存,运维只该维护一份配置。

★ 默认值必须与 Go 的 `Defaults()` **逐字段相同,连判据符号都要一样**。
  guild 这里有一处独一无二的分叉点:

      rate_quota_per_min 用的是 `== 0`,其余全是 `<= 0`。

  这不是笔误 —— 该字段的**负值语义是「关闭频率配额」**(conf.go 注释原话
  「负值 = 关闭」)。若 Python 抄成 `<= 0`,yaml 里写 `-1` 的"显式关闭"会被
  静默兜成 10/分钟:运维以为关了、实际在限流,而两边都不报错。
  反过来把别的字段抄成 `== 0` 也一样危险(负的 sweep_batch 会让每轮删 -500 行)。

端口默认值(20008/21008)同样是契约:Envoy 的 cluster、run_services.ps1 的端口
占用检查、K8s Service 都钉在这两个数上。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig
from pandorapy import dbguard
from pandorapy import internalrpcauth

DEFAULT_GRPC_ADDR = ":20008"
DEFAULT_HTTP_ADDR = ":21008"

# 与 Go 的 Defaults() 一一对应(抽成常量便于测试直接对着 Go 源码断言)。
DEFAULT_MAX_GUILD_MEMBERS = 100
DEFAULT_MAX_GROUP_MEMBERS = 50
DEFAULT_RATE_QUOTA_PER_MIN = 10
DEFAULT_MAX_PENDING_REQUESTS_PER_GUILD = 200
DEFAULT_MAX_GROUPS_PER_PLAYER = 50
DEFAULT_MAX_NAME_LEN = 24
DEFAULT_CACHE_TTL = "60s"
DEFAULT_REQUEST_RETENTION_DAYS = 90
DEFAULT_SWEEP_INTERVAL = "5m"
DEFAULT_SWEEP_BATCH = 500
DEFAULT_PLAYER_NAME_RESOLVER_AUTH_AUDIENCE = "player:name"
DEFAULT_PLAYER_NO_RESOLVER_AUTH_AUDIENCE = "login:player-no"

# DS 回调令牌校验的默认值 —— 对应 Go 的 pkg/config.DSAuthConf.Defaults()。
DEFAULT_DS_AUTH_AUTHORITY_MODE = "legacy"
DEFAULT_DS_AUTH_ISSUER = "pandora-ds-control"
DEFAULT_DS_AUTH_AUDIENCE = "pandora-ds"
DEFAULT_DS_AUTH_BATTLE_TOKEN_TTL = "4h"
DEFAULT_DS_AUTH_HUB_TOKEN_TTL = "24h"
DEFAULT_DS_AUTH_ACTIVE_HEARTBEAT_MAX_AGE = "30s"


def _duration_sec(raw: str) -> float:
    return pconfig.parse_duration(raw).total_seconds()


class SessionGateConf(BaseModel):
    """会话现行性门参数 —— 对应 Go 的 `pkg/config.SessionGateConf`。

    ★ 必须显式建模。`BaseConf` 是 extra="allow",不建模的话 `session_gate`
      整段落进 model_extra:yaml 里 `require: true` 写着,Python 侧当没看见 ——
      **prod 的强制档静默退化成 dev 宽松档**,顶号后旧 JWT 还能继续操作公会。
    """

    model_config = {"extra": "allow"}

    require: bool = False


class KafkaConf(BaseModel):
    """对应 Go 的 `pkg/config.KafkaConfig`(只建模 guild 用得到的 producer 字段)。

    guild 只做 producer(pandora.guild.event),不消费;group_id 等消费侧字段
    由 extra 吸收。
    """

    model_config = {"extra": "allow"}

    brokers: list[str] = Field(default_factory=list)
    partition_cnt: int = 0
    dial_timeout: str = ""
    write_timeout: str = ""
    retry_max: int = 0
    retry_backoff: str = ""
    compression_type: str = ""
    # Go 结构体零值是 false,但集群 yaml 普遍显式写 true,而幂等 producer 是保序前提。
    # 默认取 True 与 kafkax.ProducerConf 一致 —— 默认关掉会让重试换序而无人察觉。
    # ★ Go 的真实默认是 **False**(结构体零值),不是注释里写的 "默认 true"。
    # pkg/config/config.go:239 的注释是 aspirational 的 —— 全仓没有任何代码给它填值,
    # producer.go:85 直穿 `c.Producer.Idempotent = cfg.Idempotent`。
    # 写成 True 的后果:35 份带 kafka 段的 yaml 里有 15 份没显式配它,
    # 那些服务上两栈的**重复消息语义**不同(幂等 producer 影响重试时的去重),
    # 而两边都不报错。
    idempotent: bool = False
    def dial_timeout_ms(self) -> int:
        """拨号超时(毫秒)。kafkax.ProducerConf 收毫秒,yaml 里是 Go duration 串。

        不换算的话 `dial_timeout: "2s"` 会被整段忽略 —— broker 不通时 producer
        用客户端库默认超时(数十秒)干等,而 yaml 明明写了 2s。
        """
        return int(_duration_sec(self.dial_timeout) * 1000)

    def retry_backoff_ms(self) -> int:
        return int(_duration_sec(self.retry_backoff) * 1000)


class DSAuthFenceConf(BaseModel):
    """对应 Go 的 `pkg/config.DSAuthFenceConf`。guild 不读它,建模只为不落进 extra。"""

    model_config = {"extra": "allow"}

    etcd_endpoints: list[str] = Field(default_factory=list)
    etcd_prefix: str = ""
    etcd_lease_ttl_sec: int = 0
    etcd_dial_timeout: str = ""
    keyset_revision: str = ""


class DSAuthConf(BaseModel):
    """对应 Go 的 `pkg/config.DSAuthConf`。guild 只做**校验侧**(verify-only)。

    guild 用它守住 `GetPlayerGuild` —— 那是 DS 出生编制反查(铭牌关系上色),
    暴露在无 jwt_authn 的 DS 面(:8444)上。`systemOnly` 只证明「不带玩家 JWT」,
    证明不了「调用方是 DS」;本配置提供后者。

    mode 默认 off(不校验),与接线前行为一致。
    """

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
        """对应 Go 的 `DSAuthConf.Defaults()`。

        ★ mode / secret 留空即「不启用」,**不填默认值** —— 给 mode 填个默认
        等于替运维决定要不要开一道鉴权门。
        """
        if not self.authority_mode:
            self.authority_mode = DEFAULT_DS_AUTH_AUTHORITY_MODE
        if not self.issuer:
            self.issuer = DEFAULT_DS_AUTH_ISSUER
        if not self.audience:
            self.audience = DEFAULT_DS_AUTH_AUDIENCE
        if _duration_sec(self.battle_token_ttl) == 0:
            self.battle_token_ttl = DEFAULT_DS_AUTH_BATTLE_TOKEN_TTL
        if _duration_sec(self.hub_token_ttl) == 0:
            self.hub_token_ttl = DEFAULT_DS_AUTH_HUB_TOKEN_TTL
        if _duration_sec(self.active_heartbeat_max_age) == 0:
            self.active_heartbeat_max_age = DEFAULT_DS_AUTH_ACTIVE_HEARTBEAT_MAX_AGE


class GuildConf(BaseModel):
    """guild 私有配置段(公会 + 临时群共用)。字段名与 Go 的 yaml tag 逐字一致。"""

    model_config = {"extra": "allow"}

    # 单公会成员上限(§9.18 写入侧上限;ApproveJoin 在事务内原子校验)。
    max_guild_members: int = 0
    # 单临时群成员上限(§9.18;建群 / AddMember 事务内原子校验)。
    max_group_members: int = 0
    # 单公会挂起(pending)加入申请上限(§9.18;CreateJoinRequest 事务内校验)。
    # 没有它,公会申请列表能被任意玩家刷爆 —— 客户端可写入的累积列表必须有写入侧上限。
    max_pending_requests_per_guild: int = 0
    # 入会申请的 per-player 每分钟频率配额(anti-abuse §6 第 6 项)。
    # ★ **负值 = 关闭**,所以判据是 `== 0` 而不是 `<= 0`(见模块头注释)。
    rate_quota_per_min: int = 0
    # 单玩家可同时加入的临时群数量上限(§9.18)。
    max_groups_per_player: int = 0
    # 公会 / 群名最大长度(utf8 rune 数,**不是字节**)。
    max_name_len: int = 0
    # 入会申请公开展示投影。两个 authority 各用 guild 调用方的独立 HMAC 凭据。
    player_name_resolver_addr: str = ""
    player_name_resolver_auth_secret: str = ""
    player_name_resolver_auth_audience: str = ""
    player_no_resolver_addr: str = ""
    player_no_resolver_auth_secret: str = ""
    player_no_resolver_auth_audience: str = ""
    # 公会读缓存(Redis cache-aside)条目 TTL。写后删 + 短 TTL 兜底。
    cache_ttl: str = ""
    # 终态入会申请(approved/rejected)保留天数(§9.24)。pending 永不清。
    request_retention_days: int = 0
    # 保留期清理轮询间隔。多副本各自跑,DELETE 幂等无需锁。
    # 零值会让清理循环退化成忙等(safego.loop 对 <=0 直接返回并报错)。
    sweep_interval: str = ""
    # 每轮清理行数上限。零值 = 每轮删 0 行:循环在跑、日志正常、表永远不清。
    sweep_batch: int = 0
    # 保留期清理模式:留空 / "report_only" = 只报告不删(§9.24 默认);"delete" = 真删。
    retention_mode: str = ""

    def cache_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.cache_ttl)

    def sweep_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.sweep_interval)

    def retention_mode_parsed(self) -> dbguard.Mode:
        """对应 Go 的 `RetentionMode()`:解析失败**回落 report_only**(不猜 delete)。

        真正的拼错拦截在 `validate_retention_mode`(启动 fail-fast);这里只是
        「万一还是走到了,也绝不删数据」的第二道。
        """
        try:
            return dbguard.parse_mode(self.retention_mode)
        except ValueError:
            return dbguard.Mode.REPORT_ONLY

    def validate_retention_mode(self) -> None:
        """对应 Go 的 `ValidateRetentionMode()` —— 供 main 启动 fail-fast。

        不设这道闸:拼错的值(`delet` / `Delete ` / `report only`)会**静默回落
        report_only**,运维以为开了清理、实际一行没删,库继续无界增长,
        而启动期毫无痕迹。
        """
        dbguard.parse_mode(self.retention_mode)


class Config(pconfig.BaseConf):
    """guild 服务的完整配置。对应 Go 的 conf.Config。"""

    guild: GuildConf = Field(default_factory=GuildConf)
    session_gate: SessionGateConf = Field(default_factory=SessionGateConf)
    kafka: KafkaConf = Field(default_factory=KafkaConf)
    ds_auth: DSAuthConf = Field(default_factory=DSAuthConf)

    def apply_defaults(self) -> None:
        """填默认值 —— 对应 Go 的 `Defaults()`,**逐条同序同判据**。

        顺序照抄不是形式主义:Go 先 `DSAuth.Defaults()` 再填 guild 段,
        若哪天某个 guild 字段的默认值依赖 ds_auth(现在不依赖),顺序不同就分叉。
        """
        self.ds_auth.apply_defaults()
        if self.guild.max_guild_members <= 0:
            self.guild.max_guild_members = DEFAULT_MAX_GUILD_MEMBERS
        if self.guild.max_group_members <= 0:
            self.guild.max_group_members = DEFAULT_MAX_GROUP_MEMBERS
        # ★ `== 0`(不是 `<= 0`):负值是「显式关闭频率配额」,不能被兜成 10。
        if self.guild.rate_quota_per_min == 0:
            self.guild.rate_quota_per_min = DEFAULT_RATE_QUOTA_PER_MIN
        if self.guild.max_pending_requests_per_guild <= 0:
            self.guild.max_pending_requests_per_guild = DEFAULT_MAX_PENDING_REQUESTS_PER_GUILD
        if self.guild.max_groups_per_player <= 0:
            self.guild.max_groups_per_player = DEFAULT_MAX_GROUPS_PER_PLAYER
        if self.guild.max_name_len <= 0:
            self.guild.max_name_len = DEFAULT_MAX_NAME_LEN
        if self.guild.cache_ttl_td().total_seconds() <= 0:
            self.guild.cache_ttl = DEFAULT_CACHE_TTL
        if self.guild.request_retention_days <= 0:
            self.guild.request_retention_days = DEFAULT_REQUEST_RETENTION_DAYS
        if self.guild.sweep_interval_td().total_seconds() <= 0:
            self.guild.sweep_interval = DEFAULT_SWEEP_INTERVAL
        if self.guild.sweep_batch <= 0:
            self.guild.sweep_batch = DEFAULT_SWEEP_BATCH
        if (
            self.guild.player_name_resolver_addr
            and not self.guild.player_name_resolver_auth_audience
        ):
            self.guild.player_name_resolver_auth_audience = (
                DEFAULT_PLAYER_NAME_RESOLVER_AUTH_AUDIENCE
            )
        if (
            self.guild.player_no_resolver_addr
            and not self.guild.player_no_resolver_auth_audience
        ):
            self.guild.player_no_resolver_auth_audience = (
                DEFAULT_PLAYER_NO_RESOLVER_AUTH_AUDIENCE
            )
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR

    def validate_player_display_resolvers(self) -> None:
        """展示 resolver 的地址/凭据必须闭包，两个 authority 不得复用密钥。"""
        g = self.guild
        self._validate_resolver(
            "guild.player_name_resolver",
            g.player_name_resolver_addr,
            g.player_name_resolver_auth_secret,
            g.player_name_resolver_auth_audience,
        )
        self._validate_resolver(
            "guild.player_no_resolver",
            g.player_no_resolver_addr,
            g.player_no_resolver_auth_secret,
            g.player_no_resolver_auth_audience,
        )
        if (
            g.player_name_resolver_auth_secret
            and g.player_no_resolver_auth_secret
            and g.player_name_resolver_auth_secret == g.player_no_resolver_auth_secret
        ):
            raise ValueError(
                "guild.player_name_resolver_auth_secret must differ from "
                "guild.player_no_resolver_auth_secret"
            )

    @staticmethod
    def _validate_resolver(label: str, addr: str, secret: str, audience: str) -> None:
        if not addr:
            if secret or audience:
                raise ValueError(f"{label} credentials require addr")
            return
        if not secret:
            raise ValueError(f"{label}_auth_secret required when addr is configured")
        if not audience:
            raise ValueError(f"{label}_auth_audience required when addr is configured")
        try:
            internalrpcauth.validate_secret(secret)
            internalrpcauth.validate_identity(audience)
        except ValueError as exc:
            raise ValueError(f"{label}: {exc}") from exc

    @classmethod
    def load(cls, path: str) -> "Config":
        """从 yaml 加载并填默认值。"""
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
