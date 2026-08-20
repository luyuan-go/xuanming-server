"""player_locator 服务配置 —— 对应 Go 侧 internal/conf/conf.go + pkg/config.DSAuthConf。

读的是**同一份** services/runtime/player_locator/etc/locator-dev.yaml,不另建配置文件:
迁移期 Go 版和 Python 版并存,运维只该维护一份配置。

★ 默认值必须与 Go 的 `Defaults()` 逐字段相同,**连判据符号都要一样**。
  Go 这里全部用 `== 0`(config.Duration 是 int64 纳秒),不是 `<= 0` ——
  意味着 yaml 里写 `location_ttl: "-1s"` 在 Go 侧**不会**被兜成 30s,
  而是带着负值往下走(随后被 biz 的机械下限抬到 27s)。Python 若写成 `<= 0`,
  同一份 yaml 会得到 30s 而 Go 得到 27s,两边都不报错。所以这里照抄 `== 0`。

★ kafka 段在 Go 是 `pkg/config.Base` 的成员,Python 共享的 `pandorapy.config.BaseConf`
  还没有它(见交付说明 shared_files_needed)。这里在服务私有 Config 上补建模,是为了
  让**配了就会改变正确性**的段不落进 model_extra 被静默忽略:
  departure_event.enabled=true 时 kafka.brokers 是启动强依赖,落进 extra 的话
  Python 副本会读到空 brokers 而拒启,yaml 明明配了 broker。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig

# 端口与 Go 的 Defaults() 同值。Envoy cluster / run_services.ps1 的端口占用检查
# 都钉在 20006 / 21006,改这里等于改部署。
DEFAULT_GRPC_ADDR = ":20006"
DEFAULT_HTTP_ADDR = ":21006"

# 与 Go 侧逐个同值(抽成常量而不是内联,便于测试直接对着 Go 源码断言)。
DEFAULT_LOCATION_TTL = "30s"
DEFAULT_LAST_SEEN_RETENTION = "1h"
DEFAULT_DEBOUNCE_WINDOW = "8s"
DEFAULT_COALESCE_TICK = "1s"
DEFAULT_KILL_SWITCH_KEY = "presence/fanout"

DEFAULT_DS_AUTH_AUTHORITY_MODE = "legacy"
DEFAULT_DS_AUTH_ISSUER = "pandora-ds-control"
DEFAULT_DS_AUTH_AUDIENCE = "pandora-ds"
DEFAULT_DS_AUTH_BATTLE_TOKEN_TTL = "4h"
DEFAULT_DS_AUTH_HUB_TOKEN_TTL = "24h"
DEFAULT_DS_AUTH_ACTIVE_HEARTBEAT_MAX_AGE = "30s"


def _zero(raw: str) -> bool:
    """Go 的 `config.Duration == 0` 判据。

    空串 / 缺字段 → parse 出 0 → 视为未配置。**负值不算零**,与 Go 一致
    (Go 的 Duration 是纳秒整数,-1s 是非零,不会被 Defaults 覆盖)。
    """
    return pconfig.parse_duration(raw).total_seconds() == 0


class DepartureEventConf(BaseModel):
    """离场事件出口(topic pandora.player.presence)。对应 Go 的 DepartureEventConf。

    **默认关闭**(§14.2):关闭时 last-seen 时刻照常记录,消费方退化为
    「读到该实体时顺手复查」的兜底路径,只是不再有秒级触发器。
    这条降级是设计的一部分,不是半成品。
    """

    model_config = {"extra": "allow"}

    enabled: bool = False


class LocatorConf(BaseModel):
    """locator 私有配置段。字段名与 Go 的 yaml tag 逐字一致。"""

    model_config = {"extra": "allow"}

    # Redis 位置 hash 的 TTL。默认 30s(infra.md §3.2 的 30s heartbeat)。
    # 注意:真正生效的值还会被 biz.effective_ttl_sec 按 DS 再入屏障(27s)机械抬高。
    location_ttl: str = ""

    # 「玩家最后一次离开 Hub 的时刻」独立 key 的保留时长,默认 1h。
    # 必须**远大于**所有消费方的离线阈值(当前最长 team.offline_leave.threshold=180s):
    # 时刻先于阈值过期 → 消费方查到 UNKNOWN → 按 §9.22 fail-closed 不动作 → 功能静默失效。
    last_seen_retention: str = ""

    departure_event: DepartureEventConf = Field(default_factory=DepartureEventConf)

    def location_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.location_ttl)

    def last_seen_retention_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.last_seen_retention)


class PresenceConf(BaseModel):
    """好友在线态订阅推送 fan-out(friend-distributed-scaling.md §13.4 / §13.5)。

    默认 enabled=false:按 §13.7「先拉后推」,订阅推送是可选增强。
    """

    model_config = {"extra": "allow"}

    enabled: bool = False
    debounce_window: str = ""
    coalesce_tick: str = ""
    kill_switch_key: str = ""

    def debounce_window_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.debounce_window)

    def coalesce_tick_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.coalesce_tick)


class KafkaConf(BaseModel):
    """对应 Go 的 `pkg/config.KafkaConfig`(只建模 locator 用得到的 producer 字段)。"""

    model_config = {"extra": "allow"}

    brokers: list[str] = Field(default_factory=list)
    partition_cnt: int = 0
    dial_timeout: str = ""
    write_timeout: str = ""
    retry_max: int = 0
    retry_backoff: str = ""
    compression_type: str = ""
    # Go 结构体零值是 false,但 locator-dev.yaml 显式写 true,且幂等 producer 是保序前提。
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
        return int(pconfig.parse_duration(self.dial_timeout).total_seconds() * 1000)


class DSAuthFenceConf(BaseModel):
    """对应 Go 的 `pkg/config.DSAuthFenceConf`。

    只含非敏感控制面配置;Pod UID 与镜像 digest 必须由 Downward API 注入环境变量,
    不能从可伪造的 hostname/tag 回退。
    """

    model_config = {"extra": "allow"}

    etcd_endpoints: list[str] = Field(default_factory=list)
    etcd_prefix: str = ""
    etcd_lease_ttl_sec: int = 0
    etcd_dial_timeout: str = ""
    keyset_revision: str = ""

    def etcd_dial_timeout_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.etcd_dial_timeout)


class DSAuthConf(BaseModel):
    """对应 Go 的 `pkg/config.DSAuthConf`。player_locator 只做**校验侧**(verify-only)。

    Hub DS 经 :8444 调 SetLocation(HUB) / ReportDisconnect / RefreshHubLocations
    须带 hub 令牌(由 hub_allocator 签发,sub=pod)。
    mode 默认 off;authority_mode 默认 legacy。
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
        """对应 Go 的 `DSAuthConf.Defaults()`。Mode / Secret 留空即"不启用",不填默认。"""
        if not self.authority_mode:
            self.authority_mode = DEFAULT_DS_AUTH_AUTHORITY_MODE
        if not self.issuer:
            self.issuer = DEFAULT_DS_AUTH_ISSUER
        if not self.audience:
            self.audience = DEFAULT_DS_AUTH_AUDIENCE
        if _zero(self.battle_token_ttl):
            self.battle_token_ttl = DEFAULT_DS_AUTH_BATTLE_TOKEN_TTL
        if _zero(self.hub_token_ttl):
            self.hub_token_ttl = DEFAULT_DS_AUTH_HUB_TOKEN_TTL
        if _zero(self.active_heartbeat_max_age):
            self.active_heartbeat_max_age = DEFAULT_DS_AUTH_ACTIVE_HEARTBEAT_MAX_AGE

    def enabled(self) -> bool:
        """本服务是否启用 DS 回调令牌校验。mode 空 / "off" = 不校验。

        ★ 归一化必须与 Go 的 `ParseDSAuthMode`(pkg/middleware/dsauth.go:89)一致:
        `strings.ToLower(strings.TrimSpace(s))`。不做归一化的后果是
        `mode: "Off"` 或 `mode: " off "` 这种**Go 正常起、Python 拒启**的分叉 ——
        同一份 yaml 一个字没错,只是大小写/空格不同。
        """
        return self.mode.strip().lower() not in ("", "off")

    def authority_mode_redis(self) -> bool:
        """对应 Go 的 `AuthorityModeRedis()`。"""
        return self.authority_mode == "redis"

    def active_heartbeat_max_age_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.active_heartbeat_max_age)

    def validate_redis_fence(self) -> None:
        """对应 Go 的 `ValidateRedisFence()` —— **逐条同序同判据**。

        不设这道闸:authority_mode=redis 却没有 etcd 机械 fence,
        失租 / 旧 epoch 的副本会继续接受 Hub 写回,而权威面看起来一切正常。
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
        if self.fence.etcd_dial_timeout_td().total_seconds() < 0:
            raise ValueError(
                "ds_auth: fence.etcd_dial_timeout must be positive or zero(default)"
            )
        if self.active_heartbeat_max_age_td().total_seconds() <= 0:
            raise ValueError(
                "ds_auth: authority_mode=redis requires positive active_heartbeat_max_age"
            )


class Config(pconfig.BaseConf):
    """player_locator 服务的完整配置。对应 Go 的 conf.Config。"""

    locator: LocatorConf = Field(default_factory=LocatorConf)
    presence: PresenceConf = Field(default_factory=PresenceConf)
    kafka: KafkaConf = Field(default_factory=KafkaConf)
    ds_auth: DSAuthConf = Field(default_factory=DSAuthConf)

    def apply_defaults(self) -> None:
        """填默认值 —— 对应 Go 的 `Defaults()`,**顺序与判据符号逐条同**。"""
        if _zero(self.locator.location_ttl):
            self.locator.location_ttl = DEFAULT_LOCATION_TTL
        if _zero(self.locator.last_seen_retention):
            self.locator.last_seen_retention = DEFAULT_LAST_SEEN_RETENTION
        if _zero(self.presence.debounce_window):
            self.presence.debounce_window = DEFAULT_DEBOUNCE_WINDOW
        if _zero(self.presence.coalesce_tick):
            self.presence.coalesce_tick = DEFAULT_COALESCE_TICK
        if not self.presence.kill_switch_key:
            self.presence.kill_switch_key = DEFAULT_KILL_SWITCH_KEY
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR
        self.ds_auth.apply_defaults()
        # Go 在 DSAuth.Defaults() 之后又兜了一次 authority_mode,这里照搬:
        # 两处兜同一个字段是冗余的,但**照抄冗余比自作主张删掉安全** ——
        # 万一 pkg/config 那侧被改动,服务侧这道仍然守住 "legacy"。
        if not self.ds_auth.authority_mode:
            self.ds_auth.authority_mode = DEFAULT_DS_AUTH_AUTHORITY_MODE

    def validate_ds_auth_authority_mode(self) -> None:
        """对应 Go 的 `ValidateDSAuthAuthorityMode()`。

        拒绝拼写错误的授权权威模式:把预期的 redis 误写成别的值却静默退化为 legacy,
        SetLocation / ReportDisconnect 会**绕过** active credential 门,
        而启动日志毫无痕迹。
        """
        if self.ds_auth.authority_mode not in ("legacy", "redis"):
            raise ValueError(
                f"ds_auth.authority_mode invalid: {self.ds_auth.authority_mode!r} "
                "(want legacy|redis)"
            )

    @classmethod
    def load(cls, path: str) -> "Config":
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
