"""player 服务配置 —— 对应 Go 侧 services/account/player/internal/conf/conf.go。

读的是**同一份** services/account/player/etc/player-dev.yaml,不另建配置文件:
迁移期 Go 版与 Python 版并存,运维只该维护一份配置。

★ 默认值必须与 Go 的 `Defaults()` **逐个相同,判据符号也一样**。player 这里混用了
  三种判据,抄错任何一个都不会报错、只会让同一份 yaml 在两个实现上跑出不同行为:

    base_mmr           `<= 0`  → 1500      (写 0 或负数都兜)
    mmr_floor          `<  0`  → 0         (**不是** <=0:floor=0 是合法配置)
    max_nickname_len   `<= 0`  → 32
    default_nickname_prefix  `== ""` → "Player_"
    consume_topics     `len==0` → [pandora.player.update]
    server.*.addr      `== ""`  → :20002 / :21002

★ 两组保留期开关是**两道闸,都开才删**(§9.24):
    retention_mode          = 本服的删除总闸(运维口径,默认只报告)
    *_cleanup_enabled       = 每组表的前置条件确认(技术口径,默认 false)
  它们回答的是不同问题,合成一个开关会让「这组已确认有界、那组还没有」无法表达。
  默认值逐条对 Go:两个 cleanup_enabled 都是 **false**,retention_mode 空 = report_only。

★ kafka / session_gate / ds_auth 三段在 Go 是 `pkg/config.Base` 的成员,共享的
  `pandorapy.config.BaseConf` 还没有它们。这里在服务私有 Config 上补建模,是为了让
  **配了就会改变正确性**的段不落进 `model_extra` 被静默忽略:
    - kafka.brokers 空 = player 拒启(消费 player.update 是强依赖),落进 extra 就成了静默不消费;
    - session_gate.require=true 是 -Prod 生成器机械置的**拒启开关**;
    - ds_auth.mode 是 DS 面(:8444)那两条 RPC 的唯一身份证明,忽略它 = 安全门无声消失。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig
from pandorapy import dbguard
from pandorapy import internalrpcauth
from pandorapy import kafka_topics

DEFAULT_GRPC_ADDR = ":20002"
DEFAULT_HTTP_ADDR = ":21002"

# 与 Go 侧逐个同值。抽成常量而不是内联字面量,便于测试直接对着 Go 源码断言。
DEFAULT_BASE_MMR = 1500
DEFAULT_NICKNAME_PREFIX = "Player_"
DEFAULT_MAX_NICKNAME_LEN = 32
DEFAULT_MAX_EXP_PER_GRANT = 1_000_000
DEFAULT_PUSH_OUTBOX_INTERVAL_SEC = 1.0
DEFAULT_PUSH_OUTBOX_BATCH = 128
DEFAULT_PLAYER_NAME_RESOLVE_AUTH_AUDIENCE = "player:name"

# exp_history 留存期:下限 7 天(必须严格覆盖 battle_result progress 出箱最长重试窗),
# 上限 90 天(§9.24 硬上限:失效数据最多保留 90 天,不能只信配置)。
EXP_HISTORY_RETENTION_MIN = _dt.timedelta(days=7)
EXP_HISTORY_RETENTION_CAP = _dt.timedelta(days=90)

# mmr/点数/发卡幂等历史留存天数:默认 90,下限 30,上限 90。
HISTORY_RETENTION_DEFAULT_DAYS = 90
HISTORY_RETENTION_MIN_DAYS = 30
HISTORY_RETENTION_MAX_DAYS = 90

# 推送出箱发布器写者租约档位(与 Go 的同名常量语义一致)。
PUSH_WRITER_LEASE_OFF = "off"
PUSH_WRITER_LEASE_ENFORCE = "enforce"


class KafkaConf(BaseModel):
    """对应 Go 的 `pkg/config.KafkaConfig`(只建模 player 用得到的字段)。"""

    model_config = {"extra": "allow"}

    brokers: list[str] = Field(default_factory=list)
    group_id: str = ""
    partition_cnt: int = 0
    dial_timeout: str = ""
    retry_max: int = 0
    retry_backoff: str = ""
    compression_type: str = ""
    # Go 结构体零值是 false,但 kafkax.ProducerConf 的默认是 True。幂等 producer 是
    # 保序前提,默认关掉会让重试换序而无人察觉 —— 与 leaderboard 取同一档。
    # ★ Go 的真实默认是 **False**(结构体零值),不是注释里写的 "默认 true"。
    # pkg/config/config.go:239 的注释是 aspirational 的 —— 全仓没有任何代码给它填值,
    # producer.go:85 直穿 `c.Producer.Idempotent = cfg.Idempotent`。
    # 写成 True 的后果:35 份带 kafka 段的 yaml 里有 15 份没显式配它,
    # 那些服务上两栈的**重复消息语义**不同(幂等 producer 影响重试时的去重),
    # 而两边都不报错。
    idempotent: bool = False
    def dial_timeout_ms(self) -> int:
        return int(pconfig.parse_duration(self.dial_timeout).total_seconds() * 1000)

    def retry_backoff_ms(self) -> int:
        return int(pconfig.parse_duration(self.retry_backoff).total_seconds() * 1000)


class SessionGateConf(BaseModel):
    """对应 Go 的 `pkg/config.SessionGateConf`。

    require=false(dev 宽松档):gate 装配上了就照常判定,只有**漏配会话权威端点**时跳过;
    require=true(-Prod 生成器机械置):漏配端点直接拒启。
    """

    model_config = {"extra": "allow"}

    require: bool = False


class DSAuthConf(BaseModel):
    """对应 Go 的 `pkg/config.DSAuthConf`(player 只**验签**不签发)。

    签发侧字段(battle_token_ttl / hub_token_ttl / fence / authority_mode)在 player
    上不生效,但仍要建模并保留默认值 —— dev 模板里带着它们,不建模会落进 extra,
    将来某人以为 player 也能签发。
    """

    model_config = {"extra": "allow"}

    mode: str = ""
    issuer: str = ""
    audience: str = ""
    secret: str = ""
    additional_secrets: list[str] = Field(default_factory=list)
    authority_mode: str = ""
    battle_token_ttl: str = ""
    hub_token_ttl: str = ""
    active_heartbeat_max_age: str = ""

    def apply_defaults(self) -> None:
        """对应 Go 的 `DSAuthConf.Defaults()`。mode/secret 留空即"不启用",不填默认。"""
        if not self.authority_mode:
            self.authority_mode = "legacy"
        if not self.issuer:
            self.issuer = "pandora-ds-control"
        if not self.audience:
            self.audience = "pandora-ds"
        if not self.battle_token_ttl:
            self.battle_token_ttl = "4h"
        if not self.hub_token_ttl:
            self.hub_token_ttl = "24h"
        if not self.active_heartbeat_max_age:
            self.active_heartbeat_max_age = "30s"


class PushWriterLeaseConf(BaseModel):
    """player_push_outbox 发布器的单写者选举 —— 对应 Go 的 PushWriterLeaseConf。

    为什么 player 必须单写者:player_push_outbox 是**全局未分区**表,发布器按 id 升序
    整表 FIFO 取行,属 §9.21「作用于同一未分区权威的单写者循环」。而
    PlayerExperienceEvent 携带的是**绝对值快照**(level / exp_in_level,不是增量),
    两个副本交错投递会让旧快照后到并覆盖新的 —— 玩家看到等级/经验条**倒退**。
    事件里没有 revision;ts_ms 是各副本墙钟,不得只靠它判重。
    """

    model_config = {"extra": "allow"}

    mode: str = ""
    etcd_endpoints: list[str] = Field(default_factory=list)
    lease_ttl_sec: int = 0
    dial_timeout: str = ""

    def resolve_mode(self) -> str:
        """归一化档位,取值不认识**报错而非猜**。

        不设这道闸会怎样:`mode: "enfore"` 静默退回无保护并发发布,滚动重叠期两个
        发布器同时排空出箱,玩家的经验条来回跳,而 yaml 上写着 enforce。
        """
        text = (self.mode or "").strip().lower()
        if text in ("", PUSH_WRITER_LEASE_OFF):
            return PUSH_WRITER_LEASE_OFF
        if text == PUSH_WRITER_LEASE_ENFORCE:
            return PUSH_WRITER_LEASE_ENFORCE
        raise ValueError(
            f"player.push_writer_lease.mode={self.mode!r} 不认识"
            f"(只允许 {PUSH_WRITER_LEASE_OFF!r} / {PUSH_WRITER_LEASE_ENFORCE!r},留空={PUSH_WRITER_LEASE_OFF!r})"
        )

    def dial_timeout_sec(self) -> float:
        return pconfig.parse_duration(self.dial_timeout).total_seconds()


class PlayerConf(BaseModel):
    """player 私有配置段。字段名与 Go 的 yaml tag 逐字一致。"""

    model_config = {"extra": "allow"}

    base_mmr: int = 0
    mmr_floor: int = 0
    default_nickname_prefix: str = ""
    max_nickname_len: int = 0
    # 校验 team→player PlayerInternalService.ResolvePlayerNames 的独立
    # request-bound internalrpcauth 身份。
    player_name_resolve_auth_secret: str = ""
    player_name_resolve_auth_audience: str = ""
    # friend/guild 各持独立 caller key；不得复用上面的 team key。
    friend_player_name_resolve_auth_secret: str = ""
    friend_player_name_resolve_auth_audience: str = ""
    guild_player_name_resolve_auth_secret: str = ""
    guild_player_name_resolve_auth_audience: str = ""
    hero_selection_enabled: bool = False
    loadout_customize_enabled: bool = False
    inventory_addr: str = ""
    consume_topics: list[str] = Field(default_factory=list)

    experience_enabled: bool = False
    max_exp_per_grant: int = 0
    push_outbox_interval: str = ""
    push_outbox_batch: int = 0

    push_writer_lease: PushWriterLeaseConf = Field(default_factory=PushWriterLeaseConf)

    # 留空 / "report_only" / "report" = 只统计待清理量并 WARN,一行都不删(默认);
    # "delete" = 真删。无法识别的值由 validate_retention_mode 在启动期拒启。
    retention_mode: str = ""
    exp_history_cleanup_enabled: bool = False
    exp_history_retention: str = ""
    history_cleanup_enabled: bool = False
    history_retention_days: int = 0

    # ── 生效值访问器(对应 Go 的 *OrDefault)────────────────────────────

    def max_exp_per_grant_effective(self) -> int:
        """单次入账上限。判据 `> 0`,与 Go 同(配 0 / 负数都回默认)。"""
        return self.max_exp_per_grant if self.max_exp_per_grant > 0 else DEFAULT_MAX_EXP_PER_GRANT

    def push_outbox_interval_sec(self) -> float:
        d = pconfig.parse_duration(self.push_outbox_interval).total_seconds()
        return d if d > 0 else DEFAULT_PUSH_OUTBOX_INTERVAL_SEC

    def push_outbox_batch_effective(self) -> int:
        return self.push_outbox_batch if self.push_outbox_batch > 0 else DEFAULT_PUSH_OUTBOX_BATCH

    def exp_history_retention_effective(self) -> _dt.timedelta:
        """exp_history 留存期(未配置 → 7 天;低于 7 天按 7 天,高于 90 天按 90 天)。

        下限防手滑把幂等窗清穿(收据被提前删 = 上游重放时同一笔经验二次入账);
        上限是 §9.24 硬钳:失效数据最多保留 90 天,不能只信配置。
        """
        d = pconfig.parse_duration(self.exp_history_retention)
        if d < EXP_HISTORY_RETENTION_MIN:
            return EXP_HISTORY_RETENTION_MIN
        if d > EXP_HISTORY_RETENTION_CAP:
            return EXP_HISTORY_RETENTION_CAP
        return d

    def history_retention_effective(self) -> _dt.timedelta:
        """mmr/点数授予/发卡幂等历史留存期(默认 90 天,下限 30,上限 90)。

        ★ **先钳天数整数再乘 24h**,与 Go 逐字一致(审计 P1:先乘后判时,极大天数乘
        24h 在 Go 里溢出为负,会误落 floor 分支返回 30 天,清理开启时提前删幂等收据)。
        Python 的 int 不会溢出,但保持同一顺序 —— 两边的钳位结果必须逐字相同。
        """
        days = self.history_retention_days
        if days <= 0:
            days = HISTORY_RETENTION_DEFAULT_DAYS
        if days < HISTORY_RETENTION_MIN_DAYS:
            days = HISTORY_RETENTION_MIN_DAYS
        if days > HISTORY_RETENTION_MAX_DAYS:
            days = HISTORY_RETENTION_MAX_DAYS
        return _dt.timedelta(days=days)

    # ── 保留期模式(两道闸)──────────────────────────────────────────────

    def retention_mode_parsed(self) -> dbguard.Mode:
        """本服生效的清理总闸。**无法识别时回落 REPORT_ONLY**(不删更安全)。

        与 validate_retention_mode 分工不同:这个是运行期取值(绝不能因为配错就去删
        数据),validate 那个是启动期 fail-fast。
        """
        try:
            return dbguard.parse_mode(self.retention_mode)
        except ValueError:
            return dbguard.Mode.REPORT_ONLY

    def validate_retention_mode(self) -> None:
        """启动期 fail-fast 用。对应 Go 的 ValidateRetentionMode。

        只回落默认是不够的:运维以为配了 delete、实际一行没删,库继续增长且启动期
        毫无痕迹。拼错一个字母就开始删生产数据同样不可接受 —— 所以既不猜也不忽略。
        """
        dbguard.parse_mode(self.retention_mode)

    def exp_history_retention_mode(self) -> dbguard.Mode:
        """exp_history 这一组生效的模式:总闸与本组前置条件**都开**才删。"""
        return _gate_delete(self.retention_mode_parsed(), self.exp_history_cleanup_enabled)

    def history_retention_mode(self) -> dbguard.Mode:
        """mmr_history / attr_point_grants / talent_point_grants / skill_card_grants 这一组。"""
        return _gate_delete(self.retention_mode_parsed(), self.history_cleanup_enabled)


def _gate_delete(mode: dbguard.Mode, precondition: bool) -> dbguard.Mode:
    """把"前置条件没确认"表达成降级到 report_only,而**不是**干脆不跑 janitor。

    不跑就等于既不删也不报,§9.24 要的待清理量(WARN + pending gauge)会整个消失,
    库在无人知晓的情况下继续涨 —— Go 侧早先就是这么写的,后来改成了降级。
    """
    if not precondition:
        return dbguard.Mode.REPORT_ONLY
    return mode


class Config(pconfig.BaseConf):
    """player 服务的完整配置。对应 Go 的 conf.Config。"""

    player: PlayerConf = Field(default_factory=PlayerConf)
    kafka: KafkaConf = Field(default_factory=KafkaConf)
    session_gate: SessionGateConf = Field(default_factory=SessionGateConf)
    ds_auth: DSAuthConf = Field(default_factory=DSAuthConf)

    def apply_defaults(self) -> None:
        """填默认值 —— 对应 Go 的 Defaults()。判据符号逐条与 Go 相同,见模块头。"""
        self.ds_auth.apply_defaults()
        p = self.player
        if p.base_mmr <= 0:
            p.base_mmr = DEFAULT_BASE_MMR
        # ★ `< 0` 不是 `<= 0`:mmr_floor=0 是合法且常用的配置(dev 就写 0)。
        if p.mmr_floor < 0:
            p.mmr_floor = 0
        if not p.default_nickname_prefix:
            p.default_nickname_prefix = DEFAULT_NICKNAME_PREFIX
        if p.max_nickname_len <= 0:
            p.max_nickname_len = DEFAULT_MAX_NICKNAME_LEN
        if (
            p.player_name_resolve_auth_secret
            and not p.player_name_resolve_auth_audience
        ):
            p.player_name_resolve_auth_audience = (
                DEFAULT_PLAYER_NAME_RESOLVE_AUTH_AUDIENCE
            )
        if (
            p.friend_player_name_resolve_auth_secret
            and not p.friend_player_name_resolve_auth_audience
        ):
            p.friend_player_name_resolve_auth_audience = (
                DEFAULT_PLAYER_NAME_RESOLVE_AUTH_AUDIENCE
            )
        if (
            p.guild_player_name_resolve_auth_secret
            and not p.guild_player_name_resolve_auth_audience
        ):
            p.guild_player_name_resolve_auth_audience = (
                DEFAULT_PLAYER_NAME_RESOLVE_AUTH_AUDIENCE
            )
        if not p.consume_topics:
            p.consume_topics = [kafka_topics.TOPIC_PLAYER_UPDATE]
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR

    def validate_player_name_resolver(self) -> None:
        """兼容既有调用点；完整校验由复数版本统一执行。"""
        self.validate_player_name_resolvers()

    def validate_player_name_resolvers(self) -> None:
        """三个 caller 的名称解析凭据必须成对且 key 两两独立。"""
        p = self.player
        resolver_credentials = (
            (
                "team",
                "player_name_resolve_auth",
                p.player_name_resolve_auth_secret,
                p.player_name_resolve_auth_audience,
            ),
            (
                "friend",
                "friend_player_name_resolve_auth",
                p.friend_player_name_resolve_auth_secret,
                p.friend_player_name_resolve_auth_audience,
            ),
            (
                "guild",
                "guild_player_name_resolve_auth",
                p.guild_player_name_resolve_auth_secret,
                p.guild_player_name_resolve_auth_audience,
            ),
        )
        used_secrets: dict[str, str] = {}
        for caller, label, secret, audience in resolver_credentials:
            if not secret:
                if audience:
                    raise ValueError(
                        f"player.{label}_audience requires {label}_secret"
                    )
                continue
            try:
                internalrpcauth.validate_secret(secret)
                internalrpcauth.validate_identity(audience)
            except ValueError as exc:
                raise ValueError(f"player.{label}: {exc}") from exc
            previous = used_secrets.get(secret)
            if previous is not None:
                raise ValueError(
                    "player name resolver auth secret reused between "
                    f"{previous} and {caller} callers"
                )
            used_secrets[secret] = caller
        if used_secrets and not (
            self.node.redis_client.host or self.node.redis_client.addrs
        ):
            raise ValueError(
                "player name resolver auth requires node.redis_client "
                "replay authority"
            )

    @classmethod
    def load(cls, path: str) -> "Config":
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
