"""team 服务配置 —— 对应 Go 侧 internal/conf/conf.go + pkg/config.DSAuthConf。

读的是**同一份** services/matchmaking/team/etc/team-dev.yaml,不另建配置文件:
迁移期 Go 版和 Python 版并存,运维只该维护一份配置。

★ 默认值必须与 Go 的 `Defaults()` 逐字段相同,**连判据符号都要一样**。
  team 这里全部用 `== 0` / `== ""`(不是 `<= 0`)。差别是真实的:
  `max_members: -1` 在 Go 侧**不会**被兜成 5,而是带着负值往下走
  (于是任何 `len(members) >= max_size` 恒成立 → 队伍恒"满")。Python 若写成
  `<= 0`,同一份 yaml 会让 Python 副本得到 5 而 Go 副本得到 -1,**两边都不报错**。
  所以这里照抄 `== 0`。

★ kafka / session_gate 两段在 Go 是 `pkg/config.Base` 的成员,Python 共享的
  `pandorapy.config.BaseConf` 还没有它们(见交付说明 shared_files_needed)。
  这里在服务私有 Config 上补建模,是为了让**配了就会改变正确性**的段不落进
  model_extra 被静默忽略:
    - kafka.brokers 非空 = 队伍推送是**启动强依赖**(见 main.py 闸⑦),
      落进 extra 的话 Python 副本会读到空 brokers 而按"无 kafka 开发模式"启动,
      Invite 只写令牌不推送,而 yaml 明明配了 broker;
    - session_gate.require=true 是 prod 生成器机械置的**拒启开关**,
      忽略它等于把 fail-closed 悄悄降级成 fail-open。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig
from pandorapy import internalrpcauth

# 端口与 Go 的 Defaults() 同值。Envoy cluster / run_services.ps1 的端口占用检查
# 都钉在 20010 / 21010,改这里等于改部署。
DEFAULT_GRPC_ADDR = ":20010"
DEFAULT_HTTP_ADDR = ":21010"

# 与 Go 侧逐个同值(抽成常量而不是内联,便于测试直接对着 Go 源码断言)。
DEFAULT_INVITE_TTL = "60s"
DEFAULT_DISBANDED_RETENTION = "5m"
DEFAULT_ACTIVE_TTL = "60m"
DEFAULT_MAX_MEMBERS = 5
DEFAULT_OPTIMISTIC_RETRY = 3
DEFAULT_INVITE_PUSH_MODE = "dual"
DEFAULT_MAX_PENDING_INVITES = 10
DEFAULT_MAX_OPEN_TEAMS_PER_QUERY = 10
DEFAULT_MAX_APPLICATIONS_PER_TEAM = 10
DEFAULT_RATE_QUOTA_PER_MIN = 12
DEFAULT_APPLY_TTL = "120s"
DEFAULT_PLAYER_NO_RESOLVER_AUTH_AUDIENCE = "login:player-no"
DEFAULT_PLAYER_NAME_RESOLVER_AUTH_AUDIENCE = "player:name"

DEFAULT_OFFLINE_THRESHOLD = "180s"
DEFAULT_OFFLINE_CHECK_INTERVAL = "15s"
DEFAULT_OFFLINE_BUDGET = 200
DEFAULT_OFFLINE_KAFKA_PARTITIONS = 3

DEFAULT_DS_AUTH_AUTHORITY_MODE = "legacy"
DEFAULT_DS_AUTH_ISSUER = "pandora-ds-control"
DEFAULT_DS_AUTH_AUDIENCE = "pandora-ds"
DEFAULT_DS_AUTH_BATTLE_TOKEN_TTL = "4h"
DEFAULT_DS_AUTH_HUB_TOKEN_TTL = "24h"
DEFAULT_DS_AUTH_ACTIVE_HEARTBEAT_MAX_AGE = "30s"

# 入队策略取值(JoinPolicy),与 Go 的常量同值。
JOIN_POLICY_APPROVAL = "approval"
JOIN_POLICY_OPEN = "open"


def _zero(raw: str) -> bool:
    """Go 的 `config.Duration == 0` 判据。

    空串 / 缺字段 → parse 出 0 → 视为未配置。**负值不算零**,与 Go 一致
    (Go 的 Duration 是纳秒整数,-1s 是非零,不会被 Defaults 覆盖)。
    """
    return pconfig.parse_duration(raw).total_seconds() == 0


def parse_join_policy(s: str) -> str:
    """把配置字符串解析成合法策略值。对应 Go 的 `ParseJoinPolicy`。

    空串 = 默认 approval(保守)。**无法识别的值报错,绝不猜**:把 "aproval" 猜成
    open 会让全服队伍对陌生人敞开,属于静默的权限放大,失败模式不可接受
    (同 dbguard.parse_mode 对 retention_mode 的处理口径)。
    """
    if s == "":
        return JOIN_POLICY_APPROVAL
    if s in (JOIN_POLICY_APPROVAL, JOIN_POLICY_OPEN):
        return s
    raise ValueError(
        f'team: unknown join_policy "{s}" '
        f'(want "{JOIN_POLICY_APPROVAL}" or "{JOIN_POLICY_OPEN}")'
    )


class OfflineLeaveConf(BaseModel):
    """「队员离线超时自动移出队伍」的配置。对应 Go 的 OfflineLeaveConf。"""

    model_config = {"extra": "allow"}

    enabled: bool = False

    # 离线多久才移出队伍。默认 180s。
    #
    # 取值依据(**不要随手调小**,推导见 Go conf.go 的同名字段注释):
    #  ① 下界 = 一个会回来的玩家最快多久能回来(旧 Controller 清退 60s + Travel +
    #     Hub 地图加载 + Admission,保守 30~60s)。低于这个数,只断了几秒网、
    #     立刻开始重连的玩家也会被踢,是纯误伤。
    #  ② 上界 = 弱网玩家的连续失联时长(地铁 / 隧道 ~2 分钟)。
    #  ③ 还要叠加**检测本身的延迟**(UE ConnectionTimeout 60s + locator grace 10s),
    #     玩家实际被容忍 ≈ 阈值 + 60~90s。
    threshold: str = ""

    # 到期复查轮询周期。默认 15s。上游检测本身就有 60~90s 的模糊,
    # 压到秒级只是成倍花钱买不到精度。
    check_interval: str = ""

    # 单轮最多处理多少个到期玩家。默认 200(防积压一次性打爆 locator / matchmaker)。
    budget: int = 0

    # 离场事件 topic 的分区数(消费者按分区起 worker)。默认 3。
    kafka_partitions: int = 0

    def threshold_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.threshold)

    def check_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.check_interval)


class TeamConf(BaseModel):
    """team 私有配置段。字段名与 Go 的 yaml tag 逐字一致。"""

    model_config = {"extra": "allow"}

    # 邀请令牌 Redis key 的 TTL,客户端须在此时间内 AcceptInvite。
    invite_ttl: str = ""
    # 队伍解散后 Redis key 的保留时长,供客户端查询最终状态。
    disbanded_retention: str = ""
    # 活跃队伍(未解散)Redis key 的生命周期。队伍在此时间内无任何写操作则整体过期,
    # 防止僵尸队伍长期占用 Redis。
    active_ttl: str = ""
    # MOBA 5v5,一队最多允许多少成员。
    max_members: int = 0
    # WATCH/MULTI/EXEC 乐观锁冲突时最大重试次数。耗尽返 ErrTeamConcurrent(3007)。
    optimistic_retry: int = 0

    # matchmaker 服务 gRPC 直连地址(host:port,内网 insecure)。
    # 留空 → 不联动撤票、入队闸门跳过(弱依赖)。
    matchmaker_addr: str = ""

    # 组装客户端 TeamMember 时批量读取 login/account 权威 player_no。addr 留空时
    # 展示字段降级为 0；配置 addr 后 auth 必须齐全，禁止带着一个永远验不过的假接线启动。
    player_no_resolver_addr: str = ""
    player_no_resolver_auth_secret: str = ""
    player_no_resolver_auth_audience: str = ""

    # team→player ResolvePlayerNames 的独立内部批量读取。留空 addr 时
    # TeamMember.nickname 降级为空；启用时 HMAC 凭据必须闭合。
    player_name_resolver_addr: str = ""
    player_name_resolver_auth_secret: str = ""
    player_name_resolver_auth_audience: str = ""

    # team→matchmaker 调 ResolvePlayerMatchContext 的东西向服务鉴权凭据,
    # caller 固定签 "team"。matchmaker 侧对该方法强制验签,不签名一律 code=7。
    # ★ 这把密钥必须与 login 那把 match_resume_auth_secret **不同**。
    match_resume_auth_secret: str = ""
    match_resume_auth_audience: str = ""

    # 反方向:matchmaker → team 的组票 / 复位调用鉴权(INC-20260813-001 A-13)。
    # ★ 必须与 match_resume_auth_secret **不同** —— 那把是 team 签**出站**用的,
    #   共用等于让任一方能冒充另一方。
    # 三档:留空=不验;配了+require=false(观察期,验不过只 WARN 放行);require=true=强制。
    match_call_auth_secret: str = ""
    match_call_auth_audience: str = ""
    match_call_auth_require: bool = False

    # 邀请推送模式(金丝雀灰度用)。dual(默认)/ dedicated / legacy,见 biz.invite。
    invite_push_mode: str = ""

    # 同一被邀请人的未过期 pending 邀请数上限(§9-18)。默认 10。
    max_pending_invites: int = 0

    # ApplyToTeam 的语义,全服一份配置(不给每支队伍存,§9.22 不重复影子状态)。
    # 空串按 approval 处理:默认保守 —— 不经队长同意就把陌生人塞进队伍属于
    # 「多做了玩家没授权的事」,反过来只是多一次点击。
    join_policy: str = ""

    # 单次 ListOpenTeams 返回的队伍数上限(读取侧上限)。默认 10。
    max_open_teams_per_query: int = 0
    # 同一队伍未过期 pending 入队申请数上限(写入侧上限)。默认 10。
    max_applications_per_team: int = 0
    # 申请/邀请的 per-player 每分钟频率配额(anti-abuse §6 第 6 项)。默认 12;
    # **负值 = 关闭**(所以判据必须是 `== 0` 而不是 `<= 0`)。
    rate_quota_per_min: int = 0

    # player_locator 服务 gRPC 直连地址,离线观察的判定依据来源。
    locator_addr: str = ""

    offline_leave: OfflineLeaveConf = Field(default_factory=OfflineLeaveConf)

    # 入队申请令牌的存活时长。默认 120s(邀请是 60s,申请方向多一次队长注意力成本)。
    apply_ttl: str = ""

    def invite_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.invite_ttl)

    def disbanded_retention_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.disbanded_retention)

    def active_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.active_ttl)

    def apply_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.apply_ttl)

    def join_policy_effective(self) -> str:
        """当前生效的入队策略(运行期取值)。

        启动时 validate_join_policy 已 fail-fast,正常永远解析成功;万一运行期配置被
        改坏,一律退回最保守的 approval —— 绝不因为解析失败就把全服队伍对陌生人敞开。
        """
        try:
            return parse_join_policy(self.join_policy)
        except ValueError:
            return JOIN_POLICY_APPROVAL


class KafkaConf(BaseModel):
    """对应 Go 的 `pkg/config.KafkaConfig`(只建模 team 用得到的字段)。"""

    model_config = {"extra": "allow"}

    brokers: list[str] = Field(default_factory=list)
    partition_cnt: int = 0
    dial_timeout: str = ""
    write_timeout: str = ""
    retry_max: int = 0
    retry_backoff: str = ""
    compression_type: str = ""
    # Go 结构体零值是 false,但 team-dev.yaml 显式写 true,且幂等 producer 是保序前提。
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

    def configured(self) -> bool:
        """对应 Go 的 initializeTeamPublication 里那段 `TrimSpace(broker) != ""` 判定。

        **不是** `len(brokers) > 0`:yaml 里写 `brokers: [""]` 时 Go 判为未配置
        (纯 RPC 本地调试模式),Python 若按长度判会去建一个连不上的 producer 然后
        fail-fast —— 同一份 yaml 两边行为相反。
        """
        return any(b.strip() for b in self.brokers)


class SessionGateConf(BaseModel):
    """对应 Go 的 `pkg/config.SessionGateConf`。

    require=false(dev 宽松档):gate 装配上了就照常判定,只有**漏配会话权威端点**时跳过;
    require=true(-Prod 生成器机械置):漏配端点直接拒启。
    """

    model_config = {"extra": "allow"}

    require: bool = False


class DSAuthConf(BaseModel):
    """对应 Go 的 `pkg/config.DSAuthConf`。team 只做**校验侧**(verify-only)。

    为什么 team 需要它:GetPlayerTeam 是 DS 出生编制专用的东西向反查,挂在 Envoy DS 面,
    该监听器没有 jwt_authn,callerID 恒为 0 —— systemOnly 只能挡「带玩家 JWT 的客户端」,
    挡不住「任意能连 DS 面或直连 20010 的进程查任意玩家在哪支队」。
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
        """本服务是否启用 DS 回调令牌校验。mode 空 / "off" = 不校验。"""
        return self.mode not in ("", "off")


class Config(pconfig.BaseConf):
    """team 服务的完整配置。对应 Go 的 conf.Config。"""

    team: TeamConf = Field(default_factory=TeamConf)
    kafka: KafkaConf = Field(default_factory=KafkaConf)
    session_gate: SessionGateConf = Field(default_factory=SessionGateConf)
    ds_auth: DSAuthConf = Field(default_factory=DSAuthConf)

    def apply_defaults(self) -> None:
        """填默认值 —— 对应 Go 的 `Defaults()`,**顺序与判据符号逐条同**。

        零值在这里都是危险的而不是"中性的":
          - max_members=0     → 任何队伍恒"满",谁都进不去;
          - optimistic_retry=0→ 乐观锁一冲突就返回 3007,热点队伍寸步难行;
          - invite_ttl=0      → 令牌写进 Redis 即刻过期(PEXPIRE 0 会报错),邀请全废;
          - active_ttl=0      → 队伍 key 立刻消失,建完就没。
        """
        self.ds_auth.apply_defaults()
        t = self.team
        if _zero(t.invite_ttl):
            t.invite_ttl = DEFAULT_INVITE_TTL
        if _zero(t.disbanded_retention):
            t.disbanded_retention = DEFAULT_DISBANDED_RETENTION
        if _zero(t.active_ttl):
            t.active_ttl = DEFAULT_ACTIVE_TTL
        if t.max_members == 0:
            t.max_members = DEFAULT_MAX_MEMBERS
        if t.optimistic_retry == 0:
            t.optimistic_retry = DEFAULT_OPTIMISTIC_RETRY
        if (
            t.player_no_resolver_addr != ""
            and t.player_no_resolver_auth_audience == ""
        ):
            t.player_no_resolver_auth_audience = (
                DEFAULT_PLAYER_NO_RESOLVER_AUTH_AUDIENCE
            )
        if (
            t.player_name_resolver_addr != ""
            and t.player_name_resolver_auth_audience == ""
        ):
            t.player_name_resolver_auth_audience = (
                DEFAULT_PLAYER_NAME_RESOLVER_AUTH_AUDIENCE
            )
        if t.invite_push_mode == "":
            # 金丝雀期默认双发:老/新客户端各认各的 payload,互不干扰,各弹一次。
            t.invite_push_mode = DEFAULT_INVITE_PUSH_MODE
        if t.max_pending_invites == 0:
            t.max_pending_invites = DEFAULT_MAX_PENDING_INVITES
        if t.join_policy == "":
            # 保守默认:需要队长同意。改成 open 必须是显式配置动作。
            t.join_policy = JOIN_POLICY_APPROVAL
        if t.max_open_teams_per_query == 0:
            t.max_open_teams_per_query = DEFAULT_MAX_OPEN_TEAMS_PER_QUERY
        if t.max_applications_per_team == 0:
            t.max_applications_per_team = DEFAULT_MAX_APPLICATIONS_PER_TEAM
        if t.rate_quota_per_min == 0:
            t.rate_quota_per_min = DEFAULT_RATE_QUOTA_PER_MIN
        if _zero(t.apply_ttl):
            t.apply_ttl = DEFAULT_APPLY_TTL
        if _zero(t.offline_leave.threshold):
            t.offline_leave.threshold = DEFAULT_OFFLINE_THRESHOLD
        if _zero(t.offline_leave.check_interval):
            t.offline_leave.check_interval = DEFAULT_OFFLINE_CHECK_INTERVAL
        if t.offline_leave.budget == 0:
            t.offline_leave.budget = DEFAULT_OFFLINE_BUDGET
        if t.offline_leave.kafka_partitions == 0:
            t.offline_leave.kafka_partitions = DEFAULT_OFFLINE_KAFKA_PARTITIONS
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR

    def validate_join_policy(self) -> None:
        """启动期 fail-fast。对应 Go 的 `ValidateJoinPolicy`。

        不设这道闸:拼错一个字母(如 "aproval")若被猜成 open,会让全服队伍对任何人
        敞开 —— 这是静默的权限放大,必须启动就拒,不能等第一个玩家点"申请"才暴露。
        """
        parse_join_policy(self.team.join_policy)

    def validate_player_no_resolver(self) -> None:
        """与 Go ValidatePlayerNoResolver 同形：启用时凭据与批量上限必须闭合。"""
        t = self.team
        if not t.player_no_resolver_addr:
            if (
                t.player_no_resolver_auth_secret
                or t.player_no_resolver_auth_audience
            ):
                raise ValueError(
                    "team.player_no_resolver_auth_secret/audience requires "
                    "team.player_no_resolver_addr"
                )
            return
        try:
            internalrpcauth.validate_secret(t.player_no_resolver_auth_secret)
        except ValueError as exc:
            raise ValueError(
                f"team.player_no_resolver_auth_secret: {exc}"
            ) from exc
        try:
            internalrpcauth.validate_identity(t.player_no_resolver_auth_audience)
        except ValueError as exc:
            raise ValueError(
                f"team.player_no_resolver_auth_audience: {exc}"
            ) from exc
        if t.max_members < 1 or t.max_members > 32:
            raise ValueError(
                "team.max_members must be within [1,32] when player_no resolver is enabled"
            )
        if t.max_applications_per_team < 1 or t.max_applications_per_team > 32:
            raise ValueError(
                "team.max_applications_per_team must be within [1,32] when player_no resolver is enabled"
            )
        if t.max_open_teams_per_query < 1 or t.max_open_teams_per_query > 32:
            raise ValueError(
                "team.max_open_teams_per_query must be within [1,32] when player_no resolver is enabled"
            )

    def validate_player_name_resolver(self) -> None:
        """启用 team→player 名称解析时，地址、独立凭据和批上限必须闭合。"""
        t = self.team
        if not t.player_name_resolver_addr:
            if (
                t.player_name_resolver_auth_secret
                or t.player_name_resolver_auth_audience
            ):
                raise ValueError(
                    "team.player_name_resolver_auth_secret/audience requires "
                    "team.player_name_resolver_addr"
                )
            return
        try:
            internalrpcauth.validate_secret(t.player_name_resolver_auth_secret)
        except ValueError as exc:
            raise ValueError(
                f"team.player_name_resolver_auth_secret: {exc}"
            ) from exc
        try:
            internalrpcauth.validate_identity(t.player_name_resolver_auth_audience)
        except ValueError as exc:
            raise ValueError(
                f"team.player_name_resolver_auth_audience: {exc}"
            ) from exc
        if t.max_members < 1 or t.max_members > 32:
            raise ValueError(
                "team.max_members must be within [1,32] when player_name resolver is enabled"
            )
        if t.max_applications_per_team < 1 or t.max_applications_per_team > 32:
            raise ValueError(
                "team.max_applications_per_team must be within [1,32] when player_name resolver is enabled"
            )
        if t.max_open_teams_per_query < 1 or t.max_open_teams_per_query > 32:
            raise ValueError(
                "team.max_open_teams_per_query must be within [1,32] when player_name resolver is enabled"
            )

    def validate_offline_leave(self) -> None:
        """启动期 fail-fast。对应 Go 的 `ValidateOfflineLeave`。

        不设这道闸:开了却缺 locator / matchmaker 地址,功能会**静默不生效**
        (或更糟:缺了对局闸门就有拆掉正在打的队伍的风险)。这类「配了却不生效」
        的失败模式必须启动就暴露,不能等第一个玩家掉线才发现。
        """
        if not self.team.offline_leave.enabled:
            return
        if not self.team.locator_addr:
            raise ValueError(
                "team: offline_leave.enabled=true requires team.locator_addr"
            )
        if not self.team.matchmaker_addr:
            # 没有 matchmaker 就没法判「这支队伍是不是正被一场对局占住」(整场战斗期间的
            # 占用权威是 player→ticket claim,不是那把秒级 roster 租约),自动退队会有
            # 把在打的队伍拆掉的风险,宁可不启动。
            raise ValueError(
                "team: offline_leave.enabled=true requires team.matchmaker_addr "
                "(match commitment gate)"
            )

    @classmethod
    def load(cls, path: str) -> "Config":
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
