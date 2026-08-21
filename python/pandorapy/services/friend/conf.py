"""friend 服务配置 —— 对应 Go 侧 services/social/friend/internal/conf/conf.go。

读的是**同一份** services/social/friend/etc/friend-dev.yaml,不另建配置文件:
迁移期 Go 版和 Python 版并存,运维只该维护一份配置。

★ 默认值必须与 Go 的 `Defaults()` **逐个相同,连判据符号也一样**。
  friend 这里有一处**唯一的 `== 0`**:`rate_quota_per_min`。
  Go 写的是 `if c.Friend.RateQuotaPerMin == 0 { = 10 }`,而不是 `<= 0` ——
  因为字段注释里定义了「负值 = 关闭限流」。若 Python 抄成 `<= 0`,
  同一份写着 `rate_quota_per_min: -1` 的 yaml 会让:
      Go     → 保持 -1 → quota(limit<=0) 不限流(运维本意)
      Python → 兜成 10 → 每分钟 10 条就开始回 ERR_RATE_LIMITED
  **两边都不报错**,只有玩家看到"有的副本加好友被限流、有的不会"。

★ kafka / session_gate 两段在 Go 是 `pkg/config.Base` 的成员,Python 侧共享的
  `pandorapy.config.BaseConf` 还没有它们(见交付说明 shared_files_needed)。
  这里在服务私有 Config 上补建模,是为了让**配了就会改变正确性**的段不落进
  `model_extra` 被静默忽略:
    - kafka.brokers 留空 = 好友请求 / 接受推送不外发(Go 打 kafka_brokers_empty 的
      WARN),落进 extra 的话 Python 副本会永远静默不发,而 yaml 明明配了 broker;
    - session_gate.require=true 是 prod 生成器机械置的**拒启开关**,
      忽略它等于把 fail-closed 悄悄降级成 fail-open。
  等共享 BaseConf 补齐后,把这两段从这里删掉即可(字段名/默认值已按 Go 对齐)。
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig
from pandorapy import dbguard
from pandorapy import internalrpcauth

DEFAULT_GRPC_ADDR = ":20004"
DEFAULT_HTTP_ADDR = ":21004"

# 与 Go 侧 Defaults() 逐个同值。抽成常量而不是内联字面量,是为了让
# tests/test_friend_conf.py 能直接对着 Go 源码断言(漂移当场变红)。
DEFAULT_MAX_FRIENDS = 200
DEFAULT_MAX_INCOMING_REQUESTS = 200
DEFAULT_MAX_BLOCKS = 200
DEFAULT_RATE_QUOTA_PER_MIN = 10
DEFAULT_RECOMMEND_LIMIT = 10
# 硬上限:无论 conf / 请求填多少,推荐数绝不超过 20(Go biz.recommendMaxLimit)。
RECOMMEND_MAX_LIMIT = 20
DEFAULT_REQUEST_RETENTION_DAYS = 90
DEFAULT_SWEEP_INTERVAL_SEC = 300.0  # Go: 5 * time.Minute
DEFAULT_SWEEP_BATCH = 500
DEFAULT_PAIR_GUARD_RETENTION_DAYS = 30
DEFAULT_PLAYER_NAME_RESOLVER_AUTH_AUDIENCE = "player:name"
DEFAULT_PLAYER_NO_RESOLVER_AUTH_AUDIENCE = "login:player-no"

# 推荐策略链缺省顺序(Go buildStrategies:名单为空 → [mutual, random])。
DEFAULT_RECOMMEND_STRATEGIES = ("mutual", "random")


class KafkaConf(BaseModel):
    """对应 Go 的 `pkg/config.KafkaConfig`(只建模 friend 用得到的字段)。

    friend 只做 producer(pandora.friend.event),消费侧字段(group_id / read_timeout
    ...)配了也不改变本服务行为,由 extra 吸收。
    """

    model_config = {"extra": "allow"}

    brokers: list[str] = Field(default_factory=list)
    partition_cnt: int = 0
    dial_timeout: str = ""
    write_timeout: str = ""
    retry_max: int = 0
    retry_backoff: str = ""
    compression_type: str = ""
    # Go 的结构体零值是 false,但 kafkax.ProducerConf 的默认是幂等 producer,
    # 而幂等是保序前提(§9.9 同一接收方事件保序)。默认关掉会让重试换序而无人察觉。
    # ★ Go 的真实默认是 **False**(结构体零值),不是注释里写的 "默认 true"。
    # pkg/config/config.go:239 的注释是 aspirational 的 —— 全仓没有任何代码给它填值,
    # producer.go:85 直穿 `c.Producer.Idempotent = cfg.Idempotent`。
    # 写成 True 的后果:35 份带 kafka 段的 yaml 里有 15 份没显式配它,
    # 那些服务上两栈的**重复消息语义**不同(幂等 producer 影响重试时的去重),
    # 而两边都不报错。
    idempotent: bool = False
    def dial_timeout_ms(self) -> int:
        """拨号超时(毫秒)。kafkax.ProducerConf 收毫秒,yaml 里是 Go duration 串。

        不做这层换算的话 `dial_timeout: "2s"` 会被整段忽略 —— broker 不通时
        producer 用客户端库默认超时(数十秒)干等,而 yaml 明明写了 2s。
        """
        return int(pconfig.parse_duration(self.dial_timeout).total_seconds() * 1000)


class SessionGateConf(BaseModel):
    """对应 Go 的 `pkg/config.SessionGateConf`。

    require=false(dev 宽松档):gate 装配上了就照常判定,只有**漏配会话权威端点**时跳过;
    require=true(-Prod 生成器机械置):漏配端点直接拒启。
    """

    model_config = {"extra": "allow"}

    require: bool = False


class FriendConf(BaseModel):
    """friend 私有配置段。字段名与 Go 的 yaml tag 逐字一致。"""

    # 单玩家好友数量上限(§9.18 写入侧总量上限)。
    # AddFriend 对 requester 提前失败;权威校验在 AcceptFriend 事务内对双方原子执行。
    max_friends: int = 0

    # 单玩家「收到的待处理好友申请」上限(§9.18)。CreateRequest 事务内校验 target 的
    # pending 收件箱数量,超限回 ERR_FRIEND_REQUEST_LIMIT,防止收件箱被刷爆。
    max_incoming_requests: int = 0

    # 好友申请的 per-player 每分钟频率配额(anti-abuse §6 第 6 项)。
    # ★ 负值 = 关闭(所以 Defaults 的判据是 `== 0` 而不是 `<= 0`,见模块头注释)。
    # 与总量闸正交:总量限「同时挂多少」,本值限「刷多快」,挡「加满 → 全撤 → 再加满」
    # 的写放大循环。窗口固定 1 分钟。
    rate_quota_per_min: int = 0

    # 单玩家黑名单上限(§9.18)。Block 事务内校验,超限回 ERR_FRIEND_BLOCK_LIMIT。
    max_blocks: int = 0

    # player_locator gRPC 地址(host:port)。
    # 空 → ListFriends / RecommendFriends 不查在线状态(is_online 全 false,弱依赖)。
    locator_addr: str = ""

    # 申请列表公开展示投影。昵称与编号分属两个 authority，分别使用 friend
    # 调用方的独立 request-bound HMAC 凭据；任一 addr 留空即关闭对应弱依赖。
    player_name_resolver_addr: str = ""
    player_name_resolver_auth_secret: str = ""
    player_name_resolver_auth_audience: str = ""
    player_no_resolver_addr: str = ""
    player_no_resolver_auth_secret: str = ""
    player_no_resolver_auth_audience: str = ""

    # 单次推荐好友数量(默认 10,硬上限 20,超界收敛到 20)。
    recommend_limit: int = 0

    # 推荐策略链(按序召回直到凑够 limit)。空 → ["mutual", "random"]。
    recommend_strategies: list[str] = Field(default_factory=list)

    # ── 保留期清理(§9.24 不变量 24:只增表必须有界)──

    # 终态好友请求(accepted/rejected/expired)保留天数。pending 永不清。
    request_retention_days: int = 0

    # 保留期清理轮询间隔(Go config.Duration,默认 5m)。多副本各自跑,DELETE 幂等无需锁。
    sweep_interval: str = ""

    # 每轮清理行数上限(默认 500)。
    sweep_batch: int = 0

    # 留空 / "report_only" = 只报告不删;"delete" = 真删。对应 Go 的 RetentionModeRaw。
    retention_mode: str = ""

    # 关系对守卫行(friend_pair_guards)保留天数(默认 30)。
    # 守卫行仅是锁载体无业务数据,任意时刻删除都安全(正被持有的行锁会阻塞 DELETE
    # 到事务提交;下次 acquire 重新 INSERT),保留期只为限制清理频率 / 表规模。
    pair_guard_retention_days: int = 0

    def sweep_interval_sec(self) -> float:
        return pconfig.parse_duration(self.sweep_interval).total_seconds()

    def strategies(self) -> list[str]:
        """生效的推荐策略链。对应 Go 的 buildStrategies 的名单兜底。

        名单为空 → [mutual, random];名单里的未知名由 biz 忽略,全被忽略时
        同样回落到默认两条(与 Go 逐条一致)。
        """
        return list(self.recommend_strategies) or list(DEFAULT_RECOMMEND_STRATEGIES)

    def retention_mode_parsed(self) -> dbguard.Mode:
        """生效的清理模式。**无法识别时回落 REPORT_ONLY**(与 Go 的 RetentionMode 同)。

        看似与 validate_retention_mode 重复,分工不同:这个是运行期取值
        (绝不能因为配错就去删数据),validate 那个是启动期 fail-fast。
        """
        try:
            return dbguard.parse_mode(self.retention_mode)
        except ValueError:
            return dbguard.Mode.REPORT_ONLY

    def validate_retention_mode(self) -> None:
        """启动期 fail-fast 用。对应 Go 的 ValidateRetentionMode。

        不设这道闸的后果:`retention_mode: delet`(少个 e)会静默回落 report_only,
        运维以为开了清理、实际一行没删,库继续无界增长且启动期毫无痕迹。
        """
        dbguard.parse_mode(self.retention_mode)


class Config(pconfig.BaseConf):
    """friend 服务的完整配置。对应 Go 的 conf.Config。"""

    friend: FriendConf = Field(default_factory=FriendConf)
    kafka: KafkaConf = Field(default_factory=KafkaConf)
    session_gate: SessionGateConf = Field(default_factory=SessionGateConf)

    def apply_defaults(self) -> None:
        """填默认值 —— 对应 Go 的 Defaults()。判据符号与 Go 逐字同。

        零值在这里都是危险的而不是"中性的":
          - max_friends=0        → 上限校验整段跳过,好友列表无界(§9.18 被架空);
          - max_incoming=0       → 收件箱可被刷爆;
          - recommend_limit=0    → 推荐恒空,面板一直转圈;
          - request_retention=0  → cutoff = 现在,一轮就把全部终态申请划进清理范围;
          - sweep_interval=0     → safego.loop 判非法直接返回,**清理循环根本不跑**。
        """
        f = self.friend
        if f.max_friends <= 0:
            f.max_friends = DEFAULT_MAX_FRIENDS
        if f.max_incoming_requests <= 0:
            f.max_incoming_requests = DEFAULT_MAX_INCOMING_REQUESTS
        if f.max_blocks <= 0:
            f.max_blocks = DEFAULT_MAX_BLOCKS
        # ★ 这一条是 `== 0`,不是 `<= 0`。见模块头注释:负值 = 显式关闭限流。
        if f.rate_quota_per_min == 0:
            f.rate_quota_per_min = DEFAULT_RATE_QUOTA_PER_MIN
        if f.recommend_limit <= 0:
            f.recommend_limit = DEFAULT_RECOMMEND_LIMIT
        if f.recommend_limit > RECOMMEND_MAX_LIMIT:
            f.recommend_limit = RECOMMEND_MAX_LIMIT
        if f.request_retention_days <= 0:
            f.request_retention_days = DEFAULT_REQUEST_RETENTION_DAYS
        if f.sweep_interval_sec() <= 0:
            # Go 是 `config.Duration <= 0` → 5m。Python 侧存的是原始串,
            # 所以回填成同义的 "5m" 而不是另存一个秒数字段(两个真源必漂移)。
            f.sweep_interval = "5m"
        if f.sweep_batch <= 0:
            f.sweep_batch = DEFAULT_SWEEP_BATCH
        if f.pair_guard_retention_days <= 0:
            f.pair_guard_retention_days = DEFAULT_PAIR_GUARD_RETENTION_DAYS
        if (
            f.player_name_resolver_addr
            and not f.player_name_resolver_auth_audience
        ):
            f.player_name_resolver_auth_audience = (
                DEFAULT_PLAYER_NAME_RESOLVER_AUTH_AUDIENCE
            )
        if (
            f.player_no_resolver_addr
            and not f.player_no_resolver_auth_audience
        ):
            f.player_no_resolver_auth_audience = DEFAULT_PLAYER_NO_RESOLVER_AUTH_AUDIENCE
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR

    def validate_player_display_resolvers(self) -> None:
        """展示 resolver 的地址/凭据必须闭包，两个 authority 不得复用密钥。"""
        f = self.friend
        self._validate_resolver(
            "friend.player_name_resolver",
            f.player_name_resolver_addr,
            f.player_name_resolver_auth_secret,
            f.player_name_resolver_auth_audience,
        )
        self._validate_resolver(
            "friend.player_no_resolver",
            f.player_no_resolver_addr,
            f.player_no_resolver_auth_secret,
            f.player_no_resolver_auth_audience,
        )
        if (
            f.player_name_resolver_auth_secret
            and f.player_no_resolver_auth_secret
            and f.player_name_resolver_auth_secret == f.player_no_resolver_auth_secret
        ):
            raise ValueError(
                "friend.player_name_resolver_auth_secret must differ from "
                "friend.player_no_resolver_auth_secret"
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
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
