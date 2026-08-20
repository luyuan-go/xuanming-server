"""leaderboard 服务配置 —— 对应 Go 侧 internal/conf/conf.go。

读的是**同一份** services/runtime/leaderboard/etc/leaderboard-dev.yaml,不另建配置文件:
迁移期 Go 版和 Python 版并存,运维只该维护一份配置。

★ 默认值必须与 Go 的 `Defaults()` **逐个相同,判据符号也一样**。
  Go 这里全部用 `<= 0`(不是 `== 0`),意味着 yaml 里写 `max_list_limit: -1`
  在 Go 侧会被兜成 200;若 Python 写成 `== 0` 判据,同一份 yaml 就会让 Python 副本
  带着 -1 跑 —— GetRange 返回空列表,而**两边都不报错**,面板上只看到"某些副本查不到榜"。

★ kafka / session_gate 两段在 Go 是 `pkg/config.Base` 的成员,Python 侧共享的
  `pandorapy.config.BaseConf` 还没有它们(见结果里的 shared_files_needed)。
  这里在服务私有 Config 上补建模,是为了让**配了就会改变正确性**的段不落进
  `model_extra` 被静默忽略:
    - kafka.brokers 留空 = 结算事件不外发(Go 打 kafka_brokers_empty 的 WARN),
      落进 extra 的话 Python 副本会永远静默不发,而 yaml 明明配了 broker;
    - session_gate.require=true 是 prod 生成器机械置的**拒启开关**,
      忽略它等于把 fail-closed 悄悄降级成 fail-open。
  等共享 BaseConf 补齐后,把这两段从这里删掉即可(字段名/默认值已按 Go 对齐)。
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig
from pandorapy import dbguard

DEFAULT_GRPC_ADDR = ":20007"
DEFAULT_HTTP_ADDR = ":21007"

# 与 Go 侧 Defaults() 逐个同值。抽成常量而不是内联字面量,是为了让
# tests/test_leaderboard_conf.py 能直接对着 Go 源码断言(漂移当场变红)。
DEFAULT_LIST_LIMIT = 50
DEFAULT_MAX_LIST_LIMIT = 200
DEFAULT_AROUND_RADIUS = 10
DEFAULT_SETTLE_TOP_N = 100
DEFAULT_ESTIMATE_BUCKET_WIDTH = 25
DEFAULT_RETENTION_DAYS = 90
DEFAULT_RETENTION_SWEEP_BATCH = 500


class KafkaConf(BaseModel):
    """对应 Go 的 `pkg/config.KafkaConfig`(只建模 leaderboard 用得到的字段)。

    未建模的字段(group_id / read_timeout / retention_ms ...)由 pydantic 的
    extra 策略吸收 —— leaderboard 只做 producer,消费侧字段配了也不改变本服务行为。
    """

    model_config = {"extra": "allow"}

    brokers: list[str] = Field(default_factory=list)
    partition_cnt: int = 0
    dial_timeout: str = ""
    write_timeout: str = ""
    retry_max: int = 0
    retry_backoff: str = ""
    compression_type: str = ""
    # ★ Go 的注释写"默认 true",但结构体零值是 false,而 leaderboard-dev.yaml
    # 显式写了 idempotent: true。Python 侧默认取 True 与 kafkax.ProducerConf 的
    # 默认一致(幂等 producer 是保序前提,默认关掉会让重试换序而无人察觉)。
    # ★ Go 的真实默认是 **False**(结构体零值),不是注释里写的 "默认 true"。
    # pkg/config/config.go:239 的注释是 aspirational 的 —— 全仓没有任何代码给它填值,
    # producer.go:85 直穿 `c.Producer.Idempotent = cfg.Idempotent`。
    # 写成 True 的后果:35 份带 kafka 段的 yaml 里有 15 份没显式配它,
    # 那些服务上两栈的**重复消息语义**不同(幂等 producer 影响重试时的去重),
    # 而两边都不报错。
    idempotent: bool = False
    def dial_timeout_ms(self) -> int:
        """拨号超时(毫秒)。kafkax.ProducerConf 收的是毫秒,yaml 里是 Go duration 串。

        不做这层换算的话 `dial_timeout: "2s"` 会被整段忽略 —— broker 不通时
        producer 用客户端库的默认超时(数十秒)干等,而 yaml 明明写了 2s。
        """
        return int(pconfig.parse_duration(self.dial_timeout).total_seconds() * 1000)


class SessionGateConf(BaseModel):
    """对应 Go 的 `pkg/config.SessionGateConf`。

    require=false(dev 宽松档):gate 装配上了就照常判定,只有**漏配会话权威端点**时跳过;
    require=true(-Prod 生成器机械置):漏配端点直接拒启。
    """

    model_config = {"extra": "allow"}

    require: bool = False


class LeaderboardConf(BaseModel):
    """leaderboard 私有配置段。字段名与 Go 的 yaml tag 逐字一致。"""

    # GetRange 默认返回条数 / 单次返回上限;GetAround 默认上下名数。
    default_list_limit: int = 0
    max_list_limit: int = 0
    default_around_radius: int = 0

    # SettleBoard 未指定 top_n 时默认结算前 N 名。
    default_settle_top_n: int = 0

    # 建榜未指定 estimate_bucket_width 时的直方图桶宽(MMR 量纲;建榜后写进
    # meta 定死,后续上报忽略变更 —— 桶宽混用会毁掉整张直方图)。
    default_estimate_bucket_width: int = 0

    # inventory 内网 gRPC 地址。配了 → 结算走真实 GrantItems;
    # 留空且 allow_noop_reward=true 才退回占位发奖,否则启动期 fail-fast。
    inventory_addr: str = ""

    # ★ 默认 **False**,与 Go 同。默认 True 的后果不是"更宽松",而是
    # 生产漏配 inventory_addr 时服务静默以「结算不发奖」启动 —— 玩家榜单结算了、
    # 奖励一件没到,且启动日志全绿。
    allow_noop_reward: bool = False

    # 保留期清理(§9.24)。leaderboard_settlement 故意不清(uk 是防重复结算的永久闸)。
    retention_days: int = 0
    retention_sweep_batch: int = 0

    # 留空 / report_only = 只报告不删;delete = 真删。对应 Go 的 RetentionModeRaw。
    retention_mode: str = ""

    def retention_mode_parsed(self) -> dbguard.Mode:
        """生效的清理模式。**无法识别时回落 REPORT_ONLY**(与 Go 的 RetentionMode 同)。

        看起来和 validate_retention_mode 重复,其实分工不同:这个是运行期取值
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
    """leaderboard 服务的完整配置。对应 Go 的 conf.Config。"""

    leaderboard: LeaderboardConf = Field(default_factory=LeaderboardConf)
    kafka: KafkaConf = Field(default_factory=KafkaConf)
    session_gate: SessionGateConf = Field(default_factory=SessionGateConf)

    def apply_defaults(self) -> None:
        """填默认值 —— 对应 Go 的 Defaults()。判据一律 `<= 0`,与 Go 逐字同。

        零值在这里都是危险的而不是"中性的":
          - list_limit=0  → GetRange 恒返回空,客户端榜单永远是空白页;
          - top_n=0       → 结算 0 个人,快照空、奖一件不发,而 settle uk 已经占住,
                            重放也只会命中幂等 —— 这一期的奖就**永久没了**;
          - bucket_width=0→ Lua 里除零,Submit 整条路径报错;
          - retention_days=0 → cutoff = 现在,一轮就把全部快照划进清理范围。
        """
        lb = self.leaderboard
        if lb.default_list_limit <= 0:
            lb.default_list_limit = DEFAULT_LIST_LIMIT
        if lb.max_list_limit <= 0:
            lb.max_list_limit = DEFAULT_MAX_LIST_LIMIT
        if lb.default_around_radius <= 0:
            lb.default_around_radius = DEFAULT_AROUND_RADIUS
        if lb.default_settle_top_n <= 0:
            lb.default_settle_top_n = DEFAULT_SETTLE_TOP_N
        if lb.default_estimate_bucket_width <= 0:
            lb.default_estimate_bucket_width = DEFAULT_ESTIMATE_BUCKET_WIDTH
        if lb.retention_days <= 0:
            lb.retention_days = DEFAULT_RETENTION_DAYS
        if lb.retention_sweep_batch <= 0:
            lb.retention_sweep_batch = DEFAULT_RETENTION_SWEEP_BATCH
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR

    @classmethod
    def load(cls, path: str) -> "Config":
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
