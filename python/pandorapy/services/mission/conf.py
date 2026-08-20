"""mission 服务私有配置 —— 对应 Go 侧 services/social/mission/internal/conf/conf.go。

读的是**同一份** services/social/mission/etc/mission-dev.yaml,不另建配置文件:
迁移期 Go 版和 Python 版并存,运维只该维护一份配置。

★ 默认值必须与 Go 的 `Defaults()` **逐个相同,判据符号也一样**(Go 这里全是 `<= 0`)。
  分叉的后果不是报错,是同一份 yaml 在两个实现上跑出不同行为而**两边都不报错**:
  比如 max_facts_per_report 若在 Python 侧默认成 0,`len(facts) > 0` 恒真 ——
  一切事实上报都被判 ERR_INVALID_ARG,而 Go 副本一切正常,表现为"某些副本任务不涨"。

★ kafka / session_gate 两段在 Go 是 `pkg/config.Base` 的成员,Python 侧共享的
  `pandorapy.config.BaseConf` 还没有它们(见 shared_files_needed)。在服务私有
  Config 上补建模,是为了让**配了就会改变正确性**的段不落进 `model_extra` 被静默忽略:
    - kafka.brokers 留空 = 推送禁用(出箱堆积),落进 extra 的话 Python 副本会永远
      静默不发,而 yaml 明明配了 broker;
    - session_gate.require=true 是 prod 生成器机械置的**拒启开关**,忽略它等于把
      fail-closed 悄悄降级成 fail-open。

端口默认值(20019/21019)同样是契约:Envoy 的 cluster、run_services.ps1 的端口占用
检查、K8s Service 都钉在这两个数上。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig
from pandorapy import dbguard

DEFAULT_GRPC_ADDR = ":20019"
DEFAULT_HTTP_ADDR = ":21019"

# 与 Go 的 Defaults() 逐个同值。抽成常量而不是内联字面量,是为了让
# tests/test_mission_conf.py 能直接对着 Go 源码断言(漂移当场变红)。
DEFAULT_MAX_ACTIVE_MISSIONS = 50
DEFAULT_MAX_FACTS_PER_REPORT = 64
DEFAULT_REWARD_RETRY_INTERVAL = "1m"
DEFAULT_REWARD_RETRY_GRACE = "2m"
DEFAULT_REWARD_RETRY_BATCH = 200
DEFAULT_PUSH_PUBLISH_INTERVAL = "1s"
DEFAULT_PUSH_PUBLISH_BATCH = 128
DEFAULT_REWARD_LOG_RETENTION_DAYS = 90
DEFAULT_RECEIPT_RETENTION_DAYS = 90
DEFAULT_SWEEP_INTERVAL = "5m"
DEFAULT_SWEEP_BATCH = 500

# 推送发布器写者租约档位(与 Go 的 conf.PushWriterLeaseOff / ...Enforce 逐字相同)。
PUSH_WRITER_LEASE_OFF = "off"
PUSH_WRITER_LEASE_ENFORCE = "enforce"


class KafkaConf(BaseModel):
    """对应 Go 的 `pkg/config.KafkaConfig`(只建模 mission 用得到的字段)。

    mission 只做 producer(推送出箱 → pandora.mission.update),消费侧字段
    (group_id / read_timeout ...)配了也不改变本服务行为,交给 extra 吸收。
    """

    model_config = {"extra": "allow"}

    brokers: list[str] = Field(default_factory=list)
    partition_cnt: int = 0
    dial_timeout: str = ""
    write_timeout: str = ""
    retry_max: int = 0
    retry_backoff: str = ""
    compression_type: str = ""
    # ★ Go 结构体零值是 false,但 dev/cluster yaml 都显式写 idempotent: true。
    # Python 默认取 True 与 kafkax.ProducerConf 的默认一致 —— 幂等 producer 是保序
    # 前提,默认关掉会让 broker 重试换序而无人察觉。
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
        producer 用客户端库的默认超时(数十秒)干等,而 yaml 明明写了 2s。
        """
        return int(pconfig.parse_duration(self.dial_timeout).total_seconds() * 1000)


class SessionGateConf(BaseModel):
    """会话现行性门参数 —— 对应 Go 的 `pkg/config.SessionGateConf`。

    不建模的后果不是"读不到":BaseConf 是 extra="allow",session_gate 会整段落进
    model_extra —— yaml 里 require=true 写着,Python 侧当没看见,**prod 的强制档
    静默退化成 dev 宽松档**。
    """

    model_config = {"extra": "allow"}

    # true = 强制档(prod 生成器机械置):权威端点漏配拒启;gate 未装配时带会话证据的
    # 请求一律 fail-closed。false = dev 宽松档。
    require: bool = False
    # 仅 hub_allocator 使用;mission 不读它,建模只为不落进 extra。
    require_ticket_sjti: bool = False


class ConfigTableConf(BaseModel):
    """配置表加载(不变量 §9.15;对应 Go 的 conf.ConfigTableConf)。"""

    model_config = {"extra": "allow"}

    # active 目录。mission 的接取校验 / 进度判定 / 发奖内容全部读表,
    # 表是本服务的启动强依赖:必配,加载失败 fail-closed 拒启。
    dir: str = ""


class PushWriterLeaseConf(BaseModel):
    """推送出箱发布器的单写者选举 —— 对应 Go 的 conf.PushWriterLeaseConf。

    为什么发布器必须单写者(而补扫 / 清理不需要):
      mission_push_outbox 是**全局未分区**的一张表,发布器按 id 序整表 FIFO 取行,
      正是 §9.21 点名要串行化的「作用于同一未分区权威的单写者循环」。两个副本同时跑
      时各自持有一份内存快照,投递顺序会交错 —— 而 MissionUpdateEvent.progressed 是
      **逐任务全量快照**(不是增量),后到即覆盖:玩家 UI 进度条会从 7/10 退回 3/10,
      直到下次 ListMissions / push.resync 才恢复。事件里没有任何 revision 可判旧
      (ts_ms 是 event 级、跨副本各自墙钟,protocol-ordering-rules §5-B 明令不得只靠它)。

      对照:发奖补扫与保留期清理**刻意不选举** —— 前者正确性由下游三个幂等键保证,
      后者 DELETE 天然幂等。§9.21 明确「可并行 worker 不得为金丝雀强行全局串行化」。
    """

    model_config = {"extra": "allow"}

    # "off"(留空即 off)| "enforce"。
    #
    # 默认 off 而不是 enforce:mission 的 dev / 一键启动是单进程,没有 etcd 也必须
    # 起得来(§14.2 默认值必须保证现有行为不变)。**生产的安全网不是这个默认值,而是
    # main.py 里的机械门禁**:受管 k8s 内检测到 RollingUpdate 而 mode != enforce
    # 时 fail-closed 退出。
    mode: str = ""
    etcd_endpoints: list[str] = Field(default_factory=list)
    lease_ttl_sec: int = 0
    dial_timeout: str = ""

    def resolve_mode(self) -> str:
        """归一化档位,取值不认识**报错而非猜**。对应 Go 的 ResolveMode。

        不设这道闸会怎样:`mode: "enfore"`(少个 c)静默退回无保护并发发布,滚动
        重叠期两个发布器同时排空出箱,玩家的进度条来回跳,而 yaml 上写着 enforce。
        """
        text = (self.mode or "").strip().lower()
        if text in ("", PUSH_WRITER_LEASE_OFF):
            return PUSH_WRITER_LEASE_OFF
        if text == PUSH_WRITER_LEASE_ENFORCE:
            return PUSH_WRITER_LEASE_ENFORCE
        raise ValueError(
            f"mission.push_writer_lease.mode={self.mode!r} 不认识"
            f"(只允许 {PUSH_WRITER_LEASE_OFF!r} / {PUSH_WRITER_LEASE_ENFORCE!r},"
            f"留空={PUSH_WRITER_LEASE_OFF!r})"
        )

    def dial_timeout_sec(self) -> float:
        return pconfig.parse_duration(self.dial_timeout).total_seconds()


class MissionConf(BaseModel):
    """mission 服务私有配置段。字段名与 Go 的 yaml tag 逐字一致。"""

    model_config = {"extra": "allow"}

    # 单玩家活跃任务数上限(§9.18 写入侧上限)。AcceptMission 与完成扇出自动接链
    # 在同一事务内校验;自动接链超限跳过该条不阻断。
    max_active_missions: int = 0
    # 单次 ReportMissionFacts 的事实条数上限。上游 battle_result 按批聚合,正常远
    # 小于此;超限 ERR_INVALID_ARG(§9.18 读写边界)。
    max_facts_per_report: int = 0

    # ── 发奖下游 ────────────────────────────────────────────────────────────
    # inventory 服务 gRPC 地址(道具/装备发放,内网 insecure 直连)。
    inventory_addr: str = ""
    # player 服务 gRPC 地址(经验发放 AddExperience)。
    player_addr: str = ""
    # mail 服务 gRPC 地址(装备发放满包时溢出转邮件;空 = 满包发放失败留补扫)。
    mail_addr: str = ""
    # ★ 默认 **False**,与 Go 同。默认 True 的后果不是"更宽松",而是生产漏配
    # inventory_addr / player_addr 时服务静默以「发奖恒失败」启动 —— 任务完成了、
    # 奖励一件没到(流水滞留 PENDING),而启动日志全绿。
    allow_noop_reward: bool = False

    # 发奖补扫(对齐 leaderboard RetryUngrantedRewards)。
    reward_retry_interval: str = ""
    # 补扫只处理更新时间早于本时长的行(挡住刚创建还在同步发的批次)。
    reward_retry_grace: str = ""
    reward_retry_batch: int = 0

    # 推送出箱发布(FIFO,失败中断本轮保序)。
    push_publish_interval: str = ""
    push_publish_batch: int = 0

    push_writer_lease: PushWriterLeaseConf = Field(default_factory=PushWriterLeaseConf)

    # ── 保留期清理(§9.24)──────────────────────────────────────────────────
    # mission_reward_log 已发放(GRANTED)行保留天数。PENDING/FAILED 是补发工作集,
    # 永不清理(陈年 PENDING = 发放链 bug 的证据)。
    reward_log_retention_days: int = 0
    # mission_fact_receipts 保留天数。
    receipt_retention_days: int = 0
    # ★ 收据清理组级闸,**默认 False**(同 player exp_history)。
    # 上游 battle_progress_outbox 的重试没有总期限,删收据后迟到重放会把同一批事实
    # **双计**进任务进度;上游重试有界之前不得开启。开启后仍受 retention_mode 约束。
    receipt_cleanup_enabled: bool = False

    # 保留期清理轮询间隔。多副本各自跑,DELETE 幂等无需锁。
    sweep_interval: str = ""
    sweep_batch: int = 0
    # 留空 / "report_only" = 只报告不删(默认);"delete" = 真删。
    retention_mode: str = ""

    # ── duration 取值 ───────────────────────────────────────────────────────

    def reward_retry_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.reward_retry_interval)

    def reward_retry_grace_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.reward_retry_grace)

    def push_publish_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.push_publish_interval)

    def sweep_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.sweep_interval)

    # ── 保留期模式 ──────────────────────────────────────────────────────────

    def retention_mode_parsed(self) -> dbguard.Mode:
        """生效的清理模式。**无法识别时回落 REPORT_ONLY**(与 Go 的 RetentionMode 同)。

        看起来与 validate_retention_mode 重复,分工其实不同:这个是运行期取值
        (绝不能因为配错就去删数据),那个是启动期 fail-fast。
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
    """mission 服务的完整配置。对应 Go 的 conf.Config。"""

    config_table: ConfigTableConf = Field(default_factory=ConfigTableConf)
    mission: MissionConf = Field(default_factory=MissionConf)
    kafka: KafkaConf = Field(default_factory=KafkaConf)
    session_gate: SessionGateConf = Field(default_factory=SessionGateConf)

    def apply_defaults(self) -> None:
        """填默认值 —— 对应 Go 的 Defaults(),逐条同序同判据(全部 `<= 0`)。

        零值在这里全是危险的而不是"无限":
          push_publish_interval=0 会让发布循环退化成忙等(safego.loop 直接拒绝启动,
            表现为推送**永远不发**而出箱静默堆积);
          sweep_batch=0 会让每轮删 0 行 —— 循环在跑、日志正常、表永远不清;
          max_active_missions=0 会让一切 AcceptMission 撞 `len(active) >= 0` 直接
            ERR_MISSION_ACTIVE_LIMIT,玩家一个任务都接不了。
        """
        m = self.mission
        if m.max_active_missions <= 0:
            m.max_active_missions = DEFAULT_MAX_ACTIVE_MISSIONS
        if m.max_facts_per_report <= 0:
            m.max_facts_per_report = DEFAULT_MAX_FACTS_PER_REPORT
        if m.reward_retry_interval_td().total_seconds() <= 0:
            m.reward_retry_interval = DEFAULT_REWARD_RETRY_INTERVAL
        if m.reward_retry_grace_td().total_seconds() <= 0:
            m.reward_retry_grace = DEFAULT_REWARD_RETRY_GRACE
        if m.reward_retry_batch <= 0:
            m.reward_retry_batch = DEFAULT_REWARD_RETRY_BATCH
        if m.push_publish_interval_td().total_seconds() <= 0:
            m.push_publish_interval = DEFAULT_PUSH_PUBLISH_INTERVAL
        if m.push_publish_batch <= 0:
            m.push_publish_batch = DEFAULT_PUSH_PUBLISH_BATCH
        if m.reward_log_retention_days <= 0:
            m.reward_log_retention_days = DEFAULT_REWARD_LOG_RETENTION_DAYS
        if m.receipt_retention_days <= 0:
            m.receipt_retention_days = DEFAULT_RECEIPT_RETENTION_DAYS
        if m.sweep_interval_td().total_seconds() <= 0:
            m.sweep_interval = DEFAULT_SWEEP_INTERVAL
        if m.sweep_batch <= 0:
            m.sweep_batch = DEFAULT_SWEEP_BATCH
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR

    def validate_startup(self) -> None:
        """启动期硬校验(fail-fast)。对应 Go 的 Config.Validate()。

        缺强依赖起来了也只是慢性故障:配置表缺失 = 一切判定读不到表,接取恒
        ERR_MISSION_CONFIG_NOT_FOUND;发奖下游缺失 = 任务完成而奖励永远滞留 PENDING。
        两者都是"服务看着活着但业务是死的",比拒启难查得多。
        """
        if not self.config_table.dir:
            raise ValueError(
                "config_table.dir 必配:任务校验/进度判定/发奖内容全部读表(§9.15)"
            )
        if not self.mission.allow_noop_reward:
            if not self.mission.inventory_addr:
                raise ValueError(
                    "mission.inventory_addr 必配(发奖链交付承诺);"
                    "dev 骨架联调可置 allow_noop_reward=true"
                )
            if not self.mission.player_addr:
                raise ValueError(
                    "mission.player_addr 必配(经验发放);"
                    "dev 骨架联调可置 allow_noop_reward=true"
                )

    @classmethod
    def load(cls, path: str) -> "Config":
        """从 yaml 加载并填默认值。"""
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
