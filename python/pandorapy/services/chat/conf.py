"""chat 服务私有配置 —— 对应 Go 侧 internal/conf/conf.go。

读的是**同一份** services/social/chat/etc/chat-dev.yaml,不另建配置文件:
迁移期 Go 版和 Python 版并存,运维只该维护一份配置。

★ 默认值必须与 Go 的 `Defaults()` **逐个相同,判据符号也一样**。
  chat 这里有一处 Go 自己就不统一:WorldCooldown 判 `<= 0`,
  NonWorldCooldown 判 `== 0`(负值 = 显式关闭,原样保留)。
  两处都写成 `<= 0` 的后果:`non_world_cooldown: -1s`(显式关掉私聊冷却)
  在 Go 侧保持关闭、在 Python 侧被兜成 500ms —— 同一份 yaml 两个副本限流不同,
  而两边日志都显示"限流生效中"。

★ kafka / session_gate 两段在 Go 是 `pkg/config.Base` 的成员,Python 侧共享的
  `pandorapy.config.BaseConf` 还没有它们(见结果里的 shared_files_needed)。
  这里在服务私有 Config 上补建模,是为了让**配了就会改变正确性**的段不落进
  `model_extra` 被静默忽略:
    - kafka.brokers 留空 = 五个聊天频道全部不外发(Go 打 kafka_brokers_empty 的
      WARN),落进 extra 的话 Python 副本会永远静默不推送,而 yaml 明明配了 broker;
    - session_gate.require=true 是 prod 生成器机械置的**拒启开关**,
      忽略它等于把 fail-closed 悄悄降级成 fail-open。
"""

from __future__ import annotations

import datetime as _dt
import pathlib
from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig, dbguard

DEFAULT_GRPC_ADDR = ":20005"
DEFAULT_HTTP_ADDR = ":21005"


class KafkaConf(BaseModel):
    """对应 Go 的 `pkg/config.KafkaConfig`(只建模 chat 用得到的 producer 字段)。

    chat 只做 producer(五个 chat.* topic),消费侧字段(group_id / read_timeout ...)
    配了也不改变本服务行为,由 extra 吸收。
    """

    model_config = {"extra": "allow"}

    brokers: list[str] = Field(default_factory=list)
    partition_cnt: int = 0
    dial_timeout: str = ""
    retry_max: int = 0
    retry_backoff: str = ""
    compression_type: str = ""
    # ★ Go 的结构体零值是 false,但 kafkax.ProducerConf 的默认是 True。
    # 幂等 producer 是保序前提,默认关掉会让重试换序而无人察觉 —— 与 leaderboard 同口径。
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

    def retry_backoff_ms(self) -> int:
        return int(pconfig.parse_duration(self.retry_backoff).total_seconds() * 1000)


class SessionGateConf(BaseModel):
    """对应 Go 的 `pkg/config.SessionGateConf`。

    require=false(dev 宽松档):gate 装配上了就照常判定,只有**漏配会话权威端点**时跳过;
    require=true(-Prod 生成器机械置):漏配端点 / Ping 失败直接拒启。
    """

    model_config = {"extra": "allow"}

    require: bool = False


class ChatConf(pconfig.BaseModel):
    """chat 私有段。"""

    # max_content_len 单条消息最大长度,按 **Unicode 码点**计(默认 256)。
    max_content_len: int = 0
    # history_limit PullHistory 单次返回上限(默认 50)。
    history_limit: int = 0
    # team_addr / guild_addr:成员解析的 gRPC 直连地址。
    # **没有独立的 group_addr**:GuildService 与 GroupService 同进程,Go 侧两个 reader
    # 都拨 guild_addr,两份真实 yaml(chat-dev / chat-dev-tidb)也只写了 guild_addr。
    # 单列一个 group_addr 会让它恒为空 → GROUP 频道恒走弱依赖降级,消息静默不扇出且不报错。
    team_addr: str = ""
    guild_addr: str = ""
    # sensitive_words 敏感词列表。空 = 不过滤。
    sensitive_words: list[str] = Field(default_factory=list)
    # world_cooldown 世界频道 per-player 冷却(默认 3s)。广播成本 ≈ 速率 × 全服在线数。
    world_cooldown: str = ""
    # non_world_cooldown 非世界频道 per-player per-频道 冷却(默认 500ms)。
    non_world_cooldown: str = ""
    # history_retention_days 私聊历史保留期(默认 90 天,§9.24)。
    history_retention_days: int = 0
    sweep_interval: str = ""
    sweep_batch: int = 0
    # retention_mode 留空 = report_only(**只统计不删**)。
    retention_mode: str = ""

    def world_cooldown_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.world_cooldown)

    def non_world_cooldown_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.non_world_cooldown)

    def sweep_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.sweep_interval)

    def retention_mode_parsed(self) -> dbguard.Mode:
        """**运行期**取值。无法识别时回落 REPORT_ONLY —— 与 Go 的 `RetentionMode()` 同。

        看起来和 validate_retention_mode 重复,其实分工不同:这个绝不能因为配错就去
        删数据(任何不确定都不删),validate 那个是启动期 fail-fast。
        """
        try:
            return dbguard.parse_mode(self.retention_mode)
        except ValueError:
            return dbguard.Mode.REPORT_ONLY

    def validate_retention_mode(self) -> None:
        """启动期 fail-fast 用。对应 Go 的 `ValidateRetentionMode`。

        不设这道闸的后果:`retention_mode: delet`(少个 e)会静默回落 report_only,
        运维以为开了清理、实际一行没删,chat_private_messages 继续无界增长,
        而启动期毫无痕迹。
        """
        dbguard.parse_mode(self.retention_mode)


class Config(pconfig.BaseConf):
    chat: ChatConf = Field(default_factory=ChatConf)
    kafka: KafkaConf = Field(default_factory=KafkaConf)
    session_gate: SessionGateConf = Field(default_factory=SessionGateConf)

    def apply_defaults(self) -> None:
        c = self.chat
        if c.max_content_len <= 0:
            c.max_content_len = 256
        if c.history_limit <= 0:
            c.history_limit = 50
        # 这几项全仓 yaml 一个都没配,默认值 100% 生效 —— 所以它们不是"兜底",
        # 就是生产实际值,必须与 Go 的 Defaults() 逐个对齐,差一点就是两栈行为不同。
        if c.world_cooldown_td().total_seconds() <= 0:
            c.world_cooldown = "3s"
        # ★ 判据是 `== 0` 不是 `<= 0`,与 Go 逐字相同:负值 = 显式关闭,必须原样保留。
        if c.non_world_cooldown_td().total_seconds() == 0:
            c.non_world_cooldown = "500ms"
        if c.history_retention_days <= 0:
            c.history_retention_days = 90
        if c.sweep_interval_td().total_seconds() <= 0:
            c.sweep_interval = "5m"
        if c.sweep_batch <= 0:
            c.sweep_batch = 500
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR

    @classmethod
    def load(cls, path: str | pathlib.Path) -> "Config":
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
