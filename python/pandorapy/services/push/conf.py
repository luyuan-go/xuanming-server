"""push 服务配置 —— 对应 Go 侧 services/runtime/push/internal/conf/conf.go。

读的是**同一份** services/runtime/push/etc/push-dev.yaml,不另建配置文件。

★ 默认值必须与 Go 的 `Defaults()` 逐个相同,**判据符号也一样**。
  Go 对 `OfflineCacheTTL` 用 `== 0`,对 `OfflineCacheMaxFrames` 用 `<= 0`,
  两者不是笔误,这里逐字照搬:

    offline_cache_max_frames: -1  → Go `<= 0` 兜成 512,Python 也必须兜。
    offline_cache_ttl:        -1s → Go `== 0` **不兜**,保留 -1s(负保留窗 = 写侧
                                    修剪下界跑到未来,刚写入的帧当场出窗被修剪,
                                    每帧写完即丢且被 fl 记成丢失 → resync 风暴)。

  诚实标注一处**行为差异**:共享的 `pandorapy.config.parse_duration` 不接受
  负号(`"-1s"` 直接抛 ValueError),所以 Python 侧这个配置会在 `config_load_failed`
  就炸掉,而 Go 会带着 -1s 跑。方向是安全的(响 > 静默跑错),但不是逐字相同 ——
  若将来需要完全对齐,要改的是 config.parse_duration 而不是这里的判据符号。

★ topics 是**覆盖**而非追加(与 Go 同):yaml 写了 topics 就完全以它为准,
  少一条 = 该业务事件完全不消费。留空才回落 kafka_topics.PUSH_TOPICS。

★ kafka / session_gate 两段在 Go 是 `pkg/config.Base` 的成员,Python 侧共享的
  `pandorapy.config.BaseConf` 还没有它们(见交付说明 shared_files_needed),
  这里在服务私有 Config 上补建模 —— 落进 `model_extra` 会让 brokers 配了却不生效。
  注意 push 与 leaderboard 不同:push 是**消费方**,group_id / partition_cnt
  是它真正读的字段,漏建模会让 group_id 恒空 → 每次重启都从头/从尾重来。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig
from pandorapy import kafka_topics

# 与 Go 的 conf.Defaults() 逐个同值。抽成常量供测试直接对着 Go 源码断言。
DEFAULT_GRPC_ADDR = ":20014"
DEFAULT_HTTP_ADDR = ":21014"
DEFAULT_KAFKA_GROUP_ID = "pandora-push"
DEFAULT_OFFLINE_CACHE_TTL = _dt.timedelta(minutes=5)
DEFAULT_OFFLINE_CACHE_MAX_FRAMES = 512

# 投递缓冲整 key 的 TTL(游标基线保活)。Go 侧写在 data/offline.go 的常量里,
# 不是 yaml 字段 —— 与帧的保留窗(offline_cache_ttl)是两回事:
# 帧按窗修剪,而 key 本身要活得比客户端游标寿命更久,否则客户端断线一天回来时
# 整个 key 已经过期消失,fl 哨兵一起没了 = 丢失证据消失,resync 永远不会触发。
OFFLINE_KEY_TTL_SEC = 7 * 24 * 3600


class KafkaConf(BaseModel):
    """对应 Go 的 `pkg/config.KafkaConfig`。

    push 既是消费方(13 个业务 topic)又是生产方(13 个 DLQ topic),
    所以 producer / consumer 两侧字段都要建模。
    """

    model_config = {"extra": "allow"}

    brokers: list[str] = Field(default_factory=list)
    group_id: str = ""
    partition_cnt: int = 0
    dial_timeout: str = ""
    read_timeout: str = ""
    write_timeout: str = ""
    channel_buffer: int = 0
    retry_max: int = 0
    retry_backoff: str = ""
    compression_type: str = ""
    # Go 结构体零值是 false,但 push-dev.yaml 显式写了 idempotent: true;
    # 幂等 producer 是保序前提(见 kafkax.KeyOrderedProducer 注释),默认取 True。
    # ★ Go 的真实默认是 **False**(结构体零值),不是注释里写的 "默认 true"。
    # pkg/config/config.go:239 的注释是 aspirational 的 —— 全仓没有任何代码给它填值,
    # producer.go:85 直穿 `c.Producer.Idempotent = cfg.Idempotent`。
    # 写成 True 的后果:35 份带 kafka 段的 yaml 里有 15 份没显式配它,
    # 那些服务上两栈的**重复消息语义**不同(幂等 producer 影响重试时的去重),
    # 而两边都不报错。
    idempotent: bool = False
    def dial_timeout_ms(self) -> int:
        """拨号超时(毫秒)。kafkax.ProducerConf 收毫秒,yaml 里是 Go duration 串。

        不换算的话 `dial_timeout: "2s"` 整段被忽略 —— broker 不通时 producer 用
        客户端库默认超时(数十秒)干等,而 yaml 明明写了 2s。
        """
        return int(pconfig.parse_duration(self.dial_timeout).total_seconds() * 1000)


class SessionGateConf(BaseModel):
    """对应 Go 的 `pkg/config.SessionGateConf`。

    ⚠️ push **不读这一段**:它的会话门开关是 `push.require_session_gate`
    (Go: cfg.Push.RequireSessionGate)。这里建模只是为了让 yaml 里若出现
    session_gate 段不落进 extra 被静默吃掉;真正生效的是 push 段那个字段。
    """

    model_config = {"extra": "allow"}

    require: bool = False


class PushConf(BaseModel):
    """push 私有配置段。字段名与 Go 的 yaml tag 逐字一致。"""

    # 订阅的 topic 列表。**覆盖语义**:留空才回落 kafka_topics.PUSH_TOPICS。
    topics: list[str] = Field(default_factory=list)

    # 投递缓冲的帧保留窗(写侧修剪下界 + 读侧下界,两者必须同界)。
    offline_cache_ttl: str = ""

    # 单玩家投递缓冲条数硬上限(§9.18 有界纪律)。
    offline_cache_max_frames: int = 0

    # 会话现行性门强制档(P0,INC-20260722-004;prod 生成器机械置 true)。
    # false(dev 缺省):有 jti 仍校验,无 jti 放行;true:无 jti / 权威不可达一律拒。
    require_session_gate: bool = False

    # 托管 Redis 禁用 CONFIG 导致 maxmemory-policy 无法核验时是否放行启动。
    # 缺省 false = fail-closed 拒启:「查不了」不等于「配置正确」。
    allow_unverified_eviction_policy: bool = False

    def offline_cache_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.offline_cache_ttl)

    def offline_cache_ttl_sec(self) -> int:
        return int(self.offline_cache_ttl_td().total_seconds())


class Config(pconfig.BaseConf):
    """push 服务的完整配置。对应 Go 的 conf.Config。"""

    push: PushConf = Field(default_factory=PushConf)
    kafka: KafkaConf = Field(default_factory=KafkaConf)
    session_gate: SessionGateConf = Field(default_factory=SessionGateConf)

    def apply_defaults(self) -> None:
        """填默认值 —— 对应 Go 的 Defaults(),逐条同序同判据。

        零值在这里都是危险的,不是"中性的":
          - topics 空       → 一个 topic 都不订阅,全部业务推送静默不达;
          - ttl 为 0        → 保留窗 0,写侧修剪下界 = now,刚写入的帧当场出窗
                              (读侧读不到)→ 每帧写完即"丢失",resync 风暴;
          - max_frames 0/负 → 条数修剪把缓冲清空;
          - group_id 空     → kafka 消费组无名,断点续传失效。
        """
        p = self.push
        if not p.topics:
            p.topics = list(kafka_topics.PUSH_TOPICS)
        # ★ 判据是 `== 0` 而不是 `<= 0` —— 与 Go 逐字一致,理由见模块头注释。
        if p.offline_cache_ttl_td() == _dt.timedelta(0):
            p.offline_cache_ttl = f"{int(DEFAULT_OFFLINE_CACHE_TTL.total_seconds())}s"
        if p.offline_cache_max_frames <= 0:
            p.offline_cache_max_frames = DEFAULT_OFFLINE_CACHE_MAX_FRAMES
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR
        if not self.kafka.group_id:
            self.kafka.group_id = DEFAULT_KAFKA_GROUP_ID

    @classmethod
    def load(cls, path: str) -> "Config":
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
