"""battle_result 服务私有配置 —— 对应 Go 侧
services/battle/battle_result/internal/conf/conf.go。

读的是**同一份** services/battle/battle_result/etc/battle_result-dev.yaml,不另建配置文件:
迁移期 Go 版和 Python 版并存,运维只该维护一份配置。

★ 默认值必须与 Go 侧 `Config.Defaults()` **逐条同序同判据**。本服全部用 `<= 0`
  (没有 inventory 那种 `== 0` 的"负值=显式关闭"字段),但有两条**钳位**是本服独有的:

      history_retention_days > 180 → 钳回 180   (硬上限:战报最多留六个月)
      history_retention_days <  30 → 钳到 30    (硬下限:本服是**真删**,写错一个
                                                 数量级会不可逆删掉玩家还看得见的战报)

  钳位漏掉的后果不是报错,是同一份 yaml 在两栈上删掉不同范围的数据 —— 而删是不可逆的。

★ §9.24 例外登记:**本服 `retention_mode` 留空即 `delete`**,与其它所有域
  (dbguard 全局默认 report_only,2026-07-22 用户指令「不能因为数据大了就删我的数据」)
  **相反**。这是 2026-08-03 用户指令的按域覆盖:战报是产品上就有寿命的数据 ——
  「超过六个月的在 MySQL 里就该没有」,只报告不删交付不了这条口径。
  所以这里**不能**复用 inventory 那种 `dbguard.parse_mode("")→REPORT_ONLY` 的写法:
  照抄会让 Python 副本永远不删战报,而 Go 副本在删 —— 库涨不涨取决于请求落到哪个副本。

★ 几处"零值不是禁用"的坑(全都不会在测试里露头):

    battle.player_addr 留空
        = 不接真实 MMR,全场按 base_mmr 静态兜底算 Elo(启动只有一条
        mmr_reader_static INFO)。同时**经验入账器也不装配**(Go 复用同一地址)。

    battle.mission_addr 留空
        = **任务事实转发整体关闭**,且**已发生的战斗事实无法事后补齐**
        (不是"配好地址重启补发")。与 inventory_addr 那种"未配也不丢"语义不同。

    battle.consume_topics 显式写成 []
        Defaults 会兜成两个默认 topic;但真写成空列表时 yaml 里 `consume_topics: []`
        与"没写这一段"在 Python 侧不可区分(与 Go 的 `len()==0` 判据一致,故照搬)。

    ds_auth.mode 拼错 / retention_mode 拼错
        一律 fail-fast 拒启,**不会**静默回落 —— 静默回落等于把一道闸悄悄关掉,
        而 yaml 上还写着它是开的。

端口默认值(20022/21022)同样是契约:Envoy cluster、run_services.ps1 的端口占用检查、
K8s Service 都钉在这两个数上。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig
from pandorapy import dbguard
from pandorapy import kafka_topics

DEFAULT_GRPC_ADDR = ":20022"
DEFAULT_HTTP_ADDR = ":21022"

# ── 与 Go 侧 Defaults() 一一对应的默认值 ────────────────────────────────────
DEFAULT_ELO_K_FACTOR = 32
DEFAULT_BASE_MMR = 1500
DEFAULT_OUTBOX_PUBLISH_INTERVAL = "2s"
DEFAULT_OUTBOX_BATCH_SIZE = 128
DEFAULT_TERMINAL_RELEASE_INTERVAL = "2s"
DEFAULT_TERMINAL_RELEASE_BATCH_SIZE = 128
DEFAULT_TERMINAL_RELEASE_GRACE = "15s"
DEFAULT_DROP_PUBLISH_INTERVAL = "2s"
DEFAULT_DROP_BATCH_SIZE = 128
DEFAULT_RETENTION_SWEEP_INTERVAL = "1h"
DEFAULT_RETENTION_SWEEP_BATCH = 200

# 战报保留期边界(§9.24 登记例外:180 天 > 通用 90 天上限)。
# 与 Go 的 conf.HistoryRetentionMaxDays / MinDays 同值;budgets.py 的容量预算也引用它,
# 保留期一改预算必须同步,否则告警要么恒响要么形同虚设。
HISTORY_RETENTION_MAX_DAYS = 180
HISTORY_RETENTION_MIN_DAYS = 30

# 每玩家掉落条数硬上限:battle_drop_outbox.item_config_ids 为 VARCHAR(512),
# 46 个 10 位 uint32 + 45 个逗号 = 505 字符,是不超列宽的最大条数。
MAX_DROP_PER_PLAYER_HARD_CAP = 46

# terminal_release_grace 的合法区间(Go: ValidateRedisAuthorityIngress + prepareTerminalRelease)。
# 两处都用同一区间;配错一次会让**每一场**正常结算失败而监控面全绿。
TERMINAL_RELEASE_GRACE_MIN = _dt.timedelta(seconds=5)
TERMINAL_RELEASE_GRACE_MAX = _dt.timedelta(minutes=2)


class DSAuthFenceConf(BaseModel):
    """对应 Go 的 `pkg/config.DSAuthFenceConf`(authority_mode=redis 的机械激活栅栏)。

    main.py 的闸㉘ 会用这些字段调 `dsauthfence.acquire_runtime`,并挂失租守望
    (失租即 fail-stop),与 Go 侧同语义。必须显式建模而不是靠 model_extra 兜:
    落进 model_extra 会让 `ValidateRedisFence` 恒看不到 endpoints,于是一份缺 fence
    的非法配置在 Go 上拒启、在 Python 上放行 —— 两栈对同一份 yaml 结论不同,
    正是要避免的事。
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
    """对应 Go 的 `pkg/config.DSAuthConf`。battle_result 只做**校验侧**(verify-only)。

    ★ 本该建在 pandorapy/config.py 的 BaseConf 上(Go 侧它就在 config.Base 里),
      但那是本批次禁改的共享文件,所以在这里建模(与 inventory / mail / trade 同样处理)。
      inventory 那份是**残缺**的(没有 authority_mode / fence / active_heartbeat_max_age),
      本服必须建全 —— 少一个 authority_mode 就等于把 Model-B 的整条授权链当成不存在。
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

    def active_heartbeat_max_age_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.active_heartbeat_max_age)

    def authority_mode_redis(self) -> bool:
        """对应 Go 的 AuthorityModeRedis()。判据是**精确等于 "redis"**。"""
        return self.authority_mode == "redis"

    def apply_defaults(self) -> None:
        """对应 Go 的 DSAuthConf.Defaults()。

        ⚠️ mode / secret 留空即"不启用",**刻意不填默认** —— 给它们填默认
        等于替运维打开一道安全门(或关上),而 yaml 上看不出来。
        """
        if not self.authority_mode:
            self.authority_mode = "legacy"
        if not self.issuer:
            self.issuer = "pandora-ds-control"
        if not self.audience:
            self.audience = "pandora-ds"
        # Go 判的是 `== 0`(Duration 零值),这里等价于"解析出来是 0";
        # 本服不签发令牌,两个 TTL 只为让 yaml 段完整、不落进 extra。
        if pconfig.parse_duration(self.battle_token_ttl).total_seconds() == 0:
            self.battle_token_ttl = "4h"
        if pconfig.parse_duration(self.hub_token_ttl).total_seconds() == 0:
            self.hub_token_ttl = "24h"
        if self.active_heartbeat_max_age_td().total_seconds() == 0:
            self.active_heartbeat_max_age = "30s"

    def validate_redis_fence(self) -> None:
        """对应 Go 的 ValidateRedisFence()。

        不设这道闸会怎样:authority_mode=redis 而 fence 缺失时,失租 / 旧 epoch 的副本
        会继续结算 —— Model-B 的整个"唯一授权权威"前提失效,而两个副本都在正常写库。
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
            raise ValueError("ds_auth: fence.etcd_dial_timeout must be positive or zero(default)")
        if self.active_heartbeat_max_age_td().total_seconds() <= 0:
            raise ValueError(
                "ds_auth: authority_mode=redis requires positive active_heartbeat_max_age"
            )


class SessionGateConf(BaseModel):
    """对应 Go 的 `pkg/config.SessionGateConf`。

    ⚠️ battle_result **不读这一段**:它的入口是 DS 回调面(无玩家 JWT),会话现行性门
    对它没有意义(Go 的 server/grpc.go 也只挂了 pmw.AuthOptional(),没有 SessionCurrent)。
    建模只为让 yaml 里若出现 session_gate 段不落进 extra 被静默吃掉。
    """

    model_config = {"extra": "allow"}

    require: bool = False


class KafkaConf(BaseModel):
    """对应 Go 的 `pkg/config.KafkaConfig`。

    battle_result 既是消费方(battle.result / ds.lifecycle)又是生产方
    (player.update + 每个消费 topic 一个 DLQ),两侧字段都要建模。
    漏建 group_id 会让消费组无名 → 每次重启都从头/从尾重来(段位事件重复或丢失)。
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
    # Go 结构体零值是 false;与 push 同处理:幂等 producer 是保序前提,默认取 True。
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


class BattleConf(BaseModel):
    """battle_result 私有配置段。字段名与 Go 的 yaml tag 逐字一致。"""

    model_config = {"extra": "forbid"}

    # ── 结算 / MMR ────────────────────────────────────────────────────────
    elo_k_factor: int = 0
    base_mmr: int = 0
    consume_topics: list[str] = Field(default_factory=list)
    player_addr: str = ""
    matchmaker_addr: str = ""

    # ── player.update 事务出箱 ────────────────────────────────────────────
    outbox_publish_interval: str = ""
    outbox_batch_size: int = 0

    # ── Model-B 终态回收 ──────────────────────────────────────────────────
    ds_allocator_addr: str = ""
    terminal_release_interval: str = ""
    terminal_release_batch_size: int = 0
    terminal_release_grace: str = ""

    # ── 战斗掉落回写 ──────────────────────────────────────────────────────
    inventory_addr: str = ""
    # drop_whitelist 仅保留旧配置/单测兼容。生产由 configtable drop×item 热更视图裁决。
    drop_whitelist: list[int] = Field(default_factory=list)
    max_drop_per_player: int = 0
    drop_publish_interval: str = ""
    drop_batch_size: int = 0
    mail_addr: str = ""
    mission_addr: str = ""

    # ── 战斗中实时进度通道 ────────────────────────────────────────────────
    progress_enabled: bool = False
    max_progress_batch: int = 0
    max_progress_seq_per_match: int = 0
    max_kill_count_per_fact: int = 0
    max_pickup_count_per_fact: int = 0
    max_progress_exp_per_match: int = 0
    max_progress_items_per_match: int = 0
    max_progress_exp_per_player: int = 0
    max_progress_items_per_player: int = 0
    max_progress_kills_per_player: int = 0
    progress_publish_interval: str = ""
    progress_batch_size: int = 0

    # ── 保留期清理(§9.24)────────────────────────────────────────────────
    history_retention_days: int = 0
    retention_sweep_interval: str = ""
    retention_sweep_batch: int = 0
    # 留空 = **delete**(本服特例);"report_only"/"report" = 只报告不删。
    retention_mode: str = ""

    # ── duration 访问器 ──────────────────────────────────────────────────

    def outbox_publish_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.outbox_publish_interval)

    def terminal_release_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.terminal_release_interval)

    def terminal_release_grace_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.terminal_release_grace)

    def drop_publish_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.drop_publish_interval)

    def retention_sweep_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.retention_sweep_interval)

    def progress_publish_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.progress_publish_interval)

    # ── *_or_default 访问器(对应 Go 的同名方法)───────────────────────────
    #
    # 放访问器而不是 Defaults,保证**任何**构造路径(含测试直建 BattleConf)都有
    # 安全上限 —— Go 特意这么写,照搬。

    def max_drops_per_player(self) -> int:
        """每玩家最大掉落条数(未配置/非法 → 32;超硬上限 → 46)。"""
        n = self.max_drop_per_player
        if n <= 0:
            n = 32
        if n > MAX_DROP_PER_PLAYER_HARD_CAP:
            n = MAX_DROP_PER_PLAYER_HARD_CAP
        return n

    def max_progress_batch_or_default(self) -> int:
        return self.max_progress_batch if self.max_progress_batch > 0 else 256

    def max_progress_seq_per_match_or_default(self) -> int:
        return (
            self.max_progress_seq_per_match if self.max_progress_seq_per_match > 0 else 100_000
        )

    def max_kill_count_per_fact_or_default(self) -> int:
        return self.max_kill_count_per_fact if self.max_kill_count_per_fact > 0 else 100

    def max_pickup_count_per_fact_or_default(self) -> int:
        """★ 判据是 `== 0` 而不是 `> 0`,且**再夹紧到 46**(与 Go 逐字一致)。

        出箱按事实成行,单事实必须能装进一行 CSV 列宽;放开这个夹紧会让一条合法拾取
        事实写超 VARCHAR(512),整批进度回滚。
        """
        n = self.max_pickup_count_per_fact
        if n == 0:
            n = 10
        if n > MAX_DROP_PER_PLAYER_HARD_CAP:
            n = MAX_DROP_PER_PLAYER_HARD_CAP
        return n

    def max_progress_exp_per_match_or_default(self) -> int:
        return (
            self.max_progress_exp_per_match if self.max_progress_exp_per_match > 0 else 1_000_000
        )

    def max_progress_items_per_match_or_default(self) -> int:
        return self.max_progress_items_per_match if self.max_progress_items_per_match > 0 else 500

    def max_progress_exp_per_player_or_default(self) -> int:
        return (
            self.max_progress_exp_per_player if self.max_progress_exp_per_player > 0 else 200_000
        )

    def max_progress_items_per_player_or_default(self) -> int:
        return (
            self.max_progress_items_per_player if self.max_progress_items_per_player > 0 else 100
        )

    def max_progress_kills_per_player_or_default(self) -> int:
        return (
            self.max_progress_kills_per_player if self.max_progress_kills_per_player > 0 else 1000
        )

    def progress_publish_interval_or_default(self) -> _dt.timedelta:
        d = self.progress_publish_interval_td()
        return d if d.total_seconds() > 0 else _dt.timedelta(seconds=1)

    def progress_batch_size_or_default(self) -> int:
        return self.progress_batch_size if self.progress_batch_size > 0 else 128

    def outbox_batch_size_or_default(self) -> int:
        return self.outbox_batch_size if self.outbox_batch_size > 0 else 128

    def drop_batch_size_or_default(self) -> int:
        return self.drop_batch_size if self.drop_batch_size > 0 else 128

    def terminal_release_batch_size_or_default(self) -> int:
        return (
            self.terminal_release_batch_size if self.terminal_release_batch_size > 0 else 128
        )

    # ── 掉落白名单兜底 ────────────────────────────────────────────────────

    def is_droppable(self, item_config_id: int) -> bool:
        """旧单测/兼容部署的 fallback;生产注入 BattleItemCatalog 后不会调用。"""
        return item_config_id in self.drop_whitelist

    # ── 保留期模式 ────────────────────────────────────────────────────────

    def retention_mode_parsed(self) -> dbguard.Mode:
        """生效的保留期清理模式。

        ★★ **留空 = DELETE**(本服特例,与 dbguard 全局默认相反)。
        照抄 inventory 的 `dbguard.parse_mode(raw)` 会把留空解成 REPORT_ONLY ——
        那样 Python 副本永远不删战报,而 Go 副本在删:同一个库的增长曲线取决于
        请求落到哪个副本,而两边都不报错。这是本文件最容易被"顺手统一"掉的一条。

        无法识别的值回落 REPORT_ONLY(不删更安全),但启动期 validate_retention_mode
        已经 fail-fast 拒启,正常路径到不了这里。
        """
        if not self.retention_mode.strip():
            return dbguard.Mode.DELETE
        try:
            return dbguard.parse_mode(self.retention_mode)
        except ValueError:
            return dbguard.Mode.REPORT_ONLY

    def validate_retention_mode(self) -> None:
        """供启动 fail-fast(对应 Go 的 ValidateRetentionMode)。

        不设这道闸会怎样:拼错 "delete"(如 "delet")会让六个月口径静默失效 ——
        库继续无界增长,而 yaml 上写着 delete、启动日志全绿。
        """
        dbguard.parse_mode(self.retention_mode)


class Config(pconfig.BaseConf):
    """battle_result 服务的完整配置。对应 Go 的 conf.Config。"""

    battle: BattleConf = Field(default_factory=BattleConf)
    ds_auth: DSAuthConf = Field(default_factory=DSAuthConf)
    kafka: KafkaConf = Field(default_factory=KafkaConf)
    session_gate: SessionGateConf = Field(default_factory=SessionGateConf)

    def apply_defaults(self) -> None:
        """填默认值 —— 对应 Go 的 Defaults(),**逐条同序同判据**。"""
        b = self.battle
        if b.elo_k_factor <= 0:
            b.elo_k_factor = DEFAULT_ELO_K_FACTOR
        if b.base_mmr <= 0:
            b.base_mmr = DEFAULT_BASE_MMR
        if not b.consume_topics:
            b.consume_topics = [
                kafka_topics.TOPIC_BATTLE_RESULT,
                kafka_topics.TOPIC_DS_LIFECYCLE,
            ]
        if b.outbox_publish_interval_td().total_seconds() <= 0:
            b.outbox_publish_interval = DEFAULT_OUTBOX_PUBLISH_INTERVAL
        if b.outbox_batch_size <= 0:
            b.outbox_batch_size = DEFAULT_OUTBOX_BATCH_SIZE
        if b.terminal_release_interval_td().total_seconds() <= 0:
            b.terminal_release_interval = DEFAULT_TERMINAL_RELEASE_INTERVAL
        if b.terminal_release_batch_size <= 0:
            b.terminal_release_batch_size = DEFAULT_TERMINAL_RELEASE_BATCH_SIZE
        if b.terminal_release_grace_td().total_seconds() <= 0:
            b.terminal_release_grace = DEFAULT_TERMINAL_RELEASE_GRACE
        if b.drop_publish_interval_td().total_seconds() <= 0:
            b.drop_publish_interval = DEFAULT_DROP_PUBLISH_INTERVAL
        if b.drop_batch_size <= 0:
            b.drop_batch_size = DEFAULT_DROP_BATCH_SIZE
        if b.history_retention_days <= 0:
            b.history_retention_days = HISTORY_RETENTION_MAX_DAYS
        if b.history_retention_days > HISTORY_RETENTION_MAX_DAYS:
            # 硬上限:战报最多留六个月(配置 365 不得原样生效;要更长必须同时改这里 +
            # §9.24 登记表 + budgets.py 容量预算,不允许靠配置静默突破)。
            b.history_retention_days = HISTORY_RETENTION_MAX_DAYS
        if b.history_retention_days < HISTORY_RETENTION_MIN_DAYS:
            # 硬下限:本服清理是**真删**,配置写错一个数量级(把 180 写成 18)
            # 会不可逆地删掉玩家还看得见的战报。下限把手滑的爆炸半径钳住。
            b.history_retention_days = HISTORY_RETENTION_MIN_DAYS
        if b.retention_sweep_interval_td().total_seconds() <= 0:
            b.retention_sweep_interval = DEFAULT_RETENTION_SWEEP_INTERVAL
        if b.retention_sweep_batch <= 0:
            b.retention_sweep_batch = DEFAULT_RETENTION_SWEEP_BATCH
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR
        self.ds_auth.apply_defaults()

    def validate_redis_authority_ingress(self) -> None:
        """对应 Go 的 ValidateRedisAuthorityIngress()。

        不设这道闸会怎样:authority_mode=redis 时若还订阅 pandora.battle.result,
        那条**没有可核验凭据**的 kafka 消息会直接调 usecase 落 MySQL ——
        形成一个绕过 Guard / Redis active / receipt 的第二结算入口,
        而它写出来的战绩与授权路径的完全一样,事后分不出来。
        """
        if not self.ds_auth.authority_mode_redis():
            return
        b = self.battle
        if not b.ds_allocator_addr:
            raise ValueError(
                "battle_result: authority_mode=redis requires battle.ds_allocator_addr "
                "for terminal release outbox relay"
            )
        if (
            b.terminal_release_interval_td().total_seconds() <= 0
            or b.terminal_release_batch_size <= 0
        ):
            raise ValueError(
                "battle_result: terminal release worker interval/batch must be positive"
            )
        grace = b.terminal_release_grace_td()
        if grace < TERMINAL_RELEASE_GRACE_MIN or grace > TERMINAL_RELEASE_GRACE_MAX:
            raise ValueError("battle_result: terminal_release_grace must be within [5s,2m]")
        for topic in b.consume_topics:
            if topic == kafka_topics.TOPIC_BATTLE_RESULT:
                raise ValueError(
                    f"battle_result: authority_mode=redis forbids unauthenticated topic "
                    f'"{kafka_topics.TOPIC_BATTLE_RESULT}"; consume only '
                    f'"{kafka_topics.TOPIC_DS_LIFECYCLE}" and accept results via '
                    f"guarded ReportResult RPC"
                )

    @classmethod
    def load(cls, path: str) -> "Config":
        """从 yaml 加载并填默认值。"""
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
