"""matchmaker 服务配置 —— 对应 Go 侧 internal/conf/conf.go。

读的是**同一份** services/matchmaking/matchmaker/etc/matchmaker-{dev,pve}.yaml,
不另建配置文件:迁移期 Go 版和 Python 版并存,运维只该维护一份配置。

★ 默认值必须与 Go 的 `Defaults()` **逐个相同,判据符号也一样**。
  matchmaker 这里 Go 全部用 `== 0`(不是 `<= 0`),这不是笔误而是契约:
  多个字段用**负值表示「显式关闭整道闸」**(start_presence_grace /
  queue_absence_reap_after / start_match_cooldown / match_form_cooldown /
  no_capacity_requeue_delay)。若 Python 写成 `<= 0` 判据,同一份写着
  `start_presence_grace: "-1s"` 的 yaml 会被 Python 兜回 30s ——
  运维以为关掉了在线闸,实际它还开着,而**两边都不报错**。
  唯一的例外是 team_size:Go 在 `== 0` 兜底之后**另有**一段 [1, 50] 钳制
  (见 apply_defaults 里的注释),那一段照抄。

★ kafka / session_gate 两段在 Go 是 `pkg/config.Base` 的成员,Python 侧共享的
  `pandorapy.config.BaseConf` 还没有它们(与 leaderboard 同处境,见
  shared_files_needed)。这里在服务私有 Config 上补建模,是为了让**配了就会改变
  正确性**的段不落进 `model_extra` 被静默忽略:
    - kafka.brokers 非空 = match 进度推送是**启动强依赖**(见 main.py ⑦);
      落进 extra 会让 Python 副本永远静默不推 —— 组队里非队长成员得知 READY 的
      唯一通道就此消失,他们会一直停在大厅(Go 侧为此专门 fail-fast)。
    - session_gate.require=true 是 prod 生成器机械置的**拒启开关**,
      忽略它等于把 fail-closed 悄悄降级成 fail-open。

★ Validate() 逐条照抄 Go:它拒的全是「静默塌缩信任域」或「静默关掉一道闸」的配置,
  每一条都在函数里注明了不设这道闸会怎样。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig
from pandorapy import internalrpcauth

# 与 Go 侧 Defaults() 逐个同值。抽成常量而不是内联字面量,是为了让
# tests/test_matchmaker_conf.py 能直接对着 Go 源码断言(漂移当场变红)。
DEFAULT_GRPC_ADDR = ":20011"
DEFAULT_HTTP_ADDR = ":21011"

DEFAULT_DS_ALLOCATE_TIMEOUT = _dt.timedelta(seconds=60)
DEFAULT_CONFIRM_TIMEOUT = _dt.timedelta(seconds=15)
DEFAULT_MATCH_INTERVAL = _dt.timedelta(seconds=2)
DEFAULT_MAX_QUEUE_TICKETS = 5000
DEFAULT_ALLOCATION_WORKERS = 16
DEFAULT_START_MATCH_COOLDOWN = _dt.timedelta(seconds=3)
DEFAULT_START_PRESENCE_GRACE = _dt.timedelta(seconds=30)
DEFAULT_QUEUE_ABSENCE_REAP_AFTER = _dt.timedelta(seconds=120)
DEFAULT_MATCH_FORM_COOLDOWN = _dt.timedelta(seconds=5)
DEFAULT_NO_CAPACITY_REQUEUE_DELAY = _dt.timedelta(seconds=10)
DEFAULT_TICKET_TTL = _dt.timedelta(minutes=30)
DEFAULT_MATCH_TTL = _dt.timedelta(minutes=30)
DEFAULT_TEAM_SIZE = 5
DEFAULT_MMR_BASE_WINDOW = 200
DEFAULT_MMR_WIDEN_PER_SEC = 20
DEFAULT_MMR_MAX_WINDOW = 2000
DEFAULT_OPTIMISTIC_RETRY = 3
DEFAULT_MAP_ID = 1
DEFAULT_GAME_MODE = "5v5_ranked"

# 与 Go 的 configtable.MaxLevelTeamSize 同值。撮合按 need=side_count×team_size
# 预分配票据列表,超大值会让一次热更把撮合进程打爆(§16.5)。
MAX_LEVEL_TEAM_SIZE = 50

# Windows 本机联调档的唯一合法值(Go: auth.DSLocalProfileOffV1)。
DS_LOCAL_PROFILE_OFF_V1 = "local-off-v1"


class KafkaConf(BaseModel):
    """对应 Go 的 `pkg/config.KafkaConfig`(只建模 matchmaker 用得到的字段)。"""

    model_config = {"extra": "allow"}

    brokers: list[str] = Field(default_factory=list)
    partition_cnt: int = 0
    dial_timeout: str = ""
    write_timeout: str = ""
    retry_max: int = 0
    retry_backoff: str = ""
    compression_type: str = ""
    # Go 结构体零值是 false,而 matchmaker-dev.yaml 显式写了 idempotent: true。
    # Python 默认取 True 与 kafkax.ProducerConf 一致(幂等 producer 是保序前提,
    # 默认关掉会让重试换序而无人察觉)。
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
        producer 用客户端库的默认超时干等,而 yaml 明明写了 2s。
        """
        return int(pconfig.parse_duration(self.dial_timeout).total_seconds() * 1000)

    def configured(self) -> bool:
        """对应 Go 的 initializeMatchPublication 里那段 TrimSpace 判定。

        判据是「有没有**非空白**的 broker」,不是 `len(brokers)>0`:
        ConfigMap 渲染出 `brokers: [""]` 时长度是 1 而实际一个 broker 都没有,
        按长度判会让 producer 用空 broker 列表构造 —— 那要么当场炸、要么
        更糟:构造成功但永远发不出去。
        """
        return any((b or "").strip() for b in self.brokers)


class SessionGateConf(BaseModel):
    """对应 Go 的 `pkg/config.SessionGateConf`。

    require=false(dev 宽松档):gate 装配上了就照常判定,只有**漏配会话权威端点**
    时跳过;require=true(-Prod 生成器机械置):漏配端点直接拒启。
    """

    model_config = {"extra": "allow"}

    require: bool = False


class DSTicketConf(BaseModel):
    """对应 Go 的 `pkg/config.DSTicketConf`(玩家 DSTicket v2,RS256 方案 B)。"""

    model_config = {"extra": "allow"}

    private_key_file: str = ""
    active_kid: str = ""
    ttl: str = ""
    jwks_file: str = ""
    keyset_revision: str = ""

    def signer_enabled(self) -> bool:
        """本服务是否启用 v2 签发。对应 Go 的 SignerEnabled()。"""
        return self.private_key_file != ""


class JWTConf(BaseModel):
    """对应 Go 的 conf.JWTConf —— 签发 battle DSTicket 的 JWT 参数(镜像 login)。

    issuer / audience / secret 必须与 login 服务和 Envoy jwt_authn provider 完全一致。
    """

    model_config = {"extra": "allow"}

    issuer: str = ""
    audience: str = ""
    secret: str = ""
    additional_secrets: list[str] = Field(default_factory=list)
    session_ttl: str = ""
    ds_ticket_ttl: str = ""


class LeaderConf(BaseModel):
    """对应 Go 的 conf.LeaderConf —— 后台撮合循环的单写者选举。

    撮合循环在共享队列上做全局优化,天然是单写者问题(§9.21 未分区权威只跑一份)。
    多副本部署时若每个副本都无条件跑,会重复成局:同一玩家进两场 match,
    直接违反不变量 §1,且**没有任何下游能发现**——两场都各自 READY、各自拉一台 DS。
      - enabled=false(默认):本副本直接跑(单副本 / dev 行为不变)
      - enabled=true:经 etcd 选举,仅当选副本跑;失主取消 loop 但进程不退出,
        继续服务 RPC,新 leader 在 lease TTL 内接管(不停机滚动更新,不变量 §16)
    """

    model_config = {"extra": "allow"}

    enabled: bool = False
    etcd_endpoints: list[str] = Field(default_factory=list)
    prefix: str = ""
    lease_ttl_sec: int = 0


class MatchConf(BaseModel):
    """matchmaker 私有配置段。字段名与 Go 的 yaml tag 逐字一致。"""

    model_config = {"extra": "allow"}

    # Windows 本机联调档;留空 = 不启用(生产/灰度必须留空)。与 ds_ticket.private_key_file 互斥。
    ds_local_profile: str = ""

    team_addr: str = ""
    # 「允许 team_addr 留空」的显式开关。零值即安全:留空的后果是**静默**的
    # (StartMatch 不再校验队伍 + 对局结束不复位准备状态,INC-20260813-001 第一根因)。
    allow_missing_team: bool = False

    ds_allocator_addr: str = ""
    ds_allocate_timeout: str = ""
    locator_addr: str = ""

    match_resume_auth_secret: str = ""
    match_resume_auth_audience: str = ""
    team_resume_auth_secret: str = ""
    team_call_auth_secret: str = ""
    team_call_auth_audience: str = ""
    allocation_abort_auth_secret: str = ""
    allocation_abort_auth_audience: str = ""

    battle_gate_fail_open: bool = False
    liveness_gate_enabled: bool = False

    start_presence_grace: str = ""
    queue_absence_reap_after: str = ""

    map_id: int = 0
    game_mode: str = ""
    confirm_timeout: str = ""
    match_interval: str = ""
    allocation_workers: int = 0
    max_queue_tickets: int = 0
    start_match_cooldown: str = ""
    match_form_cooldown: str = ""
    no_capacity_requeue_delay: str = ""
    ticket_ttl: str = ""
    match_ttl: str = ""
    team_size: int = 0
    walk_in: bool = False
    # WalkIn 的**废弃旧键名**,保留仅为滚动升级期兼容(§9.21 migrate 阶段)。
    enable_solo_match: bool = False
    auto_confirm_match: bool = False
    mmr_base_window: int = 0
    mmr_widen_per_sec: int = 0
    mmr_max_window: int = 0
    optimistic_retry: int = 0

    leader: LeaderConf = Field(default_factory=LeaderConf)

    # ── Duration 取值(解析后缓存在方法里,和 Go 的 .Std() 同位置)──────────
    def ds_allocate_timeout_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.ds_allocate_timeout)

    def confirm_timeout_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.confirm_timeout)

    def match_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.match_interval)

    def start_match_cooldown_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.start_match_cooldown)

    def match_form_cooldown_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.match_form_cooldown)

    def no_capacity_requeue_delay_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.no_capacity_requeue_delay)

    def start_presence_grace_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.start_presence_grace)

    def queue_absence_reap_after_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.queue_absence_reap_after)

    def ticket_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.ticket_ttl)

    def match_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.match_ttl)


class Config(pconfig.BaseConf):
    """matchmaker 服务的完整配置。对应 Go 的 conf.Config。"""

    match: MatchConf = Field(default_factory=MatchConf)
    jwt: JWTConf = Field(default_factory=JWTConf)
    ds_ticket: DSTicketConf = Field(default_factory=DSTicketConf)
    kafka: KafkaConf = Field(default_factory=KafkaConf)
    session_gate: SessionGateConf = Field(default_factory=SessionGateConf)

    def apply_defaults(self) -> None:  # noqa: C901 —— 与 Go 同为一条线性赋值链
        """填默认值 —— 对应 Go 的 Defaults()。判据一律 `== 0`,与 Go 逐字同。

        ★ 为什么不用更"稳"的 `<= 0`:本服务有五个字段用**负值表达「显式关闭」**
        (见模块 docstring)。`<= 0` 会把 `-1s` 兜成默认值 —— 配置写着关、
        实际开着,而两边都不报错。判据符号在这里是语义的一部分。
        """
        m = self.match

        # 旧键兼容(walk_in 的前身 enable_solo_match,2026-07-25 正名)。
        # 用 OR **并入**而非覆盖:漏迁移的部署若被静默判成 false,PVE 实例会从
        # 「单人/整队直进副本」退化为「排队等对手撮合」,而 PVE 侧根本没有单边成局
        # 逻辑(只产 A/B 对战结构),玩家会永远等不到人 —— 比误开 walk-in 严重得多,
        # 故 fail-safe 方向取「保住 walk-in」。旧字段值不清空,留给 main.py 打废弃告警。
        if m.enable_solo_match:
            m.walk_in = True

        if m.ds_allocate_timeout_td() == _dt.timedelta(0):
            m.ds_allocate_timeout = _duration_str(DEFAULT_DS_ALLOCATE_TIMEOUT)
        if m.confirm_timeout_td() == _dt.timedelta(0):
            m.confirm_timeout = _duration_str(DEFAULT_CONFIRM_TIMEOUT)
        if m.match_interval_td() == _dt.timedelta(0):
            m.match_interval = _duration_str(DEFAULT_MATCH_INTERVAL)
        if m.max_queue_tickets == 0:
            m.max_queue_tickets = DEFAULT_MAX_QUEUE_TICKETS
        if m.allocation_workers == 0:
            m.allocation_workers = DEFAULT_ALLOCATION_WORKERS
        if m.start_match_cooldown_td() == _dt.timedelta(0):
            m.start_match_cooldown = _duration_str(DEFAULT_START_MATCH_COOLDOWN)
        if m.start_presence_grace_td() == _dt.timedelta(0):
            m.start_presence_grace = _duration_str(DEFAULT_START_PRESENCE_GRACE)
        if m.queue_absence_reap_after_td() == _dt.timedelta(0):
            m.queue_absence_reap_after = _duration_str(DEFAULT_QUEUE_ABSENCE_REAP_AFTER)
        if m.match_form_cooldown_td() == _dt.timedelta(0):
            m.match_form_cooldown = _duration_str(DEFAULT_MATCH_FORM_COOLDOWN)
        if m.no_capacity_requeue_delay_td() == _dt.timedelta(0):
            m.no_capacity_requeue_delay = _duration_str(DEFAULT_NO_CAPACITY_REQUEUE_DELAY)
        if m.ticket_ttl_td() == _dt.timedelta(0):
            m.ticket_ttl = _duration_str(DEFAULT_TICKET_TTL)
        if m.match_ttl_td() == _dt.timedelta(0):
            m.match_ttl = _duration_str(DEFAULT_MATCH_TTL)
        if m.team_size == 0:
            m.team_size = DEFAULT_TEAM_SIZE
        # 越界钳制(复审 P1:全局 YAML match.team_size 此前完全不校验)。
        # 撮合按 need=side_count×team_size 预分配票据列表:负值(int 型 YAML 可为负)
        # 在 Go 会让 make 负容量 panic,在 Python 会让 need<0 导致**每张票都凑不满、
        # 队列永远不成局**(更静默);巨值会 OOM。钳到 [1, MAX_LEVEL_TEAM_SIZE],
        # 与关卡表入口同一上限常量,防阈值漂移。
        if m.team_size < 1:
            m.team_size = 1
        elif m.team_size > MAX_LEVEL_TEAM_SIZE:
            m.team_size = MAX_LEVEL_TEAM_SIZE
        if m.mmr_base_window == 0:
            m.mmr_base_window = DEFAULT_MMR_BASE_WINDOW
        if m.mmr_widen_per_sec == 0:
            m.mmr_widen_per_sec = DEFAULT_MMR_WIDEN_PER_SEC
        if m.mmr_max_window == 0:
            m.mmr_max_window = DEFAULT_MMR_MAX_WINDOW
        if m.optimistic_retry == 0:
            m.optimistic_retry = DEFAULT_OPTIMISTIC_RETRY
        if m.map_id == 0:
            m.map_id = DEFAULT_MAP_ID
        if not m.game_mode:
            m.game_mode = DEFAULT_GAME_MODE
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR

    def validate_conf(self) -> None:  # noqa: C901 —— 与 Go 同为一条线性判定链
        """对应 Go 的 Validate()。任一条不过 → 启动失败。

        每一条拒的都是「静默塌缩信任域」或「静默关掉一道闸」的配置。
        ★ 方法名不叫 `validate`:pydantic BaseModel 上 `validate` 是保留名
        (v1 的类方法),覆盖它会在模型层面制造一个谁都想不到的行为差异。
        """
        m = self.match

        # ① team_addr 留空的后果是**静默**的:StartMatch 不再校验队伍(任何玩家
        # 可为任意 team_id 开局),对局结束也不再复位准备状态(INC-20260813-001
        # 第一根因)。两者都不报错、不打 ERROR,配错了没有任何人会发现 ——
        # 与事故本身同一类失效形状,因此按 fail-closed 处理:要跳过必须写明白。
        if not m.team_addr and not m.allow_missing_team:
            raise ValueError(
                "match.team_addr is required: empty silently disables both team validation "
                "on StartMatch and ready reset on match end (INC-20260813-001); "
                "set match.allow_missing_team=true to run the skeleton without team"
            )

        # ② Login→Matchmaker 的 resume 读取凭据。缺了它 ResolvePlayerMatchContext
        # 就是一个「谁都能按 player_id 读别人对局上下文(含 battle 票)」的内部接口。
        try:
            internalrpcauth.validate_secret(m.match_resume_auth_secret)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"match.match_resume_auth_secret invalid: {exc}") from exc
        try:
            internalrpcauth.validate_identity(m.match_resume_auth_audience)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"match.match_resume_auth_audience invalid: {exc}") from exc
        # 与玩家 JWT 同钥 = 任何一个登录玩家都能签出内部凭据。
        if m.match_resume_auth_secret == self.jwt.secret:
            raise ValueError(
                "match.match_resume_auth_secret must use an independent trust-domain key"
            )

        # ③ Team 的那把是**可选**的:尚未分发时保持既有 fail-closed 现状
        # (team 侧照旧被拒),部署照常启动。但**配了却非法**仍是致命的 ——
        # 打错一个字的密钥看上去是「已启用」,而每一次 team 调用都静默鉴权失败。
        if m.team_resume_auth_secret:
            try:
                internalrpcauth.validate_secret(m.team_resume_auth_secret)
            except Exception as exc:  # noqa: BLE001
                raise ValueError(f"match.team_resume_auth_secret invalid: {exc}") from exc
            for name, secret in (
                ("player JWT", self.jwt.secret),
                ("Login resume", m.match_resume_auth_secret),
            ):
                if m.team_resume_auth_secret == secret:
                    raise ValueError(
                        f"match.team_resume_auth_secret must not reuse {name} key"
                    )

        # ④ 出站签名钥(matchmaker → team)与 ③ 同款守卫。
        # 「必须与 team_resume 不同」这句话此前没有任何机械 enforcement:
        # 拷错一把钥匙即静默把 BeginTeamMatch/EndTeamMatch(能冻结/复位任意队伍)
        # 的准入边界塌缩进被复用钥匙的信任域(2026-08-17 审计确认)。
        if m.team_call_auth_secret:
            try:
                internalrpcauth.validate_secret(m.team_call_auth_secret)
            except Exception as exc:  # noqa: BLE001
                raise ValueError(f"match.team_call_auth_secret invalid: {exc}") from exc
            try:
                internalrpcauth.validate_identity(m.team_call_auth_audience)
            except Exception as exc:  # noqa: BLE001
                raise ValueError(f"match.team_call_auth_audience invalid: {exc}") from exc
            for name, secret in (
                ("player JWT", self.jwt.secret),
                ("Login resume", m.match_resume_auth_secret),
                ("Team resume", m.team_resume_auth_secret),
            ):
                if secret and m.team_call_auth_secret == secret:
                    raise ValueError(
                        f"match.team_call_auth_secret must not reuse {name} key"
                    )

        # ⑤ 只有真连分配器时才要求 abort 凭据:它签的是「撤销一次已分配但尚未
        # 发票的 DS」这一个精确动作,与上面任何一把共用都等于让持钥方能互相冒充。
        if m.ds_allocator_addr:
            try:
                internalrpcauth.validate_secret(m.allocation_abort_auth_secret)
            except Exception as exc:  # noqa: BLE001
                raise ValueError(
                    f"match.allocation_abort_auth_secret invalid: {exc}"
                ) from exc
            try:
                internalrpcauth.validate_identity(m.allocation_abort_auth_audience)
            except Exception as exc:  # noqa: BLE001
                raise ValueError(
                    f"match.allocation_abort_auth_audience invalid: {exc}"
                ) from exc
            for name, secret in (
                ("player JWT", self.jwt.secret),
                ("Login resume", m.match_resume_auth_secret),
                ("Team resume", m.team_resume_auth_secret),
                ("Team call", m.team_call_auth_secret),
            ):
                if secret and m.allocation_abort_auth_secret == secret:
                    raise ValueError(
                        f"match.allocation_abort_auth_secret must not reuse {name} key"
                    )

    @classmethod
    def load(cls, path: str) -> "Config":
        """加载 + 填默认值。**不在这里跑 validate_conf** —— Go 侧
        config_scan_failed 与 config_validation_failed 是两个独立事件名,
        main.py 按 Go 的顺序分别调用并分别打事件。
        """
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg


def _duration_str(value: _dt.timedelta) -> str:
    """把 timedelta 兜底值写回成 Go duration 串(默认值只会是整秒/整分)。

    为什么把默认值写回**字符串字段**而不是另存一个解析后的字段:
    conf 是「同一份 yaml 两个实现」的契约面,保持字段类型与 yaml 一致才能让
    `cfg.model_dump()` 原样回吐一份合法 yaml(排障时直接对比两栈的生效配置)。
    """
    total = int(value.total_seconds())
    if total % 3600 == 0:
        return f"{total // 3600}h"
    if total % 60 == 0:
        return f"{total // 60}m"
    return f"{total}s"
