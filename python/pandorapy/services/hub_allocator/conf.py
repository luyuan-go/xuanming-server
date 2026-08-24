"""hub_allocator 服务配置 —— 对应 Go 侧 services/battle/hub_allocator/internal/conf/conf.go。

读的是**同一份** services/battle/hub_allocator/etc/hub_allocator-dev.yaml,不另建配置
文件:迁移期 Go 版和 Python 版并存,运维只该维护一份配置。

★ 默认值与 Go 的 `Defaults()` **逐个相同,判据符号也一样**。
  本服 Go 侧**全部**用 `== 0` / `== ""`(没有一个 `<= 0`)。这不是笔误:

    - `transfer_cooldown` 的字段注释明写「<=0 视为不限流」—— 负值是**显式关闭切线
      冷却**的写法。若 Python 抄成 `<= 0`,一份写着 `transfer_cooldown: "-1s"` 的
      yaml 会被兜回 10s:运维以为关了防刷闸、实际它还开着,而**两边都不报错**。
    - `min_replicas` / `max_replicas` / `players_per_hub` / `consolidation_batch`
      同理:负值在 Go 侧原样保留并流进扩缩容算式,写宽判据会把「显式的荒谬值」
      悄悄修正成默认值,于是配置错误再也不会在任何地方暴露出来。

  唯一的非 `== 0` 分支是两条**交叉/机械下限**,原样照抄(见 apply_defaults):
    ① `heartbeat_timeout` 抬到再入屏障 27s(§9.22 时序不等式,正确性下限);
    ② `max_replicas < min_replicas` 时抬到 `min_replicas`。

★ 再入屏障常量**引用** `pandorapy.placement`,不在这里手抄 27。
  抄一份副本会让「Hub 与 Battle 共用同一个屏障」这句话在 Python 侧变成假的:
  真常量被调小时本模块照旧按自己那份没被改的副本兜底 —— 全绿,而脑裂窗口已经开了。

★ 本服 Go 的 `conf.go` **没有** `Validate()`。启动期的配置闸全部散在
  `cmd/hub_allocator/main.go` 里(每条都是 `helper.Errorw(...) + os.Exit(1)`)。
  这里把它们收进 `validate_conf()`,并在每条上注明「不设这道闸会怎样」,顺序与
  main.go 逐条对应(见该函数 docstring 的事件名对照表)。
"""

from __future__ import annotations

import datetime as _dt
import os
import re
from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig
from pandorapy import dsauth, placement

# ── 端口契约 ─────────────────────────────────────────────────────────────────
# Envoy cluster、run_services.ps1 的端口占用检查、K8s Service 都钉在这两个数上。
DEFAULT_GRPC_ADDR = ":20021"
DEFAULT_HTTP_ADDR = ":21021"

# ── Hub DS 分片来源模式(与 ds_allocator.mode 对齐的标准两模式 + 离线兜底)─────
MODE_LOCAL = "local"
MODE_AGONES = "agones"
MODE_MOCK = "mock"

# ── 本机 Hub DS 的两种进程形态(local_hub.launcher)────────────────────────────
LAUNCHER_PACKAGED = "packaged"
LAUNCHER_EDITOR = "editor"

# launcher=editor 专用的引擎 CVar 覆盖参数(与 Go 逐字对齐,含义见 Go 侧长注释:
# 未 cook 的 editor DS 会因 MissingLevelPackage 把 PIE 客户端直接踢掉,形成秒级
# 无限重连循环)。刻意只作用于 editor —— packaged / k8s 跑的是 cook 过的内容,
# 那里的 MissingLevelPackage 是**真的**内容不一致,必须保留引擎的踢人保护。
EDITOR_LAUNCHER_CVAR_ARG = "-DPCVars=net.SkipMissingLevelDisconnect=1"

# ── 写者继任租约档位(hub.writer_lease_mode)──────────────────────────────────
WRITER_LEASE_ENFORCE = "enforce"
WRITER_LEASE_WARMUP = "warmup"
WRITER_LEASE_OFF = "off"

# ── hub 段默认值(与 Go 的 Defaults() 逐个同值)───────────────────────────────
DEFAULT_HEARTBEAT_TIMEOUT = _dt.timedelta(seconds=30)

# 脑裂再入屏障的机械下限(§9.22)。**引用**而不是写 27:
# AssignHub 只有在分片心跳超过 heartbeat_timeout 后才会把玩家改派到新分片,
# 该窗口必须 ≥ DS 授权租约上限 + 偏差余量,否则分区的旧 Hub 尚未对存量玩家完成
# 自我 fencing,新旧两台 Hub 会同时认为自己有权控制同一个玩家。
HEARTBEAT_TIMEOUT_FLOOR = _dt.timedelta(
    seconds=placement.DS_FENCE_REENTRY_BARRIER_SECONDS
)

DEFAULT_SWEEP_INTERVAL = _dt.timedelta(seconds=5)
DEFAULT_SHARD_TTL = _dt.timedelta(minutes=30)
DEFAULT_ASSIGNMENT_TTL = _dt.timedelta(minutes=30)

# Go 的 pkg/auth.DSTicketMaxTTL。Python 侧还没有共享的 dsticket v2 常量家
# (services/login/dsticket.py 是 legacy HS256 那一支,不含 v2 的机械上限),
# 所以在这里落一份,并由 tests 对着 Go 源码断言,防止两边漂移。
DS_TICKET_MAX_TTL = _dt.timedelta(minutes=3)

# DS 侧验票 leeway(Go: main.go 的 `const dsVerifierMaxLeeway = 15 * time.Second`)。
# reservation 必须盖住「票据自身有效窗 + DS 允许的时钟偏差」,否则票还没过期、
# 服务端的座位预留已经先没了 —— 玩家拿着一张合法票被 Hub 判成没有座位。
DS_TICKET_VERIFIER_MAX_LEEWAY = _dt.timedelta(seconds=15)

DEFAULT_RESERVATION_TTL = DS_TICKET_MAX_TTL + DS_TICKET_VERIFIER_MAX_LEEWAY

DEFAULT_REGION = "global"
DEFAULT_CAPACITY = 500
DEFAULT_OPTIMISTIC_RETRY = 3
DEFAULT_MOCK_SHARD_COUNT = 3
DEFAULT_MOCK_HUB_ADDR_HOST = "127.0.0.1"
DEFAULT_MOCK_HUB_PORT_BASE = 7777
DEFAULT_PLAYERS_PER_HUB = 500
DEFAULT_MIN_REPLICAS = 1
DEFAULT_MAX_REPLICAS = 20
DEFAULT_MIGRATE_GRACE_SECONDS = 30
DEFAULT_CONSOLIDATION_BATCH = 50
DEFAULT_TRANSFER_COOLDOWN = _dt.timedelta(seconds=10)

# ── agones 段默认值(留空即 in-cluster 默认)──────────────────────────────────
DEFAULT_AGONES_API_SERVER = "https://kubernetes.default.svc"
DEFAULT_AGONES_NAMESPACE = "default"
DEFAULT_AGONES_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
DEFAULT_AGONES_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
DEFAULT_AGONES_LIST_TIMEOUT = _dt.timedelta(seconds=5)

# ── local_hub 段默认值 ───────────────────────────────────────────────────────
DEFAULT_LOCAL_HUB_ADVERTISE_HOST = "127.0.0.1"
DEFAULT_LOCAL_HUB_PORT = 7777
DEFAULT_LOCAL_HUB_LOG_DIR = "run/dev/logs/ds"

# 一键脚本(play.ps1 / start.ps1)注入的运行期覆盖变量,免改仓库 yaml。
ENV_DS_EXE = "PANDORA_DS_EXE"
ENV_DS_DIR = "PANDORA_DS_DIR"
ENV_DS_LAUNCHER = "PANDORA_DS_LAUNCHER"
ENV_DS_UPROJECT = "PANDORA_DS_UPROJECT"
ENV_DS_ADVERTISE_HOST = "PANDORA_DS_ADVERTISE_HOST"

# ── ds_auth 默认值 —— 对应 Go 的 pkg/config.DSAuthConf.Defaults() ────────────
DEFAULT_DS_AUTH_AUTHORITY_MODE = "legacy"
DEFAULT_DS_AUTH_ISSUER = "pandora-ds-control"
DEFAULT_DS_AUTH_AUDIENCE = "pandora-ds"
DEFAULT_DS_AUTH_BATTLE_TOKEN_TTL = _dt.timedelta(hours=4)
DEFAULT_DS_AUTH_HUB_TOKEN_TTL = _dt.timedelta(hours=24)
DEFAULT_DS_AUTH_ACTIVE_HEARTBEAT_MAX_AGE = _dt.timedelta(seconds=30)

# 「不续期令牌」的启动期最小值(Go: pkg/config 的 dsAuthMin*TokenTTL)。
# 下限不是防「签发即过期」,是防「运行中途过期」:战斗令牌一局一签永不续期,
# 本机 Hub 令牌 local 模式一次性签发也永不续期 —— TTL 太短 = 跑到一半回调被全拒。
DS_AUTH_MIN_BATTLE_TOKEN_TTL = _dt.timedelta(hours=1)
DS_AUTH_MIN_HUB_TOKEN_TTL = _dt.timedelta(hours=1)

# Model B「Redis 唯一授权权威」的 authority_mode 取值。
DS_AUTH_AUTHORITY_REDIS = "redis"

# 本机联调档(Go: pkg/auth.DSLocalProfileOffV1 / DSLocalHubMinTokenTTL)。
# 新 UE 不接受 legacy JWT,也不会在没有 Redis pending/ACK 的本地链路自动降级,
# 所以 mode=local 只允许这一种精确姿态 —— 其它组合继续启动只会得到永远 staged 的 DS。
DS_LOCAL_PROFILE_OFF_V1 = "local-off-v1"
DS_LOCAL_HUB_MIN_TOKEN_TTL = _dt.timedelta(hours=12)

# allocator→UE 的本机运行契约标记环境变量(Go: pkg/auth.DSLocalProfileEnv)。
# 它**不是**授权凭据:UE 侧还会同时校验本地 pod 前缀与非 Agones 运行态,
# 所以 Linux / Agones 不会因为误注入这一个变量就降级成离线档。
# 与 DS_LOCAL_PROFILE_OFF_V1 放在一起,是因为两者永远成对使用(local_fleet.build_env)。
DS_LOCAL_PROFILE_ENV = "PANDORA_DS_LOCAL_PROFILE"


def _duration_str(value: _dt.timedelta) -> str:
    """把 timedelta 兜底值写回成 Go duration 串(如 195s → "3m15s")。

    为什么把默认值写回**字符串字段**而不是另存解析后的字段:conf 是「同一份 yaml
    两个实现」的契约面,保持字段类型与 yaml 一致才能让 `cfg.model_dump()` 原样
    回吐一份合法 yaml(排障时直接对比两栈的生效配置)。
    """
    total = int(value.total_seconds())
    if total == 0:
        return "0s"
    sign = "-" if total < 0 else ""
    total = abs(total)
    hours, rem = divmod(total, 3600)
    minutes, seconds = divmod(rem, 60)
    parts: list[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")
    return sign + "".join(parts)


# Go 的 os.ExpandEnv 语法:`$name` 与 `${name}`,**未定义变量展开成空串**。
# ⚠️ 不能用 Python 的 os.path.expandvars:它把未定义变量**原样留下**。
# 差异的后果是静默的:`${PANDORA_DS_ROOT}/...` 在变量没设时,Go 得到一条以 `/` 开头
# 的(不存在的)路径 → 触发下面的 PANDORA_DS_EXE 兜底;Python 却得到一条含
# `${PANDORA_DS_ROOT}` 字面量的路径,同样不存在、同样走兜底 —— 现在恰好同结果,
# 但一旦哪天有人拿这个字段做前缀匹配或日志比对,两栈就分叉了。
def _local_ds_executable_mismatched(path: str, launcher: str) -> bool:
    """两种形态的 exe 被互换了没有 —— 只认这一种确定错配。

    editor 形态拿到出包的 PandoraServer*,或 packaged 形态拿到引擎的 UnrealEditor*。
    刻意不做白名单(不要求「必须叫 UnrealEditor」):策划机可能用 UnrealEditor-Cmd.exe、
    包装脚本,测试用 stub.exe,这些都是合法的;拦已知错配比放行未知形态更不容易误伤。
    与 Go 侧 services/battle/*/internal/conf/conf.go 的同名判据逐条一致。
    """
    base = os.path.basename(path).lower()
    if launcher == LAUNCHER_EDITOR:
        return base.startswith("pandoraserver")
    return base.startswith("unrealeditor")


def _local_ds_executable_usable(path: str, launcher: str) -> bool:
    """yaml 里写死的 DS 可执行路径,在当前 launcher 形态下算不算「可用的那一个」。

    只判 os.path.exists 是不够的:2026-08-24 事故 —— `-DsLauncher editor` 一次注入
    PANDORA_DS_LAUNCHER / PANDORA_DS_UPROJECT / PANDORA_DS_EXE 三个变量,而 dev 机
    yaml 里写死的 PandoraServer.exe 确实存在,于是唯独 EXE 那一个被跳过 → 出包 server
    拿到 .uproject 当关卡 URL → LoadPackage 失败 → world 为空 →
    UMyLevelModel::OnEnginePostLoadMap 解空指针,DS 启动十几秒后 ACCESS_VIOLATION 崩。
    """
    if path == "":
        return False
    if not os.path.exists(path):
        return False
    return not _local_ds_executable_mismatched(path, launcher)


def _resolve_ds_launcher(raw: str) -> str:
    """launcher 归一(env 优先)。与下方正式归一化逐字等价,幂等。

    需要它是因为 PANDORA_DS_EXE 的兜底判据要按形态判断,而形态在原代码里是**之后**
    才归一的 —— 只判「路径存在」看不出形态,而两种形态要的 exe 根本不是同一个。
    """
    env = (os.environ.get(ENV_DS_LAUNCHER) or "").strip()
    value = (env or raw or "").strip().lower()
    return LAUNCHER_EDITOR if value == LAUNCHER_EDITOR else LAUNCHER_PACKAGED


_ENV_REF_RE = re.compile(r"\$(?:\{([^}]*)\}|([A-Za-z0-9_]+))")


def _expand_env(text: str) -> str:
    """等价 Go 的 `os.ExpandEnv`(未定义 → 空串)。"""

    def _sub(m: re.Match[str]) -> str:
        name = m.group(1) if m.group(1) is not None else m.group(2)
        if not name:
            return ""
        return os.environ.get(name, "")

    return _ENV_REF_RE.sub(_sub, text or "")


def _from_slash(text: str) -> str:
    """等价 Go 的 `filepath.FromSlash`:只把 `/` 换成本平台分隔符,**不做规范化**。

    刻意不用 os.path.normpath —— 那会顺手折叠 `..`、去掉尾部分隔符,让同一份 yaml
    在两栈里得到**不同字符串**的路径。这里要的是「策划写正斜杠也能在 Windows 跑」,
    不是「把路径整理干净」。
    """
    if os.sep == "/":
        return text or ""
    return (text or "").replace("/", os.sep)


class SessionGateConf(BaseModel):
    """会话现行性门 —— 对应 Go 的 `pkg/config.SessionGateConf`。

    ★ 必须显式建模:`BaseConf` 是 extra="allow",不建模整段落进 model_extra,
      yaml 里 `require: true` 写着而 Python 侧当没看见 —— prod 的**拒启强制档
      静默退化成 dev 宽松档**,顶号后旧 JWT 还能继续拿 hub 票。
    """

    model_config = {"extra": "allow"}

    require: bool = False

    # 仅 hub_allocator 使用(R7 收口,分阶段激活):true = AcknowledgeAdmission 对
    # 票据缺 sjti(会话绑定)硬拒;false(默认)= 兼容档,空 sjti 告警放行。
    # 提前置 true = 旧 Hub DS 上所有玩家无法进入大厅,所以默认必须是 false。
    require_ticket_sjti: bool = False


class KafkaConf(BaseModel):
    """对应 Go 的 `pkg/config.KafkaConfig`。

    hub_allocator 只做 producer(发 `pandora.hub.migrate` 强制整合迁移通知,
    push 服务消费)。**弱依赖**:brokers 为空 + consolidation_enabled=true 时,
    迁移只剩 Hub DS drain 心跳指令兜底,玩家看不到无缝倒计时。
    """

    model_config = {"extra": "allow"}

    brokers: list[str] = Field(default_factory=list)
    group_id: str = ""
    partition_cnt: int = 0
    initial_partition: int = 0
    dial_timeout: str = ""
    read_timeout: str = ""
    write_timeout: str = ""
    retry_max: int = 0
    retry_backoff: str = ""
    channel_buffer: int = 0
    sync_interval: str = ""
    stats_interval: str = ""
    compression_type: str = ""
    # ★ Go 的真实默认是 **False**(结构体零值),不是 pkg/config 注释里写的「默认 true」。
    # 全仓没有任何代码给它填值,producer.go 直穿 `c.Producer.Idempotent = cfg.Idempotent`。
    # 本服 dev yaml 显式写了 true,所以这里的默认值只影响「没写它的部署」。
    idempotent: bool = False
    max_open_requests: int = 0
    retention_ms: int = 0

    def dial_timeout_ms(self) -> int:
        """拨号超时(毫秒)。kafkax 收毫秒,yaml 里是 Go duration 串。

        不做这层换算的话 `dial_timeout: "2s"` 会被整段忽略 —— broker 不通时
        producer 用客户端库默认超时干等,而 yaml 明明写了 2s。
        """
        return int(pconfig.parse_duration(self.dial_timeout).total_seconds() * 1000)

    def configured(self) -> bool:
        """判据是「有没有**非空白**的 broker」,不是 `len(brokers)>0`。

        ConfigMap 渲染出 `brokers: [""]` 时长度是 1 而实际一个 broker 都没有,
        按长度判会让 producer 用空 broker 列表构造 —— 要么当场炸,要么更糟:
        构造成功但迁移通知永远发不出去。
        """
        return any((b or "").strip() for b in self.brokers)


class DSTicketConf(BaseModel):
    """玩家 DSTicket v2(RS256,方案 B)—— 对应 Go 的 `pkg/config.DSTicketConf`。

    `private_key_file` 非空即启用:hub 票改由 DSTicketSigner 签发并**绑死到唯一
    Hub DS 实例**(绑定不完整时 fail-closed 拒签)。留空 = 沿用 legacy HS256。
    """

    model_config = {"extra": "allow"}

    private_key_file: str = ""
    active_kid: str = ""
    ttl: str = ""
    jwks_file: str = ""
    keyset_revision: str = ""

    def signer_enabled(self) -> bool:
        """本服务是否启用 v2 签发。对应 Go 的 `SignerEnabled()`。"""
        return self.private_key_file != ""

    def verifier_enabled(self) -> bool:
        return self.jwks_file != ""

    def ttl_td(self) -> _dt.timedelta:
        """票据有效期。零值 = 由签发器取 DSTicketDefaultTTL(120s)。

        ★ conf 层**不填默认**,与 Go 一致:默认值在 `auth.NewDSTicketSigner` 里,
        在这里替它填一个会让「conf 层看到的 TTL」与「签发器实际用的 TTL」
        在某次改动后悄悄分叉,而 reservation_ttl 的下限校验正是读这个值。
        """
        return pconfig.parse_duration(self.ttl)

    def effective_ttl_td(self) -> _dt.timedelta:
        """签发器实际生效的 TTL(零值 → 默认 120s,上限 180s)。

        `validate_conf` 的 reservation 下限要拿它比,不能拿裸 `ttl_td()`:
        yaml 没写 ttl 时裸值是 0,下限会退化成「只要 ≥15s 就算过」——
        闸看着在、其实拦不住任何东西。
        """
        ttl = self.ttl_td()
        if ttl == _dt.timedelta(0):
            return _dt.timedelta(seconds=120)
        return ttl


class DSAuthFenceConf(BaseModel):
    """对应 Go 的 `pkg/config.DSAuthFenceConf`(authority_mode=redis 的机械激活栅栏)。"""

    model_config = {"extra": "allow"}

    etcd_endpoints: list[str] = Field(default_factory=list)
    etcd_prefix: str = ""
    etcd_lease_ttl_sec: int = 0
    etcd_dial_timeout: str = ""
    keyset_revision: str = ""

    def etcd_dial_timeout_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.etcd_dial_timeout)


class DSAuthConf(BaseModel):
    """DS→后端回调服务令牌 —— 对应 Go 的 `pkg/config.DSAuthConf`。

    hub_allocator 是**双角色**服务:
      - 签发:ListShards 发现 ready Hub DS 时签 hub 令牌(绑 pod),经 GameServer
        annotation(agones,剩余寿命 < TTL/3 时续期)/ PANDORA_DS_TOKEN env(local)下发;
      - 校验:Heartbeat 按 mode(off/permissive/enforce)验证令牌 + pod 范围绑定。
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

    def battle_token_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.battle_token_ttl)

    def hub_token_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.hub_token_ttl)

    def active_heartbeat_max_age_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.active_heartbeat_max_age)

    def authority_mode_redis(self) -> bool:
        """是否启用 Model B「Redis 唯一授权权威」。对应 Go 的 `AuthorityModeRedis()`。

        ★ 判据是**精确等于** "redis",与 Go 逐字一致:不 strip、不 lower。
        写成宽松匹配会让 `"Redis"` 这种拼法在 Python 上启用 Model B、在 Go 上
        退回 legacy —— 同一份 yaml 两套授权语义。
        """
        return self.authority_mode == DS_AUTH_AUTHORITY_REDIS

    def signer_enabled(self) -> bool:
        """是否签发 DS 回调令牌。对应 Go 的 `NewDSCallbackSignerFromConf`:
        Secret 未配 → (nil, nil),即本服务不签发。"""
        return self.secret != ""

    def guard_mode(self) -> dsauth.Mode:
        """解析校验档位。非法值**报错**(Go: ParseDSAuthMode 返回 error → 启动即拒)。

        不设这道闸会怎样:`mode: "enfroce"` 静默回落成 off,Heartbeat 的令牌门
        整个不生效,而 yaml 上写着 enforce、启动日志全绿。
        """
        return dsauth.parse_mode(self.mode)

    def guard_enabled(self) -> bool:
        """是否装配校验守卫。对应 Go:mode=off(含空)→ guard 为 nil。"""
        return self.guard_mode() is not dsauth.Mode.OFF

    def apply_defaults(self) -> None:
        """对应 Go 的 `DSAuthConf.Defaults()`。

        ★ mode / secret 留空即「不启用」,**刻意不填默认** —— 给 mode 填个默认值
        等于替运维决定要不要开一道鉴权门。
        """
        if self.authority_mode == "":
            self.authority_mode = DEFAULT_DS_AUTH_AUTHORITY_MODE
        if self.issuer == "":
            self.issuer = DEFAULT_DS_AUTH_ISSUER
        if self.audience == "":
            self.audience = DEFAULT_DS_AUTH_AUDIENCE
        if self.battle_token_ttl_td() == _dt.timedelta(0):
            self.battle_token_ttl = _duration_str(DEFAULT_DS_AUTH_BATTLE_TOKEN_TTL)
        if self.hub_token_ttl_td() == _dt.timedelta(0):
            self.hub_token_ttl = _duration_str(DEFAULT_DS_AUTH_HUB_TOKEN_TTL)
        if self.active_heartbeat_max_age_td() == _dt.timedelta(0):
            self.active_heartbeat_max_age = _duration_str(
                DEFAULT_DS_AUTH_ACTIVE_HEARTBEAT_MAX_AGE
            )

    def validate_token_ttl(self, enabled: bool) -> None:
        """对应 Go 的 `DSAuthConf.Validate(enabled)`。

        不设这道闸会怎样:令牌**不续期**的两条路径(战斗 DS 一局一签、local Hub
        一次性 env 下发)会在运行中途过期,此后该 DS 的所有回调被全拒 ——
        表现是「对局跑到一半赛果结算不了」「大厅心跳突然全 401」,而配置没人动过。
        """
        if not enabled:
            return
        if self.battle_token_ttl_td() < DS_AUTH_MIN_BATTLE_TOKEN_TTL:
            raise ValueError(
                f"ds_auth: battle_token_ttl={self.battle_token_ttl} too small "
                f"(need >= {_duration_str(DS_AUTH_MIN_BATTLE_TOKEN_TTL)}; "
                f"战斗令牌不续期,须覆盖最长对局+重连窗口)"
            )
        if self.hub_token_ttl_td() < DS_AUTH_MIN_HUB_TOKEN_TTL:
            raise ValueError(
                f"ds_auth: hub_token_ttl={self.hub_token_ttl} too small "
                f"(need >= {_duration_str(DS_AUTH_MIN_HUB_TOKEN_TTL)}; "
                f"大厅令牌 local 模式不续期,须覆盖一段常驻会话)"
            )

    def validate_redis_fence(self) -> None:
        """对应 Go 的 `ValidateRedisFence()`。

        不设这道闸会怎样:Model B 把 Redis 当**唯一**授权权威,而 fence 缺失时
        没有任何机械手段阻止「两个不同版本的 allocator 副本同时写同一份授权记录」
        —— 授权权威自己先脑裂,下游所有 exact 绑定都失去意义。
        """
        if not self.authority_mode_redis():
            return
        # authority_mode=redis 却不 enforce = 权威记录照写、校验却放行,
        # 等于建了一套授权状态机然后不用它。
        if self.mode != "enforce":
            raise ValueError("ds_auth: authority_mode=redis requires mode=enforce")
        if not self.fence.etcd_endpoints:
            raise ValueError("ds_auth: authority_mode=redis requires fence.etcd_endpoints")
        if self.fence.keyset_revision == "":
            raise ValueError(
                "ds_auth: authority_mode=redis requires immutable fence.keyset_revision"
            )
        if self.fence.etcd_lease_ttl_sec < 0:
            raise ValueError(
                "ds_auth: fence.etcd_lease_ttl_sec must be positive or zero(default)"
            )
        if self.fence.etcd_dial_timeout_td() < _dt.timedelta(0):
            raise ValueError(
                "ds_auth: fence.etcd_dial_timeout must be positive or zero(default)"
            )
        # ★ 这条判据是 `<= 0`(Go 原样),不是 `== 0`:active credential 的最大
        # 心跳年龄是「多久之前的心跳还算活着」,零和负都表示**任何心跳都不算活着**,
        # 那样 Model B 下所有业务写 RPC 会被无差别拒绝。
        if self.active_heartbeat_max_age_td() <= _dt.timedelta(0):
            raise ValueError(
                "ds_auth: authority_mode=redis requires positive active_heartbeat_max_age"
            )


class JWTConf(BaseModel):
    """签发 hub DSTicket 的 JWT 参数(镜像 login.JWTConf / matchmaker.JWTConf)。

    issuer / audience / secret 必须与 login 服务 + Envoy jwt_authn provider 完全一致,
    否则签出来的 hub 票在网关那一跳就会被拒。
    """

    model_config = {"extra": "allow"}

    issuer: str = ""
    audience: str = ""
    secret: str = ""
    # 仅用于**校验**的额外可接受密钥(不用于签发),支持玩家面 JWT 不停服密钥轮换。
    additional_secrets: list[str] = Field(default_factory=list)
    session_ttl: str = ""
    ds_ticket_ttl: str = ""

    def session_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.session_ttl)

    def ds_ticket_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.ds_ticket_ttl)


class AgonesConf(BaseModel):
    """真 Agones Hub DS Fleet 发现配置(mode=agones 时生效)。

    集群内运行时 token_path / ca_path / api_server / namespace 留空即 in-cluster 默认;
    集群外联调(本机进程 → minikube)可显式指定 api_server + token_path。
    """

    model_config = {"extra": "allow"}

    # legacy 开关:`mode` 留空时用它推导 mode(向后兼容旧配置)。
    enabled: bool = False
    api_server: str = ""
    namespace: str = ""
    # mode=agones 时必填,否则 fleet provider 构造失败。
    fleet_name: str = ""
    # canary_percent>0 时必填;stable 玩家永不进入此 Fleet。
    canary_fleet_name: str = ""
    # 以 player_id 做确定性 cohort;已有 assignment 的 release_track 是**粘性权威**,
    # 不随百分比调整漂移(否则一次调参会把在场玩家来回甩到两条轨上)。
    canary_percent: int = 0
    canary_seed: str = ""
    # 覆盖返回给客户端连接的 host;留空则用 Agones status.address。
    advertise_host: str = ""
    # 留 "-" 显式禁用 token(kubectl proxy 联调)。
    token_path: str = ""
    ca_path: str = ""
    # 仅 dev;生产禁用。
    insecure_skip_tls_verify: bool = False
    list_timeout: str = ""

    def list_timeout_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.list_timeout)


class LocalHubConf(BaseModel):
    """本机 exec 一个常驻 Windows Hub DS(mode=local)。

    与 ds_allocator.LocalDSConf 对称:hub_allocator 在首次 AssignHub 时懒拉起一个
    常驻 Hub DS 进程(加载大厅关卡 / PandoraHubGameMode),把它作为唯一分片返回;
    进程随 hub_allocator 退出而 Kill。**常驻不按对局回收**(与战斗 DS 不同)。
    """

    model_config = {"extra": "allow"}

    # packaged(默认)= 跑打包好的 PandoraServer.exe;editor = 跑 UnrealEditor.exe +
    # .uproject -server,直接读未 cook 的工程内容(策划存盘即生效,免出包)。
    launcher: str = ""
    # 含义随 launcher 变化:packaged → PandoraServer.exe;editor → UnrealEditor.exe。
    executable_path: str = ""
    # 仅 launcher=editor 必填。UE 的 LaunchSetGameName 只认命令行里第一个不以 '-'
    # 开头的 token,所以它必须排在关卡 URL 之前(buildArgs 保证该顺序)。
    project_path: str = ""
    # 启动时加载的大厅关卡(DS 命令行首个位置参数)。留空则由 DS 自身默认关卡决定。
    map_name: str = ""
    advertise_host: str = ""
    port: int = 0
    # 留空取 hub.default_region / hub.default_capacity。
    region: str = ""
    capacity: int = 0
    working_dir: str = ""
    log_dir: str = ""
    extra_args: list[str] = Field(default_factory=list)
    extra_env: dict[str, str] = Field(default_factory=dict)


class HubConf(BaseModel):
    """hub_allocator 私有配置段。字段名与 Go 的 yaml tag 逐字一致。"""

    model_config = {"extra": "allow"}

    # 超过此时长没收到 Heartbeat → 分片标记 draining 并移出可分配集(不变量 §9.4)。
    # ★ 同时是 §9.22 的再入屏障:见 apply_defaults 的机械下限。
    heartbeat_timeout: str = ""
    sweep_interval: str = ""
    # 分片镜像 / 玩家归属的 Redis key TTL(每次 Assign/Heartbeat/Transfer 刷新)。
    shard_ttl: str = ""
    assignment_ttl: str = ""
    # 从 allocator 签票前占位到 Hub DS Admission ACK 的**绝对** lease。
    reservation_ttl: str = ""
    default_region: str = ""
    default_capacity: int = 0
    # WATCH/MULTI/EXEC 乐观锁冲突最大重试次数,耗尽返 ErrHubNoAvailable。
    optimistic_retry: int = 0
    # MockHubFleetProvider 每 region 种的假分片数 / host / 端口基址(分片 port = base + shard_id)。
    mock_shard_count: int = 0
    mock_hub_addr_host: str = ""
    mock_hub_port_base: int = 0
    # 自动扩缩容 / 强制整合。两者都**只在 mode=agones 下真正运行**:
    # Mock provider 是拓扑-only 不实现 scaler,开了也不会生效(见 validate_conf 的告警)。
    autoscale_enabled: bool = False
    players_per_hub: int = 0
    min_replicas: int = 0
    max_replicas: int = 0
    consolidation_enabled: bool = False
    # owner 权威服务地址。空 = 不双写实例租约(未启用,现网行为不变,安全默认)。
    owner_addr: str = ""
    # false = migrate 弱依赖(双写失败只告警);true = contract 阶段强依赖
    # (续租失败心跳必须失败 → DS 自我 fencing)。
    owner_lease_required: bool = False
    # 迁移优雅倒计时(秒);也是排空分片可被缩容回收的最短等待(避免提前杀 pod)。
    migrate_grace_seconds: int = 0
    # 单次 reconcile 每个排空分片最多迁移的玩家数(防撑死),超出部分下个周期续排。
    consolidation_batch: int = 0
    # 玩家主动切线冷却(防刷)。**<=0 视为不限流** —— 所以默认值判据必须是 `== 0`。
    transfer_cooldown: str = ""
    # player_locator gRPC 地址(切线护栏:战斗/匹配中禁切)。
    # 弱依赖:留空则跳过位置检查(locator 抖动不硬阻断低危的大厅切线)。
    locator_addr: str = ""
    # 写者继任租约档位(enforce|warmup|off;留空 = enforce,保持现网行为)。
    writer_lease_mode: str = ""

    # ── Duration 取值(与 Go 的 .Std() 同位置)────────────────────────────────
    def heartbeat_timeout_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.heartbeat_timeout)

    def sweep_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.sweep_interval)

    def shard_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.shard_ttl)

    def assignment_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.assignment_ttl)

    def reservation_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.reservation_ttl)

    def transfer_cooldown_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.transfer_cooldown)

    def resolve_writer_lease_mode(self) -> str:
        """归一化并校验 writer_lease_mode(空 → enforce)。对应 Go 的同名方法。

        ★ **不 strip、不 lower**,与 Go 的 switch 逐字一致。看起来"更稳"的
        `.strip().lower()` 会让 `"Enforce"` 在 Python 上正常启动、在 Go 上启动即拒 ——
        同一份 yaml 两个结果,而这是个**安全档位**。

        不设这道闸会怎样:非法值静默退化(比如落到 off)= 多副本 RollingUpdate 下
        没有单写者选举,两个副本同时推 fence 扫描,谁也不知道。
        """
        mode = self.writer_lease_mode
        if mode in ("", WRITER_LEASE_ENFORCE):
            return WRITER_LEASE_ENFORCE
        if mode in (WRITER_LEASE_WARMUP, WRITER_LEASE_OFF):
            return mode
        raise ValueError(
            f"hub.writer_lease_mode {mode!r} invalid (want enforce|warmup|off)"
        )


class Config(pconfig.BaseConf):
    """hub_allocator 服务的完整配置。对应 Go 的 conf.Config。"""

    # Hub DS 分片来源的**权威开关**;留空时才回退按 agones.enabled 推导。
    mode: str = ""
    hub: HubConf = Field(default_factory=HubConf)
    jwt: JWTConf = Field(default_factory=JWTConf)
    ds_ticket: DSTicketConf = Field(default_factory=DSTicketConf)
    agones: AgonesConf = Field(default_factory=AgonesConf)
    local_hub: LocalHubConf = Field(default_factory=LocalHubConf)
    ds_auth: DSAuthConf = Field(default_factory=DSAuthConf)
    kafka: KafkaConf = Field(default_factory=KafkaConf)
    session_gate: SessionGateConf = Field(default_factory=SessionGateConf)

    # ── Defaults ─────────────────────────────────────────────────────────────
    def apply_defaults(self) -> None:  # noqa: C901 —— 与 Go 同为一条线性赋值链
        """填默认值 —— 对应 Go 的 `Defaults()`,**逐条同序同判据**。

        顺序照抄不是形式主义:`local_hub.region` / `capacity` 的兜底**读的是已经
        填过默认值的** `hub.default_region` / `hub.default_capacity`。把 local_hub
        提前会让它们兜到空串 / 0,本机 Hub 分片于是落在一个不存在的 region 上,
        而 AssignHub 只会返回「无可用分片」——没有任何一条日志指向配置顺序。
        """
        # ① mode 归一化:显式 mode 优先;留空时按 legacy 的 agones.enabled 推导。
        self.mode = (self.mode or "").strip().lower()
        if self.mode == "":
            self.mode = MODE_AGONES if self.agones.enabled else MODE_MOCK

        h = self.hub
        if h.heartbeat_timeout_td() == _dt.timedelta(0):
            h.heartbeat_timeout = _duration_str(DEFAULT_HEARTBEAT_TIMEOUT)
        # ② 脑裂再入屏障机械下限(§9.22,pkg/placement 契约)。
        #
        # ★ 这是**正确性下限而非调优参数**:AssignHub 只有在分片心跳超过
        # heartbeat_timeout 之后才会把玩家改派到新分片。该窗口必须 ≥ DS 授权租约
        # 上限 + 偏差余量(27s),才能保证被网络分区的旧 Hub 已经对存量玩家完成
        # 自我 fencing。调低不会报任何错,只会在下一次分区时表现为「一个玩家同时
        # 在两台 Hub 上」——所以这里机械抬回下限,不拒启、不告警(与 Go 一致)。
        if h.heartbeat_timeout_td() < HEARTBEAT_TIMEOUT_FLOOR:
            h.heartbeat_timeout = _duration_str(HEARTBEAT_TIMEOUT_FLOOR)
        if h.sweep_interval_td() == _dt.timedelta(0):
            h.sweep_interval = _duration_str(DEFAULT_SWEEP_INTERVAL)
        if h.shard_ttl_td() == _dt.timedelta(0):
            h.shard_ttl = _duration_str(DEFAULT_SHARD_TTL)
        if h.assignment_ttl_td() == _dt.timedelta(0):
            h.assignment_ttl = _duration_str(DEFAULT_ASSIGNMENT_TTL)
        # ③ reservation 默认 = 票据机械上限 + DS 验票 leeway。默认值本身就满足
        # validate_conf 的下限不等式,所以「不配」永远是安全的。
        if h.reservation_ttl_td() == _dt.timedelta(0):
            h.reservation_ttl = _duration_str(DEFAULT_RESERVATION_TTL)
        if h.default_region == "":
            h.default_region = DEFAULT_REGION
        if h.default_capacity == 0:
            h.default_capacity = DEFAULT_CAPACITY
        if h.optimistic_retry == 0:
            h.optimistic_retry = DEFAULT_OPTIMISTIC_RETRY
        if h.mock_shard_count == 0:
            h.mock_shard_count = DEFAULT_MOCK_SHARD_COUNT
        if h.mock_hub_addr_host == "":
            h.mock_hub_addr_host = DEFAULT_MOCK_HUB_ADDR_HOST
        if h.mock_hub_port_base == 0:
            h.mock_hub_port_base = DEFAULT_MOCK_HUB_PORT_BASE
        if h.players_per_hub == 0:
            h.players_per_hub = DEFAULT_PLAYERS_PER_HUB
        if h.min_replicas == 0:
            h.min_replicas = DEFAULT_MIN_REPLICAS
        if h.max_replicas == 0:
            h.max_replicas = DEFAULT_MAX_REPLICAS
        # ④ 交叉钳制:上限低于下限时抬到下限。
        # 破了会怎样:scaler 每轮都把期望副本数钳到一个小于保底值的数,
        # 于是「开服保底 N 个大厅」这条承诺静默失效,而两个字段各自看都合法。
        if h.max_replicas < h.min_replicas:
            h.max_replicas = h.min_replicas
        if h.migrate_grace_seconds == 0:
            h.migrate_grace_seconds = DEFAULT_MIGRATE_GRACE_SECONDS
        if h.consolidation_batch == 0:
            h.consolidation_batch = DEFAULT_CONSOLIDATION_BATCH
        # ★ `== 0`:负值是「显式关闭切线冷却」(字段注释原话「<=0 视为不限流」)。
        if h.transfer_cooldown_td() == _dt.timedelta(0):
            h.transfer_cooldown = _duration_str(DEFAULT_TRANSFER_COOLDOWN)

        a = self.agones
        if a.api_server == "":
            a.api_server = DEFAULT_AGONES_API_SERVER
        if a.namespace == "":
            a.namespace = DEFAULT_AGONES_NAMESPACE
        if a.token_path == "":
            a.token_path = DEFAULT_AGONES_TOKEN_PATH
        if a.ca_path == "":
            a.ca_path = DEFAULT_AGONES_CA_PATH
        if a.list_timeout_td() == _dt.timedelta(0):
            a.list_timeout = _duration_str(DEFAULT_AGONES_LIST_TIMEOUT)

        self.ds_auth.apply_defaults()
        self._apply_local_hub_defaults()

        if self.server.grpc.addr == "":
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if self.server.http.addr == "":
            self.server.http.addr = DEFAULT_HTTP_ADDR

    def _apply_local_hub_defaults(self) -> None:
        """local_hub 段的默认值 + 环境变量兜底(对应 Go Defaults() 的后半段)。

        路径字段做三步,顺序与 Go 一致:
          1. `${VAR}` / `$VAR` 展开(Go 的 os.ExpandEnv,未定义 → 空串);
          2. `filepath.FromSlash` 归一化分隔符(策划在 yaml 里写正斜杠也能在 Windows 跑);
          3. 展开后的路径在本机不存在时,回退到启动脚本按平级 Client 目录探测注入的
             PANDORA_DS_EXE / PANDORA_DS_DIR。

        为什么第 3 步的判据是「路径为空**或**不存在」而不是只判空:仓库 yaml 里写死的是
        本开发机的 `F:\\work\\Packages\\...`,换一台机器那条路径依然非空、只是不存在。
        只判空的话策划机会拿着一条指向虚无的路径去 exec,报的是「系统找不到指定的文件」,
        而一键脚本明明已经把正确路径注入了环境变量。
        """
        lh = self.local_hub
        lh.executable_path = _from_slash(_expand_env(lh.executable_path))
        lh.working_dir = _from_slash(_expand_env(lh.working_dir))

        env_exe = os.environ.get(ENV_DS_EXE, "")
        if env_exe != "":
            if not _local_ds_executable_usable(
                lh.executable_path, _resolve_ds_launcher(lh.launcher)
            ):
                lh.executable_path = _from_slash(env_exe)
                env_dir = os.environ.get(ENV_DS_DIR, "")
                if env_dir != "":
                    lh.working_dir = _from_slash(env_dir)

        # launcher:一键脚本可用 PANDORA_DS_LAUNCHER 免改 yaml 切换(同时作用于大厅和战斗 DS)。
        env_launcher = os.environ.get(ENV_DS_LAUNCHER, "").strip()
        if env_launcher != "":
            lh.launcher = env_launcher
        # ★ 缺省 / 非法值一律归一到 packaged(现状行为),**不报错**:与 Go 逐字一致。
        # 这与 writer_lease_mode 的 fail-fast 相反,因为它不是安全档位 —— 打错字的
        # 后果是「跑了打包版而不是编辑器版」,启动日志里的 launcher 字段直接可见。
        lh.launcher = (lh.launcher or "").strip().lower()
        if lh.launcher != LAUNCHER_EDITOR:
            lh.launcher = LAUNCHER_PACKAGED

        lh.project_path = _from_slash(_expand_env(lh.project_path))
        env_proj = os.environ.get(ENV_DS_UPROJECT, "").strip()
        if env_proj != "":
            if lh.project_path == "" or not os.path.exists(lh.project_path):
                lh.project_path = _from_slash(env_proj)

        # advertise_host 是每台机器各异的运行期值:内网测试服要用局域网 IP
        # (远程策划客户端才连得到大厅 DS)。启动脚本探测后经环境变量注入,
        # 优先级**高于** yaml 写死值。
        env_host = os.environ.get(ENV_DS_ADVERTISE_HOST, "").strip()
        if env_host != "":
            lh.advertise_host = env_host
        if lh.advertise_host == "":
            lh.advertise_host = DEFAULT_LOCAL_HUB_ADVERTISE_HOST
        if lh.port == 0:
            lh.port = DEFAULT_LOCAL_HUB_PORT
        if lh.region == "":
            lh.region = self.hub.default_region
        if lh.capacity == 0:
            lh.capacity = self.hub.default_capacity
        if lh.log_dir == "":
            lh.log_dir = DEFAULT_LOCAL_HUB_LOG_DIR

    # ── 派生判定 ─────────────────────────────────────────────────────────────
    def ds_auth_enabled(self) -> bool:
        """本服务是否需要跑 ds_auth 的 TTL 校验。

        对应 Go main.go 的 `cfg.DSAuth.Validate(dsSigner != nil || dsGuard != nil)`:
        signer 在 secret 非空时构造,guard 在 mode != off 时构造。
        """
        return self.ds_auth.signer_enabled() or self.ds_auth.guard_enabled()

    def model_b_authority(self) -> bool:
        """Model B「Redis 唯一授权权威」是否启用。对应 Go main.go 的 `modelBAuthority`。

        三者齐备才启用:agones + ds_auth.mode=enforce + authority_mode=redis。
        少任何一个都退回 legacy 代际镜像门 —— 所以这里不能只判 authority_mode。
        """
        return (
            self.mode == MODE_AGONES
            and self.ds_auth.guard_mode() is dsauth.Mode.ENFORCE
            and self.ds_auth.authority_mode_redis()
        )

    def player_facing_keys(self) -> list[str]:
        """玩家面密钥集(主密钥 + 全部 additional)。

        ★ 刻意**不过滤空串**,与 Go 的 `AdditionalSecretsBytes` 一致:空条目是
        轮换清单事故(少写了一把却留了占位),静默过滤会让运维以为旧密钥仍被接受、
        实则轮换断档。
        """
        return [self.jwt.secret, *self.jwt.additional_secrets]

    def ds_callback_keys(self) -> list[str]:
        return [self.ds_auth.secret, *self.ds_auth.additional_secrets]

    # ── Validate ─────────────────────────────────────────────────────────────
    def validate_conf(self) -> list[str]:  # noqa: C901 —— 与 Go 同为一条线性判定链
        """启动期配置闸。致命项抛 ValueError,非致命项以告警文本返回。

        ★ Go 侧本服的 `conf.go` **没有** `Validate()`,这些闸散在 main.go 里。
        逐条对照(顺序与 main.go 相同):

        | # | Go 事件名 | 处置 |
        |---|---|---|
        | ① | `ds_auth_redis_authority_requires_agones` | fatal |
        | ② | `ds_auth_fence_config_invalid` | fatal |
        | ③ | `redis_endpoint_required` | fatal |
        | ④ | `agones_requires_ds_ticket_v2` | fatal |
        | ⑤ | `ds_auth_guard_init_failed` | fatal |
        | ⑥ | `ds_auth_ttl_invalid` | fatal |
        | ⑦ | `hub_reservation_ttl_invalid` | fatal |
        | ⑧ | `jwt_ds_auth_secret_overlap` / `_dev` | enforce 下 fatal,否则告警 |
        | ⑨ | `local_hub_auth_profile_invalid` | fatal(mode=local) |
        | ⑩ | `autoscale_inert_under_mock` | 告警 |
        | ⑪ | writer_lease_mode 非法 | fatal |

        为什么⑧要分两档:dev 模板两面共用同一把公开 dev 密钥,硬拒会打断本地
        一键启动(§14「默认路径不许坏」);而 enforce 是生产姿态,那里必须拒。
        """
        warnings: list[str] = []
        da = self.ds_auth

        # ① Model B 只在 agones 下有实现。非 agones 却配 redis 权威 = 配了一套
        # 根本不会被装配的授权状态机,运维以为「Redis 唯一权威」已经生效。
        if da.authority_mode_redis() and self.mode != MODE_AGONES:
            raise ValueError(
                f"ds_auth_redis_authority_requires_agones: authority_mode=redis "
                f"requires mode=agones (got mode={self.mode!r})"
            )

        # ② Redis 单一权威缺机械 fence 不能启动(理由见 validate_redis_fence)。
        da.validate_redis_fence()

        # ③ Redis 是本服**强依赖**(分片镜像 + 玩家归属 + 心跳超时 ZSET)。
        # 两者皆空才算未配置:单实例填 host,Cluster / Sentinel 只填 addrs。
        # 不设这道闸会怎样:Python 的 new_client("") 会去连 127.0.0.1:6379 ——
        # 连上一个**无关的**本机 Redis,进程照常起来,而所有归属写进了错的库。
        rc = self.node.redis_client
        if rc.host == "" and not rc.addrs:
            raise ValueError(
                "redis_endpoint_required: set node.redis_client.host (single) "
                "or node.redis_client.addrs (cluster)"
            )

        # ④ B1:k8s Hub 只允许 RS256 实例绑定票。缺 v2 私钥就退回 legacy HS256 票,
        # 而 legacy 票**不绑 DS 实例** —— 一张票能在 Fleet 里任何一台 Hub 上用,
        # §9.22 的 exact 实例绑定整个失效,且没有任何运行期信号。
        if self.mode == MODE_AGONES and not self.ds_ticket.signer_enabled():
            raise ValueError(
                "agones_requires_ds_ticket_v2: configure ds_ticket.private_key_file "
                "+ active_kid (B1 k8s Hub 只允许 RS256 实例绑定票)"
            )

        # ⑤ mode=permissive/enforce 但 secret 未配 = 配置矛盾:守卫构造不出来,
        # Go 直接 fatal 而不是静默不校验。顺带把 mode 拼写错也在这里拦下。
        guard_mode = da.guard_mode()  # 非法拼写在此抛错
        if guard_mode is not dsauth.Mode.OFF and da.secret == "":
            raise ValueError(
                f"ds_auth_guard_init_failed: ds_auth.mode={guard_mode.value} "
                f"requires ds_auth.secret"
            )
        # 空串 additional 条目是轮换清单事故(见 player_facing_keys 注释)。
        for idx, sec in enumerate(da.additional_secrets):
            if sec == "":
                raise ValueError(
                    f"ds_auth_guard_init_failed: ds_auth.additional_secrets[{idx}] "
                    f"is empty (轮换清单少写了一把却留了占位;静默过滤会让轮换断档)"
                )

        # ⑥ 不续期令牌的 TTL 下限。
        da.validate_token_ttl(self.ds_auth_enabled())

        # ⑦ Model B 的 reservation 时序不等式:
        #        DSTicket 有效窗 + DS 验票 leeway  <=  reservation_ttl  <=  assignment_ttl
        #
        # 下限破了:票还在有效期内、服务端的座位预留已经先过期 —— 玩家拿一张合法票
        # 被 Hub 判成没座位,表现为「进大厅偶发失败」且票据日志全绿。
        # 上限破了:预留活得比归属还久,归属过期后座位仍被占着,分片容量被幽灵座位吃掉。
        if self.model_b_authority():
            reservation = self.hub.reservation_ttl_td()
            minimum = self.ds_ticket.effective_ttl_td() + DS_TICKET_VERIFIER_MAX_LEEWAY
            assignment = self.hub.assignment_ttl_td()
            if reservation < minimum or reservation > assignment:
                raise ValueError(
                    f"hub_reservation_ttl_invalid: reservation_ttl={self.hub.reservation_ttl} "
                    f"must cover DSTicket TTL + "
                    f"{_duration_str(DS_TICKET_VERIFIER_MAX_LEEWAY)} verifier leeway "
                    f"(>= {_duration_str(minimum)}) and not exceed "
                    f"assignment_ttl ({_duration_str(assignment)})"
                )

        # ⑧ 玩家面 / DS 回调面密钥集必须不相交(P0)。hub_allocator 是唯一同时装配
        # 两面密钥的服务:任一交叉 = 泄露一面即可伪造另一面(拿玩家 JWT 密钥签一张
        # DS 回调令牌,就能冒充任意 Hub DS 上报心跳、改玩家归属)。
        if da.secret != "":
            overlap = self._first_secret_overlap()
            if overlap is not None:
                i, j = overlap
                msg = (
                    f"jwt_ds_auth_secret_overlap: player-facing key[{i}] equals "
                    f"ds-callback key[{j}] (P0: 玩家面 jwt.secret/additional_secrets 与 "
                    f"ds_auth.secret/additional_secrets 必须是两套完全独立的密钥)"
                )
                if guard_mode is dsauth.Mode.ENFORCE:
                    raise ValueError(msg)
                warnings.append(msg + " —— 仅 dev 可容忍;生产(enforce)会启动即拒")

        # ⑨ mode=local 只允许精确的 local-off-v1 姿态。
        # 不设这道闸会怎样:新 UE 不接受 legacy JWT,也不会在没有 Redis pending/ACK
        # 的本地链路自动降级 —— 姿态不对时进程照常起来,而每一台本机 Hub DS 永远
        # 停在 staged,玩家登录后一直等不到大厅(§9.19「无人驱动的静默等待」)。
        if self.mode == MODE_LOCAL:
            self._validate_local_off_v1_profile(guard_mode)

        # ⑩ Mock 是拓扑-only 不实现 HubFleetScaler:yaml 开了扩缩容 / 强制整合也不会
        # 运行。这不是错误(dev 默认就走 Mock),但必须说出来 —— 否则运维会以为
        # 自动扩容已经在跑,直到某天大厅满员也没扩出第二个实例。
        if self.mode != MODE_AGONES and (
            self.hub.autoscale_enabled or self.hub.consolidation_enabled
        ):
            warnings.append(
                f"autoscale_inert_under_mock: mode={self.mode} 无真实 Fleet scaler,"
                f"autoscale_enabled={self.hub.autoscale_enabled} / "
                f"consolidation_enabled={self.hub.consolidation_enabled} 不会运行,"
                f"需 mode=agones"
            )

        # ⑪ 安全档位配错必须炸,不能静默退化(见 resolve_writer_lease_mode)。
        self.hub.resolve_writer_lease_mode()

        # 强制整合依赖 kafka 推迁移通知。brokers 为空时只剩 Hub DS drain 心跳兜底,
        # 玩家不会看到无缝倒计时 —— 属"功能降级但不致命",按告警处理。
        if self.hub.consolidation_enabled and not self.kafka.configured():
            warnings.append(
                "consolidation_enabled=true 但 kafka.brokers 为空:"
                "迁移只剩 Hub DS drain 心跳兜底,玩家收不到无缝迁移倒计时"
            )

        return warnings

    def _first_secret_overlap(self) -> tuple[int, int] | None:
        """对应 Go 的 `auth.AssertDisjointSecrets`:返回第一对相等的下标。"""
        for i, p in enumerate(self.player_facing_keys()):
            for j, d in enumerate(self.ds_callback_keys()):
                if p == d:
                    return i, j
        return None

    def _validate_local_off_v1_profile(self, guard_mode: dsauth.Mode) -> None:
        """对应 Go 的 `auth.ValidateDSLocalHubProfileOffV1`。

        三条 profile 门 + 一条寿命门,缺一不可:
          - guard=off       :本机链路没有 Redis pending/ACK,permissive/enforce 起不来;
          - authority=legacy:Model B 的本地权威没有实现;
          - signer_ready    :即便 guard=off,secret 仍**必填** —— 它用来签发完整的
            Model-B tuple 经 env 下发给 UE,缺了 DS 拿不到凭据;
          - hub_token_ttl >= 12h:local-off-v1 没有 annotation 轮换,UE 到 exp 会主动
            清空 active,所以不能按 guard=off 跳过 —— 一次性凭据必须覆盖整段调试会话。
        """
        da = self.ds_auth
        signer_ready = da.signer_enabled()
        if (
            guard_mode is not dsauth.Mode.OFF
            or da.authority_mode != DEFAULT_DS_AUTH_AUTHORITY_MODE
            or not signer_ready
        ):
            raise ValueError(
                f"local_hub_auth_profile_invalid: {DS_LOCAL_PROFILE_OFF_V1} requires "
                f"guard=off authority=legacy signer_ready=true "
                f"(got guard={guard_mode.value!r} authority={da.authority_mode!r} "
                f"signer_ready={signer_ready})"
            )
        ttl = da.hub_token_ttl_td()
        if ttl < DS_LOCAL_HUB_MIN_TOKEN_TTL:
            raise ValueError(
                f"local_hub_auth_profile_invalid: {DS_LOCAL_PROFILE_OFF_V1} hub token "
                f"ttl={_duration_str(ttl)} is below local session minimum "
                f"{_duration_str(DS_LOCAL_HUB_MIN_TOKEN_TTL)}"
            )

    @classmethod
    def load(cls, path: str) -> "Config":
        """加载 + 填默认值。**不在这里跑 validate_conf** —— Go 侧
        config_scan_failed 与各条闸的事件名是分开的,main.py 按 Go 的顺序分别调用。
        """
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
