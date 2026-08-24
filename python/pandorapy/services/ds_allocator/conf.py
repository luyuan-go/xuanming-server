"""ds_allocator 服务配置 —— 对应 Go 侧 services/battle/ds_allocator/internal/conf/conf.go。

读的是**同一份** services/battle/ds_allocator/etc/ds_allocator-dev.yaml,不另建配置文件:
迁移期 Go 版和 Python 版并存,运维只该维护一份配置。

★ 默认值必须与 Go 的 `Defaults()` **逐个相同,判据符号也一样**。
  本服务的 `Defaults()` 几乎全部用 `== 0`,这不是笔误而是契约 —— 有六个字段用
  **负值表达「显式关闭整道闸」**,写成 `<= 0` 会把「关」兜回「开」而两边都不报错:

    | 字段 | 负值语义 | 写成 `<= 0` 的后果 |
    |---|---|---|
    | empty_battle_timeout      | 禁用空场兜底回收   | 以为关了空场回收,实际 5m 照杀 |
    | no_show_battle_timeout    | 禁用 no-show 差异化 | 以为退回单阈值,实际 150s 照杀 |
    | roster_join_deadline      | 关闭到齐期限整道闸 | 以为关了,实际 45s 照判弃 |
    | no_show_ledger_window     | 关闭 no-show 记罚   | 以为不记罚,实际照样退避 |
    | no_show_penalty_base      | 关闭 no-show 记罚   | 同上 |
    | no_show_penalty_free(int) | 0 次免罚(首次即罚) | 以为严格档,实际首次仍免罚 |

  **唯一的例外**是 `agones.capacity_warn_ratio`:Go 判的是 `<= 0 || > 1`(取值域
  (0,1] 之外一律回默认 0.8),照抄这一条,不要顺手统一成 `== 0`。

★ 「未在 Defaults() 里填默认」也是契约的一部分。
  `no_show_battle_timeout` / `roster_join_deadline` / `orphan_gs_reclaim_after` 三项
  **刻意不在 Defaults() 里赋值**,而是由读取点的 `resolve_*()` 现算 —— 因为它们的
  「0 = 用默认 / 负 = 关闭 / 正 = 钳制」三态必须在**原始值**上判定。一旦在 Defaults()
  里把 0 兜成默认值,`resolve` 就再也分不清「没配」和「配了默认值」,钳制链直接失效。
  移植时保留同样的分工:模型里存原始串,判定只在 resolve 里做。

★ 环境变量覆盖(local 调试路径)必须一起搬:
  `PANDORA_DS_LAUNCHER` / `PANDORA_DS_UPROJECT` / `PANDORA_DS_EXE` / `PANDORA_DS_DIR` /
  `PANDORA_DS_ADVERTISE_HOST`。一键脚本 play.ps1 靠它们免改 yaml 切换形态与探测出包目录;
  漏搬的表现是「同一台机器 Go 版能拉起 DS、Python 版报可执行文件不存在」。

★ kafka / session_gate 两段在 Go 属 `pkg/config.Base`,Python 侧共享的
  `pandorapy.config.BaseConf` 还没有它们(与 matchmaker / battle_result 同处境)。
  这里在服务私有 Config 上补建模,是为了让**配了就会改变正确性**的段不落进
  `model_extra` 被静默忽略:`kafka.brokers` 非空与否直接决定
  `validate_lifecycle_publication_config()` 放不放行 —— 落进 extra 会让一份缺 broker 的
  生产配置在 Go 上拒启、在 Python 上放行,同一份 yaml 两栈结论相反。

★ Validate 在 Go 侧不是一个 `Validate()`,而是 main.go 顺序调用的六个独立函数,
  **每个对应一个独立事件名**(排障入口)。这里逐个照搬并在 docstring 里标注事件名:

    ① cfg.DSAuth.ValidateRedisFence()          → ds_auth_fence_config_invalid
    ② cfg.ValidateLifecyclePublicationConfig() → ds_lifecycle_config_invalid
    ③ cfg.ValidateBattleDepartureConfig()      → battle_departure_config_invalid
    ④ cfg.ValidateAllocationAbortAuthConfig()  → allocation_abort_auth_config_invalid
    ⑤ cfg.ValidateLocalMapSourceConfig()       → local_map_source_config_invalid
    ⑥ cfg.ValidateRosterJoinDeadlineConfig()   → roster_join_deadline_config_invalid

  另有两条晚于装配的 TTL 闸(Go 在 main.go 拿到 signer/guard 之后才判),同样搬了过来:
  `DSAuthConf.validate_ttls()` → ds_auth_ttl_invalid、
  `Config.validate_battle_token_ttl_vs_battle_ttl()` → ds_auth_battle_token_ttl_too_small_vs_battle_ttl。

★ 本模块**没有**搬 main.go 的两条运行期档位闸(它们依赖装配结果,不是纯配置):
  - Model-B 激活闸:authority_mode=redis 必须同时 mode=agones + ds_auth.mode=enforce + 有签发密钥
    (事件 battle_model_b_invalid_activation);
  - mode=local 的 local-off-v1 隔离闸(事件 local_battle_auth_profile_invalid)。
  写 main.py 的人必须自己接上,见交付报告。
"""

from __future__ import annotations

import datetime as _dt
import os
import pathlib
import re
from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig
from pandorapy import fence_timeline, internalrpcauth, placement
from pandorapy.services.ds_allocator import orphan_reclaim

# ── DS 启动后端模式(Go: ModeLocal / ModeAgones / ModeMock)────────────────────
MODE_LOCAL = "local"
MODE_AGONES = "agones"
MODE_MOCK = "mock"

# ── 本机 DS 的两种进程形态(Go: LauncherPackaged / LauncherEditor)─────────────
LAUNCHER_PACKAGED = "packaged"
LAUNCHER_EDITOR = "editor"

# launcher=editor 专用的引擎 CVar 覆盖参数(Go: EditorLauncherCVarArg)。
#
# 为什么它是**配置层常量**而不是 allocator 里的一句字面量:未 cook 的 editor DS 与 PIE
# 客户端对 World Partition cell 的命名不同,服务端找不到 package 时默认**直接关连接**
# (MissingLevelPackage),客户端拿不到原因只会当瞬态掉线去重连 → 权威把它送回同一台 DS
# → 再被踢,形成秒级无限重连循环(2026-08-18 实测)。抄错一个字母 = 这条循环回来了。
# 刻意只作用于 editor:packaged / k8s 跑的是 cook 过的内容,那里的 Mismatch 是**真的**
# 内容不一致,必须保留引擎的踢人保护。
EDITOR_LAUNCHER_CVAR_ARG = "-DPCVars=net.SkipMissingLevelDisconnect=1"

# ── 写者继任租约档位(Go: WriterLeaseEnforce / Warmup / Off)───────────────────
WRITER_LEASE_ENFORCE = "enforce"
WRITER_LEASE_WARMUP = "warmup"
WRITER_LEASE_OFF = "off"

# ── 到齐期限激活档(Go: RosterJoinModeOff / Observe / Enforce)─────────────────
ROSTER_JOIN_MODE_OFF = "off"
ROSTER_JOIN_MODE_OBSERVE = "observe"
ROSTER_JOIN_MODE_ENFORCE = "enforce"

# ── Defaults() 的默认值,与 Go 逐个同值 ───────────────────────────────────────
# 抽成常量而不是内联字面量,是为了让 tests/test_ds_allocator_conf.py 能直接对着
# Go 源码断言(漂移当场变红),也让 main.py 打启动日志时不必再抄一遍数字。

DEFAULT_GRPC_ADDR = ":20020"
DEFAULT_HTTP_ADDR = ":21020"

# 心跳超时 = §9 不变量 4 的「Battle DS 15s 超时 → abandoned → 段位回滚」。
# ★ 引用 fence_timeline 而不是写 15:那个模块是整条 fence 时间线的唯一校验入口
#   (它断言 Battle 超时 < Hub 超时)。这里抄一份数字的话,改坏 fence_timeline 时
#   本服务的默认值不会跟着变,而两处都不报错。
DEFAULT_HEARTBEAT_TIMEOUT = _dt.timedelta(
    seconds=fence_timeline.BATTLE_HEARTBEAT_TIMEOUT_SEC
)

# 首次激活(staged→ACTIVE)所需的最少**实收**业务心跳次数与最小首尾跨度。
# 推导(INC-20260727-001 第三 P0):DS 在 PostLoadMapWithWorld 回调里发的首拍只能证明
# 「回调这一刻游戏线程活着」—— 实测 Artic01 冷加载 48s 后首拍即激活并放行 ds_addr,
# 回调后游戏线程继续阻塞 17s 被 ACTIVE 15s 阈值判弃,客户端连上一个不回包的 DS。
# 跨 ≥2 个完整心跳周期(5s)的 ≥3 次实收心跳才证明游戏线程在持续 pump TimerManager。
DEFAULT_ACTIVATION_STABILITY_BEATS = 3
DEFAULT_ACTIVATION_STABILITY_SPAN = _dt.timedelta(seconds=10)

DEFAULT_SWEEP_INTERVAL = _dt.timedelta(seconds=5)
DEFAULT_BATTLE_TTL = _dt.timedelta(hours=2)
DEFAULT_READY_WAIT_TIMEOUT = _dt.timedelta(seconds=10)
DEFAULT_EMPTY_BATTLE_TIMEOUT = _dt.timedelta(minutes=5)

# editor 形态(UnrealEditor.exe -server)的放宽档。**只在对应字段留空时**生效,
# 显式配置永远优先(Go 注释与代码在这点上一致,有 Go 测试钉死)。
EDITOR_HEARTBEAT_TIMEOUT = _dt.timedelta(seconds=120)
EDITOR_READY_WAIT_TIMEOUT = _dt.timedelta(seconds=300)

DEFAULT_NO_SHOW_LEDGER_WINDOW = _dt.timedelta(minutes=10)
DEFAULT_NO_SHOW_PENALTY_BASE = _dt.timedelta(seconds=30)
DEFAULT_NO_SHOW_PENALTY_CAP = _dt.timedelta(minutes=5)
DEFAULT_NO_SHOW_PENALTY_FREE = 1

DEFAULT_MOCK_DS_ADDR_HOST = "127.0.0.1"
DEFAULT_MOCK_DS_PORT_BASE = 30000
DEFAULT_MOCK_DS_PORT_RANGE = 1000

DEFAULT_AGONES_API_SERVER = "https://kubernetes.default.svc"
DEFAULT_AGONES_NAMESPACE = "default"
DEFAULT_AGONES_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
DEFAULT_AGONES_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
DEFAULT_AGONES_ALLOCATE_TIMEOUT = _dt.timedelta(seconds=5)
DEFAULT_CAPACITY_WATCH_INTERVAL = _dt.timedelta(seconds=30)
DEFAULT_CAPACITY_WARN_RATIO = 0.8

DEFAULT_LOCAL_ADVERTISE_HOST = "127.0.0.1"
DEFAULT_LOCAL_PORT_BASE = 7777
DEFAULT_LOCAL_PORT_RANGE = 100
DEFAULT_LOCAL_LOG_DIR = "run/dev/logs/ds"

# ── 空场回收 / 到齐期限的机械护栏(Go 侧同名常量)─────────────────────────────

# 「从未连入」局的默认回收阈值。推导:DSTicket v2 生产档 TTL 120s(pkg/auth/dsticket.go)
# + 30s 时钟/网络余量。票据是进场权威的唯一搬运通道(§9.3),过期后客户端物理上不可能
# 再凭它连入 ⇒ 150s 后「没人连入」已不可逆,回收是安全的,不是拍脑袋的经验值。
# ⚠️ Python 侧目前**没有** DSTicket TTL 的公共常量可引用(见交付报告「未完成项」),
#    所以这里是唯一一处手写推导数;改动前请同步核对 pkg/auth/dsticket.go。
DEFAULT_NO_SHOW_BATTLE_TIMEOUT = _dt.timedelta(seconds=150)

# no-show 阈值下限护栏。配得比这更短会开始误杀「正在加载地图 / 正在 travel」的正常
# 玩家,把防刷改动变成「玩家进不去场景」(§9.20 红线)。手滑配 1s 必须被钳住。
NO_SHOW_TIMEOUT_FLOOR = _dt.timedelta(seconds=60)

# 花名册到齐期限默认值(用户拍板 45s)。**这不是安全上界而是取舍**:严格安全上界是
# DSTicket TTL 120s + 余量 = 150s(那之后缺席者物理上再也进不来),45s 用「让在场玩家
# 少干等」换掉了这段安全余量,上线前必须实测「DS ready → 最后一个 Join succeeded」P99。
DEFAULT_ROSTER_JOIN_DEADLINE = _dt.timedelta(seconds=45)

# 到齐期限下限护栏。刻意**不**复用 NO_SHOW_TIMEOUT_FLOOR(60s):那条守的是「全场没人」,
# 允许等更久;本闸守的是「差几个人」,在场玩家在干等,两者时间尺度本就不同。
ROSTER_JOIN_DEADLINE_FLOOR = _dt.timedelta(seconds=30)

# 仅供 resolve_roster_join_arm_window() 推导时间窗用的 ready_wait 兜底
# (实际等待逻辑另有 editor/packaged 分档,见 EDITOR_READY_WAIT_TIMEOUT)。
ARM_WINDOW_DEFAULT_READY_WAIT = _dt.timedelta(seconds=120)

# 到齐期限武装窗的余量,取一个心跳周期量级:时间窗判定发生在心跳里,少了余量会在
# 边界上抖动(同一局忽而可武装忽而不可)。
ROSTER_JOIN_ARM_SLACK = _dt.timedelta(seconds=30)

# ── 孤儿 GameServer 回收阈值:**引用**已移植的 orphan_reclaim,不重抄数字 ──────
# 那个模块是回收判定链的实现处,阈值改了这里必须跟着改;抄一份副本会让
# 「conf 说 10m、实际按另一个数回收」这种漂移零信号。
DEFAULT_ORPHAN_GS_RECLAIM_AFTER = _dt.timedelta(
    seconds=orphan_reclaim.DEFAULT_RECLAIM_AFTER_SEC
)
ORPHAN_GS_RECLAIM_AFTER_FLOOR = _dt.timedelta(
    seconds=orphan_reclaim.RECLAIM_AFTER_FLOOR_SEC
)

# 战斗令牌不续期(一局一签、DS 一局一销毁),TTL 必须覆盖「战斗镜像 TTL + 重连/ready 余量」。
# Go 在 main.go 里以 const battleReconnectMargin 写死 15m。
BATTLE_TOKEN_RECONNECT_MARGIN = _dt.timedelta(minutes=15)

# DS 回调令牌 TTL 的启动期最小值(Go: dsAuthMinBattleTokenTTL / dsAuthMinHubTokenTTL)。
# 这两把令牌在关键路径上都有「不续期」窗口,低于下限属明显误配 → 启动即拒,
# 而不是等线上 DS 回调被莫名全拒才排查。
DS_AUTH_MIN_BATTLE_TOKEN_TTL = _dt.timedelta(hours=1)
DS_AUTH_MIN_HUB_TOKEN_TTL = _dt.timedelta(hours=1)

# DSAuthConf.Defaults() 的默认值。
DEFAULT_DS_AUTH_AUTHORITY_MODE = "legacy"
DEFAULT_DS_AUTH_ISSUER = "pandora-ds-control"
DEFAULT_DS_AUTH_AUDIENCE = "pandora-ds"
DEFAULT_DS_AUTH_BATTLE_TOKEN_TTL = _dt.timedelta(hours=4)
DEFAULT_DS_AUTH_HUB_TOKEN_TTL = _dt.timedelta(hours=24)
DEFAULT_DS_AUTH_ACTIVE_HEARTBEAT_MAX_AGE = _dt.timedelta(seconds=30)

# Go 的 os.ExpandEnv 只认 `${VAR}` / `$VAR` 两种形状,且**未定义的变量替换成空串**。
# Python 的 os.path.expandvars 在未定义时保留原文,两者会分叉,所以这里自己实现。
# 分叉的实际后果:`${PANDORA_DS_ROOT}/Packages/...` 在变量未设时,Go 得到
# `/Packages/...`(相对/绝对路径不存在 → 走 PANDORA_DS_EXE 兜底),Python 得到带
# `${...}` 字面量的串 —— 虽然最终也走兜底,但两栈打进日志的路径不同,排障时对不上。
_ENV_REF_RE = re.compile(r"\A\$(?:\{(?P<braced>[^}]*)\}|(?P<bare>[A-Za-z_][A-Za-z0-9_]*))")


def _local_ds_executable_mismatched(path: str, launcher: str) -> bool:
    """两种形态的 exe 被互换了没有 —— 只认这一种确定错配。

    editor 形态拿到出包的 PandoraServer*,或 packaged 形态拿到引擎的 UnrealEditor*。
    刻意不做白名单(不要求「必须叫 UnrealEditor」):策划机可能用 UnrealEditor-Cmd.exe、
    包装脚本,测试用 stub.exe,这些都是合法的;拦已知错配比放行未知形态更不容易误伤。
    与 Go 侧 services/battle/ds_allocator/internal/conf/conf.go 的同名判据逐条一致。
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
    launcher 在本文件里已于 apply_defaults 前段归一,这里直接用。
    """
    if path == "":
        return False
    if not os.path.exists(path):
        return False
    return not _local_ds_executable_mismatched(path, launcher)


def expand_env_go(value: str) -> str:
    """等价于 Go 的 `os.ExpandEnv`:未定义变量替换成空串。"""
    if "$" not in value:
        return value
    out: list[str] = []
    i = 0
    while i < len(value):
        if value[i] != "$":
            out.append(value[i])
            i += 1
            continue
        m = _ENV_REF_RE.match(value, i)
        if not m:
            # 孤立的 `$` 或 `$1` 之类:Go 的 getShellName 在这里返回空名字并原样保留 `$`。
            out.append("$")
            i += 1
            continue
        name = m.group("braced") if m.group("braced") is not None else m.group("bare")
        out.append(os.environ.get(name, ""))
        i = m.end()
    return "".join(out)


def from_slash(value: str) -> str:
    """等价于 Go 的 `filepath.FromSlash`:把 `/` 换成本平台分隔符。

    Windows 上策划可以在 yaml 里写正斜杠(免去 `\\\\` 转义),Linux 上是恒等变换 ——
    与 Go 同 OS 运行时结果逐字一致。
    """
    if os.sep == "/":
        return value
    return value.replace("/", os.sep)


def _duration_str(value: _dt.timedelta) -> str:
    """把 timedelta 兜底值写回成 Go duration 串(默认值只会是整秒/整分/整时)。

    为什么把默认值写回**字符串字段**而不是另存解析后的字段:conf 是「同一份 yaml 两个
    实现」的契约面,保持字段类型与 yaml 一致才能让 `cfg.model_dump()` 原样回吐一份合法
    yaml —— 排障时可以直接对比两栈的**生效**配置,而不是各自的内部表示。
    """
    total = int(value.total_seconds())
    if total and total % 3600 == 0:
        return f"{total // 3600}h"
    if total and total % 60 == 0:
        return f"{total // 60}m"
    return f"{total}s"


class KafkaConf(BaseModel):
    """对应 Go 的 `pkg/config.KafkaConfig`。

    ds_allocator 只做**生产方**:心跳超时判 abandoned 后发 `pandora.ds.lifecycle`,
    由 battle_result 做 match release + terminal placement 补偿。
    这一段配不配决定了生产授权模式能不能启动(见 validate_lifecycle_publication_config)。
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
    # ★ Go 的真实默认是 **False**(结构体零值),不是 pkg/config/config.go:239 注释里
    # 写的「默认 true」—— 全仓没有任何代码给它填值,producer.go:85 直穿该字段。
    # 与 matchmaker / battle_result 的处理保持一致,以**代码**为准。
    idempotent: bool = False
    max_open_requests: int = 0
    retention_ms: int = 0

    def dial_timeout_ms(self) -> int:
        """拨号超时(毫秒)。kafkax.ProducerConf 收毫秒,yaml 里是 Go duration 串。"""
        return int(pconfig.parse_duration(self.dial_timeout).total_seconds() * 1000)

    def configured(self) -> bool:
        """对应 Go 的 `ValidateLifecyclePublicationConfig` 里那段 TrimSpace 判定。

        判据是「有没有**非空白**的 broker」,不是 `len(brokers) > 0`:ConfigMap 渲染出
        `brokers: [""]` 时长度是 1 而实际一个 broker 都没有,按长度判会让生产授权模式
        带着一个永远发不出去的 producer 启动 —— abandoned 事件没有恢复出口,
        BattleResult 拿不到 match release / battle exit proof。
        """
        return any((b or "").strip() for b in self.brokers)


class SessionGateConf(BaseModel):
    """对应 Go 的 `pkg/config.SessionGateConf`。

    ⚠️ ds_allocator **不读这一段**:它的入口是 DS 回调面(无玩家 JWT),会话现行性门
    对它没有意义。建模只为让 yaml 里若出现 session_gate(集群生成器会机械加)
    不落进 `model_extra` 被静默吃掉 —— 「配了却不生效」是本项目的高频缺陷形状。
    """

    model_config = {"extra": "allow"}

    require: bool = False
    require_ticket_sjti: bool = False


class DSAuthFenceConf(BaseModel):
    """对应 Go 的 `pkg/config.DSAuthFenceConf`(authority_mode=redis 的机械激活栅栏)。

    Python 侧**没有**实现 fence 的获取与失租退出,但必须建模:落进 model_extra 会让
    `validate_redis_fence()` 恒看不到 endpoints,于是一份缺 fence 的非法配置在 Go 上
    拒启、在 Python 上放行 —— 两栈对同一份 yaml 结论相反,正是要避免的事。
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
    """对应 Go 的 `pkg/config.DSAuthConf`。ds_allocator **双角色**都用它:

      - 签发:AllocateBattle 时给战斗 DS 签 battle 令牌(绑 match_id),经 GameServer
        annotation(agones)/ `PANDORA_DS_TOKEN` env(local)下发;secret 配了就签;
      - 校验:Heartbeat / GmService Poll·Ack 按 mode(off/permissive/enforce)验证令牌。
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
        """对应 Go 的 `AuthorityModeRedis()`。判据是**精确等于** "redis"。

        ★ 刻意不 trim / 不 lower:Go 就是 `c.AuthorityMode == "redis"`。
          写宽了会让 `authority_mode: "Redis"` 在这里判 true、在 Go 上判 false,
          于是 Model-B 的授权闸两栈开合相反。**这条与下面
          `requires_reliable_lifecycle_publication()` 的大小写不敏感判据不一致,
          是 Go 侧就存在的既有不一致**,见交付报告「Go 注释/代码矛盾」④。
        """
        return self.authority_mode == "redis"

    def apply_defaults(self) -> None:
        """对应 Go 的 `DSAuthConf.Defaults()`。

        ⚠️ `mode` / `secret` 留空即「不启用」,**刻意不填默认** —— 给它们填默认等于
        替运维打开(或关上)一道安全门,而 yaml 上看不出来。
        """
        if not self.authority_mode:
            self.authority_mode = DEFAULT_DS_AUTH_AUTHORITY_MODE
        if not self.issuer:
            self.issuer = DEFAULT_DS_AUTH_ISSUER
        if not self.audience:
            self.audience = DEFAULT_DS_AUTH_AUDIENCE
        if self.battle_token_ttl_td() == _dt.timedelta(0):
            self.battle_token_ttl = _duration_str(DEFAULT_DS_AUTH_BATTLE_TOKEN_TTL)
        if self.hub_token_ttl_td() == _dt.timedelta(0):
            self.hub_token_ttl = _duration_str(DEFAULT_DS_AUTH_HUB_TOKEN_TTL)
        if self.active_heartbeat_max_age_td() == _dt.timedelta(0):
            self.active_heartbeat_max_age = _duration_str(
                DEFAULT_DS_AUTH_ACTIVE_HEARTBEAT_MAX_AGE
            )

    def validate_redis_fence(self) -> None:
        """对应 Go 的 `ValidateRedisFence()`(main 事件名 `ds_auth_fence_config_invalid`)。

        不设这道闸会怎样:authority_mode=redis 而 fence 缺失时,失租 / 旧 epoch 的副本
        会继续当授权权威 —— Model-B 的「唯一授权权威」前提失效,两个副本同时给同一局
        签活跃凭据,而两边日志都正常。
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
            raise ValueError(
                "ds_auth: fence.etcd_dial_timeout must be positive or zero(default)"
            )
        if self.active_heartbeat_max_age_td().total_seconds() <= 0:
            raise ValueError(
                "ds_auth: authority_mode=redis requires positive active_heartbeat_max_age"
            )

    def validate_ttls(self, enabled: bool) -> None:
        """对应 Go 的 `DSAuthConf.Validate(enabled)`(事件名 `ds_auth_ttl_invalid`)。

        `enabled` 由调用方按「本服务是否签发/校验 DS 回调令牌」传入
        (Go: `dsSigner != nil || dsGuard != nil`)。

        不设这道闸会怎样:这两把令牌在关键路径上都有**不续期**窗口(战斗 DS 一局一签、
        Hub DS 在 local 模式一次性签发)。TTL 配小了不会在签发时报错,而是让对局跑到
        一半令牌过期、DS 回调被 enforce 守卫全拒 —— 赛果无法结算,现场只看到「回调 401」。
        """
        if not enabled:
            return
        if self.battle_token_ttl_td() < DS_AUTH_MIN_BATTLE_TOKEN_TTL:
            raise ValueError(
                f"ds_auth: battle_token_ttl={self.battle_token_ttl!r} too small "
                f"(need >= {DS_AUTH_MIN_BATTLE_TOKEN_TTL}; 战斗令牌不续期,须覆盖最长对局+重连窗口)"
            )
        if self.hub_token_ttl_td() < DS_AUTH_MIN_HUB_TOKEN_TTL:
            raise ValueError(
                f"ds_auth: hub_token_ttl={self.hub_token_ttl!r} too small "
                f"(need >= {DS_AUTH_MIN_HUB_TOKEN_TTL}; 大厅令牌 local 模式不续期,须覆盖一段常驻会话)"
            )


class AgonesMapFleet(BaseModel):
    """对应 Go 的 `AgonesMapFleet` —— map_id → 专属预热 Fleet 的一条路由。"""

    model_config = {"extra": "allow"}

    # 对齐 g_关卡.xlsx 的关卡 id。Go 是 uint32,负值在 Go 侧属 Scan 失败,
    # 这里用 ge=0 让它同样在**加载阶段**炸(main 归 config_scan_failed),
    # 而不是安静地变成一条永远匹配不上的路由。
    map_id: int = Field(default=0, ge=0)
    fleet_name: str = ""
    canary_fleet_name: str = ""


class AgonesConf(BaseModel):
    """对应 Go 的 `AgonesConf` —— 真 Agones GameServerAllocation 后端配置。"""

    model_config = {"extra": "allow"}

    enabled: bool = False
    api_server: str = ""
    namespace: str = ""
    fleet_name: str = ""
    canary_fleet_name: str = ""
    canary_percent: int = Field(default=0, ge=0)
    canary_seed: str = ""
    map_fleets: list[AgonesMapFleet] = Field(default_factory=list)
    advertise_host: str = ""
    token_path: str = ""
    ca_path: str = ""
    insecure_skip_tls_verify: bool = False
    allocate_timeout: str = ""
    # 负值 = 禁用巡检(Go 注释与 biz/capacity.go 的 `<= 0` 判定一致)。
    capacity_watch_interval: str = ""
    capacity_warn_ratio: float = 0.0

    def allocate_timeout_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.allocate_timeout)

    def capacity_watch_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.capacity_watch_interval)

    def dedicated_fleet_for(self, map_id: int) -> str:
        """对应 Go 的 `DedicatedFleetFor` —— 未配置返回空串(= 只走通用池)。"""
        if map_id == 0:
            return ""
        for mf in self.map_fleets:
            if mf.map_id == map_id and mf.fleet_name:
                return mf.fleet_name
        return ""

    def dedicated_fleet_for_track(self, map_id: int, release_track: str) -> str:
        """对应 Go 的 `DedicatedFleetForTrack`。

        ★ 判据与 `dedicated_fleet_for` **不同**,不要顺手合并:本函数在 map_id 命中的
        第一条上就 return(哪怕该条的 fleet_name / canary_fleet_name 是空串),
        而 `dedicated_fleet_for` 会跳过 fleet_name 为空的条目继续找。
        这是 Go 的既有行为,合并会让「同一 map 配了两条、第一条只填 canary」的配置
        在两栈上选出不同的 Fleet。
        """
        if map_id == 0:
            return ""
        for mf in self.map_fleets:
            if mf.map_id != map_id:
                continue
            if release_track == "canary":
                return mf.canary_fleet_name
            return mf.fleet_name
        return ""


class LocalDSConf(BaseModel):
    """对应 Go 的 `LocalDSConf` —— 本机 exec Windows DS 进程的调试后端。

    ⚠️ 2026-08-04 之前这里有一张 `maps` 手抄表(map_id → UE 关卡 URL),把关卡表的
    asset_path / game_mode_class 两列抄了第二份(违反 §9.22),已整块删除。**不要复活它**:
    抄漏一行的后果不是「回退默认图」而是「这张图永远进不去」—— DS 起了默认图 → DS 侧
    关卡门判「已加载世界 ≠ 注入 map_id」→ fail-closed 自杀 → 分配卡到超时,玩家侧只看到
    「一直排队中」(map_id=11 实测)。
    """

    model_config = {"extra": "allow"}

    enabled: bool = False
    launcher: str = ""
    executable_path: str = ""
    project_path: str = ""
    loader_map: str = ""
    advertise_host: str = ""
    port_base: int = 0
    port_range: int = 0
    working_dir: str = ""
    log_dir: str = ""
    extra_args: list[str] = Field(default_factory=list)
    extra_env: dict[str, str] = Field(default_factory=dict)

    def is_editor(self) -> bool:
        """launcher 归一化后是否为 editor(归一化在 `Config.apply_defaults` 里做)。"""
        return self.launcher == LAUNCHER_EDITOR


class AllocatorConf(BaseModel):
    """ds_allocator 私有配置段。字段名与 Go 的 yaml tag 逐字一致。"""

    model_config = {"extra": "allow"}

    # 只用于 Matchmaker 的 pre-admission abort RPC 的专用信任域;任何玩家 / placement /
    # resume / DS 回调链都不得拿到这把钥匙。
    allocation_abort_auth_secret: str = ""
    allocation_abort_auth_audience: str = ""

    heartbeat_timeout: str = ""
    activation_stability_beats: int = 0
    activation_stability_span: str = ""

    owner_addr: str = ""
    owner_lease_required: bool = False

    sweep_interval: str = ""
    writer_lease_mode: str = ""

    battle_ttl: str = ""
    ready_wait_timeout: str = ""
    empty_battle_timeout: str = ""
    no_show_battle_timeout: str = ""

    roster_join_deadline: str = ""
    roster_join_deadline_mode: str = ""
    # Go 是 uint64。ge=0 让负值在加载阶段就炸:负 generation 会让
    # roster_deadline_should_abandon 恒 false —— 正是 validate_roster_join_deadline_config
    # 要防的「写着 enforce、实际静默 no-op」失效形状,只是换了个入口。
    roster_policy_generation: int = Field(default=0, ge=0)

    no_show_ledger_window: str = ""
    no_show_penalty_base: str = ""
    no_show_penalty_cap: str = ""
    no_show_penalty_free: int = 0

    orphan_gs_reclaim_after: str = ""

    mock_ds_addr_host: str = ""
    mock_ds_port_base: int = 0
    mock_ds_port_range: int = 0

    # ── Duration 取值(与 Go 的 .Std() 同位置)────────────────────────────────
    def heartbeat_timeout_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.heartbeat_timeout)

    def activation_stability_span_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.activation_stability_span)

    def sweep_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.sweep_interval)

    def battle_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.battle_ttl)

    def ready_wait_timeout_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.ready_wait_timeout)

    def empty_battle_timeout_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.empty_battle_timeout)

    def no_show_battle_timeout_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.no_show_battle_timeout)

    def roster_join_deadline_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.roster_join_deadline)

    def no_show_ledger_window_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.no_show_ledger_window)

    def no_show_penalty_base_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.no_show_penalty_base)

    def no_show_penalty_cap_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.no_show_penalty_cap)

    # ── 档位归一化(非法值一律抛,不猜)────────────────────────────────────
    def resolve_writer_lease_mode(self) -> str:
        """对应 Go 的 `ResolveWriterLeaseMode()`(空 → enforce)。

        非法值必须 fail-fast:安全档位配错不允许静默退化成 off。退化成 off 的后果是
        多副本各跑一份心跳扫描 —— 同一局被两个副本并行判弃 / 并行回收。
        """
        text = (self.writer_lease_mode or "").strip().lower()
        if text in ("", WRITER_LEASE_ENFORCE):
            return WRITER_LEASE_ENFORCE
        if text in (WRITER_LEASE_WARMUP, WRITER_LEASE_OFF):
            return text
        raise ValueError(
            f"allocator.writer_lease_mode {self.writer_lease_mode!r} invalid "
            f"(want enforce|warmup|off)"
        )

    def resolve_roster_join_mode(self) -> str:
        """对应 Go 的 `ResolveRosterJoinMode()`(空 → observe)。

        未知档位报错不猜:拼错一个字母不该悄悄改变「判不判弃一场正在打的对局」。
        """
        text = (self.roster_join_deadline_mode or "").strip().lower()
        if text == "":
            return ROSTER_JOIN_MODE_OBSERVE
        if text in (
            ROSTER_JOIN_MODE_OFF,
            ROSTER_JOIN_MODE_OBSERVE,
            ROSTER_JOIN_MODE_ENFORCE,
        ):
            return text
        raise ValueError(
            f"allocator.roster_join_deadline_mode {self.roster_join_deadline_mode!r} "
            f"invalid (want off|observe|enforce)"
        )

    # ── 三态解析(0=默认 / 负=关闭 / 正=钳制)──────────────────────────────
    def resolve_no_show_timeout(self) -> _dt.timedelta:
        """对应 Go 的 `ResolveNoShowTimeout()` —— 「从未连入」局的实际回收阈值。

        配错的两个方向都危险:配太长 → 刷进出副本能用小号把整个 Fleet 押死
        (每次分配白押一台 14Gi Pod);配太短 → 正在 travel / 加载地图的正常玩家被判
        no-show,变成「玩家进不去场景」(§9.20 红线)。

        ★ 判据符号逐个照抄,尤其头一条是 `<= 0` 而不是 `== 0`:
          empty 本身被显式禁用(负)或未配(0)时**跟随它**,不自作主张开启回收。
        """
        empty = self.empty_battle_timeout_td()
        if empty <= _dt.timedelta(0):
            return empty
        no_show = self.no_show_battle_timeout_td()
        if no_show < _dt.timedelta(0):
            # 显式禁用差异化 → 退回单阈值(改动前行为)。
            return empty
        if no_show == _dt.timedelta(0):
            no_show = DEFAULT_NO_SHOW_BATTLE_TIMEOUT
        if no_show < NO_SHOW_TIMEOUT_FLOOR:
            no_show = NO_SHOW_TIMEOUT_FLOOR
        if no_show > empty:
            no_show = empty
        return no_show

    def resolve_roster_join_deadline(self) -> _dt.timedelta:
        """对应 Go 的 `ResolveRosterJoinDeadline()`。

        负 = 显式关闭整道闸(返回 0);0 = 默认 45s;正值钳到 [30s, +∞)。
        ★ 刻意**不**用 empty_battle_timeout 做上限(no-show 那条要):本闸判的是开局
          到齐,与「打到一半全员掉线」的回收窗没有可比性,拿后者钳前者只会把语义搅在一起。
        """
        d = self.roster_join_deadline_td()
        if d < _dt.timedelta(0):
            return _dt.timedelta(0)
        if d == _dt.timedelta(0):
            d = DEFAULT_ROSTER_JOIN_DEADLINE
        if d < ROSTER_JOIN_DEADLINE_FLOOR:
            d = ROSTER_JOIN_DEADLINE_FLOOR
        return d

    def resolve_roster_join_arm_window(self) -> _dt.timedelta:
        """对应 Go 的 `ResolveRosterJoinArmWindow()` —— 到齐期限还允许被武装的时间窗。

        自 allocated_at_ms 起算,超窗后任何副本都**永不**再武装本闸。这是给滚动升级用的
        纵深防御:一局在旧副本手里已全员到齐过(没人写 roster_ever_complete)→ 打到一半
        有人掉线 → 心跳被新副本接手 → 新副本看到 census 缺人且标记为假 → deadline 到 →
        **判弃一场正在打的对局**,比本闸要防的缺员局严重得多。

        ★ ready_wait 的兜底判据是 `<= 0`(不是 `== 0`),且用的是 120s 这个**推导专用**
          默认值,不是 Defaults() 填的 10s —— 两者刻意不同,别顺手统一。
        """
        deadline = self.resolve_roster_join_deadline()
        if deadline <= _dt.timedelta(0):
            return _dt.timedelta(0)  # 闸本身关着
        ready = self.ready_wait_timeout_td()
        if ready <= _dt.timedelta(0):
            ready = ARM_WINDOW_DEFAULT_READY_WAIT
        return ready + deadline + ROSTER_JOIN_ARM_SLACK

    def resolve_orphan_gs_reclaim_after(self) -> _dt.timedelta:
        """对应 Go 的 `AllocatorUsecase.orphanGSReclaimAfter()`(在 biz,不在 conf)。

        0 / 负值 = 用默认 10m;正值钳到 [5m, +∞)。下限不是洁癖:阈值要覆盖的不只是
        DSTicket 硬上限 180s + ready_wait 120s,还有跨多轮对账 + 控制面观察余量。
        谁在 yaml 里填了 30s(手滑、照抄别的 sweep 项),没有钳制就会按 30s 回收 ——
        一台刚分配、玩家正拿着有效票据在进的 DS 就可能在进场途中被删。
        """
        d = pconfig.parse_duration(self.orphan_gs_reclaim_after)
        if d <= _dt.timedelta(0):
            return DEFAULT_ORPHAN_GS_RECLAIM_AFTER
        if d < ORPHAN_GS_RECLAIM_AFTER_FLOOR:
            return ORPHAN_GS_RECLAIM_AFTER_FLOOR
        return d

    def no_show_penalty_enabled(self) -> bool:
        """no-show 记罚整体是否生效(Go 在 biz/allocator.go recordNoShowPenalties 里判)。

        判据是 `window <= 0 或 base <= 0`(注意是 `<=` 不是 `==`)—— 两者任一被配成
        负值都表示「关掉整个记罚」。记罚是**背压**不是正确性:关掉只是让占位刷子更从容,
        判弃回收本身照常跑。
        """
        return (
            self.no_show_ledger_window_td() > _dt.timedelta(0)
            and self.no_show_penalty_base_td() > _dt.timedelta(0)
        )

    def resolve_no_show_penalty_free(self) -> int:
        """免罚次数的**消费端**钳制(Go 同样在 biz 里做,不在 Defaults)。

        负值 = 严格档「首次即罚」,在这里钳成 0。放在 conf 只是为了让移植 biz 的人
        不必自己重新发现这条:`Defaults()` 刻意保留负值(判据 `== 0`),若消费端忘了钳,
        `count - free` 会因负 free 变大 —— 严格档反而**加重**处罚(首次就按第 2 档罚),
        与注释里写的语义相反。
        """
        return max(self.no_show_penalty_free, 0)

    def activation_stability_gate_enabled(self) -> bool:
        """激活稳定性门是否生效(对应 Go 的 `battleActivationStabilityPending` 首行)。

        `beats <= 1 且 span <= 0` 时整道门关闭(仅供测试 / 回退)。判据是**与**不是**或**:
        写成 `or` 会让「beats=3 + span=0」这种只想按拍数判的配置被整道关掉,
        DS 首拍即激活并放行 ds_addr —— 正是 INC-20260727-001 第三 P0 的形状。
        """
        return not (
            self.activation_stability_beats <= 1
            and self.activation_stability_span_td() <= _dt.timedelta(0)
        )


def roster_deadline_should_abandon(mode: str, cfg_gen: int, battle_gen: int) -> bool:
    """对应 Go 的 `RosterDeadlineShouldAbandon` —— 「这一局到点了能不能真判弃」唯一判定。

    biz 与 data 两条路径共用,防两处漂移:
      - mode 必须 enforce(observe 只采证、off 整道关);
      - cfg_gen 必须非 0(enforce+0 在 validate 就被拒,这里再兜一层);
      - battle_gen 必须**等于** cfg_gen —— legacy(0)与旧代局永不执行,
        这是滚动升级不误判弃正在打的局的**机制**保证。写成 `>=` 会让旧代局重新入选。
    """
    return mode == ROSTER_JOIN_MODE_ENFORCE and cfg_gen != 0 and battle_gen == cfg_gen


class Config(pconfig.BaseConf):
    """ds_allocator 服务的完整配置。对应 Go 的 `conf.Config`。"""

    # 留空时按 legacy 的 agones.enabled / local_ds.enabled 推导(向后兼容旧配置)。
    mode: str = ""

    allocator: AllocatorConf = Field(default_factory=AllocatorConf)
    agones: AgonesConf = Field(default_factory=AgonesConf)
    local_ds: LocalDSConf = Field(default_factory=LocalDSConf)

    # player_locator gRPC 地址。留空 = 不续期 BATTLE presence(弱依赖),但**不改变**
    # 无 TTL 的权威 placement,也绝不能据此把玩家降级路由回 Hub(§9.22:key miss
    # 只说明 presence 不可见,不证明玩家已离开旧 DS)。
    locator_addr: str = ""

    ds_auth: DSAuthConf = Field(default_factory=DSAuthConf)
    kafka: KafkaConf = Field(default_factory=KafkaConf)
    session_gate: SessionGateConf = Field(default_factory=SessionGateConf)

    # ── Defaults ───────────────────────────────────────────────────────────
    def apply_defaults(self) -> None:  # noqa: C901 —— 与 Go 同为一条线性赋值链
        """填默认值 —— 对应 Go 的 `Defaults()`,**顺序也照抄**。

        ★ 顺序在这里是语义的一部分,不是风格:
          ① mode 必须先归一化,② launcher 必须先归一化,③ 之后才轮到 allocator 的超时
          默认值 —— 因为 editor 形态启动更慢,超时要按它放宽。把 ③ 提前会让 editor 的
          放宽档永远选不上,表现是「切到 editor 后每局必 ready 超时,PVE 恒 FAILED」。
        """
        # ① Mode 归一化:显式 mode 优先;留空时按 legacy 的 enabled 开关推导。
        self.mode = (self.mode or "").strip().lower()
        if self.mode == "":
            if self.agones.enabled:
                self.mode = MODE_AGONES
            elif self.local_ds.enabled:
                self.mode = MODE_LOCAL
            else:
                self.mode = MODE_MOCK

        # ② Launcher 两模式开关:env 优先于 yaml;缺省/非法值一律归一到 packaged
        #    (现状行为),旧配置零改动。
        env_launcher = (os.environ.get("PANDORA_DS_LAUNCHER") or "").strip()
        if env_launcher:
            self.local_ds.launcher = env_launcher
        self.local_ds.launcher = (self.local_ds.launcher or "").strip().lower()
        if self.local_ds.launcher != LAUNCHER_EDITOR:
            self.local_ds.launcher = LAUNCHER_PACKAGED

        # project_path:展开环境变量 + 归一化分隔符;本机不存在时回退 env 注入值。
        # ★ 判据是「路径为空 **或** stat 失败」,不是「env 非空就覆盖」——
        #   dev 机上 yaml 写死的路径存在时不能被脚本注入值顶掉。
        self.local_ds.project_path = from_slash(expand_env_go(self.local_ds.project_path))
        env_proj = (os.environ.get("PANDORA_DS_UPROJECT") or "").strip()
        if env_proj and (
            self.local_ds.project_path == ""
            or not os.path.exists(self.local_ds.project_path)
        ):
            self.local_ds.project_path = from_slash(env_proj)

        # ③ editor 形态放宽:仅在「用户没显式配置(== 0)」且 launcher=editor 时生效,
        #    显式配置永远优先。editor DS 要加载一大批编辑器模块、读未 cook 的散装资产,
        #    首次进新图还可能现场构 DDC,沿用 packaged 的 10s/15s 会在 DS 还没起来时
        #    就判 ready 超时回收 —— editor 模式永远开不了局。
        a = self.allocator
        editor_local = self.mode == MODE_LOCAL and self.local_ds.launcher == LAUNCHER_EDITOR
        if a.heartbeat_timeout_td() == _dt.timedelta(0) and editor_local:
            a.heartbeat_timeout = _duration_str(EDITOR_HEARTBEAT_TIMEOUT)
        if a.ready_wait_timeout_td() == _dt.timedelta(0) and editor_local:
            a.ready_wait_timeout = _duration_str(EDITOR_READY_WAIT_TIMEOUT)

        if a.heartbeat_timeout_td() == _dt.timedelta(0):
            a.heartbeat_timeout = _duration_str(DEFAULT_HEARTBEAT_TIMEOUT)
        if a.activation_stability_beats == 0:
            a.activation_stability_beats = DEFAULT_ACTIVATION_STABILITY_BEATS
        if a.activation_stability_span_td() == _dt.timedelta(0):
            a.activation_stability_span = _duration_str(DEFAULT_ACTIVATION_STABILITY_SPAN)
        if a.sweep_interval_td() == _dt.timedelta(0):
            a.sweep_interval = _duration_str(DEFAULT_SWEEP_INTERVAL)
        if a.battle_ttl_td() == _dt.timedelta(0):
            a.battle_ttl = _duration_str(DEFAULT_BATTLE_TTL)
        if a.ready_wait_timeout_td() == _dt.timedelta(0):
            a.ready_wait_timeout = _duration_str(DEFAULT_READY_WAIT_TIMEOUT)
        if a.empty_battle_timeout_td() == _dt.timedelta(0):
            a.empty_battle_timeout = _duration_str(DEFAULT_EMPTY_BATTLE_TIMEOUT)
        # ⚠️ no_show_battle_timeout / roster_join_deadline / orphan_gs_reclaim_after
        #    **刻意不在这里填默认**(见模块 docstring):它们的三态判定必须在原始值上做。
        if a.no_show_ledger_window_td() == _dt.timedelta(0):
            a.no_show_ledger_window = _duration_str(DEFAULT_NO_SHOW_LEDGER_WINDOW)
        if a.no_show_penalty_base_td() == _dt.timedelta(0):
            a.no_show_penalty_base = _duration_str(DEFAULT_NO_SHOW_PENALTY_BASE)
        if a.no_show_penalty_cap_td() == _dt.timedelta(0):
            a.no_show_penalty_cap = _duration_str(DEFAULT_NO_SHOW_PENALTY_CAP)
        # 负值 = 0 次免罚(首次即罚)的严格档,必须保留原值 —— 判据只能是 `== 0`。
        if a.no_show_penalty_free == 0:
            a.no_show_penalty_free = DEFAULT_NO_SHOW_PENALTY_FREE
        if a.mock_ds_addr_host == "":
            a.mock_ds_addr_host = DEFAULT_MOCK_DS_ADDR_HOST
        if a.mock_ds_port_base == 0:
            a.mock_ds_port_base = DEFAULT_MOCK_DS_PORT_BASE
        if a.mock_ds_port_range == 0:
            a.mock_ds_port_range = DEFAULT_MOCK_DS_PORT_RANGE

        ag = self.agones
        if ag.api_server == "":
            ag.api_server = DEFAULT_AGONES_API_SERVER
        if ag.namespace == "":
            ag.namespace = DEFAULT_AGONES_NAMESPACE
        if ag.token_path == "":
            ag.token_path = DEFAULT_AGONES_TOKEN_PATH
        if ag.ca_path == "":
            ag.ca_path = DEFAULT_AGONES_CA_PATH
        if ag.allocate_timeout_td() == _dt.timedelta(0):
            ag.allocate_timeout = _duration_str(DEFAULT_AGONES_ALLOCATE_TIMEOUT)
        if ag.capacity_watch_interval_td() == _dt.timedelta(0):
            ag.capacity_watch_interval = _duration_str(DEFAULT_CAPACITY_WATCH_INTERVAL)
        # ★ 全文件唯一一处**不是** `== 0` 的判据,与 Go 逐字一致:取值域是 (0,1],
        #   域外(含负数与 >1)一律回默认 0.8。写成 `== 0` 会让 `capacity_warn_ratio: 1.5`
        #   原样生效 —— 那个比例永远达不到,容量预警整道静默失效,Fleet 打满时没有任何告警。
        if ag.capacity_warn_ratio <= 0 or ag.capacity_warn_ratio > 1:
            ag.capacity_warn_ratio = DEFAULT_CAPACITY_WARN_RATIO

        self.ds_auth.apply_defaults()

        # local_ds 路径:①展开 ${VAR}/$VAR;②归一化分隔符(策划写正斜杠也能用);
        # ③展开后在本机不存在时,回退到启动脚本按平级 Client 目录探测注入的
        #   PANDORA_DS_EXE / PANDORA_DS_DIR(play.ps1 自动填充)。
        ld = self.local_ds
        ld.executable_path = from_slash(expand_env_go(ld.executable_path))
        ld.working_dir = from_slash(expand_env_go(ld.working_dir))
        env_exe = os.environ.get("PANDORA_DS_EXE") or ""
        if env_exe and not _local_ds_executable_usable(ld.executable_path, ld.launcher):
            ld.executable_path = from_slash(env_exe)
            # ★ working_dir 只在**确实用了** env 可执行文件时才跟着换。
            #   提到 if 外面会让 yaml 里写死的 exe + 脚本注入的 dir 配成一对,
            #   DS 起在错误的 staged 根目录下(找不到 pak / 日志落到别处)。
            env_dir = os.environ.get("PANDORA_DS_DIR") or ""
            if env_dir:
                ld.working_dir = from_slash(env_dir)

        # advertise_host 是「返回给客户端连接的 host」,属每台机器各异的运行期值:
        # 内网测试服要用局域网 IP(远程策划客户端才连得到战斗 DS),本机自测用 127.0.0.1。
        # env 注入优先级高于 yaml 写死值,策划零操作。
        env_host = (os.environ.get("PANDORA_DS_ADVERTISE_HOST") or "").strip()
        if env_host:
            ld.advertise_host = env_host
        if ld.advertise_host == "":
            ld.advertise_host = DEFAULT_LOCAL_ADVERTISE_HOST
        if ld.port_base == 0:
            ld.port_base = DEFAULT_LOCAL_PORT_BASE
        if ld.port_range == 0:
            ld.port_range = DEFAULT_LOCAL_PORT_RANGE
        if ld.log_dir == "":
            ld.log_dir = DEFAULT_LOCAL_LOG_DIR

        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR

    # ── 生产授权权威判定 ───────────────────────────────────────────────────
    def requires_reliable_lifecycle_publication(self) -> bool:
        """对应 Go 的 `RequiresReliableLifecyclePublication()`。

        Redis authority 是生产授权权威;Agones + enforce 的 legacy 灰度同样属于生产路径。
        两者缺了可靠的 `pandora.ds.lifecycle` 发布链,abandoned 就没有恢复出口 ——
        BattleResult 无法生成 match release / battle exit proof,不能以「镜像稍后过期」
        冒充恢复完成(过期只是删镜像,不产生任何补偿事实)。

        ★ 判据是 **trim + 大小写不敏感**,与 `DSAuthConf.authority_mode_redis()` 的
          精确比较**不一致** —— 这是 Go 侧的既有不一致,原样搬过来,见交付报告④。
        """
        authority = (self.ds_auth.authority_mode or "").strip().lower()
        if authority == "redis":
            return True
        return (self.mode or "").strip().lower() == MODE_AGONES and (
            self.ds_auth.mode or ""
        ).strip().lower() == "enforce"

    # ── Validate:逐条照抄 Go,每条注明「不设这道闸会怎样」──────────────────
    def validate_lifecycle_publication_config(self) -> None:
        """Go: `ValidateLifecyclePublicationConfig`(事件 `ds_lifecycle_config_invalid`)。

        不设这道闸会怎样:生产授权模式带着一个没有 broker 的 producer 启动,心跳超时判
        abandoned 后事件发不出去。玩家侧表现是「对局早就没了,但 match 一直没释放」——
        段位不回滚、terminal placement 不补偿,而 ds_allocator 日志里一切正常。
        闸必须在**任何 Redis / Kubernetes 副作用之前**跑,否则等 producer 初始化才发现
        没有恢复出口时,已经有局被分配出去了。

        ★ broker 列表里的空白项不算已配置(见 KafkaConf.configured)。
        """
        if not self.requires_reliable_lifecycle_publication():
            return
        if self.kafka.configured():
            return
        raise ValueError(
            "ds_allocator: production authority requires kafka.brokers for reliable "
            "pandora.ds.lifecycle publication"
        )

    def validate_battle_departure_config(self) -> None:
        """Go: `ValidateBattleDepartureConfig`(事件 `battle_departure_config_invalid`)。

        不设这道闸会怎样:生产授权模式下 locator_addr 留空 = BATTLE presence 永不续期,
        而它是局内唯一的路由信号(监控 / 客服 / 重连辅助都读它)。缺失不会报错,只会让
        presence 到 TTL 后蒸发,看起来像「所有人都不在战斗里」。
        """
        if self.requires_reliable_lifecycle_publication() and not (self.locator_addr or "").strip():
            raise ValueError(
                "ds_allocator: production authority requires locator_addr for battle "
                "presence renewal"
            )

    def validate_allocation_abort_auth_config(self) -> None:
        """Go: `ValidateAllocationAbortAuthConfig`(事件 `allocation_abort_auth_config_invalid`)。

        把「Matchmaker 撤销一次已分配但尚未发票的 DS」这个**破坏性**端点做成 Redis
        Model-B 的启动依赖。不设这道闸会怎样:
          - 密钥缺失 / 太短 → 这个能撤销任意分配的内部端点等于没有身份边界;
          - 与 ds_auth.secret 同钥 → 任何持 DS 回调密钥的一方(每台 DS 都持有可核验的
            令牌链)都能冒充 Matchmaker 撤销别人的分配 —— 信任域静默塌缩成一个。
        Legacy / local 模式不暴露更弱的兜底,RPC 本身在未接线时仍 fail-closed。
        """
        if not self.ds_auth.authority_mode_redis():
            return
        a = self.allocator
        try:
            internalrpcauth.validate_secret(a.allocation_abort_auth_secret)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(
                f"ds_allocator: allocator.allocation_abort_auth_secret invalid: {exc}"
            ) from exc
        try:
            internalrpcauth.validate_identity(a.allocation_abort_auth_audience)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(
                f"ds_allocator: allocator.allocation_abort_auth_audience invalid: {exc}"
            ) from exc
        if a.allocation_abort_auth_secret == self.ds_auth.secret:
            raise ValueError(
                "ds_allocator: allocation abort auth must use an independent trust-domain key"
            )

    def validate_local_map_source_config(self) -> None:
        """Go: `ValidateLocalMapSourceConfig`(事件 `local_map_source_config_invalid`)。

        锁死 mode=local 的「关卡从哪来」:必须有唯一权威源,二选一 ——
          - `config_table.dir`:allocator 读关卡表按 map_id 拼启动 URL;
          - `local_ds.loader_map`:DS 统一启到 Loader 关卡,由 UE 侧查**同一张**表。

        不设这道闸会怎样:两者皆空时不是「回退默认图」,而是**每一局都失败**且失败得很晚 ——
        DS 起了默认图 → DS 侧关卡门判「已加载世界 ≠ 注入 map_id」→ fail-closed 自杀 →
        分配卡到超时,玩家侧只看到「一直排队中」。把它挡在启动边界,而不是等玩家点开始。
        """
        if self.mode != MODE_LOCAL:
            return
        if (self.local_ds.loader_map or "").strip() or (self.config_table.dir or "").strip():
            return
        raise ValueError(
            "ds_allocator: mode=local 必须配 config_table.dir(关卡按 map_id 现查 g_关卡.xlsx)"
            "或 local_ds.loader_map(DS 侧 Loader 查同一张表);两者皆空则无处得知 map_id 对应哪张图"
        )

    def validate_roster_join_deadline_config(self) -> None:
        """Go: `ValidateRosterJoinDeadlineConfig`(事件 `roster_join_deadline_config_invalid`)。

        不设这道闸会怎样:`enforce + generation=0` 是自相矛盾的配置 —— 判定谓词
        (roster_deadline_should_abandon)对 gen=0 恒 false,写下 enforce 的人以为开了闸,
        实际对所有局静默 no-op。这种「看起来启用了、行为却没变」正是本项目一贯 fail-fast
        的失效形状:运维不会再去看采证指标,而缺员局照旧一场场打完。

        ★ 本函数同时承担档位拼写校验(resolve 会抛),所以它也是 writer/roster 档位
          唯一的启动期入口之一 —— main.py 不能跳过它。
        """
        mode = self.allocator.resolve_roster_join_mode()
        if mode == ROSTER_JOIN_MODE_ENFORCE and self.allocator.roster_policy_generation == 0:
            raise ValueError(
                "allocator.roster_join_deadline_mode=enforce requires "
                "roster_policy_generation > 0: generation 0 battles are permanently exempt, "
                "so enforce with 0 is a silent no-op; set roster_policy_generation to the "
                "activation generation (see decision-revisit §5)"
            )

    def validate_battle_token_ttl_vs_battle_ttl(self, signer_enabled: bool) -> None:
        """Go 在 **main.go** 里的那条(事件 `ds_auth_battle_token_ttl_too_small_vs_battle_ttl`)。

        它不在 conf.go 里,因为要等 signer 装配完才知道本副本是否签发令牌;但判据全部来自
        配置,所以搬到这里由 main.py 在拿到 signer 后调用。

        不设这道闸会怎样:战斗令牌**不续期**(一局一签、DS 一局一销毁)。
        `battle_token_ttl < battle_ttl + 15m` 时,长局跑到一半令牌过期 → 心跳 /
        ReportResult 被 enforce 守卫全拒 → 赛果无法结算。固定下限(1h)拦不住这个,
        因为 battle_ttl 是可配的。
        """
        if not signer_enabled:
            return
        need = self.allocator.battle_ttl_td() + BATTLE_TOKEN_RECONNECT_MARGIN
        if self.ds_auth.battle_token_ttl_td() < need:
            raise ValueError(
                f"ds_auth.battle_token_ttl={self.ds_auth.battle_token_ttl!r} < "
                f"battle_ttl({self.allocator.battle_ttl!r}) + 15m; "
                f"战斗令牌不续期,须 >= battle_ttl + 15m 重连余量"
            )

    def validate_conf(self) -> None:
        """按 Go main.go 的顺序跑纯配置期的五条闸(不含依赖装配结果的两条 TTL 闸)。

        ★ 提供这个聚合入口只是为了测试与本地自检方便。**main.py 必须逐条单独调用**,
          因为每条对应一个独立的事件名 —— 事件名是 Loki 告警和运维手册的入口,
          全部归成一个 `config_validation_failed` 会让排障方向从第一步就错。
        """
        self.ds_auth.validate_redis_fence()
        self.validate_lifecycle_publication_config()
        self.validate_battle_departure_config()
        self.validate_allocation_abort_auth_config()
        self.validate_local_map_source_config()
        self.validate_roster_join_deadline_config()

    @classmethod
    def load(cls, path: str | pathlib.Path) -> "Config":
        """加载 + 填默认值。**不在这里跑 validate_conf** —— Go 侧 config_scan_failed 与
        各条 validate 是不同事件名,main.py 按 Go 的顺序分别调用并分别打事件。
        """
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg


# ★ 供 main.py / 测试引用,避免再各自 import placement:
#   owner 实例租约的秒数固定用它(data/owner_lease_client.go),不可配置 —— 它是
#   §9.22 的正确性常量,调小会重新打开「一名玩家同时在两台 DS」的脑裂窗口。
DS_FENCE_LEASE_MAX_SECONDS = placement.DS_FENCE_LEASE_MAX_SECONDS
DS_FENCE_REENTRY_BARRIER_SECONDS = placement.DS_FENCE_REENTRY_BARRIER_SECONDS
