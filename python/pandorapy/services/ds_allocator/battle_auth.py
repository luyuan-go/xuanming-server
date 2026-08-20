"""Battle DS Model B 的 Redis 单一授权权威 —— 对应 Go 侧
`services/battle/ds_allocator/internal/data/battle_auth.go`(2631 行,逐函数移植)。

四把权威键共享 `{match_id}` hashtag,因此 Prepare/Stage/Activate/Delete 都能在
Redis Cluster 的单个 slot 内用 WATCH/MULTI/EXEC 建立线性化点:

    pandora:ds:battle:{<match_id>}          BattleStorageRecord
    pandora:ds:auth:{<match_id>}            BattleDSAuthStorageRecord
    pandora:ds:authgen:{<match_id>}         永不过期的凭据代际高水位计数器
    pandora:ds:authstab:{<match_id>}        两阶段激活稳定性证据 `gen|jti|firstMs|count`
    pandora:ds:result-receipt:{<match_id>}  battle_result 已落库的完整凭据(dsauthrecord)

K8s annotation 只负责投递,不参与任何授权判断。read-modify-write 使用默认
`ParseFromString`(**不** DiscardUnknown),保留滚动更新期间旧 writer 不认识的
unknown fields(§9 不变量 17)。

═══ 为什么这份代码的每一条判据都不能"顺手简化" ═══

`CLAUDE.md` §9 不变量 3 / 6 / 22 的三条硬约束在本文件汇合:

  ① 凭据身份是**四元组** `(instance_uid, instance_epoch, gen, jti)`,再加
     `kid / token_sha256 / exp_ms / writer_epoch` 的完整性绑定。只比 gen 会被
     "代际计数器复位后重新发出的第 N 代"骗过 —— counter 键虽然刻意不设 TTL,
     但 auth 键被清理/过期后重建的记录 `high_water_gen=0`,只比记录内水位一样会漏。
  ② `high_water_gen` 是**单调水位**:任何 stage 都要求 `gen > high_water_gen`,
     任何心跳都要求 `high_water_gen >= id.gen`(上报代际高于权威水位 = 这份凭据
     不可能出自本权威)。
  ③ 所有失败路径 fail-closed:读不到 / 读不确定 / 记录格式不认识,一律拒绝并
     零副作用,**绝不**退化成"当作没有凭据"或"当作校验通过"。

★ `writer_epoch` 语义与 hub 侧一致:必须**恰好等于** `DS_AUTH_WRITER_EPOCH_V2`。
  Go 的 `battleAuthRecordV2Exact` 要求记录与其 active/pending 凭据全部为 v2 ——
  更低的旧 writer 写不出满足 `required_writer_epoch=2` 的记录,更高的未来 writer
  的记录只能由对应未来二进制处理(本版本一律拒,不做隐式迁移)。

★ 本模块只做数据层,**没有后台循环**(sweep 循环在 biz 层,按 `pandorapy.safego`
  规范起协程)。这里的每个方法都是一次有界的 CAS 事务。
"""

from __future__ import annotations

import dataclasses
import hmac
import re
import time
import unicodedata
from typing import Any, Callable

from google.protobuf import unknown_fields as _unknown_fields
from pandora.ds.v1 import allocator_pb2 as dspb
from redis.exceptions import WatchError

from pandorapy import dsauthrecord, errcode, releasetrack
from pandorapy import log as plog
from pandorapy.auth import DS_AUTH_WRITER_EPOCH_V2
from pandorapy.protoenum import enum_name
from pandorapy.services.ds_allocator import conf as dsconf

__all__ = [
    "ACTIVE_KEY",
    "BATTLE_AUTH_CAS_RETRIES",
    "BATTLE_DS_WRITER_EPOCH_V2",
    "BATTLE_STATE_ALLOCATION_ABORT_PENDING",
    "BATTLE_STATE_ALLOCATION_RECONCILE_EMPTY_TOMBSTONE",
    "BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING",
    "BATTLE_STATE_ALLOCATION_UNCERTAIN",
    "BATTLE_STATE_PREACTIVE_RELEASE_PENDING",
    "BattleAbandonResult",
    "BattleActivateResult",
    "BattleAuthStaleError",
    "BattleAuthorityBinding",
    "BattleAuthoritySnapshot",
    "BattleCredentialIdentity",
    "BattleCredentialSeed",
    "BattleExpectedInstance",
    "BattleHeartbeatInput",
    "BattleQuarantineExpected",
    "BattleQuarantineResult",
    "BattleResultAuthorizationProof",
    "BattleResultCommittedError",
    "BattleResultNotRecordedError",
    "BattleStageInput",
    "BattleStaleCutoffs",
    "BattleWarmingForfeit",
    "RedisBattleAuthRepo",
    "battle_auth_gen_key",
    "battle_auth_key",
    "battle_key",
]

# ── 常量(与 Go 同值)─────────────────────────────────────────────────────────

#: Model B writer 的机械激活代际。Go: `BattleDSWriterEpochV2 = auth.DSAuthWriterEpochV2`。
#: ★ 从 `pandorapy.auth` 取,不手抄字面量 —— 两处各写一个 `2` 时,改动其一不会报错。
BATTLE_DS_WRITER_EPOCH_V2: int = DS_AUTH_WRITER_EPOCH_V2

#: 高并发 Allocate/Heartbeat 会同时争用单个 match 的多把同槽键;64 次只是 WATCH
#: 冲突后的重新读取,不重放任何外部副作用。Go: `battleAuthCASRetries`。
BATTLE_AUTH_CAS_RETRIES = 64

#: 全局心跳超时扫描索引(与 `{match_id}` **不同 slot**,只能独立命令)。Go: `activeKey`。
ACTIVE_KEY = "pandora:ds:active"

# ── battle 生命周期状态常量(Go: battle.go)────────────────────────────────────
#
# 这几个字符串是 `battle_storage_invariant.go` 的写入侧判据,strict 模式下每次写
# battle 记录都要走一遍。等 `battle.py` 移植落地后应改为从那里 import,现在自带
# 一份是为了让本模块自洽(值必须逐字符一致,漂移即静默放行非法写)。

BATTLE_STATE_ALLOCATION_UNCERTAIN = "allocation_uncertain"
BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING = "allocation_reconcile_release_pending"
BATTLE_STATE_ALLOCATION_RECONCILE_EMPTY_TOMBSTONE = "allocation_reconcile_empty_tombstone"
BATTLE_STATE_PREACTIVE_RELEASE_PENDING = "preactive_release_pending"
BATTLE_STATE_ALLOCATION_ABORT_PENDING = "allocation_abort_pending"

_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1
_INT64_MAX = (1 << 63) - 1

#: canonical UUIDv4(RFC4122 variant)的小写 36 字符形式。
#:
#: ★ `\A...\Z` 而不是 `^...$`:Python 的 `$` 会匹配**末尾换行之前**的位置,于是
#:   `"<uuid>\n"` 能通过校验 —— 一个带尾随换行的 allocation_id 会被判成 canonical,
#:   而它与 Redis 里真正的键值并不相等。Go 的 `uuid.Parse` + `String()==value`
#:   不存在这个缺口,Python 侧必须用 `\Z` 补齐。
_CANONICAL_UUID4_RE = re.compile(
    r"\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z"
)


def now_ms() -> int:
    """服务端接收时刻(unix ms)。Go: `time.Now().UnixMilli()`。"""
    return int(time.time() * 1000)


# ── Redis key 模板(逐字符照抄 Go;`{match_id}` hashtag 一个都不能少)───────────
#
# ★ 花括号不是装饰,是 Redis Cluster 的 hash tag。去掉之后单机 Redis 上一切正常,
#   上了 Cluster 才会 CROSSSLOT 拒绝整个事务 —— 而"授权键与投影键同事务"正是
#   Model B 消灭半激活 / TOCTOU 误分配的全部依据。


def battle_auth_key(match_id: int) -> str:
    """`pandora:ds:auth:{<match_id>}`。Go: `battleAuthKey`。"""
    return f"pandora:ds:auth:{{{match_id}}}"


def battle_auth_gen_key(match_id: int) -> str:
    """`pandora:ds:authgen:{<match_id>}` —— 永不过期的代际高水位计数器。Go: `battleAuthGenKey`。"""
    return f"pandora:ds:authgen:{{{match_id}}}"


def battle_key(match_id: int) -> str:
    """`pandora:ds:battle:{<match_id>}`。Go: `battleKey`(battle.go)。"""
    return f"pandora:ds:battle:{{{match_id}}}"


def battle_activation_evidence_key(match_id: int) -> str:
    """`pandora:ds:authstab:{<match_id>}` —— 两阶段激活稳定性证据。Go: `battleActivationEvidenceKey`。"""
    return f"pandora:ds:authstab:{{{match_id}}}"


# ── 错误(Go 的哨兵 error 在 Python 里做成异常类型)──────────────────────────


class BattleAuthStaleError(errcode.PandoraError):
    """对应 Go 的 `errBattleAuthStale`。

    ErrUnauthorized:对 DS 呈现明确鉴权拒绝码,fail-closed 且记录零变更。

    ★ `reason` 是**声明式 slot**,不是 `setattr`:Go 侧把拒绝原因打进日志
      (`battleAuthStale` 的 reason 枚举),Python 侧除了同样打日志,还把它挂在异常上
      供调用方与测试断言。用 `setattr` 也能写进去(Exception 自带 `__dict__`),
      但拼错一个字母不会报错,读的那侧只会永远拿到 `""` —— 与"没有原因"无法区分。

    ★ 做成**类型**而不是模块级单例:Go 用 `err == errBattleAuthStale` 比身份,
      Python 里单例异常被 raise 后会被写入 `__traceback__`,跨协程复用同一个对象
      会串味。类型判定(`except BattleAuthStaleError`)与 Go 的身份判定等价。
    """

    __slots__ = ("reason",)

    def __init__(
        self, msg: str = "battle ds credential not authoritative", *, reason: str = ""
    ) -> None:
        super().__init__(errcode.ErrUnauthorized, msg)
        self.reason = reason


class BattleResultNotRecordedError(errcode.PandoraError):
    """对应 Go 的 `errBattleResultNotRecorded`。"""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(
            errcode.ErrInvalidState, "battle result is not authoritatively recorded"
        )


class BattleResultCommittedError(errcode.PandoraError):
    """对应 Go 的 `errBattleResultCommitted`。"""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(
            errcode.ErrInvalidState,
            "battle result already committed; credential rotation is fenced",
        )


# ── ActivateHeartbeat 的授权拒绝 reason 枚举(Go 同名常量,§11.3 R2)──────────
#
# 命名对齐各自的判定依据,一条规则一个值,snake_case 稳定不变。
# 为什么必须有:近十条互不相同的 fencing 规则全部收敛成同一个 ErrUnauthorized,
# 而 ErrUnauthorized 不属 server fault,默认 info 级下"心跳被拒了"和"为什么被拒"
# 两件事都看不见。

AUTH_REJECT_IDENTITY_INVALID = "credential_identity_invalid"
AUTH_REJECT_BINDING_MISMATCH = "authority_binding_mismatch"
AUTH_REJECT_TERMINATING_PHASE = "auth_terminating_non_terminal"
AUTH_REJECT_PHASE_LOCKED = "auth_phase_locked"
AUTH_REJECT_NO_USABLE_CREDENTIAL = "no_usable_credential"
AUTH_REJECT_GEN_BELOW_HIGH_WATER = "gen_below_high_water"
AUTH_REJECT_PROMOTE_ON_TERMINAL = "promote_on_terminal_battle"
AUTH_REJECT_STATE_NOT_READY_RUN = "promote_state_not_ready"


def _battle_auth_stale(
    match_id: int, ident: BattleCredentialIdentity, reason: str, **kv: Any
) -> BattleAuthStaleError:
    """打一条带**枚举 reason** 的授权拒绝日志,再返回同一种 `BattleAuthStaleError`。

    Go: `battleAuthStale`。返回值与行为完全不变(同一个错误码),只多一条日志。
    每个拒绝点至多打一条:异常一旦抛出,外层 CAS 循环立即退出,不会因 WATCH 重跑重复。

    ★ event 名与字段名与 Go 逐字节相同(`msg` → structlog 的 event 位)。
    """
    plog.get().warning(
        "battle_ds_auth_rejected",
        match_id=match_id,
        reason=reason,
        pod=ident.pod_name,
        uid=ident.instance_uid,
        epoch=ident.instance_epoch,
        gen=ident.gen,
        jti=ident.jti,
        writer_epoch=ident.writer_epoch,
        **kv,
    )
    return BattleAuthStaleError(reason=reason)


# ── 值对象(Go struct → dataclass)────────────────────────────────────────────


@dataclasses.dataclass(frozen=True, slots=True)
class BattleCredentialIdentity:
    """中间件验签后交给权威仓的完整凭据身份。Go: `BattleCredentialIdentity`。

    `exp_ms` / `kid` / `token_sha256` 也属于身份,**禁止退化成只比较 gen/jti**。
    """

    pod_name: str = ""
    instance_uid: str = ""
    instance_epoch: int = 0
    gen: int = 0
    jti: str = ""
    exp_ms: int = 0
    kid: str = ""
    token_sha256: str = ""
    writer_epoch: int = 0


@dataclasses.dataclass(frozen=True, slots=True)
class BattleAuthorityBinding:
    """把一个 allocation_id 钉到一个真实 GameServer UID。Go: `BattleAuthorityBinding`。"""

    match_id: int = 0
    allocation_id: str = ""
    pod_name: str = ""
    instance_uid: str = ""
    required_writer_epoch: int = 0
    auth_ttl_sec: float = 0.0
    battle_ttl_sec: float = 0.0


@dataclasses.dataclass(frozen=True, slots=True)
class BattleCredentialSeed:
    """签发 JWT 前从 Redis 领取的实例纪元和全局单调代际。Go: `BattleCredentialSeed`。"""

    instance_epoch: int = 0
    gen: int = 0


@dataclasses.dataclass(frozen=True, slots=True)
class BattleExpectedInstance:
    """终止/清理 fencing token。Go: `BattleExpectedInstance`。

    allocation_id 防旧请求,UID+epoch 防同名 GameServer 重建;三者必须与 Redis
    auth+battle 绑定同时匹配。
    """

    allocation_id: str = ""
    instance_uid: str = ""
    instance_epoch: int = 0


@dataclasses.dataclass(frozen=True, slots=True)
class BattleResultAuthorizationProof:
    """battle_result 已完成 Guard + Redis active 校验并持久化进 MySQL outbox 的证明。

    Go: `BattleResultAuthorizationProof`。`authorized_at_ms` 必须早于 `exp_ms`;
    relay 当前时刻可以晚于 `exp_ms` —— 普通凭据 gen/jti 轮换不改变 stable
    GameServer identity,因而不会阻断已提交结算的资源回收。
    """

    credential: BattleCredentialIdentity = dataclasses.field(
        default_factory=BattleCredentialIdentity
    )
    authorized_at_ms: int = 0


@dataclasses.dataclass(frozen=True, slots=True)
class BattleQuarantineExpected:
    """防旧运维请求误隔离同名重建实例。Go: `BattleQuarantineExpected`。"""

    allocation_id: str = ""
    credential: BattleCredentialIdentity = dataclasses.field(
        default_factory=BattleCredentialIdentity
    )


@dataclasses.dataclass(slots=True)
class BattleQuarantineResult:
    """区分 Redis 唯一授权权威吊销与派生 battle 投影补偿。Go: `BattleQuarantineResult`。

    `auth_quarantined=True` 后泄露 token 已失效;`projection_abandoned=False`
    只表示投影需独立审计,**绝不能**反过来阻止吊销。
    """

    auth_quarantined: bool = False
    projection_abandoned: bool = False


@dataclasses.dataclass(frozen=True, slots=True)
class BattleStageInput:
    """签发完成后暂存 pending 凭据的输入。Go: `BattleStageInput`。"""

    match_id: int = 0
    allocation_id: str = ""
    credential: Any = None  # dspb.BattleDSCredential
    auth_ttl_sec: float = 0.0


@dataclasses.dataclass(slots=True)
class BattleHeartbeatInput:
    """已验签 DS 心跳的业务负载。Go: `BattleHeartbeatInput`。

    ★ `ts_ms` 故意不存在:权威心跳时间只取 Redis writer 的服务端接收时间,
      客户端时间只能在仓外做遥测。一个未来时间戳能让失联 DS 长期"心跳新鲜"。
    """

    player_count: int = 0
    state: str = ""
    auth_ttl_sec: float = 0.0
    battle_ttl_sec: float = 0.0
    empty_battle_timeout_sec: float = 0.0
    #: 「从未连入」(`ever_had_players=False`)局的空场回收阈值。与
    #: `empty_battle_timeout_sec` 二选一,依据只看 `ever_had_players`,不看当前
    #: player_count:有人连入过的局即便此刻空了也要给断线重连留路(长阈值),
    #: 从未连入的局没人要回来(短阈值)。零值 = 退化为单阈值(改动前行为)。
    no_show_timeout_sec: float = 0.0
    #: 两阶段激活稳定性门:staged→ACTIVE 提升要求 ≥beats 次实收心跳且首尾跨度
    #: ≥span_ms。`beats<=1 且 span_ms<=0` 时门关闭(零值兼容旧行为)。
    stability_beats: int = 0
    stability_span_ms: int = 0
    #: 花名册到齐期限,0 = 关闭本闸。上面两个空场阈值都只管「一个人都没有」,
    #: 拦不住「来了但没来齐」。
    roster_join_deadline_sec: float = 0.0
    #: 本闸「还允不允许被武装」的时间窗(自 allocated_at 起算)。滚动升级中新副本
    #: 接手一局**老** battle 时过窗即不动手 —— 否则会把「局中掉线」误判成
    #: 「开局没到齐」,判弃一场正在打的对局。
    roster_join_arm_window_sec: float = 0.0
    #: DS 上报的真实在场名单。**`census_present=False` 时本闸整道跳过** ——
    #: 拿 player_count 硬猜既说不出缺的是谁,也会在 legacy 档把每一局都判成缺员。
    census_present: bool = False
    active_player_ids: tuple[int, ...] = ()
    #: 到齐期限的激活档与配置策略代(observe→enforce 协议)。到点后能不能**真**判弃
    #: 由 `conf.roster_deadline_should_abandon` 统一判定(与 biz legacy 路径同一份谓词)。
    roster_join_mode: str = ""
    roster_policy_generation: int = 0


@dataclasses.dataclass(slots=True)
class BattleActivateResult:
    """pending→active ACK 与事务提交后的 Battle 镜像。Go: `BattleActivateResult`。"""

    #: staged 心跳已实收但稳定性证据不足,未提升 ACTIVE、零状态转移(battle 保持
    #: warming);响应不得携带 ACK,DS 每 tick 幂等重试。
    activation_pending: bool = False
    first_activation: bool = False
    #: 只在本事务首次把 active battle 推进为 abandoned 时为 True;外层仅赢家执行
    #: 一次 Pod 回收,补偿投递仍可由 sweep 幂等重试。
    first_abandon: bool = False
    #: 本次判弃的原因是「花名册没到齐」而不是空场。外层据此只罚缺席者。
    roster_incomplete: bool = False
    #: 到齐期限到点,但激活档/策略代不允许真判弃(observe 采证 / legacy 旧代豁免)。
    roster_would_abandon: bool = False
    terminal: bool = False
    heartbeat_ms: int = 0
    active: BattleCredentialIdentity = dataclasses.field(
        default_factory=BattleCredentialIdentity
    )
    battle: Any = None  # dspb.BattleStorageRecord


@dataclasses.dataclass(slots=True)
class BattleAbandonResult:
    """sweep 原子 stale 判定与终止结果。Go: `BattleAbandonResult`。"""

    abandoned: bool = False
    already_terminal: bool = False
    auth_found: bool = False
    active_found: bool = False
    battle: Any = None  # dspb.BattleStorageRecord


@dataclasses.dataclass(frozen=True, slots=True)
class BattleWarmingForfeit:
    """把「编排层判死」绑定到 exact 分配身份,防 ABA。Go: `BattleWarmingForfeit`。

    probe 探的是旧分配 A,事务执行时同 match_id 可能已换成新分配 B(A 被清理 →
    matchmaker 重试)。事务内 battle 的 allocation_id+gameserver_uid+instance_epoch
    与 `instance` 精确一致时才允许用 `heartbeat_ms`(判死时刻)替代常规冷加载宽限;
    身份不一致即视为旧 probe 结果作废。
    """

    instance: BattleExpectedInstance = dataclasses.field(
        default_factory=BattleExpectedInstance
    )
    heartbeat_ms: int = 0


@dataclasses.dataclass(frozen=True, slots=True)
class BattleStaleCutoffs:
    """sweep 判弃的双阈值(均为 unix ms,早于该值视为失联)。Go: `BattleStaleCutoffs`。

    - `active_heartbeat_ms`:已激活(auth.active 存在)或 allocating 快速清理路径,
      对应 HeartbeatTimeout(不变量 §4,默认 15s)。
    - `warming_heartbeat_ms`:尚未激活的 warming 冷加载窗口(大图 ServerTravel 到
      GameMode BeginPlay 前没有业务心跳是正常行为),对应 ReadyWaitTimeout。

    ★ 选择必须由 `abandon_if_stale` 在 WATCH auth+battle 事务内依据同一权威快照做:
      外层 sweep 读到的 State 快照会与首次 `activate_heartbeat` 并发(TOCTOU),
      不得用它决定单一阈值。
    """

    active_heartbeat_ms: int = 0
    warming_heartbeat_ms: int = 0
    #: 非空时表示编排层已权威确认某 exact 实例死亡,warming 可放弃时间宽限提前判弃。
    warming_forfeit: BattleWarmingForfeit | None = None

    def warming_cutoff_for(self, battle) -> int:  # noqa: ANN001
        """在事务内按 battle 精确身份选择 warming 阈值(ABA 防护本体)。Go: `warmingCutoffFor`。"""
        f = self.warming_forfeit
        if (
            f is not None
            and battle.allocation_id == f.instance.allocation_id
            and battle.gameserver_uid == f.instance.instance_uid
            and battle.instance_epoch == f.instance.instance_epoch
        ):
            return f.heartbeat_ms
        return self.warming_heartbeat_ms


@dataclasses.dataclass(slots=True)
class BattleAuthoritySnapshot:
    """auth+battle 的同一 Redis 快照。Go: `BattleAuthoritySnapshot`。"""

    auth: Any = None  # dspb.BattleDSAuthStorageRecord
    battle: Any = None  # dspb.BattleStorageRecord
    auth_found: bool = False
    battle_found: bool = False

    def ready_authorized(self, now_ms_value: int, max_heartbeat_age_ms: int) -> tuple[bool, str]:
        """`wait_battle_ready` 的最终分配门。Go: `ReadyAuthorized`。

        授权 active、实例/投影/心跳完全一致、battle ready/running 且服务端心跳新鲜。
        **任一字段缺失均 fail-closed。**
        """
        if now_ms_value <= 0:
            now_ms_value = now_ms()
        ok, reason = self._active_projection_consistent(now_ms_value)
        if not ok:
            return False, reason
        if self.battle.state not in ("ready", "running"):
            return False, "battle-not-ready"
        # Model B 不再用"必须严格晚 1ms"区分初始化时间和真实心跳;active 投影与
        # auth.last_active_heartbeat_ms 已证明这确实来自一次授权心跳,同毫秒也合法。
        if self.battle.last_heartbeat_ms < self.battle.allocated_at_ms:
            return False, "no-post-allocation-heartbeat"
        if (
            max_heartbeat_age_ms > 0
            and now_ms_value - self.auth.last_active_heartbeat_ms > max_heartbeat_age_ms
        ):
            return False, "heartbeat-stale"
        return True, ""

    def heartbeat_fresh(self, now_ms_value: int, threshold_ms: int) -> bool:
        """sweep 命中陈旧 ZSET member 后二次核验 Redis 权威记录。Go: `HeartbeatFresh`。"""
        if now_ms_value <= 0:
            now_ms_value = now_ms()
        ok, _ = self._active_projection_consistent(now_ms_value)
        return ok and self.auth.last_active_heartbeat_ms > threshold_ms

    def _active_projection_consistent(self, now_ms_value: int) -> tuple[bool, str]:
        """Go: `activeProjectionConsistent`。逐条对应,顺序不变(reason 字符串照抄)。"""
        if not self.auth_found or self.auth is None:
            return False, "auth-missing"
        if not self.battle_found or self.battle is None:
            return False, "battle-missing"
        a, b = self.auth, self.battle
        if a.match_id == 0 or a.match_id != b.match_id:
            return False, "match-mismatch"
        if a.allocation_id == "" or a.allocation_id != b.allocation_id:
            return False, "allocation-mismatch"
        if a.ds_pod_name == "" or a.ds_pod_name != b.ds_pod_name:
            return False, "pod-mismatch"
        if (
            a.instance_uid == ""
            or a.instance_uid != b.gameserver_uid
            or a.instance_epoch == 0
            or a.instance_epoch != b.instance_epoch
        ):
            return False, "instance-mismatch"
        if a.phase not in (
            dspb.BATTLE_AUTH_PHASE_ACTIVE,
            dspb.BATTLE_AUTH_PHASE_ROTATING,
        ):
            return False, "phase-not-active"
        active = _active_of(a)
        if not battle_credential_complete(active, a, now_ms_value) or a.high_water_gen < a.active.gen:
            return False, "active-incomplete"
        if (
            b.last_verified_gen != a.active.gen
            or not _str_eq(b.last_verified_jti, a.active.jti)
            or b.last_verified_writer_epoch != a.active.writer_epoch
        ):
            return False, "projection-mismatch"
        if (
            a.last_active_heartbeat_ms <= 0
            or a.last_active_heartbeat_ms > now_ms_value
            or b.last_heartbeat_ms != a.last_active_heartbeat_ms
        ):
            return False, "heartbeat-mismatch"
        return True, ""


# ── proto 小工具 ─────────────────────────────────────────────────────────────


def clone(msg):  # noqa: ANN001
    """深拷贝一条 proto。对应 Go 的 `proto.Clone`。

    ★ 必须用 `CopyFrom`:proto message 是引用语义,`a = b` 之后改 a 就是改 b
      (Go 侧同样禁止值拷贝,§5 第 10 条)。`CopyFrom` 连 unknown fields 一起带走。
    """
    out = type(msg)()
    out.CopyFrom(msg)
    return out


def proto_equal(a, b) -> bool:  # noqa: ANN001
    """完整相等(**含 unknown fields**)。对应 Go 的 `proto.Equal`。

    用确定性序列化比较而不是 `a == b`:只有 upb 实现的 `__eq__` 覆盖 unknown
    fields,pure-Python 实现不覆盖 —— 漏掉 unknown 会让滚动升级期的旧副本
    "判等成功"并覆盖抹掉新字段。
    """
    if a is None or b is None:
        return a is b
    if type(a) is not type(b):
        return False
    return a.SerializeToString(deterministic=True) == b.SerializeToString(deterministic=True)


def _unknown_signature(msg) -> tuple:  # noqa: ANN001
    """把 unknown fields 折成可比较的规范元组。对应 Go 的 `ProtoReflect().GetUnknown()` 原始字节。

    ★ Python protobuf 不暴露 unknown 的原始字节切片,只给 `UnknownFieldSet`。
      这里按 `(field_number, wire_type, data)` 递归展开成元组,对"同一份 bytes
      解出来的记录与它的克隆"这个实际用法等价于 Go 的 `bytes.Equal`。
    """
    try:
        ufs = _unknown_fields.UnknownFieldSet(msg)
    except Exception:  # noqa: BLE001 —— 运行时不支持时按"无 unknown"处理
        return ()
    out = []
    for field in ufs:
        data = field.data
        if isinstance(data, (bytes, bytearray)):
            out.append((field.field_number, field.wire_type, bytes(data)))
        elif isinstance(data, int):
            out.append((field.field_number, field.wire_type, data))
        else:  # group:嵌套 UnknownFieldSet
            out.append((field.field_number, field.wire_type, _unknown_signature_of_set(data)))
    return tuple(out)


def _unknown_signature_of_set(ufs) -> tuple:  # noqa: ANN001
    out = []
    for field in ufs:
        data = field.data
        if isinstance(data, (bytes, bytearray)):
            out.append((field.field_number, field.wire_type, bytes(data)))
        elif isinstance(data, int):
            out.append((field.field_number, field.wire_type, data))
        else:
            out.append((field.field_number, field.wire_type, _unknown_signature_of_set(data)))
    return tuple(out)


def _str_eq(a: str, b: str) -> bool:
    """凭据标识串的**恒定时间**相等比较(jti / kid / token_sha256)。

    ★ Go 用 `==`;Python 侧刻意加固:这些串是"持有即可冒充"的凭据标识,朴素
      `==` 会在首个不同字节处短路,给远程攻击者一个逐字节爆破的时序侧信道。
      `compare_digest` 对等长输入恒定时间(长度差仍会泄露,与 Go 一致,不是回归)。
      先 encode 成 bytes:`compare_digest` 对含非 ASCII 的 str 会抛 TypeError,
      而凭据串完全可能被伪造成非 ASCII —— 那会把一次"应当拒绝"变成 500。
    """
    return hmac.compare_digest(a.encode("utf-8", "surrogatepass"), b.encode("utf-8", "surrogatepass"))


# ── 相位 / 状态谓词(Go 同名函数)────────────────────────────────────────────


def battle_auth_phase_locked(phase: int) -> bool:
    """QUARANTINED / TERMINATING = 实例级永久墓碑。Go: `battleAuthPhaseLocked`。

    ★ 枚举值从 `_pb2` 取,不手抄:手抄的常量在 proto 改动后**不会报错**,
      只会让"已吊销"被判成"未知相位"→ 一台被隔离的 DS 重新可授权。
    """
    return phase in (
        dspb.BATTLE_AUTH_PHASE_QUARANTINED,
        dspb.BATTLE_AUTH_PHASE_TERMINATING,
    )


def battle_auth_phase_stageable(phase: int) -> bool:
    """Go: `battleAuthPhaseStageable`。"""
    return phase in (
        dspb.BATTLE_AUTH_PHASE_BOOTSTRAP,
        dspb.BATTLE_AUTH_PHASE_ACTIVE,
        dspb.BATTLE_AUTH_PHASE_ROTATING,
    )


def battle_terminal(state: str) -> bool:
    """Go: `battleTerminal`。"""
    return state in ("ended", "abandoned")


def battle_credential_preparable_state(state: str) -> bool:
    """Go: `battleCredentialPreparableState`。"""
    return state in ("warming", "ready", "running")


def valid_battle_heartbeat_state(state: str) -> bool:
    """Go: `validBattleHeartbeatState`。"""
    return state in ("", "ready", "running", "ended")


def contains_battle_hash_tag(key: str, match_id: int) -> bool:
    """队列键必须与 auth 键共享 `{match_id}` slot。Go: `containsBattleHashTag`。"""
    open_idx = key.find("{")
    if open_idx < 0:
        return False
    close_offset = key.find("}", open_idx + 1)
    if close_offset < 0:
        return False
    return key[open_idx + 1 : close_offset] == str(match_id)


# ── 凭据谓词(Go 同名函数)──────────────────────────────────────────────────


def _active_of(auth):  # noqa: ANN001
    """`auth.GetActive()`(nil-safe)。未设置时返回 None,而不是零值消息。"""
    return auth.active if auth is not None and auth.HasField("active") else None


def _pending_of(auth):  # noqa: ANN001
    return auth.pending if auth is not None and auth.HasField("pending") else None


def validate_battle_credential(cred, now_ms_value: int) -> None:
    """Go: `validateBattleCredential`。不完整或已过期一律 ErrInvalidArg。"""
    if cred is None or (
        cred.gen == 0
        or cred.jti == ""
        or cred.exp_ms == 0
        or cred.kid == ""
        or cred.instance_uid == ""
        or cred.instance_epoch == 0
        or cred.token_sha256 == ""
        or cred.writer_epoch != BATTLE_DS_WRITER_EPOCH_V2
    ):
        raise errcode.PandoraError(
            errcode.ErrInvalidArg,
            "battle credential requires uid/epoch/gen/jti/kid/hash/exp/writer_epoch=2",
        )
    # ★ Go 是 `c.ExpMs <= uint64(nowMs)`,编译器保证 exp_ms 在 uint64 域内。
    #   Python 的 int 无限精度:一个越界的 exp_ms 能被 SerializeToString 拒绝,
    #   但在此之前的比较会"通过",所以显式补一道域检查(要求 12)。
    if not (0 <= cred.exp_ms <= _UINT64_MAX):
        raise errcode.PandoraError(errcode.ErrInvalidArg, "battle credential exp_ms out of uint64")
    if cred.exp_ms <= now_ms_value:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "battle credential already expired")


def valid_battle_identity(ident: BattleCredentialIdentity, now_ms_value: int) -> bool:
    """Go: `validBattleIdentity`。四元组 + 完整性绑定 + 未过期 + writer=2。"""
    return (
        ident.pod_name != ""
        and ident.instance_uid != ""
        and 0 < ident.instance_epoch <= _UINT32_MAX
        and 0 < ident.gen <= _UINT64_MAX
        and ident.jti != ""
        and 0 < ident.exp_ms <= _UINT64_MAX
        and ident.exp_ms > now_ms_value
        and ident.kid != ""
        and ident.token_sha256 != ""
        and ident.writer_epoch == BATTLE_DS_WRITER_EPOCH_V2
    )


def battle_stored_credential_epochs_v2(rec) -> bool:  # noqa: ANN001
    """Go: `battleStoredCredentialEpochsV2`。"""
    if rec is None:
        return False
    active, pending = _active_of(rec), _pending_of(rec)
    return (active is None or active.writer_epoch == BATTLE_DS_WRITER_EPOCH_V2) and (
        pending is None or pending.writer_epoch == BATTLE_DS_WRITER_EPOCH_V2
    )


def battle_auth_record_v2_exact(rec) -> bool:  # noqa: ANN001
    """Go: `battleAuthRecordV2Exact`。记录与其两份凭据全部为 v2,否则整条拒。"""
    return (
        rec is not None
        and rec.required_writer_epoch == BATTLE_DS_WRITER_EPOCH_V2
        and battle_stored_credential_epochs_v2(rec)
    )


def battle_credential_equal(a, b) -> bool:  # noqa: ANN001
    """八字段全等。Go: `battleCredentialEqual`。任一位不符即视为不同凭据。"""
    return (
        a is not None
        and b is not None
        and a.gen == b.gen
        and _str_eq(a.jti, b.jti)
        and a.exp_ms == b.exp_ms
        and _str_eq(a.kid, b.kid)
        and a.instance_uid == b.instance_uid
        and a.instance_epoch == b.instance_epoch
        and _str_eq(a.token_sha256, b.token_sha256)
        and a.writer_epoch == b.writer_epoch
    )


def battle_credential_matches(cred, ident: BattleCredentialIdentity, now_ms_value: int) -> bool:
    """存储凭据与 DS 上报身份的**全等**比对。Go: `battleCredentialMatches`。

    ★ 四元组 `(instance_uid, instance_epoch, gen, jti)` 任一位不符即拒;
      `exp_ms/kid/token_sha256/writer_epoch` 再补一层完整性绑定。
    """
    return (
        cred is not None
        and valid_battle_identity(ident, now_ms_value)
        and cred.exp_ms > now_ms_value
        and cred.gen == ident.gen
        and _str_eq(cred.jti, ident.jti)
        and cred.exp_ms == ident.exp_ms
        and _str_eq(cred.kid, ident.kid)
        and cred.instance_uid == ident.instance_uid
        and cred.instance_epoch == ident.instance_epoch
        and _str_eq(cred.token_sha256, ident.token_sha256)
        and cred.writer_epoch == ident.writer_epoch
    )


def battle_credential_complete(cred, auth, now_ms_value: int) -> bool:  # noqa: ANN001
    """存储凭据自身完整且与所属 auth 记录自洽。Go: `battleCredentialComplete`。"""
    return (
        cred is not None
        and auth is not None
        and auth.match_id > 0
        and auth.allocation_id != ""
        and auth.ds_pod_name != ""
        and auth.instance_uid != ""
        and auth.instance_epoch > 0
        and cred.gen > 0
        and cred.jti != ""
        and cred.exp_ms > now_ms_value
        and cred.kid != ""
        and cred.token_sha256 != ""
        and cred.instance_uid == auth.instance_uid
        and cred.instance_epoch == auth.instance_epoch
        and battle_auth_record_v2_exact(auth)
        and cred.writer_epoch == BATTLE_DS_WRITER_EPOCH_V2
    )


def authority_binding_matches(auth, battle) -> bool:  # noqa: ANN001
    """auth↔battle 的五格绑定。Go: `authorityBindingMatches`。"""
    return (
        auth is not None
        and battle is not None
        and auth.match_id == battle.match_id
        and auth.allocation_id != ""
        and auth.allocation_id == battle.allocation_id
        and auth.ds_pod_name != ""
        and auth.ds_pod_name == battle.ds_pod_name
        and auth.instance_uid != ""
        and auth.instance_uid == battle.gameserver_uid
        and auth.instance_epoch > 0
        and auth.instance_epoch == battle.instance_epoch
    )


def battle_active_projection_structurally_consistent(auth, battle) -> bool:  # noqa: ANN001
    """sweep 用:允许 active token 已自然过期,但身份/fence/投影/心跳必须仍严格一致。

    Go: `battleActiveProjectionStructurallyConsistent`。
    """
    active = _active_of(auth)
    if (
        not authority_binding_matches(auth, battle)
        or active is None
        or auth.phase not in (dspb.BATTLE_AUTH_PHASE_ACTIVE, dspb.BATTLE_AUTH_PHASE_ROTATING)
    ):
        return False
    c = active
    return (
        c.gen > 0
        and c.jti != ""
        and c.exp_ms > 0
        and c.kid != ""
        and c.token_sha256 != ""
        and c.instance_uid == auth.instance_uid
        and c.instance_epoch == auth.instance_epoch
        and battle_auth_record_v2_exact(auth)
        and c.writer_epoch == BATTLE_DS_WRITER_EPOCH_V2
        and auth.high_water_gen >= c.gen
        and battle.last_verified_gen == c.gen
        and _str_eq(battle.last_verified_jti, c.jti)
        and battle.last_verified_writer_epoch == c.writer_epoch
        and auth.last_active_heartbeat_ms > 0
        and battle.last_heartbeat_ms == auth.last_active_heartbeat_ms
    )


def battle_preactive_authority(auth, battle) -> bool:  # noqa: ANN001
    """"GSA 已确认、但尚无任何 active 凭据"的窗口。Go: `battlePreactiveAuthority`。

    该窗口从 persistent allocation fence 继承而来,Prepare/Stage/MarkDelivered
    都只能改变内容,**不能给 auth/battle 增加 TTL**;首个 `activate_heartbeat`
    才结束它。否则两键过期会重新开放同 match 的第二次 GSA POST。
    """
    return (
        auth is not None
        and battle is not None
        and not auth.HasField("active")
        and auth.phase == dspb.BATTLE_AUTH_PHASE_BOOTSTRAP
        and battle.state == "warming"
        and battle.last_verified_gen == 0
        and battle.last_verified_jti == ""
        and battle.last_verified_writer_epoch == 0
    )


def battle_projection_matches_credential(
    auth_record, battle_record, ident: BattleCredentialIdentity
) -> bool:  # noqa: ANN001
    """Go: `battleProjectionMatchesCredential`。"""
    active = _active_of(auth_record)
    return (
        authority_binding_matches(auth_record, battle_record)
        and active is not None
        and active.instance_uid == ident.instance_uid
        and active.instance_epoch == ident.instance_epoch
        and active.gen == ident.gen
        and _str_eq(active.jti, ident.jti)
        and active.writer_epoch == ident.writer_epoch
        and auth_record.high_water_gen >= ident.gen
        and battle_record.last_verified_gen == ident.gen
        and _str_eq(battle_record.last_verified_jti, ident.jti)
        and battle_record.last_verified_writer_epoch == ident.writer_epoch
    )


def complete_expected_battle_instance(expected: BattleExpectedInstance) -> bool:
    """Go: `completeExpectedBattleInstance`。"""
    return (
        expected.allocation_id != ""
        and expected.instance_uid != ""
        and expected.instance_epoch > 0
    )


def expected_battle_instance_matches(
    auth_record, battle, expected: BattleExpectedInstance
) -> bool:  # noqa: ANN001
    """Go: `expectedBattleInstanceMatches`。auth 与 battle 双侧都要与 expected 精确一致。"""
    return (
        complete_expected_battle_instance(expected)
        and authority_binding_matches(auth_record, battle)
        and auth_record.allocation_id == expected.allocation_id
        and battle.allocation_id == expected.allocation_id
        and auth_record.instance_uid == expected.instance_uid
        and battle.gameserver_uid == expected.instance_uid
        and auth_record.instance_epoch == expected.instance_epoch
        and battle.instance_epoch == expected.instance_epoch
    )


def valid_battle_result_authorization_proof(
    match_id: int,
    expected: BattleExpectedInstance,
    proof: BattleResultAuthorizationProof,
    now_ms_value: int,
) -> bool:
    """Go: `validBattleResultAuthorizationProof`。

    ★ `id.ExpMs <= uint64(1<<63-1)` 这一条在 Go 里是为了让下一行的
      `int64(id.ExpMs)` 不发生符号翻转;Python 没有溢出,但**必须保留**同样的
      域检查 —— 它同时是"能不能安全写进 receipt 的 int64 exp_ms 字段"的前置门。
    """
    ident = proof.credential
    return (
        match_id != 0
        and complete_expected_battle_instance(expected)
        and now_ms_value > 0
        and proof.authorized_at_ms > 0
        and proof.authorized_at_ms <= now_ms_value
        and 0 <= ident.exp_ms <= _INT64_MAX
        and proof.authorized_at_ms < ident.exp_ms
        and ident.pod_name != ""
        and ident.instance_uid == expected.instance_uid
        and ident.instance_epoch == expected.instance_epoch
        and ident.gen > 0
        and ident.jti != ""
        and ident.exp_ms > 0
        and ident.kid != ""
        and ident.token_sha256 != ""
        and ident.writer_epoch == BATTLE_DS_WRITER_EPOCH_V2
    )


def battle_result_stable_projection_matches(
    battle, expected: BattleExpectedInstance, proof: BattleResultAuthorizationProof
) -> bool:  # noqa: ANN001
    """Go: `battleResultStableProjectionMatches`。"""
    if battle is None or battle.state not in ("ready", "running", "ended"):
        return False
    ident = proof.credential
    return (
        battle.match_id != 0
        and battle.allocation_id == expected.allocation_id
        and battle.ds_pod_name == ident.pod_name
        and battle.gameserver_uid == expected.instance_uid
        and battle.instance_epoch == expected.instance_epoch
        and battle.last_verified_gen > 0
        and battle.last_verified_jti != ""
        and battle.last_verified_writer_epoch == BATTLE_DS_WRITER_EPOCH_V2
    )


def battle_result_stable_authority_matches(
    auth_record, battle, expected: BattleExpectedInstance
) -> bool:  # noqa: ANN001
    """Go: `battleResultStableAuthorityMatches`。"""
    if (
        not battle_auth_record_v2_exact(auth_record)
        or not expected_battle_instance_matches(auth_record, battle, expected)
        or auth_record.phase == dspb.BATTLE_AUTH_PHASE_QUARANTINED
        or not auth_record.HasField("active")
    ):
        return False
    active = auth_record.active
    if (
        active.gen == 0
        or active.jti == ""
        or active.exp_ms == 0
        or active.kid == ""
        or active.token_sha256 == ""
        or active.instance_uid != expected.instance_uid
        or active.instance_epoch != expected.instance_epoch
        or active.writer_epoch != BATTLE_DS_WRITER_EPOCH_V2
        or auth_record.high_water_gen < active.gen
    ):
        return False
    pending = _pending_of(auth_record)
    if pending is not None and (
        pending.instance_uid != expected.instance_uid
        or pending.instance_epoch != expected.instance_epoch
        or pending.writer_epoch != BATTLE_DS_WRITER_EPOCH_V2
    ):
        return False
    if auth_record.phase in (
        dspb.BATTLE_AUTH_PHASE_ACTIVE,
        dspb.BATTLE_AUTH_PHASE_ROTATING,
    ):
        # current active/projection 仍需自洽,但允许它已经从 outbox proof 的 gen/jti 轮换。
        return battle_active_projection_structurally_consistent(auth_record, battle)
    if auth_record.phase == dspb.BATTLE_AUTH_PHASE_TERMINATING:
        return battle.state == "ended"
    return False


def terminal_auth_from_result_proof(
    match_id: int, battle, proof: BattleResultAuthorizationProof, now_ms_value: int
):  # noqa: ANN001
    """callback auth TTL 早于 BattleTTL 时,重建"只可终止"的 auth 墓碑。

    Go: `terminalAuthFromResultProof`。phase=TERMINATING 保证旧/泄漏 token
    永远不能借此恢复写权限。
    """
    ident = proof.credential
    rec = dspb.BattleDSAuthStorageRecord(
        match_id=match_id,
        ds_pod_name=ident.pod_name,
        instance_uid=ident.instance_uid,
        instance_epoch=ident.instance_epoch,
        phase=dspb.BATTLE_AUTH_PHASE_TERMINATING,
        active=dspb.BattleDSCredential(
            gen=ident.gen,
            jti=ident.jti,
            exp_ms=ident.exp_ms,
            kid=ident.kid,
            instance_uid=ident.instance_uid,
            instance_epoch=ident.instance_epoch,
            token_sha256=ident.token_sha256,
            writer_epoch=ident.writer_epoch,
        ),
        high_water_gen=max(ident.gen, battle.last_verified_gen),
        updated_at_ms=now_ms_value,
        required_writer_epoch=BATTLE_DS_WRITER_EPOCH_V2,
        allocation_id=battle.allocation_id,
        last_active_heartbeat_ms=battle.last_heartbeat_ms,
    )
    return rec


def receipt_matches_result_proof(
    receipt: dsauthrecord.BattleResultReceipt,
    match_id: int,
    expected: BattleExpectedInstance,
    proof: BattleResultAuthorizationProof,
) -> bool:
    """Go: `receiptMatchesResultProof`。"""
    ident = proof.credential
    want = dsauthrecord.new_battle_result_receipt(
        match_id,
        expected.allocation_id,
        ident.pod_name,
        ident.instance_uid,
        ident.instance_epoch,
        ident.gen,
        ident.jti,
        ident.exp_ms,
        ident.kid,
        ident.token_sha256,
        ident.writer_epoch,
        receipt.recorded_at_ms,
    )
    return receipt.same_credential(want)


def roster_absent_ids(roster, census) -> list[int]:
    """roster 里没出现在 census 中的玩家(保持 roster 原序)。Go: `rosterAbsentIDs`。

    ★ 只在 `census_present` 为真时调用:census 缺席时返回「全员缺席」会把每一局
      都判弃,那道守卫必须留在调用点,本函数不替它兜底。
    """
    if not roster:
        return []
    present = set(census)
    return [pid for pid in roster if pid not in present]


def roster_gate_armable(allocated_at_ms: int, now_ms_value: int, arm_window_ms: int) -> bool:
    """判据是 battle 年龄,对任何副本同一答案。Go: `rosterGateArmable`。"""
    if arm_window_ms <= 0 or allocated_at_ms <= 0:
        return False
    age = now_ms_value - allocated_at_ms
    return 0 <= age <= arm_window_ms


def battle_activation_stability_pending(
    raw: bytes | str | None,
    ident: BattleCredentialIdentity,
    inp: BattleHeartbeatInput,
    now_ms_value: int,
) -> tuple[bool, str]:
    """推进激活稳定性证据并判定是否仍 pending。Go: `battleActivationStabilityPending`。

    返回 `(pending, 新证据 payload)`;门关闭时恒 `(False, "")`。

    ★ 身份不匹配(凭据轮换/同名新分配)时旧证据作废,从本拍重计 —— 绝不让旧实例的
      心跳历史给新实例代付稳定性证据。
    ★ 门开关判据是**与**不是**或**:写成 `or` 会让「beats=3 + span=0」这种只想按
      拍数判的配置被整道关掉,DS 首拍即激活(INC-20260727-001 第三 P0 的形状)。
    """
    if inp.stability_beats <= 1 and inp.stability_span_ms <= 0:
        return False, ""
    beats = max(int(inp.stability_beats), 1)
    first_ms, count = now_ms_value, 1
    if raw is not None:
        text = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else raw
        parts = text.split("|")
        if len(parts) == 4 and parts[0] == str(ident.gen) and _str_eq(parts[1], ident.jti):
            try:
                f = int(parts[2])
            except ValueError:
                f = 0
            if 0 < f <= now_ms_value:
                first_ms = f
            try:
                c = int(parts[3])
            except ValueError:
                c = 0
            if c > 0:
                count = c + 1
    pending = count < beats or now_ms_value - first_ms < inp.stability_span_ms
    payload = f"{ident.gen}|{ident.jti}|{first_ms}|{count}"
    return pending, payload


# ── BattleStorageRecord 写入侧不变量(Go: battle_storage_invariant.go)──────────
#
# strict 模式下每一次 battle 写都要过这道门:任何 writer 都不得发布一条"点名了
# 物理 GameServer 的某一部分、却缺 ABA 安全元组(含 pod_uid)"的记录。
# 本模块只用到 `marshal_battle_transition`,其余是它的依赖。


def _canonical_battle_identity_value(value: str) -> bool:
    """Go: `canonicalBattleIdentityValue`。非空、无首尾空白、无空白/控制字符。"""
    if value == "" or value.strip() != value:
        return False
    return not any(ch.isspace() or unicodedata.category(ch) == "Cc" for ch in value)


def canonical_battle_allocation_id(value: str) -> bool:
    """canonical 小写 UUIDv4(RFC4122 variant)。Go: `canonicalBattleAllocationID`。"""
    return bool(_CANONICAL_UUID4_RE.match(value))


def _battle_physical_identity_empty(record) -> bool:  # noqa: ANN001
    return (
        record.ds_pod_name == ""
        and record.ds_addr == ""
        and record.gameserver_uid == ""
        and record.pod_uid == ""
        and record.release_track == ""
        and record.instance_epoch == 0
    )


def _validate_exact_battle_physical_identity(record) -> None:  # noqa: ANN001
    if not _canonical_battle_identity_value(record.ds_pod_name):
        raise ValueError(f"battle state {record.state!r} requires canonical ds_pod_name")
    if not _canonical_battle_identity_value(record.gameserver_uid):
        raise ValueError(f"battle state {record.state!r} requires canonical gameserver_uid")
    if not _canonical_battle_identity_value(record.pod_uid):
        raise ValueError(f"battle state {record.state!r} requires canonical pod_uid")
    if not releasetrack.valid(record.release_track):
        raise ValueError(f"battle state {record.state!r} requires canonical release_track")


def _validate_battle_storage_write(record) -> None:  # noqa: ANN001
    """Go: `validateBattleStorageWrite`。"""
    if record is None:
        raise ValueError("nil battle storage record")
    if record.match_id == 0 or not canonical_battle_allocation_id(record.allocation_id):
        raise ValueError("battle storage record requires match_id and canonical UUIDv4 allocation_id")
    if _unknown_signature(record) != ():
        raise ValueError("new battle storage record cannot contain protobuf unknown fields")

    state = record.state
    if state in (
        "allocating",
        BATTLE_STATE_ALLOCATION_UNCERTAIN,
        BATTLE_STATE_ALLOCATION_RECONCILE_EMPTY_TOMBSTONE,
    ):
        if not _battle_physical_identity_empty(record):
            raise ValueError(f"battle state {state!r} cannot carry physical GameServer identity")
    elif state == "abandoned":
        if not _battle_physical_identity_empty(record):
            _validate_exact_battle_physical_identity(record)
    elif state in (
        "warming",
        "ready",
        "running",
        "ended",
        BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING,
        BATTLE_STATE_PREACTIVE_RELEASE_PENDING,
        BATTLE_STATE_ALLOCATION_ABORT_PENDING,
    ):
        _validate_exact_battle_physical_identity(record)
    else:
        raise ValueError(f"battle storage record has unsupported state {state!r}")


def _validate_existing_battle_storage_shape(record) -> None:  # noqa: ANN001
    if record is None:
        raise ValueError("nil battle storage record")
    known = clone(record)
    known.DiscardUnknownFields()
    _validate_battle_storage_write(known)


def _legacy_battle_missing_pod_uid(record) -> bool:  # noqa: ANN001
    """Go: `legacyBattleMissingPodUID`。唯一合法写法是同记录 pod_uid 精确补齐。"""
    if (
        record is None
        or record.pod_uid != ""
        or not _canonical_battle_identity_value(record.ds_pod_name)
        or not _canonical_battle_identity_value(record.gameserver_uid)
        or not releasetrack.valid(record.release_track)
    ):
        return False
    return record.state in (
        "warming",
        "ready",
        "running",
        "ended",
        "abandoned",
        BATTLE_STATE_ALLOCATION_RECONCILE_RELEASE_PENDING,
        BATTLE_STATE_PREACTIVE_RELEASE_PENDING,
        BATTLE_STATE_ALLOCATION_ABORT_PENDING,
    )


def validate_battle_storage_transition(previous, next_record) -> None:  # noqa: ANN001
    """Go: `validateBattleStorageTransition`。违反即抛 ValueError(由调用方包成 PandoraError)。"""
    if next_record is None:
        raise ValueError("nil next battle storage record")
    if previous is None:
        _validate_battle_storage_write(next_record)
        return
    if (
        previous.match_id != next_record.match_id
        or previous.allocation_id != next_record.allocation_id
    ):
        raise ValueError("battle storage transition changed match/allocation identity")

    if _legacy_battle_missing_pod_uid(previous):
        if not _canonical_battle_identity_value(next_record.pod_uid):
            raise ValueError("legacy battle physical identity may only backfill pod_uid")
        if _unknown_signature(previous) != _unknown_signature(next_record):
            raise ValueError("legacy battle pod_uid backfill changed protobuf unknown bytes")
        without_pod_uid = clone(next_record)
        without_pod_uid.pod_uid = ""
        if not proto_equal(previous, without_pod_uid):
            raise ValueError("legacy battle pod_uid backfill changed another field")
        _validate_existing_battle_storage_shape(next_record)
        return

    try:
        _validate_existing_battle_storage_shape(previous)
    except ValueError as exc:
        raise ValueError(f"unsafe existing battle storage record is not writable: {exc}") from exc
    if previous.pod_uid != "" and previous.pod_uid != next_record.pod_uid:
        raise ValueError("battle storage transition changed immutable pod_uid")
    if _unknown_signature(previous) != _unknown_signature(next_record):
        raise ValueError("battle storage transition changed protobuf unknown fields")
    _validate_existing_battle_storage_shape(next_record)


def marshal_battle_transition(previous, next_record) -> bytes:  # noqa: ANN001
    """Go: `marshalBattleTransition`。"""
    validate_battle_storage_transition(previous, next_record)
    return next_record.SerializeToString()


# ── 解码 ────────────────────────────────────────────────────────────────────


def unmarshal_battle_auth(match_id: int, payload: bytes, out) -> None:  # noqa: ANN001
    """Go: `unmarshalBattleAuth`。match 不符即整条拒(别的 match 的记录绝不能当本 match 用)。"""
    try:
        out.ParseFromString(bytes(payload))
    except Exception as exc:  # noqa: BLE001 —— proto 解码失败的具体类型随实现变化
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "battle %d bad auth proto: %s", match_id, exc
        ) from exc
    if out.match_id == 0:
        out.match_id = match_id
    if out.match_id != match_id:
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "battle %d auth id mismatch: %d", match_id, out.match_id
        )


def unmarshal_battle(match_id: int, payload: bytes):
    """Go: `unmarshalBattle`(battle.go)。同样的 match 身份校验。"""
    rec = dspb.BattleStorageRecord()
    try:
        rec.ParseFromString(bytes(payload))
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "battle %d bad proto: %s", match_id, exc
        ) from exc
    if rec.match_id == 0:
        rec.match_id = match_id
    if rec.match_id != match_id:
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "battle %d id mismatch: %d", match_id, rec.match_id
        )
    return rec


# ★ 逐字符照抄 Go 的 `battleAuthorityPairScript`。
#
# 为什么是 Lua 而不是 MGET(复审 P1-1):MGET 对**存在但类型错误**的 key 静默返回
# nil,会把 WRONGTYPE 伪装成"合法缺失"(fail-open,严重时凭错误前提继续写
# abandoned/release);Lua 内 redis.call('GET') 遇到非 string 键直接抛 WRONGTYPE,
# 错误语义与逐键 GET 完全一致。Redis Lua 里缺失键的 GET 返回 false(非 nil),
# {false, value} 不会截断表,两个位置始终齐全。
_BATTLE_AUTHORITY_PAIR_SCRIPT = "return {redis.call('GET', KEYS[1]), redis.call('GET', KEYS[2])}"


async def _read_authority_pair_atomic(pipe, match_id: int, a_key: str, b_key: str):
    """用**单条 Lua 命令**读取同槽 auth+battle 快照。Go: `readAuthorityPairAtomic`。

    两次独立 GET 之间的并发写(如首次 `activate_heartbeat` 的 EXEC)会造成撕裂快照:
    auth 已 ACTIVE 而 battle 仍是旧 warming 镜像,跨键一致性校验在 EXEC 之前就把它
    误判成 "active projection corrupt"(Go 侧 race 实测 8/50)—— WATCH 只保护到
    EXEC,保护不了闭包内的分次读。单条命令原子,撕裂读从根上不可能。

    返回值:任一键缺失返回对应 None(不设错),由调用方按语义处理。
    """
    res = await pipe.eval(_BATTLE_AUTHORITY_PAIR_SCRIPT, 2, a_key, b_key)
    if not isinstance(res, (list, tuple)) or len(res) != 2:
        raise errcode.PandoraError(
            errcode.ErrInvalidState,
            "battle %d authority pair read returned %s",
            match_id,
            type(res).__name__,
        )
    auth = None
    if isinstance(res[0], (bytes, bytearray, str)):
        auth = dspb.BattleDSAuthStorageRecord()
        unmarshal_battle_auth(match_id, _as_bytes(res[0]), auth)
    battle = None
    if isinstance(res[1], (bytes, bytearray, str)):
        battle = unmarshal_battle(match_id, _as_bytes(res[1]))
    return auth, battle


async def _read_bound_authority(pipe, match_id: int, a_key: str, b_key: str):
    """Go: `readBoundAuthority`。任一键缺失 = 权威不可读 → fail-closed。"""
    auth, battle = await _read_authority_pair_atomic(pipe, match_id, a_key, b_key)
    if auth is None or battle is None:
        raise BattleAuthStaleError
    return auth, battle


def _as_bytes(value) -> bytes:  # noqa: ANN001
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8", "surrogateescape")
    raise TypeError(f"unexpected redis value type {type(value).__name__}")


async def _read_battle_from(pipe, match_id: int, key: str):
    """Go: `readBattleFrom`。缺失 = 权威不可读 → fail-closed(不是"没分配")。"""
    raw = await pipe.get(key)
    if raw is None:
        raise BattleAuthStaleError
    return unmarshal_battle(match_id, raw)


async def _battle_result_receipt_matches(
    pipe, match_id: int, key: str, auth_record, battle_record, ident, now_ms_value: int
) -> bool:  # noqa: ANN001
    """Go: `battleResultReceiptMatches`。"""
    payload = await pipe.get(key)
    if payload is None:
        return False
    try:
        receipt = dsauthrecord.unmarshal_battle_result_receipt(payload)
    except ValueError:
        return False
    if not receipt.valid(now_ms_value) or ident.exp_ms > _INT64_MAX:
        return False
    expected = dsauthrecord.new_battle_result_receipt(
        match_id,
        auth_record.allocation_id,
        ident.pod_name,
        ident.instance_uid,
        ident.instance_epoch,
        ident.gen,
        ident.jti,
        ident.exp_ms,
        ident.kid,
        ident.token_sha256,
        ident.writer_epoch,
        receipt.recorded_at_ms,
    )
    return authority_binding_matches(auth_record, battle_record) and receipt.same_credential(
        expected
    )


def _set(pipe, key: str, payload, ttl_sec: float) -> None:  # noqa: ANN001
    """`SET key payload [PX ms]`。`ttl_sec<=0` = 无 TTL(Go 的 `time.Duration(0)`)。"""
    if ttl_sec and ttl_sec > 0:
        pipe.set(key, payload, px=int(ttl_sec * 1000))
    else:
        pipe.set(key, payload)


async def _cas_backoff(attempt: int) -> None:
    """WATCH/CAS 冲突后的退避。与 hub 侧 `ledger.cas_conflict_backoff` 同策略。"""
    import asyncio
    import random

    if attempt <= 0:
        return
    shift = min(attempt - 1, 4)
    base = 0.001 * (1 << shift)
    await asyncio.sleep(base / 2 + random.random() * base)  # noqa: S311 (非密码学用途)


# ── 仓 ──────────────────────────────────────────────────────────────────────


class RedisBattleAuthRepo:
    """Battle Model B 权威仓的 Redis 实现。Go: `data.RedisBattleAuthRepo`。

    ★ 每个方法都是一次有界 CAS 事务:`BATTLE_AUTH_CAS_RETRIES` 次 WATCH 冲突重读
      **不重放任何外部副作用**;耗尽仍 fail-closed(ErrInternal),绝不"尽力而为地
      写一半"。
    """

    __slots__ = ("_rdb", "_now_ms", "_strict_model_b_writes")

    def __init__(self, rdb, now_ms_fn: Callable[[], int] | None = None) -> None:  # noqa: ANN001
        self._rdb = rdb
        self._now_ms = now_ms_fn or now_ms
        self._strict_model_b_writes = False

    # ── strict 写入模式(Go: atomic.Bool)─────────────────────────────────────

    def enable_strict_model_b_writes(self) -> None:
        """Go: `EnableStrictModelBWrites`。"""
        self._strict_model_b_writes = True

    def strict_model_b_writes_enabled(self) -> bool:
        """Go: `StrictModelBWritesEnabled`。"""
        return self._strict_model_b_writes

    def _marshal_battle_transition(self, previous, next_record) -> bytes:  # noqa: ANN001
        """Go: `marshalBattleTransition` 方法版(未开 strict 时退回裸 Marshal)。"""
        if self._strict_model_b_writes:
            try:
                return marshal_battle_transition(previous, next_record)
            except ValueError as exc:
                raise errcode.PandoraError(
                    errcode.ErrInvalidState, "battle storage invariant failed: %s", exc, cause=exc
                ) from exc
        return next_record.SerializeToString()

    # ── PrepareCredential ────────────────────────────────────────────────────

    async def prepare_credential(self, binding: BattleAuthorityBinding) -> BattleCredentialSeed:
        """在 auth+battle+counter 同槽事务内完成实例绑定与取号。Go: `PrepareCredential`。

        原子绑定当前 GameServer UID、决定 instance_epoch,并从**永不过期**的 counter
        领取一个严格递增 gen。返回后签发失败允许跳号,**绝不回收/复用**。
        """
        _validate_battle_binding(binding)
        match_id = binding.match_id
        a_key, b_key = battle_auth_key(match_id), battle_key(match_id)
        g_key = battle_auth_gen_key(match_id)
        r_key = dsauthrecord.battle_result_receipt_key(match_id)
        watch_keys = (a_key, b_key, g_key, r_key)

        for attempt in range(BATTLE_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(*watch_keys)
                    if await pipe.exists(r_key):
                        raise BattleResultCommittedError
                    battle = await _read_battle_from(pipe, match_id, b_key)
                    battle_before = clone(battle)
                    if (
                        battle.allocation_id != binding.allocation_id
                        or battle.ds_pod_name != binding.pod_name
                        or (
                            battle.gameserver_uid != ""
                            and battle.gameserver_uid != binding.instance_uid
                        )
                        or not battle_credential_preparable_state(battle.state)
                    ):
                        raise BattleAuthStaleError

                    auth = dspb.BattleDSAuthStorageRecord()
                    a_raw = await pipe.get(a_key)
                    if a_raw is None:
                        auth.match_id = match_id
                        auth.ds_pod_name = binding.pod_name
                        auth.instance_uid = binding.instance_uid
                        auth.instance_epoch = 1
                        auth.phase = dspb.BATTLE_AUTH_PHASE_BOOTSTRAP
                        auth.allocation_id = binding.allocation_id
                    else:
                        unmarshal_battle_auth(match_id, a_raw, auth)
                        if (
                            auth.required_writer_epoch != BATTLE_DS_WRITER_EPOCH_V2
                            or not battle_stored_credential_epochs_v2(auth)
                        ):
                            raise BattleAuthStaleError
                        # QUARANTINED/TERMINATING 是实例级永久墓碑。必须**先于**
                        # same_instance 判断拒绝,否则同 match 下伪造一个不同 UID
                        # 会走"换实例"分支清空 tombstone,重新开放凭据签发。
                        if battle_auth_phase_locked(auth.phase):
                            raise BattleAuthStaleError
                        same_instance = (
                            auth.allocation_id == binding.allocation_id
                            and auth.ds_pod_name == binding.pod_name
                            and auth.instance_uid == binding.instance_uid
                        )
                        if not same_instance:
                            auth.instance_epoch += 1
                            if auth.instance_epoch > _UINT32_MAX:
                                # Go 是 uint32 自增回绕成 0,紧接着的 `== 0` 判溢出;
                                # Python 的 int 无限精度,必须显式检查,否则写出一个
                                # proto 装不下的 epoch(SerializeToString 直接报错)。
                                raise errcode.PandoraError(
                                    errcode.ErrInvalidState,
                                    "battle %d instance epoch overflow",
                                    match_id,
                                )
                            auth.match_id = match_id
                            auth.ds_pod_name = binding.pod_name
                            auth.instance_uid = binding.instance_uid
                            auth.allocation_id = binding.allocation_id
                            auth.phase = dspb.BATTLE_AUTH_PHASE_BOOTSTRAP
                            auth.ClearField("active")
                            auth.ClearField("pending")
                            auth.pending_started_ms = 0
                            auth.delivered_rv = ""
                            auth.last_active_heartbeat_ms = 0
                        elif not battle_auth_phase_stageable(auth.phase):
                            raise BattleAuthStaleError
                    if auth.instance_epoch == 0:
                        auth.instance_epoch = 1
                    if auth.required_writer_epoch == 0:
                        # 仅 auth key 不存在的首建路径可写入当前精确 epoch;已存在的
                        # 低/未来 record 已在上方拒绝,不能由普通分配路径承担隐式迁移。
                        auth.required_writer_epoch = binding.required_writer_epoch
                    if auth.required_writer_epoch != BATTLE_DS_WRITER_EPOCH_V2:
                        raise BattleAuthStaleError

                    counter = 0
                    g_raw = await pipe.get(g_key)
                    if g_raw is not None:
                        text = _as_bytes(g_raw).decode("utf-8", "replace")
                        try:
                            counter = int(text)
                        except ValueError as exc:
                            raise errcode.PandoraError(
                                errcode.ErrInvalidState,
                                "battle %d bad auth generation counter %r: %s",
                                match_id,
                                text,
                                exc,
                            ) from exc
                    counter = max(counter, auth.high_water_gen)
                    if counter >= _UINT64_MAX:
                        raise errcode.PandoraError(
                            errcode.ErrInvalidState,
                            "battle %d credential generation overflow",
                            match_id,
                        )
                    counter += 1

                    server_now_ms = self._now_ms()
                    auth.updated_at_ms = server_now_ms
                    battle.gameserver_uid = binding.instance_uid
                    battle.instance_epoch = auth.instance_epoch
                    # 换实例时必须清掉旧 active 投影;否则 warming 镜像会短暂伪装成
                    # 已验证实例。
                    if battle.last_verified_gen != 0 and (
                        battle.last_verified_gen != auth.active.gen
                        or battle.gameserver_uid != auth.instance_uid
                    ):
                        battle.last_verified_gen = 0
                        battle.last_verified_jti = ""
                        battle.last_verified_writer_epoch = 0
                    a_payload = auth.SerializeToString()
                    b_payload = self._marshal_battle_transition(battle_before, battle)
                    preactive = battle_preactive_authority(auth, battle)

                    a_ttl, b_ttl = binding.auth_ttl_sec, binding.battle_ttl_sec
                    if preactive:
                        # 从 GSA POST 前的永久 uncertain fence 到首个 active 心跳之间,
                        # 任何 auth/battle 写都必须继续无 TTL。否则 Stage/PATCH/进程
                        # 崩溃后两键过期,会重新开放同 match 的第二次 GSA POST。
                        a_ttl = b_ttl = 0.0
                    pipe.multi()
                    _set(pipe, a_key, a_payload, a_ttl)
                    _set(pipe, b_key, b_payload, b_ttl)
                    # 代际 counter 故意不设 TTL;auth 过期/删除也不能使 gen 回退。
                    _set(pipe, g_key, counter, 0.0)
                    await pipe.execute()
                    return BattleCredentialSeed(
                        instance_epoch=auth.instance_epoch, gen=counter
                    )
            except WatchError:
                await _cas_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrInternal, "battle %d prepare credential cas retry exhausted", match_id
        )

    # ── StagePending ─────────────────────────────────────────────────────────

    async def stage_pending(self, inp: BattleStageInput):
        """签发完成后暂存 pending 凭据。Go: `StagePending`。"""
        server_now_ms = self._now_ms()
        if inp.match_id == 0 or inp.allocation_id == "" or inp.auth_ttl_sec <= 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "battle auth stage requires match/allocation/auth_ttl"
            )
        validate_battle_credential(inp.credential, server_now_ms)
        match_id = inp.match_id
        a_key, b_key = battle_auth_key(match_id), battle_key(match_id)
        g_key = battle_auth_gen_key(match_id)
        r_key = dsauthrecord.battle_result_receipt_key(match_id)
        watch_keys = (a_key, b_key, g_key, r_key)

        for attempt in range(BATTLE_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(*watch_keys)
                    auth, battle = await _read_bound_authority(pipe, match_id, a_key, b_key)
                    if (
                        auth.allocation_id != inp.allocation_id
                        or not authority_binding_matches(auth, battle)
                        or battle_terminal(battle.state)
                        or not battle_auth_phase_stageable(auth.phase)
                        or auth.instance_uid != inp.credential.instance_uid
                        or auth.instance_epoch != inp.credential.instance_epoch
                        or inp.credential.writer_epoch != BATTLE_DS_WRITER_EPOCH_V2
                        or not battle_auth_record_v2_exact(auth)
                    ):
                        raise BattleAuthStaleError
                    receipt_exists = await pipe.exists(r_key)
                    if receipt_exists and not battle_credential_equal(
                        _active_of(auth), inp.credential
                    ):
                        raise BattleResultCommittedError
                    # 响应丢失可幂等重试;已经被同一凭据激活也视为此前 Stage 成功。
                    if battle_credential_equal(
                        _pending_of(auth), inp.credential
                    ) or battle_credential_equal(_active_of(auth), inp.credential):
                        out = clone(auth)
                        pipe.multi()
                        if battle_preactive_authority(auth, battle):
                            # 兼容事务响应丢失后的幂等重入:即使前一次写来自旧的有限
                            # TTL 实现,也先把两键恢复为永久 fence 再返回成功。
                            pipe.persist(a_key)
                            pipe.persist(b_key)
                        else:
                            pipe.exists(a_key, b_key)
                        await pipe.execute()
                        return out
                    # counter 永不过期并代表本 match 已领取的最新签发号。auth 曾被
                    # 失败清理删除时,仅比较新建记录的 high_water=0 会让旧 gen 重放;
                    # 要求待 Stage 的 gen **正好是**最新领取号,响应丢失后重新 Prepare
                    # 会自然淘汰更旧但尚未 Stage 的 candidate。
                    g_raw = await pipe.get(g_key)
                    if g_raw is None:
                        raise BattleAuthStaleError
                    try:
                        counter = int(_as_bytes(g_raw).decode("utf-8", "replace"))
                    except ValueError:
                        raise BattleAuthStaleError from None
                    if counter != inp.credential.gen:
                        raise BattleAuthStaleError
                    active = _active_of(auth)
                    if inp.credential.gen <= auth.high_water_gen or (
                        active is not None and inp.credential.gen <= active.gen
                    ):
                        raise BattleAuthStaleError

                    auth.pending.CopyFrom(inp.credential)
                    auth.high_water_gen = inp.credential.gen
                    auth.pending_started_ms = server_now_ms
                    auth.delivered_rv = ""
                    auth.phase = (
                        dspb.BATTLE_AUTH_PHASE_ROTATING
                        if auth.HasField("active")
                        else dspb.BATTLE_AUTH_PHASE_BOOTSTRAP
                    )
                    auth.updated_at_ms = server_now_ms
                    payload = auth.SerializeToString()
                    ttl = 0.0 if battle_preactive_authority(auth, battle) else inp.auth_ttl_sec
                    pipe.multi()
                    _set(pipe, a_key, payload, ttl)
                    if ttl == 0:
                        pipe.persist(b_key)
                    await pipe.execute()
                    return clone(auth)
            except WatchError:
                await _cas_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrInternal, "battle %d stage pending cas retry exhausted", match_id
        )

    # ── MarkDelivered ────────────────────────────────────────────────────────

    async def mark_delivered(
        self, match_id: int, allocation_id: str, expected, rv: str, auth_ttl_sec: float
    ) -> None:  # noqa: ANN001
        """只允许当前 expected pending 写 delivered_rv;旧 PATCH 的晚响应零变更。

        Go: `MarkDelivered`。
        """
        server_now_ms = self._now_ms()
        if match_id == 0 or allocation_id == "" or rv == "" or auth_ttl_sec <= 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle auth mark delivered requires match/allocation/rv/auth_ttl",
            )
        validate_battle_credential(expected, server_now_ms)
        a_key, b_key = battle_auth_key(match_id), battle_key(match_id)

        for attempt in range(BATTLE_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key)
                    auth, battle = await _read_bound_authority(pipe, match_id, a_key, b_key)
                    if (
                        auth.allocation_id != allocation_id
                        or not authority_binding_matches(auth, battle)
                        or not battle_auth_record_v2_exact(auth)
                        or not battle_auth_phase_stageable(auth.phase)
                        or not battle_credential_equal(_pending_of(auth), expected)
                    ):
                        raise BattleAuthStaleError
                    auth.delivered_rv = rv
                    auth.updated_at_ms = server_now_ms
                    payload = auth.SerializeToString()
                    ttl = 0.0 if battle_preactive_authority(auth, battle) else auth_ttl_sec
                    pipe.multi()
                    _set(pipe, a_key, payload, ttl)
                    if ttl == 0:
                        pipe.persist(b_key)
                    await pipe.execute()
                    return
            except WatchError:
                await _cas_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrInternal, "battle %d mark delivered cas retry exhausted", match_id
        )

    # ── ActivateHeartbeat(唯一线性化点)─────────────────────────────────────

    async def activate_heartbeat(  # noqa: C901, PLR0912, PLR0915 —— 与 Go 逐条对应
        self, match_id: int, ident: BattleCredentialIdentity, inp: BattleHeartbeatInput
    ) -> BattleActivateResult:
        """pending→active、服务端心跳时刻、battle ready/投影的**唯一线性化点**。

        Go: `ActivateHeartbeat`。
        """
        server_now_ms = self._now_ms()
        if (
            match_id == 0
            or inp.player_count < 0
            or inp.auth_ttl_sec <= 0
            or inp.battle_ttl_sec <= 0
            or not valid_battle_heartbeat_state(inp.state)
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle heartbeat requires match/ttls/player_count/state",
            )
        if not valid_battle_identity(ident, server_now_ms):
            raise _battle_auth_stale(
                match_id,
                ident,
                AUTH_REJECT_IDENTITY_INVALID,
                exp_ms=ident.exp_ms,
                server_now_ms=server_now_ms,
            )
        a_key, b_key = battle_auth_key(match_id), battle_key(match_id)
        r_key = dsauthrecord.battle_result_receipt_key(match_id)
        s_key = battle_activation_evidence_key(match_id)
        empty_timeout_ms = int(inp.empty_battle_timeout_sec * 1000)
        no_show_timeout_ms = int(inp.no_show_timeout_sec * 1000)
        roster_deadline_ms = int(inp.roster_join_deadline_sec * 1000)
        roster_arm_window_ms = int(inp.roster_join_arm_window_sec * 1000)

        for attempt in range(BATTLE_AUTH_CAS_RETRIES):
            server_now_ms = self._now_ms()
            if not valid_battle_identity(ident, server_now_ms):
                # 重试期间凭据过期:与入口那条同 reason,靠 attempt 字段区分。
                raise _battle_auth_stale(
                    match_id,
                    ident,
                    AUTH_REJECT_IDENTITY_INVALID,
                    exp_ms=ident.exp_ms,
                    server_now_ms=server_now_ms,
                    attempt=attempt,
                )
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key, r_key, s_key)
                    auth, battle = await _read_bound_authority(pipe, match_id, a_key, b_key)
                    battle_before = clone(battle)
                    if (
                        not authority_binding_matches(auth, battle)
                        or auth.ds_pod_name != ident.pod_name
                        or auth.instance_uid != ident.instance_uid
                        or auth.instance_epoch != ident.instance_epoch
                        or ident.writer_epoch != BATTLE_DS_WRITER_EPOCH_V2
                        or not battle_auth_record_v2_exact(auth)
                    ):
                        # 最常见的一条:pod 重建 / 重分配后旧 DS 还在心跳,或
                        # writer_epoch 不是 v2。把权威侧的实际值一并打出来,
                        # 才能一眼看出是哪一格对不上。
                        raise _battle_auth_stale(
                            match_id,
                            ident,
                            AUTH_REJECT_BINDING_MISMATCH,
                            auth_pod=auth.ds_pod_name,
                            auth_uid=auth.instance_uid,
                            auth_epoch=auth.instance_epoch,
                            auth_allocation_id=auth.allocation_id,
                            battle_allocation_id=battle.allocation_id,
                            battle_state=battle.state,
                        )
                    # 终态后的同一 active 凭据允许拿到 stop/ACK,但永不续心跳或刷新
                    # TTL;QUARANTINED 及其它锁定组合仍严格拒绝。
                    if auth.phase == dspb.BATTLE_AUTH_PHASE_TERMINATING:
                        if battle_terminal(battle.state) and battle_credential_matches(
                            _active_of(auth), ident, server_now_ms
                        ):
                            out = _activate_result(auth, battle, False, False, False, True)
                            pipe.multi()
                            pipe.exists(a_key, b_key)
                            await pipe.execute()
                            return out
                        raise _battle_auth_stale(
                            match_id,
                            ident,
                            AUTH_REJECT_TERMINATING_PHASE,
                            battle_state=battle.state,
                        )
                    if battle_auth_phase_locked(auth.phase):
                        raise _battle_auth_stale(
                            match_id,
                            ident,
                            AUTH_REJECT_PHASE_LOCKED,
                            phase=enum_name(dspb.BattleAuthPhase, auth.phase),
                            battle_state=battle.state,
                            hint="授权已被隔离/回收链锁死,该实例不可能再被授权;这台 DS 会收到 stop",
                        )

                    promote = False
                    pending, active = _pending_of(auth), _active_of(auth)
                    if (
                        auth.phase
                        in (
                            dspb.BATTLE_AUTH_PHASE_BOOTSTRAP,
                            dspb.BATTLE_AUTH_PHASE_ROTATING,
                        )
                        and auth.delivered_rv != ""
                        and battle_credential_complete(pending, auth, server_now_ms)
                        and battle_credential_matches(pending, ident, server_now_ms)
                    ):
                        promote = True
                    elif (
                        auth.phase
                        in (
                            dspb.BATTLE_AUTH_PHASE_ACTIVE,
                            dspb.BATTLE_AUTH_PHASE_ROTATING,
                        )
                        and battle_credential_complete(active, auth, server_now_ms)
                        and battle_credential_matches(active, ident, server_now_ms)
                    ):
                        promote = False
                    else:
                        raise _battle_auth_stale(
                            match_id,
                            ident,
                            AUTH_REJECT_NO_USABLE_CREDENTIAL,
                            phase=enum_name(dspb.BattleAuthPhase, auth.phase),
                            delivered_rv=auth.delivered_rv,
                            has_pending=pending is not None,
                            has_active=active is not None,
                            hint="凭据既不匹配 pending 也不匹配 active:多半是凭据投递未完成或 DS 拿的是上一代",
                        )
                    if auth.high_water_gen < ident.gen:
                        raise _battle_auth_stale(
                            match_id,
                            ident,
                            AUTH_REJECT_GEN_BELOW_HIGH_WATER,
                            high_water_gen=auth.high_water_gen,
                            hint="上报代际高于权威高水位 = 凭据不可能出自本权威",
                        )
                    if promote and await pipe.exists(r_key):
                        raise BattleResultCommittedError
                    if inp.state == "ended":
                        recorded = await _battle_result_receipt_matches(
                            pipe, match_id, r_key, auth, battle, ident, server_now_ms
                        )
                        if not recorded:
                            raise BattleResultNotRecordedError
                    if battle_terminal(battle.state):
                        if promote:
                            raise _battle_auth_stale(
                                match_id,
                                ident,
                                AUTH_REJECT_PROMOTE_ON_TERMINAL,
                                battle_state=battle.state,
                            )
                        auth.phase = dspb.BATTLE_AUTH_PHASE_TERMINATING
                        auth.updated_at_ms = server_now_ms
                        a_payload = auth.SerializeToString()
                        b_payload = None
                        if battle.state == "abandoned":
                            b_payload = self._marshal_battle_transition(battle_before, battle)
                        pipe.multi()
                        if battle.state == "abandoned":
                            _set(pipe, a_key, a_payload, 0.0)
                            _set(pipe, b_key, b_payload, 0.0)
                        else:
                            _set(pipe, a_key, a_payload, inp.auth_ttl_sec)
                        await pipe.execute()
                        return _activate_result(auth, battle, False, False, False, True)
                    if promote and inp.state not in ("ready", "running"):
                        raise _battle_auth_stale(
                            match_id,
                            ident,
                            AUTH_REJECT_STATE_NOT_READY_RUN,
                            reported_state=inp.state,
                            battle_state=battle.state,
                            hint="首次激活只接受 DS 自报 ready/running;其它值(含被白名单归一成空串的)一律拒",
                        )
                    if promote and not auth.HasField("active"):
                        # 两阶段激活稳定性门,**只作用于首次激活**:首拍只能证明
                        # "此刻活着",证据不足时零状态转移 —— 不提升 ACTIVE、不写
                        # auth/battle、不刷新 TTL、battle 保持 warming,仅原子推进同槽
                        # 证据键;响应无 ACK,DS 每 tick 幂等重试 staged 心跳。
                        # ROTATING 轮换发生在已被持续心跳证明存活的 ACTIVE 实例上,
                        # 不需要也不应该再付一次稳定性证据。
                        s_raw = await pipe.get(s_key)
                        stability_pending, evidence = battle_activation_stability_pending(
                            s_raw, ident, inp, server_now_ms
                        )
                        if stability_pending:
                            pipe.multi()
                            _set(pipe, s_key, evidence, inp.battle_ttl_sec)
                            await pipe.execute()
                            return BattleActivateResult(
                                activation_pending=True, battle=clone(battle)
                            )

                    # 与 promote 同一层作用域:WATCH 冲突重跑时整个闭包重来,
                    # 标记天然按轮重置。
                    roster_incomplete = False
                    roster_would_abandon = False
                    if promote:
                        auth.active.CopyFrom(auth.pending)
                        auth.ClearField("pending")
                        auth.pending_started_ms = 0
                        auth.delivered_rv = ""
                        auth.phase = dspb.BATTLE_AUTH_PHASE_ACTIVE
                    auth.updated_at_ms = server_now_ms
                    auth.last_active_heartbeat_ms = server_now_ms

                    previous_state = battle.state
                    battle.last_heartbeat_ms = server_now_ms
                    battle.player_count = inp.player_count
                    if inp.state != "":
                        battle.state = inp.state
                    if battle.state in ("ready", "running"):
                        # 双阈值空场回收:从未连入(ever_had_players=False)的局没有
                        # "谁要回来"的问题,走短阈值;有人连入过的局必须给断线重连
                        # 留路,走长阈值。ever_had_players 一经置位永不清零。
                        #
                        # no_show<=0 一律回退 empty_battle_timeout:未配置差异化时
                        # 必须退化成改动前的单阈值行为,绝不能让 timeout 变成 0 ——
                        # 那会让 no-show 局**永不回收**,比改动前更糟。
                        timeout_ms = empty_timeout_ms
                        if not battle.ever_had_players and no_show_timeout_ms > 0:
                            timeout_ms = no_show_timeout_ms
                        if inp.player_count > 0:
                            battle.empty_since_ms = 0
                            battle.ever_had_players = True
                        elif battle.empty_since_ms == 0:
                            battle.empty_since_ms = server_now_ms
                        elif (
                            timeout_ms > 0
                            and server_now_ms - battle.empty_since_ms >= timeout_ms
                        ):
                            battle.state = "abandoned"

                        # 花名册到齐期限。与上面的空场两档并列:那两档看
                        # player_count==0,本档看 census 与 roster 的差集 ——
                        # 「6 人 roster 只进来 5 个」时 player_count 恒为 5(非 0),
                        # 空场计时器一次都不会起。
                        #
                        # 只在开局阶段生效(not roster_ever_complete):曾经全员同时
                        # 在场过就永久豁免,此后的掉线交回 empty_battle_timeout。
                        # 少了这条,局中掉线会在 deadline 后判弃一场正打着的对局。
                        if (
                            battle.state != "abandoned"
                            and not battle.roster_ever_complete
                            and roster_deadline_ms > 0
                            and inp.census_present
                            and roster_gate_armable(
                                battle.allocated_at_ms, server_now_ms, roster_arm_window_ms
                            )
                        ):
                            if not roster_absent_ids(
                                list(battle.player_ids), list(inp.active_player_ids)
                            ):
                                battle.roster_ever_complete = True
                                battle.roster_incomplete_since_ms = 0
                            elif battle.roster_incomplete_since_ms == 0:
                                battle.roster_incomplete_since_ms = server_now_ms
                            elif (
                                server_now_ms - battle.roster_incomplete_since_ms
                                >= roster_deadline_ms
                            ):
                                # 到点。真判弃须过激活档 + 策略代(与 biz legacy 路径
                                # 共用同一谓词);不允许时只采证 —— observe 期与
                                # legacy/旧代局在这里被机制性豁免。
                                if not dsconf.roster_deadline_should_abandon(
                                    inp.roster_join_mode,
                                    inp.roster_policy_generation,
                                    battle.roster_policy_generation,
                                ):
                                    roster_would_abandon = True
                                else:
                                    battle.state = "abandoned"
                                    roster_incomplete = True

                    battle.gameserver_uid = auth.instance_uid
                    battle.instance_epoch = auth.instance_epoch
                    battle.last_verified_gen = auth.active.gen
                    battle.last_verified_jti = auth.active.jti
                    battle.last_verified_writer_epoch = auth.active.writer_epoch
                    first_abandon = previous_state != "abandoned" and battle.state == "abandoned"
                    terminal = battle_terminal(battle.state)
                    if terminal:
                        # ended/abandoned 与授权锁死必须是同一个 EXEC;否则终态窗口内
                        # GM/结果写仍可能凭 active token 产生副作用。
                        auth.phase = dspb.BATTLE_AUTH_PHASE_TERMINATING
                    a_payload = auth.SerializeToString()
                    b_payload = self._marshal_battle_transition(battle_before, battle)
                    a_ttl, b_ttl = inp.auth_ttl_sec, inp.battle_ttl_sec
                    if battle.state == "abandoned":
                        a_ttl = b_ttl = 0.0
                    pipe.multi()
                    _set(pipe, a_key, a_payload, a_ttl)
                    _set(pipe, b_key, b_payload, b_ttl)
                    if battle.state == "ended":
                        pipe.delete(r_key)
                    if promote:
                        # 提升成功即清稳定性证据键(与提升同一 EXEC,原子)。
                        pipe.delete(s_key)
                    await pipe.execute()
                    out = _activate_result(
                        auth, battle, promote, first_abandon, roster_incomplete, terminal
                    )
                    out.roster_would_abandon = roster_would_abandon
            except WatchError:
                await _cas_backoff(attempt)
                continue

            if out.terminal:
                # abandoned 的 active member 同时是 lifecycle outbox:投递成功前必须
                # 保留,让 sweep 能重投。正常 ended 不需要 abandoned 补偿,可直接
                # 移出扫描集。
                if out.battle.state == "ended":
                    await self._rdb.zrem(ACTIVE_KEY, str(match_id))
                return out
            # 全局索引跨 slot,只能在权威事务后幂等更新。失败时抛错让 DS 重试;
            # auth+battle 已原子成功,不会出现误分配,下一心跳会修复索引。
            await self._rdb.zadd(ACTIVE_KEY, {str(match_id): float(server_now_ms)})
            return out
        raise errcode.PandoraError(
            errcode.ErrInternal, "battle %d activate heartbeat cas retry exhausted", match_id
        )

    # ── ReadAuthority / CheckActive ──────────────────────────────────────────

    async def read_authority(self, match_id: int) -> BattleAuthoritySnapshot:
        """通过 WATCH + 空写事务取得 auth+battle 的一致快照。Go: `ReadAuthority`。"""
        if match_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "match_id required")
        a_key, b_key = battle_auth_key(match_id), battle_key(match_id)
        for attempt in range(BATTLE_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key)
                    out = BattleAuthoritySnapshot()
                    a_raw = await pipe.get(a_key)
                    if a_raw is not None:
                        out.auth = dspb.BattleDSAuthStorageRecord()
                        unmarshal_battle_auth(match_id, a_raw, out.auth)
                        out.auth_found = True
                    b_raw = await pipe.get(b_key)
                    if b_raw is not None:
                        out.battle = unmarshal_battle(match_id, b_raw)
                        out.battle_found = True
                    # ★ 只 WATCH 不 EXEC 不具备快照一致性:WATCH 在 EXEC 之前没有
                    #   任何线性化效果。Go 的只读路径同样刻意跑一次空事务。
                    pipe.multi()
                    pipe.exists(a_key, b_key)
                    await pipe.execute()
                    return out
            except WatchError:
                await _cas_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrInternal, "battle %d read authority cas retry exhausted", match_id
        )

    async def check_active(self, match_id: int, ident: BattleCredentialIdentity) -> None:
        """所有受保护 Battle DS 副作用 RPC 的前置门。Go: `CheckActive`。

        ★ 读之前与读之后各校验一次 exp:读取本身有耗时,只校验一次会让一个刚好在
          读期间过期的 token 通过。
        """
        server_now_ms = self._now_ms()
        if not valid_battle_identity(ident, server_now_ms):
            raise BattleAuthStaleError
        snapshot = await self.read_authority(match_id)
        server_now_ms = self._now_ms()
        if not valid_battle_identity(ident, server_now_ms):
            raise BattleAuthStaleError
        ok, _ = snapshot._active_projection_consistent(server_now_ms)  # noqa: SLF001
        if (
            not ok
            or snapshot.auth.ds_pod_name != ident.pod_name
            or not battle_credential_matches(_active_of(snapshot.auth), ident, server_now_ms)
        ):
            raise BattleAuthStaleError

    # ── PopCommandsIfActive ──────────────────────────────────────────────────

    async def pop_commands_if_active(
        self, match_id: int, ident: BattleCredentialIdentity, queue_key: str, max_count: int
    ) -> list[bytes]:
        """在 auth+queue 同槽事务里重新校验完整 active 元组后才 RPOP。

        Go: `PopCommandsIfActive`。WATCH 包含队列键,因此并发消费者、轮换或隔离
        发生时 EXEC 冲突并从最新 auth 重试;权威校验失败时**不会发送 MULTI/EXEC**,
        队列保持逐字节不变。
        """
        server_now_ms = self._now_ms()
        if (
            match_id == 0
            or max_count <= 0
            or max_count > 1000
            or queue_key == ""
            or not contains_battle_hash_tag(queue_key, match_id)
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "battle command pop requires match/same-slot queue/max"
            )
        if not valid_battle_identity(ident, server_now_ms):
            raise BattleAuthStaleError
        a_key, b_key = battle_auth_key(match_id), battle_key(match_id)

        for attempt in range(BATTLE_AUTH_CAS_RETRIES):
            server_now_ms = self._now_ms()
            if not valid_battle_identity(ident, server_now_ms):
                raise BattleAuthStaleError
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key, queue_key)
                    a_raw = await pipe.get(a_key)
                    if a_raw is None:
                        raise BattleAuthStaleError
                    auth = dspb.BattleDSAuthStorageRecord()
                    unmarshal_battle_auth(match_id, a_raw, auth)
                    b_raw = await pipe.get(b_key)
                    if b_raw is None:
                        raise BattleAuthStaleError
                    battle = unmarshal_battle(match_id, b_raw)
                    snapshot = BattleAuthoritySnapshot(
                        auth=auth, battle=battle, auth_found=True, battle_found=True
                    )
                    ok, _ = snapshot._active_projection_consistent(  # noqa: SLF001
                        server_now_ms
                    )
                    if not ok or battle_terminal(battle.state):
                        raise BattleAuthStaleError
                    active = _active_of(auth)
                    if (
                        auth.ds_pod_name != ident.pod_name
                        or auth.instance_uid != ident.instance_uid
                        or auth.instance_epoch != ident.instance_epoch
                        or auth.high_water_gen < ident.gen
                        or not battle_credential_complete(active, auth, server_now_ms)
                        or not battle_credential_matches(active, ident, server_now_ms)
                    ):
                        raise BattleAuthStaleError
                    pipe.multi()
                    pipe.rpop(queue_key, max_count)
                    results = await pipe.execute()
                    popped = results[0] if results else None
                    if popped is None:
                        return []
                    if isinstance(popped, (bytes, bytearray, str)):
                        return [_as_bytes(popped)]
                    return [_as_bytes(item) for item in popped]
            except WatchError:
                await _cas_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrInternal, "battle %d command pop cas retry exhausted", match_id
        )

    async def check_heartbeat_fresh(self, match_id: int, threshold_ms: int) -> bool:
        """Go: `CheckHeartbeatFresh`。"""
        snapshot = await self.read_authority(match_id)
        return snapshot.heartbeat_fresh(self._now_ms(), threshold_ms)

    # ── QuarantineExpected ───────────────────────────────────────────────────

    async def quarantine_expected(
        self,
        match_id: int,
        expected: BattleQuarantineExpected,
        auth_ttl_sec: float,
        battle_ttl_sec: float,
    ) -> BattleQuarantineResult:
        """泄露 token 的紧急路径。Go: `QuarantineExpected`。

        同槽原子锁 auth、清 pending,并把 battle 转 abandoned 进入可靠补偿 outbox。
        墓碑**永久保留**;即使投影漂移无法安全改 state,也会 PERSIST 现有 battle,
        防两键过期后不同 UID 通过 Prepare 重建授权。普通平滑轮换仍走 ROTATING。
        """
        server_now_ms = self._now_ms()
        if (
            match_id == 0
            or expected.allocation_id == ""
            or not valid_battle_identity(expected.credential, server_now_ms)
            or auth_ttl_sec <= 0
            or battle_ttl_sec <= 0
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle quarantine requires full expected credential/allocation/ttls",
            )
        a_key, b_key = battle_auth_key(match_id), battle_key(match_id)

        for attempt in range(BATTLE_AUTH_CAS_RETRIES):
            result = BattleQuarantineResult()
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key)
                    auth_raw = await pipe.get(a_key)
                    if auth_raw is None:
                        return result
                    auth_record = dspb.BattleDSAuthStorageRecord()
                    unmarshal_battle_auth(match_id, auth_raw, auth_record)
                    if (
                        not battle_auth_record_v2_exact(auth_record)
                        or auth_record.allocation_id != expected.allocation_id
                        or auth_record.ds_pod_name != expected.credential.pod_name
                        or auth_record.instance_uid != expected.credential.instance_uid
                        or auth_record.instance_epoch != expected.credential.instance_epoch
                        or not battle_credential_matches(
                            _active_of(auth_record), expected.credential, server_now_ms
                        )
                        or auth_record.phase
                        not in (
                            dspb.BATTLE_AUTH_PHASE_ACTIVE,
                            dspb.BATTLE_AUTH_PHASE_ROTATING,
                            dspb.BATTLE_AUTH_PHASE_QUARANTINED,
                        )
                    ):
                        return result
                    battle_record = None
                    battle_raw = await pipe.get(b_key)
                    if battle_raw is not None:
                        battle_record = unmarshal_battle(match_id, battle_raw)
                    battle_before = None
                    if battle_record is not None:
                        battle_before = clone(battle_record)
                        if self._strict_model_b_writes:
                            try:
                                validate_battle_storage_transition(battle_record, battle_record)
                            except ValueError as exc:
                                raise errcode.PandoraError(
                                    errcode.ErrInvalidState,
                                    "battle %d quarantine storage invariant failed: %s",
                                    match_id,
                                    exc,
                                    cause=exc,
                                ) from exc
                    projection_matches = battle_projection_matches_credential(
                        auth_record, battle_record, expected.credential
                    )
                    auth_record.phase = dspb.BATTLE_AUTH_PHASE_QUARANTINED
                    auth_record.ClearField("pending")
                    auth_record.pending_started_ms = 0
                    auth_record.delivered_rv = ""
                    auth_record.updated_at_ms = server_now_ms
                    if projection_matches and not battle_terminal(battle_record.state):
                        battle_record.state = "abandoned"
                    auth_payload = auth_record.SerializeToString()
                    battle_payload = None
                    if projection_matches:
                        battle_payload = self._marshal_battle_transition(
                            battle_before, battle_record
                        )
                    pipe.multi()
                    _set(pipe, a_key, auth_payload, 0.0)
                    if battle_payload is not None:
                        _set(pipe, b_key, battle_payload, 0.0)
                    elif battle_record is not None:
                        pipe.persist(b_key)
                    await pipe.execute()
                    result.auth_quarantined = True
                    result.projection_abandoned = projection_matches
            except WatchError:
                await _cas_backoff(attempt)
                continue

            if not result.auth_quarantined or not result.projection_abandoned:
                return result
            # 全局 active 是可靠补偿 outbox;score=0 令下一次 sweep 立即对账/回收。
            await self._rdb.zadd(ACTIVE_KEY, {str(match_id): 0.0})
            return result
        raise errcode.PandoraError(
            errcode.ErrInternal, "battle %d quarantine cas retry exhausted", match_id
        )

    # ── AbandonIfStale ───────────────────────────────────────────────────────

    async def abandon_if_stale(  # noqa: C901, PLR0912, PLR0915 —— 与 Go 逐条对应
        self,
        match_id: int,
        cutoffs: BattleStaleCutoffs,
        auth_ttl_sec: float,
        battle_ttl_sec: float,
    ) -> BattleAbandonResult:
        """把 sweep 的二次核验与 active→TERMINATING / battle→abandoned 放进同槽事务。

        Go: `AbandonIfStale`。并发新心跳会使 WATCH 失败;重试读取新 heartbeat 后
        返回不判弃。阈值在事务内按同一权威快照选择,首次 `activate_heartbeat` 与本
        函数并发时由 WATCH 保证只有一方生效 —— 不存在"激活成功又被弃"。
        """
        if (
            match_id == 0
            or cutoffs.active_heartbeat_ms <= 0
            or cutoffs.warming_heartbeat_ms <= 0
            or auth_ttl_sec <= 0
            or battle_ttl_sec <= 0
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "battle abandon requires match/cutoffs/ttls"
            )
        f = cutoffs.warming_forfeit
        if f is not None and (
            f.heartbeat_ms <= 0
            or f.instance.allocation_id == ""
            or f.instance.instance_uid == ""
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle abandon warming forfeit requires probed instance identity and timestamp",
            )
        a_key, b_key = battle_auth_key(match_id), battle_key(match_id)

        for attempt in range(BATTLE_AUTH_CAS_RETRIES):
            out = BattleAbandonResult()
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key)
                    # 单条 Lua 原子读:两键分次 GET 会与首次 activate_heartbeat 撕裂。
                    auth, battle = await _read_authority_pair_atomic(
                        pipe, match_id, a_key, b_key
                    )
                    if battle is None:
                        # 与 _read_battle_from 的缺失语义一致:权威不可读,fail-closed。
                        raise BattleAuthStaleError
                    battle_before = clone(battle)
                    out.auth_found = auth is not None
                    out.active_found = auth is not None and auth.HasField("active")
                    out.battle = clone(battle)

                    if auth is None:
                        # PrepareCredential 之前 Redis/K8s 半成功可能只有 warming
                        # battle。auth 缺失本身不可授权;同一 WATCH 仍可按 allocation
                        # grace 安全判弃。若并发 Prepare 建 auth,EXEC 必冲突并重读,
                        # 不能误杀刚激活实例。
                        if battle_terminal(battle.state):
                            out.already_terminal = True
                            pipe.multi()
                            if battle.state == "abandoned":
                                payload = self._marshal_battle_transition(battle_before, battle)
                                _set(pipe, b_key, payload, 0.0)
                            else:
                                pipe.exists(a_key, b_key)
                            await pipe.execute()
                            return out
                        if battle.state not in ("allocating", "warming"):
                            raise errcode.PandoraError(
                                errcode.ErrInvalidState,
                                "battle %d missing auth outside allocation grace",
                                match_id,
                            )
                        # warming 是冷加载窗口(无业务心跳属正常),用 ready 等待阈值
                        # (判死 forfeit 须与本事务快照的 exact 身份一致才生效);
                        # allocating 尚无外部实例交付,保持快速清理语义。
                        cutoff_ms = cutoffs.active_heartbeat_ms
                        if battle.state == "warming":
                            cutoff_ms = cutoffs.warming_cutoff_for(battle)
                        if battle.last_heartbeat_ms > cutoff_ms:
                            pipe.multi()
                            pipe.exists(a_key, b_key)
                            await pipe.execute()
                            return out
                        battle.state = "abandoned"
                        b_payload = self._marshal_battle_transition(battle_before, battle)
                        pipe.multi()
                        _set(pipe, b_key, b_payload, 0.0)
                        await pipe.execute()
                        out.abandoned = True
                        out.battle = clone(battle)
                        return out

                    # 这两条挡住的是 sweep 的**判弃权**:拒了就代表这局既不会被判弃、
                    # 也不会回收 GameServer(fail-closed,方向正确),但只回一个
                    # ErrUnauthorized 时分不清是记录格式旧还是 auth↔battle 串了。
                    if not battle_auth_record_v2_exact(auth):
                        plog.get().warning(
                            "battle_sweep_authority_rejected",
                            match_id=match_id,
                            reason=AUTH_REJECT_BINDING_MISMATCH,
                            detail="auth_record_not_v2_exact",
                            pod=battle.ds_pod_name,
                            battle_state=battle.state,
                            auth_phase=enum_name(dspb.BattleAuthPhase, auth.phase),
                            hint="本局不会被判弃也不会回收 GS(fail-closed);持续出现即 GS 占位泄漏来源",
                        )
                        raise BattleAuthStaleError(reason=AUTH_REJECT_BINDING_MISMATCH)
                    if not authority_binding_matches(auth, battle):
                        plog.get().warning(
                            "battle_sweep_authority_rejected",
                            match_id=match_id,
                            reason=AUTH_REJECT_BINDING_MISMATCH,
                            detail="auth_battle_binding_mismatch",
                            pod=battle.ds_pod_name,
                            auth_pod=auth.ds_pod_name,
                            auth_uid=auth.instance_uid,
                            auth_allocation_id=auth.allocation_id,
                            battle_allocation_id=battle.allocation_id,
                            battle_state=battle.state,
                        )
                        raise BattleAuthStaleError(reason=AUTH_REJECT_BINDING_MISMATCH)

                    if battle_terminal(battle.state):
                        out.already_terminal = True
                        if auth.phase not in (
                            dspb.BATTLE_AUTH_PHASE_TERMINATING,
                            dspb.BATTLE_AUTH_PHASE_QUARANTINED,
                        ):
                            # 防旧 writer/历史半状态只把 battle 写终态却仍留下 active 授权。
                            auth.phase = dspb.BATTLE_AUTH_PHASE_TERMINATING
                            auth.updated_at_ms = self._now_ms()
                            a_payload = auth.SerializeToString()
                            if battle.state != "abandoned":
                                pipe.multi()
                                pipe.set(a_key, a_payload, keepttl=True)
                                await pipe.execute()
                                return out
                        if battle.state == "abandoned":
                            a_payload = auth.SerializeToString()
                            b_payload = self._marshal_battle_transition(battle_before, battle)
                            pipe.multi()
                            _set(pipe, a_key, a_payload, 0.0)
                            _set(pipe, b_key, b_payload, 0.0)
                            await pipe.execute()
                            return out
                        pipe.multi()
                        pipe.exists(a_key, b_key)
                        await pipe.execute()
                        return out
                    if auth.phase == dspb.BATTLE_AUTH_PHASE_TERMINATING:
                        raise errcode.PandoraError(
                            errcode.ErrInvalidState,
                            "battle %d terminating auth with non-terminal battle",
                            match_id,
                        )

                    if auth.HasField("active"):
                        if not battle_active_projection_structurally_consistent(auth, battle):
                            raise errcode.PandoraError(
                                errcode.ErrInvalidState,
                                "battle %d active projection corrupt",
                                match_id,
                            )
                        # 已激活过的实例(含 ready/running/rotating)一律按业务心跳
                        # 阈值,停跳即回收,不因 warming 宽限延后。
                        stale = auth.last_active_heartbeat_ms <= cutoffs.active_heartbeat_ms
                    elif auth.phase == dspb.BATTLE_AUTH_PHASE_BOOTSTRAP and battle.state in (
                        "allocating",
                        "warming",
                    ):
                        # 尚未激活的分配只认后端写入的 allocation grace 时间;
                        # pending 本身不能续命。
                        cutoff_ms = cutoffs.active_heartbeat_ms
                        if battle.state == "warming":
                            cutoff_ms = cutoffs.warming_cutoff_for(battle)
                        stale = battle.last_heartbeat_ms <= cutoff_ms
                    else:
                        raise errcode.PandoraError(
                            errcode.ErrInvalidState,
                            "battle %d auth state cannot be swept safely",
                            match_id,
                        )
                    if not stale:
                        pipe.multi()
                        pipe.exists(a_key, b_key)
                        await pipe.execute()
                        return out

                    server_now_ms = self._now_ms()
                    auth.phase = dspb.BATTLE_AUTH_PHASE_TERMINATING
                    auth.updated_at_ms = server_now_ms
                    battle.state = "abandoned"
                    a_payload = auth.SerializeToString()
                    b_payload = self._marshal_battle_transition(battle_before, battle)
                    pipe.multi()
                    # 外部 UID 条件 Release 尚未开始;两键必须先成为永久墓碑。
                    # Release 与 lifecycle 都明确成功后,expire_terminated_expected
                    # 才恢复 TTL。
                    _set(pipe, a_key, a_payload, 0.0)
                    _set(pipe, b_key, b_payload, 0.0)
                    await pipe.execute()
                    out.abandoned = True
                    out.battle = clone(battle)
                    # 不 ZREM:active member 是 abandoned lifecycle 的 Redis outbox。
                    return out
            except WatchError:
                await _cas_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrInternal, "battle %d abandon cas retry exhausted", match_id
        )

    # ── TerminateExpected ────────────────────────────────────────────────────

    async def terminate_expected(
        self,
        match_id: int,
        expected: BattleExpectedInstance,
        terminal_state: str,
        auth_ttl_sec: float,
        battle_ttl_sec: float,
    ) -> bool:
        """仅在 allocation_id 仍为调用方持有的实例时锁死授权并写终态。

        Go: `TerminateExpected`。这是外部 ReleaseExpected 之前的**永久线性化点**:
        两键无 TTL,Release 失败/响应未知时墓碑不会自行消失。
        """
        if (
            match_id == 0
            or not complete_expected_battle_instance(expected)
            or auth_ttl_sec <= 0
            or battle_ttl_sec <= 0
            or terminal_state not in ("ended", "abandoned")
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle terminate requires expected allocation and terminal state",
            )
        a_key, b_key = battle_auth_key(match_id), battle_key(match_id)
        r_key = dsauthrecord.battle_result_receipt_key(match_id)

        for attempt in range(BATTLE_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key, r_key)
                    try:
                        auth, battle = await _read_bound_authority(pipe, match_id, a_key, b_key)
                    except BattleAuthStaleError:
                        # Go: `errcode.As(err) == ErrUnauthorized` → 零副作用返回 false。
                        return False
                    battle_before = clone(battle)
                    if not battle_auth_record_v2_exact(
                        auth
                    ) or not expected_battle_instance_matches(auth, battle, expected):
                        return False
                    already_terminated = auth.phase == dspb.BATTLE_AUTH_PHASE_TERMINATING and (
                        battle_terminal(battle.state)
                    )
                    if terminal_state == "ended" and not already_terminated:
                        active = _active_of(auth)
                        ident = BattleCredentialIdentity(
                            pod_name=auth.ds_pod_name,
                            instance_uid=active.instance_uid if active is not None else "",
                            instance_epoch=active.instance_epoch if active is not None else 0,
                            gen=active.gen if active is not None else 0,
                            jti=active.jti if active is not None else "",
                            exp_ms=active.exp_ms if active is not None else 0,
                            kid=active.kid if active is not None else "",
                            token_sha256=active.token_sha256 if active is not None else "",
                            writer_epoch=active.writer_epoch if active is not None else 0,
                        )
                        recorded = await _battle_result_receipt_matches(
                            pipe, match_id, r_key, auth, battle, ident, self._now_ms()
                        )
                        if not recorded:
                            raise BattleResultNotRecordedError
                    auth.phase = dspb.BATTLE_AUTH_PHASE_TERMINATING
                    auth.updated_at_ms = self._now_ms()
                    if not battle_terminal(battle.state):
                        battle.state = terminal_state
                    a_payload = auth.SerializeToString()
                    b_payload = self._marshal_battle_transition(battle_before, battle)
                    pipe.multi()
                    _set(pipe, a_key, a_payload, 0.0)
                    _set(pipe, b_key, b_payload, 0.0)
                    pipe.delete(r_key)
                    await pipe.execute()
                    # active ZSET 在这里还是"外部 release + lifecycle"待办索引;
                    # 只有 Purge/Expire 明确完成后才能移除。提前 ZREM 会让进程崩溃时
                    # 永久墓碑失去自动重入入口(安全仍在,但回收/补偿永远停滞)。
                    return True
            except WatchError:
                await _cas_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrInternal, "battle %d terminate cas retry exhausted", match_id
        )

    # ── TerminateResultExpected ──────────────────────────────────────────────

    async def terminate_result_expected(
        self,
        match_id: int,
        expected: BattleExpectedInstance,
        proof: BattleResultAuthorizationProof,
    ) -> bool:
        """正常结算 outbox 的 Redis 线性化点。Go: `TerminateResultExpected`。

        与 `terminate_expected` 不同,它不要求 relay 时 callback token 仍未过期,
        也不要求 outbox 的 gen/jti 仍是 current active —— MySQL 行证明该凭据在
        `authorized_at_ms` 已通过服务端校验。当前 Redis stable identity 与 writer
        fence 必须一致,allocation/UID 漂移零副作用。
        """
        if self._rdb is None or self._now_ms is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle terminal result authority unavailable"
            )
        server_now_ms = self._now_ms()
        if not valid_battle_result_authorization_proof(
            match_id, expected, proof, server_now_ms
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "battle terminal result proof is incomplete"
            )
        a_key, b_key = battle_auth_key(match_id), battle_key(match_id)
        r_key = dsauthrecord.battle_result_receipt_key(match_id)

        for attempt in range(BATTLE_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key, r_key)
                    battle_raw = await pipe.get(b_key)
                    if battle_raw is None:
                        raise errcode.PandoraError(
                            errcode.ErrInvalidState,
                            "battle %d projection missing before terminal release",
                            match_id,
                        )
                    battle = unmarshal_battle(match_id, battle_raw)
                    battle_before = clone(battle)
                    if (
                        not battle_result_stable_projection_matches(battle, expected, proof)
                        or battle.state == "abandoned"
                    ):
                        raise errcode.PandoraError(
                            errcode.ErrUnauthorized,
                            "battle %d stable identity changed before terminal release",
                            match_id,
                        )

                    auth_raw = await pipe.get(a_key)
                    if auth_raw is None:
                        # callback auth TTL 可以早于 BattleTTL。只在 projection 仍携带
                        # 精确 stable identity + writer=2 时重建"只可终止"的 auth 墓碑;
                        # phase=TERMINATING 保证旧/泄漏 token 永远不能借此恢复写权限。
                        auth_record = terminal_auth_from_result_proof(
                            match_id, battle, proof, server_now_ms
                        )
                    else:
                        auth_record = dspb.BattleDSAuthStorageRecord()
                        unmarshal_battle_auth(match_id, auth_raw, auth_record)
                        if not battle_result_stable_authority_matches(
                            auth_record, battle, expected
                        ):
                            raise errcode.PandoraError(
                                errcode.ErrUnauthorized,
                                "battle %d authority changed before terminal release",
                                match_id,
                            )

                    recorded_at_ms = proof.authorized_at_ms
                    receipt = dsauthrecord.new_battle_result_receipt(
                        match_id,
                        expected.allocation_id,
                        proof.credential.pod_name,
                        proof.credential.instance_uid,
                        proof.credential.instance_epoch,
                        proof.credential.gen,
                        proof.credential.jti,
                        proof.credential.exp_ms,
                        proof.credential.kid,
                        proof.credential.token_sha256,
                        proof.credential.writer_epoch,
                        recorded_at_ms,
                    )
                    old_raw = await pipe.get(r_key)
                    if old_raw is not None:
                        decode_failed = False
                        try:
                            old = dsauthrecord.unmarshal_battle_result_receipt(old_raw)
                        except ValueError:
                            decode_failed = True
                            old = None
                        if (
                            decode_failed
                            or not old.valid(server_now_ms)
                            or not old.same_credential(receipt)
                        ):
                            raise errcode.PandoraError(
                                errcode.ErrUnauthorized,
                                "battle %d terminal receipt belongs to another proof",
                                match_id,
                            )
                        # immediate receipt 可能在 DB commit 后写入,保留其真实 recorded_at。
                        receipt.recorded_at_ms = old.recorded_at_ms

                    auth_record.phase = dspb.BATTLE_AUTH_PHASE_TERMINATING
                    auth_record.ClearField("pending")
                    auth_record.pending_started_ms = 0
                    auth_record.delivered_rv = ""
                    auth_record.updated_at_ms = server_now_ms
                    battle.state = "ended"
                    a_payload = auth_record.SerializeToString()
                    b_payload = self._marshal_battle_transition(battle_before, battle)
                    r_payload = dsauthrecord.marshal_battle_result_receipt(receipt)
                    pipe.multi()
                    # UID Release 结果明确前,三键均为永久墓碑;任何 timeout/崩溃都可重入。
                    _set(pipe, a_key, a_payload, 0.0)
                    _set(pipe, b_key, b_payload, 0.0)
                    _set(pipe, r_key, r_payload, 0.0)
                    await pipe.execute()
                    return True
            except WatchError:
                await _cas_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrInternal, "battle %d terminal result cas retry exhausted", match_id
        )

    # ── ExpireResultTerminatedExpected ───────────────────────────────────────

    async def expire_result_terminated_expected(
        self,
        match_id: int,
        expected: BattleExpectedInstance,
        proof: BattleResultAuthorizationProof,
        retention_sec: float,
    ) -> bool:
        """只在 K8s UID 条件删除明确成功、且 battle_result 已 durable CAS 后调用。

        Go: `ExpireResultTerminatedExpected`。只确认同 proof 并恢复 tombstone TTL,
        绝不再次触碰 Kubernetes;response 丢失后可安全重放。
        """
        if (
            self._rdb is None
            or self._now_ms is None
            or retention_sec <= 0
            or not valid_battle_result_authorization_proof(
                match_id, expected, proof, self._now_ms()
            )
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle terminal result expire requires proof and retention",
            )
        a_key, b_key = battle_auth_key(match_id), battle_key(match_id)
        r_key = dsauthrecord.battle_result_receipt_key(match_id)

        for attempt in range(BATTLE_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key, r_key)
                    auth_raw = await pipe.get(a_key)
                    battle_raw = await pipe.get(b_key)
                    receipt_raw = await pipe.get(r_key)
                    if auth_raw is None and battle_raw is None and receipt_raw is None:
                        # 上一次 finalize 可能已经成功但响应丢失,MySQL released 行尚未
                        # DELETE;TTL 到期后三键全无等价于 cleanup 已完成,按幂等成功
                        # 重认。仍通过 WATCH+EXEC no-op 锁定"确实同时为空"的线性化快照。
                        pipe.multi()
                        pipe.exists(a_key, b_key, r_key)
                        await pipe.execute()
                        expired = True
                    elif auth_raw is None or battle_raw is None or receipt_raw is None:
                        raise errcode.PandoraError(
                            errcode.ErrInvalidState,
                            "battle %d terminal tombstone partially missing",
                            match_id,
                        )
                    else:
                        auth_record = dspb.BattleDSAuthStorageRecord()
                        unmarshal_battle_auth(match_id, auth_raw, auth_record)
                        battle = unmarshal_battle(match_id, battle_raw)
                        if (
                            not battle_result_stable_authority_matches(
                                auth_record, battle, expected
                            )
                            or auth_record.phase != dspb.BATTLE_AUTH_PHASE_TERMINATING
                            or battle.state != "ended"
                        ):
                            raise errcode.PandoraError(
                                errcode.ErrUnauthorized,
                                "battle %d terminal tombstone changed",
                                match_id,
                            )
                        receipt_bad = False
                        try:
                            receipt = dsauthrecord.unmarshal_battle_result_receipt(receipt_raw)
                        except ValueError:
                            receipt_bad = True
                            receipt = None
                        if (
                            receipt_bad
                            or not receipt.valid(self._now_ms())
                            or not receipt_matches_result_proof(
                                receipt, match_id, expected, proof
                            )
                        ):
                            raise errcode.PandoraError(
                                errcode.ErrUnauthorized,
                                "battle %d terminal receipt changed",
                                match_id,
                            )
                        px = int(retention_sec * 1000)
                        pipe.multi()
                        pipe.pexpire(a_key, px)
                        pipe.pexpire(b_key, px)
                        pipe.pexpire(r_key, px)
                        await pipe.execute()
                        expired = True
            except WatchError:
                await _cas_backoff(attempt)
                continue

            if not expired:
                return False
            await self._rdb.zrem(ACTIVE_KEY, str(match_id))
            return True
        raise errcode.PandoraError(
            errcode.ErrInternal, "battle %d terminal result expire cas retry exhausted", match_id
        )

    # ── FencePreactiveReleaseExpected ────────────────────────────────────────

    async def fence_preactive_release_expected(
        self, match_id: int, expected: BattleExpectedInstance
    ) -> bool:
        """未激活分配回收的线性化点。Go: `FencePreactiveReleaseExpected`。

        它不删除任何权威键,而是先把 battle 置为不可路由的 release-pending,并把已有
        auth 锁成 TERMINATING;两键在同一 EXEC 内写成**永久**。外部 ReleaseExpected
        超时/响应未知时,该墓碑会一直阻止同 match 第二次 GSA POST,直到一次幂等删除
        得到明确成功。
        """
        if match_id == 0 or expected.allocation_id == "" or expected.instance_uid == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle preactive release fence requires allocation_id and instance_uid",
            )
        a_key, b_key = battle_auth_key(match_id), battle_key(match_id)

        for attempt in range(BATTLE_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key)
                    try:
                        battle = await _read_battle_from(pipe, match_id, b_key)
                    except BattleAuthStaleError:
                        return False
                    battle_before = clone(battle)
                    if (
                        battle.allocation_id != expected.allocation_id
                        or battle.gameserver_uid != expected.instance_uid
                        or battle.state
                        not in ("warming", "abandoned", BATTLE_STATE_PREACTIVE_RELEASE_PENDING)
                    ):
                        return False

                    auth_record = None
                    auth_raw = await pipe.get(a_key)
                    if auth_raw is None:
                        if expected.instance_epoch != 0 or battle.instance_epoch != 0:
                            return False
                    else:
                        auth_record = dspb.BattleDSAuthStorageRecord()
                        unmarshal_battle_auth(match_id, auth_raw, auth_record)
                        if (
                            not complete_expected_battle_instance(expected)
                            or not battle_auth_record_v2_exact(auth_record)
                            or not expected_battle_instance_matches(
                                auth_record, battle, expected
                            )
                            or auth_record.HasField("active")
                            or auth_record.phase
                            not in (
                                dspb.BATTLE_AUTH_PHASE_BOOTSTRAP,
                                dspb.BATTLE_AUTH_PHASE_TERMINATING,
                            )
                        ):
                            return False

                    battle.state = BATTLE_STATE_PREACTIVE_RELEASE_PENDING
                    battle_payload = self._marshal_battle_transition(battle_before, battle)
                    auth_payload = None
                    if auth_record is not None:
                        auth_record.phase = dspb.BATTLE_AUTH_PHASE_TERMINATING
                        auth_record.ClearField("pending")
                        auth_record.pending_started_ms = 0
                        auth_record.delivered_rv = ""
                        auth_record.updated_at_ms = self._now_ms()
                        auth_payload = auth_record.SerializeToString()
                    pipe.multi()
                    if auth_payload is not None:
                        _set(pipe, a_key, auth_payload, 0.0)
                    _set(pipe, b_key, battle_payload, 0.0)
                    await pipe.execute()
                    return True
            except WatchError:
                await _cas_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "battle %d preactive release fence cas retry exhausted",
            match_id,
        )

    # ── PurgePreactiveReleasedExpected ───────────────────────────────────────

    async def purge_preactive_released_expected(
        self, match_id: int, expected: BattleExpectedInstance
    ) -> bool:
        """只能在调用方已经明确确认外部 UID 条件删除成功后执行。

        Go: `PurgePreactiveReleasedExpected`。仓内再核验 expected 元组、
        release-pending 状态和两键 PTTL=-1,防止有限 TTL 的历史半状态或已激活赢家
        被误删。generation counter **永不删除**。
        """
        if match_id == 0 or expected.allocation_id == "" or expected.instance_uid == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle preactive purge requires allocation_id and instance_uid",
            )
        a_key, b_key = battle_auth_key(match_id), battle_key(match_id)

        for attempt in range(BATTLE_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key)
                    try:
                        battle = await _read_battle_from(pipe, match_id, b_key)
                    except BattleAuthStaleError:
                        return False
                    if (
                        battle.allocation_id != expected.allocation_id
                        or battle.gameserver_uid != expected.instance_uid
                        or battle.state != BATTLE_STATE_PREACTIVE_RELEASE_PENDING
                    ):
                        return False
                    if await pipe.pttl(b_key) != -1:
                        return False

                    auth_raw = await pipe.get(a_key)
                    if auth_raw is None:
                        if expected.instance_epoch != 0 or battle.instance_epoch != 0:
                            return False
                    else:
                        auth_record = dspb.BattleDSAuthStorageRecord()
                        unmarshal_battle_auth(match_id, auth_raw, auth_record)
                        if (
                            not complete_expected_battle_instance(expected)
                            or not battle_auth_record_v2_exact(auth_record)
                            or not expected_battle_instance_matches(
                                auth_record, battle, expected
                            )
                            or auth_record.phase != dspb.BATTLE_AUTH_PHASE_TERMINATING
                            or auth_record.HasField("active")
                        ):
                            return False
                        if await pipe.pttl(a_key) != -1:
                            return False
                    pipe.multi()
                    pipe.delete(a_key, b_key)
                    await pipe.execute()
            except WatchError:
                await _cas_backoff(attempt)
                continue

            await self._rdb.zrem(ACTIVE_KEY, str(match_id))
            return True
        raise errcode.PandoraError(
            errcode.ErrInternal, "battle %d preactive purge cas retry exhausted", match_id
        )

    # ── PurgeTerminatedExpected ──────────────────────────────────────────────

    async def purge_terminated_expected(
        self, match_id: int, expected: BattleExpectedInstance
    ) -> bool:
        """仅清理已锁死且 battle 已终态的同一 allocation。Go: `PurgeTerminatedExpected`。

        调用方必须先明确确认外部 ReleaseExpected 成功;仓内要求两键仍为**永久墓碑**,
        拒绝删除历史有限 TTL 半状态。
        """
        if match_id == 0 or not complete_expected_battle_instance(expected):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "battle purge requires expected allocation"
            )
        a_key, b_key = battle_auth_key(match_id), battle_key(match_id)

        for attempt in range(BATTLE_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key)
                    try:
                        auth, battle = await _read_bound_authority(pipe, match_id, a_key, b_key)
                    except BattleAuthStaleError:
                        return False
                    if (
                        not battle_auth_record_v2_exact(auth)
                        or not expected_battle_instance_matches(auth, battle, expected)
                        or auth.phase != dspb.BATTLE_AUTH_PHASE_TERMINATING
                        or not battle_terminal(battle.state)
                    ):
                        return False
                    if await pipe.pttl(a_key) != -1:
                        return False
                    if await pipe.pttl(b_key) != -1:
                        return False
                    pipe.multi()
                    pipe.delete(a_key, b_key)
                    await pipe.execute()
            except WatchError:
                await _cas_backoff(attempt)
                continue

            await self._rdb.zrem(ACTIVE_KEY, str(match_id))
            return True
        raise errcode.PandoraError(
            errcode.ErrInternal, "battle %d purge cas retry exhausted", match_id
        )

    # ── ExpireTerminatedExpected ─────────────────────────────────────────────

    async def expire_terminated_expected(
        self,
        match_id: int,
        expected: BattleExpectedInstance,
        auth_ttl_sec: float,
        battle_ttl_sec: float,
    ) -> bool:
        """外部 release 与 abandoned lifecycle 投递均明确成功后,把永久墓碑改为有界保留。

        Go: `ExpireTerminatedExpected`。它不改变任何 protobuf bytes;完整 expected
        元组/TERMINATING/终态仍须在同一 WATCH 中成立。
        """
        if (
            match_id == 0
            or not complete_expected_battle_instance(expected)
            or auth_ttl_sec <= 0
            or battle_ttl_sec <= 0
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle expire terminated requires expected allocation and positive ttls",
            )
        a_key, b_key = battle_auth_key(match_id), battle_key(match_id)

        for attempt in range(BATTLE_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(a_key, b_key)
                    try:
                        auth_record, battle = await _read_bound_authority(
                            pipe, match_id, a_key, b_key
                        )
                    except BattleAuthStaleError:
                        return False
                    if (
                        not battle_auth_record_v2_exact(auth_record)
                        or not expected_battle_instance_matches(auth_record, battle, expected)
                        or auth_record.phase != dspb.BATTLE_AUTH_PHASE_TERMINATING
                        or not battle_terminal(battle.state)
                    ):
                        return False
                    pipe.multi()
                    pipe.pexpire(a_key, int(auth_ttl_sec * 1000))
                    pipe.pexpire(b_key, int(battle_ttl_sec * 1000))
                    await pipe.execute()
            except WatchError:
                await _cas_backoff(attempt)
                continue

            await self._rdb.zrem(ACTIVE_KEY, str(match_id))
            return True
        raise errcode.PandoraError(
            errcode.ErrInternal, "battle %d expire terminated cas retry exhausted", match_id
        )


# ── 事务外的纯函数 ───────────────────────────────────────────────────────────


def _validate_battle_binding(binding: BattleAuthorityBinding) -> None:
    """Go: `validateBattleBinding`。"""
    if (
        binding.match_id == 0
        or binding.allocation_id == ""
        or binding.pod_name == ""
        or binding.instance_uid == ""
        or binding.auth_ttl_sec <= 0
        or binding.battle_ttl_sec <= 0
        or binding.required_writer_epoch != BATTLE_DS_WRITER_EPOCH_V2
    ):
        raise errcode.PandoraError(
            errcode.ErrInvalidArg,
            "battle prepare requires match/allocation/pod/uid/ttl/writer_epoch",
        )


def _activate_result(
    auth, battle, first: bool, first_abandon: bool, roster_incomplete: bool, terminal: bool
) -> BattleActivateResult:  # noqa: ANN001
    """Go: `activateResult`。

    ★ `auth.active.<field>` 在 Python protobuf 上读取未设置的子消息返回零值且
      **不会**设置 presence —— 与 Go 的 `auth.Active.GetXxx()`(nil-safe)等价。
    """
    return BattleActivateResult(
        first_activation=first,
        first_abandon=first_abandon,
        roster_incomplete=roster_incomplete,
        terminal=terminal,
        heartbeat_ms=auth.last_active_heartbeat_ms,
        active=BattleCredentialIdentity(
            pod_name=auth.ds_pod_name,
            instance_uid=auth.active.instance_uid,
            instance_epoch=auth.active.instance_epoch,
            gen=auth.active.gen,
            jti=auth.active.jti,
            exp_ms=auth.active.exp_ms,
            kid=auth.active.kid,
            token_sha256=auth.active.token_sha256,
            writer_epoch=auth.active.writer_epoch,
        ),
        battle=clone(battle),
    )
