"""hub_allocator 业务层**基座** —— 对应 Go 侧
`services/battle/hub_allocator/internal/biz/hub.go` 第 1–450 行。

本模块只放「字段 + 构造 + setter + accessor + 两个跨批次共用的小判定」,不含任何
RPC 方法与后台扫描:那两批分别由 `RpcMixin` / `SweepMixin` 提供,最终由

    class HubUsecase(SweepMixin, RpcMixin, ShardMixin, HubUsecaseBase): ...

组装。拆开的理由不是"文件太长",而是 Go 那 3979 行里三类关注点各自的失败模式完全
不同(基座错 = 全域行为漂移;分片辅助错 = 座位泄漏 / 双 owner;扫描错 = 后台静默停摆),
放一起时任何一处变异实验都要重跑整份测试。

## 常量为什么必须逐字抄

`reason_*` 与 `state_*` 这些串**不是给人看的自由文本**,而是运维照着建 Loki / Grafana
查询的固定词表(infra.md §11.3 R2)。两栈并存期同一条链路可能一半请求走 Go、一半走
Python,任一侧改一个字母,面板就只统计到另一半的量 —— 而"少了一半"这件事本身没有
任何告警。

## 与 Go 的两处必要形变(其余逐行同构)

  1. **TTL 一律以「秒」为单位并在方法名里写出来**(`shard_ttl_sec()`)。Go 是
     `time.Duration` 强类型,传错单位编译不过;Python 传 float 谁也拦不住,而
     `repo` / `authRepo` 全套接口收的都是秒。写进名字是这里唯一不依赖人眼的防线。
  2. **`(值, error)` 一律改成「返回值 + 抛异常」**。Go 的 `mintSourceRevision`
     失败时**同时**返回 `0` 和 err,调用方只看 err;Python 抛出即可,不需要那个 0。
     唯一例外是 `migrate_resign_session_jti` —— 它的 `(jti, ok)` 里 `ok=False` 表示
     "本 tick 跳过、下 tick 重试",是**控制流**不是错误,照抄二元组。
"""

from __future__ import annotations

import asyncio
import datetime as _dt
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from pandorapy import config as pconfig
from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import source_revision as srev
from pandorapy.services.hub_allocator import fleet as F
from pandorapy.services.hub_allocator.conf import HubConf

# ── 分片状态常量(对应 proto 的 string state 字段)────────────────────────────
#
# ★ 用字符串而不是枚举是 Go 侧既定的 wire 形状,不能"顺手改成 enum":
#   分片镜像是 Redis 里的 proto bytes,改类型即破坏 §9 不变量 17 的双向兼容。

STATE_WARMING = "warming"  # 已播种但尚未收到首个(鉴权)心跳:不可被 AssignHub 选中
STATE_READY = "ready"
STATE_DRAINING = "draining"
STATE_STOPPING = "stopping"

# ── Heartbeat 响应控制指令 ──────────────────────────────────────────────────

COMMAND_NONE = ""
COMMAND_STOP = "stop"  # 通知孤儿 Hub DS(无对应分片镜像)自行停机
COMMAND_DRAIN = "drain"  # 通知 draining 分片上的 Hub DS 开始优雅迁移

# 迁移原因(HubMigrateEvent.reason)。
MIGRATE_REASON_CONSOLIDATION = "consolidation"

# ── 日志 reason 枚举(infra.md §11.3 R2:snake_case 固定枚举串)───────────────
#
# 为什么必须由业务代码自己打(Go 头注释的理由在 Python 侧同样成立):中间件只对
# "服务端故障"类错误码升 ERROR,而 `ErrHubNoAvailable` / `ErrInvalidArg` /
# `ErrInvalidState` 与全部 fencing 码都落 DEBUG —— 线上默认 info 级完全不可见,
# 而「没有可用 hub」正是本域最关键的排障信号。

# 选分片 / 容量(hub_no_routable_shard、hub_select_rejected)
REASON_NO_SHARD_MIRROR = "no_shard_mirror"  # Redis 里一个分片镜像都没有
REASON_NO_SHARD_IN_REGION = "no_shard_in_region"  # 有分片,但没有一个在本 region
REASON_NO_SHARD_IN_RELEASE_TRACK = "no_shard_in_release_track"  # canary 无容量的典型形态
REASON_ALL_SHARDS_TRACK_INVALID = "all_shards_track_invalid"  # 持久化轨道非法,全部 fail-closed
REASON_ALL_SHARDS_WARMING = "all_shards_warming"  # 分片在但从未收到过鉴权心跳
REASON_ALL_SHARDS_DRAINING = "all_shards_draining"  # 全部 draining/stopping
REASON_ALL_SHARDS_FULL = "all_shards_full"  # 真满了,需要扩容
REASON_ALL_CANDIDATES_RESERVE_REJECTED = "all_candidates_reserve_rejected"  # 被原子授权+占座门全拒
REASON_CANDIDATES_VANISHED = "candidates_vanished"  # 有候选却一个都没尝试成
REASON_NO_SHARD_CANDIDATE = "no_shard_candidate"  # 兜底
REASON_INVALID_RELEASE_TRACK = "invalid_release_track"
REASON_LIST_SHARDS_FAILED = "list_shards_failed"
REASON_TEAM_SHARD_LOOKUP_FAILED = "team_shard_lookup_failed"
REASON_RESERVE_SEAT_ERROR = "reserve_seat_error"
# 分片明细采样里的逐分片判词(shard_census 字段)
REASON_SHARD_TRACK_INVALID = "track_invalid"
REASON_SHARD_TRACK_MISMATCH = "track_mismatch"
REASON_SHARD_REGION_MISMATCH = "region_mismatch"
REASON_SHARD_EXCLUDED_POD = "excluded_pod"
REASON_SHARD_NOT_READY = "not_ready"
REASON_SHARD_FULL = "full"
# 分配 / 释放 / 准入
REASON_ASSIGN_CAS_EXHAUSTED = "assign_cas_exhausted"
REASON_RELEASE_CAS_EXHAUSTED = "release_cas_exhausted"
REASON_OWNER_BARRIER_NOT_OPEN = "owner_barrier_not_open"
REASON_OWNER_POINTS_ELSEWHERE = "owner_points_elsewhere"
REASON_SHARD_HEARTBEAT_TIMEOUT = "heartbeat_timeout"
REASON_SEED_TOKEN_NOT_READY = "seed_token_not_ready"
REASON_WRITER_LEASE_NOT_HELD = "writer_lease_not_held"
REASON_CANARY_NO_CAPACITY = "canary_no_capacity"
REASON_TRANSFER_CAS_EXHAUSTED = "transfer_cas_exhausted"

# 心跳后异步续期在场玩家 HUB 位置的独立预算(秒)。对应 Go 的 presenceRefreshTimeout。
PRESENCE_REFRESH_TIMEOUT_SEC = 3.0


# ── 出入参结构 ───────────────────────────────────────────────────────────────


@dataclass(slots=True)
class HubTicketBinding:
    """把 hub 入场票绑定到当前归属版本和目标 DS active 凭据。对应 Go 的同名结构。

    legacy 模式使用零值;Model B 必须前七项完整。

    ★ 后两项是**独立 claim**,不参与"绑定是否完整"的判定:
      `source_match_id` 只在 AssignHub 的 Battle→Hub 回流路径 >0(Transfer / 迁移
      重签一律 0);`session_jti` 是请求方登录会话代际,盖进 v2 票据的 `sjti`。
      把它们算进完整性判定会让一次正常的 Transfer 重签被判成"绑定不全"而拒签。
    """

    pod_name: str = ""
    instance_uid: str = ""
    protocol_epoch: int = 0  # Go: uint32
    credential_gen: int = 0  # Go: uint64
    credential_jti: str = ""
    hub_assignment_id: str = ""
    writer_epoch: int = 0  # Go: uint32
    release_track: str = ""
    source_match_id: int = 0  # Go: uint64
    session_jti: str = ""


@dataclass(slots=True)
class HubCredential:
    """service 层从**验签通过**的 Model B hub 令牌抽出的凭据身份。对应 Go 的 HubCredential。"""

    instance_uid: str = ""
    protocol_epoch: int = 0  # Go: uint32
    gen: int = 0  # Go: uint64
    jti: str = ""
    token_sha256: str = ""
    kid: str = ""
    writer_epoch: int = 0  # Go: uint32


@dataclass(slots=True)
class AssignResult:
    """AssignHub 的出参。对应 Go 的 AssignResult。"""

    hub_ds_addr: str = ""
    hub_ticket: str = ""
    hub_pod_name: str = ""
    shard_id: int = 0  # Go: uint32
    ticket_exp_ms: int = 0  # Go: int64


# ── 弱依赖协议 ───────────────────────────────────────────────────────────────


@runtime_checkable
class TicketSigner(Protocol):
    """hub DSTicket 签发抽象。对应 Go 的 `TicketSigner`。

    Go 返回 `(token, expiresAtMs, err)`;Python 返回 `(token, expires_at_ms)`,
    失败抛异常。
    """

    async def sign_hub_ticket(
        self, player_id: int, role_id: int, binding: HubTicketBinding
    ) -> tuple[str, int]: ...


@runtime_checkable
class HubMigratePusher(Protocol):
    """强制整合迁移通知推送(Kafka topic `pandora.hub.migrate`,key=player_id)。

    弱依赖:None 时跳过推送(整合仍做服务端权威搬迁,drain 心跳指令兜底客户端重连)。
    """

    async def push_migrate(self, player_id: int, payload: bytes) -> None: ...


# ── 基座 ─────────────────────────────────────────────────────────────────────


class HubUsecaseBase:
    """`HubUsecase` 的字段 / 构造 / setter / accessor。对应 Go 的 `HubUsecase` 结构体前半。

    ★ 刻意**不**加 `__slots__`:三个 mixin 叠加时 slots 布局要在每层重复声明,
      漏一层就静默退回 `__dict__`,反而制造"以为有约束其实没有"的假象。
    """

    def __init__(
        self,
        repo: Any,
        fleet: Any,
        signer: TicketSigner | None,
        cfg: HubConf,
    ) -> None:
        self.repo = repo
        self.fleet = fleet
        # Go: `if s, ok := fleet.(HubFleetScaler); ok`。`HubFleetScaler` 是
        # runtime_checkable Protocol,isinstance 只查方法存在性 —— 与 Go 的接口
        # 断言语义一致(Mock / Local provider 刻意不实现,故此处恒 None)。
        self.scaler: F.HubFleetScaler | None = fleet if isinstance(fleet, F.HubFleetScaler) else None
        self.signer = signer
        self.cfg = cfg

        self.migrate: HubMigratePusher | None = None
        # player_locator 位置检查器(弱依赖:玩家切线护栏,None 时跳过战斗/匹配中检查)。
        self.locator: Any | None = None

        # owner 权威实例租约双写(owner_lease.py renew_owner_lease_gate 的两档语义)。
        self.owner_lease: Any | None = None
        self.owner_lease_required = False

        # owner 迁移弱依赖调用面 + census 已准入缓存(owner_authority.py)。
        self.owner_auth: Any | None = None
        # Go 是 `sync.Map`。asyncio 单线程事件循环下普通 dict 足够,且
        # `sweep_stale_owner_admitted` 需要遍历 + 按时间淘汰 —— 用 dict 才能做到。
        # ⚠️ 若将来把本服务改成多线程 executor,这里必须换回带锁结构。
        self.owner_admitted: dict[int, float] = {}

        # 票据 sjti 绑定强制门(默认 False = 兼容档:ACK 收到空 sjti 告警放行)。
        self.sess_gate_require_sjti = False
        # 会话现行性权威只读视图(None = dev 无权威)。
        self.sess_gate: Any | None = None

        # 播种分片镜像时先置 warming,等首个通过 Guard 的 Hub DS 心跳才转 ready。
        self.require_heartbeat_ready = False
        # 令牌代际绑定开关(仅 agones + ds_auth.mode=enforce 置 True)。
        self.ds_token_generation = False

        # Model B「Redis 唯一授权权威」授权记录仓;None = legacy 代际门路径。
        self.auth_repo: Any | None = None
        # Model B 授权记录键 TTL。★ 必须独立于 shard_ttl:授权寿命远长于分片镜像 TTL,
        # 被 shard_ttl 提前过期会导致「有效凭据被判 stale」。
        self.auth_ttl: _dt.timedelta = _dt.timedelta(0)

        # 只决定无 assignment 玩家首次尝试的轨;实际命中轨写入 assignment 后粘性。
        self.release_policy: Any | None = None

        # 写者继任租约视图(None = 未启用:dev/mock 或单副本 Recreate 部署)。
        self.writer_fence: Any | None = None

        # Hub assignment 来源版本铸号器(INC-20260818-003)。零值可用,无需注入 ——
        # 持久性来自 writer_fence 的任期号,进程内只存任期内序号。
        self.rev_minter = srev.Minter()

    # ── setter(全部对应 Go 的 Set*)────────────────────────────────────────

    def set_migrate_pusher(self, p: HubMigratePusher | None) -> None:
        """注入强制整合迁移通知推送器(弱依赖)。"""
        self.migrate = p

    def set_location_checker(self, c: Any | None) -> None:
        """注入 player_locator 位置检查器(弱依赖)。"""
        self.locator = c

    def set_require_heartbeat_ready(self, b: bool) -> None:
        """开启「先 warming、首个鉴权心跳才 ready」(agones 真 DS 链路置 True)。"""
        self.require_heartbeat_ready = b

    def set_ds_token_generation(self, b: bool) -> None:
        """开启令牌代际绑定(仅 agones + enforce)。"""
        self.ds_token_generation = b

    def set_auth_repo(self, r: Any | None) -> None:
        """注入 Model B 授权记录仓(仅 ds_auth.authority_mode=redis 时装配)。"""
        self.auth_repo = r

    def set_auth_ttl(self, d: _dt.timedelta) -> None:
        """注入 Model B 授权记录键 TTL(独立于 shard_ttl,授权寿命更长)。"""
        self.auth_ttl = d

    def set_release_track_policy(self, p: Any | None) -> None:
        """注入 player_id 级确定性 cohort 策略。"""
        self.release_policy = p

    def set_writer_fence(self, f: Any | None) -> None:
        """注入写者继任租约视图(仅 Model B 生产由 main 注入)。"""
        self.writer_fence = f

    def set_session_gate(self, g: Any | None) -> None:
        """注入会话现行性权威只读视图(None = dev 无权威)。"""
        self.sess_gate = g

    def set_session_gate_require_sjti(self, require: bool) -> None:
        """设置票据 sjti 绑定强制门(默认 False=兼容档)。

        激活前提:全 fleet Hub DS 已转发 sjti、旧 DS 排空、等满一个票据最大 TTL。
        """
        self.sess_gate_require_sjti = require

    def set_owner_lease_renewer(self, r: Any | None, required: bool) -> None:
        """注入 owner 实例租约续写器 + 档位。对应 Go 的 `SetOwnerLeaseRenewer`。

        (Go 侧定义在 `owner_lease.go`,但只写基座字段,故随基座一起提供。)
        """
        self.owner_lease = r
        self.owner_lease_required = required

    def set_owner_authority(self, a: Any | None) -> None:
        """注入 owner 权威调用面。对应 Go 的 `SetOwnerAuthority`(定义在 owner_authority.go)。"""
        self.owner_auth = a

    # ── 来源版本铸号(INC-20260818-003)────────────────────────────────────

    def mint_source_revision(self) -> int:
        """为一次**真正改变 target** 的 assignment CAS 领号。对应 Go 的 `mintSourceRevision`。

        三条纪律(逐条对应 Go,改动前先读 `pandorapy/source_revision.py` 模块头):

          1. 只在真实置换点调用。TTL 刷新、凭据轮换、cleanup-only 标记、墓碑删除都
             **不**领号,原样带走旧号 —— 领号的语义是「这是一次新的归属来源」,
             不是「这条记录被动过」。
          2. 未启用 writer_fence(dev / mock / 单副本 Recreate)返回 `LEGACY`。
             那些部署里不存在「两个写者并存」,本门无事可做,也拿不到任期号。
          3. 持有租约但铸号失败一律**上抛 fail-closed**:铸不出号时正确的动作是拒绝
             这次 assignment,不是发一个可能比旧号还小的版本出去。

        ★ `writer_fence.current()` 的返回顺序是 `(held, token)` —— 与 Go 的
          `Current() (uint64, bool)` **相反**。写反了不会报错:`held` 会拿到一个
          非零 int(真值),`term` 拿到 True(=1),于是全服所有号都落在任期 1 上,
          换届后新写者的号反而更小 —— 正是本机制要防的事。
        """
        if self.writer_fence is None:
            return srev.LEGACY
        held, term = self.writer_fence.current()
        if not held:
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "hub allocator writer lease not held; cannot mint assignment source revision",
            )
        return self.rev_minter.next(int(term))

    def backfill_source_revision(self, rec: Any) -> int:
        """给**存量** legacy(revision=0)记录补铸一个号。对应 Go 的 `backfillSourceRevision`。

        这是 `mint_source_revision` 三条纪律的**唯一例外**:纪律防的是「同一份来源被
        反复抬号」,而这里补的是 0→非零 —— 0 不是「最小版本」而是「**没有**版本」,
        与任何非零都不可比。给它一个号是**建立水位**,不是抬高水位;target 一个字节
        不变,不存在「旧来源看起来更新」的风险。

        铸不出号时保持 0 原样返回,**不阻断**这次复用:与本改动之前的行为逐字节相同。
        真正的 fail-closed 有两处且都不在这里 —— 置换点的 `mint_source_revision`
        直接上抛,以及仓储侧的 writer fence。

        ★ **本函数只改内存里的 rec,绝不打成功日志**:成功事件必须由调用方在 CAS 确认
          `swapped is True` 之后再打。在这里打等于把「打算补」记成「已补上」——
          CAS 报错或竞争落败时 Redis 里仍然是 0,而 Loki 上已经躺着一条成功事件,
          rollout 判空(证明不存在 source_revision=0 的存活 assignment)会被污染。

        Returns:
            本次真正写进 rec 的号;0 = 什么都没补。
        """
        if rec is None or rec.source_revision != srev.LEGACY:
            return 0
        try:
            revision = self.mint_source_revision()
        except asyncio.CancelledError:
            # ★ 必须先于宽 except 放行:CancelledError 继承 BaseException,
            #   被吞掉会让停机时这条路径继续往下走。
            raise
        except BaseException as exc:  # noqa: BLE001 —— 铸号失败一律降级为"没补上"
            # 失败事件在这里打是准确的:它描述的是「这次没能铸出号」,与 CAS 结果无关。
            plog.get().warning(
                "hub_assignment_source_revision_backfill_failed",
                player_id=rec.player_id,
                assignment_id=rec.assignment_id,
                err=str(exc),
            )
            return 0
        if revision == srev.LEGACY:
            # 未启用写者租约的部署(dev / 单副本):本门无事可做,不刷日志。
            return 0
        rec.source_revision = revision
        return revision

    # ── 写者租约闸门 ───────────────────────────────────────────────────────

    def require_writer(self) -> None:
        """写路径入口门:未持有写者租约的副本快速拒写。对应 Go 的 `requireWriter`。

        `ErrUnavailable` 语义 = 可重试(重试会被路由到当前写者副本)。
        注意这只是快路径礼貌拒绝;防住「检查后失主」竞态的最终防线是
        `writer_fence.py` 的同事务存储级 fencing。
        """
        if self.writer_fence is None:
            return
        held, _ = self.writer_fence.current()
        if not held:
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "hub allocator writer lease not held on this replica; retry",
            )

    def confirm_writer_for_ticket(self, player_id: int) -> None:
        """出票前写者复核。对应 Go 的 `confirmWriterForTicket`。

        assignment 单键无法进 `{pod}` slot fence 事务,票据只在「入口到返回全程持有
        租约」时交付。入口后失主的在途请求走到这里被拦 —— 存储侧可能已留下
        assignment / 席位(合法数据,继任者 CAS 接续或 TTL 回收),但票绝不交给调用方。
        """
        try:
            self.require_writer()
        except errcode.PandoraError:
            plog.get().warning("hub_ticket_withheld_writer_lost", player_id=player_id)
            raise

    # ── 会话代际 ───────────────────────────────────────────────────────────

    async def migrate_resign_session_jti(self, player_id: int) -> tuple[str, bool]:
        """为系统发起的迁移重签解析玩家当前会话 jti。对应 Go 的 `migrateResignSessionJTI`。

        四条分支(顺序即语义):

            sess_gate is None(dev 无权威) → ("", True):签空 sjti;
            权威不可达                     → ("", False):**fail-closed**,本 tick 跳过;
            无会话(已登出)                → ("", True):照常完成服务端搬迁;
            有会话                         → (当前 jti, True)。

        ★ 第二与第三条必须分开。会话权威查不到 ≠ 玩家没有会话:把不可达当成"无会话"
          会让迁移带着空 sjti 签票并推给客户端,而 prod 兑换点对空 sjti 硬拒 ——
          玩家收到一张必然被拒的票,还失去了下一 tick 重试的机会。
        """
        if self.sess_gate is None:
            return "", True
        try:
            jti, found = await self.sess_gate.current_jti(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 任何不可达都 fail-closed
            plog.get().warning(
                "migrate_resign_session_gate_unavailable", player_id=player_id, err=str(exc)
            )
            return "", False
        if not found:
            return "", True
        return jti, True

    # ── accessor ───────────────────────────────────────────────────────────

    def auth_ttl_sec(self) -> float:
        """授权键 TTL(秒)。对应 Go 的 `authTTLDur`。

        注入了用注入值;未注入(测试/兜底)回退 2×shard_ttl,**绝不返回 0** ——
        0 在 Redis 语义里是"永不过期",授权键会泄漏。
        """
        seconds = self.auth_ttl.total_seconds()
        if seconds > 0:
            return seconds
        return self.shard_ttl_sec() * 2

    def heartbeat_max_age_ms(self) -> int:
        """「分片心跳仍算新鲜」的最大毫秒(= 心跳超时阈值)。对应 Go 的 `heartbeatMaxAgeMs`。"""
        return int(pconfig.parse_duration(self.cfg.heartbeat_timeout).total_seconds() * 1000)

    def candidate_token_exp(self, exp_ms: int) -> int:
        """写入镜像的令牌 exp 镜像值(仅调试/兼容,**不再当代际**)。

        对应 Go 的 `candidateTokenExp`。代际识别已改用 `candidate_token_gen`:
        JWT exp 是秒精度,同一秒内两次重签得到相同 exp,旧令牌会被误判为当前代际。
        """
        if self.ds_token_generation and exp_ms > 0:
            return exp_ms
        return 0

    def candidate_token_gen(self, gen: int) -> int:
        """写入镜像的令牌「代际」(Redis INCR 单调值)。对应 Go 的 `candidateTokenGen`。

        其余情况恒 0(= 不启用):off/permissive 心跳无已验签 claims,开了会自锁。
        """
        if self.ds_token_generation:
            return gen
        return 0

    def initial_shard_state(self) -> str:
        """播种新分片镜像的初始状态。对应 Go 的 `initialShardState`。"""
        if self.require_heartbeat_ready:
            return STATE_WARMING
        return STATE_READY

    def shard_ttl_sec(self) -> float:
        """分片镜像 TTL(秒)。Go: `shardTTL()`。"""
        return pconfig.parse_duration(self.cfg.shard_ttl).total_seconds()

    def assign_ttl_sec(self) -> float:
        """归属记录 TTL(秒)。Go: `assignTTL()`。"""
        return pconfig.parse_duration(self.cfg.assignment_ttl).total_seconds()

    def reservation_ttl_sec(self) -> float:
        """逐 assignment reservation 的存活时长(秒)。Go: `reservationTTL()`。"""
        return pconfig.parse_duration(self.cfg.reservation_ttl).total_seconds()

    def transfer_cooldown_sec(self) -> float:
        """同玩家两次 Transfer 之间的冷却(秒)。Go: `transferCooldown()`。"""
        return pconfig.parse_duration(self.cfg.transfer_cooldown).total_seconds()

    def retry(self) -> int:
        """乐观锁重试次数。Go: `retry()`。"""
        return self.cfg.optimistic_retry

    def assignment_saga_ttl_sec(self) -> float:
        """assignment 记录(含 owner-cleanup saga 阶段字段)的持久化 TTL(秒)。

        对应 Go 的 `assignmentSagaTTL`。Release / Departure / transfer cleanup 是
        **显式精确操作**,从不按时间推断;TTL 只兜底泄漏。
        """
        return self.assign_ttl_sec()
