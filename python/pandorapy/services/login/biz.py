"""login 业务层 —— 对应 Go 侧 `internal/biz/{login,account_role,ticket,login_ratelimit}.go`。

覆盖范围(诚实边界,交付说明的 honest_gaps 里同样登记):
  ✅ Login / EnterRole / ListAccountRoles / SelectRole / Logout / GetPlayerNo /
     GetResumeContext,会话代际定序、会话现行性门、交付终检、登录失败 Quota、
     owner query-first 路由(applyOwnerPlacement 逐条移植)。
  ✅ 断线重连三态门(`_try_battle_reconnect` / `_resolve_battle_authority` /
     `_build_battle_resume`)+ Hub 放行门(`_guard_hub_route_against_active_battle`)
     + 两条路由入口(`resolve_hub_endpoint_from_match` / `resolve_battle_endpoint`)。
  ✅ DSTicket v2(RS256 / 方案 B)已接:`TicketUsecase` 的签发与验签两侧都按**配置**
     二选一(`set_ds_ticket_v2_signer` / `set_ds_ticket_v2_verifier` 任一已注入 =
     RS256-only,legacy HS256 玩家票一律拒)。绝不"签不出 v2 就退回 HS256" ——
     那会同时打穿实例绑定 / 灰度轨道粘滞 / jti 吊销三道门(§9.3),而运行期没有任何信号。

★ 战斗态判定为什么必须是**两层**权威(P0 修复 2026-07-15 的核心结论):
    locator presence 是 30s TTL 的**投影**,key 在不能证明对局还活着,key 不在更不能
    证明玩家已离开旧 DS(§9.22)。matchmaker 的 player claim + match 记录才是"玩家是否
    属于一场活跃对局"的**耐久事实**(由 ReleaseMatch 显式释放)。presence 未命中 BATTLE
    时必须再查一次 matchmaker,封住两个窗口:
      · READY 已定但 notifyBattle 尚未 / 失败 → 玩家被误路由回 Hub;
      · locator TTL 恰好蒸发但对局仍活跃 → Hub / Battle 双在场(§9 不变量 1)。
    反向同理:presence 报 BATTLE 时**不能**直接签重连票,必须过 `InspectBattleRoute`
    的显式三态(ACTIVE / TERMINAL / UNKNOWN)。把签票门的 ErrPermissionDeny 当"终局"
    正是当年那个 P0:对局明明还活着,签票被拒只是因为 roster 抖动,却被读成"已结束"
    而放行进 Hub。UNKNOWN 一律 fail-closed 重试,绝不折叠成终态。

★ 本文件里每一处 `except BaseException` 之前都先 `except asyncio.CancelledError: raise`
  (或该 except 自身无条件 raise)。吞掉取消会让 §9.16 的「先摘流量 → 再排空在途」失效。
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as _dt
import hashlib
import time
import uuid

from pandora.locator.v1 import locator_pb2
from pandora.login.v1 import login_pb2
from pandora.match.v1 import match_pb2

from pandorapy import auth as pauth
from pandorapy import dsticket as pdsticket
from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import safego
from pandorapy.protoenum import enum_name
from pandorapy.services.login import battleroute as lbattleroute
from pandorapy.services.login import clients as lclients
from pandorapy.services.login import data as ldata
from pandorapy.services.login import dsadmission as ldsadmission
from pandorapy.services.login import dsticket as ldsticket
from pandorapy.services.login import hubbinding as lhubbinding
from pandorapy.services.login import passwd as lpasswd

# ── 常量(与 Go 逐字同值)────────────────────────────────────────────────────

# 角色编号读取子预算:它挂在登录热路径上,必须有界且可降级,不进 5s 登录预算的服务扇出账。
PLAYER_NO_READ_TIMEOUT_SEC = 0.25
# 「播种 + 取当前显示名」的单角色超时。超时 = 用台账里的名字兜底,不阻断登录 ——
# 名字错了是可修的展示问题,登不进去不是。
PROFILE_SEED_TIMEOUT_SEC = 0.3
# 设备 / 角色登录记账的 detached 超时(纯记账副作用,不进关键路径)。
TOUCH_TIMEOUT_SEC = 2.0
# 失败会话补偿的 detached 超时。
SESSION_RECONCILE_TIMEOUT_SEC = 2.0

# owner 权威不可达时给客户端的建议退避。owner 是 TiDB 支撑的线性一致权威,
# 一次故障切换 / 重选举量级在秒内;1s 足够跨过瞬时抖动,又不会让玩家在真实恢复后还傻等。
OWNER_UNKNOWN_RETRY_AFTER_MS = 1000
# 判定 STABLE 时要求的租约剩余寿命下界(时钟偏移 + 网络往返 + 客户端处理的安全余量)。
# 低于它只报 PENDING:§9.22 要求「旧 DS 最晚停止可玩 < 新 DS 最早开始可玩」,
# login 用自己的时钟贴边判 STABLE 会把这条时序悄悄破掉。
OWNER_LEASE_SKEW_MARGIN_MS = 2000
# 钳住由屏障推导的 retry_after,避免权威给出异常大的 admit_not_before 时
# 让客户端长时间不重查(§9.19 每次等待必须有界)。
OWNER_RETRY_AFTER_CEILING_MS = 10000
# Login 因权威暂时不可判定而返回 WAIT 时给客户端的退避。
LOGIN_WAIT_RETRY_AFTER_MS = 1000

# locator presence 查询的就地短重试(Go: battleLocationQueryRetries / battleLocationQueryBackoff)。
# 它挡的是**单次抖动**,不是故障:presence 一次读失败就判"不在战斗"会直接放行进 Hub,
# 而玩家可能还在旧 Battle DS 上(§9 不变量 1)。重试耗尽后抛错,由调用方按 profile
# 决定 fail-closed 还是降级 —— 这里绝不吞掉。
# 3×50ms 的总额外开销 ≤ 100ms,只占 prod 5s 登录预算的 2%。
BATTLE_LOCATION_QUERY_RETRIES = 3
BATTLE_LOCATION_QUERY_BACKOFF_SEC = 0.05

# ── battle 权威判定的 decision 取值(与 Go `logBattleAuthorityResolved` 逐字同值)──
#
# 这七个字符串是"为什么这次登录走了这条路"的**唯一**可观测判据。改一个字面量,
# 按 decision 分组的看板与告警会静默丢一整类样本,而服务本身零错误。
BATTLE_AUTHORITY_PRESENCE_IN_BATTLE = "presence_in_battle"
BATTLE_AUTHORITY_PRESENCE_ONLY = "presence_only_no_match_resolver"
BATTLE_AUTHORITY_RECOVERED_FROM_CLAIM = "recovered_from_ready_claim"
BATTLE_AUTHORITY_ACTIVE_STAGE_NOT_READY = "match_active_stage_not_ready"
BATTLE_AUTHORITY_MATCH_NONE = "match_none"
BATTLE_AUTHORITY_QUERY_DEGRADED = "match_query_degraded"
BATTLE_AUTHORITY_STATE_UNKNOWN_DEGRADED = "match_state_unknown_degraded"

#: matchmaker 未被查询过时 state/stage 字段的日志占位。
#: 与 `UNSPECIFIED`(查了,但权威自己说不知道)必须可判别 —— 两者的处置完全相反:
#: 前者是本部署没接 resolver,后者是 fail-closed 重试点。
MATCH_AUTHORITY_NOT_QUERIED = "NOT_QUERIED"

# ── 在线准入(Redis authority)拒绝原因 —— 与 Go `admissionReject*` 逐字同值 ──
#
# 这五个字面量是 `ds_ticket_admission_rejected` 的 `reason` 维度。合并任意两个都会
# 让"部署没接权威件"(恒不可进,要改配置)与"这一张票不行"(单玩家,要看归属变更)
# 在同一条告警里塌成一类。
ADMISSION_REJECT_REPO_UNAVAILABLE = "admission_repo_unavailable"
ADMISSION_REJECT_TICKET_NO_JTI = "ticket_missing_jti"
ADMISSION_REJECT_MARKER_OWNER_INVALID = "admission_marker_owner_invalid"
ADMISSION_REJECT_PEEK_FAILED = "admission_peek_failed"
ADMISSION_REJECT_HUB_CHECKER_UNAVAILABLE = "hub_assignment_checker_unavailable"
ADMISSION_REJECT_HUB_ASSIGNMENT_STALE = "hub_assignment_not_current"

_R = login_pb2  # 枚举一律从生成的 pb2 引用,不手抄数值


def _now_ms() -> int:
    return int(time.time() * 1000)


def _resume_stage_from_match_stage(stage: int) -> int:
    """matchmaker 的 `PlayerMatchResumeStage` → login 的 `ResumeMatchStage`。

    **必须逐值显式映射**,不准 `ResumeMatchStage(int(stage))` 这种数值直转:
    两个 enum 分属不同 proto 文件、由不同服务演进,今天恰好同序不代表明天还同序。
    一旦其中一侧插值,数值直转会把"排队中"渲染成"确认中",而两边都不报错。
    未知值一律 UNSPECIFIED(fail-safe 到"不知道",不猜一个具体阶段)。
    """
    if stage == match_pb2.PLAYER_MATCH_RESUME_STAGE_STARTING:
        return _R.RESUME_MATCH_STAGE_QUEUED
    if stage == match_pb2.PLAYER_MATCH_RESUME_STAGE_QUEUED:
        return _R.RESUME_MATCH_STAGE_QUEUED
    if stage == match_pb2.PLAYER_MATCH_RESUME_STAGE_CONFIRMING:
        return _R.RESUME_MATCH_STAGE_CONFIRMING
    if stage == match_pb2.PLAYER_MATCH_RESUME_STAGE_ALLOCATING:
        return _R.RESUME_MATCH_STAGE_ALLOCATING
    if stage == match_pb2.PLAYER_MATCH_RESUME_STAGE_READY:
        return _R.RESUME_MATCH_STAGE_READY
    return _R.RESUME_MATCH_STAGE_UNSPECIFIED


def _match_authority_state_name(ma: "lclients.PlayerMatchAuthority | None") -> str:
    """nil-safe 的 state 名。对齐 Go `matchAuthorityStateName`。"""
    if ma is None:
        return MATCH_AUTHORITY_NOT_QUERIED
    return enum_name(match_pb2.PlayerMatchContextState, ma.state)


def _match_authority_stage_name(ma: "lclients.PlayerMatchAuthority | None") -> str:
    """nil-safe 的 stage 名。对齐 Go `matchAuthorityStageName`。"""
    if ma is None:
        return MATCH_AUTHORITY_NOT_QUERIED
    return enum_name(match_pb2.PlayerMatchResumeStage, ma.stage)


def _match_authority_match_id(ma: "lclients.PlayerMatchAuthority | None") -> int:
    if ma is None:
        return 0
    return ma.match_id


def _battle_resume_game_mode_reason(
    bl: "lclients.BattleLocation", ma: "lclients.PlayerMatchAuthority | None"
) -> str:
    """把"为什么没拿到 canonical game_mode"拆成四种可判别原因。对齐 Go 同名函数。

    折叠成一条 "game_mode missing" 会让四种完全不同的故障看起来一样:
    没接 resolver / 撮合已结束 / claim 漂移到别的 match / matchmaker 真的没写 game_mode。
    """
    if ma is None:
        return "match_authority_missing"
    if ma.state != match_pb2.PLAYER_MATCH_CONTEXT_STATE_ACTIVE:
        return "match_state_not_active"
    if ma.match_id != bl.match_id:
        return "claim_match_id_drift"
    return "game_mode_empty"


# ── 结果结构 ─────────────────────────────────────────────────────────────────


class ResumeContextResult:
    """§9.23 最小状态集。字段与 login.proto 的 ResumeContext 一一对应。"""

    __slots__ = (
        "route",
        "match_id",
        "match_stage",
        "game_mode",
        "map_id",
        "placement_state",
        "operation_id",
        "ds_pod_name",
        "ds_instance_uid",
        "ds_instance_epoch",
        "hub_assignment_id",
        "allocation_id",
        "release_track",
        "owner_epoch",
        "entry_state",
        "wait_reason",
        "retry_after_ms",
    )

    def __init__(self) -> None:
        self.route = _R.RESUME_ROUTE_UNSPECIFIED
        self.match_id = 0
        self.match_stage = _R.RESUME_MATCH_STAGE_UNSPECIFIED
        self.game_mode = ""
        self.map_id = 0
        self.placement_state = _R.RESUME_PLACEMENT_STATE_UNSPECIFIED
        self.operation_id = ""
        self.ds_pod_name = ""
        self.ds_instance_uid = ""
        self.ds_instance_epoch = 0
        self.hub_assignment_id = ""
        self.allocation_id = ""
        self.release_track = ""
        self.owner_epoch = 0
        self.entry_state = _R.RESUME_ENTRY_STATE_UNSPECIFIED
        self.wait_reason = _R.RESUME_WAIT_REASON_UNSPECIFIED
        self.retry_after_ms = 0


class AccountRoleView:
    """选角界面上一个角色的可见信息(biz 结构,service 再翻成 proto)。"""

    __slots__ = ("player_id", "role_name", "role_id", "player_no", "level", "last_login_at_ms", "slot")

    def __init__(self, player_id: int = 0) -> None:
        self.player_id = player_id
        self.role_name = ""
        self.role_id = 0
        self.player_no = 0
        self.level = 0
        self.last_login_at_ms = 0
        self.slot = 0


class LoginResult:
    """Login / EnterRole 的产出。账号层与角色层是**两个层面**,别混。"""

    __slots__ = (
        "player_id",
        "session_token",
        "session_exp_ms",
        "hub_ds_addr",
        "hub_ticket",
        "hub_ticket_exp_ms",
        "battle_ds_addr",
        "battle_ticket",
        "battle_ticket_exp_ms",
        "match_id",
        "region_id",
        "cell_id",
        "selected_role_id",
        "player_no",
        "resume",
        "account_id",
        "account_token",
        "account_token_exp_ms",
        "roles",
    )

    def __init__(self) -> None:
        self.player_id = 0
        self.session_token = ""
        self.session_exp_ms = 0
        self.hub_ds_addr = ""
        self.hub_ticket = ""
        self.hub_ticket_exp_ms = 0
        self.battle_ds_addr = ""
        self.battle_ticket = ""
        self.battle_ticket_exp_ms = 0
        self.match_id = 0
        self.region_id = 0
        self.cell_id = 0
        self.selected_role_id = 0
        self.player_no = 0
        self.resume = ResumeContextResult()
        self.account_id = 0
        self.account_token = ""
        self.account_token_exp_ms = 0
        self.roles: list[AccountRoleView] = []

    def copy_base(self) -> "LoginResult":
        out = LoginResult()
        out.player_id = self.player_id
        out.session_token = self.session_token
        out.session_exp_ms = self.session_exp_ms
        out.region_id = self.region_id
        out.cell_id = self.cell_id
        out.player_no = self.player_no
        return out


class BattleTicketResult:
    """`issue_battle_ds_ticket_at_cell` 的产出。对齐 Go `biz.DSTicketResult` 的 battle 子集。

    `battle_ds_addr` **只能**来自 roster 权威门返回的 target,不接受调用方传入 ——
    "谁报一个 match_id 谁就能拿到那局的地址"正是这道门要挡的东西。
    """

    __slots__ = ("ticket", "expires_at_ms", "battle_ds_addr", "jti")

    def __init__(
        self,
        ticket: str = "",
        expires_at_ms: int = 0,
        battle_ds_addr: str = "",
        jti: str = "",
    ) -> None:
        self.ticket = ticket
        self.expires_at_ms = expires_at_ms
        self.battle_ds_addr = battle_ds_addr
        self.jti = jti


class _AccountView:
    """认证完账号、进入角色之前掌握的全部账号层信息。

    `enabled` 区分的是「配置态」与「故障态」,这是本结构最重要的不变式:
    False = 本部署根本没有台账(dev 裸跑),其余字段全零,调用方走「一账号一角色」兼容档;
    台账**存在但读不出来**时调用方拿到的是异常而不是 enabled=False,绝不会误当兼容档。
    """

    __slots__ = ("enabled", "account_id", "token", "token_exp_ms", "roles", "default_player_id")

    def __init__(self) -> None:
        self.enabled = False
        self.account_id = 0
        self.token = ""
        self.token_exp_ms = 0
        self.roles: list[AccountRoleView] = []
        self.default_player_id = 0


def _wait_resume(reason: int, retry_after_ms: int) -> ResumeContextResult:
    """构造 §9.23 WAIT 结果:路由 UNKNOWN(明示"不知道",**不是 Hub**),带有界 retry_after。"""
    out = ResumeContextResult()
    out.route = _R.RESUME_ROUTE_UNKNOWN
    out.entry_state = _R.RESUME_ENTRY_STATE_WAIT
    out.wait_reason = reason
    out.retry_after_ms = retry_after_ms
    return out


def apply_owner_placement(
    out: ResumeContextResult, v: lclients.OwnerPlacementView, now_ms: int
) -> ResumeContextResult:
    """把 owner 权威记录翻译成 §9.23 进场状态。**纯函数**(便于表驱动测试)。

    判定顺序(先安全后可玩):
      ① admit_not_before 屏障未开 → WAIT + ADMIT_BARRIER
         (旧 DS 可能仍在可玩窗口内,此刻放行就是双 DS);retry_after 由屏障推导并钳上界。
      ② ADMITTED 且租约剩余 > 安全余量 → TARGET + STABLE
         (唯一可以宣称"可幂等 no-op"的态)。
      ③ 其余 → TARGET + PENDING(目标已定但未确证,客户端保留 operation 重试)。
    """
    out.owner_epoch = v.owner_epoch
    out.operation_id = v.operation_id
    out.ds_pod_name = v.pod_name
    out.ds_instance_uid = v.instance_uid
    out.ds_instance_epoch = v.instance_epoch
    out.release_track = v.release_track
    if v.owner_type == OWNER_TYPE_HUB:
        out.hub_assignment_id = v.assignment_or_allocation_id
    else:
        out.allocation_id = v.assignment_or_allocation_id

    if v.admit_not_before_ms > now_ms:
        # 屏障未开:保留 exact target 与 owner_epoch(客户端要拿它续用同一 operation),
        # 但状态是 WAIT —— 不得让客户端此刻 Travel / 占座。
        remain = min(v.admit_not_before_ms - now_ms, OWNER_RETRY_AFTER_CEILING_MS)
        out.entry_state = _R.RESUME_ENTRY_STATE_WAIT
        out.wait_reason = _R.RESUME_WAIT_REASON_ADMIT_BARRIER
        out.retry_after_ms = int(remain)
        out.placement_state = _R.RESUME_PLACEMENT_STATE_PENDING
        return out

    out.entry_state = _R.RESUME_ENTRY_STATE_TARGET
    if (
        v.phase == OWNER_PHASE_ADMITTED
        and v.lease_deadline_ms - now_ms > OWNER_LEASE_SKEW_MARGIN_MS
    ):
        out.placement_state = _R.RESUME_PLACEMENT_STATE_STABLE
    else:
        out.placement_state = _R.RESUME_PLACEMENT_STATE_PENDING
    return out


# owner 类型 / 阶段常量(对齐 owner.proto;biz 不依赖生成枚举以免耦合 owner 的 pb)。
OWNER_TYPE_NONE = 0
OWNER_TYPE_HUB = 1
OWNER_TYPE_BATTLE = 2
OWNER_PHASE_ADMITTED = 2


def _owner_type_to_route(owner_type: int) -> int:
    if owner_type == OWNER_TYPE_HUB:
        return _R.RESUME_ROUTE_HUB
    if owner_type == OWNER_TYPE_BATTLE:
        return _R.RESUME_ROUTE_BATTLE
    return _R.RESUME_ROUTE_UNSPECIFIED


# ── LoginUsecase ─────────────────────────────────────────────────────────────


class LoginUsecase:
    """Login / EnterRole / SelectRole / Logout 用例。"""

    def __init__(
        self,
        *,
        repo: ldata.MySQLAccountRepo,
        sessions: ldata.RedisSessionRepo | None,
        notifier: lclients.GrpcLocationNotifier | None,
        hub_assigner: lclients.GrpcHubAssigner | None,
        role_repo: ldata.MySQLPlayerRoleRepo | None,
        snowflake_node,  # noqa: ANN001
        signer: pauth.Signer,
        ds_ticket_signer: ldsticket.DSTicketSigner,
        mock_hub_ds_addr: str,
        hub_region: str,
        dev_skip_password: bool,
        dev_auto_register: bool,
        allowed_role_ids: set[int],
        dev_allow_any_role: bool,
        session_ttl_sec: float,
    ) -> None:
        self._repo = repo
        self._sessions = sessions
        self._notifier = notifier
        self._hub_assigner = hub_assigner
        self._role_repo = role_repo
        self._sf = snowflake_node
        self._signer = signer
        self._ds_signer = ds_ticket_signer
        self._hub_ds_addr = mock_hub_ds_addr
        self._hub_region = hub_region
        self._dev_skip_password = dev_skip_password
        self._dev_auto_register = dev_auto_register
        self._allowed_role_ids = allowed_role_ids
        self._dev_allow_any_role = dev_allow_any_role
        self._session_ttl_sec = session_ttl_sec
        # 以下经 setter 注入(与 Go 的 SetXxx 一一对应),默认 nil/False = 不启用。
        self._session_gen: ldata.MySQLSessionGenerationRepo | None = None
        self._session_gen_enforce = False
        self._require_ticket_sjti = False
        self._require_hub_assignment_binding = False
        self._role_ledger: ldata.MySQLAccountRoleRepo | None = None
        self._profile_seeder: lclients.GrpcProfileSeeder | None = None
        self._owner: lclients.GrpcOwnerClient | None = None
        self._limiter: ldata.RedisLoginRateLimiter | None = None
        self._match_resolver: lclients.GrpcMatchContextResolver | None = None
        self._battle_ticket_issuer: "TicketUsecase | None" = None
        self._rs256_ds_ticket_profile = False

    # ── setter(与 Go 的 SetXxx 逐一对应)──────────────────────────────────

    def set_match_context_resolver(self, resolver) -> None:  # noqa: ANN001
        """注入 matchmaker 只读耐久权威(可 None = presence-only 降级)。

        None 只在 legacy HS256 dev 裸跑档可接受:strict 档下 `_strict_battle_gate_profile`
        为 True,少了这条权威等于失去"presence 未命中 BATTLE 时的第二次确认",
        而那正是双在场窗口的唯一封口。
        """
        self._match_resolver = resolver

    def set_battle_ticket_issuer(self, issuer) -> None:  # noqa: ANN001
        """注入 Battle 票据签发 + 三态路由检查入口(TicketUsecase)。

        必须在对外监听**之前**注入:issuer 为 None 且 locator 已报 BATTLE 时,
        `_try_battle_reconnect` 一律 ErrUnavailable —— 绝不回退到直签票或继续 Hub 链。
        """
        self._battle_ticket_issuer = issuer

    def set_rs256_ds_ticket_profile(self, enabled: bool) -> None:
        """标记本部署是否处于 DSTicket v2(RS256)档。

        它与 `require_hub_assignment_binding` 一起构成 `_strict_battle_gate_profile`。
        两轴正交:前者只看 login.ds_ticket 配没配 verifier,后者是归属绑定的滚动激活
        栅栏。只按后者分档会漏掉"RS256 已配、binding 未激活"这个激活窗口 ——
        那会出现「弱档放行 + 强档出票」:玩家在依赖抖动时被判"不在战斗",却拿到一张
        DS 会正常接受的正式绑定票,于是同时在 Battle 与 Hub 两台可操作 DS(§9 不变量 1)。
        """
        self._rs256_ds_ticket_profile = enabled

    def _strict_battle_gate_profile(self) -> bool:
        """「战斗态查不到时能否放行进 Hub」的唯一档位判据。对齐 Go `strictBattleGateProfile`。

        必须与 `_resolve_hub` 的出票档位判据逐字一致,理由见 `set_rs256_ds_ticket_profile`。
        弱降级(返回 False)只允许存在于两轴都关的 legacy HS256 dev 裸跑档 ——
        那里 login 自签票,本就没有生产级权威可言。
        """
        return self._require_hub_assignment_binding or self._rs256_ds_ticket_profile

    def set_session_generation_repo(self, repo) -> None:  # noqa: ANN001
        self._session_gen = repo

    def set_session_generation_enforce(self, enforce: bool) -> None:
        self._session_gen_enforce = enforce

    def set_require_ticket_sjti(self, require: bool) -> None:
        self._require_ticket_sjti = require

    def set_require_hub_assignment_binding(self, require: bool) -> None:
        self._require_hub_assignment_binding = require

    def set_role_ledger(self, repo) -> None:  # noqa: ANN001
        self._role_ledger = repo

    def set_profile_seeder(self, seeder) -> None:  # noqa: ANN001
        self._profile_seeder = seeder

    def set_owner_client(self, client) -> None:  # noqa: ANN001
        self._owner = client

    def set_login_rate_limiter(self, limiter) -> None:  # noqa: ANN001
        self._limiter = limiter

    # ── 登录失败 Quota ─────────────────────────────────────────────────────

    async def _check_login_fail_lock(self, account: str, client_ip: str) -> None:
        """锁窗内直接拒,**连 bcrypt 都不算** —— 撞库 / 爆破在入口被吸收。

        fail-open:limiter 未注入 / Redis 故障放行(§2 铁律,背压门不是权威门)。
        """
        if self._limiter is None:
            return
        remain, err = await self._limiter.lock_remaining(account, client_ip)
        if err is not None:
            plog.get().warning(
                "login_fail_lock_probe_failed",
                err=str(err),
                hint="背压门 fail-open 放行;惩罚长期失效需要这条日志才有信号",
            )
        if remain > 0:
            plog.get().warning(
                "login_rate_limited", account=account, client_ip=client_ip,
                retry_after_sec=round(remain, 3),
            )
            raise errcode.PandoraError(
                errcode.ErrRateLimited,
                "too many failed logins; retry after %.0fs",
                remain,
            )

    async def _record_login_failure(self, account: str, client_ip: str) -> None:
        if self._limiter is None:
            return
        err = await self._limiter.record_failure(account, client_ip)
        if err is not None:
            # 惩罚窗是**写入侧**,写失败不像读失败那样有 fail-open 兜底 —— 它就是真的漏了一次罚。
            plog.get().warning("login_fail_quota_record_failed", err=str(err))

    # ── Login ──────────────────────────────────────────────────────────────

    async def login(
        self,
        account: str,
        password_hash: str,
        device_id: str,
        defer_role_entry: bool,
        client_ip: str = "",
    ) -> LoginResult:
        log = plog.get()
        started_at = time.monotonic()

        await self._check_login_fail_lock(account, client_ip)

        identity: ldata.AccountIdentity | None = None
        try:
            identity = await self._repo.find_by_account(account)
        except errcode.PandoraError as exc:
            code = errcode.as_code(exc)
            if code != errcode.ErrLoginAccountNotFound or not (
                self._dev_auto_register or self._dev_skip_password
            ):
                if code == errcode.ErrLoginAccountNotFound:
                    # 只有「明确不存在」记凭据失败(账号枚举 / 撞库面);**DB 故障绝不计** ——
                    # 否则故障风暴会把全服玩家锁死(§2 fail-open 方向)。
                    await self._record_login_failure(account, client_ip)
                    log.warning(
                        "login_account_not_found", reason="account_not_found",
                        account=account, device_id=device_id, client_ip=client_ip,
                    )
                else:
                    # 拆开:DB 抖动导致全服登不进时,日志显示成一片「账号不存在」会把排查带偏。
                    log.error(
                        "login_account_lookup_failed", err=str(exc),
                        reason="account_lookup_failed", account=account, device_id=device_id,
                    )
                raise
            identity = await self._ensure_account(account, password_hash)
            log.warning(
                "login_dev_auto_registered", account=account,
                account_id=identity.account_id, player_id=identity.player_id,
            )
        else:
            if self._dev_skip_password:
                log.warning(
                    "login_dev_skip_password", account=account, player_id=identity.player_id
                )
            elif not lpasswd.verify(identity.password_hash, password_hash):
                await self._record_login_failure(account, client_ip)
                log.warning(
                    "login_password_mismatch", reason="password_mismatch",
                    account=account, player_id=identity.player_id,
                    device_id=device_id, client_ip=client_ip,
                )
                raise errcode.PandoraError(
                    errcode.ErrLoginPasswordMismatch, "password mismatch"
                )

        player_id = identity.player_id

        try:
            banned = await self._repo.check_banned(player_id, device_id)
        except errcode.PandoraError as exc:
            # 封禁闸门是 **fail-closed** 点:查询失败 = 登录直接失败。DB 抖动时全服登不进,
            # 而 access log 只有一个泛化 code,看不出是这道闸的查询挂了。
            log.error(
                "ban_check_failed", err=str(exc), account=account,
                player_id=player_id, device_id=device_id,
            )
            raise
        if banned:
            log.warning("login_account_banned", player_id=player_id, device_id=device_id)
            raise errcode.PandoraError(
                errcode.ErrLoginAccountBanned, "account banned player_id=%d", player_id
            )

        # ===== 到这里为止认证的是**账号**;下面才开始进入某个**角色** =====
        account_view = await self._resolve_account_view(account, identity, device_id)

        if defer_role_entry:
            if not account_view.enabled:
                # 本部署没有台账 → 后续 ListAccountRoles / EnterRole 必然 ErrNotImplemented。
                # 此时若照旧回 OK,新客户端拿到「成功 + 空 token + 空列表」,只能永远卡在
                # 选角界面 —— 一个没有任何错误码的死局。把事实直接告诉它。
                log.warning(
                    "login_defer_role_entry_unsupported", account=account, device_id=device_id,
                    hint="本部署未启用角色台账;客户端应回落 defer_role_entry=false 单步登录",
                )
                raise errcode.PandoraError(
                    errcode.ErrNotImplemented,
                    "account role ledger not configured on this deployment",
                )
            out = LoginResult()
            out.account_id = account_view.account_id
            out.account_token = account_view.token
            out.account_token_exp_ms = account_view.token_exp_ms
            out.roles = account_view.roles
            log.info(
                "login_account_only", account=account, account_id=account_view.account_id,
                role_count=len(account_view.roles), device_id=device_id,
                dur_total_ms=int((time.monotonic() - started_at) * 1000),
            )
            return out

        # 兼容路径(旧客户端):服务端替它自动进入**默认角色**(= 台账排序的第一个)。
        entry_player_id = account_view.default_player_id if account_view.enabled else player_id
        res = await self._enter_role_session(
            account=account,
            account_id=account_view.account_id,
            player_id=entry_player_id,
            device_id=device_id,
            started_at=started_at,
        )
        res.account_id = account_view.account_id
        res.account_token = account_view.token
        res.account_token_exp_ms = account_view.token_exp_ms
        res.roles = account_view.roles
        return res

    async def _ensure_account(self, account: str, password_hash: str) -> ldata.AccountIdentity:
        """dev 首登自动注册。account_id / player_id 各铸一个 snowflake。"""
        account_id = self._sf.generate()
        player_id = self._sf.generate()
        stored = lpasswd.hash_password(password_hash)
        try:
            await self._repo.create_account(account_id, player_id, account, stored)
        except errcode.PandoraError as exc:
            if errcode.as_code(exc) != errcode.ErrAlreadyExists:
                raise
            # 并发首登:别人先建好了,回读拿最终结果(**绝不假装自己建成功**,
            # 否则两个请求会各自按不同 player_id 继续,同一个账号裂成两个)。
        else:
            if self._role_ledger is not None:
                with contextlib.suppress(errcode.PandoraError):
                    await self._role_ledger.create(
                        ldata.AccountRole(
                            player_id=player_id,
                            account_id=account_id,
                            slot=0,
                            role_name=account,
                        )
                    )
        return await self._repo.find_by_account(account)

    # ── 账号 / 角色分离 ────────────────────────────────────────────────────

    async def _resolve_account_view(
        self, account: str, identity: ldata.AccountIdentity, device_id: str
    ) -> _AccountView:
        """补齐账号身份、角色台账,并签账号态 token。

        **fail-closed**(2026-08-18 用户拍板):台账已启用时这里任何一步失败都直接让登录
        失败,不降级、不回落。回落的目标是 accounts.player_id,而那恰恰是账号 / 角色分离
        要管住的东西 —— 角色被软删或过户后它可能还指着那个角色,一抖就回落等于给
        「进一个已经不属于自己的角色」开了旁路,而且恰好在最查不清的时候(DB 抖动)打开。

        唯一不算失败的分支是 role_ledger 未注入 —— 那是配置态不是故障。
        """
        log = plog.get()
        view = _AccountView()
        if self._role_ledger is None:
            return view  # 未启用台账:静默降级,不打噪音日志

        try:
            account_id = await self._ensure_account_id(identity)
        except errcode.PandoraError as exc:
            log.error(
                "account_id_resolve_failed", err=str(exc), account=account,
                player_id=identity.player_id, device_id=device_id,
                hint="解不出账号身份 → 无从校验角色归属;fail-closed 拒绝登录",
            )
            raise

        try:
            roles = await self._ensure_account_roles(account_id, account, identity.player_id)
        except errcode.PandoraError as exc:
            log.error(
                "account_roles_resolve_failed", err=str(exc), account=account,
                account_id=account_id, device_id=device_id,
                hint="读不到角色台账 → 无从校验角色归属;fail-closed 拒绝登录",
            )
            raise
        if not roles:
            log.error(
                "account_has_no_role", account=account, account_id=account_id,
                hint="台账里该账号没有 status=0 的角色;fail-closed 拒绝登录,请人工核查",
            )
            raise errcode.PandoraError(
                errcode.ErrLoginNoRole, "account_id=%d has no available role", account_id
            )

        views = await self._decorate_roles(account, roles)
        try:
            token, exp_ms = self._signer.sign_account(account_id, str(uuid.uuid4()))
        except Exception as exc:
            log.error(
                "sign_account_token_failed", err=str(exc), account=account,
                account_id=account_id,
                hint="签不出账号态 token → 新客户端选不了角;fail-closed 拒绝登录",
            )
            raise errcode.PandoraError(
                errcode.ErrInternal, "sign account token failed: %s", exc
            ) from exc

        if views[0].player_id == 0:
            # player_id 是 account_roles 的主键,不可能为 0。真为 0 说明读回来的行是脏的,
            # 拿它当默认进入角色会退化成「进 player_id=0」—— 必须当故障拦下。
            log.error(
                "account_default_role_invalid", account=account, account_id=account_id,
                hint="台账首行 player_id=0;fail-closed 拒绝登录,请人工核查 account_roles",
            )
            raise errcode.PandoraError(
                errcode.ErrInternal, "account_id=%d default role has zero player_id", account_id
            )

        view.enabled = True
        view.account_id = account_id
        view.token = token
        view.token_exp_ms = exp_ms
        view.roles = views
        view.default_player_id = views[0].player_id
        return view

    async def _ensure_account_id(self, identity: ldata.AccountIdentity) -> int:
        if identity.account_id != 0:
            return identity.account_id
        if identity.player_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInternal, "cannot backfill account_id without player_id"
            )
        # 只可能是「新二进制上线后、旧二进制又注册了新账号」这一个窗口。
        return await self._repo.backfill_account_id(identity.player_id, self._sf.generate())

    async def _ensure_account_roles(
        self, account_id: int, account: str, legacy_player_id: int
    ) -> list[ldata.AccountRole]:
        """列出账号下的角色;台账里一个都没有时补建 slot 0。

        补建时把 accounts.player_id 当作既有角色登记,**不铸新 player_id**:
        那个 ID 下可能已经有玩家数据,另铸一个等于把存档丢掉。
        """
        roles = await self._role_ledger.list_by_account(account_id)
        if roles:
            return roles
        if legacy_player_id == 0:
            return []
        slot = await self._role_ledger.next_slot(account_id)
        try:
            await self._role_ledger.create(
                ldata.AccountRole(
                    player_id=legacy_player_id,
                    account_id=account_id,
                    slot=slot,
                    role_name=account,  # 创建角色功能上线前:角色名 = 账号名
                )
            )
        except errcode.PandoraError as exc:
            if errcode.as_code(exc) != errcode.ErrAlreadyExists:
                raise
            # 并发补建:别人先写好了,回读拿最终结果。
        return await self._role_ledger.list_by_account(account_id)

    async def _decorate_roles(
        self, account: str, roles: list[ldata.AccountRole]
    ) -> list[AccountRoleView]:
        views: list[AccountRoleView] = []
        for r in roles:
            v = AccountRoleView(r.player_id)
            v.role_name = r.role_name
            v.slot = r.slot
            v.last_login_at_ms = r.last_login_at_ms
            # 已选职业外观:弱依赖,查不到按 0(未选过)。
            if self._role_repo is not None:
                with contextlib.suppress(errcode.PandoraError):
                    v.role_id = await self._role_repo.get_role(r.player_id)
            # 角色编号:展示专用,fail-soft 置 0(客户端显示「生成中」)。
            with contextlib.suppress(errcode.PandoraError):
                v.player_no = await self._repo.get_player_no(r.player_id)
            profile = await self._seed_role_profile(account, r)
            if profile is not None:
                v.role_name = profile.nickname
                v.level = profile.level
            views.append(v)
        return views

    @staticmethod
    def _log_seed_failure(
        exc: errcode.PandoraError, account: str, r: ldata.AccountRole
    ) -> None:
        """播种失败的三类处置。刻意用不同事件名 / 级别 —— 它们的**后续动作不同**:

          unimplemented   等对端上线,自动收敛,不是故障(§9.21 弱依赖降级)
          name_too_long   永不自动收敛,必须人工改名或改配置
          其它(不可达)  下次登录自动重试

        抽成独立方法而不是内联在 except 里,是为了让 `except BaseException` 紧跟在
        `except asyncio.CancelledError: raise` 后面 —— 中间隔着几十行时,"取消有没有
        被放行"这件事只能靠逐行读代码确认,而机械检查(tests/test_service_layer_contract.py)
        只看邻近若干行。让结构本身把答案摆在眼前,比靠人记住可靠。
        """
        log = plog.get()
        code = errcode.as_code(exc)
        if code == errcode.ErrNotImplemented:
            # §9.21:对端还没滚上这个 RPC。重试永远不会成功,只能等它上线,不是故障。
            log.info(
                "role_profile_seed_unimplemented", player_id=r.player_id,
                hint="player 服务尚未滚上 EnsureProfile;角色名暂用台账名,对端上线后自动收敛",
            )
        elif code == errcode.ErrInvalidArg:
            # 账号名超出 player 侧昵称上限,而 accounts.account 是 VARCHAR(64) ——
            # 两侧上限本就不等宽,重试无用(名字不会自己变短),必须人工介入。
            log.warning(
                "role_profile_seed_name_too_long", player_id=r.player_id,
                account_runes=len(account), err=str(exc),
                hint="账号名超出昵称上限;该角色沿用 player 默认名,永不自动收敛,需人工改名或改配置",
            )
        else:
            log.warning(
                "role_profile_seed_failed", err=str(exc), player_id=r.player_id,
                hint="player 服务不可达/超时;角色名暂用台账名,下次登录重试",
            )

    async def _seed_role_profile(
        self, account: str, r: ldata.AccountRole
    ) -> lclients.SeededProfile | None:
        """调 player 建档 + 播种角色名,返回该角色的权威显示名。

        None 表示这次没拿到权威值(未配 seeder / 不可达 / 对端版本没这个 RPC / 重名),
        调用方继续用台账里的名字。三类失败刻意用不同日志级别 —— 见各分支。
        """
        if self._profile_seeder is None:
            return None
        log = plog.get()
        try:
            profile = await self._profile_seeder.ensure_profile(
                r.player_id, r.role_name, timeout_sec=PROFILE_SEED_TIMEOUT_SEC
            )
        except asyncio.CancelledError:
            # 取消必须穿透:吞掉之后取消不再传播,优雅停机会把它变成一次"播种失败"。
            raise
        except errcode.PandoraError as exc:
            # 三类失败的处置不同(见 _log_seed_failure),但结论都是"这次没拿到权威名"。
            self._log_seed_failure(exc, account, r)
            return None
        except BaseException as exc:  # noqa: BLE001 —— 弱依赖:任何失败都不阻断登录
            log.warning("role_profile_seed_failed", err=str(exc), player_id=r.player_id)
            return None
        if profile.created:
            log.info(
                "role_profile_seeded", account=account, player_id=r.player_id,
                role_name=profile.nickname,
            )
        return profile

    async def list_account_roles(self, account_id: int) -> list[AccountRoleView]:
        """列出账号下全部可用角色。account_id 由调用方从**账号态 JWT** 的 sub 取,
        不接受请求体自报 —— 否则任何人都能列别人账号下的角色。"""
        if account_id == 0:
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "list account roles: missing account identity"
            )
        if self._role_ledger is None:
            raise errcode.PandoraError(
                errcode.ErrNotImplemented,
                "account role ledger not configured on this deployment",
            )
        roles = await self._role_ledger.list_by_account(account_id)
        return await self._decorate_roles("", roles)

    async def enter_role(
        self, account_id: int, player_id: int, device_id: str, client_ip: str = ""
    ) -> LoginResult:
        """两步登录第二步。

        **安全要点**:player_id 来自请求体,但绝不当作身份 —— 服务端按 account_roles
        回查「这个角色是不是挂在 account_id 名下」,不属于就拒。少了这一步,
        任何拿到自己账号 token 的人都能填别人的 player_id 直接进别人的号。
        """
        del client_ip  # EnterRole 不计凭据失败(它没有凭据),保留参数与 Go 同形
        started_at = time.monotonic()
        log = plog.get()
        if account_id == 0:
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "enter role: missing account identity"
            )
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "enter role: player_id must be > 0")
        if self._role_ledger is None:
            raise errcode.PandoraError(
                errcode.ErrNotImplemented,
                "account role ledger not configured on this deployment",
            )

        role = await self._role_ledger.get_by_player(player_id)
        if role.account_id != account_id:
            # 越权尝试:必须留痕。两个 ID 都记下来,便于事后判断是改包攻击还是客户端 bug。
            log.warning(
                "enter_role_not_owned", account_id=account_id,
                role_account_id=role.account_id, player_id=player_id, device_id=device_id,
                hint="请求方账号与角色归属不符;拒绝进入",
            )
            raise errcode.PandoraError(
                errcode.ErrLoginRoleNotOwned,
                "role player_id=%d does not belong to account_id=%d",
                player_id,
                account_id,
            )
        if role.status != 0:
            raise errcode.PandoraError(
                errcode.ErrLoginRoleNotFound,
                "role player_id=%d is not available (status=%d)",
                player_id,
                role.status,
            )

        # 封禁是**账号级**的,挂在 accounts.player_id 上,不是挂在本次要进入的角色上。
        # 拿 role.player_id 去查会让账号下的其它角色绕过封禁。
        try:
            identity = await self._repo.find_by_account_id(account_id)
        except errcode.PandoraError as exc:
            log.error(
                "enter_role_account_lookup_failed", err=str(exc),
                account_id=account_id, player_id=player_id,
            )
            raise
        try:
            banned = await self._repo.check_banned(identity.player_id, device_id)
        except errcode.PandoraError as exc:
            log.error(
                "ban_check_failed", err=str(exc), account_id=account_id,
                player_id=player_id, device_id=device_id,
            )
            raise
        if banned:
            log.warning(
                "login_account_banned", account_id=account_id,
                player_id=player_id, device_id=device_id,
            )
            raise errcode.PandoraError(
                errcode.ErrLoginAccountBanned, "account banned account_id=%d", account_id
            )

        res = await self._enter_role_session(
            account=role.role_name,
            account_id=account_id,
            player_id=player_id,
            device_id=device_id,
            started_at=started_at,
        )
        res.account_id = account_id
        return res

    # ── 进入角色(Login 兼容路径与 EnterRole 唯一共用的后半段)──────────────

    async def _enter_role_session(
        self, *, account: str, account_id: int, player_id: int, device_id: str, started_at: float
    ) -> LoginResult:
        """分配会话代际 → 写 session → 路由落点 → 重连分诊 → 角色门 → 分配 Hub → owner 复核 → 交付。

        拆出来的意义不只是复用:两条路径若各写一份,任何一条漏掉交付终检、漏掉 owner 复核、
        或漏掉 WAIT 分支,都会变成一条绕过进场防线的后门,而且只在其中一条路径上复现。
        """
        del account_id  # 目前只用于日志上下文,保留参数与 Go 的 roleEntry 同形
        log = plog.get()
        sess_jti = str(uuid.uuid4())
        try:
            session_token, sess_exp_ms = self._signer.sign_session(player_id, sess_jti)
        except Exception as exc:
            log.error("sign_session_failed", err=str(exc), player_id=player_id)
            raise errcode.PandoraError(
                errcode.ErrInternal, "sign session failed: %s", exc
            ) from exc

        sess_ttl = self._session_ttl_sec
        sess_gen = 0
        if self._session_gen is not None:
            try:
                lease = await self._session_gen.persist_session_jti(player_id, sess_jti)
                sess_gen = lease.generation
            except ldata.CommitAmbiguousError as exc:
                # COMMIT 结果不确定。**不猜** —— 用本次 jti 作唯一标记读回权威判定
                # (§9.22 的落法是把不确定态判定掉,而不是把它当失败)。
                sess_gen = await self._resolve_ambiguous_session_generation(
                    player_id, sess_jti, sess_ttl, getattr(exc, "generation", 0)
                )
            except errcode.PandoraError as exc:
                log.error("session_generation_persist_failed", err=str(exc), player_id=player_id)
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "session generation persistence unavailable; login rejected: %s",
                    exc,
                ) from exc

        if self._sessions is not None:
            try:
                await self._sessions.set(
                    player_id, session_token, sess_jti, device_id, sess_ttl, sess_gen
                )
            except errcode.PandoraError as exc:
                # ErrSessionSuperseded = 并发更新一代登录已完成写入,本次定序失败;
                # 其余为基础设施错误。两者都不得交付凭据。
                superseded = errcode.as_code(exc) == errcode.ErrSessionSuperseded
                log.warning(
                    "session_set_failed", err=str(exc), player_id=player_id, gen=sess_gen,
                    reason="superseded" if superseded else "infra",
                    account=account, device_id=device_id, sess_jti=sess_jti,
                )
                if self._session_gen is not None and not superseded:
                    # 定序失败(superseded)**不进**补偿路径:行已属于赢家,墓碑会破坏别人的登录。
                    await self._reconcile_failed_session_write(
                        player_id, sess_jti, sess_gen, sess_ttl
                    )
                raise

        # 确定性 region/cell 路由落点。Python 侧只实现单 Cell(cell_route.mode 非空时
        # BaseConf 已在加载期拒启),故恒 0/0 —— 与 Go 的 router==nil 分支同值。
        region_id, cell_id = 0, 0

        # 角色编号:fail-soft —— 查失败只打日志置 0(客户端显示「生成中」),
        # 绝不因展示字段拒登录。
        player_no = 0
        try:
            player_no = await asyncio.wait_for(
                self._repo.get_player_no(player_id), timeout=PLAYER_NO_READ_TIMEOUT_SEC
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            log.warning("player_no_read_failed", err=str(exc), player_id=player_id)
            player_no = 0

        base = LoginResult()
        base.player_id = player_id
        base.session_token = session_token
        base.session_exp_ms = sess_exp_ms
        base.region_id = region_id
        base.cell_id = cell_id
        base.player_no = player_no

        def log_outcome(out: LoginResult) -> None:
            """登录返回面的**唯一收口日志**(§11.3 R1/R3)。

            必须是 INFO:Login 是未鉴权面,日志上下文里没有 player_id,这一行是全链
            **唯一**把 account ↔ player_id ↔ device_id ↔ trace_id 绑在一起的地方。
            一降 Debug,客服拿到账号名就无法映射到 player_id,hub_allocator / locator /
            matchmaker / owner 那边按 player_id 索引的日志一条都串不起来。

            WAIT 是「登录成功但进不去场景」的降级,走 Warn + 枚举 wait_reason:
            WAIT 返回的是 code=OK,access log 落 rpc_ok(Debug),线上 info 级下
            「玩家卡在登录转圈」在后端原本完全不可见。

            ★ 四个枚举必须走 `enum_name` 而不是 `.Name()`。这不是日志美观问题:
            本函数在**返回前**同步调用,`.Name()` 对未知值抛 ValueError 会把整次
            **登录本身**炸掉。而给 ResumeWaitReason / ResumeRoute 加枚举值是 additive、
            双向兼容的常规动作(§9 不变式 17),滚动升级时新副本返回、旧副本转发就会
            命中 —— 那意味着一次合法的 proto 演进能让旧副本上的玩家登不进去。
            """
            r = out.resume
            fields = dict(
                account=account, player_id=player_id, device_id=device_id,
                entry_state=enum_name(_R.ResumeEntryState, r.entry_state),
                route=enum_name(_R.ResumeRoute, r.route),
                placement_state=enum_name(_R.ResumePlacementState, r.placement_state),
                wait_reason=enum_name(_R.ResumeWaitReason, r.wait_reason),
                retry_after_ms=r.retry_after_ms,
                selected_role_id=out.selected_role_id,
                session_gen=sess_gen, sess_jti=sess_jti, session_exp_ms=out.session_exp_ms,
                owner_epoch=r.owner_epoch, operation_id=r.operation_id,
                hub_ds_addr=out.hub_ds_addr, hub_ticket_exp_ms=out.hub_ticket_exp_ms,
                hub_assignment_id=r.hub_assignment_id,
                battle_ds_addr=out.battle_ds_addr, match_id=r.match_id,
                match_stage=enum_name(_R.ResumeMatchStage, r.match_stage),
                game_mode=r.game_mode, map_id=r.map_id,
                ds_pod=r.ds_pod_name, ds_instance_uid=r.ds_instance_uid,
                ds_instance_epoch=r.ds_instance_epoch, release_track=r.release_track,
                region_id=out.region_id, cell_id=out.cell_id, player_no=out.player_no,
                dur_total_ms=int((time.monotonic() - started_at) * 1000),
            )
            if r.entry_state == _R.RESUME_ENTRY_STATE_WAIT:
                log.warning("login_wait_returned", **fields)
            else:
                log.info("login_ok", **fields)

        async def deliver(out: LoginResult) -> LoginResult:
            """交付前置终检:WAIT 与正常返回共用同一道门,避免 WAIT 路径成为绕过终检的后门。"""
            await self._fence_login_delivery(player_id, sess_jti)
            log_outcome(out)
            return out

        # ── 断线重连分诊 ────────────────────────────────────────────────
        # presence 投影 + matchmaker 耐久权威 + roster 三态门(§9.22)。
        # 三种出口:签出重连票直接返回 / 拿到终局 fence 继续 Hub 链 / 不可判定按 WAIT。
        hub_fence_match_id = 0
        if self._notifier is None:
            if self._strict_battle_gate_profile():
                # strict 档没有 locator = 无法证明玩家不在战斗。放行进 Hub 就是双在场。
                log.error(
                    "login_locator_not_configured",
                    reason="battle_gate_requires_locator",
                    account=account, player_id=player_id,
                    hint="strict 档必须配 login.locator.addr;否则无法证明玩家不在战斗",
                )
                out = base.copy_base()
                out.resume = _wait_resume(
                    _R.RESUME_WAIT_REASON_OWNER_UNKNOWN, LOGIN_WAIT_RETRY_AFTER_MS
                )
                return await deliver(out)
        else:
            try:
                reconnect, hub_fence_match_id = await self._try_battle_reconnect(
                    player_id, device_id, session_token, sess_exp_ms,
                    region_id, cell_id, sess_jti,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                # §9.23:会话已建立,暂时失败不得清会话 / 不得要求重输密码,
                # 带着 session 返回 WAIT,由客户端按 retry_after 重查同一入口。
                log.warning(
                    "login_battle_reconnect_unresolved", err=str(exc),
                    account=account, player_id=player_id,
                )
                out = base.copy_base()
                out.resume = _wait_resume(
                    _R.RESUME_WAIT_REASON_OWNER_UNKNOWN, LOGIN_WAIT_RETRY_AFTER_MS
                )
                return await deliver(out)
            if reconnect is not None:
                # 重连票也必须过交付终检(与 Hub 链同一道门):签票期间会话可能已被
                # 新登录轮换,交付旧凭据等于让被顶设备重新进场。
                await self._fence_login_delivery(player_id, sess_jti)
                reconnect.player_no = player_no
                log_outcome(reconnect)
                return reconnect

        # ── 角色权威门(§9.23 最小状态集)────────────────────────────────
        # 必须区分三种结果,不能都折叠成 role=0:
        #   查询失败 → WAIT/ROLE_UNKNOWN(不得冒充"未选角");
        #   role=0(权威明确"没选过") → ROLE_REQUIRED,并且**到此为止**
        #     (§9.23 明文:未选角时不得提前分配 Hub、占座或签进场票);
        #   role>0 → 继续分配 Hub。
        try:
            selected_role_id = await self._load_selected_role(player_id)
        except errcode.PandoraError:
            out = base.copy_base()
            out.resume = _wait_resume(
                _R.RESUME_WAIT_REASON_ROLE_UNKNOWN, LOGIN_WAIT_RETRY_AFTER_MS
            )
            return await deliver(out)
        if self._role_repo is not None and selected_role_id == 0:
            out = base.copy_base()
            out.resume.route = _R.RESUME_ROUTE_HUB
            out.resume.entry_state = _R.RESUME_ENTRY_STATE_ROLE_REQUIRED
            return await deliver(out)

        # ── B1:先建立 LOGIN_PENDING 权威位置,再调 Hub allocator ────────
        pending_notified = False
        if self._require_hub_assignment_binding:
            if self._notifier is None:
                log.error(
                    "login_locator_not_configured", reason="b1_hub_assign_requires_locator",
                    account=account, player_id=player_id,
                    hint="require_hub_assignment_binding 已开;必须配 player_locator 地址",
                )
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "player locator is required before B1 hub assignment",
                )
            try:
                await self._notifier.notify_login_pending(player_id, device_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                log.warning(
                    "locator_notify_failed", err=str(exc), player_id=player_id,
                    reason="login_pending_write_failed", device_id=device_id,
                    hint="LOGIN_PENDING 写失败:不分配 Hub,带会话返回 WAIT 由客户端重查",
                )
                out = base.copy_base()
                out.resume = _wait_resume(
                    _R.RESUME_WAIT_REASON_OWNER_UNKNOWN, LOGIN_WAIT_RETRY_AFTER_MS
                )
                return await deliver(out)
            pending_notified = True

        # ── 解析 hub 分片 + hub 票据 ────────────────────────────────────
        try:
            # hub_fence_match_id 来自三态门判定的**显式终局**对局:把它签进 hub 票的
            # source_match_id,DS 侧据此拒绝那局的残留连接(Battle→Hub 回流栅栏)。
            hub_addr, hub_ticket, hub_exp_ms = await self._resolve_hub(
                player_id, region_id, cell_id, selected_role_id,
                hub_fence_match_id, sess_jti,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            log.error(
                "resolve_hub_failed", err=str(exc), player_id=player_id,
                reason="hub_resolve_failed", account=account, role_id=selected_role_id,
                hint="带会话返回 WAIT,不清空会话不要求重新登录(§9.23)",
            )
            out = base.copy_base()
            out.resume = _wait_resume(
                _R.RESUME_WAIT_REASON_NO_CAPACITY, LOGIN_WAIT_RETRY_AFTER_MS
            )
            return await deliver(out)

        # 纯记账副作用,移出登录关键路径(不给 prod 5s 登录预算叠加 MySQL 往返)。
        self._touch_device_async(player_id, device_id)
        self._touch_role_login_async(player_id)

        # local/off 在 Hub 解析后 best-effort 通知;B1 已在分配前成功写入,不能重复写。
        if not pending_notified and self._notifier is not None:
            try:
                await self._notifier.notify_login_pending(player_id, device_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                log.warning(
                    "locator_notify_failed", err=str(exc), player_id=player_id,
                    reason="login_pending_write_failed_weak", device_id=device_id,
                    hint="local/off 弱依赖:只影响 presence 投影,不阻断登录",
                )

        out = base.copy_base()
        out.hub_ds_addr = hub_addr
        out.hub_ticket = hub_ticket
        out.hub_ticket_exp_ms = hub_exp_ms
        out.selected_role_id = selected_role_id

        # Resume 必须是**真正的 §9.23 TARGET**,不能是硬编码的 {Route: HUB}。
        decided, owned = await self._resolve_resume_from_owner(player_id)
        if not decided or owned.entry_state == _R.RESUME_ENTRY_STATE_WAIT:
            # owner 明确"无归属" / 结果不可判定,却刚拿到 Hub 票:不把一张没有 exact owner
            # TARGET 背书的票交给客户端。assignment 已 durable,下一次重查可直接重签收敛。
            log.warning(
                "login_owner_missing_after_assign", player_id=player_id,
                reason="owner_record_missing_after_assign", account=account,
                hub_ds_addr=hub_addr,
                owner_entry_state=enum_name(_R.ResumeEntryState, owned.entry_state),
                hint="扣住 Hub 票,返回 WAIT 让客户端重查 exact owner",
            )
            out.hub_ds_addr, out.hub_ticket, out.hub_ticket_exp_ms = "", "", 0
            out.resume = _wait_resume(
                _R.RESUME_WAIT_REASON_OWNER_UNKNOWN, OWNER_UNKNOWN_RETRY_AFTER_MS
            )
        else:
            out.resume = owned
        return await deliver(out)

    # ── 战斗态两层权威(presence 投影 + matchmaker 耐久事实)──────────────

    async def _query_battle_location(self, player_id: int) -> lclients.BattleLocation:
        """带就地短重试的 presence 查询。对齐 Go `queryBattleLocation`。

        重试是为了吸收**单次抖动**,不是为了等故障恢复:耗尽后抛错交给调用方按 profile
        判定(strict → fail-closed;弱档 → 继续 Hub 链)。这里绝不"重试完就当没在战斗" ——
        那正是 §16.10 点名禁止的"到期后假设成功"。
        """
        log = plog.get()
        last_err = ""
        for attempt in range(1, BATTLE_LOCATION_QUERY_RETRIES + 1):
            if attempt > 1:
                # 取消必须立刻生效:登录被上游放弃后不该继续占着 locator 的并发额度。
                await asyncio.sleep(BATTLE_LOCATION_QUERY_BACKOFF_SEC)
            try:
                return await self._notifier.get_battle_location(player_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                last_err = str(exc)
                log.debug(
                    "battle_location_query_retry", err=last_err, player_id=player_id,
                    attempt=attempt, max=BATTLE_LOCATION_QUERY_RETRIES,
                )
        raise errcode.PandoraError(
            errcode.ErrUnavailable,
            "battle location query failed after %d attempts: %s",
            BATTLE_LOCATION_QUERY_RETRIES,
            last_err,
        )

    def _log_battle_authority_resolved(
        self,
        player_id: int,
        decision: str,
        bl: lclients.BattleLocation,
        ma: "lclients.PlayerMatchAuthority | None",
    ) -> None:
        """两层权威的**唯一**收口日志。对齐 Go `logBattleAuthorityResolved`。

        必须 INFO 且每次登录一条:"玩家到底被判成在不在战斗、依据是 presence 还是
        matchmaker claim"是双在场类事故的第一判据,DEBUG 下线上完全不可见。
        """
        plog.get().info(
            "battle_authority_resolved",
            player_id=player_id,
            decision=decision,
            in_battle=bl.in_battle,
            presence_state=enum_name(locator_pb2.LocationState, bl.presence_state),
            locator_match_id=bl.match_id,
            match_state=_match_authority_state_name(ma),
            match_stage=_match_authority_stage_name(ma),
            claim_match_id=_match_authority_match_id(ma),
            strict_profile=self._strict_battle_gate_profile(),
        )

    async def _resolve_battle_authority(
        self, player_id: int
    ) -> tuple[lclients.BattleLocation, "lclients.PlayerMatchAuthority | None"]:
        """presence 投影 + matchmaker 耐久权威的合成判定。对齐 Go `resolveBattleAuthority`。

        两条不对称的短路,方向都不能反:
          · presence **已经**报 BATTLE → 不必再查 matchmaker(结论只会更强不会更弱);
          · presence 未命中 → **必须**再查一次 matchmaker,否则 READY↔投影 之间的窗口
            会把还在对局里的玩家路由回 Hub。
        """
        log = plog.get()
        bl = await self._query_battle_location(player_id)
        if bl.in_battle or self._match_resolver is None:
            decision = (
                BATTLE_AUTHORITY_PRESENCE_IN_BATTLE
                if bl.in_battle
                else BATTLE_AUTHORITY_PRESENCE_ONLY
            )
            self._log_battle_authority_resolved(player_id, decision, bl, None)
            return bl, None

        try:
            ma = await self._match_resolver.resolve_player_match_context(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            if self._strict_battle_gate_profile():
                # strict 档:查不到耐久权威 = 无法证明玩家不在对局 → fail-closed。
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "cannot consult durable match authority; retry: %s",
                    exc,
                ) from exc
            log.warning(
                "match_authority_query_degraded", err=str(exc), player_id=player_id,
                hint="legacy dev 档降级为 presence-only;strict 档在此 fail-closed",
            )
            self._log_battle_authority_resolved(
                player_id, BATTLE_AUTHORITY_QUERY_DEGRADED, bl, None
            )
            return bl, None

        if ma.state == match_pb2.PLAYER_MATCH_CONTEXT_STATE_ACTIVE:
            if ma.stage == match_pb2.PLAYER_MATCH_RESUME_STAGE_READY and ma.match_id != 0:
                # 撮合已 READY 但 presence 还没写上(或已蒸发):以耐久 claim 为准,
                # 把 in_battle 恢复出来。少了这一支就是"READY 之后立刻重登 → 掉回大厅"。
                log.info(
                    "battle_authority_recovered_from_match_claim",
                    player_id=player_id, match_id=ma.match_id,
                )
                recovered = lclients.BattleLocation(
                    in_battle=True,
                    match_id=ma.match_id,
                    battle_addr=ma.battle_ds_addr,
                    presence_state=bl.presence_state,
                )
                self._log_battle_authority_resolved(
                    player_id, BATTLE_AUTHORITY_RECOVERED_FROM_CLAIM, recovered, ma
                )
                return recovered, ma
            # ACTIVE 但还没 READY(排队 / 确认 / 分配中):玩家不在任何 DS 上,
            # 走 Hub 链是对的;但 ma 要带回去,resume 的 match_stage / game_mode 靠它。
            self._log_battle_authority_resolved(
                player_id, BATTLE_AUTHORITY_ACTIVE_STAGE_NOT_READY, bl, ma
            )
            return bl, ma

        if ma.state == match_pb2.PLAYER_MATCH_CONTEXT_STATE_NONE:
            self._log_battle_authority_resolved(
                player_id, BATTLE_AUTHORITY_MATCH_NONE, bl, ma
            )
            return bl, ma

        # UNSPECIFIED = matchmaker 自己也不确定(读取错误 / 索引漂移)。
        if self._strict_battle_gate_profile():
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "durable match authority state unknown; retry"
            )
        log.warning(
            "match_authority_state_unknown_degraded", player_id=player_id,
            hint="matchmaker 返回 UNSPECIFIED;legacy dev 档降级,strict 档 fail-closed",
        )
        # 刻意返回 None 而不是这个 ma:状态不可判定的 ma 不该再被 resume 拿去填字段。
        self._log_battle_authority_resolved(
            player_id, BATTLE_AUTHORITY_STATE_UNKNOWN_DEGRADED, bl, ma
        )
        return bl, None

    async def _build_battle_resume(
        self,
        player_id: int,
        bl: lclients.BattleLocation,
        ma: "lclients.PlayerMatchAuthority | None",
    ) -> ResumeContextResult:
        """组装 BATTLE 重连的 resume。对齐 Go `buildBattleResume`。

        路由权威仍然是 owner(§9.23 query-first):本函数只负责把 match 维度的三个字段
        (match_id / match_stage / game_mode+map_id)叠加到 owner 的判定上。
        owner 说不是 BATTLE → 一律 WAIT,**绝不**用 presence 覆盖 owner 的结论。
        """
        log = plog.get()
        if ma is None and self._match_resolver is not None:
            # 懒查:presence 直接报 BATTLE 时上游短路过 matchmaker,但 game_mode 只有
            # 那里有。冷启动客户端要用它恢复 x-pandora-game-mode 路由头。
            try:
                ma = await self._match_resolver.resolve_player_match_context(player_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                if self._strict_battle_gate_profile():
                    log.error(
                        "battle_resume_game_mode_unavailable", err=str(exc),
                        player_id=player_id, match_id=bl.match_id,
                        reason="match_query_failed",
                        hint="strict 档 fail-closed;WAIT 原因被硬编码成 OWNER_UNKNOWN,真根因在 matchmaker 查询",
                    )
                    raise errcode.PandoraError(
                        errcode.ErrUnavailable,
                        "cannot resolve canonical game_mode for battle resume; retry: %s",
                        exc,
                    ) from exc
                log.warning(
                    "battle_resume_game_mode_query_degraded", err=str(exc),
                    player_id=player_id, match_id=bl.match_id,
                )

        game_mode = ""
        map_id = 0
        # 三个条件缺一不可:claim 漂到别的 match 时,那局的 game_mode 用在这局上
        # 会把客户端路由到错误的撮合命名空间。
        if (
            ma is not None
            and ma.state == match_pb2.PLAYER_MATCH_CONTEXT_STATE_ACTIVE
            and ma.match_id == bl.match_id
        ):
            game_mode = ma.game_mode
            map_id = ma.map_id
        if not game_mode:
            if self._strict_battle_gate_profile():
                log.error(
                    "battle_resume_game_mode_unavailable", player_id=player_id,
                    match_id=bl.match_id,
                    reason=_battle_resume_game_mode_reason(bl, ma),
                    match_state=_match_authority_state_name(ma),
                    match_stage=_match_authority_stage_name(ma),
                    claim_match_id=_match_authority_match_id(ma),
                    hint="缺 game_mode 的 BATTLE resume 会让客户端拒绝路由,交付它等于交付一个 bug",
                )
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "canonical game_mode unavailable for battle resume (match_id=%d); retry",
                    bl.match_id,
                )
            log.warning(
                "battle_resume_game_mode_missing", player_id=player_id,
                match_id=bl.match_id, reason=_battle_resume_game_mode_reason(bl, ma),
            )

        # presence 确实报 BATTLE = 玩家已经在 DS 上跑着 → RUNNING;
        # 只有"从 claim 恢复出来的 in_battle"才需要按撮合阶段细分。
        stage = _R.RESUME_MATCH_STAGE_RUNNING
        if bl.presence_state != locator_pb2.LOCATION_STATE_BATTLE and ma is not None:
            stage = _resume_stage_from_match_stage(ma.stage)

        decided, owned = await self._resolve_resume_from_owner(player_id)
        if (
            not decided
            or owned.entry_state == _R.RESUME_ENTRY_STATE_WAIT
            or owned.route != _R.RESUME_ROUTE_BATTLE
        ):
            # owner 是路由唯一权威。它说"没归属 / 不确定 / 归属在 Hub"时,presence 报的
            # BATTLE 不能反过来推翻它 —— 那会造出第二个 owner(§9.22)。
            log.warning(
                "battle_resume_owner_not_battle", player_id=player_id,
                match_id=bl.match_id, owner_decided=decided,
                owner_route=enum_name(_R.ResumeRoute, owned.route),
                hint="按 WAIT/OWNER_UNKNOWN 退避重查,不用 presence 覆盖 owner 判定",
            )
            return _wait_resume(
                _R.RESUME_WAIT_REASON_OWNER_UNKNOWN, OWNER_UNKNOWN_RETRY_AFTER_MS
            )
        owned.match_id = bl.match_id
        owned.match_stage = stage
        owned.game_mode = game_mode
        owned.map_id = map_id
        return owned

    async def _try_battle_reconnect(
        self,
        player_id: int,
        device_id: str,
        session_token: str,
        sess_exp_ms: int,
        region_id: int,
        cell_id: int,
        sess_jti: str,
    ) -> tuple["LoginResult | None", int]:
        """断线重连三态门。对齐 Go `tryBattleReconnect`。

        返回 `(结果, terminal_fence_match_id)`:
          · `(LoginResult, 0)` —— 判定在对局中,已签出 roster 权威门核准的重连票;
          · `(None, 0)`        —— 判定不在对局中,交给 Hub 链;
          · `(None, match_id)` —— 对局**显式终局**,放行进 Hub 但要把这局的 match_id
            作为 source_match_id fence 签进 hub 票(Battle→Hub 回流栅栏);
          · 抛 PandoraError  —— 不可判定,调用方按 WAIT 退避重查。
        """
        log = plog.get()
        try:
            bl, ma = await self._resolve_battle_authority(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            strict = self._strict_battle_gate_profile()
            log.warning(
                "battle_location_query_failed", err=str(exc), player_id=player_id,
                reason=(
                    "battle_authority_unavailable_fail_closed"
                    if strict
                    else "battle_authority_degraded_continue_hub"
                ),
                strict_profile=strict,
            )
            if strict:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "cannot prove player is outside battle before B1 hub assignment: %s",
                    exc,
                ) from exc
            return None, 0

        if not bl.in_battle:
            return None, 0

        if self._battle_ticket_issuer is None:
            # locator 已经明确说"在战斗",却没有签票权威 —— 继续 Hub 链就是双在场。
            log.error(
                "battle_reconnect_ticket_issuer_unavailable",
                reason="ticket_issuer_not_configured",
                player_id=player_id, match_id=bl.match_id,
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle reconnect ticket authority unavailable"
            )

        state, route_err = await self._battle_ticket_issuer.inspect_battle_route(
            player_id, bl.match_id
        )
        if state == lbattleroute.BattleRouteState.TERMINAL:
            log.info(
                "battle_reconnect_skipped_terminal_match", player_id=player_id,
                match_id=bl.match_id, decision="route_hub_with_source_match_fence",
            )
            return None, bl.match_id
        if state != lbattleroute.BattleRouteState.ACTIVE:
            # UNKNOWN 一律可重试,**绝不**折叠成终态放行(P0 2026-07-15 的根因)。
            log.warning(
                "battle_reconnect_route_unknown_retryable",
                err=str(route_err) if route_err is not None else "",
                reason="battle_route_unknown", player_id=player_id, match_id=bl.match_id,
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle route authority temporarily unavailable; retry",
            )

        # resume 必须在签票**之前**建:它内部会 fail-closed(owner 不是 BATTLE / 缺
        # game_mode),签完票再失败等于白白铸一个 jti,还要靠 TTL 自然过期。
        resume = await self._build_battle_resume(player_id, bl, ma)

        try:
            issued = await self._battle_ticket_issuer.issue_battle_ds_ticket_at_cell(
                player_id, bl.match_id, region_id, cell_id, sess_jti
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            log.error(
                "authorize_battle_reconnect_ticket_failed", err=str(exc),
                reason="battle_ticket_issue_failed", player_id=player_id,
                match_id=bl.match_id, sess_jti=sess_jti,
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle reconnect ticket authority unavailable: %s",
                exc,
            ) from exc
        if not issued.battle_ds_addr:
            # 票有效但没地址 = 客户端拿到一张进不去任何地方的票。
            log.error(
                "battle_reconnect_target_addr_missing", reason="roster_target_addr_empty",
                player_id=player_id, match_id=bl.match_id, ticket_jti=issued.jti,
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle reconnect target address unavailable"
            )

        self._touch_device_async(player_id, device_id)

        out = LoginResult()
        out.player_id = player_id
        out.session_token = session_token
        out.session_exp_ms = sess_exp_ms
        out.battle_ds_addr = issued.battle_ds_addr
        out.battle_ticket = issued.ticket
        out.battle_ticket_exp_ms = issued.expires_at_ms
        out.match_id = bl.match_id
        out.region_id = region_id
        out.cell_id = cell_id
        out.resume = resume
        log.info(
            "login_battle_reconnect", player_id=player_id, device_id=device_id,
            match_id=bl.match_id, battle_ds_addr=issued.battle_ds_addr,
            ticket_jti=issued.jti, sess_jti=sess_jti,
            battle_ticket_exp_ms=issued.expires_at_ms,
            region_id=region_id, cell_id=cell_id,
            match_stage=enum_name(_R.ResumeMatchStage, resume.match_stage),
            game_mode=resume.game_mode,
            entry_state=enum_name(_R.ResumeEntryState, resume.entry_state),
            owner_epoch=resume.owner_epoch,
        )
        return out, 0

    # ── Hub 放行门 / 两条路由入口 ──────────────────────────────────────────

    async def _guard_hub_route_against_active_battle(self, player_id: int) -> int:
        """Hub 物理副作用入口的 active-BATTLE 三态门。对齐 Go `guardHubRouteAgainstActiveBattle`。

        返回 source_match_id fence(0 = 无);拒绝时抛 PandoraError。

        为什么 SelectRole / IssueDSTicket(hub) 也必须过它:这两条同样会让 allocator
        真的分配一台 Hub DS 并签出可用票。只在 Login 上设门,等于留了两扇没锁的后门。
        """
        log = plog.get()
        if self._notifier is None:
            if self._strict_battle_gate_profile():
                log.error(
                    "hub_route_rejected", reason="locator_not_configured",
                    player_id=player_id,
                    hint="strict 档必须配 player_locator;否则无法证明玩家不在战斗",
                )
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "player locator is required before hub ticket issuance",
                )
            return 0

        try:
            bl, _ma = await self._resolve_battle_authority(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            if self._strict_battle_gate_profile():
                log.warning(
                    "hub_route_rejected", err=str(exc),
                    reason="battle_authority_unavailable", player_id=player_id,
                )
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "cannot prove player is outside battle before hub ticket issuance: %s",
                    exc,
                ) from exc
            log.warning(
                "hub_route_gate_locator_degraded", err=str(exc),
                reason="battle_authority_degraded_allow", player_id=player_id,
            )
            return 0

        if not bl.in_battle:
            return 0

        if self._battle_ticket_issuer is None:
            log.error(
                "hub_route_rejected", reason="battle_route_authority_not_configured",
                player_id=player_id, match_id=bl.match_id,
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle route authority unavailable while locator reports BATTLE",
            )

        state, route_err = await self._battle_ticket_issuer.inspect_battle_route(
            player_id, bl.match_id
        )
        if state == lbattleroute.BattleRouteState.ACTIVE:
            log.warning(
                "hub_route_rejected_active_battle", reason="battle_route_active",
                player_id=player_id, match_id=bl.match_id,
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "player is in active battle (match_id=%d); reconnect via Login instead of hub ticket",
                bl.match_id,
            )
        if state == lbattleroute.BattleRouteState.TERMINAL:
            log.info(
                "hub_route_allowed_terminal_battle",
                decision="allow_hub_with_source_match_fence",
                player_id=player_id, match_id=bl.match_id,
            )
            return bl.match_id
        log.warning(
            "hub_route_rejected_unknown_battle_state",
            err=str(route_err) if route_err is not None else "",
            reason="battle_route_unknown", player_id=player_id, match_id=bl.match_id,
        )
        raise errcode.PandoraError(
            errcode.ErrUnavailable,
            "cannot prove battle is over before hub ticket issuance",
        )

    async def resolve_hub_endpoint(self, player_id: int, sess_jti: str) -> tuple[str, str, int]:
        """无来源 match 的 Hub 路由(等价 `resolve_hub_endpoint_from_match(..., 0, ...)`)。"""
        return await self.resolve_hub_endpoint_from_match(player_id, 0, sess_jti)

    async def resolve_hub_endpoint_from_match(
        self, player_id: int, source_match_id: int, sess_jti: str
    ) -> tuple[str, str, int]:
        """结算 / 主动回大厅的 Hub 路由。对齐 Go `ResolveHubEndpointFromMatch`。

        ★ 客户端上报的 `source_match_id` **只作日志参考**:真正签进票里的 fence 来自
        路由权威门(`_guard_hub_route_against_active_battle`)。信客户端报的那个,
        等于让客户端自己声明"我这局已经打完了"。
        """
        log = plog.get()
        if player_id == 0:
            log.warning(
                "hub_endpoint_rejected", reason="missing_player_id",
                source_match_id=source_match_id,
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "playerID must be > 0")

        fence_match_id = await self._guard_hub_route_against_active_battle(player_id)

        # 单 Cell(Python 侧 cell_route 非空时 BaseConf 已在加载期拒启)。
        region_id, cell_id = 0, 0

        # 角色是 fail-closed 的:查不到就不签票。签一张 role=0 的 hub 票,
        # 玩家会进到一个没有角色的大厅,表现为"进去了但什么都没有"。
        role_id = await self._load_selected_role(player_id)

        return await self._resolve_hub(
            player_id, region_id, cell_id, role_id, fence_match_id, sess_jti
        )

    async def resolve_battle_endpoint(
        self, player_id: int, match_id: int, sess_jti: str
    ) -> tuple[str, str, int]:
        """Battle 重连路由。对齐 Go `ResolveBattleEndpoint`。

        地址与票据都只能来自 roster 权威门,调用方给的 match_id 只是"要哪一局"的选择。
        """
        log = plog.get()
        if player_id == 0 or match_id == 0:
            log.warning(
                "battle_endpoint_rejected", reason="missing_player_or_match",
                player_id=player_id, match_id=match_id,
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "Battle endpoint requires player_id and match_id"
            )
        if self._battle_ticket_issuer is None:
            log.error(
                "battle_endpoint_rejected", reason="ticket_issuer_not_configured",
                player_id=player_id, match_id=match_id,
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle reconnect ticket authority unavailable"
            )

        region_id, cell_id = 0, 0
        issued: BattleTicketResult | None = None
        issue_err: BaseException | None = None
        try:
            issued = await self._battle_ticket_issuer.issue_battle_ds_ticket_at_cell(
                player_id, match_id, region_id, cell_id, sess_jti
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            issue_err = exc

        # 四种失败形态分开记 reason:签票报错 / 权威回了个空壳 / 有票没地址 /
        # 有地址没票。折叠成一条会让"roster 权威降级"与"签发器坏了"在日志上无法区分。
        reason = ""
        if issue_err is not None:
            reason = "issue_error"
        elif issued is None:
            reason = "nil_result"
        elif not issued.battle_ds_addr:
            reason = "empty_addr"
        elif not issued.ticket:
            reason = "empty_ticket"
        if reason:
            log.warning(
                "battle_endpoint_unavailable",
                err=str(issue_err) if issue_err is not None else "",
                player_id=player_id, match_id=match_id, reason=reason,
                region_id=region_id, cell_id=cell_id,
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle endpoint unavailable for player=%d match=%d (%s)",
                player_id,
                match_id,
                reason,
            )
        return issued.battle_ds_addr, issued.ticket, issued.expires_at_ms

    # ── owner query-first ──────────────────────────────────────────────────

    async def _resolve_resume_from_owner(
        self, player_id: int
    ) -> tuple[bool, ResumeContextResult]:
        """按 query-first 语义先问 owner。

        decided=False 只有一种情况:owner 明确回答"该玩家当前没有归属记录" —— 那是首次
        进场,交给角色 → 撮合 → 首个 Hub 分配链。其余情况 owner 的答案就是最终答案。
        """
        log = plog.get()
        if self._owner is None:
            # owner_addr 未配置(owner 服务未部署)。这是部署形态问题,不是"该玩家无归属" ——
            # 冒充无归属会让调用方走首次进场链、再分配一台 DS(§9.22 禁冒充默认状态)。
            log.warning(
                "owner_placement_querier_missing", player_id=player_id,
                hint="owner_addr 未配置;进场按 WAIT 处理",
            )
            return True, _wait_resume(
                _R.RESUME_WAIT_REASON_OWNER_UNKNOWN, OWNER_UNKNOWN_RETRY_AFTER_MS
            )
        try:
            v = await self._owner.query_owner_placement(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 不可判定 → WAIT。**绝不回落旧路由**:locator presence 只是投影,
            # key miss 不能证明玩家已离开旧 DS,更不能授权进入另一台 DS(§9.22)。
            log.warning(
                "owner_query_first_unavailable", player_id=player_id, err=str(exc),
                hint="query-first:返回 WAIT/UNKNOWN 让客户端退避重查,不猜路由",
            )
            return True, _wait_resume(
                _R.RESUME_WAIT_REASON_OWNER_UNKNOWN, OWNER_UNKNOWN_RETRY_AFTER_MS
            )
        now = _now_ms()
        route = _owner_type_to_route(v.owner_type)
        if route == _R.RESUME_ROUTE_UNSPECIFIED:
            # owner 权威明确回答"无归属":这是首次进场,不是故障,交旧链继续。
            log.info(
                "owner_placement_resolved", player_id=player_id, decided=False,
                owner_type=v.owner_type, owner_phase=v.phase,
            )
            return False, ResumeContextResult()
        out = ResumeContextResult()
        out.route = route
        out = apply_owner_placement(out, v, now)
        log.info(
            "owner_placement_resolved", player_id=player_id, decided=True,
            owner_type=v.owner_type, owner_phase=v.phase,
            route=enum_name(_R.ResumeRoute, out.route),
            entry_state=enum_name(_R.ResumeEntryState, out.entry_state),
            wait_reason=enum_name(_R.ResumeWaitReason, out.wait_reason),
            placement_state=enum_name(_R.ResumePlacementState, out.placement_state),
            owner_epoch=out.owner_epoch, operation_id=out.operation_id,
            ds_pod=out.ds_pod_name, ds_instance_uid=out.ds_instance_uid,
            ds_instance_epoch=out.ds_instance_epoch,
            hub_assignment_id=out.hub_assignment_id, allocation_id=out.allocation_id,
            release_track=out.release_track, retry_after_ms=out.retry_after_ms,
            admit_not_before_ms=v.admit_not_before_ms,
            admit_barrier_remain_ms=v.admit_not_before_ms - now,
            lease_deadline_ms=v.lease_deadline_ms,
            lease_remain_ms=v.lease_deadline_ms - now,
            lease_skew_margin_ms=OWNER_LEASE_SKEW_MARGIN_MS,
        )
        return True, out

    # ── Hub 解析 ───────────────────────────────────────────────────────────

    async def _resolve_hub(
        self,
        player_id: int,
        region_id: int,
        cell_id: int,
        role_id: int,
        source_match_id: int,
        sess_jti: str,
    ) -> tuple[str, str, int]:
        """hub_allocator 是 hub 票据权威;未配 / 调用失败 → 回退自签 + 静态 addr(弱依赖)。

        **strict 档例外**:require_hub_assignment_binding=true 时 allocator 是唯一签票权威,
        拿不到票就没有 Hub 可进 —— 此时自签回退会产生一张 Hub DS 必拒的票,
        表现为"登录成功但进不去",比直接失败难查得多。
        """
        log = plog.get()
        if self._hub_assigner is not None:
            assign_started = time.monotonic()
            try:
                assign = await self._hub_assigner.assign_hub(
                    player_id, self._hub_region, 0, role_id, source_match_id, sess_jti
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                assign_ms = int((time.monotonic() - assign_started) * 1000)
                if self._require_hub_assignment_binding:
                    log.error(
                        "hub_assign_failed", err=str(exc),
                        reason="allocator_unavailable_strict", player_id=player_id,
                        role_id=role_id, region=self._hub_region,
                        source_match_id=source_match_id, dur_assign_ms=assign_ms,
                        require_binding=True,
                    )
                    raise errcode.PandoraError(
                        errcode.ErrUnavailable,
                        "hub allocator required for assignment-bound ticket: %s",
                        exc,
                    ) from exc
                log.warning(
                    "hub_assign_failed_fallback_self_sign", err=str(exc), player_id=player_id,
                    reason="allocator_unavailable_fallback_self_sign",
                    role_id=role_id, region=self._hub_region, dur_assign_ms=assign_ms,
                )
            else:
                assign_ms = int((time.monotonic() - assign_started) * 1000)
                summary = self._ds_signer.verify(assign.hub_ticket)
                if summary.player_id != player_id or summary.ds_type != ldsticket.DS_TYPE_HUB:
                    # allocator 返回的票必须验得过且绑对人:半绑定票发出去只会被 Hub DS 拒,
                    # 而 login 侧看起来一切正常。
                    log.error(
                        "hub_assigner_returned_invalid_ticket",
                        reason="ticket_verify_failed", player_id=player_id,
                        hub_pod=assign.hub_pod_name,
                        ticket_player_id=summary.player_id, ticket_ds_type=summary.ds_type,
                    )
                    raise errcode.PandoraError(
                        errcode.ErrUnavailable,
                        "hub allocator returned an invalid ticket for player=%d",
                        player_id,
                    )
                log.info(
                    "hub_assigned", player_id=player_id, hub_pod=assign.hub_pod_name,
                    shard_id=assign.shard_id, hub_ds_addr=assign.hub_ds_addr,
                    hub_assignment_id=summary.hub_assignment_id,
                    ds_instance_uid=summary.ds_instance_uid,
                    ds_protocol_epoch=summary.ds_protocol_epoch,
                    release_track="", ticket_jti=summary.jti,
                    ticket_exp_ms=summary.expires_at_ms,
                    ticket_role_id=summary.role_id,
                    ticket_source_match_id=summary.source_match_id,
                    role_id=role_id, source_match_id=source_match_id, sess_jti=sess_jti,
                    region_id=region_id, cell_id=cell_id, dur_assign_ms=assign_ms,
                )
                return assign.hub_ds_addr, assign.hub_ticket, summary.expires_at_ms

        if self._require_hub_assignment_binding:
            # 部署配置缺口:strict 档却没配 hub_allocator 地址 → 本部署恒不可进 Hub。
            log.error(
                "hub_assign_failed", reason="allocator_not_configured_strict",
                player_id=player_id, role_id=role_id, require_binding=True,
                hint="配置 login.hub.addr,或关掉 assignment binding 档",
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "hub allocator is required by the assignment-bound ticket profile",
            )

        ticket, exp_ms = self._ds_signer.sign(
            player_id,
            ldsticket.DS_TYPE_HUB,
            region_id=region_id,
            cell_id=cell_id,
            role_id=role_id,
            source_match_id=source_match_id,
        )
        log.info(
            "hub_self_signed", player_id=player_id, hub_ds_addr=self._hub_ds_addr,
            ticket_exp_ms=exp_ms, role_id=role_id, source_match_id=source_match_id,
            region_id=region_id, cell_id=cell_id,
            hint="legacy HS256 dev 档:hub_allocator 未配/不可用时的自签回退",
        )
        return self._hub_ds_addr, ticket, exp_ms

    async def _load_selected_role(self, player_id: int) -> int:
        if self._role_repo is None:
            return 0
        try:
            return await self._role_repo.get_role(player_id)
        except errcode.PandoraError as exc:
            plog.get().warning(
                "load_selected_role_failed", err=str(exc), player_id=player_id,
                hint="角色权威不可判定:按 WAIT/ROLE_UNKNOWN 处理,不得当作未选角",
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "selected role unavailable player_id=%d", player_id
            ) from exc

    # ── SelectRole ─────────────────────────────────────────────────────────

    async def select_role(
        self, player_id: int, role_id: int, sess_jti: str
    ) -> tuple[str, str, int]:
        log = plog.get()
        if player_id == 0:
            log.warning("select_role_rejected", reason="missing_player_id", role_id=role_id)
            raise errcode.PandoraError(errcode.ErrInvalidArg, "playerID must be > 0")
        if role_id == 0:
            log.warning("select_role_rejected", reason="missing_role_id", player_id=player_id)
            raise errcode.PandoraError(errcode.ErrInvalidArg, "roleID must be > 0")

        # SelectRole 也是 Hub 物理副作用入口(落库 + 让 allocator 真分一台 Hub 并签票),
        # 先过 active-BATTLE 三态权威门 —— 只在 Login 上设门等于留了一扇没锁的后门。
        try:
            fence_match_id = await self._guard_hub_route_against_active_battle(player_id)
        except errcode.PandoraError as exc:
            # 门内已按 reason 落盘;这里补一条把「被拒的是 SelectRole」钉死,
            # 否则 hub_route_rejected* 分不清来自 SelectRole 还是 IssueDSTicket(hub)。
            log.warning(
                "select_role_rejected", reason="hub_route_gate_rejected",
                player_id=player_id, role_id=role_id, err=str(exc),
            )
            raise

        if self._allowed_role_ids:
            if role_id not in self._allowed_role_ids:
                log.warning(
                    "select_role_not_allowed", reason="role_not_in_whitelist",
                    player_id=player_id, role_id=role_id,
                )
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "role_id=%d not allowed", role_id
                )
        elif not self._dev_allow_any_role:
            # fail-closed:白名单没配就放行任意 role_id = 改包客户端可把任意角色配置 ID
            # 签进 hub 票据(hub_allocator 无二次校验)。
            log.error(
                "select_role_rejected_no_whitelist", reason="whitelist_not_configured",
                player_id=player_id, role_id=role_id,
                hint="configure login.allowed_role_ids (prod) or enable login.dev_allow_any_role (dev only)",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "role selection disabled: allowed_role_ids not configured"
            )

        if self._role_repo is not None:
            # 双层 fencing:
            #  ① expected_sess_jti → SetRole 在同一 MySQL 事务内 FOR UPDATE 复核持久化代际,
            #     与登录代际写串行化(主防线;由 session_generation_enforce 门控制);
            #  ② precommit → COMMIT 前读 Redis 会话权威复核(纵深,不受强制门控制)。
            precommit = None
            expected_sess_jti = ""
            if sess_jti and self._sessions is not None:
                if self._session_gen_enforce:
                    expected_sess_jti = sess_jti

                async def precommit() -> None:  # noqa: E306
                    await self._require_current_session(player_id, sess_jti)

            try:
                await self._role_repo.set_role(player_id, role_id, expected_sess_jti, precommit)
            except errcode.PandoraError as exc:
                code = errcode.as_code(exc)
                reason = {
                    errcode.ErrSessionSuperseded: "session_superseded",
                    errcode.ErrUnauthorized: "session_not_current",
                    errcode.ErrUnavailable: "session_authority_unavailable",
                }.get(code, "persist_failed")
                log.error(
                    "select_role_persist_failed", err=str(exc), reason=reason,
                    player_id=player_id, role_id=role_id,
                    gen_enforce=self._session_gen_enforce, sess_jti=sess_jti,
                )
                raise
        else:
            log.warning(
                "select_role_repo_nil_skip_persist", reason="role_repo_not_configured",
                player_id=player_id, role_id=role_id,
            )

        addr, ticket, exp_ms = await self._resolve_hub(
            player_id, 0, 0, role_id, fence_match_id, sess_jti
        )
        # R1:选角完成是不可逆状态推进(角色已落库 + 已签出新 hub 票),必须 INFO ——
        # 「玩家到底选没选角、选的哪个、拿到哪台 Hub」是"卡在选角界面"类问题的第一判据。
        log.info(
            "select_role_ok", player_id=player_id, role_id=role_id, hub_ds_addr=addr,
            hub_ticket_exp_ms=exp_ms, sess_jti=sess_jti, region_id=0, cell_id=0,
            source_match_id=fence_match_id,
        )
        return addr, ticket, exp_ms

    # ── Logout ─────────────────────────────────────────────────────────────

    async def logout(self, session_token: str) -> None:
        log = plog.get()
        if self._sessions is None:
            log.info("logout_ok_noop")
            return
        try:
            claims = self._signer.verify(session_token, expect_audience=self._signer_audience())
        except Exception as exc:  # noqa: BLE001 —— token 过期不算业务错,直接返 OK
            # 但它意味着**本次登出什么都没做**:session 没删、owner 没释放。
            # 「玩家说退出后还显示在线」的第一嫌疑就是这里。
            log.warning(
                "logout_verify_session_failed", err=str(exc), reason="session_verify_failed",
                hint="本次 Logout 未删除任何会话/归属,返回 OK 供客户端 fire-and-forget",
            )
            return
        try:
            player_id = int(claims.get("sub") or 0)
        except (TypeError, ValueError):
            player_id = 0
        if player_id == 0:
            log.warning("logout_session_no_player", reason="session_no_player_id")
            return
        jti = str(claims.get("jti") or "")

        # 只删"本 token 对应的那一代 session":顶号后旧设备的迟到 Logout 携带旧 jti,
        # CAS 不命中 → 不影响新设备 session。
        deleted = await self._sessions.delete_if_jti(player_id, jti)
        if not deleted:
            log.info(
                "logout_stale_session_ignored", player_id=player_id,
                reason="session_jti_not_current", sess_jti=jti,
            )
            return
        if self._session_gen is not None:
            try:
                await self._session_gen.tombstone_session_jti(player_id, jti)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— best-effort:Redis 主权威已删
                log.warning(
                    "logout_session_generation_tombstone_failed_weak",
                    reason="generation_tombstone_failed", player_id=player_id, err=str(exc),
                )
        if self._owner is not None:
            # Query→Release 携带观察到的 epoch+operation(compare-delete 自己):
            # 并发迁移竞态下 Release 在 owner 侧幂等 no-op,**绝不误删新 owner**。
            try:
                rec = await self._owner.query_owner_placement(player_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                log.warning(
                    "logout_owner_query_failed_weak", reason="owner_query_failed",
                    player_id=player_id, err=str(exc),
                )
            else:
                if rec.owner_type != OWNER_TYPE_NONE:
                    try:
                        await self._owner.release_owner(
                            player_id, rec.owner_epoch, rec.operation_id
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:  # noqa: BLE001
                        log.warning(
                            "logout_owner_release_failed_weak", reason="owner_release_failed",
                            player_id=player_id, owner_epoch=rec.owner_epoch,
                            operation_id=rec.operation_id, err=str(exc),
                        )
                    else:
                        log.info(
                            "logout_owner_released", player_id=player_id,
                            owner_type=rec.owner_type, owner_epoch=rec.owner_epoch,
                            operation_id=rec.operation_id,
                        )
        log.info("logout_ok", player_id=player_id, sess_jti=jti)

    def _signer_audience(self) -> str:
        # Signer 不暴露 audience,这里从其配置读(与 sign_session 用的是同一个值)。
        return self._signer._cfg.audience  # noqa: SLF001

    def _signer_account_audience(self) -> str:
        return self._signer._cfg.account_audience  # noqa: SLF001

    # ── 会话现行性门 ───────────────────────────────────────────────────────

    async def _fence_login_delivery(self, player_id: int, sess_jti: str) -> None:
        """交付终检:本流程写入的 sess_jti 必须仍是当前一代才允许交付凭据。

        sessions.set 之后的分配、locator、签票各步都不复核现行性,并发新登录 B 在其间
        再次轮换 jti 时,旧流程 A 若继续交付,旧设备将取得"看似有效"的完整登录态。
        复核失败 → 不返回任何凭据(票据已签但从未离开服务端 = 未取得)。
        """
        if self._sessions is None:
            return
        try:
            cur, found = await self._sessions.get_jti(player_id)
        except errcode.PandoraError as exc:
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "session authority unavailable; login credentials withheld: %s",
                exc,
            ) from exc
        if not found or cur != sess_jti:
            plog.get().warning("login_delivery_fenced_superseded", player_id=player_id)
            raise errcode.PandoraError(
                errcode.ErrSessionSuperseded,
                "session superseded during login; credentials withheld",
            )

    async def _require_current_session(self, player_id: int, jti: str) -> None:
        """JWT 验签只证明"曾经登录过",不证明"未被顶号"。本门用 Redis session 的 jti 判定。"""
        if self._sessions is None:
            return
        try:
            cur, found = await self._sessions.get_jti(player_id)
        except errcode.PandoraError as exc:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "session authority unavailable; retry: %s", exc
            ) from exc
        if not found:
            plog.get().warning(
                "session_gate_rejected", player_id=player_id, reason="session_not_found",
                hint="会话已过期或已登出;玩家侧表现为被弹回登录界面",
            )
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "session expired or logged out; login again"
            )
        if not jti:
            # 缺 jti 证据:无法证明现行性,维持普通未授权语义,**不得当顶号** ——
            # 客户端对 ErrUnauthorized 允许自动换新,对顶号码则转交互登录。
            plog.get().warning(
                "session_gate_rejected", player_id=player_id, reason="jti_evidence_missing",
                hint="调用方缺 x-pandora-jwt-payload:直连内网联调,或网关 jwt_authn 配置漂移",
            )
            raise errcode.PandoraError(errcode.ErrUnauthorized, "session jti evidence required")
        if cur != jti:
            # 顶号专属码(→ gRPC ABORTED):被顶设备对本码只能转交互登录,
            # 不得用缓存凭据自动完整 Login —— 那会轮换 jti 反顶新设备,两台互踢死循环。
            plog.get().warning("session_superseded_rejected", player_id=player_id)
            raise errcode.PandoraError(
                errcode.ErrSessionSuperseded, "session superseded by a newer login"
            )

    async def require_current_session_jti(self, player_id: int, jti: str) -> None:
        """请求体不带 token 的鉴权 RPC(SelectRole)的会话现行性门。"""
        if self._sessions is None:
            return  # dev 裸跑:未配会话权威,不伪造现行性结论
        if not jti:
            if self._require_hub_assignment_binding:
                # 生产 SelectRole 必经 :8443 jwt_authn,该头必然存在 ——
                # 全服集中报本 reason 意味着网关配置漂移,选角会全服静默失败。
                plog.get().warning(
                    "session_gate_rejected", player_id=player_id,
                    reason="jwt_payload_header_missing",
                    hint="SelectRole 必经 :8443 jwt_authn;全服同报=网关配置漂移",
                )
                raise errcode.PandoraError(errcode.ErrUnauthorized, "session payload required")
            return
        await self._require_current_session(player_id, jti)

    async def require_current_session_token(self, player_id: int, session_token: str) -> None:
        """携带原始 token 的 RPC(IssueDSTicket)的现行性门:验签 + 同人 + jti 当前一代。"""
        if self._sessions is None:
            return
        if not session_token:
            if self._require_hub_assignment_binding:
                plog.get().warning(
                    "session_gate_rejected", player_id=player_id,
                    reason="session_token_missing",
                )
                raise errcode.PandoraError(errcode.ErrUnauthorized, "session token required")
            return  # dev 兼容:旧客户端未传 token 时不阻断
        try:
            claims = self._signer.verify(session_token, expect_audience=self._signer_audience())
            token_player = int(claims.get("sub") or 0)
        except Exception as exc:  # noqa: BLE001
            plog.get().warning(
                "session_gate_rejected", player_id=player_id,
                reason="session_token_invalid", err=str(exc),
            )
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "session token invalid for caller"
            ) from exc
        if token_player == 0 or token_player != player_id:
            plog.get().warning(
                "session_gate_rejected", player_id=player_id, reason="session_token_invalid",
            )
            raise errcode.PandoraError(errcode.ErrUnauthorized, "session token invalid for caller")
        await self._require_current_session(player_id, str(claims.get("jti") or ""))

    async def require_ticket_session_current(self, player_id: int, ticket_sess_jti: str) -> None:
        """票据兑换点会话复核:签发与响应写出之间被新登录轮换的旧票,在兑换点作废。

        sjti 空由 require_ticket_sjti 门控制 —— 默认兼容档告警放行(滚动窗口内旧签发面
        仍持续签空票,硬拒会令混版期战斗准入整体不可用);全 fleet 签发面必带 sjti +
        旧版本排空 + 等满票据最大 TTL 后开门硬拒(空票是绕过会话绑定的万能票)。
        """
        if self._sessions is None:
            return
        if not ticket_sess_jti:
            if self._require_ticket_sjti:
                plog.get().warning(
                    "ticket_missing_session_binding_rejected", player_id=player_id
                )
                raise errcode.PandoraError(
                    errcode.ErrUnauthorized, "ticket lacks session binding (sjti); reissue required"
                )
            plog.get().info(
                "ticket_missing_session_binding_compat_allow", player_id=player_id,
                hint="混版兼容窗;签发面排空+等满票据最大 TTL 后开 login.require_ticket_sjti 收口",
            )
            return
        await self._require_current_session(player_id, ticket_sess_jti)

    # ── 失败补偿 ───────────────────────────────────────────────────────────

    async def _resolve_ambiguous_session_generation(
        self, player_id: int, sess_jti: str, sess_ttl: float, observed_gen: int
    ) -> int:
        """把不确定的 COMMIT **判定**成事实:用本次 jti 作唯一标记读回权威。"""
        jti, generation, found = await self._session_gen.load_session_generation(player_id)
        if found and jti == sess_jti:
            return generation  # 确实提交了
        # 没提交(或已被更高代际取代):本次登录必须失败,并对可能的残留写墓碑。
        await self._reconcile_failed_session_write(player_id, sess_jti, observed_gen, sess_ttl)
        raise errcode.PandoraError(
            errcode.ErrUnavailable,
            "session generation commit result unresolved for player %d; retry login",
            player_id,
        )

    async def _reconcile_failed_session_write(
        self, player_id: int, sess_jti: str, sess_gen: int, sess_ttl: float
    ) -> None:
        """收口「MySQL 代际已提交、Redis 条件写结果不确定」。

        本次 Login 必定失败且零凭据交付,因此补偿**不能猜测即时前代就是最后已交付会话**:
        A(已交付)→B(未交付)→C(未交付) 时,恢复 C 的即时前代会把 B 永久立为 current。
        标准 fail-closed 做法是两侧各自写无能力墓碑并保留单调 generation。

        两步**各自独立执行**:无论一侧 error / no-op 都执行另一侧 —— 否则
        「MySQL 已被 C 推进、C 的 Redis 尚未落地」会跳过必要的 Redis fence。
        """
        log = plog.get()
        if sess_gen == 0:
            return
        if self._session_gen is not None:
            try:
                await asyncio.wait_for(
                    self._session_gen.tombstone_failed_session_jti(
                        player_id, sess_jti, sess_gen
                    ),
                    timeout=SESSION_RECONCILE_TIMEOUT_SEC,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                log.warning(
                    "session_generation_tombstone_failed", err=str(exc),
                    player_id=player_id, gen=sess_gen,
                )
        if self._sessions is not None:
            try:
                await asyncio.wait_for(
                    self._sessions.fence_failed_set(player_id, sess_jti, sess_gen, sess_ttl),
                    timeout=SESSION_RECONCILE_TIMEOUT_SEC,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                log.warning(
                    "session_redis_fence_failed", err=str(exc),
                    player_id=player_id, gen=sess_gen,
                )

    # ── 记账副作用(detached,失败只记日志)──────────────────────────────

    def _touch_device_async(self, player_id: int, device_id: str) -> None:
        if not device_id:
            return

        async def _run() -> None:
            try:
                await asyncio.wait_for(
                    self._repo.touch_device(player_id, device_id), timeout=TOUCH_TIMEOUT_SEC
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "touch_device_failed", err=str(exc), player_id=player_id,
                    hint="只影响设备记账,不影响本次登录",
                )

        # 必须过 safego:裸 create_task 的协程抛异常后异常只躺在 Task 里,
        # 进程照跑、health 照答 SERVING、日志零行。
        safego.spawn("login_touch_device", _run)

    def _touch_role_login_async(self, player_id: int) -> None:
        if self._role_ledger is None or player_id == 0:
            return

        async def _run() -> None:
            try:
                await asyncio.wait_for(
                    self._role_ledger.touch_login(player_id), timeout=TOUCH_TIMEOUT_SEC
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                plog.get().warning(
                    "touch_role_login_failed", err=str(exc), player_id=player_id,
                    hint="只影响选角界面的默认选中项,不影响本次进入",
                )

        safego.spawn("login_touch_role", _run)

    # ── 只读查询 ───────────────────────────────────────────────────────────

    async def get_player_no(self, player_id: int) -> int:
        """查当前角色的角色编号(展示专用)。

        返回 0 不是错误,是「仍在补号窗口内」(约 15s = 5s 补号周期 + 10s 水位滞后)的正常态。
        刻意**不做**会话现行性复核:本 RPC 只读且只能读自己,零副作用、不发凭据、不改归属。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        return await self._repo.get_player_no(player_id)

    async def resolve_player_nos(self, player_ids: list[int]) -> dict[int, int]:
        """内部服务按身份键批量读取展示编号；只委托一次账号仓储批量查询。"""
        if not player_ids or any(player_id <= 0 for player_id in player_ids):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "positive player_ids required"
            )
        return await self._repo.get_player_nos(player_ids)

    async def get_resume_context(self, session_token: str) -> ResumeContextResult:
        """客户端退避重查入口:只查权威,零副作用(不分配、不签票、不占座)。"""
        try:
            claims = self._signer.verify(session_token, expect_audience=self._signer_audience())
            player_id = int(claims.get("sub") or 0)
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "resume context: invalid session token: %s", exc
            ) from exc
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrUnauthorized, "resume context: no player id")
        decided, owned = await self._resolve_resume_from_owner(player_id)
        if not decided:
            # owner 明确"无归属" = 首次进场:客户端应走 Login/EnterRole 拿 Hub,
            # 这里如实回 UNSPECIFIED,不冒充 HUB。
            return ResumeContextResult()
        return owned


# ── TicketUsecase(legacy HS256)──────────────────────────────────────────────


def _ticket_fingerprint(token: str) -> str:
    """票体 sha256 前 8 字节的 hex。对齐 Go `ticketFingerprint`。

    只用于把验签失败与签发侧日志关联成"同一张票" —— **绝不落原始 token**,
    它在有效期内就是一张可用的进场凭证。
    """
    return hashlib.sha256(token.encode()).hexdigest()[:16]


class TicketUsecase:
    """DSTicket 签发 / 兑换。**legacy HS256 与 v2 RS256 两条路径,由配置显式二选一**。

    ★ 选路判据是 `rs256_ds_ticket_profile_enabled()`(v2 signer 或 verifier 任一已注入),
      **不是**"票里带的 alg",也**不是**"绑定齐不齐":

        · 按票据 alg 兼容 = 攻击者拿一张 HS256 票就能绕开整个 v2 信任域;
        · 按绑定齐不齐降级 = 绑定缺失时签一张无绑定票,§9.22 exact 实例绑定当场失效。

      所以 v2 档下 legacy HS256 玩家票一律拒(`legacy HS256 DSTicket is disabled by
      the RS256 profile`),v2 档下 battle 目标身份不完整一律拒签,hub 票一律拒签
      (v2 hub 票必须带完整实例绑定,只能由 hub_allocator 签)。

    ★ SessionToken 仍走独立的 HS256 路径(`LoginUsecase._signer`),不受本开关影响 ——
      两者是两个信任域(`pandora-client` vs `pandora-game-ds`)。
    """

    __slots__ = (
        "_ds_signer", "_jti_repo", "_session_gate",
        "_assignment_checker", "_require_hub_assignment_binding",
        "_v2_signer", "_v2_verifier", "_battle_authorizer",
    )

    def __init__(
        self,
        ds_signer: ldsticket.DSTicketSigner,
        jti_repo: ldata.RedisTicketJTIRepo | None,
    ) -> None:
        self._ds_signer = ds_signer
        self._jti_repo = jti_repo
        self._session_gate: LoginUsecase | None = None
        self._assignment_checker: lhubbinding.HubAssignmentChecker | None = None
        self._require_hub_assignment_binding = False
        self._v2_signer: pdsticket.DSTicketSigner | None = None
        self._v2_verifier: pdsticket.DSTicketVerifier | None = None
        self._battle_authorizer: lbattleroute.RedisBattleTicketAuthorizer | None = None

    def set_ds_ticket_v2_signer(self, signer: pdsticket.DSTicketSigner | None) -> None:
        """注入 v2(RS256)签发器 —— Go `SetDSTicketV2Signer`(启动期、对外监听前调用)。

        注入后 battle 签票**全部**走 v2 实例绑定路径,不再签 legacy HS256 票;
        hub 票一律拒签(v2 hub 票只能由 hub_allocator 签,它才有实例绑定权威)。
        """
        self._v2_signer = signer

    def set_ds_ticket_v2_verifier(self, verifier: pdsticket.DSTicketVerifier | None) -> None:
        """注入严格 RS256 verifier —— Go `SetDSTicketV2Verifier`。

        只要 signer / verifier **任一**启用,玩家 DSTicket 验证就机械进入 RS256-only;
        legacy HS256 只留给完全未启用 v2 的 local/off 档。
        """
        self._v2_verifier = verifier

    def set_battle_ticket_authorizer(
        self, authorizer: lbattleroute.RedisBattleTicketAuthorizer | None
    ) -> None:
        """注入 battle 签票前的 player↔match roster 权威门 —— Go `SetBattleTicketAuthorizer`。

        未注入时 battle 签票 fail-closed(`ErrUnavailable`)—— 绝不"没有权威就直接签":
        那正是"知道 match_id 就能拿到那局进场票"的旁路。Hub 签票不受此门影响。
        """
        self._battle_authorizer = authorizer

    def rs256_ds_ticket_profile_enabled(self) -> bool:
        """Go `rs256DSTicketProfileEnabled`。

        ★ 判据是 `signer or verifier`(不是 and):只装了 verifier 的诊断副本也必须
          拒收 legacy 票,否则"半装 v2"的部署会留一个只认 HS256 的兑换点。
        """
        return self._v2_signer is not None or self._v2_verifier is not None

    def set_hub_assignment_checker(
        self, checker: lhubbinding.HubAssignmentChecker | None, *, require_binding: bool = False
    ) -> None:
        """接入 Hub 归属权威校验器(见 `hubbinding.py` 顶部注释)。

        `require_binding` 对齐 Go 的 `login.require_hub_assignment_binding`:开启后
        **无绑定的旧签发面票据也拒**。默认关是为了滚动窗口 —— 签发面排空前先兼容,
        排空后再开门硬拒。
        """
        self._assignment_checker = checker
        self._require_hub_assignment_binding = require_binding

    def set_ticket_session_gate(self, gate: LoginUsecase) -> None:
        """票据兑换点的会话现行性门。**前置到 replay marker 之前** —— 顺序不能反:
        先消费 jti 再判会话,会让被顶设备的一次重放把合法票的 jti 也烧掉。"""
        self._session_gate = gate

    async def issue_ds_ticket(
        self, player_id: int, ds_type: str, target_id: int, sess_jti: str
    ) -> tuple[str, int]:
        """公共签票入口 —— Go `TicketUsecase.IssueDSTicket`。

        `sess_jti` 是**请求方登录会话 jti**(§9.23 会话 fencing):签进 v2 票的 `sjti`
        claim,兑换点复核它仍是该玩家会话权威的当前一代。legacy HS256 票不带 sjti
        (v1 票没有这个 claim),所以那条路径上它只是被丢弃 —— 但参数必须保留:
        去掉它会让调用方"以为传了会绑定",而实际什么都没绑。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "playerID must be > 0")
        if ds_type not in (ldsticket.DS_TYPE_HUB, ldsticket.DS_TYPE_BATTLE):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "dsType must be hub|battle, got %r", ds_type
            )
        if ds_type == ldsticket.DS_TYPE_BATTLE:
            if target_id == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "battle DSTicket requires match_id (targetID)"
                )
            # region/cell 恒 0:Python 侧单 Cell(main 的 cellroute_init_failed 闸已保证
            # cell_route.mode 为空),与 Go 的 routeRegionCell 在 router==nil 时同值。
            addr, ticket, exp_ms = await self.issue_battle_ds_ticket_at_cell(
                player_id, target_id, 0, 0, sess_jti
            )
            del addr  # 公共 IssueDSTicket(battle) 响应不返回地址(地址来自 matchmaker)
            return ticket, exp_ms
        if self.rs256_ds_ticket_profile_enabled():
            # v2(方案 B):hub 票必须带完整实例绑定,只能由 hub_allocator 签;login 不自签。
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "hub DSTicket v2 must be issued by hub_allocator (instance binding required)",
            )
        if self._require_hub_assignment_binding:
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "hub DSTicket must be issued by hub_allocator while assignment binding is required",
            )
        return self._issue_ds_ticket_at_cell(player_id, ldsticket.DS_TYPE_HUB, 0, 0, 0)

    async def issue_battle_ds_ticket_at_cell(
        self,
        player_id: int,
        match_id: int,
        region_id: int,
        cell_id: int,
        sess_jti: str,
    ) -> tuple[str, str, int]:
        """**所有** login 侧 Battle 签票路径的唯一入口 —— Go `IssueBattleDSTicketAtCell`。

        返回 `(battle_ds_addr, ticket, expires_at_ms)`。

        ★ 公共 IssueDSTicket 与断线重连都必须先经过**同一个** player↔match roster
          权威门。重连路径若只相信 locator 就重新引入了"知道 match_id 即可拿票"的旁路
          (locator 是 30s TTL 投影,不是成员资格证明)。
        ★ 地址取自**授权时读到的同一份快照**(`target.ds_addr`),不回头用 locator 的地址:
          locator 可能陈旧一整个 Pod 生命周期,那样签出的票绑新实例、地址指向旧实例。
        """
        if player_id == 0 or match_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "battle ticket requires player and match"
            )
        if self._battle_authorizer is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle ticket roster authority unavailable"
            )
        target = await self._battle_authorizer.authorize_battle_ticket(player_id, match_id)
        if not target.ds_addr:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle ticket target address unavailable"
            )
        if self._v2_signer is not None:
            ticket, exp_ms = self._issue_battle_ds_ticket_v2(
                player_id, match_id, region_id, cell_id, target, sess_jti
            )
            return target.ds_addr, ticket, exp_ms
        if self.rs256_ds_ticket_profile_enabled():
            # 只装了 verifier 的 RS256 档:能验不能签。绝不退回 HS256 顶替。
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "RS256 DSTicket profile has no battle ticket signer"
            )
        ticket, exp_ms = self._issue_ds_ticket_at_cell(
            player_id, ldsticket.DS_TYPE_BATTLE, match_id, region_id, cell_id
        )
        return target.ds_addr, ticket, exp_ms

    async def inspect_battle_route(
        self, player_id: int, match_id: int
    ) -> lbattleroute.BattleRouteState:
        """显式三态判定入口 —— Go `InspectBattleRoute`。不签票、零副作用。

        authorizer 未接 → UNKNOWN fail-closed(绝不把 `ErrPermissionDeny` 当终态:
        对局还活着但 roster 抖动一次,就会被读成"已结束"而放行进 Hub → 双在场)。
        """
        if player_id == 0 or match_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "battle route check requires player and match"
            )
        if self._battle_authorizer is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle route roster authority unavailable"
            )
        return await self._battle_authorizer.inspect_battle_route(player_id, match_id)

    def _issue_battle_ds_ticket_v2(
        self,
        player_id: int,
        match_id: int,
        region_id: int,
        cell_id: int,
        target: lbattleroute.BattleTicketTarget,
        sess_jti: str,
    ) -> tuple[str, int]:
        """v2(RS256)battle 票 —— Go `issueBattleDSTicketV2`。

        实例身份**缺一即拒**(旧记录 / 降级路径),绝不退回无绑定票:那等于把 §9.22
        exact 实例绑定与 §9.21 灰度轨道粘滞一起删掉,而两边日志全绿。
        """
        log = plog.get()
        assert self._v2_signer is not None  # 调用方已判
        if (
            not target.pod_name
            or not target.instance_uid
            or target.instance_epoch == 0
            or not target.allocation_id
            or target.release_track
            not in (pdsticket.RELEASE_TRACK_STABLE, pdsticket.RELEASE_TRACK_CANARY)
        ):
            log.warning(
                "battle_ticket_v2_target_incomplete",
                player_id=player_id, match_id=match_id, pod=target.pod_name,
                uid=target.instance_uid, epoch=target.instance_epoch,
                allocation_id=target.allocation_id, release_track=target.release_track,
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "battle ticket v2 requires complete DS instance identity from roster authority",
            )
        jti = str(uuid.uuid4())
        try:
            token, exp_ms = self._v2_signer.sign_battle_ticket(
                player_id,
                region_id,
                cell_id,
                jti,
                pdsticket.DSTicketTarget(
                    ds_pod_name=target.pod_name,
                    ds_instance_uid=target.instance_uid,
                    ds_instance_epoch=target.instance_epoch,
                    release_track=target.release_track,
                    match_id=match_id,
                    allocation_id=target.allocation_id,
                    # §9.23 会话绑定:VerifyDSTicket 核销时复核。
                    session_jti=sess_jti,
                ),
            )
        except pdsticket.DSTicketConfigError as exc:
            log.error(
                "sign_ds_ticket_v2_failed", err=str(exc),
                player_id=player_id, match_id=match_id,
            )
            raise errcode.PandoraError(
                errcode.ErrInternal, "sign v2 battle ticket failed: %s", exc
            ) from exc
        # §9.3 五要件里 battle 侧的那一半必须落盘且是 INFO:DS 侧报
        # 「battle v2 ticket no longer matches roster authority」时,需要对比
        # 「签发瞬间的实例身份」与「核销瞬间的 roster 权威」——只留 pod + allocation_id
        # 或只打 DEBUG(线上 info 级一条都不出)时两边日志根本对不上。
        log.info(
            "ds_ticket_v2_issued",
            player_id=player_id, ds_type=ldsticket.DS_TYPE_BATTLE, match_id=match_id,
            jti=jti, exp_ms=exp_ms, region_id=region_id, cell_id=cell_id,
            pod=target.pod_name, allocation_id=target.allocation_id,
            ds_instance_uid=target.instance_uid, ds_instance_epoch=target.instance_epoch,
            release_track=target.release_track, sess_jti=sess_jti, ds_addr=target.ds_addr,
            ticket_sha=_ticket_fingerprint(token),
        )
        return token, exp_ms

    def _issue_ds_ticket_at_cell(
        self, player_id: int, ds_type: str, target_id: int, region_id: int, cell_id: int
    ) -> tuple[str, int]:
        """legacy HS256 签票 —— Go `issueDSTicketAtCell`。RS256 档下一律拒。"""
        if self.rs256_ds_ticket_profile_enabled():
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "legacy HS256 DSTicket signing is disabled by the RS256 profile",
            )
        jti = str(uuid.uuid4())
        if ds_type == ldsticket.DS_TYPE_BATTLE:
            token, exp_ms = self._ds_signer.sign(
                player_id, ds_type, match_id=target_id,
                region_id=region_id, cell_id=cell_id, jti=jti,
            )
        else:
            token, exp_ms = self._ds_signer.sign(
                player_id, ds_type, region_id=region_id, cell_id=cell_id, jti=jti
            )
        # §11.3 R1:签发是不可逆推进,也是 login 侧唯一能证明「我发出去的票绑的是谁」的
        # 记录。DS 拒票时后端无感,两边日志就靠这条 + jti 对账;与 v2 的
        # ds_ticket_v2_issued 同为 INFO —— 同一个动作两个级别会让「票到底发没发」
        # 在 legacy 档变成盲区。
        plog.get().info(
            "ds_ticket_issued",
            player_id=player_id, ds_type=ds_type, target_id=target_id,
            jti=jti, exp_ms=exp_ms, region_id=region_id, cell_id=cell_id,
            ticket_sha=_ticket_fingerprint(token),
        )
        return token, exp_ms

    def _verify_ds_ticket_signature(
        self, ticket: str
    ) -> tuple[ldsticket.DSTicketClaims, float]:
        """按 JOSE header 的 alg 选**严格** verifier —— Go `verifyDSTicketSignature`。

        返回 `(claims, replay_ttl_sec)`:防重放 marker 的 TTL 取自**验签路径**而不是
        票内 exp —— 票内 exp 是签发侧的说法,marker 必须覆盖本档位允许的最大票寿命。

        ★ 分发只用于选 verifier,**不是**兼容策略:装了任一 v2 组件后,HS256 玩家票
          一律拒。按票据 alg 兼容 = 攻击者拿一张自签 HS256 票就绕开整个 v2 信任域。
        """
        alg = pdsticket.ds_ticket_algorithm(ticket)
        if alg == ldsticket.ALGORITHM:  # HS256(legacy / v1)
            if self.rs256_ds_ticket_profile_enabled():
                raise errcode.PandoraError(
                    errcode.ErrLoginTicketInvalid,
                    "legacy HS256 DSTicket is disabled by the RS256 profile",
                )
            if self._ds_signer is None:
                raise errcode.PandoraError(
                    errcode.ErrUnavailable, "legacy DSTicket verifier unavailable"
                )
            claims = self._ds_signer.verify(ticket)
            ttl = self._ds_signer.ttl
            return claims, (
                ttl.total_seconds() if isinstance(ttl, _dt.timedelta) else 300.0
            )
        # RS256(v2 / B1)
        if self._v2_verifier is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "DSTicket v2 verifier unavailable"
            )
        v2 = self._v2_verifier.verify(ticket)
        # ★ replay TTL 取 `DS_TICKET_MAX_TTL`(硬上限 3min)而不是本进程 signer 的 ttl:
        #   兑换的票可能由 hub_allocator / matchmaker 签出,它们的 TTL 与本进程无关。
        #   取小了会让 marker 先于票过期 —— 票还能用、防重放已经失效。
        return _claims_from_v2(v2), pdsticket.DS_TICKET_MAX_TTL.total_seconds()

    async def verify_ds_ticket(self, ticket: str, ds_pod_name: str) -> ldsticket.DSTicketClaims:
        """off/legacy 档兑换点 —— Go `VerifyDSTicket`。单次 SETNX 核销。"""
        return await self._verify_ds_ticket(ticket, ds_pod_name, "", None)

    async def verify_ds_ticket_for_admission(
        self,
        ticket: str,
        ds_pod_name: str,
        admission_id: str,
        admission: ldsadmission.DSAdmissionBinding,
    ) -> ldsticket.DSTicketClaims:
        """Redis authority 档的在线入场兑换点 —— Go `VerifyDSTicketForAdmission`。

        调用方(service 层)必须**已经**依次完成 ① DS callback credential 验签 +
        pod scope、② Redis active 权威核对,并把核对结果作为 `admission` 传进来。
        本方法只做剩下的两步:③ 玩家票 claims 与 caller active binding 精确比对,
        ④ 以 admission owner 幂等消费 jti。

        ★ 顺序不能并成"先消费再比对":那样一次绑定不符的重放会把合法票的 jti 烧掉。
        """
        if not admission_id:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "admission_id is required")
        if admission is None or not admission.complete():
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "ds admission binding is incomplete"
            )
        return await self._verify_ds_ticket(ticket, ds_pod_name, admission_id, admission)

    async def _verify_ds_ticket(  # noqa: C901, PLR0912, PLR0915 —— 逐段对 Go,拆开会丢顺序契约
        self,
        ticket: str,
        ds_pod_name: str,
        admission_id: str,
        admission: ldsadmission.DSAdmissionBinding | None,
    ) -> ldsticket.DSTicketClaims:
        """验签 + 归属绑定核对 + 会话复核 + jti 消费。对齐 Go `verifyDSTicket`。

        顺序是契约,四步都不能换位置:

          ① **验签** —— 拿到 player_id / sjti / 绑定五要件;
          ② **归属绑定** —— 票内 ds_pod 必须等于调用方 pod,且仍是权威当前归属;
          ③ **会话现行性** —— 被顶下线的旧会话在此即被拒;
          ④ **jti 消费** —— legacy 单次 SETNX / admission 版本化 marker。

        ③ 必须在 ④ 之前:先消费 jti 再判会话,会让被顶设备的一次重放把**合法票**的
        jti 先烧掉,新设备再也进不去。② 在 ③ 之前的理由同构 —— 拿错 Pod 的票不该
        消耗任何一次性资源。
        """
        log = plog.get()
        try:
            claims, replay_ttl_sec = self._verify_ds_ticket_signature(ticket)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            # 验签失败拿不到 claims/jti,`ticket_sha`(票体 sha256 前 8 字节,**绝不落
            # 原始 token**)是唯一能与签发侧 ds_ticket_issued 关联"同一张票"的键。
            log.warning(
                "verify_ds_ticket_failed", err=str(exc), ds_pod=ds_pod_name,
                ticket_sha=_ticket_fingerprint(ticket),
            )
            raise
        if claims.player_id == 0:
            raise errcode.PandoraError(errcode.ErrLoginTicketInvalid, "ds ticket has no player")

        admission_repo = None
        attempt_owner = ""
        credential_hash = ""
        marker_status = ldata.AdmissionMarkerStatus.MISSING

        if admission is not None:
            admission_repo = _admission_jti_repo(self._jti_repo)
            if admission_repo is None or not claims.jti:
                # §11.3 R2:两个条件必须能分开 —— 前者是"部署没接 admission repo"
                # (本部署恒不可进),后者是"这张票没有 jti"(单张票的问题)。
                reason = (
                    ADMISSION_REJECT_TICKET_NO_JTI
                    if admission_repo is not None
                    else ADMISSION_REJECT_REPO_UNAVAILABLE
                )
                log.error(
                    "ds_ticket_admission_rejected", reason=reason,
                    player_id=claims.player_id, ds_pod=ds_pod_name,
                    ds_type=claims.ds_type, admission_id=admission_id,
                )
                raise errcode.PandoraError(
                    errcode.ErrUnavailable, "ticket admission replay authority unavailable"
                )
            try:
                attempt_owner = admission.admission_attempt_owner(admission_id)
                credential_hash = admission.accepted_credential_hash()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                # ErrInvalidArg 在 access log 里落 rpc_ok=Debug,线上完全不可见 —— 必须自己打。
                log.warning(
                    "ds_ticket_admission_rejected",
                    reason=ADMISSION_REJECT_MARKER_OWNER_INVALID, err=str(exc),
                    player_id=claims.player_id, ds_pod=ds_pod_name,
                    ds_type=claims.ds_type, admission_id=admission_id,
                )
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "invalid admission marker owner: %s", exc
                ) from exc
            try:
                marker_status = await admission_repo.peek_admission(claims.jti, attempt_owner)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                log.warning(
                    "ds_ticket_admission_rejected", reason=ADMISSION_REJECT_PEEK_FAILED,
                    err=str(exc), jti=claims.jti, player_id=claims.player_id,
                    ds_pod=ds_pod_name, ds_type=claims.ds_type, admission_id=admission_id,
                )
                raise
            if marker_status == ldata.AdmissionMarkerStatus.CONFLICT:
                # 票据已属于另一次 admission = replay / 双准入安全信号。ErrLoginTicketReplayed
                # 是业务码,access log 不当故障 → 这条 WARN 是唯一落盘点。
                log.warning(
                    "ds_ticket_replayed", jti=claims.jti, player_id=claims.player_id,
                    ds_pod=ds_pod_name, ds_type=claims.ds_type,
                )
                raise errcode.PandoraError(
                    errcode.ErrLoginTicketReplayed,
                    "ticket already belongs to another admission",
                )
            try:
                if marker_status == ldata.AdmissionMarkerStatus.MISSING:
                    validate_ticket_admission_strict(claims, ds_pod_name, admission)
                else:
                    validate_ticket_admission_retry(claims, ds_pod_name, admission)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                log.warning(
                    "ds_ticket_admission_binding_rejected", err=str(exc),
                    player_id=claims.player_id, ds_pod=ds_pod_name, ds_type=claims.ds_type,
                )
                raise
            if claims.ds_type == ldsticket.DS_TYPE_HUB:
                await self._check_hub_admission(
                    claims, ds_pod_name, admission_id, admission, marker_status
                )
        elif (
            claims.version == pdsticket.DS_TICKET_VERSION_2
            and claims.ds_type == ldsticket.DS_TYPE_BATTLE
        ):
            await self._check_battle_v2_binding(claims, ds_pod_name)
        elif claims.ds_type == ldsticket.DS_TYPE_HUB:
            await self._check_hub_binding(claims, ds_pod_name)

        # R7 复审 P2-1:会话现行性复核**前置到 replay marker 写入之前**。已被新登录轮换的
        # 旧票在此即被拒,不消耗 jti 防重放名额,也不占 admission 短幂等窗。
        if self._session_gate is not None:
            try:
                await self._session_gate.require_ticket_session_current(
                    claims.player_id, claims.sess_jti
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                log.warning(
                    "ds_ticket_session_gate_rejected_pre_marker",
                    player_id=claims.player_id, ds_pod=ds_pod_name,
                    jti=claims.jti, err=str(exc),
                )
                raise

        if admission is not None:
            # Redis authority:每次都用 Lua 原子确认短幂等窗。missing→marker;
            # 同 attempt 只确认(不覆盖首次 credential hash、不续 TTL)。
            assert admission_repo is not None  # 上面已 fail-closed
            try:
                status = await admission_repo.mark_used_by_admission(
                    claims.jti, attempt_owner, credential_hash, replay_ttl_sec
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                log.warning(
                    "ds_ticket_admission_replay_blocked", jti=claims.jti,
                    player_id=claims.player_id, ds_pod=ds_pod_name, err=str(exc),
                )
                raise
            if status not in (
                ldata.AdmissionMarkerStatus.CREATED,
                ldata.AdmissionMarkerStatus.EXISTING,
            ):
                # 与 peek 的 CONFLICT 同类,但发生在原子 mark 这一步:Peek 之后、Mark
                # 之前有别的 admission 抢占了这张票。
                log.warning(
                    "ds_ticket_admission_marker_conflict", jti=claims.jti,
                    player_id=claims.player_id, ds_pod=ds_pod_name,
                    ds_type=claims.ds_type, admission_id=admission_id,
                    marker_status=int(status),
                )
                raise errcode.PandoraError(
                    errcode.ErrLoginTicketReplayed, "ticket admission marker conflict"
                )
        # ★ `claims.jti != ""` 是必须的前置判据(Go ticket.go:661)。
        # pyjwt 的 `options={"require": ["jti"]}` 只保证**键存在**,不保证非空;空串会在
        # Redis 上铸一个全局共享的防重放键 —— 第二张空 jti 票起全被判重放,而第一张
        # 反而畅通无阻。
        elif self._jti_repo is not None and claims.jti:
            try:
                await self._jti_repo.mark_used(claims.jti, replay_ttl_sec)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                log.warning(
                    "ds_ticket_replay_blocked", jti=claims.jti,
                    player_id=claims.player_id, ds_pod=ds_pod_name, err=str(exc),
                )
                raise

        # INFO 而非 DEBUG(§11.3 R1):这是"玩家真正进到该 DS"的后端完成事实,每玩家每次
        # travel 一条(低频 MILESTONE)。battle 侧没有 hub_admitted 那样的 per-player ACK
        # RPC,本条是 battle 重入的**唯一**成功里程碑 —— 只有 DEBUG 时,"票签出后静默"
        # 既可能是核销成功也可能是 DS 根本没来核销,生产日志形态完全相同,"重连后卡死"
        # 无法分诊。与签发侧 ds_ticket_issued 用 jti 成对对账。
        log.info(
            "ds_ticket_verified", player_id=claims.player_id, ds_type=claims.ds_type,
            match_id=claims.match_id, jti=claims.jti, ds_pod=ds_pod_name,
        )
        return claims

    async def _check_hub_admission(
        self,
        claims: ldsticket.DSTicketClaims,
        ds_pod_name: str,
        admission_id: str,
        admission: ldsadmission.DSAdmissionBinding,
        marker_status: ldata.AdmissionMarkerStatus,
    ) -> None:
        """在线准入下的 Hub assignment 终态门 —— Go `verifyDSTicket` 的 hub admission 段。

        ★ v2 票**有意不携带** callback credential,所以 `stable` 也从 admission 构造:
          当前 credential 已由 admission checker(Redis active)证明过,票只负责搬
          `hub_assignment_id`。用票内零值去比对会让每一张 v2 票都判不符。
        """
        log = plog.get()
        checker = self._assignment_checker
        if checker is None or not hasattr(checker, "check_current_admission"):
            # 部署配置缺口:本部署恒不可进 Hub。玩家侧只看到"进不去",没这条就只能去
            # hub_allocator 侧猜(而那边根本没收到请求)。
            log.error(
                "ds_ticket_admission_rejected",
                reason=ADMISSION_REJECT_HUB_CHECKER_UNAVAILABLE,
                player_id=claims.player_id, ds_pod=ds_pod_name, admission_id=admission_id,
            )
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "hub admission assignment checker unavailable"
            )
        active = lhubbinding.binding_from_admission(admission, claims.hub_assignment_id)
        stable = (
            active
            if claims.version == pdsticket.DS_TICKET_VERSION_2
            else lhubbinding.binding_from_claims(claims)
        )
        try:
            await checker.check_current_admission(
                claims.player_id,
                stable,
                active,
                marker_status == ldata.AdmissionMarkerStatus.MISSING,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            # 重连最常见拒票原因:断线期间 assignment 被 Transfer/Release/同名 Pod 重建。
            # err 原因串否则被 as_code 丢弃(只回 code),任何级别无痕;请求也不经
            # hub_allocator,对侧不会有日志 —— 这里是唯一落盘点。
            log.warning(
                "ds_ticket_admission_rejected",
                reason=ADMISSION_REJECT_HUB_ASSIGNMENT_STALE, err=str(exc),
                player_id=claims.player_id, ds_pod=ds_pod_name, jti=claims.jti,
                ds_type=claims.ds_type, admission_id=admission_id,
                ticket_hub_assignment_id=claims.hub_assignment_id,
            )
            raise

    async def _check_battle_v2_binding(
        self, claims: ldsticket.DSTicketClaims, ds_pod_name: str
    ) -> None:
        """非 admission 档下 battle v2 票的 roster 权威复核 —— Go 的同名分支。

        七项逐条比,返回**第一个**不符的字段名并连同两侧的值落盘:塌成一句话再静默
        return,换 DS 版本(release_track stable→canary)或 instance_epoch 递增导致的
        全服进不去副本只能靠猜。
        """
        if self._battle_authorizer is None:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "battle ticket roster authority unavailable"
            )
        target = await self._battle_authorizer.authorize_battle_ticket(
            claims.player_id, claims.match_id
        )
        field = _battle_v2_binding_mismatch_field(ds_pod_name, claims, target)
        if not field:
            return
        plog.get().warning(
            "ds_ticket_binding_rejected",
            player_id=claims.player_id, ds_type=claims.ds_type, ds_pod=ds_pod_name,
            jti=claims.jti, match_id=claims.match_id, mismatch_field=field,
            ticket_pod=claims.ds_pod_name, ticket_uid=claims.ds_instance_uid,
            ticket_instance_epoch=claims.ds_instance_epoch,
            ticket_allocation_id=claims.allocation_id,
            ticket_release_track=claims.release_track,
            authority_pod=target.pod_name, authority_uid=target.instance_uid,
            authority_instance_epoch=target.instance_epoch,
            authority_allocation_id=target.allocation_id,
            authority_release_track=target.release_track,
        )
        raise errcode.PandoraError(
            errcode.ErrLoginTicketInvalid,
            "battle v2 ticket no longer matches roster authority",
        )

    async def _check_hub_binding(
        self, claims: ldsticket.DSTicketClaims, ds_pod_name: str
    ) -> None:
        """Hub 票的归属绑定门(非 admission 档)。对齐 Go `verifyDSTicket` 的 hub 分支。

        先按 `version` 分叉,**再**看字段 —— 反过来("字段空就当 legacy")会把一张 v2 票
        按 legacy 规则放行,§9.22 exact 实例绑定当场失效。

        legacy 三种绑定形态、三种处置 —— **半绑定比无绑定更危险**,不能当兼容旧票放过:

          | 形态 | 判据 | 处置 |
          |---|---|---|
          | 完整 | `complete()` | 查权威;不匹配即拒 |
          | 半截 | 非空非完整 | **直接拒** —— 签发面半截升级的信号 |
          | 全空 | `empty()` | 看 `require_hub_assignment_binding` 栅栏 |
        """
        if claims.ds_type != ldsticket.DS_TYPE_HUB:
            return
        if claims.version == pdsticket.DS_TICKET_VERSION_2:
            await self._check_hub_binding_v2(claims, ds_pod_name)
            return
        log = plog.get()
        binding = lhubbinding.binding_from_claims(claims)
        common = dict(
            player_id=claims.player_id, ds_type=claims.ds_type, ds_pod=ds_pod_name,
            jti=claims.jti, ticket_pod=claims.ds_pod_name,
            ticket_uid=claims.ds_instance_uid,
            ticket_protocol_epoch=claims.ds_protocol_epoch,
            ticket_credential_gen=claims.ds_credential_gen,
            ticket_hub_assignment_id=claims.hub_assignment_id,
        )
        if binding.complete():
            if not ds_pod_name or ds_pod_name != binding.pod_name:
                log.warning(
                    "ds_ticket_binding_rejected",
                    mismatch_field="caller_ds_pod" if ds_pod_name else "caller_ds_pod_empty",
                    **common,
                )
                raise errcode.PandoraError(
                    errcode.ErrUnauthorized, "hub ticket target pod mismatch"
                )
            if self._assignment_checker is None:
                # 校验器没装 = 权威**不可判定**,不是"通过"。fail-closed。
                raise errcode.PandoraError(
                    errcode.ErrUnavailable, "hub assignment checker unavailable"
                )
            try:
                await self._assignment_checker.check_current(claims.player_id, binding)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                # 归属权威说这张票绑的 assignment / 实例已不是当前的
                # (Transfer / Release / 同名 Pod 重建 / 灰度换轨)。
                log.warning(
                    "ds_ticket_binding_rejected", err=str(exc),
                    mismatch_field="hub_assignment_authority_legacy", **common,
                )
                raise
        elif not binding.empty():
            log.warning(
                "ds_ticket_binding_rejected", mismatch_field="binding_incomplete", **common,
            )
            raise errcode.PandoraError(
                errcode.ErrLoginTicketInvalid, "hub ticket has incomplete assignment binding"
            )
        elif self._require_hub_assignment_binding:
            # 空绑定旧票撞上已激活的栅栏:滚动窗口没排空干净的典型信号。
            log.warning(
                "ds_ticket_binding_rejected", mismatch_field="binding_missing",
                hint="require_hub_assignment_binding 已开,但收到无绑定的旧签发面票据",
                **common,
            )
            raise errcode.PandoraError(
                errcode.ErrLoginTicketInvalid, "hub ticket missing required assignment binding"
            )

    async def _check_hub_binding_v2(
        self, claims: ldsticket.DSTicketClaims, ds_pod_name: str
    ) -> None:
        """v2(RS256)hub 票的归属门 —— Go `verifyDSTicket` 的 `B1HubAssignmentChecker` 分支。

        v2 票不带 callback credential(gen/jti/writer_epoch),所以判据是四要件 +
        release_track,由 `check_current_b1` 从权威记录补齐 credential 后再跑活性校验。
        checker 缺失 = 权威**不可判定** → `ErrUnavailable`(fail-closed),不是"通过"。
        """
        log = plog.get()
        checker = self._assignment_checker
        if checker is None or not hasattr(checker, "check_current_b1"):
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "hub v2 assignment checker unavailable"
            )
        common = dict(
            player_id=claims.player_id, ds_type=claims.ds_type, ds_pod=ds_pod_name,
            jti=claims.jti, ticket_pod=claims.ds_pod_name,
            ticket_uid=claims.ds_instance_uid,
            ticket_instance_epoch=claims.ds_instance_epoch,
            ticket_hub_assignment_id=claims.hub_assignment_id,
            ticket_release_track=claims.release_track,
        )
        if not ds_pod_name or ds_pod_name != claims.ds_pod_name:
            log.warning(
                "ds_ticket_binding_rejected",
                mismatch_field="caller_ds_pod" if ds_pod_name else "caller_ds_pod_empty",
                **common,
            )
            raise errcode.PandoraError(
                errcode.ErrUnauthorized, "hub v2 ticket target pod mismatch"
            )
        try:
            await checker.check_current_b1(
                claims.player_id,
                claims.ds_pod_name,
                claims.ds_instance_uid,
                claims.ds_instance_epoch,
                claims.hub_assignment_id,
                claims.release_track,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            # 归属权威说这张票绑的 assignment / 实例已不是当前的
            # (Transfer / Release / 同名 Pod 重建 / 灰度换轨)。
            log.warning(
                "ds_ticket_binding_rejected", err=str(exc),
                mismatch_field="hub_assignment_authority", **common,
            )
            raise


# ── 兑换点辅助(模块级,与 Go 的包级函数一一对应)────────────────────────────


def _claims_from_v2(v2: pdsticket.DSTicketClaimsV2) -> ldsticket.DSTicketClaims:
    """`DSTicketClaimsV2` → 两路径共用的已验签视图。对齐 Go `verifyDSTicketSignature` 的 RS256 支。

    ★ `ds_protocol_epoch` / `ds_credential_gen` / `ds_credential_jti` / `ds_writer_epoch`
      **恒零**:v2 有意不携带 callback credential。把 `ds_instance_epoch` 顺手抄到
      `ds_protocol_epoch` 会让 v2 票拿 legacy 的绑定规则过门(§9.22 当场失效)。
    ★ `iat` / `exp` 是**秒**,这里的字段是**毫秒**。
    """
    return ldsticket.DSTicketClaims(
        player_id=v2.player_id(),
        match_id=v2.match_id,
        issued_at_ms=int(v2.issued_at * 1000) if v2.issued_at is not None else 0,
        expires_at_ms=int(v2.expires_at * 1000) if v2.expires_at is not None else 0,
        ds_type=v2.ds_type,
        jti=v2.jti,
        region_id=v2.region_id,
        cell_id=v2.cell_id,
        role_id=v2.role_id,
        ds_pod_name=v2.ds_pod_name,
        ds_instance_uid=v2.ds_instance_uid,
        hub_assignment_id=v2.hub_assignment_id,
        ds_instance_epoch=v2.ds_instance_epoch,
        allocation_id=v2.allocation_id,
        release_track=v2.release_track,
        source_match_id=v2.source_match_id,
        sess_jti=v2.sess_jti,
        version=pdsticket.DS_TICKET_VERSION_2,
    )


def _admission_jti_repo(repo):  # noqa: ANN001, ANN202
    """Go 的 `jtiRepo.(data.AdmissionTicketJTIRepo)` 类型断言在 Python 的对应物。

    判据是"两个方法都在",不是 isinstance:测试替身与未来的分片实现都不必继承
    `RedisTicketJTIRepo`。缺任一方法返回 None → 调用方 fail-closed(**不是**
    退回单次 SETNX:那会让在线准入的重试被当成重放拒掉)。
    """
    if repo is None:
        return None
    if not callable(getattr(repo, "peek_admission", None)):
        return None
    if not callable(getattr(repo, "mark_used_by_admission", None)):
        return None
    return repo


def validate_ticket_admission_strict(
    claims: ldsticket.DSTicketClaims,
    ds_pod_name: str,
    admission: ldsadmission.DSAdmissionBinding,
) -> None:
    """marker 不存在的**首次**准入 —— Go `validateTicketAdmissionStrict`。

    首次准入必须把票内完整凭据(legacy)或稳定实例身份(v2)与 caller 当前 active
    逐字段钉死;battle 票没有 assignment 字段,改钉 `match_id` + roster 成员资格。
    """
    if (
        claims is None
        or not admission.complete()
        or not ds_pod_name
        or ds_pod_name != admission.pod_name
        or claims.ds_type != admission.ds_type
    ):
        raise errcode.PandoraError(
            errcode.ErrLoginTicketInvalid, "ds ticket caller type or pod mismatch"
        )
    if claims.version == pdsticket.DS_TICKET_VERSION_2:
        if (
            claims.ds_pod_name != admission.pod_name
            or claims.ds_instance_uid != admission.instance_uid
            or claims.ds_instance_epoch != admission.protocol_epoch
            or not claims.release_track
            or claims.release_track != admission.release_track
        ):
            raise errcode.PandoraError(
                errcode.ErrLoginTicketInvalid, "v2 ticket stable instance binding mismatch"
            )
        if admission.ds_type == ldsticket.DS_TYPE_HUB:
            if claims.match_id != 0 or admission.match_id != 0 or not claims.hub_assignment_id:
                raise errcode.PandoraError(
                    errcode.ErrLoginTicketInvalid, "hub v2 ticket assignment binding invalid"
                )
            return
        if admission.ds_type == ldsticket.DS_TYPE_BATTLE:
            if (
                claims.match_id == 0
                or claims.match_id != admission.match_id
                or not claims.allocation_id
                or claims.allocation_id != admission.allocation_id
                or claims.player_id not in admission.player_ids
            ):
                raise errcode.PandoraError(
                    errcode.ErrLoginTicketInvalid, "battle v2 ticket authority binding mismatch"
                )
            return
        raise errcode.PandoraError(
            errcode.ErrLoginTicketInvalid, "ds ticket admission type invalid"
        )
    if admission.ds_type == ldsticket.DS_TYPE_HUB:
        if (
            claims.match_id != 0
            or admission.match_id != 0
            or claims.ds_pod_name != admission.pod_name
            or claims.ds_instance_uid != admission.instance_uid
            or claims.ds_protocol_epoch != admission.protocol_epoch
            or claims.ds_credential_gen != admission.credential_gen
            or claims.ds_credential_jti != admission.credential_jti
            or claims.ds_writer_epoch != admission.writer_epoch
        ):
            raise errcode.PandoraError(
                errcode.ErrLoginTicketInvalid,
                "hub ticket does not match caller active credential",
            )
        return
    if admission.ds_type == ldsticket.DS_TYPE_BATTLE:
        if (
            claims.match_id == 0
            or claims.match_id != admission.match_id
            or claims.player_id not in admission.player_ids
        ):
            raise errcode.PandoraError(
                errcode.ErrLoginTicketInvalid,
                "battle ticket match does not match caller active credential",
            )
        return
    raise errcode.PandoraError(errcode.ErrLoginTicketInvalid, "ds ticket admission type invalid")


def validate_ticket_admission_retry(
    claims: ldsticket.DSTicketClaims,
    ds_pod_name: str,
    admission: ldsadmission.DSAdmissionBinding,
) -> None:
    """同 attempt_owner marker 已存在时的重试门 —— Go `validateTicketAdmissionRetry`。

    普通 token 轮换允许 gen/jti/exp/kid/hash 变化;**稳定身份**(type/match/pod/UID/
    instance epoch/writer)与 hub assignment_id 仍必须一致。把重试当首次处理会让一次
    正常的凭据轮换把已经准入成功的玩家拒在门外。
    """
    if (
        claims is None
        or not admission.complete()
        or not ds_pod_name
        or ds_pod_name != admission.pod_name
        or claims.ds_type != admission.ds_type
    ):
        raise errcode.PandoraError(
            errcode.ErrLoginTicketInvalid, "ds ticket retry caller type or pod mismatch"
        )
    if claims.version == pdsticket.DS_TICKET_VERSION_2:
        # v2 票不绑普通 callback credential,重试仍必须精确钉住稳定实例与
        # allocation/assignment —— 没有可以放宽的项,直接复用 strict。
        validate_ticket_admission_strict(claims, ds_pod_name, admission)
        return
    if admission.ds_type == ldsticket.DS_TYPE_HUB:
        if (
            claims.match_id != 0
            or admission.match_id != 0
            or not claims.hub_assignment_id
            or claims.ds_pod_name != admission.pod_name
            or claims.ds_instance_uid != admission.instance_uid
            or claims.ds_protocol_epoch != admission.protocol_epoch
            or claims.ds_writer_epoch != admission.writer_epoch
        ):
            raise errcode.PandoraError(
                errcode.ErrLoginTicketInvalid, "hub ticket retry stable identity mismatch"
            )
        return
    if admission.ds_type == ldsticket.DS_TYPE_BATTLE:
        if (
            claims.match_id == 0
            or claims.match_id != admission.match_id
            or claims.player_id not in admission.player_ids
        ):
            raise errcode.PandoraError(
                errcode.ErrLoginTicketInvalid, "battle ticket retry match mismatch"
            )
        return
    raise errcode.PandoraError(
        errcode.ErrLoginTicketInvalid, "ds ticket retry admission type invalid"
    )


def _battle_v2_binding_mismatch_field(
    ds_pod_name: str,
    claims: ldsticket.DSTicketClaims,
    target: lbattleroute.BattleTicketTarget,
) -> str:
    """battle v2 票 ↔ roster 权威的七要件逐条核对,返回**第一个**不符的字段名。

    顺序有意义:先验调用方身份(caller pod),再验票据与权威的实例五要件 ——
    前者不匹配时后者的对比值没有意义。
    """
    if not ds_pod_name:
        return "caller_ds_pod_empty"
    if ds_pod_name != claims.ds_pod_name:
        return "caller_ds_pod"
    if target.pod_name != claims.ds_pod_name:
        return "pod_name"
    if target.instance_uid != claims.ds_instance_uid:
        return "instance_uid"
    if target.instance_epoch != claims.ds_instance_epoch:
        return "instance_epoch"
    if target.allocation_id != claims.allocation_id:
        return "allocation_id"
    if target.release_track != claims.release_track:
        return "release_track"
    return ""
