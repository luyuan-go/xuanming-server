"""login 业务层 —— 对应 Go 侧 `internal/biz/{login,account_role,ticket,login_ratelimit}.go`。

覆盖范围(诚实边界,交付说明的 honest_gaps 里同样登记):
  ✅ Login / EnterRole / ListAccountRoles / SelectRole / Logout / GetPlayerNo /
     GetResumeContext,会话代际定序、会话现行性门、交付终检、登录失败 Quota、
     owner query-first 路由(applyOwnerPlacement 逐条移植)。
  ⚠️ **断线重连三态门未移植**:Go 的 tryBattleReconnect 依赖 matchmaker 的
     ResolvePlayerMatchContext(耐久权威)+ BattleTicketIssuer(roster 权威门)。
     本版在 locator presence 报 BATTLE 时**不猜**,一律按 WAIT/OWNER_UNKNOWN 返回 ——
     方向与 Go 的 `reconnectErr → WAIT` 分支一致(§9.22:结果不确定必须 fail-closed,
     禁止冒充默认 Hub)。玩家表现为"退避重查",不会被错误地丢回大厅造成双在场。
  ⚠️ DSTicket v2(RS256)未移植:main.py 在 v2 启用时拒启,不会静默降级成 HS256。

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

from pandora.login.v1 import login_pb2

from pandorapy import auth as pauth
from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import safego
from pandorapy.protoenum import enum_name
from pandorapy.services.login import clients as lclients
from pandorapy.services.login import data as ldata
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

_R = login_pb2  # 枚举一律从生成的 pb2 引用,不手抄数值


def _now_ms() -> int:
    return int(time.time() * 1000)


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

    # ── setter(与 Go 的 SetXxx 逐一对应)──────────────────────────────────

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
        # ⚠️ 三态门(matchmaker 耐久权威 + roster 权威签票)未移植。locator presence
        # 报 BATTLE 时**不猜**:presence key 在也不能单独证明对局还活着,不在也不能证明
        # 玩家已离开旧 DS(§9.22)。按 WAIT/OWNER_UNKNOWN 返回,客户端退避重查 ——
        # 与 Go 的 reconnectErr 分支同方向,不会把玩家错误地丢回大厅造成双在场。
        if self._notifier is not None:
            try:
                loc = await self._notifier.get_battle_location(player_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                log.warning(
                    "login_battle_reconnect_unresolved", err=str(exc),
                    account=account, player_id=player_id,
                )
                out = base.copy_base()
                out.resume = _wait_resume(
                    _R.RESUME_WAIT_REASON_OWNER_UNKNOWN, LOGIN_WAIT_RETRY_AFTER_MS
                )
                return await deliver(out)
            if loc.in_battle:
                log.warning(
                    "login_battle_reconnect_unresolved",
                    reason="battle_authority_not_ported",
                    account=account, player_id=player_id, match_id=loc.match_id,
                    hint="Python 版尚未移植 matchmaker 三态权威门;按 WAIT 退避重查,不猜路由",
                )
                out = base.copy_base()
                out.resume = _wait_resume(
                    _R.RESUME_WAIT_REASON_OWNER_UNKNOWN, LOGIN_WAIT_RETRY_AFTER_MS
                )
                return await deliver(out)

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
            hub_addr, hub_ticket, hub_exp_ms = await self._resolve_hub(
                player_id, region_id, cell_id, selected_role_id, 0, sess_jti
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

        addr, ticket, exp_ms = await self._resolve_hub(player_id, 0, 0, role_id, 0, sess_jti)
        # R1:选角完成是不可逆状态推进(角色已落库 + 已签出新 hub 票),必须 INFO ——
        # 「玩家到底选没选角、选的哪个、拿到哪台 Hub」是"卡在选角界面"类问题的第一判据。
        log.info(
            "select_role_ok", player_id=player_id, role_id=role_id, hub_ds_addr=addr,
            hub_ticket_exp_ms=exp_ms, sess_jti=sess_jti, region_id=0, cell_id=0,
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
    """DSTicket 签发 / 兑换。**只覆盖 legacy HS256**;v2 RS256 由 main 拒启拦下。"""

    __slots__ = (
        "_ds_signer", "_jti_repo", "_session_gate",
        "_assignment_checker", "_require_hub_assignment_binding",
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
        del sess_jti  # legacy HS256 票不带 sjti(v2 才有),保留参数与 Go 同形
        if ds_type == ldsticket.DS_TYPE_BATTLE:
            return self._ds_signer.sign(
                player_id, ldsticket.DS_TYPE_BATTLE, match_id=target_id
            )
        if ds_type == ldsticket.DS_TYPE_HUB:
            return self._ds_signer.sign(player_id, ldsticket.DS_TYPE_HUB)
        raise errcode.PandoraError(errcode.ErrInvalidArg, "invalid ds_type %r", ds_type)

    async def verify_ds_ticket(self, ticket: str, ds_pod_name: str) -> ldsticket.DSTicketClaims:
        """验签 + 归属绑定核对 + 会话复核 + jti 一次性消费。

        顺序是契约,四步都不能换位置:

          ① **验签** —— 拿到 player_id / sjti / 绑定五要件;
          ② **归属绑定** —— 票内 ds_pod 必须等于调用方 pod,且仍是权威当前归属;
          ③ **会话现行性** —— 被顶下线的旧会话在此即被拒;
          ④ **jti 消费** —— SETNX 一次性核销。

        ③ 必须在 ④ 之前:先消费 jti 再判会话,会让被顶设备的一次重放把**合法票**的
        jti 先烧掉,新设备再也进不去。② 在 ③ 之前的理由同构 —— 拿错 Pod 的票不该
        消耗任何一次性资源。
        """
        try:
            claims = self._ds_signer.verify(ticket)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            # 验签失败拿不到 claims/jti,`ticket_sha`(票体 sha256 前 8 字节,**绝不落
            # 原始 token**)是唯一能与签发侧 ds_ticket_issued 关联"同一张票"的键。
            plog.get().warning(
                "verify_ds_ticket_failed", err=str(exc), ds_pod=ds_pod_name,
                ticket_sha=_ticket_fingerprint(ticket),
            )
            raise
        if claims.player_id == 0:
            raise errcode.PandoraError(errcode.ErrLoginTicketInvalid, "ds ticket has no player")

        await self._check_hub_binding(claims, ds_pod_name)

        if self._session_gate is not None:
            await self._session_gate.require_ticket_session_current(
                claims.player_id, claims.sess_jti
            )
        # ★ `claims.jti != ""` 是必须的前置判据(Go ticket.go:661)。
        # pyjwt 的 `options={"require": ["jti"]}` 只保证**键存在**,不保证非空;空串会在
        # Redis 上铸一个全局共享的防重放键 —— 第二张空 jti 票起全被判重放,而第一张
        # 反而畅通无阻。
        if self._jti_repo is not None and claims.jti:
            ttl = self._ds_signer.ttl
            try:
                await self._jti_repo.mark_used(
                    claims.jti, ttl.total_seconds() if isinstance(ttl, _dt.timedelta) else 300.0
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                plog.get().warning(
                    "ds_ticket_replay_blocked", jti=claims.jti,
                    player_id=claims.player_id, ds_pod=ds_pod_name, err=str(exc),
                )
                raise

        # INFO 而非 DEBUG(§11.3 R1):这是"玩家真正进到该 DS"的后端完成事实,每玩家每次
        # travel 一条(低频 MILESTONE)。battle 侧没有 hub_admitted 那样的 per-player ACK
        # RPC,本条是 battle 重入的**唯一**成功里程碑 —— 只有 DEBUG 时,"票签出后静默"
        # 既可能是核销成功也可能是 DS 根本没来核销,生产日志形态完全相同,"重连后卡死"
        # 无法分诊。与签发侧 ds_ticket_issued 用 jti 成对对账。
        plog.get().info(
            "ds_ticket_verified", player_id=claims.player_id, ds_type=claims.ds_type,
            match_id=claims.match_id, jti=claims.jti, ds_pod=ds_pod_name,
        )
        return claims

    async def _check_hub_binding(
        self, claims: ldsticket.DSTicketClaims, ds_pod_name: str
    ) -> None:
        """Hub 票的归属绑定门。对齐 Go `ticket.go:593-627` 的 legacy 分支 + `:899-915`。

        三种绑定形态、三种处置 —— **半绑定比无绑定更危险**,不能当兼容旧票放过:

          | 形态 | 判据 | 处置 |
          |---|---|---|
          | 完整 | `complete()` | 查权威;不匹配即拒 |
          | 半截 | 非空非完整 | **直接拒** —— 签发面半截升级的信号 |
          | 全空 | `empty()` | 看 `require_hub_assignment_binding` 栅栏 |
        """
        if claims.ds_type != ldsticket.DS_TYPE_HUB:
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
