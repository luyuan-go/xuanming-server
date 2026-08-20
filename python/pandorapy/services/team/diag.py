"""team 链路可诊断性的共用件 —— 对应 Go 侧 internal/biz/diag.go + internal/service/diag.go。

# 为什么单独一个文件

infra.md §11.3 的四条硬规则里有两条是「跨方法一致」才有意义的:
  - R2 要求拒绝分支带**枚举** reason —— 枚举必须只有一份定义,散在各方法里写字面量
    等于没有枚举(同一个原因迟早出现 team_full / full / TEAM_FULL 三种写法,
    看板聚合不了);
  - 「状态机每次 from→to 都有日志」要求所有写路径用**同一个 msg**,否则查一支队伍的
    状态轨迹得先知道它走过哪几个 RPC。

★ 取值必须与 Go **逐字相同**:Loki 告警与运维手册按这些字符串建,改一个字
  等于把告警静默掉,而两边都不报错。
"""

from __future__ import annotations

from pandora.team.v1 import team_pb2

from pandorapy import log as plog

# ── R2 枚举 reason(biz 层,对应 Go diag.go)────────────────────────────────────
#
# 取值一律 snake_case,且**一个 reason 只对应一个 if 条件**。
# 一个 if 里收敛了 N 个条件的(例如 `not found or disbanded or not member`),
# 必须拆成 N 个 reason —— 否则「被拒了」查得到、「为什么被拒」查不到。

# 队伍存在性与状态。
REASON_TEAM_NOT_FOUND = "team_not_found"
REASON_TEAM_DISBANDED = "team_disbanded"
REASON_TEAM_FULL = "team_full"
REASON_TEAM_NOT_RECRUITING = "team_not_recruiting"
REASON_STATE_NOT_ALLOWED = "team_state_not_allowed"

# 成员与权限。
REASON_NOT_MEMBER = "player_not_in_team"
REASON_ALREADY_IN_TEAM = "player_already_in_team"
REASON_NOT_CAPTAIN = "player_not_captain"
REASON_CAPTAIN_SELF_KICK = "captain_cannot_kick_self"
REASON_INVITER_NOT_MEMBER = "inviter_not_in_team"
REASON_INDEX_NON_MEMBER = "player_index_points_to_non_member"

# 邀请 / 申请令牌。
REASON_INVITE_NOT_FOUND = "invite_not_found_or_expired"
REASON_INVITE_TARGET_MISMATCH = "invite_target_mismatch"
REASON_INVITE_TEAM_MISMATCH = "invite_team_mismatch"
REASON_INVITE_LOOKUP_FAILED = "invite_lookup_failed"
REASON_INVITE_STORE_FAILED = "invite_store_failed"
REASON_APPLICATION_NOT_FOUND = "application_not_found_or_expired"
REASON_APPLICATION_STORE_FAILED = "application_store_failed"
# 写入侧总量上限拒绝(§9 不变量 18)。玩家侧表现是「加不进去 / 申请不了」,
# 与「Redis 写失败」必须分开,否则查不出是真满了还是清理没跑。
REASON_INVITE_PENDING_LIMIT = "invite_pending_limit_reached"
REASON_APPLICATION_PENDING_LIMIT = "application_pending_limit_reached"
# 令牌已取走(不可放回)但入队失败。
REASON_JOIN_AFTER_APPLICATION_CONSUMED = "join_failed_after_application_consumed"

# 匹配闸门 / 组票租约(与 matchmaker 的共同线性化点)。
REASON_MATCH_COMMITTED = "team_committed_to_match"
REASON_MATCH_COMMITMENT_UNKNOWN = "match_commitment_unknown"
REASON_PLAYER_COMMITTED = "player_committed_to_match"
REASON_PLAYER_COMMITMENT_UNKNOWN = "player_commitment_unknown"
REASON_ROSTER_LOCKED = "roster_locked_for_match"
# PRE_READY 图(关卡表 ready_mode=1)上队伍没全员准备就点了开始。
# 2026-08-17 曾随「全服取消 ready 门槛」一起删除,2026-08-18 两模式按图二选一后恢复。
REASON_TEAM_NOT_READY = "team_not_ready"
REASON_READY_GENERATION_MISMATCH = "ready_generation_mismatch"
REASON_READY_ALREADY_CLEARED = "ready_already_cleared"
REASON_LEGACY_READY_REVOKED = "legacy_ready_revoked"
REASON_MATCHMAKER_CANCEL_FAILED = "matchmaker_cancel_rpc_failed"
# 摘人与组票之间 TOCTOU 窗口的两种结局。
REASON_RACE_RECHECK_FAILED = "offline_race_recheck_failed"
REASON_RACE_TICKET_FROZE = "match_ticket_froze_removed_member"

# 入参与配额。
REASON_MISSING_ARG = "missing_required_arg"
REASON_RATE_LIMITED = "action_rate_limited"
REASON_RATE_QUOTA_UNKNOWN = "rate_quota_check_failed"

# 存储。
REASON_STORE_READ_FAILED = "store_read_failed"
REASON_STORE_WRITE_FAILED = "store_write_failed"
# 与 data 层 team_update_lock_exhausted 那条的 reason 取值必须一致
# (data 不反向依赖 biz,那边是同值字面量)。
REASON_OPTIMISTIC_RETRY_EXHAUSTED = "optimistic_retry_exhausted"

# 推送(加速器,不是权威;失败只影响到达延迟)。
REASON_PUSH_MARSHAL_FAILED = "push_payload_marshal_failed"
REASON_PUSH_PRODUCE_FAILED = "push_produce_failed"

# 离线判定(offline_leave.py)。
REASON_NO_TEAM = "player_has_no_team"
REASON_SINGLE_MEMBER_TEAM = "single_member_team"
REASON_FEATURE_DISABLED = "offline_leave_disabled"
REASON_NO_TEAMMATE_TO_KEEP = "no_teammate_to_keep"
REASON_PRESENCE_OBSERVE_FAIL = "presence_observe_failed"

# ── R2 枚举 reason(service 层,对应 Go internal/service/diag.go)───────────────
#
# 为什么这一层非补不可:本服务每个 RPC 都以 `Response(code=ERR_XXX)` + gRPC status OK
# 的形状拒绝,统一 access log 会把它记成 rpc_ok(DEBUG 级)——线上默认 info 级下,
# 「未登录被拒」与「team_id 没填」这两类拒绝**一条都不出**。

# ctx 里没有 JWT 注入的 player_id。正常玩家面不该出现(Envoy jwt_authn 已在路由层
# require JWT),持续出现意味着网关配置漂了或有人在绕过网关直连。
REASON_UNAUTHENTICATED = "missing_player_identity"
REASON_MISSING_TEAM_ID = "missing_team_id"
REASON_MISSING_TARGET_PLAYER_ID = "missing_target_player_id"
REASON_MISSING_APPLICANT_ID = "missing_applicant_id"
REASON_MISSING_PLAYER_ID = "missing_player_id"
# 带玩家 JWT 的调用打到了只允许内部东西向的方法(systemOnly 门)。
REASON_SYSTEM_RPC_BY_CLIENT = "system_rpc_by_client"


# ── 状态机轨迹 ────────────────────────────────────────────────────────────────


def log_team_state_changed(
    team_id: int,
    from_state: int,
    to_state: int,
    cause: str,
    ready_generation: int,
    members: int,
) -> None:
    """打一条队伍状态机迁移日志(§11.3 R1:不可逆状态推进打 INFO)。

    msg 固定 team_state_changed,**所有**写路径共用:查一支队伍这一小时的状态轨迹
    只需 `msg=team_state_changed team_id=X`,不必先知道它走过哪几个 RPC。
    from == to 时不打 —— 那不是迁移,打了只会把真正的迁移淹掉。

    cause 用与触发它的 RPC 同名的 snake_case 词根(create / join / leave / kick /
    set_ready / match_start_consume / match_end / presence_lost / offline_leave)。
    """
    if from_state == to_state:
        return
    plog.get().info(
        "team_state_changed",
        team_id=team_id,
        from_state=int(from_state),
        to_state=int(to_state),
        cause=cause,
        ready_generation=ready_generation,
        members=members,
    )


# ── 开局那一刻的成员画像 ──────────────────────────────────────────────────────


def roster_digest(members) -> list[str]:  # noqa: ANN001
    """把一份成员名单压成可读的一维数组,每项形如 `10001:ready=1,hero=7`。

    为什么要它:「队伍人数对不上就开局了」这类故障,事后**唯一**能还原现场的就是
    冻结名单那一刻每个成员的三元组。只打 member_count 查不出是谁缺席,
    而每人一条日志会在 500 人 hub 下把同文件的 WARN 冲走(§11.3 R4)——
    所以压成一条日志里的一个数组字段。
    """
    out: list[str] = []
    for m in members:
        out.append(
            f"{m.player_id}:ready={'1' if m.ready else '0'},hero={m.hero_id}"
        )
    return out


def ready_count(members) -> int:  # noqa: ANN001
    """统计名单里挂着 ready 的人数(与 roster_digest 配套的聚合值)。"""
    return sum(1 for m in members if m.ready)


def log_match_roster_frozen(
    team_id: int,
    captain_id: int,
    attempt_id: str,
    snapshot,  # noqa: ANN001
    reentry: bool,
) -> None:
    """记录「这一局是拿哪份名单开的」。

    这是 INC-20260813-001(3v3 打成 3v2)的核心取证点:BeginTeamMatch 在锁内冻结的
    这份快照就是 matchmaker 建票的输入,票据之后再不会变。

    ⚠️ **online 不在这里**:玩家在不在线的权威在 player_locator,team 只持有
    「成员表 + ready 位 + 队伍状态」。缺席排查要把本条与 locator / offlinewatch 侧
    同 trace_id 的日志并起来看(§9.22 不建影子状态)。
    """
    if snapshot is None:
        return
    members = list(snapshot.members)
    plog.get().info(
        "team_match_roster_frozen",
        team_id=team_id,
        captain_id=captain_id,
        attempt_id=attempt_id,
        reentry=reentry,
        frozen_state=int(snapshot.state),
        ready_generation=snapshot.ready_generation,
        member_count=len(members),
        ready_count=ready_count(members),
        roster=roster_digest(members),
        hint="这份名单就是 matchmaker 建票的输入;成员在线与否见 locator 侧同 trace_id 日志",
    )


def log_rpc_rejected(rpc: str, reason: str, **kv) -> None:
    """service 层拒绝日志。

    msg 固定 team_rpc_rejected + rpc 字段:一个 grep 就能拉出「这一分钟里各接口分别
    因为什么被拒了多少次」,不必按接口逐个记 msg 名。
    """
    plog.get().warning("team_rpc_rejected", rpc=rpc, reason=reason, **kv)


# 状态常量(从生成物引用,不手抄)。
STATE_FORMING = team_pb2.TEAM_STATE_FORMING
STATE_READY = team_pb2.TEAM_STATE_READY
STATE_MATCHING = team_pb2.TEAM_STATE_MATCHING
STATE_IN_BATTLE = team_pb2.TEAM_STATE_IN_BATTLE
STATE_DISBANDED = team_pb2.TEAM_STATE_DISBANDED
STATE_UNSPECIFIED = team_pb2.TEAM_STATE_UNSPECIFIED
