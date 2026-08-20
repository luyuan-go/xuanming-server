"""开局在线闸 —— 对应 Go 侧 internal/biz/match.go 的 ensureAllPresent / absentBeyond。

它防的是 INC-20260813-001 那个形状:队长在队友还没回大厅时开局,
缺席者被原样冻进票据 —— 3v3 打成 3v2。

★ 这个闸的代价方向与 §9.22 **相反**,必须理解为什么:

    §9.22 管的是**归属判定**(这台 DS 能不能操作这个玩家),
    查询失败必须 fail-closed —— 放行等于可能放出第二个 owner。

    本闸管的是**开局体验**(队友在不在大厅)。误杀一个在线玩家的代价是
    "队伍开不了局、玩家不知道为什么",而放过一个离线玩家的代价只是
    "这局少一个人",且后面还有撮合确认期兜着。

    所以判据是:**没有离开基线 = UNKNOWN = 放行**。
    「宁可放过一个,不可误杀在线的」。

★ 缺席名单必须走**结构化通道**回传(不是塞进错误文本):
    光靠 error 文本客户端点不了名,队长只看到一句「有队员不在大厅」
    却不知道该等谁。
"""

from __future__ import annotations


from pandorapy import errcode
from pandorapy import log as plog


# ★ 这里刻意**不**加 @dataclasses.dataclass:它是异常,不是数据类。
# 加上之后 dataclass 会按"没有字段"生成 __repr__ / __eq__ —— repr() 丢掉
# absent_player_ids(排障时最想看的那一项就没了)、所有实例互相相等、且不可 hash。
# slots=True 更会与异常自带的 __dict__ 打架。
class MemberOfflineError(errcode.PandoraError):
    """在线闸拒绝。**额外携带被判缺席的成员** —— service 层据此填
    `StartMatchResponse.absent_player_ids`,让客户端能点名「XX 不在大厅」。

    错误码语义不变(仍是 ErrMatchMemberOffline),`as_code` 照常解析。
    """

    def __init__(self, absent_player_ids: list[int], grace_sec: float) -> None:
        self.absent_player_ids = list(absent_player_ids)
        super().__init__(
            errcode.ErrMatchMemberOffline,
            "players %s left the hub more than %ss ago; cannot start match",
            absent_player_ids,
            grace_sec,
        )


def absent_beyond(
    player_ids: list[int], last_seen_ms: dict[int, int], window_ms: int, now_ms: int
) -> tuple[list[int], int]:
    """按「离开了多久」找出已离场超过 window 的人。返回 (缺席名单, 最长缺席毫秒)。

    ★ 三种情况都**放行**(不算缺席):
      1. 没有任何离开基线(从没上过线 / 已超保留期 / Hub DS 整台挂掉时压根没上报)
      2. 基线 <= 0(脏数据)
      3. 刚离开、还在 window 内(可能正在重连)

    第 1 条是这个函数最关键的一行:**UNKNOWN 一律放行**。
    Hub DS 整台挂掉时所有人都没有离开基线 —— 若把"查不到"当成"离线",
    整个大厅的人都开不了局,而他们其实都在线。
    """
    offline: list[int] = []
    longest_ms = 0
    for pid in player_ids:
        since = last_seen_ms.get(pid, 0)
        if since <= 0:
            continue  # ★ UNKNOWN → 放行
        elapsed = now_ms - since
        if elapsed < window_ms:
            continue  # 刚离开,可能正在重连
        offline.append(pid)
        longest_ms = max(longest_ms, elapsed)
    return offline, longest_ms


def ensure_all_present(
    player_ids: list[int],
    last_seen_ms: dict[int, int],
    *,
    grace_ms: int,
    now_ms: int,
) -> None:
    """在线闸主体。有人缺席超过 grace → 抛 MemberOfflineError。

    grace <= 0 或名单为空 → 闸关闭(直接放行)。
    """
    ids = [p for p in player_ids if p != 0]
    if grace_ms <= 0 or not ids:
        return

    offline, longest_ms = absent_beyond(ids, last_seen_ms, grace_ms, now_ms)
    if not offline:
        return

    plog.get().warning(
        "match_start_member_offline",
        reason="member_absent_beyond_grace",
        offline_players=offline,
        members=len(ids),
        grace_ms=grace_ms,
        longest_absent_ms=longest_ms,
    )
    raise MemberOfflineError(offline, grace_ms / 1000)


def presence_gate_unavailable(op: str, err: Exception, *, fail_open: bool) -> None:
    """收口在线闸的**依赖故障**分支(locator 查不通)。

    ★ 与上面的 UNKNOWN 放行是两回事:
      - UNKNOWN 放行:查通了,但这个玩家没有离开基线 → 当他在线
      - 依赖故障:压根没查通 → 由配置决定 fail-open / fail-closed

    两个分支的日志级别刻意不同:
      fail-open  → WARN(降级了但服务照常)
      fail-closed → ERROR(开局被整个挡住,必须显眼)
    """
    if fail_open:
        plog.get().warning(
            "match_start_presence_gate_fail_open",
            reason="presence_gate_locator_unavailable",
            fail_open=True,
            op=op,
            err=str(err),
        )
        return
    plog.get().error(
        "match_start_presence_gate_fail_closed",
        reason="presence_gate_locator_unavailable",
        fail_open=False,
        op=op,
        err=str(err),
    )
    raise errcode.PandoraError(
        errcode.ErrUnavailable,
        "locator unavailable, cannot verify hub presence (%s): %s",
        op,
        err,
    )


def require_local_game_mode(stored: str, local: str) -> None:
    """防止被路由到默认 PVP 实例的冷客户端去改一张 canonical PVE 票据。

    ★ 空值**只**为滚动升级期的旧记录放行(那时还没有 game_mode 字段);
    每个新写者都会持久化 canonical 命名空间。

    不校验的后果:PVE 票据被 PVP 实例改写,写进错误的队列 / 活跃索引 ——
    玩家排在一个池里却被另一个池撮合,而且**不报错**。
    """
    if stored and stored != local:
        raise errcode.PandoraError(
            errcode.ErrInvalidState,
            "match belongs to game_mode %r, request reached %r",
            stored,
            local,
        )
