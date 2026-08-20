"""matchmaker biz 的纯函数辅助 —— 对应 Go 侧 internal/biz/helpers.go。

装箱 / MMR 窗口 / 进度转换 / 成员工具。全是无副作用的纯函数,单独成文件是为了
让它们能被逐个断言(装箱错一格的后果是"某些人数组合永远凑不出局",线上表现为
"这张图匹配不了",从 usecase 里很难切出来测)。
"""

from __future__ import annotations

from pandora.match.v1 import match_pb2 as matchpb

STAGE_QUEUEING = matchpb.MATCH_STAGE_QUEUEING
STAGE_FOUND = matchpb.MATCH_STAGE_FOUND
STAGE_CONFIRM = matchpb.MATCH_STAGE_CONFIRM
STAGE_ALLOCATING = matchpb.MATCH_STAGE_ALLOCATING
STAGE_READY = matchpb.MATCH_STAGE_READY
STAGE_FAILED = matchpb.MATCH_STAGE_FAILED

CONFIRM_PENDING = matchpb.MATCH_CONFIRM_STATUS_PENDING
CONFIRM_ACCEPTED = matchpb.MATCH_CONFIRM_STATUS_ACCEPTED
CONFIRM_REJECTED = matchpb.MATCH_CONFIRM_STATUS_REJECTED

# 战斗阵营 ID 上限(与 Go 的 dsmetadata.MaxCombatFactionID 同口径)。
# 阵营是对局定义的一部分,越界必须在分配阶段就拒,不能送出一个 DS 解释不了的值。
MAX_COMBAT_FACTION_ID = 255


def within_window(
    a: matchpb.MatchTicketStorageRecord,
    b: matchpb.MatchTicketStorageRecord,
    now_ms: int,
    *,
    base_window: int,
    widen_per_sec: int,
    max_window: int,
) -> bool:
    """票据 b 是否落在以票据 a 为锚点的动态 MMR 窗口内。

    窗口 = min(max_window, base_window + widen_per_sec × 组内最长等待秒数)。
    取**组内最长**等待而不是锚点自己的等待:否则一个刚入队的锚点会把一张已经
    等了十分钟的票挡在窗外,那张票于是永远等不到放宽。
    """
    wait_ms = now_ms - a.enqueued_at_ms
    other = now_ms - b.enqueued_at_ms
    if other > wait_ms:
        wait_ms = other
    wait_sec = wait_ms // 1000
    window = base_window + widen_per_sec * int(wait_sec)
    if window > max_window:
        window = max_window
    return abs(int(a.avg_mmr) - int(b.avg_mmr)) <= window


def bin_pack(
    tickets: list[matchpb.MatchTicketStorageRecord], team_size: int, side_count: int
) -> tuple[list[list[matchpb.MatchTicketStorageRecord]] | None, bool]:
    """把若干张票据装箱成 side_count 方,每方**恰好** team_size 人。

    票据不可拆分(一支队伍必须整体在同一方),所以这是「多背包恰好装满」问题,
    用「大队优先 + 首个装得下的方」贪心。
    side_count<=0 由调用方兜底成 2(历史默认);本函数不做该兜底,免得掩盖调用方
    的取值错误。

    复杂度 O(票数 × side_count);票数被 need=side_count×team_size 上界约束,
    方数是个位数,不需要更聪明的装箱算法(§15.2 够用即可)。
    """
    if team_size <= 0 or side_count <= 0:
        return None, False
    # 稳定降序(大队优先放置,降低装不下的概率)。用 sorted 的稳定性保留同人数票据
    # 的原有 MMR 顺序 —— 顺序不稳定会让同一批票在两次 tick 里装出不同的分方结果。
    ordered = sorted(tickets, key=lambda t: len(t.members), reverse=True)
    sides: list[list[matchpb.MatchTicketStorageRecord]] = [[] for _ in range(side_count)]
    remain = [team_size] * side_count
    for ticket in ordered:
        size = len(ticket.members)
        placed = False
        for i in range(side_count):
            if size <= remain[i]:
                sides[i].append(ticket)
                remain[i] -= size
                placed = True
                break
        if not placed:
            return None, False
    # 每一方都必须恰好装满:留空位说明这批票据凑不出完整对局,交回上层继续等。
    if any(r != 0 for r in remain):
        return None, False
    return sides, True


def build_progress(
    match_id: int,
    stage: int,
    members: list[matchpb.MatchMemberStorageRecord],
    ds_addr: str,
    battle_ticket: str,
    map_id: int,
) -> matchpb.MatchProgress:
    """从成员列表构造 MatchProgress(按 side 分 team_a / team_b)。

    map_id 是本局副本编号:客户端在 READY 收到时据此预置关卡上下文,
    不等 DS 握手后按地图包名反查(同资源多关卡行会歧义)。
    """
    team_a = [m.player_id for m in members if m.side == 0]
    team_b = [m.player_id for m in members if m.side != 0]
    return matchpb.MatchProgress(
        match_id=match_id,
        stage=stage,
        battle_ds_addr=ds_addr,
        battle_ticket=battle_ticket,
        team_a=team_a,
        team_b=team_b,
        map_id=map_id,
    )


def match_to_progress(m: matchpb.MatchStorageRecord) -> matchpb.MatchProgress:
    return build_progress(
        m.match_id, m.stage, list(m.members), m.battle_ds_addr, m.battle_ticket, m.map_id
    )


def ticket_to_progress(t: matchpb.MatchTicketStorageRecord) -> matchpb.MatchProgress:
    """排队中的票据 → QUEUEING 进度(**用 ticket_id 作 match_id 句柄**)。

    句柄跨两个 ID 空间是客户端契约的一部分:排队中拿 ticket_id,成局后拿 match_id。
    """
    return matchpb.MatchProgress(match_id=t.ticket_id, stage=STAGE_QUEUEING, map_id=t.map_id)


def member_index(members, player_id: int) -> int:  # noqa: ANN001
    for i, m in enumerate(members):
        if m.player_id == player_id:
            return i
    return -1


def all_accepted(members) -> bool:  # noqa: ANN001
    """空名单一律 False —— 「没有人」不该被当成「所有人都同意了」。"""
    if not members:
        return False
    return all(m.confirm == CONFIRM_ACCEPTED for m in members)


def member_player_ids(members) -> list[int]:  # noqa: ANN001
    return [m.player_id for m in members]


def ticket_all_accepted(t: matchpb.MatchTicketStorageRecord, confirm_of: dict[int, int]) -> bool:
    """票据全体成员在 match 里是否都已确认接受。

    成员不在 confirm 表中按未确认处理(保守判责,行为确定)。
    """
    return all(confirm_of.get(m.player_id) == CONFIRM_ACCEPTED for m in t.members)


def combat_factions_from_members(members) -> dict[int, int]:  # noqa: ANN001
    """把持久化的 MatchMember.side 转成独立的 match-local 战斗阵营。

    team_id / guild_id **不参与**映射:多个队伍可共享阵营,也允许 0/1 之外的多阵营。
    任一异常一律抛错而不是"尽力而为":送出一份缺阵营的分配,DS 侧只能退化成每人
    一个独立阵营 —— 队友互相能打,对局照常进行照常结算,错误完全不可见。
    """
    if not members:
        raise ValueError("match members are empty")
    factions: dict[int, int] = {}
    for m in members:
        if m.player_id == 0:
            raise ValueError("match member player_id is empty")
        if m.side < 0 or m.side > MAX_COMBAT_FACTION_ID:
            raise ValueError(
                f"player {m.player_id} side {m.side} exceeds combat faction range"
            )
        if m.player_id in factions:
            raise ValueError(f"duplicate match member player_id {m.player_id}")
        factions[m.player_id] = int(m.side)
    return factions


def clone_match(m: matchpb.MatchStorageRecord) -> matchpb.MatchStorageRecord:
    out = matchpb.MatchStorageRecord()
    out.CopyFrom(m)
    return out


def clone_start_operation(
    op: matchpb.MatchStartOperationStorageRecord,
) -> matchpb.MatchStartOperationStorageRecord:
    out = matchpb.MatchStartOperationStorageRecord()
    out.CopyFrom(op)
    return out


def start_operation_terminal(phase: int) -> bool:
    return phase in (matchpb.MATCH_START_PHASE_QUEUED, matchpb.MATCH_START_PHASE_FAILED)


def ticket_from_start_operation(
    op: matchpb.MatchStartOperationStorageRecord,
) -> matchpb.MatchTicketStorageRecord:
    return matchpb.MatchTicketStorageRecord(
        ticket_id=op.ticket_id,
        team_id=op.team_id,
        captain_id=op.captain_id,
        members=op.members,
        avg_mmr=op.avg_mmr,
        enqueued_at_ms=op.created_at_ms,
        map_id=op.map_id,
        game_mode=op.game_mode,
        entry_mode=op.entry_mode,
    )


def start_retry_delay_sec(attempt: int) -> float:
    """saga 重试退避:1,2,4,8,16 秒后封顶 30 秒(与 Go 逐字同)。"""
    shift = min(attempt, 4)
    return min(float(1 << shift), 30.0)


def allocation_retry_delay_sec(attempt: int) -> float:
    """分配重试退避:1,2,4,8 秒后封顶 10 秒(与 Go 逐字同)。"""
    shift = min(attempt, 3)
    return min(float(1 << shift), 10.0)


def oldest_ticket_age_ms(now_ms: int, tickets: list[matchpb.MatchTicketStorageRecord]) -> int:
    """这批票据里最老一张的排队时长(ms)。

    全部无 enqueued_at_ms(滚动升级期旧票)时返回 0,调用方按「无票龄信息」处理,
    不触发 stale 告警 —— 把"不知道"报成"很老"会制造一堆假告警。
    """
    oldest = 0
    for t in tickets:
        at = t.enqueued_at_ms
        if at > 0 and (oldest == 0 or at < oldest):
            oldest = at
    return 0 if oldest == 0 else now_ms - oldest


def ticket_absent_members(
    t: matchpb.MatchTicketStorageRecord, offline: set[int]
) -> list[int]:
    return [m.player_id for m in t.members if m.player_id in offline]


def allocation_from_match(m: matchpb.MatchStorageRecord):  # noqa: ANN201
    """从 match 记录里取回**已 checkpoint 的** battle target。

    返回 (allocation, ok);ok=False 表示没有 target 或 target 不完整。
    不完整的 target 绝不能拿去签票:那样签出的票不再绑定唯一 DS 实例,
    等于任何一台 battle DS 都能兑。
    """
    from pandorapy.services.matchmaker.clients import BattleAllocation

    if not m.HasField("battle_target"):
        return None, False
    target = m.battle_target
    allocation = BattleAllocation(
        address=target.ds_addr,
        pod_name=target.ds_pod_name,
        instance_uid=target.ds_instance_uid,
        instance_epoch=target.ds_instance_epoch,
        allocation_id=target.allocation_id,
        release_track=target.release_track,
    )
    return allocation, allocation.complete_battle()


def battle_target_storage(allocation) -> matchpb.MatchBattleTargetStorageRecord:  # noqa: ANN001
    return matchpb.MatchBattleTargetStorageRecord(
        ds_addr=allocation.address,
        ds_pod_name=allocation.pod_name,
        ds_instance_uid=allocation.instance_uid,
        ds_instance_epoch=allocation.instance_epoch,
        allocation_id=allocation.allocation_id,
        release_track=allocation.release_track,
    )


def validate_signed_battle_tickets(player_ids: list[int], tickets: dict[int, str]) -> None:
    """签票结果必须**覆盖全员且无空票**。

    少签一个人的后果是那个人拿不到票、进不去,而其余 N-1 人已经被推进战斗 ——
    对局带着一个空位开打,且服务端这一侧看起来一切正常。
    """
    for pid in player_ids:
        token = tickets.get(pid)
        if not token:
            raise ValueError(f"battle ticket missing for player {pid}")
