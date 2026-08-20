"""Hub 容量账本 —— 对应 Go 侧 internal/data/hub_capacity_ledger.go 的容量派生部分。

六把 ledger key 与 auth/shard 共用 `{pod}` hashtag,可在**一次** WATCH/MULTI/EXEC
里完成(Redis Cluster 同 slot):

    pandora:hub:reservations:{pod}        HASH assignment_id -> 预留记录
    pandora:hub:reservation-expiry:{pod}  ZSET assignment_id -> 到期时刻
    pandora:hub:sessions:{pod}            HASH assignment_id -> 已连接归属
    pandora:hub:session-expiry:{pod}      ZSET assignment_id -> 到期时刻
    pandora:hub:successors:{pod}          HASH exact_capability -> 接力预留
    pandora:hub:successor-expiry:{pod}    ZSET exact_capability -> 到期时刻

★ 三条不变量,每条都是"违反了不报错、只在容量上悄悄错"的:

════ ① player_count **只由逐 assignment 并集派生** ════

    Heartbeat 上报的 count / list **只写审计字段**,绝不能覆盖 reservation
    或据此推断"这个连接离场了"。

    为什么:心跳是 DS 报的,而 DS 可能漏报、可能在网络分区里报旧数据。
    拿它覆盖账本 = 让一台失联的 DS 决定服务端认为它上面有几个人 ——
    少报会让服务端超额分配座位,多报会让 Hub 永远满员。

════ ② connected ownership **没有时间 TTL** ════

    新格式 `expires_at_ms = 0`,只由 exact Departure 或**已确认的 UID teardown**
    删除。Release / Transfer 只能下发物理 eviction 并**等待那个 proof**,
    不能直接删 ledger 冒充"Pawn 已退出"。

    删了就是假装玩家已经离开 —— 而他可能还在那台 DS 上打。

════ ③ successor 是有界接力,**不重复计容** ════

    同 assignment 重签时:旧 session 仍在 → successor 不计容;
    exact Departure 删掉旧 owner 后 → successor 立即作为 reserved seat 计容,
    直到新 Admission 原子消费或绝对到期。

    不去重的话一个玩家会占两个座位(重连高峰时 Hub 会假性满员)。
"""

from __future__ import annotations

import dataclasses

from pandorapy import errcode
from pandorapy.auth import DS_AUTH_WRITER_EPOCH_V2


@dataclasses.dataclass(slots=True)
class LedgerRecord:
    """账本里的一条记录(预留 / 已连接 / 接力)。"""

    assignment_id: str
    hub_pod_name: str = ""
    hub_instance_uid: str = ""
    auth_epoch: int = 0
    auth_writer_epoch: int = 0
    # ★ connected ownership 的新格式恒为 0 —— 见模块头 ②。
    expires_at_ms: int = 0


@dataclasses.dataclass(slots=True)
class CapacityLedger:
    """一台 Hub Pod 的完整容量账本。"""

    reservations: dict[str, LedgerRecord] = dataclasses.field(default_factory=dict)
    sessions: dict[str, LedgerRecord] = dataclasses.field(default_factory=dict)
    # key 是 exact_capability(不是 assignment_id)—— 同一 assignment 重签会换 capability。
    successors: dict[str, LedgerRecord] = dataclasses.field(default_factory=dict)


def record_matches_instance(
    rec: LedgerRecord, *, pod: str, uid: str, epoch: int, writer: int
) -> bool:
    """记录是否属于当前这个 exact 实例。

    ★ 同名 Pod 重建后 uid / epoch 会变 —— 不比对的话旧实例的账本会被当成
    新实例的,容量凭空多出一批"幽灵占座"。

    ★ 三条**对入参本身**的硬约束,一条都不能省 —— 它们防的不是"记录脏",
    而是"调用方拿着空/零身份来问":

        uid != ""      调用方还没解析出 GameServer UID 时传的是空串。
                       只做 `rec.uid == uid` 的话,空串会与同样是空串的
                       旧格式记录**互相匹配上** —— 于是任意 Pod 的残留
                       记录都被认成本实例的,幽灵占座永远清不掉。
        epoch != 0     同理:0 是"未知实例轮次"的哨兵,不是一个真轮次。
        writer == V2   **恰等于**,不是 >=。这是 Model B 的代际门:放行
                       低代际 writer 就等于承认一个不该再有写权的 DS
                       仍然占着座位 —— 门被拆掉后不会有任何运行期信号。

    这三条不成立时**整体不匹配**,记录会被 prune 清掉(fail-closed):
    宁可把座位放回去重新走一次分配,也不能让来路不明的记录留在账本里。
    """
    if uid == "" or epoch == 0 or writer != DS_AUTH_WRITER_EPOCH_V2:
        return False
    return (
        rec.hub_pod_name == pod
        and rec.hub_instance_uid == uid
        and rec.auth_epoch == epoch
        and rec.auth_writer_epoch == writer
    )


def validate_loaded(ledger: CapacityLedger) -> None:
    """账本**刚从 Redis 解码出来**时的完整性闸门。对应 Go 的加载阶段。

    ★ 这两条在 Go 里是 load 时判、判到就**整条拒**(ErrInvalidState),
    不是"挑掉坏的继续算":账本自身对不上,说明有并发写者绕过了同槽事务,
    此时任何派生值都不可信,继续算只会把错误写回 Redis。

        HASH field ↔ 记录内 assignment_id
            reservations / sessions 的 field 就是 assignment_id。两者不等
            = 有人按错 field 写了记录。若只信 field(Python 的 dict key),
            后续按记录内 id 去做的每一次匹配都会落空:座位删不掉、
            Departure 找不到对应 owner,这个座位就永久漏了。

        一个 assignment 只能有一条 successor
            successor 的 key 是 capability。同 assignment 出现两条 capability
            = 重签时旧的没删干净。派生容量那步是按 assignment_id 归并的,
            **会把这种残留悄悄吸收掉**(算出来仍是 1 个座位)—— 于是账本里
            多出来的那条永远没人发现,直到它到期前一直挡着某条清理路径。

    (successors 的 capability 本身还要与记录里的 player/assignment 互解码校验,
    那部分依赖 capability 编码,随 Redis 存取层一起移植,这里不做。)
    """
    for field, rec in ledger.reservations.items():
        if rec.assignment_id != field:
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "hub reservation field identity mismatch"
            )
    for field, rec in ledger.sessions.items():
        if rec.assignment_id != field:
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "hub session field identity mismatch"
            )
    seen: dict[str, str] = {}
    for capability, rec in ledger.successors.items():
        previous = seen.get(rec.assignment_id)
        if previous is not None and previous != capability:
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "multiple hub successors for one assignment"
            )
        seen[rec.assignment_id] = capability


def prune(
    ledger: CapacityLedger, *, pod: str, uid: str, epoch: int, writer: int, now_ms: int
) -> None:
    """清理过期与不属于本实例的记录。**就地修改**。

    ★ 三类记录的过期规则**不同**,这是最容易抹平的地方:

        reservations  按 expires_at_ms 正常过期
        sessions      新格式 expires_at_ms=0 → **永不因时间过期**
                      (只兼容清理旧/未来格式里 >0 且已到期的)
        successors    按 expires_at_ms 正常过期(它本来就是有界接力)
    """
    for aid, rec in list(ledger.reservations.items()):
        if rec.expires_at_ms <= now_ms or not record_matches_instance(
            rec, pod=pod, uid=uid, epoch=epoch, writer=writer
        ):
            del ledger.reservations[aid]

    for aid, rec in list(ledger.sessions.items()):
        # ★ `expires_at_ms > 0 and` 这个前置条件不能省 —— 省了就变成
        # "0 <= now_ms 恒真" → 每次 prune 把所有已连接玩家全删掉,
        # 服务端以为 Hub 空了,于是继续往里塞人。
        expired = rec.expires_at_ms > 0 and rec.expires_at_ms <= now_ms
        if expired or not record_matches_instance(
            rec, pod=pod, uid=uid, epoch=epoch, writer=writer
        ):
            del ledger.sessions[aid]

    for capability, rec in list(ledger.successors.items()):
        if rec.expires_at_ms <= now_ms or not record_matches_instance(
            rec, pod=pod, uid=uid, epoch=epoch, writer=writer
        ):
            del ledger.successors[capability]


def counts(ledger: CapacityLedger, capacity: int) -> tuple[int, int]:
    """派生 (reserved, connected)。★ 这是 player_count 的**唯一**来源。

    抛异常的三种情况都是"账本自身不自洽",继续用它算容量只会把错误传下去:
      - capacity <= 0:配置错
      - 同一 assignment 同时在 reservation 和 session 里:状态机漏了一次转移
      - 总数超容量:说明某处分配绕过了容量闸
    """
    if capacity <= 0:
        raise errcode.PandoraError(errcode.ErrInvalidState, "hub capacity must be positive")

    # ★ 派生之前先过完整性闸门。Go 是在 Redis 解码那一步判的(更早,且在 prune
    # 之前);Python 侧的 Redis 存取层还没移植,派生是目前唯一必经的关口,
    # 所以钉在这里。存取层落地后要把这一调用**提前到 prune 之前** ——
    # prune 会删记录,可能恰好抹掉一条重复 successor,让坏账本蒙混过关。
    validate_loaded(ledger)

    # ★ 同一 assignment 不能同时是"预留"和"已连接" —— 那是状态机漏了一次转移。
    for aid in ledger.reservations:
        if aid in ledger.sessions:
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "assignment exists in reservation and session ledgers",
            )

    # ★ 按 **assignment_id** 归并:successor 的 key 是 capability,而同一个
    # assignment 可能同时以 reservation 和 successor 两种形态出现 —— 那是同
    # 一个玩家的同一个座位,只能算一格。
    #
    # 注意这里的 set 只负责跨表归并。它**不是**"多条 successor 的去重兜底":
    # 同 assignment 出现多条 successor 属于账本损坏,已由上面的
    # validate_loaded 整条拒掉,绝不能让它在这里被悄悄吸收。
    reserved_assignments = set(ledger.reservations)
    for successor in ledger.successors.values():
        # 旧 session 还在 → 不重复计容(玩家已经占着 connected 那一格)。
        if successor.assignment_id not in ledger.sessions:
            reserved_assignments.add(successor.assignment_id)

    reserved = len(reserved_assignments)
    connected = len(ledger.sessions)
    if reserved + connected > capacity:
        raise errcode.PandoraError(errcode.ErrInvalidState, "hub capacity ledger overflow")
    return reserved, connected


@dataclasses.dataclass(slots=True)
class ShardProjection:
    """分片上的容量投影(派生值,不是权威)。"""

    capacity: int = 0
    reserved_count: int = 0
    connected_ownership_count: int = 0
    player_count: int = 0


def sync_shard_projection(shard: ShardProjection, ledger: CapacityLedger) -> None:
    """把账本派生结果同步到分片投影。

    ★ `player_count = reserved + connected`,**只从账本来**。
    Heartbeat 上报的数字永远不参与这个计算(见模块头 ①)。
    """
    reserved, connected = counts(ledger, shard.capacity)
    shard.reserved_count = reserved
    shard.connected_ownership_count = connected
    shard.player_count = reserved + connected


def apply_heartbeat_audit(shard: ShardProjection, reported_count: int) -> int:
    """处理心跳上报的人数。

    ★ **只返回审计值,不改任何容量字段**。

    这个函数存在的唯一理由是把"心跳数字该往哪放"写成代码里的事实 ——
    有人想拿它更新 player_count 时,得先把这个函数改掉,而不是顺手加一行赋值。

    返回 (上报值 - 派生值) 的差,供告警:持续非零说明 DS 与服务端对
    "这台机器上有几个人"的认知在分叉,值得查,但**不改容量**。
    """
    return reported_count - shard.player_count
