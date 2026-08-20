"""matchmaker 数据层 —— 对应 Go 侧 internal/data/match.go 的 RedisMatchRepo。

Redis key 模板(所有业务 ID 用 uint64,%d 格式化;<mode> = game_mode,
空则退回无模式段的旧全局 key)——**必须与 Go 逐字一致**,迁移期两栈并存,
同一个撮合池会被 Go 副本和 Python 副本同时读写:

    pandora:match:<mode>:queue          ZSET(score=avg_mmr,member=ticket_id)撮合池
    pandora:match:ticket:%d             MatchTicketStorageRecord proto bytes
    pandora:match:{%d}                  MatchStorageRecord proto bytes(hashtag 锁 slot)
    pandora:match:player:%d             ticket_id(SETNX)一人只在一个队列(刻意全局不分模式)
    pandora:match:<mode>:active         ZSET(score=confirm_deadline_ms)确认期超时扫描
    pandora:match:start:{%d}            MatchStartOperationStorageRecord(StartMatch saga)
    pandora:match:start:player:%d       player→start-op 派生索引
    pandora:match:<mode>:start:active   ZSET(score=next_attempt_at_ms)saga due 索引

★ 三条贯穿全文件的铁律(每条都对应一次真实事故形状):

  ① **claim / ticket / match 记录一律无 TTL**。QUEUED/CONFIRM/ALLOCATING/READY
     都是业务状态,不是缓存条目。靠 TTL 暗中释放玩家 = 玩家还在 DS 里、
     matchmaker 这边已经"没人了",下一次 StartMatch 直接放行 → 同人两场。
     只有**显式终态**(FAILED / ReleaseMatch)才用 ticket_ttl / match_ttl 留存。

  ② **派生索引失败绝不回滚权威记录**。queue / active / start:active 三个 ZSET
     都是可由 reconciler 重建的派生索引;权威记录已落库后再因 ZADD 失败去删它,
     等于把一个"索引晚一拍"的小问题升级成"受理了却没有任何记录"的撕裂。

  ③ **删除一律 compare-delete**。无条件 DEL 在「读到旧 claim → 旧 claim 被清 →
     同一玩家新 claim 写入 → 删」的窗口会误删新一局的 claim,玩家于是能开第二张票。

★ Redis Cluster:ticketKey / matchKey / 各 ZSET 分属不同 slot,不能捆同一事务
  (CROSSSLOT)。所以每处都是「单键 CAS + 跨 slot 独立索引命令」,
  与 Go 一字不差 —— 换成一把大事务在单机能过、上了 Cluster 全线报错。
"""

from __future__ import annotations

from typing import Any, Callable

from redis.exceptions import WatchError

from pandora.match.v1 import match_pb2 as matchpb

from pandorapy import errcode
from pandorapy import redisx

# CreateMatch / CreateStartOperation 中派生索引 ZADD 的有界重试参数。
CREATE_MATCH_ZADD_RETRY = 3
CREATE_MATCH_ZADD_BACKOFF_SEC = 0.02

# ReserveTicket / DeleteTicketIfUnmatched 等票据键 CAS 的冲突重试上限。
# 票据键的并发写只有"撮合循环预留"与"玩家取消删除"两方,冲突极短暂,3 次足够。
TICKET_CAS_RETRY = 3


def queue_key_for(namespace: str) -> str:
    return "pandora:match:queue" if not namespace else f"pandora:match:{namespace}:queue"


def active_key_for(namespace: str) -> str:
    return "pandora:match:active" if not namespace else f"pandora:match:{namespace}:active"


def start_active_key_for(namespace: str) -> str:
    return (
        "pandora:match:start:active"
        if not namespace
        else f"pandora:match:{namespace}:start:active"
    )


def ticket_key(ticket_id: int) -> str:
    return f"pandora:match:ticket:{ticket_id}"


def match_key(match_id: int) -> str:
    return f"pandora:match:{{{match_id}}}"


def player_key(player_id: int) -> str:
    return f"pandora:match:player:{player_id}"


def start_operation_key(ticket_id: int) -> str:
    return f"pandora:match:start:{{{ticket_id}}}"


def start_player_key(player_id: int) -> str:
    return f"pandora:match:start:player:{player_id}"


# 仅当 claim 当前值仍指向待清理的旧票据时才 DEL(原子比较,
# 防「读旧 claim → 过期 → 新 claim 写入 → 误删」的并发窗口)。
DELETE_CLAIM_SCRIPT = redisx.LuaScript(
    "match_delete_claim",
    """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0""",
)

# 仅当 claim 当前值仍是本票据时移除旧 TTL。新版本从创建起就是 persistent;
# 该脚本用于滚动升级期间把旧 claim 原子升级为 durable。
PERSIST_CLAIM_SCRIPT = redisx.LuaScript(
    "match_persist_claim",
    """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PERSIST', KEYS[1])
  return 1
end
return 0""",
)


def _as_int(raw: Any) -> int:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return int(raw)


def _unmarshal_ticket(ticket_id: int, payload: bytes) -> matchpb.MatchTicketStorageRecord:
    rec = matchpb.MatchTicketStorageRecord()
    try:
        rec.ParseFromString(payload)
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "ticket %d bad proto: %s", ticket_id, exc
        ) from exc
    if rec.ticket_id == 0:
        rec.ticket_id = ticket_id
    if rec.ticket_id != ticket_id:
        raise errcode.PandoraError(
            errcode.ErrInternal, "ticket %d id mismatch: %d", ticket_id, rec.ticket_id
        )
    return rec


def _unmarshal_match(match_id: int, payload: bytes) -> matchpb.MatchStorageRecord:
    rec = matchpb.MatchStorageRecord()
    try:
        rec.ParseFromString(payload)
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "match %d bad proto: %s", match_id, exc
        ) from exc
    if rec.match_id == 0:
        rec.match_id = match_id
    if rec.match_id != match_id:
        raise errcode.PandoraError(
            errcode.ErrInternal, "match %d id mismatch: %d", match_id, rec.match_id
        )
    return rec


def _unmarshal_start_op(
    ticket_id: int, payload: bytes
) -> matchpb.MatchStartOperationStorageRecord:
    rec = matchpb.MatchStartOperationStorageRecord()
    try:
        rec.ParseFromString(payload)
    except Exception as exc:  # noqa: BLE001
        raise errcode.PandoraError(
            errcode.ErrInternal, "start operation %d bad proto: %s", ticket_id, exc
        ) from exc
    if rec.ticket_id == 0:
        rec.ticket_id = ticket_id
    if rec.ticket_id != ticket_id:
        raise errcode.PandoraError(
            errcode.ErrInternal,
            "start operation %d id mismatch: %d",
            ticket_id,
            rec.ticket_id,
        )
    return rec


class RedisMatchRepo:
    """基于 redis-py asyncio 的 MatchRepo 实现。

    namespace 通常传服务的 game_mode(如 "5v5_ranked");空串保留旧全局 key。
    仅影响 queue / active / start:active 三个扫描索引 —— ticket / match 记录本体
    由全局唯一 snowflake ID 定址不跨模式碰撞;player claim 也保持全局,
    落实「一人同一时刻只在一个队列(跨所有模式)」(不变量 §1)。
    """

    __slots__ = ("_rdb", "queue_key", "active_key", "start_key")

    def __init__(self, rdb, namespace: str) -> None:  # noqa: ANN001
        self._rdb = rdb
        self.queue_key = queue_key_for(namespace)
        self.active_key = active_key_for(namespace)
        self.start_key = start_active_key_for(namespace)

    # ── player claim ────────────────────────────────────────────────────────

    async def claim_player(self, player_id: int, ticket_id: int) -> tuple[int, bool]:
        """SETNX 声明 player→ticket 归属。返回 (占用者 ticket_id, 是否本次抢到)。

        TTL=0(永不过期):见文件头铁律 ①。
        """
        key = player_key(player_id)
        value = str(ticket_id)
        for _attempt in range(2):
            if await self._rdb.set(key, value, nx=True):
                return ticket_id, True
            current = await self._rdb.get(key)
            if current is None:
                continue  # 占用者刚好被清掉,重试一次 SETNX
            return _as_int(current), False
        raise errcode.PandoraError(
            errcode.ErrMatchConcurrent, "claim player %d concurrent", player_id
        )

    async def get_player_ticket(self, player_id: int) -> tuple[int, bool]:
        value = await self._rdb.get(player_key(player_id))
        if value is None:
            return 0, False
        return _as_int(value), True

    async def delete_player_index_if_matches(self, player_id: int, ticket_id: int) -> None:
        """compare-delete(铁律 ③)。幂等:值不匹配 / 已不存在时不动、不报错。"""
        await DELETE_CLAIM_SCRIPT(self._rdb, keys=[player_key(player_id)], args=[str(ticket_id)])

    async def refresh_player_claim(self, player_id: int, ticket_id: int) -> None:
        """滚动升级兼容门:仅当 claim 仍指向本票据时 PERSIST。

        新 claim 本来就无 TTL;旧版本遗留 TTL claim 在退队时升级为 durable ——
        少了这一步,claim 会先于票据过期,玩家能再开一张票 → 双票双局。
        """
        await PERSIST_CLAIM_SCRIPT(self._rdb, keys=[player_key(player_id)], args=[str(ticket_id)])

    async def persist_player_claim(self, player_id: int, ticket_id: int) -> None:
        """与 refresh 同脚本,但**结果是断言**:不再属于本票据即 fail-closed。

        用在成局路径(票据已预留、即将拉 DS)上:此时 claim 若已不属于本票据,
        说明这批玩家里有人已经在别的局里了,继续走下去会拉起一场"人在两处"的对局。
        """
        result = await PERSIST_CLAIM_SCRIPT(
            self._rdb, keys=[player_key(player_id)], args=[str(ticket_id)]
        )
        if _as_int(result or 0) != 1:
            raise errcode.PandoraError(
                errcode.ErrMatchConcurrent,
                "player %d claim no longer belongs to ticket %d",
                player_id,
                ticket_id,
            )

    # ── ticket ──────────────────────────────────────────────────────────────

    async def create_ticket_record(self, ticket: matchpb.MatchTicketStorageRecord) -> None:
        """只写票据主体(SET,不入 queue)—— StartMatch 三步写序第 1 步。

        写序铁律(镜像 team CreateTeam 的结论):必须**先写票据主体、再 claim 成员、
        最后入队**。claim 的僵尸自愈以「票据主体不存在」为判据 —— 若先 claim 后写
        主体,并发的另一次 StartMatch 会把 in-flight claim 误判僵尸并 CAS 删掉,
        同批玩家两张票同时入队(违反不变量 §1)。主体先落地时 ticket_id 尚未入队、
        无人引用,天然安全。
        """
        await self._rdb.set(ticket_key(ticket.ticket_id), ticket.SerializeToString())

    async def enqueue_ticket(self, ticket: matchpb.MatchTicketStorageRecord) -> None:
        """ZADD 入 queue(第 3 步)。幂等。"""
        await self._rdb.zadd(self.queue_key, {str(ticket.ticket_id): float(ticket.avg_mmr)})

    async def add_ticket(self, ticket: matchpb.MatchTicketStorageRecord) -> None:
        """仅供 requeue / 测试造数据:主体 + 入队两步。StartMatch 入队路径不得走它。"""
        await self.create_ticket_record(ticket)
        await self.enqueue_ticket(ticket)

    async def queue_len(self) -> int:
        return int(await self._rdb.zcard(self.queue_key))

    async def get_ticket(
        self, ticket_id: int
    ) -> tuple[matchpb.MatchTicketStorageRecord | None, bool]:
        payload = await self._rdb.get(ticket_key(ticket_id))
        if payload is None:
            return None, False
        return _unmarshal_ticket(ticket_id, payload), True

    async def range_queue_tickets(self) -> list[int]:
        """按 avg_mmr 升序返回 queue 中全部 ticket_id。"""
        values = await self._rdb.zrange(self.queue_key, 0, -1)
        return [_as_int(v) for v in values]

    async def reserve_ticket(self, ticket: matchpb.MatchTicketStorageRecord) -> None:
        """把票据从 queue 移出并写回(撮合命中:调用方已写好 ticket.match_id)。

        WATCH CAS 而非盲 SET,封的是**双向**竞态:
          - CancelMatch 并发 CAS 删票后盲 SET 会把票据"复活"进 match,而成员 claim
            已释放 → 玩家可再排队,同人两场;
          - leader 交接重叠时两个循环可能同时预留同一张票 → 重复成局。
        """
        payload = ticket.SerializeToString()
        key = ticket_key(ticket.ticket_id)
        for _attempt in range(TICKET_CAS_RETRY):
            gone = False
            conflict_match = 0
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    if raw is None:
                        gone = True
                    else:
                        current = _unmarshal_ticket(ticket.ticket_id, raw)
                        if current.match_id not in (0, ticket.match_id):
                            conflict_match = current.match_id
                    if not gone and conflict_match == 0:
                        pipe.multi()
                        pipe.set(key, payload)
                        await pipe.execute()
                    else:
                        await pipe.reset()
            except WatchError:
                continue  # 并发写(取消 / 另一次预留),重读再判
            if gone:
                raise errcode.PandoraError(
                    errcode.ErrMatchNotFound, "ticket %d gone (cancelled)", ticket.ticket_id
                )
            if conflict_match != 0:
                raise errcode.PandoraError(
                    errcode.ErrMatchConcurrent,
                    "ticket %d already reserved by match %d",
                    ticket.ticket_id,
                    conflict_match,
                )
            # 跨 slot:ZREM 独立执行。失败残留队列项无害 —— matchOnce 加载时
            # 会看到 match_id != 0 而跳过(自愈)。
            try:
                await self._rdb.zrem(self.queue_key, str(ticket.ticket_id))
            except Exception:  # noqa: BLE001, S110 —— best-effort,见上
                pass
            return
        raise errcode.PandoraError(
            errcode.ErrMatchConcurrent,
            "reserve ticket %d concurrent retry exhausted",
            ticket.ticket_id,
        )

    async def requeue_ticket_if_owned(
        self, ticket: matchpb.MatchTicketStorageRecord, expected_match_id: int
    ) -> bool:
        """守卫退队:仅当票据仍存在且存储态 match_id == expected 时写回并重入队。

        竞态背景:退票路径读到票据 T(属 match M)后,若并发的 CancelMatch 先 CAS
        删了 T 并释放全员 claim,盲 SET 会把 T「复活」进队列 —— 而 claim 已释放,
        已取消的玩家会被下一轮重新凑局 / 白拉 DS → no_show 记罚,
        直接违反「退出路径零副作用」。
        """
        payload = ticket.SerializeToString()  # 调用方已把 match_id 置 0
        key = ticket_key(ticket.ticket_id)
        for _attempt in range(TICKET_CAS_RETRY):
            wrote = False
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    if raw is not None:
                        current = _unmarshal_ticket(ticket.ticket_id, raw)
                        # 已退队(0)/已归属他局 → no-op,绝不窃取他局在票
                        if current.match_id == expected_match_id:
                            pipe.multi()
                            pipe.set(key, payload)
                            await pipe.execute()
                            wrote = True
                    if not wrote:
                        await pipe.reset()
            except WatchError:
                continue
            if not wrote:
                return False
            await self.enqueue_ticket(ticket)
            return True
        raise errcode.PandoraError(
            errcode.ErrMatchConcurrent,
            "requeue ticket %d concurrent retry exhausted",
            ticket.ticket_id,
        )

    async def delete_ticket(self, ticket_id: int) -> None:
        """删票据 record + 移出 queue。两步都幂等。"""
        await self._rdb.delete(ticket_key(ticket_id))
        await self._rdb.zrem(self.queue_key, str(ticket_id))

    async def delete_ticket_if_match(
        self, ticket_id: int, expected_match_id: int
    ) -> tuple[bool, bool, int]:
        """仅当票据仍精确属于 expected_match_id 时 CAS 删除。

        返回 (deleted, found, current_match_id)。
        ReleaseMatch 不得在「先读旧 ticket → ticket 被新局复用 → 无条件 DEL」的窗口
        误删新局:found=True 但 current 不等于 expected 必须 fail-closed。
        """
        if expected_match_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "expected match_id required")
        key = ticket_key(ticket_id)
        for _attempt in range(TICKET_CAS_RETRY):
            found = False
            deleted = False
            current_match_id = 0
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    if raw is not None:
                        rec = _unmarshal_ticket(ticket_id, raw)
                        found = True
                        current_match_id = rec.match_id
                        if current_match_id == expected_match_id:
                            pipe.multi()
                            pipe.delete(key)
                            await pipe.execute()
                            deleted = True
                    if not deleted:
                        await pipe.reset()
            except WatchError:
                continue
            if not found:
                # 之前那次精确删除可能已提交而派生 ZREM 失败/响应丢失。
                # ticket_id 是全局不复用的围栏身份,所以缺失的权威票据仍须先修好
                # 陈旧队列成员,ReleaseMatch 才能 ACK 它的 outbox 行。
                await self._rdb.zrem(self.queue_key, str(ticket_id))
                return False, False, 0
            if not deleted:
                return False, True, current_match_id
            # queue 是派生的跨 slot 索引:只在精确删除提交之后才移除;
            # 失败如实返回,免得释放 outbox 对一次没清干净的操作 ACK。
            await self._rdb.zrem(self.queue_key, str(ticket_id))
            return True, True, current_match_id
        raise errcode.PandoraError(
            errcode.ErrMatchConcurrent,
            "delete ticket %d for match %d concurrent retry exhausted",
            ticket_id,
            expected_match_id,
        )

    async def delete_ticket_if_unmatched(self, ticket_id: int) -> tuple[bool, int]:
        """仅当票据仍未被撮合(match_id==0)时 CAS 删除并移出 queue。

        返回 (deleted, reserved_match_id):
          (True, 0)  已删除
          (False, M) 已被撮合进 match M
          (False, 0) 票据已不存在
        """
        key = ticket_key(ticket_id)
        for _attempt in range(TICKET_CAS_RETRY):
            missing = False
            reserved = 0
            deleted = False
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    if raw is None:
                        missing = True
                    else:
                        rec = _unmarshal_ticket(ticket_id, raw)
                        if rec.match_id != 0:
                            reserved = rec.match_id
                        else:
                            pipe.multi()
                            pipe.delete(key)
                            await pipe.execute()
                            deleted = True
                    if not deleted:
                        await pipe.reset()
            except WatchError:
                continue  # 撮合循环并发预留了票据 → 重读再判
            if missing:
                return False, 0
            if reserved != 0:
                return False, reserved
            try:
                await self._rdb.zrem(self.queue_key, str(ticket_id))
            except Exception:  # noqa: BLE001, S110 —— best-effort,matchOnce miss 自愈
                pass
            return True, 0
        raise errcode.PandoraError(
            errcode.ErrMatchConcurrent, "delete ticket %d concurrent retry exhausted", ticket_id
        )

    # ── StartMatch durable saga ─────────────────────────────────────────────

    async def create_start_operation(
        self, op: matchpb.MatchStartOperationStorageRecord
    ) -> None:
        """StartMatch RPC 的**唯一落库点**(commit point)。

        为什么不在 RPC 里同步把「写票据主体 → 逐个 claim 成员 → 入队」三步做完:
        这三步中间任何一步之后玩家断线、RPC 被取消或进程重启,都会留下半成品
        (有的成员已被 claim、票据却没入队),违反「一人只在一个队列」(不变量 §1)
        与「不卡死 / 可恢复」(§9.19/§9.23)。

        三步走,权威关系必须分清:
          ① SETNX 写权威 record,**TTL=0**。ACCEPTED 是业务事实不是缓存条目;
             同 operation_id 重入视为幂等重试,不同 operation 占同一 ticket 才是冲突。
          ② best-effort 建 player→start-op 派生索引。走到这里 record 已落库、
             本次 RPC 已经是「已受理」,**不能**再因 claim 失败向调用方返回失败 ——
             否则会出现「调用方收到失败、record 却仍被 worker 入队」的撕裂。
          ③ ZADD due 索引唤醒 worker。失败**不回滚 record、也不报错**:
             丢了这条索引由全 master reconciler 扫 canonical record 补回。
        """
        payload = op.SerializeToString()
        key = start_operation_key(op.ticket_id)
        created = await self._rdb.set(key, payload, nx=True)
        if not created:
            existing, found = await self.get_start_operation(op.ticket_id)
            if not found or existing.operation_id != op.operation_id:
                raise errcode.PandoraError(
                    errcode.ErrMatchConcurrent,
                    "start operation ticket %d already exists",
                    op.ticket_id,
                )

        for member in op.members:
            try:
                existing_ticket, claimed = await self.claim_start_player(
                    member.player_id, op.ticket_id
                )
            except Exception:  # noqa: BLE001 —— 见 ② :已受理,不得反向失败
                break
            if not claimed and existing_ticket != op.ticket_id:
                break

        await self._zadd_with_retry(
            self.start_key, str(op.ticket_id), float(op.next_attempt_at_ms)
        )

    async def _zadd_with_retry(self, key: str, member: str, score: float) -> None:
        """有界重试的派生索引 ZADD。**吞掉最终失败**(铁律 ②)。"""
        import asyncio

        for attempt in range(CREATE_MATCH_ZADD_RETRY):
            try:
                await self._rdb.zadd(key, {member: score})
                return
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                if attempt == CREATE_MATCH_ZADD_RETRY - 1:
                    return
                await asyncio.sleep(CREATE_MATCH_ZADD_BACKOFF_SEC)

    async def claim_start_player(self, player_id: int, ticket_id: int) -> tuple[int, bool]:
        """SETNX 建 player→start-op 索引。持久(无 TTL)直到 QUEUED 交接或补偿删除。"""
        key = start_player_key(player_id)
        value = str(ticket_id)
        for _attempt in range(2):
            if await self._rdb.set(key, value, nx=True):
                return ticket_id, True
            current = await self._rdb.get(key)
            if current is None:
                continue
            existing = _as_int(current)
            if existing == ticket_id:
                # 旧版本可能留了 TTL:原子升级成 durable(不做的话索引会先于
                # saga 消失,冷启动 / 并发 Start 就发现不了这条在途 saga)。
                await PERSIST_CLAIM_SCRIPT(self._rdb, keys=[key], args=[value])
            return existing, False
        raise errcode.PandoraError(
            errcode.ErrMatchConcurrent, "claim start player %d concurrent", player_id
        )

    async def get_start_player_operation(self, player_id: int) -> tuple[int, bool]:
        value = await self._rdb.get(start_player_key(player_id))
        if value is None:
            return 0, False
        ticket_id = _as_int(value)
        if ticket_id == 0:
            raise errcode.PandoraError(
                errcode.ErrInternal, "start player %d bad ticket id", player_id
            )
        return ticket_id, True

    async def delete_start_player_if_matches(self, player_id: int, ticket_id: int) -> None:
        await DELETE_CLAIM_SCRIPT(
            self._rdb, keys=[start_player_key(player_id)], args=[str(ticket_id)]
        )

    async def get_start_operation(
        self, ticket_id: int
    ) -> tuple[matchpb.MatchStartOperationStorageRecord | None, bool]:
        payload = await self._rdb.get(start_operation_key(ticket_id))
        if payload is None:
            return None, False
        return _unmarshal_start_op(ticket_id, payload), True

    async def update_start_operation_with_lock(
        self,
        ticket_id: int,
        max_retry: int,
        fn: Callable[[matchpb.MatchStartOperationStorageRecord], None],
        ttl_sec: float,
    ) -> None:
        """WATCH/MULTI/EXEC 读-改-写 start operation。

        只有推进到**显式终态**(QUEUED / FAILED)时才打 retention TTL;
        非终态一律无 TTL —— 一条正在推进的 saga 因 TTL 消失会让被 claim 的成员
        永远卡在"有 claim 没票据"(玩家再也开不了局,且没有任何日志)。
        """
        key = start_operation_key(ticket_id)
        for _attempt in range(max_retry + 1):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    if raw is None:
                        await pipe.reset()
                        raise errcode.PandoraError(
                            errcode.ErrMatchNotFound, "start operation %d not found", ticket_id
                        )
                    op = _unmarshal_start_op(ticket_id, raw)
                    fn(op)  # 业务错误在这里抛出,由下面的 PandoraError 分支原样冒泡
                    terminal = op.phase in (
                        matchpb.MATCH_START_PHASE_QUEUED,
                        matchpb.MATCH_START_PHASE_FAILED,
                    )
                    pipe.multi()
                    if terminal and ttl_sec > 0:
                        pipe.set(key, op.SerializeToString(), ex=int(ttl_sec))
                    else:
                        pipe.set(key, op.SerializeToString())
                    await pipe.execute()
                return
            except WatchError:
                continue  # CAS 冲突,重试
            except errcode.PandoraError:
                raise  # 业务错误不重试(否则会把"拒绝"变成"重试耗尽")
        raise errcode.PandoraError(
            errcode.ErrMatchConcurrent,
            "start operation %d update concurrent retry exhausted",
            ticket_id,
        )

    async def ensure_start_active(self, ticket_id: int, score_ms: int) -> None:
        await self._rdb.zadd(self.start_key, {str(ticket_id): float(score_ms)})

    async def range_due_start_operations(self, now_ms: int) -> list[int]:
        values = await self._rdb.zrangebyscore(self.start_key, "-inf", now_ms)
        return [_as_int(v) for v in values]

    async def remove_start_active(self, ticket_id: int) -> None:
        await self._rdb.zrem(self.start_key, str(ticket_id))

    async def delete_start_operation(self, ticket_id: int) -> None:
        await self._rdb.zrem(self.start_key, str(ticket_id))
        await self._rdb.delete(start_operation_key(ticket_id))

    async def scan_start_operation_ids(self, count: int = 128) -> list[int]:
        keys = await self._scan_all_masters("pandora:match:start:{*}", count)
        return _parse_ids_from_keys(keys, "pandora:match:start:{")

    # ── match ───────────────────────────────────────────────────────────────

    async def create_match(self, match: matchpb.MatchStorageRecord) -> None:
        """写 match 记录(SETNX,无 TTL)并 ZADD 进 active。

        SETNX 而非 SET:match_id 一律是新雪花,权威记录绝不允许被同 ID 后来者覆盖。
        旧「无 NX 的 SET」在 solo 复用 ticket_id 做 match_id 的年代会静默清掉既有
        match(requeue 风暴的放大器)。冲突 = ID 生成/复用 bug,fail-fast 交上游:
        票据未动仍在队列,下轮换新 ID 重试。
        """
        created = await self._rdb.set(
            match_key(match.match_id), match.SerializeToString(), nx=True
        )
        if not created:
            raise errcode.PandoraError(
                errcode.ErrAlreadyExists, "match %d already exists", match.match_id
            )
        await self._zadd_with_retry(
            self.active_key, str(match.match_id), float(match.confirm_deadline_ms)
        )

    async def get_match(self, match_id: int) -> tuple[matchpb.MatchStorageRecord | None, bool]:
        payload = await self._rdb.get(match_key(match_id))
        if payload is None:
            return None, False
        return _unmarshal_match(match_id, payload), True

    async def update_match_with_lock(
        self,
        match_id: int,
        max_retry: int,
        fn: Callable[[matchpb.MatchStorageRecord], None],
    ) -> None:
        """WATCH/MULTI/EXEC 读-改-写 match;CAS 失败重试 max_retry 次。

        写回**不带 TTL**:非终态 match 是业务状态(铁律 ①);终态留存由
        expire_match 单独打 TTL。
        """
        key = match_key(match_id)
        for _attempt in range(max_retry + 1):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    raw = await pipe.get(key)
                    if raw is None:
                        await pipe.reset()
                        raise errcode.PandoraError(
                            errcode.ErrMatchNotFound, "match %d not found", match_id
                        )
                    match = _unmarshal_match(match_id, raw)
                    fn(match)
                    pipe.multi()
                    pipe.set(key, match.SerializeToString())
                    await pipe.execute()
                return
            except WatchError:
                continue
            except errcode.PandoraError:
                raise  # fn 的业务错误,不重试
        raise errcode.PandoraError(
            errcode.ErrMatchConcurrent, "match %d update concurrent retry exhausted", match_id
        )

    async def remove_active(self, match_id: int) -> None:
        await self._rdb.zrem(self.active_key, str(match_id))

    async def ensure_active(self, match_id: int, score_ms: int) -> None:
        await self._rdb.zadd(self.active_key, {str(match_id): float(score_ms)})

    async def range_active_matches(self) -> list[int]:
        values = await self._rdb.zrange(self.active_key, 0, -1)
        return [_as_int(v) for v in values]

    async def range_expired_matches(self, now_ms: int) -> list[int]:
        values = await self._rdb.zrangebyscore(self.active_key, "-inf", now_ms)
        return [_as_int(v) for v in values]

    async def scan_match_ids(self, count: int = 128) -> list[int]:
        keys = await self._scan_all_masters("pandora:match:{*}", count)
        return _parse_ids_from_keys(keys, "pandora:match:{")

    async def expire_match(self, match_id: int, ttl_sec: float) -> None:
        """终态留存:打上 retention TTL 并移出 active。

        顺带在同一次 CAS 里给 FAILED 记录写 allocation_next_attempt_at_ms = -1。
        那是一个**内部持久哨兵**:「票据/claim 补偿已全部成功」。reconciler 只有
        看到它才敢把 FAILED 从 active 里摘掉 —— 否则一次没做完的补偿会随着索引
        一起消失,票据永远退不回队列。
        """
        key = match_key(match_id)
        try:
            async with self._rdb.pipeline(transaction=True) as pipe:
                await pipe.watch(key)
                raw = await pipe.get(key)
                if raw is None:
                    await pipe.reset()
                else:
                    m = _unmarshal_match(match_id, raw)
                    if m.stage == matchpb.MATCH_STAGE_FAILED:
                        m.allocation_next_attempt_at_ms = -1
                    pipe.multi()
                    if ttl_sec > 0:
                        pipe.set(key, m.SerializeToString(), ex=int(ttl_sec))
                    else:
                        pipe.set(key, m.SerializeToString())
                    await pipe.execute()
        except WatchError:
            # 并发写者已经在改这条记录了;它会自己带上终态语义,这里不覆盖。
            pass
        await self._rdb.zrem(self.active_key, str(match_id))

    async def delete_match(self, match_id: int) -> None:
        """硬删 match 记录并移出 active(对局结算 / 废弃后释放撮合状态)。

        两步都执行(不在前一步失败时 early-return,否则会残留另一半);
        任一步残留均可由 range_expired_matches → get_match miss 自愈。
        """
        zerr: BaseException | None = None
        try:
            await self._rdb.zrem(self.active_key, str(match_id))
        except Exception as exc:  # noqa: BLE001
            zerr = exc
        await self._rdb.delete(match_key(match_id))
        if zerr is not None:
            raise zerr

    # ── 全 master 扫描 ──────────────────────────────────────────────────────

    async def _scan_all_masters(self, pattern: str, count: int) -> list[str]:
        """完整遍历 Redis Cluster 每个 master。

        ★ 不能用 client.scan:它在 Cluster 上只发给单一节点,会**永久漏掉**其他
        slot 上的权威记录。而这个方法的用途正是"从权威记录修复丢失的索引" ——
        漏掉的那些局会永远卡在 ALLOCATING/CONFIRM 没有任何人推进。
        """
        keys: list[str] = []

        async def _scan_one(node) -> None:  # noqa: ANN001
            cursor = 0
            while True:
                cursor, batch = await node.scan(cursor=cursor, match=pattern, count=count)
                keys.extend(k.decode() if isinstance(k, bytes) else str(k) for k in batch)
                if cursor == 0:
                    return

        nodes_fn = getattr(self._rdb, "get_primaries", None)
        if callable(nodes_fn):  # RedisCluster
            for node in nodes_fn():
                await _scan_one(node.redis_connection)
        else:
            await _scan_one(self._rdb)
        return keys


def _parse_ids_from_keys(keys: list[str], prefix: str) -> list[int]:
    seen: set[int] = set()
    out: list[int] = []
    for key in keys:
        if not key.startswith(prefix) or not key.endswith("}"):
            continue
        raw = key[len(prefix) : -1]
        try:
            value = int(raw)
        except ValueError:
            continue
        if value == 0 or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out
