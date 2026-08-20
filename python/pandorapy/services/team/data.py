"""team 服务的数据层 —— 对应 Go 侧 internal/data/team.go。

Redis key 模板(所有业务 ID 用 uint64,十进制格式化):

    pandora:team:{%d}        → protobuf bytes(TeamStorageRecord)
                               hashtag {} 确保同 team 的所有 key 落同一 cluster slot
    pandora:team:player:%d   → string(team_id),TTL 跟随队伍生命周期
    pandora:team:invite:%d   → hash(team_id/target_player_id/inviter_id/expires_at_ms),
                               TTL=invite_ttl(60s)
    pandora:team:invite:target:%d → zset(member=invite_id,score=expires_at_ms),
                               被邀请人维度的 pending 邀请索引:写入侧限流(§9-18)+ 拉取兜底
    pandora:team:apply:{%d}  → zset(member=applicant_id,score=expires_at_ms),
                               队伍维度的 pending 入队申请索引;hashtag 与队伍主体同源
    pandora:team:open:all    → zset(member=team_id,score=索引项过期时刻 ms)
    pandora:team:open:map:%d → zset(同上),按目标关卡分桶

⚠️ open 索引是**非权威投影**(§9.22):唯一权威永远是 pandora:team:{id} 记录本身。
索引只用来"找候选",biz 逐条回权威记录复核后才返回给客户端;索引脏了只会少返几条或
多读几次,永远不会返回一支实际已满/已解散/已开打的队伍。因此索引写失败一律 best-effort
告警,不回滚已提交的队伍状态机迁移。

状态机写用 WATCH/MULTI/EXEC 乐观锁:
    GET(proto bytes) → fn(modify) → MULTI/SET/EXEC
    EXEC 返回 nil(key 被并发修改) → 重试至 max_retry 次 → 返 ErrTeamConcurrent(3007)

★ 三段 Lua 与 Go 侧**逐字节相同**。迁移期两栈并存,同一批 key 会被 Go 副本和 Python
  副本同时读写:脚本差一行(比如少了「已存在则不查上限」那一支)就会让重复申请把
  自己挤掉,而两边都不报错。
"""

from __future__ import annotations

import asyncio
import time

from pandora.team.v1 import team_pb2
from redis.exceptions import WatchError

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import redisx

# invite hash 的字段名(与 Go 逐字同:同一批 hash 会被两栈读写)。
FIELD_TEAM_ID = "team_id"
FIELD_TARGET_PLAYER_ID = "target_player_id"
FIELD_INVITER_ID = "inviter_id"
FIELD_EXPIRES_AT_MS = "expires_at_ms"

OPEN_ALL_KEY = "pandora:team:open:all"


def team_key(team_id: int) -> str:
    """hashtag 括住 team_id 保 cluster slot 一致性。"""
    return f"pandora:team:{{{team_id}}}"


def player_key(player_id: int) -> str:
    return f"pandora:team:player:{player_id}"


def invite_key(invite_id: int) -> str:
    return f"pandora:team:invite:{invite_id}"


def invite_target_key(player_id: int) -> str:
    """被邀请人维度的 pending 邀请索引(zset,member=invite_id,score=expires_at_ms)。"""
    return f"pandora:team:invite:target:{player_id}"


def apply_key(team_id: int) -> str:
    """队伍维度的 pending 入队申请索引。hashtag 内容与 team_key 一致(都是 team_id),
    保证申请索引与队伍主体同 cluster slot。"""
    return f"pandora:team:apply:{{{team_id}}}"


def open_map_key(map_id: int) -> str:
    """按目标关卡分桶的开放队伍索引。map_id=0 也有自己的桶(队长未选图的队伍)。"""
    return f"pandora:team:open:map:{map_id}"


class InviteRecord:
    """邀请令牌的内存表示,对应 Redis hash pandora:team:invite:{invite_id}。

    邀请是短 TTL 小令牌,按 §5.9 保留 hash 不升级为 proto bytes,
    因此用本地类(它不是 proto 存储记录,不叫 StorageRecord)。
    """

    __slots__ = ("invite_id", "team_id", "target_player_id", "inviter_id", "expires_at_ms")

    def __init__(
        self,
        invite_id: int,
        team_id: int,
        target_player_id: int,
        inviter_id: int,
        expires_at_ms: int,
    ) -> None:
        self.invite_id = invite_id
        self.team_id = team_id
        self.target_player_id = target_player_id
        self.inviter_id = inviter_id
        self.expires_at_ms = expires_at_ms


class ApplicationRecord:
    """一份 pending 入队申请,对应 zset 里的一个成员(member=player_id,score=expires_at_ms)。

    为什么不像邀请那样再配一个 hash:申请只需要「谁、到期时刻」两项,zset 成员本身就
    装得下,多一个 hash 只会多一个可漂移的存储(§15.2 能一份就不拆两份)。
    """

    __slots__ = ("player_id", "expires_at_ms")

    def __init__(self, player_id: int, expires_at_ms: int) -> None:
        self.player_id = player_id
        self.expires_at_ms = expires_at_ms


# ── Lua(与 Go 侧逐字节相同,见模块头)────────────────────────────────────────

_DELETE_PLAYER_INDEX = redisx.LuaScript(
    name="team_delete_player_index",
    body="""
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0""",
)

# 在被邀请人 pending 索引(单 key,cluster 安全)上原子完成:
# 清理已过期成员 → 校验上限 → 占位 + 刷新 TTL。返回 1=占位成功,0=已达上限。
_CLAIM_INVITE_SLOT = redisx.LuaScript(
    name="team_claim_invite_slot",
    body="""
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[4]) then
	return 0
end
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return 1""",
)

# 「已存在则不查上限」是幂等的关键:申请人重复点申请只是刷新自己那条的过期时间,
# 不应该因为队伍恰好满 10 条(其中一条就是他自己)而被自己挤掉。
_CLAIM_APPLICATION = redisx.LuaScript(
    name="team_claim_application",
    body="""
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if redis.call('ZSCORE', KEYS[1], ARGV[2]) == false then
	if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[4]) then
		return 0
	end
end
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return 1""",
)

# 原子取走一份申请:存在即删,并按 score 判定是否仍有效。
# 返回 1=取到有效申请,0=不存在或已过期(过期成员同样被删掉,顺手清理)。
_TAKE_APPLICATION = redisx.LuaScript(
    name="team_take_application",
    body="""
local score = redis.call('ZSCORE', KEYS[1], ARGV[2])
if score == false then
	return 0
end
redis.call('ZREM', KEYS[1], ARGV[2])
if tonumber(score) <= tonumber(ARGV[1]) then
	return 0
end
return 1""",
)


def _now_ms() -> int:
    return int(time.time() * 1000)


def marshal_team(team) -> bytes:  # noqa: ANN001
    if team is None:
        raise ValueError("nil team")
    return team.SerializeToString()


def unmarshal_team(team_id: int, payload: bytes):  # noqa: ANN201
    """从 Redis value 反序列化成 TeamStorageRecord。

    ★ **不能 DiscardUnknown**(§9 不变量 17):本函数在 read-modify-write 路径上,
    丢弃 unknown fields 会让旧副本回写时静默抹掉新副本刚写进去的新字段。
    protobuf-python 的 ParseFromString 默认保留 unknown fields,不要去关它。
    """
    rec = team_pb2.TeamStorageRecord()
    try:
        rec.ParseFromString(payload)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"team {team_id} bad proto: {exc}") from exc
    if rec.team_id == 0:
        rec.team_id = team_id
    if rec.team_id != team_id:
        raise ValueError(f"team {team_id} id mismatch: {rec.team_id}")
    return rec


class RedisTeamRepo:
    """基于 redis-py asyncio 的 TeamRepo 实现。biz 层只依赖本类的方法名。"""

    __slots__ = ("_rdb",)

    def __init__(self, rdb) -> None:  # noqa: ANN001
        self._rdb = rdb

    # ── 队伍主体 ────────────────────────────────────────────────────────────

    async def get(self, team_id: int):  # noqa: ANN201
        """读取队伍。not found 时返回 (None, False)(**不报错**)。"""
        b = await self._rdb.get(team_key(team_id))
        if b is None:
            return None, False
        return unmarshal_team(team_id, b), True

    async def create(self, team, team_ttl_sec: float) -> None:  # noqa: ANN001
        """创建队伍:**仅**写 team protobuf value + TTL。

        player 归属由上层 claim_player(SETNX)独立保证(不变量 §1),不在此处写
        player index —— 在这里写会覆盖已声明的 claim。
        """
        await self._rdb.set(
            team_key(team.team_id), marshal_team(team), px=int(team_ttl_sec * 1000)
        )

    async def update_with_lock(
        self,
        team_id: int,
        max_retry: int,
        fn,  # noqa: ANN001
        team_ttl_sec: float,
    ) -> None:
        """WATCH/MULTI/EXEC 读-改-写。

          1. WATCH team key
          2. GET → proto 反序列化
          3. 调 fn(team) —— fn 抛异常则放弃本次写并原样冒泡(**不重试**)
          4. MULTI → SET(value+TTL) → EXEC
          5. EXEC 冲突 → 重试,耗尽返 ErrTeamConcurrent(3007)

        ★ fn 抛的异常必须与 redis / WatchError 分开处理,对应 Go 的
        `txErr == fnErr` 那一支。混在一起有两个后果:被冲突分支吞掉会变成
        「重试三次后 CAS 耗尽」(线上看到的原因是错的);被 store 分支接住会给
        `errReadyNoChange` 这种**正常**哨兵刷一条 team_update_store_failed 的假故障。
        """
        key = team_key(team_id)
        started = time.monotonic()
        conflicts = 0

        for attempt in range(max_retry + 1):
            # fn_raised 就是 Go 的 fnErr:标记「本轮异常来自业务回调而非存储」。
            fn_raised = False
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    b = await pipe.get(key)
                    if b is None:
                        fn_raised = True
                        raise errcode.PandoraError(
                            errcode.ErrTeamNotFound, "team %d not found", team_id
                        )
                    team = unmarshal_team(team_id, b)
                    try:
                        result = fn(team)
                        if result is not None and hasattr(result, "__await__"):
                            await result
                    except BaseException:
                        fn_raised = True
                        raise
                    payload = marshal_team(team)
                    pipe.multi()
                    pipe.set(key, payload, px=int(team_ttl_sec * 1000))
                    await pipe.execute()
            except WatchError:
                conflicts += 1
                continue
            except BaseException as exc:  # noqa: BLE001
                if fn_raised:
                    raise  # 业务回调的判定 / 哨兵 —— 不重试、不打存储故障日志
                if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt)):
                    raise
                # 透传的是裸 redis / proto 错误(非 errcode),上层只能把它归到
                # store_write_failed 一大类。不在这里留一条带 team_id 的痕,
                # 就完全分不出是连不上 redis、proto 坏了、还是 MULTI 写失败。
                plog.get().warning(
                    "team_update_store_failed",
                    reason="store_write_failed",
                    team_id=team_id,
                    attempt=attempt,
                    conflicts=conflicts,
                    elapsed_ms=int((time.monotonic() - started) * 1000),
                    err=str(exc),
                )
                raise
            else:
                if conflicts > 0:
                    # 并发重试后成功是常态(热点队伍),Debug 即可;但它是「写变慢了」
                    # 与下面那条耗尽 WARN 之间的唯一中间证据。
                    plog.get().debug(
                        "team_update_lock_retried",
                        team_id=team_id,
                        conflicts=conflicts,
                        max_retry=max_retry,
                        elapsed_ms=int((time.monotonic() - started) * 1000),
                    )
                return

        # 乐观锁重试耗尽(§16 TOCTOU / 重试耗尽盲点):某热点队伍被并发写打爆。
        # 经 in-band 码返回被 access log 记成泛化失败(无 team_id),故此处 WARN 留证。
        # reason 与 biz 层同一套枚举取值(§11.3 R2),data 不反向依赖 biz,
        # 所以这里是同值字面量,两侧注释互指。
        plog.get().warning(
            "team_update_lock_exhausted",
            reason="optimistic_retry_exhausted",
            team_id=team_id,
            max_retry=max_retry,
            conflicts=conflicts,
            elapsed_ms=int((time.monotonic() - started) * 1000),
            hint="热点队伍被并发写打爆;调用方会收到 3007,属暂态",
        )
        raise errcode.PandoraError(
            errcode.ErrTeamConcurrent,
            "team %d update concurrent retry exhausted",
            team_id,
        )

    async def delete_team(self, team_id: int) -> None:
        """删队伍主体 key。**仅**供 create_team 声明失败时回滚自己刚写的主体:
        team_id 是 Snowflake 新发、返回前仅创建者可见,无条件 DEL 安全。"""
        await self._rdb.delete(team_key(team_id))

    async def expire_team(self, team_id: int, ttl_sec: float) -> None:
        """单独刷新 team key 的 TTL(不读改写 value),供解散后改短 TTL 用。"""
        await self._rdb.pexpire(team_key(team_id), int(ttl_sec * 1000))

    async def touch_team(self, team_id: int, player_id: int, ttl_sec: float) -> None:
        """一次 pipeline 同时刷新队伍 key + 玩家索引 key 的 TTL(在线心跳保活)。"""
        pipe = self._rdb.pipeline(transaction=False)
        pipe.pexpire(team_key(team_id), int(ttl_sec * 1000))
        pipe.pexpire(player_key(player_id), int(ttl_sec * 1000))
        await pipe.execute()

    # ── player 归属索引 ─────────────────────────────────────────────────────

    async def get_player_team_id(self, player_id: int) -> tuple[int, bool]:
        val = await self._rdb.get(player_key(player_id))
        if val is None:
            return 0, False
        return int(val), True

    async def set_player_index(
        self, player_id: int, team_id: int, ttl_sec: float
    ) -> None:
        await self._rdb.set(
            player_key(player_id), str(team_id), px=int(ttl_sec * 1000)
        )

    async def claim_player(
        self, player_id: int, team_id: int, ttl_sec: float
    ) -> tuple[int, bool]:
        """SETNX 原子声明 player→team 归属(不变量 §1:一人只能在一个队)。

        声明成功返 (team_id, True);玩家已属其他队伍返 (existing_team_id, False)。

        ★ 最多两次:首次 SETNX 失败后若发现占用者**刚好过期**(GET 返回 None)再抢一次。
        这个循环**只在 claim_player_healing_orphan 内部经由本方法使用** —— create_team /
        join_team 不直连它(别照着"看起来该有"去加第三处调用)。
        """
        key = player_key(player_id)
        val = str(team_id)
        for _attempt in range(2):
            ok = await self._rdb.set(key, val, px=int(ttl_sec * 1000), nx=True)
            if ok:
                return team_id, True
            cur = await self._rdb.get(key)
            if cur is None:
                continue  # 占用者刚好过期,重试一次 SETNX
            return int(cur), False
        raise errcode.PandoraError(
            errcode.ErrTeamConcurrent, "claim player %d concurrent", player_id
        )

    async def delete_player_index_if_matches(
        self, player_id: int, team_id: int
    ) -> None:
        """仅当索引当前值仍指向 team_id 时才 DEL(原子比较)。

        biz 层所有索引清理路径一律用它:无条件 DEL 存在「读旧索引 → 玩家并发声明新归属
        → 误删新 claim」的窗口(镜像 matchmaker DeletePlayerIndexIfMatches 的结论)。
        """
        await _DELETE_PLAYER_INDEX(
            self._rdb, keys=[player_key(player_id)], args=[str(team_id)]
        )

    async def delete_player_index(self, player_id: int) -> None:
        """无条件删除。**不在 biz 的清理路径上**(那里一律走 CAS 版),仅供测试造数据。"""
        await self._rdb.delete(player_key(player_id))

    async def scan_player_index(
        self, cursor: int, count: int
    ) -> tuple[list[int], int]:
        """增量扫描「有队伍的玩家」全集,供 offlinewatch 的兜底候选源。

        用 SCAN 而不是另建一个活跃玩家集合:这份索引**本来就是**「有队伍的玩家」全集,
        再维护一个平行集合就是 §9.22 的重复影子状态(还要在建队/离队/解散/TTL 过期
        四处保持一致,漏一处就漂移)。
        """
        try:
            next_cursor, keys = await self._rdb.scan(
                cursor=cursor, match="pandora:team:player:*", count=count
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "redis scan player index: %s", exc
            ) from exc
        ids: list[int] = []
        for k in keys:
            key = k.decode() if isinstance(k, bytes) else str(k)
            idx = key.rfind(":")
            if idx < 0 or idx + 1 >= len(key):
                continue
            try:
                pid = int(key[idx + 1 :])
            except ValueError:
                continue  # 脏 key 跳过,不让一条坏数据卡住整轮兜底
            if pid == 0:
                continue
            ids.append(pid)
        return ids, int(next_cursor)

    # ── 邀请 ────────────────────────────────────────────────────────────────

    async def set_invite(
        self,
        invite_id: int,
        team_id: int,
        inviter_id: int,
        target_player_id: int,
        ttl_sec: float,
        max_pending: int,
    ) -> None:
        """存邀请令牌 + 记入被邀请人 pending 索引。

        1. 先在 pending 索引上原子「限流 + 占位」(§9-18 写入侧上限)。
           索引与令牌 hash 分属不同 cluster slot,不能跨 key 原子;**先占位后写 hash**:
           hash 写失败时占位残留至多 TTL(60s)且 list/get 都会跳过,自愈,方向安全
           (反过来先写 hash 会出现「令牌可 Accept 但不受限流管控」的超限窗口)。
        2. 写令牌 hash(权威)。失败则 best-effort 回收占位,避免白占配额 60s。
        """
        now = _now_ms()
        ttl_ms = int(ttl_sec * 1000)
        expires_at_ms = now + ttl_ms

        ok = await _CLAIM_INVITE_SLOT(
            self._rdb,
            keys=[invite_target_key(target_player_id)],
            args=[now, str(invite_id), expires_at_ms, max_pending, ttl_ms],
        )
        if int(ok) == 0:
            # R2 + §9.18:写入侧总量上限拒绝。必须带**当前 pending 数与上限**,
            # 否则玩家报「收不到邀请」时分不出是真满了还是过期成员没被清。
            plog.get().warning(
                "team_invite_slot_rejected",
                reason="invite_pending_limit_reached",
                team_id=team_id,
                inviter_id=inviter_id,
                target_player_id=target_player_id,
                invite_id=invite_id,
                pending=await self._pending_count(invite_target_key(target_player_id)),
                max_pending=max_pending,
                hint="计数不含已过期成员(写入前已 ZREMRANGEBYSCORE 清过)",
            )
            raise errcode.PandoraError(
                errcode.ErrTeamInvitePendingLimit,
                "player %d has too many pending invites (max %d)",
                target_player_id,
                max_pending,
            )

        key = invite_key(invite_id)
        try:
            pipe = self._rdb.pipeline(transaction=True)
            pipe.hset(
                key,
                mapping={
                    FIELD_TEAM_ID: str(team_id),
                    FIELD_TARGET_PLAYER_ID: str(target_player_id),
                    FIELD_INVITER_ID: str(inviter_id),
                    FIELD_EXPIRES_AT_MS: str(expires_at_ms),
                },
            )
            pipe.pexpire(key, ttl_ms)
            await pipe.execute()
        except Exception:
            await self._rdb.zrem(
                invite_target_key(target_player_id), str(invite_id)
            )
            raise

    async def get_invite(self, invite_id: int):  # noqa: ANN201
        fields = await self._rdb.hgetall(invite_key(invite_id))
        if not fields:
            return None, False
        return _invite_from_hash(invite_id, _decode_map(fields)), True

    async def delete_invite(self, invite_id: int, target_player_id: int) -> None:
        """删邀请令牌 + 从被邀请人 pending 索引移除,释放配额。

        两个 key 分属不同 cluster slot,顺序两条命令(非原子):**hash 先删**保证令牌
        立即失效;索引 ZREM 失败只影响配额释放,由 list/set_invite 的过期清理兜底。
        """
        await self._rdb.delete(invite_key(invite_id))
        await self._rdb.zrem(invite_target_key(target_player_id), str(invite_id))

    async def list_pending_invites(
        self, target_player_id: int, limit: int
    ) -> list[InviteRecord]:
        """列出发给 target 的未过期 pending 邀请(拉取兜底,§9-22 唯一权威查询)。"""
        if limit <= 0:
            return []
        idx_key = invite_target_key(target_player_id)
        now = _now_ms()

        await self._rdb.zremrangebyscore(idx_key, "-inf", now)
        ids = await self._rdb.zrangebyscore(
            idx_key, "-inf", "+inf", start=0, num=limit
        )
        if not ids:
            return []

        # 逐条读令牌 hash(权威)。hash 已没(已接受 / TTL 竞态)→ 跳过并顺手清索引残留。
        pipe = self._rdb.pipeline(transaction=False)
        parsed_ids: list[int] = []
        for raw in ids:
            s = raw.decode() if isinstance(raw, bytes) else str(raw)
            try:
                iid = int(s)
            except ValueError as exc:
                raise ValueError(
                    f"invite index of player {target_player_id} bad member {s!r}"
                ) from exc
            parsed_ids.append(iid)
            pipe.hgetall(invite_key(iid))
        results = await pipe.execute()

        invites: list[InviteRecord] = []
        stale: list[str] = []
        for iid, fields in zip(parsed_ids, results, strict=True):
            if not fields:
                stale.append(str(iid))
                continue
            invites.append(_invite_from_hash(iid, _decode_map(fields)))
        if stale:
            # best-effort:残留成员本来也会随 score 过期被清,失败无碍。
            await self._rdb.zrem(idx_key, *stale)
        return invites

    # ── 入队申请 ────────────────────────────────────────────────────────────

    async def claim_application(
        self, team_id: int, applicant_id: int, ttl_sec: float, max_pending: int
    ) -> int:
        """写入或刷新一份 pending 入队申请,返回过期时刻(unix ms)。

        清理过期成员 → 校验上限 → 写入/刷新 在同一段 Lua 内原子完成(单 key,
        cluster 安全),无 TOCTOU。重复申请同一队伍幂等:成员已存在时只刷新 score。
        """
        now = _now_ms()
        ttl_ms = int(ttl_sec * 1000)
        expires_at_ms = now + ttl_ms

        ok = await _CLAIM_APPLICATION(
            self._rdb,
            keys=[apply_key(team_id)],
            args=[now, str(applicant_id), expires_at_ms, max_pending, ttl_ms],
        )
        if int(ok) == 0:
            # R2 + §9.18:3009 不是 IsServerFault,access log 只会记成 rpc_ok=DEBUG,
            # 线上不可见;玩家报「申请不了」时,只有 pending/max 同框才分得出是真满了
            # 还是清理没跑。
            plog.get().warning(
                "team_application_slot_rejected",
                reason="application_pending_limit_reached",
                team_id=team_id,
                player_id=applicant_id,
                pending=await self._pending_count(apply_key(team_id)),
                max_pending=max_pending,
                ttl_ms=ttl_ms,
                hint="重复申请同一队伍不占新名额;计数不含已过期成员",
            )
            raise errcode.PandoraError(
                errcode.ErrTeamApplyPendingLimit,
                "team %d has too many pending applications (max %d)",
                team_id,
                max_pending,
            )
        return expires_at_ms

    async def take_application(self, team_id: int, applicant_id: int) -> bool:
        """原子取走一份申请(校验未过期 + 删除),返回是否取到**有效**申请。

        队长审批(同意/拒绝)与申请人重复提交之间靠本操作的原子性定序:先取到的才算数。
        """
        taken = await _TAKE_APPLICATION(
            self._rdb, keys=[apply_key(team_id)], args=[_now_ms(), str(applicant_id)]
        )
        return int(taken) == 1

    async def list_applications(
        self, team_id: int, limit: int
    ) -> list[ApplicationRecord]:
        if limit <= 0:
            return []
        key = apply_key(team_id)
        now = _now_ms()
        await self._rdb.zremrangebyscore(key, "-inf", now)
        items = await self._rdb.zrangebyscore(
            key, "-inf", "+inf", start=0, num=limit, withscores=True
        )
        out: list[ApplicationRecord] = []
        for member, score in items:
            s = member.decode() if isinstance(member, bytes) else str(member)
            try:
                pid = int(s)
            except ValueError as exc:
                raise ValueError(
                    f"team {team_id} application index bad member {s!r}"
                ) from exc
            out.append(ApplicationRecord(pid, int(score)))
        return out

    async def delete_applications(self, team_id: int) -> None:
        """删整支队伍的申请索引(解散时清理)。best-effort:失败无碍,索引自带 TTL。"""
        await self._rdb.delete(apply_key(team_id))

    # ── 开放招募队伍索引(非权威投影) ──────────────────────────────────────

    async def sync_open_team(
        self,
        team_id: int,
        map_id: int,
        prev_map_id: int,
        open_for_recruit: bool,
        expires_at_ms: int,
        ttl_sec: float,
    ) -> None:
        """同步队伍在「开放招募」索引中的存在性。

        prev_map_id != map_id 时(队长换了目标关卡),无论开不开放都先从旧分桶移除 ——
        否则同一支队伍会同时挂在两个 map 桶下。

        跨 slot 多 key,**不做原子保证**:索引是非权威投影,部分失败最坏只是多/少几条
        候选,由 score 过期与读取侧复核收敛。
        """
        member = str(team_id)
        ttl_ms = int(ttl_sec * 1000)

        if prev_map_id != map_id:
            await self._rdb.zrem(open_map_key(prev_map_id), member)

        if not open_for_recruit:
            await self._rdb.zrem(OPEN_ALL_KEY, member)
            await self._rdb.zrem(open_map_key(map_id), member)
            return

        await self._rdb.zadd(OPEN_ALL_KEY, {member: expires_at_ms})
        await self._rdb.pexpire(OPEN_ALL_KEY, ttl_ms)
        await self._rdb.zadd(open_map_key(map_id), {member: expires_at_ms})
        await self._rdb.pexpire(open_map_key(map_id), ttl_ms)

    async def list_open_team_ids(self, map_id: int, limit: int) -> list[int]:
        """惰性清理过期成员后,按最近活跃(score 降序)返回至多 limit 个**候选** team_id。

        返回的只是候选,调用方必须逐条回权威队伍记录复核(见模块头)。
        """
        if limit <= 0:
            return []
        key = OPEN_ALL_KEY if map_id <= 0 else open_map_key(map_id)
        now = _now_ms()
        # score = 索引项最晚存活时刻;已过期 = 对应队伍 key 也早该没了,直接清掉。
        await self._rdb.zremrangebyscore(key, "-inf", now)
        # score 降序 = 最近一次写入(最近活跃)在前,让玩家优先看到还在动的队伍。
        members = await self._rdb.zrevrangebyscore(
            key, "+inf", "-inf", start=0, num=limit
        )
        out: list[int] = []
        for m in members:
            s = m.decode() if isinstance(m, bytes) else str(m)
            try:
                out.append(int(s))
            except ValueError as exc:
                raise ValueError(f"open team index {key!r} bad member {s!r}") from exc
        return out

    async def remove_open_team_candidate(self, team_id: int, map_id: int) -> None:
        """从索引里剔除一个复核不通过的候选。best-effort 自愈,失败无碍(score 过期兜底)。"""
        member = str(team_id)
        await self._rdb.zrem(OPEN_ALL_KEY, member)
        await self._rdb.zrem(open_map_key(map_id), member)

    # ── 诊断辅助 ────────────────────────────────────────────────────────────

    async def _pending_count(self, key: str) -> int:
        """读一个 pending 索引 zset 的成员数,**只用于上限拒绝那一条日志**。

        不让 Lua 顺带返回:改脚本返回值会动到已被测试与调用方依赖的契约(1/0),
        而这里只在**已经确定要拒**的分支上多读一次 ZCARD —— 不改控制流、不改返回值、
        不改错误码,失败也只是把日志里的数字记成 -1。
        """
        try:
            return int(await self._rdb.zcard(key))
        except Exception:  # noqa: BLE001
            return -1


def _decode_map(fields: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in fields.items():
        key = k.decode() if isinstance(k, bytes) else str(k)
        val = v.decode() if isinstance(v, bytes) else str(v)
        out[key] = val
    return out


def _invite_from_hash(invite_id: int, fields: dict[str, str]) -> InviteRecord:
    """把 invite hash 字段解析成 InviteRecord。

    inviter_id / expires_at_ms 缺失(旧版本写入的记录)按 0 处理不报错:
    记录只活 60s,自然换代。**team_id / target_player_id 缺失则报错** ——
    那两个是判定归属的依据,猜一个 0 会让 AcceptInvite 的校验形同虚设。
    """
    try:
        team_id = int(fields[FIELD_TEAM_ID])
    except (KeyError, ValueError) as exc:
        raise ValueError(f"invite {invite_id} bad team_id") from exc
    try:
        target_player_id = int(fields[FIELD_TARGET_PLAYER_ID])
    except (KeyError, ValueError) as exc:
        raise ValueError(f"invite {invite_id} bad target_player_id") from exc
    try:
        inviter_id = int(fields.get(FIELD_INVITER_ID, "0"))
    except ValueError:
        inviter_id = 0
    try:
        expires_at_ms = int(fields.get(FIELD_EXPIRES_AT_MS, "0"))
    except ValueError:
        expires_at_ms = 0
    return InviteRecord(
        invite_id, team_id, target_player_id, inviter_id, expires_at_ms
    )


