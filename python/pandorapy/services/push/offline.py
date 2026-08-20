"""push 投递缓冲 —— 对应 Go 侧 internal/data/offline.go。

Redis 单 key 是**唯一定序与投递权威**(2026-07-22 审计 v2):

    key   = pandora:push:offline:<player_id>   (ZSET)
    score = **投递游标**
    哨兵 wm = 最后分配的游标(基线,修剪/重启不丢)
    哨兵 fl = 最高被修剪掉的游标(gap 判定 / resync 信号)

★ 游标基线用**服务端 now 而不是 kafka ts**(审计 R4 P1-1)。理由不是"更准",
  而是同一个 Lua 里紧接着要做窗口修剪:
      拿 kafka ts 当游标 → 重投/积压帧的原始 ts 可能早于保留窗下界
      → 修剪把**刚写入的帧连同哨兵一起删掉** → 随后 ack = 静默丢帧
  用 now 则写入的帧必然存活到本次修剪之后。
  副产品:不再信任任何外部时间戳,远未来 ts 污染游标一并消除
  (游标量级恒 ≈ 墙钟毫秒 ~2^41,远低于 Lua double 的 2^53 精度界)。

★ 单 Lua = 游标分配与入缓冲**不可分割**。
  任意多个 Pod / topic consumer 并发写同一玩家时,缓冲内容恒等于"已分配游标的全集" ——
  不存在"游标 C+1 可见而更早分配的 C 不可见"的窗口,于是 Range(>X) 永远返回
  X 之后的**完整前缀**。跨 Pod 顺序由 Redis 单点定序,**不依赖进程内锁**
  (进程内玩家锁只在单 Pod 内成立,多 Pod 时形同虚设)。

★ 交付语义是 **at-least-once,不承诺不重**(诚实契约)。
  kafka 重投 / redis 结果不确定时的重试会给同一业务事件分配新游标 →
  客户端可能重复收到。游标保证的是**不漏**与**每玩家全序**,不是 exactly-once。
  业务事件必须幂等或按业务 ID 判重(chat 有 message_id;状态类推送天然幂等)。
"""

from __future__ import annotations

import dataclasses

from pandora.push.v1 import push_pb2
from redis.asyncio.client import Redis

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import redisx

# 哨兵 member 名。它们参与 score 排序但不是帧,修剪时必须跳过。
SENTINEL_WATERMARK = "wm"  # 最后分配的游标
SENTINEL_FLOOR = "fl"  # 最高被修剪掉的游标

# member 格式:%020d 游标前缀 + 0x1f 分隔 + protobuf payload。
# 前缀保证同一游标下 member 唯一(ZSET 按 member 去重,score 相同的两帧不能互相覆盖)。
MEMBER_SEP = "\x1f"
_SEP_BYTE = ord(MEMBER_SEP)   # 按字节比较,避免 bytes 索引取到 int 时与 str 比不上
CURSOR_PREFIX_WIDTH = 20


def offline_key(player_id: int) -> str:
    """与 Go 侧逐字一致。"""
    return f"pandora:push:offline:{player_id}"


# ★ 从 Go 侧**原样搬来,一个字符没改**。
#
# 这段 Lua 是整个投递语义的地基:游标分配、入缓冲、窗口修剪、条数修剪、
# 哨兵维护五件事在**同一次 Redis 执行**里完成。任何拆分都会打开一个窗口,
# 让并发写者看到"部分已分配的游标"。
_ASSIGN_AND_BUFFER = redisx.LuaScript(
    name="push_assign_and_buffer",
    body="""
local base = 0
local wmScore = redis.call('ZSCORE', KEYS[1], 'wm')
if wmScore then base = tonumber(wmScore) end
local top = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if top[2] and tonumber(top[2]) > base then base = tonumber(top[2]) end
local cursor = base + 1
local now = tonumber(ARGV[1])
if now > cursor then cursor = now end
local member = string.format('%020d', cursor) .. string.char(31) .. ARGV[2]
redis.call('ZADD', KEYS[1], cursor, member)
redis.call('ZADD', KEYS[1], cursor, 'wm')
local fl = 0
local flScore = redis.call('ZSCORE', KEYS[1], 'fl')
if flScore then fl = tonumber(flScore) end
-- 窗口修剪:score < now-ttl 的帧删除,记录最高被删游标
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', '(' .. ARGV[3], 'WITHSCORES')
for i = 1, #expired, 2 do
  local v = expired[i]
  if v ~= 'wm' and v ~= 'fl' then
    redis.call('ZREM', KEYS[1], v)
    local s = tonumber(expired[i + 1])
    if s > fl then fl = s end
  end
end
-- 条数修剪:帧数(去掉哨兵)超上限时从最旧删起,同样记录最高被删游标
local sentinels = 1
if redis.call('ZSCORE', KEYS[1], 'fl') then sentinels = 2 end
local excess = redis.call('ZCARD', KEYS[1]) - sentinels - tonumber(ARGV[4])
if excess > 0 then
  local victims = redis.call('ZRANGE', KEYS[1], 0, excess + 1, 'WITHSCORES')
  local removed = 0
  for i = 1, #victims, 2 do
    if removed >= excess then break end
    local v = victims[i]
    if v ~= 'wm' and v ~= 'fl' then
      redis.call('ZREM', KEYS[1], v)
      local s = tonumber(victims[i + 1])
      if s > fl then fl = s end
      removed = removed + 1
    end
  end
end
if fl > 0 then redis.call('ZADD', KEYS[1], fl, 'fl') end
redis.call('EXPIRE', KEYS[1], ARGV[5])
return cursor
""",
)


# ★ 同样从 Go 侧 markCorruptScript **原样搬来**(services/runtime/push/internal/data/offline.go)。
# 单 key 原子「删坏 member + fl 哨兵单调抬升到坏帧最高游标」。
# ARGV[1]=坏帧最高游标,ARGV[2..]=坏 member 原文。
#
# fl 只增不减:多个 Pod 各自扫到不同批坏帧时取 max —— 若写成无条件覆盖,后扫到
# 较低坏帧的那个 Pod 会把丢失上界**回退**,已经报给客户端的 resync 上界之下的
# 帧就再也不会被判为丢失。
_MARK_CORRUPT = redisx.LuaScript(
    name="push_mark_corrupt",
    body="""
local top = tonumber(ARGV[1])
for i = 2, #ARGV do
  redis.call('ZREM', KEYS[1], ARGV[i])
end
local fl = 0
local flScore = redis.call('ZSCORE', KEYS[1], 'fl')
if flScore then fl = tonumber(flScore) end
if top > fl then redis.call('ZADD', KEYS[1], top, 'fl') end
return top
""",
)


@dataclasses.dataclass(slots=True)
class OfflineFrame:
    frame: object
    cursor: int


class RedisOfflineCache:
    """投递缓冲。对应 Go 的 data.OfflineCacheRepo。"""

    __slots__ = ("_rdb", "_ttl_sec", "_max_frames", "_key_ttl_sec")

    def __init__(
        self,
        rdb: Redis,
        *,
        retention_sec: int = 300,
        max_frames: int = 500,
        key_ttl_sec: int = 7 * 24 * 3600,
    ) -> None:
        self._rdb = rdb
        self._ttl_sec = retention_sec  # 保留窗(默认 5min)
        self._max_frames = max_frames  # 条数上限(§9.18)
        self._key_ttl_sec = key_ttl_sec  # key TTL ≥ 客户端游标寿命量级

    async def assign_and_buffer(self, player_id: int, frame, now_ms: int) -> int:
        """单 Lua 原子:分配游标 + 入缓冲。返回分配的游标。

        ★ 失败必须让调用方**拒绝 ack kafka** —— 缓冲是交付权威,跳过就是丢帧。
        """
        payload = frame.SerializeToString()
        floor_ms = now_ms - self._ttl_sec * 1000
        try:
            cursor = await _ASSIGN_AND_BUFFER(
                self._rdb,
                keys=[offline_key(player_id)],
                args=[now_ms, payload, floor_ms, self._max_frames, self._key_ttl_sec],
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "assign push cursor for %d: %s", player_id, exc
            ) from exc
        return int(cursor)

    async def range_after(
        self, player_id: int, after_cursor: int, now_ms: int, max_frames: int = 0
    ) -> list[OfflineFrame]:
        """拉 score **严格大于** after_cursor 且不早于保留窗下界的帧(升序)。

        ★ 读侧下界与写侧修剪**同界**(now - ttl):否则读结果会随"最近有没有写触发
        修剪"漂移 —— 同一个 after_cursor 在两次调用间返回不同的帧集,而且没有任何
        错误可观测。
        """
        floor_ms = now_ms - self._ttl_sec * 1000
        lo = max(after_cursor, floor_ms - 1)
        limit = max_frames if max_frames > 0 else self._max_frames
        raw = await self._rdb.zrangebyscore(
            offline_key(player_id),
            f"({lo}",
            "+inf",
            start=0,
            num=limit + 2,  # +2 给两个哨兵留位
            withscores=True,
        )
        out: list[OfflineFrame] = []
        # 坏 member 记账(Go 侧 R5 复审 P2-1 / R7 P1 的收口,逐条对齐)。
        # ★ 坏帧**不能只是跳过**:它上面的好帧一旦交付,客户端游标就越过了坏帧,
        #   而 Range 严格 >after_cursor —— 坏 member 永远不会再被扫到,于是既没有
        #   fl 记账也没有 resync,变成永久静默漏报(投递契约里最坏的一种失败)。
        corrupt_top = 0
        corrupt_min = -1
        corrupt_members: list[object] = []

        def _mark_corrupt(score_ms: int, member_raw) -> None:  # noqa: ANN001
            nonlocal corrupt_top, corrupt_min
            if score_ms > corrupt_top:
                corrupt_top = score_ms
            if corrupt_min < 0 or score_ms < corrupt_min:
                corrupt_min = score_ms
            corrupt_members.append(member_raw)

        for member, score in raw:
            text = member.decode("utf-8", "surrogateescape") if isinstance(member, bytes) else member
            if text in (SENTINEL_WATERMARK, SENTINEL_FLOOR):
                continue
            parsed = _parse_member(member)
            if parsed is None:
                # 格式不识别(dev 旧格式残留 / 外部脏写):按坏帧记账,不阻断其它帧
                # (一条脏数据不该拖死整次补推),但必须留痕 + 折进 fl 触发 resync。
                # ⚠️ 这不是"兼容手段" —— 上线后演进 member 格式必须按 §9.17
                # 双向兼容纪律另行设计,不得复用静默跳过。
                _mark_corrupt(int(score), member)
                continue
            cursor = int(score)
            # ★ 把 frame.ts_ms **重铸为投递游标**再交付。
            # 客户端用它推进恢复游标;若交付原始 kafka ts,客户端游标会与服务端
            # 游标体系脱节(补推跳过 / 重复)。
            parsed.ts_ms = cursor
            out.append(OfflineFrame(frame=parsed, cursor=cursor))
            if len(out) >= limit:
                break

        if corrupt_members:
            plog.get().error(
                "push_offline_corrupt_members",
                player_id=player_id,
                count=len(corrupt_members),
                top_cursor=corrupt_top,
            )
            try:
                await _MARK_CORRUPT(
                    self._rdb,
                    keys=[offline_key(player_id)],
                    args=[corrupt_top, *corrupt_members],
                )
            except Exception as exc:  # noqa: BLE001
                # ★ fl 抬升失败时**绝不交付坏帧之上的好帧**:交付了客户端游标就越过
                # 坏帧,而这次丢失既没记进 fl 也没发 resync = 永久静默漏报。
                # 截到最低坏帧之下,坏 member 仍留在 key 里,下一轮重扫重试记账 ——
                # 宁可这一轮少推几帧(补推本就会重来),也不能让游标越过未记账的丢失。
                plog.get().warning(
                    "push_offline_corrupt_cleanup_failed",
                    player_id=player_id,
                    err=str(exc),
                    withheld_above=corrupt_min,
                )
                out = [f for f in out if f.cursor < corrupt_min]
        return out

    async def lost_since(self, player_id: int, after_cursor: int, now_ms: int) -> int:
        """返回从 after_cursor 续传时**确定分配过但不再可交付**的最高游标(0 = 无丢失)。

        > after_cursor 时补推无法闭合,调用方必须向客户端发 resync 信号
        并把本地游标跳到该上界 —— 否则客户端会永远等一段永远不会到的帧。
        """
        # 来源 ①:被修剪掉的(fl 哨兵,写侧 Lua 维护)。
        floor_score = await self._rdb.zscore(offline_key(player_id), SENTINEL_FLOOR)
        lost = int(floor_score) if floor_score else 0

        # 来源 ②:**仍在 ZSET 里但已滑出读侧保留窗**的帧 —— 它们没被删,
        # 但 range_after 的下界读不到,对客户端同样是不可交付。
        #
        # ⚠️ 这里**不能**直接把 window_floor 当成丢失上界(我第一版就是这么写的,
        # 被测试当场抓住):什么都没修剪、所有帧都在窗内时,window_floor 本身
        # 是个大于 0 的数,于是每次都报告"丢了",客户端每次连上都被迫全量 resync。
        # 必须实际查一下窗下**有没有帧**。
        window_floor = now_ms - self._ttl_sec * 1000
        below = await self._rdb.zrevrangebyscore(
            offline_key(player_id),
            f"({window_floor}",
            "-inf",
            start=0,
            num=3,  # 取几条,跳过可能混在里面的哨兵
            withscores=True,
        )
        for member, score in below:
            text = (
                member.decode("utf-8", "surrogateescape")
                if isinstance(member, bytes)
                else str(member)
            )
            if text in (SENTINEL_WATERMARK, SENTINEL_FLOOR):
                continue
            lost = max(lost, int(score))
            break  # zrevrangebyscore 已降序,第一条非哨兵就是最高的

        return lost if lost > after_cursor else 0


def _parse_member(member) -> object | None:  # noqa: ANN001
    """拆 `%020d` + 0x1f + payload。格式不识别返回 None。"""
    raw = member if isinstance(member, bytes) else str(member).encode("utf-8", "surrogateescape")
    if len(raw) <= CURSOR_PREFIX_WIDTH or raw[CURSOR_PREFIX_WIDTH] != _SEP_BYTE:
        return None
    # ★ 前 20 字节必须**全是数字**(Go decodeMember 的 digits 循环,offline.go:135-141)。
    # 只看分隔符位置不够:proto3 反序列化极宽松,连**空 payload 都能解成功**,
    # 于是 `<20 字节垃圾>\x1f` 这种脏写会被当好帧投出去 —— Go 判它是坏帧(删除+折账),
    # Python 却投一条 ts_ms=游标的空 PushFrame,两侧对同一条脏数据的判定正相反。
    if not raw[:CURSOR_PREFIX_WIDTH].isdigit():
        return None
    sep = CURSOR_PREFIX_WIDTH
    frame = push_pb2.PushFrame()
    try:
        frame.ParseFromString(raw[sep + 1 :])
    except Exception:  # noqa: BLE001
        return None
    return frame
