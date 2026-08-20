"""Redis ZSET 排行榜:实时排名的权威计算层 —— 对应 Go 侧 internal/data/board_store.go。

key(同一 board 用 hashtag 锁同一 Redis Cluster slot,submit 的 Lua 同时碰全部 key,避免 CROSSSLOT):

    pandora:lb:{<board>}:z   ZSET   member=entity_id,score=packed(时间 tie-break 打包;max_size 截断只保留 Top-N)
    pandora:lb:{<board>}:t   HASH   entity_id → updated_at_ms(只保留榜内成员,随截断清理)
    pandora:lb:{<board>}:m   HASH   榜元信息(asc / tie / bw 桶宽,建榜时定死)
    pandora:lb:{<board>}:s   HASH   entity_id → 真实分(全员,不随截断清理)
    pandora:lb:{<board>}:h   HASH   bucket → count 分数直方图(全员;榜外名次区间估算)

<board> = "<board_type>:<scope>:<scope_id>:<period>"(period 为空用 "-" 占位避免空段)。

★ 两段 Lua **逐字**从 Go 侧搬过来,一个字符都没改。
  它们在 Redis 服务端执行,与调用方语言无关 —— 原样搬等于把「按 mode 算新分 +
  打包 + 截断 + 直方图增减」这一整块原子性的正确性风险降到接近零,需要重新验证的
  只剩"参数传对没有"。反过来若借迁移之机用 Python 命令序列重写,每一步之间都是
  新的竞态入口(截断与直方图回扣分家 = 直方图永久偏移,且没有任何运行期信号)。

★ key 前缀 / board 串格式 / 分数打包系数是**跨语言硬契约**:迁移期 Go 副本和
  Python 副本会读写同一批榜。格式差一个字符 = 两边各写各的 key,两边都"成功",
  玩家看到的名次取决于请求落到哪个副本。
"""

from __future__ import annotations

import dataclasses
import math

from pandora.leaderboard.v1 import leaderboard_pb2

from pandorapy import errcode
from pandorapy import redisx
from pandorapy.services.leaderboard import estimate as lbest

# 排行榜时间 tie-break 的纪元(2026-01-01 UTC,毫秒)。normTs = ts_ms - LB_EPOCH_MS。
# ★ 与 Go 的 lbEpochMs 必须同值:它参与 packed 分数计算,两边不同 = 同一份榜里
# 一部分成员的 tie-break 方向被算反(差值可能为负 → normTs 钳到 0 → 同分全部并列)。
LB_EPOCH_MS = 1767225600000

# 榜归属维度 / 上报模式 —— **直接引用 proto 生成常量,不手抄数值**。
# 手抄的话测试会抄同一个错值,于是永远不红(本仓刚修完 13 处这种错位)。
SCOPE_GLOBAL = leaderboard_pb2.LEADERBOARD_SCOPE_GLOBAL
SCOPE_GUILD = leaderboard_pb2.LEADERBOARD_SCOPE_GUILD
SCOPE_INSTANCE = leaderboard_pb2.LEADERBOARD_SCOPE_INSTANCE
SCOPE_CUSTOM = leaderboard_pb2.LEADERBOARD_SCOPE_CUSTOM

MODE_SET_IF_HIGHER = leaderboard_pb2.SUBMIT_MODE_SET_IF_HIGHER
MODE_SET = leaderboard_pb2.SUBMIT_MODE_SET
MODE_INCREMENT = leaderboard_pb2.SUBMIT_MODE_INCREMENT

# 估算直方图桶宽的兜底默认值。真源在 estimate.py(conf 默认值也引用同一个数)。
DEFAULT_ESTIMATE_BUCKET_WIDTH = lbest.DEFAULT_ESTIMATE_BUCKET_WIDTH


@dataclasses.dataclass(frozen=True, slots=True)
class BoardKey:
    """榜的复合标识(存储层内部结构)。对应 Go 的 data.BoardKey。"""

    board_type: int
    scope: int
    scope_id: int = 0
    period: str = ""

    def board_str(self) -> str:
        """board 串。period 空用 "-" 占位 —— 不占位的话 `1:1:0:` 与 `1:1:0` 之后
        再拼别的段会撞在一起,两个不同的榜共用一个 key。"""
        return f"{self.board_type}:{int(self.scope)}:{self.scope_id}:{self.period or '-'}"

    def z_key(self) -> str:
        return f"pandora:lb:{{{self.board_str()}}}:z"

    def t_key(self) -> str:
        return f"pandora:lb:{{{self.board_str()}}}:t"

    def m_key(self) -> str:
        return f"pandora:lb:{{{self.board_str()}}}:m"

    def s_key(self) -> str:
        return f"pandora:lb:{{{self.board_str()}}}:s"

    def h_key(self) -> str:
        return f"pandora:lb:{{{self.board_str()}}}:h"

    def all_keys(self) -> list[str]:
        """五个 key,顺序即 Lua 的 KEYS[1..5] —— 顺序错了 Lua 会往错的结构写。"""
        return [self.z_key(), self.t_key(), self.m_key(), self.s_key(), self.h_key()]


@dataclasses.dataclass(frozen=True, slots=True)
class Options:
    """建榜 / 写入行为参数。对应 Go 的 data.Options。"""

    ttl_seconds: int = 0
    max_size: int = 0
    tie_break_by_time: bool = False
    ascending: bool = False
    # 榜外估算直方图桶宽(>0;biz 层已用服务默认值兜底)。建榜时写入 meta 定死。
    estimate_bucket_width: int = 0


@dataclasses.dataclass(frozen=True, slots=True)
class Entry:
    """榜上一项(存储层视图)。rank 是 1-based。"""

    entity_id: int
    score: int
    rank: int = 0
    updated_at_ms: int = 0


def unpack_real(packed: float) -> int:
    """把 ZSET packed score 还原成真实整数分。

    Go 写的是 `int64(math.Floor(packed + 0.5))`,不是 Python 内置 `round()` ——
    后者是**银行家舍入**(round(0.5)==0、round(1.5)==2),在 packed 恰好落在
    x.5 上时与 Go 结果差 1 分。时间 tie-break 项的量级是 1e-13,正常不会踩到,
    但 score 本身来自客户端上报,不做假设。
    """
    return int(math.floor(packed + 0.5))


# ── Lua(逐字搬自 Go 侧,勿改)────────────────────────────────────────────────

# 原子完成:读旧分(:s 全员分,回退 :z)→ 按 mode 算新真实分 → 打包 → ZADD/HSET →
# 维护全员分数 :s 与直方图 :h → 截断 maxSize(只清 :z/:t)→ 设 TTL → 返回真实分 + 名次。
# KEYS[1]=zkey KEYS[2]=tkey KEYS[3]=mkey KEYS[4]=skey KEYS[5]=hkey
# ARGV: 1 member 2 score 3 mode 4 tieBreak(0/1) 5 ascending(0/1) 6 tsMs 7 epochMs
#       8 maxSize 9 ttlSeconds 10 bucketWidth
# 返回: {newReal, rank1Based}
SUBMIT_LUA = """
local zkey, tkey, mkey, skey, hkey = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local member = ARGV[1]
local score  = tonumber(ARGV[2])
local mode   = tonumber(ARGV[3])
local tie    = tonumber(ARGV[4])
local asc    = tonumber(ARGV[5])
local ts     = tonumber(ARGV[6])
local epoch  = tonumber(ARGV[7])
local maxSize= tonumber(ARGV[8])
local ttl    = tonumber(ARGV[9])
local bwArg  = tonumber(ARGV[10])

local function realOf(p) return math.floor(p + 0.5) end

-- 桶索引(floor 除法;钳制 ±1048576,与 Go 侧 maxBucketIdx 一致)
local MAXB = 1048576
local function bucketOf(v, w)
  local q = math.floor(v / w)
  if q > MAXB then q = MAXB end
  if q < -MAXB then q = -MAXB end
  return q
end

-- 首次写定义榜元信息(asc / tie),供后续读查询判定排序方向
if redis.call('EXISTS', mkey) == 0 then
  redis.call('HSET', mkey, 'asc', asc, 'tie', tie)
end
-- 桶宽只在首次定死(升级前旧榜的 meta 无 bw,首次再上报时补记);后续变更忽略
if redis.call('HEXISTS', mkey, 'bw') == 0 then
  redis.call('HSET', mkey, 'bw', bwArg)
end
local bw = tonumber(redis.call('HGET', mkey, 'bw'))

-- 旧真实分:优先 :s 全员分(截断后仍在);:s 无记录回退 :z(升级前旧榜存量成员)
local sRaw = redis.call('HGET', skey, member)
local sExisted = (sRaw ~= false)
local curReal = nil
if sExisted then
  curReal = tonumber(sRaw)
else
  local cur = redis.call('ZSCORE', zkey, member)
  if cur then curReal = realOf(tonumber(cur)) end
end

-- 决定新真实分与是否写入
local newReal
local doWrite = true
if mode == 3 then
  newReal = (curReal or 0) + score
else
  newReal = score
  if mode == 1 and curReal ~= nil then
    if asc == 1 then
      if newReal >= curReal then doWrite = false end
    else
      if newReal <= curReal then doWrite = false end
    end
  end
end

if doWrite then
  local normTs = ts - epoch
  if normTs < 0 then normTs = 0 end
  local packed = newReal
  if tie == 1 then
    if asc == 1 then packed = newReal + normTs * 1e-13
    else packed = newReal - normTs * 1e-13 end
  end
  redis.call('ZADD', zkey, packed, member)
  redis.call('HSET', tkey, member, ts)
else
  newReal = curReal
end

-- 全员分数 :s + 直方图 :h(doWrite=false 且已记录时分数未变,无需动)
if doWrite or not sExisted then
  redis.call('HSET', skey, member, newReal)
  local newB = bucketOf(newReal, bw)
  if not sExisted then
    -- 首次进直方图(含旧榜存量成员补记)
    redis.call('HINCRBY', hkey, newB, 1)
  else
    local oldB = bucketOf(curReal, bw)
    if oldB ~= newB then
      redis.call('HINCRBY', hkey, newB, 1)
      local c = redis.call('HINCRBY', hkey, oldB, -1)
      if c <= 0 then redis.call('HDEL', hkey, oldB) end
    end
  end
end

-- 截断 maxSize(精确榜只保留最优 Top-N,清理被挤出者的 t 记录;:s/:h 保留全员供估算)
if maxSize > 0 then
  local n = redis.call('ZCARD', zkey)
  if n > maxSize then
    local victims
    if asc == 1 then
      victims = redis.call('ZRANGE', zkey, maxSize, -1)            -- 升序:最优在前,挤出尾部(高分)
    else
      victims = redis.call('ZRANGE', zkey, 0, n - maxSize - 1)     -- 降序:最优在后,挤出头部(低分)
    end
    if victims and #victims > 0 then
      redis.call('ZREM', zkey, unpack(victims))
      redis.call('HDEL', tkey, unpack(victims))
    end
  end
end

-- TTL(临时榜)
if ttl > 0 then
  redis.call('EXPIRE', zkey, ttl)
  redis.call('EXPIRE', tkey, ttl)
  redis.call('EXPIRE', mkey, ttl)
  redis.call('EXPIRE', skey, ttl)
  redis.call('EXPIRE', hkey, ttl)
end

-- 名次(1-based;被截断 / 不在榜 → 0)
local rank = 0
local idx
if asc == 1 then idx = redis.call('ZRANK', zkey, member)
else idx = redis.call('ZREVRANK', zkey, member) end
if idx ~= false and idx ~= nil then rank = idx + 1 end

if newReal == nil then newReal = 0 end
return {newReal, rank}
"""

# 原子移除某 entity(封号 / 作弊清理):按 :s 记录的分回扣直方图,再清 z/t/s。
# KEYS[1]=zkey KEYS[2]=tkey KEYS[3]=mkey KEYS[4]=skey KEYS[5]=hkey;ARGV: 1 member
REMOVE_LUA = """
local zkey, tkey, mkey, skey, hkey = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local member = ARGV[1]

local MAXB = 1048576
local function bucketOf(v, w)
  local q = math.floor(v / w)
  if q > MAXB then q = MAXB end
  if q < -MAXB then q = -MAXB end
  return q
end

local sRaw = redis.call('HGET', skey, member)
if sRaw then
  local bwRaw = redis.call('HGET', mkey, 'bw')
  if bwRaw then
    local bw = tonumber(bwRaw)
    if bw and bw > 0 then
      local bkt = bucketOf(tonumber(sRaw), bw)
      local c = redis.call('HINCRBY', hkey, bkt, -1)
      if c <= 0 then redis.call('HDEL', hkey, bkt) end
    end
  end
  redis.call('HDEL', skey, member)
end
redis.call('ZREM', zkey, member)
redis.call('HDEL', tkey, member)
return 1
"""

_SUBMIT = redisx.LuaScript(name="lb_submit", body=SUBMIT_LUA)
_REMOVE = redisx.LuaScript(name="lb_remove", body=REMOVE_LUA)


def _to_int(raw) -> int | None:  # noqa: ANN001
    """redis 回来的 bytes/str/int → int。decode_responses=False 所以多数是 bytes。"""
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


class RedisBoardStore:
    """基于 redis-py async 的排行榜存储。对应 Go 的 data.RedisBoardStore。"""

    __slots__ = ("_rdb",)

    def __init__(self, rdb) -> None:  # noqa: ANN001
        self._rdb = rdb

    # ── 写 ───────────────────────────────────────────────────────────────────

    async def submit(
        self,
        b: BoardKey,
        entity_id: int,
        score: int,
        mode: int,
        opt: Options,
        ts_ms: int,
    ) -> tuple[int, int]:
        """按 mode 写入分数并(可选)截断 / 设 TTL。返回 (真实分, 1-based 名次;0=未上榜)。"""
        bw = opt.estimate_bucket_width
        if bw <= 0:
            # 防御:biz 已兜底,这里保证 Lua 不除零(除零会让整条 Submit 路径报错)。
            bw = DEFAULT_ESTIMATE_BUCKET_WIDTH
        try:
            res = await _SUBMIT(
                self._rdb,
                keys=b.all_keys(),
                args=[
                    str(entity_id),
                    score,
                    int(mode),
                    1 if opt.tie_break_by_time else 0,
                    1 if opt.ascending else 0,
                    ts_ms,
                    LB_EPOCH_MS,
                    opt.max_size,
                    opt.ttl_seconds,
                    bw,
                ],
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "lb submit board=%s entity=%d: %s",
                b.board_str(),
                entity_id,
                exc,
            ) from exc
        if not isinstance(res, (list, tuple)) or len(res) != 2:
            raise errcode.PandoraError(
                errcode.ErrInternal, "lb submit bad reply board=%s", b.board_str()
            )
        return int(res[0]), int(res[1])

    async def remove(self, b: BoardKey, entity_id: int) -> None:
        """移除某 entity(直方图同步回扣)。"""
        try:
            await _REMOVE(self._rdb, keys=b.all_keys(), args=[str(entity_id)])
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "lb remove board=%s entity=%d: %s",
                b.board_str(),
                entity_id,
                exc,
            ) from exc

    async def delete(self, b: BoardKey) -> None:
        """删整个榜(z + t + meta + 全员分 + 直方图)。"""
        try:
            await self._rdb.delete(*b.all_keys())
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "lb delete board=%s: %s", b.board_str(), exc
            ) from exc

    async def clear(self, b: BoardKey) -> None:
        """清空榜分数(周期 reset)。

        ★ **保留 meta(:m)** —— 与 delete 的唯一区别就在这里。meta 存的是 asc/tie/bw,
        清掉的话下一周期首次上报会用**新传进来的**参数重新建榜:桶宽一变,
        直方图口径就跟同一榜的历史对不上;asc 一变,整个榜的排序方向翻转。
        """
        try:
            await self._rdb.delete(b.z_key(), b.t_key(), b.s_key(), b.h_key())
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "lb clear board=%s: %s", b.board_str(), exc
            ) from exc

    # ── 读 ───────────────────────────────────────────────────────────────────

    async def rank(self, b: BoardKey, entity_id: int, ascending: bool) -> tuple[Entry, bool]:
        """查某 entity 的名次 + 分;不在榜 found=False。"""
        member = str(entity_id)
        zkey = b.z_key()
        try:
            idx = (
                await self._rdb.zrank(zkey, member)
                if ascending
                else await self._rdb.zrevrank(zkey, member)
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "lb rank board=%s entity=%d: %s",
                b.board_str(),
                entity_id,
                exc,
            ) from exc
        if idx is None:
            # redis-py 的 None 对应 go-redis 的 redis.Nil —— 是"不在榜"而不是故障。
            return Entry(entity_id=entity_id, score=0), False
        try:
            packed = await self._rdb.zscore(zkey, member)
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "lb score board=%s entity=%d: %s",
                b.board_str(),
                entity_id,
                exc,
            ) from exc
        if packed is None:
            # rank 拿到了 score 却没了 = 两条命令之间被 ZREM。按"不在榜"处理:
            # 与 Go 的差别只在错误码(那边报 ErrInternal),但把一次并发删除当故障
            # 会让正常的封号清理在读侧刷 ERROR。
            return Entry(entity_id=entity_id, score=0), False
        updated = _to_int(await self._rdb.hget(b.t_key(), member)) or 0
        return (
            Entry(
                entity_id=entity_id,
                score=unpack_real(float(packed)),
                rank=int(idx) + 1,
                updated_at_ms=updated,
            ),
            True,
        )

    async def estimate(
        self, b: BoardKey, entity_id: int, ascending: bool
    ) -> tuple[Entry, int, bool]:
        """用分数直方图估算未进精确榜 entity 的名次(约值,updated_at_ms 恒 0)。

        返回 (entry, 直方图口径参与总人数, found)。
        从未上报 / 榜无直方图(升级前旧榜)→ found=False。
        """
        member = str(entity_id)
        try:
            score_raw = await self._rdb.hget(b.s_key(), member)
            if score_raw is None:
                return Entry(entity_id=entity_id, score=0), 0, False  # 从未上报
            bw_raw = await self._rdb.hget(b.m_key(), "bw")
            if bw_raw is None:
                return Entry(entity_id=entity_id, score=0), 0, False  # 旧榜无直方图配置
            hist_raw = await self._rdb.hgetall(b.h_key())
            on_board = int(await self._rdb.zcard(b.z_key()) or 0)
        except errcode.PandoraError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "lb estimate board=%s entity=%d: %s",
                b.board_str(),
                entity_id,
                exc,
            ) from exc

        score = _to_int(score_raw)
        if score is None:
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "lb estimate bad score board=%s entity=%d: %r",
                b.board_str(),
                entity_id,
                score_raw,
            )
        bw = _to_int(bw_raw)
        if bw is None or bw <= 0:
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "lb estimate bad bw board=%s: %r",
                b.board_str(),
                bw_raw,
            )

        histogram: dict[int, int] = {}
        for idx_raw, cnt_raw in (hist_raw or {}).items():
            idx = _to_int(idx_raw)
            cnt = _to_int(cnt_raw)
            if idx is None or cnt is None:
                continue  # 脏 field 跳过,不影响其余桶(与 Go 同)
            histogram[idx] = cnt

        # 估算算法本体在 estimate.py —— 那里已有跨语言对拍测试,不在这里重写一遍。
        out = lbest.estimate_rank(
            entity_id=entity_id,
            score=score,
            bucket_width=bw,
            histogram=histogram,
            on_board_count=on_board,
            ascending=ascending,
        )
        return Entry(entity_id=out.entity_id, score=out.score, rank=out.rank), out.total_submitters, True

    async def range(  # noqa: A003 —— 与 Go 的 Range 同名,改名会让两边对不上
        self, b: BoardKey, offset: int, limit: int, ascending: bool
    ) -> list[Entry]:
        """取榜区间(offset 0-based)。limit<=0 或 offset<0 → 空(与 Go 同,不报错)。"""
        if limit <= 0 or offset < 0:
            return []
        zkey = b.z_key()
        stop = offset + limit - 1
        try:
            zs = await self._rdb.zrange(
                zkey, offset, stop, desc=not ascending, withscores=True
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "lb range board=%s: %s", b.board_str(), exc
            ) from exc
        return await self._to_entries(b, zs, offset)

    async def around(
        self, b: BoardKey, entity_id: int, radius: int, ascending: bool
    ) -> tuple[list[Entry], bool]:
        """取某 entity 上下 radius 名(含自身);不在榜 found=False。"""
        member = str(entity_id)
        zkey = b.z_key()
        try:
            idx = (
                await self._rdb.zrank(zkey, member)
                if ascending
                else await self._rdb.zrevrank(zkey, member)
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "lb around rank board=%s entity=%d: %s",
                b.board_str(),
                entity_id,
                exc,
            ) from exc
        if idx is None:
            return [], False
        start = max(0, int(idx) - radius)
        stop = int(idx) + radius
        try:
            zs = await self._rdb.zrange(
                zkey, start, stop, desc=not ascending, withscores=True
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "lb around range board=%s: %s", b.board_str(), exc
            ) from exc
        return await self._to_entries(b, zs, start), True

    async def total(self, b: BoardKey) -> int:
        """榜总人数(精确榜口径 = ZCARD,不含被截断出榜的人)。"""
        try:
            return int(await self._rdb.zcard(b.z_key()) or 0)
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "lb total board=%s: %s", b.board_str(), exc
            ) from exc

    async def get_meta(self, b: BoardKey) -> tuple[bool, bool, bool]:
        """读榜元信息。返回 (ascending, tie_break, exists);榜不存在 exists=False。

        ★ exists=False 与 "ascending=False" 是两件事:SettleBoard 靠 exists 判断
        "榜压根不存在"并回 ErrLeaderboardBoardNotFound。混为一谈会让结算在空榜上
        照常建批次 + 占掉 settle uk —— 那一期就再也结不出来了。
        """
        try:
            vals = await self._rdb.hmget(b.m_key(), ["asc", "tie"])
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "lb meta board=%s: %s", b.board_str(), exc
            ) from exc
        if not vals or vals[0] is None:
            return False, False, False
        asc = _to_int(vals[0]) == 1
        tie = len(vals) > 1 and _to_int(vals[1]) == 1
        return asc, tie, True

    # ── 辅助 ─────────────────────────────────────────────────────────────────

    async def _to_entries(self, b: BoardKey, zs, start_rank: int) -> list[Entry]:  # noqa: ANN001
        """把 ZSET 区间结果 + updated_at 拼成 Entry 列表(start_rank 是首项 0-based 名次)。"""
        if not zs:
            return []
        members: list[str] = []
        for member, _score in zs:
            members.append(member.decode() if isinstance(member, (bytes, bytearray)) else str(member))
        try:
            updated = await self._rdb.hmget(b.t_key(), members)
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "lb hmget board=%s: %s", b.board_str(), exc
            ) from exc
        out: list[Entry] = []
        for i, (_member, score) in enumerate(zs):
            entity_id = _to_int(members[i])
            if entity_id is None:
                continue  # 非数字 member(不该出现)跳过,不让一条脏数据毁掉整页
            up = _to_int(updated[i]) if i < len(updated) else None
            out.append(
                Entry(
                    entity_id=entity_id,
                    score=unpack_real(float(score)),
                    rank=start_rank + i + 1,
                    updated_at_ms=up or 0,
                )
            )
        return out
