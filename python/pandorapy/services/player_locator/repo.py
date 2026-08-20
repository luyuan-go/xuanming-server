"""player_locator 数据层(redis-only)—— 对应 Go 侧 internal/data/location.go。

★ 三段 Lua **逐字从 Go 搬过来**,一个字符都没改。

  为什么不"用 Python 重写得更好看":这三段脚本是 Go 副本和 Python 副本在灰度期
  **同时**打到同一个 Redis 上的。脚本行为只要差一点(比如 uint64 比较从字符串
  改成 tonumber),两个实现对同一份 meta 的判定就会分叉 —— 而分叉的表现是
  「有的副本接受这次 HUB 写、有的拒绝」,没有任何一边报错。

  同理 key 名(pandora:locator:*)也必须逐字一致:前缀差一个字符 =
  两个实现各写各的 key,§1 的「单写者覆盖 = 自动顶号」当场失效。

★ locator 是 **presence 投影**,不是归属权威(§9.22)。
  key miss 只说明 presence 不可见,**不能**证明玩家已离开旧 DS,
  更不能授权进入另一台 DS。归属判定必须查 owner 权威。
"""

from __future__ import annotations

import dataclasses
import time
from typing import Any, Awaitable, Callable

from redis.exceptions import WatchError

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import logwindow
from pandorapy import redisx

# ── key 口径:必须与 Go 逐字一致 ──────────────────────────────────────────────


def loc_key(player_id: int) -> str:
    return f"pandora:locator:{player_id}"


def last_seen_key(player_id: int) -> str:
    """旧协议留下的 string key。

    新协议只在 hub_meta_key 完全不存在时兼容读取;一旦新 locator 见过一次
    HUB Set,meta marker 就会屏蔽这份旧时刻。
    """
    return f"pandora:locator:lastseen:{player_id}"


def hub_meta_key(player_id: int) -> str:
    """当前 Hub 连接 fence + 可选 left_at_ms。

    必须**独立于**带 presence TTL 的 location key,才能在 location 过期后
    继续回答离线时长;同时所有代际推进 / 清理 / 记录离开都只在本单 key Lua 内
    完成,避免 Redis Cluster 的 CROSSSLOT。
    """
    return f"pandora:locator:hubmeta:{player_id}"


# last_alive_ms 的最小写入间隔。取 30s 的依据:消费方最短阈值是 team 的 180s,
# 30s 误差占 1/6 不影响判定;而 BATTLE 心跳每 5s 每人一次,不节流会让这条链的
# Redis 写量按 6 倍放大且毫无收益。调大要与最短阈值一起看。
ALIVE_TOUCH_THROTTLE_SEC = 30

# 差集样本名单长度上限:告警要能点名,但不能让单条日志随 census 规模(500 人/hub)
# 无界膨胀。
REFRESH_DIFF_SAMPLE_CAP = 8

# 「投影蒸发」告警的限流窗口(首错必打 + 每窗口一条)。
REFRESH_DIFF_WINDOW_MS = 60_000

# LOCATION_STATE_HUB 的字面值。Lua / redis hash 里存的是十进制串,
# 这里保留 Go 同样的内联注释形态,数值来源见 biz.LOCATION_STATE_HUB。
_HUB_STATE_STR = "3"


@dataclasses.dataclass(frozen=True, slots=True)
class HubPresenceFence:
    """一次 Hub 物理连接的精确 identity —— 与 proto 字段 1:1,不多不少。

    职责边界(§9.22):只解决投影自己的问题「同一个 Pod 上,哪条物理连接是当前这条」。
    **跨 assignment 的归属顺序刻意不在这里判** —— 那是 hub_allocator 的权威。
    """

    assignment_id: str = ""
    admission_id: str = ""
    admission_seq: int = 0

    def is_zero(self) -> bool:
        return not self.assignment_id and not self.admission_id and self.admission_seq == 0

    def is_complete(self) -> bool:
        return bool(self.assignment_id) and bool(self.admission_id) and self.admission_seq > 0

    def equal(self, other: "HubPresenceFence") -> bool:
        return (
            self.assignment_id == other.assignment_id
            and self.admission_id == other.admission_id
            and self.admission_seq == other.admission_seq
        )


@dataclasses.dataclass(slots=True)
class LocationRecord:
    """写入 / 读出 redis 的中间结构(避免 data 层依赖 proto)。"""

    state: int = 0
    hub_pod: str = ""
    shard_id: int = 0
    match_id: int = 0
    battle_pod: str = ""
    updated_at_ms: int = 0
    hub_presence_fence: HubPresenceFence = dataclasses.field(default_factory=HubPresenceFence)


# ── Lua:逐字搬自 Go(勿改一字,见模块头)──────────────────────────────────

HUB_PRESENCE_SCRIPT = redisx.LuaScript(
    name="locator_hub_presence",
    body="""
local function compare_uint_decimal(left, right)
  left = string.gsub(left or '0', '^0+', '')
  right = string.gsub(right or '0', '^0+', '')
  if left == '' then left = '0' end
  if right == '' then right = '0' end
  if string.len(left) < string.len(right) then return -1 end
  if string.len(left) > string.len(right) then return 1 end
  if left < right then return -1 end
  if left > right then return 1 end
  return 0
end

local action = ARGV[1]
local incoming_mode = ARGV[2]
local current_mode = redis.call('HGET', KEYS[1], 'mode')
if action ~= 'validate' and action ~= 'commit' then return 0 end
if not current_mode and redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
if current_mode and current_mode ~= 'legacy' and current_mode ~= 'fenced' then return 0 end

if incoming_mode == 'legacy' then
  if current_mode == 'fenced' then return 0 end
  if action == 'validate' then return 1 end
  redis.call('HSET', KEYS[1], 'mode', 'legacy')
  redis.call('HDEL', KEYS[1], 'assignment_id', 'admission_id', 'admission_seq', 'left_at_ms', 'last_alive_ms')
  redis.call('PEXPIRE', KEYS[1], ARGV[6])
  return 1
end

if current_mode == 'fenced' then
  if (redis.call('HGET', KEYS[1], 'assignment_id') or '') == ARGV[3] then
    local seq_cmp = compare_uint_decimal(redis.call('HGET', KEYS[1], 'admission_seq'), ARGV[5])
    if seq_cmp > 0 then return 0 end
    if seq_cmp == 0 and (redis.call('HGET', KEYS[1], 'admission_id') or '') ~= ARGV[4] then return 0 end
    if seq_cmp == 0 and redis.call('HEXISTS', KEYS[1], 'left_at_ms') == 1 then return 0 end
  end
end

if action == 'validate' then return 1 end
redis.call('HSET', KEYS[1],
  'mode', 'fenced',
  'assignment_id', ARGV[3],
  'admission_id', ARGV[4],
  'admission_seq', ARGV[5])
redis.call('HDEL', KEYS[1], 'left_at_ms')
redis.call('PEXPIRE', KEYS[1], ARGV[6])
return 1""",
)

# ★ 只在 meta 已存在时写:HSET 会凭空建 key,而一个「有内容但没有 mode 字段」的 meta
# 会被 HUB_PRESENCE_SCRIPT 判为损坏数据并 fail-closed(那条判定是对的,不能放松),
# 结果就是给 legacy 玩家造出一个永远无法接受 HUB 写的毒 key。EXISTS 守卫挡住这一点。
#
# ⚠️ 三个 ARGV 一个都不能少:脚本用 ARGV[3] 做节流比较,少传会在「meta 已有
# last_alive_ms」时(即第 2 次心跳起)必然 Lua 报错 `attempt to compare number with nil`,
# 整条链静默失效(INC-20260813-001)。
TOUCH_HUB_ALIVE_BODY = """
if redis.call('EXISTS', KEYS[1]) ~= 1 then return 0 end
local now = tonumber(ARGV[1])
local prev = tonumber(redis.call('HGET', KEYS[1], 'last_alive_ms'))
if prev and (now - prev) < tonumber(ARGV[3]) then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 0
end
redis.call('HSET', KEYS[1], 'last_alive_ms', ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1"""

TOUCH_HUB_ALIVE_SCRIPT = redisx.LuaScript(
    name="locator_touch_hub_alive", body=TOUCH_HUB_ALIVE_BODY
)

# 原子完成「守卫校验 + 缩 TTL」(单 key,Lua 内无并发写插入的窗口)。
# 返回 0=身份不匹配,1=实际缩短,2=身份匹配但 TTL 已经更短(幂等接受)。
# 若非原子(先 HMGET 再 EXPIRE),窗口内状态被并发写成 MATCHING/BATTLE 会误缩
# 新状态的 TTL 到 grace,与「不误伤对局态」的设计目标冲突。
SHRINK_HUB_TTL_SCRIPT = redisx.LuaScript(
    name="locator_shrink_hub_ttl",
    body="""
if redis.call('HGET', KEYS[1], 'state') ~= '3' then return 0 end
if redis.call('HGET', KEYS[1], 'hub_pod') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'hub_assignment_id') ~= ARGV[2] then return 0 end
if redis.call('HGET', KEYS[1], 'hub_admission_id') ~= ARGV[3] then return 0 end
if redis.call('HGET', KEYS[1], 'hub_admission_seq') ~= ARGV[4] then return 0 end
local changed = redis.call('PEXPIRE', KEYS[1], ARGV[5], 'LT')
if changed == 1 then return 1 end
return 2""",
)

# 只给当前 exact fence 写首次离开时刻。location 守卫已经通过而 meta 恰好丢失时
# 允许重建;若重连已推进 meta,则旧请求严格返回 0。
RECORD_LAST_SEEN_SCRIPT = redisx.LuaScript(
    name="locator_record_last_seen",
    body="""
local current_mode = redis.call('HGET', KEYS[1], 'mode')
if not current_mode and redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
if current_mode then
  if current_mode ~= 'fenced' then return 0 end
  if redis.call('HGET', KEYS[1], 'assignment_id') ~= ARGV[1] then return 0 end
  if redis.call('HGET', KEYS[1], 'admission_id') ~= ARGV[2] then return 0 end
  if redis.call('HGET', KEYS[1], 'admission_seq') ~= ARGV[3] then return 0 end
else
  redis.call('HSET', KEYS[1],
    'mode', 'fenced',
    'assignment_id', ARGV[1],
    'admission_id', ARGV[2],
    'admission_seq', ARGV[3])
end
local existing = redis.call('HGET', KEYS[1], 'left_at_ms')
if existing then
  redis.call('PEXPIRE', KEYS[1], ARGV[5])
  return tonumber(existing) or 0
end
redis.call('HSET', KEYS[1], 'left_at_ms', ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return tonumber(ARGV[4])""",
)


def _s(v: Any) -> str:
    """redis 客户端建的是 decode_responses=False,所有回值都是 bytes。"""
    if v is None:
        return ""
    if isinstance(v, bytes):
        return v.decode("utf-8", "replace")
    return str(v)


def _parse_int(raw: Any, default: int = 0) -> int:
    try:
        return int(_s(raw))
    except (TypeError, ValueError):
        return default


def parse_location_map(m: dict) -> LocationRecord:
    """把 redis hash 解析成 LocationRecord(容错:解析失败的字段留零值)。

    容错是刻意的:一条字段坏掉不该让整个玩家的位置查询失败 ——
    §9.22 下「查不到」会被上游当成一个需要 fail-closed 的信号,
    而这里坏的只是一个 shard_id。
    """
    d = {_s(k): v for k, v in m.items()}
    return LocationRecord(
        state=_parse_int(d.get("state")),
        hub_pod=_s(d.get("hub_pod")),
        shard_id=_parse_int(d.get("shard_id")),
        match_id=_parse_int(d.get("match_id")),
        battle_pod=_s(d.get("battle_pod")),
        updated_at_ms=_parse_int(d.get("updated_at_ms")),
        hub_presence_fence=HubPresenceFence(
            assignment_id=_s(d.get("hub_assignment_id")),
            admission_id=_s(d.get("hub_admission_id")),
            admission_seq=_parse_int(d.get("hub_admission_seq")),
        ),
    )


@dataclasses.dataclass(slots=True)
class _RefreshDiff:
    """一次 RefreshHubLocations 的差集分类(census 在场却未续期的原因)。"""

    missing_total: int = 0
    missing_sample: list[int] = dataclasses.field(default_factory=list)
    pod_mismatch_total: int = 0
    pod_mismatch_sample: list[int] = dataclasses.field(default_factory=list)
    other_state: int = 0
    read_failed: int = 0


class RedisLocationRepo:
    """基于 redis.asyncio 的位置仓储。对应 Go 的 RedisLocationRepo。"""

    def __init__(self, rdb) -> None:  # noqa: ANN001
        self._rdb = rdb
        # 限流「census 在场但投影已蒸发」告警(首错 + 窗口):撮合失败后 MATCHING
        # 到期的玩家会连续多拍落进差集,逐拍 WARN 会刷屏。
        self._refresh_diff_log = logwindow.Window()

    # ── 长 TTL meta:代际闸 ────────────────────────────────────────────────

    async def validate_hub_presence(self, player_id: int, fence: HubPresenceFence) -> bool:
        """只读校验,不改变 meta。

        必须是**只读**的:业务状态守卫(MATCHING/BATTLE)随后可能拒绝这次写,
        那时 meta 不得留下半步副作用 —— 否则一次被拒的写会把当前代顶掉。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "playerID must be valid")
        if not fence.is_zero() and not fence.is_complete():
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "hub presence fence must be complete or empty"
            )
        mode = "legacy" if fence.is_zero() else "fenced"
        try:
            accepted = await HUB_PRESENCE_SCRIPT(
                self._rdb,
                keys=[hub_meta_key(player_id)],
                args=["validate", mode, fence.assignment_id, fence.admission_id,
                      fence.admission_seq, 0],
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "redis validate hub presence: %s", exc
            ) from exc
        return _parse_int(accepted) == 1

    async def activate_hub_presence(
        self, player_id: int, fence: HubPresenceFence, retention_sec: float
    ) -> bool:
        """在 location CAS 写成功后 commit meta。

        legacy(全零 fence)只在尚无 fenced 当前代时接受,用于滚动升级安全降级。
        """
        if player_id == 0 or retention_sec <= 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "playerID and retention must be valid"
            )
        if not fence.is_zero() and not fence.is_complete():
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "hub presence fence must be complete or empty"
            )
        mode = "legacy" if fence.is_zero() else "fenced"
        try:
            accepted = await HUB_PRESENCE_SCRIPT(
                self._rdb,
                keys=[hub_meta_key(player_id)],
                args=["commit", mode, fence.assignment_id, fence.admission_id,
                      fence.admission_seq, int(retention_sec * 1000)],
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "redis activate hub presence: %s", exc
            ) from exc
        return _parse_int(accepted) == 1

    # ── location 主键:CAS 写 ──────────────────────────────────────────────

    async def set_guarded(
        self,
        player_id: int,
        rec: LocationRecord,
        ttl_sec: float,
        max_retry: int,
        guard: Callable[[LocationRecord, bool], Awaitable[None] | None] | None,
    ) -> None:
        """WATCH/MULTI/EXEC 原子读-判-写。对应 Go 的 SetGuarded。

        流程(每次重试一轮 WATCH):
          1. WATCH key 并读当前记录
          2. guard(cur, found):抛错 → 中止写,原样抛出(业务守卫拒绝,**不重试**)
          3. MULTI:DEL + HSET 覆盖 + EXPIRE 刷新 TTL

        先 DEL 再 HSET:不同 state 切换时不残留旧字段 ——
        BATTLE → HUB 时 match_id 不清除会被别的服务误读成"还在打"。
        """
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "playerID must > 0")
        key = loc_key(player_id)
        if rec.updated_at_ms == 0:
            rec.updated_at_ms = int(time.time() * 1000)

        for _attempt in range(max_retry + 1):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    raw = await pipe.hgetall(key)
                    cur = parse_location_map(raw) if raw else LocationRecord()
                    found = bool(raw)
                    if guard is not None:
                        # 守卫抛的是业务错误(ErrLocatorConflict),必须原样冒泡:
                        # 被 WatchError 分支吞掉就会变成"重试三次后 CAS 耗尽",
                        # 线上看到的原因是错的。
                        result = guard(cur, found)
                        if result is not None and hasattr(result, "__await__"):
                            await result
                    pipe.multi()
                    pipe.delete(key)
                    pipe.hset(
                        key,
                        mapping={
                            "state": rec.state,
                            "hub_pod": rec.hub_pod,
                            "shard_id": rec.shard_id,
                            "match_id": rec.match_id,
                            "battle_pod": rec.battle_pod,
                            "updated_at_ms": rec.updated_at_ms,
                            "hub_assignment_id": rec.hub_presence_fence.assignment_id,
                            "hub_admission_id": rec.hub_presence_fence.admission_id,
                            "hub_admission_seq": rec.hub_presence_fence.admission_seq,
                        },
                    )
                    pipe.expire(key, int(ttl_sec))
                    await pipe.execute()
                return
            except WatchError:
                continue  # CAS 冲突,重试
            except errcode.PandoraError:
                raise  # 业务守卫拒绝,不重试
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "redis location set: %s", exc
                ) from exc

        # WATCH/MULTI/EXEC 连续 max_retry+1 次都被并发写打断。返回的是
        # ErrLocatorConflict(in-band 业务码 → access log 只记 DEBUG),不显式打就
        # 完全不可见;而现象是「玩家位置怎么写都写不进去」,与守卫拒绝、redis 故障
        # 三者在线上无法区分。
        plog.get().warning(
            "locator_set_cas_exhausted",
            reason="optimistic_retry_exhausted",
            player_id=player_id,
            presence_state=rec.state,
            hub_pod=rec.hub_pod,
            battle_pod=rec.battle_pod,
            attempts=max_retry + 1,
            ttl_ms=int(ttl_sec * 1000),
        )
        raise errcode.PandoraError(
            errcode.ErrLocatorConflict,
            "player %d location set concurrent retry exhausted",
            player_id,
        )

    # ── 查询面 ────────────────────────────────────────────────────────────

    async def get(self, player_id: int) -> tuple[LocationRecord, bool]:
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "playerID must > 0")
        try:
            raw = await self._rdb.hgetall(loc_key(player_id))
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "redis location get: %s", exc
            ) from exc
        if not raw:
            return LocationRecord(), False
        return parse_location_map(raw), True

    async def batch_get(self, player_ids: list[int]) -> dict[int, LocationRecord]:
        """pipeline 一次往返批量 HGETALL。

        返回 map **只含命中的玩家**:未在线 / 不存在的 player_id 不出现在里面
        (调用方按缺席判离线),避免响应被大量离线占位撞胀。
        """
        out: dict[int, LocationRecord] = {}
        if not player_ids:
            return out
        uniq: list[int] = []
        seen: set[int] = set()
        for pid in player_ids:
            if pid == 0 or pid in seen:
                continue
            seen.add(pid)
            uniq.append(pid)
        if not uniq:
            return out
        pipe = self._rdb.pipeline(transaction=False)
        for pid in uniq:
            pipe.hgetall(loc_key(pid))
        try:
            results = await pipe.execute(raise_on_error=False)
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "redis location batch get: %s", exc
            ) from exc
        for pid, res in zip(uniq, results):
            # 单命令失败 / key 不存在 → 缺席判离线,不让整批失败。
            if isinstance(res, BaseException) or not res:
                continue
            out[pid] = parse_location_map(res)
        return out

    async def batch_get_last_seen(self, player_ids: list[int]) -> dict[int, int]:
        """批量读 last-seen 时刻。meta 只要存在就拥有优先级。

        没有 left_at_ms 明确表示当前连接**未离开**,绝不能再回退到重连前的旧 string
        时刻;只有 meta 完全不存在时才读旧 key,供滚动升级窗口兼容。

        返回 map 只含有记录的玩家;缺席 = UNKNOWN(从未记录 / 已超 retention),
        调用方不得当成 0 或「刚离开」(§9.22:不确定不得冒充默认值)。
        """
        out: dict[int, int] = {}
        if not player_ids:
            return out
        uniq: list[int] = []
        seen: set[int] = set()
        for pid in player_ids:
            if pid == 0 or pid in seen:
                continue
            seen.add(pid)
            uniq.append(pid)
        if not uniq:
            return out
        pipe = self._rdb.pipeline(transaction=False)
        for pid in uniq:
            pipe.hgetall(hub_meta_key(pid))
            pipe.get(last_seen_key(pid))
        try:
            results = await pipe.execute(raise_on_error=False)
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "redis batch get last seen: %s", exc
            ) from exc
        for idx, pid in enumerate(uniq):
            meta = results[idx * 2]
            legacy = results[idx * 2 + 1]
            if not isinstance(meta, BaseException) and meta:
                d = {_s(k): _s(v) for k, v in meta.items()}
                if d.get("mode") == "fenced":
                    # 两级来源,精确的优先:
                    #  ① left_at_ms  —— Hub DS 显式上报的离开时刻;
                    #  ② last_alive_ms —— 最后一次心跳把该玩家报为在场的时刻。
                    # ② 的意义是 **Hub DS 整台崩溃**:那种情况下没有任何
                    # ReportDisconnect,写不出 ①,此前只能返回 UNKNOWN →
                    # 消费方一律不动作 → 那批玩家永远挂在队伍里。
                    v = d.get("left_at_ms")
                    if v is None:
                        v = d.get("last_alive_ms")
                    if v is None:
                        continue
                    ms = _parse_int(v)
                    if ms > 0:
                        out[pid] = ms
                continue  # meta marker 存在即禁止读旧 key
            if isinstance(legacy, BaseException) or legacy is None:
                continue
            ms = _parse_int(legacy)
            if ms > 0:  # 坏值当缺席,不喂给调用方
                out[pid] = ms
        return out

    # ── 续期 / 断线 ───────────────────────────────────────────────────────

    async def refresh_hub_locations(
        self, hub_pod: str, player_ids: list[int], ttl_sec: float, meta_ttl_sec: float
    ) -> int:
        """批量续期 HUB 位置 TTL,并把整份 census 的 last_alive_ms 推到当下。

        两件事的作用域**刻意不同**,别把它们合并回同一个 if:
          - EXPIRE 写的是 §1 的位置投影,必须严守「非 HUB 态 / 别的 pod 的记录一律不动」;
          - last_alive_ms 写的是另一把长 TTL meta key,语义是「最后一次被 Hub DS
            观测在场」。玩家出现在 census 里就是这个事实本身,与投影处于哪一态无关
            (2026-08-13,INC-20260813-001)。

        非事务:步骤 1→2 之间状态若被并发写成 MATCHING/BATTLE,EXPIRE 只多续一次
        TTL(无害:对局态由战斗链路持续刷新,且下次写会重置 TTL),不值得上 WATCH。
        """
        if not hub_pod or not player_ids:
            return 0
        now_ms = int(time.time() * 1000)
        uniq: list[int] = []
        seen: set[int] = set()
        for pid in player_ids:
            if pid == 0 or pid in seen:
                continue
            seen.add(pid)
            uniq.append(pid)
        if not uniq:
            return 0

        read_pipe = self._rdb.pipeline(transaction=False)
        for pid in uniq:
            read_pipe.hmget(loc_key(pid), "state", "hub_pod")
        try:
            reads = await read_pipe.execute(raise_on_error=False)
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "redis hub refresh read: %s", exc
            ) from exc

        write_pipe = self._rdb.pipeline(transaction=False)
        refreshed = 0
        queued = 0
        diff = _RefreshDiff()
        for pid, vals in zip(uniq, reads):
            if meta_ttl_sec > 0:
                # 用 eval(全文)而不是 register_script:后者在 pipeline 里只发
                # EVALSHA,脚本没被本连接加载过就整批 NOSCRIPT 失败,
                # 而 pipeline 内拿不到单命令的自动 fallback。
                write_pipe.eval(
                    TOUCH_HUB_ALIVE_BODY,
                    1,
                    hub_meta_key(pid),
                    now_ms,
                    int(meta_ttl_sec * 1000),
                    ALIVE_TOUCH_THROTTLE_SEC * 1000,
                )
                queued += 1

            if isinstance(vals, BaseException) or not isinstance(vals, (list, tuple)) or len(vals) != 2:
                diff.read_failed += 1
                continue
            state_raw, pod_raw = vals
            if state_raw is None or pod_raw is None:
                # key 不存在 / 字段缺失:census 证明玩家此刻连在本 Hub,位置投影却
                # 已蒸发 ——「人在大厅但对 locator 不可见」(典型:撮合失败后
                # MATCHING 30s 到期无人重建,INC-20260724-001 的结构性形态)。
                if len(diff.missing_sample) < REFRESH_DIFF_SAMPLE_CAP:
                    diff.missing_sample.append(pid)
                diff.missing_total += 1
                continue
            state_str = _s(state_raw)
            try:
                int(state_str)
            except ValueError:
                diff.read_failed += 1
                continue
            if state_str != _HUB_STATE_STR:
                diff.other_state += 1  # MATCHING/BATTLE 过渡态:预期内,不动不点名
                continue
            if _s(pod_raw) != hub_pod:
                # state==HUB 但投影指向别台 Hub:Hub→Hub 迁移的过渡拍属正常,
                # 持续出现则是双 hub / 迟到 census 异常。
                if len(diff.pod_mismatch_sample) < REFRESH_DIFF_SAMPLE_CAP:
                    diff.pod_mismatch_sample.append(pid)
                diff.pod_mismatch_total += 1
                continue
            write_pipe.expire(loc_key(pid), int(ttl_sec))
            queued += 1
            refreshed += 1

        if queued > 0:
            try:
                await write_pipe.execute(raise_on_error=False)
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "redis hub refresh expire: %s", exc
                ) from exc
        self._log_refresh_diff(hub_pod, now_ms, diff)
        return refreshed

    def _log_refresh_diff(self, hub_pod: str, now_ms: int, d: _RefreshDiff) -> None:
        """把「Hub census 在场却没被续上」的差集变成可查日志。

        此前差集只有 DEBUG 的 requested-refreshed 计数 ——「我掉线了但还显示在线 /
        我怎么突然变离线了」在生产 info 级零痕迹,连 LOG_LEVEL=debug 都拿不到名单。
        """
        logger = plog.get()
        if d.pod_mismatch_total > 0:
            logger.warning(
                "location_refresh_pod_mismatch",
                hub_pod=hub_pod,
                mismatch=d.pod_mismatch_total,
                sample_player_ids=d.pod_mismatch_sample,
                hint="本台 Hub census 有人、投影却指向别台:迁移过渡拍偶发正常,"
                "持续出现查双 hub/迟到 census",
            )
        if d.missing_total > 0:
            ok, streak = self._refresh_diff_log.admit(now_ms, REFRESH_DIFF_WINDOW_MS)
            if ok:
                logger.warning(
                    "location_refresh_projection_missing",
                    hub_pod=hub_pod,
                    missing=d.missing_total,
                    sample_player_ids=d.missing_sample,
                    other_state=d.other_state,
                    read_failed=d.read_failed,
                    streak_batches=streak,
                    hint="玩家连在 Hub 上但位置投影已蒸发(好友视角离线):"
                    "典型为撮合失败后 MATCHING 到期无人重建",
                )
            return
        n, _extra = self._refresh_diff_log.recovered()
        if n > 0:
            logger.info(
                "location_refresh_projection_recovered", hub_pod=hub_pod, degraded_batches=n
            )

    async def shrink_hub_ttl(
        self, hub_pod: str, player_id: int, fence: HubPresenceFence, grace_sec: float
    ) -> tuple[bool, bool]:
        """快速断线上报:守卫通过后把剩余 TTL 缩到 grace。

        PEXPIRE LT 语义(Redis 7):仅当新 TTL 小于当前剩余 TTL 才生效 —— 只缩不涨,
        重复上报天然幂等。返回 (accepted, shrunk):
        身份匹配但 TTL 已更短 → accepted=True / shrunk=False,仍允许补写 meta。
        """
        if not hub_pod or player_id == 0 or not fence.is_complete():
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "hub_pod, player_id and complete fence required"
            )
        try:
            result = await SHRINK_HUB_TTL_SCRIPT(
                self._rdb,
                keys=[loc_key(player_id)],
                args=[hub_pod, fence.assignment_id, fence.admission_id,
                      fence.admission_seq, int(grace_sec * 1000)],
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "redis shrink hub ttl: %s", exc
            ) from exc
        code = _parse_int(result)
        return code in (1, 2), code == 1

    async def record_last_seen(
        self, player_id: int, fence: HubPresenceFence, at_ms: int, retention_sec: float
    ) -> tuple[bool, int]:
        """精确记录该连接首次离开时刻;重复调用返回第一次的时刻(不后移)。"""
        if player_id == 0 or not fence.is_complete() or at_ms <= 0 or retention_sec <= 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "valid player, fence, timestamp and retention required"
            )
        try:
            effective = await RECORD_LAST_SEEN_SCRIPT(
                self._rdb,
                keys=[hub_meta_key(player_id)],
                args=[fence.assignment_id, fence.admission_id, fence.admission_seq,
                      at_ms, int(retention_sec * 1000)],
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "redis record last seen: %s", exc
            ) from exc
        effective_ms = _parse_int(effective)
        return effective_ms > 0, effective_ms

    async def touch_alive(self, player_id: int, at_ms: int, retention_sec: float) -> None:
        """推进 last_alive_ms(带节流)。给**非 HUB 在线状态**(BATTLE/MATCHING)用。

        它们走 SetLocation 而不是 RefreshHubLocations,少了这条 meta 会停在 Hub 阶段
        ——「打完一局直接退游戏」的离线时刻会停留在很早的 Hub 阶段(判定偏早)。

        best-effort:meta 不存在(从没走过 fenced 路径)时是 no-op,不会凭空建毒 key。
        """
        if player_id == 0 or at_ms <= 0 or retention_sec <= 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "valid player, timestamp and retention required"
            )
        try:
            await TOUCH_HUB_ALIVE_SCRIPT(
                self._rdb,
                keys=[hub_meta_key(player_id)],
                args=[at_ms, int(retention_sec * 1000), ALIVE_TOUCH_THROTTLE_SEC * 1000],
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "redis touch alive: %s", exc
            ) from exc

    async def delete(self, player_id: int) -> None:
        """UNLINK(异步删,避免大 key 阻塞)。TTL 已经在 set 时挂了,Delete 失败不致命。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "playerID must > 0")
        try:
            await self._rdb.unlink(loc_key(player_id))
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "redis location del: %s", exc
            ) from exc
