"""单玩家 active/PENDING 拍卖订单配额(§9.18 的 auction 行)——
对应 Go 侧 internal/data/owner_slot_limiter.go。

Redis SET 是**跨 market、跨 MySQL 分片**的原子计数索引;MySQL 订单状态仍是
"某个成员能不能被清掉"的权威依据。两者的分工不能反过来:
拿 Redis 当权威会让一次 Redis 故障直接放开配额,拿 MySQL 做计数则跨分片没法原子。

★ 两段 Lua **原样搬自 Go,一个字都没改**(pandorapy/redisx.py 头注释的迁移铁律):
  Lua 在 Redis 服务端执行,与调用方语言无关。原样搬过来,需要重新验证的只剩
  "参数传对没有";借迁移之机用 Python 重写成几条命令 = 把一个已在生产跑过的
  原子操作重新实现一遍,每一次都是新的竞态入口。

member 编码 `%010d:%020d`(market_id:order_id)也是契约:market_id 必须随 order_id
存进成员,分库模式下惰性清理才知道该去哪个 MySQL 分片回查权威状态。
只存 order_id 的话,两分片部署下清理会去错库、查无此单 → 按"不存在"释放名额,
而那张单其实还活着 —— 配额被穿透且不报错。
"""

from __future__ import annotations

import dataclasses

from pandorapy import errcode
from pandorapy import redisx


@dataclasses.dataclass(frozen=True, slots=True)
class OwnerOrderSlot:
    """唯一定位玩家的一张订单。"""

    market_id: int
    order_id: int


def owner_slot_key(owner_id: int) -> str:
    """ownerID hashtag 让该玩家的配额操作固定在一个 Redis Cluster slot。"""
    return f"pandora:auction:owner-slots:{{{owner_id}}}"


def owner_slot_member(slot: OwnerOrderSlot) -> str:
    return f"{slot.market_id:010d}:{slot.order_id:020d}"


def parse_owner_slot_member(member: str) -> OwnerOrderSlot:
    parts = member.split(":")
    if len(parts) != 2:
        raise ValueError(f"invalid owner slot member {member!r}")
    market_id = int(parts[0])
    order_id = int(parts[1])
    if market_id == 0:
        raise ValueError(f"invalid owner slot market {member!r}")
    if order_id == 0:
        raise ValueError(f"invalid owner slot order {member!r}")
    return OwnerOrderSlot(market_id=market_id, order_id=order_id)


# 已有成员幂等成功;否则只有 SCARD < hard max 才 SADD。
# SCARD/SADD 在同一 Lua 脚本内原子执行,多实例并发不会突破上限。
_RESERVE_SCRIPT = redisx.LuaScript(
    name="auction_reserve_owner_slot",
    body="""
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then
  return 1
end
if redis.call('SCARD', KEYS[1]) >= tonumber(ARGV[2]) then
  return 0
end
redis.call('SADD', KEYS[1], ARGV[1])
return 1""",
)

# 把权威库读出的 legacy 活跃成员按传入顺序补入同一个 SET。
# 允许保留成功前缀:当权威活跃数已超过 max 时,SET 会被填满但绝不会越界,
# 后续新写因此稳定失败。SISMEMBER 使重试和并发预热幂等。
_SYNC_SCRIPT = redisx.LuaScript(
    name="auction_sync_owner_slots",
    body="""
local max_slots = tonumber(ARGV[1])
for i = 2, #ARGV do
  if redis.call('SISMEMBER', KEYS[1], ARGV[i]) == 0 then
    if redis.call('SCARD', KEYS[1]) >= max_slots then
      return 0
    end
    redis.call('SADD', KEYS[1], ARGV[i])
  end
end
return 1""",
)


class RedisOwnerSlotLimiter:
    """Redis SET 实现的单玩家订单配额索引。"""

    __slots__ = ("_rdb",)

    def __init__(self, rdb) -> None:  # noqa: ANN001
        self._rdb = rdb

    async def reserve(self, owner_id: int, slot: OwnerOrderSlot, max_slots: int) -> bool:
        if owner_id == 0 or slot.market_id == 0 or slot.order_id == 0 or max_slots <= 0:
            raise errcode.PandoraError(
                errcode.ErrInternal, "invalid owner slot reserve arguments"
            )
        try:
            result = await _RESERVE_SCRIPT(
                self._rdb,
                keys=[owner_slot_key(owner_id)],
                args=[owner_slot_member(slot), max_slots],
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "reserve owner slot owner=%d market=%d order=%d: %s",
                owner_id,
                slot.market_id,
                slot.order_id,
                exc,
            ) from exc
        return int(result) == 1

    async def sync(
        self, owner_id: int, slots: list[OwnerOrderSlot], max_slots: int
    ) -> bool:
        """按 slots 顺序把 MySQL 已有 PENDING/活跃订单补入配额 SET(单脚本原子)。

        ★ `len(slots) > max_slots + 1` 直接判非法,与 Go 同:调用方只允许传
        "上限 + 1"条(多读一条用于判定是否已经触顶)。放开这个判据会让一次
        Sync 把任意多个成员塞进脚本,脚本执行时间随之无界 —— Redis 是单线程,
        一条慢脚本会把**整个实例**卡住。
        """
        if owner_id == 0 or max_slots <= 0 or len(slots) > max_slots + 1:
            raise errcode.PandoraError(errcode.ErrInternal, "invalid owner slot sync arguments")
        args: list[object] = [max_slots]
        for slot in slots:
            if slot.market_id == 0 or slot.order_id == 0:
                raise errcode.PandoraError(
                    errcode.ErrInternal, "invalid owner slot sync member"
                )
            args.append(owner_slot_member(slot))
        try:
            result = await _SYNC_SCRIPT(self._rdb, keys=[owner_slot_key(owner_id)], args=args)
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "sync owner slots owner=%d: %s", owner_id, exc
            ) from exc
        return int(result) == 1

    async def release(self, owner_id: int, slot: OwnerOrderSlot) -> None:
        try:
            await self._rdb.srem(owner_slot_key(owner_id), owner_slot_member(slot))
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "release owner slot owner=%d market=%d order=%d: %s",
                owner_id,
                slot.market_id,
                slot.order_id,
                exc,
            ) from exc

    async def list(self, owner_id: int, limit: int) -> list[OwnerOrderSlot]:
        """最多返回 limit 个成员,供配额满时按 MySQL 权威状态做**有界**惰性清理。

        ★ 用 SSCAN 而不是 SMEMBERS:成员数被 max_slots 兜住(200),但一个被
        Redis 故障或 bug 撑大的 SET 会让 SMEMBERS 一次拉回全部成员并卡住实例。
        """
        if owner_id == 0 or limit <= 0:
            raise errcode.PandoraError(errcode.ErrInternal, "invalid owner slot list arguments")
        out: list[OwnerOrderSlot] = []
        cursor = 0
        key = owner_slot_key(owner_id)
        while len(out) < limit:
            try:
                cursor, members = await self._rdb.sscan(
                    key, cursor=cursor, match="*", count=limit - len(out)
                )
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "list owner slots owner=%d: %s", owner_id, exc
                ) from exc
            for member in members:
                text = member if isinstance(member, str) else bytes(member).decode()
                try:
                    out.append(parse_owner_slot_member(text))
                except ValueError as exc:
                    raise errcode.PandoraError(
                        errcode.ErrInternal, "list owner slots owner=%d: %s", owner_id, exc
                    ) from exc
                if len(out) == limit:
                    break
            if cursor == 0:
                break
        return out
