"""Redis ZSET 订单簿**兼容缓存** —— 对应 Go 侧 internal/data/book.go。

★ 名字里的"兼容"是全部重点:MySQL 才是撮合候选与订单状态的权威。
  保留这份缓存只为滚动/回滚期间仍在运行的**旧版本实例**能看见新挂单。
  新实现**绝不**从这里选撮合候选 —— 旧 key 只含 market_id,无法区分同品类里的
  不同 item_config_id,拿它选单会导致跨物品成交、固定前缀饥饿,
  以及"缓存写失败/进程崩溃后订单永久不可见"。

key:pandora:auction:book:{<market_id>}:ask / :bid
  - hashtag `{<market_id>}` 把同一市场的买/卖盘锁到同一 Redis Cluster slot。
  - 卖盘 score = price(升序 → 最低价在前);买盘 score = -price(升序 → 最高价在前)。
  - member = 零填充 20 位 order_id;雪花 order_id 时序递增 → 同价按字典序 = 最早在前。

  ★ 这套编码是**跨版本契约**:改 key 前缀、改 score 正负、改 member 宽度,
    都会让旧实例读到一个空簿(或读到顺序反了的簿),而两边都不报错。
"""

from __future__ import annotations

from pandorapy import errcode
from pandorapy.services.auction.submit import SIDE_BUY


def book_key(market_id: int, side: int) -> str:
    suffix = "bid" if side == SIDE_BUY else "ask"
    return f"pandora:auction:book:{{{market_id}}}:{suffix}"


def score_of(side: int, price: int) -> float:
    """买盘取负分,升序即可拿到最高价。float64 对整数价的精度足够(与 Go 同)。"""
    return -float(price) if side == SIDE_BUY else float(price)


def member_of(order_id: int) -> str:
    return f"{order_id:020d}"


class RedisBookStore:
    """基于 ZSET 的订单簿兼容缓存。写失败只告警,不得影响权威撮合。"""

    __slots__ = ("_rdb",)

    def __init__(self, rdb) -> None:  # noqa: ANN001
        self._rdb = rdb

    async def add(self, market_id: int, side: int, order_id: int, price: int) -> None:
        try:
            await self._rdb.zadd(
                book_key(market_id, side), {member_of(order_id): score_of(side, price)}
            )
        except Exception as exc:  # noqa: BLE001 —— 缓存失败转成业务错误交调用方降级
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "book add market=%d order=%d: %s",
                market_id,
                order_id,
                exc,
            ) from exc

    async def remove(self, market_id: int, side: int, order_id: int) -> None:
        try:
            await self._rdb.zrem(book_key(market_id, side), member_of(order_id))
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "book remove market=%d order=%d: %s",
                market_id,
                order_id,
                exc,
            ) from exc
