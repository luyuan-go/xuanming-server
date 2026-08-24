"""trade 数据层(订单存 Redis)—— 对应 Go 侧 internal/data/trade_repo.go。

Redis key 模板(与 Go 侧**逐字一致** —— 迁移期两个实现读写同一批 key):

    pandora:trade:order:{%d}   → protobuf bytes(trade/v1.Order)
                                 hashtag {} 确保同订单的 key 落同一 cluster slot
    pandora:trade:player:%d    → set(成员是 order_id 文本),供 ListMyOrders 反查

订单主体直接存 proto 序列化 bytes:Order 已是完整的客户端可见结构且无服务端独有字段,
故存储 / 视图同构,不另造 OrderStorageRecord(§5.10 仅在有存储独有字段时才强制分离)。

状态机写用 WATCH/MULTI/EXEC 乐观锁。redis-py 的映射:
    Go:  rdb.Watch(ctx, func(tx *redis.Tx) error {...}, key)   冲突 → redis.TxFailedErr
    Py:  async with client.pipeline() as pipe:
             await pipe.watch(key)      # 进入 immediate 模式,可直接读
             ...
             pipe.multi()               # 转入排队模式
             pipe.set(...)
             await pipe.execute()       # 冲突 → WatchError
语义等价。**关键**:fn 自己返回的业务错误必须透传不重试(否则一个"状态不对"的
拒绝会被当成锁冲突重试 3 次,把业务语义变成偶发的锁失败)。
"""

from __future__ import annotations

import asyncio

import contextlib
import datetime as _dt
from typing import Awaitable, Callable, Protocol

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.common.v1 import currency_pb2
from pandora.inventory.v1 import inventory_pb2, inventory_pb2_grpc
from pandora.trade.v1 import trade_pb2
from redis.asyncio.client import Redis
from redis.exceptions import WatchError

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import redisx


def order_key(order_id: int) -> str:
    """与 Go 侧 orderKey 逐字一致。hashtag 括住 orderID 保 cluster slot 一致。"""
    return f"pandora:trade:order:{{{order_id}}}"


def player_key(player_id: int) -> str:
    """与 Go 侧 playerKey 逐字一致。"""
    return f"pandora:trade:player:{player_id}"


# 配额预留脚本 —— **从 Go 侧原样搬过来,一个字符没改**。
#
# 这正是 redisx 模块头注释里说的策略:Lua 在 Redis 服务端执行,与调用方语言无关。
# 原样搬等于把这段原子逻辑的正确性风险降到接近零;若"顺手用 Python 重写成
# SISMEMBER + SCARD + SADD 三条命令",就引入了一个全新的竞态窗口。
_RESERVE_ORDER_SLOT = redisx.LuaScript(
    name="trade_reserve_order_slot",
    body="""
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
  return 1
end
if redis.call('SCARD', KEYS[1]) >= tonumber(ARGV[2]) then
  return 0
end
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1""",
)


class TradeRepo(Protocol):
    """数据层抽象。biz 只依赖此协议,不依赖 redis。"""

    async def create_order(self, order, order_ttl: _dt.timedelta) -> None: ...
    async def delete_order(self, order_id: int) -> None: ...
    async def reserve_order_slot(
        self, player_id: int, order_id: int, max_orders: int, ttl: _dt.timedelta
    ) -> bool: ...
    async def release_order_slot(self, player_id: int, order_id: int) -> None: ...
    async def get_order(self, order_id: int): ...
    async def update_with_lock(
        self,
        order_id: int,
        max_retry: int,
        fn: Callable[[object], Awaitable[None] | None],
        order_ttl: _dt.timedelta,
    ) -> None: ...
    async def list_player_order_ids(self, player_id: int) -> list[int]: ...


class RedisTradeRepo:
    """基于 redis-py async 的 TradeRepo 实现。"""

    __slots__ = ("_rdb",)

    def __init__(self, rdb: Redis) -> None:
        self._rdb = rdb

    async def create_order(self, order, order_ttl: _dt.timedelta) -> None:
        """只写订单主体。**不写反查索引** —— 那由 biz 在主体落地后原子预留。

        写序铁律(镜像 team/matchmaker 的结论):先写主体、后预留索引名额。
        主体先落地时 order_id 是新发 snowflake、无人引用,天然安全;由此
        「索引成员指向 X 而 X 主体不在」≡ 真死成员,配额清理绝不会误删 in-flight 预留。
        """
        payload = order.SerializeToString()
        try:
            await self._rdb.set(
                order_key(order.order_id), payload, px=_ms(order_ttl)
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "create order %d: %s", order.order_id, exc
            ) from exc

    async def delete_order(self, order_id: int) -> None:
        """无条件删主体。仅供 CreateOrder 回滚(配额预留失败)使用 ——
        回滚时 order_id 尚未对外返回、反查索引未建,无条件 DEL 安全。"""
        await self._rdb.delete(order_key(order_id))

    async def reserve_order_slot(
        self, player_id: int, order_id: int, max_orders: int, ttl: _dt.timedelta
    ) -> bool:
        """原子预留反查索引名额。返回 False = 已满(不变量 §18)。幂等:成员已在直接成功。"""
        try:
            result = await _RESERVE_ORDER_SLOT(
                self._rdb,
                keys=[player_key(player_id)],
                args=[str(order_id), max_orders, _ms(ttl)],
            )
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "reserve order slot player %d order %d: %s",
                player_id,
                order_id,
                exc,
            ) from exc
        return int(result) == 1

    async def release_order_slot(self, player_id: int, order_id: int) -> None:
        """SREM,幂等。"""
        await self._rdb.srem(player_key(player_id), str(order_id))

    async def get_order(self, order_id: int):
        """读订单。不存在返回 None(对应 Go 的 (nil, false, nil))。"""
        try:
            raw = await self._rdb.get(order_key(order_id))
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "get order %d: %s", order_id, exc
            ) from exc
        if raw is None:
            return None
        order = trade_pb2.Order()
        try:
            order.ParseFromString(raw)
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "unmarshal order %d: %s", order_id, exc
            ) from exc
        return order

    async def update_with_lock(
        self,
        order_id: int,
        max_retry: int,
        fn: Callable[[object], Awaitable[None] | None],
        order_ttl: _dt.timedelta,
    ) -> None:
        """WATCH/MULTI/EXEC 读-改-写。

        错误分流必须与 Go 侧完全一致,三种情况三种处理:
          1. fn 返回业务错误   → **透传不重试**
          2. WATCH 冲突        → 重试,耗尽返 ErrTradeLockFailed(7005)
          3. 其他 redis 错误   → 不重试,直接抛

        第 1 条最关键:若把业务拒绝(状态不对、无权限)也当锁冲突重试,
        调用方看到的会是偶发的 ErrTradeLockFailed 而不是真实原因。
        """
        key = order_key(order_id)

        for _attempt in range(max_retry + 1):
            business_error: BaseException | None = None
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    raw = await pipe.get(key)  # watch 后处于 immediate 模式,可直接读
                    if raw is None:
                        raise errcode.PandoraError(
                            errcode.ErrTradeOrderNotFound, "order %d not found", order_id
                        )
                    order = trade_pb2.Order()
                    order.ParseFromString(raw)

                    try:
                        maybe = fn(order)
                        if maybe is not None and hasattr(maybe, "__await__"):
                            await maybe
                    except BaseException as exc:
                        business_error = exc
                        raise

                    payload = order.SerializeToString()
                    pipe.multi()
                    pipe.set(key, payload, px=_ms(order_ttl))
                    await pipe.execute()
                return
            except WatchError:
                continue  # 并发改动,重试
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:
                if business_error is not None and exc is business_error:
                    raise  # fn 的业务错误:透传不重试
                raise  # 其他错误(含 order not found):不重试

        # 乐观锁重试耗尽:热点订单锁竞争。经 in-band 码返回会被 access log 记成
        # 泛化失败、无法与其它失败区分 → WARN 带 order_id + 竞争强度。
        plog.get().warning(
            "trade_update_lock_exhausted", order_id=order_id, max_retry=max_retry
        )
        raise errcode.PandoraError(
            errcode.ErrTradeLockFailed,
            "order %d update concurrent retry exhausted",
            order_id,
        )

    async def list_player_order_ids(self, player_id: int) -> list[int]:
        """读玩家 order set 全部成员。集合大小被 reserve 的 max 硬上限兜住(默认 200)。"""
        try:
            members = await self._rdb.smembers(player_key(player_id))
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "list player orders %d: %s", player_id, exc
            ) from exc
        ids: list[int] = []
        for member in members:
            text = member.decode() if isinstance(member, bytes) else str(member)
            try:
                ids.append(int(text))
            except ValueError:
                continue  # 跳过脏成员(与 Go 一致)
        return ids


def _ms(td: _dt.timedelta) -> int:
    """timedelta → 毫秒整数(Redis PX / PEXPIRE 参数)。"""
    return int(td.total_seconds() * 1000)


# ── 结算账本:直连 inventory 的 P2P 原子对转 ────────────────────────────────
#
# 对应 Go 侧 internal/data/settlement_client.go。迁移期 inventory **仍是 Go 服务**,
# Python trade 直接拨它 —— strangler 迁移的正常形态:被迁的服务换实现,它的下游不动。
#
# 与 Go 逐条对齐的地方(每条改了都不报错):
#   - insecure 明文:内网直连,没有 JWT ⇒ inventory 侧 callerID==0,
#     SettlePlayerTrade 是系统接口只认内网直连。加了 TLS 反而连不上。
#   - 单次 RPC 15s deadline:Go 由 grpcclient.DefaultTimeout 经 Kratos middleware 施加。
#     不设 deadline 的话 inventory 卡住会把 ConfirmOrder 挂到客户端超时,而订单
#     停在 SELLER_CONFIRMED —— 玩家看到的是"确认没反应",查不到这里。
#   - 道具一律用 item_config_id 对齐 inventory 的可堆叠模型。

# 与 Go 侧 grpcclient.DefaultTimeout 同值(pkg/grpcclient/client.go)。
INVENTORY_RPC_TIMEOUT_SEC = 15.0


class GrpcResourceLedger:
    """用 inventory 服务 gRPC client 实现 biz.ResourceLedger。

    ★ 刻意**不**在构造时阻塞等连接就绪(不调 channel_ready())。Go 侧
    `grpcclient.MustDialInsecure` 走的是非阻塞 DialContext,只有 target 本身
    非法才失败;若 Python 这边改成"连不上就拒启",就凭空造出一条 Go 没有的
    启动顺序依赖 —— inventory 比 trade 晚起几秒(k8s 滚动更新里是常态)就会让
    trade CrashLoop,而 Go 版同样的部署顺序完全正常。方向必须与 Go 相同。
    结算路径本身的不可达由 biz._drive_settlement 按"瞬时失败可重试"处置。
    """

    __slots__ = ("_channel", "_stub", "_addr")

    def __init__(self, inventory_addr: str, *, channel=None) -> None:  # noqa: ANN001
        self._addr = inventory_addr
        # channel 参数供测试注入(与 kafkax 的 producer_factory 同一意图)。
        self._channel = channel if channel is not None else grpc.aio.insecure_channel(
            inventory_addr
        )
        self._stub = inventory_pb2_grpc.InventoryServiceStub(self._channel)

    async def close(self) -> None:
        """关闭底层连接。对应 Go 的 Close。"""
        with contextlib.suppress(Exception):
            await self._channel.close()

    async def settle(self, order, idempotency_key: int) -> None:
        """调 inventory.SettlePlayerTrade 完成本笔交易的资产对转(幂等键 = order_id)。

        三条返回路径与 Go 逐条一致,**分流错了后果不对称**:
          - OK                          → 成功(含幂等回放)
          - ERR_INVENTORY_INSUFFICIENT  → ErrTradeInsufficient:资产**未动**,
                                          biz 据此把订单置 FAILED 终态
          - 其它非 OK code               → 原样透传该码(便于上游定位)
          - 传输错误(AioRpcError)        → 原样抛:结算**可能已生效**,biz 据此
                                          把订单留在 SELLER_CONFIRMED 等重试收敛

        若把最后一类误当成 INSUFFICIENT,订单会被置 FAILED 而资产可能已经过户
        (钱货两清却显示交易失败);反过来把 INSUFFICIENT 当成瞬时错误,则是让
        一笔注定失败的订单永远卡在"请重试"。
        """
        req = inventory_pb2.SettlePlayerTradeRequest(
            order_id=idempotency_key,
            seller_id=order.seller_id,
            buyer_id=order.buyer_id,
            seller_items=_to_item_grants(order.items),
            buyer_items=_to_item_grants(order.buyer_items),
            # P2P 交易当前只用金币计价。显式给 kind:inventory 对 UNSPECIFIED 一律
            # fail-closed,**不会**回退成金币(currency.proto)。
            # price=0 的纯物物交换也带 kind,语义更清楚。
            price_amount=currency_pb2.CurrencyAmount(
                kind=currency_pb2.CURRENCY_KIND_GOLD, amount=order.price
            ),
        )
        # 传输层异常(AioRpcError / 超时)在这里**不捕获**,原样上抛 —— 见 docstring。
        resp = await self._stub.SettlePlayerTrade(req, timeout=INVENTORY_RPC_TIMEOUT_SEC)

        code = int(resp.code)
        if code == errcode_pb2.OK:
            return
        if code == errcode_pb2.ERR_INVENTORY_INSUFFICIENT:
            raise errcode.PandoraError(
                errcode.ErrTradeInsufficient,
                "trade settle insufficient order=%d seller=%d buyer=%d",
                idempotency_key,
                order.seller_id,
                order.buyer_id,
            )
        # inventory 返回未预期结算码(非 OK / 非 INSUFFICIENT):可能是**永久**错误
        # (如 INVALID_ARG),但上游 _drive_settlement 会把它当"瞬时可重试" →
        # 客户端无限重试 Confirm。独立 WARN 以便把这类死循环从正常重试里区分出来。
        plog.get().warning(
            "trade_settle_unexpected_code",
            order_id=idempotency_key,
            code=code,
            seller_id=order.seller_id,
            buyer_id=order.buyer_id,
        )
        raise errcode.PandoraError(
            code, "trade settle failed order=%d code=%d", idempotency_key, code
        )


def _to_item_grants(items) -> list:  # noqa: ANN001
    """trade 道具(item_config_id + count)→ inventory.ItemGrant。对应 Go 的 toItemGrants。"""
    return [
        inventory_pb2.ItemGrant(item_config_id=it.item_config_id, count=it.count)
        for it in items
    ]
