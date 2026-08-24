"""结算账本 —— 对应 Go 侧 internal/data/settlement_client.go + biz.NoopSettlementLedger。

三段式 escrow(挂单冻结 / 成交从 escrow 消费 / 撤单过期退还),消除"成交瞬间余额不足而失败":

    Freeze   挂单/出价时把资产冻进 escrow(SELL 冻道具 / BUY 冻金币),幂等键 = order_id
    Ensure   验证或补冻旧版本活跃单的剩余 escrow(仅后台 legacy 恢复调用)
    Settle   每笔撮合成交从双方 escrow 消费完成对转,幂等键 = match_id(资产只转一次)
    Release  撤单/过期/完全成交后退还 escrow 残余,幂等键 = order_id

★ 三个幂等键**各管一段,不能共用**。用 order_id 当结算幂等键会让同一挂单的第二笔
  成交被幂等吸收(买家收到货、卖家没收到钱);用 match_id 当冻结幂等键则每次重试
  都是新键 → 重复冻结。

★ 内网直连、无 JWT:inventory 侧 callerID==0 才允许调这几个系统 RPC。
  给它加上玩家 JWT 会被 inventory 当成"玩家自己来结算"而拒(ERR_PERMISSION_DENY)。
"""

from __future__ import annotations

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.common.v1 import currency_pb2
from pandora.inventory.v1 import inventory_pb2, inventory_pb2_grpc

from pandorapy import errcode
from pandorapy.services.auction.repo import MatchRecord


class NoopSettlementLedger:
    """占位实现:冻结 / 结算 / 退还都成功(**不真实扣转资产**)。

    只允许在 `auction.allow_noop_settlement=true` 时装配 —— main 里那道
    `settlement_ledger_missing` fail-fast 的全部意义,就是防止生产漏配
    inventory 地址后静默以"成交不结算"启动:玩家的道具会凭空出现和消失,
    而 auction 自己的日志一切正常。
    """

    __slots__ = ()

    async def freeze(
        self,
        *,
        owner_id: int,
        order_id: int,
        side: int,
        item_config_id: int,
        quantity: int,
        price: int,
    ) -> None:
        return None

    async def ensure(
        self,
        *,
        owner_id: int,
        order_id: int,
        side: int,
        item_config_id: int,
        remaining: int,
        price: int,
    ) -> None:
        return None

    async def settle(self, m: MatchRecord) -> None:
        return None

    async def release(self, owner_id: int, order_id: int) -> None:
        return None


class GrpcInventoryLedger:
    """用 inventory gRPC client 实现结算账本。"""

    __slots__ = ("_channel", "_stub")

    def __init__(self, inventory_addr: str) -> None:
        # 内网 insecure(与 Go 的 grpcclient.MustDialInsecure 同):
        # 这条链路不经 Envoy,mTLS 由网络边界负责。
        self._channel = grpc.aio.insecure_channel(inventory_addr)
        self._stub = inventory_pb2_grpc.InventoryServiceStub(self._channel)

    async def close(self) -> None:
        await self._channel.close()

    async def freeze(
        self,
        *,
        owner_id: int,
        order_id: int,
        side: int,
        item_config_id: int,
        quantity: int,
        price: int,
    ) -> None:
        resp = await self._stub.FreezeForOrder(
            inventory_pb2.FreezeForOrderRequest(
                player_id=owner_id,
                order_id=order_id,
                # ★ side 直接当 EscrowSide 传:两个枚举同号(SELL=1 / BUY=2)。
                # 错位的后果见 tests/test_auction_submit.py 那条断言 ——
                # 买单会去冻**道具**而不是金币,且冻结照样成功、不报错。
                side=side,
                item_config_id=item_config_id,
                quantity=quantity,
                unit_price=price,
                # 拍卖行当前只用金币计价。显式传而不是留 UNSPECIFIED:
                # inventory 侧对未知币种一律 fail-closed,**不会**回退成金币(currency.proto)。
                currency_kind=currency_pb2.CURRENCY_KIND_GOLD,
            )
        )
        _raise_unless_ok(
            resp.code,
            insufficient_msg=f"auction freeze insufficient player={owner_id} order={order_id}",
            failed_msg=f"auction freeze failed player={owner_id} order={order_id}",
        )

    async def ensure(
        self,
        *,
        owner_id: int,
        order_id: int,
        side: int,
        item_config_id: int,
        remaining: int,
        price: int,
    ) -> None:
        if remaining <= 0 or price <= 0:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "invalid ensure escrow remaining=%d price=%d",
                remaining,
                price,
            )
        resp = await self._stub.EnsureAuctionEscrow(
            inventory_pb2.EnsureAuctionEscrowRequest(
                player_id=owner_id,
                order_id=order_id,
                side=side,
                item_config_id=item_config_id,
                remaining_quantity=remaining,
                unit_price=price,
                currency_kind=currency_pb2.CURRENCY_KIND_GOLD,
            )
        )
        _raise_unless_ok(
            resp.code,
            insufficient_msg=(
                f"auction ensure escrow insufficient player={owner_id} order={order_id}"
            ),
            failed_msg=f"auction ensure escrow failed player={owner_id} order={order_id}",
        )

    async def settle(self, m: MatchRecord) -> None:
        resp = await self._stub.SettleAuctionMatch(
            inventory_pb2.SettleAuctionMatchRequest(
                match_id=m.match_id,
                seller_id=m.seller_id,
                buyer_id=m.buyer_id,
                sell_order_id=m.sell_order_id,
                buy_order_id=m.buy_order_id,
                item_config_id=m.item_config_id,
                quantity=m.quantity,
                # 成交价 = **被动挂单价**。传 incoming 的报价会让买家多付 / 卖家少收,
                # 而账目两边都平 —— 对不出账,只有玩家能察觉。
                unit_price=m.price,
                currency_kind=currency_pb2.CURRENCY_KIND_GOLD,
            )
        )
        _raise_unless_ok(
            resp.code,
            insufficient_msg=(
                f"auction settle insufficient match={m.match_id} "
                f"seller={m.seller_id} buyer={m.buyer_id}"
            ),
            failed_msg=f"auction settle failed match={m.match_id}",
        )

    async def release(self, owner_id: int, order_id: int) -> None:
        resp = await self._stub.ReleaseEscrow(
            inventory_pb2.ReleaseEscrowRequest(player_id=owner_id, order_id=order_id)
        )
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                int(resp.code),
                "auction release failed player=%d order=%d code=%d",
                owner_id,
                order_id,
                int(resp.code),
            )


def _raise_unless_ok(code: int, *, insufficient_msg: str, failed_msg: str) -> None:
    """把 inventory 的 code 翻成 auction 域的错误。

    ★ ERR_INVENTORY_INSUFFICIENT 必须映射成 ErrAuctionInsufficient,不能原样透传:
    biz 的 legacy 恢复路径按 `ErrAuctionInsufficient / ErrInventoryInsufficient /
    ErrInventoryIdempotencyConflict / ErrInvalidArg` 判定"确定性不一致 → 取消订单",
    其余错误一律当"暂时失败,下轮重试"。少了这条映射,资产真的不足的订单
    会被永远当成"暂时失败"重试下去,补偿链永不收敛。
    """
    if code == errcode_pb2.OK:
        return
    if code == errcode_pb2.ERR_INVENTORY_INSUFFICIENT:
        raise errcode.PandoraError(errcode.ErrAuctionInsufficient, "%s", insufficient_msg)
    raise errcode.PandoraError(int(code), "%s code=%d", failed_msg, int(code))
