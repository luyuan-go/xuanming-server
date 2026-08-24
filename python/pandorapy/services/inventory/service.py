"""inventory gRPC service 层(pandora.inventory.v1)—— 对应 Go 侧 internal/service/。

职责:proto ↔ 内部结构互转、错误码映射、**鉴权边界**。

★ 鉴权边界(2026-06-17 安全审查修复)有两种、方向相反,逐个 RPC 对着 Go 抄,
  不许统一成一种:

    客户端 RPC(GetInventory / UseItem / SellItem / DiscardItem / IdentifyItem /
               DiscardInstance / SellInstance / MoveInstance)
        以 Envoy jwt_authn 注入的**调用者身份**为准,不信任请求体 player_id;
        callerID==0(内网直连、无 JWT)→ ERR_UNAUTHORIZED;
        请求体 player_id 与调用者不一致 → ERR_PERMISSION_DENY。
        —— 防伪造 player_id 读 / 用 / 卖他人背包。

    系统 RPC(GrantItems / GrantInstances / Consume|DiscardBattleItem / 两种结算 /
             escrow 三件套 / transfer 四件套 / CheckItemsOwned / CheckInstancesOwned)
        只允许后端内部直连(**无** JWT,callerID==0);带玩家 JWT 的调用一律拒。
        —— 杜绝玩家自助发道具 / 自助结算套现 / 探测他人背包。
        路由层另有 Envoy 精确 403 兜底,双保险。

★ 业务失败一律走 **in-band code**(返回 response.code,gRPC status 保持 OK),
  与 Go 侧逐字一致 —— 调用方按 code 分支,不靠 gRPC status 猜。
  Go 侧本服务的每一个 RPC 都是这个形状,没有例外。

★ 每处宽 except 之前必须先放行 asyncio.CancelledError:
  它在 3.8+ 是 BaseException,而 grpc.aio 正是**用取消**终止在途 handler。
  吞掉 = 停机时把取消映射成业务错误码返回一个正常响应,客户端每次滚动更新
  都会收到一批假失败。
"""

from __future__ import annotations

import asyncio

from pandora.bag.v1 import bag_pb2
from pandora.common.v1 import errcode_pb2 as commonpb
from pandora.inventory.v1 import inventory_pb2 as pb
from pandora.inventory.v1 import inventory_pb2_grpc as pbgrpc

from pandorapy import errcode
from pandorapy import interceptors as pintercept
from pandorapy import log as plog
from pandorapy.services.inventory import biz as ibiz
from pandorapy.services.inventory import currency_biz as cbiz
from pandorapy.services.inventory.models import (
    InstanceOwnershipQuery,
    ItemGrant,
    ItemInstance,
    TransferClaimItem,
)

GRPC_SERVICE_FULL_NAME = "pandora.inventory.v1.InventoryService"


def _code_of(exc: BaseException) -> int:
    """内部异常 → in-band ErrCode(与 Go 的 errcode.As 同口径)。"""
    if isinstance(exc, errcode.PandoraError):
        return exc.code
    return errcode.ErrInternal


def _to_proto_instance(inst: ItemInstance) -> pb.ItemInstance:
    """内部实例 → proto。slot_index=-1 原样保留,客户端据此识别未分配格。"""
    return pb.ItemInstance(
        instance_id=inst.instance_id,
        item_config_id=inst.item_config_id,
        identified=inst.identified,
        attributes=[
            pb.ItemAttribute(attr_id=a.attr_id, value=a.value) for a in inst.attributes
        ],
        slot_index=inst.slot_index,
        bound=inst.bound,
    )


def _balance_of(balances, kind: int) -> int:  # noqa: ANN001
    """从余额快照取某币种(缺项 = 0)。

    下行只带**本次涉及的那一种**币种余额(与 Go 的
    `outcome.Balances.Get(outcome.Kind)` 同口径):出售 / 购买响应回答的是
    "这笔之后你这种钱还有多少",不是"你全部资产快照"——后者要另调 GetInventory。
    """
    return int((balances or {}).get(kind, 0))


def _to_bag_item(row) -> bag_pb2.BagItem:  # noqa: ANN001 —— models.EscrowedInstance
    """托管快照 → BagItem(TransferAttachment.item 形状:count 恒 1,slot 无意义留 0)。"""
    return bag_pb2.BagItem(
        item_config_id=row.item_config_id,
        count=1,
        instance_id=row.instance_id,
        identified=row.identified,
        attrs=[
            bag_pb2.BagItemAttribute(attr_id=a.attr_id, value=a.value) for a in row.attributes
        ],
    )


class InventoryService(pbgrpc.InventoryServiceServicer):
    """实现 pandora.inventory.v1.InventoryService 的 23 个 RPC。"""

    def __init__(self, uc: ibiz.InventoryUsecase) -> None:
        self._uc = uc

    # ── 鉴权辅助 ──────────────────────────────────────────────────────────

    @staticmethod
    def _caller_player_id(context, req_player_id: int) -> tuple[int, int]:  # noqa: ANN001
        """客户端 RPC 的身份闸。返回 (权威 player_id, err_code);err_code==OK 才可用。

        权威 player_id **恒等于调用者身份**,后续业务一律用它,不信任 req.player_id。
        请求体带了不同的 player_id 就直接拒(而不是"忽略它") —— 忽略会让一次
        越权尝试悄悄变成一次正常的自查,攻击面上完全看不见。
        """
        caller_id = pintercept.extract_player_id(context)
        if caller_id == 0:
            return 0, commonpb.ERR_UNAUTHORIZED
        if req_player_id != 0 and req_player_id != caller_id:
            return 0, commonpb.ERR_PERMISSION_DENY
        return caller_id, commonpb.OK

    @staticmethod
    def _reject_client_caller(context) -> bool:  # noqa: ANN001
        """系统 RPC 的身份闸:带玩家 JWT(caller_id>0)的调用一律拒。"""
        return pintercept.extract_player_id(context) != 0

    @staticmethod
    def _log_client_caller_denied(context, rpc: str, req_player_id: int) -> None:  # noqa: ANN001
        """记录一次玩家 JWT 直敲内部接口的拒绝。

        ERR_PERMISSION_DENY 是 in-band code,handler 不抛错 → access log 只记 DEBUG。
        稳态下本日志应恒 0(Envoy 已在前置 403);一旦出现就是「Envoy 拦截漏了」或
        「内部调用方错把玩家上下文透传进来了」,必须可见。
        """
        plog.get().warning(
            "inventory_caller_denied",
            rpc=rpc,
            reason="client_caller_on_internal_rpc",
            caller_player_id=pintercept.extract_player_id(context),
            player_id=req_player_id,
        )

    # ── 背包读 ────────────────────────────────────────────────────────────

    async def GetInventory(self, request, context):  # noqa: N802
        player_id, code = self._caller_player_id(context, request.player_id)
        if code != commonpb.OK:
            return pb.GetInventoryResponse(code=code)
        try:
            balances, items, capacity, instances = await self._uc.get_inventory_full(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.GetInventoryResponse(code=_code_of(exc))
        return pb.GetInventoryResponse(
            code=commonpb.OK,
            inventory=pb.Inventory(
                player_id=player_id,
                currencies=cbiz.balances_to_proto(balances),
                items=[
                    pb.ItemStack(item_config_id=it.item_config_id, count=it.count)
                    for it in items
                ],
                capacity=capacity,
                instances=[_to_proto_instance(i) for i in instances],
            ),
        )

    # ── 堆叠道具 ──────────────────────────────────────────────────────────

    async def GrantItems(self, request, context):  # noqa: N802
        """幂等发放道具 + 货币(系统接口,仅后端内部可调)。"""
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "GrantItems", request.player_id)
            return pb.GrantItemsResponse(code=commonpb.ERR_PERMISSION_DENY)
        if request.player_id == 0:
            return pb.GrantItemsResponse(code=commonpb.ERR_INVALID_ARG)
        items = [
            ItemGrant(item_config_id=it.item_config_id, count=it.count) for it in request.items
        ]
        try:
            # balances_from_proto 是**唯一**的类型边界:币种未知 / 数量为 0 / 同币种重复
            # 都在这里拒。重复币种必须拒而不是相加 —— "相加"和"取后者"都是猜,猜错就是发错钱。
            currencies = cbiz.balances_from_proto(request.currencies)
            balances = await self._uc.grant_items(
                request.player_id, items, currencies, request.idempotency_key
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.GrantItemsResponse(code=_code_of(exc))
        return pb.GrantItemsResponse(
            code=commonpb.OK, currencies=cbiz.balances_to_proto(balances)
        )

    async def UseItem(self, request, context):  # noqa: N802
        player_id, code = self._caller_player_id(context, request.player_id)
        if code != commonpb.OK:
            return pb.UseItemResponse(code=code)
        try:
            remaining = await self._uc.use_item(
                player_id, request.item_config_id, request.count, request.idempotency_key
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.UseItemResponse(code=_code_of(exc))
        return pb.UseItemResponse(code=commonpb.OK, remaining=remaining)

    async def ConsumeBattleItem(self, request, context):  # noqa: N802
        """按可信战斗进度事实持久扣减局内消耗品(系统接口)。"""
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "ConsumeBattleItem", request.player_id)
            return pb.ConsumeBattleItemResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            remaining = await self._uc.consume_battle_item(
                request.player_id,
                request.item_config_id,
                request.count,
                request.idempotency_key,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.ConsumeBattleItemResponse(code=_code_of(exc))
        return pb.ConsumeBattleItemResponse(code=commonpb.OK, remaining=remaining)

    async def DiscardBattleItem(self, request, context):  # noqa: N802
        """按可信战斗进度事实持久丢弃堆叠物(系统接口)。"""
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "DiscardBattleItem", request.player_id)
            return pb.DiscardBattleItemResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            remaining = await self._uc.discard_battle_item(
                request.player_id,
                request.item_config_id,
                request.count,
                request.idempotency_key,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.DiscardBattleItemResponse(code=_code_of(exc))
        return pb.DiscardBattleItemResponse(code=commonpb.OK, remaining=remaining)

    async def SellItem(self, request, context):  # noqa: N802
        player_id, code = self._caller_player_id(context, request.player_id)
        if code != commonpb.OK:
            return pb.SellItemResponse(code=code)
        try:
            outcome = await self._uc.sell_item(
                player_id, request.item_config_id, request.count, request.idempotency_key
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.SellItemResponse(code=_code_of(exc))
        return pb.SellItemResponse(
            code=commonpb.OK,
            remaining=outcome.remaining,
            balance=cbiz.currency_amount_proto(
                outcome.kind, _balance_of(outcome.balances, outcome.kind)
            ),
            earned=cbiz.currency_amount_proto(outcome.kind, outcome.earned),
        )

    async def DiscardItem(self, request, context):  # noqa: N802
        player_id, code = self._caller_player_id(context, request.player_id)
        if code != commonpb.OK:
            return pb.DiscardItemResponse(code=code)
        try:
            remaining = await self._uc.discard_item(
                player_id, request.item_config_id, request.count, request.idempotency_key
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.DiscardItemResponse(code=_code_of(exc))
        return pb.DiscardItemResponse(code=commonpb.OK, remaining=remaining)

    # ── 装备实例 ──────────────────────────────────────────────────────────

    async def GrantInstances(self, request, context):  # noqa: N802
        """幂等发放装备实例(系统接口)。"""
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "GrantInstances", request.player_id)
            return pb.GrantInstancesResponse(code=commonpb.ERR_PERMISSION_DENY)
        if request.player_id == 0:
            return pb.GrantInstancesResponse(code=commonpb.ERR_INVALID_ARG)
        try:
            insts = await self._uc.grant_instances(
                request.player_id, list(request.item_config_ids), request.idempotency_key
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.GrantInstancesResponse(code=_code_of(exc))
        return pb.GrantInstancesResponse(
            code=commonpb.OK, instances=[_to_proto_instance(i) for i in insts]
        )

    async def IdentifyItem(self, request, context):  # noqa: N802
        player_id, code = self._caller_player_id(context, request.player_id)
        if code != commonpb.OK:
            return pb.IdentifyItemResponse(code=code)
        try:
            inst = await self._uc.identify_item(player_id, request.instance_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.IdentifyItemResponse(code=_code_of(exc))
        return pb.IdentifyItemResponse(code=commonpb.OK, instance=_to_proto_instance(inst))

    async def DiscardInstance(self, request, context):  # noqa: N802
        player_id, code = self._caller_player_id(context, request.player_id)
        if code != commonpb.OK:
            return pb.DiscardInstanceResponse(code=code)
        try:
            await self._uc.discard_instance(player_id, request.instance_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.DiscardInstanceResponse(code=_code_of(exc))
        return pb.DiscardInstanceResponse(code=commonpb.OK)

    async def MoveInstance(self, request, context):  # noqa: N802
        player_id, code = self._caller_player_id(context, request.player_id)
        if code != commonpb.OK:
            return pb.MoveInstanceResponse(code=code)
        try:
            await self._uc.move_instance(player_id, request.instance_id, request.to_slot_index)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.MoveInstanceResponse(code=_code_of(exc))
        return pb.MoveInstanceResponse(code=commonpb.OK)

    async def SellInstance(self, request, context):  # noqa: N802
        player_id, code = self._caller_player_id(context, request.player_id)
        if code != commonpb.OK:
            return pb.SellInstanceResponse(code=code)
        try:
            outcome = await self._uc.sell_instance(
                player_id,
                request.instance_id,
                request.item_config_id,
                request.idempotency_key,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.SellInstanceResponse(code=_code_of(exc))
        return pb.SellInstanceResponse(
            code=commonpb.OK,
            balance=cbiz.currency_amount_proto(
                outcome.kind, _balance_of(outcome.balances, outcome.kind)
            ),
            earned=cbiz.currency_amount_proto(outcome.kind, outcome.earned),
        )

    # ── NPC 商店(客户端接口)────────────────────────────────────────────

    async def GetShop(self, request, context):  # noqa: N802
        """读某个 NPC 商店的权威价目表。

        ★ 刻意**不校验调用者身份**:价目表本来就是要在客户端上展示的公开信息,
          加鉴权只会让 DS / 工具查价变麻烦。与 Go 侧 shop.go 的边界一致。
        客户端拿它渲染商品列表与单价,于是展示口径与扣费口径同源 ——
        旧的客户端本地商店从道具表读 SellPrice 当买入价,改表后两边会静默漂移(§17.3)。
        """
        try:
            entries = await self._uc.get_shop(request.shop_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.GetShopResponse(code=_code_of(exc))
        return pb.GetShopResponse(
            code=commonpb.OK,
            shop_id=request.shop_id,
            entries=[
                pb.ShopEntry(
                    item_config_id=e.item_config_id,
                    count_per_unit=e.count_per_unit,
                    currency_kind=e.currency_kind,
                    unit_price=e.unit_price,
                    sort_order=e.sort_order,
                )
                for e in entries
            ],
        )

    async def PurchaseShopItem(self, request, context):  # noqa: N802
        """向 NPC 商店购买道具(服务端权威扣费 + 入包,原子且幂等)。

        动玩家钱包 → 一律以 Envoy 注入的调用者身份为准,**不信任请求体 player_id**,
        防止替别人花钱 / 给自己刷货。
        """
        player_id, code = self._caller_player_id(context, request.player_id)
        if code != commonpb.OK:
            return pb.PurchaseShopItemResponse(code=code)
        try:
            outcome = await self._uc.purchase_shop_item(
                player_id,
                request.shop_id,
                request.item_config_id,
                request.unit_count,
                request.idempotency_key,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.PurchaseShopItemResponse(code=_code_of(exc))
        return pb.PurchaseShopItemResponse(
            code=commonpb.OK,
            balance=cbiz.currency_amount_proto(
                outcome.kind, _balance_of(outcome.balances, outcome.kind)
            ),
            cost=cbiz.currency_amount_proto(outcome.kind, outcome.cost),
            granted_items=[
                pb.ItemGrant(item_config_id=it.item_config_id, count=it.count)
                for it in outcome.items
            ],
            granted_instances=[_to_proto_instance(i) for i in outcome.instances],
        )

    # ── 拍卖托管 / 结算(系统接口)────────────────────────────────────────

    async def FreezeForOrder(self, request, context):  # noqa: N802
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "FreezeForOrder", request.player_id)
            return pb.FreezeForOrderResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            await self._uc.freeze_for_order(
                request.player_id,
                request.order_id,
                int(request.side),
                request.item_config_id,
                request.quantity,
                int(request.currency_kind),
                request.unit_price,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.FreezeForOrderResponse(code=_code_of(exc))
        return pb.FreezeForOrderResponse(code=commonpb.OK)

    async def EnsureAuctionEscrow(self, request, context):  # noqa: N802
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "EnsureAuctionEscrow", request.player_id)
            return pb.EnsureAuctionEscrowResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            await self._uc.ensure_auction_escrow(
                request.player_id,
                request.order_id,
                int(request.side),
                request.item_config_id,
                request.remaining_quantity,
                int(request.currency_kind),
                request.unit_price,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.EnsureAuctionEscrowResponse(code=_code_of(exc))
        return pb.EnsureAuctionEscrowResponse(code=commonpb.OK)

    async def SettleAuctionMatch(self, request, context):  # noqa: N802
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "SettleAuctionMatch", request.seller_id)
            return pb.SettleAuctionMatchResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            await self._uc.settle_auction_match(
                request.match_id,
                request.seller_id,
                request.buyer_id,
                request.sell_order_id,
                request.buy_order_id,
                request.item_config_id,
                request.quantity,
                int(request.currency_kind),
                request.unit_price,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.SettleAuctionMatchResponse(code=_code_of(exc))
        return pb.SettleAuctionMatchResponse(code=commonpb.OK)

    async def SettlePlayerTrade(self, request, context):  # noqa: N802
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "SettlePlayerTrade", request.seller_id)
            return pb.SettlePlayerTradeResponse(code=commonpb.ERR_PERMISSION_DENY)

        def _grants(items):  # noqa: ANN001
            return [ItemGrant(item_config_id=it.item_config_id, count=it.count) for it in items]

        try:
            await self._uc.settle_player_trade(
                request.order_id,
                request.seller_id,
                request.buyer_id,
                _grants(request.seller_items),
                _grants(request.buyer_items),
                int(request.price_amount.kind),
                int(request.price_amount.amount),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.SettlePlayerTradeResponse(code=_code_of(exc))
        return pb.SettlePlayerTradeResponse(code=commonpb.OK)

    async def ReleaseEscrow(self, request, context):  # noqa: N802
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "ReleaseEscrow", request.player_id)
            return pb.ReleaseEscrowResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            await self._uc.release_escrow(request.player_id, request.order_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.ReleaseEscrowResponse(code=_code_of(exc))
        return pb.ReleaseEscrowResponse(code=commonpb.OK)

    # ── 邮件 transfer 托管(系统接口)────────────────────────────────────

    async def EscrowOutInstances(self, request, context):  # noqa: N802
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "EscrowOutInstances", request.source_player_id)
            return pb.EscrowOutInstancesResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            rows = await self._uc.escrow_out_instances(
                request.source_player_id,
                request.to_player_id,
                list(request.instance_ids),
                request.escrow_key,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.EscrowOutInstancesResponse(code=_code_of(exc))
        return pb.EscrowOutInstancesResponse(
            code=commonpb.OK, items=[_to_bag_item(r) for r in rows]
        )

    async def ClaimTransferInstances(self, request, context):  # noqa: N802
        if self._reject_client_caller(context):
            self._log_client_caller_denied(
                context, "ClaimTransferInstances", request.to_player_id
            )
            return pb.ClaimTransferInstancesResponse(code=commonpb.ERR_PERMISSION_DENY)
        items = [
            TransferClaimItem(instance_id=it.instance_id, item_config_id=it.item_config_id)
            for it in request.items
        ]
        try:
            await self._uc.claim_transfer_instances(
                request.to_player_id, items, request.idempotency_key
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.ClaimTransferInstancesResponse(code=_code_of(exc))
        return pb.ClaimTransferInstancesResponse(code=commonpb.OK)

    async def ReleaseTransferEscrow(self, request, context):  # noqa: N802
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "ReleaseTransferEscrow", 0)
            return pb.ReleaseTransferEscrowResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            await self._uc.release_transfer_escrow(list(request.instance_ids))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.ReleaseTransferEscrowResponse(code=_code_of(exc))
        return pb.ReleaseTransferEscrowResponse(code=commonpb.OK)

    async def ConsumeTransferEscrow(self, request, context):  # noqa: N802
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "ConsumeTransferEscrow", request.to_player_id)
            return pb.ConsumeTransferEscrowResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            await self._uc.consume_transfer_escrow(
                request.to_player_id, list(request.instance_ids)
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.ConsumeTransferEscrowResponse(code=_code_of(exc))
        return pb.ConsumeTransferEscrowResponse(code=commonpb.OK)

    # ── 拥有权查询(系统接口)────────────────────────────────────────────

    async def CheckItemsOwned(self, request, context):  # noqa: N802
        """批量拥有权查询。调用方 = player 服务的 SetEquipment 校验。"""
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "CheckItemsOwned", request.player_id)
            return pb.CheckItemsOwnedResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            owned = await self._uc.check_items_owned(
                request.player_id, list(request.item_config_ids)
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.CheckItemsOwnedResponse(code=_code_of(exc))
        return pb.CheckItemsOwnedResponse(code=commonpb.OK, owned_item_config_ids=owned)

    async def CheckInstancesOwned(self, request, context):  # noqa: N802
        """精确实例拥有权查询(返回权威快照,供 player.GetLoadout 保真带到 DS)。"""
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "CheckInstancesOwned", request.player_id)
            return pb.CheckInstancesOwnedResponse(code=commonpb.ERR_PERMISSION_DENY)
        queries = [
            InstanceOwnershipQuery(instance_id=q.instance_id, item_config_id=q.item_config_id)
            for q in request.instances
        ]
        try:
            owned = await self._uc.check_instances_owned(request.player_id, queries)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return pb.CheckInstancesOwnedResponse(code=_code_of(exc))
        return pb.CheckInstancesOwnedResponse(
            code=commonpb.OK,
            owned_instance_ids=[i.instance_id for i in owned],
            owned_instances=[_to_proto_instance(i) for i in owned],
        )
