"""mail → inventory 的 gRPC 客户端 —— 对应 Go 侧 internal/data/inventory_client.go。

内网 insecure 直连,不带 JWT(对齐 trade / auction 的 GrpcResourceLedger)。
四条路径各自的幂等键由 biz 生成并传入,inventory 侧按键去重 —— 所以重试 / 重领
在这里都是安全的,**不要**在本文件里再加一层"记住发过了"的缓存:
那份缓存进程重启就没了,而真正的幂等权威在 inventory 的流水表里。

★ 四条路径**语义完全不同**,混用就是资产事故:
    grant                 stack     按 config_id + count 计数入账(铸数量)
    grant_instances       instance  按件逐个铸造新实例(铸凭证)
    claim_transfers       transfer  既存实例托管转移(**搬运**,不铸)
    consume_transfer_escrow         资产已由 bag journal 入包,托管行只删不物化
  用 grant_instances 处理 transfer = 玩家收到的是"另一件同款装备",原件滞留 escrow。
"""

from __future__ import annotations

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.inventory.v1 import inventory_pb2 as inv_pb
from pandora.inventory.v1 import inventory_pb2_grpc as inv_grpc

from pandorapy import errcode


class GrpcItemGranter:
    """一条连接同时承担四种发放路径(与 Go 侧同一个 GrpcItemGranter)。"""

    __slots__ = ("_channel", "_stub", "_addr")

    def __init__(self, addr: str) -> None:
        self._addr = addr
        self._channel = grpc.aio.insecure_channel(addr)
        self._stub = inv_grpc.InventoryServiceStub(self._channel)

    async def close(self) -> None:
        await self._channel.close()

    async def grant(self, player_id: int, atts, key: str) -> None:  # noqa: ANN001
        """stack 形态入账(inventory.GrantItems)。

        混入非 stack 形态说明上游分组逻辑被破坏 —— **报错而不是静默跳过**:
        跳过会让那件附件消失而邮件被标成已领。
        """
        items = []
        for a in atts:
            if a.WhichOneof("body") != "stack":
                raise errcode.PandoraError(
                    errcode.ErrMailAttachmentUnsupported, "non-stack attachment in stack grant"
                )
            items.append(
                inv_pb.ItemGrant(item_config_id=a.stack.item_config_id, count=int(a.stack.count))
            )
        resp = await self._stub.GrantItems(
            inv_pb.GrantItemsRequest(player_id=player_id, items=items, idempotency_key=key)
        )
        _raise_on_code(resp.code, "inventory grant")

    async def grant_instances(self, player_id: int, item_config_ids: list[int], key: str) -> None:
        """instance 形态逐件铸造(inventory.GrantInstances)。"""
        resp = await self._stub.GrantInstances(
            inv_pb.GrantInstancesRequest(
                player_id=player_id, item_config_ids=item_config_ids, idempotency_key=key
            )
        )
        _raise_on_code(resp.code, "inventory grant instances")

    async def claim_transfers(self, player_id: int, atts, key: str) -> None:  # noqa: ANN001
        """transfer 形态交付(inventory.ClaimTransferInstances)。

        请求只带 instance_id + config 做核对:**领取内容以 inventory 托管行为权威**,
        附件里的快照不参与写入。所以伪造一封带 transfer 附件的邮件必然 fail-closed
        (托管行不存在 → 领取失败),而不是凭附件凭空造出一件装备。
        """
        items = []
        for a in atts:
            if a.WhichOneof("body") != "transfer":
                raise errcode.PandoraError(
                    errcode.ErrMailAttachmentUnsupported,
                    "non-transfer attachment in transfer claim",
                )
            item = a.transfer.item
            items.append(
                inv_pb.TransferClaimItem(
                    instance_id=item.instance_id, item_config_id=item.item_config_id
                )
            )
        resp = await self._stub.ClaimTransferInstances(
            inv_pb.ClaimTransferInstancesRequest(
                to_player_id=player_id, items=items, idempotency_key=key
            )
        )
        _raise_on_code(resp.code, "inventory claim transfers")

    async def consume_transfer_escrow(self, player_id: int, instance_ids: list[int]) -> None:
        """DS 三段式 Mark:资产已经 journal 入包,托管行只删不物化(幂等,缺行 no-op)。

        少了这一步 = 实例同时存在于玩家背包和 escrow 表,也就是**双持**。
        """
        resp = await self._stub.ConsumeTransferEscrow(
            inv_pb.ConsumeTransferEscrowRequest(to_player_id=player_id, instance_ids=instance_ids)
        )
        _raise_on_code(resp.code, "inventory consume escrow")


def _raise_on_code(code: int, op: str) -> None:
    """把 in-band 业务码翻成异常。

    ★ 本仓的 RPC 失败走 response.code 而 gRPC status 恒 OK —— 不看 code 直接当成功
    是最容易漏的一处:调用返回了、没抛异常、而 inventory 其实一件东西都没发。
    """
    if code != errcode_pb2.OK:
        raise errcode.PandoraError(int(code), "%s code=%d", op, int(code))
