"""player → inventory 的 gRPC 客户端 —— 对应 Go 侧 internal/data/inventory_client.go。

内网 insecure 直连、不带 JWT。CheckInstancesOwned 是**系统接口**,要求 callerID==0
(后端内部直连);客户端 RPC GetInventory 反过来要求 callerID>0,内部直连会被判
ERR_UNAUTHORIZED —— 两者不能复用同一条调用形态。

★ 传输错误与非 OK 业务码都**原样抛出**,绝不降级成空集:调用方要靠「查询失败」与
  「一件都没有」可区分才能 fail-closed(§9.22)。把 error 吞成空集的后果是 SetEquipment
  把"查不到"当成"没有这件",玩家的合法装备被拒;而 GetLoadout 那边更糟——空集会让
  校验以为没有任何实例需要核对。
"""

from __future__ import annotations

import grpc

from pandora.common.v1 import errcode_pb2 as commonpb
from pandora.inventory.v1 import inventory_pb2 as inv_pb
from pandora.inventory.v1 import inventory_pb2_grpc as inv_grpc

from pandorapy import errcode
from pandorapy.services.player import models as m


class GrpcInstanceOwnershipChecker:
    """inventory.CheckInstancesOwned 的客户端实现。"""

    __slots__ = ("_channel", "_stub", "_addr")

    def __init__(self, addr: str) -> None:
        self._addr = addr
        self._channel = grpc.aio.insecure_channel(addr)
        self._stub = inv_grpc.InventoryServiceStub(self._channel)

    async def close(self) -> None:
        await self._channel.close()

    async def check_instances_owned(
        self, player_id: int, equipment: list[m.EquipmentSlot]
    ) -> m.InstanceOwnershipResult:
        """返回入参中 instance_id + item_config_id **都精确匹配**当前归属的 ID 子集与
        权威鉴定快照。"""
        queries = [
            inv_pb.InstanceOwnershipQuery(instance_id=e.instance_id, item_config_id=e.item_config_id)
            for e in equipment
        ]
        resp = await self._stub.CheckInstancesOwned(
            inv_pb.CheckInstancesOwnedRequest(player_id=player_id, instances=queries)
        )
        if resp.code != commonpb.OK:
            raise errcode.PandoraError(
                int(resp.code), "inventory check instances owned code=%d", int(resp.code)
            )
        instances = tuple(
            m.OwnedEquipmentInstance(
                instance_id=int(inst.instance_id),
                item_config_id=int(inst.item_config_id),
                identified=bool(inst.identified),
                attributes=tuple(
                    m.EquipmentAttributeSnapshot(attr_id=int(a.attr_id), value=int(a.value))
                    for a in inst.attributes
                ),
            )
            for inst in resp.owned_instances
        )
        return m.InstanceOwnershipResult(
            owned_instance_ids=tuple(int(i) for i in resp.owned_instance_ids),
            owned_instances=instances,
        )
