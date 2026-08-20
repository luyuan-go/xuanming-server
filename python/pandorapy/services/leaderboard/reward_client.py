"""结算发奖客户端 —— 对应 Go 侧 internal/data/reward_client.go + biz.NoopRewardGranter。

把结算名次奖励经 gRPC 交给 inventory 服务幂等发放到玩家背包
(GrantItems,幂等键 = `lb:<settlement_id>:<entity_id>`,不变量 §9.7)。

接线:
  - main.py 直连内网 endpoint(insecure;GrantItems 是系统接口,只认内网直连);
  - inventory_addr 未配 **且** allow_noop_reward=true 时才退回 NoopRewardGranter,
    否则启动期 fail-fast(见 main.py 的 reward_granter_missing 闸)。

⚠️ 工会榜(scope=GUILD)的 entity_id 是 guild_id,不是 player_id;GrantItems 是发给
玩家的。工会奖励的分发(发到工会仓库 / 拆给成员)不在 leaderboard 职责内 ——
biz 仅对"按玩家发奖"的榜调本 granter,工会榜结算只落快照 + 发 kafka,由工会服务消费分发。
"""

from __future__ import annotations

import dataclasses

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.inventory.v1 import inventory_pb2, inventory_pb2_grpc

from pandorapy import errcode


@dataclasses.dataclass(frozen=True, slots=True)
class RewardGrant:
    """发给单个玩家的一份奖励(item + count,对齐 inventory ItemGrant)。"""

    item_config_id: int
    count: int


class NoopRewardGranter:
    """占位实现:发奖总成功(**不真实入账**)。仅供无背包联调 / 单测。

    ★ 它的存在本身是个风险点 —— 生产漏配 inventory_addr 时若默默退回这里,
    结算会"成功"、reward_log 全写 GRANTED、玩家一件奖也收不到,而且没有任何错误。
    所以退回本实现必须由 `allow_noop_reward=true` **显式**授权(默认 false)。
    """

    __slots__ = ()

    async def grant(self, player_id: int, idem_key: str, items: list[RewardGrant]) -> None:
        return None


class GrpcInventoryRewardGranter:
    """用 inventory 服务 gRPC client 发奖。对应 Go 的 GrpcInventoryRewardGranter。"""

    __slots__ = ("_channel", "_stub", "_timeout_sec")

    def __init__(self, inventory_addr: str, *, timeout_sec: float = 5.0) -> None:
        # insecure:内网直连,与 Go 的 grpcclient.MustDialInsecure 一致。
        self._channel = grpc.aio.insecure_channel(inventory_addr)
        self._stub = inventory_pb2_grpc.InventoryServiceStub(self._channel)
        # ★ 必须给超时。没有超时的话 inventory 卡住会把结算的发奖循环整条挂住,
        # 而结算是同步 RPC —— 调用方(运营后台 / 定时任务)也一起挂住。
        self._timeout_sec = timeout_sec

    async def close(self) -> None:
        await self._channel.close()

    async def grant(self, player_id: int, idem_key: str, items: list[RewardGrant]) -> None:
        """幂等发放一组奖励给玩家。

        - inventory 回 OK → 正常返回(发放成功 / 幂等回放)
        - 其它非 OK code → ErrLeaderboardRewardFailed(透传 code 便于定位)
        - 传输层异常 → 原样抛出,由 biz 标 FAILED 后交补发扫描重试
        """
        grants = [
            inventory_pb2.ItemGrant(item_config_id=it.item_config_id, count=it.count)
            for it in items
            if it.count > 0
        ]
        if not grants:
            # 与 Go 同:没有有效道具就不打这一趟 RPC。注意**不能**当成失败 ——
            # 空奖励是配置意图(某个名次段不给东西),标 FAILED 会让补扫永远重试。
            return
        resp = await self._stub.GrantItems(
            inventory_pb2.GrantItemsRequest(
                player_id=player_id, items=grants, idempotency_key=idem_key
            ),
            timeout=self._timeout_sec,
        )
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                errcode.ErrLeaderboardRewardFailed,
                "lb reward grant failed player=%d key=%s code=%d",
                player_id,
                idem_key,
                int(resp.code),
            )
