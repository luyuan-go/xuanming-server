"""发奖下游 gRPC 客户端 —— 对应 Go 侧 internal/data/granter.go。

接线对齐 leaderboard reward_client / battle_result mail_client:内网 insecure 直连,
无 JWT(GrantItems / GrantInstances / AddExperience 都是系统 RPC,只认 callerID==0)。
幂等键由 biz 给定(stack / inst / quest 三通道**分键**,见 biz.py 文件头)。

★ 三下游各用独立幂等键的理由:inventory_ledger 的唯一键是 (player_id, idempotency_key),
  GrantItems 与 GrantInstances 同键会撞收据指纹冲突(fail-closed),必须分键。
"""

from __future__ import annotations

import dataclasses

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.inventory.v1 import inventory_pb2, inventory_pb2_grpc
from pandora.mail.v1 import mail_pb2, mail_pb2_grpc
from pandora.player.v1 import player_pb2, player_pb2_grpc

from pandorapy import errcode

# 单次下游调用超时。
# ★ 必须给超时:没有超时的话下游卡住会把补扫循环整条挂住(补扫是串行的,一行卡住
#   后面全部行都不再被处理),而且**没有任何日志** —— 表现为"发奖突然全部停了"。
DEFAULT_TIMEOUT_SEC = 5.0

# 溢出邮件文案(对齐 Go 的 overflowMailTitle / overflowMailBody,逐字相同 ——
# 文案漂移会让玩家在灰度期收到两种不同措辞的同一封邮件)。
OVERFLOW_MAIL_TITLE = "任务奖励"
OVERFLOW_MAIL_BODY = "背包已满,任务奖励的装备已放入邮件,请清理背包后领取。"


@dataclasses.dataclass(frozen=True, slots=True)
class RewardItem:
    """一条道具发放(数量在 uint32 域,发给 inventory 时转 int64)。"""

    item_config_id: int
    count: int


# ── inventory(道具 / 装备)──────────────────────────────────────────────────


class GrpcItemGranter:
    """道具/装备发放。对应 Go 的 GrpcItemGranter。"""

    __slots__ = ("_channel", "_stub", "_timeout_sec")

    def __init__(self, addr: str, *, timeout_sec: float = DEFAULT_TIMEOUT_SEC) -> None:
        self._channel = grpc.aio.insecure_channel(addr)
        self._stub = inventory_pb2_grpc.InventoryServiceStub(self._channel)
        self._timeout_sec = timeout_sec

    async def close(self) -> None:
        await self._channel.close()

    async def grant_items(
        self, player_id: int, idem_key: str, items: list[RewardItem]
    ) -> None:
        """幂等发放可堆叠道具。非 OK 一律抛错(留 PENDING/FAILED 给补扫)。"""
        grants = [
            inventory_pb2.ItemGrant(item_config_id=it.item_config_id, count=int(it.count))
            for it in items
            if it.count > 0
        ]
        if not grants:
            # 与 Go 同:没有有效道具就不打这趟 RPC。**不能当成失败** —— 空内容是配置
            # 意图,标 FAILED 会让补扫永远重试一条永远发不出东西的行。
            return
        resp = await self._stub.GrantItems(
            inventory_pb2.GrantItemsRequest(
                player_id=player_id, items=grants, idempotency_key=idem_key
            ),
            timeout=self._timeout_sec,
        )
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                int(resp.code),
                "grant items player=%d key=%s code=%d",
                player_id,
                idem_key,
                int(resp.code),
            )

    async def grant_instances(
        self, player_id: int, idem_key: str, item_config_ids: list[int]
    ) -> bool:
        """逐件发装备实例。返回 capacity_full(True = 背包满,调用方转邮件)。

        ★ 背包满**不是错误**,是一条业务分支:当成错误抛出去会让补扫每轮重试一条
        永远塞不进去的行,而正确处置(转邮件)永远不会发生。
        """
        if not item_config_ids:
            return False
        resp = await self._stub.GrantInstances(
            inventory_pb2.GrantInstancesRequest(
                player_id=player_id,
                item_config_ids=item_config_ids,
                idempotency_key=idem_key,
            ),
            timeout=self._timeout_sec,
        )
        if resp.code == errcode_pb2.OK:
            return False
        if resp.code == errcode_pb2.ERR_INVENTORY_CAPACITY_FULL:
            return True
        raise errcode.PandoraError(
            int(resp.code),
            "grant instances player=%d key=%s code=%d",
            player_id,
            idem_key,
            int(resp.code),
        )


class NoopItemGranter:
    """allow_noop_reward=true 的 dev 骨架占位。

    ★ 刻意**返回错误而不是假装成功**(§8 不假装成功):发奖流水滞留 PENDING 可见
    可补,补扫日志每轮揭示一次;假装成功会把流水写成 GRANTED —— 那条奖励就永久
    丢失且无任何痕迹。
    """

    __slots__ = ()

    async def close(self) -> None:
        return None

    async def grant_items(self, player_id: int, idem_key: str, items) -> None:  # noqa: ANN001
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "noop item granter(inventory_addr 未配)"
        )

    async def grant_instances(self, player_id: int, idem_key: str, item_config_ids) -> bool:  # noqa: ANN001
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "noop item granter(inventory_addr 未配)"
        )


# ── player(经验)────────────────────────────────────────────────────────────


class GrpcExpGranter:
    """经验发放(player.AddExperience,reason="quest")。对应 Go 的 GrpcExpGranter。"""

    __slots__ = ("_channel", "_stub", "_timeout_sec")

    def __init__(self, addr: str, *, timeout_sec: float = DEFAULT_TIMEOUT_SEC) -> None:
        self._channel = grpc.aio.insecure_channel(addr)
        self._stub = player_pb2_grpc.PlayerServiceStub(self._channel)
        self._timeout_sec = timeout_sec

    async def close(self) -> None:
        await self._channel.close()

    async def add_experience(self, player_id: int, exp_delta: int, idem_key: str) -> None:
        """幂等入账经验(幂等命中 already 也视为成功)。"""
        resp = await self._stub.AddExperience(
            player_pb2.AddExperienceRequest(
                player_id=player_id,
                exp_delta=exp_delta,
                # reason 是 player.proto 注释的预留口径,与幂等键 quest:<p>:<m> 配套。
                reason="quest",
                idempotency_key=idem_key,
            ),
            timeout=self._timeout_sec,
        )
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                int(resp.code),
                "add experience player=%d key=%s code=%d",
                player_id,
                idem_key,
                int(resp.code),
            )


class NoopExpGranter:
    """同 NoopItemGranter:返回错误,不假装成功。"""

    __slots__ = ()

    async def close(self) -> None:
        return None

    async def add_experience(self, player_id: int, exp_delta: int, idem_key: str) -> None:
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "noop exp granter(player_addr 未配)"
        )


# ── mail(满包溢出转邮件)────────────────────────────────────────────────────


class GrpcOverflowMailSender:
    """满包溢出转邮件。对应 Go 的 GrpcOverflowMailSender。"""

    __slots__ = ("_channel", "_stub", "_timeout_sec")

    def __init__(self, addr: str, *, timeout_sec: float = DEFAULT_TIMEOUT_SEC) -> None:
        self._channel = grpc.aio.insecure_channel(addr)
        self._stub = mail_pb2_grpc.MailServiceStub(self._channel)
        self._timeout_sec = timeout_sec

    async def close(self) -> None:
        await self._channel.close()

    async def send_overflow_mail(
        self, player_id: int, item_config_ids: list[int], grant_key: str
    ) -> None:
        """把满包装备按 config_id 聚合成 instance 附件发个人邮件。

        grant_key 传直发链同款 `:inst` 键作 instance_grant_key:邮件领取走
        GrantInstances 用同键去重 → 直发链与邮件链**至多一次**。传别的键(或不传)
        会让同一批装备被发两次 —— 幂等键防不住,因为压根不是同一个键。
        """
        if not item_config_ids:
            return
        order: list[int] = []
        counts: dict[int, int] = {}
        for cid in item_config_ids:
            if cid not in counts:
                order.append(cid)
                counts[cid] = 0
            counts[cid] += 1
        atts = [
            mail_pb2.MailAttachment(
                instance=mail_pb2.InstanceAttachment(item_config_id=cid, count=counts[cid])
            )
            for cid in order
        ]
        resp = await self._stub.SendPersonalMail(
            mail_pb2.SendPersonalMailRequest(
                to_player_id=player_id,
                title=OVERFLOW_MAIL_TITLE,
                body=OVERFLOW_MAIL_BODY,
                attachments=atts,
                instance_grant_key=grant_key,
            ),
            timeout=self._timeout_sec,
        )
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                int(resp.code),
                "overflow mail player=%d code=%d",
                player_id,
                int(resp.code),
            )
