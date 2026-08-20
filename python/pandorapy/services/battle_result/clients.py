"""battle_result 的下游 gRPC 客户端 —— 对应 Go 侧 internal/data 的
mmr_reader.go / inventory_client.go / mail_client.go / match_releaser.go。

四个客户端全部**内网 insecure 直连、不带 JWT**(系统接口,对齐 trade / auction / mail)。

★ 幂等键一律由 biz 生成并传入,下游按键去重 —— **不要**在本文件里再加一层
  "记住发过了"的缓存:那份缓存进程重启就没了,而真正的幂等权威在下游的流水表里。

★ 四条发放路径语义完全不同,混用就是资产事故:
    grant_items      stack     按 config_id + count 计数入账(铸数量)
    grant_instances  instance  按件逐个铸造新实例(铸凭证)
  掉落路由在**首次入箱时冻结**(repo.DropOutboxRecord),发布重试绝不重算 ——
  重算会在"stack 已成功、instance 失败"的窗口里因类型热更换掉 method + 幂等键,
  把已成功那部分再发一遍。
"""

from __future__ import annotations

import dataclasses

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.inventory.v1 import inventory_pb2 as inv_pb
from pandora.inventory.v1 import inventory_pb2_grpc as inv_grpc
from pandora.mail.v1 import mail_pb2 as mail_pb
from pandora.mail.v1 import mail_pb2_grpc as mail_grpc
from pandora.match.v1 import match_pb2 as match_pb
from pandora.match.v1 import match_pb2_grpc as match_grpc
from pandora.player.v1 import player_pb2 as player_pb
from pandora.player.v1 import player_pb2_grpc as player_grpc

from pandorapy import errcode

# 溢出邮件标题/正文(与 Go 侧 mail_client.go 逐字相同 —— 玩家看得见的文案,
# 两栈不一致会让同一件事在收件箱里长出两种样子)。
OVERFLOW_MAIL_TITLE = "战斗掉落"
OVERFLOW_MAIL_BODY = "背包已满,战斗掉落的装备已放入邮件,请清理背包后领取。"


@dataclasses.dataclass(frozen=True, slots=True)
class StackGrant:
    """一条可堆叠发放。对应 Go 的 data.StackGrant。"""

    item_config_id: int
    count: int


def _raise_on_code(code: int, what: str) -> None:
    """非 OK 的**业务码**要抬成异常,让出箱行保留重试。

    静默吞掉非 OK 的后果:出箱行被当成"发放成功"删掉 —— 玩家的掉落永久消失,
    而本服日志一片正常。
    """
    if code != errcode_pb2.OK:
        raise errcode.PandoraError(int(code), "%s code=%d", what, int(code))


class GrpcMMRReader:
    """读玩家在**某段位池**下的当前 MMR。对应 Go 的 data.GrpcMMRReader。

    弱依赖:调用失败由 biz.assign_mmr 回退到 cfg.base_mmr,不阻断落库。
    ★ rating_pool 必须传本局定格值:算期望胜率必须用**同一份**段位的分,
      拿另一池的分当输入会让 Elo 完全失真(3v3 高分玩家打 5v5 会被当成高手压分)。
    """

    __slots__ = ("_channel", "_stub")

    def __init__(self, addr: str) -> None:
        self._channel = grpc.aio.insecure_channel(addr)
        self._stub = player_grpc.PlayerServiceStub(self._channel)

    async def close(self) -> None:
        await self._channel.close()

    async def get_mmr(self, player_id: int, rating_pool: str) -> int:
        resp = await self._stub.GetMMR(
            player_pb.GetMMRRequest(player_id=player_id, rating_pool=rating_pool)
        )
        _raise_on_code(resp.code, f"player.GetMMR player={player_id} pool={rating_pool}")
        return int(resp.mmr)


class GrpcInstanceGranter:
    """把战斗掉落按真实类型幂等写入 inventory。对应 Go 的 data.GrpcInstanceGranter。"""

    __slots__ = ("_channel", "_stub")

    def __init__(self, addr: str) -> None:
        self._channel = grpc.aio.insecure_channel(addr)
        self._stub = inv_grpc.InventoryServiceStub(self._channel)

    async def close(self) -> None:
        await self._channel.close()

    async def grant_items(
        self, player_id: int, items: list[StackGrant], idempotency_key: str
    ) -> None:
        """可堆叠战利品按配置 ID 聚合后写入计数背包。"""
        grants = [
            inv_pb.ItemGrant(item_config_id=it.item_config_id, count=it.count) for it in items
        ]
        resp = await self._stub.GrantItems(
            inv_pb.GrantItemsRequest(
                player_id=player_id, items=grants, idempotency_key=idempotency_key
            )
        )
        _raise_on_code(resp.code, "inventory grant items")

    async def grant_instances(
        self, player_id: int, item_config_ids: list[int], idempotency_key: str
    ) -> None:
        """装备逐件铸造独立实例。"""
        resp = await self._stub.GrantInstances(
            inv_pb.GrantInstancesRequest(
                player_id=player_id,
                item_config_ids=item_config_ids,
                idempotency_key=idempotency_key,
            )
        )
        _raise_on_code(resp.code, "inventory grant instances")


class GrpcMailSender:
    """把背包满溢出的战斗装备掉落转个人邮件。对应 Go 的 data.GrpcMailSender。

    ★ 传的 grant_key 是**源键** battle_drop:{match}:{player} —— 与直发
      GrantInstances 用的是同一把键。领取时 mail 侧再调 GrantInstances 用同键去重,
      于是"直发链"与"邮件领取链"共享幂等键 → **至多一次**(即便偶发重复邮件)。
      换一把键就等于把至多一次变成至少一次:玩家能拿到两份装备。
    """

    __slots__ = ("_channel", "_stub")

    def __init__(self, addr: str) -> None:
        self._channel = grpc.aio.insecure_channel(addr)
        self._stub = mail_grpc.MailServiceStub(self._channel)

    async def close(self) -> None:
        await self._channel.close()

    async def send_overflow_mail(
        self, player_id: int, item_config_ids: list[int], grant_key: str
    ) -> None:
        if not item_config_ids:
            return
        resp = await self._stub.SendPersonalMail(
            mail_pb.SendPersonalMailRequest(
                to_player_id=player_id,
                title=OVERFLOW_MAIL_TITLE,
                body=OVERFLOW_MAIL_BODY,
                attachments=group_instance_attachments(item_config_ids),
                instance_grant_key=grant_key,
            )
        )
        _raise_on_code(resp.code, "mail send personal")


def group_instance_attachments(item_config_ids: list[int]) -> list[mail_pb.MailAttachment]:
    """把逐件展开的配置 ID 按 config_id 聚合成 count,拼 instance 形态附件。

    保持**首次出现顺序**(与 Go 的 order 切片一致):附件顺序会出现在玩家的邮件里,
    用 dict 无序聚合会让同一封邮件在两栈上长得不一样。
    """
    order: list[int] = []
    counts: dict[int, int] = {}
    for cid in item_config_ids:
        if cid not in counts:
            order.append(cid)
            counts[cid] = 0
        counts[cid] += 1
    return [
        mail_pb.MailAttachment(
            instance=mail_pb.InstanceAttachment(item_config_id=cid, count=counts[cid])
        )
        for cid in order
    ]


class GrpcMatchReleaser:
    """通知 matchmaker 释放一场已结算/废弃对局的撮合状态。对应 Go 的 data.GrpcMatchReleaser。

    不调它的后果很具体:matchmaker 故意保留的 player→ticket claim + 票据 + match 镜像
    只能等 30min TTL 自然过期,期间玩家回 Hub 再次 StartMatch 撞
    ErrMatchAlreadyMatching(4002) —— 就是"结算返回大厅后无法再次匹配"那个老问题。
    """

    __slots__ = ("_channel", "_stub")

    def __init__(self, addr: str) -> None:
        self._channel = grpc.aio.insecure_channel(addr)
        self._stub = match_grpc.MatchServiceStub(self._channel)

    async def close(self) -> None:
        await self._channel.close()

    async def release_match(self, match_id: int, player_ids: list[int]) -> None:
        resp = await self._stub.ReleaseMatch(
            match_pb.ReleaseMatchRequest(match_id=match_id, player_ids=player_ids)
        )
        _raise_on_code(resp.code, f"matchmaker.ReleaseMatch match={match_id}")


class StaticMMRReader:
    """固定返回 base 的 MMRReader(player 服务未上线 / player_addr 未配时兜底)。

    对应 Go 的 biz.StaticMMRReader。此时两队均值相等 → expected=0.5 →
    胜 +K/2、负 -K/2(对称)。
    """

    __slots__ = ("_base",)

    def __init__(self, base: int) -> None:
        self._base = base

    async def get_mmr(self, player_id: int, rating_pool: str) -> int:  # noqa: ARG002
        return self._base
