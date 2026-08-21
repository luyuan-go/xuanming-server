"""好友图分片落点 + 幂等键口径(对应 Go `services/social/friend/internal/biz/friend_sharding.go`)。

背景(`docs/design/friend-distributed-scaling.md` §5/§6):好友图一旦按 owner(player_id)
分库分表,当前 `accept_request` 的「单事务双向建边」就不再成立,要拆成
「request 单点 CAS → outbox 事件 → 两条边各自 owner 分片幂等写」。

本模块**只落纯逻辑**,不改现状的单 MySQL 事务实现:

  * 统一幂等键口径(`accept_idempotency_key` / `edge_build_key`),避免分片落地时
    各消费者各写一份格式 —— 幂等键差一个字符就是重复建边。
  * 用确定性的 `cellroute.Router` 把一条好友请求的两名玩家解析到各自 owner
    `(region, cell)`,判定这条边是否跨分片 / 跨 region,作为**可观测信号**
    (分片上线前评估跨 region 好友占比)。

★ 为什么这些是纯函数而不是方法:分片判定要能在没有 DB、没有 Redis、没有 router 的
  情况下逐条断言。Go 侧同样把它们提在 usecase 之外。

★ `router` 为 None(单 Cell / dev)时整条路径不执行,行为与迁移前逐字节相同 ——
  这不是「以后可能扩展」的空架子(§15.3),而是 Go 侧已经在跑的同一条分支。
"""

from __future__ import annotations

import dataclasses

from pandorapy import cellroute
from pandorapy import log as plog


def accept_idempotency_key(request_id: int) -> str:
    """一条好友请求 accept 流程的幂等键(saga key)。

    口径统一为 canonical ``friend_accept:<request_id>``:§5.1/§5.3 定「幂等键 = request_id」,
    request 单点 CAS、outbox 事件、两条边幂等写全部锚定同一个 request_id。

    request_id 为 0 时返回的键里就带着 0(调用方应先校验),**不在这里静默改写** ——
    幂等键被悄悄换成另一个值,等于这次写入的去重保护整个消失。
    """
    return f"friend_accept:{request_id}"


def edge_build_key(request_id: int, owner_id: int) -> str:
    """「某 owner 分片建某方向好友边」的幂等键。

    双向建边拆成两条 Kafka 消费(requester 分片写 requester→target,target 分片写反向),
    两条各自在自己分片幂等写,必须带**不同**的键,否则后写的会被前一条的去重记录挡掉
    (表现为「只建了一条边」,而且两侧都显示成功)。故在 saga key 上再缀 owner_id。
    """
    return f"friend_accept:{request_id}:{owner_id}"


@dataclasses.dataclass(frozen=True, slots=True)
class EdgeOwner:
    """一条好友边 owner 的分片落点。

    刻意**不复用** `cellroute.Location`:分片判定只需要 (region, cell) 两维加 player_id,
    带上 logical_cell 会让「两名玩家是否同分片」的判据多一个无关维度。
    """

    player_id: int
    region_id: int
    cell_id: int


def distinct_edge_regions(owners: list[EdgeOwner]) -> list[int]:
    """去重后的 region 列表,**升序**(确定性)。空输入返回 []。

    排序不是为了好看:这个列表会进日志,不排序的话同一组落点在两次运行里
    可能打出不同顺序,按它做 diff 的排查手段就废了。
    """
    seen: set[int] = set()
    regions: list[int] = []
    for o in owners:
        if o.region_id in seen:
            continue
        seen.add(o.region_id)
        regions.append(o.region_id)
    regions.sort()
    return regions


def distinct_edge_cells(owners: list[EdgeOwner]) -> int:
    """去重后的 (region, cell) 数。用于判定这条边的双向建边是否落两个分片。"""
    return len({(o.region_id, o.cell_id) for o in owners})


def cross_shard_friendship(owners: list[EdgeOwner]) -> bool:
    """两名玩家是否落不同 Cell(双向建边跨分片)。单 Cell / 空 → False。"""
    return distinct_edge_cells(owners) > 1


def cross_region_friendship(owners: list[EdgeOwner]) -> bool:
    """这条边是否跨 region。跨 region 好友走 §4.4「最小跨 region 通道」,占比应极低。"""
    return len(distinct_edge_regions(owners)) > 1


def edge_owners(
    router: cellroute.Router | None, requester_id: int, target_id: int
) -> list[EdgeOwner] | None:
    """解析两名玩家的 owner 落点;无法确定时返回 None(调用方退化为不做观测)。

    三种情况都返回 None:router 未注入(单 Cell / dev)、任一 player_id 为 0、
    任一玩家路由失败。**不返回半份结果** —— 只解析出一名玩家的落点无法判定跨不跨分片,
    拿它去算 `cross_shard=False` 会得到一个看起来正常、实际没有依据的结论。
    """
    if router is None or requester_id == 0 or target_id == 0:
        return None
    owners: list[EdgeOwner] = []
    for pid in (requester_id, target_id):
        try:
            loc = router.route(pid)
        except cellroute.CellRouteError:
            return None
        owners.append(EdgeOwner(player_id=pid, region_id=loc.region_id, cell_id=loc.cell_id))
    return owners


def log_friendship_sharding(
    router: cellroute.Router | None,
    request_id: int,
    requester_id: int,
    target_id: int,
) -> None:
    """accept 成功后把这条好友边的分片落点打成观测日志(事件名与 Go 逐字一致)。

    **只观测,不改建边路径**:真正的分片 MySQL / Kafka 双向建边消费者 / 软上限对账
    属基础设施(AGENTS.md §11.1)。router 为 None 时整条不执行,行为不变。

    跨 region 的边额外带一个 `sample_edge_key`(`edge_build_key` 口径样例),
    作为将来排查分片建边幂等键时的锚点 —— 光有「跨了」这个布尔值,
    排查的人还得自己拼键,拼错了就查不到。
    """
    owners = edge_owners(router, requester_id, target_id)
    if owners is None:
        return
    regions = distinct_edge_regions(owners)
    if len(regions) > 1:
        plog.get().debug(
            "friend_edge_sharding",
            request_id=request_id,
            region_count=len(regions),
            cross_shard=True,
            cross_region=True,
            sample_edge_key=edge_build_key(request_id, owners[0].player_id),
        )
        return
    plog.get().debug(
        "friend_edge_sharding",
        request_id=request_id,
        region_count=len(regions),
        cross_shard=cross_shard_friendship(owners),
        cross_region=False,
    )
