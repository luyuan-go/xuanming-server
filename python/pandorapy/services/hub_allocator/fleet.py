"""Hub DS Fleet 分片拓扑抽象 + Mock 实现 —— 对应 Go 侧
`services/battle/hub_allocator/internal/biz/fleet.go`。

三种分片来源(mock / local / agones)共用本文件定义的 `ShardCandidate` 与
`HubFleetProvider` 协议;biz 层只依赖协议,换实现不改业务逻辑。

★ 本层只给**拓扑**,不给实时负载。
  `player_count` / `state` 的权威在 Redis 分片镜像(容量账本见 capacity.py)。
  这条边界不是洁癖:如果让 provider 顺手返回"当前几个人",那个数就会有两个来源
  (k8s 里的 label/annotation 与 Redis 账本),而它们必然在某个时刻不一致 ——
  不一致时不会报错,只会在容量判定上悄悄错(§9.22「不重复存影子状态」)。

★ `token_ready` 的**默认值必须是 False**(Go 的结构体零值)。
  它表示"该分片的 DS 回调令牌已就绪"。写成默认 True 的话,任何一条**忘了显式赋值**
  的构造路径都会产出一个"看起来可用、实际回调会被 enforce 守卫全拒"的候选分片,
  玩家被路由过去后大厅心跳全 401 —— 而拓扑发现日志全绿。所以这里 fail-closed:
  三个 provider 都**显式**赋 True/False,没有一处依赖默认值。
"""

from __future__ import annotations

import dataclasses
from typing import Protocol, runtime_checkable

from pandorapy import releasetrack
from pandorapy.services.hub_allocator.conf import HubConf


@dataclasses.dataclass(slots=True)
class ShardCandidate:
    """一个候选大厅 DS 分片(拓扑信息,不含实时负载)。对应 Go 的 `ShardCandidate`。

    字段与 Go 逐个对应(Go 的整型宽度写在注释里,Python 侧不另设类型):

    | Go                     | 宽度     | 这里 |
    |------------------------|----------|------|
    | ShardID                | uint32   | shard_id |
    | Capacity               | int32    | capacity |
    | TokenExpMs             | int64    | token_exp_ms |
    | TokenGen               | uint64   | token_gen |
    | ProtocolEpoch          | uint32   | protocol_epoch |
    """

    pod_name: str = ""
    addr: str = ""
    region: str = ""
    shard_id: int = 0
    capacity: int = 0

    # ★ 必须来自**实际** GameServer metadata;local / mock 固定 stable。
    # 不能拿"灰度策略算出来的 cohort 意图"填这里 —— 意图不是事实,
    # canary 无容量回落 stable 时两者会分叉(见 releasetrack.py 头注释)。
    release_track: str = ""

    # DS 回调令牌是否就绪。False 只出现在 agones + enforce 且签发/patch 失败时:
    # 该分片仍在拓扑里返回(供对账区分「Fleet 里没有」vs「Fleet 里有但令牌不可用」),
    # 但不会被当作可用镜像分配出去。mock / local / off / permissive 恒 True。
    token_ready: bool = False

    # 当前 DS 回调令牌的 exp(unix ms;annotation 镜像)。0 = 未知/未启用。
    # 【仅供续期判定,不当代际】exp 是秒精度,同秒重签会碰撞(Go 审核 P1-6)。
    token_exp_ms: int = 0
    # 令牌「代际」(Redis INCR 权威、独立、单调;annotation 镜像)。0 = 未知/未启用。
    token_gen: int = 0

    # exact DS 实例身份(§9.22 四元组前两项)。**仅 mode=local 播种**:
    #   - agones 留空 —— 线上实例身份由 Model B 授权记录 promote 后投影,
    #     不能由拓扑发现抢先写(抢先写 = 拓扑扫描成了身份权威,与授权记录会分叉);
    #   - mock 留空 —— 没有真实实例,不允许伪造身份。
    instance_uid: str = ""
    protocol_epoch: int = 0


@dataclasses.dataclass(slots=True)
class HubInstanceObservation:
    """一次**物理存活**观测,刻意与 `ShardCandidate` 分开。对应 Go 的同名结构。

    一个 GameServer 可能处于 Scheduled / Unhealthy,或暂时没有回调凭据,而它的
    进程 / Pod 仍然活着 —— 这些状态让它**不可路由**,但**不是拆机证明**。
    把"不可路由"当"已拆机"就会在旧 DS 还持有玩家时开放第二台 DS(§9.22 脑裂)。
    """

    game_server_found: bool = False
    game_server_uid: str = ""
    pod_found: bool = False
    pod_owner_game_server_uid: str = ""

    def proves_teardown(self, expected_game_server_uid: str) -> bool:
        """只有当 exact 期望 UID **既不再拥有 GameServer 对象、也不再拥有其 Pod** 时才为真。

        对应 Go 的 `HubInstanceObservation.ProvesTeardown`,四条判据逐条同序:

          1. 期望 UID 为空 → False。没有期望身份就没有"谁被拆了"这个命题,
             返回 True 等于给任意残留发一张通行证。
          2. GameServer 还在且 UID **正是**期望的 → False(它显然没被拆)。
          3. Pod 还在 → 只有当 GameServer 存在、UID 非空、UID **已换**成别的,
             且该 Pod 的 owner 正是这个新 UID 时,才算旧实例被替换掉了。
             owner 未知 / 畸形 → False(fail-closed)。
          4. Pod 不在 → GameServer 不在,或 UID 已经不是期望的,即为已拆。
        """
        if expected_game_server_uid == "":
            return False
        if self.game_server_found and self.game_server_uid == expected_game_server_uid:
            return False
        if self.pod_found:
            return (
                self.game_server_found
                and self.game_server_uid != ""
                and self.game_server_uid != expected_game_server_uid
                and self.pod_owner_game_server_uid == self.game_server_uid
            )
        return not self.game_server_found or self.game_server_uid != expected_game_server_uid


@runtime_checkable
class HubFleetProvider(Protocol):
    """返回某 region 的候选分片拓扑。对应 Go 的 `HubFleetProvider`。"""

    async def list_shards(self, region: str) -> list[ShardCandidate]:
        """列出 region 下的全部候选分片(静态拓扑,不含实时负载)。"""
        ...


@runtime_checkable
class HubFleetPhysicalObserver(Protocol):
    """可选的物理存活观测能力。对应 Go 的 `HubFleetPhysicalObserver`。

    ★ Go 头注释的告诫必须原样保留:
      「**仅仅是 `list_shards` 里没有**,永远不能被当成物理死亡。」
      对账拿它铸 exact UID 拆机证明,但 Fleet 列举缺席只说明"不可路由"。
    """

    async def observe_shard_instance(self, pod: str) -> HubInstanceObservation: ...


@runtime_checkable
class HubFleetScaler(Protocol):
    """Hub Fleet 副本扩缩容能力(可选)。对应 Go 的 `HubFleetScaler`。

    ★ **只有真 Agones provider 实现本协议**。`MockHubFleetProvider` /
    `LocalHubFleetProvider` **刻意不实现** —— 实现一个"Get 返回假副本数、Set 是
    no-op"的退化版本会让 `auto_scale_enabled()` 误以为可扩缩容:每轮 reconcile
    都跑、实际什么都没变,还会对假分片 / 假玩家跑 consolidation。
    conf.validate_conf 的 `autoscale_inert_under_mock` 告警就是配合这条的。
    """

    async def get_fleet_replicas(self) -> int: ...
    async def set_fleet_replicas(self, replicas: int) -> None: ...


def sticky_release_track(track: str) -> str:
    """**已持久化记录**的轨道读取规则。对应 Go 的 `biz/hub.go: stickyReleaseTrack`。

    ★ 空串 → stable。这是 additive rollout 的唯一旧值迁移规则:轨道字段是后加的,
      滚动升级前落库的分片镜像 / 归属记录里它是空的。不补 stable 的话这些老玩家
      会被判成"轨道非法"而整条路由失败 —— 一次发布就把在场玩家全踢下线。
    ★ 其它未知值 **fail-closed 抛错**,绝不被 cohort 策略重算:重算 = 已经粘在
      canary 的玩家可能因为一次调参被甩回 stable(§9.21 要求同一玩家固定轨道)。

    ⚠️ **这条规则只适用于持久化记录,绝不能搬进 Agones 拓扑发现路径。**
    `agones_fleet._list_track_shards` 对 GameServer metadata 是**严格相等**判定,
    缺 label / annotation 一律跳过并告警。理由:GameServer 是编排层的实时对象,
    它没有"历史遗留"这回事;把一台**没打轨道标签的** canary GameServer 默认成
    stable,等于把灰度实例混进正式池 —— 这正是 §9.21 明令禁止的方向。
    两处方向相反不是笔误,是"旧数据兼容"与"实时发现 fail-closed"的分工。
    """
    if track == "":
        return releasetrack.STABLE
    if not releasetrack.valid(track):
        raise ValueError(f"invalid persisted hub release_track {track!r}")
    return track


class MockHubFleetProvider:
    """打桩实现:不连 k8s,按 region 生成 `mock_shard_count` 个确定性假分片。

    对应 Go 的 `MockHubFleetProvider`。

        pod   = pandora-hub-<region>-<i>   (i 从 1 起)
        addr  = <mock_hub_addr_host>:(<mock_hub_port_base> + i)

    ★ 确定性是本实现的**全部价值**:同 region 多次列举必须得到同一份拓扑,
      否则 dev 环境里每次 reconcile 都会看到"一批分片消失、另一批出现",
      对账逻辑会不停地建 / 删 Redis 镜像,把真实缺陷淹在噪声里。

    ★ 拓扑-only:**不实现** `HubFleetScaler`(理由见该协议的 docstring)。
    """

    def __init__(self, cfg: HubConf) -> None:
        self._cfg = cfg

    async def list_shards(self, region: str) -> list[ShardCandidate]:
        """返回 region 的确定性假分片拓扑。对应 Go 的 `ListShards`。

        ★ 循环边界 `1..=mock_shard_count` 与 Go 逐字一致(Go: `for i := 1; i <= n`)。
          写成 `range(n)` 会让第一个分片变成 `-0` 号、端口变成 base+0 ——
          与 Go 副本算出不同的 addr,同一份 dev 配置两栈连不同端口。
        ★ `mock_shard_count <= 0` 时返回空列表(Go 同):负值不兜默认,
          见 conf.py 关于「显式的荒谬值不许被悄悄修正」的长注释。
        """
        out: list[ShardCandidate] = []
        for i in range(1, self._cfg.mock_shard_count + 1):
            out.append(
                ShardCandidate(
                    pod_name=f"pandora-hub-{region}-{i}",
                    addr=f"{self._cfg.mock_hub_addr_host}:{self._cfg.mock_hub_port_base + i}",
                    region=region,
                    shard_id=i,
                    capacity=self._cfg.default_capacity,
                    release_track=releasetrack.STABLE,
                    token_ready=True,
                    # instance_uid / protocol_epoch 刻意留空:没有真实实例,
                    # 伪造身份会让 legacy 归属定案拼出一个指向虚无的 owner 目标。
                )
            )
        return out
