"""战斗 DS pod 分配抽象 + Mock 实现 —— 对应 Go 侧
`services/battle/ds_allocator/internal/biz/gameserver.go`。

本模块**只声明能力边界**(Protocol)与一个不连 k8s 的确定性打桩实现;真实实现在
`agones_allocator.py`(Agones Model B)与 `local_allocator.py`(mode=local 本机
exec DS)。分成三份不是为了"分层好看",而是因为三条路径的**授权语义不同**:

    Agones(生产 Model B)   实例身份取自 K8s 严格 GET 确认的 GameServer UID;
                            凭据经 annotation 用 UID+resourceVersion 条件 PATCH 投递。
                            K8s 只是**投递镜像**,不是授权权威。
    local(mode=local)      实例身份是拉起进程时生成、且已签进该 DS 回调令牌的
                            同一组值 —— 也不是凭空构造。
    mock                    没有真实 pod,只按 match_id 算一个确定性假地址。

## 为什么 local 专属能力要写成**独立** Protocol,而不是往主接口上加方法

`LocalInstanceIdentitySource` / `LocalBattleCredentialSource` / `LocalBattleRosterSink`
只有 `local_allocator.LocalGameServerAllocator` 实现。Agones / Mock 都不实现,因此
调用方那几条 legacy 回填分支在生产(Model B)与离线 mock 下是**机械死代码** ——
两条路径的分配结果与心跳应答逐字节不变。合并进主接口就必须给 Agones 也写一个
实现(哪怕是返回"不支持"),那一刻"生产上这条分支不可达"就从**结构性事实**降级成
了一句需要人去核对的注释。

## Python 与 Go 的签名差异(都是"Go 多返回值"的直译,不是语义放宽)

    Go `(uid, epoch, ok)`         → `tuple[str, int] | None`
    Go `(identity, ok)`           → `BattleCredentialIdentity | None`
    Go `(allocation, found, err)` → `tuple[Allocation | None, bool]` + 异常
    Go `(bool, error)`            → `bool` + 异常

★ 三态(成功 / 权威缺席 / 查不了)绝不能压成两态:把"查不了"折成"没有",正是
  §9.22 明令禁止的「把 UNKNOWN 冒充成确定值」,而在分配链上它的下游动作是**删除**。
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from pandorapy import errcode
from pandorapy.services.ds_allocator import conf as dsconf
from pandorapy.services.ds_allocator.agones_allocator import (
    AuthoritativeGameServerAllocation,
)

if TYPE_CHECKING:
    # ⚠️ 仓里有**两个**同名的 `BattleCredentialIdentity`(`battle_auth` 一个、
    # `local_allocator` 一个,字段一致但 local 那份多了 `complete_for_ack()`)。
    # `local_credential_ack` 返回的是 **local_allocator 那份**,所以这里必须引它,
    # 不能顺手引 battle_auth —— 名字一样、来源不同的类型标注是最难发现的错标注。
    # 只在类型检查期引入:Protocol 的运行期检查只看方法名,不看标注,没必要为一行
    # 标注把进程拉起路径与 DS 拉起模块耦合起来。
    from pandorapy.services.ds_allocator.local_allocator import BattleCredentialIdentity

# uint64 上界。Python 整数不回绕,而 Go 的 `matchID % uint64(range)` 是在 64 位
# 无符号域里算的 —— 不显式判界的话,一个越界 match_id 在两栈上会算出不同的端口
# (Go 那侧甚至根本走不到这里,类型系统先把它挡了)。
_UINT64_MAX = (1 << 64) - 1


class GameServerAllocator(Protocol):
    """向底层编排申请 / 释放一个战斗 DS pod。Go: `biz.GameServerAllocator`。"""

    async def allocate(
        self, match_id: int, map_id: int, game_mode: str, release_track: str
    ) -> tuple[str, str, str]:
        """申请一个战斗 DS,返回 `(pod_name, addr, actual_release_track)`。

        ★ 第三个返回值是**编排层实际命中**的发布轨,不是入参的意图轨。
          回传意图的后果:canary 容量回退(GSA 落到 stable Fleet)时服务端会把这局
          记成 canary,§9.21 要求的"同一对局固定 release track"就此断掉 ——
          之后所有按轨粘滞的判定(重连、回流、灰度分流)都会指错。
        """
        ...

    async def release(self, pod_name: str) -> None:
        """释放(回收)一个战斗 DS pod。"""
        ...


@runtime_checkable
class LocalInstanceIdentitySource(Protocol):
    """mode=local 分配器给出本机 DS 进程的 exact 实例身份。Go: `localInstanceIdentitySource`。

    返回 `(instance_uid, instance_epoch)`;未知返回 `None`(Go 的 `ok=false`)。
    """

    def local_instance_identity(self, pod_name: str) -> tuple[str, int] | None: ...


@runtime_checkable
class LocalBattleCredentialSource(Protocol):
    """mode=local 分配器给出经 env 下发给本机 DS 的**完整**凭据身份。
    Go: `localBattleCredentialSource`。

    与 `LocalInstanceIdentitySource` 是两件事,别合并:那个只回 uid/epoch(够 legacy
    回填战斗记录),而 UE 的心跳 ACK 比对要求 uid/epoch/gen/jti/writer_epoch **五项
    全等**,少一项即整份判不匹配 —— DS 于是收不到任何可消费的指令。
    """

    def local_credential_ack(self, pod_name: str) -> BattleCredentialIdentity | None: ...


@runtime_checkable
class LocalBattleRosterSink(Protocol):
    """mode=local 分配器接收本局的权威准入元数据。Go: `localBattleRosterSink`。

    生产走 Agones GameServer annotation 下发
    roster / allocation-id / release-track / combat-factions;local 没有 annotation
    通道,改用 DS 进程 env 投递**同一份事实**。

    ★ 四件套是一份不可拆的事实,必须整份进出。漏投的后果不是"少个字段":

        缺 roster          → UE 的 ExpectedPlayers 恒空 → 玩家能进副本能打怪,
                             但点「退出副本」永远被判 AuthorityNotReady(2026-08-04 实伤);
        缺 combat-factions → UE 的 ResolveCampForSpawn 解不出权威阵营 → RejectSpawn
                             且禁止回退默认 Pawn → 玩家进了图但**根本没有角色**,
                             表现为进副本即卡死(2026-08-13 实伤)。

    Go 侧是同步方法,Python 侧实现是 `async`(要写进程内登记表并与拉起流程串起来),
    故 Protocol 声明为 async。
    """

    async def set_pending_battle_roster(
        self,
        match_id: int,
        player_ids: list[int],
        combat_faction_by_player: dict[int, int] | None,
        allocation_id: str,
        release_track: str,
    ) -> None: ...


class AuthoritativeGameServerAllocator(Protocol):
    """Agones Model B 的额外能力。Go: `biz.AuthoritativeGameServerAllocator`。

    分配时先取得实例 UID/resourceVersion,Redis stage 成功后再用 UID+RV 条件 PATCH
    投递 annotation。**K8s 仅是投递镜像,不是授权权威。**
    """

    async def allocate_authoritative(
        self,
        match_id: int,
        allocation_id: str,
        player_ids: list[int],
        combat_faction_by_player: dict[int, int],
        map_id: int,
        game_mode: str,
        release_track: str,
    ) -> AuthoritativeGameServerAllocation: ...

    async def deliver_credential(
        self,
        allocation: AuthoritativeGameServerAllocation | None,
        annotations: dict[str, str],
    ) -> str:
        """投递凭据,返回**已确认**的 resourceVersion。"""
        ...

    async def resolve_expected_pod_uid(
        self, allocation: AuthoritativeGameServerAllocation | None
    ) -> str:
        """滚动升级期唯一允许的 pod_uid 回填(给 pod_uid 落库之前创建的旧 Redis 记录)。

        必须走 exact GameServer GET(name + UID + allocation_id 绑定)再 GET 其 owned
        Pod,且只返回该 Pod 的 UID。对象缺失 / 同名重建一律**报错**:调用方绝不能在
        删除之前猜一个身份出来。
        """
        ...

    async def release_expected(
        self, allocation: AuthoritativeGameServerAllocation | None
    ) -> None:
        """用 Kubernetes UID delete precondition 回收本实例。

        ★ 同名 GameServer 已重建时必须**失败且零删除** —— 禁止旧 cleanup 按名字误杀
          新实例(那正是「绝不删仍承载玩家的 Allocated GS」要防的形状)。
        """
        ...


@runtime_checkable
class WarmingInstanceProber(Protocol):
    """warming 冷加载宽限期 sweep 的可选加速能力(advisory)。Go: `biz.WarmingInstanceProber`。

    只读探测 exact 实例(GameServer name+UID,可选关联 Pod UID)是否已被编排层权威
    确认死亡(物理消失,或 Agones 依据 SDK health ping 判 Unhealthy)。

    ★ 只有返回 `True` 才允许 sweep 放弃 ready_wait 时间宽限提前交判弃事务;
      **任何异常都必须回退到时间界**(不实现本 Protocol 的 local/mock 亦然)。
      把"探不了"当成"已死"会在 DS 还在冷加载时把整局判弃。
    """

    async def probe_expected_instance_gone(
        self, pod_name: str, instance_uid: str, pod_uid: str
    ) -> bool: ...


@runtime_checkable
class UncertainGameServerAllocationResolver(Protocol):
    """GSA POST 结果未知之后的**只读**对账能力。Go: `biz.UncertainGameServerAllocationResolver`。

    实现按不可变的 allocation_id label 解析,返回三者之一:恰一份完整校验过的
    GameServer+Pod 身份 / 权威缺席 / 抛异常(任何未知或有歧义的结果)。
    **绝不允许再发一次分配。**

    ★ 刻意做成可选 Protocol 而不是往 `AuthoritativeGameServerAllocator` 上再加一个
      方法:滚动升级期的旧写者与测试替身不懂对账,它们必须继续停在
      `allocation_uncertain` 这道永久 fence 上 fail-closed,而不是因为多了个方法签名
      就被当作"支持对账"。
    """

    async def resolve_allocation_by_id(
        self,
        match_id: int,
        allocation_id: str,
        player_ids: list[int],
        combat_faction_by_player: dict[int, int],
        map_id: int,
        game_mode: str,
    ) -> tuple[AuthoritativeGameServerAllocation | None, bool]: ...


class MockGameServerAllocator:
    """W4 ② 打桩实现:不连 k8s,按 match_id 算确定性假地址。Go: `biz.MockGameServerAllocator`。

    端口 = `mock_ds_port_base + (match_id % mock_ds_port_range)`,保证同一 match 多次
    分配地址稳定(幂等场景下 biz 会先查镜像不重复 Allocate,这里只保证可复现)。
    """

    __slots__ = ("_cfg",)

    def __init__(self, cfg: dsconf.AllocatorConf) -> None:
        self._cfg = cfg

    async def allocate(
        self, match_id: int, map_id: int, game_mode: str, release_track: str
    ) -> tuple[str, str, str]:
        """返回确定性假 `(pod_name, addr, release_track)`。Go: `Allocate`。

        ★ `release_track` **原样回传**,不改写成 "stable"。Mock 也要遵守"回传实际命中
          的轨"这条约定,否则用 mock 跑的灰度用例会给出一个生产上不成立的结论。
        """
        if not isinstance(match_id, int) or isinstance(match_id, bool):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "mock ds: match_id must be int, got %r", match_id
            )
        if match_id < 0 or match_id > _UINT64_MAX:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "mock ds: match_id out of uint64 range: %d", match_id
            )
        port_range = self._cfg.mock_ds_port_range
        if port_range <= 0:
            # Go 侧这里会直接 panic(整数除零)。让它变成一个带业务码、说得清是哪个
            # 配置项的错误,而不是一个 ZeroDivisionError —— 后者在 access log 里
            # 看起来和"随便哪里的 bug"没有区别。
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "mock ds: mock_ds_port_range must be positive, got %d",
                port_range,
            )
        port = self._cfg.mock_ds_port_base + (match_id % port_range)
        pod_name = f"pandora-battle-{match_id}"
        addr = f"{self._cfg.mock_ds_addr_host}:{port}"
        return pod_name, addr, release_track

    async def release(self, pod_name: str) -> None:
        """对 Mock 无操作(无真实 pod 可回收)。Go: `Release`。"""
        return None
