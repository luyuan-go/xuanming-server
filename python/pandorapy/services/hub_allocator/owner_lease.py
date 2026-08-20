"""Hub 心跳里的 owner 实例租约双写门 —— 对应 Go 侧
`services/battle/hub_allocator/internal/biz/owner_lease.go`。

## 这道门在时间轴上的位置

    DS 心跳到达
      → 刷新本侧分片心跳
      → **renew_owner_lease_gate**(本文件)
      → 心跳响应返回

必须在**响应返回之前**完成:DS 拿到 200 就认为"我的租约还在,可以继续持有玩家"。
如果租约续期是异步补的,就会出现"DS 以为自己还持有、owner 那边租约已经过期"的
窗口 —— 而 §9.22 的整条脑裂防线正是靠 owner 侧那个租约截止时间划分
"旧 DS 最晚停止可玩"与"新 DS 最早开始可玩"。这个窗口一旦存在,同一玩家可以
同时在两台 DS 上可玩。

## required 的两个档位(migrate → contract)

    required=False   **弱依赖**(migrate 阶段,现网默认)。
                     双写失败只告警,心跳照常成功。理由:owner 服务刚上线时,
                     它自己的抖动不应该把整个 Hub 的心跳打挂 —— 那会让一堆
                     健康 DS 因为"心跳超时"被判掉线,是拿可用性去换一个还没
                     真正被依赖的一致性。
    required=True    **强依赖**(contract 阶段)。续租失败 → 心跳失败 →
                     DS 侧连续续租不上就自我 fencing(Kick/Despawn)。
                     这是 §9.22 要的最终形态:"续租不确定"必须按"已失租"处理。

★ 切档位不是改一行 bool:required=True 之前必须先确认 owner 服务的可用性
  已经高于 Hub 心跳链路本身,否则等于给心跳链路串了一个更脆的单点。

## 降级日志为什么要限流

弱依赖失败时业务照常成功(心跳返回 200、access log 记 rpc_ok),这条 Warn 是
"租约双写正在降级"的**唯一**信号,删不得;但心跳是每 DS 每 5s 一次的高频路径,
owner 整体不可达时逐次打会按 QPS 刷屏。故用 `logwindow.Window`:
首错必打 + 每窗口一条 + 带累计失败数(streak)。
"""

from __future__ import annotations

import asyncio
import time
from typing import Protocol

from pandorapy import log as plog
from pandorapy import logwindow

# 降级日志的最小重打间隔(毫秒),与 Go 的 ownerLeaseLogWindowMs 同值。
OWNER_LEASE_LOG_WINDOW_MS = 5000

# 模块级单例:与 Go 的 `var ownerLeaseWeakLog plog.Window` 同位置同寿命。
# ★ 刻意**不**按 pod/uid 分桶 —— 那是高基数 key,分桶会让内存随 DS 数无界增长,
#   而"哪台 DS 在降级"本来就写在日志字段里。
_owner_lease_weak_log = logwindow.Window()


class OwnerLeaseRenewer(Protocol):
    """owner 实例租约续写器。对应 Go 的 biz.OwnerLeaseRenewer。

    实现见 `owner_lease_client.GrpcOwnerLeaseRenewer`;失败以抛异常表达
    (Go 是返回 error)。
    """

    async def renew_instance_lease(
        self, pod_name: str, instance_uid: str, instance_epoch: int, release_track: str
    ) -> None: ...


async def renew_owner_lease_gate(
    renewer: OwnerLeaseRenewer | None,
    required: bool,
    pod_name: str,
    instance_uid: str,
    instance_epoch: int,
    release_track: str,
) -> None:
    """心跳响应返回前的租约双写门。对应 Go 的 renewOwnerLeaseGate。

    四条分支(与 Go 逐格对应):
        renewer 为 None            → 直接返回(未启用双写,现网行为不变)
        续期成功                    → 直接返回
        续期失败 且 required=True   → 上抛(心跳必须失败)
        续期失败 且 required=False  → 限流告警后返回(弱依赖不阻断心跳)
    """
    if renewer is None:
        return
    try:
        await renewer.renew_instance_lease(pod_name, instance_uid, instance_epoch, release_track)
        return
    except asyncio.CancelledError:
        # ★ 必须先于宽 except 放行:CancelledError 继承自 BaseException,
        #   被下面那条一起吞掉的话,停机时这条心跳路径不会退出,优雅排空失效。
        raise
    except BaseException as exc:
        if required:
            # contract 阶段:续租失败 = 心跳失败,交给 DS 侧自我 fencing。
            raise
        should_log, streak = _owner_lease_weak_log.admit(
            int(time.time() * 1000), OWNER_LEASE_LOG_WINDOW_MS
        )
        if should_log:
            plog.get().warning(
                "owner_lease_renew_failed_weak",
                pod=pod_name,
                uid=instance_uid,
                epoch=instance_epoch,
                streak=streak,
                err=str(exc),
            )
