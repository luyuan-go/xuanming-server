"""Battle DS 授权心跳里的 owner 实例租约双写门 —— 对应 Go 侧
`services/battle/ds_allocator/internal/biz/owner_lease.go`(owner-authority.md migrate ⑥)。

## 这道门在时间轴上的位置

    Battle DS 授权心跳到达
      → 校验凭据 / 刷新对局镜像的 last_heartbeat_ms
      → **renew_owner_lease_gate**(本文件)
      → 心跳响应返回

必须在**响应返回之前**完成。DS 拿到成功响应才会延长本地租约,而 §9.22 整条脑裂
防线是靠 owner 侧那个实例租约截止时间划出「旧 DS 最晚停止可玩」与「新 DS 最早
开始可玩」两条线的:玩家级 owner lease 由实例租约派生,`BeginTransition` 的
`admit_not_before` 屏障就按它算。若双写晚于响应返回,权威侧会在一段窗口里观察到
一个**偏小**的旧 deadline,于是屏障被提前打开 —— 同一玩家可以同时在两台 DS 可玩。

## required 的两个档位(migrate → contract)

    required=False   **弱依赖**(migrate 阶段,现网默认)。双写失败只告警,心跳照常
                     成功;由旧 `last_heartbeat_ms` 再入门
                     (`placement.DS_FENCE_REENTRY_BARRIER_SECONDS`)双门并行兜底。
                     理由:owner 服务刚上线时它自己的抖动不该把整批对局的心跳打挂 ——
                     那会让一堆健康 Battle DS 因「心跳超时」被判弃(§9.4 段位回滚),
                     是拿玩家已打完的一局去换一个还没真正被依赖的一致性。
    required=True    **强依赖**(contract 阶段)。续租失败 → 心跳失败;DS 拿不到响应
                     就不会延长本地租约,连续失败按 fence 契约自我 fencing。
                     也就是说权威侧租约滞后时 DS 也必然停玩,时序仍然闭合。

★ 切档位不是改一行 bool:required=True 之前必须先确认 owner 服务的可用性已经高于
  心跳链路本身,否则等于给心跳串了一个更脆的单点。

## 降级日志为什么要限流(模式 C)

弱依赖失败时业务照常成功(心跳返回 OK、access log 记 rpc_ok),这条 Warn 是
「租约双写正在降级」的**唯一**信号,删不得;但授权心跳是**每对局每 ~5s** 一次,
owner 整体不可达时逐次打会按「并发对局数 / 5s」刷屏。故用 `logwindow.Window`:
首错必打 + 每窗口一条 + 带累计失败数(streak)。
"""

from __future__ import annotations

import asyncio
import time
from typing import Protocol

from pandorapy import log as plog
from pandorapy import logwindow

# 降级日志的最小重打间隔(毫秒)。与 Go 的 `ownerLeaseLogWindowMs` 同值。
OWNER_LEASE_LOG_WINDOW_MS = 5000

# 模块级单例 —— 与 Go 的 `var ownerLeaseWeakLog plog.Window` 同位置同寿命。
#
# ★ 刻意**不**按 pod/uid 分桶:那是高基数 key,分桶会让内存随历史 Battle 实例数
#   单调增长(Battle DS 打完即销毁、UID 永不复用),而「哪台 DS 在降级」本来就写在
#   日志字段里。一个计数器 = 常量内存,这正是 Go 侧包级变量的理由。
_owner_lease_weak_log = logwindow.Window()


class OwnerLeaseRenewer(Protocol):
    """把已授权 DS 实例心跳代写进 owner 权威的实例租约。Go: `biz.OwnerLeaseRenewer`。

    实现见 `clients.GrpcOwnerLeaseRenewer`;失败以**抛异常**表达(Go 是返回 error)。
    可为 None(未配 `allocator.owner_addr` → 不双写,migrate 前行为不变)。
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
    """心跳响应返回前的租约双写门。对应 Go 的 `renewOwnerLeaseGate`。

    四条分支(与 Go 逐格对应):

        renewer 为 None            → 直接返回(未启用双写)
        续期成功                    → 直接返回
        续期失败 且 required=True   → 上抛(心跳必须失败,交给 DS 侧自我 fencing)
        续期失败 且 required=False  → 限流告警后返回(弱依赖不阻断心跳)
    """
    if renewer is None:
        return
    try:
        await renewer.renew_instance_lease(pod_name, instance_uid, instance_epoch, release_track)
        return
    except asyncio.CancelledError:
        # ★ 必须**紧邻**在宽 except 之上:CancelledError 继承自 BaseException,
        #   被下面那条一起吞掉的话,停机时这条心跳路径不会退出,优雅排空失效;
        #   required=False 时还会把一次正常取消记成一条假的「owner 降级」告警。
        raise
    except BaseException as exc:  # noqa: BLE001 —— 弱依赖要兜住全部失败形态
        if required:
            # contract 阶段:续租失败 = 心跳失败。
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
