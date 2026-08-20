"""friend → player_locator gRPC 客户端 —— 对应 Go 侧 internal/data/locator_client.go。

设计:
  - biz 只依赖 `batch_online(ids) -> {player_id: OnlineStatus}` 这一个方法;
  - main 按 friend.locator_addr 决定注入本实现还是 None(**弱依赖**);
  - locator 不可达 / 整体失败 → 返回空 dict(全部按离线),**绝不让 ListFriends 整体失败**
    —— 好友列表是只读展示,在线状态可降级;为了一个绿点让整个面板打不开是本末倒置。

一次 BatchGetLocation 批量查,而不是逐好友 N 次 unary 扇出
(docs/design/friend-distributed-scaling.md §13.3:服务端 Redis pipeline 一次往返)。
"""

from __future__ import annotations

import dataclasses
import time

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.locator.v1 import locator_pb2, locator_pb2_grpc

from pandorapy import log as plog
from pandorapy import logwindow

# 降级日志的最小重打间隔(毫秒),与 Go 的 locatorLogWindowMs 同值。
LOCATOR_LOG_WINDOW_MS = 5000

# 单次 BatchGetLocation 的超时。★ 必须有:locator 卡住时,没有超时会把
# ListFriends 这条高频只读路径整条挂住 —— 弱依赖反而变成了可用性单点。
DEFAULT_TIMEOUT_SEC = 3.0


@dataclasses.dataclass(frozen=True, slots=True)
class OnlineStatus:
    """单个玩家的在线状态(biz 据此填 FriendInfo.is_online / last_seen_ms)。"""

    online: bool
    last_seen_ms: int


class GrpcOnlineStatusReader:
    """用 player_locator gRPC client 批量查在线状态。对应 Go 的 GrpcOnlineStatusReader。"""

    __slots__ = ("_channel", "_stub", "_timeout_sec", "_degrade_log")

    def __init__(self, locator_addr: str, *, timeout_sec: float = DEFAULT_TIMEOUT_SEC) -> None:
        # insecure:内网直连,与 Go 的 grpcclient.MustDialInsecure 一致。
        self._channel = grpc.aio.insecure_channel(locator_addr)
        self._stub = locator_pb2_grpc.PlayerLocatorServiceStub(self._channel)
        self._timeout_sec = timeout_sec
        # 降级日志限流(模式 C):BatchOnline 挂在社交面板的高频只读路径上,而 locator
        # 失败时请求仍然成功(access log 记 rpc_ok)—— 这条 Warn 是"在线态降级"的
        # **唯一**信号,不能删;但整体不可达时逐请求打会按 QPS 刷屏,故限流为
        # 首错 + 每窗口一条 + 累计数。
        self._degrade_log = logwindow.Window()

    async def close(self) -> None:
        await self._channel.close()

    async def batch_online(self, player_ids: list[int]) -> dict[int, OnlineStatus]:
        """一次 BatchGetLocation 批量查。

        - 整批失败 / locator 不可达 → 返回空 dict(全部按离线);
        - 响应里缺席的好友按离线处理(调用方默认 False);
        - state != OFFLINE 且 != UNSPECIFIED 视为在线
          (★ UNSPECIFIED 必须算离线:它代表"查不到 / 说不准",按在线会让面板
           在 locator 半失效时显示一片假在线,玩家点进去全是空)。
        """
        out: dict[int, OnlineStatus] = {}
        if not player_ids:
            return out
        logger = plog.get()
        try:
            resp = await self._stub.BatchGetLocation(
                locator_pb2.BatchGetLocationRequest(player_ids=player_ids),
                timeout=self._timeout_sec,
            )
        except Exception as exc:  # noqa: BLE001 —— 弱依赖:任何传输失败都降级为"全离线"
            ok, streak = self._degrade_log.admit(int(time.time() * 1000), LOCATOR_LOG_WINDOW_MS)
            if ok:
                logger.warning(
                    "locator_batch_get_location_failed",
                    count=len(player_ids),
                    streak=streak,
                    err=str(exc),
                )
            return out
        if resp.code != errcode_pb2.OK:
            ok, streak = self._degrade_log.admit(int(time.time() * 1000), LOCATOR_LOG_WINDOW_MS)
            if ok:
                logger.warning(
                    "locator_batch_get_location_not_ok",
                    count=len(player_ids),
                    streak=streak,
                    code=int(resp.code),
                )
            return out
        failed_total, _ = self._degrade_log.recovered()
        if failed_total > 0:
            # 恢复日志给降级区间画右边界 —— 只有"开始降级"没有"恢复了",
            # 值班的人无法判断故障是否还在持续。
            logger.info("locator_batch_get_location_recovered", failed_total=failed_total)

        for pid, loc in resp.locations.items():
            state = loc.state
            online = (
                state != locator_pb2.LOCATION_STATE_OFFLINE
                and state != locator_pb2.LOCATION_STATE_UNSPECIFIED
            )
            out[int(pid)] = OnlineStatus(online=online, last_seen_ms=int(loc.updated_at_ms))
        return out
