"""hub_allocator → player_locator gRPC 客户端 —— 对应 Go 侧
`internal/data/locator_client.go`(玩家主动切线护栏用)。

设计:
  - data 层暴露 `HubLocationChecker` 协议,biz 只依赖协议(便于单测注入假实现);
  - 实际实现 `GrpcHubLocationChecker` 内嵌 grpc.aio channel + PlayerLocatorServiceStub;
  - main 按配置的 locator addr 决定注入本实现还是 None(**仅限 dev 联调跳过检查,
    生产必须装配**,见 INC-20260722-002)。

────────────────────────────────────────────────────────────────────────────
★ 调用语义(TransferToLine 护栏,INC-20260722-002 修订为 **fail-closed**)

    MATCHING / BATTLE            → True(战斗 / 匹配中禁止切大厅线路)
    HUB                          → False(**唯一**放行态:presence 明确证明玩家在大厅)
    RPC 失败 / 响应非 OK /
    OFFLINE / UNSPECIFIED / 未知  → **抛异常**

  最后一行是本文件唯一重要的判据,也是最容易被"顺手优化"掉的一行:

    §9.22 —— presence 投影的 key miss / UNKNOWN **不能证明玩家已离开旧 DS**,
    因此不得授权新归属。切线会把玩家送进**另一台** Hub DS;不确定态放行 =
    潜在双 DS(§9 不变量 1)。

  原契约"locator 抖动时放行低危切线"已**废止**。看起来它只是让玩家体验更顺,
  实际是在 locator 半失效期间批量制造双 DS —— 而双 DS 不会报错,只会让两台 DS
  各自认为自己有权写这个玩家。

  biz 必须在产生**任何副作用之前**调用本检查并 fail-closed 拒绝(可重试)。
"""

from __future__ import annotations

from typing import Protocol

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.locator.v1 import locator_pb2, locator_pb2_grpc

from pandorapy import errcode
from pandorapy.protoenum import enum_name

# 单次 locator 调用的超时。★ 必须有:locator 卡住时,没有超时会把切线路径整条挂住,
# 玩家停在一个没有 deadline 的等待里(§9 不变量 19/20 明令禁止)。
DEFAULT_TIMEOUT_SEC = 3.0


class HubLocationChecker(Protocol):
    """给 hub_allocator.biz 查玩家是否在匹配 / 战斗中。

    None(未装配)仅限 dev 联调跳过检查;生产必须装配。
    """

    async def in_battle_or_matching(self, player_id: int) -> bool:
        """True = 玩家在匹配 / 战斗中(应拒绝切线);
        False 且不抛异常 = presence 明确为 HUB(唯一放行态)。
        抛异常 = presence 不能证明可切线,调用方必须 fail-closed 且零副作用。
        """

    async def refresh_hub_locations(
        self, hub_pod: str, player_ids: list[int], bearer_token: str
    ) -> int:
        """把 Hub DS 心跳捎带的在场 player_ids 转发给 locator 批量续期 HUB TTL。"""


class GrpcHubLocationChecker:
    """`HubLocationChecker` 的 gRPC 实现。对应 Go 的 `GrpcHubLocationChecker`。"""

    __slots__ = ("_channel", "_stub", "_timeout_sec", "_owns_channel")

    def __init__(
        self,
        locator_addr: str = "",
        *,
        channel: grpc.aio.Channel | None = None,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC,
    ) -> None:
        # insecure:内网直连,与 Go 的 grpcclient.MustDialInsecure 一致。
        # 允许注入现成 channel(对应 Go 的「调用方负责 conn 生命周期」),
        # 此时本对象**不负责**关闭它 —— 关掉别人的 channel 会让共享它的其它
        # client 在下一次调用时莫名其妙地拿到 CANCELLED。
        if channel is not None:
            self._channel = channel
            self._owns_channel = False
        else:
            if not locator_addr:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "locator addr required to build hub location checker"
                )
            self._channel = grpc.aio.insecure_channel(locator_addr)
            self._owns_channel = True
        self._stub = locator_pb2_grpc.PlayerLocatorServiceStub(self._channel)
        self._timeout_sec = timeout_sec

    async def close(self) -> None:
        if self._owns_channel:
            await self._channel.close()

    async def in_battle_or_matching(self, player_id: int) -> bool:
        """查玩家当前 Location。判定顺序即契约,不能重排也不能合并。

        ★ `default` 分支覆盖 OFFLINE(含 key miss / TTL 消失)、UNSPECIFIED 和
        **未来新增的状态**。写成 `if state == OFFLINE: return False` 会有两个洞:
          ① OFFLINE 只说明 presence 不可见,不能证明玩家已离开旧 DS 的战斗 / 匹配;
          ② 新增状态在旧副本上落进"未知",按放行处理就是滚动升级期的静默放行 ——
             而 proto 加枚举值是 additive、双向兼容的常规动作(§9 不变量 17),
             它随时会发生。
        """
        try:
            resp = await self._stub.GetLocation(
                locator_pb2.GetLocationRequest(player_id=player_id),
                timeout=self._timeout_sec,
            )
        except grpc.aio.AioRpcError as exc:
            # ★ 传输失败必须**抛**,不能返回 False。返回 False 的语义是
            # "已证明玩家在 Hub,可以切线" —— 那是 locator 挂掉时最危险的答案。
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "locator get location rpc failed: %s",
                exc.details() or exc.code().name,
                cause=exc,
            ) from exc
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "locator get location code=%d", int(resp.code)
            )
        state = resp.location.state
        if state in (
            locator_pb2.LOCATION_STATE_MATCHING,
            locator_pb2.LOCATION_STATE_BATTLE,
        ):
            return True
        if state == locator_pb2.LOCATION_STATE_HUB:
            return False
        # ★ 这里必须用 `enum_name` 而不是 `LocationState.Name(state)`:走到本分支的
        # 典型原因**正是** state 是个本副本不认识的新枚举值,而 Python 的 `.Name()`
        # 对未知值抛 ValueError。那会让本该 fail-closed 的路径改抛 ValueError,
        # 调用方收到 gRPC UNKNOWN 而非 ErrUnavailable,分不清"该退避重查"还是
        # "服务端有 bug"——恰好在滚动升级的混版窗口把上面那段契约打穿。
        raise errcode.PandoraError(
            errcode.ErrUnavailable,
            "player %d presence state %s cannot prove hub residency",
            player_id,
            enum_name(locator_pb2.LocationState, state),
        )

    async def refresh_hub_locations(
        self, hub_pod: str, player_ids: list[int], bearer_token: str
    ) -> int:
        """转发在场玩家列表,批量续期 HUB 位置 TTL。返回实际续期成功条数。

        ★ locator 侧只续 `state == HUB` 且 `hub_pod` 匹配的记录 —— 所以返回值
        小于 `len(player_ids)` 是正常的(有人已经进了战斗),不是错误。

        失败由调用方 best-effort 处理(不影响心跳主流程):这条与
        `in_battle_or_matching` 相反,是**保活**而不是**准入**,失败只会让某个
        玩家的 presence 早一点过期,不会放行任何写。
        """
        metadata = None
        if bearer_token:
            metadata = (("authorization", f"Bearer {bearer_token}"),)
        try:
            resp = await self._stub.RefreshHubLocations(
                locator_pb2.RefreshHubLocationsRequest(
                    hub_pod=hub_pod, player_ids=player_ids
                ),
                timeout=self._timeout_sec,
                metadata=metadata,
            )
        except grpc.aio.AioRpcError as exc:
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "locator refresh hub locations rpc failed: %s",
                exc.details() or exc.code().name,
                cause=exc,
            ) from exc
        if resp.code != errcode_pb2.OK:
            # ★ 错误码照抄 Go:这里是 ErrInternal,而 GetLocation 的非 OK 是
            # ErrUnavailable。两者不是笔误 —— 保活失败属于"本该成功的内部调用没成功",
            # 准入失败属于"暂时证明不了,稍后重试"。抄成同一个会让上层的重试策略走错。
            raise errcode.PandoraError(
                errcode.ErrInternal, "locator refresh hub locations code=%d", int(resp.code)
            )
        return int(resp.refreshed)
