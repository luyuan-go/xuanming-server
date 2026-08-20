"""team → login 内部批量解析玩家展示编号。"""

from __future__ import annotations

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.login.v1 import login_pb2, login_pb2_grpc

from pandorapy import internalrpcauth

RESOLVE_PLAYER_NOS_METHOD = "/pandora.login.v1.LoginInternalService/ResolvePlayerNos"
MAX_PLAYER_IDS = 32
# 这条弱依赖位于写响应 / 推送路径；250ms 与 Go 侧及 login 单次编号读取预算一致。
DEFAULT_TIMEOUT_SEC = 0.25


class GrpcPlayerNoResolver:
    """一次有界 RPC 解析整支队伍；请求整包受 internalrpcauth 绑定。"""

    __slots__ = ("_channel", "_stub", "_signer", "_timeout_sec")

    def __init__(
        self,
        login_addr: str,
        signer: internalrpcauth.Signer,
        *,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC,
        stub=None,  # noqa: ANN001 —— 测试注入生成 stub 的最小替身
    ) -> None:
        if signer is None:
            raise ValueError("player_no resolver signer is required")
        if timeout_sec <= 0:
            raise ValueError("player_no resolver timeout must be positive")
        self._channel = None
        if stub is None:
            if not login_addr:
                raise ValueError("player_no resolver login address is required")
            self._channel = grpc.aio.insecure_channel(login_addr)
            stub = login_pb2_grpc.LoginInternalServiceStub(self._channel)
        self._stub = stub
        self._signer = signer
        self._timeout_sec = timeout_sec

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()

    async def resolve_player_nos(self, player_ids: list[int]) -> dict[int, int]:
        """先按 raw 长度限 32，再 sort + dedupe；空批次与 0 都是调用方错误。"""
        raw_ids = [int(pid) for pid in player_ids]
        if not raw_ids:
            raise ValueError("player_no resolver batch must not be empty")
        if len(raw_ids) > MAX_PLAYER_IDS:
            raise ValueError(
                f"player_no resolver raw batch too large: {len(raw_ids)} > {MAX_PLAYER_IDS}"
            )
        if any(pid <= 0 for pid in raw_ids):
            raise ValueError("player_no resolver player_ids must all be positive")
        ids = sorted(set(raw_ids))

        request = login_pb2.ResolvePlayerNosRequest(player_ids=ids)
        payload = request.SerializeToString(deterministic=True)
        metadata = self._signer.sign_metadata_with_payload(
            RESOLVE_PLAYER_NOS_METHOD, ids[0], payload
        )
        response = await self._stub.ResolvePlayerNos(
            request, timeout=self._timeout_sec, metadata=metadata
        )
        if response.code != errcode_pb2.OK:
            raise RuntimeError(f"ResolvePlayerNos returned code={int(response.code)}")

        requested = set(ids)
        out: dict[int, int] = {}
        for entry in response.entries:
            player_id = int(entry.player_id)
            player_no = int(entry.player_no)
            if player_id not in requested:
                continue
            previous = out.get(player_id)
            if previous is not None and previous != player_no:
                raise RuntimeError(
                    f"ResolvePlayerNos conflicting duplicate player_id={player_id}"
                )
            out[player_id] = player_no
        return out
