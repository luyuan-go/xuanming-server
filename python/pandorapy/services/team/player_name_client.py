"""team → player 内部批量解析角色显示名。"""

from __future__ import annotations

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.player.v1 import player_pb2, player_pb2_grpc

from pandorapy import errcode, internalrpcauth

RESOLVE_PLAYER_NAMES_METHOD = (
    "/pandora.player.v1.PlayerInternalService/ResolvePlayerNames"
)
MAX_PLAYER_IDS = 32
DEFAULT_TIMEOUT_SEC = 0.25


class GrpcPlayerNameResolver:
    """一次有界 RPC 解析整支队伍；请求整包受 internalrpcauth 绑定。"""

    __slots__ = ("_channel", "_stub", "_signer", "_timeout_sec")

    def __init__(
        self,
        player_addr: str,
        signer: internalrpcauth.Signer,
        *,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC,
        stub=None,  # noqa: ANN001
    ) -> None:
        if signer is None:
            raise ValueError("player name resolver signer is required")
        if timeout_sec <= 0:
            raise ValueError("player name resolver timeout must be positive")
        self._channel = None
        if stub is None:
            if not player_addr:
                raise ValueError("player name resolver address is required")
            self._channel = grpc.aio.insecure_channel(player_addr)
            stub = player_pb2_grpc.PlayerInternalServiceStub(self._channel)
        self._stub = stub
        self._signer = signer
        self._timeout_sec = timeout_sec

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()

    async def resolve_player_names(self, player_ids: list[int]) -> dict[int, str]:
        """先按 raw 长度限 32，再 sort + dedupe；空批次与 0 都是调用方错误。"""
        raw_ids = [int(player_id) for player_id in player_ids]
        if not raw_ids:
            raise ValueError("player name resolver batch must not be empty")
        if len(raw_ids) > MAX_PLAYER_IDS:
            raise ValueError(
                f"player name resolver raw batch too large: "
                f"{len(raw_ids)} > {MAX_PLAYER_IDS}"
            )
        if any(player_id <= 0 for player_id in raw_ids):
            raise ValueError("player name resolver player_ids must all be positive")
        ids = sorted(set(raw_ids))

        request = player_pb2.GetPlayerNamesRequest(player_ids=ids)
        payload = request.SerializeToString(deterministic=True)
        metadata = self._signer.sign_metadata_with_payload(
            RESOLVE_PLAYER_NAMES_METHOD, ids[0], payload
        )
        response = await self._stub.ResolvePlayerNames(
            request, timeout=self._timeout_sec, metadata=metadata
        )
        if response.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                int(response.code),
                "player ResolvePlayerNames code=%d",
                int(response.code),
            )

        requested = set(ids)
        out: dict[int, str] = {}
        for name in response.names:
            player_id = int(name.player_id)
            if player_id not in requested:
                continue
            out[player_id] = str(name.nickname)
        return out
