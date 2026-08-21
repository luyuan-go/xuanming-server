"""客户端申请列表所需的公开玩家展示投影。

昵称与 ``player_no`` 分属 player/login 两个权威，调用方只保存不可变
``player_id``。这里统一做稳定去重、32-ID 分块和总并发限制；单批或单权威失败
只丢对应投影，绝不把弱依赖故障升级成申请列表不可用。
"""

from __future__ import annotations

import asyncio
from typing import Protocol

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.login.v1 import login_pb2, login_pb2_grpc
from pandora.player.v1 import player_pb2, player_pb2_grpc

from pandorapy import errcode, internalrpcauth
from pandorapy import log as plog

MAX_PLAYER_IDS_PER_BATCH = 32
MAX_CONCURRENT_BATCHES = 4
DEFAULT_TIMEOUT_SEC = 0.25
RESOLVE_PLAYER_NAMES_METHOD = (
    "/pandora.player.v1.PlayerInternalService/ResolvePlayerNames"
)
RESOLVE_PLAYER_NOS_METHOD = "/pandora.login.v1.LoginInternalService/ResolvePlayerNos"


class PlayerNameResolver(Protocol):
    async def resolve_player_names(self, player_ids: list[int]) -> dict[int, str]: ...


class PlayerNoResolver(Protocol):
    async def resolve_player_nos(self, player_ids: list[int]) -> dict[int, int]: ...


class GrpcPlayerNameResolver:
    """调用 player 权威批量解析角色昵称；每次请求都做精确载荷签名。"""

    __slots__ = ("_channel", "_stub", "_signer", "_timeout_sec")

    def __init__(
        self,
        addr: str,
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
            if not addr:
                raise ValueError("player name resolver address is required")
            self._channel = grpc.aio.insecure_channel(addr)
            stub = player_pb2_grpc.PlayerInternalServiceStub(self._channel)
        self._stub = stub
        self._signer = signer
        self._timeout_sec = timeout_sec

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()

    async def resolve_player_names(self, player_ids: list[int]) -> dict[int, str]:
        raw_ids = [int(player_id) for player_id in player_ids]
        if not raw_ids or len(raw_ids) > MAX_PLAYER_IDS_PER_BATCH:
            raise ValueError("player name resolver batch size must be within [1,32]")
        if any(player_id <= 0 for player_id in raw_ids):
            raise ValueError("player name resolver player_ids must all be positive")
        ids = sorted(set(raw_ids))
        request = player_pb2.GetPlayerNamesRequest(player_ids=ids)
        payload = request.SerializeToString(deterministic=True)
        response = await self._stub.ResolvePlayerNames(
            request,
            timeout=self._timeout_sec,
            metadata=self._signer.sign_metadata_with_payload(
                RESOLVE_PLAYER_NAMES_METHOD, ids[0], payload
            ),
        )
        if response.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                int(response.code), "player ResolvePlayerNames code=%d", int(response.code)
            )
        requested = set(ids)
        return {
            int(item.player_id): str(item.nickname)
            for item in response.names
            if int(item.player_id) in requested
        }


class GrpcPlayerNoResolver:
    """调用 login 权威批量解析玩家编号；每次请求都做精确载荷签名。"""

    __slots__ = ("_channel", "_stub", "_signer", "_timeout_sec")

    def __init__(
        self,
        addr: str,
        signer: internalrpcauth.Signer,
        *,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC,
        stub=None,  # noqa: ANN001
    ) -> None:
        if signer is None:
            raise ValueError("player_no resolver signer is required")
        if timeout_sec <= 0:
            raise ValueError("player_no resolver timeout must be positive")
        self._channel = None
        if stub is None:
            if not addr:
                raise ValueError("player_no resolver address is required")
            self._channel = grpc.aio.insecure_channel(addr)
            stub = login_pb2_grpc.LoginInternalServiceStub(self._channel)
        self._stub = stub
        self._signer = signer
        self._timeout_sec = timeout_sec

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()

    async def resolve_player_nos(self, player_ids: list[int]) -> dict[int, int]:
        raw_ids = [int(player_id) for player_id in player_ids]
        if not raw_ids or len(raw_ids) > MAX_PLAYER_IDS_PER_BATCH:
            raise ValueError("player_no resolver batch size must be within [1,32]")
        if any(player_id <= 0 for player_id in raw_ids):
            raise ValueError("player_no resolver player_ids must all be positive")
        ids = sorted(set(raw_ids))
        request = login_pb2.ResolvePlayerNosRequest(player_ids=ids)
        payload = request.SerializeToString(deterministic=True)
        response = await self._stub.ResolvePlayerNos(
            request,
            timeout=self._timeout_sec,
            metadata=self._signer.sign_metadata_with_payload(
                RESOLVE_PLAYER_NOS_METHOD, ids[0], payload
            ),
        )
        if response.code != errcode_pb2.OK:
            raise RuntimeError(f"ResolvePlayerNos returned code={int(response.code)}")
        requested = set(ids)
        out: dict[int, int] = {}
        for item in response.entries:
            player_id = int(item.player_id)
            player_no = int(item.player_no)
            if player_id not in requested:
                continue
            previous = out.get(player_id)
            if previous is not None and previous != player_no:
                raise RuntimeError(
                    f"ResolvePlayerNos conflicting duplicate player_id={player_id}"
                )
            out[player_id] = player_no
        return out


def _stable_positive_ids(player_ids: list[int]) -> list[int]:
    seen: set[int] = set()
    out: list[int] = []
    for raw in player_ids:
        player_id = int(raw)
        if player_id <= 0 or player_id in seen:
            continue
        seen.add(player_id)
        out.append(player_id)
    return out


def _chunks(player_ids: list[int]) -> list[list[int]]:
    return [
        player_ids[index : index + MAX_PLAYER_IDS_PER_BATCH]
        for index in range(0, len(player_ids), MAX_PLAYER_IDS_PER_BATCH)
    ]


async def resolve_player_display(
    player_ids: list[int],
    name_resolver: PlayerNameResolver | None,
    no_resolver: PlayerNoResolver | None,
    *,
    service: str,
) -> tuple[dict[int, str], dict[int, int]]:
    """分别解析角色昵称和玩家编号，返回两张可独立缺失的投影表。"""
    ids = _stable_positive_ids(player_ids)
    if not ids or (name_resolver is None and no_resolver is None):
        return {}, {}

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)

    async def resolve_names(batch: list[int]) -> tuple[str, dict]:
        async with semaphore:
            try:
                values = await name_resolver.resolve_player_names(batch)  # type: ignore[union-attr]
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 —— 展示弱依赖按批 fail-soft
                plog.get().warning(
                    "player_display_projection_failed",
                    service=service,
                    projection="player_name",
                    batch_size=len(batch),
                    err=str(exc),
                    fail_soft=True,
                )
                return "name", {}
            requested = set(batch)
            return "name", {
                int(player_id): str(name)
                for player_id, name in values.items()
                if int(player_id) in requested
            }

    async def resolve_nos(batch: list[int]) -> tuple[str, dict]:
        async with semaphore:
            try:
                values = await no_resolver.resolve_player_nos(batch)  # type: ignore[union-attr]
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 —— 展示弱依赖按批 fail-soft
                plog.get().warning(
                    "player_display_projection_failed",
                    service=service,
                    projection="player_no",
                    batch_size=len(batch),
                    err=str(exc),
                    fail_soft=True,
                )
                return "no", {}
            requested = set(batch)
            return "no", {
                int(player_id): int(player_no)
                for player_id, player_no in values.items()
                if int(player_id) in requested and int(player_no) > 0
            }

    jobs = []
    batches = _chunks(ids)
    if name_resolver is not None:
        jobs.extend(resolve_names(batch) for batch in batches)
    if no_resolver is not None:
        jobs.extend(resolve_nos(batch) for batch in batches)

    tasks = [asyncio.create_task(job) for job in jobs]
    try:
        results = await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    names: dict[int, str] = {}
    numbers: dict[int, int] = {}
    for kind, values in results:
        if kind == "name":
            names.update(values)
        else:
            numbers.update(values)
    return names, numbers
