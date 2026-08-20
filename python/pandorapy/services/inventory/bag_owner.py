"""背包域五要件② owner 授权 —— 对应 Go 侧 internal/data/owner_authorizer.go。

查 owner authority(§9.22)的记录,校验调用方声称的 (player, owner_epoch):

  - epoch 必须等于当前记录(旧 epoch = 失租旧 owner 迟到写,拒);
  - phase 必须 ADMITTED(PENDING = 屏障未开,新 DS 只可预载,不得产生业务写);
  - owner_type 必须 HUB/BATTLE(NONE = 无 owner,离线改包必须走邮件,§2);
  - 实例租约必须在效(lease_deadline > now;失租 DS 的写一律拒,§9.22 fencing)。

查询失败 / 结果不确定 → ErrUnavailable fail-closed(禁冒充"无 owner"放行);
判定不通过 → ErrBagEpochFenced(调用方停写重查,语义与存储侧 CAS fencing 一致)。

不缓存:授权结果参与权威写决策,§9.22 明令缓存不得参与;journal 批量写天然摊薄查询量。
"""

from __future__ import annotations

import time

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.owner.v1 import owner_pb2, owner_pb2_grpc

from pandorapy import errcode
from pandorapy import log as plog

# owner 查询是背包写的**同步前置**:必须有界(硬性要求 14),否则 owner 服务卡住会把
# DS 的背包写全部拖成无限等待。0.5s 与内网单跳 RPC 预算一致,超时按 ErrUnavailable
# fail-closed(调用方退避重试),不放行。
DEFAULT_TIMEOUT_SEC = 0.5


class GrpcOwnerAuthorizer:
    """用 owner 服务 gRPC client 实现 biz.OwnerAuthorizer 协议。"""

    __slots__ = ("_channel", "_stub", "_timeout_sec", "_now_ms")

    def __init__(
        self,
        owner_addr: str,
        *,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC,
        stub=None,  # noqa: ANN001 —— 测试注入
        now_ms=None,  # noqa: ANN001 —— 测试注入(默认 time.time)
    ) -> None:
        if timeout_sec <= 0:
            raise ValueError("owner authorizer timeout must be positive")
        self._channel = None
        if stub is None:
            if not owner_addr:
                raise ValueError("owner authorizer address is required")
            self._channel = grpc.aio.insecure_channel(owner_addr)
            stub = owner_pb2_grpc.OwnerServiceStub(self._channel)
        self._stub = stub
        self._timeout_sec = timeout_sec
        self._now_ms = now_ms if now_ms is not None else (lambda: int(time.time() * 1000))

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()

    async def authorize_owner_write(  # noqa: C901 —— 与 Go 同为一串线性判定
        self, player_id: int, claimed_epoch: int, caller_pod: str, caller_uid: str
    ) -> int:
        """校验并返回当前 owner_epoch。

        caller_pod 来自要件①已验身份,record.target 必须与之全等;caller_pod 空
        (guard off/dev)时必须以 claimed_epoch 作证明(=当前 epoch 才放行),两者都缺
        → fail-closed 拒。
        """
        # bag 写的 owner 授权拒绝(要件②)是 stale writer / 屏障未开 / 失租的直接证据,
        # 但都返回业务码 ErrBagEpochFenced/ErrUnauthorized(不被 access log 中间件当故障)
        # → 显式 WARN 留证。ErrUnavailable(owner 服务不可达)不在此打:它是 server fault,
        # 已由 access log 中间件升 ERROR。
        def log_reject(reason: str, **extra: object) -> None:
            plog.get().warning(
                "bag_owner_authz_rejected", player_id=player_id, reason=reason, **extra
            )

        try:
            resp = await self._stub.QueryOwner(
                owner_pb2.QueryOwnerRequest(player_id=player_id),
                timeout=self._timeout_sec,
            )
        except grpc.aio.AioRpcError as exc:
            raise errcode.PandoraError(
                errcode.ErrUnavailable, "owner query player=%d: %s", player_id, exc
            ) from exc
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                errcode.ErrUnavailable,
                "owner query player=%d code=%d",
                player_id, int(resp.code),
            )

        rec = resp.record
        if rec.owner_type not in (owner_pb2.OWNER_TYPE_HUB, owner_pb2.OWNER_TYPE_BATTLE):
            log_reject("no_active_owner", owner_type=int(rec.owner_type))
            raise errcode.PandoraError(
                errcode.ErrBagEpochFenced,
                "player=%d has no active owner (type=%d)",
                player_id, int(rec.owner_type),
            )

        if caller_pod:
            # 五要件①身份在手:owner 记录必须指向调用方实例(pod+uid 全等;旧 owner 迟到写必拒)。
            if rec.target.pod_name != caller_pod or rec.target.instance_uid != caller_uid:
                log_reject(
                    "target_mismatch",
                    caller_pod=caller_pod,
                    caller_uid=caller_uid,
                    owner_pod=rec.target.pod_name,
                    owner_uid=rec.target.instance_uid,
                )
                raise errcode.PandoraError(
                    errcode.ErrBagEpochFenced,
                    "owner target mismatch player=%d caller=%s/%s owner=%s/%s",
                    player_id, caller_pod, caller_uid,
                    rec.target.pod_name, rec.target.instance_uid,
                )
        elif claimed_epoch == 0:
            # 无验签身份(ds_auth off)时退化为 epoch 证明;两者都缺 → fail-closed。
            log_reject("missing_identity_and_epoch")
            raise errcode.PandoraError(
                errcode.ErrUnauthorized,
                "bag write requires DS credential identity or explicit owner_epoch (player=%d)",
                player_id,
            )

        if claimed_epoch != 0 and rec.owner_epoch != claimed_epoch:
            log_reject(
                "epoch_mismatch", claimed_epoch=claimed_epoch, current_epoch=rec.owner_epoch
            )
            raise errcode.PandoraError(
                errcode.ErrBagEpochFenced,
                "owner epoch mismatch player=%d claim=%d current=%d",
                player_id, claimed_epoch, rec.owner_epoch,
            )
        if rec.phase != owner_pb2.OWNER_PHASE_ADMITTED:
            log_reject("not_admitted", phase=int(rec.phase))
            raise errcode.PandoraError(
                errcode.ErrBagEpochFenced,
                "owner not admitted player=%d phase=%d",
                player_id, int(rec.phase),
            )
        if rec.lease_deadline_ms <= self._now_ms():
            log_reject("lease_expired", lease_deadline_ms=rec.lease_deadline_ms)
            raise errcode.PandoraError(
                errcode.ErrBagEpochFenced,
                "owner lease expired player=%d deadline=%d",
                player_id, rec.lease_deadline_ms,
            )
        return int(rec.owner_epoch)
