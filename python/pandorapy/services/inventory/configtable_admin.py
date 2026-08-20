"""配置表热更入口(pandora.config.v1.ConfigTableAdminService)—— 对应 Go 的
pkg/configtable/admin.go。

★ 只有一个 RPC:ReloadConfigTable。它重读 active 目录,**整批校验成功才原子切换**,
  失败保留旧快照(§9.15 标准热更流水线)。

★ 纵深防御:带玩家身份的调用一律拒。本服务只在内部端口注册,
  Envoy 侧另有 /pandora.config.v1/ 前缀拦截 —— 这里是第二道。
  少了它,一个能打到内部端口的玩家就能触发全服配置重载。

★ 失败一律返回 in-band `ERR_INVALID_STATE` + detail,**不抛 gRPC 错误**:
  与 Go 逐字一致,运维靠 detail 看到到底是哪张表哪条校验没过。
"""

from __future__ import annotations

import asyncio

from pandora.common.v1 import errcode_pb2 as commonpb
from pandora.config.v1 import configtable_pb2 as cfgpb
from pandora.config.v1 import configtable_pb2_grpc as cfggrpc

from pandorapy import interceptors as pintercept
from pandorapy import log as plog
from pandorapy.services.inventory import catalog as cat

GRPC_SERVICE_FULL_NAME = "pandora.config.v1.ConfigTableAdminService"


class ConfigTableAdminService(cfggrpc.ConfigTableAdminServiceServicer):
    """配置表热更。store 必须已完成启动首载。"""

    def __init__(self, store: cat.Store) -> None:
        self._store = store

    async def ReloadConfigTable(self, request, context):  # noqa: N802
        if pintercept.extract_player_id(context) != 0:
            return cfgpb.ReloadConfigTableResponse(
                code=commonpb.ERR_PERMISSION_DENY,
                detail="player-facing calls are not allowed",
            )
        if self._store is None or not self._store.active_dir:
            return cfgpb.ReloadConfigTableResponse(
                code=commonpb.ERR_INVALID_STATE,
                active_version=0,
                detail="config table store is not initialized",
            )

        # ★ 整段热更持锁:load + 单调检查 + 切换必须是一个原子段。
        #   current_version **读在锁内** —— 读在 await 之前会让并发 reload
        #   拿过期基准做单调判断,版本静默回退且两次都报成功。
        async with self._store.reload_mutex:
            current_version = self._store.tables.version
            try:
                result = await asyncio.to_thread(
                    cat.load_tables, self._store.active_dir, request.expect_version
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                # ★ 加载失败**保留旧快照**:半批切换比不切换危险得多
                #   (一半新表一半旧表,外键与 pool 引用会互相对不上)。
                plog.get().warning("configtable_reload_failed", err=str(exc))
                return cfgpb.ReloadConfigTableResponse(
                    code=commonpb.ERR_INVALID_STATE,
                    active_version=current_version,
                    detail=str(exc),
                )

            if result.version == current_version:
                # 版本未变 = no-op。仍返回 OK:重复触发热更不该被当成失败。
                return cfgpb.ReloadConfigTableResponse(
                    code=commonpb.OK,
                    active_version=result.version,
                    reloaded=False,
                    detail="version unchanged, no-op",
                )
            if result.version < current_version:
                # ★ 版本单调闸(防回退),对齐 Go pkg/configtable/store.go:71-73。
                #
                # 缺了这道闸的后果**不是**"多热更一次":§9.15 要求 version 单调递增,
                # 因为一旦回退,内存里生效的是旧批次而 etcd/运维视图上是新版本号 ——
                # 之后所有人对"现在生效的是哪批"的判断都是错的。热更一旦开放给运营,
                # 误点一个旧批次就会被**静默接受**(Python 原先只判 `== current_version`,
                # 小于的一律当成"新批次"照常切)。
                plog.get().warning(
                    "configtable_reload_rejected_rollback",
                    active_version=result.version,
                    current_version=current_version,
                    hint="active 版本低于当前生效版本,拒绝回退;要回滚请重新发布一个更大的版本号",
                )
                return cfgpb.ReloadConfigTableResponse(
                    code=commonpb.ERR_INVALID_STATE,
                    active_version=current_version,
                    detail=(
                        f"active 版本 {result.version} 低于当前生效 {current_version},拒绝回退"
                    ),
                )
            self._store.replace(result.tables)
            plog.get().info(
                "configtable_reloaded",
                version=result.version,
                source_rev=result.source_rev,
                items=result.tables.item_count(),
            )
            return cfgpb.ReloadConfigTableResponse(
                code=commonpb.OK,
                active_version=result.version,
                reloaded=True,
                detail="; ".join(result.warnings),
            )
