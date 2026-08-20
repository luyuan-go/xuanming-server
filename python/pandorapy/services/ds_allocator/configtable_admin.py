"""配置表热更入口(`pandora.config.v1.ConfigTableAdminService`)—— 对应 Go 的
`pkg/configtable/admin.go`(ds_allocator 在 main.go 里 `configtable.NewAdminService(ctStore,
cfg.ConfigTable.Dir)` 接的就是它),与 `services/inventory/configtable_admin.py` 同形。

★ 只有一个 RPC:`ReloadConfigTable`。它重读 active 目录,**整批校验成功才原子切换**,
  失败保留旧快照(§9.15 标准热更流水线:版本号 + checksum + staging 目录 + reload 接口
  + 加载成功才切换 + 失败保留旧配置)。

★ 本服热更的是**关卡表**:策划改完 g_关卡.xlsx 重导表后直接 reload,新增副本立刻可开局,
  无需重启 ds_allocator。切换后生效的路径是 `catalog.Store.battle_launch_url` ——
  local_allocator 每次 allocate **现查** Store(不是启动快照),所以这里换掉 Tables
  指针就等于换掉了下一局的关卡解析结果,没有第二处内存引用要刷。

★ 纵深防御:带玩家身份的调用一律拒。本服务只在内部端口(:20020)注册,Envoy 没有本
  service 的路由 —— 这里是第二道。少了它,一个能打到内部端口的玩家就能触发全服配置重载。

★ 失败一律返回 in-band `ERR_INVALID_STATE` + detail,**不抛 gRPC 错误**:与 Go 逐字一致,
  运维靠 detail 看到到底是哪张表哪条校验没过。
"""

from __future__ import annotations

import asyncio

from pandora.common.v1 import errcode_pb2 as commonpb
from pandora.config.v1 import configtable_pb2 as cfgpb
from pandora.config.v1 import configtable_pb2_grpc as cfggrpc

from pandorapy import interceptors as pintercept
from pandorapy import log as plog
from pandorapy.services.ds_allocator import catalog as cat

GRPC_SERVICE_FULL_NAME = "pandora.config.v1.ConfigTableAdminService"


class ConfigTableAdminService(cfggrpc.ConfigTableAdminServiceServicer):
    """配置表热更。store 必须已完成启动首载(Go 侧 `ctStore.Load` 失败即 os.Exit)。"""

    __slots__ = ("_store",)

    def __init__(self, store: cat.Store) -> None:
        self._store = store

    async def ReloadConfigTable(self, request, context):  # noqa: N802
        if pintercept.extract_player_id(context) != 0:
            plog.get().warning(
                "configtable_reload_rejected",
                reason="player_jwt_not_allowed",
                expect_version=request.expect_version,
            )
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
                # 加载走线程池:json 解析 + checksum 是纯 CPU / 阻塞 IO,直接在事件循环里
                # 跑会把同进程的 sweep / capacity 循环和所有在途分配 RPC 一起卡住。
                result = await asyncio.to_thread(
                    cat.load_tables, self._store.active_dir, request.expect_version
                )
                # ★ 批次级校验器(本服登记的是"每张战斗关卡都能拼出启动 URL")在**切换前**跑:
                #   只在启动时查的话,一个坏批次热更后所有走该图的 allocate 会全部失败,
                #   而热更本身报的是成功。
                self._store.run_validators(result.tables)
            except asyncio.CancelledError:
                # ★ 3.8+ 起 CancelledError 是 BaseException,而 grpc.aio 正是**用取消**终止
                #   在途 handler。吞掉 = 停机时把取消映射成 in-band 业务码返回一个正常响应。
                raise
            except BaseException as exc:  # noqa: BLE001
                # ★ 加载 / 校验失败**保留旧快照**:半批切换比不切换危险得多
                #   (一半新图一半旧图,分配出去的 DS 会去加载一张已改名的关卡)。
                plog.get().error(
                    "configtable_reload_failed",
                    dir=self._store.active_dir,
                    expect_version=request.expect_version,
                    err=str(exc),
                )
                return cfgpb.ReloadConfigTableResponse(
                    code=commonpb.ERR_INVALID_STATE,
                    active_version=current_version,
                    detail=str(exc),
                )
            for warning in result.warnings:
                plog.get().warning("configtable_reload_warning", warning=warning)

            if result.version == current_version:
                # 版本未变 = no-op,仍返回 OK:重复触发热更不该被当成失败。
                return cfgpb.ReloadConfigTableResponse(
                    code=commonpb.OK,
                    active_version=result.version,
                    reloaded=False,
                    detail="version unchanged, no-op",
                )
            if result.version < current_version:
                # ★ 版本单调闸(防回退),对齐 Go pkg/configtable/store.go。
                # 缺了它不是"多热更一次":回退之后内存里生效的是旧批次而运维视图上是
                # 新版本号,此后所有人对"现在生效的是哪批"的判断都是错的。
                plog.get().warning(
                    "configtable_reload_rejected_rollback",
                    active_version=result.version,
                    current_version=current_version,
                    hint="active 版本低于当前生效版本,拒绝回退;要回滚请重新发布更大的版本号",
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
                levels=result.tables.level_count(),
            )
            return cfgpb.ReloadConfigTableResponse(
                code=commonpb.OK,
                active_version=result.version,
                reloaded=True,
                detail="; ".join(result.warnings),
            )


__all__ = ["GRPC_SERVICE_FULL_NAME", "ConfigTableAdminService"]
