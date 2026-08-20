"""配置表热更入口(pandora.config.v1.ConfigTableAdminService)—— 对应 Go 的
pkg/configtable/admin.go,由 player 的 gRPC server **条件注册**(store 装配成功才注册)。

★ 只有一个 RPC:ReloadConfigTable。它重读 active 目录,**整批校验成功才原子切换**,
  失败保留旧快照(§9.15)。

★ player 特有的一条热更门禁:**不得缩短最高等级**。新批次的 player_level_exp 行数若
  小于当前生效批次,直接拒绝换批 —— 否则已有高等级玩家会在后续 AddExperience 时按新
  上限被重新结算,等级凭空掉下去且不可逆。这条通过 load_tables(current_max_level=...)
  传下去,与启动首载共用同一个校验入口。

★ 纵深防御:带玩家身份的调用一律拒。本服务只在内部端口注册,Envoy 侧另有
  /pandora.config.v1/ 前缀拦截 —— 这里是第二道。少了它,一个能打到内部端口的玩家就能
  触发全服配置重载。

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
from pandorapy.services.player import tables as ptables

GRPC_SERVICE_FULL_NAME = "pandora.config.v1.ConfigTableAdminService"


class ConfigTableAdminService(cfggrpc.ConfigTableAdminServiceServicer):
    """配置表热更。store 必须已完成启动首载。"""

    def __init__(self, store: ptables.Store) -> None:
        self._store = store

    async def ReloadConfigTable(self, request, context):  # noqa: N802
        if pintercept.extract_player_id(context) != 0:
            return cfgpb.ReloadConfigTableResponse(
                code=commonpb.ERR_PERMISSION_DENY,
                detail="player-facing calls are not allowed",
            )
        current_version = self._store.tables.version if self._store is not None else 0
        if self._store is None or not self._store.active_dir:
            return cfgpb.ReloadConfigTableResponse(
                code=commonpb.ERR_INVALID_STATE,
                active_version=current_version,
                detail="config table store is not initialized",
            )
        current_max_level = self._store.tables.max_level()
        try:
            result = await asyncio.to_thread(
                ptables.load_tables,
                self._store.active_dir,
                request.expect_version,
                current_max_level=current_max_level,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # ★ 加载失败**保留旧快照**:半批切换比不切换危险得多(一半新表一半旧表,
            #   专精前置与技能卡曲线会互相对不上)。
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
        self._store.replace(result.tables)
        plog.get().info(
            "configtable_reloaded",
            version=result.version,
            source_rev=result.source_rev,
            max_level=result.tables.max_level(),
        )
        return cfgpb.ReloadConfigTableResponse(
            code=commonpb.OK,
            active_version=result.version,
            reloaded=True,
            detail="; ".join(result.warnings),
        )
