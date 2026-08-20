"""data_service 业务逻辑层(cache-aside 读写编排)—— 对应 Go 侧 internal/biz/data.go。

一致性约定(照抄 Go 侧,这些是设计决定不是实现细节):
  - **MySQL 是事实源**,Redis 仅旁路缓存,弱一致
  - 写采用 cache-aside「先写库、后删缓存」,删失败只告警不回滚(缓存最终随 TTL 失效)
  - 不接 kafka:避免与 player.update 事件语义重复

缓存降级日志限流(模式 C):
    cache 是弱依赖(失败只影响命中率,不影响正确性),但 ReadPlayer 是本服务最热的 RPC ——
    Redis 抖动时逐请求打 Warn 会按 QPS 刷屏,且一次读可能叠 get + set 两条。
    用 logwindow.Window:首错 + 每窗口一条 + 累计数,恢复时打一条界定降级区间。
    读写两侧分开计数,便于区分是读挂还是写挂。
"""

from __future__ import annotations

import time

from pandorapy import errcode, logwindow
from pandorapy import log as plog

# 缓存降级日志的最小重打间隔(毫秒),与 Go 侧 cacheLogWindowMs 一致。
CACHE_LOG_WINDOW_MS = 5000


class DataUsecase:
    """data_service 业务逻辑核心。对应 Go 的 biz.DataUsecase。"""

    __slots__ = ("_store", "_cache", "_cfg", "_router", "_read_log", "_write_log")

    def __init__(self, store, cache, cfg) -> None:
        self._store = store
        self._cache = cache  # 弱依赖,可为 None(无缓存时直连 MySQL)
        self._cfg = cfg
        self._router = None
        self._read_log = logwindow.Window()
        self._write_log = logwindow.Window()

    def set_cell_router(self, router) -> None:
        self._router = router

    async def read_player(self, player_id: int):
        """cache-aside 读:缓存命中直返;miss 读 MySQL 并回填。

        玩家无数据 → None(由 service 转 ErrNotFound)。
        """
        if player_id == 0:
            return None

        # 1) 查缓存。读失败**只告警,继续回落 MySQL** —— 缓存挂了不能让读挂。
        if self._cache is not None:
            try:
                pd, hit = await self._cache.get(player_id)
            except Exception as exc:  # noqa: BLE001
                should_log, streak = self._read_log.admit(_now_ms(), CACHE_LOG_WINDOW_MS)
                if should_log:
                    plog.get().warning(
                        "cache_get_failed", player_id=player_id, streak=streak, err=str(exc)
                    )
            else:
                failed, _ = self._read_log.recovered()
                if failed > 0:
                    plog.get().info("cache_get_recovered", failed_total=failed)
                if hit:
                    return pd

        # 2) 读 MySQL(事实源)。
        try:
            pd = await self._store.read(player_id)
        except Exception as exc:  # noqa: BLE001
            # 读失败经 service 层 in-band Code + 成功的 gRPC status 返回,access log 记 rpc_ok。
            # 若不在这里显式 ERROR,**源库读故障线上零可见**(§16 禁止静默吞错)。
            plog.get().error("player_read_failed", player_id=player_id, err=str(exc))
            raise
        if pd is None:
            return None

        # 3) 回填缓存(失败只告警)。
        await self._fill_cache(pd)
        return pd

    async def write_player(self, pd, update_fields: list[str]) -> int:
        """乐观锁写 MySQL,成功后删缓存。返回写入后的新版本号。"""
        if pd.player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")

        # ★ 校验 update_mask(仅对更新有意义;新建整条 INSERT 时掩码无效)。
        if pd.version > 0:
            if not update_fields:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "update_mask required for player_data %d update "
                    "(empty mask would overwrite unknown new columns)",
                    pd.player_id,
                )
            from pandorapy.services.data_service import data as ddata

            for field in update_fields:
                if not ddata.is_updatable_field(field):
                    raise errcode.PandoraError(
                        errcode.ErrInvalidArg,
                        "invalid update_mask path %r (not an updatable player_data field)",
                        field,
                    )

        try:
            new_version = await self._store.write(pd, update_fields)
        except Exception as exc:  # noqa: BLE001
            # 版本不匹配是**良性**乐观锁竞争(DEBUG);其余是源库写故障,必须 ERROR ——
            # 否则经 in-band Code 返回被 access log 记成 rpc_ok,与良性冲突无法区分。
            if errcode.as_code(exc) == errcode.ErrDataVersionMismatch:
                plog.get().debug(
                    "player_write_version_mismatch",
                    player_id=pd.player_id,
                    expect_version=pd.version,
                )
            else:
                plog.get().error(
                    "player_write_failed",
                    player_id=pd.player_id,
                    version=pd.version,
                    err=str(exc),
                )
            raise

        # 写后删缓存(避免读到旧版本)。删失败只告警,缓存随 TTL 自然失效。
        if self._cache is not None:
            try:
                await self._cache.delete(pd.player_id)
            except Exception as exc:  # noqa: BLE001
                should_log, streak = self._write_log.admit(_now_ms(), CACHE_LOG_WINDOW_MS)
                if should_log:
                    plog.get().warning(
                        "cache_del_after_write_failed",
                        player_id=pd.player_id,
                        streak=streak,
                        err=str(exc),
                    )

        self._log_placement(pd.player_id, "write_player")
        return new_version

    async def invalidate_cache(self, player_id: int) -> None:
        """主动删缓存(供上层在外部直写 DB 后强制失效)。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if self._cache is None:
            return
        try:
            await self._cache.delete(player_id)
        except Exception as exc:  # noqa: BLE001
            plog.get().warning("cache_invalidate_failed", player_id=player_id, err=str(exc))
            raise

    async def _fill_cache(self, pd) -> None:
        """回填缓存,失败只告警(缓存是旁路,不影响读正确性)。"""
        if self._cache is None:
            return
        try:
            await self._cache.set(pd, self._cfg.cache_ttl_td())
        except Exception as exc:  # noqa: BLE001
            # 每次 miss 都会走这里,Redis 挂时与 get 侧叠成每请求两条 → 同样限流
            # (与 del-after-write 共用写侧计数器)。
            should_log, streak = self._write_log.admit(_now_ms(), CACHE_LOG_WINDOW_MS)
            if should_log:
                plog.get().warning(
                    "cache_set_failed", player_id=pd.player_id, streak=streak, err=str(exc)
                )
        else:
            failed, _ = self._write_log.recovered()
            if failed > 0:
                plog.get().info("cache_write_recovered", failed_total=failed)

    def _log_placement(self, player_id: int, op: str) -> None:
        """PlayerData 行是 owner 数据,锁定玩家 owner cell。router 为 None → 不打。"""
        if self._router is None:
            return
        try:
            loc = self._router.route(player_id)
        except Exception:  # noqa: BLE001
            return
        plog.get().debug(
            "player_data_placement",
            player_id=player_id,
            op=op,
            region=loc.region_id,
            cell=loc.cell_id,
        )


def _now_ms() -> int:
    return int(time.time() * 1000)
