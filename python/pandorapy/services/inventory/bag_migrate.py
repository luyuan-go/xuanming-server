"""旧 inventory 存量迁移用例(biz 层)。

对应 Go 侧 `services/economy/inventory/internal/biz/bag_migrate.go`
(decision-revisit-bag-replay-semantics.md D5,bag-domain.md §10 phase 3)。

职责:游标批量枚举有存量的玩家 → 读 legacy 快照 → bag 库幂等落位仓库段 → 迁后对账。
单玩家失败只计数告警不阻断(bound 实例 fail-closed 等属预期拦截,逐个排障);
全部玩家跑完即收敛,重跑 no-op。配置门 `legacy_migration_enabled` 默认关,contract 阶段
旧写路径冻结后才准开启(时序纪律见 `bag_migration.py` 文件头)。
"""

from __future__ import annotations

import asyncio
import dataclasses
from typing import Protocol

from pandora.bag.v1 import bag_pb2

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy.services.inventory import bag_apply as bapply
from pandorapy.services.inventory import conf as iconf


class LegacyBagSource(Protocol):
    """legacy 存量读取抽象(pandora_trade 库;`MySQLInventoryRepo` 实现)。"""

    async def list_legacy_bag_players(
        self, after_player_id: int, limit: int
    ) -> list[int]: ...

    async def load_legacy_bag_stock(self, player_id: int) -> list[bag_pb2.BagItem]: ...


class BagSeeder(Protocol):
    """bag 库迁移落位抽象(`MySQLBagRepo` 实现)。"""

    async def seed_legacy_warehouse(
        self, player_id: int, items, max_stack: bapply.MaxStackFn  # noqa: ANN001
    ) -> bool: ...

    async def verify_legacy_warehouse(self, player_id: int, legacy) -> None: ...  # noqa: ANN001


@dataclasses.dataclass(slots=True)
class BagMigrationSummary:
    """一轮迁移的结果统计(字段与 Go 的 BagMigrationSummary 同名同义)。"""

    scanned: int = 0  # 枚举到的玩家数
    migrated: int = 0  # 本轮真实完成迁移的玩家数
    skipped: int = 0  # 幂等闸已存在(此前已迁)的玩家数
    failed: int = 0  # 迁移或对账失败的玩家数(已告警,重跑重试)


class BagMigrationUsecase:
    """存量迁移用例。对应 Go 的 BagMigrationUsecase。"""

    __slots__ = ("_legacy", "_bag", "_cfg")

    def __init__(self, legacy: LegacyBagSource, bag: BagSeeder, cfg: iconf.BagConf) -> None:
        self._legacy = legacy
        self._bag = bag
        self._cfg = cfg

    async def run_once(self, summary: BagMigrationSummary | None = None) -> BagMigrationSummary:
        """全量跑一轮迁移(游标直至枚举耗尽或被取消)。

        幂等:已迁玩家 no-op 计 skipped。枚举本身失败(整批读不出来)向上抛 —— 那不是
        "某个玩家有问题",而是"这一轮统计不可信",继续跑只会得到一份假的 done 汇报。

        ★ `summary` 参数存在的原因:Go 的 `RunOnce` 返回 `(sum, err)`,中断时依然交出
          已累计的部分统计;Python 抛异常会把它丢掉,告警变成全 0 看不出断在哪。
          调用方传一个容器进来,就能在 except 里拿到真实进度。
        """
        if summary is None:
            summary = BagMigrationSummary()
        cursor = 0
        while True:
            players = await self._legacy.list_legacy_bag_players(
                cursor, self._cfg.migration_batch
            )
            if not players:
                return summary
            for player_id in players:
                cursor = player_id
                summary.scanned += 1
                try:
                    await self._migrate_one(player_id, summary)
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    # 单玩家失败不阻断整轮(bound 实例等预期拦截逐个排障;重跑重试)。
                    summary.failed += 1
                    plog.get().error(
                        "bag_legacy_migration_player_failed",
                        player_id=player_id,
                        code=errcode.as_code(exc),
                        err=str(exc),
                    )

    async def _migrate_one(self, player_id: int, summary: BagMigrationSummary) -> None:
        """迁移单玩家:快照 → 落位 →(真实迁移时)对账。"""
        legacy = await self._legacy.load_legacy_bag_stock(player_id)
        migrated = await self._bag.seed_legacy_warehouse(
            player_id, legacy, self._cfg.item_max_stack_of
        )
        if not migrated:
            summary.skipped += 1
            return
        # 对账用同一份快照(冻结窗口内 legacy 静止;对账失败按玩家计失败,资产已落位不回滚,
        # 漂移属冻结纪律被违反,需人工排障 —— 绝不静默)。
        await self._bag.verify_legacy_warehouse(player_id, legacy)
        summary.migrated += 1


async def run_legacy_bag_migration(uc: BagMigrationUsecase) -> None:
    """一次性存量迁移作业(D5;幂等可重跑,失败玩家逐个告警不阻断)。

    对应 Go `cmd/inventory/main.go` 的 runLegacyBagMigration,事件名逐字相同。
    """
    summary = BagMigrationSummary()
    try:
        await uc.run_once(summary)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001
        plog.get().error(
            "bag_legacy_migration_aborted",
            err=str(exc),
            scanned=summary.scanned,
            migrated=summary.migrated,
            skipped=summary.skipped,
            failed=summary.failed,
        )
        return
    if summary.failed > 0:
        plog.get().error(
            "bag_legacy_migration_done_with_failures",
            scanned=summary.scanned,
            migrated=summary.migrated,
            skipped=summary.skipped,
            failed=summary.failed,
            hint="失败玩家已逐个告警(bound 实例等预期拦截),排障后重启作业重试",
        )
        return
    plog.get().info(
        "bag_legacy_migration_done",
        scanned=summary.scanned,
        migrated=summary.migrated,
        skipped=summary.skipped,
        failed=summary.failed,
    )


__all__ = [
    "BagMigrationSummary",
    "BagMigrationUsecase",
    "BagSeeder",
    "LegacyBagSource",
    "run_legacy_bag_migration",
]
