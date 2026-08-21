"""旧 inventory 存量 → 背包域仓库段迁移(data 层)。

对应 Go 侧 `services/economy/inventory/internal/data/bag_migration.go`
(decision-revisit-bag-replay-semantics.md D5,bag-domain.md §10 phase 3)。

时序纪律(与 Go 逐字同源):迁移作业只能在旧写路径(GrantItems / UseItem / SellItem /
escrow)全部冻结后运行(biz 配置门 `legacy_migration_enabled` 默认关,contract 阶段才开);
跨库(pandora_trade → pandora_bag)无法同事务,幂等由 `bag_migration` **一玩家一行永久闸**
承担:

  - 读侧:legacy 快照为普通读(冻结窗口内静止);
  - 写侧:bag 库单事务 = 锁 bag_meta 行(串行化该玩家全部背包写,**不 CAS epoch** ——
    迁移不是 owner 写者,不得推进 / 受制于 owner_epoch)→ 查迁移闸 → 仓库段合并入段
    (容量豁免,超容落位只出不进,§3.2)→ upsert bag_section + INSERT bag_migration,原子提交。
  - 玩家在线也安全:仓库是后端驻留段(存储侧权威,不 checkout 进 DS),并发的 journal 写
    与迁移写都锁同一 bag_meta 行,天然串行。

bound 实例 fail-closed:`BagItem` 尚无 bound 字段(phase 3 proto 批次补),绑定实例迁移
会静默丢失绑定约束 → 整玩家拒迁并报错,绝不静默降级(同 §7.1 transfer 接线前纪律)。

★ 为什么不能在 Python 侧"打个 WARN 跳过":静默跳过的后果是运维以为迁移在跑、实际一行没搬,
  contract 阶段冻结旧写路径后玩家的存量道具凭空消失(§14 接线完整性)。
"""

from __future__ import annotations

from pandora.bag.v1 import bag_pb2

from pandorapy import errcode
from pandorapy.services.inventory import bag_apply as bapply
from pandorapy.services.inventory import repo_instance as rinst
from pandorapy.services.inventory import repo_sql as rsql

# uint32 上限;stack count 溢出检查用(Go 侧 count 列是有符号 BIGINT,转 uint32 前必须查)。
_UINT32_MAX = bapply.UINT32_MAX

# 游标枚举:两表并集(堆叠 count>0 或持有实例),升序,LIMIT。与 Go 逐字同一条 SQL。
_LIST_LEGACY_PLAYERS = """
SELECT player_id FROM (
  SELECT DISTINCT player_id FROM player_items WHERE count > 0 AND player_id > %s
  UNION
  SELECT DISTINCT player_id FROM player_item_instance WHERE player_id > %s
) u ORDER BY player_id LIMIT %s"""

_LOAD_LEGACY_STACKS = (
    "SELECT item_config_id, count FROM player_items "
    "WHERE player_id = %s AND count > 0 ORDER BY item_config_id"
)

_LOAD_LEGACY_INSTANCES = (
    "SELECT instance_id, item_config_id, identified, attributes, bound "
    "FROM player_item_instance WHERE player_id = %s ORDER BY instance_id"
)


def legacy_migration_totals(items) -> tuple[int, int, int]:  # noqa: ANN001
    """统计 legacy 快照的对账三元组 (stack_kinds, stack_total, instance_count)。

    与 Go 的 legacyMigrationTotals 同口径:实例条目只计件数不进堆叠统计。
    """
    kinds: set[int] = set()
    stack_total = 0
    instance_count = 0
    for it in items:
        if it.instance_id != 0:
            instance_count += 1
            continue
        kinds.add(it.item_config_id)
        stack_total += it.count
    return len(kinds), stack_total, instance_count


class LegacyBagSourceMixin:
    """`MySQLInventoryRepo` 的 legacy 存量读取部分(pandora_trade 库)。

    `self._pool` 由主类提供。对应 Go 的 `MySQLInventoryRepo` 上同名两个方法。
    """

    __slots__ = ()

    async def list_legacy_bag_players(self, after_player_id: int, limit: int) -> list[int]:
        """游标枚举仍有存量(堆叠 count>0 或持有实例)的玩家(升序,含两表并集)。"""
        if limit <= 0:
            return []
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(
                    _LIST_LEGACY_PLAYERS, (after_player_id, after_player_id, limit)
                )
                rows = await cur.fetchall()
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "list legacy bag players after=%d: %s",
                after_player_id, exc,
            ) from exc
        return [int(r[0]) for r in rows or ()]

    async def load_legacy_bag_stock(self, player_id: int) -> list[bag_pb2.BagItem]:
        """读取单玩家 legacy 存量快照(堆叠 + 实例),转成 bag 域 BagItem 形状。

        bound=1 实例 fail-closed 拒(见模块头);attributes pb 解码失败同样拒
        (不静默丢词条)。
        """
        items: list[bag_pb2.BagItem] = []

        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(_LOAD_LEGACY_STACKS, (player_id,))
                stack_rows = await cur.fetchall()
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "load legacy stacks player=%d: %s", player_id, exc
            ) from exc
        for config_id, count in stack_rows or ():
            config_id = int(config_id)
            count = int(count)
            if count <= 0:
                continue
            if count > _UINT32_MAX:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "legacy stack count overflows uint32 player=%d config=%d count=%d",
                    player_id, config_id, count,
                )
            items.append(bag_pb2.BagItem(item_config_id=config_id, count=count))

        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(_LOAD_LEGACY_INSTANCES, (player_id,))
                inst_rows = await cur.fetchall()
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "load legacy instances player=%d: %s", player_id, exc
            ) from exc
        for instance_id, config_id, identified, attrs_raw, bound in inst_rows or ():
            instance_id = int(instance_id)
            if bound:
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "legacy bound instance blocks migration player=%d instance=%d "
                    "(BagItem 尚无 bound 字段,phase 3 proto 批次补齐后放开;拒迁防绑定约束静默丢失)",
                    player_id, instance_id,
                )
            item = bag_pb2.BagItem(
                item_config_id=int(config_id),
                count=1,
                instance_id=instance_id,
                identified=bool(identified),
            )
            try:
                attrs = rinst.decode_instance_attrs(attrs_raw)
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "decode legacy instance attrs player=%d instance=%d: %s",
                    player_id, instance_id, exc,
                ) from exc
            for a in attrs:
                item.attrs.add(attr_id=a.attr_id, value=a.value)
            items.append(item)
        return items


class BagSeederMixin:
    """`MySQLBagRepo` 的迁移落位部分(pandora_bag 库)。`self._pool` 由主类提供。"""

    __slots__ = ()

    async def seed_legacy_warehouse(
        self, player_id: int, items, max_stack: bapply.MaxStackFn  # noqa: ANN001
    ) -> bool:
        """把 legacy 快照合并进仓库段(bag 库单事务;容量豁免超容落位)。

        返回 True = 本次完成迁移;False = 迁移闸已存在(幂等重放 no-op)。
        """
        async with rsql.transaction(self._pool) as cur:
            # 锁 bag_meta 行串行化该玩家全部背包写;迁移不是 owner 写者,不 CAS 不推进 epoch。
            try:
                await cur.execute(
                    "INSERT IGNORE INTO bag_meta (player_id, owner_epoch, last_journal_seq) "
                    "VALUES (%s, 0, 0)",
                    (player_id,),
                )
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "ensure bag_meta player=%d: %s", player_id, exc
                ) from exc
            try:
                await cur.execute(
                    "SELECT owner_epoch FROM bag_meta WHERE player_id = %s FOR UPDATE",
                    (player_id,),
                )
                meta_row = await cur.fetchone()
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "lock bag_meta player=%d: %s", player_id, exc
                ) from exc
            if meta_row is None:
                raise errcode.PandoraError(
                    errcode.ErrInternal, "lock bag_meta player=%d: row vanished", player_id
                )

            # 幂等闸:行已存在 = 已迁移,no-op(多副本并发 / 断点重跑安全)。
            try:
                await cur.execute(
                    "SELECT 1 FROM bag_migration WHERE player_id = %s", (player_id,)
                )
                gate_row = await cur.fetchone()
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "check bag_migration player=%d: %s", player_id, exc
                ) from exc
            if gate_row is not None:
                return False

            stack_kinds, stack_total, instance_count = legacy_migration_totals(items)
            if items:
                # 加载仓库段既有内容(phase 2 期间的转移 / 领取可能已建段),合并入段。
                sec = bag_pb2.BagSection(bag_type=bapply.BAG_WAREHOUSE_TYPE)
                try:
                    await cur.execute(
                        "SELECT section FROM bag_section "
                        "WHERE player_id = %s AND bag_type = %s FOR UPDATE",
                        (player_id, bapply.BAG_WAREHOUSE_TYPE),
                    )
                    sec_row = await cur.fetchone()
                except Exception as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "lock warehouse section player=%d: %s", player_id, exc,
                    ) from exc
                if sec_row is not None:
                    # read-modify-write 路径:不调用任何 DiscardUnknownFields(§9 不变量 17)。
                    try:
                        sec.ParseFromString(bytes(sec_row[0] or b""))
                    except Exception as exc:  # noqa: BLE001
                        raise errcode.PandoraError(
                            errcode.ErrInternal,
                            "decode warehouse section player=%d: %s", player_id, exc,
                        ) from exc
                    sec.bag_type = bapply.BAG_WAREHOUSE_TYPE
                # 容量豁免:capacity 传 uint32 上限 = 迁移一次性超容落位;其后新开格被真实
                # 容量拒(section_add_items 容量门只拦新开格),即 §3.2 的"只出不进",随取用自愈。
                # 拆堆 / 实例查重复用 journal 写路径同一函数,语义单源。
                bapply.section_add_items(sec, items, _UINT32_MAX, max_stack)
                sec_blob = sec.SerializeToString()
                try:
                    await cur.execute(
                        "INSERT INTO bag_section (player_id, bag_type, generation, section) "
                        "VALUES (%s, %s, 0, %s) "
                        "ON DUPLICATE KEY UPDATE section = VALUES(section)",
                        (player_id, bapply.BAG_WAREHOUSE_TYPE, sec_blob),
                    )
                except Exception as exc:  # noqa: BLE001
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "upsert warehouse section player=%d: %s", player_id, exc,
                    ) from exc

            try:
                await cur.execute(
                    "INSERT INTO bag_migration "
                    "(player_id, stack_kinds, stack_total, instance_count) "
                    "VALUES (%s, %s, %s, %s)",
                    (player_id, stack_kinds, stack_total, instance_count),
                )
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal, "insert bag_migration player=%d: %s", player_id, exc
                ) from exc
        return True

    async def verify_legacy_warehouse(self, player_id: int, legacy) -> None:  # noqa: ANN001
        """迁后对账(只在割接窗口、读流量放开前有意义):

          - 每个 legacy 实例必须在仓库段中(逐 instance_id);
          - 每个 legacy config 的仓库段总数 ≥ legacy 总数(段内可能含 phase 2 既有同 config 存量);
          - `bag_migration` 记录的三元组必须与 legacy 快照一致(冻结窗口被违反时立刻暴露)。
        """
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(
                    "SELECT stack_kinds, stack_total, instance_count FROM bag_migration "
                    "WHERE player_id = %s",
                    (player_id,),
                )
                row = await cur.fetchone()
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "read bag_migration player=%d: %s", player_id, exc
            ) from exc
        if row is None:
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "verify before migration player=%d", player_id
            )
        stack_kinds, stack_total, instance_count = int(row[0]), int(row[1]), int(row[2])
        want_kinds, want_total, want_instances = legacy_migration_totals(legacy)
        if (
            stack_kinds != want_kinds
            or stack_total != want_total
            or instance_count != want_instances
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "migration totals drift player=%d recorded=(%d,%d,%d) legacy=(%d,%d,%d) "
                "— 冻结窗口被违反?",
                player_id, stack_kinds, stack_total, instance_count,
                want_kinds, want_total, want_instances,
            )

        sec = bag_pb2.BagSection()
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(
                    "SELECT section FROM bag_section WHERE player_id = %s AND bag_type = %s",
                    (player_id, bapply.BAG_WAREHOUSE_TYPE),
                )
                sec_row = await cur.fetchone()
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "read warehouse section player=%d: %s", player_id, exc
            ) from exc
        if sec_row is not None:
            try:
                sec.ParseFromString(bytes(sec_row[0] or b""))
            except Exception as exc:  # noqa: BLE001
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "decode warehouse section player=%d: %s", player_id, exc,
                ) from exc

        have_instances: set[int] = set()
        have_counts: dict[int, int] = {}
        for it in sec.items:
            if it.instance_id != 0:
                have_instances.add(it.instance_id)
                continue
            have_counts[it.item_config_id] = have_counts.get(it.item_config_id, 0) + it.count
        want_counts: dict[int, int] = {}
        for it in legacy:
            if it.instance_id != 0:
                if it.instance_id not in have_instances:
                    raise errcode.PandoraError(
                        errcode.ErrInvalidState,
                        "migrated instance missing player=%d instance=%d",
                        player_id, it.instance_id,
                    )
                continue
            want_counts[it.item_config_id] = want_counts.get(it.item_config_id, 0) + it.count
        # 字典序遍历:同一批漂移在两栈报同一条 config(parity 探针可比对)。
        for config_id in sorted(want_counts):
            want = want_counts[config_id]
            have = have_counts.get(config_id, 0)
            if have < want:
                raise errcode.PandoraError(
                    errcode.ErrInvalidState,
                    "migrated stack short player=%d config=%d have=%d want>=%d",
                    player_id, config_id, have, want,
                )


__all__ = [
    "BagSeederMixin",
    "LegacyBagSourceMixin",
    "legacy_migration_totals",
]
