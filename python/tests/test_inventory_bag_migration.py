"""旧 inventory 存量 → 背包域仓库段迁移(D5)测试。

对应 Go `internal/data/bag_migration_mysql_test.go` + `internal/biz/bag_migrate.go`。

分两层:

  ① **打真实 MySQL** 的数据层测试 —— 被测的就是事务本身(锁 bag_meta 行、
     `bag_migration` 一玩家一行永久幂等闸、超容落位)。fake 上永远绿,真库上少一步
     就会撞唯一键或自己等自己的锁。没有库就整体 skip(不假装通过)。
  ② 纯内存的用例层测试 —— 统计口径、游标推进、单玩家失败不阻断整轮、
     枚举失败必须上抛(而不是汇报一份假的 done)。

跑之前:
    docker run -d --name pandora-mysql-verify -p 13306:3306 \\
      -e MYSQL_ROOT_PASSWORD=pandora_dev_root \\
      mysql:8.4 --sql-mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"
    或设 PANDORA_TEST_MYSQL_DSN 指向已有实例。
"""

from __future__ import annotations

import asyncio

import pytest
from pandora.bag.v1 import bag_pb2

from mysqlfixture import MYSQL_DSN as DSN
from mysqlfixture import ensure_database, parse_go_dsn

from pandorapy import errcode
from pandorapy.services.inventory import bag_apply as bapply
from pandorapy.services.inventory import bag_migrate as bmigrate
from pandorapy.services.inventory import bag_migration as bmig
from pandorapy.services.inventory import bag_repo as brepo
from pandorapy.services.inventory import conf as iconf
from pandorapy.services.inventory import repo as irepo
from pandorapy.services.inventory import repo_instance as rinst
from pandorapy.services.inventory.models import ItemAttribute

# ── 与 deploy/mysql-init/{08-inventory,14-bag}-tables.sql 同构 ────────────────
_DDL = [
    """CREATE TABLE IF NOT EXISTS player_items (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        player_id BIGINT UNSIGNED NOT NULL,
        item_config_id INT UNSIGNED NOT NULL,
        count BIGINT NOT NULL DEFAULT 0,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uk_player_item (player_id, item_config_id),
        KEY idx_player (player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS player_item_instance (
        instance_id BIGINT UNSIGNED NOT NULL,
        player_id BIGINT UNSIGNED NOT NULL,
        item_config_id INT UNSIGNED NOT NULL,
        identified TINYINT NOT NULL DEFAULT 0,
        attributes VARBINARY(1024) NULL,
        slot_index INT NULL DEFAULT NULL,
        bound TINYINT NOT NULL DEFAULT 0,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (instance_id),
        KEY idx_player (player_id),
        UNIQUE KEY uk_player_slot (player_id, slot_index)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS bag_meta (
        player_id BIGINT UNSIGNED NOT NULL,
        owner_epoch BIGINT UNSIGNED NOT NULL DEFAULT 0,
        last_journal_seq BIGINT UNSIGNED NOT NULL DEFAULT 0,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS bag_section (
        player_id BIGINT UNSIGNED NOT NULL,
        bag_type INT UNSIGNED NOT NULL,
        generation BIGINT UNSIGNED NOT NULL DEFAULT 0,
        section MEDIUMBLOB NOT NULL,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (player_id, bag_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS bag_migration (
        player_id BIGINT UNSIGNED NOT NULL,
        stack_kinds INT UNSIGNED NOT NULL DEFAULT 0,
        stack_total BIGINT UNSIGNED NOT NULL DEFAULT 0,
        instance_count INT UNSIGNED NOT NULL DEFAULT 0,
        migrated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
]

_TABLES = (
    "player_items",
    "player_item_instance",
    "bag_meta",
    "bag_section",
    "bag_migration",
)

# 迁移用的堆叠上限:固定 10,好让"拆堆"在用例里是可数的。
_MAX_STACK = 10


def _max_stack_of(_item_config_id: int) -> int:
    return _MAX_STACK


@pytest.fixture
async def pool():
    """每个用例一个连接池 —— 必须 function 作用域(asyncmy 池绑创建时的 loop)。"""
    asyncmy = pytest.importorskip("asyncmy")
    pytest.importorskip("cryptography", reason="MySQL 8.x caching_sha2_password 需要 cryptography")
    cfg = parse_go_dsn(DSN, default_db="pandora_trade")
    try:
        await ensure_database(asyncmy, cfg)
        p = await asyncio.wait_for(
            asyncmy.create_pool(
                host=cfg["host"],
                port=cfg["port"],
                user=cfg["user"],
                password=cfg["password"],
                db=cfg["db"],
                minsize=1,
                maxsize=8,
                autocommit=True,
            ),
            timeout=8,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MySQL 不可用 @ {cfg['host']}:{cfg['port']} ({exc}) —— D5 迁移数据层测试整体跳过")
    try:
        async with p.acquire() as conn, conn.cursor() as cur:
            for ddl in _DDL:
                await cur.execute(ddl)
            for t in _TABLES:
                await cur.execute(f"TRUNCATE TABLE {t}")  # noqa: S608
        yield p
    finally:
        p.close()
        await p.wait_closed()


@pytest.fixture
async def repos(pool):
    """(legacy 源, bag 落位)两个仓储。

    真实部署是两个库两个池;这里共用一个 schema —— 被测的是**事务与幂等闸**,
    跨库这一点在 Go/Python 两侧都体现为"无法同事务",而那正是 bag_migration
    行闸存在的理由,共库不会把它测没。
    """
    from mysqlfixture import session_db_name

    cfg = parse_go_dsn(DSN, default_db="pandora_trade")
    db = cfg["db"] or session_db_name()
    return irepo.MySQLInventoryRepo(pool, db=db), brepo.MySQLBagRepo(pool, db=db)


async def _seed_legacy(
    pool,  # noqa: ANN001
    player_id: int,
    stacks: list[tuple[int, int]] | None = None,
    instances: list[tuple[int, int, bool, list[ItemAttribute] | None, bool]] | None = None,
) -> None:
    """往 legacy 两张表塞存量。instances 元素 = (instance_id, config, identified, attrs, bound)。"""
    async with pool.acquire() as conn, conn.cursor() as cur:
        for config_id, count in stacks or ():
            await cur.execute(
                "INSERT INTO player_items (player_id, item_config_id, count) VALUES (%s, %s, %s)",
                (player_id, config_id, count),
            )
        for i, (inst_id, config_id, identified, attrs, bound) in enumerate(instances or ()):
            await cur.execute(
                "INSERT INTO player_item_instance "
                "(instance_id, player_id, item_config_id, identified, attributes, slot_index, bound)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    inst_id,
                    player_id,
                    config_id,
                    1 if identified else 0,
                    rinst.encode_instance_attrs(attrs or []),
                    i,
                    1 if bound else 0,
                ),
            )


async def _read_warehouse(pool, player_id: int) -> bag_pb2.BagSection:  # noqa: ANN001
    sec = bag_pb2.BagSection()
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT section FROM bag_section WHERE player_id = %s AND bag_type = %s",
            (player_id, bapply.BAG_WAREHOUSE_TYPE),
        )
        row = await cur.fetchone()
    if row is not None:
        sec.ParseFromString(bytes(row[0] or b""))
    return sec


# ── ① 纯函数:对账三元组 ─────────────────────────────────────────────────


def test_totals_counts_kinds_total_and_instances() -> None:
    items = [
        bag_pb2.BagItem(item_config_id=1001, count=3),
        bag_pb2.BagItem(item_config_id=1001, count=4),
        bag_pb2.BagItem(item_config_id=1002, count=5),
        bag_pb2.BagItem(item_config_id=2001, count=1, instance_id=90001),
    ]
    assert bmig.legacy_migration_totals(items) == (2, 12, 1)


def test_totals_ignores_instance_count_in_stack_total() -> None:
    """实例条目 count=1,但**不得**计进堆叠总量 —— 否则对账永远差 N。"""
    items = [bag_pb2.BagItem(item_config_id=2001, count=1, instance_id=i) for i in (1, 2, 3)]
    assert bmig.legacy_migration_totals(items) == (0, 0, 3)


def test_totals_empty_is_all_zero() -> None:
    assert bmig.legacy_migration_totals([]) == (0, 0, 0)


# ── ② 数据层:枚举 / 快照 / 落位 / 对账(真库)────────────────────────────


async def test_list_legacy_players_unions_two_tables_and_paginates(repos, pool) -> None:
    """两表并集、升序、游标翻页;count<=0 的行不算存量。"""
    legacy, _bag = repos
    await _seed_legacy(pool, 11, stacks=[(1001, 5)])
    await _seed_legacy(pool, 12, stacks=[(1001, 0)])  # 空堆不算
    await _seed_legacy(pool, 13, instances=[(9001, 2001, True, None, False)])
    await _seed_legacy(pool, 14, stacks=[(1002, 1)], instances=[(9002, 2001, False, None, False)])

    first = await legacy.list_legacy_bag_players(0, 2)
    assert first == [11, 13]
    second = await legacy.list_legacy_bag_players(first[-1], 2)
    assert second == [14]
    assert await legacy.list_legacy_bag_players(second[-1], 2) == []


async def test_list_legacy_players_limit_zero_returns_empty(repos) -> None:
    """limit<=0 直接空 —— 不能翻译成 `LIMIT 0` 之外的任何东西,更不能无限拉。"""
    legacy, _bag = repos
    assert await legacy.list_legacy_bag_players(0, 0) == []
    assert await legacy.list_legacy_bag_players(0, -1) == []


async def test_load_legacy_stock_maps_stacks_and_instances(repos, pool) -> None:
    legacy, _bag = repos
    attrs = [ItemAttribute(attr_id=7, value=42)]
    await _seed_legacy(
        pool,
        21,
        stacks=[(1001, 3), (1002, 7)],
        instances=[(9101, 2001, True, attrs, False)],
    )
    items = await legacy.load_legacy_bag_stock(21)
    stacks = [(it.item_config_id, it.count) for it in items if it.instance_id == 0]
    assert stacks == [(1001, 3), (1002, 7)]
    inst = next(it for it in items if it.instance_id != 0)
    assert (inst.instance_id, inst.item_config_id, inst.identified) == (9101, 2001, True)
    assert [(a.attr_id, a.value) for a in inst.attrs] == [(7, 42)]


async def test_load_legacy_stock_rejects_bound_instance(repos, pool) -> None:
    """bound=1 fail-closed:BagItem 尚无 bound 字段,静默迁过去 = 绑定约束凭空消失。"""
    legacy, _bag = repos
    await _seed_legacy(pool, 22, instances=[(9201, 2001, True, None, True)])
    with pytest.raises(errcode.PandoraError) as ei:
        await legacy.load_legacy_bag_stock(22)
    assert errcode.as_code(ei.value) == errcode.ErrInvalidState


async def test_seed_is_idempotent_and_second_run_is_noop(repos, pool) -> None:
    """一玩家一行永久闸:重跑返回 False 且**不重复入账**(否则重跑 = 翻倍发货)。"""
    legacy, bag = repos
    await _seed_legacy(pool, 31, stacks=[(1001, 4)])
    items = await legacy.load_legacy_bag_stock(31)

    assert await bag.seed_legacy_warehouse(31, items, _max_stack_of) is True
    sec1 = await _read_warehouse(pool, 31)
    assert [(it.item_config_id, it.count) for it in sec1.items] == [(1001, 4)]

    assert await bag.seed_legacy_warehouse(31, items, _max_stack_of) is False
    sec2 = await _read_warehouse(pool, 31)
    assert [(it.item_config_id, it.count) for it in sec2.items] == [(1001, 4)]


async def test_seed_splits_stacks_by_max_stack(repos, pool) -> None:
    """拆堆复用 journal 写路径同一函数(语义单源):25 个按 10 拆成 10/10/5。"""
    legacy, bag = repos
    await _seed_legacy(pool, 32, stacks=[(1001, 25)])
    items = await legacy.load_legacy_bag_stock(32)
    assert await bag.seed_legacy_warehouse(32, items, _max_stack_of) is True
    sec = await _read_warehouse(pool, 32)
    assert sorted(it.count for it in sec.items) == [5, 10, 10]


async def test_seed_merges_into_existing_warehouse_section(repos, pool) -> None:
    """phase 2 期间已建段:迁移必须**合并**而不是覆盖(覆盖 = 抹掉已领取的东西)。"""
    legacy, bag = repos
    pre = bag_pb2.BagSection(bag_type=bapply.BAG_WAREHOUSE_TYPE)
    pre.items.add(item_config_id=1001, count=2, slot=0)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO bag_section (player_id, bag_type, generation, section) "
            "VALUES (%s, %s, 0, %s)",
            (33, bapply.BAG_WAREHOUSE_TYPE, pre.SerializeToString()),
        )
    await _seed_legacy(pool, 33, stacks=[(1001, 3)])
    items = await legacy.load_legacy_bag_stock(33)
    assert await bag.seed_legacy_warehouse(33, items, _max_stack_of) is True
    sec = await _read_warehouse(pool, 33)
    # 先填既有未满堆(2+3=5),不新开格。
    assert [(it.item_config_id, it.count) for it in sec.items] == [(1001, 5)]


async def test_seed_is_capacity_exempt(repos, pool) -> None:
    """容量豁免:迁移一次性超容落位(§3.2 只出不进),不得因为格子多就拒迁。

    200 个按 max_stack=10 要 20 格,远超任何合理仓库容量 —— 必须全落进去。
    """
    legacy, bag = repos
    await _seed_legacy(pool, 34, stacks=[(1001, 200)])
    items = await legacy.load_legacy_bag_stock(34)
    assert await bag.seed_legacy_warehouse(34, items, _max_stack_of) is True
    sec = await _read_warehouse(pool, 34)
    assert len(sec.items) == 20
    assert sum(it.count for it in sec.items) == 200


async def test_seed_empty_stock_still_records_gate(repos, pool) -> None:
    """空存量玩家也要落闸行,否则每轮都会把他重新"迁"一遍(scanned 永不收敛)。"""
    _legacy, bag = repos
    assert await bag.seed_legacy_warehouse(35, [], _max_stack_of) is True
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT stack_kinds, stack_total, instance_count "
                          "FROM bag_migration WHERE player_id = %s", (35,))
        row = await cur.fetchone()
    assert row == (0, 0, 0)
    # 没有物品就不该建段(建一个空段是纯噪声)。
    assert len((await _read_warehouse(pool, 35)).items) == 0


async def test_seed_does_not_touch_owner_epoch(repos, pool) -> None:
    """迁移不是 owner 写者:锁 bag_meta 行串行化,但**不得**推进 owner_epoch。

    推进了会把在场 DS 的合法写 fence 掉(玩家背包突然写不进去)。
    """
    _legacy, bag = repos
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO bag_meta (player_id, owner_epoch, last_journal_seq) VALUES (%s, 7, 3)",
            (36,),
        )
    await bag.seed_legacy_warehouse(36, [], _max_stack_of)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT owner_epoch, last_journal_seq FROM bag_meta WHERE player_id = %s", (36,)
        )
        row = await cur.fetchone()
    assert row == (7, 3)


async def test_verify_passes_after_migration(repos, pool) -> None:
    legacy, bag = repos
    await _seed_legacy(
        pool, 41, stacks=[(1001, 6)], instances=[(9401, 2001, True, None, False)]
    )
    items = await legacy.load_legacy_bag_stock(41)
    assert await bag.seed_legacy_warehouse(41, items, _max_stack_of) is True
    await bag.verify_legacy_warehouse(41, items)  # 不抛即通过


async def test_verify_before_migration_fails(repos) -> None:
    _legacy, bag = repos
    with pytest.raises(errcode.PandoraError) as ei:
        await bag.verify_legacy_warehouse(42, [])
    assert errcode.as_code(ei.value) == errcode.ErrInvalidState
    assert "verify before migration" in str(ei.value)


async def test_verify_detects_totals_drift(repos, pool) -> None:
    """冻结窗口被违反(迁完 legacy 又变了)必须立刻暴露,不能静默当成功。"""
    legacy, bag = repos
    await _seed_legacy(pool, 43, stacks=[(1001, 6)])
    items = await legacy.load_legacy_bag_stock(43)
    await bag.seed_legacy_warehouse(43, items, _max_stack_of)
    drifted = [bag_pb2.BagItem(item_config_id=1001, count=9)]
    with pytest.raises(errcode.PandoraError) as ei:
        await bag.verify_legacy_warehouse(43, drifted)
    assert "migration totals drift" in str(ei.value)


async def test_verify_detects_missing_instance(repos, pool) -> None:
    """闸行的三元组对得上、但实例没进段 —— 只对总量不查逐件就会漏掉这种。"""
    legacy, bag = repos
    await _seed_legacy(pool, 44, instances=[(9441, 2001, True, None, False)])
    items = await legacy.load_legacy_bag_stock(44)
    await bag.seed_legacy_warehouse(44, items, _max_stack_of)
    # 人为把段清空(模拟落位丢件),闸行与三元组保持不变。
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE bag_section SET section = %s WHERE player_id = %s AND bag_type = %s",
            (
                bag_pb2.BagSection(bag_type=bapply.BAG_WAREHOUSE_TYPE).SerializeToString(),
                44,
                bapply.BAG_WAREHOUSE_TYPE,
            ),
        )
    with pytest.raises(errcode.PandoraError) as ei:
        await bag.verify_legacy_warehouse(44, items)
    assert "migrated instance missing" in str(ei.value)


async def test_verify_detects_short_stack(repos, pool) -> None:
    legacy, bag = repos
    await _seed_legacy(pool, 45, stacks=[(1001, 6)])
    items = await legacy.load_legacy_bag_stock(45)
    await bag.seed_legacy_warehouse(45, items, _max_stack_of)
    async with pool.acquire() as conn, conn.cursor() as cur:
        short = bag_pb2.BagSection(bag_type=bapply.BAG_WAREHOUSE_TYPE)
        short.items.add(item_config_id=1001, count=2, slot=0)
        await cur.execute(
            "UPDATE bag_section SET section = %s WHERE player_id = %s AND bag_type = %s",
            (short.SerializeToString(), 45, bapply.BAG_WAREHOUSE_TYPE),
        )
    with pytest.raises(errcode.PandoraError) as ei:
        await bag.verify_legacy_warehouse(45, items)
    assert "migrated stack short" in str(ei.value)


async def test_verify_allows_extra_stock_in_section(repos, pool) -> None:
    """段内可能含 phase 2 既有同 config 存量 → 判据是 `>=` 不是 `==`。"""
    legacy, bag = repos
    await _seed_legacy(pool, 46, stacks=[(1001, 3)])
    items = await legacy.load_legacy_bag_stock(46)
    await bag.seed_legacy_warehouse(46, items, _max_stack_of)
    async with pool.acquire() as conn, conn.cursor() as cur:
        more = bag_pb2.BagSection(bag_type=bapply.BAG_WAREHOUSE_TYPE)
        more.items.add(item_config_id=1001, count=3, slot=0)
        more.items.add(item_config_id=1001, count=5, slot=1)
        await cur.execute(
            "UPDATE bag_section SET section = %s WHERE player_id = %s AND bag_type = %s",
            (more.SerializeToString(), 46, bapply.BAG_WAREHOUSE_TYPE),
        )
    await bag.verify_legacy_warehouse(46, items)  # 不抛


async def test_end_to_end_usecase_against_real_db(repos, pool) -> None:
    """全链:枚举 → 快照 → 幂等落位 → 对账;第二轮全 skipped。"""
    legacy, bag = repos
    await _seed_legacy(pool, 51, stacks=[(1001, 12)])
    await _seed_legacy(pool, 52, instances=[(9521, 2001, True, None, False)])
    cfg = iconf.BagConf(migration_batch=1, default_max_stack=_MAX_STACK)
    uc = bmigrate.BagMigrationUsecase(legacy, bag, cfg)

    first = await uc.run_once()
    assert (first.scanned, first.migrated, first.skipped, first.failed) == (2, 2, 0, 0)

    second = await uc.run_once()
    assert (second.scanned, second.migrated, second.skipped, second.failed) == (2, 0, 2, 0)


async def test_bound_instance_fails_only_that_player(repos, pool) -> None:
    """单玩家 fail-closed 不阻断整轮:好玩家照迁,坏玩家计 failed 等排障。"""
    legacy, bag = repos
    await _seed_legacy(pool, 61, stacks=[(1001, 2)])
    await _seed_legacy(pool, 62, instances=[(9621, 2001, True, None, True)])  # bound
    await _seed_legacy(pool, 63, stacks=[(1002, 3)])
    cfg = iconf.BagConf(migration_batch=10, default_max_stack=_MAX_STACK)
    uc = bmigrate.BagMigrationUsecase(legacy, bag, cfg)

    summary = await uc.run_once()
    assert (summary.scanned, summary.migrated, summary.failed) == (3, 2, 1)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT player_id FROM bag_migration ORDER BY player_id")
        rows = await cur.fetchall()
    assert [r[0] for r in rows] == [61, 63]


# ── ③ 用例层:统计口径 / 游标 / 失败传播(纯内存,不需要库)────────────────


class _FakeLegacy:
    """可编排的 legacy 源:按 player_id 给快照,可注入枚举 / 读取失败。"""

    def __init__(self, stock: dict[int, list], *, list_error=None, load_errors=None) -> None:  # noqa: ANN001
        self.stock = stock
        self.list_error = list_error
        self.load_errors = load_errors or {}
        self.list_calls: list[tuple[int, int]] = []

    async def list_legacy_bag_players(self, after_player_id: int, limit: int) -> list[int]:
        self.list_calls.append((after_player_id, limit))
        if self.list_error is not None:
            raise self.list_error
        ids = sorted(p for p in self.stock if p > after_player_id)
        return ids[:limit]

    async def load_legacy_bag_stock(self, player_id: int) -> list:
        err = self.load_errors.get(player_id)
        if err is not None:
            raise err
        return self.stock[player_id]


class _FakeSeeder:
    """可编排的落位端:记录调用,可指定谁已迁过 / 谁对账失败。"""

    def __init__(self, *, already=(), verify_errors=None, seed_errors=None) -> None:
        self.already = set(already)
        self.verify_errors = verify_errors or {}
        self.seed_errors = seed_errors or {}
        self.seeded: list[int] = []
        self.verified: list[int] = []

    async def seed_legacy_warehouse(self, player_id: int, items, max_stack) -> bool:  # noqa: ANN001, ARG002
        err = self.seed_errors.get(player_id)
        if err is not None:
            raise err
        if player_id in self.already:
            return False
        self.seeded.append(player_id)
        return True

    async def verify_legacy_warehouse(self, player_id: int, legacy) -> None:  # noqa: ANN001, ARG002
        err = self.verify_errors.get(player_id)
        if err is not None:
            raise err
        self.verified.append(player_id)


def _cfg(batch: int = 2) -> iconf.BagConf:
    return iconf.BagConf(migration_batch=batch, default_max_stack=_MAX_STACK)


async def test_run_once_advances_cursor_across_batches() -> None:
    """游标必须按 player_id 前进,否则第二批会重复枚举第一批 → 死循环。"""
    legacy = _FakeLegacy({1: [], 2: [], 3: [], 4: [], 5: []})
    seeder = _FakeSeeder()
    uc = bmigrate.BagMigrationUsecase(legacy, seeder, _cfg(batch=2))

    summary = await uc.run_once()
    assert (summary.scanned, summary.migrated) == (5, 5)
    assert legacy.list_calls == [(0, 2), (2, 2), (4, 2), (5, 2)]


async def test_run_once_counts_skipped_for_existing_gate() -> None:
    legacy = _FakeLegacy({1: [], 2: [], 3: []})
    seeder = _FakeSeeder(already=(2,))
    uc = bmigrate.BagMigrationUsecase(legacy, seeder, _cfg(batch=10))

    summary = await uc.run_once()
    assert (summary.scanned, summary.migrated, summary.skipped, summary.failed) == (3, 2, 1, 0)
    # 已迁玩家**不得**再对账(它的 legacy 快照此刻可能已被清理)。
    assert seeder.verified == [1, 3]


async def test_run_once_counts_verify_failure_without_aborting() -> None:
    legacy = _FakeLegacy({1: [], 2: [], 3: []})
    boom = errcode.PandoraError(errcode.ErrInvalidState, "totals drift")
    seeder = _FakeSeeder(verify_errors={2: boom})
    uc = bmigrate.BagMigrationUsecase(legacy, seeder, _cfg(batch=10))

    summary = await uc.run_once()
    assert (summary.scanned, summary.migrated, summary.failed) == (3, 2, 1)


async def test_run_once_propagates_enumeration_failure() -> None:
    """枚举整批读不出来 = 这一轮统计不可信,必须上抛 —— 不能汇报一份假的 done。"""
    legacy = _FakeLegacy({1: []}, list_error=errcode.PandoraError(errcode.ErrInternal, "db down"))
    uc = bmigrate.BagMigrationUsecase(legacy, _FakeSeeder(), _cfg())
    with pytest.raises(errcode.PandoraError):
        await uc.run_once()


async def test_run_once_reports_partial_summary_on_abort() -> None:
    """中断时也要交出已累计的进度,否则告警是全 0,看不出断在哪。"""

    class _FailSecondBatch(_FakeLegacy):
        async def list_legacy_bag_players(self, after_player_id: int, limit: int) -> list[int]:
            if after_player_id != 0:
                raise errcode.PandoraError(errcode.ErrInternal, "db down")
            return await super().list_legacy_bag_players(after_player_id, limit)

    legacy = _FailSecondBatch({1: [], 2: [], 3: []})
    uc = bmigrate.BagMigrationUsecase(legacy, _FakeSeeder(), _cfg(batch=2))
    summary = bmigrate.BagMigrationSummary()
    with pytest.raises(errcode.PandoraError):
        await uc.run_once(summary)
    assert (summary.scanned, summary.migrated) == (2, 2)


async def test_run_once_cancellation_is_not_swallowed() -> None:
    """CancelledError 必须穿透:吞掉的话停机时这条作业退不出去(§9.16)。"""
    legacy = _FakeLegacy({1: [], 2: []}, load_errors={1: asyncio.CancelledError()})
    uc = bmigrate.BagMigrationUsecase(legacy, _FakeSeeder(), _cfg(batch=10))
    with pytest.raises(asyncio.CancelledError):
        await uc.run_once()


async def test_job_wrapper_never_raises_on_abort() -> None:
    """作业外壳吞掉失败只打 aborted 日志 —— 它是 background task,抛出去没人接。"""
    legacy = _FakeLegacy({1: []}, list_error=errcode.PandoraError(errcode.ErrInternal, "db down"))
    uc = bmigrate.BagMigrationUsecase(legacy, _FakeSeeder(), _cfg())
    await bmigrate.run_legacy_bag_migration(uc)  # 不抛即通过


async def test_job_wrapper_lets_cancellation_through() -> None:
    legacy = _FakeLegacy({1: []}, list_error=asyncio.CancelledError())
    uc = bmigrate.BagMigrationUsecase(legacy, _FakeSeeder(), _cfg())
    with pytest.raises(asyncio.CancelledError):
        await bmigrate.run_legacy_bag_migration(uc)


# ── ④ 事件名守护 ────────────────────────────────────────────────────────
#
# 这三个事件名与 Go 逐字相同(告警规则按事件名建)。改名 / 删日志都不会让上面任何
# 一条断言变红 —— 变异验证时把 `plog.get().error(...)` 整块删掉,29 个用例全绿。
# 那正是"迁移失败了但告警一行没响"的形状,所以单独把名字钉住。


async def test_player_failure_emits_go_event_name() -> None:
    from structlog.testing import capture_logs

    legacy = _FakeLegacy({1: [], 2: []}, load_errors={2: errcode.PandoraError(12, "bound")})
    uc = bmigrate.BagMigrationUsecase(legacy, _FakeSeeder(), _cfg(batch=10))
    with capture_logs() as logs:
        await uc.run_once()
    hit = [e for e in logs if e.get("event") == "bag_legacy_migration_player_failed"]
    assert len(hit) == 1
    assert hit[0]["player_id"] == 2
    assert hit[0]["code"] == 12
    assert hit[0]["log_level"] == "error"


async def test_job_wrapper_emits_go_event_names() -> None:
    from structlog.testing import capture_logs

    def _events(logs) -> list[str]:  # noqa: ANN001
        return [e.get("event") for e in logs]

    # ① 正常收敛
    uc_ok = bmigrate.BagMigrationUsecase(_FakeLegacy({1: []}), _FakeSeeder(), _cfg(batch=10))
    with capture_logs() as logs:
        await bmigrate.run_legacy_bag_migration(uc_ok)
    assert "bag_legacy_migration_done" in _events(logs)

    # ② 有失败玩家 —— 必须是 error 级的 done_with_failures,不能报成普通 done
    uc_bad = bmigrate.BagMigrationUsecase(
        _FakeLegacy({1: []}, load_errors={1: errcode.PandoraError(12, "bound")}),
        _FakeSeeder(),
        _cfg(batch=10),
    )
    with capture_logs() as logs:
        await bmigrate.run_legacy_bag_migration(uc_bad)
    assert "bag_legacy_migration_done_with_failures" in _events(logs)
    assert "bag_legacy_migration_done" not in _events(logs)

    # ③ 整轮中断 —— 必须带上已累计的部分进度(全 0 看不出断在哪)
    class _FailSecond(_FakeLegacy):
        async def list_legacy_bag_players(self, after_player_id: int, limit: int) -> list[int]:
            if after_player_id != 0:
                raise errcode.PandoraError(errcode.ErrInternal, "db down")
            return await super().list_legacy_bag_players(after_player_id, limit)

    uc_abort = bmigrate.BagMigrationUsecase(
        _FailSecond({1: [], 2: [], 3: []}), _FakeSeeder(), _cfg(batch=2)
    )
    with capture_logs() as logs:
        await bmigrate.run_legacy_bag_migration(uc_abort)
    aborted = [e for e in logs if e.get("event") == "bag_legacy_migration_aborted"]
    assert len(aborted) == 1
    assert (aborted[0]["scanned"], aborted[0]["migrated"]) == (2, 2)
