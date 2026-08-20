"""player 数据层 —— **打真实 MySQL**,不用 mock。

为什么必须打真库:被测的就是**事务与唯一键语义本身** —— `SELECT ... FOR UPDATE` 的
串行化、1062 幂等命中、`ON DUPLICATE KEY UPDATE`、乐观锁 rowcount、`INSERT IGNORE`
的静默跳过。用 fake repo 测等于把被测对象换成了"我对 MySQL 语义的想象",而这一层的
全部价值恰恰在于那些语义真的成立。

没有库就整体 skip(**不假装通过**):
    docker run -d --name pandora-mysql-verify -p 13306:3306 \\
      -e MYSQL_ROOT_PASSWORD=pandora_dev_root \\
      mysql:8.4 --sql-mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"

环境变量 PANDORA_TEST_MYSQL_DSN 可覆盖(与 Go 侧 CI 的门控变量同名);未设时按本仓
一键栈的 dev 端口 3307 试一次。

★ DSN 里刻意**不带库名**:mysqlfixture 会给本 pytest 进程建一个独占库
  (`pandora_test_<pid>_<ts>`)并在会话结束时删掉。共享库时两个 pytest 进程会互踩
  TRUNCATE,表现为"上限被突破"这类**看起来像业务 bug 的假红**,而代码一个字没改。
  也因此用的是 root:建库需要 CREATE DATABASE 权限,业务账号 pandora 只有既有库的权限。
"""

from __future__ import annotations

import asyncio
import datetime as _dt

import pytest

from mysqlfixture import MYSQL_DSN as DSN
from mysqlfixture import ensure_database, parse_go_dsn, skip_only_if_mysql_is_down

from pandora.player.v1 import player_pb2 as ppb

from pandorapy import dbguard, errcode
from pandorapy.services.player import models as m
from pandorapy.services.player import repo as prepo


# 与 deploy/mysql-init/04-player-tables.sql + 13-reward-claim-tables.sql 同构。
# ★ 唯一键必须逐字照抄:`uk_player_idem` / `uk_player_instance` / `uk_player_card_once`
#   这几个是幂等与"同卡不占两槽"的**全部**机制,写成普通索引测试照样绿,线上直接双发。
_DDL = [
    """CREATE TABLE IF NOT EXISTS `players` (
        `player_id` BIGINT UNSIGNED NOT NULL,
        `nickname` VARCHAR(64) NOT NULL,
        `level` INT NOT NULL DEFAULT 1,
        `exp` BIGINT UNSIGNED NOT NULL DEFAULT 0,
        `mmr` INT NOT NULL DEFAULT 1500,
        `avatar` VARCHAR(255) NOT NULL DEFAULT '',
        `total_battles` INT NOT NULL DEFAULT 0,
        `total_wins` INT NOT NULL DEFAULT 0,
        `active_hero_id` INT UNSIGNED NOT NULL DEFAULT 0,
        `unspent_attr_points` INT NOT NULL DEFAULT 0,
        `total_talent_points` INT NOT NULL DEFAULT 0,
        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        `last_seen_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (`player_id`),
        UNIQUE KEY `uk_nickname` (`nickname`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS `player_heroes` (
        `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        `player_id` BIGINT UNSIGNED NOT NULL,
        `hero_id` INT UNSIGNED NOT NULL,
        `source` VARCHAR(32) NOT NULL DEFAULT '',
        `unlocked_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (`id`),
        UNIQUE KEY `uk_player_hero` (`player_id`, `hero_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS `player_mmr` (
        `player_id` BIGINT UNSIGNED NOT NULL,
        `rating_pool` VARCHAR(32) NOT NULL,
        `mmr` INT NOT NULL DEFAULT 1500,
        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (`player_id`, `rating_pool`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS `mmr_history` (
        `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        `player_id` BIGINT UNSIGNED NOT NULL,
        `idempotency_key` VARCHAR(64) NOT NULL,
        `rating_pool` VARCHAR(32) NOT NULL DEFAULT 'default',
        `delta` INT NOT NULL,
        `reason` VARCHAR(32) NOT NULL DEFAULT '',
        `old_mmr` INT NOT NULL,
        `new_mmr` INT NOT NULL,
        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (`id`),
        UNIQUE KEY `uk_player_idem` (`player_id`, `idempotency_key`),
        KEY `idx_created` (`created_at`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS `player_attributes` (
        `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        `player_id` BIGINT UNSIGNED NOT NULL,
        `attr_key` VARCHAR(32) NOT NULL,
        `points` INT NOT NULL DEFAULT 0,
        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (`id`),
        UNIQUE KEY `uk_player_attr` (`player_id`, `attr_key`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS `attr_point_grants` (
        `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        `player_id` BIGINT UNSIGNED NOT NULL,
        `idempotency_key` VARCHAR(64) NOT NULL,
        `points` INT NOT NULL,
        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (`id`),
        UNIQUE KEY `uk_player_grant` (`player_id`, `idempotency_key`),
        KEY `idx_created` (`created_at`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS `player_equipment` (
        `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        `player_id` BIGINT UNSIGNED NOT NULL,
        `slot` INT UNSIGNED NOT NULL,
        `item_config_id` INT UNSIGNED NOT NULL,
        `instance_id` BIGINT UNSIGNED NULL,
        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (`id`),
        UNIQUE KEY `uk_player_slot` (`player_id`, `slot`),
        UNIQUE KEY `uk_player_instance` (`player_id`, `instance_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS `player_talents` (
        `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        `player_id` BIGINT UNSIGNED NOT NULL,
        `talent_id` INT UNSIGNED NOT NULL,
        `level` INT NOT NULL DEFAULT 0,
        `spent_points` INT NOT NULL DEFAULT 0,
        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (`id`),
        UNIQUE KEY `uk_player_talent` (`player_id`, `talent_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS `talent_point_grants` (
        `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        `player_id` BIGINT UNSIGNED NOT NULL,
        `idempotency_key` VARCHAR(64) NOT NULL,
        `points` INT NOT NULL,
        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (`id`),
        UNIQUE KEY `uk_player_talent_grant` (`player_id`, `idempotency_key`),
        KEY `idx_created` (`created_at`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS `exp_history` (
        `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        `player_id` BIGINT UNSIGNED NOT NULL,
        `idempotency_key` VARCHAR(64) NOT NULL,
        `exp_delta` BIGINT UNSIGNED NOT NULL,
        `reason` VARCHAR(32) NOT NULL DEFAULT '',
        `old_level` INT NOT NULL,
        `old_exp` BIGINT UNSIGNED NOT NULL,
        `new_level` INT NOT NULL,
        `new_exp` BIGINT UNSIGNED NOT NULL,
        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (`id`),
        UNIQUE KEY `uk_player_idem` (`player_id`, `idempotency_key`),
        KEY `idx_created` (`created_at`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS `player_push_outbox` (
        `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        `player_id` BIGINT UNSIGNED NOT NULL,
        `event_type` INT UNSIGNED NOT NULL,
        `payload` VARBINARY(512) NOT NULL,
        `created_at_ms` BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS `player_skill_cards` (
        `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        `player_id` BIGINT UNSIGNED NOT NULL,
        `card_id` INT UNSIGNED NOT NULL,
        `level` INT UNSIGNED NOT NULL DEFAULT 1,
        `shards` INT UNSIGNED NOT NULL DEFAULT 0,
        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (`id`),
        UNIQUE KEY `uk_player_card` (`player_id`, `card_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS `player_skill_slots` (
        `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        `player_id` BIGINT UNSIGNED NOT NULL,
        `slot` INT UNSIGNED NOT NULL,
        `card_id` INT UNSIGNED NOT NULL,
        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (`id`),
        UNIQUE KEY `uk_player_slot` (`player_id`, `slot`),
        UNIQUE KEY `uk_player_card_once` (`player_id`, `card_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS `skill_card_grants` (
        `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        `player_id` BIGINT UNSIGNED NOT NULL,
        `idempotency_key` VARCHAR(128) NOT NULL,
        `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (`id`),
        UNIQUE KEY `uk_player_key` (`player_id`, `idempotency_key`),
        KEY `idx_created` (`created_at`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS `player_reward_claims` (
        `player_id` BIGINT UNSIGNED NOT NULL,
        `record` LONGBLOB NOT NULL,
        `version` INT NOT NULL DEFAULT 0,
        `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (`player_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
]

_TABLES = (
    "players",
    "player_heroes",
    "player_mmr",
    "mmr_history",
    "player_attributes",
    "attr_point_grants",
    "player_equipment",
    "player_talents",
    "talent_point_grants",
    "exp_history",
    "player_push_outbox",
    "player_skill_cards",
    "player_skill_slots",
    "skill_card_grants",
    "player_reward_claims",
)

CURVE = (100, 200)  # Lv1→2 需 100,Lv2→3 需 200;max_level = 3


@pytest.fixture
async def pool():
    """**函数作用域**的池 —— 这是 Python 迁移特有的坑,不是"没必要的重建"。

    pytest-asyncio 默认给每个用例新建 event loop,而 asyncmy 的池把内部 Task 绑在
    **创建时**的 loop 上。module 作用域的池会让第二个用例开始全部报
    `RuntimeError: got Future attached to a different loop`,表现是"第一个失败、其余
    全 ERROR",很容易被误读成连接池坏了。Go 侧没有这一层(database/sql 与 goroutine
    无绑定关系)。代价是每个用例重建池(本地 MySQL 上几十毫秒),换来完全隔离。
    """
    asyncmy = pytest.importorskip("asyncmy")
    pytest.importorskip(
        "cryptography",
        reason="MySQL 8.x 默认 caching_sha2_password 认证,Python 驱动需要 cryptography",
    )
    cfg = parse_go_dsn(DSN, default_db="pandora_player")
    # ★ **显式**挑字段,不用 `**cfg` 整个 splat —— 与 `mysqlx.pool_kwargs` 同一条纪律。
    # 真事:`parse_go_dsn` 加了 `net` 字段(为了支持 unix socket)之后,这里的 splat 让
    # `create_pool()` 抛 `TypeError: unexpected keyword argument 'net'`,而下面那个
    # `except Exception → pytest.skip` 把它**洗成了"MySQL 不可用"**:
    # 32 条真库用例悄悄停跑,全量套件照样报绿。
    conn_kw = {
        "host": cfg["host"], "port": cfg["port"], "user": cfg["user"],
        "password": cfg["password"], "db": cfg["db"],
    }
    # ★ 原先这里只白名单了 `except TypeError: raise` —— 只挡住了引发那次事故的
    #   **那一个**异常类型,换成 AttributeError / ValueError / KeyError 照样被洗成
    #   "MySQL 不可用" 并整体跳过(实测 32 passed → 32 skipped 且退出码 0)。
    #   判据改成放行集:只有环境类异常才 skip,其余原样冒红。见 tests/mysqlfixture.py。
    with skip_only_if_mysql_is_down(cfg, "player 数据层测试"):
        await ensure_database(asyncmy, cfg)
        p = await asyncio.wait_for(
            asyncmy.create_pool(**conn_kw, minsize=1, maxsize=8, autocommit=True), timeout=8
        )
    try:
        async with p.acquire() as conn, conn.cursor() as cur:
            for ddl in _DDL:
                await cur.execute(ddl)
            for table in _TABLES:
                await cur.execute(f"TRUNCATE TABLE `{table}`")  # noqa: S608
        yield p
    finally:
        p.close()
        await p.wait_closed()


@pytest.fixture
async def repo(pool):  # noqa: ANN001
    # ★ schema 必须是**本进程独占的测试库**。写死 pandora_player 的话,保留期清理用例
    # 会去 COUNT / DELETE 真实的 dev 库 —— 一个"验证清理"的测试反而删了别人的数据。
    return prepo.MySQLPlayerRepo(pool, parse_go_dsn(DSN, default_db="pandora_player")["db"])


# ── 档案 ─────────────────────────────────────────────────────────────────────


async def test_ensure_profile_is_insert_ignore_not_upsert(repo) -> None:  # noqa: ANN001
    """★ 已存在就**一个字都不改**。改成 UPSERT 会让玩家自己改的名字在下次登录被冲掉。"""

    assert await repo.ensure_profile(1, "第一个名字", 1500) is True
    assert await repo.ensure_profile(1, "第二个名字", 1500) is False
    profile = await repo.get_profile(1)
    assert profile.nickname == "第一个名字"
    assert profile.level == 1


async def test_ensure_profile_silently_skips_on_nickname_conflict(repo) -> None:  # noqa: ANN001
    """★ 昵称撞 uk 在 INSERT IGNORE 下**不报错**,只是没插进去。

    调用方必须按 created=False 复查 —— 不能默认"没建就是已经有了"。
    """

    assert await repo.ensure_profile(1, "同名", 1500) is True
    assert await repo.ensure_profile(2, "同名", 1500) is False
    assert await repo.get_profile(2) is None  # 2 号至此**没有**档案


async def test_update_nickname_conflict_and_not_found(repo) -> None:  # noqa: ANN001
    await repo.ensure_profile(1, "a", 1500)
    await repo.ensure_profile(2, "b", 1500)
    with pytest.raises(errcode.PandoraError) as taken:
        await repo.update_nickname(2, "a")
    assert taken.value.code == errcode.ErrPlayerNicknameTaken

    with pytest.raises(errcode.PandoraError) as missing:
        await repo.update_nickname(999, "c")
    assert missing.value.code == errcode.ErrPlayerNotFound

    # ★ 改成同一个名字 = 0 行受影响,但必须是**幂等成功**而不是 not found。
    await repo.update_nickname(1, "a")


async def test_list_nicknames_omits_missing(repo) -> None:  # noqa: ANN001
    await repo.ensure_profile(1, "n1", 1500)
    found = await repo.list_nicknames([1, 2])
    assert found == {1: "n1"}
    assert await repo.list_nicknames([]) == {}


# ── MMR ──────────────────────────────────────────────────────────────────────


async def test_apply_mmr_change_is_idempotent_on_duplicate_key(repo) -> None:  # noqa: ANN001
    """★ 不变量 §2:同一 idempotency_key 只算一次,重投读回已记录的 new_mmr。"""

    await repo.ensure_profile(1, "n", 1500)
    change = m.MMRChange(
        player_id=1,
        idempotency_key="match-1",
        rating_pool="3v3_ranked",
        baseline=1500,
        delta=25,
        reason="win",
        floor=0,
        inc_battle=True,
        inc_win=True,
    )
    first, already1 = await repo.apply_mmr_change(change)
    assert (first, already1) == (1525, False)

    second, already2 = await repo.apply_mmr_change(change)
    assert (second, already2) == (1525, True)

    # 战绩只加了一次。
    profile = await repo.get_profile(1)
    assert (profile.total_battles, profile.total_wins) == (1, 1)


async def test_idempotency_key_does_not_include_rating_pool(repo) -> None:  # noqa: ANN001
    """★ 一场对局只属于一个池:把池并入幂等键,同一 match 换个池名就能重复入账。"""

    await repo.ensure_profile(1, "n", 1500)
    base = dict(
        player_id=1,
        idempotency_key="match-1",
        baseline=1500,
        delta=25,
        reason="win",
        floor=0,
        inc_battle=True,
        inc_win=True,
    )
    await repo.apply_mmr_change(m.MMRChange(rating_pool="3v3_ranked", **base))
    _mmr, already = await repo.apply_mmr_change(
        m.MMRChange(rating_pool="5v5_ranked", **base)
    )
    assert already is True  # 换池不能绕过幂等


async def test_mmr_floor_is_clamped(repo) -> None:  # noqa: ANN001
    await repo.ensure_profile(1, "n", 1500)
    new_mmr, _ = await repo.apply_mmr_change(
        m.MMRChange(
            player_id=1,
            idempotency_key="k",
            rating_pool="3v3_ranked",
            baseline=10,
            delta=-100,
            reason="lose",
            floor=0,
            inc_battle=True,
            inc_win=False,
        )
    )
    assert new_mmr == 0


async def test_default_pool_dual_writes_legacy_column(repo) -> None:  # noqa: ANN001
    """expand 期 default 池双写 players.mmr —— 尚未排空的旧副本读该列才看得到新分。"""

    await repo.ensure_profile(1, "n", 1500)
    await repo.apply_mmr_change(
        m.MMRChange(
            player_id=1,
            idempotency_key="k",
            rating_pool="default",
            baseline=1500,
            delta=30,
            reason="win",
            floor=0,
            inc_battle=True,
            inc_win=True,
        )
    )
    profile = await repo.get_profile(1)
    assert profile.mmr == 1530
    assert await repo.get_mmr(1, "default") == (1530, True)


async def test_get_mmr_found_false_for_untouched_pool(repo) -> None:  # noqa: ANN001
    """★ found=False 表示该池**没有任何记录**,不是"分是 0"。

    建档时 players.mmr=1500 的兼容默认值不得被误认为"已有 default 段位记录"。
    """

    await repo.ensure_profile(1, "n", 1500)
    assert await repo.get_mmr(1, "default") == (0, False)
    assert await repo.get_mmr(1, "3v3_ranked") == (0, False)
    assert await repo.list_ratings(1) == []


async def test_apply_mmr_change_requires_existing_profile(repo) -> None:  # noqa: ANN001
    with pytest.raises(errcode.PandoraError) as excinfo:
        await repo.apply_mmr_change(
            m.MMRChange(
                player_id=404,
                idempotency_key="k",
                rating_pool="default",
                baseline=1500,
                delta=1,
                reason="win",
                floor=0,
                inc_battle=False,
                inc_win=False,
            )
        )
    assert excinfo.value.code == errcode.ErrPlayerNotFound


# ── 属性点 ───────────────────────────────────────────────────────────────────


async def test_grant_and_allocate_attribute_points(repo) -> None:  # noqa: ANN001
    await repo.ensure_profile(1, "n", 1500)
    assert await repo.grant_attribute_points(1, 10, "lv2") == (10, False)
    # 幂等命中:读回当前 unspent,不重复加。
    assert await repo.grant_attribute_points(1, 10, "lv2") == (10, True)

    unspent = await repo.allocate_attribute_points(
        1, [m.AttrAllocation(key="str", points=3), m.AttrAllocation(key="str", points=2)]
    )
    assert unspent == 5
    attrs, left = await repo.get_attributes(1)
    assert [(a.key, a.points) for a in attrs] == [("str", 5)]
    assert left == 5

    with pytest.raises(errcode.PandoraError) as insufficient:
        await repo.allocate_attribute_points(1, [m.AttrAllocation(key="agi", points=99)])
    assert insufficient.value.code == errcode.ErrPlayerInsufficientPoints

    assert await repo.reset_attributes(1) == 10
    attrs2, _ = await repo.get_attributes(1)
    assert attrs2 == []


async def test_allocate_is_zero_write_on_rejection(repo) -> None:  # noqa: ANN001
    """★ 越界必须**零写入**:所有校验都在第一条写之前完成。"""

    await repo.ensure_profile(1, "n", 1500)
    await repo.grant_attribute_points(1, 5, "g")
    with pytest.raises(errcode.PandoraError):
        await repo.allocate_attribute_points(
            1,
            [
                m.AttrAllocation(key="str", points=1),
                m.AttrAllocation(key="agi", points=99),  # 超过 unspent
            ],
        )
    attrs, unspent = await repo.get_attributes(1)
    assert attrs == [] and unspent == 5


# ── 装备预设 ─────────────────────────────────────────────────────────────────


async def test_set_equipment_rejects_zero_instance_id(repo) -> None:  # noqa: ANN001
    await repo.ensure_profile(1, "n", 1500)
    with pytest.raises(errcode.PandoraError) as excinfo:
        await repo.set_equipment(
            1, [m.EquipmentSlot(slot=1, item_config_id=10, instance_id=0)]
        )
    assert excinfo.value.code == errcode.ErrInvalidArg


async def test_set_equipment_full_replace(repo) -> None:  # noqa: ANN001
    await repo.ensure_profile(1, "n", 1500)
    await repo.set_equipment(
        1,
        [
            m.EquipmentSlot(slot=1, item_config_id=10, instance_id=101),
            m.EquipmentSlot(slot=2, item_config_id=11, instance_id=102),
        ],
    )
    await repo.set_equipment(
        1, [m.EquipmentSlot(slot=1, item_config_id=12, instance_id=103)]
    )
    slots = await repo.get_equipment(1)
    assert [(s.slot, s.item_config_id, s.instance_id) for s in slots] == [(1, 12, 103)]


async def test_equipment_schema_gate_passes_on_correct_ddl(repo) -> None:  # noqa: ANN001
    """★ 启动闸打真库:uk_player_instance 必须是 (player_id, instance_id) 且非前缀索引。"""
    await repo.validate_equipment_schema()


# ── 天赋 ─────────────────────────────────────────────────────────────────────


async def test_talent_spent_points_drive_unspent(repo) -> None:  # noqa: ANN001
    """★ 可点数按 spent_points 算,不按 Σ level 反推(cost_per_level≠1 时反推会算少)。"""

    await repo.ensure_profile(1, "n", 1500)
    assert await repo.grant_talent_points(1, 10, "g1") == (10, False)
    assert await repo.grant_talent_points(1, 10, "g1") == (10, True)  # 幂等

    left = await repo.set_talents(
        1, [m.TalentLevel(talent_id=1, level=2, spent_points=6)]
    )
    assert left == 4
    talents, unspent = await repo.get_talents(1)
    assert [(t.talent_id, t.level, t.spent_points) for t in talents] == [(1, 2, 6)]
    assert unspent == 4

    with pytest.raises(errcode.PandoraError) as insufficient:
        await repo.set_talents(1, [m.TalentLevel(talent_id=2, level=99, spent_points=99)])
    assert insufficient.value.code == errcode.ErrPlayerInsufficientPoints

    assert await repo.reset_talents(1) == 10


async def test_set_talents_rejects_missing_spent_points(repo) -> None:  # noqa: ANN001
    """spent_points 缺失说明调用方绕过了专精表校验,不能按"免费"落库。"""

    await repo.ensure_profile(1, "n", 1500)
    await repo.grant_talent_points(1, 10, "g")
    with pytest.raises(errcode.PandoraError) as excinfo:
        await repo.set_talents(1, [m.TalentLevel(talent_id=1, level=1, spent_points=0)])
    assert excinfo.value.code == errcode.ErrInvalidArg


async def test_legacy_rows_fall_back_to_level(repo, pool) -> None:  # noqa: ANN001
    """★ 共存窗口的桥:老副本写的行 spent_points=0,读侧回退按 level 计。

    不回退的话新副本会把那份分配当"没花点",玩家的可点数**虚高**。
    """

    await repo.ensure_profile(1, "n", 1500)
    await repo.grant_talent_points(1, 10, "g")
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO player_talents (player_id, talent_id, level, spent_points) "
            "VALUES (1, 7, 3, 0)"
        )
    talents, unspent = await repo.get_talents(1)
    assert talents[0].spent_points == 3  # 回退按 level
    assert unspent == 7


# ── 技能卡 ───────────────────────────────────────────────────────────────────


async def test_grant_skill_cards_idempotent_and_shards_accumulate(repo) -> None:  # noqa: ANN001
    await repo.ensure_profile(1, "n", 1500)
    cards, already = await repo.grant_skill_cards(
        1, [m.SkillCardGrant(card_id=5, shards=3)], "k1"
    )
    assert already is False
    assert [(c.card_id, c.level, c.shards) for c in cards] == [(5, 1, 3)]

    # 幂等命中:一张卡一片碎片都不加。
    cards2, already2 = await repo.grant_skill_cards(
        1, [m.SkillCardGrant(card_id=5, shards=99)], "k1"
    )
    assert already2 is True
    assert cards2[0].shards == 3

    # 新幂等键:碎片累加,**等级不动**(发放不改变培养进度)。
    cards3, _ = await repo.grant_skill_cards(
        1, [m.SkillCardGrant(card_id=5, shards=4)], "k2"
    )
    assert (cards3[0].level, cards3[0].shards) == (1, 7)


async def test_upgrade_skill_card_prices_from_locked_level(repo) -> None:  # noqa: ANN001
    await repo.ensure_profile(1, "n", 1500)
    await repo.grant_skill_cards(1, [m.SkillCardGrant(card_id=5, shards=20)], "k")
    curve = {2: 5, 3: 10}
    card, cost = await repo.upgrade_skill_card(1, 5, curve, 3)
    assert (card.level, card.shards, cost) == (2, 15, 5)
    card2, cost2 = await repo.upgrade_skill_card(1, 5, curve, 3)
    assert (card2.level, card2.shards, cost2) == (3, 5, 10)

    with pytest.raises(errcode.PandoraError) as maxed:
        await repo.upgrade_skill_card(1, 5, curve, 3)
    assert maxed.value.code == errcode.ErrSkillCardMaxLevel


async def test_upgrade_skill_card_error_codes(repo) -> None:  # noqa: ANN001
    await repo.ensure_profile(1, "n", 1500)
    with pytest.raises(errcode.PandoraError) as not_owned:
        await repo.upgrade_skill_card(1, 5, {2: 5}, 3)
    assert not_owned.value.code == errcode.ErrSkillCardNotOwned

    await repo.grant_skill_cards(1, [m.SkillCardGrant(card_id=5, shards=1)], "k")
    with pytest.raises(errcode.PandoraError) as shards:
        await repo.upgrade_skill_card(1, 5, {2: 5}, 3)
    assert shards.value.code == errcode.ErrSkillCardInsufficientShards

    # ★ 曲线断档绝不能当免费升级放行。
    with pytest.raises(errcode.PandoraError) as gap:
        await repo.upgrade_skill_card(1, 5, {}, 3)
    assert gap.value.code == errcode.ErrInternal


async def test_set_skill_slots_requires_ownership_and_is_full_replace(repo) -> None:  # noqa: ANN001
    await repo.ensure_profile(1, "n", 1500)
    await repo.grant_skill_cards(
        1, [m.SkillCardGrant(card_id=5, shards=0), m.SkillCardGrant(card_id=6, shards=0)], "k"
    )
    with pytest.raises(errcode.PandoraError) as not_owned:
        await repo.set_skill_slots(1, [m.SkillSlot(slot=0, card_id=999)])
    assert not_owned.value.code == errcode.ErrSkillCardNotOwned

    await repo.set_skill_slots(
        1, [m.SkillSlot(slot=0, card_id=5), m.SkillSlot(slot=1, card_id=6)]
    )
    await repo.set_skill_slots(1, [m.SkillSlot(slot=0, card_id=6)])
    slots = await repo.get_skill_slots(1)
    assert [(s.slot, s.card_id) for s in slots] == [(0, 6)]


async def test_uk_player_card_once_blocks_same_card_in_two_slots(repo) -> None:  # noqa: ANN001
    """库是最后一道:biz 已校验过一次,并发两次 SetSkillSlots 交错时靠它兜。"""

    await repo.ensure_profile(1, "n", 1500)
    await repo.grant_skill_cards(1, [m.SkillCardGrant(card_id=5, shards=0)], "k")
    with pytest.raises(errcode.PandoraError) as excinfo:
        await repo.set_skill_slots(
            1, [m.SkillSlot(slot=0, card_id=5), m.SkillSlot(slot=1, card_id=5)]
        )
    assert excinfo.value.code == errcode.ErrSkillCardSlotInvalid


# ── 经验 ─────────────────────────────────────────────────────────────────────


def _apply(player_id: int, delta: int, key: str) -> m.ExpApply:
    return m.ExpApply(
        player_id=player_id, delta=delta, reason="battle", idempotency_key=key, curve=CURVE
    )


async def test_apply_experience_writes_outbox_in_same_transaction(repo) -> None:  # noqa: ANN001
    """★ 入账与出箱**同一事务**(不变量 §4):payload 是入账后的权威快照。"""

    await repo.ensure_profile(1, "n", 1500)
    state, already = await repo.apply_experience(_apply(1, 250, "k1"))
    assert (state.level, state.exp_in_level, state.levels_gained) == (2, 150, 1)
    assert already is False

    records = await repo.fetch_push_outbox(10)
    assert len(records) == 1
    evt = ppb.PlayerExperienceEvent()
    evt.ParseFromString(records[0].payload)
    assert (evt.player_id, evt.level, evt.exp_in_level, evt.levels_gained) == (1, 2, 150, 1)
    assert records[0].event_type == int(ppb.PLAYER_PUSH_EVENT_TYPE_EXPERIENCE)

    await repo.delete_push_outbox(records[0].id)
    assert await repo.fetch_push_outbox(10) == []


async def test_apply_experience_idempotent_hit_does_not_double_count(repo) -> None:  # noqa: ANN001
    await repo.ensure_profile(1, "n", 1500)
    await repo.apply_experience(_apply(1, 250, "k1"))
    state, already = await repo.apply_experience(_apply(1, 250, "k1"))
    assert already is True
    assert (state.level, state.exp_in_level) == (2, 150)
    # 出箱只有一条(重放不重复出箱)。
    assert len(await repo.fetch_push_outbox(10)) == 1


async def test_max_level_noop_still_consumes_idempotency_key(repo) -> None:  # noqa: ANN001
    """★ 满级 no-op **仍落收据**:不落的话,曲线扩容后滞留的同一事件会被重新入账。"""

    await repo.ensure_profile(1, "n", 1500)
    await repo.apply_experience(_apply(1, 300, "k1"))  # 直接升到满级 3
    state, already = await repo.apply_experience(_apply(1, 50, "k2"))
    assert (state.level, state.exp_in_level, state.is_max_level) == (3, 0, True)
    assert already is False  # 首次消费该键

    # 重放同一键 → already=True(契约:true = 幂等命中)。
    _state, already2 = await repo.apply_experience(_apply(1, 50, "k2"))
    assert already2 is True


async def test_experience_schema_gate_passes_on_correct_ddl(repo) -> None:  # noqa: ANN001
    """★ 唯一索引探测:列名、顺序、SUB_PART 都要对。前缀索引会静默少发经验。"""
    await repo.validate_experience_schema()


async def test_experience_level_gate_rejects_out_of_range_rows(repo, pool) -> None:  # noqa: ANN001
    await repo.ensure_profile(1, "n", 1500)
    await repo.validate_experience_levels(3)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("UPDATE players SET level = 99 WHERE player_id = 1")
    with pytest.raises(errcode.PandoraError) as excinfo:
        await repo.validate_experience_levels(3)
    assert excinfo.value.code == errcode.ErrInvalidState


# ── 领奖(乐观锁)─────────────────────────────────────────────────────────────


async def test_reward_claims_optimistic_lock(repo) -> None:  # noqa: ANN001
    assert await repo.load_reward_claims(1) == (b"", 0)
    await repo.save_reward_claims(1, b"\x01", 0)
    record, version = await repo.load_reward_claims(1)
    assert (record, version) == (b"\x01", 1)

    await repo.save_reward_claims(1, b"\x02", 1)
    assert (await repo.load_reward_claims(1))[1] == 2

    # 版本不匹配 → ErrPlayerVersionMismatch(由 biz 决定是否重试)。
    with pytest.raises(errcode.PandoraError) as stale:
        await repo.save_reward_claims(1, b"\x03", 1)
    assert stale.value.code == errcode.ErrPlayerVersionMismatch

    # expect_version=0 但行已存在 → 同样是版本冲突,不是内部错。
    with pytest.raises(errcode.PandoraError) as dup:
        await repo.save_reward_claims(1, b"\x04", 0)
    assert dup.value.code == errcode.ErrPlayerVersionMismatch


# ── 保留期清理 ───────────────────────────────────────────────────────────────


async def test_sweep_report_only_counts_without_deleting(repo, pool) -> None:  # noqa: ANN001
    """★ §9.24 默认档:只统计不删。删了才发现是不可逆的。"""

    await repo.ensure_profile(1, "n", 1500)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO exp_history "
            "(player_id, idempotency_key, exp_delta, old_level, old_exp, new_level, new_exp, created_at) "
            "VALUES (1, 'old', 1, 1, 0, 1, 1, '2000-01-01 00:00:00')"
        )
    cutoff = _dt.datetime(2020, 1, 1)
    out = await repo.sweep_exp_history(dbguard.Mode.REPORT_ONLY, cutoff, 100)
    assert (out.matched, out.deleted) == (1, 0)

    out2 = await repo.sweep_exp_history(dbguard.Mode.DELETE, cutoff, 100)
    assert (out2.matched, out2.deleted) == (1, 1)
    out3 = await repo.sweep_exp_history(dbguard.Mode.DELETE, cutoff, 100)
    assert (out3.matched, out3.deleted) == (0, 0)


async def test_all_five_sweeps_hit_real_tables(repo) -> None:  # noqa: ANN001
    """五张只增表的 sweep 都要能真的执行(表名拼错在 report_only 下也会当场炸)。"""

    cutoff = _dt.datetime(2020, 1, 1)
    for sweep in (
        repo.sweep_exp_history,
        repo.sweep_mmr_history,
        repo.sweep_attr_point_grants,
        repo.sweep_talent_point_grants,
        repo.sweep_skill_card_grants,
    ):
        out = await sweep(dbguard.Mode.REPORT_ONLY, cutoff, 100)
        assert out.matched == 0


# ── 容量预算 ─────────────────────────────────────────────────────────────────


async def test_capacity_budget_tables_all_exist(repo, pool) -> None:  # noqa: ANN001
    """★ 预算清单里的表名拼错 = 那张表**永远不会被巡检**,且零信号。"""
    from pandorapy.services.player import budgets as pbudgets

    declared = {b.table for b in pbudgets.budgets()}
    assert declared == set(_TABLES), f"预算清单与 DDL 不一致: {declared ^ set(_TABLES)}"
    async with pool.acquire() as conn:
        result = await dbguard.check_budgets(
            conn, parse_go_dsn(DSN, default_db="pandora_player")["db"],
            pbudgets.budgets(),
        )
    assert result.checked >= 0

