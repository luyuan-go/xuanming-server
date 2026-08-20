"""mail 数据层测试 —— **打真实 MySQL**,不用 mock。

为什么必须打真库:被测的就是 SQL 本身 —— 收件箱上限的
`COUNT(*) FOR UPDATE + 驱逐 + INSERT` 同事务、游标的 `GREATEST` 单调、
`INSERT IGNORE` 的 rowcount 语义、`UPDATE` 未变更行 rowcount=0 的歧义、
按 channel 三段式的权限过滤。用 fake repo 测等于把被测对象换成了
"我对 SQL 的想象",而这些语义错了全都是**静默**错(读到别人的邮件、
水位回退导致重复收信、收件箱涨破上限),不会抛异常。

没有库就整体 skip(**不假装通过**):
  docker run -d --name pandora-mysql-verify -p 13306:3306 \
    -e MYSQL_ROOT_PASSWORD=pandora_dev_root -e MYSQL_DATABASE=pandora_owner \
    mysql:8.4 --sql-mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"

环境变量 PANDORA_TEST_MYSQL_DSN 可覆盖(与 Go 侧 CI 的门控变量同名)。
"""

from __future__ import annotations

import asyncio

import pytest

from mysqlfixture import MYSQL_DSN as DSN
from mysqlfixture import ensure_database, parse_go_dsn, skip_only_if_mysql_is_down

from pandorapy import errcode
from pandorapy.services.mail import data as mdata


# 与 deploy/mysql-init/12-mail-tables.sql 同构(guild_members 取自 11-guild-tables.sql,
# 公会邮件的领取权限判定要 JOIN 它)。
_TABLES = (
    "sys_mail",
    "guild_mail",
    "player_mail",
    "player_mail_cursor",
    "player_mail_claim",
    "player_mail_archive",
    "guild_members",
)

_DDL = [
    """CREATE TABLE IF NOT EXISTS sys_mail (
        mail_id BIGINT UNSIGNED NOT NULL,
        start_ms BIGINT NOT NULL DEFAULT 0,
        end_ms BIGINT NOT NULL DEFAULT 0,
        payload BLOB NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (mail_id),
        KEY idx_end (end_ms)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS guild_mail (
        mail_id BIGINT UNSIGNED NOT NULL,
        guild_id BIGINT UNSIGNED NOT NULL,
        start_ms BIGINT NOT NULL DEFAULT 0,
        end_ms BIGINT NOT NULL DEFAULT 0,
        payload BLOB NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (mail_id),
        KEY idx_guild (guild_id, mail_id),
        KEY idx_end (end_ms)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS player_mail (
        mail_id BIGINT UNSIGNED NOT NULL,
        player_id BIGINT UNSIGNED NOT NULL,
        status TINYINT NOT NULL DEFAULT 1,
        claimed TINYINT NOT NULL DEFAULT 0,
        expire_ms BIGINT NOT NULL DEFAULT 0,
        payload BLOB NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (mail_id),
        KEY idx_player_status (player_id, status),
        KEY idx_expire (expire_ms)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS player_mail_cursor (
        player_id BIGINT UNSIGNED NOT NULL,
        last_sys_mail_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
        last_guild_mail_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS player_mail_claim (
        player_id BIGINT UNSIGNED NOT NULL,
        mail_id BIGINT UNSIGNED NOT NULL,
        claimed TINYINT NOT NULL DEFAULT 1,
        intent_payload BLOB NULL,
        claimed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (player_id, mail_id),
        KEY idx_mail (mail_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS player_mail_archive (
        mail_id BIGINT UNSIGNED NOT NULL,
        player_id BIGINT UNSIGNED NOT NULL,
        status TINYINT NOT NULL,
        expire_ms BIGINT NOT NULL,
        created_ms BIGINT NOT NULL,
        payload BLOB NOT NULL,
        archived_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (mail_id),
        KEY idx_player (player_id),
        KEY idx_archived (archived_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS guild_members (
        player_id BIGINT UNSIGNED NOT NULL,
        guild_id BIGINT UNSIGNED NOT NULL,
        role TINYINT NOT NULL DEFAULT 3,
        joined_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (player_id),
        KEY idx_guild_role (guild_id, role)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
]

PLAYER = 4242
OTHER = 4343
GUILD = 77
NOW = 1_800_000_000_000


@pytest.fixture
async def pool():
    """每个用例一个连接池 —— **必须 function 作用域**。

    pytest-asyncio 给每个用例新建 event loop,而 asyncmy 的池把内部 Task 绑在
    创建时的 loop 上;module 作用域会让第二个用例起全部报
    `got Future attached to a different loop`(与 test_owner_repo.py 同因)。

    ★ autocommit=True 与生产一致(见 data.MySQLMailRepo 头注释):
      建成 False 会让每条 SELECT 隐式开事务不提交,连接复用时读到陈旧快照。
    """
    asyncmy = pytest.importorskip("asyncmy")
    pytest.importorskip(
        "cryptography",
        reason="MySQL 8.x 默认 caching_sha2_password 认证,Python 驱动需要 cryptography",
    )
    cfg = parse_go_dsn(DSN, default_db="pandora_social")
    # * 只有"MySQL 没起来"才允许跳过 —— 夹具自己的代码(ensure_database / cfg 取键 /
    #   create_pool 形参)抛的异常必须原样冒红。判据、放行集与 CI 文案契约
    #   见 tests/mysqlfixture.py 的 skip_only_if_mysql_is_down。
    with skip_only_if_mysql_is_down(
        cfg,
        "mail 数据层测试",
        hint=(
            "起一个:docker run -d -p 13306:3306 -e MYSQL_ROOT_PASSWORD=pandora_dev_root "
            'mysql:8.4 --sql-mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"'
        ),
    ):
        await ensure_database(asyncmy, cfg)
        p = await asyncio.wait_for(
            asyncmy.create_pool(
                host=cfg["host"], port=cfg["port"], user=cfg["user"],
                password=cfg["password"], db=cfg["db"], minsize=1, maxsize=8,
                autocommit=True,
            ),
            timeout=8,
        )
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
async def repo(pool):
    return mdata.MySQLMailRepo(pool)


async def _exec(pool, sql, args=()):  # noqa: ANN001
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(sql, args)
        return await cur.fetchall()


# ── 个人邮件:写入 / 列表 / 上限 ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_insert_and_list_personal(repo) -> None:
    for i in range(3):
        await repo.insert_personal_mail(100 + i, PLAYER, 0, b"p%d" % i, 200)
    rows = await repo.list_personal(PLAYER, NOW, 0, 10)
    assert [r.mail_id for r in rows] == [102, 101, 100]  # 倒序
    assert rows[0].status == mdata.STATUS_UNREAD
    assert rows[0].claimed is False
    assert rows[0].created_ms > 0
    # before_id 翻页
    page2 = await repo.list_personal(PLAYER, NOW, 102, 10)
    assert [r.mail_id for r in page2] == [101, 100]


@pytest.mark.asyncio
async def test_list_personal_hides_expired(repo) -> None:
    """过期行必须对玩家不可见 —— 它要等 sweep 的缓冲期才物理删,
    期间若还能列出来,玩家会看到一封点开就报错的邮件。"""
    await repo.insert_personal_mail(1, PLAYER, NOW - 1, b"gone", 200)
    await repo.insert_personal_mail(2, PLAYER, 0, b"forever", 200)  # 0 = 永不过期
    await repo.insert_personal_mail(3, PLAYER, NOW + 1, b"alive", 200)
    rows = await repo.list_personal(PLAYER, NOW, 0, 10)
    assert {r.mail_id for r in rows} == {2, 3}


@pytest.mark.asyncio
async def test_inbox_full_evicts_oldest_claimed(repo) -> None:
    """满时先驱逐最旧的**已领**邮件(附件已落袋,删掉无损)。"""
    await repo.insert_personal_mail(1, PLAYER, 0, b"a", 3)
    await repo.insert_personal_mail(2, PLAYER, 0, b"b", 3)
    await repo.insert_personal_mail(3, PLAYER, 0, b"c", 3)
    await repo.set_personal_status(PLAYER, 1, mdata.STATUS_CLAIMED)

    await repo.insert_personal_mail(4, PLAYER, 0, b"d", 3)
    rows = await repo.list_personal(PLAYER, NOW, 0, 10)
    assert {r.mail_id for r in rows} == {2, 3, 4}


@pytest.mark.asyncio
async def test_inbox_full_rejects_when_nothing_evictable(repo) -> None:
    """★ 驱逐不出名额时必须回 ERR_MAIL_BOX_FULL 且**整笔回滚**。

    这里若把错误包成 ErrInternal,调用方(battle_result 掉落转邮件)分不出
    "该补扫重试"和"真出故障";若不回滚,收件箱就会涨破上限。
    """
    for i in range(3):
        await repo.insert_personal_mail(i + 1, PLAYER, 0, b"x", 3)
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.insert_personal_mail(9, PLAYER, 0, b"y", 3)
    assert ei.value.code == errcode.ErrMailBoxFull
    rows = await repo.list_personal(PLAYER, NOW, 0, 10)
    assert {r.mail_id for r in rows} == {1, 2, 3}


@pytest.mark.asyncio
async def test_inbox_limit_disabled_when_zero(repo) -> None:
    """max_inbox<=0 = 不校验(与 Go 的 `if maxInbox > 0` 一致),不是"一封都不许发"。"""
    for i in range(5):
        await repo.insert_personal_mail(i + 1, PLAYER, 0, b"x", 0)
    assert len(await repo.list_personal(PLAYER, NOW, 0, 10)) == 5


@pytest.mark.asyncio
async def test_set_status_claimed_column_only_sets(repo) -> None:
    """claimed 列**只置不清**:领过奖是既成事实,不该被后续状态变更抹掉。"""
    await repo.insert_personal_mail(1, PLAYER, 0, b"x", 200)
    await repo.set_personal_status(PLAYER, 1, mdata.STATUS_CLAIMED)
    await repo.set_personal_status(PLAYER, 1, mdata.STATUS_READ)
    rows = await repo.list_personal(PLAYER, NOW, 0, 10)
    assert rows[0].status == mdata.STATUS_READ
    assert rows[0].claimed is True


@pytest.mark.asyncio
async def test_delete_personal_scoped_by_player(repo) -> None:
    """删除带 player_id 条件 → 越权删除在 SQL 层就不可能,不靠上层校验。"""
    await repo.insert_personal_mail(1, PLAYER, 0, b"x", 200)
    await repo.delete_personal(OTHER, 1)
    assert len(await repo.list_personal(PLAYER, NOW, 0, 10)) == 1
    await repo.delete_personal(PLAYER, 1)
    assert await repo.list_personal(PLAYER, NOW, 0, 10) == []


# ── 游标 ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cursor_defaults_to_zero_and_is_monotonic(repo) -> None:
    """★ 水位必须单调不回退。

    并发 ListMail(多端登录 / 重试)各自算出自己那批的最大值,后到的若比先到的小,
    直接赋值就会把水位推回去 —— 玩家重复收到同一批系统邮件。
    """
    assert await repo.get_cursor(PLAYER) == (0, 0)
    await repo.advance_cursor(PLAYER, 50, 60)
    assert await repo.get_cursor(PLAYER) == (50, 60)
    await repo.advance_cursor(PLAYER, 10, 70)  # sys 回退、guild 前进
    assert await repo.get_cursor(PLAYER) == (50, 70)


# ── 领取权限:三个 channel ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_claimable_personal_only_recipient(repo) -> None:
    await repo.insert_personal_mail(1, PLAYER, 0, b"mine", 200)
    assert await repo.get_claimable_payload(PLAYER, 1, NOW) == b"mine"
    assert await repo.get_claimable_payload(OTHER, 1, NOW) is None


@pytest.mark.asyncio
async def test_claimable_personal_expired_is_not_claimable(repo) -> None:
    await repo.insert_personal_mail(1, PLAYER, NOW - 1, b"old", 200)
    assert await repo.get_claimable_payload(PLAYER, 1, NOW) is None


@pytest.mark.asyncio
async def test_claimable_sys_respects_window(repo) -> None:
    await repo.insert_sys_mail(1, 0, 0, b"always")  # 0/0 = 立即生效、永不过期
    await repo.insert_sys_mail(2, NOW + 1000, 0, b"future")
    await repo.insert_sys_mail(3, 0, NOW - 1, b"ended")
    assert await repo.get_claimable_payload(PLAYER, 1, NOW) == b"always"
    assert await repo.get_claimable_payload(PLAYER, 2, NOW) is None
    assert await repo.get_claimable_payload(PLAYER, 3, NOW) is None


@pytest.mark.asyncio
async def test_claimable_guild_requires_current_membership(pool, repo) -> None:
    """★ 公会邮件的权限是"**当前**仍是成员",不是"发信时是成员"。

    退会之后还能领,等于退会玩家继续吃公会福利。
    """
    await repo.insert_guild_mail(1, GUILD, 0, 0, b"guildmail")
    assert await repo.get_claimable_payload(PLAYER, 1, NOW) is None
    await _exec(pool, "INSERT INTO guild_members (player_id, guild_id) VALUES (%s, %s)",
                (PLAYER, GUILD))
    assert await repo.get_claimable_payload(PLAYER, 1, NOW) == b"guildmail"
    await _exec(pool, "DELETE FROM guild_members WHERE player_id = %s", (PLAYER,))
    assert await repo.get_claimable_payload(PLAYER, 1, NOW) is None


@pytest.mark.asyncio
async def test_get_player_guild(pool, repo) -> None:
    assert await repo.get_player_guild(PLAYER) is None
    await _exec(pool, "INSERT INTO guild_members (player_id, guild_id) VALUES (%s, %s)",
                (PLAYER, GUILD))
    assert await repo.get_player_guild(PLAYER) == GUILD


@pytest.mark.asyncio
async def test_list_channel_since_watermark(pool, repo) -> None:
    await repo.insert_sys_mail(10, 0, 0, b"a")
    await repo.insert_sys_mail(11, 0, 0, b"b")
    await repo.insert_guild_mail(20, GUILD, 0, 0, b"g1")
    await repo.insert_guild_mail(21, 999, 0, 0, b"other-guild")
    assert [m.mail_id for m in await repo.list_sys_since(10, NOW)] == [11]
    assert [m.mail_id for m in await repo.list_guild_since(GUILD, 0, NOW)] == [20]


# ── 领取记录 / DS 意图 ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_record_claim_is_idempotent(repo) -> None:
    assert await repo.record_claim(PLAYER, 1) is True
    assert await repo.record_claim(PLAYER, 1) is False
    assert await repo.has_claimed(PLAYER, 1) is True


@pytest.mark.asyncio
async def test_has_claimed_ignores_open_intent(repo) -> None:
    """★ 意图行(claimed=0)**不算已领**。

    算成已领的话,列表里那封邮件会立刻显示"已领"而物品还没入包。
    """
    await repo.create_claim_intent(PLAYER, 1, b"intent")
    assert await repo.has_claimed(PLAYER, 1) is False
    assert await repo.get_claim_state(PLAYER, 1) == (False, True)


@pytest.mark.asyncio
async def test_create_claim_intent_never_overwrites(repo) -> None:
    """★ INSERT IGNORE:已有行不覆盖。

    覆盖会换掉已铸的 instance_id,而 bag journal 靠内容指纹去重 —— 换了 ID 就是双发。
    """
    assert await repo.create_claim_intent(PLAYER, 1, b"first") is True
    assert await repo.create_claim_intent(PLAYER, 1, b"second") is False
    assert await repo.get_claim_intent(PLAYER, 1) == b"first"


@pytest.mark.asyncio
async def test_get_claim_intent_hidden_after_terminal(repo) -> None:
    await repo.create_claim_intent(PLAYER, 1, b"intent")
    await repo.mark_claimed(PLAYER, 1)
    assert await repo.get_claim_intent(PLAYER, 1) is None
    assert await repo.get_claim_state(PLAYER, 1) == (True, False)


@pytest.mark.asyncio
async def test_mark_claimed_is_idempotent(repo) -> None:
    """★ 重复 Mark 必须成功。

    MySQL 的 UPDATE 不计未变更行,rowcount=0 同时意味着"没这行"和"已经是 1 了"——
    直接把 0 当"没这行"会让崩溃重放这条正常路径报错。
    """
    await repo.create_claim_intent(PLAYER, 1, b"intent")
    assert await repo.mark_claimed(PLAYER, 1) is True
    assert await repo.mark_claimed(PLAYER, 1) is True  # 第二次:rowcount=0 但行存在
    assert await repo.mark_claimed(PLAYER, 999) is False  # 真的没这行


@pytest.mark.asyncio
async def test_claim_state_absent(repo) -> None:
    assert await repo.get_claim_state(PLAYER, 1) == (False, False)


# ── sweep ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_expired_personal_excludes_never_expiring(repo) -> None:
    """expire_ms=0 是"永不过期",漏掉 `expire_ms > 0` 这一半会把永久邮件全清了。"""
    await repo.insert_personal_mail(1, PLAYER, 0, b"forever", 200)
    await repo.insert_personal_mail(2, PLAYER, NOW - 10, b"old", 200)
    await repo.insert_personal_mail(3, PLAYER, NOW + 10, b"future", 200)
    rows = await repo.list_expired_personal(NOW, 10)
    assert [r.mail_id for r in rows] == [2]
    assert rows[0].player_id == PLAYER
    assert rows[0].payload == b"old"


@pytest.mark.asyncio
async def test_archive_and_delete_is_atomic(pool, repo) -> None:
    """归档 + 删除同事务。分两步的话"删了但没归档"= 带未领附件的邮件被无声销毁。"""
    await repo.insert_personal_mail(1, PLAYER, NOW - 10, b"withatt", 200)
    await repo.insert_personal_mail(2, PLAYER, NOW - 10, b"noatt", 200)
    rows = await repo.list_expired_personal(NOW, 10)
    await repo.archive_and_delete_personal([r for r in rows if r.mail_id == 1],
                                           [r.mail_id for r in rows])
    assert await repo.list_personal(PLAYER, NOW, 0, 10) == []
    archived = await _exec(pool, "SELECT mail_id, payload FROM player_mail_archive")
    assert [(int(r[0]), bytes(r[1])) for r in archived] == [(1, b"withatt")]


@pytest.mark.asyncio
async def test_archive_insert_ignore_is_replay_safe(pool, repo) -> None:
    """多副本各自跑 sweep,同一行可能被归档两次 —— INSERT IGNORE 必须吃得下。"""
    row = mdata.ExpiredPersonalRow(1, PLAYER, mdata.STATUS_UNREAD, NOW, NOW, b"x")
    await repo.archive_and_delete_personal([row], [1])
    await repo.archive_and_delete_personal([row], [1])
    cnt = await _exec(pool, "SELECT COUNT(*) FROM player_mail_archive")
    assert int(cnt[0][0]) == 1


@pytest.mark.asyncio
async def test_delete_channel_mail_ended_before(repo) -> None:
    await repo.insert_sys_mail(1, 0, NOW - 10, b"a")
    await repo.insert_sys_mail(2, 0, 0, b"forever")
    await repo.insert_guild_mail(3, GUILD, 0, NOW - 10, b"b")
    assert await repo.delete_sys_mail_ended_before(NOW, 100) == 1
    assert await repo.delete_guild_mail_ended_before(NOW, 100) == 1
    # end_ms=0 的永久邮件不受影响
    assert await repo.get_claimable_payload(PLAYER, 2, NOW) == b"forever"


@pytest.mark.asyncio
async def test_delete_claims_before_and_purge_archive(pool, repo) -> None:
    await repo.record_claim(PLAYER, 100)
    await repo.record_claim(PLAYER, 200)
    assert await repo.delete_claims_before(150, 100) == 1
    assert await repo.has_claimed(PLAYER, 100) is False
    assert await repo.has_claimed(PLAYER, 200) is True

    row = mdata.ExpiredPersonalRow(1, PLAYER, mdata.STATUS_UNREAD, NOW, NOW, b"x")
    await repo.archive_and_delete_personal([row], [1])
    # 归档表自身也要有界:把 archived_at 拨到过去再按保留期清。
    await _exec(pool, "UPDATE player_mail_archive SET archived_at = DATE_SUB(NOW(), INTERVAL 10 DAY)")
    assert await repo.purge_archive_before(5, 100) == 1


@pytest.mark.asyncio
async def test_sweep_batch_limit_is_honored(repo) -> None:
    """单批 limit 有界 —— 一次删几十万行会长事务锁表,在线玩家收件箱直接卡死。"""
    for i in range(5):
        await repo.insert_sys_mail(i + 1, 0, NOW - 10, b"x")
    assert await repo.delete_sys_mail_ended_before(NOW, 2) == 2
    assert await repo.delete_sys_mail_ended_before(NOW, 2) == 2
    assert await repo.delete_sys_mail_ended_before(NOW, 2) == 1
