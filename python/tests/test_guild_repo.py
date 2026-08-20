"""公会数据层测试 —— 打真实 MySQL。

咬住的是**只在并发 / 事务里才暴露、平时全绿**的四类事:

  ① §9.18 两个写入侧上限必须在**事务内原子**校验:
       max_guild_members(ApproveJoin)、max_pending_requests_per_guild(CreateJoinRequest)
     并发压上去还能突破 = 那道闸等于没有。
  ② 「申满 → 全被拒 → 全部复开」不能绕过 pending 上限
     (复开走的是 UPDATE 分支,少校验一次就漏)。
  ③ 会长 / 群主的 TOCTOU:退会与转让交错不能删掉刚晋升的新会长。
  ④ ListJoinRequests 必须是 cursor 分页(§9.18 读取侧)。

没有库就整体 skip(不假装通过)。
"""

from __future__ import annotations

import asyncio

import pytest

from mysqlfixture import MYSQL_DSN as DSN
from mysqlfixture import ensure_database, parse_go_dsn, skip_only_if_mysql_is_down

from pandorapy import dbguard, errcode
from pandorapy.services.guild import guild_repo as grepo
from pandorapy.services.guild import rows as growsmod
from pandorapy.services.guild import schema as gschema


# ★ 与 deploy/mysql-init/11-guild-tables.sql 逐列一致 —— 尤其是
#   guilds.pending_request_count(signed int NOT NULL DEFAULT 0)与
#   player_group_counts 的单列主键:schema 契约闸就是在校验这几样。
_DDL = [
    """CREATE TABLE IF NOT EXISTS guilds (
        guild_id BIGINT UNSIGNED NOT NULL,
        name VARCHAR(64) NOT NULL,
        leader_id BIGINT UNSIGNED NOT NULL,
        member_count INT NOT NULL DEFAULT 1,
        pending_request_count INT NOT NULL DEFAULT 0,
        max_members INT NOT NULL DEFAULT 100,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (guild_id),
        UNIQUE KEY uk_name (name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS guild_members (
        player_id BIGINT UNSIGNED NOT NULL,
        guild_id BIGINT UNSIGNED NOT NULL,
        role TINYINT NOT NULL DEFAULT 3,
        joined_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (player_id),
        KEY idx_guild_role (guild_id, role)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS guild_join_requests (
        request_id BIGINT UNSIGNED NOT NULL,
        guild_id BIGINT UNSIGNED NOT NULL,
        player_id BIGINT UNSIGNED NOT NULL,
        status TINYINT NOT NULL DEFAULT 1,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                   ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (request_id),
        UNIQUE KEY uk_guild_player (guild_id, player_id),
        KEY idx_guild_status (guild_id, status),
        KEY idx_status_updated (status, updated_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS player_group_counts (
        player_id BIGINT UNSIGNED NOT NULL,
        group_count INT NOT NULL DEFAULT 0,
        PRIMARY KEY (player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
]

_TABLES = ["guilds", "guild_members", "guild_join_requests", "player_group_counts"]


@pytest.fixture
async def pool():
    asyncmy = pytest.importorskip("asyncmy")
    pytest.importorskip("cryptography", reason="MySQL 8.x caching_sha2_password 需要它")
    cfg = parse_go_dsn(DSN, default_db="pandora_social")
    # * 只有"MySQL 没起来"才允许跳过 —— 夹具自己的代码(ensure_database / cfg 取键 /
    #   create_pool 形参)抛的异常必须原样冒红。判据、放行集与 CI 文案契约
    #   见 tests/mysqlfixture.py 的 skip_only_if_mysql_is_down。
    with skip_only_if_mysql_is_down(cfg, "公会数据层测试"):
        await ensure_database(asyncmy, cfg)
        p = await asyncio.wait_for(
            asyncmy.create_pool(
                host=cfg["host"], port=cfg["port"], user=cfg["user"],
                password=cfg["password"], db=cfg["db"],
                minsize=2, maxsize=24, autocommit=True,
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
    # schema 取本进程独占库(sweep 走全限定表名,见 MySQLGuildRepo.__init__ 注释)。
    cfg = parse_go_dsn(DSN, default_db="pandora_social")
    return grepo.MySQLGuildRepo(pool, cfg["db"])


# ── 单归属 / 建会 ────────────────────────────────────────────────────────────


async def test_create_guild_enforces_single_membership(repo) -> None:
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.create_guild(2, leader_id=100, name="B", max_members=100)
    assert exc.value.code == errcode.ErrGuildAlreadyInGuild


async def test_duplicate_name_maps_to_business_code(repo) -> None:
    """uk_name 冲突必须译成 ErrGuildNameTaken,而不是漏成 ErrInternal(=客户端看到 500)。"""
    await repo.create_guild(1, leader_id=100, name="Same", max_members=100)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.create_guild(2, leader_id=101, name="Same", max_members=100)
    assert exc.value.code == errcode.ErrGuildNameTaken


# ── ★ §9.18 pending 申请上限 ────────────────────────────────────────────────


async def test_pending_request_limit(repo) -> None:
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    for i in range(3):
        await repo.create_join_request(1000 + i, 1, 200 + i, max_pending=3)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.create_join_request(1099, 1, 299, max_pending=3)
    assert exc.value.code == errcode.ErrGuildRequestLimit


async def test_reopening_rejected_requests_still_hits_pending_limit(repo) -> None:
    """★ 「申满 → 全被拒 → 全部复开」不能绕过上限。

    复开走的是 UPDATE 分支(复用 request_id),与新增是两段代码;
    只在新增那段校验上限的话,这条路径能把 pending 数刷到任意大。
    """
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    for i in range(2):
        rid, _ = await repo.create_join_request(1000 + i, 1, 200 + i, max_pending=2)
        assert await repo.reject_join(rid, 100) is True
    # 两条已终态 → pending 归零,可以再申请两条
    await repo.create_join_request(1000, 1, 200, max_pending=2)
    await repo.create_join_request(1001, 1, 201, max_pending=2)
    # 第三个人被上限挡住
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.create_join_request(1002, 1, 202, max_pending=2)
    assert exc.value.code == errcode.ErrGuildRequestLimit


async def test_repeated_apply_is_idempotent_and_takes_one_slot(repo) -> None:
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    rid1, reused1 = await repo.create_join_request(1000, 1, 200, max_pending=1)
    rid2, reused2 = await repo.create_join_request(1001, 1, 200, max_pending=1)
    assert reused1 is False
    assert reused2 is True and rid2 == rid1, "重复申请必须复用同一 request_id,不占新名额"


async def test_concurrent_applies_do_not_exceed_pending_limit(repo) -> None:
    """★ 上限必须在事务里原子成立 —— 并发压上去也不能突破。"""
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    limit = 4

    async def apply(i: int):
        try:
            await repo.create_join_request(2000 + i, 1, 300 + i, max_pending=limit)
            return None
        except errcode.PandoraError as exc:
            return exc.code

    results = await asyncio.gather(*[apply(i) for i in range(16)])
    ok = [r for r in results if r is None]
    limited = [r for r in results if r == errcode.ErrGuildRequestLimit]
    assert len(ok) == limit, f"pending 上限被突破:成功了 {len(ok)} 条"
    assert len(ok) + len(limited) == 16, "有非预期错误"


async def test_pending_count_column_self_heals_from_details(repo, pool) -> None:
    """计数列被写脏时,上限判定仍必须正确 —— 权威是明细 COUNT,计数列绝对值回写。"""
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    await repo.create_join_request(1000, 1, 200, max_pending=5)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("UPDATE guilds SET pending_request_count = 999 WHERE guild_id = 1")
    # 脏计数不能把后续申请判成超限
    await repo.create_join_request(1001, 1, 201, max_pending=5)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT pending_request_count FROM guilds WHERE guild_id = 1")
        assert int((await cur.fetchone())[0]) == 2, "计数列必须被明细绝对值回写自愈"


# ── ★ §9.18 公会成员上限 ────────────────────────────────────────────────────


async def test_concurrent_approvals_do_not_exceed_member_limit(repo) -> None:
    """★ max_guild_members 必须在 ApproveJoin 事务内原子校验。"""
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    max_members = 4  # 含会长
    ids = []
    for i in range(10):
        rid, _ = await repo.create_join_request(3000 + i, 1, 400 + i, max_pending=100)
        ids.append(rid)

    async def approve(rid: int):
        try:
            return await repo.approve_join(rid, 100, max_members)
        except errcode.PandoraError as exc:
            return exc.code

    results = await asyncio.gather(*[approve(r) for r in ids])
    approved = [r for r in results if r is True]
    full = [r for r in results if r == errcode.ErrGuildFull]
    assert len(approved) == max_members - 1, f"成员上限被突破:批准了 {len(approved)} 人"
    assert len(approved) + len(full) == 10, f"有非预期错误:{results}"


async def test_only_leader_or_officer_can_approve(repo) -> None:
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    rid, _ = await repo.create_join_request(1000, 1, 200, max_pending=10)
    assert await repo.approve_join(rid, 100, 100) is True  # 会长批准
    rid2, _ = await repo.create_join_request(1001, 1, 201, max_pending=10)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.approve_join(rid2, 200, 100)  # 200 只是普通成员
    assert exc.value.code == errcode.ErrGuildNoPermission


async def test_approve_twice_returns_not_pending(repo) -> None:
    """第二次审批返回 False(已被并发处理),而不是重复加人。"""
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    rid, _ = await repo.create_join_request(1000, 1, 200, max_pending=10)
    assert await repo.approve_join(rid, 100, 100) is True
    assert await repo.approve_join(rid, 100, 100) is False


# ── ★ 会长 TOCTOU ──────────────────────────────────────────────────────────


async def test_leader_cannot_be_removed(repo) -> None:
    """会长必须先转让 / 解散才能走 —— 否则会留下悬空 leader_id,公会永久无主。"""
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.remove_member(1, 100)
    assert exc.value.code == errcode.ErrGuildNotLeader


async def test_stale_leader_cannot_disband_after_transfer(repo) -> None:
    """转让之后旧会长再解散必须失败(持父行锁复核现任会长)。"""
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    rid, _ = await repo.create_join_request(1000, 1, 200, max_pending=10)
    await repo.approve_join(rid, 100, 100)
    await repo.transfer_leader(1, 100, 200)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.disband_guild(1, 100)
    assert exc.value.code == errcode.ErrGuildNotLeader


async def test_disband_returns_all_members_atomically(repo) -> None:
    """解散必须返回**实际被删**的成员集合 —— 缓存失效与通知都靠它。"""
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    for i in range(3):
        rid, _ = await repo.create_join_request(1000 + i, 1, 200 + i, max_pending=10)
        await repo.approve_join(rid, 100, 100)
    deleted = await repo.disband_guild(1, 100)
    assert sorted(deleted) == [100, 200, 201, 202]
    assert await repo.get_guild(1) is None


async def test_set_role_cannot_touch_current_leader(repo) -> None:
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.set_role(1, 100, 100, growsmod.GUILD_ROLE_MEMBER)
    assert exc.value.code == errcode.ErrGuildNoPermission


async def test_officer_cannot_kick_officer(repo) -> None:
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    for pid in (200, 201):
        rid, _ = await repo.create_join_request(1000 + pid, 1, pid, max_pending=10)
        await repo.approve_join(rid, 100, 100)
    await repo.set_role(1, 100, 200, growsmod.GUILD_ROLE_OFFICER)
    await repo.set_role(1, 100, 201, growsmod.GUILD_ROLE_OFFICER)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.kick_member(1, 200, 201)
    assert exc.value.code == errcode.ErrGuildNoPermission


# ── ★ 读取侧 cursor 分页(§9.18)────────────────────────────────────────────


async def test_list_pending_requests_is_cursor_paged(repo) -> None:
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    for i in range(5):
        await repo.create_join_request(1000 + i, 1, 200 + i, max_pending=100)
    page1 = await repo.list_pending_requests(1, 0, 2)
    assert [r.request_id for r in page1] == [1000, 1001]
    page2 = await repo.list_pending_requests(1, page1[-1].request_id, 2)
    assert [r.request_id for r in page2] == [1002, 1003]
    page3 = await repo.list_pending_requests(1, page2[-1].request_id, 2)
    assert [r.request_id for r in page3] == [1004]


async def test_list_members_is_cursor_paged(repo) -> None:
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    for i in range(3):
        rid, _ = await repo.create_join_request(1000 + i, 1, 200 + i, max_pending=100)
        await repo.approve_join(rid, 100, 100)
    page1 = await repo.list_members(1, 0, 2)
    assert [m.player_id for m in page1] == [100, 200]
    page2 = await repo.list_members(1, page1[-1].player_id, 2)
    assert [m.player_id for m in page2] == [201, 202]


# ── ★ 保留期清理默认只报告不删(§9.24)──────────────────────────────────────


async def test_sweep_defaults_to_report_only(repo, pool) -> None:
    """★ 默认档**一行都不能删**(用户 2026-07-22 指令)。"""
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    rid, _ = await repo.create_join_request(1000, 1, 200, max_pending=10)
    await repo.reject_join(rid, 100)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE guild_join_requests SET updated_at = DATE_SUB(NOW(), INTERVAL 200 DAY) "
            "WHERE request_id = %s",
            (rid,),
        )
    out = await repo.sweep_terminal_join_requests(dbguard.Mode.REPORT_ONLY, 90, 500)
    assert out.matched == 1 and out.deleted == 0
    assert await repo.get_request(rid) is not None


async def test_sweep_delete_mode_never_touches_pending(repo, pool) -> None:
    """pending 是工作集,真删档也**永不清** —— 清了玩家的申请就凭空消失。"""
    await repo.create_guild(1, leader_id=100, name="A", max_members=100)
    pending_id, _ = await repo.create_join_request(1000, 1, 200, max_pending=10)
    rejected_id, _ = await repo.create_join_request(1001, 1, 201, max_pending=10)
    await repo.reject_join(rejected_id, 100)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE guild_join_requests SET updated_at = DATE_SUB(NOW(), INTERVAL 200 DAY)"
        )
    out = await repo.sweep_terminal_join_requests(dbguard.Mode.DELETE, 90, 500)
    assert out.deleted == 1
    assert await repo.get_request(pending_id) is not None, "pending 被清了"
    assert await repo.get_request(rejected_id) is None


# ── ★ schema 契约闸(打真库)────────────────────────────────────────────────


async def test_required_schema_passes_on_real_tables(pool) -> None:
    async with pool.acquire() as conn:
        await gschema.validate_required_schema(conn)  # 不抛即通过


async def test_required_schema_rejects_missing_counter_column(pool) -> None:
    """★ 计数列缺了必须拒启 —— 带着跑的话 §9.18 的上限闸静默失效。"""
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("ALTER TABLE guilds DROP COLUMN pending_request_count")
    try:
        async with pool.acquire() as conn:
            with pytest.raises(gschema.SchemaIncompatibleError) as exc:
                await gschema.validate_required_schema(conn)
        assert "pending_request_count" in str(exc.value)
    finally:
        async with pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "ALTER TABLE guilds ADD COLUMN pending_request_count INT NOT NULL DEFAULT 0"
            )
