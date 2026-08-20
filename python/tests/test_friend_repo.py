"""friend 数据层测试 —— **打真实 MySQL**。

★ 本文件存在的首要理由是复现 2026-08-11 那次 1213 死锁:

    16 个并发申请(**互不相同的 requester 与 target**,没有任何共享行)在 RR 隔离下必炸。
    修复是两条:① 写事务显式 READ COMMITTED;② player 守卫提到所有锁定读之前。

    这个死锁**只在 MySQL 上炸**(TiDB 无 gap 锁),所以只跑 TiDB 会一直是绿的 ——
    双后端都要跑才看得见。

没有库就整体 skip(不假装通过)。
"""

from __future__ import annotations

import asyncio

import pytest

from mysqlfixture import MYSQL_DSN as DSN
from mysqlfixture import ensure_database, parse_go_dsn, skip_only_if_mysql_is_down

from pandorapy import dbguard, errcode
from pandorapy.services.friend import repo as frepo


_DDL = [
    """CREATE TABLE IF NOT EXISTS friendships (
        player_id BIGINT UNSIGNED NOT NULL,
        friend_id BIGINT UNSIGNED NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (player_id, friend_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS friend_requests (
        request_id BIGINT UNSIGNED NOT NULL,
        requester_id BIGINT UNSIGNED NOT NULL,
        target_id BIGINT UNSIGNED NOT NULL,
        status INT NOT NULL DEFAULT 1,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (request_id),
        UNIQUE KEY uk_requester_target (requester_id, target_id),
        KEY idx_target_status (target_id, status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS blocks (
        player_id BIGINT UNSIGNED NOT NULL,
        blocked_id BIGINT UNSIGNED NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (player_id, blocked_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS friend_player_guards (
        player_id BIGINT UNSIGNED NOT NULL,
        PRIMARY KEY (player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS friend_pair_guards (
        lo_id BIGINT UNSIGNED NOT NULL,
        hi_id BIGINT UNSIGNED NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (lo_id, hi_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
]

# 既有测试库里的表建于本文件更早的版本,`CREATE TABLE IF NOT EXISTS` 补不上后加的列
# (库不会随文件重建,除非有人手动 DROP)。这里按生产 DDL
# (deploy/mysql-init/06-social-tables.sql)把缺列补齐,否则测试会在一个与线上不同形状的
# 表上跑绿。
_COLUMN_PATCHES = [
    (
        "friend_requests",
        "created_at",
        "ALTER TABLE friend_requests "
        "ADD COLUMN created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER status",
    ),
    (
        # 保留期 sweep 的扫描列(R9 复审 P1)。缺了它 delete_pair_guards_before
        # 会报 Unknown column —— 而线上是有这一列的,测试必须在同形状的表上跑。
        "friend_pair_guards",
        "created_at",
        "ALTER TABLE friend_pair_guards "
        "ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    ),
]

_TABLES = [
    "friendships",
    "friend_requests",
    "blocks",
    "friend_player_guards",
    "friend_pair_guards",
]


@pytest.fixture
async def pool():
    """function 作用域 —— async fixture 的 event loop 绑定,见 test_owner_repo 的解释。"""
    asyncmy = pytest.importorskip("asyncmy")
    pytest.importorskip("cryptography", reason="MySQL 8.x caching_sha2_password 需要它")
    cfg = parse_go_dsn(DSN, default_db="pandora_social")
    # * 只有"MySQL 没起来"才允许跳过 —— 夹具自己的代码(ensure_database / cfg 取键 /
    #   create_pool 形参)抛的异常必须原样冒红。判据、放行集与 CI 文案契约
    #   见 tests/mysqlfixture.py 的 skip_only_if_mysql_is_down。
    with skip_only_if_mysql_is_down(cfg, "friend 数据层测试"):
        # 见 tests/mysqlfixture.py:CI 的 DSN 无库名,库必须自己建。
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
            for table, column, alter in _COLUMN_PATCHES:
                await cur.execute(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND table_name = %s AND column_name = %s",
                    (table, column),
                )
                if await cur.fetchone() is None:
                    await cur.execute(alter)
            for t in _TABLES:
                await cur.execute(f"TRUNCATE TABLE {t}")  # noqa: S608
        yield p
    finally:
        p.close()
        await p.wait_closed()


@pytest.fixture
async def repo(pool):
    return frepo.MySQLFriendRepo(pool)


def _friend_ids(rows) -> list[int]:
    """list_friends 返回 (friend_id, since_ms) 行 —— 断言只关心 id 时用它取列。

    行里带 since_ms 是刻意的(FriendInfo.since_ms 要它),与 Go 的 FriendRow 一致。
    """
    return [r[0] for r in rows]


# ── ★ 死锁复现(本文件的首要理由)──────────────────────────────────────────


async def test_many_distinct_pairs_concurrently_no_deadlock(repo) -> None:
    """★ 16 个并发申请、**互不相同的 requester 与 target** → 必须全部成功。

    这正是 2026-08-11 的死锁形状:这些事务**没有任何共享行**,
    但 RR 下未命中的 FOR UPDATE 锁的是间隙,间隙跨 pair 共享 → 1213。

    若把 repo 的隔离级别改回 RR(或把 player 守卫挪到探针之后),这条必红。
    """
    results = await asyncio.gather(
        *(
            repo.create_request(900_000 + i, 1000 + i, 2000 + i, max_incoming=200)
            for i in range(16)
        ),
        return_exceptions=True,
    )
    failures = [r for r in results if isinstance(r, BaseException)]
    assert not failures, f"{len(failures)}/16 个并发申请失败(死锁?):{failures[:2]}"
    assert all(created for _rid, created in results)


async def test_many_requesters_same_target_no_deadlock(repo) -> None:
    """★ N 个不同 requester 指向**同一 target** —— 死锁日志里那个 supremum 间隙的形状。

    它们共享 target 的 player 守卫,所以会串行;串行是对的,死锁不是。
    """
    results = await asyncio.gather(
        *(
            repo.create_request(910_000 + i, 3000 + i, 4242, max_incoming=200)
            for i in range(16)
        ),
        return_exceptions=True,
    )
    failures = [r for r in results if isinstance(r, BaseException)]
    assert not failures, f"{len(failures)}/16 个失败(死锁?):{failures[:2]}"
    incoming = await repo.list_incoming_requests(4242)
    assert len(incoming) == 16


async def test_accept_and_reapply_same_pair_no_deadlock(repo) -> None:
    """★ 同一 pair 上「接受」与「再次申请」并发 —— 锁序反了就是 ABBA 环。

    create_request / block 的锁序是「守卫行 → 业务行」。accept 若先 FOR UPDATE 请求行
    再取守卫,两条路径就在同一 pair 上反序:
        create 持 pair 守卫、等请求行;accept 持请求行、等 pair 守卫 → 1213。
    _write_tx **没有** 1213/1205 重试兜底,环一成就是一个错误直接抛给玩家,
    所以这里除了"accept 先提交 → 再申请撞到已是好友"这个正常结果之外,
    任何异常都算失败。
    """
    pairs = 10
    for i in range(pairs):
        await repo.create_request(995_000 + i, 11_000 + i, 12_000 + i, max_incoming=200)

    tasks = []
    for i in range(pairs):
        tasks.append(repo.accept_request(995_000 + i, actor_id=12_000 + i, max_friends=200))
        tasks.append(
            repo.create_request(996_000 + i, 11_000 + i, 12_000 + i, max_incoming=200)
        )
    results = await asyncio.gather(*tasks, return_exceptions=True)

    failures = [
        r
        for r in results
        if isinstance(r, BaseException)
        and not (
            isinstance(r, errcode.PandoraError) and r.code == errcode.ErrFriendAlreadyAdded
        )
    ]
    assert not failures, f"并发 accept/再申请 失败(锁序反了→死锁?):{failures[:2]}"


# ── ★ §9.18 三个列表上限 ────────────────────────────────────────────────────


async def test_incoming_request_limit_enforced(repo) -> None:
    """★ 收件箱上限在**事务内**校验,并发也不能突破。"""
    for i in range(3):
        await repo.create_request(920_000 + i, 5000 + i, 6001, max_incoming=3)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.create_request(920_099, 5099, 6001, max_incoming=3)
    assert exc.value.code == errcode.ErrFriendRequestLimit


async def test_incoming_limit_holds_under_concurrency(repo) -> None:
    """★ 并发申请不能突破上限 —— 守卫行 + 守卫锁内的锁定读才是权威。

    若限额只靠事务外预检(或 COUNT 用普通读),这里会超。
    """
    limit = 5
    results = await asyncio.gather(
        *(
            repo.create_request(930_000 + i, 7000 + i, 8001, max_incoming=limit)
            for i in range(20)
        ),
        return_exceptions=True,
    )
    ok = [r for r in results if not isinstance(r, BaseException)]
    rejected = [r for r in results if isinstance(r, errcode.PandoraError)]
    assert len(ok) == limit, f"上限 {limit} 被突破:成功了 {len(ok)} 个"
    assert all(r.code == errcode.ErrFriendRequestLimit for r in rejected)
    assert len(await repo.list_incoming_requests(8001)) == limit


async def test_friend_limit_checked_for_both_sides(repo) -> None:
    """★ 接受申请时**双方**的好友数上限都要校验 —— 只校验一方会让另一方越界。"""
    # 给 9001 塞满好友
    async with repo._pool.acquire() as conn, conn.cursor() as cur:  # noqa: SLF001
        for i in range(3):
            await cur.execute(
                "INSERT INTO friendships (player_id, friend_id) VALUES (%s, %s)",
                (9001, 100 + i),
            )
    # 9002 申请加 9001,9001 接受 —— 应因 **9001** 满而拒
    await repo.create_request(940_001, 9002, 9001, max_incoming=200)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.accept_request(940_001, actor_id=9001, max_friends=3)
    assert exc.value.code == errcode.ErrFriendLimit


async def test_block_limit_enforced(repo) -> None:
    for i in range(3):
        await repo.block(9101, 200 + i, max_blocks=3)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.block(9101, 299, max_blocks=3)
    assert exc.value.code == errcode.ErrFriendBlockLimit


async def test_list_read_hard_limit_caps_result(repo) -> None:
    """★ 读取侧 SQL LIMIT 兜底 —— 防历史脏数据造成无界返回。"""
    async with repo._pool.acquire() as conn, conn.cursor() as cur:  # noqa: SLF001
        for i in range(20):
            await cur.execute(
                "INSERT INTO friendships (player_id, friend_id) VALUES (%s, %s)",
                (9201, 300 + i),
            )
    assert len(await repo.list_friends(9201, limit=5)) == 5
    # limit 超过硬上限时被钳到硬上限
    assert len(await repo.list_friends(9201, limit=99999)) == 20


# ── 幂等与状态机 ────────────────────────────────────────────────────────────


async def test_duplicate_request_is_idempotent_and_takes_no_new_slot(repo) -> None:
    """★ 重复申请同一目标 → 幂等返回既有 pending,**不占新名额**。

    否则一个人反复点"加好友"就能刷爆别人的收件箱。
    """
    rid1, created1 = await repo.create_request(950_001, 9301, 9302, max_incoming=200)
    rid2, created2 = await repo.create_request(950_002, 9301, 9302, max_incoming=200)
    assert created1 and not created2
    assert rid1 == rid2
    assert len(await repo.list_incoming_requests(9302)) == 1


async def test_blocked_pair_cannot_request(repo) -> None:
    """★ 拉黑是双向的 —— 任一方向拉黑都不允许申请。"""
    await repo.block(9401, 9402, max_blocks=200)
    for a, b in ((9401, 9402), (9402, 9401)):
        with pytest.raises(errcode.PandoraError) as exc:
            await repo.create_request(960_001, a, b, max_incoming=200)
        assert exc.value.code == errcode.ErrFriendBlocked


async def test_block_removes_friendship_and_pending(repo) -> None:
    """★ 拉黑必须彻底:删好友关系 + 作废两个方向的 pending。

    留着关系或 pending 会让"拉黑了还能收到他消息 / 还能被他加回来"。
    """
    await repo.create_request(970_001, 9501, 9502, max_incoming=200)
    await repo.accept_request(970_001, actor_id=9502, max_friends=200)
    assert 9502 in _friend_ids(await repo.list_friends(9501))

    await repo.create_request(970_002, 9503, 9501, max_incoming=200)  # 另一条 pending
    await repo.block(9501, 9502, max_blocks=200)

    assert 9502 not in _friend_ids(await repo.list_friends(9501))
    assert 9501 not in _friend_ids(await repo.list_friends(9502))
    # 只作废与被拉黑者相关的那条,别人的 pending 不受影响
    assert [r[1] for r in await repo.list_incoming_requests(9501)] == [9503]


async def test_only_target_can_accept_or_reject(repo) -> None:
    """★ 只有 target 能处理申请 —— requester 自己不能接受自己的申请。

    ★ 返回码必须是 ErrFriendNotFound 而**不是** ErrUnauthorized:
    后者等于告诉调用方「这条申请确实存在」,是信息泄露。
    非 target 应当无从区分"没这条申请"和"有但不是给你的"(与 Go 侧一致)。
    """
    await repo.create_request(980_001, 9601, 9602, max_incoming=200)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.accept_request(980_001, actor_id=9601, max_friends=200)
    assert exc.value.code == errcode.ErrFriendNotFound
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.reject_request(980_001, actor_id=9601)
    assert exc.value.code == errcode.ErrFriendNotFound


async def test_accept_creates_bidirectional_friendship(repo) -> None:
    await repo.create_request(990_001, 9701, 9702, max_incoming=200)
    await repo.accept_request(990_001, actor_id=9702, max_friends=200)
    assert 9702 in _friend_ids(await repo.list_friends(9701))
    assert 9701 in _friend_ids(await repo.list_friends(9702))


async def test_accept_twice_is_rejected(repo) -> None:
    """已处理的申请不能再处理(状态机)。"""
    await repo.create_request(991_001, 9801, 9802, max_incoming=200)
    await repo.accept_request(991_001, actor_id=9802, max_friends=200)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.accept_request(991_001, actor_id=9802, max_friends=200)
    assert exc.value.code == errcode.ErrFriendNotFound


async def test_rejected_request_can_be_resent(repo, pool) -> None:
    """被拒后可以重新申请(复活成 pending),且要重新过限额。

    ★ 复用的是**行**不是 ID:request_id 必须轮换成新 ID、created_at 必须刷新。
    推送是 at-least-once,客户端按 (request_id, reason) 判重 —— 若沿用旧 ID,
    第二次申请的推送会被客户端当成第一次的重投直接丢掉,玩家永远收不到。
    """
    await repo.create_request(992_001, 9901, 9902, max_incoming=200)
    await repo.reject_request(992_001, actor_id=9902)
    assert await repo.list_incoming_requests(9902) == []

    # created_at 拨回远古,这样"是否刷新"不依赖秒级时间差,判定是确定性的。
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE friend_requests SET created_at = '2000-01-01 00:00:00' "
            "WHERE request_id = %s",
            (992_001,),
        )

    rid, created = await repo.create_request(992_002, 9901, 9902, max_incoming=200)
    assert created
    assert rid == 992_002, "再次申请必须换新 request_id,否则新推送被客户端当重投丢弃"
    assert [(r[0], r[1]) for r in await repo.list_incoming_requests(9902)] == [
        (992_002, 9901)
    ]

    async with pool.acquire() as conn, conn.cursor() as cur:
        # 仍是同一行(uk_requester_target 决定只能有一行),只是换了 ID:总行数还是 1。
        await cur.execute("SELECT COUNT(*) FROM friend_requests")
        assert int((await cur.fetchone())[0]) == 1
        await cur.execute(
            "SELECT created_at FROM friend_requests WHERE request_id = %s", (992_002,)
        )
        (created_at,) = await cur.fetchone()
    assert created_at.year > 2000, "created_at 必须刷新,否则列表把重新申请排到陈旧位置"

    # 旧 ID 随之失效 —— 迟到的旧 Accept 应当查无此请求。
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.accept_request(992_001, actor_id=9902, max_friends=200)
    assert exc.value.code == errcode.ErrFriendNotFound


async def test_already_friends_cannot_request(repo) -> None:
    await repo.create_request(993_001, 9911, 9912, max_incoming=200)
    await repo.accept_request(993_001, actor_id=9912, max_friends=200)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.create_request(993_002, 9911, 9912, max_incoming=200)
    assert exc.value.code == errcode.ErrFriendAlreadyAdded


# ── 守卫行 ──────────────────────────────────────────────────────────────────


async def test_pair_guard_is_order_independent(repo, pool) -> None:
    """★ 同一对玩家无论谁发起,必须落到**同一行**守卫,否则守卫形同虚设。"""
    await repo.create_request(994_001, 100, 200, max_incoming=200)
    await repo.block(200, 300, max_blocks=200)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT lo_id, hi_id FROM friend_pair_guards ORDER BY lo_id")
        rows = await cur.fetchall()
    for lo, hi in rows:
        assert lo <= hi, f"守卫行未归一化:({lo},{hi})"


async def test_accept_also_resolves_the_reverse_pending(repo, pool) -> None:
    """★ 接受 A→B 时，反向的 B→A pending 必须一并收敛成 accepted。

    对应 Go friend_repo.go:465-470（R5 复审 P2-8）。留着它的话：那条申请一直挂在
    对方收件箱里，被接受时会对**已经是好友**的两人重复走一遍建边流程 ——
    `INSERT IGNORE` 不报错，于是又推一次"XX 接受了你的好友申请"，客户端看到重复通知。
    """
    await repo.create_request(995_001, 700, 800, max_incoming=200)   # A→B
    await repo.create_request(995_002, 800, 700, max_incoming=200)   # B→A（反向也 pending）
    await repo.accept_request(995_001, 800, max_friends=200)

    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT request_id, status FROM friend_requests "
            "WHERE request_id IN (995001, 995002) ORDER BY request_id"
        )
        rows = {int(r[0]): int(r[1]) for r in await cur.fetchall()}
    assert rows[995_001] == frepo.REQUEST_STATUS_ACCEPTED
    assert rows[995_002] == frepo.REQUEST_STATUS_ACCEPTED, (
        "反向 pending 没被一并终结 —— 它会被再接受一次并重复推送"
    )


async def test_accept_does_not_touch_unrelated_pending(repo, pool) -> None:
    """反向收敛必须**精确**：只动这一对，别把第三方的申请也扫了。"""
    await repo.create_request(996_001, 710, 810, max_incoming=200)   # A→B
    await repo.create_request(996_002, 900, 810, max_incoming=200)   # C→B（无关第三方）
    await repo.accept_request(996_001, 810, max_friends=200)

    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT status FROM friend_requests WHERE request_id = 996002")
        (status,) = await cur.fetchone()
    assert int(status) == frepo.REQUEST_STATUS_PENDING, "把无关第三方的申请一起改掉了"


async def test_isolation_level_is_read_committed() -> None:
    """★ 隔离级别是**正确性要求**,不是调优 —— 改回 RR 会让并发申请死锁。"""
    assert frepo.WRITE_TX_ISOLATION == "READ COMMITTED"


async def test_accept_is_blocked_when_pair_blocked_concurrently(repo, pool) -> None:
    """★ accept 的守卫锁内必须自己再查一次 blocks(Go friend_repo.go 步骤 4)。

    这里直接把 blocks 行塞进去(绕开 Block 的"顺带置 rejected"),模拟
    「Block 已插黑名单、其状态更新还卡在请求行锁上」的交错。少了这道查,
    两笔都提交后就是「既好友又拉黑」—— 而两笔各自都成功,没有任何报错。
    """
    await repo.create_request(753_001, 2301, 2302, max_incoming=200)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "INSERT IGNORE INTO blocks (player_id, blocked_id) VALUES (%s, %s)",
            (2302, 2301),
        )
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.accept_request(753_001, actor_id=2302, max_friends=200)
    assert exc.value.code == errcode.ErrFriendBlocked
    assert not await repo.are_friends(2301, 2302)


# ── 谓词 / 单点读(biz 的 fail-fast 预检建立在它们之上)────────────────────


async def test_are_friends_and_count(repo) -> None:
    await repo.create_request(700_001, 1101, 1102, max_incoming=200)
    await repo.accept_request(700_001, actor_id=1102, max_friends=200)
    # 好友边双向落库 —— 任一方向查都必须为真,否则"我看他是好友、他看我不是"
    assert await repo.are_friends(1101, 1102)
    assert await repo.are_friends(1102, 1101)
    assert not await repo.are_friends(1101, 9999)
    assert await repo.count_friends(1101) == 1
    assert await repo.count_friends(9999) == 0


async def test_is_blocked_is_bidirectional(repo) -> None:
    """★ 任一方向拉黑都算。只查单向会让"被对方拉黑的人还能发申请"。"""
    await repo.block(1201, 1202, max_blocks=200)
    assert await repo.is_blocked(1201, 1202)
    assert await repo.is_blocked(1202, 1201)
    assert not await repo.is_blocked(1201, 1203)


async def test_get_request_returns_row_and_none(repo) -> None:
    await repo.create_request(700_002, 1301, 1302, max_incoming=200)
    assert await repo.get_request(700_002) == (
        700_002,
        1301,
        1302,
        frepo.REQUEST_STATUS_PENDING,
    )
    assert await repo.get_request(1) is None


async def test_remove_friend_deletes_both_directions_and_is_idempotent(repo) -> None:
    """★ 单条 DELETE 覆盖两个方向:只删一边会留下"他的列表里还有我"的半边关系。"""
    await repo.create_request(700_003, 1401, 1402, max_incoming=200)
    await repo.accept_request(700_003, actor_id=1402, max_friends=200)
    await repo.remove_friend(1401, 1402)
    assert await repo.list_friends(1401) == []
    assert await repo.list_friends(1402) == []
    await repo.remove_friend(1401, 1402)  # 幂等:删不到行不报错


async def test_list_rows_carry_timestamps(repo) -> None:
    """三个列表都要带时间戳 —— FriendInfo.since_ms / FriendRequestInfo.created_ms
    直接取它;返回 0 的话客户端"好友多久了 / 什么时候申请的"永远显示 1970。"""
    await repo.create_request(700_004, 1501, 1502, max_incoming=200)
    incoming = await repo.list_incoming_requests(1502)
    assert incoming and incoming[0][0] == 700_004 and incoming[0][2] > 0

    await repo.accept_request(700_004, actor_id=1502, max_friends=200)
    friends = await repo.list_friends(1501)
    assert friends and friends[0][0] == 1502 and friends[0][1] > 0

    await repo.block(1501, 1503, max_blocks=200)
    blocks = await repo.list_blocks(1501)
    assert blocks and blocks[0][0] == 1503 and blocks[0][1] > 0


# ── 推荐 ─────────────────────────────────────────────────────────────────────


async def _make_friends(repo, a: int, b: int, rid: int) -> None:  # noqa: ANN001
    await repo.create_request(rid, a, b, max_incoming=200)
    await repo.accept_request(rid, actor_id=b, max_friends=200)


def _candidate_ids(rows) -> set:  # noqa: ANN001
    return {cid for cid, _mutual in rows}


async def test_recommend_by_mutual_returns_friend_of_friend(repo) -> None:
    """A-B、B-C → 给 A 推荐 C,mutual=1(共同好友只有 B)。"""
    await _make_friends(repo, 1601, 1602, 710_001)
    await _make_friends(repo, 1602, 1603, 710_002)
    recs = await repo.recommend_by_mutual(1601, [1601], 10)
    assert (1603, 1) in recs


async def test_recommend_by_mutual_excludes_everything_it_should(repo) -> None:
    """★ 五条排除少一条,面板就会把已是好友 / 刚拉黑 / 已申请过的人推回来。"""
    await _make_friends(repo, 1701, 1702, 720_001)
    await _make_friends(repo, 1702, 1703, 720_002)  # 候选
    await _make_friends(repo, 1702, 1704, 720_003)  # 候选
    await _make_friends(repo, 1702, 1705, 720_004)  # 候选

    base = _candidate_ids(await repo.recommend_by_mutual(1701, [1701], 10))
    assert {1703, 1704, 1705} <= base
    assert 1701 not in base, "把自己推荐给了自己"
    assert 1702 not in base, "已经是好友的人又被推回来"

    await repo.block(1701, 1703, max_blocks=200)
    assert 1703 not in _candidate_ids(await repo.recommend_by_mutual(1701, [1701], 10))

    await repo.create_request(720_010, 1701, 1704, max_incoming=200)
    assert 1704 not in _candidate_ids(await repo.recommend_by_mutual(1701, [1701], 10))

    assert 1705 not in _candidate_ids(
        await repo.recommend_by_mutual(1701, [1701, 1705], 10)
    )


async def test_recommend_random_stays_in_bounds(repo) -> None:
    """兜底策略:必须能出候选,且不违反排除条件(自己 / 已是好友)。

    ★ pivot 必须落进真实 id 区间 —— 直接取随机 int64 会永远落在最大 id 之后,
    一个候选都扫不出来,而"返回空"看起来只是"没人可推"。
    """
    await _make_friends(repo, 1801, 1802, 730_001)
    await _make_friends(repo, 1803, 1804, 730_002)
    found: set = set()
    for _ in range(20):  # 随机锚点,多试几轮覆盖整个区间
        found |= _candidate_ids(await repo.recommend_random(1801, [1801], 10))
    assert found, "随机兜底一个候选都没出 —— pivot 很可能落在了 id 区间之外"
    assert 1801 not in found and 1802 not in found


async def test_recommend_limit_is_honored(repo) -> None:
    await _make_friends(repo, 1901, 1902, 740_001)
    for i, other in enumerate((1903, 1904, 1905)):
        await _make_friends(repo, 1902, other, 740_010 + i)
    assert len(await repo.recommend_by_mutual(1901, [1901], 2)) == 2


# ── 保留期清理(§9.24)──────────────────────────────────────────────────────


async def _age_request(pool, request_id: int, days: int) -> None:  # noqa: ANN001
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE friend_requests SET updated_at = DATE_SUB(NOW(), INTERVAL %s DAY) "
            "WHERE request_id = %s",
            (days, request_id),
        )


async def test_sweep_report_only_deletes_nothing(repo, pool) -> None:
    """★ 默认档只报告不删(§9.24 用户指令):matched 有值、deleted 恒 0。

    这条如果反了,一次误配就会真的删生产数据 —— 而且删完才发现。
    """
    await repo.create_request(750_001, 2001, 2002, max_incoming=200)
    await repo.reject_request(750_001, actor_id=2002)
    await _age_request(pool, 750_001, 100)
    out = await repo.sweep_terminal_requests_before(dbguard.Mode.REPORT_ONLY, 90, 500)
    assert out.matched == 1
    assert out.deleted == 0
    assert await repo.get_request(750_001) is not None


async def test_sweep_delete_removes_only_aged_terminal_rows(repo, pool) -> None:
    """真删档:只删终态且超期的。**pending 永不清** —— 它是玩家的待办不是垃圾。"""
    await repo.create_request(751_001, 2101, 2102, max_incoming=200)  # 终态 + 超期
    await repo.reject_request(751_001, actor_id=2102)
    await _age_request(pool, 751_001, 100)

    await repo.create_request(751_002, 2103, 2104, max_incoming=200)  # 终态但没超期
    await repo.reject_request(751_002, actor_id=2104)

    await repo.create_request(751_003, 2105, 2106, max_incoming=200)  # pending 且超期
    await _age_request(pool, 751_003, 100)

    out = await repo.sweep_terminal_requests_before(dbguard.Mode.DELETE, 90, 500)
    assert out.deleted == 1
    assert await repo.get_request(751_001) is None
    assert await repo.get_request(751_002) is not None
    assert await repo.get_request(751_003) is not None, "pending 被清了 —— 玩家的待办没了"


async def test_pair_guard_sweep_deletes_aged_rows(repo, pool) -> None:
    """pair 守卫随社交图 O(n²) 累积,必须有清理(R9 复审 P1)。

    守卫行仅是锁载体:删掉后下次 acquire 会重新 INSERT,所以任何时候删都安全。
    """
    await repo.create_request(752_001, 2201, 2202, max_incoming=200)  # 顺带建守卫行
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE friend_pair_guards SET created_at = DATE_SUB(NOW(), INTERVAL 60 DAY)"
        )
    assert await repo.delete_pair_guards_before(30, 500) >= 1
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT COUNT(*) FROM friend_pair_guards")
        (left,) = await cur.fetchone()
    assert int(left) == 0
    # 守卫行被清掉后再次申请必须照常成功(下次 acquire 重建)
    await repo.create_request(752_002, 2203, 2204, max_incoming=200)
