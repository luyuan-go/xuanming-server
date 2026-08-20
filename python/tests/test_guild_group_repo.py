"""临时群名额占用测试 —— 打真实 MySQL。

重点是那个"两把锁 + 对账"模式的三条约束:
  1. ★ 明细 COUNT 是权威,计数行只是串行化点 —— 计数行写脏不能让上限判错
  2. ★ 对账用**绝对值回写**,脏计数必须自愈(不是 ±1 把错误累积下去)
  3. ★ 并发不能突破上限

没有库就整体 skip(不假装通过)。
"""

from __future__ import annotations

import asyncio

import pytest

from mysqlfixture import MYSQL_DSN as DSN
from mysqlfixture import ensure_database, parse_go_dsn, skip_only_if_mysql_is_down

from pandora.group.v1 import group_pb2

from pandorapy import errcode
from pandorapy.services.guild import group_repo as grepo


_DDL = [
    # ★ 列必须与 deploy/mysql-init/11-guild-tables.sql 一致(name / max_members /
    # created_at 都是 NOT NULL)。此前这里少了这三列,于是"测试全绿但真库上 INSERT
    # 缺 name 直接失败"——建表 DDL 与生产漂移的测试,绿得毫无意义。
    """CREATE TABLE IF NOT EXISTS chat_groups (
        group_id BIGINT UNSIGNED NOT NULL,
        name VARCHAR(64) NOT NULL,
        owner_id BIGINT UNSIGNED NOT NULL,
        member_count INT NOT NULL DEFAULT 0,
        max_members INT NOT NULL DEFAULT 50,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (group_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS chat_group_members (
        group_id BIGINT UNSIGNED NOT NULL,
        player_id BIGINT UNSIGNED NOT NULL,
        role INT NOT NULL DEFAULT 0,
        joined_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (group_id, player_id),
        KEY idx_player (player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS player_group_counts (
        player_id BIGINT UNSIGNED NOT NULL,
        group_count INT NOT NULL DEFAULT 0,
        PRIMARY KEY (player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
]

_TABLES = ["chat_groups", "chat_group_members", "player_group_counts"]


@pytest.fixture
async def pool():
    asyncmy = pytest.importorskip("asyncmy")
    pytest.importorskip("cryptography", reason="MySQL 8.x caching_sha2_password 需要它")
    cfg = parse_go_dsn(DSN, default_db="pandora_social")
    # * 只有"MySQL 没起来"才允许跳过 —— 夹具自己的代码(ensure_database / cfg 取键 /
    #   create_pool 形参)抛的异常必须原样冒红。判据、放行集与 CI 文案契约
    #   见 tests/mysqlfixture.py 的 skip_only_if_mysql_is_down。
    with skip_only_if_mysql_is_down(cfg, "临时群测试"):
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
            for t in _TABLES:
                await cur.execute(f"TRUNCATE TABLE {t}")  # noqa: S608
        yield p
    finally:
        p.close()
        await p.wait_closed()


@pytest.fixture
async def repo(pool):
    return grepo.MySQLGroupRepo(pool)


# ── ★ 明细 COUNT 是权威 ─────────────────────────────────────────────────────


async def test_dirty_counter_does_not_break_limit(repo, pool) -> None:
    """★ 计数行被写脏时,上限判定仍必须正确 —— 权威是明细 COUNT。

    这是这个模式存在的理由:计数行只是串行化点,写脏(旧 Pod 残留 / 手工改)
    不能让玩家凭空多出名额或被误判满员。
    """
    await repo.create_group(1, owner_id=100, member_ids=[], max_members=50,
                            max_groups_per_player=3)
    # 手工把计数行改成一个荒谬的大值(模拟旧 Pod 留脏)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE player_group_counts SET group_count = 999 WHERE player_id = 100"
        )
    # 上限 3、实际只在 1 个群 → 必须还能继续建群
    await repo.create_group(2, owner_id=100, member_ids=[], max_members=50,
                            max_groups_per_player=3)
    await repo.create_group(3, owner_id=100, member_ids=[], max_members=50,
                            max_groups_per_player=3)
    assert sorted(await repo.list_my_groups(100)) == [1, 2, 3]


async def test_dirty_counter_self_heals(repo, pool) -> None:
    """★ 脏计数必须**自愈** —— 对账是绝对值回写,不是 ±1。

    写成 `group_count + 1` 的话,一旦脏了就永远脏下去并越滚越大。
    """
    await repo.create_group(1, owner_id=200, member_ids=[], max_members=50,
                            max_groups_per_player=10)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE player_group_counts SET group_count = 777 WHERE player_id = 200"
        )
    assert await repo.player_group_count(200) == 777  # 确认脏了
    # 任意一次名额操作都应把它拉回真实值
    await repo.create_group(2, owner_id=200, member_ids=[], max_members=50,
                            max_groups_per_player=10)
    assert await repo.player_group_count(200) == 2, "脏计数没有自愈"


async def test_counter_matches_details_after_leave(repo) -> None:
    """退群后计数行必须与明细一致(release 是重算不是 -1)。

    注意退的是**非群主**:群 1 的主是 999,300 只是成员。原先这条让 300 退出自己当主的群,
    那在 Go 侧是被 ErrGroupNotOwner 拒的(见下面两条),等于用测试固化了一个不该有的行为。
    """
    await repo.create_group(1, owner_id=999, member_ids=[300], max_members=50,
                            max_groups_per_player=10)
    await repo.create_group(2, owner_id=300, member_ids=[], max_members=50,
                            max_groups_per_player=10)
    assert await repo.player_group_count(300) == 2
    await repo.remove_member(1, 300)
    assert await repo.player_group_count(300) == 1
    assert await repo.list_my_groups(300) == [2]


async def test_owner_cannot_leave_their_own_group(repo) -> None:
    """★ 群主不能直接退群(Go group_repo.go:285-288,三审 P1-9 TOCTOU)。

    放行的话,"退群 / 踢人"与"转让群主"交错会删掉**刚晋升的新群主**,
    群里留下一个指向已不在群的 owner_id —— 之后谁都转让不了、也解散不掉。
    """
    await repo.create_group(1, owner_id=310, member_ids=[311], max_members=50,
                            max_groups_per_player=10)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.remove_member(1, 310)
    assert exc.value.code == errcode.ErrGroupNotOwner
    # 群主仍在群里,计数没被扣
    assert await repo.player_group_count(310) == 1
    # 非群主照常可以退
    await repo.remove_member(1, 311)
    assert await repo.player_group_count(311) == 0


async def test_leaving_a_dissolved_group_is_idempotent_success(repo, pool) -> None:
    """群已解散时退群 / 踢人幂等成功(Go :281 `return nil`),不是 NotFound。

    "解散"与"退群"并发是常见交错,两件事本来就该都成功;抛错会让客户端看到
    一个它无从处理的失败。
    """
    await repo.create_group(1, owner_id=320, member_ids=[321], max_members=50,
                            max_groups_per_player=10)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("DELETE FROM chat_groups WHERE group_id = 1")
        await conn.commit()
    await repo.remove_member(1, 321)   # 不该抛


# ── ★ 上限(§9.18)────────────────────────────────────────────────────────────


async def test_groups_per_player_limit(repo) -> None:
    for gid in range(1, 4):
        await repo.create_group(gid, owner_id=400, member_ids=[], max_members=50,
                                max_groups_per_player=3)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.create_group(99, owner_id=400, member_ids=[], max_members=50,
                                max_groups_per_player=3)
    assert exc.value.code == errcode.ErrGroupJoinLimit


async def test_group_member_limit(repo) -> None:
    await repo.create_group(1, owner_id=500, member_ids=[501, 502], max_members=3,
                            max_groups_per_player=10)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.add_member(1, 503, max_members=3, max_groups_per_player=10)
    assert exc.value.code == errcode.ErrGroupFull


async def test_create_rejects_oversized_roster_upfront(repo) -> None:
    """建群时成员数就超上限 → 直接拒,不留半个群。"""
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.create_group(1, owner_id=600, member_ids=[601, 602, 603],
                                max_members=2, max_groups_per_player=10)
    assert exc.value.code == errcode.ErrGroupFull
    assert await repo.list_my_groups(600) == []


# ── ★ 并发 ──────────────────────────────────────────────────────────────────


async def test_concurrent_joins_do_not_exceed_group_limit(repo) -> None:
    """★ 并发加人不能突破群成员上限 —— 群行锁是每群的串行化点。"""
    await repo.create_group(1, owner_id=700, member_ids=[], max_members=5,
                            max_groups_per_player=100)
    results = await asyncio.gather(
        *(
            repo.add_member(1, 800 + i, max_members=5, max_groups_per_player=100)
            for i in range(20)
        ),
        return_exceptions=True,
    )
    ok = [r for r in results if not isinstance(r, BaseException)]
    full = [r for r in results if isinstance(r, errcode.PandoraError)
            and r.code == errcode.ErrGroupFull]
    assert len(ok) == 4, f"上限 5(含 owner)被突破:成功了 {len(ok)} 个"
    assert len(full) == 16
    assert len(ok) + len(full) == 20, "有非预期错误(死锁没被重试掉?)"


async def test_concurrent_creates_do_not_exceed_player_limit(repo) -> None:
    """★ 同一玩家并发建群不能突破"所在群"上限 —— 计数行是串行化点。"""
    results = await asyncio.gather(
        *(
            repo.create_group(1000 + i, owner_id=900, member_ids=[],
                              max_members=50, max_groups_per_player=4)
            for i in range(16)
        ),
        return_exceptions=True,
    )
    ok = [r for r in results if not isinstance(r, BaseException)]
    limited = [r for r in results if isinstance(r, errcode.PandoraError)
               and r.code == errcode.ErrGroupJoinLimit]
    assert len(ok) == 4, f"上限被突破:成功建了 {len(ok)} 个群"
    assert len(ok) + len(limited) == 16, "有非预期错误"
    assert len(await repo.list_my_groups(900)) == 4


# ── 幂等 ────────────────────────────────────────────────────────────────────


async def test_add_existing_member_is_idempotent(repo) -> None:
    """重复加同一个人 → 幂等 no-op,**且不占额外名额**。"""
    await repo.create_group(1, owner_id=1100, member_ids=[1101], max_members=50,
                            max_groups_per_player=10)
    await repo.add_member(1, 1101, max_members=50, max_groups_per_player=10)
    await repo.add_member(1, 1101, max_members=50, max_groups_per_player=10)
    assert await repo.player_group_count(1101) == 1
    assert await repo.list_my_groups(1101) == [1]


async def test_remove_nonexistent_member_is_noop(repo) -> None:
    await repo.create_group(1, owner_id=1200, member_ids=[], max_members=50,
                            max_groups_per_player=10)
    await repo.remove_member(1, 9999)  # 不在群里
    assert await repo.player_group_count(1200) == 1


async def test_add_to_missing_group_rejected(repo) -> None:
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.add_member(9999, 1300, max_members=50, max_groups_per_player=10)
    assert exc.value.code == errcode.ErrGroupNotFound


# ── 读取侧上限 ──────────────────────────────────────────────────────────────


async def test_list_my_groups_has_sql_limit(repo) -> None:
    """§9.18 读取侧兜底。"""
    for gid in range(1, 8):
        await repo.create_group(gid, owner_id=1400, member_ids=[], max_members=50,
                                max_groups_per_player=100)
    assert len(await repo.list_my_groups(1400, limit=3)) == 3
    assert len(await repo.list_my_groups(1400)) == 7


# ── ★ role 列的值就是 proto 枚举 ────────────────────────────────────────────


async def test_member_role_is_proto_enum_value(repo, pool) -> None:
    """★ 落库的 role 必须是 GroupRole 枚举值,不是随手写的 0/1。

    咬两件事:
      1. 成员不能是 UNSPECIFIED —— 这个值会原样进 GetGroupMembers 应答,
         客户端据它决定"能不能踢人/解散",落成 0 就是无职位。
      2. `ORDER BY role ASC` 下 owner 必须排在 member 前面(owner=1 < member=2)。
         member 写 0 会让列表把普通成员顶到群主上面。
    """
    await repo.create_group(1, owner_id=1500, member_ids=[1501], max_members=50,
                            max_groups_per_player=10)
    await repo.add_member(1, 1502, max_members=50, max_groups_per_player=10)

    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT player_id, role FROM chat_group_members WHERE group_id = 1 "
            "ORDER BY role ASC, player_id ASC"
        )
        rows = [(int(a), int(b)) for a, b in await cur.fetchall()]

    assert rows == [
        (1500, group_pb2.GROUP_ROLE_OWNER),
        (1501, group_pb2.GROUP_ROLE_MEMBER),
        (1502, group_pb2.GROUP_ROLE_MEMBER),
    ], f"role 值或 ORDER BY role ASC 的排序不对:{rows}"
    assert all(r != group_pb2.GROUP_ROLE_UNSPECIFIED for _, r in rows)


# ── ★ 固定锁顺序:计数行 → 明细范围 ─────────────────────────────────────────
#
# 这两条不打库:死锁只在特定交错下才发生,靠并发压测抓是碰运气,而 _run_tx 的
# 1213 重试还会把它掩盖成"偶尔慢一点"。所以直接录下 SQL 序列,断言每条写路径
# 取锁的先后 —— 反向取锁在语句顺序上就是可见的。


class _RecordingCursor:
    """录 SQL 的假游标。按语句文本返回预设行,不连库。"""

    def __init__(self, log: list[str]) -> None:
        self._log = log
        self.rowcount = 1

    async def execute(self, sql: str, args=None) -> None:  # noqa: ANN001, ARG002
        self._log.append(" ".join(sql.split()))

    async def fetchone(self):
        last = self._log[-1]
        if "SELECT owner_id FROM chat_groups" in last:
            return (99999,)  # 群存在,且群主不是被操作的玩家
        if "SELECT member_count FROM chat_groups" in last:
            return (1,)
        if "SELECT group_count FROM player_group_counts" in last:
            return (0,)
        if "SELECT COUNT(*) FROM chat_group_members" in last:
            return (0,)
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc) -> None:  # noqa: ANN002
        return None


class _RecordingConn:
    def __init__(self, log: list[str]) -> None:
        self._log = log

    async def begin(self) -> None:
        return None

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    def cursor(self):
        return _RecordingCursor(self._log)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc) -> None:  # noqa: ANN002
        return None


class _RecordingPool:
    def __init__(self) -> None:
        self.log: list[str] = []

    def acquire(self):
        return _RecordingConn(self.log)


def _first_index(log: list[str], needle: str) -> int:
    for i, sql in enumerate(log):
        if needle in sql:
            return i
    raise AssertionError(f"SQL 序列里没有 {needle!r}:{log}")


async def test_remove_member_locks_count_row_before_detail() -> None:
    """★ 退群必须先锁计数行,再动成员明细。

    DELETE 自己就会锁明细行,所以"先 DELETE 后对账"= 明细 → 计数行,与
    add_member/create_group 的计数行 → 明细正好反向,两条并发路径互等成环。
    """
    pool = _RecordingPool()
    await grepo.MySQLGroupRepo(pool).remove_member(1, 1600)

    lock_count_row = _first_index(pool.log, "SELECT group_count FROM player_group_counts")
    touch_detail = _first_index(pool.log, "DELETE FROM chat_group_members")
    assert lock_count_row < touch_detail, (
        f"remove_member 反向取锁(明细 → 计数行),与 add_member 成环:{pool.log}"
    )


async def test_add_member_locks_count_row_before_detail() -> None:
    """add_member 侧的参照系:它必须一直是计数行 → 明细,上一条才有意义。"""
    pool = _RecordingPool()
    await grepo.MySQLGroupRepo(pool).add_member(
        1, 1601, max_members=50, max_groups_per_player=10
    )

    lock_count_row = _first_index(pool.log, "SELECT group_count FROM player_group_counts")
    touch_detail = _first_index(pool.log, "INSERT INTO chat_group_members")
    assert lock_count_row < touch_detail, f"add_member 取锁顺序变了:{pool.log}"
