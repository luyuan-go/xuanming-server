"""battle_result 数据层 —— **打真实 MySQL**,不用 mock。

为什么必须打真库:被测的就是**事务本身** —— 一个事务里插 battles/stats/三张出箱、
撞 1062 后的幂等重放分支、`SELECT ... FOR UPDATE` 的进度水位收口、多表成组批删。
用 fake 测等于把被测对象换成了"我对事务语义的想象"。

★★ 本文件最要紧的一组是 `_settle_progress_stream_tx`(见下方"进度收口"一节)。
   它决定**掉落会不会双发**,而这个 bug 单跑 Python 永远复现不了(Python 侧没有
   实时进度通道,水位恒 0)—— 只有先手工造出 last_applied_seq>0 的水位行才能验到。

没有库就整体 skip(**不假装通过**):
    docker run -d --name pandora-mysql-verify -p 13306:3306 \
      -e MYSQL_ROOT_PASSWORD=pandora_dev_root -e MYSQL_DATABASE=pandora_battle \
      mysql:8.4 --sql-mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"

环境变量 PANDORA_TEST_MYSQL_DSN 可覆盖(与 Go 侧 CI 的门控变量同名)。
"""

from __future__ import annotations

import asyncio
import time

import pytest
from pandora.battle.v1 import battle_pb2
from pandora.match.v1 import match_pb2

from mysqlfixture import MYSQL_DSN as DSN
from mysqlfixture import ensure_database, parse_go_dsn, skip_only_if_mysql_is_down

from pandorapy import dbguard, errcode
from pandorapy.services.battle_result import repo as brepo


# 与 deploy/mysql-init/03-battle-tables.sql + 05-battle-outbox.sql 同构。
# ★ 列定义(尤其 battle_progress_stream.stopped_at_ms 的 BIGINT NOT NULL DEFAULT 0)
#   必须逐字同构:validate_progress_schema 会**核对列级契约**,抄松了会让那道闸测不到。
_DDL = [
    """CREATE TABLE IF NOT EXISTS battles (
        match_id BIGINT UNSIGNED NOT NULL,
        started_at_ms BIGINT NOT NULL DEFAULT 0,
        ended_at_ms BIGINT NOT NULL DEFAULT 0,
        winner_team TINYINT NOT NULL DEFAULT 2,
        outcome TINYINT UNSIGNED NOT NULL DEFAULT 1,
        ds_pod_name VARCHAR(128) NOT NULL DEFAULT '',
        game_mode VARCHAR(32) NOT NULL DEFAULT '',
        map_id INT UNSIGNED NOT NULL DEFAULT 0,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (match_id),
        KEY idx_ended (ended_at_ms),
        KEY idx_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS battle_player_stats (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        match_id BIGINT UNSIGNED NOT NULL,
        player_id BIGINT UNSIGNED NOT NULL,
        hero_id INT UNSIGNED NOT NULL DEFAULT 0,
        team TINYINT NOT NULL DEFAULT 0,
        kills INT NOT NULL DEFAULT 0,
        deaths INT NOT NULL DEFAULT 0,
        assists INT NOT NULL DEFAULT 0,
        damage_dealt BIGINT NOT NULL DEFAULT 0,
        damage_taken BIGINT NOT NULL DEFAULT 0,
        healing BIGINT NOT NULL DEFAULT 0,
        gold BIGINT NOT NULL DEFAULT 0,
        mmr_delta INT NOT NULL DEFAULT 0,
        PRIMARY KEY (id),
        UNIQUE KEY uk_match_player (match_id, player_id),
        KEY idx_player_match (player_id, match_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS player_update_outbox (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        match_id BIGINT UNSIGNED NOT NULL,
        player_id BIGINT UNSIGNED NOT NULL,
        payload VARBINARY(512) NOT NULL,
        created_at_ms BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (id),
        UNIQUE KEY uk_match_player (match_id, player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS battle_drop_outbox (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        match_id BIGINT UNSIGNED NOT NULL,
        player_id BIGINT UNSIGNED NOT NULL,
        item_config_ids VARCHAR(512) NOT NULL,
        stack_item_config_ids VARCHAR(512) NOT NULL DEFAULT '',
        instance_item_config_ids VARCHAR(512) NOT NULL DEFAULT '',
        -- currency_amount 必须 UNSIGNED(同 000011 迁移):有符号列会静默收下负数发放。
        currency_amount BIGINT UNSIGNED NOT NULL DEFAULT 0,
        created_at_ms BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (id),
        UNIQUE KEY uk_match_player (match_id, player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS terminal_release_outbox (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        match_id BIGINT UNSIGNED NOT NULL,
        allocation_id CHAR(36) NOT NULL,
        ds_pod_name VARCHAR(253) NOT NULL,
        gameserver_uid VARCHAR(64) NOT NULL,
        instance_epoch INT UNSIGNED NOT NULL,
        auth_gen BIGINT UNSIGNED NOT NULL,
        auth_jti VARCHAR(256) NOT NULL,
        auth_exp_ms BIGINT NOT NULL,
        auth_kid VARCHAR(128) NOT NULL,
        auth_token_sha256 CHAR(64) NOT NULL,
        auth_writer_epoch INT UNSIGNED NOT NULL,
        authorized_at_ms BIGINT NOT NULL,
        release_after_ms BIGINT NOT NULL,
        released_at_ms BIGINT NOT NULL DEFAULT 0,
        created_at_ms BIGINT NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uk_terminal_release_match (match_id),
        KEY idx_terminal_release_due (release_after_ms, id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS match_release_outbox (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        match_id BIGINT UNSIGNED NOT NULL,
        payload VARBINARY(1024) NOT NULL,
        next_attempt_at_ms BIGINT NOT NULL DEFAULT 0,
        attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
        created_at_ms BIGINT NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uk_match_release_match (match_id),
        KEY idx_match_release_due (next_attempt_at_ms, id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS battle_progress_stream (
        match_id BIGINT UNSIGNED NOT NULL,
        last_applied_seq BIGINT UNSIGNED NOT NULL DEFAULT 0,
        total_exp BIGINT UNSIGNED NOT NULL DEFAULT 0,
        total_items INT UNSIGNED NOT NULL DEFAULT 0,
        final_seq BIGINT UNSIGNED NOT NULL DEFAULT 0,
        settled_at_ms BIGINT NOT NULL DEFAULT 0,
        stopped_at_ms BIGINT NOT NULL DEFAULT 0,
        updated_at_ms BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (match_id),
        KEY idx_settled (settled_at_ms)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS battle_progress_outbox (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        match_id BIGINT UNSIGNED NOT NULL,
        seq BIGINT UNSIGNED NOT NULL,
        player_id BIGINT UNSIGNED NOT NULL,
        kind TINYINT UNSIGNED NOT NULL,
        exp_delta BIGINT UNSIGNED NOT NULL DEFAULT 0,
        item_config_ids VARCHAR(512) NOT NULL DEFAULT '',
        item_count INT UNSIGNED NOT NULL DEFAULT 0,
        next_attempt_at_ms BIGINT NOT NULL DEFAULT 0,
        attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
        created_at_ms BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (id),
        UNIQUE KEY uk_match_seq_player_kind (match_id, seq, player_id, kind)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS battle_progress_player (
        match_id BIGINT UNSIGNED NOT NULL,
        player_id BIGINT UNSIGNED NOT NULL,
        total_exp BIGINT UNSIGNED NOT NULL DEFAULT 0,
        total_items INT UNSIGNED NOT NULL DEFAULT 0,
        total_kills INT UNSIGNED NOT NULL DEFAULT 0,
        updated_at_ms BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (match_id, player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS battle_progress_item_balance (
        match_id BIGINT UNSIGNED NOT NULL,
        player_id BIGINT UNSIGNED NOT NULL,
        item_config_id INT UNSIGNED NOT NULL,
        picked_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
        spent_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
        updated_at_ms BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (match_id, player_id, item_config_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS battle_progress_action (
        match_id BIGINT UNSIGNED NOT NULL,
        seq BIGINT UNSIGNED NOT NULL,
        player_id BIGINT UNSIGNED NOT NULL,
        kind TINYINT UNSIGNED NOT NULL,
        item_config_id INT UNSIGNED NOT NULL,
        count INT UNSIGNED NOT NULL,
        status TINYINT UNSIGNED NOT NULL DEFAULT 0,
        result_code INT NOT NULL DEFAULT 0,
        created_at_ms BIGINT NOT NULL DEFAULT 0,
        updated_at_ms BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (match_id, seq, player_id, kind)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS battle_mission_outbox (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        match_id BIGINT UNSIGNED NOT NULL,
        seq BIGINT UNSIGNED NOT NULL,
        player_id BIGINT UNSIGNED NOT NULL,
        category INT UNSIGNED NOT NULL,
        slot_value INT UNSIGNED NOT NULL,
        amount INT UNSIGNED NOT NULL,
        pending_action TINYINT UNSIGNED NOT NULL DEFAULT 0,
        next_attempt_at_ms BIGINT NOT NULL DEFAULT 0,
        attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
        created_at_ms BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (id),
        UNIQUE KEY uk_match_seq_player (match_id, seq, player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
]

_TABLES = (
    "battles",
    "battle_player_stats",
    "player_update_outbox",
    "battle_drop_outbox",
    "terminal_release_outbox",
    "match_release_outbox",
    "battle_progress_stream",
    "battle_progress_outbox",
    "battle_progress_player",
    "battle_progress_item_balance",
    "battle_progress_action",
    "battle_mission_outbox",
)


@pytest.fixture
async def pool():
    """每个用例一个连接池 —— **必须 function 作用域**。

    pytest-asyncio 默认每个用例一个新 event loop,而 asyncmy 的池把内部 Task 绑在
    创建时的 loop 上;module 作用域会让第二个用例起全部报
    `got Future attached to a different loop`(见 test_owner_repo.py 的同款说明)。
    """
    asyncmy = pytest.importorskip("asyncmy")
    pytest.importorskip(
        "cryptography",
        reason="MySQL 8.x 默认 caching_sha2_password 认证,Python 驱动需要 cryptography",
    )
    cfg = parse_go_dsn(DSN, default_db="pandora_battle")
    # * 只有"MySQL 没起来"才允许跳过 —— 夹具自己的代码(ensure_database / cfg 取键 /
    #   create_pool 形参)抛的异常必须原样冒红。判据、放行集与 CI 文案契约
    #   见 tests/mysqlfixture.py 的 skip_only_if_mysql_is_down。
    with skip_only_if_mysql_is_down(cfg, "battle_result 数据层测试"):
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
    return brepo.MySQLBattleRepo(pool)


def _result(match_id=1001, outcome=battle_pb2.BATTLE_OUTCOME_NORMAL, ended=2000, players=((1, 0), (2, 1))):
    return battle_pb2.BattleResult(
        match_id=match_id,
        started_at_ms=1000,
        ended_at_ms=ended,
        winner_team=0,
        outcome=outcome,
        ds_pod_name="battle-pod-1",
        game_mode="pvp",
        map_id=7,
        stats=[
            battle_pb2.PlayerStats(player_id=pid, team=team, kills=3, mmr_delta=16 if team == 0 else -16)
            for pid, team in players
        ],
    )


def _outbox(*player_ids) -> list[brepo.OutboxRecord]:
    return [brepo.OutboxRecord(player_id=pid, payload=b"pb-" + str(pid).encode()) for pid in player_ids]


def _drop(player_id=1, currency_amount=0) -> brepo.DropOutboxRecord:
    return brepo.DropOutboxRecord(
        player_id=player_id,
        item_config_ids=[10001, 10002],
        stack_item_config_ids=[10002],
        instance_item_config_ids=[10001],
        currency_amount=currency_amount,
    )


async def _count(pool, table: str, where: str = "1=1") -> int:
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}")  # noqa: S608
        return int((await cur.fetchone())[0])


async def _row(pool, sql: str, params: tuple = ()):
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(sql, params)
        return await cur.fetchone()


# ── 启动期 schema 探测 ──────────────────────────────────────────────────────


async def test_schema_probes_pass_on_correct_schema(repo) -> None:
    await repo.validate_recovery_outbox_schema()
    await repo.validate_progress_schema()
    await repo.validate_mission_outbox_schema()


async def test_progress_schema_probe_catches_column_contract_drift(pool, repo) -> None:
    """★ 只探测"列存在"是不够的:停流 fencing 依赖 "缺省 0 = 未停流" 的语义。

    手工把 stopped_at_ms 改成可空后,列还在、SELECT 还能跑,而 NULL 值会让
    "未停流"的判定失效 —— 这道闸必须抓到它。
    """
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "ALTER TABLE battle_progress_stream MODIFY COLUMN stopped_at_ms BIGINT NULL DEFAULT NULL"
        )
    try:
        with pytest.raises(errcode.PandoraError, match="stopped_at_ms contract violated"):
            await repo.validate_progress_schema()
    finally:
        async with pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "ALTER TABLE battle_progress_stream MODIFY COLUMN stopped_at_ms "
                "BIGINT NOT NULL DEFAULT 0"
            )


async def test_recovery_schema_probe_fails_when_table_missing(pool, repo) -> None:
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("DROP TABLE match_release_outbox")
    try:
        with pytest.raises(errcode.PandoraError, match="recovery outbox schema invalid"):
            await repo.validate_recovery_outbox_schema()
    finally:
        async with pool.acquire() as conn, conn.cursor() as cur:
            for ddl in _DDL:
                await cur.execute(ddl)


# ── 结算落库(一个事务)──────────────────────────────────────────────────────


async def test_save_result_writes_everything_atomically(pool, repo) -> None:
    already, info = await repo.save_result(_result(), _outbox(1, 2), [_drop(1)], None, 0)
    assert already is False
    assert info == brepo.ProgressSettleInfo()

    assert await _count(pool, "battles") == 1
    assert await _count(pool, "battle_player_stats") == 2
    assert await _count(pool, "player_update_outbox") == 2
    assert await _count(pool, "battle_drop_outbox") == 1
    assert await _count(pool, "match_release_outbox") == 1
    # 本场未走实时通道 → 插一行打了终局标记的水位行,封死迟到进度。
    row = await _row(
        pool,
        "SELECT last_applied_seq, final_seq, settled_at_ms FROM battle_progress_stream "
        "WHERE match_id=%s",
        (1001,),
    )
    assert row[0] == 0 and row[1] == 0 and row[2] > 0


async def test_save_result_rolls_back_on_failure(pool, repo) -> None:
    """★ 事务性:出箱 payload 超列宽(VARBINARY(512))必须让**整场**回滚。

    半落库的后果:battles 有行但出箱没有 → 玩家有战绩没段位,而幂等键让重试直接
    命中 already,永远补不回来。
    """
    too_long = [brepo.OutboxRecord(player_id=1, payload=b"x" * 600)]
    with pytest.raises(errcode.PandoraError):
        await repo.save_result(_result(), too_long, None, None, 0)
    assert await _count(pool, "battles") == 0
    assert await _count(pool, "battle_player_stats") == 0
    assert await _count(pool, "match_release_outbox") == 0


async def test_save_result_idempotent_replay(pool, repo) -> None:
    """撞 1062 → already=True,且**不重复写** stats / 出箱。"""
    await repo.save_result(_result(), _outbox(1, 2), [_drop(1)], None, 0)
    already, info = await repo.save_result(_result(), _outbox(1, 2), [_drop(1)], None, 0)
    assert already is True
    # 重放返回**零值** info:重放不产生掉落 / 对账副作用。
    assert info == brepo.ProgressSettleInfo()
    assert await _count(pool, "battle_player_stats") == 2
    assert await _count(pool, "player_update_outbox") == 2
    assert await _count(pool, "battle_drop_outbox") == 1


async def test_idempotent_replay_restores_match_release_outbox(pool, repo) -> None:
    """★ 重放要**恢复**已被删掉的 recovery 出箱行,名单从**已落库 stats** 回读。

    绝不信本次重复 payload —— 重放的请求体可能来自僵尸 DS。
    """
    await repo.save_result(_result(), _outbox(1, 2), None, None, 0)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("DELETE FROM match_release_outbox")
    # 重放时给一份**空** stats 的 result:名单必须从库里回读,而不是从请求体拿。
    replay = _result()
    del replay.stats[:]
    replay.stats.append(battle_pb2.PlayerStats(player_id=1, team=0))
    already, _ = await repo.save_result(replay, [], None, None, 0)
    assert already is True
    rows = await repo.fetch_match_release_outbox(10, int(time.time() * 1000) + 1000)
    assert len(rows) == 1
    assert sorted(rows[0].player_ids) == [1, 2]  # ← 来自 battle_player_stats,不是 replay.stats


async def test_terminal_release_row_written_with_result(pool, repo) -> None:
    rec = brepo.TerminalReleaseRecord(
        match_id=1001,
        allocation_id="a" * 36,
        ds_pod_name="battle-pod-1",
        gameserver_uid="uid-1",
        instance_epoch=3,
        auth_gen=5,
        auth_jti="jti-1",
        auth_exp_ms=9_999_999_999_999,
        auth_kid="kid-1",
        auth_token_sha256="b" * 64,
        auth_writer_epoch=2,
        authorized_at_ms=100,
        release_after_ms=200,
        player_ids=[1, 2],
    )
    await repo.save_result(_result(), _outbox(1, 2), None, rec, 0)
    row = await _row(
        pool,
        "SELECT allocation_id, ds_pod_name, release_after_ms, released_at_ms "
        "FROM terminal_release_outbox WHERE match_id=%s",
        (1001,),
    )
    assert row[0] == "a" * 36
    assert row[1] == "battle-pod-1"
    assert row[2] == 200
    assert row[3] == 0  # 只允许 phase1 worker CAS 推进


async def test_match_release_roster_prefers_canonical_over_stats(pool, repo) -> None:
    """★ recovery 名单优先用 canonical roster,而不是 DS 上报的 stats。

    失陷 DS 少报一个玩家时,用 stats 会让那个玩家的撮合状态永不释放(回 Hub 撞 4002)。
    """
    rec = brepo.TerminalReleaseRecord(match_id=1001, player_ids=[1, 2, 3])
    await repo.save_result(_result(players=((1, 0), (2, 1))), _outbox(1, 2), None, rec, 0)
    rows = await repo.fetch_match_release_outbox(10, int(time.time() * 1000) + 1000)
    assert sorted(rows[0].player_ids) == [1, 2, 3]


# ── ★★ 进度收口(决定掉落会不会双发)──────────────────────────────────────


async def _seed_stream(pool, match_id: int, last_seq: int, settled_ms: int = 0) -> None:
    """手工造一行水位 —— 模拟"这场对局的进度批次落在别的(Go)副本上"。"""
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO battle_progress_stream "
            "(match_id, last_applied_seq, settled_at_ms, updated_at_ms) VALUES (%s,%s,%s,%s)",
            (match_id, last_seq, settled_ms, 1),
        )


async def test_settlement_marks_terminal_on_fresh_stream(pool, repo) -> None:
    """未走实时通道的对局也要插一行终局标记 —— 封死僵尸 DS 的迟到进度。"""
    await repo.save_result(_result(), _outbox(1, 2), None, None, 0)
    row = await _row(
        pool, "SELECT settled_at_ms, final_seq FROM battle_progress_stream WHERE match_id=%s", (1001,)
    )
    assert row[0] > 0
    assert row[1] == 0


async def test_settlement_records_final_seq(pool, repo) -> None:
    await repo.save_result(_result(), _outbox(1, 2), None, None, 42)
    row = await _row(pool, "SELECT final_seq FROM battle_progress_stream WHERE match_id=%s", (1001,))
    assert row[0] == 42


async def test_watermark_gt_zero_suppresses_settlement_drops(pool, repo) -> None:
    """★★ 本文件最重要的一条:**水位 >0 → 结算路径的掉落一行都不写**。

    这条漏了会让一场走过实时通道的对局被"实时通道发一遍 + 结算路径再发一遍" ——
    而且单跑 Python 永远复现不了(Python 侧没有实时通道,水位恒 0),
    只有像这里一样先手工造出水位才验得到。
    """
    await _seed_stream(pool, 1001, last_seq=9)
    already, info = await repo.save_result(_result(), _outbox(1, 2), [_drop(1)], None, 9)
    assert already is False
    assert info.stream_existed is True
    assert info.last_applied_seq == 9
    assert info.drops_suppressed is True
    assert await _count(pool, "battle_drop_outbox") == 0  # ← 一行都没写
    row = await _row(
        pool, "SELECT settled_at_ms, final_seq FROM battle_progress_stream WHERE match_id=%s", (1001,)
    )
    assert row[0] > 0 and row[1] == 9


async def test_watermark_zero_does_not_suppress_drops(pool, repo) -> None:
    """水位行存在但 last_applied_seq==0(开了流一条都没入账)→ 掉落仍走结算路径。

    判据是**水位值**而不是"行存不存在":按行存在判会让这类对局的掉落两边都不发。
    """
    await _seed_stream(pool, 1001, last_seq=0)
    _, info = await repo.save_result(_result(), _outbox(1, 2), [_drop(1)], None, 0)
    assert info.stream_existed is True
    assert info.drops_suppressed is False
    assert await _count(pool, "battle_drop_outbox") == 1


async def test_already_settled_stream_is_not_overwritten(pool, repo) -> None:
    """★ 幂等:已打终局标记的行原样返回,**不改写首次结算的审计值**。

    覆盖了的话 final_seq / settled_at_ms 会变成最后一次重放的时刻,
    「这场是什么时候结算的、对账水位是多少」这两个审计事实就丢了。
    """
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO battle_progress_stream "
            "(match_id, last_applied_seq, final_seq, settled_at_ms, updated_at_ms) "
            "VALUES (%s,%s,%s,%s,%s)",
            (1001, 9, 9, 111_111, 111_111),
        )
    _, info = await repo.save_result(_result(), _outbox(1, 2), None, None, 77)
    assert info.last_applied_seq == 9
    row = await _row(
        pool, "SELECT settled_at_ms, final_seq FROM battle_progress_stream WHERE match_id=%s", (1001,)
    )
    assert row[0] == 111_111
    assert row[1] == 9  # 不被 77 覆盖


async def test_idempotent_replay_also_settles_unmarked_stream(pool, repo) -> None:
    """★ 审计 P0:幂等重放分支**也要**收口水位。

    首笔落库若由不含进度逻辑的旧副本完成,水位行会一直没打终局标记 ——
    僵尸 / 分区恢复 DS 的迟到进度仍会被接受并发放。
    """
    await repo.save_result(_result(), _outbox(1, 2), None, None, 0)
    # 手工把终局标记抹掉,模拟"旧副本落的库"。
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE battle_progress_stream SET settled_at_ms=0, last_applied_seq=5 "
            "WHERE match_id=%s",
            (1001,),
        )
    already, _ = await repo.save_result(_result(), _outbox(1, 2), None, None, 5)
    assert already is True
    row = await _row(
        pool, "SELECT settled_at_ms, final_seq FROM battle_progress_stream WHERE match_id=%s", (1001,)
    )
    assert row[0] > 0  # ← 重放把标记补上了
    assert row[1] == 5


async def test_abandoned_also_settles_stream(pool, repo) -> None:
    """ABANDONED 同样收口:崩溃后分区恢复的 DS 不得再往这一局里刷进度。"""
    await repo.save_result(
        _result(outcome=battle_pb2.BATTLE_OUTCOME_ABANDONED), _outbox(1, 2), None, None, 0
    )
    row = await _row(pool, "SELECT settled_at_ms FROM battle_progress_stream WHERE match_id=%s", (1001,))
    assert row[0] > 0


# ── 查询 ─────────────────────────────────────────────────────────────────────


async def test_get_result_round_trip(repo) -> None:
    await repo.save_result(_result(), _outbox(1, 2), None, None, 0)
    got = await repo.get_result(1001)
    assert got is not None
    assert got.match_id == 1001
    assert got.game_mode == "pvp"
    assert got.map_id == 7
    assert got.ds_pod_name == "battle-pod-1"
    assert got.outcome == battle_pb2.BATTLE_OUTCOME_NORMAL
    assert [(s.player_id, s.team, s.kills, s.mmr_delta) for s in got.stats] == [
        (1, 0, 3, 16),
        (2, 1, 3, -16),
    ]


async def test_get_result_missing_returns_none(repo) -> None:
    """"查不到"必须是 None,不能返回空 result —— 否则调用方分不清"没结算"和"零战绩"。"""
    assert await repo.get_result(404) is None


async def test_list_player_history_desc_and_cursor(repo) -> None:
    for i, ended in enumerate((1000, 2000, 3000), start=1):
        await repo.save_result(
            _result(match_id=2000 + i, ended=ended, players=((7, 0), (8, 1))),
            _outbox(7, 8),
            None,
            None,
            0,
        )
    got = await repo.list_player_history(7, 10, 0)
    assert [r.ended_at_ms for r in got] == [3000, 2000, 1000]
    # 游标:严格小于 before_ms。
    page = await repo.list_player_history(7, 10, 3000)
    assert [r.ended_at_ms for r in page] == [2000, 1000]
    # limit 生效。
    assert len(await repo.list_player_history(7, 1, 0)) == 1
    # 不是本人的对局不出现。
    assert await repo.list_player_history(999, 10, 0) == []


async def test_list_player_history_limit_defaults_to_20(repo) -> None:
    """limit<=0 时兜 20(与 Go 同):兜 0 会让查询恒空,兜"无上限"会拉爆一次响应。"""
    for i in range(25):
        await repo.save_result(
            _result(match_id=3000 + i, ended=1000 + i, players=((9, 0),)), _outbox(9), None, None, 0
        )
    assert len(await repo.list_player_history(9, 0, 0)) == 20


# ── 出箱 ─────────────────────────────────────────────────────────────────────


async def test_outbox_fifo_and_delete(repo) -> None:
    await repo.save_result(_result(), _outbox(1, 2), None, None, 0)
    rows = await repo.fetch_outbox(10)
    assert [r.player_id for r in rows] == [1, 2]  # 按 id 升序 = FIFO 保序
    assert rows[0].payload == b"pb-1"
    await repo.delete_outbox(rows[0].id)
    assert [r.player_id for r in await repo.fetch_outbox(10)] == [2]


async def test_drop_outbox_csv_round_trip(repo) -> None:
    """三份 CSV 必须原样往返 —— 路由是**首次入箱时冻结**的,重试不得重算。"""
    await repo.save_result(_result(), _outbox(1, 2), [_drop(1)], None, 0)
    rows = await repo.fetch_drop_outbox(10)
    assert len(rows) == 1
    assert rows[0].match_id == 1001
    assert rows[0].item_config_ids == [10001, 10002]
    assert rows[0].stack_item_config_ids == [10002]
    assert rows[0].instance_item_config_ids == [10001]
    await repo.delete_drop_outbox(rows[0].id)
    assert await repo.fetch_drop_outbox(10) == []


async def test_drop_outbox_currency_round_trips(repo) -> None:
    """★ currency_amount 必须**写得进也读得回**。

    漏写列(走默认 0)与漏读列(恒 0)都是同一种失败:金币静默不发、零报错、
    战绩表上却明明白白记着这局赚了多少。
    """
    await repo.save_result(_result(), _outbox(1, 2), [_drop(1, currency_amount=777)], None, 0)
    rows = await repo.fetch_drop_outbox(10)
    assert [r.currency_amount for r in rows] == [777]


async def test_gold_only_row_is_persisted(pool, repo) -> None:
    """★ 没掉落但有金币的行必须入库。

    旧判据 `没有 item_config_ids 就 continue` 会把它整条丢掉 —— 而"这局没爆装备
    只赚了钱"是最常见的一局。
    """
    gold_only = brepo.DropOutboxRecord(player_id=1, item_config_ids=[], currency_amount=88)
    await repo.save_result(_result(), _outbox(1, 2), [gold_only], None, 0)
    assert await _count(pool, "battle_drop_outbox") == 1
    assert [r.currency_amount for r in await repo.fetch_drop_outbox(10)] == [88]


async def test_suppressed_drops_still_persist_currency(pool, repo) -> None:
    """★★ 水位 >0 抑制的是**掉落**,不是金币。

    实时进度通道逐事件发掉落(所以结算路径再写一遍就是双发),但它**不发金币** ——
    金币只有结算这一条路径。整行跳过等于这局的金币蒸发,且没有任何报错。
    落库的行必须是"三份路由全空 + 金币"的纯货币行,不能把掉落也捎带写进去。
    """
    await _seed_stream(pool, 1001, last_seq=9)
    _, info = await repo.save_result(
        _result(), _outbox(1, 2), [_drop(1, currency_amount=66)], None, 9
    )
    assert info.drops_suppressed is True
    rows = await repo.fetch_drop_outbox(10)
    assert len(rows) == 1
    assert rows[0].currency_amount == 66
    assert rows[0].item_config_ids == []
    assert rows[0].stack_item_config_ids == []
    assert rows[0].instance_item_config_ids == []


async def test_drop_outbox_skips_empty_rows(pool, repo) -> None:
    """既无掉落也无金币的行不入库:空行会让发布器每轮取出来又发现无路由。"""
    empty = brepo.DropOutboxRecord(player_id=1, item_config_ids=[])
    await repo.save_result(_result(), _outbox(1, 2), [empty], None, 0)
    assert await _count(pool, "battle_drop_outbox") == 0


@pytest.mark.parametrize(
    ("csv", "want"),
    [("", []), ("10001", [10001]), ("10001,10002", [10001, 10002]),
     ("10001, ,0,x,10002", [10001, 10002])],
)
def test_decode_config_ids_is_defensive(csv: str, want: list[int]) -> None:
    """坏 CSV 只跳过坏段,**不抛异常** —— 一行坏数据不该让整批出箱取不出来。"""
    assert brepo.decode_config_ids(csv) == want


def test_encode_config_ids() -> None:
    assert brepo.encode_config_ids([10001, 10002]) == "10001,10002"
    assert brepo.encode_config_ids([]) == ""


# ── 撮合释放出箱 ─────────────────────────────────────────────────────────────


async def test_match_release_payload_decodes(repo) -> None:
    await repo.save_result(_result(), _outbox(1, 2), None, None, 0)
    rows = await repo.fetch_match_release_outbox(10, int(time.time() * 1000) + 1000)
    assert len(rows) == 1
    assert rows[0].match_id == 1001
    assert rows[0].operation_id  # uuid4,非空
    assert sorted(rows[0].player_ids) == [1, 2]


async def test_match_release_defer_bumps_attempt_and_hides_row(repo) -> None:
    await repo.save_result(_result(), _outbox(1, 2), None, None, 0)
    row = (await repo.fetch_match_release_outbox(10, int(time.time() * 1000) + 1000))[0]
    future_ms = int(time.time() * 1000) + 60_000
    await repo.defer_match_release_outbox(row.id, future_ms)
    # 退避后本轮不该再被取到(否则等于没退避,每轮都打一遍 matchmaker)。
    assert await repo.fetch_match_release_outbox(10, int(time.time() * 1000)) == []
    again = await repo.fetch_match_release_outbox(10, future_ms + 1)
    assert again[0].attempt_count == 1


async def test_match_release_delete(repo) -> None:
    await repo.save_result(_result(), _outbox(1, 2), None, None, 0)
    row = (await repo.fetch_match_release_outbox(10, int(time.time() * 1000) + 1000))[0]
    await repo.delete_match_release_outbox(row.id)
    assert await repo.fetch_match_release_outbox(10, int(time.time() * 1000) + 1000) == []


async def test_match_release_rejects_invalid_payload(pool, repo) -> None:
    """坏 payload 整批停:继续跑等于拿一个不知道该释放谁的记录去调 matchmaker。"""
    bad = match_pb2.MatchReleaseStorageRecord(match_id=0, operation_id="")
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO match_release_outbox (match_id, payload, created_at_ms) VALUES (%s,%s,%s)",
            (1, bad.SerializeToString(), 1),
        )
    with pytest.raises(errcode.PandoraError, match="invalid match release outbox"):
        await repo.fetch_match_release_outbox(10, int(time.time() * 1000) + 1000)


async def test_match_release_defer_requires_args(repo) -> None:
    with pytest.raises(errcode.PandoraError):
        await repo.defer_match_release_outbox(0, 1)
    with pytest.raises(errcode.PandoraError):
        await repo.defer_match_release_outbox(1, 0)


# ── 保留期清理(§9.24)──────────────────────────────────────────────────────


async def _age_battle(pool, match_id: int, days: int) -> None:
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE battles SET created_at = DATE_SUB(NOW(), INTERVAL %s DAY) WHERE match_id=%s",
            (days, match_id),
        )


def _cutoff_ms(days: int) -> int:
    return int((time.time() - days * 86400) * 1000)


async def test_sweep_expired_battles_deletes_group(pool, repo) -> None:
    """battles + battle_player_stats **同事务成组删** —— 只删主表会留下孤儿战绩行。"""
    await repo.save_result(_result(match_id=1001), _outbox(1, 2), None, None, 0)
    await repo.save_result(_result(match_id=1002), _outbox(1, 2), None, None, 0)
    await _age_battle(pool, 1001, 200)
    out = await repo.sweep_expired_battles(dbguard.Mode.DELETE, _cutoff_ms(180), 100)
    assert out.deleted == 1
    assert out.truncated is False
    assert await _count(pool, "battles") == 1
    assert await _count(pool, "battle_player_stats") == 2  # 只剩 1002 那两行
    assert await _count(pool, "battle_player_stats", "match_id=1001") == 0


async def test_sweep_report_only_counts_without_deleting(pool, repo) -> None:
    """★ report_only 一行都不删,且计数**不受 batch 截断**(给真实积压规模)。"""
    for mid in (1001, 1002, 1003):
        await repo.save_result(_result(match_id=mid), _outbox(1, 2), None, None, 0)
        await _age_battle(pool, mid, 200)
    out = await repo.sweep_expired_battles(dbguard.Mode.REPORT_ONLY, _cutoff_ms(180), 1)
    assert out.deleted == 0
    assert out.matched == 3  # ← batch=1 也数出全部 3 场
    assert await _count(pool, "battles") == 3


async def test_sweep_truncated_flag_drives_drain_loop(pool, repo) -> None:
    """truncated 是"追平积压"循环的终止条件;恒 False 会让积压永远追不平。"""
    for mid in (1001, 1002, 1003):
        await repo.save_result(_result(match_id=mid), _outbox(1, 2), None, None, 0)
        await _age_battle(pool, mid, 200)
    out = await repo.sweep_expired_battles(dbguard.Mode.DELETE, _cutoff_ms(180), 2)
    assert out.deleted == 2 and out.truncated is True
    out2 = await repo.sweep_expired_battles(dbguard.Mode.DELETE, _cutoff_ms(180), 2)
    assert out2.deleted == 1 and out2.truncated is False


async def test_sweep_settled_progress_group(pool, repo) -> None:
    """已结算且超期的进度五表成组删;**未结算行永不清**(补偿链 bug 证据)。"""
    old = _cutoff_ms(200)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO battle_progress_stream (match_id, last_applied_seq, settled_at_ms, updated_at_ms) "
            "VALUES (%s,%s,%s,%s)",
            (5001, 3, old, old),
        )
        await cur.execute(
            "INSERT INTO battle_progress_player (match_id, player_id, updated_at_ms) VALUES (%s,%s,%s)",
            (5001, 1, old),
        )
        # 未结算的陈年行:settled_at_ms=0
        await cur.execute(
            "INSERT INTO battle_progress_stream (match_id, last_applied_seq, settled_at_ms, updated_at_ms) "
            "VALUES (%s,%s,%s,%s)",
            (5002, 3, 0, old),
        )
    out = await repo.sweep_settled_progress(dbguard.Mode.DELETE, _cutoff_ms(180), 100)
    assert out.deleted == 1
    assert await _count(pool, "battle_progress_stream", "match_id=5001") == 0
    assert await _count(pool, "battle_progress_player", "match_id=5001") == 0
    assert await _count(pool, "battle_progress_stream", "match_id=5002") == 1  # ← 永不清


async def test_count_stale_unsettled_progress(pool, repo) -> None:
    old = _cutoff_ms(200)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO battle_progress_stream (match_id, settled_at_ms, updated_at_ms) VALUES (%s,0,%s)",
            (5002, old),
        )
        await cur.execute(
            "INSERT INTO battle_progress_stream (match_id, settled_at_ms, updated_at_ms) VALUES (%s,%s,%s)",
            (5003, old, old),
        )
    assert await repo.count_stale_unsettled_progress(_cutoff_ms(180)) == 1


async def test_sweep_uses_created_at_not_ds_reported_ended_at(pool, repo) -> None:
    """★ 清理依据是**服务端落库时间** created_at,不是 DS 上报的 ended_at_ms(§9.6)。

    用 ended_at_ms 的话,伪造一个很老的 ended_at 就能让 DS 提前删掉自己的战绩。
    """
    await repo.save_result(_result(match_id=1001, ended=1), _outbox(1, 2), None, None, 0)
    out = await repo.sweep_expired_battles(dbguard.Mode.DELETE, _cutoff_ms(180), 100)
    assert out.deleted == 0  # ended_at_ms=1(1970 年)也删不掉
    assert await _count(pool, "battles") == 1
