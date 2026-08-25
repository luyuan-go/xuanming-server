"""owner 数据层测试 —— **打真实 MySQL**,不用 mock。

为什么必须打真库:
    被测的就是**事务本身** —— SELECT ... FOR UPDATE 的串行化、同事务读旧租约算屏障、
    epoch 单调 CAS、PENDING→ADMITTED 推进。用 fake repo 测等于把被测对象换成了
    我对事务语义的想象,而 §9.22 的全部价值恰恰在于这些语义真的成立。

没有库就整体 skip(**不假装通过**):
    docker run -d --name pandora-mysql-verify -p 13306:3306 \
      -e MYSQL_ROOT_PASSWORD=pandora_dev_root -e MYSQL_DATABASE=pandora_owner \
      mysql:8.4 --sql-mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"

环境变量 PANDORA_TEST_MYSQL_DSN 可覆盖(与 Go 侧 CI 的门控变量同名)。
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from mysqlfixture import MYSQL_DSN as DSN
from mysqlfixture import ensure_database, parse_go_dsn, skip_only_if_mysql_is_down

from pandorapy import errcode, placement
from pandorapy.services.owner import data as odata
from pandorapy.services.owner import repo as orepo


# 与 deploy/mysql-init/15-owner-tables.sql 同构(TiDB 侧 02-owner-tidb.sql 同 DDL)。
_DDL = [
    """CREATE TABLE IF NOT EXISTS owner_record (
        player_id BIGINT UNSIGNED NOT NULL,
        owner_epoch BIGINT UNSIGNED NOT NULL DEFAULT 0,
        owner_type TINYINT NOT NULL DEFAULT 0,
        phase TINYINT NOT NULL DEFAULT 0,
        pod_name VARCHAR(128) NOT NULL DEFAULT '',
        instance_uid VARCHAR(128) NOT NULL DEFAULT '',
        instance_epoch INT UNSIGNED NOT NULL DEFAULT 0,
        assignment_or_allocation_id VARCHAR(128) NOT NULL DEFAULT '',
        release_track VARCHAR(32) NOT NULL DEFAULT '',
        operation_id VARCHAR(64) NOT NULL DEFAULT '',
        admit_not_before_ms BIGINT NOT NULL DEFAULT 0,
        hub_source_revision BIGINT UNSIGNED NOT NULL DEFAULT 0,
        updated_at_ms BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS ds_instance_lease (
        instance_uid VARCHAR(128) NOT NULL,
        pod_name VARCHAR(128) NOT NULL DEFAULT '',
        instance_epoch INT UNSIGNED NOT NULL DEFAULT 0,
        release_track VARCHAR(32) NOT NULL DEFAULT '',
        lease_deadline_ms BIGINT NOT NULL DEFAULT 0,
        updated_at_ms BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (instance_uid)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS owner_transition_log (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        player_id BIGINT UNSIGNED NOT NULL,
        from_epoch BIGINT UNSIGNED NOT NULL,
        to_epoch BIGINT UNSIGNED NOT NULL,
        op TINYINT NOT NULL,
        operation_id VARCHAR(64) NOT NULL,
        detail VARCHAR(512) NOT NULL DEFAULT '',
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        KEY idx_player (player_id),
        KEY idx_created_at (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
]


@pytest.fixture
async def pool():
    """每个用例一个连接池 —— **必须 function 作用域**。

    ⚠️ Python 迁移特有的坑(2026-08-18 实测):
        pytest-asyncio 默认给**每个用例**新建一个 event loop,而 module 作用域的
        async fixture 建在另一个 loop 里。asyncmy 的池把内部 Task 绑在创建时的 loop 上,
        于是第二个用例开始全部报
            RuntimeError: got Future attached to a different loop
        表现是"第一个用例失败、其余 21 个 ERROR",很容易被误读成连接池坏了。

        Go 侧没有这一层 —— database/sql 的池与 goroutine 无绑定关系。
        凡是"持有后台任务的异步资源"(DB 池、redis 池、grpc channel、kafka client)
        在 Python 测试里都要注意这个作用域约束。

    代价是每个用例重建池(本地 MySQL 上约几十毫秒),换来完全隔离,值得。
    """
    asyncmy = pytest.importorskip("asyncmy")
    pytest.importorskip(
        "cryptography",
        reason="MySQL 8.x 默认 caching_sha2_password 认证,Python 驱动需要 cryptography",
    )
    cfg = parse_go_dsn(DSN, default_db="pandora_owner")
    # * 只有"MySQL 没起来"才允许跳过 —— 夹具自己的代码(ensure_database / cfg 取键 /
    #   create_pool 形参)抛的异常必须原样冒红。判据、放行集与 CI 文案契约
    #   见 tests/mysqlfixture.py 的 skip_only_if_mysql_is_down。
    with skip_only_if_mysql_is_down(
        cfg,
        "owner 数据层测试",
        hint=(
            "起一个:docker run -d -p 13306:3306 -e MYSQL_ROOT_PASSWORD=pandora_dev_root "
            "-e MYSQL_DATABASE=pandora_owner mysql:8.4 "
            '--sql-mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"'
        ),
    ):
        # CI 的 DSN 无库名且 ci-db 的 mysql 没有 init 脚本 —— 库得自己建,
        # 与 Go 侧 *_mysql_test.go 同做法。理由见 tests/mysqlfixture.py 头注释。
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
            for t in ("owner_record", "ds_instance_lease", "owner_transition_log"):
                await cur.execute(f"TRUNCATE TABLE {t}")  # noqa: S608
        yield p
    finally:
        p.close()
        await p.wait_closed()


@pytest.fixture
async def repo(pool):
    return orepo.MySQLOwnerRepo(pool)


def _target(**kw) -> odata.OwnerTarget:
    base = {
        "pod_name": "hub-1",
        "instance_uid": "uid-A",
        "instance_epoch": 1,
        "assignment_or_allocation_id": "assign-1",
        "release_track": "stable",
    }
    base.update(kw)
    return odata.OwnerTarget(**base)


def _op() -> str:
    return str(uuid.uuid4())


SKEW = placement.DS_FENCE_SKEW_MARGIN_SECONDS


# ── 严格模式(§9.24 唯一允许拒绝启动的检查)────────────────────────────────


async def test_sql_mode_is_strict(pool) -> None:
    """★ 前置条件:测试库必须是严格模式。

    非严格模式下超长写入**静默截断**,后面所有"列宽钳制"的断言都会变成假通过。
    """
    from pandorapy import dbguard

    async with pool.acquire() as conn:
        await dbguard.assert_strict_mode(conn)  # 不抛即通过


# ── ★ epoch 单调 CAS ────────────────────────────────────────────────────────


async def test_first_transition_starts_at_epoch_1(repo) -> None:
    rec = await repo.begin_transition(
        1001, 0, _op(), odata.OWNER_TYPE_HUB, _target(), 0, SKEW
    )
    assert rec.owner_epoch == 1
    assert rec.phase == odata.OWNER_PHASE_PENDING


async def test_concurrent_begin_only_one_wins(repo) -> None:
    """★ 并发 Begin 同一玩家、同一 expect_epoch → **只有一个成功**。

    这是"玩家同一时刻只在一个可操作 DS"的第一道闸。若两个都成功,
    两台 DS 会各自拿到一个 PENDING 记录并各自去 Admit。
    """
    results = await asyncio.gather(
        *(
            repo.begin_transition(
                2001, 0, _op(), odata.OWNER_TYPE_HUB,
                _target(instance_uid=f"uid-{i}", assignment_or_allocation_id=f"a-{i}"),
                0, SKEW,
            )
            for i in range(12)
        ),
        return_exceptions=True,
    )
    ok = [r for r in results if isinstance(r, odata.OwnerRecord)]
    conflicts = [r for r in results if isinstance(r, errcode.PandoraError)]
    assert len(ok) == 1, f"{len(ok)} 个并发 Begin 同时成功 —— epoch CAS 失效"
    assert all(c.code == errcode.ErrOwnerEpochConflict for c in conflicts)


async def test_epoch_conflict_carries_current_record(repo) -> None:
    """★ epoch 冲突必须**附当前记录** —— 调用方靠它决定重试还是放弃。"""
    await repo.begin_transition(3001, 0, _op(), odata.OWNER_TYPE_HUB, _target(), 0, SKEW)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.begin_transition(
            3001, 0, _op(), odata.OWNER_TYPE_HUB, _target(instance_uid="uid-B"), 0, SKEW
        )
    assert exc.value.code == errcode.ErrOwnerEpochConflict
    assert getattr(exc.value, "current_record", None) is not None
    assert exc.value.current_record.owner_epoch == 1


async def test_same_exact_target_is_idempotent_noop(repo) -> None:
    """★ 同 exact 实例的重复投递 → 原样返回既有记录(含**原** operation_id)。

    这是"权威铸造 operation"能成立的前提:重复投递不换 operation,
    否则同一次进场的重连/重复交付会写出不同 operation,幂等键失效。
    """
    target = _target()
    first = await repo.begin_transition(
        4001, 0, _op(), odata.OWNER_TYPE_HUB, target, 0, SKEW
    )
    again = await repo.begin_transition(
        4001, 999, _op(), odata.OWNER_TYPE_HUB, target, 0, SKEW  # expect_epoch 故意乱填
    )
    assert again.owner_epoch == first.owner_epoch
    assert again.operation_id == first.operation_id, "重复投递换了 operation_id"


# ── ★ 屏障 ──────────────────────────────────────────────────────────────────


async def test_admit_rejected_before_barrier(repo) -> None:
    """★ 屏障未开时 Admit 必须拒 —— 这是核心时序不等式的执行点。"""
    battle = _target(pod_name="battle-1", instance_uid="uid-B", assignment_or_allocation_id="al-1")
    # 先让 battle 实例有一个还没过期的租约
    await repo.renew_instance_lease(battle, 20)
    op1 = _op()
    await repo.begin_transition(5001, 0, op1, odata.OWNER_TYPE_BATTLE, battle, 0, SKEW)
    await repo.admit(5001, 1, op1, battle)  # BATTLE 首次:旧 owner 是 none,屏障 = now

    # 现在从 BATTLE 迁到 HUB —— 旧 owner 是 BATTLE,屏障 = 旧租约截止 + 余量
    hub = _target()
    op2 = _op()
    rec = await repo.begin_transition(5001, 1, op2, odata.OWNER_TYPE_HUB, hub, 0, SKEW)
    assert rec.admit_not_before_ms > odata.now_ms(), "BATTLE→HUB 屏障没有推迟"

    with pytest.raises(errcode.PandoraError) as exc:
        await repo.admit(5001, 2, op2, hub)
    assert exc.value.code == errcode.ErrOwnerBarrierNotOpen


async def test_admit_succeeds_after_barrier_opens(repo) -> None:
    """屏障开后 Admit 成功,phase → ADMITTED。"""
    op = _op()
    target = _target()
    await repo.begin_transition(6001, 0, op, odata.OWNER_TYPE_HUB, target, 0, SKEW)
    rec, retry = await repo.admit(6001, 1, op, target)
    assert rec.phase == odata.OWNER_PHASE_ADMITTED
    assert retry == 0


async def test_hub_predecessor_opens_barrier_immediately(repo) -> None:
    """★ HUB→HUB 迁移屏障不等待 —— 否则每次换线都卡 27 秒。"""
    op1, op2 = _op(), _op()
    a = _target(instance_uid="uid-A")
    b = _target(instance_uid="uid-B", assignment_or_allocation_id="assign-2")
    await repo.begin_transition(7001, 0, op1, odata.OWNER_TYPE_HUB, a, 0, SKEW)
    await repo.admit(7001, 1, op1, a)
    await repo.renew_instance_lease(a, 20)  # 旧 hub 租约还很长

    rec = await repo.begin_transition(7001, 1, op2, odata.OWNER_TYPE_HUB, b, 0, SKEW)
    assert rec.admit_not_before_ms <= odata.now_ms() + 50, "HUB 前任把屏障推迟了"
    admitted, _ = await repo.admit(7001, 2, op2, b)
    assert admitted.phase == odata.OWNER_PHASE_ADMITTED


# ── ★ Admit 幂等与 exact 匹配 ───────────────────────────────────────────────


async def test_admit_replay_is_idempotent(repo) -> None:
    """★ ACK 丢失后重放必须返回**同一结果**,不能再分配或创建第二 owner(§9.23)。"""
    op, target = _op(), _target()
    await repo.begin_transition(8001, 0, op, odata.OWNER_TYPE_HUB, target, 0, SKEW)
    first, _ = await repo.admit(8001, 1, op, target)
    for _ in range(5):
        again, retry = await repo.admit(8001, 1, op, target)
        assert again.owner_epoch == first.owner_epoch
        assert again.phase == odata.OWNER_PHASE_ADMITTED
        assert retry == 0


@pytest.mark.parametrize(
    ("bad_epoch", "bad_op", "bad_target", "expect_reason"),
    [
        (99, False, False, "owner_epoch_mismatch"),
        (None, True, False, "operation_id_mismatch"),
        (None, False, True, "target_instance_mismatch"),
    ],
)
async def test_admit_rejects_any_identity_mismatch(
    repo, bad_epoch, bad_op, bad_target, expect_reason
) -> None:
    """★ epoch / operation / exact 实例任一不符 → fail-closed 拒。"""
    op, target = _op(), _target()
    await repo.begin_transition(9001, 0, op, odata.OWNER_TYPE_HUB, target, 0, SKEW)
    use_epoch = bad_epoch if bad_epoch is not None else 1
    use_op = _op() if bad_op else op
    use_target = _target(instance_uid="uid-OTHER") if bad_target else target
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.admit(9001, use_epoch, use_op, use_target)
    assert exc.value.code == errcode.ErrOwnerIdentityMismatch


# ── ★ 租约只前进 ────────────────────────────────────────────────────────────


async def test_lease_deadline_only_advances(repo) -> None:
    """★ deadline **只前进**。

    允许回退等于让一次迟到的短续租**缩短**已经算进屏障的截止时刻,
    新 owner 就可能提前开始可玩 —— 脑裂。
    """
    target = _target()
    long_deadline = await repo.renew_instance_lease(target, 20)
    short_deadline = await repo.renew_instance_lease(target, 1)  # 迟到的短续租
    assert short_deadline == long_deadline, "短续租把 deadline 拉回去了"


async def test_lease_epoch_mismatch_rejected(repo) -> None:
    """实例纪元不符拒(只对双方都非零且不同)。"""
    await repo.renew_instance_lease(_target(instance_epoch=3), 10)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.renew_instance_lease(_target(instance_epoch=5), 10)
    assert exc.value.code == errcode.ErrOwnerLeaseRegressed


async def test_lease_zero_epoch_is_allowed(repo) -> None:
    """hub 凭据不携带实例纪元 → 0 放行(uid 全局唯一已足够)。"""
    await repo.renew_instance_lease(_target(instance_epoch=3), 10)
    assert await repo.renew_instance_lease(_target(instance_epoch=0), 10) > 0


# ── ★ Release ───────────────────────────────────────────────────────────────


async def test_release_keeps_epoch_and_source_revision(repo) -> None:
    """★ Release **不清 epoch、不清 hub_source_revision**。

    清 epoch → 下一次 Begin 的 expect_epoch=0 能通过,旧写者迟到 CAS 又能命中。
    清 revision → 「打完一局回大厅」就把门重新对 legacy(0)敞开,
                  滚动窗口里的旧写者随即又能写进来(INC-20260818-003)。
    """
    op, target = _op(), _target()
    await repo.begin_transition(
        10001, 0, op, odata.OWNER_TYPE_HUB, target, source_revision=77, skew_margin_seconds=SKEW
    )
    await repo.admit(10001, 1, op, target)
    released = await repo.release(10001, 1, op, SKEW)
    assert released.owner_epoch == 1, "Release 把 epoch 清零了"
    assert released.hub_source_revision == 77, "Release 把来源版本清零了"
    assert released.owner_type == odata.OWNER_TYPE_NONE
    persisted = await repo.query(10001)
    assert persisted.owner_epoch == 1
    assert persisted.hub_source_revision == 77


async def test_stale_release_is_noop_not_error(repo) -> None:
    """★ 迟到 Release 幂等 no-op,**不报错**。

    迟到登出是正常现象,报错会让调用方无谓重试;更重要的是它绝不能删掉新会话的记录。
    """
    op1, target = _op(), _target()
    await repo.begin_transition(11001, 0, op1, odata.OWNER_TYPE_HUB, target, 0, SKEW)
    # 迟到的 Release 拿着旧 epoch / 别的 operation
    rec = await repo.release(11001, 999, _op(), SKEW)
    assert rec.owner_epoch == 1, "迟到 Release 影响了当前记录"
    assert rec.owner_type == odata.OWNER_TYPE_HUB


# ── ★ hub_source_revision 单调 ──────────────────────────────────────────────


async def test_source_revision_advances(repo) -> None:
    op1, op2 = _op(), _op()
    a, b = _target(instance_uid="uid-A"), _target(instance_uid="uid-B", assignment_or_allocation_id="assign-2")
    await repo.begin_transition(12001, 0, op1, odata.OWNER_TYPE_HUB, a, 10, SKEW)
    rec = await repo.begin_transition(12001, 1, op2, odata.OWNER_TYPE_HUB, b, 20, SKEW)
    assert rec.hub_source_revision == 20


async def test_stale_source_revision_rejected(repo) -> None:
    """★ 来源版本倒退 → 拒。

    事故反例里旧 binary 恰好能拿到**合法的** expect_epoch(它先 Begin 后 CAS),
    所以只靠 epoch 挡不住它;能挡住的只有来源版本。
    """
    op1, op2 = _op(), _op()
    a = _target(instance_uid="uid-A")
    b = _target(instance_uid="uid-B", assignment_or_allocation_id="assign-2")
    await repo.begin_transition(13001, 0, op1, odata.OWNER_TYPE_HUB, a, 50, SKEW)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.begin_transition(13001, 1, op2, odata.OWNER_TYPE_HUB, b, 20, SKEW)
    assert exc.value.code == errcode.ErrOwnerSourceRevisionStale


async def test_legacy_zero_revision_allowed_only_before_any_version(repo) -> None:
    """source_revision=0(兼容窗)只在该玩家**从未见过版本**时放行,且不推进高水位。"""
    op1, op2 = _op(), _op()
    a = _target(instance_uid="uid-A")
    b = _target(instance_uid="uid-B", assignment_or_allocation_id="assign-2")
    await repo.begin_transition(14001, 0, op1, odata.OWNER_TYPE_HUB, a, 0, SKEW)
    rec = await repo.begin_transition(14001, 1, op2, odata.OWNER_TYPE_HUB, b, 0, SKEW)
    assert rec.hub_source_revision == 0
    assert rec.owner_epoch == 2, "兼容窗内的 legacy 迁移应该正常推进 epoch"


async def test_legacy_zero_revision_rejected_after_a_version_was_seen(repo) -> None:
    """★ 见过非零版本之后,legacy(=0)必须被**永久拒绝**。

    这是整道门的关键,也是 INC-20260818-003 的事故形状本身:
    0 与任何非零 revision **不可比**,不能当成"最小值"放行 ——
    否则旧 hub_allocator 只要"不带版本"就能绕过整道门,把玩家写回旧 Hub,
    而 Redis 与 Owner 从此分叉(静默失主,没有任何运行期信号)。

    这条曾经是反着钉死的:老实现写的是 `if source_revision > 0:`(=0 整条跳过闸),
    配套用例还断言"放行且不覆盖高水位"。测试全绿,门却是开的。
    """
    op1, op2 = _op(), _op()
    a = _target(instance_uid="uid-A")
    b = _target(instance_uid="uid-B", assignment_or_allocation_id="assign-2")
    await repo.begin_transition(14101, 0, op1, odata.OWNER_TYPE_HUB, a, 60, SKEW)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.begin_transition(14101, 1, op2, odata.OWNER_TYPE_HUB, b, 0, SKEW)
    assert exc.value.code == errcode.ErrOwnerSourceRevisionStale
    assert "legacy_after_versioned" in str(exc.value)
    # 高水位不能被这次被拒的写动到
    assert (await repo.query(14101)).hub_source_revision == 60


async def test_same_revision_different_target_is_rejected(repo) -> None:
    """★ 同一版本号指向**不同** target = 铸号被复制(两个写者共用了同一任期)。

    这不是"旧写者迟到"(那是 stale),是全序前提本身被打破 —— 必须拒。
    老实现用的是严格 `<`,相等一律放行,于是两个共用任期的写者可以互相覆盖。
    """
    op1, op2 = _op(), _op()
    a = _target(instance_uid="uid-A")
    b = _target(instance_uid="uid-B", assignment_or_allocation_id="assign-2")
    await repo.begin_transition(14201, 0, op1, odata.OWNER_TYPE_HUB, a, 70, SKEW)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.begin_transition(14201, 1, op2, odata.OWNER_TYPE_HUB, b, 70, SKEW)
    assert exc.value.code == errcode.ErrOwnerSourceRevisionStale
    assert "same_revision_different_target" in str(exc.value)


async def test_global_legacy_gate_rejects_even_before_any_version(repo) -> None:
    """rollout 最后一步:全局门打开后,连兼容窗内的 legacy 也一律拒。"""
    op = _op()
    repo.set_reject_legacy_source_revision(True)
    try:
        with pytest.raises(errcode.PandoraError) as exc:
            await repo.begin_transition(
                14301, 0, op, odata.OWNER_TYPE_HUB, _target(instance_uid="uid-A"), 0, SKEW
            )
        assert "legacy_rejected_globally" in str(exc.value)
    finally:
        repo.set_reject_legacy_source_revision(False)


# ── 审计 ────────────────────────────────────────────────────────────────────


async def test_transition_log_written_for_each_op(repo, pool) -> None:
    op, target = _op(), _target()
    await repo.begin_transition(15001, 0, op, odata.OWNER_TYPE_HUB, target, 0, SKEW)
    await repo.admit(15001, 1, op, target)
    await repo.release(15001, 1, op, SKEW)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT op, detail FROM owner_transition_log WHERE player_id=%s ORDER BY id",
            (15001,),
        )
        rows = await cur.fetchall()
    assert [r[0] for r in rows] == [
        odata.TRANSITION_OP_BEGIN,
        odata.TRANSITION_OP_ADMIT,
        odata.TRANSITION_OP_RELEASE,
    ]
    assert "uid=uid-A" in rows[0][1], "审计没带 exact 实例身份"


async def test_oversized_detail_does_not_fail_transition(repo) -> None:
    """★ 超长审计字段**不能**让一次本该成功的迁移失败。

    严格模式下超长是 Error 1406 而非截断 —— 所以必须在应用侧钳到列宽。
    这条用真库跑才有意义:非严格模式下会静默截断,断言会假通过。
    """
    op = _op()
    huge = _target(
        pod_name="p" * 120,
        assignment_or_allocation_id="a" * 120,
        instance_uid="u" * 120,
    )
    rec = await repo.begin_transition(16001, 0, op, odata.OWNER_TYPE_HUB, huge, 0, SKEW)
    assert rec.owner_epoch == 1


# ══ 2026-08-19 与 Go 逐条对照时抓到的三处移植缺陷 ═════════════════════════════
#
# 三条的共同点:**功能全对、测试全绿、diff 也看不出来**,只有把 Go 与 Python 起在
# 同一个库上跑同一份场景才暴露。所以每条都在这里钉一个机械判据。


async def test_source_revision_gate_runs_before_noop_branches(repo) -> None:
    """★ 来源版本闸必须排在 **no-op 早退分支之前**(INC-20260818-003)。

    移植时把顺序写成了「同 target no-op → epoch CAS → 版本闸」,后果是
    **重复投递整条跳过版本校验**:旧 hub_allocator 拿着更旧的 revision 重投同一
    target,权威照样回 OK。

    为什么 epoch 检查兜不住:事故反例里旧 binary 手上握着一个**合法**的
    expect_epoch(它先 Begin 后 CAS),epoch 一致 —— 能判定"谁的来源更新"的只有本闸。
    """
    pid = 9101
    high, low = (1 << 24) | 2, (1 << 24) | 1
    target = _target(instance_uid="uid-rev-a")
    await repo.begin_transition(pid, 0, _op(), odata.OWNER_TYPE_HUB, target, high, SKEW)

    with pytest.raises(errcode.PandoraError) as exc:
        # 同一个 target(会命中 no-op 分支),但 revision 更旧
        await repo.begin_transition(pid, 1, _op(), odata.OWNER_TYPE_HUB, target, low, SKEW)
    assert exc.value.code == errcode.ErrOwnerSourceRevisionStale, (
        "同 target 的重复投递绕过了来源版本闸 —— 闸被排在 no-op 之后了"
    )


async def test_high_water_advances_on_noop_branch(repo) -> None:
    """★ 高水位必须在 no-op 早退分支里**也推进**。

    hub 侧把存量 legacy(0)补成 R 时,target 一个字节都不变 —— 这次 Begin 必然落到
    幂等重放 / same_target 的 return。推水位的代码若只写在下游就永远走不到,水位
    永久停在旧值,「某玩家见过非零版本就永久拒 legacy」这条逐玩家防线对这批玩家
    **从不 arm**,只剩全局开关一道保护。

    判据用"后续更旧的版本会不会被拒",而不是直接读水位 —— 读到的数对不对是表象,
    门有没有 arm 才是要的东西。
    """
    pid = 9102
    r1, r2 = (1 << 24) | 1, (1 << 24) | 2
    target = _target(instance_uid="uid-rev-b")
    await repo.begin_transition(pid, 0, _op(), odata.OWNER_TYPE_HUB, target, r1, SKEW)

    # 同 target 重投更高版本 → 走 no-op 分支,但水位必须推到 r2
    noop = await repo.begin_transition(pid, 1, _op(), odata.OWNER_TYPE_HUB, target, r2, SKEW)
    assert noop.hub_source_revision == r2, "no-op 分支没有推进高水位"

    # 换 target 用回 r1 → 必须被拒(证明水位真的 arm 了,不只是返回值好看)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.begin_transition(
            pid, 1, _op(), odata.OWNER_TYPE_HUB, _target(instance_uid="uid-rev-c"), r1, SKEW
        )
    assert exc.value.code == errcode.ErrOwnerSourceRevisionStale


async def test_barrier_error_carries_retry_after_and_record(repo) -> None:
    """★ 屏障未开的错误必须**同时**带 retry_after_ms 与当前记录。

    Go 的 Admit 签名是 `(rec, retryAfterMs, err)` 三元,三样一起返回;Python 用异常
    传播,不显式挂上就等于丢掉 —— 调用方收到 `retry_after_ms=0` 的 WAIT,只能空转
    或干等,§9.23「不得无出口等待」当场被打穿,而且**没有任何报错**。
    """
    pid = 9103
    battle = _target(
        pod_name="battle-r", instance_uid="uid-rev-d", assignment_or_allocation_id="al-r"
    )
    await repo.renew_instance_lease(battle, 20)
    await repo.begin_transition(pid, 0, _op(), odata.OWNER_TYPE_BATTLE, battle, 0, SKEW)
    op2 = _op()
    nxt = _target(
        pod_name="battle-r2", instance_uid="uid-rev-e", assignment_or_allocation_id="al-r2"
    )
    await repo.begin_transition(pid, 1, op2, odata.OWNER_TYPE_BATTLE, nxt, 0, SKEW)

    with pytest.raises(errcode.PandoraError) as exc:
        await repo.admit(pid, 2, op2, nxt)
    assert exc.value.code == errcode.ErrOwnerBarrierNotOpen
    assert exc.value.retry_after_ms > 0, (
        "屏障未开却没给 retry_after_ms —— 调用方无法退避,只能空转或干等"
    )
    assert exc.value.current_record is not None, "屏障未开时没有附当前记录"
    assert exc.value.current_record.owner_epoch == 2


async def test_release_preserves_operation_and_barrier_columns(repo) -> None:
    """★ Release 不得清 operation_id / admit_not_before_ms / hub_source_revision。

    注:2026-08-24 起 admit_not_before_ms 对 **BATTLE** 归属会被盖上算好的再入屏障
    (INC-20260824-003,见 test_released_battle_retains_barrier)。本用例走的是 HUB
    归属,该分支不盖新值,「不得清空」在这里仍是原样成立的断言。


    清掉 hub_source_revision 的后果:「打完一局 / 掉一次线」就把该玩家的门重新对
    legacy(0)敞开,滚动窗口里的旧写者随即又能写进来。

    清掉 operation_id 的后果更隐蔽:「已释放」这个状态失去锚点,迟到 Release 的判定
    只能靠 operation 对不上来兜 —— 而那与「另一条链拿着过期 operation 来释放」
    在日志里完全无法区分,两者都只剩 operation_mismatch。
    """
    pid = 9104
    rev = (1 << 24) | 5
    op = _op()
    target = _target(instance_uid="uid-rev-f")
    await repo.begin_transition(pid, 0, op, odata.OWNER_TYPE_HUB, target, rev, SKEW)
    await repo.admit(pid, 1, op, target)

    released = await repo.release(pid, 1, op, SKEW)
    assert released.owner_type == odata.OWNER_TYPE_NONE
    assert released.owner_epoch == 1, "epoch 被清零 —— 下一次 Begin 的 expect_epoch=0 会通过"
    assert released.hub_source_revision == rev, "释放把来源版本高水位一起抹掉了"
    assert released.operation_id == op, "释放清掉了 operation_id"

    # 重放的 Release 必须是 no-op(靠守卫里的 owner_type == NONE 拦下,
    # 而不是靠"operation_id 已被清空"这种副作用)
    again = await repo.release(pid, 1, op, SKEW)
    assert again.owner_type == odata.OWNER_TYPE_NONE
    assert again.hub_source_revision == rev


async def test_released_battle_retains_barrier(repo) -> None:
    """★ 释放 BATTLE 归属不得把再入屏障一起抹掉(INC-20260824-003)。

    屏障的唯一判据是 owner_type=BATTLE + instance_uid,而 release 的 UPDATE 正要清空
    这两列。修复前:释放之后的下一次 begin 落到「无旧 BATTLE 归属」分支、屏障 = now,
    而那台旧战斗 DS 可能仍活着(Pawn 仍被模拟、journal 迟到写在途)——§9.22 的核心
    时序不等式「旧 DS 最晚停止可玩 < 新 DS 最早开始可玩」被反转。

    这条路径不是边角:login 登出释放对 BATTLE 归属一视同仁(判据只有 owner_type != 0),
    「对局中登出 → 立刻重登」早就走在上面。

    ★ 变异(两处任一,都会让本用例红):
      ① 删掉 release 里算 retained_barrier 并写进 _SQL_RELEASE 的那段;
      ② 删掉 begin_transition 里 `current.admit_not_before_ms > barrier` 的取 max。
    """
    pid = 9105
    op_a, op_b = _op(), _op()
    battle_target = _target(instance_uid="uid-r-battle")
    hub_target = _target(instance_uid="uid-r-hub")
    await repo.begin_transition(
        pid, 0, op_a, odata.OWNER_TYPE_BATTLE, battle_target, 0, SKEW
    )
    await repo.admit(pid, 1, op_a, battle_target)
    lease_deadline = await repo.renew_instance_lease(battle_target, 20)
    want_min = lease_deadline + SKEW * 1000

    released = await repo.release(pid, 1, op_a, SKEW)
    # ① 释放照常放弃归属……
    assert released.owner_type == odata.OWNER_TYPE_NONE
    assert released.target.instance_uid == ""
    # ……但必须把屏障算好留下,而不是连同判据一起清零。
    assert released.admit_not_before_ms >= want_min, (
        f"释放必须留存屏障: admit={released.admit_not_before_ms} "
        f"want>={want_min}(旧租约={lease_deadline} 余量={SKEW * 1000}ms)"
    )
    # ② 留存值必须真的落库,不能只活在返回值里。
    persisted = await repo.query(pid)
    assert persisted.admit_not_before_ms == released.admit_not_before_ms, "留存屏障未落库"

    # ③ 释放后的下一次迁移此时**已无旧 owner**,正是修复前塌成 now 的那一步。
    #    这里刻意传 skew=0:屏障必须完全由留存值顶住,不靠本次余量。
    nxt = await repo.begin_transition(pid, 1, op_b, odata.OWNER_TYPE_HUB, hub_target, 0, 0)
    assert nxt.admit_not_before_ms >= want_min, (
        f"释放后 begin 屏障塌陷: admit={nxt.admit_not_before_ms} want>={want_min}"
    )

    # ④ 屏障必须**真生效**,不只是存下来的一个数:未开时 admit 须拒并带 retry_after。
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.admit(pid, 2, op_b, hub_target)
    assert ei.value.code == errcode.ErrOwnerBarrierNotOpen


async def test_release_noop_reason_distinguishes_causes(repo) -> None:
    """★ 迟到 Release 的四种成因必须能分开 —— 处置完全不同。

    already_released 是正常重放;epoch_mismatch / operation_mismatch 说明调用方拿着
    过期上下文在释放(可能是另一条链的残留);record_absent 是权威侧丢了记录。
    只打一句"no-op"的话,值班的人分不出该不该管。
    """
    rec = odata.OwnerRecord(
        player_id=1, owner_epoch=7, owner_type=odata.OWNER_TYPE_HUB, operation_id="op-x"
    )
    assert odata.release_noop_reason(False, rec, 7, "op-x") == "record_absent"
    assert odata.release_noop_reason(True, rec, 8, "op-x") == "epoch_mismatch"
    assert odata.release_noop_reason(True, rec, 7, "op-y") == "operation_mismatch"
    freed = odata.OwnerRecord(
        player_id=1, owner_epoch=7, owner_type=odata.OWNER_TYPE_NONE, operation_id="op-x"
    )
    assert odata.release_noop_reason(True, freed, 7, "op-x") == "already_released"


# ── autocommit=False 下归还连接会不会留下未提交事务(2026-08-19 实测定谳)──────
#
# 起因:owner/repo.py 的 `query()` 是**唯一**既不 commit 也不 rollback 的路径,而池建成
# `autocommit=False`。审计当时把它列为"需要证据"的疑点,mail/main.py 的注释也断言
# 「建成 False 会让连接归池后带着旧快照被复用,读到陈旧数据且零报错」。
#
# **实测结论:这个危险在 asyncmy 的池上不成立。** 用 maxsize=1 强制复用同一条连接,
# 第一次 SELECT 后不提交就归还,另一条独立连接插入并提交,再借出同一条连接 ——
# 能读到新行(隔离级别确认是 REPEATABLE-READ)。说明池在归还/借出时重置了事务。
#
# 所以:
#   - owner.query() 缺 rollback **不是** live bug,不要按那个理由去"修"它;
#   - mail 选 autocommit=True 的正当理由是**与 Go 的 database/sql 默认语义对齐**,
#     不是"否则会读到陈旧数据"。
#
# 这条测试把该行为钉住:asyncmy 换版本改掉这个语义时要能当场发现,
# 因为那时 owner.query() 就**真的**需要补 rollback 了。


async def test_pool_reset_transaction_on_release_with_autocommit_false(pool) -> None:
    """★ 池归还连接时必须重置事务,否则 owner.query() 的无提交读会污染下一个请求。"""
    asyncmy = pytest.importorskip("asyncmy")
    table = "autocommit_probe"

    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(f"DROP TABLE IF EXISTS {table}")
        await cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY)")
        await conn.commit()

    # ① 复刻 owner.query() 的形状:只读、**不提交**就归还
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT COUNT(*) FROM {table}")
        before = (await cur.fetchone())[0]

    # ② 用一条**完全独立**的连接写入并提交(不走这个池)
    cfg = parse_go_dsn(DSN, default_db="pandora_owner")
    other = await asyncmy.connect(
        host=cfg["host"], port=cfg["port"], user=cfg["user"],
        password=cfg["password"], db=cfg["db"], autocommit=True,
    )
    try:
        async with other.cursor() as cur:
            await cur.execute(f"INSERT INTO {table} (id) VALUES (1)")
    finally:
        await other.ensure_closed()

    # ③ 再从池里读:读不到新行 = 旧快照跟着连接回到了池里
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT COUNT(*) FROM {table}")
        after = (await cur.fetchone())[0]
        await cur.execute(f"DROP TABLE IF EXISTS {table}")
        await conn.commit()

    assert after == before + 1, (
        "归还连接时没有重置事务 —— 上一个请求的未提交只读事务把 REPEATABLE READ 快照"
        "带给了下一个请求。此时 owner.query() 必须补 rollback/commit,"
        "否则归属权威会基于过期事实判定(§9.22)。"
    )
