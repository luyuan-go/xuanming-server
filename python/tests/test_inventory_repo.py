"""inventory 数据层测试 —— **打真实 MySQL**,不用 mock。

为什么必须打真库:
    被测的就是**事务本身**。inventory 的原子性全压在三样东西上 ——
    本地事务、`SELECT ... FOR UPDATE` 行锁、唯一键 1062;这三样都**不写在类型里**,
    用 fake repo 测等于把被测对象换成了我对事务语义的想象。
    尤其是 EnsureAuctionEscrow 的「1062 → 先回滚 → 新事务复核」:
    在 fake 上永远绿,在真库上只要少一步 rollback 就会自己等自己的锁到 1205 超时。

没有库就整体 skip(**不假装通过**):
    docker run -d --name pandora-mysql-verify -p 13306:3306 \
      -e MYSQL_ROOT_PASSWORD=pandora_dev_root \
      mysql:8.4 --sql-mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"

环境变量 PANDORA_TEST_MYSQL_DSN 可覆盖(与 Go 侧 CI 的门控变量同名)。
"""

from __future__ import annotations

import asyncio

import pytest

from mysqlfixture import MYSQL_DSN as DSN
from mysqlfixture import ensure_database, parse_go_dsn

from pandorapy import dbguard, errcode
from pandorapy.services.inventory import fingerprint as ifp
from pandorapy.services.inventory import repo as irepo
from pandorapy.services.inventory.models import (
    ESCROW_STATUS_CLOSED,
    EscrowKind,
    InstanceOwnershipQuery,
    ItemAttribute,
    ItemGrant,
    TransferClaimItem,
)


# 与 deploy/mysql-init/08-inventory-tables.sql 同构。
_DDL = [
    """CREATE TABLE IF NOT EXISTS player_currency (
        player_id BIGINT UNSIGNED NOT NULL,
        gold BIGINT NOT NULL DEFAULT 0,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
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
    """CREATE TABLE IF NOT EXISTS inventory_ledger (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        player_id BIGINT UNSIGNED NOT NULL,
        idempotency_key VARCHAR(64) NOT NULL,
        op VARCHAR(16) NOT NULL,
        request_fingerprint CHAR(64) NOT NULL DEFAULT '',
        result_remaining BIGINT NOT NULL DEFAULT 0,
        result_gold BIGINT NOT NULL DEFAULT 0,
        detail VARCHAR(255) NOT NULL DEFAULT '',
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uk_player_idem (player_id, idempotency_key),
        KEY idx_player_created (player_id, created_at),
        KEY idx_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS auction_escrow (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        player_id BIGINT UNSIGNED NOT NULL,
        order_id BIGINT UNSIGNED NOT NULL,
        kind TINYINT NOT NULL,
        item_config_id INT UNSIGNED NOT NULL DEFAULT 0,
        frozen_qty BIGINT NOT NULL DEFAULT 0,
        frozen_gold BIGINT NOT NULL DEFAULT 0,
        status TINYINT NOT NULL DEFAULT 1,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uk_player_order (player_id, order_id),
        KEY idx_player (player_id),
        KEY idx_status_updated (status, updated_at)
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
    """CREATE TABLE IF NOT EXISTS mail_transfer_escrow (
        instance_id BIGINT UNSIGNED NOT NULL,
        item_config_id INT UNSIGNED NOT NULL,
        identified TINYINT NOT NULL DEFAULT 0,
        attributes VARBINARY(1024) NULL,
        bound TINYINT NOT NULL DEFAULT 0,
        source_player_id BIGINT UNSIGNED NOT NULL,
        to_player_id BIGINT UNSIGNED NOT NULL,
        escrow_key VARCHAR(64) NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (instance_id),
        KEY idx_to_player (to_player_id),
        KEY idx_source_player (source_player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
]

_TABLES = (
    "player_currency",
    "player_items",
    "inventory_ledger",
    "auction_escrow",
    "player_item_instance",
    "mail_transfer_escrow",
)


@pytest.fixture
async def pool():
    """每个用例一个连接池 —— **必须 function 作用域**。

    pytest-asyncio 给每个用例新建 event loop,而 asyncmy 的池把内部 Task 绑在
    创建时的 loop 上;module 作用域会让第二个用例开始全部报
    `got Future attached to a different loop`(表现像"连接池坏了",其实是作用域)。
    """
    asyncmy = pytest.importorskip("asyncmy")
    pytest.importorskip(
        "cryptography",
        reason="MySQL 8.x 默认 caching_sha2_password 认证,Python 驱动需要 cryptography",
    )
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
                # ★ 与 main.py 建池的方式一致(autocommit=True + 显式 begin)。
                # 用 False 建的话,本文件全部用例都会在一个隐式事务里跑,
                # 测出来的"原子性"是假的。
                autocommit=True,
            ),
            timeout=8,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(
            f"MySQL 不可用 @ {cfg['host']}:{cfg['port']} ({exc}) —— "
            f"inventory 数据层测试整体跳过(不假装通过)"
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
async def repo(pool, request):  # noqa: ARG001
    # 保留期清理的 SQL 要按 schema 限定表名,所以这里传本进程的临时库名 ——
    # 用默认的 pandora_trade 会让 sweep 用例去扫一个本机不存在的库。
    from mysqlfixture import session_db_name

    cfg = parse_go_dsn(DSN, default_db="pandora_trade")
    return irepo.MySQLInventoryRepo(pool, db=cfg["db"] or session_db_name())


# ── 幂等流水:回放 / 指纹冲突 ─────────────────────────────────────────────


async def test_grant_items_is_idempotent_and_replays_snapshot(repo) -> None:
    """同 key 重放只入账一次,且返回**首次结果快照**。

    回放当前状态而不是首次快照的话:玩家两次都拿到"成功",但金额不同 ——
    调用方(mail 领奖)无法判断到底发了多少。
    """
    items = [ItemGrant(item_config_id=1001, count=3)]
    gold1, already1 = await repo.grant_items(1, items, 100, "k-grant", "d")
    assert (gold1, already1) == (100, False)

    gold2, already2 = await repo.grant_items(1, items, 100, "k-grant", "d")
    assert (gold2, already2) == (100, True)

    gold, stacks = await repo.get_inventory(1)
    assert gold == 100
    assert [(s.item_config_id, s.count) for s in stacks] == [(1001, 3)]


async def test_same_key_different_request_is_idempotency_conflict(repo) -> None:
    """同 key 复用到不同内容 → fail-closed(防 key 复用串改账)。

    不判冲突的话,"发 3 个" 的 key 被拿去请求 "发 3000 个",服务端返回成功却什么都没做
    —— 或者更糟,回放出与本次请求无关的结果。
    """
    await repo.grant_items(1, [ItemGrant(item_config_id=1001, count=3)], 0, "k", "d")
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.grant_items(1, [ItemGrant(item_config_id=1001, count=3000)], 0, "k", "d")
    assert ei.value.code == errcode.ErrInventoryIdempotencyConflict


async def test_use_item_deducts_and_replays_remaining(repo) -> None:
    await repo.grant_items(1, [ItemGrant(item_config_id=1001, count=5)], 0, "g", "d")
    remaining, already = await repo.use_item(1, 1001, 2, "u1", "d")
    assert (remaining, already) == (3, False)
    # 再扣一次(不同 key)→ 真扣;回放旧 key → 拿到首次快照 3 而不是当前的 1。
    assert (await repo.use_item(1, 1001, 2, "u2", "d"))[0] == 1
    assert await repo.use_item(1, 1001, 2, "u1", "d") == (3, True)


async def test_deduct_to_zero_deletes_the_row(repo) -> None:
    """扣空即删行(2026-07-22 用户要求):不留 count=0 死行。

    留行会让 player_items 无界堆积;读侧本就过滤 count>0,留着没有任何收益。
    """
    await repo.grant_items(1, [ItemGrant(item_config_id=1001, count=2)], 0, "g", "d")
    assert (await repo.use_item(1, 1001, 2, "u", "d"))[0] == 0
    _gold, stacks = await repo.get_inventory(1)
    assert stacks == []


async def test_insufficient_and_not_found_are_distinct_codes(repo) -> None:
    """"没有这个道具" 与 "数量不够" 必须是两个码 —— 客户端提示文案不同。"""
    with pytest.raises(errcode.PandoraError) as missing:
        await repo.use_item(1, 1001, 1, "u1", "d")
    assert missing.value.code == errcode.ErrInventoryItemNotFound

    await repo.grant_items(1, [ItemGrant(item_config_id=1001, count=1)], 0, "g", "d")
    with pytest.raises(errcode.PandoraError) as short:
        await repo.use_item(1, 1001, 5, "u2", "d")
    assert short.value.code == errcode.ErrInventoryInsufficient


async def test_sell_gold_zero_rolls_back_the_claim_row(repo, pool) -> None:
    """gold<=0 的首次请求整笔回滚 —— **连 claim 行一起没有**。

    留下脏 claim 行会把这个 key 永久占死:配置修好之后同 key 重试会命中幂等回放,
    返回一个"成功但没卖出去"的结果。
    """
    await repo.grant_items(1, [ItemGrant(item_config_id=1001, count=5)], 0, "g", "d")
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.sell_item(1, 1001, 1, 0, "s", "sell item=1001 count=1 gold=0")
    assert ei.value.code == errcode.ErrInventoryNotSellable

    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT COUNT(*) FROM inventory_ledger WHERE player_id=%s AND idempotency_key=%s",
            (1, "s"),
        )
        assert (await cur.fetchone())[0] == 0
    # 配置修好后同 key 重试必须能真卖。
    remaining, gold, already = await repo.sell_item(
        1, 1001, 1, 50, "s", "sell item=1001 count=1 gold=50"
    )
    assert (remaining, gold, already) == (4, 50, False)


async def test_legacy_sale_ledger_row_is_replayable_after_price_change(repo, pool) -> None:
    """升级前提交的 sell 行(指纹含首次成交价)必须仍能回放。

    ★ 这条防的是:热更改价之后,同 key 的重试因为"当前价 ≠ 首次价"被误判为冲突,
      严重时诱发二次出售。新指纹不含 gold,旧行靠 detail 里的首次价重算旧指纹识别。
    """
    await repo.grant_items(1, [ItemGrant(item_config_id=1001, count=5)], 0, "g", "d")
    legacy_detail = "sell item=1001 count=1 gold=50"
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO inventory_ledger "
            "(player_id, idempotency_key, op, request_fingerprint, result_remaining, "
            "result_gold, detail) VALUES (%s,%s,'sell',%s,%s,%s,%s)",
            (1, "old", ifp.legacy_sell_fingerprint(1001, 1, 50), 4, 50, legacy_detail),
        )
    # 售价已热更成 80:仍必须回放首次快照,而不是判冲突,更不是再卖一次。
    remaining, gold, already = await repo.sell_item(
        1, 1001, 1, 80, "old", "sell item=1001 count=1 gold=80"
    )
    assert (remaining, gold, already) == (4, 50, True)
    _g, stacks = await repo.get_inventory(1)
    assert stacks[0].count == 5  # 一件都没再扣


async def test_legacy_matcher_rejects_tampered_detail(repo, pool) -> None:
    """detail 必须**完全等于**回拼结果:它是唯一能恢复首次价的地方,可构造就形同虚设。"""
    await repo.grant_items(1, [ItemGrant(item_config_id=1001, count=5)], 0, "g", "d")
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO inventory_ledger "
            "(player_id, idempotency_key, op, request_fingerprint, detail) "
            "VALUES (%s,%s,'sell',%s,%s)",
            (1, "old", ifp.legacy_sell_fingerprint(1001, 1, 50), "sell item=1001 count=1 gold=50 X"),
        )
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.sell_item(1, 1001, 1, 80, "old", "sell item=1001 count=1 gold=80")
    assert ei.value.code == errcode.ErrInventoryIdempotencyConflict


# ── 拍卖托管:冻结 / 成交 / 退还 ─────────────────────────────────────────


async def test_freeze_settle_and_release_full_cycle(repo, pool) -> None:
    """卖家冻道具、买家冻金币 → 成交对转 → 退还残余,一条链跑通。"""
    seller, buyer = 10, 20
    await repo.grant_items(seller, [ItemGrant(item_config_id=1001, count=5)], 0, "gs", "d")
    await repo.grant_items(buyer, [], 1000, "gb", "d")

    assert await repo.freeze_for_order(seller, 101, EscrowKind.ITEM, 1001, 5, 0) is False
    assert await repo.freeze_for_order(buyer, 202, EscrowKind.GOLD, 1001, 5, 500) is False
    # 冻结是幂等的:重复冻不会二次扣。
    assert await repo.freeze_for_order(seller, 101, EscrowKind.ITEM, 1001, 5, 0) is True

    gold_s, stacks_s = await repo.get_inventory(seller)
    assert stacks_s == []  # 5 个全冻进 escrow
    gold_b, _ = await repo.get_inventory(buyer)
    assert (gold_s, gold_b) == (0, 500)

    already = await repo.settle_auction_match(
        901, seller, buyer, 101, 202, 1001, 2, 200, "auction:settle:901", "d"
    )
    assert already is False
    # 重复结算只生效一次(不变量 §9.2 / §9.7)。
    assert (
        await repo.settle_auction_match(
            901, seller, buyer, 101, 202, 1001, 2, 200, "auction:settle:901", "d"
        )
        is True
    )

    gold_s, _ = await repo.get_inventory(seller)
    gold_b, stacks_b = await repo.get_inventory(buyer)
    assert gold_s == 200
    assert [(s.item_config_id, s.count) for s in stacks_b] == [(1001, 2)]

    # 退还残余:卖家拿回 3 个道具,买家拿回 300 金币。
    assert await repo.release_escrow(seller, 101) is False
    assert await repo.release_escrow(buyer, 202) is False
    _g, stacks_s = await repo.get_inventory(seller)
    assert [(s.item_config_id, s.count) for s in stacks_s] == [(1001, 3)]
    gold_b, _ = await repo.get_inventory(buyer)
    assert gold_b == 800
    # 已 closed → 幂等 no-op(只退一次)。
    assert await repo.release_escrow(seller, 101) is True

    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT status FROM auction_escrow WHERE player_id=%s", (seller,))
        assert (await cur.fetchone())[0] == ESCROW_STATUS_CLOSED


async def test_release_escrow_on_missing_row_is_noop(repo) -> None:
    """冻结失败的挂单从没建过 escrow —— 退还必须 no-op 而不是报错(fail-safe)。"""
    assert await repo.release_escrow(10, 999) is True


async def test_freeze_rolls_back_escrow_row_when_balance_short(repo, pool) -> None:
    """资产不足时 escrow 行必须**一起回滚**,否则会留下一条"冻了但没扣"的假托管行。"""
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.freeze_for_order(10, 101, EscrowKind.GOLD, 1001, 5, 500)
    assert ei.value.code == errcode.ErrInventoryInsufficient
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT COUNT(*) FROM auction_escrow WHERE player_id=10")
        assert (await cur.fetchone())[0] == 0


async def test_ensure_escrow_creates_then_validates_existing(repo) -> None:
    """EnsureAuctionEscrow:首次真扣建行;再调时走 1062 → 回滚 → 新事务复核。

    ★ 这一条是 1062 语义的核心验证。少了"先回滚"那一步,第二次调用的
      `SELECT ... FOR UPDATE` 会等自己那个失败事务持有的锁,直到 1205 锁等待超时
      —— 在 fake repo 上永远测不出来。
    """
    await repo.grant_items(10, [ItemGrant(item_config_id=1001, count=5)], 0, "g", "d")
    assert await repo.ensure_auction_escrow(10, 101, EscrowKind.ITEM, 1001, 5, 7) is False
    _g, stacks = await repo.get_inventory(10)
    assert stacks == []  # 真扣了
    # 第二次:already,且**不再扣**活跃资产。
    assert await repo.ensure_auction_escrow(10, 101, EscrowKind.ITEM, 1001, 5, 7) is True


async def test_ensure_escrow_conflicts_on_identity_mismatch(repo) -> None:
    """已存在的托管行 kind/item 对不上 → 冲突,而不是"就当已经补齐了"。"""
    await repo.grant_items(10, [ItemGrant(item_config_id=1001, count=5)], 0, "g", "d")
    await repo.ensure_auction_escrow(10, 101, EscrowKind.ITEM, 1001, 5, 7)
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.ensure_auction_escrow(10, 101, EscrowKind.ITEM, 2002, 5, 7)
    assert ei.value.code == errcode.ErrInventoryIdempotencyConflict


async def test_ensure_escrow_short_frozen_is_insufficient(repo) -> None:
    """既有托管量不够覆盖剩余量 → Insufficient(不能当幂等成功放过)。"""
    await repo.grant_items(10, [ItemGrant(item_config_id=1001, count=5)], 0, "g", "d")
    await repo.ensure_auction_escrow(10, 101, EscrowKind.ITEM, 1001, 2, 7)
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.ensure_auction_escrow(10, 101, EscrowKind.ITEM, 1001, 5, 7)
    assert ei.value.code == errcode.ErrInventoryInsufficient


# ── P2P 结算 ──────────────────────────────────────────────────────────────


async def test_settle_player_trade_swaps_items_and_gold(repo) -> None:
    seller, buyer = 10, 20
    await repo.grant_items(seller, [ItemGrant(item_config_id=1001, count=3)], 0, "gs", "d")
    await repo.grant_items(buyer, [ItemGrant(item_config_id=2002, count=1)], 500, "gb", "d")

    already = await repo.settle_player_trade(
        77,
        seller,
        buyer,
        [ItemGrant(item_config_id=1001, count=2)],
        [ItemGrant(item_config_id=2002, count=1)],
        300,
        "trade:settle:77",
        "d",
    )
    assert already is False
    gold_s, stacks_s = await repo.get_inventory(seller)
    gold_b, stacks_b = await repo.get_inventory(buyer)
    assert gold_s == 300
    assert dict((s.item_config_id, s.count) for s in stacks_s) == {1001: 1, 2002: 1}
    assert gold_b == 200
    assert dict((s.item_config_id, s.count) for s in stacks_b) == {1001: 2}
    # 重复结算只生效一次。
    assert (
        await repo.settle_player_trade(
            77,
            seller,
            buyer,
            [ItemGrant(item_config_id=1001, count=2)],
            [ItemGrant(item_config_id=2002, count=1)],
            300,
            "trade:settle:77",
            "d",
        )
        is True
    )
    assert (await repo.get_inventory(seller))[0] == 300


async def test_settle_player_trade_rolls_back_when_one_side_is_short(repo) -> None:
    """P2P 无预冻结:任一方不足 → 整笔回滚(成交失败),**不能只成一半**。"""
    seller, buyer = 10, 20
    await repo.grant_items(seller, [ItemGrant(item_config_id=1001, count=3)], 0, "gs", "d")
    await repo.grant_items(buyer, [], 100, "gb", "d")  # 金币不够付 300
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.settle_player_trade(
            78, seller, buyer, [ItemGrant(item_config_id=1001, count=2)], [], 300, "k78", "d"
        )
    assert ei.value.code == errcode.ErrInventoryInsufficient
    _g, stacks_s = await repo.get_inventory(seller)
    assert [(s.item_config_id, s.count) for s in stacks_s] == [(1001, 3)]  # 一件没动


# ── 装备实例 ──────────────────────────────────────────────────────────────


async def test_grant_instances_assigns_lowest_free_slots_and_replays(repo) -> None:
    insts, already = await repo.grant_instances(1, [901, 902], [10, 11], 5, "gi")
    assert already is False
    assert [(i.instance_id, i.slot_index) for i in insts] == [(901, 0), (902, 1)]

    replay, already2 = await repo.grant_instances(1, [903, 904], [10, 11], 5, "gi")
    assert already2 is True
    # 回放按 ledger.detail 里的**首次** id 读回,不会再发两件。
    assert [i.instance_id for i in replay] == [901, 902]
    assert len(await repo.list_instances(1)) == 2


async def test_grant_instances_capacity_full(repo) -> None:
    await repo.grant_instances(1, [901, 902], [10, 11], 2, "g1")
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.grant_instances(1, [903], [10], 2, "g2")
    assert ei.value.code == errcode.ErrInventoryCapacityFull


async def test_identify_instance_is_idempotent_and_does_not_reroll(repo) -> None:
    """已鉴定 → 回放已落定属性。重 roll 会让同一件装备在重试时变出不同词条。"""
    await repo.grant_instances(1, [901], [10], 5, "g")
    attrs = [ItemAttribute(attr_id=3, value=7)]
    inst, already = await repo.identify_instance(1, 901, attrs)
    assert (already, inst.identified) == (False, True)
    assert inst.attributes == attrs

    inst2, already2 = await repo.identify_instance(1, 901, [ItemAttribute(attr_id=9, value=99)])
    assert already2 is True
    assert inst2.attributes == attrs  # 没被第二次 roll 覆盖


async def test_instance_attributes_survive_round_trip_as_protobuf(repo) -> None:
    """词条列是 pb 二进制(§5.8):读回来必须逐条一致,不能靠 JSON 之类的宽松解析。"""
    await repo.grant_instances(1, [901], [10], 5, "g")
    attrs = [ItemAttribute(attr_id=3, value=5), ItemAttribute(attr_id=9, value=-1)]
    await repo.identify_instance(1, 901, attrs)
    listed = await repo.list_instances(1)
    assert listed[0].attributes == attrs


async def test_move_instance_rejects_occupied_and_out_of_range(repo) -> None:
    await repo.grant_instances(1, [901, 902], [10, 11], 5, "g")
    with pytest.raises(errcode.PandoraError) as occupied:
        await repo.move_instance(1, 901, 1, 5)  # 1 号格被 902 占着
    assert occupied.value.code == errcode.ErrInventorySlotOccupied
    with pytest.raises(errcode.PandoraError) as oob:
        await repo.move_instance(1, 901, 99, 5)
    assert oob.value.code == errcode.ErrInventorySlotOccupied
    # 移到空格成功;移到自己所在格 no-op。
    assert (await repo.move_instance(1, 901, 3, 5)).slot_index == 3
    assert (await repo.move_instance(1, 901, 3, 5)).slot_index == 3


async def test_discard_instance_is_idempotent_and_rejects_bound(repo, pool) -> None:
    await repo.grant_instances(1, [901, 902], [10, 11], 5, "g")
    await repo.discard_instance(1, 901)
    await repo.discard_instance(1, 901)  # 幂等 no-op
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("UPDATE player_item_instance SET bound=1 WHERE instance_id=902")
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.discard_instance(1, 902)
    assert ei.value.code == errcode.ErrInventoryInstanceBound


async def test_instance_ownership_is_scoped_to_the_owner(repo) -> None:
    """条件里带 player_id 是鉴权的一部分:别人的实例必须查不到、动不了。"""
    await repo.grant_instances(1, [901], [10], 5, "g1")
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.move_instance(2, 901, 0, 5)
    assert ei.value.code == errcode.ErrInventoryItemNotFound

    owned = await repo.check_instances_owned(
        2, [InstanceOwnershipQuery(instance_id=901, item_config_id=10)]
    )
    assert owned == []


async def test_check_instances_owned_requires_config_match(repo) -> None:
    """instance_id 对但 item_config_id 不对 = 漂移 / 伪造组合,不算持有。"""
    await repo.grant_instances(1, [901], [10], 5, "g")
    assert (
        await repo.check_instances_owned(
            1, [InstanceOwnershipQuery(instance_id=901, item_config_id=999)]
        )
        == []
    )
    got = await repo.check_instances_owned(
        1, [InstanceOwnershipQuery(instance_id=901, item_config_id=10)]
    )
    assert [i.instance_id for i in got] == [901]


async def test_sell_instance_deletes_and_credits_atomically(repo) -> None:
    await repo.grant_instances(1, [901], [10], 5, "g")
    gold, already = await repo.sell_instance(
        1, 901, 10, 180, "s", "sell instance=901 item=10 gold=180"
    )
    assert (gold, already) == (180, False)
    assert await repo.list_instances(1) == []
    # 幂等回放:不会再加一次钱。
    assert await repo.sell_instance(1, 901, 10, 180, "s", "sell instance=901 item=10 gold=180") == (
        180,
        True,
    )
    assert (await repo.get_inventory(1))[0] == 180


# ── 邮件 transfer 托管 ────────────────────────────────────────────────────


async def test_escrow_out_then_claim_moves_the_row_exactly_once(repo, pool) -> None:
    """"同一 instance 全局唯一":任一时刻只存在于两表之一。"""
    await repo.grant_instances(1, [901], [10], 5, "g")
    await repo.identify_instance(1, 901, [ItemAttribute(attr_id=3, value=5)])

    rows, already = await repo.escrow_out_instances(1, 2, [901], "ek", "d")
    assert already is False
    assert rows[0].to_player_id == 2 and rows[0].identified is True
    assert await repo.list_instances(1) == []  # 已不在源玩家背包
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT COUNT(*) FROM mail_transfer_escrow")
        assert (await cur.fetchone())[0] == 1

    # 领取:原样搬进领取人实例表,词条逐字节保留(INSERT...SELECT,不经 Go 回写)。
    assert (
        await repo.claim_transfer_instances(
            2, [TransferClaimItem(instance_id=901, item_config_id=10)], 5, "ck", "d"
        )
        is False
    )
    claimed = await repo.list_instances(2)
    assert [i.instance_id for i in claimed] == [901]
    assert claimed[0].attributes == [ItemAttribute(attr_id=3, value=5)]
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT COUNT(*) FROM mail_transfer_escrow")
        assert (await cur.fetchone())[0] == 0


async def test_claim_rejects_wrong_recipient_and_config_drift(repo) -> None:
    """领取只认托管行:收件人不符 / config 漂移 → 整批拒(邮件保持未领取,可重试)。"""
    await repo.grant_instances(1, [901], [10], 5, "g")
    await repo.escrow_out_instances(1, 2, [901], "ek", "d")

    with pytest.raises(errcode.PandoraError) as wrong:
        await repo.claim_transfer_instances(
            3, [TransferClaimItem(instance_id=901, item_config_id=10)], 5, "c1", "d"
        )
    assert wrong.value.code == errcode.ErrInventoryItemNotFound

    with pytest.raises(errcode.PandoraError) as drift:
        await repo.claim_transfer_instances(
            2, [TransferClaimItem(instance_id=901, item_config_id=999)], 5, "c2", "d"
        )
    assert drift.value.code == errcode.ErrInventoryItemNotFound


async def test_release_transfer_escrow_returns_to_source_without_capacity_gate(repo) -> None:
    """释放不设容量闸(资产归还优先):一律以未分配格入包。

    设了容量闸的话,背包满的玩家会让补偿永久失败,资产卡在托管表里谁也拿不到。
    """
    await repo.grant_instances(1, [901], [10], 1, "g")  # capacity=1,已占满
    await repo.escrow_out_instances(1, 2, [901], "ek", "d")
    assert await repo.release_transfer_escrow([901]) == 1
    back = await repo.list_instances(1)
    assert [(i.instance_id, i.slot_index) for i in back] == [(901, -1)]  # slot NULL
    # 行已不在托管表 → 再释放是 0 行 no-op。
    assert await repo.release_transfer_escrow([901]) == 0


async def test_consume_transfer_escrow_only_deletes_and_checks_recipient(repo) -> None:
    """消托管行不物化(bag phase 2):资产已由 journal 入包,这里只删,防双持。"""
    await repo.grant_instances(1, [901], [10], 5, "g")
    await repo.escrow_out_instances(1, 2, [901], "ek", "d")
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.consume_transfer_escrow(3, [901])
    assert ei.value.code == errcode.ErrInventoryItemNotFound
    assert await repo.consume_transfer_escrow(2, [901]) == 1
    assert await repo.list_instances(2) == []  # 不物化
    assert await repo.consume_transfer_escrow(2, [901]) == 0  # 幂等


async def test_escrow_out_replay_missing_rows_fails_closed(repo) -> None:
    """幂等回放时缺行 = 调用方违反 saga 顺序 —— 必须报错而不是当成功。

    当成功会让邮件带着一批不存在的附件发出去,玩家永远领不到。
    """
    await repo.grant_instances(1, [901], [10], 5, "g")
    await repo.escrow_out_instances(1, 2, [901], "ek", "d")
    await repo.consume_transfer_escrow(2, [901])  # 托管行没了
    with pytest.raises(errcode.PandoraError) as ei:
        await repo.escrow_out_instances(1, 2, [901], "ek", "d")
    assert ei.value.code == errcode.ErrInventoryItemNotFound


# ── 保留期清理 ────────────────────────────────────────────────────────────


async def test_sweep_defaults_to_report_only_and_deletes_nothing(repo, pool) -> None:
    """默认只报告不删(§9.24 用户指令):matched 有值、deleted 恒 0。"""
    await repo.grant_items(1, [ItemGrant(item_config_id=1001, count=1)], 0, "k", "d")
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("UPDATE inventory_ledger SET created_at = '2000-01-01'")

    out = await repo.sweep_ledger_before(dbguard.Mode.REPORT_ONLY, 90, 500)
    assert out.matched == 1 and out.deleted == 0
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT COUNT(*) FROM inventory_ledger")
        assert (await cur.fetchone())[0] == 1

    out = await repo.sweep_ledger_before(dbguard.Mode.DELETE, 90, 500)
    assert out.deleted == 1


async def test_sweep_never_touches_active_escrow(repo, pool) -> None:
    """active 行永不清理:EnsureAuctionEscrow 依赖它们的存在性核对遗留订单。

    删了 active 行,那些订单会被判成"从没冻结过"而**重新扣一次活跃资产**。
    """
    await repo.grant_items(10, [ItemGrant(item_config_id=1001, count=5)], 0, "g", "d")
    await repo.freeze_for_order(10, 101, EscrowKind.ITEM, 1001, 5, 0)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("UPDATE auction_escrow SET updated_at = '2000-01-01'")

    out = await repo.sweep_closed_escrow_before(dbguard.Mode.DELETE, 90, 500)
    assert out.matched == 0 and out.deleted == 0
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT COUNT(*) FROM auction_escrow")
        assert (await cur.fetchone())[0] == 1

    await repo.release_escrow(10, 101)  # → closed
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("UPDATE auction_escrow SET updated_at = '2000-01-01'")
    assert (await repo.sweep_closed_escrow_before(dbguard.Mode.DELETE, 90, 500)).deleted == 1
