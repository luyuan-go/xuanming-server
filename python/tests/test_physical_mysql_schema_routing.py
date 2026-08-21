"""中心 MySQL 每台策划机物理 schema 的路由回归。"""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid

import pytest

from pandorapy import dbguard
from pandorapy.services.auction import main as auction_main
from pandorapy.services.auction import repo as auction_repo
from pandorapy.services.auction import retention as auction_retention
from pandorapy.services.battle_result import main as battle_main
from pandorapy.services.chat import data as chat_data
from pandorapy.services.chat import main as chat_main
from pandorapy.services.data_service import main as data_main
from pandorapy.services.friend import main as friend_main
from pandorapy.services.guild import main as guild_main
from pandorapy.services.inventory import budgets as inventory_budgets
from pandorapy.services.inventory import main as inventory_main
from pandorapy.services.leaderboard import main as leaderboard_main
from pandorapy.services.login import data as login_data
from pandorapy.services.login import main as login_main
from pandorapy.services.mail import main as mail_main
from pandorapy.services.mission import main as mission_main
from pandorapy.services.owner import main as owner_main


class _Cursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, object]] = []
        self.rowcount = 0
        self.did_execute = asyncio.Event()

    async def __aenter__(self) -> "_Cursor":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def execute(self, sql: str, params: object = None) -> None:
        self.executed.append((sql, params))
        self.did_execute.set()

    async def fetchall(self) -> list:
        return []

    async def fetchone(self) -> tuple[int]:
        return (0,)


class _Connection:
    def __init__(self) -> None:
        self.cursor_obj = _Cursor()

    async def __aenter__(self) -> "_Connection":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    def cursor(self) -> _Cursor:
        return self.cursor_obj


class _Pool:
    def __init__(self) -> None:
        self.connection = _Connection()

    def acquire(self) -> _Connection:
        return self.connection


async def test_data_service_capacity_guard_queries_physical_schema() -> None:
    pool = _Pool()
    await data_main._capacity_round(pool, "pandora_player_ws_alpha")

    _, params = pool.connection.cursor_obj.executed[0]
    assert params[0] == "pandora_player_ws_alpha"
    assert params[0] != data_main.DEFAULT_DB


def _guard_coro(module, shape: str, pool: _Pool, schema: str):  # noqa: ANN001
    guard = module._run_capacity_guard
    if shape == "interval":
        return guard(pool, schema, 3600.0)
    if shape == "auction":
        return guard(pool, schema, 7)
    if shape == "budgets":
        # inventory 是唯一一个**两个库**的服务(trade + bag),预算必须按库传参:
        # 共用一份会让 bag 的三个 blob 列(深度失控的高风险点)完全没有巡检。
        # 本用例只验"查的是物理 schema",预算内容不参与断言,给 trade 的即可。
        return guard(pool, schema, 3600.0, inventory_budgets.trade_budgets())
    return guard(pool, schema)


@pytest.mark.parametrize(
    ("name", "module", "shape", "canonical"),
    [
        ("inventory", inventory_main, "budgets", inventory_main.TRADE_DB),
        ("chat", chat_main, "interval", chat_main.CHAT_DB),
        ("auction", auction_main, "auction", auction_main.DB_SCHEMA),
        ("guild", guild_main, "interval", guild_main.GUILD_DB),
        ("battle_result", battle_main, "plain", battle_main.BATTLE_DB),
        ("friend", friend_main, "interval", friend_main.DB_SCHEMA),
        ("login", login_main, "plain", login_main.ACCOUNT_DB),
        ("leaderboard", leaderboard_main, "interval", leaderboard_main.DB_SCHEMA),
        ("mission", mission_main, "interval", mission_main.MISSION_DB),
        ("owner", owner_main, "interval", "pandora_owner"),
        ("mail", mail_main, "interval", mail_main.MAIL_DB),
    ],
)
async def test_capacity_guard_queries_each_physical_schema(
    name: str, module, shape: str, canonical: str  # noqa: ANN001
) -> None:
    physical = f"pandora_{name}_ws_alpha"
    pool = _Pool()
    task = asyncio.create_task(_guard_coro(module, shape, pool, physical))
    try:
        await asyncio.wait_for(pool.connection.cursor_obj.did_execute.wait(), timeout=1)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    _, params = pool.connection.cursor_obj.executed[0]
    assert params[0] == physical
    assert params[0] != canonical


def _qualified_schemas(pool: _Pool) -> list[str]:
    return [sql.split("`")[1] for sql, _ in pool.connection.cursor_obj.executed]


async def test_chat_cleanup_qualifies_only_the_physical_schema() -> None:
    pool = _Pool()
    repo = chat_data.MySQLPrivateRepo(pool, db="pandora_social_ws_alpha")
    await repo.sweep_messages_before(dbguard.Mode.REPORT_ONLY, 100, 10)

    assert _qualified_schemas(pool) == ["pandora_social_ws_alpha"]
    assert "pandora_social" not in _qualified_schemas(pool)


async def test_login_device_cleanup_qualifies_only_the_physical_schema() -> None:
    pool = _Pool()
    await login_data.sweep_stale_devices(
        pool,
        dbguard.Mode.REPORT_ONLY,
        90,
        10,
        schema="pandora_account_ws_alpha",
    )

    assert _qualified_schemas(pool) == ["pandora_account_ws_alpha"]
    assert "pandora_account" not in _qualified_schemas(pool)


async def test_inventory_main_binds_cleanup_repo_to_parsed_physical_schema() -> None:
    pool = _Pool()
    repo = inventory_main._new_inventory_repo(
        pool, {"db": "pandora_trade_ws_alpha"}
    )
    await repo.sweep_ledger_before(dbguard.Mode.REPORT_ONLY, 90, 10)
    await repo.sweep_closed_escrow_before(dbguard.Mode.REPORT_ONLY, 90, 10)

    assert _qualified_schemas(pool) == [
        "pandora_trade_ws_alpha",
        "pandora_trade_ws_alpha",
    ]
    assert "pandora_trade" not in _qualified_schemas(pool)


async def test_auction_retention_keeps_each_pool_bound_to_its_physical_schema() -> None:
    pools = [_Pool(), _Pool()]
    schemas = ["pandora_auction_ws_alpha", "pandora_auction_ws_beta"]
    router = auction_repo.DBRouter(pools, schemas=schemas)

    await auction_retention.sweep_retention(
        router, dbguard.Mode.REPORT_ONLY, cutoff_ms=100, limit=10
    )

    assert _qualified_schemas(pools[0]) == [schemas[0], schemas[0], schemas[0]]
    assert _qualified_schemas(pools[1]) == [schemas[1], schemas[1], schemas[1]]
    assert "pandora_auction" not in _qualified_schemas(pools[0])
    assert "pandora_auction" not in _qualified_schemas(pools[1])


async def test_inventory_cleanup_real_mysql_cannot_touch_canonical_sentinel() -> None:
    """窄权限账号只授权 physical；清理 canonical 会立即拒绝且哨兵必须保留。"""
    asyncmy = pytest.importorskip("asyncmy")
    from mysqlfixture import MYSQL_DSN, skip_only_if_mysql_is_down
    from pandorapy import mysqlx

    admin_cfg = mysqlx.parse_go_dsn(MYSQL_DSN)
    suffix = f"{os.getpid()}_{uuid.uuid4().hex[:8]}"
    physical = f"pandora_rt_phys_{suffix}"
    canonical = f"pandora_rt_canon_{suffix}"
    app_user = f"prt_{suffix}"
    app_password = uuid.uuid4().hex
    for identifier in (physical, canonical, app_user):
        mysqlx.require_mysql_identifier(identifier, kind="test identifier")

    admin = None
    app_pool = None
    with skip_only_if_mysql_is_down(admin_cfg, "physical schema 路由真库测试"):
        admin = await asyncmy.connect(
            host=admin_cfg["host"],
            port=admin_cfg["port"],
            user=admin_cfg["user"],
            password=admin_cfg["password"],
            autocommit=True,
        )
    try:
        async with admin.cursor() as cur:
            for schema in (physical, canonical):
                await cur.execute(f"CREATE DATABASE `{schema}`")  # noqa: S608
                await cur.execute(  # noqa: S608
                    f"CREATE TABLE `{schema}`.`inventory_ledger` ("
                    "id BIGINT PRIMARY KEY, created_at DATETIME(6) NOT NULL)"
                )
                await cur.execute(  # noqa: S608
                    f"INSERT INTO `{schema}`.`inventory_ledger` VALUES "
                    "(1, UTC_TIMESTAMP(6) - INTERVAL 100 DAY)"
                )
            await cur.execute(  # noqa: S608
                f"CREATE USER '{app_user}'@'%' IDENTIFIED BY '{app_password}'"
            )
            await cur.execute(  # noqa: S608
                f"GRANT SELECT, DELETE ON `{physical}`.* TO '{app_user}'@'%'"
            )

        app_pool = await asyncmy.create_pool(
            host=admin_cfg["host"],
            port=admin_cfg["port"],
            user=app_user,
            password=app_password,
            db=physical,
            minsize=1,
            maxsize=1,
            autocommit=True,
        )
        repo = inventory_main._new_inventory_repo(app_pool, {"db": physical})
        out = await repo.sweep_ledger_before(dbguard.Mode.DELETE, 90, 10)
        assert out.deleted == 1

        counts: list[int] = []
        async with admin.cursor() as cur:
            for schema in (physical, canonical):
                await cur.execute(  # noqa: S608
                    f"SELECT COUNT(*) FROM `{schema}`.`inventory_ledger`"
                )
                row = await cur.fetchone()
                counts.append(int(row[0]))
        assert counts == [0, 1]
    finally:
        if app_pool is not None:
            app_pool.close()
            await app_pool.wait_closed()
        if admin is not None:
            async with admin.cursor() as cur:
                await cur.execute(f"DROP USER IF EXISTS '{app_user}'@'%'")  # noqa: S608
                for schema in (physical, canonical):
                    await cur.execute(f"DROP DATABASE IF EXISTS `{schema}`")  # noqa: S608
            await admin.ensure_closed()
