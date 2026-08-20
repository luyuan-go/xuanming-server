"""MySQL 数据层测试的共享夹具件。

★ 为什么必须**自建库**,而不是假设库已经在(2026-08-19 修)

    CI 发下来的 DSN 是**无库名**的 ——
        tools/scripts/ci_db.ps1:166
        root@tcp(127.0.0.1:<port>)/?parseTime=true&loc=UTC&charset=utf8mb4
    而 deploy/docker-compose.ci-db.yml 的 mysql 服务**没有任何 init 脚本**,
    一个库都不存在。Go 侧的做法是连上去先 `CREATE DATABASE`(见
    services/**/*_mysql_test.go 里成片的那行),Python 侧此前直接拿默认库名去连:

        连接失败 → pytest.skip → 整份数据层测试在 CI 上**一次都没跑过**,而流水线打绿。

    开发机上看不出来:本地那个 pandora-mysql-verify 容器是带
    `-e MYSQL_DATABASE=pandora_owner` 起的,库天然存在。
    「本机绿、CI 静默不覆盖」的经典形状 —— 与 tools/scripts/lib/go_test_skip_audit.ps1
    当初要解决的是同一件事。

★ 库名必须过白名单

    库名来自 DSN(环境变量),而 MySQL 的 DDL **不支持**把库名做成占位参数,
    只能拼进 SQL。所以这里强制它只能是 [A-Za-z0-9_],拼串前先卡住。

★ DSN 不带库名时,**每个进程用自己的独占库**(2026-08-19 补)

    数据层用例用固定表名 + 固定 player_id,且每个用例在 fixture 里 TRUNCATE。
    两个 pytest 进程打同一个库时必然互踩:一边 TRUNCATE、另一边正在断言条数
    → 表现为 1205 锁等待、"上限 5 被突破:成功了 6 个" 这类
    **看起来像业务 bug 的假红**,而代码一个字没变。

    实测(本机同时跑着 5 个 pytest 时):共享库 6 failed / 82s,
    换独占库 18 passed / 4.8s —— 同一份代码。

    Go 侧本来就是每次跑 `CREATE DATABASE <唯一名>`(见 services/**/*_mysql_test.go),
    这里补齐同一做法。DSN 里显式写了库名的仍照用(那是调用方明确要求)。

★ DSN 只能有**一个**来源(2026-08-19 修)

    此前 10 个数据层测试文件各写一份 `os.getenv("PANDORA_TEST_MYSQL_DSN", <默认>)`,
    而两份默认值不是同一个端口:test_player_repo 写 3307(仓库一键栈
    deploy/docker-compose.dev.yml),另外 9 个 + tests/conftest.py 的会话清理写 13306
    (python/README.md 教的那个 verify 容器)。后果两条,都是静默的:

      ① 不设环境变量时,本机起哪个容器就有**另一半**用例跳过而 pytest 退出码 0
         —— 只起 13306 时 player 那 32 条跳过;只起 3307 时另外 9 个文件的 200 条跳过。
         没有任何一种本地环境能让这 232 条全部执行。
      ② conftest 的会话清理打 13306,而 player 在 3307 上建的
         `pandora_test_<pid>_<ts>` **永远删不掉** —— 实测 3307 上已攒了 40+ 个孤儿库。

    Go 侧结构上不可能出这个问题:`services/**/*_mysql_test.go` 的门控路径**零硬编码端口**,
    只读 `os.Getenv("PANDORA_TEST_MYSQL_DSN")`,为空 `t.Skip`、已设但 Ping 不通
    `t.Fatalf("…不允许静默 PASS")`。这里补齐"单一来源"这一半:全部调用点一律用
    本模块的 `MYSQL_DSN`,**不准再在测试文件里写 `os.getenv("PANDORA_TEST_MYSQL_DSN", ...)`**
    (由 tests/test_dependency_gate_contract.py 机械拦住)。

★ 跳过只允许"环境没起来",代码错了必须冒红(2026-08-19 修)

    10 个夹具原先一律是 `except Exception -> pytest.skip("MySQL 不可用")`,而 try 块里
    包着**我们自己的代码**(`ensure_database`、`cfg[...]` 取键、`create_pool(**conn_kw)`)。
    真事:`parse_go_dsn` 加了 `net` 字段之后 splat 抛 `TypeError`,被洗成"MySQL 不可用",
    32 条真库用例悄悄停跑而套件报绿;当时只在 test_player_repo 里补了
    `except TypeError: raise`,**只白名单了引发那次事故的那一个异常类型**,
    换成 AttributeError / ValueError / KeyError 照样被洗白
    (`ensure_database` 的库名白名单本身就抛 ValueError,走的正是这条路)。

    所以判据改成**放行集**而不是**黑名单**:只有"环境类"异常才允许 skip,其余原样抛。
    放行集按实测定,不按直觉:asyncmy 把"端口没人听"和"口令错"**统统**包成
    `asyncmy.errors.OperationalError`(实测),socket 层的 ConnectionRefusedError
    根本不会冒到调用方,所以照字面写 `ConnectionError` 是无效护栏。
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
import time

# 库名白名单。DDL 无法参数化库名,只能拼串,所以拼之前必须先卡。
_DB_NAME_RE = re.compile(r"^[A-Za-z0-9_]{1,64}$")

# 与 deploy/mysql-init/*.sql 一致的字符集/排序规则。不写死会跟着服务端默认走,
# utf8mb4_general_ci 与 utf8mb4_0900_ai_ci 对中文比较的结果不同。
_CHARSET_CLAUSE = "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"


# ★ 解析器从 pandorapy.mysqlx 复用,不在这里再写一份。
# 两份实现必然漂移(一份改了另一份没改,而两边都"跑得过"),
# 而 DSN 解析错的表现是连错库 —— 测试反而更容易在错的库上跑绿。
from pandorapy.mysqlx import parse_go_dsn as _parse_go_dsn  # noqa: E402

# 本进程的独占库名。只在 DSN 不带库名时用;一个 pytest 进程内全局一致,
# 三个数据层测试文件共用它(否则每个文件各建一个库,清理更麻烦)。
_SESSION_DB = f"pandora_test_{os.getpid()}_{int(time.time())}"


def session_db_name() -> str:
    """本进程的独占库名(供会话级清理用)。"""
    return _SESSION_DB


def parse_go_dsn(dsn: str, *, default_db: str) -> dict:  # noqa: ARG001
    """解析 Go 风格 DSN;不带库名时用本进程的独占库。

    ★ default_db 参数保留只为兼容调用点写法,**不再作为回落值** ——
    共享一个固定库名正是"两个 pytest 同时跑就假红"的根因,见模块头。
    """
    cfg = _parse_go_dsn(dsn, default_db="")
    if not cfg["db"]:
        cfg["db"] = _SESSION_DB
    return cfg


async def drop_database(asyncmy, cfg: dict, *, timeout: float = 8.0) -> None:
    """删掉本进程建的独占库。会话结束时调;失败静默(清理不该让结果变红)。

    ★ 只删**本进程生成的那个名字** —— 绝不删调用方在 DSN 里显式指定的库。
    """
    db = cfg.get("db", "")
    if db != _SESSION_DB or not _DB_NAME_RE.match(db):
        return
    try:
        conn = await asyncio.wait_for(
            asyncmy.connect(
                host=cfg["host"], port=cfg["port"], user=cfg["user"],
                password=cfg["password"], autocommit=True,
            ),
            timeout=timeout,
        )
    except Exception:  # noqa: BLE001
        return
    try:
        async with conn.cursor() as cur:
            # 先查存在再删:库多半没被建出来(没有 MySQL 时数据层用例整体 skip),
            # 而 `DROP DATABASE IF EXISTS` 对不存在的库会让 MySQL 回一条 Note,
            # asyncmy 把它打成 warning —— 每次跑测试都刷一条无意义的告警。
            await cur.execute("SHOW DATABASES LIKE %s", (db,))
            if await cur.fetchone():
                await cur.execute(f"DROP DATABASE `{db}`")
    except Exception:  # noqa: BLE001
        pass
    finally:
        with contextlib.suppress(Exception):
            await conn.ensure_closed()


async def ensure_database(asyncmy, cfg: dict, *, timeout: float = 8.0) -> None:
    """用**不带库名**的连接把目标库建出来(已存在则什么都不做)。

    连不上直接把异常抛给调用方 —— 由调用方决定是 skip 还是失败,
    这里不吞异常(吞了就又变成"跳过等于通过")。
    """
    db = cfg["db"]
    if not _DB_NAME_RE.match(db):
        raise ValueError(f"库名不合法,拒绝拼进 DDL:{db!r}")
    conn = await asyncio.wait_for(
        asyncmy.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=cfg["password"],
            autocommit=True,
        ),
        timeout=timeout,
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db}` {_CHARSET_CLAUSE}")
    finally:
        await conn.ensure_closed()


# ── 唯一 DSN 来源 ────────────────────────────────────────────────────────
#
# 默认值取 13306(python/README.md 里"起依赖"那三行 docker run 用的端口,
# 文档原话是"端口与测试默认值一致,起了就不用配任何环境变量")。
# CI 不吃这个默认值:tools/scripts/ci_backend.ps1 从 ci_db 状态文件导入
# PANDORA_TEST_MYSQL_DSN,永远显式覆盖。
_DEFAULT_DSN = "root:pandora_dev_root@tcp(127.0.0.1:13306)/"

MYSQL_DSN = os.getenv("PANDORA_TEST_MYSQL_DSN", _DEFAULT_DSN)


def _env_failure_types() -> tuple[type[BaseException], ...]:
    """"环境没起来"的异常放行集 —— 只有这几类才允许 skip。

    实测(2026-08-19,asyncmy 0.2.x + MySQL 8.4):
      - 端口没人听  → asyncmy.errors.OperationalError(2003)
      - 口令错      → asyncmy.errors.OperationalError(1045)
    也就是说 `ConnectionRefusedError` / `socket.timeout` 这些直觉上的类型
    **根本不会冒到调用方**,照字面写进 except 等于没写。
    `TimeoutError` 留着是因为夹具用 `asyncio.wait_for(..., timeout=8)` 包了连接;
    `OSError` 留着是给 DNS / 网络栈那一层兜底。

    刻意**不**收 `ProgrammingError` / `InterfaceError`:前者是我们的 SQL 写错了,
    后者多半是连接被用错了,两者都该冒红。
    """
    types: list[type[BaseException]] = [TimeoutError, OSError]
    try:
        import asyncmy.errors as _ae
    except Exception:  # noqa: BLE001  —— 没装 asyncmy 时调用方早已 importorskip
        return tuple(types)
    types.append(_ae.OperationalError)
    return tuple(types)


@contextlib.contextmanager
def skip_only_if_mysql_is_down(cfg: dict, what: str, *, hint: str = ""):  # noqa: ANN201
    """把"连库"这一段包起来:**只有**环境类异常才 skip,其余原样抛出去变红。

    ★ 为什么不能直接写 `except Exception -> pytest.skip`

        try 块里包的是我们自己的代码(`ensure_database`、`cfg[...]` 取键、
        `create_pool(**conn_kw)`)。任何 KeyError / AttributeError / TypeError /
        ValueError 都会被报成"MySQL 不可用",跳过文案里的异常 repr 短得像连接噪声
        (那次事故里只有 `('db')` 三个字符),看上去完全不像自己的 bug。
        后果是**整批真库用例停跑而退出码 0**,而这批用例正是唯一打真 MySQL 的覆盖。

    ★ Go 那边为什么是另一种写法

        `services/**/*_mysql_test.go` 里只有一个 Skip 条件:`PANDORA_TEST_MYSQL_DSN`
        没设。DSN 解析失败 / DSN 带库名 / Ping 不可达 / CREATE DATABASE 失败
        一律 `t.Fatalf`(注释原文:"已设测试 DSN 但 MySQL 不可达(不允许静默 PASS)")。
        Go 侧压根没有"把异常转成跳过"这个动作。Python 这里保留"环境没起来就 skip"
        是为了本机没起容器时还能跑纯逻辑用例,但**放行集必须是白名单**,
        不能是"除了我上次踩到的那一个之外都跳过"。

    ⚠️ 文案里的 "MySQL" 三个字母是**隐式契约**:tools/scripts/ci_backend.ps1:311 用
       `-match 'MySQL|TiDB'` 把这类跳过在 `-RequireDbTests` 下升级成门禁失败。
       改文案时别把它删了(测试见 tests/test_dependency_gate_contract.py)。
    """
    import pytest

    try:
        yield
    except _env_failure_types() as exc:
        pytest.skip(
            f"MySQL 不可用 @ {cfg.get('host')}:{cfg.get('port')} ({exc}) —— "
            f"{what}整体跳过(不假装通过)。{hint}"
        )
