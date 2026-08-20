"""MySQL / TiDB 连接 —— 对应 Go 侧 pkg/mysqlx。

选型:`asyncmy`(全 async)+ 回退 `aiomysql`。
    grpc.aio 下**整条链路必须 async** —— 任何一个同步 DB 调用都会阻塞整个 event loop,
    把并发打回单请求串行。这是迁 Python 最容易在压测时才暴露的性能陷阱,
    所以驱动层从一开始就不给同步选项。

TiDB 检测(对应 pkg/mysqlx/backend_check.go):
    TiDB 对客户端就是 MySQL 线协议,全仓唯一的 TiDB 专属代码就是解析 VERSION() 里的
    `-TiDB-vX.Y.Z`。login 等服务用 `require_tidb: true` 断言权威库确实是 TiDB ——
    误连到普通 MySQL 会让依赖 TiDB 特性的逻辑(如无 gap 锁前提下的守卫行写法)
    行为漂移,而且不报错。
"""

from __future__ import annotations

import re
import ssl
from urllib.parse import parse_qsl

from pandorapy import errcode

# 与 Go 侧 tidbVersionRe 完全一致。
_TIDB_VERSION_RE = re.compile(r"-TiDB-v(\d+)\.(\d+)\.(\d+)")
_MYSQL_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9_]{1,64}\Z")

# MySQL 错误码:数据被截断 / 超长。严格模式下会以这些码报错而不是静默砍断。
ER_DATA_TOO_LONG = 1406
ER_DUP_ENTRY = 1062
ER_LOCK_DEADLOCK = 1213


class NotTiDBError(RuntimeError):
    """要求 TiDB 但实际连的不是。调用方打 account_backend_not_tidb 后退出。"""


def require_mysql_identifier(value: str, *, kind: str = "identifier") -> str:
    """返回经白名单确认可安全放进反引号限定名的 MySQL 标识符。"""
    if not _MYSQL_IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"MySQL {kind} 必须是 1..64 位 ASCII 字母、数字或下划线"
        )
    return value


def parse_tidb_version(version_string: str) -> tuple[int, int, int] | None:
    """从 VERSION() 结果解析 TiDB 版本。不是 TiDB 返回 None。

    形如:`8.0.11-TiDB-v8.5.0` → (8, 5, 0)
    """
    m = _TIDB_VERSION_RE.search(version_string)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


async def assert_tidb(conn, *, min_major: int = 0, min_minor: int = 0) -> tuple[int, int, int]:  # noqa: ANN001
    """断言连的是 TiDB(可选最低版本)。对应 Go 的 mysqlx 后端校验。

    误连普通 MySQL 不会报错、只会让依赖 TiDB 语义的逻辑悄悄跑偏 ——
    典型的是 TiDB 无 gap 锁,`FOR UPDATE` 在零行时不加锁,所以 friend / mission
    的限额校验必须先锁守卫行。在 MySQL 上那套写法是多余但无害的,
    反过来(以为是 TiDB 其实是 MySQL)才危险。
    """
    async with conn.cursor() as cur:
        await cur.execute("SELECT VERSION()")
        row = await cur.fetchone()
    version_string = (row[0] if row else "") or ""
    parsed = parse_tidb_version(version_string)
    if parsed is None:
        raise NotTiDBError(
            f"权威库要求 TiDB(require_tidb=true),实际 VERSION()={version_string!r}"
        )
    if (parsed[0], parsed[1]) < (min_major, min_minor):
        raise NotTiDBError(
            f"TiDB 版本过低:要求 >= v{min_major}.{min_minor},实际 "
            f"v{parsed[0]}.{parsed[1]}.{parsed[2]}"
        )
    return parsed


def is_deadlock(exc: BaseException) -> bool:
    """判断是否 MySQL 1213 死锁(可重试)。

    Go 侧靠 errors.As 沿链检出 *mysql.MySQLError 再看 Number;Python 侧驱动把错误码
    放在 args[0]。死锁**必须**可重试而不是当成业务失败返回给客户端 ——
    TiDB 下并发事务撞死锁是正常现象,不重试会让玩家看到随机失败。
    """
    args = getattr(exc, "args", ())
    return bool(args) and args[0] == ER_LOCK_DEADLOCK


def is_duplicate_entry(exc: BaseException) -> bool:
    """判断是否唯一键冲突(1062)。

    这是幂等实现的主力:插入幂等键冲突 = 这次操作之前已经做过,
    应当返回"已完成"而不是报错(与 Go 侧各服务的幂等写法一致)。
    """
    args = getattr(exc, "args", ())
    return bool(args) and args[0] == ER_DUP_ENTRY


def is_data_too_long(exc: BaseException) -> bool:
    """判断是否 1406 数据超长。

    只有在 sql_mode 含 STRICT_TRANS_TABLES 时才会抛这个错;非严格模式下会**静默截断**
    —— 这正是 dbguard.assert_strict_mode 必须在启动期 fail-fast 的原因。
    """
    args = getattr(exc, "args", ())
    return bool(args) and args[0] == ER_DATA_TOO_LONG


def map_db_error(exc: BaseException) -> int:
    """把数据库异常映射成业务错误码。

    刻意**不**把死锁映射成错误码 —— 死锁应当在数据层重试,不该走到这里。
    走到这里的死锁说明重试已耗尽,那才是真的内部错误。
    """
    if is_duplicate_entry(exc):
        return errcode.ErrAlreadyExists
    if is_data_too_long(exc):
        # 对客户端是"参数非法",服务端另有 WARN 暴露真实原因 ——
        # 不把列容量这种内部细节泄露给客户端。
        return errcode.ErrInvalidArg
    return errcode.ErrInternal


async def check_tables(conn, migration_hint: str, *tables: str) -> None:  # noqa: ANN001
    """建表校验 —— 对应 Go 的 mysqlx.CheckTables。

    ★ 为什么是 fail-fast 而不是"跑起来再说":

    后建的库(如 pandora_owner)在**既有 volume 上不会自动重放 init SQL**。缺表时
    服务照常启动、照常 Ready、日志毫无痕迹,直到第一个真实请求打进来才报
    "Table doesn't exist" —— 那时故障已经在玩家面上了。启动就拒,把发现时点
    提前到部署那一刻。

    错误信息必须带 migration_hint(该跑哪个 SQL),否则值班的人拿到"缺表"
    还得自己翻仓库找建表脚本。
    """
    if not tables:
        return
    placeholders = ",".join(["%s"] * len(tables))
    sql = (
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = DATABASE() AND table_name IN ({placeholders})"
    )
    async with conn.cursor() as cur:
        await cur.execute(sql, tuple(tables))
        rows = await cur.fetchall()
    # information_schema 的表名大小写随平台不同(Windows 下 MySQL 默认 lower_case=1),
    # 两侧都归一化再比,与 Go 侧 strings.ToLower 同口径。
    present = {str(r[0]).lower() for r in rows}
    missing = [t for t in tables if t.lower() not in present]
    if missing:
        raise RuntimeError(
            f"缺表 {', '.join(missing)};请先执行 {migration_hint}"
        )


async def assert_column_exists(
    conn, table: str, column: str, *, hint: str = ""  # noqa: ANN001
) -> None:
    """列存在校验 —— 对应 Go 的 data.AssertSourceRevisionColumn 那一类闸。

    ★ 用 information_schema 而不是 `SELECT <col> FROM <table> LIMIT 1`:
    后者在**空表**上不会报错(MySQL 先做行扫描,零行直接返回空集),
    于是这道闸在新装环境上永远是绿的 —— 而新装环境恰恰是最可能漏 DDL 的。

    缺列的后果与缺表同级但更隐蔽:SELECT 里引用了该列 ⇒ **每一次** RPC 都以
    Error 1054 失败,而启动日志一行异常都没有(INC-20260818-003 就是这么发生的:
    报错只在 owner.log 里,err.log 干干净净)。
    """
    q = (
        "SELECT COUNT(*) FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s"
    )
    async with conn.cursor() as cur:
        await cur.execute(q, (table, column))
        row = await cur.fetchone()
    if not row or int(row[0]) == 0:
        raise RuntimeError(
            f"{table} 缺列 {column}"
            + (f";{hint}" if hint else "")
        )


#: `user:pass@net(addr)/db?params`。net 只认 Go 支持且我们真会用到的两种。
_DSN_RE = re.compile(
    r"^(?P<user>[^:@]*)(?::(?P<pw>[^@]*))?@(?P<net>tcp|unix)\((?P<addr>[^)]*)\)/(?P<db>.*)$"
)


def parse_go_dsn(dsn: str, *, default_db: str = "") -> dict:
    """解析 Go 风格 DSN `user:pass@tcp(host:port)/db?params`。

    ★ 为什么必须解析而不是直接用:同一份 etc/*.yaml 要**同时**喂给 Go 版和 Python 版
    (这是本次迁移的前提),而 yaml 里的 dsn 是 go-sql-driver 的格式,
    aiomysql / asyncmy 都不认。所以解析放在这里,配置一个字节都不用改。

    无库名(CI 下发的形态)时回落 default_db。
    """
    m = _DSN_RE.match(dsn.strip())
    if m is None:
        # ★ 解析不了必须**抛**,绝不能回落默认值。
        #
        # 原实现是 `dsn.partition("@tcp(")` —— 对不含 `@tcp(` 的 DSN,partition
        # 返回 `(dsn, "", "")`,于是 host 回落 `127.0.0.1`、port 回落 3306、
        # db 变空串。喂一份 `user:pw@unix(/var/run/mysqld.sock)/pandora_x` 进去,
        # 得到的是 **127.0.0.1:3306 上的空库名**,而 socket 路径被整个吞进
        # password 字段 —— 没有任何错误。
        #
        # 这条路径上有 16 个服务的全部 DSN(44 处调用)。静默连错库是本次迁移
        # 明确要抓的那类缺陷:配置写错了,服务照常起来,连的却不是你以为的那个库。
        raise ValueError(
            f"无法解析 MySQL DSN(期望 `user:pass@tcp(host:port)/db` 或 "
            f"`user:pass@unix(/path/to.sock)/db`): {mask_dsn(dsn)}"
        )
    user, password, net, addr, dbpart = (
        m.group("user"), m.group("pw") or "", m.group("net"),
        m.group("addr"), m.group("db") or "",
    )
    db_name, separator, query = dbpart.partition("?")
    if separator:
        query_keys = {key.casefold() for key, _ in parse_qsl(query, keep_blank_values=True)}
        if query_keys & {"tls", "ssl"}:
            raise ValueError(
                "Python MySQL DSN 不接受 tls/ssl bool 或驱动模式；"
                "请用 tls_ca_file + tls_server_name 生成显式 SSLContext"
            )
    if net == "unix":
        # Go 的 mysql.ParseDSN 对 unix 保留整条 socket 路径当 Addr。
        host, port = addr, 0
    else:
        if ":" in addr:
            host, _, port_s = addr.rpartition(":")
        else:
            host, port_s = addr, ""      # `@tcp(host)/db`,没写端口
        host = host or "127.0.0.1"       # `@tcp(:3306)/db` —— Go 也回落 127.0.0.1
        port = int(port_s or 3306)
    resolved_db = db_name or default_db
    if resolved_db:
        require_mysql_identifier(resolved_db, kind="schema")
    return {
        "user": user,
        "password": password,
        "net": net,
        "host": host,
        "port": port,
        "db": resolved_db,
    }


def mask_dsn(dsn: str) -> str:
    """把 DSN 里的口令抹掉再打日志 —— 对应 Go 侧 owner main.go 的 maskDSN。

    连接串是最容易被顺手打进日志的凭据。抹掉的是 `user:PASSWORD@` 中间那段,
    其余原样保留(排障要看的是连到了哪台哪个库)。
    """
    at = dsn.rfind("@")
    if at < 0:
        return dsn
    head = dsn[:at]
    colon = head.find(":")
    if colon < 0:
        return dsn
    return head[: colon + 1] + "***" + dsn[at:]


# ─────────────────── 按配置建连接池(对齐 Go 的 MustNewClient)──────────────────
#
# 为什么要有这个共享入口:owner 的 main.py 当初直接手写 `asyncmy.create_pool(host=...,
# port=..., user=..., password=..., db=...)`,只传了连接身份、**四个池参数一个没接**。
# 后来 data_service / leaderboard / mail 照着这份模板抄,于是同一个缺口复制了四份。
#
# 缺口的形状是本仓最典型的那一类:yaml 里明明白白写着 `conn_max_lifetime: 30m`、
# Go 侧读它并 SetConnMaxLifetime,Python 侧**不读、不报错、行为不同**。
# 具体后果:没有 pool_recycle 的长空闲连接会撞上 MySQL 的 wait_timeout 被服务端断掉,
# 而客户端并不知道 —— **下一条业务 SQL 才暴露**,表现为随机的 "Lost connection"。
# 低峰期尤其容易触发(空闲久),而低峰期恰恰是最不该有报错的时候。

# Go 侧 pkg/mysqlx 的默认值(mysqlx.go:29-34)。留空时两边必须回落到同一组数,
# 否则"都没配"的默认部署上两栈的池行为就不同。
DEFAULT_MAX_OPEN_CONNS = 32
DEFAULT_CONN_MAX_LIFETIME_SEC = 30 * 60
DEFAULT_PING_TIMEOUT_SEC = 3.0


def pool_kwargs(conf, dsn: dict, *, autocommit: bool) -> dict:  # noqa: ANN001
    """把 `node.mysql_client` 翻译成 asyncmy.create_pool 的参数。

    conf 是 `pandorapy.config.MySQLConf`;dsn 是 `parse_go_dsn` 的结果(连接身份)。

    ★ `autocommit` **必须显式传,没有默认值** —— 各服务的正确取值不同,而取错了
    两边都不报错:

      - `True`(Go `database/sql` 的默认语义):适合以单条语句为主、显式事务用
        `begin()` 包起来的数据层。mail 就是这一档,它的注释写明了理由 ——
        建成 False 时 MySQL 会在**每条 SELECT** 上隐式开事务并一直挂着(没人 COMMIT),
        连接归池被复用时 REPEATABLE READ 的快照还停在上一次,**读到陈旧数据且零报错**。
      - `False`:适合每条写路径都自己 commit、且乐观锁 CAS 的 rowcount 判定必须
        落在同一事务里的数据层(owner / data_service / leaderboard 是这一档)。
        选它的前提是**只读路径也必须 commit 或 rollback**,否则会踩上面那个陈旧快照。

    给一个默认值就等于替所有服务做了一个它们并不一致的决定,所以这里不给。

    映射关系(左 Go / 右 asyncmy):
        MaxOpenConns     → maxsize          留空 32
        MaxIdleConns     → (无公开等价物)  绝不能冒充 minsize
        (启动预建数)     → minsize          固定 0
        ConnMaxLifetime  → pool_recycle(秒) 留空 30m。**这一个最要紧**,见上面注释
        PingTimeout      → connect_timeout  留空 3s。Go 的 Ping 是有界的;
                           asyncmy 默认 10s,启动期连不上时两边"多久才报错"不同
        ConnMaxIdleTime  → (无对应)        asyncmy 没有等价物,见下

    ⚠️ `conn_max_idle_time` 在 asyncmy 里**没有对应参数**(Go 是
    `SetConnMaxIdleTime`,按空闲时长回收)。它在 Go 侧是 opt-in、留空即不生效,
    所以留空时两边一致;**配了非零值时 Python 侧不生效**。这里不静默忽略 ——
    由 `assert_pool_conf_supported` 显式拒启,理由见那个函数。
    """
    ca_file = conf.tls_ca_file.strip()
    server_name = conf.tls_server_name.strip()
    if bool(ca_file) != bool(server_name):
        raise ValueError(
            "MySQL tls_ca_file and tls_server_name must be configured together"
        )
    if ca_file:
        if dsn.get("net") != "tcp":
            raise ValueError(
                f"MySQL TLS requires tcp DSN, got network {dsn.get('net')!r}"
            )
        host = str(dsn.get("host") or "")
        if host.casefold() != server_name.casefold():
            raise ValueError(
                f"MySQL DSN host {host!r} must equal tls_server_name {server_name!r}"
            )

    max_open = conf.max_open_conns if conf.max_open_conns > 0 else DEFAULT_MAX_OPEN_CONNS
    life = conf.conn_max_lifetime_td().total_seconds()
    if life <= 0:
        life = DEFAULT_CONN_MAX_LIFETIME_SEC
    ping = conf.ping_timeout_td().total_seconds()
    if ping <= 0:
        ping = DEFAULT_PING_TIMEOUT_SEC

    # ★ **显式**挑字段,不用 `**dsn` 整个 splat。
    #
    # splat 的问题是:`parse_go_dsn` 的返回值一旦多一个键,就会原样漏进
    # `asyncmy.create_pool()` 这个第三方 API 并 TypeError。真事:给 DSN 解析加
    # `net` 字段(为了支持 unix socket)那次,19 个服务里凡是连库的全部起不来,
    # 报的是 `mysql_init_failed` —— 与"DSN 解析"看不出任何关系。
    #
    # 显式列字段等于把"我们这边的解析结果"和"第三方接受的参数"解耦:
    # 前者以后随便加,后者只在这里翻译一次。
    kwargs = {
        "user": dsn["user"],
        "password": dsn["password"],
        "db": dsn["db"],
        # asyncmy 的 minsize 是建池时立即建立的连接数，不是 Go 的 MaxIdleConns。
        # asyncmy 没有公开的“最多保留多少空闲连接”参数；固定 0 表示不预热，
        # 上限只由 maxsize 约束，不能用一个名称相似但语义不同的参数假装已实现。
        "minsize": 0,
        "maxsize": max_open,
        "pool_recycle": int(life),
        "connect_timeout": ping,
        "autocommit": autocommit,
    }
    if dsn.get("net") == "unix":
        # Go 的 `unix(/path/to.sock)` → asyncmy 的 unix_socket=
        kwargs["unix_socket"] = dsn["host"]
    else:
        kwargs["host"] = dsn["host"]
        kwargs["port"] = dsn["port"]
    if ca_file:
        # 只装载 SVN bundle CA；create_default_context() 会继承系统根，使同名但
        # 非内部 CA 签发的证书也能通过，破坏 bundle 作为唯一信任锚的契约。
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = True
        try:
            context.load_verify_locations(cafile=ca_file)
        except (OSError, ssl.SSLError) as exc:
            raise ValueError(f"MySQL tls_ca_file {ca_file!r} 无法加载: {exc}") from exc
        # asyncmy 会用 host 作为 server_hostname；上面的相等闸保证它就是配置身份。
        kwargs["ssl"] = context
    return kwargs


def assert_pool_conf_supported(conf) -> None:  # noqa: ANN001
    """配了 Python 侧无法实现的池参数就**拒启**,不静默忽略。

    只有 `conn_max_idle_time` 这一个:Go 有 `SetConnMaxIdleTime`,asyncmy 没有等价物。

    为什么是拒启而不是 WARN:配上它是为了改变行为的(按空闲时长回收连接,
    §9.24 门禁-B)。忽略掉之后系统行为与配置意图不一致**而且不报错** ——
    运维看着 yaml 以为空闲连接会被回收,实际不会。起不来是刺眼的,静默跑错是致命的
    (CLAUDE.md §14)。留空(Go 侧同样不生效)则两边一致,不拦。
    """
    # MySQLConf 没有 conn_max_idle_time_td() 访问器,直接用 parse_duration ——
    # 不为这一处去动共享的 config.py。
    from pandorapy.config import parse_duration

    if conf.conn_max_idle_time and parse_duration(conf.conn_max_idle_time).total_seconds() > 0:
        raise NotImplementedError(
            f"配置要求 node.mysql_client.conn_max_idle_time="
            f"{conf.conn_max_idle_time!r},但 asyncmy 没有 SetConnMaxIdleTime 的等价物"
            "(pool_recycle 按**连接寿命**回收,不是按空闲时长)。"
            "继续启动会让空闲连接不被回收,与配置意图不符 —— "
            "要么把这一项去掉(与当前实际行为一致),要么用 Go 版跑这个服务。"
        )
