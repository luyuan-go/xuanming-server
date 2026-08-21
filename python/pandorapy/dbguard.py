"""数据库守卫 —— 对应 Go 侧 pkg/dbguard。

三件事,重要性递减:

1. **sql_mode 严格模式断言(启动期 fail-fast)**
   这是 CLAUDE.md §9.24 里**唯一允许因数据库检查而拒绝启动**的场景。理由:
   非严格模式下超长写入不报错而是**静默截断** —— 真 MySQL 8.4 实测:往
   VARBINARY(16) 写 100 字节,严格模式 → Error 1406 写入失败;非严格 → err=nil
   且实际只存 16 字节。玩家数据被无声砍断且无任何错误可观测。
   静默数据损坏远比服务起不来严重,所以这里必须硬失败。

   ⚠️ 必须断言 `@@session.sql_mode` 而不是 `@@global` —— DSN 参数能覆盖 session,
   查 global 会得到"看起来没问题"的假象。

2. **容量预算巡检(只告警不阻断)**
   超预算是"要去查的问题",不是"服务不能跑的理由";拒绝启动会把容量问题升级成
   可用性事故。走 information_schema 估算(毫秒级不锁表),**禁止 COUNT(*)**
   拖垮启动。

3. **写入侧 payload 上限(三档告警)**
   达上限拒写 / 达 80% 放行但 WARN(留排查窗口)/ 否则静默。
   §9.24 要求集合序列化列同时有单元素、集合条目、整体字节三个上限,缺一个就有洞。
"""

from __future__ import annotations

import asyncio
import dataclasses
import enum

from pandorapy import log as plog
from pandorapy import mysqlx

# ── Prometheus 指标 ──────────────────────────────────────────────────────────
#
# ★ 名字与 label 必须与 Go 侧 pkg/dbguard **逐字相同**。Grafana 的容量面板与告警
# 规则按这些名字建；Python 副本不写它们的后果不是"少一块图"，而是**同一块面板在
# 灰度期只反映 Go 副本**，容量问题在 Python 这边完全不可见（NoData 而非告警）。
from prometheus_client import Counter, Gauge  # noqa: E402

TABLE_ROWS = Gauge(
    "pandora_db_table_rows", "表行数(information_schema 估算)。", ["db", "table"]
)
TABLE_ROWS_BUDGET = Gauge(
    "pandora_db_table_rows_budget", "表行数预算上限(0=未设预算)。", ["db", "table"]
)
TABLE_BYTES = Gauge(
    "pandora_db_table_bytes",
    "表数据字节数(information_schema DATA_LENGTH,**不含索引** —— 与 Go 同口径)。",
    ["db", "table"],
)
AVG_ROW_BYTES = Gauge(
    "pandora_db_avg_row_bytes",
    "表平均行字节数。排查大字段最灵敏的信号:突增 = 单行变胖 = blob 内部无界增长。",
    ["db", "table"],
)
COLUMN_MAX_BYTES = Gauge(
    "pandora_db_column_max_bytes",
    "单列最大字节数(全表扫描,低频)。label 只有 db/table/column(低基数)。",
    ["db", "table", "column"],
)
BUDGET_VIOLATIONS = Counter(
    "pandora_db_budget_violations_total",
    "容量预算超限次数。kind=rows|bytes|avg_row_bytes|column_bytes。",
    ["db", "table", "kind"],
)
RETENTION_PENDING = Gauge(
    "pandora_db_retention_pending_rows",
    "满足保留期清理条件但尚未删除的行数(report_only 下即持续积压量)。",
    ["db", "table"],
)
RETENTION_DELETED = Counter(
    "pandora_db_retention_deleted_rows_total",
    "保留期清理实际删除的行数(仅 delete 模式非零)。",
    ["db", "table"],
)


# 与 Go 侧 strictModeProbeTimeout 一致:够慢网络一次往返,又不会把启动挂死。
STRICT_MODE_PROBE_TIMEOUT_SEC = 5.0

# 逼近告警阈值:达 80% 放行但 WARN,留出排查窗口(与 Go 侧一致)。
WARN_RATIO = 0.8


class StrictModeError(RuntimeError):
    """sql_mode 缺 STRICT_TRANS_TABLES。调用方必须打 mysql_strict_mode_required 后退出。"""


class PayloadTooLargeError(RuntimeError):
    """序列化 payload 超过列容量上限。"""


async def assert_strict_mode(conn) -> None:  # noqa: ANN001 —— 兼容 aiomysql / asyncmy 游标
    """断言 session sql_mode 含 STRICT_TRANS_TABLES。对应 Go 的 dbguard.AssertStrictMode。

    调用方(各服务 main 装配完连接池后立刻调):

        try:
            await dbguard.assert_strict_mode(conn)
        except dbguard.StrictModeError as exc:
            logger.error("mysql_strict_mode_required", err=str(exc))
            return 1

    为什么不在本函数里直接退出:各服务的日志与退出约定不同,把决定权留给调用方,
    这里只统一"怎么探测"(与 Go 侧同样的分工)。
    """
    async def _probe():  # noqa: ANN202
        async with conn.cursor() as cur:
            # 必须是 session 不是 global —— DSN 参数可覆盖 session。
            await cur.execute("SELECT @@session.sql_mode")
            return await cur.fetchone()

    # ★ 探测必须有界。这是**启动期**的第一条 SQL:库半死不活(TCP 通了但不回包)时
    # 无界等待会让进程永远停在"正在启动",k8s 既不 Ready 也不重启,而日志一行不打。
    # 常量此前定义了却没人用 —— 定义即失效的那种缺陷。
    try:
        row = await asyncio.wait_for(_probe(), timeout=STRICT_MODE_PROBE_TIMEOUT_SEC)
    except TimeoutError as exc:
        raise StrictModeError(
            f"dbguard: 探测 sql_mode 超过 {STRICT_MODE_PROBE_TIMEOUT_SEC}s 未返回 —— "
            f"无法证明严格模式已开启,按 fail-fast 处理(静默截断比起不来严重得多)"
        ) from exc
    mode = (row[0] if row else "") or ""
    for part in mode.split(","):
        if part.strip() == "STRICT_TRANS_TABLES":
            return
    raise StrictModeError(
        f"dbguard: session sql_mode 缺 STRICT_TRANS_TABLES(当前={mode!r})。"
        "非严格模式下超长写入会被静默截断(err=nil 但数据被砍断),等于无声的数据损坏。"
        "修法:MySQL 服务端 --sql-mode 保留默认值,或从 DSN 中移除覆盖 sql_mode 的参数"
    )


# ── 容量预算 ─────────────────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True, slots=True)
class TableBudget:
    """一张表的容量预算。各服务在自己的 budgets.py 里声明。

    上限值按**设计期望**定,不按列类型上限定 —— 写成列类型上限等于没设(数据涨到
    快撑爆才告警,业务语义早已崩坏)。这是 §9.24 明写的要求。
    """

    table: str
    max_rows: int = 0
    # ★ max_bytes 是**独立的第三个维度**,不能用「行数 × 平均行长」推:
    # 那个乘积不含索引,而 DATA_LENGTH+INDEX_LENGTH 才是真正占的盘。
    # 漏掉它 = 行数正常、平均行长正常、但表已经把盘吃满 —— 三个维度里最先撞墙的往往是它。
    max_bytes: int = 0
    max_avg_row_bytes: int = 0
    # note 是超限时打进日志的**排查方向**(例如"检查 xxx sweep 是否在跑")。
    # 只打一句"超预算"而不说往哪查,值班的人只能干瞪眼 —— Go 侧特意带了这个字段。
    note: str = ""


@dataclasses.dataclass(frozen=True, slots=True)
class ColumnBudget:
    """单个大字段的字节预算(列级检查)—— 对应 Go 的 `dbguard.ColumnBudget`。

    ★ 与 `TableBudget` 的区别不只是粒度,而是**成本与触发时机**:
    表级走 information_schema(毫秒级、不锁表),可以挂周期 ticker;
    列级是 `MAX(LENGTH(col))` **全表扫描**,放进周期路径会把生产库扫死。
    所以 `check_columns` 只在两种场景调:①表级 avg_row_bytes 告警后人工 / 工具触发定位;
    ②天级低频巡检。**不要把它接到 sweep ticker 上。**
    """

    table: str
    column: str
    max_bytes: int = 0
    # note 同 TableBudget:超限时打进日志的排查方向。
    note: str = ""


@dataclasses.dataclass(frozen=True, slots=True)
class Violation:
    table: str
    # kind 取值与 Go 逐字一致:rows / bytes / avg_row_bytes / column_bytes。
    # ⚠️ 曾经写成 avg_row_length,和 Go 的 avg_row_bytes 对不上 ——
    # Grafana 上按 kind 分组的面板会凭空多出一个分类、旧分类查不到 Python 服务。
    kind: str
    actual: int
    budget: int
    note: str = ""
    # 只有列级检查(kind=column_bytes)才有值—— 与 Go `Violation.Column` 同语义。
    column: str = ""


@dataclasses.dataclass(slots=True)
class CheckResult:
    checked: int
    violations: list[Violation]


async def check_budgets(conn, schema: str, budgets: list[TableBudget]) -> CheckResult:  # noqa: ANN001
    """跑一轮容量巡检。**只告警不阻断** —— 返回结果由调用方打日志 + 计数。

    走 information_schema.TABLES 估算(毫秒级、不锁表)。刻意**不用 COUNT(*)**:
    在大表上会拖垮启动,而这只是个告警指标,不需要精确值。
    """
    schema = mysqlx.require_mysql_identifier(schema, kind="schema")
    if not budgets:
        return CheckResult(checked=0, violations=[])

    for budget in budgets:
        mysqlx.require_mysql_identifier(budget.table, kind="table")
    by_table = {b.table: b for b in budgets}
    placeholders = ",".join(["%s"] * len(by_table))
    async with conn.cursor() as cur:
        await cur.execute(
            f"SELECT TABLE_NAME, TABLE_ROWS, "  # noqa: S608 —— 表名来自代码常量非用户输入
            # ★ 只取 DATA_LENGTH,**不加 INDEX_LENGTH** —— 与 Go pkg/dbguard/dbguard.go:268
            # 同口径。加上索引会让同一张表的 max_bytes 判定两栈不同:索引占比大的表
            # (如全是二级索引的流水表)在 Python 侧会先超限,而告警阈值是按 Go 的口径定的。
            f"COALESCE(DATA_LENGTH,0), AVG_ROW_LENGTH "
            f"FROM information_schema.TABLES "
            f"WHERE TABLE_SCHEMA = %s AND TABLE_NAME IN ({placeholders})",
            (schema, *by_table.keys()),
        )
        rows = await cur.fetchall()

    violations: list[Violation] = []
    for name, table_rows, total_bytes, avg_len in rows:
        budget = by_table.get(name)
        if budget is None:
            continue

        # ★ 每轮都写 gauge —— 对齐 Go Check()(dbguard.go:228-236)。
        # 只打日志不写 metric 的后果不是"少一块图":Grafana 上按这些名字建的容量面板
        # 在 Python 副本上是**空的**,而 avg_row_bytes 正是文档点名"排查大字段最灵敏
        # 的信号"。日志只在超限那一刻有,趋势看不了。
        TABLE_ROWS.labels(schema, name).set(table_rows or 0)
        TABLE_ROWS_BUDGET.labels(schema, name).set(budget.max_rows or 0)
        TABLE_BYTES.labels(schema, name).set(total_bytes or 0)
        AVG_ROW_BYTES.labels(schema, name).set(avg_len or 0)

        def _check(kind: str, actual, limit: int, b=budget, n=name) -> None:  # noqa: ANN001
            if limit and (actual or 0) > limit:
                violations.append(Violation(n, kind, int(actual or 0), limit, b.note))
                BUDGET_VIOLATIONS.labels(schema, n, kind).inc()

        # 三个维度各查各的 —— 与 Go 侧同序同名。
        _check("rows", table_rows, budget.max_rows)
        _check("bytes", total_bytes, budget.max_bytes)
        _check("avg_row_bytes", avg_len, budget.max_avg_row_bytes)
    return CheckResult(checked=len(rows), violations=violations)


# 与 Go 侧逐字相同的排查指引。写成常量而不是内联,是为了让"两边必须一样"这件事
# 有一个可 grep 的落点。
BUDGET_HINT = (
    "见 docs/design/db-capacity-guard.md 排查手册;"
    "行数超限先查清理任务是否在跑,平均行长超限查 blob 内 repeated 字段是否无界"
)


def log_violations(result: CheckResult, db: str = "") -> None:
    """把巡检结果打成 ERROR 日志。

    ★ 事件名与字段名必须与 Go 侧**逐字一致**(db_capacity_budget_exceeded /
    db / table / kind / actual / budget / note / hint)。

    这不是洁癖:Loki 的告警规则和 Grafana 面板是按事件名和字段名建的,
    名字对不上的后果是「Python 服务的容量告警永远不触发」——
    而日志本身一条不少、级别也是 ERROR,看上去完全正常。
    (2026-08-19 实测:Python 曾打成 db_budget_violation + metric=avg_row_length,
    与 Go 的 db_capacity_budget_exceeded + kind=avg_row_bytes 全线对不上。)

    ERROR 级:这是"需要人去查"的信号(§9.24 有界承诺已被突破),不是可忽略的 Warn。
    """
    logger = plog.get()
    for v in result.violations:
        logger.error(
            "db_capacity_budget_exceeded",
            db=db,
            table=v.table,
            kind=v.kind,
            actual=v.actual,
            budget=v.budget,
            note=v.note,
            hint=BUDGET_HINT,
        )


# 与 Go 侧 CheckColumns 的 hint 逐字一致。
COLUMN_HINT = (
    "用 dbguard.TopLargeRows 或 dbcheck -top-rows 定位到具体主键,"
    "再反序列化看是哪个字段爆了"
)


async def check_columns(conn, schema: str, budgets: list[ColumnBudget]) -> CheckResult:  # noqa: ANN001
    """跑一轮**列级**字节巡检 —— 对应 Go `Guard.CheckColumns`。超预算只告警不阻断。

    ⚠️ `MAX(LENGTH(col))` 是**全表扫描**,成本远高于表级巡检。
    只在「表级 avg_row_bytes 告警后人工定位」或「天级低频巡检」时调,
    **不要挂到 sweep ticker 上**。

    单列扫描失败只 WARN 并继续下一列(与 Go 同):一列扫不动不该让整轮巡检哑掉,
    而巡检本身只是告警,失败不影响正确性。
    """
    if not budgets:
        return CheckResult(checked=0, violations=[])
    logger = plog.get()
    violations: list[Violation] = []
    checked = 0
    for budget in budgets:
        # 表名 / 列名要拼进 SQL(标识符位置用不了占位符),所以必须先过白名单校验。
        # 它们全部来自各服务 budgets.py 的代码常量,不接受外部输入。
        table = mysqlx.require_mysql_identifier(budget.table, kind="table")
        column = mysqlx.require_mysql_identifier(budget.column, kind="column")
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT MAX(LENGTH(`{column}`)), AVG(LENGTH(`{column}`)) "  # noqa: S608 —— 标识符已过白名单校验
                    f"FROM `{table}`"
                )
                row = await cur.fetchone()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 —— 扫不动只是少一列指标,不阻断整轮
            logger.warning(
                "dbguard_column_scan_failed",
                db=schema,
                table=budget.table,
                column=budget.column,
                err=str(exc),
            )
            continue

        checked += 1
        max_len = int(row[0] or 0) if row else 0
        avg_len = int(row[1] or 0) if row else 0
        COLUMN_MAX_BYTES.labels(schema, budget.table, budget.column).set(max_len)
        if budget.max_bytes <= 0 or max_len <= budget.max_bytes:
            continue

        BUDGET_VIOLATIONS.labels(schema, budget.table, "column_bytes").inc()
        violations.append(
            Violation(
                table=budget.table,
                kind="column_bytes",
                actual=max_len,
                budget=budget.max_bytes,
                note=budget.note,
                column=budget.column,
            )
        )
        # ★ 列级超限**在这里直接打**,不走 log_violations —— 与 Go 同:
        # 它带 column / avg_bytes 两个表级日志没有的字段,合并成一个函数
        # 就得给表级路径塞空字段,反而让 Loki 上两类告警长得一样。
        logger.error(
            "db_column_size_budget_exceeded",
            db=schema,
            table=budget.table,
            column=budget.column,
            max_bytes=max_len,
            avg_bytes=avg_len,
            budget=budget.max_bytes,
            note=budget.note,
            hint=COLUMN_HINT,
        )
    return CheckResult(checked=checked, violations=violations)


@dataclasses.dataclass(frozen=True, slots=True)
class LargeRow:
    """一条大行定位结果(主键 + 该列字节数)—— 对应 Go 的 `dbguard.LargeRow`。"""

    pk: str
    size_bytes: int


async def top_large_rows(  # noqa: ANN001
    conn, table: str, pk_col: str, column: str, limit: int = 20
) -> list[LargeRow]:
    """定位某列最大的 N 行,返回主键与字节数 —— **排查大字段的第一步落点**。

    拿到主键后的标准下一步:把该行的 blob dump 出来反序列化(proto / JSON),
    看是哪个 repeated 字段元素数异常,再回到写入路径找为什么没有上限。

    `table` / `pk_col` / `column` 必须是调用方硬编码的标识符(不接受外部输入):
    标识符位置用不了参数化占位符,这里靠白名单校验兜底。
    limit 有界(1..100,越界回落 20)防一次拉太多。
    """
    table = mysqlx.require_mysql_identifier(table, kind="table")
    pk_col = mysqlx.require_mysql_identifier(pk_col, kind="column")
    column = mysqlx.require_mysql_identifier(column, kind="column")
    if limit <= 0 or limit > 100:
        limit = 20
    async with conn.cursor() as cur:
        await cur.execute(
            f"SELECT `{pk_col}`, LENGTH(`{column}`) AS n "  # noqa: S608 —— 标识符已过白名单校验
            f"FROM `{table}` ORDER BY n DESC LIMIT %s",
            (limit,),
        )
        rows = await cur.fetchall()
    return [LargeRow(pk=str(pk), size_bytes=int(n or 0)) for pk, n in rows]


# ── 保留期清理(§9.24)──────────────────────────────────────────────────────


class Mode(str, enum.Enum):
    """清理模式。**零值 / 留空 = REPORT_ONLY**(用户 2026-07-22 指令)。

    为什么默认只报告不删:
        自动删生产数据不可逆。清理条件 / 保留期 / 幂等窗口任一处配错都会静默删掉
        不该删的玩家数据,而且**删完才发现**。把"何时删"的决定权交回人手里。
        代价是 report_only 下库会继续增长 —— 所以待清理量必须持续可见
        (WARN + metric + dbcheck -pending),让人能判断何时开删。

    唯一例外:battle_result 的战报清理默认真删(产品口径"最多存最近六个月")。
    """

    REPORT_ONLY = "report_only"
    DELETE = "delete"


def parse_mode(raw: str) -> Mode:
    """解析配置里的 retention_mode。

    ⚠️ 无法识别的值**报错而非猜成 delete** —— 拼错一个字母就开始删生产数据
    是不可接受的失败模式(与 Go 侧 ParseMode 同一决定)。
    """
    text = (raw or "").strip().lower()
    # ★ Go 的 ParseMode 认四个写法:"" / report_only / report / report-only。
    # 少认两个别名的后果不是"配置不生效",而是**启动直接失败**
    # (ValidateRetentionMode fail-fast)—— 一份在 Go 上跑得好好的 yaml,
    # 换成 Python 副本就起不来,而错误信息只说"无法识别",看不出是两栈词表不同。
    if text in ("", "report", "report-only", "report_only"):
        return Mode.REPORT_ONLY
    try:
        return Mode(text)
    except ValueError:
        raise ValueError(
            f"dbguard: 无法识别的 retention_mode={raw!r}(只支持 report_only / delete)。"
            f"拒绝猜测 —— 猜成 delete 会开始删生产数据"
        ) from None


@dataclasses.dataclass(slots=True)
class Outcome:
    """一轮清理的结果。"""

    mode: Mode
    matched: int  # 满足清理条件的行数
    deleted: int  # 实际删除的行数(report_only 恒为 0)
    # ★ truncated 是**唯一不冗余的那一位**,少了它调用方就无法判断"还有积压"。
    #
    # 为什么不能从 matched/deleted 推回来:DELETE 路径上 `matched == deleted` 恒成立
    # (Go 的 `out.Matched, out.Deleted = n, n` 与本文件同形),两个字段携带的是同一个数;
    # 而 `matched > deleted` 只在 REPORT_ONLY 档才可能为真 —— 那一档根本不需要循环。
    # 所以"照抄两个看得见的计数、把这个纯派生的布尔位省掉"必然导致调用方编出一个恒真
    # (或恒假)的替代判据:真实事故形状见 player 的保留期清理循环 —— 判据恒真 → 每轮
    # 只删一批就退出 → 只增表积压永远追不平(§9.24 的容量守护形同虚设),而日志里
    # `db_retention_deleted truncated=True` 明明白白写着"还有积压"。
    #
    # 带默认值是为了不破现有构造点(全仓构造点均为关键字形式);默认 False 对齐 Go 的
    # 零值语义 —— Go 在 report_only 分支根本不给 Truncated 赋值。
    truncated: bool = False


async def sweep_table(  # noqa: ANN001
    conn,
    mode: Mode,
    schema: str,
    table: str,
    where: str,
    limit: int,
    *params,
) -> Outcome:
    """按 mode 处理满足 where 的行。对应 Go 的 dbguard.SweepTable。

    ★ Count 与 Delete **共用同一个 where 字符串** —— 这是从机制上排除
    "报告说 0 行、实际删了 10 万行"的条件漂移。条件只写一遍,不允许调用方传两份。

    小批量 DELETE ... LIMIT 防长事务锁表;多副本并发跑幂等(删的是同一批行)。
    """
    schema = mysqlx.require_mysql_identifier(schema, kind="schema")
    table = mysqlx.require_mysql_identifier(table, kind="table")
    if mode is Mode.REPORT_ONLY:
        # report_only:只 COUNT,一行都不删。
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT COUNT(*) FROM `{schema}`.`{table}` WHERE {where}",  # noqa: S608
                params,
            )
            row = await cur.fetchone()
            matched = int(row[0]) if row else 0

        RETENTION_PENDING.labels(schema, table).set(matched)
        if matched > 0:
            # 待清理量必须持续可见 —— 这是 report_only 默认的配套要求,
            # 否则库悄悄涨到撑爆都没人知道。
            # ★ 事件名与 Go sweep.go:165 逐字相同:按它建的 Loki 告警要能同时命中两栈。
            plog.get().warning(
                "db_retention_pending_not_deleted",
                db=schema,
                table=table,
                pending_rows=matched,
                mode=mode.value,
                hint="按当前配置只报告不删除(retention_mode=report_only);"
                "库会继续增长,确认清理条件无误后设 retention_mode=delete 开启实删",
            )
        return Outcome(mode=mode, matched=matched, deleted=0)

    # delete:真删。
    #
    # ★ **不先 COUNT**(对齐 Go sweep.go:174-177)。delete 路径上那次全条件 COUNT
    # 是纯浪费:结果只用来决定"要不要删",而 `DELETE ... LIMIT` 自己就会返回删了几行。
    # inventory_ledger 的行数预算是 5400 万,每轮多一次全条件 COUNT 是实打实的负担,
    # 而且它扫的正是要删的那批行(索引再好也要走一遍)。
    async with conn.cursor() as cur:
        await cur.execute(
            f"DELETE FROM `{schema}`.`{table}` WHERE {where} LIMIT %s",  # noqa: S608
            (*params, limit),
        )
        deleted = cur.rowcount or 0

    # Go 的 Truncated(sweep.go:182):本批打满了 LIMIT = 可能还有积压,调用方应继续下一批。
    # ★ 只算一次并同时喂给日志与返回值 —— 两处各写一遍表达式迟早漂移,而漂移的表现是
    #   "日志说还有积压、调用方却停了"(本条缺陷此前正是这个形状)。
    truncated = limit > 0 and deleted >= limit
    if deleted > 0:
        RETENTION_DELETED.labels(schema, table).inc(deleted)
        plog.get().info(
            "db_retention_deleted",
            db=schema,
            table=table,
            deleted=deleted,
            batch_limit=limit,
            truncated=truncated,
        )
    # matched 在 delete 路径上等于实际删除数(Go 的 out.Matched, out.Deleted = n, n)。
    return Outcome(mode=mode, matched=deleted, deleted=deleted, truncated=truncated)


# ── 写入侧 payload 上限 ───────────────────────────────────────────────────────


def check_payload(name: str, payload: bytes, max_bytes: int) -> None:
    """整体字节上限(§9.24 三个上限里的第 ③ 条)。对应 Go 的 dbguard.CheckPayload。

    三档(与 Go 一致):
      - 超上限   → 抛异常拒写(数据会被静默截断的唯一防线)
      - 达 80%   → 放行但 WARN,留出排查窗口
      - 否则     → 静默

    ⚠️ 这一条只管"整体字节"。§9.24 要求集合序列化列**同时**有:
       ① 单元素上限 ② 集合条目上限 ③ 整体字节上限
    只设 ③ 会漏掉"单个格子胖到 60KB 但整体没超";只设 ①② 会漏掉"每项合规但
    项数×大小仍超列容量"。①② 属于业务层校验,不在本函数职责内 —— 别以为调了
    check_payload 就达标了。真实教训:bag 管住 items 条数却没管单个 item 的
    attrs 条数(深度无闸);rewardclaim 管住单条位图大小却没管位图条目数(广度无闸)。
    """
    size = len(payload)
    # ★ 未设预算 = 不校验(与 Go 的 `if limit.Max <= 0 { return nil }` 一致)。
    # 少这一分支的话,任何还没定阈值的列都会因为 max_bytes=0 而**拒掉一切写入** ——
    # 一个容量守护把正常业务全挡了,方向反了。
    if max_bytes <= 0:
        return
    # ★ 边界是 `>=` 不是 `>`(Go:`if size >= limit.Max`)。
    # 差这一格的后果是"恰好等于上限"的 payload 两栈判定**相反**:
    # Go 拒、Python 放行 —— 而列宽本身就是上限,放行的那条要么被 MySQL 报错、
    # 要么在非严格模式下被静默截断(§9.24 点名的那种数据损坏)。
    if size >= max_bytes:
        # ★ 拒写必须留下**可观测信号**。Go 侧这里有 ERROR 日志 + 拒写计数器
        # (pkg/dbguard/payload.go:81-87),Python 侧原先只抛异常 ——
        # 异常会被调用方按业务错误处理掉,于是"某个玩家的数据一直写不进去"
        # 在运维视角上**完全不可见**:没有日志、没有指标,只有客服工单。
        plog.get().error(
            "db_payload_too_large_rejected",
            name=name,
            size=size,
            limit=max_bytes,
            hint=(
                "已拒绝本次写入(fail-closed);放行会被 MySQL 报错或在非严格模式下"
                "静默截断。见 docs/design/db-capacity-guard.md"
            ),
        )
        raise PayloadTooLargeError(
            f"dbguard: {name} 序列化后 {size} 字节,达到/超过上限 {max_bytes}。"
            f"拒写而不是让它进库被静默截断"
        )
    # ★ 浮点比较,不提前 int() —— Go 是 `float64(size) >= float64(limit.Max)*WarnRatio`。
    # int() 会向下取整,于是恰好落在阈值那一格的 payload 两栈一个告警一个不告警;
    # "逼近上限"本来就是给人留排查窗口的信号,差一格就少一次提醒。
    if float(size) >= max_bytes * WARN_RATIO:
        plog.get().warning(
            "db_payload_approaching_limit",
            name=name,
            size=size,
            limit=max_bytes,
            ratio=round(size / max_bytes, 3),
        )
