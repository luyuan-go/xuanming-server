"""启动期 schema / 后端**语义行为**探针 —— 对应 Go 的 `pkg/mysqlx/schema.go`
`CheckColumnSpecs` 与 `pkg/mysqlx/backend_check.go` `AssertColumnCollationSemantics`。

★ 为什么写在 login 服务里而不是 `pandorapy/mysqlx.py`:
  共享件本轮刚统一修过,本次迁移不改它(交付说明的 `shared_files_needed` 里已登记
  「mysqlx 需补 check_column_specs / assert_column_collation_semantics」)。这两个函数
  与 login 之外的服务无关(只有 login 校验 accounts / player_session_generations 的
  列形状),放在这里不会造成重复实现;共享件补齐后把本文件删掉、改调 mysqlx 即可。

★ 这里的三道探针**不是"查列名存在"**,区别很要紧:

  ① `check_column_specs` 对照 DATA_TYPE / IS_NULLABLE / COLUMN_KEY。
     只查列名会放过「旧库手工建过同名列但形状不对」的半旧 schema ——
     那种库能正常启动,直到第一条含该列的 SQL 才炸在运行期。
  ② `accounts.account_id` 必须是 `bigint` 且 **Nullable=YES**。
     方向刻意是"不许写成 NOT NULL":expand 窗口内旧二进制(不认识这列)的
     INSERT 不带该列,列若 NOT NULL 且无默认值,旧二进制注册直接失败 ——
     一道本意是保护数据的闸,反而打穿滚动升级。
  ③ `assert_column_collation_semantics` 是**行为探针**,不是名字比对。
     TiDB 的 `_ci` 排序规则只在集群首次 bootstrap 时启用新 collation 框架
     (`new_collations_enabled_on_first_bootstrap`)才真正大小写不敏感,否则
     **语法上接受、语义上按 binary 比较且不报错**;该参数事后不可更改。
     Go 侧对账号串零归一化,唯一性完全由列 collation 决定,漂移即
     「老玩家换个大小写登不进 + 大小写变体抢注同名账号」。
"""

from __future__ import annotations

import dataclasses
import re

# collation 名要拼进 SQL(它是**标识符**不是值,不能用占位符传参),
# 故做白名单校验。来源本就是 information_schema 而非用户输入,这是纵深防御。
_SAFE_COLLATION_NAME = re.compile(r"^[A-Za-z0-9_]+$")


@dataclasses.dataclass(frozen=True, slots=True)
class ColumnSpec:
    """对应 Go 的 `mysqlx.ColumnSpec`。

    data_type   对照 information_schema.columns.DATA_TYPE(小写,如 "bigint" / "varchar")
    nullable    对照 IS_NULLABLE:"NO" / "YES"
    key         对照 COLUMN_KEY:"PRI" / "UNI" / "MUL" / ""(空串 = 不校验本维度)
    """

    name: str
    data_type: str = ""
    nullable: str = ""
    key: str = ""


class SchemaShapeError(RuntimeError):
    """列形状与迁移产物不一致。调用方打 mysql_schema_check_failed 后退出。"""


class CollationSemanticsError(RuntimeError):
    """排序规则的**实际行为**与预期不符。调用方打 account_collation_semantics_mismatch。"""


async def check_column_specs(  # noqa: ANN001 —— conn 是 asyncmy/aiomysql 连接
    conn, migration_hint: str, table: str, *specs: ColumnSpec
) -> None:
    """校验表中给定列存在且类型 / 可空性 / 键形状符合预期。

    比较一律不区分大小写(与 Go 的 strings.ToLower / ToUpper 同口径)。
    specs 为空直接返回,与 Go 同。
    """
    if not specs:
        return
    q = (
        "SELECT column_name, data_type, is_nullable, column_key "
        "FROM information_schema.columns "
        "WHERE table_schema = DATABASE() AND table_name = %s"
    )
    async with conn.cursor() as cur:
        await cur.execute(q, (table,))
        rows = await cur.fetchall()
    present = {
        str(r[0]).lower(): (
            str(r[1]).lower(),
            str(r[2]).upper(),
            str(r[3] or "").upper(),
        )
        for r in rows
    }

    problems: list[str] = []
    for spec in specs:
        shape = present.get(spec.name.lower())
        if shape is None:
            problems.append(f"{spec.name}: 列缺失")
            continue
        data_type, nullable, key = shape
        if spec.data_type and data_type != spec.data_type.lower():
            problems.append(f"{spec.name}: 类型 {data_type} ≠ 期望 {spec.data_type.lower()}")
        if spec.nullable and nullable != spec.nullable.upper():
            problems.append(
                f"{spec.name}: 可空性 {nullable} ≠ 期望 {spec.nullable.upper()}"
            )
        if spec.key and key != spec.key.upper():
            problems.append(f"{spec.name}: 键形状 {key!r} ≠ 期望 {spec.key.upper()!r}")
    if problems:
        raise SchemaShapeError(
            f"表 {table} 列形状不符 [{'; '.join(problems)}]:"
            "schema 与迁移产物不一致(旧库手工改过表/迁移未跑全),"
            f"请对照 {migration_hint} 修复后再启动"
        )


async def assert_column_collation_semantics(  # noqa: ANN001
    conn, table: str, column: str, want_case_insensitive: bool, want_no_pad: bool
) -> None:
    """断言 `table.column` 的排序规则在**当前后端上的实际行为**符合预期。

    两个维度都要查,因为退化路径不同:
      - want_case_insensitive:退化成 `_bin` 时 'A' != 'a' —— 老玩家换个大小写就
        登不进,且能用大小写变体抢注同名账号;
      - want_no_pad:`utf8mb4_0900_ai_ci` 是 NO PAD,而 `utf8mb4_bin` /
        `utf8mb4_general_ci` 都是 PAD SPACE。退化成 PAD SPACE 会让 "abc " 与 "abc"
        塌成同一个值 —— 存量导入时唯一键直接冲突,或静默把两个账号并成一个。

    探针取该列**实际生效**的 collation 名(而不是假设 DDL 已生效),再用它做真实比较。
    """
    q = (
        "SELECT COLLATION_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s"
    )
    async with conn.cursor() as cur:
        await cur.execute(q, (table, column))
        row = await cur.fetchone()
        if row is None:
            raise CollationSemanticsError(
                f"列 {table}.{column} 不存在,无法校验排序规则(schema 未就绪?)"
            )
        name = row[0]
        if not name:
            raise CollationSemanticsError(
                f"列 {table}.{column} 无排序规则(非字符串列?),拒绝按预期比较语义启动"
            )
        name = str(name)
        if not _SAFE_COLLATION_NAME.match(name):
            raise CollationSemanticsError(
                f"列 {table}.{column} 的排序规则名 {name!r} 含非法字符,拒绝拼接探针 SQL"
            )
        # 显式 COLLATE 的一侧决定比较规则。collation 不被后端支持时本语句直接报错,
        # 同样 fail-closed(拿不到结论 = 不放行)。
        probe = f"SELECT 'A' COLLATE {name} = 'a', 'a ' COLLATE {name} = 'a'"
        await cur.execute(probe)
        result = await cur.fetchone()
    if result is None:
        raise CollationSemanticsError(
            f"排序规则 {name} 行为探针无返回(后端可能不支持该 collation)"
        )
    got_case_insensitive = bool(result[0])
    got_pad_space = bool(result[1])

    if want_case_insensitive and not got_case_insensitive:
        raise CollationSemanticsError(
            f"列 {table}.{column} 声明排序规则 {name},但后端实际按**大小写敏感**比较:"
            "TiDB 需 v7.4.0+ 且集群首次 bootstrap 时启用新 collation 框架"
            "(new_collations_enabled_on_first_bootstrap,该配置事后不可更改);"
            "继续启动会让唯一键语义与单机 MySQL 不一致,拒绝启动"
        )
    if want_no_pad and got_pad_space:
        raise CollationSemanticsError(
            f"列 {table}.{column} 声明排序规则 {name},但后端实际是 PAD SPACE"
            "(尾随空格被忽略),而单机 MySQL 的 utf8mb4_0900_ai_ci 是 NO PAD:"
            "两者会把 'abc ' 与 'abc' 判成不同/相同两种结果,"
            "存量数据迁移时唯一键冲突或静默并号,拒绝启动"
        )
