"""pandora_social schema 契约闸 —— 对应 Go 侧 internal/data/schema.go。

★ 为什么校验的是**物理契约**而不是 `schema_migrations` 里的版本号:

    fresh schema(容器 initdb 直接建表)根本没有 schema_migrations 表,
    只信版本号会把干净的新库判成不兼容。反过来,只按"同名列存在"放行也不够 ——
    列在、但类型 / signedness / NULL / default / 主键不对时,后果全是**静默**的:

      guilds.pending_request_count 若是 signed 且可 NULL
          → 计数列写回 NULL 不报错,pending 上限判定读到 NULL 当 0,§9.18 的闸失效
      player_group_counts.player_id 若是 signed
          → BIGINT UNSIGNED 的 player_id 存进 signed 列会溢出成负数,
            计数行与明细对不上,而每一步 SQL 都成功
      player_group_counts 若是复合主键
          → `INSERT ... ON DUPLICATE KEY UPDATE` 的"存在就锁、不存在就建并锁"
            不再命中同一行,那把**串行化锁**就没了(TiDB 无 gap 锁,零行 FOR UPDATE
            一把锁都不加)→ 并发入群能突破上限

    这些都要等到首个业务请求才暴露,而且暴露形式是数据不对、不是报错。
    所以在接流量前查一次 information_schema(毫秒级、不锁表)。
"""

from __future__ import annotations

import dataclasses

# 新计数写路径要求的 pandora_social 最低迁移版本(与 Go 的 RequiredSchemaVersion 同值)。
REQUIRED_SCHEMA_VERSION = 2

_COLUMNS_SQL = """
SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND (
    (TABLE_NAME = 'guilds' AND COLUMN_NAME = 'pending_request_count')
    OR
    (TABLE_NAME = 'player_group_counts' AND COLUMN_NAME IN ('player_id', 'group_count'))
  )
"""

_PK_SQL = """
SELECT COLUMN_NAME
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'player_group_counts'
  AND INDEX_NAME = 'PRIMARY'
ORDER BY SEQ_IN_INDEX
"""


@dataclasses.dataclass(frozen=True, slots=True)
class ColumnMeta:
    data_type: str
    column_type: str
    nullable: str
    default: str | None


@dataclasses.dataclass(frozen=True, slots=True)
class SchemaMeta:
    columns: dict[str, ColumnMeta]
    player_group_counts_primary: list[str]


class SchemaIncompatibleError(RuntimeError):
    """schema 不满足契约。main 打 `guild_schema_incompatible` 后退出。"""


async def read_schema_meta(conn) -> SchemaMeta:  # noqa: ANN001
    """读一次 information_schema。库名取连接当前库(`DATABASE()`),与 Go 同。"""
    columns: dict[str, ColumnMeta] = {}
    primary: list[str] = []
    async with conn.cursor() as cur:
        await cur.execute(_COLUMNS_SQL)
        for row in await cur.fetchall():
            table, column, data_type, column_type, nullable, default = row
            columns[f"{table}.{column}"] = ColumnMeta(
                data_type=str(data_type),
                column_type=str(column_type),
                nullable=str(nullable),
                default=None if default is None else str(default),
            )
        await cur.execute(_PK_SQL)
        for row in await cur.fetchall():
            primary.append(str(row[0]))
    return SchemaMeta(columns=columns, player_group_counts_primary=primary)


def validate_schema_meta(meta: SchemaMeta) -> None:
    """对应 Go 的 `validateRequiredSchemaMetadata` —— 逐列同判据、同报错文本口径。

    违规项汇总后**一次性**报出(而不是遇到第一条就返回):运维一次改完,
    不用"改一条、重启、再发现下一条"。
    """
    violations: list[str] = []

    def check_column(key: str, data_type: str, unsigned: bool, has_default: bool, default: str) -> None:
        col = meta.columns.get(key)
        if col is None:
            violations.append(f"{key} 缺失")
            return
        if col.data_type.lower() != data_type.lower():
            violations.append(f"{key} DATA_TYPE={col.data_type},期望 {data_type}")
        actual_unsigned = "unsigned" in col.column_type.lower()
        if actual_unsigned != unsigned:
            violations.append(
                f"{key} COLUMN_TYPE={col.column_type},期望 {'unsigned' if unsigned else 'signed'}"
            )
        if col.nullable.upper() != "NO":
            violations.append(f"{key} IS_NULLABLE={col.nullable},期望 NO")
        actual_has_default = col.default is not None
        if actual_has_default != has_default or (
            has_default and (col.default or "").strip() != default
        ):
            actual = col.default if col.default is not None else "NULL"
            want = default if has_default else "NULL"
            violations.append(f"{key} COLUMN_DEFAULT={actual},期望 {want}")

    check_column("guilds.pending_request_count", "int", False, True, "0")
    check_column("player_group_counts.player_id", "bigint", True, False, "")
    check_column("player_group_counts.group_count", "int", False, True, "0")

    pk = meta.player_group_counts_primary
    if len(pk) != 1 or pk[0].lower() != "player_id":
        violations.append(f"player_group_counts PRIMARY KEY={pk},期望 ['player_id']")

    if not violations:
        return
    violations.sort()
    raise SchemaIncompatibleError(
        "pandora_social schema 不兼容:"
        + "; ".join(violations)
        + f";请先执行 pandora_social 迁移至 version={REQUIRED_SCHEMA_VERSION}"
    )


async def validate_required_schema(conn) -> None:  # noqa: ANN001
    """在服务装配前检查计数列 / 计数表的完整物理契约。对应 Go 的 `ValidateRequiredSchema`。"""
    validate_schema_meta(await read_schema_meta(conn))
