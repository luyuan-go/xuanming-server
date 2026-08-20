"""从 proto 描述符推导 MySQL schema —— 替代 Go 侧的 `proto2mysql` 库。

背景:
    `github.com/luyuancpp/proto2mysql` 是**你们自己的库**(与 pandora 同一 GitHub 账号),
    不是第三方生态缺失。它做的事是:扫描带 `(proto2mysql.db)` + `(proto2mysql.table_name)`
    注解的 message,按 pb 字段自动建表 / 同步表结构,并提供 CRUD。

    Python 侧不存在对等物,必须自己写。但**不必照抄整个库** —— data_service 只用到
    其中很小一块:PlayerData 一张表的建表 + 读 + 乐观锁 CAS 写。

    另外用户 2026-08-18 确认「现在没有上线、数据库可以清空」,所以不需要兼容既有 schema,
    也不需要实现 proto2mysql 的"存在则补缺列 / 对齐类型"那套增量同步 ——
    直接按 pb 建表即可。真需要增量同步时再补,不预先复杂化(§15.3)。

schema 唯一来源仍是 proto:
    表名 / 主键写在 proto option 里,每个标量字段即一列。这条不变 ——
    它保证 MySQL 里的列与协议字段永远一致,新增 proto 字段自动成列,
    不会出现"加了字段但忘了改 SQL,于是永远写不进库"。
"""

from __future__ import annotations

import dataclasses

from google.protobuf.descriptor import Descriptor, FieldDescriptor

# proto2mysql 的 option 字段号(vendored 在 proto/proto2mysql/proto2mysql_option.proto)。
# 字段编号 500000~600100 必须与上游一致,改动会让 option 读不到。
_OPT_TABLE_NAME = 500001
_OPT_PRIMARY_KEY = 500002
_OPT_AUTO_INCREMENT = 500006

# proto 标量类型 → MySQL 列类型。
#
# 无符号语义必须落到列上:CLAUDE.md §5.12 要求非负整型用 uint32/uint64,
# 若建成有符号列,超过 2^31 的 player_id 会**静默溢出**(严格模式下报错,
# 非严格模式下截断 —— 两种都不可接受)。
_TYPE_MAP: dict[int, str] = {
    FieldDescriptor.TYPE_DOUBLE: "DOUBLE",
    FieldDescriptor.TYPE_FLOAT: "FLOAT",
    FieldDescriptor.TYPE_INT64: "BIGINT",
    FieldDescriptor.TYPE_UINT64: "BIGINT UNSIGNED",
    FieldDescriptor.TYPE_INT32: "INT",
    FieldDescriptor.TYPE_FIXED64: "BIGINT UNSIGNED",
    FieldDescriptor.TYPE_FIXED32: "INT UNSIGNED",
    FieldDescriptor.TYPE_BOOL: "TINYINT(1)",
    # ★ string / bytes 必须与 Go 的 proto2mysql 一致(v0.0.18 proto2mysql.go:152/159:
    # StringKind→MEDIUMTEXT、BytesKind→MEDIUMBLOB),**哪怕它比 §9.24 期望的更宽**。
    #
    # 原先这里是 VARCHAR(255) / VARBINARY(4096)。看起来更符合 §9.24「能用 VARBINARY(N)
    # 就不用 LONGBLOB」的偏好,但在 strangler 迁移期是错的:两栈写**同一张表**,
    # 表由先启动的那个服务建。列窄的一侧会让同一条写入
    #   Go 副本成功 / Python 副本报 1406
    # ——同一个玩家的同一次改名,成功与否取决于请求落到了哪个副本,而且**不可复现**。
    # 2026-08-19 实测撞到:dev 库 player_data 被 Python 重建后 nickname 从 mediumtext
    # 变成 varchar(255),等于替 Go 服务收窄了列。
    #
    # §9.24 的写入侧上限**不靠列类型**兜底 —— 它要求的是「单元素 / 条目数 / 整体字节」
    # 三道写入侧闸(见 dbguard.check_payload),那三道仍然生效。列类型只是最后一道
    # 物理上限,与 Go 保持一致比自己收窄更重要。
    FieldDescriptor.TYPE_STRING: "MEDIUMTEXT",
    FieldDescriptor.TYPE_BYTES: "MEDIUMBLOB",
    FieldDescriptor.TYPE_UINT32: "INT UNSIGNED",
    FieldDescriptor.TYPE_ENUM: "INT",
    FieldDescriptor.TYPE_SFIXED32: "INT",
    FieldDescriptor.TYPE_SFIXED64: "BIGINT",
    FieldDescriptor.TYPE_SINT32: "INT",
    FieldDescriptor.TYPE_SINT64: "BIGINT",
}


class SchemaError(RuntimeError):
    """proto 描述符不满足建表要求。启动期 fail-fast。"""


@dataclasses.dataclass(frozen=True, slots=True)
class Column:
    name: str
    sql_type: str
    proto_field: FieldDescriptor


@dataclasses.dataclass(frozen=True, slots=True)
class TableSchema:
    """从一个 message 推导出的表结构。"""

    table_name: str
    primary_key: tuple[str, ...]
    columns: tuple[Column, ...]

    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def updatable_fields(self, version_field: str = "version") -> list[str]:
        """可经 update_mask 更新的业务列 = 全部列 - 主键 - 乐观锁列。

        对应 Go 侧 buildPlayerDataUpdateFields。**从描述符动态推导**而不是手工维护 ——
        手工列表漏一个字段,那个字段就永远写不进 MySQL,而且不报错。
        """
        excluded = {*self.primary_key, version_field}
        return [c.name for c in self.columns if c.name not in excluded]

    def create_table_sql(self) -> str:
        """生成 CREATE TABLE IF NOT EXISTS。"""
        cols = ",\n  ".join(f"`{c.name}` {c.sql_type} NOT NULL" for c in self.columns)
        pk = ", ".join(f"`{k}`" for k in self.primary_key)
        return (
            f"CREATE TABLE IF NOT EXISTS `{self.table_name}` (\n"
            f"  {cols},\n"
            f"  PRIMARY KEY ({pk})\n"
            f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        )


def _read_option(descriptor: Descriptor, field_number: int) -> object | None:
    """读一个 proto2mysql option 值。

    ⚠️ protobuf Python 的坑:如果 extension 在解析该描述符时还没注册,
    option 值会留在 unknown_fields 里,`opts.Extensions[ext]` 读回是**空值不报错**。
    所以这里直接从序列化后的 options 里按字段号解析 —— 不依赖 extension 注册顺序。
    (Go 侧靠 store.go 的 blank-import 触发注册来规避同一问题。)
    """
    # 常规路径:extension 已注册时,ListFields 能直接看到。
    for ext, value in descriptor.GetOptions().ListFields():
        if ext.number == field_number:
            return value

    # 兜底:extension 未注册时值留在 unknown_fields 里。
    # protobuf Python 在这种情况下 `opts.Extensions[ext]` 读回是**空值且不报错** ——
    # 静默拿到 "" 会让调用方以为"没配表名",所以必须显式从 unknown 里挖。
    from google.protobuf import descriptor_pb2

    opts = descriptor_pb2.MessageOptions()
    opts.ParseFromString(descriptor.GetOptions().SerializeToString())
    unknown = getattr(opts, "UnknownFields", None)
    if unknown is None:
        return None
    for item in unknown():
        if item.field_number != field_number:
            continue
        data = item.data
        if isinstance(data, bytes):
            try:
                return data.decode("utf-8")
            except UnicodeDecodeError:
                return None
        return data
    return None


def schema_of(message_class, *, table_name: str = "", primary_key: str = "") -> TableSchema:
    """从 message 类推导表结构。

    table_name / primary_key 显式传入时优先(推荐:显式 > 隐式,且避开上面那个
    extension 注册顺序的坑);留空则尝试从 proto option 读。
    """
    descriptor: Descriptor = message_class.DESCRIPTOR

    name = table_name or _read_option(descriptor, _OPT_TABLE_NAME)
    if not name:
        raise SchemaError(
            f"{descriptor.full_name}: 未声明表名"
            f"(proto option (proto2mysql.table_name) 或显式传 table_name)"
        )

    pk_raw = primary_key or _read_option(descriptor, _OPT_PRIMARY_KEY)
    if not pk_raw:
        raise SchemaError(f"{descriptor.full_name}: 未声明主键")
    pk = tuple(part.strip() for part in str(pk_raw).split(",") if part.strip())

    columns: list[Column] = []
    for field in descriptor.fields:
        # protobuf 7.x 的 upb 实现**移除了** FieldDescriptor.label,只有 is_repeated。
        # 照着老教程写 `field.label == LABEL_REPEATED` 会直接 AttributeError(实测踩到)。
        if field.is_repeated:
            raise SchemaError(
                f"{descriptor.full_name}.{field.name}: repeated 字段无法映射成标量列。"
                f"集合类数据应序列化成 bytes 列并受 §9.24 的三个上限约束"
            )
        if field.type == FieldDescriptor.TYPE_MESSAGE:
            raise SchemaError(
                f"{descriptor.full_name}.{field.name}: 嵌套 message 无法映射成标量列"
            )
        sql_type = _TYPE_MAP.get(field.type)
        if sql_type is None:
            raise SchemaError(
                f"{descriptor.full_name}.{field.name}: 不支持的 proto 类型 {field.type}"
            )
        columns.append(Column(name=field.name, sql_type=sql_type, proto_field=field))

    known = {c.name for c in columns}
    missing = [k for k in pk if k not in known]
    if missing:
        raise SchemaError(f"{descriptor.full_name}: 主键列 {missing} 不在字段列表里")

    return TableSchema(table_name=str(name), primary_key=pk, columns=tuple(columns))
