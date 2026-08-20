"""`protosql` 的 proto→MySQL 类型映射必须与 Go 的 proto2mysql 一致。

**为什么这条是硬的**：strangler 迁移期两栈写**同一张表**，而表由先启动的那个服务建
（`proto2mysql.SyncAllTables` / `protosql.ensure_schema` 都是 CREATE IF NOT EXISTS）。
列宽不一致的后果是同一条写入 —— 比如一个 300 字的昵称 ——

    Go 副本   → 成功（mediumtext）
    Python 副本 → 1406 Data too long（varchar(255)）

成功与否取决于请求落到了哪个副本，**不可复现**，而且两边的代码都"没错"。

2026-08-19 实测撞到过一次：dev 库 `pandora_player.player_data` 被 Python 重建后，
`nickname` 从 `mediumtext` 变成 `varchar(255)`，等于替正在跑的 Go 服务收窄了列。

⚠️ 关于 §9.24「能用 VARBINARY(N) 就不用 LONGBLOB」：那条偏好在这里**让位于跨栈一致**。
§9.24 真正要求的是**写入侧**三道闸（单元素 / 条目数 / 整体字节，见 `dbguard.check_payload`），
那三道仍然生效；列类型只是最后一道物理上限，与 Go 保持一致比自己收窄更重要 ——
自己收窄不会让数据更安全，只会让一部分写入随机失败。
"""

from __future__ import annotations

import os
import pathlib
import re

import pytest
from google.protobuf.descriptor import FieldDescriptor

from pandorapy import protosql

# Go 侧 proto2mysql 的 MySQLFieldTypes（proto2mysql.go:149-161）。
# 只列**跨语言会建出不同列**的那几个；整型族两边都是等价写法。
_GO_EXPECTED = {
    "StringKind": "MEDIUMTEXT",
    "BytesKind": "MEDIUMBLOB",
    "MessageKind": "MEDIUMBLOB",
}

_PY_UNDER_TEST = {
    "StringKind": protosql._TYPE_MAP[FieldDescriptor.TYPE_STRING],  # noqa: SLF001
    "BytesKind": protosql._TYPE_MAP[FieldDescriptor.TYPE_BYTES],  # noqa: SLF001
}


@pytest.mark.parametrize(("kind", "want"), sorted(_PY_UNDER_TEST.items()))
def test_string_and_bytes_match_go_proto2mysql(kind: str, want: str) -> None:
    """★ 收窄这两个列 = 同一条写入在两栈上一成一败，且不可复现。"""
    assert _PY_UNDER_TEST[kind].upper() == _GO_EXPECTED[kind], (
        f"{kind} 与 Go 的 proto2mysql 不一致：Python={_PY_UNDER_TEST[kind]!r} "
        f"Go={_GO_EXPECTED[kind]!r}。表由先启动的服务建，列窄的一侧会让写入随机失败。"
    )


def _find_proto2mysql_source() -> pathlib.Path | None:
    """在 GOPATH 里找 proto2mysql 模块源码。找不到就 skip（CI 上可能没有 Go 模块缓存）。"""
    gopath = os.getenv("GOPATH") or str(pathlib.Path.home() / "go")
    root = pathlib.Path(gopath) / "pkg" / "mod" / "github.com" / "luyuancpp"
    if not root.is_dir():
        return None
    for d in sorted(root.glob("proto2mysql@*"), reverse=True):
        f = d / "proto2mysql.go"
        if f.is_file():
            return f
    return None


def test_expected_table_is_taken_from_the_real_go_source() -> None:
    """★ 上面那张 `_GO_EXPECTED` 是手抄的 —— 这条负责证明它没抄错。

    真正的对拍模板（`test_kafkax_parity.py`）是直接跑 Go 包；proto2mysql 是外部 module，
    跑不了，退而求其次：**解析它的源码**，确认手抄值与真源一致。
    Go 侧升级 proto2mysql 改了映射时，这条会红。

    GOPATH 里没有模块缓存时 skip —— 但上面那条逐值断言仍然生效，
    不会因为这条 skip 就完全失去保护。
    """
    src = _find_proto2mysql_source()
    if src is None:
        pytest.skip("GOPATH 里没有 proto2mysql 模块缓存（跑一次 go mod download 即可）")

    text = src.read_text(encoding="utf-8", errors="replace")
    block = re.search(r"MySQLFieldTypes = map\[protoreflect\.Kind\]string\{(.*?)\n\}", text, re.S)
    assert block, f"没在 {src} 里找到 MySQLFieldTypes —— proto2mysql 结构变了，本测试要跟着改"

    actual = dict(
        re.findall(r"protoreflect\.(\w+):\s*\"([^\"]+)\"", block.group(1))
    )
    for kind, want in _GO_EXPECTED.items():
        assert kind in actual, f"Go 侧没有 {kind} 的映射了：{sorted(actual)}"
        assert actual[kind].upper() == want, (
            f"手抄的 Go 映射过期了：{kind} 实际是 {actual[kind]!r}，本文件写的是 {want!r}。"
            f"请同时更新 _GO_EXPECTED 与 protosql._TYPE_MAP。"
        )


def test_generated_ddl_uses_the_go_types() -> None:
    """端到端：真的建出来的 DDL 里是 MEDIUMTEXT 而不是 VARCHAR。"""
    from pandora.data_service.v1 import data_service_pb2 as dpb

    ddl = protosql.schema_of(dpb.PlayerData).create_table_sql().upper()
    assert "MEDIUMTEXT" in ddl, f"nickname/avatar 没建成 MEDIUMTEXT：\n{ddl}"
    assert "VARCHAR(255)" not in ddl, f"仍在用 VARCHAR(255)：\n{ddl}"
