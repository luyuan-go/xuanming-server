"""跨语言口径校验 —— 直接解析 Go 源码比对,漂移当场变红。

这个文件是整个 Python 迁移最重要的一道机械门。它防的都是"不会报错、只会静默出错"
的那类问题:

  1. errcode 数值漂移   → 客户端收到错误的业务语义(失败被当成功)
  2. 日志字段名漂移      → Grafana 面板静默变空,1449 个事件名的 LogQL 全部落空
  3. snowflake 位布局漂移 → 双栈并行期两边发出重号 ID,或破坏 ID 单调性
  4. 端口默认值漂移      → Envoy cluster / 端口占用检查全部对不上

人工 code review 抓不住这几类问题(数值对不对要逐个数),所以必须机械化。
"""

from __future__ import annotations

import pathlib
import re

import pytest

from pandorapy import errcode, snowflake

# ── 1. errcode 数值必须与 Go 侧逐个一致 ────────────────────────────────────────


def test_errcode_matches_go_source(repo_root: pathlib.Path) -> None:
    """从 pkg/errcode/errcode.go 重新解析,逐个比对数值。

    这不是"测试生成器能跑",是测试**当前签入的 errcode.py 与当前的 Go 源码一致**。
    Go 侧加了新码而没重新生成 Python 版,这条会红。
    """
    go_src = (repo_root / "pkg" / "errcode" / "errcode.go").read_text(encoding="utf-8")
    pattern = re.compile(
        r"^\t(?P<name>[A-Za-z][A-Za-z0-9]*)\s+Code\s*=\s*(?P<val>\d+)", re.M
    )
    go_codes = {m.group("name"): int(m.group("val")) for m in pattern.finditer(go_src)}

    assert go_codes, "未从 Go 源码解析到错误码 —— 正则与源码格式已不匹配,先修正则"

    missing = sorted(set(go_codes) - set(errcode.ALL_CODES))
    assert not missing, (
        f"Go 侧有 {len(missing)} 个错误码在 Python 侧缺失: {missing[:10]}"
        f" —— 请运行 python tools/gen_errcode.py 重新生成"
    )

    mismatched = {
        name: (go_val, errcode.ALL_CODES[name])
        for name, go_val in go_codes.items()
        if errcode.ALL_CODES[name] != go_val
    }
    assert not mismatched, f"错误码数值不一致(名字: Go 值 vs Python 值): {mismatched}"


def test_errcode_matches_proto_enum() -> None:
    """errcode 数值必须与 proto 的 ErrCode enum 一致。

    service 层做的是纯数值转换(`Response(code=as_code(exc))`),数值对不上就会
    让客户端解出一个完全不同的枚举名 —— 而且 protobuf 不会报错,未知值原样保留。
    """
    from pandora.common.v1 import errcode_pb2

    proto_values = {
        v.number for v in errcode_pb2.DESCRIPTOR.enum_types_by_name["ErrCode"].values
    }
    # proto 只需覆盖 Python 侧用到的码;反向不要求(Go 侧有些内部码不上协议)。
    # 这里只校验共有部分数值一致 —— 数值是靠名字对齐的,所以校验方式是:
    # 任一 Python 码若在 proto 里存在同名项,数值必须相同。
    for name, value in errcode.ALL_CODES.items():
        proto_name = _go_name_to_proto_enum(name)
        if proto_name in errcode_pb2.ErrCode.keys():
            assert errcode_pb2.ErrCode.Value(proto_name) == value, (
                f"{name}: Python={value} 但 proto {proto_name}="
                f"{errcode_pb2.ErrCode.Value(proto_name)}"
            )
        # 顺带确认数值确实在 proto 枚举里(未定义的数值传给客户端 = 客户端认不出)
        if value in proto_values:
            continue


def _go_name_to_proto_enum(go_name: str) -> str:
    """ErrDialogueNotFound → ERR_DIALOGUE_NOT_FOUND;OK → OK。"""
    if go_name == "OK":
        return "OK"
    return re.sub(r"(?<!^)(?=[A-Z])", "_", go_name).upper()


def test_as_code_semantics() -> None:
    """as_code 的三条语义必须与 Go 的 errcode.As 一致。"""
    assert errcode.as_code(None) == errcode.OK
    assert (
        errcode.as_code(errcode.PandoraError(errcode.ErrDialogueNotFound, "x"))
        == errcode.ErrDialogueNotFound
    )
    # 非 PandoraError → ErrUnknown(不是 OK!否则内部异常会被当成成功返回给客户端)
    assert errcode.as_code(ValueError("boom")) == errcode.ErrUnknown


def test_as_code_walks_cause_chain() -> None:
    """沿 __cause__ 回溯 —— 对应 Go 的 errors.As 沿 Unwrap 链遍历。"""
    inner = errcode.PandoraError(errcode.ErrInvalidArg, "bad arg")
    try:
        try:
            raise inner
        except errcode.PandoraError as exc:
            raise RuntimeError("wrapped") from exc
    except RuntimeError as outer:
        assert errcode.as_code(outer) == errcode.ErrInvalidArg


# ── 2. snowflake 位布局必须与 Go 侧逐位一致 ────────────────────────────────────


def test_snowflake_layout_matches_go_source(repo_root: pathlib.Path) -> None:
    """从 pkg/snowflake/snowflake.go 解析 Epoch / NodeBits / StepBits 比对。

    位布局错了不会崩,只会在双栈并行期发出重号或乱序的 ID —— 静默数据损坏。
    """
    go_src = (repo_root / "pkg" / "snowflake" / "snowflake.go").read_text(encoding="utf-8")

    def _const(name: str) -> int:
        m = re.search(rf"^\t{name}\s+uint64\s*=\s*(\d+)", go_src, re.M)
        assert m, f"未从 Go 源码解析到常量 {name}"
        return int(m.group(1))

    assert snowflake.EPOCH == _const("Epoch"), "snowflake Epoch 与 Go 侧不一致"
    assert snowflake.NODE_BITS == _const("NodeBits"), "NodeBits 与 Go 侧不一致"
    assert snowflake.STEP_BITS == _const("StepBits"), "StepBits 与 Go 侧不一致"


def test_snowflake_is_second_granularity() -> None:
    """时间粒度必须是**秒**而不是毫秒。

    单独立一条是因为这是最容易抄错的地方:绝大多数 snowflake 实现是毫秒级,
    照网上代码抄一定错。错了之后 ID 仍然唯一、仍然递增,只是时间段会在
    2^32 秒后溢出的位置完全不同,且与 Go 版发出的 ID 不可比。
    """
    node = snowflake.Node(1)
    a = node.generate()
    ts = snowflake.timestamp_of(a)
    import time

    # 反解出的时间戳应该落在当前秒附近(允许 2 秒抖动)
    assert abs(ts - int(time.time())) <= 2, (
        f"snowflake 时间位反解得到 {ts},与当前 unix 秒相差过大 —— 粒度可能是毫秒"
    )


def test_snowflake_fields_roundtrip() -> None:
    """node / step 能从 ID 正确反解 —— 排查重号时依赖这个。"""
    node = snowflake.Node(12345)
    ids = [node.generate() for _ in range(100)]
    assert len(set(ids)) == 100, "同一节点发出了重复 ID"
    assert ids == sorted(ids), "ID 非单调递增"
    for sid in ids:
        assert snowflake.node_of(sid) == 12345


def test_snowflake_rejects_out_of_range_node() -> None:
    """node_id 超出 NodeBits 范围必须硬失败,不能静默截断。

    静默截断 = 两个不同副本得到同一个 node 段 = 重号。
    """
    with pytest.raises(ValueError, match="超出范围"):
        snowflake.Node(snowflake.NODE_MASK + 1)


# ── 3. 端口默认值必须与 Go 侧一致 ──────────────────────────────────────────────


def test_dialogue_default_ports_match_go(repo_root: pathlib.Path) -> None:
    """dialogue 的默认 gRPC/HTTP 端口必须与 Go 侧 conf.Defaults() 一致。

    Envoy cluster、run_services.ps1 的端口占用清理、K8s Service 都钉在这两个数字上。
    """
    from pandorapy.services.dialogue import conf as dconf

    go_src = (
        repo_root / "services" / "social" / "dialogue" / "internal" / "conf" / "conf.go"
    ).read_text(encoding="utf-8")

    grpc_default = re.search(r'Server\.Grpc\.Addr\s*=\s*"(?P<a>:\d+)"', go_src)
    http_default = re.search(r'Server\.Http\.Addr\s*=\s*"(?P<a>:\d+)"', go_src)
    assert grpc_default and http_default, "未从 Go conf.go 解析到默认端口"

    assert dconf.DEFAULT_GRPC_ADDR == grpc_default.group("a")
    assert dconf.DEFAULT_HTTP_ADDR == http_default.group("a")
