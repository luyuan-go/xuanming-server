"""proto 枚举名格式化的机械闸。

## 这道闸挡的是什么

proto3 枚举是**开放**的:滚动升级期的旧副本、存储里的陈旧记录、上游新版本发来的
消息,都可以合法携带一个本副本不认识的数值。Python 的 `EnumTypeWrapper.Name()`
对未知值**抛 ValueError**,而 Go 生成代码的 `.String()` 返回数字串、永不 panic。

这 24 个调用点当初全在日志/错误消息的格式化路径上 —— 于是未知枚举值不是让日志
少一行,而是把整条业务路径炸掉,且**只在混版窗口触发**(单测、单版本联调、压测
全绿)。详见 `pandorapy/protoenum.py` 的模块头。

## 为什么用 AST 而不是正则

handoff §5.10 记了四次机械检查自己出错的教训,其中三次换成 AST 就对了。这里同样
用 AST:正则区分不了 `x.Name(...)`(枚举格式化,危险)与 `foo.Name`(属性读取,
无害),也区分不了字符串/注释里出现的 `.Name(`。

**误报的代价比漏报还大** —— 一条会误报的检查最终会被 `# noqa` 掉或整条删掉,
连它本来能抓住的真缺陷也一起没了。所以这里只认**方法调用**这一种形状。
"""

from __future__ import annotations

import ast
import pathlib

import pytest

_PANDORAPY = pathlib.Path(__file__).resolve().parents[1] / "pandorapy"

# `pandorapy/protoenum.py` 自己必须调 `.Name()` —— 它就是那个唯一的收口点。
_ALLOWED = {_PANDORAPY / "protoenum.py"}


def _python_files() -> list[pathlib.Path]:
    return sorted(p for p in _PANDORAPY.rglob("*.py") if p not in _ALLOWED)


class _BareEnumNameVisitor(ast.NodeVisitor):
    """收集形如 `<任意表达式>.Name(<参数>)` 的方法调用。"""

    def __init__(self) -> None:
        self.hits: list[int] = []

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802 (ast 约定的驼峰)
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == "Name":
            self.hits.append(node.lineno)
        self.generic_visit(node)


def test_no_bare_proto_enum_name_calls() -> None:
    """新增的裸 `.Name(` 一律判失败,必须改用 `protoenum.enum_name()`。

    ★ 这条测试自己做过变异验证:在 `hub_allocator/locator_client.py` 里把
      `enum_name(...)` 改回 `locator_pb2.LocationState.Name(...)`,本用例当场变红,
      同时 `test_hub_allocator_repo.py::test_locator_uncertain_states_are_fail_closed`
      也从"抛 ErrUnavailable"退化成"抛 ValueError"。两条一起证明这道闸有牙。
    """
    violations: list[str] = []
    for path in _python_files():
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - 并发会话写到一半的文件
            continue
        visitor = _BareEnumNameVisitor()
        visitor.visit(tree)
        rel = path.relative_to(_PANDORAPY.parent)
        violations.extend(f"{rel}:{line}" for line in visitor.hits)

    assert not violations, (
        "以下位置直接调用了 proto 枚举的 `.Name()`,未知枚举值会抛 ValueError 把\n"
        "日志/错误消息的格式化路径炸掉(只在滚动升级的混版窗口触发)。\n"
        "改用 `from pandorapy.protoenum import enum_name` -> `enum_name(Enum, v)`。\n"
        "理由见 pandorapy/protoenum.py 模块头。\n  " + "\n  ".join(violations)
    )


@pytest.mark.parametrize("bad_value", [99, -1, 2**31 - 1])
def test_enum_name_never_raises_on_unknown_values(bad_value: int) -> None:
    """未知值回落成十进制数字串 —— 与 Go `.String()` 实测结果逐字相同。

    Go 侧实测(2026-08-19,`go run` 真跑):
        LocationState(99).String()  == "99"
        LocationState(-1).String()  == "-1"
    """
    from pandora.locator.v1 import locator_pb2

    from pandorapy.protoenum import enum_name

    assert enum_name(locator_pb2.LocationState, bad_value) == str(bad_value)


def test_enum_name_returns_declared_name_for_known_values() -> None:
    """已知值仍必须返回 proto 里声明的名字,不能一律退化成数字。

    少了这条断言,把 `enum_name` 写成 `return str(value)` 也能让上面那条通过 ——
    那样两栈日志会**全部**变成数字,按枚举名建的 Loki 查询对 Python 副本失明。
    """
    from pandora.locator.v1 import locator_pb2

    from pandorapy.protoenum import enum_name

    assert (
        enum_name(locator_pb2.LocationState, locator_pb2.LOCATION_STATE_HUB)
        == "LOCATION_STATE_HUB"
    )
