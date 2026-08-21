"""`tests.srcprobe` 自身的守卫。

`srcprobe` 是给别的机械断言用的取源器 —— 它要是坏了（或者悄悄退化成
「原样返回」），依赖它的那二十来条接线断言会**一起变成恒绿**，而且不会有任何
报错。所以它必须有自己的正反用例。

反面用例都取自真实栽过的形状：注释顶替真实调用、docstring 里抄一遍接线代码。
"""

from __future__ import annotations

import ast
import pathlib
import textwrap

import pytest

from tests.srcprobe import code_text, module_code_text


def test_comment_is_not_evidence_of_wiring() -> None:
    """★ 核心：注释里提到某个调用，不能算这个调用还在。

    这正是 `_unreferenced_runner_defs` 和 CancelledError 检查各栽过一次的形状。
    """
    src = textwrap.dedent(
        """
        def wire(uc, resolver):
            # 这里原本调 uc.set_player_no_resolver(resolver),已挪到 biz 层
            pass
        """
    )
    assert "uc.set_player_no_resolver(resolver)" in src, "前提：原始文本里确实有"
    assert "uc.set_player_no_resolver(resolver)" not in code_text(src)


def test_docstring_is_not_evidence_of_wiring() -> None:
    """docstring 同样不算数 —— 本仓 docstring 大量逐字抄接线代码。"""
    src = textwrap.dedent(
        '''
        """装配说明：这里会调 uc.set_player_no_resolver(resolver)。"""

        def wire(uc, resolver):
            pass
        '''
    )
    assert "uc.set_player_no_resolver(resolver)" not in code_text(src)


def test_real_call_survives() -> None:
    """正面：真实调用必须原样保留，否则断言会全线误报。"""
    src = textwrap.dedent(
        """
        def wire(uc, resolver):
            uc.set_player_no_resolver(resolver)
        """
    )
    assert "uc.set_player_no_resolver(resolver)" in code_text(src)


def test_real_string_literals_survive() -> None:
    """代码里的字符串字面量不是 docstring，必须留下。

    Redis key 前缀、日志事件名、错误文案都靠它们做跨栈对拍。
    """
    src = textwrap.dedent(
        '''
        def f():
            """说明"""
            logger.info("player_no_allocated")
            return "pandora:player:name-resolve:nonce:"
        '''
    )
    out = code_text(src)
    assert '"player_no_allocated"' in out
    assert '"pandora:player:name-resolve:nonce:"' in out
    assert "说明" not in out


def test_trailing_comment_does_not_eat_the_code_on_its_line() -> None:
    """行尾注释只抹注释部分，同行代码要留住。"""
    src = "x = compute(1)  # 说明:这里是 1 不是 0\n"
    out = code_text(src)
    assert "x = compute(1)" in out
    assert "说明" not in out


def test_line_structure_is_preserved() -> None:
    """抹成等量空白而不是删除 —— 行数与行号必须对得上。

    否则断言失败时报出来的行号会指向错误的位置。
    """
    src = textwrap.dedent(
        '''
        """模块说明
        第二行
        第三行
        """

        def f():
            # 注释
            return 1
        '''
    )
    assert len(code_text(src).splitlines()) == len(src.splitlines())


def test_chinese_docstring_does_not_corrupt_following_code() -> None:
    """中文 docstring 的边界换算坑：`ast` 的 col_offset 是 UTF-8 **字节**偏移。

    docstring 走整行抹就是为了绕开它。这条用例把中文塞满 docstring，
    确认紧随其后的代码一个字符都没被吃掉。
    """
    src = textwrap.dedent(
        '''
        def f():
            """取消必须穿透:CancelledError 是 BaseException,会被宽 except 吞掉。"""
            uc.set_player_no_resolver(resolver)
            return "pandora:player:name-resolve:nonce:"
        '''
    )
    out = code_text(src)
    assert "uc.set_player_no_resolver(resolver)" in out
    assert '"pandora:player:name-resolve:nonce:"' in out
    assert "CancelledError" not in out


def test_class_and_nested_def_docstrings_are_stripped() -> None:
    """class 与嵌套 def 的 docstring 也要抹，不只是模块级。"""
    src = textwrap.dedent(
        '''
        class A:
            """类说明 marker_class"""

            def m(self):
                """方法说明 marker_method"""

                def inner():
                    """内层说明 marker_inner"""
                    return 1

                return inner
        '''
    )
    out = code_text(src)
    for marker in ("marker_class", "marker_method", "marker_inner"):
        assert marker not in out, f"{marker} 没被抹掉"


def test_module_code_text_matches_code_text() -> None:
    """`module_code_text` 只是便捷入口，结果必须与 `code_text` 一致。"""
    from pandorapy.services.leaderboard import main as lbmain

    path = pathlib.Path(lbmain.__file__)
    assert module_code_text(lbmain) == code_text(path)


@pytest.mark.parametrize(
    "path",
    sorted(
        (pathlib.Path(__file__).resolve().parents[1] / "pandorapy" / "services").glob("*/main.py")
    ),
    ids=lambda p: p.parent.name,
)
def test_code_text_is_lossless_on_real_sources(path: pathlib.Path) -> None:
    """在**真实**服务源码上验证：抹掉的只有注释和 docstring。

    判据不看文本，看 AST —— 抹之前和抹之后，去掉 docstring 的语法树必须完全相同。
    这条能挡住「列偏移算错，多吃了一个字符」这类不会立刻表现为语法错误的退化。
    """
    original = path.read_text(encoding="utf-8")
    stripped = code_text(original)

    def norm(src: str) -> str:
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                # 去掉 docstring 后可能剩下空体，补一个 pass 保证可 unparse
                node.body = body[1:] or [ast.Pass()]
        return ast.unparse(ast.fix_missing_locations(tree))

    assert norm(stripped) == norm(original), f"{path.name} 被 code_text 改坏了"
