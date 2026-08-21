"""按源码做机械断言时的取源器：**只留代码，抹掉注释与 docstring**。

## 为什么需要它

本仓有大量「接线还在不在」的机械断言，形状都是：

    src = pathlib.Path(xxx_main.__file__).read_text(encoding="utf-8")
    assert "uc.set_player_no_resolver(player_no_resolver)" in src

这类断言有一个**共同的失效模式**：注释和 docstring 也是源码文本。把真正的
调用删掉、只留一句提到它的注释（或者在模块 docstring 里写一句「这里接了
`uc.set_player_no_resolver(...)`」），断言照样绿。而删接线时**最可能顺手写下的
恰恰就是这样一句注释** —— 于是检查在最需要它的那一刻失灵。

这不是假想。本仓已经栽过两次：

1. `_unreferenced_runner_defs` 第一版用 `re.findall(name, src)` 数出现次数，
   被补接线时留的那句注释抵消掉；
2. `test_cancelled_error_is_re_raised_before_any_broad_except` 第一版判据是
   「前 12 行里出现过 `CancelledError` 字符串」。把真的
   `except asyncio.CancelledError: raise` 删掉、只留那句解释性注释，
   **238 个用例全绿**（实测）。改成 AST 后同一变异立刻被抓（第 435 行）。

`test_login_main.py` 里那句手工跳过模块 docstring 的 `.split(...)`（按三引号切两刀
再取后半段）是同一个坑的第三个现场 —— 只是当时就地打了个补丁，没有归到一处。

## 处理办法

能用 AST 表达的判据一律用 AST（`test_service_layer_contract.py` 里的那几条就是）。
但「某个调用/某个字面量还在不在」这类断言用 AST 表达要为每种形状写一套匹配，
按 `CLAUDE.md §15.2` 属于把简单问题复杂化。所以这里走另一条同样可靠的路：

**把注释和 docstring 替换成等量空白，代码原样保留**，之后仍旧用子串匹配。

替换成空白（而不是删除）是为了保住行列结构 —— 断言里的多行片段、以及失败信息
里的行号，都还能对得上。

## 边界

- 只抹 module / class / def 的**首条**字符串表达式（即 docstring）。代码里真正
  用到的字符串字面量（Redis key 前缀、日志事件名、错误码文案）全部保留 ——
  那些正是断言要找的东西。
- docstring 按**整行**抹。理由：`ast` 的 `col_offset` 是 **UTF-8 字节偏移**，而
  本仓 docstring 里全是中文，字节偏移和字符下标对不上；docstring 又总是独占若干
  行，整行抹既避开了这个换算坑，也不会误伤同行的代码。
- 注释按 token 的精确列范围抹（`tokenize` 给的是字符偏移，无此问题），所以
  `x = 1  # 说明` 里的 `x = 1` 完整保留。
- 抹完的文本**不保证还能被 `ast.parse`**（例如函数体只有一句 docstring 时会变成
  空体）。它只供子串匹配使用，不要拿去再解析。
"""

from __future__ import annotations

import ast
import io
import pathlib
import tokenize
from types import ModuleType

__all__ = ["code_text", "module_code_text"]

_DEF_NODES = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)


def _blank_cols(rows: list[list[str]], lineno: int, col: int, end_lineno: int, end_col: int) -> None:
    """把 [lineno,col) → (end_lineno,end_col] 这段字符换成空格（换行符保留）。"""
    for ln in range(lineno, end_lineno + 1):
        row = rows[ln - 1]
        start = col if ln == lineno else 0
        stop = end_col if ln == end_lineno else len(row)
        for i in range(start, min(stop, len(row))):
            if row[i] not in ("\n", "\r"):
                row[i] = " "


def _blank_lines(rows: list[list[str]], lineno: int, end_lineno: int) -> None:
    """整行抹（用于 docstring，见模块 docstring 里的「边界」）。"""
    for ln in range(lineno, end_lineno + 1):
        row = rows[ln - 1]
        for i, ch in enumerate(row):
            if ch not in ("\n", "\r"):
                row[i] = " "


def code_text(src: str | pathlib.Path) -> str:
    """返回抹掉注释与 docstring 的源码。传路径则先读文件。"""
    if isinstance(src, pathlib.Path):
        src = src.read_text(encoding="utf-8")
    rows = [list(line) for line in src.splitlines(keepends=True)]

    # ① 注释：tokenize 给的是字符偏移，可以精确到列
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type == tokenize.COMMENT:
            _blank_cols(rows, tok.start[0], tok.start[1], tok.end[0], tok.end[1])

    # ② docstring：在**原始**源码的 AST 上定位（抹注释不改变行列，位置仍然有效）
    for node in ast.walk(ast.parse(src)):
        if not isinstance(node, _DEF_NODES):
            continue
        body = node.body
        if not body:
            continue
        first = body[0]
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
            and first.end_lineno is not None
        ):
            _blank_lines(rows, first.lineno, first.end_lineno)

    return "".join("".join(row) for row in rows)


def module_code_text(mod: ModuleType) -> str:
    """`code_text` 的便捷入口：直接给一个已 import 的模块。"""
    assert mod.__file__ is not None, f"{mod!r} 没有 __file__，拿不到源码"
    return code_text(pathlib.Path(mod.__file__))
