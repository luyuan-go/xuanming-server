"""变异探针残留的机械闸。

## 这道闸挡的是什么

本项目要求每条新测试做**变异验证**:把产品代码里对应的那一行改坏,确认测试变红,
再改回。做法本身没问题 —— 问题是"再改回"这一步靠人记性,而它偏偏是唯一一步
**忘了也不会有任何反馈**的:探针的形状就是"让某个判断恒不成立",于是测试照样绿、
lint 照样过、类型检查照样过,只有真实故障发生时才会发现那道闸早就关了。

这不是假想。2026-08-19 移植 hub_allocator owner 三件套时,一次就残留了 5 处,其中
`owner_authority.py` 的:

    if not owner_record_exactly_targets(next_rec, owner_type, target) and False:

正是 `CLAUDE.md` §9 不变量 22 要求的 fail-closed 校验 —— 它挡的是"owner 权威回传了
一个并非本次意图 target 的记录"。`and False` 让这条恒不触发,等于允许把玩家归属
写到**不是自己请求的那台 DS** 上,而调用方还以为 Begin 成功了。那次是靠同文件另一处
探针写坏了缩进、Python 直接拒绝 import 才暴露的 —— 换句话说,**是运气**。

## 为什么挡"形状"而不是挡"标记"

按注释标记(`# MUT...`)扫是没用的:忘了还原的人同样会忘了写标记,而且标记只要不写
就绕过了。这里改成扫**恒定式布尔短路**这个语法形状 —— `and False` / `or True` 让
整个条件退化成常量,是变异探针唯一必须使用、而正常业务代码几乎不会写的构造
(要恒真恒假,直接写 `if True:` 或把分支删掉,不会绕这么一圈)。

同理用 AST 而不是正则:正则分不清字符串字面量、注释、以及 `and False_flag` 这类
前缀撞名的标识符。误报的检查最终会被整条删掉,连它本来能抓的真缺陷一起没了。

## 覆盖范围

只扫 `pandorapy/`(产品代码)。测试代码里写 `and False` 有正当用途(构造参数化的
恒假条件、对照实验),不在本闸范围内。
"""

from __future__ import annotations

import ast
import pathlib

import pytest

_PANDORAPY = pathlib.Path(__file__).resolve().parents[1] / "pandorapy"


def _python_files() -> list[pathlib.Path]:
    return sorted(_PANDORAPY.rglob("*.py"))


def _is_const(node: ast.expr, want: bool) -> bool:
    """判断节点是否就是字面量 `True` / `False`。

    只认 `ast.Constant` 且值**是 bool**:`1` / `0` 虽然真值相同,但写 `and 0` 不是
    探针的惯用形状,认它只会增加误报。
    """
    return isinstance(node, ast.Constant) and node.value is want


class _ShortCircuitVisitor(ast.NodeVisitor):
    """收集 `... and False` / `... or True` 这类使整式退化为常量的布尔运算。"""

    def __init__(self) -> None:
        self.hits: list[tuple[int, str]] = []

    def visit_BoolOp(self, node: ast.BoolOp) -> None:  # noqa: N802 (ast 约定的驼峰)
        want = False if isinstance(node.op, ast.And) else True
        op = "and False" if isinstance(node.op, ast.And) else "or True"
        # 首项不算:`if False and expensive()` 是有意的短路禁用写法,而探针的形状
        # 一定是"保留原判断、在后面缀一个常量",即常量出现在**非首位**。
        for operand in node.values[1:]:
            if _is_const(operand, want):
                self.hits.append((node.lineno, op))
        self.generic_visit(node)


def _scan(path: pathlib.Path) -> list[tuple[int, str]]:
    visitor = _ShortCircuitVisitor()
    visitor.visit(ast.parse(path.read_text(encoding="utf-8"), filename=str(path)))
    return visitor.hits


def test_no_constant_short_circuit_in_product_code() -> None:
    """产品代码里不得残留恒定式布尔短路。

    命中后的处理**只有一种**:回去看那一行原本的判断是什么,把常量删掉。不要用
    `# noqa` 压掉 —— 被压掉的每一条都是一道已经关掉、且不会再有人发现的闸。
    """
    offenders: list[str] = []
    for path in _python_files():
        for lineno, op in _scan(path):
            rel = path.relative_to(_PANDORAPY.parent)
            offenders.append(f"{rel}:{lineno}: 形如 `... {op}`")

    assert not offenders, (
        "发现恒定式布尔短路,极可能是变异验证后忘了还原的探针;"
        "它会让对应的判断永久失效而测试仍然全绿:\n  " + "\n  ".join(offenders)
    )


@pytest.mark.parametrize(
    "src",
    [
        "if not ok(x) and False:\n    raise E()\n",
        "if retry_after > 0 and False:\n    pass\n",
        "if err is None or True:\n    pass\n",
        "if a and b and False:\n    pass\n",
    ],
)
def test_gate_catches_probe_shapes(tmp_path: pathlib.Path, src: str) -> None:
    """闸本身必须真的能抓到探针 —— 否则它只是一条永远绿的装饰。

    这些样本抄自 2026-08-19 那次真实残留的四种形状。
    """
    f = tmp_path / "probe.py"
    f.write_text(src, encoding="utf-8")
    assert _scan(f), f"闸漏掉了探针形状:{src!r}"


@pytest.mark.parametrize(
    "src",
    [
        # 首位常量:有意的整块禁用,不是探针
        "if False and expensive():\n    pass\n",
        # 正常业务布尔
        "if a and b:\n    pass\n",
        "ok = x is None or y > 0\n",
        # 前缀撞名的标识符:正则会误报,AST 不会
        "if flag and False_LIKE:\n    pass\n",
        # 字符串/注释里出现:正则会误报,AST 不会
        "s = 'and False'  # or True\n",
        # `and 0` 真值相同但不是探针形状,刻意不认(认了只增误报)
        "if flag and 0:\n    pass\n",
    ],
)
def test_gate_does_not_false_positive(tmp_path: pathlib.Path, src: str) -> None:
    """误报的代价比漏报大:会误报的检查最终会被整条删掉,连真缺陷一起漏。"""
    f = tmp_path / "clean.py"
    f.write_text(src, encoding="utf-8")
    assert not _scan(f), f"闸误报了正常代码:{src!r}"
