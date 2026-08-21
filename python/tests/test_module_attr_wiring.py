"""机械闸：**跨模块属性引用必须真的存在**。

## 这条闸挡的是什么

2026-08-21 的对拍验证里，Python 版 `ds_allocator` 连启动都做不到，连着撞了四个
`AttributeError`：

    uc.set_noshow_recorder(...)             → 实际叫 set_no_show_recorder
    cfg.allocator.noshow_ledger_window_td() → 实际叫 no_show_ledger_window_td
    dsrepo.KafkaDSLifecyclePusher(producer) → 这个类**根本不存在**（漏移植）
    dsgm.Service(rdb)                       → 实际叫 GmService

而当时 4669 条单测**全绿**。原因很直白：单测测的是 biz / repo / service 层，
装配代码（`main.py` 的 `_main_async`）没有任何测试会执行到它——它要 Redis、要
Kafka、要监听端口。于是"每一块零件都对，但拼不起来"这一整类缺陷对测试完全隐形。

## 判据

对 `pandorapy/**` 每个模块，解析 `import a.b.c as X` / `from a.b import c as X`
形式绑定到**本仓模块**的别名，然后检查源码里每一处 `X.Name`：`Name` 必须在被引
模块的顶层定义中出现（class / def / 赋值 / `__all__` / 再导出的 import 别名）。

命中的是"引用了另一个模块里不存在的名字"这一类——`dsrepo.KafkaDSLifecyclePusher`
和 `dsgm.Service` 都在此列。实例属性（`uc.xxx` / `cfg.allocator.xxx`）需要类型推导，
不在本闸射程内，靠对拍探针把服务真正拉起来兜底。

## 为什么用 AST 而不是正则

`srcprobe` 那份 docstring 已经讲透了：注释和 docstring 也是源码文本。这里更进一步
直接用 AST —— 判据本身就是"某个 `Attribute` 节点的 `value` 是不是模块别名"，正则
表达不了。
"""

from __future__ import annotations

import ast
import pathlib

import pytest

PANDORAPY = pathlib.Path(__file__).resolve().parents[1] / "pandorapy"

#: 本仓模块的顶层包名。只有绑定到这些包的别名才纳入检查——三方库（redis、grpc、
#: prometheus_client）的属性面千奇百怪（动态生成、C 扩展、`__getattr__`），静态解析
#: 会产生大量假阳性，而它们的错误用起来立刻就炸，不是本闸要防的隐形缺陷。
FIRST_PARTY_ROOT = "pandorapy"


def _iter_modules() -> list[pathlib.Path]:
    return sorted(p for p in PANDORAPY.rglob("*.py") if "__pycache__" not in p.parts)


def _module_path(dotted: str) -> pathlib.Path | None:
    """`pandorapy.services.ds_allocator.repo` → 磁盘路径。找不到返回 None。"""
    if not dotted.startswith(FIRST_PARTY_ROOT + "."):
        return None
    rel = pathlib.Path(*dotted.split(".")[1:])
    cand = PANDORAPY / rel.with_suffix(".py")
    if cand.is_file():
        return cand
    pkg = PANDORAPY / rel / "__init__.py"
    return pkg if pkg.is_file() else None


def _exported_names(path: pathlib.Path) -> set[str]:
    """模块顶层**可被别人 `mod.X` 取到**的名字。

    ★ 必须把 `import`（含再导出）算进来：`from pandorapy import errcode` 之后
      `othermod.errcode` 是合法的。漏掉会造成假阳性。
    ★ `try/except ImportError` 之类嵌套结构里的顶层定义同样可见，所以用 `ast.walk`
      而不是只看 `tree.body`——宁可放宽也不误报。
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            names.add(node.id)
        elif isinstance(node, ast.alias):
            names.add((node.asname or node.name).split(".")[0])
    return names


def _module_aliases(tree: ast.AST) -> dict[str, str]:
    """别名 → 本仓模块 dotted 名。只收**模块**别名，不收 `from mod import Class`。"""
    out: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                dotted = a.name
                if dotted.startswith(FIRST_PARTY_ROOT + "."):
                    out[a.asname or dotted.split(".")[0]] = dotted
        elif isinstance(node, ast.ImportFrom):
            if node.level or not node.module:
                continue  # 相对导入不解析：本仓装配代码一律用绝对导入
            for a in node.names:
                dotted = f"{node.module}.{a.name}"
                if _module_path(dotted) is not None:
                    out[a.asname or a.name] = dotted
    return out


def _violations(path: pathlib.Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    aliases = _module_aliases(tree)
    if not aliases:
        return []
    cache: dict[str, set[str]] = {}
    bad: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Attribute) or not isinstance(node.value, ast.Name):
            continue
        dotted = aliases.get(node.value.id)
        if dotted is None:
            continue
        target = _module_path(dotted)
        if target is None:
            continue
        if dotted not in cache:
            cache[dotted] = _exported_names(target)
        if node.attr not in cache[dotted]:
            bad.append(f"{path.name}:{node.lineno} {node.value.id}.{node.attr} → {dotted} 里没有这个名字")
    return bad


@pytest.mark.parametrize("path", _iter_modules(), ids=lambda p: str(p.relative_to(PANDORAPY)))
def test_cross_module_attribute_exists(path: pathlib.Path) -> None:
    """引用别的模块的名字时，那个名字必须真的存在。"""
    bad = _violations(path)
    assert not bad, "\n".join(bad)


def test_gate_is_not_vacuous() -> None:
    """非空性哨兵：这条闸必须真的解析出了成规模的跨模块引用。

    没有它，`_module_aliases` 哪天回归成永远返回空 dict，上面几百个用例会**全绿**
    地什么都不查——这正是 CLAUDE.md 反复强调的"机械检查必须配非空性哨兵"。
    """
    total = 0
    for path in _iter_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        aliases = _module_aliases(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                if node.value.id in aliases:
                    total += 1
    assert total >= 500, f"只解析出 {total} 处跨模块属性引用，闸大概率已失效"
