#!/usr/bin/env python3
"""RPC 方法级迁移覆盖矩阵（Go ↔ Python），机械推导、不靠手写。

**为什么要有它**：`docs/design/python-migration.md` 的服务清单是手写的，会漂移。
这个脚本把三份事实拼起来，每次跑都是当下的真相：

  ① proto 里声明了哪些 rpc            —— `proto/pandora/**/*.proto`
  ② Go 侧哪个服务注册了哪个 servicer  —— `services/*/*/internal/server/*.go` 的 `RegisterXxxServer(`
  ③ Python 侧实现了哪些方法           —— `python/pandorapy/services/*/**.py` 里继承 `*Servicer` 的类

②是关键：**一个 Go 服务经常注册不止一个 servicer**（inventory 还挂了 BagService、
guild 还挂了 GroupService、ds_allocator 还挂了 GmService、四个服务还挂了
ConfigTableAdminService），只看与服务同名的那个 proto 会把 RPC 面算少。
这些"搭车"的 servicer 正是最容易在迁移时整个忘掉的。

**`--check` 门禁**（可进 CI）刻意只拦"静默"的那一类，不拦"迁移未完成"：

  - Python 的 servicer 子类里有一个 **proto 里不存在**的方法名 → 拼错的方法**永远不会被调用**，
    grpcio 按 proto 名分发，回落到基类的 UNIMPLEMENTED。没有任何启动期信号，
    只在那个 RPC 被调时才炸。
  - Python 有名字像 servicer 的类却**没继承**生成的 `*Servicer` 基类 → 注册时不会报错，
    但缺失的方法也不会有 UNIMPLEMENTED 兜底。

"Python 还没实现某个 rpc"是**预期状态**，只报告不判失败 —— 否则第一天就是红的，
门禁会立刻被关掉（§7.4：能拦住人的只有一条会变红的机械检查，前提是它平时是绿的）。

用法（从任何 cwd 都能跑）：

    python tools/parity/coverage.py                  # markdown 矩阵
    python tools/parity/coverage.py --json           # 机器可读
    python tools/parity/coverage.py --check          # CI 门禁，静默缺陷 → exit 1
    python tools/parity/coverage.py --service owner  # 只看一个服务
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

# 无论从哪个 cwd 跑都能定位仓库根：本文件在 <root>/python/tools/parity/ 下。
# （与 probe_*.py 同一套做法 —— 那两个探针曾经只能在 python/ 下跑，踩过。）
_HERE = Path(__file__).resolve()
ROOT = _HERE.parents[3]
PROTO_DIR = ROOT / "proto"
SERVICES_DIR = ROOT / "services"
PY_SERVICES_DIR = ROOT / "python" / "pandorapy" / "services"

sys.path.insert(0, str(ROOT / "python"))
try:
    from pandorapy import _utf8  # noqa: F401  —— Windows cp1252 stdout 会盖掉真实错误
except Exception:  # pragma: no cover —— 独立跑时容忍缺失
    pass


_RPC_RE = re.compile(r"^\s*rpc\s+(\w+)\s*\(", re.MULTILINE)
_SERVICE_RE = re.compile(r"^service\s+(\w+)\s*\{", re.MULTILINE)
# RegisterFooServiceServer / RegisterFooServiceHTTPServer
_REGISTER_RE = re.compile(r"Register([A-Za-z0-9]+?)(HTTP)?Server\s*\(")
# Python: class Foo(bar_pb2_grpc.BazServicer):
_PY_CLASS_RE = re.compile(r"^class\s+(\w+)\s*\(([^)]*)\)\s*:", re.MULTILINE)
_PY_METHOD_RE = re.compile(r"^(\s+)(?:async\s+)?def\s+(\w+)\s*\(")


@dataclass
class ProtoService:
    name: str
    file: str
    rpcs: list[str]


@dataclass
class PyServicer:
    cls: str
    file: str
    base: str  # 继承的 *Servicer 名；空串 = 没继承生成基类
    methods: list[str]


@dataclass
class ServiceRow:
    name: str
    go_path: str
    servicers: list[str] = field(default_factory=list)
    conditional: set[str] = field(default_factory=set)
    py_dir: str | None = None
    py_servicers: list[PyServicer] = field(default_factory=list)
    py_files: list[str] = field(default_factory=list)
    py_loc: int = 0
    go_loc: int = 0


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="replace")


def scan_protos() -> dict[str, ProtoService]:
    """servicer 名 → 它声明的 rpc 列表。"""
    out: dict[str, ProtoService] = {}
    for f in sorted(PROTO_DIR.rglob("*.proto")):
        text = _read(f)
        bounds = [(m.group(1), m.start()) for m in _SERVICE_RE.finditer(text)]
        for i, (name, start) in enumerate(bounds):
            end = bounds[i + 1][1] if i + 1 < len(bounds) else len(text)
            rel = f.relative_to(ROOT).as_posix()
            if name in out:
                # 同名 service 出现在两个 proto 里 —— Register 映射会有歧义，必须知道
                print(
                    f"WARN: service {name} 在多个 proto 里定义：{out[name].file} / {rel}",
                    file=sys.stderr,
                )
            out[name] = ProtoService(name=name, file=rel, rpcs=_RPC_RE.findall(text[start:end]))
    return out


def scan_go_services() -> dict[str, ServiceRow]:
    """走 services/<domain>/<name>/internal/server/*.go，看它注册了哪些 servicer。"""
    rows: dict[str, ServiceRow] = {}
    for server_dir in sorted(SERVICES_DIR.glob("*/*/internal/server")):
        svc_root = server_dir.parents[1]
        row = ServiceRow(name=svc_root.name, go_path=svc_root.relative_to(ROOT).as_posix())
        for gof in sorted(server_dir.glob("*.go")):
            if gof.name.endswith("_test.go"):
                continue
            for line in _read(gof).splitlines():
                m = _REGISTER_RE.search(line)
                if m is None or m.group(2):  # ...HTTPServer 是同一 servicer 的 HTTP 绑定，不重复计
                    continue
                servicer = m.group(1)
                if servicer not in row.servicers:
                    row.servicers.append(servicer)
                # 缩进 > 1 层 ⇒ 写在 if/for 里 ⇒ 条件注册（由配置开关控制）
                if len(line) - len(line.lstrip("\t")) > 1:
                    row.conditional.add(servicer)
        row.go_loc = sum(
            len(_read(g).splitlines())
            for g in svc_root.rglob("*.go")
            if not g.name.endswith("_test.go")
        )
        rows[row.name] = row
    return rows


def scan_py_service(dirpath: Path) -> tuple[list[PyServicer], list[str], int]:
    """扫一个 Python 服务目录：继承 *Servicer 的类、文件清单、总行数。"""
    servicers: list[PyServicer] = []
    files: list[str] = []
    loc = 0
    for pyf in sorted(dirpath.rglob("*.py")):
        if "__pycache__" in pyf.parts:
            continue
        text = _read(pyf)
        files.append(pyf.relative_to(ROOT).as_posix())
        loc += len(text.splitlines())

        lines = text.splitlines()
        for m in _PY_CLASS_RE.finditer(text):
            cls, bases = m.group(1), m.group(2)
            base_hit = re.search(r"(\w+Servicer)\b", bases)
            if base_hit is None and "Servicer" not in cls:
                continue
            # 类体 = class 行往下，直到出现缩进为 0 的非空行
            start_line = text[: m.start()].count("\n")
            methods: list[str] = []
            for ln in lines[start_line + 1 :]:
                if ln.strip() and not ln[0].isspace():
                    break
                mm = _PY_METHOD_RE.match(ln)
                # 限缩进 ≤ 8：只收类体直属方法，不收嵌套函数
                if not mm or len(mm.group(1)) > 8 or mm.group(2).startswith("_"):
                    continue
                name = mm.group(2)
                # ★ 只收**看起来像 RPC** 的方法名（首字母大写）。
                #
                # gRPC 生成的 servicer 方法名恒为 PascalCase（proto 里怎么写就怎么生成），
                # 而 servicer 类上完全可以有正当的 Python 辅助方法 —— 依赖注入的
                # `set_ds_callback_guard()`、`set_cell_router()` 之类。第一版把它们
                # 一并当成"proto 里不存在的方法名"报了出来，是**误报**：
                # 它们本来就不该被 grpcio 分发。
                #
                # 判据取 PascalCase 而不是白名单：拼错的 RPC 名（`DoThinng`）仍然是
                # PascalCase，照样会被抓；而 snake_case 的辅助方法一个都不会误伤。
                if name[:1].isupper():
                    methods.append(name)
            servicers.append(
                PyServicer(
                    cls=cls,
                    file=pyf.relative_to(ROOT).as_posix(),
                    base=base_hit.group(1) if base_hit else "",
                    methods=methods,
                )
            )
    return servicers, files, loc


def build() -> tuple[dict[str, ProtoService], dict[str, ServiceRow]]:
    protos = scan_protos()
    rows = scan_go_services()
    for name, row in rows.items():
        pdir = PY_SERVICES_DIR / name
        if pdir.is_dir():
            row.py_dir = pdir.relative_to(ROOT).as_posix()
            row.py_servicers, row.py_files, row.py_loc = scan_py_service(pdir)
    return protos, rows


def analyse(protos: dict[str, ProtoService], row: ServiceRow) -> dict:
    """把三份事实对齐成可比较的结构。"""
    surface: list[dict] = []
    py_by_base = {s.base: s for s in row.py_servicers if s.base}
    for servicer in row.servicers:
        ps = protos.get(servicer)
        rpcs = ps.rpcs if ps else []
        py = py_by_base.get(servicer + "Servicer")
        impl = set(py.methods) if py else set()
        surface.append(
            {
                "servicer": servicer,
                "proto_file": ps.file if ps else None,
                "conditional": servicer in row.conditional,
                "rpcs": rpcs,
                "py_class": py.cls if py else None,
                "py_file": py.file if py else None,
                "py_implemented": sorted(impl & set(rpcs)),
                "py_missing": [r for r in rpcs if r not in impl],
                "py_extra": sorted(impl - set(rpcs)),  # ← 静默缺陷：proto 里没有的方法名
            }
        )
    return {
        "service": row.name,
        "go_path": row.go_path,
        "go_loc_nontest": row.go_loc,
        "py_dir": row.py_dir,
        "py_files": len(row.py_files),
        "py_loc": row.py_loc,
        "rpc_total": sum(len(s["rpcs"]) for s in surface),
        "rpc_done": sum(len(s["py_implemented"]) for s in surface),
        "surface": surface,
        "orphan_classes": [
            {"cls": s.cls, "file": s.file} for s in row.py_servicers if not s.base
        ],
        "has_main": any(f.endswith("/main.py") for f in row.py_files),
        "has_conf": any(f.endswith("/conf.py") for f in row.py_files),
    }


def stage(a: dict) -> str:
    if a["rpc_total"] and a["rpc_done"] == a["rpc_total"]:
        return "可跑" if (a["has_main"] and a["has_conf"]) else "RPC 齐但无入口"
    if a["rpc_done"] > 0:
        return "部分 RPC"
    return "仅核心不变量" if a["py_loc"] > 0 else "未开始"


def render_markdown(rows: list[dict]) -> str:
    out = [
        "# Go → Python 迁移覆盖矩阵（机械推导）",
        "",
        "> 由 `python/tools/parity/coverage.py` 生成。**不要手改** —— 改了下次重跑就没了。",
        "> `RPC` 的分母是该服务**实际注册的全部 servicer** 的 rpc 之和，不是同名 proto 一个文件；",
        "> 有几个服务挂了搭车 servicer（见表内），只看同名 proto 会把 RPC 面算少。",
        "",
        "| 服务 | Go 行 | Py 行 | RPC | 阶段 | main | conf | 注册的 servicer |",
        "|---|---:|---:|---:|---|:-:|:-:|---|",
    ]
    for a in sorted(rows, key=lambda r: (-(r["rpc_done"] / max(r["rpc_total"], 1)), -r["go_loc_nontest"])):
        svcs = ", ".join(s["servicer"] + ("*" if s["conditional"] else "") for s in a["surface"])
        out.append(
            f"| `{a['service']}` | {a['go_loc_nontest']} | {a['py_loc']} | "
            f"{a['rpc_done']}/{a['rpc_total']} | {stage(a)} | "
            f"{'✓' if a['has_main'] else '—'} | {'✓' if a['has_conf'] else '—'} | {svcs} |"
        )
    total_rpc = sum(a["rpc_total"] for a in rows)
    done_rpc = sum(a["rpc_done"] for a in rows)
    out += [
        "",
        "`*` = 条件注册（Go 侧写在 `if` 里，由配置开关控制）。",
        "",
        f"**合计：{done_rpc}/{total_rpc} 个 RPC 有 Python 实现"
        f"（{done_rpc * 100 // max(total_rpc, 1)}%）；"
        f"Go {sum(a['go_loc_nontest'] for a in rows)} 行 / "
        f"Py {sum(a['py_loc'] for a in rows)} 行。**",
        "",
        "## 逐服务未实现的 RPC",
        "",
    ]
    for a in sorted(rows, key=lambda r: r["service"]):
        missing = [(s["servicer"], s["py_missing"]) for s in a["surface"] if s["py_missing"]]
        if not missing:
            continue
        out.append(f"### `{a['service']}`（Go {a['go_loc_nontest']} 行）")
        out.append("")
        for servicer, ms in missing:
            out.append(f"- **{servicer}** — 缺 {len(ms)}：" + ", ".join(f"`{m}`" for m in ms))
        out.append("")
    return "\n".join(out)


def check(rows: list[dict]) -> int:
    """只拦"静默"缺陷。迁移未完成不算失败。"""
    problems: list[str] = []
    for a in rows:
        for s in a["surface"]:
            for extra in s["py_extra"]:
                problems.append(
                    f"{a['service']}: {s['py_file']} 的 {s['py_class']}.{extra}() "
                    f"在 {s['proto_file']} 的 {s['servicer']} 里不存在 —— "
                    f"grpcio 按 proto 名分发，这个方法**永远不会被调用**，"
                    f"该 RPC 实际返回 UNIMPLEMENTED"
                )
        for orphan in a["orphan_classes"]:
            problems.append(
                f"{a['service']}: {orphan['file']} 的 class {orphan['cls']} 名字像 servicer "
                f"却没继承生成的 *Servicer 基类 —— 注册不报错，但缺的方法没有 UNIMPLEMENTED 兜底"
            )
    if problems:
        print('FAIL —— 发现静默缺陷（不是"迁移未完成"，是"写了但不生效"）：\n', file=sys.stderr)
        for p in problems:
            print(f"  x {p}", file=sys.stderr)
        return 1
    print(
        f"OK —— {len(rows)} 个服务，无静默缺陷"
        f"（未实现的 RPC 是预期状态，跑 `coverage.py` 看清单）"
    )
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--json", action="store_true", help="输出 JSON")
    ap.add_argument("--check", action="store_true", help="CI 门禁：静默缺陷 → exit 1")
    ap.add_argument("--service", help="只看一个服务")
    ap.add_argument("-o", "--out", help="写入文件而不是 stdout")
    args = ap.parse_args()

    protos, go_rows = build()
    rows = [analyse(protos, r) for r in go_rows.values()]
    if args.service:
        rows = [r for r in rows if r["service"] == args.service]
        if not rows:
            print(f"没有这个服务：{args.service}", file=sys.stderr)
            return 2

    if args.check:
        return check(rows)

    text = json.dumps(rows, ensure_ascii=False, indent=2) if args.json else render_markdown(rows)
    if args.out:
        Path(args.out).write_text(text + "\n", encoding="utf-8", newline="\n")
        print(f"已写入 {args.out}")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
