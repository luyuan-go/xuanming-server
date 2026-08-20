"""跨语言对拍的辅助件:把 Go 源码里的函数抠出来做**逐字核对**。

★ 为什么需要这个(2026-08-19 建)

    test_leaderboard_estimate / test_player_experience 号称"跨语言对拍",做法是把
    Go 算法**手抄**进一段 `package main`(零 import 项目自身的包)、用真 Go 编译器跑一遍、
    与 Python 逐条比对。这一层是真的有价值:它能抓 Python `//` 是 floor 而 Go `/` 是
    向零截断、uint64 回绕这类**跨语言语义**差异 —— docstring 声称的正是这个。

    但它抓不到**实现漂移**:手抄件和被测 Python 是同一次理解的产物,Go 侧真改了
    两边都不会红。实测:把 `board_store.go` 的 `q--` 改成 `q++`,
    `pytest tests/test_leaderboard_estimate.py` 仍然全绿 —— 而那个文件从头到尾
    **没被打开过**(用例的失败文案却写着"多半是 board_store.go 的直方图变了")。
    更糟的是 bucketOf 的负分 floor 语义在**全仓零覆盖**:同一变异下
    `go test ./services/runtime/leaderboard/internal/data/` 也全绿(该包没有负分样本)。

    所以修法是"保留现有跨语言执行 + 增加手抄件与真源的核对",不是把 `_GO_DUMP_PROGRAM`
    删掉换成 import 真包 —— **那条路走不通**:`bucketOf` 是 `internal/data` 包内未导出
    函数,且每个服务各有一份 `go.mod`(独立 module),外部 `package main` import 不了。
    退而求其次解析源码,先例是 `tests/test_protosql_type_parity.py:88`(它用正则把手抄的
    类型映射表与真源逐值核对)。

★ 局限(写清楚,免得下一个人以为它比实际更强)

    这是**文本级**核对,不是语义级:Go 侧把实现挪进被调用的另一个函数、或改了传进来的
    常量,这里都看不出来。它守的是"手抄件与真源逐字一致"这一条,够用是因为这两个函数
    都是自包含的纯函数。注释与空行会被剥掉(手抄件刻意没抄注释),所以改注释不会误红。
"""

from __future__ import annotations

import json
import pathlib
import re
import shutil
import subprocess
import tempfile

import pytest


def run_go_json(repo_root: pathlib.Path, source: str, payload: object) -> object:
    """在 pkg module 里跑一段临时 Go 程序:stdin 喂 JSON,stdout 收 JSON。

    ★ 为什么用 stdin 传用例而不是把用例写死进 Go 源码:
        用例写两遍(Python 一份、Go 一份)时,两边**各自**漂移不会被发现 ——
        Python 少测了一个 case,对拍照样全绿。让 Python 成为用例的唯一来源,
        Go 只当"参照实现",漏测就变成两边都漏,至少不会出现"以为对拍过了"的假象。

    ★ 三种结局必须分清(与 tests/test_kafkax_parity.py 同一条纪律):
        go 不在 PATH        → 返回 None,调用方 skip 并说明"不假装通过"
        go 在但编译/运行失败 → fail 并带出 stderr(说明**对拍对象变了**,正是这道门的用途)
        跑通                 → 返回解析后的 JSON
    """
    if shutil.which("go") is None:
        return None
    with tempfile.TemporaryDirectory() as tmp:
        main_go = pathlib.Path(tmp) / "main.go"
        main_go.write_text(source, encoding="utf-8")
        try:
            proc = subprocess.run(
                ["go", "run", str(main_go)],
                cwd=repo_root / "pkg",  # 在 pkg module 里跑才能 import 被测包
                input=json.dumps(payload, ensure_ascii=False),
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=300,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            pytest.fail(f"go 在 PATH 上却跑不起来:{exc}")
    if proc.returncode != 0:
        pytest.fail(
            "跨语言对拍程序编译/运行失败 —— 多半是被对拍的 Go 包 API 变了,"
            "**不是**环境问题。stderr:\n" + (proc.stderr or "(空)")[:4000]
        )
    return json.loads(proc.stdout)


def go_func(src: str, name: str) -> str:
    """抠出 `func <name>(...)` 的完整定义(签名 + 花括号配对的函数体)。

    找不到就抛 —— Go 侧改了函数名 / 挪了位置时必须**红**,不能静默返回空串再比一个
    空等于空。这正是原来那两个用例"最该响的时候不响"的病根。
    """
    m = re.search(rf"^func(?:\s+\([^)]*\))?\s+{re.escape(name)}\(", src, re.M)
    if m is None:
        raise LookupError(f"Go 源码里找不到 func {name} —— 被对拍的对象改名或挪走了")
    start = m.start()
    brace = src.index("{", m.end() - 1)
    depth = 0
    for i in range(brace, len(src)):
        if src[i] == "{":
            depth += 1
        elif src[i] == "}":
            depth -= 1
            if depth == 0:
                return src[start : i + 1]
    raise LookupError(f"func {name} 的花括号没配平")


def go_const(src: str, name: str) -> str:
    """抠出 `const <name> ... = <值>` 的右值(去掉注释与首尾空白)。"""
    m = re.search(rf"^const\s+{re.escape(name)}\b[^=]*=\s*(.+)$", src, re.M)
    if m is None:
        raise LookupError(f"Go 源码里找不到 const {name}")
    return _strip_line_comment(m.group(1)).strip()


def _strip_line_comment(line: str) -> str:
    """去掉行尾 `//` 注释。行里有引号时保守放过(本模块对拍的都是无字符串的纯函数)。"""
    if '"' in line or "`" in line:
        return line
    return line.split("//", 1)[0]


def normalize(go_src: str) -> str:
    """归一化:剥注释、剥空行、每行折叠空白 —— 只留下"代码本身"。

    手抄件刻意没抄注释,缩进也可能被编辑器动过;把这些噪声剥掉之后,
    任何**语句级**差异都会让比较失败。
    """
    out: list[str] = []
    for raw in go_src.split("\n"):
        line = _strip_line_comment(raw).strip()
        if not line:
            continue
        out.append(re.sub(r"\s+", " ", line))
    return "\n".join(out)
