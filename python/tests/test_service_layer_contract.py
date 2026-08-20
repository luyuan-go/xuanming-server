"""所有 service 层共同的机械契约 —— 扫全部 `services/*/service.py`。

这类检查存在的理由：**同一个缺口会随模板复制**。owner 是第一个写完的 service 层，
leaderboard 照着它抄，于是 `except BaseException` 吞掉 `CancelledError` 这一条
一次变成了两次。剩下 15 个服务还要照同一份模板写，靠 review 逐个盯不住。

所以判据放在这里、按目录扫，新服务加进来自动纳入。
"""

from __future__ import annotations

import pathlib
import re

import pytest

SERVICES_DIR = pathlib.Path(__file__).resolve().parents[1] / "pandorapy" / "services"


def _service_files() -> list[pathlib.Path]:
    """扫服务目录下**全部** .py，不只是 service.py。

    原先只扫 `*/service.py`，于是 `main.py` / `repo.py` / `biz.py` 里的宽 except
    完全不在覆盖内 —— 复核在 player_locator 的 main.py 里就找到 3 处未被覆盖的
    `except BaseException`。停机时 main 的装配路径吞掉取消，同样会让排空失效。
    """
    return sorted(
        f for f in SERVICES_DIR.glob("*/*.py")
        if f.name != "__init__.py" and "__pycache__" not in f.parts
    )


def test_there_are_service_files_to_check() -> None:
    """防止本文件因为路径写错而变成一个恒绿的空检查。"""
    assert _service_files(), f"没扫到任何服务源文件（{SERVICES_DIR}）"


@pytest.mark.parametrize("path", _service_files(), ids=lambda p: f"{p.parent.name}/{p.name}")
def test_cancelled_error_is_re_raised_before_any_broad_except(path: pathlib.Path) -> None:
    """★ `except BaseException` 之前必须先放行 `asyncio.CancelledError`。

    `CancelledError` 在 3.8+ 继承自 `BaseException`，会被宽 except 一起吞掉。
    而 grpc.aio **正是用取消**来终止在途 handler，所以吞掉的后果是：

      - 优雅停机时把取消映射成 in-band 业务错误码并返回一个**正常响应**
        → 客户端在每次滚动更新时收到一批莫名其妙的业务失败；
      - 取消没有穿透 → 任务不会真的停 → §9.16 的「排空在途」并没有按设计发生。

    与 §5.2.1 ⑰（panic 兜底把 CancelledError 也算 panic）同一个坑，
    只是位置从拦截器层挪到了 service 层。

    注：`except Exception` 不在此列 —— 它本来就抓不到 `CancelledError`，是安全的。

    ⚠️ **不算违规的两种写法**（第一版检查在它们上面误报了 13 处）：

      1. 前面已经有一条 `except asyncio.CancelledError: raise`；
      2. 这条宽 except **自己无条件 re-raise**（事务回滚的标准写法：
         `except BaseException: rollback(); raise`）—— 取消照样穿透出去，
         回滚本身是必须做的清理，不做才会留下悬挂事务。

    误报的检查会被 noqa 掉或整条删掉，最后什么都不剩，所以这两种必须放行。
    """
    lines = path.read_text(encoding="utf-8").split("\n")
    offenders: list[int] = []
    for i, ln in enumerate(lines):
        m = re.match(r"^(\s*)except BaseException", ln)
        if not m:
            continue
        # ① 前面已放行 CancelledError？
        guarded = False
        for prev in reversed(lines[max(0, i - 12) : i]):
            if "CancelledError" in prev:
                guarded = True
                break
            if re.match(r"^\s*(try:|async def |def )", prev):
                break
        if guarded:
            continue
        # ② 这条 except 自己无条件 re-raise？扫它的块体（缩进更深的连续行）
        indent = len(m.group(1))
        reraises = False
        for nxt in lines[i + 1 :]:
            if not nxt.strip():
                continue
            cur_indent = len(nxt) - len(nxt.lstrip())
            if cur_indent <= indent:
                break  # 块体结束
            if re.match(r"^\s*raise\s*$", nxt) and cur_indent == indent + 4:
                # 只认块体**顶层**的裸 raise：嵌在 if 里的是条件 re-raise，不算
                reraises = True
        if not reraises:
            offenders.append(i + 1)
    assert not offenders, (
        f"{path.parent.name}/{path.name} 第 {offenders} 行的 `except BaseException` "
        f"没有先放行 CancelledError —— 优雅停机时会把取消吞成业务错误码。"
        f"在它前面加：\n"
        f"    except asyncio.CancelledError:\n"
        f"        raise"
    )


# ── 刻意**没有**加的一条：R5「player_id 必须取自鉴权上下文」的机械检查 ──────
#
# 试过，用正则做不可靠，已放弃。记在这里是为了下一个人别再试一遍：
#
#   - `player_id=request.player_id` 既可能是「拿请求体当身份」（危险），
#     也可能是**日志字段**或响应字段回填（完全正常）。owner/service.py:131
#     就是后者，第一版检查在它上面误报。
#   - 「有没有用 extract_player_id」也区分不了：客户端面服务用它**取**身份，
#     而 owner 用它**拒**带玩家 JWT 的调用（内网接口不该被玩家直敲）。
#     同一个符号，两种相反的用途。
#
# 误报的检查比没有检查更糟 —— 它会被 noqa 掉或整条删掉，最后什么都不剩。
# 这条不变量目前只能靠 review 与逐服务的针对性用例（各 service 测试里都有）守。


# ── 后台协程必须有可辨认的点位名 ────────────────────────────────────────


def _has_background_kwarg(tree) -> bool:  # noqa: ANN001
    """本文件里到底有没有向 `server.run` 传过 background(不管什么形状)。

    跳过的**唯一**合法理由是"这个服务真的没有后台协程"(trade 就是),
    而不是"我的形状匹配没命中"—— 后者会把 10 个服务里的违规静默洗掉。
    """
    import ast

    return any(
        kw.arg == "background"
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for kw in node.keywords
    )


def _background_variable_names(tree) -> set[str]:  # noqa: ANN001
    """哪些局部变量最终会被当成 background 传出去。

    三条来源,缺一个就会漏掉一批服务:
      ① 字面量约定:本仓 19 个 main.py 里这个列表一律叫 `background`;
      ② `run(background=<Name>)` 里那个 Name(万一有人改名);
      ③ `<tracked> = _build_background(...)` —— auction 把整个列表的组装搬进了
         另一个函数,裸 lambda 全在那个函数里,而 `background=` 关键字在调用方。
         只看关键字所在的那个函数就一处都看不见。
    """
    import ast

    names = {"background"}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for kw in node.keywords:
            if kw.arg == "background" and isinstance(kw.value, ast.Name):
                names.add(kw.value.id)

    # ③ 顺着 `<tracked> = helper(...)` 把 helper 里 return 出来的那个局部名也纳入。
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)):
            continue
        targets = {t.id for t in node.targets if isinstance(t, ast.Name)}
        if not targets & names:
            continue
        func = node.value.func
        fname = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
        if not fname:
            continue
        for fn in ast.walk(tree):
            if not isinstance(fn, ast.FunctionDef | ast.AsyncFunctionDef) or fn.name != fname:
                continue
            for ret in ast.walk(fn):
                if isinstance(ret, ast.Return) and isinstance(ret.value, ast.Name):
                    names.add(ret.value.id)
    return names


def _bare_lambda_lines(tree, names: set[str]) -> list[int]:  # noqa: ANN001
    """收集所有会进 background 的**裸** lambda 的行号。

    `("点位名", lambda: ...)` 这种二元组不算违规 —— 元素是 ast.Tuple 不是 ast.Lambda,
    天然被放行,这正是我们想要的写法。
    """
    import ast

    bad: list[int] = []

    def _scan_elts(seq) -> None:  # noqa: ANN001
        for elt in getattr(seq, "elts", []):
            if isinstance(elt, ast.Lambda):
                bad.append(elt.lineno)

    for node in ast.walk(tree):
        # ① 内联:run(background=[...])
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "background" and isinstance(kw.value, ast.List | ast.Tuple):
                    _scan_elts(kw.value)
        # ② 赋值:background = [...] / background: list = [...]
        if isinstance(node, ast.Assign):
            targets = {t.id for t in node.targets if isinstance(t, ast.Name)}
            if targets & names and isinstance(node.value, ast.List | ast.Tuple):
                _scan_elts(node.value)
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id in names
            and isinstance(node.value, ast.List | ast.Tuple)
        ):
            _scan_elts(node.value)
        # ③ background.append(...) / background.extend([...] | 生成式)
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id in names
        ):
            if node.func.attr == "append":
                for arg in node.args:
                    if isinstance(arg, ast.Lambda):
                        bad.append(arg.lineno)
            elif node.func.attr == "extend":
                for arg in node.args:
                    if isinstance(arg, ast.List | ast.Tuple | ast.Set):
                        _scan_elts(arg)
                    elif isinstance(arg, ast.GeneratorExp | ast.ListComp) and isinstance(
                        arg.elt, ast.Lambda
                    ):
                        bad.append(arg.elt.lineno)
    return sorted(set(bad))

@pytest.mark.parametrize(
    "path", [p for p in _service_files() if p.name == "main.py"],
    ids=lambda p: p.parent.name,
)
def test_background_factories_are_named(path: pathlib.Path) -> None:
    r"""★ `server.run(background=[...])` 里不许出现裸 lambda。

    点位名会进 `pandora_safego_panic_recovered_total{name}` 与 `panic_recovered` 日志。
    传裸 lambda 的话 `__name__` 恒为 `<lambda>` —— **全服所有后台协程共用同一个 label**，
    告警只能告诉你"有条后台循环死了"，不能告诉你**是哪一条**。
    而后台循环恰恰是 safego 那条缺陷里最难发现的一类（进程照跑、health 照答 SERVING、
    日志零行）。

    合法写法：`background=[("mail_sweep", lambda: run_sweep(...))]`
    或直接传具名函数 `background=[_run_capacity_guard]`。

    ⚠️ 这条检查的第一版用正则数"具名二元组"，判据是 `\(\s*["\']`（左括号+引号）——
    而 `safego.loop("mail_sweep"` 恰好也匹配，于是每个裸 lambda 都被它自己内部的
    调用抵消掉，检查**恒绿**。改用 AST 看节点类型：能拿到确定答案时就别猜文本。

    ⚠️ 第二版只认**内联字面量** `run(background=[...])`(判据 `isinstance(kw.value, ast.List)`),
    于是 19 个服务里 10 个写成 `background = [...]` + `background=background`
    (`kw.value` 是 `ast.Name`)的全部落进 `pytest.skip("该服务没有 background=[...]")` ——
    **跳过理由是假的**:它们都有 background 列表,而 auction / mission / player 里合计
    9 处裸 lambda 正是本检查要抓的东西,一次都没被跑到过。`-rs` 里只显示一行
    `SKIPPED [10]`,理由读起来完全合理,没有任何"出事了"的形状。

    这是本文件开头那句"同一个缺口会随模板复制"的镜像版:`background = [...]` 加条件
    `.append(...)` 是**多数派**写法,也是所有"按配置决定要不要多起一条循环"的服务
    (team / push / player_locator / matchmaker / login / battle_result)**唯一**能用的写法。
    所以判据必须跟着**变量**走,不能只认字面量;跳过也必须以"本文件真的一个
    `background=` 都没有"为条件,而不是以"没匹配上我的形状"为条件。

    Go 侧为什么不需要这条:`pkg/safego` 的 `Go(ctx, name, fn)` / `Loop(ctx, name, interval, fn)`
    把 `name string` 写成**必填形参** —— 匿名点位在类型层就不可能存在(40+ 个调用点
    全传字面量名)。Python 允许直接传裸 callable,这条约束才降级成 `server.py:345-356`
    的启动期 WARN + 本机械检查。本检查是**唯一**的 CI 闸,它空转就等于这条约束不存在。
    """
    import ast

    tree = ast.parse(path.read_text(encoding="utf-8"))
    # 没有 background= 的服务(trade:过期惰性判定、死名额按需清理,真的没有后台循环)
    # 这条不变量平凡成立,直接过 —— **不跳过**:跳过与"我的形状匹配没命中"长得一模一样,
    # 而后者正是上一版的病。"整条检查是不是还认得出 background" 由下面那条金丝雀守。
    bare = _bare_lambda_lines(tree, _background_variable_names(tree))
    assert not bare, (
        f"{path.parent.name}/main.py 第 {bare} 行的 background 元素是**裸 lambda** —— "
        f"panic 计数与日志里会记成 bg_anonymous，出事时分不清是哪条循环。"
        f'改成 ("点位名", lambda: ...) 或直接传具名函数。'
    )


def test_the_background_check_still_recognizes_real_services() -> None:
    """★ 金丝雀:防止上面那条检查**整体**空转。

    上一版的失效方式是"形状没匹配上 → 跳过",表现成一行读起来完全合理的
    `SKIPPED [10]`。改成"匹配不上就平凡通过"之后,同样的失效会更安静 ——
    关键字改名 / AST 形状变了,19 个服务会**全部**打绿。

    所以在这里钉一个下限:大多数 main.py 必须仍然被认出"传了 background"。
    下限是防空转用的,不是精确计数:少一两个服务不该误红,而一旦归零必然是
    判据本身坏了。
    """
    import ast

    mains = [p for p in _service_files() if p.name == "main.py"]
    recognized = sorted(
        p.parent.name
        for p in mains
        if _has_background_kwarg(ast.parse(p.read_text(encoding="utf-8")))
    )
    assert len(recognized) >= 15, (
        f"只认出 {len(recognized)}/{len(mains)} 个服务传了 background({recognized})—— "
        f"判据自己坏了,上面那条按服务的检查此刻对每个服务都平凡通过。"
    )
    # 两种写法各钉一个样本:内联字面量 与 局部变量 + 条件 append。
    # 只覆盖一种时另一种就是盲区 —— 这正是上一版漏掉 9 处裸 lambda 的原因。
    for service in ("dialogue", "auction", "player"):
        assert service in recognized, f"{service} 的 background 没被认出来"


# ── kafka ProducerConf 只许有一个映射点 ────────────────────────────────

@pytest.mark.parametrize(
    "path", [p for p in _service_files() if p.name == "main.py"],
    ids=lambda p: p.parent.name,
)
def test_producer_conf_is_not_hand_copied(path: pathlib.Path) -> None:
    """★ 不许在 main.py 里手写 `kafkax.ProducerConf(...)`,一律走 `producer_conf_from()`。

    这条不是风格洁癖。收敛之前 **11 个服务 12 处手抄,没有一处是完整的**:

        auction / battle_result / friend / leaderboard / mission /
        player_locator / push / team   → 漏 retry_backoff + read_timeout + write_timeout
        chat / guild / player           → 漏 read_timeout + write_timeout

    漏掉的后果全是同一种:**yaml 里配了,程序不用,且没有任何提示**。运维把
    `retry_backoff: 500ms` 写进配置、重启、观察——行为一点没变,因为那个值从
    conf 读出来之后就没人往下传。这正是本次迁移要抓的"写错了不报错"。

    手抄 N 遍的必然结果就是 N 份各漏各的。判据放这里,新服务加进来自动纳入;
    Go 那边再加字段,只改 `kafkax.producer_conf_from` 一个地方。
    """
    import ast

    tree = ast.parse(path.read_text(encoding="utf-8"))
    hits = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "ProducerConf"
    ]
    assert not hits, (
        f"{path.parent.name}/main.py 第 {hits} 行手写了 ProducerConf。"
        f"改成 `kafkax.producer_conf_from(cfg.kafka)` —— 手抄一定会漏字段,"
        f'而漏掉的字段全是「配了不生效且不报错」。'
    )
