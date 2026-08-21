"""跨栈配置默认值对账 —— 补齐 matchmaker / player / owner / data_service 四个漏网服务。

## 为什么单开这个文件

21 个服务里有 17 个在自己的测试文件里对着 Go 的 `internal/conf/conf.go` 断言过默认值,
只有这四个没有。**这不是"少几个用例"级别的问题**:两栈读的是同一份 yaml,默认值分叉的
表现形式是"两边都不报错地跑出不同行为" —— 比如 yaml 里没写 `team_size` 时 Go 按 5 组队、
Python 按 0 组队(need = side_count×0 = 0,撮合循环空转),没有任何日志会说这件事。

更值得记一笔的是 matchmaker:`pandorapy/services/matchmaker/conf.py` 顶上的注释白纸黑字写着

    抽成常量而不是内联字面量,是为了让 tests/test_matchmaker_conf.py 能直接对着 Go 源码断言

而 `tests/test_matchmaker_conf.py` **从来没有存在过**。注释描述了一个不存在的门禁,读代码的人
会以为这块已经被守住了。这正是本轮在修的那一类缺陷(把"说了"当成"做了")。

## 判据都取自 Go 源码

所有期望值都从 `conf.go` 现场解析,不把数字抄进测试 —— 抄一遍就多一个会漂移的真相。
**判据符号(`<= 0` / `== 0` / `< 0`)也要抓**:符号本身就是行为契约。
`mmr_floor` 是 `< 0`(0 是合法配置,dev 就写 0),抄成 `<= 0` 的话 yaml 里显式写的 0
会被兜成"默认值",而两边都不报错。

读 **Go** 源码用正则是可以的(异种语言,没有 AST 可用);读 Python 侧一律走对象属性,
不数文本。
"""

from __future__ import annotations

import ast
import datetime as _dt
import pathlib
import re
from typing import Any

import pytest

from pandorapy import config as pconfig
from pandorapy.services.data_service import conf as dconf
from pandorapy.services.matchmaker import conf as mconf
from pandorapy.services.owner import conf as oconf
from pandorapy.services.player import conf as pconf

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

# service key → (Go conf.go 路径, Go 结构体字段名, Python conf 模块, Python 段属性名)
_TARGETS: dict[str, tuple[pathlib.Path, str, Any, str]] = {
    "matchmaker": (
        REPO_ROOT / "services/matchmaking/matchmaker/internal/conf/conf.go",
        "Match",
        mconf,
        "match",
    ),
    "player": (
        REPO_ROOT / "services/account/player/internal/conf/conf.go",
        "Player",
        pconf,
        "player",
    ),
    "owner": (
        REPO_ROOT / "services/runtime/owner/internal/conf/conf.go",
        "Owner",
        oconf,
        "owner",
    ),
    "data_service": (
        REPO_ROOT / "services/data/data_service/internal/conf/conf.go",
        "Data",
        dconf,
        "data",
    ),
}


def _camel_to_snake(name: str) -> str:
    """Go 字段名 → Python 字段名,**认识连写的大写缩写**。

    朴素的 `(?<!^)(?=[A-Z])` 会把 `BaseMMR` 拆成 `base_m_m_r`、`MMRFloor` 拆成
    `m_m_r_floor`,于是这两个字段永远"在 Python 侧找不到",断言变成静默跳过 ——
    对账测试最怕的就是这种"看着绿其实没查"。
    """
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)  # BaseMMR   -> Base_MMR
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", s)  # MMRFloor  -> MMR_Floor
    return s.lower()


def _go_src(service: str) -> str:
    path = _TARGETS[service][0]
    assert path.exists(), f"Go conf.go 不存在:{path}(服务被挪了?)"
    return path.read_text(encoding="utf-8")


def _go_int_defaults(service: str) -> dict[str, tuple[str, int]]:
    """解析 `if c.X.F <op> 0 { c.X.F = N }` → {python 字段名: (判据符号, 默认值)}。"""
    src = _go_src(service)
    struct = _TARGETS[service][1]
    pattern = re.compile(
        rf"if c\.{struct}\.(?P<field>\w+) (?P<op><=|==|<) 0 \{{\s*"
        rf"c\.{struct}\.(?P=field) = (?P<value>\d+)\s*\}}",
        re.M,
    )
    return {
        _camel_to_snake(m.group("field")): (m.group("op"), int(m.group("value")))
        for m in pattern.finditer(src)
    }


_UNIT = {"time.Second": 1, "time.Minute": 60, "time.Hour": 3600, "time.Millisecond": 0.001}


def _go_duration_defaults(service: str) -> dict[str, _dt.timedelta]:
    """解析 `c.X.F = config.Duration(N * time.Second)` → {python 字段名: timedelta}。"""
    src = _go_src(service)
    struct = _TARGETS[service][1]
    pattern = re.compile(
        rf"c\.{struct}\.(?P<field>\w+) = config\.Duration\("
        rf"(?P<n>\d+) \* (?P<unit>time\.\w+)\)"
    )
    out: dict[str, _dt.timedelta] = {}
    for m in pattern.finditer(src):
        unit = _UNIT.get(m.group("unit"))
        assert unit is not None, f"没见过的时间单位 {m.group('unit')} —— 先扩 _UNIT"
        out[_camel_to_snake(m.group("field"))] = _dt.timedelta(
            seconds=int(m.group("n")) * unit
        )
    return out


def _go_string_defaults(service: str) -> dict[str, str]:
    """解析 `c.X.F = "literal"` → {python 字段名: 字面量}。"""
    src = _go_src(service)
    struct = _TARGETS[service][1]
    pattern = re.compile(rf'c\.{struct}\.(?P<field>\w+) = "(?P<value>[^"]*)"')
    return {
        _camel_to_snake(m.group("field")): m.group("value")
        for m in pattern.finditer(src)
    }


def _go_clamped_fields(service: str) -> set[str]:
    """在 `Defaults()` 里被**多次赋值**的字段(先填默认、再钳边界)。

    matchmaker 的 `TeamSize` 就是:`== 0 → 5`,紧接着 `< 1 → 1` / `> 50 → 50`。
    所以它的负值**不会**被原样保留 —— 不认这件事的话,下面那条判据符号
    测试会把一个**正确的** Python 实现判成错。
    """
    src = _go_src(service)
    struct = _TARGETS[service][1]
    counts: dict[str, int] = {}
    for m in re.finditer(rf"c\.{struct}\.(\w+) = ", src):
        counts[_camel_to_snake(m.group(1))] = counts.get(_camel_to_snake(m.group(1)), 0) + 1
    return {f for f, n in counts.items() if n > 1}


def _fresh(service: str):  # noqa: ANN201
    mod = _TARGETS[service][2]
    cfg = mod.Config()
    cfg.apply_defaults()
    return getattr(cfg, _TARGETS[service][3])


ALL = sorted(_TARGETS)


@pytest.mark.parametrize("service", ALL)
def test_go_defaults_are_parseable(service: str) -> None:
    """★ 非空金丝雀:解析规则失效时必须变红,而不是"零个字段全部通过"。

    没有这条的话,Go 侧把 `Defaults()` 改个写法(比如换成 helper 函数)会让下面所有
    对账用例静默退化成空循环 —— 测试全绿、门禁没了。
    """
    ints = _go_int_defaults(service)
    durs = _go_duration_defaults(service)
    strs = _go_string_defaults(service)
    assert ints or durs or strs, (
        f"{service}: 没能从 Go conf.go 解析出任何默认值 —— 解析规则该跟着 Go 改"
    )


@pytest.mark.parametrize("service", ALL)
def test_int_defaults_match_go(service: str) -> None:
    """整数默认值逐字段比对。分叉 = 同一份 yaml 两个实现跑出不同行为。"""
    sec = _fresh(service)
    for field, (_op, expected) in _go_int_defaults(service).items():
        assert hasattr(sec, field), f"{service}.{field} 在 Python 侧不存在(配了不生效)"
        assert getattr(sec, field) == expected, f"{service}.{field} 默认值与 Go 不一致"


@pytest.mark.parametrize("service", ALL)
def test_int_default_operator_matches_go(service: str) -> None:
    """★ 判据符号本身就是契约:`<= 0` 兜负值,`== 0` / `< 0` 的语义各不相同。

    - `<= 0`:负值和 0 都兜成默认(负值无意义)
    - `== 0` :只兜 0,负值**原样保留**(负值多半是"显式关闭"的约定)
    - `< 0`  :只兜负值,0 是合法配置(player.mmr_floor 就是,dev 环境写 0)

    只比默认值不比符号的话,把 `<` 抄成 `<=` 这种改动测试照样绿,而 yaml 里显式
    写的 `mmr_floor: 0` 会在 Python 上被悄悄改成别的值。

    ★ 被后续钳位覆盖的字段跳过"原样保留"这一半(见 `_go_clamped_fields`)。

    ★ 已知盲区(变异验证时实测):**默认值恰好是 0** 的字段(如 player.mmr_floor)
      查不出符号漂移 —— `< 0` 与 `<= 0` 对任何输入的结果都是 0,两者行为完全等价,
      没有可观测差异可断言。不是漏,是这一格本来就无从区分。
    """
    clamped = _go_clamped_fields(service)
    for field, (op, default) in _go_int_defaults(service).items():
        for probe in (-1, 0):
            sec_cls_cfg = _TARGETS[service][2].Config()
            sec = getattr(sec_cls_cfg, _TARGETS[service][3])
            if not hasattr(sec, field):
                pytest.fail(f"{service}.{field} 在 Python 侧不存在")
            setattr(sec, field, probe)
            sec_cls_cfg.apply_defaults()
            actual = getattr(getattr(sec_cls_cfg, _TARGETS[service][3]), field)
            hit = (op == "<=" and probe <= 0) or (op == "==" and probe == 0) or (
                op == "<" and probe < 0
            )
            if hit:
                assert actual == default, (
                    f"{service}.{field}: Go 判据是 `{op} 0`,{probe} 应被兜成 {default}"
                )
            elif field not in clamped:
                assert actual == probe, (
                    f"{service}.{field}: Go 判据是 `{op} 0`,{probe} 必须原样保留"
                )


@pytest.mark.parametrize("service", ALL)
def test_duration_defaults_match_go(service: str) -> None:
    """时长默认值比对**解析后的 timedelta**,不是字符串字面量。

    Go 写 `60 * time.Second`、Python 写 `"60s"` 或 `"1m"` —— 字面量不同但语义相同,
    比字符串会误报;而 `"60"`(缺单位)这种真错误只有解析后才看得出来。
    """
    sec = _fresh(service)
    for field, expected in _go_duration_defaults(service).items():
        assert hasattr(sec, field), f"{service}.{field} 在 Python 侧不存在(配了不生效)"
        raw = getattr(sec, field)
        assert isinstance(raw, str), f"{service}.{field} 应是时长字符串,实际 {type(raw)}"
        assert pconfig.parse_duration(raw) == expected, (
            f"{service}.{field}: Go 默认 {expected},Python 是 {raw!r}"
        )


@pytest.mark.parametrize("service", ALL)
def test_string_defaults_match_go(service: str) -> None:
    """字符串默认值比对(端口地址、game_mode、昵称前缀等)。"""
    sec = _fresh(service)
    for field, expected in _go_string_defaults(service).items():
        if not hasattr(sec, field):
            continue  # 条件性默认(如"配了 secret 才补 audience")不在本段建模
        actual = getattr(sec, field)
        if actual == "":
            continue  # 同上:Go 侧是带前置条件的赋值,空配置下本就不该被填
        assert actual == expected, f"{service}.{field} 默认值与 Go 不一致"


@pytest.mark.parametrize("service", ALL)
def test_ports_match_go(service: str) -> None:
    """端口是契约:Envoy cluster 与 run_services.ps1 的端口检查都钉死在这些值上。

    分叉的表现是"服务起来了但没人调得到" —— Envoy 依旧往老端口发,Python 副本
    监听在别处,健康检查还是绿的。
    """
    src = _go_src(service)
    sec_cfg = _TARGETS[service][2].Config()
    sec_cfg.apply_defaults()
    for kind, attr in (("Grpc", "grpc"), ("Http", "http")):
        m = re.search(rf'c\.Server\.{kind}\.Addr = "(:\d+)"', src)
        assert m, f"{service}: 没解析出 Go 的 {kind} 监听地址 —— 先修正则"
        assert getattr(sec_cfg.server, attr).addr == m.group(1), (
            f"{service} 的 {kind} 端口与 Go 不一致"
        )


# ── ★ 全覆盖闸:不准再漏下一个服务 ──────────────────────────────────────────


def _string_constants(path: pathlib.Path) -> set[str]:
    """取一个测试文件里所有**字符串字面量**(AST,不是原文)。

    走 AST 而不是数原文,是因为注释里出现 `conf.go` 三个字不该被当成"这里有门禁"。
    这正是本轮在修的那类缺陷:机械检查数文本,注释就能把它冒充过去。
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return {
        n.value
        for n in ast.walk(tree)
        if isinstance(n, ast.Constant) and isinstance(n.value, str)
    }


def test_every_service_has_a_go_conf_gate() -> None:
    """21 个 Go `internal/conf/conf.go`,每个都必须被某个测试文件对账过。

    历史缺口就是这么来的:17 个服务各自在自己的测试里顺手加了门禁,剩下 4 个
    (matchmaker / player / owner / data_service)谁也没管,而且**没有任何机制
    会报告这件事** —— 直到有人手工把两边列出来数一遍。这条就是那个机制。
    """
    go_confs = sorted((REPO_ROOT / "services").rglob("internal/conf/conf.go"))
    assert len(go_confs) == 21, f"服务数变了({len(go_confs)}),先确认是新增还是删除"

    tests_dir = pathlib.Path(__file__).resolve().parent
    per_file = {p: _string_constants(p) for p in tests_dir.rglob("test_*.py")}

    ungated: list[str] = []
    for go_conf in go_confs:
        service = go_conf.parents[2].name  # .../<service>/internal/conf/conf.go
        gated = False
        for consts in per_file.values():
            # 各测试文件写法不一:有的是一整条
            # "services/economy/auction/internal/conf/conf.go",有的是
            # `repo_root / "services" / "social" / "chat" / ... / "conf.go"` 拆成多段。
            # 两种都要认,否则会把已有门禁误判成"漏了"。
            if not any(c.replace("\\", "/").endswith("conf.go") for c in consts):
                continue
            if service in consts or any(f"/{service}/" in c for c in consts):
                gated = True
                break
        if not gated:
            ungated.append(service)

    assert not ungated, (
        f"这些服务没有任何 Go conf.go 默认值对账门禁:{ungated}。"
        f"两栈读同一份 yaml,默认值分叉不会报错,只会静默跑出不同行为"
    )
