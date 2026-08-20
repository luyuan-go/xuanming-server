"""依赖门控(MySQL / etcd)的机械契约 —— 防"跳过洗白"。

★ 这批检查在防什么

    真库 / 真 etcd 用例都靠一段"连不上就 pytest.skip"的夹具门控。这类门控有两个
    在本仓真实发生过的失效形状,共同点是**退出码 0、摘要里只多一个数字**:

      ① 把**我们自己代码的 bug** 洗成"环境不可用"。
         真事:`parse_go_dsn` 加了 `net` 字段 → splat 抛 TypeError → 被
         `except Exception -> pytest.skip("MySQL 不可用")` 吞掉 → 32 条真库用例
         悄悄停跑而套件报绿。异常 repr 只有 `('db')` 三个字符,看着像连接噪声。
         当时的修法只白名单了 TypeError 一种,换个异常类型照样洗白。
      ② 门控**默认值分叉**。10 个测试文件各写一份 `os.getenv(..., <默认>)`,
         其中 9 个是 13306、1 个是 3307 → 本机起哪个容器就有另一半用例静默跳过,
         而且 conftest 的会话清理打错端口,孤儿库永远删不掉。

    这两条都不是"写得不优雅",是**覆盖整片消失而没有任何异常形状**。所以判据放在
    这里按目录扫,新加的夹具自动纳入 —— 靠 review 逐个盯不住(①已经复制了 10 份)。

★ Go 那边为什么不需要这些

    `services/**/*_mysql_test.go` 唯一的 Skip 条件是 `PANDORA_TEST_MYSQL_DSN` 没设;
    DSN 解析失败 / DSN 带库名 / Ping 不可达 / 建库失败一律 `t.Fatalf`
    (注释原文:"已设测试 DSN 但 MySQL 不可达(不允许静默 PASS)"),门控路径里
    **零硬编码端口**。Go 侧压根没有"把异常转成跳过"这个动作,也就没有洗白的空间。
    Python 保留"环境没起来就跳过"是为了本机没起容器时还能跑纯逻辑用例,
    代价就是必须有这一组机械检查兜着。
"""

from __future__ import annotations

import pathlib
import re

import pytest

import etcdfixture as efixture
import mysqlfixture as mf

TESTS_DIR = pathlib.Path(__file__).resolve().parent

_CFG = {"host": "127.0.0.1", "port": 13306, "user": "root", "password": "x", "db": "d"}


# ── MySQL:只有环境类异常才允许 skip ──────────────────────────────────────


@pytest.mark.parametrize(
    "exc",
    [
        KeyError("db"),          # cfg 少了一个键(那次事故的近亲)
        TypeError("unexpected keyword argument 'net'"),  # 那次事故的**原形状**
        AttributeError("'dict' object has no attribute 'charset_clause'"),
        ValueError("库名不合法"),  # ensure_database 的白名单就抛这个
        RuntimeError("随便什么代码 bug"),
    ],
    ids=lambda e: type(e).__name__,
)
def test_code_class_exceptions_are_not_laundered_into_skip(exc: Exception) -> None:
    """★ 夹具自己的代码抛的异常必须**原样冒红**,不许变成"MySQL 不可用"。

    只白名单 TypeError 是不够的:`ensure_database` 是 10 个夹具的共享件,
    它里面任何一种代码错误都会被同一个 `except Exception` 洗掉,
    而洗掉的是**全部**真库覆盖,不止一个文件。

    ⚠️ 这里**不能**写成 `with pytest.raises(type(exc)), guard(...)` ——
    守卫真的洗白时抛的是 `Skipped`,`pytest.raises` 拦不住它,本用例会变成
    **skipped 而不是 failed**(实测过:变异后 5 skipped 而非 5 failed)。
    一个"检测跳过洗白"的用例自己被洗成跳过,正是它要防的那件事。
    """
    try:
        with mf.skip_only_if_mysql_is_down(_CFG, "契约自检"):
            raise exc
    except pytest.skip.Exception as skipped:
        pytest.fail(f"{type(exc).__name__} 被洗成了跳过 —— 代码 bug 伪装成环境缺失:{skipped}")
    except type(exc):
        return
    pytest.fail(f"{type(exc).__name__} 被守卫吞掉了(既没跳过也没抛出)")


def test_environment_class_exceptions_still_skip() -> None:
    """环境真没起来时仍然跳过 —— 本机不起容器也要能跑纯逻辑用例。

    放行集按**实测**定:asyncmy 把"端口没人听"和"口令错"统统包成 OperationalError,
    直觉里的 ConnectionRefusedError / socket.timeout 根本不会冒到调用方。
    """
    import asyncmy.errors as ae

    for exc in (ae.OperationalError(2003, "Can't connect"), TimeoutError(), OSError()):
        with pytest.raises(pytest.skip.Exception), mf.skip_only_if_mysql_is_down(_CFG, "契约自检"):
            raise exc


def test_skip_reason_keeps_the_word_mysql_for_the_ci_gate() -> None:
    """★ 跳过文案必须含 "MySQL" —— 这是与 CI 的**隐式契约**,不是措辞偏好。

    tools/scripts/ci_backend.ps1:311 是 `$pySkips | Where-Object { $_ -match 'MySQL|TiDB' }`,
    命中才在 `-RequireDbTests` 下升级成门禁失败。把文案写成"数据库起不来"
    就整条溜过 CI,而写文案的人不会知道有这么个契约 —— 所以在这里钉住。
    同时文案里必须带上真异常,否则跳过原因等于没说。
    """
    import asyncmy.errors as ae

    with pytest.raises(pytest.skip.Exception) as caught:  # noqa: PT012
        with mf.skip_only_if_mysql_is_down(_CFG, "契约自检"):
            raise ae.OperationalError(2003, "Can't connect to MySQL server")
    reason = str(caught.value)
    assert "MySQL" in reason, f"文案丢了 MySQL 三个字母,CI 的 skip 审计会漏掉它:{reason}"
    assert "2003" in reason, f"文案没带真异常,读日志的人无从判断是不是自己的 bug:{reason}"


# ── MySQL:DSN 只能有一个来源 ────────────────────────────────────────────


def test_no_test_file_hardcodes_its_own_mysql_dsn_default() -> None:
    """★ 只有 mysqlfixture 能读 PANDORA_TEST_MYSQL_DSN。

    各文件各写一份默认值 → 两个端口打架 → 本机起哪个容器就有另一半用例静默跳过,
    而且 conftest 的会话清理打错端口时孤儿库永远删不掉(实测攒了 40+ 个)。
    Go 侧门控路径零硬编码端口,结构上不会出这个问题。
    """
    offenders = [
        f.name
        for f in sorted(TESTS_DIR.glob("*.py"))
        if f.name not in {"mysqlfixture.py", "test_dependency_gate_contract.py"}
        and re.search(r'getenv\(\s*"PANDORA_TEST_MYSQL_DSN"', f.read_text(encoding="utf-8"))
    ]
    assert not offenders, (
        f"{offenders} 自己读了 PANDORA_TEST_MYSQL_DSN —— 改成 "
        f"`from mysqlfixture import MYSQL_DSN as DSN`。两份默认值必然漂移,"
        f"而漂移的表现是一半用例静默跳过 + 会话库删不掉。"
    )


def test_session_cleanup_shares_the_same_dsn_as_the_fixtures() -> None:
    """★ conftest 的会话清理必须与夹具用同一个 DSN,否则建的库删不掉。"""
    src = (TESTS_DIR / "conftest.py").read_text(encoding="utf-8")
    assert "mf.MYSQL_DSN" in src, "conftest 的 _drop_session_database 没走共享 DSN"
    assert "PANDORA_TEST_MYSQL_DSN" not in src, "conftest 又自己读了一遍环境变量"


# ── etcd:同一套判据 ─────────────────────────────────────────────────────


def test_malformed_etcd_endpoint_is_an_error_not_unavailable() -> None:
    """★ 端点漏写端口是**配置写错**,必须冒红。

    这是零 mock 的真实触发路径:`PANDORA_TEST_ETCD_ENDPOINTS=127.0.0.1`(漏端口)
    让原来的 `int(port)` 抛 ValueError,而探针的 `except Exception: return False`
    把它折叠成"etcd 不可用",44 条真 etcd 用例跳过、退出码 0,
    文案还让你去起一个已经在跑的容器。
    """
    with pytest.raises(ValueError, match="host:port"):
        efixture.host_port("127.0.0.1")


def test_etcd_endpoint_parses_normally() -> None:
    assert efixture.host_port("127.0.0.1:12379") == ("127.0.0.1", 12379)


async def test_etcd_probe_reraises_code_class_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """★ 驱动 API 变化 / 探针自己写错 → 冒红,不是"etcd 不可用"。

    同上:不用 `pytest.raises`,否则守卫洗白时本用例会变成 skipped 而不是 failed。
    """
    import aetcd

    class _Broken:
        def __init__(self, *_a: object, **_kw: object) -> None:
            raise TypeError("Client() got an unexpected keyword argument 'timeout'")

    monkeypatch.setattr(aetcd, "Client", _Broken)
    try:
        await efixture.require_etcd("契约自检")
    except pytest.skip.Exception as skipped:
        pytest.fail(f"驱动 API 变化被洗成了 etcd 不可用:{skipped}")
    except TypeError:
        return
    pytest.fail("探针把 TypeError 吞掉了")


async def test_etcd_probe_skips_only_on_connection_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """连不上仍然跳过,但**文案必须带上真异常** —— 否则线索指向错误的方向。

    对照:tests/test_login_main.py 的 MySQL 探针一直是 `f"...:{exc}"`,
    所以"三个 etcd 探针不带异常"不是项目惯例,是对本套自有惯例的偏离。
    """
    import aetcd
    import aetcd.exceptions as ax

    class _Down:
        def __init__(self, *_a: object, **_kw: object) -> None:
            raise ax.ConnectionTimeoutError

    monkeypatch.setattr(aetcd, "Client", _Down)
    with pytest.raises(pytest.skip.Exception) as caught:
        await efixture.require_etcd("契约自检")
    assert "ConnectionTimeoutError" in str(caught.value)


def test_etcd_allowlist_excludes_request_level_errors() -> None:
    """★ 放行集不能收 aetcd 的基类 ClientError —— 那会把"请求发错了"也洗成跳过。"""
    import aetcd.exceptions as ax

    allowed = efixture._env_failure_types()  # noqa: SLF001
    assert ax.ConnectionTimeoutError in allowed
    assert ax.ClientError not in allowed, "收了基类等于把 InvalidArgumentError 一起放行"
    assert not issubclass(ax.InvalidArgumentError, tuple(allowed))


def test_no_test_file_hardcodes_its_own_etcd_endpoint_default() -> None:
    """★ 只有 etcdfixture 能读 PANDORA_TEST_ETCD_ENDPOINTS(同 DSN 的理由)。"""
    offenders = [
        f.name
        for f in sorted(TESTS_DIR.glob("*.py"))
        if f.name not in {"etcdfixture.py", "test_dependency_gate_contract.py"}
        and re.search(r'getenv\(\s*"PANDORA_TEST_ETCD_ENDPOINTS"', f.read_text(encoding="utf-8"))
    ]
    assert not offenders, f"{offenders} 自己读了 PANDORA_TEST_ETCD_ENDPOINTS,改用 etcdfixture.ENDPOINT"


def test_no_probe_folds_exceptions_into_a_bool() -> None:
    """★ 不许再出现 `except Exception: return False` 这种"把异常折叠成布尔"的探针。

    折叠之后调用方只剩"可用/不可用"两个值,真因永久丢失;而后面那条
    pytest.skip 文案会**主动指控环境不可用**,把排查方向整个带偏。

    用 AST 而不是正则:etcdfixture 的模块头**原样引用了这个坏形状**当反面教材,
    正则会把文档本身报成违规(第一版就是这么误报的),而误报的检查最后会被删掉。
    """
    import ast

    def _folds(src: str) -> bool:
        for node in ast.walk(ast.parse(src)):
            if not isinstance(node, ast.ExceptHandler):
                continue
            if not (isinstance(node.type, ast.Name) and node.type.id == "Exception"):
                continue
            body = node.body
            if (
                len(body) == 1
                and isinstance(body[0], ast.Return)
                and isinstance(body[0].value, ast.Constant)
                and body[0].value.value is False
            ):
                return True
        return False

    offenders = [
        f.name
        for f in sorted(TESTS_DIR.glob("*.py"))
        if f.name != "test_dependency_gate_contract.py" and _folds(f.read_text(encoding="utf-8"))
    ]
    assert not offenders, (
        f"{offenders} 里还有把异常折叠成 False 的探针 —— 只放行连接类异常,"
        f"其余原样抛;跳过文案必须带 {{exc}}。"
    )
