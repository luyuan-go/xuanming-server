"""`config_load_failed` / `config_scan_failed` 的分界闸。

★ 这条检查存在的理由:同一个缺口在 chat / battle_result / mission 三个服务里
**同时**出现 —— 它是跟着 main.py 模板复制的。修三处不解决问题,第 20 个服务
照抄照错。

Go 的分界:

  | 阶段 | Go | 事件名 |
  |---|---|---|
  | 读文件 + 解析 yaml | `c.Load()` | `config_load_failed` |
  | 结构映射到模型 | `c.Scan(&cfg)` | `config_scan_failed` |

移植初版写成 `except FileNotFoundError → load_failed` + `except Exception → scan_failed`,
于是 **yaml 语法错 / 权限拒 / 编码坏 / 根节点非 mapping 全被报成 `config_scan_failed`**。

这不是洁癖:事件名是 Loki 告警和运维手册的入口。运维看到 `config_scan_failed` 会去
查"哪个字段填错了",而真实原因是文件根本没读成 —— 排查方向从第一步就错。
"""

from __future__ import annotations

import pathlib

import tempfile

import pytest

from pandorapy import config as pconfig

SERVICES_DIR = pathlib.Path(__file__).resolve().parents[1] / "pandorapy" / "services"


def _mains() -> list[pathlib.Path]:
    return sorted(
        p for p in SERVICES_DIR.glob("*/main.py") if "config_load_failed" in p.read_text(encoding="utf-8")
    )


def test_there_are_mains_to_check() -> None:
    """防止本文件因为路径写错变成恒绿的空检查。"""
    assert len(_mains()) >= 15, f"只扫到 {len(_mains())} 个 main,路径大概写错了"


# ── load_yaml 侧:四类读取失败都必须是 ConfigLoadError ──────────────────

@pytest.mark.parametrize(
    ("name", "body"),
    [
        ("语法错", "bad: [unclosed\n"),
        ("根节点是列表", "- a\n- b\n"),
        ("根节点是标量", "just-a-string\n"),
    ],
)
def test_parse_failures_are_config_load_error(name: str, body: str) -> None:
    d = pathlib.Path(tempfile.mkdtemp())
    f = d / "c.yaml"
    f.write_text(body, encoding="utf-8")
    with pytest.raises(pconfig.ConfigLoadError):
        pconfig.load_yaml(f)


def test_missing_file_is_config_load_error() -> None:
    with pytest.raises(pconfig.ConfigLoadError):
        pconfig.load_yaml(pathlib.Path(tempfile.mkdtemp()) / "nope.yaml")


def test_bad_encoding_is_config_load_error() -> None:
    """★ 非 UTF-8 字节 —— 初版这一支掉进 catch-all 被报成 scan_failed。"""
    d = pathlib.Path(tempfile.mkdtemp())
    f = d / "c.yaml"
    f.write_bytes(b"key: \xff\xfe not-utf8\n")
    with pytest.raises(pconfig.ConfigLoadError):
        pconfig.load_yaml(f)


def test_empty_file_is_not_an_error() -> None:
    """空文件是合法的(全走默认值),不能当失败 —— 方向别修反了。"""
    d = pathlib.Path(tempfile.mkdtemp())
    f = d / "c.yaml"
    f.write_text("", encoding="utf-8")
    assert pconfig.load_yaml(f) == {}


# ── main 侧:不许再用具体异常类型当 load 的判据 ─────────────────────────

@pytest.mark.parametrize("path", _mains(), ids=lambda p: p.parent.name)
def test_main_catches_config_load_error_not_concrete_types(path: pathlib.Path) -> None:
    """★ 打 `config_load_failed` 的那一支必须捕 `ConfigLoadError`。

    捕具体异常类型(`FileNotFoundError` / `yaml.YAMLError` / …)有两个问题:

      1. **漏** —— 少列一个就掉进 catch-all 报成 scan_failed(初版 15 个服务只列了
         `FileNotFoundError`);
      2. **脆** —— `load_yaml` 内部换实现时这些 main 会集体静默失配。三个原本
         列全了四元组的服务,就是被 `ConfigLoadError` 的包装打穿的。

    有类型的异常把"归哪一类"的判断收在**产生错误的那一侧**,而不是散在 19 个 main 里。

    ⚠️ 本检查的第一版用"往下看 6 行有没有 config_load_failed"定位分支,结果窗口
    **溢进了下一个 try 块** —— 把打 `abs_conf_path_failed` 的 `except OSError` 也
    报成违规(7 处误报)。改用 AST 只看 handler **自己的块体**:
    能拿到确定答案时就别猜文本。
    """
    import ast

    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.ExceptHandler):
            continue
        # 这个 handler **自己的块体**里打了 config_load_failed 吗?
        body_src = " ".join(ast.dump(n) for n in node.body)
        if "config_load_failed" not in body_src:
            continue
        caught = ast.dump(node.type) if node.type is not None else "<bare>"
        assert "ConfigLoadError" in caught, (
            f"{path.parent.name}/main.py:{node.lineno} 用 `{ast.unparse(node.type) if node.type else 'except:'}` "
            f"当 config_load_failed 的判据。改成 `except pconfig.ConfigLoadError as exc:` —— "
            f"具体异常类型会漏(yaml 语法错 / 权限拒 / 编码坏被报成 config_scan_failed)。"
        )
        return
    pytest.fail(f"{path.parent.name}/main.py 里没找到打 config_load_failed 的 except 分支")
