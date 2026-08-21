"""运行期第三方依赖必须同时进入项目声明和 CI 锁文件。"""

from __future__ import annotations

import pathlib
import re
import tomllib


PYTHON_ROOT = pathlib.Path(__file__).resolve().parents[1]


def _requirement_name(requirement: str) -> str:
    """抽出 PEP 508 依赖名；本契约只需处理仓库当前的普通依赖写法。"""
    match = re.match(r"\s*([A-Za-z0-9_.-]+)", requirement)
    assert match is not None, f"无法解析依赖声明:{requirement!r}"
    return match.group(1).lower().replace("_", "-")


def test_agones_http_client_dependency_is_declared_and_locked() -> None:
    """Agones 两个实现顶层 import httpx，干净环境必须能直接收集测试。"""
    project = tomllib.loads((PYTHON_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    declared = {_requirement_name(item) for item in project["project"]["dependencies"]}
    assert "httpx" in declared, (
        "ds_allocator/hub_allocator 顶层 import httpx，但 pyproject.toml 未声明；"
        "复用旧 .venv 会假绿，CI 的 uv pip sync 干净环境会 collection error"
    )

    lock = (PYTHON_ROOT / "requirements.lock").read_text(encoding="utf-8")
    assert re.search(r"(?m)^httpx==", lock), (
        "pyproject.toml 已声明 httpx，但 requirements.lock 未重生成；"
        "请按锁文件头注释运行 uv pip compile"
    )
