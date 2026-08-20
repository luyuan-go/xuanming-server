"""pytest 共享夹具。

sys.path 两条根:
  - python/          → pandorapy 业务代码
  - python/gen/      → proto 生成代码(第一级包名固定是 pandora)
两者必须分开,原因见 proto/buf.gen.python.yaml 的头注释。
"""

from __future__ import annotations

import pathlib
import sys

PY_ROOT = pathlib.Path(__file__).resolve().parents[1]
REPO_ROOT = PY_ROOT.parent

for path in (PY_ROOT, PY_ROOT / "gen"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from pandorapy import _utf8  # noqa: E402,F401  —— Windows 下测试输出含中文

import pytest  # noqa: E402


@pytest.fixture(scope="session")
def repo_root() -> pathlib.Path:
    """仓库根(XuanMing-Server/)。用于读 Go 源码做 parity 校验、读 configtable/dist。"""
    return REPO_ROOT


@pytest.fixture(scope="session")
def configtable_dist(repo_root: pathlib.Path) -> pathlib.Path:
    """真实的配置表 active 批次目录 —— 测试刻意用真数据而不是造夹具。

    理由:这批 json 的 checksum、行数、起始节点唯一性都是**当前线上事实**,
    用它做输入才能验证 Python 版加载器和 Go 版看到的是同一个批次。
    造一份假数据只能验代码自己,验不了跨语言一致性。
    """
    return repo_root / "configtable" / "dist"


@pytest.fixture(scope="session", autouse=True)
def _drop_session_database():
    """会话结束时删掉本进程建的独占测试库。

    ★ 为什么需要独占库见 tests/mysqlfixture.py 头注释(两个 pytest 同时跑会假红)。
    这里只负责收尾:不删的话开发机上会攒出一堆 pandora_test_<pid>_<ts>。

    清理失败一律静默 —— 清理不该让测试结果变红,而且库很可能压根没建出来
    (没有 MySQL 时数据层用例整体 skip)。
    """
    yield
    import asyncio
    import importlib

    try:
        mf = importlib.import_module("mysqlfixture")
        asyncmy = importlib.import_module("asyncmy")
    except Exception:  # noqa: BLE001
        return
    # ★ 必须与各数据层测试用**同一个** DSN。此前这里写死 13306 而
    #   test_player_repo 写死 3307,于是 player 在 3307 上建的
    #   `pandora_test_<pid>_<ts>` 永远删不掉(实测已攒 40+ 个孤儿库)。
    cfg = mf.parse_go_dsn(mf.MYSQL_DSN, default_db="")
    try:
        asyncio.run(mf.drop_database(asyncmy, cfg))
    except Exception:  # noqa: BLE001
        pass
