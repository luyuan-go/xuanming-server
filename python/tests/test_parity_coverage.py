"""`tools/parity/coverage.py` 的牙齿验证（§7.2：拆掉防护必须变红）。

这个矩阵有两重身份，两重都要钉：

  1. **门禁**：`--check` 拦的是"写了但不生效"的静默缺陷（方法名拼错 / 没继承生成基类）。
     必须证明它真的会红，而且**不会**因为"迁移还没做完"而红 —— 平时是绿的门禁才有人留着。
  2. **事实来源**：矩阵的分母来自 Go 侧**实际注册**的全部 servicer。有几个服务挂了
     搭车 servicer（inventory→Bag、guild→Group、ds_allocator→Gm、四个服务→ConfigTableAdmin），
     只看与服务同名的那个 proto 会把 RPC 面算少一截，而"算少了"这件事本身没有任何信号。

第 3 组用例守的是**解析器静默失效**：如果 Python 侧的类体扫描哪天返回空，
矩阵会变成"什么都没迁"（看起来只是更悲观），但同时 `py_extra` 恒为空 ——
**门禁被无声关掉**。所以拿三个已知完整的服务当金丝雀。

⚠️ 变异实验刻意不改仓库文件（§7.2 的警告：共享工作区里就地改，
   另一个会话会看到一堆红去追不存在的 bug）。这里直接喂合成行给 `check()`。
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys

import pytest

_COVERAGE_PY = pathlib.Path(__file__).resolve().parents[1] / "tools" / "parity" / "coverage.py"


def _load():
    spec = importlib.util.spec_from_file_location("parity_coverage", _COVERAGE_PY)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    # 必须先进 sys.modules 再 exec —— dataclasses 会回查 cls.__module__，
    # 不注册的话 @dataclass 直接 AttributeError（踩过）。
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


cov = _load()


def _row(*, surface=(), orphans=()):
    return {"service": "demo", "surface": list(surface), "orphan_classes": list(orphans)}


def _surface(rpcs, implemented, extra=()):
    return {
        "servicer": "DemoService",
        "proto_file": "proto/pandora/demo/v1/demo.proto",
        "py_class": "DemoService",
        "py_file": "python/pandorapy/services/demo/service.py",
        "rpcs": list(rpcs),
        "py_implemented": list(implemented),
        "py_missing": [r for r in rpcs if r not in implemented],
        "py_extra": list(extra),
    }


# ── 1. 门禁必须拦住的（静默缺陷）─────────────────────────────────────────

def test_misspelled_method_is_caught():
    """proto 里没有的方法名 = 该 RPC 实际返回 UNIMPLEMENTED，且启动期零信号。"""
    rows = [_row(surface=[_surface(["DoThing"], [], extra=["DoThinng"])])]
    assert cov.check(rows) == 1


def test_class_not_inheriting_generated_servicer_is_caught():
    """名字像 servicer 却没继承生成基类：注册不报错，缺的方法也没有 UNIMPLEMENTED 兜底。"""
    rows = [_row(orphans=[{"cls": "DemoServicer", "file": "python/.../service.py"}])]
    assert cov.check(rows) == 1


# ── 2. 门禁必须放行的（否则第一天就红，会被关掉）─────────────────────────

def test_fully_implemented_service_passes():
    assert cov.check([_row(surface=[_surface(["DoThing"], ["DoThing"])])]) == 0


def test_incomplete_migration_is_not_a_failure():
    """"这个 RPC 还没迁"是预期状态。把它判红等于让门禁在第一天就没人看。"""
    assert cov.check([_row(surface=[_surface(["A", "B"], [])])]) == 0


# ── 3. 事实来源：搭车 servicer 不能被算漏 ────────────────────────────────

@pytest.fixture(scope="module")
def scanned():
    protos, go_rows = cov.build()
    return protos, {name: cov.analyse(protos, row) for name, row in go_rows.items()}


# 每一条都在 Go 侧 internal/server/grpc.go 里实际注册，
# 但 proto 文件名与服务名对不上 —— 靠"服务名猜 proto"的做法会整个漏掉。
PIGGYBACKED = [
    ("inventory", "BagService"),
    ("guild", "GroupService"),
    ("ds_allocator", "GmService"),
    ("player", "ConfigTableAdminService"),
    ("inventory", "ConfigTableAdminService"),
    ("matchmaker", "ConfigTableAdminService"),
    ("ds_allocator", "ConfigTableAdminService"),
]


@pytest.mark.parametrize(("service", "servicer"), PIGGYBACKED)
def test_piggybacked_servicers_are_counted(scanned, service, servicer):
    _, rows = scanned
    assert service in rows, f"没扫到服务 {service}"
    names = [s["servicer"] for s in rows[service]["surface"]]
    assert servicer in names, (
        f"{service} 在 Go 侧注册了 {servicer}，矩阵却没算进 RPC 分母 —— "
        f"当前只看到 {names}"
    )


def test_every_registered_servicer_resolves_to_a_proto(scanned):
    """Register 到的 servicer 必须都能在 proto 里找到 —— 找不到说明映射断了，分母会静默变 0。"""
    _, rows = scanned
    broken = [
        f"{name}:{s['servicer']}"
        for name, a in rows.items()
        for s in a["surface"]
        if s["proto_file"] is None
    ]
    assert not broken, f"这些注册的 servicer 在 proto 里找不到：{broken}"


# ── 4. 金丝雀：Python 侧解析器静默失效会让门禁一起哑掉 ───────────────────

# 这三个服务的 service.py 当前实现了对应 servicer 的**全部** rpc。
# 掉到 0 意味着类体扫描坏了 —— 那样 py_extra 也会恒空，门禁被无声关掉。
FULLY_IMPLEMENTED = {"owner": 5, "dialogue": 3, "trade": 4}


@pytest.mark.parametrize(("service", "expected"), sorted(FULLY_IMPLEMENTED.items()))
def test_python_side_parser_still_sees_implemented_rpcs(scanned, service, expected):
    _, rows = scanned
    a = rows[service]
    assert a["rpc_done"] == expected, (
        f"{service} 的 Python 实现数从 {expected} 变成 {a['rpc_done']}。"
        f"若不是真的删了 RPC，就是 coverage.py 的类体扫描坏了 —— "
        f"那样 py_extra 恒空，--check 门禁会静默失效"
    )


def test_repo_is_currently_free_of_silent_defects(scanned):
    """真仓库上的门禁必须是绿的 —— 这条红了就是真有一个 RPC 永远不会被调用。"""
    _, rows = scanned
    assert cov.check(list(rows.values())) == 0
