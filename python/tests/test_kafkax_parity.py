"""Kafka 一致性哈希跨语言比对 —— 直接跑 Go 代码取路由表,逐条断言。

为什么必须做到这个强度:
    §9.9 "kafka topic key = 业务实体 ID" 的目的是同一玩家 / 同一对局的事件有序,
    而有序性只在**单个 partition 内**成立。迁移期两个实现同时生产同一批 topic,
    若 key→partition 映射有任何差异:
      - 同一玩家的事件被劈到两个 partition
      - 消费侧看到后发生的事件先到
      - **不报错**,只表现为偶发状态错乱(重复入账、进度回退),极难定位
    单元测试自己跟自己比是没用的 —— 必须跟 Go 的真实输出比。

做法:用 `go run` 跑一个临时程序导出 Go 侧完整路由表(207 个 key × 8 partition 的环),
Python 侧算同一批 key,逐条对比。Go 不可用时 skip 并说明,不假装通过。
"""

from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
import sys
import tempfile

import pytest

from pandorapy import kafkax

# 与 tests 里导出程序保持同一套输入。改这里必须同步改 _GO_DUMP_PROGRAM。
_PARTITION_COUNT = 8
_EXTRA_KEYS = [
    "0",
    "1",
    "18446744073709551615",  # uint64 上界
    "",  # 空 key
    "player:1001",
    "match:abc-123",
    "队伍-中文键",  # 非 ASCII:验证两边都按 UTF-8 字节喂 hash
]

_GO_DUMP_PROGRAM = """package main

import (
	"fmt"

	"github.com/luyuancpp/pandora/pkg/kafkax"
)

func main() {
	c := kafkax.NewConsistent()
	for p := int32(0); p < %d; p++ {
		c.AddPartition(p)
	}
	keys := []string{}
	for i := 0; i < 200; i++ {
		keys = append(keys, fmt.Sprintf("%%d", 25380000000000000+int64(i)*7919))
	}
	keys = append(keys, %s)
	for _, k := range keys {
		p, ok := c.GetPartition(k)
		fmt.Printf("%%s\\t%%d\\t%%t\\n", k, p, ok)
	}
}
"""


def _python_keys() -> list[str]:
    keys = [str(25380000000000000 + i * 7919) for i in range(200)]
    keys.extend(_EXTRA_KEYS)
    return keys


def _go_routing_table(repo_root: pathlib.Path) -> dict[str, tuple[int, bool]]:
    """跑 Go 程序拿路由表。Go 不可用返回空 dict(调用方 skip)。"""
    if shutil.which("go") is None:
        return {}
    quoted = ", ".join(f'"{k}"' for k in _EXTRA_KEYS)
    source = _GO_DUMP_PROGRAM % (_PARTITION_COUNT, quoted)
    with tempfile.TemporaryDirectory() as tmp:
        main_go = pathlib.Path(tmp) / "main.go"
        main_go.write_text(source, encoding="utf-8")
        try:
            proc = subprocess.run(
                ["go", "run", str(main_go)],
                cwd=repo_root / "pkg",  # 在 pkg module 里跑,才能 import kafkax
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=180,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            pytest.fail(f"go 在 PATH 上却跑不起来:{exc}")
    if proc.returncode != 0:
        # ★ 与 tests/test_source_revision.py 同一条纪律:「go 不在」才 skip,
        # 「go 在但编译失败」必须 fail 并带出 stderr —— 后者说明**对拍对象变了**,
        # 那正是这道门存在的唯一理由,不能让它退化成 skip。
        pytest.fail(
            "跨语言对拍程序编译/运行失败 —— 多半是 pkg/kafkax 的 API 变了,"
            "**不是**环境问题。stderr:\n" + (proc.stderr or "(空)")[:2000]
        )
    table: dict[str, tuple[int, bool]] = {}
    for line in proc.stdout.splitlines():
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        key, partition, ok = parts
        table[key] = (int(partition), ok == "true")
    return table


def test_routing_table_identical_to_go(repo_root: pathlib.Path) -> None:
    """★ Python 与 Go 的 key→partition 映射必须逐条相同。"""
    go_table = _go_routing_table(repo_root)
    if not go_table:
        pytest.skip(
            "无法运行 Go 导出程序(go 不可用或编译失败)—— 跨语言比对跳过,不假装通过"
        )

    consistent = kafkax.Consistent()
    for p in range(_PARTITION_COUNT):
        consistent.add_partition(p)

    mismatches: list[str] = []
    for key in _python_keys():
        py_partition, py_ok = consistent.get_partition(key)
        go_partition, go_ok = go_table[key]
        if (py_partition, py_ok) != (go_partition, go_ok):
            mismatches.append(
                f"key={key!r}: Go=({go_partition},{go_ok}) Python=({py_partition},{py_ok})"
            )

    assert not mismatches, (
        f"{len(mismatches)}/{len(go_table)} 个 key 路由到了不同 partition —— "
        f"同一玩家的事件会被劈到两个 partition,有序性(§9.9)被破坏。前 5 条:\n"
        + "\n".join(mismatches[:5])
    )
    assert len(go_table) == len(_python_keys()), "两边 key 集合不一致,比对无意义"


def test_fnv1a_matches_go_hash_fnv() -> None:
    """FNV-1a 32 位必须与 Go 的 hash/fnv.New32a 一致。

    用公开的 FNV-1a 测试向量固定住,不依赖 Go 可用性 —— 这样即使 CI 没装 Go,
    哈希函数本身仍有回归保护。
    """
    # 标准 FNV-1a 32 测试向量
    assert kafkax.fnv1a_32(b"") == 0x811C9DC5
    assert kafkax.fnv1a_32(b"a") == 0xE40C292C
    assert kafkax.fnv1a_32(b"foobar") == 0xBF9CF968


def test_empty_ring_returns_not_ok() -> None:
    """环为空时返回 (0, False) —— 与 Go 的 (0, false) 一致。

    调用方必须靠这个 False 走 fail-closed,不能把 partition 0 当默认值发出去
    (那会让所有 key 挤到 0 号 partition 并破坏有序性)。
    """
    consistent = kafkax.Consistent()
    assert consistent.get_partition("anything") == (0, False)


def test_add_partition_is_idempotent() -> None:
    """重复添加同一 partition 是 no-op —— 否则环会被同一 partition 的多份虚拟节点污染。"""
    consistent = kafkax.Consistent()
    consistent.add_partition(3)
    first = [consistent.get_partition(f"k{i}") for i in range(50)]
    consistent.add_partition(3)
    consistent.add_partition(3)
    assert consistent.partition_count() == 1
    assert [consistent.get_partition(f"k{i}") for i in range(50)] == first


def test_key_routing_is_insertion_order_independent() -> None:
    """同样的 partition 集合必须产出同样的路由,与插入顺序无关。"""
    a = kafkax.Consistent()
    for p in (0, 1, 2, 3):
        a.add_partition(p)
    b = kafkax.Consistent()
    for p in (3, 2, 1, 0):  # 反序添加
        b.add_partition(p)
    for i in range(200):
        key = f"player:{i}"
        assert a.get_partition(key) == b.get_partition(key), f"{key} 路由不稳定"


_HASHSEED_PROBE = """
import sys
sys.path.insert(0, %r)
from pandorapy import kafkax
c = kafkax.Consistent()
for p in (0, 1, 2, 3):
    c.add_partition(p)
print(",".join(str(c.get_partition("player:%%d" %% i)) for i in range(64)))
"""


def test_key_routing_survives_a_different_hash_seed() -> None:
    """★ 换一个 PYTHONHASHSEED 起新进程,路由必须**逐位相同**。

    这条才是"误用 Python 内置 hash()"的真哨兵。原来的版本在**同一个进程里**比两个
    实例 —— 而同进程内 `hash()` 本来就一致(种子是进程启动时定的),
    所以哪怕真的误用了 hash(),它也全绿。

    误用的后果是分区路由随进程随机:同一个玩家的事件在不同副本上落到不同 partition,
    §9.9「同一实体事件有序」当场失效,而且**重启才会变**,极难复现。
    """
    py_root = str(pathlib.Path(__file__).resolve().parents[1])
    outs = []
    for seed in ("0", "1", "12345"):
        env = dict(os.environ, PYTHONHASHSEED=seed, PYTHONUTF8="1")
        proc = subprocess.run(
            [sys.executable, "-c", _HASHSEED_PROBE % py_root],
            capture_output=True, text=True, encoding="utf-8", timeout=120,
            env=env, check=False,
        )
        assert proc.returncode == 0, f"探针进程失败(seed={seed}):{proc.stderr[:400]}"
        outs.append(proc.stdout.strip())
    assert outs[0] and len(set(outs)) == 1, (
        "不同 PYTHONHASHSEED 下路由不一致 —— 分区哈希用到了 Python 内置 hash()。 "
        + " | ".join(outs)
    )
