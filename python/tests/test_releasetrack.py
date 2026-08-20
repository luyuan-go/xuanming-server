"""releasetrack 跨语言对拍 —— 对应 Go 侧 pkg/releasetrack/policy.go。

★ 这个文件守的是整个迁移里后果最重的一条不变量:
    同一个 player_id,Go 副本判 canary、Python 副本判 stable = 玩家在两个 Fleet 之间漂。
    不报错,只表现为"灰度比例对不上""回滚后还有人在 canary"。
    所以必须跟**真的 Go 编译器**比,不是自己跟自己比。

样本量:4 个 seed × 5 个 percent × 11 个 id = 220 例(要求 ≥200),外加
0% / 100% 两个边界各 11 例,共 242 例,必须 0 例不一致。
"""

from __future__ import annotations

import pathlib

import pytest

from goparity import run_go_json
from pandorapy import releasetrack

# ── 对拍样本(Python 是用例的唯一来源,Go 程序从 stdin 读)────────────────────

_SEEDS = ["release-2026-07", "s", "灰度-种子", "seed:with:colons"]
_PERCENTS = [1, 7, 37, 50, 99]
_IDS = [
    0,  # 无玩家上下文 → 恒 stable(即使 percent=100)
    1,
    2,
    3,
    42,
    1000,
    12345,
    99999,
    25380000000000000,  # 真实量级的雪花 ID
    9223372036854775808,  # 2**63:有符号/无符号解释不同则此例必炸
    18446744073709551615,  # uint64 上界
]

_GO_PROGRAM = """package main

import (
	"encoding/json"
	"os"

	"github.com/luyuancpp/pandora/pkg/releasetrack"
)

type input struct {
	Seeds    []string `json:"seeds"`
	Percents []uint32 `json:"percents"`
	IDs      []uint64 `json:"ids"`
}

type row struct {
	Seed    string `json:"seed"`
	Percent uint32 `json:"percent"`
	ID      uint64 `json:"id"`
	Track   string `json:"track"`
}

func main() {
	var in input
	if err := json.NewDecoder(os.Stdin).Decode(&in); err != nil {
		os.Stderr.WriteString(err.Error())
		os.Exit(2)
	}
	rows := []row{}
	emit := func(seed string, percent uint32) {
		p, err := releasetrack.New(percent, seed)
		if err != nil {
			os.Stderr.WriteString(err.Error())
			os.Exit(3)
		}
		for _, id := range in.IDs {
			rows = append(rows, row{Seed: seed, Percent: percent, ID: id, Track: p.Select(id)})
		}
	}
	for _, seed := range in.Seeds {
		for _, percent := range in.Percents {
			emit(seed, percent)
		}
	}
	emit("", 0)
	emit(in.Seeds[0], 100)
	out := map[string]any{
		"rows":   rows,
		"stable": releasetrack.Stable,
		"canary": releasetrack.Canary,
		"valid": map[string]bool{
			"stable": releasetrack.Valid("stable"),
			"canary": releasetrack.Valid("canary"),
			"":       releasetrack.Valid(""),
			"STABLE": releasetrack.Valid("STABLE"),
			"stable\\n": releasetrack.Valid("stable\\n"),
			"beta":   releasetrack.Valid("beta"),
		},
	}
	json.NewEncoder(os.Stdout).Encode(out)
}
"""


def _python_rows() -> list[tuple[str, int, int, str]]:
    rows: list[tuple[str, int, int, str]] = []

    def emit(seed: str, percent: int) -> None:
        policy = releasetrack.new(percent, seed)
        for pid in _IDS:
            rows.append((seed, percent, pid, policy.select(pid)))

    for seed in _SEEDS:
        for percent in _PERCENTS:
            emit(seed, percent)
    emit("", 0)
    emit(_SEEDS[0], 100)
    return rows


def test_cohort_selection_identical_to_go(repo_root: pathlib.Path) -> None:
    """★ 核心:242 例 cohort 选择必须与 Go 逐例相同,0 例不一致。"""
    got = run_go_json(
        repo_root,
        _GO_PROGRAM,
        {"seeds": _SEEDS, "percents": _PERCENTS, "ids": _IDS},
    )
    if got is None:
        pytest.skip("go 不在 PATH 上 —— 跨语言对拍跳过,不假装通过")

    go_rows = [(r["seed"], r["percent"], r["id"], r["track"]) for r in got["rows"]]
    py_rows = _python_rows()
    assert len(go_rows) >= 200, f"样本量 {len(go_rows)} < 200,不足以称为对拍"
    assert len(go_rows) == len(py_rows)

    mismatched = [
        (g, p) for g, p in zip(go_rows, py_rows, strict=True) if g != p
    ]
    assert not mismatched, (
        f"{len(mismatched)}/{len(go_rows)} 例 cohort 选择与 Go 不一致,"
        f"前 5 例(Go, Python): {mismatched[:5]}"
    )

    # 轨道字面量本身也对拍:改成 "gray" 之类会让 Agones Fleet 名与已落盘记录全部对不上。
    assert got["stable"] == releasetrack.STABLE
    assert got["canary"] == releasetrack.CANARY

    for track, want in got["valid"].items():
        assert releasetrack.valid(track) is want, f"valid({track!r}) 与 Go 不一致"


def test_select_is_deterministic_and_valid() -> None:
    """同一 ID 反复选必须同一结果,且结果恒在两个合法轨道内(对应 Go 的单测)。"""
    policy = releasetrack.new(37, "release-2026-07")
    for pid in range(1, 1001):
        first = policy.select(pid)
        assert releasetrack.valid(first)
        assert policy.select(pid) == first


def test_boundary_order_zero_id_beats_full_canary() -> None:
    """percent=100 且 id=0 → stable。

    这是 Go 里两个 if 的先后顺序造成的(policy.go:33 的 `p.percent == 0 || id == 0`
    在 `p.percent == 100` 之前)。把两个判据调换会让所有"无玩家上下文"的分配
    全部跑去 canary —— 灰度比例静默失真。
    """
    full = releasetrack.new(100, "release-2026-07")
    assert full.select(0) == releasetrack.STABLE
    assert full.select(1) == releasetrack.CANARY

    zero = releasetrack.new(0, "")
    assert zero.select(0) == releasetrack.STABLE
    assert zero.select(42) == releasetrack.STABLE


def test_new_rejects_out_of_range_percent_and_missing_seed() -> None:
    """两条 fail-closed 前置,方向必须与 Go 一致。"""
    with pytest.raises(ValueError, match="out of range"):
        releasetrack.new(101, "seed")
    with pytest.raises(ValueError, match="canary_seed required"):
        releasetrack.new(1, "")
    # percent=0 时空 seed 合法(Go 的 New(0, "") 不报错)
    assert releasetrack.new(0, "").select(42) == releasetrack.STABLE


def test_new_rejects_values_go_cannot_represent() -> None:
    """★ Python 特有的收紧:Go 的 uint32 形参挡掉的东西这里必须显式挡。

    不挡的话 percent=-1 会让 `bucket < percent` 恒 false —— 灰度**静默全关**,
    配置上看在放量,实际一个人都没进 canary。
    """
    with pytest.raises(ValueError, match="out of range"):
        releasetrack.new(-1, "seed")
    with pytest.raises(ValueError, match="out of range"):
        releasetrack.new(1 << 32, "seed")
    with pytest.raises(ValueError, match="out of range"):
        releasetrack.new("50", "seed")  # type: ignore[arg-type]


def test_select_rejects_ids_go_cannot_represent() -> None:
    """Select(id uint64):负数与超 64 位在 Go 里不可能出现,Python 必须显式抛。"""
    policy = releasetrack.new(50, "seed")
    with pytest.raises(ValueError, match="uint64"):
        policy.select(-1)
    with pytest.raises(ValueError, match="uint64"):
        policy.select(1 << 64)


def test_valid_is_fail_closed() -> None:
    """未知轨道一律拒。空串被放行会让"没写 track 的脏记录"被当成合法。"""
    assert releasetrack.valid("stable")
    assert releasetrack.valid("canary")
    assert not releasetrack.valid("")
    assert not releasetrack.valid("Stable")
    assert not releasetrack.valid("beta")
    # ★ 正则陷阱的等价体检:Go 的字符串比较不会放过尾随换行,Python 的 == 也不会。
    #   (本模块刻意不用正则;这条是防止将来有人把它改写成 re.match(r"^(stable|canary)$")。)
    assert not releasetrack.valid("stable\n")


def test_distribution_roughly_matches_percent() -> None:
    """分桶大致均匀 —— 不是精度测试,是"哈希取模没写反"的粗筛。

    若把 `bucket < percent` 写成 `>`,这条会立刻红(37% 变成 63%)。
    """
    policy = releasetrack.new(37, "release-2026-07")
    canary = sum(
        1 for pid in range(1, 20001) if policy.select(pid) == releasetrack.CANARY
    )
    ratio = canary / 20000
    assert 0.34 < ratio < 0.40, f"canary 占比 {ratio:.3f},与配置的 37% 相差过大"
