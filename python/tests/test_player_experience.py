"""玩家等级经验结算测试 —— 与 Go 侧 AdvanceExperience 逐条对拍。

进位循环的四个边界都是"写错了不报错"的:
  ① 满级 no-op(漏了 → 满级玩家 exp 无限涨、升级表现反复播)
  ② 升到满级瞬间清 0(保留了 → 满级后经验条是满的,像"还能再升")
  ③ 曲线项为 0 停止进位(不停 → 配错一个 0 就无限循环)
  ④ 极大 delta 按"足够升满"处理
"""

from __future__ import annotations

import pathlib
import shutil
import subprocess
import tempfile

import pytest

import goparity

from pandorapy.services.player import experience as exp

_GO_DUMP_PROGRAM = """package main

import "fmt"

func AdvanceExperience(level int32, expInLevel uint64, delta uint64, curve []uint64) (int32, uint64, uint32) {
	maxLevel := int32(len(curve)) + 1
	if level < 1 {
		level = 1
	}
	if level >= maxLevel {
		return maxLevel, 0, 0
	}
	exp := expInLevel + delta
	if exp < expInLevel {
		exp = ^uint64(0)
	}
	var gained uint32
	for level < maxLevel {
		need := curve[level-1]
		if need == 0 || exp < need {
			break
		}
		exp -= need
		level++
		gained++
		if level >= maxLevel {
			exp = 0
			break
		}
	}
	return level, exp, gained
}

func main() {
	curves := [][]uint64{
		{100, 200, 300, 400},
		{100},
		{100, 0, 300},
		{1, 1, 1, 1, 1, 1, 1, 1},
		{},
	}
	levels := []int32{-1, 0, 1, 2, 3, 4, 5, 6, 99}
	exps := []uint64{0, 1, 50, 99, 100, 199, 999}
	deltas := []uint64{0, 1, 50, 99, 100, 250, 1000, 100000, 18446744073709551615}
	for ci, curve := range curves {
		for _, lv := range levels {
			for _, e := range exps {
				for _, d := range deltas {
					nl, ne, g := AdvanceExperience(lv, e, d, curve)
					fmt.Printf("%d\\t%d\\t%d\\t%d\\t%d\\t%d\\t%d\\n", ci, lv, e, d, nl, ne, g)
				}
			}
		}
	}
}
"""

_CURVES = [
    [100, 200, 300, 400],
    [100],
    [100, 0, 300],   # 含非法 0 项
    [1] * 8,
    [],              # 空曲线 = 只有 1 级
]


def _go_table() -> list[tuple[int, int, int, int, int, int, int]]:
    if shutil.which("go") is None:
        return []
    with tempfile.TemporaryDirectory() as tmp:
        main_go = pathlib.Path(tmp) / "main.go"
        main_go.write_text(_GO_DUMP_PROGRAM, encoding="utf-8")
        try:
            proc = subprocess.run(
                ["go", "run", str(main_go)],
                capture_output=True, text=True, encoding="utf-8",
                timeout=180, check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return []
    if proc.returncode != 0:
        # ★ 「go 不在 PATH」与「go 在、但编译/运行失败」必须分两条路:
        #   前者 = 环境不具备      → skip
        #   后者 = **对拍对象变了** → fail,并把 stderr 带出来
        # 合成一条(rc!=0 就返回空表 → skip)的后果最坏:Go 侧真改了签名时,
        # 这道 parity 门**恰好在最该响的那一刻不响**,文案还指控环境不可用。
        pytest.fail(
            "跨语言对拍程序编译/运行失败 —— 多半是 internal/data/experience_repo.go 的经验曲线 变了,"
            "**不是**环境问题。stderr:\n" + (proc.stderr or "(空)")[:2000]
        )
    rows = []
    for line in proc.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) == 7:
            rows.append(tuple(int(p) for p in parts))
    return rows


# ── ★ 跨语言对拍 ────────────────────────────────────────────────────────────


def test_advance_experience_matches_go_exactly() -> None:
    """★ 逐条对拍。覆盖 5 条曲线 × 9 个等级 × 7 个级内经验 × 9 个 delta。

    包含负等级、满级、非法 0 曲线项、空曲线、uint64 上界 delta。
    """
    table = _go_table()
    if not table:
        pytest.skip("go 不可用 —— 经验结算跨语言对拍跳过(不假装通过)")

    mismatches = []
    for ci, lv, e, d, want_lv, want_e, want_g in table:
        got = exp.advance_experience(lv, e, d, _CURVES[ci])
        if got != (want_lv, want_e, want_g):
            mismatches.append(
                f"curve={_CURVES[ci]} level={lv} exp={e} delta={d}: "
                f"Go=({want_lv},{want_e},{want_g}) Py={got}"
            )
    assert not mismatches, (
        f"{len(mismatches)}/{len(table)} 条不一致。前 5 条:\n" + "\n".join(mismatches[:5])
    )
    assert len(table) > 2000, f"对拍样本太少({len(table)})"


# ── ★ 四个边界单独钉住 ──────────────────────────────────────────────────────


def test_max_level_is_noop() -> None:
    """★ ① 满级 no-op:不加经验,级内经验恒 0。

    漏了会让满级玩家的 exp 无限涨,客户端反复播升级表现。
    """
    curve = [100, 200]  # max_level = 3
    assert exp.advance_experience(3, 0, 99999, curve) == (3, 0, 0)
    assert exp.advance_experience(99, 500, 99999, curve) == (3, 0, 0)


def test_reaching_max_level_clears_overflow_exp() -> None:
    """★ ② 升到满级的瞬间级内经验清 0 —— 溢出经验**不保留**。

    保留的话玩家满级后经验条是满的,视觉上像"还能再升"。
    """
    curve = [100, 200]  # max_level = 3
    level, exp_in, gained = exp.advance_experience(1, 0, 100_000, curve)
    assert (level, exp_in, gained) == (3, 0, 2)


def test_zero_curve_entry_stops_progression() -> None:
    """★ ③ 曲线项为 0(非法配置)→ 停止进位,不是无限循环。"""
    curve = [100, 0, 300]  # 2 级升 3 级需要 0 经验 = 非法
    level, exp_in, gained = exp.advance_experience(1, 0, 1_000_000, curve)
    assert level == 2, "在非法 0 项处没有停下"
    assert gained == 1
    assert exp_in == 999_900  # 剩余经验原样留着


def test_huge_delta_does_not_overflow() -> None:
    """★ ④ 极大 delta 按"足够升满"处理,不回绕。"""
    curve = [100] * 5
    level, exp_in, gained = exp.advance_experience(1, 0, 2**64 - 1, curve)
    assert level == 6  # max_level = 6
    assert exp_in == 0
    assert gained == 5


# ── 常规进位 ────────────────────────────────────────────────────────────────


def test_single_level_up_keeps_remainder() -> None:
    curve = [100, 200, 300]
    assert exp.advance_experience(1, 0, 150, curve) == (2, 50, 1)


def test_multi_level_up_in_one_call() -> None:
    """连升多级 —— 一次入账跨过好几级是正常场景(战斗结算给一大笔经验)。"""
    curve = [100, 200, 300, 400]
    level, exp_in, gained = exp.advance_experience(1, 0, 350, curve)
    assert (level, exp_in, gained) == (3, 50, 2)


def test_not_enough_exp_stays() -> None:
    curve = [100, 200]
    assert exp.advance_experience(1, 30, 50, curve) == (1, 80, 0)


def test_exact_threshold_levels_up() -> None:
    """恰好达到阈值应当升级(>= 而不是 >)。"""
    curve = [100, 200]
    assert exp.advance_experience(1, 0, 100, curve) == (2, 0, 1)


def test_level_below_one_is_clamped() -> None:
    """等级列被写成 0 / 负数时按 1 级处理,不让索引越界。"""
    curve = [100, 200]
    assert exp.advance_experience(0, 0, 100, curve) == (2, 0, 1)
    assert exp.advance_experience(-5, 0, 100, curve) == (2, 0, 1)


def test_empty_curve_means_only_level_one() -> None:
    """空曲线 → max_level = 1,任何玩家都已满级。"""
    assert exp.advance_experience(1, 0, 99999, []) == (1, 0, 0)


# ── 展示装饰 ────────────────────────────────────────────────────────────────


def test_decorate_clamps_max_level_exp_to_zero() -> None:
    """满级展示时级内经验夹到 0 —— 防历史残留数据露出来。"""
    curve = [100, 200]
    assert exp.decorate_experience(3, 999, curve) == (0, True)
    assert exp.decorate_experience(2, 50, curve) == (50, False)


def test_decorate_disabled_passes_through() -> None:
    """功能关闭 / 曲线未配置 → 不标满级、exp 原样(与历史行为一致)。"""
    assert exp.decorate_experience(99, 500, [100], enabled=False) == (500, False)
    assert exp.decorate_experience(99, 500, []) == (500, False)


# ── ★ 手抄件必须与真源逐字一致 ──────────────────────────────────────────

_GO_EXP_SOURCE = "services/account/player/internal/data/experience_repo.go"


def test_hand_copied_go_matches_the_real_source(repo_root: pathlib.Path) -> None:
    """★ `_GO_DUMP_PROGRAM` 是**手抄**的 —— 这条证明它没过期。理由见
    tests/goparity.py 与 test_leaderboard_estimate 里的同名用例。

    这一半比 leaderboard 那半轻:实测把 `exp = 0`(升到满级瞬间清零)改成别的值,
    Go 侧自己的 TestAdvanceExperience_ReachMaxClampsExp / _WrapGuard 会当场变红,
    所以 Go 改坏了合并前会被拦住;缺的只是"Python 侧不会自动跟着红"。
    即便如此仍要有这条 —— strangler 共存期两栈同时对外服务,
    等级/经验算错在两边表现不同是玩家可见的。
    """
    go_src = (repo_root / _GO_EXP_SOURCE).read_text(encoding="utf-8")
    real = goparity.normalize(goparity.go_func(go_src, "AdvanceExperience"))
    copied = goparity.normalize(goparity.go_func(_GO_DUMP_PROGRAM, "AdvanceExperience"))
    assert copied == real, (
        f"手抄的 AdvanceExperience 与 {_GO_EXP_SOURCE} 已经不一致 —— "
        f"跨语言对拍对的是一份过期副本。\n真源:\n{real}\n手抄:\n{copied}"
    )
