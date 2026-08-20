"""排行榜名次估算测试。

重点:
  1. ★ bucket_of 与 Go 逐条对拍(负分的 floor 除法是跨语言差异高发区)
  2. ★ 榜外估算名次不得落进精确区
  3. ★ 桶索引钳制(防异常分数撑爆 HASH field 数)
  4. 升序 / 降序两种榜的"比我优"方向相反
"""

from __future__ import annotations

import pathlib
import shutil
import subprocess
import tempfile

import pytest

import goparity

from pandorapy.services.leaderboard import estimate as est

# 与 tests 里的导出程序共用同一套输入。
_GO_DUMP_PROGRAM = """package main

import "fmt"

const maxBucketIdx int64 = 1 << 20

func bucketOf(score, width int64) int64 {
	q := score / width
	if score%width != 0 && (score < 0) != (width < 0) {
		q--
	}
	if q > maxBucketIdx {
		q = maxBucketIdx
	}
	if q < -maxBucketIdx {
		q = -maxBucketIdx
	}
	return q
}

func main() {
	widths := []int64{1, 25, 100, 7}
	scores := []int64{}
	for s := int64(-300); s <= 300; s++ {
		scores = append(scores, s)
	}
	scores = append(scores,
		-1, 0, 1, 24, 25, 26, -24, -25, -26, -1 << 40, 1 << 40,
		9223372036854775807, -9223372036854775808)
	for _, w := range widths {
		for _, s := range scores {
			fmt.Printf("%d\\t%d\\t%d\\n", s, w, bucketOf(s, w))
		}
	}
}
"""


def _go_bucket_table() -> list[tuple[int, int, int]]:
    """跑 Go 程序拿 bucketOf 输出。Go 不可用返回空(调用方 skip)。"""
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
            "跨语言对拍程序编译/运行失败 —— 多半是 internal/data/board_store.go 的直方图 变了,"
            "**不是**环境问题。stderr:\n" + (proc.stderr or "(空)")[:2000]
        )
    rows: list[tuple[int, int, int]] = []
    for line in proc.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) == 3:
            rows.append((int(parts[0]), int(parts[1]), int(parts[2])))
    return rows


# ── ★ 跨语言对拍 ────────────────────────────────────────────────────────────


def test_bucket_of_matches_go_exactly() -> None:
    """★ Python 的 `//` 与 Go 手工补的 floor 除法必须逐条相同。

    Go 的整数 `/` 是**向零截断**,所以那边写了
        q := score / width
        if score%width != 0 && (score<0) != (width<0) { q-- }
    Python 的 `//` 本身就是 floor —— 直接用是对的,但这不是显然的。

    写错的后果:负分玩家(MMR 可以为负)被分到错误的桶,
    估算名次系统性偏移一格,而且**不报错**。
    """
    table = _go_bucket_table()
    if not table:
        pytest.skip("go 不可用 —— bucket_of 跨语言对拍跳过(不假装通过)")

    mismatches = []
    for score, width, go_bucket in table:
        py_bucket = est.bucket_of(score, width)
        if py_bucket != go_bucket:
            mismatches.append(f"score={score} width={width}: Go={go_bucket} Py={py_bucket}")
    assert not mismatches, (
        f"{len(mismatches)}/{len(table)} 条桶归属不一致。前 5 条:\n"
        + "\n".join(mismatches[:5])
    )
    assert len(table) > 2000, f"对拍样本太少({len(table)}),覆盖不够"


def test_negative_score_floors_not_truncates() -> None:
    """★ 单独钉住负分方向 —— 这是最容易写错的一格。

    -1 // 25 = -1(floor),不是 0(截断)。
    """
    assert est.bucket_of(-1, 25) == -1
    assert est.bucket_of(-25, 25) == -1
    assert est.bucket_of(-26, 25) == -2
    assert est.bucket_of(0, 25) == 0
    assert est.bucket_of(24, 25) == 0
    assert est.bucket_of(25, 25) == 1


# ── ★ 桶索引钳制 ────────────────────────────────────────────────────────────


def test_bucket_index_is_clamped_both_directions() -> None:
    """★ 异常分数不能把 HASH 的 field 数撑到失控。"""
    assert est.bucket_of(2**62, 1) == est.MAX_BUCKET_IDX
    assert est.bucket_of(-(2**62), 1) == -est.MAX_BUCKET_IDX


def test_zero_width_defends_against_div_by_zero() -> None:
    """桶宽为 0 时用默认值兜底(biz 层已挡,这里再挡一道)。"""
    assert est.bucket_of(100, 0) == est.bucket_of(100, est.DEFAULT_ESTIMATE_BUCKET_WIDTH)


# ── ★ 估算名次不得落进精确区 ────────────────────────────────────────────────


def test_estimate_never_lands_inside_exact_range() -> None:
    """★ 榜外估算名次必须钳到 ZCARD+1 之后。

    直方图是约值,精确榜是精确值。不钳的话会出现"榜外玩家显示第 5 名,
    而精确榜第 5 名是另一个人"这种自相矛盾的展示。
    """
    # 直方图里几乎没人比我优 → 裸估算会算出很靠前的名次
    result = est.estimate_rank(
        entity_id=7, score=100, bucket_width=25,
        histogram={4: 1},  # 只有我自己这个桶
        on_board_count=500,  # 但精确榜有 500 人
        ascending=False,
    )
    assert result.rank == 501, f"估算名次落进了精确区:{result.rank}"


def test_estimate_uses_bucket_midpoint() -> None:
    """桶内取中位:比我优的桶计数和 + 本桶的一半。"""
    result = est.estimate_rank(
        entity_id=7, score=100, bucket_width=25,
        histogram={10: 30, 9: 20, 4: 9},  # 桶 4 是我的(100//25=4)
        on_board_count=0,
        ascending=False,  # 分高者前 → 桶索引大的更优
    )
    # better = 30 + 20 = 50;own = 9 → (9+1)//2 = 5
    assert result.rank == 55


def test_lone_member_in_bucket_ranks_one_not_zero() -> None:
    """★ 本桶只有我自己时应当算 1 不是 0 —— (own+1)//2 而不是 own//2。

    算成 0 会让名次出现"第 0 名"。
    """
    result = est.estimate_rank(
        entity_id=7, score=100, bucket_width=25,
        histogram={4: 1},
        on_board_count=0,
        ascending=False,
    )
    assert result.rank == 1


# ── ★ 升序 / 降序方向相反 ───────────────────────────────────────────────────


def test_descending_board_counts_higher_buckets_as_better() -> None:
    """降序榜(MMR):分数**高**的排前面 → 桶索引大的算"比我优"。"""
    result = est.estimate_rank(
        entity_id=7, score=100, bucket_width=25,
        histogram={4: 1, 8: 10, 2: 99},  # 桶 8 更高,桶 2 更低
        on_board_count=0, ascending=False,
    )
    assert result.rank == 10 + 1  # 只有桶 8 那 10 人比我优


def test_ascending_board_counts_lower_buckets_as_better() -> None:
    """升序榜(竞速用时):分数**低**的排前面 → 桶索引小的算"比我优"。"""
    result = est.estimate_rank(
        entity_id=7, score=100, bucket_width=25,
        histogram={4: 1, 8: 10, 2: 99},
        on_board_count=0, ascending=True,
    )
    assert result.rank == 99 + 1  # 只有桶 2 那 99 人比我优


# ── 脏数据与总数 ────────────────────────────────────────────────────────────


def test_dirty_buckets_are_skipped_not_fatal() -> None:
    """负计数 / 零计数的脏桶跳过,不影响其余桶。"""
    result = est.estimate_rank(
        entity_id=7, score=100, bucket_width=25,
        histogram={8: 10, 9: 0, 10: -5, 4: 1},
        on_board_count=0, ascending=False,
    )
    assert result.rank == 11
    assert result.total_submitters == 11  # 只统计正计数的桶


def test_total_submitters_counts_all_valid_buckets() -> None:
    """总人数供百分位展示,统计全部有效桶(含我自己那桶)。"""
    result = est.estimate_rank(
        entity_id=7, score=100, bucket_width=25,
        histogram={2: 100, 4: 50, 8: 30},
        on_board_count=0, ascending=False,
    )
    assert result.total_submitters == 180


def test_empty_histogram_yields_rank_after_board() -> None:
    """直方图为空(刚建榜)→ 名次仍须排在精确榜之后。"""
    result = est.estimate_rank(
        entity_id=7, score=100, bucket_width=25,
        histogram={}, on_board_count=200, ascending=False,
    )
    assert result.rank == 201
    assert result.total_submitters == 0


def test_default_bucket_width_matches_go() -> None:
    """桶宽默认值与 Go 侧同值(MMR 量纲)。"""
    assert est.DEFAULT_ESTIMATE_BUCKET_WIDTH == 25
    assert est.MAX_BUCKET_IDX == 1 << 20


# ── ★ 手抄件必须与真源逐字一致 ──────────────────────────────────────────

_GO_BUCKET_SOURCE = "services/runtime/leaderboard/internal/data/board_store.go"


def test_hand_copied_go_matches_the_real_source(repo_root: pathlib.Path) -> None:
    """★ 上面那段 `_GO_DUMP_PROGRAM` 是**手抄**的 —— 这条负责证明它没过期。

    没有这条时的形状:手抄件与被测 Python 是同一次理解的产物,Go 侧真改了两边都不红。
    实测过 —— 把 board_store.go 的 `q--` 改成 `q++`(负分 floor 补偿),
    本文件仍然 13 passed;而 `go test ./services/runtime/leaderboard/internal/data/`
    也全绿(该包没有负分样本)。也就是说 bucketOf 的负分语义在**全仓零覆盖**,
    而这个用例的失败文案却写着"多半是 board_store.go 的直方图变了" —— 那个文件
    从头到尾没被打开过。

    为什么不像 test_kafkax_parity 那样直接 import 真包:`bucketOf` 是
    `internal/data` 包内**未导出**函数,且 leaderboard 是**独立 go module**
    (services/runtime/leaderboard/go.mod),外部 `package main` 两条路都走不通。
    所以按 test_protosql_type_parity 的先例解析源码逐字核对。

    保留跨语言执行那一半(它抓的是 `//` floor vs `/` 截断这类**语言语义**差异),
    这一条补的是**实现漂移**那一半。两条缺一不可,别把 _GO_DUMP_PROGRAM 删了。
    """
    go_src = (repo_root / _GO_BUCKET_SOURCE).read_text(encoding="utf-8")
    real = goparity.normalize(goparity.go_func(go_src, "bucketOf"))
    copied = goparity.normalize(goparity.go_func(_GO_DUMP_PROGRAM, "bucketOf"))
    assert copied == real, (
        f"手抄的 bucketOf 与 {_GO_BUCKET_SOURCE} 已经不一致 —— "
        f"跨语言对拍对的是一份过期副本,等于没对。\n真源:\n{real}\n手抄:\n{copied}"
    )
    # 钳制常量单独核:它不在函数体里,改了同样会让桶索引上限漂移。
    assert goparity.go_const(go_src, "maxBucketIdx") == goparity.go_const(
        _GO_DUMP_PROGRAM, "maxBucketIdx"
    ), "maxBucketIdx 与真源不一致"
    assert est.MAX_BUCKET_IDX == 1 << 20, "Python 侧的钳制上限也要跟着改"
