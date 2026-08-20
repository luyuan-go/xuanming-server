"""来源版本编码测试(INC-20260818-003)—— 与 Go 逐条对拍。

这是事故的直接修复,三条必须钉死:
  1. ★ 编码与 Go 逐位一致(全序跨语言成立,否则迁移期两栈判定相反)
  2. ★ 溢出 fail-closed,**绝不回绕**(回绕会让旧来源看起来更新)
  3. ★ legacy(0)与任何非零**不可比** —— 见过版本就永久拒 legacy
"""

from __future__ import annotations

import pathlib
import shutil
import subprocess
import tempfile

import pytest

from pandorapy import errcode
from pandorapy import source_revision as sr

# ★ 这段 Go 程序 **import 真正的 pkg/placement**,不是在测试文件里手抄一份重实现。
#
# 手抄版本是没有牙齿的:Go 侧改了编码规则,手抄的那份不会跟着变,对拍照样零差异 ——
# 而这条对拍存在的唯一理由,就是发现"Go 改了 Python 没跟上"。
# (真对拍的模板见 tests/test_kafkax_parity.py:同样 cwd=pkg 跑真包。)
_GO_DUMP = """package main

import (
	"fmt"

	"github.com/luyuancpp/pandora/pkg/placement"
)

func main() {
	terms := []uint64{
		0, 1, 2, 100, 65535, 1 << 20,
		placement.SourceRevisionMaxTerm,
		placement.SourceRevisionMaxTerm + 1,
	}
	seqs := []uint64{
		0, 1, 2, 100, 65535,
		placement.SourceRevisionMaxSeq,
		placement.SourceRevisionMaxSeq + 1,
	}
	for _, tm := range terms {
		for _, s := range seqs {
			v, err := placement.ComposeSourceRevision(tm, s)
			fmt.Printf("%d\\t%d\\t%d\\t%t\\n", tm, s, v, err == nil)
		}
	}
}
"""


def _go_table():
    """跑真 Go 包取对拍表。

    ★ 「go 不在 PATH」与「go 在、但编译/运行失败」必须**分成两条路**:

        前者 = 环境不具备      → skip(诚实,且文案指向装 go)
        后者 = **对拍对象变了** → **fail**,并把 stderr 带出来

    合成一条(rc != 0 就返回空表 → skip)的后果最坏:Go 侧真的改了签名 / 改了
    位布局时,临时程序编译失败 → 与"没装 go"走同一条路径 → 唯一一道跨语言
    parity 门**恰好在最该响的那一刻不响**,而且文案还指控环境不可用,
    把人往装 go 的方向带。
    """
    if shutil.which("go") is None:
        return []
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory() as tmp:
        p = pathlib.Path(tmp) / "main.go"
        p.write_text(_GO_DUMP, encoding="utf-8")
        try:
            proc = subprocess.run(
                ["go", "run", str(p)],
                # ★ 必须在 pkg module 里跑,否则 import placement 找不到。
                cwd=repo_root / "pkg",
                capture_output=True, text=True,
                encoding="utf-8", timeout=180, check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            pytest.fail(f"go 在 PATH 上却跑不起来:{exc}")
    if proc.returncode != 0:
        pytest.fail(
            "跨语言对拍程序编译/运行失败 —— 多半是 pkg/placement 的 API 变了,"
            "**不是**环境问题。stderr:\n" + (proc.stderr or "(空)")[:2000]
        )
    rows = []
    for line in proc.stdout.splitlines():
        parts = line.split("	")
        if len(parts) == 4:
            rows.append((int(parts[0]), int(parts[1]), int(parts[2]), parts[3] == "true"))
    return rows


# ── ★ 跨语言对拍 ────────────────────────────────────────────────────────────


def test_compose_matches_go_exactly() -> None:
    """★ 编码必须与 Go 逐位一致。

    不一致的后果:迁移期两栈对同一对 (term, seq) 算出不同的 revision,
    于是「谁的来源更新」在两边**结论相反** —— 而这道门正是为了消除这种分歧。
    """
    table = _go_table()
    if not table:
        pytest.skip("go 不可用 —— 来源版本编码对拍跳过(不假装通过)")

    mismatches = []
    for term, seq, want, ok in table:
        if ok:
            got = sr.compose(term, seq)
            if got != want:
                mismatches.append(f"term={term} seq={seq}: Go={want} Py={got}")
        else:
            try:
                sr.compose(term, seq)
                mismatches.append(f"term={term} seq={seq}: Go 拒绝而 Python 放行了")
            except errcode.PandoraError:
                pass
    assert not mismatches, "\n".join(mismatches[:5])
    assert len(table) >= 50


def test_bit_layout_matches_go() -> None:
    assert sr.SEQ_BITS == 24
    assert sr.MAX_SEQ == (1 << 24) - 1 == 16_777_215
    assert sr.MAX_TERM == (1 << 40) - 1
    assert sr.LEGACY == 0


# ── ★ 溢出 fail-closed,绝不回绕 ────────────────────────────────────────────


def test_term_zero_rejected() -> None:
    """★ term=0 = 没持有租约 —— 没有任期就没有全序,铸出来的号无法与他人比较。"""
    with pytest.raises(errcode.PandoraError, match="writer term"):
        sr.compose(0, 1)


def test_seq_zero_rejected() -> None:
    """★ seq 从 1 开始 —— 0 被 legacy 哨兵占用。"""
    with pytest.raises(errcode.PandoraError, match="seq must start at 1"):
        sr.compose(1, 0)


def test_overflow_is_error_not_wraparound() -> None:
    """★ 越界 fail-closed,**绝不回绕**。

    回绕会让一个旧来源看起来更新 —— 那正是本机制要防的事。
    """
    with pytest.raises(errcode.PandoraError, match="exceeds"):
        sr.compose(sr.MAX_TERM + 1, 1)
    with pytest.raises(errcode.PandoraError, match="exhausted"):
        sr.compose(1, sr.MAX_SEQ + 1)


def test_boundary_values_are_accepted() -> None:
    """边界值本身合法(守卫不能过严)。"""
    assert sr.compose(sr.MAX_TERM, sr.MAX_SEQ) == (sr.MAX_TERM << 24) | sr.MAX_SEQ
    assert sr.compose(1, 1) == (1 << 24) | 1


# ── ★ 全序 ─────────────────────────────────────────────────────────────────


def test_total_order_across_terms_and_seqs() -> None:
    """★ 全序:跨任期、跨进程重启都成立。

    这是整个机制的核心性质 —— 高位任期严格递增(etcd 保证),
    低位同任期内严格递增(单写者进程内保证)。
    """
    values = [
        sr.compose(1, 1), sr.compose(1, 2), sr.compose(1, sr.MAX_SEQ),
        sr.compose(2, 1),  # 新任期的第一个号 > 旧任期的最后一个号
        sr.compose(2, 2), sr.compose(100, 1),
    ]
    assert values == sorted(values), f"全序不成立:{values}"


def test_new_term_beats_old_term_max_seq() -> None:
    """★ 这一条最关键:进程崩溃重启拿到更大任期,低位从头开始也不会被旧号压过。"""
    old_max = sr.compose(5, sr.MAX_SEQ)
    new_first = sr.compose(6, 1)
    assert new_first > old_max


def test_split_is_only_for_logging() -> None:
    """拆分只用于排障 —— 判定一律直接比 revision 本身。"""
    rev = sr.compose(12345, 678)
    assert sr.split(rev) == (12345, 678)


# ── ★ Minter ───────────────────────────────────────────────────────────────


def test_minter_increases_within_term() -> None:
    m = sr.Minter()
    vals = [m.next(7) for _ in range(100)]
    assert vals == sorted(vals)
    assert len(set(vals)) == 100


def test_minter_resets_seq_on_new_term_but_stays_ordered() -> None:
    """★ 任期变了序号归零,但全序仍然成立(高位更大)。"""
    m = sr.Minter()
    old = [m.next(3) for _ in range(5)]
    new = [m.next(4) for _ in range(5)]
    assert min(new) > max(old), "新任期的号被旧任期压过了"
    assert sr.split(new[0])[1] == 1, "新任期序号没有从 1 重新开始"


def test_minter_never_yields_zero_seq() -> None:
    """seq 恒从 1 开始 —— 0 是 legacy 哨兵。"""
    m = sr.Minter()
    for term in (1, 2, 3):
        assert sr.split(m.next(term))[1] == 1


# ── ★ legacy 判定矩阵 ──────────────────────────────────────────────────────


def test_higher_revision_accepted() -> None:
    assert sr.classify(incoming=100, high_water=50) == sr.CLASSIFY_ACCEPT


def test_lower_revision_is_stale() -> None:
    assert sr.classify(incoming=49, high_water=50) == sr.CLASSIFY_STALE
    assert not sr.is_allowed(sr.CLASSIFY_STALE)


def test_same_revision_must_split_by_target() -> None:
    """★ incoming == high_water 必须按 target 分成两格,合成一格两个方向都会出事。

        一律放行 → 两个共用同一任期的写者互相覆盖(全序前提被打破却没人拦)
        一律拒   → at-least-once 的重复投递被判 stale,迁移永远完不成

    Go 的 classifySourceRevision 正是分成 same_revision_same_target /
    same_revision_different_target 两格,这里逐格对齐。
    """
    same = sr.classify(incoming=50, high_water=50, same_target=True)
    assert same == sr.CLASSIFY_SAME_REVISION_SAME_TARGET
    assert sr.is_allowed(same), "同一来源的重复投递必须幂等放行"
    assert not sr.advances_high_water(same), "重复投递不该推进高水位"

    reused = sr.classify(incoming=50, high_water=50, same_target=False)
    assert reused == sr.CLASSIFY_SAME_REVISION_REUSED
    assert not sr.is_allowed(reused), "同一版本号产出两个 target = 铸号被复制,必须拒"


def test_only_advancing_revision_moves_high_water() -> None:
    """唯一会推进高水位的路径就是"来源更新"这一格。"""
    assert sr.advances_high_water(sr.classify(incoming=51, high_water=50))
    for d in (
        sr.classify(incoming=49, high_water=50),
        sr.classify(incoming=50, high_water=50, same_target=True),
        sr.classify(incoming=sr.LEGACY, high_water=sr.LEGACY),
    ):
        assert not sr.advances_high_water(d)


def test_legacy_accepted_only_when_never_seen_a_version() -> None:
    """兼容窗:双方都没滚上本协议时放行。"""
    assert sr.classify(incoming=sr.LEGACY, high_water=sr.LEGACY) == sr.CLASSIFY_LEGACY_ACCEPTED


def test_legacy_permanently_rejected_after_seeing_a_version() -> None:
    """★ 整道门的关键:0 与任何非零**不可比**,不能当成"最小值"放行。

    否则旧写者靠「我不带版本」就能绕过整道门 —— 事故正是这个形状。
    """
    assert sr.classify(incoming=sr.LEGACY, high_water=1) == sr.CLASSIFY_LEGACY_REJECTED
    assert sr.classify(incoming=sr.LEGACY, high_water=10**9) == sr.CLASSIFY_LEGACY_REJECTED


def test_global_legacy_rejection_switch() -> None:
    """rollout 最后一步:全局开关打开后一律拒 legacy(连兼容窗那格也拒)。

    ★ 两种拒的**理由必须可区分**:全局门拒 vs 该玩家见过版本后拒 —— 处置完全不同
    (前者是发布节奏走到了最后一步,后者是有旧写者真的还在写)。
    """
    globally = sr.classify(
        incoming=sr.LEGACY, high_water=sr.LEGACY, reject_legacy_globally=True
    )
    assert globally == sr.CLASSIFY_LEGACY_REJECTED_GLOBALLY
    assert not sr.is_allowed(globally)
    assert globally != sr.CLASSIFY_LEGACY_REJECTED


def test_incident_scenario_is_blocked() -> None:
    """★ 直接复现事故:旧 binary 拿新 epoch 盲写旧 target。

    R1/R2 同任期,所以 writer_token 分不出;但 seq 不同 → revision 可比。
    旧来源(seq 小)必须被判 stale。
    """
    r1 = sr.compose(writer_term=1000, seq=1)  # 旧写者的来源
    r2 = sr.compose(writer_term=1000, seq=2)  # 新写者的来源(同任期!)
    assert r2 > r1
    # Owner 已接受 r2 后,r1 的迟到 Begin 必须被拒
    assert sr.classify(incoming=r1, high_water=r2) == sr.CLASSIFY_STALE
