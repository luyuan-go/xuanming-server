"""battleabort 跨语言对拍 —— 对应 Go 侧 pkg/battleabort/abort.go。

守三件事:
  1. **待签字节逐字节相同**。Go 签、Python 验(或反过来),差一个字节就是全部验签失败。
  2. **形状闸逐例相同**。Python 更严 → 合法 abort 被拒(分配拆不掉,DS 白占);
     Python 更松 → 放进 Go 会拒的字节。
  3. **长度前缀的单射性**。字段边界移动必须改变签名体,否则针对 A 实例的签名能被
     重放成针对 B 实例的中止。

顺带对拍 placement.Target 的 complete_battle / complete_hub —— 它们用的是 Go 的
strings.TrimSpace 语义,而 Python 的 str.strip() 词表不同(见 placement.go_is_space)。
"""

from __future__ import annotations

import pathlib

import pytest

from goparity import run_go_json
from pandorapy import battleabort, placement

_OP = "550e8400-e29b-41d4-a716-446655440000"
_OP2 = "550e8400-e29b-41d4-a716-446655440001"


def _case(**kw) -> dict:
    base = {
        "match_id": 42,
        "operation_id": _OP,
        "pod_name": "battle-42",
        "instance_uid": "uid-42",
        "instance_epoch": 7,
        "assignment_id": "",
        "allocation_id": "alloc-42",
        "release_track": "stable",
    }
    base.update(kw)
    return base


_CASES: list[dict] = [
    _case(),  # Go 单测原样的合法请求
    _case(release_track="canary"),
    _case(match_id=43),
    _case(operation_id=_OP2),
    _case(pod_name="other"),
    _case(instance_uid="other"),
    _case(instance_epoch=8),
    _case(allocation_id="other"),
    # ── 形状闸的拒绝面 ──
    {
        "match_id": 0,
        "operation_id": "",
        "pod_name": "",
        "instance_uid": "",
        "instance_epoch": 0,
        "assignment_id": "",
        "allocation_id": "",
        "release_track": "",
    },
    _case(match_id=0),
    _case(operation_id="not-an-operation"),
    _case(operation_id=_OP.upper()),  # 大写 UUID:能解析但非 canonical → 拒
    _case(operation_id="{" + _OP + "}"),  # 花括号写法 → 拒
    _case(operation_id="00000000-0000-4000-8000-000000000000"),  # 非 Nil 的合法 v4
    _case(assignment_id="hub-assignment"),  # 带 Hub 身份 → 拒
    _case(release_track="future"),
    _case(release_track="Stable"),
    _case(release_track=""),
    _case(instance_epoch=0),
    _case(allocation_id=""),
    # ── 空白 / 控制符 ──
    _case(pod_name="pod\nshift"),
    _case(pod_name="pod shift"),
    _case(pod_name=" pod"),
    _case(pod_name="pod "),
    _case(pod_name="\x1c"),  # ★ Go 的 IsSpace 不认它,IsControl 认;Python str.strip() 会剥掉它
    _case(pod_name="a\x1cb"),
    _case(pod_name="\xa0pod"),  # NBSP:Go 的 White_Space 认
    _case(pod_name="pod\u2028x"),  # LINE SEPARATOR
    _case(pod_name="pod\u200bx"),  # ZERO WIDTH SPACE:Unicode 里**不是**空白 → 放行
    _case(pod_name="pod\u3000x"),  # 表意空格
    _case(pod_name="\x7f"),  # DEL:C1 之前的控制符
    _case(pod_name="\u0085"),  # NEL:既是 C1 控制符又是 White_Space
    # ── 字节长度上限(Go 的 len 是字节数,Python 的 len 是字符数)──
    _case(pod_name="p" * 253),
    _case(pod_name="p" * 254),
    _case(pod_name="中" * 84),  # 252 字节 → 过
    _case(pod_name="中" * 85),  # 255 字节 → 拒(按字符数算只有 85,会误放行)
    _case(instance_uid="u" * 128),
    _case(instance_uid="u" * 129),
    _case(instance_uid="中" * 43),  # 129 字节 → 拒
    _case(allocation_id="a" * 128),
    _case(allocation_id="a" * 129),
    # ── 长度前缀单射性:分隔符跨字段移动 ──
    _case(pod_name="a\nb", instance_uid="c"),
    _case(pod_name="a", instance_uid="b\nc"),
    _case(pod_name="ab", instance_uid="c"),
    _case(pod_name="a", instance_uid="bc"),
]

_GO_PROGRAM = """package main

import (
	"encoding/hex"
	"encoding/json"
	"os"

	"github.com/luyuancpp/pandora/pkg/battleabort"
	"github.com/luyuancpp/pandora/pkg/placement"
)

type caseIn struct {
	MatchID       uint64 `json:"match_id"`
	OperationID   string `json:"operation_id"`
	PodName       string `json:"pod_name"`
	InstanceUID   string `json:"instance_uid"`
	InstanceEpoch uint32 `json:"instance_epoch"`
	AssignmentID  string `json:"assignment_id"`
	AllocationID  string `json:"allocation_id"`
	ReleaseTrack  string `json:"release_track"`
}

type caseOut struct {
	Complete       bool   `json:"complete"`
	ValidTarget    bool   `json:"valid_target"`
	CompleteBattle bool   `json:"complete_battle"`
	CompleteHub    bool   `json:"complete_hub"`
	Canonical      string `json:"canonical"`
}

func main() {
	var cases []caseIn
	if err := json.NewDecoder(os.Stdin).Decode(&cases); err != nil {
		os.Stderr.WriteString(err.Error())
		os.Exit(2)
	}
	out := make([]caseOut, 0, len(cases))
	for _, c := range cases {
		t := placement.Target{
			PodName: c.PodName, InstanceUID: c.InstanceUID, InstanceEpoch: c.InstanceEpoch,
			AssignmentID: c.AssignmentID, AllocationID: c.AllocationID, ReleaseTrack: c.ReleaseTrack,
		}
		r := battleabort.Request{MatchID: c.MatchID, OperationID: c.OperationID, Target: t}
		out = append(out, caseOut{
			Complete:       r.Complete(),
			ValidTarget:    battleabort.ValidTarget(t),
			CompleteBattle: t.CompleteBattle(),
			CompleteHub:    t.CompleteHub(),
			Canonical:      hex.EncodeToString(r.Canonical()),
		})
	}
	json.NewEncoder(os.Stdout).Encode(out)
}
"""


def _py_request(case: dict) -> battleabort.Request:
    return battleabort.Request(
        match_id=case["match_id"],
        operation_id=case["operation_id"],
        target=placement.Target(
            pod_name=case["pod_name"],
            instance_uid=case["instance_uid"],
            instance_epoch=case["instance_epoch"],
            assignment_id=case["assignment_id"],
            allocation_id=case["allocation_id"],
            release_track=case["release_track"],
        ),
    )


def test_canonical_and_gates_identical_to_go(repo_root: pathlib.Path) -> None:
    """★ 核心:每个用例的待签字节 + 四个布尔闸必须与 Go 逐例相同。"""
    got = run_go_json(repo_root, _GO_PROGRAM, _CASES)
    if got is None:
        pytest.skip("go 不在 PATH 上 —— 跨语言对拍跳过,不假装通过")

    assert len(got) == len(_CASES)
    for case, go_row in zip(_CASES, got, strict=True):
        req = _py_request(case)
        label = {k: v for k, v in case.items() if v not in ("", 0)}
        assert req.canonical().hex() == go_row["canonical"], f"待签字节不一致:{label}"
        assert req.complete() is go_row["complete"], f"complete 不一致:{label}"
        assert battleabort.valid_target(req.target) is go_row["valid_target"], (
            f"valid_target 不一致:{label}"
        )
        assert req.target.complete_battle() is go_row["complete_battle"], (
            f"complete_battle 不一致(TrimSpace 词表差异?):{label}"
        )
        assert req.target.complete_hub() is go_row["complete_hub"], (
            f"complete_hub 不一致:{label}"
        )

    # 用例集里必须同时有通过与拒绝的样本,否则"全拒"也能全绿
    assert any(r["complete"] for r in got), "没有一个用例通过 complete —— 对拍没有正样本"
    assert any(not r["complete"] for r in got), "没有一个用例被拒 —— 对拍没有负样本"


def test_every_signed_field_changes_canonical_body() -> None:
    """签名体必须绑定每一个字段(对应 Go 的 TestRequestCanonicalBindsEveryField)。

    某个字段没进编码 = 拿着一个合法签名可以任意改那个字段 —— 例如改 instance_epoch
    就能把中止指向同名 Pod 的**另一个**实例。
    """
    base = _py_request(_case())
    want = base.canonical()
    mutations = [
        _case(match_id=43),
        _case(operation_id=_OP2),
        _case(pod_name="other"),
        _case(instance_uid="other"),
        _case(instance_epoch=8),
        _case(allocation_id="other"),
        _case(release_track="canary"),
    ]
    for i, mutation in enumerate(mutations):
        assert _py_request(mutation).canonical() != want, f"变异 {i} 没有改变签名体"


def test_length_prefix_prevents_field_boundary_collision() -> None:
    """★ 分隔符跨字段移动必须改变签名体(对应 Go 的同名单测)。

    换行拼接的编码里 ("a\\nb","c") 与 ("a","b\\nc") 会拼出同一个串。
    """
    left = _py_request(_case(pod_name="a\nb", instance_uid="c"))
    right = _py_request(_case(pod_name="a", instance_uid="b\nc"))
    assert left.canonical() != right.canonical()
    # 两者都含控制符,签名前的形状闸也必须先拒掉
    assert not left.complete()
    assert not right.complete()

    # 无控制符版本同样不能碰撞
    assert (
        _py_request(_case(pod_name="ab", instance_uid="c")).canonical()
        != _py_request(_case(pod_name="a", instance_uid="bc")).canonical()
    )


def test_canonical_domain_is_first_field() -> None:
    """域分隔串必须是第一段,且带 4 字节长度前缀。

    域串缺失/改动 = 同一把密钥签出的其它类型消息可能被当成 abort 重放。
    """
    body = _py_request(_case()).canonical()
    domain = battleabort.CANONICAL_DOMAIN.encode("utf-8")
    assert body[:4] == len(domain).to_bytes(4, "big")
    assert body[4 : 4 + len(domain)] == domain
    assert battleabort.CANONICAL_DOMAIN == "pandora-battle-allocation-abort-v1"


def test_canonical_rejects_values_go_cannot_represent() -> None:
    """★ Python 特有的收紧:Go 的 uint64/uint32 静态类型挡掉的东西必须显式抛。

    照抄 Go 的"回绕"(uint32(len) 静默截断)在 Python 里是主动制造签名体碰撞,
    所以这里选 fail-closed —— 详见 battleabort._pack_uint32 的注释。
    """
    with pytest.raises(ValueError, match="uint64"):
        _py_request(_case(match_id=1 << 64)).canonical()
    with pytest.raises(ValueError, match="uint64"):
        _py_request(_case(match_id=-1)).canonical()
    with pytest.raises(ValueError, match="uint32"):
        _py_request(_case(instance_epoch=1 << 32)).canonical()
    with pytest.raises(ValueError, match="uint32"):
        _py_request(_case(instance_epoch=-1)).canonical()
    # complete() 不能因为越界而抛 —— 它是布尔闸,必须能对任意输入给出答案
    assert _py_request(_case(match_id=1 << 64)).complete() is False
    assert _py_request(_case(instance_epoch=1 << 32)).complete() is False


def test_go_space_table_differs_from_python_strip() -> None:
    """★ 显式钉住"不能用 Python 的 str.strip()/str.isspace()"这条。

    U+001C(文件分隔符)在 Python 里算空白、在 Go 的 unicode.IsSpace 里不算。
    照 Go 的语义 "\\x1c" 是个非空字段(complete_battle 那一格通过),
    照 Python 的语义它 strip 完是空串。这条差异只在 complete_battle 上可见 ——
    valid_target 那边有控制符循环兜着,看不出来。
    """
    assert "\x1c".isspace() is True  # Python 认
    assert placement.go_is_space("\x1c") is False  # Go 不认
    assert placement.go_trim_space("\x1c") == "\x1c"
    assert "\x1c".strip() == ""

    target = placement.Target(
        pod_name="\x1c",
        instance_uid="uid",
        instance_epoch=1,
        allocation_id="alloc",
        release_track="stable",
    )
    assert target.complete_battle() is True  # 与 Go 一致
    assert battleabort.valid_target(target) is False  # 控制符循环仍然拒签


def test_target_epoch_must_fit_uint32() -> None:
    """instance_epoch 在 Go 里是 uint32;Python 的无限精度 int 必须显式收口。

    不收的话一个 2**40 的 epoch 会通过 complete 校验、被签进 abort body,
    而 Go 副本读回来时 uint32 装不下直接解码失败。
    """
    ok = placement.Target(
        pod_name="p", instance_uid="u", instance_epoch=(1 << 32) - 1,
        allocation_id="a", release_track="stable",
    )
    assert ok.complete_battle() is True
    over = placement.Target(
        pod_name="p", instance_uid="u", instance_epoch=1 << 32,
        allocation_id="a", release_track="stable",
    )
    assert over.complete_battle() is False
    assert over.complete_hub() is False


def test_valid_target_rejects_hub_assignment() -> None:
    """带 assignment_id = 拿错了 target 类型,继续走下去会去拆一个 Hub 座位。"""
    battle = placement.Target(
        pod_name="p", instance_uid="u", instance_epoch=1,
        allocation_id="a", release_track="stable",
    )
    assert battleabort.valid_target(battle) is True
    hybrid = placement.Target(
        pod_name="p", instance_uid="u", instance_epoch=1,
        assignment_id="hub-1", allocation_id="a", release_track="stable",
    )
    assert battleabort.valid_target(hybrid) is False


def test_byte_length_limit_not_character_length() -> None:
    """★ 253/128 是**字节**上限。按字符数算会放行 3 倍长的中文名。"""
    ok = _py_request(_case(pod_name="中" * 84))  # 252 字节
    assert ok.complete() is True
    over = _py_request(_case(pod_name="中" * 85))  # 255 字节,但只有 85 个字符
    assert over.complete() is False
