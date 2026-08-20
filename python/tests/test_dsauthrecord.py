"""dsauthrecord 跨语言对拍 —— 对应 Go 侧 pkg/dsauthrecord/battle_result.go。

这是一份落在 Redis 里、由 ds_allocator 在终态 CAS 中与 auth / battle 同事务写入的 JSON。
双栈并行期 Go 写 Python 读(或反过来),所以三样东西必须对齐:
  1. **Redis key**(含 Cluster hash tag 的花括号)
  2. **序列化字节**(字段名、字段顺序、转义词表)
  3. **Valid / SameCredential 的判据与方向**(尤其"过期不使 receipt 失效"这条 fail-open)
"""

from __future__ import annotations

import pathlib

import pytest

from goparity import run_go_json
from pandorapy import dsauthrecord as rec

# ── 对拍样本 ────────────────────────────────────────────────────────────────


def _case(**kw) -> dict:
    base = {
        "match_id": 9,
        "allocation_id": "alloc",
        "pod_name": "battle-9",
        "instance_uid": "uid",
        "instance_epoch": 3,
        "gen": 7,
        "jti": "jti",
        "exp_ms": 2000,
        "kid": "kid",
        "token_sha256": "hash",
        "writer_epoch": 2,
        "recorded_at_ms": 1000,
        "now_ms": 1500,
    }
    base.update(kw)
    return base


_CASES: list[dict] = [
    _case(),  # Go 单测原样
    _case(now_ms=3000),  # ★ token 早已过期(exp_ms=2000)但 receipt 仍必须有效
    _case(now_ms=1000),  # now == recorded_at
    _case(now_ms=999),  # now < recorded_at → 无效(未来时间戳)
    _case(match_id=0),
    _case(allocation_id=""),
    _case(pod_name=""),
    _case(instance_uid=""),
    _case(instance_epoch=0),
    _case(gen=0),
    _case(jti=""),
    _case(kid=""),
    _case(token_sha256=""),
    _case(writer_epoch=0),
    _case(recorded_at_ms=0),
    _case(recorded_at_ms=-1),
    _case(exp_ms=1000),  # exp == recorded → 无效
    _case(exp_ms=999),  # exp < recorded → 无效
    _case(match_id=18446744073709551615, now_ms=1500),
    _case(gen=18446744073709551615),
    _case(instance_epoch=4294967295, writer_epoch=4294967295),
    _case(exp_ms=9223372036854775807),
    # ── 字符串转义:Go 与 Python 的 JSON 转义词表有四处不同 ──
    _case(kid='he said "hi"'),
    _case(kid="back\\slash"),
    _case(kid="tab\there"),
    _case(kid="nl\nhere"),
    _case(kid="cr\rhere"),
    _case(kid="bs\bhere"),  # 短转义 \b —— 实测 Go 与 Python 一致(曾被误以为不同)
    _case(kid="ff\fhere"),  # 短转义 \f —— 同上
    _case(kid="<script>&amp;"),  # Go 转义 <>& 成 \u00xx,Python 不转
    _case(kid="line\u2028sep\u2029par"),  # Go 转义,Python 不转
    _case(kid="中文 kid"),  # Go 原样 UTF-8,Python 默认 ensure_ascii 会转义
    _case(pod_name="pod-\u00e9\u4e2d"),
    _case(jti="\x01\x1f"),  # C0 控制符 → 两边都是 \u00xx
]

# 直接喂给 Unmarshal 的原始 payload —— 覆盖 Go 的 json 解码会报错的每一类。
_PAYLOADS: list[str] = [
    '{"version":1,"match_id":9,"allocation_id":"a","pod_name":"p","instance_uid":"u",'
    '"instance_epoch":3,"gen":7,"jti":"j","exp_ms":2000,"kid":"k",'
    '"token_sha256":"h","writer_epoch":2,"recorded_at_ms":1000}',
    "{}",
    "null",
    '{"version":1,"unknown_field":"ignored","match_id":5}',
    '{"Version":1,"MATCH_ID":5}',  # ★ Go 的 json 解码是大小写不敏感回退匹配
    '{"match_id":-1}',  # 负数进 uint64 → Go 报错
    '{"instance_epoch":4294967296}',  # 溢出 uint32
    '{"exp_ms":9223372036854775808}',  # 溢出 int64
    '{"match_id":"5"}',  # 字符串进数字字段
    '{"match_id":true}',  # 布尔进数字字段
    '{"match_id":1.5}',  # 小数进整数字段
    '{"match_id":2.0}',  # ★ 即使是整数值的浮点字面量,Go 也拒
    '{"match_id":null}',  # null → 不改动字段,不报错
    '{"kid":123}',  # 数字进字符串字段
    "[1,2]",  # 顶层不是对象
    "not json",
    '{"version":1} trailing',
    "",  # 空 → "empty battle result receipt"
]

_GO_PROGRAM = """package main

import (
	"encoding/json"
	"os"

	"github.com/luyuancpp/pandora/pkg/dsauthrecord"
)

type caseIn struct {
	MatchID       uint64 `json:"match_id"`
	AllocationID  string `json:"allocation_id"`
	PodName       string `json:"pod_name"`
	InstanceUID   string `json:"instance_uid"`
	InstanceEpoch uint32 `json:"instance_epoch"`
	Gen           uint64 `json:"gen"`
	JTI           string `json:"jti"`
	ExpMs         int64  `json:"exp_ms"`
	Kid           string `json:"kid"`
	TokenSHA256   string `json:"token_sha256"`
	WriterEpoch   uint32 `json:"writer_epoch"`
	RecordedAtMs  int64  `json:"recorded_at_ms"`
	NowMs         int64  `json:"now_ms"`
}

type caseOut struct {
	Key         string `json:"key"`
	Valid       bool   `json:"valid"`
	SameAsFirst bool   `json:"same_as_first"`
	Marshaled   string `json:"marshaled"`
	MarshalErr  string `json:"marshal_err"`
}

type decodeOut struct {
	Err      string                             `json:"err"`
	Receipt  dsauthrecord.BattleResultReceipt   `json:"receipt"`
}

type input struct {
	Receipts []caseIn `json:"receipts"`
	Payloads []string `json:"payloads"`
}

func build(c caseIn) dsauthrecord.BattleResultReceipt {
	return dsauthrecord.NewBattleResultReceipt(
		c.MatchID, c.AllocationID, c.PodName, c.InstanceUID, c.InstanceEpoch,
		c.Gen, c.JTI, c.ExpMs, c.Kid, c.TokenSHA256, c.WriterEpoch, c.RecordedAtMs)
}

func main() {
	var in input
	if err := json.NewDecoder(os.Stdin).Decode(&in); err != nil {
		os.Stderr.WriteString(err.Error())
		os.Exit(2)
	}
	if len(in.Receipts) == 0 {
		os.Exit(3)
	}
	first := build(in.Receipts[0])
	receipts := []caseOut{}
	for _, c := range in.Receipts {
		r := build(c)
		row := caseOut{
			Key:         dsauthrecord.BattleResultReceiptKey(c.MatchID),
			Valid:       r.Valid(c.NowMs),
			SameAsFirst: r.SameCredential(first),
		}
		raw, err := dsauthrecord.MarshalBattleResultReceipt(r)
		if err != nil {
			row.MarshalErr = err.Error()
		} else {
			row.Marshaled = string(raw)
		}
		receipts = append(receipts, row)
	}
	decoded := []decodeOut{}
	for _, p := range in.Payloads {
		got, err := dsauthrecord.UnmarshalBattleResultReceipt([]byte(p))
		row := decodeOut{Receipt: got}
		if err != nil {
			row.Err = err.Error()
		}
		decoded = append(decoded, row)
	}
	json.NewEncoder(os.Stdout).Encode(map[string]any{
		"receipts": receipts,
		"decoded":  decoded,
	})
}
"""


def _py_receipt(case: dict) -> rec.BattleResultReceipt:
    return rec.new_battle_result_receipt(
        case["match_id"],
        case["allocation_id"],
        case["pod_name"],
        case["instance_uid"],
        case["instance_epoch"],
        case["gen"],
        case["jti"],
        case["exp_ms"],
        case["kid"],
        case["token_sha256"],
        case["writer_epoch"],
        case["recorded_at_ms"],
    )


def test_receipt_identical_to_go(repo_root: pathlib.Path) -> None:
    """★ 核心:key / Valid / SameCredential / 序列化字节必须与 Go 逐例相同。"""
    got = run_go_json(
        repo_root, _GO_PROGRAM, {"receipts": _CASES, "payloads": _PAYLOADS}
    )
    if got is None:
        pytest.skip("go 不在 PATH 上 —— 跨语言对拍跳过,不假装通过")

    first = _py_receipt(_CASES[0])
    assert len(got["receipts"]) == len(_CASES)
    for case, go_row in zip(_CASES, got["receipts"], strict=True):
        receipt = _py_receipt(case)
        label = {k: v for k, v in case.items() if v not in ("", 0)}
        assert rec.battle_result_receipt_key(case["match_id"]) == go_row["key"]
        assert receipt.valid(case["now_ms"]) is go_row["valid"], f"Valid 不一致:{label}"
        assert receipt.same_credential(first) is go_row["same_as_first"], (
            f"SameCredential 不一致:{label}"
        )
        if go_row["marshal_err"]:
            with pytest.raises(ValueError) as exc:
                rec.marshal_battle_result_receipt(receipt)
            assert str(exc.value) == go_row["marshal_err"], "marshal 错误文案漂移"
        else:
            payload = rec.marshal_battle_result_receipt(receipt)
            assert payload.decode("utf-8") == go_row["marshaled"], (
                f"★ 序列化字节与 Go 不一致(转义词表?字段顺序?):{label}\n"
                f"Go    ={go_row['marshaled']}\nPython={payload.decode('utf-8')}"
            )

    # 正负样本都得有,否则"全部报错"也能全绿
    assert any(r["marshaled"] for r in got["receipts"]), "没有一个用例序列化成功"
    assert any(r["marshal_err"] for r in got["receipts"]), "没有一个用例被 Valid 拒"
    assert any(r["valid"] for r in got["receipts"])
    assert any(not r["valid"] for r in got["receipts"])


def test_unmarshal_identical_to_go(repo_root: pathlib.Path) -> None:
    """★ 反序列化的接受/拒绝面必须与 Go 一致。

    Python 的 json.loads 什么都收,Go 的 json.Unmarshal 会因类型不符 / 数值越界报错。
    差额不补齐的话,一条被截断的脏记录会被解成"各字段零值"的 receipt 一路走下去。
    """
    got = run_go_json(
        repo_root, _GO_PROGRAM, {"receipts": _CASES, "payloads": _PAYLOADS}
    )
    if got is None:
        pytest.skip("go 不在 PATH 上 —— 跨语言对拍跳过,不假装通过")

    assert len(got["decoded"]) == len(_PAYLOADS)
    for payload, go_row in zip(_PAYLOADS, got["decoded"], strict=True):
        go_failed = bool(go_row["err"])
        try:
            receipt = rec.unmarshal_battle_result_receipt(payload.encode("utf-8"))
            py_failed = False
        except ValueError:
            receipt = None
            py_failed = True
        assert py_failed is go_failed, (
            f"接受/拒绝方向不一致:payload={payload!r} Go 错误={go_row['err']!r} "
            f"Python {'抛了' if py_failed else '没抛'}"
        )
        if go_failed:
            continue
        # Go 的 json tag 名与 Python 属性名一一对应,逐字段比。
        for name, json_name, _kind in rec._FIELDS:
            assert getattr(receipt, name) == go_row["receipt"][json_name], (
                f"字段 {name} 解出的值与 Go 不一致:payload={payload!r}"
            )

    assert any(r["err"] for r in got["decoded"]), "没有一个 payload 被 Go 拒 —— 负样本缺失"
    assert any(not r["err"] for r in got["decoded"]), "没有一个 payload 被 Go 接受"


def test_round_trip_and_credential_identity() -> None:
    """对应 Go 的 TestBattleResultReceiptRoundTripAndIdentity。"""
    r = rec.new_battle_result_receipt(
        9, "alloc", "battle-9", "uid", 3, 7, "jti", 2000, "kid", "hash", 2, 1000
    )
    raw = rec.marshal_battle_result_receipt(r)
    got = rec.unmarshal_battle_result_receipt(raw)
    assert got.valid(1500)
    assert got.same_credential(r)

    got.jti = "other"
    assert not got.same_credential(r)


def test_expiry_does_not_invalidate_receipt() -> None:
    """★ fail-open 方向必须与 Go 一致:token 过期不抹掉"结算已被接收"这个事实。

    加上 `exp_ms > now_ms` 的检查会让一局对局在 token TTL 之后永远无法完成终态释放。
    """
    r = rec.new_battle_result_receipt(
        9, "alloc", "battle-9", "uid", 3, 7, "jti", 2000, "kid", "hash", 2, 1000
    )
    assert r.valid(1500)
    assert r.valid(3000)  # exp_ms=2000 早过了,仍必须有效
    assert r.valid(10**15)


def test_recorded_at_ms_excluded_from_credential_identity() -> None:
    """★ SameCredential 刻意不比 recorded_at_ms。

    immediate receipt 可能在 DB commit 之后才写入,ds_allocator 会保留旧记录的
    真实 recorded_at(battle_auth.go:1829)。把它加进比较会让那条重入路径
    判成"属于另一个 proof",终态释放直接失败。
    """
    a = rec.new_battle_result_receipt(
        9, "alloc", "pod", "uid", 3, 7, "jti", 2000, "kid", "hash", 2, 1000
    )
    b = rec.new_battle_result_receipt(
        9, "alloc", "pod", "uid", 3, 7, "jti", 2000, "kid", "hash", 2, 1234
    )
    assert a.same_credential(b)
    # 但凭据本身的任何一个字段不同都必须判不同
    for field, value in (
        ("match_id", 10),
        ("allocation_id", "x"),
        ("pod_name", "x"),
        ("instance_uid", "x"),
        ("instance_epoch", 4),
        ("gen", 8),
        ("jti", "x"),
        ("exp_ms", 2001),
        ("kid", "x"),
        ("token_sha256", "x"),
        ("writer_epoch", 3),
        ("version", 2),
    ):
        other = rec.new_battle_result_receipt(
            9, "alloc", "pod", "uid", 3, 7, "jti", 2000, "kid", "hash", 2, 1000
        )
        setattr(other, field, value)
        assert not a.same_credential(other), f"{field} 变了却仍判同一凭据"


def test_key_carries_cluster_hash_tag() -> None:
    """花括号不是装饰,是 Redis Cluster 的 hash tag —— 去掉它上了 Cluster 才炸。"""
    assert rec.battle_result_receipt_key(9) == "pandora:ds:result-receipt:{9}"
    assert (
        rec.battle_result_receipt_key(18446744073709551615)
        == "pandora:ds:result-receipt:{18446744073709551615}"
    )
    with pytest.raises(ValueError, match="uint64"):
        rec.battle_result_receipt_key(-1)
    with pytest.raises(ValueError, match="uint64"):
        rec.battle_result_receipt_key(1 << 64)


def test_marshal_rejects_values_go_cannot_represent() -> None:
    """★ Python 特有的收紧:越界的数字写出去,Go 副本一条都读不了(overflows int64)。"""
    r = rec.new_battle_result_receipt(
        9, "alloc", "pod", "uid", 3, 7, "jti", 2000, "kid", "hash", 2, 1000
    )
    r.match_id = 1 << 64
    with pytest.raises(ValueError, match="uint64 range"):
        rec.marshal_battle_result_receipt(r)

    r = rec.new_battle_result_receipt(
        9, "alloc", "pod", "uid", 3, 7, "jti", 2000, "kid", "hash", 2, 1000
    )
    r.exp_ms = 1 << 63
    with pytest.raises(ValueError, match="int64 range"):
        rec.marshal_battle_result_receipt(r)


def test_field_order_is_go_declaration_order() -> None:
    """字段顺序 = Go struct 声明顺序,不是 dataclass 的巧合、也不是字典序。"""
    r = rec.new_battle_result_receipt(
        9, "alloc", "pod", "uid", 3, 7, "jti", 2000, "kid", "hash", 2, 1000
    )
    payload = rec.marshal_battle_result_receipt(r).decode("utf-8")
    assert payload == (
        '{"version":1,"match_id":9,"allocation_id":"alloc","pod_name":"pod",'
        '"instance_uid":"uid","instance_epoch":3,"gen":7,"jti":"jti","exp_ms":2000,'
        '"kid":"kid","token_sha256":"hash","writer_epoch":2,"recorded_at_ms":1000}'
    )
    assert " " not in payload.replace('"alloc"', "")  # 紧凑分隔符,无多余空格
