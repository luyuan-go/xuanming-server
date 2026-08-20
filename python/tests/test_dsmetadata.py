"""dsmetadata 跨语言对拍 —— 对应 Go 侧 pkg/dsmetadata/roster.go。

守两件事:
  1. annotation 串逐字相同(它是 DS 侧对账与日志关联的键,两边写出不同的串 = 排障时对不上)
  2. 拒绝/接受的边界逐条相同(Python 更严会让某些对局只在 Python 副本上分配失败,
     更松会把 Go 拒绝的 roster 送进 DS)

用例由 Python 定义、经 stdin 喂给 Go,避免两边各写一份用例后各自漂移。
"""

from __future__ import annotations

import pathlib

import pytest

from goparity import go_const, run_go_json
from pandorapy import dsmetadata

# ── 对拍样本 ────────────────────────────────────────────────────────────────

_ROSTER_CASES: list[list[int]] = [
    [9, 2, 9, 1],  # Go 单测原样:去重 + 升序
    [1],
    [5, 5, 5, 5],  # 全重复 → 去重成 1 人
    [3, 2, 1],
    [18446744073709551615, 1],  # uint64 上界
    [],  # 空 → 拒
    [0, 1],  # 含 0 → 拒
    [1, 0],  # 0 不在首位也要拒(排序后才检查)
    list(range(1, dsmetadata.MAX_BATTLE_ROSTER_PLAYERS + 1)),  # 恰好 128 → 过
    list(range(1, dsmetadata.MAX_BATTLE_ROSTER_PLAYERS + 2)),  # 129 → 拒
    # ★ 上限判定在去重之后:130 个里 5 个重复 → 125 人,必须通过。
    #   若把长度检查提到去重之前,这一例会红。
    list(range(1, 126)) + [1, 2, 3, 4, 5],
]

_FACTION_CASES: list[dict] = [
    {"players": [99, 7, 42], "map": {"7": 3, "42": 3, "99": 9}},  # Go 单测原样
    {"players": [1, 2], "map": {"1": 0}},  # 少一个 → exactly cover
    {"players": [1], "map": {"1": 0, "2": 0}},  # 多一个 → exactly cover
    {"players": [1, 2], "map": {"1": 0, "3": 1}},  # 条数对但键错位 → missing
    {"players": [1], "map": {"1": dsmetadata.MAX_COMBAT_FACTION_ID}},  # 恰好上限 → 过
    {"players": [1], "map": {"1": dsmetadata.MAX_COMBAT_FACTION_ID + 1}},  # 越界 → 拒
    # 去重后条数变化:map 必须按**去重后**的人数给,不是按入参长度。
    {"players": [5, 5, 7], "map": {"5": 1, "7": 2}},
    {"players": [5, 5, 7], "map": {"5": 1, "7": 2, "9": 3}},  # 按入参长度给 → 拒
    {"players": [], "map": {}},  # 空 roster 的错误来自 CanonicalRoster
    {"players": [0, 1], "map": {"0": 1, "1": 2}},  # 含 0
]

_GO_PROGRAM = """package main

import (
	"encoding/json"
	"os"
	"strconv"

	"github.com/luyuancpp/pandora/pkg/dsmetadata"
)

type factionCase struct {
	Players []uint64          `json:"players"`
	Map     map[string]uint32 `json:"map"`
}

type input struct {
	Roster   [][]uint64    `json:"roster"`
	Factions []factionCase `json:"factions"`
}

type result struct {
	IDs []uint64 `json:"ids"`
	Ann string   `json:"ann"`
	Err string   `json:"err"`
}

func toResult(ids []uint64, ann string, err error) result {
	r := result{IDs: ids, Ann: ann}
	if r.IDs == nil {
		r.IDs = []uint64{}
	}
	if err != nil {
		r.Err = err.Error()
	}
	return r
}

func main() {
	var in input
	if err := json.NewDecoder(os.Stdin).Decode(&in); err != nil {
		os.Stderr.WriteString(err.Error())
		os.Exit(2)
	}
	roster := []result{}
	for _, players := range in.Roster {
		ids, ann, err := dsmetadata.CanonicalRoster(players)
		roster = append(roster, toResult(ids, ann, err))
	}
	factions := []result{}
	for _, c := range in.Factions {
		m := map[uint64]uint32{}
		for k, v := range c.Map {
			id, perr := strconv.ParseUint(k, 10, 64)
			if perr != nil {
				os.Stderr.WriteString(perr.Error())
				os.Exit(3)
			}
			m[id] = v
		}
		ids, ann, err := dsmetadata.CanonicalCombatFactions(c.Players, m)
		factions = append(factions, toResult(ids, ann, err))
	}
	json.NewEncoder(os.Stdout).Encode(map[string]any{
		"roster":            roster,
		"factions":          factions,
		"max_roster":        dsmetadata.MaxBattleRosterPlayers,
		"max_combat_faction": uint64(dsmetadata.MaxCombatFactionID),
	})
}
"""


def _py_roster(players: list[int]) -> dict:
    try:
        ids, ann = dsmetadata.canonical_roster(players)
    except ValueError as exc:
        return {"ids": [], "ann": "", "err": str(exc)}
    return {"ids": ids, "ann": ann, "err": ""}


def _py_factions(case: dict) -> dict:
    mapping = {int(k): v for k, v in case["map"].items()}
    try:
        ids, ann = dsmetadata.canonical_combat_factions(case["players"], mapping)
    except ValueError as exc:
        return {"ids": [], "ann": "", "err": str(exc)}
    return {"ids": ids, "ann": ann, "err": ""}


def test_roster_and_factions_identical_to_go(repo_root: pathlib.Path) -> None:
    """★ 核心:每个用例的 (ids, annotation, 是否报错) 必须与 Go 一致。

    错误**文案**也一并比对 —— Go 的三条错误串("exactly cover" / "missing" /
    "range")是 Go 单测直接断言的判据,文案漂移意味着两边拒绝的理由不是同一个。
    """
    got = run_go_json(
        repo_root, _GO_PROGRAM, {"roster": _ROSTER_CASES, "factions": _FACTION_CASES}
    )
    if got is None:
        pytest.skip("go 不在 PATH 上 —— 跨语言对拍跳过,不假装通过")

    assert len(got["roster"]) == len(_ROSTER_CASES)
    for case, go_row in zip(_ROSTER_CASES, got["roster"], strict=True):
        py_row = _py_roster(case)
        assert py_row["ids"] == go_row["ids"], f"roster ids 不一致,输入={case[:8]}..."
        assert py_row["ann"] == go_row["ann"], f"annotation 不一致,输入={case[:8]}..."
        assert bool(py_row["err"]) == bool(go_row["err"]), (
            f"接受/拒绝方向不一致,输入={case[:8]}... "
            f"Go={go_row['err']!r} Python={py_row['err']!r}"
        )
        if go_row["err"]:
            assert py_row["err"] == go_row["err"], "错误文案漂移(拒绝理由不是同一个)"

    assert len(got["factions"]) == len(_FACTION_CASES)
    for case, go_row in zip(_FACTION_CASES, got["factions"], strict=True):
        py_row = _py_factions(case)
        assert py_row["ids"] == go_row["ids"], f"faction ids 不一致,输入={case}"
        assert py_row["ann"] == go_row["ann"], f"faction annotation 不一致,输入={case}"
        assert bool(py_row["err"]) == bool(go_row["err"]), (
            f"接受/拒绝方向不一致,输入={case} "
            f"Go={go_row['err']!r} Python={py_row['err']!r}"
        )
        if go_row["err"]:
            assert py_row["err"] == go_row["err"], "错误文案漂移(拒绝理由不是同一个)"

    assert got["max_roster"] == dsmetadata.MAX_BATTLE_ROSTER_PLAYERS
    assert got["max_combat_faction"] == dsmetadata.MAX_COMBAT_FACTION_ID


def test_max_combat_faction_id_precedence_trap(repo_root: pathlib.Path) -> None:
    """★ Go 的 `1<<31 - 3` 直译到 Python 是 `1 << 28`,差了 8 倍。

    Go 里移位优先级**高于**减法,Python 里**低于**。这条用例同时守两件事:
      1. Go 源码的常量表达式没变(变了就该重新推导 Python 侧的值)
      2. Python 侧的值是按 Go 的优先级算的,不是按 Python 的
    """
    go_src = (repo_root / "pkg" / "dsmetadata" / "roster.go").read_text(encoding="utf-8")
    expr = go_const(go_src, "MaxCombatFactionID")
    assert expr == "1<<31 - 3", (
        f"Go 侧常量表达式变成了 {expr!r} —— 必须按 Go 的运算符优先级重新推导 Python 值"
    )
    assert dsmetadata.MAX_COMBAT_FACTION_ID == (1 << 31) - 3 == 2147483645
    # 反证:按 Python 的优先级直译会得到完全不同的数
    assert (1 << 31 - 3) == 268435456
    assert dsmetadata.MAX_COMBAT_FACTION_ID != (1 << 31 - 3)


def test_roster_is_sorted_not_insertion_ordered() -> None:
    """★ Python 的 list/dict "有序" 是插入序,不是排序。

    Go 那边是 sort.Slice,这里必须显式 sorted。不排的话同一批玩家会因为上游
    传入顺序不同而产出两个不同的 annotation。
    """
    ids_a, ann_a = dsmetadata.canonical_roster([9, 2, 1])
    ids_b, ann_b = dsmetadata.canonical_roster([1, 9, 2])
    assert ids_a == ids_b == [1, 2, 9]
    assert ann_a == ann_b == "1,2,9"


def test_faction_annotation_follows_roster_order_not_dict_order() -> None:
    """annotation 顺序来自 canonical roster,不是 dict 的插入序。"""
    _, ann = dsmetadata.canonical_combat_factions([99, 7, 42], {99: 9, 42: 3, 7: 3})
    assert ann == "7=3,42=3,99=9"


def test_rejects_values_go_cannot_represent() -> None:
    """★ Python 特有的收紧:Go 的 []uint64 / map[uint64]uint32 挡掉的东西必须显式挡。

    不挡的话负 player_id 会一路写进 annotation 变成 "-1",DS 解析出一个不存在的玩家。
    """
    with pytest.raises(ValueError, match="uint64"):
        dsmetadata.canonical_roster([-1, 2])
    with pytest.raises(ValueError, match="uint64"):
        dsmetadata.canonical_roster([1 << 64])
    with pytest.raises(ValueError, match="uint32"):
        dsmetadata.canonical_combat_factions([1], {1: -1})
    with pytest.raises(ValueError, match="uint32"):
        dsmetadata.canonical_combat_factions([1], {1: 1 << 32})


def test_never_truncates_oversized_roster() -> None:
    """超上限必须抛,绝不截断后继续。

    截断的后果是被砍掉的玩家永远进不了这局 DS,而分配"成功"了 ——
    客户端只会看到无限 loading(§9.20)。
    """
    too_many = list(range(1, dsmetadata.MAX_BATTLE_ROSTER_PLAYERS + 2))
    with pytest.raises(ValueError, match="exceeds 128 players"):
        dsmetadata.canonical_roster(too_many)
    exact = list(range(1, dsmetadata.MAX_BATTLE_ROSTER_PLAYERS + 1))
    ids, _ = dsmetadata.canonical_roster(exact)
    assert len(ids) == dsmetadata.MAX_BATTLE_ROSTER_PLAYERS
