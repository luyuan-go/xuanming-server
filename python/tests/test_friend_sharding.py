"""好友图分片落点 + 幂等键口径的逐条断言(对应 Go 的 friend_sharding_test.go)。

★ 这些全是纯函数,不需要 DB / Redis / router —— 分片判定错了会让「双向建边」
  变成「只建一条」,而且两侧都不报错,只能靠断言抓。
"""

from __future__ import annotations

import pytest

from pandorapy import cellroute
from pandorapy.services.friend import sharding as fshard


def _router(mapping: dict[int, tuple[int, int]], default: tuple[int, int]) -> cellroute.Router:
    """按 logical_cell → (region, cell) 铺一张全表,未列出的落 default。

    走 `build_static_table_from_raw` 而不是直接构造 `StaticTable`:后者要求调用方
    自己把 cell→region 的反查表算对,在测试里手算等于把被测的一致性校验绕过去。
    """
    raw = {
        lc: cellroute.encode_entry(
            cellroute.Entry(*mapping.get(lc, default))
        )
        for lc in range(cellroute.LOGICAL_CELL_COUNT)
    }
    return cellroute.Router(cellroute.build_static_table_from_raw(raw))


# ── 幂等键口径 ────────────────────────────────────────────────────────────────


def test_accept_idempotency_key():
    assert fshard.accept_idempotency_key(42) == "friend_accept:42"


def test_edge_build_key_differs_per_owner():
    """两条边必须拿到**不同**的键,否则后写的会被前一条的去重记录挡掉。"""
    a = fshard.edge_build_key(42, 100)
    b = fshard.edge_build_key(42, 200)
    assert a == "friend_accept:42:100"
    assert b == "friend_accept:42:200"
    assert a != b


def test_edge_build_key_extends_saga_key():
    """边键必须以 saga key 为前缀 —— 排查时能从一条 accept 正查到它的两条边。"""
    assert fshard.edge_build_key(42, 100).startswith(fshard.accept_idempotency_key(42))


# ── 落点判定 ──────────────────────────────────────────────────────────────────


def _owners(*rc: tuple[int, int]) -> list[fshard.EdgeOwner]:
    return [
        fshard.EdgeOwner(player_id=i + 1, region_id=r, cell_id=c) for i, (r, c) in enumerate(rc)
    ]


def test_distinct_edge_regions_sorted_and_deduped():
    got = fshard.distinct_edge_regions(_owners((2, 1), (1, 5), (2, 9)))
    assert got == [1, 2]


def test_distinct_edge_regions_empty():
    assert fshard.distinct_edge_regions([]) == []


def test_distinct_edge_cells_counts_region_cell_pairs():
    # (1,7) 与 (2,7) cell 号相同但 region 不同 → 是两个分片,不能只按 cell 去重。
    assert fshard.distinct_edge_cells(_owners((1, 7), (2, 7))) == 2
    assert fshard.distinct_edge_cells(_owners((1, 7), (1, 7))) == 1


def test_cross_shard_and_cross_region():
    same = _owners((1, 7), (1, 7))
    assert fshard.cross_shard_friendship(same) is False
    assert fshard.cross_region_friendship(same) is False

    diff_cell = _owners((1, 7), (1, 8))
    assert fshard.cross_shard_friendship(diff_cell) is True
    assert fshard.cross_region_friendship(diff_cell) is False

    diff_region = _owners((1, 7), (2, 8))
    assert fshard.cross_shard_friendship(diff_region) is True
    assert fshard.cross_region_friendship(diff_region) is True


def test_empty_owners_are_not_cross_anything():
    assert fshard.cross_shard_friendship([]) is False
    assert fshard.cross_region_friendship([]) is False


# ── edge_owners 的三条 None 分支 ──────────────────────────────────────────────


def test_edge_owners_without_router_is_none():
    assert fshard.edge_owners(None, 1, 2) is None


def test_edge_owners_rejects_zero_player():
    r = _router({}, (1, 7))
    assert fshard.edge_owners(r, 0, 2) is None
    assert fshard.edge_owners(r, 1, 0) is None


def test_edge_owners_resolves_both_players():
    r = _router({}, (3, 9))
    owners = fshard.edge_owners(r, 111, 222)
    assert owners is not None
    assert [o.player_id for o in owners] == [111, 222]
    assert all(o.region_id == 3 and o.cell_id == 9 for o in owners)


def test_edge_owners_is_deterministic():
    """同一对玩家两次解析必须完全一样 —— 否则跨分片判定会在两次运行里翻转。"""
    r = _router({0: (1, 1), 1: (2, 2)}, (9, 9))
    assert fshard.edge_owners(r, 4096, 4097) == fshard.edge_owners(r, 4096, 4097)


def test_edge_owners_picks_up_two_regions():
    # player 4096 → logical_cell 0 → (1,1);player 4097 → logical_cell 1 → (2,2)
    r = _router({0: (1, 1), 1: (2, 2)}, (9, 9))
    owners = fshard.edge_owners(r, 4096, 4097)
    assert owners is not None
    assert fshard.cross_region_friendship(owners) is True
    assert fshard.cross_shard_friendship(owners) is True


# ── 观测日志 ──────────────────────────────────────────────────────────────────


def test_log_friendship_sharding_without_router_is_noop():
    """router 未注入时整条不执行(单 Cell 行为不变)。不抛即通过。"""
    fshard.log_friendship_sharding(None, 42, 1, 2)


def test_log_friendship_sharding_same_cell(monkeypatch):
    calls = []
    monkeypatch.setattr(
        fshard.plog, "get", lambda: type("L", (), {"debug": lambda _s, e, **kw: calls.append((e, kw))})()
    )
    fshard.log_friendship_sharding(_router({}, (1, 7)), 42, 111, 222)
    assert len(calls) == 1
    event, kw = calls[0]
    assert event == "friend_edge_sharding"
    assert kw["cross_region"] is False
    assert kw["cross_shard"] is False
    assert kw["region_count"] == 1
    # 同 region 时**不带** sample_edge_key(与 Go 逐字一致)。
    assert "sample_edge_key" not in kw


def test_log_friendship_sharding_cross_region_carries_sample_key(monkeypatch):
    calls = []
    monkeypatch.setattr(
        fshard.plog, "get", lambda: type("L", (), {"debug": lambda _s, e, **kw: calls.append((e, kw))})()
    )
    r = _router({0: (1, 1), 1: (2, 2)}, (9, 9))
    fshard.log_friendship_sharding(r, 42, 4096, 4097)
    assert len(calls) == 1
    event, kw = calls[0]
    assert event == "friend_edge_sharding"
    assert kw["cross_region"] is True
    assert kw["cross_shard"] is True
    assert kw["region_count"] == 2
    # 排障锚点:必须是第一名玩家(requester)的边键,拼错就查不到。
    assert kw["sample_edge_key"] == "friend_accept:42:4096"


@pytest.mark.parametrize("pid", [0, 1, 2**63, 2**64 - 1])
def test_profile_like_ids_do_not_crash(pid):
    """极值 player_id 不得让落点解析抛出 —— 观测路径不能把一次成功的写变成错误。"""
    r = _router({}, (1, 7))
    fshard.edge_owners(r, pid, 1)
