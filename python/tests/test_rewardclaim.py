"""领奖位图 —— 与 Go 侧 pkg/rewardclaim 的**逐位**一致性。

★ 为什么字节序必须钉死:同一条 player_reward_claims.record 会被 Go 副本与 Python 副本
  交替读写(灰度期两个实现并存)。bit 布局若取反,另一个实现会把「已领」读成「未领」
  —— 玩家重复领奖,而两边都不报错。这里用**硬编码的期望字节**而不是"自己写自己读",
  后者对任何一致的错误布局都是绿的。
"""

from __future__ import annotations

import pytest

from pandorapy import rewardclaim


def test_bit_layout_is_little_endian_within_byte() -> None:
    """bit i 落在 bits[i>>3] 的第 (i&7) 位,低位在前 —— 与 Go 的 `1 << (i&7)` 同。"""
    rec = rewardclaim.Record()
    rec.claim_permanent("sign_in", 0)
    permanent, _ = rec.snapshot()
    assert permanent["sign_in"] == b"\x01"

    rec2 = rewardclaim.Record()
    rec2.claim_permanent("sign_in", 7)
    permanent2, _ = rec2.snapshot()
    assert permanent2["sign_in"] == b"\x80"

    rec3 = rewardclaim.Record()
    rec3.claim_permanent("sign_in", 8)
    permanent3, _ = rec3.snapshot()
    assert permanent3["sign_in"] == b"\x00\x01"


def test_snapshot_trims_trailing_zero_bytes() -> None:
    """落地最小化:尾部全零字节去掉,全空条目不出现在结果里(与 Go 的 trimmed 同)。"""
    rec = rewardclaim.Record()
    # 只碰一下位图(取/建)但不置位 —— 该条目不该被写进落地形态。
    rec.permanent_claimed_indices("never_claimed")
    rec.claim_permanent("sign_in", 1)
    permanent, activity = rec.snapshot()
    assert permanent == {"sign_in": b"\x02"}
    assert activity == {}


def test_round_trip_through_load() -> None:
    rec = rewardclaim.Record()
    rec.claim_permanent("sign_in", 3)
    rec.claim_permanent("sign_in", 40)
    rec.claim_activity(9001, 2)
    permanent, activity = rec.snapshot()

    restored = rewardclaim.load(permanent, activity)
    assert restored.permanent_claimed_indices("sign_in") == [3, 40]
    assert restored.activity_claimed_indices(9001) == [2]
    assert restored.is_permanent_claimed("sign_in", 3)
    assert not restored.is_permanent_claimed("sign_in", 4)


def test_load_does_not_share_buffer() -> None:
    """防御性拷贝:改回原 dict 的 bytes 不该影响已重建的 Record(Go 侧同)。"""
    raw = {"sign_in": bytes([0b0000_0001])}
    rec = rewardclaim.load(raw, None)
    rec.claim_permanent("sign_in", 1)
    assert raw["sign_in"] == b"\x01"


def test_already_claimed_is_idempotent_signal() -> None:
    rec = rewardclaim.Record()
    rec.claim_permanent("sign_in", 5)
    with pytest.raises(rewardclaim.AlreadyClaimedError):
        rec.claim_permanent("sign_in", 5)


def test_index_upper_bound() -> None:
    rec = rewardclaim.Record()
    with pytest.raises(rewardclaim.IndexTooLargeError):
        rec.claim_permanent("sign_in", rewardclaim.MAX_BIT_INDEX)
    # 上界前一位是合法的(边界不能少一位也不能多一位)。
    rec.claim_permanent("sign_in", rewardclaim.MAX_BIT_INDEX - 1)


def test_entry_limits_only_block_new_entries() -> None:
    """条目上限**只拦新增** —— 已存在的来源继续领取永不因上限被拒(否则是回档)。"""
    rec = rewardclaim.Record()
    for i in range(rewardclaim.MAX_PERMANENT_SOURCES):
        rec.claim_permanent(f"src_{i}", 0)
    with pytest.raises(rewardclaim.TooManyEntriesError):
        rec.claim_permanent("one_too_many", 0)
    # 已在册的来源不受影响。
    rec.claim_permanent("src_0", 1)
    assert rec.permanent_claimed_indices("src_0") == [0, 1]


def test_activity_entry_limit() -> None:
    rec = rewardclaim.Record()
    for i in range(rewardclaim.MAX_ACTIVITY_INSTANCES):
        rec.claim_activity(i + 1, 0)
    with pytest.raises(rewardclaim.TooManyEntriesError):
        rec.claim_activity(999_999, 0)


def test_source_name_length_is_measured_in_bytes() -> None:
    """按**字节**判长,与 Go 的 len(string) 同口径。

    按字符数判会让一个 64 字的中文来源名在 Python 侧通过、Go 侧拒掉 —— 同一条请求两个
    副本结论不同。
    """
    rec = rewardclaim.Record()
    long_ascii = "s" * (rewardclaim.MAX_SOURCE_NAME_LEN + 1)
    with pytest.raises(rewardclaim.SourceNameTooLongError):
        rec.claim_permanent(long_ascii, 0)

    # 22 个中文 = 66 字节 > 64,即便只有 22 个"字符"也必须拒。
    long_cjk = "签" * 22
    assert len(long_cjk) < rewardclaim.MAX_SOURCE_NAME_LEN
    with pytest.raises(rewardclaim.SourceNameTooLongError):
        rec.claim_permanent(long_cjk, 0)


def test_erase_activity_reclaims_whole_entry() -> None:
    rec = rewardclaim.Record()
    rec.claim_activity(7, 1)
    assert rec.has_activity(7)
    assert rec.erase_activity(7) is True
    assert rec.erase_activity(7) is False
    assert rec.activity_claimed_indices(7) == []
    # 下期新活动用新 ID 从零开始,即使复用相同档位 bit 也不串味。
    rec.claim_activity(8, 1)
    assert rec.activity_claimed_indices(8) == [1]


def test_retain_activities() -> None:
    rec = rewardclaim.Record()
    for i in (1, 2, 3):
        rec.claim_activity(i, 0)
    removed = rec.retain_activities({2})
    assert removed == 2
    assert sorted(rec.activity_ids()) == [2]
