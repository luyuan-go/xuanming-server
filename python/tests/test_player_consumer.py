"""player.update 消费 handler —— event_type header 的三档语义。

★ `pandora.player.update` 是**单事件类型 topic**(§21):旧 player 副本消费它时不看
  event_type header,直接按 PlayerUpdateEvent 解码。往它加新 event_type 会**静默污染 MMR**。
  本 handler 的三档判定就是这条不变量在消费侧的兑现:

    缺失 / "0"          → 旧事件,正常处理(兼容旧 producer 不写 header)
    合法非 0            → 未来事件,跳过并告警(**不得**按 MMR 误解码)
    存在但非法(非数字)→ **毒丸进 DLQ 留证**,绝不能降级当旧事件解码

★ 三条"跳过"分支都必须 ack(return),不能抛错:它们是确定性不可处理的消息,不 ack 会让
  该分区永久卡死(CrashLoop)。
"""

from __future__ import annotations

import dataclasses

import pytest

from pandora.player.v1 import player_pb2 as ppb

from pandorapy import kafkax
from pandorapy.services.player import consumer as pconsumer


@dataclasses.dataclass
class FakeMsg:
    value: bytes
    headers: list = dataclasses.field(default_factory=list)
    offset: int = 7
    key: bytes = b"1001"
    topic: str = "pandora.player.update"
    partition: int = 0


class FakeUC:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def update_mmr(self, player_id, delta, reason, key, pool):  # noqa: ANN001
        self.calls.append((player_id, delta, reason, key, pool))
        return 1515, False


def _event(**kw) -> bytes:
    base = {"player_id": 1001, "match_id": 42, "mmr_delta": 15, "reason": "win"}
    base.update(kw)
    return ppb.PlayerUpdateEvent(**base).SerializeToString()


@pytest.mark.asyncio
async def test_missing_header_is_treated_as_legacy_event() -> None:
    """兼容旧 producer 不写 header —— 这是滚动升级期的正常形态。"""
    uc = FakeUC()
    await pconsumer.player_update_handler(uc)(FakeMsg(value=_event()))
    assert uc.calls == [(1001, 15, "win", "42", "")]


@pytest.mark.asyncio
async def test_explicit_zero_header_is_legacy_event() -> None:
    uc = FakeUC()
    msg = FakeMsg(value=_event(), headers=[(kafkax.HEADER_EVENT_TYPE, b"0")])
    await pconsumer.player_update_handler(uc)(msg)
    assert len(uc.calls) == 1


@pytest.mark.asyncio
async def test_future_event_type_is_skipped_not_decoded() -> None:
    """★ 合法非 0 = 不属本消费者的未来事件。按 MMR 解码会污染段位。"""
    uc = FakeUC()
    msg = FakeMsg(value=b"\xff\xff", headers=[(kafkax.HEADER_EVENT_TYPE, b"1")])
    await pconsumer.player_update_handler(uc)(msg)  # 不抛错 = ack
    assert uc.calls == []


@pytest.mark.parametrize("raw", [b"abc", b"-1", b"", b"99999999999999999999"])
@pytest.mark.asyncio
async def test_malformed_header_is_poison(raw: bytes) -> None:
    """★ 非法 header **不得降级当旧事件解码** —— 那等于把来路不明的 payload 加进段位。"""
    uc = FakeUC()
    msg = FakeMsg(value=_event(), headers=[(kafkax.HEADER_EVENT_TYPE, raw)])
    with pytest.raises(kafkax.PoisonError):
        await pconsumer.player_update_handler(uc)(msg)
    assert uc.calls == []


@pytest.mark.asyncio
async def test_undecodable_payload_is_poison() -> None:
    uc = FakeUC()
    with pytest.raises(kafkax.PoisonError):
        await pconsumer.player_update_handler(uc)(FakeMsg(value=b"\xff\xfe\xfd\x01\x02"))


@pytest.mark.asyncio
async def test_missing_player_id_is_skipped() -> None:
    uc = FakeUC()
    await pconsumer.player_update_handler(uc)(FakeMsg(value=_event(player_id=0)))
    assert uc.calls == []


@pytest.mark.asyncio
async def test_missing_match_id_is_skipped() -> None:
    """幂等键缺失 → 无法保证不变量 §2,只能丢弃(battle_result 正常路径必带 match_id)。"""
    uc = FakeUC()
    await pconsumer.player_update_handler(uc)(FakeMsg(value=_event(match_id=0)))
    assert uc.calls == []


@pytest.mark.asyncio
async def test_rating_pool_is_forwarded_verbatim() -> None:
    """池由事件带来;空 = 旧 battle_result,由 update_mmr 归一(不在这里猜)。"""
    uc = FakeUC()
    await pconsumer.player_update_handler(uc)(
        FakeMsg(value=_event(rating_pool="3v3_ranked"))
    )
    assert uc.calls[0][4] == "3v3_ranked"


@pytest.mark.asyncio
async def test_business_error_propagates_for_retry() -> None:
    """业务瞬时错误必须**抛出去**让消费者重试;吞掉 = 静默丢 MMR 更新。"""

    class BoomUC:
        async def update_mmr(self, *_args):  # noqa: ANN002
            raise RuntimeError("db down")

    with pytest.raises(RuntimeError):
        await pconsumer.player_update_handler(BoomUC())(FakeMsg(value=_event()))
