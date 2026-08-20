"""`kafkax.KeyOrderedConsumer` 的三条契约 —— 逐条对齐 Go 的 consumer.go。

每一条都是"写反了不报错"的：

  ① **毒丸绕过重试**：确定性错误重试多少次都一样。不绕过 = 该分区一直卡在同一条上，
     消费组看起来活着，只是 lag 一直涨。
  ② **handler 的任何异常都不许逃出消费循环**：逃出去 → 循环崩 → 重启重放同 offset →
     同异常 → 分区**永久卡死**（CrashLoop）。Go 靠 recover 把 panic 归一成毒丸；
     Python 分不出 panic 与"返回 error"，约定为**只有 PoisonError 跳过重试**，
     其余按可重试处理、耗尽再进 DLQ（两个方向各有一条用例守着）。
  ③ **DLQ 投递失败时不得 ack**。写反的后果是"处理不了、没留证、还告诉 broker 收到了"
     —— 事件静默消失。三档必须分清：没配 DLQ → 丢弃+ack（但要打 DROPPED）；
     DLQ 成功 → ack；DLQ 失败 → 不 ack。

用假 consumer / 假 DLQ 注入。这些契约与 broker 无关，接真 Kafka 只会把它们淹掉。
"""

from __future__ import annotations

import asyncio
import dataclasses
import io
import json

import pytest
import structlog

from pandorapy import kafkax
from pandorapy import log as plog


@dataclasses.dataclass
class _Msg:
    topic: str = "pandora.team.update"
    partition: int = 0
    offset: int = 0
    key: bytes = b"1001"
    value: bytes = b"payload"
    headers: list | None = None


class _FakeKafkaConsumer:
    """poll 一次返回预置批次，之后返回空。"""

    def __init__(self, batches: list[dict]) -> None:
        self._batches = list(batches)
        self.commits: list[dict] = []
        self.closed = False

    def poll(self, timeout_ms):  # noqa: ANN001, ARG002
        return self._batches.pop(0) if self._batches else {}

    def commit(self, offsets):  # noqa: ANN001
        self.commits.append(offsets)

    def close(self):
        self.closed = True


class _FakeDLQ:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.sent: list[tuple] = []

    async def send_raw_with_headers(self, key, payload, headers):  # noqa: ANN001
        if self.fail:
            raise RuntimeError("dlq broker down")
        self.sent.append((key, payload, headers))


@pytest.fixture
def logbuf():
    buf = io.StringIO()
    plog.setup("test-kafka-consumer")
    structlog.configure(
        processors=structlog.get_config()["processors"],
        wrapper_class=structlog.get_config()["wrapper_class"],
        logger_factory=structlog.PrintLoggerFactory(file=buf),
        cache_logger_on_first_use=False,
    )
    return buf


def _events(buf: io.StringIO) -> list[dict]:
    out = []
    for ln in buf.getvalue().splitlines():
        if ln.strip().startswith("{"):
            try:
                out.append(json.loads(ln))
            except json.JSONDecodeError:
                pass
    return out


def _msgs(buf: io.StringIO) -> set[str]:
    return {r.get("msg", "") for r in _events(buf)}


def _make(handler, *, dlq=None, retry=None, batches=None, disable_commit=False):
    fake = _FakeKafkaConsumer(batches if batches is not None else [{("tp", 0): [_Msg()]}])
    conf = kafkax.ConsumerConf(
        topic="pandora.team.update",
        group_id="g",
        retry=retry or kafkax.RetryPolicy(),
        disable_offset_commit=disable_commit,
        poll_timeout_ms=1,
    )
    c = kafkax.KeyOrderedConsumer(conf, handler, dlq=dlq, consumer_factory=lambda: fake)
    return c, fake


async def _run_once(c: kafkax.KeyOrderedConsumer, *, until) -> None:
    """跑到 `until()` 成立（或该批消息处理完）再 stop，让循环自然退出。

    ⚠️ 这里**不能**用固定 sleep（§5.3：固定 sleep 的并发测试全量跑时偶发红）。
    原来是 `sleep(0.05)` 后 stop —— 机器一忙（本仓常有并行 agent 在跑），
    `stop()` 会插进重试循环中间，而 `_process_message` 的重试前有一条
    `if self._stopped: return False`，于是重试被截断，用例变成
    「期望 3 次调用、实际 2 次」。红的是**测试的等待方式**，不是被测行为 ——
    这类假红最费人，因为它指向的是一个并不存在的 bug。

    `until` **必传**：曾经给过一个"预置批次已取空"的弱默认，但那个条件在 `poll()`
    返回的一瞬间就成立，**早于消息真正被处理完** —— 用它等价于没等。
    每个调用方都有自己的可观测终点（调用次数 / commits / dlq.sent / 日志事件），
    强制说清楚比给一个看起来省事的默认安全。
    """
    task = asyncio.create_task(c.run())
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 5
    cond = until
    try:
        while loop.time() < deadline:
            if cond():
                break
            await asyncio.sleep(0.005)
    finally:
        c.stop()
        await asyncio.wait_for(task, timeout=5)


# ── 正常路径 ─────────────────────────────────────────────────────────────

async def test_success_acks_and_commits_offset_plus_one() -> None:
    seen = []
    c, fake = _make(lambda m: seen.append(m.offset))
    await _run_once(c, until=lambda: bool(fake.commits))
    assert seen == [0]
    assert fake.commits, "成功处理却没提交 offset —— 重启会重放"
    committed = next(iter(fake.commits[0].values()))
    assert committed.offset == 1, "kafka 的提交语义是「下一条要读的 offset」"


async def test_disable_offset_commit_never_commits() -> None:
    """广播 per-Pod group：提交了 offset，同名 Pod 重启会整段重放积压广播。"""
    seen: list[int] = []
    c, fake = _make(lambda m: seen.append(m.offset), disable_commit=True)
    # 等「消息确实被处理过」再停 —— 否则可能在 poll 之前就 stop，
    # 那样 commits 为空是因为压根没跑，不是因为 disable_offset_commit 生效。
    await _run_once(c, until=lambda: bool(seen))
    assert fake.commits == []


# ── ① 毒丸绕过重试 ───────────────────────────────────────────────────────

async def test_poison_goes_straight_to_dlq_without_retrying(logbuf) -> None:
    attempts = []

    def handler(msg):
        attempts.append(1)
        raise kafkax.poison("decode failed")

    dlq = _FakeDLQ()
    c, fake = _make(handler, dlq=dlq, retry=kafkax.RetryPolicy(max_retries=3, backoff_sec=0.001))
    await _run_once(c, until=lambda: bool(dlq.sent))
    assert len(attempts) == 1, f"毒丸被重试了 {len(attempts)} 次 —— 确定性错误重试多少次都一样"
    assert len(dlq.sent) == 1
    assert fake.commits, "毒丸投 DLQ 成功后必须 ack，否则该分区永远卡在这条上"


# ── ② handler 异常归一化 ─────────────────────────────────────────────────

async def test_arbitrary_handler_exception_never_escapes_the_loop(logbuf) -> None:
    """★ 任其展开 = 重启重放同 offset = 同异常 = 分区永久卡死（CrashLoop）。

    与 Go 的语言性差异：Go 用 panic / 返回值区分"意外"与"可重试"，Python 两者都是
    抛异常，分不开。约定是**只有 PoisonError 跳过重试**，其余按可重试处理、
    重试耗尽再进 DLQ（见 _call_handler 的注释）。这里用零重试策略，所以一次就进 DLQ。
    """
    def handler(msg):
        raise ValueError("业务里的确定性 bug")

    dlq = _FakeDLQ()
    c, fake = _make(handler, dlq=dlq)
    await _run_once(c, until=lambda: bool(dlq.sent))   # 跑完不该抛出来
    assert len(dlq.sent) == 1, "异常没有最终进 DLQ"
    assert "kafka_handler_retries_exhausted" in _msgs(logbuf)
    assert fake.commits, "投 DLQ 后必须 ack 才能让分区前进"


async def test_non_poison_exception_is_retried_not_treated_as_poison() -> None:
    """★ 反向：普通异常**不能**被当毒丸。

    当毒丸的话下游抖一下就整条进 DLQ，瞬时故障永远没有自愈机会 ——
    这个代价比"确定性 bug 多跑几次重试"大得多。
    """
    calls = {"n": 0}

    def handler(msg):
        calls["n"] += 1
        raise RuntimeError("下游抖动")

    c, _fake = _make(handler, dlq=_FakeDLQ(),
                     retry=kafkax.RetryPolicy(max_retries=2, backoff_sec=0.001))
    await _run_once(c, until=lambda: calls["n"] >= 3)
    assert calls["n"] == 3, f"普通异常被当成毒丸跳过了重试（只调了 {calls['n']} 次）"


async def test_cancellation_propagates_so_shutdown_works() -> None:
    """CancelledError 必须穿透 —— 被当成业务错误吞掉的话停机时循环退不出去。"""
    async def handler(msg):
        raise asyncio.CancelledError

    c, _fake = _make(handler)
    task = asyncio.create_task(c.run())
    await asyncio.sleep(0.05)
    assert task.done() or task.cancelled(), "取消被吞了"
    with pytest.raises(asyncio.CancelledError):
        await task


# ── 瞬时错误重试 ─────────────────────────────────────────────────────────

async def test_transient_error_is_retried_then_succeeds(logbuf) -> None:
    calls = {"n": 0}

    def handler(msg):
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("下游抖动")

    c, fake = _make(handler, retry=kafkax.RetryPolicy(max_retries=3, backoff_sec=0.001))
    await _run_once(c, until=lambda: calls["n"] >= 3)
    assert calls["n"] == 3
    assert fake.commits, "重试成功后应当 ack"
    assert "kafka_handler_retry_failed" in _msgs(logbuf)


async def test_retries_exhausted_goes_to_dlq(logbuf) -> None:
    def handler(msg):
        raise RuntimeError("一直失败")

    dlq = _FakeDLQ()
    c, _fake = _make(handler, dlq=dlq, retry=kafkax.RetryPolicy(max_retries=2, backoff_sec=0.001))
    await _run_once(c, until=lambda: bool(dlq.sent))
    assert "kafka_handler_retries_exhausted" in _msgs(logbuf)
    assert len(dlq.sent) == 1


async def test_zero_retry_policy_means_no_retry() -> None:
    """RetryPolicy 零值 = 不重试（与 Go 的零值语义一致）。"""
    calls = {"n": 0}

    def handler(msg):
        calls["n"] += 1
        raise RuntimeError("x")

    dlq = _FakeDLQ()
    c, _fake = _make(handler, dlq=dlq)
    await _run_once(c, until=lambda: bool(dlq.sent))
    assert calls["n"] == 1


# ── ③ DLQ 三档 ───────────────────────────────────────────────────────────

async def test_no_dlq_drops_and_acks_but_says_so(logbuf) -> None:
    """没配 DLQ 时消息被丢弃并 ack —— 但必须打 DROPPED。

    上游日志写的是"→ DLQ"，运维照此去 DLQ 会白找；这条日志是丢弃事件唯一的痕迹。
    """
    c, fake = _make(lambda m: (_ for _ in ()).throw(kafkax.poison("bad")), dlq=None)
    await _run_once(c, until=lambda: bool(fake.commits))
    assert "kafka_message_dropped_no_dlq" in _msgs(logbuf)
    assert fake.commits, "loss-tolerant 消费者仍然要 ack，否则分区卡死"


async def test_dlq_send_failure_must_not_ack(logbuf) -> None:
    """★ 本文件最重要的一条。

    ack 了就是"处理不了、没留证、还告诉 broker 我收到了"—— 事件静默消失。
    把 _to_dlq 的失败分支改成 return True，这条当场红。
    """
    dlq = _FakeDLQ(fail=True)
    c, fake = _make(lambda m: (_ for _ in ()).throw(kafkax.poison("bad")), dlq=dlq)
    await _run_once(c, until=lambda: any("dlq_send_failed" in m for m in _msgs(logbuf)))
    assert fake.commits == [], "DLQ 投递失败却 ack 了 —— 这条消息就此静默消失"
    assert "kafka_dlq_send_failed_will_not_ack" in _msgs(logbuf)


async def test_failed_message_blocks_the_rest_of_the_partition_batch() -> None:
    """不 ack 的那条之后的消息本轮不能被处理，水位也不能推进。

    继续处理会跳过它 —— 而它正是"不可丢"的那一类。
    """
    handled = []

    def handler(msg):
        handled.append(msg.offset)
        if msg.offset == 1:
            raise kafkax.poison("bad")

    dlq = _FakeDLQ(fail=True)
    batch = {("tp", 0): [_Msg(offset=0), _Msg(offset=1), _Msg(offset=2)]}
    c, fake = _make(handler, dlq=dlq, batches=[batch])
    await _run_once(c, until=lambda: len(handled) >= 2)
    assert handled == [0, 1], f"越过了不可 ack 的那条：{handled}"
    committed = next(iter(fake.commits[0].values()))
    assert committed.offset == 1, "水位只能推到坏消息之前（offset 0 已成功 → 提交 1）"


async def test_dlq_preserves_original_headers_and_adds_provenance() -> None:
    """原 header 丢了，回放 DLQ 时消息会被当 legacy 解码；没有溯源就定位不到来源。"""
    dlq = _FakeDLQ()
    msg = _Msg(offset=42, partition=3, headers=[("event_type", b"7")])
    c, _fake = _make(
        lambda m: (_ for _ in ()).throw(kafkax.poison("bad")),
        dlq=dlq, batches=[{("tp", 3): [msg]}],
    )
    await _run_once(c, until=lambda: bool(dlq.sent))
    _key, _payload, headers = dlq.sent[0]
    as_dict = dict(headers)
    assert as_dict["event_type"] == b"7", "原 header 没保留 —— 回放会被当 legacy 解码"
    assert as_dict["dlq-src-topic"] == b"pandora.team.update"
    assert as_dict["dlq-src-partition"] == b"3"
    assert as_dict["dlq-src-offset"] == b"42"
