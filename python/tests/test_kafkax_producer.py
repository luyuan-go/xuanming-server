"""`kafkax.KeyOrderedProducer` 的行为契约 —— 逐条对齐 Go 的 producer.go。

这里钉的全是"改了不报错"的东西：

  - **kafka key 的格式**：`str(player_id)` 十进制，无补零无前缀。key 决定 partition，
    partition 内才有序 —— 格式变一个字符，同一玩家的事件被打散到多个 partition 后乱序，
    而生产与消费两侧都不会报错。任务域已经证过这条链：后环事实提前到达 = 静默永久丢失。
  - **`caller_player_id == 0` 表示不排除任何人**（推送原则 3 的例外，
    如 `pandora.match.progress` 的 stage 变化必须发给所有人含发起方）。
    写成"总是排除"会让某类推送稳定少发一个人，客户端只表现为"偶尔没收到"。
  - **event_type=0 时不写 header**：0 是各 topic 的旧事件兼容值，总是写 0 会给现网
    旧事件平白加 header。
  - **单个目标失败不中断整批**，末尾只汇总一条 WARN（hub 500 人广播时逐条打 = 500 行）。

用假 producer 注入，不连真 broker —— 这些契约与 broker 无关，靠真 Kafka 反而会
把它们淹没在网络噪音里。
"""

from __future__ import annotations

import pytest

from pandorapy import kafkax


class _FakeFuture:
    def __init__(self, exc: BaseException | None) -> None:
        self._exc = exc

    def get(self, timeout=None):  # noqa: ANN001, ARG002
        if self._exc is not None:
            raise self._exc
        return object()


class _FakeProducer:
    """记录每次 send 的完整参数；可按 player_id 编程失败。"""

    def __init__(self, fail_keys: set[str] | None = None) -> None:
        self.calls: list[dict] = []
        self.fail_keys = fail_keys or set()
        self.closed = False

    def send(self, topic, value=None, key=None, partition=None, headers=None):  # noqa: ANN001
        self.calls.append({
            "topic": topic, "value": value, "key": key,
            "partition": partition, "headers": headers,
        })
        k = key.decode() if isinstance(key, bytes) else key
        if k in self.fail_keys:
            return _FakeFuture(RuntimeError(f"broker refused {k}"))
        return _FakeFuture(None)

    def close(self):
        self.closed = True


def _make(topic="pandora.team.update", fail_keys=None, partition_cnt=4):
    fake = _FakeProducer(fail_keys)
    p = kafkax.KeyOrderedProducer(
        kafkax.ProducerConf(brokers=("x:9092",), partition_cnt=partition_cnt),
        topic,
        producer_factory=lambda: fake,
    )
    return p, fake


# ── key 格式 ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize(("pid", "want"), [(0, "0"), (1, "1"), (10086, "10086"),
                                           (2**63, str(2**63))])
def test_player_key_is_plain_decimal(pid: int, want: str) -> None:
    """对应 Go 的 strconv.FormatUint(pid, 10)。补零 / 十六进制 / 前缀都会改 partition。"""
    assert kafkax.player_key(pid) == want


async def test_send_uses_player_key_and_explicit_partition() -> None:
    p, fake = _make()
    await p.push_to_players(0, [10086], b"payload")
    call = fake.calls[-1]
    assert call["key"] == b"10086"
    # ★ partition 必须由我们的一致性环显式指定，不能交给客户端库自己算 ——
    # kafka-python 默认是 murmur2，与 Go 侧的环不同，交出去两栈当场分叉。
    want, ok = p._consistent.get_partition("10086")  # noqa: SLF001
    assert ok and call["partition"] == want


# ── 原则 2 与它的例外 ─────────────────────────────────────────────────────

async def test_caller_is_excluded_when_nonzero() -> None:
    """推送原则 2：不发给发起方。"""
    p, fake = _make()
    sent, err = await p.push_to_players(1001, [1001, 1002, 1003], b"x")
    assert err is None and sent == 2
    assert {c["key"] for c in fake.calls} == {b"1002", b"1003"}


async def test_caller_zero_excludes_nobody() -> None:
    """★ caller=0 = 全发（原则 3 例外，如 match.progress 的 stage 变化）。

    这条最容易被"顺手改成总是排除 caller"打掉，而后果是稳定少发一个人。
    """
    p, fake = _make()
    sent, err = await p.push_to_players(0, [1001, 1002], b"x")
    assert err is None and sent == 2
    assert {c["key"] for c in fake.calls} == {b"1001", b"1002"}


async def test_player_id_zero_in_list_is_not_special_cased() -> None:
    """名单里出现 player_id=0 且 caller=0 时会被跳过 —— 与 Go 的 `pid == caller` 一致。

    这不是 bug，是 Go 的同一行代码；写成测试是为了让下次有人"修"它之前先看到这句。
    """
    p, fake = _make()
    sent, _ = await p.push_to_players(0, [0, 1002], b"x")
    assert sent == 1
    assert {c["key"] for c in fake.calls} == {b"1002"}


# ── event_type header ────────────────────────────────────────────────────

async def test_event_type_zero_writes_no_header() -> None:
    p, fake = _make()
    await p.push_to_players(0, [1001], b"x", event_type=0)
    assert fake.calls[-1]["headers"] is None, "0 是旧事件兼容值，不该平白加 header"


async def test_event_type_nonzero_writes_the_go_header_name() -> None:
    p, fake = _make()
    await p.push_to_players(0, [1001], b"x", event_type=7)
    headers = fake.calls[-1]["headers"]
    assert headers == [("event_type", b"7")], (
        "header 名必须逐字是 event_type（Go: kafkax.HeaderEventType）—— "
        "改了名 consumer 读到 0，客户端会按该 topic 的旧事件去解析"
    )


# ── 部分失败 ─────────────────────────────────────────────────────────────

async def test_one_bad_target_does_not_abort_the_batch() -> None:
    """hub 广播 500 人时，一个坏目标不该让其余 499 人收不到。"""
    p, fake = _make(fail_keys={"1002"})
    sent, err = await p.push_to_players(0, [1001, 1002, 1003], b"x")
    assert sent == 2, "坏目标中断了整批"
    assert err is not None, "整批有失败却没把错误回给调用方"
    assert {c["key"] for c in fake.calls} == {b"1001", b"1002", b"1003"}


async def test_batch_failure_logs_once_not_per_target(logbuf) -> None:
    """逐条打日志的话，一次 broker 抖动 = 500 行。Go 是批尾汇总一条。"""
    p, _ = _make(fail_keys={"1002", "1003"})
    await p.push_to_players(0, [1001, 1002, 1003], b"x")
    hits = [r for r in logbuf if r.get("msg") == "push_to_players_send_failed"]
    assert len(hits) == 1, f"应当只汇总一条，实际 {len(hits)} 条"
    assert hits[0]["failed"] == 2 and hits[0]["sent"] == 1 and hits[0]["targets"] == 3


async def test_no_log_when_nothing_failed(logbuf) -> None:
    p, _ = _make()
    await p.push_to_players(0, [1001, 1002], b"x")
    assert not [r for r in logbuf if r.get("msg") == "push_to_players_send_failed"]


# ── 关闭后拒收 ───────────────────────────────────────────────────────────

async def test_closed_producer_refuses_new_sends() -> None:
    """关闭后仍接收 = 把发送误报成功。"""
    p, fake = _make()
    await p.close()
    assert fake.closed
    with pytest.raises(RuntimeError, match="closed"):
        await p.send_raw("1001", b"x")


@pytest.fixture
def logbuf():
    """把 structlog 输出接到内存并解析成 dict 列表（与 test_log_field_contract 同做法）。"""
    import io
    import json

    import structlog

    from pandorapy import log as plog

    buf = io.StringIO()
    plog.setup("test-kafkax")
    structlog.configure(
        processors=structlog.get_config()["processors"],
        wrapper_class=structlog.get_config()["wrapper_class"],
        logger_factory=structlog.PrintLoggerFactory(file=buf),
        cache_logger_on_first_use=False,
    )

    class _Lines(list):
        def __iter__(self):
            out = []
            for ln in buf.getvalue().splitlines():
                if ln.strip().startswith("{"):
                    try:
                        out.append(json.loads(ln))
                    except json.JSONDecodeError:
                        pass
            return iter(out)

    return _Lines()
