"""Kafka topic 常量与 Go 的一致性 —— 生成物 `pandorapy/kafka_topics.py` 的门禁。

**为什么单独一个文件**：topic 名写错一个字符，producer 发到一个没有消费者的 topic，
**两侧都不报错** —— Kafka 自动建 topic、生产返回成功、消费端只是"没有消息"。
事实静默永久丢失，而两个进程的日志都干干净净。这与配置表 checksum、errcode 数值
是同一类"只能靠机械检查拦住"的东西（§7.4）。

本文件刻意**不走生成器**去核对，而是自己再解析一遍 `pkg/kafkax/topics.go`：
生成器本身写错了的话，只跑 `--check` 是发现不了的（它拿自己的输出跟自己比）。
两条独立路径都指向 Go 源码，才算真的钉住。
"""

from __future__ import annotations

import pathlib
import re

import pytest

from pandorapy import kafka_topics as kt

_CONST_RE = re.compile(r'^\s*(Topic\w+)\s*=\s*"([^"]+)"', re.MULTILINE)
_IDENT_RE = re.compile(r"\bTopic\w+\b")


@pytest.fixture(scope="module")
def go_topics(repo_root: pathlib.Path) -> dict[str, str]:
    text = (repo_root / "pkg" / "kafkax" / "topics.go").read_text(encoding="utf-8")
    consts = {m.group(1): m.group(2) for m in _CONST_RE.finditer(text)}
    assert consts, "没从 Go 侧解析到 Topic 常量 —— topics.go 结构变了，本测试要跟着改"
    return consts


def _py_name(go_name: str) -> str:
    body = go_name[len("Topic") :]
    body = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", body)
    body = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", body)
    return "TOPIC_" + body.upper()


def test_every_go_topic_exists_in_python_with_the_same_value(go_topics) -> None:
    """★ 逐个比字面量。少一个 = 那类事件在 Python 侧根本发不出去。"""
    missing, wrong = [], []
    for go_name, value in go_topics.items():
        py_name = _py_name(go_name)
        if not hasattr(kt, py_name):
            missing.append(f"{go_name} → 期望 {py_name}")
            continue
        got = getattr(kt, py_name)
        if got != value:
            wrong.append(f"{py_name}: Python={got!r} Go={value!r}")
    assert not missing, f"Python 侧缺这些 topic：{missing}"
    assert not wrong, f"topic 值与 Go 不一致：{wrong}"


def test_python_has_no_topic_that_go_does_not(go_topics) -> None:
    """反向也要查：多出来的 topic 意味着有人手加了一个 Go 侧没有的约会地点。"""
    expected = {_py_name(n) for n in go_topics}
    actual = {n for n in dir(kt) if n.startswith("TOPIC_")}
    assert actual - expected == set(), f"Python 多出来的 topic：{actual - expected}"


def test_push_subscription_set_matches_go(repo_root: pathlib.Path, go_topics) -> None:
    """push 的订阅集合漏一个 = 那一类推送整条失踪，客户端只是"没收到"。"""
    text = (repo_root / "pkg" / "kafkax" / "topics.go").read_text(encoding="utf-8")
    block = re.search(r"var PushTopics = \[\]string\{(.*?)\n\}", text, re.DOTALL)
    assert block, "Go 侧 PushTopics 结构变了"
    want = [go_topics[n] for n in _IDENT_RE.findall(block.group(1)) if n in go_topics]
    assert list(kt.PUSH_TOPICS) == want, "push 订阅集合与 Go 不一致（顺序也比，便于逐行对照）"


def test_broadcast_set_matches_go(repo_root: pathlib.Path, go_topics) -> None:
    """判错的后果不对称：广播当定向 → 空 key 解析失败被 ack 丢弃，全服公告静默不达。"""
    text = (repo_root / "pkg" / "kafkax" / "topics.go").read_text(encoding="utf-8")
    block = re.search(r"var BroadcastTopics = map\[string\]struct\{\}\{(.*?)\n\}", text, re.DOTALL)
    assert block, "Go 侧 BroadcastTopics 结构变了"
    want = {go_topics[n] for n in _IDENT_RE.findall(block.group(1)) if n in go_topics}
    assert set(kt.BROADCAST_TOPICS) == want
    for t in want:
        assert kt.is_broadcast_topic(t)
    assert not kt.is_broadcast_topic(kt.TOPIC_CHAT_PRIVATE), "定向 topic 被判成了广播"


@pytest.mark.parametrize(
    ("src", "want"),
    [
        ("pandora.battle.result", "pandora.dlq.battle.result"),
        ("pandora.chat.world", "pandora.dlq.chat.world"),
        # ★ 不带 pandora. 前缀时 Go 是**直接拼**而不是原样返回（config.go:543-549）。
        # 这一格最容易在移植时"顺手改进"成 early-return。
        ("weird-topic", "pandora.dlq.weird-topic"),
        ("pandora.", "pandora.dlq.pandora."),  # 长度不大于前缀 → 走 else 分支
    ],
)
def test_build_dlq_topic_matches_go_branch_for_branch(src: str, want: str) -> None:
    assert kt.build_dlq_topic(src) == want


def test_generator_check_is_green(repo_root: pathlib.Path) -> None:
    """生成物没被手改过 —— 手改能过 review，拦得住的只有会红的机械检查（§7.4）。"""
    import subprocess
    import sys

    gen = repo_root / "python" / "tools" / "gen_kafka_topics.py"
    proc = subprocess.run(
        [sys.executable, str(gen), "--check"],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    assert proc.returncode == 0, f"生成物与 Go 不一致：\n{proc.stdout}\n{proc.stderr}"
