#!/usr/bin/env python3
"""从 Go 的 `pkg/kafkax/topics.go` 生成 `pandorapy/kafka_topics.py`。

**为什么必须生成而不是手抄**：topic 名是 producer 与 consumer 之间唯一的约会地点，
写错一个字符的后果是 —— 生产者发到一个没有消费者的 topic，**两侧都不报错**：
Kafka 会自动建 topic（`auto.create.topics.enable`），生产成功，消费端只是"没有消息"。
事实静默永久丢失，而日志里一行异常都没有。

这正是 2026-08-19 那批「已落码但是错的」的同一形状：跨语言手抄常量错位 +
测试抄了同一个错值。已经对拍过的纯函数零差异，没对拍的全错。所以这里不给手抄的机会。

同样必须机械对齐的还有两个集合：
  - `PushTopics`      —— push 服务默认订阅哪些；漏一个 = 那类推送整条失踪
  - `BroadcastTopics` —— 哪些 topic 的 kafka key 为空、必须走 Broadcast；
                         判错的后果是空 key 去 ParseUint 失败 → 消息被当 invalid ack 丢弃

用法（从任何 cwd 都能跑）：

    python tools/gen_kafka_topics.py           # 生成
    python tools/gen_kafka_topics.py --check   # CI 门禁：与 Go 不一致 → exit 1
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

_HERE = Path(__file__).resolve()
ROOT = _HERE.parents[2]
GO_TOPICS = ROOT / "pkg" / "kafkax" / "topics.go"
GO_CONFIG = ROOT / "pkg" / "config" / "config.go"
OUT = ROOT / "python" / "pandorapy" / "kafka_topics.py"

sys.path.insert(0, str(ROOT / "python"))
try:
    from pandorapy import _utf8  # noqa: F401  —— Windows cp1252 会把真实错误盖掉
except Exception:  # pragma: no cover
    pass


_CONST_RE = re.compile(r'^\s*(Topic\w+)\s*=\s*"([^"]+)"', re.MULTILINE)
_PUSH_RE = re.compile(r"var PushTopics = \[\]string\{(.*?)\n\}", re.DOTALL)
_BROADCAST_RE = re.compile(r"var BroadcastTopics = map\[string\]struct\{\}\{(.*?)\n\}", re.DOTALL)
_IDENT_RE = re.compile(r"\bTopic\w+\b")


def _go_name_to_py(go_name: str) -> str:
    """TopicChatPrivate → TOPIC_CHAT_PRIVATE。TopicDSLifecycle → TOPIC_DS_LIFECYCLE。"""
    body = go_name[len("Topic") :]
    # 先切连续大写（DSLifecycle → DS|Lifecycle），再切普通驼峰
    body = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", body)
    body = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", body)
    return "TOPIC_" + body.upper()


def parse_go() -> tuple[dict[str, str], list[str], list[str]]:
    text = GO_TOPICS.read_text(encoding="utf-8")
    consts = {m.group(1): m.group(2) for m in _CONST_RE.finditer(text)}
    if not consts:
        raise SystemExit(f"没从 {GO_TOPICS} 解析到任何 Topic 常量 —— 文件结构变了？")

    push_block = _PUSH_RE.search(text)
    if push_block is None:
        raise SystemExit("没找到 var PushTopics —— Go 侧结构变了，生成器要跟着改")
    push = [n for n in _IDENT_RE.findall(push_block.group(1)) if n in consts]

    bc_block = _BROADCAST_RE.search(text)
    if bc_block is None:
        raise SystemExit("没找到 var BroadcastTopics")
    broadcast = [n for n in _IDENT_RE.findall(bc_block.group(1)) if n in consts]

    return consts, push, broadcast


def _doc_for(go_name: str, text: str) -> str:
    """把 Go 常量上方那段注释原样搬过来 —— 里面写着 key 是什么、原则 2/3 的例外在哪。

    这些不是装饰：`match.progress` 必须发给发起方、`player.update` 永远只能承载一种
    event_type，都只写在注释里。丢了注释，下一个人就会按"通例"写出错的 producer。
    """
    lines = text.splitlines()
    idx = next((i for i, ln in enumerate(lines) if re.match(rf"\s*{go_name}\s*=", ln)), None)
    if idx is None:
        return ""
    out: list[str] = []
    for ln in reversed(lines[:idx]):
        s = ln.strip()
        if not s.startswith("//"):
            break
        out.append(s[2:].strip())
    return "\n".join(f"# {ln}" if ln else "#" for ln in reversed(out))


HEADER = '''"""Kafka topic 常量 —— **由 tools/gen_kafka_topics.py 从 pkg/kafkax/topics.go 生成，勿手改。**

改 topic 名请改 Go 侧那份，然后重跑生成器；CI 有 `--check` 门禁盯着两边一致。

为什么要机械同步：topic 名是 producer 与 consumer 唯一的约会地点，写错一个字符
**两侧都不报错**（Kafka 自动建 topic，生产成功，消费端只是"没有消息"）——
事实静默永久丢失，日志里一行异常都没有。
"""

from __future__ import annotations

'''

FOOTER = '''

def build_dlq_topic(original_topic: str) -> str:
    """构造死信队列 topic（infra.md §4.4）。对应 Go 的 kafkax.BuildDLQTopic。

        build_dlq_topic("pandora.battle.result") → "pandora.dlq.battle.result"

    注意 Go 侧的实现（config.go:543-549）对**不带** `pandora.` 前缀的输入是直接拼，
    不是原样返回；这里逐分支照搬，不"改进"。
    """
    prefix = "pandora."
    if len(original_topic) > len(prefix) and original_topic.startswith(prefix):
        return "pandora.dlq." + original_topic[len(prefix) :]
    return "pandora.dlq." + original_topic


def is_broadcast_topic(topic: str) -> bool:
    """是否广播类（kafka key 为空，消费侧必须走 Broadcast 而不是按 player_id 路由）。

    判错的后果不对称：把广播类当定向 → 空 key 解析 player_id 失败，消息被当
    invalid key ack 掉，**全服公告静默不达**；反过来则是把定向消息广播给所有人。
    """
    return topic in BROADCAST_TOPICS
'''


def render() -> str:
    text = GO_TOPICS.read_text(encoding="utf-8")
    consts, push, broadcast = parse_go()

    parts = [HEADER]
    for go_name, value in consts.items():
        doc = _doc_for(go_name, text)
        if doc:
            parts.append(doc + "\n")
        parts.append(f'{_go_name_to_py(go_name)} = "{value}"  # Go: kafkax.{go_name}\n\n')

    parts.append("# push 服务默认订阅的 topic 集合（Go: kafkax.PushTopics，顺序一并对齐）。\n")
    parts.append("PUSH_TOPICS: tuple[str, ...] = (\n")
    parts += [f"    {_go_name_to_py(n)},\n" for n in push]
    parts.append(")\n\n")

    parts.append(
        "# 广播类：kafka key 为空，消费侧必须 Broadcast（Go: kafkax.BroadcastTopics）。\n"
    )
    parts.append("BROADCAST_TOPICS: frozenset[str] = frozenset(\n    {\n")
    parts += [f"        {_go_name_to_py(n)},\n" for n in broadcast]
    parts.append("    }\n)\n")
    parts.append(FOOTER)
    return "".join(parts)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true", help="只校验不写文件（CI 用）")
    args = ap.parse_args()

    want = render()
    if not args.check:
        OUT.write_text(want, encoding="utf-8", newline="\n")
        consts, push, broadcast = parse_go()
        print(f"[OK ] 已生成 {OUT.relative_to(ROOT)}"
              f"（{len(consts)} 个 topic / push 订阅 {len(push)} / 广播 {len(broadcast)}）")
        return 0

    if not OUT.exists():
        print(f"[FAIL] {OUT.relative_to(ROOT)} 不存在，跑一次生成器", file=sys.stderr)
        return 1
    got = OUT.read_text(encoding="utf-8")
    if got != want:
        print("[FAIL] kafka topic 与 Go 侧不一致 —— 重跑 `python tools/gen_kafka_topics.py`",
              file=sys.stderr)
        # 只报差异的常量，别刷屏
        import difflib

        diff = list(difflib.unified_diff(got.splitlines(), want.splitlines(),
                                         "当前", "应有", lineterm="", n=1))
        for ln in diff[:40]:
            print("  " + ln, file=sys.stderr)
        return 1
    consts, push, broadcast = parse_go()
    print(f"[OK ] kafka topic 与 Go 侧一致（{len(consts)} 个 topic）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
