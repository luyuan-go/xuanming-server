"""dialogue 数据层 —— 对应 Go 侧 internal/data(session.go + tree.go)。

当前最小版本(与 Go 侧同一阶段):
  - 对话树:从配置表现取现组,内存只读(ConfigTreeProvider)
  - 会话:单实例内存会话(MemorySessionStore)

阶段限制(照抄 Go 侧的话,因为限制本身没变):
    内存会话不跨实例、进程重启即丢。多实例部署需把 SessionStore 换 Redis 版
    (biz / service 不动)。
"""

from __future__ import annotations

import dataclasses
import threading
import time
from typing import Protocol

from pandorapy import configtable as pct


def now_ms() -> int:
    """当前毫秒时间戳。对应 Go 的 nowMs()。"""
    return int(time.time() * 1000)


@dataclasses.dataclass(slots=True)
class Session:
    """一次 NPC 对话的服务端会话状态。

    dialogue_id 是服务端持有的会话 ID —— 不变量:由 snowflake 生成,客户端不可伪造。
    """

    dialogue_id: int
    player_id: int
    npc_id: int
    node_id: str  # 当前所在节点
    created_ms: int
    expires_ms: int  # 绝对过期时间戳(毫秒);超过即视为不存在


@dataclasses.dataclass(slots=True)
class DialogueOption:
    """对话节点上的一个选项(领域类型,非 proto)。"""

    option_id: str
    text: str
    visible: bool
    # next_node 选择后跳转的节点 ID;空或指向不存在的节点 = 结束对话。
    next_node: str


@dataclasses.dataclass(slots=True)
class DialogueNode:
    """对话树的一个节点。options 为空 = 终止节点。"""

    node_id: str
    # speaker 本节点说话人;空 = 沿用 DialogueTree.speaker。
    # 源表逐行有「说话人」列(分身 / 旁白可与 NPC 主名不同),这里如实承载,
    # 不在组树时压成一个值。
    speaker: str
    text: str
    options: list[DialogueOption]


@dataclasses.dataclass(slots=True)
class DialogueTree:
    """单个 NPC 的完整对话树。"""

    npc_id: int
    speaker: str
    start_node: str
    nodes: dict[str, DialogueNode]


class TreeProvider(Protocol):
    """按 npc_id 查对话树。对应 Go 的 data.TreeProvider。"""

    def get_tree(self, npc_id: int) -> DialogueTree | None: ...


class SessionStore(Protocol):
    """对话会话存储抽象。对应 Go 的 data.SessionStore。

    换 Redis 版只需换实现,biz / service 不动 —— 这个接缝是 Go 侧刻意留的,照搬。
    """

    def create(self, session: Session) -> bool: ...
    def get(self, dialogue_id: int, at_ms: int) -> Session | None: ...
    def update(self, session: Session) -> None: ...
    def delete(self, dialogue_id: int) -> None: ...


class MemorySessionStore:
    """进程内内存会话存储(惰性 + 主动过期回收)。对应 Go 的 MemorySessionStore。

    为什么用 threading.Lock 而不是 asyncio.Lock:
        所有操作都是纯内存字典读写,不 await。用线程锁可以同时保护 asyncio 路径和
        (未来可能的)线程池路径,而且不必把整条 biz 链路变成必须 await 的协程。
        Go 侧用的也是 sync.Mutex。
    """

    __slots__ = ("_lock", "_sessions")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sessions: dict[int, Session] = {}

    def create(self, session: Session) -> bool:
        """新建会话。dialogue_id 冲突返回 False(snowflake 唯一,几乎不会发生)。"""
        with self._lock:
            if session.dialogue_id in self._sessions:
                return False
            self._sessions[session.dialogue_id] = dataclasses.replace(session)
            return True

    def get(self, dialogue_id: int, at_ms: int) -> Session | None:
        """取会话;不存在或已过期返回 None(惰性过期:命中过期会话则删除)。"""
        with self._lock:
            session = self._sessions.get(dialogue_id)
            if session is None:
                return None
            if session.expires_ms > 0 and at_ms >= session.expires_ms:
                del self._sessions[dialogue_id]
                return None
            # 返回副本:调用方改了不影响存储,与 Go 侧 `cp := *s` 一致。
            return dataclasses.replace(session)

    def update(self, session: Session) -> None:
        """覆盖写已存在的会话(推进节点)。"""
        with self._lock:
            self._sessions[session.dialogue_id] = dataclasses.replace(session)

    def delete(self, dialogue_id: int) -> None:
        """删除会话(幂等)。"""
        with self._lock:
            self._sessions.pop(dialogue_id, None)

    def sweep_expired(self, at_ms: int) -> int:
        """主动清理已过期会话,返回清理数量。

        main 用一个周期任务调用它,避免被遗弃的会话(创建后不再访问)堆积 ——
        惰性过期只在被访问时才回收,遗弃的会话永远不会被访问。
        """
        with self._lock:
            expired = [
                sid
                for sid, s in self._sessions.items()
                if s.expires_ms > 0 and at_ms >= s.expires_ms
            ]
            for sid in expired:
                del self._sessions[sid]
            return len(expired)

    def __len__(self) -> int:
        with self._lock:
            return len(self._sessions)


def _node_key(node_id: int) -> str:
    """表内 uint32 节点 ID → 协议里的 node_id 字符串。

    对应 Go 的 dialogueNodeKey。协议里 DialogueState.node_id / ChooseOptionRequest.option_id
    都是 string(已上线字段,按 §9.21 不改类型),这里只做十进制文本化,
    不引入第二套 ID 语义。
    """
    return str(node_id)


class ConfigTreeProvider:
    """用配置表 dialogue 表实现 TreeProvider。对应 Go 的 dialogueTreesFromStore。

    每次 get_tree 现取当前批次并就地组树:表是整批不可变快照原子切换的,单次调用内
    不会跨版本;热更后下一次 get_tree 自然拿到新树,不需要重启,也不需要在本服再缓存
    一份(§9.22 不重复存储影子状态)。

    树的规模由源表行数决定(当前 4 行),组装成本可忽略;真涨到需要缓存时再按批次
    version 做记忆化,不预先复杂化(§15.3)。
    """

    __slots__ = ("_table",)

    def __init__(self, table: pct.DialogueTable) -> None:
        self._table = table

    def get_tree(self, npc_id: int) -> DialogueTree | None:
        rows = self._table.list_by_npc_id(npc_id)
        if not rows:
            return None
        start = self._table.start_node_of(npc_id)
        if start is None:
            return None

        nodes: dict[str, DialogueNode] = {}
        for row in rows:
            options: list[DialogueOption] = []
            for opt in pct.dialogue_options(row):
                options.append(
                    DialogueOption(
                        # option_id = 选项在源表里的列序号(1 基):随表稳定,
                        # 改文案不影响客户端回传。
                        option_id=str(opt.index),
                        text=opt.text,
                        # 表暂无「可见条件」列。可见性一旦要按玩家数据判定,是加表列 +
                        # 服务端判定,不是让客户端自己决定(§17.3),因此这里恒 True。
                        visible=True,
                        next_node=_node_key(opt.next_node_id) if opt.next_node_id else "",
                    )
                )
            key = _node_key(row.id)
            nodes[key] = DialogueNode(
                node_id=key,
                speaker=row.speaker,
                text=row.text,
                options=options,
            )

        return DialogueTree(
            npc_id=npc_id,
            speaker=start.speaker,
            start_node=_node_key(start.id),
            nodes=nodes,
        )
