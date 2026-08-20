"""dialogue 业务逻辑层 —— 对应 Go 侧 internal/biz(dialogue.go + dialogue_sharding.go)。

职责(docs/design/go-services.md §2.10):NPC 对话树运行时。
  - start_dialogue:按 npc_id 取对话树,创建服务端会话,返回起始节点 DialogueState
  - choose_option:校验选项合法性 + 前置可见性,推进会话到下一节点
  - end_dialogue:结束并回收会话(幂等)

安全规则(逐条照抄 Go 侧,这些是不变量不是实现细节):
  - 对话树是服务端权威配置;客户端只渲染 DialogueState,选择只回传 option_id。
  - dialogue_id 由 snowflake 生成(服务端持有),会话归属用 player_id 校验
    (R5:player_id 来自 JWT),非本人会话一律按「不存在」处理,不泄露他人会话。
  - 选项可见性(DialogueOption.visible)在服务端判定;不可见选项即使客户端回传其
    option_id 也拒绝(ErrDialogueOptionInvalid)。

日志事件名(msg)必须与 Go 侧逐字一致 —— 它们是运维判据,进了
docs/ops/player-journey-log-map.md,改名等于让对应的 LogQL 查询静默失效:
    dialogue_id_conflict
    dialogue_cross_player_access
    dialogue_tree_missing_active_session
    dialogue_node_missing_active_session
    dialogue_session_placement
    dialogue_session_route_failed
"""

from __future__ import annotations

import datetime as _dt

from pandora.dialogue.v1 import dialogue_pb2

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy.services.dialogue import data as pdata

DEFAULT_SESSION_TTL = _dt.timedelta(minutes=5)


def session_shard_key(player_id: int) -> str:
    """dialogue 服务端会话的存储分片键口径(canonical)。

    对应 Go 的 biz.SessionShardKey。口径统一 = player_id 十进制串
    (owner cell 决定者,scale-cellular-20m.md §4.2 owner 不变量)。

    ⚠️ **不取 dialogue_id** —— 它是 snowflake,全局唯一但与玩家落点无关;误用它分片
    会让会话与玩家其余 owner 数据(档案 / 背包 / 段位 / 好友)落到不同 cell,
    于是 start → choose → end 三步可能落不同 cell,会话读不回。
    """
    return str(player_id)


class DialogueUsecase:
    """dialogue 业务逻辑核心。对应 Go 的 biz.DialogueUsecase。"""

    __slots__ = ("_trees", "_sessions", "_session_ttl_ms", "_router")

    def __init__(
        self,
        trees: pdata.TreeProvider,
        sessions: pdata.SessionStore,
        session_ttl: _dt.timedelta | None = None,
    ) -> None:
        ttl = session_ttl if session_ttl and session_ttl.total_seconds() > 0 else DEFAULT_SESSION_TTL
        self._trees = trees
        self._sessions = sessions
        self._session_ttl_ms = int(ttl.total_seconds() * 1000)
        # router 是确定性 region/cell 路由器(scale-cellular-20m.md §4.2)。
        # 可为 None:单 Cell / dev / 阶段 1~2 不分片,会话 owner 落点观测退化为不打日志。
        # 分片部署时由 main 经 set_cell_router 注入。None-safe。
        #
        # Python 侧本轮**未实现** cellroute(它依赖 etcd,而 Python 的 etcd 客户端生态
        # 是这次迁移唯一的高风险项 —— 最主流的 python-etcd3 已 20 个月未更新)。
        # 保留这个接缝和 None 分支,行为与 Go 侧单 Cell 完全一致。
        self._router = None

    def set_cell_router(self, router: object | None) -> None:
        """注入 region/cell 路由器。None-safe。对应 Go 的 SetCellRouter。

        用 setter 而不是构造参数,避免单 Cell 阶段调用点被迫改签名
        (与 matchmaker / auction / battle_result / friend / chat / trade 一致)。
        """
        self._router = router

    # ── RPC 对应的三个用例 ────────────────────────────────────────────────────

    def start_dialogue(
        self, player_id: int, npc_id: int, new_dialogue_id: int
    ) -> dialogue_pb2.DialogueState:
        """开启一次 NPC 对话。new_dialogue_id 由 service 层用 snowflake 预生成。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if npc_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "npc_id required")
        if new_dialogue_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "dialogue_id required")

        tree = self._trees.get_tree(npc_id)
        if tree is None:
            raise errcode.PandoraError(
                errcode.ErrDialogueNotFound, "no dialogue tree for npc %d", npc_id
            )
        node = tree.nodes.get(tree.start_node)
        if node is None:
            # 配置错误:起始节点不存在。
            raise errcode.PandoraError(
                errcode.ErrDialogueNotFound,
                "start node %r missing for npc %d",
                tree.start_node,
                npc_id,
            )

        now = pdata.now_ms()
        session = pdata.Session(
            dialogue_id=new_dialogue_id,
            player_id=player_id,
            npc_id=npc_id,
            node_id=tree.start_node,
            created_ms=now,
            expires_ms=now + self._session_ttl_ms,
        )
        if not self._sessions.create(session):
            # snowflake 预生成的 dialogue_id 已被占用 = 唯一键冲突,几乎只可能由
            # snowflake node_id 重号(多副本配置错)引起;对客户端屏蔽为 NotFound,
            # 服务端 WARN 暴露真实原因。
            plog.get().warning(
                "dialogue_id_conflict",
                dialogue_id=new_dialogue_id,
                player_id=player_id,
                npc_id=npc_id,
            )
            raise errcode.PandoraError(
                errcode.ErrDialogueNotFound,
                "dialogue_id %d already in use",
                new_dialogue_id,
            )

        # 分片:会话创建成功后观测本会话锁定的 owner 落点。router 为 None(单 Cell)→ 不打。
        self._log_session_placement(new_dialogue_id, player_id)

        state = _build_state(new_dialogue_id, tree, node)
        # 起始节点即终止节点(无可见选项)→ 对话立即结束,回收会话。
        if state.ended:
            self._sessions.delete(new_dialogue_id)
        return state

    def choose_option(
        self, player_id: int, dialogue_id: int, option_id: str
    ) -> dialogue_pb2.DialogueState:
        """选择一个选项,推进会话到下一节点。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if dialogue_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "dialogue_id required")
        if not option_id:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "option_id required")

        session = self._sessions.get(dialogue_id, pdata.now_ms())
        # R5:非本人会话按不存在处理,不泄露他人会话。
        if session is None or session.player_id != player_id:
            if session is not None and session.player_id != player_id:
                # 有人拿 dialogue_id 访问他人会话(IDOR / 越权探测);对客户端屏蔽为
                # NotFound,服务端 WARN 留证,否则无法发现有人在猜别人的 dialogue_id。
                plog.get().warning(
                    "dialogue_cross_player_access",
                    dialogue_id=dialogue_id,
                    caller_id=player_id,
                    owner_id=session.player_id,
                )
            raise errcode.PandoraError(
                errcode.ErrDialogueNotFound, "dialogue %d not found", dialogue_id
            )

        tree = self._trees.get_tree(session.npc_id)
        if tree is None:
            # 活跃会话的对话树消失 = 滚动更新换配置 / 配置漂移;
            # 对客户端是 NotFound,服务端 WARN 暴露内部一致性。
            plog.get().warning(
                "dialogue_tree_missing_active_session",
                dialogue_id=dialogue_id,
                npc_id=session.npc_id,
            )
            raise errcode.PandoraError(
                errcode.ErrDialogueNotFound,
                "no dialogue tree for npc %d",
                session.npc_id,
            )

        node = tree.nodes.get(session.node_id)
        if node is None:
            # 会话推进到不存在的节点 = 配置漂移或内部 bug,同上 WARN 暴露。
            plog.get().warning(
                "dialogue_node_missing_active_session",
                dialogue_id=dialogue_id,
                npc_id=session.npc_id,
                node_id=session.node_id,
            )
            raise errcode.PandoraError(
                errcode.ErrDialogueNotFound,
                "node %r missing for npc %d",
                session.node_id,
                session.npc_id,
            )

        # 选项必须存在且可见(不可见选项即使客户端回传也拒绝)。
        chosen = _find_visible_option(node, option_id)
        if chosen is None:
            raise errcode.PandoraError(
                errcode.ErrDialogueOptionInvalid,
                "option %r invalid at node %r",
                option_id,
                session.node_id,
            )

        nxt = tree.nodes.get(chosen.next_node) if chosen.next_node else None
        if not chosen.next_node or nxt is None:
            # 选项无后续节点 → 对话结束,回收会话。
            self._sessions.delete(dialogue_id)
            return _ended_state(dialogue_id, tree)

        session.node_id = chosen.next_node
        self._sessions.update(session)

        state = _build_state(dialogue_id, tree, nxt)
        # 跳转到的节点是终止节点 → 展示其文本后对话结束,回收会话。
        if state.ended:
            self._sessions.delete(dialogue_id)
        return state

    def end_dialogue(self, player_id: int, dialogue_id: int) -> None:
        """结束对话,回收会话(幂等:会话不存在 / 非本人均返回成功)。"""
        if player_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if dialogue_id == 0:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "dialogue_id required")

        session = self._sessions.get(dialogue_id, pdata.now_ms())
        # 仅回收本人会话;非本人 / 不存在不报错(幂等结束语义)。
        if session is not None and session.player_id == player_id:
            self._sessions.delete(dialogue_id)

    # ── 分片观测(对应 Go 的 dialogue_sharding.go)──────────────────────────────

    def _log_session_placement(self, dialogue_id: int, player_id: int) -> None:
        """把一次会话创建锚定的 owner 落点打成观测日志。

        仅可观测,不改会话路径。router 为 None(单 Cell)时直接返回,行为与 Go 侧一致。
        """
        if self._router is None or player_id == 0:
            return
        route = getattr(self._router, "route", None)
        location = route(player_id) if route is not None else None
        if location is None:
            # router 已注入且 player_id 非 0 时走到这里,只可能是路由解析失败
            # (路由表缺 player→region/cell 映射)。此前若静默吞错,分片部署下
            # 落点校验会变盲且查不到原因。
            plog.get().debug(
                "dialogue_session_route_failed",
                dialogue_id=dialogue_id,
                player_id=player_id,
                hint="cellroute 解析失败,会话落点观测降级",
            )
            return
        plog.get().debug(
            "dialogue_session_placement",
            dialogue_id=dialogue_id,
            player_id=player_id,
            region=location.region_id,
            cell=location.cell_id,
            shard_key=session_shard_key(player_id),
        )


# ── 辅助(对应 Go 侧同名函数)───────────────────────────────────────────────────


def _build_state(
    dialogue_id: int, tree: pdata.DialogueTree, node: pdata.DialogueNode
) -> dialogue_pb2.DialogueState:
    """把对话树节点渲染成客户端可见的 DialogueState。

    只输出可见选项;无可见选项即为终止节点(ended=True)。
    """
    options = [
        dialogue_pb2.DialogueOption(option_id=o.option_id, text=o.text, visible=True)
        for o in node.options
        if o.visible
    ]
    # 节点未单独指定说话人 → 用该 NPC 的主名。
    speaker = node.speaker or tree.speaker
    return dialogue_pb2.DialogueState(
        dialogue_id=dialogue_id,
        npc_id=tree.npc_id,
        node_id=node.node_id,
        speaker=speaker,
        text=node.text,
        options=options,
        ended=len(options) == 0,
    )


def _ended_state(dialogue_id: int, tree: pdata.DialogueTree) -> dialogue_pb2.DialogueState:
    """「选项无后续节点」时返回的结束态(无当前节点文本)。"""
    return dialogue_pb2.DialogueState(
        dialogue_id=dialogue_id,
        npc_id=tree.npc_id,
        speaker=tree.speaker,
        ended=True,
    )


def _find_visible_option(
    node: pdata.DialogueNode, option_id: str
) -> pdata.DialogueOption | None:
    """在节点里找可见且 id 匹配的选项;找不到返回 None。"""
    for opt in node.options:
        if opt.option_id == option_id and opt.visible:
            return opt
    return None
