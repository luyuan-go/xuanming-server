"""dialogue 业务逻辑测试 —— 覆盖 Go 侧 internal/biz 的全部分支与安全不变量。

输入刻意用**真实的 configtable/dist**,不造夹具:那批 json 的 checksum、行数、
起始节点唯一性是当前线上事实,用它做输入才能验证 Python 版看到的树和 Go 版一致。
"""

from __future__ import annotations

import datetime as _dt
import pathlib

import pytest

from pandorapy import configtable as pct
from pandorapy import errcode
from pandorapy.services.dialogue import biz as pbiz
from pandorapy.services.dialogue import data as pdata


class FakeSnowflake:
    """确定性发号器 —— 测试要能断言具体的 dialogue_id。"""

    def __init__(self, start: int = 1000) -> None:
        self._next = start

    def generate(self) -> int:
        value = self._next
        self._next += 1
        return value


@pytest.fixture
def tree_provider(configtable_dist: pathlib.Path) -> pdata.ConfigTreeProvider:
    result = pct.load_dialogue(configtable_dist)
    assert result.dialogue is not None
    return pdata.ConfigTreeProvider(result.dialogue)


@pytest.fixture
def usecase(tree_provider: pdata.ConfigTreeProvider) -> pbiz.DialogueUsecase:
    return pbiz.DialogueUsecase(
        tree_provider, pdata.MemorySessionStore(), _dt.timedelta(minutes=5)
    )


# ── 正常路径 ──────────────────────────────────────────────────────────────────


def test_start_dialogue_returns_start_node(usecase: pbiz.DialogueUsecase) -> None:
    """开对话应返回起始节点的文本 + 可见选项。

    真表事实(configtable/dist/dialogue.json):NPC 1001 起始节点 10011,
    说话人「商店老板」,3 个选项。
    """
    state = usecase.start_dialogue(player_id=1001, npc_id=1001, new_dialogue_id=9001)
    assert state.dialogue_id == 9001
    assert state.npc_id == 1001
    assert state.node_id == "10011"
    assert state.speaker == "商店老板"
    assert state.text
    assert not state.ended
    # 选项 id 是源表列序号(1 基),随表稳定,改文案不影响客户端回传。
    assert [o.option_id for o in state.options] == ["1", "2", "3"]
    assert all(o.visible for o in state.options)


def test_choose_option_advances_node(usecase: pbiz.DialogueUsecase) -> None:
    """选有后继的选项应推进到下一节点。选项 1 的后继是 10012。"""
    usecase.start_dialogue(1001, 1001, 9002)
    state = usecase.choose_option(1001, 9002, "1")
    assert state.node_id == "10012"
    assert not state.ended


def test_choose_option_without_next_ends_dialogue(usecase: pbiz.DialogueUsecase) -> None:
    """选无后继的选项 → 对话结束并回收会话。

    真表事实:节点 10011 的选项 3「没事,告辞」没有 option3_next → 结束。
    """
    usecase.start_dialogue(1001, 1001, 9003)
    state = usecase.choose_option(1001, 9003, "3")
    assert state.ended
    # 会话已回收:再选一次应当按"不存在"处理。
    with pytest.raises(errcode.PandoraError) as exc:
        usecase.choose_option(1001, 9003, "1")
    assert exc.value.code == errcode.ErrDialogueNotFound


def test_terminal_node_ends_dialogue(usecase: pbiz.DialogueUsecase) -> None:
    """跳到终止节点(无选项)→ 展示其文本后结束。

    真表事实:节点 10013「听说东边山洞里有宝藏」没有任何选项 = 终止节点。
    """
    usecase.start_dialogue(1001, 1001, 9004)
    state = usecase.choose_option(1001, 9004, "2")  # 选项 2 后继 10013
    assert state.node_id == "10013"
    assert state.ended
    assert not state.options
    assert state.text  # 终止节点仍要把文本给客户端展示


def test_start_node_that_is_terminal_ends_immediately(
    usecase: pbiz.DialogueUsecase,
) -> None:
    """起始节点即终止节点时,对话立即结束。

    真表事实:NPC 1002 守卫只有一行 10021,一个选项「抱歉,我这就走」且无后继。
    起始节点有可见选项 → 不立即结束;选了才结束。这里验的是"选完即结束"。
    """
    state = usecase.start_dialogue(1001, 1002, 9005)
    assert state.node_id == "10021"
    assert not state.ended  # 有一个可见选项
    ended = usecase.choose_option(1001, 9005, "1")
    assert ended.ended


def test_end_dialogue_is_idempotent(usecase: pbiz.DialogueUsecase) -> None:
    """结束对话幂等:会话不存在 / 已结束都不报错。"""
    usecase.start_dialogue(1001, 1001, 9006)
    usecase.end_dialogue(1001, 9006)
    usecase.end_dialogue(1001, 9006)  # 第二次不应抛
    usecase.end_dialogue(1001, 999999)  # 从未存在过也不应抛


# ── 安全不变量(R5)——这几条是 Go 侧注释里点名的,不能弱化 ──────────────────────


def test_cross_player_access_is_treated_as_not_found(
    usecase: pbiz.DialogueUsecase,
) -> None:
    """★ 别人的会话必须按「不存在」处理,不能泄露存在性。

    这是 IDOR 防护:攻击者猜 dialogue_id 时,"不属于你"和"不存在"必须返回同一个码,
    否则可以用返回码差异枚举出哪些 dialogue_id 是有效的。
    """
    usecase.start_dialogue(player_id=1001, npc_id=1001, new_dialogue_id=9007)
    with pytest.raises(errcode.PandoraError) as exc:
        usecase.choose_option(player_id=2002, dialogue_id=9007, option_id="1")
    assert exc.value.code == errcode.ErrDialogueNotFound, (
        "他人会话返回了与「不存在」不同的错误码 —— 可被用于枚举有效 dialogue_id"
    )


def test_end_dialogue_does_not_reap_others_session(
    usecase: pbiz.DialogueUsecase,
) -> None:
    """结束别人的会话必须无效果(且不报错)—— 否则可以踢掉任意玩家的对话。"""
    usecase.start_dialogue(player_id=1001, npc_id=1001, new_dialogue_id=9008)
    usecase.end_dialogue(player_id=2002, dialogue_id=9008)  # 幂等语义:不报错
    # 但会话必须还在 —— 原主人还能继续。
    state = usecase.choose_option(1001, 9008, "1")
    assert state.node_id == "10012"


def test_invisible_option_is_rejected(tree_provider: pdata.ConfigTreeProvider) -> None:
    """不可见选项即使客户端回传其 option_id 也必须拒绝。

    真表当前没有「可见条件」列(全部 visible=True),所以这里用一棵手工树覆盖该分支 ——
    这是 Go 侧 findVisibleOption 明确防的攻击面:客户端拿到过一次可见的 option_id 后,
    在条件不再满足时重放它。
    """
    tree = pdata.DialogueTree(
        npc_id=7777,
        speaker="测试NPC",
        start_node="1",
        nodes={
            "1": pdata.DialogueNode(
                node_id="1",
                speaker="",
                text="选一个",
                options=[
                    pdata.DialogueOption(option_id="1", text="可见", visible=True, next_node=""),
                    pdata.DialogueOption(option_id="2", text="隐藏", visible=False, next_node="1"),
                ],
            )
        },
    )

    class OneTree:
        def get_tree(self, npc_id: int) -> pdata.DialogueTree | None:
            return tree if npc_id == 7777 else None

    uc = pbiz.DialogueUsecase(OneTree(), pdata.MemorySessionStore())
    state = uc.start_dialogue(1001, 7777, 9100)
    # 不可见选项不下发给客户端
    assert [o.option_id for o in state.options] == ["1"]
    # 硬回传也要拒
    with pytest.raises(errcode.PandoraError) as exc:
        uc.choose_option(1001, 9100, "2")
    assert exc.value.code == errcode.ErrDialogueOptionInvalid


def test_dialogue_id_conflict_is_masked_as_not_found(
    tree_provider: pdata.ConfigTreeProvider,
) -> None:
    """dialogue_id 冲突(snowflake 重号)对客户端屏蔽为 NotFound。

    重号只可能由 snowflake node_id 配错(多副本同号)引起。对客户端不能暴露内部原因,
    但服务端要有 dialogue_id_conflict 这条 WARN 留证。
    """
    uc = pbiz.DialogueUsecase(tree_provider, pdata.MemorySessionStore())
    uc.start_dialogue(1001, 1001, 9200)
    with pytest.raises(errcode.PandoraError) as exc:
        uc.start_dialogue(1002, 1001, 9200)  # 同一个 dialogue_id
    assert exc.value.code == errcode.ErrDialogueNotFound


# ── 参数校验 ──────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("player_id", "npc_id", "dialogue_id"),
    [(0, 1001, 1), (1001, 0, 1), (1001, 1001, 0)],
)
def test_start_dialogue_rejects_zero_args(
    usecase: pbiz.DialogueUsecase, player_id: int, npc_id: int, dialogue_id: int
) -> None:
    with pytest.raises(errcode.PandoraError) as exc:
        usecase.start_dialogue(player_id, npc_id, dialogue_id)
    assert exc.value.code == errcode.ErrInvalidArg


def test_unknown_npc_returns_not_found(usecase: pbiz.DialogueUsecase) -> None:
    with pytest.raises(errcode.PandoraError) as exc:
        usecase.start_dialogue(1001, 999999, 9300)
    assert exc.value.code == errcode.ErrDialogueNotFound


# ── 会话过期 ──────────────────────────────────────────────────────────────────


def test_expired_session_is_gone(tree_provider: pdata.ConfigTreeProvider) -> None:
    """TTL 到期的会话按不存在处理(惰性过期)。"""
    store = pdata.MemorySessionStore()
    uc = pbiz.DialogueUsecase(tree_provider, store, _dt.timedelta(milliseconds=1))
    uc.start_dialogue(1001, 1001, 9400)
    import time

    time.sleep(0.01)
    with pytest.raises(errcode.PandoraError) as exc:
        uc.choose_option(1001, 9400, "1")
    assert exc.value.code == errcode.ErrDialogueNotFound


def test_sweep_reclaims_abandoned_sessions(
    tree_provider: pdata.ConfigTreeProvider,
) -> None:
    """主动清理必须能回收「创建后再也没被访问」的会话。

    惰性过期只在被访问时回收 —— 被遗弃的会话永远不会被访问,不靠 sweep 就会堆积到 OOM。
    """
    store = pdata.MemorySessionStore()
    uc = pbiz.DialogueUsecase(tree_provider, store, _dt.timedelta(milliseconds=1))
    for i in range(5):
        uc.start_dialogue(1001 + i, 1001, 9500 + i)
    assert len(store) == 5
    import time

    time.sleep(0.01)
    assert store.sweep_expired(pdata.now_ms()) == 5
    assert len(store) == 0


# ── 分片键口径 ────────────────────────────────────────────────────────────────


def test_session_shard_key_is_player_id_not_dialogue_id() -> None:
    """★ 会话分片键必须取 player_id,不能取 dialogue_id。

    dialogue_id 是 snowflake:全局唯一但与玩家落点无关。误用它分片会让会话与玩家其余
    owner 数据(档案 / 背包 / 段位 / 好友)落到不同 cell,于是 start → choose → end
    三步可能落不同 cell,会话读不回(scale-cellular-20m.md §4.2 owner 不变量)。
    """
    assert pbiz.session_shard_key(1001) == "1001"
    assert pbiz.session_shard_key(0) == "0"
