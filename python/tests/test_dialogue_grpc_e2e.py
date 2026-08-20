"""端到端 gRPC 测试 —— 验证前面承诺的 4 个兼容点。

这些是"客户端零改动"这句话的实际内容。每一条如果不对,表现都是客户端行为异常而
服务端日志正常:

  1. grpc-status 映射  业务失败必须 **gRPC status = OK + body.code = ErrXxx**,
                       不能 abort。若改成 abort,UE 客户端会走到完全不同的错误分支。
  2. metadata 透传     player_id 只能来自 Envoy 注入的 x-pandora-player-id 头,
                       不能来自请求体(R5 已把那些字段 reserved)。
  3. grpc-timeout      客户端设的 deadline 要能被服务端看到(context.time_remaining)。
  4. 日志字段口径      见 test_log_field_contract.py。

用真实的 grpc.aio server + 真实的 stub 对打,不用 mock —— mock 验不了 wire 语义。
"""

from __future__ import annotations

import datetime as _dt
import pathlib

import grpc
import pytest
from pandora.common.v1 import errcode_pb2
from pandora.dialogue.v1 import dialogue_pb2, dialogue_pb2_grpc

from pandorapy import configtable as pct
from pandorapy import interceptors as pintercept
from pandorapy import server as pserver
from pandorapy.services.dialogue import biz as pbiz
from pandorapy.services.dialogue import data as pdata
from pandorapy.services.dialogue import service as psvc

PLAYER_HEADER = pintercept.METADATA_KEY_PLAYER_ID


class FakeSnowflake:
    def __init__(self, start: int = 5000) -> None:
        self._next = start

    def generate(self) -> int:
        value = self._next
        self._next += 1
        return value


@pytest.fixture
async def dialogue_channel(configtable_dist: pathlib.Path):
    """起一个真实的 grpc.aio server(随机端口)并返回连上去的 channel。"""
    result = pct.load_dialogue(configtable_dist)
    assert result.dialogue is not None
    usecase = pbiz.DialogueUsecase(
        pdata.ConfigTreeProvider(result.dialogue),
        pdata.MemorySessionStore(),
        _dt.timedelta(minutes=5),
    )
    service = psvc.DialogueService(usecase, FakeSnowflake())

    from pandorapy import config as pconfig

    server = pserver.build_grpc_server(
        pconfig.GrpcConf(addr="127.0.0.1:0"), auth_required=False
    )
    dialogue_pb2_grpc.add_DialogueServiceServicer_to_server(service, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    try:
        yield channel
    finally:
        await channel.close()
        await server.stop(grace=None)


# ── 兼容点 1:业务失败走 body.code,gRPC status 恒 OK ──────────────────────────


async def test_business_failure_keeps_grpc_status_ok(dialogue_channel) -> None:
    """★ 不存在的 NPC:gRPC 调用必须**成功返回**,错误在 body.code 里。

    这是 Go 侧的语义(service 层 `return &Response{Code: ...}, nil`)。
    若 Python 侧改用 context.abort,客户端收到的是 gRPC 错误而不是一个带 code 的响应,
    UE 侧的处理分支完全不同 —— 而服务端日志看起来一切正常。
    """
    stub = dialogue_pb2_grpc.DialogueServiceStub(dialogue_channel)
    call = stub.StartDialogue(
        dialogue_pb2.StartDialogueRequest(npc_id=999999),
        metadata=((PLAYER_HEADER, "1001"),),
    )
    response = await call
    # gRPC 层必须是 OK
    assert await call.code() == grpc.StatusCode.OK, (
        "业务失败被抬成了 gRPC 错误 —— 客户端会走错误分支,与 Go 版行为不一致"
    )
    # 业务错误在 body 里
    assert response.code == errcode_pb2.ERR_DIALOGUE_NOT_FOUND
    assert not response.HasField("state")


async def test_unauthenticated_returns_body_code_not_grpc_error(dialogue_channel) -> None:
    """不带 player_id 头 → body.code = ERR_UNAUTHORIZED,gRPC status 仍是 OK。

    dialogue 用的是 AuthOptional(Envoy 已在路由层 require JWT),所以拦截器不拒绝,
    由 service 层兜底成业务码。直连内网端口联调时就是这条路径。
    """
    stub = dialogue_pb2_grpc.DialogueServiceStub(dialogue_channel)
    call = stub.StartDialogue(dialogue_pb2.StartDialogueRequest(npc_id=1001))
    response = await call
    assert await call.code() == grpc.StatusCode.OK
    assert response.code == errcode_pb2.ERR_UNAUTHORIZED


# ── 兼容点 2:身份只认 Envoy 注入的头 ─────────────────────────────────────────


async def test_identity_comes_from_metadata_only(dialogue_channel) -> None:
    """带头 → 正常;不带头 → 未授权。身份的唯一来源是 metadata。"""
    stub = dialogue_pb2_grpc.DialogueServiceStub(dialogue_channel)
    ok = await stub.StartDialogue(
        dialogue_pb2.StartDialogueRequest(npc_id=1001),
        metadata=((PLAYER_HEADER, "1001"),),
    )
    assert ok.code == errcode_pb2.OK
    assert ok.state.speaker == "商店老板"


async def test_session_is_scoped_to_header_identity(dialogue_channel) -> None:
    """★ A 开的会话,B 拿着同一个 dialogue_id 必须访问不到。

    端到端复现 IDOR 防护:两次调用只有 metadata 里的 player_id 不同。
    """
    stub = dialogue_pb2_grpc.DialogueServiceStub(dialogue_channel)
    started = await stub.StartDialogue(
        dialogue_pb2.StartDialogueRequest(npc_id=1001),
        metadata=((PLAYER_HEADER, "1001"),),
    )
    dialogue_id = started.state.dialogue_id

    # 本人:能推进
    mine = await stub.ChooseOption(
        dialogue_pb2.ChooseOptionRequest(dialogue_id=dialogue_id, option_id="1"),
        metadata=((PLAYER_HEADER, "1001"),),
    )
    assert mine.code == errcode_pb2.OK

    # 他人:按不存在处理(与"真不存在"同码,不可区分)
    theirs = await stub.ChooseOption(
        dialogue_pb2.ChooseOptionRequest(dialogue_id=dialogue_id, option_id="1"),
        metadata=((PLAYER_HEADER, "2002"),),
    )
    assert theirs.code == errcode_pb2.ERR_DIALOGUE_NOT_FOUND

    nonexistent = await stub.ChooseOption(
        dialogue_pb2.ChooseOptionRequest(dialogue_id=12345678, option_id="1"),
        metadata=((PLAYER_HEADER, "2002"),),
    )
    assert nonexistent.code == theirs.code, "「他人会话」与「不存在」返回码不同,可被枚举"


async def test_malformed_player_header_is_anonymous(dialogue_channel) -> None:
    """非法的 player_id 头按匿名处理,不能崩也不能当成合法身份。"""
    stub = dialogue_pb2_grpc.DialogueServiceStub(dialogue_channel)
    for bad in ("", "abc", "-1", "0"):
        response = await stub.StartDialogue(
            dialogue_pb2.StartDialogueRequest(npc_id=1001),
            metadata=((PLAYER_HEADER, bad),),
        )
        assert response.code == errcode_pb2.ERR_UNAUTHORIZED, f"非法头 {bad!r} 被当成了合法身份"


# ── 兼容点 3:deadline 能被服务端看到 ─────────────────────────────────────────


async def test_client_deadline_is_visible_to_server(configtable_dist: pathlib.Path) -> None:
    """客户端设的 timeout 要能在服务端读到(对应 Go 的 ctx deadline)。

    Envoy 的 allow_headers 里有 grpc-timeout,客户端确实会设。若服务端读不到 deadline,
    就没法在长操作里主动放弃,超时会变成"客户端已断开但服务端还在跑"。
    """
    seen: dict[str, float | None] = {}

    class Probe(dialogue_pb2_grpc.DialogueServiceServicer):
        async def StartDialogue(self, request, context):  # noqa: N802
            seen["remaining"] = context.time_remaining()
            return dialogue_pb2.StartDialogueResponse(code=errcode_pb2.OK)

    from pandorapy import config as pconfig

    server = pserver.build_grpc_server(pconfig.GrpcConf())
    dialogue_pb2_grpc.add_DialogueServiceServicer_to_server(Probe(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = dialogue_pb2_grpc.DialogueServiceStub(channel)
            await stub.StartDialogue(
                dialogue_pb2.StartDialogueRequest(npc_id=1001),
                timeout=15.0,
                metadata=((PLAYER_HEADER, "1001"),),
            )
    finally:
        await server.stop(grace=None)

    remaining = seen.get("remaining")
    assert remaining is not None, "服务端读不到 deadline —— grpc-timeout 没透传"
    # 容差 1s:grpcio 的 deadline 由客户端设置时刻换算,和服务端读取时刻之间有
    # 时钟换算与舍入(实测会略微超过标称值,如 15.098)。这里要验的是"deadline 被
    # 透传且量级正确",不是精确值 —— 卡死在 <= 15.0 只会让测试随机变红。
    assert 14.0 < remaining < 16.0, f"deadline 量级不对: {remaining}"


# ── 完整对话流程(端到端) ──────────────────────────────────────────────────────


async def test_full_conversation_flow(dialogue_channel) -> None:
    """走一遍完整对话:开始 → 推进 → 到终止节点 → 显式结束(幂等)。"""
    stub = dialogue_pb2_grpc.DialogueServiceStub(dialogue_channel)
    md = ((PLAYER_HEADER, "1001"),)

    started = await stub.StartDialogue(dialogue_pb2.StartDialogueRequest(npc_id=1001), metadata=md)
    assert started.code == errcode_pb2.OK
    did = started.state.dialogue_id
    assert started.state.node_id == "10011"

    # 选项 2 → 节点 10013(终止节点,有文本无选项)
    advanced = await stub.ChooseOption(
        dialogue_pb2.ChooseOptionRequest(dialogue_id=did, option_id="2"), metadata=md
    )
    assert advanced.code == errcode_pb2.OK
    assert advanced.state.node_id == "10013"
    assert advanced.state.ended
    assert advanced.state.text

    # 会话已被回收,显式结束仍应成功(幂等)
    ended = await stub.EndDialogue(dialogue_pb2.EndDialogueRequest(dialogue_id=did), metadata=md)
    assert ended.code == errcode_pb2.OK


async def test_invalid_option_rejected(dialogue_channel) -> None:
    """回传不存在的 option_id → ERR_DIALOGUE_OPTION_INVALID。"""
    stub = dialogue_pb2_grpc.DialogueServiceStub(dialogue_channel)
    md = ((PLAYER_HEADER, "1001"),)
    started = await stub.StartDialogue(dialogue_pb2.StartDialogueRequest(npc_id=1001), metadata=md)
    bad = await stub.ChooseOption(
        dialogue_pb2.ChooseOptionRequest(
            dialogue_id=started.state.dialogue_id, option_id="99"
        ),
        metadata=md,
    )
    assert bad.code == errcode_pb2.ERR_DIALOGUE_OPTION_INVALID
