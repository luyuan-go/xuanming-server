"""chat 服务入口(main.py / service.py)的启动闸与装配契约测试。

覆盖的是"起不来 / 起错了 / 起来了但装错了"这一族缺陷 —— 它们全都**不会**在
biz 测试里露头:

  - 启动闸事件名 / 顺序与 Go 分叉:Loki 上按事件名建的告警静默失去覆盖
  - kafka / team / guild 被写成 fail-fast:broker 抖一下整个聊天服务起不来
  - 五 producer 部分成功:某几个频道能推、另几个静默不推,只有一条启动 WARN
  - 世界频道 kafka key 写成 player_id:广播退化成逐人定向,push 侧 Broadcast 不触发
  - GROUP 频道拨错地址:恒降级,消息静默不扇出且**不报错**

判据尽量**从 Go 源码里读**而不是抄一份常量:抄一份的话 Go 改了这个测试照样绿
(它验的是"我抄的值等于我抄的值")。
"""

from __future__ import annotations

import asyncio
import pathlib
import re

import pytest
from pandora.chat.v1 import chat_pb2
from pandora.common.v1 import errcode_pb2

from pandorapy import errcode
from pandorapy.services.chat import conf as cconf
from pandorapy.services.chat import main as cmain
from pandorapy.services.chat import readers as creaders
from pandorapy.services.chat import service as csvc

GO_MAIN = "services/social/chat/cmd/chat/main.go"
GO_CONF = "services/social/chat/internal/conf/conf.go"
GO_BUDGETS = "services/social/chat/internal/data/budgets.go"


def _py_main_src() -> str:
    return pathlib.Path(cmain.__file__).read_text(encoding="utf-8")


def _log_pos(src: str, level: str, event: str) -> int:
    """找 `logger.<level>("<event>"` 的位置,允许中间换行(长调用会被拆行)。

    ★ 用正则而不是子串匹配:写死 `logger.error("x"` 会因为格式化换行而**假绿** ——
    闸删掉了、测试照样过,正是这个测试要防的事。
    """
    m = re.search(rf'logger\.{level}\(\s*"{event}"', src)
    return -1 if m is None else m.start()


def _has_log(src: str, level: str, event: str) -> bool:
    return _log_pos(src, level, event) >= 0


# ── 启动闸:事件名与方向 ──────────────────────────────────────────────────────


def test_fail_fast_gates_are_present_and_in_go_order(repo_root: pathlib.Path) -> None:
    """★ 12 道 fail-fast 闸的事件名逐字相同,且相对顺序与 Go 一致。

    顺序本身是契约:运维手册按"卡在哪道闸"定位问题,顺序换了之后
    "看到 mysql_connected 就说明配置没问题"这类推理全部失效。
    """
    src = _py_main_src()
    go = (repo_root / GO_MAIN).read_text(encoding="utf-8")

    # 与 Go 逐字相同的事件名(Go 侧确实存在的那些)。
    for event in (
        "abs_conf_path_failed",
        "config_load_failed",
        "config_scan_failed",
        "chat_retention_mode_invalid",
        "mysql_dsn_required",
        "mysql_strict_mode_required",
        "cellroute_init_failed",
    ):
        assert f'"{event}"' in go, f"Go 侧已经没有 {event},Python 需要同步"
        assert _has_log(src, "error", event), f"Python 缺少 fail-fast 闸 {event}"

    # Go 侧是 panic(没有结构化事件名)的三处,Python 补了事件名 —— 方向相同:
    # 连不上 / 抢不到号 / 会话权威不可用一律不带着起来。
    for event in (
        "mysql_connect_failed",
        "snowflake_init_failed",
        "snowflake_nodeid_acquire_failed",
        "session_gate_redis_failed",
        "session_gate_endpoint_required",
    ):
        assert _has_log(src, "error", event), f"Python 缺少 fail-fast 闸 {event}"

    # 相对顺序(取 Go 与 Python 都有的那几道)。
    ordered = [
        "abs_conf_path_failed",
        "config_load_failed",
        "chat_retention_mode_invalid",
        "mysql_dsn_required",
        "mysql_connect_failed",
        "mysql_strict_mode_required",
        "snowflake_init_failed",
        "session_gate_endpoint_required",
    ]
    positions = [_log_pos(src, "error", e) for e in ordered]
    assert all(p >= 0 for p in positions), f"有闸不在 main.py 里:{ordered}"
    assert positions == sorted(positions), f"启动闸顺序与 Go 不一致:{ordered}"


@pytest.mark.parametrize(
    "event",
    [
        "kafka_brokers_empty",
        "team_addr_empty",
        "guild_addr_empty",
        "world_ratelimit_disabled",
        "kafka_producer_init_failed",
    ],
)
def test_weak_dependencies_only_warn(repo_root: pathlib.Path, event: str) -> None:
    """★ 弱依赖只 WARN,不许升级成 fail-fast。

    改成拒启的后果不是"更安全",而是把"聊天推送不可用"升级成"聊天服务起不来" ——
    而私聊照样落库、历史照样能拉,本来是可以继续服务的。
    """
    src = _py_main_src()
    assert _has_log(src, "warning", event), f"{event} 不再是 WARN"
    assert not _has_log(src, "error", event), f"{event} 被升级成了 fail-fast"
    # Go 侧同样是 Warnw(方向对齐的判据取自 Go 源码,不是我说了算)。
    go = (repo_root / GO_MAIN).read_text(encoding="utf-8")
    assert re.search(rf'Warnw\("msg", "{event}"', go), f"Go 侧 {event} 不再是 Warn"


# ── kafka:五 producer 全有或全无 + 世界频道 key 为空 ─────────────────────────


class _FakeProducer:
    """只记 (topic, key) 的假 producer。这组测试验的是**路由口径**,不是 kafka 行为。"""

    def __init__(self, topic: str, sink: list, fail: bool = False) -> None:
        if fail:
            raise RuntimeError("broker unreachable")
        self.topic = topic
        self.closed = False
        self._sink = sink

    async def send(self, key: str, msg) -> None:  # noqa: ANN001
        self._sink.append((self.topic, key))

    async def close(self) -> None:
        self.closed = True


def _cfg_with_kafka() -> cconf.Config:
    cfg = cconf.Config(kafka=cconf.KafkaConf(brokers=["127.0.0.1:9093"]))
    cfg.apply_defaults()
    return cfg


async def test_world_push_uses_empty_kafka_key() -> None:
    """★ 世界频道 key 必须为空(广播),其余四个频道 key = 收件方 player_id。

    写成 player_id 的后果:500 人在场的一条世界消息变成 500 条 kafka 写,
    而 push 侧的 Broadcast 分支根本不触发 —— 行为静默变形,没有任何报错。
    """
    sink: list[tuple[str, str]] = []
    pusher = cmain.ChatPusher(
        _FakeProducer("private", sink),
        _FakeProducer("team", sink),
        _FakeProducer("world", sink),
        _FakeProducer("guild", sink),
        _FakeProducer("group", sink),
    )
    evt = chat_pb2.ChatPushEvent()
    await pusher.push_world(evt)
    await pusher.push_private(1001, evt)
    await pusher.push_team(1002, evt)
    await pusher.push_guild(1003, evt)
    await pusher.push_group(1004, evt)
    assert sink == [
        ("world", ""),
        ("private", "1001"),
        ("team", "1002"),
        ("guild", "1003"),
        ("group", "1004"),
    ]


async def test_pusher_is_all_or_nothing(monkeypatch) -> None:  # noqa: ANN001
    """★ 任一 producer 建不起来就整体降级,并**关掉已经建好的那几个**。

    部分成功的后果:某几个频道能推、另几个静默不推,玩家看到的是"公会频道时灵
    时不灵",而服务侧只有一条早已被刷过去的启动 WARN,排查时根本对不上。
    漏关已建的那几个则是连接泄漏 —— 降级路径上没人再引用它们。
    """
    built: list[_FakeProducer] = []
    sink: list = []

    def _factory(conf, topic):  # noqa: ANN001, ARG001
        # 第 4 个(guild)失败:前 3 个必须被关掉。
        p = _FakeProducer(topic, sink, fail=len(built) == 3)
        built.append(p)
        return p

    monkeypatch.setattr(cmain.kafkax, "KeyOrderedProducer", _factory)

    class _Logger:
        def __init__(self) -> None:
            self.events: list[str] = []

        def warning(self, event: str, **kw) -> None:  # noqa: ANN003, ARG002
            self.events.append(event)

        def info(self, event: str, **kw) -> None:  # noqa: ANN003, ARG002
            self.events.append(event)

    logger = _Logger()
    pusher = await cmain._new_chat_pusher(_cfg_with_kafka(), logger)
    assert pusher is None, "部分成功被当成了可用的 pusher"
    assert "kafka_producer_init_failed" in logger.events
    assert all(p.closed for p in built), "已建好的 producer 没被关掉(连接泄漏)"


async def test_pusher_success_reports_all_five_topics(monkeypatch) -> None:  # noqa: ANN001
    """五个 topic 全建成时打一条 kafka_producer_ready,且顺序与 Go 逐字相同。"""
    from pandorapy import kafka_topics

    sink: list = []
    monkeypatch.setattr(
        cmain.kafkax, "KeyOrderedProducer", lambda conf, topic: _FakeProducer(topic, sink)
    )

    captured: dict = {}

    class _Logger:
        def warning(self, event: str, **kw) -> None:  # noqa: ANN003, ARG002
            raise AssertionError(f"不该有 WARN: {event}")

        def info(self, event: str, **kw) -> None:  # noqa: ANN003
            captured[event] = kw

    pusher = await cmain._new_chat_pusher(_cfg_with_kafka(), _Logger())
    assert pusher is not None
    assert captured["kafka_producer_ready"]["topics"] == [
        kafka_topics.TOPIC_CHAT_PRIVATE,
        kafka_topics.TOPIC_CHAT_TEAM,
        kafka_topics.TOPIC_CHAT_WORLD,
        kafka_topics.TOPIC_CHAT_GUILD,
        kafka_topics.TOPIC_CHAT_GROUP,
    ]


# ── GROUP 与 GUILD 共用同一个地址 ─────────────────────────────────────────────


def test_group_reader_dials_guild_addr(repo_root: pathlib.Path) -> None:
    """★ GroupReader 拨的是 guild_addr(GuildService 与 GroupService 同进程)。

    拨一个不存在的 group_addr 会让 GROUP 频道**恒走弱依赖降级** —— 合法档,
    不报错,消息静默不扇出。判据同时钉在 Go 源码上,免得两栈各改各的。
    """
    go = (repo_root / GO_MAIN).read_text(encoding="utf-8")
    assert "NewGrpcGroupReader(cfg.Chat.GuildAddr)" in go, "Go 侧不再共用 guild_addr"

    src = _py_main_src()
    assert "creaders.GrpcGroupReader(cfg.chat.guild_addr)" in src
    assert "group_addr" not in cconf.ChatConf.model_fields


# ── 成员解析:不可达 vs 不存在,两种结果不能混 ───────────────────────────────


class _FakeStub:
    def __init__(self, resp=None, exc: BaseException | None = None) -> None:  # noqa: ANN001
        self._resp = resp
        self._exc = exc

    async def _call(self, req, timeout=None):  # noqa: ANN001, ARG002
        if self._exc is not None:
            raise self._exc
        return self._resp

    GetTeam = _call
    ListMembers = _call
    ListGroupMembers = _call


async def test_reader_maps_non_ok_code_to_not_found() -> None:
    """服务答了但 code != OK → ([], False),由 biz 报 ErrChatChannelInvalid。"""
    from pandora.guild.v1 import guild_pb2

    reader = creaders.GrpcGuildReader("127.0.0.1:1")
    reader._stub = _FakeStub(
        guild_pb2.ListMembersResponse(code=errcode_pb2.ERR_GUILD_NOT_FOUND)
    )
    try:
        assert await reader.members(7001) == ([], False)
    finally:
        await reader.close()


async def test_reader_propagates_transport_error() -> None:
    """★ 服务不可达必须**抛出去**,不能压成 ([], False)。

    压成"不存在"的后果:玩家看到的是"你不在这个公会"而不是"稍后重试",
    而运维侧完全没有 chat 侧的不可达信号。
    """
    reader = creaders.GrpcGroupReader("127.0.0.1:1")
    reader._stub = _FakeStub(exc=RuntimeError("connection refused"))
    try:
        with pytest.raises(RuntimeError):
            await reader.members(8001)
    finally:
        await reader.close()


async def test_team_reader_treats_missing_team_as_not_found() -> None:
    """code=OK 但没带 team → ([], False),与 Go 的 `resp.GetTeam() == nil` 同。

    只判 code 的话,"OK 但没带 team"会被当成"队伍存在且零成员" ——
    发送者被判为非成员,报的却是"你不在这个队伍"。
    """
    from pandora.team.v1 import team_pb2

    reader = creaders.GrpcTeamReader("127.0.0.1:1")
    reader._stub = _FakeStub(team_pb2.GetTeamResponse(code=errcode_pb2.OK))
    try:
        assert await reader.members(9001) == ([], False)
    finally:
        await reader.close()


# ── conf:Go 自己就不统一的两个判据符号 ───────────────────────────────────────


def test_non_world_cooldown_default_uses_eq_zero(repo_root: pathlib.Path) -> None:
    """★ NonWorldCooldown 判 `== 0`(负值 = 显式关闭),WorldCooldown 判 `<= 0`。

    两处都写成 `<= 0` 的话,`non_world_cooldown: -1s`(显式关掉私聊冷却)
    在 Go 侧保持关闭、在 Python 侧被兜成 500ms —— 同一份 yaml 两个副本限流不同,
    而两边日志都显示"限流生效中"。
    """
    go = (repo_root / GO_CONF).read_text(encoding="utf-8")
    assert re.search(r"if c\.Chat\.NonWorldCooldown == 0 \{", go), "Go 侧判据变了"
    assert re.search(r"if c\.Chat\.WorldCooldown <= 0 \{", go), "Go 侧判据变了"

    cfg = cconf.Config(
        chat=cconf.ChatConf(non_world_cooldown="-1s", world_cooldown="-1s")
    )
    cfg.apply_defaults()
    assert cfg.chat.non_world_cooldown_td().total_seconds() == -1, "负值被默认值吃掉了"
    # world 侧相反:负值会被兜成 3s(Go 也是)。
    assert cfg.chat.world_cooldown_td().total_seconds() == 3


def test_capacity_budget_matches_go_source(repo_root: pathlib.Path) -> None:
    """容量预算与 Go 的 budgets.go 逐个相等(超限阈值只能有一个真值来源)。"""
    from pandorapy.services.chat import budgets as cbudgets

    go = (repo_root / GO_BUDGETS).read_text(encoding="utf-8")
    m = re.search(
        r'Table: "(\w+)", MaxRows: ([0-9_]+) \* ([0-9_]+) \* ([0-9_]+) \* ([0-9_]+),'
        r" MaxAvgRowBytes: ([0-9_]+)",
        go,
    )
    assert m, "没在 Go 源码里找到 chat 的 TableBudget"
    table, a, b, c, d, avg = m.groups()
    expect_rows = int(a.replace("_", "")) * int(b.replace("_", ""))
    expect_rows *= int(c.replace("_", "")) * int(d.replace("_", ""))

    got = cbudgets.budgets()
    assert len(got) == 1
    assert got[0].table == table
    assert got[0].max_rows == expect_rows
    assert got[0].max_avg_row_bytes == int(avg.replace("_", ""))


# ── service 层:无身份必须是 ERR_UNAUTHORIZED,且 gRPC status 保持 OK ─────────


class _FakeContext:
    """只提供 invocation_metadata 的假 ServicerContext。"""

    def __init__(self, player_id: int = 0) -> None:
        self._md = (
            (("x-pandora-player-id", str(player_id)),) if player_id else ()
        )

    def invocation_metadata(self):  # noqa: ANN201
        return self._md


class _FakeUsecase:
    def __init__(self, exc: BaseException | None = None) -> None:
        self.calls: list[tuple] = []
        self._exc = exc

    async def send_message(self, sender_id, channel, target_id, content, mid):  # noqa: ANN001
        if self._exc is not None:
            raise self._exc
        self.calls.append((sender_id, channel, target_id, content, mid))
        return mid

    async def pull_history(self, player_id, channel, peer_id, limit, before_ms):  # noqa: ANN001
        self.calls.append((player_id, channel, peer_id, limit, before_ms))
        return []


class _FakeSnowflake:
    def generate(self) -> int:
        return 424242


async def test_send_message_without_identity_is_unauthorized() -> None:
    """★ 无身份 → ERR_UNAUTHORIZED,而不是让 biz 报"参数错误"。

    少了这一条,任何没过 Envoy jwt_authn 的直连都能以 sender_id=0 走到 biz,
    客户端看到的是"参数不对"而不是"你没登录"。
    """
    uc = _FakeUsecase()
    svc = csvc.ChatService(uc, _FakeSnowflake())
    resp = await svc.SendMessage(chat_pb2.SendMessageRequest(), _FakeContext(0))
    assert resp.code == errcode_pb2.ERR_UNAUTHORIZED
    assert not uc.calls, "无身份的请求不该进 biz"

    resp = await svc.PullHistory(chat_pb2.PullHistoryRequest(), _FakeContext(0))
    assert resp.code == errcode_pb2.ERR_UNAUTHORIZED


async def test_sender_comes_from_auth_context_not_request() -> None:
    """★ 发送者取鉴权上下文(R5)。message_id 在进 biz 之前铸好并原样返回。"""
    uc = _FakeUsecase()
    svc = csvc.ChatService(uc, _FakeSnowflake())
    req = chat_pb2.SendMessageRequest(
        channel=chat_pb2.ChatChannel.CHAT_CHANNEL_WORLD, content="hi"
    )
    resp = await svc.SendMessage(req, _FakeContext(1001))
    assert resp.code == errcode_pb2.OK
    assert resp.message_id == 424242
    assert uc.calls[0][0] == 1001
    assert uc.calls[0][4] == 424242


async def test_business_failure_is_in_band_code_not_grpc_error() -> None:
    """★ 业务失败返回 in-band code,gRPC status 保持 OK。

    改成 abort() 会让客户端走到完全不同的错误分支(它读的是 body 里的 code)。
    """
    uc = _FakeUsecase(
        exc=errcode.PandoraError(errcode.ErrChatMessageTooLong, "too long")
    )
    svc = csvc.ChatService(uc, _FakeSnowflake())
    resp = await svc.SendMessage(
        chat_pb2.SendMessageRequest(content="x"), _FakeContext(1001)
    )
    assert resp.code == errcode_pb2.ERR_CHAT_MESSAGE_TOO_LONG
    assert resp.message_id == 0


async def test_cancellation_propagates_through_service() -> None:
    """★ CancelledError 必须穿透,不能被翻译成业务错误码。

    吞掉之后:优雅停机时客户端收到一批假的业务失败,而在途请求也没有真的排空。
    """
    uc = _FakeUsecase(exc=asyncio.CancelledError())
    svc = csvc.ChatService(uc, _FakeSnowflake())
    with pytest.raises(asyncio.CancelledError):
        await svc.SendMessage(
            chat_pb2.SendMessageRequest(content="x"), _FakeContext(1001)
        )
