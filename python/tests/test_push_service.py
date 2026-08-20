"""push 服务(Python 版)测试。

覆盖三类**判错了两边都不报错**的东西:

  A. 配置默认值与 Go 的 `conf.Defaults()` 逐字段对齐(直接读 Go 源码断言,漂移当场红)
  B. 三道会话闸的**方向**与**裁决顺序**(顺序写反 = INC-20260722-004 的互踢循环)
  C. kafka 消费侧的广播 / 定向分叉与毒丸判定(判错 = 全服公告静默不达)

★ 不打真 Redis / 真 kafka:这些用例测的是**判定逻辑**,注入假仓库就够。
  真 Lua 的语义由 tests/test_push_offline.py 打真 Redis 覆盖,两边不重复。
"""

from __future__ import annotations

import asyncio
import pathlib
import re

import pytest
from pandora.push.v1 import push_pb2

from pandorapy import errcode, kafka_topics, kafkax
from pandorapy.services.push import biz as pbiz
from pandorapy.services.push import conf as pconf
from pandorapy.services.push import connection as pconn
from pandorapy.services.push import consumer as pcons
from pandorapy.services.push import main as pmain
from pandorapy.services.push import offline as poff

PUSH_DEV_YAML = "services/runtime/push/etc/push-dev.yaml"
GO_CONF = "services/runtime/push/internal/conf/conf.go"
GO_BIZ = "services/runtime/push/internal/biz/push.go"


# ══════════════════════════════════════════════════════════════════════════
# A. 配置默认值 —— 直接对着 Go 源码断言
# ══════════════════════════════════════════════════════════════════════════


def _go_source(repo_root: pathlib.Path, rel: str) -> str:
    return (repo_root / rel).read_text(encoding="utf-8")


def test_defaults_match_go_source(repo_root: pathlib.Path) -> None:
    """默认值 + **判据符号**都对着 Go 的 Defaults() 断言。

    判据符号也测,是因为 `== 0` 与 `<= 0` 的分叉不会报错,只会让同一份 yaml
    在两个副本上表现不同(见 conf.py 头注释)。
    """
    src = _go_source(repo_root, GO_CONF)
    assert 'c.Server.Grpc.Addr = ":20014"' in src
    assert 'c.Server.Http.Addr = ":21014"' in src
    assert 'c.Kafka.GroupID = "pandora-push"' in src
    assert "c.Push.OfflineCacheMaxFrames = 512" in src
    assert "config.Duration(5 * time.Minute)" in src
    # ★ 判据符号:TTL 用 ==,MaxFrames 用 <=
    assert re.search(r"if c\.Push\.OfflineCacheTTL == 0", src)
    assert re.search(r"if c\.Push\.OfflineCacheMaxFrames <= 0", src)

    assert pconf.DEFAULT_GRPC_ADDR == ":20014"
    assert pconf.DEFAULT_HTTP_ADDR == ":21014"
    assert pconf.DEFAULT_KAFKA_GROUP_ID == "pandora-push"
    assert pconf.DEFAULT_OFFLINE_CACHE_MAX_FRAMES == 512
    assert int(pconf.DEFAULT_OFFLINE_CACHE_TTL.total_seconds()) == 300


def test_defaults_applied_to_empty_config() -> None:
    cfg = pconf.Config.model_validate({})
    cfg.apply_defaults()
    assert cfg.server.grpc.addr == pconf.DEFAULT_GRPC_ADDR
    assert cfg.server.http.addr == pconf.DEFAULT_HTTP_ADDR
    assert cfg.kafka.group_id == pconf.DEFAULT_KAFKA_GROUP_ID
    assert cfg.push.offline_cache_max_frames == 512
    assert cfg.push.offline_cache_ttl_sec() == 300
    # topics 留空 → 回落 PUSH_TOPICS(**覆盖**语义,不是追加)
    assert list(cfg.push.topics) == list(kafka_topics.PUSH_TOPICS)


def test_max_frames_negative_is_defaulted_but_ttl_symbol_is_eq_zero() -> None:
    """`<= 0` 那一格必须真的兜:-1 → 512。"""
    cfg = pconf.Config.model_validate({"push": {"offline_cache_max_frames": -1}})
    cfg.apply_defaults()
    assert cfg.push.offline_cache_max_frames == 512


def test_real_dev_yaml_loads(repo_root: pathlib.Path) -> None:
    """★ 必须能读**同一份** Go 在用的 yaml —— 迁移期两个实现共享一份配置。"""
    cfg = pconf.Config.load(str(repo_root / PUSH_DEV_YAML))
    assert cfg.server.grpc.addr == ":20014"
    assert cfg.server.http.addr == ":21014"
    assert cfg.kafka.group_id == "pandora-push"
    assert cfg.kafka.brokers == ["127.0.0.1:9093"]
    assert cfg.push.offline_cache_ttl_sec() == 300
    assert cfg.push.offline_cache_max_frames == 512  # yaml 没写,走默认
    assert cfg.push.require_session_gate is False    # dev 宽松档
    # 顺序不比:yaml 的排列是历史追加顺序,与常量表不同(Go 的 dev contract
    # test 同样按集合比)。集合一致性由下一条用例专门盯。
    assert set(cfg.push.topics) == set(kafka_topics.PUSH_TOPICS)


def test_dev_yaml_topics_match_push_topics_constant(repo_root: pathlib.Path) -> None:
    """yaml 的 topics 清单必须与 PUSH_TOPICS 集合一致(对齐 Go 的 dev contract test)。

    少一条 = 该业务事件在 dev 完全不消费(出箱堆积、客户端只能靠回源兜底),
    而生产侧一切正常。顺序不比对(yaml 的排列与常量表历史顺序不同,Go 那份
    contract test 也是按集合比)。
    """
    raw = (repo_root / PUSH_DEV_YAML).read_text(encoding="utf-8")
    listed = set(re.findall(r'^\s+- "(pandora\.[^"]+)"', raw, flags=re.M))
    assert listed == set(kafka_topics.PUSH_TOPICS)


# ══════════════════════════════════════════════════════════════════════════
# B. 会话闸
# ══════════════════════════════════════════════════════════════════════════


class FakeGate:
    """会话权威假实现。unavailable=True 时抛 ErrUnavailable(权威不可达)。"""

    def __init__(self, jti: str | None = "j1", unavailable: bool = False) -> None:
        self.jti = jti          # None = 无会话(已登出/过期)
        self.unavailable = unavailable
        self.calls = 0

    async def current_jti(self, player_id: int) -> tuple[str, bool]:
        self.calls += 1
        if self.unavailable:
            raise errcode.PandoraError(errcode.ErrUnavailable, "authority down")
        if self.jti is None:
            return "", False
        return self.jti, True


def _uc(gate=None, require: bool = False) -> pbiz.PushUsecase:  # noqa: ANN001
    uc = pbiz.PushUsecase(pconn.ConnectionManager(), None)
    uc.set_session_gate(gate, require)
    return uc


@pytest.mark.asyncio
async def test_authorize_dev_anonymous_allowed_but_rejected_in_require_mode() -> None:
    """player_id=0(dev 直连)在宽松档放行,在 require 档拒。

    方向写反的后果:生产档放行匿名 = 任何人都能建流收推送。
    """
    await _uc(FakeGate()).authorize_subscribe(0, pbiz.SessionInfo())
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(FakeGate(), require=True).authorize_subscribe(0, pbiz.SessionInfo())
    assert ei.value.code == errcode.ErrUnauthorized


@pytest.mark.asyncio
async def test_authorize_missing_jti_matrix() -> None:
    """无 jti:宽松档放行(dev 直连联调),require 档拒(绕网关)。"""
    await _uc(FakeGate()).authorize_subscribe(7, pbiz.SessionInfo(jti=""))
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(FakeGate(), require=True).authorize_subscribe(7, pbiz.SessionInfo(jti=""))
    assert ei.value.code == errcode.ErrUnauthorized


@pytest.mark.asyncio
async def test_authorize_gate_not_wired_fail_closed_only_in_require_mode() -> None:
    await _uc(None).authorize_subscribe(7, pbiz.SessionInfo(jti="j1"))
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(None, require=True).authorize_subscribe(7, pbiz.SessionInfo(jti="j1"))
    assert ei.value.code == errcode.ErrUnavailable


@pytest.mark.asyncio
async def test_authorize_superseded_uses_dedicated_code() -> None:
    """★ 顶号必须是 ErrSessionSuperseded(→ABORTED),不能是 ErrUnauthorized。

    写成 UNAUTHENTICATED 的话被顶设备会把它当自然过期,用缓存凭据自动完整
    Login 轮换 jti **反顶**新设备 —— 互踢循环。
    """
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(FakeGate(jti="new")).authorize_subscribe(7, pbiz.SessionInfo(jti="old"))
    assert ei.value.code == errcode.ErrSessionSuperseded


@pytest.mark.asyncio
async def test_authorize_no_session_and_authority_down() -> None:
    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(FakeGate(jti=None)).authorize_subscribe(7, pbiz.SessionInfo(jti="j1"))
    assert ei.value.code == errcode.ErrUnauthorized  # 登出/过期,允许自动换新

    with pytest.raises(errcode.PandoraError) as ei:
        await _uc(FakeGate(unavailable=True)).authorize_subscribe(7, pbiz.SessionInfo(jti="j1"))
    assert ei.value.code == errcode.ErrUnavailable   # 查不了 ≠ 仍现行(§9.22)


@pytest.mark.asyncio
async def test_recheck_decides_generation_before_expiry() -> None:
    """★ R5 复审 P0-2:「已过期**且**已被顶」必须回 ErrSessionSuperseded。

    先判 exp 的旧顺序只会得到 ErrUnauthorized —— 到期分支把错误码 14 的互踢
    防护整条短路掉。这条用例专门钉死这个顺序。
    """
    uc = _uc(FakeGate(jti="new"))
    sess = pbiz.SessionInfo(jti="old", exp_ms=1)  # 早已过期 + 已被顶
    retryable, err = await uc.recheck_session(7, sess)
    assert retryable is False
    assert isinstance(err, errcode.PandoraError)
    assert err.code == errcode.ErrSessionSuperseded


@pytest.mark.asyncio
async def test_recheck_expired_alone_is_plain_unauthorized() -> None:
    """jti 仍是当前一代、只是到期:普通未授权(客户端自动换新不构成反顶)。"""
    uc = _uc(FakeGate(jti="j1"))
    retryable, err = await uc.recheck_session(7, pbiz.SessionInfo(jti="j1", exp_ms=1))
    assert retryable is False
    assert err.code == errcode.ErrUnauthorized


@pytest.mark.asyncio
async def test_recheck_authority_down_is_retryable() -> None:
    """权威不可达 = 可重试(计连败),不是立即关流 —— 短抖动不该踢光全服长连。"""
    uc = _uc(FakeGate(unavailable=True))
    retryable, err = await uc.recheck_session(7, pbiz.SessionInfo(jti="j1"))
    assert retryable is True
    assert err is not None


def test_session_fail_close_and_intervals_match_go(repo_root: pathlib.Path) -> None:
    """看门狗常量与 Go 逐值对齐(改一个就改变了对外承诺的暴露窗口)。"""
    src = _go_source(repo_root, GO_BIZ)
    assert "pollFallbackInterval = 30 * time.Second" in src
    assert "sessionRecheckInterval = 30 * time.Second" in src
    assert "sessionFailClose = 3" in src
    assert "drainBackoffMax = time.Minute" in src
    assert "authRegStripes = 64" in src
    assert 'ResyncTopic = "pandora.push.resync"' in src

    assert pbiz.POLL_FALLBACK_SEC == 30.0
    assert pbiz.SESSION_RECHECK_SEC == 30.0
    assert pbiz.SESSION_FAIL_CLOSE == 3
    assert pbiz.DRAIN_BACKOFF_MAX_SEC == 60.0
    assert pbiz.AUTH_REG_STRIPES == 64
    assert pbiz.RESYNC_TOPIC == "pandora.push.resync"


def test_drain_backoff_curve() -> None:
    """1s,2s,4s...封顶 60s(Go 的 shift>6 钳位)。"""
    assert [pbiz.drain_backoff_sec(i) for i in range(1, 8)] == [1, 2, 4, 8, 16, 32, 60]
    assert pbiz.drain_backoff_sec(99) == 60


# ══════════════════════════════════════════════════════════════════════════
# B2. drain_buffer:gap 预检 / 逐帧 fence
# ══════════════════════════════════════════════════════════════════════════


class FakeOffline:
    """投递缓冲假实现。pages 是每次 range_after 依次返回的页。"""

    def __init__(self, pages: list[list[int]], lost: int = 0) -> None:
        self._pages = list(pages)
        self.lost = lost

    async def range_after(self, player_id: int, after_cursor: int, now_ms: int, max_frames: int = 0):  # noqa: ANN001
        if not self._pages:
            return []
        cursors = self._pages.pop(0)
        return [
            poff.OfflineFrame(frame=push_pb2.PushFrame(topic="t", ts_ms=c), cursor=c)
            for c in cursors
        ]

    async def lost_since(self, player_id: int, after_cursor: int, now_ms: int) -> int:
        return self.lost if self.lost > after_cursor else 0


class FakeSlot:
    """只记录被写出去的帧。"""

    def __init__(self) -> None:
        self.sent: list = []
        self.closed = asyncio.Event()

    async def write(self, frame) -> None:  # noqa: ANN001
        self.sent.append(frame)


@pytest.mark.asyncio
async def test_drain_delivers_all_pages_and_advances_cursor() -> None:
    uc = pbiz.PushUsecase(pconn.ConnectionManager(), FakeOffline([[10, 20], [30]]))
    slot = FakeSlot()
    cursor, err = await uc.drain_buffer(slot, 7, 0, pbiz.SessionInfo())
    assert err is None
    assert cursor == 30
    assert [f.ts_ms for f in slot.sent] == [10, 20, 30]


@pytest.mark.asyncio
async def test_drain_signals_resync_before_frames_that_cross_the_gap() -> None:
    """★ resync 必须**先于**越过缺口的帧发出(Go R7 复审 P1-1)。

    只在拉空后终检的话,多页补推期间客户端游标已被幸存帧推过缺口;
    resync 发出前断流重连 → 新流 last_seen_ms 已越过缺口 → 该缺口永远不会
    再被信号(permanent miss)。这条用例断言「第二页之前先来一条 resync」。
    """
    offline = FakeOffline([[10], [20]], lost=15)
    uc = pbiz.PushUsecase(pconn.ConnectionManager(), offline)
    slot = FakeSlot()
    cursor, err = await uc.drain_buffer(slot, 7, 0, pbiz.SessionInfo())
    assert err is None
    topics = [f.topic for f in slot.sent]
    # 首页(baseline=0)不预检;第二页之前检出 lost=15 > baseline=10 → 先发 resync。
    assert topics == ["t", pbiz.RESYNC_TOPIC, "t"]
    # resync 帧不推进客户端游标:ts_ms 必须是 0。
    assert slot.sent[1].ts_ms == 0
    # 游标最终停在最后一条交付帧(20 > lost_bound 15)。
    assert cursor == 20


@pytest.mark.asyncio
async def test_drain_same_gap_is_signaled_only_once() -> None:
    """同一段丢失只信号一次:baseline 跳到丢失上界后终检不再重复发。"""
    offline = FakeOffline([[10], [20]], lost=15)
    uc = pbiz.PushUsecase(pconn.ConnectionManager(), offline)
    slot = FakeSlot()
    await uc.drain_buffer(slot, 7, 0, pbiz.SessionInfo())
    assert [f.topic for f in slot.sent].count(pbiz.RESYNC_TOPIC) == 1


@pytest.mark.asyncio
async def test_drain_lost_bound_advances_cursor_past_the_gap() -> None:
    """丢失上界高于任何幸存帧时,游标必须跳到丢失上界。

    不跳的话下一轮会把同一段丢失再当缺口信号 —— 客户端被无限 resync。
    """
    offline = FakeOffline([[10]], lost=99)
    uc = pbiz.PushUsecase(pconn.ConnectionManager(), offline)
    slot = FakeSlot()
    cursor, err = await uc.drain_buffer(slot, 7, 0, pbiz.SessionInfo())
    assert err is None
    assert cursor == 99


@pytest.mark.asyncio
async def test_drain_first_connect_with_empty_buffer_does_not_check_gap() -> None:
    """cursor=0 且缓冲无帧:不做 gap 检测(新客户端无增量历史)。"""
    offline = FakeOffline([], lost=999)
    uc = pbiz.PushUsecase(pconn.ConnectionManager(), offline)
    slot = FakeSlot()
    cursor, err = await uc.drain_buffer(slot, 7, 0, pbiz.SessionInfo())
    assert (cursor, err) == (0, None)
    assert slot.sent == []


@pytest.mark.asyncio
async def test_drain_per_frame_fence_stops_mid_batch() -> None:
    """★ 逐帧 fence:批内轮换后**后续帧一律不发**。

    按批 fence(Go R6 之前)会把整批最多 512 帧全发完 —— 旧流在轮换后仍收到
    一整批私有推送。这条用例让 gate 在第 2 次查询时"轮换",断言只发了 1 帧。
    """

    class RotatingGate:
        def __init__(self) -> None:
            self.n = 0

        async def current_jti(self, player_id: int) -> tuple[str, bool]:
            self.n += 1
            return ("j1", True) if self.n == 1 else ("j2", True)

    uc = pbiz.PushUsecase(pconn.ConnectionManager(), FakeOffline([[10, 20, 30]]))
    uc.set_session_gate(RotatingGate(), False)
    slot = FakeSlot()
    cursor, err = await uc.drain_buffer(slot, 7, 0, pbiz.SessionInfo(jti="j1"))
    assert len(slot.sent) == 1
    assert cursor == 10  # 停在最后一条已交付帧:不漏不重
    assert pbiz.is_session_fence_close(err) is True


@pytest.mark.asyncio
async def test_drain_gap_check_failure_does_not_advance_cursor() -> None:
    """★ gap 检测失败必须返回错误,**不得当「无丢失」继续**。

    继续的后果:游标越过缺口后 resync 永远无法触发 = 永久静默漏报。
    """

    class ExplodingLost(FakeOffline):
        async def lost_since(self, player_id, after_cursor, now_ms):  # noqa: ANN001
            raise RuntimeError("redis down")

    uc = pbiz.PushUsecase(pconn.ConnectionManager(), ExplodingLost([[10], [20]]))
    slot = FakeSlot()
    cursor, err = await uc.drain_buffer(slot, 7, 0, pbiz.SessionInfo())
    assert err is not None
    assert pbiz.is_session_fence_close(err) is False  # 瞬时失败 → 退避,不是关流
    assert cursor == 10  # 停在预检之前那一页的上界


# ══════════════════════════════════════════════════════════════════════════
# B3. 连接索引
# ══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_register_supersedes_old_slot() -> None:
    cm = pconn.ConnectionManager()

    async def _w(_frame) -> None:  # noqa: ANN001
        return None

    old = cm.register(7, _w)
    new = cm.register(7, _w)
    assert old.closed.is_set() is True   # 顶号:旧槽被关
    assert new.closed.is_set() is False
    assert cm.size() == 1


@pytest.mark.asyncio
async def test_unregister_only_removes_its_own_slot() -> None:
    """★ 旧流的 finally 跑在新流注册之后;无条件 delete 会把**新流**删掉。"""
    cm = pconn.ConnectionManager()

    async def _w(_frame) -> None:  # noqa: ANN001
        return None

    old = cm.register(7, _w)
    new = cm.register(7, _w)
    cm.unregister(7, old)          # 旧流退出
    assert cm.size() == 1          # 新流还在
    assert cm.send_to(7) is True
    cm.unregister(7, new)
    assert cm.size() == 0
    assert cm.send_to(7) is False  # 不在线不是错误,只是返回 False


@pytest.mark.asyncio
async def test_broadcast_drops_when_box_is_full() -> None:
    """广播箱满即丢并计数(丢失容忍,离线不补推)。"""
    cm = pconn.ConnectionManager()

    async def _w(_frame) -> None:  # noqa: ANN001
        return None

    cm.register(7, _w)
    frame = push_pb2.PushFrame(topic="w")
    for _ in range(pconn.BROADCAST_QUEUE_SIZE):
        assert cm.broadcast(frame) == (1, 0)
    assert cm.broadcast(frame) == (0, 1)


# ══════════════════════════════════════════════════════════════════════════
# C. kafka 消费侧
# ══════════════════════════════════════════════════════════════════════════


class FakeMsg:
    def __init__(self, topic: str, key=None, value: bytes = b"p", headers=None, ts: int = 111):  # noqa: ANN001
        self.topic = topic
        self.key = key
        self.value = value
        self.headers = headers or []
        self.timestamp = ts
        self.partition = 0
        self.offset = 0


class RecordingOffline:
    def __init__(self) -> None:
        self.calls: list[tuple[int, object]] = []
        self.fail = False

    async def assign_and_buffer(self, player_id: int, frame, now_ms: int) -> int:  # noqa: ANN001
        if self.fail:
            raise RuntimeError("redis down")
        self.calls.append((player_id, frame))
        return 1234


def _consumer(topic: str, conns=None, offline=None) -> pcons.PushKafkaConsumer:  # noqa: ANN001
    return pcons.PushKafkaConsumer(
        brokers=["127.0.0.1:9093"],
        group_id="pandora-push",
        topic=topic,
        conns=conns or pconn.ConnectionManager(),
        offline=offline or RecordingOffline(),
        dlq=None,
        consumer_factory=lambda: None,  # 不真连 kafka
    )


def test_broadcast_topic_consumer_config() -> None:
    """★ 广播 topic 的三件套:独立 group / latest / **关 offset 提交**。

    第三条漏掉的后果:Pod 同名重启会把停机窗口积压的广播整段重放给全部在线连接。
    """
    kc = _consumer(kafka_topics.TOPIC_CHAT_WORLD)
    conf = kc._consumer._conf  # noqa: SLF001 —— 断言的正是这份配置本身
    assert kc.broadcast is True
    assert conf.group_id.startswith("pandora-push-bcast-")
    assert conf.group_id != "pandora-push"
    assert conf.initial_offset == "latest"
    assert conf.disable_offset_commit is True


def test_directed_topic_consumer_config() -> None:
    kc = _consumer(kafka_topics.TOPIC_TEAM_UPDATE)
    conf = kc._consumer._conf  # noqa: SLF001
    assert kc.broadcast is False
    assert conf.group_id == "pandora-push"
    assert conf.initial_offset == "earliest"
    assert conf.disable_offset_commit is False
    assert conf.retry.max_retries == pcons.DLQ_MAX_RETRIES


@pytest.mark.asyncio
async def test_broadcast_topic_goes_to_broadcast_not_key_routing() -> None:
    """★ 广播 topic 的 key 是空的。当成定向去解析 player_id = 全服公告静默不达。"""
    cm = pconn.ConnectionManager()
    delivered: list = []

    async def _w(frame) -> None:  # noqa: ANN001
        delivered.append(frame)

    cm.register(7, _w)
    offline = RecordingOffline()
    kc = _consumer(kafka_topics.TOPIC_CHAT_WORLD, conns=cm, offline=offline)
    await kc.handle(FakeMsg(kafka_topics.TOPIC_CHAT_WORLD, key=None, ts=999))
    assert offline.calls == []            # 广播不入投递缓冲
    slot = cm._by_player[7]               # noqa: SLF001
    frame = slot.bcast.get_nowait()
    # ts_ms 必须置 0:携带 kafka 时间戳会让客户端游标永久越过较小的定向游标。
    assert frame.ts_ms == 0
    assert frame.topic == kafka_topics.TOPIC_CHAT_WORLD


@pytest.mark.asyncio
async def test_directed_topic_buffers_and_wakes() -> None:
    cm = pconn.ConnectionManager()

    async def _w(_frame) -> None:  # noqa: ANN001
        return None

    slot = cm.register(7, _w)
    offline = RecordingOffline()
    kc = _consumer(kafka_topics.TOPIC_TEAM_UPDATE, conns=cm, offline=offline)
    await kc.handle(FakeMsg(kafka_topics.TOPIC_TEAM_UPDATE, key=b"7"))
    assert [pid for pid, _ in offline.calls] == [7]
    assert slot.notify.is_set() is True


@pytest.mark.asyncio
@pytest.mark.parametrize("key", [None, b"", b"abc", b" 7 ", b"1_0", b"-3", "٧".encode()])
async def test_invalid_key_is_poison(key) -> None:  # noqa: ANN001
    """★ key 判据必须是「全 ASCII 十进制」。

    Python 的 int() 比 Go 的 ParseUint 宽得多(空白 / 下划线 / Unicode 数字 / 正负号),
    宽在这里不是宽容而是**分叉**:Go 判毒丸留证,Python 却把它当成**别的**
    player_id 写进那个人的缓冲 —— 一条消息投给了错的玩家,两边都不报错。
    """
    kc = _consumer(kafka_topics.TOPIC_TEAM_UPDATE)
    with pytest.raises(kafkax.PoisonError):
        await kc.handle(FakeMsg(kafka_topics.TOPIC_TEAM_UPDATE, key=key))


@pytest.mark.asyncio
async def test_zero_player_key_is_poison() -> None:
    """key="0" 能过解析,但 Snowflake 恒非 0 —— 不拦就是静默吞掉一条定向消息。"""
    offline = RecordingOffline()
    kc = _consumer(kafka_topics.TOPIC_TEAM_UPDATE, offline=offline)
    with pytest.raises(kafkax.PoisonError):
        await kc.handle(FakeMsg(kafka_topics.TOPIC_TEAM_UPDATE, key=b"0"))
    assert offline.calls == []


@pytest.mark.asyncio
async def test_malformed_event_type_is_poison_not_legacy_zero() -> None:
    """★ event_type 非法**不得降级为 legacy 0**。

    降级 = 把新事件按旧 message 路由,客户端用错误的 proto 解析 payload;
    字段可能凑巧对上,表现为误弹提示 / 污染缓存,而不是干脆的解析失败。
    """
    kc = _consumer(kafka_topics.TOPIC_TEAM_UPDATE)
    msg = FakeMsg(
        kafka_topics.TOPIC_TEAM_UPDATE, key=b"7", headers=[("event_type", b"abc")]
    )
    with pytest.raises(kafkax.PoisonError):
        await kc.handle(msg)


@pytest.mark.asyncio
async def test_missing_event_type_is_legacy_zero() -> None:
    """缺失 / 空值 → 0,是显式的 legacy 兼容契约(旧 producer 不填)。"""
    assert pcons.parse_event_type_header([]) == 0
    assert pcons.parse_event_type_header([("event_type", b"")]) == 0
    assert pcons.parse_event_type_header([("event_type", b"42")]) == 42


@pytest.mark.asyncio
async def test_buffer_failure_raises_retryable_pandora_error() -> None:
    """★ 入缓冲失败必须**拒 ack**(抛可重试错误),不能吞掉。

    吞掉 = kafka ack 了但这一帧没有任何持久化,客户端重连也补不回来。
    """
    offline = RecordingOffline()
    offline.fail = True
    kc = _consumer(kafka_topics.TOPIC_TEAM_UPDATE, offline=offline)
    with pytest.raises(errcode.PandoraError) as ei:
        await kc.handle(FakeMsg(kafka_topics.TOPIC_TEAM_UPDATE, key=b"7"))
    assert ei.value.code == errcode.ErrPushOfflineCorrupted
    assert not isinstance(ei.value, kafkax.PoisonError)  # 瞬时错误要重试,不是毒丸


@pytest.mark.asyncio
async def test_wake_publish_failure_does_not_block_ack() -> None:
    """跨 Pod 唤醒是 best-effort:publish 失败不得让消息不 ack(帧已在缓冲)。"""

    class BadWake:
        async def publish_wake(self, player_id: int) -> None:
            raise RuntimeError("pubsub down")

    kc = _consumer(kafka_topics.TOPIC_TEAM_UPDATE)
    kc.set_wake_publisher(BadWake())
    await kc.handle(FakeMsg(kafka_topics.TOPIC_TEAM_UPDATE, key=b"7"))  # 不抛即为通过


# ══════════════════════════════════════════════════════════════════════════
# D. 公共件缺口 / 启动闸
# ══════════════════════════════════════════════════════════════════════════


def test_producer_has_send_raw_with_headers() -> None:
    """★ `KeyOrderedConsumer._to_dlq` 一直在调这个方法,而它此前不存在。

    缺了它 AttributeError 会被 `_to_dlq` 的宽 except 当成"DLQ 投递失败" →
    **不 ack** → 该 partition 永久卡在第一条毒丸上。push 13 个 topic 全配 DLQ,
    是第一个会踩到的服务。
    """
    assert hasattr(kafkax.KeyOrderedProducer, "send_raw_with_headers")


def test_dlq_topic_naming() -> None:
    assert kafka_topics.build_dlq_topic("pandora.team.update") == "pandora.dlq.team.update"


class FakeRedis:
    def __init__(self, values) -> None:  # noqa: ANN001
        self._values = values

    async def config_get(self, name: str, **kw):  # noqa: ANN001
        if isinstance(self._values, BaseException):
            raise self._values
        return self._values


@pytest.mark.asyncio
async def test_eviction_gate_accepts_noeviction() -> None:
    assert await pmain.verify_eviction_policy(
        FakeRedis({"maxmemory-policy": "noeviction"}), False
    ) is True
    # bytes 形态(decode_responses=False 的客户端)也必须认。
    assert await pmain.verify_eviction_policy(
        FakeRedis({b"maxmemory-policy": b"noeviction"}), False
    ) is True


@pytest.mark.asyncio
async def test_eviction_gate_rejects_lru() -> None:
    """★ 非 noeviction 必须拒启:内存压力下会**静默驱逐**投递缓冲与会话 key。"""
    assert await pmain.verify_eviction_policy(
        FakeRedis({"maxmemory-policy": "allkeys-lru"}), False
    ) is False


@pytest.mark.asyncio
async def test_eviction_gate_fails_closed_when_unverifiable() -> None:
    """★ CONFIG GET 失败缺省拒启:「查不了」≠「配置正确」。"""
    assert await pmain.verify_eviction_policy(
        FakeRedis(RuntimeError("CONFIG disabled")), False
    ) is False
    # 显式授权后降级为 WARN 放行(托管 Redis 禁用 CONFIG 的部署)。
    assert await pmain.verify_eviction_policy(
        FakeRedis(RuntimeError("CONFIG disabled")), True
    ) is True


def test_per_node_shape_detection() -> None:
    """cluster 的 config_get 返回形状判错 → 这道闸恒通过、静默失效。"""
    assert pmain._looks_like_per_node({"n1": {"maxmemory-policy": "noeviction"}}) is True
    assert pmain._looks_like_per_node({"maxmemory-policy": "noeviction"}) is False
    assert pmain._looks_like_per_node({}) is False
