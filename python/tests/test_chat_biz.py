"""chat 业务逻辑测试。

重点盯三条最容易在移植中改错的规则(见 biz.py 头注释):
  1. 推送原则 2 —— 不回发自己(世界频道例外)
  2. 成员解析失败必须诚实报错,**不能假成功**(假成功 = 消息静默丢失 + 跳过身份校验)
  3. 限流一律 fail-open(背压门不是权威门)

外加一条 Python 特有的坑:内容长度必须按 **Unicode 码点**计,不是字节。
"""

from __future__ import annotations

import datetime as _dt
import pathlib
import re

import pytest
from pandora.chat.v1 import chat_pb2

from pandorapy import errcode, redisx
from pandorapy.services.chat import biz as cbiz
from pandorapy.services.chat import conf as cconf
from pandorapy.services.chat import data as cdata

_C = chat_pb2.ChatChannel


class FakeRepo:
    def __init__(self) -> None:
        self.saved: list = []
        self.fail = False

    async def save_private(self, msg) -> None:
        if self.fail:
            raise errcode.PandoraError(errcode.ErrInternal, "mysql down")
        self.saved.append(msg)

    async def list_private(self, player_id, peer_id, limit, before_ms):  # noqa: ANN001
        return [m for m in self.saved][:limit]

    async def sweep_messages_before(self, mode, max_message_id, limit):  # noqa: ANN001
        return None


class RecordingPusher:
    def __init__(self, fail_for: set[int] | None = None) -> None:
        self.private: list[tuple[int, object]] = []
        self.team: list[tuple[int, object]] = []
        self.world: list = []
        self.guild: list[tuple[int, object]] = []
        self.group: list[tuple[int, object]] = []
        self.fail_for = fail_for or set()

    async def push_private(self, to, evt) -> None:  # noqa: ANN001
        if to in self.fail_for:
            raise RuntimeError("kafka down")
        self.private.append((to, evt))

    async def push_team(self, to, evt) -> None:  # noqa: ANN001
        if to in self.fail_for:
            raise RuntimeError("kafka down")
        self.team.append((to, evt))

    async def push_world(self, evt) -> None:  # noqa: ANN001
        self.world.append(evt)

    async def push_guild(self, to, evt) -> None:  # noqa: ANN001
        if to in self.fail_for:
            raise RuntimeError("kafka down")
        self.guild.append((to, evt))

    async def push_group(self, to, evt) -> None:  # noqa: ANN001
        if to in self.fail_for:
            raise RuntimeError("kafka down")
        self.group.append((to, evt))


class FakeReader:
    """成员解析。raises=True 模拟服务不可达;found=False 模拟容器不存在。"""

    def __init__(self, members: list[int], *, found: bool = True, raises: bool = False) -> None:
        self._members = members
        self._found = found
        self._raises = raises

    async def members(self, container_id: int) -> tuple[list[int], bool]:
        if self._raises:
            raise RuntimeError("team service unreachable")
        return self._members, self._found


def _cfg(**overrides) -> cconf.ChatConf:
    full = cconf.Config(chat=cconf.ChatConf(**overrides))
    full.apply_defaults()
    return full.chat


def _uc(repo=None, pusher=None, team=None, guild=None, group=None, cfg=None):
    return cbiz.ChatUsecase(
        repo or FakeRepo(), pusher, team, guild, group, cfg or _cfg()
    )


# ── 频道校验 ──────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "channel", [_C.CHAT_CHANNEL_UNSPECIFIED, _C.CHAT_CHANNEL_SYSTEM]
)
async def test_client_cannot_send_system_or_unspecified(channel) -> None:
    """★ 客户端不能发 SYSTEM / UNSPECIFIED —— 否则可以伪造系统公告。"""
    uc = _uc()
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.send_message(1001, channel, 0, "hi", 900001)
    assert exc.value.code == errcode.ErrChatChannelInvalid


# ── 内容校验 ──────────────────────────────────────────────────────────────────


async def test_empty_and_whitespace_content_rejected() -> None:
    uc = _uc()
    for content in ("", "   ", "\n\t "):
        with pytest.raises(errcode.PandoraError, match="empty content"):
            await uc.send_message(1001, _C.CHAT_CHANNEL_WORLD, 0, content, 900001)


async def test_content_length_counts_codepoints_not_bytes() -> None:
    """★ 长度按 Unicode 码点计,不是字节。

    Go 用 utf8.RuneCountInString。若 Python 侧误用 len(content.encode()),
    中文用户的可发长度会只有 1/3 —— 而且不报错,只是"发不出去",
    表现为"英文能发中文不能发"这种莫名其妙的现象。
    """
    uc = _uc(cfg=_cfg(max_content_len=10))
    # 10 个中文 = 30 字节,按码点算正好到上限,应当放行
    assert await uc.send_message(1001, _C.CHAT_CHANNEL_WORLD, 0, "中" * 10, 900001) == 900001
    # 11 个则超限
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.send_message(1001, _C.CHAT_CHANNEL_WORLD, 0, "中" * 11, 900002)
    assert exc.value.code == errcode.ErrChatMessageTooLong


async def test_sensitive_words_masked_with_equal_length() -> None:
    """敏感词替换为**等长** `*` —— 长度变了会让长度校验与实际展示对不上。"""
    pusher = RecordingPusher()
    uc = _uc(pusher=pusher, cfg=_cfg(sensitive_words=["外挂", "代练"]))
    await uc.send_message(1001, _C.CHAT_CHANNEL_WORLD, 0, "卖外挂和代练", 900001)
    content = pusher.world[0].message.content
    assert content == "卖**和**"
    assert len(content) == len("卖外挂和代练")


# ── ★ 推送原则 2 ──────────────────────────────────────────────────────────────


async def test_team_fanout_excludes_sender() -> None:
    """★ 队伍频道不回发自己 —— 客户端本地回显己方消息。

    发错会让客户端把自己的消息显示两遍。
    """
    pusher = RecordingPusher()
    uc = _uc(pusher=pusher, team=FakeReader([1001, 2002, 3003]))
    await uc.send_message(1001, _C.CHAT_CHANNEL_TEAM, 500, "hi team", 900001)
    assert sorted(to for to, _ in pusher.team) == [2002, 3003]


async def test_guild_and_group_fanout_exclude_sender() -> None:
    pusher = RecordingPusher()
    uc = _uc(
        pusher=pusher,
        guild=FakeReader([1001, 2002]),
        group=FakeReader([1001, 4004]),
    )
    await uc.send_message(1001, _C.CHAT_CHANNEL_GUILD, 600, "hi guild", 900001)
    await uc.send_message(1001, _C.CHAT_CHANNEL_GROUP, 700, "hi group", 900002)
    assert [to for to, _ in pusher.guild] == [2002]
    assert [to for to, _ in pusher.group] == [4004]


async def test_world_is_broadcast_not_per_member() -> None:
    """世界频道是广播:to_player_id=0,一条事件而不是逐人推(原则 2 的唯一例外)。"""
    pusher = RecordingPusher()
    uc = _uc(pusher=pusher)
    await uc.send_message(1001, _C.CHAT_CHANNEL_WORLD, 0, "hello world", 900001)
    assert len(pusher.world) == 1
    assert pusher.world[0].to_player_id == 0


# ── ★ 成员解析失败不能假成功 ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("channel", "reader_kw"),
    [
        (_C.CHAT_CHANNEL_TEAM, "team"),
        (_C.CHAT_CHANNEL_GUILD, "guild"),
        (_C.CHAT_CHANNEL_GROUP, "group"),
    ],
)
async def test_member_service_unreachable_raises_not_fake_success(channel, reader_kw) -> None:
    """★ 成员服务不可达必须报 ErrUnavailable,不能返回 message_id 假装成功。

    假成功的后果:没有任何人收到消息而发送者以为已送达(**静默丢失**),
    并且成员身份校验被整个跳过 —— 非成员也能在该频道说话。
    """
    pusher = RecordingPusher()
    uc = _uc(pusher=pusher, **{reader_kw: FakeReader([], raises=True)})
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.send_message(1001, channel, 500, "hi", 900001)
    assert exc.value.code == errcode.ErrUnavailable
    assert not (pusher.team or pusher.guild or pusher.group), "报错了却仍推送了消息"


@pytest.mark.parametrize(
    ("channel", "reader_kw"),
    [
        (_C.CHAT_CHANNEL_TEAM, "team"),
        (_C.CHAT_CHANNEL_GUILD, "guild"),
        (_C.CHAT_CHANNEL_GROUP, "group"),
    ],
)
async def test_non_member_cannot_speak(channel, reader_kw) -> None:
    """★ 非成员不能在该频道说话。"""
    uc = _uc(pusher=RecordingPusher(), **{reader_kw: FakeReader([2002, 3003])})
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.send_message(1001, channel, 500, "hi", 900001)
    assert exc.value.code == errcode.ErrChatChannelInvalid
    assert "not in" in exc.value.msg


async def test_container_not_found_rejected() -> None:
    uc = _uc(pusher=RecordingPusher(), team=FakeReader([], found=False))
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.send_message(1001, _C.CHAT_CHANNEL_TEAM, 500, "hi", 900001)
    assert exc.value.code == errcode.ErrChatChannelInvalid


async def test_unconfigured_reader_degrades_silently() -> None:
    """弱依赖**未配置**(部署形态)→ 降级返回 message_id,与"不可达"(故障)区别对待。"""
    uc = _uc(pusher=RecordingPusher(), team=None)
    assert await uc.send_message(1001, _C.CHAT_CHANNEL_TEAM, 500, "hi", 900001) == 900001


# ── 私聊 ──────────────────────────────────────────────────────────────────────


async def test_private_requires_target_and_rejects_self() -> None:
    uc = _uc()
    with pytest.raises(errcode.PandoraError, match="requires target_id"):
        await uc.send_message(1001, _C.CHAT_CHANNEL_PRIVATE, 0, "hi", 900001)
    with pytest.raises(errcode.PandoraError, match="cannot private chat self"):
        await uc.send_message(1001, _C.CHAT_CHANNEL_PRIVATE, 1001, "hi", 900002)


async def test_private_save_is_hard_dependency() -> None:
    """★ 落库是**强依赖** —— MySQL 失败则整条失败,让客户端重试。

    私聊历史不可丢:若吞掉落库错误只推送,接收方离线时消息就永久消失了。
    """
    repo = FakeRepo()
    repo.fail = True
    pusher = RecordingPusher()
    uc = _uc(repo=repo, pusher=pusher)
    with pytest.raises(errcode.PandoraError):
        await uc.send_message(1001, _C.CHAT_CHANNEL_PRIVATE, 2002, "hi", 900001)
    assert not pusher.private, "落库失败后仍然推送了 —— 接收方会看到一条查不到历史的消息"


async def test_private_push_is_weak_dependency() -> None:
    """推送是**弱依赖** —— 失败只 warn,消息已落库,接收方上线 PullHistory 兜底。"""
    repo = FakeRepo()
    pusher = RecordingPusher(fail_for={2002})
    uc = _uc(repo=repo, pusher=pusher)
    assert await uc.send_message(1001, _C.CHAT_CHANNEL_PRIVATE, 2002, "hi", 900001) == 900001
    assert len(repo.saved) == 1  # 落库成功


async def test_fanout_push_failure_does_not_fail_the_send() -> None:
    """扇出推送部分失败不影响整体成功(弱依赖),但要有汇总告警信号。"""
    pusher = RecordingPusher(fail_for={2002})
    uc = _uc(pusher=pusher, team=FakeReader([1001, 2002, 3003]))
    assert await uc.send_message(1001, _C.CHAT_CHANNEL_TEAM, 500, "hi", 900001) == 900001
    assert [to for to, _ in pusher.team] == [3003]  # 2002 失败,3003 成功


# ── ★ 限流 fail-open ─────────────────────────────────────────────────────────


async def test_world_cooldown_rejects_before_any_kafka_write() -> None:
    """★ 冷却期内直接拒绝,**不产生任何 kafka 写**。

    广播成本 ≈ 发送速率 × 全服在线数,必须在生产侧压掉。
    """

    class DenyWorld:
        async def allow_world(self, player_id, cooldown):  # noqa: ANN001
            return False

    pusher = RecordingPusher()
    uc = _uc(pusher=pusher)
    uc.set_world_rate_limiter(DenyWorld())
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.send_message(1001, _C.CHAT_CHANNEL_WORLD, 0, "spam", 900001)
    assert exc.value.code == errcode.ErrRateLimited
    assert not pusher.world, "冷却拒绝后仍写了 kafka"


async def test_rate_limiter_failure_is_fail_open() -> None:
    """★ 限流判定失败必须放行 —— 背压门不是权威门。

    Redis 抖动时把聊天整个关掉,等于把防刷工具变成故障开关。
    """

    class Broken:
        async def allow_world(self, player_id, cooldown):  # noqa: ANN001
            raise RuntimeError("redis down")

        async def allow_channel(self, channel, player_id, cooldown):  # noqa: ANN001
            raise RuntimeError("redis down")

    pusher = RecordingPusher()
    uc = _uc(pusher=pusher, repo=FakeRepo())
    uc.set_world_rate_limiter(Broken())
    uc.set_channel_rate_limiter(Broken())
    assert await uc.send_message(1001, _C.CHAT_CHANNEL_WORLD, 0, "hi", 900001) == 900001
    assert await uc.send_message(1001, _C.CHAT_CHANNEL_PRIVATE, 2002, "hi", 900002) == 900002


async def test_channel_cooldown_rejects_before_side_effects() -> None:
    """非世界频道冷却必须在**落库/推送之前**判定。"""

    class DenyChannel:
        async def allow_channel(self, channel, player_id, cooldown):  # noqa: ANN001
            return False

    repo = FakeRepo()
    uc = _uc(repo=repo, pusher=RecordingPusher())
    uc.set_channel_rate_limiter(DenyChannel())
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.send_message(1001, _C.CHAT_CHANNEL_PRIVATE, 2002, "hi", 900001)
    assert exc.value.code == errcode.ErrRateLimited
    assert not repo.saved, "冷却拒绝前已经落库了"


async def test_channel_cooldown_is_per_channel() -> None:
    """按频道独立占窗:队聊不占私聊的窗。"""
    seen: list[str] = []

    class Recorder:
        async def allow_channel(self, channel, player_id, cooldown):  # noqa: ANN001
            seen.append(channel)
            return True

    uc = _uc(repo=FakeRepo(), pusher=RecordingPusher(), team=FakeReader([1001, 2002]))
    uc.set_channel_rate_limiter(Recorder())
    await uc.send_message(1001, _C.CHAT_CHANNEL_PRIVATE, 2002, "a", 900001)
    await uc.send_message(1001, _C.CHAT_CHANNEL_TEAM, 500, "b", 900002)
    assert seen == ["private", "team"]


# ── PullHistory ───────────────────────────────────────────────────────────────


async def test_pull_history_only_for_private() -> None:
    """即时频道无持久化历史 → 返回空列表(不是报错)。"""
    uc = _uc(repo=FakeRepo())
    for ch in (_C.CHAT_CHANNEL_WORLD, _C.CHAT_CHANNEL_TEAM, _C.CHAT_CHANNEL_GUILD):
        assert await uc.pull_history(1001, ch, 2002, 10, 0) == []


async def test_pull_history_requires_peer() -> None:
    uc = _uc(repo=FakeRepo())
    with pytest.raises(errcode.PandoraError, match="peer_id required"):
        await uc.pull_history(1001, _C.CHAT_CHANNEL_PRIVATE, 0, 10, 0)


async def test_pull_history_limit_is_clamped() -> None:
    """limit <= 0 或超过 history_limit 都钳到 history_limit(读取侧单次返回上限,§9.18)。"""
    captured: dict[str, int] = {}

    class CaptureRepo(FakeRepo):
        async def list_private(self, player_id, peer_id, limit, before_ms):  # noqa: ANN001
            captured["limit"] = limit
            return []

    uc = _uc(repo=CaptureRepo(), cfg=_cfg(history_limit=50))
    await uc.pull_history(1001, _C.CHAT_CHANNEL_PRIVATE, 2002, 0, 0)
    assert captured["limit"] == 50
    await uc.pull_history(1001, _C.CHAT_CHANNEL_PRIVATE, 2002, 9999, 0)
    assert captured["limit"] == 50
    await uc.pull_history(1001, _C.CHAT_CHANNEL_PRIVATE, 2002, 20, 0)
    assert captured["limit"] == 20


# ── 配置默认值 ────────────────────────────────────────────────────────────────


def test_retention_mode_defaults_to_report_only() -> None:
    """★ 保留期清理默认**只报告不删**(用户 2026-07-22 指令)。"""
    from pandorapy import dbguard

    cfg = _cfg()
    assert cfg.retention_mode_parsed() is dbguard.Mode.REPORT_ONLY


def test_retention_mode_typo_is_rejected() -> None:
    """拼错的 retention_mode 必须在**启动期**报错,不能猜成 delete。

    ★ 两个方法分工不同(与 Go 的 ValidateRetentionMode / RetentionMode 逐条对应):
      - validate_retention_mode() 抛 → main 打 chat_retention_mode_invalid 后拒启;
      - retention_mode_parsed() **回落 report_only** → 任何不确定都不删数据。
    把 parsed 也做成抛异常的话,后台清理循环每轮抛一次异常(而不是安全地不删),
    真正的配置错误反而淹没在循环噪声里。
    """
    cfg = _cfg(retention_mode="delet")
    with pytest.raises(ValueError, match="无法识别"):
        cfg.validate_retention_mode()
    # 运行期取值绝不能猜成 delete。
    from pandorapy import dbguard

    assert cfg.retention_mode_parsed() is dbguard.Mode.REPORT_ONLY


# ── ★ 与 Go 的跨栈一致性(判据全部取自 Go 源码,不在测试里手抄常量)────────────

_GO_DURATION_UNITS = {
    "Nanosecond": 1e-9,
    "Microsecond": 1e-6,
    "Millisecond": 1e-3,
    "Second": 1.0,
    "Minute": 60.0,
    "Hour": 3600.0,
}


def _go_chat_defaults(repo_root: pathlib.Path) -> dict[str, float]:
    """抠出 Go 侧 chat 的 Defaults()。

    刻意读源码而不是把数字抄进测试:手抄的常量和被测实现是同一次手抄的产物,
    抄错时两边一起错,测试永远绿(这一处正是这么漏过去的)。
    """
    src = (
        repo_root / "services" / "social" / "chat" / "internal" / "conf" / "conf.go"
    ).read_text(encoding="utf-8")
    body = re.search(r"func \(c \*Config\) Defaults\(\) \{(.*?)\n\}", src, re.S)
    assert body, "没在 Go 源码里找到 chat 的 Defaults()"
    out: dict[str, float] = {}
    for name, num, unit in re.findall(
        r"c\.Chat\.(\w+) = config\.Duration\((\d+) \* time\.(\w+)\)", body.group(1)
    ):
        out[name] = int(num) * _GO_DURATION_UNITS[unit]
    for name, num in re.findall(r"c\.Chat\.(\w+) = (\d+)\n", body.group(1)):
        out[name] = int(num)
    return out


def test_defaults_match_go_source(repo_root: pathlib.Path) -> None:
    """★ 默认值必须与 Go 逐个相等。

    全仓 21 份 yaml 一份都没配 world_cooldown / non_world_cooldown /
    sweep_interval / sweep_batch —— 默认值就是生产实际值,不是"兜底"。
    差一点就是同一个玩家在 Go 副本和 Python 副本上限速不同、清理批量不同。
    """
    go = _go_chat_defaults(repo_root)
    cfg = _cfg()
    assert cfg.max_content_len == go["MaxContentLen"]
    assert cfg.history_limit == go["HistoryLimit"]
    assert cfg.history_retention_days == go["HistoryRetentionDays"]
    assert cfg.world_cooldown_td().total_seconds() == go["WorldCooldown"]
    assert cfg.non_world_cooldown_td().total_seconds() == go["NonWorldCooldown"]
    assert cfg.sweep_interval_td().total_seconds() == go["SweepInterval"]
    assert cfg.sweep_batch == go["SweepBatch"]


def test_group_channel_has_no_own_addr_field(repo_root: pathlib.Path) -> None:
    """★ GROUP 频道**没有**独立地址字段,与 GUILD 共用 guild_addr。

    多出一个 group_addr 不会报错 —— 它恒为空,GROUP 频道就恒走弱依赖降级,
    消息静默不扇出。这类"合法档"的缺陷只能靠盯字段本身抓。
    """
    assert "group_addr" not in cconf.ChatConf.model_fields, "重新长出了 group_addr"
    main_src = (
        repo_root / "services" / "social" / "chat" / "cmd" / "chat" / "main.go"
    ).read_text(encoding="utf-8")
    assert "NewGrpcGroupReader(cfg.Chat.GuildAddr)" in main_src, "Go 侧不再共用 guild_addr"
    assert "cfg.Chat.GroupAddr" not in main_src


class _KeyCapturingRedis:
    """只记 key 的假 Redis —— 这组测试验的是 key 拼法,不是 Redis 行为。"""

    def __init__(self) -> None:
        self.keys: list[str] = []

    async def set(self, key, value, nx=False, px=None):  # noqa: ANN001, ANN201, ARG002
        self.keys.append(key)
        return True


async def test_world_cooldown_key_matches_go_source(repo_root: pathlib.Path) -> None:
    """★ 世界频道冷却 key 与 Go 逐字一致。

    迁移期两栈同时在线共用一套 Redis。key 不同 = 同一玩家在两侧各占一个窗,
    实际冷却被放宽一倍,而两边日志都显示"限流生效中"。
    """
    src = (
        repo_root / "services" / "social" / "chat" / "internal" / "data" / "world_ratelimit.go"
    ).read_text(encoding="utf-8")
    m = re.search(
        r'func worldCooldownKey\([^)]*\)\s*string\s*\{\s*return fmt\.Sprintf\("([^"]+)"', src
    )
    assert m, "没在 Go 源码里找到 worldCooldownKey 的格式串"
    expected = m.group(1).replace("%d", "{}").format(1001)

    rdb = _KeyCapturingRedis()
    limiter = cdata.RedisRateLimiter(rdb)
    assert await limiter.allow_world(1001, _dt.timedelta(seconds=3))
    assert rdb.keys == [expected]


async def test_channel_cooldown_key_follows_rl_convention(repo_root: pathlib.Path) -> None:
    """★ 非世界频道 key 走 pandora:rl:* 规范,域名取自 Go 的 RLKey 调用。

    按 pandora:rl:* 建的限流巡检脚本靠这个前缀发现主体;前缀不对,
    Python 副本在巡检里整体不可见 —— 没有告警不等于没在被刷。
    """
    src = (
        repo_root / "services" / "social" / "chat" / "internal" / "data" / "world_ratelimit.go"
    ).read_text(encoding="utf-8")
    m = re.search(r'redisx\.RLKey\("(\w+)", channel, playerID\)', src)
    assert m, "Go 侧非世界频道 key 不再走 redisx.RLKey,Python 需要同步"
    domain = m.group(1)

    rdb = _KeyCapturingRedis()
    limiter = cdata.RedisRateLimiter(rdb)
    assert await limiter.allow_channel("team", 1001, _dt.timedelta(seconds=1))
    assert rdb.keys == [redisx.rl_key(domain, "team", 1001)]
    assert rdb.keys[0].startswith("pandora:rl:")
