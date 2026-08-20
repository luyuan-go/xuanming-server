"""friend 服务:配置默认值对齐、鉴权边界、推送方向、限流门、sweep 兜错。

这份用例挑的全是**静默出错**的点位 —— 不会崩、日志正常、但语义已经错了:

  1. 配置默认值与 Go 分叉(尤其 `== 0` vs `<= 0` 的判据符号)
     → 同一份 yaml 两个实现行为不同,**两边都不报错**
  2. player_id 不取鉴权上下文 → 越权操作他人好友关系(R5)
  3. 推送方向反了 → "你收到好友申请"发给了发起者自己,接收方永远收不到
  4. 拒绝也推送 → 玩家被通知"你被拒绝了"(业界惯例是不发)
  5. 非 target 处理申请时回 UNAUTHORIZED 而不是 NOT_FOUND
     → 可用来探测"这条申请存不存在" = 探测他人社交关系
  6. 限流门放在写库之后 → 限流形同虚设(照样打库)
  7. sweep 第一件事失败就整轮 return → pair 守卫行**永远不被清**(§9.24)
"""

from __future__ import annotations

import asyncio
import pathlib
import re

import pytest
from pandora.common.v1 import errcode_pb2
from pandora.friend.v1 import friend_pb2

from pandorapy import dbguard, errcode, interceptors
from pandorapy.services.friend import biz as fbiz
from pandorapy.services.friend import conf as fconf
from pandorapy.services.friend import locator_client as flocator
from pandorapy.services.friend import repo as frepo
from pandorapy.services.friend import service as fsvc

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
GO_CONF = REPO_ROOT / "services" / "social" / "friend" / "internal" / "conf" / "conf.go"


# ── 1. 配置默认值必须与 Go 逐个相同(含判据符号)──────────────────────────


def _camel_to_snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _go_int_defaults() -> dict[str, tuple[str, int]]:
    """解析 Go 的 Defaults() → {python 字段名: (判据符号, 默认值)}。

    ★ 解析 Go 源码而不是把数字抄进测试:抄一遍就多一个会漂移的真相。
    ★ **判据符号也要抓**:friend 里 rate_quota_per_min 用的是 `== 0`(负值 = 关闭),
      其余全是 `<= 0`。只比默认值、不比符号的话,把 `==` 抄成 `<=` 这种改动
      测试照样绿,而 `rate_quota_per_min: -1` 会在两个实现上跑出不同行为。
    """
    src = GO_CONF.read_text(encoding="utf-8")
    pattern = re.compile(
        r"if c\.Friend\.(?P<field>\w+) (?P<op><=|==) 0 \{\s*"
        r"c\.Friend\.(?P=field) = (?P<value>\d+)\s*\}",
        re.M,
    )
    return {
        _camel_to_snake(m.group("field")): (m.group("op"), int(m.group("value")))
        for m in pattern.finditer(src)
    }


def test_friend_conf_defaults_match_go() -> None:
    """逐字段比对 Go 的 Defaults()。分叉的后果是两边都不报错地跑出不同行为。"""
    go = _go_int_defaults()
    assert go, "没能从 Go conf.go 解析出任何默认值 —— 解析规则该跟着 Go 改"
    cfg = fconf.Config()
    cfg.apply_defaults()
    for field, (_op, expected) in go.items():
        assert getattr(cfg.friend, field) == expected, f"friend.{field} 默认值与 Go 不一致"


def test_friend_conf_covers_every_go_default_field() -> None:
    """Go 设了默认值的字段,Python 必须都建模 —— 漏一个就是"配了却不生效"。"""
    for field in _go_int_defaults():
        assert field in fconf.FriendConf.model_fields, f"FriendConf 缺字段 {field}"


def test_friend_conf_negative_follows_go_operator() -> None:
    """★ 判据符号:`<= 0` 的字段负值要被兜成默认,`== 0` 的字段负值必须**原样保留**。

    rate_quota_per_min 的注释里定义了「负值 = 关闭限流」。抄成 `<= 0` 的后果:
    写着 -1 的 yaml 在 Go 上不限流、在 Python 上每分钟 10 条就开始回
    ERR_RATE_LIMITED,而两边都不报错。
    """
    for field, (op, default) in _go_int_defaults().items():
        cfg = fconf.Config()
        setattr(cfg.friend, field, -1)
        cfg.apply_defaults()
        actual = getattr(cfg.friend, field)
        if op == "<=":
            assert actual == default, f"friend.{field}: Go 是 `<= 0`,负值应兜成 {default}"
        else:
            assert actual == -1, f"friend.{field}: Go 是 `== 0`,负值必须保留(= 显式关闭)"


def test_friend_conf_recommend_limit_clamped_like_go() -> None:
    """推荐数硬上限 20(防爆量)。Go 的钳位在 Defaults 里,Python 必须同值。"""
    src = GO_CONF.read_text(encoding="utf-8")
    m = re.search(r"if c\.Friend\.RecommendLimit > (\d+) \{", src)
    assert m, "Go 的 RecommendLimit 钳位写法变了 —— 先修正则"
    go_max = int(m.group(1))
    assert go_max == fconf.RECOMMEND_MAX_LIMIT
    cfg = fconf.Config()
    cfg.friend.recommend_limit = 999
    cfg.apply_defaults()
    assert cfg.friend.recommend_limit == go_max


def test_friend_conf_sweep_interval_default_matches_go() -> None:
    """sweep_interval=0 会让 safego.loop 判非法直接返回 —— **清理循环根本不跑**。"""
    src = GO_CONF.read_text(encoding="utf-8")
    m = re.search(r"c\.Friend\.SweepInterval = config\.Duration\((\d+) \* time\.Minute\)", src)
    assert m, "Go 的 SweepInterval 默认值写法变了 —— 先修正则"
    cfg = fconf.Config()
    cfg.apply_defaults()
    assert cfg.friend.sweep_interval_sec() == int(m.group(1)) * 60
    assert cfg.friend.sweep_interval_sec() == fconf.DEFAULT_SWEEP_INTERVAL_SEC


def test_friend_conf_ports_match_go() -> None:
    """端口是契约:Envoy cluster / run_services.ps1 端口检查都钉在 20004/21004。"""
    src = GO_CONF.read_text(encoding="utf-8")
    assert f'c.Server.Grpc.Addr = "{fconf.DEFAULT_GRPC_ADDR}"' in src
    assert f'c.Server.Http.Addr = "{fconf.DEFAULT_HTTP_ADDR}"' in src


def test_friend_retention_mode_validation() -> None:
    """拼错的模式必须启动期报错,而不是静默回落 report_only(§9.24)。"""
    cfg = fconf.Config()
    cfg.friend.retention_mode = "delet"
    with pytest.raises(ValueError):
        cfg.friend.validate_retention_mode()
    # 运行期取值方向相反:配错绝不能去删数据。
    assert cfg.friend.retention_mode_parsed() is dbguard.Mode.REPORT_ONLY
    cfg.friend.retention_mode = "delete"
    cfg.friend.validate_retention_mode()
    assert cfg.friend.retention_mode_parsed() is dbguard.Mode.DELETE


# ── 夹具 ─────────────────────────────────────────────────────────────────────


class FakeContext:
    """最小 ServicerContext:只需要 invocation_metadata()。"""

    def __init__(self, player_id: int = 0) -> None:
        self._md = (
            ((interceptors.METADATA_KEY_PLAYER_ID, str(player_id)),) if player_id else ()
        )

    def invocation_metadata(self):  # noqa: ANN201
        return self._md


class FakeSnowflake:
    """确定性发号器 —— 断言"新建请求用的是预生成 ID"需要它可预测。"""

    def __init__(self, start: int = 5000) -> None:
        self._next = start

    def generate(self) -> int:
        self._next += 1
        return self._next


class FakeRepo:
    """内存 repo。方法名与 MySQLFriendRepo 一一对应(名字错了这里会 AttributeError)。"""

    def __init__(self) -> None:
        self.blocked: set[tuple[int, int]] = set()
        self.friends: set[tuple[int, int]] = set()
        self.friend_count = 0
        self.requests: dict[int, tuple[int, int, int, int]] = {}
        self.created: list[tuple] = []
        self.accept_result: tuple[int, int] = (0, 0)
        self.accept_error: BaseException | None = None
        self.rejected: list[tuple[int, int]] = []
        self.removed: list[tuple[int, int]] = []
        self.blocks_called: list[tuple[int, int, int]] = []
        self.unblocks_called: list[tuple[int, int]] = []
        self.friend_rows: list[tuple[int, int]] = []
        self.incoming_rows: list[tuple[int, int, int]] = []
        self.block_rows: list[tuple[int, int]] = []
        self.mutual: list[tuple[int, int]] = []
        self.random_rows: list[tuple[int, int]] = []
        self.mutual_calls: list[tuple] = []
        self.random_calls: list[tuple] = []
        self.sweep_error: BaseException | None = None
        self.sweep_outcome = dbguard.Outcome(mode=dbguard.Mode.DELETE, matched=3, deleted=3)
        self.pair_guard_deleted = 0
        self.pair_guard_called = False

    async def is_blocked(self, a, b):  # noqa: ANN001
        return (a, b) in self.blocked or (b, a) in self.blocked

    async def are_friends(self, a, b):  # noqa: ANN001
        return (a, b) in self.friends

    async def count_friends(self, player_id):  # noqa: ANN001
        return self.friend_count

    async def create_request(self, request_id, requester_id, target_id, max_incoming):  # noqa: ANN001
        self.created.append((request_id, requester_id, target_id, max_incoming))
        return request_id, True

    async def get_request(self, request_id):  # noqa: ANN001
        return self.requests.get(request_id)

    async def accept_request(self, request_id, actor_id, max_friends):  # noqa: ANN001
        if self.accept_error is not None:
            raise self.accept_error
        return self.accept_result

    async def reject_request(self, request_id, actor_id):  # noqa: ANN001
        self.rejected.append((request_id, actor_id))
        return (0, actor_id)

    async def remove_friend(self, player_id, target_id):  # noqa: ANN001
        self.removed.append((player_id, target_id))

    async def block(self, player_id, blocked_id, max_blocks):  # noqa: ANN001
        self.blocks_called.append((player_id, blocked_id, max_blocks))

    async def unblock(self, player_id, blocked_id):  # noqa: ANN001
        self.unblocks_called.append((player_id, blocked_id))

    async def list_friends(self, player_id):  # noqa: ANN001
        return self.friend_rows

    async def list_incoming_requests(self, player_id):  # noqa: ANN001
        return self.incoming_rows

    async def list_blocks(self, player_id):  # noqa: ANN001
        return self.block_rows

    async def recommend_by_mutual(self, player_id, exclude, limit):  # noqa: ANN001
        self.mutual_calls.append((player_id, list(exclude), limit))
        return self.mutual[:limit]

    async def recommend_random(self, player_id, exclude, limit):  # noqa: ANN001
        self.random_calls.append((player_id, list(exclude), limit))
        return self.random_rows[:limit]

    async def sweep_terminal_requests_before(self, mode, retention_days, limit):  # noqa: ANN001
        if self.sweep_error is not None:
            raise self.sweep_error
        return self.sweep_outcome

    async def delete_pair_guards_before(self, retention_days, limit):  # noqa: ANN001
        self.pair_guard_called = True
        return self.pair_guard_deleted


class FakePusher:
    def __init__(self) -> None:
        self.sent: list[tuple[int, object]] = []
        self.error: BaseException | None = None

    async def push_friend_event(self, to_player_id, evt):  # noqa: ANN001
        if self.error is not None:
            raise self.error
        self.sent.append((to_player_id, evt))


class FakeOnline:
    def __init__(self, table: dict) -> None:
        self.table = table
        self.calls: list[list[int]] = []

    async def batch_online(self, player_ids):  # noqa: ANN001
        self.calls.append(list(player_ids))
        return self.table


class FakeQuota:
    """★ 契约是 `(ok, exc)` —— 与 `redisx.ActionQuota.allow` 一致。

    原替身**抛异常**来模拟 Redis 故障,而真实的 `ActionQuota` 从不抛
    (它内部把故障转成返回值)。于是 `test_rate_quota_failure_is_fail_open`
    一直在验证一条**生产上走不到**的 except 分支,而真实的故障路径没人测。

    `error=` 保留:它模拟的是"连 await 都被打断"(取消穿透用例需要真抛)。
    Redis 故障用 `fault=`。
    """

    def __init__(
        self,
        allow: bool = True,
        error: BaseException | None = None,
        fault: Exception | None = None,
    ) -> None:
        self._allow = allow
        self._error = error
        self._fault = fault
        self.calls: list[tuple[str, int]] = []

    async def allow(self, action, subject):  # noqa: ANN001
        self.calls.append((action, subject))
        if self._error is not None:
            raise self._error
        if self._fault is not None:
            return True, self._fault      # fail-open：放行 + 把故障交回调用方
        return self._allow, None


def _cfg() -> fconf.FriendConf:
    cfg = fconf.Config()
    cfg.apply_defaults()
    return cfg.friend


def _uc(repo: FakeRepo, pusher=None, online=None) -> fbiz.FriendUsecase:  # noqa: ANN001
    return fbiz.FriendUsecase(repo, pusher, online, _cfg())


# ── 2. 鉴权边界(R5)──────────────────────────────────────────────────────


def test_no_request_message_carries_caller_identity() -> None:
    """★ R5 的结构保证:请求体里根本没有"我是谁"的字段。

    有那个字段就迟早有人拿它当身份 —— 本服务的入参只有 target_player_id /
    request_id,身份只能来自鉴权上下文。这条用例把这个前提钉死。
    """
    for msg in (
        friend_pb2.AddFriendRequest,
        friend_pb2.AcceptFriendRequest,
        friend_pb2.RejectFriendRequest,
        friend_pb2.RemoveFriendRequest,
        friend_pb2.BlockRequest,
        friend_pb2.UnblockRequest,
        friend_pb2.ListFriendsRequest,
        friend_pb2.ListFriendRequestsRequest,
        friend_pb2.ListBlocksRequest,
        friend_pb2.RecommendFriendsRequest,
    ):
        fields = {f.name for f in msg.DESCRIPTOR.fields}
        assert "player_id" not in fields, f"{msg.__name__} 不该有自报身份字段"


@pytest.mark.parametrize(
    ("method", "request_factory"),
    [
        ("AddFriend", lambda: friend_pb2.AddFriendRequest(target_player_id=2)),
        ("AcceptFriend", lambda: friend_pb2.AcceptFriendRequest(request_id=1)),
        ("RejectFriend", lambda: friend_pb2.RejectFriendRequest(request_id=1)),
        ("RemoveFriend", lambda: friend_pb2.RemoveFriendRequest(target_player_id=2)),
        ("Block", lambda: friend_pb2.BlockRequest(target_player_id=2)),
        ("Unblock", lambda: friend_pb2.UnblockRequest(target_player_id=2)),
        ("ListFriends", friend_pb2.ListFriendsRequest),
        ("ListFriendRequests", friend_pb2.ListFriendRequestsRequest),
        ("ListBlocks", friend_pb2.ListBlocksRequest),
        ("RecommendFriends", friend_pb2.RecommendFriendsRequest),
    ],
)
async def test_anonymous_caller_is_unauthorized(method, request_factory) -> None:  # noqa: ANN001
    """全部 10 个 RPC:拿不到 player_id 一律 ERR_UNAUTHORIZED,且**不碰 repo**。

    直连内网端口(绕过 Envoy)时,匿名不能被当成"某个玩家"。
    """
    repo = FakeRepo()
    svc = fsvc.FriendService(_uc(repo), FakeSnowflake())
    resp = await getattr(svc, method)(request_factory(), FakeContext(player_id=0))
    assert resp.code == errcode_pb2.ERR_UNAUTHORIZED
    assert not repo.created and not repo.removed and not repo.blocks_called


async def test_business_failure_is_in_band_not_grpc_error() -> None:
    """业务失败必须是 body 里的 code + gRPC OK,不是 abort。

    改成 abort 的话客户端走"网络错误"分支,ERR_FRIEND_LIMIT 这类要给玩家看
    具体原因的码就全丢了。
    """
    repo = FakeRepo()
    repo.friends.add((1, 2))
    svc = fsvc.FriendService(_uc(repo), FakeSnowflake())
    resp = await svc.AddFriend(
        friend_pb2.AddFriendRequest(target_player_id=2), FakeContext(player_id=1)
    )
    assert resp.code == errcode_pb2.ERR_FRIEND_ALREADY_ADDED
    assert resp.request_id == 0


async def test_zero_target_is_invalid_arg() -> None:
    repo = FakeRepo()
    svc = fsvc.FriendService(_uc(repo), FakeSnowflake())
    resp = await svc.AddFriend(
        friend_pb2.AddFriendRequest(target_player_id=0), FakeContext(player_id=1)
    )
    assert resp.code == errcode_pb2.ERR_INVALID_ARG
    assert not repo.created


async def test_add_friend_uses_caller_from_context() -> None:
    """requester 取自上下文,并把预生成的 snowflake ID 传给 repo。"""
    repo = FakeRepo()
    sf = FakeSnowflake(start=7000)
    svc = fsvc.FriendService(_uc(repo), sf)
    resp = await svc.AddFriend(
        friend_pb2.AddFriendRequest(target_player_id=22), FakeContext(player_id=11)
    )
    assert resp.code == errcode_pb2.OK
    assert resp.request_id == 7001
    assert repo.created == [(7001, 11, 22, 200)]


# ── 3. 推送方向(推送原则 2)────────────────────────────────────────────────


async def test_request_push_goes_to_target_not_requester() -> None:
    """★ 好友申请通知发给**接收方**。发反了的话接收方永远收不到,
    而发起者收到一条"你收到了自己的申请"。"""
    repo = FakeRepo()
    pusher = FakePusher()
    uc = _uc(repo, pusher)
    await uc.add_friend(11, 22, 900)
    assert [to for to, _ in pusher.sent] == [22]
    evt = pusher.sent[0][1]
    assert evt.by_player_id == 11
    assert evt.reason == friend_pb2.FRIEND_EVENT_REASON_REQUEST_RECEIVED
    assert evt.ts_ms > 0


async def test_accept_push_goes_to_requester() -> None:
    """接受通知发给**发起方**(不是操作者自己)。"""
    repo = FakeRepo()
    repo.requests[900] = (900, 11, 22, frepo.REQUEST_STATUS_PENDING)
    repo.accept_result = (11, 22)
    pusher = FakePusher()
    uc = _uc(repo, pusher)
    await uc.accept_friend(22, 900)
    assert [to for to, _ in pusher.sent] == [11]
    assert pusher.sent[0][1].reason == friend_pb2.FRIEND_EVENT_REASON_REQUEST_ACCEPTED


async def test_reject_pushes_nothing() -> None:
    """拒绝不推送(业界惯例)。推了就是把"你被拒绝了"直接怼到玩家脸上。"""
    repo = FakeRepo()
    repo.requests[901] = (901, 11, 22, frepo.REQUEST_STATUS_PENDING)
    pusher = FakePusher()
    uc = _uc(repo, pusher)
    await uc.reject_friend(22, 901)
    assert pusher.sent == []
    assert repo.rejected == [(901, 22)]


async def test_push_failure_does_not_fail_the_operation() -> None:
    """★ 推送是弱依赖:好友申请已经落库了,通知发不出去不能把整笔报失败 ——
    否则玩家会重复点"添加",而每次都真的成功。"""
    repo = FakeRepo()
    pusher = FakePusher()
    pusher.error = RuntimeError("kafka down")
    uc = _uc(repo, pusher)
    assert await uc.add_friend(11, 22, 900) == 900


async def test_non_target_gets_not_found_not_unauthorized() -> None:
    """★ 非 target 处理申请一律 NOT_FOUND。

    回 UNAUTHORIZED 等于确认"这条申请确实存在" —— 可用来探测他人社交关系。
    """
    repo = FakeRepo()
    repo.requests[902] = (902, 11, 22, frepo.REQUEST_STATUS_PENDING)
    uc = _uc(repo)
    for call in (uc.accept_friend(33, 902), uc.reject_friend(33, 902)):
        with pytest.raises(errcode.PandoraError) as exc:
            await call
        assert exc.value.code == errcode.ErrFriendNotFound


async def test_accept_lost_race_propagates_not_found() -> None:
    """预检通过、权威步骤判定并发丢工作 → 仍是 NOT_FOUND,且**不推送**。

    推了就是"假成功":玩家收到"XX 接受了你的申请",而好友边根本没建。
    """
    repo = FakeRepo()
    repo.requests[903] = (903, 11, 22, frepo.REQUEST_STATUS_PENDING)
    repo.accept_error = errcode.PandoraError(errcode.ErrFriendNotFound, "lost")
    pusher = FakePusher()
    uc = _uc(repo, pusher)
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.accept_friend(22, 903)
    assert exc.value.code == errcode.ErrFriendNotFound
    assert pusher.sent == []


# ── 4. 限流门 ────────────────────────────────────────────────────────────────


async def test_rate_quota_rejects_before_any_write() -> None:
    """★ 超配额必须**先于一切读写**返回 —— 放在写库之后等于没限流。"""
    repo = FakeRepo()
    uc = _uc(repo)
    uc.set_rate_quota(FakeQuota(allow=False))
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.add_friend(11, 22, 900)
    assert exc.value.code == errcode.ErrRateLimited
    assert repo.created == []


async def test_rate_quota_failure_is_fail_open() -> None:
    """限流是背压门不是权威门:判定失败一律放行(§9.20 限流不得卡玩家)。"""
    repo = FakeRepo()
    uc = _uc(repo)
    uc.set_rate_quota(FakeQuota(fault=RuntimeError("redis down")))
    assert await uc.add_friend(11, 22, 900) == 900


async def test_rate_quota_cancellation_propagates() -> None:
    """★ 取消必须穿透 fail-open 的宽 except,否则停机时排空不了在途请求。"""
    repo = FakeRepo()
    uc = _uc(repo)
    uc.set_rate_quota(FakeQuota(error=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        await uc.add_friend(11, 22, 900)


# ── 5. 只读投影 ──────────────────────────────────────────────────────────────


async def test_list_friends_fills_online_and_leaves_nickname_empty() -> None:
    """nickname 由客户端向 player 服务解析(§5.8);在线态经 locator 填。"""
    repo = FakeRepo()
    repo.friend_rows = [(21, 111), (22, 222)]
    # 刻意用 locator_client 的真实 OnlineStatus 结构:字段名写错时这里会红,
    # 而自造一个 duck-type 假对象只会验证测试自己。
    online = FakeOnline({21: flocator.OnlineStatus(online=True, last_seen_ms=999)})
    uc = _uc(repo, online=online)
    friends = await uc.list_friends(1)
    assert [f.player_id for f in friends] == [21, 22]
    assert friends[0].is_online and friends[0].last_seen_ms == 999
    assert friends[0].since_ms == 111
    # 查不到的按离线 —— 绝不能默认在线(面板一片假在线,点进去全是空)
    assert not friends[1].is_online
    assert all(f.nickname == "" for f in friends)


async def test_list_friends_skips_locator_when_empty() -> None:
    """没有好友时不打 locator —— 一次注定返回空的 RPC 也是一次超时风险。"""
    repo = FakeRepo()
    online = FakeOnline({})
    uc = _uc(repo, online=online)
    assert await uc.list_friends(1) == []
    assert online.calls == []


async def test_recommend_excludes_self_and_accumulates_picked() -> None:
    """★ exclude 必须带上自己,并把已选中的追加进去 ——
    否则会把自己推荐给自己,或让第二条策略重复推同一个人。"""
    repo = FakeRepo()
    repo.mutual = [(31, 3)]
    repo.random_rows = [(32, 0)]
    uc = _uc(repo)
    recs = await uc.recommend_friends(7, limit=2, exclude=[99])
    assert [r.player_id for r in recs] == [31, 32]
    assert recs[0].mutual_friend_count == 3
    assert repo.mutual_calls[0][1] == [99, 7]
    # 第二条策略拿到的 exclude 必须已经含第一条选中的 31
    assert repo.random_calls[0][1] == [99, 7, 31]


async def test_recommend_limit_is_clamped() -> None:
    """请求填多大都不超过硬上限 —— 客户端可以填 10 万。"""
    repo = FakeRepo()
    repo.mutual = [(i, 0) for i in range(100)]
    uc = _uc(repo)
    recs = await uc.recommend_friends(7, limit=100_000, exclude=[])
    assert len(recs) == fconf.RECOMMEND_MAX_LIMIT


# ── 6. 自我操作 ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize("op", ["add_friend", "remove_friend", "block", "unblock"])
async def test_self_operations_are_invalid_arg(op: str) -> None:
    """加/删/拉黑自己都必须拒 —— 放过去会在好友图里造出自环。"""
    repo = FakeRepo()
    uc = _uc(repo)
    fn = getattr(uc, op)
    with pytest.raises(errcode.PandoraError) as exc:
        await (fn(1, 1, 900) if op == "add_friend" else fn(1, 1))
    assert exc.value.code == errcode.ErrInvalidArg


# ── 7. sweep 兜错(§9.24)────────────────────────────────────────────────────


async def test_sweep_continues_to_pair_guards_after_first_failure() -> None:
    """★ 第一件事失败不能让整轮 return —— 否则 pair 守卫行**永远不被清**,
    而它随社交图 O(n²) 增长(R9 复审 P1)。"""
    repo = FakeRepo()
    repo.sweep_error = RuntimeError("table gone")
    repo.pair_guard_deleted = 7
    uc = _uc(repo)
    await uc.sweep_terminal_requests()  # 不抛
    assert repo.pair_guard_called


async def test_sweep_cancellation_propagates() -> None:
    """取消穿透:停机时清理循环必须能真的停下。"""
    repo = FakeRepo()
    repo.sweep_error = asyncio.CancelledError()
    uc = _uc(repo)
    with pytest.raises(asyncio.CancelledError):
        await uc.sweep_terminal_requests()
