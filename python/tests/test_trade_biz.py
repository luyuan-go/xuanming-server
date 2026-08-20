"""trade 业务逻辑测试 —— 打 fakeredis(真 Lua + 真 WATCH/MULTI/EXEC)。

用 fakeredis 而不是 mock repo:
    这个服务的正确性几乎全在 Redis 的原子语义上 —— Lua 配额脚本、WATCH 乐观锁、
    SREM 幂等。mock 掉 repo 就等于把被测对象换成我对 Redis 的想象。
    fakeredis 会真的执行 Lua 脚本和事务语义,所以能验到真东西。
    (对应 Go 侧用 alicebob/miniredis 的同一意图,10 个模块在用。)

重点覆盖 INC-20260722-001 那次事故的不变量 —— 这些是移植时最不能"简化"掉的:
  - SELLER_CONFIRMED 是结算围栏:Cancel / 惰性过期 / 配额清理一律拒
  - Settle 在**锁外**执行,意图先落库
  - 结算瞬时失败 → 停留 SELLER_CONFIRMED,可重试收敛,绝不回滚
  - 结算幂等键恒为 order_id
"""

from __future__ import annotations


import pytest
from pandora.trade.v1 import trade_pb2

from pandorapy import errcode
from pandorapy.services.trade import biz as tbiz
from pandorapy.services.trade import conf as tconf
from pandorapy.services.trade import data as tdata

_S = trade_pb2.OrderState


class FakeSnowflake:
    def __init__(self, start: int = 700000) -> None:
        self._next = start

    def generate(self) -> int:
        v = self._next
        self._next += 1
        return v


class RecordingLedger:
    """记录每次 settle 的幂等键,可编程失败。"""

    def __init__(self, fail_with: BaseException | None = None) -> None:
        self.calls: list[tuple[int, int]] = []  # (order_id, idempotency_key)
        self.fail_with = fail_with

    async def settle(self, order, idempotency_key: int) -> None:
        self.calls.append((order.order_id, idempotency_key))
        if self.fail_with is not None:
            raise self.fail_with


class RecordingAudit:
    def __init__(self) -> None:
        self.states: list[int] = []

    async def push_audit(self, order) -> None:
        self.states.append(order.state)


@pytest.fixture
async def rdb():
    """每个用例一个**独立**的 fakeredis server。

    ⚠️ 必须显式传 server= —— `fakeredis.FakeRedis()` 默认连的是进程级共享 server,
    于是用例之间数据互相泄漏。本文件里每个用例的 FakeSnowflake 都从 700000 起,
    共享 server 时 order_id 会撞上前一个用例的残留订单,表现为「单独跑过、全量跑挂」
    这种最难查的形态(实测踩到:4 个用例挂在这上面)。

    另外需要 lupa:`fakeredis[lua]`。没装 lua extra 时 EVALSHA 会报
    "unknown command 'evalsha'" —— 而本服务的配额上限全靠 Lua 原子脚本,
    等于把最关键的一组用例整批变红。
    """
    fakeredis = pytest.importorskip("fakeredis")
    pytest.importorskip("lupa", reason="Lua 脚本需要 fakeredis[lua]")
    server = fakeredis.FakeServer()
    client = fakeredis.aioredis.FakeRedis(server=server, decode_responses=False)
    try:
        yield client
    finally:
        await client.aclose()


def _cfg(**overrides) -> tconf.TradeConf:
    c = tconf.TradeConf(**overrides)
    full = tconf.Config(trade=c)
    full.apply_defaults()
    return full.trade


def _items(*specs: tuple[int, int]) -> list:
    return [trade_pb2.TradeItem(item_config_id=cid, count=n) for cid, n in specs]


async def _make(rdb, *, ledger=None, audit=None, cfg=None):
    repo = tdata.RedisTradeRepo(rdb)
    uc = tbiz.TradeUsecase(
        repo, ledger or RecordingLedger(), audit, FakeSnowflake(), cfg or _cfg()
    )
    return repo, uc


# ── 正常两阶段流程 ────────────────────────────────────────────────────────────


async def test_full_two_phase_flow_completes(rdb) -> None:
    ledger = RecordingLedger()
    audit = RecordingAudit()
    _repo, uc = await _make(rdb, ledger=ledger, audit=audit)

    order_id = await uc.create_order(1001, 2002, _items((5001, 3)), [], price=100)
    assert order_id > 0

    # 买方确认 → BUYER_CONFIRMED
    assert await uc.confirm_order(2002, order_id) == _S.ORDER_STATE_BUYER_CONFIRMED
    # 卖方确认 → 结算 → COMPLETED
    assert await uc.confirm_order(1001, order_id) == _S.ORDER_STATE_COMPLETED

    # ★ 幂等键必须是 order_id(不变量 §9.7)
    assert ledger.calls == [(order_id, order_id)]
    assert _S.ORDER_STATE_COMPLETED in audit.states


async def test_seller_cannot_confirm_before_buyer(rdb) -> None:
    """卖方不能先确认 —— 双确认防单方面成交。"""
    _repo, uc = await _make(rdb)
    order_id = await uc.create_order(1001, 2002, _items((5001, 1)), [], 10)
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.confirm_order(1001, order_id)
    assert exc.value.code == errcode.ErrTradeWrongState


async def test_non_party_cannot_confirm_or_cancel(rdb) -> None:
    """第三方不能确认/取消别人的订单。"""
    _repo, uc = await _make(rdb)
    order_id = await uc.create_order(1001, 2002, _items((5001, 1)), [], 10)
    for fn in (uc.confirm_order, uc.cancel_order):
        with pytest.raises(errcode.PandoraError) as exc:
            await fn(9999, order_id)
        assert exc.value.code == errcode.ErrUnauthorized


# ── ★ 结算围栏(INC-20260722-001)────────────────────────────────────────────


async def test_seller_confirmed_rejects_cancel(rdb) -> None:
    """★ SELLER_CONFIRMED 下 Cancel 必须被拒。

    结算意图已落库,资产转移可能已发生或即将发生;允许取消就会与账本撕裂 ——
    这正是那次事故的形态。
    """
    # 让 settle 瞬时失败,把订单**停在** SELLER_CONFIRMED
    ledger = RecordingLedger(fail_with=RuntimeError("inventory unreachable"))
    _repo, uc = await _make(rdb, ledger=ledger)
    order_id = await uc.create_order(1001, 2002, _items((5001, 1)), [], 10)
    await uc.confirm_order(2002, order_id)
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.confirm_order(1001, order_id)  # 卖方确认 → 意图落库 → settle 失败
    assert exc.value.code == errcode.ErrUnavailable

    # 现在订单停在 SELLER_CONFIRMED
    order = await _repo.get_order(order_id)
    assert order.state == _S.ORDER_STATE_SELLER_CONFIRMED

    # 双方都不能取消
    for pid in (1001, 2002):
        with pytest.raises(errcode.PandoraError) as cexc:
            await uc.cancel_order(pid, order_id)
        assert cexc.value.code == errcode.ErrTradeWrongState
        assert "settling" in cexc.value.msg


async def test_seller_confirmed_does_not_expire(rdb) -> None:
    """★ SELLER_CONFIRMED 不受惰性过期影响。

    置 EXPIRED 会与账本撕裂 —— 资产可能已转移。只允许向 COMPLETED / FAILED 收敛。
    """
    ledger = RecordingLedger(fail_with=RuntimeError("timeout"))
    # order_expire 设成 1ms,让订单立刻"过期"
    _repo, uc = await _make(rdb, ledger=ledger, cfg=_cfg(order_expire="1ms"))
    order_id = await uc.create_order(1001, 2002, _items((5001, 1)), [], 10)

    # 买方确认时订单已过期 → 应被置 EXPIRED
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.confirm_order(2002, order_id)
    assert exc.value.code == errcode.ErrTradeOrderExpired

    # 换一单:在过期前推进到 SELLER_CONFIRMED,再验它不会被过期
    _repo2, uc2 = await _make(
        rdb, ledger=RecordingLedger(fail_with=RuntimeError("timeout")),
        cfg=_cfg(order_expire="10m"),
    )
    oid2 = await uc2.create_order(1003, 2004, _items((5001, 1)), [], 10)
    await uc2.confirm_order(2004, oid2)
    with pytest.raises(errcode.PandoraError):
        await uc2.confirm_order(1003, oid2)
    order = await _repo2.get_order(oid2)
    assert order.state == _S.ORDER_STATE_SELLER_CONFIRMED
    # 手工把它改成已过期,再确认 —— 状态必须仍是 SELLER_CONFIRMED,不能变 EXPIRED
    order.expires_at_ms = 1
    await rdb.set(tdata.order_key(oid2), order.SerializeToString())
    with pytest.raises(errcode.PandoraError):
        await uc2.confirm_order(1003, oid2)
    again = await _repo2.get_order(oid2)
    assert again.state == _S.ORDER_STATE_SELLER_CONFIRMED, (
        "SELLER_CONFIRMED 被惰性过期改掉了 —— 会与资产账本撕裂"
    )


async def test_transient_settle_failure_is_retryable_and_converges(rdb) -> None:
    """★ 结算瞬时失败 → 停留 SELLER_CONFIRMED;重试 Confirm 幂等收敛到 COMPLETED。

    这是事故修复后的恢复路径:**不回滚**,靠重试收敛。
    """
    ledger = RecordingLedger(fail_with=RuntimeError("net timeout"))
    _repo, uc = await _make(rdb, ledger=ledger)
    order_id = await uc.create_order(1001, 2002, _items((5001, 1)), [], 10)
    await uc.confirm_order(2002, order_id)

    with pytest.raises(errcode.PandoraError) as exc:
        await uc.confirm_order(1001, order_id)
    assert exc.value.code == errcode.ErrUnavailable
    assert (await _repo.get_order(order_id)).state == _S.ORDER_STATE_SELLER_CONFIRMED

    # inventory 恢复,任一方重试都能推进
    ledger.fail_with = None
    assert await uc.confirm_order(2002, order_id) == _S.ORDER_STATE_COMPLETED
    # 幂等键始终是 order_id(重试也一样)
    assert {k for _, k in ledger.calls} == {order_id}


async def test_insufficient_resources_goes_to_failed(rdb) -> None:
    """余额/物品不足(资产未动)→ FAILED 终态。"""
    ledger = RecordingLedger(
        fail_with=errcode.PandoraError(errcode.ErrTradeInsufficient, "not enough")
    )
    _repo, uc = await _make(rdb, ledger=ledger)
    order_id = await uc.create_order(1001, 2002, _items((5001, 1)), [], 10)
    await uc.confirm_order(2002, order_id)
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.confirm_order(1001, order_id)
    assert exc.value.code == errcode.ErrTradeInsufficient
    assert (await _repo.get_order(order_id)).state == _S.ORDER_STATE_FAILED


async def test_settle_is_called_outside_the_lock(rdb) -> None:
    """★ Settle 必须在锁外执行 —— 事故根因就是它曾在 WATCH 回调里。

    验证方式:settle 期间去读订单,必须能读到(说明没有 WATCH 持有中的事务阻塞),
    且读到的状态已经是 SELLER_CONFIRMED(说明意图**已提交**才调 settle)。
    """
    observed: dict[str, int] = {}

    class ProbeLedger:
        async def settle(self, order, idempotency_key: int) -> None:
            # settle 执行期间读库 —— 意图必须已经落库
            probe = await tdata.RedisTradeRepo(rdb).get_order(order.order_id)
            observed["state_during_settle"] = probe.state

    _repo, uc = await _make(rdb, ledger=ProbeLedger())
    order_id = await uc.create_order(1001, 2002, _items((5001, 1)), [], 10)
    await uc.confirm_order(2002, order_id)
    assert await uc.confirm_order(1001, order_id) == _S.ORDER_STATE_COMPLETED
    assert observed["state_during_settle"] == _S.ORDER_STATE_SELLER_CONFIRMED, (
        "settle 执行时意图还没落库 —— 回到了事故前的形状"
    )


# ── 配额上限(不变量 §18)──────────────────────────────────────────────────────


async def test_order_limit_enforced_by_lua(rdb) -> None:
    """★ 单玩家订单总量上限由 Lua 原子预留,超限抛 ErrTradeOrderLimit。"""
    _repo, uc = await _make(rdb, cfg=_cfg(max_orders_per_player=3))
    created = []
    for i in range(3):
        created.append(await uc.create_order(1001, 2000 + i, _items((5001, 1)), [], 10))
    assert len(created) == 3
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.create_order(1001, 2099, _items((5001, 1)), [], 10)
    assert exc.value.code == errcode.ErrTradeOrderLimit


async def test_limit_prunes_dead_slots_then_retries(rdb) -> None:
    """满员时先清死成员再重试一次 —— 取消后应能重新下单(自愈)。"""
    _repo, uc = await _make(rdb, cfg=_cfg(max_orders_per_player=2))
    a = await uc.create_order(1001, 2002, _items((5001, 1)), [], 10)
    await uc.create_order(1001, 2003, _items((5001, 1)), [], 10)
    # 满了
    with pytest.raises(errcode.PandoraError):
        await uc.create_order(1001, 2004, _items((5001, 1)), [], 10)
    # 取消一单(进终态,但索引成员还在)→ 再下单时会被清理并腾出名额
    await uc.cancel_order(1001, a)
    fresh = await uc.create_order(1001, 2004, _items((5001, 1)), [], 10)
    assert fresh > 0


async def test_create_rollback_leaves_no_body_on_slot_failure(rdb) -> None:
    """★ 配额预留失败必须回滚订单主体 —— 不能留下无索引的孤儿。

    写序是"先主体后索引",所以预留失败时主体已经写进去了,必须删掉。
    """
    _repo, uc = await _make(rdb, cfg=_cfg(max_orders_per_player=1))
    await uc.create_order(1001, 2002, _items((5001, 1)), [], 10)
    before = await rdb.dbsize()
    with pytest.raises(errcode.PandoraError):
        await uc.create_order(1001, 2003, _items((5001, 1)), [], 10)
    after = await rdb.dbsize()
    assert after == before, "配额失败后留下了孤儿订单主体"


# ── 参数校验 ──────────────────────────────────────────────────────────────────


async def test_rejects_self_trade(rdb) -> None:
    _repo, uc = await _make(rdb)
    with pytest.raises(errcode.PandoraError, match="cannot trade with self"):
        await uc.create_order(1001, 1001, _items((5001, 1)), [], 10)


async def test_rejects_too_many_items(rdb) -> None:
    _repo, uc = await _make(rdb, cfg=_cfg(max_items_per_order=2))
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.create_order(1001, 2002, _items((1, 1), (2, 1), (3, 1)), [], 10)
    assert exc.value.code == errcode.ErrInvalidArg


async def test_rejects_invalid_item_and_negative_price(rdb) -> None:
    _repo, uc = await _make(rdb)
    with pytest.raises(errcode.PandoraError, match="invalid item"):
        await uc.create_order(1001, 2002, _items((0, 1)), [], 10)
    with pytest.raises(errcode.PandoraError, match="invalid item"):
        await uc.create_order(1001, 2002, _items((5001, 0)), [], 10)
    with pytest.raises(errcode.PandoraError, match="price must be"):
        await uc.create_order(1001, 2002, _items((5001, 1)), [], -1)


async def test_empty_items_rejected_but_buyer_items_optional(rdb) -> None:
    """items 必填(卖家必须交付东西);buyer_items 可空(纯金币购买)。"""
    _repo, uc = await _make(rdb)
    with pytest.raises(errcode.PandoraError, match="items required"):
        await uc.create_order(1001, 2002, [], _items((5001, 1)), 10)
    assert await uc.create_order(1001, 2002, _items((5001, 1)), [], 100) > 0


# ── 频率配额 ──────────────────────────────────────────────────────────────────


async def test_rate_quota_rejects_before_side_effects(rdb) -> None:
    """★ 配额拒绝必须发生在任何副作用之前 —— 不能先写订单再拒。"""

    class DenyAll:
        async def allow(self, action: str, subject: int) -> tuple[bool, Exception | None]:
            return False, None

    _repo, uc = await _make(rdb)
    uc.set_rate_quota(DenyAll())
    before = await rdb.dbsize()
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.create_order(1001, 2002, _items((5001, 1)), [], 10)
    assert exc.value.code == errcode.ErrRateLimited
    assert await rdb.dbsize() == before, "配额拒绝前已经产生了副作用"


async def test_rate_quota_failure_is_fail_open(rdb) -> None:
    """配额判定失败 fail-open 放行 —— 它是背压门不是权威门。"""

    class Broken:
        # 契约是 `(ok, exc)`:真实的 ActionQuota 从不抛,故障走返回值。
        async def allow(self, action: str, subject: int) -> tuple[bool, Exception | None]:
            return True, RuntimeError("quota store down")

    _repo, uc = await _make(rdb)
    uc.set_rate_quota(Broken())
    assert await uc.create_order(1001, 2002, _items((5001, 1)), [], 10) > 0


# ── 分页 ──────────────────────────────────────────────────────────────────────


async def test_list_my_orders_paginates_descending(rdb) -> None:
    _repo, uc = await _make(rdb)
    ids = [await uc.create_order(1001, 2000 + i, _items((5001, 1)), [], 10) for i in range(5)]
    page1, next1 = await uc.list_my_orders(1001, False, 0, 2)
    assert [o.order_id for o in page1] == sorted(ids, reverse=True)[:2]
    assert next1 == page1[-1].order_id
    page2, _next2 = await uc.list_my_orders(1001, False, next1, 2)
    assert [o.order_id for o in page2] == sorted(ids, reverse=True)[2:4]


async def test_list_limit_is_clamped() -> None:
    assert tbiz._clamp_limit(0) == tbiz.DEFAULT_PAGE_LIMIT
    assert tbiz._clamp_limit(-5) == tbiz.DEFAULT_PAGE_LIMIT
    assert tbiz._clamp_limit(9999) == tbiz.MAX_PAGE_LIMIT
    assert tbiz._clamp_limit(10) == 10


async def test_active_only_filters_terminal(rdb) -> None:
    _repo, uc = await _make(rdb)
    a = await uc.create_order(1001, 2002, _items((5001, 1)), [], 10)
    b = await uc.create_order(1001, 2003, _items((5001, 1)), [], 10)
    await uc.cancel_order(1001, a)
    active, _ = await uc.list_my_orders(1001, True, 0, 50)
    assert [o.order_id for o in active] == [b]


# ── key 格式(跨语言契约)─────────────────────────────────────────────────────


def test_redis_keys_match_go_format() -> None:
    """★ key 格式必须与 Go 侧逐字一致 —— 迁移期两个实现读写同一批 key。

    order key 的 hashtag {} 必须保留:它确保同订单的 key 落同一 cluster slot。
    """
    assert tdata.order_key(12345) == "pandora:trade:order:{12345}"
    assert tdata.player_key(1001) == "pandora:trade:player:1001"


# ── ★ 没有真实账本时必须拒绝构造 ────────────────────────────────────────────


async def test_missing_ledger_is_rejected_at_construction(rdb) -> None:
    """★ 不注入 ResourceLedger 且未显式授权 → **构造就失败**。

    Noop 账本会让成交"不真实扣转"背包 / 货币,而订单状态、审计流水、客户端提示
    全都显示成功 —— 这是审计点名过的降级风险,且没有任何运行期信号。

    这道闸原先只写在 Go 的 main.go 里,Python 侧 trade **没有 main**,
    docstring 却声称"生产由 main 强制 fail-fast"。闸下沉到构造函数之后,
    "忘记接账本"在结构上不可能通过 —— 不依赖任何人记得。
    """
    from pandorapy import errcode

    repo = tdata.RedisTradeRepo(rdb)
    with pytest.raises(errcode.PandoraError) as exc:
        tbiz.TradeUsecase(repo, None, None, FakeSnowflake(), _cfg())
    assert exc.value.code == errcode.ErrInvalidState
    assert "allow_noop_ledger" in str(exc.value)


async def test_noop_ledger_allowed_only_when_explicitly_enabled(rdb) -> None:
    """显式开 allow_noop_ledger 才放行(联调 / 单测路径仍可用)。"""
    cfg = _cfg(allow_noop_ledger=True)
    uc = tbiz.TradeUsecase(tdata.RedisTradeRepo(rdb), None, None, FakeSnowflake(), cfg)
    assert isinstance(uc._ledger, tbiz.NoopResourceLedger)  # noqa: SLF001


async def test_real_action_quota_plugs_into_the_rate_seam(rdb) -> None:
    """★ `redisx.ActionQuota` 必须能直接插进 trade 的频率配额接缝。

    这个接缝(`set_rate_quota`)一直存在,但**没有任何可注入的实现** ——
    `rate_quota_per_min` 配了也不生效,而配置看起来是有频率限制的。
    本用例同时钉住两件事:协议形状对得上、且真的会拒。

    总量闸(max_orders_per_player)只限「同时挂多少」,挡不住
    「下单-撤单-再下单」的循环 —— 每轮都产生托管写 + 流水行,频率闸补的正是这一维。
    """
    from pandorapy import errcode, redisx

    _repo, uc = await _make(rdb)
    uc.set_rate_quota(redisx.ActionQuota(rdb, "trade", limit=2, window_sec=60))

    seller, buyer = 7001, 7002
    ok1 = await uc.create_order(seller, buyer, _items((5001, 1)), [], price=10)
    ok2 = await uc.create_order(seller, buyer, _items((5001, 1)), [], price=10)
    assert ok1 > 0 and ok2 > 0

    with pytest.raises(errcode.PandoraError) as exc:
        await uc.create_order(seller, buyer, _items((5001, 1)), [], price=10)
    assert exc.value.code == errcode.ErrRateLimited


# ── 失败路径必须回传"订单现在停在哪"(与 Go 逐路径一致)─────────────────────
#
# Go 的 biz.ConfirmOrder 是 `(state, err)` 双返回,**失败时 state 仍然有效**,
# service 层原样透传(service/trade.go:61-62)。Python 用异常传播,不显式挂上就丢成 0,
# 而 0 = ORDER_STATE_UNSPECIFIED,与"服务端不知道"无法区分 —— 这两件事对客户端
# 却是相反的指令:SELLER_CONFIRMED 必须重试(钱货可能已过户),FAILED/EXPIRED 别重试。
#
# 拆掉 biz.py 里的 _with_state(...) 这几条会当场红。


async def test_transient_failure_reports_seller_confirmed(rdb) -> None:
    """★ 结算在途(可能已生效)—— 必须回 SELLER_CONFIRMED,客户端据此重试。

    对应 Go trade.go:403。
    """
    ledger = RecordingLedger(fail_with=RuntimeError("net timeout"))
    _repo, uc = await _make(rdb, ledger=ledger)
    order_id = await uc.create_order(1001, 2002, _items((5001, 1)), [], 10)
    await uc.confirm_order(2002, order_id)
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.confirm_order(1001, order_id)
    assert exc.value.order_state == _S.ORDER_STATE_SELLER_CONFIRMED, (
        "结算在途却回了 UNSPECIFIED —— 客户端会当订单没动而放弃,而资产可能已经过户"
    )


async def test_insufficient_reports_failed(rdb) -> None:
    """资产未动的终态 —— 回 FAILED,客户端不该重试。对应 Go trade.go:398。"""
    ledger = RecordingLedger(
        fail_with=errcode.PandoraError(errcode.ErrTradeInsufficient, "not enough")
    )
    _repo, uc = await _make(rdb, ledger=ledger)
    order_id = await uc.create_order(1001, 2002, _items((5001, 1)), [], 10)
    await uc.confirm_order(2002, order_id)
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.confirm_order(1001, order_id)
    assert exc.value.order_state == _S.ORDER_STATE_FAILED


async def test_expired_reports_expired(rdb) -> None:
    """惰性过期 —— 回 EXPIRED。对应 Go trade.go:351。"""
    _repo, uc = await _make(rdb, cfg=_cfg(order_expire="1ms"))
    order_id = await uc.create_order(1001, 2002, _items((5001, 1)), [], 10)
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.confirm_order(2002, order_id)
    assert exc.value.code == errcode.ErrTradeOrderExpired
    assert exc.value.order_state == _S.ORDER_STATE_EXPIRED


async def test_pandora_error_order_state_is_declared_and_defaulted() -> None:
    """order_state 必须是**声明并初始化**的字段,读的那侧不用 getattr(..., 默认值)。

    注意这道保护的方向:`PandoraError` 继承 `Exception`,而 Exception 自带 `__dict__`,
    所以 `__slots__` **拦不住写侧拼错**(`exc.oder_state = 3` 照样成功)。
    它拦的是**读侧** —— 字段在 `__init__` 里初始化过,读的地方可以直接写
    `exc.order_state`;一旦读侧拼错就当场 AttributeError,而不是像
    `getattr(exc, "oder_state", 0)` 那样永远悄悄返回 0。
    service.py 正是因此从 getattr 改成了直接属性访问。
    """
    assert "order_state" in errcode.PandoraError.__slots__
    err = errcode.PandoraError(errcode.ErrUnavailable, "x")
    assert err.order_state == 0, "没初始化的话读侧就只能退回 getattr + 默认值"
    with pytest.raises(AttributeError):
        _ = err.oder_state  # 读侧拼错 → 当场炸
