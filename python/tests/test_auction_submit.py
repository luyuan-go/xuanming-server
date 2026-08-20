"""拍卖挂单流程测试。

重点全部是**资产会出错但不报错**的那几条:
  1. ★ 必须沿用权威快照的 canonical order_id(用本地新铸的会绕开冻结幂等)
  2. ★ PENDING 态存在的理由:未冻结的订单不能被撮合看到
  3. ★ 冻结失败要终态化 + 登记 release_pending(可能其实冻成功了)
  4. ★ 名额预留失败必须终态化,不能留可恢复 PENDING(绕过配额)
  5. ★ quantity × price 溢出守卫
"""

from __future__ import annotations

import pytest
from pandora.auction.v1 import auction_pb2
from pandora.inventory.v1 import inventory_pb2

from pandorapy import errcode
from pandorapy.services.auction import submit as sub


class FakeSnowflake:
    def __init__(self, start: int = 900000) -> None:
        self._n = start

    def generate(self) -> int:
        v = self._n
        self._n += 1
        return v


class FakeRepo:
    """claim_order 可编程:返回 (权威快照, 是否已存在)。"""

    def __init__(self) -> None:
        self.claim_result: tuple[sub.OrderRecord | None, bool] = (None, False)
        self.rejected: list[tuple[int, int]] = []
        self.activated: list[tuple[int, int]] = []
        self.confirmed: list[tuple[int, int]] = []
        self.confirm_result = True
        self.orders: dict[int, sub.OrderRecord] = {}

    async def claim_order(self, rec):  # noqa: ANN001
        got, already = self.claim_result
        return got, already

    async def reject_pending_order(self, market_id, order_id, now_ms):  # noqa: ANN001
        self.rejected.append((market_id, order_id))
        return True

    async def activate_order(self, market_id, order_id, now_ms):  # noqa: ANN001
        self.activated.append((market_id, order_id))
        return True

    async def confirm_order_escrow(self, market_id, order_id, now_ms):  # noqa: ANN001
        # 确认 escrow 是撮合前的必经闸(Go: ConfirmOrderEscrow)。
        # 这里恒 True;它返回 False 的那条路径由 test_confirm_escrow_lost_is_fatal 覆盖。
        self.confirmed.append((market_id, order_id))
        return self.confirm_result

    async def get_order(self, market_id, order_id):  # noqa: ANN001
        return self.orders.get(order_id), order_id in self.orders


class RecordingLedger:
    def __init__(self, freeze_fails: bool = False) -> None:
        self.freezes: list[tuple] = []
        self.releases: list[tuple] = []
        self.freeze_fails = freeze_fails

    async def freeze(self, *, owner_id, order_id, side, item_config_id, quantity, price):  # noqa: ANN001
        self.freezes.append((owner_id, order_id, side, item_config_id, quantity, price))
        if self.freeze_fails:
            raise errcode.PandoraError(errcode.ErrUnavailable, "inventory unreachable")

    async def release(self, owner_id, order_id):  # noqa: ANN001
        self.releases.append((owner_id, order_id))


class FakeSlots:
    def __init__(self, full: bool = False) -> None:
        self.full = full
        self.reserved: list[int] = []
        self.released: list[int] = []

    async def reserve(self, rec):  # noqa: ANN001
        if self.full:
            raise errcode.PandoraError(
                errcode.ErrAuctionOrderLimit, "order limit reached for %d", rec.owner_id
            )
        self.reserved.append(rec.order_id)

    async def release(self, rec):  # noqa: ANN001
        self.released.append(rec.order_id)


class Cfg:
    max_quantity_per_order = 1000
    max_price = 1_000_000


def _submitter(repo=None, ledger=None, slots=None, matcher=None):
    return sub.AuctionSubmitter(
        repo or FakeRepo(), ledger or RecordingLedger(), slots or FakeSlots(),
        FakeSnowflake(), Cfg(), matcher=matcher,
    )


async def _submit(s, **kw):
    """跑一次 submit 并取出**权威记录**。

    返回值是 SubmitOutcome 而不是裸记录:`finalize` 区分"本次真的推进了这张单"
    与"只是回放了一份快照",调用方据此决定要不要写订单簿缓存 / 发 audit。
    """
    base = dict(
        owner_id=1001, side=sub.SIDE_SELL, market_id=1, item_config_id=5001,
        quantity=10, price=100, idem_key="k-1", now_ms=1_760_000_000_000,
    )
    base.update(kw)
    return await s.submit(**base)


# ── ★ 1. canonical order_id ─────────────────────────────────────────────────


async def test_uses_canonical_order_id_from_authoritative_snapshot() -> None:
    """★ 拿到权威快照就必须用它的 order_id 去冻结。

    用本地新铸的 order_id 会**绕开冻结幂等** —— 同一次重试冻两次资产。
    这是 Go 侧专门写了注释警告的一条。
    """
    repo = FakeRepo()
    ledger = RecordingLedger()
    # 服务端已有一条 PENDING,order_id 与本次新铸的不同
    canonical = sub.OrderRecord(
        order_id=777777, market_id=1, owner_id=1001, side=sub.SIDE_SELL,
        item_config_id=5001, quantity=10, price=100, status=sub.STATUS_PENDING,
        idempotency_key="k-1",
    )
    repo.claim_result = (canonical, True)

    s = _submitter(repo, ledger)
    await _submit(s)

    assert ledger.freezes, "没有冻结"
    frozen_order_id = ledger.freezes[0][1]
    assert frozen_order_id == 777777, (
        f"用了本地新铸的 order_id {frozen_order_id} 而不是权威的 777777 —— "
        f"冻结幂等被绕开"
    )


async def test_already_terminal_replays_without_freezing() -> None:
    """已终态的重试直接回放,**不再冻结**。"""
    repo = FakeRepo()
    ledger = RecordingLedger()
    slots = FakeSlots()
    repo.claim_result = (
        sub.OrderRecord(order_id=777777, status=sub.STATUS_CANCELED, owner_id=1001),
        True,
    )
    s = _submitter(repo, ledger, slots)
    rec = (await _submit(s)).record
    assert rec.status == sub.STATUS_CANCELED
    assert not ledger.freezes, "已终态还去冻结了"
    assert slots.released == [777777], "终态回放没释放名额"


async def test_already_open_replays_without_freezing() -> None:
    repo = FakeRepo()
    ledger = RecordingLedger()
    repo.claim_result = (
        sub.OrderRecord(order_id=777777, status=sub.STATUS_OPEN, owner_id=1001),
        True,
    )
    s = _submitter(repo, ledger)
    rec = (await _submit(s)).record
    assert rec.status == sub.STATUS_OPEN
    assert not ledger.freezes


async def test_pending_retry_resumes_freeze() -> None:
    """★ PENDING 的重试必须**继续**冻结+激活,而不是直接回放。

    停在 PENDING 说明上次在"登记后、冻结前"崩了 —— 资产还没冻,
    直接回放会让一个未冻结的订单进入撮合。
    """
    repo = FakeRepo()
    ledger = RecordingLedger()
    repo.claim_result = (
        sub.OrderRecord(
            order_id=777777, market_id=1, owner_id=1001, quantity=10, price=100,
            status=sub.STATUS_PENDING,
        ),
        True,
    )
    s = _submitter(repo, ledger)
    await _submit(s)
    assert ledger.freezes, "PENDING 重试没有继续冻结"
    assert repo.activated == [(1, 777777)]


# ── ★ 2/3. 冻结失败的处置 ───────────────────────────────────────────────────


async def test_freeze_failure_rejects_and_releases_escrow() -> None:
    """★ 冻结失败 → 终态化 + 尝试退还 escrow。

    失败结果**可能包含网络不确定性**(也许其实冻成功了只是响应丢了),
    所以必须走 Release(幂等键 order_id)消除永久锁资。
    """
    repo = FakeRepo()
    ledger = RecordingLedger(freeze_fails=True)
    slots = FakeSlots()
    s = _submitter(repo, ledger, slots)

    with pytest.raises(errcode.PandoraError):
        await _submit(s)

    assert repo.rejected, "冻结失败却没有终态化"
    assert slots.released, "冻结失败却没释放名额"
    assert ledger.releases, "冻结失败却没尝试退还 escrow —— 可能永久锁资"
    # 退还用的必须是同一个 order_id(幂等键)
    assert ledger.releases[0][1] == ledger.freezes[0][1]


async def test_freeze_failure_does_not_activate() -> None:
    """冻结失败绝不能激活 —— 未冻结的订单不能进撮合。"""
    repo = FakeRepo()
    s = _submitter(repo, RecordingLedger(freeze_fails=True))
    with pytest.raises(errcode.PandoraError):
        await _submit(s)
    assert not repo.activated


# ── ★ 4. 名额预留失败 ───────────────────────────────────────────────────────


async def test_slot_failure_terminalizes_pending() -> None:
    """★ 名额预留失败必须**条件终态化**。

    留下一个可恢复的 PENDING = 绕过配额的后门:玩家反复重试同一个 idem,
    每次都停在 PENDING,配额永远不生效。
    """
    repo = FakeRepo()
    ledger = RecordingLedger()
    s = _submitter(repo, ledger, FakeSlots(full=True))

    with pytest.raises(errcode.PandoraError) as exc:
        await _submit(s)
    assert exc.value.code == errcode.ErrAuctionOrderLimit
    assert repo.rejected, "名额失败却留下了可恢复的 PENDING"
    assert not ledger.freezes, "名额都没占到就去冻结了"


async def test_slot_reserved_after_pending_persisted() -> None:
    """名额预留在 PENDING 落库**之后** —— 成员始终能按 market+order 回查权威状态。"""
    repo = FakeRepo()
    slots = FakeSlots()
    s = _submitter(repo, RecordingLedger(), slots)
    await _submit(s)
    assert slots.reserved, "没预留名额"


# ── ★ 5. 溢出守卫 ──────────────────────────────────────────────────────────


def test_total_value_overflow_rejected() -> None:
    """★ quantity × price 溢出 int64 必须在入口拒。

    下游 inventory 会算 total = quantity * unit_price —— 溢出后金额回绕,
    可能变成负数或极小值,**不报错**。
    """
    with pytest.raises(errcode.PandoraError, match="overflow"):
        sub.validate_submit(
            owner_id=1, market_id=1, item_config_id=1,
            quantity=2**40, price=2**40,
            idem_key="k", max_quantity=2**62, max_price=2**62,
        )


def test_near_limit_combination_is_allowed() -> None:
    """不溢出的极端组合应当放行(守卫不能过严)。"""
    sub.validate_submit(
        owner_id=1, market_id=1, item_config_id=1,
        quantity=2, price=(2**62) - 1,
        idem_key="k", max_quantity=2**62, max_price=2**62,
    )


# ── 幂等键字符集 ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("key", ["a", "A-1", "x.y:z_1", "a" * 64, "0123456789"])
def test_valid_idempotency_keys(key: str) -> None:
    assert sub.valid_idempotency_key(key)


@pytest.mark.parametrize(
    "key",
    [
        "", "a" * 65,  # 长度越界
        "has space", "中文", "a/b", "a\\b", "a\n",  # 字符集外
        "a'; DROP TABLE x; --",  # 注入面
    ],
)
def test_invalid_idempotency_keys(key: str) -> None:
    """★ 字符集限死 —— 它会进 uk 索引并出现在日志/审计里。"""
    assert not sub.valid_idempotency_key(key)


# ── 入口校验 ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("owner_id", 0),
        ("market_id", 0),
        ("item_config_id", 0),
        ("quantity", 0),
        ("quantity", -1),
        ("quantity", 99999),
        ("price", 0),
        ("price", -1),
        ("price", 9_999_999),
    ],
)
def test_out_of_range_rejected(field: str, value: int) -> None:
    kw = dict(
        owner_id=1, market_id=1, item_config_id=1, quantity=10, price=100,
        idem_key="k", max_quantity=1000, max_price=1_000_000,
    )
    kw[field] = value
    with pytest.raises(errcode.PandoraError):
        sub.validate_submit(**kw)


def test_terminal_status_set() -> None:
    """★ 终态集合必须与 Go 的 isTerminal 逐值一致。

    咬住的不变量:**PARTIAL 不是终态**。它剩余量仍挂在簿上等撮合,
    一旦被算成终态,submit 的回放分支会提前释放 owner 名额 ——
    玩家凭一个还活着的挂单白拿额度,配额恒被穿透且不报错。
    """
    for s in (sub.STATUS_FILLED, sub.STATUS_CANCELED, sub.STATUS_EXPIRED):
        assert sub.is_terminal(s)
    for s in (sub.STATUS_PENDING, sub.STATUS_OPEN, sub.STATUS_PARTIAL):
        assert not sub.is_terminal(s)


def test_status_and_side_match_proto_enum() -> None:
    """★ 状态 / 方向必须逐值等于 proto 生成物,不许手抄。

    咬住的不变量:这两组值同时是落库值与跨服务传输值。抄错一位,
    库里的历史行与对 inventory 的冻结方向会同时错,而两边都不报错。
    """
    assert sub.STATUS_PENDING == auction_pb2.AUCTION_ORDER_STATUS_UNSPECIFIED
    assert sub.STATUS_OPEN == auction_pb2.AUCTION_ORDER_STATUS_OPEN
    assert sub.STATUS_PARTIAL == auction_pb2.AUCTION_ORDER_STATUS_PARTIALLY_FILLED
    assert sub.STATUS_FILLED == auction_pb2.AUCTION_ORDER_STATUS_FILLED
    assert sub.STATUS_CANCELED == auction_pb2.AUCTION_ORDER_STATUS_CANCELED
    assert sub.STATUS_EXPIRED == auction_pb2.AUCTION_ORDER_STATUS_EXPIRED
    assert sub.SIDE_SELL == auction_pb2.ORDER_SIDE_SELL
    assert sub.SIDE_BUY == auction_pb2.ORDER_SIDE_BUY


def test_side_aligns_with_inventory_escrow_side() -> None:
    """★ 挂单方向直接当 inventory 的 EscrowSide 传下去,必须同号。

    咬住的不变量:SIDE_BUY 若错位成 1,就别名到 ESCROW_SIDE_SELL ——
    买单会去冻**道具**而不是金币,并且绕开只在 BUY 分支上的
    quantity*price 溢出守卫(inventory.go safeMulInt64)。冻结照样成功,没有报错。
    """
    assert sub.SIDE_SELL == inventory_pb2.ESCROW_SIDE_SELL
    assert sub.SIDE_BUY == inventory_pb2.ESCROW_SIDE_BUY


async def test_partially_filled_replay_keeps_owner_slot() -> None:
    """★ 部分成交的回放**不得**释放 owner 名额 —— 它还挂在簿上。"""
    repo = FakeRepo()
    ledger = RecordingLedger()
    slots = FakeSlots()
    repo.claim_result = (
        sub.OrderRecord(order_id=777777, status=sub.STATUS_PARTIAL, owner_id=1001),
        True,
    )
    s = _submitter(repo, ledger, slots)
    rec = (await _submit(s)).record
    assert rec.status == sub.STATUS_PARTIAL
    assert not ledger.freezes, "部分成交还去重复冻结了"
    assert slots.released == [], "部分成交被当成终态释放了名额 —— 配额被穿透"


# ── slot 预留失败路径:对齐 Go rejectPendingAfterSlotFailure ──────────────
#
# 这三条锁的是移植初版比 Go 弱的三处。它们共同的后果是:一张**已落库、已占配额
# 名额**的 PENDING 单没有被终态化 —— 既绕过了 max_active_orders_per_player,
# 又处在"可恢复"状态等着被继续推进。


class _RawFailSlots:
    """reserve 抛**裸异常**(不是 PandoraError)—— 模拟 Redis 连接被重置。"""

    def __init__(self) -> None:
        self.released: list[int] = []

    async def reserve(self, rec):  # noqa: ANN001
        raise ConnectionResetError("redis connection reset by peer")

    async def release(self, rec):  # noqa: ANN001
        self.released.append(rec.order_id)


class _NoChangeRepo(FakeRepo):
    """reject_pending_order 返回 False —— 条件更新没命中任何 PENDING 行。"""

    async def reject_pending_order(self, market_id, order_id, now_ms):  # noqa: ANN001
        self.rejected.append((market_id, order_id))
        return False


@pytest.mark.asyncio
async def test_raw_exception_from_slot_reserve_still_terminalises_pending() -> None:
    """★ slot 预留抛裸异常时,PENDING 单**必须**被终态化。

    初版写的是 `except errcode.PandoraError:` —— 只收业务错误码。
    Redis 连接重置 / 超时 / DNS 失败都是裸异常,会直接从 submit 逃出去,
    留下一张已占配额名额、状态仍可恢复的 PENDING 单。

    Go 那边是 `if slotErr := reserveOwnerSlotPruning(...); slotErr != nil`,
    对**任何** error 都去终态化。
    """
    repo, slots = FakeRepo(), _RawFailSlots()
    s = _submitter(repo=repo, slots=slots)
    with pytest.raises(ConnectionResetError):
        await _submit(s)
    assert repo.rejected, "裸异常路径没有终态化 PENDING —— 配额名额被永久占用"
    assert slots.released, "owner 名额没释放"


@pytest.mark.asyncio
async def test_reject_not_changed_after_slot_failure_is_internal_error() -> None:
    """★ `changed == False` 是异常,不是"没什么可做"。

    刚亲手写下过一张 PENDING 单,条件更新却说没有 PENDING 可改 —— 要么有并发写者,
    要么状态机不自洽。静默放过会让调用方只看到原始的 slot 错误,没有任何线索
    指向这里,而那张单继续占名额、继续可恢复。
    """
    repo = _NoChangeRepo()
    s = _submitter(repo=repo, slots=FakeSlots(full=True))
    with pytest.raises(errcode.PandoraError) as ei:
        await _submit(s)
    assert ei.value.code == errcode.ErrInternal, (
        f"应当抛 ErrInternal,实际 {ei.value.code}"
    )


@pytest.mark.asyncio
async def test_slot_failure_releases_escrow_not_just_the_slot() -> None:
    """★ 终态化之后要退 escrow,不能只放 owner 名额。

    只放名额解决的是"这个玩家还能挂几张单",解决不了"资产被锁住"。
    Go 的 `rejectPendingAfterSlotFailure` 里 `releaseOwnerSlot` 和
    `tryReleaseOrder` 是两件事,少任何一件都留下残留。
    """
    repo, ledger, slots = FakeRepo(), RecordingLedger(), FakeSlots(full=True)
    s = _submitter(repo=repo, ledger=ledger, slots=slots)
    with pytest.raises(errcode.PandoraError):
        await _submit(s)
    assert slots.released, "owner 名额没释放"
    assert ledger.releases, "escrow 没退 —— 资产会被锁住直到补偿链兜底"
