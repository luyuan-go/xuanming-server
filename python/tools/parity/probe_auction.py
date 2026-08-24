"""auction 服务 Go/Python 对照探针。

    用法:probe_auction.py <auction_grpc_port> <base>

`base` 同时用来切分**四种跨运行共享的资源**,两次运行必须给不相交的段:

    player_id   auction_orders.owner_id / inventory 的 player_wallet+player_items
    market_id   auction_orders 按 market_id 分片,ListMarket 是按 market_id 全表扫
    idem_key    auction_idempotency_keys 的 uk 是 (owner_id, idempotency_key)
    频率配额键   Redis `auction:{action}:{player_id}` 窗口 60s

★ 为什么 market_id 也必须分段(踩过):ListMarket 返回的是**整个 market 的簿**,
  不按 player 过滤。两次运行若共用 market_id,第二次的 "ListMarket 应为空" 前置断言
  会被上一次的残单打破 —— 而那看起来像"实现分叉",其实是探针自己的残留。

★ 为什么每步都先断言前置状态:探针最容易的失败模式是"什么都没测到但全绿"。
  例:S6 的撮合如果因为 S3 的挂单没进簿而其实没发生,`filled_quantity=0` 也是
  一个"两边一致"的结果 —— 于是 diff 全绿,而撮合这条最贵的路径**一次都没跑过**。
  所以每个 ★ 场景都先打印前置事实(簿里有几单、各自成交了多少、库存/金币多少),
  再打印结果。

★ 归一化只盖"必然不同"的:order_id(雪花,含时间+节点)、owner_id(段不同)、
  market_id(段不同)、绝对时间戳。**刻意不盖**:code、status、filled_quantity、
  has_more、next_cursor 是不是 0、以及库存/金币的**增量**(绝对值盖掉、增量保留)——
  那些正是要验的东西。
"""

import asyncio
import re
import sys

import pathlib as _pl

# 按**文件位置**解析,不依赖调用时的 cwd —— 探针会被从各种目录调起。
_ROOT = _pl.Path(__file__).resolve().parents[2]  # python/
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "gen"))

import grpc
from google.protobuf import text_format

from pandora.auction.v1 import auction_pb2 as apb
from pandora.auction.v1 import auction_pb2_grpc as agrpc
from pandora.common.v1 import errcode_pb2 as ec
from pandora.common.v1 import currency_pb2 as cpb
from pandora.inventory.v1 import inventory_pb2 as ipb
from pandora.inventory.v1 import inventory_pb2_grpc as igrpc

PORT = sys.argv[1]
BASE = int(sys.argv[2])  # player_id / market_id / idem_key 段起点

INVENTORY_ADDR = "127.0.0.1:20015"

# 10002「腐蚀残片」:可堆叠(max_stack 99)、非装备,是拍卖行唯一合适的品类。
# 装备类走 instance 路径(GrantInstances),不经 item_config_id 的数量撮合。
ITEM = 10002

SELLER = BASE + 1
BUYER = BASE + 2
QUOTA_PLAYER = BASE + 3   # 只用来打满频率配额,不参与交易
STRANGER = BASE + 4       # 只用来验"非本人撤单"

# market_id 是 uint32,不能直接用 BASE(可能超界);取低位并避开 0。
MARKET = (BASE % 4_000_000) + 7

# 配额上限来自 auction-dev.yaml 的 rate_quota_per_min 默认 20。
# 探针不读配置 —— 写死 20 是**刻意**的:两栈都按同一个数打,
# 谁先/后翻 ERR_RATE_LIMITED 会直接体现在 diff 里。
RATE_QUOTA_PER_MIN = 20

# 这个 trace_id 只走 charset [A-Za-z0-9_-](两栈的 isSafeTraceID / is_safe_trace_id
# 都只放行这个集合;带 '.' 或 ':' 会被判不安全并被服务端换成新生成的 UUID,
# 那样就验不出"到底有没有透传"了)。
TRACE_MARKER = f"probeauctiontrace-{BASE}"


# ── 归一化 ──────────────────────────────────────────────────────────────────
class Norm:
    """把随运行必然不同的值换成稳定占位符。"""

    def __init__(self) -> None:
        self.orders: dict[int, str] = {}
        self.players: dict[int, str] = {}

    def order(self, oid: int) -> str:
        if oid == 0:
            return "0"
        if oid not in self.orders:
            self.orders[oid] = f"<O{len(self.orders) + 1}>"
        return self.orders[oid]

    def player(self, pid: int) -> str:
        if pid == 0:
            return "0"
        if pid not in self.players:
            self.players[pid] = f"<P{len(self.players) + 1}>"
        return self.players[pid]

    def text(self, body: str) -> str:
        body = re.sub(r"order_id: (\d+)", lambda m: f"order_id: {self.order(int(m.group(1)))}", body)
        body = re.sub(
            r"next_cursor_order_id: (\d+)",
            lambda m: f"next_cursor_order_id: {self.order(int(m.group(1)))}",
            body,
        )
        body = re.sub(r"owner_id: (\d+)", lambda m: f"owner_id: {self.player(int(m.group(1)))}", body)
        body = re.sub(r"market_id: (\d+)", "market_id: <M>", body)
        for key in ("created_at_ms", "updated_at_ms"):
            body = re.sub(rf"{key}: (\d+)",
                          lambda m, k=key: f"{k}: <TS>" if int(m.group(1)) else f"{k}: 0", body)
        return body


N = Norm()
FAILS: list[str] = []


def md(player_id: int, trace: str | None = None):
    """构造鉴权 metadata。player_id=0 → 完全不带头(模拟未鉴权直连)。"""
    entries = []
    if player_id:
        entries.append(("x-pandora-player-id", str(player_id)))
    if trace:
        entries.append(("x-pandora-trace-id", trace))
    return tuple(entries)


def code(resp) -> str:
    return ec.ErrCode.Name(resp.code)


def show(tag: str, resp, extra: str = "") -> None:
    print(f"--- {tag}")
    print(f"    code={code(resp)}{extra}")
    body = text_format.MessageToString(resp, as_utf8=True).rstrip()
    body = "\n".join(ln for ln in body.splitlines() if not ln.startswith("code:"))
    body = N.text(body)
    print("\n".join("    " + ln for ln in body.splitlines()) or "    <empty>")


def expect(tag: str, got, want) -> None:
    """前置/结果断言。★ 失败**只记录不中断** —— 中断会让后面的场景整块消失,
    两侧输出行数不同时 diff 会变成一片红,反而看不出真正的分叉在哪。"""
    mark = "OK " if got == want else "!! "
    if got != want:
        FAILS.append(f"{tag}: got={got!r} want={want!r}")
    print(f"    [{mark}断言] {tag}: got={got!r} want={want!r}")


# ── 便捷读取 ────────────────────────────────────────────────────────────────
async def book(stub, side=apb.ORDER_SIDE_UNSPECIFIED):
    """读当前 market 的簿。返回 (code_name, [AuctionOrder])。"""
    r = await stub.ListMarket(
        apb.ListMarketRequest(market_id=MARKET, side=side), metadata=md(SELLER)
    )
    return code(r), list(r.orders)


def find(orders, order_id):
    for o in orders:
        if o.order_id == order_id:
            return o
    return None


async def wallet(inv, player_id) -> tuple[int, int]:
    """返回 (金币余额, ITEM 数量)。ERR 时返回 (-1,-1) 让断言直接暴露。

    多币种改造(2026-08-22)后 Inventory 下发的是 `currencies` 列表(按 kind 升序、
    只含非零项),不再有 `gold` 标量。缺项 = 0,**不是**错误。
    """
    r = await inv.GetInventory(ipb.GetInventoryRequest(player_id=player_id),
                               metadata=md(player_id))
    if r.code != ec.OK:
        return -1, -1
    n = 0
    for it in r.inventory.items:
        if it.item_config_id == ITEM:
            n = it.count
    gold = 0
    for c in r.inventory.currencies:
        if c.kind == cpb.CURRENCY_KIND_GOLD:
            gold = c.amount
    return gold, n


# ── 主流程 ──────────────────────────────────────────────────────────────────
async def main() -> None:
    print(f"### probe_auction port={PORT} base={BASE} market=<M> item={ITEM}")
    print(f"### trace_marker={TRACE_MARKER}")
    print("###   （用它 grep inventory 日志,验证 auction→inventory 是否透传 trace_id）")

    async with grpc.aio.insecure_channel(INVENTORY_ADDR) as ich:
        inv = igrpc.InventoryServiceStub(ich)

        # ── S0 铺资产 ───────────────────────────────────────────────────
        # 卖家没有道具 → Freeze 会返回 ERR_AUCTION_INSUFFICIENT,后面所有交易场景
        # 都会停在同一个错误码上("两边一致"但一条撮合都没跑)。所以这一步的断言
        # 是整个探针的地基,必须先证明它成功了。
        print("\n=== S0 铺底资产(证明后续交易场景真的有货可动)===")
        g1 = await inv.GrantItems(ipb.GrantItemsRequest(
            player_id=SELLER, items=[ipb.ItemGrant(item_config_id=ITEM, count=50)],
            idempotency_key=f"probe-seed-s-{BASE}"))
        expect("给卖家发 50 个道具", code(g1), "OK")
        g2 = await inv.GrantItems(ipb.GrantItemsRequest(
            player_id=BUYER, items=[],
            currencies=[cpb.CurrencyAmount(kind=cpb.CURRENCY_KIND_GOLD, amount=1_000_000)],
            idempotency_key=f"probe-seed-b-{BASE}"))
        expect("给买家发 100 万金币", code(g2), "OK")

        s_gold0, s_item0 = await wallet(inv, SELLER)
        b_gold0, b_item0 = await wallet(inv, BUYER)
        expect("卖家道具数(前置)", s_item0, 50)
        expect("买家金币(前置)", b_gold0, 1_000_000)
        expect("买家道具数(前置)", b_item0, 0)

        async with grpc.aio.insecure_channel(f"127.0.0.1:{PORT}") as ch:
            stub = agrpc.AuctionServiceStub(ch)

            # ── S1 鉴权闸 ───────────────────────────────────────────────
            print("\n=== S1 鉴权闸(不带 x-pandora-player-id)===")
            for name, call in (
                ("PlaceOrder", stub.PlaceOrder(apb.PlaceOrderRequest(
                    market_id=MARKET, item_config_id=ITEM, quantity=1, price=1,
                    idempotency_key=f"na-{BASE}"), metadata=md(0))),
                ("Bid", stub.Bid(apb.BidRequest(
                    market_id=MARKET, item_config_id=ITEM, quantity=1, price=1,
                    idempotency_key=f"nb-{BASE}"), metadata=md(0))),
                ("CancelOrder", stub.CancelOrder(apb.CancelOrderRequest(
                    market_id=MARKET, order_id=1), metadata=md(0))),
                ("ListMarket", stub.ListMarket(apb.ListMarketRequest(
                    market_id=MARKET), metadata=md(0))),
                ("ListMyOrders", stub.ListMyOrders(apb.ListMyOrdersRequest(
                    active_only=True), metadata=md(0))),
            ):
                r = await call
                show(f"S1 {name} 匿名", r)
                expect(f"S1 {name} 必须被拒", code(r), "ERR_UNAUTHORIZED")

            # ── S2 入参校验 ────────────────────────────────────────────
            # ★ 前置:簿必须是空的。不断言的话,下面 6 条"应该被拒"里
            #   万一有一条其实建了单,也看不出来。
            print("\n=== S2 入参校验(全部应拒且不留副作用)===")
            c0, orders0 = await book(stub)
            expect("S2 前置:本 market 簿为空", (c0, len(orders0)), ("OK", 0))

            bad = [
                ("quantity=0", 0, 100, f"v1-{BASE}"),
                ("quantity 超上限", 1_000_001, 100, f"v2-{BASE}"),
                ("price=0", 5, 0, f"v3-{BASE}"),
                ("price 超上限", 5, 1_000_000_001, f"v4-{BASE}"),
                ("总额溢出 int64", 1_000_000, 1_000_000_000, f"v5-{BASE}"),
                # ★ Python 的 `$` 会匹配"末尾换行之前",照抄 Go 正则会静默放宽。
                #   这条专门验那道闸没有被放宽。
                ("幂等键带尾换行", 5, 100, f"v6-{BASE}\n"),
                ("幂等键含非法字符", 5, 100, f"v7/{BASE}"),
                ("幂等键为空", 5, 100, ""),
                ("幂等键 65 字符", 5, 100, "z" * 65),
            ]
            for name, q, p, key in bad:
                r = await stub.PlaceOrder(apb.PlaceOrderRequest(
                    market_id=MARKET, item_config_id=ITEM, quantity=q, price=p,
                    idempotency_key=key), metadata=md(STRANGER))
                show(f"S2 {name}", r)
                expect(f"S2 {name} 必须被拒", code(r), "ERR_INVALID_ARG")

            r = await stub.PlaceOrder(apb.PlaceOrderRequest(
                market_id=0, item_config_id=ITEM, quantity=5, price=100,
                idempotency_key=f"v8-{BASE}"), metadata=md(STRANGER))
            show("S2 market_id=0", r)
            expect("S2 market_id=0 必须被拒", code(r), "ERR_INVALID_ARG")

            r = await stub.PlaceOrder(apb.PlaceOrderRequest(
                market_id=MARKET, item_config_id=0, quantity=5, price=100,
                idempotency_key=f"v9-{BASE}"), metadata=md(STRANGER))
            show("S2 item_config_id=0", r)
            expect("S2 item_config_id=0 必须被拒", code(r), "ERR_INVALID_ARG")

            c1, orders1 = await book(stub)
            expect("S2 结果:簿仍为空(没有非法单漏进去)", (c1, len(orders1)), ("OK", 0))

            # ── S3 挂单(★ 走真实 Freeze)────────────────────────────────
            print("\n=== S3 卖家挂单 10 个 @100(★ 会经 inventory 冻结)===")
            sell = await stub.PlaceOrder(apb.PlaceOrderRequest(
                market_id=MARKET, item_config_id=ITEM, quantity=10, price=100,
                idempotency_key=f"sell-{BASE}"), metadata=md(SELLER, TRACE_MARKER))
            show("S3 PlaceOrder", sell)
            expect("S3 挂单成功", code(sell), "OK")
            expect("S3 状态=OPEN", sell.status, apb.AUCTION_ORDER_STATUS_OPEN)
            expect("S3 即时成交量=0(簿上无对手)", sell.filled_quantity, 0)

            s_gold1, s_item1 = await wallet(inv, SELLER)
            # ★ 这条是"真的走到了 Freeze"的判据:冻结会把 10 个道具从背包扣进 escrow。
            #   只看 code=OK 是不够的 —— NoopSettlementLedger 也返回 OK。
            expect("S3 卖家道具被冻走 10 个", s_item0 - s_item1, 10)

            c2, orders2 = await book(stub)
            expect("S3 簿上恰好 1 单", (c2, len(orders2)), ("OK", 1))

            # ── S4 幂等重放 ────────────────────────────────────────────
            print("\n=== S4 同 idempotency_key 重放(必须回放同一单,不得新建)===")
            replay = await stub.PlaceOrder(apb.PlaceOrderRequest(
                market_id=MARKET, item_config_id=ITEM, quantity=10, price=100,
                idempotency_key=f"sell-{BASE}"), metadata=md(SELLER))
            show("S4 PlaceOrder 重放", replay)
            expect("S4 order_id 与首次相同", replay.order_id == sell.order_id, True)
            c3, orders3 = await book(stub)
            expect("S4 簿上仍是 1 单(没重复建单)", (c3, len(orders3)), ("OK", 1))
            s_gold2, s_item2 = await wallet(inv, SELLER)
            expect("S4 没有二次冻结", s_item2, s_item1)

            # ── S5 不交叉的买单 ────────────────────────────────────────
            print("\n=== S5 买家出价 3 @50(低于卖价,★ 不应撮合)===")
            low = await stub.Bid(apb.BidRequest(
                market_id=MARKET, item_config_id=ITEM, quantity=3, price=50,
                idempotency_key=f"lowbid-{BASE}"), metadata=md(BUYER))
            show("S5 Bid", low)
            expect("S5 出价成功", code(low), "OK")
            expect("S5 状态=OPEN", low.status, apb.AUCTION_ORDER_STATUS_OPEN)
            expect("S5 成交量=0(价格不交叉)", low.filled_quantity, 0)
            b_gold1, b_item1 = await wallet(inv, BUYER)
            expect("S5 买家金币被冻 3*50=150", b_gold0 - b_gold1, 150)
            c4, orders4 = await book(stub)
            expect("S5 簿上 2 单(一买一卖)", (c4, len(orders4)), ("OK", 2))
            expect("S5 卖单成交量仍为 0", find(orders4, sell.order_id).filled_quantity, 0)

            # ── S6 交叉撮合(★ 本探针最贵的一条路径)──────────────────
            print("\n=== S6 买家出价 4 @100(交叉,★ 应撮合 + 经 inventory 结算)===")
            # 前置事实全部打出来,免得"其实没撮合"也能全绿。
            expect("S6 前置:卖单剩余 10", find(orders4, sell.order_id).quantity
                   - find(orders4, sell.order_id).filled_quantity, 10)
            cross = await stub.Bid(apb.BidRequest(
                market_id=MARKET, item_config_id=ITEM, quantity=4, price=100,
                idempotency_key=f"crossbid-{BASE}"), metadata=md(BUYER, TRACE_MARKER))
            show("S6 Bid", cross)
            expect("S6 出价成功", code(cross), "OK")
            expect("S6 买单全成交", cross.status, apb.AUCTION_ORDER_STATUS_FILLED)
            expect("S6 成交量=4", cross.filled_quantity, 4)

            c5, orders5 = await book(stub)
            sell_now = find(orders5, sell.order_id)
            expect("S6 卖单变 PARTIALLY_FILLED",
                   sell_now.status, apb.AUCTION_ORDER_STATUS_PARTIALLY_FILLED)
            expect("S6 卖单已成交 4", sell_now.filled_quantity, 4)

            s_gold3, s_item3 = await wallet(inv, SELLER)
            b_gold2, b_item2 = await wallet(inv, BUYER)
            # ★ 资产真的动了才算撮合跑通:卖家收 4*100 金币,买家收 4 个道具。
            #   只断言 filled_quantity 的话,一个"只改数据库不调 inventory"的实现
            #   也会全绿 —— 而那正是 auction 最不能出的错。
            expect("S6 卖家进账 400 金币", s_gold3 - s_gold2, 400)
            expect("S6 买家到货 4 个道具", b_item2 - b_item1, 4)
            expect("S6 买家金币再冻 4*100=400", b_gold1 - b_gold2, 400)

            # ── S7 非本人撤单 ──────────────────────────────────────────
            print("\n=== S7 非本人撤单(必须拒,且不得改变簿)===")
            r = await stub.CancelOrder(apb.CancelOrderRequest(
                market_id=MARKET, order_id=sell.order_id), metadata=md(STRANGER))
            show("S7 陌生人撤卖单", r)
            expect("S7 必须被拒", code(r), "ERR_AUCTION_NOT_OWNER")
            c6, orders6 = await book(stub)
            expect("S7 簿未变(仍 2 单)", (c6, len(orders6)), ("OK", 2))

            # ── S8 撤单入参 ────────────────────────────────────────────
            print("\n=== S8 撤单入参校验 ===")
            r = await stub.CancelOrder(apb.CancelOrderRequest(
                market_id=0, order_id=sell.order_id), metadata=md(SELLER))
            show("S8 market_id=0", r)
            expect("S8 market_id=0 必须被拒", code(r), "ERR_INVALID_ARG")
            r = await stub.CancelOrder(apb.CancelOrderRequest(
                market_id=MARKET, order_id=0), metadata=md(SELLER))
            show("S8 order_id=0", r)
            expect("S8 order_id=0 必须被拒", code(r), "ERR_INVALID_ARG")
            r = await stub.CancelOrder(apb.CancelOrderRequest(
                market_id=MARKET, order_id=sell.order_id + 987654321), metadata=md(SELLER))
            show("S8 order_id 不存在", r)
            expect("S8 不存在的单必须被拒", code(r), "ERR_AUCTION_ORDER_NOT_FOUND")

            # ── S9 本人撤单(★ 退还 escrow)─────────────────────────────
            print("\n=== S9 本人撤单(★ 应退还未成交的 6 个道具)===")
            r = await stub.CancelOrder(apb.CancelOrderRequest(
                market_id=MARKET, order_id=sell.order_id), metadata=md(SELLER, TRACE_MARKER))
            show("S9 撤卖单", r)
            expect("S9 撤单成功", code(r), "OK")
            c7, orders7 = await book(stub)
            expect("S9 卖单已出簿", find(orders7, sell.order_id) is None, True)
            s_gold4, s_item4 = await wallet(inv, SELLER)
            expect("S9 退回 6 个未成交道具", s_item4 - s_item3, 6)

            # ── S10 重复撤单 ───────────────────────────────────────────
            print("\n=== S10 重复撤单(终态,必须拒)===")
            r = await stub.CancelOrder(apb.CancelOrderRequest(
                market_id=MARKET, order_id=sell.order_id), metadata=md(SELLER))
            show("S10 再撤一次", r)
            expect("S10 必须被拒", code(r), "ERR_AUCTION_WRONG_STATE")

            # ── S11 ListMarket 侧别与 limit ────────────────────────────
            print("\n=== S11 ListMarket 侧别过滤 / limit ===")
            for tag, side in (("SELL", apb.ORDER_SIDE_SELL), ("BUY", apb.ORDER_SIDE_BUY)):
                cx, ox = await book(stub, side)
                print(f"    {tag} 侧 {len(ox)} 单")
                expect(f"S11 {tag} 侧数量", (cx, len(ox)),
                       ("OK", 0 if tag == "SELL" else 1))
            r = await stub.ListMarket(apb.ListMarketRequest(
                market_id=MARKET, limit=1), metadata=md(SELLER))
            expect("S11 limit=1 生效", (code(r), len(r.orders)), ("OK", 1))
            r = await stub.ListMarket(apb.ListMarketRequest(
                market_id=MARKET, limit=100000), metadata=md(SELLER))
            expect("S11 limit 超上限被收敛(仍返回)", code(r), "OK")

            # ── S12 ListMyOrders 分页 ──────────────────────────────────
            # 买家此刻有 2 单(S5 的 OPEN 低价单 + S6 的 FILLED 单)。
            print("\n=== S12 ListMyOrders 分页 / active_only ===")
            r = await stub.ListMyOrders(apb.ListMyOrdersRequest(
                active_only=False, limit=1), metadata=md(BUYER))
            show("S12 买家第 1 页 limit=1", r)
            expect("S12 第 1 页 1 条", len(r.orders), 1)
            expect("S12 has_more=True", r.has_more, True)
            expect("S12 游标=本页末单", r.next_cursor_order_id, r.orders[-1].order_id)
            page2 = await stub.ListMyOrders(apb.ListMyOrdersRequest(
                active_only=False, cursor_order_id=r.next_cursor_order_id, limit=10),
                metadata=md(BUYER))
            show("S12 买家第 2 页", page2)
            expect("S12 第 2 页 1 条", len(page2.orders), 1)
            expect("S12 第 2 页 has_more=False", page2.has_more, False)
            expect("S12 第 2 页游标归 0", page2.next_cursor_order_id, 0)
            act = await stub.ListMyOrders(apb.ListMyOrdersRequest(
                active_only=True, limit=10), metadata=md(BUYER))
            show("S12 买家 active_only", act)
            expect("S12 active_only 只剩低价 OPEN 单", len(act.orders), 1)
            expect("S12 该单是 OPEN", act.orders[0].status if act.orders else None,
                   apb.AUCTION_ORDER_STATUS_OPEN)

            # ── S13 频率配额(★ 顺便验"配额闸在参数校验之前")────────
            # 请求本身是非法的(quantity=0)。如果配额闸在校验之后,前 N 条会一直
            # 是 ERR_INVALID_ARG 永不翻 RATE_LIMITED;翻了就说明配额确实更靠前。
            print(f"\n=== S13 频率配额(独立玩家,连打 {RATE_QUOTA_PER_MIN + 3} 条非法请求)===")
            seq = []
            for i in range(RATE_QUOTA_PER_MIN + 3):
                r = await stub.PlaceOrder(apb.PlaceOrderRequest(
                    market_id=MARKET, item_config_id=ITEM, quantity=0, price=100,
                    idempotency_key=f"q{i}-{BASE}"), metadata=md(QUOTA_PLAYER))
                seq.append(code(r))
            first_limited = next(
                (i for i, c in enumerate(seq) if c == "ERR_RATE_LIMITED"), -1)
            print(f"    码序列:{'|'.join(seq)}")
            expect("S13 第一次被限流的下标", first_limited, RATE_QUOTA_PER_MIN)
            expect("S13 限流前全部是参数错(证明配额闸更靠前)",
                   set(seq[:RATE_QUOTA_PER_MIN]), {"ERR_INVALID_ARG"})
            expect("S13 限流后不再变回参数错",
                   set(seq[RATE_QUOTA_PER_MIN:]), {"ERR_RATE_LIMITED"})

            # ── S14 收尾:把低价买单也撤掉,避免给下一次运行留残单 ─────
            print("\n=== S14 收尾撤单(让本 market 归零)===")
            r = await stub.CancelOrder(apb.CancelOrderRequest(
                market_id=MARKET, order_id=low.order_id), metadata=md(BUYER))
            show("S14 撤低价买单", r)
            expect("S14 撤单成功", code(r), "OK")
            c8, orders8 = await book(stub)
            expect("S14 本 market 已清空", (c8, len(orders8)), ("OK", 0))
            b_gold3, _ = await wallet(inv, BUYER)
            expect("S14 退回 150 金币", b_gold3 - b_gold2, 150)

    print("\n### 断言汇总")
    if FAILS:
        print(f"###   失败 {len(FAILS)} 条:")
        for f in FAILS:
            print(f"###     - {f}")
    else:
        print("###   全部通过")


asyncio.run(main())
