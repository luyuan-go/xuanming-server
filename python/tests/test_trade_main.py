"""trade 服务入口(main.py)与配置的启动闸测试。

覆盖的是"起不来 / 起错了"这一族缺陷 —— 它们全都**不会**在业务测试里露头:
  - conf 默认值与 Go 侧 Defaults() 分叉:同一份 yaml 喂两个实现行为不同,两边都不报错
  - 账本闸漏掉:成交但不扣转背包 / 货币,而订单状态 / 审计 / 客户端提示全显示成功
  - kafka 弱依赖被写成 fail-fast:broker 抖一下交易全停
  - Redis 端点漏配:host='' 被解成 127.0.0.1:6379,连上无关库照常启动

默认值 parity 刻意**从 Go 源码里读**而不是抄一份常量:抄一份的话,Go 改了默认值
这个测试照样绿(它验的是"我抄的值等于我抄的值")。
"""

from __future__ import annotations

import asyncio
import pathlib
import re

import pytest

from pandorapy import errcode
from pandorapy.services.trade import conf as tconf
from pandorapy.services.trade import data as tdata
from pandorapy.services.trade import main as tmain

GO_CONF = "services/economy/trade/internal/conf/conf.go"
DEV_YAML = "services/economy/trade/etc/trade-dev.yaml"


# ── 配置:默认值与 Go 逐个对齐 ───────────────────────────────────────────────


def test_defaults_match_go_source(repo_root: pathlib.Path) -> None:
    """conf.Defaults() 的每个默认值都从 Go 源码抓出来比对(含判据符号)。"""
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")

    cfg = tconf.Config()
    cfg.apply_defaults()
    t = cfg.trade

    # 时长类:Go 写成 `config.Duration(10 * time.Minute)`。
    assert t.order_ttl_td().total_seconds() == 600
    assert t.order_expire_td().total_seconds() == 300
    assert re.search(r"OrderTTL\s*=\s*config\.Duration\(10 \* time\.Minute\)", src)
    assert re.search(r"OrderExpire\s*=\s*config\.Duration\(5 \* time\.Minute\)", src)

    # 数值类。
    for field, value, go_name in (
        ("optimistic_retry", 3, "OptimisticRetry"),
        ("max_items_per_order", 20, "MaxItemsPerOrder"),
        ("rate_quota_per_min", 20, "RateQuotaPerMin"),
        ("max_orders_per_player", 200, "MaxOrdersPerPlayer"),
    ):
        assert getattr(t, field) == value, field
        assert re.search(rf"c\.Trade\.{go_name}\s*=\s*{value}\b", src), go_name

    # 端口:Envoy cluster 和 run_services.ps1 的端口占用检查都钉在这两个值上。
    assert cfg.server.grpc.addr == ":20012"
    assert cfg.server.http.addr == ":21012"
    assert '":20012"' in src and '":21012"' in src


def test_rate_quota_uses_equals_zero_not_le_zero(repo_root: pathlib.Path) -> None:
    """rate_quota_per_min 的判据是 `== 0`,不是 `<= 0` —— 负值 = **显式关闭**配额。

    换成 `<= 0` 会把 "-1 关闭限流" 悄悄改写成 20,配置意图被反转且无任何日志。
    """
    src = (repo_root / GO_CONF).read_text(encoding="utf-8")
    assert re.search(r"c\.Trade\.RateQuotaPerMin\s*==\s*0", src)

    cfg = tconf.Config(trade=tconf.TradeConf(rate_quota_per_min=-1))
    cfg.apply_defaults()
    assert cfg.trade.rate_quota_per_min == -1


def test_loads_the_same_dev_yaml_as_go(repo_root: pathlib.Path) -> None:
    """读 Go 版**同一份** yaml,而不是测试专用配置。

    kafka / session_gate 两段尤其要验:它们在 Go 侧属于 config.Base,Python 的
    BaseConf 还没建到 —— 没建模就会静默落进 extra,表现为"配了却不生效、零信号"。
    """
    cfg = tconf.Config.load(repo_root / DEV_YAML)

    assert cfg.node.redis_client.endpoints() == ["127.0.0.1:6380"]
    assert cfg.kafka.brokers == ["127.0.0.1:9093"]
    assert cfg.kafka.idempotent is True
    assert cfg.session_gate.require is False
    assert cfg.trade.allow_noop_ledger is True
    assert cfg.trade.inventory_addr == ""


def test_kafka_idempotent_defaults_false_like_go_zero_value() -> None:
    """未配 idempotent 时必须是 False(Go 结构体零值),不能借用 ProducerConf 的 True。

    分叉的后果:同一份 yaml 在 Go 上非幂等、在 Python 上幂等(或反之),
    重试窗口里的重复投递行为不同,而两边都不报错。
    """
    kc = tconf.KafkaConf(brokers=["b:9093"])
    assert kc.idempotent is False
    assert kc.producer_conf().idempotent is False


def test_producer_conf_maps_durations_to_ms() -> None:
    kc = tconf.KafkaConf(
        brokers=["b:9093"], dial_timeout="2s", retry_backoff="150ms", retry_max=5
    )
    pc = kc.producer_conf()
    assert pc.dial_timeout_ms == 2000
    assert pc.retry_backoff_ms == 150
    assert pc.retry_max == 5
    assert pc.brokers == ("b:9093",)


# ── 启动闸 ────────────────────────────────────────────────────────────────


def _run(argv: list[str]) -> int:
    return tmain.main(argv)


def test_missing_conf_file_exits_nonzero(tmp_path: pathlib.Path) -> None:
    assert _run(["-conf", str(tmp_path / "nope.yaml")]) == 1


def test_cell_route_mode_is_rejected_at_config_load(tmp_path: pathlib.Path) -> None:
    """配了多 Cell 但 Python 只实现单 Cell → 拒启(不是忽略 + WARN)。

    忽略掉的后果是所有玩家静默落在单 Cell 上,与配置意图不符且零信号。
    """
    yaml_path = tmp_path / "trade.yaml"
    yaml_path.write_text(
        "node:\n"
        # node_id 必须给合法值:0 是 UE DS 本地发号器的保留号,static 档会被
        # snowflake 闸拒启(对齐 Go provider.go:96-104)。这几条用例要测的是
        # **后面**那道闸,所以除被测项外配置必须合法 —— 否则测的其实是 snowflake。
        # Go 的闸序同样是 snowflake(main.go:105) → ledger(main.go:125)。
        "  node_id: 1\n"
        "  redis_client:\n"
        '    host: "127.0.0.1:6380"\n'
        "cell_route:\n"
        '  mode: "static"\n',
        encoding="utf-8",
    )
    assert _run(["-conf", str(yaml_path)]) == 1


def test_redis_endpoint_required(tmp_path: pathlib.Path) -> None:
    """host 与 addrs 皆空 → 拒启。不拒的话会连上本机一个无关 Redis 并照常 Ready。"""
    yaml_path = tmp_path / "trade.yaml"
    yaml_path.write_text("trade:\n  allow_noop_ledger: true\n", encoding="utf-8")
    assert _run(["-conf", str(yaml_path)]) == 1


def test_ledger_not_configured_refuses_to_start(tmp_path: pathlib.Path) -> None:
    """既没有 inventory_addr 也没显式开 Noop → 拒启。

    这是本服务最要紧的一道:漏配的后果是"成交但不扣转",而订单状态、审计流水、
    客户端提示全部显示成功。这里必须走到账本闸才算数 —— 所以先让 Redis 那两道过。
    """
    yaml_path = tmp_path / "trade.yaml"
    yaml_path.write_text(
        "node:\n"
        "  node_id: 1\n"
        "  redis_client:\n"
        '    host: "127.0.0.1:6380"\n'
        "trade:\n"
        "  allow_noop_ledger: false\n",
        encoding="utf-8",
    )
    # Redis 大概率连不上(本机没起 6380),那也是 exit 1 —— 但那样就没验到账本闸。
    # 用假的 must_connect 把前两道闸放过去,确保失败点确实落在账本闸上。
    events: list[str] = []
    assert _run_with_fake_redis(yaml_path, events) == 1
    assert "resource_ledger_not_configured" in events
    assert "service_ready" not in events


def test_noop_ledger_allowed_when_explicitly_enabled_and_leaves_a_trace(
    tmp_path: pathlib.Path,
) -> None:
    """显式开了 Noop 要能过闸,但**必须留 WARN**。

    没有这条日志的话,一个误开了 allow_noop_ledger 的环境从启动到出事之间零信号,
    而"成交不扣转"发现时通常已经产生了一批脏数据。
    """
    yaml_path = tmp_path / "trade.yaml"
    yaml_path.write_text(
        "node:\n"
        "  node_id: 1\n"
        "  redis_client:\n"
        '    host: "127.0.0.1:6380"\n'
        "trade:\n"
        "  allow_noop_ledger: true\n",
        encoding="utf-8",
    )
    events: list[str] = []
    # 装配到 pserver.run 之前都要跑通;run 被替换成立即返回,等价于"起来了又收到停止信号"。
    assert _run_with_fake_redis(yaml_path, events) == 0
    assert "resource_ledger_noop" in events
    assert "service_ready" in events
    # kafka 未配:必须是 WARN 继续,不是拒启(审计是弱依赖)。
    assert "kafka_brokers_empty" in events
    assert "trade_rate_quota_ready" in events


def _run_with_fake_redis(yaml_path: pathlib.Path, events: list[str]) -> int:
    """跑 main,但把 Redis 换成 fakeredis、把 pserver.run 换成立即返回。

    刻意**不**mock 掉闸本身:验的就是这些闸在真实装配链上确实被执行到。
    """
    import fakeredis

    import pandorapy.log as plog
    import pandorapy.redisx as redisx
    import pandorapy.server as pserver

    server = fakeredis.FakeServer()

    async def fake_connect(conf, **kw):  # noqa: ANN001, ARG001
        return fakeredis.aioredis.FakeRedis(server=server, decode_responses=False)

    async def fake_run(**kwargs):  # noqa: ANN003
        on_ready = kwargs.get("on_ready")
        if on_ready is not None:
            on_ready()

    class _Recorder:
        """只记事件名。断言事件名而不是日志文本 —— Loki 告警也是按事件名建的。"""

        def __getattr__(self, level):  # noqa: ANN001
            def emit(event, **kw):  # noqa: ANN001, ANN003
                events.append(event)

            return emit

    real_connect = redisx.must_connect
    real_run = pserver.run
    real_setup, real_get = plog.setup, plog.get
    recorder = _Recorder()
    redisx.must_connect = fake_connect
    pserver.run = fake_run
    plog.setup = lambda *_a, **_k: recorder
    plog.get = lambda *_a, **_k: recorder
    try:
        return tmain.main(["-conf", str(yaml_path)])
    finally:
        redisx.must_connect = real_connect
        pserver.run = real_run
        plog.setup, plog.get = real_setup, real_get


# ── GrpcResourceLedger:结算错误分流 ────────────────────────────────────────


class _FakeChannel:
    """最小 grpc.aio.Channel 替身。

    刻意注入 **channel** 而不是直接换掉 `_stub`:这样 InventoryServiceStub 是真的被
    构造出来的,方法路径 / 序列化器都走真代码。换掉 _stub 的话,把 RPC 名写错
    (SettlePlayerTrade → SettleTrade)测试照样绿。
    """

    def __init__(self, code=None, raise_exc: BaseException | None = None) -> None:
        self.code = code
        self.raise_exc = raise_exc
        self.requests: list = []
        self.timeouts: list = []
        self.closed = False

    def unary_unary(self, method, **kwargs):  # noqa: ANN001, ANN003
        if not method.endswith("/SettlePlayerTrade"):
            return lambda *a, **kw: None

        async def call(req, timeout=None, **_kw):  # noqa: ANN001, ANN003
            self.requests.append(req)
            self.timeouts.append(timeout)
            if self.raise_exc is not None:
                raise self.raise_exc
            from pandora.inventory.v1 import inventory_pb2

            return inventory_pb2.SettlePlayerTradeResponse(code=self.code)

        return call

    def unary_stream(self, method, **kwargs):  # noqa: ANN001, ANN003, ARG002
        return lambda *a, **kw: None

    async def close(self) -> None:
        self.closed = True


def _ledger_with(channel: _FakeChannel) -> tdata.GrpcResourceLedger:
    return tdata.GrpcResourceLedger("unused:0", channel=channel)


def _order():
    from pandora.trade.v1 import trade_pb2

    return trade_pb2.Order(
        order_id=901,
        seller_id=11,
        buyer_id=22,
        items=[trade_pb2.TradeItem(item_config_id=1001, count=2)],
        buyer_items=[trade_pb2.TradeItem(item_config_id=2002, count=1)],
        price=500,
    )


def test_settle_ok_passes_and_carries_order_id_as_idempotency_key() -> None:
    from pandora.common.v1 import errcode_pb2

    ch = _FakeChannel(code=errcode_pb2.OK)
    ledger = _ledger_with(ch)
    order = _order()
    asyncio.run(ledger.settle(order, order.order_id))

    req = ch.requests[0]
    # 幂等键恒为 order_id(不变量 §9.7);对错了会让重试变成第二次真扣减。
    assert req.order_id == 901
    assert req.seller_id == 11 and req.buyer_id == 22
    # 多币种改造后价格是 CurrencyAmount。★ kind 必须**显式**给金币:
    # inventory 对 UNSPECIFIED 一律 fail-closed,不会回退成金币(currency.proto),
    # 漏传会让每一笔 P2P 结算都报 ERR_INVALID_ARG。
    from pandora.common.v1 import currency_pb2

    assert req.price_amount.amount == 500
    assert req.price_amount.kind == currency_pb2.CURRENCY_KIND_GOLD
    assert [(g.item_config_id, g.count) for g in req.seller_items] == [(1001, 2)]
    assert [(g.item_config_id, g.count) for g in req.buyer_items] == [(2002, 1)]


def test_settle_insufficient_maps_to_trade_insufficient() -> None:
    """必须映射成 ErrTradeInsufficient —— biz 靠这个码把订单置 FAILED 终态。"""
    from pandora.common.v1 import errcode_pb2

    ledger = _ledger_with(_FakeChannel(code=errcode_pb2.ERR_INVENTORY_INSUFFICIENT))
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(ledger.settle(_order(), 901))
    assert ei.value.code == errcode.ErrTradeInsufficient


def test_settle_unexpected_code_is_passed_through_not_flattened() -> None:
    """未预期码原样透传,**不能**折成 ErrTradeInsufficient(那会把订单错置 FAILED)。"""
    from pandora.common.v1 import errcode_pb2

    ledger = _ledger_with(_FakeChannel(code=errcode_pb2.ERR_INVALID_ARG))
    with pytest.raises(errcode.PandoraError) as ei:
        asyncio.run(ledger.settle(_order(), 901))
    assert ei.value.code == errcode.ErrInvalidArg
    assert ei.value.code != errcode.ErrTradeInsufficient


def test_settle_transport_error_is_not_swallowed() -> None:
    """传输错误原样上抛:结算**可能已生效**,biz 据此把订单留在 SELLER_CONFIRMED。

    若在这里吞成 INSUFFICIENT,订单会被置 FAILED 而资产可能已经过户 —— 钱货两清
    却显示交易失败,且没有任何日志指向根因。
    """
    boom = RuntimeError("inventory unreachable")
    ledger = _ledger_with(_FakeChannel(raise_exc=boom))
    with pytest.raises(RuntimeError):
        asyncio.run(ledger.settle(_order(), 901))
    # 不是 PandoraError → biz 的 as_code 得到 ErrUnknown ≠ ErrTradeInsufficient,
    # 走"瞬时可重试"分支而不是终态 FAILED。
    assert errcode.as_code(boom) != errcode.ErrTradeInsufficient


def test_audit_kafka_key_is_order_id() -> None:
    """kafka key = order_id:同一订单的审计事件必须落同一 partition 才有序。"""

    class _P:
        def __init__(self) -> None:
            self.sent: list[tuple[str, int]] = []

        async def send(self, key, msg) -> None:  # noqa: ANN001
            self.sent.append((key, msg.order_id))

    p = _P()
    asyncio.run(tmain.KafkaTradeAuditPusher(p).push_audit(_order()))
    assert p.sent == [("901", 901)]


def test_audit_topic_matches_go_build_topic() -> None:
    """topic 名写错一个字符两侧都不报错(Kafka 自动建 topic,消费端只是"没有消息")。"""
    assert tmain.AUDIT_TOPIC == "pandora.trade.audit"
