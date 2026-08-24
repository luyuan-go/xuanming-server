"""trade 服务私有配置 —— 对应 Go 侧 internal/conf/conf.go。

读同一份 services/economy/trade/etc/trade-dev.yaml,默认值逐个与 Go 侧对齐。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import Field

from pandorapy import config as pconfig
from pandorapy import kafkax

DEFAULT_GRPC_ADDR = ":20012"
DEFAULT_HTTP_ADDR = ":21012"


class KafkaConf(pconfig.BaseModel):
    """对应 Go 的 pkg/config.KafkaConfig。

    ★ 本该建在共享的 pandorapy/config.py 的 BaseConf 上(Go 侧就在 config.Base 里),
    但那个文件本轮由别的会话持有,所以先落在 trade 私有 conf 上。**必须建模、不能
    留给 BaseConf 的 extra**:落进 extra 的字段"配了却不生效"且零信号 ——
    brokers 写在 yaml 里、审计流一条也没发出去,而日志全绿(RedisConf 那次的同型缺陷)。

    ⚠️ idempotent 默认 **False**,与 Go 的结构体零值一致(Go 侧没有任何地方给它填
    默认值,yaml 注释里的"默认 true"是过期说明)。Python 的 kafkax.ProducerConf
    默认是 True —— 所以这里必须**逐字段显式**构造 ProducerConf,不能只填一半靠默认,
    否则同一份 yaml 在两个实现上跑出不同的幂等语义而两边都不报错。
    """

    brokers: list[str] = Field(default_factory=list)
    group_id: str = ""
    partition_cnt: int = 0
    initial_partition: int = 0
    dial_timeout: str = ""
    read_timeout: str = ""
    write_timeout: str = ""
    retry_max: int = 0
    retry_backoff: str = ""
    channel_buffer: int = 0
    sync_interval: str = ""
    stats_interval: str = ""
    compression_type: str = ""
    idempotent: bool = False
    max_open_requests: int = 0
    retention_ms: int = 0

    def producer_conf(self) -> kafkax.ProducerConf:
        """yaml kafka 段 → kafkax.ProducerConf,对应 Go 的 buildProducerConfig。

        只映射 producer 侧真正被消费的字段;read_timeout / channel_buffer 等
        Python 客户端没有对应旋钮的,**刻意不假装映射**(映射到别的旋钮上会让
        yaml 的意图和实际行为悄悄分叉)。
        """
        return kafkax.ProducerConf(
            brokers=tuple(self.brokers),
            partition_cnt=self.partition_cnt,
            retry_max=self.retry_max,
            retry_backoff_ms=_ms(self.retry_backoff),
            compression_type=self.compression_type,
            idempotent=self.idempotent,
            dial_timeout_ms=_ms(self.dial_timeout),
        )


class SessionGateConf(pconfig.BaseModel):
    """对应 Go 的 pkg/config.SessionGateConf(trade 只用到 require)。

    require=true 是 prod 强制档(gen_cluster_config.ps1 机械置):会话权威端点漏配
    直接拒启,带会话证据的请求在 gate 未装配时一律 fail-closed。
    留空(dev)= 宽松档:仅漏配端点时跳过判定,gate 已装配时顶号 / 登出照常拒。
    """

    require: bool = False


def _ms(raw: str) -> int:
    """Go duration 文本 → 毫秒整数;空 / 零一律 0(= 沿用客户端库默认)。"""
    return int(pconfig.parse_duration(raw).total_seconds() * 1000)


class TradeConf(pconfig.BaseModel):
    """trade 私有段。"""

    # order_ttl 订单 Redis key 存活时长(默认 10m)。应 > order_expire,
    # 给已结算 / 已取消订单留一段查询窗口(ListMyOrders 客户端回看)。
    order_ttl: str = ""
    # order_expire 订单从创建到自动过期的时长(默认 5m)。
    # 超时未完成两阶段确认的订单在被访问时惰性置 EXPIRED。
    order_expire: str = ""
    # optimistic_retry WATCH/MULTI/EXEC 乐观锁最大重试次数(默认 3)。耗尽 → ErrTradeLockFailed。
    optimistic_retry: int = 0
    # max_items_per_order 单订单最大物品条目数(默认 20)。
    max_items_per_order: int = 0
    # rate_quota_per_min 下单/撤单的 per-player 每分钟频率配额(默认 20;负值 = 关闭)。
    # 与 max_orders_per_player 总量闸正交:总量限「同时挂多少」,本值限「刷多快」。
    rate_quota_per_min: int = 0
    # max_orders_per_player 单玩家同时参与的订单总数上限(默认 200,不变量 §18)。
    max_orders_per_player: int = 0
    # max_trade_price 单笔交易金额上限(默认 1_000_000_000,与拍卖 max_price 同量级)。
    #
    # 2026-08-22 货币无符号化时补:此前 trade 只有 `price < 0` 一道闸,**没有任何上界**。
    # CreateOrderRequest.price 已是 uint64,那道闸恒为 False(Python 连类型报错都不会有),
    # 一个 price=-1 的旧请求会被解成 1.8e19 并直接建单、占配额、写 Redis。
    # 上界闸是这条链唯一的防线,必须与 Go 侧 conf.MaxTradePrice 逐字同步。
    max_trade_price: int = 0
    # inventory_addr inventory 服务 gRPC 直连地址。配置后走真实 P2P 原子对转。
    inventory_addr: str = ""
    # allow_noop_ledger 显式允许退回 NoopResourceLedger(结算永远成功、不真实扣转)。
    # 默认 False:未接真实账本即 fail-fast,防止生产漏配后仍以「成交不扣减」静默启动。
    allow_noop_ledger: bool = False

    def order_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.order_ttl)

    def order_expire_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.order_expire)


class Config(pconfig.BaseConf):
    """trade 完整配置。"""

    trade: TradeConf = Field(default_factory=TradeConf)
    # kafka / session_gate 在 Go 侧属于 config.Base(全服务共有);Python 侧的 BaseConf
    # 还没建到它们,先在这里补上。见 KafkaConf 头注释里"为什么不能留给 extra"。
    kafka: KafkaConf = Field(default_factory=KafkaConf)
    session_gate: SessionGateConf = Field(default_factory=SessionGateConf)

    def apply_defaults(self) -> None:
        """默认值必须与 Go 侧 Defaults() 逐个相同 —— 端口尤其重要(Envoy cluster 钉在上面)。"""
        t = self.trade
        if t.order_ttl_td().total_seconds() <= 0:
            t.order_ttl = "10m"
        if t.order_expire_td().total_seconds() <= 0:
            t.order_expire = "5m"
        if t.optimistic_retry <= 0:
            t.optimistic_retry = 3
        if t.max_items_per_order <= 0:
            t.max_items_per_order = 20
        if t.rate_quota_per_min == 0:
            t.rate_quota_per_min = 20
        if t.max_orders_per_player <= 0:
            t.max_orders_per_player = 200
        # 0 = 未配置 → 取默认。刻意**不**把 0 当成"不限价":
        # 一个漏配的字段不该等于拆掉唯一的价格防线。
        if t.max_trade_price <= 0:
            t.max_trade_price = 1_000_000_000
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR

    @classmethod
    def load(cls, path: str) -> "Config":
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
