"""auction 服务私有配置 —— 对应 Go 侧 services/economy/auction/internal/conf/conf.go。

读的是**同一份** services/economy/auction/etc/auction-dev.yaml,不另建配置文件:
迁移期 Go 版与 Python 版并存,运维只该维护一份配置。

★ 默认值必须与 Go 的 `Config.Defaults()` **逐条同序同判据**。判据符号尤其要看:

    rate_quota_per_min      判据是 `== 0`(**负值 = 显式关闭频率配额**)。
                            改成 `<= 0` 会把 "-1 关闭" 悄悄改写成 20,
                            配置意图被反转且没有任何日志。
    market_lock_ttl_seconds 判据是 `<= 0 或 > 30`(不变量 §10:Redis lock TTL ≤ 30s)。
                            只判 `<= 0` 会放行 yaml 里写的 300s —— 一个卡死的实例
                            会把整个 market 锁住 5 分钟,而锁本身"工作正常"。
    其余                    一律 `<= 0`。

★ 几个"零值不是禁用"的坑(全都不会在测试里露头):

    order_ttl_seconds <= 0
        = 挂单**永不过期**。不是"用默认 TTL"。写成 0 的后果是过期清扫循环
        根本不启动(main 里 `if > 0` 才起),冻结的资产永远挂着,
        唯一判据是启动日志**没有** `expiry_sweeper_ready`。

    passive_warmup = true
        = 只读预热档:挂单/出价/撤单全拒,补偿链与过期清扫全停。
        蓝绿 R3 用完必须改回 false 并重启,否则拍卖行看着在跑、实际一单也接不了。
        唯一判据是启动日志的 `auction_passive_warmup_enabled` WARN。

    allow_shard_topology_bootstrap = true
        首次双分片登记后必须立刻恢复 false。留着不会报错,但它是"拓扑漂移"
        这道唯一防线的旁路开关。

    retention_mode 拼错
        Go 会 fail-fast 拒启,**不会**静默回落 report_only:运维以为开了清理、
        实际一行没删,库继续无界增长且启动期毫无痕迹。

★ cell_route 段在 auction 上有两层含义:market_self / market_peers 走 HRW 市场归属
  路由(auction 独有),`mode` / `cells` / `etcd_*` 走玩家分片路由(全局共用)。
  两者都已实现;`mode` 的合法性校验统一走 `cellroute.RouterConfig.validate_mode`。

端口默认值(20016/21016)同样是契约:Envoy 的 cluster、run_services.ps1 的端口
占用检查、K8s Service 都钉在这两个数上。
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from pandorapy import cellroute
from pandorapy import config as pconfig
from pandorapy import dbguard

DEFAULT_GRPC_ADDR = ":20016"
DEFAULT_HTTP_ADDR = ":21016"

# 与 Go 侧 Defaults() 一一对应的默认值(顺序也照抄,便于逐条比对)。
DEFAULT_SHARD_TOPOLOGY_GENERATION = "auction-v1"
DEFAULT_MAX_QUANTITY_PER_ORDER = 1_000_000
DEFAULT_MAX_PRICE = 1_000_000_000
DEFAULT_MAX_ACTIVE_ORDERS_PER_PLAYER = 200
DEFAULT_RATE_QUOTA_PER_MIN = 20
DEFAULT_LIST_LIMIT = 50
DEFAULT_MAX_LIST_LIMIT = 200
DEFAULT_EXPIRY_SWEEP_INTERVAL_SECONDS = 60
DEFAULT_EXPIRY_SWEEP_BATCH = 200
DEFAULT_SIDE_EFFECT_RECONCILE_INTERVAL_SECONDS = 5
DEFAULT_SIDE_EFFECT_RECONCILE_BATCH = 100
DEFAULT_AUDIT_QUEUE_CAPACITY = 1024
DEFAULT_MARKET_LOCK_TTL_SECONDS = 30
MAX_MARKET_LOCK_TTL_SECONDS = 30
DEFAULT_MARKET_LOCK_MAX_WAIT_MS = 3000
DEFAULT_RETENTION_DAYS = 90
DEFAULT_RETENTION_SWEEP_INTERVAL_SECONDS = 3600
DEFAULT_RETENTION_SWEEP_BATCH = 500

# auction 只批准 2 个 MySQL 分片(对齐 Go 的 maxSupportedMySQLShards)。
MAX_SUPPORTED_MYSQL_SHARDS = 2


class KafkaConf(BaseModel):
    """对应 Go 的 `pkg/config.KafkaConfig`(只建模 auction 用得到的字段)。

    ★ 本该建在 pandorapy/config.py 的 BaseConf 上(Go 侧它就在 config.Base 里),
      但那是本批次禁改的共享文件,所以先在这里建模(与 leaderboard / mail 同样处理)。
      不建模的后果不是"读不到":BaseConf 是 extra="allow",kafka 段会整段落进
      model_extra —— yaml 里 brokers 写着,Python 侧当没看见,于是启动期
      `kafka_brokers_required` 这道闸把一份**配置正确**的部署拒之门外。
    """

    model_config = {"extra": "allow"}

    brokers: list[str] = Field(default_factory=list)
    partition_cnt: int = 0
    dial_timeout: str = ""
    write_timeout: str = ""
    retry_max: int = 0
    retry_backoff: str = ""
    compression_type: str = ""
    # ★ Go 结构体零值是 false,但 auction-dev.yaml 显式写了 idempotent: true。
    # Python 默认取 True 与 kafkax.ProducerConf 的默认一致 —— 幂等 producer 是
    # 保序前提,默认关掉会让重试换序而无人察觉。
    # ★ Go 的真实默认是 **False**(结构体零值),不是注释里写的 "默认 true"。
    # pkg/config/config.go:239 的注释是 aspirational 的 —— 全仓没有任何代码给它填值,
    # producer.go:85 直穿 `c.Producer.Idempotent = cfg.Idempotent`。
    # 写成 True 的后果:35 份带 kafka 段的 yaml 里有 15 份没显式配它,
    # 那些服务上两栈的**重复消息语义**不同(幂等 producer 影响重试时的去重),
    # 而两边都不报错。
    idempotent: bool = False
    def dial_timeout_ms(self) -> int:
        """拨号超时(毫秒)。kafkax.ProducerConf 收毫秒,yaml 里是 Go duration 串。"""
        return int(pconfig.parse_duration(self.dial_timeout).total_seconds() * 1000)


class CellRouteConf(BaseModel):
    """对应 Go 的 `pkg/cellroute.RouterConfig`(auction 只用 market_* 两个字段)。

    ★ 建模它是**必需的**:auction 的 main 要读 market_self / market_peers 才能
      装配 HRW 市场归属路由。不建模的话这两个字段落进 model_extra,多实例部署下
      每个实例都以为自己拥有全部 market —— 跨实例锁竞争回到最坏情形,
      而且没有任何日志说明路由没生效(`market_router_enabled` 这条 INFO 不会出现)。
    """

    model_config = {"extra": "allow"}

    mode: str = ""
    self_region: int = 0
    self_cell: int = 0
    # market_peers 是全部 auction 实例 ID(含 self),HRW 市场归属用。
    market_peers: list[str] = Field(default_factory=list)
    # market_self 是本实例 ID;留空退化为"单实例拥有全部市场"。
    market_self: str = ""

    def market_peer_list(self) -> list[str]:
        """归一化 auction market peers(去空去重,确保 self 在内)。

        逐行对应 Go 的 `RouterConfig.MarketPeerList()`。**不许"顺手排序"** ——
        HRW 与顺序无关,但 peers 的成员集合决定每个 market 落谁;
        去重逻辑写歪(比如漏了 self 的补入)会让本实例把自己的 market 判给别人,
        每条写请求都打一条误路由 WARN 而业务照常(锁兜底),排查时极难联想到配置。
        """
        out: list[str] = []
        seen: set[str] = set()
        for peer in self.market_peers:
            peer = peer.strip()
            if not peer:
                continue
            if peer in seen:
                continue
            seen.add(peer)
            out.append(peer)
        self_id = self.market_self.strip()
        if self_id and self_id not in seen:
            out.append(self_id)
        return out

    def validate_mode(self) -> None:
        """转发给基类的同一份判据 —— **不得在这里另写一套**。

        基类 `BaseConf` 的校验器调的就是本方法。auction 为了读 market_* 把
        cell_route 建成了自己的类型,如果这里不提供 `validate_mode`,
        那道全局闸会变成 AttributeError 而不是校验 —— "给模型加一个字段就
        意外关掉一道全局闸"的典型形状。判据本体只能有一份(在 cellroute)。
        """
        cellroute.RouterConfig.model_validate(
            self.model_dump(exclude_none=True)
        ).validate_mode()


class AuctionConf(BaseModel):
    """auction 私有配置段。字段名与 Go 的 yaml tag 逐字一致。"""

    model_config = {"extra": "allow"}

    # 单挂单 / 出价最大数量。防一次挂天量。
    max_quantity_per_order: int = 0
    # 单价上限。防溢出 / 异常价。
    max_price: int = 0
    # 挂单/出价/撤单的 per-player 每分钟频率配额(anti-abuse §6 第 6 项)。
    # 与 max_active_orders_per_player 总量闸正交:总量限"同时挂多少",本值限"刷多快"。
    rate_quota_per_min: int = 0
    # 单玩家 PENDING+OPEN+PARTIALLY_FILLED 订单硬上限。跨 market/分片由 Redis Lua 原子预留。
    max_active_orders_per_player: int = 0
    default_list_limit: int = 0
    max_list_limit: int = 0

    # inventory 内网 gRPC 地址。配了 → 成交走真实结算;留空且 allow_noop_settlement=true
    # 才退回占位账本,否则启动期 fail-fast。
    inventory_addr: str = ""
    allow_noop_settlement: bool = False
    allow_noop_match_events: bool = False

    # 蓝绿 R3 的只读预热门禁,见模块头注释。
    passive_warmup: bool = False

    shard_topology_generation: str = ""
    allow_shard_topology_bootstrap: bool = False

    # 挂单存活时长(秒)。> 0 才启用过期清扫;<= 0 = 永不过期。
    order_ttl_seconds: int = 0
    expiry_sweep_interval_seconds: int = 0
    expiry_sweep_batch: int = 0

    side_effect_reconcile_interval_seconds: int = 0
    side_effect_reconcile_batch: int = 0

    # 弱依赖 audit 的进程内异步队列上限。队列满时只告警丢弃,绝不反压交易主路径。
    audit_queue_capacity: int = 0

    # 保留给旧二进制读取;新实现因 Redis 已是强依赖而**始终**启用跨实例锁。
    cross_instance_lock: bool = False
    market_lock_ttl_seconds: int = 0
    market_lock_max_wait_ms: int = 0

    # ── 保留期清理(CLAUDE.md §9 不变量 24:只增表必须有界)──
    retention_days: int = 0
    retention_sweep_interval_seconds: int = 0
    retention_sweep_batch: int = 0
    retention_mode: str = ""

    def retention_mode_parsed(self) -> dbguard.Mode:
        """生效的清理模式。解析失败回落 REPORT_ONLY —— **任何不确定都不删**。

        (启动期已由 validate_retention_mode 拒过非法值,这里只是二重保险,
        对应 Go 的 `RetentionMode()`。)
        """
        try:
            return dbguard.parse_mode(self.retention_mode)
        except ValueError:
            return dbguard.Mode.REPORT_ONLY

    def validate_retention_mode(self) -> None:
        """供 main 启动 fail-fast(对应 Go 的 `ValidateRetentionMode`)。

        不设这道闸会怎样:写了无法识别的模式会静默按 report_only 跑 ——
        运维以为开了删除、实际没开,三张只增表继续无界增长且无人知。
        """
        dbguard.parse_mode(self.retention_mode)


class Config(pconfig.BaseConf):
    """auction 服务的完整配置。对应 Go 的 conf.Config。"""

    auction: AuctionConf = Field(default_factory=AuctionConf)
    kafka: KafkaConf = Field(default_factory=KafkaConf)
    cell_route: CellRouteConf = Field(default_factory=CellRouteConf)

    def assert_unsupported_sections(self) -> None:
        """★ 保留重写只为一件事:让 auction 自定义的 `CellRouteConf` 走到基类同一道闸。

        基类 `BaseConf` 的校验器调 `self.cell_route.validate_mode()`。auction 为了读
        market_self / market_peers 把 cell_route 换成了自己的类型 —— 那个类型的
        `validate_mode` 直接转发给 `cellroute.RouterConfig`,判据只有一份。

        历史教训(保留在此):基类那道闸曾经读 `model_extra`,而 auction 一建模
        cell_route 就把它变成了**静默空操作** —— 配了 `mode: static` 的部署照常启动、
        所有玩家落在单 Cell 上,而 yaml 看起来一切正常。现在改成读**字段**,
        字段被覆盖也仍然走同一道校验。
        """
        super().assert_unsupported_sections()

    def apply_defaults(self) -> None:
        """填默认值 —— 对应 Go 的 Defaults(),**逐条同序同判据**。"""
        a = self.auction
        if not a.shard_topology_generation:
            a.shard_topology_generation = DEFAULT_SHARD_TOPOLOGY_GENERATION
        if a.max_quantity_per_order <= 0:
            a.max_quantity_per_order = DEFAULT_MAX_QUANTITY_PER_ORDER
        if a.max_price <= 0:
            a.max_price = DEFAULT_MAX_PRICE
        if a.max_active_orders_per_player <= 0:
            a.max_active_orders_per_player = DEFAULT_MAX_ACTIVE_ORDERS_PER_PLAYER
        # ★ 判据是 `== 0` 而不是 `<= 0`:负值 = **显式关闭**频率配额。
        # 改成 `<= 0` 会把 "-1 关闭" 悄悄改写成 20,配置意图被反转且无日志。
        if a.rate_quota_per_min == 0:
            a.rate_quota_per_min = DEFAULT_RATE_QUOTA_PER_MIN
        if a.default_list_limit <= 0:
            a.default_list_limit = DEFAULT_LIST_LIMIT
        if a.max_list_limit <= 0:
            a.max_list_limit = DEFAULT_MAX_LIST_LIMIT
        if a.expiry_sweep_interval_seconds <= 0:
            a.expiry_sweep_interval_seconds = DEFAULT_EXPIRY_SWEEP_INTERVAL_SECONDS
        if a.expiry_sweep_batch <= 0:
            a.expiry_sweep_batch = DEFAULT_EXPIRY_SWEEP_BATCH
        if a.side_effect_reconcile_interval_seconds <= 0:
            a.side_effect_reconcile_interval_seconds = (
                DEFAULT_SIDE_EFFECT_RECONCILE_INTERVAL_SECONDS
            )
        if a.side_effect_reconcile_batch <= 0:
            a.side_effect_reconcile_batch = DEFAULT_SIDE_EFFECT_RECONCILE_BATCH
        if a.audit_queue_capacity <= 0:
            a.audit_queue_capacity = DEFAULT_AUDIT_QUEUE_CAPACITY
        # ★ 上界钳制不能省(不变量 §10):yaml 写 300 时 Go 会钳回 30,
        # 少了后半条判据,Python 版会拿着 5 分钟的锁跑 —— 持锁实例一崩,
        # 那个 market 五分钟内谁也撮合不了,而锁"工作正常"。
        if (
            a.market_lock_ttl_seconds <= 0
            or a.market_lock_ttl_seconds > MAX_MARKET_LOCK_TTL_SECONDS
        ):
            a.market_lock_ttl_seconds = DEFAULT_MARKET_LOCK_TTL_SECONDS
        if a.market_lock_max_wait_ms <= 0:
            a.market_lock_max_wait_ms = DEFAULT_MARKET_LOCK_MAX_WAIT_MS
        if a.retention_days <= 0:
            a.retention_days = DEFAULT_RETENTION_DAYS
        if a.retention_sweep_interval_seconds <= 0:
            a.retention_sweep_interval_seconds = DEFAULT_RETENTION_SWEEP_INTERVAL_SECONDS
        if a.retention_sweep_batch <= 0:
            a.retention_sweep_batch = DEFAULT_RETENTION_SWEEP_BATCH
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR

    @classmethod
    def load(cls, path: str) -> "Config":
        """从 yaml 加载并填默认值。"""
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
