"""inventory 服务私有配置 —— 对应 Go 侧 services/economy/inventory/internal/conf/conf.go。

读的是**同一份** services/economy/inventory/etc/inventory-dev.yaml,不另建配置文件:
迁移期 Go 版和 Python 版并存,运维只该维护一份配置。

★ 默认值必须与 Go 侧 `Config.Defaults()` **逐条同序同判据**。
  判据符号尤其要看:Go 里 `hourly_journal_quota` 用的是 `== 0`(负值 = 显式关闭配额),
  其余全是 `<= 0`;`capacity_purchases` 判的是 **nil**(未写)而不是"空列表"
  —— 显式写成空列表 = "全部段都不可买",是合法配置,不能被默认值覆盖回去。
  默认值分叉的后果不是报错,是同一份 yaml 在两个实现上跑出不同行为而**两边都不报错**。

★ 本文件里几个"零值不是禁用"的坑,逐个写清楚(它们全都不会在测试里露头):

    inventory.capacity <= 0
        = 实例背包**未启用**(安全默认)。main.py 因此不装配 snowflake、不查
        player_item_instance 的表结构,GrantInstances 一律返回 ErrInvalidArg。
        Validate() **刻意不校验本键**:写成 0 / 负数 / 拼错键名都不报错、服务照常启动、
        行为与没配一模一样。唯一判据是启动日志出现 `instance_bag_enabled capacity=200`。
        (2026-08-06 定谳:本键从来没配过,导致掉落落地端一直是断的。)

    sweep_interval = 0
        清理循环会退化成忙等(每轮立刻返回,烧满一个核)。

    sweep_batch = 0
        每轮删 0 行 —— 循环在跑、日志正常、表永远不清。

    retention_mode 拼错
        Go 会 fail-fast 拒启,**不会**静默回落 report_only:运维以为开了清理、
        实际一行没删,库继续无界增长且启动期毫无痕迹。

端口默认值(20015/21015)同样是契约:Envoy 的 cluster、run_services.ps1 的端口占用
检查、K8s Service 都钉在这两个数上。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, Field

from pandorapy import config as pconfig
from pandorapy import dbguard

DEFAULT_GRPC_ADDR = ":20015"
DEFAULT_HTTP_ADDR = ":21015"

# 与 Go 侧 Defaults() 一一对应的默认值。
DEFAULT_SWEEP_INTERVAL = "5m"
DEFAULT_SWEEP_BATCH = 500
DEFAULT_LEDGER_RETENTION_DAYS = 90
DEFAULT_ESCROW_RETENTION_DAYS = 90

DEFAULT_BAG_MAX_JOURNAL_BATCH = 64
DEFAULT_BAG_MAX_ITEMS_PER_OP = 64
DEFAULT_BAG_HOURLY_JOURNAL_QUOTA = 2000
DEFAULT_BAG_DEFAULT_MAX_STACK = 99
DEFAULT_BAG_MIGRATION_BATCH = 200
DEFAULT_BAG_JOURNAL_RETENTION_DAYS = 90


class SessionGateConf(BaseModel):
    """会话现行性门参数 —— 对应 Go 的 pkg/config.SessionGateConf。

    ★ 本该建在 pandorapy/config.py 的 BaseConf 上(Go 侧它就在 config.Base 里),
      但那是本批次禁改的共享文件,所以先在这里建模(与 mail / trade 同样处理)。
      不建模的后果不是"读不到":BaseConf 是 extra="allow",session_gate 会整段
      落进 model_extra —— yaml 里 require=true 写着,Python 侧当没看见,
      **prod 的强制档静默退化成 dev 宽松档**。
    """

    require: bool = False
    # 仅 hub_allocator 使用;inventory 不读它,建模只为不落进 extra。
    require_ticket_sjti: bool = False


class DSAuthConf(BaseModel):
    """DS 回调令牌校验参数 —— 对应 Go 的 pkg/config.DSAuthConf。

    inventory 只在**背包域**(BagService)上用它:owner DS 直写背包时携带 DS 凭据令牌,
    验签抽出 pod/uid 与 owner 记录 target 全等校验(bag phase 2 五要件①)。
    背包域在 Python 侧尚未实现(见 main.py 头注释),这里建模只为把整段接住,
    不让它落进 model_extra 后被误读成"没配"。
    """

    mode: str = ""
    issuer: str = ""
    audience: str = ""
    secret: str = ""
    additional_secrets: list[str] = Field(default_factory=list)
    battle_token_ttl: str = ""
    hub_token_ttl: str = ""


class IdentifyAttrRoll(BaseModel):
    """鉴定属性池里的一条候选属性(值在 [min,max] 均匀 roll)。"""

    attr_id: int = 0
    min: int = 0
    max: int = 0


class IdentifyRule(BaseModel):
    """某配置装备鉴定时的随机属性规则。

    ⚠️ 生产路径**不走这里**:正式进程强制加载 configtable equipment_affix
    (main.py 缺 config_table.dir 直接拒启)。本结构只保留给未装配 Catalog 的旧单测,
    但 Validate() 仍然校验它 —— yaml 里写了非法值必须拒启,而不是"反正不生效"。
    """

    item_config_id: int = 0
    attr_count: int = 0
    pool: list[IdentifyAttrRoll] = Field(default_factory=list)


class BagSectionCapacityRule(BaseModel):
    """后端驻留段容量(仓库 / 活动段;未配置的段 fail-closed 拒写)。"""

    bag_type: int = 0
    capacity: int = 0


class BagItemStackRule(BaseModel):
    """可堆叠道具的单格堆叠上限覆盖(未列道具用 default_max_stack)。"""

    item_config_id: int = 0
    max_stack: int = 0


class BagCapacityTier(BaseModel):
    """一档扩容(阶梯价;第 N 档 = tiers[N-1])。"""

    slots: int = 0
    price_gold: int = 0


class BagCapacityPurchaseRule(BaseModel):
    """某段的容量购买规则(bag-domain.md §5.3)。"""

    bag_type: int = 0
    max_extra: int = 0
    tiers: list[BagCapacityTier] = Field(default_factory=list)


class BagConf(BaseModel):
    """背包域配置(bag-domain.md §4/§5)。对应 Go 的 conf.BagConf。

    dsn 为空 = 未启用(Go 侧不注册 BagService,现网行为不变,安全默认)。
    """

    dsn: str = ""
    # 与 Go bag 独立 DSN 同名；由 mysql_client_conf() 一次性翻译成共享 mysqlx seam，
    # 不能让中心 TLS/小池字段被 pydantic 静默丢弃。
    tls_ca_file: str = ""
    tls_server_name: str = ""
    max_open_conns: int = 0
    max_idle_conns: int = 0
    conn_max_lifetime: str = ""
    conn_max_idle_time: str = ""
    ping_timeout: str = ""
    owner_addr: str = ""
    allow_unverified_owner: bool = False
    max_journal_batch: int = 0
    max_items_per_op: int = 0
    hourly_journal_quota: int = 0
    section_capacities: list[BagSectionCapacityRule] = Field(default_factory=list)
    default_max_stack: int = 0
    item_max_stacks: list[BagItemStackRule] = Field(default_factory=list)
    # ★ 默认必须是 None 而不是 []:Go 判的是 `== nil`。
    #   显式写 `capacity_purchases: []` 的意思是"全部段都不可买",
    #   用 [] 当默认值会让这份意图被默认档位悄悄覆盖回去。
    capacity_purchases: list[BagCapacityPurchaseRule] | None = None
    journal_retention_days: int = 0
    legacy_migration_enabled: bool = False
    migration_batch: int = 0

    # ── 查询辅助(对应 Go 的同名方法)──

    def mysql_client_conf(self) -> pconfig.MySQLConf:
        """把 bag 独立连接配置一次性翻译成共享 mysqlx seam。对应 Go 的 MySQLClientConf()。

        只传 DSN 会**静默丢掉**中心 MySQL 的 TLS 身份(tls_ca_file/tls_server_name)与
        小池参数(max_open/conn_max_lifetime/ping_timeout)—— 配了不生效且不报错,
        正是 mysqlx.pool_kwargs 文件头点名的那种坑。
        """
        return pconfig.MySQLConf(
            dsn=self.dsn,
            tls_ca_file=self.tls_ca_file,
            tls_server_name=self.tls_server_name,
            max_open_conns=self.max_open_conns,
            max_idle_conns=self.max_idle_conns,
            conn_max_lifetime=self.conn_max_lifetime,
            conn_max_idle_time=self.conn_max_idle_time,
            ping_timeout=self.ping_timeout,
        )

    def section_capacity_of(self, bag_type: int) -> int:
        """某段容量(0 = 未配置,调用侧 fail-closed)。"""
        for rule in self.section_capacities:
            if rule.bag_type == bag_type:
                return rule.capacity
        return 0

    def item_max_stack_of(self, item_config_id: int) -> int:
        """某道具单格堆叠上限(覆盖表优先,否则 default_max_stack)。

        0 = 配置非法,调用侧 fail-closed 拒写,**不静默无限合并**。
        """
        for rule in self.item_max_stacks:
            if rule.item_config_id == item_config_id:
                return rule.max_stack
        return self.default_max_stack

    def capacity_purchase_rule_of(self, bag_type: int) -> BagCapacityPurchaseRule | None:
        """某段的容量购买规则(不存在 → None = 不可购买)。"""
        for rule in self.capacity_purchases or ():
            if rule.bag_type == bag_type:
                return rule
        return None

    def validate_rules(self) -> None:
        """校验背包域配置(启动 fail-fast)。对应 Go 的 BagConf.Validate()。

        不设这道闸会怎样:段容量写 0 会让整段变成"永远装不下",而调用方看到的是
        普通的容量满错误;堆叠上限写 0 会让服务端权威拆堆算出 0 个格子;
        档位 slots 之和超过 max_extra 会让最后几档买了不给格子(玩家付了钱没东西)。
        全部都是"配置非法但服务照常跑"的形状,所以必须在启动期拒。
        """
        seen_section: set[int] = set()
        for i, rule in enumerate(self.section_capacities):
            if rule.bag_type in seen_section:
                raise ValueError(f"bag.section_capacities[{i}]: duplicate bag_type {rule.bag_type}")
            seen_section.add(rule.bag_type)
            if rule.capacity == 0:
                raise ValueError(
                    f"bag.section_capacities[{i}]: capacity must be > 0 (bag_type {rule.bag_type})"
                )

        seen_item: set[int] = set()
        for i, rule in enumerate(self.item_max_stacks):
            if rule.item_config_id == 0:
                raise ValueError(f"bag.item_max_stacks[{i}]: item_config_id must not be 0")
            if rule.item_config_id in seen_item:
                raise ValueError(
                    f"bag.item_max_stacks[{i}]: duplicate item_config_id {rule.item_config_id}"
                )
            seen_item.add(rule.item_config_id)
            if rule.max_stack == 0:
                raise ValueError(
                    f"bag.item_max_stacks[{i}]: max_stack must be > 0 (item {rule.item_config_id})"
                )

        seen_purchase: set[int] = set()
        for i, rule in enumerate(self.capacity_purchases or ()):
            # §5.3 拍板:仅身上(0)/仓库(1)可买;装备栏/临时格/活动段(代际清空)不可买。
            if rule.bag_type not in (0, 1):
                raise ValueError(
                    f"bag.capacity_purchases[{i}]: bag_type {rule.bag_type} not purchasable "
                    f"(§5.3 只准 0/1)"
                )
            if rule.bag_type in seen_purchase:
                raise ValueError(f"bag.capacity_purchases[{i}]: duplicate bag_type {rule.bag_type}")
            seen_purchase.add(rule.bag_type)
            if self.section_capacity_of(rule.bag_type) == 0:
                raise ValueError(
                    f"bag.capacity_purchases[{i}]: bag_type {rule.bag_type} has no base capacity "
                    f"in section_capacities"
                )
            if not rule.tiers:
                raise ValueError(
                    f"bag.capacity_purchases[{i}]: tiers required (bag_type {rule.bag_type})"
                )
            total_slots = 0
            for j, tier in enumerate(rule.tiers):
                if tier.slots == 0:
                    raise ValueError(f"bag.capacity_purchases[{i}].tiers[{j}]: slots must be > 0")
                if tier.price_gold <= 0:
                    raise ValueError(
                        f"bag.capacity_purchases[{i}].tiers[{j}]: price_gold must be > 0"
                    )
                total_slots += tier.slots
            if total_slots > rule.max_extra:
                raise ValueError(
                    f"bag.capacity_purchases[{i}]: tiers total slots {total_slots} exceed "
                    f"max_extra {rule.max_extra} (bag_type {rule.bag_type})"
                )


class InventoryConf(BaseModel):
    """inventory 私有配置段。对应 Go 的 conf.InventoryConf。

    ★ 道具的类型 / 堆叠 / 售价 / 局内可消费**一律来自 configtable item 表**
      (与 UE 同源)。曾经的 `item_rules` YAML 表已整块删除,不要再加回来 ——
      一份可能与客户端漂移的数值参与扣减 / 入账,比拒掉一次请求危险得多。
    """

    # capacity 装备实例背包格子容量。<=0 = 未启用实例背包(见模块头注释的坑)。
    capacity: int = 0

    # identify_rules / default_identify_rule 只留给未装配 Catalog 的旧单测兼容。
    identify_rules: list[IdentifyRule] = Field(default_factory=list)
    default_identify_rule: IdentifyRule | None = None

    # ── 保留期清理(CLAUDE.md §9 不变量 24:只增表必须有界)──
    sweep_interval: str = ""
    sweep_batch: int = 0
    # ledger_retention_days:必须远大于一切发放/使用/出售/结算的重试窗口(分钟级),
    # 另须 ≥ mail 邮件最长可领窗口(mail 发送侧已把邮件寿命钳到 claim_retention_days
    # 内闭环,不依赖本流水永久兜底)。
    ledger_retention_days: int = 0
    # escrow_retention_days:只删 closed 行;active 行永不清理
    # (EnsureAuctionEscrow 核对遗留订单依赖其存在性)。
    escrow_retention_days: int = 0
    # retention_mode 留空 / "report_only" = 只统计待清理量并 WARN,一行都不删。
    retention_mode: str = ""

    def sweep_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.sweep_interval)

    def identify_rule_of(self, item_config_id: int) -> IdentifyRule | None:
        """某装备的鉴定规则(不存在 → default_identify_rule,仍无 → None)。"""
        for rule in self.identify_rules:
            if rule.item_config_id == item_config_id:
                return rule
        return self.default_identify_rule

    def retention_mode_parsed(self) -> dbguard.Mode:
        """生效的清理模式。解析失败回落 REPORT_ONLY —— **任何不确定都不删**。

        (启动期已由 validate_retention_mode 拒过非法值,这里只是二重保险。)
        """
        try:
            return dbguard.parse_mode(self.retention_mode)
        except ValueError:
            return dbguard.Mode.REPORT_ONLY

    def validate_retention_mode(self) -> None:
        """供 main 启动 fail-fast。

        不设这道闸会怎样:写了无法识别的模式会静默按 report_only 跑 ——
        运维以为开了删除、实际没开,库继续涨且无人知。
        """
        dbguard.parse_mode(self.retention_mode)

    def validate_rules(self) -> None:
        """校验服务私有规则(启动 fail-fast)。对应 Go 的 InventoryConf.Validate()。

        道具的 usable / sellable / 售价不在此校验:它们来自 configtable item 表,
        由配置表整批校验器把关(缺表 / checksum / 外键异常一律拒启)。

        不设这道闸会怎样:重复的 item_config_id 会让"哪条规则生效"取决于遍历顺序;
        min > max 会让 roll 的区间宽度为负,鉴定出的数值不可预期。
        """
        seen_id: set[int] = set()
        for i, rule in enumerate(self.identify_rules):
            if rule.item_config_id == 0:
                raise ValueError(f"identify_rules[{i}]: item_config_id must not be 0")
            if rule.item_config_id in seen_id:
                raise ValueError(f"identify_rules[{i}]: duplicate item_config_id {rule.item_config_id}")
            seen_id.add(rule.item_config_id)
            if rule.attr_count <= 0:
                raise ValueError(
                    f"identify_rules[{i}]: attr_count must be > 0 (got {rule.attr_count})"
                )
            seen_attr: set[int] = set()
            for j, p in enumerate(rule.pool):
                if p.attr_id == 0:
                    raise ValueError(f"identify_rules[{i}].pool[{j}]: attr_id must not be 0")
                if p.attr_id in seen_attr:
                    raise ValueError(
                        f"identify_rules[{i}].pool[{j}]: duplicate attr_id {p.attr_id}"
                    )
                seen_attr.add(p.attr_id)
                if p.min > p.max:
                    raise ValueError(
                        f"identify_rules[{i}].pool[{j}]: min {p.min} must be <= max {p.max}"
                    )
        rule = self.default_identify_rule
        if rule is not None:
            if rule.item_config_id != 0:
                raise ValueError(
                    f"default_identify_rule.item_config_id must be 0 (got {rule.item_config_id})"
                )
            _validate_identify_pool("default_identify_rule", rule)


def _validate_identify_pool(path: str, rule: IdentifyRule) -> None:
    """兜底规则的池校验(对应 Go 的 validateIdentifyPool)。

    与 identify_rules 的差别:兜底规则**必须有非空池**(它没有"按 item 找不到就跳过"
    的退路,空池等于把每件装备写成 identified=true 且零词条)。
    """
    if rule.attr_count <= 0:
        raise ValueError(f"{path}.attr_count must be > 0 (got {rule.attr_count})")
    if not rule.pool:
        raise ValueError(f"{path}.pool must not be empty")
    seen: set[int] = set()
    for i, p in enumerate(rule.pool):
        if p.attr_id == 0:
            raise ValueError(f"{path}.pool[{i}]: attr_id must not be 0")
        if p.attr_id in seen:
            raise ValueError(f"{path}.pool[{i}]: duplicate attr_id {p.attr_id}")
        seen.add(p.attr_id)
        if p.min > p.max:
            raise ValueError(f"{path}.pool[{i}]: min {p.min} must be <= max {p.max}")


class Config(pconfig.BaseConf):
    """inventory 服务的完整配置。对应 Go 的 conf.Config。"""

    inventory: InventoryConf = Field(default_factory=InventoryConf)
    bag: BagConf = Field(default_factory=BagConf)
    ds_auth: DSAuthConf = Field(default_factory=DSAuthConf)
    session_gate: SessionGateConf = Field(default_factory=SessionGateConf)

    def apply_defaults(self) -> None:
        """填默认值 —— 对应 Go 的 Defaults(),**逐条同序同判据**。"""
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR

        inv = self.inventory
        if inv.sweep_interval_td().total_seconds() <= 0:
            inv.sweep_interval = DEFAULT_SWEEP_INTERVAL
        if inv.sweep_batch <= 0:
            inv.sweep_batch = DEFAULT_SWEEP_BATCH
        if inv.ledger_retention_days <= 0:
            inv.ledger_retention_days = DEFAULT_LEDGER_RETENTION_DAYS
        if inv.escrow_retention_days <= 0:
            inv.escrow_retention_days = DEFAULT_ESCROW_RETENTION_DAYS

        bag = self.bag
        if bag.max_journal_batch <= 0:
            bag.max_journal_batch = DEFAULT_BAG_MAX_JOURNAL_BATCH
        if bag.max_items_per_op <= 0:
            bag.max_items_per_op = DEFAULT_BAG_MAX_ITEMS_PER_OP
        # ★ 判据是 `== 0` 而不是 `<= 0`:负值 = **显式关闭**每小时配额(仅测试用)。
        #   改成 `<= 0` 会把 "-1 关闭配额" 悄悄改写成 2000,配置意图被反转且无日志。
        if bag.hourly_journal_quota == 0:
            bag.hourly_journal_quota = DEFAULT_BAG_HOURLY_JOURNAL_QUOTA
        if not bag.section_capacities:
            # base 容量(§5.3):身上 100(LoadBag 权威下发随身段容量)+ 仓库 200。
            bag.section_capacities = [
                BagSectionCapacityRule(bag_type=0, capacity=100),
                BagSectionCapacityRule(bag_type=1, capacity=200),
            ]
        if bag.capacity_purchases is None:
            # 按 2026-07-22 拍板:身上 10 档 ×10 格(100→200),第 N 档 100N 金币;
            # 仓库 15 档 ×20 格(200→500),第 N 档 200N 金币。
            carry = BagCapacityPurchaseRule(
                bag_type=0,
                max_extra=100,
                tiers=[BagCapacityTier(slots=10, price_gold=100 * n) for n in range(1, 11)],
            )
            warehouse = BagCapacityPurchaseRule(
                bag_type=1,
                max_extra=300,
                tiers=[BagCapacityTier(slots=20, price_gold=200 * n) for n in range(1, 16)],
            )
            bag.capacity_purchases = [carry, warehouse]
        if bag.default_max_stack == 0:
            bag.default_max_stack = DEFAULT_BAG_DEFAULT_MAX_STACK
        if bag.migration_batch <= 0:
            bag.migration_batch = DEFAULT_BAG_MIGRATION_BATCH
        if bag.journal_retention_days <= 0:
            bag.journal_retention_days = DEFAULT_BAG_JOURNAL_RETENTION_DAYS

    @classmethod
    def load(cls, path: str) -> "Config":
        """从 yaml 加载并填默认值。"""
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
