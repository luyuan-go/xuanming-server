"""Pandora auction 服务入口(Python 版)—— 对应 Go 侧
services/economy/auction/cmd/auction/main.go。

auction 是**全服拍卖行**:玩家的道具与金币在这里易手。它带病上线的后果不是
"某个功能不好用",而是**资产凭空出现或消失**,而且往往要到对账时才发现。
所以启动闸一条都不能省。

★ 启动闸(逐条对应 Go 的 os.Exit / Must* / Errorw,**事件名逐字相同** ——
  Loki 上按事件名建的告警对不上就是静默失去覆盖;**顺序也与 Go 相同** ——
  顺序不同会让同一份坏配置在两栈上报不同的第一个错误):

    ①  abs_conf_path_failed                    fail-fast  配置路径解析
    ②  config_load_failed                      fail-fast  读不到 / 解析不了 yaml
    ③  config_scan_failed                      fail-fast  结构对不上(含 cell_route.mode 未实现闸)
    ④  auction_retention_mode_invalid          fail-fast  清理模式拼错(会静默不删)
    ⑤  auction_mysql_shard_count_unsupported   fail-fast  分片数 > 2
    ⑥  auction_mysql_single_shard_list_invalid fail-fast  shards 只写了 1 个
    ⑦  mysql_required / mysql_shardset_failed /
        mysql_connect_failed                   fail-fast  撮合权威库连不上
    ⑧  mysql_strict_mode_required              fail-fast  **逐分片**;非严格 = 静默截断
    ⑨  auction_mysql_shard_topology_rejected   fail-fast  分片拓扑漂移 / 未授权 bootstrap
    ⑩  redis_endpoint_required                 fail-fast  跨实例锁 + 名额索引不可降级
    ⑪  redis_ping_failed                       fail-fast  带着死 Redis Ready 会在第一条业务命令才暴露
    ⑫  snowflake_init_failed /
        snowflake_nodeid_acquire_failed        fail-fast  重号 = 幂等键撞车
    ⑬  kafka_brokers_required                  fail-fast  成交事件被静默禁用
    ⑭  settlement_ledger_missing               fail-fast  "成交不结算"静默启动
    ⑮  app_run_failed                          fail-fast  服务跑不起来

  方向也与 Go 一致:kafka producer 初始化失败只 WARN(outbox 会重试),
  audit producer 失败只 WARN(弱依赖),容量巡检超预算只告警不阻断。

★ 后台循环七条,全部走 `pandorapy.safego`(裸 create_task 的协程死掉后进程照跑、
  health 照答 SERVING、**零日志**):

    1. 容量巡检          **逐分片各一条**(各分片可能连不同实例,预算是按单分片给的)
    2. 副作用补偿        结算 / escrow 释放 / PENDING 恢复 / legacy 验证 / 撮合续跑
    3. 成交事件 outbox   **必须与 2 分开**:同步 producer 无法被取消可靠中断,
                         broker 故障不能拖住资产补偿
    4. 保留期清理        逐分片批删,默认只报告不删
    5. 过期清扫          仅 order_ttl_seconds > 0
    6. audit worker      弱依赖有界队列(在 AuctionUsecase 内)
    7. market 锁续租     每次持锁一条(在 market_locker 内),续租失败 fail-stop 退出进程

  `passive_warmup=true` 时 2/3/4/5 全部**不启动**(只读预热档),并打一条刺眼 WARN。

运行:
    cd services/economy/auction
    PYTHONPATH=../../../python:../../../python/gen \\
        python -m pandorapy.services.auction.main -conf etc/auction-dev.yaml
"""

from __future__ import annotations

# ★ 必须最先 import:Windows 控制台默认 cp1252,日志里的中文会抛 UnicodeEncodeError
# 并把真正的启动错误顶掉(实测踩过多次)。见 pandorapy/_utf8.py。
from pandorapy import _utf8  # noqa: F401  isort:skip
from pandorapy import config as pconfig

import argparse
import asyncio
import contextlib
import pathlib
import sys

import asyncmy
from pandora.auction.v1 import auction_pb2_grpc

from pandorapy import dbguard
from pandorapy import kafkax
from pandorapy import log as plog
from pandorapy import mysqlx
from pandorapy import redisx
from pandorapy import safego
from pandorapy import server as pserver
from pandorapy import snowflake as psnowflake
from pandorapy import snowflake_etcd as psnowflake_etcd
from pandorapy.services.auction import biz as abiz
from pandorapy.services.auction import book as abook
from pandorapy.services.auction import budgets as abudgets
from pandorapy.services.auction import conf as aconf
from pandorapy.services.auction import events as aevents
from pandorapy.services.auction import ledger as aledger
from pandorapy.services.auction import market_locker as alocker
from pandorapy.services.auction import owner_slots as aslots
from pandorapy.services.auction import repo as arepo
from pandorapy.services.auction import retention as aretention
from pandorapy.services.auction import service as asvc
from pandorapy.services.auction import shard_topology as atopology
from pandorapy.services.auction.market_router import MarketRouter

SERVICE_NAME = "auction"
HTTP_DEFAULT_PORT = 21016
DB_SCHEMA = arepo.DB_SCHEMA

# 容量巡检节拍(Go: time.NewTicker(time.Hour))。
HOUR_SEC = 3600.0

# 启动期 DB 调用的超时,对齐 Go 的 context.WithTimeout(5s)。
# ★ 没有超时时库"半死不活"(TCP 通但不回包)会让进程**挂着不退** ——
# 表现是 Pod 一直卡在启动中,而 k8s 只看到「未 Ready」,排障的人根本不知道卡在哪道闸。
STARTUP_DB_TIMEOUT_SEC = 5.0

# 频率配额窗口固定 1 分钟(与 Go 的 `Window: time.Minute` 同)。
RATE_QUOTA_WINDOW_SEC = 60.0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线)而不是 argparse 惯常的 `--conf`:Go 侧 flag 包用单横线,
    而 run_services.ps1 / start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。
    Python 版必须能被**同一条命令行**拉起,否则那些脚本都要改。
    """
    ap = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    ap.add_argument("-conf", dest="conf", default="etc/auction-dev.yaml")
    return ap.parse_args(argv)


async def _run_capacity_guard(pool, schema: str, shard_index: int) -> None:  # noqa: ANN001
    """容量巡检(§9.24):**只告警不阻断**。

    容量超限是"要去查的问题",不是"服务不能跑的理由" ——
    拒绝启动会把容量问题升级成可用性事故。

    走 information_schema 估算(毫秒级、不锁表、不扫数据);
    绝不用 COUNT(*)(千万行表几十秒,会拖垮滚动更新)。

    ★ 逐分片各一条:各分片可能连不同 MySQL 实例,而预算是按**单分片**量级给的
    (见 budgets.py)。把总量摊在一个分片上等于把阈值放宽了 N 倍。
    """
    budgets = abudgets.auction_budgets()

    async def _once() -> None:
        async with pool.acquire() as conn:
            result = await dbguard.check_budgets(conn, schema, budgets)
        dbguard.log_violations(result, db=f"{schema}#{shard_index}")

    # 启动即一轮拿基线(上线时就已超限当场可见),再进周期循环(与 Go 的 runCapacityGuard 同序)。
    await safego.run_once("db_capacity_guard_initial", _once)
    await safego.loop("db_capacity_guard", HOUR_SEC, _once)


async def _run_now_then_loop(name: str, interval_sec: float, fn) -> None:  # noqa: ANN001
    """先跑一轮再进周期循环(对应 Go 那两个 reconciler 的 `run(); ticker...`)。

    ★ "先跑一轮"不能省:补偿链的第一要务是**恢复上个进程遗留的意图**
    (已冻结未激活的 PENDING、已成交未结算的 match)。空等一个周期意味着
    每次重启都让玩家的资产多锁 interval 秒。
    """
    await safego.run_once(name, fn)
    await safego.loop(name, interval_sec, fn)


def _build_pools_plan(cfg: aconf.Config, logger) -> tuple[list[str], str] | None:  # noqa: ANN001
    """决定用分库还是单库,并返回**有序** DSN 列表。返回 None 表示已打过闸日志、应退出。

    ★ 顺序是拓扑身份的一部分(shard_index 按下标写进 marker),
      调换两个 DSN 的顺序 = 全部历史数据被路由到另一个库。
    """
    shards = list(cfg.node.mysql_client.shards)
    if len(shards) > aconf.MAX_SUPPORTED_MYSQL_SHARDS:
        # ⑤ 扩到更多分片前必须先完成 owner idempotency registry 回填与完成标记,
        # 否则跨分片幂等查询会漏掉尚未回填的历史订单 → 同一 idem 重复挂单。
        logger.error(
            "auction_mysql_shard_count_unsupported",
            shards=len(shards),
            max_supported=aconf.MAX_SUPPORTED_MYSQL_SHARDS,
            hint="扩到更多分片前必须完成 owner idempotency registry 回填与完成标记",
        )
        return None
    if len(shards) == 1:
        # ⑥ 单库写 shards 会让路由公式变成 `id % 1`(恒 0)却仍走分库代码路径,
        # 将来加第二片时全部历史数据都在错的位置 —— 现在拒掉比那时迁移便宜得多。
        logger.error(
            "auction_mysql_single_shard_list_invalid",
            hint="单库必须使用 node.mysql_client.dsn;shards 只允许恰好 2 个有序 DSN",
        )
        return None
    if shards:
        return shards, "sharded"
    if cfg.node.mysql_client.dsn:
        return [cfg.node.mysql_client.dsn], "single"
    logger.error(
        "mysql_required",
        hint="node.mysql_client.dsn or .shards required (pandora_auction)",
    )
    return None


async def _main_async(args: argparse.Namespace) -> int:  # noqa: C901 —— 与 Go 同为线性启动闸
    # 1. Logger
    logger = plog.setup(SERVICE_NAME)
    logger.info("service_starting", conf=args.conf)

    # ── ① 配置路径 ───────────────────────────────────────────────────────
    try:
        conf_path = pathlib.Path(args.conf).resolve()
    except OSError as exc:
        logger.error("abs_conf_path_failed", err=str(exc))
        return 1

    # ── ②③ 加载 + 结构校验 ───────────────────────────────────────────────
    try:
        cfg = aconf.Config.load(conf_path)
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        # Go 把"读不到文件"和"结构对不上"分成两个事件名,这里保持同样区分。
        # 「配了 cell_route.mode 但 Python 侧只实现单 Cell」的拒启也落在这一支
        # (那道闸挂在 conf.Config.assert_unsupported_sections 上,加载时就抛了)。
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── ④ 保留期清理模式 ─────────────────────────────────────────────────
    # 拼错的值会静默回落 report_only:运维以为开了清理、实际一行没删,
    # 三张只增表继续无界增长且启动期毫无痕迹。
    try:
        cfg.auction.validate_retention_mode()
    except ValueError as exc:
        logger.error(
            "auction_retention_mode_invalid",
            err=str(exc),
            hint='auction.retention_mode 只接受 "report_only"(默认,不删) 或 "delete"',
        )
        return 1

    # ── ⑤⑥ 分库形态 ─────────────────────────────────────────────────────
    plan = _build_pools_plan(cfg, logger)
    if plan is None:
        return 1
    dsns, mode = plan

    # ── ⑦ 连库(强依赖:撮合权威库 pandora_auction)────────────────────
    pools: list = []
    pool_schemas: list[str] = []
    try:
        for index, dsn in enumerate(dsns):
            conn_cfg = mysqlx.parse_go_dsn(dsn, default_db=DB_SCHEMA)
            try:
                # ★ 池参数走 mysqlx.pool_kwargs 统一翻译 —— max_open/max_idle/
                # conn_max_lifetime/ping_timeout 四个字段 yaml 里都写了、Go 侧都读,
                # 手写 create_pool 只传连接身份的话它们**配了不生效且不报错**。
                # 最要紧的是 conn_max_lifetime:没有 pool_recycle,长空闲连接撞上
                # MySQL 的 wait_timeout 被服务端断掉,客户端不知道,
                # **下一条业务 SQL 才暴露**。
                #
                # ★ autocommit=True:与 Go 的 database/sql **默认语义**一致。
                # auction 的每一条需要多语句原子性的路径都显式 begin()
                # (repo._transaction:ClaimOrder 的 coordinator 事务、ReserveMatch);
                # 其余是单条条件 UPDATE / SELECT,CAS 判定靠 rowcount,
                # 建成 False 会让每条只读 SELECT 也开一个没人提交的事务,
                # 连接归池复用时 REPEATABLE READ 的快照停在上一次 ——
                # **读到陈旧订单状态且零报错**,对撮合是致命的。
                mysqlx.assert_pool_conf_supported(cfg.node.mysql_client)
                pool = await asyncmy.create_pool(
                    **mysqlx.pool_kwargs(cfg.node.mysql_client, conn_cfg, autocommit=True)
                )
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽
                # except 吞掉。吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成
                #   一个正常应答;启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸
                #   的失败,报出假的失败原因。两种都让 §9.16 的
                #   「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                event = "mysql_shardset_failed" if mode == "sharded" else "mysql_connect_failed"
                logger.error(
                    event, err=str(exc), shard_index=index, dsn=mysqlx.mask_dsn(dsn)
                )
                return 1
            pools.append(pool)
            pool_schemas.append(conn_cfg["db"])
        if mode == "sharded":
            logger.info("mysql_connected", mode="sharded", shards=len(pools))
        else:
            logger.info("mysql_connected", mode="single", dsn=mysqlx.mask_dsn(dsns[0]))

        router = arepo.DBRouter(pools, schemas=pool_schemas)

        # ── ⑧ 严格模式断言(§9.24)——**逐分片** ─────────────────────────
        # 各分片可能连不同 MySQL 实例,配置漂移完全可能只出现在个别分片上,只查一个会漏。
        # 非严格 sql_mode 下超长写入被**静默截断**(err=nil 但数据被砍断),
        # 等于无声的数据损坏。
        for index, pool in enumerate(pools):
            try:
                async with pool.acquire() as conn:
                    await asyncio.wait_for(
                        dbguard.assert_strict_mode(conn), timeout=STARTUP_DB_TIMEOUT_SEC
                    )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("mysql_strict_mode_required", shard_index=index, err=str(exc))
                return 1

        # ── ⑨ 分片拓扑门禁 ───────────────────────────────────────────────
        # 片数 / 顺序 / 目标库任一漂移都必须 fail-fast:历史数据会被路由丢失,
        # 而"查不到订单"在业务上看起来只是"这单没了"。
        try:
            await asyncio.wait_for(
                atopology.validate_shard_topology(
                    pools,
                    cfg.auction.shard_topology_generation,
                    dsns,
                    cfg.auction.allow_shard_topology_bootstrap,
                ),
                timeout=STARTUP_DB_TIMEOUT_SEC,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error(
                "auction_mysql_shard_topology_rejected",
                err=str(exc),
                hint="do not change shard count/order/identity; "
                "first 2-shard start requires reviewed bootstrap",
            )
            return 1
        logger.info(
            "auction_mysql_shard_topology_verified",
            generation=cfg.auction.shard_topology_generation,
            shards=len(dsns),
        )

        # ── ⑩ Redis 端点(强依赖:跨实例锁 + owner 名额索引 + 兼容缓存)──
        rc = cfg.node.redis_client
        if not rc.host and not rc.addrs:
            logger.error(
                "redis_endpoint_required",
                hint="set node.redis_client.host (single) or node.redis_client.addrs (cluster)",
            )
            return 1

        # ── ⑪ Ping ───────────────────────────────────────────────────────
        # 不探的话服务会带着一个死 Redis 正常 Ready,k8s 把流量切过来后
        # **第一条业务命令**才暴露 —— 而 auction 的第一条业务命令就是占名额。
        try:
            rdb = await redisx.must_connect(rc)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error(
                "redis_ping_failed", err=str(exc), addr=rc.host, addrs=list(rc.addrs)
            )
            return 1
        logger.info("redis_connected", addr=rc.host, addrs=list(rc.addrs))

        node_holder = None
        events = None
        grpc_ledger = None
        uc = None
        try:
            # ── ⑫ Snowflake(order_id / match_id 两个独立 ID 空间)──────
            # 失租**必须退出进程**,不能降级继续发号:此刻另一个副本可能已经抢到
            # 同一个 nodeID,继续发就是重号 —— order_id 重号会让两张不同的挂单
            # 撞同一个冻结幂等键,第二笔冻结被幂等吸收 → **卖了没扣东西**。
            try:
                order_sf, node_holder = await psnowflake_etcd.provide_node(
                    list(cfg.snowflake.etcd_endpoints),
                    cfg.snowflake.etcd_service_name or SERVICE_NAME,
                    cfg.node.node_id,
                    cfg.snowflake.node_id_source,
                    on_lost=psnowflake_etcd.exit_process_on_lost,
                    **(
                        {"prefix": cfg.snowflake.etcd_prefix}
                        if cfg.snowflake.etcd_prefix
                        else {}
                    ),
                    **(
                        {"lease_ttl_sec": cfg.snowflake.etcd_lease_ttl_sec}
                        if cfg.snowflake.etcd_lease_ttl_sec > 0
                        else {}
                    ),
                )
            except ValueError as exc:
                # static 档的号段闸(拒 node_id=0)在 provide_node 内部,这里只报事件。
                logger.error(
                    "snowflake_init_failed",
                    err=str(exc),
                    node_id=cfg.node.node_id,
                    node_id_source=cfg.snowflake.node_id_source,
                )
                return 1
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "snowflake_nodeid_acquire_failed",
                    err=str(exc),
                    node_id_source=cfg.snowflake.node_id_source,
                    hint="etcd 档抢不到 nodeID 时**不得**退回 static —— 那正好会与别的副本重号",
                )
                return 1
            effective_node_id = (
                node_holder.node_id if node_holder is not None else cfg.node.node_id
            )
            if node_holder is not None:
                logger.info(
                    "snowflake_nodeid_acquired", node_id=node_holder.node_id, source="etcd"
                )
            # ★ 第二个发号器**共用同一 nodeID、各自独立 step 池**
            # (对应 Go 的 etcdnode.MustProvideSnowflakeN(..., 2))。
            # ⚠️ 两个空间会发出**逐位相同**的 ID,分别留在 auction_orders /
            # auction_matches 里,禁止混进同一容器比较。
            match_sf = psnowflake.Node(effective_node_id)

            # ── ⑬ kafka(match 强持久 outbox / audit 弱依赖)──────────
            if cfg.kafka.brokers:
                producer_conf = kafkax.producer_conf_from(cfg.kafka)
                match_producer = None
                audit_producer = None
                try:
                    match_producer = kafkax.KeyOrderedProducer(
                        producer_conf, aevents.MATCH_TOPIC
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001 —— broker 不通不阻塞启动
                    # marker 绝不清除,由可重连 producer 后台重试(见 events.py)。
                    logger.warning(
                        "kafka_match_producer_init_failed_outbox_will_retry", err=str(exc)
                    )
                else:
                    logger.info("kafka_producer_ready", topic=aevents.MATCH_TOPIC)
                try:
                    audit_producer = kafkax.KeyOrderedProducer(
                        producer_conf, aevents.AUDIT_TOPIC
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001 —— audit 是弱依赖
                    logger.warning("kafka_audit_producer_init_failed", err=str(exc))
                else:
                    logger.info("kafka_producer_ready", topic=aevents.AUDIT_TOPIC)
                events = aevents.AuctionEventPusher(
                    producer_conf, match_producer, audit_producer
                )
            elif cfg.auction.allow_noop_match_events:
                logger.warning(
                    "kafka_match_events_explicitly_disabled",
                    hint="allow_noop_match_events=true; only valid for local no-event integration",
                )
            else:
                logger.error(
                    "kafka_brokers_required",
                    hint="configure kafka.brokers; only local no-event integration may set "
                    "auction.allow_noop_match_events=true",
                )
                return 1

            # ── ⑭ 结算账本 ───────────────────────────────────────────────
            if cfg.auction.inventory_addr:
                grpc_ledger = aledger.GrpcInventoryLedger(cfg.auction.inventory_addr)
                ledger = grpc_ledger
                logger.info(
                    "settlement_ledger_ready",
                    mode="inventory_grpc",
                    inventory_addr=cfg.auction.inventory_addr,
                )
            elif cfg.auction.allow_noop_settlement:
                ledger = aledger.NoopSettlementLedger()
                logger.warning(
                    "settlement_ledger_noop",
                    hint="auction.inventory_addr empty; matches settle as no-op "
                    "(allow_noop_settlement=true)",
                )
            else:
                # 漏配 inventory 地址后仍以「成交不结算」启动 = 玩家的道具凭空
                # 出现和消失,而 auction 自己的日志一切正常。
                logger.error(
                    "settlement_ledger_missing",
                    hint="auction.inventory_addr 必填(真实结算);"
                    "仅联调/单测可显式设 auction.allow_noop_settlement=true",
                )
                return 1

            # ── 装配链 ───────────────────────────────────────────────────
            repo = arepo.MySQLAuctionRepo(router)
            book = abook.RedisBookStore(rdb)
            owner_slots = aslots.RedisOwnerSlotLimiter(rdb)
            uc = abiz.AuctionUsecase(
                repo, book, owner_slots, ledger, events, order_sf, match_sf, cfg.auction
            )
            uc.start()  # audit worker(后台循环 6)

            # 频率配额:复用共享 rdb,窗口固定 1 分钟。
            uc.set_rate_quota(
                redisx.ActionQuota(
                    rdb,
                    "auction",
                    cfg.auction.rate_quota_per_min,
                    RATE_QUOTA_WINDOW_SEC,
                )
            )
            logger.info("auction_rate_quota_ready", per_min=cfg.auction.rate_quota_per_min)

            router_obj = MarketRouter.build(
                cfg.cell_route.market_self, cfg.cell_route.market_peer_list()
            )
            if router_obj is not None:
                uc.set_market_router(router_obj)
                logger.info(
                    "market_router_enabled",
                    self=router_obj.self_id(),
                    peers=router_obj.peer_count(),
                )

            # ★ Redis 本身已是强依赖,新实现**始终**装配跨实例 market 锁。
            # 保留 cross_instance_lock 字段只为旧版配置兼容;不能让自定义线上配置
            # 漏字段后静默退回进程锁而破坏单写者。
            if not cfg.auction.cross_instance_lock:
                logger.warning(
                    "cross_instance_lock_forced_on",
                    hint="field retained for old-version compatibility; "
                    "new auction always enables Redis market lock",
                )
            uc.set_market_locker(
                alocker.RedisMarketLocker(
                    rdb,
                    float(cfg.auction.market_lock_ttl_seconds),
                    cfg.auction.market_lock_max_wait_ms / 1000.0,
                )
            )
            logger.info(
                "market_locker_ready",
                mode="redis_cross_instance",
                ttl_s=cfg.auction.market_lock_ttl_seconds,
                max_wait_ms=cfg.auction.market_lock_max_wait_ms,
            )

            svc = asvc.AuctionService(uc)

            # auth_required=False 对应 Go 的 pmw.AuthOptional():
            # Envoy jwt_authn 已在路由层 require JWT,service 层再做一次
            # callerID==0 拦截兜底。这里用 AuthRequired 会把内网联调直接挡死,
            # 而真正的鉴权已经在两处都有。
            grpc_server = pserver.build_grpc_server(cfg.server.grpc, auth_required=False)
            auction_pb2_grpc.add_AuctionServiceServicer_to_server(svc, grpc_server)
            if cfg.server.grpc.enable_reflection:
                pserver.enable_reflection(grpc_server, [asvc.GRPC_SERVICE_FULL_NAME])

            http_app = pserver.build_http_app(SERVICE_NAME)

            background = _build_background(cfg, logger, uc, router, pools)

            def _on_ready() -> None:
                logger.info(
                    "service_ready",
                    grpc=cfg.server.grpc.addr,
                    http=cfg.server.http.addr,
                    redis_addr=rc.host,
                    kafka_brokers=list(cfg.kafka.brokers),
                    shards=len(pools),
                    runtime="python",  # 灰度期用它在 Grafana 里区分两个实现
                )

            await pserver.run(
                service_name=SERVICE_NAME,
                grpc_server=grpc_server,
                grpc_addr=cfg.server.grpc.addr,
                http_app=http_app,
                http_addr=cfg.server.http.addr,
                http_default_port=HTTP_DEFAULT_PORT,
                on_ready=_on_ready,
                background=background,
            )
            return 0
        finally:
            # ★ 关闭顺序与 Go 的 defer 栈一致,且**必须**是这个顺序:
            # 先停 audit worker,再关 Kafka producer —— 反过来会让 worker 拿着
            # 一个已关闭的 producer 继续发,每条 audit 都抛一次异常。
            if uc is not None:
                await uc.close()
            if events is not None:
                await events.close()
            if grpc_ledger is not None:
                with contextlib.suppress(Exception):
                    await grpc_ledger.close()
            # redis-py 5 是 aclose(),更早的版本只有 close()。两个都试是因为
            # 锁定文件与运行环境可能不同版本,关不掉不该盖住停机流程。
            closer = getattr(rdb, "aclose", None) or getattr(rdb, "close", None)
            if closer is not None:
                with contextlib.suppress(Exception):
                    await closer()
            if node_holder is not None:
                # 正常退出:停续约并断开 etcd。**刻意不 revoke** —— 立刻释放会让
                # 新副本在同一日历秒抢到同号并从 step 0 重数,逐位重号。
                await node_holder.close()
    finally:
        for pool in pools:
            pool.close()
            with contextlib.suppress(Exception):
                await pool.wait_closed()


def _build_background(cfg, logger, uc, router, pools) -> list:  # noqa: ANN001
    """组装后台循环。passive_warmup 时只留容量巡检。"""
    background: list = []

    # 循环 1:容量巡检,**逐分片各一条**。
    for index, shard in enumerate(router.shards()):
        # 点位名必须带分片号:裸 lambda 的 __name__ 恒为 <lambda>,server.run 会把它降级成
        # bg_anonymous —— N 个分片 + 下面 4 条循环全塌成同一个 label,panic 计数与
        # task dump 里分不清死的是哪一片。分片数由配置有界,不会把 label 基数撑爆。
        background.append(
            (
                f"db_capacity_guard_shard{index}",
                lambda p=shard.pool, s=shard.schema, i=index: _run_capacity_guard(p, s, i),
            )
        )

    if cfg.auction.passive_warmup:
        # R3 green 与旧 matcher 共存时必须完全只读:尤其不能先把 legacy 单置 verified,
        # 否则旧实例在 Settle 后崩溃会让新 matcher 按陈旧 remaining 再次 Reserve。
        logger.warning(
            "auction_passive_warmup_enabled",
            hint="writes, legacy verifier, side-effect reconciler and expiry sweeper "
            "are disabled until all old auction instances stop",
        )
        return background

    interval = float(cfg.auction.side_effect_reconcile_interval_seconds)

    # 循环 2:持久副作用补偿。事务内只预留成交/终态并登记 PENDING,事务提交后调用 inventory。
    async def _reconcile_side_effects() -> None:
        settled, released, err = await uc.reconcile_pending_side_effects()
        if err is not None:
            logger.warning(
                "auction_side_effect_reconcile_incomplete",
                err=str(err),
                settled=settled,
                released=released,
            )
        elif settled > 0 or released > 0:
            logger.info(
                "auction_side_effect_reconcile", settled=settled, released=released
            )

    background.append(
        (
            "auction_side_effect_reconcile",
            lambda: _run_now_then_loop(
                "auction_side_effect_reconcile", interval, _reconcile_side_effects
            ),
        )
    )
    logger.info(
        "side_effect_reconciler_ready",
        interval_s=cfg.auction.side_effect_reconcile_interval_seconds,
        batch_per_shard=cfg.auction.side_effect_reconcile_batch,
    )

    # 循环 3:成交事件 outbox。**必须与循环 2 分开**——底层同步 producer 不可靠响应取消,
    # broker 故障只能阻塞事件 worker,不能阻塞下一轮 Settle/Release。
    async def _reconcile_match_events() -> None:
        published, err = await uc.reconcile_pending_match_events()
        if err is not None:
            logger.warning(
                "auction_match_event_reconcile_incomplete", err=str(err), published=published
            )
        elif published > 0:
            logger.info("auction_match_event_reconcile", published=published)

    background.append(
        (
            "auction_match_event_reconcile",
            lambda: _run_now_then_loop(
                "auction_match_event_reconcile", interval, _reconcile_match_events
            ),
        )
    )
    logger.info(
        "match_event_reconciler_ready",
        interval_s=cfg.auction.side_effect_reconcile_interval_seconds,
        batch_per_shard=cfg.auction.side_effect_reconcile_batch,
    )

    # 循环 4:保留期清理(§9.24)。多副本各自跑,DELETE 幂等无需锁。
    async def _sweep_retention() -> None:
        cutoff_ms = abiz.now_ms() - cfg.auction.retention_days * 86400 * 1000
        mode = cfg.auction.retention_mode_parsed()
        try:
            out = await aretention.sweep_retention(
                router, mode, cutoff_ms, cfg.auction.retention_sweep_batch
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.warning("auction_retention_sweep_failed", err=str(exc))
            return
        # mode 默认 report_only:各表待清理量由 dbguard.sweep_table 统一 WARN 告警,
        # 这里只在真删发生时补一条业务 INFO。
        if out.cleaned():
            logger.info(
                "auction_retention_swept",
                orders=out.orders_deleted,
                matches=out.matches_deleted,
                idem_keys=out.idem_keys_deleted,
                retention_days=cfg.auction.retention_days,
            )

    background.append(
        (
            "auction_retention_sweep",
            lambda: safego.loop(
                "auction_retention_sweep",
                float(cfg.auction.retention_sweep_interval_seconds),
                _sweep_retention,
            ),
        )
    )
    logger.info(
        "retention_sweeper_ready",
        retention_days=cfg.auction.retention_days,
        interval_s=cfg.auction.retention_sweep_interval_seconds,
        batch_per_shard=cfg.auction.retention_sweep_batch,
    )

    # 循环 5:过期清扫(限制#1 补偿)。仅 order_ttl_seconds > 0 时启动 ——
    # 写成 0 的后果是这条循环**根本不存在**,冻结的资产永远挂着,
    # 唯一判据就是启动日志里没有 expiry_sweeper_ready。
    if cfg.auction.order_ttl_seconds > 0:

        async def _sweep_expiry() -> None:
            try:
                n = await uc.expire_due_orders()
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.warning("auction_expiry_sweep_failed", err=str(exc))
                return
            if n > 0:
                logger.info("auction_expiry_sweep", expired=n)

        background.append(
            (
                "auction_expiry_sweep",
                lambda: safego.loop(
                    "auction_expiry_sweep",
                    float(cfg.auction.expiry_sweep_interval_seconds),
                    _sweep_expiry,
                ),
            )
        )
        logger.info(
            "expiry_sweeper_ready",
            ttl_s=cfg.auction.order_ttl_seconds,
            interval_s=cfg.auction.expiry_sweep_interval_seconds,
        )
    return background


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        return asyncio.run(_main_async(args))
    except KeyboardInterrupt:
        return 0
    except Exception:  # noqa: BLE001
        # 对应 Go 侧 app_run_failed。
        plog.get().exception("app_run_failed")
        return 1


if __name__ == "__main__":
    with contextlib.suppress(BrokenPipeError):
        sys.exit(main())
