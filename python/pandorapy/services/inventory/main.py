"""Pandora inventory 服务入口(Python 版)—— 对应 Go 侧
services/economy/inventory/cmd/inventory/main.go。

inventory 是 **7 个服务的下游**(mail 领附件、auction 挂单/成交、trade P2P 结算、
battle_result 掉落发放、player 出战校验、mission 发奖、bag 域容量购买)。
它带病上线的后果不是"某个功能不好用",而是**资产账目错乱**,而且往往要到对账时才发现。
所以启动闸一条都不能省。

★ 启动闸(逐条对应 Go 侧的 os.Exit / Must* / Validate,**事件名逐字相同**——
  Loki 上按事件名建的告警对不上就是静默失去覆盖;**顺序也与 Go 相同**——
  顺序不同会让同一份坏配置在两栈上报不同的第一个错误):

    ①  abs_conf_path_failed              fail-fast  配置路径解析
    ②  config_load_failed                fail-fast  读不到 yaml
    ③  config_scan_failed                fail-fast  结构对不上(含 cell_route 未实现闸)
    ④  inventory_item_rules_invalid      fail-fast  鉴定规则表非法
    ⑤  configtable_dir_required          fail-fast  道具规则唯一权威缺失
    ⑥  configtable_load_failed           fail-fast  manifest/checksum/行数/整批校验
    ⑦  configtable_load_warning          WARN       manifest 未列出的脏文件
    ⑧  inventory_item_table_loaded       INFO       批次版本 + 行数
    ⑨  bag_conf_invalid                  fail-fast  背包域段容量 / 堆叠 / 档位非法
    ⑩  inventory_retention_mode_invalid  fail-fast  清理模式拼错(静默不删)
    ⑪  mysql_dsn_required                fail-fast  权威库缺失(背包落库不可降级)
    ⑫  mysql_connect_failed              fail-fast  Go 侧是 MustNewClient 的 panic
    ⑬  mysql_strict_mode_required        fail-fast  非严格 sql_mode = 静默截断
    ⑭  mysql_schema_check_failed         fail-fast  仅 capacity>0:实例表是后补的
    ⑮  snowflake_init_failed /           fail-fast  仅 capacity>0:etcd 抢不到号
        snowflake_nodeid_acquire_failed              **不得**退回 static(会重号)
    ⑯  instance_bag_enabled              INFO       ★ 实例背包是否真的启用的唯一判据
    ⑰  retention_sweep_enabled           INFO       清理参数
    ⑱  session_gate_endpoint_required    fail-fast  require=true 时端点漏配拒启

  方向也与 Go 一致:配置表加载的 warning 是 WARN 放行(脏文件不拒批次),
  容量巡检超预算只 ERROR 不阻断。

★ **背包域(pandora.bag.v1)在 Python 侧尚未实现**,见文件末 `_warn_bag_domain_skipped`。
  bag.dsn 非空时打一条显式 WARN 并跳过 BagService 注册,不 fail-fast ——
  fail-fast 会让**同一份 dev yaml** 在 Go 上起得来、Python 上起不来,
  而 §14.2 要求默认值保证现有行为不变。这条 WARN 是它唯一的可见性。

后台循环两条,都走 safego(单轮异常只丢本轮,不静默弄死循环):
    - 保留期清理(每 sweep_interval 一轮)
    - 容量巡检(启动即一轮拿基线,之后挂同一间隔;超预算只告警不阻断)

运行:
    cd python
    .venv/Scripts/python.exe -m pandorapy.services.inventory.main \
        -conf ../services/economy/inventory/etc/inventory-dev.yaml
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
from pandora.config.v1 import configtable_pb2_grpc as cfggrpc
from pandora.inventory.v1 import inventory_pb2_grpc as invgrpc

from pandorapy import dbguard
from pandorapy import godur
from pandorapy import log as plog
from pandorapy import mysqlx
from pandorapy import redisx
from pandorapy import safego
from pandorapy import server as pserver
from pandorapy import sessiongate
from pandorapy import snowflake_etcd as psnowflake_etcd
from pandorapy.services.inventory import biz as ibiz
from pandorapy.services.inventory import budgets as ibudgets
from pandorapy.services.inventory import catalog as icat
from pandorapy.services.inventory import conf as iconf
from pandorapy.services.inventory import configtable_admin as ictadmin
from pandorapy.services.inventory import repo as irepo
from pandorapy.services.inventory import service as isvc

SERVICE_NAME = "inventory"
HTTP_DEFAULT_PORT = 21015

# 货币 / 道具 / 流水 / 托管所在的库。
TRADE_DB = "pandora_trade"

# 实例背包表是**后补的**:既有 MySQL volume / PVC 不会自动重放 init SQL,
# 缺表时实例背包全链路必炸。缺表提示直接指向迁移 SQL,省得值班的人翻仓库。
SCHEMA_HINT = "deploy/mysql-init/08-inventory-tables.sql"
INSTANCE_TABLE = "player_item_instance"

# 会话权威 Redis 的启动期 Ping 超时,与 Go 侧 sessiongate.MustBuild 同为 3s。
SESSION_GATE_PING_TIMEOUT_SEC = 3.0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线)而不是 argparse 惯常的 `--conf`:Go 侧 flag 包用单横线,
    而 run_services.ps1 / start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。
    Python 版必须能被**同一条命令行**拉起,否则那些脚本都要改。
    """
    ap = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    ap.add_argument("-conf", dest="conf", default="etc/inventory-dev.yaml")
    return ap.parse_args(argv)


def _new_inventory_repo(pool, conn_cfg: dict) -> irepo.MySQLInventoryRepo:  # noqa: ANN001
    schema = mysqlx.require_mysql_identifier(str(conn_cfg.get("db") or ""), kind="schema")
    return irepo.MySQLInventoryRepo(pool, db=schema)


async def _run_capacity_guard(pool, schema: str, interval_sec: float) -> None:
    """容量巡检(§9.24):**只告警不阻断**。

    容量超限是"要去查的问题",不是"服务不能跑的理由" ——
    拒绝启动会把容量问题升级成可用性事故。

    走 information_schema 估算(毫秒级、不锁表、不扫数据),放启动路径安全;
    绝不用 COUNT(*)(千万行表几十秒,会拖垮滚动更新)。
    """
    budgets = ibudgets.trade_budgets()

    async def _once() -> None:
        async with pool.acquire() as conn:
            result = await dbguard.check_budgets(conn, schema, budgets)
        dbguard.log_violations(result, db=schema)

    # 启动即一轮拿基线(上线时就已超限当场可见),再进周期循环(与 Go 的 runCapacityGuard 同序)。
    await safego.run_once("db_capacity_guard_initial", _once)
    await safego.loop("db_capacity_guard", interval_sec, _once)


def _warn_bag_domain_skipped(logger, cfg: iconf.Config) -> None:  # noqa: ANN001
    """bag.dsn 配了但 Python 侧没有 BagService —— 必须留一条刺眼的 WARN。

    为什么不是 fail-fast:同一份 yaml 要同时喂 Go 和 Python(迁移期两栈并存),
    拒启会让 dev 环境的 Python 版直接起不来。
    为什么不能静默:配了 bag.dsn 的环境里,DS 与 mail 的背包域调用会收到
    UNIMPLEMENTED,而启动日志一片正常 —— 排查起来只能从客户端往回追。
    """
    logger.warning(
        "bag_domain_not_implemented",
        dsn=mysqlx.mask_dsn(cfg.bag.dsn),
        owner_addr=cfg.bag.owner_addr,
        hint=(
            "bag.dsn 已配置,但 Python 版 inventory 尚未实现 pandora.bag.v1 BagService;"
            "本进程不注册该服务,背包域请求会收到 UNIMPLEMENTED。需要背包域请用 Go 版跑 inventory"
        ),
    )


async def _main_async(args: argparse.Namespace) -> int:  # noqa: C901 —— 与 Go 同为线性启动闸
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
        cfg = iconf.Config.load(conf_path)
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        # Go 侧把"读不到文件"和"结构对不上"分成两个事件名,这里保持同样区分。
        # cell_route.mode 非空的拒启也落在这一支(见 pandorapy/config.py 的模型校验器)。
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── ④ 鉴定规则表校验 ─────────────────────────────────────────────────
    # 不设这道闸会怎样:重复 item_config_id 让"哪条规则生效"取决于遍历顺序;
    # min>max 让 roll 区间宽度为负,鉴定出的数值不可预期。
    try:
        cfg.inventory.validate_rules()
    except Exception as exc:  # noqa: BLE001
        logger.error("inventory_item_rules_invalid", err=str(exc))
        return 1

    # ── ⑤ 配置表目录(道具规则的唯一权威)──────────────────────────────
    # 缺了它就没有 usable / sellable / 售价 / 是否装备的裁决依据。
    # 曾经的 item_rules YAML 兜底已整块删除:一份可能与 UE 漂移的数值参与扣减 / 入账,
    # 比拒掉一次请求危险得多。
    if not cfg.config_table.dir:
        logger.error(
            "configtable_dir_required",
            hint="inventory Use/Sell/Grant/Identify rules require config_table.dir",
        )
        return 1

    # ── ⑥⑦⑧ 加载配置表(整批 fail-closed)───────────────────────────────
    #
    # yaml 里的 config_table.dir 是**相对进程工作目录**的,不是相对配置文件。
    # 这是 Go 版的既有契约:run_services.ps1 用 `-WorkingDirectory $svcDir` 启动进程
    # (services/economy/inventory),所以 "../../../configtable/dist" 从服务目录往上
    # 三级才是仓库根。Python 版必须用**同一个**契约,否则同一份 yaml 两个实现解出
    # 不同路径(与 dialogue 同处理)。
    #
    # 刻意不做"找不到就换个基准再试"的兜底:那样会在某些机器上碰巧成功、某些机器上
    # 加载到错误批次,而配置表是策划数值的唯一权威,加载错批次比启动失败严重得多。
    ct_dir = pathlib.Path(cfg.config_table.dir)
    if not ct_dir.is_absolute():
        ct_dir = (pathlib.Path.cwd() / ct_dir).resolve()
    try:
        ct_result = icat.load_tables(ct_dir)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "configtable_load_failed",
            dir=str(ct_dir),
            err=str(exc),
            # 把 cwd 一起打出来:这个失败几乎总是"启动时工作目录不对"造成的,
            # 只报解析后的路径会让人以为是配置写错了。
            cwd=str(pathlib.Path.cwd()),
            hint="config_table.dir 相对进程工作目录;须在服务目录下启动(与 Go 版一致)",
        )
        return 1
    for warning in ct_result.warnings:
        # 脏文件不拒批次(hotreload doc §5:服务端只加载 manifest 列出的表),
        # 但必须可见 —— 它通常意味着发布脚本漏删了上一批的产物。
        logger.warning("configtable_load_warning", warning=warning)
    ct_store = icat.Store(ct_result.tables, str(ct_dir))
    logger.info(
        "inventory_item_table_loaded",
        dir=str(ct_dir),
        version=ct_result.version,
        items=ct_result.tables.item_count(),
        identify_pools=ct_result.tables.pool_count(),
        identify_candidates=ct_result.tables.affix_count(),
    )
    # 把同源 item.max_stack_size 投影给后端驻留背包段(对应 Go 的 itemMaxStacksFromTables)。
    # BagConf 是启动快照:热更后改堆叠上限需滚动重启,但规则值本身不再手抄 YAML。
    cfg.bag.item_max_stacks = [
        iconf.BagItemStackRule(item_config_id=cid, max_stack=stack)
        for cid, stack in ct_store.item_max_stacks()
    ]

    # ── ⑨ 背包域配置校验 ─────────────────────────────────────────────────
    # 即使本进程不提供 BagService,这道闸也要照跑:它校验的是 yaml 本身,
    # 漏掉会让一份非法配置在 Go 版拒启、Python 版放行 —— 两栈对同一份 yaml 结论不同。
    try:
        cfg.bag.validate_rules()
    except Exception as exc:  # noqa: BLE001
        logger.error("bag_conf_invalid", err=str(exc))
        return 1

    # ── ⑩ 保留期清理模式 ─────────────────────────────────────────────────
    # 拼错的值在 Go 侧会 fail-fast 而不是静默回落 report_only:
    # 运维以为开了清理、实际一行没删,库继续无界增长且启动期毫无痕迹。
    try:
        cfg.inventory.validate_retention_mode()
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "inventory_retention_mode_invalid",
            err=str(exc),
            hint='inventory.retention_mode 只接受 "report_only"(默认,不删) 或 "delete"',
        )
        return 1

    # ── ⑪ MySQL 强依赖 ───────────────────────────────────────────────────
    raw_dsn = cfg.node.mysql_client.dsn
    if not raw_dsn:
        logger.error("mysql_dsn_required", hint="node.mysql_client.dsn required (pandora_trade)")
        return 1

    conn_cfg = mysqlx.parse_go_dsn(raw_dsn, default_db=TRADE_DB)
    try:
        # ★ 池参数走 mysqlx.pool_kwargs 统一翻译 —— max_open/max_idle/
        # conn_max_lifetime/ping_timeout 四个字段 yaml 里都写了、Go 侧都读,
        # 手写 create_pool 只传连接身份的话它们**配了不生效且不报错**。
        # 最要紧的是 conn_max_lifetime:没有 pool_recycle,长空闲连接撞上 MySQL 的
        # wait_timeout 被服务端断掉,客户端不知道,**下一条业务 SQL 才暴露**。
        #
        # ★ autocommit=True:与 Go 的 database/sql **默认语义**一致。
        # inventory 的每一条写路径都显式 BeginTx(repo_sql.transaction 包起来),
        # 只读路径是单条 SELECT —— 建成 False 会让每条只读 SELECT 也开一个事务
        # 并要求调用方 commit/rollback,而 rowcount 判定在本服务里不承担 CAS
        # (幂等靠唯一键 1062,不靠影响行数)。
        mysqlx.assert_pool_conf_supported(cfg.node.mysql_client)
        pool = await asyncmy.create_pool(
            **mysqlx.pool_kwargs(cfg.node.mysql_client, conn_cfg, autocommit=True)
        )
    except asyncio.CancelledError:
        # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
        # 吞掉之后取消就**不再传播** —— 该停的停不下来:
        #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
        #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
        # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
        raise
    except BaseException as exc:  # noqa: BLE001
        # ⑫ Go 侧这里是 mysqlx.MustNewClient 的 panic(它内部 Ping 过)。
        # Python 打成结构化事件后 exit 1 —— 方向相同:连不上库绝不带着起来,
        # 否则 Pod Ready、流量切过来,第一条业务请求才暴露。
        logger.error("mysql_connect_failed", err=str(exc), dsn=mysqlx.mask_dsn(raw_dsn))
        return 1
    logger.info("mysql_connected", dsn=mysqlx.mask_dsn(raw_dsn))

    node_holder = None
    try:
        async with pool.acquire() as conn:
            # ── ⑬ 严格模式断言(§9.24)────────────────────────────────────
            # 非严格 sql_mode 下超长写入会被 MySQL **静默截断**(err=nil 但数据被砍断),
            # 等于无声的数据损坏 —— 装备词条 pb 被截断后解不出来,那件装备永久损坏。
            # 这是唯一值得因数据库检查而 fail-fast 的场景。
            try:
                # ★ 5s 超时,对齐 Go main.go:139/158 的 context.WithTimeout。
                # 库半死不活(TCP 通但不回包)时,没有超时的话进程会**挂在这里不退** ——
                # 表现是 Pod 一直卡在启动中,而 k8s 只看到「未 Ready」,
                # 排障的人根本不知道卡在哪道闸上。有超时才会 5 秒后打出事件名再退。
                await asyncio.wait_for(dbguard.assert_strict_mode(conn), timeout=5.0)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("mysql_strict_mode_required", err=str(exc))
                return 1

            # ── ⑭ 实例背包 schema 闸(仅 capacity>0)──────────────────────
            # 未启用时不检也不读该表(biz.get_inventory_full 同步跳过),旧库升级不受影响。
            if cfg.inventory.capacity > 0:
                try:
                    # ★ 5s 超时,对齐 Go main.go:139/158 的 context.WithTimeout。
                    # 库半死不活(TCP 通但不回包)时,没有超时的话进程会**挂在这里不退** ——
                    # 表现是 Pod 一直卡在启动中,而 k8s 只看到「未 Ready」,
                    # 排障的人根本不知道卡在哪道闸上。有超时才会 5 秒后打出事件名再退。
                    await asyncio.wait_for(
                        mysqlx.check_tables(conn, SCHEMA_HINT, INSTANCE_TABLE), timeout=5.0
                    )
                except asyncio.CancelledError:
                    # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                    # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                    #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                    #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                    # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                    raise
                except BaseException as exc:  # noqa: BLE001
                    logger.error("mysql_schema_check_failed", err=str(exc))
                    return 1

        # ── 装配链 ────────────────────────────────────────────────────────
        repo = _new_inventory_repo(pool, conn_cfg)
        uc = ibiz.InventoryUsecase(repo, cfg.inventory)
        uc.set_item_catalog(ct_store)

        # ── ⑮⑯ Snowflake(instance_id)——仅在启用实例背包时装配 ──────────
        if cfg.inventory.capacity > 0:
            try:
                snowflake_node, node_holder = await psnowflake_etcd.provide_node(
                    list(cfg.snowflake.etcd_endpoints),
                    # ⚠️ etcd_service_name 留空时回落服务名 —— 而 inventory 与 mail
                    # **共铸 instance_id**(邮件附件的实例会汇进同一个背包),
                    # 两者必须显式共用同一个 etcd_service_name(集群产物里是
                    # "instance-id")。各用各的服务名 = 两个空间各自从 0 发号 =
                    # 逐位重号,汇合到同一个背包时被 duplicate instance 检查
                    # fail-closed,玩家领不了邮件。
                    cfg.snowflake.etcd_service_name or SERVICE_NAME,
                    cfg.node.node_id,
                    cfg.snowflake.node_id_source,
                    # 失主 = 独占权不可证明 = 继续发号就是重号,没有安全的降级 → 退出进程。
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
            if node_holder is not None:
                logger.info(
                    "snowflake_nodeid_acquired", node_id=node_holder.node_id, source="etcd"
                )
            uc.set_snowflake(snowflake_node)
            # ★ 这条 INFO 是「实例背包到底启没启用」的**唯一判据**:
            #   capacity 写成 0 / 负数 / 拼错键名都不报错、服务照常启动、
            #   行为与没配一模一样(2026-08-06 定谳)。
            logger.info(
                "instance_bag_enabled",
                capacity=cfg.inventory.capacity,
                identify_pools=ct_result.tables.pool_count(),
            )

        svc = isvc.InventoryService(uc)
        ct_admin = ictadmin.ConfigTableAdminService(ct_store)

        # 背包域:Python 侧未实现,配了就打刺眼的 WARN(见函数头注释)。
        if cfg.bag.dsn:
            _warn_bag_domain_skipped(logger, cfg)

        # ── ⑱ 会话现行性门(R5 复审 P0-1,INC-20260722-004)────────────────
        # 校验客户端面请求的 jti == login 会话权威(pandora:sess,node.redis_client
        # 指向的共享 Redis)当前一代 —— 顶号后旧 JWT 在 exp 之前就失去按 player_id
        # 定向操作的能力,否则被顶号的那一方还能继续用 / 卖受害者的背包。
        #
        # ★ Ping 的档位与 Go 逐条对齐(pkg/sessiongate.MustBuild):
        #   require=true  端点漏配 → 拒启;Ping 失败 → 拒启
        #   require=false 端点漏配 → gate=None(dev 直连联调);**不 Ping**
        #   把 require=false 也改成 Ping 会让本机无 Redis 时 Go 版起得来、
        #   Python 版起不来 —— 同一份 yaml 两个实现行为分叉,正是要避免的事。
        require_gate = cfg.session_gate.require
        rdb = None
        if cfg.node.redis_client.endpoints():
            try:
                if require_gate:
                    rdb = await redisx.must_connect(
                        cfg.node.redis_client, ping_timeout_sec=SESSION_GATE_PING_TIMEOUT_SEC
                    )
                else:
                    rdb = redisx.new_universal_client(cfg.node.redis_client)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("session_gate_redis_failed", err=str(exc), require=require_gate)
                return 1
        try:
            sess_gate = sessiongate.must_build(rdb, require_gate)
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
            # 吞掉之后取消就**不再传播** —— 该停的停不下来:
            #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
            #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
            # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("session_gate_endpoint_required", err=str(exc))
            return 1

        # ── gRPC / HTTP ────────────────────────────────────────────────
        # auth_required=False 对应 Go 的 pmw.AuthOptional():GrantItems / 结算 /
        # escrow / transfer 等系统 RPC 是后端内部直连(无 JWT),用 AuthRequired
        # 会把它们全挡在门外;客户端 RPC 在 service 层用 caller 身份强制鉴权。
        grpc_server = pserver.build_grpc_server(
            cfg.server.grpc,
            auth_required=False,
            extra_interceptors=[
                sessiongate.SessionCurrentInterceptor(sess_gate, require_gate)
            ],
        )
        invgrpc.add_InventoryServiceServicer_to_server(svc, grpc_server)
        cfggrpc.add_ConfigTableAdminServiceServicer_to_server(ct_admin, grpc_server)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(
                grpc_server, [isvc.GRPC_SERVICE_FULL_NAME, ictadmin.GRPC_SERVICE_FULL_NAME]
            )

        http_app = pserver.build_http_app(SERVICE_NAME)

        sweep_interval = cfg.inventory.sweep_interval_td().total_seconds()

        # ── ⑰ 保留期清理参数(启动即打,便于对账"到底配的是多少")────────
        logger.info(
            "retention_sweep_enabled",
            interval=godur.duration_string(cfg.inventory.sweep_interval_td()),
            batch=cfg.inventory.sweep_batch,
            ledger_retention_days=cfg.inventory.ledger_retention_days,
            escrow_retention_days=cfg.inventory.escrow_retention_days,
            retention_mode=cfg.inventory.retention_mode_parsed().value,
        )

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                item_rules_source="configtable/item",
                capacity=cfg.inventory.capacity,
                session_gate_require=require_gate,
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
            background=[
                # 保留期清理:多副本各自跑,DELETE 幂等无需锁(对齐 mail sweep)。
                ("inventory_retention_sweep", lambda: safego.loop(
                    "inventory_retention_sweep", sweep_interval, uc.sweep_retention
                )),
                # 容量巡检挂**同一间隔**(§16.10:不新建 timer 状态机)。
                (
                    "capacity_guard",
                    lambda: _run_capacity_guard(pool, conn_cfg["db"], sweep_interval),
                ),
            ],
        )
        if node_holder is not None:
            # 正常退出:停续约并断开 etcd。**刻意不 revoke** —— 立刻释放会让新副本
            # 在同一日历秒抢到同号并从 step 0 重数,逐位重号。
            await node_holder.close()
        return 0
    finally:
        pool.close()
        with contextlib.suppress(Exception):
            await pool.wait_closed()


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
