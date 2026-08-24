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

★ **背包域(pandora.bag.v1)已移植**:`bag.dsn` 非空时走 `_setup_bag_domain`
  装配并注册 BagService(`bag_domain_enabled` INFO);留空 = 不启用该域。
  inventory 是**唯一的两库服务**(trade + bag),容量巡检预算必须按库分传
  (共用一份会让 bag 的三个 blob 列 —— 深度失控的高风险点 —— 完全没有巡检)。

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
from pandora.bag.v1 import bag_pb2_grpc as baggrpc
from pandora.config.v1 import configtable_pb2_grpc as cfggrpc
from pandora.inventory.v1 import inventory_pb2_grpc as invgrpc

from pandorapy import dbguard
from pandorapy import dsauth
from pandorapy import godur
from pandorapy import log as plog
from pandorapy import mysqlx
from pandorapy import redisx
from pandorapy import safego
from pandorapy import server as pserver
from pandorapy import sessiongate
from pandorapy import snowflake_etcd as psnowflake_etcd
from pandorapy.services.inventory import bag_biz as bbiz
from pandorapy.services.inventory import bag_migrate as bmigrate
from pandorapy.services.inventory import bag_owner as bowner
from pandorapy.services.inventory import bag_repo as brepo
from pandorapy.services.inventory import bag_service as bsvc
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

# 背包域(pandora.bag.v1)独立库:与 trade 分库,连接参数走 cfg.bag 自己那套。
BAG_DB = "pandora_bag"

# 实例背包表是**后补的**:既有 MySQL volume / PVC 不会自动重放 init SQL,
# 缺表时实例背包全链路必炸。缺表提示直接指向迁移 SQL,省得值班的人翻仓库。
SCHEMA_HINT = "deploy/mysql-init/08-inventory-tables.sql"
INSTANCE_TABLE = "player_item_instance"

# 背包域同样是后建库:缺表时背包域全链路必炸,fail-fast 并指向迁移 SQL。
BAG_SCHEMA_HINT = "deploy/mysql-init/14-bag-tables.sql"
BAG_TABLES = (
    "bag_meta",
    "bag_checkpoint",
    "bag_section",
    "bag_journal",
    "bag_generation",
    "bag_migration",
    "bag_capacity",
)

# 会话权威 Redis 的启动期 Ping 超时,与 Go 侧 sessiongate.MustBuild 同为 3s。
SESSION_GATE_PING_TIMEOUT_SEC = 3.0

# 背包流水清理单轮的超时,与 Go 的 runBagJournalSweep 同为 30s。
BAG_SWEEP_TIMEOUT_SEC = 30.0


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


async def _run_capacity_guard(
    pool, schema: str, interval_sec: float, budgets  # noqa: ANN001
) -> None:
    """容量巡检(§9.24):**只告警不阻断**。

    容量超限是"要去查的问题",不是"服务不能跑的理由" ——
    拒绝启动会把容量问题升级成可用性事故。

    走 information_schema 估算(毫秒级、不锁表、不扫数据),放启动路径安全;
    绝不用 COUNT(*)(千万行表几十秒,会拖垮滚动更新)。

    ★ budgets 是参数而不是写死 trade:trade 与 bag 是**两个库两套预算**,
      共用一份会让 bag 的三个 blob 列(深度失控的高风险点)完全没有巡检。
    """

    async def _once() -> None:
        async with pool.acquire() as conn:
            result = await dbguard.check_budgets(conn, schema, budgets)
        dbguard.log_violations(result, db=schema)

    # 启动即一轮拿基线(上线时就已超限当场可见),再进周期循环(与 Go 的 runCapacityGuard 同序)。
    await safego.run_once("db_capacity_guard_initial", _once)
    await safego.loop("db_capacity_guard", interval_sec, _once)


async def _run_bag_journal_sweep(uc: bbiz.BagUsecase, interval_sec: float, batch: int) -> None:
    """周期清理超保留期背包流水(§9.24;多副本各自跑,DELETE 幂等无需锁)。

    对应 Go 的 runBagJournalSweep:单轮包 30s 超时 —— 没有超时的话,一轮卡在
    半死不活的库上会让这条循环**永远不再有下一轮**,而日志一片安静。
    """

    async def _once() -> None:
        try:
            await asyncio.wait_for(uc.run_journal_sweep(batch), timeout=BAG_SWEEP_TIMEOUT_SEC)
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,吞掉之后停机时
            # 这条循环退不出去,§9.16 的「先摘流量 → 再排空在途」失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().error("bag_journal_sweep_failed", err=str(exc))

    await safego.loop("bag_journal_sweep", interval_sec, _once)


async def _setup_bag_domain(  # noqa: C901 —— 与 Go 同为一串线性启动闸
    logger,  # noqa: ANN001
    cfg: iconf.Config,
    inv_repo: irepo.MySQLInventoryRepo,
    closables: list,
) -> tuple[bsvc.BagService, bbiz.BagUsecase, object, str, brepo.MySQLBagRepo] | None:
    """装配背包域(pandora.bag.v1,bag-domain.md phase 1 由本进程承载)。

    对应 Go 侧 main.go 的 `if cfg.Bag.DSN != ""` 整段,闸的**先后次序与 Go 相同**
    (§16.8:启动闸顺序本身就是契约,重排会让同一份坏配置在两栈上报不同的第一个错误):

        bag_mysql_connect_failed        fail-fast  Go 侧是 MustNewClient 的 panic
        bag_mysql_strict_mode_required  fail-fast  三个 blob 列最怕静默截断
        bag_schema_check_failed         fail-fast  pandora_bag 是后建库
        bag_owner_authorizer_ready      INFO       五要件② 已装配
        bag_owner_addr_required         fail-fast  生产禁止无授权写
        bag_owner_unverified            WARN       仅 dev 显式放行
        ds_auth_guard_init_failed       fail-fast  mode=permissive/enforce 但缺 secret
        bag_ds_guard_ready              INFO       五要件① 已装配

    返回 (bag_service, bag_usecase, bag_pool, bag_schema, bag_repo);失败返回 None(调用方 exit 1)。

    ★ bag_repo 也要交出去:D5 存量迁移直说仓库段落位(不走 journal),它不是
      BagUsecase 的能力;为此给 BagUsecase 开一个 public repo 反而把内部仓储漏给全部调用方。
    """
    conn_cfg = mysqlx.parse_go_dsn(cfg.bag.dsn, default_db=BAG_DB)
    bag_client_conf = cfg.bag.mysql_client_conf()
    try:
        # 池参数与主库同样走 mysqlx 统一翻译:只传 DSN 会**静默丢掉**中心 MySQL 的
        # TLS 身份与小池参数(配了不生效且不报错)。
        mysqlx.assert_pool_conf_supported(bag_client_conf)
        bag_pool = await asyncmy.create_pool(
            **mysqlx.pool_kwargs(bag_client_conf, conn_cfg, autocommit=True)
        )
    except asyncio.CancelledError:
        # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
        # 启动路径上吞掉会把 Ctrl-C / 上层取消翻译成某道闸的失败,报出假的失败原因。
        raise
    except BaseException as exc:  # noqa: BLE001
        logger.error(
            "bag_mysql_connect_failed", err=str(exc), dsn=mysqlx.mask_dsn(cfg.bag.dsn)
        )
        return None
    closables.append(_PoolCloser(bag_pool))

    async with bag_pool.acquire() as conn:
        # 背包库同样断言严格模式:三个 blob 列(snapshot/section/payload)是最怕静默截断的
        # ——截断后 proto 解不出来,该玩家背包直接读不出来。
        try:
            await asyncio.wait_for(dbguard.assert_strict_mode(conn), timeout=5.0)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("bag_mysql_strict_mode_required", err=str(exc))
            return None
        # 启动期 schema gate:pandora_bag 是后建库,既有 MySQL volume 不会自动重放 init SQL;
        # 缺表时背包域全链路必炸,fail-fast 并指向迁移 SQL。
        try:
            await asyncio.wait_for(
                mysqlx.check_tables(conn, BAG_SCHEMA_HINT, *BAG_TABLES), timeout=5.0
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("bag_schema_check_failed", err=str(exc))
            return None

    bag_schema = mysqlx.require_mysql_identifier(
        str(conn_cfg.get("db") or ""), kind="schema"
    )
    bag_repo = brepo.MySQLBagRepo(bag_pool, db=bag_schema)
    bag_uc = bbiz.BagUsecase(bag_repo, cfg.bag)
    # 容量购买扣费(§5.3):经济域同进程直用 inventory repo(trade 库 ledger 幂等)。
    bag_uc.set_capacity_charger(inv_repo)

    # 五要件② owner 授权(phase 2 写权威切换):背包写路径逐调校验当前 ADMITTED owner。
    # owner_addr 缺省且未显式开 allow_unverified_owner → 拒启(生产禁止无授权写)。
    if cfg.bag.owner_addr:
        owner_auth = bowner.GrpcOwnerAuthorizer(cfg.bag.owner_addr)
        closables.append(owner_auth)
        bag_uc.set_owner_authorizer(owner_auth)
        logger.info("bag_owner_authorizer_ready", owner_addr=cfg.bag.owner_addr)
    elif not cfg.bag.allow_unverified_owner:
        logger.error(
            "bag_owner_addr_required",
            hint="bag.owner_addr required (CLAUDE.md §9.6 要件②), "
            "or set bag.allow_unverified_owner for dev only",
        )
        return None
    else:
        logger.warning(
            "bag_owner_unverified",
            hint="bag writes accepted WITHOUT owner authorization (dev only, never in production)",
        )

    bag_svc = bsvc.BagService(bag_uc)
    # 五要件① DS 凭据身份:ds_auth.mode=enforce 时验签抽取 pod/uid 供 owner target 全等校验。
    try:
        ds_guard = dsauth.guard_from_conf(cfg.ds_auth)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001
        logger.error("ds_auth_guard_init_failed", err=str(exc))
        return None
    if ds_guard is not None:
        bag_svc.set_ds_guard(ds_guard)
        logger.info("bag_ds_guard_ready", mode=ds_guard.mode.value)

    return bag_svc, bag_uc, bag_pool, bag_schema, bag_repo


class _PoolCloser:
    """把 asyncmy 池包成 `close()` 协程,好与 GrpcOwnerAuthorizer 共用同一条收尾链。

    asyncmy 的 `close()` 是同步的、`wait_closed()` 才是协程 —— 直接丢进 closables
    会在停机时静默不等连接排空。
    """

    __slots__ = ("_pool",)

    def __init__(self, pool) -> None:  # noqa: ANN001
        self._pool = pool

    async def close(self) -> None:
        self._pool.close()
        with contextlib.suppress(Exception):
            await self._pool.wait_closed()


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
    # 背包域自带的可关闭资源(bag 库连接池、owner 授权连接)统一挂这里,
    # 与主库 pool 共用同一条 finally 收尾链 —— 少了它,SIGTERM 后 bag 库连接
    # 不排空就退进程,在途事务被 MySQL 侧当连接中断回滚(§9.16 “先摘流量 → 排空在途”)。
    closables: list = []
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
        # 商店表与道具表同一个 Store(同一原子批次):热更后下一次读商店 / 购买立即生效,
        # 而且两张表不会各自看到不同批次(购买要同时查商店定价与道具是否装备类)。
        uc.set_shop_catalog(ct_store)

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

        # ── 背包域(pandora.bag.v1,bag-domain.md phase 1 由本进程承载)──────
        # bag.dsn 为空 = 未启用(不注册 BagService,现网行为不变,安全默认)。
        bag_svc = None
        bag_uc = None
        bag_pool = None
        bag_schema = ""
        bag_mig_uc = None
        if cfg.bag.dsn:
            bag_parts = await _setup_bag_domain(logger, cfg, repo, closables)
            if bag_parts is None:
                return 1
            bag_svc, bag_uc, bag_pool, bag_schema, bag_repo = bag_parts
            # 存量迁移(D5,decision-revisit-bag-replay-semantics.md):默认关;contract 阶段
            # 旧写路径冻结后开启,一次性幂等作业(重跑 no-op,多副本并发安全)。
            if cfg.bag.legacy_migration_enabled:
                bag_mig_uc = bmigrate.BagMigrationUsecase(repo, bag_repo, cfg.bag)
                logger.warning(
                    "bag_legacy_migration_enabled",
                    hint="只准在旧写路径(GrantItems/UseItem/SellItem/escrow)冻结后运行(D5 时序纪律)",
                )
            logger.info(
                "bag_domain_enabled",
                dsn=mysqlx.mask_dsn(cfg.bag.dsn),
                max_journal_batch=cfg.bag.max_journal_batch,
                hourly_journal_quota=cfg.bag.hourly_journal_quota,
                section_capacities=len(cfg.bag.section_capacities),
                journal_retention_days=cfg.bag.journal_retention_days,
            )

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
        # 背包域条件注册(对齐 Go 侧 `cfg.Bag.DSN != ""` 才注册):
        # bag.dsn 为空 = 未启用,不注册 BagService,现网行为不变。
        reflection_names = [isvc.GRPC_SERVICE_FULL_NAME, ictadmin.GRPC_SERVICE_FULL_NAME]
        if bag_svc is not None:
            baggrpc.add_BagServiceServicer_to_server(bag_svc, grpc_server)
            reflection_names.append(bsvc.GRPC_SERVICE_FULL_NAME)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(grpc_server, reflection_names)

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

        background: list = [
            # 保留期清理:多副本各自跑,DELETE 幂等无需锁(对齐 mail sweep)。
            ("inventory_retention_sweep", lambda: safego.loop(
                "inventory_retention_sweep", sweep_interval, uc.sweep_retention
            )),
            # 容量巡检挂**同一间隔**(§16.10:不新建 timer 状态机)。
            (
                "capacity_guard",
                lambda: _run_capacity_guard(
                    pool, conn_cfg["db"], sweep_interval, ibudgets.trade_budgets()
                ),
            ),
        ]
        if bag_pool is not None:
            # trade 与 bag 是**两个库两套预算**:共用一份会把 bag 表当成“未登记表”
            # 而 trade 表在 bag 库里永远查不到,两边都报假警。挂同一间隔,不新建 timer。
            background.append(
                (
                    "bag_capacity_guard",
                    lambda: _run_capacity_guard(
                        bag_pool, bag_schema, sweep_interval, ibudgets.bag_budgets()
                    ),
                )
            )
            # ★ bag_journal 保留期清理(§9.24 只增表):对应 Go main.go 的
            #   `go runBagJournalSweep(...)`。2026-08-21 补接 —— 此前 `_run_bag_journal_sweep`
            #   在本文件**定义了但从未挂进 background**,等于背包流水表永不清理,
            #   而 ruff 的 F401 / F841 都盯不住"模块级函数没人调"这种漏接线。
            background.append(
                (
                    "bag_journal_sweep",
                    lambda: _run_bag_journal_sweep(
                        bag_uc, sweep_interval, cfg.inventory.sweep_batch
                    ),
                )
            )
        if bag_mig_uc is not None:
            # 一次性作业(不是循环):跑完即返回,失败玩家逐个告警不阻断整轮。
            # safego.spawn 已经是 panic 兜底,这里不再套 run_once(双层包装只会多一层噪声)。
            background.append(
                ("bag_legacy_migration", lambda: bmigrate.run_legacy_bag_migration(bag_mig_uc))
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
        if node_holder is not None:
            # 正常退出:停续约并断开 etcd。**刻意不 revoke** —— 立刻释放会让新副本
            # 在同一日历秒抢到同号并从 step 0 重数,逐位重号。
            await node_holder.close()
        return 0
    finally:
        # 后进先出:bag owner 连接先于 bag 池关闭,顺序与获取时相反。
        for closable in reversed(closables):
            with contextlib.suppress(Exception):
                await closable.close()
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
