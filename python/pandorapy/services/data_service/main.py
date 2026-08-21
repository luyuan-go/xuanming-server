"""Pandora data_service 服务入口(Python 版)—— 对应 Go 侧 cmd/data_service/main.go。

职责:玩家数据统一读写网关。
  - ReadPlayer:cache-aside(Redis 命中直返,miss 读 MySQL 回填)
  - WritePlayer:MySQL 乐观锁版本写(UPDATE ... WHERE version=?),写后删缓存
  - InvalidateCache:主动删缓存

依赖策略(照抄 Go 的头注释,这是设计决定不是实现细节):
  - MySQL **强依赖**(事实源,不可降级,连不上直接退出)
  - Redis **弱依赖**(旁路缓存,Ping 失败则降级为直连 MySQL,cache=None)
  - 不接 kafka(避免与 player.update 语义重复)

★ 闸的**方向**必须与 Go 逐条相同,不能"顺手加严"。
  这里最容易被改坏的是 Redis:它在 Go 里是 Warn + 降级,不是 fail-fast。
  改成拒启的后果是 —— Redis 抖一下,本该只掉命中率的事故升级成"整个玩家数据网关起不来",
  而玩家数据链上游(player / inventory)全部跟着停。反过来把 MySQL 的 fail-fast
  改成降级则是静默数据损坏。两个方向都错得很贵,所以逐条对着 Go 搬。

启动顺序(对齐 Go 侧;顺序本身是契约:日志事件名和失败点位被运维手册引用):
 1. log.setup → 全局 logger
 2. 解析 -conf 路径,加载 yaml + apply_defaults
 3. MySQL(强依赖)+ 严格模式断言
 4. 容量巡检后台循环(只告警不阻断)
 5. Redis + Ping(弱依赖,失败降级)
 6. 装配 DataUsecase → DataService → gRPC/HTTP server
 7. 阻塞运行

运行:
    cd services/data/data_service
    python -m pandorapy.services.data_service.main -conf etc/data_service-dev.yaml
"""

from __future__ import annotations

# ★ 必须最先 import:Windows 控制台默认 cp1252,日志里的中文会抛 UnicodeEncodeError,
# 把真正的启动错误顶掉(实测踩过多次)。
from pandorapy import _utf8  # noqa: F401  isort:skip
from pandorapy import config as pconfig

import argparse
import asyncio
import contextlib
import pathlib
import sys

import asyncmy

from pandorapy import cellroute_etcd
from pandorapy import dbguard
from pandorapy import godur
from pandorapy import log as plog
from pandorapy import mysqlx
from pandorapy import redisx
from pandorapy import safego
from pandorapy import server as pserver
from pandora.data_service.v1 import data_service_pb2_grpc as dgrpc

from pandorapy.services.data_service import biz as dbiz
from pandorapy.services.data_service import budgets as dbudgets
from pandorapy.services.data_service import conf as dconf
from pandorapy.services.data_service import data as ddata
from pandorapy.services.data_service import service as dsvc

SERVICE_NAME = "data_service"
HTTP_DEFAULT_PORT = 21003

# gRPC service 全名,开 reflection 时要用(grpcurl list 靠它)。
GRPC_SERVICE_FULL_NAME = "pandora.data_service.v1.DataService"

# 事实源库名。DSN 里没写库名时(CI 下发的形态)回落到它,并作为容量巡检的 schema。
DEFAULT_DB = "pandora_player"

# 容量巡检周期,与 Go 的 time.NewTicker(time.Hour) 同值。
CAPACITY_INTERVAL_SEC = 3600.0

# asyncmy 的池必须给个上限(Go 的 database/sql 不给就是无上限)。yaml 没配时回落到
# data_service-dev.yaml 里的现值,而不是让池大小随实现漂移。
DEFAULT_MAX_OPEN_CONNS = 32


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线):Go 侧 flag 包用的是单横线,而 run_services.ps1 /
    start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。Python 版必须能被**同一条命令行**
    拉起,否则那些脚本都要改。argparse 支持单横线长选项,所以这里能对齐。
    """
    parser = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    parser.add_argument(
        "-conf",
        dest="conf",
        default="etc/data_service-dev.yaml",
        help="config file path(与 Go 版同名同默认值)",
    )
    return parser.parse_args(argv)


async def _capacity_round(pool, schema: str = DEFAULT_DB) -> None:  # noqa: ANN001
    """跑一轮容量巡检并打违规日志。对应 Go 的 g.Check(ctx)。

    走 information_schema 估算(毫秒级、不锁表、不扫数据),所以放启动路径安全;
    绝不用 COUNT(*) —— 千万行表几十秒,会拖垮滚动更新。
    """
    async with pool.acquire() as conn:
        result = await dbguard.check_budgets(conn, schema, dbudgets.budgets())
    dbguard.log_violations(result, db=schema)


async def _run_capacity_guard(
    pool, schema: str = DEFAULT_DB, interval_sec: float = CAPACITY_INTERVAL_SEC  # noqa: ANN001
) -> None:
    """容量巡检:启动即跑一轮拿基线,之后每小时一轮(§9.24)。对应 Go 的 runCapacityGuard。

    ★ "启动即一轮"不是可省的优化:没有它,上线时就已超预算的表要等**一小时后**
    才有第一条告警,而那一小时正是刚发版、最需要知道基线的窗口。

    ★ 超预算只告警不阻断:容量超限是"要去查的问题",不是"服务不能跑的理由" ——
    在这里拒启会把容量问题升级成可用性事故。

    单轮异常兜底靠 safego(对应 Go 的 safego.Run):一次意外异常若不兜住,这条循环会
    **静默死掉**而服务看起来完全正常 —— 从此再没有容量告警,且没有任何信号说明为什么。
    两个点位名与 Go 逐字相同(db_capacity_guard_initial / db_capacity_guard),
    panic_recovered 的 name 标签和 Loki 查询都按它建。
    """
    await safego.run_once("db_capacity_guard_initial", lambda: _capacity_round(pool, schema))
    # safego.loop 的首轮在第一个 tick **之后**执行(time.Ticker 语义),
    # 正好接上上面那轮基线,与 Go 的 initial + ticker 组合逐拍对齐。
    await safego.loop("db_capacity_guard", interval_sec, lambda: _capacity_round(pool, schema))


async def _main_async(args: argparse.Namespace) -> int:  # noqa: C901 —— 与 Go 同为线性启动闸
    # 1. Logger
    logger = plog.setup(SERVICE_NAME)
    logger.info("service_starting", conf=args.conf)

    # 2. 加载 yaml
    try:
        conf_path = pathlib.Path(args.conf).resolve()
    except OSError as exc:
        logger.error("abs_conf_path_failed", err=str(exc))
        return 1

    try:
        cfg = dconf.Config.load(conf_path)
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except NotImplementedError as exc:
        # cell_route 装配闸 —— 对应 Go 的 etcdtable.WireRouter 失败分支。
        #
        # Python 侧这道闸物理上挂在 pandorapy.config.BaseConf 的 pydantic 校验器里
        # (放那儿是为了"任何服务只要加载配置就必然过闸",不靠每个 main 记得调),
        # 所以它以 NotImplementedError 的形态从 load() 抛出来。这里**必须**把它
        # 单独接住并还原成 Go 的事件名:Loki 上的告警规则是按 cellroute_init_failed 建的,
        # 混进 config_scan_failed 就等于这条告警对 Python 副本静默失效。
        if "cell_route" in str(exc):
            logger.error("cellroute_init_failed", err=str(exc), path=str(conf_path))
        else:
            logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        # Go 把"读不到文件"和"结构对不上"分成 config_load_failed / config_scan_failed
        # 两个事件名。这里保持同样区分:能读到文件但解析/校验失败 = scan。
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── 闸① MySQL DSN(强依赖:玩家数据事实源,不可降级)────────────────────
    raw_dsn = cfg.node.mysql_client.dsn
    if not raw_dsn:
        logger.error(
            "mysql_dsn_required",
            hint="node.mysql_client.dsn required (pandora_player)",
        )
        return 1

    conn_cfg = mysqlx.parse_go_dsn(raw_dsn, default_db=DEFAULT_DB)
    try:
        # ★ 池参数走 mysqlx.pool_kwargs 统一翻译 —— 四个字段(max_open/max_idle/
        # conn_max_lifetime/ping_timeout)yaml 里都写了、Go 侧都读,手写 create_pool
        # 只传连接身份的话它们**配了不生效且不报错**。最要紧的是 conn_max_lifetime:
        # 没有 pool_recycle,长空闲连接撞上 MySQL 的 wait_timeout 被服务端断掉,
        # 客户端不知道,**下一条业务 SQL 才暴露**。
        # autocommit=False 由 pool_kwargs 统一给:事务型写路径的 rowcount 判定
        # 必须落在同一个事务里才有意义。
        mysqlx.assert_pool_conf_supported(cfg.node.mysql_client)
        pool = await asyncmy.create_pool(
            **mysqlx.pool_kwargs(cfg.node.mysql_client, conn_cfg, autocommit=False)
        )
    except asyncio.CancelledError:
        # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
        # 吞掉之后取消就**不再传播** —— 该停的停不下来:
        #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
        #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
        # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
        raise
    except BaseException as exc:  # noqa: BLE001
        # Go 在这里是 mysqlx.MustNewClient 的 panic(进程直接死)。方向相同(fail-fast),
        # 但 panic 不带结构化事件名 —— Python 侧补一个,好让"连不上库"在 Loki 上可查,
        # 而不是只留一行 traceback。
        logger.error("mysql_connect_failed", err=str(exc), dsn=mysqlx.mask_dsn(raw_dsn))
        return 1
    logger.info("mysql_connected", dsn=mysqlx.mask_dsn(raw_dsn))

    try:
        # ── 闸② 严格模式断言(§9.24)────────────────────────────────────────
        # 非严格 sql_mode 下超长写入会被 MySQL **静默截断**(err=nil 但数据被砍断),
        # 等于无声的数据损坏。player_data 的 string 列是 MEDIUMTEXT、DB 层几乎不设防,
        # 写入侧也没有长度校验 —— 这道闸是唯一挡住"昵称/头像被砍一半还写成功"的东西。
        # 故 fail-fast 而不是继续产生坏数据。
        async with pool.acquire() as conn:
            try:
                await dbguard.assert_strict_mode(conn)
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

        # ── 闸③ Redis(弱依赖:Ping 失败**降级**,不拒启)────────────────────
        # 顺序与 Go 一致:Redis 在 store 之前。看着无所谓,其实是日志契约的一部分 ——
        # 排障时"redis_connected 之后才出现 player_store_init_failed"这个前后关系
        # 会被拿来判断故障发生在哪一段;两个实现把顺序调换,同一条时间线读出来的结论就不同。
        # 单实例填 host,Redis Cluster / Sentinel 只填 addrs,两者皆空才算未配置 ——
        # 判据必须是"两者皆空",不能只看 host:只填 addrs 的 Sentinel 部署会被判成未配置,
        # 缓存悄悄关掉而没有任何人知道(表现只是 MySQL QPS 变高)。
        cache = None
        rc = cfg.node.redis_client
        rdb = None
        # ★ 必须在 try **之前**声明:它在 finally 里被读。
        cell_watcher = None
        if rc.endpoints():
            try:
                # must_connect 内含启动期 Ping 闸:不探的话服务会带着一个死 Redis
                # 正常 Ready,第一条业务命令才暴露。这里探到失败**不拒启**(与 Go 同向),
                # 只降级 —— 但必须留一条 Warn,否则"缓存没生效"这件事零信号。
                rdb = await redisx.must_connect(rc)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.warning(
                    "redis_ping_failed",
                    err=str(exc),
                    addr=rc.host,
                    addrs=list(rc.addrs),
                    hint="degrade to direct MySQL (no cache)",
                )
                rdb = None
            else:
                cache = ddata.RedisPlayerCache(rdb)
                logger.info(
                    "redis_connected",
                    addr=rc.host,
                    addrs=list(rc.addrs),
                    # Go 打的是 time.Duration.String(),5 分钟是 "5m0s" 而不是 yaml 里的 "5m"。
                    cache_ttl=godur.duration_string(cfg.data.cache_ttl_td()),
                )
        else:
            logger.warning("redis_endpoint_empty", hint="cache disabled (direct MySQL)")

        try:
            # ── 闸④ 建表 / schema(对应 Go 的 NewMySQLPlayerStore)──────────
            # Go 侧在 store 构造时经 proto2mysql RegisterAllTables + SyncAllTables 建表/同步;
            # Python 侧由 protosql 从 PlayerData 描述符推导出 CREATE TABLE IF NOT EXISTS。
            # 失败拒启:表建不出来时**每一次** WritePlayer 都会失败,让它在启动期响,
            # 比让它在第一个玩家请求上响便宜得多。
            store = ddata.MySQLPlayerStore(pool)
            try:
                await store.ensure_schema()
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("player_store_init_failed", err=str(exc))
                return 1

            # ── 装配链 ────────────────────────────────────────────────────
            uc = dbiz.DataUsecase(store, cache, cfg.data)

            # cellroute 装配(对应 Go 的 `etcdtable.WireRouter`)。
            # off(mode 空)→ router 为 None = 单 Cell,玩家数据 owner 落点观测不打日志,
            # 与 Go 侧 router 为 nil 完全一致;static / etcd → 真正建表并注入。
            # 非法 mode 在上面 config 加载阶段就已经打 cellroute_init_failed 拒启了。
            try:
                router, cell_watcher = await cellroute_etcd.build_router(cfg.cell_route)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("cellroute_init_failed", err=str(exc))
                return 1
            if router is not None:
                uc.set_cell_router(router)
                logger.info(
                    "cellroute_enabled",
                    self_region=cfg.cell_route.self_region,
                    self_cell=cfg.cell_route.self_cell,
                )

            svc = dsvc.DataService(uc)

            # ★ auth_required=False:对齐 Go 的 NewGRPCServer(没挂 AuthRequired)。
            #   data_service 是内网数据网关,调用方是别的服务而不是玩家,它们不带玩家 JWT。
            #   挂上鉴权的后果是每个 RPC 都 401,而不是"更安全" —— 访问控制由 Envoy /
            #   内网 RPC 黑白名单在路由层做。
            grpc_server = pserver.build_grpc_server(cfg.server.grpc, auth_required=False)
            dgrpc.add_DataServiceServicer_to_server(svc, grpc_server)
            if cfg.server.grpc.enable_reflection:
                pserver.enable_reflection(grpc_server, [GRPC_SERVICE_FULL_NAME])

            http_app = pserver.build_http_app(SERVICE_NAME)

            def _on_ready() -> None:
                logger.info(
                    "service_ready",
                    grpc=cfg.server.grpc.addr,
                    http=cfg.server.http.addr,
                    mysql=mysqlx.mask_dsn(raw_dsn),
                    cache_enabled=cache is not None,
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
                    ("capacity_guard", lambda: _run_capacity_guard(pool, conn_cfg["db"]))
                ],
            )
            return 0
        finally:
            if cell_watcher is not None:
                with contextlib.suppress(Exception):
                    await cell_watcher.close()
            if rdb is not None:
                with contextlib.suppress(Exception):
                    await rdb.aclose()
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
