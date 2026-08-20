"""owner 服务入口 —— 对应 Go 侧 services/runtime/owner/cmd/owner/main.go。

owner 是 §9.22 的**玩家归属唯一权威**:哪台 DS 有权控制和修改某个玩家,由这里的
`owner_epoch` / lease / `admit_not_before` 三件套决定。它错一次的后果是两台 DS
同时认为自己拥有同一个玩家 —— 也就是"一人一 DS"这条不变量被打穿。

★ 因此本服务的启动闸**全部 fail-fast**,一条都不能降级成 WARN 继续跑。
  每一条挡的都是"服务看起来完全正常、但权威面已经不可信"的状态:

    ① DSN 缺失          → owner CAS 不可降级,没有权威库就没有 owner
    ② sql_mode 非严格   → 超长写入被**静默截断**,owner 记录/租约字段损坏且无错可查
    ③ 缺表              → 后建库在既有 volume 上不重放 init SQL,缺表要到第一个请求才炸
    ④ 后端不是 TiDB     → MySQL 异步复制切换会**回滚已确认写**,owner CAS 回滚即双 owner
    ⑤ 缺 hub_source_revision 列 → 本版 SELECT 已引用它,缺列让每个 RPC 报 1054
                                  而启动日志毫无痕迹(INC-20260818-003 的原样)

  ④ 由 owner.require_tidb 控制:dev 单机 MySQL 无复制、天然线性一致,保持 false;
  -Prod 产物由 gen_cluster_config.ps1 机械翻 true,不允许线上继承 dev 宽松档。

后台任务两个,都挂在既有循环上(§16.10:不新建 timer 状态机):
    - 审计流水保留期清理(§9.24,多副本各自跑,DELETE 幂等无需锁)
    - 容量巡检(§9.24,超预算只告警不阻断)
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

from pandorapy import dbguard
from pandorapy import godur
from pandorapy import log as plog
from pandorapy import mysqlx
from pandorapy import server as pserver
from pandora.owner.v1 import owner_pb2_grpc as ogrpc

from pandorapy.services.owner import biz as obiz
from pandorapy.services.owner import conf as oconf
from pandorapy.services.owner import budgets as obudgets
from pandorapy.services.owner import repo as orepo
from pandorapy.services.owner import service as osvc

SERVICE_NAME = "owner"
HTTP_DEFAULT_PORT = 21017

# 与 Go 侧同名同值:缺表提示指向哪份 SQL。少了它,值班的人拿到"缺表"还得翻仓库。
SCHEMA_HINT = "deploy/mysql-init/15-owner-tables.sql(TiDB 见 02-owner-tidb.sql)"
REQUIRED_TABLES = ("owner_record", "ds_instance_lease", "owner_transition_log")

# INC-20260818-003 expand DDL:本版 SELECT 已引用该列。
SOURCE_REVISION_HINT = (
    "INC-20260818-003:本版 owner 的 SELECT 已引用该列,缺列会让 owner 权威面每个 RPC "
    "都报 Error 1054。补 DDL:"
    "ALTER TABLE owner_record ADD COLUMN `hub_source_revision` BIGINT UNSIGNED NOT NULL DEFAULT 0"
)

# 容量巡检周期。启动即跑一轮拿基线,之后每小时一轮。
CAPACITY_INTERVAL_SEC = 3600.0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog=SERVICE_NAME, add_help=True)
    # 与 Go 侧同名:`-conf`(单横线),这样两个实现的启动命令完全一样。
    ap.add_argument("-conf", dest="conf", default="etc/owner-dev.yaml")
    return ap.parse_args(argv)


async def _run_transition_log_sweep(uc, interval_sec: float, batch: int) -> None:
    """周期清理超保留期审计流水(§9.24)。

    ★ 删除行数必须打出来。Go 侧曾只在失败时可见,成功时行数被丢弃 ——
    于是"sweep 到底有没有在删"这个问题只能开库查。这里成功也打。
    """
    logger = plog.get()
    while True:
        await asyncio.sleep(interval_sec)
        try:
            # 单轮有界超时:sweep 卡住不能拖住整个后台循环。
            deleted = await asyncio.wait_for(uc.run_transition_log_sweep(batch), timeout=30.0)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("owner_transition_log_sweep_failed", err=str(exc))
            continue
        if deleted:
            logger.info("owner_transition_log_swept", deleted=deleted, batch=batch)


async def _run_capacity_guard(pool, schema: str, interval_sec: float) -> None:
    """容量巡检(§9.24):超预算只告警不阻断。

    容量超限是"要去查的问题",不是"服务不能跑的理由" ——
    拒绝启动会把容量问题升级成可用性事故。
    """
    logger = plog.get()
    budgets = obudgets.budgets()
    first = True
    while True:
        if not first:
            await asyncio.sleep(interval_sec)
        first = False
        try:
            async with pool.acquire() as conn:
                result = await dbguard.check_budgets(conn, schema, budgets)
            dbguard.log_violations(result, db=schema)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("owner_capacity_guard_failed", err=str(exc))


async def _main_async(args: argparse.Namespace) -> int:  # noqa: C901 —— 与 Go 同为线性启动闸
    logger = plog.setup(SERVICE_NAME)
    logger.info("service_starting", conf=args.conf)

    # ── 配置 ──────────────────────────────────────────────────────────────
    try:
        conf_path = pathlib.Path(args.conf).resolve()
    except OSError as exc:
        logger.error("abs_conf_path_failed", err=str(exc))
        return 1
    try:
        cfg = oconf.Config.load(conf_path)
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── 闸① 权威存储 DSN(强依赖:owner CAS 不可降级)────────────────────
    raw_dsn = cfg.node.mysql_client.dsn
    if not raw_dsn:
        logger.error(
            "mysql_dsn_required",
            hint="node.mysql_client.dsn required (pandora_owner;生产必须 TiDB,§9.22)",
        )
        return 1

    conn_cfg = mysqlx.parse_go_dsn(raw_dsn, default_db="pandora_owner")
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
        logger.error("owner_store_connect_failed", err=str(exc), dsn=mysqlx.mask_dsn(raw_dsn))
        return 1
    logger.info("owner_store_connected", dsn=mysqlx.mask_dsn(raw_dsn))

    try:
        # ── 闸②③⑤ 都在一条连接上做完,任一不过即拒启 ────────────────────
        async with pool.acquire() as conn:
            # ② 严格模式(§9.24):owner 是玩家归属权威,非严格 sql_mode 下超长写入被
            #    静默截断会让 owner 记录 / 租约字段损坏,后果比一般业务表更重。
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

            # ③ schema gate:pandora_owner 是后建库,既有 volume 不会自动重放 init SQL。
            try:
                await mysqlx.check_tables(conn, SCHEMA_HINT, *REQUIRED_TABLES)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("owner_schema_check_failed", err=str(exc))
                return 1

            # ④ 后端强校验(§9.22):require_tidb=true(-Prod 注入)时权威库必须是 TiDB。
            if cfg.owner.require_tidb:
                try:
                    ver = await mysqlx.assert_tidb(conn)
                except asyncio.CancelledError:
                    # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                    # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                    #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                    #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                    # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                    raise
                except BaseException as exc:  # noqa: BLE001
                    logger.error("owner_backend_not_tidb", err=str(exc))
                    return 1
                logger.info("owner_backend_tidb_verified", version=".".join(map(str, ver)))

            # ⑤ expand DDL 校验(INC-20260818-003 分阶段发布第 1 步)。
            try:
                await mysqlx.assert_column_exists(
                    conn, "owner_record", "hub_source_revision", hint=SOURCE_REVISION_HINT
                )
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("owner_source_revision_column_missing", err=str(exc))
                return 1

        # ── 装配链 ────────────────────────────────────────────────────────
        owner_repo = orepo.MySQLOwnerRepo(pool)
        owner_repo.set_reject_legacy_source_revision(cfg.owner.reject_legacy_source_revision)
        if cfg.owner.reject_legacy_source_revision:
            logger.warning(
                "owner_legacy_source_revision_gate_enabled",
                hint="已全局拒绝不带来源版本的 Begin;确认旧 hub_allocator 副本已全部排空",
            )
        uc = obiz.OwnerUsecase(owner_repo, cfg.owner)
        svc = osvc.OwnerService(uc)

        # ★ auth_required=False:owner 的全部 RPC 是**内部**接口,调用方不带玩家身份。
        #   守卫方向相反(带玩家 JWT 才拒),由 service 层做,见 service.py 头注释。
        grpc_server = pserver.build_grpc_server(cfg.server.grpc)
        ogrpc.add_OwnerServiceServicer_to_server(svc, grpc_server)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(grpc_server, ["pandora.owner.v1.OwnerService"])

        http_app = pserver.build_http_app(SERVICE_NAME)

        sweep_interval = cfg.owner.sweep_interval_td().total_seconds()

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                require_tidb=cfg.owner.require_tidb,
                sweep_interval=godur.duration_string(cfg.owner.sweep_interval_td()),
                sweep_batch=cfg.owner.sweep_batch,
                log_retention_days=cfg.owner.log_retention_days,
                reject_legacy_source_revision=cfg.owner.reject_legacy_source_revision,
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
                ("transition_log_sweep", lambda: _run_transition_log_sweep(uc, sweep_interval, cfg.owner.sweep_batch)),
                (
                    "capacity_guard",
                    lambda: _run_capacity_guard(
                        pool, conn_cfg["db"], CAPACITY_INTERVAL_SEC
                    ),
                ),
            ],
        )
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
        plog.get().exception("app_run_failed")
        return 1


if __name__ == "__main__":
    with contextlib.suppress(BrokenPipeError):
        sys.exit(main())
