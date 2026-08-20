"""Pandora mail 服务入口(Python 版)—— 对应 Go 侧 services/social/mail/cmd/mail/main.go。

装配链与 Go 逐段对齐:logger → MySQL(强依赖)→ Snowflake → inventory 客户端 →
repo/usecase/service → 会话现行性门 → gRPC/HTTP → 后台循环 → 阻塞运行。

mail **不依赖 kafka**(系统/公会邮件拉取式,个人邮件落库即达;红点推送复用
system.notify 由运营侧发),也**不读配置表**。

★ 启动闸(逐条对应 Go 侧的 os.Exit / Must*,事件名逐字相同 —— Loki 上按事件名
  建的告警对不上就是静默失去覆盖):

    ① abs_conf_path_failed / config_load_failed / config_scan_failed  fail-fast
    ② mysql_dsn_required        MySQL 是强依赖:没有权威库就没有邮件
    ③ (MySQL 连接失败)          Go 是 mysqlx.MustNewClient 的 panic;Python 打
                                mysql_connect_failed 后 exit 1(方向相同)
    ④ mysql_strict_mode_required 非严格 sql_mode 下超长写入被**静默截断**
                                (err=nil 而数据被砍断)= 无声的数据损坏
    ⑤ (snowflake nodeID)        static / etcd 二选一;etcd 抢不到号**不得**退回
                                static —— 那正好会与别的副本重号
    ⑥ inventory_addr_required   未配 inventory 又没开空领 → 拒启,防裸奔丢奖
    ⑦ (会话现行性门)            Go 是 sessiongate.MustBuild 的 panic:
                                require=true 时端点漏配 / Ping 失败一律拒启

  方向也与 Go 一致:inventory 未配但开了 allow_noop_grant 时是 **WARN 放行**
  (测试档),不是拒启。

后台循环两条,都走 safego(单轮异常只丢本轮,不静默弄死循环):
    - mail 过期清理(每 sweep_interval 一轮)
    - 容量巡检(启动即一轮拿基线,之后每小时;超预算只告警不阻断)

运行:
    cd python
    .venv/Scripts/python.exe -m pandorapy.services.mail.main \
        -conf ../services/social/mail/etc/mail-dev.yaml
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
from pandora.mail.v1 import mail_pb2_grpc

from pandorapy import dbguard
from pandorapy import godur
from pandorapy import log as plog
from pandorapy import mysqlx
from pandorapy import redisx
from pandorapy import safego
from pandorapy import server as pserver
from pandorapy import sessiongate
from pandorapy import snowflake_etcd as psnowflake_etcd
from pandorapy.services.mail import biz as mbiz
from pandorapy.services.mail import budgets as mbudgets
from pandorapy.services.mail import conf as mconf
from pandorapy.services.mail import data as mdata
from pandorapy.services.mail import inventory_client as minv
from pandorapy.services.mail import service as msvc

SERVICE_NAME = "mail"
HTTP_DEFAULT_PORT = 21009
GRPC_SERVICE_FULL_NAME = "pandora.mail.v1.MailService"

# 邮件相关表所在的库(pandora_social 由 chat/friend/guild/mail 共用)。
MAIL_DB = "pandora_social"

# 容量巡检周期:启动即跑一轮拿基线(上线时就已超限当场可见),之后每小时一轮。
# 走 information_schema 估算(毫秒级、不锁表、不扫数据),放启动路径安全;
# 绝不用 COUNT(*)(千万行表几十秒,会拖垮滚动更新)。
CAPACITY_INTERVAL_SEC = 3600.0

# 会话权威 Redis 的启动期 Ping 超时,与 Go 侧 sessiongate.MustBuild 同为 3s。
SESSION_GATE_PING_TIMEOUT_SEC = 3.0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线)而不是 argparse 惯常的 `--conf`:Go 侧 flag 包用单横线,
    而 run_services.ps1 / start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。
    Python 版必须能被**同一条命令行**拉起,否则那些脚本都要改。
    """
    ap = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    ap.add_argument("-conf", dest="conf", default="etc/mail-dev.yaml")
    return ap.parse_args(argv)


async def _run_capacity_guard(pool, schema: str, interval_sec: float) -> None:
    """容量巡检(§9.24):**只告警不阻断**。

    容量超限是"要去查的问题",不是"服务不能跑的理由" ——
    拒绝启动会把容量问题升级成可用性事故。
    """
    budgets = mbudgets.budgets()

    async def _once() -> None:
        async with pool.acquire() as conn:
            result = await dbguard.check_budgets(conn, schema, budgets)
        dbguard.log_violations(result, db=schema)

    # 启动即一轮拿基线,再进周期循环(与 Go 的 runCapacityGuard 同序)。
    await safego.run_once("db_capacity_guard_initial", _once)
    await safego.loop("db_capacity_guard", interval_sec, _once)


async def _main_async(args: argparse.Namespace) -> int:  # noqa: C901 —— 与 Go 同为线性启动闸
    logger = plog.setup(SERVICE_NAME)
    logger.info("service_starting", conf=args.conf)

    # ── ① 配置 ───────────────────────────────────────────────────────────
    try:
        conf_path = pathlib.Path(args.conf).resolve()
    except OSError as exc:
        logger.error("abs_conf_path_failed", err=str(exc))
        return 1
    try:
        cfg = mconf.Config.load(conf_path)
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        # Go 侧把"读不到文件"和"结构对不上"分成两个事件名,这里保持同样区分。
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── ② MySQL 强依赖(pandora_social:邮件表 + 读 guild_members 判所属公会)──
    raw_dsn = cfg.node.mysql_client.dsn
    if not raw_dsn:
        logger.error(
            "mysql_dsn_required", hint="node.mysql_client.dsn required (pandora_social)"
        )
        return 1

    conn_cfg = mysqlx.parse_go_dsn(raw_dsn, default_db=MAIL_DB)
    try:
        # ★ 池参数走 mysqlx.pool_kwargs 统一翻译 —— max_open/max_idle/
        # conn_max_lifetime/ping_timeout 四个字段 yaml 里都写了、Go 侧都读,
        # 手写 create_pool 只传连接身份的话它们**配了不生效且不报错**。
        # 最要紧的是 conn_max_lifetime:没有 pool_recycle,长空闲连接撞上 MySQL 的
        # wait_timeout 被服务端断掉,客户端不知道,**下一条业务 SQL 才暴露**。
        # ★ autocommit=True 是刻意的:与 Go 的 `database/sql` **默认语义**一致。
        # 真正需要原子性的两处(收件箱上限、归档+删除)由 data 层显式 begin() 包起来。
        #
        # ⚠️ 这里原先写的理由是「建成 False 会让连接归池后带着 REPEATABLE READ 旧快照
        # 被复用,读到陈旧数据且零报错」——**2026-08-19 实测证伪**:asyncmy 的池在
        # 归还/借出时会重置事务(maxsize=1 强制复用同一条连接,不提交就归还,
        # 另一条独立连接写入并提交后,再借出能读到新行)。回归测试钉在
        # tests/test_owner_repo.py 末尾。
        # 留这段是因为:那个理由是错的,但结论(用 True)是对的,别因为理由被推翻就把它改回 False。
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
        # ③ Go 侧这里是 mysqlx.MustNewClient 的 panic(它内部 Ping 过)。
        # Python 打成结构化事件后 exit 1 —— 方向相同:连不上库绝不带着起来,
        # 否则 Pod Ready、流量切过来,第一条业务请求才暴露。
        logger.error("mysql_connect_failed", err=str(exc), dsn=mysqlx.mask_dsn(raw_dsn))
        return 1
    logger.info("mysql_connected", dsn=mysqlx.mask_dsn(raw_dsn))

    granter: minv.GrpcItemGranter | None = None
    try:
        # ── ④ 严格模式断言(§9.24)────────────────────────────────────────
        # 非严格 sql_mode 下超长写入会被 MySQL **静默截断**(err=nil 但数据被砍断),
        # 等于无声的数据损坏 —— 邮件的 payload 被截断 = 玩家的附件凭空少几件,
        # 而且没有任何错误可查。所以 fail-fast 而不是继续产生坏数据。
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

        # ── ⑤ Snowflake(mail_id 与 DS 领取意图的 instance_id 共用同一节点)──
        #
        # node_id_source=""/"static" 用 yaml 的 node.node_id;="etcd" 走 etcd 抢占。
        #
        # ⚠️ etcd 模式只解决"重号",**不解决"多副本"**:ListMail 的系统/公会增量按
        # advance_cursor(max mail_id) 推水位,这依赖"同一 channel 的 mail_id 递增顺序
        # = 提交顺序"。该单调性只在单个发号器内成立 —— 两个副本各自铸号并发落库时,
        # ID 大小与提交顺序脱钩,水位推过大 ID 之后晚提交的小 ID 邮件会被**永久跳过**
        # (玩家收不到)。所以 mail 扩到 >1 副本(含金丝雀)前必须先二选一:
        #   ① 系统/公会写路径保持单写者(leader election + fencing);
        #   ② 游标改用 DB 自增 / 提交水位列,mail_id 只当主键不当游标。
        # 滚动更新的新旧并存窗口同样受此约束。
        #
        # ⚠️ etcd_service_name 留空时回落服务名 —— 而 mail 与 inventory **共铸
        # instance_id**,两者必须显式共用同一个 etcd_service_name(集群产物里是
        # "instance-id")。各用各的服务名 = 两个空间各自从 0 发号 = 逐位重号,
        # 汇合到同一个背包时被 duplicate instance 检查 fail-closed,玩家领不了邮件。
        node_holder = None
        try:
            snowflake_node, node_holder = await psnowflake_etcd.provide_node(
                list(cfg.snowflake.etcd_endpoints),
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

        # ── ⑥ inventory 客户端(领附件入库用)─────────────────────────────
        inst_granter = None
        xfer_claimer = None
        escrow_consumer = None
        if cfg.mail.inventory_addr:
            granter = minv.GrpcItemGranter(cfg.mail.inventory_addr)
            # 同一连接承担四条路径(与 Go 侧同一个 GrpcItemGranter)。
            inst_granter = granter
            xfer_claimer = granter
            escrow_consumer = granter
            logger.info("inventory_client_ready", addr=cfg.mail.inventory_addr)
        elif not cfg.mail.allow_noop_grant:
            # 地址缺省且没开测试空领 → 拒启,防裸奔丢奖:带着"领了但没发"的状态
            # 跑起来,玩家的邮件会被标成已领而背包里什么都没有。
            logger.error(
                "inventory_addr_required",
                hint="mail.inventory_addr required, or set mail.allow_noop_grant for test",
            )
            return 1
        else:
            # ★ 方向与 Go 一致:这是 WARN 放行不是拒启(测试档)。
            #   注意 transfer 形态**不受**空领豁免 —— 见 biz 模块头 ②。
            logger.warning(
                "inventory_noop_grant",
                hint="claim will only mark, no items granted (transfer claim stays rejected)",
            )

        # ── 装配链 ──────────────────────────────────────────────────────
        repo = mdata.MySQLMailRepo(pool)
        uc = mbiz.MailUsecase(repo, granter, inst_granter, xfer_claimer, cfg.mail)
        if escrow_consumer is not None:
            uc.set_transfer_escrow_consumer(escrow_consumer)
        # DS 三段式领取意图展开时铸 instance_id —— 与系统/公会邮件的 mail_id
        # 共用同一个雪花节点(所以 node_id 必须与 inventory 不同,见上)。
        uc.set_instance_id_gen(snowflake_node)
        mail_svc = msvc.MailService(uc, snowflake_node)

        # ── ⑦ 会话现行性门(R5 复审 P0-1,INC-20260722-004)────────────────
        # 校验客户端面请求的 jti == login 会话权威(pandora:sess,node.redis_client
        # 指向的共享 Redis)当前一代 —— 顶号后旧 JWT 在 exp 之前就失去按 player_id
        # 定向操作的能力,否则被顶号的那一方还能继续删/领受害者的邮件。
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
                        cfg.node.redis_client,
                        ping_timeout_sec=SESSION_GATE_PING_TIMEOUT_SEC,
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
        # auth_required=False 对应 Go 的 pmw.AuthOptional():三个 Send* 与两个 DS
        # 领取 RPC 是内网系统接口,调用方不带玩家身份;玩家 RPC 在 service 层
        # 兜底 caller_id == 0(见 service.py 头注释)。
        grpc_server = pserver.build_grpc_server(
            cfg.server.grpc,
            auth_required=False,
            extra_interceptors=[
                sessiongate.SessionCurrentInterceptor(sess_gate, require_gate)
            ],
        )
        mail_pb2_grpc.add_MailServiceServicer_to_server(mail_svc, grpc_server)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(grpc_server, [GRPC_SERVICE_FULL_NAME])

        http_app = pserver.build_http_app(SERVICE_NAME)

        sweep_interval = cfg.mail.sweep_interval_td().total_seconds()

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                # Go 打的是 time.Duration.String(),5 分钟是 "5m0s" 而不是 yaml 里的 "5m"。
                sweep_interval=godur.duration_string(cfg.mail.sweep_interval_td()),
                sweep_batch=cfg.mail.sweep_batch,
                max_inbox_size=cfg.mail.max_inbox_size,
                claim_retention_days=cfg.mail.claim_retention_days,
                inventory_addr=cfg.mail.inventory_addr,
                session_gate_require=require_gate,
                runtime="python",  # 灰度期用它在 Grafana 里区分两个实现
            )

        async def _sweep_once() -> None:
            await uc.sweep_expired(_now_ms())

        await pserver.run(
            service_name=SERVICE_NAME,
            grpc_server=grpc_server,
            grpc_addr=cfg.server.grpc.addr,
            http_app=http_app,
            http_addr=cfg.server.http.addr,
            http_default_port=HTTP_DEFAULT_PORT,
            on_ready=_on_ready,
            background=[
                # 过期清理:多副本各自跑,删除幂等无需锁(对齐 leaderboard 补扫模式)。
                ("mail_expired_sweep", lambda: safego.loop("mail_expired_sweep", sweep_interval, _sweep_once)),
                (
                    "capacity_guard",
                    lambda: _run_capacity_guard(
                        pool, conn_cfg["db"], CAPACITY_INTERVAL_SEC
                    ),
                ),
            ],
        )
        if node_holder is not None:
            # 正常退出:停续约并断开 etcd。**刻意不 revoke** —— 立刻释放会让新副本
            # 在同一日历秒抢到同号并从 step 0 重数,逐位重号。
            await node_holder.close()
        return 0
    finally:
        if granter is not None:
            with contextlib.suppress(Exception):
                await granter.close()
        pool.close()
        with contextlib.suppress(Exception):
            await pool.wait_closed()


def _now_ms() -> int:
    return msvc.now_ms()


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
