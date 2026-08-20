"""Pandora leaderboard 服务入口(Python 版)—— 对应 Go 侧 cmd/leaderboard/main.go。

职责(docs/design/decision-revisit-leaderboard.md):
    通用 / 可扩展排行榜(全服 / 类型 / 工会 / 副本局内临时);
    Redis ZSET 做实时排名(强依赖);
    结算 SettleBoard 取 Top-N 落 MySQL 快照 + 按 RewardTable 幂等发奖
    (调 inventory.GrantItems)+ 发 kafka pandora.leaderboard.settle(弱依赖)。

★ 启动闸逐条对齐 Go(**顺序本身是契约**:事件名是 Loki 告警和运维手册的入口):

    ① abs_conf_path_failed              -conf 解不成绝对路径              fail-fast
    ② config_load_failed                yaml 读不到                        fail-fast
    ③ config_scan_failed                yaml 结构对不上 / 配了未实现的段    fail-fast
    ④ leaderboard_retention_mode_invalid retention_mode 拼错               fail-fast
    ⑤ mysql_required                     结算归档库 DSN 缺失               fail-fast
    ⑥ mysql_connect_failed               连不上归档库                       fail-fast
    ⑦ mysql_strict_mode_required         sql_mode 非严格                    fail-fast
    ⑧ redis_endpoint_required            排行榜 ZSET 端点缺失               fail-fast
    ⑨ redis_ping_failed                  Redis 不通                         fail-fast
    ⑩ snowflake_*                        settlement_id 发号器建不起来       fail-fast
    ⑪ reward_granter_missing             inventory_addr 漏配且未显式允许占位 fail-fast
    ⑫ session_gate                       require=true 时漏配会话权威        fail-fast
    弱依赖两条**刻意只 WARN**,不得改成 fail-fast:
      kafka_brokers_empty / kafka_settle_producer_init_failed(结算事件是通知,
      不是结算本身;broker 抖动不该把结算这条稀有管理操作也一起挡掉)
      reward_granter_noop(显式授权过的占位模式)

后台循环三条(全部走 pandorapy.safego:裸 create_task 的协程死掉后进程照跑、
health 照答 SERVING、**零日志**):
    - 容量巡检(§9.24,启动即一轮拿基线,之后每小时)
    - 发奖补扫(每分钟,消除"inventory 抖动 → 奖励漏发到人工介入")
    - 保留期清理(§9.24,每小时;默认 report_only 只报告不删)

运行:
    cd services/runtime/leaderboard
    PYTHONPATH=../../../python:../../../python/gen \\
        python -m pandorapy.services.leaderboard.main -conf etc/leaderboard-dev.yaml
"""

from __future__ import annotations

# ★ 必须最先 import:Windows 控制台默认 cp1252,日志里的中文会抛 UnicodeEncodeError
# 并把真正的启动错误顶掉(实测踩过多次)。
from pandorapy import _utf8  # noqa: F401  isort:skip
from pandorapy import config as pconfig

import argparse
import asyncio
import contextlib
import pathlib
import sys

import asyncmy

from pandora.leaderboard.v1 import leaderboard_pb2, leaderboard_pb2_grpc

from pandorapy import dbguard
from pandorapy import kafkax
from pandorapy import log as plog
from pandorapy import mysqlx
from pandorapy import redisx
from pandorapy import safego
from pandorapy import server as pserver
from pandorapy import sessiongate
from pandorapy import snowflake_etcd as psnowflake_etcd
from pandorapy.services.leaderboard import biz as lbbiz
from pandorapy.services.leaderboard import board_store as bs
from pandorapy.services.leaderboard import budgets as lbbudgets
from pandorapy.services.leaderboard import conf as lbconf
from pandorapy.services.leaderboard import repo as lbrepo
from pandorapy.services.leaderboard import reward_client as lbreward
from pandorapy.services.leaderboard import service as lbsvc

SERVICE_NAME = "leaderboard"
HTTP_DEFAULT_PORT = 21007
GRPC_SERVICE_FULL_NAME = "pandora.leaderboard.v1.LeaderboardService"
DB_SCHEMA = "pandora_leaderboard"

# 发奖补扫参数(与 Go 的 rewardSweepInterval / Grace / Limit 同值)。
# grace 的作用:把"刚结算还在同步发"的批次挡在扫描外 —— 少了它,同步发奖路径和
# 补扫会对同一条记录同时发起 Grant(幂等键兜住不双发,但会白跑并制造 FAILED 噪声)。
REWARD_SWEEP_INTERVAL_SEC = 60.0
REWARD_SWEEP_GRACE_SEC = 120.0
REWARD_SWEEP_LIMIT = 200

# 容量巡检 / 保留期清理节拍(Go 均为 time.NewTicker(time.Hour))。
HOUR_SEC = 3600.0


def build_topic(domain: str, event: str) -> str:
    """kafka topic 构造 —— 对应 Go 的 `pkg/config.BuildTopic`。

    ⚠️ 本该从 `pandorapy.kafka_topics` 取常量,但 leaderboard 的 settle topic 在
    Go 侧也是**运行期拼**的(cmd/leaderboard/main.go 用 config.BuildTopic),
    `pkg/kafkax/topics.go` 里没有对应常量,所以生成出来的 kafka_topics.py 里也没有。
    这里照抄同一个拼法而不是写死字面量,是为了让"两边拼法一致"这件事有唯一落点。
    正解是 Go 侧补 TopicLeaderboardSettle 常量后重跑生成器 —— 见交付说明。
    """
    return f"pandora.{domain}.{event}"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线):Go 侧 flag 包用的是单横线,而 run_services.ps1 /
    start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。Python 版必须能被**同一条
    命令行**拉起,否则那些脚本都要改。
    """
    ap = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    ap.add_argument("-conf", dest="conf", default="etc/leaderboard-dev.yaml")
    return ap.parse_args(argv)


async def _run_capacity_guard(
    pool, schema: str = DB_SCHEMA, interval_sec: float = HOUR_SEC  # noqa: ANN001
) -> None:
    async def _capacity_round() -> None:
        async with pool.acquire() as conn:
            result = await dbguard.check_budgets(conn, schema, lbbudgets.budgets())
        dbguard.log_violations(result, db=schema)

    await safego.run_once("db_capacity_guard_initial", _capacity_round)
    await safego.loop("db_capacity_guard", interval_sec, _capacity_round)


class SettleEventPusher:
    """把结算事件发到 kafka —— 对应 Go 的 settleEventPusher。

    kafka key = settlement_id:同一结算的事件保序(不变量 §9)。
    """

    __slots__ = ("_producer",)

    def __init__(self, producer: kafkax.KeyOrderedProducer) -> None:
        self._producer = producer

    async def push_settle(self, settlement_id: int, board: bs.BoardKey, winners: list) -> None:
        evt = leaderboard_pb2.LeaderboardSettleEvent(
            settlement_id=settlement_id,
            board=leaderboard_pb2.BoardKey(
                board_type=board.board_type,
                scope=board.scope,
                scope_id=board.scope_id,
                period=board.period,
            ),
            winners=winners,
            settled_at_ms=lbbiz.now_ms(),
        )
        await self._producer.send(str(settlement_id), evt)


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
        cfg = lbconf.Config.load(conf_path)
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        # Go 把"读不到文件"和"结构对不上"分成两个事件名,这里保持同样区分。
        # 「配了但 Python 侧没实现的功能段拒启」也落在这个分支 —— 那道闸挂在
        # pandorapy.config.BaseConf 的 pydantic 校验器上,加载时就抛了。
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── 闸④ 保留期清理模式必须能被识别(§9.24)────────────────────────────
    # 拼错的值会静默回落 report_only:运维以为开了清理、实际一行没删,
    # 库继续无界增长且启动期毫无痕迹。
    try:
        cfg.leaderboard.validate_retention_mode()
    except ValueError as exc:
        logger.error(
            "leaderboard_retention_mode_invalid",
            err=str(exc),
            hint='leaderboard.retention_mode 只接受 "report_only"(默认,不删) 或 "delete"',
        )
        return 1

    # ── 闸⑤ MySQL DSN(强依赖:结算归档库)─────────────────────────────────
    raw_dsn = cfg.node.mysql_client.dsn
    if not raw_dsn:
        logger.error(
            "mysql_required", hint="node.mysql_client.dsn required (pandora_leaderboard)"
        )
        return 1

    # ── 闸⑥ 连库 ─────────────────────────────────────────────────────────
    # Go 这里是 mysqlx.MustNewClient 的 panic(没有结构化事件名),Python 侧补一个
    # 事件名 mysql_connect_failed —— panic 的 stack 在 Loki 上没法按事件名建告警。
    conn_cfg = mysqlx.parse_go_dsn(raw_dsn, default_db=DB_SCHEMA)
    try:
        # ★ 池参数走 mysqlx.pool_kwargs 统一翻译 —— max_open/max_idle/
        # conn_max_lifetime/ping_timeout 四个字段 yaml 里都写了、Go 侧都读,
        # 手写 create_pool 只传连接身份的话它们**配了不生效且不报错**。
        # 最要紧的是 conn_max_lifetime:没有 pool_recycle,长空闲连接撞上 MySQL 的
        # wait_timeout 被服务端断掉,客户端不知道,**下一条业务 SQL 才暴露**。
        # autocommit=False:乐观锁 CAS 的 rowcount 判定必须落在同一事务里。
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
        logger.error("mysql_connect_failed", err=str(exc), dsn=mysqlx.mask_dsn(raw_dsn))
        return 1
    logger.info("mysql_connected", dsn=mysqlx.mask_dsn(raw_dsn))

    rdb = None
    granter = None
    producer = None
    try:
        # ── 闸⑦ 严格模式(§9.24)──────────────────────────────────────────
        # 非严格 sql_mode 下超长写入会被 MySQL **静默截断**(err=nil 但数据被砍断)。
        # 对 leaderboard 具体表现为:reward_pb 被截断 → 补发时解不出奖励 → 那条奖
        # 永久标 FAILED,玩家收不到且查无对证。
        try:
            async with pool.acquire() as conn:
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

        # ── 闸⑧ Redis 端点(强依赖:排行榜 ZSET 不可降级)──────────────────
        rc = cfg.node.redis_client
        if not rc.host and not rc.addrs:
            logger.error(
                "redis_endpoint_required",
                hint="set node.redis_client.host (single) or node.redis_client.addrs (cluster)",
            )
            return 1

        # ── 闸⑨ Ping ─────────────────────────────────────────────────────
        # 不探的话服务会带着一个死 Redis 正常 Ready,k8s 把流量切过来后
        # **第一条业务命令**才暴露 —— 那时错误已经落在玩家请求上了。
        try:
            rdb = await redisx.must_connect(rc)
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
            # 吞掉之后取消就**不再传播** —— 该停的停不下来:
            #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
            #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
            # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error(
                "redis_ping_failed", err=str(exc), addr=rc.host, addrs=list(rc.addrs)
            )
            return 1
        logger.info("redis_connected", addr=rc.host, addrs=list(rc.addrs))

        # ── 闸⑩ Snowflake(settlement_id 生成)────────────────────────────
        # 失租**必须退出进程**,不能降级继续发号:此刻另一个副本可能已经抢到同一个
        # nodeID,继续发就是重号 —— settlement_id 重号会让两次不同结算撞同一个
        # grant_idempotency_key,后一次的奖被幂等吸收,静默漏发。
        node_holder = None
        try:
            snowflake_node, node_holder = await psnowflake_etcd.provide_node(
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

        # ── kafka producer(**弱依赖**:失败只 WARN)──────────────────────
        events = None
        settle_topic = build_topic("leaderboard", "settle")  # pandora.leaderboard.settle
        if cfg.kafka.brokers:
            try:
                producer = kafkax.KeyOrderedProducer(
                    kafkax.producer_conf_from(cfg.kafka),
                    settle_topic,
                )
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001 —— 弱依赖:broker 不通照常启动
                logger.warning("kafka_settle_producer_init_failed", err=str(exc))
                producer = None
            else:
                events = SettleEventPusher(producer)
                logger.info("kafka_producer_ready", topic=settle_topic)
        else:
            logger.warning(
                "kafka_brokers_empty", hint="leaderboard settle events disabled"
            )

        # ── 闸⑪ RewardGranter ────────────────────────────────────────────
        # 配了 inventory_addr → 真实发奖;留空**且** allow_noop_reward=true 才退回
        # 占位,否则 fail-fast。默认拒启是刻意的:生产漏配后以"结算不发奖"静默启动
        # 的话,榜结算了、奖一件没到,而启动日志全绿。
        inv_addr = cfg.leaderboard.inventory_addr
        if inv_addr:
            granter = lbreward.GrpcInventoryRewardGranter(inv_addr)
            logger.info(
                "reward_granter_ready", mode="inventory_grpc", inventory_addr=inv_addr
            )
        elif cfg.leaderboard.allow_noop_reward:
            granter = lbreward.NoopRewardGranter()
            logger.warning(
                "reward_granter_noop",
                hint="leaderboard.inventory_addr empty; settle grants nothing (allow_noop_reward=true)",
            )
        else:
            logger.error(
                "reward_granter_missing",
                hint="leaderboard.inventory_addr 必填(真实发奖);仅联调/单测可显式设 leaderboard.allow_noop_reward=true",
            )
            return 1

        # ── 装配链 ────────────────────────────────────────────────────────
        # 保留期清理要写全限定表名,所以库名传的是 **DSN 里实际连上的那个库**
        # 而不是写死的 pandora_leaderboard(见 repo.MySQLLeaderboardRepo 的说明)。
        repo = lbrepo.MySQLLeaderboardRepo(pool, conn_cfg["db"])
        board = bs.RedisBoardStore(rdb)
        uc = lbbiz.LeaderboardUsecase(
            repo, board, granter, events, snowflake_node, cfg.leaderboard
        )
        svc = lbsvc.LeaderboardService(uc)

        # ── 闸⑫ 会话现行性门(R5 复审 P0-1,INC-20260722-004)──────────────
        # 客户端面请求的 jti 必须是 login 会话权威(pandora:sess,与 node.redis_client
        # 同一 Redis)当前一代;顶号后旧 JWT 立即失去按 player_id 定向的能力。
        # prod 生成器机械置 require=true(漏配端点拒启)。
        try:
            sess_gate = sessiongate.must_build(rdb, cfg.session_gate.require)
        except RuntimeError as exc:
            logger.error("session_gate_required", err=str(exc))
            return 1

        # auth_required=False:读 RPC 允许不带 JWT;写 / 系统 RPC 的守卫方向相反
        # (**带**玩家 JWT 才拒),由 service 层做 —— 见 service.py 头注释。
        grpc_server = pserver.build_grpc_server(
            cfg.server.grpc,
            auth_required=False,
            extra_interceptors=[
                sessiongate.SessionCurrentInterceptor(sess_gate, cfg.session_gate.require)
            ],
        )
        leaderboard_pb2_grpc.add_LeaderboardServiceServicer_to_server(svc, grpc_server)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(grpc_server, [GRPC_SERVICE_FULL_NAME])

        http_app = pserver.build_http_app(SERVICE_NAME)

        # ── 后台循环三条 ──────────────────────────────────────────────────

        async def leaderboard_reward_sweep() -> None:
            """发奖补扫:周期重试 FAILED / PENDING(崩残)的奖励。

            没有这条循环,"inventory 抖动 → 奖励漏发"就要一直挂到人工介入。
            Grant 幂等(grant_idempotency_key),多副本并发补扫安全。
            """
            await safego.loop(
                "leaderboard_reward_sweep",
                REWARD_SWEEP_INTERVAL_SEC,
                lambda: uc.retry_ungranted_rewards(REWARD_SWEEP_GRACE_SEC, REWARD_SWEEP_LIMIT),
            )

        async def leaderboard_retention_sweep() -> None:
            """保留期清理(§9.24,每小时一轮)。

            名次快照 + 已发放发奖记录超保留期批删;settlement 行**故意保留**
            (settle uk 是防重复结算的永久闸,每批次 1 行慢增长豁免)。
            多副本各自跑,DELETE 幂等无需锁;单批有界,积压跨轮摊平。
            """
            await safego.loop("leaderboard_retention_sweep", HOUR_SEC, _retention_round)

        async def _retention_round() -> None:
            mode = cfg.leaderboard.retention_mode_parsed()
            days = cfg.leaderboard.retention_days
            batch = cfg.leaderboard.retention_sweep_batch
            cutoff_ms = lbbiz.now_ms() - days * 86400 * 1000
            # mode 默认 report_only:待清理量由 dbguard.sweep_table 统一 WARN 告警,
            # 这里只在**真删发生**时补一条业务 INFO(与 Go 的 out.Cleaned() 同)。
            try:
                out = await repo.sweep_snapshots_before(mode, cutoff_ms, batch)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.warning("leaderboard_snapshot_sweep_failed", err=str(exc))
            else:
                if out.deleted > 0:
                    logger.info(
                        "leaderboard_snapshot_purged", rows=out.deleted, retention_days=days
                    )
            try:
                out = await repo.sweep_granted_rewards_before(mode, cutoff_ms, batch)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.warning("leaderboard_reward_log_sweep_failed", err=str(exc))
            else:
                if out.deleted > 0:
                    logger.info(
                        "leaderboard_reward_log_purged", rows=out.deleted, retention_days=days
                    )

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                redis_addr=rc.host,
                kafka_brokers=list(cfg.kafka.brokers),
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
                (
                    "db_capacity_guard",
                    lambda: _run_capacity_guard(pool, conn_cfg["db"]),
                ),
                leaderboard_reward_sweep,
                leaderboard_retention_sweep,
            ],
        )
        if node_holder is not None:
            # 正常退出:停续约并断开 etcd。**刻意不 revoke** —— 立刻释放会让新副本
            # 在同一日历秒抢到同号并从 step 0 重数,逐位重号。
            await node_holder.close()
        return 0
    finally:
        # 关闭顺序与建立顺序相反。每一步都 suppress:停机路径上的次生异常会顶掉
        # 真正的退出原因(实测排查成本极高)。
        if producer is not None:
            with contextlib.suppress(Exception):
                await producer.close()
        if granter is not None and hasattr(granter, "close"):
            with contextlib.suppress(Exception):
                await granter.close()
        if rdb is not None:
            # redis-py 5 是 aclose(),更早的版本只有 close()。两个都试是因为
            # 停机路径上关不掉连接不该变成启动失败的替罪羊。
            closer = getattr(rdb, "aclose", None) or getattr(rdb, "close", None)
            if closer is not None:
                with contextlib.suppress(Exception):
                    await closer()
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
