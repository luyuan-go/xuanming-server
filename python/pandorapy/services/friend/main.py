"""Pandora friend 服务入口(Python 版)—— 对应 Go 侧 cmd/friend/main.go。

职责:好友请求 / 接受 / 拒绝 / 列表 / 删好友 / 拉黑 / 推荐;
好友图落 pandora_social(MySQL **强依赖**);好友请求 / 接受经 kafka
pandora.friend.event → push 推给接收方(**弱依赖**);ListFriends / RecommendFriends
经 player_locator 填在线状态(**弱依赖**,addr 空则全部离线)。

★ 启动闸逐条对齐 Go(**顺序本身是契约**:事件名是 Loki 告警和运维手册的入口):

    ① abs_conf_path_failed          -conf 解不成绝对路径                fail-fast
    ② config_load_failed            yaml 读不到                          fail-fast
    ③ config_scan_failed            yaml 结构对不上 / 配了未实现的段      fail-fast
    ④ friend_retention_mode_invalid retention_mode 拼错                  fail-fast
    ⑤ mysql_dsn_required            好友图库 DSN 缺失                    fail-fast
    ⑥ mysql_connect_failed          连不上好友图库                        fail-fast
    ⑦ mysql_strict_mode_required    sql_mode 非严格                       fail-fast
    ⑧ mysql_schema_check_failed     五张表(含后补的守卫表)缺一          fail-fast
    ⑨ snowflake_init_failed /       request_id 发号器建不起来 /           fail-fast
      snowflake_nodeid_acquire_failed  etcd 档抢不到 nodeID
    ⑩ session_gate_required         require=true 时漏配 / 探不通会话权威   fail-fast
    ⑪ grpc/http 装配失败            端口占用、enable_rate_limit 未实现等   fail-fast

    弱依赖四条**刻意只 WARN**,不得改成 fail-fast(好友图已经落库,通知 / 在线态 /
    限流都是锦上添花;把它们改成拒启 = 用可降级的东西换掉可用性):
      kafka_brokers_empty / kafka_producer_init_failed  → 推送静默丢弃,离线补拉仍可用
      locator_addr_empty                                → 在线状态全 false
      friend_rate_quota_disabled                        → 不限流(总量闸仍在)

★ cellroute 装配(Go 的 `etcdtable.WireRouter`)已于 2026-08-20 补齐,落在建完 usecase
  之后:`cell_route.mode` 为空 → 单 Cell、router 为 None、行为不变;`static` / `etcd`
  → 真正建表并注入,失败打 `cellroute_init_failed` 拒启。非法 mode 仍在闸③被拦。

后台循环两条(全部走 pandorapy.safego:裸 create_task 的协程死掉后进程照跑、
health 照答 SERVING、**零日志**):
    - 容量巡检(§9.24,启动即一轮拿基线,之后每小时;只告警不阻断)
    - 终态申请 + pair 守卫行保留期清理(默认 report_only 只报告不删)

运行:
    cd services/social/friend
    PYTHONPATH=../../../python:../../../python/gen \\
        python -m pandorapy.services.friend.main -conf etc/friend-dev.yaml
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

from pandora.friend.v1 import friend_pb2_grpc

from pandorapy import cellroute_etcd
from pandorapy import dbguard
from pandorapy import internalrpcauth
from pandorapy import kafka_topics
from pandorapy import kafkax
from pandorapy import log as plog
from pandorapy import mysqlx
from pandorapy import redisx
from pandorapy import safego
from pandorapy import server as pserver
from pandorapy import sessiongate
from pandorapy import snowflake_etcd as psnowflake_etcd
from pandorapy.services import player_display
from pandorapy.services.friend import biz as fbiz
from pandorapy.services.friend import budgets as fbudgets
from pandorapy.services.friend import conf as fconf
from pandorapy.services.friend import locator_client as flocator
from pandorapy.services.friend import repo as frepo
from pandorapy.services.friend import service as fsvc

SERVICE_NAME = "friend"
HTTP_DEFAULT_PORT = 21004
GRPC_SERVICE_FULL_NAME = "pandora.friend.v1.FriendService"
DB_SCHEMA = "pandora_social"

# 容量巡检节拍(Go: time.NewTicker(time.Hour))。
HOUR_SEC = 3600.0

# 频率配额窗口固定 1 分钟(Go: Window: time.Minute)。配成别的值会让
# rate_quota_per_min 这个名字与实际行为对不上。
RATE_QUOTA_WINDOW_SEC = 60.0

# 好友域五张表 —— 守卫表是后补的(R5 复审 P1-2/3/4),既有库不会自动重放 init SQL。
# 缺表时 acquire_pair_guard / acquire_player_guard 的首条 INSERT 即炸,好友操作
# **全量**内部错误;启动期查一次比让玩家先撞上强。
REQUIRED_TABLES = (
    "friendships",
    "friend_requests",
    "blocks",
    "friend_player_guards",
    "friend_pair_guards",
)
MIGRATION_HINT = (
    "tools/migrate/migrations/pandora_social/000006_friend_guard_tables.up.sql"
    "(或重放 deploy/mysql-init/06-social-tables.sql)"
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线):Go 侧 flag 包用的是单横线,而 run_services.ps1 /
    start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。Python 版必须能被**同一条
    命令行**拉起,否则那些脚本都要改。
    """
    ap = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    ap.add_argument("-conf", dest="conf", default="etc/friend-dev.yaml")
    return ap.parse_args(argv)


async def _run_capacity_guard(
    pool, schema: str = DB_SCHEMA, interval_sec: float = HOUR_SEC  # noqa: ANN001
) -> None:
    async def _capacity_round() -> None:
        async with pool.acquire() as conn:
            result = await dbguard.check_budgets(conn, schema, fbudgets.budgets())
        dbguard.log_violations(result, db=schema)

    await safego.run_once("db_capacity_guard_initial", _capacity_round)
    await safego.loop("db_capacity_guard", interval_sec, _capacity_round)


class KafkaFriendEventPusher:
    """把 FriendEvent 发到 kafka —— 对应 Go 的 friendEventPusher。

    ★ kafka key = **to_player_id**(接收方),不是发起方:同一接收方的事件必须保序
    (不变量 §9.9),push 服务按 key 路由到该玩家的 stream。用发起方当 key 会让
    同一个玩家的"收到申请"和"对方接受了"落到不同分区,顺序不可控。
    """

    __slots__ = ("_producer",)

    def __init__(self, producer: kafkax.KeyOrderedProducer) -> None:
        self._producer = producer

    async def push_friend_event(self, to_player_id: int, evt) -> None:  # noqa: ANN001
        await self._producer.send(str(to_player_id), evt)


async def _main_async(args: argparse.Namespace) -> int:  # noqa: C901 —— 与 Go 同为线性启动闸
    # 1. Logger
    logger = plog.setup(SERVICE_NAME)
    logger.info("service_starting", conf=args.conf)

    # ── 闸① / ②/ ③ 配置 ──────────────────────────────────────────────────
    try:
        conf_path = pathlib.Path(args.conf).resolve()
    except OSError as exc:
        logger.error("abs_conf_path_failed", err=str(exc))
        return 1
    try:
        cfg = fconf.Config.load(conf_path)
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        # Go 把"读不到文件"和"结构对不上"分成两个事件名,这里保持同样区分。
        # 「配了但 Python 侧没实现的功能段拒启」以及非法的 `cell_route.mode`
        # (`static` / `etcd` 之外的值)也落在这个分支。
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── 闸④ 保留期清理模式必须能被识别(§9.24)────────────────────────────
    # 拼错的值会静默回落 report_only:运维以为开了清理、实际一行没删,
    # 库继续无界增长且启动期毫无痕迹。
    try:
        cfg.friend.validate_retention_mode()
    except ValueError as exc:
        logger.error(
            "friend_retention_mode_invalid",
            err=str(exc),
            hint='friend.retention_mode 只接受 "report_only"(默认,不删) 或 "delete"',
        )
        return 1
    try:
        cfg.validate_player_display_resolvers()
    except ValueError as exc:
        logger.error("player_display_resolver_config_invalid", err=str(exc))
        return 1

    # ── 闸⑤ MySQL DSN(强依赖:好友图落库不可降级)─────────────────────────
    raw_dsn = cfg.node.mysql_client.dsn
    if not raw_dsn:
        logger.error("mysql_dsn_required", hint="node.mysql_client.dsn required (pandora_social)")
        return 1

    # ── 闸⑥ 连库 ─────────────────────────────────────────────────────────
    # Go 这里是 mysqlx.MustNewClient 的 panic(没有结构化事件名),Python 侧补一个
    # 事件名 mysql_connect_failed —— panic 的 stack 在 Loki 上没法按事件名建告警。
    conn_cfg = mysqlx.parse_go_dsn(raw_dsn, default_db=DB_SCHEMA)
    try:
        # ★ 池参数走 mysqlx.pool_kwargs 统一翻译 —— max_open/max_idle/
        # conn_max_lifetime/ping_timeout 四个字段 yaml 里都写了、Go 侧都读,
        # 手写 create_pool 只传连接身份的话它们**配了不生效且不报错**。
        #
        # ★ autocommit=**True**:与 Go 的 database/sql 默认一致,也是 repo 的前提 ——
        # repo 的写路径显式 `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` + BEGIN
        # + COMMIT(见 repo.py 文件头 ①),读路径是单条 SELECT。若设成 False,
        # 只读路径会各自开一个事务并且永不提交:连接带着一个长事务回到池里,
        # 既压着 undo(purge 追不上),后续复用的读还可能看见陈旧快照 —— 而且不报错。
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
        logger.error("mysql_connect_failed", err=str(exc), dsn=mysqlx.mask_dsn(raw_dsn))
        return 1
    logger.info("mysql_connected", dsn=mysqlx.mask_dsn(raw_dsn))

    rdb = None
    producer = None
    online = None
    player_name_resolver = None
    player_no_resolver = None
    # ★ 必须在 try **之前**声明:它在 finally 里被读。写在 try 内部的话,
    # 任何在赋值行之前失败的闸都会让 finally 抛 NameError,把真正的退出原因顶掉。
    cell_watcher = None
    try:
        # ── 闸⑦ 严格模式(§9.24)──────────────────────────────────────────
        # 非严格 sql_mode 下超长写入会被 MySQL **静默截断**(err=nil 但数据被砍断),
        # 等于无声的数据损坏 —— 这是唯一允许因数据库检查而拒启的场景。
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

        # ── 闸⑧ 启动期 schema 检查 ────────────────────────────────────────
        try:
            async with pool.acquire() as conn:
                await mysqlx.check_tables(conn, MIGRATION_HINT, *REQUIRED_TABLES)
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

        # ── 闸⑨ Snowflake(request_id 生成)───────────────────────────────
        # 失租**必须退出进程**,不能降级继续发号:此刻另一副本可能已抢到同一 nodeID,
        # 继续发就是重号 —— request_id 重号会让「A 的申请」和「B 的申请」撞同一个 ID,
        # 客户端按 (request_id, reason) 判重,后到的那条推送被当重投丢掉。
        node_holder = None
        try:
            snowflake_node, node_holder = await psnowflake_etcd.provide_node(
                list(cfg.snowflake.etcd_endpoints),
                cfg.snowflake.etcd_service_name or SERVICE_NAME,
                cfg.node.node_id,
                cfg.snowflake.node_id_source,
                on_lost=psnowflake_etcd.exit_process_on_lost,
                **({"prefix": cfg.snowflake.etcd_prefix} if cfg.snowflake.etcd_prefix else {}),
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
            logger.info("snowflake_nodeid_acquired", node_id=node_holder.node_id, source="etcd")

        # ── kafka producer(**弱依赖**:失败只 WARN)──────────────────────
        pusher = None
        if cfg.kafka.brokers:
            try:
                producer = kafkax.KeyOrderedProducer(
                    kafkax.producer_conf_from(cfg.kafka),
                    kafka_topics.TOPIC_FRIEND_EVENT,
                )
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001 —— 弱依赖:broker 不通照常启动
                logger.warning(
                    "kafka_producer_init_failed",
                    err=str(exc),
                    hint="friend push silently dropped until kafka is available",
                )
                producer = None
            else:
                pusher = KafkaFriendEventPusher(producer)
                logger.info("kafka_producer_ready", topic=kafka_topics.TOPIC_FRIEND_EVENT)
        else:
            logger.warning("kafka_brokers_empty", hint="friend push disabled")

        # ── player_locator(**弱依赖**:addr 空则在线状态全离线)──────────
        if cfg.friend.locator_addr:
            online = flocator.GrpcOnlineStatusReader(cfg.friend.locator_addr)
            logger.info("locator_client_ready", locator_addr=cfg.friend.locator_addr)
        else:
            logger.warning(
                "locator_addr_empty", hint="friend online status disabled (all offline)"
            )

        # ── player/login 公开展示投影(**弱依赖**)────────────────────────
        # 凭据配置错误已在触库前 fail-fast；运行期 RPC 失败由 biz 按批 fail-soft。
        if cfg.friend.player_name_resolver_addr:
            signer = internalrpcauth.Signer(
                cfg.friend.player_name_resolver_auth_secret,
                SERVICE_NAME,
                cfg.friend.player_name_resolver_auth_audience,
            )
            player_name_resolver = player_display.GrpcPlayerNameResolver(
                cfg.friend.player_name_resolver_addr, signer
            )
            logger.info(
                "player_name_resolver_ready",
                addr=cfg.friend.player_name_resolver_addr,
                caller=SERVICE_NAME,
                audience=cfg.friend.player_name_resolver_auth_audience,
            )
        else:
            logger.warning("player_name_resolver_disabled")
        if cfg.friend.player_no_resolver_addr:
            signer = internalrpcauth.Signer(
                cfg.friend.player_no_resolver_auth_secret,
                SERVICE_NAME,
                cfg.friend.player_no_resolver_auth_audience,
            )
            player_no_resolver = player_display.GrpcPlayerNoResolver(
                cfg.friend.player_no_resolver_addr, signer
            )
            logger.info(
                "player_no_resolver_ready",
                addr=cfg.friend.player_no_resolver_addr,
                caller=SERVICE_NAME,
                audience=cfg.friend.player_no_resolver_auth_audience,
            )
        else:
            logger.warning("player_no_resolver_disabled")

        # ── 装配链 ────────────────────────────────────────────────────────
        # 保留期清理要写全限定表名,库名传的是 **DSN 里实际连上的那个库**
        # 而不是写死的 pandora_social(TiDB 档 / 测试库连的都不是它)。
        repo = frepo.MySQLFriendRepo(pool, conn_cfg["db"])
        uc = fbiz.FriendUsecase(repo, pusher, online, cfg.friend)
        uc.set_player_name_resolver(player_name_resolver)
        uc.set_player_no_resolver(player_no_resolver)

        # ── cellroute 装配(位置与 Go 的 `etcdtable.WireRouter` 同在 usecase 建完之后)─
        #
        # off(mode 空,当前唯一形态)→ router 为 None,分片观测不执行,行为不变。
        # static → 本地铺表;etcd → 连 etcd 全量 Get + watch 热更,watcher 在 finally 关。
        #
        # ★ 这里 fail-fast 而不是降级成单 Cell:好友图本轮仍是单库单事务,
        #   但“配了分片却按单 Cell 跑”意味着运维以为已经在分片、实际一个分片没分 ——
        #   起不来是刺眼的,静默跑错形态才是致命的。
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

        # ── Redis:频率配额(弱依赖)+ 会话现行性门 ──────────────────────
        # ★ Ping 的时机与 Go 逐条一致:sessiongate.MustBuild **只在 require=true 时**
        # 探活。require=false(dev)时配了个连不上的 Redis 照常启动,限流运行期
        # fail-open —— 那是刻意的:限流是背压门,不该让服务起不来。
        rc = cfg.node.redis_client
        if rc.host or rc.addrs:
            if cfg.session_gate.require:
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
                        "session_gate_required",
                        err=str(exc),
                        addr=rc.host,
                        addrs=list(rc.addrs),
                        hint="session_gate.require=true:会话权威 Redis 探不通拒绝启动(fail-closed)",
                    )
                    return 1
            else:
                rdb = redisx.new_universal_client(rc)

        if rdb is not None:
            # ★ 判据只看"有没有 Redis",不看 per_min 的正负 —— 与 Go 逐字一致。
            # per_min<=0(显式关闭)时 ActionQuota 内部就不限流了(redisx.quota
            # 对 limit<=0 直接放行),不需要在这里再判一次;多一个判据反而会让
            # 同一份 yaml 在两个实现上打出不同的启动事件,Loki 上对不上账。
            uc.set_rate_quota(
                redisx.ActionQuota(
                    rdb, "friend", cfg.friend.rate_quota_per_min, RATE_QUOTA_WINDOW_SEC
                )
            )
            logger.info("friend_rate_quota_ready", per_min=cfg.friend.rate_quota_per_min)
        else:
            # 纯 MySQL 骨架联调(没配 Redis)不限流,与 chat 的弱依赖边界一致。
            logger.warning("friend_rate_quota_disabled", reason="redis not configured")

        svc = fsvc.FriendService(uc, snowflake_node)

        # ── 闸⑩ 会话现行性门(R5 复审 P0-1,INC-20260722-004)──────────────
        # 客户端面请求的 jti 必须是 login 会话权威(pandora:sess,与 node.redis_client
        # 同一 Redis)当前一代;顶号后旧 JWT 立即失去按 player_id 定向的能力。
        # prod 生成器机械置 require=true(漏配端点拒启)。
        try:
            sess_gate = sessiongate.must_build(rdb, cfg.session_gate.require)
        except RuntimeError as exc:
            logger.error("session_gate_required", err=str(exc))
            return 1

        # ── 闸⑪ gRPC / HTTP 装配 ─────────────────────────────────────────
        # auth_required=False:对齐 Go 的 pmw.AuthOptional() —— Envoy 已在路由层
        # require JWT,service 层再做一次 player_id==0 拦截兜底。
        grpc_server = pserver.build_grpc_server(
            cfg.server.grpc,
            auth_required=False,
            extra_interceptors=[
                sessiongate.SessionCurrentInterceptor(sess_gate, cfg.session_gate.require)
            ],
        )
        friend_pb2_grpc.add_FriendServiceServicer_to_server(svc, grpc_server)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(grpc_server, [GRPC_SERVICE_FULL_NAME])

        http_app = pserver.build_http_app(SERVICE_NAME)

        # ── 后台循环两条 ──────────────────────────────────────────────────

        async def friend_request_sweep() -> None:
            """终态申请 + pair 守卫行保留期清理(§9.24)。

            多副本各自跑,DELETE 幂等无需锁(并发只多花空批);单批有界,积压跨轮摊平。
            默认 report_only:只统计待清理量并 WARN,一行都不删。
            """
            await safego.loop(
                "friend_request_sweep",
                cfg.friend.sweep_interval_sec(),
                uc.sweep_terminal_requests,
            )

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                kafka_brokers=list(cfg.kafka.brokers),
                locator_addr=cfg.friend.locator_addr,
                max_friends=cfg.friend.max_friends,
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
                friend_request_sweep,
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
        if cell_watcher is not None:
            with contextlib.suppress(Exception):
                await cell_watcher.close()
        if online is not None:
            with contextlib.suppress(Exception):
                await online.close()
        for resolver in (player_no_resolver, player_name_resolver):
            if resolver is not None:
                with contextlib.suppress(Exception):
                    await resolver.close()
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
