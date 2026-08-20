"""Pandora mission 服务入口(Python 版)—— 对应 Go 侧
services/social/mission/cmd/mission/main.go。

职责(docs/design/mission.md):通用任务域 —— 接取 / 放弃 / 条件事实驱动进度 /
完成扇出(发奖或标记可领、自动接后续链)/ 领奖。

★ 启动闸逐条对应 Go 侧的 os.Exit / Must*,**事件名逐字相同、方向相同、顺序相同**
  ——Loki 上按事件名建的告警对不上就是静默失去覆盖:

   ①  abs_conf_path_failed          -conf 路径解析失败
   ②  config_load_failed            yaml 读不到 / 语法错(Go 的 c.Load() 覆盖两步)
   ③  config_scan_failed            结构对不上 / 字段校验不过
   ④  mission_retention_mode_invalid  清理模式拼错(静默回落 report_only 不可接受)
   ⑤  config_validation_failed      config_table.dir / 发奖下游地址缺失
   ⑥  configtable_load_failed       配置表强依赖 fail-closed(§9.15)
   ⑦  mysql_dsn_required            MySQL 是任务状态唯一权威
   ⑧  mysql_connect_failed          Go 是 mysqlx.MustNewClient 的 panic,方向相同
   ⑨  mysql_strict_mode_required    非严格 sql_mode 下超长写入被**静默截断**
   ⑩  mysql_schema_check_failed     缺表 fail-fast 并指向迁移 SQL
   ⑪  session_gate_*                Go 是 sessiongate.MustBuild 的 panic
   ⑫  mission_push_writer_lease_mode_invalid                选举档位拼错
   ⑬  mission_push_writer_lease_rollingupdate_without_enforce  滚动重叠期双发布器
   ⑭  mission_push_writer_lease_strategy_annotation_missing    受管 k8s 内无法机械校验
   ⑮  mission_push_writer_lease_endpoints_missing            enforce 却没配 etcd
   ⑯  mission_push_writer_lease_start_failed                 选举起不来

  **WARN 放行**(方向必须与 Go 一致,写成拒启会让 dev 起不来):
   kafka_producer_init_failed / kafka_brokers_empty        推送禁用,出箱堆积可见
   inventory_granter_noop / exp_granter_noop               allow_noop_reward=true 档
   overflow_mail_disabled                                  满包发放失败留补扫
   mission_push_writer_lease_strategy_unknown / ..._disabled  非 k8s / mode=off

后台循环四条,都走 safego(单轮异常只丢本轮,不静默弄死循环):
    - mission_reward_retry      发奖补扫(多副本并跑安全,靠下游幂等键)
    - mission_push_publish      推送出箱发布(**单写者**,见 push_writer_lease)
    - mission_retention_sweep   保留期清理(DELETE 幂等,多副本并跑安全)
    - db_capacity_guard         容量巡检(启动即一轮拿基线,之后每小时;只告警不阻断)

运行:
    cd python
    .venv/Scripts/python.exe -m pandorapy.services.mission.main \
        -conf ../services/social/mission/etc/mission-dev.yaml
"""

from __future__ import annotations

# ★ 必须最先 import:Windows 控制台默认 cp1252,日志里的中文会抛 UnicodeEncodeError
# 并把真正的启动错误顶掉。见 pandorapy/_utf8.py。
from pandorapy import _utf8  # noqa: F401  isort:skip
from pandorapy import config as pconfig

import argparse
import asyncio
import contextlib
import os
import pathlib
import sys

import asyncmy
from pandora.mission.v1 import mission_pb2_grpc

from pandorapy import dbguard
from pandorapy import godur
from pandorapy import kafka_topics
from pandorapy import kafkax
from pandorapy import log as plog
from pandorapy import mysqlx
from pandorapy import redisx
from pandorapy import safego
from pandorapy import server as pserver
from pandorapy import sessiongate
from pandorapy import writerlease
from pandorapy.configtable import ConfigTableError
from pandorapy.services.mission import biz as mbiz
from pandorapy.services.mission import budgets as mbudgets
from pandorapy.services.mission import catalog as mcat
from pandorapy.services.mission import conf as mconf
from pandorapy.services.mission import granter as mgranter
from pandorapy.services.mission import repo as mrepo
from pandorapy.services.mission import service as msvc

SERVICE_NAME = "mission"
HTTP_DEFAULT_PORT = 21019
GRPC_SERVICE_FULL_NAME = "pandora.mission.v1.MissionService"

# 任务域独库(dbguard / check_tables 都要显式库名,它把表名拼进 SQL)。
MISSION_DB = "pandora_mission"

REQUIRED_TABLES = (
    "player_mission_active",
    "player_mission_done",
    "mission_reward_log",
    "mission_fact_receipts",
    "mission_push_outbox",
    "mission_player_guards",
)
SCHEMA_HINT = (
    "tools/migrate/migrations/pandora_mission/000001_baseline.up.sql"
    "(或重放 deploy/mysql-init/16-mission-tables.sql)"
)

# 容量巡检周期:启动即跑一轮拿基线(上线时就已超限当场可见),之后每小时一轮。
# 走 information_schema 估算(毫秒级、不锁表、不扫数据),放启动路径安全;
# 绝不用 COUNT(*)(千万行表几十秒,会拖垮滚动更新)。
CAPACITY_INTERVAL_SEC = 3600.0

# 启动期 DB 调用的超时,对齐 Go 的 context.WithTimeout(5s)。
# 没有超时时库半死不活会让进程**挂着不退** —— 既不 Ready 也不失败,K8s 只能等到
# startupProbe 超时才杀,而日志停在最后一条 info 上,看着像"卡在这一步"。
STARTUP_DB_TIMEOUT_SEC = 5.0

# 会话权威 Redis 的启动期 Ping 超时,与 Go 侧 sessiongate.MustBuild 同为 3s。
SESSION_GATE_PING_TIMEOUT_SEC = 3.0

# 选举名。与 Go 的 writerlease.Config.Election 逐字相同 —— 改了它,滚动升级期新旧
# 副本会各自在**不同的 key 前缀**下选举,两个都当选,单写者保证凭空消失且零报错。
PUSH_LEASE_ELECTION = "mission/push_publisher"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线)而不是 argparse 惯常的 `--conf`:Go 侧 flag 包用单横线,
    而 run_services.ps1 / start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。
    Python 版必须能被**同一条命令行**拉起,否则那些脚本都要改。
    """
    ap = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    ap.add_argument("-conf", dest="conf", default="etc/mission-dev.yaml")
    return ap.parse_args(argv)


class MissionUpdatePusher:
    """把出箱行投 kafka —— 对应 Go 的 missionUpdatePusher。

    kafka key = player_id(不变量 §9:同玩家事件保序;push 服务按 key 路由到该玩家
    stream);payload 是事务出箱里已序列化的 MissionUpdateEvent,直接透传不再解码。
    """

    __slots__ = ("_producer",)

    def __init__(self, producer: kafkax.KeyOrderedProducer) -> None:
        self._producer = producer

    async def push_mission_update(self, player_id: int, payload: bytes) -> None:
        await self._producer.send_raw(str(player_id), payload)


def _check_push_lease_deploy_strategy(logger, lease_mode: str) -> bool:  # noqa: ANN001
    """机械门禁:RollingUpdate × 非 enforce = 滚动重叠期两个发布器并发。返回是否放行。

    进程看不到 spec.strategy,由 Deployment 把策略作为 annotation 注入 env
    (PANDORA_DEPLOY_STRATEGY),Go 侧另有 manifest 测试钉住 annotation 与真实
    strategy 一致。

    不设这道闸会怎样:MissionUpdateEvent.progressed 是**逐任务全量快照**(不是增量),
    两个发布器并发投递会让同玩家的旧进度快照后到并覆盖新的 —— 玩家 UI 上进度条从
    7/10 退回 3/10,而事件里没有 revision、ts_ms 是各副本墙钟不足以判序,客户端无从
    去重,只能等下次 ListMissions / push.resync 才恢复。
    """
    deploy_strategy = (os.environ.get("PANDORA_DEPLOY_STRATEGY") or "").strip()
    in_managed_k8s = bool((os.environ.get("KUBERNETES_SERVICE_HOST") or "").strip())
    if deploy_strategy:
        if (
            deploy_strategy.lower() == "rollingupdate"
            and lease_mode != mconf.PUSH_WRITER_LEASE_ENFORCE
        ):
            logger.error(
                "mission_push_writer_lease_rollingupdate_without_enforce",
                strategy=deploy_strategy,
                mode=lease_mode,
                hint=(
                    "RollingUpdate × push_writer_lease.mode!=enforce = 滚动重叠期两个"
                    "发布器并发,同玩家旧进度快照会后到并覆盖新的(progressed 是全量快照,"
                    "无 revision 可判旧);要么把 mission.push_writer_lease.mode 改 "
                    "enforce,要么把 Deployment 改回单副本 Recreate"
                ),
            )
            return False
        logger.info(
            "mission_push_writer_lease_strategy_checked",
            strategy=deploy_strategy,
            mode=lease_mode,
        )
        return True
    if in_managed_k8s:
        logger.error(
            "mission_push_writer_lease_strategy_annotation_missing",
            mode=lease_mode,
            hint=(
                "受管 k8s 内必须注入 PANDORA_DEPLOY_STRATEGY(取自 Deployment 的 "
                "pandora.dev/deploy-strategy annotation);缺失则无法机械校验 "
                "RollingUpdate×非 enforce 组合,fail-closed 退出。"
                "见 deploy/k8s/services/services.yaml"
            ),
        )
        return False
    # ★ WARN 放行:阻断会把开发环境一起打死(本机裸跑没有这个 env)。
    logger.warning(
        "mission_push_writer_lease_strategy_unknown",
        mode=lease_mode,
        hint="非 k8s 环境(本机裸跑/dev):跳过部署策略机械校验",
    )
    return True


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


async def _main_async(args: argparse.Namespace) -> int:  # noqa: C901, PLR0911, PLR0912, PLR0915
    # 与 Go 同为线性启动闸:拆函数会让"顺序即契约"这件事散掉,故刻意保持长函数。
    logger = plog.setup(SERVICE_NAME)
    logger.info("service_starting", conf=args.conf)

    # ── ① 配置路径 ────────────────────────────────────────────────────────
    try:
        conf_path = pathlib.Path(args.conf).resolve()
    except OSError as exc:
        logger.error("abs_conf_path_failed", err=str(exc))
        return 1

    # ── ②③ 加载 yaml ─────────────────────────────────────────────────────
    try:
        cfg = mconf.Config.load(conf_path)
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        # Go 把"读不到文件"和"结构对不上"分成两个事件名,这里保持同样区分。
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── ④ 保留期清理模式必须能被识别(§9.24)──────────────────────────────
    # 拼错的值会静默回落 report_only:运维以为开了清理、实际一行没删,
    # 库继续无界增长且启动期毫无痕迹。
    try:
        cfg.mission.validate_retention_mode()
    except ValueError as exc:
        logger.error(
            "mission_retention_mode_invalid",
            err=str(exc),
            hint='mission.retention_mode 只接受 "report_only"(默认,不删) 或 "delete"',
        )
        return 1

    # ── ⑤ 启动期硬校验 ───────────────────────────────────────────────────
    try:
        cfg.validate_startup()
    except ValueError as exc:
        logger.error("config_validation_failed", err=str(exc))
        return 1

    # ── ⑥ 配置表(不变量 §9.15)───────────────────────────────────────────
    # 启动强依赖 fail-closed:接取校验 / 进度判定 / 发奖内容全读表。跨表校验器与
    # 首载走**同一门禁**,失败整批不切换保留旧表(对齐 matchmaker)。
    try:
        ct = mcat.load_tables(cfg.config_table.dir)
    except ConfigTableError as exc:
        logger.error("configtable_load_failed", dir=cfg.config_table.dir, err=str(exc))
        return 1
    except OSError as exc:
        logger.error("configtable_load_failed", dir=cfg.config_table.dir, err=str(exc))
        return 1
    for warning in ct.warnings:
        logger.warning("configtable_load_warning", warning=warning)
    logger.info(
        "configtable_loaded",
        dir=cfg.config_table.dir,
        version=ct.version,
        missions=ct.tables.mission_count(),
        conditions=ct.tables.condition_count(),
        rewards=ct.tables.reward_count(),
    )

    # ── ⑦ MySQL 强依赖(pandora_mission:任务状态唯一权威)────────────────
    raw_dsn = cfg.node.mysql_client.dsn
    if not raw_dsn:
        logger.error(
            "mysql_dsn_required",
            hint="node.mysql_client.dsn required (pandora_mission)",
        )
        return 1

    conn_cfg = mysqlx.parse_go_dsn(raw_dsn, default_db=MISSION_DB)
    try:
        # ★ 池参数走 mysqlx.pool_kwargs 统一翻译 —— max_open/max_idle/
        # conn_max_lifetime/ping_timeout 四个字段 yaml 里都写了、Go 侧都读,
        # 手写 create_pool 只传连接身份的话它们**配了不生效且不报错**。
        # 最要紧的是 conn_max_lifetime:没有 pool_recycle,长空闲连接撞上 MySQL 的
        # wait_timeout 被服务端断掉,客户端不知道,**下一条业务 SQL 才暴露**。
        #
        # ★ autocommit=True 与 Go 的 database/sql **默认语义**一致。任务域真正需要
        # 原子性的两处(mutate_player / apply_facts_tx)由 repo 显式
        # `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` + begin() 包起来;
        # 建成 False 会让那两处之外的单语句(mark_reward / delete_push_outbox / sweep)
        # 全部悬在一个从不提交的隐式事务里 —— 补扫标记写了等于没写,下轮再捞一遍。
        mysqlx.assert_pool_conf_supported(cfg.node.mysql_client)
        pool = await asyncmy.create_pool(
            **mysqlx.pool_kwargs(cfg.node.mysql_client, conn_cfg, autocommit=True)
        )
    except asyncio.CancelledError:
        # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
        # 吞掉之后取消就**不再传播** —— 该停的停不下来(§9.16 先摘流量 → 再排空在途)。
        raise
    except BaseException as exc:  # noqa: BLE001
        # ⑧ Go 侧这里是 mysqlx.MustNewClient 的 panic(它内部 Ping 过)。
        # Python 打成结构化事件后 exit 1 —— 方向相同:连不上库绝不带着起来,
        # 否则 Pod Ready、流量切过来,第一条业务请求才暴露。
        logger.error("mysql_connect_failed", err=str(exc), dsn=mysqlx.mask_dsn(raw_dsn))
        return 1
    logger.info("mysql_connected", dsn=mysqlx.mask_dsn(raw_dsn))

    items = None
    exp = None
    mail = None
    producer: kafkax.KeyOrderedProducer | None = None
    push_lease = None
    try:
        # ── ⑨ 严格模式断言(§9.24)────────────────────────────────────────
        # 非严格 sql_mode 下超长写入会被 MySQL **静默截断**(err=nil 但数据被砍断),
        # 等于无声的数据损坏 —— progress blob 被截断 = 玩家进度变成一个解不开的坏行,
        # 而且没有任何错误可查。所以 fail-fast 而不是继续产生坏数据。
        try:
            async with pool.acquire() as conn:
                await asyncio.wait_for(
                    dbguard.assert_strict_mode(conn), timeout=STARTUP_DB_TIMEOUT_SEC
                )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("mysql_strict_mode_required", err=str(exc))
            return 1

        # ── ⑩ schema 检查(缺表 fail-fast 并指向迁移 SQL)─────────────────
        # 后建的库在**既有 volume 上不会自动重放 init SQL**:缺表时服务照常启动、
        # 照常 Ready,直到第一个真实请求才报 "Table doesn't exist"。
        try:
            async with pool.acquire() as conn:
                await asyncio.wait_for(
                    mysqlx.check_tables(conn, SCHEMA_HINT, *REQUIRED_TABLES),
                    timeout=STARTUP_DB_TIMEOUT_SEC,
                )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("mysql_schema_check_failed", err=str(exc))
            return 1

        # ── kafka producer(**弱依赖**:未配 / 失败只 WARN)────────────────
        # 推送禁用后出箱会堆积,由 dbcheck outbox 检查与容量巡检揭示 ——
        # 拒启会让"kafka 抖一下"升级成"任务服务整个不可用"。
        pusher = None
        topic = kafka_topics.TOPIC_MISSION_UPDATE
        if cfg.kafka.brokers:
            try:
                producer = kafkax.KeyOrderedProducer(
                    kafkax.producer_conf_from(cfg.kafka),
                    topic,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.warning(
                    "kafka_producer_init_failed",
                    err=str(exc),
                    hint="mission push disabled until kafka is available; outbox will accumulate",
                )
                producer = None
            else:
                pusher = MissionUpdatePusher(producer)
                logger.info("kafka_producer_ready", topic=topic)
        else:
            logger.warning("kafka_brokers_empty", hint="mission push disabled")

        # ── 发奖下游(inventory / player 强依赖已在 ⑤ 校验;这里只装配)────
        # allow_noop_reward=true 仅 dev 骨架联调 —— noop 返回**错误**让流水滞留
        # PENDING,绝不假装发放成功(§8)。
        if cfg.mission.inventory_addr:
            items = mgranter.GrpcItemGranter(cfg.mission.inventory_addr)
            logger.info("inventory_granter_ready", addr=cfg.mission.inventory_addr)
        else:
            items = mgranter.NoopItemGranter()
            logger.warning(
                "inventory_granter_noop",
                hint="allow_noop_reward=true;道具发放将滞留 PENDING",
            )
        if cfg.mission.player_addr:
            exp = mgranter.GrpcExpGranter(cfg.mission.player_addr)
            logger.info("exp_granter_ready", addr=cfg.mission.player_addr)
        else:
            exp = mgranter.NoopExpGranter()
            logger.warning(
                "exp_granter_noop", hint="allow_noop_reward=true;经验发放将滞留 PENDING"
            )
        if cfg.mission.mail_addr:
            mail = mgranter.GrpcOverflowMailSender(cfg.mission.mail_addr)
            logger.info("overflow_mail_ready", addr=cfg.mission.mail_addr)
        else:
            mail = None
            logger.warning(
                "overflow_mail_disabled",
                hint="背包满时装备发放失败留补扫(mail_addr 未配)",
            )

        # ── 装配链 ────────────────────────────────────────────────────────
        # 保留期清理要写全限定表名,库名传的是 **DSN 里实际连上的那个库**
        # 而不是写死 pandora_mission(TiDB 档 / 测试库连的都不是它)。
        repo = mrepo.MySQLMissionRepo(pool, conn_cfg.get("db") or MISSION_DB)
        catalogs = mcat.CatalogSource(ct.tables)
        uc = mbiz.MissionUsecase(
            repo, catalogs, items, exp, mail, pusher, cfg.mission
        )
        svc = msvc.MissionService(uc)

        # ── ⑪ 会话现行性门(INC-20260722-004)──────────────────────────────
        # 校验客户端面请求的 jti == login 会话权威(pandora:sess)当前一代 ——
        # 顶号后旧 JWT 在 exp 之前就失去按 player_id 定向操作的能力,否则被顶号的
        # 那一方还能继续接/弃/领受害者的任务。
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
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error(
                    "session_gate_redis_failed", err=str(exc), require=require_gate
                )
                return 1
        try:
            sess_gate = sessiongate.must_build(rdb, require_gate)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("session_gate_endpoint_required", err=str(exc))
            return 1

        # ── gRPC / HTTP ──────────────────────────────────────────────────
        # auth_required=False 对应 Go 的 pmw.AuthOptional():两个系统 RPC 无 JWT,
        # caller_id==0 直接过这两层;service 层 systemOnly 负责拒带玩家身份的越权调用。
        grpc_server = pserver.build_grpc_server(
            cfg.server.grpc,
            auth_required=False,
            extra_interceptors=[
                sessiongate.SessionCurrentInterceptor(sess_gate, require_gate)
            ],
        )
        mission_pb2_grpc.add_MissionServiceServicer_to_server(svc, grpc_server)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(grpc_server, [GRPC_SERVICE_FULL_NAME])

        http_app = pserver.build_http_app(SERVICE_NAME)

        # ── ⑫⑬⑭⑮⑯ 推送发布器的写者继任租约 ─────────────────────────────
        # 只包发布器进选举:补扫靠下游三个幂等键、清理靠 DELETE 幂等,两者多副本
        # 并跑本就安全,按 §9.21「可并行 worker 不得为金丝雀强行全局串行化」
        # 不得一起包进来。
        try:
            push_lease_mode = cfg.mission.push_writer_lease.resolve_mode()
        except ValueError as exc:
            logger.error("mission_push_writer_lease_mode_invalid", err=str(exc))
            return 1
        if not _check_push_lease_deploy_strategy(logger, push_lease_mode):
            return 1
        if push_lease_mode == mconf.PUSH_WRITER_LEASE_ENFORCE:
            if not cfg.mission.push_writer_lease.etcd_endpoints:
                logger.error(
                    "mission_push_writer_lease_endpoints_missing",
                    hint="push_writer_lease.mode=enforce 必须配 etcd_endpoints,否则选举无从谈起",
                )
                return 1
            try:
                lease_kwargs = {}
                if cfg.mission.push_writer_lease.lease_ttl_sec > 0:
                    lease_kwargs["lease_ttl_sec"] = (
                        cfg.mission.push_writer_lease.lease_ttl_sec
                    )
                push_lease = writerlease.WriterLease(
                    list(cfg.mission.push_writer_lease.etcd_endpoints),
                    PUSH_LEASE_ELECTION,
                    **lease_kwargs,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("mission_push_writer_lease_start_failed", err=str(exc))
                return 1
            uc.set_push_writer_lease(push_lease)
            logger.info(
                "mission_push_writer_lease_started",
                election=PUSH_LEASE_ELECTION,
                mode=push_lease_mode,
                hint="enforce:只有当选副本跑推送发布器,热备副本照常服务 RPC 与补扫/清理",
            )
        else:
            # ★ WARN 放行:mode=off 时单发布者只由部署形态保证。
            logger.warning(
                "mission_push_writer_lease_disabled",
                hint="mode=off:单发布者只由部署形态保证,只允许单进程 / 单副本 Recreate",
            )

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                configtable_dir=cfg.config_table.dir,
                max_active_missions=cfg.mission.max_active_missions,
                inventory_addr=cfg.mission.inventory_addr,
                player_addr=cfg.mission.player_addr,
                mail_addr=cfg.mission.mail_addr,
                # Go 打的是 time.Duration.String(),5 分钟是 "5m0s" 而不是 yaml 里的 "5m"。
                sweep_interval=godur.duration_string(cfg.mission.sweep_interval_td()),
                session_gate_require=require_gate,
                runtime="python",  # 灰度期用它在 Grafana 里区分两个实现
            )

        background = [
            uc.run_reward_retry,
            uc.run_push_publisher,
            uc.run_retention_sweep,
            (
                "db_capacity_guard",
                lambda: _run_capacity_guard(
                    pool, conn_cfg["db"], CAPACITY_INTERVAL_SEC
                ),
            ),
        ]
        if push_lease is not None:
            # 无 on_elected:接任不推进任何 fence 水位 —— 发布器不携带跨轮次权威意图,
            # 每轮从 MySQL 重读出箱行,动作只是「投 kafka → 删行」,权威态全程不被触碰。
            background.append(("push_writer_lease", lambda: push_lease.run(_noop_on_elected)))

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
        await mbiz.close_quietly(items)
        await mbiz.close_quietly(exp)
        await mbiz.close_quietly(mail)
        if producer is not None:
            with contextlib.suppress(Exception):
                await producer.close()
        pool.close()
        with contextlib.suppress(Exception):
            await pool.wait_closed()


async def _noop_on_elected(_token: int) -> None:
    """当选钩子留空 —— 发布器不携带跨轮次权威意图,接任不推进任何 fence 水位。"""
    return None


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
