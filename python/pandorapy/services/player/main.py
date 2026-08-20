"""Pandora player 服务入口(Python 版)—— 对应 Go 侧 services/account/player/cmd/player/main.go。

职责:玩家档案 / 分池段位 MMR / 英雄池 / 出战养成 / 玩家等级经验 / 领奖记录;
消费 pandora.player.update 幂等 UpdateMMR(idempotency_key=match_id,不变量 §2);
GetMMR 供 battle_result 当真实 MMRReader;GetLoadout / GetPlayerNames 供 DS 面调用。

★ **27 道启动闸,顺序、事件名、方向都与 Go 逐条相同** —— 事件名是 Loki 告警和运维手册
  的入口,改一个字等于静默失去那条告警的覆盖:

    ①  abs_conf_path_failed                         -conf 解不成绝对路径
    ②  config_load_failed                           yaml 读不到
    ③  config_scan_failed                           yaml 结构对不上
    ④  cellroute_init_failed                        cell_route 配了但 Python 只实现单 Cell
    ⑤  player_retention_mode_invalid                retention_mode 拼错
    ⑥  configtable_dir_required                     player 强依赖等级经验表,无 YAML 兜底
    ⑦  configtable_load_failed                      整批校验任一条不过
    ⑧  mysql_dsn_required                           玩家档案落库不可降级
    ⑨  mysql_connect_failed                         连不上库绝不带着起来
    ⑩  mysql_strict_mode_required                   非严格 sql_mode 会静默截断 LONGBLOB
    ⑪  player_experience_schema_invalid             迁移顺序错误要在发布时拦住
    ⑫  player_equipment_schema_invalid              000006 未落地
    ⑬  player_experience_level_invalid              库里等级超出当前表上限
    ⑭  ds_auth_guard_init_failed                    mode 拼错 / 缺 secret
    ⑮  session_gate_redis_failed                    require=true 时 Redis 不通
    ⑯  session_gate_endpoint_required               require=true 时端点漏配
    ⑰  player_push_writer_lease_mode_invalid        选举档位拼错
    ⑱  player_push_writer_lease_rollingupdate_without_enforce  滚动重叠期双发布器
    ⑲  player_push_writer_lease_strategy_annotation_missing    受管 k8s 内无法机械校验
    ⑳  player_push_writer_lease_endpoints_missing   enforce 却没配 etcd
    ㉑  player_push_writer_lease_start_failed        选举起不来
    ㉒  kafka_brokers_empty                         不消费 player.update 就做不了幂等 UpdateMMR
    ㉓  consume_topics_empty                        同上
    ㉔  dlq_producer_init_failed                    player.update 不可静默降级
    ㉕  kafka_consumer_new_failed                   消费者建不起来
    ㉖  no_valid_consumer                           consume_topics 全部无效
    ㉗  app_run_failed                              运行期致命

★ **方向也必须一致**,这六条在 Go 里是 WARN 放行,改成 fail-fast 会让能跑的部署起不来:
    configtable_load_warning / instance_ownership_checker_missing /
    player_push_producer_init_failed / player_push_writer_lease_strategy_unknown /
    player_push_writer_lease_disabled / unknown_consume_topic_skipped

后台循环四条(全部走 safego:裸 create_task 的协程死掉后进程照跑、health 照答 SERVING、
**零日志**):
    - 经验推送出箱发布器(单写者选举下才发布)
    - exp_history 保留期 janitor(默认 report_only)
    - mmr/点数/发卡 幂等历史 janitor(默认 report_only)
    - 容量巡检(启动即一轮拿基线,之后每小时)

运行:
    cd services/account/player
    PYTHONPATH=../../../python:../../../python/gen \\
        ../../../python/.venv/Scripts/python.exe -m pandorapy.services.player.main \\
        -conf etc/player-dev.yaml
"""

from __future__ import annotations

# ★ 必须最先 import:Windows 控制台默认 cp1252,日志里的中文会抛 UnicodeEncodeError
# 并把真正的启动错误顶掉(实测踩过多次)。
from pandorapy import _utf8  # noqa: F401  isort:skip
from pandorapy import config as pconfig

import argparse
import asyncio
import contextlib
import os
import pathlib
import sys

import asyncmy

from pandora.config.v1 import configtable_pb2_grpc as cfggrpc
from pandora.player.v1 import player_pb2_grpc as pgrpc

from pandorapy import dbguard
from pandorapy import dsauth
from pandorapy import internalrpcauth
from pandorapy import kafka_topics
from pandorapy import kafkax
from pandorapy import log as plog
from pandorapy import mysqlx
from pandorapy import redisx
from pandorapy import safego
from pandorapy import server as pserver
from pandorapy import sessiongate
from pandorapy import writerlease
from pandorapy.services.player import biz as pbiz
from pandorapy.services.player import budgets as pbudgets
from pandorapy.services.player import conf as pconf
from pandorapy.services.player import configtable_admin as pctadmin
from pandorapy.services.player import consumer as pconsumer
from pandorapy.services.player import inventory_client as pinv
from pandorapy.services.player import repo as prepo
from pandorapy.services.player import service as psvc
from pandorapy.services.player import tables as ptables

SERVICE_NAME = "player"
HTTP_DEFAULT_PORT = 21002
GRPC_SERVICE_FULL_NAME = "pandora.player.v1.PlayerService"
INTERNAL_GRPC_SERVICE_FULL_NAME = "pandora.player.v1.PlayerInternalService"
CONFIG_ADMIN_SERVICE_FULL_NAME = "pandora.config.v1.ConfigTableAdminService"

PLAYER_NAME_RESOLVE_MAX_CLOCK_SKEW_SEC = 30.0
PLAYER_NAME_RESOLVE_NONCE_PREFIX = "pandora:player:name-resolve:nonce:"

# 玩家域库(dbguard 的 schema 需要显式给,它把表名拼进 SQL)。
PLAYER_DB = "pandora_player"

# 容量巡检周期:启动即跑一轮拿基线(上线时就已超限当场可见),之后每小时一轮。
CAPACITY_INTERVAL_SEC = 3600.0

# Kafka 消费失败处理:业务瞬时错误进程内重试 3 次(间隔 500ms)后进 DLQ
# (infra.md §4.4「失败 3 次进 DLQ」)。与 Go 的 dlqMaxRetries / dlqRetryBackoff 同值。
DLQ_MAX_RETRIES = 3
DLQ_RETRY_BACKOFF_SEC = 0.5

# 会话权威 Redis 的启动期 Ping 超时,与 Go 侧 sessiongate.MustBuild 同为 3s。
SESSION_GATE_PING_TIMEOUT_SEC = 3.0

# 选举名。与 Go 的 writerlease.Config.Election 逐字相同 —— 改了它,滚动升级期新旧副本
# 会各自在**不同的 key 前缀**下选举,两个都当选,单写者保证凭空消失且零报错。
PUSH_LEASE_ELECTION = "player/push_publisher"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线)而不是 argparse 惯常的 `--conf`:Go 侧 flag 包用单横线,
    而 run_services.ps1 / start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。
    Python 版必须能被**同一条命令行**拉起,否则那些脚本都要改。
    """
    ap = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    ap.add_argument("-conf", dest="conf", default="etc/player-dev.yaml")
    return ap.parse_args(argv)


class PlayerEventPusher:
    """把经验出箱行投 kafka —— 对应 Go 的 playerEventPusher。

    key=player_id(同玩家事件保序,不变量 §9);event_type 走 kafka header,push 透传。

    ⚠️ topic 是 **pandora.player.experience**,绝不能发 pandora.player.update:
    旧 player 副本消费 player.update 时不看 event_type header,会把经验事件误解码成
    MMR 事件**污染段位**(金丝雀混跑,§21)。
    """

    __slots__ = ("_producer",)

    def __init__(self, producer: kafkax.KeyOrderedProducer) -> None:
        self._producer = producer

    async def push_player_event(self, player_id: int, event_type: int, payload: bytes) -> None:
        await self._producer.send_raw(str(player_id), payload, event_type)


async def _run_capacity_guard(pool, schema: str, interval_sec: float) -> None:
    """容量巡检(§9.24):**只告警不阻断**。

    容量超限是"要去查的问题",不是"服务不能跑的理由" —— 拒绝启动会把容量问题升级成
    可用性事故。走 information_schema 估算(毫秒级、不锁表、不扫数据),放启动路径安全;
    绝不用 COUNT(*)(千万行表几十秒,会拖垮滚动更新)。
    """
    budgets = pbudgets.budgets()

    async def _once() -> None:
        # schema 用 DSN 里的实际库名(同 repo 的保留期清理):写死的话,DSN 指向别的库时
        # 巡检报的是**另一个库**的行数与平均行长,而两边都不报错。
        async with pool.acquire() as conn:
            result = await dbguard.check_budgets(conn, schema, budgets)
        dbguard.log_violations(result, db=schema)

    # 启动即一轮拿基线,再进周期循环(与 Go 的 runCapacityGuard 同序)。
    await safego.run_once("db_capacity_guard_initial", _once)
    await safego.loop("db_capacity_guard", interval_sec, _once)


def _build_consumers(cfg, uc, logger) -> tuple[list, list] | None:  # noqa: ANN001
    """按 cfg.player.consume_topics 建 KafkaConsumer,handler 按 topic 路由。

    返回 (consumers, dlq_producers);任一致命闸命中返回 None(调用方 exit 1)。

    每个消费者配一个 DLQ producer(topic=pandora.dlq.<topic>):解码毒丸直接进 DLQ,
    业务瞬时错误重试 DLQ_MAX_RETRIES 次后进 DLQ。**DLQ producer 构造失败致命** ——
    不可静默丢 MMR 更新。
    """
    if not cfg.kafka.brokers:
        # ㉒ brokers 空:player 不消费 player.update 就无法做幂等 UpdateMMR。
        logger.error("kafka_brokers_empty", hint="kafka.brokers required")
        return None
    if not cfg.player.consume_topics:
        logger.error("consume_topics_empty", hint="player.consume_topics required")
        return None

    producer_conf = kafkax.producer_conf_from(cfg.kafka)

    consumers: list = []
    dlq_producers: list = []
    for topic in cfg.player.consume_topics:
        if topic == kafka_topics.TOPIC_PLAYER_UPDATE:
            handler = pconsumer.player_update_handler(uc)
        else:
            # ★ WARN 放行,不是拒启:未知 topic 跳过是 Go 的既定方向。
            logger.warning("unknown_consume_topic_skipped", topic=topic)
            continue
        dlq_topic = kafka_topics.build_dlq_topic(topic)
        try:
            dlq = kafkax.KeyOrderedProducer(producer_conf, dlq_topic)
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
            # 吞掉之后取消就**不再传播** —— 该停的停不下来:
            #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
            #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
            # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error(
                "dlq_producer_init_failed",
                topic=topic,
                dlq_topic=dlq_topic,
                err=str(exc),
                hint="player.update 不可静默降级,DLQ 必须可用",
            )
            return None
        dlq_producers.append(dlq)
        try:
            kc = kafkax.KeyOrderedConsumer(
                kafkax.ConsumerConf(
                    brokers=tuple(cfg.kafka.brokers),
                    topic=topic,
                    group_id=cfg.kafka.group_id,
                    retry=kafkax.RetryPolicy(
                        max_retries=DLQ_MAX_RETRIES, backoff_sec=DLQ_RETRY_BACKOFF_SEC
                    ),
                ),
                handler,
                dlq=dlq,
            )
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
            # 吞掉之后取消就**不再传播** —— 该停的停不下来:
            #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
            #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
            # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("kafka_consumer_new_failed", topic=topic, err=str(exc))
            return None
        consumers.append(kc)
        logger.info(
            "kafka_consumer_ready", topic=topic, group=cfg.kafka.group_id, dlq_topic=dlq_topic
        )
    if not consumers:
        logger.error("no_valid_consumer", hint="consume_topics 全部无效")
        return None
    return consumers, dlq_producers


def _check_push_lease_deploy_strategy(logger, lease_mode: str) -> bool:  # noqa: ANN001
    """机械门禁:RollingUpdate × 非 enforce = 滚动重叠期两个发布器并发。返回是否放行。

    进程看不到 spec.strategy,由 Deployment 把策略作为 annotation 注入 env
    (PANDORA_DEPLOY_STRATEGY),Go 侧另有 manifest 测试钉住 annotation 与真实 strategy 一致。

    不设这道闸会怎样:PlayerExperienceEvent 携带的是**绝对值快照**(level / exp_in_level),
    两个发布器并发投递会让旧快照后到覆盖新的 —— 玩家看到等级经验条**倒退**,而事件里
    没有 revision、ts_ms 是各副本墙钟不足以判序,客户端无从去重。
    """
    deploy_strategy = (os.environ.get("PANDORA_DEPLOY_STRATEGY") or "").strip()
    in_managed_k8s = bool((os.environ.get("KUBERNETES_SERVICE_HOST") or "").strip())
    if deploy_strategy:
        if (
            deploy_strategy.lower() == "rollingupdate"
            and lease_mode != pconf.PUSH_WRITER_LEASE_ENFORCE
        ):
            logger.error(
                "player_push_writer_lease_rollingupdate_without_enforce",
                strategy=deploy_strategy,
                mode=lease_mode,
                hint=(
                    "RollingUpdate × push_writer_lease.mode!=enforce = 滚动重叠期两个发布器并发;"
                    "PlayerExperienceEvent 携带绝对值快照(level/exp_in_level),旧快照后到会让玩家"
                    "等级经验条倒退。要么把 player.push_writer_lease.mode 改 enforce,"
                    "要么改回单副本 Recreate"
                ),
            )
            return False
        logger.info(
            "player_push_writer_lease_strategy_checked",
            strategy=deploy_strategy,
            mode=lease_mode,
        )
        return True
    if in_managed_k8s:
        logger.error(
            "player_push_writer_lease_strategy_annotation_missing",
            mode=lease_mode,
            hint=(
                "受管 k8s 内必须注入 PANDORA_DEPLOY_STRATEGY(取自 Deployment 的 "
                "pandora.dev/deploy-strategy annotation),否则无法机械校验 "
                "RollingUpdate×非 enforce 组合"
            ),
        )
        return False
    # ★ WARN 放行:非 k8s 环境(本机裸跑/dev)跳过部署策略机械校验。
    logger.warning(
        "player_push_writer_lease_strategy_unknown",
        mode=lease_mode,
        hint="非 k8s 环境(本机裸跑/dev):跳过部署策略机械校验",
    )
    return True


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

    # ── ②③④ 加载 yaml ────────────────────────────────────────────────────
    try:
        cfg = pconf.Config.load(str(conf_path))
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except NotImplementedError as exc:
        # ★ cell_route.mode 配了但 Python 侧只实现单 Cell(BaseConf 的 after 校验器抛的)。
        #   对应 Go 的 etcdtable.WireRouter 失败分支,事件名保持一致 —— 继续启动会让所有
        #   玩家静默落在单 Cell 上,与配置意图不符。
        logger.error("cellroute_init_failed", err=str(exc))
        return 1
    except Exception as exc:  # noqa: BLE001
        # Go 把"读不到文件"和"结构对不上"分成两个事件名,这里保持同样区分。
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── ⑤ 保留期清理模式 fail-fast(§9.24)────────────────────────────────
    # 拼错 retention_mode(如 "delet" / "true" / "1")时 retention_mode_parsed() 会静默
    # 回落 report_only —— 运维以为开了清理、实际一行没删,库继续增长且启动期毫无痕迹。
    # 这里在触碰 MySQL 之前拒启,把配置错误暴露在发布阶段。
    try:
        cfg.player.validate_retention_mode()
    except ValueError as exc:
        logger.error("player_retention_mode_invalid", err=str(exc))
        return 1
    try:
        cfg.validate_player_name_resolver()
    except ValueError as exc:
        logger.error("player_name_resolve_config_invalid", err=str(exc))
        return 1

    # ── ⑥⑦ 配置表(强依赖:玩家等级经验唯一数值源)──────────────────────
    if not cfg.config_table.dir:
        logger.error(
            "configtable_dir_required",
            hint="config_table.dir required; player experience reads j_玩家等级经验.xlsx only",
        )
        return 1
    try:
        load_result = await asyncio.to_thread(ptables.load_tables, cfg.config_table.dir)
    except asyncio.CancelledError:
        # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
        # 吞掉之后取消就**不再传播** —— 该停的停不下来:
        #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
        #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
        # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
        raise
    except BaseException as exc:  # noqa: BLE001
        logger.error("configtable_load_failed", dir=cfg.config_table.dir, err=str(exc))
        return 1
    for warning in load_result.warnings:
        # ★ WARN 不拒载:manifest 未列出的脏文件不该阻断发布。
        logger.warning("configtable_load_warning", warning=warning)
    ct_store = ptables.Store(load_result.tables, cfg.config_table.dir)
    max_level = load_result.tables.max_level()
    logger.info(
        "player_level_exp_loaded",
        dir=cfg.config_table.dir,
        version=load_result.version,
        levels=len(load_result.tables.levels),
        max_level=max_level,
    )

    # ── ⑧⑨ MySQL(强依赖:玩家档案落库不可降级)──────────────────────────
    raw_dsn = cfg.node.mysql_client.dsn
    if not raw_dsn:
        logger.error(
            "mysql_dsn_required", hint="node.mysql_client.dsn required (pandora_player)"
        )
        return 1
    conn_cfg = mysqlx.parse_go_dsn(raw_dsn, default_db=PLAYER_DB)
    try:
        # ★ 池参数走 mysqlx.pool_kwargs 统一翻译 —— max_open/max_idle/conn_max_lifetime/
        # ping_timeout 四个字段 yaml 里都写了、Go 侧都读,手写 create_pool 只传连接身份的话
        # 它们**配了不生效且不报错**。最要紧的是 conn_max_lifetime:没有 pool_recycle,
        # 长空闲连接撞上 MySQL 的 wait_timeout 被服务端断掉,**下一条业务 SQL 才暴露**。
        #
        # ★ autocommit=True 是刻意的:与 Go 的 `database/sql` **默认语义**一致。player 的
        # 每一处需要原子性的地方(MMR / 加点 / 天赋 / 技能卡 / 经验入账+出箱)都在 repo 里
        # 显式 begin() 包起来,且它们靠 FOR UPDATE 而不是靠"连接级隐式事务"取得互斥。
        # 建成 False 会让**每条只读 SELECT** 也开一个不提交的事务挂在连接上。
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
        # Go 侧这里是 mysqlx.MustNewClient 的 panic(它内部 Ping 过)。Python 打成结构化
        # 事件后 exit 1 —— **方向相同**:连不上库绝不带着起来,否则 Pod Ready、流量切过来,
        # 第一条业务请求才暴露。
        logger.error("mysql_connect_failed", err=str(exc), dsn=mysqlx.mask_dsn(raw_dsn))
        return 1
    logger.info("mysql_connected", dsn=mysqlx.mask_dsn(raw_dsn))

    ownership_checker: pinv.GrpcInstanceOwnershipChecker | None = None
    exp_producer: kafkax.KeyOrderedProducer | None = None
    dlq_producers: list = []
    consumers: list = []
    rdb = None
    try:
        # ── ⑩ 严格模式断言(§9.24)────────────────────────────────────────
        # player_reward_claims.record 是 LONGBLOB,非严格模式下超长**静默截断**会让位图
        # 直接损坏(领奖记录错乱、玩家重复领或永远领不到),而且 err=nil。
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

        # schema 取 DSN 里的实际库名(默认 pandora_player):保留期清理与容量巡检都要把
        # 表名拼进 SQL,写死库名时 DSN 指向别的库会让它们操作**另一个库**的同名表,
        # 且不报错(report_only 下报别人的待清理量,delete 下删别人的行)。
        db_schema = conn_cfg.get("db") or PLAYER_DB
        repo = prepo.MySQLPlayerRepo(pool, db_schema)

        # ── ⑪⑫⑬ 启动 schema 闸 ────────────────────────────────────────
        # 经验相关表列缺失 / 装备实例列未迁移 / 库里等级超出当前表上限时 fail-fast,
        # 不能让副本 Ready 后在首个 GetProfile / AddExperience 才大面积报错。
        try:
            await repo.validate_experience_schema()
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
            # 吞掉之后取消就**不再传播** —— 该停的停不下来:
            #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
            #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
            # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error(
                "player_experience_schema_invalid",
                err=str(exc),
                hint="先执行 pandora_player migration 000002_experience"
                "(players.exp / exp_history / player_push_outbox)",
            )
            return 1
        try:
            await repo.validate_equipment_schema()
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
            # 吞掉之后取消就**不再传播** —— 该停的停不下来:
            #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
            #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
            # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error(
                "player_equipment_schema_invalid",
                err=str(exc),
                hint="先执行 pandora_player migration 000006_equipment_instance_id",
            )
            return 1
        try:
            await repo.validate_experience_levels(max_level)
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
            # 吞掉之后取消就**不再传播** —— 该停的停不下来:
            #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
            #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
            # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error(
                "player_experience_level_invalid",
                err=str(exc),
                hint="修复 players.level 脏数据或发布不低于现存等级上限的玩家等级经验表",
            )
            return 1

        # ── 装配链 ────────────────────────────────────────────────────────
        uc = pbiz.PlayerUsecase(repo, cfg.player)
        uc.set_config_tables(ct_store)

        # 出战装备预设的精确实例归属校验器。未配 inventory_addr 时不接线 —— SetEquipment
        # 随即 fail-closed 拒绝,**不会退化成"不校验就放行"**,因此这里只警告不退出;
        # 真正的门在 loadout_customize_enabled。
        if cfg.player.inventory_addr:
            ownership_checker = pinv.GrpcInstanceOwnershipChecker(cfg.player.inventory_addr)
            uc.set_instance_ownership_checker(ownership_checker)
            logger.info(
                "instance_ownership_checker_grpc", inventory_addr=cfg.player.inventory_addr
            )
        elif cfg.player.loadout_customize_enabled:
            # ★ WARN 放行(方向与 Go 一致),不是拒启。
            logger.warning(
                "instance_ownership_checker_missing",
                hint="loadout_customize_enabled=true 但未配 player.inventory_addr → "
                "SetEquipment 将一律拒绝",
            )

        # ── ⑭ DS 回调令牌守卫 ────────────────────────────────────────────
        # GetLoadout / GetPlayerNames 挂在 Envoy DS 面(:8444),经它进来的调用须带 DS 服务
        # 令牌。配错 mode / 缺 secret 则启动即失败,**不静默退回不校验**。
        try:
            ds_guard = dsauth.guard_from_conf(cfg.ds_auth)
        except ValueError as exc:
            logger.error("ds_auth_guard_init_failed", err=str(exc))
            return 1
        player_svc = psvc.PlayerService(uc, ds_guard)
        if ds_guard is not None:
            logger.info("ds_callback_guard_ready", mode=ds_guard.mode.value)

        # ── ⑮⑯ 会话现行性门(R5 复审 P0-1,INC-20260722-004)────────────
        # 校验客户端面请求的 jti == login 会话权威(pandora:sess)当前一代 —— 顶号后旧 JWT
        # 在 exp 之前就失去按 player_id 定向操作的能力。
        #
        # ★ Ping 的档位与 Go 逐条对齐(pkg/sessiongate.MustBuild):
        #   require=true  端点漏配 → 拒启;Ping 失败 → 拒启
        #   require=false 端点漏配 → gate=None(dev 直连联调);**不 Ping**
        #   把 require=false 也改成 Ping 会让本机无 Redis 时 Go 版起得来、Python 版起不来。
        require_gate = cfg.session_gate.require
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

        # ── gRPC / HTTP ──────────────────────────────────────────────────
        # auth_required=False 对应 Go 的 pmw.AuthOptional():调用方既有后端内部直连
        # (battle_result / login / DS)也有经 Envoy 的客户端,身份差异在 service 层判。
        # Team 名称解析是独立内部信任域：精确 payload-bound HMAC
        # + 跨副本 Redis nonce 消费。不复用 DS callback 密钥或玩家 JWT。
        player_name_verifier = None
        if cfg.player.player_name_resolve_auth_secret:
            try:
                if rdb is None:
                    raise ValueError(
                        "shared Redis nonce authority is required for "
                        "PlayerInternalService.ResolvePlayerNames"
                    )
                await asyncio.wait_for(
                    rdb.ping(), timeout=SESSION_GATE_PING_TIMEOUT_SEC
                )
                replay = internalrpcauth.RedisReplayStore(
                    rdb, PLAYER_NAME_RESOLVE_NONCE_PREFIX
                )
                player_name_verifier = internalrpcauth.Verifier(
                    cfg.player.player_name_resolve_auth_secret,
                    "team",
                    cfg.player.player_name_resolve_auth_audience,
                    PLAYER_NAME_RESOLVE_MAX_CLOCK_SKEW_SEC,
                    replay,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error(
                    "player_name_resolve_verifier_init_failed",
                    err=str(exc),
                    hint=(
                        "shared Redis nonce authority is required for "
                        "PlayerInternalService.ResolvePlayerNames"
                    ),
                )
                return 1
            logger.info(
                "player_name_resolve_verifier_ready",
                caller="team",
                audience=cfg.player.player_name_resolve_auth_audience,
                max_batch=32,
            )
        player_internal_svc = psvc.PlayerInternalService(uc, player_name_verifier)

        grpc_server = pserver.build_grpc_server(
            cfg.server.grpc,
            auth_required=False,
            extra_interceptors=[
                sessiongate.SessionCurrentInterceptor(sess_gate, require_gate)
            ],
        )
        pgrpc.add_PlayerServiceServicer_to_server(player_svc, grpc_server)
        pgrpc.add_PlayerInternalServiceServicer_to_server(
            player_internal_svc, grpc_server
        )
        # ConfigTableAdminService 条件注册:store 装配成功才挂(与 Go 的 ctAdmin != nil 同)。
        reflection_names = [GRPC_SERVICE_FULL_NAME, INTERNAL_GRPC_SERVICE_FULL_NAME]
        if ct_store is not None:
            cfggrpc.add_ConfigTableAdminServiceServicer_to_server(
                pctadmin.ConfigTableAdminService(ct_store), grpc_server
            )
            reflection_names.append(CONFIG_ADMIN_SERVICE_FULL_NAME)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(grpc_server, reflection_names)

        http_app = pserver.build_http_app(SERVICE_NAME)

        # ── 经验推送出箱 producer(弱依赖)────────────────────────────────
        # producer 可用才注入,失败**只警告**(出箱积压不丢,producer 可用后重启补发)。
        if cfg.kafka.brokers:
            try:
                exp_producer = kafkax.KeyOrderedProducer(
                    kafkax.producer_conf_from(cfg.kafka),
                    kafka_topics.TOPIC_PLAYER_EXPERIENCE,
                )
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.warning(
                    "player_push_producer_init_failed",
                    err=str(exc),
                    hint="经验推送出箱积压不丢,producer 可用后重启补发",
                )
            else:
                uc.set_experience_pusher(PlayerEventPusher(exp_producer))
                logger.info(
                    "player_push_producer_ready", topic=kafka_topics.TOPIC_PLAYER_EXPERIENCE
                )

        # ── ⑰⑱⑲⑳㉑ 推送出箱发布器的单写者选举 ───────────────────────
        try:
            push_lease_mode = cfg.player.push_writer_lease.resolve_mode()
        except ValueError as exc:
            logger.error("player_push_writer_lease_mode_invalid", err=str(exc))
            return 1
        if not _check_push_lease_deploy_strategy(logger, push_lease_mode):
            return 1

        push_lease: writerlease.WriterLease | None = None
        if push_lease_mode == pconf.PUSH_WRITER_LEASE_ENFORCE:
            if not cfg.player.push_writer_lease.etcd_endpoints:
                logger.error(
                    "player_push_writer_lease_endpoints_missing",
                    hint="push_writer_lease.mode=enforce 必须配 etcd_endpoints,否则选举无从谈起",
                )
                return 1
            try:
                lease_kwargs = {}
                if cfg.player.push_writer_lease.lease_ttl_sec > 0:
                    lease_kwargs["lease_ttl_sec"] = cfg.player.push_writer_lease.lease_ttl_sec
                push_lease = writerlease.WriterLease(
                    list(cfg.player.push_writer_lease.etcd_endpoints),
                    PUSH_LEASE_ELECTION,
                    **lease_kwargs,
                )
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("player_push_writer_lease_start_failed", err=str(exc))
                return 1
            uc.set_push_writer_lease(push_lease)
            logger.info(
                "player_push_writer_lease_started",
                election=PUSH_LEASE_ELECTION,
                mode=push_lease_mode,
            )
        else:
            # ★ WARN 放行:mode=off 时单发布者只由部署形态保证。
            logger.warning(
                "player_push_writer_lease_disabled",
                hint="mode=off:单发布者只由部署形态保证,只允许单进程 / 单副本 Recreate",
            )

        # ── ㉒㉓㉔㉕㉖ kafka 消费者 ────────────────────────────────────
        built = _build_consumers(cfg, uc, logger)
        if built is None:
            return 1
        consumers, dlq_producers = built

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                kafka_brokers=list(cfg.kafka.brokers),
                kafka_group=cfg.kafka.group_id,
                consume_topics=list(cfg.player.consume_topics),
                base_mmr=cfg.player.base_mmr,
                runtime="python",  # 灰度期用它在 Grafana 里区分两个实现
            )

        background = [
            # 只包 RunPushOutboxPublisher 进选举:两个 janitor 的 DELETE 天然幂等、多副本
            # 并跑安全,按 §9.21「可并行 worker 不得为金丝雀强行全局串行化」不得一起包进来。
            uc.run_push_outbox_publisher,
            uc.run_exp_history_janitor,
            uc.run_history_janitor,
            (
                "db_capacity_guard",
                lambda: _run_capacity_guard(pool, db_schema, CAPACITY_INTERVAL_SEC),
            ),
        ]
        if push_lease is not None:
            # 无 on_elected:接任不推进任何 fence 水位(发布器不携带跨轮次权威意图)。
            background.append(("push_writer_lease", lambda: push_lease.run(_noop_on_elected)))
        background.extend(_consumer_runner(kc) for kc in consumers)

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
        for kc in consumers:
            with contextlib.suppress(Exception):
                kc.stop()
        for dlq in dlq_producers:
            with contextlib.suppress(Exception):
                await dlq.close()
        if exp_producer is not None:
            with contextlib.suppress(Exception):
                await exp_producer.close()
        if ownership_checker is not None:
            with contextlib.suppress(Exception):
                await ownership_checker.close()
        if rdb is not None:
            with contextlib.suppress(Exception):
                await rdb.aclose()
        pool.close()
        with contextlib.suppress(Exception):
            await pool.wait_closed()


async def _noop_on_elected(_token: int) -> None:
    """当选钩子留空 —— 发布器不携带跨轮次权威意图,接任不推进任何 fence 水位。"""
    return None


def _consumer_runner(kc):  # noqa: ANN001, ANN202
    async def _run() -> None:
        await kc.run()

    _run.__name__ = "kafka_consumer"
    return _run


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        return asyncio.run(_main_async(args))
    except KeyboardInterrupt:
        return 0
    except Exception:  # noqa: BLE001
        # ㉗ 对应 Go 侧 app_run_failed。
        plog.get().exception("app_run_failed")
        return 1


if __name__ == "__main__":
    with contextlib.suppress(BrokenPipeError):
        sys.exit(main())
