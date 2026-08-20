"""Pandora battle_result 服务入口(Python 版)—— 对应 Go 侧
services/battle/battle_result/cmd/battle_result/main.go。

职责:Model-B 经 Guard + Redis active 校验的同步 ReportResult 幂等落库并算 MMR;
legacy/off 才可选消费 pandora.battle.result。始终消费 pandora.ds.lifecycle 的 ABANDONED
做 DS 崩溃补偿(不变量 §4),落库同事务写 player.update / drop / match_release 出箱
+ 后台发布器可靠投递(不变量 §4),并提供战绩查询 RPC。

★ 启动闸(逐条对应 Go 侧的 os.Exit / Must* / Validate,**事件名逐字相同** ——
  Loki 上按事件名建的告警对不上就是静默失去覆盖;**顺序也与 Go 相同** ——
  顺序不同会让同一份坏配置在两栈上报不同的第一个错误):

    ①  abs_conf_path_failed                fail-fast  配置路径解析
    ②  config_load_failed                  fail-fast  读不到 yaml / 语法错
    ③  config_scan_failed                  fail-fast  结构对不上(含 cell_route 未实现闸)
    ④  ds_auth_fence_config_invalid        fail-fast  redis 权威缺 fence 配置
    ⑤  battle_result_ingress_invalid       fail-fast  redis 权威还订阅无凭据 topic
    ⑥  battle_retention_mode_invalid       fail-fast  清理模式拼错(六个月口径静默失效)
    ⑦  battle_authority_mode_unsupported   fail-fast  **Python 专有**,见下方"诚实差异"
    ⑧  mysql_dsn_required                  fail-fast  权威库缺失(结算落库不可降级)
    ⑨  mysql_connect_failed                fail-fast  Go 侧是 MustNewClient 的 panic
    ⑩  mysql_strict_mode_required          fail-fast  非严格 sql_mode = 静默截断
    ⑪  mmr_reader_grpc / mmr_reader_static  INFO      读真实 MMR 还是静态兜底的唯一判据
    ⑫  player_update_producer_init_failed   WARN      弱依赖:出箱积压不丢
        / player_update_producer_ready       INFO
        / kafka_brokers_empty                WARN      (brokers 空时;闸⑯ 才是 fatal 那条)
    ⑬  battle_recovery_outbox_schema_invalid fail-fast match_release / drop 出箱缺表
    ⑭  battle_progress_schema_invalid       fail-fast  进度五表缺表 / 列契约漂移
    ⑮  match_releaser_grpc                  INFO
        / match_releaser_disabled            WARN      legacy 档地址未配
    ⑯  drop_granter_grpc / drop_granter_disabled       弱依赖
    ⑰  configtable_dir_required             fail-fast  怪物击杀经验唯一权威缺失
    ⑱  configtable_load_failed              fail-fast  manifest/checksum/行数/整批校验
        / configtable_load_warning           WARN
        / configtable_loaded                 INFO
    ⑲  drop_overflow_mail_grpc / drop_overflow_mail_disabled  弱依赖
    ⑳  mission_outbox_schema_check_failed   fail-fast  已开转发但缺 000010 迁移
        / mission_forward_grpc / mission_forward_disabled
    ㉑  ds_auth_guard_init_failed            fail-fast  mode=permissive/enforce 但缺 secret
        / ds_callback_guard_ready            INFO
    ㉒  kafka_brokers_empty                  fail-fast  **消费侧**:不消费就不结算
    ㉓  consume_topics_empty                 fail-fast
    ㉔  unknown_consume_topic_skipped        WARN
    ㉕  dlq_producer_init_failed             fail-fast  不可静默降级为丢消息模式
    ㉖  kafka_consumer_new_failed            fail-fast
    ㉗  no_valid_consumer                    fail-fast  consume_topics 全部无效

  方向也与 Go 一致:配置表加载 warning 是 WARN 放行(脏文件不拒批次),
  容量巡检超预算只 ERROR 不阻断。

★★ **诚实标注的两处差异**(不是遗漏,是本轮刻意的边界):

  (A) 闸⑦ `battle_authority_mode_unsupported` —— Go 没有这道闸,Python 有。

      `ds_auth.authority_mode=redis`(Model-B)需要三样 Python 侧**没有**的东西:
      Redis active credential checker、terminal_release_outbox 的两阶段 relay、
      dsauth etcd capability fence。缺了它们而照常启动的后果不是"少个功能":
        · 结算 DS 的 pod **永远不会被回收**(terminal release worker 不存在)→ 资源泄漏;
        · 失租 / 旧 epoch 的副本继续结算(fence 不存在)→ Model-B 的"唯一授权权威"前提失效。
      两条都是静默的。所以这里选**拒绝启动**而不是 WARN 放行 ——
      同一份 prod yaml 在 Go 上起得来、Python 上起不来,方向是响亮的。
      **dev / legacy 档(`authority_mode: legacy`,仓库里的 battle_result-dev.yaml)不受影响。**

  (B) `ReportProgress`(实时进度通道)Python 侧未实现,启动打一条
      `battle_progress_channel_not_implemented` WARN,RPC 返回 ERR_INVALID_STATE
      (与 Go 在 `progress_enabled=false` 时**同一个码**,DS 收到即停流回退局后结算)。
      **但结算侧的进度收口是完整的**(repo._settle_progress_stream_tx):它打终局标记、
      并按服务端水位决定是否抑制结算掉落 —— 少了它,一场走过实时通道的对局会被
      实时通道发一遍掉落、再被结算路径发一遍。
      ⚠️ 混部风险(必须知道):若同一对局的进度批次先落在 **Go 副本**、结算落在
      **Python 副本**,DS 会在中途被 Python 拒掉停流,而结算时水位 >0 又会抑制结算掉落
      → 停流之后那段的掉落两边都不发。**progress_enabled=true 的环境不要 Go/Python 混部**。

后台循环(全部走 safego / server.run:裸 create_task 的协程死掉后进程照跑、
health 照答 SERVING、**零日志**):
    ① kafka 消费循环 × N(每 topic 一条)
    ② player.update 出箱发布器(FIFO 保序,失败中断本轮)
    ③ 掉落出箱发布器(单行失败 continue,不阻塞其他玩家)
    ④ 撮合状态释放出箱发布器(明确成功才删行)
    ⑤ 保留期清理(§9.24,本服默认**真删**,留 180 天)
    ⑥ 容量巡检(启动即一轮拿基线;超预算只告警不阻断)

  Go 还有 RunProgressPublisher / RunMissionForwarder / RunTerminalReleasePublisher,
  随各自通道一起未迁移(见上方差异 A/B)。

运行:
    cd services/battle/battle_result
    PYTHONPATH=../../../python:../../../python/gen \\
        python -m pandorapy.services.battle_result.main -conf etc/battle_result-dev.yaml
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
from pandora.battle.v1 import battle_pb2_grpc

from pandorapy import dbguard, dsauth, godur, kafka_topics, kafkax, mysqlx, safego
from pandorapy import log as plog
from pandorapy import server as pserver
from pandorapy.services.battle_result import biz as bbiz
from pandorapy.services.battle_result import budgets as bbudgets
from pandorapy.services.battle_result import catalog as bcat
from pandorapy.services.battle_result import clients as bcli
from pandorapy.services.battle_result import conf as bconf
from pandorapy.services.battle_result import consumer as bcons
from pandorapy.services.battle_result import repo as brepo
from pandorapy.services.battle_result import service as bsvc

SERVICE_NAME = "battle_result"
HTTP_DEFAULT_PORT = 21022

# 结算权威库(与 Go 的 dbguard.New(db, "pandora_battle", ...) 同)。
BATTLE_DB = brepo.BATTLE_DB

# Kafka 消费失败处理:业务瞬时错误进程内重试 3 次(间隔 500ms)后进 DLQ
# (infra.md §4.4「失败 3 次进 DLQ」)。与 Go 的 dlqMaxRetries / dlqRetryBackoff 同值。
DLQ_MAX_RETRIES = 3
DLQ_RETRY_BACKOFF_SEC = 0.5

# 启动期 DB 调用的超时,对齐 Go main.go 的 context.WithTimeout(5s)。
# 库半死不活(TCP 通但不回包)时,没有超时的话进程会**挂在这里不退** ——
# 表现是 Pod 一直卡在启动中,而 k8s 只看到「未 Ready」,排障的人根本不知道卡在哪道闸上。
STARTUP_DB_TIMEOUT_SEC = 5.0

# 容量巡检间隔(Go 的 runCapacityGuard 用固定 1h ticker)。
CAPACITY_GUARD_INTERVAL_SEC = 3600.0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线)而不是 argparse 惯常的 `--conf`:Go 侧 flag 包用单横线,
    而 run_services.ps1 / start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。
    Python 版必须能被**同一条命令行**拉起,否则那些脚本都要改。
    """
    ap = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    ap.add_argument("-conf", dest="conf", default="etc/battle_result-dev.yaml")
    return ap.parse_args(argv)


class PlayerUpdatePusher:
    """把 kafkax.KeyOrderedProducer 适配成 biz 需要的 push_player_update。

    key=player_id(不变量 §9 同玩家事件保序)。对应 Go 的 playerUpdatePusher。
    """

    __slots__ = ("_producer",)

    def __init__(self, producer: kafkax.KeyOrderedProducer) -> None:
        self._producer = producer

    async def push_player_update(self, player_id: int, payload: bytes) -> None:
        await self._producer.send_raw(str(player_id), payload)


async def _run_capacity_guard(pool, schema: str = BATTLE_DB) -> None:  # noqa: ANN001
    """容量巡检(§9.24):**只告警不阻断**。

    容量超限是"要去查的问题",不是"服务不能跑的理由" ——
    拒绝启动会把容量问题升级成可用性事故。

    走 information_schema 估算(毫秒级、不锁表、不扫数据),放启动路径安全;
    绝不用 COUNT(*)(千万行表几十秒,会拖垮滚动更新)。
    """
    table_budgets = bbudgets.budgets()

    async def _once() -> None:
        async with pool.acquire() as conn:
            result = await dbguard.check_budgets(conn, schema, table_budgets)
        dbguard.log_violations(result, db=schema)

    # 启动即一轮拿基线(上线时就已超限当场可见),再进周期循环(与 Go 的 runCapacityGuard 同序)。
    await safego.run_once("db_capacity_guard_initial", _once)
    await safego.loop("db_capacity_guard", CAPACITY_GUARD_INTERVAL_SEC, _once)


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
        cfg = bconf.Config.load(str(conf_path))
    except pconfig.ConfigLoadError as exc:
        # Go 的 c.Load() 覆盖"读"与"解析"两步,两者都归 config_load_failed。
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        # 结构对不上才是 config_scan_failed(含 cell_route.mode 非空的未实现闸,
        # 以及 battle 段出现 monster_exp 这类已删除键 —— extra="forbid" 会拒)。
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── ④ DS 授权 fence 配置 ─────────────────────────────────────────────
    try:
        cfg.ds_auth.validate_redis_fence()
    except Exception as exc:  # noqa: BLE001
        logger.error("ds_auth_fence_config_invalid", err=str(exc))
        return 1

    # ── ⑤ Model-B 入口收敛 ───────────────────────────────────────────────
    try:
        cfg.validate_redis_authority_ingress()
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "battle_result_ingress_invalid",
            err=str(exc),
            hint="Model-B 只允许受 Guard/Redis active/receipt 保护的 ReportResult RPC;"
            "Kafka 只保留 ds.lifecycle",
        )
        return 1

    # ── ⑥ 保留期清理模式 ─────────────────────────────────────────────────
    # 本服默认真删(战报只留六个月),拼错的模式值会静默回落 report_only ——
    # 库继续无界增长且没人发现,必须拒启。
    try:
        cfg.battle.validate_retention_mode()
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "battle_retention_mode_invalid",
            err=str(exc),
            hint='battle.retention_mode 只接受 "delete"(留空即此) 或 "report_only"',
        )
        return 1

    # ── ⑦ authority_mode(Python 专有闸,见模块头差异 A)──────────────────
    if cfg.ds_auth.authority_mode_redis():
        logger.error(
            "battle_authority_mode_unsupported",
            authority_mode=cfg.ds_auth.authority_mode,
            hint="Python 版 battle_result 未实现 Model-B:缺 Redis active credential checker、"
            "terminal_release_outbox 两阶段 relay、dsauth etcd fence。"
            "带病启动会让结算 DS 的 pod 永不回收、且失租副本仍能结算(两者都静默)。"
            "请用 Go 版跑本服务,或把 ds_auth.authority_mode 设为 legacy",
        )
        return 1

    # ── ⑧ MySQL 强依赖(结算落库不可降级)────────────────────────────────
    raw_dsn = cfg.node.mysql_client.dsn
    if not raw_dsn:
        logger.error("mysql_dsn_required", hint="node.mysql_client.dsn required (pandora_battle)")
        return 1

    conn_cfg = mysqlx.parse_go_dsn(raw_dsn, default_db=BATTLE_DB)
    try:
        # ★ 池参数走 mysqlx.pool_kwargs 统一翻译 —— max_open/max_idle/conn_max_lifetime/
        # ping_timeout 四个字段 yaml 里都写了、Go 侧都读,手写 create_pool 只传连接身份的话
        # 它们**配了不生效且不报错**。最要紧的是 conn_max_lifetime:没有 pool_recycle,
        # 长空闲连接撞上 MySQL 的 wait_timeout 被服务端断掉,客户端不知道,
        # **下一条业务 SQL 才暴露**。
        #
        # ★ autocommit=True:与 Go 的 database/sql **默认语义**一致。
        # 本服的每一条多语句写路径都显式 begin()(repo.save_result / _delete_by_match_ids_tx),
        # 只读路径是单条 SELECT —— 建成 False 会让每条只读 SELECT 也开一个事务并挂着,
        # 连接归池后被下个请求复用时 REPEATABLE READ 的快照仍停在上次那一刻,
        # **读到任意陈旧的数据且零报错**(表现:刚落库的战绩查不到)。
        mysqlx.assert_pool_conf_supported(cfg.node.mysql_client)
        pool = await asyncmy.create_pool(
            **mysqlx.pool_kwargs(cfg.node.mysql_client, conn_cfg, autocommit=True)
        )
    except asyncio.CancelledError:
        # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
        # 启动路径上吞掉会把 Ctrl-C / 上层取消翻译成某道闸的失败,报出假的失败原因。
        raise
    except BaseException as exc:  # noqa: BLE001
        # ⑨ Go 侧这里是 mysqlx.MustNewClient 的 panic(它内部 Ping 过)。
        logger.error("mysql_connect_failed", err=str(exc), dsn=mysqlx.mask_dsn(raw_dsn))
        return 1
    logger.info("mysql_connected", dsn=mysqlx.mask_dsn(raw_dsn))

    producers: list[kafkax.KeyOrderedProducer] = []
    consumers: list[tuple[kafkax.KeyOrderedConsumer, str]] = []
    closables: list[object] = []
    try:
        async with pool.acquire() as conn:
            # ── ⑩ 严格模式断言(§9.24)────────────────────────────────────
            # 非严格 sql_mode 下超长写入会被 MySQL **静默截断**(err=nil 但数据被砍断),
            # 等于无声的数据损坏 —— drop 出箱的 CSV 被砍掉尾部 = 玩家少拿几件掉落,
            # 而全链零报错。这是唯一值得因数据库检查而 fail-fast 的场景。
            try:
                await asyncio.wait_for(
                    dbguard.assert_strict_mode(conn), timeout=STARTUP_DB_TIMEOUT_SEC
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("mysql_strict_mode_required", err=str(exc))
                return 1

        # ── ⑪ MMR reader ────────────────────────────────────────────────
        # player_addr 空 → 静态 base_mmr 兜底。这条 INFO 是"到底读没读真实 MMR"的唯一判据:
        # 静态兜底下两队均分恒等 → 胜 +K/2、负 -K/2,看起来完全正常。
        if cfg.battle.player_addr:
            mmr_reader = bcli.GrpcMMRReader(cfg.battle.player_addr)
            closables.append(mmr_reader)
            logger.info("mmr_reader_grpc", player_addr=cfg.battle.player_addr)
        else:
            mmr_reader = bcli.StaticMMRReader(cfg.battle.base_mmr)
            logger.info(
                "mmr_reader_static",
                base_mmr=cfg.battle.base_mmr,
                hint="player_addr 未配置 → StaticMMRReader 兜底",
            )

        # ── ⑫ player.update producer(弱依赖)───────────────────────────
        # init 失败则出箱积压等 producer 可用,**不丢**(行已随结算同事务落库)。
        pusher = None
        producer_conf = kafkax.producer_conf_from(cfg.kafka)
        if cfg.kafka.brokers:
            try:
                producer = kafkax.KeyOrderedProducer(
                    producer_conf, kafka_topics.TOPIC_PLAYER_UPDATE
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.warning(
                    "player_update_producer_init_failed",
                    err=str(exc),
                    hint="outbox rows accumulate (not dropped); "
                    "publisher resumes when producer is available",
                )
            else:
                producers.append(producer)
                pusher = PlayerUpdatePusher(producer)
                logger.info(
                    "player_update_producer_ready", topic=kafka_topics.TOPIC_PLAYER_UPDATE
                )
        else:
            logger.warning(
                "kafka_brokers_empty", hint="outbox publisher idle until brokers configured"
            )

        # ── ⑬⑭ 出箱 / 进度 schema 探测 ──────────────────────────────────
        repo = brepo.MySQLBattleRepo(pool)
        try:
            await asyncio.wait_for(
                repo.validate_recovery_outbox_schema(), timeout=STARTUP_DB_TIMEOUT_SEC
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error(
                "battle_recovery_outbox_schema_invalid",
                err=str(exc),
                hint=brepo.RECOVERY_SCHEMA_HINT,
            )
            return 1
        try:
            # 实时进度三表在**每次结算**都被无条件访问(_settle_progress_stream_tx 收口),
            # 与 progress_enabled 开关无关,必须启动即探测(不能 Ready 后首个结算才炸,§16.4)。
            await asyncio.wait_for(
                repo.validate_progress_schema(), timeout=STARTUP_DB_TIMEOUT_SEC
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error(
                "battle_progress_schema_invalid", err=str(exc), hint=brepo.PROGRESS_SCHEMA_HINT
            )
            return 1

        # ── ⑮ matchmaker releaser ───────────────────────────────────────
        # 结算/废弃落库后调 matchmaker.ReleaseMatch 释放残留撮合状态,修复
        # 「结算返回 Hub 后玩家无法再次匹配(StartMatch 4002)」。
        # (Go 在 authority_mode=redis 下此项为强依赖 fail-fast;那条路径已被闸⑦拦下。)
        releaser = None
        if cfg.battle.matchmaker_addr:
            releaser = bcli.GrpcMatchReleaser(cfg.battle.matchmaker_addr)
            closables.append(releaser)
            logger.info("match_releaser_grpc", matchmaker_addr=cfg.battle.matchmaker_addr)
        else:
            logger.warning(
                "match_releaser_disabled",
                hint="local legacy profile only: matchmaker_addr is empty and "
                "match release publisher is disabled",
            )

        uc = bbiz.BattleResultUsecase(repo, mmr_reader, pusher, releaser, cfg.battle)

        # ── ⑯ inventory 掉落发放器(弱依赖)──────────────────────────────
        if cfg.battle.inventory_addr:
            granter = bcli.GrpcInstanceGranter(cfg.battle.inventory_addr)
            closables.append(granter)
            uc.set_instance_granter(granter)
            logger.info(
                "drop_granter_grpc",
                inventory_addr=cfg.battle.inventory_addr,
                item_rules_source="configtable/drop+item",
            )
        else:
            logger.warning(
                "drop_granter_disabled",
                hint="inventory_addr 未配置 → 战斗装备掉落不发放"
                "(drop 出箱积压不丢,配好地址重启补发)",
            )

        # ── ⑰⑱ 配置表(不变量 §9.15)────────────────────────────────────
        # 怪物击杀经验的**唯一数值权威**(role_level 表的「击杀经验」列)。
        # 与 matchmaker 的可选模式不同,这里是**启动强依赖**:progress 通道开着却没有
        # 经验表时,每条击杀事实都会被按可重试错误退回,DS 原批重试到天荒地老。
        # 通道关闭时表也照样加载 —— 配置齐备是部署契约的一部分,少一张表说明 ConfigMap
        # 没挂对,早失败早发现。
        if not cfg.config_table.dir:
            logger.error(
                "configtable_dir_required",
                hint="battle_result 的怪物击杀经验只来自 role_level 表;"
                "请配置 config_table.dir 并挂载 configtable 卷",
            )
            return 1
        # yaml 里的 config_table.dir 是**相对进程工作目录**的,不是相对配置文件。
        # 这是 Go 版的既有契约(run_services.ps1 用 -WorkingDirectory $svcDir 启动进程),
        # Python 版必须用同一个契约,否则同一份 yaml 两个实现解出不同路径。
        # 刻意不做"找不到就换个基准再试"的兜底:那样会在某些机器上碰巧成功、
        # 某些机器上加载到错误批次,而加载错批次比启动失败严重得多。
        ct_dir = pathlib.Path(cfg.config_table.dir)
        if not ct_dir.is_absolute():
            ct_dir = (pathlib.Path.cwd() / ct_dir).resolve()
        try:
            ct_result = bcat.load_tables(ct_dir)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
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
        ct_store = bcat.Store(ct_result.tables, str(ct_dir))
        logger.info(
            "configtable_loaded",
            dir=str(ct_dir),
            version=ct_result.version,
            role_level_rows=ct_result.tables.role_level_count(),
            item_rows=ct_result.tables.item_count(),
            drop_rows=ct_result.tables.drop_count(),
        )
        # 注入的是 **Store** 而不是当前批次的表快照:热更(ReloadConfigTable)后
        # Store 原子换指针;注入快照会让经验表 / 掉落白名单一直停在启动那一版。
        uc.set_monster_exp_table(ct_store)
        uc.set_battle_item_catalog(ct_store)

        # ── ⑲ 背包满溢出转邮件(弱依赖)──────────────────────────────────
        # 传源键 battle_drop:{match}:{player} 至 mail,领取时 GrantInstances 同键去重
        # (直发与邮件链至多一次)。
        if cfg.battle.mail_addr:
            mail_sender = bcli.GrpcMailSender(cfg.battle.mail_addr)
            closables.append(mail_sender)
            uc.set_mail_sender(mail_sender)
            logger.info("drop_overflow_mail_grpc", mail_addr=cfg.battle.mail_addr)
        else:
            logger.warning(
                "drop_overflow_mail_disabled",
                hint="mail_addr 未配置 → 背包满掉落留在出箱轮询重试(不丢,不转邮件)",
            )

        # ── ⑳ 任务事实转发(弱依赖 + schema 强前置)────────────────────────
        # mission_addr 空 = 转发整体关闭;已启用则必须先确认出箱表存在(000010 迁移):
        # 缺表会让每次 ReportProgress 在事务里炸 = 整条实时进度通道不可用。
        if cfg.battle.mission_addr:
            try:
                await asyncio.wait_for(
                    repo.validate_mission_outbox_schema(), timeout=STARTUP_DB_TIMEOUT_SEC
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error(
                    "mission_outbox_schema_check_failed",
                    err=str(exc),
                    hint=brepo.MISSION_SCHEMA_HINT,
                )
                return 1
            # ⚠️ Go 在这里还会注入 GrpcMissionReporter 并起 RunMissionForwarder。
            # 任务事实**只由 ReportProgress 产生**,而该通道 Python 侧未实现 →
            # 一行 battle_mission_outbox 都不会产生,起转发器只是空转。
            # schema 闸照跑:它校验的是部署契约,漏掉会让一份缺迁移的环境在 Go 上拒启、
            # Python 上放行。
            logger.warning(
                "mission_forward_disabled",
                mission_addr=cfg.battle.mission_addr,
                hint="mission_addr 已配置且出箱表就绪,但 Python 版未实现实时进度通道 → "
                "不产生任务出箱行,任务进度不受战斗事实驱动。需要任务转发请用 Go 版跑本服务",
            )
        else:
            logger.warning(
                "mission_forward_disabled",
                hint="mission_addr 未配置 → 不产生任务出箱行,任务进度不受战斗事实驱动",
            )

        # 实时进度通道未实现的**唯一启动期判据**(§14.2 同 inventory 的 bag 域处理):
        # 不打这条的话,配了 progress_enabled: true 的环境启动日志一片正常,
        # 而 DS 的进度上报会成批拿 ERR_INVALID_STATE —— 排查只能从 DS 侧往回追。
        logger.warning(
            "battle_progress_channel_not_implemented",
            progress_enabled=cfg.battle.progress_enabled,
            hint="Python 版未实现 ReportProgress(实时进度通道);DS 会收到 ERR_INVALID_STATE "
            "并停流回退局后结算路径。结算侧的水位收口与掉落抑制**是完整的**。"
            "progress_enabled=true 的环境不要 Go/Python 混部(会丢停流后那段掉落)",
        )

        svc = bsvc.BattleResultService(uc)

        # ── ㉑ DS 回调令牌守卫 ──────────────────────────────────────────
        # 校验 Battle DS 经 :8444 的 ReportResult / ReportProgress。
        # mode=off(默认)→ guard 为 None,不校验。
        try:
            ds_guard = dsauth.guard_from_conf(cfg.ds_auth)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("ds_auth_guard_init_failed", err=str(exc))
            return 1
        svc.set_ds_callback_guard(ds_guard)
        if ds_guard is not None:
            logger.info("ds_callback_guard_ready", mode=ds_guard.mode.value)

        # ── ㉒㉓㉔㉕㉖㉗ kafka 消费者 ───────────────────────────────────
        # battle_result **不可降级**:不消费就不结算,所以这里是 fail-fast 而不是 WARN。
        if not cfg.kafka.brokers:
            logger.error("kafka_brokers_empty", hint="kafka.brokers required")
            return 1
        if not cfg.battle.consume_topics:
            logger.error("consume_topics_empty", hint="battle.consume_topics required")
            return 1

        handlers = {
            kafka_topics.TOPIC_BATTLE_RESULT: bcons.battle_result_handler,
            kafka_topics.TOPIC_DS_LIFECYCLE: bcons.ds_lifecycle_handler,
        }
        for topic in cfg.battle.consume_topics:
            factory = handlers.get(topic)
            if factory is None:
                logger.warning("unknown_consume_topic_skipped", topic=topic)
                continue
            dlq_topic = kafka_topics.build_dlq_topic(topic)
            try:
                dlq = kafkax.KeyOrderedProducer(producer_conf, dlq_topic)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error(
                    "dlq_producer_init_failed",
                    topic=topic,
                    dlq_topic=dlq_topic,
                    err=str(exc),
                    hint="battle_result 不可静默降级,DLQ 必须可用",
                )
                return 1
            producers.append(dlq)
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
                    factory(uc),
                    dlq=dlq,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("kafka_consumer_new_failed", topic=topic, err=str(exc))
                return 1
            consumers.append((kc, topic))
            logger.info(
                "kafka_consumer_ready",
                topic=topic,
                group=cfg.kafka.group_id,
                dlq_topic=dlq_topic,
            )
        if not consumers:
            logger.error("no_valid_consumer", hint="consume_topics 全部无效")
            return 1

        # ── gRPC / HTTP ─────────────────────────────────────────────────
        # auth_required=False 对应 Go 的 pmw.AuthOptional():ReportResult 是 DS 回调
        # (无玩家 JWT),用 AuthRequired 会把它整个挡在门外;DS 身份由 ds_auth 守卫校验。
        # ★ 刻意**不挂** SessionCurrentInterceptor —— Go 的 server/grpc.go 也没有:
        #   DS 回调面没有玩家 jti,挂上去只是个不生效的假象。
        grpc_server = pserver.build_grpc_server(cfg.server.grpc, auth_required=False)
        battle_pb2_grpc.add_BattleResultServiceServicer_to_server(svc, grpc_server)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(grpc_server, [bsvc.GRPC_SERVICE_FULL_NAME])

        http_app = pserver.build_http_app(SERVICE_NAME)

        def _make_consumer_task(kc: kafkax.KeyOrderedConsumer, topic: str):  # noqa: ANN202
            async def _run() -> None:
                await kc.run()

            # 任务名进 safego 的兜底日志:五条消费循环共用一个名字的话,
            # 「哪个 topic 的消费者死了」查不出来。
            _run.__name__ = f"kafka_consumer_{topic}"
            return _run

        background = [_make_consumer_task(kc, topic) for kc, topic in consumers]
        background.append(uc.run_outbox_publisher)
        background.append(uc.run_drop_publisher)
        background.append(uc.run_match_release_publisher)
        background.append(uc.run_retention_sweep)
        # ★ 用 (name, factory) 二元组而不是裸 lambda:匿名 lambda 在 safego 的兜底日志里
        # 只会显示 bg_anonymous,出事时看不出死的是哪条后台循环(server.run 会为此打 WARN)。
        background.append(
            ("db_capacity_guard", lambda: _run_capacity_guard(pool, conn_cfg["db"]))
        )

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                kafka_brokers=list(cfg.kafka.brokers),
                kafka_group=cfg.kafka.group_id,
                consume_topics=list(cfg.battle.consume_topics),
                elo_k=cfg.battle.elo_k_factor,
                base_mmr=cfg.battle.base_mmr,
                outbox_interval=godur.duration_string(
                    cfg.battle.outbox_publish_interval_td()
                ),
                retention_days=cfg.battle.history_retention_days,
                retention_mode=cfg.battle.retention_mode_parsed().value,
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
        # 关闭顺序与建立顺序相反。每一步都 suppress:停机路径上的次生异常会顶掉
        # 真正的退出原因(实测排查成本极高)。
        # 先停消费者:再往库里写就是给一批马上要停的发布器白写。
        for kc, _topic in consumers:
            with contextlib.suppress(Exception):
                kc.stop()
        for producer in producers:
            with contextlib.suppress(Exception):
                await producer.close()
        for closable in closables:
            with contextlib.suppress(Exception):
                await closable.close()  # type: ignore[attr-defined]
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
