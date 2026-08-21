"""Pandora matchmaker 服务入口(Python 版)—— 对应 Go 侧
services/matchmaking/matchmaker/cmd/matchmaker/main.go。

matchmaker 是**开局链路的唯一权威**:它决定谁和谁一局、谁进哪台 DS、谁拿哪张
battle 票。它带病上线的后果不是"匹配不好用",而是**同一玩家进两场对局**
(违反不变量 §1,两场各自 READY、各自拉一台 DS,下游没有任何人能发现),
或者**非队长成员永远停在大厅**(READY 推送静默丢失)。所以启动闸一条都不能省。

★ 启动闸(逐条对应 Go 的 os.Exit / Must* / Errorw,**事件名逐字相同** ——
  Loki 上按事件名建的告警对不上就是静默失去覆盖;**顺序也与 Go 相同** ——
  顺序不同会让同一份坏配置在两栈上报不同的"第一个错误"):

    ①  abs_conf_path_failed                    fail-fast  -conf 解不成绝对路径
    ②  config_load_failed                      fail-fast  yaml 读不到 / 语法坏
    ③  config_scan_failed                      fail-fast  yaml 结构对不上
        └ cellroute_init_failed                fail-fast  cell_route.mode 非空(位置比 Go 早,见下)
    ④  config_validation_failed                fail-fast  team_addr / 四把内部密钥的信任域校验
    ⑤  configtable_load_failed                 fail-fast  关卡表整批 fail-closed
        ├ configtable_load_warning             WARN       manifest 未列出的脏文件(不拒批次)
        ├ configtable_loaded                   INFO       批次版本 + 关卡数
        └ configtable_disabled                 WARN       未配 dir:StartMatch 不校验 map_id
    ⑥  redis_endpoint_required                 fail-fast  撮合队列 / match 状态的唯一权威
    ⑦  redis_ping_failed                       fail-fast  (+ redis_connected INFO)
    ⑧  snowflake_init_failed                   fail-fast  ticket_id / match_id 两个空间
    ⑨  team_call_signer_init_failed            fail-fast  出站组票签名器
        ├ team_reader_ready                    INFO
        └ team_addr_empty                      WARN       跳过队伍校验(须显式 allow_missing_team)
    ⑩  kafka_producer_required_but_unavailable fail-fast  配了 broker 却建不起来
        ├ kafka_producer_ready                 INFO
        └ kafka_producer_disabled_dev_only     WARN       brokers 显式为空(纯轮询档)
    ⑪  session_gate_init_failed                fail-fast  require=true 时漏配端点拒启
    ⑫  ds_allocator_requires_ds_ticket_v2     fail-fast  配了 addr 却没有任何签票档
        ├ ds_ticket_profile_conflict          fail-fast  v2 与 local-off-v1 同时配(互斥)
        ├ ds_ticket_v2_signer_init_failed     fail-fast  (+ ds_ticket_v2_signer_ready INFO)
        ├ local_legacy_signer_init_failed     fail-fast  local-off-v1 档 jwt.secret 不合法
        ├ ds_ticket_local_off_v1_legacy_signer WARN      本机联调档(生产绝不允许)
        ├ allocation_abort_service_auth_init_failed fail-fast  abort 补偿的专用签名器
        ├ ds_allocator_grpc_ready             INFO
        └ ds_allocator_addr_empty             WARN       用 StubDSAllocator(本地骨架档)
    ⑬  locator_notifier_ready                  INFO
        └ locator_addr_empty                   WARN       不上报位置 + 在线闸整道关闭
    ⑭  entry_ratelimiter_ready                 INFO       进场侧限流参数
    ⑮  match_resume_replay_store_init_failed   fail-fast  验签 nonce 的共享消费存储
    ⑯  match_resume_service_auth_init_failed   fail-fast  login → matchmaker 验签器
        ├ team_resume_service_auth_init_failed fail-fast  配了却非法(打错一个字=静默全拒)
        └ team_resume_service_auth_disabled    WARN       未配:team 入队闸 fail-closed
    ⑰  grpc_server_init_failed                 fail-fast
    ⑱  match_loop_leader_gated /               INFO       撮合循环的单写者姿态
        match_loop_direct
        └ match_leader_run_failed              ERROR      选举跑挂(进程不退,继续服务 RPC)
    ⑲  service_ready                           INFO
        └ deprecated_config_key                WARN       enable_solo_match 迁移进度信号
    ⑳  app_run_failed                          fail-fast

  ⚠️ **弱依赖只 WARN 的五条不得改成 fail-fast**(方向错了会把可降级增强升级成
     可用性事故):configtable_disabled / team_addr_empty /
     kafka_producer_disabled_dev_only / ds_allocator_addr_empty / locator_addr_empty。
     反过来,**闸⑩ 配了 broker 却建不起来必须 fail-fast**:组队匹配里只有队长持有
     StartMatch 返回的 match_id 能轮询 GetMatchProgress 兜底,其余成员得知成局 /
     READY / Battle 落点的**唯一通道**就是 pandora.match.progress 推送。以
     pusher=None 受理匹配后把整场进度静默丢弃,比不 Ready 严重得多。

★ 一处与 Go 的**落点差异**(诚实标注,不是等价实现):

  1) 闸③ 的 cellroute_init_failed:Go 在装配链末尾调 `etcdtable.BuildRouter`,失败
     os.Exit。Python 的 `cellroute` 只有静态表与路由算法、没有 BuildRouter 装配,
     于是 `pandorapy.config.BaseConf` 的 pydantic 校验器在**加载配置时**就对
     `cell_route.mode` 非空拒启。事件名保持不变(告警按事件名建),方向一致
     (fail-fast,不会静默按单 Cell 跑);单 Cell(当前唯一形态)两边完全相同。

★ 后台循环只有一条:撮合主循环(`uc.run_match_loop`,内部含 start saga 推进 /
  装箱成局 / 分配推进 / 确认期超时 / 离线回收 6+2 个步骤)。
  **它必须过 leader election**(§9.21):撮合在共享队列上做全局优化,是典型的
  未分区权威。多副本各跑一份会重复成局 —— 同一玩家进两场 match,而且
  **两场都各自"成功"**,没有任何下游能发现。
    - leader.enabled=false(默认):本副本直接跑(单副本 / dev 行为不变)
    - leader.enabled=true:经 etcd 选举,仅当选副本跑;失主取消循环但**进程不退出**,
      继续服务 RPC,新 leader 在 lease TTL 内接管(不停机滚动更新,不变量 §16)

运行:
    cd services/matchmaking/matchmaker
    PYTHONPATH=../../../python:../../../python/gen \\
        python -m pandorapy.services.matchmaker.main -conf etc/matchmaker-dev.yaml
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

from pandora.config.v1 import configtable_pb2_grpc as cfggrpc
from pandora.match.v1 import match_pb2_grpc as matchgrpc

from pandorapy import etcdleader
from pandorapy import godur
from pandorapy import internalrpcauth
from pandorapy import kafka_topics
from pandorapy import kafkax
from pandorapy import log as plog
from pandorapy import offlinewatch
from pandorapy import redisx
from pandorapy import server as pserver
from pandorapy import sessiongate
from pandorapy import snowflake
from pandorapy import snowflake_etcd
from pandorapy import dsticket as pdsticket
from pandorapy.services.login import dsticket as ldsticket
from pandorapy.services.matchmaker import biz as mbiz
from pandorapy.services.matchmaker import catalog as mcat
from pandorapy.services.matchmaker import clients as mclients
from pandorapy.services.matchmaker import conf as mconf
from pandorapy.services.matchmaker import entry_limiter as mlimiter
from pandorapy.services.matchmaker import repo as mrepo
from pandorapy.services.matchmaker import service as msvc

SERVICE_NAME = "matchmaker"
HTTP_DEFAULT_PORT = 21011

# 内部东西向验签的时钟偏差窗。与 Go 的 `30*time.Second` 同值 —— 调大等于把重放窗放宽。
RESUME_AUTH_MAX_CLOCK_SKEW_SEC = 30.0

# 验签 nonce 的 Redis key 前缀。与 Go 的
# "pandora:matchmaker:resolve-context:nonce:" 逐字相同:两个实现共用同一个重放存储,
# 前缀不一致 = 同一份凭证能在两侧各用一次(等于重放防护整个失效)。
RESUME_NONCE_PREFIX = "pandora:matchmaker:resolve-context:nonce:"

# Go 的 sessiongate.MustBuild 内部 Ping 也是 3s。
SESSION_GATE_PING_TIMEOUT_SEC = 3.0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线)而不是 argparse 惯常的 `--conf`:Go 侧 flag 包用单横线,
    而 run_services.ps1 / start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。
    Python 版必须能被**同一条命令行**拉起,否则那些脚本都要改。
    """
    ap = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    ap.add_argument("-conf", dest="conf", default="etc/matchmaker-dev.yaml")
    return ap.parse_args(argv)


class KafkaMatchPusher:
    """`biz.MatchEventPusher` ← `kafkax.KeyOrderedProducer` 的适配层。

    ★ 这一层**不是**纯粹的接口对齐(Go 那边的 kafkaPusher 是),它修一个真实的语义差:
    `push_to_players` 部分失败时**不抛异常**,而是返回 `(sent, last_err)`。
    biz 的 `push_ready_strict` 靠 try/except 判定交付成败 —— 直接把 producer 当
    pusher 用的话,READY 推送失败会被当成**成功**:match 被移出 active ZSET,
    补推循环不再重试,非队长成员永远收不到 READY,而日志上一片正常。
    所以这里把"有错"翻译成异常,让重试驱动照常工作。
    """

    __slots__ = ("_producer",)

    def __init__(self, producer: kafkax.KeyOrderedProducer) -> None:
        self._producer = producer

    async def push_match_progress(
        self, caller_player_id: int, to_player_ids: list[int], payload: bytes
    ) -> int:
        # ★ caller_player_id 恒由调用方(biz)传 0 = 发给所有人**含发起方**:
        # 这是推送原则 3 的**例外**。写成"排除发起方"会让组队非队长成员唯一的
        # READY 通道永久静默丢失。本层原样透传,不在这里替 biz 做决定。
        sent, err = await self._producer.push_to_players(
            caller_player_id, to_player_ids, payload
        )
        if err is not None:
            raise err
        if sent == 0 and to_player_ids:
            raise ConnectionError(
                f"match progress push delivered to 0 of {len(to_player_ids)} players"
            )
        return sent

    async def close(self) -> None:
        await self._producer.close()


def _self_region(cfg: mconf.Config) -> int:
    """本副本的 region 编号 —— 只用于 leader 选举的分片键。

    Python 侧 cell_route 段未建模(mode 非空时在加载配置阶段就拒启,见闸③),
    所以从 `model_extra` 里读。读不到按 0,与 Go 单 Cell 部署的 SelfRegion 零值一致。

    为什么分片键里要有 region:同一 (game_mode, region) 的副本才该竞争同一个 leader。
    少了 region,跨 region 部署会把所有副本挤到一个选举里 —— 另一个 region 的撮合
    直接停摆(它的副本永远选不上),而且没有任何错误日志。
    """
    extra = cfg.model_extra or {}
    section = extra.get("cell_route") or {}
    if not isinstance(section, dict):
        return 0
    try:
        return int(section.get("self_region") or 0)
    except (TypeError, ValueError):
        return 0


async def _main_async(args: argparse.Namespace) -> int:  # noqa: C901, PLR0911, PLR0912, PLR0915 —— 与 Go 同为线性启动闸
    # 1. Logger 先起(后面每一道闸都要靠它发事件)
    logger = plog.setup(SERVICE_NAME)
    logger.info("service_starting", conf=args.conf)

    # ── 闸① -conf 解绝对路径 ─────────────────────────────────────────────
    # 不设这道闸会怎样:相对路径在不同工作目录下解到不同文件,进程照常起来,
    # 加载的却是另一份配置 —— 而日志里只会显示那个相对路径,看不出加载的是谁。
    try:
        conf_path = pathlib.Path(args.conf).resolve()
    except OSError as exc:
        logger.error("abs_conf_path_failed", err=str(exc))
        return 1

    # ── 闸②③ 加载 yaml ──────────────────────────────────────────────────
    try:
        cfg = mconf.Config.load(str(conf_path))
    except pconfig.ConfigLoadError as exc:
        # ★ Go 的 `c.Load()` 覆盖"读文件 + 解析 yaml"两步,两者失败都是
        # config_load_failed。只认 FileNotFoundError 的话,**yaml 语法错会落到下面的
        # config_scan_failed** —— 而那个事件名在 Go 侧的含义是"结构/校验不过",
        # 排障时会往完全错误的方向查(去看模型定义,而不是去看 yaml 有没有写坏)。
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except NotImplementedError as exc:
        # 见模块头「落点差异 1」。方向与 Go 一致(fail-fast),位置比 Go 早。
        logger.error("cellroute_init_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── 闸④ 配置校验 ─────────────────────────────────────────────────────
    # 拒的全是「静默塌缩信任域」或「静默关掉一道闸」的配置(逐条理由见 conf.py):
    # team_addr 留空 = StartMatch 不校验队伍 + 对局结束不复位准备状态
    # (INC-20260813-001 两个根因),而两者都不报错、不打 ERROR,配错了没人会发现。
    try:
        cfg.validate_conf()
    except Exception as exc:  # noqa: BLE001
        logger.error("config_validation_failed", err=str(exc))
        return 1

    # ── 闸⑤ 关卡表(不变量 §9.15)──────────────────────────────────────────
    #
    # dir 非空 = 启动强依赖,加载失败直接退出(fail-closed);未配置则不启用,
    # StartMatch 跳过 map_id 表校验(历史行为)。
    #
    # ★ 兜底默认副本(match.map_id)必须是关卡表里的战斗类关卡,而且注册成**批次级
    # 校验器** —— 启动首载与之后每次热 reload 走同一道门禁。只查启动时的话,
    # 一个坏批次热更后所有走默认 map_id 的请求会全部失败,而热更本身报的是成功。
    ct_store: mcat.Store | None = None
    if cfg.config_table.dir:
        # yaml 里的 config_table.dir 是**相对进程工作目录**的,不是相对配置文件 ——
        # 这是 Go 版的既有契约(run_services.ps1 用 -WorkingDirectory $svcDir 启动)。
        # 刻意不做"找不到就换个基准再试"的兜底:那样会在某些机器上碰巧成功、
        # 某些机器上加载到错误批次,而关卡表是开局形状的唯一权威。
        ct_dir = pathlib.Path(cfg.config_table.dir)
        if not ct_dir.is_absolute():
            ct_dir = (pathlib.Path.cwd() / ct_dir).resolve()
        validator = mcat.default_map_validator(cfg.match.map_id)
        try:
            ct_result = mcat.load_tables(ct_dir)
            validator(ct_result.tables)
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
        ct_store = mcat.Store(ct_result.tables, str(ct_dir))
        ct_store.add_validator(validator)
        logger.info(
            "configtable_loaded",
            dir=str(ct_dir),
            version=ct_result.version,
            levels=ct_result.tables.level_count(),
        )
    else:
        logger.warning(
            "configtable_disabled",
            hint="config_table.dir empty; StartMatch map_id will not be validated "
            "against level table",
        )

    # ── 闸⑥ Redis 端点(强依赖:排队队列 + match 状态的唯一权威)────────────
    # 单实例填 host,Redis Cluster / Sentinel 只填 addrs,两者皆空才算未配置。
    rc = cfg.node.redis_client
    if not rc.host and not rc.addrs:
        logger.error(
            "redis_endpoint_required",
            hint="set node.redis_client.host (single) or node.redis_client.addrs (cluster)",
        )
        return 1

    # ── 闸⑦ Ping ─────────────────────────────────────────────────────────
    # 不探的话服务会带着一个死 Redis 正常 Ready,k8s 把流量切过来后**第一条
    # StartMatch** 才暴露 —— 那时错误已经落在玩家身上了。
    try:
        rdb = await redisx.must_connect(rc)
    except asyncio.CancelledError:
        # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
        # 吞掉之后取消就**不再传播** —— 启动路径上 Ctrl-C / 上层取消会被翻译成某道闸
        # 的失败,报出假的失败原因,让 §9.16 的「先摘流量 → 再排空在途」失效。
        raise
    except BaseException as exc:  # noqa: BLE001
        logger.error(
            "redis_ping_failed", err=str(exc), addr=rc.host, addrs=list(rc.addrs)
        )
        return 1
    logger.info("redis_connected", addr=rc.host, addrs=list(rc.addrs))

    sf_holder = None
    producer: kafkax.KeyOrderedProducer | None = None
    team_reader: mclients.GrpcTeamReader | None = None
    locator: mclients.GrpcLocationNotifier | None = None
    presence: offlinewatch.GrpcPresenceReader | None = None
    ds_allocator: mclients.GrpcDSAllocator | None = None
    try:
        # ── 闸⑧ Snowflake ────────────────────────────────────────────────
        #
        # ticket_id 与 match_id 是两个独立 ID 空间,各取一个发号器(共用同一 nodeID /
        # 同一把 lease)。⚠️ 共用 nodeID ⇒ 两个空间会发出**逐位相同**的 ID,
        # 禁止跨空间放进同一个容器比较。
        #
        # node_id_source=static 时 provide_node 内部还有号段闸(0 是 UE DS 本地发号器
        # 的保留号,> NODE_MASK 会被静默截断成别的副本的号)—— 那两种都只在数据层面
        # 表现为重号,不拦就永远查不出来。
        try:
            ticket_sf, sf_holder = await snowflake_etcd.provide_node(
                list(cfg.snowflake.etcd_endpoints),
                cfg.snowflake.etcd_service_name or SERVICE_NAME,
                cfg.node.node_id,
                cfg.snowflake.node_id_source,
                # 失主 = 独占权不可证明 = 继续发号就是重号,没有安全的降级 → 退出进程。
                on_lost=snowflake_etcd.exit_process_on_lost,
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
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error(
                "snowflake_init_failed",
                err=str(exc),
                node_id=cfg.node.node_id,
                node_id_source=cfg.snowflake.node_id_source or "static",
                hint="etcd 档抢不到 nodeID 时**不得**退回 static —— 那正好会与别的副本重号",
            )
            return 1
        # 第二个发号器复用同一 nodeID —— 与 Go 的 MustProvideSnowflakeN(..., 2) 同语义。
        # ★ 绝不能再抢一个 nodeID:那会让本进程占掉两个号段,而 lease 只守着一个,
        #   另一个失租后不会有人退出进程(静默重号)。
        match_sf = snowflake.Node(ticket_sf.node_id)
        logger.info(
            "snowflake_ready",
            node_id_source=cfg.snowflake.node_id_source or "static",
            spaces=2,
            hint="ticket_id 与 match_id 共用 nodeID,会发出逐位相同的 ID,禁止跨空间比较",
        )

        # ── 闸⑨ team gRPC reader(弱依赖:留空 → 跳过队伍校验)──────────────
        reader = None
        if cfg.match.team_addr:
            # A-13:给 matchmaker→team 的组票 / 复位调用签名。未配密钥则不签,
            # team 侧按其 match_call_auth_require 决定观察放行还是拒
            # (两边可分两次发布配上,§9.21 不靠发布顺序)。
            team_signer: internalrpcauth.Signer | None = None
            if cfg.match.team_call_auth_secret:
                try:
                    team_signer = internalrpcauth.Signer(
                        cfg.match.team_call_auth_secret,
                        SERVICE_NAME,
                        cfg.match.team_call_auth_audience,
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    logger.error("team_call_signer_init_failed", err=str(exc))
                    return 1
            team_reader = mclients.GrpcTeamReader(cfg.match.team_addr, team_signer)
            reader = team_reader
            logger.info("team_reader_ready", team_addr=cfg.match.team_addr)
        else:
            logger.warning(
                "team_addr_empty", hint="StartMatch will skip team validation"
            )

        # ── 闸⑩ Kafka producer → match 进度推送 ───────────────────────────
        #
        # ★ 判据是「有非空 broker」而不是 len(brokers)>0:ConfigMap 渲染出
        # `brokers: [""]` 时 Go 判为未配置(纯 RPC 本地调试模式),按长度判会去建一个
        # 空 broker 列表的 producer —— 要么当场炸,要么更糟:建成功但永远发不出去。
        pusher: KafkaMatchPusher | None = None
        if cfg.kafka.configured():
            try:
                producer = kafkax.KeyOrderedProducer(
                    kafkax.producer_conf_from(cfg.kafka),
                    kafka_topics.TOPIC_MATCH_PROGRESS,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error(
                    "kafka_producer_required_but_unavailable",
                    err=str(exc),
                    topic=kafka_topics.TOPIC_MATCH_PROGRESS,
                    hint="matchmaker exits before Ready so the orchestrator can retry "
                    "after Kafka recovers",
                )
                return 1
            pusher = KafkaMatchPusher(producer)
            logger.info(
                "kafka_producer_ready",
                topic=kafka_topics.TOPIC_MATCH_PROGRESS,
                required=True,
            )
        else:
            logger.warning(
                "kafka_producer_disabled_dev_only",
                reason="kafka.brokers is empty",
                hint="match progress push disabled; only the captain can see READY via "
                "GetMatchProgress polling in this explicit no-Kafka development mode",
            )

        # ── 闸⑪ 会话现行性门(R5 复审 P0-1,INC-20260722-004)────────────────
        # 客户端面请求 jti 必须是 login 会话权威(pandora:sess,node.redis_client 指向的
        # 共享 Redis)当前一代 —— 顶号后旧 JWT 在 exp 之前就失去按 player_id 定向操作的
        # 能力,否则被顶号的那一方还能继续用受害者的身份开局 / 取消排队。
        # prod 生成器机械置 session_gate.require=true(漏配端点拒启)。
        try:
            sess_gate = sessiongate.must_build(rdb, cfg.session_gate.require)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("session_gate_init_failed", err=str(exc))
            return 1

        # ── 闸⑫ DS allocator ──────────────────────────────────────────────
        #
        # battle DSTicket 的签发档由**显式配置**三选一,不做"有钥匙就用 v2"的推断:
        #   ① ds_ticket.signer_enabled()  → RS256 v2(生产/灰度唯一形态)
        #   ② match.ds_local_profile == local-off-v1 → HS256 legacy(Windows 本机联调)
        #   ③ 都没配 → 拒启
        # ①② **互斥**:两者同时配说明部署意图自相矛盾(同机 DS 只认其中一档),
        # 猜任何一边都会产出"签得出、验不过"的票 —— 那时 DS 已经拉起来了。
        if cfg.match.ds_allocator_addr:
            legacy_signer = None
            v2_signer = None
            if cfg.ds_ticket.signer_enabled():
                if cfg.match.ds_local_profile:
                    logger.error(
                        "ds_ticket_profile_conflict",
                        ds_local_profile=cfg.match.ds_local_profile,
                        hint="ds_ticket.private_key_file 与 match.ds_local_profile 互斥,"
                        "二选一",
                    )
                    return 1
                try:
                    v2_signer = pdsticket.new_ds_ticket_signer_from_conf(
                        pdsticket.DSTicketConf(
                            private_key_file=cfg.ds_ticket.private_key_file,
                            active_kid=cfg.ds_ticket.active_kid,
                            # 零值翻 None 交签发器取默认(同 hub_allocator 闸⑤):
                            # 传 0 会被当成"显式要求 0 秒有效期"。
                            ttl=cfg.ds_ticket.ttl_td() or None,
                            jwks_file=cfg.ds_ticket.jwks_file,
                            keyset_revision=cfg.ds_ticket.keyset_revision,
                        )
                    )
                except pdsticket.DSTicketConfigError as exc:
                    logger.error(
                        "ds_ticket_v2_signer_init_failed",
                        err=str(exc),
                        hint="check ds_ticket.private_key_file / active_kid / ttl",
                    )
                    return 1
                logger.info(
                    "ds_ticket_v2_signer_ready",
                    kid=v2_signer.kid(),
                    ttl=godur.duration_string(v2_signer.ttl()),
                )
            elif cfg.match.ds_local_profile == mconf.DS_LOCAL_PROFILE_OFF_V1:
                if len(cfg.jwt.secret) < 32:
                    logger.error(
                        "local_legacy_signer_init_failed",
                        err="jwt.secret must be >= 32 bytes",
                        hint="jwt.secret must be >=32 bytes and match login/envoy",
                    )
                    return 1
                legacy_signer = ldsticket.DSTicketSigner(
                    secret=cfg.jwt.secret,
                    issuer=cfg.jwt.issuer,
                    audience=cfg.jwt.audience,
                    ttl=cfg.jwt.ds_ticket_ttl_td(),
                    additional_secrets=tuple(cfg.jwt.additional_secrets),
                )
                logger.warning(
                    "ds_ticket_local_off_v1_legacy_signer",
                    profile=cfg.match.ds_local_profile,
                    hint="Windows 本机联调专用:HS256 票无实例绑定 / 无灰度粘滞 / "
                    "无 kid 吊销;生产与灰度绝不允许启用",
                )
            else:
                logger.error(
                    "ds_allocator_requires_ds_ticket_v2",
                    ds_allocator_addr=cfg.match.ds_allocator_addr,
                    hint="配置带版本的 ds_ticket.private_key_file + active_kid;"
                    "Windows 本机联调设 match.ds_local_profile=local-off-v1;"
                    "静默回落 legacy 是禁止的",
                )
                return 1
            # 补偿签名器:破坏性 abort RPC 的专用密钥,**不复用**玩家 JWT / DS 回调密钥
            # (那几把钥匙的持有者范围比"能拆一台 DS"宽得多)。
            try:
                abort_auth = internalrpcauth.Signer(
                    cfg.match.allocation_abort_auth_secret,
                    SERVICE_NAME,
                    cfg.match.allocation_abort_auth_audience,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error(
                    "allocation_abort_service_auth_init_failed",
                    err=str(exc),
                    hint="check match.allocation_abort_auth_secret / _audience "
                    "(必须与 ds_allocator 侧一致)",
                )
                return 1
            grpc_allocator = mclients.GrpcDSAllocator(
                cfg.match.ds_allocator_addr,
                legacy_signer,
                v2_signer,
                abort_auth,
                cfg.match.map_id,
                cfg.match.game_mode,
                cfg.match.ds_allocate_timeout_td().total_seconds(),
            )
            grpc_allocator.set_session_gate(sess_gate)
            if ct_store is not None:
                grpc_allocator.set_config_tables(ct_store)
            ds_allocator = grpc_allocator
            allocator = grpc_allocator
            logger.info(
                "ds_allocator_grpc_ready",
                ds_allocator_addr=cfg.match.ds_allocator_addr,
                map_id=cfg.match.map_id,
                game_mode=cfg.match.game_mode,
            )
        else:
            allocator = mclients.StubDSAllocator("")
            logger.warning(
                "ds_allocator_addr_empty",
                hint="using StubDSAllocator (mock ds_addr + mock tickets)",
            )

        # ── 闸⑬ player_locator(弱依赖:留空 → 不上报位置)──────────────────
        # 撮合成局→MATCHING、全员确认就绪→BATTLE(不变量 §1)。
        if cfg.match.locator_addr:
            locator = mclients.GrpcLocationNotifier(cfg.match.locator_addr)
            # StartMatch 在线闸刻意直接用 pkg/offlinewatch 那份 reader:
            # 「玩家离开大厅多久了」全仓只能有一个判定口径,team 自动退队与本闸
            # 读同一份事实 —— 两份口径会让"被 team 摘掉的人还留在匹配票里"。
            presence = offlinewatch.GrpcPresenceReader(cfg.match.locator_addr)
            logger.info(
                "locator_notifier_ready",
                locator_addr=cfg.match.locator_addr,
                start_presence_grace=cfg.match.start_presence_grace,
            )
        else:
            logger.warning(
                "locator_addr_empty",
                hint="match state (MATCHING/BATTLE) will not be reported to "
                "player_locator; StartMatch presence gate disabled",
            )

        # ── 装配链 ────────────────────────────────────────────────────────
        # namespace=game_mode:queue / active / start:active 三个扫描索引按模式分池,
        # 而 player claim 保持全局 —— 落实「一人同一时刻只在一个队列(跨所有模式)」。
        repo = mrepo.RedisMatchRepo(rdb, cfg.match.game_mode)
        uc = mbiz.MatchUsecase(
            repo, reader, pusher, allocator, match_sf, locator, cfg.match
        )
        uc.set_presence_reader(presence)
        if ct_store is not None:
            uc.set_config_tables(ct_store)

        # ── 闸⑭ 进场侧限流(anti-abuse §6 第 2/3/7/8 项)────────────────────
        # StartMatch 冷却 + 成局级冷却 + 容量耗尽静默窗 + no-show 退避执行。
        # 复用共享 rdb;这是**背压不是不变量**,故障时 fail-open(见 entry_limiter.py)。
        uc.set_entry_limiter(mlimiter.RedisEntryLimiter(rdb))
        logger.info(
            "entry_ratelimiter_ready",
            start_match_cooldown=cfg.match.start_match_cooldown,
            match_form_cooldown=cfg.match.match_form_cooldown,
            no_capacity_requeue_delay=cfg.match.no_capacity_requeue_delay,
        )

        # ── 闸⑮⑯ ResolvePlayerMatchContext 的内部验签 ─────────────────────
        #
        # 这个接口按 player_id 读任意玩家的对局上下文(**含 battle 票**)。caller==0
        # 只能证明「不带玩家 JWT」——:8444 没有 jwt_authn,集群内任何 Pod 都满足。
        # 所以必须要 request-bound 签名 + 共享 Redis 里原子消费一次的 nonce。
        try:
            replay = internalrpcauth.RedisReplayStore(rdb, RESUME_NONCE_PREFIX)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("match_resume_replay_store_init_failed", err=str(exc))
            return 1
        try:
            login_resume_auth = internalrpcauth.Verifier(
                cfg.match.match_resume_auth_secret,
                "login",
                cfg.match.match_resume_auth_audience,
                RESUME_AUTH_MAX_CLOCK_SKEW_SEC,
                replay,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("match_resume_service_auth_init_failed", err=str(exc))
            return 1
        verifiers = [login_resume_auth]
        # team 的那把是**可选**的:尚未分发时保持既有 fail-closed 现状(team 侧照旧被拒),
        # 部署照常启动。但**配了却非法**仍是致命的 —— 打错一个字的密钥看上去是
        # 「已启用」,而每一次 team 调用都静默鉴权失败(招募列表恒空、入队恒拒)。
        if cfg.match.team_resume_auth_secret:
            try:
                verifiers.append(
                    internalrpcauth.Verifier(
                        cfg.match.team_resume_auth_secret,
                        "team",
                        cfg.match.match_resume_auth_audience,
                        RESUME_AUTH_MAX_CLOCK_SKEW_SEC,
                        replay,
                    )
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("team_resume_service_auth_init_failed", err=str(exc))
                return 1
        else:
            logger.warning(
                "team_resume_service_auth_disabled",
                hint="match.team_resume_auth_secret is empty: team ListOpenTeams returns "
                "empty and team join is rejected with ERR_UNAVAILABLE",
            )
        try:
            resume_auth = msvc.MultiCallerVerifier(*verifiers)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("match_resume_service_auth_init_failed", err=str(exc))
            return 1

        svc = msvc.MatchService(uc, ticket_sf, resume_auth)

        # ── 闸⑰ gRPC + HTTP ──────────────────────────────────────────────
        #
        # auth_required=False 对应 Go 的 pmw.AuthOptional():ReleaseMatch /
        # ResolvePlayerMatchContext / ReloadConfigTable 是后端内部直连(无 JWT),
        # 用 AuthRequired 会把它们全挡在门外;客户端 RPC 在 service 层逐个做
        # caller==0 兜底拦截。
        try:
            grpc_server = pserver.build_grpc_server(
                cfg.server.grpc,
                auth_required=False,
                extra_interceptors=[
                    sessiongate.SessionCurrentInterceptor(
                        sess_gate, cfg.session_gate.require
                    )
                ],
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("grpc_server_init_failed", err=str(exc))
            return 1
        matchgrpc.add_MatchServiceServicer_to_server(svc, grpc_server)
        reflection_names = [msvc.GRPC_SERVICE_FULL_NAME]
        # 配置表热更入口只在 config_table.dir 配置时注册(与 Go 的条件注册同):
        # 没有 store 的进程注册它只会返回一个必然失败的 RPC。
        if ct_store is not None:
            cfggrpc.add_ConfigTableAdminServiceServicer_to_server(
                msvc.ConfigTableAdminService(ct_store), grpc_server
            )
            reflection_names.append(msvc.CONFIGTABLE_GRPC_SERVICE_FULL_NAME)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(grpc_server, reflection_names)

        http_app = pserver.build_http_app(SERVICE_NAME)

        # ── 闸⑱ 后台撮合循环(单写者)──────────────────────────────────────
        if cfg.match.leader.enabled:
            # 分片键 = game_mode × region:同一 (mode, region) 的副本竞争同一个 leader。
            election = f"{SERVICE_NAME}/{cfg.match.game_mode}/r{_self_region(cfg)}"
            leader_kwargs: dict = {}
            if cfg.match.leader.prefix:
                leader_kwargs["prefix"] = cfg.match.leader.prefix
            if cfg.match.leader.lease_ttl_sec > 0:
                leader_kwargs["lease_ttl_sec"] = cfg.match.leader.lease_ttl_sec

            async def match_loop_leader() -> None:
                """经 etcd 选举跑撮合循环。失主取消循环但**进程不退出** ——
                继续服务 RPC,新 leader 在 lease TTL 内接管(不停机滚动更新)。"""
                try:
                    await etcdleader.run_leader(
                        list(cfg.match.leader.etcd_endpoints),
                        election,
                        uc.run_match_loop,
                        **leader_kwargs,
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    # 与 Go 同:选举本身跑挂只记 ERROR,不拖垮进程 ——
                    # RPC 面(含 GetMatchProgress 兜底轮询)仍然可用。
                    logger.error(
                        "match_leader_run_failed", election=election, err=str(exc)
                    )

            background = [match_loop_leader]
            logger.info(
                "match_loop_leader_gated",
                election=election,
                etcd_endpoints=list(cfg.match.leader.etcd_endpoints),
            )
        else:

            async def match_loop_direct() -> None:
                await uc.run_match_loop()

            background = [match_loop_direct]
            logger.info(
                "match_loop_direct",
                hint="single-replica / leader election disabled",
            )

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                redis_addr=rc.host,
                team_addr=cfg.match.team_addr,
                confirm_timeout=cfg.match.confirm_timeout,
                match_interval=cfg.match.match_interval,
                team_size=cfg.match.team_size,
                walk_in=cfg.match.walk_in,
                auto_confirm_match=cfg.match.auto_confirm_match,
                session_gate_require=cfg.session_gate.require,
                runtime="python",  # 灰度期用它在 Grafana 里区分两个实现
            )
            # 废弃键告警:enable_solo_match 已于 2026-07-25 正名为 walk_in,
            # apply_defaults() 仍兼容读取旧键(漏迁移时保住 PVE 的 walk-in 行为)。
            # 这条 WARN 是 contract 阶段删除旧字段前的**迁移进度信号** ——
            # 线上不再出现它,才说明所有部署的 yaml / ConfigMap 都已改用新键。
            if cfg.match.enable_solo_match:
                logger.warning(
                    "deprecated_config_key",
                    key="match.enable_solo_match",
                    replacement="match.walk_in",
                    hint="旧键仍生效(已并入 walk_in),请迁移配置后再删除",
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
        if producer is not None:
            with contextlib.suppress(Exception):
                await producer.close()
        for closable in (team_reader, locator, presence, ds_allocator):
            if closable is not None:
                with contextlib.suppress(Exception):
                    await closable.close()
        if sf_holder is not None:
            # 主动释放 nodeID:不放的话要等 lease 自然过期才被下一个副本抢到,
            # 滚动更新期间会看到「新 Pod 抢不到号」的假故障。
            with contextlib.suppress(Exception):
                await sf_holder.close()
        with contextlib.suppress(Exception):
            await rdb.aclose()


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
