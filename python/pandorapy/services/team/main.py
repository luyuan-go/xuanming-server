"""Pandora team 服务入口(Python 版)—— 对应 Go 侧
services/matchmaking/team/cmd/team/main.go。

职责:队伍权威(建队 / 邀请 / 申请 / 准备 / 招募列表)+ 与 matchmaker 的组票 roster
fence。Redis 是**强依赖**(队伍状态走 WATCH/MULTI/EXEC 乐观锁,启动期 Ping 失败直接退出)。

★ 启动闸逐条对齐 Go(**顺序本身是契约**:事件名是 Loki 告警与运维手册的入口):

    ①  abs_conf_path_failed                 -conf 解不成绝对路径              fail-fast
    ②  config_load_failed                   yaml 读不到 / 语法坏               fail-fast
    ③  config_scan_failed                   yaml 结构对不上                    fail-fast
    ④  team_join_policy_invalid             join_policy 拼错                   fail-fast
    ⑤  team_offline_leave_config_invalid    开了离线退队却缺依赖地址           fail-fast
    ⑥  redis_endpoint_required              队伍存储端点缺失                   fail-fast
    ⑦  redis_ping_failed                    Redis 不通                         fail-fast
    ⑧  snowflake_init_failed                team_id / invite_id 发号器建不起来 fail-fast
    ⑨  kafka_producer_required_but_unavailable  配了 broker 却建不起 producer  fail-fast
        └ kafka_producer_disabled_dev_only   brokers 显式为空                   **WARN**
    ⑩  match_resume_signer_init_failed      出站签名器建不起来                 fail-fast
        └ match_resume_signer_missing        配了 matchmaker 却没密钥           **WARN**
        └ matchmaker_addr_empty              没配 matchmaker                    **WARN**
    ⑪  offline_watch_init_failed            离线复查骨架建不起来               fail-fast
        └ offline_leave_without_kafka        开了离线退队但没 broker            **WARN**
        └ offline_leave_disabled             功能关闭(留启动痕迹)             INFO
    ⑫  offline_watch_consumer_init_failed   离场事件消费者建不起来             fail-fast
    ⑬  cellroute_init_failed                cell 路由表装配失败                fail-fast
    ⑭  ds_auth_guard_init_failed            DS 回调令牌守卫建不起来            fail-fast
    ⑮  match_call_replay_store_init_failed  验签重放存储建不起来               fail-fast
    ⑯  match_call_verifier_init_failed      入站验签器建不起来                 fail-fast
        └ match_call_verifier_disabled       密钥留空                           **WARN**
    ⑰  session_gate_init_failed             会话现行性门装配失败               fail-fast
    ⑱  grpc_server_init_failed              gRPC server 建不起来               fail-fast
    ⑲  app_run_failed                       跑起来之后异常退出                 fail-fast

  ⚠️ **弱依赖只 WARN 的五条不得改成 fail-fast**(方向错了会把可降级增强升级成
     可用性事故):kafka_producer_disabled_dev_only / matchmaker_addr_empty /
     match_resume_signer_missing / match_call_verifier_disabled /
     offline_leave_without_kafka。反过来,**闸⑨ 配了 broker 却建不起来必须 fail-fast**:
     以 pusher=None 接受 Invite 后静默丢掉被邀请方唯一能看到的通知,比不 Ready 严重。

  ⚠️ 闸⑬ 的落点差异(诚实标注):Go 在装配链末尾调 `etcdtable.WireRouter`,失败 os.Exit。
     Python 侧 `cellroute` 只有静态表与路由算法、缺 BuildRouter 装配,所以
     `pandorapy.config.BaseConf` 的校验器在**加载配置时**就对 `cell_route.mode` 非空
     拒启。事件名仍是 cellroute_init_failed(Loki 告警按事件名建),只是位置比 Go 早。
     单 Cell(当前唯一形态)两边行为完全相同。

后台循环(全部走 pandorapy.safego / server.run:裸 create_task 的协程死掉后进程照跑、
health 照答 SERVING、**零日志**):
    ① offline_watch_sweep       到期复查 + 兜底 roster 提名(offline_leave.enabled 时起)
    ② offline_watch_consumer    kafka pandora.player.presence → 排期(有 broker 时起)
    ③ snowflake 续约            node_id_source=etcd 时由 snowflake_etcd.provide_node 内部
                                拉起,失租即退出进程(交给 k8s 重抢号)
    ④ http /metrics             server.run 内部
    ⑤ gRPC server               server.run 内部

运行:
    cd services/matchmaking/team
    PYTHONPATH=../../../python:../../../python/gen \\
        python -m pandorapy.services.team.main -conf etc/team-dev.yaml
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

from pandora.team.v1 import team_pb2_grpc

from pandorapy import dsauth
from pandorapy import internalrpcauth
from pandorapy import kafka_topics
from pandorapy import kafkax
from pandorapy import log as plog
from pandorapy import offlinewatch
from pandorapy import redisx
from pandorapy import safego
from pandorapy import server as pserver
from pandorapy import sessiongate
from pandorapy import snowflake
from pandorapy import snowflake_etcd
from pandorapy.services.team import biz as tbiz
from pandorapy.services.team import conf as tconf
from pandorapy.services.team import data as tdata
from pandorapy.services.team import match_client as tmatch
from pandorapy.services.team import offline_leave as toffline
from pandorapy.services.team import player_name_client as tplayername
from pandorapy.services.team import player_no_client as tplayerno
from pandorapy.services.team import service as tsvc

SERVICE_NAME = "team"
HTTP_DEFAULT_PORT = 21010
GRPC_SERVICE_FULL_NAME = "pandora.team.v1.TeamService"

# 频率配额窗口固定 1 分钟(少一个可配错的旋钮)。与 Go 的 `Window: time.Minute` 同值。
RATE_QUOTA_WINDOW_SEC = 60.0

# 内部东西向验签的时钟偏差窗。与 Go 的 `30*time.Second` 同值 —— 调大等于把重放窗放宽。
MATCH_CALL_MAX_CLOCK_SKEW_SEC = 30.0

# 验签 nonce 的 Redis key 前缀。与 Go 的 "pandora:team:match-call:nonce:" 逐字相同:
# 两个实现共用同一个重放存储,前缀不一致 = 同一份凭证能在两侧各用一次。
MATCH_CALL_NONCE_PREFIX = "pandora:team:match-call:nonce:"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线):Go 侧 flag 包用的是单横线,而 run_services.ps1 /
    start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。Python 版必须能被**同一条
    命令行**拉起,否则那些脚本都要改。
    """
    ap = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    ap.add_argument("-conf", dest="conf", default="etc/team-dev.yaml")
    return ap.parse_args(argv)


def _producer_conf(kafka: tconf.KafkaConf) -> kafkax.ProducerConf:
    return kafkax.producer_conf_from(kafka)


async def _main_async(args: argparse.Namespace) -> int:  # noqa: C901, PLR0911, PLR0912, PLR0915 —— 与 Go 同为线性启动闸
    # 1. Logger 先起(后面每一道闸都要靠它发事件)
    logger = plog.setup(SERVICE_NAME)
    logger.info("service_starting", conf=args.conf)

    # ── 闸① -conf 解绝对路径 ─────────────────────────────────────────────
    try:
        conf_path = pathlib.Path(args.conf).resolve()
    except OSError as exc:
        logger.error("abs_conf_path_failed", err=str(exc))
        return 1

    # ── 闸②③ 加载 yaml / 闸⑬ cell 路由 ──────────────────────────────────
    try:
        cfg = tconf.Config.load(str(conf_path))
    except pconfig.ConfigLoadError as exc:
        # ★ Go 的 `c.Load()` 覆盖"读文件 + 解析 yaml"两步,两者失败都是
        # config_load_failed。只认 FileNotFoundError 的话,**yaml 语法错会落到下面的
        # config_scan_failed** —— 而那个事件名在 Go 侧的含义是"结构/校验不过",
        # 排障时会往完全错误的方向查(去看模型定义,而不是去看 yaml 有没有写坏)。
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except NotImplementedError as exc:
        # ★ 事件名保持 Go 的 cellroute_init_failed(Loki 告警按它建),位置比 Go 早:
        # Python 的「配了未实现的功能段就拒启」挂在 BaseConf 的 pydantic 校验器上,
        # 加载配置时就抛了。方向与 Go 一致(fail-fast),不会静默按单 Cell 跑。
        logger.error("cellroute_init_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── 闸④ join_policy 拼写 ─────────────────────────────────────────────
    # 拼错一个字母(如 "aproval")若被猜成 open,会让全服队伍对任何人敞开 ——
    # 这是静默的权限放大,必须启动就拒,不能等第一个玩家点"申请"才暴露。
    try:
        cfg.validate_join_policy()
    except ValueError as exc:
        logger.error(
            "team_join_policy_invalid", err=str(exc), value=cfg.team.join_policy
        )
        return 1

    # ── 闸⑤ 离线自动退队的依赖齐备性 ─────────────────────────────────────
    # 开了却缺 locator / matchmaker 地址,功能会静默不生效(或更糟:缺了对局闸门就有
    # 拆掉正在打的队伍的风险)。这类「配了却不生效」的失败模式必须启动就暴露。
    try:
        cfg.validate_offline_leave()
    except ValueError as exc:
        logger.error("team_offline_leave_config_invalid", err=str(exc))
        return 1

    try:
        cfg.validate_player_no_resolver()
    except ValueError as exc:
        logger.error("player_no_resolver_init_failed", err=str(exc), phase="config")
        return 1
    try:
        cfg.validate_player_name_resolver()
    except ValueError as exc:
        logger.error("player_name_resolver_init_failed", err=str(exc), phase="config")
        return 1

    # ── 闸⑥ Redis 端点(强依赖:队伍状态的唯一权威)──────────────────────
    # 单实例填 host,Redis Cluster / Sentinel 只填 addrs,两者皆空才算未配置。
    rc = cfg.node.redis_client
    if not rc.host and not rc.addrs:
        logger.error(
            "redis_endpoint_required",
            hint="set node.redis_client.host (single) or node.redis_client.addrs (cluster)",
        )
        return 1

    # ── 闸⑦ Ping ─────────────────────────────────────────────────────────
    # 不探的话服务会带着一个死 Redis 正常 Ready,k8s 把流量切过来后**第一条建队请求**
    # 才暴露 —— 那时错误已经落在玩家身上了。
    try:
        rdb = await redisx.must_connect(rc)
    except asyncio.CancelledError:
        # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
        # 吞掉之后取消就**不再传播** —— 该停的停不下来:启动路径上 Ctrl-C / 上层取消
        # 会被翻译成某道闸的失败,报出假的失败原因,让 §9.16 的
        # 「先摘流量 → 再排空在途」失效。
        raise
    except BaseException as exc:  # noqa: BLE001
        logger.error(
            "redis_ping_failed", err=str(exc), addr=rc.host, addrs=list(rc.addrs)
        )
        return 1
    logger.info("redis_connected", addr=rc.host, addrs=list(rc.addrs))

    sf_holder = None
    producer: kafkax.KeyOrderedProducer | None = None
    match_cli: tmatch.GrpcMatchClient | None = None
    player_name_resolver: tplayername.GrpcPlayerNameResolver | None = None
    player_no_resolver: tplayerno.GrpcPlayerNoResolver | None = None
    presence_reader: offlinewatch.GrpcPresenceReader | None = None
    consumer = None
    try:
        # ── 闸⑧ Snowflake ────────────────────────────────────────────────
        #
        # team_id 与 invite_id 是两个独立 ID 空间,各取一个发号器(共用同一 nodeID /
        # lease)。⚠️ 共用 nodeID ⇒ 两个空间会发出**逐位相同**的 ID,禁止跨空间放进
        # 同一容器比较。
        #
        # node_id_source=static 时 provide_node 内部还有号段闸(0 是 UE DS 本地发号器的
        # 保留号,> NODE_MASK 会被静默截断成别的副本的号)—— 那两种都只在数据层面表现
        # 为重号,不拦就永远查不出来。
        try:
            team_sf, sf_holder = await snowflake_etcd.provide_node(
                list(cfg.snowflake.etcd_endpoints),
                SERVICE_NAME,
                cfg.node.node_id,
                cfg.snowflake.node_id_source,
                on_lost=snowflake_etcd.exit_process_on_lost,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error(
                "snowflake_init_failed",
                err=str(exc),
                node_id=cfg.node.node_id,
                node_id_source=cfg.snowflake.node_id_source or "static",
            )
            return 1
        # 第二个发号器复用同一 nodeID —— 与 Go 的 MustProvideSnowflakeN(..., 2) 同语义:
        # 两个 Node 共用 nodeID 与**同一把 lease**,只是各走各的序列池。
        # ★ 绝不能再抢一个 nodeID:那会让本进程占掉两个号段,而 lease 只守着一个,
        #   另一个失租后不会有人退出进程(§9 不变量 11 的静默重号形态)。
        invite_sf = snowflake.Node(team_sf.node_id)
        logger.info(
            "snowflake_ready",
            node_id_source=cfg.snowflake.node_id_source or "static",
            spaces=2,
            hint="team_id 与 invite_id 共用 nodeID,会发出逐位相同的 ID,禁止跨空间比较",
        )

        # ── 闸⑨ Kafka producer(队伍推送)─────────────────────────────────
        #
        # kafka.brokers 非空表示启用队伍推送;此时 producer 是启动强依赖。初始化失败
        # 必须在 gRPC server 对外 Ready 前退出,让 Kubernetes 保留旧 Pod 并重试新 Pod,
        # 不能再以 pusher=None 接受 Invite 后静默丢掉被邀请方唯一能看到的通知。
        #
        # ★ 判据是「有非空 broker」而不是 len(brokers)>0:yaml 里写 `brokers: [""]` 时
        # Go 判为未配置(纯 RPC 本地调试模式),按长度判会去建一个连不上的 producer
        # 然后 fail-fast —— 同一份 yaml 两边行为相反。见 conf.KafkaConf.configured。
        pusher = None
        if cfg.kafka.configured():
            try:
                producer = kafkax.KeyOrderedProducer(
                    _producer_conf(cfg.kafka), kafka_topics.TOPIC_TEAM_UPDATE
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error(
                    "kafka_producer_required_but_unavailable",
                    err=str(exc),
                    topic=kafka_topics.TOPIC_TEAM_UPDATE,
                    hint="team service exits before Ready so the orchestrator can retry "
                    "after Kafka recovers",
                )
                return 1
            # KeyOrderedProducer 的 push_to_players 就是 biz 需要的推送面,
            # 不再包一层适配器(Go 那层 kafkaPusher 只是接口对齐,Python 无需)。
            pusher = producer
            logger.info(
                "kafka_producer_ready",
                topic=kafka_topics.TOPIC_TEAM_UPDATE,
                required=True,
            )
        else:
            logger.warning(
                "kafka_producer_disabled_dev_only",
                reason="kafka.brokers is empty",
                hint="Invite only stores tokens in this explicit no-Kafka development "
                "mode; configure kafka.brokers for player-visible invitations",
            )

        # ── 装配链 ────────────────────────────────────────────────────────
        repo = tdata.RedisTeamRepo(rdb)
        uc = tbiz.TeamUsecase(repo, pusher, cfg.team)
        # player_no 只在客户端视图组装时读取 login/account 权威，不进入 Redis。
        # addr 留空是显式降级档；addr 已配但凭据无效则拒启，避免“看似接线、永远为 0”。
        if cfg.team.player_no_resolver_addr:
            try:
                player_no_signer = internalrpcauth.Signer(
                    cfg.team.player_no_resolver_auth_secret,
                    "team",
                    cfg.team.player_no_resolver_auth_audience,
                )
                player_no_resolver = tplayerno.GrpcPlayerNoResolver(
                    cfg.team.player_no_resolver_addr, player_no_signer
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("player_no_resolver_init_failed", err=str(exc))
                return 1
            uc.set_player_no_resolver(player_no_resolver)
            logger.info(
                "player_no_resolver_ready",
                addr=cfg.team.player_no_resolver_addr,
                audience=cfg.team.player_no_resolver_auth_audience,
            )
        else:
            logger.warning(
                "player_no_resolver_disabled",
                hint="TeamMember.player_no will be 0; configure team.player_no_resolver_addr",
            )
        # nickname 只从 player 域权威批量读取，不信任 Team storage 中的旧值。
        if cfg.team.player_name_resolver_addr:
            try:
                player_name_signer = internalrpcauth.Signer(
                    cfg.team.player_name_resolver_auth_secret,
                    "team",
                    cfg.team.player_name_resolver_auth_audience,
                )
                player_name_resolver = tplayername.GrpcPlayerNameResolver(
                    cfg.team.player_name_resolver_addr, player_name_signer
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("player_name_resolver_init_failed", err=str(exc))
                return 1
            uc.set_player_name_resolver(player_name_resolver)
            logger.info(
                "player_name_resolver_ready",
                addr=cfg.team.player_name_resolver_addr,
                audience=cfg.team.player_name_resolver_auth_audience,
                timeout="250ms",
                max_batch=tplayername.MAX_PLAYER_IDS,
            )
        else:
            logger.warning(
                "player_name_resolver_disabled",
                hint=(
                    "TeamMember.nickname will remain empty; configure "
                    "team.player_name_resolver_addr"
                ),
            )
        # 申请/邀请频率配额(anti-abuse §6 第 6 项):复用共享 rdb,窗口固定 1 分钟。
        uc.set_rate_quota(
            redisx.ActionQuota(
                rdb, "team", cfg.team.rate_quota_per_min, RATE_QUOTA_WINDOW_SEC
            )
        )
        logger.info("team_rate_quota_ready", per_min=cfg.team.rate_quota_per_min)

        # ── 闸⑩ matchmaker 联动(弱依赖)─────────────────────────────────
        #
        # matchmaker_addr 留空 → 离队/踢人不撤匹配票据,且入队闸门跳过 —— 没有匹配链路
        # 的部署本就不存在"被对局占住的队伍"。
        if cfg.team.matchmaker_addr:
            # ResolvePlayerMatchContext 在 matchmaker 侧强制验签(caller="team",独立密钥)。
            # 密钥缺失**不阻断启动**:CancelMatch 那半边不需要签名,仍然可用;但入队闸门
            # 会 fail-closed、招募列表会恒空,所以必须打 WARN 而不是静默降级。
            match_signer: internalrpcauth.Signer | None = None
            if cfg.team.match_resume_auth_secret:
                try:
                    match_signer = internalrpcauth.Signer(
                        cfg.team.match_resume_auth_secret,
                        "team",
                        cfg.team.match_resume_auth_audience,
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    logger.error("match_resume_signer_init_failed", err=str(exc))
                    return 1
            else:
                logger.warning(
                    "match_resume_signer_missing",
                    hint="team.match_resume_auth_secret is empty: join gate will reject "
                    "with ERR_UNAVAILABLE and ListOpenTeams will return empty",
                )
            match_cli = tmatch.GrpcMatchClient(cfg.team.matchmaker_addr, match_signer)
            uc.set_match_canceler(match_cli)
            uc.set_match_commitment_reader(match_cli)
            logger.info(
                "match_client_ready",
                matchmaker_addr=cfg.team.matchmaker_addr,
                resume_auth_signed=match_signer is not None,
            )
        else:
            logger.warning(
                "matchmaker_addr_empty",
                hint="leave/kick will not cancel matchmaking tickets; "
                "team join match-gate disabled",
            )

        # ── 闸⑪⑫ 离线成员自动退队 ────────────────────────────────────────
        #
        # 装配三件:locator 只读客户端(判定依据)、offlinewatch 复查骨架(排期 + 到期
        # 回查)、kafka 消费者(离场事件 → 排期)。三者任一装不起来都 fail-fast:
        # 半截接线会让功能看起来在跑却永不触发,排查成本远高于起不来。
        watcher: offlinewatch.Watcher | None = None
        if cfg.team.offline_leave.enabled:
            try:
                presence_reader = offlinewatch.GrpcPresenceReader(cfg.team.locator_addr)
                watcher = offlinewatch.Watcher(
                    rdb,
                    presence_reader,
                    uc,
                    offlinewatch.Options(
                        namespace=SERVICE_NAME,
                        threshold_sec=cfg.team.offline_leave.threshold_td().total_seconds(),
                        interval_sec=cfg.team.offline_leave.check_interval_td().total_seconds(),
                        budget=cfg.team.offline_leave.budget,
                        retry_backoff_sec=cfg.team.offline_leave.check_interval_td().total_seconds(),
                    ),
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("offline_watch_init_failed", err=str(exc))
                return 1
            # 读路径兜底(GetMyTeam 顺手复查),与下面的事件链是两条独立触发源:
            # 事件丢了靠它补,没人看队伍时靠事件。少接一条就会有清不掉的残留。
            uc.set_presence_inspector(watcher)
            # 兜底候选源:整支队伍一起掉线时,事件链(Hub 崩溃没有 Logout)与读路径
            # (没人打开面板)会同时失效,只有主动提名能发现残留。
            watcher.set_roster_source(toffline.TeamRosterSource(repo))

            if not cfg.kafka.brokers:
                # 允许:没有 kafka 时退化成「纯兜底」——玩家打开面板才复查。
                # 这不是半成品,是明确的降级档;但必须让运维看见,否则会误以为有秒级时效。
                logger.warning(
                    "offline_leave_without_kafka",
                    hint="no kafka.brokers: offline members are only reaped when someone "
                    "reads the team; configure kafka + locator.departure_event.enabled "
                    "for timely removal",
                )
            else:
                try:
                    consumer = watcher.new_consumer(
                        list(cfg.kafka.brokers),
                        cfg.team.offline_leave.kafka_partitions,
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    logger.error("offline_watch_consumer_init_failed", err=str(exc))
                    return 1
            logger.info(
                "offline_leave_enabled",
                locator_addr=cfg.team.locator_addr,
                threshold=cfg.team.offline_leave.threshold,
                check_interval=cfg.team.offline_leave.check_interval,
                budget=cfg.team.offline_leave.budget,
            )
        else:
            # 关闭态也要留启动痕迹(与 offline_leave_without_kafka 同精神):否则线上查
            # 「离线队员为什么不被摘」时,日志里没有任何「本链路未启用」的证据,
            # 排查会在 locator/offlinewatch 侧空转。
            logger.info(
                "offline_leave_disabled",
                hint="team.offline_leave.enabled=false: 离线成员不会被自动摘出队伍",
            )

        svc = tsvc.TeamService(uc, team_sf, invite_sf)

        # ── 闸⑭ DS 回调令牌守卫 ──────────────────────────────────────────
        # GetPlayerTeam 经 :8444 进来的调用须带 DS 服务令牌。
        # mode=off(默认)→ guard 为 None,check 直接放行,与接线前完全一致。
        try:
            ds_guard = dsauth.guard_from_conf(cfg.ds_auth)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("ds_auth_guard_init_failed", err=str(exc))
            return 1
        svc.set_ds_callback_guard(ds_guard)

        # ── 闸⑮⑯ matchmaker → team 的组票 / 复位调用验签(A-13)──────────
        #
        # 三档可降级:留空=不验;配了+require=false 观察期(验不过只 WARN);
        # require=true 强制。上线顺序「两边配密钥 → 观察 → 翻 require」,每一步单独
        # 都安全,不存在「谁必须先上线」(§9.21)。
        if cfg.team.match_call_auth_secret:
            try:
                replay = internalrpcauth.RedisReplayStore(rdb, MATCH_CALL_NONCE_PREFIX)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("match_call_replay_store_init_failed", err=str(exc))
                return 1
            try:
                verifier = internalrpcauth.Verifier(
                    cfg.team.match_call_auth_secret,
                    "matchmaker",
                    cfg.team.match_call_auth_audience,
                    MATCH_CALL_MAX_CLOCK_SKEW_SEC,
                    replay,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("match_call_verifier_init_failed", err=str(exc))
                return 1
            svc.set_match_call_auth(verifier, cfg.team.match_call_auth_require)
            logger.info(
                "match_call_verifier_ready", require=cfg.team.match_call_auth_require
            )
        else:
            logger.warning(
                "match_call_verifier_disabled",
                hint="team.match_call_auth_secret 留空:BeginTeamMatch/EndTeamMatch "
                "只有 systemOnly,集群内任何 Pod 都可调",
            )
        if ds_guard is not None:
            logger.info("ds_callback_guard_ready", mode=ds_guard.mode.value)

        # ── 闸⑰ 会话现行性门(R5 复审 P0-1,INC-20260722-004)──────────────
        # 客户端面请求 jti 必须是 login 会话权威(pandora:sess,node.redis_client 指向的
        # 共享 Redis)当前一代;prod 生成器机械置 session_gate.require=true(漏配端点拒启)。
        try:
            sess_gate = sessiongate.must_build(rdb, cfg.session_gate.require)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("session_gate_init_failed", err=str(exc))
            return 1

        # ── 闸⑱ gRPC + HTTP ──────────────────────────────────────────────
        #
        # auth_required=False:与 Go 的 pmw.AuthOptional() 同档 —— 身份从 Envoy 注入的
        # x-pandora-player-id 读出后注入上下文,service 层再逐个做 caller==0 兜底拦截。
        # 挂 AuthRequired 会把 GetPlayerTeam / BeginTeamMatch / EndTeamMatch 这三个
        # **必须** caller==0 的东西向方法全部拒掉。
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
        team_pb2_grpc.add_TeamServiceServicer_to_server(svc, grpc_server)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(grpc_server, [GRPC_SERVICE_FULL_NAME])

        http_app = pserver.build_http_app(SERVICE_NAME)

        # ── 后台循环 ①② ──────────────────────────────────────────────────
        background: list = []
        if watcher is not None:
            w = watcher

            async def offline_watch_sweep() -> None:
                """到期复查 + 兜底 roster 提名。

                走 safego.loop:单轮抛异常时只跳过本轮并留日志,下一拍继续 ——
                裸 create_task 的话这条循环死掉后进程照跑、health 照答 SERVING、零日志,
                表现是「离线队员永远不被摘」而没有任何报错。
                """
                await safego.loop(
                    "offline_watch_sweep", w.options.interval_sec, w.sweep
                )

            background.append(offline_watch_sweep)

        if consumer is not None:
            kc = consumer

            async def offline_watch_consumer() -> None:
                """kafka pandora.player.presence → 排期。

                没有它离线摘人只剩「有人打开组队面板才复查」的兜底档:成员最终会被摘,
                但延迟远超 threshold,与 offline_leave 的时效口径不符。
                """
                await kc.run()

            background.append(offline_watch_consumer)

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                redis_addr=rc.host,
                invite_ttl=cfg.team.invite_ttl,
                max_members=cfg.team.max_members,
                join_policy=cfg.team.join_policy,
                invite_push_mode=cfg.team.invite_push_mode,
                offline_leave=cfg.team.offline_leave.enabled,
                matchmaker_addr=cfg.team.matchmaker_addr,
                kafka_topic=(
                    kafka_topics.TOPIC_TEAM_UPDATE if producer is not None else ""
                ),
                ds_auth_mode=cfg.ds_auth.mode or "off",
                session_gate_require=cfg.session_gate.require,
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
        # 关闭顺序与建立顺序相反。每一步都 suppress:停机路径上的次生异常会顶掉真正的
        # 退出原因(实测排查成本极高)。
        if consumer is not None:
            with contextlib.suppress(Exception):
                consumer.stop()
        if producer is not None:
            with contextlib.suppress(Exception):
                await producer.close()
        for closable in (
            player_name_resolver,
            player_no_resolver,
            match_cli,
            presence_reader,
        ):
            if closable is not None:
                with contextlib.suppress(Exception):
                    await closable.close()
        if sf_holder is not None:
            # 主动释放 nodeID:不放的话它要等 lease 自然过期才被下一个副本抢到,
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
        plog.get().exception("app_run_failed")
        return 1


if __name__ == "__main__":
    with contextlib.suppress(BrokenPipeError):
        sys.exit(main())
