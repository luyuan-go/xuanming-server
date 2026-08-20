"""Pandora chat 服务入口(Python 版)—— 对应 Go 侧 services/social/chat/cmd/chat/main.go。

职责:五频道聊天(世界 / 队伍 / 私聊 / 公会 / 临时群)。
私聊落 pandora_social(MySQL 强依赖,离线历史);五频道经 kafka
pandora.chat.{world,team,private,guild,group} → push 推送(弱依赖);
队伍 / 公会 / 群成员经 team / guild gRPC 解析(弱依赖,addr 空则该频道降级)。

★ 启动闸逐条对齐 Go(**顺序本身是契约**:事件名是 Loki 告警和运维手册的入口):

    ①  abs_conf_path_failed          -conf 解不成绝对路径              fail-fast
    ②  config_load_failed            yaml 读不到 / 语法错              fail-fast
    ③  config_scan_failed            yaml 结构对不上                    fail-fast
    ④  cellroute_init_failed         cell_route 配了但 Python 只单 Cell fail-fast
    ⑤  chat_retention_mode_invalid   retention_mode 拼错               fail-fast
    ⑥  mysql_dsn_required            私聊历史库 DSN 缺失               fail-fast
    ⑦  mysql_connect_failed          连不上私聊历史库                   fail-fast
    ⑧  mysql_strict_mode_required    sql_mode 非严格                    fail-fast
    ⑨  snowflake_init_failed         static 档 node_id 不合法           fail-fast
    ⑩  snowflake_nodeid_acquire_failed etcd 档抢不到 nodeID             fail-fast
    ⑪  session_gate_redis_failed     require=true 时会话权威 Ping 不通  fail-fast
    ⑫  session_gate_endpoint_required require=true 时漏配会话权威端点   fail-fast

  弱依赖**刻意只 WARN**,不得改成 fail-fast(改了就是把"聊天推送不可用"升级成
  "聊天服务起不来",而私聊照样落库、历史照样能拉):
    kafka_brokers_empty / kafka_producer_init_failed
    team_addr_empty / guild_addr_empty
    world_ratelimit_disabled

  ⚠️ 闸④的**位置**与 Go 不同:Go 在装配完限流器之后才 `etcdtable.WireRouter`,
  Python 侧这道判定挂在 `pandorapy.config.BaseConf` 的 pydantic 校验器上,
  所以在加载配置时就触发。事件名保持 `cellroute_init_failed` 不变 ——
  Loki 上的告警是按事件名建的,位置提前不影响命中,改名才会静默失去覆盖。

后台循环两条,都走 safego(裸 create_task 的协程死掉后进程照跑、health 照答
SERVING、**零日志**):
    - 私聊历史保留期清理(每 sweep_interval 一轮;默认 report_only 只报告不删)
    - 容量巡检(§9.24,启动即一轮拿基线,之后每小时)

运行:
    cd python
    .venv/Scripts/python.exe -m pandorapy.services.chat.main \
        -conf ../services/social/chat/etc/chat-dev.yaml
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
from pandora.chat.v1 import chat_pb2_grpc

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
from pandorapy import snowflake_etcd as psnowflake_etcd
from pandorapy.services.chat import biz as cbiz
from pandorapy.services.chat import budgets as cbudgets
from pandorapy.services.chat import conf as cconf
from pandorapy.services.chat import data as cdata
from pandorapy.services.chat import readers as creaders
from pandorapy.services.chat import service as csvc

SERVICE_NAME = "chat"
HTTP_DEFAULT_PORT = 21005

# 聊天相关表所在的库(pandora_social 由 chat / friend / guild / mail 共用)。
CHAT_DB = "pandora_social"

# 容量巡检周期:启动即跑一轮拿基线(上线时就已超限当场可见),之后每小时一轮 ——
# 与 Go 的 runCapacityGuard(time.NewTicker(time.Hour))同节拍。
# 走 information_schema 估算(毫秒级、不锁表、不扫数据),放启动路径安全;
# 绝不用 COUNT(*)(千万行表几十秒,会拖垮滚动更新)。
CAPACITY_INTERVAL_SEC = 3600.0

# 会话权威 Redis 的启动期 Ping 超时,与 Go 侧 sessiongate.MustBuild 同为 3s。
SESSION_GATE_PING_TIMEOUT_SEC = 3.0

# 启动期 DB 探测超时,对齐 Go 的 context.WithTimeout(5s)。
# 没有超时的话,库半死不活(TCP 通但不回包)时进程会**挂在这里不退** ——
# 表现是 Pod 一直卡在启动中,而 k8s 只看到「未 Ready」,排障的人不知道卡在哪道闸。
DB_PROBE_TIMEOUT_SEC = 5.0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线)而不是 argparse 惯常的 `--conf`:Go 侧 flag 包用单横线,
    而 run_services.ps1 / start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。
    Python 版必须能被**同一条命令行**拉起,否则那些脚本都要改。
    """
    ap = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    ap.add_argument("-conf", dest="conf", default="etc/chat-dev.yaml")
    return ap.parse_args(argv)


class ChatPusher:
    """把 biz.ChatPusher 适配到五个 kafkax.KeyOrderedProducer —— 对应 Go 的 chatPusher。

    kafka key 的口径必须与 Go 逐字相同(consumer 侧按 key 分区消费):
      - 私聊 / 队伍 / 公会 / 群:key = **收件方 player_id**(同一接收方保序);
      - 世界频道:key = **空串**,走 push 服务的 Broadcast 路由给全体。
        ★ 世界频道如果按 player_id 发,就不再是广播而是逐人定向 ——
        500 人在场的一条世界消息会变成 500 条 kafka 写,而且 push 侧的
        Broadcast 分支根本不会被触发,行为静默变形。
    """

    __slots__ = ("_private", "_team", "_world", "_guild", "_group")

    def __init__(
        self,
        private: kafkax.KeyOrderedProducer,
        team: kafkax.KeyOrderedProducer,
        world: kafkax.KeyOrderedProducer,
        guild: kafkax.KeyOrderedProducer,
        group: kafkax.KeyOrderedProducer,
    ) -> None:
        self._private = private
        self._team = team
        self._world = world
        self._guild = guild
        self._group = group

    async def push_private(self, to_player_id: int, evt) -> None:  # noqa: ANN001
        await self._private.send(str(to_player_id), evt)

    async def push_team(self, to_player_id: int, evt) -> None:  # noqa: ANN001
        await self._team.send(str(to_player_id), evt)

    async def push_world(self, evt) -> None:  # noqa: ANN001
        # 世界频道广播:key 空,push 服务侧 Broadcast 路由给全体(见类注释)。
        await self._world.send("", evt)

    async def push_guild(self, to_player_id: int, evt) -> None:  # noqa: ANN001
        await self._guild.send(str(to_player_id), evt)

    async def push_group(self, to_player_id: int, evt) -> None:  # noqa: ANN001
        await self._group.send(str(to_player_id), evt)

    async def close(self) -> None:
        for producer in (self._private, self._team, self._world, self._guild, self._group):
            with contextlib.suppress(Exception):
                await producer.close()


async def _new_chat_pusher(cfg: cconf.Config, logger) -> ChatPusher | None:  # noqa: ANN001
    """初始化五个 producer;**任一失败则关闭已建的并返回 None**(整体降级)。

    ★ 全有或全无是刻意的(与 Go 的 newChatPusher 同):部分成功会让某几个频道
    能推、另几个静默不推 —— 玩家看到的是"公会频道时灵时不灵",而服务侧
    只有一条早已被刷过去的启动 WARN,排查时根本对不上。
    """
    topics = (
        kafka_topics.TOPIC_CHAT_PRIVATE,
        kafka_topics.TOPIC_CHAT_TEAM,
        kafka_topics.TOPIC_CHAT_WORLD,
        kafka_topics.TOPIC_CHAT_GUILD,
        kafka_topics.TOPIC_CHAT_GROUP,
    )
    producer_conf = kafkax.producer_conf_from(cfg.kafka)
    built: list[kafkax.KeyOrderedProducer] = []
    for topic in topics:
        try:
            built.append(kafkax.KeyOrderedProducer(producer_conf, topic))
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except
            # 吞掉,于是启动期被 Ctrl-C 时日志上是一条**假的** kafka 初始化失败。
            # 构造本身是同步的、当前不会被取消 —— 但纪律不留例外:例外一旦存在,
            # 下一个复制这段代码的人就会带着例外走。
            for p in built:
                with contextlib.suppress(Exception):
                    await p.close()
            raise
        except BaseException as exc:  # noqa: BLE001 —— 弱依赖:broker 不通照常启动
            logger.warning("kafka_producer_init_failed", topic=topic, err=str(exc))
            for p in built:
                with contextlib.suppress(Exception):
                    await p.close()
            return None
    logger.info("kafka_producer_ready", topics=list(topics))
    return ChatPusher(*built)


async def _run_capacity_guard(pool, schema: str, interval_sec: float) -> None:  # noqa: ANN001
    """容量巡检(§9.24):**只告警不阻断**。

    容量超限是"要去查的问题",不是"服务不能跑的理由" ——
    拒绝启动会把容量问题升级成可用性事故。
    """
    budgets = cbudgets.budgets()

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

    # ── ②③④ 加载 yaml ────────────────────────────────────────────────────
    try:
        cfg = cconf.Config.load(conf_path)
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except NotImplementedError as exc:
        # ★ cell_route.mode 配了但 Python 侧只实现单 Cell(BaseConf 的 after 校验器抛的)。
        #   对应 Go 的 etcdtable.WireRouter 失败分支,事件名保持一致 ——
        #   继续启动会让所有玩家静默落在单 Cell 上,与配置意图不符。
        logger.error("cellroute_init_failed", err=str(exc))
        return 1
    except Exception as exc:  # noqa: BLE001
        # Go 把"读不到文件"和"结构对不上"分成两个事件名,这里保持同样区分
        # (c.Load() 覆盖读 + 解析两步,都归 config_load_failed;Scan 才是 scan)。
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── ⑤ 保留期清理模式 fail-fast(§9.24)────────────────────────────────
    # 拼错的值(如 "delet" / "true")会静默回落 report_only —— 运维以为开了清理、
    # 实际一行没删,chat_private_messages 继续无界增长且启动期毫无痕迹。
    try:
        cfg.chat.validate_retention_mode()
    except ValueError as exc:
        logger.error(
            "chat_retention_mode_invalid",
            err=str(exc),
            hint='chat.retention_mode 只接受 "report_only"(默认,不删) 或 "delete"',
        )
        return 1

    # ── ⑥ MySQL DSN(强依赖:私聊历史落库不可降级)────────────────────────
    raw_dsn = cfg.node.mysql_client.dsn
    if not raw_dsn:
        logger.error(
            "mysql_dsn_required", hint="node.mysql_client.dsn required (pandora_social)"
        )
        return 1

    # ── ⑦ 连库 ───────────────────────────────────────────────────────────
    conn_cfg = mysqlx.parse_go_dsn(raw_dsn, default_db=CHAT_DB)
    try:
        # ★ 池参数走 mysqlx.pool_kwargs 统一翻译 —— max_open/max_idle/
        # conn_max_lifetime/ping_timeout 四个字段 yaml 里都写了、Go 侧都读,
        # 手写 create_pool 只传连接身份的话它们**配了不生效且不报错**。
        # 最要紧的是 conn_max_lifetime:没有 pool_recycle,长空闲连接撞上 MySQL 的
        # wait_timeout 被服务端断掉,客户端不知道,**下一条业务 SQL 才暴露**。
        #
        # ★ autocommit=True 是想清楚的:与 Go 的 database/sql **默认语义**一致。
        # chat 只有两条写路径:①私聊单条 INSERT(data.save_private 里还显式
        # commit 了一次,autocommit 下是无害的空提交);②保留期清理的
        # `DELETE ... LIMIT`(走 dbguard.sweep_table,**它内部不 commit**)。
        # 建成 False 的话第二条会在连接归池时被回滚 —— 表现是「清理跑了、日志说
        # 删了 N 行、库里一行没少」,而且不报错。chat 没有任何依赖 rowcount 的
        # CAS,不需要 autocommit=False 换来的事务边界。
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
        # Go 侧这里是 mysqlx.MustNewClient 的 panic(它内部 Ping 过)。
        # Python 打成结构化事件后 exit 1 —— 方向相同:连不上库绝不带着起来,
        # 否则 Pod Ready、流量切过来,第一条业务请求才暴露。
        logger.error("mysql_connect_failed", err=str(exc), dsn=mysqlx.mask_dsn(raw_dsn))
        return 1
    logger.info("mysql_connected", dsn=mysqlx.mask_dsn(raw_dsn))

    pusher: ChatPusher | None = None
    team_reader: creaders.GrpcTeamReader | None = None
    guild_reader: creaders.GrpcGuildReader | None = None
    group_reader: creaders.GrpcGroupReader | None = None
    rdb = None
    node_holder = None
    try:
        # ── ⑧ 严格模式断言(§9.24)────────────────────────────────────────
        # 非严格 sql_mode 下超长写入会被 MySQL **静默截断**(err=nil 但数据被砍断),
        # 等于无声的数据损坏 —— 对 chat 的具体表现是玩家的私聊内容被砍掉尾巴,
        # 而 max_content_len 校验明明通过了,谁也想不到去查 sql_mode。
        async with pool.acquire() as conn:
            try:
                await asyncio.wait_for(
                    dbguard.assert_strict_mode(conn), timeout=DB_PROBE_TIMEOUT_SEC
                )
            except asyncio.CancelledError:
                # ★ 取消必须穿透(理由同上)。
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("mysql_strict_mode_required", err=str(exc))
                return 1

        # ── ⑨⑩ Snowflake(message_id 生成)───────────────────────────────
        #
        # node_id_source=""/"static" 用 yaml 的 node.node_id;="etcd" 走 etcd 抢占。
        # 失租**必须退出进程**,不能降级继续发号:此刻另一个副本可能已经抢到同一个
        # nodeID,继续发就是重号 —— message_id 重号会让私聊历史的主键冲突(写失败,
        # 玩家发不出消息),而保留期清理按 message_id 范围删,重号还会让清理误伤。
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
            # static 档的号段闸(拒 node_id=0)在 provide_node 内部,这里只报事件。
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

        # ── kafka 五 producer(**弱依赖**:失败只 WARN,私聊仍照常落库)────
        if cfg.kafka.brokers:
            pusher = await _new_chat_pusher(cfg, logger)
        else:
            logger.warning(
                "kafka_brokers_empty", hint="chat push disabled (private still persisted)"
            )

        # ── team gRPC client(**弱依赖**:addr 空则队伍频道降级)───────────
        if cfg.chat.team_addr:
            team_reader = creaders.GrpcTeamReader(cfg.chat.team_addr)
            logger.info("team_client_ready", team_addr=cfg.chat.team_addr)
        else:
            logger.warning("team_addr_empty", hint="team channel fan-out disabled")

        # ── guild gRPC client(**弱依赖**:addr 空则公会 / 群频道一起降级)──
        # ★ GuildService 与 GroupService **同进程**,共用 cfg.chat.guild_addr。
        # 给 GROUP 单列一个 group_addr 会让它恒为空 → 群频道恒走降级,消息静默
        # 不扇出且不报错(合法档缺陷,只能靠盯字段本身抓)。
        if cfg.chat.guild_addr:
            guild_reader = creaders.GrpcGuildReader(cfg.chat.guild_addr)
            group_reader = creaders.GrpcGroupReader(cfg.chat.guild_addr)
            logger.info("guild_client_ready", guild_addr=cfg.chat.guild_addr)
        else:
            logger.warning("guild_addr_empty", hint="guild / group channel fan-out disabled")

        # ── 装配链 ────────────────────────────────────────────────────────
        repo = cdata.MySQLPrivateRepo(pool, db=conn_cfg["db"])
        uc = cbiz.ChatUsecase(repo, pusher, team_reader, guild_reader, group_reader, cfg.chat)

        # ── 世界 / 非世界频道冷却(压测审核【必修-5】+ anti-abuse §6 第 6 项)──
        #
        # ★ 判据是 `host` 非空,与 Go 逐字相同(main.go 的 `cfg.Node.RedisClient.Host != ""`),
        # **不是** endpoints()。只填 addrs(Sentinel / Cluster 档)时 Go 侧同样不装限流器 ——
        # 这里跟着 Go 走,是因为两栈灰度期同时在线:一侧限流、另一侧不限,
        # 同一个玩家的实际冷却取决于请求落到哪个副本,而两边日志都显示"正常"。
        # 真要支持 addrs-only 必须两栈一起改,不能只在 Python 侧"顺手修好"。
        rc = cfg.node.redis_client
        if rc.host:
            try:
                # Go 这里是 redisx.NewClient(不 Ping)—— 限流是背压手段,
                # Redis 不通时 biz 侧 fail-open 放行,不该因此拒启。
                rdb = redisx.new_universal_client(rc)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                # 建客户端本身失败(地址畸形等)在 Go 侧是 panic;这里保持"限流可降级"
                # 的方向,打 WARN 后继续 —— 但必须留证,否则"没限流"这件事零信号。
                logger.warning("world_ratelimit_disabled", err=str(exc), addr=rc.host)
                rdb = None
            if rdb is not None:
                limiter = cdata.RedisRateLimiter(rdb)
                uc.set_world_rate_limiter(limiter)
                # 非世界频道冷却:同一 limiter 实例,按频道独立占窗(与 Go 一致)。
                uc.set_channel_rate_limiter(limiter)
                logger.info(
                    "world_ratelimit_ready",
                    # Go 打的是 time.Duration.String():3 秒是 "3s",500 毫秒是 "500ms"。
                    cooldown=godur.duration_string(cfg.chat.world_cooldown_td()),
                    non_world_cooldown=godur.duration_string(
                        cfg.chat.non_world_cooldown_td()
                    ),
                )
        else:
            logger.warning(
                "world_ratelimit_disabled",
                hint="node.redis_client.host empty, all channels unthrottled",
                addrs=list(rc.addrs),
            )

        svc = csvc.ChatService(uc, snowflake_node)

        # ── ⑪⑫ 会话现行性门(R5 复审 P0-1,INC-20260722-004)───────────────
        # 校验客户端面请求的 jti == login 会话权威(pandora:sess,node.redis_client
        # 指向的共享 Redis)当前一代 —— 顶号后旧 JWT 在 exp 之前就失去按 player_id
        # 定向操作的能力,否则被顶号的那一方还能继续以受害者身份发言。
        #
        # ★ Ping 的档位与 Go 逐条对齐(pkg/sessiongate.MustBuild):
        #   require=true  端点漏配 → 拒启;Ping 失败 → 拒启
        #   require=false 端点漏配 → gate=None(dev 直连联调);**不 Ping**
        #   把 require=false 也改成 Ping 会让本机无 Redis 时 Go 版起得来、
        #   Python 版起不来 —— 同一份 yaml 两个实现行为分叉,正是要避免的事。
        require_gate = cfg.session_gate.require
        sess_rdb = rdb
        if sess_rdb is None and rc.endpoints():
            # 限流那步没建成(host 空但配了 addrs,或建客户端失败)时,
            # 会话门仍要按自己的档位独立建一次 —— 两者在 Go 侧本来就是两个客户端。
            try:
                if require_gate:
                    sess_rdb = await redisx.must_connect(
                        rc, ping_timeout_sec=SESSION_GATE_PING_TIMEOUT_SEC
                    )
                else:
                    sess_rdb = redisx.new_universal_client(rc)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("session_gate_redis_failed", err=str(exc), require=require_gate)
                return 1
            rdb = sess_rdb
        elif sess_rdb is not None and require_gate:
            # 复用限流那条连接,但 require 档必须**探一次**(Go 的 MustBuild 会 Ping)。
            # 不探的话服务会带着一个死 Redis 正常 Ready,而会话门在 fail-closed 档
            # 下每条客户端请求都要读它 —— 流量切过来时第一条业务请求才暴露。
            try:
                await asyncio.wait_for(
                    sess_rdb.ping(), timeout=SESSION_GATE_PING_TIMEOUT_SEC
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("session_gate_redis_failed", err=str(exc), require=require_gate)
                return 1
        try:
            sess_gate = sessiongate.must_build(sess_rdb, require_gate)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("session_gate_endpoint_required", err=str(exc))
            return 1

        # ── gRPC / HTTP ───────────────────────────────────────────────────
        # auth_required=False 对应 Go 的 pmw.AuthOptional():Envoy jwt_authn 已在路由层
        # require JWT,service 层再做一次 caller_id == 0 拦截兜底(见 service.py)。
        grpc_server = pserver.build_grpc_server(
            cfg.server.grpc,
            auth_required=False,
            extra_interceptors=[
                sessiongate.SessionCurrentInterceptor(sess_gate, require_gate)
            ],
        )
        chat_pb2_grpc.add_ChatServiceServicer_to_server(svc, grpc_server)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(grpc_server, [csvc.GRPC_SERVICE_FULL_NAME])

        http_app = pserver.build_http_app(SERVICE_NAME)

        sweep_interval = cfg.chat.sweep_interval_td().total_seconds()

        async def _sweep_once() -> None:
            await uc.sweep_history(cbiz.now_ms())

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                kafka_brokers=list(cfg.kafka.brokers),
                team_addr=cfg.chat.team_addr,
                guild_addr=cfg.chat.guild_addr,
                max_content_len=cfg.chat.max_content_len,
                retention_mode=cfg.chat.retention_mode_parsed().value,
                session_gate_require=require_gate,
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
                # 私聊历史保留期清理:多副本各自跑,DELETE 幂等无需锁(对齐 mail sweep)。
                ("chat_history_sweep", lambda: safego.loop("chat_history_sweep", sweep_interval, _sweep_once)),
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
        # 关闭顺序与建立顺序相反。每一步都 suppress:停机路径上的次生异常会顶掉
        # 真正的退出原因(实测排查成本极高)。
        if pusher is not None:
            with contextlib.suppress(Exception):
                await pusher.close()
        for reader in (team_reader, guild_reader, group_reader):
            if reader is not None:
                with contextlib.suppress(Exception):
                    await reader.close()
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
        # 对应 Go 侧 app_run_failed。
        plog.get().exception("app_run_failed")
        return 1


if __name__ == "__main__":
    with contextlib.suppress(BrokenPipeError):
        sys.exit(main())
