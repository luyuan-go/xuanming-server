"""Pandora guild 服务入口(Python 版)—— 对应 Go 侧 services/social/guild/cmd/guild/main.go。

一个进程注册**两套 RPC**:GuildService(公会,14)+ GroupService(临时群,9)= 23 RPC。

装配链与 Go 逐段对齐:
    logger → yaml → 保留期模式校验 → MySQL(强依赖)→ 严格模式 → schema 契约
    → Snowflake ×3 → Redis(弱)→ kafka(弱)→ 装配 → 频率配额 → DS 令牌守卫
    → 会话现行性门 → gRPC/HTTP → 后台循环 → 阻塞运行

★ 启动闸(逐条对应 Go 的 os.Exit / Must*,**事件名逐字相同** —— Loki 上按事件名建的
  告警对不上就是静默失去覆盖;方向也相同,Go 是 warn 的这里绝不能改成 fail-fast):

    ①  abs_conf_path_failed        -conf 路径解析不了
    ②  config_load_failed          yaml 读不到
    ③  config_scan_failed          yaml 结构对不上
    ④  guild_retention_mode_invalid retention_mode 拼错 → 静默回落 report_only,
                                    运维以为开了清理、实际一行没删(§9.24 fail-fast)
    ⑤  mysql_dsn_required          MySQL 是**强依赖**:公会 / 群关系落库不可降级
    ⑥  mysql_connect_failed        Go 是 mysqlx.MustNewClient 的 panic(它内部 Ping 过);
                                   方向相同 —— 连不上库绝不带着起来
    ⑦  mysql_strict_mode_required  非严格 sql_mode 下超长写入被**静默截断**
                                   (err=nil 而数据被砍断)= 无声的数据损坏
    ⑧  guild_schema_incompatible   计数列 / 计数表的物理契约不对 → 上限闸会静默失效
    ⑨  snowflake_init_failed /
        snowflake_nodeid_acquire_failed
                                   static 号段闸 + etcd 抢占;etcd 抢不到号
                                   **不得**退回 static —— 那正好会与别的副本重号
    ⑩  ds_auth_guard_init_failed   mode!=off 却没配 secret / 模式拼错 → 拒启,
                                   绝不静默当 off(fail-closed 的门不能悄悄变 fail-open)
    ⑪  session_gate_redis_failed /
        session_gate_endpoint_required
                                   Go 是 sessiongate.MustBuild 的 panic:
                                   require=true 时端点漏配 / Ping 失败一律拒启
    ⑫  app_run_failed              运行期异常退出

  **弱依赖一律 warn 放行**(与 Go 同方向,改成拒启就是把降级设计变成可用性事故):
    redis_ping_failed / redis_endpoint_empty   → 公会读缓存关闭,直连 MySQL
    kafka_producer_init_failed / kafka_brokers_empty → 公会事件推送静默丢弃
    guild_rate_quota_disabled                  → 申请频率配额不限(总量闸仍在)

后台循环两条,都走 safego(单轮异常只丢本轮,不静默弄死循环):
    - 终态入会申请保留期清理(每 sweep_interval 一轮)
    - 容量巡检(启动即一轮拿基线,之后每小时;超预算只告警不阻断)

运行:
    cd python
    .venv/Scripts/python.exe -m pandorapy.services.guild.main \
        -conf ../services/social/guild/etc/guild-dev.yaml
"""

from __future__ import annotations

# ★ 必须最先 import:Windows 控制台默认 cp1252,日志里的中文会抛 UnicodeEncodeError
# 并把真正的启动错误顶掉。见 pandorapy/_utf8.py。
from pandorapy import _utf8  # noqa: F401  isort:skip
from pandorapy import config as pconfig

import argparse
import asyncio
import contextlib
import pathlib
import sys

import asyncmy
from pandora.group.v1 import group_pb2_grpc
from pandora.guild.v1 import guild_pb2, guild_pb2_grpc

from pandorapy import dbguard, godur, kafka_topics, kafkax, mysqlx, redisx, safego, sessiongate
from pandorapy import log as plog
from pandorapy import server as pserver
from pandorapy import snowflake as psnowflake
from pandorapy import snowflake_etcd as psnowflake_etcd
from pandorapy.services.guild import biz as gbiz
from pandorapy.services.guild import budgets as gbudgets
from pandorapy.services.guild import cache as gcache
from pandorapy.services.guild import conf as gconf
from pandorapy.services.guild import ds_guard as gds
from pandorapy.services.guild import group_repo as ggrouprepo
from pandorapy.services.guild import guild_repo as gguildrepo
from pandorapy.services.guild import schema as gschema
from pandorapy.services.guild import service as gsvc

SERVICE_NAME = "guild"
HTTP_DEFAULT_PORT = 21008
GRPC_SERVICE_FULL_NAMES = (
    "pandora.guild.v1.GuildService",
    "pandora.group.v1.GroupService",
)

# 公会 / 群表所在的库(pandora_social 由 chat/friend/guild/mail 共用)。
GUILD_DB = "pandora_social"

# 容量巡检周期:启动即跑一轮拿基线(上线时就已超限当场可见),之后每小时一轮。
# 走 information_schema 估算(毫秒级、不锁表、不扫数据);绝不用 COUNT(*)。
CAPACITY_INTERVAL_SEC = 3600.0

# schema 契约闸的超时,与 Go 的 5s context 同值。
SCHEMA_CHECK_TIMEOUT_SEC = 5.0

# 弱依赖 Redis 的启动期 Ping 超时,与 Go 的 3s 同值。
REDIS_PING_TIMEOUT_SEC = 3.0
# 会话权威 Redis 的启动期 Ping 超时,与 Go 的 sessiongate.MustBuild 同为 3s。
SESSION_GATE_PING_TIMEOUT_SEC = 3.0

# 频率配额窗口固定 1 分钟(与 Go 的 `Window: time.Minute` 同)。
RATE_QUOTA_WINDOW_SEC = 60.0
# 配额 key 的域名前缀,与 Go 的 `Domain: "guild"` 同 —— 改了等于换了一批 Redis key,
# 滚动窗口里新旧副本各限各的,上限直接翻倍。
RATE_QUOTA_DOMAIN = "guild"

# 三个独立 ID 空间:guild_id / request_id / group_id。
SNOWFLAKE_SPACE_COUNT = 3


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线)而不是 argparse 惯常的 `--conf`:Go 侧 flag 包用单横线,
    而 run_services.ps1 / start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。
    Python 版必须能被**同一条命令行**拉起,否则那些脚本都要改。
    """
    ap = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    ap.add_argument("-conf", dest="conf", default="etc/guild-dev.yaml")
    return ap.parse_args(argv)


class KafkaGuildEventPusher:
    """把公会事件发到 kafka —— 对应 Go 的 `guildEventPusher`。

    kafka key = to_player_id(§9.9 不变量:同接收方事件保序;
    push 服务按 key 路由到该玩家的 stream)。key 换成 guild_id 会让同一个玩家的
    「被踢」和「公会解散」跨 partition,到达顺序不再确定。
    """

    __slots__ = ("_producer",)

    def __init__(self, producer: kafkax.KeyOrderedProducer) -> None:
        self._producer = producer

    async def push_guild_event(self, to_player_id: int, evt: guild_pb2.GuildEvent) -> None:
        await self._producer.send(str(to_player_id), evt)


class RedisActionRateQuota:
    """把 `redisx.ActionQuota` 适配到 biz 的 `ActionRateQuota` 协议。"""

    __slots__ = ("_quota",)

    def __init__(self, quota) -> None:  # noqa: ANN001
        self._quota = quota

    async def allow(self, action: str, subject: int) -> tuple[bool, Exception | None]:
        # 透传 `(ok, exc)`:适配器不能在这里把故障吞掉,否则 biz 侧的
        # guild_rate_quota_check_failed 又变回不可达。
        return await self._quota.allow(action, subject)


async def _run_capacity_guard(pool, schema: str, interval_sec: float) -> None:
    """容量巡检(§9.24):**只告警不阻断**。

    容量超限是"要去查的问题",不是"服务不能跑的理由" ——
    拒绝启动会把容量问题升级成可用性事故。
    """
    table_budgets = gbudgets.budgets()

    async def _once() -> None:
        async with pool.acquire() as conn:
            result = await dbguard.check_budgets(conn, schema, table_budgets)
        dbguard.log_violations(result, db=schema)

    # 启动即一轮拿基线,再进周期循环(与 Go 的 runCapacityGuard 同序)。
    await safego.run_once("db_capacity_guard_initial", _once)
    await safego.loop("db_capacity_guard", interval_sec, _once)


async def _main_async(args: argparse.Namespace) -> int:  # noqa: C901 —— 与 Go 同为线性启动闸
    logger = plog.setup(SERVICE_NAME)
    logger.info("service_starting", conf=args.conf)

    # ── ①②③ 配置 ────────────────────────────────────────────────────────
    try:
        conf_path = pathlib.Path(args.conf).resolve()
    except OSError as exc:
        logger.error("abs_conf_path_failed", err=str(exc))
        return 1
    try:
        cfg = gconf.Config.load(str(conf_path))
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        # Go 把"读不到文件"和"结构对不上"分成两个事件名,这里保持同样区分。
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── ④ 保留期清理模式(§9.24 fail-fast)────────────────────────────────
    # 拼错的值会静默回落 report_only:运维以为开了清理、实际一行没删,
    # 库继续无界增长且启动期毫无痕迹。
    try:
        cfg.guild.validate_retention_mode()
    except ValueError as exc:
        logger.error(
            "guild_retention_mode_invalid",
            err=str(exc),
            hint='guild.retention_mode 只接受 "report_only"(默认,不删) 或 "delete"',
        )
        return 1

    # ── ⑤ MySQL 强依赖(pandora_social)──────────────────────────────────
    raw_dsn = cfg.node.mysql_client.dsn
    if not raw_dsn:
        logger.error(
            "mysql_dsn_required", hint="node.mysql_client.dsn required (pandora_social)"
        )
        return 1

    conn_cfg = mysqlx.parse_go_dsn(raw_dsn, default_db=GUILD_DB)
    try:
        # ★ 池参数走 mysqlx.pool_kwargs 统一翻译 —— max_open/max_idle/
        # conn_max_lifetime/ping_timeout 四个字段 yaml 里都写了、Go 侧都读,
        # 手写 create_pool 只传连接身份的话它们**配了不生效且不报错**。
        #
        # ★ autocommit=True 是刻意的,与 Go 的 database/sql **默认语义**一致:
        # 本服务的复合一致性操作(建会 / 审批 / 踢人 / 解散 / 转让 / 建群 …)
        # 全部由 repo 显式 `begin()` 包起来并在其中判 rowcount;
        # 只读路径是单条语句,不需要额外的 commit/rollback 收尾。
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
        # ⑥ Go 这里是 mysqlx.MustNewClient 的 panic(它内部 Ping 过)。
        logger.error("mysql_connect_failed", err=str(exc), dsn=mysqlx.mask_dsn(raw_dsn))
        return 1

    rdb = None
    session_rdb = None
    producer: kafkax.KeyOrderedProducer | None = None
    node_holder = None
    try:
        # ── ⑦ 严格模式断言(§9.24)────────────────────────────────────────
        # 非严格 sql_mode 下超长写入会被 MySQL **静默截断**(err=nil 但数据被砍断),
        # 等于无声的数据损坏 —— 公会名被截断后 uk_name 的唯一性判定就落在截断值上。
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

        # ── ⑧ schema 契约闸 ─────────────────────────────────────────────
        # 计数列 / 计数表的物理契约(类型 / signedness / NULL / default / 主键)不对时,
        # §9.18 的上限闸会**静默失效**而每一步 SQL 都成功(详见 schema.py 模块头)。
        try:
            async with pool.acquire() as conn:
                await asyncio.wait_for(
                    gschema.validate_required_schema(conn), timeout=SCHEMA_CHECK_TIMEOUT_SEC
                )
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
            # 吞掉之后取消就**不再传播** —— 该停的停不下来:
            #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
            #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
            # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error(
                "guild_schema_incompatible",
                err=str(exc),
                required_migration_version=gschema.REQUIRED_SCHEMA_VERSION,
                hint="先运行 tools/migrate 将 pandora_social 升至 version 2,再滚动 guild",
            )
            return 1
        logger.info("mysql_connected", dsn=mysqlx.mask_dsn(raw_dsn))

        # ── ⑨ Snowflake ×3 ─────────────────────────────────────────────
        # guild_id / request_id / group_id 是三个独立 ID 空间,各取一个发号器
        # (共用同一 nodeID / lease)。
        # ⚠️ 共用 nodeID ⇒ 三个空间会发出**逐位相同**的 ID,禁止跨空间放进同一容器比较。
        try:
            first_node, node_holder = await psnowflake_etcd.provide_node(
                list(cfg.snowflake.etcd_endpoints),
                cfg.snowflake.etcd_service_name or SERVICE_NAME,
                cfg.node.node_id,
                cfg.snowflake.node_id_source,
                # 失主 = 独占权不可证明 = 继续发号就是重号,没有安全的降级 → 退出进程。
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
            logger.info(
                "snowflake_nodeid_acquired", node_id=node_holder.node_id, source="etcd"
            )
        # 对应 Go 的 buildNodes(nodeID, n):同一个 nodeID 造 N 个各自持 step 池的节点。
        # 各空间各走各的 step,不会因为共用一个 Node 而把三条写路径挤在同一个每秒 32768。
        guild_sf = first_node
        request_sf = psnowflake.Node(first_node.node_id)
        group_sf = psnowflake.Node(first_node.node_id)
        assert SNOWFLAKE_SPACE_COUNT == 3  # 空间数变了这里要跟着加节点

        # ── Redis(**弱依赖**:公会资料读缓存 + 申请频率配额)────────────────
        # Ping 失败 → 降级直连 MySQL(cache=None)、配额不限。
        # 单实例填 host,Cluster / Sentinel 只填 addrs,两者皆空才算未配置。
        guild_cache = None
        quota_rdb = None
        rc = cfg.node.redis_client
        if rc.endpoints():
            try:
                rdb = await redisx.must_connect(rc, ping_timeout_sec=REDIS_PING_TIMEOUT_SEC)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                # ★ 方向必须与 Go 一致:**warn 放行**,不是拒启。
                #   缓存是显式弱依赖,拒启会把"少一层缓存"升级成整服不可用。
                logger.warning(
                    "redis_ping_failed",
                    err=str(exc),
                    addr=rc.host,
                    addrs=list(rc.addrs),
                    hint="degrade to direct MySQL (no guild cache)",
                )
                rdb = None
            else:
                guild_cache = gcache.RedisGuildCache(rdb)
                quota_rdb = rdb
                logger.info(
                    "redis_connected",
                    addr=rc.host,
                    addrs=list(rc.addrs),
                    cache_ttl=godur.duration_string(cfg.guild.cache_ttl_td()),
                )
        else:
            logger.warning(
                "redis_endpoint_empty", hint="guild cache disabled (direct MySQL)"
            )

        # ── kafka producer(**弱依赖**:broker 不通则 warn 并继续,推送静默 fail)──
        pusher = None
        if cfg.kafka.brokers:
            try:
                producer = kafkax.KeyOrderedProducer(
                    kafkax.producer_conf_from(cfg.kafka),
                    kafka_topics.TOPIC_GUILD_EVENT,
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
                    "kafka_producer_init_failed",
                    err=str(exc),
                    hint="guild push silently dropped until kafka is available",
                )
                producer = None
            else:
                pusher = KafkaGuildEventPusher(producer)
                logger.info("kafka_producer_ready", topic=kafka_topics.TOPIC_GUILD_EVENT)
        else:
            logger.warning("kafka_brokers_empty", hint="guild push disabled")

        # ── 装配链(公会 + 临时群同进程)──────────────────────────────────
        # schema 名给保留期清理的全限定 DELETE 用;取 DSN 里的库名(生产 = pandora_social)。
        guild_repo = gguildrepo.MySQLGuildRepo(pool, conn_cfg.get("db") or GUILD_DB)
        group_repo = ggrouprepo.MySQLGroupRepo(pool)
        guild_uc = gbiz.GuildUsecase(guild_repo, guild_cache, pusher, cfg.guild)

        # 入会申请频率配额(anti-abuse §6 第 6 项):Redis 健康时启用,否则不限。
        # ★ 这是 fail-open 边界,且**刻意如此**:总量闸(每公会 pending 200)在事务里,
        #   不会因为这里放行而被突破;而 Redis 一抖就把所有人的申请拒掉更糟。
        if quota_rdb is not None:
            guild_uc.set_rate_quota(
                RedisActionRateQuota(
                    redisx.ActionQuota(
                        quota_rdb,
                        RATE_QUOTA_DOMAIN,
                        cfg.guild.rate_quota_per_min,
                        RATE_QUOTA_WINDOW_SEC,
                    )
                )
            )
            logger.info("guild_rate_quota_ready", per_min=cfg.guild.rate_quota_per_min)
        else:
            logger.warning("guild_rate_quota_disabled", reason="redis not available")

        group_uc = gbiz.GroupUsecase(group_repo, cfg.guild)
        guild_svc = gsvc.GuildService(guild_uc, guild_sf, request_sf)

        # ── ⑩ DS 回调令牌守卫 ────────────────────────────────────────────
        # GetPlayerGuild 经 :8444 进来的调用须带 DS 服务令牌。
        # mode=off(默认)→ 守卫为 None,Check 直接放行,与接线前完全一致。
        try:
            ds_guard = gds.new_from_conf(cfg.ds_auth)
        except ValueError as exc:
            logger.error("ds_auth_guard_init_failed", err=str(exc))
            return 1
        guild_svc.set_ds_callback_guard(ds_guard)
        if ds_guard is not None:
            logger.info("ds_callback_guard_ready", mode=ds_guard.mode)

        group_svc = gsvc.GroupService(group_uc, group_sf)

        # ── ⑪ 会话现行性门(R5 复审 P0-1,INC-20260722-004)─────────────────
        # 校验客户端面请求的 jti == login 会话权威(pandora:sess,node.redis_client
        # 指向的共享 Redis)当前一代 —— 顶号后旧 JWT 在 exp 之前就失去按 player_id
        # 定向操作的能力,否则被顶号的那一方还能继续解散 / 转让受害者的公会。
        #
        # ★ Ping 的档位与 Go 逐条对齐(pkg/sessiongate.MustBuild):
        #   require=true  端点漏配 → 拒启;Ping 失败 → 拒启
        #   require=false 端点漏配 → gate=None(dev 直连联调);**不 Ping**
        #   把 require=false 也改成 Ping 会让本机无 Redis 时 Go 版起得来、
        #   Python 版起不来 —— 同一份 yaml 两个实现行为分叉,正是要避免的事。
        #
        # ★ 与上面公会缓存那条**各建各的客户端**(Go 也是两个):缓存那条 Ping 失败要
        #   降级放行,会话权威这条 require=true 时 Ping 失败必须拒启 —— 共用一个
        #   连接就无法让两条闸有不同方向。
        require_gate = cfg.session_gate.require
        if rc.endpoints():
            try:
                if require_gate:
                    session_rdb = await redisx.must_connect(
                        rc, ping_timeout_sec=SESSION_GATE_PING_TIMEOUT_SEC
                    )
                else:
                    session_rdb = redisx.new_universal_client(rc)
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
            sess_gate = sessiongate.must_build(session_rdb, require_gate)
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

        # ── gRPC / HTTP ───────────────────────────────────────────────────
        # auth_required=False 对应 Go 的 pmw.AuthOptional():Envoy 的 jwt_authn 已在
        # 路由层 require JWT,service 层再做一次 caller_id==0 兜底;而 GetPlayerGuild
        # 是内部接口,本就不带玩家身份(强制 auth 会把它整条拒掉)。
        #
        # SessionCurrent 挂在同一条 unary 链上 —— GuildService 与 GroupService
        # 共用,一次接线双服务生效(与 Go 的 NewGRPCServer 同)。
        grpc_server = pserver.build_grpc_server(
            cfg.server.grpc,
            auth_required=False,
            extra_interceptors=[
                sessiongate.SessionCurrentInterceptor(sess_gate, require_gate)
            ],
        )
        guild_pb2_grpc.add_GuildServiceServicer_to_server(guild_svc, grpc_server)
        group_pb2_grpc.add_GroupServiceServicer_to_server(group_svc, grpc_server)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(grpc_server, list(GRPC_SERVICE_FULL_NAMES))

        http_app = pserver.build_http_app(SERVICE_NAME)

        sweep_interval = cfg.guild.sweep_interval_td().total_seconds()

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                kafka_brokers=list(cfg.kafka.brokers),
                cache_enabled=guild_cache is not None,
                max_guild_members=cfg.guild.max_guild_members,
                max_group_members=cfg.guild.max_group_members,
                max_pending_requests_per_guild=cfg.guild.max_pending_requests_per_guild,
                max_groups_per_player=cfg.guild.max_groups_per_player,
                # Go 打的是 time.Duration.String(),5 分钟是 "5m0s" 而不是 yaml 里的 "5m"。
                sweep_interval=godur.duration_string(cfg.guild.sweep_interval_td()),
                retention_mode=cfg.guild.retention_mode_parsed().value,
                session_gate_require=require_gate,
                ds_auth_mode=ds_guard.mode if ds_guard is not None else "off",
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
                # 终态申请清理:多副本各自跑,DELETE 幂等无需锁(对齐 mail sweep)。
                ("guild_request_sweep", lambda: safego.loop(
                    "guild_request_sweep", sweep_interval, guild_uc.sweep_terminal_join_requests
                )),
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
        if producer is not None:
            with contextlib.suppress(Exception):
                await producer.close()
        for client in (rdb, session_rdb):
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.aclose()
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
        # ⑫ 对应 Go 的 app_run_failed。
        plog.get().exception("app_run_failed")
        return 1


if __name__ == "__main__":
    with contextlib.suppress(BrokenPipeError):
        sys.exit(main())
