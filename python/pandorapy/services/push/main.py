"""Pandora push 服务入口(Python 版)—— 对应 Go 侧 cmd/push/main.go。

职责:客户端长连接推送。唯一 RPC `Subscribe` 是 **server stream**;
kafka 13 个业务 topic 的消息先原子写入 Redis 投递缓冲(唯一定序与投递权威),
再唤醒该玩家的连接写者去拉取投递。

★ 启动闸逐条对齐 Go(**顺序本身是契约**:事件名是 Loki 告警与运维手册的入口):

    ① abs_conf_path_failed          -conf 解不成绝对路径              fail-fast
    ② config_load_failed            yaml 读不到                       fail-fast
    ③ config_scan_failed            yaml 结构对不上 / 配了未实现的段   fail-fast
    ④ redis_endpoint_empty          投递缓冲端点缺失                  fail-fast
    ⑤ redis_ping_failed             Redis 不通                        fail-fast
    ⑥ redis_eviction_policy_unsafe  maxmemory-policy ≠ noeviction     fail-fast
    ⑦ redis_eviction_policy_unverifiable  CONFIG GET 查不了           fail-fast
                                    (allow_unverified=true 时降为
                                     redis_eviction_policy_unverifiable_allowed WARN)
    ⑧ cellroute_init_failed         多 Cell 路由表建不起来            fail-fast
    ⑨ kafka_brokers_empty           brokers 缺失                      fail-fast
    ⑩ push_topics_empty             topics 缺失                       fail-fast
    ⑪ dlq_producer_init_failed      DLQ producer 建不起来             fail-fast
    ⑫ kafka_consumer_new_failed     消费者建不起来                    fail-fast

  ⚠️ **push 没有"弱依赖只 WARN"的闸**。kafka 在别的服务是通知通道(断了只是没通知),
     在 push 这里是**业务本体**:brokers 不通 = 一条推送都消费不到。所以 Go 侧
     kafka_brokers_empty 是 os.Exit 而不是 WARN,这里逐条照搬,**不要"顺手统一"成
     其它服务的宽松档**。

  ⚠️ 闸⑧与 Go 同位:建完 consumer 之后调 `cellroute_etcd.build_router`,失败即拒启,
     成功则把 router + 本实例 (self_region, self_cell) 注入每个消费者。
     `cell_route.mode` 为空(单 Cell,当前唯一形态)时 router 为 None,
     消费者拥有全部玩家,行为与单 Cell 历史完全一致。
     配置本身的自检(未知 mode / static 缺 cells / etcd 缺 endpoints)仍在闸③,
     与 Go 的 `RouterConfig.Validate` 同一份判据。

后台循环五条(全部走 pandorapy.safego / server.run:裸 create_task 的协程死掉后
进程照跑、health 照答 SERVING、**零日志**):
    ① kafka 消费循环 × N(每 topic 一条,13 条)
    ② 跨 Pod 唤醒信号订阅(Redis pub/sub,断连 1s 退避重建)
    ③ 每流的连接写者循环(唤醒 / 广播 / 30s 兜底轮询 → 拉缓冲投递)  ← 随 stream 起落
    ④ 每流的会话看门狗(30s 复查 jti/exp,连败 3 次 fail-closed 关流)← 随 stream 起落
    ⑤ 30s 兜底轮询节拍(③ 的 asyncio.wait timeout,兜"写入落在别的 Pod")
  ③④⑤ 在 biz.run_subscribe_stream 里,生命周期绑在 Subscribe 请求上(§16.7:
  协程不得逃逸出请求生命周期),不进 server.run(background=[...])。

运行:
    cd services/runtime/push
    PYTHONPATH=../../../python:../../../python/gen \\
        python -m pandorapy.services.push.main -conf etc/push-dev.yaml
"""

from __future__ import annotations

# ★ 必须最先 import:Windows 控制台默认 cp1252,日志里的中文会抛 UnicodeEncodeError
# 并把真正的启动错误顶掉。
from pandorapy import _utf8  # noqa: F401  isort:skip
from pandorapy import config as pconfig

import argparse
import asyncio
import contextlib
import pathlib
import sys

from pandora.push.v1 import push_pb2_grpc

from pandorapy import kafka_topics, kafkax
from pandorapy import cellroute_etcd
from pandorapy import log as plog
from pandorapy import redisx
from pandorapy import server as pserver
from pandorapy import sessiongate
from pandorapy.services.push import biz as pbiz
from pandorapy.services.push import conf as pconf
from pandorapy.services.push import connection as pconn
from pandorapy.services.push import consumer as pcons
from pandorapy.services.push import offline as poff
from pandorapy.services.push import service as psvc
from pandorapy.services.push import wake as pwake

SERVICE_NAME = "push"
HTTP_DEFAULT_PORT = 21014
GRPC_SERVICE_FULL_NAME = "pandora.push.v1.PushService"

# Redis 驱逐策略核验的唯一合法取值。
REQUIRED_MAXMEMORY_POLICY = "noeviction"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线):Go 侧 flag 包用单横线,而 run_services.ps1 /
    start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。Python 版必须能被**同一条
    命令行**拉起,否则那些脚本都要改。
    """
    ap = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    ap.add_argument("-conf", dest="conf", default="etc/push-dev.yaml")
    return ap.parse_args(argv)


async def verify_eviction_policy(rdb, allow_unverified: bool) -> bool:  # noqa: ANN001
    """核验 Redis(含 Cluster 全部 master)maxmemory-policy=noeviction。

    返回 True = 放行启动;False = 拒启(调用方 return 1)。

    ★ 为什么这道闸必须是 fail-fast:投递缓冲与会话门都以这台 Redis 为**权威**。
    maxmemory-policy 非 noeviction 时,内存压力下 allkeys-lru 等策略会**静默驱逐**
    `pandora:push:offline:*` 与 `pandora:sess:*` —— 表现是"无告警丢帧 + 会话门失效",
    而 Redis 自己一切正常。

    ★ CONFIG GET 失败缺省也是拒启(「查不了」≠「配置正确」)。托管 Redis 禁用
    CONFIG 的环境须人工确认策略后显式置 `push.allow_unverified_eviction_policy`,
    并把该确认列入部署核对清单。

    ★ Cluster 逐 master 核验:单次 CONFIG GET 只落在被路由到的一个节点,
    证明不了整个拓扑。
    """
    logger = plog.get()

    def _policy_of(values) -> str:  # noqa: ANN001
        """CONFIG GET 的返回值归一化。redis-py 可能给 bytes / str key。"""
        if not isinstance(values, dict):
            return ""
        for key, value in values.items():
            name = key.decode() if isinstance(key, bytes) else str(key)
            if name == "maxmemory-policy":
                return value.decode() if isinstance(value, bytes) else str(value)
        return ""

    def _unverifiable(err: BaseException) -> bool:
        if allow_unverified:
            logger.warning(
                "redis_eviction_policy_unverifiable_allowed",
                err=str(err),
                hint="allow_unverified_eviction_policy=true 放行:必须已人工确认全拓扑 "
                "maxmemory-policy=noeviction",
            )
            return True
        logger.error(
            "redis_eviction_policy_unverifiable",
            err=str(err),
            hint="CONFIG GET 失败,无法证明 maxmemory-policy=noeviction,fail-closed 拒启动;"
            "托管 Redis 禁用 CONFIG 时人工确认策略后置 push.allow_unverified_eviction_policy=true",
        )
        return False

    is_cluster = type(rdb).__name__ == "RedisCluster"
    try:
        if is_cluster:
            # target_nodes=PRIMARIES:逐 master 各返回一份,结果是 {node_name: {...}}。
            raw = await rdb.config_get(
                "maxmemory-policy", target_nodes=type(rdb).PRIMARIES
            )
            per_node = raw if _looks_like_per_node(raw) else {"primary": raw}
        else:
            per_node = {"primary": await rdb.config_get("maxmemory-policy")}
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001
        return _unverifiable(exc)

    for node, values in per_node.items():
        policy = _policy_of(values)
        if policy != REQUIRED_MAXMEMORY_POLICY:
            logger.error(
                "redis_eviction_policy_unsafe",
                policy=policy,
                node=str(node),
                hint="push 投递缓冲/会话门要求 maxmemory-policy=noeviction,"
                "驱逐策略会静默丢帧/放行旧会话",
            )
            return False
    return True


def _looks_like_per_node(raw) -> bool:  # noqa: ANN001
    """判断 cluster 的 config_get 返回的是 {node: {k: v}} 还是扁平 {k: v}。

    redis-py 在不同版本/不同 target_nodes 下两种形状都出现过;判错的后果是
    把整张表当成一个节点的配置 —— 核验会恒通过,这道闸静默失效。
    """
    if not isinstance(raw, dict) or not raw:
        return False
    return all(isinstance(v, dict) for v in raw.values())


async def _main_async(args: argparse.Namespace) -> int:  # noqa: C901 —— 与 Go 同为线性启动闸
    # 1. Logger 先起(后面每一道闸都要靠它发事件)
    logger = plog.setup(SERVICE_NAME)
    logger.info("service_starting", conf=args.conf)

    # ── 闸① -conf 解绝对路径 ───────────────────────────────────────────
    try:
        conf_path = pathlib.Path(args.conf).resolve()
    except OSError as exc:
        logger.error("abs_conf_path_failed", err=str(exc))
        return 1

    # ── 闸②③ 加载 yaml ────────────────────────────────────────────────
    try:
        cfg = pconf.Config.load(str(conf_path))
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        # Go 把"读不到文件"和"结构对不上"分成两个事件名,这里保持同样区分。
        # 闸⑧(cell_route.mode 非空)也落在这个分支 —— 见模块头注释。
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── 闸④ Redis 端点(强依赖:没有投递缓冲就没有 push,不可降级)────────
    rc = cfg.node.redis_client
    if not rc.host and not rc.addrs:
        logger.error(
            "redis_endpoint_empty",
            hint="node.redis_client.host (single) or addrs (cluster) required "
            "for push offline cache",
        )
        return 1

    # ── 闸⑤ Ping ───────────────────────────────────────────────────────
    # 不探的话服务会带着一个死 Redis 正常 Ready,k8s 把流量切过来后
    # **第一条 Subscribe** 才暴露 —— 那时错误已经落在玩家身上了。
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

    producers: list = []
    consumers: list[pcons.PushKafkaConsumer] = []
    # ★ 必须在 try **之前**声明:它在 finally 里被读。写在 try 内部的话,
    # 任何在赋值行之前失败的闸都会让 finally 抛 NameError,把真正的退出原因顶掉。
    cell_watcher = None
    try:
        # ── 闸⑥⑦ 持久性/驱逐门 ─────────────────────────────────────────
        if not await verify_eviction_policy(
            rdb, cfg.push.allow_unverified_eviction_policy
        ):
            return 1
        logger.info("redis_connected", addr=rc.host, addrs=list(rc.addrs), db=rc.db)

        # ── 三层装配 ────────────────────────────────────────────────────
        conns = pconn.ConnectionManager()
        offline = poff.RedisOfflineCache(
            rdb,
            retention_sec=cfg.push.offline_cache_ttl_sec(),
            max_frames=cfg.push.offline_cache_max_frames,
            key_ttl_sec=pconf.OFFLINE_KEY_TTL_SEC,
        )
        uc = pbiz.PushUsecase(conns, offline)

        # 会话现行性门(P0,INC-20260722-004):login 的 pandora:sess 权威在同一 Redis。
        # ★ 与其它 13 个服务不同,push **不能**靠 sessiongate 的 unary 拦截器 ——
        #   Subscribe 是 server stream,拦截器对它一律不生效(见 biz.py 头注释)。
        #   这里只把 gate 注入 usecase,判定在 biz 手写。
        sess_gate = sessiongate.must_build(rdb, cfg.push.require_session_gate)
        uc.set_session_gate(sess_gate, cfg.push.require_session_gate)
        svc = psvc.PushService(uc)

        # ── 闸⑨ kafka brokers(强依赖,见模块头注释)────────────────────
        if not cfg.kafka.brokers:
            logger.error("kafka_brokers_empty", hint="kafka.brokers required")
            return 1
        # ── 闸⑩ topics ─────────────────────────────────────────────────
        # conf.apply_defaults() 已经兜过 PUSH_TOPICS,走到这里为空只可能是 yaml
        # 显式写了 `topics: []` —— 那是"一条都不订阅",必须拒而不是当默认。
        if not cfg.push.topics:
            logger.error(
                "push_topics_empty", hint="push.topics required (or rely on conf defaults)"
            )
            return 1

        # ── 闸⑪⑫ 每 topic 一个 DLQ producer + 一个消费者 ────────────────
        producer_conf = kafkax.producer_conf_from(cfg.kafka)
        for topic in cfg.push.topics:
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
                    topic=topic, dlq_topic=dlq_topic, err=str(exc),
                    hint="push 离线信箱不可静默降级,DLQ 必须可用",
                )
                return 1
            producers.append(dlq)
            try:
                kc = pcons.PushKafkaConsumer(
                    brokers=list(cfg.kafka.brokers),
                    group_id=cfg.kafka.group_id,
                    topic=topic,
                    conns=conns,
                    offline=offline,
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
                return 1
            consumers.append(kc)
            logger.info(
                "kafka_consumer_ready",
                topic=topic, group=cfg.kafka.group_id, dlq_topic=dlq_topic,
            )

        # ── ⑧ cellroute 装配(位置是契约:与 Go 同在建完 consumer 之后)─────
        #
        # off(mode 空,当前唯一形态)→ router 为 None,消费者拥有全部玩家,行为不变。
        # static → 本地铺表;etcd → 连 etcd 全量 Get + watch 热更,watcher 在 finally 关。
        #
        # ★ 为什么这道闸必须 fail-fast:配了 cell_route 却按单 Cell 跑,消息会被写进
        #   **本 cell** 的 Redis 投递缓冲,而玩家连接在别的 cell —— 写错缓存 + ACK,
        #   静默丢,不报错。起不来是刺眼的,静默投错是致命的。
        try:
            router, cell_watcher = await cellroute_etcd.build_router(cfg.cell_route)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("cellroute_init_failed", err=str(exc))
            return 1
        if router is not None:
            for kc in consumers:
                kc.set_cell_ownership(
                    router, cfg.cell_route.self_region, cfg.cell_route.self_cell
                )
            logger.info(
                "cellroute_enabled",
                self_region=cfg.cell_route.self_region,
                self_cell=cfg.cell_route.self_cell,
            )

        # 跨 Pod 唤醒信号:写缓冲的 Pod 本地无连接时 PUBLISH player_id,
        # 持有连接的 Pod 订阅后立即拉取投递;30s 兜底轮询保留为信号丢失时的兜底。
        wake_signal = pwake.RedisWakeSignal(rdb)
        for kc in consumers:
            kc.set_wake_publisher(wake_signal)

        # ── gRPC + HTTP ─────────────────────────────────────────────────
        # auth_required=False:Subscribe 允许匿名直连联调(生产必经 Envoy jwt_authn)。
        # ★ **刻意不挂 SessionCurrentInterceptor** —— push 只有一个 stream RPC,
        #   挂上去是纯粹的假象(对 Subscribe 不生效),会让人以为门已经有了。
        #   真正的门在 biz.py 手写的三处。将来 push 若新增 unary RPC,再挂上它。
        grpc_server = pserver.build_grpc_server(cfg.server.grpc, auth_required=False)
        push_pb2_grpc.add_PushServiceServicer_to_server(svc, grpc_server)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(grpc_server, [GRPC_SERVICE_FULL_NAME])

        http_app = pserver.build_http_app(SERVICE_NAME)

        # ── 后台循环 ①② ─────────────────────────────────────────────────
        def _make_consumer_task(kc: pcons.PushKafkaConsumer):
            async def _run() -> None:
                await kc.run()

            _run.__name__ = f"kafka_consumer_{kc.topic}"
            return _run

        async def push_wake_subscriber() -> None:
            """跨 Pod 唤醒信号订阅。

            没有它跨 Pod 写入只剩 30s 兜底轮询:消息不丢,但延迟近 30s,
            与 push p99 <200ms 的验收口径不符。
            """
            await pwake.run_wake_subscriber(rdb, conns.send_to)

        background = [_make_consumer_task(kc) for kc in consumers]
        background.append(push_wake_subscriber)

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                redis_addr=rc.host,
                kafka_brokers=list(cfg.kafka.brokers),
                kafka_group=cfg.kafka.group_id,
                topics=list(cfg.push.topics),
                offline_ttl=cfg.push.offline_cache_ttl,
                require_session_gate=cfg.push.require_session_gate,
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
        # 先停消费者:再往缓冲里写就是给一批马上要断的连接白写。
        for kc in consumers:
            with contextlib.suppress(Exception):
                kc.stop()
        if cell_watcher is not None:
            with contextlib.suppress(Exception):
                await cell_watcher.close()
        for producer in producers:
            with contextlib.suppress(Exception):
                await producer.close()
        closer = getattr(rdb, "aclose", None) or getattr(rdb, "close", None)
        if closer is not None:
            with contextlib.suppress(Exception):
                await closer()


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
