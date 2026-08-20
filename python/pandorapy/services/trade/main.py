"""Pandora trade 服务入口(Python 版)—— 对应 Go 侧 cmd/trade/main.go。

职责:玩家间两阶段确认交易;订单存 Redis(强依赖);结算走 ResourceLedger 原子扣减 +
幂等键 = order_id(不变量 §9.7);状态流转快照发 kafka pandora.trade.audit(弱依赖)。

★ 本服务**没有**后台循环、**没有** MySQL:订单过期是惰性判定(被访问时置 EXPIRED),
  死配额名额在下单撞上限时按需清理。别为了"看起来完整"给它加一个定时扫描 ——
  那是一份新的、没有事实驱动的状态机。

启动闸(逐条对齐 Go 侧;事件名逐字相同,Loki 告警按事件名建,改一个字就是静默失去覆盖):

  ① redis_endpoint_required        host 与 addrs 皆空 → 拒启
  ② redis_ping_failed              订单状态机不可降级,带着死 Redis Ready 就是把
                                   第一条业务命令变成事故
  ③ snowflake 抢号                  失败拒启;etcd 档抢不到**不得**退回 static(重号)
  ④ kafka_brokers_empty /          **WARN 不拒启**:审计是弱依赖。方向必须与 Go 相同,
     kafka_producer_init_failed    改成 fail-fast 会让 broker 抖一下就交易全停
  ⑤ resource_ledger_not_configured 没接真实账本又没显式开 Noop → 拒启。
                                   这是本服务最关键的一道:漏配的后果是"成交但不扣转",
                                   而订单状态 / 审计流水 / 客户端提示**全部显示成功**
  ⑥ session_gate                   require=true 时权威端点漏配拒启(fail-closed)

cell_route 的闸**不在这里**:Python 侧只实现了单 Cell,配了 cell_route.mode 会在
`Config.load` 的 pydantic 校验器里抛(表现为 config_scan_failed)。刻意不在这里
再写一份"cellroute_init_failed" —— 那是永远执行不到的死代码,而它的存在会让人
以为这道闸归 main 管(对照 dialogue/main.py 里同样的说明)。

运行:
    cd python
    .venv/Scripts/python.exe -m pandorapy.services.trade.main \
        -conf ../services/economy/trade/etc/trade-dev.yaml
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

from pandora.trade.v1 import trade_pb2_grpc

from pandorapy import godur
from pandorapy import kafkax
from pandorapy import log as plog
from pandorapy import redisx
from pandorapy import server as pserver
from pandorapy import sessiongate
from pandorapy import snowflake_etcd as psnowflake_etcd
from pandorapy.services.trade import biz as tbiz
from pandorapy.services.trade import conf as tconf
from pandorapy.services.trade import data as tdata
from pandorapy.services.trade import service as tsvc

SERVICE_NAME = "trade"
HTTP_DEFAULT_PORT = 21012
GRPC_SERVICE_FULL_NAME = "pandora.trade.v1.TradeService"

# 频率配额窗口固定 1 分钟(Go: Window: time.Minute)。配额值本身来自 yaml。
RATE_QUOTA_WINDOW_SEC = 60.0


def _build_topic(domain: str, event: str) -> str:
    """对应 Go 的 config.BuildTopic(infra.md §4 的 topic 命名规范)。

    ★ 为什么这里有一个本地实现:trade 的 audit topic 在 Go 侧不是常量,而是
    `config.BuildTopic("trade", "audit")` 现算的,所以生成的 pandorapy/kafka_topics.py
    里**没有**它。写死字面量是最差的选项 —— topic 名是 producer 与 consumer 唯一的
    约会地点,写错一个字符两侧都不报错(Kafka 自动建 topic,生产成功,消费端只是
    "没有消息")。用同一个构造公式至少保证与 Go 算出同一个名字。
    这个函数应当上提到共享的 pandorapy/config.py(与 Go 的 config.BuildTopic 同位置)。
    """
    return f"pandora.{domain}.{event}"


AUDIT_TOPIC = _build_topic("trade", "audit")  # pandora.trade.audit


class KafkaTradeAuditPusher:
    """把 biz.TradeAuditPusher 适配到 kafkax.KeyOrderedProducer。对应 Go 的 tradeAuditPusher。

    kafka key = order_id:同一订单的审计事件必须保序(key 决定 partition,
    partition 内才有序)。key 格式变一个字符就会算出别的 partition,
    同一订单的 PENDING / COMPLETED 被打散后乱序,而两侧都不报错。
    """

    __slots__ = ("_producer",)

    def __init__(self, producer: kafkax.KeyOrderedProducer) -> None:
        self._producer = producer

    async def push_audit(self, order) -> None:  # noqa: ANN001
        await self._producer.send(str(order.order_id), order)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线)而不是 `--conf`:Go 侧 flag 包用单横线,而
    run_services.ps1 / start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。
    Python 版必须能被**同一条命令行**拉起,否则那些脚本都要改。
    """
    ap = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    ap.add_argument(
        "-conf",
        dest="conf",
        default="etc/trade-dev.yaml",
        help="config file path(与 Go 版同名同默认值)",
    )
    return ap.parse_args(argv)


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
        cfg = tconf.Config.load(conf_path)
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        # Go 把"读不到文件"和"结构对不上"分成两个事件名,这里保持同样区分。
        # 「配了但 Python 侧没实现的段」(cell_route)也落在这一支 —— 闸挂在
        # BaseConf 的 pydantic 校验器上,加载时就抛了。
        logger.error("config_scan_failed", err=str(exc), path=str(conf_path))
        return 1

    # ── 闸① Redis 端点(强依赖:订单状态机不可降级)────────────────────────
    # 单实例填 host,Cluster / Sentinel 只填 addrs,两者皆空才算未配置。
    # 不设这道闸:host='' 会被客户端库解成 127.0.0.1:6379,连上一个无关的本机 Redis
    # 照常启动 —— 订单写进了一个没人读的库,而日志全绿。
    rc = cfg.node.redis_client
    if not rc.endpoints():
        logger.error(
            "redis_endpoint_required",
            hint="set node.redis_client.host (single) or node.redis_client.addrs (cluster)",
        )
        return 1

    # ── 闸② 启动期 Ping ──────────────────────────────────────────────────
    # 不探的话服务会带着一个死 Redis 正常 Ready,k8s 把流量切过来,**第一条业务命令**
    # 才暴露 —— 那时错误已经落在玩家请求上。启动期失败是刺眼可回滚的,运行期是事故。
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
        logger.error("redis_ping_failed", err=str(exc), addr=rc.host, addrs=list(rc.addrs))
        return 1
    logger.info("redis_connected", addr=rc.host, addrs=list(rc.addrs))

    ledger: tdata.GrpcResourceLedger | None = None
    producer: kafkax.KeyOrderedProducer | None = None
    node_holder = None
    try:
        # ── 闸③ Snowflake(order_id 生成)─────────────────────────────────
        # node_id_source ""/"static" → 用 yaml 的 node.node_id;"etcd" → 自动抢占 + 失租退出。
        # ★ 失租必须退出进程,不能降级继续发号:此刻另一副本可能已抢到同一 nodeID,
        # 继续发就是重号(§9 不变量 11),而重号在交易域表现为两笔不同交易共用 order_id
        # —— 幂等键撞车 = 第二笔被当成第一笔的重放,静默不结算。
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

        # ── 闸④ kafka producer(弱依赖)──────────────────────────────────
        # ★ 方向必须与 Go 相同:**WARN 并继续**,不是 fail-fast。审计流断了是要去查的
        # 问题,不是交易不能做的理由;改成拒启会让 broker 抖一下就把交易全停掉
        # (把一个对账工具变成故障开关)。代价是审计静默 fail —— 所以必须留 WARN。
        audit = None
        if cfg.kafka.brokers:
            try:
                # KafkaProducer 构造会真的去 bootstrap(与 Go 的 sarama.NewClient 同),
                # 是阻塞调用 —— 丢到线程里,免得 broker 不通时把事件循环卡住。
                producer = await asyncio.to_thread(
                    kafkax.KeyOrderedProducer, cfg.kafka.producer_conf(), AUDIT_TOPIC
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
                    hint="trade audit silently dropped until kafka is available",
                )
            else:
                audit = KafkaTradeAuditPusher(producer)
                logger.info("kafka_producer_ready", topic=AUDIT_TOPIC)
        else:
            logger.warning("kafka_brokers_empty", hint="trade audit disabled")

        # ── 闸⑤ ResourceLedger ──────────────────────────────────────────
        #
        # 配 trade.inventory_addr        → 直连(仍在跑的 Go)inventory,真实 P2P 原子对转
        # 未配且 allow_noop_ledger=true  → 退回 NoopResourceLedger(联调 / 单测)
        # 都没有                          → **拒启**
        #
        # 最后一支是本服务最要紧的一道闸:漏配后果是"成交但不扣转背包 / 货币",
        # 而订单状态、审计流水、客户端提示**全部显示成功** —— 等发现时已经产生了
        # 一批脏数据,且没有任何一条日志指向根因。
        #
        # ★ 这道闸同时**还在 TradeUsecase.__init__ 里**(ledger=None 且未显式开 Noop
        # 就构造失败)。两处不是冗余:构造函数那道管的是"任何人接线时忘了账本",
        # 本处管的是"给出可运维的诊断"(带 hint 的 Errorw + 退出码,而不是栈回溯)。
        if cfg.trade.inventory_addr:
            try:
                ledger = tdata.GrpcResourceLedger(cfg.trade.inventory_addr)
            except asyncio.CancelledError:
                # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                raise
            except BaseException as exc:  # noqa: BLE001
                # Go 侧这里是 grpcclient.MustDial 的 panic(无结构化事件),
                # 只有 target 本身非法才会走到 —— 不阻塞等连接就绪,见 GrpcResourceLedger。
                logger.error(
                    "resource_ledger_dial_failed",
                    err=str(exc),
                    inventory_addr=cfg.trade.inventory_addr,
                )
                return 1
            logger.info("resource_ledger_grpc", inventory_addr=cfg.trade.inventory_addr)
        elif cfg.trade.allow_noop_ledger:
            # 刻意**不**在这里打 resource_ledger_noop:TradeUsecase 构造时会打,
            # 事件名逐字相同。两处都打的话 Loki 上同一次启动出现两条,看起来像
            # 有两个实例误开了 Noop。
            pass
        else:
            logger.error(
                "resource_ledger_not_configured",
                hint="设 trade.inventory_addr 接真实 inventory P2P 原子对转;"
                "联调/单测可显式设 trade.allow_noop_ledger=true,生产不可空跑",
            )
            return 1

        # ── 装配链 ──────────────────────────────────────────────────────
        repo = tdata.RedisTradeRepo(rdb)
        uc = tbiz.TradeUsecase(repo, ledger, audit, snowflake_node, cfg.trade)

        # 下单/撤单频率配额(anti-abuse §6 第 6 项):复用共享 rdb,窗口固定 1 分钟。
        # 与 max_orders_per_player 总量闸正交 —— 总量限"同时挂多少",本值限"刷多快",
        # 挡的是「下单-撤单-再下单」的托管写 + 流水行放大循环。
        uc.set_rate_quota(
            redisx.ActionQuota(
                rdb, "trade", cfg.trade.rate_quota_per_min, RATE_QUOTA_WINDOW_SEC
            )
        )
        logger.info("trade_rate_quota_ready", per_min=cfg.trade.rate_quota_per_min)

        svc = tsvc.TradeService(uc)

        # ── 闸⑥ 会话现行性门(R5 复审 P0-1,INC-20260722-004)───────────
        # 客户端面请求的 jti 必须是 login 会话权威(pandora:sess,与 node.redis_client
        # 同一 Redis)的当前一代;顶号后旧 JWT 立即失去按 player_id 定向操作的能力。
        # require=true(prod 生成器机械置)时权威端点漏配 → 拒启:
        # 不拒的话被顶号的设备能继续用旧 JWT 交易到 token 自然过期。
        try:
            gate = sessiongate.must_build(rdb, cfg.session_gate.require)
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
            # 吞掉之后取消就**不再传播** —— 该停的停不下来:
            #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
            #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
            # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("session_gate_required", err=str(exc))
            return 1

        grpc_server = pserver.build_grpc_server(
            cfg.server.grpc,
            # Go 用 pmw.AuthOptional():身份从 Envoy 注入的头读出来即可,
            # callerID==0 的拦截由 service 层做(见 service.py)。
            auth_required=False,
            extra_interceptors=[
                sessiongate.SessionCurrentInterceptor(gate, cfg.session_gate.require)
            ],
        )
        trade_pb2_grpc.add_TradeServiceServicer_to_server(svc, grpc_server)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(grpc_server, [GRPC_SERVICE_FULL_NAME])

        http_app = pserver.build_http_app(SERVICE_NAME)

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                redis_addr=rc.host,
                kafka_brokers=list(cfg.kafka.brokers),
                # Go 打的是 time.Duration.String():5 分钟是 "5m0s" 而不是 yaml 里的 "5m"。
                order_expire=godur.duration_string(cfg.trade.order_expire_td()),
                runtime="python",  # 灰度期用它在 Grafana 里区分两个实现
            )

        # trade 没有后台循环(过期惰性判定、死名额按需清理),background 留空。
        await pserver.run(
            service_name=SERVICE_NAME,
            grpc_server=grpc_server,
            grpc_addr=cfg.server.grpc.addr,
            http_app=http_app,
            http_addr=cfg.server.http.addr,
            http_default_port=HTTP_DEFAULT_PORT,
            on_ready=_on_ready,
        )
        return 0
    finally:
        # 清理顺序与获取相反。每一步都 suppress:停机路径上一个清理失败不该
        # 掩盖真正的退出原因(也不该阻止后面的资源被释放)。
        if ledger is not None:
            await ledger.close()
        if producer is not None:
            with contextlib.suppress(Exception):
                await producer.close()
        if node_holder is not None:
            # 正常退出:停续约并断开 etcd。**刻意不 revoke** —— 立刻释放会让新副本
            # 在同一日历秒抢到同号并从 step 0 重数,逐位重号。
            with contextlib.suppress(Exception):
                await node_holder.close()
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
