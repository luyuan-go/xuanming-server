"""Pandora player_locator 服务入口(Python 版)—— 对应 Go 侧
services/runtime/player_locator/cmd/locator/main.go。

职责:玩家 presence 投影(「玩家此刻在哪」)。
  Redis hash `pandora:locator:<player_id>` + 长 TTL meta `pandora:locator:hubmeta:<id>`;
  Redis 是**强依赖**(启动期 Ping 失败直接退出)。

★ 首要语义(§9.22),移植时最容易丢的一条:
  locator 只是 presence / 最近活跃投影,**不是归属权威**。
  key miss 只说明 presence 不可见,不能证明玩家已离开旧 DS,也不能授权进入另一台 DS。
  真正的归属权威是 owner 服务。

★ 启动闸逐条对齐 Go(**顺序本身是契约**:事件名是 Loki 告警和运维手册的入口):

    ①  abs_conf_path_failed                  -conf 解不成绝对路径             fail-fast
    ②  config_load_failed                    yaml 读不到                       fail-fast
    ③  config_scan_failed                    yaml 结构对不上                   fail-fast
    ④  ds_auth_authority_mode_invalid        authority_mode 拼错               fail-fast
    ⑤  ds_auth_fence_config_invalid          authority_mode=redis 缺机械 fence fail-fast
    ⑥  redis_endpoint_required               presence 存储端点缺失             fail-fast
    ⑦  redis_ping_failed                     Redis 不通                        fail-fast
    ⑧  departure_event_enabled_but_no_kafka  开了离场事件却没 broker           fail-fast
    ⑨  departure_event_producer_init_failed  离场事件 producer 建不起来        fail-fast
    ⑩  cellroute_init_failed                 cell 路由表装配失败               fail-fast
    ⑪  ds_auth_guard_init_failed             DS 回调令牌守卫建不起来           fail-fast
    ⑫  ds_auth_fence_acquire_failed          DS 授权 capability 抢不到         fail-fast

  弱依赖三条**刻意只 WARN**,不得改成 fail-fast(改了会让好友在线态推送这种
  可降级增强把整个 presence 主链路一起拖停):
      presence_enabled_but_no_kafka / presence_producer_init_failed
      —— 降级为纯拉模式(§13.7「先拉后推」本来就是默认档)。

  ⚠️ 离场事件为什么**必须** fail-fast 而不像 presence 那样降级:
     消费方(pkg/offlinewatch)的时效是按「有事件」设计的,producer 静默不可用会让
     整条链**看起来在跑却永不触发**,排查时还会误以为是消费方的问题。
     宁可不 Ready 让编排器重试。关闭时 last-seen 照常记录,消费方走兜底复查。

★ 与 Go 的两处**已知差异**(都是 fail-fast,不是静默降级,详见交付说明):
    - 闸⑩ 在 Python 侧提前到了配置加载阶段:`cell_route.mode` 非空由
      `pandorapy.config.BaseConf` 的 pydantic 校验器拒绝。事件名仍是
      cellroute_init_failed(Loki 告警按事件名建),只是位置比 Go 早。
    - 闸⑪/⑫:Python 侧尚无 DS 回调令牌守卫与 dsauthfence capability 实现,
      `ds_auth.mode != off` 或 `authority_mode=redis` 时**拒绝启动**。
      绝不能"当成 off 继续跑" —— 那是把 fail-closed 的令牌校验降级成 fail-open。
      dev / 当前生产档是 mode=off + authority_mode=legacy,Go 侧此时守卫本身
      也是 no-op,两个实现行为一致。

后台循环(全部走 pandorapy.safego:裸 create_task 的协程死掉后进程照跑、
health 照答 SERVING、**零日志**):
    - presence fan-out tick(去抖结算 + 合并 flush);presence.enabled=false 时不起。
    - DS 授权 fence 失租守望(authority_mode=redis 专用)—— Python 侧未实现,
      因为闸⑫已经把该模式挡在启动之外,不存在"起来了但没人守望"的状态。

运行:
    cd services/runtime/player_locator
    PYTHONPATH=../../../python:../../../python/gen \\
        python -m pandorapy.services.player_locator.main -conf etc/locator-dev.yaml
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


from pandora.locator.v1 import locator_pb2, locator_pb2_grpc

from pandorapy import godur
from pandorapy import kafka_topics
from pandorapy import kafkax
from pandorapy import killswitch
from pandorapy import log as plog
from pandorapy import redisx
from pandorapy import safego
from pandorapy import server as pserver
from pandorapy.services.player_locator import conf as lconf
from pandorapy.services.player_locator import presence as lpresence
from pandorapy.services.player_locator import repo as lrepo
from pandorapy.services.player_locator import service as lsvc
from pandorapy.services.player_locator import usecase as lusecase

SERVICE_NAME = "player_locator"
HTTP_DEFAULT_PORT = 21006
GRPC_SERVICE_FULL_NAME = "pandora.locator.v1.PlayerLocatorService"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析命令行。

    刻意用 `-conf`(单横线):Go 侧 flag 包用的是单横线,而 run_services.ps1 /
    start.ps1 / K8s manifest 里全是 `-conf xxx.yaml`。Python 版必须能被**同一条
    命令行**拉起,否则那些脚本都要改。
    """
    ap = argparse.ArgumentParser(prog=f"pandora-{SERVICE_NAME}", add_help=True)
    ap.add_argument("-conf", dest="conf", default="etc/locator-dev.yaml")
    return ap.parse_args(argv)


class KafkaPresencePusher:
    """把合并后的一批 presence 变更发到 kafka —— 对应 Go 的 presencePusher。

    kafka key = subscriber_id(不变量 §9:同订阅者事件保序;push 服务按 key 路由
    到该玩家的 stream)。payload = PresenceBatchEvent(push 透传给客户端解码)。
    """

    __slots__ = ("_producer",)

    def __init__(self, producer: kafkax.KeyOrderedProducer) -> None:
        self._producer = producer

    async def push_presence(
        self, subscriber_id: int, changes: list[lpresence.PresenceChangeOut]
    ) -> None:
        evt = locator_pb2.PresenceBatchEvent(
            changes=[
                locator_pb2.PresenceChange(
                    player_id=c.player_id, status=c.status, ts_ms=c.ts_ms
                )
                for c in changes
            ]
        )
        await self._producer.send(str(subscriber_id), evt)


class KafkaDepartureNotifier:
    """把「玩家离开 Hub」发成服务间事件 —— 对应 Go 的 departureNotifier。

    kafka key = player_id(不变量 §9:同玩家事件保序 ——「离开」与后续可能的
    「又离开」必须落同一分区,否则消费方会先看到旧的那条)。
    payload = PlayerLeftHubEvent(服务间事件,push 不订阅、不下发客户端)。
    """

    __slots__ = ("_producer",)

    def __init__(self, producer: kafkax.KeyOrderedProducer) -> None:
        self._producer = producer

    async def notify_left_hub(self, player_id: int, left_at_ms: int, hub_pod: str) -> None:
        evt = locator_pb2.PlayerLeftHubEvent(
            player_id=player_id, left_at_ms=left_at_ms, hub_pod=hub_pod
        )
        await self._producer.send(str(player_id), evt)


def _producer_conf(kafka: lconf.KafkaConf) -> kafkax.ProducerConf:
    return kafkax.producer_conf_from(kafka)


async def _main_async(args: argparse.Namespace) -> int:  # noqa: C901 —— 与 Go 同为线性启动闸
    # 1. Logger
    logger = plog.setup(SERVICE_NAME)
    logger.info("service_starting", conf=args.conf)

    # ── 闸① 配置路径 ─────────────────────────────────────────────────────
    try:
        conf_path = pathlib.Path(args.conf).resolve()
    except OSError as exc:
        logger.error("abs_conf_path_failed", err=str(exc))
        return 1

    # ── 闸② 读 yaml / 闸③ 结构 / 闸⑩ cell 路由 ───────────────────────────
    try:
        cfg = lconf.Config.load(str(conf_path))
    except pconfig.ConfigLoadError as exc:
        # ★ Go 的 `c.Load()`(main.go:67)覆盖"读文件 + 解析 yaml"两步,两者失败都是
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

    # ── 闸④ 授权权威模式拼写 ─────────────────────────────────────────────
    # 把预期的 redis 误写成别的值却静默退化为 legacy,SetLocation / ReportDisconnect
    # 会**绕过** active credential 门,而启动日志毫无痕迹。
    try:
        cfg.validate_ds_auth_authority_mode()
    except ValueError as exc:
        logger.error("ds_auth_authority_mode_invalid", err=str(exc))
        return 1

    # ── 闸⑤ Redis 单一权威的机械 fence ───────────────────────────────────
    # 缺 fence 时失租 / 旧 epoch 的副本会继续接受 Hub 写回,而权威面看起来一切正常。
    if cfg.ds_auth.authority_mode_redis():
        try:
            cfg.ds_auth.validate_redis_fence()
        except ValueError as exc:
            logger.error("ds_auth_fence_config_invalid", err=str(exc))
            return 1

    # ── 闸⑥ Redis 端点(强依赖:本服务是「玩家在哪」的投影存储)────────────
    # 单实例填 host,Redis Cluster / Sentinel 只填 addrs,两者皆空才算未配置。
    rc = cfg.node.redis_client
    if not rc.host and not rc.addrs:
        logger.error(
            "redis_endpoint_required",
            hint="set node.redis_client.host (single) or node.redis_client.addrs (cluster)",
        )
        return 1

    # ── 闸⑦ Ping ─────────────────────────────────────────────────────────
    # 不探的话服务会带着一个死 Redis 正常 Ready,k8s 把流量切过来后**第一条业务
    # 命令**才暴露 —— 那时错误已经落在玩家请求上了。
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

    presence_producer: kafkax.KeyOrderedProducer | None = None
    departure_producer: kafkax.KeyOrderedProducer | None = None
    try:
        repo = lrepo.RedisLocationRepo(rdb)

        # ── presence fan-out worker(§13.4 / §13.5)——**弱依赖,默认关闭** ──
        # 开启需 kafka.brokers(往 pandora.presence.update 生产 → push 投递)。
        # 这两条只 WARN:好友在线态推送是可降级增强,broker 抖动不该把 presence
        # 主链路(位置读写)一起拖停;降级后退回纯拉模式,功能仍在,只是不实时。
        presence_hub: lpresence.PresenceHub | None = None
        if cfg.presence.enabled:
            if not cfg.kafka.brokers:
                logger.warning(
                    "presence_enabled_but_no_kafka",
                    hint="set kafka.brokers; fan-out disabled, fallback pure-pull",
                )
            else:
                try:
                    presence_producer = kafkax.KeyOrderedProducer(
                        _producer_conf(cfg.kafka), kafka_topics.TOPIC_PRESENCE_UPDATE
                    )
                except asyncio.CancelledError:
                    # ★ 取消必须穿透:CancelledError 是 BaseException,会被下面那条宽 except 吞掉。
                    # 吞掉之后取消就**不再传播** —— 该停的停不下来:
                    #   业务路径上 grpc.aio 用取消终止在途 handler,吞了会把取消变成一个正常应答;
                    #   启动路径上则是 Ctrl-C / 上层取消被翻译成某道闸的失败,报出假的失败原因。
                    # 两种都让 §9.16 的「先摘流量 → 再排空在途」失效。
                    raise
                except BaseException as exc:  # noqa: BLE001 —— 弱依赖
                    logger.warning(
                        "presence_producer_init_failed",
                        err=str(exc), hint="fan-out disabled, fallback pure-pull",
                    )
                    presence_producer = None
                else:
                    ks_key = cfg.presence.kill_switch_key
                    presence_hub = lpresence.PresenceHub(
                        KafkaPresencePusher(presence_producer),
                        cfg.presence.debounce_window_td().total_seconds(),
                        cfg.presence.coalesce_tick_td().total_seconds(),
                        lambda key=ks_key: killswitch.disabled(key),
                    )
                    logger.info(
                        "presence_fanout_enabled",
                        debounce=cfg.presence.debounce_window,
                        coalesce_tick=cfg.presence.coalesce_tick,
                        kill_switch_key=ks_key,
                    )

        # presence_hub 为 None 时 usecase 走纯拉(SubscribePresence no-op)。
        uc = lusecase.LocatorUsecase(
            repo, cfg.locator.location_ttl_td().total_seconds(), presence_hub
        )
        uc.set_last_seen_retention(cfg.locator.last_seen_retention_td().total_seconds())

        # ── 闸⑧⑨ 离场事件出口(topic pandora.player.presence)──────────────
        # 默认关;开启后 kafka 是**强依赖**(理由见模块头)。
        if cfg.locator.departure_event.enabled:
            if not cfg.kafka.brokers:
                logger.error(
                    "departure_event_enabled_but_no_kafka",
                    hint="set kafka.brokers, or turn off locator.departure_event.enabled",
                )
                return 1
            try:
                departure_producer = kafkax.KeyOrderedProducer(
                    _producer_conf(cfg.kafka), kafka_topics.TOPIC_PLAYER_PRESENCE
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
                    "departure_event_producer_init_failed",
                    err=str(exc), topic=kafka_topics.TOPIC_PLAYER_PRESENCE,
                )
                return 1
            uc.set_departure_notifier(KafkaDepartureNotifier(departure_producer))
            logger.info(
                "departure_event_enabled", topic=kafka_topics.TOPIC_PLAYER_PRESENCE
            )

        # ── 闸⑪ DS 回调令牌守卫 ───────────────────────────────────────────
        # mode=off(默认)→ 不校验,与 Go 的 nil guard 行为一致。
        # mode!=off:Python 侧尚未实现该 middleware —— **拒绝启动**。
        # 静默当成 off 继续跑 = 把 fail-closed 的令牌校验降级成 fail-open:
        # 任何能到达 :8444 的东西都能改别人的位置投影,而 yaml 写着 enforce。
        if cfg.ds_auth.enabled():
            logger.error(
                "ds_auth_guard_init_failed",
                mode=cfg.ds_auth.mode,
                err="python runtime has no DS callback token guard "
                    "(pkg/middleware.DSCallbackGuard not ported)",
                hint="用 Go 版跑这个服务,或先把 DS 回调令牌校验实现出来;"
                     "绝不能把 mode=permissive/enforce 当成 off 继续跑",
            )
            return 1
        # ⚠️ 这里**不能**打 `ds_callback_guard_ready`。
        #
        # Go 侧那条日志的条件是 `if dsGuard != nil`(main.go:184),即**装配出了守卫**
        # 才打;而 Python 走到这一行时恰恰是 mode=off / 空(enabled 档在上面已经拒启),
        # 也就是**没有守卫**。沿用同一个事件名会让运维在 Loki 上看到
        # "DS 回调守卫就绪",而真相是这台副本根本不校验 DS 回调 —— 语义正好相反。
        logger.info(
            "ds_callback_guard_disabled",
            mode=cfg.ds_auth.mode.strip().lower() or "off",
            hint="mode=off:本服务不校验 DS 回调令牌(与 Go 的 off 档同行为)",
        )

        # ── 闸⑫ DS 授权 capability(authority_mode=redis)──────────────────
        # Go 在这里向 etcd 注册带租约的 capability 并守望失租;Python 侧未实现。
        # 注:闸⑤已要求 redis 模式必须 mode=enforce,因此实际会先被闸⑪拦下;
        # 这道仍然保留 —— 少了它,将来若闸⑪被实现,这里会变成静默缺口。
        if cfg.ds_auth.authority_mode_redis():
            logger.error(
                "ds_auth_fence_acquire_failed",
                err="python runtime has no dsauthfence capability lease "
                    "(pkg/dsauthfence.AcquireRuntime not ported)",
                hint="失租 / 旧 epoch 副本必须立即退出,禁止继续接受 Hub 写回",
            )
            return 1

        # ── 装配链 ────────────────────────────────────────────────────────
        svc = lsvc.LocatorService(uc)

        # auth_required=False:本服务的 RPC 由内网 DS / login / matchmaker 调用,
        # 不直接暴露给玩家(Go 的 NewGRPCServer 同样没挂 AuthRequired);
        # 调用方限制由 Envoy 路由层的内网黑白名单承担。
        # 也**没有** sessiongate:Go 的 internal/server/grpc.go 里没有
        # pmw.SessionCurrent —— 这不是客户端面服务,加上去会把所有内部调用拒掉。
        grpc_server = pserver.build_grpc_server(cfg.server.grpc, auth_required=False)
        locator_pb2_grpc.add_PlayerLocatorServiceServicer_to_server(svc, grpc_server)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(grpc_server, [GRPC_SERVICE_FULL_NAME])

        http_app = pserver.build_http_app(SERVICE_NAME)

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                redis_addr=rc.host,
                location_ttl=godur.duration_string(cfg.locator.location_ttl_td()),
                # 配置值可能低于机械下限(27s),生效值以 usecase 为准 —— 两个都打,
                # 否则运维看 yaml 与看行为对不上时无从判断是哪一侧的问题。
                effective_ttl_sec=uc.ttl_sec,
                last_seen_retention=godur.duration_string(
                    cfg.locator.last_seen_retention_td()
                ),
                departure_event=cfg.locator.departure_event.enabled,
                presence_fanout=presence_hub is not None,
                ds_auth_mode=cfg.ds_auth.mode or "off",
                ds_auth_authority_mode=cfg.ds_auth.authority_mode,
                runtime="python",  # 灰度期用它在 Grafana 里区分两个实现
            )

        background: list = []
        if presence_hub is not None:
            hub = presence_hub

            async def presence_fanout_tick() -> None:
                """去抖结算 + 合并 flush。

                走 safego.loop:单轮抛异常时只跳过本轮并留日志,下一拍继续 ——
                裸 create_task 的话这条循环死掉后进程照跑、health 照答 SERVING、
                零日志,表现是「好友在线态永远不更新」而没有任何报错。
                (对应 Go 的 stepSafely + safego.Recover。)
                """
                await safego.loop("presence_fanout_tick", hub.tick_sec, hub.step)

            background.append(presence_fanout_tick)

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
        for producer in (presence_producer, departure_producer):
            if producer is not None:
                with contextlib.suppress(Exception):
                    await producer.close()
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
