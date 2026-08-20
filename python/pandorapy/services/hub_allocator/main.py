"""hub_allocator 服务入口 —— 对应 Go 侧
`services/battle/hub_allocator/cmd/hub_allocator/main.go`
(以及 `internal/server/grpc.go` / `internal/server/http.go`,两者在 Python 侧折进本文件)。

职责:大厅 DS 分片调度。login 登录成功后调 AssignHub 给玩家分一个 hub DS 分片并签
hub 票据;Hub DS 每 5s 调 Heartbeat 续命,心跳超时由后台扫描标记 draining 停止分配。

★ 本文件是一条**线性启动闸链**,顺序照抄 Go,不是形式主义:
  后面的闸依赖前面已经确立的前提。举两个真会出事的例子 ——
    · Model B 的 reservation_ttl 下限校验读的是 `dstV2.TTL()`,必须排在 v2 签发器
      构造之后;提前判定会拿 0 当 TTL,于是任何配置都"通过"。
    · 写者继任租约的 `on_elected` 推扫是**接流前硬门**,必须排在 gRPC server 起来
      之前;顺序一换,"当选即接写、推扫尚未完成"的窗口里前任仍能写(§9.22 单写者破)。

★ 关于「fail-fast vs. warn-only」:方向弄反两种都是事故,但**不是同一种事故** ——
    该拒的没拒 = 带病上线(权威面已不可信,服务却看起来完全正常);
    该 warn 的却拒了 = 把配置问题升级成可用性事故。
  因此下面每一条闸的档位都逐条对着 Go 抄,注释里写明"挡的是什么"。

★ 未移植能力一律 fail-fast,**不静默降级**(与 player_locator/main.py 同姿态):
  Python 运行时目前没有 RS256 DSTicket 签发器、没有 DS 回调令牌**签发**器、
  没有 dsauthfence capability 租约。这三样缺任何一个都不能"当成没配置"继续跑 ——
  那会让 agones / Model B / enforce 档变成"看起来在跑、实际零鉴权"。
  因此 Python 侧当前可运行档位是 `mode=mock`(离线联调)与 legacy HS256 票据面。
"""

from __future__ import annotations

# ★ 必须最先 import:Windows 控制台默认 cp1252,日志里的中文会抛 UnicodeEncodeError,
# 把真正的启动错误顶掉(实测踩过多次)。
from pandorapy import _utf8  # noqa: F401  isort:skip
from pandorapy import config as pconfig

import argparse
import asyncio
import contextlib
import datetime as _dt
import os
import pathlib
import sys
import uuid

from pandora.hub.v1 import allocator_pb2_grpc as hubgrpc

from pandorapy import dsauth
from pandorapy import godur
from pandorapy import kafka_topics
from pandorapy import kafkax
from pandorapy import log as plog
from pandorapy import redisx
from pandorapy import releasetrack
from pandorapy import safego
from pandorapy import server as pserver
from pandorapy import sessiongate

from pandorapy.services.hub_allocator import biz as hbiz
from pandorapy.services.hub_allocator import biz_base as hbase
from pandorapy.services.hub_allocator import conf as hconf
from pandorapy.services.hub_allocator import fleet as hfleet
from pandorapy.services.hub_allocator import ledger as hledger  # noqa: F401  —— repo 依赖自注册
from pandorapy.services.hub_allocator import locator_client as hlocator
from pandorapy.services.hub_allocator import owner_lease_client as howner
from pandorapy.services.hub_allocator import repo as hrepo
from pandorapy.services.hub_allocator import service as hsvc
from pandorapy.services.login import dsticket as ldsticket

SERVICE_NAME = "hub_allocator"
HTTP_DEFAULT_PORT = 20022
GRPC_SERVICE_FULL_NAME = "pandora.hub.v1.HubAllocatorService"

#: DS 验签器允许的最大时钟偏移。Go `main.go:208` 的 `dsVerifierMaxLeeway`。
DS_VERIFIER_MAX_LEEWAY = _dt.timedelta(seconds=15)

#: 授权记录 / 代际计数器 TTL 地板。Go `main.go:191`、`main.go:222` 各写了一次同一个值。
AUTH_RECORD_TTL_FLOOR = _dt.timedelta(hours=48)


def hub_token_gen_key(pod: str) -> str:
    """某 Hub DS pod 的令牌代际计数器 key(Redis INCR 权威、独立、单调)。

    对应 Go 的 `hubTokenGenKey`。hashtag `{pod}` 锁 cluster slot,与该 pod 的分片镜像
    key 同 slot,便于 cluster 部署 —— 改动这里会让 Go / Python 两个实现在灰度期
    读写**两个不同的计数器**,代际单调性直接失效。
    """
    return "pandora:hub:tokengen:{" + pod + "}"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog=SERVICE_NAME, add_help=True)
    # 与 Go 侧同名:`-conf`(单横线),这样两个实现的启动命令完全一样。
    ap.add_argument("-conf", dest="conf", default="etc/hub_allocator-dev.yaml")
    return ap.parse_args(argv)


class HubTicketSigner:
    """把 `biz.TicketSigner` 适配到 legacy HS256 签发器。对应 Go 的 `hubTicketSigner`。

    hub DSTicket:`ds_type=hub`、`match_id=0`;`jti` 用 uuid v4 防重放(§9.3)。
    `role_id > 0` 时盖进票据(选角权威化 2026-07-08);region/cell 恒 0 ——
    hub_allocator 不做 cell 路由,与 Go 历史行为一致。

    ★ Go 的 v2(RS256 实例绑定)分支在 Python 侧**没有对应实现**,因此本类只覆盖
      legacy 路径;v2 是否需要在启动闸里判定,由 `_main_async` 负责,不在这里静默降级。
    """

    __slots__ = ("_signer",)

    def __init__(self, signer: ldsticket.DSTicketSigner) -> None:
        self._signer = signer

    async def sign_hub_ticket(
        self, player_id: int, role_id: int, binding: hbase.HubTicketBinding
    ) -> tuple[str, int]:
        jti = str(uuid.uuid4())
        # legacy 无绑定票:Go 的 SignHubDSTicketFull。绑定字段(pod/uid/...)在 legacy
        # HS256 票里没有对应 claim,Python 的 login.dsticket 也只实现了无绑定形态。
        return self._signer.sign(
            player_id,
            ldsticket.DS_TYPE_HUB,
            role_id=role_id,
            source_match_id=binding.source_match_id,
            jti=jti,
        )


class KafkaMigratePusher:
    """把 `biz.HubMigratePusher` 适配到 `kafkax.KeyOrderedProducer`。

    对应 Go 的 `kafkaMigratePusher`。强制整合时把 HubMigrateEvent payload
    按 `player_id`(kafka key)推给被迁移玩家本人 —— key 必须是玩家 ID 而不是分片 ID,
    否则同一玩家的迁移事件会散落到不同 partition,失去顺序(§9 不变量 9)。
    """

    __slots__ = ("_p",)

    def __init__(self, producer: kafkax.KeyOrderedProducer) -> None:
        self._p = producer

    async def push_migrate(self, player_id: int, payload: bytes) -> None:
        # ★ `push_to_players` 不抛异常,失败只体现在返回的 `last_err` 里(部分失败不该
        # 阻断同批其他玩家)。这里是**单目标**扇出,丢掉 last_err 就等于把"迁移通知没
        # 推出去"变成静默成功 —— 对应 Go 的 `return err`,调用方据此决定是否降级到
        # drain 心跳兜底。所以必须显式抛出。
        _, err = await self._p.push_to_players(0, [player_id], payload)
        if err is not None:
            raise err


async def _main_async(args: argparse.Namespace) -> int:  # noqa: C901 —— 与 Go 同为线性启动闸链
    # ── ① Logger ────────────────────────────────────────────────────────────
    logger = plog.setup(SERVICE_NAME)
    logger.info("service_starting", conf=args.conf)

    # ── ② 加载 yaml ─────────────────────────────────────────────────────────
    try:
        conf_path = pathlib.Path(args.conf).resolve()
    except OSError as exc:
        logger.error("abs_conf_path_failed", err=str(exc))
        return 1
    try:
        cfg = hconf.Config.load(str(conf_path))
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.error("config_scan_failed", err=str(exc))
        return 1

    # ── ③ 配置闸链(Go 的 ①②④⑤⑥⑦⑧⑨⑪ 全在这里)──────────────────────────
    #
    # ★ 这些闸**不是**在 main 里重写一遍,而是调 `conf.validate_conf()` —— 它已经
    #   逐条对着 Go 的 main.go 实现,包含每条的 fail-fast / warn-only 档位。在这里
    #   照抄一份的话,两处判据一旦漂移,"哪一份才是真的"就没人说得清了。
    #
    # ★ 致命项抛 ValueError,消息前缀就是 Go 的事件名(如
    #   `ds_auth_redis_authority_requires_agones: ...`),这里拆出来当事件名打,
    #   Loki 上两个实现的告警可以用同一条查询。
    #
    # ★ 返回值是**告警**,不是错误。方向弄反会出事:比如 ⑧ 密钥交叉在 off/permissive
    #   下 Go 只 warn(dev 模板两面共用同一把公开 dev 密钥),这里若按错误处理,
    #   标准 dev yaml 会直接起不来 —— 把一个配置提示升级成可用性事故。
    try:
        warnings = cfg.validate_conf()
    except ValueError as exc:
        event, _, detail = str(exc).partition(": ")
        logger.error(event, err=detail or str(exc))
        return 1
    for warning in warnings:
        event, _, detail = warning.partition(": ")
        logger.warning(event, err=detail or warning)

    # ── ④ Redis(强依赖:分片镜像 + 玩家归属)────────────────────────────────
    # 端点是否配置由闸③的 `redis_endpoint_required` 判过,这里只管连通性。
    rc = cfg.node.redis_client
    rdb = redisx.new_universal_client(rc)
    try:
        try:
            await asyncio.wait_for(rdb.ping(), timeout=3.0)
        except asyncio.CancelledError:
            # ★ 取消必须穿透:CancelledError 是 BaseException,被下面那条宽 except 吞掉
            # 之后取消就**不再传播** —— 该停的停不下来,Ctrl-C 会被翻译成"redis ping 失败"
            # 这种假原因(§9.16 的「先摘流量 → 再排空在途」也会一并失效)。
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("redis_ping_failed", err=str(exc), addr=rc.host, addrs=rc.addrs)
            return 1
        logger.info("redis_connected", addr=rc.host, addrs=rc.addrs)

        # ── ⑤ 未移植能力闸(Python 专有,**不静默降级**)────────────────────
        #
        # Go 在这里构造三样 Python 没有的东西:RS256 DSTicket 签发器、DS 回调令牌
        # **签发**器、dsauthfence capability 租约。判据按**谁真的会用到**来分,
        # 而不是按"配没配" —— 否则 mock 档只要 yaml 里留了个 ds_auth.secret 就起不来,
        # 那是把"配置里有个用不上的字段"升级成可用性事故。
        #
        #   · mode=agones:两样都要(v2 签票 + 回调令牌派发)。缺任一 → 分片永远翻不到
        #     ready、票据从实例绑定静默退回无绑定 legacy 票(§9.3/§9.22 全线失效)。
        #   · mode=local :要回调凭据签发器(local-off-v1 靠它经 env 播种本机 Hub DS)。
        #     缺了 DS 拿不到凭据,准入租约只认绑定式心跳 ACK → 玩家被 Hub DS 踢下线。
        #   · mode=mock  :两样都用不到(Mock provider 不派发也不验票),照常启动。
        if cfg.mode == hconf.MODE_AGONES:
            logger.error(
                "ds_ticket_v2_signer_init_failed",
                err="python runtime has no RS256 DSTicket signer / DS callback token "
                    "signer (pkg/auth.NewDSTicketSignerFromConf + "
                    "pkg/middleware.NewDSCallbackSignerFromConf not ported)",
                hint="用 Go 版跑 mode=agones;绝不能把配了私钥的 v2 档静默退回"
                     "无绑定 legacy 票,也不能不派发回调令牌就让分片翻 ready",
            )
            return 1
        if cfg.mode == hconf.MODE_LOCAL:
            logger.error(
                "local_hub_auth_profile_invalid",
                err="python runtime has no DS callback credential signer "
                    "(local-off-v1 requires it to seed the local Hub DS)",
                hint="mode=local requires ds_auth.mode=off + authority_mode=legacy + "
                     "signing key (local-off-v1); 用 Go 版跑 mode=local",
            )
            return 1

        # ── ⑥ legacy HS256 票据签发器 ────────────────────────────────────────
        # 走到这里 mode 必然是 mock:v2 未配(闸③④已保证 agones 才强制 v2),
        # 故与 Go 的 `dstV2 == nil` 分支一致,构造 legacy signer。
        if len(cfg.jwt.secret) < 32:
            logger.error(
                "hub_ticket_signer_init_failed",
                err="jwt.secret must be >= 32 bytes",
                hint="jwt.secret must be >=32 bytes and match login/envoy",
            )
            return 1
        ticket_ttl = cfg.jwt.ds_ticket_ttl_td()
        signer = HubTicketSigner(
            ldsticket.DSTicketSigner(
                secret=cfg.jwt.secret,
                issuer=cfg.jwt.issuer,
                audience=cfg.jwt.audience,
                ttl=ticket_ttl,
                additional_secrets=tuple(cfg.jwt.additional_secrets),
            )
        )
        logger.info(
            "hub_ticket_legacy_signer_ready",
            ds_ticket_ttl=godur.duration_string(ticket_ttl),
        )

        # ── ⑦ DS 回调守卫(校验 Hub DS Heartbeat 回调)────────────────────────
        # 档位合法性、secret 齐备性已由闸③的 `ds_auth_guard_init_failed` 判过。
        ds_guard = None
        try:
            ds_guard = dsauth.guard_from_conf(cfg.ds_auth)
        except Exception as exc:  # noqa: BLE001
            logger.error("ds_auth_guard_init_failed", err=str(exc))
            return 1
        # Go 的 dsGuard 恒非 nil(off 档是个"什么都放行"的守卫);Python 的
        # guard_from_conf 在 off 档返回 None。两者语义等价,判据统一用 mode 字符串。
        guard_mode = (cfg.ds_auth.mode or "").strip().lower() or "off"
        ds_enforce = guard_mode == "enforce"
        if ds_guard is None and guard_mode != "off":
            # 守卫构造不出来却不是 off 档 = 声称校验实际不校验,必须拒。
            logger.error(
                "ds_auth_guard_init_failed",
                err=f"ds_auth.mode={guard_mode} but no guard could be built",
                hint="绝不能把 mode=permissive/enforce 当成 off 继续跑",
            )
            return 1
        if ds_guard is not None:
            logger.info(
                "ds_callback_token_issuer_ready",
                hub_token_ttl=godur.duration_string(cfg.ds_auth.hub_token_ttl_td()),
                guard_mode=guard_mode,
            )

        # Model B「Redis 唯一授权权威」(§7)。Go 的判据是三者齐备;走到这里 mode
        # 必然是 mock(闸⑤已拒 agones / local),故恒为 False。这里**照抄判据**而不是
        # 写死 False:将来接上 v2 与回调签发器时,漏改这一行就是静默缺口 ——
        # Model B 没启用,而 service 层的 `set_model_b_authority(False)` 会让
        # 心跳接受 legacy 令牌(CE1/CE2 的门整条失效)。
        model_b_authority = (
            cfg.mode == hconf.MODE_AGONES
            and ds_enforce
            and cfg.ds_auth.authority_mode_redis()
        )

        # ── ⑧ 装配链 ─────────────────────────────────────────────────────────
        repo = hrepo.RedisHubRepo(rdb)
        # Hub DS 分片来源由 cfg.mode 单一开关决定,biz 逻辑零改。agones / local 两条
        # 已在闸⑤拒掉,故这里只剩 mock —— **刻意不保留那两个分支的空壳**:留着会让人
        # 以为 Python 支持它们。将来接 v2 时按 Go `main.go:337-470` 补回。
        fleet = hfleet.MockHubFleetProvider(cfg.hub)
        logger.warning(
            "mock_fleet_provider_active",
            mode=cfg.mode,
            hint="mode=mock,用确定性假分片(无真实 Hub DS)",
        )
        # Mock 是拓扑-only 不实现 HubFleetScaler:autoscale/consolidation 在此模式下
        # 不会运行。闸③的 `autoscale_inert_under_mock` 已经告警过,这里不重复。

        uc = hbiz.HubUsecase(repo, fleet, signer, cfg.hub)

        # ── owner 权威实例租约双写(owner-authority.md migrate ⑥)────────────
        # owner_addr 空 = 不启用。弱/强依赖语义见 conf.owner_lease_required。
        owner_lease = None
        if cfg.hub.owner_addr != "":
            owner_lease = howner.GrpcOwnerLeaseRenewer(cfg.hub.owner_addr)
            uc.set_owner_lease_renewer(owner_lease, cfg.hub.owner_lease_required)
            # migrate ①/③④:签票点 Begin(HUB) + census 代提交 Admit。
            #
            # ★ 接权威面的前提是本形态能产出 **exact owner 身份**(pod + instance_uid +
            #   instance_epoch + assignment_id + release_track)。否则 Begin 携空身份必被
            #   owner 判 15005,而签票是 fail-closed 的(§9.3/§9.22)→ 玩家永远拿不到 hub 票。
            #   mock 没有真实实例,取不出身份,故不接权威面(与 Go 一致)。
            owner_authority_enabled = model_b_authority or cfg.mode == hconf.MODE_LOCAL
            if owner_authority_enabled:  # pragma: no cover —— 两个分支在 Python 侧都已被拒
                uc.set_owner_authority(owner_lease)
            else:
                logger.warning(
                    "owner_authority_skipped_without_exact_identity",
                    authority_mode=cfg.ds_auth.authority_mode,
                    mode=cfg.mode,
                    hint="该形态产不出 exact 实例身份(如 mock),接权威面会让签票恒 15005;"
                         "仅保留租约双写",
                )
            logger.info(
                "owner_lease_dual_write_enabled",
                owner_addr=cfg.hub.owner_addr,
                required=cfg.hub.owner_lease_required,
                owner_authority=owner_authority_enabled,
            )

        # ── 灰度轨道策略(§9 不变量 21)───────────────────────────────────────
        canary_percent, canary_seed = 0, ""
        if cfg.mode == hconf.MODE_AGONES:  # pragma: no cover —— 闸④已拒
            canary_percent, canary_seed = cfg.agones.canary_percent, cfg.agones.canary_seed
        try:
            release_policy = releasetrack.new(canary_percent, canary_seed)
        except ValueError as exc:
            logger.error("hub_release_track_policy_invalid", err=str(exc))
            return 1
        uc.set_release_track_policy(release_policy)
        logger.info("hub_release_track_policy_ready", canary_percent=canary_percent)

        # agones 真 DS 链路的两条额外收紧(warming→ready 需真实鉴权心跳、令牌代际门)
        # 在 Python 侧走不到(闸④已拒),故不复制那一整段 —— 复制一段永远不执行的代码
        # 反而会让人误以为 Python 支持 agones。将来接 v2 时按 Go `main.go:472-524` 补齐。

        # ── ⑤.1 Kafka producer → migratePusher ───────────────────────────────
        # 弱依赖:broker 不通则 warn 并继续,迁移推送静默丢弃,Hub DS drain 心跳指令
        # 仍兜底让客户端重连到新分片。强制整合 consolidation 才需要。
        producer = None
        if cfg.kafka.brokers:
            try:
                producer = kafkax.KeyOrderedProducer(
                    kafkax.producer_conf_from(cfg.kafka), kafka_topics.TOPIC_HUB_MIGRATE
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.warning(
                    "kafka_producer_init_failed",
                    err=str(exc),
                    hint="hub migrate push will be silently dropped until kafka is available",
                )
            else:
                uc.set_migrate_pusher(KafkaMigratePusher(producer))
                logger.info("kafka_producer_ready", topic=kafka_topics.TOPIC_HUB_MIGRATE)
        elif cfg.hub.consolidation_enabled:
            logger.warning(
                "kafka_brokers_empty",
                hint="consolidation_enabled 但无 kafka:迁移仅靠 Hub DS drain 心跳兜底,"
                     "无无缝倒计时推送",
            )

        # ── ⑤.2 player_locator client → HubLocationChecker ───────────────────
        # 弱依赖:玩家主动切线护栏。addr 空则跳过战斗/匹配中检查;
        # 真正的"一人一 DS"仍由 DS 侧 SetLocation 强制。
        location_checker = None
        if cfg.hub.locator_addr != "":
            location_checker = hlocator.GrpcHubLocationChecker(cfg.hub.locator_addr)
            uc.set_location_checker(location_checker)
            logger.info("locator_client_ready", locator_addr=cfg.hub.locator_addr)
        else:
            logger.warning(
                "locator_addr_empty",
                hint="玩家切线不做战斗/匹配中检查(弱依赖,DS 侧 SetLocation 仍强制一人一 DS)",
            )

        svc = hsvc.HubService(uc)
        svc.set_ds_callback_guard(ds_guard)          # DS 回调令牌校验(Heartbeat);None=off
        svc.set_model_b_authority(model_b_authority)  # Model B:心跳必须携带 Model B 凭据
        # mode=local 专用准入通道:local-off-v1 无 Redis 授权面也无 DS 回调令牌,
        # Model B 那条 AcknowledgeAdmission 必拒 → Hub DS 把玩家踢下线。
        # agones/mock 恒为 false,线上不受影响。
        svc.set_local_admission(cfg.mode == hconf.MODE_LOCAL)

        # ── ⑥ 会话现行性门(R5 复审 P0-1,INC-20260722-004)────────────────────
        # ListHubLines / TransferToLine 两个客户端面 method 的 jti 必须是 login 会话
        # 权威当前一代;内部 / DS RPC 无 payload 头天然放行。
        sess_gate = sessiongate.must_build(rdb, cfg.session_gate.require)
        # R7 复审 P0-3:biz 也持有 session gate —— 迁移重签取当前会话 jti 签进 sjti;
        # AcknowledgeAdmission 在消费 reservation 前后双复核票据 sjti 现行性。
        uc.set_session_gate(sess_gate)
        # R7 收口(P0-5):票据 sjti 绑定强制门分阶段激活。默认兼容档(空 sjti 告警放行)。
        uc.set_session_gate_require_sjti(cfg.session_gate.require_ticket_sjti)
        # R9 复审 P1(开关依赖门禁):require_ticket_sjti=true 依赖 session gate 存在。
        # gate 未装配时开关只会静默变形为"永不复核" —— 安全开关必须 fail-fast,
        # 不能装饰性存在。
        if cfg.session_gate.require_ticket_sjti and sess_gate is None:
            logger.error(
                "require_ticket_sjti_needs_session_gate",
                hint="配置 node.redis_client(会话权威)或按 rollout 文档显式关闭 "
                     "session_gate.require_ticket_sjti",
            )
            return 1
        if sess_gate is not None:
            if cfg.session_gate.require_ticket_sjti:
                logger.info(
                    "hub_admission_sjti_require_active",
                    note="空 sjti 硬拒;前提=全 fleet DS 已转发 sjti 且旧票已过期",
                )
            else:
                logger.info(
                    "hub_admission_sjti_tolerant",
                    note="空 sjti 告警放行(混版兼容窗);排空后开 session_gate.require_ticket_sjti",
                )

        # ── 写者继任租约前置:dsauthfence capability ──────────────────────────
        # Go 在这里向 etcd 注册带租约的 capability、守望失租并在失租时立即退出。
        # Python 未移植 —— 少了它,失租 / epoch 回退的旧副本会继续写(§9.22 单写者破)。
        # 注:authority_mode=redis 在闸②已被要求 mode=agones,而 agones 在闸④被拒,
        # 因此实际到不了这里;保留本闸避免将来接 v2 后变成静默缺口。
        if cfg.ds_auth.authority_mode_redis():  # pragma: no cover
            logger.error(
                "ds_auth_fence_acquire_failed",
                err="python runtime has no dsauthfence capability lease "
                    "(pkg/dsauthfence.AcquireRuntime not ported)",
                hint="失租 / 旧 epoch 副本必须立即退出,禁止旧 writer 在 epoch 回退后继续写",
            )
            return 1
        # 写者继任租约只在 authority_mode=redis 下启动(Go 同样嵌在该分支内),
        # 故这里一并不启动。writer_lease_mode 的部署策略机械门禁同理。
        writer_mode = hconf.WRITER_LEASE_OFF
        try:
            writer_mode = cfg.hub.resolve_writer_lease_mode()
        except ValueError as exc:
            logger.error("hub_writer_lease_mode_invalid", err=str(exc))
            return 1
        if os.environ.get("KUBERNETES_SERVICE_HOST", "").strip():
            # 受管 k8s 里跑 Python 版 = 走的是 legacy 档但部署侧可能是 RollingUpdate。
            # 这里不能只告警:Python 没有继任租约,滚动重叠期两个副本都会写。
            logger.error(
                "hub_writer_lease_start_failed",
                err="python runtime has no writer succession lease wired for this profile",
                mode=writer_mode,
                hint="受管 k8s 内必须由运行时协议保证单写者;用 Go 版部署到 k8s",
            )
            return 1
        logger.warning(
            "hub_writer_lease_disabled",
            mode=writer_mode,
            hint="writer_lease_mode 未接线:单写者不再由运行时协议保证,只允许单副本 "
                 "Recreate 部署;RollingUpdate 下必须用 Go 版并改回 enforce",
        )

        # ── gRPC + HTTP ──────────────────────────────────────────────────────
        extra: list = []
        if sess_gate is not None:
            extra.append(
                sessiongate.SessionCurrentInterceptor(sess_gate, cfg.session_gate.require)
            )
        grpc_server = pserver.build_grpc_server(
            cfg.server.grpc, auth_required=False, extra_interceptors=extra
        )
        hubgrpc.add_HubAllocatorServiceServicer_to_server(svc, grpc_server)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(grpc_server, [GRPC_SERVICE_FULL_NAME])

        http_app = pserver.build_http_app(SERVICE_NAME)

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                redis_addr=rc.host,
                heartbeat_timeout=godur.duration_string(cfg.hub.heartbeat_timeout_td()),
                sweep_interval=godur.duration_string(cfg.hub.sweep_interval_td()),
                default_region=cfg.hub.default_region,
                mock_shard_count=cfg.hub.mock_shard_count,
                fleet_mode=cfg.mode,
                autoscale_enabled=cfg.hub.autoscale_enabled,
                consolidation_enabled=cfg.hub.consolidation_enabled,
                runtime="python",  # 灰度期用它在 Grafana 里区分两个实现
            )

        # ── ⑦ 后台心跳超时扫描(随进程生命周期启停)──────────────────────────
        async def heartbeat_sweep() -> None:
            """对应 Go 的 `go uc.RunHeartbeatSweep(sweepCtx)`。

            走 safego:单轮抛异常时只跳过本轮并留日志,下一拍继续。裸 create_task
            的话这条循环死掉后进程照跑、health 照答 SERVING、零日志,表现是
            「心跳超时的分片永远不转 draining」而没有任何报错 —— 玩家被持续路由到
            一台已经死掉的 Hub。
            """
            await safego.loop(
                "hub_heartbeat_sweep",
                cfg.hub.sweep_interval_td().total_seconds(),
                uc.sweep_once,
            )

        await pserver.run(
            service_name=SERVICE_NAME,
            grpc_server=grpc_server,
            grpc_addr=cfg.server.grpc.addr,
            http_app=http_app,
            http_addr=cfg.server.http.addr,
            http_default_port=HTTP_DEFAULT_PORT,
            on_ready=_on_ready,
            background=[heartbeat_sweep],
        )
        return 0
    finally:
        # 关闭顺序与 Go 的 defer 栈同序(后进先出):locator → owner → kafka → redis。
        for closable in (location_checker, owner_lease, producer):
            if closable is not None:
                with contextlib.suppress(Exception):
                    await closable.close()
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
