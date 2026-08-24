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

★ legacy HS256 与 v2 RS256 的选择**由配置显式决定**,不看"有没有私钥"猜:
  `ds_ticket.private_key_file` 非空 = 启用 v2(`DSTicketConf.signer_enabled()`),
  否则 legacy。这条不能写成"构造 v2 失败就退回 legacy" —— 那会在私钥路径打错一个
  字母时静默降级成**无实例绑定**的票,同时打穿实例绑定、灰度粘滞、jti 吊销三道门
  (§9.3),而运行期没有任何信号。构造失败一律 fail-fast,错误信息指向缺哪个配置。

★ 各能力的判据是「**谁真的会用到**」,不是「配没配」:
    · mode=agones:必须有 v2 签发器(闸③的 `agones_requires_ds_ticket_v2` 已强制)
      与 DS 回调令牌签发器(否则分片永远翻不到 ready);
    · mode=local :必须有 DS 回调凭据签发器(local-off-v1 靠它经 env 播种本机 Hub DS);
    · mode=mock  :两样都用不到,yaml 里留着用不上的字段也不该挡启动。
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
import socket
import sys
import uuid
from collections.abc import Awaitable, Callable

from pandora.hub.v1 import allocator_pb2 as hubv1
from pandora.hub.v1 import allocator_pb2_grpc as hubgrpc

from pandorapy import auth as pauth
from pandorapy import dsauth
from pandorapy import dsauthfence
from pandorapy import dsticket as pdsticket
from pandorapy import godur
from pandorapy import kafka_topics
from pandorapy import kafkax
from pandorapy import log as plog
from pandorapy import redisx
from pandorapy import releasetrack
from pandorapy import safego
from pandorapy import server as pserver
from pandorapy import sessiongate
from pandorapy import writerlease

from pandorapy.services.hub_allocator import agones_fleet as hagones
from pandorapy.services.hub_allocator import auth_repo as hauthrepo
from pandorapy.services.hub_allocator import biz as hbiz
from pandorapy.services.hub_allocator import biz_base as hbase
from pandorapy.services.hub_allocator import conf as hconf
from pandorapy.services.hub_allocator import fleet as hfleet
from pandorapy.services.hub_allocator import http as hhttp
from pandorapy.services.hub_allocator import ledger as hledger  # noqa: F401  —— repo 依赖自注册
from pandorapy.services.hub_allocator import local_fleet as hlocal
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

#: Redis 单命令预算(令牌代际 INCR / EXPIRE)。Go 那边是 `context.WithTimeout(ctx, 3*time.Second)`。
REDIS_CMD_TIMEOUT_SEC = 3.0

#: 写者继任租约的 etcd election 名。Go `main.go` 的 `Election: "hub_allocator/writer"`。
WRITER_LEASE_ELECTION = "hub_allocator/writer"

#: 部署策略机械门禁读的两个环境变量(Go 同名常量)。
DEPLOY_STRATEGY_ENV = "PANDORA_DEPLOY_STRATEGY"
KUBERNETES_SERVICE_HOST_ENV = "KUBERNETES_SERVICE_HOST"
#: Deployment.spec.strategy.type 的滚动值(大小写不敏感比较,同 Go 的 EqualFold)。
DEPLOY_STRATEGY_ROLLING_UPDATE = "rollingupdate"

# ── dsauthfence capability 契约(Go `main.go` 的 `features = []string{...}`)────
#
# ★ 逐字照抄,且只在 Model B 下声明(Go: `if modelBAuthority`)。feature 集合在 etcd
#   侧是**精确相等**比较(见 `dsauthfence.equal_feature_set` 与
#   `REQUIRED_POLICY_V3_FEATURES["hub_allocator"]`),多一个 / 少一个 / 拼错一个字母
#   都不是"降级注册",而是 capability 直接注册不上 → 进程 fail-closed 退出。
# ★ 非 Model B 时是**空集**而不是这一组:带着 V3 feature 去注册一个用不上继任租约的
#   副本,等于替一个不存在的 writer 占住 required 策略。
HUB_AUTH_FENCE_FEATURES_MODEL_B = (
    "hub-reservation-ledger-v1",
    "hub-heartbeat-capacity-v1",
    "hub-owner-cleanup-v1",
    "hub-physical-eviction-v1",
    "hub-successor-lease-v1",
)


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
    """把 `biz.TicketSigner` 适配到 v2(RS256)/ legacy(HS256)两条签发路径。
    对应 Go 的 `hubTicketSigner`。

    hub DSTicket:`ds_type=hub`、`match_id=0`;`jti` 用 uuid v4 防重放(§9.3)。
    `role_id > 0` 时盖进票据(选角权威化 2026-07-08);region/cell 恒 0 ——
    hub_allocator 不做 cell 路由,与 Go 历史行为一致。

    ★ 两条路径**互斥且由配置决定**(见模块头):`_v2` 非空即走 v2,不看绑定是否齐、
      也不因 v2 签失败退回 legacy。v2 档下绑定不完整一律拒签 —— 那正是 §9.22 exact
      实例绑定的执行点,"绑定缺就签一张不绑的"等于把这道门直接拆掉。
    """

    __slots__ = ("_legacy", "_v2")

    def __init__(
        self,
        *,
        legacy: ldsticket.DSTicketSigner | None = None,
        v2: pdsticket.DSTicketSigner | None = None,
    ) -> None:
        if (legacy is None) == (v2 is None):
            # 两个都给 = 签发路径不确定(同一张票可能忽 v2 忽 legacy);两个都不给 =
            # 签不出票。两种都必须在构造期炸,不能等到第一个玩家进不去大厅才发现。
            raise ValueError(
                "hub ticket signer requires exactly one of legacy(HS256) / v2(RS256)"
            )
        self._legacy = legacy
        self._v2 = v2

    async def sign_hub_ticket(
        self, player_id: int, role_id: int, binding: hbase.HubTicketBinding
    ) -> tuple[str, int]:
        jti = str(uuid.uuid4())
        if self._v2 is not None:
            # ★ 判据逐条对 Go:pod / uid / epoch / assignment 四项 + release_track 合法。
            #   凑不齐就拒签,错误文本照抄 Go(运维按它去查是哪一项没填进来)。
            if (
                binding.pod_name == ""
                or binding.instance_uid == ""
                or binding.protocol_epoch == 0
                or binding.hub_assignment_id == ""
                or not releasetrack.valid(binding.release_track)
            ):
                raise pdsticket.DSTicketConfigError(
                    "ds_ticket v2: hub 票必须带完整实例绑定"
                    f"(pod={binding.pod_name!r} uid={binding.instance_uid!r} "
                    f"epoch={binding.protocol_epoch} "
                    f"assignment={binding.hub_assignment_id!r} "
                    f"track={binding.release_track!r}),拒签无绑定票"
                )
            return self._v2.sign_hub_ticket(
                player_id,
                0,  # region_id:hub_allocator 不做 cell 路由
                0,  # cell_id
                role_id,
                jti,
                pdsticket.DSTicketTarget(
                    ds_pod_name=binding.pod_name,
                    ds_instance_uid=binding.instance_uid,
                    ds_instance_epoch=binding.protocol_epoch,
                    hub_assignment_id=binding.hub_assignment_id,
                    release_track=binding.release_track,
                    source_match_id=binding.source_match_id,
                    session_jti=binding.session_jti,
                ),
            )
        assert self._legacy is not None  # 构造期已保证二选一
        # legacy HS256:绑定齐备时签**绑定票**(Go 的 SignBoundHubDSTicket),
        # 零值绑定签兼容票(SignHubDSTicketFull)。判据是 `empty()`,不是"某一项非空" ——
        # 半绑定票在 DS 侧会被逐字段判空放行,等于能跨实例兑换。
        return self._legacy.sign(
            player_id,
            ldsticket.DS_TYPE_HUB,
            role_id=role_id,
            source_match_id=binding.source_match_id,
            jti=jti,
            binding=ldsticket.DSTicketBinding(
                ds_pod_name=binding.pod_name,
                ds_instance_uid=binding.instance_uid,
                protocol_epoch=binding.protocol_epoch,
                credential_gen=binding.credential_gen,
                credential_jti=binding.credential_jti,
                hub_assignment_id=binding.hub_assignment_id,
                writer_epoch=binding.writer_epoch,
            ),
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


def _exit_process() -> None:
    """capability 失租 / 旧 epoch 时的 fail-stop —— 对应 Go 那处 `os.Exit(1)`。

    ★ 用 `os._exit` 而不是 `sys.exit`:此刻身处一个后台 task 里,`sys.exit` 只会让
      **那个 task** 结束,进程照跑、gRPC 照答 SERVING —— 而"进程还在但已经证明不了
      自己该写"正是双写者窗口本身(§9.22)。
    ★ 独立成模块级函数只为可测:测试把它替换掉,否则真调 os._exit 会让 pytest 当场
      消失且不产生任何报告。
    """
    os._exit(1)


async def _watch_fence_lost(fence: dsauthfence.Holder) -> None:
    """capability 失租守望 —— 对应 Go 的 `go func(){ <-fence.Lost(); …; os.Exit(1) }`。

    ★ 这**不是**"用定时器掩盖时序"(§16.10):没有轮询、没有猜测,只是把 Holder 已经
      判定好的失效事件翻译成进程退出。到期动作是**停止一切写**,不是"假设已经好了
      继续往下走" —— 判别口诀的那一半正好相反。
    ★ 失租原因先于 `lost` 置位写入(见 `Holder._signal_lost`),所以这里读到的 reason
      必然非空,不会打出一条"失租了但不知道为什么"的日志。
    """
    await fence.lost.wait()
    plog.get().error(
        "ds_auth_fence_lost",
        reason=fence.lost_reason(),
        hint="立即退出，禁止旧 writer 在失租/epoch 回退后继续写",
    )
    _exit_process()


async def _noop_on_elected(_token: int) -> None:
    """warmup 档的空激活钩子 —— 对应 Go 在 `writerMode != enforce` 时**不设** `OnElected`。

    warmup 的定义就是"只竞选、观测 token 单调,不改写路径"(rollout §5.4 引导升级的
    第一跳)。此时既没有接 `uc.set_writer_fence`,也就没有任何 fence 水位需要在接流前
    推进 —— 这里刻意什么都不做,不是"以后再补"。

    ★ 反过来说:enforce 档**必须**给真钩子(`repo.advance_writer_fences_for_token`)。
      拿这个空实现去顶 enforce,等于把"接流前推扫硬门"删掉:当选即接写、而前任在
      继任者尚未触碰的 {pod} slot 上仍能写。
    """
    return None


def _writer_identity() -> str:
    """本副本的人读身份 `hostname/pid` —— 对应 Go 的
    `hostname, _ := os.Hostname(); fmt.Sprintf("%s/%d", hostname, os.Getpid())`。

    ★ 它**只是可观测标签**,不参与选举判定:Python 侧 `writerlease.WriterLease` 的
      参选键是 etcd 租约 ID(身份由 etcd 分配而不是自报),任期 token 取 leader key 的
      CreateRevision。两者都满足"同一副本重启后不会顶掉自己的旧任期"(旧租约到期
      即消失),所以这里不能、也不需要把 hostname 塞进 fencing 语义。
    ★ 但这条日志字段不能省:多副本告警时,`sum(...writer_held)==1` 只说明"有人在写",
      要定位**是谁**只能靠它(Go 的 `hub_writer_lease_started` 同样打 identity)。
    ★ `socket.gethostname()` 在极端环境可能抛(hostname 未配置);Go 那边是忽略 err
      拿空串,这里对齐 —— 一个观测标签绝不该把启动闸链打断。
    """
    try:
        hostname = socket.gethostname()
    except OSError:
        hostname = ""
    return f"{hostname}/{os.getpid()}"


def _check_writer_lease_deploy_strategy(logger, lease_mode: str) -> bool:  # noqa: ANN001
    """部署策略机械门禁 —— 对应 Go `main.go` 5.1 段里那段 strategy 判定。返回是否放行。

    进程看不到 `spec.strategy`,由 Deployment 把策略作为 annotation 注入 env
    (`PANDORA_DEPLOY_STRATEGY`);Go 侧另有清单契约测试钉住 annotation 与真实
    strategy 一致。

    三档刻意不同,方向弄反两种都是事故:
      · strategy=RollingUpdate 且 mode!=enforce → **fail-closed 退出**;
      · 受管 k8s 内 + env 缺失 → **fail-closed 退出**(清单回归必须炸,不能靠人看日志);
      · 非 k8s(本机裸跑 / dev)+ env 缺失 → 只告警(阻断会把开发环境一起打死)。

    不设这道闸会怎样:RollingUpdate × 非 enforce = 滚动重叠期新旧副本都在写席位 /
    容量账本,§9.1「一人一 hub」与 §9.22 单写者同时破,而此前仓库里没有任何机制
    阻止这个组合 —— warmup 连 off 的那条告警都没有。
    """
    strategy = (os.environ.get(DEPLOY_STRATEGY_ENV) or "").strip()
    in_managed_k8s = bool((os.environ.get(KUBERNETES_SERVICE_HOST_ENV) or "").strip())
    if strategy:
        if (
            strategy.lower() == DEPLOY_STRATEGY_ROLLING_UPDATE
            and lease_mode != hconf.WRITER_LEASE_ENFORCE
        ):
            logger.error(
                "hub_writer_lease_rollingupdate_without_enforce",
                strategy=strategy,
                mode=lease_mode,
                hint="RollingUpdate × writer_lease_mode!=enforce = 滚动重叠期无保护双写;"
                "要么把 writer_lease_mode 改 enforce,要么把 Deployment 改回单副本 "
                "Recreate(rollout §5.4 引导升级每跳 Recreate)",
            )
            return False
        logger.info(
            "hub_writer_lease_strategy_checked", strategy=strategy, mode=lease_mode
        )
        return True
    if in_managed_k8s:
        logger.error(
            "hub_writer_lease_strategy_annotation_missing",
            mode=lease_mode,
            hint="受管 k8s 内必须注入 PANDORA_DEPLOY_STRATEGY(取自 Deployment 的 "
            "pandora.dev/deploy-strategy annotation);缺失则无法机械校验 RollingUpdate×非 enforce "
            "的无保护双写组合,fail-closed 退出。见 deploy/k8s/services/services.yaml",
        )
        return False
    logger.warning(
        "hub_writer_lease_strategy_unknown",
        mode=lease_mode,
        hint="非 k8s 环境(本机裸跑/dev):跳过部署策略机械校验",
    )
    return True


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
    # capability 租约句柄提前声明:`finally` 里要在**任何其它资源之前**主动交还它
    # (它是"本副本还有没有写权"的唯一凭证,晚还一步继任副本就要多空等一个 TTL)。
    fence: dsauthfence.Holder | None = None
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

        # ── ⑤ DSTicket v2(RS256 实例绑定)签发器 ────────────────────────────
        #
        # 启用与否**只看配置**:`ds_ticket.private_key_file` 非空即启用。构造失败一律
        # fail-fast,绝不退回 legacy —— 静默降级会同时打穿实例绑定 / 灰度粘滞 / jti
        # 吊销三道门(§9.3),而运行期没有任何信号。
        #
        # ★ 顺序:必须排在闸③之后 —— `hub_reservation_ttl_invalid` 的下限读的是
        #   v2 有效 TTL(`effective_ttl_td()`),两者用同一个来源,不会漂移。
        dst_v2 = None
        if cfg.ds_ticket.signer_enabled():
            try:
                dst_v2 = pdsticket.new_ds_ticket_signer_from_conf(
                    pdsticket.DSTicketConf(
                        private_key_file=cfg.ds_ticket.private_key_file,
                        active_kid=cfg.ds_ticket.active_kid,
                        # conf 层不填默认(见 DSTicketConf.ttl_td 注释);零值在这里
                        # 翻成 None,由签发器取 DSTicketDefaultTTL —— 传 0 会被当作
                        # "显式要求 0 秒有效期"。
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
                kid=dst_v2.kid(),
                ttl=godur.duration_string(dst_v2.ttl()),
            )
        # B1:k8s Hub 只允许 RS256 实例绑定票。闸③的 `agones_requires_ds_ticket_v2`
        # 已按配置判过,这里再按**构造结果**判一次 —— 两者判的不是同一件事
        # (前者"配了没",后者"真的建起来了没"),缺后者会让构造失败的 agones 档
        # 掉进下面的 legacy 分支。
        if cfg.mode == hconf.MODE_AGONES and dst_v2 is None:
            logger.error(
                "agones_requires_ds_ticket_v2",
                hint="B1 k8s Hub 只允许 RS256;配置 ds_ticket.private_key_file + active_kid",
            )
            return 1

        # ── ⑥ legacy HS256 票据签发器(仅 v2 未启用时)────────────────────────
        # Go: `if dstV2 == nil { ... }` —— 两者互斥,不是"都建好再挑"。
        signer: HubTicketSigner
        if dst_v2 is not None:
            signer = HubTicketSigner(v2=dst_v2)
        else:
            if len(cfg.jwt.secret) < 32:
                logger.error(
                    "hub_ticket_signer_init_failed",
                    err="jwt.secret must be >= 32 bytes",
                    hint="jwt.secret must be >=32 bytes and match login/envoy",
                )
                return 1
            ticket_ttl = cfg.jwt.ds_ticket_ttl_td()
            signer = HubTicketSigner(
                legacy=ldsticket.DSTicketSigner(
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

        # ── ⑦ DS 回调令牌:签发器 + 守卫 + 验签器 ─────────────────────────────
        #
        # 三者判据各不相同(见 dsauth.signer_from_conf 的注释):
        #   · 签发看 secret —— mode=off 也照样要给 DS 派发令牌,否则灰度期把 mode
        #     切成 enforce 的那一刻,全部 DS 手上没有令牌 → 成批被拒;
        #   · 守卫看 mode  —— 本服务验不验回调;
        #   · 验签器看 secret —— 供 fleet 复核 annotation 上已有的令牌确实还能验过。
        try:
            ds_signer = dsauth.signer_from_conf(cfg.ds_auth)
        except (ValueError, pauth.TokenError) as exc:
            logger.error("ds_auth_signer_init_failed", err=str(exc))
            return 1
        ds_guard = None
        try:
            ds_guard = dsauth.guard_from_conf(cfg.ds_auth)
        except Exception as exc:  # noqa: BLE001
            logger.error("ds_auth_guard_init_failed", err=str(exc))
            return 1
        ds_verifier = None
        if cfg.ds_auth.secret != "":
            try:
                ds_verifier = dsauth.DSCallbackVerifier(
                    issuer=cfg.ds_auth.issuer,
                    audience=cfg.ds_auth.audience,
                    secret=cfg.ds_auth.secret,
                    additional_secrets=list(cfg.ds_auth.additional_secrets),
                )
            except ValueError as exc:
                logger.error("ds_auth_verifier_init_failed", err=str(exc))
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

        hub_token_ttl = cfg.ds_auth.hub_token_ttl_td()
        # 代际计数器 / 授权记录键的 TTL:2× 令牌寿命,再取 48h 地板。
        # ★ 地板不是保守起见:计数器一旦被 TTL 清掉,第 1 代会与历史第 1 代撞号,
        #   于是一张早已作废的旧令牌重新变成"当前代际"。
        auth_record_ttl = hub_token_ttl * 2
        if auth_record_ttl < AUTH_RECORD_TTL_FLOOR:
            auth_record_ttl = AUTH_RECORD_TTL_FLOOR

        async def _next_token_gen(pod: str, key_ttl: _dt.timedelta) -> int:
            """领一个严格递增的令牌代际(Redis INCR 权威)并续期计数器。

            对应 Go 的 `rdb.Incr(...)` + `rdb.Expire(...)`。两条命令都带独立预算 ——
            Redis 卡住时必须报错让签发失败,而不是让签发挂在那里把整条心跳链拖死。
            """
            gen = await asyncio.wait_for(
                rdb.incr(hub_token_gen_key(pod)), timeout=REDIS_CMD_TIMEOUT_SEC
            )
            # Expire 失败只影响"计数器什么时候被回收",不影响本次代际的正确性 ——
            # 与 Go 一致:忽略其错误,绝不因此让签发失败。
            with contextlib.suppress(Exception):
                await asyncio.wait_for(
                    rdb.expire(hub_token_gen_key(pod), int(key_ttl.total_seconds())),
                    timeout=REDIS_CMD_TIMEOUT_SEC,
                )
            return int(gen)

        async def issue_hub_token(pod: str) -> tuple[str, int, int]:
            """legacy 代际门:给某个 Hub DS pod 签一张带 `ds_gen` 的回调令牌。

            对应 Go 的 `issueHubToken`。返回 `(token, exp_ms, gen)`。
            """
            assert ds_signer is not None  # 仅在 ds_signer 非空时被注入
            gen = await _next_token_gen(pod, auth_record_ttl)
            token, exp_ms = ds_signer.sign_ds_callback_with_gen(
                pauth.DS_TYPE_HUB, pod, 0, gen, hub_token_ttl
            )
            return token, exp_ms, gen

        async def issue_hub_credential(
            pod: str, instance_uid: str, protocol_epoch: int
        ) -> tuple[str, hubv1.HubDSCredential]:
            """Model B:签一张绑完整实例身份的 hub 回调凭据。对应 Go 的 `issueHubCredential`。"""
            assert ds_signer is not None
            gen = await _next_token_gen(pod, auth_record_ttl)
            jti = str(uuid.uuid4())
            res = ds_signer.sign_hub_credential(
                pod, instance_uid, protocol_epoch, gen, jti, hub_token_ttl
            )
            return res.token, hubv1.HubDSCredential(
                gen=gen,
                jti=jti,
                exp_ms=res.exp_ms,
                kid=res.kid,
                instance_uid=instance_uid,
                protocol_epoch=protocol_epoch,
                token_sha256=res.token_sha256,
                writer_epoch=res.writer_epoch,
            )

        async def issue_local_hub_credential(
            pod: str, instance_uid: str, protocol_epoch: int
        ) -> tuple[str, hlocal.LocalHubCredential]:
            """mode=local:一次性 env 凭据。对应 Go 的 `issueLocalHubCredential`。

            ★ 与 Model B 的差别只有一处:**不给代际计数器设 TTL**。本机档没有
              annotation 轮换,计数器被 TTL 清掉就会撞号,而这里没有 Redis 授权记录
              能发现撞号(Go 同样只 Incr 不 Expire)。
            """
            assert ds_signer is not None
            gen = await asyncio.wait_for(
                rdb.incr(hub_token_gen_key(pod)), timeout=REDIS_CMD_TIMEOUT_SEC
            )
            jti = str(uuid.uuid4())
            res = ds_signer.sign_hub_credential(
                pod, instance_uid, protocol_epoch, int(gen), jti, hub_token_ttl
            )
            return res.token, hlocal.LocalHubCredential(
                instance_uid=instance_uid,
                protocol_epoch=protocol_epoch,
                gen=int(gen),
                jti=jti,
                writer_epoch=res.writer_epoch,
                expires_at_ms=res.exp_ms,
            )

        async def verify_hub_credential(token: str) -> hagones.HubCredentialClaims:
            """验签 + 范围校验一张 hub 回调凭据。对应 Go 的 `verifyHubCredential`。

            ★ 范围判据一条都不能省:验签只证明"这是我们签的",不证明"这是**hub**
              令牌"。少判 `ds_type` 就意味着一张 battle 令牌能拿来冒充 Hub DS。
            """
            assert ds_verifier is not None
            claims = ds_verifier.verify(token)
            if claims.ds_type != pauth.DS_TYPE_HUB:
                raise ValueError(f"ds_type mismatch: {claims.ds_type!r}")
            if claims.pod == "":
                raise ValueError("hub credential requires pod")
            if claims.match_id != 0:
                raise ValueError("hub credential must not carry match_id")
            if claims.exp_ms <= 0:
                raise ValueError("hub credential requires exp")
            return hagones.HubCredentialClaims(
                pod=claims.pod,
                instance_uid=claims.instance_uid,
                protocol_epoch=claims.protocol_epoch,
                gen=claims.gen,
                jti=claims.jti,
                exp_ms=claims.exp_ms,
                kid=claims.kid,
                writer_epoch=claims.writer_epoch,
            )

        async def verify_hub_ds_token(token: str, pod: str) -> None:
            """legacy 现存令牌复核:验签 + 绑定到该 pod。对应 Go 的 `SetDSTokenVerifier` 闭包。"""
            assert ds_verifier is not None
            claims = ds_verifier.verify(token)
            if claims.ds_type != pauth.DS_TYPE_HUB:
                raise ValueError(f"ds_type mismatch: {claims.ds_type!r}")
            if claims.pod != pod:
                raise ValueError(f"pod mismatch: token={claims.pod!r} want={pod!r}")

        if ds_signer is not None:
            logger.info(
                "ds_callback_token_issuer_ready",
                hub_token_ttl=godur.duration_string(hub_token_ttl),
                guard_mode=guard_mode,
            )

        # Model B「Redis 唯一授权权威」(§7)。三者齐备才算启用 —— 判据照抄 Go,
        # 缺任一项都退回 legacy 代际门(而不是"半个 Model B")。
        model_b_authority = (
            cfg.mode == hconf.MODE_AGONES
            and ds_enforce
            and cfg.ds_auth.authority_mode_redis()
        )

        # ── ⑧ 装配链 ─────────────────────────────────────────────────────────
        repo = hrepo.RedisHubRepo(rdb)
        hub_auth_repo = None
        agones_fleet = None
        local_fleet = None
        # Hub DS 分片来源由 cfg.mode 单一开关决定,biz 逻辑零改。
        fleet: hfleet.HubFleetProvider
        if cfg.mode == hconf.MODE_AGONES:
            try:
                agones_fleet = hagones.AgonesHubFleetProvider(cfg)
            except ValueError as exc:
                logger.error(
                    "agones_fleet_provider_init_failed",
                    err=str(exc),
                    hint="检查 agones.fleet_name / ca_path 配置",
                )
                return 1
            fleet = agones_fleet
            if ds_signer is not None:
                if model_b_authority:
                    # Model B:Redis 是授权唯一权威,凭据由 allocator 签、DS 心跳回带,
                    # fleet 负责在 annotation 上轮换并复核。
                    hub_auth_repo = hauthrepo.RedisHubAuthRepo(rdb)
                    agones_fleet.set_hub_authority(
                        hub_auth_repo,
                        issue_hub_credential,
                        verify_hub_credential,
                        hub_token_ttl / 3,
                        auth_record_ttl,
                    )
                    logger.info(
                        "hub_authority_model_b_ready",
                        auth_record_ttl=godur.duration_string(auth_record_ttl),
                    )
                else:
                    # legacy 代际门:只派发带 gen 的回调令牌。`required=ds_enforce` ——
                    # permissive 档下派发失败不该挡住分片翻 ready(否则灰度期切档
                    # 会把整个 fleet 冻住),enforce 档下必须挡。
                    agones_fleet.set_ds_token_issuer(
                        issue_hub_token, hub_token_ttl / 3, ds_enforce
                    )
                    if ds_verifier is not None:
                        agones_fleet.set_ds_token_verifier(verify_hub_ds_token)
            logger.info(
                "agones_fleet_provider_ready",
                fleet_name=cfg.agones.fleet_name,
                namespace=cfg.agones.namespace,
                model_b=model_b_authority,
                ds_enforce=ds_enforce,
            )
        elif cfg.mode == hconf.MODE_LOCAL:
            # local-off-v1:先校验姿态,再建 provider。顺序不能换 —— provider 一建
            # 起来就会往 env 播种凭据,姿态校验必须在**播种之前**拦住生产/灰度配置。
            try:
                pdsticket.validate_ds_local_hub_profile_off_v1(
                    guard_mode,
                    cfg.ds_auth.authority_mode,
                    ds_signer is not None,
                    hub_token_ttl,
                )
            except pdsticket.DSTicketConfigError as exc:
                logger.error("local_hub_auth_profile_invalid", err=str(exc))
                return 1
            try:
                local_fleet = hlocal.LocalHubFleetProvider(cfg.local_hub)
            except ValueError as exc:
                logger.error(
                    "local_hub_fleet_provider_init_failed",
                    err=str(exc),
                    hint="检查 local_hub.launcher / exe_path / map_name / port 配置;"
                         "editor 档还需 editor_path + uproject_path",
                )
                return 1
            fleet = local_fleet
            # required=True:本机档没有第二条准入通道,凭据签不出来就必须让分片起不来,
            # 否则 DS 拿不到凭据 → 准入租约拒 ACK → 玩家刚进大厅就被踢。
            local_fleet.set_ds_token_issuer(issue_local_hub_credential, True)
            logger.info(
                "local_hub_fleet_provider_ready",
                pod=local_fleet.pod_name,
                launcher=cfg.local_hub.launcher,
            )
        else:
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
            if owner_authority_enabled:
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
        if cfg.mode == hconf.MODE_AGONES:
            canary_percent, canary_seed = cfg.agones.canary_percent, cfg.agones.canary_seed
        try:
            release_policy = releasetrack.new(canary_percent, canary_seed)
        except ValueError as exc:
            logger.error("hub_release_track_policy_invalid", err=str(exc))
            return 1
        uc.set_release_track_policy(release_policy)
        logger.info("hub_release_track_policy_ready", canary_percent=canary_percent)

        # ── agones 真 DS 链路的两条额外收紧 ──────────────────────────────────
        if cfg.mode == hconf.MODE_AGONES:
            # warming→ready 必须见到**真实鉴权心跳**:mock 档没有真 DS 所以不设,
            # 真链路下不设就等于把一台还没起来的 DS 当成可分配。
            uc.set_require_heartbeat_ready(True)
            if model_b_authority and hub_auth_repo is not None:
                uc.set_auth_repo(hub_auth_repo)
                uc.set_auth_ttl(auth_record_ttl)
                # Model B 下代际门由授权记录接管:再开 legacy 代际门会让两套判据
                # 同时生效,轮换窗口里互相判对方的令牌"过期"。
                uc.set_ds_token_generation(False)
                logger.info(
                    "hub_usecase_model_b_authority",
                    auth_ttl=godur.duration_string(auth_record_ttl),
                )
            else:
                uc.set_ds_token_generation(ds_enforce)
            if not ds_enforce:
                logger.warning(
                    "agones_ds_auth_not_enforce",
                    guard_mode=guard_mode,
                    hint="真 DS 链路下非 enforce 档 = 回调令牌可缺可伪;仅允许灰度切档窗口",
                )

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

        # ── ⑨ dsauthfence capability(任何后台 reconcile/sweep 与 RPC server 之前)──
        #
        # 向 etcd 注册一份带租约的 capability,并守望失租:失租 / epoch 回退的旧副本
        # 必须**立即退出**,否则它会带着已经作废的写权继续写席位与容量账本
        # (§9.22 单写者直接破)。取不到 capability 时进程零业务写。
        #
        # ★ 位置是契约:必须排在 gRPC server 构造与后台扫描之前。顺序一换,
        #   "capability 还没到手就已经对外接 RPC"的窗口里,一个证明不了自己该写的
        #   副本照样在写。
        #
        # writerHealth 由下方写者租约启动后注入(`/healthz/writer` 只做观测,不参与
        # 流量门,理由见 `http.py` 模块头)。它必须在闸链之前造好,因为租约就绪点在
        # http_app 构造之前。
        writer_health = hhttp.WriterHealthHolder()
        writer_lease: writerlease.WriterLease | None = None
        # 激活钩子与租约一起定下来(而不是到后面再按 mode 现推):让"enforce 才给真推扫"
        # 这一条只有**一个**判据点。两处各判一次的话,改档位时漏改其中一处 = enforce 拿到
        # 空钩子(接流前硬门静默消失),而日志与 /healthz/writer 看起来完全正常。
        writer_on_elected: Callable[[int], Awaitable[None]] = _noop_on_elected
        if cfg.ds_auth.authority_mode_redis():
            # ★ features 只在 Model B 下声明(Go 同判据);etcd 侧是**精确相等**比较,
            #   多一个 / 少一个 / 拼错一个字母都不是"降级注册",而是直接注册不上。
            # ★ 身份不从 hostname / image tag 推:两者都可伪造、可漂移,而 capability
            #   key 的唯一性正建立在 PodUID 上(`acquire_runtime` 只认 Downward API)。
            features = HUB_AUTH_FENCE_FEATURES_MODEL_B if model_b_authority else ()
            try:
                fence = await dsauthfence.acquire_runtime(
                    dsauthfence.RuntimeConfig(
                        endpoints=list(cfg.ds_auth.fence.etcd_endpoints),
                        prefix=cfg.ds_auth.fence.etcd_prefix,
                        service=SERVICE_NAME,
                        keyset_revision=cfg.ds_auth.fence.keyset_revision,
                        writer_epoch=dsauthfence.PROTOCOL_EPOCH_V2,
                        features=features,
                        lease_ttl_sec=cfg.ds_auth.fence.etcd_lease_ttl_sec,
                        dial_timeout_sec=cfg.ds_auth.fence.etcd_dial_timeout_td().total_seconds(),
                    )
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error("ds_auth_fence_acquire_failed", err=str(exc))
                return 1
            if (
                fence.required_policy_generation()
                != dsauthfence.REQUIRED_POLICY_GENERATION_V3
            ):
                # 控制面还停在 V2:本二进制的 V3 feature 集只能作**激活审计**用,
                # 不能据此开写。这里不是"等一会儿再试"(那才是拿定时器掩盖时序),
                # 而是挂在权威的 Lost 事件上:V3 生效会让本 capability 失效并唤醒
                # 这一等待,进程干净退出、由编排重启后按新策略重新注册。
                logger.warning(
                    "ds_auth_fence_staging_only",
                    required_policy_generation=fence.required_policy_generation(),
                    required_policy=dsauthfence.REQUIRED_POLICY_V3,
                    hint="capability 仅供 V3 激活审计；V3 生效并触发 Lost/重启前禁止启动 RPC 与后台 writer",
                )
                await fence.lost.wait()
                await fence.close()
                fence = None
                return 0
            logger.info(
                "ds_auth_fence_ready",
                required_writer_epoch=fence.required_epoch(),
                reclaimed_stale_capability=fence.reclaimed,
            )

            # ── ⑨.1 写者继任租约(session-generation-rollout.md §5)────────────
            #
            # 把「单写者」从部署策略(Recreate)下沉为运行时协议:全体副本竞选同一
            # etcd election,仅当选副本可写(biz 入口 gate),存储层在同一 Redis 事务
            # 内比较 / 推进单调 fencing token(`writer_fence.py`),迟到旧写者零写入。
            # etcd 已是本模式硬依赖(上面的 capability),此处不新增依赖类别。
            #
            # 档位(rollout §5.4):enforce=稳态;warmup=只竞选不接线(引导升级第一跳);
            # off=不启动(仅历史 Recreate 单副本)。非法值 fail-fast。
            try:
                writer_mode = cfg.hub.resolve_writer_lease_mode()
            except ValueError as exc:
                logger.error("hub_writer_lease_mode_invalid", err=str(exc))
                return 1
            if not _check_writer_lease_deploy_strategy(logger, writer_mode):
                return 1
            if writer_mode == hconf.WRITER_LEASE_OFF:
                logger.warning(
                    "hub_writer_lease_disabled",
                    hint="writer_lease_mode=off:单写者不再由运行时协议保证,只允许单副本 "
                         "Recreate 部署;RollingUpdate 下必须改回 enforce",
                )
            else:
                identity = _writer_identity()

                async def _advance_fences_on_elected(token: int) -> None:
                    """接流前硬门(Go 的 `leaseCfg.OnElected`)。

                    当选后先把**全部已知 pod** 的 fence 水位推进到本届 token,成功才
                    宣告持有领导权。挂在后台 sweep tick 上懒执行是不行的:那样
                    "当选即接写、推扫尚未完成"的窗口里,前任在未被触碰的 {pod} slot
                    上仍能写。钩子失败 = 让位重选,本副本恒不持有(写请求继续可重试拒绝)。

                    ★ 用 `advance_writer_fences_for_token` 而不是无参版:此刻
                      `current()` 故意还不返回 held —— 推扫成功是**获得**写权的前置
                      条件,不能反过来依赖写权(那是循环依赖,钩子永远跑不起来)。
                    """
                    await repo.advance_writer_fences_for_token(token)

                # ★ lease_ttl_sec 只在配了正值时传:传 0 会被当成"TTL 为零"而不是
                #   "用默认",而 TTL 为零的租约等于**永远持不住**,enforce 档下写路径
                #   将永久拒写(表现为 AssignHub 恒可重试失败)。
                lease_kwargs: dict[str, int] = {}
                if cfg.ds_auth.fence.etcd_lease_ttl_sec > 0:
                    lease_kwargs["lease_ttl_sec"] = cfg.ds_auth.fence.etcd_lease_ttl_sec
                try:
                    writer_lease = writerlease.WriterLease(
                        list(cfg.ds_auth.fence.etcd_endpoints),
                        WRITER_LEASE_ELECTION,
                        **lease_kwargs,
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    logger.error("hub_writer_lease_start_failed", err=str(exc))
                    return 1
                if writer_mode == hconf.WRITER_LEASE_ENFORCE:
                    # warmup 档**刻意不注入**:只竞选、观测 token 单调,不改写路径。
                    # 三处注入缺一不可 —— biz 入口 gate、分片 / 席位存储、Model B
                    # 授权记录各自独立走 fenced 事务,漏掉任何一处,前任就还剩一条
                    # 没上锁的写路径。
                    uc.set_writer_fence(writer_lease)
                    repo.set_writer_fence(writer_lease)
                    if hub_auth_repo is not None:
                        hub_auth_repo.set_writer_fence(writer_lease)
                    writer_on_elected = _advance_fences_on_elected
                writer_health.set(writer_lease, writer_mode)
                logger.info(
                    "hub_writer_lease_started",
                    election=WRITER_LEASE_ELECTION,
                    identity=identity,
                    mode=writer_mode,
                    hint="enforce:未当选副本拒写(可重试)+接流前推扫硬门+存储级 fencing;"
                         "warmup:只竞选观测 token 单调,不改写路径",
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
        # 写者继任租约的健康面(Go `server.NewHTTPServer(&cfg, writerHealth)`):
        # `/healthz/writer` + 抓取即刷新的 `/metrics`。
        #
        # ★ **无条件**挂上,不只在 Model B 下挂:租约未注入时 snapshot() 明确回
        #   `enabled=false` 且 gauge 归零,而不是乐观的 `held=true`。少挂的话
        #   "全集群没有写者"这类静默故障连一个可查的端点都没有。
        hhttp.install(http_app, writer_health)

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
            # 必须挂 uc.run_heartbeat_sweep,不能挂 uc.sweep_once:sweep_once 只是那一拍里
            # 的第 3 步(过期分片转 draining);reconcile_owner_cleanups /
            # reconcile_shard_topology / reconcile_fleet_replicas 以及写者门控全都在
            # heartbeat_sweep_tick 里,而 run_heartbeat_sweep 才是它的驱动器(自带
            # safego.loop + _SweepState,以及与 Go 逐字节一致的
            # hub_heartbeat_sweep_started/stopped 事件)。
            #
            # 挂成 sweep_once 的后果:本机 Hub DS 永远不被拉起 —— 它靠
            # reconcile_shard_topology -> list_shards -> ensure_started 这条懒拉起链;
            # 而且写者门控也在 tick 里,整条链失效时一条日志都不会打,表现为 allocator
            # 一切正常、一键启动却恒报「90s 内没拉起 Hub DS」(2026-08-24 事故)。
            # ds_allocator/biz_sweep.py:355 一直是对的写法。
            await uc.run_heartbeat_sweep()

        background: list = [heartbeat_sweep]
        if fence is not None:
            # 失租守望必须与 sweep 同生命周期:它是 capability 失效到进程退出之间
            # 唯一的传导路径,漏挂等于把 fail-closed 退化成 fail-open。
            background.append(("hub_auth_fence_lost_watch", lambda: _watch_fence_lost(fence)))
        if writer_lease is not None:
            background.append(("hub_writer_lease", lambda: writer_lease.run(writer_on_elected)))

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
        # 关闭顺序与 Go 的 defer 栈同序(后进先出):
        # capability → locator → owner → kafka → redis。
        #
        # ★ capability 排最前:它是"本副本还有没有写权"的唯一凭证,必须在任何
        #   其它资源被拆掉之前主动交还,好让继任副本尽早接管(否则要空等一个 TTL)。
        if fence is not None:
            with contextlib.suppress(Exception):
                await fence.close()
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
