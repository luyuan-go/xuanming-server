"""ds_allocator 服务入口 —— 对应 Go 侧
`services/battle/ds_allocator/cmd/ds_allocator/main.go`(694 行,主体),
`services/battle/ds_allocator/cmd/ds_allocator/pod_uid_preflight.go`(224 行)与
`services/battle/ds_allocator/cmd/ds_allocator/pod_uid_config_compare.go`(65 行)。

职责:战斗 DS 调度。matchmaker 全员确认后调 AllocateBattle 申请 DS,
战斗 DS 每 5s 调 Heartbeat 续命,心跳超时由后台扫描标记 abandoned。

★ 本文件是一条**线性启动闸链**,顺序照抄 Go,不是形式主义:
  后面的闸依赖前面已经确立的前提。举两个真会出事的例子 ——
    · Model B 的 token TTL 下限校验读的是 signer 的配置,必须排在签发器构造之后;
      提前判定会拿空配置当 TTL,于是任何配置都"通过"。
    · 写者继任租约的 `on_elected` 必须排在 gRPC server 起来之前;顺序一换,
      "当选即接写、推扫尚未完成"的窗口里前任仍能写(§9.22 单写者破)。

★ 关于「fail-fast vs. warn-only」:方向弄反两种都是事故,但**不是同一种事故** ——
    该拒的没拒 = 带病上线(权威面已不可信,服务却看起来完全正常);
    该 warn 的却拒了 = 把配置问题升级成可用性事故。
  因此下面每一条闸的档位都逐条对着 Go 抄,注释里写明"挡的是什么"。

★ 缺配置一律 fail-fast,**不静默降级**:任何一道安全闸(DS 回调令牌签发 / 校验、
  DSTicket、dsauthfence capability 租约、写者继任租约、AllocationAbort 服务间验签)
  缺了必需配置都不能"当成没配置"继续跑 —— 那会让 agones / Model B / enforce 档
  变成"看起来在跑、实际零鉴权"。错误信息说的必须是**缺了哪一项配置**,而不是
  "某某能力没实现":后者会让运维去改部署方式,而真正的修法是补一行 yaml。

★ 三档 allocator(agones / local / mock)、DSTicket v2 签发、DS 回调令牌签发、
  capability 租约与写者继任租约均已接线,`mode`/`ds_auth.mode`/`authority_mode`
  三个开关的所有合法组合都是完整可用实现,没有"打开后是空壳"的分支(§14)。

★ Pod UID preflight / config compare 两条独立入口(Go 的两个单独文件)并入本文件
  末尾,判据是 `args.pod_uid_release_preflight` 与 `args.pod_uid_release_preflight_compare_configs`。
  Python 不用单独文件的原因:三条路径都跑不到 gRPC server,且只在严格 Model-B
  激活 Job 里跑,保持一个文件避免漏改。
"""

from __future__ import annotations

# ★ 必须最先 import:Windows 控制台默认 cp1252,日志里的中文会抛 UnicodeEncodeError,
# 把真正的启动错误顶掉(实测踩过多次)。
from pandorapy import _utf8  # noqa: F401  isort:skip
from pandorapy import config as pconfig

import argparse
import asyncio
import base64
import contextlib
import json
import os
import pathlib
import re
import sys
import uuid as _uuid

from pandora.config.v1 import configtable_pb2_grpc as cfggrpc
from pandora.ds.v1 import allocator_pb2_grpc as dsgrpc
from pandora.gm.v1 import gm_pb2_grpc as gmgrpc

from pandorapy import auth as pauth
from pandorapy import dsauth
from pandorapy import dsauthfence
from pandorapy import dsticket
from pandorapy import errcode
from pandorapy import godur
from pandorapy import internalrpcauth
from pandorapy import kafka_topics
from pandorapy import kafkax
from pandorapy import log as plog
from pandorapy import redisx
from pandorapy import releasetrack
from pandorapy import safego
from pandorapy import server as pserver
from pandorapy import writerlease

from pandorapy.services.ds_allocator import battle_auth as dsbattleauth
from pandorapy.services.ds_allocator import biz
from pandorapy.services.ds_allocator import capacity as dscapacity
from pandorapy.services.ds_allocator import catalog
from pandorapy.services.ds_allocator import clients as dsclients
from pandorapy.services.ds_allocator import conf as dsconf
from pandorapy.services.ds_allocator import configtable_admin
from pandorapy.services.ds_allocator import departure as dsdeparture
from pandorapy.services.ds_allocator import gameserver as dsgameserver
from pandorapy.services.ds_allocator import gm as dsgm
from pandorapy.services.ds_allocator import http as dshttp
from pandorapy.services.ds_allocator import repo as dsrepo
from pandorapy.services.ds_allocator import service as dssvc
from pandorapy.services.ds_allocator.agones_allocator import AgonesGameServerAllocator
from pandorapy.services.ds_allocator.local_allocator import (
    BattleCredentialIdentity,
    LocalGameServerAllocator,
)
from pandorapy.services.ds_allocator.poduidpreflight import (
    CANONICAL_READ_ONLY_USERNAME,
    AuditSummary,
    RedisTargetIdentity,
    audit_redis,
    compare_redis_config_yaml,
    identify_redis_config,
    prove_read_only_and_identify,
    valid_target_identity,
)

SERVICE_NAME = "ds_allocator"
HTTP_DEFAULT_PORT = 21020
GRPC_SERVICE_FULL_NAME = "pandora.ds.v1.DSAllocatorService"

# ── dsauthfence capability 契约(Go `main.go` 的 `Features: []string{...}`)─────
#
# ★ 逐字照抄。feature 集合在 etcd 侧是**精确相等**比较(见 dsauthfence
#   `equal_feature_set`),多一个 / 少一个 / 拼错一个字母都不是"降级注册",而是
#   capability 直接注册不上 → 进程 fail-closed 退出。
DS_AUTH_FENCE_FEATURES = (
    "battle-release-expected-tuple-v1",
    "battle-storage-pod-uid-write-invariant-v1",
)

# ── 写者继任租约(Go `main.go` 5.1 段)────────────────────────────────────────
#: etcd election 名。两个实现必须同名,否则灰度期 Go 副本与 Python 副本各选各的
#: leader,"单扫描者"这条前提在混跑窗口里静默失效。
DS_WRITER_LEASE_ELECTION = "ds_allocator/sweep"
#: 部署策略 annotation 注入的 env(Go: `PANDORA_DEPLOY_STRATEGY`)。
DEPLOY_STRATEGY_ENV = "PANDORA_DEPLOY_STRATEGY"
#: 受管 k8s 判据(Go: `KUBERNETES_SERVICE_HOST`)。
KUBERNETES_SERVICE_HOST_ENV = "KUBERNETES_SERVICE_HOST"

#: local-off-v1 一局一进程,实例内不换令牌,故 gen 恒 1(Go `localBattleGen`)。
#: 它自洽的前提是"不接 Redis pending→active 提升",所以不需要 Redis 单调计数器。
LOCAL_BATTLE_GEN = 1

# ── matchmaker → allocator 的 AllocationAbort 服务间凭据(Go `main.go` 的三个字面量)──
#
# ★ 这是一个**独立信任域**:只用于 matchmaker 的 pre-admission abort 这一条破坏性
#   RPC,与玩家 JWT、placement proof、login resume auth、DS 回调凭据刻意互不复用。
#   任何一套被复用,单点泄漏的爆炸半径就会外溢到另外三条链。
#: nonce 键前缀(与 Go 逐字节一致:换前缀 = 灰度期两栈各记各的 nonce,重放窗口失效)。
ALLOCATION_ABORT_NONCE_PREFIX = "pandora:ds-allocator:allocation-abort:nonce:"
#: 唯一被接受的调用方身份(Go: `"matchmaker"`)。
ALLOCATION_ABORT_CALLER = "matchmaker"
#: 允许的最大时钟偏差(Go: `30*time.Second`)。verifier 据此推 nonce TTL,不能随手放大。
ALLOCATION_ABORT_MAX_CLOCK_SKEW_SEC = 30.0

# ── Pod UID preflight 环境变量(Go `pod_uid_preflight.go` 的常量)─────────────────
POD_UID_PREFLIGHT_REDIS_USERNAME_ENV = "PANDORA_POD_UID_PREFLIGHT_REDIS_USERNAME"
POD_UID_PREFLIGHT_REDIS_PASSWORD_ENV = "PANDORA_POD_UID_PREFLIGHT_REDIS_PASSWORD"
MINIMUM_POD_UID_PREFLIGHT_REDIS_PASSWORD_BYTES = 32

# ── Pod UID preflight 正则(Go `pod_uid_preflight.go` 的两个编译正则)──────────────
_PREFLIGHT_RUN_ID_PATTERN = re.compile(r"\A[a-z0-9][a-z0-9-]{7,23}\Z")
_PREFLIGHT_DIGEST_PATTERN = re.compile(r"\Asha256:[0-9a-f]{64}\Z")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog=SERVICE_NAME, add_help=True)
    # 与 Go 侧同名:`-conf`(单横线),这样两个实现的启动命令完全一样。
    ap.add_argument("-conf", dest="conf", default="etc/ds_allocator-dev.yaml")
    # Pod UID preflight 独立入口标志(对应 Go 的 flagPodUIDReleasePreflight 等)。
    ap.add_argument(
        "--pod-uid-release-preflight",
        dest="pod_uid_release_preflight",
        action="store_true",
        help="run the read-only Model-B legacy pod_uid release audit and exit",
    )
    ap.add_argument(
        "--pod-uid-release-preflight-timeout",
        dest="pod_uid_release_preflight_timeout",
        type=float,
        default=600.0,
        help="hard deadline for the read-only Model-B pod_uid audit (seconds)",
    )
    ap.add_argument(
        "--pod-uid-release-preflight-scan-count",
        dest="pod_uid_release_preflight_scan_count",
        type=int,
        default=1000,
        help="Redis SCAN count hint per master for the Model-B pod_uid audit",
    )
    ap.add_argument(
        "--pod-uid-release-preflight-run-id",
        dest="pod_uid_release_preflight_run_id",
        default="",
        help="immutable strict activation run identity",
    )
    ap.add_argument(
        "--pod-uid-release-preflight-phase",
        dest="pod_uid_release_preflight_phase",
        default="",
        help="strict activation phase: prepare, drained or final",
    )
    ap.add_argument(
        "--pod-uid-release-preflight-image-digest",
        dest="pod_uid_release_preflight_image_digest",
        default="",
        help="immutable image digest bound by the activation Job contract",
    )
    ap.add_argument(
        "--pod-uid-release-preflight-expected-target-identity",
        dest="pod_uid_release_preflight_expected_target_identity",
        default="",
        help="prepare leaves empty; drained/final must bind the prepare Redis target identity",
    )
    # Config comparison 独立入口标志(Go 的 flagPodUIDReleasePreflightCompareConfigs)。
    ap.add_argument(
        "--pod-uid-release-preflight-compare-configs",
        dest="pod_uid_release_preflight_compare_configs",
        action="store_true",
        help="read two base64 YAML snapshots from stdin, compare safe Redis target identity and exit",
    )
    return ap.parse_args(argv)


def _validate_preflight_evidence(
    run_id: str, phase: str, image_digest: str, expected_target: str
) -> bool:
    """校验 Pod UID preflight 证据字段完整性(对应 Go 的 `podUIDPreflightEvidence.valid()`)。"""
    if not _PREFLIGHT_RUN_ID_PATTERN.match(run_id):
        return False
    if not _PREFLIGHT_DIGEST_PATTERN.match(image_digest):
        return False
    if phase == "prepare":
        return expected_target == ""
    elif phase in ("drained", "final"):
        return valid_target_identity(expected_target)
    else:
        return False


def _load_pod_uid_preflight_redis_credentials() -> tuple[str, str]:
    """从环境变量加载 preflight 专用 Redis 凭据(Go `loadPodUIDPreflightRedisCredentials`)。

    Raises:
        ValueError: 凭据缺失/不符合
    """
    username = os.environ.get(POD_UID_PREFLIGHT_REDIS_USERNAME_ENV, "")
    password = os.environ.get(POD_UID_PREFLIGHT_REDIS_PASSWORD_ENV, "")
    if username != CANONICAL_READ_ONLY_USERNAME:
        raise ValueError(
            f"{POD_UID_PREFLIGHT_REDIS_USERNAME_ENV} must be the canonical dedicated read-only identity"
        )
    if len(password) < MINIMUM_POD_UID_PREFLIGHT_REDIS_PASSWORD_BYTES or "\x00" in password:
        raise ValueError(
            f"{POD_UID_PREFLIGHT_REDIS_PASSWORD_ENV} must provide a dedicated high-entropy Secret credential of at least {MINIMUM_POD_UID_PREFLIGHT_REDIS_PASSWORD_BYTES} bytes"
        )
    return username, password


async def _run_pod_uid_release_preflight(
    rc: pconfig.RedisConf,
    scan_count: int,
    run_id: str,
    phase: str,
    image_digest: str,
    expected_target: str,
    username: str,
    password: str,
) -> tuple[bool, str]:
    """执行 Pod UID release preflight 审计(Go `runPodUIDReleasePreflight`)。

    返回 (通过?, 输出消息)。通过时输出写 PASSED 行,失败时输出写 FAILED + findings。
    """
    if scan_count <= 0:
        raise ValueError("pod_uid release preflight requires positive scan count")
    if not _validate_preflight_evidence(run_id, phase, image_digest, expected_target):
        raise ValueError(
            "pod_uid release preflight requires canonical run_id, prepare/drained/final phase, image digest and phase target binding"
        )
    if username != CANONICAL_READ_ONLY_USERNAME or len(
        password
    ) < MINIMUM_POD_UID_PREFLIGHT_REDIS_PASSWORD_BYTES:
        raise ValueError(
            "pod_uid release preflight requires dedicated read-only Redis credentials"
        )
    if rc.host == "" and not rc.addrs:
        raise ValueError("ds_allocator Redis endpoint is not configured")
    if rc.password != "":
        raise ValueError(
            "pod_uid release preflight config must not contain the writer Redis password"
        )
    if rc.maint_notifications != "disabled":
        raise ValueError(
            "pod_uid release preflight config must set Redis maint_notifications=disabled"
        )

    config_identity = identify_redis_config(rc)
    rdb = redisx.new_universal_client_with_credentials(rc, username, password)
    try:
        await rdb.ping()
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001
        await rdb.aclose()
        raise ValueError(f"Redis ping failed: {exc}") from exc

    try:
        # 显式标注:两次 identify 的返回值必须是**同一种**身份视图,`after != before`
        # 那一步的相等判定才有意义(拿到别的类型时相等恒 False,会把一次正常审计
        # 判成"审计期间目标身份变了")。
        before: RedisTargetIdentity = await prove_read_only_and_identify(rdb, rc, username)
        if expected_target != "" and before.digest != expected_target:
            raise ValueError("Redis target identity differs from the prepare evidence")
        summary = AuditSummary()
        await audit_redis(rdb, scan_count, summary)
        if summary.masters_visited == 0:
            raise ValueError("Redis audit visited zero masters")
        scan_master_set_digest = summary.runtime_master_set_digest()
        if (
            summary.masters_visited != before.nodes
            or scan_master_set_digest != before.master_set_digest
        ):
            raise ValueError("Redis audit did not scan the exact preflight master identity set")
        after = await prove_read_only_and_identify(rdb, rc, username)
        if after != before:
            raise ValueError(
                "Redis target identity changed during the audit; start a new activation evidence chain"
            )
        if scan_master_set_digest != after.master_set_digest:
            raise ValueError("Redis audit master identity set differs from post-audit topology")

        summary.sort_findings()
        findings_str = ""
        for finding in summary.findings:
            findings_str += (
                f"UNSAFE source={finding.source!r} key={finding.key!r} match_id={finding.match_id} reason={finding.reason!r}\n"
            )

        if summary.findings:
            msg = (
                f"pod_uid release preflight FAILED: run_id={run_id} phase={phase} image_digest={image_digest} "
                f"redis_config_identity={config_identity.digest} redis_target_identity={before.digest} "
                f"redis_topology={before.topology} redis_acl_user={username} "
                f"visited_masters={summary.masters_visited} visited_keys={summary.keys_visited} "
                f"decoded_records={summary.records_decoded} allocation_uncertain={summary.allocation_uncertain} "
                f"findings={len(summary.findings)}; no data was modified\n{findings_str}"
            )
            return False, msg
        else:
            msg = (
                f"pod_uid release preflight PASSED: run_id={run_id} phase={phase} image_digest={image_digest} "
                f"redis_config_identity={config_identity.digest} redis_target_identity={before.digest} "
                f"redis_topology={before.topology} redis_acl_user={username} "
                f"visited_masters={summary.masters_visited} visited_keys={summary.keys_visited} "
                f"decoded_records={summary.records_decoded} allocation_uncertain={summary.allocation_uncertain} "
                f"findings=0; no data was modified\n"
            )
            return True, msg
    finally:
        await rdb.aclose()


def _run_pod_uid_config_compare_sync(stdin_text: str) -> tuple[bool, str]:
    """Pod UID config comparison(Go `runPodUIDConfigCompare`)。

    Returns:
        (matched?, output_json)
    """
    try:
        input_obj = json.loads(stdin_text)
    except json.JSONDecodeError as exc:
        raise ValueError("pod_uid config comparison input is invalid") from exc

    writer_config_base64 = input_obj.get("writer_config_base64", "")
    read_only_config_base64 = input_obj.get("read_only_config_base64", "")
    try:
        writer_bytes = base64.b64decode(writer_config_base64, validate=True)
    except Exception as exc:  # noqa: BLE001
        raise ValueError("writer config snapshot is not canonical base64") from exc
    if not writer_bytes:
        raise ValueError("writer config snapshot is not canonical base64")
    try:
        read_only_bytes = base64.b64decode(read_only_config_base64, validate=True)
    except Exception as exc:  # noqa: BLE001
        raise ValueError("read-only config snapshot is not canonical base64") from exc
    if not read_only_bytes:
        raise ValueError("read-only config snapshot is not canonical base64")

    identity = compare_redis_config_yaml(writer_bytes, read_only_bytes)
    return True, json.dumps(
        {
            "matched": True,
            "redis_config_identity": identity.digest,
            "redis_topology": identity.topology,
        }
    )


def _local_map_source(loader_map: str, config_table_ready: bool) -> str:
    """只用于启动日志:说清 mode=local 这台 allocator 的关卡到底由谁查表决定,
    免得下一个人再去 yaml 里找那张已经删掉的 maps 映射。

    对应 Go `cmd/ds_allocator/main.go:localMapSource`,三个返回值逐字节一致
    (跨栈 grep 同一条 `map_source=` 日志时不能有两种写法)。
    """
    if loader_map.strip() != "":
        return "loader_map(DS 侧 Loader GameMode 查 g_关卡.xlsx)"
    if config_table_ready:
        return "config_table(allocator 现查 g_关卡.xlsx)"
    return "none"


class DSLifecyclePusher:
    """把 `biz.DSLifecyclePusher` 适配到 `kafkax.KeyOrderedProducer`。

    对应 Go `cmd/ds_allocator/main.go:dsLifecyclePusher`,同样定义在启动入口而不是
    repo 层 —— 它只是 producer 的一层薄壳,没有独立的存储语义。

    ★ key = match_id 的十进制字符串(§9 不变量 9:同一对局事件必须落同一分区保序),
      与 Go 的 `strconv.FormatUint(evt.GetMatchId(), 10)` 逐字节一致。
    ★ 序列化失败 / 投递失败一律**抛出**,由 `sweep_once` 把对局留在 active ZSET 下轮
      重试(Go 那侧是 `return err`);这里吞异常会让 abandoned 事件静默丢失。
    """

    __slots__ = ("_producer",)

    def __init__(self, producer: kafkax.KeyOrderedProducer) -> None:
        self._producer = producer

    async def publish_lifecycle(self, evt) -> None:  # noqa: ANN001
        await self._producer.send_raw(str(evt.match_id), evt.SerializeToString())


def _exit_process() -> None:
    """capability 失租 / 旧 epoch 时的 fail-stop —— 对应 Go 那两处 `os.Exit(1)`。

    ★ 用 `os._exit` 而不是 `sys.exit`:此刻身处一个后台 task 里,`sys.exit` 只会让
      **那个 task** 结束,进程照跑、gRPC 照答 SERVING —— 而"进程还在但已经证明不了
      自己该写"正是双写者窗口本身(§9.22)。
    ★ 独立成模块级函数只为可测:测试把它替换掉,否则真调 os._exit 会让 pytest 当场
      消失且不产生任何报告(auction/market_locker.py 踩过同一个坑,同样的写法)。
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
        hint="立即退出，禁止失租/旧 epoch allocator 继续分配或接收 DS 写回",
    )
    _exit_process()


async def _noop_on_elected(_token: int) -> None:
    """写者继任租约的空激活钩子 —— 对应 Go 的 `// 无 OnElected`。

    ds_allocator 的 sweep **不携带跨轮次权威意图**:每轮从 active ZSET 重读,动作是
    按 match 凭据 CAS,防脑裂由那把 CAS 承担。接任时没有任何 fence 水位需要推进,
    所以这里刻意什么都不做 —— 不是"以后再补",而是 Go 就没有这一步。
    """
    return None


def _check_writer_lease_deploy_strategy(logger, lease_mode: str) -> bool:  # noqa: ANN001
    """部署策略机械门禁 —— 对应 Go `main.go` 5.1 段里那段 strategy 判定。返回是否放行。

    进程看不到 `spec.strategy`,由 Deployment 把策略作为 annotation 注入 env
    (`PANDORA_DEPLOY_STRATEGY`);Go 侧另有清单契约测试钉住 annotation 与真实
    strategy 一致。

    两档刻意不同,方向弄反两种都是事故:
      · 受管 k8s 内 + env 缺失 → **fail-closed 退出**(清单回归必须炸,不能靠人看日志);
      · 非 k8s(本机裸跑 / dev)+ env 缺失 → 只告警(阻断会把开发环境一起打死)。

    不设这道闸会怎样:RollingUpdate × 非 enforce = 滚动重叠期新旧副本都在扫描,
    同一局被两个副本并行判弃 / 并行回收。
    """
    strategy = (os.environ.get(DEPLOY_STRATEGY_ENV) or "").strip()
    in_managed_k8s = bool((os.environ.get(KUBERNETES_SERVICE_HOST_ENV) or "").strip())
    if strategy:
        if (
            strategy.lower() == "rollingupdate"
            and lease_mode != dsconf.WRITER_LEASE_ENFORCE
        ):
            logger.error(
                "ds_writer_lease_rollingupdate_without_enforce",
                strategy=strategy,
                mode=lease_mode,
                hint="RollingUpdate × writer_lease_mode!=enforce = 滚动重叠期出现并发心跳扫描者;"
                "要么把 allocator.writer_lease_mode 改 enforce,要么把 Deployment 改回单副本 Recreate",
            )
            return False
        logger.info(
            "ds_writer_lease_strategy_checked", strategy=strategy, mode=lease_mode
        )
        return True
    if in_managed_k8s:
        logger.error(
            "ds_writer_lease_strategy_annotation_missing",
            mode=lease_mode,
            hint="受管 k8s 内必须注入 PANDORA_DEPLOY_STRATEGY(取自 Deployment 的 "
            "pandora.dev/deploy-strategy annotation);缺失则无法机械校验 RollingUpdate×非 enforce "
            "的并发扫描组合,fail-closed 退出。见 deploy/k8s/services/services.yaml",
        )
        return False
    logger.warning(
        "ds_writer_lease_strategy_unknown",
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
        cfg = dsconf.Config.load(str(conf_path))
    except pconfig.ConfigLoadError as exc:
        logger.error("config_load_failed", err=str(exc), path=str(conf_path))
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.error("config_scan_failed", err=str(exc))
        return 1

    # ── Pod UID preflight 独立入口(在服务配置校验之前退出)────────────────────
    if args.pod_uid_release_preflight:
        if (
            args.pod_uid_release_preflight_timeout <= 0
            or args.pod_uid_release_preflight_scan_count <= 0
        ):
            logger.error(
                "pod_uid_release_preflight_flags_invalid",
                timeout=args.pod_uid_release_preflight_timeout,
                scan_count=args.pod_uid_release_preflight_scan_count,
            )
            return 2
        try:
            username, password = _load_pod_uid_preflight_redis_credentials()
        except ValueError as exc:
            logger.error("pod_uid_release_preflight_redis_credentials_invalid", err=str(exc))
            return 1
        try:
            passed, msg = await asyncio.wait_for(
                _run_pod_uid_release_preflight(
                    cfg.node.redis_client,
                    args.pod_uid_release_preflight_scan_count,
                    args.pod_uid_release_preflight_run_id,
                    args.pod_uid_release_preflight_phase,
                    args.pod_uid_release_preflight_image_digest,
                    args.pod_uid_release_preflight_expected_target_identity,
                    username,
                    password,
                ),
                timeout=args.pod_uid_release_preflight_timeout,
            )
        except asyncio.TimeoutError:
            logger.error(
                "pod_uid_release_preflight_timeout",
                timeout=args.pod_uid_release_preflight_timeout,
            )
            return 1
        except ValueError as exc:
            logger.error("pod_uid_release_preflight_failed", err=str(exc))
            return 1
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            logger.error("pod_uid_release_preflight_failed", err=str(exc))
            return 1
        # 结果只写 stdout/stderr,不进日志(activation controller 消费这些机器 JSON)。
        if passed:
            print(msg, end="")  # noqa: T201
            return 0
        else:
            print(msg, end="", file=sys.stderr)  # noqa: T201
            return 1

    # ── ③ 配置闸链(Go 的 ①②④⑤⑥)─────────────────────────────────────────────
    #
    # ★ 判据**不**在 main 里重写一遍,而是逐条调 `conf` 上已经对着 Go 实现好的方法 ——
    #   在这里照抄一份的话,两处判据一旦漂移,"哪一份才是真的"就没人说得清了。
    #
    # ★ 但**必须逐条单独调**,不能图省事调聚合的 `validate_conf()`:每条闸对应一个
    #   独立事件名,而事件名是 Loki 告警与运维手册的入口。全部归成一个
    #   `config_validation_failed`,排障方向从第一步就错。
    for gate, event in (
        (cfg.ds_auth.validate_redis_fence, "ds_auth_fence_config_invalid"),
        (cfg.validate_lifecycle_publication_config, "ds_lifecycle_config_invalid"),
        (cfg.validate_battle_departure_config, "battle_departure_config_invalid"),
        (cfg.validate_allocation_abort_auth_config, "allocation_abort_auth_config_invalid"),
        (cfg.validate_local_map_source_config, "local_map_source_config_invalid"),
        (cfg.validate_roster_join_deadline_config, "roster_join_deadline_config_invalid"),
    ):
        try:
            gate()
        except ValueError as exc:
            logger.error(event, err=str(exc))
            return 1

    # ── ③.5 配置表(不变量 §9.15)─────────────────────────────────────────────
    # config_table.dir 配置后是启动强依赖,加载失败直接退出。
    # ds_allocator 只用其中的关卡表:mode=local 起 DS 时按 map_id 现查 g_关卡.xlsx 拼关卡 URL
    # (asset_path + game_mode_class),取代 2026-08-04 之前那张手抄的 local_ds.maps 影子表。
    # mode=agones 不读它(关卡由 DS 侧 Loader GameMode 查同一张表决定),留空即可。
    #
    # ★ 位置照抄 Go 的 2.5:必须排在 allocator 装配**之前** —— mode=local 的
    #   `set_map_url_resolver` 要拿它;顺序一换,local DS 就只能起默认图,随后被
    #   DS 侧关卡门判 Mismatch 自杀,表现成"玩家一直排队中"。
    ct_store: catalog.Store | None = None
    ct_dir = cfg.config_table.dir.strip()
    if ct_dir:
        try:
            res = catalog.load_tables(ct_dir, 0)
            # 批次级校验器:关卡表里**每一张**战斗类关卡都必须能构造出合法启动 URL。
            # 启动首载与之后每次热 reload 走同一门禁,坏批次整批不切换、保留旧表——
            # 把"某张图资源列填错"挡在加载边界,而不是等玩家恰好选中那张图才炸。
            catalog.validate_battle_launch_urls(res.tables)
        except catalog.ConfigTableError as exc:
            logger.error("configtable_load_failed", dir=ct_dir, err=str(exc))
            return 1
        ct_store = catalog.Store(res.tables, ct_dir)
        ct_store.add_validator(catalog.validate_battle_launch_urls)
        for w in res.warnings:
            logger.warning("configtable_load_warning", warning=w)
        logger.info(
            "configtable_loaded",
            dir=ct_dir,
            version=res.version,
            levels=ct_store.tables.level_count(),
        )

    # ── ④ Redis(强依赖:DS 状态镜像)─────────────────────────────────────────
    # 单实例填 host,Redis Cluster / Sentinel 只填 addrs,两者皆空才算未配置。
    rc = cfg.node.redis_client
    if rc.host == "" and not rc.addrs:
        logger.error(
            "redis_endpoint_required",
            hint="set node.redis_client.host (single) or node.redis_client.addrs (cluster)",
        )
        return 1
    # ★ 凡是 `finally` 里要善后的句柄,一律在 `try` **之前**声明。
    #   下面每一道闸都可能 `return 1` 早退,而 finally 引用一个尚未赋值的局部
    #   会抛 UnboundLocalError —— 把"配置错了"这条已经打清楚的失败翻译成一条看不懂
    #   的崩溃,且真正的错误日志已经先打出去了,排障时两条对不上。
    fence: dsauthfence.Holder | None = None
    local_alloc: LocalGameServerAllocator | None = None  # 仅 local 非空,退出时收孤儿 DS
    owner_lease = None
    producer = None
    location_refresher = None
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

        # ── ⑤ DS 回调服务令牌:签发器 + 守卫(Go `main.go` 4.0 段)──────────
        #
        # 两个方向、两套判据,不能合并:
        #   · **签发**看的是 `ds_auth.secret`(secret 未配 → signer=None)。签不签跟
        #     mode 无关 —— ds_allocator 即使 mode=off 也照样要给 DS 签令牌下发,
        #     否则灰度期把 mode 切成 enforce 的那一刻,全部在场 DS 手上没有令牌 → 成批被拒。
        #   · **校验**看的是 `ds_auth.mode`(off → guard=None,等价"什么都放行")。
        #
        # 判据按**谁真的会用到**分,不是按"配没配":mock 档两样都用不到
        # (Mock provider 不派发也不验票),yaml 里留了个用不上的 ds_auth.secret
        # 也不该让它起不来 —— 那是把"配置里有个用不上的字段"升级成可用性事故。
        try:
            ds_signer = dsauth.signer_from_conf(cfg.ds_auth)
        except (ValueError, pauth.TokenError) as exc:
            logger.error("ds_auth_signer_init_failed", err=str(exc))
            return 1
        try:
            ds_guard = dsauth.guard_from_conf(cfg.ds_auth)
        except ValueError as exc:
            logger.error("ds_auth_guard_init_failed", err=str(exc))
            return 1
        # Go 的 dsGuard 恒非 nil(off 档是个"什么都放行"的守卫);Python 的
        # guard_from_conf 在 off 档返回 None。两者语义等价,判据统一用解析后的档位。
        guard_mode = dsauth.parse_mode(cfg.ds_auth.mode)
        if ds_guard is None and guard_mode is not dsauth.Mode.OFF:
            # 守卫构造不出来却不是 off 档 = 声称校验实际不校验,必须拒。
            logger.error(
                "ds_auth_guard_init_failed",
                err=f"ds_auth.mode={guard_mode.value} but no guard could be built",
                hint="绝不能把 mode=permissive/enforce 当成 off 继续跑",
            )
            return 1

        # 启动期 TTL 闸:本服务签发或校验 DS 回调令牌时,两个 TTL 必须过最小值,
        # 否则令牌签发即过期属误配,启动即拒(Go: `cfg.DSAuth.Validate(signer||guard)`)。
        try:
            cfg.ds_auth.validate_ttls(ds_signer is not None or ds_guard is not None)
        except ValueError as exc:
            logger.error("ds_auth_ttl_invalid", err=str(exc))
            return 1
        # 战斗令牌**不续期**(一局一签、DS 一局一销毁),TTL 必须覆盖
        # 「战斗镜像 TTL(battle_ttl)+ 重连/ready 余量」。固定下限拦不住这个,
        # 因为 battle_ttl 是可配的:长局跑到一半令牌过期 → 心跳 / ReportResult 被
        # enforce 守卫全拒 → 赛果无法结算。
        try:
            cfg.validate_battle_token_ttl_vs_battle_ttl(ds_signer is not None)
        except ValueError as exc:
            logger.error("ds_auth_battle_token_ttl_too_small_vs_battle_ttl", err=str(exc))
            return 1

        battle_token_ttl = cfg.ds_auth.battle_token_ttl_td()

        async def issue_battle_token(match_id: int) -> str:
            """agones legacy 档的令牌签发回调(经 GSA annotation 下发)。

            battle 令牌绑 match_id 而不绑 pod:分配那一刻还不知道 Agones 会选中哪个
            GameServer,pod↔match 的绑定由心跳 pod_mismatch 逻辑兜底。
            """
            # ★ signer 在这里必然非 None:注入点由 `ds_signer is not None` 守着。
            #   写成"None 就返回空串"会让 enforce 档分配出一台永远回调不来的 DS。
            token, _exp_ms = ds_signer.sign_ds_callback(  # type: ignore[union-attr]
                pauth.DS_TYPE_BATTLE, "", match_id, battle_token_ttl
            )
            return token

        async def issue_local_battle_credential(
            match_id: int, pod_name: str, instance_uid: str, instance_epoch: int
        ) -> tuple[str, BattleCredentialIdentity]:
            """local-off-v1 的完整 Model-B tuple 签发(经 env 下发)。

            local-off-v1 不接 Redis pending/ACK,但仍必须给 UE **完整**的 Model-B
            tuple,不能回退 legacy JWT。每个本机进程有随机实例 UID 与 jti;一局一实例,
            epoch/gen 从 1 起且不会在实例内回退。

            ★ "不接 Redis ACK" 只免掉了 staged→active 的提升,**没有**免掉心跳应答的
              ACK 回显:UE 的 SendBattleHeartbeat 无条件按 uid/epoch/gen/jti/writer_epoch
              五项比对应答 ACK,不过就丢掉整个 Command 与驱逐单。所以这里必须把
              jti/writer_epoch 一并回吐给分配器留存,供 legacy 心跳逐字段回显。
            """
            jti = str(_uuid.uuid4())
            res = ds_signer.sign_battle_credential(  # type: ignore[union-attr]
                match_id,
                pod_name,
                instance_uid,
                instance_epoch,
                LOCAL_BATTLE_GEN,
                jti,
                battle_token_ttl,
            )
            return res.token, BattleCredentialIdentity(
                pod_name=pod_name,
                instance_uid=instance_uid,
                instance_epoch=instance_epoch,
                gen=LOCAL_BATTLE_GEN,
                jti=jti,
                exp_ms=res.exp_ms,
                kid=res.kid,
                token_sha256=res.token_sha256,
                writer_epoch=res.writer_epoch,
            )

        # enforce 下签发失败必须 fail-closed(不分配无令牌的 DS,否则回调被守卫全拒)。
        ds_enforce = guard_mode is dsauth.Mode.ENFORCE
        model_b = cfg.ds_auth.authority_mode_redis()
        if model_b and (
            cfg.mode != dsconf.MODE_AGONES or not ds_enforce or ds_signer is None
        ):
            logger.error(
                "battle_model_b_invalid_activation",
                allocator_mode=cfg.mode,
                guard_mode=guard_mode.value,
                signer_ready=ds_signer is not None,
                hint="authority_mode=redis requires mode=agones + ds_auth.mode=enforce + "
                "signing key; no legacy fallback",
            )
            return 1
        if ds_signer is not None:
            logger.info(
                "ds_callback_token_issuer_ready",
                battle_token_ttl=godur.duration_string(battle_token_ttl),
                guard_mode=guard_mode.value,
            )

        # ── ⑥ 装配链 ─────────────────────────────────────────────────────────
        #
        # ★ 装的是 `departure.RedisBattleDepartureRepo` 而不是裸 `repo.RedisBattleRepo`:
        #   后者能跑,但少了离场 / teardown / lifecycle 证明能力,而缺失表现为
        #   `AttributeError` 发生在**第一次真的有玩家要离场**的时候,不是启动时。
        repo = dsdeparture.RedisBattleDepartureRepo(rdb)

        # DS 启动方式由 cfg.mode 单一开关决定(标准两模式 + 离线兜底),biz 逻辑零改:
        #   - mode=agones → 真 GameServerAllocation(Linux 生产)
        #   - mode=local  → 本机拉起 Windows DS 进程(Windows 单机自测)
        #   - mode=mock   → Mock 确定性假地址(无真实 DS,离线联调)
        allocator: dsgameserver.GameServerAllocator
        agones_alloc: AgonesGameServerAllocator | None = None  # 仅 agones 非空,供 Fleet 容量巡检
        if cfg.mode == dsconf.MODE_AGONES:
            try:
                agones_alloc = AgonesGameServerAllocator(cfg.agones)
            except ValueError as exc:
                logger.error(
                    "agones_allocator_init_failed",
                    err=str(exc),
                    hint="检查 agones.fleet_name / ca_path 配置",
                )
                return 1
            allocator = agones_alloc
            if ds_signer is not None and not model_b:
                # 令牌经 GameServerAllocation annotation 下发。
                # model_b 下不走这条:凭据由 Redis 权威仓在 pending→active 里签发。
                agones_alloc.set_ds_token_issuer(issue_battle_token, ds_enforce)
            logger.info(
                "agones_allocator_ready",
                api_server=cfg.agones.api_server,
                namespace=cfg.agones.namespace,
                fleet=cfg.agones.fleet_name,
            )
        elif cfg.mode == dsconf.MODE_LOCAL:
            # 本机 allocator 不得把**生产 / 灰度姿态**误标成离线 profile:
            # guard=off + authority=legacy + signer 就绪,三条缺一不可。
            # noqa 说明见 `_local_map_source`(下方模块级函数)。
            try:
                dsticket.validate_ds_local_profile_off_v1(
                    guard_mode.value, cfg.ds_auth.authority_mode, ds_signer is not None
                )
            except dsticket.DSTicketConfigError as exc:
                logger.error(
                    "local_battle_auth_profile_invalid",
                    err=str(exc),
                    hint="mode=local requires ds_auth.mode=off + authority_mode=legacy + "
                    "signing key (local-off-v1); Redis Model-B local authority is not implemented",
                )
                return 1
            try:
                local_alloc = LocalGameServerAllocator(cfg.local_ds)
            except ValueError as exc:
                logger.error(
                    "local_ds_allocator_init_failed",
                    err=str(exc),
                    launcher=cfg.local_ds.launcher,
                    hint="mode=local 两种 DS 形态二选一:launcher=packaged 需 local_ds.executable_path "
                    "指向打包好的 UE Windows DS(PandoraServer.exe);launcher=editor 需 executable_path "
                    "指向 UnrealEditor.exe 且 project_path 指向 Pandora.uproject(免出包,直接读未 cook "
                    "的工程内容)。一键脚本:start.ps1 -Mode local -DsLauncher editor 会自动探测引擎与工程"
                    "并经 PANDORA_DS_LAUNCHER/PANDORA_DS_EXE/PANDORA_DS_UPROJECT 注入。",
                )
                return 1
            allocator = local_alloc
            # 完整 tuple 经 env 下发;失败必须 fail-closed(required=True),
            # 与 guard 档位无关 —— UE 侧 local-off-v1 无条件要求完整凭据。
            local_alloc.set_ds_token_issuer(issue_local_battle_credential, True)
            # 关卡解析器:每局**现查**关卡表(唯一权威源 g_关卡.xlsx),不再有第二份 yaml 映射。
            # 现查而非启动时快照 —— 这样 ReloadConfigTable 之后新增的副本无需重启本服务即可开局。
            # ct_store 为空时不注入:此时必然配了 loader_map(validate_local_map_source_config
            # 已挡),DS 侧 Loader GameMode 查同一张表决定目标图。
            if ct_store is not None:
                local_alloc.set_map_url_resolver(ct_store.battle_launch_url)
            logger.info(
                "local_ds_allocator_ready",
                launcher=cfg.local_ds.launcher,
                project=cfg.local_ds.project_path,
                executable=cfg.local_ds.executable_path,
                map_source=_local_map_source(cfg.local_ds.loader_map, ct_store is not None),
                loader_map=cfg.local_ds.loader_map,
                advertise_host=cfg.local_ds.advertise_host,
                port_base=cfg.local_ds.port_base,
                port_range=cfg.local_ds.port_range,
            )
        else:
            allocator = dsgameserver.MockGameServerAllocator(cfg.allocator)
            logger.warning(
                "mock_allocator_active",
                mode=cfg.mode,
                hint="mode=mock,用确定性假地址(无真实 DS)",
            )

        uc = biz.AllocatorUsecase(repo, allocator, cfg.allocator)
        lifecycle_required = cfg.requires_reliable_lifecycle_publication()
        uc.set_lifecycle_pusher_required(lifecycle_required)

        # no-show 记账 → 进入侧退避(anti-abuse):空场判弃 reason=no_show 时对 roster 记账并
        # 布退避窗,matchmaker StartMatch 读取执行。共享 rdb;fail-open。
        uc.set_no_show_recorder(dsrepo.RedisNoShowRecorder(rdb))
        logger.info(
            "noshow_recorder_ready",
            ledger_window=godur.duration_string(cfg.allocator.no_show_ledger_window_td()),
            penalty_base=godur.duration_string(cfg.allocator.no_show_penalty_base_td()),
            penalty_cap=godur.duration_string(cfg.allocator.no_show_penalty_cap_td()),
            penalty_free=cfg.allocator.no_show_penalty_free,
        )

        # ⑥.2 owner 权威实例租约双写(owner-authority.md migrate ⑥):owner_addr 空 = 不启用。
        # 弱/强依赖语义见 conf.OwnerLeaseRequired 注释。
        if cfg.allocator.owner_addr != "":
            owner_lease = dsclients.GrpcOwnerLeaseRenewer(cfg.allocator.owner_addr)
            uc.set_owner_lease_renewer(owner_lease, cfg.allocator.owner_lease_required)
            # migrate ②/③:READY 交付 Begin(BATTLE) + census 代提交 Admit(同一连接,弱依赖)。
            uc.set_owner_authority(owner_lease)
            logger.info(
                "owner_lease_dual_write_enabled",
                owner_addr=cfg.allocator.owner_addr,
                required=cfg.allocator.owner_lease_required,
            )

        # ⑥.3 灰度轨道策略(§9 不变量 21)───────────────────────────────────────
        try:
            release_policy = releasetrack.new(cfg.agones.canary_percent, cfg.agones.canary_seed)
        except ValueError as exc:
            logger.error("battle_release_track_policy_invalid", err=str(exc))
            return 1
        uc.set_release_track_policy(release_policy)
        logger.info(
            "battle_release_track_policy_ready",
            canary_percent=cfg.agones.canary_percent,
            canary_seed_configured=bool(cfg.agones.canary_seed),
        )

        # ⑥.4 Battle Model B 权威仓 + 授权激活(Go `main.go` 的 `EnableRedisAuthority`)──
        #
        # ★ 仓**无条件**构造、authority 只在 model_b 下打开(Go 同形)。构造本身不产生
        #   任何 IO,也不等于启用 Model B;之所以要提到这里,是因为 gm 侧必须复用
        #   **同一个实例** —— 两处各造一个的话,不可逆的 strict 写档只会开在其中一个上,
        #   另一个仍按宽松档写,连续存储不变量形同虚设。
        battle_auth_repo = dsbattleauth.RedisBattleAuthRepo(rdb)
        if model_b:
            # ★ 依赖必须一次完整注入:任何一项缺失都拒绝启动,禁止"配置说 redis
            #   authority、实际悄悄回退 legacy"的半开启状态 —— 那种状态下玩家重连
            #   被静默退化成回大厅,而运维看到的是"服务健康"。
            try:
                uc.enable_redis_authority(
                    battle_auth_repo, ds_signer, battle_token_ttl.total_seconds()
                )
            except errcode.PandoraError as exc:
                logger.error("battle_model_b_init_failed", err=str(exc))
                return 1
            logger.info(
                "battle_model_b_enabled",
                required_writer_epoch=dsbattleauth.BATTLE_DS_WRITER_EPOCH_V2,
                authority="redis",
                k8s_role="delivery-only",
            )

        # ⑥.5 mode=local 的孤儿回收开关(Go 的 `uc.SetKillOrphanOnStop(true)`)────
        #
        # local 模式的 UE DS 没有 Agones,收到 stop 指令不会自杀 → 让后端在
        # orphan / pod_mismatch / 终态心跳时主动 kill 该 DS,防幽灵进程占着监听端口
        # 污染下一局(与端口 bind 探测双保险)。
        #
        # ★ 判据只有 `mode == local` 一条,与 guard 档位、signer 有没有、model_b 都无关。
        # ★ agones 模式刻意**不**打开:孤儿 GameServer 由 Agones 生命周期回收,
        #   Redis 抖动误判 orphan 时主动 kill 会误删正在打的正常 pod。
        if cfg.mode == dsconf.MODE_LOCAL:
            uc.set_kill_orphan_on_stop(True)

        # ⑥.6 ds.lifecycle producer。弱依赖:broker 不通则 warn 并继续,abandoned 推送
        # 静默丢弃,battle_result 的既有兜底(match release + battle exit proof)仍生效。
        if cfg.kafka.brokers:
            try:
                producer = kafkax.KeyOrderedProducer(
                    kafkax.producer_conf_from(cfg.kafka), kafka_topics.TOPIC_DS_LIFECYCLE
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                if lifecycle_required:
                    logger.error(
                        "ds_lifecycle_producer_required_but_unavailable",
                        err=str(exc),
                        hint="reliable publication is required but kafka init failed",
                    )
                    return 1
                else:
                    logger.warning(
                        "ds_lifecycle_producer_init_failed",
                        err=str(exc),
                        hint="abandoned push will be silently dropped until kafka is available",
                    )
            else:
                uc.set_lifecycle_pusher(DSLifecyclePusher(producer))
                logger.info(
                    "ds_lifecycle_producer_ready",
                    topic=kafka_topics.TOPIC_DS_LIFECYCLE,
                    required=lifecycle_required,
                )
        elif lifecycle_required:
            logger.error(
                "ds_lifecycle_producer_required_but_unavailable",
                err="kafka.brokers is empty",
                hint="reliable publication is required but kafka is not configured",
            )
            return 1
        else:
            logger.warning(
                "ds_lifecycle_disabled_dev_only",
                reason="kafka.brokers is empty",
                hint="only local/off development may run without abandoned recovery publication",
            )

        # 装配期的最后一道 lifecycle 门(Go 的 `uc.ValidateLifecyclePusherReady()`)。
        #
        # 前面那一堆分支里任何一条未来被改坏,都会在这里被拦住:配置宣称
        # "必须可靠发布"时,publisher 为 None 不允许启动任何 sweep / RPC —— 否则
        # abandoned 事件静默丢失,battle_result 永远等不到那条恢复消息,而表面上
        # 服务完全健康。重复判定是有意的（“最后一道”就是它的全部价值）。
        try:
            uc.validate_lifecycle_pusher_ready()
        except errcode.PandoraError as exc:
            logger.error("ds_lifecycle_startup_gate_failed", err=str(exc))
            return 1

        # ⑥.7 player_locator client → LocationRefresher ───────────────────────
        # 弱依赖:续期短 TTL BATTLE presence(玩家在线/战斗中的唯一路由信号)。
        # locator_addr 留空不续期,长对局中途重登可能因位置过期退化为回大厅。
        if cfg.locator_addr != "":
            location_refresher = dsclients.GrpcLocationRefresher(cfg.locator_addr)
            uc.set_location_refresher(location_refresher)
            logger.info("locator_client_ready", locator_addr=cfg.locator_addr)
        else:
            logger.warning(
                "locator_addr_empty",
                hint="BATTLE presence 不续期；玩家战斗中无法被 login 检测到",
            )

        # ⑥.8 gRPC service + matchmaker→allocator 破坏性 RPC 的服务间验签 ─────────
        #
        # ★ 守卫在闸⑤就已经构造完并做过档位校验,这里**只注入不重造**:重造一份等于
        #   让"档位判定"有两个入口,两处一旦漂移,日志里看到的是一个档、真正生效的是
        #   另一个 —— 而这种漂移在测试里几乎不可能被发现。
        svc = dssvc.AllocatorService(uc)
        svc.set_ds_callback_guard(ds_guard)  # DS 回调令牌校验(Heartbeat);None=off

        if model_b:
            # replay store 与 verifier 分两步构造、分两个事件名(照抄 Go):
            # 前者失败 = Redis 客户端没给对,后者失败 = secret / audience 配错,
            # 归成一条事件名的话运维得把两种完全不同的排障路径先猜一遍。
            #
            # ★ 共享 replay store 是**必填**:没有它,一份合法凭证可以在时钟偏差窗内
            #   被重放到另一个副本上,而那个副本毫不知情(pre-admission abort 是破坏性
            #   RPC,重放一次就多废掉一场已经分配好的对局)。
            try:
                abort_replay_store = internalrpcauth.RedisReplayStore(
                    rdb, ALLOCATION_ABORT_NONCE_PREFIX
                )
            except ValueError as exc:
                logger.error("allocation_abort_replay_store_init_failed", err=str(exc))
                return 1
            try:
                abort_verifier = internalrpcauth.Verifier(
                    cfg.allocator.allocation_abort_auth_secret,
                    ALLOCATION_ABORT_CALLER,
                    cfg.allocator.allocation_abort_auth_audience,
                    ALLOCATION_ABORT_MAX_CLOCK_SKEW_SEC,
                    abort_replay_store,
                )
            except ValueError as exc:
                logger.error("allocation_abort_verifier_init_failed", err=str(exc))
                return 1
            svc.set_allocation_abort_verifier(abort_verifier)
            logger.info(
                "allocation_abort_service_auth_ready",
                audience=cfg.allocator.allocation_abort_auth_audience,
            )

        # ⑥.9 GmService(GM / 运维指令下发):与 ds_allocator 同进程复用 gRPC 端口。
        # 运维 GM 工具 SendCommand 入 Redis 队列 → 战斗 DS 轮询 PollCommands 拉取执行。
        # 内部接口,不经 Envoy 暴露给玩家客户端。
        gm_svc = dsgm.GmService(rdb)
        gm_svc.set_ds_callback_guard(ds_guard)  # DS 回调令牌校验(PollCommands/AckCommand);None=off
        if model_b:
            # 出队 / Ack 都要过 active 校验。★ 传的是 ⑥.4 那个**同一个** repo 实例:
            # 另造一个的话它的 strict 写档没被打开,GM 侧就绕过了连续存储不变量。
            try:
                gm_svc.enable_redis_authority(battle_auth_repo)
            except errcode.PandoraError as exc:
                logger.error("gm_battle_model_b_init_failed", err=str(exc))
                return 1
        # SendCommand 前置校验目标对局是否有活跃战斗镜像:repo 天然满足 BattleLivenessChecker。
        gm_svc.set_battle_checker(repo)

        # ⑥.10 配置表热更入口(§9.15 标准流水线):启用配置表时一并注册,策划改完 g_关卡.xlsx
        # 重导表后直接 ReloadConfigTable 即可让新副本可开局,无需重启 ds_allocator。
        ct_admin_svc = None
        if ct_store is not None:
            ct_admin_svc = configtable_admin.ConfigTableAdminService(ct_store)

        # ── ⑦ gRPC + HTTP server 构造(Go `main.go` 第 5 段)──────────────────────
        #
        # ★ 只**构造**、不监听:真正开始 accept 在函数末尾的 `pserver.run` 里。
        #   位置照抄 Go —— 闸⑧的 capability 租约必须在任何 RPC 可见之前取得,而
        #   writer health 探针要挂在 http_app 造好之后、租约注入之前。
        grpc_server = pserver.build_grpc_server(cfg.server.grpc, auth_required=False)
        dsgrpc.add_DSAllocatorServiceServicer_to_server(svc, grpc_server)
        gmgrpc.add_GmServiceServicer_to_server(gm_svc, grpc_server)
        if ct_admin_svc is not None:
            cfggrpc.add_ConfigTableAdminServiceServicer_to_server(ct_admin_svc, grpc_server)
        if cfg.server.grpc.enable_reflection:
            pserver.enable_reflection(
                grpc_server,
                [
                    GRPC_SERVICE_FULL_NAME,
                    dsgm.GRPC_SERVICE_FULL_NAME,
                    configtable_admin.GRPC_SERVICE_FULL_NAME,
                ],
            )

        http_app = pserver.build_http_app(SERVICE_NAME)
        # sweep 单写者租约的健康面(Go `server.NewHTTPServer(&cfg, writerHealth)`):
        # `/healthz/writer` + 抓取即刷新的 `/metrics`。
        #
        # ★ **无条件**挂上,不只在 model_b 下挂:租约未注入时 snapshot() 明确回
        #   `enabled=false` 且 gauge 归零,而不是乐观的 `held=true`。少挂的话
        #   "没人在扫"这类静默故障连一个可查的端点都没有。
        writer_health = dshttp.WriterHealthHolder()
        dshttp.install(http_app, writer_health)

        # ── ⑧ capability 租约 + 写者继任租约(Go `main.go` 第 5 段尾 / 5.1)────────
        #
        # sweep / capacity watcher 也是 writer;capability 未取得前禁止启动任何后台
        # 循环或对外 RPC —— 所以整块必须排在 `pserver.run` 之前、server 构造之后。
        writer_lease: writerlease.WriterLease | None = None
        if model_b:
            # ★ 身份不从 hostname / image tag 推:两者都可伪造、可漂移,而 capability
            #   key 的唯一性正建立在 PodUID 上(`acquire_runtime` 只认 Downward API)。
            # ★ features 逐字照抄常量元组:etcd 侧是**精确相等**比较,多一个 / 少一个 /
            #   拼错一个字母都不是"降级注册",而是直接注册不上 → fail-closed。
            try:
                fence = await dsauthfence.acquire_runtime(
                    dsauthfence.RuntimeConfig(
                        endpoints=list(cfg.ds_auth.fence.etcd_endpoints),
                        prefix=cfg.ds_auth.fence.etcd_prefix,
                        service=SERVICE_NAME,
                        keyset_revision=cfg.ds_auth.fence.keyset_revision,
                        writer_epoch=dsauthfence.PROTOCOL_EPOCH_V2,
                        features=DS_AUTH_FENCE_FEATURES,
                        lease_ttl_sec=cfg.ds_auth.fence.etcd_lease_ttl_sec,
                        dial_timeout_sec=cfg.ds_auth.fence.etcd_dial_timeout_td().total_seconds(),
                    )
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                logger.error(
                    "ds_auth_fence_acquire_failed",
                    err=str(exc),
                    hint="失租 / 旧 epoch 副本必须立即退出,禁止旧 writer 在 epoch 回退后继续写",
                )
                return 1
            logger.info(
                "ds_auth_fence_ready",
                required_writer_epoch=fence.required_epoch(),
                reclaimed_stale_capability=fence.reclaimed,
            )

            # 5.1 心跳扫描的写者继任租约(2026-07-29 事故闭环)。
            #
            # 目的**不是**给 sweep 加防脑裂 —— 那由既有的按 match 凭据 CAS 承担;
            # 目的是让 ds_allocator 能安全地跑**多副本 + RollingUpdate**,从而把
            # "单副本重启 = 全服 Heartbeat 不可用 = 所有 Battle DS 在 20s 后踢人"
            # 这条链断开。capability key 按 (service, PodUID) 唯一,多副本天然共存,
            # 故此处不需要放宽任何 fencing。
            try:
                writer_mode = cfg.allocator.resolve_writer_lease_mode()
            except ValueError as exc:
                logger.error("ds_writer_lease_mode_invalid", err=str(exc))
                return 1
            if not _check_writer_lease_deploy_strategy(logger, writer_mode):
                return 1
            if writer_mode == dsconf.WRITER_LEASE_OFF:
                logger.warning(
                    "ds_writer_lease_disabled",
                    hint="writer_lease_mode=off:单扫描者只由部署策略保证,只允许单副本 Recreate",
                )
            else:
                # ★ Go 那边显式传 `Identity: hostname/pid`;Python 侧的 WriterLease 用
                #   etcd 租约 ID 作为参选键,身份由 etcd 分配而不是自报 —— 两者都满足
                #   "同一副本重启后不会顶掉自己的旧任期"(旧租约到期即消失)。
                # ★ lease_ttl_sec 只在配了正值时传:传 0 会被当成"TTL 为零"而不是
                #   "用默认",而 TTL 为零的租约等于**永远持不住**,enforce 档下心跳
                #   扫描将永久停摆(表现为超时对局永不判弃)。
                lease_kwargs: dict[str, int] = {}
                if cfg.ds_auth.fence.etcd_lease_ttl_sec > 0:
                    lease_kwargs["lease_ttl_sec"] = cfg.ds_auth.fence.etcd_lease_ttl_sec
                try:
                    writer_lease = writerlease.WriterLease(
                        list(cfg.ds_auth.fence.etcd_endpoints),
                        DS_WRITER_LEASE_ELECTION,
                        **lease_kwargs,
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001
                    logger.error("ds_writer_lease_start_failed", err=str(exc))
                    return 1
                if writer_mode == dsconf.WRITER_LEASE_ENFORCE:
                    # warmup 档**刻意不注入**:只竞选、观测 token 单调,不改扫描行为。
                    uc.set_sweep_writer_lease(writer_lease)
                writer_health.set(writer_lease, writer_mode)
                logger.info(
                    "ds_writer_lease_started",
                    election=DS_WRITER_LEASE_ELECTION,
                    mode=writer_mode,
                    hint="enforce:只有当选副本跑心跳超时扫描,热备副本照常服务 Heartbeat/AllocateBattle;"
                    "warmup:只竞选观测 token 单调,不改扫描行为",
                )

        def _on_ready() -> None:
            logger.info(
                "service_ready",
                grpc=cfg.server.grpc.addr,
                http=cfg.server.http.addr,
                redis_addr=rc.host,
                heartbeat_timeout=godur.duration_string(cfg.allocator.heartbeat_timeout_td()),
                sweep_interval=godur.duration_string(cfg.allocator.sweep_interval_td()),
                allocator_mode=cfg.mode,
                runtime="python",  # 灰度期用它在 Grafana 里区分两个实现
            )

        # ── ⑨ 后台心跳超时扫描(随进程生命周期启停)──────────────────────────
        async def heartbeat_sweep() -> None:
            """对应 Go 的 `go uc.RunHeartbeatSweep(sweepCtx)`。

            走 safego:单轮抛异常时只跳过本轮并留日志,下一拍继续。裸 create_task
            的话这条循环死掉后进程照跑、health 照答 SERVING、零日志,表现是
            「心跳超时的 DS 永远不转 abandoned」而没有任何报错 —— 玩家被持续路由到
            一台已经死掉的战斗 DS。
            """
            await safego.loop(
                "ds_heartbeat_sweep",
                cfg.allocator.sweep_interval_td().total_seconds(),
                uc.sweep_once,
            )

        background: list = [heartbeat_sweep]
        if fence is not None:
            # 失租守望必须与 sweep 同生命周期:它是 capability 失效到进程退出之间
            # 唯一的传导路径,漏挂等于把 fail-closed 退化成 fail-open。
            background.append(("ds_auth_fence_lost_watch", lambda: _watch_fence_lost(fence)))
        if writer_lease is not None:
            background.append(
                ("ds_writer_lease", lambda: writer_lease.run(_noop_on_elected))
            )

        # ── ⑩ Fleet 容量巡检(仅 agones 模式;Go `main.go` 6.1)────────────────
        # 定期 GET Fleet status → 暴露 pandora_ds_allocator_fleet_* 指标,容量快到上限
        # 时打预警日志,让运维在打满**之前**扩 Fleet(打满后表现是"匹配成功却进不去")。
        # capacity_watch_interval 设负值 = 显式禁用(工厂返回 None)。
        if agones_alloc is not None:
            capacity_watcher = dscapacity.new_capacity_watcher(agones_alloc, cfg.agones)
            if capacity_watcher is not None:
                background.append(("fleet_capacity_watch", capacity_watcher.run))
                logger.info(
                    "fleet_capacity_watch_enabled",
                    # 打的是**配置原值**(与 Go 的 `cfg.Agones.CapacityWatchInterval.String()`
                    # 同口径):未配时这里是 "0s",而实际取默认 30s —— 想知道实际值看
                    # fleet_capacity_watch_started 那条,两条各自忠实于自己的来源。
                    interval=godur.duration_string(cfg.agones.capacity_watch_interval_td()),
                    warn_ratio=cfg.agones.capacity_warn_ratio,
                    fleets=agones_alloc.watched_fleets(),
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
        # 关闭顺序与 Go 的 defer 栈同序(后进先出):
        # capability 租约 → locator → owner → kafka → local DS → redis。
        #
        # ★ capability 排最前:它是"本副本还有没有写权"的唯一凭证,必须在任何
        #   其它资源被拆掉之前主动交还,好让继任副本尽早接管(否则要空等一个 TTL)。
        # ★ local DS 排在 redis 之前:kill 在管的 DS 进程要能写最后一次状态。
        if fence is not None:
            with contextlib.suppress(Exception):
                await fence.close()
        for closable in (location_refresher, owner_lease, producer, local_alloc):
            if closable is not None:
                with contextlib.suppress(Exception):
                    await closable.close()
        with contextlib.suppress(Exception):
            await rdb.aclose()


def main(argv: list[str] | None = None) -> int:
    """服务主入口(正常启动路径)。"""
    args = _parse_args(argv)

    # ── Pod UID config comparison 独立入口(在 asyncio.run 之前退出)─────────────
    if args.pod_uid_release_preflight_compare_configs:
        if args.pod_uid_release_preflight:
            print(  # noqa: T201
                "pod_uid config comparison mode conflict", file=sys.stderr
            )
            return 2
        try:
            stdin_text = sys.stdin.read()
            matched, output_json = _run_pod_uid_config_compare_sync(stdin_text)
        except ValueError as exc:
            print(  # noqa: T201
                f"pod_uid config comparison failed: {exc}", file=sys.stderr
            )
            return 1
        except Exception as exc:  # noqa: BLE001
            print(  # noqa: T201
                f"pod_uid config comparison failed: {exc}", file=sys.stderr
            )
            return 1
        print(output_json)  # noqa: T201
        return 0

    # ── 正常服务启动(async 链路)─────────────────────────────────────────────
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
