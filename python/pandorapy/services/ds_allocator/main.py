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

★ 未移植能力一律 fail-fast,**不静默降级**(与 hub_allocator/main.py 同姿态):
  Python 运行时目前没有 RS256 DSTicket 签发器、没有 DS 回调令牌**签发**器、
  没有 dsauthfence capability 租约。这三样缺任何一个都不能"当成没配置"继续跑 ——
  那会让 agones / Model B / enforce 档变成"看起来在跑、实际零鉴权"。
  因此 Python 侧当前可运行档位是 `mode=mock`(离线联调)与 legacy HS256 票据面。

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
import datetime as _dt
import json
import os
import pathlib
import re
import sys
from typing import Any

from pandora.config.v1 import configtable_pb2_grpc as cfggrpc
from pandora.ds.v1 import allocator_pb2_grpc as dsgrpc
from pandora.gm.v1 import gm_pb2_grpc as gmgrpc

from pandorapy import godur
from pandorapy import internalrpcauth
from pandorapy import kafka_topics
from pandorapy import kafkax
from pandorapy import log as plog
from pandorapy import redisx
from pandorapy import releasetrack
from pandorapy import safego
from pandorapy import server as pserver

from pandorapy.services.ds_allocator import biz
from pandorapy.services.ds_allocator import catalog
from pandorapy.services.ds_allocator import clients as dsclients
from pandorapy.services.ds_allocator import conf as dsconf
from pandorapy.services.ds_allocator import configtable_admin
from pandorapy.services.ds_allocator import gm as dsgm
from pandorapy.services.ds_allocator import repo as dsrepo
from pandorapy.services.ds_allocator import service as dssvc
from pandorapy.services.ds_allocator.agones_allocator import AgonesGameServerAllocator
from pandorapy.services.ds_allocator.local_allocator import LocalGameServerAllocator
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
    rc: pconfig.RedisClientConf,
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
        before = await prove_read_only_and_identify(rdb, rc, username)
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
    # ★ 这些闸**不是**在 main 里重写一遍,而是调 `conf.validate_conf()` —— 它已经
    #   逐条对着 Go 的 main.go 实现,包含每条的 fail-fast / warn-only 档位。在这里
    #   照抄一份的话,两处判据一旦漂移,"哪一份才是真的"就没人说得清了。
    #
    # ★ 致命项抛 ValueError,消息前缀就是 Go 的事件名(如
    #   `ds_auth_fence_config_invalid: ...`),这里拆出来当事件名打,
    #   Loki 上两个实现的告警可以用同一条查询。
    #
    # ★ 返回值是**告警**,不是错误。方向弄反会出事:比如密钥交叉在 off/permissive
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

    # ── ④ Redis(强依赖:DS 状态镜像)─────────────────────────────────────────
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
        #   · mode=agones:要 v2 签票 + 回调令牌派发。缺任一 → 分配出去的 DS
        #     永远翻不到 ready、票据从实例绑定静默退回无绑定 legacy 票(§9.3/§9.22 全线失效)。
        #   · mode=local :要回调凭据签发器(local-off-v1 靠它经 env 播种本机 DS)。
        #     缺了 DS 拿不到凭据,准入租约只认绑定式心跳 ACK → 玩家被踢下线。
        #   · mode=mock  :两样都用不到(Mock provider 不派发也不验票),照常启动。
        if cfg.mode == dsconf.MODE_AGONES:
            logger.error(
                "battle_model_b_invalid_activation",
                err="python runtime has no RS256 DSTicket signer / DS callback token "
                "signer (pkg/auth.NewDSTicketSignerFromConf + "
                "pkg/middleware.NewDSCallbackSignerFromConf not ported)",
                hint="用 Go 版跑 mode=agones;绝不能把配了私钥的 v2 档静默退回"
                "无绑定 legacy 票,也不能不派发回调令牌就让分片翻 ready",
            )
            return 1
        if cfg.mode == dsconf.MODE_LOCAL:
            logger.error(
                "local_battle_auth_profile_invalid",
                err="python runtime has no DS callback credential signer "
                "(local-off-v1 requires it to seed the local Battle DS)",
                hint="mode=local requires ds_auth.mode=off + authority_mode=legacy + "
                "signing key (local-off-v1); 用 Go 版跑 mode=local",
            )
            return 1

        # ── ⑥ 装配链 ─────────────────────────────────────────────────────────
        repo = dsrepo.RedisHubRepo(rdb)

        # DS 启动方式由 cfg.mode 单一开关决定(标准两模式 + 离线兜底),biz 逻辑零改:
        #   - mode=agones → 真 GameServerAllocation(Linux 生产)
        #   - mode=local  → 本机拉起 Windows DS 进程(Windows 单机自测)
        #   - mode=mock   → Mock 确定性假地址(无真实 DS,离线联调)
        #
        # ⚠️ agones / local 两条在闸⑤已经被拒掉,走到这里只剩 mock。
        # **刻意不保留那两个分支的空壳**(与 hub_allocator 同),留着会让人以为
        # Python 支持它们。将来接 v2 时按 Go `main.go:300-359` 补回。
        allocator: biz.GameServerAllocator = biz.MockGameServerAllocator(cfg.allocator)
        logger.warning(
            "mock_allocator_active",
            mode=cfg.mode,
            hint="mode=mock,用确定性假地址(无真实 DS)",
        )

        # ⑥.1 配置表(不变量 §9.15):config_table.dir 配置后是启动强依赖,加载失败直接退出。
        # ds_allocator 只用其中的关卡表:mode=local 起 DS 时按 map_id 现查 g_关卡.xlsx 拼关卡 URL
        # (asset_path + game_mode_class),取代 2026-08-04 之前那张手抄的 local_ds.maps 影子表。
        # mode=agones 不读它(关卡由 DS 侧 Loader GameMode 查同一张表决定),留空即可。
        ct_store: catalog.Store | None = None
        if cfg.config_table.dir.strip():
            ct_store = catalog.Store()
            # 批次级校验器:关卡表里**每一张**战斗类关卡都必须能构造出合法启动 URL。
            # 启动首载与之后每次热 reload 走同一门禁,坏批次整批不切换、保留旧表——
            # 把"某张图资源列填错"挡在加载边界,而不是等玩家恰好选中那张图才炸。
            ct_store.add_validator(catalog.validate_battle_launch_urls)
            try:
                res = await ct_store.load(cfg.config_table.dir, 0)
            except catalog.ConfigTableError as exc:
                logger.error("configtable_load_failed", dir=cfg.config_table.dir, err=str(exc))
                return 1
            for w in res.warnings:
                logger.warning("configtable_load_warning", warning=w)
            logger.info(
                "configtable_loaded",
                dir=cfg.config_table.dir,
                version=res.version,
                levels=ct_store.tables().level.count() if ct_store.tables() else 0,
            )

        uc = biz.AllocatorUsecase(repo, allocator, cfg.allocator)
        lifecycle_required = cfg.requires_reliable_lifecycle_publication()
        uc.set_lifecycle_pusher_required(lifecycle_required)

        # no-show 记账 → 进入侧退避(anti-abuse):空场判弃 reason=no_show 时对 roster 记账并
        # 布退避窗,matchmaker StartMatch 读取执行。共享 rdb;fail-open。
        uc.set_noshow_recorder(dsrepo.RedisNoShowRecorder(rdb))
        logger.info(
            "noshow_recorder_ready",
            ledger_window=godur.duration_string(cfg.allocator.noshow_ledger_window_td()),
            penalty_base=godur.duration_string(cfg.allocator.noshow_penalty_base_td()),
            penalty_cap=godur.duration_string(cfg.allocator.noshow_penalty_cap_td()),
            penalty_free=cfg.allocator.noshow_penalty_free,
        )

        # ⑥.2 owner 权威实例租约双写(owner-authority.md migrate ⑥):owner_addr 空 = 不启用。
        # 弱/强依赖语义见 conf.OwnerLeaseRequired 注释。
        owner_lease = None
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

        # Model B 那套整链在 Python 侧走不到(闸⑤已拒 agones),不复制 Go `main.go:394-406`
        # 的那段 `if modelB { uc.EnableRedisAuthority(...) }`。将来接 v2 时按 Go 补齐。

        # mode=local 专用的 kill 孤儿逻辑在 Python 侧也走不到(闸⑤已拒 local)。
        # 将来接 local 时补齐 `if cfg.mode == MODE_LOCAL: uc.set_kill_orphan_on_stop(True)`。

        # ⑥.4 ds.lifecycle producer。弱依赖:broker 不通则 warn 并继续,abandoned 推送
        # 静默丢弃,battle_result 的既有兜底(match release + battle exit proof)仍生效。
        producer = None
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
                uc.set_lifecycle_pusher(dsrepo.KafkaDSLifecyclePusher(producer))
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

        # ⑥.5 player_locator client → LocationRefresher ───────────────────────
        # 弱依赖:续期短 TTL BATTLE presence(玩家在线/战斗中的唯一路由信号)。
        # locator_addr 留空不续期,长对局中途重登可能因位置过期退化为回大厅。
        location_refresher = None
        if cfg.locator_addr != "":
            location_refresher = dsclients.GrpcLocationRefresher(cfg.locator_addr)
            uc.set_location_refresher(location_refresher)
            logger.info("locator_client_ready", locator_addr=cfg.locator_addr)
        else:
            logger.warning(
                "locator_addr_empty",
                hint="BATTLE presence 不续期；玩家战斗中无法被 login 检测到",
            )

        # ⑥.6 DS 回调守卫(未移植 signer,只有守卫可用)─────────────────────────
        # 档位合法性、secret 齐备性已由闸③的 `ds_auth_guard_init_failed` 判过。
        ds_guard = None
        try:
            ds_guard = cfg.ds_auth.guard()
        except ValueError as exc:
            logger.error("ds_auth_guard_init_failed", err=str(exc))
            return 1
        # Go 的 dsGuard 恒非 nil(off 档是个"什么都放行"的守卫);Python 的
        # guard_from_conf 在 off 档返回 None。两者语义等价,判据统一用 mode 字符串。
        guard_mode = (cfg.ds_auth.mode or "").strip().lower() or "off"
        if ds_guard is None and guard_mode != "off":
            # 守卫构造不出来却不是 off 档 = 声称校验实际不校验,必须拒。
            logger.error(
                "ds_auth_guard_init_failed",
                err=f"ds_auth.mode={guard_mode} but no guard could be built",
                hint="绝不能把 mode=permissive/enforce 当成 off 继续跑",
            )
            return 1

        svc = dssvc.AllocatorService(uc)
        svc.set_ds_callback_guard(ds_guard)  # DS 回调令牌校验(Heartbeat);None=off

        # Model B 那套的 AllocationAbort verifier 在 Python 侧走不到(未接 Redis authority),
        # 不复制 Go `main.go:462-478` 的那段 `if modelB { svc.SetAllocationAbortVerifier(...) }`。
        # 将来接 v2 时按 Go 补齐。

        # ⑥.7 GmService(GM / 运维指令下发):与 ds_allocator 同进程复用 gRPC 端口。
        # 运维 GM 工具 SendCommand 入 Redis 队列 → 战斗 DS 轮询 PollCommands 拉取执行。
        # 内部接口,不经 Envoy 暴露给玩家客户端。
        gm_svc = dsgm.Service(rdb)
        gm_svc.set_ds_callback_guard(ds_guard)  # DS 回调令牌校验(PollCommands/AckCommand);None=off
        # Model B 那套 gm_svc.enable_redis_authority 在 Python 侧走不到,将来接 v2 时补齐。
        # SendCommand 前置校验目标对局是否有活跃战斗镜像:repo 天然满足 BattleLivenessChecker。
        gm_svc.set_battle_checker(repo)

        # ⑥.8 配置表热更入口(§9.15 标准流水线):启用配置表时一并注册,策划改完 g_关卡.xlsx
        # 重导表后直接 ReloadConfigTable 即可让新副本可开局,无需重启 ds_allocator。
        ct_admin_svc = None
        if ct_store is not None:
            ct_admin_svc = configtable_admin.ConfigTableAdminService(ct_store)

        # ── ⑦ 写者继任租约前置(未移植)────────────────────────────────────────
        # Go 在这里向 etcd 注册带租约的 capability、守望失租并在失租时立即退出。
        # Python 未移植 —— 少了它,失租 / epoch 回退的旧副本会继续写(§9.22 单写者破)。
        # 注:authority_mode=redis 在闸③已被要求 mode=agones,而 agones 在闸⑤被拒,
        # 因此实际到不了这里;保留本闸避免将来接 v2 后变成静默缺口。
        if cfg.ds_auth.authority_mode_redis():  # pragma: no cover
            logger.error(
                "ds_auth_fence_acquire_failed",
                err="python runtime has no dsauthfence capability lease "
                "(pkg/dsauthfence.AcquireRuntime not ported)",
                hint="失租 / 旧 epoch 副本必须立即退出,禁止旧 writer 在 epoch 回退后继续写",
            )
            return 1

        # 写者继任租约只在 authority_mode=redis 下启动,Python 侧一并不启动。
        # writer_lease_mode 的部署策略机械门禁同理(与 hub_allocator 同因)。
        try:
            writer_mode = cfg.allocator.resolve_writer_lease_mode()
        except ValueError as exc:
            logger.error("ds_writer_lease_mode_invalid", err=str(exc))
            return 1
        if os.environ.get("KUBERNETES_SERVICE_HOST", "").strip():
            # 受管 k8s 里跑 Python 版 = 走的是 legacy 档但部署侧可能是 RollingUpdate。
            # 这里不能只告警:Python 没有继任租约,滚动重叠期两个副本都会写。
            logger.error(
                "ds_writer_lease_start_failed",
                err="python runtime has no writer succession lease wired for this profile",
                mode=writer_mode,
                hint="受管 k8s 内必须由运行时协议保证单写者;用 Go 版部署到 k8s",
            )
            return 1
        logger.warning(
            "ds_writer_lease_disabled",
            mode=writer_mode,
            hint="writer_lease_mode 未接线:单写者不再由运行时协议保证,只允许单副本 "
            "Recreate 部署;RollingUpdate 下必须用 Go 版并改回 enforce",
        )

        # ── ⑧ gRPC + HTTP ──────────────────────────────────────────────────────
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

        # Fleet 容量巡检在 Python 侧走不到(闸⑤已拒 agones),将来接 agones 时按 Go
        # `main.go:566-574` 补齐 `if watcher := biz.NewCapacityWatcher(...); watcher != nil`。

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
        for closable in (location_refresher, owner_lease, producer):
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
